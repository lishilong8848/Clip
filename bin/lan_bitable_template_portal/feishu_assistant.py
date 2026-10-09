"""Feishu message transport for the existing assistant and its existing permissions."""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import os
from pathlib import Path
import re
import secrets
import sqlite3
import subprocess
import sys
import time
from contextlib import closing, contextmanager
from urllib.parse import urlsplit

from fastapi import Request
from fastapi.responses import JSONResponse

from openclaw_service.assistant.lighthouse_ai import AssistantError, safe_text, private_identifier, unprotect_key
from .portal_auth import AUTH_COOKIE_NAME, AUTH_SESSION_TTL_SECONDS


def parse_message(payload, app_id, bot_id):
    header, event = payload.get("header") or {}, payload.get("event") or {}
    if header.get("app_id") != app_id or header.get("event_type") != "im.message.receive_v1":
        return None
    sender, message = event.get("sender") or {}, event.get("message") or {}
    ids = sender.get("sender_id") or {}
    if sender.get("sender_type") != "user" or message.get("chat_type") not in {"p2p", "group"}:
        return None
    if not all(re.fullmatch(pattern, str(value or "")) for value, pattern in (
            (message.get("message_id"), r"om_[A-Za-z0-9]+"), (message.get("chat_id"), r"oc_[A-Za-z0-9]+"),
            (ids.get("open_id"), r"ou_[A-Za-z0-9]+"))):
        return None
    mentions = message.get("mentions") or []
    if message["chat_type"] == "group" and not any((m.get("id") or {}).get("open_id") == bot_id for m in mentions):
        return None
    content = json.loads(message.get("content") or "{}")
    question, problem = "", ""
    if message.get("message_type") == "text":
        question = content.get("text") or ""
    elif message.get("message_type") == "post":
        post = content.get("zh_cn") or content
        question = "\n".join([post.get("title") or ""] + ["".join(str(cell.get("text") or "")
            for cell in row if cell.get("tag") in {"text", "a", "md"}) for row in post.get("content", [])])
    for mention in mentions:
        question = question.replace(str(mention.get("key") or "\x00"), "")
    question = question.strip()
    if not question:
        question, problem = "[不支持的消息格式]", "unsupported"
    if len(question) > 12000:
        question, problem = "[消息超过12000字]", "oversize"
    if private_identifier(question):
        question, problem = "[敏感查询]", "private"
    return {"message_id": message["message_id"], "chat_id": message["chat_id"], "chat_type": message["chat_type"],
            "sender_id": ids["open_id"], "union_id": str(ids.get("union_id") or ""), "problem": problem,
            "question": safe_text(question, limit=12000) or "[内容已隐去]"}


class Inbox:
    """Durable, deduplicated inbox. No account count limit or per-user process."""
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.execute("CREATE TABLE IF NOT EXISTS messages (id TEXT PRIMARY KEY, owner TEXT NOT NULL, payload TEXT NOT NULL, state TEXT NOT NULL, reply TEXT, retry_at REAL NOT NULL DEFAULT 0)")
            db.execute("CREATE INDEX IF NOT EXISTS message_owner_state ON messages(state,retry_at,owner)")
            db.execute("UPDATE messages SET state='pending' WHERE state='processing'")

    @contextmanager
    def connect(self):
        with closing(sqlite3.connect(self.path, timeout=5)) as db:
            with db:
                yield db

    def add(self, message):
        with self.connect() as db:
            return db.execute("INSERT OR IGNORE INTO messages(id,owner,payload,state) VALUES(?,?,?,'pending')",
                (message["message_id"], message["union_id"] or "sender:" + message["sender_id"], json.dumps(message, ensure_ascii=False))).rowcount == 1

    def owners(self):
        with self.connect() as db:
            return [r[0] for r in db.execute("SELECT DISTINCT owner FROM messages WHERE state IN ('pending','reply') AND retry_at<=?", (time.time(),))]

    def take(self, owner):
        with self.connect() as db:
            row = db.execute("SELECT id,payload,state,reply FROM messages WHERE owner=? AND state IN ('pending','reply') AND retry_at<=? ORDER BY rowid LIMIT 1", (owner, time.time())).fetchone()
            if not row: return None
            db.execute("UPDATE messages SET state='processing' WHERE id=?", (row[0],))
            return {**json.loads(row[1]), "reply": json.loads(row[3]) if row[3] else None}

    def set(self, identity, state, reply=None, delay=0):
        with self.connect() as db:
            db.execute("UPDATE messages SET state=?,reply=COALESCE(?,reply),retry_at=? WHERE id=?",
                (state, json.dumps(reply, ensure_ascii=False) if reply is not None else None, time.time() + delay, identity))

    def claim_reaction(self, identity):
        with self.connect() as db:
            return db.execute("UPDATE messages SET payload=json_set(payload,'$.reaction_attempted',1) WHERE id=? AND COALESCE(json_extract(payload,'$.reaction_attempted'),0)=0",
                              (identity,)).rowcount == 1


class FeishuMessenger:
    def __init__(self, config):
        self.config, self.http, self.token, self.expires = config, None, "", 0
        self.lock = asyncio.Lock()
        self.http_lock = asyncio.Lock()

    async def client(self):
        async with self.http_lock:
            if self.http is None:
                import httpx
                self.http = await asyncio.to_thread(httpx.AsyncClient, timeout=20,
                    limits=httpx.Limits(max_connections=40, max_keepalive_connections=10))
        return self.http

    async def headers(self):
        async with self.lock:
            if time.monotonic() >= self.expires:
                client = await self.client()
                response = await client.post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
                    json={"app_id": self.config["app_id"], "app_secret": unprotect_key(self.config["secret_cipher"])})
                data = response.json()
                if data.get("code") != 0 or not data.get("tenant_access_token"):
                    raise AssistantError("飞书消息应用认证失败，请检查应用凭证。", 503)
                self.token, self.expires = data["tenant_access_token"], time.monotonic() + max(60, data.get("expire", 7200) - 120)
            return {"Authorization": "Bearer " + self.token}

    async def react(self, message):
        from .portal_service import external_real_write_guard
        if not external_real_write_guard()["real_write_allowed"]:
            return
        client = await self.client()
        response = await client.post("https://open.feishu.cn/open-apis/im/v1/messages/" + message["message_id"] + "/reactions",
            headers=await self.headers(), json={"reaction_type": {"emoji_type": "Typing"}}, timeout=3)
        data = response.json()
        if data.get('code') != 0:
            raise AssistantError('敲键盘表情未发送，飞书返回码：' + str(data.get('code')), 503)

    async def send(self, message, reply):
        from .portal_service import external_real_write_guard
        if not external_real_write_guard()["real_write_allowed"]:
            raise AssistantError("当前运行模式不允许发送真实飞书消息。", 403)
        client = await self.client()
        # Reply to the originating message; identity and conversation remain per sender.
        text = safe_text(reply.get("text") or '', limit=30000) or "本轮未取得回答，请稍后重新提问。"
        for number, start in enumerate(range(0, len(text), 5000)):
            card = {"schema": "2.0", "body": {"elements": [{"tag": "markdown", "content": text[start:start + 5000]}]}}
            body = {"msg_type": "interactive", "content": json.dumps(card, ensure_ascii=False),
                "uuid": hashlib.sha256((message["message_id"] + ":answer:" + str(number)).encode()).hexdigest()[:32]}
            url = "https://open.feishu.cn/open-apis/im/v1/messages/" + message["message_id"] + "/reply"
            response = await client.post(url, headers=await self.headers(), json=body)
            data = response.json()
            if data.get("code") != 0:
                if data["code"] in {99991663, 99991664, 99991665}: self.expires = 0
                raise AssistantError("飞书回复发送未完成，已保留回答稍后重试。", 503)

    async def close(self):
        if self.http is not None: await self.http.aclose()


class FeishuAssistant:
    def __init__(self, controller, runtime, ready):
        self.controller, self.runtime, self.ready = controller, runtime, ready
        path = getattr(runtime.state_store, "db_path", None)
        self.root = Path(path).parent if isinstance(path, (str, Path)) else None
        self.config_path = self.root / "feishu_assistant.json" if self.root else None
        self.config, self.inbox, self.messenger = {}, None, None
        self.process, self.runner, self.bot_id = None, None, ""
        self.tasks, self.sessions = {}, {}
        self.reactions = {}
        self.reaction_slots = asyncio.Semaphore(4)
        self.closing = False
        self.event = asyncio.Event()
        self.starts = []

    async def _start_worker(self):
        from upload_event_module.services.process_lifetime import register_child_process
        callback = f"http://127.0.0.1:{self.controller.bound_port or self.controller.preferred_port}/api/assistant/feishu-event"
        flags = subprocess.CREATE_NO_WINDOW | subprocess.BELOW_NORMAL_PRIORITY_CLASS if os.name == "nt" else 0
        self.starts.append(time.monotonic())
        self.process = await asyncio.to_thread(subprocess.Popen,
            [sys.executable, "-m", "lan_bitable_template_portal.feishu_assistant_worker", "--config", str(self.config_path), "--callback", callback],
            cwd=str(Path(__file__).resolve().parents[1]), creationflags=flags)
        if not register_child_process(self.process.pid):
            self.process.terminate()
            await asyncio.to_thread(self.process.wait, 5)
            raise RuntimeError("Unable to bind Feishu worker lifetime")

    async def start(self):
        if not self.config_path or not self.config_path.is_file(): return
        try:
            self.config = await asyncio.to_thread(lambda: json.loads(self.config_path.read_text(encoding="utf-8")))
            if not self.config.get("enabled"): return
            from .portal_service import external_real_write_guard
            if not external_real_write_guard()["real_write_allowed"]: return
            self.secret = unprotect_key(self.config["bridge_cipher"])
            self.inbox = await asyncio.to_thread(Inbox, self.root / "feishu_assistant.sqlite3")
            self.messenger = FeishuMessenger(self.config)
            await self._start_worker()
            self.runner = asyncio.create_task(self._run())
            print("[ClipFlow][FeishuAssistant] 正在后台建立消息长连接，不影响其他业务", flush=True)
        except Exception as exc:
            logging.warning("Feishu assistant initialization failed: %s", type(exc).__name__)

    async def receive(self, request):
        token = request.headers.get("authorization", "")
        if (not self.inbox or not request.client or request.client.host != "127.0.0.1" or request.headers.get("origin")
                or not token.isascii() or not hmac.compare_digest(token, "Bearer " + self.secret)):
            return JSONResponse({"ok": False}, status_code=403)
        body = bytearray()
        async for chunk in request.stream():
            body.extend(chunk)
            if len(body) > 128000: return JSONResponse({"ok": False}, status_code=413)
        try:
            data = json.loads(body)
            if data.get("kind") == "ready":
                if not re.fullmatch(r"ou_[A-Za-z0-9]+", str(data.get("bot_id") or "")): raise ValueError()
                self.bot_id = data["bot_id"]
                print("[ClipFlow][FeishuAssistant] 消息接收器已准备，正在连接飞书；等待消息事件", flush=True)
            else:
                message = parse_message(data, self.config["app_id"], self.bot_id)
                if message:
                    added = await asyncio.to_thread(self.inbox.add, message)
                    if added:
                        mid = message['message_id']
                        self.reactions[mid] = asyncio.create_task(self._react(message))
                        self.reactions[mid].add_done_callback(lambda _, mid=mid: self.reactions.pop(mid, None))
                    self.event.set()
            return JSONResponse({"ok": True})
        except (ValueError, TypeError, AttributeError):
            return JSONResponse({"ok": False}, status_code=400)

    def _user(self, union_id):
        if not re.fullmatch(r"on_[A-Za-z0-9]+", union_id):
            raise AssistantError("飞书未提供可核验的人员标识，无法使用你的灯塔权限，请联系管理员检查消息事件权限。", 403)
        store, auth = self.runtime.state_store, self.runtime.auth_manager
        cached = store.get_document("feishu_assistant_identity", union_id)
        user = cached
        if not user:
            with closing(sqlite3.connect(store.db_path, timeout=5)) as db:
                rows = db.execute("SELECT user_json FROM auth_sessions WHERE json_extract(user_json,'$.union_id')=?", (union_id,)).fetchall()
            known = {json.loads(row[0]).get("open_id"): json.loads(row[0]) for row in rows}
            if len(known) == 1: user = next(iter(known.values()))
        if not user:
            service = self.runtime.service
            data = service._request_payload("GET", "https://open.feishu.cn/open-apis/contact/v3/users/" + union_id,
                context="助手人员身份核对", headers=service._auth_headers(), params={"user_id_type": "union_id"})
            found = data.get("data", {}).get("user", {}) if not data.get("code") else {}
            if found.get("union_id") == union_id and found.get("open_id"): user = found
        if not user or user.get("union_id") != union_id or not re.fullmatch(r"ou_[A-Za-z0-9]+", str(user.get("open_id") or "")):
            raise AssistantError("无法可靠匹配你的灯塔账号，请先在灯塔完成一次飞书登录，再重新提问。", 403)
        if auth._open_id_explicitly_disabled(user["open_id"]): raise AssistantError("当前灯塔账号已停用。", 403)
        user = {key: user.get(key, "") for key in ("open_id", "union_id", "name", "employee_no")}
        if user != cached: store.put_document("feishu_assistant_identity", union_id, user)
        return user

    def _session(self, user):
        auth = self.runtime.auth_manager
        token = self.sessions.get(user["open_id"])
        if token and auth.get_session(token): return token
        token = secrets.token_urlsafe(32)
        now = time.time()
        session = {"user": user, "role": auth.role_for_open_id(user["open_id"]),
            "allowed_scopes": auth.scopes_for_open_id(user["open_id"]), "created_at_ts": now,
            "expires_at": now + AUTH_SESSION_TTL_SECONDS, "source": "feishu_assistant"}
        auth._state_store.put_auth_session(auth._secret_hash(token), session)
        self.sessions[user["open_id"]] = token
        return token

    async def _request_context(self, message):
        user = await asyncio.to_thread(self._user, message["union_id"])
        cookie = await asyncio.to_thread(self._session, user)
        port = self.controller.bound_port or self.controller.preferred_port
        origin = f"http://127.0.0.1:{port}"
        request = Request({"type": "http", "http_version": "1.1", "method": "POST", "scheme": "http", "path": "/api/assistant/chat",
            "query_string": b"", "root_path": "", "server": ("127.0.0.1", port), "client": ("127.0.0.1", 0),
            "headers": [(b"host", f"127.0.0.1:{port}".encode()), (b"origin", origin.encode()), (b"cookie", f"{AUTH_COOKIE_NAME}={cookie}".encode())],
            "state": {"lighthouse_channel": "feishu:" + message["chat_id"], "feishu_question": message['question']}})
        from .lighthouse_bridge import actor_for
        actor = await actor_for(self.controller, self.runtime, request)
        return request, actor

    async def _call(self, request, actor, path, body=None, plan_id=None):
        current, authority = await self.ready()
        context = authority.context(request, actor, plan_id=plan_id)
        headers = {"Authorization": "Bearer " + current.key, "x-clipflow-instance": current.instance,
            "x-clipflow-lease": current.lease, "x-clipflow-context": context}
        client = await current.http_client()
        response = await client.request("GET" if body is None else "POST",
            f"http://127.0.0.1:{current.descriptor['port']}/api/assistant/" + path, headers=headers, json=body, timeout=360)
        result = response.json()
        if not result.get("ok"): raise AssistantError(result.get("error") or "助手暂时不可用，原消息已保留。", response.status_code)
        if plan_id is None:
            authority.contexts.pop(context, None)
        return result.get("data") or {}

    async def answer(self, message):
        if message.get("problem"):
            return {"text": "请发送不超过12000字的文字或富文本问题。身份证号、住址不提供查询；附件请在灯塔网页助手中上传。"}
        request, actor = await self._request_context(message)
        from .assistant_read_consent import ReadConsent
        consent = ReadConsent(self.runtime.state_store)
        if message['question'].strip() == '取消查询':
            await asyncio.to_thread(consent.cancel, actor)
            return {'text': '本会话的查询授权已取消，未开始新的数据读取。'}
        confirmed = re.fullmatch(r'确认查询\s+([a-f0-9]{8})', message['question'].strip())
        if confirmed:
            original = await asyncio.to_thread(consent.approve, actor, confirmed[1])
            message = {**message, 'question': original}
            request.state.feishu_question = original
        conversation = await self._call(request, actor, "conversation")
        question = message["question"].strip()
        plans = {t['plan'].get('id', str(i)): t['plan'] for i, t in enumerate(conversation.get('turns', [])) if t.get('plan')}
        pending = [p for p in plans.values() if p.get('status') in {'awaiting_confirmation', 'awaiting_second_confirmation'}]
        if question in {"确认执行", "再次确认执行"}:
            if len(pending) != 1: return {"text": "当前没有唯一可确认的操作，请先说明需要办理的业务并核对预览。"}
            plan = pending[0]
            if len(json.dumps(plan.get("operations") or [], ensure_ascii=False)) > 12000:
                return {"text": "这项操作的完整内容较长，飞书内未展示全部字段；请到灯塔原业务页面核对提交，当前未执行。"}
            stage = "execute" if plan["status"] == "awaiting_second_confirmation" else "review"
            if stage == "execute" and question != "再次确认执行":
                return {"text": "此操作需要再次确认，请核对后发送“再次确认执行”。"}
            result = await self._call(request, actor, f"plans/{plan['id']}/confirm", {"version": plan["version"], "stage": stage}, plan_id=plan["id"])
            return {"text": self.plan_text(result)}
        if question == "取消操作":
            if len(pending) != 1: return {"text": "当前没有唯一待确认的操作。"}
            await self._call(request, actor, f"plans/{pending[0]['id']}/cancel", {})
            return {"text": "已取消该待确认操作，未提交新的业务。"}
        operation = "feishu_" + hashlib.sha256(message["message_id"].encode()).hexdigest()[:40]
        data = await self._call(request, actor, "chat", {"conversation_id": conversation["conversation_id"], "operation_id": operation,
            "attempt_id": operation, "question": question, "file_ids": []})
        turn = next((turn for turn in data.get("turns", []) if turn.get("operation_id") == operation), None)
        if not turn: raise AssistantError("助手没有返回本条消息的处理结果，请稍后重试。", 503)
        text = turn.get("answer") or turn.get("error") or "本轮没有生成有效回答，请重新提问。"
        pending_reads = await asyncio.to_thread(consent.pending, actor, question)
        if pending_reads:
            destination = '原群内公开回复' if message['chat_type'] == 'group' else '本私聊回复'
            text = '\n\n'.join(f"需要你确认：{item['summary']}。仅查询你的权限范围，结果将在{destination}。\n"
                f"回复“确认查询 {item['code']}”继续（5分钟内有效），或回复“取消查询”。未确认则不读取。" for item in pending_reads)
        if turn.get("plan"): text += "\n\n" + self.plan_text(turn["plan"])
        return {"text": safe_text(text, limit=30000)}

    @staticmethod
    def plan_text(plan):
        status = plan.get("status")
        title = str(plan.get("title") or "业务操作")
        lines = ["**" + title + "**", str(plan.get("explanation") or "")]
        operations = json.dumps(plan.get("operations") or [], ensure_ascii=False, indent=2)
        if status in {"awaiting_confirmation", "awaiting_second_confirmation"}:
            if len(json.dumps(plan.get("operations") or [], ensure_ascii=False)) > 12000:
                return "\n".join(lines + ["操作内容较长，请到灯塔原业务页面核对全部字段后提交。飞书内不执行这项操作。"])
            lines.append("操作及字段预览：\n```json\n" + operations + "\n```")
        if status == "awaiting_confirmation": lines.append("尚未执行。核对以上内容后发送“确认执行”，或发送“取消操作”。")
        elif status == "awaiting_second_confirmation": lines.append("此操作风险较高，尚未执行；核对后发送“再次确认执行”。")
        elif status in {"running", "submitted"}: lines.append("已提交原业务流程，后台正在处理；请查询原任务结果，不要重复新增。")
        elif status == "completed": lines.append("操作已完成。")
        elif status == "needs_input":
            labels = [str(field.get("label") or field.get("path") or "填写项") for field in plan.get("fields", [])]
            lines.append("待补充：" + "、".join(labels[:20]) + "。请补充信息；涉及选人、签名、附件等表单，请在灯塔原业务页面填写。当前未执行业务。")
        else: lines.append(str(plan.get("error") or "请在灯塔中核对原操作状态。"))
        return "\n".join(filter(None, lines))

    async def _react(self, message):
        async def send():
            async with self.reaction_slots:
                await self.messenger.react(message)
        try:
            if not await asyncio.to_thread(self.inbox.claim_reaction, message['message_id']):
                return
            await asyncio.wait_for(send(), timeout=5)
        except Exception as exc:
            logging.warning('Feishu typing reaction unavailable; answer continues: %s',
                            str(exc) if isinstance(exc, AssistantError) else type(exc).__name__)

    async def _drain(self, owner):
        while not self.closing:
            message = await asyncio.to_thread(self.inbox.take, owner)
            if not message: return
            reply = message.get("reply")
            if reply is None:
                pending_reaction = self.reactions.get(message['message_id'])
                if pending_reaction:
                    await asyncio.shield(pending_reaction)
                else:
                    await self._react(message)
                try: reply = await self.answer(message)
                except Exception as exc:
                    reply = {"text": safe_text(str(exc)) if isinstance(exc, AssistantError) else "助手暂时无法处理本条消息，请稍后重新提问；未自动重发业务。"}
                await asyncio.to_thread(self.inbox.set, message["message_id"], "reply", reply)
            try:
                await self.messenger.send(message, reply)
                await asyncio.to_thread(self.inbox.set, message["message_id"], "done")
            except Exception as exc:
                logging.warning("Feishu assistant reply deferred: %s", type(exc).__name__)
                await asyncio.to_thread(self.inbox.set, message["message_id"], "reply", delay=60)

    async def _run(self):
        while not self.closing:
            self.event.clear()
            self.starts = [stamp for stamp in self.starts if time.monotonic() - stamp < 600]
            if self.process and self.process.poll() is not None and len(self.starts) < 3 and (
                    not self.starts or time.monotonic() - self.starts[-1] > 120):
                try: await self._start_worker()
                except Exception as exc: logging.warning("Feishu connection restart deferred: %s", type(exc).__name__)
            for owner in await asyncio.to_thread(self.inbox.owners):
                if owner not in self.tasks or self.tasks[owner].done():
                    self.tasks[owner] = asyncio.create_task(self._drain(owner))
            for owner in list(self.tasks):
                if self.tasks[owner].done():
                    task = self.tasks.pop(owner)
                    if not task.cancelled() and task.exception(): logging.warning("Feishu assistant worker interrupted")
            try: await asyncio.wait_for(self.event.wait(), 10)
            except asyncio.TimeoutError: pass

    async def close(self):
        self.closing = True
        if self.process and self.process.poll() is None:
            self.process.terminate()
            await asyncio.to_thread(self.process.wait, 5)
        pending = [task for task in [self.runner, *self.tasks.values(), *self.reactions.values()] if task and not task.done()]
        for task in pending: task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        if self.messenger: await self.messenger.close()


def install_feishu_assistant(app, controller, runtime, ready):
    service = FeishuAssistant(controller, runtime, ready)

    async def incoming(request: Request):
        return await service.receive(request)

    app.add_api_route("/api/assistant/feishu-event", incoming, methods=["POST"])
    return service
