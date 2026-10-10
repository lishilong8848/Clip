"""Feishu message transport for the existing assistant and its existing permissions."""
from __future__ import annotations

import asyncio
import datetime as _dt
import hashlib
import hmac
import json
import logging
import math
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
    from .feishu_assistant_files import parse_resources
    try:
        resources = parse_resources(message, content)
    except AssistantError:
        resources, problem = [], 'oversize'
    if resources and not question:
        question = '请读取附件，结合当前会话继续处理。'
    if not question:
        question, problem = "[不支持的消息格式]", "unsupported"
    if len(question) > 12000:
        question, problem = "[消息超过12000字]", "oversize"
    if private_identifier(question):
        question, problem = "[敏感查询]", "private"
    return {"message_id": message["message_id"], "chat_id": message["chat_id"], "chat_type": message["chat_type"],
            "sender_id": ids["open_id"], "union_id": str(ids.get("union_id") or ""), "problem": problem,
            "question": safe_text(question, limit=12000) or "[内容已隐去]",
            'content': {**({key: content[key] for key in ('file_key', 'file_name') if key in content} if any(r['kind'] == 'file' for r in resources) else {}),
                        'content': [[{'tag': 'img', 'image_key': r['key']} for r in resources if r['kind'] == 'image']]}}


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
        self.picked_plan = {}
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
            print("[ClipFlow][FeishuAssistant] 飞书智能体正在后台建立消息长连接，不影响其他业务", flush=True)
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

    async def _call(self, request, actor, path, body=None, plan_id=None, *, method=None, params=None):
        current, authority = await self.ready()
        context = authority.context(request, actor, plan_id=plan_id)
        headers = {"Authorization": "Bearer " + current.key, "x-clipflow-instance": current.instance,
            "x-clipflow-lease": current.lease, "x-clipflow-context": context}
        http_method = method or ("GET" if body is None else "POST")
        try:
            # Creating the transport is part of the guarded call: if it fails we
            # must still release the non-execution context created above.
            client = await current.http_client()
            response = await client.request(http_method,
                f"http://127.0.0.1:{current.descriptor['port']}/api/assistant/" + path, headers=headers,
                json=body if http_method in ("POST", "PATCH", "PUT", "DELETE") else None,
                params=params, timeout=360)
            result = response.json()
            if not result.get("ok"):
                raise AssistantError(result.get("error") or "助手暂时不可用，原消息已保留。", response.status_code)
            return result.get("data") or {}
        finally:
            # Non-execution contexts are always released, even on HTTP error, so no
            # per-chat authorization is leaked. Confirmation contexts (confirm/retry)
            # are retained because the background original task still needs them.
            if plan_id is None:
                authority.contexts.pop(context, None)

    async def answer(self, message):
        if message.get("problem"):
            return {"text": "请发送不超过12000字的文字、图片或文件，每次最多10个附件。身份证号、住址不提供查询。"}
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
        from .feishu_assistant_files import handle_files
        file_reply = await handle_files(self, message, request, actor, conversation)
        if file_reply and 'text' in file_reply:
            return file_reply
        file_ids = file_reply.get('file_ids', []) if file_reply else []
        if file_reply and file_reply.get('question'):
            message = {**message, 'question': file_reply['question']}
            request.state.feishu_question = message['question']
        question = message["question"].strip()
        plans = {t['plan'].get('id', str(i)): t['plan'] for i, t in enumerate(conversation.get('turns', [])) if t.get('plan')}
        plan_reply = await self._feishu_plan_command(message, request, actor, plans) if not file_ids else None
        if plan_reply is not None:
            return {"text": safe_text(plan_reply, limit=30000)}
        operation = "feishu_" + hashlib.sha256(message["message_id"].encode()).hexdigest()[:40]
        data = await self._call(request, actor, "chat", {"conversation_id": conversation["conversation_id"], "operation_id": operation,
            "attempt_id": operation, "question": question, "file_ids": file_ids})
        turn = next((turn for turn in data.get("turns", []) if turn.get("operation_id") == operation), None)
        if not turn: raise AssistantError("助手没有返回本条消息的处理结果，请稍后重试。", 503)
        text = turn.get("answer") or turn.get("error") or "本轮没有生成有效回答，请重新提问。"
        pending_reads = await asyncio.to_thread(consent.pending, actor, question)
        if pending_reads:
            destination = '原群内公开回复' if message['chat_type'] == 'group' else '本私聊回复'
            text = '\n\n'.join(f"需要你确认：{item['summary']}。仅查询你的权限范围，结果将在{destination}。\n"
                f"回复“确认查询 {item['code']}”继续（5分钟内有效），或回复“取消查询”。未确认则不读取。" for item in pending_reads)
        if turn.get("plan"): text += "\n\n" + self.plan_text(turn["plan"])
        company_sources = [source for source in turn.get('sources', []) if source.get('kind') == 'company_knowledge']
        if company_sources:
            text += '\n\n公司资料来源：\n' + '\n'.join(f"[{source['number']}] {source['title']}" for source in company_sources)
        return {"text": safe_text(text, limit=30000)}

    async def _feishu_plan_command(self, message, request, actor, plans):
        """Deterministic Feishu text commands for the existing plan edit APIs.

        Only exact reserved commands alter a plan; everything else falls through
        to the normal assistant chat. Field lines are accepted only when every
        non-empty line matches 字段名：值 and each name belongs to the unique
        ``needs_input`` plan; complex (object/file/native) fields are reported as
        requiring the authorized original page, never silently written.
        """
        question = (message["question"] or "").strip()
        if not plans:
            # With no pending plans there is nothing to edit. Ordinary prose that
            # contains a colon must fall through to the normal chat and never be
            # mistaken for a field write.
            return None
        key = (actor.get("id") or message["sender_id"], message["chat_id"])
        select = re.fullmatch(r"选择计划\s*(\d+)", question)
        if select:
            all_ids = list(plans)
            index = int(select.group(1)) - 1
            if not (0 <= index < len(all_ids)):
                return "计划编号无效，请重新选择。"
            plan = plans[all_ids[index]]
            self.picked_plan[key] = plan["id"]
            if len(self.picked_plan) > 2000:
                self.picked_plan.pop(next(iter(self.picked_plan)), None)
            hint = _plan_state_hint(plan)
            return ("已选择：**" + safe_text(str(plan.get("title") or "业务操作"), limit=120)
                    + f"**（状态：{plan.get('status')}，版本：{plan.get('version')}）。\n" + (hint or "此操作无需文本补充/重试/确认。"))

        if question == "查看当前计划":
            plan, prompt = self._resolve_plan(message, plans, {"needs_input"}, actor)
            if plan is None:
                return prompt if prompt else self._no_pending_hint(plans)
            return self._fields_text(plan)

        option = re.fullmatch(r"查看选项\s+(.+?)(?:\s+(.+))?", question)
        if option and option.group(1).strip():
            return await self._show_options(message, request, actor, plans, option.group(1).strip(),
                                            (option.group(2) or "").strip())

        if question == "操作状态":
            plan, prompt = self._resolve_plan(message, plans, None, actor)
            if plan is None:
                return prompt if prompt else "当前没有可查看的操作计划。"
            return self._status_text(plan)

        if question == "重试操作":
            plan, prompt = self._resolve_plan(message, plans, {"failed"}, actor)
            if plan is None:
                return prompt if prompt else "当前没有可重试的操作。"
            result = await self._call(request, actor, f"plans/{plan['id']}/retry",
                                      {"version": plan["version"]}, plan_id=plan["id"])
            return self.plan_text(result)

        if question in {"确认执行", "再次确认执行"}:
            plan, prompt = self._resolve_plan(message, plans,
                                              {"awaiting_confirmation", "awaiting_second_confirmation"}, actor)
            if plan is None:
                return prompt if prompt else "当前没有唯一可确认的操作，请先说明需要办理的业务并核对预览。"
            if len(json.dumps(plan.get("operations") or [], ensure_ascii=False)) > 12000:
                return "这项操作的完整内容较长，飞书内未展示全部字段；请到灯塔原业务页面核对提交，当前未执行。"
            stage = "execute" if plan["status"] == "awaiting_second_confirmation" else "review"
            if stage == "execute" and question != "再次确认执行":
                return "此操作需要再次确认，请核对后发送“再次确认执行”。"
            if stage == "review" and question != "确认执行":
                return "此操作需要先确认，请核对后发送“确认执行”。"
            result = await self._call(request, actor, f"plans/{plan['id']}/confirm",
                                      {"version": plan["version"], "stage": stage}, plan_id=plan["id"])
            return self.plan_text(result)

        if question == "取消操作":
            plan, prompt = self._resolve_plan(message, plans,
                                              {"needs_input", "awaiting_confirmation", "awaiting_second_confirmation"}, actor)
            if plan is None:
                return prompt if prompt else "当前没有唯一待补充或待确认的操作，请明确要取消哪项。已提交业务不会被撤销。"
            await self._call(request, actor, f"plans/{plan['id']}/cancel", {})
            return "已取消该待补充或待确认操作，未提交新的业务。"

        if _looks_like_field_lines(question):
            plan, prompt = self._resolve_plan(message, plans, {"needs_input"}, actor)
            if plan is not None:
                names = [m.group(1).strip() for line in (question or "").splitlines()
                         for m in [re.fullmatch(r"([^:：]{1,120})[:：].*", line.strip())] if m]
                recognized = any(_resolve_field(plan.get("fields", []), name) != (None, None) for name in names)
                if not recognized:
                    return None  # prose with a colon, not a field edit
                return await self._fill_fields(message, request, actor, plans)
            if prompt:
                return prompt  # ambiguous pending plans: ask which to edit
            return None
        return None

    def _resolve_plan(self, message, plans, statuses, actor=None):
        matches = [plan for plan in plans.values() if (statuses is None or plan.get("status") in statuses)]
        if len(matches) == 1:
            return matches[0], None
        if len(matches) > 1:
            # The selection hint is ephemeral, but ownership is always rechecked:
            # keying by the stable actor identity keeps one person's pick from
            # leaking into another user's conversation, and we only honour it in
            # this exact chat and against the current candidate plans.
            key = ((actor or {}).get("id") or message["sender_id"], message["chat_id"])
            picked_id = self.picked_plan.get(key)
            for plan in matches:
                if picked_id is not None and plan.get("id") == picked_id:
                    return plan, None
            return None, _choose_text(plans)
        return None, None

    async def _show_options(self, message, request, actor, plans, field_txt, search):
        plan, prompt = self._resolve_plan(message, plans, {"needs_input"}, actor)
        if plan is None:
            return prompt if prompt else "当前没有待补充字段的操作计划。"
        fields = plan.get("fields", [])
        field, error = _resolve_field(fields, field_txt)
        if field is None:
            return error or ("未找到字段“" + safe_text(field_txt, limit=120) + "”。可用字段：" + _available_fields_text(fields))
        inline = field.get("options") or []
        if inline and field.get("type") in {"select", "multiselect", "radio"} and not field.get("options_source"):
            return _options_text(field, _filter_options(inline, search), search)
        if not field.get("options_source"):
            return "字段“" + safe_text(field["name"], limit=120) + "”没有可读取的候选选项，请按上一条查看结果中的字段类型直接填写。"
        # The options endpoint ignores any keyword: it returns the currently
        # loaded candidate page. We therefore never claim the server searched the
        # full candidate set and filter that loaded page locally instead.
        params = {"field": field["name"]}
        result = await self._call(request, actor, f"plans/{plan['id']}/options", params=params)
        updated = next((item for item in result.get("fields", []) if item.get("name") == field["name"]), None) or field
        options = _filter_options(updated.get("options") or [], search)
        return _options_text(updated, options, search,
                             server_partial=bool(updated.get("options_has_more")))

    async def _fill_fields(self, message, request, actor, plans):
        plan, prompt = self._resolve_plan(message, plans, {"needs_input"}, actor)
        if plan is None:
            return prompt if prompt else "当前没有待补充字段的操作计划。"
        fields = plan.get("fields", [])
        entries = []
        for line in [item.strip() for item in (message["question"] or "").splitlines() if item.strip()]:
            match = re.fullmatch(r"([^:：]{1,120})[:：](.*)", line)
            name = (match.group(1).strip() if match else "")
            if not match:
                return "未识别字段“" + safe_text(name or line, limit=120) + "”。请逐行回复“字段名：值”。可用字段：" + _available_fields_text(fields)
            field, error = _resolve_field(fields, name)
            if field is None:
                return error or ("未识别字段“" + safe_text(name, limit=120) + "”。可用字段：" + _available_fields_text(fields))
            if _field_protected(field):
                return "字段“" + safe_text(name, limit=120) + "”为只读字段，不能通过飞书文本修改，请在灯塔原业务页面核对。"
            entries.append((field, match.group(2).strip()))
        if not entries:
            return None
        if len({id(field) for field, _ in entries}) != len(entries):
            return "填写的字段有重复，请每行只写一个字段。"
        unsupported = [field for field, _ in entries if not _feishu_supported_type(field)]
        if unsupported:
            return "字段 " + "、".join(_field_label(field) for field in unsupported[:20]) + \
                   " 属于复杂表单（选人/签名/附件/对象等），飞书文本无法安全填写。请在灯塔原业务页面填写提交，飞书内保持不变。"
        values = {}
        for field, raw in entries:
            try:
                values[field["name"]] = _typed_value(field, raw)
            except AssistantError as exc:
                return "字段“" + _field_label(field) + "”： " + str(exc)
        for field, raw in entries:
            if not field.get("options_source"):
                continue
            options = {str(option.get("value")) for option in (field.get("options") or [])}
            chosen = [values[field["name"]]] if field.get("type") != "multiselect" else list(values[field["name"]])
            if not options or not all(item in options for item in chosen):
                return "字段“" + _field_label(field) + "”的候选项尚未加载，请先发送“查看选项 " + field["name"] + "”，再填写。"
        result = await self._call(request, actor, "plans/" + plan["id"],
                                  {"version": plan["version"], "values": values}, method="PATCH")
        return "已按你的填写更新操作计划（未执行）。\n\n" + self.plan_text(result)

    def _no_pending_hint(self, plans):
        awaiting = any(plan.get("status") in {"awaiting_confirmation", "awaiting_second_confirmation"} for plan in plans.values())
        failed = any(plan.get("status") == "failed" for plan in plans.values())
        if awaiting:
            return "当前操作等待确认。请发送“确认执行”或“再次确认执行”，或发送“取消操作”。"
        if failed:
            return "当前操作执行失败。可发送“重试操作”重试，或发送“取消操作”。"
        return "当前没有待补充字段的操作计划。"

    @staticmethod
    def _fields_text(plan):
        lines = ["**" + safe_text(str(plan.get("title") or "业务操作"), limit=120)
                 + "**（待补充字段，版本 " + str(plan.get("version")) + "）"]
        fields = plan.get("fields", [])
        if not fields:
            lines.append("（没有需要补充的字段）")
        editable = False
        for field in fields[:40]:
            name = str(field.get("name"))
            label = safe_text(str(field.get("label") or ""), limit=60)
            value = _field_value_display(field)
            if _field_protected(field):
                lines.append(f"{name}（{label}）：{value}（只读，不可修改）")
                continue
            if not _feishu_supported_type(field):
                lines.append(f"{name}（{label}）：{value}（复杂表单，请在灯塔原业务页面填写）")
                continue
            editable = True
            line = f"{name}（{label}）：{value}"
            if field.get("options_source") and field.get("type") in {"select", "multiselect", "radio"}:
                line += "  → 查看选项 " + name
            lines.append(line)
        if len(fields) > 40:
            lines.append("（共" + str(len(fields)) + "个字段，仅展示前40个）")
        lines.append("")
        if editable:
            lines.append("请按“字段名：值”逐行回复；不填写的行保持不变。多选用“、”分隔。")
        else:
            lines.append("以上字段均不可通过飞书文本修改；请在灯塔原业务页面填写提交。")
        return "\n".join(lines)

    def _status_text(self, plan):
        status = plan.get("status")
        label = {"needs_input": "待补充字段", "awaiting_confirmation": "待确认执行",
                 "awaiting_second_confirmation": "待再次确认", "running": "处理中",
                 "submitted": "已提交，后台处理中", "completed": "已完成", "failed": "失败",
                 "cancelled": "已取消", "superseded": "已由最新提交替代"}.get(status, str(status))
        lines = [f"**{safe_text(str(plan.get('title') or '业务操作'), limit=120)}**",
                 "状态：" + label + "，版本：" + str(plan.get("version"))]
        if status == "needs_input":
            # The full field list (including read-only and complex markers) is
            # rendered by _plan_state_hint below, so no duplicate name list here.
            pass
        elif status == "failed":
            lines.append("原因：" + safe_text(str(plan.get("error") or "请查看原任务"), limit=300))
        lines.append(_plan_state_hint(plan) or "")
        return "\n".join(filter(None, lines))

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
            lines.append(FeishuAssistant._fields_text(plan))
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


def _looks_like_field_lines(question):
    lines = [line.strip() for line in (question or "").splitlines() if line.strip()]
    return bool(lines) and all(re.fullmatch(r"[^:：]{1,120}[:：].+", line) for line in lines)


def _plan_state_hint(plan):
    status = plan.get("status")
    if status == "needs_input":
        # Show the actual editable field list (read-only and complex fields are
        # marked) instead of only telling the user which command to send.
        return FeishuAssistant._fields_text(plan)
    if status in {"awaiting_confirmation", "awaiting_second_confirmation"}:
        return "核对后发送“确认执行”或“再次确认执行”，或发送“取消操作”。"
    if status == "failed":
        return "可发送“重试操作”重试，或发送“取消操作”。"
    if status == "cancelled":
        return "该操作已取消，未提交业务。"
    return ""


def _choose_text(plans):
    lines = ["当前没有唯一匹配的操作计划（共" + str(len(plans)) + "个），请在以下编号中选择："]
    for index, plan in enumerate(list(plans.values()), 1):
        lines.append(f"{index}. **{safe_text(str(plan.get('title') or '业务操作'), limit=120)}**"
                     f"（状态：{plan.get('status')}，版本：{plan.get('version')}）")
    lines.append("回复“选择计划 N”继续。")
    return "\n".join(lines)


def _feishu_supported_type(field):
    ftype = field.get("type")
    if ftype not in {"text", "textarea", "number", "integer", "float", "date", "datetime-local",
                     "time", "month", "checkbox", "boolean", "select", "radio", "multiselect"}:
        return False
    for marker in ("native_repair", "native_notice", "native_notice_sop", "native_notice_binding",
                   "native_notice_identity", "native_morning_meeting", "native_drill_create",
                   "native_water_record", "native_cabinet_text_fill", "native_cabinet_text_create",
                   "native_cabinet_edit", "native_cabinet_proof", "native_message_content",
                   "native_guard", "native_plan_detail", "native_drill", "native_plan_rules"):
        if field.get(marker):
            return False
    return True


def _typed_value(field, raw):
    ftype = field.get("type")
    if ftype in {"text", "textarea"}:
        return raw
    if ftype == "number":
        try:
            value = float(raw)
        except (ValueError, TypeError):
            raise AssistantError("请填写数字。") from None
        if not math.isfinite(value):
            raise AssistantError("数字无效。")
        return value
    if ftype in {"integer", "float"}:
        try:
            value = int(raw) if ftype == "integer" else float(raw)
        except (ValueError, TypeError):
            raise AssistantError("请填写" + ("整数。" if ftype == "integer" else "数字。")) from None
        if ftype == "float" and not math.isfinite(value):
            raise AssistantError("数字无效。")
        return value
    if ftype == "date":
        try:
            _dt.date.fromisoformat(raw)
        except (ValueError, TypeError):
            raise AssistantError("请填写日期，格式 YYYY-MM-DD。") from None
        return raw
    if ftype == "datetime-local":
        try:
            _dt.datetime.fromisoformat(raw.replace(" ", "T"))
        except (ValueError, TypeError):
            raise AssistantError("请填写日期时间，格式如 2026-10-09T10:30。") from None
        return raw.replace(" ", "T")
    if ftype == "time":
        try:
            _dt.time.fromisoformat(raw)
        except (ValueError, TypeError):
            raise AssistantError("请填写时间，格式 HH:MM:SS。") from None
        return raw
    if ftype == "month":
        try:
            _dt.date.fromisoformat(raw + "-01")
        except (ValueError, TypeError):
            raise AssistantError("请填写月份，格式 YYYY-MM。") from None
        return raw
    if ftype in {"checkbox", "boolean"}:
        low = raw.strip().lower()
        if low in {"true", "1", "是", "勾选", "checked", "对"}:
            return True
        if low in {"false", "0", "否", "取消", "unchecked", "不对"}:
            return False
        raise AssistantError("勾选项请填写 是/否。")
    if ftype in {"select", "radio"}:
        options = _options_value_label(field.get("options") or [])
        exact = [value for value, _ in options if value == raw]
        if len(exact) == 1:
            return exact[0]
        if len(exact) > 1:
            raise AssistantError("候选选项值重复，无法确定要选择哪一项。")
        label_matches = [value for value, label in options if label == raw]
        if len(label_matches) == 1:
            return label_matches[0]
        if len(label_matches) > 1:
            raise AssistantError("候选中存在多个名为“" + safe_text(raw, limit=80)
                                 + "”的选项，请填写唯一的选项值。可先发送“查看选项 " + str(field.get("name")) + "”。")
        raise AssistantError("请从候选选项中选择，可先发送“查看选项 " + str(field.get("name")) + "”。")
    if ftype == "multiselect":
        # Labels and values may themselves contain "/", so only the delimiter
        # characters we define (comma / ideographic comma / enumeration comma)
        # split a value list.
        options = _options_value_label(field.get("options") or [])
        parts = [part.strip() for part in re.split(r"[,，、]+", raw) if part.strip()]
        chosen = []
        for part in parts:
            exact = [value for value, _ in options if value == part]
            if len(exact) == 1:
                chosen.append(exact[0])
                continue
            if len(exact) > 1:
                raise AssistantError("候选选项值重复，无法确定“" + safe_text(part, limit=80) + "”对应哪一项。")
            label_matches = [value for value, label in options if label == part]
            if len(label_matches) == 1:
                chosen.append(label_matches[0])
                continue
            if len(label_matches) > 1:
                raise AssistantError("候选中存在多个名为“" + safe_text(part, limit=80)
                                     + "”的选项，请填写唯一的选项值。")
            raise AssistantError("“" + safe_text(part, limit=80) + "”不在候选项中，可用“查看选项 "
                                 + str(field.get("name")) + "”查看后选择。")
        return chosen
    raise AssistantError("该字段类型不支持文本填写，请在灯塔原业务页面填写。")


def _field_value_display(field):
    ftype = field.get("type")
    value = field.get("value")
    if value in (None, ""):
        return "（空）"
    if ftype in {"object", "array"} or (isinstance(value, (dict, list)) and ftype != "multiselect"):
        return "复杂内容，请在原页面查看/填写"
    if isinstance(value, list):
        labels = {str(option.get("value")): str(option.get("label") or "") for option in (field.get("options") or [])}
        return "、".join(labels.get(str(item), safe_text(str(item), limit=80)) for item in value) or "（空）"
    if ftype in {"checkbox", "boolean"}:
        return "是" if value else "否"
    if ftype in {"select", "radio"}:
        labels = {str(option.get("value")): str(option.get("label") or "") for option in (field.get("options") or [])}
        return labels.get(str(value)) or safe_text(str(value), limit=120)
    return safe_text(str(value), limit=120)


def _options_text(field, options, search, *, server_partial=False):
    lines = ["字段 **" + safe_text(str(field.get("name")), limit=120) + "**（"
             + safe_text(str(field.get("label") or ""), limit=80) + "）候选选项："]
    limit = 50
    shown = [option for option in (options or [])][:limit]
    has_more_server = bool(field.get("options_has_more"))
    if not shown:
        if server_partial and search:
            lines.append("已加载候选项中无匹配“" + safe_text(search, limit=80)
                         + "”，但服务端仅返回了部分候选且未按关键词过滤，完整候选项中仍可能包含匹配项。")
        else:
            lines.append("（无匹配候选项）" + ("，请更换搜索词。" if search else ""))
    for index, option in enumerate(shown, 1):
        label = safe_text(str(option.get("label") or option.get("value") or ""), limit=120)
        value = safe_text(str(option.get("value") or ""), limit=120)
        lines.append(f"{index}. {label}（{value}）")
    total = field.get("options_total")
    if len(options) > limit or has_more_server:
        lines.append(f"（仅显示前{len(shown)}条；{('共' + str(total) + '条' if total is not None else '结果可能更多')}；"
                     + "可用 查看选项 " + safe_text(str(field.get("name")), limit=120) + " [搜索词] 继续查找）")
    if search and has_more_server:
        lines.append("说明：服务端仅返回部分候选且未按关键词过滤，以上为已加载候选项中的匹配结果，可能遗漏更多匹配项。")
    lines.append("单选写“字段名：选项”，多选用“字段名：选项1、选项2”分隔。")
    return "\n".join(lines)


def _resolve_field(fields, raw_name):
    """Resolve a user-supplied name to exactly one field.

    Prefers an exact ``name`` match, then a unique displayed ``label``. Duplicate
    names/labels are ambiguous and must be rejected instead of silently picking
    the first one.
    """
    exact = [field for field in (fields or []) if str(field.get("name")) == raw_name]
    if len(exact) == 1:
        return exact[0], None
    if len(exact) > 1:
        return None, "字段名“" + safe_text(raw_name, limit=120) + "”重复出现，无法确定要修改哪一项。"
    labels = [field for field in (fields or []) if str(field.get("label") or "") == raw_name]
    if len(labels) == 1:
        return labels[0], None
    if len(labels) > 1:
        return None, "标签“" + safe_text(raw_name, limit=120) + "”对应多个字段，无法确定要修改哪一项，请使用字段名（路径）。"
    return None, None


def _field_protected(field):
    return bool(field.get("readonly") or field.get("read_only") or field.get("disabled"))


def _field_label(field):
    return str(field.get("label") or field.get("name") or "字段")


def _field_candidate_label(field):
    label = str(field.get("label") or "").strip()
    name = str(field.get("name") or "").strip()
    if label and label != name:
        return f"{label}（{name}）"
    return label or name or "（无名称字段）"


def _available_fields_text(fields):
    labels = [_field_candidate_label(field) for field in fields or []]
    return "、".join(labels)[:500] or "（暂无）"


def _options_value_label(options):
    return [(str(option.get("value") if option is not None else ""), str(option.get("label") or ""))
            for option in options or []]


def _filter_options(options, term):
    if not term:
        return list(options or [])
    lowered = term.casefold()
    return [option for option in (options or [])
            if lowered in str(option.get("label") or "").casefold()
            or lowered in str(option.get("value") or "").casefold()]


def install_feishu_assistant(app, controller, runtime, ready):
    service = FeishuAssistant(controller, runtime, ready)

    async def incoming(request: Request):
        return await service.receive(request)

    app.add_api_route("/api/assistant/feishu-event", incoming, methods=["POST"])
    return service
