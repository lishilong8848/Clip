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
from . import feishu_notice_forms as notice_forms
from .portal_auth import AUTH_COOKIE_NAME, AUTH_SESSION_TTL_SECONDS


# One pending plan-status result poll per plan (deduplicated per person+plan).
# Incremental delays grow up to a 15s cap; after at least ~15 minutes of polling
# a clear "still processing" notice is sent once and polling stops (audit #6).
_PLAN_STATUS_DELAYS = (3, 5, 8, 10, 12, 15)
_PLAN_STATUS_MAX_ATTEMPTS = 64  # 64 polls, most at the 15s cap ≈ 16 minutes.
_PLAN_STATUS_TERMINAL = frozenset({"completed", "failed", "cancelled", "superseded"})
_PLAN_STATUS_FEEDBACK = frozenset({"needs_input", "awaiting_confirmation", "awaiting_second_confirmation"})


def _reply_text(value):
    text = safe_text(value, limit=120000).strip()
    if text:
        return text
    return safe_text("本轮回复包含受保护内容，已停止展示。请调整问题后重试。" if str(value or '').strip()
                     else "本轮暂未生成回答，请稍后重试。")


def parse_message(payload, app_id, bot_id, *, inbox=None):
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
    root, thread = str(message.get('root_id') or ''), str(message.get('thread_id') or '')
    parent = str(message.get('parent_id') or '')
    if any(value and not re.fullmatch(pattern, value) for value, pattern in
           ((root, r'om_[A-Za-z0-9]+'), (thread, r'omt_[A-Za-z0-9]+'), (parent, r'om_[A-Za-z0-9]+'))):
        return None
    in_topic = bool(inbox and (root or thread) and inbox.has_thread(message['chat_id'], root, thread))
    if message["chat_type"] == "group" and not in_topic and not any((m.get("id") or {}).get("open_id") == bot_id for m in mentions):
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
            **({'root_id': root} if root else {}), **({'thread_id': thread} if thread else {}),
             **({'parent_id': parent} if parent else {}),
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
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("CREATE TABLE IF NOT EXISTS messages (id TEXT PRIMARY KEY, owner TEXT NOT NULL, payload TEXT NOT NULL, state TEXT NOT NULL, reply TEXT, retry_at REAL NOT NULL DEFAULT 0)")
            db.execute("CREATE INDEX IF NOT EXISTS message_owner_state ON messages(state,retry_at,owner)")
            db.execute("CREATE TABLE IF NOT EXISTS reply_threads (chat_id TEXT NOT NULL, root_id TEXT NOT NULL, thread_id TEXT NOT NULL, chat_type TEXT NOT NULL, ticket TEXT NOT NULL DEFAULT '', PRIMARY KEY(chat_id,thread_id))")
            db.execute("CREATE INDEX IF NOT EXISTS reply_thread_root ON reply_threads(chat_id,root_id)")
            db.execute("CREATE TABLE IF NOT EXISTS reply_thread_messages (message_id TEXT PRIMARY KEY, chat_id TEXT NOT NULL, thread_id TEXT NOT NULL)")
            for name in ('ticket', 'plan_id'):
                if name not in {row[1] for row in db.execute('PRAGMA table_info(reply_thread_messages)')}:
                    db.execute("ALTER TABLE reply_thread_messages ADD COLUMN " + name + " TEXT NOT NULL DEFAULT ''")
            columns = {row[1] for row in db.execute('PRAGMA table_info(messages)')}
            for name, declaration in [('created_at', 'REAL NOT NULL DEFAULT 0'), ('updated_at', 'REAL NOT NULL DEFAULT 0'), ('retries', 'INTEGER NOT NULL DEFAULT 0')]:
                if name not in columns:
                    db.execute('ALTER TABLE messages ADD COLUMN ' + name + ' ' + declaration)
            if 'created_at' not in columns:
                db.execute('UPDATE messages SET created_at=?,updated_at=? WHERE created_at=0', (time.time(), time.time()))
            db.execute("CREATE TABLE IF NOT EXISTS topic_roots (chat_id TEXT,root_id TEXT,chat_type TEXT,updated_at REAL,PRIMARY KEY(chat_id,root_id))")
            db.execute("CREATE TABLE IF NOT EXISTS topic_plans (chat_id TEXT,root_id TEXT,plan_id TEXT,ticket TEXT,card_id TEXT,updated_at REAL,PRIMARY KEY(chat_id,root_id,plan_id))")
            db.execute("CREATE TABLE IF NOT EXISTS topic_posts (message_id TEXT PRIMARY KEY,chat_id TEXT,root_id TEXT,question TEXT,answer TEXT,access TEXT,created_at REAL)")
            db.execute('CREATE INDEX IF NOT EXISTS topic_posts_recent ON topic_posts(chat_id,root_id,created_at)')
            db.execute('CREATE INDEX IF NOT EXISTS topic_posts_expiry ON topic_posts(created_at)')
            db.execute('CREATE INDEX IF NOT EXISTS topic_plans_expiry ON topic_plans(updated_at)')
            db.execute('CREATE INDEX IF NOT EXISTS messages_retention ON messages(state,updated_at)')
            db.execute("UPDATE messages SET state='pending' WHERE state='processing'")

    @contextmanager
    def connect(self):
        with closing(sqlite3.connect(self.path, timeout=5)) as db:
            with db:
                yield db

    def add(self, message):
        with self.connect() as db:
            added = db.execute("INSERT OR IGNORE INTO messages(id,owner,payload,state,created_at,updated_at) VALUES(?,?,?,'pending',?,?)",
                (message["message_id"], message["union_id"] or "sender:" + message["sender_id"], json.dumps(message, ensure_ascii=False), time.time(), time.time())).rowcount == 1
            if added and not message.get('kind'):
                db.execute('INSERT INTO topic_roots VALUES(?,?,?,?) ON CONFLICT(chat_id,root_id) DO UPDATE SET updated_at=excluded.updated_at',
                           (message['chat_id'], message.get('root_id') or message['message_id'], message['chat_type'], time.time()))
            return added

    def add_plan_status(self, payload, delay):
        """Insert (or refresh) one delayed, deduplicated plan-status poll event."""
        payload = dict(payload)
        payload["kind"] = "plan_status"
        owner = str(payload.get("owner") or "") or "plan_status"
        with self.connect() as db:
            db.execute(
                "INSERT INTO messages(id,owner,payload,state,retry_at,created_at,updated_at) VALUES(?,?,?,?,?,?,?) "
                "ON CONFLICT(id) DO UPDATE SET payload=excluded.payload,state='pending',retry_at=excluded.retry_at",
                (payload["message_id"], owner, json.dumps(payload, ensure_ascii=False),
                  "pending", time.time() + delay, time.time(), time.time()))

    def due_plan_statuses(self, now=None):
        with self.connect() as db:
            rows = db.execute(
                "SELECT id,payload FROM messages WHERE state IN ('pending','reply') AND retry_at<=? "
                "AND COALESCE(json_extract(payload,'$.kind'),'')='plan_status' "
                "ORDER BY retry_at LIMIT 50", (now if now is not None else time.time(),)).fetchall()
            return [json.loads(r[1]) for r in rows]

    def reschedule_plan_status(self, identity, delay, payload):
        payload = dict(payload)
        payload["kind"] = "plan_status"
        with self.connect() as db:
            db.execute("UPDATE messages SET state='pending',retry_at=?,payload=? WHERE id=?",
                       (time.time() + delay, json.dumps(payload, ensure_ascii=False), identity))

    def owners(self):
        with self.connect() as db:
            return [r[0] for r in db.execute(
                "SELECT DISTINCT owner FROM messages WHERE state IN ('pending','reply') AND retry_at<=? "
                "AND COALESCE(json_extract(payload,'$.kind'),'')<>'plan_status'", (time.time(),))]

    def take(self, owner):
        with self.connect() as db:
            row = db.execute("SELECT id,payload,state,reply FROM messages WHERE owner=? AND state IN ('pending','reply') "
                             "AND retry_at<=? AND COALESCE(json_extract(payload,'$.kind'),'')<>'plan_status' "
                             "ORDER BY rowid LIMIT 1", (owner, time.time())).fetchone()
            if not row: return None
            db.execute("UPDATE messages SET state='processing' WHERE id=?", (row[0],))
            return {**json.loads(row[1]), "reply": json.loads(row[3]) if row[3] else None}

    def set(self, identity, state, reply=None, delay=0):
        with self.connect() as db:
            db.execute("UPDATE messages SET state=?,reply=COALESCE(?,reply),retry_at=?,updated_at=? WHERE id=?",
                (state, json.dumps(reply, ensure_ascii=False) if reply is not None else None, time.time() + delay, time.time(), identity))
            route = (reply or {}).get('thread_route') or {}
            if all(re.fullmatch(pattern, str(route.get(key) or '')) for key, pattern in
                   (('chat_id', r'oc_[A-Za-z0-9]+'), ('root_id', r'om_[A-Za-z0-9]+'), ('thread_id', r'omt_[A-Za-z0-9]+'))):
                db.execute("INSERT INTO reply_threads(chat_id,root_id,thread_id,chat_type,ticket) VALUES(?,?,?,?,?) "
                           "ON CONFLICT(chat_id,thread_id) DO UPDATE SET ticket=CASE WHEN excluded.ticket<>'' THEN excluded.ticket ELSE reply_threads.ticket END",
                           (route['chat_id'], route['root_id'], route['thread_id'], route.get('chat_type', ''), reply.get('ticket', '')))
                for mid in reply.get('thread_messages', []):
                    if re.fullmatch(r'om_[A-Za-z0-9]+', str(mid)):
                        db.execute('INSERT OR IGNORE INTO reply_thread_messages(message_id,chat_id,thread_id,ticket,plan_id) VALUES(?,?,?,?,?)',
                                   (mid, route['chat_id'], route['thread_id'], reply.get('ticket', ''), reply.get('topic_plan_id', '')))
                plan_id = reply.get('topic_plan_id')
                if plan_id and reply.get('ticket'):
                    db.execute('INSERT INTO topic_plans VALUES(?,?,?,?,?,?) ON CONFLICT(chat_id,root_id,plan_id) DO UPDATE SET ticket=excluded.ticket,card_id=excluded.card_id,updated_at=excluded.updated_at',
                               (route['chat_id'], route['root_id'], plan_id, reply['ticket'], reply.get('card_message_id', ''), time.time()))

    def thread_for(self, message):
        with self.connect() as db:
            db.row_factory = sqlite3.Row
            if message.get('thread_id'):
                row = db.execute('SELECT * FROM reply_threads WHERE chat_id=? AND thread_id=?',
                                 (message['chat_id'], message['thread_id'])).fetchone()
            elif message.get('root_id'):
                row = db.execute('SELECT * FROM reply_threads WHERE chat_id=? AND root_id=?',
                                 (message['chat_id'], message['root_id'])).fetchone()
            else:
                row = db.execute('SELECT t.* FROM reply_threads t JOIN reply_thread_messages m '
                                 'ON t.chat_id=m.chat_id AND t.thread_id=m.thread_id WHERE m.chat_id=? AND m.message_id=?',
                                 (message['chat_id'], message.get('reply_to_message_id', ''))).fetchone()
            root = message.get('root_id') or (message.get('message_id') if message.get('chat_type') == 'group' else '')
            if not row and root:
                row = db.execute("SELECT chat_id,root_id,chat_type,'' AS ticket,? AS thread_id FROM topic_roots WHERE chat_id=? AND root_id=?",
                                 (message.get('thread_id', ''), message['chat_id'], root)).fetchone()
            return dict(row) if row else None

    def has_thread(self, chat_id, root_id, thread_id):
        if not root_id and not thread_id:
            return False
        with self.connect() as db:
            if thread_id:
                row = db.execute('SELECT root_id FROM reply_threads WHERE chat_id=? AND thread_id=?',
                                 (chat_id, thread_id)).fetchone()
                if row:
                    return bool(not root_id or row[0] == root_id)
                if root_id and db.execute('SELECT 1 FROM reply_threads WHERE chat_id=? AND root_id=?', (chat_id, root_id)).fetchone():
                    return False
            return bool(root_id and (db.execute('SELECT 1 FROM reply_threads WHERE chat_id=? AND root_id=? LIMIT 1',
                              (chat_id, root_id)).fetchone() or db.execute('SELECT 1 FROM topic_roots WHERE chat_id=? AND root_id=?',
                              (chat_id, root_id)).fetchone()))

    def topic_plans(self, message):
        topic = self.thread_for(message)
        if not topic:
            return []
        with self.connect() as db:
            db.row_factory = sqlite3.Row
            return [dict(row) for row in db.execute('SELECT * FROM topic_plans WHERE chat_id=? AND root_id=? ORDER BY plan_id',
                                                   (topic['chat_id'], topic['root_id']))]

    def referenced_plan(self, message):
        reference = message.get('parent_id') or message.get('reply_to_message_id')
        with self.connect() as db:
            db.row_factory = sqlite3.Row
            row = db.execute('SELECT ticket,plan_id FROM reply_thread_messages WHERE chat_id=? AND message_id=?',
                             (message['chat_id'], reference)).fetchone()
            return dict(row) if row else {}

    def retire_topic_plan(self, message, plan_id):
        with self.connect() as db:
            row = db.execute('SELECT ticket FROM topic_plans WHERE chat_id=? AND root_id=? AND plan_id=?',
                             (message['chat_id'], message.get('root_id'), plan_id)).fetchone()
            db.execute('DELETE FROM topic_plans WHERE chat_id=? AND root_id=? AND plan_id=?',
                       (message['chat_id'], message.get('root_id'), plan_id))
            if row:
                db.execute("UPDATE reply_threads SET ticket='' WHERE chat_id=? AND root_id=? AND ticket=?",
                           (message['chat_id'], message.get('root_id'), row[0]))

    def remember_public_reply(self, message, reply):
        route = reply.get('thread_route') or {}
        if not route.get('root_id') or reply.get('thread_fallback') or not reply.get('delivery_access'):
            return
        with self.connect() as db:
            db.execute('INSERT OR IGNORE INTO topic_posts VALUES(?,?,?,?,?,?,?)',
                       (message['message_id'], route['chat_id'], route['root_id'], safe_text(message.get('question'), limit=1500),
                        safe_text(reply.get('text'), limit=2500), json.dumps(reply['delivery_access']), time.time()))

    def topic_context(self, message, actor):
        topic = self.thread_for(message)
        if not topic:
            return ''
        with self.connect() as db:
            rows = db.execute('SELECT question,answer,access FROM topic_posts WHERE chat_id=? AND root_id=? ORDER BY created_at DESC LIMIT 4',
                              (topic['chat_id'], topic['root_id'])).fetchall()
        parts = []
        for question, answer, raw in reversed(rows):
            access = json.loads(raw)
            if set(access.get('scopes', [])) <= set(actor.get('scopes', [])) and set(access.get('modules', [])) <= set(actor.get('modules', [])):
                parts.append('话题问题：' + question + '\n已公开回复（历史）：' + answer)
        return '\n\n'.join(parts)[-5000:]

    def defer_delivery(self, identity, *, permanent=False, reason=''):
        with self.connect() as db:
            row = db.execute('SELECT retries FROM messages WHERE id=?', (identity,)).fetchone()
            attempts = (row[0] if row else 0) + 1
            stopped = permanent or attempts >= 8
            db.execute("UPDATE messages SET retries=?,state=?,retry_at=?,updated_at=?,reply=json_set(COALESCE(reply,'{}'),'$.delivery_error',?) WHERE id=?",
                       (attempts, 'stopped' if stopped else 'reply', time.time() + min(300, 5 * 2 ** min(attempts, 6)), time.time(), safe_text(reason, limit=240), identity))
            return stopped

    def cleanup(self, now=None):
        now = time.time() if now is None else now
        with self.connect() as db:
            # Keep message IDs as tombstones so delayed duplicate events stay deduplicated.
            db.execute("UPDATE messages SET state='archived',owner='',payload='{}',reply=NULL WHERE id IN (SELECT id FROM messages WHERE state IN ('done','stopped') AND updated_at<? LIMIT 1000)", (now - 30 * 86400,))
            db.execute('DELETE FROM topic_posts WHERE rowid IN (SELECT rowid FROM topic_posts WHERE created_at<? LIMIT 1000)', (now - 30 * 86400,))
            db.execute("UPDATE reply_threads SET ticket='' WHERE ticket IN (SELECT ticket FROM topic_plans WHERE updated_at<? LIMIT 500)", (now - 1800,))
            db.execute('DELETE FROM topic_plans WHERE rowid IN (SELECT rowid FROM topic_plans WHERE updated_at<? LIMIT 500)', (now - 1800,))

    def reply_for(self, identity):
        """Read a previously persisted outgoing reply for the row (no consume).

        Used by the plan-status notifier so a lost-response retry resends the
        exact same card/ticket/uuid instead of minting a new one that would
        invalidate the already delivered card."""
        with self.connect() as db:
            row = db.execute("SELECT reply,state FROM messages WHERE id=?", (identity,)).fetchone()
            if not row or row[1] == 'done':
                return None
            return json.loads(row[0]) if row[0] else None

    def store_plan_status_reply(self, identity, reply):
        """Upsert an outgoing result reply onto a plan-status row before sending.

        Keeps the same card/ticket/uuid available for a lost-response retry.  A
        row normally already exists (created by add_plan_status); this upserts so
        short-lived notifies are persisted even when the poll row was removed."""
        with self.connect() as db:
            db.execute(
                "INSERT INTO messages(id,owner,payload,state,reply,retry_at,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?) "
                "ON CONFLICT(id) DO UPDATE SET state='reply',reply=excluded.reply,retry_at=0",
                (identity, "plan_status", "{}", "reply",
                  json.dumps(reply, ensure_ascii=False), 0, time.time(), time.time()))

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

    async def sender_name(self, message):
        """Read the actual sender's name, never a name supplied in chat text."""
        sender = str(message.get('sender_id') or '')
        if not re.fullmatch(r'ou_[A-Za-z0-9]+', sender):
            raise AssistantError('飞书消息缺少有效的发送人标识。', 403)
        client, headers = await self.client(), await self.headers()
        response = await client.get('https://open.feishu.cn/open-apis/contact/v3/users/' + sender,
            headers=headers, params={'user_id_type': 'open_id'})
        data = response.json()
        found = (data.get('data') or {}).get('user') or {}
        if data.get('code') == 0 and found.get('open_id') == sender and str(found.get('name') or '').strip():
            return str(found['name']).strip()
        # External group members may not belong to the application's address book.
        if message.get('chat_type') == 'group' and re.fullmatch(r'oc_[A-Za-z0-9]+', str(message.get('chat_id') or '')):
            token, seen = '', set()
            for _ in range(20):
                response = await client.get('https://open.feishu.cn/open-apis/im/v1/chats/' + message['chat_id'] + '/members',
                    headers=headers, params={'member_id_type': 'open_id', 'page_size': 100,
                                             **({'page_token': token} if token else {})})
                payload = response.json()
                if payload.get('code') != 0:
                    break
                page = payload.get('data') or {}
                matches = [item for item in page.get('items', []) if item.get('member_id_type') == 'open_id'
                           and item.get('member_id') == sender and str(item.get('name') or '').strip()]
                if len(matches) == 1:
                    return str(matches[0]['name']).strip()
                if not page.get('has_more'):
                    break
                token = page.get('page_token')
                if not token or token in seen:
                    break
                seen.add(token)
        raise AssistantError('飞书暂未提供你的人员姓名，无法关联灯塔账号。请管理员检查机器人读取人员或群成员信息的权限。',
                             403, category='feishu_sender_name_unavailable')

    async def upload_file(self, name, content):
        from .portal_service import external_real_write_guard
        if not external_real_write_guard()["real_write_allowed"]:
            raise AssistantError("当前运行模式不允许发送真实飞书文件。", 403)
        if not content or len(content) > 20 * 1024 * 1024:
            raise AssistantError('发送文件须非空且不超过20MiB。', 413)
        client = await self.client()
        response = await client.post('https://open.feishu.cn/open-apis/im/v1/files', headers=await self.headers(),
            data={'file_type': 'stream', 'file_name': name}, files={'file': (name, content, 'application/octet-stream')}, timeout=90)
        data = response.json()
        key = (data.get('data') or {}).get('file_key')
        if data.get('code') != 0 or not key:
            from upload_event_module.services.http_client import classify_feishu_error
            if classify_feishu_error(getattr(response, 'status_code', 0), data.get('code')) == 'permission':
                raise AssistantError('飞书应用缺少文件上传权限，请联系管理员配置；未发送附件，原内容已保留。',
                                     403, category='feishu_file_upload_permission')
            if data.get('code') in {99991663, 99991664, 99991665}: self.expires = 0
            raise AssistantError('飞书文件上传未完成，将保留文件重试。', 503)
        return key

    async def send(self, message, reply, *, checkpoint=None, authorize=None):
        from .portal_service import external_real_write_guard
        if not external_real_write_guard()["real_write_allowed"]:
            raise AssistantError("当前运行模式不允许发送真实飞书消息。", 403)
        client = await self.client()
        # Threads affect delivery only; each participant keeps their own account context.
        text = _reply_text(reply.get("text"))
        if len(text) > 3500 and not reply.get('card'):
            if not reply.get('long_text_file_key'):
                reply['long_text_file_key'] = await self.upload_file('完整回复.md', text.encode('utf-8'))
                if checkpoint: await checkpoint()
            text = '完整回复已整理为Markdown文件，请查看附件。涉及业务操作时，请先核对文件中的操作与字段，再回复确认。'
        card = reply.get('card') or {"schema": "2.0", "body": {"elements": [{"tag": "markdown", "content": text}]}}
        if reply.get('thread_fallback'):
            card = {**card, 'body': {**card.get('body', {}), 'elements': [
                {'tag': 'markdown', 'content': '当前会话不支持话题回复，已保留原回复和表单。'},
                *card.get('body', {}).get('elements', [])]}}
        parts = [('attachment-stopped' if reply.get('attachment_stopped') else 'answer:fallback' if reply.get('card_rejected') else 'answer:0', 'interactive', card)]
        if reply.get('long_text_file_key'):
            parts.append(('long-text', 'file', {'file_key': reply['long_text_file_key']}))
        parts.extend(('file:' + file['id'], 'file', {'file_key': file['file_key']}) for file in reply.get('files', []) if file.get('file_key'))
        for part, kind, content in parts:
            if part in reply.get('sent_parts', []):
                continue
            if authorize:
                await authorize()
            body = {"msg_type": kind, "content": json.dumps(content, ensure_ascii=False),
                "reply_in_thread": not reply.get('thread_fallback', False),
                "uuid": hashlib.sha256((message["message_id"] + ":" + part).encode()).hexdigest()[:32]}
            target = message.get('root_id') or message.get('reply_to_message_id') or message['message_id']
            url = "https://open.feishu.cn/open-apis/im/v1/messages/" + target + "/reply"
            response = await client.post(url, headers=await self.headers(), json=body)
            data = response.json()
            if data.get("code") != 0:
                if data.get('code') in {230071, 230072} and not reply.get('thread_fallback'):
                    reply['thread_fallback'] = True
                    if checkpoint: await checkpoint()
                    return await self.send(message, reply, checkpoint=checkpoint, authorize=authorize)
                if reply.get('card') and data.get('code') in {230001, 230099}:
                    reply.pop('card', None)
                    reply['card_rejected'] = True
                    reply['text'] = '交互表单暂未显示，原填写仍保留，可继续回复补充。\n\n' + text
                    if checkpoint:
                        await checkpoint()
                    return await self.send(message, reply, checkpoint=checkpoint, authorize=authorize)
                if data["code"] in {99991663, 99991664, 99991665}: self.expires = 0
                if data['code'] in {230001, 230002, 230006, 230013, 230038, 230050, 230054, 230055, 230075, 230099, 230110, 230111, 232009, 99991672, 99991679}:
                    raise AssistantError('飞书回复无法送达（权限、目标或消息格式不可用），已停止自动重试。返回码：' + str(data['code']),
                                         403, category='feishu_delivery_permanent')
                raise AssistantError("飞书回复发送未完成，已保留回答稍后重试。", 503)
            delivered = data.get('data') or {}
            if delivered.get('thread_id'):
                reply['thread_route'] = {'chat_id': message['chat_id'],
                    'chat_type': message['chat_type'],
                    'root_id': delivered.get('root_id') or message.get('root_id') or target,
                    'thread_id': delivered['thread_id']}
                if delivered.get('message_id'):
                    reply.setdefault('thread_messages', []).append(delivered['message_id'])
                    if kind == 'interactive':
                        reply['card_message_id'] = delivered['message_id']
            reply.setdefault('sent_parts', []).append(part)
            if checkpoint: await checkpoint()

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
        self.topic_choices = {}
        self.plan_tasks = {}  # identity -> plan_status poll task (independent of normal drain)
        self.plan_status_slots = asyncio.Semaphore(4)
        self.reactions = {}
        self.reaction_slots = asyncio.Semaphore(4)
        self.closing = False
        self.event = asyncio.Event()
        self.starts = []
        self.next_cleanup = 0

    async def _start_worker(self):
        from upload_event_module.services.process_lifetime import register_child_process
        from openclaw_service.protocol import atomic_json
        # A saved configuration takes effect next startup, not on a worker retry.
        worker_config = self.root / "feishu_assistant_active.json"
        await asyncio.to_thread(atomic_json, worker_config, self.config)
        callback = f"http://127.0.0.1:{self.controller.bound_port or self.controller.preferred_port}/api/assistant/feishu-event"
        flags = subprocess.CREATE_NO_WINDOW | subprocess.BELOW_NORMAL_PRIORITY_CLASS if os.name == "nt" else 0
        self.starts.append(time.monotonic())
        self.process = await asyncio.to_thread(subprocess.Popen,
            [sys.executable, "-m", "lan_bitable_template_portal.feishu_assistant_worker", "--config", str(worker_config), "--callback", callback],
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
            suffix = "_" + hashlib.sha256(self.config["app_id"].encode()).hexdigest()[:24] if self.config.get("isolated_inbox") else ""
            self.inbox = await asyncio.to_thread(Inbox, self.root / ("feishu_assistant" + suffix + ".sqlite3"))
            self.messenger = FeishuMessenger(self.config)
            await self._start_worker()
            if getattr(self, "permission_cards", None):
                await self.permission_cards.recover()
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
                action_value = ((data.get('event') or {}).get('action') or {}).get('value') or {}
                if isinstance(action_value, dict) and action_value.get('kind') == 'sharing_exercise':
                    sharing = getattr(self.runtime, 'sharing_exercises', None)
                    if sharing is None:
                        return JSONResponse({'ok': False, 'toast': '分享会学练服务暂不可用，请稍后重试。'})
                    return JSONResponse(await sharing.callback(data, self.config['app_id']))
                from .feishu_assistant_questions import parse_question_event
                from .feishu_notice_cards import parse_card_event
                from .permission_review_cards import parse_event as parse_permission_event
                message = (parse_permission_event(data, self.config['app_id'])
                           or parse_question_event(data, self.config['app_id'])
                           or parse_card_event(data, self.config['app_id'])
                           or await asyncio.to_thread(parse_message, data, self.config["app_id"], self.bot_id, inbox=self.inbox))
                if not message and (data.get('header') or {}).get('event_type') == 'card.action.trigger':
                    return JSONResponse({'ok': False}, status_code=400)
                if message:
                    added = await asyncio.to_thread(self.inbox.add, message)
                    if added and not message.get('kind'):
                        mid = message['message_id']
                        self.reactions[mid] = asyncio.create_task(self._react(message))
                        self.reactions[mid].add_done_callback(lambda _, mid=mid: self.reactions.pop(mid, None))
                    self.event.set()
            return JSONResponse({"ok": True})
        except (ValueError, TypeError, AttributeError):
            return JSONResponse({"ok": False}, status_code=400)

    def _user(self, union_id):
        if not re.fullmatch(r"on_[A-Za-z0-9]+", union_id):
            raise AssistantError("飞书账号尚未关联灯塔人员。", 403, category='feishu_identity_unmapped')
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
            from .portal_service import PortalError
            try:
                data = service._request_payload("GET", "https://open.feishu.cn/open-apis/contact/v3/users/" + union_id,
                    context="助手人员身份核对", headers=service._auth_headers(), params={"user_id_type": "union_id"})
            except PortalError:
                data = {}
            found = data.get("data", {}).get("user", {}) if not data.get("code") else {}
            if found.get("union_id") == union_id and found.get("open_id"): user = found
        if not user or user.get("union_id") != union_id or not re.fullmatch(r"ou_[A-Za-z0-9]+", str(user.get("open_id") or "")):
            raise AssistantError("飞书账号尚未关联灯塔人员。", 403, category='feishu_identity_unmapped')
        if auth._open_id_explicitly_disabled(user["open_id"]): raise AssistantError("当前灯塔账号已停用。", 403)
        user = {key: user.get(key, "") for key in ("open_id", "union_id", "name", "employee_no")}
        if user != cached: store.put_document("feishu_assistant_identity", union_id, user)
        return user

    def _sender_user(self, message, *, name=None):
        store, auth = self.runtime.state_store, self.runtime.auth_manager
        sender = str(message.get('sender_id') or '')
        if not re.fullmatch(r'ou_[A-Za-z0-9]+', sender):
            raise AssistantError('飞书消息缺少有效的发送人标识。', 403)
        lookup = getattr(auth, 'directory_person', lambda _: {})
        person = lookup(sender)
        if person:
            identity = sender
        else:
            key = str(self.config.get('app_id') or '') + ':' + sender
            binding = store.get_document('feishu_assistant_sender', key) or {}
            if binding:
                identity = str(binding.get('open_id') or '')
                person = lookup(identity)
                if not person or person.get('id') != binding.get('record_id'):
                    raise AssistantError('已关联的人员记录发生变化，请管理员核对账号关联。', 403)
            elif name is not None:
                provider = getattr(auth, '_account_directory_provider', None)
                snapshot = provider() if provider else {}
                if time.time() - float(snapshot.get('loaded_at') or 0) >= 300:
                    loader = getattr(auth, '_account_directory_loader', None)
                    if loader:
                        loader()
                        snapshot = provider() if provider else {}
                if not snapshot.get('loaded_at'):
                    raise AssistantError('灯塔人员目录暂未就绪，请稍后重试。', 503)
                people = [item for item in snapshot.get('people', {}).values()
                          if not item.get('inactive') and str(item.get('name') or '').strip() == name.strip()]
                if len(people) != 1:
                    raise AssistantError('灯塔在岗目录存在重名，无法确定对应账号，请管理员核对。' if people
                                         else '灯塔在岗目录未找到与你的飞书姓名一致的人员，请管理员核对人员姓名。', 403)
                person = people[0]
                identities = person.get('login_ids') or []
                if len(identities) != 1 or not re.fullmatch(r'ou_[A-Za-z0-9]+', str(identities[0])):
                    raise AssistantError('该人员缺少唯一有效的灯塔账号，请管理员核对。', 403)
                identity = identities[0]
                if not person.get('selectable') or auth._open_id_explicitly_disabled(identity):
                    raise AssistantError('当前灯塔账号已停用或人员身份待核对。', 403)
                store.put_document('feishu_assistant_sender', key, {'open_id': identity,
                    'record_id': person['id'], 'name': person['name'], 'matched_by': 'name'})
            else:
                return None
        if not person.get('selectable') or person.get('inactive') or auth._open_id_explicitly_disabled(identity):
            raise AssistantError('当前灯塔账号已停用或人员身份待核对。', 403)
        return {'open_id': identity, 'name': person['name'], 'employee_no': person.get('employee_no', '')}

    async def _resolve_user(self, message):
        user = await asyncio.to_thread(self._sender_user, message)
        if user:
            return user
        try:
            return await asyncio.to_thread(self._user, str(message.get('union_id') or ''))
        except AssistantError as exc:
            if exc.category != 'feishu_identity_unmapped':
                raise
        name = await self.messenger.sender_name(message)
        return await asyncio.to_thread(self._sender_user, message, name=name)

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
        user = await self._resolve_user(message)
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
        shared = getattr(getattr(request, 'state', None), 'feishu_shared_plan', None)
        sharing = shared and (path == 'plans/' + shared['id'] or path.startswith('plans/' + shared['id'] + '/'))
        context = authority.context(request, actor, plan_id=plan_id, **({'shared_plan': shared} if sharing else {}))
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
            data = result.get('data') or {}
            if path.startswith('plans/') and isinstance(data, dict) and data.get('id'):
                request.state.feishu_plan = data
            return data
        finally:
            # Non-execution contexts are always released, even on HTTP error, so no
            # per-chat authorization is leaked. Confirmation contexts (confirm/retry)
            # are retained because the background original task still needs them.
            if plan_id is None:
                authority.contexts.pop(context, None)

    async def answer(self, message):
        if message.get("kind") == "permission_review":
            return await self.permission_cards.handle(message)
        if message.get("problem"):
            return {"text": "请发送不超过12000字的文字、图片或文件，每次最多10个附件。身份证号、住址不提供查询。"}
        request, actor = await self._request_context(message)
        conversation = await self._call(request, actor, "conversation")
        topic = await asyncio.to_thread(self.inbox.thread_for, message) if self.inbox else None
        if topic:
            message.update(root_id=topic['root_id'], thread_id=topic['thread_id'], chat_type=topic['chat_type'])
            if request is not None:
                request.state.feishu_topic = {'id': message['chat_id'] + ':' + topic['root_id'],
                                             'context': await asyncio.to_thread(self.inbox.topic_context, message, actor)}
        clarification_id = None
        if message.get('kind') == 'question_action':
            from .feishu_assistant_questions import read_question_reply
            question, clarification_id = await asyncio.to_thread(read_question_reply, self, message, actor, conversation)
            message = {**message, 'question': question}
            request.state.feishu_question = question
        def direct_reply(reply):
            return {**reply, 'actor_id': actor['id'], 'conversation_id': conversation.get('conversation_id'),
                    'delivery_access': self._delivery_access(actor),
                    'sync_history': True}
        if message.get('kind') == 'card_action':
            from .feishu_notice_cards import handle_card, bind_shared_card
            card_conversation = await bind_shared_card(self, message, request, actor, conversation)
            return direct_reply(await handle_card(self, message, request, actor, card_conversation))
        from .assistant_read_consent import ReadConsent
        consent = ReadConsent(self.runtime.state_store)
        if message['question'].strip() == '取消查询':
            await asyncio.to_thread(consent.cancel, actor)
            return direct_reply({'text': '本会话的查询授权已取消，未开始新的数据读取。'})
        confirmed = re.fullmatch(r'确认查询\s+([a-f0-9]{8})', message['question'].strip())
        if confirmed:
            original = await asyncio.to_thread(consent.approve, actor, confirmed[1])
            message = {**message, 'question': original}
            request.state.feishu_question = original
        from .feishu_assistant_files import handle_files
        plan_conversation = conversation
        question = message['question'].strip()
        field_lines = _looks_like_field_lines(question)
        topic_command = question in {'确认', '确认发送', '确认执行', '再次确认执行', '取消操作', '编辑操作', '查看当前计划', '操作状态', '重试操作'} or question.startswith(('补充 ', '修改 ', '查看选项 ', '选择计划', '补照片计划'))
        content = message.get('content') or {}
        topic_photos = any(row for row in content.get('content', []))
        if message.get('chat_type') == 'group' and (message.get('root_id') or message.get('thread_id')):
            # A topic must never fall back to the person's unrelated private plans.
            plan_conversation = {**conversation, 'turns': []}
            if topic_command or topic_photos or field_lines:
                plan_conversation, problem = await self._topic_plan(message, request, actor, conversation, topic)
                if problem:
                    return direct_reply({'text': problem})
                if (topic_command or field_lines and message.get('root_id') != message['message_id']) and not plan_conversation.get('turns'):
                    return direct_reply({'text': '当前话题没有可确认或编辑的业务表单，请先在本话题发起操作；未处理你在其他会话中的待办。'})
        file_reply = await handle_files(self, message, request, actor, plan_conversation)
        if file_reply and 'text' in file_reply:
            return direct_reply(file_reply)
        file_ids = file_reply.get('file_ids', []) if file_reply else []
        if file_reply and file_reply.get('question'):
            message = {**message, 'question': file_reply['question']}
            request.state.feishu_question = message['question']
        question = message["question"].strip()
        plans = {t['plan'].get('id', str(i)): t['plan'] for i, t in enumerate(plan_conversation.get('turns', [])) if t.get('plan')}
        command_message = {**message, 'question': '操作状态'} if topic and re.fullmatch(r'选择计划\s*\d+', question) else message
        plan_reply = await self._feishu_plan_command(command_message, request, actor, plans, conversation) if not file_ids and not clarification_id else None
        if plan_reply is not None:
            reply = direct_reply({"text": safe_text(plan_reply, limit=30000)})
            selected, _ = self._resolve_plan(message, plans, None, actor)
            current = getattr(getattr(request, 'state', None), 'feishu_plan', None) or selected
            if current and current.get('id'):
                await self._attach_form_card(reply, message, request, actor, conversation, current)
            return reply
        operation = "feishu_" + hashlib.sha256(message["message_id"].encode()).hexdigest()[:40]
        data = await self._call(request, actor, "chat", {"conversation_id": conversation["conversation_id"], "operation_id": operation,
            "attempt_id": operation, "question": question, "file_ids": file_ids,
            **({'clarification_operation_id': clarification_id} if clarification_id else {})})
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
        from .feishu_assistant_questions import prepare_question_card
        try:
            card = await asyncio.to_thread(prepare_question_card, self, message, actor, conversation, turn)
        except (AssistantError, OSError, sqlite3.Error, ValueError) as exc:
            logging.warning('Feishu question form unavailable: %s', type(exc).__name__)
            card = None
        reply = {"text": _reply_text(text), **({'card': card} if card else {}), 'actor_id': actor['id'],
                'conversation_id': conversation['conversation_id'],
                'delivery_access': self._delivery_access(actor),
                'files': [{'id': file['id'], 'name': file['name']} for file in ([] if pending_reads else turn.get('output_files', []))
                          if isinstance(file, dict) and re.fullmatch('[a-f0-9]{32}', str(file.get('id', '')))]}
        if turn.get('plan') and not pending_reads:
            await self._attach_form_card(reply, message, request, actor, conversation, turn['plan'])
        return reply

    async def _attach_form_card(self, reply, message, request, actor, conversation, plan):
        reply['topic_plan_id'] = plan.get('id')
        reply['topic_plan_status'] = plan.get('status')
        if plan.get('status') in {'completed', 'cancelled', 'superseded'}:
            if self.inbox and message.get('root_id') and plan.get('id'):
                await asyncio.to_thread(self.inbox.retire_topic_plan, message, plan['id'])
            return
        try:
            from .feishu_notice_cards import prepare_card
            result = await prepare_card(self, message, actor, conversation, plan)
            if result.get('card'):
                reply['card'] = result['card']
                reply['ticket'] = result['ticket']
        except Exception as exc:
            logging.warning('Feishu form unavailable, original reply retained: %s', type(exc).__name__)

    @staticmethod
    def _delivery_access(actor):
        return {'scopes': sorted(actor.get('scopes', [])), 'modules': sorted(actor.get('modules', [])),
                'is_admin': bool(actor.get('is_admin'))}

    async def _validate_delivery(self, message, reply):
        if not reply.get('actor_id'):
            return  # Transport/authentication error notices contain no model result.
        request, actor = await self._request_context(message)
        prior = reply.get('delivery_access')
        if (actor['id'] != reply['actor_id'] or not isinstance(prior, dict)
                or set(prior.get('scopes', [])) - set(actor.get('scopes', []))
                or set(prior.get('modules', [])) - set(actor.get('modules', []))
                or prior.get('is_admin') and not actor.get('is_admin')):
            raise AssistantError('原回复权限已变化，请在当前会话重新提问。', 403, category='delivery_revoked')
        current = await self._call(request, actor, 'conversation')
        if current.get('conversation_id') != reply.get('conversation_id'):
            raise AssistantError('原会话已清空，旧回复不再发送。', 410, category='delivery_revoked')

    async def _topic_plan(self, message, request, actor, conversation, topic):
        from .feishu_notice_cards import bind_shared_card
        empty = {**conversation, 'turns': []}
        if not topic:
            return empty, None
        bindings = await asyncio.to_thread(self.inbox.topic_plans, message)
        if not bindings and topic.get('ticket'):
            bindings = [{'ticket': topic['ticket']}]
        candidates = []
        for binding in bindings:
            try:
                scoped = await bind_shared_card(self, message, request, actor, conversation, ticket=binding['ticket'])
                plan = scoped['turns'][0]['plan']
                if plan.get('status') in {'completed', 'cancelled', 'superseded'}:
                    await asyncio.to_thread(self.inbox.retire_topic_plan, message, plan['id'])
                else:
                    candidates.append((binding, plan))
            except AssistantError as exc:
                if exc.status not in {403, 404, 409, 410}:
                    raise
        request.state.feishu_shared_plan = None
        message.pop('_shared_plan', None)
        if not candidates:
            return empty, None
        selected = re.fullmatch(r'选择计划\s*(\d+)', message.get('question', '').strip())
        if selected:
            index = int(selected[1]) - 1
            choices = self.topic_choices.get(_plan_key(message, actor)) or {}
            ids = choices.get('ids', [])
            if not 0 <= index < len(ids) or choices.get('expires', 0) < time.time() or ids[index] not in {p['id'] for _, p in candidates}:
                return empty, '计划选项已变化或过期，请回复“查看当前计划”重新选择。'
            self.picked_plan[_plan_key(message, actor)] = ids[index]
        reference = message.get('parent_id') or message.get('reply_to_message_id')
        referenced = await asyncio.to_thread(self.inbox.referenced_plan, message)
        if referenced.get('plan_id') and not any(p['id'] == referenced['plan_id'] and b['ticket'] == referenced['ticket'] for b, p in candidates):
            return empty, '你回复的卡片已更新或结束，请核对最新卡片后再操作，未提交业务。'
        exact = [item for item in candidates if reference and item[0].get('card_id') == reference]
        picked = self.picked_plan.get(_plan_key(message, actor))
        chosen = exact or [item for item in candidates if item[1]['id'] == picked]
        if not chosen and len(candidates) == 1:
            chosen = candidates
        if len(chosen) != 1:
            self.topic_choices[_plan_key(message, actor)] = {'ids': [p['id'] for _, p in candidates], 'expires': time.time() + 1800}
            if len(self.topic_choices) > 2000:
                self.topic_choices.pop(next(iter(self.topic_choices)), None)
            return empty, '本话题有多项待办，请先回复“选择计划 编号”，或直接回复对应卡片：\n' + '\n'.join(
                f"{i + 1}、{safe_text(item[1].get('title') or '业务操作', limit=120)}（{item[1].get('status')}）" for i, item in enumerate(candidates))
        return await bind_shared_card(self, message, request, actor, conversation, ticket=chosen[0][0]['ticket']), None

    async def _sync_reply_history(self, message, reply):
        if not reply.get('sync_history'):
            return
        request, actor = await self._request_context(message)
        if actor['id'] != reply.get('actor_id'):
            raise AssistantError('人员身份已变化，本条回复未同步。', 403)
        try:
            await self._call(request, actor, 'channel-replies', {
                'conversation_id': reply['conversation_id'],
                'operation_id': 'feishu_' + hashlib.sha256(message['message_id'].encode()).hexdigest()[:40],
                'question': message['question'], 'answer': reply['text'],
            })
        except AssistantError as exc:
            if exc.status != 409:
                raise
            # A deliberate clear must not restore an older channel reply.
        reply.pop('sync_history', None)
        await asyncio.to_thread(self.inbox.set, message['message_id'], 'reply', reply)

    async def _reply_file_context(self, message, reply):
        request, actor = await self._request_context(message)
        if actor['id'] != reply.get('actor_id'):
            raise AssistantError('登录身份已变化，附件未发送。', 403)
        conversation = await self._call(request, actor, 'conversation')
        if conversation['conversation_id'] != reply.get('conversation_id'):
            raise AssistantError('原会话已清空，附件未发送。', 410)
        return request, actor

    async def _prepare_reply_files(self, message, reply):
        if not reply.get('files'):
            return
        request, actor = await self._reply_file_context(message, reply)
        from .feishu_assistant_files import _assistant_download
        uploaded = False
        for file in reply['files']:
            metadata = await self._call(request, actor, 'files/' + file['id'], params={'metadata': '1'})
            if not file.get('file_key'):
                content = await _assistant_download(self, request, actor, file['id'])
                file['file_key'] = await self.messenger.upload_file(metadata['name'], content)
                await asyncio.to_thread(self.inbox.set, message['message_id'], 'reply', reply)
                uploaded = True
        if uploaded:
            # A slow cloud upload can outlive the original conversation or authorization.
            request, actor = await self._reply_file_context(message, reply)
            for file in reply['files']:
                await self._call(request, actor, 'files/' + file['id'], params={'metadata': '1'})

    async def _feishu_plan_command(self, message, request, actor, plans, conversation=None):
        """Deterministic Feishu text commands for the existing plan edit APIs.

        Only exact reserved commands alter a plan; everything else falls through
        to the normal assistant chat. Field lines are accepted only when every
        non-empty line matches 字段名：值 and each name belongs to the unique
        ``needs_input`` plan; complex (object/file/native) fields are reported as
        requiring the authorized original page, never silently written.
        """
        question = (message["question"] or "").strip()
        if message.get('_shared_plan') and question in {'确认', '确认发送'}:
            question = '确认执行'
        if not plans:
            # With no pending plans there is nothing to edit. Ordinary prose that
            # contains a colon must fall through to the normal chat and never be
            # mistaken for a field write.
            return None
        key = _plan_key(message, actor)
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
            # Both text and card confirmations must be tracked; schedule the
            # pending-result poll (running/submitted) and give clear photo
            # guidance when the kernel returns the plan to needs_input.
            await self.schedule_plan_status(message, result, conversation, actor.get("id"))
            if result.get("status") == "needs_input":
                photo_hint = FeishuAssistant._pending_photo_hint(result)
                base = self.plan_text(result)
                return base + (("\n\n" + photo_hint) if photo_hint else "\n\n请补充缺少的字段或发送现场照片后重新确认，填写已保留，不会重复提交。")
            return self.plan_text(result)

        if question == "取消操作":
            plan, prompt = self._resolve_plan(message, plans,
                                              {"needs_input", "awaiting_confirmation", "awaiting_second_confirmation"}, actor)
            if plan is None:
                return prompt if prompt else "当前没有唯一待补充或待确认的操作，请明确要取消哪项。已提交业务不会被撤销。"
            await self._call(request, actor, f"plans/{plan['id']}/cancel", {})
            return "已取消该待补充或待确认操作，未提交新的业务。"

        if _looks_like_field_lines(question):
            # Awaiting-confirmation plans can also be edited (returned to
            # needs_input by the Agent). Detection must consider native notice
            # and notice_sop leaves in addition to the simple top-level fields.
            plan, prompt = self._resolve_plan(message, plans,
                                              {"needs_input", "awaiting_confirmation", "awaiting_second_confirmation"}, actor)
            if plan is not None:
                names = [m.group(1).strip() for line in (question or "").splitlines()
                         for m in [re.fullmatch(r"([^:：]{1,120})[:：].*", line.strip())] if m]

                def _is_plan_field(name):
                    field, error = _resolve_field(plan.get("fields", []), name)
                    if field is not None or error:
                        return True  # a real field line, even if ambiguous
                    try:
                        if notice_forms.resolve_notice_leaf(plan.get("fields", []), name) is not None:
                            return True
                    except AssistantError:
                        return True  # clearly a notice leaf line, ambiguity surfaced on edit
                    # A name under a native field's qualified prefix (even with no
                    # resolvable child) is still a planned edit and must be denied,
                    # not silently treated as prose.
                    for f in (plan.get("fields") or []):
                        if not (notice_forms.is_native_notice(f) or notice_forms.is_native_notice_sop(f)):
                            continue
                        fname = str(f.get("name") or "")
                        if (name or "") == fname or (name or "").startswith(fname + ".") or (fname + ".") in (name or ""):
                            return True
                    return False

                if not any(_is_plan_field(name) for name in names):
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
            # Selection follows the person; current plan ownership is still rechecked.
            key = _plan_key(message, actor)
            picked_id = self.picked_plan.get(key)
            for plan in matches:
                if picked_id is not None and plan.get("id") == picked_id:
                    return plan, None
            return None, _choose_text(plans)
        return None, None

    async def _plan_to_needs_input(self, request, actor, plan):
        """Return an awaiting-confirmation plan to needs_input (idempotent).

        The real ``PortalAgent.amend`` only accepts direct PATCH values while the
        plan is ``needs_input``, and ``PortalAgent.field_options`` returns 409 for
        any other status.  Reusing ``action: \"edit\"`` (with version anti-replay and
        ownership enforcement from ``_call``) is the same path the web edit uses.
        """
        if plan.get("status") in {"awaiting_confirmation", "awaiting_second_confirmation"}:
            return await self._call(request, actor, f"plans/{plan['id']}",
                                    {"version": plan["version"], "action": "edit"}, method="PATCH")
        return plan

    async def _show_options(self, message, request, actor, plans, field_txt, search):
        plan, prompt = self._resolve_plan(message, plans,
                                              {"needs_input", "awaiting_confirmation", "awaiting_second_confirmation"}, actor)
        if plan is None:
            return prompt if prompt else "当前没有待补充字段的操作计划。"
        plan = await self._plan_to_needs_input(request, actor, plan)
        fields = plan.get("fields", [])
        field, error = _resolve_field(fields, field_txt)
        if field is None:
            # Native notice/SOP leaves are resolved as text names (child or SOP leaf).
            try:
                leaf_target = notice_forms.resolve_notice_leaf(fields, field_txt)
            except AssistantError as exc:
                return str(exc)
            if leaf_target is not None:
                parent_field, child = leaf_target
                if notice_forms.is_native_notice(parent_field):
                    return self._native_leaf_options(parent_field, child, search)
                return await self._show_notice_sop_options(request, actor, plan, parent_field, search)
            return error or ("未找到字段“" + safe_text(field_txt, limit=120) + "”。可用字段：" + _available_fields_text(fields))
        if error:
            return error
        if notice_forms.is_native_notice(field):
            return "字段“" + safe_text(field["name"], limit=120) + "”为通告整体对象；可选看其中叶子项的候选选项，例如“查看选项 " + field["name"] + ".楼栋”。"
        if notice_forms.is_native_notice_sop(field):
            return await self._show_notice_sop_options(request, actor, plan, field, search)
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

    @staticmethod
    def _native_leaf_options(parent_field, child, search):
        if notice_forms.field_protected(parent_field):
            return ("字段“" + safe_text(parent_field.get("name"), limit=120)
                    + "”为只读/隐藏字段，不能通过飞书文本查看/修改，请在灯塔原业务页面核对。")
        if child.get("hidden"):
            return "该项为内部只读/隐藏字段，不暴露给飞书文本填写，请在灯塔原业务页面核对。"
        if notice_forms.child_protected(child):
            return "该项为只读字段，不需要也无法在飞书内填写。"
        if not notice_forms.child_supported(child):
            return "该项为复杂内容，请在灯塔原业务页面填写。"
        options = child.get("options") or []
        if child.get("type") not in {"select", "radio", "multiselect"} or child.get("options_source") or not options:
            return ("该填写项没有独立候选选项，请按上一条查看结果中的字段类型直接填写（可用“查看选项 "
                    + safe_text(parent_field.get("name"), limit=120) + "”查看整体）。")
        return notice_forms.leaf_options_line(parent_field, child, search)

    async def _show_notice_sop_options(self, request, actor, plan, field, search):
        plan = await self._plan_to_needs_input(request, actor, plan)
        name = str(field.get("name") or "step0.notice_sop")
        params = {"field": name}
        if search:
            params["scope"] = search
        result = await self._call(request, actor, f"plans/{plan['id']}/options", params=params)
        updated = next((item for item in result.get("fields", []) if item.get("name") == name), None) or field
        lines = ["字段 **" + safe_text(name, limit=120) + "**（SOP/人员选择）已加载:（楼栋 "
                 + safe_text(str(updated.get("directory_scope") or search or "（未限定）"), limit=40) + "）"]
        sops = updated.get("sops") or []
        if not sops:
            lines.append("（该楼栋暂无可用 SOP）")
        for sop in sops:
            blocked = "（不可用：" + safe_text(str(sop.get("blocked_reason") or "已停用"), limit=40) + "）" if sop.get("blocked_reason") else ""
            lines.append("·  SOP " + safe_text(str(sop.get("name") or sop.get("sop_id")), limit=80)
                         + "（" + notice_forms._mode_label(sop.get("mode")) + "）" + blocked)
        people = updated.get("people") or []
        if people:
            lines.append("可选人员：")
            lines.append("·  " + "、".join(
                safe_text(str(p.get("name") or p.get("label") or p.get("record_id")), limit=40)
                for p in people[:30]))
        scopes = updated.get("scopes") or []
        if scopes:
            lines.append("可选楼栋：" + "、".join(
                safe_text(scope, limit=20) for scope in notice_forms.scope_display_names(scopes)[:30]))
        lines.append("")
        lines.append("填写方式：")
        lines.append("  " + name + ".不使用工单：是/否")
        lines.append("  " + name + ".楼栋：[楼栋代码]")
        lines.append("  " + name + ".SOP：[SOP名称]")
        lines.append("  " + name + ".操作人：[人员姓名]")
        lines.append("  " + name + ".审核人：[人员姓名]")
        lines.append("  " + name + ".运行：[起点#→终点#]（轮巡可多组，调整用“起点#→终点#／调整单元”）")
        lines.append("未发送“不使用工单：是”时不会跳过工单确认，服务端仍会校验选择。")
        return "\n".join(lines)

    async def _fill_fields(self, message, request, actor, plans):
        statuses = {"needs_input", "awaiting_confirmation", "awaiting_second_confirmation"}
        plan, prompt = self._resolve_plan(message, plans, statuses, actor)
        if plan is None:
            return prompt if prompt else "当前没有待补充字段的操作计划。"

        def _parse(fields):
            entries = []
            for line in [item.strip() for item in (message["question"] or "").splitlines() if item.strip()]:
                match = re.fullmatch(r"([^:：]{1,120})[:：](.*)", line)
                name = (match.group(1).strip() if match else "")
                if not match:
                    return None, ("未识别字段“" + safe_text(name or line, limit=120)
                                  + "”。请逐行回复“字段名：值”。可用字段：" + _available_fields_text(fields))
                raw = match.group(2).strip()
                field, error = _resolve_field(fields, name)
                if field is not None:
                    if notice_forms.is_native_notice(field) or notice_forms.is_native_notice_sop(field):
                        return None, ("字段“" + safe_text(name, limit=120) + "”为整体对象，请按上一条展示的叶子字段名填写（如“"
                                      + name + ".标题：xxx”）。")
                    if _field_protected(field):
                        return None, ("字段“" + safe_text(name, limit=120)
                                      + "”为只读字段，不能通过飞书文本修改，请在灯塔原业务页面核对。")
                    if not _feishu_supported_type(field):
                        return None, ("字段“" + safe_text(name, limit=120)
                                      + "”属于复杂表单（选人/签名/附件/对象等），飞书文本无法安全填写。请在灯塔原业务页面填写提交，飞书内保持不变。")
                    entries.append(("simple", field, None, raw))
                    continue
                if error:
                    return None, error
                try:
                    leaf_target = notice_forms.resolve_notice_leaf(fields, name)
                except AssistantError as exc:
                    return None, str(exc)
                if leaf_target is None:
                    return None, ("未识别字段“" + safe_text(name, limit=120)
                                  + "”。可用字段：" + _available_fields_text(fields))
                parent_field, child = leaf_target
                if notice_forms.is_native_notice(parent_field):
                    # Defect #1: a protected parent field blocks ALL leaf edits,
                    # even when an individual child looks writable.
                    if notice_forms.field_protected(parent_field):
                        return None, ("字段“" + safe_text(name, limit=120)
                                      + "”所属的父字段为只读/隐藏字段，不能通过飞书文本修改，请在灯塔原业务页面核对。")
                    if child.get("hidden"):
                        return None, ("字段“" + safe_text(name, limit=120)
                                      + "”为内部只读字段，不能通过飞书文本修改，请在灯塔原业务页面核对进度。")
                    if notice_forms.child_protected(child):
                        return None, ("字段“" + safe_text(name, limit=120)
                                      + "”为只读字段，不能通过飞书文本修改，请在灯塔原业务页面核对。")
                    if not notice_forms.child_supported(child):
                        return None, ("字段“" + safe_text(name, limit=120)
                                      + "”为复杂内容，飞书文本无法安全填写，请在灯塔原业务页面填写。")
                    entries.append(("native_notice", parent_field, child, raw))
                else:
                    # native_sop parent protection too: a read-only/hidden
                    # native SOP field can never be edited through leaf lines.
                    if notice_forms.field_protected(parent_field):
                        return None, ("字段“" + safe_text(name, limit=120)
                                      + "”所属的父字段为只读/隐藏字段，不能通过飞书文本修改，请在灯塔原业务页面核对。")
                    entries.append(("native_sop", parent_field, child, raw))
            if not entries:
                return None, None
            return entries, None

        # Pass 1: validate every line against the CURRENT plan before any
        # action=edit, so a typo/unknown/protected field never silently strips
        # an awaiting-confirmation preview.
        entries, error = _parse(plan.get("fields", []))
        if error:
            return error
        if entries is None:
            return None

        returned_to_input = False
        if plan["status"] in {"awaiting_confirmation", "awaiting_second_confirmation"}:
            # Field lines are valid; only now is it safe to return the plan to
            # needs_input so the Agent accepts the actual PATCH values.
            plan = await self._plan_to_needs_input(request, actor, plan)
            returned_to_input = True
            # Re-parse against the refreshed plan (same names, refreshed data).
            entries, error = _parse(plan.get("fields", []))
            if error:
                return error
            if entries is None:
                return None

        seen = set()
        for kind, field, child, _ in entries:
            if kind == "simple":
                leaf_id = ""
            else:
                leaf_id = str(child.get("path")) if kind == "native_notice" else str(child.get("key"))
            identity = (kind, field.get("name"), leaf_id)
            if identity in seen:
                if kind == "simple":
                    return "填写的字段有重复，请每行只写一个字段。"
                return ("填写的叶子项“" + safe_text((child.get("label") or child.get("path") or child.get("key")), limit=80)
                        + "”有重复，请每行只写一项。")
            seen.add(identity)
        values = {}
        try:
            for kind, field, child, raw in entries:
                if kind == "simple":
                    values.setdefault(field["name"], _typed_value(field, raw))
                elif kind == "native_notice":
                    typed = notice_forms.typed_child_value(child, raw)
                    parent_value = values.setdefault(field["name"], dict(field))
                    # Merge into the parent placeholder built separately below.
                    parent_value.setdefault("__edits__", {})[child["path"]] = typed
                else:
                    sop_value = values.setdefault(field["name"], dict(field))
                    sop_value.setdefault("__sop_edits__", {})[child["key"]] = raw
        except AssistantError as exc:
            return str(exc)
        # Validate simple option-source fields against their loaded options.
        for kind, field, child, raw in entries:
            if kind != "simple" or not field.get("options_source"):
                continue
            options = {str(option.get("value")) for option in (field.get("options") or [])}
            chosen = [values[field["name"]]] if field.get("type") != "multiselect" else list(values[field["name"]])
            if not options or not all(item in options for item in chosen):
                return "字段“" + _field_label(field) + "”的候选项尚未加载，请先发送“查看选项 " + field["name"] + "”，再填写。"
        # Convert native placeholders into real parent objects.
        final = {}
        try:
            for key, value in values.items():
                if isinstance(value, dict) and "__edits__" in value:
                    final[key] = notice_forms.build_native_notice_value(value, value["__edits__"])
                elif isinstance(value, dict) and "__sop_edits__" in value:
                    final[key] = notice_forms.build_notice_sop_value(value, value["__sop_edits__"])
                else:
                    final[key] = value
        except AssistantError as exc:
            return str(exc)
        result = await self._call(request, actor, "plans/" + plan["id"],
                                  {"version": plan["version"], "values": final}, method="PATCH")
        note = ("已现将待确认计划返回到可编辑状态，" if returned_to_input else "")
        return "已按你的填写更新操作计划（未执行）。" + note + "\n\n" + self.plan_text(result)

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
        native = any(notice_forms.is_native_notice(field) or notice_forms.is_native_notice_sop(field)
                     for field in plan.get("fields", [])[:40])
        if native:
            text, _ = notice_forms.full_native_fields_text(plan)
            photo_hint = FeishuAssistant._pending_photo_hint(plan)
            return text + (("\n\n" + photo_hint) if photo_hint else "")
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

    @staticmethod
    def _photo_value_count(field):
        value = field.get("value")
        if value is None:
            value = field.get("selected_files") or []
        if isinstance(value, (list, tuple)):
            return sum(1 for item in value if item not in (None, ""))
        return 1 if value not in (None, "") else 0

    @staticmethod
    def _pending_photo_hint(plan):
        """Short, plain-language instruction about missing REQUIRED notice
        photos.  ``required`` means at least one photo (never maxItems) and
        optional photo fields are never flagged missing (audit #1). No internal
        field names or ids are shown (audit #7)."""
        missing, shown = [], []
        for field in (plan.get("fields") or []):
            if not field.get("native_notice_photos"):
                continue
            kind = str(field.get("native_notice_photos"))
            label = safe_text(str(field.get("label") or ""), limit=60) or (
                "现场照片" if kind == "site" else "资料截图")
            count = FeishuAssistant._photo_value_count(field)
            if count:
                shown.append(f"{label}（已选 {count} 张）")
            elif field.get("required"):
                missing.append(label)
        photo_hint_parts = []
        if shown:
            photo_hint_parts.append("已选照片：" + "、".join(dict.fromkeys(shown)))
        if missing:
            photo_hint_parts.append("缺少：" + "、".join(dict.fromkeys(missing)))
        if not photo_hint_parts:
            return ""
        guide = ("飞书卡片不能直接上传文件控件，请把照片图片直接发送到本会话补充。"
                 "若同时有多个待补充照片的通告/类别，请先回复“补照片计划 N”指定要补哪一项。")
        return "照片提示：" + "；".join(photo_hint_parts) + "。" + guide

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
                elif not message.get('kind'):
                    await self._react(message)
                try: reply = await self.answer(message)
                except Exception as exc:
                    reply = {"text": safe_text(str(exc)) if isinstance(exc, AssistantError) else "助手暂时无法处理本条消息，请稍后重新提问；未自动重发业务。"}
                await asyncio.to_thread(self.inbox.set, message["message_id"], "reply", reply)
            try:
                await self._validate_delivery(message, reply)
                await self._sync_reply_history(message, reply)
                await self._prepare_reply_files(message, reply)
                if reply.get('files'):
                    await self._validate_delivery(message, reply)
                async def checkpoint():
                    await asyncio.to_thread(self.inbox.set, message['message_id'], 'reply', reply)
                await self.messenger.send(message, reply, checkpoint=checkpoint, authorize=lambda: self._validate_delivery(message, reply))
                await asyncio.to_thread(self.inbox.remember_public_reply, message, reply)
                if reply.get('topic_plan_id') and reply.get('topic_plan_status') in {'completed', 'cancelled', 'superseded'}:
                    route_message = {**message, **(reply.get('thread_route') or {})}
                    await asyncio.to_thread(self.inbox.retire_topic_plan, route_message, reply['topic_plan_id'])
                await asyncio.to_thread(self.inbox.set, message["message_id"], "done")
            except Exception as exc:
                if isinstance(exc, AssistantError) and (exc.category == 'delivery_revoked' or exc.status in {401, 403, 404, 410} and reply.get('actor_id') and exc.category != 'feishu_file_upload_permission'):
                    if exc.category == 'feishu_delivery_permanent':
                        await asyncio.to_thread(self.inbox.defer_delivery, message['message_id'], permanent=True, reason=str(exc))
                    else:
                        await asyncio.to_thread(self.inbox.set, message['message_id'], 'reply', {
                            'text': '原回复无法通过当前权限及会话校验，旧内容已停止发送。请重新提问。', 'attachment_stopped': True}, delay=0)
                    continue
                upload_denied = isinstance(exc, AssistantError) and exc.category == 'feishu_file_upload_permission'
                if isinstance(exc, AssistantError) and (upload_denied or exc.status in {401, 403, 404, 410} and reply.get('files')):
                    stopped = {'text': str(exc) if upload_denied else '原会话或文件访问权限已变化，本次文件发送已停止。请在当前会话重新提出需要。',
                               'attachment_stopped': True}
                    await asyncio.to_thread(self.inbox.set, message['message_id'], 'reply', stopped, delay=5)
                    return
                logging.warning("Feishu assistant reply deferred: %s", type(exc).__name__)
                await asyncio.to_thread(self.inbox.defer_delivery, message['message_id'],
                                        permanent=isinstance(exc, AssistantError) and exc.category == 'feishu_delivery_permanent',
                                        reason=str(exc) if isinstance(exc, AssistantError) else type(exc).__name__)

    async def schedule_plan_status(self, message, plan, conversation=None, actor_id=None):
        """Register one deduplicated pending-result poll per person+plan.

        Only called right after a text or card confirmation that moved the plan
        into an accepted/running/submitted state (an *accepted* plan is NOT the
        same as send/receipt success, see audit #6).  The payload is bound to
        the operator and conversation so a cleared/rotated session cancels the
        outstanding notification; it never re-submits business, it only re-reads
        the plan later (growing delays, capped at 15s) and pushes the actual
        terminal result to the original person/session/group.
        """
        if not (self.inbox and plan and plan.get("status") in {"running", "submitted"}):
            return
        owner = message.get("union_id") or ("sender:" + message["sender_id"] if message.get("sender_id") else "")
        message_id = message.get("message_id") or message.get("reply_to_message_id")
        if not owner or not message_id:
            return
        identity = "plan_status_" + hashlib.sha256((owner + ":" + str(plan.get("id") or "")).encode()).hexdigest()[:40]
        original = {key: message.get(key) for key in ("message_id", "reply_to_message_id", "chat_id", "chat_type", "sender_id", "union_id", "root_id", "thread_id", "_shared_plan")}
        payload = {
            "message_id": identity,
            "owner": owner,
            "plan_id": str(plan.get("id") or ""),
            "plan_version": plan.get("version"),
            "conversation_id": str(message.get('_actor_conversation_id') or (conversation or {}).get("conversation_id") or ""),
            "actor_id": str(actor_id or message.get("sender_id") or ""),
            "attempts": 0,
            "created_at": time.time(),
            "original": {key: value for key, value in original.items() if value is not None},
        }
        await asyncio.to_thread(self.inbox.add_plan_status, payload, _PLAN_STATUS_DELAYS[0])

    async def _run_plan_status(self, identity, payload):
        try:
            async with self.plan_status_slots:
                await self._poll_plan_status(identity, payload)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logging.warning("Feishu plan-status poll deferred: %s: %s", type(exc).__name__, str(exc)[:200])
            try:
                await asyncio.to_thread(self.inbox.reschedule_plan_status, identity, _PLAN_STATUS_DELAYS[-1], payload)
            except sqlite3.Error:
                pass
        finally:
            self.plan_tasks.pop(identity, None)
            if self.event:
                self.event.set()

    async def _poll_plan_status(self, identity, payload):
        original = payload.get("original") or {}
        if not original.get("message_id"):
            await asyncio.to_thread(self.inbox.set, identity, "done")
            return
        request_context = {**original, "question": "[状态查询]"}
        try:
            request, actor = await self._request_context(request_context)
            if original.get('_shared_plan'):
                request.state.feishu_shared_plan = original['_shared_plan']
            # The pending poll is bound to the operator who confirmed and the
            # conversation they used; a rotated identity or cleared conversation
            # cancels the outstanding notification without erroring (audit #6).
            bound_actor = payload.get("actor_id")
            if bound_actor and str(actor.get("id") or "") != str(bound_actor):
                await asyncio.to_thread(self.inbox.set, identity, "done")
                return
            conversation = await self._call(request, actor, "conversation")
            bound_conversation = payload.get("conversation_id")
            if bound_conversation and str(conversation.get("conversation_id") or "") != str(bound_conversation):
                await asyncio.to_thread(self.inbox.set, identity, "done")
                return
        except AssistantError as exc:
            # Session or conversation was closed: clear-session must cancel the
            # outstanding notification without erroring or re-submitting.
            if exc.status in {401, 403, 410}:
                await asyncio.to_thread(self.inbox.set, identity, "done")
                return
            raise
        plan = next((turn.get("plan") for turn in (conversation.get("turns") or [])
                     if turn.get("plan") and str(turn["plan"].get("id") or "") == str(payload.get("plan_id") or "")), None)
        if original.get('_shared_plan'):
            try:
                plan = await self._call(request, actor, 'plans/' + payload['plan_id'])
            except AssistantError as exc:
                if exc.status in {401, 403, 404, 409, 410}:
                    await asyncio.to_thread(self.inbox.set, identity, 'done')
                    return
                raise
        if plan is None:
            # Original conversation no longer exposes this plan (e.g. cleared);
            # drop the pending-result task as the requirement expects.
            await asyncio.to_thread(self.inbox.set, identity, "done")
            return
        status = plan.get("status")
        if status in _PLAN_STATUS_TERMINAL or status in _PLAN_STATUS_FEEDBACK:
            # Terminal results, plans returned for more input, and plans that
            # moved back into (second) confirmation all get explicit feedback
            # instead of being left waiting forever (audit #6).
            await self._notify_plan_status(identity, original, request, actor, plan, conversation.get("conversation_id"))
            await asyncio.to_thread(self.inbox.set, identity, "done")
            return
        # Still running/submitted: schedule the next (capped) poll.
        attempts = int(payload.get("attempts") or 0)
        if attempts >= _PLAN_STATUS_MAX_ATTEMPTS:
            text = ("该业务操作已提交原流程处理，已等待超过15分钟仍无最终结果，说明仍在处理中。"
                    "无需重复提交；可发送“操作状态”查询最新进度，或到灯塔网页查看最终结果。")
            await self._notify_plan_status(identity, original, request, actor, plan, conversation.get("conversation_id"), text)
            await asyncio.to_thread(self.inbox.set, identity, "done")
            return
        delay = _PLAN_STATUS_DELAYS[min(attempts, len(_PLAN_STATUS_DELAYS) - 1)]
        await asyncio.to_thread(self.inbox.reschedule_plan_status, identity, delay, {**payload, "attempts": attempts + 1})

    async def _notify_plan_status(self, identity, original, request, actor, plan, conversation_id, preset_text=None):
        status = plan.get("status")
        if preset_text:
            text = preset_text
        elif status == "completed":
            title = safe_text(str(plan.get("title") or "业务操作"), limit=60)
            text = "你确认的“" + title + "”操作已完成。"
        elif status == "failed":
            text = "该业务操作执行失败：" + safe_text(str(plan.get("error") or "请查看原任务详情"), limit=300)
        elif status in {"cancelled", "superseded"}:
            text = "该业务操作%s；已执行的部分不会自动撤销。" % ("已取消" if status == "cancelled" else "已被最新的提交替代")
        elif status in {"needs_input", "awaiting_confirmation", "awaiting_second_confirmation"}:
            text = _plain_status_line(plan)
        else:
            return
        reply = {"text": text, 'actor_id': actor['id'], 'conversation_id': conversation_id,
                 'delivery_access': self._delivery_access(actor), 'topic_plan_id': plan.get('id'), 'topic_plan_status': status}
        original_target = original.get("reply_to_message_id") or original.get("message_id") or ""
        result_tag = hashlib.sha256((original_target + ":" + str(plan.get("id")) + ":" + str(plan.get("version")) + ":" + str(status)).encode()).hexdigest()[:32]
        stored_reply = await asyncio.to_thread(self.inbox.reply_for, identity)
        reused = stored_reply is not None and stored_reply.get("_result_tag") == result_tag
        if reused:
            reply = stored_reply
        elif status in {"needs_input", "awaiting_confirmation", "awaiting_second_confirmation", "failed"}:
            # Attach a fresh, editable card so the operator can finish the form
            # directly in Feishu; the explanation must be plain language, never
            # kernel status terms (audit #5/#7).
            try:
                from .feishu_notice_cards import prepare_card
                card_result = await prepare_card(self, original, actor,
                                                 {"conversation_id": conversation_id} if conversation_id else {},
                                                 plan)
                if card_result.get("card"):
                    reply["card"] = card_result["card"]
                    reply["ticket"] = card_result["ticket"]
                    reply["text"] = _plain_status_line(plan)
            except Exception:
                logging.warning("Feishu result card unavailable for status %s", status)

        # The result message must have its OWN deterministic message_id (independent
        # of the original confirmation message) derived from the original reply
        # target + plan id/version/status.  Reusing time.time() made the id random
        # on every retry, which rebuilt the card/ticket and invalidated an already
        # delivered card (audit #5 / lost-response retry).  ``reply_to_message_id``
        # is the true original real message (original.reply_to_message_id first).
        original_target = (original or {}).get("reply_to_message_id") or (original or {}).get("message_id") or ""
        plan_id = str(plan.get("id") or "")
        plan_version = plan.get("version")
        # Track the exact terminal result the outgoing notification reflects, so a
        # same-status/version retry reuses the persisted reply while a genuinely
        # different result rebuilds it.
        result_tag = hashlib.sha256(
            (original_target + ":" + plan_id + ":" + str(plan_version) + ":" + str(status)).encode()
        ).hexdigest()[:32]
        notify_id = "notify_" + hashlib.sha256(
            (original_target + ":" + plan_id + ":" + str(plan_version) + ":" + str(status)).encode()
        ).hexdigest()[:32]
        notify_message = {
            **{key: original[key] for key in ('root_id', 'thread_id', '_shared_plan') if original.get(key)},
            "message_id": notify_id,
            "reply_to_message_id": original_target or original.get("message_id"),
            "chat_id": original.get("chat_id"),
            "chat_type": original.get("chat_type"),
            "sender_id": original.get("sender_id"),
            "union_id": original.get("union_id"),
            "question": "[状态通知]",
        }
        # Reuse a previously persisted outgoing reply when it reflects the same
        # actual result.  This keeps the exact same card/ticket on a lost-response
        # retry instead of rebuilding new tickets that would invalidate the card
        # already delivered to the user.
        if not reused:
            # Persist the result reply on the plan-status inbox row BEFORE sending,
            # so a lost response can resend the identical payload later.
            reply["_result_tag"] = result_tag
            await asyncio.to_thread(self.inbox.store_plan_status_reply, identity, reply)
        send_reply = {key: value for key, value in reply.items() if key != "_result_tag"}
        async def checkpoint():
            await asyncio.to_thread(self.inbox.set, identity, 'reply', {**send_reply, '_result_tag': result_tag})
        await self._validate_delivery(notify_message, send_reply)
        await self.messenger.send(notify_message, send_reply, checkpoint=checkpoint, authorize=lambda: self._validate_delivery(notify_message, send_reply))
        await asyncio.to_thread(self.inbox.remember_public_reply, notify_message, send_reply)
        if status in {'completed', 'cancelled', 'superseded'}:
            await asyncio.to_thread(self.inbox.retire_topic_plan, {**notify_message, **(send_reply.get('thread_route') or {})}, plan['id'])
        if conversation_id:
            operation_id = "feishu_" + hashlib.sha256((notify_id + ":result").encode()).hexdigest()[:40]
            try:
                # channel-replies must NOT carry a non-existent status param
                # (audit #5); only the fields the endpoint expects are sent.
                await self._call(request, actor, "channel-replies",
                                 {"conversation_id": conversation_id, "operation_id": operation_id,
                                  "question": "[操作结果]", "answer": send_reply["text"]})
            except AssistantError:
                pass

    async def _run(self):
        while not self.closing:
            self.event.clear()
            if time.monotonic() >= self.next_cleanup:
                try:
                    await asyncio.to_thread(self.inbox.cleanup)
                except sqlite3.Error:
                    logging.warning('Feishu inbox cleanup deferred')
                self.next_cleanup = time.monotonic() + 3600
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
            for status_payload in await asyncio.to_thread(self.inbox.due_plan_statuses):
                identity = status_payload.get("message_id")
                if not identity or (identity in self.plan_tasks and not self.plan_tasks[identity].done()):
                    continue
                self.plan_tasks[identity] = asyncio.create_task(self._run_plan_status(identity, status_payload))
            for identity in list(self.plan_tasks):
                if self.plan_tasks[identity].done():
                    self.plan_tasks.pop(identity, None)
            try: await asyncio.wait_for(self.event.wait(), 10)
            except asyncio.TimeoutError: pass

    async def close(self):
        self.closing = True
        if self.process and self.process.poll() is None:
            self.process.terminate()
            await asyncio.to_thread(self.process.wait, 5)
        pending = [task for task in [self.runner, *self.tasks.values(), *self.reactions.values(), *self.plan_tasks.values()] if task and not task.done()]
        for task in pending: task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        if self.messenger: await self.messenger.close()


def _plan_key(message, actor=None):
    identity = (actor or {}).get('id') or message['sender_id']
    root = message.get('root_id') or message.get('thread_id')
    return identity + ':' + message['chat_id'] + ':' + root if root else identity


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


def _plain_status_line(plan):
    """Plain-language explanation of a non-terminal plan state for result
    notifications.  Never uses kernel status terms (audit #5/#7)."""
    status = plan.get("status")
    if status == "needs_input":
        base = ("业务核对未通过，还需要补充一些必填内容后再确认（比如现场照片）。"
                "你之前填写的内容已经保留，不会重复提交。")
        photo_hint = FeishuAssistant._pending_photo_hint(plan)
        return base + (("\n" + photo_hint) if photo_hint else "")
    if status == "awaiting_confirmation":
        return ("操作还没执行，正在等你再次确认。请核对内容后确认执行，"
                "或取消这次操作。你之前的填写还保留着。")
    if status == "awaiting_second_confirmation":
        return ("操作还没执行，风险较高，正在等你再次确认。请核对后再次确认，"
                "或取消这次操作。你之前的填写还保留着。")
    return "该业务操作仍在处理中，请稍后查看最新结果。"


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
    return bool(field.get("readonly") or field.get("read_only") or field.get("disabled")
                or field.get("hidden") or field.get("只读"))


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
    from .feishu_assistant_settings import install_feishu_assistant_settings
    install_feishu_assistant_settings(app, controller, runtime, service)
    from .permission_review_cards import install as install_permission_cards
    install_permission_cards(app, service)

    async def incoming(request: Request):
        return await service.receive(request)

    app.add_api_route("/api/assistant/feishu-event", incoming, methods=["POST"])
    return service
