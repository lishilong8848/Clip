"""Optional Feishu bot receiver. Run with one application worker."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import math
import os
import secrets
import sqlite3
import time
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

MAX_BODY_BYTES = 20 * 1024
HOOK_PREFIX = "/open-apis/bot/v2/hook/"
TOKEN_URL = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
MESSAGE_URL = "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id"
TOKEN_REFRESH_MARGIN = 60.0


@dataclass
class Settings:
    webhook_url: str = ""
    token: str = ""
    sign_secret: str = ""
    db_path: str = str(Path(__file__).resolve().parent / "data" / "dedup.sqlite3")
    dedup_seconds: int = 86400
    timeout_seconds: float = 8.0
    app_id: str = ""
    app_secret: str = ""
    chat_id: str = ""
    dedup_mode: str = "alarm"

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            webhook_url=os.getenv("FEISHU_WEBHOOK_URL", ""),
            token=os.getenv("ALARM_RECEIVER_TOKEN", ""),
            sign_secret=os.getenv("FEISHU_SIGN_SECRET", ""),
            app_id=os.getenv("FEISHU_APP_ID", ""),
            app_secret=os.getenv("FEISHU_APP_SECRET", ""),
            chat_id=os.getenv("FEISHU_CHAT_ID", ""),
            dedup_mode=os.getenv("FEISHU_DEDUP_MODE", "alarm"),
            db_path=os.getenv("FEISHU_DEDUP_DB", cls.db_path),
            dedup_seconds=int(os.getenv("FEISHU_DEDUP_SECONDS", "86400")),
            timeout_seconds=float(os.getenv("FEISHU_TIMEOUT_SECONDS", "8")),
        )


def _app_mode(settings: Settings) -> bool:
    """Application mode is selected when any application field is nonempty."""
    for name in ("app_id", "app_secret", "chat_id"):
        value = getattr(settings, name)
        if not isinstance(value, str) or value:
            return True
    return False


def _valid(settings: Settings) -> bool:
    try:
        if not all(isinstance(value, str) for value in (
            settings.webhook_url, settings.token, settings.sign_secret,
            settings.app_id, settings.app_secret, settings.chat_id, settings.db_path,
        )):
            return False
        if not settings.token.strip() or not settings.db_path.strip():
            return False
        if settings.dedup_mode not in ("alarm", "scan"):
            return False
        if type(settings.dedup_seconds) is not int or settings.dedup_seconds <= 0:
            return False
        if type(settings.timeout_seconds) not in (int, float):
            return False
        if not math.isfinite(settings.timeout_seconds) or settings.timeout_seconds <= 0:
            return False
        if _app_mode(settings):
            # Application mode never falls back to the bot webhook.
            return (
                bool(settings.app_id.strip())
                and bool(settings.app_secret.strip())
                and bool(settings.chat_id.strip())
                and settings.chat_id == settings.chat_id.strip()
                and settings.chat_id.startswith("oc_")
            )
        url = settings.webhook_url
        parts = urlsplit(url)
        return (
            parts.scheme == "https"
            and parts.hostname in {"open.feishu.cn", "open.larksuite.com"}
            and parts.port in (None, 443)
            and parts.username is None and parts.password is None
            and "?" not in url and "#" not in url
            and parts.path.startswith(HOOK_PREFIX)
            and bool(parts.path[len(HOOK_PREFIX):])
            and url == url.strip()
        )
    except (ValueError, TypeError, AttributeError):
        return False


def _error(status: int, code: str) -> JSONResponse:
    return JSONResponse({"status": "error", "code": code}, status_code=status)


def _upstream_ok(response: httpx.Response) -> bool:
    if not 200 <= response.status_code < 300:
        return False
    try:
        body = response.json()
    except ValueError:
        return False
    if not isinstance(body, dict):
        return False
    code = body.get("code") if "code" in body else body.get("StatusCode")
    return type(code) is int and code == 0


def _payload(settings: Settings, content: str) -> bytes:
    body = {"msg_type": "text", "content": {"text": content}}
    if settings.sign_secret:
        timestamp = str(int(time.time()))
        key = f"{timestamp}\n{settings.sign_secret}".encode("utf-8")
        body["timestamp"] = timestamp
        body["sign"] = base64.b64encode(
            hmac.new(key, b"", hashlib.sha256).digest()
        ).decode("ascii")
    return json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _app_payload(chat_id: str, content: str, uuid: str) -> bytes:
    body = {
        "receive_id": chat_id,
        "msg_type": "text",
        "content": json.dumps({"text": content}, ensure_ascii=False, separators=(",", ":")),
        "uuid": uuid,
    }
    return json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _json_object(response: httpx.Response) -> dict | None:
    try:
        body = response.json()
    except ValueError:
        return None
    return body if isinstance(body, dict) else None


def _exact_code(body: dict | None) -> int | None:
    if body is None:
        return None
    code = body.get("code")
    return code if type(code) is int else None


def _upstream_rejected(response: httpx.Response) -> JSONResponse:
    """Expose only a nonzero integer diagnostic code, never upstream text."""
    body = _json_object(response)
    code = _exact_code(body)
    if code is None or code == 0:
        return _error(502, "upstream_rejected")
    return JSONResponse(
        {"status": "error", "code": "upstream_rejected", "upstream_code": code},
        status_code=502,
    )


async def _tenant_token(state: dict, settings: Settings):
    """Return (token, failure). Cached in app state; refresh TOKEN_REFRESH_MARGIN early."""
    cached = state.get("tenant_token")
    if cached is not None and time.monotonic() < cached[1]:
        return cached[0], None
    state["tenant_token"] = None
    body = json.dumps({"app_id": settings.app_id, "app_secret": settings.app_secret},
                      ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    try:
        response = await state["client"].post(
            TOKEN_URL, content=body,
            headers={"Content-Type": "application/json; charset=utf-8"},
        )
    except httpx.TimeoutException:
        return None, _error(504, "upstream_timeout")
    except httpx.HTTPError:
        return None, _error(502, "upstream_error")
    data = _json_object(response)
    status = response.status_code
    if status < 200 or status >= 300:
        return None, _upstream_rejected(response)
    if _exact_code(data) != 0:
        return None, _upstream_rejected(response)
    token = data.get("tenant_access_token")
    expire_in = data.get("expire")
    if not isinstance(token, str) or not token.strip():
        return None, _error(502, "upstream_rejected")
    if type(expire_in) not in (int, float) or not math.isfinite(expire_in) or expire_in <= 0:
        return None, _error(502, "upstream_rejected")
    ttl = max(0.0, float(expire_in) - TOKEN_REFRESH_MARGIN)
    state["tenant_token"] = (token, time.monotonic() + ttl)
    return token, None


def _app_message_ok(response: httpx.Response) -> str | None:
    """Return message_id only for HTTP 2xx + exact code 0 + nonempty message_id."""
    if not 200 <= response.status_code < 300:
        return None
    data = _json_object(response)
    if _exact_code(data) != 0:
        return None
    payload = data.get("data")
    message_id = payload.get("message_id") if isinstance(payload, dict) else None
    if not isinstance(message_id, str) or not message_id.strip():
        return None
    return message_id


async def _send_app(state: dict, settings: Settings, body: bytes, allow_retry: bool):
    """Send via the application API. Returns (message_id, retry_after_refresh, failure)."""
    token, failure = await _tenant_token(state, settings)
    if failure is not None:
        return None, False, failure
    headers = {"Authorization": f"Bearer {token}",
               "Content-Type": "application/json; charset=utf-8"}
    try:
        response = await state["client"].post(MESSAGE_URL, content=body, headers=headers)
    except httpx.TimeoutException:
        return None, False, _error(504, "upstream_timeout")
    except httpx.HTTPError:
        return None, False, _error(502, "upstream_error")
    if response.status_code == 401:
        if allow_retry:
            return None, True, None
        # A second 401 after the single refresh is a sanitized rejection.
        return None, False, _upstream_rejected(response)
    message_id = _app_message_ok(response)
    if message_id is None:
        return None, False, _upstream_rejected(response)
    return message_id, False, None


def create_app(settings: Settings | None = None, transport=None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        state = {"ready": False}
        app.state.receiver = state
        async with AsyncExitStack() as stack:
            try:
                current = settings if settings is not None else Settings.from_env()
                if _valid(current):
                    db_file = Path(current.db_path)
                    db_file.parent.mkdir(parents=True, exist_ok=True)
                    db = sqlite3.connect(str(db_file), timeout=5.0)
                    stack.callback(db.close)
                    db.execute("CREATE TABLE IF NOT EXISTS delivered "
                               "(event_key TEXT PRIMARY KEY, sent_at REAL NOT NULL)")
                    db.execute("CREATE TABLE IF NOT EXISTS pending_delivery (event_key TEXT PRIMARY KEY, created_at REAL NOT NULL)")
                    db.commit()
                    client = await stack.enter_async_context(httpx.AsyncClient(
                        transport=transport, timeout=current.timeout_seconds,
                        follow_redirects=False, trust_env=False,
                    ))
                    state.update(settings=current, db=db, client=client,
                                 lock=asyncio.Lock(), ready=True,
                                 app_mode=_app_mode(current), tenant_token=None)
            except Exception:
                # Startup failures remain visible as sanitized, unready health.
                state["ready"] = False
            try:
                yield
            finally:
                state["ready"] = False

    app = FastAPI(title="Feishu Alarm Receiver", lifespan=lifespan)

    @app.get("/health")
    async def health(request: Request):
        ready = getattr(request.app.state, "receiver", {}).get("ready", False)
        return {"status": "ok" if ready else "not_configured", "ready": ready}

    @app.post("/api/alarms")
    async def alarms(request: Request):
        state = getattr(request.app.state, "receiver", {})
        if not state.get("ready"):
            return _error(503, "not_configured")
        current = state["settings"]
        scheme, _, supplied = request.headers.get("authorization", "").partition(" ")
        if scheme.lower() != "bearer" or not secrets.compare_digest(
            supplied.strip().encode("utf-8"), current.token.encode("utf-8")
        ):
            return _error(401, "unauthorized")
        raw = await request.body()
        if len(raw) > 256 * 1024:
            return _error(413, "payload_too_large")
        try:
            body = json.loads(raw)
        except (ValueError, UnicodeDecodeError):
            return _error(400, "invalid_json")
        if not isinstance(body, dict):
            return _error(400, "invalid_payload")
        content, louhao, ts = body.get("content"), body.get("louhao", ""), body.get("ts", 0)
        if not isinstance(content, str) or not content.strip() or len(content) > 12000:
            return _error(400, "invalid_content")
        if not isinstance(louhao, str) or len(louhao) > 64:
            return _error(400, "invalid_louhao")
        if type(ts) is not int:
            return _error(400, "invalid_ts")
        if current.dedup_mode == "scan" and ts <= 0:
            return _error(400, "invalid_scan_timestamp")
        app_mode = state.get("app_mode", False)
        try:
            identity = [louhao, content, ts] if current.dedup_mode == "scan" else [louhao, content]
            canonical = json.dumps(identity, ensure_ascii=False, separators=(",", ":"))
            key = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
            if app_mode:
                serialized = _app_payload(current.chat_id, content, key[:32])
            else:
                serialized = _payload(current, content)
        except (TypeError, ValueError, UnicodeEncodeError):
            return _error(400, "invalid_payload")
        if len(serialized) > MAX_BODY_BYTES:
            return _error(413, "payload_too_large")
        db = state["db"]
        try:
            async with state["lock"]:
                now = time.time()
                row = db.execute("SELECT sent_at FROM delivered WHERE event_key = ?", (key,)).fetchone()
                if row is not None:
                    return {"status": "duplicate", "duplicate": True}
                if db.execute('SELECT 1 FROM pending_delivery WHERE event_key=?', (key,)).fetchone():
                    return _error(409, 'delivery_uncertain')
                message_id = None
                if app_mode:
                    # Resolve authentication before marking a possible external write.
                    _, token_failure = await _tenant_token(state, current)
                    if token_failure is not None:
                        return token_failure
                    with db:
                        claimed = db.execute('INSERT OR IGNORE INTO pending_delivery VALUES (?, ?)', (key, now))
                    if not claimed.rowcount:
                        return _error(409, 'delivery_uncertain')
                    # HTTP 401 only: one token refresh, then one retry of the exact body.
                    message_id, retry, failure = await _send_app(
                        state, current, serialized, allow_retry=True)
                    if retry:
                        state["tenant_token"] = None
                        message_id, _, failure = await _send_app(
                            state, current, serialized, allow_retry=False)
                    if failure is not None:
                        detail = json.loads(failure.body)
                        if detail.get('upstream_code'):
                            with db:
                                db.execute('DELETE FROM pending_delivery WHERE event_key=?', (key,))
                        return failure
                else:
                    try:
                        response = await state["client"].post(
                            current.webhook_url, content=serialized,
                            headers={"Content-Type": "application/json; charset=utf-8"},
                        )
                    except httpx.TimeoutException:
                        return _error(504, "upstream_timeout")
                    except httpx.HTTPError:
                        return _error(502, "upstream_error")
                    if not _upstream_ok(response):
                        return _error(502, "upstream_rejected")
                with db:
                    sent_at = time.time()
                    db.execute("INSERT INTO delivered (event_key, sent_at) VALUES (?, ?) "
                               "ON CONFLICT(event_key) DO UPDATE SET sent_at=excluded.sent_at",
                               (key, sent_at))
                    db.execute('DELETE FROM pending_delivery WHERE event_key=?', (key,))
        except sqlite3.Error:
            return _error(500, "storage_error")
        if app_mode:
            return {"status": "sent", "duplicate": False, "message_id": message_id}
        return {"status": "sent", "duplicate": False}

    return app


app = create_app()
