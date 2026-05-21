# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import copy
import hashlib
import html
import json
import logging
import mimetypes
import socket
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse

from .portal_auth import AUTH_COOKIE_NAME, PortalAuthManager
from .portal_service import (
    DEFAULT_APP_TOKEN,
    DEFAULT_TABLE_ID,
    MaintenancePortalService,
    PortalError,
    SCOPE_OPTIONS,
    SOURCE_CACHE_TTL_SECONDS,
)
from .state_store import LanPortalStateStore
from upload_event_module.services.robot_webhook import send_text_to_open_ids


STATIC_DIR = Path(__file__).resolve().parent / "static"
DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 18766
CLIENT_DISCONNECT_WINERRORS = {10053, 10054, 10058}
MAX_JSON_BODY_BYTES = 512 * 1024


def find_available_port(host: str, preferred_port: int) -> int:
    port = preferred_port
    while port < preferred_port + 20:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind((host, port))
                return port
            except OSError:
                port += 1
    raise RuntimeError(f"未找到可用端口，起始端口={preferred_port}")


class PortalHandler(BaseHTTPRequestHandler):
    service = MaintenancePortalService()
    auth_manager = PortalAuthManager()
    state_store = LanPortalStateStore()
    notice_callback = None
    ongoing_callback = None
    ongoing_delete_callback = None
    maintenance_action_callback = None
    last_ongoing_error = ""
    action_queue: list[str] = []
    action_queue_lock = threading.RLock()
    action_queue_event = threading.Event()
    action_worker_thread: threading.Thread | None = None
    action_worker_stop = False
    action_upload_timeout_s = 30 * 60
    upload_wait_thread: threading.Thread | None = None
    upload_wait_jobs: dict[str, float] = {}
    upload_wait_lock = threading.RLock()
    upload_wait_event = threading.Event()
    upload_wait_stop = False
    message_queue: list[str] = []
    message_queue_lock = threading.RLock()
    message_queue_event = threading.Event()
    message_worker_threads: list[threading.Thread] = []
    message_worker_stop = False
    message_worker_count = 5
    payload_cache_lock = threading.RLock()
    payload_cache: dict[tuple, tuple[float, dict]] = {}
    payload_cache_inflight: dict[tuple, threading.Event] = {}
    payload_cache_generation = 0
    payload_cache_ttl_s = 3
    payload_cache_max_entries = 64
    payload_cache_max_payload_bytes = 1024 * 1024
    orphan_reconcile_lock = threading.RLock()
    orphan_reconcile_last: dict[str, float] = {}
    orphan_reconcile_pending: set[str] = set()
    orphan_reconcile_interval_s = 15 * 60
    source_refresh_thread: threading.Thread | None = None
    source_refresh_stop = False
    source_refresh_event = threading.Event()
    source_refresh_lock = threading.RLock()
    repair_refresh_lock = threading.RLock()
    repair_refresh_inflight = False
    repair_refresh_event = threading.Event()
    repair_refresh_last_result: dict = {}
    repair_refresh_last_error = ""
    repair_refresh_last_finished = 0.0
    repair_refresh_reuse_window_s = 10.0

    @staticmethod
    def _is_client_disconnect(exc: BaseException) -> bool:
        if isinstance(exc, (BrokenPipeError, ConnectionAbortedError, ConnectionResetError)):
            return True
        return isinstance(exc, OSError) and getattr(exc, "winerror", None) in CLIENT_DISCONNECT_WINERRORS

    def _write_response(self, status: int, headers: dict[str, str], body: bytes) -> None:
        try:
            self.send_response(status)
            for name, value in headers.items():
                self.send_header(name, value)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except OSError as exc:
            if not self._is_client_disconnect(exc):
                raise

    def _send_json(self, status: int, payload: dict) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self._write_response(
            status,
            {
                "Content-Type": "application/json; charset=utf-8",
                "Cache-Control": "no-store",
            },
            body,
        )

    @classmethod
    def _with_runtime_warnings(cls, payload: dict) -> dict:
        if cls.last_ongoing_error and isinstance(payload, dict):
            warnings = payload.setdefault("warnings", [])
            if isinstance(warnings, list) and cls.last_ongoing_error not in warnings:
                warnings.append(cls.last_ongoing_error)
        return payload

    def _send_html(self, path: Path) -> None:
        body = path.read_bytes()
        self._write_response(
            HTTPStatus.OK,
            {"Content-Type": "text/html; charset=utf-8"},
            body,
        )

    def _send_static_file(self, path: Path) -> None:
        resolved = path.resolve()
        static_root = STATIC_DIR.resolve()
        try:
            resolved.relative_to(static_root)
        except ValueError:
            return self._send_json(404, {"ok": False, "error": "Not Found"})
        if not resolved.is_file():
            return self._send_json(404, {"ok": False, "error": "Not Found"})
        body = resolved.read_bytes()
        content_type = mimetypes.guess_type(str(resolved))[0] or "application/octet-stream"
        self._write_response(
            HTTPStatus.OK,
            {
                "Content-Type": content_type,
                "Cache-Control": "public, max-age=86400",
            },
            body,
        )

    def _read_json_body(self) -> dict:
        raw_length = self.headers.get("Content-Length", "0") or "0"
        try:
            length = int(raw_length)
        except ValueError as exc:
            raise ValueError("请求体长度无效。") from exc
        if length < 0:
            raise ValueError("请求体长度无效。")
        if length > MAX_JSON_BODY_BYTES:
            raise ValueError("请求体过大，请减少粘贴内容后重试。")
        raw = self.rfile.read(length) if length > 0 else b"{}"
        try:
            payload = json.loads(raw.decode("utf-8"))
        except UnicodeDecodeError as exc:
            raise ValueError("请求体必须是 UTF-8 JSON。") from exc
        if not isinstance(payload, dict):
            raise ValueError("请求体必须是 JSON 对象。")
        return payload

    def _cookie_value(self, name: str) -> str:
        raw_cookie = self.headers.get("Cookie", "") or ""
        prefix = f"{name}="
        for part in raw_cookie.split(";"):
            text = part.strip()
            if text.startswith(prefix):
                return text[len(prefix) :]
        return ""

    def _current_session(self) -> dict | None:
        return PortalHandler.auth_manager.get_session(
            self._cookie_value(AUTH_COOKIE_NAME)
        )

    def _fallback_request_host(self) -> str:
        host, port = self.server.server_address[:2]
        host = str(host or "127.0.0.1")
        if host in {"0.0.0.0", "::"}:
            host = "127.0.0.1"
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        return f"{host}:{port}"

    @staticmethod
    def _safe_proto_value(raw_proto: str) -> str:
        proto = str(raw_proto or "http").split(",", 1)[0].strip().lower()
        return proto if proto in {"http", "https"} else "http"

    @staticmethod
    def _safe_host_value(raw_host: str, fallback: str) -> str:
        text = str(raw_host or "").strip()
        if (
            not text
            or "@" in text
            or any(ch.isspace() for ch in text)
            or any(ch in text for ch in "/\\")
            or any(ord(ch) < 32 for ch in text)
        ):
            return fallback
        try:
            parsed = urlparse(f"http://{text}")
            if not parsed.hostname:
                return fallback
            if parsed.hostname in {"0.0.0.0", "::"}:
                return fallback
            _ = parsed.port
        except ValueError:
            return fallback
        return parsed.netloc or fallback

    def _request_base_url(self) -> str:
        host = self._safe_host_value(
            self.headers.get("Host", ""),
            self._fallback_request_host(),
        )
        proto = self._safe_proto_value(self.headers.get("X-Forwarded-Proto", "http"))
        return f"{proto}://{host}"

    def _request_target(self) -> str:
        target = self.path or "/"
        if not target.startswith("/"):
            return "/"
        return target

    def _send_redirect(self, location: str, *, set_cookie: str = "") -> None:
        headers = {"Location": location}
        if set_cookie:
            headers["Set-Cookie"] = set_cookie
        self._write_response(302, headers, b"")

    def _redirect_root_oauth_callback(self, parsed) -> bool:
        if parsed.path != "/":
            return False
        qs = parse_qs(parsed.query)
        code = (qs.get("code") or [""])[0]
        state = (qs.get("state") or [""])[0]
        if not code or not state:
            return False
        sanitized_query = {
            key: value
            for key, value in qs.items()
            if key not in {"code", "state"}
        }
        sanitized = "/"
        if sanitized_query:
            sanitized = f"/?{urlencode(sanitized_query, doseq=True)}"
        if self._current_session() is not None:
            self._send_redirect(sanitized)
            return True
        redirect_uri = f"{self._request_base_url()}/api/auth/feishu/callback"
        try:
            session_id, next_path = PortalHandler.auth_manager.complete_login(
                code=code,
                state=state,
                redirect_uri=redirect_uri,
            )
        except PortalError as exc:
            self._send_html_message(400, "飞书登录失败", str(exc))
            return True
        self._send_redirect(
            next_path or sanitized,
            set_cookie=PortalHandler.auth_manager.cookie_header(session_id),
        )
        return True

    def _send_html_message(self, status: int, title: str, message: str) -> None:
        body = (
            "<!doctype html><html lang=\"zh-CN\"><head><meta charset=\"utf-8\">"
            f"<title>{html.escape(title)}</title></head><body>"
            f"<h2>{html.escape(title)}</h2><p>{html.escape(message)}</p>"
            "<p><a href=\"/\">返回工作台</a></p></body></html>"
        ).encode("utf-8")
        self._write_response(
            status,
            {"Content-Type": "text/html; charset=utf-8"},
            body,
        )

    def _require_auth(self) -> dict | None:
        session = self._current_session()
        if session is not None:
            return session
        return None

    def _require_auth_json(self) -> dict | None:
        session = self._require_auth()
        if session is not None:
            return session
        self._send_json(
            401,
            {
                "ok": False,
                "error": "请先使用飞书扫码登录。",
                "auth_required": True,
            },
        )
        return None

    def _require_admin_json(self, session: dict) -> bool:
        if PortalHandler.auth_manager.is_admin(session):
            return True
        self._send_json(403, {"ok": False, "error": "只有管理员可以执行该操作。"})
        return False

    def _authorized_scope_or_error(self, session: dict, scope: str) -> str:
        normalized = PortalHandler.auth_manager.normalize_scope(scope)
        if not PortalHandler.auth_manager.scope_allowed(session, normalized):
            raise PortalError(
                f"当前飞书账号无权访问 {PortalHandler.auth_manager.scope_label(normalized)}。"
            )
        return normalized

    def _with_auth_context(self, payload: dict, session: dict) -> dict:
        if not isinstance(payload, dict):
            return payload
        result = dict(payload)
        result["auth"] = PortalHandler.auth_manager.public_status(
            session,
            next_path=self._request_target(),
            redirect_uri=f"{self._request_base_url()}/api/auth/feishu/callback",
        )
        scope_options = result.get("scope_options")
        if isinstance(scope_options, list):
            result["scope_options"] = PortalHandler.auth_manager.filter_scope_options(
                scope_options, session
            )
        return result

    def _job_visible_to_session(self, job: dict, session: dict) -> bool:
        if PortalHandler.auth_manager.is_admin(session):
            return True
        open_id = str((session.get("user") or {}).get("open_id") or "")
        request = job.get("request") if isinstance(job.get("request"), dict) else {}
        return bool(open_id and str(request.get("_auth_open_id") or "") == open_id)

    @classmethod
    def clear_payload_cache(cls) -> None:
        with cls.payload_cache_lock:
            cls.payload_cache_generation += 1
            cls.payload_cache.clear()

    @classmethod
    def _prune_payload_cache_locked(cls, now: float) -> None:
        expired_keys = [
            key for key, (expires_at, _payload) in cls.payload_cache.items()
            if expires_at <= now
        ]
        for key in expired_keys:
            cls.payload_cache.pop(key, None)
        extra = len(cls.payload_cache) - int(cls.payload_cache_max_entries)
        if extra <= 0:
            return
        ordered = sorted(
            cls.payload_cache.items(),
            key=lambda item: item[1][0],
        )
        for key, _value in ordered[:extra]:
            cls.payload_cache.pop(key, None)

    @staticmethod
    def _ongoing_items_marker(ongoing: list[dict] | None) -> tuple:
        marker = []
        for item in ongoing or []:
            if not isinstance(item, dict):
                continue
            try:
                payload_hash = hashlib.sha1(
                    json.dumps(
                        item,
                        ensure_ascii=False,
                        sort_keys=True,
                        default=str,
                        separators=(",", ":"),
                    ).encode("utf-8")
                ).hexdigest()
            except Exception:
                payload_hash = hashlib.sha1(
                    str(item).encode("utf-8", errors="ignore")
                ).hexdigest()
            marker.append(
                (
                    str(item.get("work_type") or ""),
                    str(item.get("active_item_id") or ""),
                    str(item.get("record_id") or item.get("target_record_id") or ""),
                    str(item.get("source_record_id") or ""),
                    str(item.get("status") or ""),
                    str(item.get("updated_at") or item.get("last_updated_at") or ""),
                    payload_hash,
                )
            )
        return tuple(sorted(marker))

    def _cached_service_payload(self, key_parts: tuple, builder) -> dict:
        service_version = 0
        try:
            service_version = self.service.state_cache_version()
        except Exception:
            pass
        key = (
            PortalHandler.payload_cache_generation,
            service_version,
            str(getattr(self.service, "_last_loaded_at", "") or ""),
            *key_parts,
        )
        now = time.monotonic()
        with PortalHandler.payload_cache_lock:
            PortalHandler._prune_payload_cache_locked(now)
            cached = PortalHandler.payload_cache.get(key)
            if cached and cached[0] > now:
                return copy.deepcopy(cached[1])
            inflight = PortalHandler.payload_cache_inflight.get(key)
            if inflight is None:
                inflight = threading.Event()
                PortalHandler.payload_cache_inflight[key] = inflight
                owner = True
            else:
                owner = False
        if not owner:
            inflight.wait(timeout=5)
            with PortalHandler.payload_cache_lock:
                cached = PortalHandler.payload_cache.get(key)
                if cached and cached[0] > time.monotonic():
                    return copy.deepcopy(cached[1])
        try:
            payload = builder()
        except Exception:
            if owner:
                with PortalHandler.payload_cache_lock:
                    event = PortalHandler.payload_cache_inflight.pop(key, None)
                    if event is not None:
                        event.set()
            raise
        if not isinstance(payload, dict):
            if owner:
                with PortalHandler.payload_cache_lock:
                    event = PortalHandler.payload_cache_inflight.pop(key, None)
                    if event is not None:
                        event.set()
            return payload
        try:
            payload_size = len(
                json.dumps(
                    payload,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    default=str,
                ).encode("utf-8")
            )
        except Exception:
            payload_size = 0
        if (
            payload_size
            and payload_size > int(PortalHandler.payload_cache_max_payload_bytes)
        ):
            if owner:
                with PortalHandler.payload_cache_lock:
                    event = PortalHandler.payload_cache_inflight.pop(key, None)
                    if event is not None:
                        event.set()
            return payload
        with PortalHandler.payload_cache_lock:
            PortalHandler.payload_cache[key] = (
                now + float(PortalHandler.payload_cache_ttl_s),
                copy.deepcopy(payload),
            )
            PortalHandler._prune_payload_cache_locked(now)
            if owner:
                event = PortalHandler.payload_cache_inflight.pop(key, None)
                if event is not None:
                    event.set()
        return payload

    @classmethod
    def _refresh_repair_source_singleflight(cls) -> dict:
        now = time.monotonic()
        with cls.repair_refresh_lock:
            if (
                cls.repair_refresh_last_result
                and now - float(cls.repair_refresh_last_finished or 0)
                <= float(cls.repair_refresh_reuse_window_s)
            ):
                result = copy.deepcopy(cls.repair_refresh_last_result)
                result["repair_refresh_reused"] = True
                return result
            if cls.repair_refresh_inflight:
                event = cls.repair_refresh_event
                owner = False
            else:
                event = threading.Event()
                cls.repair_refresh_event = event
                cls.repair_refresh_inflight = True
                cls.repair_refresh_last_error = ""
                owner = True

        if not owner:
            if not event.wait(timeout=120):
                raise PortalError("检修源表刷新仍在进行，请稍后查看。")
            with cls.repair_refresh_lock:
                if cls.repair_refresh_last_error:
                    raise PortalError(cls.repair_refresh_last_error)
                if not cls.repair_refresh_last_result:
                    raise PortalError("检修源表刷新未返回结果，请稍后重试。")
                result = copy.deepcopy(cls.repair_refresh_last_result)
                result["repair_refresh_reused"] = True
                return result

        try:
            result = cls.service.refresh_repair_source()
            if not isinstance(result, dict):
                result = {}
            result = copy.deepcopy(result)
            result["repair_refresh_reused"] = False
            cls.clear_payload_cache()
            with cls.repair_refresh_lock:
                cls.repair_refresh_last_result = copy.deepcopy(result)
                cls.repair_refresh_last_error = ""
                cls.repair_refresh_last_finished = time.monotonic()
            return result
        except Exception as exc:
            error = str(exc)
            with cls.repair_refresh_lock:
                cls.repair_refresh_last_error = error
                cls.repair_refresh_last_finished = time.monotonic()
            if isinstance(exc, PortalError):
                raise
            raise PortalError(error) from exc
        finally:
            with cls.repair_refresh_lock:
                cls.repair_refresh_inflight = False
                event.set()

    def _reconcile_orphan_started_items(
        self, scope: str, ongoing: list[dict] | None, *, force: bool = False
    ) -> None:
        scope = PortalHandler.auth_manager.normalize_scope(scope)
        now = time.monotonic()
        with PortalHandler.orphan_reconcile_lock:
            if scope in PortalHandler.orphan_reconcile_pending:
                return
            last = float(PortalHandler.orphan_reconcile_last.get(scope) or 0)
            if not force and now - last < PortalHandler.orphan_reconcile_interval_s:
                return
            PortalHandler.orphan_reconcile_pending.add(scope)
            PortalHandler.orphan_reconcile_last[scope] = now
        ongoing_copy = [
            dict(item) for item in (ongoing or []) if isinstance(item, dict)
        ]

        def _worker() -> None:
            try:
                result = self.service.reconcile_orphan_started_items(
                    scope=scope, ongoing_items=ongoing_copy
                )
                if int((result or {}).get("removed") or 0) > 0:
                    PortalHandler.clear_payload_cache()
            except Exception as exc:
                warning = f"本地已开始状态清理失败: {exc}"
                if warning not in self.service._load_warnings:
                    self.service._load_warnings.append(warning)
            finally:
                with PortalHandler.orphan_reconcile_lock:
                    PortalHandler.orphan_reconcile_pending.discard(scope)

        try:
            threading.Thread(
                target=_worker,
                name=f"LANOrphanReconcile-{scope}",
                daemon=True,
            ).start()
        except Exception as exc:
            with PortalHandler.orphan_reconcile_lock:
                PortalHandler.orphan_reconcile_pending.discard(scope)
            warning = f"本地已开始状态清理启动失败: {exc}"
            if warning not in self.service._load_warnings:
                self.service._load_warnings.append(warning)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if self._redirect_root_oauth_callback(parsed):
            return
        if parsed.path == "/":
            return self._send_html(STATIC_DIR / "index.html")
        if parsed.path.startswith("/assets/"):
            relative = Path(*parsed.path.lstrip("/").split("/"))
            return self._send_static_file(STATIC_DIR / relative)
        if parsed.path == "/api/auth/status":
            session = self._current_session()
            data = PortalHandler.auth_manager.public_status(
                session,
                next_path=(parse_qs(parsed.query).get("next") or [self._request_target()])[0],
                redirect_uri=f"{self._request_base_url()}/api/auth/feishu/callback",
            )
            return self._send_json(200, {"ok": True, "data": data})
        if parsed.path == "/api/auth/login":
            qs = parse_qs(parsed.query)
            next_path = (qs.get("next") or ["/"])[0]
            redirect_uri = f"{self._request_base_url()}/api/auth/feishu/callback"
            try:
                login_url = PortalHandler.auth_manager.start_login(
                    redirect_uri=redirect_uri,
                    next_path=next_path,
                )
            except PortalError as exc:
                return self._send_html_message(500, "飞书登录未启用", str(exc))
            return self._send_redirect(login_url)
        if parsed.path == "/api/auth/feishu/callback":
            qs = parse_qs(parsed.query)
            code = (qs.get("code") or [""])[0]
            state = (qs.get("state") or [""])[0]
            redirect_uri = f"{self._request_base_url()}/api/auth/feishu/callback"
            try:
                session_id, next_path = PortalHandler.auth_manager.complete_login(
                    code=code,
                    state=state,
                    redirect_uri=redirect_uri,
                )
            except PortalError as exc:
                return self._send_html_message(400, "飞书登录失败", str(exc))
            return self._send_redirect(
                next_path,
                set_cookie=PortalHandler.auth_manager.cookie_header(session_id),
            )
        if parsed.path == "/api/auth/logout":
            PortalHandler.auth_manager.clear_session(
                self._cookie_value(AUTH_COOKIE_NAME)
            )
            return self._send_redirect(
                "/",
                set_cookie=PortalHandler.auth_manager.clear_cookie_header(),
            )
        session = self._require_auth_json()
        if session is None:
            return
        if parsed.path == "/api/bootstrap":
            qs = parse_qs(parsed.query)
            scope = (qs.get("scope") or ["ALL"])[0]
            try:
                scope = self._authorized_scope_or_error(session, scope)
                ongoing = self._get_ongoing(scope)
                self._reconcile_orphan_started_items(scope, ongoing)
                open_id = str((session.get("user") or {}).get("open_id") or "")
                data = self._cached_service_payload(
                    (
                        "bootstrap",
                        open_id,
                        scope,
                        self._ongoing_items_marker(ongoing),
                    ),
                    lambda: self.service.get_bootstrap(
                        scope=scope, ongoing_items=ongoing
                    ),
                )
                return self._send_json(
                    200,
                    {
                        "ok": True,
                        "data": self._with_auth_context(
                            self._with_runtime_warnings(data), session
                        ),
                    },
                )
            except PortalError as exc:
                return self._send_json(403, {"ok": False, "error": str(exc)})
        if parsed.path == "/api/scope-overview":
            try:
                ongoing = self._get_ongoing("ALL")
                self._reconcile_orphan_started_items("ALL", ongoing)
                allowed_options = PortalHandler.auth_manager.filter_scope_options(
                    SCOPE_OPTIONS, session
                )
                allowed_scopes = [
                    str(option.get("value") or "")
                    for option in allowed_options
                    if str(option.get("value") or "").strip()
                ]
                open_id = str((session.get("user") or {}).get("open_id") or "")
                data = self._cached_service_payload(
                    (
                        "scope-overview",
                        open_id,
                        tuple(sorted(allowed_scopes)),
                        self._ongoing_items_marker(ongoing),
                    ),
                    lambda: self.service.get_scope_overview(
                        ongoing_items=ongoing,
                        scopes=allowed_scopes,
                        include_prepared=False,
                    ),
                )
                data = PortalHandler.auth_manager.filter_scope_overview(data, session)
                return self._send_json(
                    200,
                    {
                        "ok": True,
                        "data": self._with_auth_context(
                            self._with_runtime_warnings(data), session
                        ),
                    },
                )
            except PortalError as exc:
                return self._send_json(500, {"ok": False, "error": str(exc)})
        if parsed.path == "/api/history-summary":
            qs = parse_qs(parsed.query)
            try:
                scope = self._authorized_scope_or_error(
                    session, (qs.get("scope") or ["ALL"])[0]
                )
                payload = self.service.get_history_summary(
                    scope=scope,
                    month=(qs.get("month") or [""])[0],
                    work_type=(qs.get("work_type") or ["all"])[0],
                )
                return self._send_json(
                    200,
                    {
                        "ok": True,
                        "data": self._with_auth_context(
                            self._with_runtime_warnings(payload), session
                        ),
                    },
                )
            except PortalError as exc:
                return self._send_json(403, {"ok": False, "error": str(exc)})
        if parsed.path == "/api/handover-links":
            try:
                data = PortalHandler.auth_manager.filter_handover_links(
                    self.service.get_handover_links(), session
                )
                return self._send_json(
                    200,
                    {"ok": True, "data": self._with_auth_context(data, session)},
                )
            except PortalError as exc:
                return self._send_json(500, {"ok": False, "error": str(exc)})
        if parsed.path == "/api/auth/permissions":
            if not self._require_admin_json(session):
                return
            data = PortalHandler.auth_manager.get_permissions_payload()
            return self._send_json(
                200,
                {"ok": True, "data": self._with_auth_context(data, session)},
            )
        if parsed.path == "/api/auth/permission-requests/current":
            user = session.get("user") if isinstance(session.get("user"), dict) else {}
            request = PortalHandler.auth_manager.get_current_permission_request(
                str(user.get("open_id") or "")
            )
            data = {"request": request or None}
            return self._send_json(
                200,
                {"ok": True, "data": self._with_auth_context(data, session)},
            )
        if parsed.path == "/api/records":
            qs = parse_qs(parsed.query)
            try:
                scope = self._authorized_scope_or_error(
                    session, (qs.get("scope") or ["ALL"])[0]
                )
                ongoing = self._get_ongoing(scope)
                self._reconcile_orphan_started_items(scope, ongoing)
                month = (qs.get("month") or [""])[0]
                specialty = (qs.get("specialty") or [""])[0]
                open_id = str((session.get("user") or {}).get("open_id") or "")
                payload = self._cached_service_payload(
                    (
                        "records",
                        open_id,
                        scope,
                        month,
                        specialty,
                        self._ongoing_items_marker(ongoing),
                    ),
                    lambda: self.service.query_records(
                        month=month,
                        specialty=specialty,
                        scope=scope,
                        ongoing_items=ongoing,
                    ),
                )
                return self._send_json(
                    200,
                    {
                        "ok": True,
                        "data": self._with_auth_context(
                            self._with_runtime_warnings(payload), session
                        ),
                    },
                )
            except PortalError as exc:
                return self._send_json(403, {"ok": False, "error": str(exc)})
        if parsed.path == "/api/workbench":
            qs = parse_qs(parsed.query)
            try:
                scope = self._authorized_scope_or_error(
                    session, (qs.get("scope") or ["ALL"])[0]
                )
                ongoing = self._get_ongoing(scope)
                self._reconcile_orphan_started_items(scope, ongoing)
                month = (qs.get("month") or [""])[0]
                specialty = (qs.get("specialty") or [""])[0]
                open_id = str((session.get("user") or {}).get("open_id") or "")
                payload = self._cached_service_payload(
                    (
                        "workbench",
                        open_id,
                        scope,
                        month,
                        specialty,
                        self._ongoing_items_marker(ongoing),
                    ),
                    lambda: self.service.query_records(
                        month=month,
                        specialty=specialty,
                        scope=scope,
                        ongoing_items=ongoing,
                    ),
                )
                return self._send_json(
                    200,
                    {
                        "ok": True,
                        "data": self._with_auth_context(
                            self._with_runtime_warnings(payload), session
                        ),
                    },
                )
            except PortalError as exc:
                return self._send_json(403, {"ok": False, "error": str(exc)})
        if parsed.path == "/api/refresh":
            if not self._require_admin_json(session):
                return
            qs = parse_qs(parsed.query)
            try:
                scope = self._authorized_scope_or_error(
                    session, (qs.get("scope") or ["ALL"])[0]
                )
                PortalHandler.clear_payload_cache()
                PortalHandler.wake_source_refresh_worker()
                refreshed = True
                ongoing = self._get_ongoing(scope)
                self._reconcile_orphan_started_items(scope, ongoing, force=True)
                data = self.service.get_bootstrap(
                    scope=scope, ongoing_items=ongoing
                )
                data["source_refresh_triggered"] = refreshed
                return self._send_json(
                    200,
                    {
                        "ok": True,
                        "data": self._with_auth_context(
                            self._with_runtime_warnings(data), session
                        ),
                    },
                )
            except PortalError as exc:
                return self._send_json(403, {"ok": False, "error": str(exc)})
        if parsed.path == "/api/repair-refresh":
            qs = parse_qs(parsed.query)
            try:
                scope = self._authorized_scope_or_error(
                    session, (qs.get("scope") or ["ALL"])[0]
                )
                refresh_result = PortalHandler._refresh_repair_source_singleflight()
                ongoing = self._get_ongoing(scope)
                self._reconcile_orphan_started_items(scope, ongoing)
                data = self.service.get_bootstrap(
                    scope=scope, ongoing_items=ongoing
                )
                data.update(refresh_result)
                data["repair_source_refreshed"] = True
                return self._send_json(
                    200,
                    {
                        "ok": True,
                        "data": self._with_auth_context(
                            self._with_runtime_warnings(data), session
                        ),
                    },
                )
            except PortalError as exc:
                return self._send_json(403, {"ok": False, "error": str(exc)})
        if parsed.path.startswith("/api/jobs/"):
            job_id = parsed.path.rsplit("/", 1)[-1].strip()
            job = self.service.get_job(job_id)
            if not job:
                return self._send_json(404, {"ok": False, "error": "任务不存在"})
            if not self._job_visible_to_session(job, session):
                return self._send_json(403, {"ok": False, "error": "无权查看该任务。"})
            return self._send_json(200, {"ok": True, "data": job})
        return self._send_json(404, {"ok": False, "error": "Not Found"})

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/api/auth/logout":
            PortalHandler.auth_manager.clear_session(
                self._cookie_value(AUTH_COOKIE_NAME)
            )
            body = json.dumps({"ok": True, "data": {}}, ensure_ascii=False).encode(
                "utf-8"
            )
            return self._write_response(
                200,
                {
                    "Content-Type": "application/json; charset=utf-8",
                    "Set-Cookie": PortalHandler.auth_manager.clear_cookie_header(),
                },
                body,
            )
        session = self._require_auth_json()
        if session is None:
            return
        if parsed.path in {"/api/maintenance-actions", "/api/workbench-actions"}:
            if PortalHandler.maintenance_action_callback is None:
                return self._send_json(
                    503,
                    {"ok": False, "error": "主窗口未连接，无法上传多维。"},
                )
            try:
                payload = self._read_json_body()
                scope = self._authorized_scope_or_error(
                    session, payload.get("scope") or "ALL"
                )
                payload["scope"] = scope
                payload["_auth_open_id"] = str((session.get("user") or {}).get("open_id") or "")
                payload["_auth_user_name"] = str(
                    (session.get("user") or {}).get("name")
                    or (session.get("user") or {}).get("en_name")
                    or ""
                )
                job_id, should_start = self.service.create_action_job(payload)
                if should_start:
                    PortalHandler.clear_payload_cache()
                    PortalHandler.enqueue_initial_message_or_upload_job(job_id)
                job = self.service.get_job(job_id) or {}
                return self._send_json(
                    202,
                    {
                        "ok": True,
                        "data": {
                            "job_id": job_id,
                            "accepted_at": job.get("accepted_at") or 0,
                            "initial_phase": job.get("phase") or "accepted",
                        },
                    },
                )
            except (PortalError, ValueError, json.JSONDecodeError) as exc:
                return self._send_json(403, {"ok": False, "error": str(exc)})
        if parsed.path == "/api/ongoing-items/delete":
            try:
                payload = self._read_json_body()
                scope = self._authorized_scope_or_error(
                    session, payload.get("scope") or "ALL"
                )
                payload["scope"] = scope
                payload["_auth_open_id"] = str((session.get("user") or {}).get("open_id") or "")
                payload["_auth_user_name"] = str(
                    (session.get("user") or {}).get("name")
                    or (session.get("user") or {}).get("en_name")
                    or ""
                )
                self.service.validate_ongoing_delete_item(payload, scope=scope)
                callback = PortalHandler.ongoing_delete_callback
                if callback is None:
                    return self._send_json(
                        503,
                        {"ok": False, "error": "主窗口未连接，无法删除进行中通告。"},
                    )
                try:
                    accepted = callback(payload)
                except Exception as exc:
                    return self._send_json(500, {"ok": False, "error": str(exc)})
                if isinstance(accepted, dict):
                    qt_deleted = bool(accepted.get("ok"))
                    error = str(accepted.get("error") or "").strip()
                else:
                    qt_deleted = bool(accepted)
                    error = ""
                if not qt_deleted:
                    return self._send_json(
                        409,
                        {"ok": False, "error": error or "Qt 主界面拒绝删除该条目。"},
                    )
                data = self.service.hide_ongoing_item(
                    payload,
                    scope=scope,
                    deleted_by=payload["_auth_open_id"],
                )
                data.update(
                    self.service.discard_deleted_ongoing_state(payload, scope=scope)
                )
                PortalHandler.clear_payload_cache()
                data["qt_deleted"] = True
                data["remote_deleted"] = bool(
                    accepted.get("remote_deleted")
                    if isinstance(accepted, dict)
                    else False
                )
                return self._send_json(200, {"ok": True, "data": data})
            except (PortalError, ValueError, json.JSONDecodeError) as exc:
                return self._send_json(403, {"ok": False, "error": str(exc)})
        if parsed.path == "/api/handover-links":
            if not self._require_admin_json(session):
                return
            try:
                payload = self._read_json_body()
                data = self.service.save_handover_links(
                    payload.get("links") or {},
                    password=str(payload.get("password") or ""),
                )
                data = PortalHandler.auth_manager.filter_handover_links(data, session)
                return self._send_json(
                    200, {"ok": True, "data": self._with_auth_context(data, session)}
                )
            except (PortalError, ValueError, json.JSONDecodeError) as exc:
                return self._send_json(400, {"ok": False, "error": str(exc)})
        if parsed.path == "/api/handover-links-auth":
            if not self._require_admin_json(session):
                return
            try:
                payload = self._read_json_body()
                ok = self.service.verify_handover_settings_password(
                    str(payload.get("password") or "")
                )
                if not ok:
                    return self._send_json(403, {"ok": False, "error": "设置密码错误。"})
                return self._send_json(200, {"ok": True, "data": {"authorized": True}})
            except (PortalError, ValueError, json.JSONDecodeError) as exc:
                return self._send_json(400, {"ok": False, "error": str(exc)})
        if parsed.path == "/api/handover-password-reset/request":
            if not self._require_admin_json(session):
                return
            try:
                data = self.service.request_handover_password_reset()
                return self._send_json(200, {"ok": True, "data": data})
            except (PortalError, ValueError, json.JSONDecodeError) as exc:
                return self._send_json(400, {"ok": False, "error": str(exc)})
        if parsed.path == "/api/handover-password-reset/confirm":
            if not self._require_admin_json(session):
                return
            try:
                payload = self._read_json_body()
                data = self.service.reset_handover_password_with_code(
                    reset_id=str(payload.get("reset_id") or ""),
                    code=str(payload.get("code") or ""),
                    new_password=str(payload.get("new_password") or ""),
                )
                return self._send_json(200, {"ok": True, "data": data})
            except (PortalError, ValueError, json.JSONDecodeError) as exc:
                return self._send_json(400, {"ok": False, "error": str(exc)})
        if parsed.path == "/api/auth/permission-requests":
            try:
                payload = self._read_json_body()
                user = session.get("user") if isinstance(session.get("user"), dict) else {}
                data = PortalHandler.auth_manager.create_permission_request(
                    open_id=str(user.get("open_id") or ""),
                    name=str(user.get("name") or user.get("en_name") or "飞书用户"),
                    scopes=payload.get("scopes") or [],
                    reason=str(payload.get("reason") or ""),
                )
                code = str(data.pop("code") or "")
                recipients = PortalHandler.auth_manager.admin_open_ids()
                labels = "、".join(data.get("requested_scope_labels") or [])
                reason = str(data.get("reason") or "").strip() or "未填写"
                text = (
                    "南通基地-运维灯塔工作台权限申请。\n"
                    f"申请人：{data.get('name') or '飞书用户'}\n"
                    f"openid：{data.get('open_id') or ''}\n"
                    f"申请范围：{labels}\n"
                    f"申请原因：{reason}\n"
                    f"申请编号：{data.get('request_id') or ''}\n"
                    f"验证码：{code}\n"
                    f"有效期至：{data.get('expires_at') or ''}\n"
                    "请管理员确认申请人身份和申请范围后，再将验证码告知申请人。"
                )
                ok, message, _ = send_text_to_open_ids(text, recipients)
                if not ok:
                    PortalHandler.auth_manager.mark_permission_request_notify_failed(
                        str(data.get("request_id") or "")
                    )
                    raise PortalError(f"通知管理员失败: {message}")
                activated = PortalHandler.auth_manager.activate_permission_request(
                    str(data.get("request_id") or "")
                )
                data.update(activated)
                PortalHandler.auth_manager.supersede_other_permission_requests(
                    open_id=str(data.get("open_id") or ""),
                    keep_request_id=str(data.get("request_id") or ""),
                )
                data["notification"] = {
                    "ok": True,
                    "recipients_count": len(recipients),
                    "message": "已发送给管理员",
                }
                return self._send_json(
                    200,
                    {"ok": True, "data": self._with_auth_context({"request": data}, session)},
                )
            except (PortalError, ValueError, json.JSONDecodeError) as exc:
                return self._send_json(400, {"ok": False, "error": str(exc)})
        if parsed.path == "/api/auth/permission-requests/confirm":
            try:
                payload = self._read_json_body()
                user = session.get("user") if isinstance(session.get("user"), dict) else {}
                permissions = PortalHandler.auth_manager.confirm_permission_request(
                    open_id=str(user.get("open_id") or ""),
                    request_id=str(payload.get("request_id") or ""),
                    code=str(payload.get("code") or ""),
                    updated_by=str(user.get("open_id") or "permission_request"),
                )
                refreshed_session = self._current_session() or session
                data = {
                    "approved": True,
                    "permissions": permissions,
                }
                return self._send_json(
                    200,
                    {
                        "ok": True,
                        "data": self._with_auth_context(data, refreshed_session),
                    },
                )
            except (PortalError, ValueError, json.JSONDecodeError) as exc:
                return self._send_json(400, {"ok": False, "error": str(exc)})
        if parsed.path == "/api/auth/permissions":
            if not self._require_admin_json(session):
                return
            try:
                payload = self._read_json_body()
                actor = session.get("user") if isinstance(session.get("user"), dict) else {}
                data, changed_open_ids = PortalHandler.auth_manager.save_permissions_payload(
                    payload.get("users") or [],
                    updated_by=str(actor.get("open_id") or ""),
                )
                if changed_open_ids:
                    actor_name = str(actor.get("name") or actor.get("en_name") or "管理员")
                    text = (
                        "南通基地-运维灯塔工作台权限已更新。\n"
                        f"操作人：{actor_name}\n"
                        "请重新进入门户或刷新页面查看最新楼栋权限。"
                    )
                    threading.Thread(
                        target=send_text_to_open_ids,
                        args=(text, changed_open_ids),
                        name="LANPortalPermissionNotify",
                        daemon=True,
                    ).start()
                    notify_message = "已加入飞书通知队列"
                else:
                    notify_message = "无权限变更通知"
                data["changed_open_ids"] = changed_open_ids
                data["notification"] = {
                    "ok": True,
                    "message": notify_message,
                    "recipients_count": len(changed_open_ids),
                }
                return self._send_json(
                    200,
                    {"ok": True, "data": self._with_auth_context(data, session)},
                )
            except (PortalError, ValueError, json.JSONDecodeError) as exc:
                return self._send_json(400, {"ok": False, "error": str(exc)})
        if parsed.path == "/api/generate":
            try:
                payload = self._read_json_body()
                scope = self._authorized_scope_or_error(
                    session, payload.get("scope") or PortalHandler.auth_manager.default_scope(session) or "ALL"
                )
                drafts = payload.get("drafts") or []
                self.service.assert_generated_drafts_allowed(drafts, scope=scope)
                generated = self.service.generate_templates(drafts)
                return self._send_json(200, {"ok": True, "data": {"items": generated}})
            except (PortalError, ValueError, json.JSONDecodeError) as exc:
                return self._send_json(403, {"ok": False, "error": str(exc)})
        if parsed.path == "/api/send-generated":
            if PortalHandler.notice_callback is None:
                return self._send_json(
                    503,
                    {
                        "ok": False,
                        "error": "主窗口未连接，无法发送后回填主界面。",
                    },
                )
            try:
                payload = self._read_json_body()
                scope = self._authorized_scope_or_error(
                    session, payload.get("scope") or PortalHandler.auth_manager.default_scope(session) or "ALL"
                )
                items = payload.get("items") or []
                self.service.assert_generated_items_allowed(items, scope=scope)
                results = self.service.send_generated_templates(
                    items, notice_callback=PortalHandler.notice_callback
                )
                return self._send_json(200, {"ok": True, "data": {"items": results}})
            except (PortalError, ValueError, json.JSONDecodeError) as exc:
                return self._send_json(403, {"ok": False, "error": str(exc)})
        return self._send_json(404, {"ok": False, "error": "Not Found"})

    def _get_ongoing(self, scope: str) -> list[dict]:
        try:
            snapshot = PortalHandler.state_store.get_ongoing_snapshot()
            if snapshot.get("exists"):
                PortalHandler.last_ongoing_error = ""
                return [
                    dict(item)
                    for item in snapshot.get("items", [])
                    if isinstance(item, dict)
                    and self.service._scope_matches_item(scope, item)
                ]
        except Exception as exc:
            warning = f"SQLite 进行中状态读取失败: {exc}"
            PortalHandler.last_ongoing_error = warning
            logging.warning(warning)
        callback = PortalHandler.ongoing_callback
        if callback is None:
            if not PortalHandler.last_ongoing_error:
                PortalHandler.last_ongoing_error = ""
            return []
        try:
            result = callback("ALL")
            if isinstance(result, list):
                try:
                    PortalHandler.state_store.replace_ongoing_items(result)
                except Exception as store_exc:
                    logging.warning(f"SQLite 进行中状态写入失败: {store_exc}")
            PortalHandler.last_ongoing_error = ""
        except Exception as exc:
            warning = f"主界面进行中状态读取失败: {exc}"
            PortalHandler.last_ongoing_error = warning
            logging.warning(warning)
            return []
        if not isinstance(result, list):
            return []
        return [
            dict(item)
            for item in result
            if isinstance(item, dict) and self.service._scope_matches_item(scope, item)
        ]

    @classmethod
    def ensure_source_refresh_worker(cls) -> None:
        with cls.source_refresh_lock:
            if cls.source_refresh_thread and cls.source_refresh_thread.is_alive():
                return
            cls.source_refresh_stop = False
            cls.source_refresh_event.clear()
            cls.source_refresh_thread = threading.Thread(
                target=cls._source_refresh_loop,
                name="LANSourceSnapshotRefresh",
                daemon=True,
            )
            cls.source_refresh_thread.start()

    @classmethod
    def stop_source_refresh_worker(cls) -> None:
        with cls.source_refresh_lock:
            cls.source_refresh_stop = True
            cls.source_refresh_event.set()
            worker = cls.source_refresh_thread
        if worker and worker.is_alive():
            try:
                worker.join(timeout=2)
            except Exception:
                pass

    @classmethod
    def wake_source_refresh_worker(cls) -> None:
        cls.source_refresh_event.set()

    @classmethod
    def _source_refresh_loop(cls) -> None:
        next_wait = 0.0
        while True:
            if cls.source_refresh_event.wait(timeout=next_wait):
                cls.source_refresh_event.clear()
            with cls.source_refresh_lock:
                if cls.source_refresh_stop:
                    return
            try:
                cls.service.refresh()
                cls.clear_payload_cache()
            except Exception as exc:
                warning = f"源表后台同步失败: {exc}"
                if warning not in cls.service._load_warnings:
                    cls.service._load_warnings.append(warning)
            try:
                cls.service.process_due_repair_link_tasks(limit=3)
            except Exception as exc:
                warning = f"检修源表关联补写失败: {exc}"
                if warning not in cls.service._load_warnings:
                    cls.service._load_warnings.append(warning)
            try:
                ttl = int(cls.service._source_cache_ttl_seconds())
            except Exception:
                ttl = SOURCE_CACHE_TTL_SECONDS
            next_wait = max(60.0, float(ttl or SOURCE_CACHE_TTL_SECONDS))

    @classmethod
    def ensure_message_workers(cls) -> None:
        with cls.message_queue_lock:
            cls.message_worker_threads = [
                worker
                for worker in cls.message_worker_threads
                if worker and worker.is_alive()
            ]
            if len(cls.message_worker_threads) >= cls.message_worker_count:
                return
            cls.message_worker_stop = False
            missing = cls.message_worker_count - len(cls.message_worker_threads)
            for _ in range(max(0, missing)):
                worker = threading.Thread(
                    target=cls._message_worker_loop,
                    name="LANPersonalMessageQueue",
                    daemon=True,
                )
                cls.message_worker_threads.append(worker)
                worker.start()

    @classmethod
    def stop_message_workers(cls) -> None:
        with cls.message_queue_lock:
            cls.message_worker_stop = True
            cls.message_queue.clear()
            cls.message_queue_event.set()
            workers = list(cls.message_worker_threads)
        for worker in workers:
            if worker and worker.is_alive():
                try:
                    worker.join(timeout=2)
                except Exception:
                    pass
        with cls.message_queue_lock:
            cls.message_worker_threads = []

    @classmethod
    def enqueue_initial_message_or_upload_job(cls, job_id: str) -> None:
        job = cls.service.get_job(job_id) or {}
        request = job.get("request") if isinstance(job.get("request"), dict) else {}
        work_type = str((request or {}).get("work_type") or "maintenance").strip()
        if work_type in {"change", "repair"}:
            cls.enqueue_action_job(job_id)
            return
        cls.enqueue_message_job(job_id)

    @classmethod
    def enqueue_message_job(cls, job_id: str) -> None:
        job_id = str(job_id or "").strip()
        if not job_id:
            return
        with cls.message_queue_lock:
            if job_id not in cls.message_queue:
                cls.message_queue.append(job_id)
            cls._update_message_queue_positions_locked()
        cls.ensure_message_workers()
        cls.message_queue_event.set()

    @classmethod
    def _update_message_queue_positions_locked(cls) -> None:
        total = len(cls.message_queue)
        for index, queued_job_id in enumerate(cls.message_queue, start=1):
            cls.service.mark_job(
                queued_job_id,
                phase="accepted",
                message_queue_position=index,
                message_queue_size=total,
                _persist=False,
            )

    @classmethod
    def _message_worker_loop(cls) -> None:
        while True:
            cls.message_queue_event.wait(timeout=1)
            if cls.message_worker_stop:
                return
            job_id = ""
            with cls.message_queue_lock:
                if cls.message_queue:
                    job_id = cls.message_queue.pop(0)
                    cls._update_message_queue_positions_locked()
                else:
                    cls.message_queue_event.clear()
                    continue
            if not job_id:
                continue
            cls._process_initial_message_job(job_id)

    @classmethod
    def _process_initial_message_job(cls, job_id: str) -> None:
        try:
            prepared = cls.service.prepare_action_job(job_id)
            if prepared.get("skip_personal_message"):
                cls.enqueue_action_job(job_id)
                return
            message_signature = str(prepared.get("message_signature") or "")
            if prepared.get("message_sent"):
                cls.service.mark_job(
                    job_id,
                    phase="upload_queued",
                    message_sent=True,
                    message_signature=message_signature,
                    message_queue_position=0,
                    queue_position=0,
                )
                cls.enqueue_action_job(job_id)
                return
            cls.service.mark_job(
                job_id,
                phase="sending_message",
                message_queue_position=0,
                queue_position=0,
            )
            ok, message = cls.service.send_action_personal_message(prepared)
            if not ok:
                cls.service.mark_job(
                    job_id,
                    phase="failed",
                    error=message,
                    message_sent=False,
                )
                return
            cls.service.mark_job(
                job_id,
                phase="upload_queued",
                message_sent=True,
                message_signature=message_signature,
                message_queue_position=0,
                queue_position=0,
            )
            cls.enqueue_action_job(job_id)
        except Exception as exc:
            cls.service.mark_job(job_id, phase="failed", error=str(exc))

    @classmethod
    def ensure_action_worker(cls) -> None:
        with cls.action_queue_lock:
            if cls.action_worker_thread and cls.action_worker_thread.is_alive():
                return
            cls.action_worker_stop = False
            cls.action_queue_event.clear()
            cls.action_worker_thread = threading.Thread(
                target=cls._action_worker_loop,
                name="LANMaintenanceActionQueue",
                daemon=True,
            )
            cls.action_worker_thread.start()

    @classmethod
    def stop_action_worker(cls) -> None:
        with cls.action_queue_lock:
            cls.action_worker_stop = True
            cls.action_queue.clear()
            cls._update_queue_positions_locked()
            cls.action_queue_event.set()
            worker = cls.action_worker_thread
        if worker and worker.is_alive():
            try:
                worker.join(timeout=2)
            except Exception:
                pass

    @classmethod
    def enqueue_action_job(cls, job_id: str) -> None:
        job_id = str(job_id or "").strip()
        if not job_id:
            return
        with cls.action_queue_lock:
            if job_id not in cls.action_queue:
                cls.action_queue.append(job_id)
            cls._update_queue_positions_locked()
        cls.ensure_action_worker()
        cls.action_queue_event.set()

    @classmethod
    def _update_queue_positions_locked(cls) -> None:
        total = len(cls.action_queue)
        for index, queued_job_id in enumerate(cls.action_queue, start=1):
            cls.service.mark_job(
                queued_job_id,
                phase="upload_queued",
                queue_position=index,
                queue_size=total,
                upload_queue_position=index,
                upload_queue_size=total,
                _persist=False,
            )

    @classmethod
    def _action_worker_loop(cls) -> None:
        while True:
            cls.action_queue_event.wait(timeout=1)
            if cls.action_worker_stop:
                return
            job_id = ""
            with cls.action_queue_lock:
                if cls.action_queue:
                    job_id = cls.action_queue.pop(0)
                    cls._update_queue_positions_locked()
                else:
                    cls.action_queue_event.clear()
                    continue
            if not job_id:
                continue
            cls.service.mark_job(job_id, queue_position=0, upload_queue_position=0)
            cls._process_maintenance_action_job(job_id)
            cls.track_upload_wait_job(job_id)

    @classmethod
    def ensure_upload_wait_worker(cls) -> None:
        with cls.upload_wait_lock:
            if cls.upload_wait_thread and cls.upload_wait_thread.is_alive():
                return
            cls.upload_wait_stop = False
            cls.upload_wait_event.clear()
            cls.upload_wait_thread = threading.Thread(
                target=cls._upload_wait_worker_loop,
                name="LANUploadResultMonitor",
                daemon=True,
            )
            cls.upload_wait_thread.start()

    @classmethod
    def stop_upload_wait_worker(cls) -> None:
        with cls.upload_wait_lock:
            cls.upload_wait_stop = True
            cls.upload_wait_jobs.clear()
            cls.upload_wait_event.set()
            worker = cls.upload_wait_thread
        if worker and worker.is_alive():
            try:
                worker.join(timeout=2)
            except Exception:
                pass
        with cls.upload_wait_lock:
            cls.upload_wait_thread = None
            cls.upload_wait_event.clear()

    @classmethod
    def track_upload_wait_job(cls, job_id: str) -> None:
        job_id = str(job_id or "").strip()
        if not job_id:
            return
        job = cls.service.get_job(job_id) or {}
        if str(job.get("phase") or "") != "uploading":
            return
        cls.ensure_upload_wait_worker()
        with cls.upload_wait_lock:
            cls.upload_wait_jobs[job_id] = time.monotonic() + cls.action_upload_timeout_s
            cls.upload_wait_event.set()

    @classmethod
    def _upload_wait_worker_loop(cls) -> None:
        while True:
            cls.upload_wait_event.wait(timeout=1.2)
            if cls.upload_wait_stop or cls.action_worker_stop:
                return
            now = time.monotonic()
            to_remove: list[str] = []
            timed_out: list[str] = []
            with cls.upload_wait_lock:
                items = list(cls.upload_wait_jobs.items())
            for job_id, deadline in items:
                job = cls.service.get_job(job_id) or {}
                phase = str(job.get("phase") or "")
                if phase in {"success", "failed"} or phase != "uploading":
                    to_remove.append(job_id)
                    continue
                if now >= deadline:
                    to_remove.append(job_id)
                    timed_out.append(job_id)
            if to_remove:
                with cls.upload_wait_lock:
                    for job_id in to_remove:
                        cls.upload_wait_jobs.pop(job_id, None)
            for job_id in timed_out:
                cls.service.mark_job(
                    job_id,
                    phase="failed",
                    error="上传多维超时，请在主界面确认实际结果后重试。",
                )
            with cls.upload_wait_lock:
                if not cls.upload_wait_jobs:
                    cls.upload_wait_event.clear()

    @classmethod
    def _process_maintenance_action_job(cls, job_id: str) -> None:
        try:
            prepared = cls.service.prepare_action_job(job_id)
            if (
                not prepared.get("skip_personal_message")
                and not prepared.get("message_sent")
            ):
                cls.enqueue_message_job(job_id)
                return
            if prepared.get("skip_personal_message"):
                cls.service.mark_job(
                    job_id,
                    phase="uploading",
                    queue_position=0,
                    message_sent=True,
                    message_signature=str(prepared.get("message_signature") or ""),
                )
            else:
                cls.service.mark_job(
                    job_id,
                    phase="uploading",
                    queue_position=0,
                )
            if not prepared.get("skip_personal_message"):
                cls.service.mark_job(
                    job_id,
                    phase="uploading",
                    message_sent=True,
                    message_signature=str(prepared.get("message_signature") or ""),
                )
            callback = cls.maintenance_action_callback
            if callback is None:
                cls.service.mark_job(
                    job_id, phase="failed", error="主窗口未连接，无法上传多维。"
                )
                return
            accepted = callback(prepared)
            if isinstance(accepted, dict):
                ok = bool(accepted.get("ok"))
                error = str(accepted.get("error") or "").strip()
            else:
                ok = bool(accepted)
                error = ""
            if not ok:
                cls.service.mark_job(
                    job_id,
                    phase="failed",
                    error=error or "主窗口拒绝执行本次上传。",
                    message_sent=True,
                )
        except Exception as exc:
            cls.service.mark_job(job_id, phase="failed", error=str(exc))

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return


class PortalServerController:
    def __init__(
        self,
        *,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
        app_token: str = DEFAULT_APP_TOKEN,
        table_id: str = DEFAULT_TABLE_ID,
    ) -> None:
        self.host = str(host or DEFAULT_HOST).strip() or DEFAULT_HOST
        self.preferred_port = int(port or DEFAULT_PORT)
        self.app_token = str(app_token or DEFAULT_APP_TOKEN).strip()
        self.table_id = str(table_id or DEFAULT_TABLE_ID).strip()
        self.server: ThreadingHTTPServer | None = None
        self.thread: threading.Thread | None = None
        self.bound_port: int | None = None
        self.notice_callback = None
        self.ongoing_callback = None
        self.ongoing_delete_callback = None
        self.maintenance_action_callback = None

    def start(self) -> str:
        if self.server and self.thread and self.thread.is_alive():
            return self.get_url()
        bound_port = find_available_port(self.host, self.preferred_port)
        PortalHandler.service = MaintenancePortalService(
            app_token=self.app_token,
            table_id=self.table_id,
        )
        try:
            PortalHandler.service.ensure_snapshot_loaded()
        except Exception:
            pass
        PortalHandler.auth_manager = PortalAuthManager()
        PortalHandler.state_store = LanPortalStateStore()
        PortalHandler.notice_callback = self.notice_callback
        PortalHandler.ongoing_callback = self.ongoing_callback
        PortalHandler.ongoing_delete_callback = self.ongoing_delete_callback
        PortalHandler.maintenance_action_callback = self.maintenance_action_callback
        with PortalHandler.repair_refresh_lock:
            PortalHandler.repair_refresh_inflight = False
            PortalHandler.repair_refresh_event = threading.Event()
            PortalHandler.repair_refresh_last_result = {}
            PortalHandler.repair_refresh_last_error = ""
            PortalHandler.repair_refresh_last_finished = 0.0
        with PortalHandler.action_queue_lock:
            PortalHandler.action_queue.clear()
            PortalHandler.action_worker_stop = False
            PortalHandler.action_queue_event.clear()
        with PortalHandler.upload_wait_lock:
            PortalHandler.upload_wait_jobs.clear()
            PortalHandler.upload_wait_stop = False
            PortalHandler.upload_wait_event.clear()
        with PortalHandler.message_queue_lock:
            PortalHandler.message_queue.clear()
            PortalHandler.message_worker_stop = False
            PortalHandler.message_queue_event.clear()
        self.server = ThreadingHTTPServer((self.host, bound_port), PortalHandler)
        self.thread = threading.Thread(
            target=self.server.serve_forever,
            name="LANBitableTemplatePortal",
            daemon=True,
        )
        self.thread.start()
        PortalHandler.ensure_message_workers()
        PortalHandler.ensure_action_worker()
        PortalHandler.ensure_source_refresh_worker()
        self.bound_port = bound_port
        return self.get_url()

    def get_url(self) -> str:
        port = self.bound_port or self.preferred_port
        display_host = self.host if self.host != "0.0.0.0" else "127.0.0.1"
        return f"http://{display_host}:{port}"

    def set_notice_callback(self, callback) -> None:
        self.notice_callback = callback
        PortalHandler.notice_callback = callback

    def set_ongoing_callback(self, callback) -> None:
        self.ongoing_callback = callback
        PortalHandler.ongoing_callback = callback

    def set_ongoing_delete_callback(self, callback) -> None:
        self.ongoing_delete_callback = callback
        PortalHandler.ongoing_delete_callback = callback

    def set_maintenance_action_callback(self, callback) -> None:
        self.maintenance_action_callback = callback
        PortalHandler.maintenance_action_callback = callback

    def mark_job_upload_result(
        self,
        job_id: str,
        *,
        success: bool,
        message: str = "",
        record_id: str = "",
        active_item_id: str = "",
    ) -> None:
        if not job_id:
            return
        PortalHandler.service.mark_action_upload_result(
            job_id,
            success=success,
            message=message,
            record_id=record_id,
            active_item_id=active_item_id,
        )

    def get_job(self, job_id: str) -> dict | None:
        return PortalHandler.service.get_job(job_id)

    def stop(self) -> None:
        if not self.server:
            return
        try:
            self.server.shutdown()
        except Exception:
            pass
        try:
            self.server.server_close()
        except Exception:
            pass
        if self.thread and self.thread.is_alive():
            try:
                self.thread.join(timeout=2)
            except Exception:
                pass
        self.server = None
        self.thread = None
        self.bound_port = None
        PortalHandler.stop_message_workers()
        PortalHandler.stop_action_worker()
        PortalHandler.stop_upload_wait_worker()
        PortalHandler.stop_source_refresh_worker()
        PortalHandler.notice_callback = None
        PortalHandler.ongoing_callback = None
        PortalHandler.ongoing_delete_callback = None
        PortalHandler.maintenance_action_callback = None


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="局域网多维表模板生成门户")
    parser.add_argument("--host", default=DEFAULT_HOST, help="监听地址，默认 0.0.0.0")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="监听端口，默认 18766")
    parser.add_argument(
        "--app-token",
        default=MaintenancePortalService().app_token,
        help="飞书多维表 app_token",
    )
    parser.add_argument(
        "--table-id",
        default=MaintenancePortalService().table_id,
        help="飞书多维表 table_id",
    )
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    controller = PortalServerController(
        host=str(args.host or DEFAULT_HOST).strip() or DEFAULT_HOST,
        port=int(args.port or DEFAULT_PORT),
        app_token=str(args.app_token or "").strip(),
        table_id=str(args.table_id or "").strip(),
    )
    url = controller.start()
    print(f"局域网模板门户已启动: {url}")
    print("如需局域网访问，请将 127.0.0.1 替换为本机局域网 IP。")
    try:
        assert controller.thread is not None
        controller.thread.join()
    except KeyboardInterrupt:
        controller.stop()


if __name__ == "__main__":
    main()
