# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import base64
import copy
import hashlib
import html
import json
import logging
import mimetypes
import os
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
    external_real_write_guard,
)
from .state_store import LanPortalStateStore
from upload_event_module.services.handlers import NoticePayload
from upload_event_module.services.service_registry import (
    create_bitable_record_by_payload,
    query_record_by_id,
    upload_media_to_feishu,
    update_bitable_record_fields,
    update_bitable_record_by_payload,
)
from upload_event_module.services.feishu_service import delete_bitable_record
from upload_event_module.services.robot_webhook import send_text_to_open_ids


STATIC_DIR = Path(__file__).resolve().parent / "static"
FRONTEND_DIST_DIR = Path(__file__).resolve().parent / "frontend" / "dist"
FRONTEND_READY_MARKER = FRONTEND_DIST_DIR / "clipflow-frontend-ready.json"
DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 18766
CLIENT_DISCONNECT_WINERRORS = {10053, 10054, 10058}
MAX_JSON_BODY_BYTES = 512 * 1024


def portal_frontend_dist_enabled() -> bool:
    if os.environ.get("CLIPFLOW_FRONTEND_LEGACY") == "1":
        return False
    return portal_frontend_dist_ready()


def portal_frontend_dist_ready() -> bool:
    """Return true only when the Vue dist has been marked production-ready.

    The repository may contain a migration/preview Vue build. Serving that build
    to site users is worse than falling back to the legacy page, so the runtime
    requires an explicit marker file produced by the release process.
    """
    if not (FRONTEND_DIST_DIR / "index.html").is_file():
        return False
    try:
        payload = json.loads(FRONTEND_READY_MARKER.read_text(encoding="utf-8"))
    except Exception:
        return False
    if not isinstance(payload, dict):
        return False
    return (
        payload.get("productionReady") is True
        and str(payload.get("app") or "").strip() == "clipflow-lan-portal"
    )


def portal_index_file() -> Path:
    if portal_frontend_dist_enabled():
        return FRONTEND_DIST_DIR / "index.html"
    return STATIC_DIR / "index.html"


def portal_asset_file(relative: Path) -> Path:
    if portal_frontend_dist_enabled():
        candidate = FRONTEND_DIST_DIR / "assets" / relative
        if candidate.is_file():
            return candidate
    return STATIC_DIR / "assets" / relative


def portal_static_roots() -> list[Path]:
    roots = [STATIC_DIR.resolve()]
    if portal_frontend_dist_enabled():
        roots.append(FRONTEND_DIST_DIR.resolve())
    return roots


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


def _send_text_to_open_ids_guarded(text: str, recipients: list[str]) -> tuple[bool, str, list[dict]]:
    clean_recipients = [
        str(open_id or "").strip()
        for open_id in (recipients or [])
        if str(open_id or "").strip()
    ]
    guard = external_real_write_guard()
    if guard.get("mock_external"):
        return True, "mock external send skipped", [
            {"open_id": open_id, "ok": True, "message": "mock external send skipped"}
            for open_id in clean_recipients
        ]
    if not guard.get("real_write_allowed"):
        return False, str(guard.get("reason") or "真实外部写入未确认。"), []
    return send_text_to_open_ids(text, clean_recipients)


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
    message_batch_wait_seconds = 1.2
    message_batch_max_jobs = 20
    message_scope_inflight: set[str] = set()
    payload_cache_lock = threading.RLock()
    payload_cache: dict[tuple, tuple[float, dict]] = {}
    payload_cache_inflight: dict[tuple, threading.Event] = {}
    payload_cache_inflight_started: dict[tuple, float] = {}
    payload_cache_generation = 0
    payload_cache_ttl_s = 5
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
    source_refresh_run_lock = threading.Lock()
    source_refresh_inflight = False
    source_refresh_last_result: dict = {}
    source_refresh_last_finished = 0.0
    repair_refresh_lock = threading.RLock()
    repair_refresh_inflight = False
    repair_refresh_event = threading.Event()
    repair_refresh_last_result: dict = {}
    repair_refresh_last_error = ""
    repair_refresh_last_finished = 0.0
    repair_refresh_reuse_window_s = 10.0
    qt_action_interval_ms = 250
    source_refresh_defer_when_busy = True

    @staticmethod
    def _bounded_int(value, default: int, minimum: int, maximum: int) -> int:
        try:
            number = int(value)
        except Exception:
            number = int(default)
        return max(int(minimum), min(int(maximum), number))

    @staticmethod
    def _bounded_float(value, default: float, minimum: float, maximum: float) -> float:
        try:
            number = float(value)
        except Exception:
            number = float(default)
        return max(float(minimum), min(float(maximum), number))

    @classmethod
    def runtime_limits(cls) -> dict:
        return {
            "message_worker_count": int(cls.message_worker_count or 0),
            "message_batch_wait_seconds": float(
                getattr(cls, "message_batch_wait_seconds", 1.2)
            ),
            "message_batch_max_jobs": int(getattr(cls, "message_batch_max_jobs", 20)),
            "qt_items_per_tick": 1,
            "qt_tick_interval_ms": int(getattr(cls, "qt_action_interval_ms", 250) or 250),
            "upload_seconds_per_record": float(getattr(cls, "upload_seconds_per_record", 2.0)),
            "upload_batch_wait_seconds": float(getattr(cls, "upload_batch_wait_seconds", 30.0)),
            "upload_batch_max_records": int(getattr(cls, "upload_batch_max_records", 5)),
            "source_refresh_minutes": 30,
            "frontend_snapshot_refresh_minutes": 10,
            "source_refresh_defer_when_busy": bool(
                getattr(cls, "source_refresh_defer_when_busy", True)
            ),
        }

    @classmethod
    def apply_runtime_settings(cls) -> dict:
        try:
            settings = cls.state_store.get_settings() or {}
        except Exception:
            settings = {}
        low_performance = (
            str(settings.get("lan_low_performance_mode") or "").strip() in {"1", "true", "是", "开启"}
            or os.environ.get("CLIPFLOW_LOW_PERFORMANCE_MODE") == "1"
        )
        cls.message_worker_count = cls._bounded_int(
            settings.get("lan_message_worker_count"),
            5,
            1,
            5,
        )
        cls.message_batch_wait_seconds = cls._bounded_float(
            settings.get("lan_message_batch_wait_seconds"),
            1.2,
            0.0,
            5.0,
        )
        cls.message_batch_max_jobs = cls._bounded_int(
            settings.get("lan_message_batch_max_jobs"),
            20,
            1,
            100,
        )
        cls.upload_seconds_per_record = cls._bounded_float(
            settings.get("lan_upload_seconds_per_record"),
            5.0 if low_performance else 2.0,
            0.5,
            10.0,
        )
        cls.upload_batch_wait_seconds = cls._bounded_float(
            settings.get("lan_upload_batch_wait_seconds"),
            30.0,
            1.0,
            30.0,
        )
        cls.upload_batch_max_records = cls._bounded_int(
            settings.get("lan_upload_batch_max_records"),
            100,
            1,
            100,
        )
        cls.qt_action_interval_ms = cls._bounded_int(
            settings.get("lan_qt_action_interval_ms"),
            500 if low_performance else 250,
            100,
            3000,
        )
        cls.source_refresh_defer_when_busy = (
            str(settings.get("lan_source_refresh_defer_when_busy") or "").strip()
            not in {"0", "false", "否", "关闭"}
        )
        return cls.runtime_limits()

    @classmethod
    def runtime_pressure(cls) -> dict:
        with cls.message_queue_lock:
            message_queue_size = len(cls.message_queue)
        with cls.action_queue_lock:
            qt_queue_size = len(cls.action_queue)
        with cls.upload_wait_lock:
            upload_wait_size = len(cls.upload_wait_jobs)
        runtime_counts = {}
        try:
            runtime_counts = cls.state_store.runtime_queue_details()
        except Exception:
            runtime_counts = {}
        queued_due = 0
        processing_active = 0
        for details in (runtime_counts or {}).values():
            if not isinstance(details, dict):
                continue
            queued_due += int(details.get("queued_due") or 0)
            processing_active += int(details.get("processing_active") or 0)
        active_jobs = 0
        try:
            with cls.service._jobs_lock:
                for job in cls.service._jobs.values():
                    if str((job or {}).get("phase") or "") not in {"success", "failed"}:
                        active_jobs += 1
        except Exception:
            active_jobs = 0
        busy = any(
            value > 0
            for value in (
                message_queue_size,
                qt_queue_size,
                upload_wait_size,
                queued_due,
                processing_active,
                active_jobs,
            )
        )
        return {
            "busy": bool(busy),
            "message_queue_size": message_queue_size,
            "qt_queue_size": qt_queue_size,
            "upload_wait_size": upload_wait_size,
            "runtime_queue_due": queued_due,
            "runtime_queue_processing": processing_active,
            "active_jobs": active_jobs,
        }

    @classmethod
    def should_defer_source_refresh(cls) -> bool:
        if not bool(getattr(cls, "source_refresh_defer_when_busy", True)):
            return False
        return bool(cls.runtime_pressure().get("busy"))

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
        allowed = False
        for static_root in portal_static_roots():
            try:
                resolved.relative_to(static_root)
                allowed = True
                break
            except ValueError:
                continue
        if not allowed:
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
            for event in cls.payload_cache_inflight.values():
                try:
                    event.set()
                except Exception:
                    pass
            cls.payload_cache_inflight.clear()
            cls.payload_cache_inflight_started.clear()

    @classmethod
    def _prune_payload_cache_locked(cls, now: float) -> None:
        expired_keys = [
            key for key, (expires_at, _payload) in cls.payload_cache.items()
            if expires_at <= now
        ]
        for key in expired_keys:
            cls.payload_cache.pop(key, None)
        stale_inflight_keys = [
            key
            for key, started_at in cls.payload_cache_inflight_started.items()
            if now - float(started_at or 0.0) > 30.0
        ]
        for key in stale_inflight_keys:
            event = cls.payload_cache_inflight.pop(key, None)
            cls.payload_cache_inflight_started.pop(key, None)
            if event is not None:
                try:
                    event.set()
                except Exception:
                    pass
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
                PortalHandler.payload_cache_inflight_started[key] = now
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
                    PortalHandler.payload_cache_inflight_started.pop(key, None)
                    if event is not None:
                        event.set()
            raise
        if not isinstance(payload, dict):
            if owner:
                with PortalHandler.payload_cache_lock:
                    event = PortalHandler.payload_cache_inflight.pop(key, None)
                    PortalHandler.payload_cache_inflight_started.pop(key, None)
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
                    PortalHandler.payload_cache_inflight_started.pop(key, None)
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
                PortalHandler.payload_cache_inflight_started.pop(key, None)
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

    @classmethod
    def request_repair_source_refresh(cls) -> dict:
        """Start repair source refresh in the background and return immediately."""
        if os.environ.get("CLIPFLOW_BACKEND_MOCK_EXTERNAL") == "1":
            return {
                "repair_refresh_started": False,
                "repair_refresh_inflight": False,
                "repair_refresh_reused": False,
                "mock_external": True,
            }
        now = time.monotonic()
        with cls.repair_refresh_lock:
            if (
                cls.repair_refresh_last_result
                and now - float(cls.repair_refresh_last_finished or 0)
                <= float(cls.repair_refresh_reuse_window_s)
            ):
                result = copy.deepcopy(cls.repair_refresh_last_result)
                result["repair_refresh_started"] = False
                result["repair_refresh_inflight"] = False
                result["repair_refresh_reused"] = True
                return result
            if cls.repair_refresh_inflight:
                return {
                    "repair_refresh_started": False,
                    "repair_refresh_inflight": True,
                    "repair_refresh_reused": True,
                }
            event = threading.Event()
            cls.repair_refresh_event = event
            cls.repair_refresh_inflight = True
            cls.repair_refresh_last_error = ""

        def _worker() -> None:
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
            except Exception as exc:
                error = str(exc)
                with cls.repair_refresh_lock:
                    cls.repair_refresh_last_error = error
                    cls.repair_refresh_last_finished = time.monotonic()
                logging.warning("检修源表后台刷新失败: %s", error)
            finally:
                with cls.repair_refresh_lock:
                    cls.repair_refresh_inflight = False
                    event.set()

        try:
            threading.Thread(
                target=_worker,
                name="LANRepairRefresh",
                daemon=True,
            ).start()
        except Exception as exc:
            with cls.repair_refresh_lock:
                cls.repair_refresh_inflight = False
                cls.repair_refresh_last_error = str(exc)
                event.set()
            raise PortalError(f"检修源表刷新启动失败: {exc}") from exc
        return {
            "repair_refresh_started": True,
            "repair_refresh_inflight": True,
            "repair_refresh_reused": False,
        }

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
            return self._send_html(portal_index_file())
        if parsed.path.startswith("/assets/"):
            relative_text = parsed.path[len("/assets/") :]
            relative = Path(*relative_text.split("/")) if relative_text else Path()
            return self._send_static_file(portal_asset_file(relative))
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
                refresh_result = PortalHandler.request_source_refresh(force=True)
                refreshed = bool(refresh_result.get("refreshed", False))
                ongoing = self._get_ongoing(scope)
                self._reconcile_orphan_started_items(scope, ongoing, force=True)
                data = self.service.get_bootstrap(
                    scope=scope, ongoing_items=ongoing
                )
                data["source_refresh_triggered"] = refreshed
                data.update(refresh_result)
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
                refresh_result = PortalHandler.request_repair_source_refresh()
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
                        {"ok": False, "error": "主窗口删除通道未连接，请确认 Qt 主程序仍在运行。"},
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
                ok, message, _ = _send_text_to_open_ids_guarded(text, recipients)
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
                        target=_send_text_to_open_ids_guarded,
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
    def refresh_sources_once(
        cls,
        *,
        force: bool = False,
        min_interval_seconds: int = 60,
        defer_if_busy: bool = False,
    ) -> dict:
        if os.environ.get("CLIPFLOW_BACKEND_MOCK_EXTERNAL") == "1":
            return {"refreshed": False, "warnings": [], "mock_external": True}
        if defer_if_busy and cls.should_defer_source_refresh():
            pressure = cls.runtime_pressure()
            return {
                "refreshed": False,
                "warnings": [],
                "source_refresh_inflight": False,
                "source_refresh_reused": True,
                "source_refresh_deferred": True,
                "source_refresh_defer_reason": "当前存在待发送/待显示/待上传任务，后台源表刷新已避让。",
                "runtime_pressure": pressure,
            }
        if not cls.source_refresh_run_lock.acquire(blocking=False):
            return {
                "refreshed": False,
                "warnings": [],
                "source_refresh_inflight": True,
                "source_refresh_reused": True,
            }
        return cls._run_source_refresh_with_reserved_lock(
            force=force,
            min_interval_seconds=min_interval_seconds,
        )

    @classmethod
    def _run_source_refresh_with_reserved_lock(
        cls, *, force: bool, min_interval_seconds: int = 60
    ) -> dict:
        try:
            with cls.source_refresh_lock:
                cls.source_refresh_inflight = True
            refreshed = False
            warnings: list[str] = []
            try:
                if force:
                    cls.service.refresh()
                    refreshed = True
                else:
                    refreshed = bool(
                        cls.service.refresh_if_interval_elapsed(
                            min_interval_seconds=min_interval_seconds
                        )
                    )
                if refreshed:
                    try:
                        service_warnings = (
                            cls.service._current_load_warnings()
                            if hasattr(cls.service, "_current_load_warnings")
                            else list(getattr(cls.service, "_load_warnings", []) or [])
                        )
                    except Exception:
                        service_warnings = []
                    for service_warning in service_warnings or []:
                        service_warning = str(service_warning or "").strip()
                        if service_warning and service_warning not in warnings:
                            warnings.append(service_warning)
                if refreshed:
                    cls.clear_payload_cache()
            except Exception as exc:
                warning = f"源表后台同步失败: {exc}"
                warnings.append(warning)
                load_warnings = getattr(cls.service, "_load_warnings", None)
                if isinstance(load_warnings, list) and warning not in load_warnings:
                    load_warnings.append(warning)
                try:
                    meta = (
                        cls.service._snapshot_meta()
                        if hasattr(cls.service, "_snapshot_meta")
                        else {"warnings": [warning]}
                    )
                    cls.state_store.record_failed_source_snapshot(
                        meta=meta,
                        error=warning,
                    )
                except Exception:
                    pass
                logging.warning(warning)
            try:
                cls.service.process_due_repair_link_tasks(limit=3)
            except Exception as exc:
                warning = f"检修源表关联补写失败: {exc}"
                warnings.append(warning)
                load_warnings = getattr(cls.service, "_load_warnings", None)
                if isinstance(load_warnings, list) and warning not in load_warnings:
                    load_warnings.append(warning)
                logging.warning(warning)
            result = {
                "refreshed": refreshed,
                "warnings": warnings,
                "source_refresh_inflight": False,
                "source_refresh_reused": False,
            }
            with cls.source_refresh_lock:
                cls.source_refresh_last_result = copy.deepcopy(result)
                cls.source_refresh_last_finished = time.monotonic()
            return result
        finally:
            with cls.source_refresh_lock:
                cls.source_refresh_inflight = False
            cls.source_refresh_run_lock.release()

    @classmethod
    def source_snapshot_ready(cls, scope: str = "ALL") -> bool:
        try:
            return bool(cls.service._source_snapshot_exists(scope or "ALL"))
        except Exception:
            return False

    @classmethod
    def ensure_source_snapshot_refresh_started(cls) -> dict:
        """Start one background source refresh when no SQLite source snapshot exists."""
        if cls.source_snapshot_ready("ALL"):
            return {"source_snapshot_ready": True, "source_refresh_started": False}
        with cls.source_refresh_lock:
            if cls.source_refresh_inflight:
                return {
                    "source_snapshot_ready": False,
                    "source_refresh_started": False,
                    "source_refresh_inflight": True,
                }
        try:
            result = cls.request_source_refresh(force=True)
        except Exception as exc:
            warning = f"源表快照缺失，后台刷新启动失败: {exc}"
            load_warnings = getattr(cls.service, "_load_warnings", None)
            if isinstance(load_warnings, list) and warning not in load_warnings:
                load_warnings.append(warning)
            logging.warning(warning)
            return {
                "source_snapshot_ready": False,
                "source_refresh_started": False,
                "error": warning,
            }
        result["source_snapshot_ready"] = False
        return result

    @classmethod
    def request_source_refresh(cls, *, force: bool = True) -> dict:
        """Start source refresh in the background and return immediately."""
        if os.environ.get("CLIPFLOW_BACKEND_MOCK_EXTERNAL") == "1":
            return {"refreshed": False, "source_refresh_started": False, "mock_external": True}
        if not cls.source_refresh_run_lock.acquire(blocking=False):
            return {
                "refreshed": False,
                "source_refresh_started": False,
                "source_refresh_inflight": True,
                "source_refresh_reused": True,
            }
        with cls.source_refresh_lock:
            cls.source_refresh_inflight = True

        def _worker() -> None:
            try:
                cls._run_source_refresh_with_reserved_lock(force=force)
            except Exception as exc:
                warning = f"源表后台刷新启动后失败: {exc}"
                if warning not in cls.service._load_warnings:
                    cls.service._load_warnings.append(warning)
                logging.warning(warning)

        try:
            threading.Thread(
                target=_worker,
                name="LANSourceRefresh",
                daemon=True,
            ).start()
        except Exception as exc:
            with cls.source_refresh_lock:
                cls.source_refresh_inflight = False
            try:
                cls.source_refresh_run_lock.release()
            except Exception:
                pass
            raise PortalError(f"源表刷新启动失败: {exc}") from exc
        return {
            "refreshed": False,
            "source_refresh_started": True,
            "source_refresh_inflight": True,
            "source_refresh_reused": False,
        }

    @classmethod
    def _source_refresh_loop(cls) -> None:
        try:
            next_wait = float(os.environ.get("CLIPFLOW_SOURCE_REFRESH_STARTUP_DELAY_SECONDS", "") or 8.0)
        except Exception:
            next_wait = 8.0
        next_wait = max(0.0, min(next_wait, 120.0))
        while True:
            if cls.source_refresh_event.wait(timeout=next_wait):
                cls.source_refresh_event.clear()
            with cls.source_refresh_lock:
                if cls.source_refresh_stop:
                    return
            has_snapshot = cls.source_snapshot_ready("ALL")
            cls.refresh_sources_once(force=True, defer_if_busy=has_snapshot)
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
            cls.message_scope_inflight.clear()
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
        try:
            cls.state_store.upsert_runtime_queue_item("message", job_id)
        except Exception:
            pass
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
            job_id = cls._dequeue_runtime_job("message")
            if not job_id:
                continue
            try:
                cls.state_store.mark_runtime_queue_item("message", job_id, "processing")
            except Exception:
                pass
            cls._process_initial_message_job(job_id)

    @classmethod
    def _message_job_scope(cls, job_id: str) -> str:
        job = cls.service.get_job(job_id) or {}
        request = job.get("request") if isinstance(job.get("request"), dict) else {}
        return str((request or {}).get("scope") or "").strip().upper()

    @classmethod
    def _collect_message_batch_job_ids(cls, primary_job_id: str) -> list[str]:
        primary_job_id = str(primary_job_id or "").strip()
        if not primary_job_id:
            return []
        primary_job = cls.service.get_job(primary_job_id) or {}
        request = primary_job.get("request") if isinstance(primary_job.get("request"), dict) else {}
        if str((request or {}).get("work_type") or "").strip() != "maintenance":
            return [primary_job_id]
        scope = str((request or {}).get("scope") or "").strip().upper()
        if not scope:
            return [primary_job_id]
        wait_seconds = float(getattr(cls, "message_batch_wait_seconds", 0.0) or 0.0)
        if wait_seconds > 0:
            time.sleep(wait_seconds)
        batch = [primary_job_id]
        max_jobs = max(1, int(getattr(cls, "message_batch_max_jobs", 20) or 20))
        with cls.message_queue_lock:
            if not cls.message_queue:
                return batch
            remaining: list[str] = []
            for queued_job_id in cls.message_queue:
                queued_job_id = str(queued_job_id or "").strip()
                if not queued_job_id:
                    continue
                if len(batch) < max_jobs and cls._message_job_scope(queued_job_id) == scope:
                    batch.append(queued_job_id)
                    try:
                        cls.state_store.mark_runtime_queue_item(
                            "message",
                            queued_job_id,
                            "processing",
                        )
                    except Exception:
                        pass
                    continue
                remaining.append(queued_job_id)
            cls.message_queue = remaining
            cls._update_message_queue_positions_locked()
            if not cls.message_queue:
                cls.message_queue_event.clear()
        return batch

    @classmethod
    def _release_message_scope(cls, job_id: str) -> None:
        scope = cls._message_job_scope(job_id)
        if not scope:
            return
        with cls.message_queue_lock:
            cls.message_scope_inflight.discard(scope)
            if cls.message_queue:
                cls.message_queue_event.set()

    @classmethod
    def _process_message_batch(cls, batched_prepared: list[tuple[str, dict]]) -> None:
        if not batched_prepared:
            return
        if len(batched_prepared) == 1:
            job_id, prepared = batched_prepared[0]
            ok, message = cls.service.send_action_personal_message(prepared)
            if not ok:
                cls.service.mark_job(
                    job_id,
                    phase="failed",
                    error=message,
                    message_sent=False,
                )
                try:
                    cls.state_store.mark_runtime_queue_item(
                        "message",
                        job_id,
                        "failed",
                        error=message,
                    )
                except Exception:
                    pass
                return
            cls.service.mark_job(
                job_id,
                phase="message_sent",
                message_sent=True,
                message_signature=str(prepared.get("message_signature") or ""),
                message_queue_position=0,
                queue_position=0,
            )
            try:
                cls.state_store.mark_runtime_queue_item("message", job_id, "done")
            except Exception:
                pass
            cls.enqueue_action_job(job_id)
            return
        recipients = [
            str(open_id or "").strip()
            for open_id in (batched_prepared[0][1].get("recipients") or [])
            if str(open_id or "").strip()
        ]
        combined_text = "\n\n".join(
            str((prepared or {}).get("text") or "").strip()
            for _job_id, prepared in batched_prepared
            if str((prepared or {}).get("text") or "").strip()
        )
        ok, message, results = _send_text_to_open_ids_guarded(combined_text, recipients)
        for job_id, prepared in batched_prepared:
            if not ok:
                cls.service.mark_job(
                    job_id,
                    phase="failed",
                    error=message,
                    message_sent=False,
                )
                try:
                    cls.state_store.mark_runtime_queue_item(
                        "message",
                        job_id,
                        "failed",
                        error=message,
                    )
                except Exception:
                    pass
                continue
            cls.service.mark_job(
                job_id,
                phase="message_sent",
                message_sent=True,
                message_signature=str(prepared.get("message_signature") or ""),
                recipient_results=copy.deepcopy(results),
                message_queue_position=0,
                queue_position=0,
            )
            try:
                cls.state_store.mark_runtime_queue_item("message", job_id, "done")
            except Exception:
                pass
            cls.enqueue_action_job(job_id)

    @classmethod
    def _dequeue_runtime_job(cls, queue_name: str) -> str:
        queue_name = str(queue_name or "").strip()
        if queue_name == "message":
            with cls.message_queue_lock:
                if cls.message_queue:
                    selected_index = -1
                    selected_scope = ""
                    for index, queued_job_id in enumerate(cls.message_queue):
                        queued_job_id = str(queued_job_id or "").strip()
                        if not queued_job_id:
                            continue
                        scope = cls._message_job_scope(queued_job_id)
                        if scope and scope in cls.message_scope_inflight:
                            continue
                        selected_index = index
                        selected_scope = scope
                        break
                    if selected_index >= 0:
                        job_id = cls.message_queue.pop(selected_index)
                        if selected_scope:
                            cls.message_scope_inflight.add(selected_scope)
                        cls._update_message_queue_positions_locked()
                        return str(job_id or "").strip()
                    cls.message_queue_event.clear()
                    return ""
                cls.message_queue_event.clear()
        elif queue_name == "qt_action":
            with cls.action_queue_lock:
                if cls.action_queue:
                    job_id = cls.action_queue.pop(0)
                    cls._update_queue_positions_locked()
                    return str(job_id or "").strip()
                cls.action_queue_event.clear()
        for _attempt in range(5):
            try:
                leased = cls.state_store.lease_runtime_queue_items(
                    queue_name,
                    limit=1,
                    lease_seconds=30,
                )
            except Exception:
                leased = []
            if not leased:
                return ""
            job_id = str((leased[0] or {}).get("job_id") or "").strip()
            if cls._runtime_queue_job_processable(queue_name, job_id):
                return job_id
        return ""

    @classmethod
    def _runtime_queue_job_processable(cls, queue_name: str, job_id: str) -> bool:
        queue_name = str(queue_name or "").strip()
        job_id = str(job_id or "").strip()
        if not job_id:
            return False
        try:
            job = cls.service.get_job(job_id) or {}
        except Exception:
            job = {}
        phase = str((job or {}).get("phase") or "").strip()
        if not job:
            try:
                cls.state_store.mark_runtime_queue_item(
                    queue_name,
                    job_id,
                    "cancelled",
                    error="任务记录不存在，已跳过队列项。",
                )
            except Exception:
                pass
            return False
        if phase in {"success", "failed"}:
            try:
                cls.state_store.mark_runtime_queue_item(
                    queue_name,
                    job_id,
                    "done" if phase == "success" else "failed",
                    error=str((job or {}).get("error") or ""),
                )
            except Exception:
                pass
            return False
        depends_on_job_id = str((job or {}).get("depends_on_job_id") or "").strip()
        if depends_on_job_id and depends_on_job_id != job_id:
            try:
                depends_on_job = cls.service.get_job(depends_on_job_id) or {}
            except Exception:
                depends_on_job = {}
            depends_on_phase = str((depends_on_job or {}).get("phase") or "").strip()
            if depends_on_job and depends_on_phase not in {"success", "failed"}:
                cls.service.mark_job(
                    job_id,
                    depends_on_phase=depends_on_phase,
                    _persist=False,
                )
                try:
                    cls.state_store.requeue_runtime_queue_item(
                        queue_name,
                        job_id,
                        available_at=time.time() + 1.5,
                        error=(
                            f"waiting_for:{depends_on_job_id}:{depends_on_phase}"
                        ),
                    )
                except Exception:
                    pass
                return False
            if str((job or {}).get("depends_on_phase") or "").strip():
                cls.service.mark_job(job_id, depends_on_phase="", _persist=False)
        return True

    @classmethod
    def _process_initial_message_job(cls, job_id: str) -> None:
        try:
            job_ids = cls._collect_message_batch_job_ids(job_id)
            batched_prepared: list[tuple[str, dict]] = []
            for current_job_id in job_ids:
                prepared = cls.service.prepare_action_job(current_job_id)
                if prepared.get("skip_personal_message"):
                    try:
                        cls.state_store.mark_runtime_queue_item(
                            "message", current_job_id, "done"
                        )
                    except Exception:
                        pass
                    cls.enqueue_action_job(current_job_id)
                    continue
                message_signature = str(prepared.get("message_signature") or "")
                if prepared.get("message_sent"):
                    cls.service.mark_job(
                        current_job_id,
                        phase="message_sent",
                        message_sent=True,
                        message_signature=message_signature,
                        message_queue_position=0,
                        queue_position=0,
                    )
                    try:
                        cls.state_store.mark_runtime_queue_item(
                            "message", current_job_id, "done"
                        )
                    except Exception:
                        pass
                    cls.enqueue_action_job(current_job_id)
                    continue
                cls.service.mark_job(
                    current_job_id,
                    phase="sending_message",
                    message_queue_position=0,
                    queue_position=0,
                )
                batched_prepared.append((current_job_id, prepared))
            if batched_prepared:
                cls._process_message_batch(batched_prepared)
        except Exception as exc:
            cls.service.mark_job(job_id, phase="failed", error=str(exc))
            try:
                cls.state_store.mark_runtime_queue_item(
                    "message",
                    job_id,
                    "failed",
                    error=str(exc),
                )
            except Exception:
                pass
        finally:
            cls._release_message_scope(job_id)

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
        try:
            cls.state_store.upsert_runtime_queue_item("qt_action", job_id)
        except Exception:
            pass
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
                phase="qt_queued",
                qt_phase="queued",
                qt_queue_position=index,
                qt_queue_size=total,
                queue_position=index,
                queue_size=total,
                upload_queue_position=0,
                upload_queue_size=0,
                _persist=False,
            )

    @classmethod
    def _action_worker_loop(cls) -> None:
        while True:
            cls.action_queue_event.wait(timeout=1)
            if cls.action_worker_stop:
                return
            job_id = cls._dequeue_runtime_job("qt_action")
            if not job_id:
                continue
            try:
                cls.state_store.mark_runtime_queue_item("qt_action", job_id, "processing")
            except Exception:
                pass
            cls.service.mark_job(
                job_id,
                queue_position=0,
                qt_queue_position=0,
                upload_queue_position=0,
            )
            cls._process_maintenance_action_job(job_id)

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
            deadline = time.monotonic() + cls.action_upload_timeout_s
            cls.upload_wait_jobs[job_id] = deadline
            cls.upload_wait_event.set()
        try:
            cls.state_store.upsert_runtime_queue_item(
                "upload_wait",
                job_id,
                payload={"deadline_monotonic": deadline},
            )
        except Exception:
            pass

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
                    try:
                        cls.state_store.mark_runtime_queue_item(
                            "upload_wait",
                            job_id,
                            "done" if phase == "success" else "failed" if phase == "failed" else "cancelled",
                        )
                    except Exception:
                        pass
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
                try:
                    cls.state_store.mark_runtime_queue_item(
                        "upload_wait",
                        job_id,
                        "failed",
                        error="上传多维超时，请在主界面确认实际结果后重试。",
                    )
                except Exception:
                    pass
            with cls.upload_wait_lock:
                if not cls.upload_wait_jobs:
                    cls.upload_wait_event.clear()

    @staticmethod
    def _existing_tokens_for_notice_type(notice_type: str, fields: dict) -> tuple[list[str], list[str], str]:
        notice_type = str(notice_type or "").strip()
        fields = fields if isinstance(fields, dict) else {}
        existing_tokens: list[str] = []
        existing_extra_tokens: list[str] = []
        if notice_type == "事件通告":
            for item in fields.get("进展更新截图", []) or []:
                if "file_token" in item:
                    existing_tokens.append(item["file_token"])
            for item in fields.get("事件恢复截图", []) or []:
                if "file_token" in item:
                    existing_extra_tokens.append(item["file_token"])
        elif notice_type == "维保通告":
            for item in fields.get("过程通告图片", []) or []:
                if "file_token" in item:
                    existing_tokens.append(item["file_token"])
            for item in fields.get("过程现场图片", []) or []:
                if "file_token" in item:
                    existing_extra_tokens.append(item["file_token"])
        elif notice_type == "设备检修":
            for item in fields.get("过程通告截图", []) or []:
                if "file_token" in item:
                    existing_tokens.append(item["file_token"])
            for item in fields.get("过程现场图片", []) or []:
                if "file_token" in item:
                    existing_extra_tokens.append(item["file_token"])
        elif notice_type in ("设备变更", "变更通告"):
            for item in fields.get("过程更新钉钉截图", []) or []:
                if "file_token" in item:
                    existing_tokens.append(item["file_token"])
        elif notice_type in ("上下电通告", "上电通告", "下电通告", "设备轮巡", "设备调整"):
            for item in fields.get("过程通告截图", []) or []:
                if "file_token" in item:
                    existing_tokens.append(item["file_token"])
        else:
            for item in fields.get("截图", []) or []:
                if "file_token" in item:
                    existing_tokens.append(item["file_token"])
        if notice_type in ("设备变更", "变更通告"):
            existing_response_time = fields.get("过程更新时间", "")
        else:
            existing_response_time = fields.get("进展更新时间", "")
        return existing_tokens, existing_extra_tokens, existing_response_time

    @staticmethod
    def _prepared_to_notice_payload(
        prepared: dict,
        *,
        existing_file_tokens: list[str] | None = None,
        existing_extra_file_tokens: list[str] | None = None,
        existing_response_time: str | None = None,
    ) -> NoticePayload:
        prepared = prepared if isinstance(prepared, dict) else {}
        return NoticePayload(
            text=str(prepared.get("text") or ""),
            level=str(prepared.get("level") or "").strip() or None,
            buildings=[
                str(item or "").strip()
                for item in (prepared.get("buildings") or [])
                if str(item or "").strip()
            ]
            or None,
            specialty=str(prepared.get("specialty") or "").strip() or None,
            event_source=str(prepared.get("event_source") or "").strip() or None,
            response_time=str(prepared.get("response_time") or "").strip() or None,
            occurrence_date=str(prepared.get("time_str") or "").strip() or None,
            existing_file_tokens=list(existing_file_tokens or []) or None,
            existing_extra_file_tokens=list(existing_extra_file_tokens or []) or None,
            existing_response_time=existing_response_time or None,
            transfer_to_overhaul=prepared.get("transfer_to_overhaul"),
            recover=bool(prepared.get("recover_selected", False)),
            robot_group_choice="auto",
            maintenance_cycle=str(prepared.get("maintenance_cycle") or "").strip() or None,
        )

    @classmethod
    def _execute_backend_prepared_upload(
        cls, prepared: dict
    ) -> tuple[bool, str, str]:
        prepared = prepared if isinstance(prepared, dict) else {}
        notice_type = str(prepared.get("notice_type") or "").strip()
        action = str(prepared.get("action") or "").strip().lower()
        if not notice_type or action not in {"start", "update", "end"}:
            return False, "后端上传缺少必要字段。", ""
        guard = external_real_write_guard()
        if guard["mock_external"]:
            job_id = str(prepared.get("job_id") or "").strip() or "mock"
            record_id = (
                str(prepared.get("target_record_id") or "").strip()
                or f"mock_backend_{job_id[:12]}"
            )
            return True, record_id, record_id
        if not guard["real_write_allowed"]:
            return False, str(guard["reason"] or "真实外部写入未确认。"), ""

        if action == "start":
            payload = cls._prepared_to_notice_payload(prepared)
            ok, result = create_bitable_record_by_payload(notice_type, payload)
            record_id = str(result or "").strip() if ok else ""
            return bool(ok), str(result or ""), record_id

        record_id = str(
            prepared.get("target_record_id")
            or prepared.get("record_id")
            or ""
        ).strip()
        if not record_id:
            return False, "缺少目标多维 record_id。", ""
        ok_query, query_result = query_record_by_id(record_id, notice_type)
        if not ok_query:
            return False, f"查询失败: {query_result}", record_id
        fields = query_result.get("fields", {}) if isinstance(query_result, dict) else {}
        existing_tokens, existing_extra_tokens, existing_response_time = (
            cls._existing_tokens_for_notice_type(notice_type, fields)
        )
        payload = cls._prepared_to_notice_payload(
            prepared,
            existing_file_tokens=existing_tokens,
            existing_extra_file_tokens=existing_extra_tokens,
            existing_response_time=existing_response_time if action == "update" else "",
        )
        ok, result = update_bitable_record_by_payload(record_id, notice_type, payload)
        return bool(ok), str(result or ""), record_id

    @classmethod
    def _prepared_to_qt_ui_payload(
        cls, prepared: dict, *, remote_record_id: str = ""
    ) -> dict:
        prepared = dict(prepared or {})
        source_record_id = str(prepared.get("record_id") or "").strip()
        target_record_id = (
            str(remote_record_id or "").strip()
            or str(prepared.get("target_record_id") or "").strip()
            or source_record_id
        )
        prepared["ui_only"] = True
        prepared["source_record_id"] = source_record_id
        prepared["record_id"] = target_record_id
        prepared["target_record_id"] = target_record_id
        return prepared

    @staticmethod
    def _decode_local_notice_bytes(encoded: str) -> bytes:
        text = str(encoded or "").strip()
        if not text:
            return b""
        return base64.b64decode(text.encode("utf-8"))

    @classmethod
    def execute_local_notice_upload(cls, request_payload: dict) -> dict:
        payload = dict(request_payload or {})
        data = payload.get("data_dict")
        data = dict(data) if isinstance(data, dict) else {}
        if not data:
            raise PortalError("Qt 上传请求缺少 data_dict。")
        action_type = str(payload.get("action_type") or "").strip().lower()
        if action_type not in {"upload", "update", "end", "upload_replace"}:
            raise PortalError("Qt 上传请求 action_type 不支持。")
        notice_type = str(data.get("notice_type") or "").strip()
        if not notice_type:
            raise PortalError("Qt 上传请求缺少 notice_type。")
        record_id = str(data.get("record_id") or "").strip()
        if not record_id:
            raise PortalError("Qt 上传请求缺少 record_id。")

        screenshot_bytes = b""
        screenshot_b64 = str(payload.get("screenshot_bytes_b64") or "").strip()
        if screenshot_b64:
            screenshot_bytes = cls._decode_local_notice_bytes(screenshot_b64)
        extra_images = payload.get("extra_images") if isinstance(payload.get("extra_images"), list) else []
        extra_file_tokens: list[str] = []
        file_tokens: list[str] = []

        if screenshot_bytes:
            success, result = upload_media_to_feishu(screenshot_bytes)
            if not success:
                return {
                    "ok": False,
                    "name": "截图上传",
                    "message": str(result or ""),
                    "record_id": record_id,
                    "real_record_id": "",
                }
            file_tokens.append(str(result or "").strip())
        for index, entry in enumerate(extra_images, start=1):
            entry = entry if isinstance(entry, dict) else {}
            image_b64 = str(entry.get("bytes_b64") or "").strip()
            if not image_b64:
                continue
            file_name = str(entry.get("file_name") or f"extra_{index}.png").strip()
            image_bytes = cls._decode_local_notice_bytes(image_b64)
            success, result = upload_media_to_feishu(image_bytes, file_name=file_name)
            if not success:
                return {
                    "ok": False,
                    "name": "截图上传",
                    "message": str(result or ""),
                    "record_id": record_id,
                    "real_record_id": "",
                }
            extra_file_tokens.append(str(result or "").strip())

        notice_payload = cls._prepared_to_notice_payload(data)
        notice_payload.file_tokens = file_tokens or None
        notice_payload.extra_file_tokens = extra_file_tokens or None
        notice_payload.response_time = str(payload.get("response_time") or "").strip() or None
        notice_payload.recover = bool(payload.get("recover_selected", False))
        notice_payload.robot_group_choice = (
            str(payload.get("robot_group_choice") or "auto").strip() or "auto"
        )
        notice_payload.transfer_to_overhaul = data.get("transfer_to_overhaul")
        notice_payload.occurrence_date = str(data.get("time_str") or "").strip() or None

        if action_type == "upload":
            success, result = create_bitable_record_by_payload(notice_type, notice_payload)
            real_record_id = str(result or "").strip() if success else ""
            return {
                "ok": bool(success),
                "name": "上传",
                "message": str(result or ""),
                "record_id": record_id,
                "real_record_id": real_record_id,
            }

        existing_tokens: list[str] = []
        existing_extra_tokens: list[str] = []
        existing_response_time = ""
        target_record_id = record_id
        if not bool(data.get("_is_placeholder_record")):
            ok_query, query_result = query_record_by_id(record_id, notice_type)
            if not ok_query:
                action_name = "结束" if action_type == "end" else "更新" if action_type == "update" else "归档"
                return {
                    "ok": False,
                    "name": action_name,
                    "message": f"查询失败: {query_result}",
                    "record_id": record_id,
                    "real_record_id": "",
                }
            fields = query_result.get("fields", {}) if isinstance(query_result, dict) else {}
            (
                existing_tokens,
                existing_extra_tokens,
                existing_response_time,
            ) = cls._existing_tokens_for_notice_type(notice_type, fields)
        notice_payload.existing_file_tokens = existing_tokens or None
        notice_payload.existing_extra_file_tokens = existing_extra_tokens or None
        notice_payload.existing_response_time = (
            existing_response_time if action_type in {"update", "upload_replace"} else None
        )

        if action_type == "upload_replace" and bool(data.get("_is_placeholder_record")):
            success, result = create_bitable_record_by_payload(notice_type, notice_payload)
            real_record_id = str(result or "").strip() if success else ""
            return {
                "ok": bool(success),
                "name": "归档",
                "message": str(result or ""),
                "record_id": record_id,
                "real_record_id": real_record_id,
            }

        action_name = "结束" if action_type == "end" else "更新" if action_type == "update" else "归档"
        success, result = update_bitable_record_by_payload(
            target_record_id,
            notice_type,
            notice_payload,
        )
        return {
            "ok": bool(success),
            "name": action_name,
            "message": str(result or ""),
            "record_id": record_id,
            "real_record_id": target_record_id if success else "",
        }

    @classmethod
    def execute_local_delete_active_item(cls, payload: dict) -> dict:
        payload = dict(payload or {})
        nested = payload.get("data_dict")
        if isinstance(nested, dict):
            payload = dict(nested)
        elif isinstance(payload.get("data"), dict):
            payload = dict(payload.get("data") or {})
        record_id = str(payload.get("record_id") or "").strip()
        if not record_id:
            record_id = str(
                payload.get("target_record_id") or payload.get("raw_record_id") or ""
            ).strip()
        active_item_id = str(payload.get("active_item_id") or "").strip()
        notice_type = str(payload.get("notice_type") or "").strip()
        is_placeholder = bool(payload.get("_is_placeholder_record"))
        remote_deleted = False
        if record_id and notice_type and not is_placeholder:
            ok, result = delete_bitable_record(record_id, notice_type)
            if not ok:
                return {
                    "ok": False,
                    "message": str(result or "多维记录删除失败。"),
                    "record_id": record_id,
                }
            remote_deleted = True
        try:
            cls.state_store.delete_qt_active_item(
                active_item_id=active_item_id,
                record_id=record_id,
            )
        except Exception:
            pass
        return {
            "ok": True,
            "message": "",
            "record_id": record_id,
            "active_item_id": active_item_id,
            "remote_deleted": remote_deleted,
        }

    @classmethod
    def execute_local_today_progress_update(cls, payload: dict) -> dict:
        payload = dict(payload or {})
        record_id = str(
            payload.get("target_record_id") or payload.get("record_id") or ""
        ).strip()
        notice_type = str(payload.get("notice_type") or "").strip()
        field_name = str(payload.get("field_name") or "").strip()
        field_value = payload.get("field_value")
        if not record_id or not notice_type or not field_name:
            return {
                "ok": False,
                "message": "缺少今日是否进行所需参数。",
                "record_id": record_id,
            }
        ok, result = update_bitable_record_fields(
            record_id,
            notice_type,
            {field_name: field_value},
        )
        return {
            "ok": bool(ok),
            "message": str(result or ""),
            "record_id": record_id,
        }

    @classmethod
    def _process_maintenance_action_job(cls, job_id: str) -> None:
        try:
            prepared = cls.service.prepare_action_job(job_id)
            if (
                not prepared.get("skip_personal_message")
                and not prepared.get("message_sent")
            ):
                try:
                    cls.state_store.mark_runtime_queue_item(
                        "qt_action",
                        job_id,
                        "waiting_message",
                    )
                except Exception:
                    pass
                cls.enqueue_message_job(job_id)
                return
            if prepared.get("skip_personal_message"):
                cls.service.mark_job(
                    job_id,
                    phase="qt_queued",
                    qt_phase="queued",
                    queue_position=0,
                    message_sent=True,
                    message_signature=str(prepared.get("message_signature") or ""),
                )
            else:
                cls.service.mark_job(
                    job_id,
                    phase="qt_queued",
                    qt_phase="queued",
                    queue_position=0,
                )
            if not prepared.get("skip_personal_message"):
                cls.service.mark_job(
                    job_id,
                    phase="upload_waiting",
                    qt_phase="backend_upload",
                    message_sent=True,
                    message_signature=str(prepared.get("message_signature") or ""),
                )
            cls.service.mark_job(
                job_id,
                phase="uploading",
                qt_phase="backend_upload",
                qt_queue_position=0,
                qt_queue_size=0,
                upload_queue_position=0,
                upload_queue_size=0,
            )
            ok, result_message, remote_record_id = cls._execute_backend_prepared_upload(prepared)
            if not ok:
                cls.service.mark_action_upload_result(
                    job_id,
                    success=False,
                    message=result_message,
                    record_id=str(remote_record_id or prepared.get("target_record_id") or prepared.get("record_id") or ""),
                    active_item_id=str(prepared.get("active_item_id") or ""),
                )
                try:
                    cls.state_store.mark_runtime_queue_item(
                        "qt_action",
                        job_id,
                        "failed",
                        error=result_message,
                    )
                except Exception:
                    pass
                return
            cls.service.mark_action_upload_result(
                job_id,
                success=True,
                message=result_message,
                record_id=str(remote_record_id or ""),
                active_item_id=str(prepared.get("active_item_id") or ""),
            )
            event_payload = cls._prepared_to_qt_ui_payload(
                prepared,
                remote_record_id=remote_record_id,
            )
            try:
                event_id = cls.state_store.enqueue_outbox_event(
                    "qt_action",
                    {
                        "kind": "maintenance_action",
                        "job_id": job_id,
                        "payload": event_payload,
                    },
                )
                cls.service.mark_job(
                    job_id,
                    qt_phase="outbox",
                    qt_queue_position=0,
                    qt_queue_size=0,
                    qt_event_id=event_id,
                )
            except Exception as exc:
                log_warning(f"Qt UI 事件投递失败: job_id={job_id}, error={exc}")
            try:
                cls.state_store.mark_runtime_queue_item("qt_action", job_id, "done")
            except Exception:
                pass
        except Exception as exc:
            cls.service.mark_job(job_id, phase="failed", error=str(exc))
            try:
                cls.state_store.mark_runtime_queue_item(
                    "qt_action",
                    job_id,
                    "failed",
                    error=str(exc),
                )
            except Exception:
                pass

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
        start_source_refresh_worker: bool = True,
    ) -> None:
        self.host = str(host or DEFAULT_HOST).strip() or DEFAULT_HOST
        self.preferred_port = int(port or DEFAULT_PORT)
        self.app_token = str(app_token or DEFAULT_APP_TOKEN).strip()
        self.table_id = str(table_id or DEFAULT_TABLE_ID).strip()
        self.start_source_refresh_worker = bool(start_source_refresh_worker)
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
        PortalHandler.apply_runtime_settings()
        PortalHandler.notice_callback = self.notice_callback
        PortalHandler.ongoing_callback = self.ongoing_callback
        PortalHandler.ongoing_delete_callback = self.ongoing_delete_callback
        PortalHandler.maintenance_action_callback = self.maintenance_action_callback
        with PortalHandler.source_refresh_lock:
            PortalHandler.source_refresh_inflight = False
            PortalHandler.source_refresh_last_result = {}
            PortalHandler.source_refresh_last_finished = 0.0
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
            PortalHandler.message_scope_inflight.clear()
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
        for job_id in PortalHandler.service.recoverable_action_job_ids():
            PortalHandler.enqueue_initial_message_or_upload_job(job_id)
        if self.start_source_refresh_worker:
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

    def mark_job_progress(self, job_id: str, **patch) -> None:
        if not job_id:
            return
        PortalHandler.service.mark_job(job_id, **patch)
        phase = str((patch or {}).get("phase") or "").strip()
        if phase == "uploading":
            PortalHandler.track_upload_wait_job(job_id)
        elif phase in {"success", "failed"}:
            with PortalHandler.upload_wait_lock:
                PortalHandler.upload_wait_jobs.pop(job_id, None)

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
        if self.start_source_refresh_worker:
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
    parser.add_argument(
        "--allow-real-external",
        action="store_true",
        help="允许直接命令行启动的旧门户执行真实飞书/多维写入。",
    )
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    if args.allow_real_external:
        os.environ["CLIPFLOW_REQUIRE_REAL_EXTERNAL_CONFIRM"] = "1"
        os.environ["CLIPFLOW_REAL_EXTERNAL_CONFIRMED"] = "1"
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
