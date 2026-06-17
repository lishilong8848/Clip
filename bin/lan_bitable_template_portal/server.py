# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import copy
import hashlib
import html
import json
import logging
import mimetypes
import os
import re
import socket
import threading
import time
from http import HTTPStatus
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
    engineer_mop_fill_kwargs_from_payload,
    engineer_mop_upload_signed_kwargs_from_payload,
    external_real_write_guard,
)
from .identity_utils import (
    canonical_target_record_id,
    is_local_record_id,
    normalize_notice_identity_payload,
)
from .state_store import LanPortalStateStore
from upload_event_module.config import get_field_config
from upload_event_module.services.handlers import NoticePayload
from upload_event_module.services.service_registry import (
    create_bitable_record_fields,
    create_bitable_record_by_payload,
    query_record_by_id,
    upload_media_to_feishu,
    update_bitable_record_fields,
    update_bitable_record_by_payload,
)
from upload_event_module.services.feishu_service import delete_bitable_record
from upload_event_module.services.robot_webhook import send_text_to_open_ids
from upload_event_module.core.parser import extract_event_info, extract_notice_info


FRONTEND_DIST_DIR = Path(__file__).resolve().parent / "frontend" / "dist"
DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 18766
CLIENT_DISCONNECT_WINERRORS = {10053, 10054, 10058}
MAX_JSON_BODY_BYTES = 512 * 1024


def portal_frontend_dist_enabled() -> bool:
    return (FRONTEND_DIST_DIR / "index.html").is_file()


def portal_frontend_dist_ready() -> bool:
    return portal_frontend_dist_enabled()


def portal_index_file() -> Path:
    return FRONTEND_DIST_DIR / "index.html"


def portal_asset_file(relative: Path) -> Path:
    return FRONTEND_DIST_DIR / "assets" / relative


def portal_static_roots() -> list[Path]:
    return [FRONTEND_DIST_DIR.resolve()]


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


class PortalRuntime:
    """Shared portal runtime used by the FastAPI backend.

    The old built-in HTTP server entry has been removed from the production
    path. This class is now a shared runtime container for the FastAPI router
    and Qt bridge while the service layer is being split further.
    """

    service = MaintenancePortalService()
    auth_manager = PortalAuthManager()
    state_store = LanPortalStateStore()
    notice_callback = None
    ongoing_callback = None
    ongoing_delete_callback = None
    maintenance_action_callback = None
    last_ongoing_error = ""
    action_queue_lock = threading.RLock()
    action_queue_event = threading.Event()
    action_worker_thread: threading.Thread | None = None
    action_worker_stop = False
    action_upload_timeout_s = 30 * 60
    upload_wait_thread: threading.Thread | None = None
    upload_wait_lock = threading.RLock()
    upload_wait_event = threading.Event()
    upload_wait_stop = False
    message_queue_lock = threading.RLock()
    message_queue_event = threading.Event()
    message_worker_threads: list[threading.Thread] = []
    message_worker_stop = False
    message_worker_count = 5
    message_batch_wait_seconds = 1.2
    message_batch_max_jobs = 20
    message_scope_inflight: set[str] = set()
    local_upload_locks: dict[str, threading.RLock] = {}
    local_upload_locks_lock = threading.RLock()
    local_upload_created_targets: dict[str, str] = {}
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
    change_refresh_lock = threading.RLock()
    change_refresh_inflight = False
    change_refresh_event = threading.Event()
    change_refresh_last_result: dict = {}
    change_refresh_last_error = ""
    change_refresh_last_finished = 0.0
    change_refresh_reuse_window_s = 10.0
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
        message_queue_size = 0
        qt_queue_size = 0
        upload_wait_size = 0
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
        message_details = (runtime_counts or {}).get("message")
        if isinstance(message_details, dict):
            message_queue_size = int(message_details.get("queued_due") or 0) + int(
                message_details.get("queued_future") or 0
            )
        qt_details = (runtime_counts or {}).get("qt_action")
        if isinstance(qt_details, dict):
            qt_queue_size = int(qt_details.get("queued_due") or 0) + int(
                qt_details.get("queued_future") or 0
            )
        upload_wait_details = (runtime_counts or {}).get("upload_wait")
        if isinstance(upload_wait_details, dict):
            upload_wait_size = int(upload_wait_details.get("queued_due") or 0) + int(
                upload_wait_details.get("queued_future") or 0
            ) + int(upload_wait_details.get("processing_active") or 0)
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

    def _read_json_body(self, *, max_bytes: int | None = None) -> dict:
        raw_length = self.headers.get("Content-Length", "0") or "0"
        try:
            length = int(raw_length)
        except ValueError as exc:
            raise ValueError("请求体长度无效。") from exc
        if length < 0:
            raise ValueError("请求体长度无效。")
        limit = int(max_bytes or MAX_JSON_BODY_BYTES)
        if length > limit:
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
        return PortalRuntime.auth_manager.get_session(
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
            session_id, next_path = PortalRuntime.auth_manager.complete_login(
                code=code,
                state=state,
                redirect_uri=redirect_uri,
            )
        except PortalError as exc:
            self._send_html_message(400, "飞书登录失败", str(exc))
            return True
        self._send_redirect(
            next_path or sanitized,
            set_cookie=PortalRuntime.auth_manager.cookie_header(session_id),
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
        if PortalRuntime.auth_manager.is_admin(session):
            return True
        self._send_json(403, {"ok": False, "error": "只有管理员可以执行该操作。"})
        return False

    def _authorized_scope_or_error(self, session: dict, scope: str) -> str:
        normalized = PortalRuntime.auth_manager.normalize_scope(scope)
        if not PortalRuntime.auth_manager.scope_allowed(session, normalized):
            raise PortalError(
                f"当前飞书账号无权访问 {PortalRuntime.auth_manager.scope_label(normalized)}。"
            )
        return normalized

    def _with_auth_context(self, payload: dict, session: dict) -> dict:
        if not isinstance(payload, dict):
            return payload
        result = dict(payload)
        result["auth"] = PortalRuntime.auth_manager.public_status(
            session,
            next_path=self._request_target(),
            redirect_uri=f"{self._request_base_url()}/api/auth/feishu/callback",
        )
        scope_options = result.get("scope_options")
        if isinstance(scope_options, list):
            result["scope_options"] = PortalRuntime.auth_manager.filter_scope_options(
                scope_options, session
            )
        return result

    def _job_visible_to_session(self, job: dict, session: dict) -> bool:
        if PortalRuntime.auth_manager.is_admin(session):
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
            PortalRuntime.payload_cache_generation,
            service_version,
            str(getattr(self.service, "_last_loaded_at", "") or ""),
            *key_parts,
        )
        now = time.monotonic()
        with PortalRuntime.payload_cache_lock:
            PortalRuntime._prune_payload_cache_locked(now)
            cached = PortalRuntime.payload_cache.get(key)
            if cached and cached[0] > now:
                return copy.deepcopy(cached[1])
            inflight = PortalRuntime.payload_cache_inflight.get(key)
            if inflight is None:
                inflight = threading.Event()
                PortalRuntime.payload_cache_inflight[key] = inflight
                PortalRuntime.payload_cache_inflight_started[key] = now
                owner = True
            else:
                owner = False
        if not owner:
            inflight.wait(timeout=5)
            with PortalRuntime.payload_cache_lock:
                cached = PortalRuntime.payload_cache.get(key)
                if cached and cached[0] > time.monotonic():
                    return copy.deepcopy(cached[1])
        try:
            payload = builder()
        except Exception:
            if owner:
                with PortalRuntime.payload_cache_lock:
                    event = PortalRuntime.payload_cache_inflight.pop(key, None)
                    PortalRuntime.payload_cache_inflight_started.pop(key, None)
                    if event is not None:
                        event.set()
            raise
        if not isinstance(payload, dict):
            if owner:
                with PortalRuntime.payload_cache_lock:
                    event = PortalRuntime.payload_cache_inflight.pop(key, None)
                    PortalRuntime.payload_cache_inflight_started.pop(key, None)
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
            and payload_size > int(PortalRuntime.payload_cache_max_payload_bytes)
        ):
            if owner:
                with PortalRuntime.payload_cache_lock:
                    event = PortalRuntime.payload_cache_inflight.pop(key, None)
                    PortalRuntime.payload_cache_inflight_started.pop(key, None)
                    if event is not None:
                        event.set()
            return payload
        with PortalRuntime.payload_cache_lock:
            PortalRuntime.payload_cache[key] = (
                now + float(PortalRuntime.payload_cache_ttl_s),
                copy.deepcopy(payload),
            )
            PortalRuntime._prune_payload_cache_locked(now)
            if owner:
                event = PortalRuntime.payload_cache_inflight.pop(key, None)
                PortalRuntime.payload_cache_inflight_started.pop(key, None)
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

    @classmethod
    def request_change_source_refresh(cls) -> dict:
        """Refresh change and Zhihang change source tables with singleflight."""
        if os.environ.get("CLIPFLOW_BACKEND_MOCK_EXTERNAL") == "1":
            return {
                "change_refresh_started": False,
                "change_refresh_inflight": False,
                "change_refresh_reused": False,
                "mock_external": True,
            }
        now = time.monotonic()
        with cls.change_refresh_lock:
            if (
                cls.change_refresh_last_result
                and now - float(cls.change_refresh_last_finished or 0)
                <= float(cls.change_refresh_reuse_window_s)
            ):
                result = copy.deepcopy(cls.change_refresh_last_result)
                result["change_refresh_started"] = False
                result["change_refresh_inflight"] = False
                result["change_refresh_reused"] = True
                return result
            if cls.change_refresh_inflight:
                event = cls.change_refresh_event
                owner = False
            else:
                event = threading.Event()
                cls.change_refresh_event = event
                cls.change_refresh_inflight = True
                cls.change_refresh_last_error = ""
                owner = True

        if not owner:
            if not event.wait(timeout=120):
                raise PortalError("变更源表刷新仍在进行，请稍后查看。")
            with cls.change_refresh_lock:
                if cls.change_refresh_last_error:
                    raise PortalError(cls.change_refresh_last_error)
                if not cls.change_refresh_last_result:
                    raise PortalError("变更源表刷新未返回结果，请稍后重试。")
                result = copy.deepcopy(cls.change_refresh_last_result)
                result["change_refresh_started"] = False
                result["change_refresh_inflight"] = False
                result["change_refresh_reused"] = True
                return result

        try:
            result = cls.service.refresh_change_source()
            if not isinstance(result, dict):
                result = {}
            result = copy.deepcopy(result)
            result["change_refresh_started"] = True
            result["change_refresh_inflight"] = False
            result["change_refresh_reused"] = False
            cls.clear_payload_cache()
            with cls.change_refresh_lock:
                cls.change_refresh_last_result = copy.deepcopy(result)
                cls.change_refresh_last_error = ""
                cls.change_refresh_last_finished = time.monotonic()
            return result
        except Exception as exc:
            error = str(exc)
            with cls.change_refresh_lock:
                cls.change_refresh_last_error = error
                cls.change_refresh_last_finished = time.monotonic()
            if isinstance(exc, PortalError):
                raise
            raise PortalError(error) from exc
        finally:
            with cls.change_refresh_lock:
                cls.change_refresh_inflight = False
                event.set()

    def _reconcile_orphan_started_items(
        self, scope: str, ongoing: list[dict] | None, *, force: bool = False
    ) -> None:
        scope = PortalRuntime.auth_manager.normalize_scope(scope)
        now = time.monotonic()
        with PortalRuntime.orphan_reconcile_lock:
            if scope in PortalRuntime.orphan_reconcile_pending:
                return
            last = float(PortalRuntime.orphan_reconcile_last.get(scope) or 0)
            if not force and now - last < PortalRuntime.orphan_reconcile_interval_s:
                return
            PortalRuntime.orphan_reconcile_pending.add(scope)
            PortalRuntime.orphan_reconcile_last[scope] = now
        ongoing_copy = [
            dict(item) for item in (ongoing or []) if isinstance(item, dict)
        ]

        def _worker() -> None:
            try:
                result = self.service.reconcile_orphan_started_items(
                    scope=scope, ongoing_items=ongoing_copy
                )
                if int((result or {}).get("removed") or 0) > 0:
                    PortalRuntime.clear_payload_cache()
            except Exception as exc:
                warning = f"本地已开始状态清理失败: {exc}"
                if warning not in self.service._load_warnings:
                    self.service._load_warnings.append(warning)
            finally:
                with PortalRuntime.orphan_reconcile_lock:
                    PortalRuntime.orphan_reconcile_pending.discard(scope)

        try:
            threading.Thread(
                target=_worker,
                name=f"LANOrphanReconcile-{scope}",
                daemon=True,
            ).start()
        except Exception as exc:
            with PortalRuntime.orphan_reconcile_lock:
                PortalRuntime.orphan_reconcile_pending.discard(scope)
            warning = f"本地已开始状态清理启动失败: {exc}"
            if warning not in self.service._load_warnings:
                self.service._load_warnings.append(warning)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if self._redirect_root_oauth_callback(parsed):
            return
        if parsed.path in {
            "/",
            "/signature",
            "/signature/",
            "/engineer/mop",
            "/engineer/mop/",
        }:
            return self._send_html(portal_index_file())
        if parsed.path.startswith("/assets/"):
            relative_text = parsed.path[len("/assets/") :]
            relative = Path(*relative_text.split("/")) if relative_text else Path()
            return self._send_static_file(portal_asset_file(relative))
        if parsed.path == "/api/engineer/mop/bootstrap":
            session = self._require_auth_json()
            if session is None:
                return
            try:
                qs = parse_qs(parsed.query)
                scope = self._authorized_scope_or_error(
                    session, (qs.get("scope") or ["ALL"])[0]
                )
                ongoing = self._get_ongoing(scope)
                data = self.service.engineer_mop_bootstrap(
                    scope=scope,
                    ongoing_items=ongoing,
                )
                return self._send_json(
                    200,
                    {
                        "ok": True,
                        "data": self._with_auth_context(data, session),
                    },
                )
            except (PortalError, ValueError) as exc:
                return self._send_json(403, {"ok": False, "error": str(exc)})
        if parsed.path == "/api/engineer/mop/preview":
            session = self._require_auth_json()
            if session is None:
                return
            try:
                qs = parse_qs(parsed.query)
                scope = self._authorized_scope_or_error(
                    session, (qs.get("scope") or ["ALL"])[0]
                )
                data = self.service.preview_engineer_mop_attachment(
                    scope=scope,
                    mop_record_id=(qs.get("mop_record_id") or [""])[0],
                    file_token=(qs.get("file_token") or [""])[0],
                    file_name=(qs.get("file_name") or [""])[0],
                )
                return self._send_json(
                    200,
                    {
                        "ok": True,
                        "data": self._with_auth_context(data, session),
                    },
                )
            except (PortalError, ValueError) as exc:
                return self._send_json(403, {"ok": False, "error": str(exc)})
        if parsed.path == "/api/signatures/people":
            try:
                qs = parse_qs(parsed.query)
                session = self._current_session()
                record_id = (qs.get("record_id") or [""])[0]
                link_token = (qs.get("token") or [""])[0]
                if session is None and not self.service.validate_signature_link_token(
                    record_id=record_id,
                    token=link_token,
                ):
                    return self._send_json(403, {"ok": False, "error": "签名链接无效或已过期。"})
                data = self.service.signature_people(
                    scope=(qs.get("scope") or [""])[0],
                    query=(qs.get("q") or [""])[0],
                    record_id=record_id,
                    link_token=link_token if session is None else "",
                    limit=int((qs.get("limit") or ["80"])[0] or 80),
                    refresh=str((qs.get("refresh") or [""])[0]).lower() in {"1", "true", "yes"},
                )
                return self._send_json(200, {"ok": True, "data": data})
            except (PortalError, ValueError) as exc:
                return self._send_json(500, {"ok": False, "error": str(exc)})
        if parsed.path == "/api/signatures/image":
            try:
                qs = parse_qs(parsed.query)
                session = self._current_session()
                record_id = (qs.get("record_id") or [""])[0]
                if session is None and not self.service.validate_signature_link_token(
                    record_id=record_id,
                    token=(qs.get("token") or [""])[0],
                ):
                    return self._send_json(404, {"ok": False, "error": "签名链接无效或已过期。"})
                content, content_type = self.service.signature_image_bytes(
                    record_id=record_id,
                )
                return self._write_response(
                    200,
                    {
                        "Content-Type": content_type,
                        "Cache-Control": "private, max-age=300",
                    },
                    content,
                )
            except (PortalError, ValueError) as exc:
                return self._send_json(404, {"ok": False, "error": str(exc)})
        if parsed.path == "/api/auth/status":
            session = self._current_session()
            data = PortalRuntime.auth_manager.public_status(
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
                login_url = PortalRuntime.auth_manager.start_login(
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
                session_id, next_path = PortalRuntime.auth_manager.complete_login(
                    code=code,
                    state=state,
                    redirect_uri=redirect_uri,
                )
            except PortalError as exc:
                return self._send_html_message(400, "飞书登录失败", str(exc))
            return self._send_redirect(
                next_path,
                set_cookie=PortalRuntime.auth_manager.cookie_header(session_id),
            )
        if parsed.path == "/api/auth/logout":
            PortalRuntime.auth_manager.clear_session(
                self._cookie_value(AUTH_COOKIE_NAME)
            )
            return self._send_redirect(
                "/",
                set_cookie=PortalRuntime.auth_manager.clear_cookie_header(),
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
                allowed_options = PortalRuntime.auth_manager.filter_scope_options(
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
                data = PortalRuntime.auth_manager.filter_scope_overview(data, session)
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
                data = PortalRuntime.auth_manager.filter_handover_links(
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
            data = PortalRuntime.auth_manager.get_permissions_payload()
            return self._send_json(
                200,
                {"ok": True, "data": self._with_auth_context(data, session)},
            )
        if parsed.path == "/api/auth/permission-requests/current":
            user = session.get("user") if isinstance(session.get("user"), dict) else {}
            request = PortalRuntime.auth_manager.get_current_permission_request(
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
                refresh_result = PortalRuntime.request_source_refresh(force=True)
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
                refresh_result = PortalRuntime._refresh_repair_source_singleflight()
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
        if parsed.path == "/api/change-refresh":
            qs = parse_qs(parsed.query)
            try:
                scope = self._authorized_scope_or_error(
                    session, (qs.get("scope") or ["ALL"])[0]
                )
                refresh_result = PortalRuntime.request_change_source_refresh()
                ongoing = self._get_ongoing(scope)
                self._reconcile_orphan_started_items(scope, ongoing)
                data = self.service.get_bootstrap(
                    scope=scope, ongoing_items=ongoing
                )
                data.update(refresh_result)
                data["change_source_refreshed"] = True
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
            PortalRuntime.auth_manager.clear_session(
                self._cookie_value(AUTH_COOKIE_NAME)
            )
            body = json.dumps({"ok": True, "data": {}}, ensure_ascii=False).encode(
                "utf-8"
            )
            return self._write_response(
                200,
                {
                    "Content-Type": "application/json; charset=utf-8",
                    "Set-Cookie": PortalRuntime.auth_manager.clear_cookie_header(),
                },
                body,
            )
        if parsed.path == "/api/signatures/save":
            try:
                session = self._current_session()
                payload = self._read_json_body(max_bytes=4 * 1024 * 1024)
                record_id = str(payload.get("record_id") or "")
                link_token = str(payload.get("token") or "")
                if session is None and not self.service.validate_signature_link_token(
                    record_id=record_id,
                    token=link_token,
                ):
                    return self._send_json(403, {"ok": False, "error": "签名链接无效或已过期。"})
                data = self.service.save_signature_for_person(
                    record_id=record_id,
                    signature_png=str(payload.get("signature_png") or ""),
                    signer_name=str(payload.get("signer_name") or ""),
                    link_token=link_token if session is None else "",
                )
                if session is None:
                    self.service.mark_signature_link_token_used(
                        record_id=record_id,
                        token=link_token,
                    )
                return self._send_json(200, {"ok": True, "data": data})
            except (PortalError, ValueError, json.JSONDecodeError) as exc:
                return self._send_json(400, {"ok": False, "error": str(exc)})
        if parsed.path == "/api/signatures/send-link":
            session = self._require_auth_json()
            if session is None:
                return
            try:
                payload = self._read_json_body(max_bytes=64 * 1024)
                scope = self._authorized_scope_or_error(
                    session,
                    str(payload.get("scope") or "ALL"),
                )
                data = self.service.build_signature_link_message(
                    record_id=str(payload.get("record_id") or ""),
                    signer_name=str(payload.get("signer_name") or ""),
                    scope=scope,
                    request_base_url=self._request_base_url(),
                    created_by=str((session.get("user") or {}).get("open_id") if isinstance(session.get("user"), dict) else ""),
                )
                ok, message, results = _send_text_to_open_ids_guarded(
                    str(data.get("text") or ""),
                    [str(data.get("open_id") or "")],
                )
                if not ok:
                    return self._send_json(
                        400,
                        {
                            "ok": False,
                            "error": message or "签名链接发送失败。",
                            "data": {
                                "person": data.get("person") or {},
                                "link_url": data.get("link_url") or "",
                                "results": results,
                            },
                        },
                    )
                return self._send_json(
                    200,
                    {
                        "ok": True,
                        "data": {
                            "person": data.get("person") or {},
                            "link_url": data.get("link_url") or "",
                            "message": message,
                            "results": results,
                        },
                    },
                )
            except (PortalError, ValueError, json.JSONDecodeError) as exc:
                return self._send_json(400, {"ok": False, "error": str(exc)})
        session = self._require_auth_json()
        if session is None:
            return
        if parsed.path == "/api/engineer/mop/bind":
            try:
                payload = self._read_json_body(max_bytes=512 * 1024)
                scope = self._authorized_scope_or_error(
                    session, str(payload.get("scope") or "ALL")
                )
                payload["scope"] = scope
                user = session.get("user") if isinstance(session.get("user"), dict) else {}
                data = self.service.bind_engineer_mop_notice(
                    payload=payload,
                    updated_by=str(user.get("open_id") or ""),
                )
                PortalRuntime.clear_payload_cache()
                return self._send_json(
                    200,
                    {
                        "ok": True,
                        "data": self._with_auth_context(data, session),
                    },
                )
            except (PortalError, ValueError, json.JSONDecodeError) as exc:
                return self._send_json(403, {"ok": False, "error": str(exc)})
        if parsed.path == "/api/engineer/mop/fill":
            try:
                payload = self._read_json_body(max_bytes=4 * 1024 * 1024)
                scope = self._authorized_scope_or_error(
                    session, str(payload.get("scope") or "ALL")
                )
                data = self.service.fill_engineer_mop_file(
                    **engineer_mop_fill_kwargs_from_payload(payload, scope=scope),
                )
                return self._send_json(
                    200,
                    {
                        "ok": True,
                        "data": self._with_auth_context(data, session),
                    },
                )
            except (PortalError, ValueError, json.JSONDecodeError) as exc:
                return self._send_json(403, {"ok": False, "error": str(exc)})
        if parsed.path == "/api/engineer/mop/upload-signed":
            try:
                payload = self._read_json_body(max_bytes=4 * 1024 * 1024)
                scope = self._authorized_scope_or_error(
                    session, str(payload.get("scope") or "ALL")
                )
                user = session.get("user") if isinstance(session.get("user"), dict) else {}
                data = self.service.upload_signed_engineer_mop_file(
                    **engineer_mop_upload_signed_kwargs_from_payload(
                        payload,
                        scope=scope,
                        operator_open_id=str(user.get("open_id") or ""),
                        operator_name=str(user.get("name") or ""),
                    ),
                )
                PortalRuntime.clear_payload_cache()
                return self._send_json(
                    200,
                    {
                        "ok": True,
                        "data": self._with_auth_context(data, session),
                    },
                )
            except (PortalError, ValueError, json.JSONDecodeError) as exc:
                return self._send_json(403, {"ok": False, "error": str(exc)})
        if parsed.path == "/api/engineer/mop/reset":
            try:
                payload = self._read_json_body(max_bytes=256 * 1024)
                scope = self._authorized_scope_or_error(
                    session, str(payload.get("scope") or "ALL")
                )
                data = self.service.reset_engineer_mop_file(
                    scope=scope,
                    filled_file_path=str(payload.get("filled_file_path") or ""),
                    mop_record_id=str(payload.get("mop_record_id") or ""),
                    file_token=str(payload.get("file_token") or ""),
                    file_name=str(payload.get("file_name") or ""),
                )
                return self._send_json(
                    200,
                    {
                        "ok": True,
                        "data": self._with_auth_context(data, session),
                    },
                )
            except (PortalError, ValueError, json.JSONDecodeError) as exc:
                return self._send_json(403, {"ok": False, "error": str(exc)})
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
                    PortalRuntime.clear_payload_cache()
                    PortalRuntime.enqueue_initial_message_or_upload_job(job_id)
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
        if parsed.path in {"/api/change-target-candidates", "/api/notice-target-candidates"}:
            try:
                payload = self._read_json_body()
                scope = self._authorized_scope_or_error(
                    session, payload.get("scope") or "ALL"
                )
                if parsed.path == "/api/change-target-candidates":
                    data = self.service.lookup_change_target_candidates(
                        scope=scope,
                        title=payload.get("title") or "",
                        start_time=payload.get("start_time") or "",
                        end_time=payload.get("end_time") or "",
                        action=payload.get("action") or "update",
                        content=payload.get("content") or "",
                        reason=payload.get("reason") or "",
                        impact=payload.get("impact") or "",
                        progress=payload.get("progress") or "",
                        text=payload.get("text") or "",
                    )
                else:
                    data = self.service.lookup_notice_target_candidates(
                        work_type=payload.get("work_type") or "maintenance",
                        scope=scope,
                        title=payload.get("title") or "",
                        start_time=payload.get("start_time") or "",
                        end_time=payload.get("end_time") or "",
                        action=payload.get("action") or "update",
                        content=payload.get("content") or "",
                        reason=payload.get("reason") or "",
                        impact=payload.get("impact") or "",
                        progress=payload.get("progress") or "",
                        text=payload.get("text") or "",
                    )
                return self._send_json(200, {"ok": True, "data": data})
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
                callback = PortalRuntime.ongoing_delete_callback
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
                PortalRuntime.clear_payload_cache()
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
                data = PortalRuntime.auth_manager.filter_handover_links(data, session)
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
                data = PortalRuntime.auth_manager.create_permission_request(
                    open_id=str(user.get("open_id") or ""),
                    name=str(user.get("name") or user.get("en_name") or "飞书用户"),
                    scopes=payload.get("scopes") or [],
                    reason=str(payload.get("reason") or ""),
                )
                code = str(data.pop("code") or "")
                recipients = PortalRuntime.auth_manager.admin_open_ids()
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
                    PortalRuntime.auth_manager.mark_permission_request_notify_failed(
                        str(data.get("request_id") or "")
                    )
                    raise PortalError(f"通知管理员失败: {message}")
                activated = PortalRuntime.auth_manager.activate_permission_request(
                    str(data.get("request_id") or "")
                )
                data.update(activated)
                PortalRuntime.auth_manager.supersede_other_permission_requests(
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
                permissions = PortalRuntime.auth_manager.confirm_permission_request(
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
                data, changed_open_ids = PortalRuntime.auth_manager.save_permissions_payload(
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
                    session, payload.get("scope") or PortalRuntime.auth_manager.default_scope(session) or "ALL"
                )
                drafts = payload.get("drafts") or []
                self.service.assert_generated_drafts_allowed(drafts, scope=scope)
                generated = self.service.generate_templates(drafts)
                return self._send_json(200, {"ok": True, "data": {"items": generated}})
            except (PortalError, ValueError, json.JSONDecodeError) as exc:
                return self._send_json(403, {"ok": False, "error": str(exc)})
        if parsed.path == "/api/send-generated":
            if PortalRuntime.notice_callback is None:
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
                    session, payload.get("scope") or PortalRuntime.auth_manager.default_scope(session) or "ALL"
                )
                items = payload.get("items") or []
                self.service.assert_generated_items_allowed(items, scope=scope)
                results = self.service.send_generated_templates(
                    items, notice_callback=PortalRuntime.notice_callback
                )
                return self._send_json(200, {"ok": True, "data": {"items": results}})
            except (PortalError, ValueError, json.JSONDecodeError) as exc:
                return self._send_json(403, {"ok": False, "error": str(exc)})
        return self._send_json(404, {"ok": False, "error": "Not Found"})

    def _get_ongoing(self, scope: str) -> list[dict]:
        def _is_ended(item: dict) -> bool:
            status = str(item.get("status") or "").strip()
            if status == "结束":
                return True
            text = str(item.get("text") or item.get("content") or "").strip()
            if not text:
                return False
            info = extract_event_info(text) or {}
            return str(info.get("status") or "").strip() == "结束"

        try:
            snapshot = PortalRuntime.state_store.get_ongoing_snapshot()
            if snapshot.get("exists"):
                PortalRuntime.last_ongoing_error = ""
                filtered = [
                    dict(item)
                    for item in snapshot.get("items", [])
                    if isinstance(item, dict)
                    and not _is_ended(item)
                    and self.service._scope_matches_item(scope, item)
                ]
                return self.service._merge_ongoing_items(scope, filtered)
        except Exception as exc:
            warning = f"SQLite 进行中状态读取失败: {exc}"
            PortalRuntime.last_ongoing_error = warning
            logging.warning(warning)
        callback = PortalRuntime.ongoing_callback
        if callback is None:
            if not PortalRuntime.last_ongoing_error:
                PortalRuntime.last_ongoing_error = ""
            return []
        try:
            result = callback("ALL")
            if isinstance(result, list):
                try:
                    PortalRuntime.state_store.replace_ongoing_items(result)
                except Exception as store_exc:
                    logging.warning(f"SQLite 进行中状态写入失败: {store_exc}")
            PortalRuntime.last_ongoing_error = ""
        except Exception as exc:
            warning = f"主界面进行中状态读取失败: {exc}"
            PortalRuntime.last_ongoing_error = warning
            logging.warning(warning)
            return []
        if not isinstance(result, list):
            return []
        filtered = [
            dict(item)
            for item in result
            if isinstance(item, dict)
            and not _is_ended(item)
            and self.service._scope_matches_item(scope, item)
        ]
        return self.service._merge_ongoing_items(scope, filtered)

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
            cls._update_message_queue_positions_locked()
        cls.ensure_message_workers()
        cls.message_queue_event.set()

    @classmethod
    def _update_message_queue_positions_locked(cls) -> None:
        try:
            queued_items = cls.state_store.list_runtime_queue_items(
                "message",
                statuses=("queued",),
                due_only=False,
                limit=500,
            )
        except Exception:
            queued_items = []
        total = len(queued_items)
        for index, item in enumerate(queued_items, start=1):
            queued_job_id = str((item or {}).get("job_id") or "").strip()
            if not queued_job_id:
                continue
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
        # 只合并明确单楼入口的维保个人消息。ALL/园区等汇总入口可能包含
        # 不同楼栋、不同 openid 收件人，合并会造成误发或漏发。
        if scope not in {"110", "A", "B", "C", "D", "E", "H"}:
            return [primary_job_id]
        wait_seconds = float(getattr(cls, "message_batch_wait_seconds", 0.0) or 0.0)
        if wait_seconds > 0:
            time.sleep(wait_seconds)
        batch = [primary_job_id]
        max_jobs = max(1, int(getattr(cls, "message_batch_max_jobs", 20) or 20))
        with cls.message_queue_lock:
            try:
                queued_items = cls.state_store.list_runtime_queue_items(
                    "message",
                    statuses=("queued",),
                    due_only=True,
                    limit=max_jobs * 4,
                )
            except Exception:
                queued_items = []
            if not queued_items:
                return batch
            for item in queued_items:
                queued_job_id = str((item or {}).get("job_id") or "").strip()
                if not queued_job_id:
                    continue
                if len(batch) >= max_jobs:
                    break
                if cls._message_job_scope(queued_job_id) == scope:
                    if not cls._runtime_queue_job_processable("message", queued_job_id):
                        continue
                    try:
                        leased = cls.state_store.lease_runtime_queue_item(
                            "message",
                            queued_job_id,
                            lease_seconds=30,
                        )
                    except Exception:
                        leased = False
                    if leased:
                        batch.append(queued_job_id)
            cls._update_message_queue_positions_locked()
        return batch

    @classmethod
    def _release_message_scope(cls, job_id: str) -> None:
        scope = cls._message_job_scope(job_id)
        if not scope:
            return
        with cls.message_queue_lock:
            cls.message_scope_inflight.discard(scope)
            cls.message_queue_event.set()

    @classmethod
    def _process_message_batch(cls, batched_prepared: list[tuple[str, dict]]) -> None:
        if not batched_prepared:
            return
        if len(batched_prepared) == 1:
            job_id, prepared = batched_prepared[0]
            ok, message = cls.service.send_action_personal_message(prepared)
            if not ok:
                warning = f"个人消息发送失败，已继续上传多维，可复制通告文本：{message}"
                cls.service.mark_job(
                    job_id,
                    phase="upload_queued",
                    message=str(warning),
                    message_error=str(message or ""),
                    message_warning=warning,
                    message_failed=True,
                    message_failed_continue=True,
                    message_sent=False,
                    message_signature=str(prepared.get("message_signature") or ""),
                    message_queue_position=0,
                    queue_position=0,
                )
                try:
                    cls.state_store.mark_runtime_queue_item(
                        "message",
                        job_id,
                        "done",
                        error=message,
                    )
                except Exception:
                    pass
                cls.enqueue_action_job(job_id)
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
                warning = f"个人消息发送失败，已继续上传多维，可复制通告文本：{message}"
                cls.service.mark_job(
                    job_id,
                    phase="upload_queued",
                    message=str(warning),
                    message_error=str(message or ""),
                    message_warning=warning,
                    message_failed=True,
                    message_failed_continue=True,
                    message_sent=False,
                    message_signature=str(prepared.get("message_signature") or ""),
                    recipient_results=copy.deepcopy(results),
                    message_queue_position=0,
                    queue_position=0,
                )
                try:
                    cls.state_store.mark_runtime_queue_item(
                        "message",
                        job_id,
                        "done",
                        error=message,
                    )
                except Exception:
                    pass
                cls.enqueue_action_job(job_id)
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
                for _attempt in range(10):
                    try:
                        leased = cls.state_store.lease_runtime_queue_items(
                            "message",
                            limit=1,
                            lease_seconds=30,
                        )
                    except Exception:
                        leased = []
                    if not leased:
                        cls.message_queue_event.clear()
                        return ""
                    job_id = str((leased[0] or {}).get("job_id") or "").strip()
                    if not cls._runtime_queue_job_processable("message", job_id):
                        continue
                    scope = cls._message_job_scope(job_id)
                    if scope and scope in cls.message_scope_inflight:
                        try:
                            cls.state_store.requeue_runtime_queue_item(
                                "message",
                                job_id,
                                available_at=time.time() + 0.5,
                                error="waiting_for_same_scope_message_batch",
                            )
                        except Exception:
                            pass
                        continue
                    if scope:
                        cls.message_scope_inflight.add(scope)
                    cls._update_message_queue_positions_locked()
                    return job_id
                cls.message_queue_event.clear()
                return ""
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
                if queue_name == "qt_action":
                    cls.action_queue_event.clear()
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
            cls._update_queue_positions_locked()
        cls.ensure_action_worker()
        cls.action_queue_event.set()

    @classmethod
    def _update_queue_positions_locked(cls) -> None:
        try:
            queued_items = cls.state_store.list_runtime_queue_items(
                "qt_action",
                statuses=("queued",),
                due_only=False,
                limit=500,
            )
        except Exception:
            queued_items = []
        total = len(queued_items)
        for index, item in enumerate(queued_items, start=1):
            queued_job_id = str((item or {}).get("job_id") or "").strip()
            if not queued_job_id:
                continue
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
        deadline_at = time.time() + cls.action_upload_timeout_s
        with cls.upload_wait_lock:
            cls.upload_wait_event.set()
        try:
            cls.state_store.upsert_runtime_queue_item(
                "upload_wait",
                job_id,
                payload={"deadline_at": deadline_at},
                available_at=deadline_at,
            )
        except Exception:
            pass

    @classmethod
    def _upload_wait_worker_loop(cls) -> None:
        while True:
            cls.upload_wait_event.wait(timeout=5.0)
            if cls.upload_wait_stop or cls.action_worker_stop:
                return
            pending_count = cls._scan_upload_wait_queue_once()
            if pending_count == 0:
                with cls.upload_wait_lock:
                    cls.upload_wait_event.clear()

    @classmethod
    def _scan_upload_wait_queue_once(cls) -> int:
        now = time.time()
        pending_count = 0
        timed_out: list[str] = []
        try:
            items = cls.state_store.list_runtime_queue_items(
                "upload_wait",
                statuses=("queued", "processing"),
                due_only=False,
                limit=500,
            )
        except Exception:
            items = []
        for item in items:
            job_id = str((item or {}).get("job_id") or "").strip()
            if not job_id:
                continue
            job = cls.service.get_job(job_id) or {}
            phase = str(job.get("phase") or "")
            payload = (item or {}).get("payload") if isinstance(item, dict) else {}
            payload = payload if isinstance(payload, dict) else {}
            try:
                deadline_at = float(payload.get("deadline_at") or item.get("available_at") or 0)
            except Exception:
                deadline_at = 0.0
            if phase in {"success", "failed"} or phase != "uploading":
                try:
                    cls.state_store.mark_runtime_queue_item(
                        "upload_wait",
                        job_id,
                        "done" if phase == "success" else "failed" if phase == "failed" else "cancelled",
                    )
                except Exception:
                    pass
                continue
            if deadline_at and now >= deadline_at:
                timed_out.append(job_id)
                continue
            pending_count += 1
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
        return pending_count

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
            for item in fields.get("过程现场图片", []) or []:
                if "file_token" in item:
                    existing_extra_tokens.append(item["file_token"])
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
        file_tokens: list[str] | None = None,
        extra_file_tokens: list[str] | None = None,
        existing_file_tokens: list[str] | None = None,
        existing_extra_file_tokens: list[str] | None = None,
        existing_response_time: str | None = None,
    ) -> NoticePayload:
        prepared = prepared if isinstance(prepared, dict) else {}
        notice_type = str(prepared.get("notice_type") or "").strip()
        level = str(prepared.get("level") or "").strip()
        if notice_type in ("设备变更", "变更通告"):
            level = PortalRuntime._normalize_change_notice_level(
                text=str(prepared.get("text") or ""),
                level=level,
            )
        return NoticePayload(
            text=str(prepared.get("text") or ""),
            level=level or None,
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
            file_tokens=list(file_tokens or []) or None,
            extra_file_tokens=list(extra_file_tokens or []) or None,
            existing_file_tokens=list(existing_file_tokens or []) or None,
            existing_extra_file_tokens=list(existing_extra_file_tokens or []) or None,
            existing_response_time=existing_response_time or None,
            transfer_to_overhaul=prepared.get("transfer_to_overhaul"),
            recover=bool(prepared.get("recover_selected", False)),
            robot_group_choice="auto",
            maintenance_cycle=str(prepared.get("maintenance_cycle") or "").strip() or None,
        )

    @staticmethod
    def _normalize_change_notice_level(*, text: str = "", level: str = "") -> str:
        explicit_level = str(level or "").strip()
        if explicit_level:
            return explicit_level
        info = extract_event_info(text or "") or {}
        raw_level = str(info.get("level") or "").strip()
        if not raw_level:
            match = re.search(
                r"【等级】(?P<value>.*?)(?=【[^】]+】|$)",
                text or "",
                re.S,
            )
            raw_level = str(match.group("value") if match else "").strip()
        normalized = raw_level.upper()
        if normalized in {"I3", "I2", "I1", "E0", "/"}:
            return normalized
        if "超低" in raw_level:
            return "I3"
        if "低" in raw_level:
            return "I3"
        if "中" in raw_level:
            return "I2"
        if "高" in raw_level:
            return "I1"
        return "I3"

    @staticmethod
    def _notice_supports_site_image_field(notice_type: str) -> bool:
        return MaintenancePortalService._end_site_photo_required(
            notice_type=notice_type,
        )

    @staticmethod
    def _end_site_photo_required(notice_type: str) -> bool:
        return MaintenancePortalService._end_site_photo_required(
            notice_type=notice_type,
        )

    @classmethod
    def _has_extra_images_payload(cls, payload: dict) -> bool:
        images = payload.get("extra_images") if isinstance(payload, dict) else []
        if not isinstance(images, list):
            return False
        for item in images:
            if isinstance(item, dict):
                if str(
                    item.get("upload_id")
                    or item.get("file_token")
                    or item.get("token")
                    or ""
                ).strip():
                    return True
        return False

    @classmethod
    def _upload_extra_images_for_notice(
        cls,
        payload: dict,
        notice_type: str,
    ) -> tuple[bool, str, list[str], list[str]]:
        images = payload.get("extra_images") if isinstance(payload, dict) else []
        if not isinstance(images, list):
            return True, "", [], []
        uploaded_tokens: list[str] = []
        for index, entry in enumerate(images, start=1):
            entry = entry if isinstance(entry, dict) else {}
            existing_token = str(entry.get("file_token") or entry.get("token") or "").strip()
            if existing_token:
                uploaded_tokens.append(existing_token)
                continue
            upload_id = str(entry.get("upload_id") or "").strip()
            if upload_id:
                attachment = cls.state_store.get_notice_upload_attachment(upload_id)
                if not attachment:
                    return False, "现场照片已过期或不存在，请重新添加。", [], []
                image_bytes = bytes(attachment.get("content") or b"")
                file_name = str(
                    entry.get("file_name")
                    or attachment.get("file_name")
                    or f"site_photo_{index}.png"
                ).strip()
                if not image_bytes:
                    return False, "现场照片内容为空，请重新添加。", [], []
                success, result = upload_media_to_feishu(image_bytes, file_name=file_name)
                if not success:
                    return False, str(result or "现场照片上传失败。"), [], []
                cls.state_store.mark_notice_upload_attachment_used(upload_id)
                uploaded_tokens.append(str(result or "").strip())
                continue
        if not uploaded_tokens:
            return True, "", [], []
        if cls._notice_supports_site_image_field(notice_type):
            return True, "", [], uploaded_tokens
        return True, "", uploaded_tokens, []

    @staticmethod
    def _remote_record_not_found(message: object) -> bool:
        text = str(message or "")
        return any(
            token in text
            for token in (
                "1254043",
                "RecordidNotFound",
                "RecordIdNotFound",
                "record not found",
                "记录不存在",
            )
        )

    @staticmethod
    def _candidate_target_record_id(candidate: dict) -> str:
        if not isinstance(candidate, dict):
            return ""
        return str(
            candidate.get("target_record_id")
            or candidate.get("record_id")
            or ""
        ).strip()

    @staticmethod
    def _notice_work_type_from_notice_type(notice_type: str) -> str:
        notice_type = str(notice_type or "").strip()
        return {
            "事件通告": "event",
            "维保通告": "maintenance",
            "设备变更": "change",
            "变更通告": "change",
            "设备检修": "repair",
        }.get(notice_type, "")

    @classmethod
    def _local_upload_lock_for_key(cls, key: str) -> threading.RLock:
        key = str(key or "").strip()
        with cls.local_upload_locks_lock:
            lock = cls.local_upload_locks.get(key)
            if lock is None:
                lock = threading.RLock()
                cls.local_upload_locks[key] = lock
            return lock

    @classmethod
    def _release_local_upload_lock_for_key(cls, key: str, lock: threading.RLock) -> None:
        # Keep the lock object around. Removing it immediately can let a third
        # request create a new lock while a second request is still waiting on
        # the old one, which would reopen the duplicate-create race.
        return

    @classmethod
    def _local_upload_dedupe_key(cls, data: dict, notice_type: str) -> str:
        data = data if isinstance(data, dict) else {}
        notice_type = str(notice_type or data.get("notice_type") or "").strip()
        active_item_id = str(data.get("active_item_id") or "").strip()
        if active_item_id:
            return f"{notice_type}:active:{active_item_id}"
        record_id = str(data.get("record_id") or "").strip()
        if record_id:
            return f"{notice_type}:record:{record_id}"
        info = extract_notice_info(str(data.get("text") or "")) or {}
        unique_key = str(info.get("unique_key") or "").strip()
        if unique_key:
            return f"{notice_type}:unique:{hashlib.sha256(unique_key.encode('utf-8', errors='ignore')).hexdigest()}"
        return ""

    @classmethod
    def _existing_target_for_local_upload(cls, data: dict, notice_type: str) -> str:
        data = normalize_notice_identity_payload(dict(data or {}), action="upload")
        work_type = str(data.get("work_type") or data.get("lan_work_type") or "").strip()
        if not work_type:
            work_type = cls._notice_work_type_from_notice_type(notice_type)
        try:
            identity = cls.state_store.resolve_notice_identity(
                work_type=work_type,
                active_item_id=str(data.get("active_item_id") or "").strip(),
                source_record_id=str(data.get("source_record_id") or "").strip(),
                target_record_id="",
            )
        except Exception:
            identity = None
        target_record_id = (
            str((identity or {}).get("target_record_id") or "").strip()
            if isinstance(identity, dict)
            else ""
        )
        if not target_record_id:
            return ""
        guard = external_real_write_guard()
        if guard.get("mock_external"):
            return target_record_id
        ok_query, query_result = query_record_by_id(target_record_id, notice_type)
        if ok_query:
            return target_record_id
        if cls._remote_record_not_found(query_result):
            return ""
        return target_record_id

    @classmethod
    def _remember_local_upload_target(
        cls,
        data: dict,
        *,
        notice_type: str,
        target_record_id: str,
    ) -> None:
        target_record_id = str(target_record_id or "").strip()
        if not target_record_id:
            return
        payload = normalize_notice_identity_payload(
            {
                **dict(data or {}),
                "record_id": target_record_id,
                "target_record_id": target_record_id,
                "feishu_record_id": target_record_id,
                "raw_record_id": target_record_id,
                "_record_id_kind": "target",
                "_is_placeholder_record": False,
            },
            action="update",
        )
        if not str(payload.get("work_type") or "").strip():
            payload["work_type"] = (
                str(payload.get("lan_work_type") or "").strip()
                or cls._notice_work_type_from_notice_type(notice_type)
            )
        section = "event" if notice_type == "事件通告" else "other"
        try:
            cls.state_store.upsert_notice_identity(payload, origin="qt_upload")
        except Exception:
            pass

    @classmethod
    def _source_record_id_from_prepared_start(cls, prepared: dict) -> str:
        prepared = prepared if isinstance(prepared, dict) else {}
        source_record_id = str(prepared.get("source_record_id") or "").strip()
        if source_record_id:
            return source_record_id
        record_id = str(prepared.get("record_id") or "").strip()
        if not record_id or is_local_record_id(record_id):
            return ""
        if str(prepared.get("source_app_token") or "").strip() or str(
            prepared.get("source_table_id") or ""
        ).strip():
            return record_id
        return ""

    @classmethod
    def _existing_target_for_prepared_start(cls, prepared: dict, notice_type: str) -> str:
        prepared = normalize_notice_identity_payload(dict(prepared or {}), action="start")
        work_type = str(prepared.get("work_type") or "").strip()
        if not work_type:
            work_type = cls._notice_work_type_from_notice_type(notice_type)
        active_item_id = str(prepared.get("active_item_id") or "").strip()
        source_record_id = cls._source_record_id_from_prepared_start(prepared)

        target_record_id = ""
        try:
            identity = cls.state_store.resolve_notice_identity(
                work_type=work_type,
                active_item_id=active_item_id,
                source_record_id=source_record_id,
                target_record_id="",
            )
        except Exception:
            identity = None
        if isinstance(identity, dict):
            target_record_id = str(identity.get("target_record_id") or "").strip()
        if not target_record_id and (active_item_id or source_record_id):
            try:
                target_record_id = cls.service._target_record_id_from_work_status(
                    work_type=work_type,
                    active_item_id=active_item_id,
                    source_record_id=source_record_id,
                )
            except Exception:
                target_record_id = ""
        if not target_record_id:
            return ""
        guard = external_real_write_guard()
        if guard.get("mock_external"):
            return target_record_id
        ok_query, query_result = query_record_by_id(target_record_id, notice_type)
        if ok_query:
            return target_record_id
        if cls._remote_record_not_found(query_result):
            return ""
        return target_record_id

    @staticmethod
    def _target_candidate_score(prepared: dict, candidate: dict) -> int:
        prepared = prepared if isinstance(prepared, dict) else {}
        candidate = candidate if isinstance(candidate, dict) else {}
        score = 0
        if candidate.get("date_matched"):
            score += 30
        if candidate.get("title_matched"):
            score += 35
        score += int(candidate.get("business_match_count") or 0) * 18
        source_codes = {
            str(item or "").strip().upper()
            for item in (prepared.get("building_codes") or [])
            if str(item or "").strip()
        }
        candidate_codes = {
            str(item or "").strip().upper()
            for item in (candidate.get("building_codes") or [])
            if str(item or "").strip()
        }
        if source_codes and candidate_codes and source_codes & candidate_codes:
            score += 20
        if str(candidate.get("status") or "").strip() not in {"结束", "已结束"}:
            score += 10
        if str(prepared.get("level") or "").strip() and str(prepared.get("level") or "").strip() in str(candidate.get("fields") or ""):
            score += 5
        if str(prepared.get("specialty") or "").strip() and str(prepared.get("specialty") or "").strip() in str(candidate.get("fields") or ""):
            score += 5
        return score

    @classmethod
    def _enrich_prepared_notice_lookup_fields(cls, prepared: dict) -> dict:
        prepared = normalize_notice_identity_payload(dict(prepared or {}))
        text = str(prepared.get("text") or prepared.get("content") or "").strip()
        if not text:
            return prepared
        sections = {}
        try:
            sections = cls.service._parse_notice_sections(text)
        except Exception:
            sections = {}
        parsed = extract_notice_info(text) or {}

        def section_value(*names: str) -> str:
            for name in names:
                value = str(sections.get(name) or "").strip()
                if value:
                    return value
            return ""

        if not str(prepared.get("title") or "").strip():
            prepared["title"] = (
                str(parsed.get("title") or "").strip()
                or section_value("名称", "标题", "事件描述", "通告名称")
            )
        if not str(prepared.get("start_time") or "").strip() and not str(
            prepared.get("end_time") or ""
        ).strip():
            time_text = str(parsed.get("time_str") or "").strip() or section_value("时间")
            if time_text:
                prepared["start_time"] = time_text
        if not str(prepared.get("content") or "").strip():
            prepared["content"] = section_value("内容") or text
        if not str(prepared.get("reason") or "").strip():
            prepared["reason"] = (
                str(parsed.get("reason") or "").strip()
                or section_value("原因", "故障原因")
            )
        if not str(prepared.get("impact") or "").strip():
            prepared["impact"] = section_value("影响", "影响范围")
        if not str(prepared.get("progress") or "").strip():
            prepared["progress"] = section_value("进度", "完成情况", "解决方案")
        return prepared

    @classmethod
    def _target_selection_response(
        cls,
        prepared: dict,
        *,
        stale_record_id: str,
        message: str,
        action_name: str,
    ) -> dict:
        prepared = cls._enrich_prepared_notice_lookup_fields(prepared)
        work_type = str(prepared.get("work_type") or "").strip()
        if not work_type:
            work_type = cls._notice_work_type_from_notice_type(
                str(prepared.get("notice_type") or "")
            )
        title = str(prepared.get("title") or "").strip()
        candidates: list[dict] = []
        lookup_error = ""
        if title:
            try:
                result = cls.service.lookup_notice_target_candidates(
                    work_type=work_type,
                    scope=str(prepared.get("scope") or "ALL").strip() or "ALL",
                    title=title,
                    start_time=str(prepared.get("start_time") or ""),
                    end_time=str(prepared.get("end_time") or ""),
                    action=str(prepared.get("action") or "update"),
                    content=str(prepared.get("content") or ""),
                    reason=str(prepared.get("reason") or ""),
                    impact=str(prepared.get("impact") or ""),
                    progress=str(prepared.get("progress") or ""),
                    text=str(prepared.get("text") or ""),
                    limit=20,
                )
                for item in result.get("candidates") or []:
                    if not isinstance(item, dict):
                        continue
                    target_record_id = cls._candidate_target_record_id(item)
                    if not target_record_id or target_record_id == stale_record_id:
                        continue
                    copied = dict(item)
                    copied["target_record_id"] = target_record_id
                    copied["record_id"] = target_record_id
                    copied["match_score"] = cls._target_candidate_score(prepared, copied)
                    candidates.append(copied)
            except Exception as exc:
                lookup_error = str(exc)
        candidates.sort(
            key=lambda item: (
                -int(item.get("match_score") or 0),
                0 if item.get("date_matched") else 1,
                str(item.get("start_time") or ""),
            )
        )
        detail = str(message or "目标多维记录不存在。")
        if lookup_error:
            detail = f"{detail} 候选查询失败：{lookup_error}"
        elif candidates:
            detail = f"{detail} 已找到 {len(candidates)} 条可能相关记录，请选择后重试。"
        else:
            detail = f"{detail} 未找到可选择的相关记录。"
        return {
            "ok": False,
            "name": action_name,
            "message": detail,
            "record_id": stale_record_id,
            "real_record_id": "",
            "needs_target_selection": bool(candidates),
            "target_record_missing": True,
            "target_candidates": candidates,
        }

    @classmethod
    def _rebind_missing_target_record(
        cls,
        prepared: dict,
        *,
        stale_record_id: str,
    ) -> tuple[str, str]:
        prepared = normalize_notice_identity_payload(dict(prepared or {}))
        stale_record_id = str(stale_record_id or "").strip()
        title = str(prepared.get("title") or "").strip()
        if not title:
            prepared = cls._enrich_prepared_notice_lookup_fields(prepared)
            title = str(prepared.get("title") or "").strip()
        if not title:
            return "", "目标多维记录不存在，且通告缺少标题，无法自动重新关联。"
        work_type = str(prepared.get("work_type") or "").strip()
        if not work_type:
            notice_type = str(prepared.get("notice_type") or "").strip()
            work_type = cls._notice_work_type_from_notice_type(notice_type)
        scope = str(prepared.get("scope") or "ALL").strip() or "ALL"
        try:
            result = cls.service.lookup_notice_target_candidates(
                work_type=work_type,
                scope=scope,
                title=title,
                start_time=str(prepared.get("start_time") or ""),
                end_time=str(prepared.get("end_time") or ""),
                action=str(prepared.get("action") or "update"),
                content=str(prepared.get("content") or ""),
                reason=str(prepared.get("reason") or ""),
                impact=str(prepared.get("impact") or ""),
                progress=str(prepared.get("progress") or ""),
                text=str(prepared.get("text") or ""),
                limit=20,
            )
        except Exception as exc:
            return "", f"目标多维记录不存在，自动查询候选失败：{exc}"
        candidates = [
            item
            for item in (result.get("candidates") or [])
            if isinstance(item, dict)
            and cls._candidate_target_record_id(item)
            and cls._candidate_target_record_id(item) != stale_record_id
        ]
        date_matched = [item for item in candidates if item.get("date_matched")]
        if len(date_matched) == 1:
            candidates = date_matched
        if len(candidates) != 1:
            return (
                "",
                "目标多维记录不存在，且无法自动唯一匹配；请在前端选择正确的目标记录后再更新。"
                if candidates
                else "目标多维记录不存在，且未找到可重新关联的目标记录。",
            )
        record_id = cls._candidate_target_record_id(candidates[0])
        try:
            cls.state_store.upsert_notice_identity(
                {
                    **prepared,
                    "target_record_id": record_id,
                    "record_id": record_id,
                    "_record_id_kind": "target",
                },
                origin="auto_rebind_target",
            )
        except Exception:
            pass
        return record_id, ""

    @staticmethod
    def _undo_restore_fields(notice_type: str, fields: dict) -> dict:
        fields = fields if isinstance(fields, dict) else {}
        writable_names = {
            str(name or "").strip()
            for name in get_field_config(notice_type).values()
            if str(name or "").strip()
        }
        if not writable_names:
            return dict(fields)
        return {
            str(name): value
            for name, value in fields.items()
            if str(name or "").strip() in writable_names
        }

    @classmethod
    def _create_backend_undo_checkpoint(
        cls,
        action_type: str,
        context: dict,
        *,
        remote_fields: dict | None = None,
        remote_missing: bool = False,
        job_id: str = "",
    ) -> str:
        creator = getattr(cls.service, "create_notice_undo_checkpoint", None)
        if not callable(creator):
            return ""
        try:
            return creator(
                action_type,
                context,
                remote_fields=remote_fields or {},
                remote_missing=remote_missing,
                job_id=job_id,
                created_by=str((context or {}).get("_auth_open_id") or ""),
                scope=str((context or {}).get("scope") or "ALL"),
            )
        except Exception as exc:
            raise PortalError(f"创建回退点失败: {exc}") from exc

    @classmethod
    def execute_notice_undo(cls, undo_id: str, *, job_id: str = "", requested_by: str = "") -> dict:
        undo = cls.state_store.get_notice_undo_action(undo_id)
        if not undo:
            raise PortalError("回退记录不存在。")
        if str(undo.get("status") or "") != "available":
            raise PortalError("该回退记录不可用或已处理。")
        if float(undo.get("expires_at") or 0) <= time.time():
            cls.state_store.mark_notice_undo_action(undo_id, "expired", error="回退记录已过期")
            raise PortalError("该回退记录已过期。")
        notice_type = str(undo.get("notice_type") or "").strip()
        target_record_id = str(undo.get("target_record_id") or "").strip()
        action_type = str(undo.get("action_type") or "").strip().lower()
        remote = undo.get("remote") if isinstance(undo.get("remote"), dict) else {}
        remote_fields = cls._undo_restore_fields(
            notice_type,
            remote.get("fields") if isinstance(remote.get("fields"), dict) else {},
        )
        restored_record_id = target_record_id
        remote_message = ""
        guard = external_real_write_guard()
        if remote_fields and not bool(remote.get("missing")):
            if guard["mock_external"]:
                remote_message = "mock external undo skipped"
            elif not guard["real_write_allowed"]:
                raise PortalError(str(guard["reason"] or "真实外部写入未确认。"))
            elif action_type in {"update", "end"}:
                ok, result = update_bitable_record_fields(
                    target_record_id,
                    notice_type,
                    remote_fields,
                )
                if not ok:
                    raise PortalError(str(result or "回退多维失败。"))
                restored_record_id = str(result or target_record_id)
                remote_message = "多维已恢复"
            elif action_type == "delete":
                ok, query_result = query_record_by_id(target_record_id, notice_type)
                if ok:
                    ok_update, result = update_bitable_record_fields(
                        target_record_id,
                        notice_type,
                        remote_fields,
                    )
                    if not ok_update:
                        raise PortalError(str(result or "回退多维失败。"))
                    restored_record_id = target_record_id
                    remote_message = "多维已恢复"
                else:
                    ok_create, result = create_bitable_record_fields(
                        notice_type,
                        remote_fields,
                    )
                    if not ok_create:
                        raise PortalError(str(result or "重建多维记录失败。"))
                    restored_record_id = str(result or "").strip()
                    remote_message = "多维已重建"
        else:
            remote_message = "远端记录不可恢复，仅恢复本地状态。"

        if job_id:
            cls.service.mark_job(job_id, phase="undoing_local", upload_message=remote_message)
        local_result = cls.service.restore_notice_undo_local(
            undo,
            target_record_id=restored_record_id,
            applied_by=requested_by,
            job_id=job_id,
        )
        cls.state_store.mark_notice_undo_action(
            undo_id,
            "undone",
            payload_patch={
                "restored_target_record_id": restored_record_id,
                "remote_message": remote_message,
                "applied_by": requested_by,
                "applied_job_id": job_id,
            },
        )
        event_payload = local_result.get("active_payload") if isinstance(local_result, dict) else {}
        if isinstance(event_payload, dict) and event_payload:
            cls.state_store.enqueue_outbox_event(
                "qt_action",
                {
                    "kind": "active_upsert",
                    "payload": event_payload,
                },
            )
        else:
            cls.state_store.enqueue_outbox_event(
                "qt_action",
                {
                    "kind": "active_delete",
                    "payload": {
                        "active_item_id": str(undo.get("active_item_id") or ""),
                        "record_id": restored_record_id,
                        "source_record_id": str(undo.get("source_record_id") or ""),
                        "work_type": str(undo.get("work_type") or ""),
                        "notice_type": notice_type,
                    },
                },
            )
        return {
            "ok": True,
            "undo_id": undo_id,
            "record_id": restored_record_id,
            "message": remote_message,
            **(local_result if isinstance(local_result, dict) else {}),
        }

    @classmethod
    def _execute_backend_prepared_upload(
        cls, prepared: dict
    ) -> tuple[bool, str, str]:
        prepared = prepared if isinstance(prepared, dict) else {}
        notice_type = str(prepared.get("notice_type") or "").strip()
        action = str(prepared.get("action") or "").strip().lower()
        if not notice_type or action not in {"start", "update", "end"}:
            return False, "后端上传缺少必要字段。", ""
        if (
            action == "end"
            and cls._end_site_photo_required(notice_type)
            and not cls._has_extra_images_payload(prepared)
        ):
            return False, "结束通告前必须添加至少一张现场照片。", ""
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
        images_ok, images_error, image_file_tokens, image_extra_file_tokens = (
            cls._upload_extra_images_for_notice(prepared, notice_type)
        )
        if not images_ok:
            return False, images_error or "现场照片上传失败。", ""

        if action == "start":
            existing_target = cls._existing_target_for_prepared_start(
                prepared,
                notice_type,
            )
            if existing_target:
                return True, existing_target, existing_target
            payload = cls._prepared_to_notice_payload(
                prepared,
                file_tokens=image_file_tokens,
                extra_file_tokens=image_extra_file_tokens,
            )
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
        if not ok_query and cls._remote_record_not_found(query_result):
            rebound_record_id, rebound_error = cls._rebind_missing_target_record(
                prepared,
                stale_record_id=record_id,
            )
            if rebound_record_id:
                record_id = rebound_record_id
                prepared = {
                    **prepared,
                    "target_record_id": record_id,
                    "record_id": record_id,
                    "_record_id_kind": "target",
                }
                ok_query, query_result = query_record_by_id(record_id, notice_type)
            elif rebound_error:
                return False, rebound_error, record_id
        if not ok_query:
            return False, f"查询失败: {query_result}", record_id
        fields = query_result.get("fields", {}) if isinstance(query_result, dict) else {}
        checkpoint_id = cls._create_backend_undo_checkpoint(
            "end" if action == "end" else "update",
            {
                **prepared,
                "target_record_id": record_id,
                "_record_id_kind": "source",
            },
            remote_fields=fields,
            remote_missing=False,
            job_id=str(prepared.get("job_id") or ""),
        )
        existing_tokens, existing_extra_tokens, existing_response_time = (
            cls._existing_tokens_for_notice_type(notice_type, fields)
        )
        payload = cls._prepared_to_notice_payload(
            prepared,
            file_tokens=image_file_tokens,
            extra_file_tokens=image_extra_file_tokens,
            existing_file_tokens=existing_tokens,
            existing_extra_file_tokens=existing_extra_tokens,
            existing_response_time=existing_response_time if action == "update" else "",
        )
        ok, result = update_bitable_record_by_payload(record_id, notice_type, payload)
        if not ok and checkpoint_id:
            cls.state_store.mark_notice_undo_action(
                checkpoint_id,
                "failed",
                error=str(result or "多维更新失败。"),
            )
        return bool(ok), str(result or ""), record_id

    @classmethod
    def confirm_change_target_candidate(
        cls,
        *,
        scope: str,
        title: str,
        start_time: str = "",
        end_time: str = "",
        action: str = "update",
        record_id: str = "",
    ) -> dict:
        action = str(action or "update").strip().lower()
        if action != "update":
            raise PortalError("只有更新通告需要确认目标变更记录。")
        record_id = str(record_id or "").strip()
        if not record_id:
            raise PortalError("请选择要关联的设备变更记录。")
        result = cls.service.lookup_change_target_candidates(
            scope=scope,
            title=title,
            start_time=start_time,
            end_time=end_time,
            action=action,
            limit=50,
        )
        candidates = [
            item
            for item in result.get("candidates", [])
            if str(item.get("record_id") or item.get("target_record_id") or "").strip()
            == record_id
        ]
        if not candidates:
            raise PortalError("选择的设备变更记录不在当前入口可关联范围内。")
        candidate = candidates[0]
        fields = candidate.get("fields") if isinstance(candidate.get("fields"), dict) else {}
        field_config = get_field_config("设备变更")
        clear_field_names: list[str] = []
        for field_name in (
            "实际结束时间",
            field_config.get("actual_end", ""),
            field_config.get("end_time", ""),
        ):
            field_name = str(field_name or "").strip()
            if not field_name or field_name in clear_field_names:
                continue
            value = fields.get(field_name)
            if value in (None, "", [], {}):
                continue
            if str(value).strip():
                clear_field_names.append(field_name)

        clear_result = {"cleared": False, "fields": [], "message": ""}
        if clear_field_names:
            guard = external_real_write_guard()
            if guard.get("mock_external"):
                clear_result = {
                    "cleared": True,
                    "fields": clear_field_names,
                    "message": "mock external update skipped",
                }
            elif not guard.get("real_write_allowed"):
                raise PortalError(str(guard.get("reason") or "真实外部写入未确认。"))
            else:
                ok, update_result = update_bitable_record_fields(
                    record_id,
                    "设备变更",
                    {field_name: None for field_name in clear_field_names},
                )
                if not ok:
                    raise PortalError(f"清空设备变更实际结束时间失败：{update_result}")
                cls.service.clear_target_record_cache()
                clear_result = {
                    "cleared": True,
                    "fields": clear_field_names,
                    "message": str(update_result or ""),
                }

        return {
            "ok": True,
            "candidate": candidate,
            "record_id": record_id,
            "target_record_id": record_id,
                "clear_actual_end": clear_result,
            "source_candidates": result.get("source_candidates") or [],
        }

    @classmethod
    def _prepared_to_qt_ui_payload(
        cls, prepared: dict, *, remote_record_id: str = ""
    ) -> dict:
        prepared = normalize_notice_identity_payload(dict(prepared or {}))
        source_record_id = str(
            prepared.get("source_record_id") or prepared.get("record_id") or ""
        ).strip()
        target_record_id = (
            str(remote_record_id or "").strip()
            or str(prepared.get("target_record_id") or "").strip()
        )
        prepared["ui_only"] = True
        prepared["source_record_id"] = source_record_id
        prepared["record_id"] = target_record_id
        prepared["target_record_id"] = target_record_id
        if target_record_id and not str(prepared.get("active_item_id") or "").strip():
            prepared["active_item_id"] = target_record_id
        prepared.setdefault("origin", "portal")
        prepared["lan_created_from_portal"] = True
        prepared["_is_placeholder_record"] = False
        prepared["_record_id_kind"] = "target"
        prepared["_upload_in_progress"] = False
        prepared["_has_unuploaded_changes"] = False
        return prepared

    @classmethod
    def _upsert_backend_active_notice(
        cls, prepared: dict, *, remote_record_id: str = "", job_id: str = ""
    ) -> str:
        event_payload = cls._prepared_to_qt_ui_payload(
            prepared,
            remote_record_id=remote_record_id,
        )
        notice_type = str(event_payload.get("notice_type") or "").strip()
        section = "event" if notice_type == "事件通告" else "other"
        try:
            cls.state_store.upsert_qt_active_item(
                event_payload,
                section=section,
                sort_order=0,
                origin=str(event_payload.get("origin") or "portal"),
            )
        except Exception as exc:
            log_warning(
                f"后端 active item 投影失败: job_id={job_id}, error={exc}"
            )
            raise
        try:
            cls.clear_payload_cache()
            if hasattr(cls.service, "_touch_state_cache_version"):
                cls.service._touch_state_cache_version()
        except Exception:
            pass
        active_item_id = str(event_payload.get("active_item_id") or "").strip()
        target_record_id = str(event_payload.get("target_record_id") or "").strip()
        projected_item = {
            "active_item_id": active_item_id,
            "record_id": target_record_id,
            "notice_type": notice_type,
            "section": section,
            "sort_order": 0,
            "origin": str(event_payload.get("origin") or "portal"),
            "payload": event_payload,
        }
        return cls.state_store.enqueue_outbox_event(
            "qt_action",
            {
                "kind": "active_upsert",
                "job_id": str(job_id or event_payload.get("job_id") or ""),
                "payload": {
                    "item": projected_item,
                    "source": "portal",
                },
            },
        )

    @classmethod
    def _enqueue_active_delete_for_ended_notice(
        cls, prepared: dict, *, remote_record_id: str = "", job_id: str = ""
    ) -> str:
        prepared = normalize_notice_identity_payload(dict(prepared or {}))
        target_record_id = (
            str(remote_record_id or "").strip()
            or str(prepared.get("target_record_id") or "").strip()
        )
        active_item_id = str(prepared.get("active_item_id") or "").strip()
        try:
            cls.state_store.delete_qt_active_item(
                active_item_id=active_item_id,
                record_id=target_record_id,
            )
        except Exception:
            pass
        return cls.state_store.enqueue_outbox_event(
            "qt_action",
            {
                "kind": "active_delete",
                "job_id": str(job_id or prepared.get("job_id") or ""),
                "payload": {
                    "active_item_id": active_item_id,
                    "record_id": target_record_id,
                    "target_record_id": target_record_id,
                    "source_record_id": str(
                        prepared.get("source_record_id")
                        or (
                            prepared.get("record_id")
                            if str(prepared.get("record_id") or "").strip()
                            != target_record_id
                            else ""
                        )
                        or ""
                    ),
                    "work_type": str(
                        prepared.get("work_type") or prepared.get("lan_work_type") or ""
                    ),
                    "notice_type": str(prepared.get("notice_type") or ""),
                },
            },
        )

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
        data = normalize_notice_identity_payload(data, action=action_type)
        notice_type = str(data.get("notice_type") or "").strip()
        if not notice_type:
            raise PortalError("Qt 上传请求缺少 notice_type。")
        record_id = str(data.get("record_id") or "").strip()
        if not record_id:
            raise PortalError("Qt 上传请求缺少 record_id。")
        source_record_id = str(data.get("source_record_id") or "").strip()
        target_record_id = str(data.get("target_record_id") or "").strip()
        if action_type == "upload" and target_record_id:
            action_type = "update"
            data["action_type"] = action_type
            data["record_id"] = target_record_id
            data["target_record_id"] = target_record_id
            data["_record_id_kind"] = "target"
            data["_is_placeholder_record"] = False
            record_id = target_record_id
        elif (
            action_type == "upload"
            and not bool(data.get("_is_placeholder_record", True))
            and record_id
            and not is_local_record_id(record_id)
            and record_id != source_record_id
            and str(data.get("_record_id_kind") or "").strip().lower() != "source"
            and not bool(data.get("source_only"))
        ):
            action_type = "update"
            data["action_type"] = action_type
            data["target_record_id"] = record_id
            data["_record_id_kind"] = "target"
            target_record_id = record_id
        if action_type in {"update", "end"} and not target_record_id:
            identity = cls.state_store.resolve_notice_identity(
                work_type=str(data.get("work_type") or "").strip(),
                active_item_id=str(data.get("active_item_id") or "").strip(),
                source_record_id=source_record_id,
                target_record_id="",
            )
            if isinstance(identity, dict):
                target_record_id = str(identity.get("target_record_id") or "").strip()
        if action_type in {"update", "end"} and not target_record_id:
            raise PortalError("Qt 更新/结束缺少目标多维 target_record_id。")

        prequery_result: dict | None = None
        if not bool(data.get("_is_placeholder_record")) and action_type in {
            "update",
            "end",
            "upload_replace",
        }:
            query_record_id = target_record_id or record_id
            ok_query, query_result = query_record_by_id(query_record_id, notice_type)
            if not ok_query:
                action_name = "结束" if action_type == "end" else "更新" if action_type == "update" else "归档"
                if cls._remote_record_not_found(query_result):
                    return cls._target_selection_response(
                        data,
                        stale_record_id=query_record_id,
                        message=f"查询失败: {query_result}",
                        action_name=action_name,
                    )
                return {
                    "ok": False,
                    "name": action_name,
                    "message": f"查询失败: {query_result}",
                    "record_id": query_record_id,
                    "real_record_id": "",
                }
            prequery_result = query_result if isinstance(query_result, dict) else {}

        extra_images = payload.get("extra_images") if isinstance(payload.get("extra_images"), list) else []
        if (
            action_type == "end"
            and cls._end_site_photo_required(notice_type)
            and not cls._has_extra_images_payload({"extra_images": extra_images})
        ):
            return {
                "ok": False,
                "name": "结束",
                "message": "结束通告前必须添加至少一张现场照片。",
                "record_id": target_record_id or record_id,
                "real_record_id": "",
            }

        extra_file_tokens: list[str] = []
        file_tokens: list[str] = []

        screenshot_upload_id = str(payload.get("screenshot_upload_id") or "").strip()
        screenshot_bytes = b""
        screenshot_file_name = str(
            payload.get("screenshot_file_name") or "notice_screenshot.png"
        ).strip()
        if screenshot_upload_id:
            attachment = cls.state_store.get_notice_upload_attachment(screenshot_upload_id)
            if not attachment:
                return {
                    "ok": False,
                    "name": "截图上传",
                    "message": "截图已过期或不存在，请重新选择。",
                    "record_id": record_id,
                    "real_record_id": "",
                }
            screenshot_bytes = bytes(attachment.get("content") or b"")
            screenshot_file_name = str(
                attachment.get("file_name") or screenshot_file_name
            ).strip()
        if screenshot_bytes:
            success, result = upload_media_to_feishu(
                screenshot_bytes,
                file_name=screenshot_file_name,
            )
            if not success:
                return {
                    "ok": False,
                    "name": "截图上传",
                    "message": str(result or ""),
                    "record_id": record_id,
                    "real_record_id": "",
                }
            if screenshot_upload_id:
                cls.state_store.mark_notice_upload_attachment_used(screenshot_upload_id)
            file_tokens.append(str(result or "").strip())
        images_ok, images_error, image_file_tokens, image_extra_file_tokens = (
            cls._upload_extra_images_for_notice({"extra_images": extra_images}, notice_type)
        )
        if not images_ok:
            return {
                "ok": False,
                "name": "截图上传",
                "message": images_error or "现场照片上传失败。",
                "record_id": record_id,
                "real_record_id": "",
            }
        file_tokens.extend(image_file_tokens)
        extra_file_tokens.extend(image_extra_file_tokens)

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
            dedupe_key = cls._local_upload_dedupe_key(data, notice_type)
            lock = cls._local_upload_lock_for_key(dedupe_key) if dedupe_key else None

            def _run_create_once() -> dict:
                if dedupe_key:
                    cached_target = str(
                        cls.local_upload_created_targets.get(dedupe_key) or ""
                    ).strip()
                    if cached_target:
                        return {
                            "ok": True,
                            "name": "上传",
                            "message": cached_target,
                            "record_id": record_id,
                            "real_record_id": cached_target,
                            "deduped": True,
                        }
                existing_target = cls._existing_target_for_local_upload(data, notice_type)
                if existing_target:
                    if dedupe_key:
                        cls.local_upload_created_targets[dedupe_key] = existing_target
                    return {
                        "ok": True,
                        "name": "上传",
                        "message": existing_target,
                        "record_id": record_id,
                        "real_record_id": existing_target,
                        "deduped": True,
                    }
                success, result = create_bitable_record_by_payload(
                    notice_type,
                    notice_payload,
                )
                real_record_id = str(result or "").strip() if success else ""
                if success:
                    if dedupe_key:
                        cls.local_upload_created_targets[dedupe_key] = real_record_id
                    cls._remember_local_upload_target(
                        data,
                        notice_type=notice_type,
                        target_record_id=real_record_id,
                    )
                return {
                    "ok": bool(success),
                    "name": "上传",
                    "message": str(result or ""),
                    "record_id": record_id,
                    "real_record_id": real_record_id,
                }

            if lock is None:
                return _run_create_once()
            with lock:
                try:
                    return _run_create_once()
                finally:
                    cls._release_local_upload_lock_for_key(dedupe_key, lock)

        existing_tokens: list[str] = []
        existing_extra_tokens: list[str] = []
        existing_response_time = ""
        checkpoint_id = ""
        if not bool(data.get("_is_placeholder_record")):
            query_record_id = target_record_id or record_id
            if prequery_result is None:
                ok_query, query_result = query_record_by_id(query_record_id, notice_type)
                if not ok_query:
                    action_name = "结束" if action_type == "end" else "更新" if action_type == "update" else "归档"
                    if cls._remote_record_not_found(query_result):
                        return cls._target_selection_response(
                            data,
                            stale_record_id=query_record_id,
                            message=f"查询失败: {query_result}",
                            action_name=action_name,
                        )
                    return {
                        "ok": False,
                        "name": action_name,
                        "message": f"查询失败: {query_result}",
                        "record_id": query_record_id,
                        "real_record_id": "",
                    }
            else:
                query_result = prequery_result
            fields = query_result.get("fields", {}) if isinstance(query_result, dict) else {}
            if action_type in {"update", "end"}:
                checkpoint_id = cls._create_backend_undo_checkpoint(
                    "end" if action_type == "end" else "update",
                    {
                        **data,
                        "target_record_id": query_record_id,
                        "_record_id_kind": "target",
                    },
                    remote_fields=fields,
                    remote_missing=False,
                    job_id=str(payload.get("job_id") or ""),
                )
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
        if not success and checkpoint_id:
            cls.state_store.mark_notice_undo_action(
                checkpoint_id,
                "failed",
                error=str(result or "多维更新失败。"),
            )
        if success and action_type == "end":
            cls._enqueue_active_delete_for_ended_notice(
                data,
            remote_record_id=target_record_id,
                job_id=str(payload.get("job_id") or ""),
            )
        return {
            "ok": bool(success),
            "name": action_name,
            "message": str(result or ""),
            "record_id": target_record_id or record_id,
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
        payload = normalize_notice_identity_payload(payload)
        target_record_id = canonical_target_record_id(payload)
        record_id = target_record_id
        source_record_id = str(payload.get("source_record_id") or "").strip()
        active_item_id = str(payload.get("active_item_id") or "").strip()
        work_type = str(payload.get("work_type") or "").strip()
        if not target_record_id:
            try:
                identity = cls.state_store.resolve_notice_identity(
                    work_type=work_type,
                    active_item_id=active_item_id,
                    source_record_id=source_record_id,
                    target_record_id=record_id,
                )
            except Exception:
                identity = None
            if isinstance(identity, dict):
                resolved_target = str(identity.get("target_record_id") or "").strip()
                if resolved_target:
                    target_record_id = resolved_target
                    record_id = resolved_target
        notice_type = str(payload.get("notice_type") or "").strip()
        is_placeholder = bool(payload.get("_is_placeholder_record"))
        remote_deleted = False
        checkpoint_fields: dict = {}
        checkpoint_remote_missing = True
        supports_undo = callable(
            getattr(cls.service, "create_notice_undo_checkpoint", None)
        )
        if supports_undo and record_id and notice_type and not is_placeholder:
            guard = external_real_write_guard()
            if guard.get("mock_external"):
                checkpoint_remote_missing = False
            else:
                ok_query, query_result = query_record_by_id(record_id, notice_type)
                if ok_query and isinstance(query_result, dict):
                    checkpoint_fields = query_result.get("fields", {}) if isinstance(query_result.get("fields"), dict) else {}
                    checkpoint_remote_missing = False
        checkpoint_id = ""
        if supports_undo:
            checkpoint_id = cls._create_backend_undo_checkpoint(
                "delete",
                {
                    **payload,
                    "target_record_id": record_id,
                    "_record_id_kind": "target",
                },
                remote_fields=checkpoint_fields,
                remote_missing=checkpoint_remote_missing,
                job_id=str(payload.get("job_id") or ""),
            )
        if record_id and notice_type and not is_placeholder:
            ok, result = delete_bitable_record(record_id, notice_type)
            if not ok:
                if checkpoint_id:
                    cls.state_store.mark_notice_undo_action(
                        checkpoint_id,
                        "failed",
                        error=str(result or "多维记录删除失败。"),
                    )
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
        try:
            cls.state_store.mark_notice_identity_deleted(
                work_type=work_type,
                active_item_id=active_item_id,
                source_record_id=source_record_id,
                target_record_id=record_id,
            )
        except Exception:
            pass
        return {
            "ok": True,
            "message": "",
            "record_id": record_id,
            "active_item_id": active_item_id,
            "remote_deleted": remote_deleted,
            "undo_id": checkpoint_id,
            "undo_available": bool(checkpoint_id),
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
        projection_warning = ""
        event_ids: list[str] = []
        if ok:
            state_text = str(field_value or "").strip()
            if state_text == "是":
                normalized_state = "yes"
            elif state_text == "否":
                normalized_state = "no"
            else:
                normalized_state = "unknown"
            try:
                active_item_id = str(payload.get("active_item_id") or "").strip()
                for item in cls.state_store.list_qt_active_items():
                    item_record_id = str(item.get("record_id") or "").strip()
                    item_active_id = str(item.get("active_item_id") or "").strip()
                    if (
                        (record_id and item_record_id == record_id)
                        or (active_item_id and item_active_id == active_item_id)
                    ):
                        event_payload = dict(item.get("payload") or {})
                        event_payload.setdefault("active_item_id", item_active_id)
                        event_payload["record_id"] = record_id
                        event_payload["target_record_id"] = record_id
                        event_payload["today_in_progress_state"] = normalized_state
                        event_payload.pop("_today_in_progress_syncing", None)
                        cls.state_store.upsert_qt_active_item(
                            event_payload,
                            section=str(item.get("section") or "other"),
                            sort_order=int(item.get("sort_order") or 0),
                            origin=str(item.get("origin") or event_payload.get("origin") or "qt"),
                        )
                        event_ids.append(
                            cls.state_store.enqueue_outbox_event(
                                "qt_action",
                                {
                                    "kind": "active_upsert",
                                    "payload": {
                                        "item": {
                                            "active_item_id": str(
                                                event_payload.get("active_item_id")
                                                or item_active_id
                                            ),
                                            "record_id": record_id,
                                            "notice_type": notice_type,
                                            "section": str(item.get("section") or "other"),
                                            "sort_order": int(item.get("sort_order") or 0),
                                            "origin": str(
                                                item.get("origin")
                                                or event_payload.get("origin")
                                                or "qt"
                                            ),
                                            "payload": event_payload,
                                        },
                                        "source": "qt",
                                    },
                                },
                            )
                        )
                cls.clear_payload_cache()
                if hasattr(cls.service, "_touch_state_cache_version"):
                    cls.service._touch_state_cache_version()
            except Exception as exc:
                projection_warning = str(exc)
        return {
            "ok": bool(ok),
            "message": str(result or ""),
            "record_id": record_id,
            "today_in_progress_state": (
                "yes" if str(field_value or "").strip() == "是"
                else "no" if str(field_value or "").strip() == "否"
                else "unknown"
            ),
            "event_ids": event_ids,
            "projection_warning": projection_warning,
        }

    @classmethod
    def _process_maintenance_action_job(cls, job_id: str) -> None:
        try:
            prepared = cls.service.prepare_action_job(job_id)
            current_job = cls.service.get_job(job_id) or {}
            message_failed_continue = bool(current_job.get("message_failed_continue"))
            if (
                not prepared.get("skip_personal_message")
                and not prepared.get("message_sent")
                and not message_failed_continue
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
                    message_sent=bool(prepared.get("message_sent")),
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
            if str(prepared.get("action") or "").strip().lower() == "end":
                try:
                    event_id = cls._enqueue_active_delete_for_ended_notice(
                        prepared,
                        remote_record_id=remote_record_id,
                        job_id=job_id,
                    )
                    cls.service.mark_job(
                        job_id,
                        qt_phase="outbox",
                        qt_queue_position=0,
                        qt_queue_size=0,
                        qt_event_id=event_id,
                    )
                except Exception as exc:
                    log_warning(f"Qt 结束删除事件投递失败: job_id={job_id}, error={exc}")
                try:
                    cls.state_store.mark_runtime_queue_item("qt_action", job_id, "done")
                except Exception:
                    pass
                return
            try:
                event_id = cls._upsert_backend_active_notice(
                    prepared,
                    remote_record_id=remote_record_id,
                    job_id=job_id,
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
                cls.service.mark_job(
                    job_id,
                    qt_phase="sync_failed",
                    error=f"通告已上传，但界面同步失败：{exc}",
                )
                try:
                    cls.state_store.mark_runtime_queue_item(
                        "qt_action",
                        job_id,
                        "failed",
                        error=str(exc),
                    )
                except Exception:
                    pass
                return
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
        self.server = None
        self.thread: threading.Thread | None = None
        self.bound_port: int | None = None
        self.notice_callback = None
        self.ongoing_callback = None
        self.ongoing_delete_callback = None
        self.maintenance_action_callback = None

    def start(self) -> str:
        raise RuntimeError(
            "旧 ThreadingHTTPServer 门户入口已移除，请通过 "
            "clipflow_backend.process_controller 启动 FastAPI 后端。"
        )

    def get_url(self) -> str:
        port = self.bound_port or self.preferred_port
        display_host = self.host if self.host != "0.0.0.0" else "127.0.0.1"
        return f"http://{display_host}:{port}"

    def set_notice_callback(self, callback) -> None:
        self.notice_callback = callback
        PortalRuntime.notice_callback = callback

    def set_ongoing_callback(self, callback) -> None:
        self.ongoing_callback = callback
        PortalRuntime.ongoing_callback = callback

    def set_ongoing_delete_callback(self, callback) -> None:
        self.ongoing_delete_callback = callback
        PortalRuntime.ongoing_delete_callback = callback

    def set_maintenance_action_callback(self, callback) -> None:
        self.maintenance_action_callback = callback
        PortalRuntime.maintenance_action_callback = callback

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
        PortalRuntime.service.mark_action_upload_result(
            job_id,
            success=success,
            message=message,
            record_id=record_id,
            active_item_id=active_item_id,
        )

    def mark_job_progress(self, job_id: str, **patch) -> None:
        if not job_id:
            return
        PortalRuntime.service.mark_job(job_id, **patch)
        phase = str((patch or {}).get("phase") or "").strip()
        if phase == "uploading":
            PortalRuntime.track_upload_wait_job(job_id)
        elif phase in {"success", "failed"}:
            try:
                PortalRuntime.state_store.mark_runtime_queue_item(
                    "upload_wait",
                    job_id,
                    "done" if phase == "success" else "failed",
                    error=str((patch or {}).get("error") or ""),
                )
            except Exception:
                pass

    def get_job(self, job_id: str) -> dict | None:
        return PortalRuntime.service.get_job(job_id)

    def stop(self) -> None:
        self.server = None
        self.thread = None
        self.bound_port = None
        PortalRuntime.stop_message_workers()
        PortalRuntime.stop_action_worker()
        PortalRuntime.stop_upload_wait_worker()
        if self.start_source_refresh_worker:
            PortalRuntime.stop_source_refresh_worker()
        PortalRuntime.notice_callback = None
        PortalRuntime.ongoing_callback = None
        PortalRuntime.ongoing_delete_callback = None
        PortalRuntime.maintenance_action_callback = None
        try:
            PortalRuntime.state_store.shutdown_write_worker(timeout=2.0)
        except Exception as exc:
            log_warning(f"SQLite写入队列停止失败: {exc}")


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
    build_arg_parser().parse_args()
    raise SystemExit(
        "旧局域网门户命令行入口已移除。请启动 clipflow_backend.main "
        "或通过 Qt 主程序自动拉起 FastAPI 后端。"
    )


if __name__ == "__main__":
    main()
