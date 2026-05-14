# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
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
from urllib.parse import parse_qs, urlparse

from .portal_auth import AUTH_COOKIE_NAME, PortalAuthManager
from .portal_service import (
    DEFAULT_APP_TOKEN,
    DEFAULT_TABLE_ID,
    MaintenancePortalService,
    PortalError,
)


STATIC_DIR = Path(__file__).resolve().parent / "static"
DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 18766
CLIENT_DISCONNECT_WINERRORS = {10053, 10054, 10058}


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
        length = int(self.headers.get("Content-Length", "0") or "0")
        raw = self.rfile.read(length) if length > 0 else b"{}"
        return json.loads(raw.decode("utf-8"))

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

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
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
                data = self.service.get_bootstrap(scope=scope, ongoing_items=ongoing)
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
                data = self.service.get_scope_overview(ongoing_items=ongoing)
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
        if parsed.path == "/api/records":
            qs = parse_qs(parsed.query)
            try:
                scope = self._authorized_scope_or_error(
                    session, (qs.get("scope") or ["ALL"])[0]
                )
                payload = self.service.query_records(
                    month=(qs.get("month") or [""])[0],
                    specialty=(qs.get("specialty") or [""])[0],
                    scope=scope,
                    ongoing_items=self._get_ongoing(scope),
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
                payload = self.service.query_records(
                    month=(qs.get("month") or [""])[0],
                    specialty=(qs.get("specialty") or [""])[0],
                    scope=scope,
                    ongoing_items=self._get_ongoing(scope),
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
            qs = parse_qs(parsed.query)
            try:
                scope = self._authorized_scope_or_error(
                    session, (qs.get("scope") or ["ALL"])[0]
                )
                self.service.refresh()
                data = self.service.get_bootstrap(
                    scope=scope, ongoing_items=self._get_ongoing(scope)
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
                    PortalHandler.enqueue_action_job(job_id)
                return self._send_json(202, {"ok": True, "data": {"job_id": job_id}})
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
                data = self.service.hide_ongoing_item(
                    payload,
                    scope=scope,
                    deleted_by=payload["_auth_open_id"],
                )
                callback = PortalHandler.ongoing_delete_callback
                if callback is not None:
                    try:
                        accepted = callback(payload)
                        if isinstance(accepted, dict):
                            data["qt_deleted"] = bool(accepted.get("ok"))
                            if accepted.get("error"):
                                data["qt_delete_warning"] = str(accepted.get("error") or "")
                        else:
                            data["qt_deleted"] = bool(accepted)
                    except Exception as exc:
                        data["qt_deleted"] = False
                        data["qt_delete_warning"] = str(exc)
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
        callback = PortalHandler.ongoing_callback
        if callback is None:
            PortalHandler.last_ongoing_error = ""
            return []
        try:
            result = callback(scope)
            PortalHandler.last_ongoing_error = ""
        except Exception as exc:
            warning = f"主界面进行中状态读取失败: {exc}"
            PortalHandler.last_ongoing_error = warning
            logging.warning(warning)
            return []
        return result if isinstance(result, list) else []

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
                phase="queued",
                queue_position=index,
                queue_size=total,
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
            cls.service.mark_job(job_id, queue_position=0)
            cls._process_maintenance_action_job(job_id)
            cls._wait_action_upload_finished(job_id)

    @classmethod
    def _wait_action_upload_finished(cls, job_id: str) -> None:
        deadline = time.monotonic() + cls.action_upload_timeout_s
        while time.monotonic() < deadline:
            job = cls.service.get_job(job_id) or {}
            phase = str(job.get("phase") or "")
            if phase in {"success", "failed"}:
                return
            if phase != "uploading":
                return
            if cls.action_worker_stop:
                return
            time.sleep(0.5)
        cls.service.mark_job(
            job_id,
            phase="failed",
            error="上传多维超时，请在主界面确认实际结果后重试。",
        )

    @classmethod
    def _process_maintenance_action_job(cls, job_id: str) -> None:
        try:
            prepared = cls.service.prepare_action_job(job_id)
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
                    phase="sending_message",
                    queue_position=0,
                )
            if not prepared.get("skip_personal_message") and not prepared.get("message_sent"):
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
                    phase="uploading",
                    message_sent=True,
                    message_signature=str(prepared.get("message_signature") or ""),
                )
            elif not prepared.get("skip_personal_message"):
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
        PortalHandler.auth_manager = PortalAuthManager()
        PortalHandler.notice_callback = self.notice_callback
        PortalHandler.ongoing_callback = self.ongoing_callback
        PortalHandler.ongoing_delete_callback = self.ongoing_delete_callback
        PortalHandler.maintenance_action_callback = self.maintenance_action_callback
        with PortalHandler.action_queue_lock:
            PortalHandler.action_queue.clear()
            PortalHandler.action_worker_stop = False
            PortalHandler.action_queue_event.clear()
        self.server = ThreadingHTTPServer((self.host, bound_port), PortalHandler)
        self.thread = threading.Thread(
            target=self.server.serve_forever,
            name="LANBitableTemplatePortal",
            daemon=True,
        )
        self.thread.start()
        PortalHandler.ensure_action_worker()
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
        PortalHandler.stop_action_worker()
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
