# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import socket
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from .portal_service import (
    DEFAULT_APP_TOKEN,
    DEFAULT_TABLE_ID,
    MaintenancePortalService,
    PortalError,
)


STATIC_DIR = Path(__file__).resolve().parent / "static"
DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 18766


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
    notice_callback = None
    ongoing_callback = None
    maintenance_action_callback = None
    action_queue: list[str] = []
    action_queue_lock = threading.RLock()
    action_queue_event = threading.Event()
    action_worker_thread: threading.Thread | None = None
    action_worker_stop = False
    action_upload_timeout_s = 30 * 60

    def _send_json(self, status: int, payload: dict) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, path: Path) -> None:
        body = path.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json_body(self) -> dict:
        length = int(self.headers.get("Content-Length", "0") or "0")
        raw = self.rfile.read(length) if length > 0 else b"{}"
        return json.loads(raw.decode("utf-8"))

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/":
            return self._send_html(STATIC_DIR / "index.html")
        if parsed.path == "/api/bootstrap":
            qs = parse_qs(parsed.query)
            scope = (qs.get("scope") or ["ALL"])[0]
            try:
                ongoing = self._get_ongoing(scope)
                return self._send_json(
                    200,
                    {
                        "ok": True,
                        "data": self.service.get_bootstrap(
                            scope=scope, ongoing_items=ongoing
                        ),
                    },
                )
            except PortalError as exc:
                return self._send_json(500, {"ok": False, "error": str(exc)})
        if parsed.path == "/api/records":
            qs = parse_qs(parsed.query)
            try:
                scope = (qs.get("scope") or ["ALL"])[0]
                payload = self.service.query_records(
                    month=(qs.get("month") or [""])[0],
                    specialty=(qs.get("specialty") or [""])[0],
                    scope=scope,
                    ongoing_items=self._get_ongoing(scope),
                )
                return self._send_json(200, {"ok": True, "data": payload})
            except PortalError as exc:
                return self._send_json(500, {"ok": False, "error": str(exc)})
        if parsed.path == "/api/workbench":
            qs = parse_qs(parsed.query)
            try:
                scope = (qs.get("scope") or ["ALL"])[0]
                payload = self.service.query_records(
                    month=(qs.get("month") or [""])[0],
                    specialty=(qs.get("specialty") or [""])[0],
                    scope=scope,
                    ongoing_items=self._get_ongoing(scope),
                )
                return self._send_json(200, {"ok": True, "data": payload})
            except PortalError as exc:
                return self._send_json(500, {"ok": False, "error": str(exc)})
        if parsed.path == "/api/refresh":
            qs = parse_qs(parsed.query)
            scope = (qs.get("scope") or ["ALL"])[0]
            try:
                self.service.refresh()
                return self._send_json(
                    200,
                    {
                        "ok": True,
                        "data": self.service.get_bootstrap(
                            scope=scope, ongoing_items=self._get_ongoing(scope)
                        ),
                    },
                )
            except PortalError as exc:
                return self._send_json(500, {"ok": False, "error": str(exc)})
        if parsed.path.startswith("/api/jobs/"):
            job_id = parsed.path.rsplit("/", 1)[-1].strip()
            job = self.service.get_job(job_id)
            if not job:
                return self._send_json(404, {"ok": False, "error": "任务不存在"})
            return self._send_json(200, {"ok": True, "data": job})
        return self._send_json(404, {"ok": False, "error": "Not Found"})

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/api/maintenance-actions":
            if PortalHandler.maintenance_action_callback is None:
                return self._send_json(
                    503,
                    {"ok": False, "error": "主窗口未连接，无法上传多维。"},
                )
            try:
                payload = self._read_json_body()
                job_id, should_start = self.service.create_action_job(payload)
                if should_start:
                    PortalHandler.enqueue_action_job(job_id)
                return self._send_json(202, {"ok": True, "data": {"job_id": job_id}})
            except (PortalError, ValueError, json.JSONDecodeError) as exc:
                return self._send_json(400, {"ok": False, "error": str(exc)})
        if parsed.path == "/api/generate":
            try:
                payload = self._read_json_body()
                drafts = payload.get("drafts") or []
                generated = self.service.generate_templates(drafts)
                return self._send_json(200, {"ok": True, "data": {"items": generated}})
            except (PortalError, ValueError, json.JSONDecodeError) as exc:
                return self._send_json(400, {"ok": False, "error": str(exc)})
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
                items = payload.get("items") or []
                results = self.service.send_generated_templates(
                    items, notice_callback=PortalHandler.notice_callback
                )
                return self._send_json(200, {"ok": True, "data": {"items": results}})
            except (PortalError, ValueError, json.JSONDecodeError) as exc:
                return self._send_json(400, {"ok": False, "error": str(exc)})
        return self._send_json(404, {"ok": False, "error": "Not Found"})

    def _get_ongoing(self, scope: str) -> list[dict]:
        callback = PortalHandler.ongoing_callback
        if callback is None:
            return []
        try:
            result = callback(scope)
        except Exception:
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
            cls.service.mark_job(
                job_id,
                phase="sending_message",
                queue_position=0,
            )
            if not prepared.get("message_sent"):
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
        self.maintenance_action_callback = None

    def start(self) -> str:
        if self.server and self.thread and self.thread.is_alive():
            return self.get_url()
        bound_port = find_available_port(self.host, self.preferred_port)
        PortalHandler.service = MaintenancePortalService(
            app_token=self.app_token,
            table_id=self.table_id,
        )
        PortalHandler.notice_callback = self.notice_callback
        PortalHandler.ongoing_callback = self.ongoing_callback
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
