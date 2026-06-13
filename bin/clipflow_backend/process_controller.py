# -*- coding: utf-8 -*-
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from lan_bitable_template_portal.server import DEFAULT_HOST, DEFAULT_PORT
from upload_event_module.logger import log_error, log_info, log_warning


def _env_int(name: str, default: int, *, minimum: int, maximum: int) -> int:
    try:
        value = int(str(os.environ.get(name, "") or "").strip() or default)
    except Exception:
        value = int(default)
    return max(int(minimum), min(int(maximum), value))


def _env_flag(name: str, default: bool = False) -> bool:
    raw = str(os.environ.get(name, "") or "").strip().lower()
    if not raw:
        return bool(default)
    return raw in {"1", "true", "yes", "on"}


def _display_host(host: str) -> str:
    text = str(host or DEFAULT_HOST).strip() or DEFAULT_HOST
    return "127.0.0.1" if text in {"0.0.0.0", "::"} else text


def _probe_host(host: str) -> str:
    text = str(host or DEFAULT_HOST).strip() or DEFAULT_HOST
    return "127.0.0.1" if text in {"0.0.0.0", "::"} else text


class BackendProcessPortalController:
    """Qt-side controller for the standalone FastAPI backend process."""

    def __init__(
        self,
        *,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
        app_token: str = "",
        table_id: str = "",
    ) -> None:
        self.host = str(host or DEFAULT_HOST).strip() or DEFAULT_HOST
        self.preferred_port = int(port or DEFAULT_PORT)
        self.app_token = str(app_token or "").strip()
        self.table_id = str(table_id or "").strip()
        self.bound_port = self.preferred_port
        self._process: subprocess.Popen | None = None
        self._owns_process = False
        self._stop_event = threading.Event()
        self._event_thread: threading.Thread | None = None
        self._snapshot_thread: threading.Thread | None = None
        self.notice_callback = None
        self.ongoing_callback = None
        self.ongoing_delete_callback = None
        self.maintenance_action_callback = None
        self.shell_event_callback = None
        self._last_snapshot_hash = ""
        self._last_bridge_heartbeat = 0.0
        self._slow_event_count = 0
        self._last_event_duration_ms = 0.0
        self._last_event_kind = ""
        self._last_event_error = ""
        self._slow_event_history: list[dict[str, Any]] = []
        self._event_stream_connected = False
        self._event_stream_connect_count = 0
        self._event_stream_disconnect_count = 0
        self._event_stream_last_connected_at = 0.0
        self._event_stream_last_event_at = 0.0
        self._event_stream_last_error = ""
        self._event_poll_fallback_count = 0
        self._active_delta_lock = threading.Lock()
        self._active_delta_upserts: list[dict[str, Any]] = []
        self._active_delta_deletes: list[dict[str, Any]] = []
        self._active_delta_thread: threading.Thread | None = None
        self._event_fetch_limit = _env_int(
            "CLIPFLOW_QT_EVENT_FETCH_LIMIT",
            5,
            minimum=1,
            maximum=5,
        )
        self._legacy_portal_callbacks_enabled = _env_flag(
            "CLIPFLOW_ENABLE_QT_LEGACY_PORTAL_CALLBACKS",
            False,
        )
        self._snapshot_interval_s = _env_int(
            "CLIPFLOW_QT_SNAPSHOT_INTERVAL_SECONDS",
            300,
            minimum=30,
            maximum=3600,
        )
        self._startup_timeout_s = _env_int(
            "CLIPFLOW_BACKEND_STARTUP_TIMEOUT_SECONDS",
            75,
            minimum=15,
            maximum=180,
        )
        self._event_stream_timeout_s = _env_int(
            "CLIPFLOW_QT_EVENT_STREAM_TIMEOUT_SECONDS",
            45,
            minimum=15,
            maximum=180,
        )

    def get_url(self) -> str:
        return f"http://{_display_host(self.host)}:{self.bound_port}"

    def _local_url(self) -> str:
        return f"http://{_probe_host(self.host)}:{self.bound_port}"

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        payload: dict[str, Any] | None = None,
        timeout: float = 5.0,
    ) -> dict[str, Any]:
        url = f"{self._local_url()}{path}"
        data = None
        headers = {"Accept": "application/json"}
        if payload is not None:
            data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json; charset=utf-8"
        request = urllib.request.Request(url, data=data, headers=headers, method=method)
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read()
        if not body:
            return {}
        result = json.loads(body.decode("utf-8"))
        return result if isinstance(result, dict) else {}

    def _request_binary_json(
        self,
        method: str,
        path: str,
        *,
        content: bytes,
        content_type: str = "application/octet-stream",
        timeout: float = 30.0,
    ) -> dict[str, Any]:
        url = f"{self._local_url()}{path}"
        headers = {
            "Accept": "application/json",
            "Content-Type": str(content_type or "application/octet-stream"),
        }
        request = urllib.request.Request(
            url,
            data=bytes(content or b""),
            headers=headers,
            method=method,
        )
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read()
        if not body:
            return {}
        result = json.loads(body.decode("utf-8"))
        return result if isinstance(result, dict) else {}

    def _health_ok(self) -> bool:
        try:
            result = self._request_json("GET", "/api/health", timeout=1.2)
            return bool(result.get("ok"))
        except Exception:
            return False

    def _shutdown_existing_backend(self) -> bool:
        try:
            self._request_json("POST", "/api/backend/shutdown", timeout=2.0)
        except Exception as exc:
            log_warning(f"关闭旧后端失败，将尝试继续复用: {exc}")
            return False
        deadline = time.monotonic() + 8.0
        while time.monotonic() < deadline:
            if not self._health_ok():
                return True
            time.sleep(0.25)
        log_warning("旧后端关闭超时，将尝试继续复用。")
        return False

    def get_qt_shell_bootstrap(self) -> dict[str, Any]:
        result = self._request_json("GET", "/api/qt/shell/bootstrap", timeout=10.0)
        if not bool(result.get("ok")):
            raise RuntimeError(str(result.get("error") or "读取 Qt 壳 bootstrap 失败。"))
        data = result.get("data")
        return data if isinstance(data, dict) else {}

    def _wait_for_health(self, timeout_s: float = 30.0) -> bool:
        deadline = time.monotonic() + max(1.0, float(timeout_s or 0))
        while time.monotonic() < deadline:
            if self._health_ok():
                return True
            if self._process is not None and self._process.poll() is not None:
                return False
            time.sleep(0.25)
        return False

    def _build_backend_command(self) -> tuple[list[str], dict[str, str], str]:
        bin_dir = Path(__file__).resolve().parents[1]
        python_exe = self._python_executable(bin_dir)
        env = os.environ.copy()
        existing = env.get("PYTHONPATH", "")
        env["PYTHONPATH"] = os.pathsep.join(
            [os.fspath(bin_dir), existing] if existing else [os.fspath(bin_dir)]
        )
        # The desktop app is the trusted operator entrypoint. Directly running the
        # backend from a shell still requires an explicit confirmation env var.
        env.setdefault("CLIPFLOW_REQUIRE_REAL_EXTERNAL_CONFIRM", "1")
        env.setdefault("CLIPFLOW_REAL_EXTERNAL_CONFIRMED", "1")
        args = [
            os.fspath(python_exe),
            "-m",
            "clipflow_backend.main",
            "--host",
            self.host,
            "--port",
            str(self.preferred_port),
        ]
        return args, env, os.fspath(bin_dir)

    @staticmethod
    def _python_executable(bin_dir: Path) -> Path:
        override = os.environ.get("CLIPFLOW_BACKEND_PYTHON", "").strip()
        if override:
            candidate = Path(override)
            if candidate.exists():
                return candidate
        candidates = [
            bin_dir / ".venv" / "Scripts" / "python.exe",
            bin_dir / ".venv" / "bin" / "python",
            bin_dir.parent / ".venv" / "Scripts" / "python.exe",
            bin_dir.parent / ".venv" / "bin" / "python",
        ]
        for candidate in candidates:
            if candidate.exists():
                return candidate
        return Path(sys.executable)

    def start(self) -> str:
        if self._health_ok():
            if os.environ.get("CLIPFLOW_REUSE_EXISTING_BACKEND") != "1":
                if self._shutdown_existing_backend():
                    time.sleep(0.2)
                else:
                    self._owns_process = False
                    self._stop_event.clear()
                    self._ensure_bridge_threads()
                    return self.get_url()
            else:
                self._owns_process = False
                self._stop_event.clear()
                self._ensure_bridge_threads()
                return self.get_url()
        if self._health_ok():
            self._owns_process = False
            self._stop_event.clear()
            self._ensure_bridge_threads()
            return self.get_url()
        args, env, cwd = self._build_backend_command()
        startupinfo = None
        creationflags = 0
        if os.name == "nt":
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            startupinfo.wShowWindow = 0  # SW_HIDE
            creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        self._process = subprocess.Popen(
            args,
            cwd=cwd,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            startupinfo=startupinfo,
            creationflags=creationflags,
        )
        self._owns_process = True
        self._stop_event.clear()
        if not self._wait_for_health(timeout_s=float(self._startup_timeout_s)):
            code = self._process.poll() if self._process else None
            self.stop()
            raise RuntimeError(f"局域网后端进程启动失败，退出码={code}")
        self._ensure_bridge_threads()
        log_info(f"局域网后端子进程已启动: {self.get_url()}")
        return self.get_url()

    def stop(self) -> None:
        self._stop_event.set()
        self._flush_active_delta_once(timeout=3.0)
        if self._owns_process:
            try:
                self._request_json("POST", "/api/backend/shutdown", payload={}, timeout=1.5)
            except Exception:
                pass
        for attr in ("_event_thread", "_snapshot_thread", "_active_delta_thread"):
            thread = getattr(self, attr, None)
            if thread and thread.is_alive():
                try:
                    thread.join(timeout=2)
                except Exception:
                    pass
            if not thread or not thread.is_alive():
                setattr(self, attr, None)
        process = self._process
        if process is not None and process.poll() is None:
            try:
                process.wait(timeout=4)
            except subprocess.TimeoutExpired:
                process.terminate()
                try:
                    process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    process.kill()
        self._process = None
        self._owns_process = False

    def _ensure_bridge_threads(self) -> None:
        if self._event_thread is None or not self._event_thread.is_alive():
            self._event_thread = threading.Thread(
                target=self._event_loop,
                name="ClipFlowBackendQtEventBridge",
                daemon=True,
            )
            self._event_thread.start()
        if self._snapshot_thread is None or not self._snapshot_thread.is_alive():
            self._snapshot_thread = threading.Thread(
                target=self._snapshot_loop,
                name="ClipFlowBackendSnapshotBridge",
                daemon=True,
            )
            self._snapshot_thread.start()
        self._post_bridge_heartbeat(force=True)

    def _event_loop(self) -> None:
        delay = 0.25
        next_sse_attempt = 0.0
        while not self._stop_event.wait(delay):
            if (
                self.maintenance_action_callback is None
                and self.ongoing_delete_callback is None
                and self.notice_callback is None
                and self.shell_event_callback is None
            ):
                delay = 1.0
                continue
            now = time.monotonic()
            if now >= next_sse_attempt:
                if self._event_stream_once():
                    delay = 0.25
                    continue
                next_sse_attempt = time.monotonic() + 5.0
            try:
                self._event_poll_fallback_count += 1
                result = self._request_json(
                    "GET",
                    f"/api/qt/events?limit={self._event_fetch_limit}",
                    timeout=3.0,
                )
                items = ((result.get("data") or {}).get("items") or []) if result.get("ok") else []
            except Exception:
                delay = 5.0
                continue
            for item in items:
                self._dispatch_event(item)
            delay = 0.5 if items else 2.5

    def _event_stream_once(self) -> bool:
        url = f"{self._local_url()}/api/qt/events/stream"
        request = urllib.request.Request(
            url,
            headers={"Accept": "text/event-stream"},
            method="GET",
        )
        lines: list[str] = []
        try:
            with urllib.request.urlopen(
                request,
                timeout=float(self._event_stream_timeout_s),
            ) as response:
                self._event_stream_connected = True
                self._event_stream_connect_count += 1
                self._event_stream_last_connected_at = time.time()
                self._event_stream_last_error = ""
                while not self._stop_event.is_set():
                    raw_line = response.readline()
                    if not raw_line:
                        return False
                    line = raw_line.decode("utf-8", errors="replace").rstrip("\r\n")
                    if line:
                        lines.append(line)
                        continue
                    self._handle_sse_event(lines)
                    lines = []
            return True
        except Exception as exc:
            self._event_stream_last_error = str(exc)
            return False
        finally:
            if self._event_stream_connected:
                self._event_stream_disconnect_count += 1
            self._event_stream_connected = False

    def _handle_sse_event(self, lines: list[str]) -> None:
        if not lines:
            return
        event_name = "message"
        data_lines: list[str] = []
        for line in lines:
            if line.startswith(":"):
                continue
            if line.startswith("event:"):
                event_name = line.split(":", 1)[1].strip() or "message"
                continue
            if line.startswith("data:"):
                data_lines.append(line.split(":", 1)[1].lstrip())
        if event_name != "qt_event" or not data_lines:
            return
        try:
            item = json.loads("\n".join(data_lines))
        except Exception:
            return
        if isinstance(item, dict):
            self._event_stream_last_event_at = time.time()
            self._dispatch_event(item)

    def _dispatch_event(self, item: dict[str, Any]) -> None:
        event_id = int((item or {}).get("id") or 0)
        payload = (item or {}).get("payload") if isinstance(item, dict) else {}
        payload = payload if isinstance(payload, dict) else {}
        kind = str(payload.get("kind") or "").strip()
        ok = False
        error = ""
        started = time.perf_counter()
        try:
            if kind == "maintenance_action":
                if not self._legacy_portal_callbacks_enabled:
                    log_warning("已忽略旧门户 maintenance_action 事件；通告业务应由后端执行。")
                    ok = True
                else:
                    callback_payload = payload.get("payload") if isinstance(payload.get("payload"), dict) else {}
                    callback = self.maintenance_action_callback
                    if callback is None:
                        raise RuntimeError("Qt 通告动作回调未连接。")
                    accepted = callback(callback_payload)
                    ok = bool(accepted.get("ok")) if isinstance(accepted, dict) else bool(accepted)
                    error = str(accepted.get("error") or "") if isinstance(accepted, dict) else ""
            elif kind == "ongoing_delete":
                if not self._legacy_portal_callbacks_enabled:
                    log_warning("已忽略旧门户 ongoing_delete 事件；删除业务应由后端执行。")
                    ok = True
                else:
                    callback_payload = payload.get("payload") if isinstance(payload.get("payload"), dict) else {}
                    callback = self.ongoing_delete_callback
                    if callback is None:
                        raise RuntimeError("Qt 删除回调未连接。")
                    accepted = callback(callback_payload)
                    ok = bool(accepted.get("ok")) if isinstance(accepted, dict) else bool(accepted)
                    error = str(accepted.get("error") or "") if isinstance(accepted, dict) else ""
            elif kind == "notice":
                callback_payload = payload.get("payload") if isinstance(payload.get("payload"), dict) else {}
                callback = self.notice_callback
                if callback is None:
                    raise RuntimeError("Qt 通告回填回调未连接。")
                accepted = callback(callback_payload)
                ok = bool(accepted.get("ok")) if isinstance(accepted, dict) else bool(accepted)
                error = str(accepted.get("error") or "") if isinstance(accepted, dict) else ""
            elif kind in {
                "clipboard_candidate",
                "dialog_request",
                "active_upsert",
                "active_delete",
                "history_append",
                "status_banner",
                "clipboard_status",
            }:
                callback_payload = payload.get("payload") if isinstance(payload.get("payload"), dict) else {}
                callback = self.shell_event_callback
                if callback is None:
                    raise RuntimeError(f"Qt shell 事件回调未连接: {kind}")
                accepted = callback(kind, callback_payload)
                ok = bool(accepted.get("ok")) if isinstance(accepted, dict) else bool(accepted)
                error = str(accepted.get("error") or "") if isinstance(accepted, dict) else ""
            else:
                ok = True
        except Exception as exc:
            ok = False
            error = str(exc)
            log_warning(f"Qt 后端事件执行失败: event_id={event_id}, error={exc}")
        finally:
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            self._last_event_duration_ms = round(elapsed_ms, 1)
            self._last_event_kind = kind
            self._last_event_error = error
            if elapsed_ms >= 500.0:
                self._slow_event_count += 1
                self._slow_event_history.append(
                    {
                        "event_id": event_id,
                        "kind": kind,
                        "elapsed_ms": round(elapsed_ms, 1),
                        "error": error,
                        "at": time.time(),
                    }
                )
                self._slow_event_history = self._slow_event_history[-10:]
                log_error(
                    "Qt后端事件执行耗时过长: "
                    f"event_id={event_id}, kind={kind or '-'}, elapsed_ms={elapsed_ms:.1f}"
                )
        if event_id:
            self._ack_event(event_id, ok=ok, error=error)

    def _ack_event(self, event_id: int, *, ok: bool, error: str = "") -> None:
        event_id = int(event_id or 0)
        if not event_id:
            return
        payload = {"ok": bool(ok), "error": str(error or "")}
        last_error = ""
        for attempt in range(1, 4):
            try:
                self._request_json(
                    "POST",
                    f"/api/qt/events/{event_id}/ack",
                    payload=payload,
                    timeout=3.0,
                )
                return
            except Exception as exc:
                last_error = str(exc)
                if attempt < 3:
                    time.sleep(0.25 * attempt)
        log_warning(f"Qt 后端事件确认失败: event_id={event_id}, error={last_error}")

    def _post_bridge_heartbeat(self, *, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - float(self._last_bridge_heartbeat or 0) < 10.0:
            return
        self._last_bridge_heartbeat = now
        try:
            self._request_json(
                "POST",
                "/api/qt/bridge-heartbeat",
                payload={
                    "notice_callback": self.notice_callback is not None,
                    "ongoing_callback": self.ongoing_callback is not None,
                    "ongoing_delete_callback": self.ongoing_delete_callback is not None,
                    "maintenance_action_callback": self.maintenance_action_callback is not None,
                    "shell_event_callback": self.shell_event_callback is not None,
                    "event_thread_alive": bool(
                        self._event_thread and self._event_thread.is_alive()
                    ),
                    "snapshot_thread_alive": bool(
                        self._snapshot_thread and self._snapshot_thread.is_alive()
                    ),
                    "owns_process": bool(self._owns_process),
                    "pid": int(self._process.pid) if self._process and self._process.pid else 0,
                    "slow_event_count": int(self._slow_event_count),
                    "last_event_duration_ms": float(self._last_event_duration_ms or 0),
                    "last_event_kind": self._last_event_kind,
                    "last_event_error": self._last_event_error,
                    "slow_event_history": list(self._slow_event_history),
                    "event_stream": {
                        "connected": bool(self._event_stream_connected),
                        "connect_count": int(self._event_stream_connect_count),
                        "disconnect_count": int(self._event_stream_disconnect_count),
                        "last_connected_at": float(
                            self._event_stream_last_connected_at or 0
                        ),
                        "last_event_at": float(self._event_stream_last_event_at or 0),
                        "last_error": self._event_stream_last_error,
                        "poll_fallback_count": int(self._event_poll_fallback_count),
                    },
                },
                timeout=2.0,
            )
        except Exception:
            return

    @staticmethod
    def _snapshot_digest(items: list[dict[str, Any]]) -> str:
        digest = hashlib.sha1()
        digest.update(str(len(items or [])).encode("utf-8"))
        digest.update(b"\0")
        fields = (
            "active_item_id",
            "record_id",
            "notice_type",
            "status",
            "text",
            "building",
            "building_codes",
            "start_time",
            "end_time",
            "time",
            "source_record_id",
            "source_only",
            "maintenance_cycle",
            "lan_created_from_portal",
        )
        for item in items or []:
            if not isinstance(item, dict):
                continue
            for field in fields:
                value = item.get(field)
                if isinstance(value, (list, tuple, set)):
                    value = ",".join(str(part) for part in value)
                digest.update(str(value or "").encode("utf-8", errors="ignore"))
                digest.update(b"\0")
        return digest.hexdigest()

    def _snapshot_loop(self) -> None:
        while not self._stop_event.wait(float(self._snapshot_interval_s or 8)):
            self._post_bridge_heartbeat()
            callback = self.ongoing_callback
            if callback is None:
                continue
            try:
                items = callback("ALL")
                if not isinstance(items, list):
                    items = []
                digest = self._snapshot_digest(items)
                if digest == self._last_snapshot_hash:
                    continue
                self._last_snapshot_hash = digest
                self._request_json(
                    "POST",
                    "/api/qt/ongoing-snapshot",
                    payload={"items": items},
                    timeout=5.0,
                )
            except Exception as exc:
                log_warning(f"Qt 进行中快照上报失败: {exc}")

    def set_notice_callback(self, callback) -> None:
        self.notice_callback = callback

    def set_ongoing_callback(self, callback) -> None:
        self.ongoing_callback = callback
        self._ensure_bridge_threads()

    def set_ongoing_delete_callback(self, callback) -> None:
        self.ongoing_delete_callback = (
            callback if self._legacy_portal_callbacks_enabled else None
        )
        self._ensure_bridge_threads()

    def set_maintenance_action_callback(self, callback) -> None:
        self.maintenance_action_callback = (
            callback if self._legacy_portal_callbacks_enabled else None
        )
        self._ensure_bridge_threads()

    def set_shell_event_callback(self, callback) -> None:
        self.shell_event_callback = callback
        self._ensure_bridge_threads()

    def mark_job_upload_result(self, job_id: str, **kwargs) -> None:
        job_id = str(job_id or "").strip()
        if not job_id:
            return
        try:
            self._request_json(
                "POST",
                f"/api/qt/jobs/{urllib.parse.quote(job_id)}/result",
                payload=dict(kwargs or {}),
                timeout=5.0,
            )
        except Exception as exc:
            log_warning(f"后端任务结果回写失败: job_id={job_id}, error={exc}")

    def mark_job_progress(self, job_id: str, **patch) -> None:
        job_id = str(job_id or "").strip()
        if not job_id:
            return
        try:
            self._request_json(
                "POST",
                f"/api/qt/jobs/{urllib.parse.quote(job_id)}/progress",
                payload=dict(patch or {}),
                timeout=5.0,
            )
        except Exception as exc:
            log_warning(f"后端任务进度回写失败: job_id={job_id}, error={exc}")

    def get_job(self, job_id: str) -> dict | None:
        job_id = str(job_id or "").strip()
        if not job_id:
            return None
        try:
            result = self._request_json(
                "GET",
                f"/api/qt/jobs/{urllib.parse.quote(job_id)}",
                timeout=3.0,
            )
            if result.get("ok") and isinstance(result.get("data"), dict):
                return result.get("data")
        except Exception:
            return None
        return None

    def execute_qt_notice_upload(self, payload: dict[str, Any]) -> dict:
        payload = dict(payload or {})
        action_type = str(payload.get("action_type") or "").strip()
        command = {
            "upload": "notice_upload",
            "update": "notice_update",
            "end": "notice_end",
            "upload_replace": "notice_archive",
        }.get(action_type, "notice_upload")
        try:
            return self.submit_qt_command(command, payload)
        except Exception as exc:
            raise RuntimeError(f"本机后端执行 Qt 上传失败: {exc}") from exc

    def upload_notice_attachment(
        self,
        content: bytes,
        *,
        file_name: str = "notice_image.png",
        mime_type: str = "image/png",
    ) -> dict[str, Any]:
        if not content:
            raise RuntimeError("图片内容为空。")
        quoted_name = urllib.parse.quote(str(file_name or "notice_image.png"))
        result = self._request_binary_json(
            "POST",
            f"/api/qt/local/notice-attachments?file_name={quoted_name}",
            content=bytes(content),
            content_type=str(mime_type or "image/png"),
            timeout=60.0,
        )
        if not bool(result.get("ok")):
            raise RuntimeError(str(result.get("error") or "上传本机图片附件失败。"))
        data = result.get("data")
        return data if isinstance(data, dict) else {}

    def submit_qt_command(
        self,
        command: str,
        payload: dict[str, Any] | None = None,
        *,
        timeout: float = 120.0,
    ) -> dict:
        request_payload = {
            "command": str(command or "").strip(),
            "payload": dict(payload or {}),
        }
        result = self._request_json(
            "POST",
            "/api/qt/commands",
            payload=request_payload,
            timeout=max(1.0, float(timeout or 120.0)),
        )
        if not bool(result.get("ok")):
            raise RuntimeError(str(result.get("error") or "提交 Qt command 失败。"))
        data = result.get("data")
        return data if isinstance(data, dict) else {}

    def submit_qt_dialog_result(self, session_id: str, *, status: str, result_payload: dict[str, Any] | None = None) -> dict:
        request_payload = {
            "session_id": str(session_id or "").strip(),
            "status": str(status or "").strip(),
            "result": dict(result_payload or {}),
        }
        result = self._request_json(
            "POST",
            "/api/qt/dialog-result",
            payload=request_payload,
            timeout=30.0,
        )
        if not bool(result.get("ok")):
            raise RuntimeError(str(result.get("error") or "提交 dialog 结果失败。"))
        data = result.get("data")
        return data if isinstance(data, dict) else {}

    def create_qt_dialog_session(self, payload: dict[str, Any] | None = None) -> dict:
        result = self._request_json(
            "POST",
            "/api/qt/dialog-sessions",
            payload=dict(payload or {}),
            timeout=30.0,
        )
        if not bool(result.get("ok")):
            raise RuntimeError(str(result.get("error") or "创建 dialog session 失败。"))
        data = result.get("data")
        return data if isinstance(data, dict) else {}

    def post_local_clipboard_event(
        self,
        content: str,
        *,
        ts: int | None = None,
        source: str = "clipboard",
        target_record_id: str = "",
    ) -> dict:
        payload = {
            "content": str(content or "").strip(),
            "ts": int(ts or time.time() * 1000),
            "source": str(source or "clipboard"),
            "target_record_id": str(target_record_id or "").strip(),
        }
        result = self._request_json(
            "POST",
            "/api/qt/local/clipboard-event",
            payload=payload,
            timeout=10.0,
        )
        if not bool(result.get("ok")):
            raise RuntimeError(str(result.get("error") or "提交剪贴板事件失败。"))
        data = result.get("data")
        return data if isinstance(data, dict) else {}

    def post_local_heartbeat(self, payload: dict[str, Any] | None = None) -> dict:
        result = self._request_json(
            "POST",
            "/api/qt/local/heartbeat",
            payload=dict(payload or {}),
            timeout=5.0,
        )
        if not bool(result.get("ok")):
            raise RuntimeError(str(result.get("error") or "Qt 本机心跳失败。"))
        data = result.get("data")
        return data if isinstance(data, dict) else {}

    def acknowledge_clipboard_candidate(
        self,
        candidate_id: str,
        *,
        ok: bool = True,
        status: str = "",
    ) -> dict:
        candidate_id = str(candidate_id or "").strip()
        if not candidate_id:
            raise RuntimeError("缺少 clipboard candidate_id。")
        result = self._request_json(
            "POST",
            f"/api/qt/clipboard-candidates/{urllib.parse.quote(candidate_id)}/ack",
            payload={"ok": bool(ok), "status": str(status or "")},
            timeout=10.0,
        )
        if not bool(result.get("ok")):
            raise RuntimeError(str(result.get("error") or "确认 clipboard candidate 失败。"))
        data = result.get("data")
        return data if isinstance(data, dict) else {}

    def post_active_items_delta(
        self,
        *,
        upserts: list[dict[str, Any]] | None = None,
        deletes: list[dict[str, Any]] | None = None,
    ) -> None:
        clean_upserts = [item for item in (upserts or []) if isinstance(item, dict)]
        clean_deletes = [item for item in (deletes or []) if isinstance(item, dict)]
        if not clean_upserts and not clean_deletes:
            return
        with self._active_delta_lock:
            self._active_delta_upserts.extend(clean_upserts)
            self._active_delta_deletes.extend(clean_deletes)
            if self._active_delta_thread and self._active_delta_thread.is_alive():
                return
            self._active_delta_thread = threading.Thread(
                target=self._active_delta_loop,
                name="ClipFlowBackendActiveDeltaBridge",
                daemon=True,
            )
            self._active_delta_thread.start()

    def _active_delta_loop(self) -> None:
        while not self._stop_event.is_set():
            time.sleep(0.1)
            with self._active_delta_lock:
                upserts = self._active_delta_upserts
                deletes = self._active_delta_deletes
                self._active_delta_upserts = []
                self._active_delta_deletes = []
            if upserts or deletes:
                self._post_active_delta(upserts, deletes, timeout=3.0)
            with self._active_delta_lock:
                if not self._active_delta_upserts and not self._active_delta_deletes:
                    self._active_delta_thread = None
                    return

    def _post_active_delta(
        self,
        upserts: list[dict[str, Any]],
        deletes: list[dict[str, Any]],
        *,
        timeout: float,
    ) -> None:
        try:
            self._request_json(
                "POST",
                "/api/qt/active-items/delta",
                payload={"upserts": upserts, "deletes": deletes},
                timeout=timeout,
            )
        except Exception as exc:
            log_warning(f"Qt active item 增量上报失败: {exc}")

    def _flush_active_delta_once(self, *, timeout: float = 3.0) -> None:
        with self._active_delta_lock:
            upserts = self._active_delta_upserts
            deletes = self._active_delta_deletes
            self._active_delta_upserts = []
            self._active_delta_deletes = []
        if upserts or deletes:
            self._post_active_delta(upserts, deletes, timeout=timeout)
