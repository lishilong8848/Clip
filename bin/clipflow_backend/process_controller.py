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
        self._last_snapshot_hash = ""
        self._last_bridge_heartbeat = 0.0
        self._slow_event_count = 0
        self._last_event_duration_ms = 0.0
        self._last_event_kind = ""
        self._last_event_error = ""
        self._slow_event_history: list[dict[str, Any]] = []

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

    def _health_ok(self) -> bool:
        try:
            result = self._request_json("GET", "/api/health", timeout=1.2)
            return bool(result.get("ok"))
        except Exception:
            return False

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
        if not self._wait_for_health():
            code = self._process.poll() if self._process else None
            self.stop()
            raise RuntimeError(f"局域网后端进程启动失败，退出码={code}")
        self._ensure_bridge_threads()
        log_info(f"局域网后端子进程已启动: {self.get_url()}")
        return self.get_url()

    def stop(self) -> None:
        self._stop_event.set()
        if self._owns_process:
            try:
                self._request_json("POST", "/api/backend/shutdown", payload={}, timeout=1.5)
            except Exception:
                pass
        for attr in ("_event_thread", "_snapshot_thread"):
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
        while not self._stop_event.wait(delay):
            if (
                self.maintenance_action_callback is None
                and self.ongoing_delete_callback is None
                and self.notice_callback is None
            ):
                delay = 1.0
                continue
            try:
                result = self._request_json("GET", "/api/qt/events?limit=1", timeout=3.0)
                items = ((result.get("data") or {}).get("items") or []) if result.get("ok") else []
            except Exception:
                delay = 2.0
                continue
            for item in items:
                self._dispatch_event(item)
            delay = 0.25 if items else 1.0

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
                callback_payload = payload.get("payload") if isinstance(payload.get("payload"), dict) else {}
                callback = self.maintenance_action_callback
                if callback is None:
                    raise RuntimeError("Qt 通告动作回调未连接。")
                accepted = callback(callback_payload)
                ok = bool(accepted.get("ok")) if isinstance(accepted, dict) else bool(accepted)
                error = str(accepted.get("error") or "") if isinstance(accepted, dict) else ""
            elif kind == "ongoing_delete":
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
                },
                timeout=2.0,
            )
        except Exception:
            return

    def _snapshot_loop(self) -> None:
        while not self._stop_event.wait(3.0):
            self._post_bridge_heartbeat()
            callback = self.ongoing_callback
            if callback is None:
                continue
            try:
                items = callback("ALL")
                if not isinstance(items, list):
                    items = []
                raw = json.dumps(items, ensure_ascii=False, sort_keys=True, default=str)
                digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()
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
        self.ongoing_delete_callback = callback
        self._ensure_bridge_threads()

    def set_maintenance_action_callback(self, callback) -> None:
        self.maintenance_action_callback = callback
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
