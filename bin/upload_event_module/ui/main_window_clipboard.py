# -*- coding: utf-8 -*-
import os
import json
import time
import sys
import hashlib
from PyQt6.QtCore import Qt, QTimer, QProcess, QProcessEnvironment

from ..config import config
from ..logger import log_info, log_error, log_warning
from ..core.parser import extract_event_info

class MainWindowClipboardMixin:
    def _is_clipboard_listener_disabled(self) -> bool:
        return bool(getattr(config, "disable_clipboard_listener", False))

    def _is_manual_clipboard_resume_in_progress(self) -> bool:
        return bool(
            self._clipboard_toggle_transition_in_progress
            and not self._clipboard_toggle_target_disabled
            and not self._clipboard_desired_disabled
        )

    def _set_clipboard_toggle_checked(self, checked: bool):
        toggle = getattr(self, "clipboard_toggle", None)
        if toggle is None:
            return
        try:
            toggle.blockSignals(True)
            toggle.setChecked(bool(checked))
        finally:
            toggle.blockSignals(False)

    def _clipboard_runtime_status_text(self) -> str:
        if self._clipboard_toggle_transition_in_progress:
            return (
                "剪贴板监听: 正在暂停..."
                if self._clipboard_toggle_target_disabled
                else "剪贴板监听: 正在恢复..."
            )
        if self._clipboard_degraded:
            return "剪贴板监听: 已降级暂停"
        if self._is_clipboard_listener_disabled():
            return "剪贴板监听: 已手动暂停"
        if bool(getattr(self, "_clipboard_paused_for_patch_update", False)):
            return "剪贴板监听: 临时暂停（补丁更新）"
        if self.clipboard_paused:
            if self._is_manual_add_dialog_active():
                return "剪贴板监听: 临时暂停（手动添加）"
            if self._is_screenshot_dialog_active():
                return "剪贴板监听: 临时暂停（截图中）"
            return "剪贴板监听: 临时暂停"
        return "剪贴板监听: 已启用"

    def _clipboard_runtime_detail_text(self) -> str:
        reason = str(self._clipboard_last_restore_failure_reason or "").strip()
        if not reason:
            return ""
        if self._clipboard_toggle_transition_in_progress:
            return ""
        if self._clipboard_effective_running:
            return ""
        if not self._is_clipboard_listener_disabled():
            return ""
        compact_reason = reason.replace("\n", " | ")
        if len(compact_reason) > 72:
            compact_reason = compact_reason[:69] + "..."
        return f"最近恢复失败: {compact_reason}"

    def _refresh_clipboard_toggle_ui(self):
        toggle = getattr(self, "clipboard_toggle", None)
        if toggle is not None:
            self._set_clipboard_toggle_checked(self._is_clipboard_listener_disabled())
            toggle.setEnabled(not self._clipboard_toggle_transition_in_progress)
            toggle.setToolTip(self._clipboard_last_restore_failure_reason or "")
        label = getattr(self, "clipboard_status_label", None)
        if label is not None:
            label.setText(self._clipboard_runtime_status_text())
            label.setToolTip(self._clipboard_last_restore_failure_reason or "")
            text = label.text()
            color = "#10B981"
            if "已手动暂停" in text or "临时暂停" in text:
                color = "#F59E0B"
            if "正在" in text:
                color = "#3B82F6"
            if "降级" in text:
                color = "#EF4444"
            if (
                "已启用" in text
                and not self._clipboard_effective_running
                and self._clipboard_last_restore_failure_reason
            ):
                color = "#EF4444"
            label.setStyleSheet(f"color: {color}; font-size: 11px;")
        detail_label = getattr(self, "clipboard_status_detail_label", None)
        if detail_label is not None:
            detail_text = self._clipboard_runtime_detail_text()
            detail_label.setText(detail_text)
            detail_label.setToolTip(self._clipboard_last_restore_failure_reason or "")
            detail_label.setVisible(bool(detail_text))

    def _set_clipboard_transition(self, in_progress: bool, target_disabled: bool | None = None):
        self._clipboard_toggle_transition_in_progress = bool(in_progress)
        if target_disabled is not None:
            self._clipboard_toggle_target_disabled = bool(target_disabled)
        self._refresh_clipboard_toggle_ui()

    def _build_clipboard_restore_failure_reason(self) -> str:
        parts = []
        error_text = str(self._clipboard_last_error or "").strip()
        if error_text:
            parts.append(error_text)
        stderr_tail = "\n".join(self._clipboard_stderr_tail[-3:]).strip()
        if stderr_tail:
            compact_tail = stderr_tail.replace("\n", " | ")
            if compact_tail not in parts:
                parts.append(compact_tail)
        if not parts and self._clipboard_trace_file.exists():
            parts.append(f"详见 {self._clipboard_trace_file}")
        return " | ".join(parts)

    def _mark_clipboard_restore_failed(self, *, request_seq: int, reason: str = ""):
        if request_seq != self._clipboard_toggle_request_seq:
            return
        if (
            not self._clipboard_toggle_transition_in_progress
            or self._clipboard_toggle_target_disabled
        ):
            return
        timer = getattr(self, "clipboard_file_timer", None)
        if timer:
            try:
                timer.stop()
            except Exception:
                pass
        process = getattr(self, "_clipboard_process", None)
        if process and process.state() != QProcess.ProcessState.NotRunning:
            self._stop_clipboard_process(wait_ms=500)
        self._clipboard_effective_running = False
        self._clipboard_last_restore_failure_reason = (
            str(reason or "").strip() or self._build_clipboard_restore_failure_reason()
        )
        self._finalize_clipboard_toggle_transition(
            success=False,
            disabled=False,
            request_seq=request_seq,
        )
        detail = (
            f" ({self._clipboard_last_restore_failure_reason})"
            if self._clipboard_last_restore_failure_reason
            else ""
        )
        log_warning(f"剪贴板监听: 恢复失败，已回滚显示状态{detail}")

    def _confirm_clipboard_listener_restored(self, *, request_seq: int, timeout: bool = False):
        if request_seq != self._clipboard_toggle_request_seq:
            return
        if (
            not self._clipboard_toggle_transition_in_progress
            or self._clipboard_toggle_target_disabled
        ):
            return
        process = getattr(self, "_clipboard_process", None)
        success = bool(process and process.state() == QProcess.ProcessState.Running)
        if success:
            self._clipboard_effective_running = True
            self._clipboard_last_restore_failure_reason = ""
            self._finalize_clipboard_toggle_transition(
                success=True,
                disabled=False,
                request_seq=request_seq,
            )
            log_info("剪贴板监听: 已启用")
            return
        if not timeout and process and process.state() == QProcess.ProcessState.Starting:
            return
        self._mark_clipboard_restore_failed(
            request_seq=request_seq,
            reason=(
                self._build_clipboard_restore_failure_reason()
                or "启动超时，未收到 started 信号"
            ),
        )

    def _begin_clipboard_pause_transition(self, *, request_seq: int):
        process = getattr(self, "_clipboard_process", None)
        if not process or process.state() == QProcess.ProcessState.NotRunning:
            self._clipboard_effective_running = False
            self._clipboard_last_restore_failure_reason = ""
            self._finalize_clipboard_toggle_transition(
                success=True,
                disabled=True,
                request_seq=request_seq,
            )
            log_info("剪贴板监听: 已禁用")
            return
        self._clipboard_stopping = True
        try:
            process.terminate()
        except Exception:
            pass
        QTimer.singleShot(
            1500,
            lambda seq=request_seq: self._confirm_clipboard_listener_paused(
                request_seq=seq,
                timeout=True,
            ),
        )

    def _confirm_clipboard_listener_paused(
        self,
        *,
        request_seq: int,
        timeout: bool = False,
        kill_attempted: bool = False,
    ):
        if request_seq != self._clipboard_toggle_request_seq:
            return
        if (
            not self._clipboard_toggle_transition_in_progress
            or not self._clipboard_toggle_target_disabled
        ):
            return
        process = getattr(self, "_clipboard_process", None)
        if not process or process.state() == QProcess.ProcessState.NotRunning:
            self._clipboard_effective_running = False
            self._clipboard_last_restore_failure_reason = ""
            self._finalize_clipboard_toggle_transition(
                success=True,
                disabled=True,
                request_seq=request_seq,
            )
            log_info("剪贴板监听: 已禁用")
            return
        if not timeout:
            return
        if not kill_attempted:
            try:
                process.kill()
            except Exception:
                pass
            QTimer.singleShot(
                350,
                lambda seq=request_seq: self._confirm_clipboard_listener_paused(
                    request_seq=seq,
                    timeout=True,
                    kill_attempted=True,
                ),
            )
            return
        timer = getattr(self, "clipboard_file_timer", None)
        if timer:
            try:
                if not timer.isActive():
                    timer.start(200)
            except Exception:
                pass
        self._clipboard_effective_running = bool(
            process and process.state() != QProcess.ProcessState.NotRunning
        )
        self._clipboard_last_restore_failure_reason = "暂停失败，监听进程未能及时停止"
        self._finalize_clipboard_toggle_transition(
            success=False,
            disabled=False,
            request_seq=request_seq,
        )
        log_warning("剪贴板监听: 暂停失败，已回滚显示状态")

    def _finalize_clipboard_toggle_transition(
        self,
        *,
        success: bool,
        disabled: bool,
        request_seq: int,
    ):
        if request_seq != self._clipboard_toggle_request_seq:
            return
        if success:
            config.disable_clipboard_listener = bool(disabled)
            self._clipboard_desired_disabled = bool(disabled)
            if not disabled:
                self._clipboard_auto_restart_blocked = False
                self._clipboard_resume_after_stop = False
                self._clipboard_degraded = False
                self._clipboard_restart_count = 0
                self._clipboard_crash_timestamps.clear()
            try:
                config.save(disable_clipboard_listener=bool(disabled))
            except Exception as exc:
                log_error(f"保存剪贴板监听开关失败: {exc}")
        self._set_clipboard_transition(False, target_disabled=disabled)

    def _on_clipboard_toggle_changed(self, checked: bool):
        disabled = bool(checked)
        if self._clipboard_toggle_transition_in_progress:
            self._refresh_clipboard_toggle_ui()
            return
        self._clipboard_toggle_request_seq += 1
        self._clipboard_desired_disabled = disabled
        self._set_clipboard_transition(True, target_disabled=disabled)
        self._schedule_clipboard_listener_setting_apply()

    def _schedule_clipboard_listener_setting_apply(self):
        if self._clipboard_apply_setting_pending:
            return
        self._clipboard_apply_setting_pending = True

        def _apply():
            self._clipboard_apply_setting_pending = False
            self._apply_clipboard_listener_setting()

        QTimer.singleShot(0, _apply)

    def _apply_clipboard_listener_setting(self):
        request_seq = self._clipboard_toggle_request_seq
        disabled = bool(self._clipboard_desired_disabled)
        if disabled:
            self._clipboard_pending_lines.clear()
            timer = getattr(self, "clipboard_file_timer", None)
            if timer:
                try:
                    timer.stop()
                except Exception:
                    pass
            self._begin_clipboard_pause_transition(request_seq=request_seq)
            return

        timer = getattr(self, "clipboard_file_timer", None)
        if timer:
            try:
                if not timer.isActive():
                    timer.start(200)
            except Exception:
                pass
        self._clipboard_last_restore_failure_reason = ""
        self._start_clipboard_listener()
        process = getattr(self, "_clipboard_process", None)
        if not process:
            self._mark_clipboard_restore_failed(
                request_seq=request_seq,
                reason=self._build_clipboard_restore_failure_reason(),
            )
            return
        QTimer.singleShot(
            int(self._clipboard_restore_verify_timeout_ms),
            lambda seq=request_seq: self._confirm_clipboard_listener_restored(
                request_seq=seq, timeout=True
            ),
        )

    def _write_clipboard_listener_trace(self, text: str):
        trace_text = str(text or "").strip()
        if not trace_text:
            return
        try:
            path = self._clipboard_trace_file
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as fh:
                fh.write(
                    f"\n[{time.strftime('%Y-%m-%d %H:%M:%S')}] "
                    f"clipboard_listener\n{trace_text}\n"
                )
        except Exception:
            pass

    def _read_clipboard_process_stream(
        self, stream_name: str, process: QProcess | None = None
    ):
        proc = process or getattr(self, "_clipboard_process", None)
        if proc is None:
            return
        try:
            if stream_name == "stdout":
                raw = bytes(proc.readAllStandardOutput())
            else:
                raw = bytes(proc.readAllStandardError())
        except Exception:
            return
        if not raw:
            return
        text = raw.decode("utf-8", errors="ignore").strip()
        if not text:
            return
        for line in text.splitlines():
            cleaned = line.strip()
            if cleaned:
                self._clipboard_stderr_tail.append(cleaned)
        if len(self._clipboard_stderr_tail) > self._clipboard_stderr_tail_limit:
            self._clipboard_stderr_tail = self._clipboard_stderr_tail[
                -self._clipboard_stderr_tail_limit :
            ]
        self._write_clipboard_listener_trace(text)

    def _record_clipboard_crash(
        self,
        *,
        exit_code: int,
        exit_status: str,
        error_text: str,
        stderr_tail: str,
    ):
        now = time.monotonic()
        self._clipboard_crash_timestamps.append(now)
        window = float(self._clipboard_crash_window_seconds or 0.0)
        while (
            self._clipboard_crash_timestamps
            and (now - self._clipboard_crash_timestamps[0]) > window
        ):
            self._clipboard_crash_timestamps.popleft()

        detail_parts = [
            f"exit_code={exit_code}",
            f"exit_status={exit_status}",
        ]
        if error_text:
            detail_parts.append(f"error={error_text}")
        if stderr_tail:
            detail_parts.append(f"stderr_tail={stderr_tail}")
        detail = " | ".join(detail_parts)
        self._emit_system_alert(
            event_code="clipboard.listener.crashed",
            title="剪贴板监听进程异常退出",
            detail=detail,
            dedup_key=f"{exit_code}:{error_text}",
            extra={"trace_path": str(self._clipboard_trace_file)},
        )

    def _maybe_degrade_clipboard_listener(self) -> bool:
        now = time.monotonic()
        window = float(self._clipboard_crash_window_seconds or 0.0)
        while (
            self._clipboard_crash_timestamps
            and (now - self._clipboard_crash_timestamps[0]) > window
        ):
            self._clipboard_crash_timestamps.popleft()
        if len(self._clipboard_crash_timestamps) < int(
            self._clipboard_crash_threshold or 0
        ):
            return False
        if self._clipboard_degraded:
            return True

        self._clipboard_degraded = True
        self._clipboard_auto_restart_blocked = True
        self._clipboard_resume_after_stop = False
        log_error("剪贴板监听已进入降级模式：崩溃次数过多，自动暂停监听。")
        self._emit_system_alert(
            event_code="clipboard.listener.degraded",
            title="剪贴板监听已自动降级暂停",
            detail=("剪贴板监听在5分钟内崩溃次数达到阈值，已自动勾选暂停剪贴板监听。"),
            dedup_key="clipboard_degraded",
            extra={
                "trace_path": str(self._clipboard_trace_file),
                "crash_count": len(self._clipboard_crash_timestamps),
                "window_seconds": int(self._clipboard_crash_window_seconds),
            },
        )
        toggle = getattr(self, "clipboard_toggle", None)
        self._clipboard_desired_disabled = True
        self._clipboard_toggle_request_seq += 1
        if toggle is not None and not toggle.isChecked():
            self._set_clipboard_toggle_checked(True)
        try:
            config.disable_clipboard_listener = True
            config.save(disable_clipboard_listener=True)
        except Exception as exc:
            log_error(f"降级状态保存失败: {exc}")
        self._refresh_clipboard_toggle_ui()
        return True

    def _init_clipboard_ipc(self):
        try:
            self.clipboard_event_file.parent.mkdir(parents=True, exist_ok=True)
            self.clipboard_event_file.write_text("", encoding="utf-8")
        except Exception:
            pass
        self._clipboard_file_index = 0
        self._clipboard_partial_line = ""

        self.clipboard_file_timer = QTimer(self)
        self.connection_registry.connect(
            "clipboard_file_timer",
            self.clipboard_file_timer,
            "timeout",
            self._poll_clipboard_event_file,
        )

        self.connection_registry.connect(
            "main_window",
            self,
            "clipboard_entry_received",
            self._on_clipboard_entry_received,
            Qt.ConnectionType.QueuedConnection,
        )

        if not self._is_clipboard_listener_disabled():
            self.clipboard_file_timer.start(200)
            QTimer.singleShot(200, self._start_clipboard_listener)
            QTimer.singleShot(0, self._replay_pending_clipboard_entries)
        else:
            log_info("剪贴板监听: 启动时已禁用")
        self._clipboard_effective_running = bool(
            self._clipboard_process
            and self._clipboard_process.state() == QProcess.ProcessState.Running
        )
        self._refresh_clipboard_toggle_ui()

    def _start_clipboard_listener(self):
        if self._is_clipboard_listener_disabled() and not self._is_manual_clipboard_resume_in_progress():
            self._clipboard_effective_running = False
            self._refresh_clipboard_toggle_ui()
            return
        if self._closing:
            return
        if (
            self._clipboard_process
            and self._clipboard_process.state() != QProcess.ProcessState.NotRunning
        ):
            self._clipboard_effective_running = True
            self._refresh_clipboard_toggle_ui()
            return
        try:
            self._clipboard_stopping = False
            self._clipboard_last_error = ""
            self._clipboard_stderr_tail = []
            self._clipboard_process = QProcess(self)
            env = QProcessEnvironment.systemEnvironment()
            env.insert("CLIPFLOW_CLIPBOARD_FILE", str(self.clipboard_event_file))
            env.insert("CLIPFLOW_CLIPBOARD_TRACE_FILE", str(self._clipboard_trace_file))
            env.insert("PYTHONIOENCODING", "utf-8")
            env.insert("PYTHONUTF8", "1")
            self._clipboard_process.setProcessEnvironment(env)
            self._clipboard_process.started.connect(self._on_clipboard_process_started)
            self._clipboard_process.finished.connect(
                self._on_clipboard_process_finished
            )
            self._clipboard_process.errorOccurred.connect(
                self._on_clipboard_process_error
            )
            self._clipboard_process.readyReadStandardError.connect(
                lambda: self._read_clipboard_process_stream("stderr")
            )
            self._clipboard_process.readyReadStandardOutput.connect(
                lambda: self._read_clipboard_process_stream("stdout")
            )

            script_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "剪贴板监听测试.py"
            )
            if not os.path.exists(script_path):
                log_error(f"找不到剪贴板监听脚本: {script_path}")
                self._emit_system_alert(
                    event_code="clipboard.listener.start_failed",
                    title="剪贴板监听启动失败",
                    detail=f"找不到监听脚本: {script_path}",
                    dedup_key="script_missing",
                )
                self._clipboard_last_error = f"找不到监听脚本: {script_path}"
                self._clipboard_process = None
                return

            python_exe = sys.executable
            self._clipboard_process.start(python_exe, ["-u", script_path])
            self._clipboard_effective_running = False
            self._refresh_clipboard_toggle_ui()
        except Exception as exc:
            log_error(f"启动剪贴板监听进程失败: {exc}")
            self._clipboard_last_error = str(exc)
            self._emit_system_alert(
                event_code="clipboard.listener.start_failed",
                title="剪贴板监听启动失败",
                detail=str(exc),
                dedup_key="process_start_exception",
                extra={"trace_path": str(self._clipboard_trace_file)},
            )
            self._clipboard_process = None
            self._clipboard_effective_running = False
            self._refresh_clipboard_toggle_ui()

    def _on_clipboard_process_started(self, process: QProcess | None = None):
        process = process or self.sender()
        current = getattr(self, "_clipboard_process", None)
        if process is not None and process is not current:
            return
        self._clipboard_last_error = ""
        self._clipboard_effective_running = True
        if (
            self._clipboard_toggle_transition_in_progress
            and not self._clipboard_toggle_target_disabled
        ):
            self._confirm_clipboard_listener_restored(
                request_seq=self._clipboard_toggle_request_seq
            )
            return
        self._refresh_clipboard_toggle_ui()

    def _on_clipboard_process_finished(self, exit_code, exit_status):
        process = self._clipboard_process
        self._read_clipboard_process_stream("stderr", process=process)
        self._read_clipboard_process_stream("stdout", process=process)
        stderr_tail = "\n".join(self._clipboard_stderr_tail[-10:])
        expected_stop = bool(self._clipboard_stopping)
        restart_blocked = bool(getattr(self, "_clipboard_auto_restart_blocked", False))
        resume_after_stop = bool(getattr(self, "_clipboard_resume_after_stop", False))
        error_text = str(self._clipboard_last_error or "").strip()
        exit_code_int = int(exit_code or 0)
        exit_status_name = (
            "CrashExit"
            if exit_status == QProcess.ExitStatus.CrashExit
            else "NormalExit"
        )
        abnormal_exit = bool(
            exit_status == QProcess.ExitStatus.CrashExit
            or exit_code_int != 0
            or error_text
        )
        self._clipboard_process = None
        self._clipboard_effective_running = False
        self._clipboard_stopping = False
        self._refresh_clipboard_toggle_ui()
        if (
            self._clipboard_toggle_transition_in_progress
            and self._clipboard_toggle_target_disabled
        ):
            self._clipboard_last_restore_failure_reason = ""
            self._finalize_clipboard_toggle_transition(
                success=True,
                disabled=True,
                request_seq=self._clipboard_toggle_request_seq,
            )
            self._clipboard_restart_count = 0
            self._clipboard_stderr_tail = []
            log_info("剪贴板监听: 已禁用")
            return
        if (
            self._clipboard_toggle_transition_in_progress
            and not self._clipboard_toggle_target_disabled
        ):
            if abnormal_exit:
                self._record_clipboard_crash(
                    exit_code=exit_code_int,
                    exit_status=exit_status_name,
                    error_text=error_text,
                    stderr_tail=stderr_tail,
                )
            self._mark_clipboard_restore_failed(
                request_seq=self._clipboard_toggle_request_seq,
                reason=self._build_clipboard_restore_failure_reason(),
            )
            return
        if (
            self._closing
            or self._is_clipboard_listener_disabled()
            or expected_stop
            or restart_blocked
            or bool(getattr(self, "_clipboard_paused_for_patch_update", False))
            or self.clipboard_paused
        ):
            self._clipboard_restart_count = 0
            if (
                resume_after_stop
                and not self._closing
                and not self._is_clipboard_listener_disabled()
                and not self.clipboard_paused
            ):
                self._clipboard_resume_after_stop = False
                self._clipboard_auto_restart_blocked = False
                QTimer.singleShot(0, self._start_clipboard_listener)
                log_info("剪贴板监听: 更新完成后已恢复")
            self._clipboard_last_error = ""
            self._clipboard_stderr_tail = []
            return
        if abnormal_exit:
            self._record_clipboard_crash(
                exit_code=exit_code_int,
                exit_status=exit_status_name,
                error_text=error_text,
                stderr_tail=stderr_tail,
            )
            if self._maybe_degrade_clipboard_listener():
                self._clipboard_restart_count = 0
                self._clipboard_last_error = ""
                self._clipboard_stderr_tail = []
                return
        self._clipboard_restart_count += 1
        delay = min(1000 * (2 ** (self._clipboard_restart_count - 1)), 30000)
        if self._clipboard_restart_count > 5:
            delay = 5000
        log_warning(f"剪贴板监听进程退出(代码 {exit_code})，{delay}ms 后重启")
        self._clipboard_last_error = ""
        self._clipboard_stderr_tail = []
        QTimer.singleShot(delay, self._start_clipboard_listener)

    def _on_clipboard_process_error(self, error):
        error_text = str(error)
        self._clipboard_last_error = error_text
        if (
            self._clipboard_toggle_transition_in_progress
            and not self._clipboard_toggle_target_disabled
        ):
            log_error(f"剪贴板监听进程错误: {error}")
            self._emit_system_alert(
                event_code="clipboard.listener.process_error",
                title="剪贴板监听进程错误",
                detail=error_text,
                dedup_key=f"restore:{error_text}",
                extra={"trace_path": str(self._clipboard_trace_file)},
            )
            self._mark_clipboard_restore_failed(
                request_seq=self._clipboard_toggle_request_seq,
                reason=self._build_clipboard_restore_failure_reason(),
            )
            return
        if (
            self._closing
            or self._is_clipboard_listener_disabled()
            or self._clipboard_stopping
            or bool(getattr(self, "_clipboard_auto_restart_blocked", False))
            or bool(getattr(self, "_clipboard_paused_for_patch_update", False))
        ):
            log_info(f"剪贴板监听进程已停止: {error}")
            self._refresh_clipboard_toggle_ui()
            return
        log_error(f"剪贴板监听进程错误: {error}")
        self._emit_system_alert(
            event_code="clipboard.listener.process_error",
            title="剪贴板监听进程错误",
            detail=error_text,
            dedup_key=error_text,
            extra={"trace_path": str(self._clipboard_trace_file)},
        )

    def _stop_clipboard_process(self, wait_ms: int = 0):
        process = getattr(self, "_clipboard_process", None)
        if not process:
            self._clipboard_stopping = False
            self._clipboard_effective_running = False
            self._refresh_clipboard_toggle_ui()
            return
        wait_value = max(0, int(wait_ms or 0))
        try:
            self._clipboard_stopping = True
            process.terminate()
            if wait_value > 0:
                try:
                    finished = process.waitForFinished(wait_value)
                except Exception:
                    finished = False
                if not finished and process.state() != QProcess.ProcessState.NotRunning:
                    try:
                        process.kill()
                    except Exception:
                        pass
                    try:
                        process.waitForFinished(500)
                    except Exception:
                        pass
                if process.state() == QProcess.ProcessState.NotRunning:
                    self._clipboard_process = None
                    self._clipboard_stopping = False
                    self._clipboard_effective_running = False
                    self._refresh_clipboard_toggle_ui()
                    return
            QTimer.singleShot(
                1500, lambda p=process: self._force_kill_clipboard_process(p)
            )
        except Exception:
            self._clipboard_stopping = False
            pass

    def _force_kill_clipboard_process(self, process):
        try:
            if process and process.state() != QProcess.ProcessState.NotRunning:
                process.kill()
        except Exception:
            pass

    def _shutdown_clipboard_ipc(self, wait_ms: int = 0):
        timer = getattr(self, "clipboard_file_timer", None)
        if timer:
            try:
                timer.stop()
            except Exception:
                pass
        self._stop_clipboard_process(wait_ms=wait_ms)

    def _poll_clipboard_event_file(self):
        if self._closing:
            return
        if self._is_clipboard_listener_disabled():
            return
        if self.clipboard_paused:
            return
        if self._ui_update_in_progress:
            return
        cooldown = self._is_in_clipboard_cooldown()
        path = self.clipboard_event_file
        if not path.exists():
            if self._clipboard_pending_lines and not cooldown:
                self._drain_clipboard_pending_lines()
            return
        try:
            file_size = path.stat().st_size
        except Exception:
            if self._clipboard_pending_lines and not cooldown:
                self._drain_clipboard_pending_lines()
            return
        if file_size <= 0:
            self._clipboard_file_index = 0
            self._clipboard_partial_line = ""
            if self._clipboard_pending_lines and not cooldown:
                self._drain_clipboard_pending_lines()
            return
        if file_size > int(self._clipboard_file_max_bytes or 0):
            try:
                path.write_text("", encoding="utf-8")
                file_size = 0
            except Exception:
                pass
            self._clipboard_file_index = 0
            self._clipboard_partial_line = ""
            return
        if self._clipboard_file_index > file_size:
            self._clipboard_file_index = 0
            self._clipboard_partial_line = ""
        if self._clipboard_file_index >= file_size:
            if self._clipboard_pending_lines and not cooldown:
                self._drain_clipboard_pending_lines()
            return
        try:
            with path.open("rb") as f:
                f.seek(self._clipboard_file_index)
                chunk = f.read()
                self._clipboard_file_index = f.tell()
        except Exception:
            if self._clipboard_pending_lines and not cooldown:
                self._drain_clipboard_pending_lines()
            return
        if not chunk:
            if self._clipboard_pending_lines and not cooldown:
                self._drain_clipboard_pending_lines()
            return
        chunk_text = chunk.decode("utf-8", errors="ignore")
        if self._clipboard_partial_line:
            chunk_text = self._clipboard_partial_line + chunk_text
            self._clipboard_partial_line = ""
        if chunk_text and not chunk_text.endswith("\n"):
            parts = chunk_text.splitlines()
            if parts:
                self._clipboard_partial_line = parts[-1]
                new_lines = parts[:-1]
            else:
                self._clipboard_partial_line = chunk_text
                new_lines = []
        else:
            new_lines = chunk_text.splitlines()
        for raw_line in new_lines:
            raw_line = (raw_line or "").strip()
            if not raw_line:
                continue
            try:
                data = json.loads(raw_line)
            except Exception:
                continue
            content = (data.get("content") or "").strip()
            if not content:
                continue
            self._update_last_clipboard_snapshot(content, data.get("ts"))
            self._clipboard_pending_lines.append(content)
        if self._clipboard_pending_lines and not cooldown:
            self._drain_clipboard_pending_lines()

    def _drain_clipboard_pending_lines(self):
        if not self._clipboard_pending_lines:
            return
        if self._is_clipboard_listener_disabled() or self.clipboard_paused:
            return
        if self._ui_update_in_progress:
            return
        if self._is_in_clipboard_cooldown():
            self._set_last_ui_op("clipboard_cooldown_drain")
            return
        max_count = max(1, int(self._clipboard_max_lines_per_tick or 1))
        batch = self._clipboard_pending_lines[:max_count]
        self._clipboard_pending_lines = self._clipboard_pending_lines[max_count:]
        self._set_last_ui_op("clipboard_drain", batch=len(batch))
        for text in batch:
            info = extract_event_info(text)
            if not info:
                continue
            entry = {
                "content": info.get("content") or text,
                "status": info.get("status", ""),
                "title": info.get("title", ""),
                "notice_type": info.get("notice_type", ""),
                "level": info.get("level"),
                "source": info.get("source", ""),
                "time_str": info.get("time_str", ""),
                "unique_key": info.get("unique_key", ""),
                "ts": int(time.time() * 1000),
            }
            entry_id = self._build_clipboard_entry_id(entry)
            entry["entry_id"] = entry_id
            if entry_id:
                if self._is_recent_clipboard_entry(entry_id):
                    continue
                self._mark_recent_clipboard_entry(entry_id)
            self._enqueue_ui_mutation(
                "clipboard_entry",
                lambda e=entry: self._on_clipboard_entry_received(e),
            )

    def _is_in_clipboard_cooldown(self):
        try:
            return time.monotonic() < float(self._clipboard_cooldown_until or 0.0)
        except Exception:
            return False

    def _build_clipboard_entry_id(self, entry: dict) -> str:
        key = (
            f"{entry.get('unique_key', '')}|{entry.get('status', '')}|"
            f"{entry.get('notice_type', '')}|{entry.get('title', '')}"
        )
        return hashlib.md5(key.encode("utf-8", errors="ignore")).hexdigest()

    def _prune_recent_clipboard_entries(self, now: float):
        window = float(self._clipboard_entry_dedupe_window_seconds or 0.0)
        if window <= 0:
            self._clipboard_recent_entries.clear()
            return
        stale_keys = [
            key
            for key, ts in self._clipboard_recent_entries.items()
            if (now - ts) >= window
        ]
        for key in stale_keys:
            self._clipboard_recent_entries.pop(key, None)

    def _is_recent_clipboard_entry(self, entry_id: str) -> bool:
        if not entry_id:
            return False
        now = time.monotonic()
        with self._clipboard_recent_lock:
            self._prune_recent_clipboard_entries(now)
            ts = self._clipboard_recent_entries.get(entry_id)
            if ts is None:
                return False
            return (now - ts) < float(self._clipboard_entry_dedupe_window_seconds)

    def _mark_recent_clipboard_entry(self, entry_id: str):
        if not entry_id:
            return
        now = time.monotonic()
        with self._clipboard_recent_lock:
            self._prune_recent_clipboard_entries(now)
            self._clipboard_recent_entries[entry_id] = now

    def _has_clipboard_pending_entry(self, entry_id: str) -> bool:
        if not entry_id:
            return False
        with self._clipboard_pending_lock:
            return any(
                e.get("entry_id") == entry_id for e in self._clipboard_pending_entries
            )

    def _add_clipboard_pending_entry(self, entry: dict) -> bool:
        entry_id = entry.get("entry_id")
        if not entry_id:
            return False
        with self._clipboard_pending_lock:
            if any(
                e.get("entry_id") == entry_id for e in self._clipboard_pending_entries
            ):
                return False
            self._clipboard_pending_entries.append(entry)
            if len(self._clipboard_pending_entries) > self._clipboard_retention:
                self._clipboard_pending_entries = self._clipboard_pending_entries[
                    -self._clipboard_retention :
                ]
        self.save_active_cache()
        return True

    def _remove_clipboard_pending_entry(self, entry_id: str):
        if not entry_id:
            return
        removed = False
        with self._clipboard_pending_lock:
            before = len(self._clipboard_pending_entries)
            self._clipboard_pending_entries = [
                e
                for e in self._clipboard_pending_entries
                if e.get("entry_id") != entry_id
            ]
            removed = len(self._clipboard_pending_entries) != before
        if removed:
            self.save_active_cache()

    def _get_clipboard_cache_payload(self):
        with self._clipboard_pending_lock:
            return list(self._clipboard_pending_entries)

    def _set_clipboard_cache_payload(self, entries):
        if not isinstance(entries, list):
            entries = []
        with self._clipboard_pending_lock:
            self._clipboard_pending_entries = [
                e for e in entries if isinstance(e, dict)
            ][-self._clipboard_retention :]

    def _replay_pending_clipboard_entries(self):
        with self._clipboard_pending_lock:
            pending = list(self._clipboard_pending_entries)
        for entry in pending:
            entry_id = entry.get("entry_id", "")
            if entry_id:
                if self._is_recent_clipboard_entry(entry_id):
                    continue
                self._mark_recent_clipboard_entry(entry_id)
            self._on_clipboard_entry_received(entry)

    def _pause_clipboard_timer(self):
        self.clipboard_paused = True
        self._refresh_clipboard_toggle_ui()

    def _resume_clipboard_timer(self):
        self.clipboard_paused = False
        self._refresh_clipboard_toggle_ui()
        if self._is_clipboard_listener_disabled():
            return
        self._try_process_deferred_events()

