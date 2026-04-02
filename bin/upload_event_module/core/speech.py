# -*- coding: utf-8 -*-
import threading
import queue
import time
import subprocess
import sys
from ..logger import log_error, log_info
from ..config import config


class SpeechManager:
    def __init__(self):
        self._queue = queue.Queue()
        self._thread = None
        self._enabled = self._speech_allowed_by_config()
        self._cooldown_seconds = 10.0
        self._last_spoken_at = 0.0
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._current_process = None
        log_info(
            "Speech status on startup: enabled=%s, disable_speech=%s, disable_alerts=%s"
            % (
                self._enabled,
                bool(getattr(config, "disable_speech", True)),
                bool(getattr(config, "disable_alerts", False)),
            )
        )
        if self._enabled:
            self._start_thread()

    @staticmethod
    def _speech_allowed_by_config() -> bool:
        return not bool(getattr(config, "disable_speech", True)) and not bool(
            getattr(config, "disable_alerts", False)
        )

    def _start_thread(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._worker_loop, daemon=True)
        self._thread.start()

    def speak(self, text):
        """Queue a speech task."""
        if not self._enabled or not self._speech_allowed_by_config():
            return
        now = time.monotonic()
        with self._lock:
            if now - self._last_spoken_at < self._cooldown_seconds:
                return
            self._last_spoken_at = now
        if not self._thread or not self._thread.is_alive():
            self._start_thread()
        try:
            self._queue.put_nowait(text)
        except Exception:
            self._queue.put(text)

    def set_enabled(self, enabled: bool):
        requested = bool(enabled)
        enabled = requested
        if enabled and not self._speech_allowed_by_config():
            enabled = False
        log_info(
            "Speech set_enabled request=%s result=%s disable_speech=%s disable_alerts=%s"
            % (
                requested,
                bool(enabled),
                bool(getattr(config, "disable_speech", True)),
                bool(getattr(config, "disable_alerts", False)),
            )
        )
        if enabled == self._enabled:
            return
        if not enabled:
            self.shutdown()
            return
        self._enabled = True
        self._stop_event.clear()
        self._start_thread()

    def shutdown(self):
        self._enabled = False
        self._stop_event.set()
        # 清空队列，避免重启后播旧内容
        try:
            while True:
                item = self._queue.get_nowait()
                try:
                    self._queue.task_done()
                except Exception:
                    pass
                if item is None:
                    continue
        except queue.Empty:
            pass

        # 终止正在播放的进程
        proc = None
        with self._lock:
            proc = self._current_process
        if proc and proc.poll() is None:
            try:
                proc.terminate()
                proc.wait(timeout=0.5)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass

        try:
            self._queue.put_nowait(None)
        except Exception:
            pass

        thread = self._thread
        if thread and thread.is_alive():
            try:
                thread.join(timeout=0.5)
            except Exception:
                pass

        with self._lock:
            self._current_process = None
        self._thread = None

    def _worker_loop(self):
        """Speech worker loop using SAPI via PowerShell."""
        while True:
            text = self._queue.get()
            if text is None:  # Sentinel to stop
                try:
                    self._queue.task_done()
                except Exception:
                    pass
                break
            if self._stop_event.is_set():
                try:
                    self._queue.task_done()
                except Exception:
                    pass
                continue

            try:
                log_info(f"Speech start: {text}")
                self._speak_sapi(text)
                log_info(f"Speech done: {text}")
            except Exception as e:
                log_error(f"Speech task error: {e}")
            finally:
                self._queue.task_done()

    def _speak_sapi(self, text):
        """Speak via Windows SAPI (PowerShell subprocess)."""
        try:
            # Use a separate PowerShell process to avoid COM issues in-process.
            safe_text = text.replace('"', '\\"')
            cmd = [
                "powershell",
                "-Command",
                (
                    "Add-Type -AssemblyName System.Speech; "
                    "$s = New-Object System.Speech.Synthesis.SpeechSynthesizer; "
                    f'$s.Speak("{safe_text}")'
                ),
            ]
            startupinfo = None
            if sys.platform == "win32":
                startupinfo = subprocess.STARTUPINFO()
                startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            proc = subprocess.Popen(cmd, startupinfo=startupinfo)
            with self._lock:
                self._current_process = proc
            try:
                while True:
                    if self._stop_event.is_set():
                        try:
                            proc.terminate()
                        except Exception:
                            pass
                        break
                    if proc.poll() is not None:
                        break
                    time.sleep(0.1)
                try:
                    proc.wait(timeout=0.5)
                except Exception:
                    pass
            finally:
                with self._lock:
                    if self._current_process is proc:
                        self._current_process = None
        except Exception as e:
            log_error(f"All speech methods failed: {e}")


# Global singleton
speech_manager = SpeechManager()
