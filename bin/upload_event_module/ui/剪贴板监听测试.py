# -*- coding: utf-8 -*-
import time
import os
import json
import sys
import ctypes
import hashlib
import traceback
import win32gui
import win32con
import win32clipboard


class StableClipboardMonitor:
    def __init__(self, output_path: str = ""):
        self.last_hash = None
        self.hwnd = None
        self.output_path = output_path or os.environ.get("CLIPFLOW_CLIPBOARD_FILE", "")

        wc = win32gui.WNDCLASS()
        wc.lpfnWndProc = self._wnd_proc
        wc.lpszClassName = "StableClipboardMonitor"
        wc.hInstance = win32gui.GetModuleHandle(None)

        try:
            class_atom = win32gui.RegisterClass(wc)
        except Exception:
            class_atom = wc.lpszClassName

        self.hwnd = win32gui.CreateWindow(
            class_atom, "ClipboardMonitor", 0, 0, 0, 0, 0, 0, 0, wc.hInstance, None
        )

        ctypes.windll.user32.AddClipboardFormatListener(self.hwnd)
        # Establish startup baseline to avoid treating existing clipboard text
        # as a "new copy" right after listener starts.
        self._init_baseline_hash()

    def _init_baseline_hash(self):
        text = self._safe_get_clipboard_text()
        if not text:
            return
        try:
            self.last_hash = hashlib.md5(
                text.encode("utf-16-le", errors="ignore")
            ).hexdigest()
        except Exception:
            self.last_hash = None

    def _wnd_proc(self, hwnd, msg, wparam, lparam):
        if msg == 0x031D:  # WM_CLIPBOARDUPDATE
            self._handle_clipboard_change()
        return win32gui.DefWindowProc(hwnd, msg, wparam, lparam)

    def _handle_clipboard_change(self):
        time.sleep(0.1)

        text = self._safe_get_clipboard_text()
        if not text:
            return

        current_hash = hashlib.md5(
            text.encode("utf-16-le", errors="ignore")
        ).hexdigest()
        # Fallback safety: if startup baseline couldn't be read, treat the first
        # observed clipboard event as baseline instead of a new copy.
        if self.last_hash is None:
            self.last_hash = current_hash
            return
        if current_hash != self.last_hash:
            self.last_hash = current_hash
            self._output_result(text)

    def _safe_get_clipboard_text(self):
        for _ in range(5):
            try:
                win32clipboard.OpenClipboard()
                if win32clipboard.IsClipboardFormatAvailable(win32con.CF_UNICODETEXT):
                    data = win32clipboard.GetClipboardData(win32con.CF_UNICODETEXT)
                    win32clipboard.CloseClipboard()
                    return data
                win32clipboard.CloseClipboard()
                return None
            except Exception:
                time.sleep(0.05)
        return None

    def _output_result(self, text):
        if not self.output_path:
            return
        payload = {"content": text.strip(), "ts": int(time.time() * 1000)}
        try:
            dir_path = os.path.dirname(self.output_path)
            if dir_path:
                os.makedirs(dir_path, exist_ok=True)
            with open(self.output_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(payload, ensure_ascii=False) + "\n")
        except Exception:
            pass

    def run(self):
        try:
            win32gui.PumpMessages()
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        if self.hwnd:
            ctypes.windll.user32.RemoveClipboardFormatListener(self.hwnd)
            try:
                win32gui.PostMessage(self.hwnd, win32con.WM_QUIT, 0, 0)
            except Exception:
                pass


if __name__ == "__main__":
    trace_file = os.environ.get("CLIPFLOW_CLIPBOARD_TRACE_FILE", "").strip()
    try:
        monitor = StableClipboardMonitor()
        monitor.run()
    except Exception:
        trace_text = traceback.format_exc()
        if trace_file:
            try:
                trace_dir = os.path.dirname(trace_file)
                if trace_dir:
                    os.makedirs(trace_dir, exist_ok=True)
                with open(trace_file, "a", encoding="utf-8") as fh:
                    fh.write(
                        "\n=== Clipboard Listener Fatal Exception ===\n"
                        + trace_text
                        + "\n"
                    )
            except Exception:
                pass
        try:
            print(trace_text, file=sys.stderr)
        except Exception:
            pass
        raise
