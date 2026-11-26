"""Anonymous entries into explicitly registered ClipFlow pages."""
import logging
import os
import sqlite3
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from PyQt6.QtCore import QEvent, QObject
from PyQt6.QtWidgets import QApplication


def backend_state_dir():
    from ..utils import DATA_DIR

    override = os.environ.get("CLIPFLOW_DATA_DIR")
    if override and not Path(override).is_absolute():
        # BackendProcessPortalController runs its child with bin/ as its cwd.
        return (Path(__file__).resolve().parents[2] / override).resolve()
    return Path(DATA_DIR)


class QtPageUsage:
    def __init__(self, state_dir):
        self.state_dir = state_dir
        self.worker = ThreadPoolExecutor(max_workers=1, thread_name_prefix="QtPageUsage")

    def enter(self, module, page):
        event = dict(event_id=str(uuid.uuid4()), client="qt", module=module, page=page,
                     occurred_at=time.time_ns() // 1_000_000)
        self.worker.submit(self._write, event)

    def _write(self, event):
        try:
            from lan_bitable_template_portal.usage_metrics import record_usage_events

            for attempt in range(2):
                try:
                    if record_usage_events(self.state_dir, [event]) != {"accepted": 1}:
                        raise OSError("Incomplete usage receipt")
                    return
                except (OSError, sqlite3.OperationalError):
                    if attempt:
                        raise
                    time.sleep(0.1)
        except Exception as exc:
            logging.warning("Qt 页面进入统计写入失败 (%s)", type(exc).__name__)

    def close(self):
        self.worker.shutdown(wait=False)


def _recorder():
    return getattr(QApplication.instance(), "_qt_page_usage", None)


def enter_page(module, page):
    recorder = _recorder()
    if recorder is not None:
        recorder.enter(module, page)


def _notice_page(window):
    if window.stack.currentWidget() is window.deleted_history_container:
        return "deleted_history"
    return "event" if window.notice_tab == "event" else "other"


def notice_window_shown(window):
    if _recorder() is not None and not window.isMinimized():
        enter_page("notices", _notice_page(window))


def navigate_notice(window, navigate, *args):
    """Called only by user navigation, after asynchronous loads have succeeded."""
    if _recorder() is None or not window.isVisible() or window.isMinimized():
        return navigate(*args)
    before = _notice_page(window)
    navigate(*args)
    after = _notice_page(window)
    if before != after:
        enter_page("notices", after)


class _WindowEntry(QObject):
    def __init__(self, window, module, page):
        super().__init__(window)
        self.module = module
        self.page = page
        self.shown = False
        window.installEventFilter(self)

    def eventFilter(self, window, event):
        if event.type() == QEvent.Type.Show and not self.shown and not window.isMinimized():
            self.shown = True
            enter_page(self.module, self.page)
        elif event.type() == QEvent.Type.Hide:
            self.shown = False
        return False


def watch_window(window, module, page):
    if _recorder() is not None:
        _WindowEntry(window, module, page)
