"""Anonymous Qt navigation. No full ClipFlow startup, remote service or live stores."""
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
import uuid
from contextlib import closing
from collections import Counter
from pathlib import Path
from unittest.mock import Mock, call, patch

BIN = Path(__file__).resolve().parent
sys.path.insert(0, str(BIN))

from lan_bitable_template_portal import usage_metrics
from upload_event_module.ui.page_usage import QtPageUsage


class QtPageUsageTests(unittest.TestCase):
    def test_catalog_and_delayed_beijing_day_in_temporary_database(self):
        catalog = json.loads((BIN / "lan_bitable_template_portal/usage_qt_pages.json").read_text("utf-8"))
        pages = [(module, page) for module, spec in catalog.items() for page in spec["pages"]]
        with tempfile.TemporaryDirectory() as root:
            usage = QtPageUsage(Path(root))
            # 2024-01-01 16:00 UTC is January 2 in Beijing.
            with patch("time.time_ns", return_value=1704124800000000000):
                for module, page in pages:
                    usage.enter(module, page)
            usage.worker.shutdown(wait=True)
            with closing(sqlite3.connect(Path(root) / "page_usage.sqlite3")) as db:
                rows = db.execute("SELECT module,page,day,client,count FROM daily").fetchall()
                self.assertCountEqual(rows, [(m, p, "2024-01-02", "qt", 1) for m,p in pages])
                receipts = db.execute("SELECT id FROM receipts").fetchall()
                self.assertEqual(len(receipts), len(pages))
                self.assertTrue(all(uuid.UUID(bytes=raw).version == 4 for raw, in receipts))

    def test_worker_retries_uncertain_commit_with_original_anonymous_event(self):
        entered = threading.Event()
        release = threading.Event()
        main_thread = threading.get_ident()
        calls = []
        original = usage_metrics.record_usage_events

        def write(root, events):
            calls.append((threading.get_ident(), events[0].copy()))
            entered.set()
            release.wait(3)
            result = original(root, events)
            return result if len(calls) > 1 else {"accepted": 0}

        with tempfile.TemporaryDirectory() as root, patch.object(usage_metrics, "record_usage_events", write):
            usage = QtPageUsage(Path(root))
            try:
                usage.enter("notices", "detail")
                self.assertTrue(entered.wait(3))
            finally:
                release.set()
                usage.worker.shutdown(wait=True)
            self.assertEqual(len(calls), 2)
            self.assertEqual(calls[0], calls[1])
            thread, event = calls[0]
            self.assertNotEqual(thread, main_thread)
            self.assertEqual(set(event), {"event_id", "client", "module", "page", "occurred_at"})
            self.assertIsInstance(event["occurred_at"], int)
            with closing(sqlite3.connect(Path(root) / "page_usage.sqlite3")) as db:
                self.assertEqual(db.execute("SELECT count FROM daily").fetchall(), [(1,)])

    def test_real_qt_navigation_in_isolated_process(self):
        with tempfile.TemporaryDirectory() as root:
            env = dict(os.environ, CLIPFLOW_DATA_DIR=root, LOCALAPPDATA=root, QT_QPA_PLATFORM="offscreen")
            subprocess.run([sys.executable, __file__, "--navigation"], cwd=root, env=env, check=True, timeout=45)


def exercise_navigation():
    # Config imports legacy files from the binary directory. Use an empty portable root.
    with patch.object(sys, "frozen", True, create=True), patch.object(sys, "executable", str(Path.cwd() / "python.exe")):
        from upload_event_module import utils
    from PyQt6.QtCore import QTimer
    from PyQt6.QtWidgets import QApplication, QListWidgetItem, QWidget
    from lan_bitable_template_portal.state_store import LanPortalStateStore
    from upload_event_module.ui.main_window_ui import MainWindowUiMixin
    from upload_event_module.ui.main_window_records import MainWindowRecordsMixin
    from upload_event_module.ui.main_window_patch import PatchUpdateMixin
    from upload_event_module.ui.main_window_workflow import MainWindowWorkflowMixin
    from upload_event_module.ui.page_usage import backend_state_dir
    from upload_event_module.ui.dialogs import (
        AddDialog, DetailDialog, ScreenshotConfirmDialog, ScreenshotConfirmOverlay, ScreenshotRegionSelector,
    )
    from upload_event_module.ui.settings_dialog import SettingsDialog

    assert utils.BASE_DIR == str(Path.cwd())
    assert backend_state_dir() == LanPortalStateStore().db_path.parent == Path.cwd()
    with patch.dict(os.environ, CLIPFLOW_DATA_DIR="relative-state"):
        assert backend_state_dir() == BIN / "relative-state"

    app = QApplication([])
    usage = QtPageUsage(backend_state_dir())
    app._qt_page_usage = Mock(wraps=usage)
    check = unittest.TestCase()

    class Window(MainWindowUiMixin, MainWindowWorkflowMixin, QWidget):
        _is_active_view = MainWindowRecordsMixin._is_active_view
        _disable_effects = True
        current_theme = "dark"
        _build_display_version = lambda self: "fixture"
        show_remote_update_error = apply_patch_update = check_remote_update_now = Mock()
        _init_update_overlay = _init_active_model_view = apply_theme = Mock()
        refresh_lan_template_portal_link = refresh_table_links = _refresh_patch_button = Mock()
        _flush_pending_cache_refresh = _open_lan_template_portal = show_message = Mock()
        _list_deleted_notice_undos_from_backend = Mock(return_value=(True, "", []))

        def _enqueue_ui_mutation(self, name, callback):
            self.finish_history = callback
            loaded.set()

    loaded = threading.Event()
    w = Window()
    w.init_ui()
    enter = app._qt_page_usage.enter
    check.assertEqual(enter.call_count, 0)
    w.show()
    w.show()
    w.event_tab_btn.click()
    w.other_tab_btn.click()
    w.other_tab_btn.click()
    w._focus_event_tab()  # Incoming event auto-focuses the event tab.
    w.other_tab_btn.click()
    w.deleted_history_btn.click()
    check.assertTrue(loaded.wait(3))
    w.finish_history()
    w._render_deleted_notice_history([])  # Refresh while still on history.
    w.event_tab_btn.click()  # Cross both stacks in one navigation.
    w.hide()
    w.other_tab_btn.click()
    w.show()
    loaded.clear()
    w.deleted_history_btn.click()
    # Resolve this user request after the window goes to the tray.
    w.hide()
    check.assertTrue(loaded.wait(3))
    w.finish_history()
    w.show()
    w.deleted_history_back_btn.click()
    loaded.clear()
    w._list_deleted_notice_undos_from_backend.return_value = (False, "fixture", [])
    w.deleted_history_btn.click()
    check.assertTrue(loaded.wait(3))
    w.finish_history()  # A failed navigation leaves the page count unchanged.
    check.assertEqual(enter.call_args_list, [
        call("notices", page) for page in
        ("event", "other", "other", "deleted_history", "event", "other", "deleted_history", "other")
    ])
    enter.reset_mock()
    dialog = AddDialog(w)
    dialog.show()
    dialog.text_input.setPlainText("仅供测试的内容")
    dialog.show()
    dialog.update()
    app.processEvents()
    dialog.hide()
    dialog.show()
    check.assertEqual(enter.call_args_list, [call("notices", "manual_add")] * 2)
    dialog.hide()
    settings = SettingsDialog()
    settings.show()
    settings.app_id_input.setText("fixture account")
    settings.hide()
    detail = DetailDialog()
    detail.show()
    detail.update()
    detail.hide()
    def dismiss_modal():
        QTimer.singleShot(0, lambda: app.activeModalWidget().reject())

    QTimer.singleShot(0, lambda: app.activeModalWidget().accept())
    candidate = {"record_id": "fixture"}
    check.assertEqual(w._prompt_target_record_candidate({"target_candidates": [candidate]}), candidate)
    w._find_active_item_by_record_id = Mock(return_value=(None, QListWidgetItem()))
    w._is_valid_list_item = w._is_placeholder_record = Mock(return_value=True)
    dismiss_modal()
    MainWindowRecordsMixin._request_safe_bind_record_id(w, "fixture")
    w._remote_update_diagnostic = {"title": "fixture", "detail": "fixture"}
    dismiss_modal()
    PatchUpdateMixin.show_remote_update_error(w)
    upload = ScreenshotConfirmDialog()
    upload.set_data({"notice_type": "事件通告", "record_id": "fixture", "text": "fixture"})
    upload.show()
    upload._open_datetime_picker()
    upload.datetime_popup.hide()
    dismiss_modal()
    upload._prompt_response_time_choice(["01:02", "03:04"])
    upload.hide()
    selector = ScreenshotRegionSelector()
    overlay = ScreenshotConfirmOverlay((0, 0, 8, 8), selector.geometry())
    overlay.show()
    overlay.hide()
    selector.hide()
    w.hide()
    usage.worker.shutdown(wait=True)
    with closing(sqlite3.connect(Path.cwd() / "page_usage.sqlite3")) as db:
        counts = dict(db.execute("SELECT page,count FROM daily WHERE client='qt'"))
        check.assertEqual(counts, Counter(
            event=2, other=4, deleted_history=2, manual_add=2, preferences=1, detail=1,
            select_record=1, bind_record=1, update_diagnostic=1, upload=1, datetime_picker=1,
            response_time=1, select_region=1, confirm_region=1,
        ))


if __name__ == "__main__":
    if sys.argv[1:] == ["--navigation"]:
        with patch("socket.socket.connect", side_effect=AssertionError("Network forbidden")):
            exercise_navigation()
    else:
        unittest.main()
