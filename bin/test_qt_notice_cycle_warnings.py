"""Qt cycle send guards, with temporary data and no network or uploads."""
import copy
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch


BIN = str(Path(__file__).resolve().parent)
if BIN not in sys.path:
    sys.path.insert(0, BIN)


def notice(notice_type="维保通告", cycle="每月", content="季度检查过滤器", reason="按年度计划维保"):
    return {
        "record_id": "fixture-maintenance",
        "active_item_id": "fixture-item",
        "notice_type": notice_type,
        "text": f"【{notice_type}】状态：开始\n【名称】A楼过滤器检查\n"
                f"【内容】{content}\n【原因】{reason}\n【进度】准备完成",
        "buildings": ["A楼"],
        "specialty": "暖通",
        "maintenance_cycle": cycle,
        "execution_party": "自维",
    }


class QtNoticeCycleWarningTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.tmp.cleanup)
        cls.enterClassContext(patch.dict(os.environ, {
            "QT_QPA_PLATFORM": "offscreen", "CLIPFLOW_DATA_DIR": cls.tmp.name,
        }))
        cls.enterClassContext(patch("requests.sessions.Session.request", side_effect=AssertionError("No network")))
        cls.enterClassContext(patch("urllib.request.urlopen", side_effect=AssertionError("No network")))
        cls.enterClassContext(patch("os.replace", side_effect=AssertionError("No legacy data migration")))
        cls.enterClassContext(patch("shutil.copy2", side_effect=AssertionError("No legacy data migration")))
        from PyQt6.QtWidgets import QApplication
        from upload_event_module.ui import dialogs
        from upload_event_module import logger

        trace = getattr(logger, "_crash_trace_file", None)
        if trace and Path(trace.name).resolve().is_relative_to(Path(cls.tmp.name).resolve()):
            def close_fixture_logging():
                listener = logger._logging_listener
                logger._stop_logging_listener()
                if listener:
                    for handler in listener.handlers:
                        handler.close()
                logger._close_crash_trace_stream()
            cls.addClassCleanup(close_fixture_logging)

        cls.app = QApplication.instance() or QApplication([])
        cls.dialogs = dialogs

    def make_upload(self, data=None, action="upload", theme="dark"):
        dialog = self.dialogs.ScreenshotConfirmDialog(theme=theme)
        self.addCleanup(self.dispose, dialog)
        dialog.set_data(data or notice(), action_type=action)
        dialog.time_input.setText("2026-10-22 09:30")
        return dialog

    def make_detail(self, data=None, editable=True, theme="dark"):
        dialog = self.dialogs.DetailDialog(theme=theme)
        self.addCleanup(self.dispose, dialog)
        data = data or notice()
        dialog.update_content(data, data["record_id"], editable=editable)
        return dialog

    def dispose(self, dialog):
        dialog.close()
        dialog.deleteLater()
        self.app.processEvents()

    def test_upload_lists_both_fields_without_changing_the_notice(self):
        data = notice()
        original = copy.deepcopy(data)
        dialog = self.make_upload(data)
        self.assertFalse(dialog.maintenance_cycle_warning_label.isHidden())
        warning = dialog.maintenance_cycle_warning_label.text()
        self.assertIn("【内容】", warning)
        self.assertIn("【原因】", warning)
        self.assertIn("修改一致后再发送", warning)
        self.assertEqual(dialog.data_dict["text"], original["text"])
        self.assertEqual(data, original)
        self.assertFalse(dialog.btn_confirm.isEnabled())

    def test_cycle_selection_and_clearing_refresh_the_advisory(self):
        dialog = self.make_upload(notice(reason="按季度计划维保"))
        for cycle, hidden in (("每季", True), ("每月", False), ("/", True)):
            with self.subTest(cycle=cycle):
                index = dialog.maintenance_cycle_combo.findText(cycle)
                dialog.maintenance_cycle_combo.setCurrentIndex(index)
                dialog.maintenance_cycle_combo.activated.emit(index)
                self.assertEqual(dialog.maintenance_cycle_warning_label.isHidden(), hidden)
                self.assertEqual(dialog.data_dict["maintenance_cycle"], cycle)
                self.assertEqual(dialog.btn_confirm.isEnabled(),hidden)
        dialog.maintenance_cycle_combo.setCurrentIndex(0)
        dialog.maintenance_cycle_combo.activated.emit(0)
        self.assertTrue(dialog.maintenance_cycle_warning_label.isHidden())
        self.assertEqual(dialog.maintenance_cycle_warning_label.text(), "")
        self.assertFalse(dialog.btn_confirm.isEnabled())
        self.assertIn("维保周期", dialog.btn_confirm.toolTip())

    def test_text_update_and_reopening_clear_stale_warning(self):
        dialog = self.make_upload()
        corrected = notice(content="月度检查过滤器", reason="按月度计划维保")["text"]
        dialog._update_data_dict_field("text", corrected)
        self.assertTrue(dialog.maintenance_cycle_warning_label.isHidden())
        dialog.set_data(notice(), action_type="upload")
        self.assertFalse(dialog.maintenance_cycle_warning_label.isHidden())
        dialog.set_data(notice(content="检查过滤器", reason="例行维保"))
        self.assertTrue(dialog.maintenance_cycle_warning_label.isHidden())

    def test_other_notice_types_never_call_cycle_checker(self):
        dialog = self.make_upload()
        detail = self.make_detail()
        with patch.object(self.dialogs, "maintenance_warnings_from_notice") as check:
            for kind in ("变更通告", "设备检修", "设备轮巡", "设备调整", "上下电通告", "事件通告", "维护通告", ""):
                with self.subTest(kind=kind):
                    data = notice(notice_type=kind)
                    dialog.set_data(data)
                    detail.update_content(data, data["record_id"])
                    self.assertTrue(dialog.maintenance_cycle_warning_label.isHidden())
                    self.assertTrue(detail.maintenance_cycle_warning_label.isHidden())
                    self.assertEqual(detail.text_area.styleSheet(), "")
            check.assert_not_called()

    def test_start_update_end_block_conflicts_but_preserve_original_parameters_after_correction(self):
        with patch.object(self.dialogs, "show_simple_message") as message:
            for action in ("upload", "update", "end"):
                with self.subTest(action=action):
                    data = notice()
                    dialog = self.make_upload(data, action=action)
                    dialog.screenshot_bytes = b"fixture-screenshot"
                    dialog.extra_images = [(b"fixture-site-photo", "site.png")]
                    before = copy.deepcopy(dialog.data_dict)
                    received = []
                    dialog.upload_confirmed.connect(lambda *args: received.append(args))
                    self.assertFalse(dialog.btn_confirm.isEnabled())
                    dialog.confirm_upload()
                    self.assertEqual(received, [])
                    self.assertEqual(dialog.data_dict, before)
                    dialog._update_data_dict_field('text',notice(content='每月检查过滤器',reason='按月度计划维保')['text'])
                    self.assertTrue(dialog.btn_confirm.isEnabled())
                    dialog.confirm_upload()
                    self.assertEqual(len(received), 1)
                    args = received[0]
                    self.assertEqual(args[0]["text"], notice(content='每月检查过滤器',reason='按月度计划维保')['text'])
                    self.assertEqual(args[0]["maintenance_cycle"], "每月")
                    self.assertEqual(args[0]["execution_party"], "自维")
                    self.assertEqual(args[1:4], (b"fixture-screenshot", action, "2026-10-22 09:30"))
                    self.assertEqual(args[4], ["A楼"])
                    self.assertEqual(args[5], [(b"fixture-site-photo", "site.png")])
                    self.assertEqual(args[6:], ("暖通", "", "", "", False, "auto"))
            self.assertEqual(message.call_count,3)

    def test_skip_screenshot_cannot_bypass_cycle_guard(self):
        dialog=self.make_upload()
        received=[]
        dialog.upload_confirmed.connect(lambda *args:received.append(args))
        with patch.object(self.dialogs,'show_simple_message') as message:
            dialog.skip_screenshot()
        self.assertEqual(received,[])
        message.assert_called_once()

    def test_existing_required_fields_and_screenshot_rules_are_unchanged(self):
        data = notice(content='月度检查',reason='按月度计划维保')
        data.pop("execution_party")
        dialog = self.make_upload(data, action="update")
        self.assertFalse(dialog.btn_confirm.isEnabled())
        self.assertIn("执行方", dialog.btn_confirm.toolTip())
        index = dialog.execution_party_combo.findText("自维")
        dialog.execution_party_combo.setCurrentIndex(index)
        dialog.execution_party_combo.activated.emit(index)
        self.assertTrue(dialog.btn_confirm.isEnabled())
        self.assertFalse(dialog.btn_skip.isEnabled())
        self.assertIn("必须上传通告截图", dialog.btn_skip.toolTip())

    def test_saved_cycle_wins_over_cache_without_changing_recovery(self):
        dialog = self.make_upload()
        store = Mock()
        store.get_record_fields.return_value = {"maintenance_cycle": "每季"}
        dialog.bind_cache_store(store)
        data = notice(reason="按季度计划维保")
        dialog.set_data(data)
        self.assertEqual(dialog.selected_maintenance_cycle, "每月")
        self.assertFalse(dialog.maintenance_cycle_warning_label.isHidden())
        data.pop("maintenance_cycle")
        dialog.set_data(data)
        self.assertEqual(dialog.selected_maintenance_cycle, "每季")
        self.assertTrue(dialog.maintenance_cycle_warning_label.isHidden())

    def test_detail_original_text_red_border_clears_after_correction_in_both_themes(self):
        for theme in ("dark", "light"):
            with self.subTest(theme=theme):
                dialog = self.make_detail(theme=theme)
                self.assertIn("#EF4444", dialog.text_area.styleSheet())
                self.assertFalse(dialog.maintenance_cycle_warning_label.isHidden())
                self.assertFalse(dialog.save_btn.isEnabled())
                text = notice(content="月度检查过滤器", reason="按月度计划维保")["text"]
                dialog.text_area.setPlainText(text)
                self.assertTrue(dialog.maintenance_cycle_warning_label.isHidden())
                self.assertEqual(dialog.text_area.styleSheet(), "")
                self.assertTrue(dialog.save_btn.isEnabled())
                self.assertEqual(dialog.text_area.toPlainText(), text)

    def test_detail_warning_does_not_block_edit_or_change_saved_text(self):
        dialog = self.make_detail()
        received = []
        dialog.content_changed.connect(lambda *args: received.append(args))
        text = notice(content="季度检查过滤器及阀门")["text"]
        dialog.text_area.setPlainText(text)
        self.assertTrue(dialog.save_btn.isEnabled())
        self.assertFalse(dialog.text_area.isReadOnly())
        with patch.object(self.dialogs.QTimer, "singleShot"):
            dialog.save_content()
        self.assertEqual(received, [("fixture-item", "fixture-maintenance", {"text": text})])
        self.assertFalse(dialog.maintenance_cycle_warning_label.isHidden())

    def test_readonly_detail_remains_readonly_and_supports_selection(self):
        dialog = self.make_detail(editable=False)
        self.assertTrue(dialog.text_area.isReadOnly())
        self.assertTrue(dialog.save_btn.isHidden())
        dialog.text_area.selectAll()
        self.assertTrue(dialog.text_area.textCursor().hasSelection())
        self.assertEqual(dialog.text_area.toPlainText(), notice()["text"])

    def test_same_text_new_cycle_refreshes_detail_without_marking_it_edited(self):
        data = notice(reason="季度维护")
        dialog = self.make_detail(data)
        data["maintenance_cycle"] = "每季"
        dialog.update_content(data, data["record_id"])
        self.assertTrue(dialog.maintenance_cycle_warning_label.isHidden())
        self.assertEqual(dialog.text_area.styleSheet(), "")
        self.assertFalse(dialog.save_btn.isEnabled())

    def test_normalized_real_detail_payload_retains_sections_and_cycle(self):
        from upload_event_module.ui.display_state import normalize_active_item_data

        data = normalize_active_item_data(notice())
        self.assertEqual(data["maintenance_cycle"], "每月")
        self.assertIn("【内容】", data["text"])
        self.assertIn("【原因】", data["text"])
        dialog = self.make_detail(data)
        self.assertFalse(dialog.maintenance_cycle_warning_label.isHidden())
        self.assertIn("【内容】", dialog.maintenance_cycle_warning_label.text())
        self.assertIn("【原因】", dialog.maintenance_cycle_warning_label.text())

    def test_declared_notice_type_does_not_require_a_type_heading(self):
        data = notice()
        data["text"] = data["text"].split("\n", 1)[1]
        for dialog in (self.make_upload(data), self.make_detail(data)):
            self.assertEqual(dialog.notice_type, "维保通告")
            self.assertFalse(dialog.maintenance_cycle_warning_label.isHidden())

    def test_warning_labels_render_untrusted_terms_as_plain_text(self):
        from PyQt6.QtCore import Qt

        for dialog in (self.make_upload(), self.make_detail()):
            self.assertEqual(dialog.maintenance_cycle_warning_label.textFormat(), Qt.TextFormat.PlainText)
            self.assertTrue(dialog.maintenance_cycle_warning_label.wordWrap())

    def test_shown_warning_does_not_overlap_submit_or_save_buttons(self):
        upload = self.make_upload()
        detail = self.make_detail()
        for dialog, button in ((upload, upload.btn_confirm), (detail, detail.save_btn)):
            dialog.show()
            self.app.processEvents()
            label = dialog.maintenance_cycle_warning_label
            self.assertTrue(label.isVisible())
            self.assertLess(label.geometry().bottom(), button.geometry().top())
        self.assertFalse(upload.btn_confirm.isEnabled())


if __name__ == "__main__":
    unittest.main()
