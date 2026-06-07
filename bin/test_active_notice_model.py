import os
import sys
import unittest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PyQt6.QtCore import QEvent, QPointF, QRect, Qt
from PyQt6.QtGui import QMouseEvent
from PyQt6.QtWidgets import QApplication, QListWidget, QStyleOptionViewItem


current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

from upload_event_module.ui.active_notice_model import ActiveNoticeListRoute, ActiveNoticeModel
from upload_event_module.ui.active_notice_delegate import ActiveNoticeDelegate
from upload_event_module.ui.main_window_records import MainWindowRecordsMixin


class _RecordsFlagHarness(MainWindowRecordsMixin):
    def __init__(self, model_view_visible=False):
        self._model_view_visible = model_view_visible

    def _active_model_view_visible(self):
        return self._model_view_visible


class _AddItemHarness(MainWindowRecordsMixin):
    def __init__(self, model_view_visible=True):
        self._model_view_visible = model_view_visible
        if model_view_visible:
            self.list_active_event = ActiveNoticeListRoute("event")
            self.list_active_other = ActiveNoticeListRoute("other")
        else:
            self.list_active_event = QListWidget()
            self.list_active_other = QListWidget()
        self._delete_interaction_enabled = True

    def _active_model_view_visible(self):
        return self._model_view_visible

    def _ensure_active_item_identity(self, data):
        data = dict(data or {})
        data.setdefault("active_item_id", "aid-test")
        data.setdefault("record_id", "rid-test")
        return data

    def _ensure_payload_for_data(self, data):
        return data

    def _schedule_today_in_progress_sync(self, data):
        return None

    def _schedule_record_binding_validation(self, data):
        return None

    def _schedule_active_route_reconcile(self, data=None, **_kwargs):
        return None


class _TodayProgressController:
    def __init__(self):
        self.calls = []

    def submit_qt_command(self, command, payload):
        self.calls.append((command, dict(payload or {})))
        return {"ok": True, "message": ""}


class _TodayProgressHarness(_AddItemHarness):
    def __init__(self):
        super().__init__(model_view_visible=True)
        self.cache_store = None
        self.lan_template_portal_controller = _TodayProgressController()
        self._today_in_progress_pending_record_ids = set()
        self._today_in_progress_synced_record_ids = set()
        self.messages = []

    def _enqueue_ui_mutation(self, _label, func):
        func()

    def _build_clipboard_entry(self, raw_text):
        return {"content": raw_text}

    def _ensure_payload_for_data(self, data, entry=None):
        return dict(entry or {"content": data.get("text", "")})

    def _maybe_update_detail_dialog(self, *args, **kwargs):
        return None

    def show_message(self, message):
        self.messages.append(str(message))


class ActiveNoticeModelTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._app = QApplication.instance() or QApplication([])

    def test_replace_and_roles(self):
        model = ActiveNoticeModel()
        model.replace_records(
            [
                {
                    "active_item_id": "aid-1",
                    "record_id": "rid-1",
                    "notice_type": "维保通告",
                    "text": "【维保通告】状态：开始\n\n【标题】A楼维保",
                    "lan_created_from_portal": True,
                    "_has_unuploaded_changes": False,
                }
            ]
        )

        index = model.index(0, 0)

        self.assertEqual(model.rowCount(), 1)
        self.assertEqual(index.data(ActiveNoticeModel.RecordIdRole), "rid-1")
        self.assertEqual(index.data(ActiveNoticeModel.OriginRole), "portal")
        self.assertTrue(index.data(ActiveNoticeModel.UploadedRole))
        self.assertIn("A楼维保", index.data(Qt.ItemDataRole.DisplayRole))

    def test_roles_and_lookup_use_target_record_id(self):
        model = ActiveNoticeModel()
        model.replace_records(
            [
                {
                    "record_id": "src-old",
                    "source_record_id": "src-old",
                    "target_record_id": "target-real",
                    "notice_type": "设备变更",
                    "text": "【设备变更】状态：更新\n\n【名称】A楼变更",
                    "_is_placeholder_record": False,
                }
            ]
        )

        index = model.index(0, 0)

        self.assertEqual(index.data(ActiveNoticeModel.RecordIdRole), "target-real")
        self.assertEqual(model.row_for_record_id("target-real"), 0)
        self.assertEqual(model.row_for_record_id("src-old"), -1)
        self.assertEqual(model.row_for_source_record_id("src-old"), 0)

    def test_upsert_move_and_remove(self):
        model = ActiveNoticeModel()
        first = {"active_item_id": "aid-1", "text": "one"}
        second = {"active_item_id": "aid-2", "text": "two"}

        self.assertTrue(model.upsert_record(first))
        self.assertTrue(model.upsert_record(second))
        self.assertTrue(model.upsert_record({"active_item_id": "aid-1", "text": "one-new"}))
        self.assertEqual(model.rowCount(), 2)
        self.assertEqual(model.record_at(0)["text"], "one-new")

        identity = ActiveNoticeModel.identity_for_record({"active_item_id": "aid-2"})
        self.assertTrue(model.move_record(identity, 0))
        self.assertEqual(model.record_at(0)["active_item_id"], "aid-2")
        self.assertTrue(model.remove_record(second))
        self.assertEqual(model.rowCount(), 1)
        self.assertEqual(model.record_at(0)["active_item_id"], "aid-1")

    def test_action_state_helpers_match_legacy_widget_rules(self):
        placeholder = {
            "active_item_id": "aid-1",
            "record_id": "local-1",
            "_is_placeholder_record": True,
            "_has_unuploaded_changes": True,
            "text": "【维保通告】状态：开始\n\n【标题】待上传",
        }
        uploaded = dict(placeholder, _has_unuploaded_changes=False)
        update = dict(
            placeholder,
            record_id="real-1",
            _is_placeholder_record=False,
            _has_unuploaded_changes=True,
        )
        end = dict(update, text="【维保通告】状态：结束\n\n【标题】结束\n\n【时间】2026-01-01")

        self.assertEqual(ActiveNoticeModel.action_for_record(placeholder), "upload")
        self.assertEqual(ActiveNoticeModel.action_label_for_record(placeholder), "上传")
        self.assertEqual(ActiveNoticeModel.action_for_record(update), "update")
        self.assertEqual(ActiveNoticeModel.action_label_for_record(update), "更新")
        self.assertEqual(ActiveNoticeModel.action_for_record(end), "end")
        self.assertEqual(ActiveNoticeModel.action_label_for_record(end), "结束")
        self.assertEqual(ActiveNoticeModel.action_for_record(uploaded), "")
        self.assertEqual(ActiveNoticeModel.action_label_for_record(uploaded), "已上传")

    def test_today_progress_helpers(self):
        change = {
            "notice_type": "设备变更",
            "record_id": "rid-1",
            "_is_placeholder_record": False,
            "today_in_progress_state": "yes",
        }
        maintenance = dict(change, notice_type="维保通告")

        self.assertTrue(ActiveNoticeModel.supports_today_progress(change))
        self.assertFalse(ActiveNoticeModel.supports_today_progress(maintenance))
        self.assertEqual(ActiveNoticeModel.today_progress_label(change), "在进行")
        self.assertEqual(ActiveNoticeModel.next_today_progress_state(change), "no")

    def test_today_progress_supports_target_record_id(self):
        change = {
            "notice_type": "设备变更",
            "record_id": "",
            "target_record_id": "target-1",
            "_is_placeholder_record": False,
            "today_in_progress_state": "unknown",
        }

        self.assertTrue(ActiveNoticeModel.supports_today_progress(change))

    def test_model_view_mode_disables_widget_virtualization(self):
        self.assertFalse(_RecordsFlagHarness(True)._active_item_widgets_required())
        self.assertFalse(_RecordsFlagHarness(True)._active_list_virtualization_enabled())
        self.assertFalse(_RecordsFlagHarness(False)._active_item_widgets_required())

    def test_delegate_emits_action_today_and_delete_signals(self):
        model = ActiveNoticeModel()
        record = {
            "active_item_id": "aid-1",
            "record_id": "rid-1",
            "notice_type": "设备变更",
            "_is_placeholder_record": False,
            "_has_unuploaded_changes": True,
            "today_in_progress_state": "yes",
            "text": "【设备变更】状态：开始\n\n【标题】A楼变更\n\n【时间】2026-01-01",
        }
        model.replace_records([record])
        index = model.index(0, 0)
        option = QStyleOptionViewItem()
        option.rect = QRect(0, 0, 420, 88)
        delegate = ActiveNoticeDelegate()
        buttons = ActiveNoticeDelegate._button_rects(option, index)
        emitted = []
        model.actionRequested.connect(lambda data, action: emitted.append(("action", action)))
        model.todayProgressRequested.connect(lambda data, state: emitted.append(("today", state)))
        model.deleteRequested.connect(lambda data: emitted.append(("delete", data.get("record_id"))))

        def click_button(name):
            center = buttons[name].center()
            event = QMouseEvent(
                QEvent.Type.MouseButtonRelease,
                QPointF(center),
                Qt.MouseButton.LeftButton,
                Qt.MouseButton.LeftButton,
                Qt.KeyboardModifier.NoModifier,
            )
            self.assertTrue(delegate.editorEvent(event, model, option, index))

        click_button("action")
        click_button("today")
        click_button("delete")
        click_button("delete")

        self.assertIn(("action", "update"), emitted)
        self.assertIn(("today", "no"), emitted)
        self.assertIn(("delete", "rid-1"), emitted)

    def test_model_view_add_active_item_uses_lightweight_item_only(self):
        harness = _AddItemHarness(model_view_visible=True)
        item, widget = harness.add_active_item(
            {
                "notice_type": "维保通告",
                "text": "【维保通告】状态：开始\n\n【标题】A楼维保\n\n【时间】2026-01-01",
                "_is_placeholder_record": True,
                "_has_unuploaded_changes": True,
            },
            skip_cache=True,
        )

        self.assertIsNotNone(item)
        self.assertIsNone(widget)
        self.assertIsInstance(harness.list_active_other, ActiveNoticeListRoute)
        self.assertEqual(harness.list_active_other.count(), 0)
        model = harness._active_notice_model_for_list(harness.list_active_other)
        self.assertEqual(model.rowCount(), 1)
        list_widget, found = harness._find_active_item_by_record_id("rid-test")
        self.assertIs(list_widget, harness.list_active_other)
        self.assertTrue(harness._is_valid_list_item(found))
        self.assertEqual(found.data(Qt.ItemDataRole.UserRole)["active_item_id"], "aid-test")

    def test_today_progress_toggle_updates_model_and_uses_target_record_id(self):
        harness = _TodayProgressHarness()
        item, _widget = harness.add_active_item(
            {
                "active_item_id": "aid-change",
                "record_id": "source-1",
                "target_record_id": "target-1",
                "notice_type": "设备变更",
                "today_in_progress_state": "unknown",
                "_is_placeholder_record": False,
                "_has_unuploaded_changes": False,
                "text": "【设备变更】状态：开始\n\n【标题】A楼变更\n\n【时间】2026-01-01",
            },
            skip_cache=True,
        )

        harness._handle_today_in_progress_toggle(
            item.data(Qt.ItemDataRole.UserRole),
            "yes",
        )

        model = harness._active_notice_model_for_list(harness.list_active_other)
        updated = model.record_by_active_item_id("aid-change")
        self.assertEqual(updated["today_in_progress_state"], "yes")
        self.assertEqual(
            harness.lan_template_portal_controller.calls[0][1]["record_id"],
            "target-1",
        )


if __name__ == "__main__":
    unittest.main()
