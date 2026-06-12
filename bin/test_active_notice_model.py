import os
import sys
import time
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
from upload_event_module.ui.main_window_runtime import MainWindowRuntimeMixin


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


class _RuntimeOngoingStore:
    def __init__(self, records):
        self._records = records

    def entries(self):
        return [(None, None, record) for record in self._records]


class _RuntimeCacheStore:
    def __init__(self, fields_by_record_id):
        self._fields_by_record_id = fields_by_record_id

    def get_record_fields(self, record_id="", fields=None):
        values = dict(self._fields_by_record_id.get(record_id, {}) or {})
        if not fields:
            return values
        return {key: values[key] for key in fields if key in values}


class _RuntimeOngoingHarness(MainWindowRuntimeMixin):
    def __init__(self, records, cache_fields=None):
        self._records = records
        self.cache_store = _RuntimeCacheStore(cache_fields or {})
        self._payload_store = {}
        self._payload_alias = {}

    def _active_notice_store(self):
        return _RuntimeOngoingStore(self._records)

    @staticmethod
    def _extract_section_text(text, labels):
        return MainWindowRecordsMixin._extract_section_text(text, labels)

    @staticmethod
    def _normalize_buildings_value(value):
        return MainWindowRecordsMixin._normalize_buildings_value(value)

    @classmethod
    def _infer_buildings_from_notice_text(cls, text):
        return MainWindowRecordsMixin._infer_buildings_from_notice_text(text)

    @staticmethod
    def _is_placeholder_record(_data):
        return False

    @staticmethod
    def _has_pending_upload(_record_id):
        return False

    @staticmethod
    def _is_record_binding_conflicted(_data):
        return False

    @staticmethod
    def _is_routing_conflicted(_data):
        return False

    @staticmethod
    def _record_binding_error_text(_data):
        return ""

    @staticmethod
    def _routing_error_text(_data):
        return ""


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

    def test_runtime_collects_all_non_event_notice_types_for_portal(self):
        records = [
            {
                "active_item_id": "aid-power",
                "record_id": "rid-power",
                "notice_type": "上下电通告",
                "buildings": ["A楼"],
                "text": "【上下电通告】状态：开始\n\n【名称】A楼上电\n\n【机柜】A-101\n\n【数量】1",
            },
            {
                "active_item_id": "aid-polling",
                "record_id": "rid-polling",
                "notice_type": "设备轮巡",
                "buildings": ["B楼"],
                "text": "【设备轮巡】状态：开始\n\n【名称】B楼轮巡\n\n【设备】冷机",
            },
            {
                "active_item_id": "aid-adjust",
                "record_id": "rid-adjust",
                "notice_type": "设备调整",
                "buildings": ["C楼"],
                "text": "【设备调整】状态：开始\n\n【名称】C楼调整",
            },
            {
                "active_item_id": "aid-event",
                "record_id": "rid-event",
                "notice_type": "事件通告",
                "buildings": ["D楼"],
                "text": "【事件通告】状态：开始\n\n【标题】D楼事件",
            },
        ]

        ongoing = _RuntimeOngoingHarness(records)._collect_lan_maintenance_ongoing_notices("ALL")
        by_title = {item["title"]: item for item in ongoing}

        self.assertEqual({item["work_type"] for item in ongoing}, {"power", "polling", "adjust"})
        self.assertEqual(by_title["A楼上电"]["cabinet"], "A-101")
        self.assertEqual(by_title["A楼上电"]["quantity"], "1")
        self.assertEqual(by_title["B楼轮巡"]["device"], "冷机")
        self.assertNotIn("D楼事件", by_title)

    def test_runtime_ongoing_projection_uses_cached_dialog_fields(self):
        records = [
            {
                "active_item_id": "aid-maint",
                "record_id": "rid-maint",
                "notice_type": "维保通告",
                "text": "【维保通告】状态：开始\n\n【名称】A楼维保",
            }
        ]
        cache_fields = {
            "rid-maint": {
                "buildings": ["A楼"],
                "specialty": "暖通",
                "maintenance_cycle": "每月",
            }
        }

        ongoing = _RuntimeOngoingHarness(records, cache_fields)._collect_lan_maintenance_ongoing_notices("A")

        self.assertEqual(len(ongoing), 1)
        self.assertEqual(ongoing[0]["specialty"], "暖通")
        self.assertEqual(ongoing[0]["maintenance_cycle"], "每月")
        self.assertEqual(ongoing[0]["building"], "A楼")

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

    def test_restore_button_state_matches_upload_aliases(self):
        harness = _AddItemHarness(model_view_visible=True)
        harness._closing = False
        harness.pending_action_record_ids = {"placeholder-1"}
        harness.pending_action_types = {"placeholder-1": "update"}
        harness.pending_upload_rollback_by_record_id = {}
        harness.current_screenshot_record_id = "target-1"
        harness.current_screenshot_action_type = "update"
        harness._payload_alias = {"placeholder-1": "target-1", "target-1": "target-1"}
        harness._upload_key_alias = {"target-1": "placeholder-1"}
        harness.save_active_cache = lambda: None
        harness._set_last_ui_op = lambda *args, **kwargs: None
        harness._maybe_update_detail_dialog = lambda *args, **kwargs: None

        item, _widget = harness.add_active_item(
            {
                "active_item_id": "aid-upload",
                "record_id": "target-1",
                "target_record_id": "target-1",
                "notice_type": "维保通告",
                "text": "【维保通告】状态：更新\n\n【标题】A楼维保\n\n【时间】2026-01-01",
                "_is_placeholder_record": False,
                "_has_unuploaded_changes": False,
                "_upload_in_progress": True,
                "_pending_upload_hash": "hash-1",
            },
            skip_cache=True,
        )

        harness.restore_button_state(True, "更新", "placeholder-1")

        updated = item.data(Qt.ItemDataRole.UserRole)
        self.assertFalse(updated.get("_upload_in_progress"))
        self.assertIsNone(updated.get("_pending_upload_hash"))
        self.assertNotIn("placeholder-1", harness.pending_action_record_ids)
        self.assertNotIn("placeholder-1", harness.pending_action_types)
        self.assertIsNone(harness.current_screenshot_record_id)
        self.assertIsNone(harness.current_screenshot_action_type)

    def test_recover_stale_upload_state_without_pending_queue(self):
        harness = _AddItemHarness(model_view_visible=True)
        harness.pending_action_record_ids = set()
        harness.pending_action_types = {}
        harness.pending_upload_rollback_by_record_id = {}
        harness.pending_new_by_record_id = {}
        harness.pending_update_after_upload = {}
        harness.current_screenshot_record_id = ""
        harness._payload_alias = {}
        harness._upload_key_alias = {}
        harness.save_active_cache = lambda: None
        harness.schedule_active_cache_save = lambda *_args, **_kwargs: None

        item, _widget = harness.add_active_item(
            {
                "active_item_id": "aid-stale-upload",
                "record_id": "target-stale",
                "target_record_id": "target-stale",
                "notice_type": "维保通告",
                "text": "【维保通告】状态：更新\n\n【标题】A楼维保\n\n【时间】2026-01-01",
                "_is_placeholder_record": False,
                "_has_unuploaded_changes": False,
                "_upload_in_progress": True,
                "_pending_upload_hash": "hash-stale",
                "_upload_started_monotonic": time.monotonic() - 30.0,
            },
            skip_cache=True,
        )

        result = harness._recover_stale_upload_states()

        updated = item.data(Qt.ItemDataRole.UserRole)
        self.assertEqual(result["stale_upload_recovered"], 1)
        self.assertFalse(updated.get("_upload_in_progress"))
        self.assertIsNone(updated.get("_pending_upload_hash"))
        self.assertNotIn("_upload_started_monotonic", updated)

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
