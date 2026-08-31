import os
import sys
import time
import unittest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PyQt6.QtCore import QEvent, QPointF, QRect, Qt
from PyQt6.QtGui import QMouseEvent
from PyQt6.QtWidgets import QApplication, QListWidget, QListWidgetItem, QStyleOptionViewItem


current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

from upload_event_module.ui.active_notice_model import (
    ActiveNoticeListRoute,
    ActiveNoticeModel,
    ActiveNoticeModelItem,
)
from upload_event_module.ui.active_notice_delegate import ActiveNoticeDelegate
from upload_event_module.ui.display_state import (
    build_notice_display_snapshot,
    persistent_active_item_data,
)
from upload_event_module.ui.main_window_cache import ActiveCacheMixin
from upload_event_module.ui.main_window_records import MainWindowRecordsMixin
from upload_event_module.ui.main_window_runtime import MainWindowRuntimeMixin
from upload_event_module.ui.main_window_workflow import MainWindowWorkflowMixin


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
        data.setdefault("target_record_id", "rid-test")
        data.setdefault("record_id", data["target_record_id"])
        return data

    def _ensure_payload_for_data(self, data, *_args, **_kwargs):
        return data

    @staticmethod
    def _build_clipboard_entry(text):
        return {"content": str(text or "")}

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


class _RenameCacheStore:
    def __init__(self):
        self.renamed = []

    def rename_record_id(self, old_id, new_id):
        self.renamed.append((old_id, new_id))


class _CommitCacheStore:
    def __init__(self):
        self.upserts = []

    def upsert_record(self, data):
        self.upserts.append(dict(data or {}))
        return True


class _ReplaceRecordIdHarness(MainWindowWorkflowMixin, MainWindowRecordsMixin):
    def __init__(self):
        self.list_active_event = QListWidget()
        self.list_active_other = QListWidget()
        self.pending_replace_by_record_id = {"placeholder-1": {"x": 1}}
        self.pending_upload_rollback_by_record_id = {}
        self.pending_end_rollback_by_record_id = {}
        self.pending_new_by_record_id = {}
        self.pending_update_after_upload = {}
        self.pending_action_types = {}
        self.pending_action_record_ids = {"placeholder-1"}
        self._today_in_progress_pending_record_ids = set()
        self._today_in_progress_synced_record_ids = set()
        self._record_binding_validation_pending_ids = set()
        self._record_binding_validated_ids = set()
        self._lan_portal_jobs_by_record_id = {}
        self._payload_alias = {}
        self._payload_store = {}
        self._pending_force_uploads = [{"record_id": "placeholder-1"}]
        self.current_screenshot_record_id = "placeholder-1"
        self.cache_store = _RenameCacheStore()
        self.detail_dialog = None

    def _active_model_view_visible(self):
        return False

    def _upsert_active_notice_model_item(self, *_args, **_kwargs):
        return None

    def _rebuild_active_item_widget(self, *_args, **_kwargs):
        return None

    def _should_defer_ui_refresh(self):
        return False

    def _mark_cache_refresh_needed(self):
        return None


class _RuntimeOngoingStore:
    def __init__(self, records):
        self._records = records

    def entries(self):
        return [(None, None, record) for record in self._records]


class _RuntimeCacheStore:
    def __init__(self, fields_by_record_id):
        self._fields_by_record_id = fields_by_record_id

    def get_record_fields(self, record_id="", active_item_id="", fields=None):
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


class _QtDeleteOperationHarness(MainWindowRuntimeMixin):
    def __init__(self):
        self.item = QListWidgetItem()
        self.item.setData(
            Qt.ItemDataRole.UserRole,
            {
                "active_item_id": "active-delete-operation",
                "record_id": "rec-delete-operation",
                "target_record_id": "rec-delete-operation",
                "operation_id": "upload-operation-must-not-be-reused",
                "notice_type": "维保通告",
                "work_type": "maintenance",
                "buildings": ["A"],
                "text": "【维保通告】状态：开始\n【名称】A楼删除幂等测试",
            },
        )
        self.lan_template_portal_controller = _TodayProgressController()
        self.command_results = [
            {"ok": False, "message": "temporary failure"},
            {"ok": True, "message": "deleted"},
        ]
        self.submitted_payloads = []
        self.removed = False
        self.cache_save_requests = 0
        self._today_in_progress_pending_record_ids = set()
        self._today_in_progress_synced_record_ids = set()
        self.pending_new_by_record_id = {}
        self.pending_replace_by_record_id = {}
        self.pending_update_after_upload = {}
        self.pending_action_record_ids = set()
        self.pending_action_types = {}

    @staticmethod
    def _is_screenshot_dialog_active():
        return False

    @staticmethod
    def _recover_stale_upload_states():
        return {}

    def _find_lan_ongoing_item_for_payload(self, _payload):
        return None, self.item

    @staticmethod
    def _is_valid_list_item(item):
        return item is not None

    @staticmethod
    def _normalize_buildings_value(value):
        return list(value or [])

    @staticmethod
    def _infer_buildings_from_notice_text(_text):
        return []

    @staticmethod
    def _lan_scope_matches(_scope, _buildings):
        return True

    @staticmethod
    def _has_pending_upload(_record_id):
        return False

    def _submit_qt_command(self, command, payload, *, timeout=120.0):
        self.submitted_payloads.append(
            {"command": command, "payload": dict(payload), "timeout": timeout}
        )
        return self.command_results.pop(0)

    @staticmethod
    def _clear_upload_queue(_record_id):
        return None

    def _remove_active_item_widget_only(self, _list_widget, _item):
        self.removed = True

    def request_active_cache_save(self, *_args, **_kwargs):
        self.cache_save_requests += 1


class _ActiveCacheScheduleHarness(ActiveCacheMixin):
    def __init__(self):
        self._is_restoring_cache = False
        self._active_cache_dirty = False
        self._active_cache_full_replace_enabled = False
        self._defer_active_cache_save_count = 0
        self.save_calls = 0

    def save_active_cache(self):
        self.save_calls += 1


class _RuntimeActiveUpsertHarness(MainWindowRuntimeMixin, MainWindowRecordsMixin):
    def __init__(self, record):
        self.list_active_event = ActiveNoticeListRoute("event")
        self.list_active_other = ActiveNoticeListRoute("other")
        self._active_notice_event_model = ActiveNoticeModel()
        self._active_notice_other_model = ActiveNoticeModel()
        self._active_notice_event_model.replace_records([dict(record)])
        self.cache_store = None
        self.detail_dialog = None

    @staticmethod
    def _active_model_view_visible():
        return True

    @staticmethod
    def _maybe_update_detail_dialog(*_args, **_kwargs):
        return None


class ActiveNoticeRouteIdentityTests(unittest.TestCase):
    def setUp(self):
        self.harness = _RecordsFlagHarness()

    def _match_key(self, text):
        _, key = self.harness._build_match_identity(text=text)
        return key

    def test_repair_same_title_different_faults_do_not_conflict(self):
        first = (
            "【设备检修】状态：开始\n"
            "【标题】EA118_C01机房C楼C-345-HVDC-112、C-345-HVDC-2通信中断检修\n"
            "【地点】C-345配电室\n"
            "【发现故障时间】2026-06-18 09:30\n"
            "【期望完成时间】2026-06-18 18:30\n"
            "【维修设备】C-345-HVDC-112\n"
            "【维修故障】C-345-HVDC-112通讯中断\n"
            "【故障现象】C-345-HVDC-112通讯中断\n"
            "【故障原因】AU采集器串口故障\n"
            "【完成情况】准备工作已完成"
        )
        second = (
            "【设备检修】状态：开始\n"
            "【标题】EA118_C01机房C楼C-345-HVDC-112、C-345-HVDC-2通信中断检修\n"
            "【地点】C-345配电室\n"
            "【发现故障时间】2026-06-18 09:30\n"
            "【期望完成时间】2026-06-18 18:30\n"
            "【维修设备】C-345-HVDC-2\n"
            "【维修故障】C-345-HVDC-2通讯中断\n"
            "【故障现象】C-345-HVDC-2通讯中断\n"
            "【故障原因】AU采集器串口故障\n"
            "【完成情况】准备工作已完成"
        )

        self.assertNotEqual(self._match_key(first), self._match_key(second))

    def test_repair_progress_only_change_keeps_same_route(self):
        base = (
            "【设备检修】状态：开始\n"
            "【标题】EA118_C01机房C楼C-345-HVDC-112通信中断检修\n"
            "【地点】C-345配电室\n"
            "【发现故障时间】2026-06-18 09:30\n"
            "【期望完成时间】2026-06-18 18:30\n"
            "【维修设备】C-345-HVDC-112\n"
            "【维修故障】C-345-HVDC-112通讯中断\n"
            "【故障现象】C-345-HVDC-112通讯中断\n"
            "【故障原因】AU采集器串口故障\n"
            "【完成情况】准备工作已完成"
        )
        updated = base.replace("准备工作已完成", "现场正在更换串口")

        self.assertEqual(self._match_key(base), self._match_key(updated))

    def test_maintenance_reason_or_time_change_is_distinct(self):
        base = (
            "【维保通告】状态：开始\n"
            "【名称】EA118机房C楼交直流列头柜及PDU维护\n"
            "【时间】2026-06-18 09:30~2026-06-18 18:30\n"
            "【位置】C楼\n"
            "【内容】按计划对C栋直流列头柜及PDU季度维护\n"
            "【原因】按计划对C栋直流列头柜及PDU季度维护，保证供电正常\n"
            "【进度】准备工作已完成"
        )
        monthly = base.replace("季度维护", "月度维护")
        shifted = base.replace("09:30", "10:30")
        progress = base.replace("准备工作已完成", "人员已到场")

        self.assertNotEqual(self._match_key(base), self._match_key(monthly))
        self.assertNotEqual(self._match_key(base), self._match_key(shifted))
        self.assertEqual(self._match_key(base), self._match_key(progress))


class ActiveNoticeModelTests(unittest.TestCase):
    def test_textless_bound_event_uses_structured_fields_for_display(self):
        normalized = persistent_active_item_data(
            {
                "active_item_id": "target-event-rec-display",
                "target_record_id": "rec-event-display",
                "record_id": "rec-event-display",
                "notice_type": "事件通告",
                "work_type": "event",
                "status": "开始",
                "title": "A楼空调压差过大告警",
                "event_source": "巡检发现",
                "start_time": "2026-08-11T11:05",
                "content": "巡检发现A楼空调压差过大",
                "text": "",
                "_is_placeholder_record": False,
            }
        )
        snapshot = build_notice_display_snapshot(normalized)

        self.assertIn("【事件通告】状态：更新", normalized["text"])
        self.assertIn("【标题】A楼空调压差过大告警", normalized["text"])
        self.assertIn("【时间】2026-08-11 11:05", normalized["text"])
        self.assertNotIn("rec-event-display", normalized["text"])
        self.assertEqual(snapshot["title"], "A楼空调压差过大告警")

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
                    "target_record_id": "rid-1",
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
                    "notice_type": "变更通告",
                    "text": "【变更通告】状态：更新\n\n【名称】A楼变更",
                    "_is_placeholder_record": False,
                }
            ]
        )

        index = model.index(0, 0)

        self.assertEqual(index.data(ActiveNoticeModel.RecordIdRole), "target-real")
        self.assertEqual(model.row_for_record_id("target-real"), 0)
        self.assertEqual(model.row_for_record_id("src-old"), -1)
        self.assertEqual(model.row_for_source_record_id("src-old"), 0)

    def test_lookup_indexes_update_after_target_and_source_change(self):
        model = ActiveNoticeModel()
        model.replace_records(
            [
                {
                    "active_item_id": "aid-1",
                    "source_record_id": "source-old",
                    "target_record_id": "target-old",
                    "record_id": "target-old",
                    "notice_type": "变更通告",
                    "text": "【变更通告】状态：更新\n\n【名称】A楼变更",
                }
            ]
        )

        self.assertEqual(model.row_for_active_item_id("aid-1"), 0)
        self.assertEqual(model.row_for_source_record_id("source-old"), 0)
        self.assertEqual(model.row_for_record_id("target-old"), 0)

        model.upsert_record(
            {
                "active_item_id": "aid-1",
                "source_record_id": "source-new",
                "target_record_id": "target-new",
                "record_id": "target-new",
                "notice_type": "变更通告",
                "text": "【变更通告】状态：更新\n\n【名称】A楼变更",
            }
        )

        self.assertEqual(model.row_for_active_item_id("aid-1"), 0)
        self.assertEqual(model.row_for_source_record_id("source-new"), 0)
        self.assertEqual(model.row_for_record_id("target-new"), 0)
        self.assertEqual(model.row_for_source_record_id("source-old"), -1)
        self.assertEqual(model.row_for_record_id("target-old"), -1)

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

    def test_model_item_identity_repair_replaces_row_without_duplicate(self):
        model = ActiveNoticeModel()
        route = ActiveNoticeListRoute("event")
        original = {
            "active_item_id": "local-event-id",
            "target_record_id": "rec-event-id",
            "record_id": "rec-event-id",
            "notice_type": "事件通告",
            "text": "【事件通告】状态：新增\n【概述】旧进展",
        }
        model.replace_records([original])
        item = ActiveNoticeModelItem(
            route,
            model,
            ActiveNoticeModel.identity_for_record(original),
        )

        self.assertTrue(
            item.setData(
                Qt.ItemDataRole.UserRole,
                {
                    **original,
                    "active_item_id": "rec-event-id",
                    "text": "【事件通告】状态：更新\n【概述】最新进展",
                },
            )
        )
        self.assertEqual(model.rowCount(), 1)
        self.assertEqual(model.record_at(0)["active_item_id"], "rec-event-id")
        self.assertIn("最新进展", model.record_at(0)["text"])

    def test_runtime_upsert_repairs_target_id_churn_in_place(self):
        initial = {
            "active_item_id": "stable-event-ui-id",
            "target_record_id": "recqXV62LUkuK",
            "record_id": "recqXV62LUkuK",
            "notice_type": "事件通告",
            "work_type": "event",
            "text": (
                "【事件通告】状态：新增\n"
                "【标题】EA118机房C楼I3级事件通报\n"
                "【来源】BMS发现\n"
                "【时间】2026-08-11 13:24分\n"
                "【概述】BMS发现C楼311空调间漏水告警"
            ),
        }
        harness = _RuntimeActiveUpsertHarness(initial)

        result = harness._apply_backend_active_upsert(
            {
                "item": {
                    "active_item_id": "recqXV62LUkuK",
                    "record_id": "recqXV62LUkuK",
                    "origin": "clipboard",
                    "payload": {
                        **initial,
                        "active_item_id": "recqXV62LUkuK",
                        "status": "更新",
                        "text": initial["text"].replace(
                            "状态：新增",
                            "状态：更新",
                        )
                        + "\n【进展】2、现场正在处理积水中",
                    },
                }
            }
        )

        records = harness._active_notice_event_model.records()
        self.assertTrue(result["ok"])
        self.assertTrue(result["updated"])
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["active_item_id"], "stable-event-ui-id")
        self.assertIn("状态：更新", records[0]["text"])
        self.assertIn("现场正在处理积水中", records[0]["text"])

    def test_stale_delete_cannot_remove_recreated_event_with_new_target(self):
        current = {
            "active_item_id": "stable-recreated-event",
            "record_id": "rec-new-event-target",
            "target_record_id": "rec-new-event-target",
            "notice_type": "事件通告",
            "work_type": "event",
            "_is_placeholder_record": False,
            "text": "【事件通告】状态：新增\n【标题】E楼重新新增事件",
        }
        canonical_row = {
            "active_item_id": current["active_item_id"],
            "record_id": current["record_id"],
            "notice_type": current["notice_type"],
            "payload": dict(current),
        }

        class StateStore:
            @staticmethod
            def list_visible_qt_active_items():
                return [canonical_row]

        harness = _RuntimeActiveUpsertHarness(current)
        harness.cache_store = type(
            "CacheStore", (), {"_state_store": StateStore()}
        )()

        result = harness._apply_backend_active_delete(
            {
                "active_item_id": current["active_item_id"],
                "record_id": "rec-deleted-old-target",
                "target_record_id": "rec-deleted-old-target",
                "notice_type": "事件通告",
                "work_type": "event",
                "source": "remote_delete_finalize",
            }
        )

        records = harness._active_notice_event_model.records()
        self.assertTrue(result["stale"])
        self.assertFalse(result["deleted"])
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["target_record_id"], "rec-new-event-target")

    def test_runtime_upsert_routes_new_event_text_to_serial_queue_while_uploading(self):
        old_text = (
            "【事件通告】状态：更新\n【标题】EA118机房A楼I3级事件通报\n"
            "【来源】BMS\n【时间】2026-08-21 10:00\n【概述】旧上传内容"
        )
        new_text = old_text.replace("状态：更新", "状态：结束").replace(
            "旧上传内容",
            "新复制的结束内容",
        )
        harness = _RuntimeActiveUpsertHarness(
            {
                "active_item_id": "aid-runtime-serial",
                "record_id": "rec-runtime-serial",
                "target_record_id": "rec-runtime-serial",
                "notice_type": "事件通告",
                "_is_placeholder_record": False,
                "_has_unuploaded_changes": False,
                "_upload_in_progress": True,
                "_upload_operation_id": "qt_notice:runtime-old",
                "text": old_text,
            }
        )
        harness.pending_action_record_ids = {"rec-runtime-serial"}
        captured = {}

        def queue_content(record_id, content, status, *, active_item_id=""):
            captured.update(
                {
                    "record_id": record_id,
                    "content": content,
                    "status": status,
                    "active_item_id": active_item_id,
                }
            )
            return True

        harness._queue_pending_content = queue_content

        result = harness._apply_backend_active_upsert(
            {
                "item": {
                    "active_item_id": "aid-runtime-serial",
                    "record_id": "rec-runtime-serial",
                    "payload": {
                        "active_item_id": "aid-runtime-serial",
                        "record_id": "rec-runtime-serial",
                        "target_record_id": "rec-runtime-serial",
                        "notice_type": "事件通告",
                        "_is_placeholder_record": False,
                        "_has_unuploaded_changes": True,
                        "text": new_text,
                    },
                }
            }
        )

        self.assertTrue(result["queued"])
        self.assertEqual(captured["record_id"], "rec-runtime-serial")
        self.assertEqual(captured["content"], new_text)
        self.assertEqual(captured["status"], "结束")
        self.assertEqual(captured["active_item_id"], "aid-runtime-serial")

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
        queued_update = dict(
            placeholder,
            _queued_after_upload=True,
            _queued_action="update",
        )
        self.assertEqual(ActiveNoticeModel.action_for_record(queued_update), "update")
        queued_update["_queued_upload_requested"] = True
        self.assertEqual(ActiveNoticeModel.action_for_record(queued_update), "")
        self.assertEqual(
            ActiveNoticeModel.action_label_for_record(queued_update),
            "已排队",
        )

    def test_today_progress_helpers(self):
        change = {
            "notice_type": "变更通告",
            "record_id": "rid-1",
            "target_record_id": "rid-1",
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
            "notice_type": "变更通告",
            "record_id": "",
            "target_record_id": "target-1",
            "_is_placeholder_record": False,
            "today_in_progress_state": "unknown",
        }

        self.assertTrue(ActiveNoticeModel.supports_today_progress(change))

    def test_active_cache_schedule_does_not_full_scan_by_default(self):
        harness = _ActiveCacheScheduleHarness()

        harness.schedule_active_cache_save()

        self.assertTrue(harness._active_cache_dirty)
        self.assertEqual(harness.save_calls, 0)

        harness.request_active_cache_save(force=True)

        self.assertEqual(harness.save_calls, 1)

    def test_replace_record_id_updates_placeholder_item_without_target_index(self):
        harness = _ReplaceRecordIdHarness()
        item = QListWidgetItem("placeholder")
        harness.list_active_event.addItem(item)
        item.setData(
            Qt.ItemDataRole.UserRole,
            {
                "active_item_id": "aid-1",
                "record_id": "placeholder-1",
                "payload_key": "placeholder-1",
                "_is_placeholder_record": True,
                "notice_type": "事件通告",
                "text": "【事件通告】状态：开始\n【标题】测试",
            },
        )

        changed = harness._replace_record_id_everywhere("placeholder-1", "rec-real")

        self.assertTrue(changed)
        data = item.data(Qt.ItemDataRole.UserRole)
        self.assertEqual(data["record_id"], "rec-real")
        self.assertEqual(data["target_record_id"], "rec-real")
        self.assertEqual(data["payload_key"], "rec-real")
        self.assertFalse(data["_is_placeholder_record"])
        self.assertEqual(harness.current_screenshot_record_id, "rec-real")
        self.assertIn("rec-real", harness.pending_action_record_ids)
        self.assertEqual(harness._pending_force_uploads[0]["record_id"], "rec-real")
        self.assertEqual(harness.cache_store.renamed, [("placeholder-1", "rec-real")])

    def test_event_update_does_not_reuse_title_match_when_event_time_differs(self):
        harness = _ReplaceRecordIdHarness()
        item = QListWidgetItem("event")
        harness.list_active_event.addItem(item)
        item.setData(
            Qt.ItemDataRole.UserRole,
            {
                "active_item_id": "aid-event",
                "record_id": "rec-event-target",
                "target_record_id": "rec-event-target",
                "payload_key": "rec-event-target",
                "_is_placeholder_record": False,
                "notice_type": "事件通告",
                "text": (
                    "【事件通告】状态：开始\n"
                    "【标题】D楼直流屏系统总故障\n"
                    "【时间】2026-06-24 10:00"
                ),
                "match_title": "D楼直流屏系统总故障",
                "match_key": "事件通告|D楼直流屏系统总故障|2026-06-24 10:00",
            },
        )

        list_widget, found = harness._find_active_item_by_content_or_title(
            (
                "【事件通告】状态：更新\n"
                "【标题】D楼直流屏系统总故障\n"
                "【时间】2026-06-24 10:30"
            ),
            title="D楼直流屏系统总故障",
            notice_type="事件通告",
            unique_key="事件通告|D楼直流屏系统总故障|2026-06-24 10:30",
        )

        self.assertIsNone(list_widget)
        self.assertIsNone(found)

    def test_event_update_does_not_reuse_parsed_title_when_event_time_differs(self):
        harness = _ReplaceRecordIdHarness()
        item = QListWidgetItem("event")
        harness.list_active_event.addItem(item)
        item.setData(
            Qt.ItemDataRole.UserRole,
            {
                "active_item_id": "aid-event",
                "record_id": "rec-event-target",
                "target_record_id": "rec-event-target",
                "payload_key": "rec-event-target",
                "_is_placeholder_record": False,
                "notice_type": "事件通告",
                "text": (
                    "【事件通告】状态：开始\n"
                    "【标题】D楼直流屏系统总故障\n"
                    "【时间】2026-06-24 10:00"
                ),
            },
        )

        list_widget, found = harness._find_active_item_by_content_or_title(
            (
                "【事件通告】状态：更新\n"
                "【标题】D楼直流屏系统总故障\n"
                "【时间】2026-06-24 10:30"
            ),
            title="D楼直流屏系统总故障",
            notice_type="事件通告",
            unique_key="事件通告|D楼直流屏系统总故障|2026-06-24 10:30",
        )

        self.assertIsNone(list_widget)
        self.assertIsNone(found)

    def test_event_update_without_dialog_level_reuses_unique_active_item(self):
        harness = _ReplaceRecordIdHarness()
        item = QListWidgetItem("event")
        harness.list_active_event.addItem(item)
        item.setData(
            Qt.ItemDataRole.UserRole,
            {
                "active_item_id": "aid-event",
                "record_id": "rec-event-target",
                "target_record_id": "rec-event-target",
                "_is_placeholder_record": False,
                "notice_type": "事件通告",
                "buildings": ["B楼"],
                "level": "I3",
                "source": "BMS系统",
                "event_source": "BMS系统",
                "text": (
                    "【事件通告】状态：开始\n"
                    "【标题】EA118机房B楼I3级事件通报\n"
                    "【来源】BMS系统\n"
                    "【时间】2026-06-24 10:00\n"
                    "【概述】BMS报B-301支路功率过高报警"
                ),
            },
        )

        list_widget, found = harness._find_active_item_by_content_or_title(
            (
                "【事件通告】状态：更新\n"
                "【标题】EA118机房B楼I3级事件通报\n"
                "【来源】BMS系统\n"
                "【时间】2026-06-24 10:00\n"
                "【概述】BMS报B-301支路功率过高报警\n"
                "【进展】继续排查"
            ),
            title="EA118机房B楼I3级事件通报",
            notice_type="事件通告",
        )

        self.assertIs(list_widget, harness.list_active_event)
        self.assertIs(found, item)

    def test_event_update_allows_summary_correction_when_lifecycle_is_unique(self):
        harness = _ReplaceRecordIdHarness()
        item = QListWidgetItem("event")
        harness.list_active_event.addItem(item)
        item.setData(
            Qt.ItemDataRole.UserRole,
            {
                "active_item_id": "aid-event-summary",
                "record_id": "rec-event-summary",
                "target_record_id": "rec-event-summary",
                "_is_placeholder_record": False,
                "notice_type": "事件通告",
                "buildings": ["A楼"],
                "level": "I3",
                "source": "BMS系统",
                "event_source": "BMS系统",
                "text": (
                    "【事件通告】状态：新增\n"
                    "【标题】EA118机房A楼I3级事件通报\n"
                    "【来源】BMS系统\n"
                    "【时间】2026-08-21 10:00\n"
                    "【概述】A楼空调压差告警"
                ),
            },
        )
        update_text = (
            "【事件通告】状态：更新\n"
            "【标题】EA118机房A楼I3级事件通报\n"
            "【来源】BMS系统\n"
            "【时间】2026-08-21 10:00\n"
            "【概述】经现场确认改为A楼过滤器堵塞告警"
        )

        list_widget, found = harness._find_active_item_by_content_or_title(
            update_text,
            title="EA118机房A楼I3级事件通报",
            notice_type="事件通告",
        )

        self.assertIs(list_widget, harness.list_active_event)
        self.assertIs(found, item)

    def test_manual_event_detail_save_enables_update_and_end(self):
        harness = _ReplaceRecordIdHarness()
        item = QListWidgetItem("event")
        harness.list_active_event.addItem(item)
        base = {
            "active_item_id": "aid-event-manual-edit",
            "record_id": "rec-event-manual-edit",
            "target_record_id": "rec-event-manual-edit",
            "_is_placeholder_record": False,
            "_has_unuploaded_changes": False,
            "notice_type": "事件通告",
            "status": "新增",
            "text": (
                "【事件通告】状态：新增\n"
                "【标题】EA118机房A楼I3级事件通报\n"
                "【来源】BMS系统\n"
                "【时间】2026-08-21 10:00\n"
                "【概述】A楼空调压差告警"
            ),
        }
        item.setData(Qt.ItemDataRole.UserRole, base)

        for status, action in (("更新", "update"), ("结束", "end")):
            text = (
                f"【事件通告】状态：{status}\n"
                "【标题】EA118机房A楼I3级事件通报\n"
                "【来源】BMS系统\n"
                "【时间】2026-08-21 10:00\n"
                "【概述】现场手动修正后的事件详情\n"
                f"【进展】准备发送{status}"
            )
            harness.sync_content_to_widget(
                "aid-event-manual-edit",
                "rec-event-manual-edit",
                {"text": text},
            )
            saved = item.data(Qt.ItemDataRole.UserRole)

            self.assertEqual(saved["target_record_id"], "rec-event-manual-edit")
            self.assertEqual(saved["status"], status)
            self.assertTrue(saved["_has_unuploaded_changes"])
            self.assertEqual(saved["content"], "现场手动修正后的事件详情")
            self.assertEqual(ActiveNoticeModel.action_for_record(saved), action)

    def test_event_copy_during_upload_is_visible_and_actionable_as_next_generation(self):
        harness = _ReplaceRecordIdHarness()
        harness.pending_action_record_ids = {"rec-event-serial"}
        harness.pending_action_types = {"rec-event-serial": "update"}
        harness.pending_upload_rollback_by_record_id = {}
        harness.pending_update_after_upload = {}
        item = QListWidgetItem("event")
        harness.list_active_event.addItem(item)
        old_text = (
            "【事件通告】状态：更新\n"
            "【标题】EA118机房A楼I3级事件通报\n"
            "【来源】BMS系统\n"
            "【时间】2026-08-21 10:00\n"
            "【概述】当前正在上传的内容"
        )
        new_text = old_text.replace("状态：更新", "状态：结束").replace(
            "当前正在上传的内容",
            "上传期间复制的新结束内容",
        )
        item.setData(
            Qt.ItemDataRole.UserRole,
            {
                "active_item_id": "aid-event-serial",
                "record_id": "rec-event-serial",
                "target_record_id": "rec-event-serial",
                "notice_type": "事件通告",
                "_is_placeholder_record": False,
                "_has_unuploaded_changes": False,
                "_upload_in_progress": True,
                "_upload_operation_id": "qt_notice:old",
                "text": old_text,
            },
        )

        queued = harness._queue_pending_content(
            "rec-event-serial",
            new_text,
            "结束",
            active_item_id="aid-event-serial",
        )
        data = item.data(Qt.ItemDataRole.UserRole)

        self.assertTrue(queued)
        self.assertEqual(data["text"], new_text)
        self.assertTrue(data["_queued_after_upload"])
        self.assertEqual(data["_queued_action"], "end")
        self.assertFalse(data["_upload_in_progress"])
        self.assertTrue(data["_has_unuploaded_changes"])
        self.assertEqual(data["_upload_operation_id"], "qt_notice:old")
        self.assertEqual(ActiveNoticeModel.action_for_record(data), "end")
        self.assertEqual(
            harness.pending_upload_rollback_by_record_id[
                "rec-event-serial"
            ]["old_data"]["text"],
            old_text,
        )

    def test_same_event_text_is_not_queued_again_while_uploading(self):
        harness = _ReplaceRecordIdHarness()
        harness.pending_action_record_ids = {"rec-event-same"}
        harness.pending_upload_rollback_by_record_id = {}
        harness.pending_update_after_upload = {}
        item = QListWidgetItem("event")
        harness.list_active_event.addItem(item)
        text = (
            "【事件通告】状态：新增\n"
            "【标题】EA118机房E楼I2级事件通报\n"
            "【来源】BMS\n【概述】冷机故障"
        )
        item.setData(
            Qt.ItemDataRole.UserRole,
            {
                "active_item_id": "aid-event-same",
                "record_id": "rec-event-same",
                "target_record_id": "rec-event-same",
                "notice_type": "事件通告",
                "_upload_in_progress": True,
                "text": text,
            },
        )

        queued = harness._queue_pending_content(
            "rec-event-same",
            text.replace("\n", "\r\n "),
            "新增",
        )

        data = item.data(Qt.ItemDataRole.UserRole)
        self.assertFalse(queued)
        self.assertNotIn("_queued_after_upload", data)
        self.assertFalse(harness.pending_upload_rollback_by_record_id)

    def test_recreated_event_start_upload_queues_latest_end_generation(self):
        harness = _ReplaceRecordIdHarness()
        record_id = "local_recreated_event"
        harness.pending_action_record_ids = {record_id}
        harness.pending_action_types = {record_id: "upload"}
        harness.pending_upload_rollback_by_record_id = {}
        harness.pending_update_after_upload = {}
        harness._pending_update_after_upload_scheduled = False
        item = QListWidgetItem("recreated-event")
        harness.list_active_event.addItem(item)
        start_text = (
            "【事件通告】状态：新增\n【标题】E楼事件\n"
            "【来源】BMS\n【时间】2026-08-26 17:00\n【概述】重新新增"
        )
        update_text = start_text.replace("状态：新增", "状态：更新").replace(
            "重新新增", "正在处理"
        )
        end_text = start_text.replace("状态：新增", "状态：结束").replace(
            "重新新增", "工作已完成"
        )
        item.setData(
            Qt.ItemDataRole.UserRole,
            {
                "active_item_id": "active-recreated-event",
                "record_id": record_id,
                "target_record_id": "",
                "notice_type": "事件通告",
                "origin": "clipboard_recreated_after_delete",
                "_is_placeholder_record": True,
                "_has_unuploaded_changes": False,
                "_upload_in_progress": True,
                "_upload_operation_id": "qt_notice:recreated-start",
                "text": start_text,
            },
        )

        queued = harness._queue_pending_content(
            record_id,
            update_text,
            "更新",
            active_item_id="active-recreated-event",
        )
        self.assertTrue(queued)
        self.assertTrue(
            harness._queue_confirmed_upload_if_busy(
                item.data(Qt.ItemDataRole.UserRole),
                screenshot_bytes=b"update-image",
                action_type="update",
                response_time="",
                buildings=["E楼"],
                extra_images=[],
                specialty="电气",
                change_level="",
                event_level="I3",
                event_source="BMS",
                recover_selected=False,
                robot_group_choice="auto",
            )
        )
        queued = harness._queue_pending_content(
            record_id,
            end_text,
            "结束",
            active_item_id="active-recreated-event",
        )
        data = item.data(Qt.ItemDataRole.UserRole)

        self.assertTrue(queued)
        self.assertEqual(data["text"], end_text)
        self.assertEqual(data["_queued_action"], "end")
        self.assertTrue(data["_queued_after_upload"])
        self.assertTrue(data["_queued_upload_requested"])
        pending = harness.pending_update_after_upload[record_id]
        self.assertEqual(pending["action_type"], "end")
        self.assertEqual(pending["data"]["text"], end_text)
        self.assertEqual(
            harness.pending_upload_rollback_by_record_id[record_id]["old_data"][
                "text"
            ],
            start_text,
        )
        self.assertEqual(ActiveNoticeModel.action_label_for_record(data), "已排队")

    def test_queued_event_upload_success_keeps_next_text_and_dispatch_request(self):
        harness = _ReplaceRecordIdHarness()
        harness._closing = False
        harness._set_last_ui_op = lambda *_args, **_kwargs: None
        harness.pending_action_record_ids = {"rec-event-serial-success"}
        harness.pending_action_types = {"rec-event-serial-success": "update"}
        harness.pending_upload_rollback_by_record_id = {}
        harness.pending_update_after_upload = {}
        harness.current_screenshot_record_id = ""
        harness.current_screenshot_action_type = None
        item = QListWidgetItem("event")
        harness.list_active_event.addItem(item)
        old_text = (
            "【事件通告】状态：更新\n【标题】A楼I3事件\n"
            "【来源】BMS\n【时间】2026-08-21 10:00\n【概述】旧上传"
        )
        new_text = old_text.replace("【概述】旧上传", "【概述】下一条更新")
        item.setData(
            Qt.ItemDataRole.UserRole,
            {
                "active_item_id": "aid-event-serial-success",
                "record_id": "rec-event-serial-success",
                "target_record_id": "rec-event-serial-success",
                "notice_type": "事件通告",
                "_is_placeholder_record": False,
                "_has_unuploaded_changes": False,
                "_upload_in_progress": True,
                "_upload_operation_id": "qt_notice:old-success",
                "text": old_text,
            },
        )
        harness._queue_pending_content(
            "rec-event-serial-success",
            new_text,
            "更新",
        )
        captured = {}
        harness._queue_update_after_upload = (
            lambda record_id, request: captured.update(
                {"record_id": record_id, "request": request}
            )
        )

        queued = harness._queue_confirmed_upload_if_busy(
            item.data(Qt.ItemDataRole.UserRole),
            screenshot_bytes=b"image",
            action_type="update",
            response_time="",
            buildings=["A楼"],
            extra_images=[],
            specialty="电气",
            change_level="",
            event_level="I3",
            event_source="BMS",
            recover_selected=False,
            robot_group_choice="auto",
        )
        harness.restore_button_state(
            True,
            "更新",
            "rec-event-serial-success",
        )
        data = item.data(Qt.ItemDataRole.UserRole)

        self.assertTrue(queued)
        self.assertEqual(captured["record_id"], "rec-event-serial-success")
        self.assertEqual(captured["request"]["data"]["text"], new_text)
        self.assertNotIn(
            "_upload_operation_id",
            captured["request"]["data"],
        )
        self.assertEqual(data["text"], new_text)
        self.assertTrue(data["_has_unuploaded_changes"])
        self.assertTrue(data["_queued_upload_requested"])
        self.assertEqual(ActiveNoticeModel.action_label_for_record(data), "已排队")
        self.assertFalse(harness.pending_action_record_ids)

    def test_queued_event_upload_failure_rolls_back_and_discards_next_generation(self):
        harness = _ReplaceRecordIdHarness()
        harness._closing = False
        harness._set_last_ui_op = lambda *_args, **_kwargs: None
        harness.pending_action_record_ids = {"rec-event-serial-fail"}
        harness.pending_action_types = {"rec-event-serial-fail": "update"}
        harness.pending_upload_rollback_by_record_id = {}
        harness.pending_update_after_upload = {}
        harness.current_screenshot_record_id = ""
        harness.current_screenshot_action_type = None
        persisted = []
        harness._upsert_active_cache_record = (
            lambda data: persisted.append(dict(data)) or True
        )
        item = QListWidgetItem("event")
        harness.list_active_event.addItem(item)
        old_text = (
            "【事件通告】状态：更新\n【标题】A楼I3事件\n"
            "【来源】BMS\n【时间】2026-08-21 10:00\n【概述】失败的当前上传"
        )
        new_text = old_text.replace("失败的当前上传", "应被丢弃的下一条")
        item.setData(
            Qt.ItemDataRole.UserRole,
            {
                "active_item_id": "aid-event-serial-fail",
                "record_id": "rec-event-serial-fail",
                "target_record_id": "rec-event-serial-fail",
                "notice_type": "事件通告",
                "_is_placeholder_record": False,
                "_has_unuploaded_changes": False,
                "_upload_in_progress": True,
                "_upload_operation_id": "qt_notice:old-fail",
                "text": old_text,
            },
        )
        harness._queue_pending_content(
            "rec-event-serial-fail",
            new_text,
            "更新",
        )
        harness.pending_update_after_upload["rec-event-serial-fail"] = {
            "data": {"text": new_text},
            "action_type": "update",
        }

        harness.restore_button_state(
            False,
            "更新",
            "rec-event-serial-fail",
        )
        data = item.data(Qt.ItemDataRole.UserRole)

        self.assertEqual(data["text"], old_text)
        self.assertNotIn("_queued_after_upload", data)
        self.assertNotIn("_queued_upload_requested", data)
        self.assertTrue(data["_has_unuploaded_changes"])
        self.assertFalse(data["_upload_in_progress"])
        self.assertIn("失败", data["_last_upload_error"])
        self.assertFalse(harness.pending_update_after_upload)
        self.assertFalse(harness.pending_upload_rollback_by_record_id)
        self.assertEqual(persisted[-1]["text"], old_text)

    def test_queued_event_dispatches_automatically_after_current_upload_finishes(self):
        harness = _ReplaceRecordIdHarness()
        harness._closing = False
        harness._pending_update_after_upload_scheduled = True
        harness.pending_action_record_ids = set()
        harness.pending_action_types = {}
        harness.current_screenshot_record_id = ""
        harness.screenshot_dialog = type(
            "HiddenDialog",
            (),
            {"isVisible": lambda self: False},
        )()
        item = QListWidgetItem("event")
        harness.list_active_event.addItem(item)
        text = (
            "【事件通告】状态：结束\n【标题】A楼I3事件\n"
            "【来源】BMS\n【时间】2026-08-21 10:00\n【概述】排队结束"
        )
        item.setData(
            Qt.ItemDataRole.UserRole,
            {
                "active_item_id": "aid-event-auto-dispatch",
                "record_id": "rec-event-auto-dispatch",
                "target_record_id": "rec-event-auto-dispatch",
                "notice_type": "事件通告",
                "_is_placeholder_record": False,
                "_has_unuploaded_changes": True,
                "_queued_after_upload": True,
                "_queued_action": "end",
                "_queued_upload_requested": True,
                "_upload_operation_id": "qt_notice:finished-old",
                "text": text,
            },
        )
        harness.pending_update_after_upload = {
            "rec-event-auto-dispatch": {
                "data": {
                    "active_item_id": "aid-event-auto-dispatch",
                    "record_id": "local_event_auto_dispatch",
                    "target_record_id": "",
                    "notice_type": "事件通告",
                    "_is_placeholder_record": False,
                    "_has_unuploaded_changes": True,
                    "text": text,
                },
                "action_type": "end",
                "event_level": "I3",
                "event_source": "BMS",
            }
        }
        harness._resolve_upload_fields_from_cache = (
            lambda _data, _fields: {
                "buildings": ["A楼"],
                "specialty": "电气",
                "level": "I3",
                "event_source": "BMS",
            }
        )
        captured = {}

        def upload(data, screenshot, action, **kwargs):
            captured.update(
                {
                    "data": dict(data),
                    "screenshot": screenshot,
                    "action": action,
                    "kwargs": kwargs,
                }
            )

        harness.do_feishu_upload = upload

        harness._try_process_pending_update_after_upload()

        self.assertEqual(captured["action"], "end")
        self.assertEqual(captured["data"]["text"], text)
        self.assertEqual(
            captured["data"]["target_record_id"],
            "rec-event-auto-dispatch",
        )
        self.assertNotIn("_upload_operation_id", captured["data"])
        self.assertFalse(harness.pending_update_after_upload)
        current = item.data(Qt.ItemDataRole.UserRole)
        self.assertNotIn("_queued_after_upload", current)
        self.assertNotIn("_queued_upload_requested", current)
        self.assertNotIn("_upload_operation_id", current)

    def test_pending_event_dispatch_only_waits_for_the_same_event(self):
        harness = _ReplaceRecordIdHarness()
        harness._closing = False
        harness._pending_update_after_upload_scheduled = True
        harness.pending_action_record_ids = {"rec-event-busy"}
        harness.pending_action_types = {"rec-event-busy": "update"}
        harness.current_screenshot_record_id = ""
        harness.screenshot_dialog = type(
            "HiddenDialog",
            (),
            {"isVisible": lambda self: False},
        )()
        pending = {}
        for suffix in ("busy", "ready-a", "ready-b"):
            record_id = f"rec-event-{suffix}"
            item = QListWidgetItem(suffix)
            harness.list_active_event.addItem(item)
            text = (
                "【事件通告】状态：更新\n"
                f"【标题】{suffix}\n【来源】BMS\n"
                "【时间】2026-08-26 15:00\n【概述】并发上传测试"
            )
            item.setData(
                Qt.ItemDataRole.UserRole,
                {
                    "active_item_id": f"aid-event-{suffix}",
                    "record_id": record_id,
                    "target_record_id": record_id,
                    "notice_type": "事件通告",
                    "_is_placeholder_record": False,
                    "_upload_in_progress": suffix == "busy",
                    "text": text,
                },
            )
            pending[record_id] = {
                "data": dict(item.data(Qt.ItemDataRole.UserRole)),
                "action_type": "update",
                "event_level": "I3",
                "event_source": "BMS",
            }
        harness.pending_update_after_upload = pending
        harness._resolve_upload_fields_from_cache = (
            lambda _data, _fields: {
                "buildings": ["A楼"],
                "specialty": "电气",
                "level": "I3",
                "event_source": "BMS",
            }
        )
        dispatched = []
        harness.do_feishu_upload = (
            lambda data, _screenshot, _action, **_kwargs: dispatched.append(
                data["record_id"]
            )
        )
        scheduled = []
        harness._schedule_pending_update_after_upload = (
            lambda delay=300: scheduled.append(delay)
        )

        harness._try_process_pending_update_after_upload()

        self.assertEqual(
            dispatched,
            ["rec-event-ready-a", "rec-event-ready-b"],
        )
        self.assertEqual(
            set(harness.pending_update_after_upload),
            {"rec-event-busy"},
        )
        self.assertEqual(scheduled, [500])

    def test_sparse_event_update_does_not_choose_between_two_active_items(self):
        harness = _ReplaceRecordIdHarness()
        for code in ("A", "B"):
            item = QListWidgetItem(f"event-{code}")
            harness.list_active_event.addItem(item)
            item.setData(
                Qt.ItemDataRole.UserRole,
                {
                    "active_item_id": f"aid-event-{code}",
                    "record_id": f"rec-event-{code}",
                    "target_record_id": f"rec-event-{code}",
                    "_is_placeholder_record": False,
                    "notice_type": "事件通告",
                    "buildings": [f"{code}楼"],
                    "level": "I3",
                    "source": "BMS系统",
                    "event_source": "BMS系统",
                    "text": (
                        "【事件通告】状态：开始\n"
                        "【标题】EA118机房事件通报\n"
                        "【来源】BMS系统\n"
                        "【时间】2026-06-24 10:00\n"
                        "【概述】公共告警描述"
                    ),
                },
            )

        list_widget, found = harness._find_active_item_by_content_or_title(
            (
                "【事件通告】状态：更新\n"
                "【标题】EA118机房事件通报\n"
                "【来源】BMS系统\n"
                "【时间】2026-06-24 10:00\n"
                "【概述】公共告警描述\n"
                "【进展】继续排查"
            ),
            title="EA118机房事件通报",
            notice_type="事件通告",
        )

        self.assertIsNone(list_widget)
        self.assertIsNone(found)

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
            "target_record_id": "rid-1",
            "notice_type": "变更通告",
            "_is_placeholder_record": False,
            "_has_unuploaded_changes": True,
            "today_in_progress_state": "yes",
            "text": "【变更通告】状态：开始\n\n【标题】A楼变更\n\n【时间】2026-01-01",
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

    def test_runtime_upload_fields_are_inherited_during_cache_refresh(self):
        harness = _RecordsFlagHarness()
        merged = harness._inherit_active_runtime_fields(
            {
                "record_id": "target-runtime",
                "_has_unuploaded_changes": True,
            },
            {
                "record_id": "target-runtime",
                "_upload_in_progress": True,
                "_upload_pending_dialog": False,
                "_upload_started_monotonic": 123.0,
                "_pending_upload_hash": "hash-runtime",
                "_upload_operation_id": "qt_notice:current",
                "_has_unuploaded_changes": False,
            },
        )

        self.assertTrue(merged["_upload_in_progress"])
        self.assertEqual(merged["_upload_operation_id"], "qt_notice:current")
        self.assertEqual(merged["_pending_upload_hash"], "hash-runtime")
        self.assertFalse(merged["_has_unuploaded_changes"])

    def test_commit_runtime_only_state_does_not_write_active_cache(self):
        harness = _AddItemHarness(model_view_visible=True)
        harness.cache_store = _CommitCacheStore()
        item, _widget = harness.add_active_item(
            {
                "active_item_id": "aid-runtime-only",
                "record_id": "target-runtime-only",
                "target_record_id": "target-runtime-only",
                "notice_type": "事件通告",
                "text": "【事件通告】状态：更新\n【标题】运行态测试",
                "_is_placeholder_record": False,
                "_upload_in_progress": True,
                "_upload_operation_id": "qt_notice:runtime-only",
                "_has_unuploaded_changes": False,
            },
            skip_cache=True,
        )
        harness.cache_store.upserts.clear()

        committed = harness._commit_active_record(
            item.data(Qt.ItemDataRole.UserRole),
            persist_cache=False,
            refresh_detail=False,
            rebuild_widget=False,
            list_widget=harness.list_active_event,
            item=item,
        )

        self.assertEqual(harness.cache_store.upserts, [])
        self.assertTrue(committed["_upload_in_progress"])
        self.assertEqual(
            committed["_upload_operation_id"],
            "qt_notice:runtime-only",
        )

    def test_upload_result_generation_rejects_stale_callback(self):
        harness = _AddItemHarness(model_view_visible=True)
        harness.pending_action_record_ids = {"target-generation"}
        harness._payload_alias = {}
        harness._upload_key_alias = {}
        harness.add_active_item(
            {
                "active_item_id": "aid-generation",
                "record_id": "target-generation",
                "target_record_id": "target-generation",
                "notice_type": "事件通告",
                "text": "【事件通告】状态：更新\n【标题】代次测试",
                "_is_placeholder_record": False,
                "_upload_in_progress": True,
                "_upload_operation_id": "qt_notice:new",
            },
            skip_cache=True,
        )

        self.assertTrue(
            harness._is_current_upload_operation(
                "target-generation", "qt_notice:new"
            )
        )
        self.assertFalse(
            harness._is_current_upload_operation(
                "target-generation", "qt_notice:old"
            )
        )

    def test_clear_upload_runtime_state_covers_old_and_real_record_ids(self):
        harness = _AddItemHarness(model_view_visible=True)
        harness.pending_action_record_ids = {"placeholder-2", "target-2"}
        harness.pending_action_types = {
            "placeholder-2": "upload",
            "target-2": "update",
        }
        harness.pending_upload_rollback_by_record_id = {
            "placeholder-2": {"old_data": {}},
            "target-2": {"old_data": {}},
        }
        harness._payload_alias = {"placeholder-2": "target-2"}
        harness._upload_key_alias = {"target-2": "placeholder-2"}
        harness.save_active_cache = lambda: None
        harness.schedule_active_cache_save = lambda *_args, **_kwargs: None

        item, _widget = harness.add_active_item(
            {
                "active_item_id": "aid-upload-clear",
                "record_id": "target-2",
                "target_record_id": "target-2",
                "notice_type": "维保通告",
                "text": "【维保通告】状态：开始\n\n【标题】A楼维保\n\n【时间】2026-01-01",
                "_is_placeholder_record": False,
                "_has_unuploaded_changes": False,
                "_upload_in_progress": True,
                "_pending_upload_hash": "hash-2",
                "_upload_started_monotonic": 123.0,
            },
            skip_cache=True,
        )

        harness.clear_upload_runtime_state_for_ids("placeholder-2", "target-2")

        updated = item.data(Qt.ItemDataRole.UserRole)
        self.assertFalse(updated.get("_upload_in_progress"))
        self.assertIsNone(updated.get("_pending_upload_hash"))
        self.assertNotIn("_upload_started_monotonic", updated)
        self.assertFalse(harness.pending_action_record_ids)
        self.assertFalse(harness.pending_action_types)
        self.assertFalse(harness.pending_upload_rollback_by_record_id)

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

    def test_recover_stale_upload_state_clears_orphan_pending_after_hard_timeout(self):
        harness = _AddItemHarness(model_view_visible=True)
        harness.pending_action_record_ids = {"target-hard-timeout"}
        harness.pending_action_types = {"target-hard-timeout": "update"}
        harness.pending_upload_rollback_by_record_id = {
            "target-hard-timeout": {"old_data": {}}
        }
        harness.pending_new_by_record_id = {}
        harness.pending_update_after_upload = {}
        harness.current_screenshot_record_id = ""
        harness._payload_alias = {}
        harness._upload_key_alias = {}
        harness.save_active_cache = lambda: None
        harness.schedule_active_cache_save = lambda *_args, **_kwargs: None

        item, _widget = harness.add_active_item(
            {
                "active_item_id": "aid-stale-hard",
                "record_id": "target-hard-timeout",
                "target_record_id": "target-hard-timeout",
                "notice_type": "维保通告",
                "text": "【维保通告】状态：更新\n\n【标题】A楼维保\n\n【时间】2026-01-01",
                "_is_placeholder_record": False,
                "_has_unuploaded_changes": False,
                "_upload_in_progress": True,
                "_pending_upload_hash": "hash-hard",
                "_upload_started_monotonic": time.monotonic() - 600.0,
            },
            skip_cache=True,
        )

        result = harness._recover_stale_upload_states()

        updated = item.data(Qt.ItemDataRole.UserRole)
        self.assertEqual(result["stale_upload_recovered"], 1)
        self.assertFalse(updated.get("_upload_in_progress"))
        self.assertFalse(harness.pending_action_record_ids)
        self.assertFalse(harness.pending_action_types)

    def test_today_progress_toggle_updates_model_and_uses_target_record_id(self):
        harness = _TodayProgressHarness()
        item, _widget = harness.add_active_item(
            {
                "active_item_id": "aid-change",
                "record_id": "source-1",
                "target_record_id": "target-1",
                "notice_type": "变更通告",
                "today_in_progress_state": "unknown",
                "_is_placeholder_record": False,
                "_has_unuploaded_changes": False,
                "text": "【变更通告】状态：开始\n\n【标题】A楼变更\n\n【时间】2026-01-01",
            },
            skip_cache=True,
        )

        harness._handle_today_in_progress_toggle(
            item.data(Qt.ItemDataRole.UserRole),
            "yes",
        )

        model = harness._active_notice_model_for_list(harness.list_active_other)
        deadline = time.time() + 2.0
        updated = model.record_by_active_item_id("aid-change")
        while (
            time.time() < deadline
            and (updated or {}).get("today_in_progress_state") != "yes"
        ):
            app = QApplication.instance()
            if app is not None:
                app.processEvents()
            time.sleep(0.01)
            updated = model.record_by_active_item_id("aid-change")
        self.assertEqual(updated["today_in_progress_state"], "yes")
        self.assertEqual(
            harness.lan_template_portal_controller.calls[0][1]["record_id"],
            "target-1",
        )

    def test_qt_delete_retry_uses_stable_id_separate_from_upload_operation(self):
        harness = _QtDeleteOperationHarness()

        first = harness._execute_lan_ongoing_delete(
            {"scope": "A", "active_item_id": "active-delete-operation"}
        )
        second = harness._execute_lan_ongoing_delete(
            {"scope": "A", "active_item_id": "active-delete-operation"}
        )

        self.assertFalse(first["ok"])
        self.assertTrue(second["ok"])
        operation_ids = [
            item["payload"]["data_dict"]["operation_id"]
            for item in harness.submitted_payloads
        ]
        self.assertEqual(len(set(operation_ids)), 1)
        self.assertTrue(operation_ids[0].startswith("qt-delete:"))
        self.assertNotEqual(operation_ids[0], "upload-operation-must-not-be-reused")
        item_data = harness.item.data(Qt.ItemDataRole.UserRole)
        self.assertEqual(item_data["operation_id"], "upload-operation-must-not-be-reused")
        self.assertEqual(item_data["_delete_operation_id"], operation_ids[0])
        self.assertTrue(harness.removed)
        self.assertEqual(harness.cache_save_requests, 1)


if __name__ == "__main__":
    unittest.main()
