import datetime as dt
import sys
import tempfile
import threading
import unittest
from pathlib import Path


BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from upload_event_module.ui.main_window_runtime import MainWindowRuntimeMixin  # noqa: E402
from upload_event_module.ui.main_window_clipboard import MainWindowClipboardMixin  # noqa: E402
from upload_event_module.ui.main_window_records import MainWindowRecordsMixin  # noqa: E402
from upload_event_module.ui.main_window_workflow import MainWindowWorkflowMixin  # noqa: E402
from upload_event_module.core.parser import extract_notice_info  # noqa: E402
from clipflow_backend.main import FastAPIPortalController  # noqa: E402
from lan_bitable_template_portal.server import PortalRuntime  # noqa: E402
from lan_bitable_template_portal.portal_service import MaintenancePortalService  # noqa: E402
from lan_bitable_template_portal.state_store import LanPortalStateStore  # noqa: E402


class _Controller:
    def __init__(self):
        self.acks = []
        self.clipboard_events = []
        self.fail_contents: set[str] = set()

    def acknowledge_clipboard_candidate(self, candidate_id, *, ok=True, status=""):
        self.acks.append({"candidate_id": candidate_id, "ok": ok, "status": status})
        return {"candidate_id": candidate_id}

    def post_local_clipboard_event(
        self, content, *, ts=None, source="clipboard", target_record_id=""
    ):
        if content in self.fail_contents:
            raise RuntimeError("mock projection failed")
        self.clipboard_events.append(
            {
                "content": content,
                "ts": ts,
                "source": source,
                "target_record_id": target_record_id,
            }
        )
        return {
            "ok": True,
            "projection": {
                "ok": True,
                "item": {
                    "active_item_id": "active-from-sqlite",
                    "payload": {
                        "active_item_id": "active-from-sqlite",
                        "record_id": "local_active-from-sqlite",
                        "notice_type": "维保通告",
                        "text": content,
                    },
                },
            },
        }


class _Harness(MainWindowRuntimeMixin):
    def __init__(self):
        self.lan_template_portal_controller = _Controller()
        self._qt_shell_dialog_sessions = []
        self.reproject_called = False
        self.applied_projection_payloads = []

    def _submit_notice_text_to_backend_projection(self, *args, **kwargs):
        self.reproject_called = True
        raise AssertionError("Qt shell must not re-project backend clipboard candidates")

    def _apply_backend_active_upsert(self, payload):
        self.applied_projection_payloads.append(payload)
        return {"ok": True, "created": True}

    def _enqueue_ui_mutation(self, _name, callback):
        callback()


class _ClipboardHarness(MainWindowClipboardMixin):
    def __init__(self, store: LanPortalStateStore, clipboard_file: Path):
        self._closing = False
        self._clipboard_state_store = store
        self._clipboard_sqlite_last_event_id = 0
        self.clipboard_paused = False
        self._ui_update_in_progress = False
        self.clipboard_event_file = clipboard_file
        self._clipboard_pending_lines = []
        self._clipboard_file_index = 0
        self._clipboard_partial_line = ""
        self._clipboard_file_max_bytes = 1024 * 1024
        self.lan_template_portal_controller = _Controller()
        self.snapshots = []
        self.projections = []
        self.failures = []

    def _is_clipboard_listener_disabled(self):
        return False

    def _is_in_clipboard_cooldown(self):
        return False

    def _update_last_clipboard_snapshot(self, content, timestamp_ms=None):
        self.snapshots.append({"content": content, "ts": timestamp_ms})

    def _apply_clipboard_projection_result(self, result):
        self.projections.append(result)
        return {"ok": True}

    def _remember_clipboard_failure(self, reason: str):
        self.failures.append(reason)


class _ImmediateDeleteHarness(MainWindowWorkflowMixin):
    def __init__(self, *, remote_deleted: bool):
        self.remote_deleted = remote_deleted
        self.backend_started = threading.Event()
        self.backend_release = threading.Event()
        self.backend_finished = threading.Event()
        self.cache_delete_count = 0
        self.messages = []
        self._today_in_progress_pending_record_ids = set()
        self._today_in_progress_synced_record_ids = set()
        self.pending_new_by_record_id = {}
        self.pending_replace_by_record_id = {}
        self.pending_update_after_upload = {}
        self.pending_action_record_ids = set()
        self.pending_action_types = {}

    def _is_screenshot_dialog_active(self):
        return False

    def _find_active_item_by_record_id(self, _record_id):
        return None, None

    def _find_active_item_by_active_item_id(self, _active_item_id):
        return None, None

    def _safe_item_widget(self, _list_widget, _item):
        return None

    def _clear_upload_queue(self, _record_id):
        return None

    def _delete_active_cache_record(self, _data_dict):
        self.cache_delete_count += 1
        return True

    def request_active_cache_save(self, *args, **kwargs):
        return None

    def _submit_delete_active_item_to_backend(self, _data_dict):
        self.backend_started.set()
        self.backend_release.wait(2.0)
        return True, "", {"remote_deleted": self.remote_deleted}

    def _enqueue_ui_mutation(self, _name, callback):
        callback()
        self.backend_finished.set()

    def _remember_delete_undo(self, _data_dict, _result):
        return None

    def show_message(self, message):
        self.messages.append(str(message))


class _RecordsHarness(MainWindowRecordsMixin):
    pass


class _ActiveUpsertVisibilityHarness(MainWindowRuntimeMixin):
    def __init__(self):
        self.added = []

    @staticmethod
    def _ensure_active_item_identity(data):
        return dict(data)

    @staticmethod
    def _find_active_item_by_active_item_id(_active_item_id):
        return None, None

    @staticmethod
    def _find_active_item_by_record_id(_record_id):
        return None, None

    @staticmethod
    def _is_valid_list_item(_item):
        return False

    def add_active_item(self, data, **_kwargs):
        self.added.append(dict(data))
        return object(), None


class QtShellBackendEventTests(unittest.TestCase):
    def test_scoped_qt_active_identities_drop_deleted_local_event(self):
        previous_store = PortalRuntime.state_store
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            PortalRuntime.state_store = store
            try:
                month = dt.datetime.now().strftime("%Y-%m")
                payload = {
                    "active_item_id": "active-local-event-e",
                    "record_id": "localid-event-e",
                    "target_record_id": "localid-event-e",
                    "notice_type": "事件通告",
                    "work_type": "event",
                    "scope": "E",
                    "building_codes": ["E"],
                    "status": "更新",
                    "title": "E楼未上传事件",
                    "text": (
                        "【事件通告】状态：更新\n"
                        "【标题】E楼未上传事件\n"
                        f"【时间】{month}-15 09:35"
                    ),
                }
                self.assertTrue(
                    store.upsert_qt_active_item(
                        payload,
                        section="event",
                        origin="qt",
                    )
                )

                identities = FastAPIPortalController._scoped_qt_active_identities(
                    "E",
                    month_key=month,
                )

                self.assertEqual(len(identities), 1)
                self.assertEqual(
                    identities[0]["active_item_id"],
                    "active-local-event-e",
                )
                self.assertTrue(identities[0]["local_only"])
                self.assertTrue(
                    store.delete_qt_active_item(
                        active_item_id="active-local-event-e",
                    )
                )
                self.assertEqual(
                    FastAPIPortalController._scoped_qt_active_identities(
                        "E",
                        month_key=month,
                    ),
                    [],
                )
            finally:
                PortalRuntime.state_store = previous_store

    def test_lite_workbench_subscribes_to_immediate_qt_delete_updates(self):
        workbench_text = (
            BIN_DIR / "lan_bitable_template_portal" / "workbench_lite.py"
        ).read_text(encoding="utf-8")
        backend_text = (
            BIN_DIR / "clipflow_backend" / "main.py"
        ).read_text(encoding="utf-8")

        self.assertIn(
            "new EventSource(streamUrl.pathname + streamUrl.search)",
            workbench_text,
        )
        self.assertIn("applyQtActiveIdentitySnapshot", workbench_text)
        self.assertIn(
            '.ongoing-row[data-local-only="1"]:not(.optimistic)',
            workbench_text,
        )
        self.assertIn('"active_identities": active_identities', backend_text)
        self.assertIn("self._notify_qt_active_streams()", backend_text)

    def test_local_only_delete_removes_qt_cache_before_backend_finishes(self):
        harness = _ImmediateDeleteHarness(remote_deleted=False)
        payload = {
            "active_item_id": "active-local-delete",
            "record_id": "localid-event-update",
            "target_record_id": "localid-event-update",
            "notice_type": "事件通告",
            "work_type": "event",
        }

        harness._delete_active_item(payload)

        self.assertTrue(harness.backend_started.wait(1.0))
        self.assertEqual(harness.cache_delete_count, 1)
        self.assertIn("未上传通告已移除", harness.messages[0])
        harness.backend_release.set()
        self.assertTrue(harness.backend_finished.wait(1.0))
        self.assertEqual(harness.cache_delete_count, 1)

    def test_remote_delete_waits_for_backend_before_removing_qt_cache(self):
        harness = _ImmediateDeleteHarness(remote_deleted=True)
        payload = {
            "active_item_id": "active-remote-delete",
            "record_id": "rec-event-update",
            "target_record_id": "rec-event-update",
            "notice_type": "事件通告",
            "work_type": "event",
        }

        harness._delete_active_item(payload)

        self.assertTrue(harness.backend_started.wait(1.0))
        self.assertEqual(harness.cache_delete_count, 0)
        harness.backend_release.set()
        self.assertTrue(harness.backend_finished.wait(1.0))
        self.assertEqual(harness.cache_delete_count, 1)

    def test_runtime_active_upsert_ignores_items_outside_current_month(self):
        harness = _ActiveUpsertVisibilityHarness()
        previous_month = (
            dt.datetime.now().replace(day=1) - dt.timedelta(days=1)
        ).strftime("%Y-%m")
        result = harness._apply_backend_active_upsert(
            {
                "item": {
                    "active_item_id": "active-old-runtime",
                    "record_id": "rec-old-runtime",
                    "payload": {
                        "active_item_id": "active-old-runtime",
                        "record_id": "rec-old-runtime",
                        "notice_type": "变更通告",
                        "text": (
                            "【变更通告】状态：更新\n"
                            "【名称】旧月份变更\n"
                            f"【时间】{previous_month}-08 09:00"
                            f"~{previous_month}-08 18:00"
                        ),
                    },
                }
            }
        )

        self.assertTrue(result["ok"])
        self.assertTrue(result["ignored_outside_current_month"])
        self.assertEqual(harness.added, [])

    def test_event_parser_accepts_long_source_and_level_labels(self):
        text = (
            "【事件通告】状态：开始\n"
            "【标题】A楼冷机告警\n"
            "【事件发生时间】2026-06-24 10:00\n"
            "【机楼】A楼\n"
            "【事件等级】I2\n"
            "【事件发现来源】BMS"
        )

        info = extract_notice_info(text)

        self.assertIsNotNone(info)
        self.assertEqual(info["source"], "BMS")
        self.assertEqual(info["level"], "I2")
        self.assertIn("BMS", info["unique_key"])
        self.assertIn("I2", info["unique_key"])

    def test_event_parser_accepts_alarm_description_as_title(self):
        text = (
            "【事件通告】状态：开始\n"
            "【告警描述】BMS报A楼冷机高压告警\n"
            "【事件发生时间】2026-06-24 10:00\n"
            "【机楼】A楼\n"
            "【事件等级】I2\n"
            "【事件发现来源】BMS"
        )

        info = extract_notice_info(text)

        self.assertIsNotNone(info)
        self.assertEqual(info["title"], "BMS报A楼冷机高压告警")
        self.assertEqual(info["source"], "BMS")
        self.assertEqual(info["level"], "I2")

    def test_backend_event_clipboard_entry_does_not_use_clipboard_as_event_source(self):
        text = (
            "【事件通告】状态：开始\n"
            "【标题】A楼冷机告警\n"
            "【事件发生时间】2026-06-24 10:00\n"
            "【机楼】A楼\n"
            "【事件等级】I2"
        )

        entry = FastAPIPortalController._clipboard_entry_from_content(text)

        self.assertIsNotNone(entry)
        self.assertEqual(entry["source"], "")
        self.assertEqual(entry["origin"], "clipboard")

    def test_event_active_update_inherits_existing_target_record_id(self):
        existing = {
            "active_item_id": "event-active-1",
            "record_id": "rec_event_target_1",
            "target_record_id": "rec_event_target_1",
            "_is_placeholder_record": False,
            "event_source": "BMS",
            "source": "BMS",
            "event_identity_key": "事件|2026-06-24 10:00|A楼|BMS|I2",
            "event_match_fields": {
                "title": "A楼冷机告警",
                "event_time": "2026-06-24 10:00",
                "building": "A楼",
                "source": "BMS",
                "level": "I2",
            },
            "site_photo_count": 1,
            "extra_image_count": 2,
        }
        incoming = {
            "active_item_id": "localid_event_active_1",
            "record_id": "localid_event_update_1",
            "target_record_id": "localid_event_update_1",
            "_is_placeholder_record": True,
            "notice_type": "事件通告",
            "text": "【事件通告】状态：更新\n【标题】A楼冷机告警",
        }

        updated = _RecordsHarness()._inherit_active_runtime_fields(incoming, existing)

        self.assertEqual(updated["active_item_id"], "event-active-1")
        self.assertEqual(updated["record_id"], "rec_event_target_1")
        self.assertEqual(updated["target_record_id"], "rec_event_target_1")
        self.assertFalse(updated["_is_placeholder_record"])
        self.assertEqual(updated["event_source"], "BMS")
        self.assertEqual(updated["source"], "BMS")
        self.assertEqual(updated["event_identity_key"], existing["event_identity_key"])
        self.assertEqual(updated["event_match_fields"], existing["event_match_fields"])
        self.assertEqual(updated["site_photo_count"], 1)
        self.assertEqual(updated["extra_image_count"], 2)

    def test_notice_text_projection_covers_all_non_event_work_types(self):
        cases = [
            (
                "maintenance",
                (
                    "【维保通告】状态：开始\n"
                    "【名称】EA118机房B楼过滤网维护\n"
                    "【时间】2026-06-18 09:00~2026-06-18 18:00\n"
                    "【位置】B楼空调间\n"
                    "【内容】更换过滤网\n"
                    "【原因】周期维保\n"
                    "【影响】无影响\n"
                    "【进度】准备完成"
                ),
                {"location": "B楼空调间", "content": "更换过滤网", "reason": "周期维保", "impact": "无影响", "progress": "准备完成"},
            ),
            (
                "repair",
                (
                    "【设备检修】状态：更新\n"
                    "【标题】EA118_C01机房D楼直流屏系统总故障告警检修\n"
                    "【地点】D-178配电室\n"
                    "【紧急程度】低\n"
                    "【专业】电气\n"
                    "【发现故障时间】2026-06-18 10:44\n"
                    "【期望完成时间】2026-06-18 23:50\n"
                    "【维修设备】D-178-AD001\n"
                    "【维修故障】直流屏系统总故障\n"
                    "【故障类型】设备故障\n"
                    "【维修方式】自维\n"
                    "【影响范围】无影响\n"
                    "【故障发现方式】告警发现\n"
                    "【故障现象】系统总故障\n"
                    "【故障原因】BMS告警\n"
                    "【解决方案】检查直流屏\n"
                    "【备件更换情况】无\n"
                    "【完成情况】处理中"
                ),
                {"location": "D-178配电室", "repair_device": "D-178-AD001", "fault_type": "设备故障", "repair_mode": "自维", "discovery": "告警发现", "symptom": "系统总故障", "progress": "处理中"},
            ),
            (
                "power",
                (
                    "【上电通告】状态：开始\n"
                    "【名称】EA118机房E楼设备上电通告\n"
                    "【时间】2026-06-18 09:00~2026-06-18 18:00\n"
                    "【柜号】E-201 B01\n"
                    "【数量】2个\n"
                    "【进度】准备上电"
                ),
                {"cabinet": "E-201 B01", "quantity": "2个", "progress": "准备上电"},
            ),
            (
                "polling",
                (
                    "【设备轮巡】状态：开始\n"
                    "【标题】EA118机房C楼制冷单元轮巡通告\n"
                    "【时间】2026-06-18 09:00~2026-06-18 18:00\n"
                    "【设备】C-127制冷单元\n"
                    "【内容】3号轮巡至2号运行\n"
                    "【影响】无影响\n"
                    "【进度】准备完成"
                ),
                {"device": "C-127制冷单元", "content": "3号轮巡至2号运行", "impact": "无影响", "progress": "准备完成"},
            ),
            (
                "adjust",
                (
                    "【设备调整】状态：开始\n"
                    "【名称】EA118机房H楼空调调整通告\n"
                    "【时间】2026-06-18 09:00~2026-06-18 18:00\n"
                    "【位置】H-440空调间\n"
                    "【内容】调整空调参数\n"
                    "【原因】环境优化\n"
                    "【影响】无影响\n"
                    "【进度】准备完成"
                ),
                {"location": "H-440空调间", "content": "调整空调参数", "reason": "环境优化", "impact": "无影响", "progress": "准备完成"},
            ),
        ]
        for work_type, text, expected in cases:
            with self.subTest(work_type=work_type):
                fields = FastAPIPortalController._projected_notice_fields_from_text(text)
                self.assertEqual(fields["work_type"], work_type)
                for key, value in expected.items():
                    self.assertEqual(fields.get(key), value)

    def test_clipboard_projection_keeps_full_change_fields_and_normalizes_heading(self):
        current_month = dt.datetime.now().strftime("%Y-%m")
        text = (
            "【变更通告】状态：开始\n"
            "【名称】EA118机房A楼蓄电池测试变更\n"
            "【等级】I3\n"
            f"【时间】{current_month}-18 09:00~{current_month}-18 18:00\n"
            "【位置】A-245配电室\n"
            "【内容】工程师对蓄电池进行测试\n"
            "【原因】容量测试\n"
            "【影响】对IT业务无影响\n"
            "【进度】准备工作已完成"
        )
        with tempfile.TemporaryDirectory() as tmp:
            original_store = PortalRuntime.state_store
            PortalRuntime.state_store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            try:
                entry = FastAPIPortalController._clipboard_entry_from_content(text)
                self.assertIsNotNone(entry)
                result = FastAPIPortalController._project_clipboard_entry_to_active(entry or {})
                payload = result["item"]["payload"]
                self.assertEqual(payload["notice_type"], "变更通告")
                self.assertEqual(payload["work_type"], "change")
                self.assertEqual(payload["location"], "A-245配电室")
                self.assertEqual(payload["content"], "工程师对蓄电池进行测试")
                self.assertEqual(payload["reason"], "容量测试")
                self.assertEqual(payload["impact"], "对IT业务无影响")
                self.assertEqual(payload["progress"], "准备工作已完成")
                self.assertEqual(payload["start_time"], f"{current_month}-18 09:00")
                self.assertEqual(payload["end_time"], f"{current_month}-18 18:00")

                ongoing = FastAPIPortalController._get_ongoing("A")
                self.assertEqual(len(ongoing), 1)
                self.assertEqual(ongoing[0]["location"], "A-245配电室")
                self.assertEqual(ongoing[0]["content"], "工程师对蓄电池进行测试")
                self.assertEqual(ongoing[0]["impact"], "对IT业务无影响")
                self.assertEqual(ongoing[0]["progress"], "准备工作已完成")
            finally:
                PortalRuntime.state_store = original_store

    def test_event_clipboard_projection_reuses_existing_target_record_by_event_identity(self):
        current_month = dt.datetime.now().strftime("%Y-%m")
        first_text = (
            "【事件通告】状态：开始\n"
            "【标题】D楼直流屏系统总故障\n"
            f"【时间】{current_month}-24 10:00\n"
            "【机楼】D楼\n"
            "【来源】BMS\n"
            "【等级】I2"
        )
        update_text = (
            "【事件通告】状态：更新\n"
            "【标题】D楼直流屏系统总故障\n"
            f"【时间】{current_month}-24 10:00\n"
            "【机楼】D楼\n"
            "【来源】BMS\n"
            "【等级】I2\n"
            "【进展】处理中"
        )
        with tempfile.TemporaryDirectory() as tmp:
            original_store = PortalRuntime.state_store
            PortalRuntime.state_store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            try:
                PortalRuntime.state_store.upsert_qt_active_item(
                    {
                        "active_item_id": "event-active-1",
                        "record_id": "rec-event-target",
                        "target_record_id": "rec-event-target",
                        "notice_type": "事件通告",
                        "work_type": "event",
                        "title": "D楼直流屏系统总故障",
                        "text": first_text,
                        "_is_placeholder_record": False,
                    },
                    section="event",
                    origin="clipboard",
                )

                entry = FastAPIPortalController._clipboard_entry_from_content(update_text)
                self.assertIsNotNone(entry)
                result = FastAPIPortalController._project_clipboard_entry_to_active(entry or {})

                self.assertEqual(result["active_item_id"], "event-active-1")
                self.assertEqual(result["record_id"], "rec-event-target")
                items = PortalRuntime.state_store.list_qt_active_items()
                self.assertEqual(len(items), 1)
                self.assertEqual(items[0]["active_item_id"], "event-active-1")
                self.assertEqual(items[0]["record_id"], "rec-event-target")
                payload = items[0]["payload"]
                self.assertEqual(payload["target_record_id"], "rec-event-target")
                self.assertIn("状态：更新", payload["text"])
            finally:
                PortalRuntime.state_store = original_store

    def test_qt_upload_result_binds_backend_active_item_before_next_update_projection(self):
        current_month = dt.datetime.now().strftime("%Y-%m")
        first_text = (
            "【事件通告】状态：开始\n"
            "【标题】D楼直流屏系统总故障\n"
            f"【时间】{current_month}-24 10:00\n"
            "【机楼】D楼\n"
            "【来源】BMS\n"
            "【等级】I2"
        )
        update_text = (
            "【事件通告】状态：更新\n"
            "【标题】D楼直流屏系统总故障\n"
            f"【时间】{current_month}-24 10:00\n"
            "【机楼】D楼\n"
            "【来源】BMS\n"
            "【等级】I2\n"
            "【进展】处理中"
        )
        with tempfile.TemporaryDirectory() as tmp:
            original_store = PortalRuntime.state_store
            original_service = PortalRuntime.service
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            service = MaintenancePortalService()
            service._state_store = store
            PortalRuntime.state_store = store
            PortalRuntime.service = service
            try:
                store.upsert_qt_active_item(
                    {
                        "active_item_id": "event-active-1",
                        "record_id": "local_event_active_1",
                        "target_record_id": "",
                        "notice_type": "事件通告",
                        "work_type": "event",
                        "title": "D楼直流屏系统总故障",
                        "text": first_text,
                        "_is_placeholder_record": True,
                    },
                    section="event",
                    origin="clipboard",
                )
                service._jobs["qt-job-1"] = {
                    "job_id": "qt-job-1",
                    "phase": "uploading",
                    "prepared": {
                        "action": "start",
                        "active_item_id": "event-active-1",
                        "record_id": "local_event_active_1",
                        "notice_type": "事件通告",
                        "work_type": "event",
                        "title": "D楼直流屏系统总故障",
                        "text": first_text,
                    },
                }

                controller = FastAPIPortalController()
                controller.mark_job_upload_result(
                    "qt-job-1",
                    success=True,
                    message="rec-event-target",
                    record_id="rec-event-target",
                    active_item_id="event-active-1",
                )

                items = store.list_qt_active_items()
                self.assertEqual(len(items), 1)
                self.assertEqual(items[0]["record_id"], "rec-event-target")
                payload = items[0]["payload"]
                self.assertEqual(payload["record_id"], "rec-event-target")
                self.assertEqual(payload["target_record_id"], "rec-event-target")
                self.assertFalse(payload["_is_placeholder_record"])

                entry = FastAPIPortalController._clipboard_entry_from_content(update_text)
                self.assertIsNotNone(entry)
                result = FastAPIPortalController._project_clipboard_entry_to_active(entry or {})
                self.assertEqual(result["active_item_id"], "event-active-1")
                self.assertEqual(result["record_id"], "rec-event-target")
                payload = result["item"]["payload"]
                self.assertEqual(payload["target_record_id"], "rec-event-target")
                self.assertNotIn("local_event_active_1", payload["record_id"])
            finally:
                PortalRuntime.state_store = original_store
                PortalRuntime.service = original_service

    def test_local_qt_upload_remember_target_updates_backend_active_item(self):
        current_month = dt.datetime.now().strftime("%Y-%m")
        first_text = (
            "【事件通告】状态：开始\n"
            "【标题】D楼直流屏系统总故障\n"
            f"【时间】{current_month}-24 10:00\n"
            "【机楼】D楼\n"
            "【来源】BMS\n"
            "【等级】I2"
        )
        update_text = (
            "【事件通告】状态：更新\n"
            "【标题】D楼直流屏系统总故障\n"
            f"【时间】{current_month}-24 10:00\n"
            "【机楼】D楼\n"
            "【来源】BMS\n"
            "【等级】I2\n"
            "【进展】处理中"
        )
        with tempfile.TemporaryDirectory() as tmp:
            original_store = PortalRuntime.state_store
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            PortalRuntime.state_store = store
            try:
                store.upsert_qt_active_item(
                    {
                        "active_item_id": "event-active-1",
                        "record_id": "local_event_active_1",
                        "target_record_id": "",
                        "notice_type": "事件通告",
                        "work_type": "event",
                        "title": "D楼直流屏系统总故障",
                        "text": first_text,
                        "_is_placeholder_record": True,
                    },
                    section="event",
                    origin="clipboard",
                )

                PortalRuntime._remember_local_upload_target(
                    {
                        "active_item_id": "event-active-1",
                        "record_id": "local_event_active_1",
                        "notice_type": "事件通告",
                        "work_type": "event",
                        "title": "D楼直流屏系统总故障",
                        "text": first_text,
                        "_is_placeholder_record": True,
                    },
                    notice_type="事件通告",
                    target_record_id="rec-event-target",
                )

                items = store.list_qt_active_items()
                self.assertEqual(len(items), 1)
                self.assertEqual(items[0]["record_id"], "rec-event-target")
                self.assertEqual(items[0]["payload"]["target_record_id"], "rec-event-target")

                entry = FastAPIPortalController._clipboard_entry_from_content(update_text)
                self.assertIsNotNone(entry)
                result = FastAPIPortalController._project_clipboard_entry_to_active(entry or {})
                self.assertEqual(result["active_item_id"], "event-active-1")
                self.assertEqual(result["record_id"], "rec-event-target")
            finally:
                PortalRuntime.state_store = original_store

    def test_event_clipboard_projection_recovers_target_from_identity_map(self):
        current_month = dt.datetime.now().strftime("%Y-%m")
        first_text = (
            "【事件通告】状态：新增\n"
            "【标题】D楼直流屏系统总故障\n"
            f"【时间】{current_month}-24 10:00\n"
            "【机楼】D楼\n"
            "【来源】BMS\n"
            "【等级】I2"
        )
        update_text = (
            "【事件通告】状态：更新\n"
            "【标题】D楼直流屏系统总故障\n"
            f"【时间】{current_month}-24 10:00\n"
            "【机楼】D楼\n"
            "【来源】BMS\n"
            "【等级】I2\n"
            "【进展】处理中"
        )
        with tempfile.TemporaryDirectory() as tmp:
            original_store = PortalRuntime.state_store
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            PortalRuntime.state_store = store
            try:
                store.upsert_notice_identity(
                    {
                        "active_item_id": "event-active-1",
                        "record_id": "rec-event-target",
                        "target_record_id": "rec-event-target",
                        "notice_type": "事件通告",
                        "work_type": "event",
                        "title": "D楼直流屏系统总故障",
                        "text": first_text,
                    },
                    origin="qt_upload",
                )

                entry = FastAPIPortalController._clipboard_entry_from_content(update_text)
                self.assertIsNotNone(entry)
                result = FastAPIPortalController._project_clipboard_entry_to_active(entry or {})

                self.assertFalse(result.get("ignored"))
                self.assertEqual(result["record_id"], "rec-event-target")
                payload = result["item"]["payload"]
                self.assertEqual(payload["target_record_id"], "rec-event-target")
                self.assertFalse(payload["_is_placeholder_record"])
            finally:
                PortalRuntime.state_store = original_store

    def test_record_not_found_variants_are_treated_as_missing_remote_record(self):
        self.assertTrue(PortalRuntime._remote_record_not_found("1254043-RecordIdNotFound"))
        self.assertTrue(PortalRuntime._remote_record_not_found("1254043-RecordldNotFo"))

    def test_sparse_qt_active_payload_is_backfilled_from_notice_text(self):
        current_month = dt.datetime.now().strftime("%Y-%m")
        text = (
            "【变更通告】状态：开始\n"
            "【名称】EA118机房A楼冷源设备变更\n"
            f"【时间】{current_month}-18 09:00~{current_month}-18 18:00\n"
            "【位置】A-127冷站\n"
            "【内容】调整冷源设备\n"
            "【原因】运行优化\n"
            "【影响】无业务影响\n"
            "【进度】执行中"
        )
        with tempfile.TemporaryDirectory() as tmp:
            original_store = PortalRuntime.state_store
            PortalRuntime.state_store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            try:
                PortalRuntime.state_store.upsert_qt_active_item(
                    {
                        "active_item_id": "sparse-change-1",
                        "record_id": "local_sparse-change-1",
                        "notice_type": "变更通告",
                        "work_type": "change",
                        "title": "EA118机房A楼冷源设备变更",
                        "text": text,
                    },
                    origin="clipboard",
                )
                ongoing = FastAPIPortalController._get_ongoing("A")
                self.assertEqual(len(ongoing), 1)
                item = ongoing[0]
                self.assertEqual(item["notice_type"], "变更通告")
                self.assertEqual(item["location"], "A-127冷站")
                self.assertEqual(item["content"], "调整冷源设备")
                self.assertEqual(item["reason"], "运行优化")
                self.assertEqual(item["impact"], "无业务影响")
                self.assertEqual(item["progress"], "执行中")
            finally:
                PortalRuntime.state_store = original_store

    def test_bootstrap_clipboard_candidates_are_acknowledged_not_reprojected(self):
        harness = _Harness()

        harness._consume_qt_shell_bootstrap_state(
            {
                "clipboard_candidates": [
                    {
                        "candidate_id": "cand-1",
                        "content": "【维保通告】状态：开始\n\n【标题】测试",
                    }
                ],
                "dialog_sessions": [{"session_id": "dlg-1"}],
            }
        )

        self.assertFalse(harness.reproject_called)
        self.assertEqual(
            harness.lan_template_portal_controller.acks,
            [{"candidate_id": "cand-1", "ok": True, "status": "backend_projected"}],
        )
        self.assertEqual(harness._qt_shell_dialog_sessions[0]["session_id"], "dlg-1")

    def test_clipboard_projection_response_is_applied_directly(self):
        harness = _Harness()

        result = harness._apply_clipboard_projection_result(
            {
                "projection": {
                    "ok": True,
                    "item": {
                        "active_item_id": "active-1",
                        "record_id": "local_active-1",
                        "payload": {
                            "active_item_id": "active-1",
                            "record_id": "local_active-1",
                            "notice_type": "维保通告",
                            "text": "【维保通告】状态：开始\n\n【标题】测试",
                        },
                    },
                }
            }
        )

        self.assertTrue(result["ok"])
        self.assertEqual(len(harness.applied_projection_payloads), 1)
        self.assertEqual(
            harness.applied_projection_payloads[0]["item"]["active_item_id"],
            "active-1",
        )

    def test_sqlite_clipboard_fallback_events_are_projected_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            store.append_event(
                "clipboard",
                {
                    "content": "【维保通告】状态：开始\n\n【标题】A楼测试测试测试",
                    "ts": 1779660000000,
                },
            )
            harness = _ClipboardHarness(store, Path(tmp) / "clipboard.jsonl")

            harness._poll_clipboard_event_file()
            harness._poll_clipboard_event_file()

            self.assertEqual(len(harness.lan_template_portal_controller.clipboard_events), 1)
            self.assertEqual(
                harness.lan_template_portal_controller.clipboard_events[0]["source"],
                "clipboard_sqlite_fallback",
            )
            self.assertEqual(len(harness.projections), 1)

    def test_sqlite_clipboard_bad_event_does_not_block_later_events(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            bad_text = "【维保通告】状态：开始\n\n【标题】坏事件"
            good_text = "【维保通告】状态：开始\n\n【标题】后续好事件"
            store.append_event("clipboard", {"content": bad_text, "ts": 1})
            store.append_event("clipboard", {"content": good_text, "ts": 2})
            harness = _ClipboardHarness(store, Path(tmp) / "clipboard.jsonl")
            harness._clipboard_sqlite_event_max_failures = 2
            harness.lan_template_portal_controller.fail_contents.add(bad_text)

            harness._poll_clipboard_event_file()
            harness._poll_clipboard_event_file()
            harness._poll_clipboard_event_file()

            self.assertEqual(
                [event["content"] for event in harness.lan_template_portal_controller.clipboard_events],
                [good_text],
            )
            self.assertEqual(harness._clipboard_sqlite_last_event_id, 2)
            self.assertTrue(harness.failures)


if __name__ == "__main__":
    unittest.main()
