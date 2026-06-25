import sys
import tempfile
import unittest
from pathlib import Path


BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from upload_event_module.ui.main_window_runtime import MainWindowRuntimeMixin  # noqa: E402
from upload_event_module.ui.main_window_clipboard import MainWindowClipboardMixin  # noqa: E402
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


class QtShellBackendEventTests(unittest.TestCase):
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
        text = (
            "【变更通告】状态：开始\n"
            "【名称】EA118机房A楼蓄电池测试变更\n"
            "【等级】I3\n"
            "【时间】2026-06-18 09:00~2026-06-18 18:00\n"
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
                self.assertEqual(payload["start_time"], "2026-06-18 09:00")
                self.assertEqual(payload["end_time"], "2026-06-18 18:00")

                ongoing = FastAPIPortalController._get_ongoing("A")
                self.assertEqual(len(ongoing), 1)
                self.assertEqual(ongoing[0]["location"], "A-245配电室")
                self.assertEqual(ongoing[0]["content"], "工程师对蓄电池进行测试")
                self.assertEqual(ongoing[0]["impact"], "对IT业务无影响")
                self.assertEqual(ongoing[0]["progress"], "准备工作已完成")
            finally:
                PortalRuntime.state_store = original_store

    def test_event_clipboard_projection_reuses_existing_target_record_by_title(self):
        first_text = (
            "【事件通告】状态：开始\n"
            "【标题】D楼直流屏系统总故障\n"
            "【时间】2026-06-24 10:00"
        )
        update_text = (
            "【事件通告】状态：更新\n"
            "【标题】D楼直流屏系统总故障\n"
            "【时间】2026-06-24 10:30"
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
        first_text = (
            "【事件通告】状态：开始\n"
            "【标题】D楼直流屏系统总故障\n"
            "【时间】2026-06-24 10:00"
        )
        update_text = (
            "【事件通告】状态：更新\n"
            "【标题】D楼直流屏系统总故障\n"
            "【时间】2026-06-24 10:30"
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
        first_text = (
            "【事件通告】状态：开始\n"
            "【标题】D楼直流屏系统总故障\n"
            "【时间】2026-06-24 10:00"
        )
        update_text = (
            "【事件通告】状态：更新\n"
            "【标题】D楼直流屏系统总故障\n"
            "【时间】2026-06-24 10:30"
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

    def test_sparse_qt_active_payload_is_backfilled_from_notice_text(self):
        text = (
            "【变更通告】状态：开始\n"
            "【名称】EA118机房A楼冷源设备变更\n"
            "【时间】2026-06-18 09:00~2026-06-18 18:00\n"
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
