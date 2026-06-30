import sys
import threading
import unittest
from pathlib import Path


BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal.portal_service import MaintenancePortalService  # noqa: E402
from lan_bitable_template_portal.portal_service import PortalError  # noqa: E402
from lan_bitable_template_portal.server import PortalRuntime  # noqa: E402
from lan_bitable_template_portal import workbench_lite  # noqa: E402
from upload_event_module.config import EVENT_NOTICE_FIELDS  # noqa: E402
from upload_event_module.core.parser import extract_event_info  # noqa: E402


class NoticeIdentityBoundaryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = object.__new__(MaintenancePortalService)

    def test_source_record_id_is_not_treated_as_target_record_id(self) -> None:
        payload = {
            "action": "update",
            "source_record_id": "recSource123",
            "target_record_id": "recSource123",
            "record_id": "recSource123",
        }

        result = self.service._target_record_id_from_request_payload(
            payload,
            source_record_id="recSource123",
            action="update",
        )

        self.assertEqual(result, "")

    def test_explicit_target_record_id_is_preserved(self) -> None:
        payload = {
            "action": "end",
            "source_record_id": "recSource123",
            "target_record_id": "recTarget456",
            "record_id": "recSource123",
        }

        result = self.service._target_record_id_from_request_payload(
            payload,
            source_record_id="recSource123",
            action="end",
        )

        self.assertEqual(result, "recTarget456")

    def test_record_id_alias_can_supply_target_for_update(self) -> None:
        payload = {
            "action": "update",
            "source_record_id": "recSource123",
            "record_id": "recTarget456",
        }

        result = self.service._target_record_id_from_request_payload(
            payload,
            source_record_id="recSource123",
            action="update",
        )

        self.assertEqual(result, "recTarget456")

    def test_local_record_id_alias_is_ignored(self) -> None:
        payload = {
            "action": "update",
            "source_record_id": "recSource123",
            "record_id": "localid-abc",
        }

        result = self.service._target_record_id_from_request_payload(
            payload,
            source_record_id="recSource123",
            action="update",
        )

        self.assertEqual(result, "")

    def test_command_expansion_rejects_source_record_as_update_target(self) -> None:
        payload = {
            "command_format": "notice_command",
            "action": "update",
            "scope": "C",
            "work_type": "maintenance",
            "source_record_id": "recSource123",
            "record_id": "recSource123",
            "patch": {
                "title": "测试维保",
                "source_record_id": "recSource123",
                "record_id": "recSource123",
            },
        }

        with self.assertRaises(PortalError):
            self.service.expand_workbench_action_command(
                payload,
                scope="C",
                ongoing_items=[],
            )

    def test_command_expansion_preserves_explicit_target_record(self) -> None:
        payload = {
            "command_format": "notice_command",
            "action": "update",
            "scope": "C",
            "work_type": "maintenance",
            "source_record_id": "recSource123",
            "target_record_id": "recTarget456",
            "record_id": "recSource123",
            "patch": {
                "title": "测试维保",
                "source_record_id": "recSource123",
                "record_id": "recSource123",
            },
        }

        result = self.service.expand_workbench_action_command(
            payload,
            scope="C",
            ongoing_items=[],
        )

        self.assertEqual(result.get("source_record_id"), "recSource123")
        self.assertEqual(result.get("target_record_id"), "recTarget456")
        self.assertEqual(result.get("record_id"), "recTarget456")

    def test_existing_site_photo_count_satisfies_end_check(self) -> None:
        class EmptyStateStore:
            def list_qt_active_items(self, include_deleted: bool = False) -> list[dict]:
                return []

        self.service._summary_lock = threading.RLock()
        self.service._state_store = EmptyStateStore()
        self.service._load_work_status_items_locked = lambda scope: [
            {
                "work_type": "maintenance",
                "target_record_id": "recTarget456",
                "record_id": "recTarget456",
                "site_photo_count": "2",
            }
        ]

        self.service._require_end_site_photo_cumulative(
            {
                "work_type": "maintenance",
                "target_record_id": "recTarget456",
                "record_id": "recTarget456",
            },
            "end",
            work_type="maintenance",
        )

    def test_workbench_lite_preserves_site_photo_upload_id(self) -> None:
        workbench_lite = (BIN_DIR / "lan_bitable_template_portal" / "workbench_lite.py").read_text(
            encoding="utf-8",
            errors="ignore",
        )

        self.assertIn("'upload_id', 'file_token', 'token'", workbench_lite)

    def test_qt_runtime_accepts_site_photos_alias_and_count(self) -> None:
        self.assertTrue(
            PortalRuntime._has_extra_images_payload(
                {"site_photos": [{"upload_id": "upload123"}]}
            )
        )
        self.assertTrue(PortalRuntime._has_extra_images_payload({"site_photo_count": "1"}))

    def test_qt_runtime_records_uploaded_site_photo_count(self) -> None:
        server_text = (BIN_DIR / "lan_bitable_template_portal" / "server.py").read_text(
            encoding="utf-8",
            errors="ignore",
        )

        self.assertIn('extra_images = payload.get("site_photos")', server_text)
        self.assertIn('data["site_photo_count"] = max(previous_count, uploaded_site_photo_count)', server_text)

    def test_notice_type_is_projected_as_matching_ongoing_work_type(self) -> None:
        self.service._is_ongoing_hidden = lambda item: False
        self.service._normalize_scope = MaintenancePortalService._normalize_scope
        self.service._scope_matches_item = MaintenancePortalService._scope_matches_item
        cases = [
            ("维保通告", "maintenance", "EA118机房E楼维保通告"),
            ("变更通告", "change", "EA118机房E楼变更通告"),
            ("设备检修", "repair", "EA118机房E楼检修通告"),
            ("下电通告", "power", "EA118机房E楼测试下电通告"),
            ("设备轮巡", "polling", "EA118机房E楼制冷单元轮巡通告"),
            ("设备调整", "adjust", "EA118机房E楼空调调整通告"),
        ]
        for notice_type, expected_work_type, title in cases:
            with self.subTest(notice_type=notice_type):
                item = {
                    "notice_type": notice_type,
                    "title": title,
                    "building": "E楼",
                    "building_codes": ["E"],
                    "record_id": f"rec-{expected_work_type}",
                    "target_record_id": f"rec-{expected_work_type}",
                    "active_item_id": f"active-{expected_work_type}",
                }

                projected = self.service._project_ongoing_items("E", [item])
                counts = MaintenancePortalService._work_type_counts(projected)

                self.assertEqual(projected[0]["work_type"], expected_work_type)
                self.assertEqual(counts[expected_work_type], 1)

    def test_workbench_lite_infers_work_type_from_notice_type(self) -> None:
        cases = [
            ("维保通告", "maintenance"),
            ("变更通告", "change"),
            ("设备检修", "repair"),
            ("上电通告", "power"),
            ("下电通告", "power"),
            ("设备轮巡", "polling"),
            ("设备调整", "adjust"),
        ]
        for notice_type, expected_work_type in cases:
            with self.subTest(notice_type=notice_type):
                self.assertEqual(
                    workbench_lite._item_work_type({"notice_type": notice_type}),
                    expected_work_type,
                )

    def test_event_identity_requires_event_time(self) -> None:
        base_text = (
            "【事件通告】状态：新增\n"
            "【标题】BMS报E-217-CRAC-02压缩机高压报警: 告警\n"
            "【时间】{time}\n"
            "【进展】测试"
        )
        first = {"notice_type": "事件通告", "text": base_text.format(time="2026-06-24 17:20")}
        second = {"notice_type": "事件通告", "text": base_text.format(time="2026-06-24 18:20")}

        self.assertNotEqual(
            PortalRuntime._event_notice_identity_key(first),
            PortalRuntime._event_notice_identity_key(second),
        )
        self.assertNotEqual(
            PortalRuntime._local_upload_dedupe_key(first, "事件通告"),
            PortalRuntime._local_upload_dedupe_key(second, "事件通告"),
        )

    def test_event_title_only_does_not_build_remote_reuse_identity(self) -> None:
        payload = {
            "notice_type": "事件通告",
            "text": (
                "【事件通告】状态：新增\n"
                "【标题】BMS报E-217-CRAC-02压缩机高压报警: 告警\n"
                "【进展】测试"
            ),
        }

        self.assertEqual(PortalRuntime._event_notice_identity_key(payload), "")

    def test_event_occurrence_time_label_builds_identity(self) -> None:
        text = (
            "【事件通告】状态：新增\n"
            "【标题】BMS报E-217-CRAC-02压缩机高压报警: 告警\n"
            "【事件发生时间】2026-06-24 17:20\n"
            "【进展】测试"
        )

        info = extract_event_info(text) or {}

        self.assertEqual(info.get("time_str"), "2026-06-24 17:20")
        self.assertTrue(PortalRuntime._event_notice_identity_key({"notice_type": "事件通告", "text": text}))

    def test_event_delete_requires_remote_fields(self) -> None:
        payload = {
            "notice_type": "事件通告",
            "text": (
                "【事件通告】状态：新增\n"
                "【标题】BMS报E-217-CRAC-02压缩机高压报警: 告警\n"
                "【时间】2026-06-24 17:20\n"
                "【进展】测试"
            ),
        }

        allowed, message = PortalRuntime._validate_event_delete_target(payload, {})

        self.assertFalse(allowed)
        self.assertIn("已阻止删除", message)

    def test_event_delete_requires_same_remote_identity(self) -> None:
        payload = {
            "notice_type": "事件通告",
            "text": (
                "【事件通告】状态：新增\n"
                "【标题】BMS报E-217-CRAC-02压缩机高压报警: 告警\n"
                "【时间】2026-06-24 17:20\n"
                "【进展】测试"
            ),
        }
        remote_fields = {
            EVENT_NOTICE_FIELDS["alarm_desc"]: "BMS报E-217-CRAC-02压缩机高压报警: 告警",
            EVENT_NOTICE_FIELDS["occurrence_time"]: "2026-06-24 17:20",
        }
        other_remote_fields = {
            EVENT_NOTICE_FIELDS["alarm_desc"]: "BMS报E-217-CRAC-02压缩机高压报警: 告警",
            EVENT_NOTICE_FIELDS["occurrence_time"]: "2026-06-24 18:20",
        }

        self.assertTrue(PortalRuntime._validate_event_delete_target(payload, remote_fields)[0])
        self.assertFalse(PortalRuntime._validate_event_delete_target(payload, other_remote_fields)[0])

    def test_event_existing_target_requires_same_event_time(self) -> None:
        class FakeStateStore:
            def list_qt_active_items(self, include_deleted: bool = False) -> list[dict]:
                return [
                    {
                        "record_id": "recOldEvent",
                        "notice_type": "事件通告",
                        "payload": {
                            "notice_type": "事件通告",
                            "target_record_id": "recOldEvent",
                            "record_id": "recOldEvent",
                            "text": (
                                "【事件通告】状态：新增\n"
                                "【标题】BMS报E-217-CRAC-02压缩机高压报警: 告警\n"
                                "【时间】2026-06-24 17:20\n"
                                "【进展】测试"
                            ),
                        },
                    }
                ]

        old_state_store = PortalRuntime.state_store
        PortalRuntime.state_store = FakeStateStore()
        try:
            same_title_different_time = {
                "notice_type": "事件通告",
                "text": (
                    "【事件通告】状态：新增\n"
                    "【标题】BMS报E-217-CRAC-02压缩机高压报警: 告警\n"
                    "【时间】2026-06-24 18:20\n"
                    "【进展】测试"
                ),
            }
            same_title_same_time = {
                **same_title_different_time,
                "text": same_title_different_time["text"].replace("18:20", "17:20"),
            }

            self.assertEqual(
                PortalRuntime._existing_event_target_for_local_notice(
                    same_title_different_time,
                    "事件通告",
                ),
                "",
            )
            self.assertEqual(
                PortalRuntime._existing_event_target_for_local_notice(
                    same_title_same_time,
                    "事件通告",
                ),
                "recOldEvent",
            )
        finally:
            PortalRuntime.state_store = old_state_store


if __name__ == "__main__":
    unittest.main()
