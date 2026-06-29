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


if __name__ == "__main__":
    unittest.main()
