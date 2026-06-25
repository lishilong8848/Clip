import sys
import tempfile
import unittest
from pathlib import Path


BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal.identity_utils import (  # noqa: E402
    canonical_source_record_id,
    canonical_target_record_id,
    normalize_notice_identity_payload,
)
from lan_bitable_template_portal.state_store import LanPortalStateStore  # noqa: E402


class NoticeIdentityTests(unittest.TestCase):
    def test_start_record_id_is_not_target(self):
        payload = normalize_notice_identity_payload(
            {"action": "start", "record_id": "src-1", "work_type": "maintenance"}
        )
        self.assertEqual(canonical_target_record_id(payload), "")

    def test_update_record_id_alone_is_not_target(self):
        payload = normalize_notice_identity_payload(
            {"action": "update", "record_id": "tar-1", "work_type": "change"}
        )
        self.assertEqual(canonical_target_record_id(payload), "")

    def test_explicit_target_record_id_wins_even_for_upload_action(self):
        payload = normalize_notice_identity_payload(
            {
                "action": "upload",
                "target_record_id": "tar-upload-1",
                "_is_placeholder_record": False,
            }
        )
        self.assertEqual(canonical_target_record_id(payload), "tar-upload-1")

    def test_source_marker_blocks_record_id_as_target(self):
        payload = normalize_notice_identity_payload(
            {
                "action": "update",
                "record_id": "src-1",
                "source_record_id": "src-1",
                "source_app_token": "app",
                "work_type": "repair",
            }
        )
        self.assertEqual(canonical_source_record_id(payload), "src-1")
        self.assertEqual(canonical_target_record_id(payload), "")

    def test_old_aliases_do_not_normalize_to_source_or_target(self):
        payload = normalize_notice_identity_payload(
            {
                "action": "end",
                "record_id": "local-only",
            }
        )
        self.assertEqual(canonical_source_record_id(payload), "")
        self.assertEqual(canonical_target_record_id(payload), "")

    def test_state_store_repair_backfills_identity_from_qt_active(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "state.sqlite3"
            store = LanPortalStateStore(db_path)
            store.upsert_qt_active_item(
                {
                    "active_item_id": "active-1",
                    "record_id": "tar-3",
                    "target_record_id": "tar-3",
                    "source_record_id": "src-3",
                    "work_type": "maintenance",
                    "notice_type": "维保通告",
                    "title": "测试通告",
                },
                origin="test",
            )
            identity = store.resolve_notice_identity(
                work_type="maintenance",
                active_item_id="active-1",
            )
            self.assertIsNotNone(identity)
            self.assertEqual(identity["source_record_id"], "src-3")
            self.assertEqual(identity["target_record_id"], "tar-3")


if __name__ == "__main__":
    unittest.main()
