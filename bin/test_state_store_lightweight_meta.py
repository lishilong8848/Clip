import sys
import tempfile
import unittest
from pathlib import Path


BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal.state_store import LanPortalStateStore  # noqa: E402


class StateStoreLightweightMetaTests(unittest.TestCase):
    def test_ongoing_snapshot_meta_tracks_replace_without_loading_items(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")

            initial = store.get_ongoing_snapshot_meta()
            self.assertFalse(initial["exists"])
            self.assertEqual(initial["count"], 0)

            store.replace_ongoing_items(
                [
                    {
                        "active_item_id": "active-1",
                        "record_id": "rec-target-1",
                        "work_type": "maintenance",
                        "notice_type": "维保通告",
                        "title": "测试维保",
                        "building": "A楼",
                    }
                ]
            )

            meta = store.get_ongoing_snapshot_meta()
            self.assertTrue(meta["exists"])
            self.assertEqual(meta["count"], 1)
            self.assertTrue(meta["snapshot_id"])
            self.assertTrue(meta["hash"])
            self.assertGreater(meta["updated_at"], 0)

    def test_source_and_qt_active_meta_track_versions(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")

            source_result = store.replace_all_source_scope_snapshots(
                {
                    "A": {
                        "records": [
                            {
                                "record_id": "source-1",
                                "source_record_id": "source-1",
                                "work_type": "maintenance",
                                "title": "测试源表事项",
                            }
                        ],
                        "zhihang_records": [],
                    }
                },
                meta={"warnings": []},
            )
            source_meta = store.active_source_snapshot_meta()
            self.assertEqual(source_meta["snapshot_id"], source_result["snapshot_id"])
            self.assertGreater(source_meta["updated_at"], 0)

            self.assertTrue(
                store.upsert_qt_active_item(
                    {
                        "active_item_id": "active-1",
                        "target_record_id": "target-1",
                        "work_type": "maintenance",
                        "notice_type": "维保通告",
                    }
                )
            )
            qt_meta = store.qt_active_items_meta()
            self.assertEqual(qt_meta["active"], 1)
            self.assertEqual(qt_meta["deleted"], 0)
            self.assertGreater(qt_meta["updated_at"], 0)

            self.assertTrue(store.delete_qt_active_item(active_item_id="active-1"))
            deleted_meta = store.qt_active_items_meta()
            self.assertEqual(deleted_meta["active"], 0)
            self.assertEqual(deleted_meta["deleted"], 1)
            self.assertGreaterEqual(deleted_meta["updated_at"], qt_meta["updated_at"])


if __name__ == "__main__":
    unittest.main()
