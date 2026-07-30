import sys
import tempfile
import time
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

    def test_source_table_snapshots_advance_independently(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            maintenance_v1 = store.replace_source_table_snapshot(
                "maintenance",
                {
                    "A": [
                        {
                            "record_id": "maintenance-v1",
                            "work_type": "maintenance",
                        }
                    ]
                },
            )
            change_v1 = store.replace_source_table_snapshot(
                "change",
                {
                    "A": [
                        {
                            "record_id": "change-v1",
                            "work_type": "change",
                        }
                    ]
                },
            )
            maintenance_v2 = store.replace_source_table_snapshot(
                "maintenance",
                {
                    "A": [
                        {
                            "record_id": "maintenance-v2",
                            "work_type": "maintenance",
                        }
                    ]
                },
            )
            store.record_failed_source_table_snapshot(
                "change",
                error="change unavailable",
            )

            maintenance = store.get_source_table_scope_snapshot(
                "maintenance", "A"
            )
            change = store.get_source_table_scope_snapshot("change", "A")
            combined = store.get_source_scope_snapshot("A")
            active_meta = store.active_source_snapshot_meta()
            work_type_stats = store.source_snapshot_work_type_stats()

            self.assertNotEqual(
                maintenance_v1["snapshot_id"],
                maintenance_v2["snapshot_id"],
            )
            self.assertEqual(
                maintenance["snapshot_id"],
                maintenance_v2["snapshot_id"],
            )
            self.assertEqual(change["snapshot_id"], change_v1["snapshot_id"])
            self.assertEqual(
                {item["record_id"] for item in combined["records"]},
                {"maintenance-v2", "change-v1"},
            )
            self.assertEqual(
                active_meta["source_versions"]["change"]["snapshot_id"],
                change_v1["snapshot_id"],
            )
            self.assertEqual(
                work_type_stats["snapshot_ids"]["maintenance"],
                maintenance_v2["snapshot_id"],
            )
            self.assertEqual(
                work_type_stats["snapshot_ids"]["change"],
                change_v1["snapshot_id"],
            )
            self.assertEqual(work_type_stats["totals"]["maintenance"], 1)
            self.assertEqual(work_type_stats["totals"]["change"], 1)

    def test_notice_remote_operation_replays_across_store_reopen(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "state.sqlite3"
            store = LanPortalStateStore(db_path)
            created = store.begin_notice_remote_operation(
                operation_id="operation-1",
                operation_type="update",
                lock_key="maintenance:target:rec-1",
                request={"action": "update", "target_record_id": "rec-1"},
                target_record_id="rec-1",
                expected_record_version="version-1",
            )
            self.assertTrue(created["created"])
            store.mark_notice_remote_operation(
                "operation-1",
                status="remote_written",
                target_record_id="rec-1",
                observed_record_version="version-2",
                result={"record_id": "rec-1"},
            )

            reopened = LanPortalStateStore(db_path)
            replay = reopened.begin_notice_remote_operation(
                operation_id="operation-1",
                operation_type="update",
                lock_key="maintenance:target:rec-1",
                request={"action": "update", "target_record_id": "rec-1"},
                target_record_id="rec-1",
                expected_record_version="version-1",
            )
            conflict = reopened.begin_notice_remote_operation(
                operation_id="operation-1",
                operation_type="update",
                lock_key="maintenance:target:rec-1",
                request={"action": "end", "target_record_id": "rec-1"},
                target_record_id="rec-1",
            )

            self.assertFalse(replay["created"])
            self.assertTrue(replay["replay"])
            self.assertEqual(replay["status"], "remote_written")
            self.assertEqual(replay["observed_record_version"], "version-2")
            self.assertTrue(conflict["conflict"])

    def test_notice_remote_operation_cleanup_preserves_recoverable_states(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            for operation_id in (
                "operation-completed",
                "operation-failed",
                "operation-executing",
            ):
                store.begin_notice_remote_operation(
                    operation_id=operation_id,
                    operation_type="update",
                    request={"operation_id": operation_id},
                )
            store.mark_notice_remote_operation(
                "operation-completed",
                status="completed",
            )
            store.mark_notice_remote_operation(
                "operation-failed",
                status="failed",
            )
            store.mark_notice_remote_operation(
                "operation-executing",
                status="executing",
            )

            removed = store.cleanup_notice_remote_operations(
                completed_before=time.time() + 1,
                failed_before=time.time() + 1,
            )

            self.assertEqual(removed, 2)
            self.assertIsNone(
                store.get_notice_remote_operation("operation-completed")
            )
            self.assertIsNone(
                store.get_notice_remote_operation("operation-failed")
            )
            self.assertEqual(
                store.get_notice_remote_operation("operation-executing")[
                    "status"
                ],
                "executing",
            )

    def test_repair_snapshot_page_filters_before_decoding_payloads(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            store.replace_repair_snapshot(
                "repair_projects",
                app_token="app",
                table_id="table",
                records=[
                    {
                        "record_id": "rec_a_new",
                        "scope_codes": ["A"],
                        "title": "A楼新项目",
                        "status": "维修中",
                        "search_text": "A楼 新项目 暖通",
                        "sort_time": 4,
                        "payload": {
                            "record_id": "rec_a_new",
                            "last_modified_time": "4",
                        },
                    },
                    {
                        "record_id": "rec_a_old",
                        "scope_codes": ["A"],
                        "title": "A楼旧项目",
                        "status": "维修中",
                        "search_text": "A楼 旧项目 电气",
                        "sort_time": 2,
                        "payload": {
                            "record_id": "rec_a_old",
                            "last_modified_time": "2",
                        },
                    },
                    {
                        "record_id": "rec_a_done",
                        "scope_codes": ["A"],
                        "title": "A楼完成项目",
                        "status": "维修完成",
                        "search_text": "A楼 完成项目",
                        "sort_time": 5,
                        "payload": {
                            "record_id": "rec_a_done",
                            "last_modified_time": "5",
                        },
                    },
                    {
                        "record_id": "rec_b",
                        "scope_codes": ["B"],
                        "title": "B楼项目",
                        "status": "维修中",
                        "search_text": "B楼 项目",
                        "sort_time": 3,
                        "payload": {
                            "record_id": "rec_b",
                            "last_modified_time": "3",
                        },
                    },
                ],
            )

            page = store.query_repair_snapshot_page(
                "repair_projects",
                scope="A",
                excluded_statuses=["维修完成"],
                limit=1,
                focus_record_id="rec_a_old",
            )
            self.assertEqual(page["total"], 2)
            self.assertEqual(page["offset"], 1)
            self.assertEqual(
                [item["record_id"] for item in page["records"]],
                ["rec_a_old"],
            )

            searched = store.query_repair_snapshot_page(
                "repair_projects",
                scope="A",
                query="暖通",
                excluded_statuses=["维修完成"],
            )
            self.assertEqual(searched["total"], 1)
            self.assertEqual(searched["records"][0]["record_id"], "rec_a_new")

            completed = store.query_repair_snapshot_page(
                "repair_projects",
                scope="A",
                included_statuses=["维修完成"],
            )
            self.assertEqual(completed["total"], 1)
            self.assertEqual(completed["records"][0]["record_id"], "rec_a_done")

    def test_repair_snapshot_page_sorts_numeric_times_numerically(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            store.replace_repair_snapshot(
                "repair_projects",
                records=[
                    {
                        "record_id": "rec_9",
                        "scope_codes": ["A"],
                        "sort_time": 9,
                        "payload": {
                            "record_id": "rec_9",
                            "last_modified_time": "9",
                        },
                    },
                    {
                        "record_id": "rec_10",
                        "scope_codes": ["A"],
                        "sort_time": 10,
                        "payload": {
                            "record_id": "rec_10",
                            "last_modified_time": "10",
                        },
                    },
                ],
            )

            page = store.query_repair_snapshot_page(
                "repair_projects",
                scope="A",
            )

            self.assertEqual(
                [item["record_id"] for item in page["records"]],
                ["rec_10", "rec_9"],
            )

    def test_repair_snapshot_parent_counts_only_returns_known_parents(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            store.replace_repair_snapshot(
                "repair_followups",
                records=[
                    {
                        "record_id": "followup_1",
                        "parent_record_id": "project_1",
                        "payload": {"record_id": "followup_1"},
                    },
                    {
                        "record_id": "followup_2",
                        "parent_record_id": "project_1",
                        "payload": {"record_id": "followup_2"},
                    },
                    {
                        "record_id": "followup_3",
                        "parent_record_id": "project_2",
                        "payload": {"record_id": "followup_3"},
                    },
                ],
            )

            self.assertEqual(
                store.repair_snapshot_parent_counts(
                    "repair_followups",
                    ["project_1", "project_missing"],
                ),
                {"project_1": 2},
            )


if __name__ == "__main__":
    unittest.main()
