import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path


BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal.state_store import LanPortalStateStore  # noqa: E402


class StateStoreMigrationTests(unittest.TestCase):
    def test_schema_migration_registry_is_initialized(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            health = store.schema_health()
            self.assertTrue(health["ok"], health)
            self.assertGreaterEqual(
                health["latest_migration_version"],
                LanPortalStateStore.SCHEMA_VERSION,
            )
            self.assertEqual(health["missing_tables"], [])
            self.assertEqual(health["missing_indexes"], [])

    def test_existing_sqlite_is_not_replaced_by_schema_check(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "state.sqlite3"
            store = LanPortalStateStore(db_path)
            store.put_settings({"sentinel": "keep"})
            before = store.get_settings()

            store_again = LanPortalStateStore(db_path)
            health = store_again.schema_health()
            after = store_again.get_settings()

            self.assertTrue(health["ok"], health)
            self.assertEqual(before.get("sentinel"), "keep")
            self.assertEqual(after.get("sentinel"), "keep")

    def test_recreated_database_at_same_path_is_initialized_again(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "state.sqlite3"
            store = LanPortalStateStore(db_path)
            store.put_settings({"sentinel": "old"})

            for candidate in db_path.parent.glob(f"{db_path.name}*"):
                candidate.unlink()

            store.put_settings({"sentinel": "new"})
            health = store.schema_health()

            self.assertTrue(health["ok"], health)
            self.assertEqual(store.get_settings().get("sentinel"), "new")

    def test_schema_migrations_table_is_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "state.sqlite3"
            store = LanPortalStateStore(db_path)
            first = store.schema_health()
            second = store.schema_health()
            conn = sqlite3.connect(db_path)
            try:
                count = conn.execute(
                    "SELECT COUNT(*) FROM schema_migrations"
                ).fetchone()[0]
            finally:
                conn.close()
            self.assertTrue(first["ok"], first)
            self.assertTrue(second["ok"], second)
            self.assertEqual(count, second["migration_count"])

    def test_runtime_health_report_includes_schema_and_database(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            report = store.runtime_health_report()
            self.assertTrue(report["ok"], report)
            self.assertTrue(report["schema"]["ok"], report)
            self.assertTrue(report["database"]["exists"], report)
            self.assertIn("source_snapshot", report)
            self.assertIn("repair_snapshot", report)
            self.assertIn("write_worker", report)

    def test_repair_snapshot_replace_and_incremental_updates(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            store.replace_repair_snapshot(
                "repair_projects",
                records=[
                    {
                        "record_id": "rec_project_1",
                        "title": "测试维修项目",
                        "scope_codes": ["A"],
                        "payload": {
                            "record_id": "rec_project_1",
                            "display_fields": {"维修名称": "测试维修项目"},
                        },
                    }
                ],
                fields=[{"field_name": "维修名称"}],
            )
            snapshot = store.get_repair_snapshot("repair_projects")
            self.assertTrue(snapshot["exists"], snapshot)
            self.assertEqual(snapshot["record_count"], 1)
            self.assertEqual(snapshot["records"][0]["record_id"], "rec_project_1")

            store.upsert_repair_snapshot_record(
                "repair_projects",
                "rec_project_2",
                {"record_id": "rec_project_2", "display_fields": {}},
            )
            self.assertEqual(
                store.get_repair_snapshot("repair_projects")["record_count"],
                2,
            )
            self.assertTrue(
                store.delete_repair_snapshot_record(
                    "repair_projects", "rec_project_1"
                )
            )
            self.assertEqual(store.repair_snapshot_stats()["record_count"], 1)

    def test_repair_snapshot_failure_preserves_last_good_records(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            store.replace_repair_snapshot(
                "repair_followups",
                records=[
                    {
                        "record_id": "rec_followup_1",
                        "parent_record_id": "rec_project_1",
                        "payload": {"record_id": "rec_followup_1"},
                    }
                ],
            )
            store.mark_repair_snapshot_failed(
                "repair_followups", "Data not ready"
            )
            snapshot = store.get_repair_snapshot(
                "repair_followups",
                parent_record_id="rec_project_1",
            )
            self.assertEqual(snapshot["status"], "failed")
            self.assertEqual(snapshot["error"], "Data not ready")
            self.assertEqual(
                [item["record_id"] for item in snapshot["records"]],
                ["rec_followup_1"],
            )


if __name__ == "__main__":
    unittest.main()
