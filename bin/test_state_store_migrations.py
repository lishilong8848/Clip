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
            self.assertIn("write_worker", report)


if __name__ == "__main__":
    unittest.main()
