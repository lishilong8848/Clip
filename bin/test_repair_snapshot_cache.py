import sys
import tempfile
import unittest
from pathlib import Path


BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal.portal_service import (  # noqa: E402
    FieldMeta,
    MaintenancePortalService,
)
from lan_bitable_template_portal.state_store import LanPortalStateStore  # noqa: E402


class RepairSnapshotCacheTests(unittest.TestCase):
    def test_project_list_reuses_snapshot_until_forced_refresh(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = MaintenancePortalService(enable_repair_snapshots=True)
            service._state_store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            calls = {"fields": 0, "records": 0}
            title_meta = FieldMeta(
                "fld_title",
                "维修名称",
                "Text",
                1,
                True,
                {},
                [],
                False,
            )

            def load_fields(**_kwargs):
                calls["fields"] += 1
                return [title_meta], {title_meta.field_name: title_meta}

            def load_records(**_kwargs):
                calls["records"] += 1
                return [
                    {
                        "record_id": "rec_project_1",
                        "display_fields": {
                            "维修名称": "测试维修项目",
                            "流程": "维修中",
                        },
                        "raw_fields": {},
                        "created_time": "1",
                        "last_modified_time": "2",
                    }
                ]

            service._load_table_fields = load_fields  # type: ignore[method-assign]
            service._load_table_records = load_records  # type: ignore[method-assign]

            first = service.get_repair_management_records(scope="ALL")
            second = service.get_repair_management_records(scope="ALL")
            refreshed = service.get_repair_management_records(
                scope="ALL",
                force_refresh=True,
            )

            self.assertEqual(first["total"], 1)
            self.assertEqual(second["total"], 1)
            self.assertEqual(refreshed["total"], 1)
            self.assertEqual(calls, {"fields": 2, "records": 2})


if __name__ == "__main__":
    unittest.main()
