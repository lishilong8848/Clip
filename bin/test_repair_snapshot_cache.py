import sys
import tempfile
import time
import unittest
from pathlib import Path


BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal.portal_service import (  # noqa: E402
    FieldMeta,
    MaintenancePortalService,
    REPAIR_CMDB_SNAPSHOT_VERSION,
    REPAIR_CMDB_TABLE_ID,
    REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME,
    REPAIR_FOLLOWUP_TABLE_ID,
    REPAIR_MANAGEMENT_TABLE_ID,
    REPAIR_SNAPSHOT_SOURCE_CMDB,
    REPAIR_SOURCE_APP_TOKEN,
)
from lan_bitable_template_portal.state_store import LanPortalStateStore  # noqa: E402


class RepairSnapshotCacheTests(unittest.TestCase):
    @staticmethod
    def _wait_for_cmdb_refresh(service: MaintenancePortalService) -> dict:
        deadline = time.time() + 2.0
        status = service.repair_management_cmdb_cache_status()
        while status.get("refreshing") and time.time() < deadline:
            time.sleep(0.01)
            status = service.repair_management_cmdb_cache_status()
        return status

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
            followup_parent_meta = FieldMeta(
                "fld_parent_l",
                REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME,
                "Text",
                1,
                False,
                {},
                [],
                False,
            )
            service._repair_followup_schema_ready = True
            service._repair_management_progress_schema_ready = True

            def load_fields(**kwargs):
                if kwargs.get("table_id") == REPAIR_FOLLOWUP_TABLE_ID:
                    return [followup_parent_meta], {
                        followup_parent_meta.field_name: followup_parent_meta
                    }
                calls["fields"] += 1
                return [title_meta], {title_meta.field_name: title_meta}

            def load_records(**kwargs):
                if kwargs.get("table_id") == REPAIR_FOLLOWUP_TABLE_ID:
                    return []
                self.assertEqual(kwargs.get("table_id"), REPAIR_MANAGEMENT_TABLE_ID)
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

    def test_cmdb_candidate_list_reads_local_snapshot_without_remote_wait(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = MaintenancePortalService(enable_repair_snapshots=True)
            service._state_store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            service._state_store.replace_repair_snapshot(
                REPAIR_SNAPSHOT_SOURCE_CMDB,
                app_token=REPAIR_SOURCE_APP_TOKEN,
                table_id=REPAIR_CMDB_TABLE_ID,
                meta={"catalog_version": REPAIR_CMDB_SNAPSHOT_VERSION},
                records=[
                    {
                        "record_id": "rec_cmdb_local",
                        "scope_codes": ["A"],
                        "title": "A楼 UPS",
                        "search_text": "A楼 UPS 电气",
                        "payload": {
                            "record_id": "rec_cmdb_local",
                            "display_fields": {
                                "智航唯一ID": "CMDB-001",
                                "设备名称": "A楼 UPS",
                                "分类名称": "UPS",
                                "位置": "A楼",
                                "楼栋": "A楼",
                            },
                            "raw_fields": {},
                        },
                    }
                ],
            )

            def reject_remote(*args, **kwargs):
                raise AssertionError("CMDB 候选列表不应直接读取飞书")

            service._load_repair_management_cmdb_records_remote = reject_remote  # type: ignore[method-assign]
            result = service.list_repair_management_cmdb_candidates(
                scope="A",
                query="UPS",
            )

            self.assertEqual(result["total"], 1)
            self.assertEqual(result["records"][0]["record_id"], "rec_cmdb_local")
            self.assertEqual(result["cache"]["table_id"], "tblJTRguSUij2RUM")
            self.assertTrue(result["cache"]["ready"])

    def test_cmdb_candidate_list_returns_immediately_when_local_snapshot_is_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = MaintenancePortalService(enable_repair_snapshots=True)
            service._state_store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            starts = {"count": 0}

            def start_background():
                starts["count"] += 1
                return {"started": True}

            def reject_remote(*args, **kwargs):
                raise AssertionError("空缓存候选请求也不能同步读取飞书")

            service.start_repair_management_cmdb_cache_refresh = start_background  # type: ignore[method-assign]
            service._load_repair_management_cmdb_records_remote = reject_remote  # type: ignore[method-assign]
            result = service.list_repair_management_cmdb_candidates(scope="ALL")

            self.assertEqual(result["records"], [])
            self.assertEqual(result["total"], 0)
            self.assertEqual(starts["count"], 1)
            self.assertFalse(result["cache"]["ready"])

    def test_cmdb_manual_refresh_replaces_snapshot_and_preserves_stale_on_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = MaintenancePortalService(enable_repair_snapshots=True)
            service._state_store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            title_meta = FieldMeta(
                "fld_device",
                "设备名称",
                "Text",
                1,
                True,
                {},
                [],
                False,
            )

            def load_remote(*, force_refresh=False):
                self.assertTrue(force_refresh)
                return (
                    [title_meta],
                    {title_meta.field_name: title_meta},
                    [
                        {
                            "record_id": "rec_cmdb_new",
                            "display_fields": {
                                "智航唯一ID": "CMDB-NEW",
                                "设备名称": "新设备",
                                "分类名称": "测试分类",
                                "位置": "B楼",
                                "楼栋": "B楼",
                            },
                            "raw_fields": {},
                        }
                    ],
                )

            service._load_repair_management_cmdb_records_remote = load_remote  # type: ignore[method-assign]
            started = service.start_repair_management_cmdb_cache_refresh()
            self.assertTrue(started["started"])
            status = self._wait_for_cmdb_refresh(service)
            self.assertFalse(status["refreshing"])
            self.assertTrue(status["ready"])
            self.assertEqual(status["record_count"], 1)
            self.assertEqual(status["table_id"], "tblJTRguSUij2RUM")

            def fail_remote(*, force_refresh=False):
                raise RuntimeError("模拟飞书下载失败")

            service._load_repair_management_cmdb_records_remote = fail_remote  # type: ignore[method-assign]
            service.start_repair_management_cmdb_cache_refresh()
            failed_status = self._wait_for_cmdb_refresh(service)
            self.assertEqual(failed_status["status"], "failed")
            self.assertTrue(failed_status["ready"])
            self.assertEqual(failed_status["record_count"], 1)
            stale = service.list_repair_management_cmdb_candidates(
                scope="ALL",
                query="新设备",
            )
            self.assertEqual(stale["records"][0]["record_id"], "rec_cmdb_new")


if __name__ == "__main__":
    unittest.main()
