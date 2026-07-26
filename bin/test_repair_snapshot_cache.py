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
    PortalConflictError,
    REPAIR_CMDB_SNAPSHOT_VERSION,
    REPAIR_CMDB_TABLE_ID,
    REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME,
    REPAIR_FOLLOWUP_TABLE_ID,
    REPAIR_MANAGEMENT_FOLLOWUP_LINK_FIELD_NAME,
    REPAIR_MANAGEMENT_FOLLOWUP_LINK_STORAGE_FIELD_NAME,
    REPAIR_MANAGEMENT_TABLE_ID,
    REPAIR_SNAPSHOT_SOURCE_CMDB,
    REPAIR_SNAPSHOT_SOURCE_FOLLOWUPS,
    REPAIR_SNAPSHOT_SOURCE_PROJECTS,
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

    def test_followup_snapshot_can_be_read_by_selected_parent_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            store.replace_repair_snapshot(
                REPAIR_SNAPSHOT_SOURCE_FOLLOWUPS,
                app_token=REPAIR_SOURCE_APP_TOKEN,
                table_id=REPAIR_FOLLOWUP_TABLE_ID,
                records=[
                    {
                        "record_id": "rec_followup_a",
                        "parent_record_id": "rec_project_a",
                        "payload": {"record_id": "rec_followup_a"},
                    },
                    {
                        "record_id": "rec_followup_b",
                        "parent_record_id": "rec_project_b",
                        "payload": {"record_id": "rec_followup_b"},
                    },
                ],
            )

            grouped = store.repair_snapshot_records_by_parents(
                REPAIR_SNAPSHOT_SOURCE_FOLLOWUPS,
                ["rec_project_a"],
            )

            self.assertEqual(
                [item["record_id"] for item in grouped["rec_project_a"]],
                ["rec_followup_a"],
            )
            self.assertNotIn("rec_project_b", grouped)

    def test_repair_list_summary_omits_full_raw_fields(self):
        service = MaintenancePortalService()
        payload = service._repair_management_record_payload(
            {
                "record_id": "rec_project_summary",
                "last_modified_time": "123",
                "display_fields": {
                    "维修名称": "测试维修项目",
                    "所属专业": "电气",
                    "故障维修原因": "测试原因",
                    "大段内部字段": "不应进入列表响应",
                },
                "raw_fields": {
                    "维修名称": "测试维修项目",
                    "大段内部字段": "不应进入列表响应",
                },
            },
            authoritative_followups=[],
            summary_only=True,
        )

        self.assertTrue(payload["summary_only"])
        self.assertNotIn("raw_fields", payload)
        self.assertNotIn("大段内部字段", payload["display_fields"])
        self.assertTrue(payload["record_version"])

    def test_repair_record_version_rejects_stale_update(self):
        service = MaintenancePortalService()
        record = {
            "record_id": "rec_project_version",
            "last_modified_time": "123",
            "raw_fields": {"维修名称": "版本一"},
        }
        version = service._repair_record_version(record)
        service._assert_repair_record_version(record, version)

        with self.assertRaises(PortalConflictError):
            service._assert_repair_record_version(record, "stale-version")

    def test_stale_project_version_stops_before_remote_write(self):
        service = MaintenancePortalService()
        service._repair_management_snapshot_schema = (  # type: ignore[method-assign]
            lambda **_kwargs: ([], {})
        )
        service._ensure_repair_management_record_in_scope = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: {
                "record_id": "rec_project_stale",
                "last_modified_time": "2",
                "raw_fields": {"维修名称": "服务器新版本"},
            }
        )
        service._patch_record_fields = (  # type: ignore[method-assign]
            lambda **_kwargs: self.fail("版本冲突时不应写飞书")
        )

        with self.assertRaises(PortalConflictError):
            service.update_repair_management_record(
                "rec_project_stale",
                {"维修名称": "浏览器旧版本"},
                expected_version="stale-version",
            )

    def test_stale_followup_version_stops_before_remote_write(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = MaintenancePortalService(enable_repair_snapshots=True)
            service._state_store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            service._state_store.replace_repair_snapshot(
                REPAIR_SNAPSHOT_SOURCE_FOLLOWUPS,
                app_token=REPAIR_SOURCE_APP_TOKEN,
                table_id=REPAIR_FOLLOWUP_TABLE_ID,
                records=[
                    {
                        "record_id": "rec_followup_stale",
                        "parent_record_id": "rec_project_stale",
                        "payload": {
                            "record_id": "rec_followup_stale",
                            "last_modified_time": "2",
                            "raw_fields": {"维修进展描述": "服务器新版本"},
                        },
                    }
                ],
            )
            service._patch_record_fields = (  # type: ignore[method-assign]
                lambda **_kwargs: self.fail("版本冲突时不应写飞书")
            )

            with self.assertRaises(PortalConflictError):
                service.update_repair_followup_record(
                    "rec_followup_stale",
                    summary_record_id="rec_project_stale",
                    fields={"维修进展描述": "浏览器旧版本"},
                    expected_version="stale-version",
                )

    def test_project_update_operation_replays_without_second_write(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = MaintenancePortalService()
            service._state_store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            calls = {"count": 0}

            def fake_update(record_id, fields, **_kwargs):
                calls["count"] += 1
                return {
                    "record_id": record_id,
                    "fields": dict(fields or {}),
                    "record_version": "version-after-update",
                }

            service._update_repair_management_record_unlocked = (  # type: ignore[method-assign]
                fake_update
            )
            request = {
                "record_id": "rec_project_update",
                "fields": {"维修名称": "测试维修"},
                "operation_id": "project-update-operation-1",
                "expected_version": "version-before-update",
                "scope": "A",
            }

            first = service.update_repair_management_record(**request)
            second = service.update_repair_management_record(**request)

            self.assertEqual(calls["count"], 1)
            self.assertEqual(first["record_version"], "version-after-update")
            self.assertTrue(second["idempotent_replay"])
            self.assertEqual(second["record_id"], "rec_project_update")

    def test_followup_update_operation_replays_without_second_write(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = MaintenancePortalService()
            service._state_store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            calls = {"count": 0}

            def fake_update(record_id, **kwargs):
                calls["count"] += 1
                return {
                    "record_id": record_id,
                    "summary_record_id": kwargs["summary_record_id"],
                    "fields": dict(kwargs.get("fields") or {}),
                    "record_version": "version-after-update",
                }

            service._update_repair_followup_record_unlocked = (  # type: ignore[method-assign]
                fake_update
            )
            request = {
                "record_id": "rec_followup_update",
                "summary_record_id": "rec_project_update",
                "fields": {"维修进展描述": "处理中"},
                "operation_id": "followup-update-operation-1",
                "expected_version": "version-before-update",
                "scope": "A",
            }

            first = service.update_repair_followup_record(**request)
            second = service.update_repair_followup_record(**request)

            self.assertEqual(calls["count"], 1)
            self.assertEqual(first["record_version"], "version-after-update")
            self.assertTrue(second["idempotent_replay"])
            self.assertEqual(second["record_id"], "rec_followup_update")

    def test_failed_repair_sync_is_visible_and_can_be_requeued(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = MaintenancePortalService()
            service._state_store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            service._ensure_repair_management_record_in_scope = (  # type: ignore[method-assign]
                lambda *_args, **_kwargs: {"record_id": "rec_project_sync"}
            )
            service._process_repair_sync_tasks_async = (  # type: ignore[method-assign]
                lambda: None
            )
            operation_id = "repair_sync:followup_summary_sync:rec_project_sync"
            service._state_store.begin_repair_management_operation(
                operation_id,
                operation_type="followup_summary_sync",
                scope="A",
                payload_hash="test",
                summary_record_id="rec_project_sync",
            )
            service._state_store.update_repair_management_operation(
                operation_id,
                status="failed",
                result={"task_payload": {}, "attempts": 12},
                error="模拟同步失败",
            )

            status = service.get_repair_management_sync_status(
                "rec_project_sync",
                scope="A",
            )
            retried = service.retry_repair_management_sync(
                "rec_project_sync",
                scope="A",
            )
            operation = service._state_store.get_repair_management_operation(
                operation_id
            )

            self.assertEqual(status["status"], "failed")
            self.assertEqual(status["failed_count"], 1)
            self.assertEqual(retried["retried"], 1)
            self.assertEqual(operation["status"], "sync_pending")
            self.assertEqual(operation["result"]["attempts"], 0)

    def test_project_status_index_pages_active_and_completed_records(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = MaintenancePortalService(enable_repair_snapshots=True)
            service._state_store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            now_ms = str(int(time.time() * 1000))
            service._state_store.replace_repair_snapshot(
                REPAIR_SNAPSHOT_SOURCE_PROJECTS,
                app_token=REPAIR_SOURCE_APP_TOKEN,
                table_id=REPAIR_MANAGEMENT_TABLE_ID,
                records=[
                    {
                        "record_id": "rec_project_active",
                        "scope_codes": ["A"],
                        "title": "未完成项目",
                        "search_text": "未完成项目 A楼",
                        "sort_time": float(now_ms),
                        "payload": {
                            "record_id": "rec_project_active",
                            "created_time": now_ms,
                            "last_modified_time": now_ms,
                            "display_fields": {
                                "维修名称": "未完成项目",
                                "所属数据中心/楼栋-使用": "南通A楼",
                            },
                            "raw_fields": {},
                        },
                    },
                    {
                        "record_id": "rec_project_completed",
                        "scope_codes": ["A"],
                        "title": "已完成项目",
                        "search_text": "已完成项目 A楼",
                        "sort_time": float(now_ms) - 1,
                        "payload": {
                            "record_id": "rec_project_completed",
                            "created_time": now_ms,
                            "last_modified_time": now_ms,
                            "display_fields": {
                                "维修名称": "已完成项目",
                                "所属数据中心/楼栋-使用": "南通A楼",
                                "维修结束时间（2026）": "2026-07-25 10:00",
                            },
                            "raw_fields": {
                                "维修结束时间（2026）": "2026-07-25 10:00",
                            },
                        },
                    },
                ],
            )
            service._state_store.replace_repair_snapshot(
                REPAIR_SNAPSHOT_SOURCE_FOLLOWUPS,
                app_token=REPAIR_SOURCE_APP_TOKEN,
                table_id=REPAIR_FOLLOWUP_TABLE_ID,
                records=[
                    {
                        "record_id": "rec_followup_completed",
                        "parent_record_id": "rec_project_completed",
                        "scope_codes": ["A"],
                        "payload": {
                            "record_id": "rec_followup_completed",
                            "created_time": now_ms,
                            "last_modified_time": now_ms,
                            "display_fields": {
                                REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME: (
                                    "rec_project_completed"
                                ),
                                "维修进度": "100%",
                            },
                            "raw_fields": {
                                REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME: (
                                    "rec_project_completed"
                                ),
                                "维修进度": 1,
                            },
                        },
                    }
                ],
            )
            service._repair_management_snapshot_schema = (  # type: ignore[method-assign]
                lambda **_kwargs: ([], {})
            )

            active = service.get_repair_management_records(
                scope="A",
                state="active",
                summary_only=True,
            )
            completed = service.get_repair_management_records(
                scope="A",
                state="completed",
                summary_only=True,
            )
            index_meta = service._state_store.repair_project_status_index_meta()

            self.assertEqual(active["total"], 1)
            self.assertEqual(
                active["records"][0]["record_id"],
                "rec_project_active",
            )
            self.assertEqual(completed["total"], 1)
            self.assertEqual(
                completed["records"][0]["record_id"],
                "rec_project_completed",
            )
            self.assertTrue(completed["records"][0]["is_completed"])
            self.assertEqual(index_meta["record_count"], 2)

    def test_integrity_detects_missing_and_stale_followup_links(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = MaintenancePortalService(enable_repair_snapshots=True)
            service._state_store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            service._state_store.replace_repair_snapshot(
                REPAIR_SNAPSHOT_SOURCE_PROJECTS,
                app_token=REPAIR_SOURCE_APP_TOKEN,
                table_id=REPAIR_MANAGEMENT_TABLE_ID,
                records=[
                    {
                        "record_id": "rec_project_integrity",
                        "scope_codes": ["A"],
                        "payload": {
                            "record_id": "rec_project_integrity",
                            "display_fields": {
                                "维修名称": "关联检查",
                                "所属数据中心/楼栋-使用": "南通A楼",
                            },
                            "raw_fields": {
                                REPAIR_MANAGEMENT_FOLLOWUP_LINK_STORAGE_FIELD_NAME: (
                                    "rec_followup_stale"
                                )
                            },
                        },
                    }
                ],
            )
            service._state_store.replace_repair_snapshot(
                REPAIR_SNAPSHOT_SOURCE_FOLLOWUPS,
                app_token=REPAIR_SOURCE_APP_TOKEN,
                table_id=REPAIR_FOLLOWUP_TABLE_ID,
                records=[
                    {
                        "record_id": "rec_followup_actual",
                        "parent_record_id": "rec_project_integrity",
                        "scope_codes": ["A"],
                        "payload": {
                            "record_id": "rec_followup_actual",
                            "display_fields": {
                                REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME: (
                                    "rec_project_integrity"
                                )
                            },
                            "raw_fields": {
                                REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME: (
                                    "rec_project_integrity"
                                )
                            },
                        },
                    }
                ],
            )

            integrity = service.get_repair_management_integrity(
                scope="A",
                force_refresh=True,
            )

            self.assertEqual(integrity["issue_count"], 1)
            issue = integrity["issues"][0]
            self.assertEqual(
                issue["missing_followup_ids"],
                ["rec_followup_actual"],
            )
            self.assertEqual(
                issue["stale_followup_ids"],
                ["rec_followup_stale"],
            )

    def test_global_sync_retry_is_isolated_by_scope(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = MaintenancePortalService()
            service._state_store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            service._process_repair_sync_tasks_async = (  # type: ignore[method-assign]
                lambda: None
            )
            for scope in ("A", "B", "ALL"):
                operation_id = f"repair_sync:followup_summary_sync:{scope}"
                service._state_store.begin_repair_management_operation(
                    operation_id,
                    operation_type="followup_summary_sync",
                    scope=scope,
                    payload_hash=scope,
                    summary_record_id=f"rec_project_{scope}",
                )
                service._state_store.update_repair_management_operation(
                    operation_id,
                    status="failed",
                    result={"task_payload": {}, "attempts": 12},
                    error=f"{scope} 同步失败",
                )

            status = service.get_repair_management_global_sync_status(scope="A")
            retried = service.retry_all_repair_management_sync(scope="A")

            self.assertEqual(status["failed_count"], 1)
            self.assertEqual(retried["retried"], 1)
            self.assertEqual(
                service._state_store.get_repair_management_operation(
                    "repair_sync:followup_summary_sync:A"
                )["status"],
                "sync_pending",
            )
            self.assertEqual(
                service._state_store.get_repair_management_operation(
                    "repair_sync:followup_summary_sync:B"
                )["status"],
                "failed",
            )
            self.assertEqual(
                service._state_store.get_repair_management_operation(
                    "repair_sync:followup_summary_sync:ALL"
                )["status"],
                "failed",
            )

    def test_operation_cleanup_respects_total_limit(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            for suffix, status in (("completed", "completed"), ("failed", "failed")):
                operation_id = f"repair_sync:followup_summary_sync:{suffix}"
                store.begin_repair_management_operation(
                    operation_id,
                    operation_type="followup_summary_sync",
                    scope="A",
                    payload_hash=suffix,
                )
                store.update_repair_management_operation(
                    operation_id,
                    status=status,
                    error="测试失败" if status == "failed" else "",
                )

            deleted = store.cleanup_repair_management_operations(
                completed_before=time.time() + 1,
                failed_before=time.time() + 1,
                limit=1,
            )

            self.assertEqual(deleted["completed"] + deleted["failed"], 1)

    def test_followup_delete_operation_replays_without_second_remote_delete(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = MaintenancePortalService()
            service._state_store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            calls = {"count": 0}

            def fake_delete(record_id, **kwargs):
                calls["count"] += 1
                return {
                    "record_id": record_id,
                    "summary_record_id": kwargs["summary_record_id"],
                    "deleted": True,
                    "warnings": [],
                }

            service._delete_repair_followup_record_unlocked = (  # type: ignore[method-assign]
                fake_delete
            )
            request = {
                "record_id": "rec_followup_delete",
                "summary_record_id": "rec_project_delete",
                "operation_id": "followup-delete-operation-1",
                "expected_version": "version-before-delete",
                "scope": "A",
            }

            first = service.delete_repair_followup_record(**request)
            second = service.delete_repair_followup_record(**request)

            self.assertEqual(calls["count"], 1)
            self.assertTrue(first["deleted"])
            self.assertTrue(second["idempotent_replay"])
            self.assertEqual(second["record_id"], "rec_followup_delete")

    def test_followup_change_event_contains_authoritative_project_patch(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = MaintenancePortalService(enable_repair_snapshots=True)
            service._state_store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            service._state_store.replace_repair_snapshot(
                REPAIR_SNAPSHOT_SOURCE_PROJECTS,
                app_token=REPAIR_SOURCE_APP_TOKEN,
                table_id=REPAIR_MANAGEMENT_TABLE_ID,
                records=[
                    {
                        "record_id": "rec_project_patch",
                        "scope_codes": ["A"],
                        "title": "A楼测试维修",
                        "search_text": "A楼测试维修",
                        "sort_time": time.time(),
                        "payload": {
                            "record_id": "rec_project_patch",
                            "display_fields": {
                                "维修名称": "A楼测试维修",
                                "所属数据中心/楼栋-使用": "南通A楼",
                            },
                            "raw_fields": {},
                        },
                    }
                ],
            )
            service._state_store.replace_repair_snapshot(
                REPAIR_SNAPSHOT_SOURCE_FOLLOWUPS,
                app_token=REPAIR_SOURCE_APP_TOKEN,
                table_id=REPAIR_FOLLOWUP_TABLE_ID,
                records=[],
            )

            service._upsert_repair_snapshot_fields(
                source_key=REPAIR_SNAPSHOT_SOURCE_FOLLOWUPS,
                record_id="rec_followup_patch",
                fields={
                    REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME: "rec_project_patch",
                    "维修进度": 0.5,
                    "维修进展描述": "处理中",
                },
                parent_record_id="rec_project_patch",
            )
            first_changes = service.list_repair_management_changes(
                scope="A",
                after_id=0,
            )
            followup_change = next(
                item
                for item in first_changes
                if item["entity_type"] == "followup"
            )
            first_patch = followup_change["payload"]["record_patch"]

            self.assertTrue(followup_change["payload"]["created"])
            self.assertEqual(first_patch["record_id"], "rec_project_patch")
            self.assertEqual(first_patch["followup_count"], 1)
            self.assertEqual(first_patch["progress_percent"], 50)

            service._upsert_repair_snapshot_fields(
                source_key=REPAIR_SNAPSHOT_SOURCE_FOLLOWUPS,
                record_id="rec_followup_patch",
                fields={"维修进度": 1},
                parent_record_id="rec_project_patch",
            )
            second_changes = service.list_repair_management_changes(
                scope="A",
                after_id=int(followup_change["id"]),
            )
            second_change = next(
                item
                for item in second_changes
                if item["entity_type"] == "followup"
            )

            self.assertFalse(second_change["payload"]["created"])
            self.assertEqual(
                second_change["payload"]["record_patch"]["progress_percent"],
                100,
            )

    def test_completed_history_period_uses_local_status_index(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = MaintenancePortalService(enable_repair_snapshots=True)
            service._state_store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            now = time.time()
            old = now - 60 * 86400
            now_text = time.strftime("%Y-%m-%d %H:%M", time.localtime(now))
            old_text = time.strftime("%Y-%m-%d %H:%M", time.localtime(old))

            def project(record_id: str, title: str, ended_at: str, sort_time: float):
                return {
                    "record_id": record_id,
                    "scope_codes": ["A"],
                    "title": title,
                    "search_text": title,
                    "sort_time": sort_time,
                    "payload": {
                        "record_id": record_id,
                        "display_fields": {
                            "维修名称": title,
                            "所属数据中心/楼栋-使用": "南通A楼",
                            "维修结束时间（2026）": ended_at,
                        },
                        "raw_fields": {
                            "维修结束时间（2026）": ended_at,
                        },
                    },
                }

            def followup(
                record_id: str,
                parent_id: str,
                updated_at: str,
                sort_time: float,
            ):
                return {
                    "record_id": record_id,
                    "parent_record_id": parent_id,
                    "scope_codes": ["A"],
                    "sort_time": sort_time,
                    "payload": {
                        "record_id": record_id,
                        "last_modified_time": updated_at,
                        "display_fields": {
                            REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME: parent_id,
                            "维修进度": "100%",
                        },
                        "raw_fields": {
                            REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME: parent_id,
                            "维修进度": 1,
                        },
                    },
                }

            service._state_store.replace_repair_snapshot(
                REPAIR_SNAPSHOT_SOURCE_PROJECTS,
                app_token=REPAIR_SOURCE_APP_TOKEN,
                table_id=REPAIR_MANAGEMENT_TABLE_ID,
                records=[
                    project("rec_completed_now", "本月完成", now_text, now),
                    project("rec_completed_old", "历史完成", old_text, old),
                ],
            )
            service._state_store.replace_repair_snapshot(
                REPAIR_SNAPSHOT_SOURCE_FOLLOWUPS,
                app_token=REPAIR_SOURCE_APP_TOKEN,
                table_id=REPAIR_FOLLOWUP_TABLE_ID,
                records=[
                    followup(
                        "rec_followup_now",
                        "rec_completed_now",
                        now_text,
                        now,
                    ),
                    followup(
                        "rec_followup_old",
                        "rec_completed_old",
                        old_text,
                        old,
                    ),
                ],
            )

            all_history = service.get_repair_management_records(
                scope="A",
                state="completed",
                period="all",
                summary_only=True,
            )
            month_history = service.get_repair_management_records(
                scope="A",
                state="completed",
                period="month",
                summary_only=True,
            )

            self.assertEqual(all_history["total"], 2)
            self.assertEqual(month_history["total"], 1)
            self.assertEqual(
                month_history["records"][0]["record_id"],
                "rec_completed_now",
            )

    def test_integration_check_reads_only_and_never_writes_remote(self):
        service = MaintenancePortalService()
        project_meta = FieldMeta(
            "fld_project_title",
            "维修名称",
            "Text",
            1,
            True,
            {},
            [],
            False,
        )
        workflow_meta = FieldMeta(
            "fld_project_workflow",
            "流程-L",
            "SingleSelect",
            3,
            False,
            {},
            ["未开始", "维修中", "维修完成"],
            False,
        )
        link_meta = FieldMeta(
            "fld_followup_links",
            REPAIR_MANAGEMENT_FOLLOWUP_LINK_STORAGE_FIELD_NAME,
            "Text",
            1,
            False,
            {},
            [],
            False,
        )
        parent_meta = FieldMeta(
            "fld_parent",
            REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME,
            "Text",
            1,
            False,
            {},
            [],
            False,
        )
        progress_meta = FieldMeta(
            "fld_progress",
            "维修进度",
            "Number",
            2,
            False,
            {},
            [],
            False,
        )
        project = {
            "record_id": "rec_project_read",
            "display_fields": {
                "维修名称": "A楼读取校验",
                "所属数据中心/楼栋-使用": "南通A楼",
            },
            "raw_fields": {},
        }
        followup = {
            "record_id": "rec_followup_read",
            "display_fields": {
                REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME: "rec_project_read"
            },
            "raw_fields": {
                REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME: "rec_project_read"
            },
        }
        project_metas = service._repair_logical_field_metas(
            REPAIR_MANAGEMENT_TABLE_ID,
            [project_meta, workflow_meta, link_meta],
        )
        self.assertIn(
            REPAIR_MANAGEMENT_FOLLOWUP_LINK_FIELD_NAME,
            {meta.field_name for meta in project_metas},
        )
        self.assertNotIn(
            REPAIR_MANAGEMENT_FOLLOWUP_LINK_STORAGE_FIELD_NAME,
            {meta.field_name for meta in project_metas},
        )
        followup_metas = [parent_meta, progress_meta]
        service._load_repair_management_project_records = (  # type: ignore[method-assign]
            lambda **_kwargs: (
                project_metas,
                {meta.field_name: meta for meta in project_metas},
                [project],
            )
        )
        service._load_repair_followup_snapshot = (  # type: ignore[method-assign]
            lambda **_kwargs: (
                followup_metas,
                {meta.field_name: meta for meta in followup_metas},
                [followup],
            )
        )
        service._patch_record_fields = (  # type: ignore[method-assign]
            lambda **_kwargs: self.fail("读取校验不应更新远端记录")
        )
        service._create_record_fields = (  # type: ignore[method-assign]
            lambda **_kwargs: self.fail("读取校验不应创建远端记录")
        )
        service._delete_record_fields = (  # type: ignore[method-assign]
            lambda **_kwargs: self.fail("读取校验不应删除远端记录")
        )

        result = service.check_repair_management_integration(scope="A")

        self.assertEqual(result["status"], "ready")
        self.assertTrue(result["read_verified"])
        self.assertFalse(result["remote_write_performed"])
        self.assertEqual(result["project_record_count"], 1)
        self.assertEqual(result["followup_record_count"], 1)

    def test_remote_reconcile_only_replaces_local_snapshots(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = MaintenancePortalService(enable_repair_snapshots=True)
            service._state_store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            service._state_store.replace_repair_snapshot(
                REPAIR_SNAPSHOT_SOURCE_PROJECTS,
                app_token=REPAIR_SOURCE_APP_TOKEN,
                table_id=REPAIR_MANAGEMENT_TABLE_ID,
                records=[
                    {
                        "record_id": "rec_project_before",
                        "scope_codes": ["A"],
                        "payload": {
                            "record_id": "rec_project_before",
                            "display_fields": {
                                "维修名称": "对账前",
                                "所属数据中心/楼栋-使用": "南通A楼",
                            },
                            "raw_fields": {},
                        },
                    }
                ],
            )
            service._state_store.replace_repair_snapshot(
                REPAIR_SNAPSHOT_SOURCE_FOLLOWUPS,
                app_token=REPAIR_SOURCE_APP_TOKEN,
                table_id=REPAIR_FOLLOWUP_TABLE_ID,
                records=[],
            )

            def fake_check(*, scope: str = "ALL"):
                service._state_store.replace_repair_snapshot(
                    REPAIR_SNAPSHOT_SOURCE_PROJECTS,
                    app_token=REPAIR_SOURCE_APP_TOKEN,
                    table_id=REPAIR_MANAGEMENT_TABLE_ID,
                    records=[
                        {
                            "record_id": "rec_project_after",
                            "scope_codes": ["A"],
                            "payload": {
                                "record_id": "rec_project_after",
                                "display_fields": {
                                    "维修名称": "对账后",
                                    "所属数据中心/楼栋-使用": "南通A楼",
                                },
                                "raw_fields": {},
                            },
                        }
                    ],
                )
                return {
                    "scope": scope,
                    "status": "ready",
                    "remote_write_performed": False,
                }

            service.check_repair_management_integration = fake_check  # type: ignore[method-assign]
            service._patch_record_fields = (  # type: ignore[method-assign]
                lambda **_kwargs: self.fail("对账不应更新远端记录")
            )
            service._create_record_fields = (  # type: ignore[method-assign]
                lambda **_kwargs: self.fail("对账不应创建远端记录")
            )
            service._delete_record_fields = (  # type: ignore[method-assign]
                lambda **_kwargs: self.fail("对账不应删除远端记录")
            )

            result = service.reconcile_repair_management_remote(scope="A")

            self.assertEqual(result["changed_count"], 2)
            self.assertEqual(result["project"]["added"], 1)
            self.assertEqual(result["project"]["removed"], 1)
            self.assertFalse(result["remote_write_performed"])

    def test_failed_action_job_finishes_audit_and_keeps_audit_id(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = MaintenancePortalService(enable_repair_snapshots=True)
            service._state_store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            audit_id = "audit_failed_job"
            service._state_store.record_business_operation_audit(
                audit_id=audit_id,
                operation_id="job_failed",
                domain="notice",
                action="start",
                status="started",
            )
            service._jobs["job_failed"] = {
                "job_id": "job_failed",
                "phase": "accepted",
                "business_audit_id": audit_id,
                "request": {
                    "work_type": "maintenance",
                    "notice_type": "维保通告",
                },
            }

            service.mark_job(
                "job_failed",
                phase="failed",
                error="准备通告失败",
            )

            job = service.get_job("job_failed") or {}
            audits = service._state_store.list_business_operation_audits(
                operation_id="job_failed"
            )
            self.assertEqual(job.get("business_audit_id"), audit_id)
            self.assertEqual(len(audits), 1)
            self.assertEqual(audits[0]["status"], "failed")
            self.assertEqual(audits[0]["error"], "准备通告失败")

    def test_completed_action_job_compaction_keeps_audit_id(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = MaintenancePortalService(enable_repair_snapshots=True)
            service._state_store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            service._jobs["job_success"] = {
                "job_id": "job_success",
                "phase": "success",
                "business_audit_id": "audit_success_job",
                "request": {"work_type": "maintenance"},
            }

            with service._jobs_lock:
                service._compact_completed_job_locked("job_success")

            job = service.get_job("job_success") or {}
            self.assertEqual(
                job.get("business_audit_id"),
                "audit_success_job",
            )


if __name__ == "__main__":
    unittest.main()
