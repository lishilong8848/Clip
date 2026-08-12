import sqlite3
import sys
import tempfile
import time
import unittest
from pathlib import Path


BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal.state_store import LanPortalStateStore  # noqa: E402
from lan_bitable_template_portal.operation_audit import (  # noqa: E402
    begin_business_audit,
    finish_business_audit,
    safe_audit_metadata,
)


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

    def test_event_snapshot_hides_failures_older_than_latest_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            store.record_failed_event_month_snapshot(
                "2026-07",
                error="old TLS timeout",
            )
            store.replace_event_month_snapshot(
                "2026-07",
                [{"record_id": "event-1", "building_codes": ["A"]}],
            )

            recovered = store.get_event_month_snapshot("2026-07")
            self.assertEqual(recovered["last_failed"], {})

            store.record_failed_event_month_snapshot(
                "2026-07",
                error="latest refresh failed",
            )
            failed_again = store.get_event_month_snapshot("2026-07")
            self.assertEqual(
                failed_again["last_failed"]["error"],
                "latest refresh failed",
            )

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

    def test_old_repair_status_index_adds_completed_at_without_data_loss(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "state.sqlite3"
            conn = sqlite3.connect(db_path)
            try:
                conn.execute(
                    """
                    CREATE TABLE repair_project_status_index (
                        record_id TEXT PRIMARY KEY,
                        state TEXT NOT NULL DEFAULT 'active',
                        workflow TEXT NOT NULL DEFAULT '',
                        followup_count INTEGER NOT NULL DEFAULT 0,
                        progress_percent INTEGER NOT NULL DEFAULT 0,
                        latest_followup_time TEXT NOT NULL DEFAULT '',
                        latest_followup_sort_time REAL NOT NULL DEFAULT 0,
                        state_verified INTEGER NOT NULL DEFAULT 1,
                        updated_at REAL NOT NULL
                    )
                    """
                )
                conn.execute(
                    """
                    INSERT INTO repair_project_status_index(
                        record_id, state, workflow, updated_at
                    ) VALUES ('rec_old_index', 'completed', '维修完成', 1)
                    """
                )
                conn.commit()
            finally:
                conn.close()

            store = LanPortalStateStore(db_path)
            health = store.schema_health()
            conn = sqlite3.connect(db_path)
            try:
                columns = {
                    str(row[1])
                    for row in conn.execute(
                        "PRAGMA table_info(repair_project_status_index)"
                    ).fetchall()
                }
                row = conn.execute(
                    """
                    SELECT record_id, completed_at
                    FROM repair_project_status_index
                    WHERE record_id = 'rec_old_index'
                    """
                ).fetchone()
            finally:
                conn.close()

            self.assertTrue(health["ok"], health)
            self.assertIn("completed_at", columns)
            self.assertEqual(row, ("rec_old_index", 0.0))

    def test_scope_template_revision_columns_upgrade_without_data_loss(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "state.sqlite3"
            conn = sqlite3.connect(db_path)
            try:
                conn.execute(
                    """
                    CREATE TABLE critical_guard_scope_templates (
                        scope_code TEXT NOT NULL,
                        sheet_type TEXT NOT NULL,
                        template_version TEXT NOT NULL DEFAULT '',
                        revision INTEGER NOT NULL DEFAULT 1,
                        items_json TEXT NOT NULL DEFAULT '[]',
                        updated_by_open_id TEXT NOT NULL DEFAULT '',
                        updated_by_name TEXT NOT NULL DEFAULT '',
                        created_at REAL NOT NULL,
                        updated_at REAL NOT NULL,
                        PRIMARY KEY(scope_code, sheet_type)
                    )
                    """
                )
                conn.execute(
                    """
                    INSERT INTO critical_guard_scope_templates(
                        scope_code, sheet_type, template_version, revision,
                        items_json, created_at, updated_at
                    ) VALUES ('A', '设备安全', 'old-template', 4,
                              '[{"key":"old","category":"旧项","content":"保留"}]',
                              1, 1)
                    """
                )
                conn.commit()
            finally:
                conn.close()

            store = LanPortalStateStore(db_path)
            template = store.get_critical_guard_scope_template(
                scope="A",
                sheet_type="设备安全",
            )
            conn = sqlite3.connect(db_path)
            try:
                columns = {
                    str(row[1])
                    for row in conn.execute(
                        "PRAGMA table_info(critical_guard_scope_templates)"
                    ).fetchall()
                }
            finally:
                conn.close()

            self.assertTrue(store.schema_health()["ok"])
            self.assertTrue(template["customized"])
            self.assertEqual(template["revision"], 4)
            self.assertEqual(template["items"][0]["content"], "保留")
            self.assertTrue(
                {
                    "is_customized",
                    "last_operation_id",
                    "last_operation_response_id",
                }.issubset(columns)
            )

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

    def test_completed_history_uses_explicit_completed_at_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            now = time.time()
            store.replace_repair_snapshot(
                "repair_projects",
                records=[
                    {
                        "record_id": "rec_completed_today",
                        "scope_codes": ["A"],
                        "title": "今天完成",
                        "sort_time": now,
                        "payload": {"record_id": "rec_completed_today"},
                    },
                    {
                        "record_id": "rec_completed_old",
                        "scope_codes": ["A"],
                        "title": "旧完成后又编辑",
                        "sort_time": now,
                        "payload": {"record_id": "rec_completed_old"},
                    },
                    {
                        "record_id": "rec_completed_without_time",
                        "scope_codes": ["A"],
                        "title": "缺少完成时间",
                        "sort_time": now,
                        "payload": {"record_id": "rec_completed_without_time"},
                    },
                    {
                        "record_id": "rec_completed_future",
                        "scope_codes": ["A"],
                        "title": "未来完成时间",
                        "sort_time": now,
                        "payload": {"record_id": "rec_completed_future"},
                    },
                ],
            )
            store.replace_repair_project_status_index(
                [
                    {
                        "record_id": "rec_completed_today",
                        "state": "completed",
                        "completed_at": now,
                        "latest_followup_sort_time": now,
                    },
                    {
                        "record_id": "rec_completed_old",
                        "state": "completed",
                        "completed_at": now - 90 * 24 * 3600,
                        "latest_followup_sort_time": now,
                    },
                    {
                        "record_id": "rec_completed_without_time",
                        "state": "completed",
                        "completed_at": 0,
                        "latest_followup_sort_time": now,
                    },
                    {
                        "record_id": "rec_completed_future",
                        "state": "completed",
                        "completed_at": now + 30 * 24 * 3600,
                        "latest_followup_sort_time": now,
                    },
                ],
                source_signature="completed-at-v2",
            )

            page = store.query_repair_project_status_page(
                "repair_projects",
                scope="A",
                state="completed",
                period="month",
            )

            self.assertEqual(page["total"], 1)
            self.assertEqual(
                page["records"][0]["record_id"],
                "rec_completed_today",
            )
            self.assertGreater(
                page["records"][0]["_repair_status_index"]["completed_at"],
                0,
            )
            all_completed = store.query_repair_project_status_page(
                "repair_projects",
                scope="A",
                state="completed",
                period="all",
            )
            self.assertEqual(all_completed["total"], 4)

    def test_repair_project_status_page_orders_workflow_before_recency(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            now = time.time()
            project_rows = [
                ("rec_completed", "已完成", now + 30),
                ("rec_in_progress_old", "维修中旧记录", now + 10),
                ("rec_not_started", "未开始", now),
                ("rec_in_progress_new", "维修中新记录", now + 20),
            ]
            store.replace_repair_snapshot(
                "repair_projects",
                records=[
                    {
                        "record_id": record_id,
                        "scope_codes": ["A"],
                        "title": title,
                        "sort_time": sort_time,
                        "payload": {"record_id": record_id},
                    }
                    for record_id, title, sort_time in project_rows
                ],
            )
            store.replace_repair_project_status_index(
                [
                    {
                        "record_id": "rec_completed",
                        "state": "completed",
                        "workflow": "维修完成",
                        "completed_at": now + 30,
                    },
                    {
                        "record_id": "rec_in_progress_old",
                        "state": "active",
                        "workflow": "维修中",
                        "latest_followup_sort_time": now + 10,
                    },
                    {
                        "record_id": "rec_not_started",
                        "state": "active",
                        "workflow": "未开始",
                    },
                    {
                        "record_id": "rec_in_progress_new",
                        "state": "active",
                        "workflow": "维修中",
                        "latest_followup_sort_time": now + 20,
                    },
                ],
                source_signature="workflow-order-v1",
            )

            first_page = store.query_repair_project_status_page(
                "repair_projects",
                scope="A",
                state="all",
                limit=2,
            )
            second_page = store.query_repair_project_status_page(
                "repair_projects",
                scope="A",
                state="all",
                limit=2,
                offset=2,
            )
            active_page = store.query_repair_project_status_page(
                "repair_projects",
                scope="A",
                state="active",
            )

            self.assertEqual(first_page["total"], 4)
            self.assertEqual(
                [item["record_id"] for item in first_page["records"]],
                ["rec_not_started", "rec_in_progress_new"],
            )
            self.assertEqual(
                [item["record_id"] for item in second_page["records"]],
                ["rec_in_progress_old", "rec_completed"],
            )
            self.assertEqual(
                [item["record_id"] for item in active_page["records"]],
                [
                    "rec_not_started",
                    "rec_in_progress_new",
                    "rec_in_progress_old",
                ],
            )

    def test_business_operation_audit_preserves_context_on_completion(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            started = store.record_business_operation_audit(
                audit_id="audit_notice_1",
                operation_id="job_1",
                domain="notice",
                action="start",
                status="started",
                scope="A",
                actor_open_id="ou_test",
                actor_name="测试用户",
                source_record_id="rec_source",
                metadata={"work_type": "maintenance"},
            )
            completed = store.record_business_operation_audit(
                audit_id="audit_notice_1",
                status="success",
                target_record_id="rec_target",
                remote_written=True,
                message_sent=False,
                warning="个人消息发送失败",
            )
            items = store.list_business_operation_audits(
                domain="notice",
                statuses=("success",),
                scope="A",
            )

            self.assertEqual(started["status"], "started")
            self.assertEqual(completed["actor_open_id"], "ou_test")
            self.assertEqual(completed["source_record_id"], "rec_source")
            self.assertEqual(completed["target_record_id"], "rec_target")
            self.assertTrue(completed["remote_written"])
            self.assertFalse(completed["message_sent"])
            self.assertGreater(completed["completed_at"], 0)
            self.assertEqual(len(items), 1)
            self.assertEqual(items[0]["operation_id"], "job_1")

    def test_business_operation_audit_failure_does_not_block_business(self):
        class StoreWithoutAuditApi:
            pass

        store = StoreWithoutAuditApi()
        with self.assertLogs(
            "lan_bitable_template_portal.operation_audit",
            level="WARNING",
        ):
            audit_id = begin_business_audit(
                store,  # type: ignore[arg-type]
                domain="notice",
                action="start",
                operation_id="job_without_audit_store",
            )
            result = finish_business_audit(
                store,  # type: ignore[arg-type]
                audit_id,
                success=True,
                result={"target_record_id": "rec_target"},
            )

        self.assertTrue(audit_id)
        self.assertEqual(result, {})

    def test_business_operation_audit_metadata_rejects_nested_payloads(self):
        metadata = safe_audit_metadata(
            {
                "work_type": "maintenance",
                "file_name": "x" * 800,
                "paired_upload_status": {"secret": "must_not_persist"},
                "unknown": "ignored",
            }
        )

        self.assertEqual(metadata["work_type"], "maintenance")
        self.assertEqual(len(metadata["file_name"]), 503)
        self.assertNotIn("paired_upload_status", metadata)
        self.assertNotIn("unknown", metadata)

    def test_business_operation_audit_retry_clears_previous_result(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            store.record_business_operation_audit(
                audit_id="audit_retry",
                operation_id="job_retry",
                domain="notice",
                action="update",
                status="failed",
                remote_written=True,
                message_sent=True,
                warning="旧告警",
                error_stage="remote_write",
                error="旧错误",
            )
            restarted = store.record_business_operation_audit(
                audit_id="audit_retry",
                status="started",
            )

            self.assertEqual(restarted["status"], "started")
            self.assertEqual(restarted["completed_at"], 0)
            self.assertFalse(restarted["remote_written"])
            self.assertFalse(restarted["message_sent"])
            self.assertEqual(restarted["warning"], "")
            self.assertEqual(restarted["error_stage"], "")
            self.assertEqual(restarted["error"], "")

    def test_business_operation_audit_cleanup_is_bounded_by_age(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "state.sqlite3"
            store = LanPortalStateStore(db_path)
            for audit_id, status in (
                ("audit_old_success", "success"),
                ("audit_old_started", "started"),
                ("audit_recent", "success"),
            ):
                store.record_business_operation_audit(
                    audit_id=audit_id,
                    domain="repair",
                    action="update_project",
                    status=status,
                )
            old_time = time.time() - 120 * 24 * 3600
            conn = sqlite3.connect(db_path)
            try:
                conn.execute(
                    """
                    UPDATE business_operation_audits
                    SET updated_at = ?
                    WHERE audit_id IN ('audit_old_success', 'audit_old_started')
                    """,
                    (old_time,),
                )
                conn.commit()
            finally:
                conn.close()

            removed = store.cleanup_business_operation_audits(
                terminal_retention_seconds=90 * 24 * 3600,
                unfinished_retention_seconds=7 * 24 * 3600,
                max_delete=10,
            )
            remaining = store.list_business_operation_audits(limit=10)

            self.assertEqual(removed, 2)
            self.assertEqual(
                [item["audit_id"] for item in remaining],
                ["audit_recent"],
            )

    def test_deletion_audit_outbox_is_idempotent_and_retryable(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "state.sqlite3"
            store = LanPortalStateStore(db_path)
            first = store.enqueue_deletion_audit(
                audit_id="del_test_1",
                operation_id="delete-operation-1",
                source="网页",
                deletion_type="删除通告",
                business_type="维保通告",
                scope="A",
                record_name="测试维保通告",
                actor_name="测试用户",
                actor_open_id="ou_test",
                target_record_id="rec_target",
                remote_deleted=True,
                detail={"reason": "用户确认删除"},
            )
            duplicate = store.enqueue_deletion_audit(
                audit_id="del_test_1",
                operation_id="delete-operation-1",
                source="网页",
                deletion_type="删除通告",
                business_type="维保通告",
                scope="A",
                record_name="更新后的日志名称",
                actor_name="测试用户",
                actor_open_id="ou_test",
                target_record_id="rec_target",
                remote_deleted=True,
            )

            self.assertEqual(first["status"], "pending")
            self.assertEqual(duplicate["status"], "pending")
            self.assertEqual(duplicate["record_name"], "测试维保通告")
            self.assertEqual(len(store.list_pending_deletion_audits()), 1)

            failed = store.mark_deletion_audit_failed(
                "del_test_1",
                error="temporary timeout",
            )
            self.assertEqual(failed["status"], "failed")
            self.assertEqual(failed["attempts"], 1)
            self.assertEqual(store.list_pending_deletion_audits(), [])

            conn = sqlite3.connect(db_path)
            try:
                conn.execute(
                    "UPDATE deletion_audit_outbox SET next_attempt_at = 0 "
                    "WHERE audit_id = ?",
                    ("del_test_1",),
                )
                conn.commit()
                row_count = conn.execute(
                    "SELECT COUNT(*) FROM deletion_audit_outbox"
                ).fetchone()[0]
            finally:
                conn.close()
            self.assertEqual(row_count, 1)
            self.assertEqual(len(store.list_pending_deletion_audits()), 1)

            uploaded = store.mark_deletion_audit_uploaded(
                "del_test_1",
                remote_record_id="rec_audit",
            )
            self.assertEqual(uploaded["status"], "uploaded")
            self.assertEqual(uploaded["remote_record_id"], "rec_audit")
            self.assertEqual(store.list_pending_deletion_audits(), [])

            replay = store.enqueue_deletion_audit(
                audit_id="del_test_1",
                operation_id="delete-operation-1",
                source="网页",
                deletion_type="删除通告",
                business_type="维保通告",
                scope="A",
                record_name="重复回调",
                target_record_id="rec_target",
                remote_deleted=True,
            )
            self.assertEqual(replay["status"], "uploaded")
            self.assertEqual(replay["remote_record_id"], "rec_audit")
            self.assertEqual(replay["record_name"], "测试维保通告")
            self.assertEqual(store.list_pending_deletion_audits(), [])

            pending = store.enqueue_deletion_audit(
                audit_id="del_pending_kept",
                source="Qt",
                deletion_type="移除未上传通告",
                local_only=True,
            )
            self.assertEqual(pending["status"], "pending")
            old_time = time.time() - 60 * 24 * 3600
            conn = sqlite3.connect(db_path)
            try:
                conn.execute(
                    "UPDATE deletion_audit_outbox SET uploaded_at = ?, "
                    "updated_at = ? WHERE audit_id = ?",
                    (old_time, old_time, "del_test_1"),
                )
                conn.execute(
                    "UPDATE deletion_audit_outbox SET created_at = ?, "
                    "updated_at = ? WHERE audit_id = ?",
                    (old_time, old_time, "del_pending_kept"),
                )
                conn.commit()
            finally:
                conn.close()

            removed = store.cleanup_uploaded_deletion_audits(
                retention_seconds=30 * 24 * 3600,
                max_delete=10,
            )
            self.assertEqual(removed, 1)
            self.assertEqual(
                [item["audit_id"] for item in store.list_pending_deletion_audits()],
                ["del_pending_kept"],
            )

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
