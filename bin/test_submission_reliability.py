import sys
import tempfile
import time
import unittest
import io
import logging
import queue
from logging.handlers import QueueHandler
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lan_bitable_template_portal.portal_service import (
    MaintenancePortalService, FieldMeta, PortalError, PortalConflictError,
    REPAIR_FOLLOWUP_TABLE_ID, REPAIR_MANAGEMENT_TABLE_ID, REPAIR_SOURCE_APP_TOKEN,
)
from lan_bitable_template_portal.repair_operations import repair_mutation
from lan_bitable_template_portal.state_store import LanPortalStateStore
from lan_bitable_template_portal.server import PortalRuntime
import lan_bitable_template_portal.server as server


class TestService(MaintenancePortalService):
    @repair_mutation("followup_create")
    def create_test(self, fields, *, operation_id="", scope="A", summary_record_id="project"):
        prepared = {**fields, "first": self.first}
        result = self._create_record_fields(app_token=REPAIR_SOURCE_APP_TOKEN, table_id=REPAIR_FOLLOWUP_TABLE_ID, fields=prepared)
        record_id = self._created_record_id(result)
        self.first = "no"
        self._upsert_repair_snapshot_fields(source_key="followups", record_id=record_id, fields=prepared)
        return {"record_id": record_id, "summary_record_id": summary_record_id, "fields": prepared}

    @repair_mutation("project_update")
    def update_test(self, fields, *, operation_id="", record_id="project", scope="A"):
        self._patch_record_fields_exact(app_token=REPAIR_SOURCE_APP_TOKEN, table_id=REPAIR_MANAGEMENT_TABLE_ID, record_id=record_id, fields=fields)
        self._upsert_repair_snapshot_fields(source_key="projects", record_id=record_id, fields=fields)
        return {"record_id": record_id, "fields": fields}


class SubmissionReliabilityTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = TestService(enable_repair_snapshots=True)
        self.store = LanPortalStateStore(Path(self.tmp.name) / "state.sqlite3")
        self.service._state_store = self.store
        self.service.first = "yes"
        for name in ("_auth_headers", "_upsert_repair_snapshot_fields", "_schedule_repair_sync_task"):
            setattr(self.service, name, Mock(return_value={}))
        self.service._repair_physical_record_fields = lambda table, fields: fields
        self.service._repair_logical_record_fields = lambda table, fields: fields
        self.service._repair_snapshot_record_version = Mock(return_value="v2")
        self.service._request_payload = Mock(return_value={"code": 0, "data": {"record": {"record_id": "remote1"}}})
        self.service._request_json = Mock(side_effect=AssertionError("unexpected network read"))
        self.old_store = PortalRuntime.state_store
        PortalRuntime.state_store = self.store

    def tearDown(self):
        PortalRuntime.state_store = self.old_store
        self.store.shutdown_write_worker(timeout=2)
        self.tmp.cleanup()

    def test_local_failure_keeps_remote_id_and_recovers_without_second_create(self):
        self.service._upsert_repair_snapshot_fields.side_effect = OSError("disk busy")
        with self.assertRaises(OSError):
            self.service.create_test({"text": "one"}, operation_id="create")
        saved = self.store.get_repair_management_operation("create")
        self.assertEqual((saved["status"], saved["record_id"]), ("remote_written", "remote1"))
        self.service._upsert_repair_snapshot_fields.side_effect = None
        result = self.service.create_test({"text": "one"}, operation_id="create")
        self.assertEqual(result["record_id"], "remote1")
        self.service._request_payload.assert_called_once()

    def test_derived_first_followup_value_does_not_change_request_identity(self):
        first = self.service.create_test({"text": "one"}, operation_id="create")
        retry = self.service.create_test({"text": "one"}, operation_id="create")
        self.assertEqual(retry["record_id"], first["record_id"])
        self.service._request_payload.assert_called_once()
        with self.assertRaisesRegex(PortalError, "内容已变化"):
            self.service.create_test({"text": "changed"}, operation_id="create")

    def test_unknown_create_never_blindly_retries(self):
        self.service._request_payload.side_effect = TimeoutError("response lost")
        with self.assertRaises(TimeoutError):
            self.service.create_test({"text": "one"}, operation_id="unknown")
        state = self.service.repair_operation_status("unknown", recover=True)
        self.assertEqual(state["status"], "uncertain")
        self.assertFalse(state["retryable"])
        with self.assertRaisesRegex(PortalError, "待核实"):
            self.service.create_test({"text": "one"}, operation_id="unknown")
        self.service._request_payload.assert_called_once()

    def test_interrupted_before_write_is_retryable_not_permanently_started(self):
        self.store.begin_repair_management_operation("interrupted", operation_type="project_update", scope="A", payload_hash="h")
        self.store.update_repair_management_operation("interrupted", status="started", result={"checkpoint": {"phase": "preparing", "pid": 0}})
        state = self.service.repair_operation_status("interrupted")
        self.assertEqual(state["status"], "failed")
        self.assertTrue(state["retryable"])

    def test_explicit_remote_rejection_is_safe_to_edit(self):
        self.service._request_payload.return_value = {"code": 1254060, "msg": "TextFieldConvFail"}
        with self.assertRaises(PortalError):
            self.service.create_test({"text": "one"}, operation_id="rejected")
        self.assertTrue(self.service.repair_operation_status("rejected")["retryable"])

    def test_unknown_update_can_be_verified_without_resending(self):
        self.service._request_payload.side_effect = TimeoutError("response lost")
        with self.assertRaises(TimeoutError):
            self.service.update_test({"text": "one"}, operation_id="update")
        self.service._request_json.side_effect = None
        self.service._request_json.return_value = {"data": {"record": {"fields": {"text": [{"text": "one", "type": "text"}]}}}}
        self.service._load_table_fields = Mock(return_value=([], {}))
        state = self.service.repair_operation_status("update", recover=True)
        self.assertEqual(state["status"], "completed")
        self.service._request_payload.assert_called_once()

    def test_remote_change_detected_even_when_local_version_matches(self):
        meta = FieldMeta("field", "text", "Text", 1, False, {}, [], False)
        with self.assertRaises(PortalConflictError):
            self.service._assert_repair_remote_unchanged({"raw_fields": {"text": "old"}}, {"raw_fields": {"text": "new"}}, {"text": meta})

    def test_due_task_after_twenty_delayed_tasks_is_processed(self):
        for index in range(21):
            op = f"sync-{index}"
            self.store.begin_repair_management_operation(op, operation_type="followup_summary_sync", scope="A", payload_hash=op, summary_record_id="project")
            self.store.update_repair_management_operation(op, status="sync_pending", result={"available_at": time.time() + 600 if index < 20 else 0})
        self.service._execute_repair_sync_task = Mock(return_value=[])
        result = self.service._process_due_repair_sync_tasks_unlocked(limit=20)
        self.assertEqual(result["completed"], 1)
        self.assertEqual(self.store.get_repair_management_operation("sync-20")["status"], "completed")

    def test_operation_claim_rejects_stale_version(self):
        old = self.store.begin_repair_management_operation("cas", operation_type="followup_summary_sync", scope="A", payload_hash="h")
        self.store.update_repair_management_operation("cas", status="sync_pending")
        self.assertFalse(self.store.update_repair_management_operation("cas", status="completed", expected_updated_at=old["updated_at"]))

    def test_delete_collects_all_501_followups_and_checks_parent_last(self):
        children = {f"child{i}" for i in range(501)}
        def load(summary, **kwargs):
            self.assertIsNone(kwargs["limit"])
            self.assertTrue(kwargs["force_refresh"])
            return [], {}, [{"record_id": rid} for rid in sorted(children)]
        deleted = []
        def delete(**kwargs):
            rid = kwargs["record_id"]
            if rid == "recProject": self.assertFalse(children)
            else: children.discard(rid)
            deleted.append(rid)
        self.service._load_repair_followups_for_summary = load
        self.service._delete_record_fields = delete
        self.service._delete_repair_snapshot_item = Mock()
        self.service._remove_repair_source_projection = Mock(return_value=True)
        op = self.store.begin_repair_management_operation("delete", operation_type="project_delete", scope="A", payload_hash="h", summary_record_id="recProject")
        self.service._execute_repair_project_delete_task(op)
        self.assertEqual(len(deleted), 502)
        self.assertEqual(deleted[-1], "recProject")

    def test_fresh_projection_uses_accepted_fields_without_remote_query(self):
        payload = {"_accepted_target_fields": {"status": "ended"}, "_accepted_target_at": time.time()}
        with patch.object(server, "query_record_by_id", side_effect=AssertionError("extra GET")):
            ok, result = PortalRuntime._projection_record(payload, "record", "维保通告")
        self.assertTrue(ok)
        self.assertEqual(result["fields"], {"status": "ended"})

    def test_source_sync_failure_is_durable_secondary_work(self):
        payload = {"source_record_id": "recSource", "target_record_id": "recTarget", "work_type": "maintenance", "notice_type": "维保通告"}
        self.assertIn("后台", PortalRuntime._queue_source_end_sync(payload, "recTarget", "维保通告"))
        with patch.object(server, "query_record_by_id", return_value=(False, "offline")):
            PortalRuntime.process_source_end_sync()
        events = self.store.list_outbox_events("notice_source_finalize", status="pending")
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["attempts"], 1)

    def test_superseded_projection_reads_latest_state(self):
        payload = {"_accepted_target_fields": {"status": "ended"}, "_accepted_target_at": time.time() - 5}
        with patch.object(self.store, "list_notice_remote_operations_for_target", return_value=[
            {"operation_id": "newer", "updated_at": time.time(), "status": "completed"}
        ]), patch.object(server, "query_record_by_id", return_value=(True, {"fields": {"status": "active"}})) as query:
            self.assertEqual(PortalRuntime._projection_record(payload, "record", "维保通告")[1]["fields"]["status"], "active")
            query.assert_called_once()

    def test_old_source_end_task_does_not_end_reopened_notice(self):
        PortalRuntime._queue_source_end_sync({"source_record_id": "recSource", "work_type": "maintenance"}, "recTarget", "维保通告")
        with patch.object(PortalRuntime, "service") as service, patch.object(server, "query_record_by_id", return_value=(True, {"fields": {"status": "active"}})):
            service._target_record_lifecycle.return_value = {"active": True, "finished": False}
            PortalRuntime.process_source_end_sync()
            service.sync_notice_source_ended_fields.assert_not_called()
        self.assertEqual(len(self.store.list_outbox_events("notice_source_finalize", status="done")), 1)

    def test_logging_formats_once_and_does_not_echo_stream_twice(self):
        from upload_event_module.logger import SafeConsoleHandler, StreamLogger
        console = io.StringIO()
        handler = SafeConsoleHandler(console)
        handler.setFormatter(logging.Formatter("%(levelname)s:%(message)s"))
        target = logging.Logger("test", logging.INFO)
        target.addHandler(handler)
        writer = StreamLogger(console, logging.ERROR)
        writer.logger = target
        writer.write("once\n")
        self.assertEqual(console.getvalue(), "once\n")
        items = queue.Queue()
        queued = QueueHandler(items)
        queued.setFormatter(logging.Formatter("%(message)s"))
        queued.emit(logging.LogRecord("test", logging.ERROR, __file__, 1, "message", (), None))
        handler.emit(items.get())
        self.assertEqual(console.getvalue(), "once\nERROR:message\n")

    def test_operation_api_checks_stored_scope_before_recovery(self):
        from clipflow_backend.main import FastAPIPortalController
        from fastapi.testclient import TestClient
        controller = FastAPIPortalController(host="127.0.0.1", port=18766)
        self.store.begin_repair_management_operation("scope-check", operation_type="project_update", scope="B", payload_hash="h")
        with patch.object(PortalRuntime, "service", self.service), patch.object(self.service, "repair_operation_status", return_value={"status": "processing"}) as recover:
            client = TestClient(controller._build_app())
            with patch.object(controller, "_current_session", return_value=None):
                self.assertEqual(client.get("/api/repair-management/operations/scope-check").status_code, 401)
            with patch.object(controller, "_current_session", return_value={"user": {"open_id": "test"}}), patch.object(controller, "_authorized_scope_or_error", side_effect=PortalError("denied")) as authorize:
                response = client.post("/api/repair-management/operations/scope-check?scope=A")
                self.assertGreaterEqual(response.status_code, 400)
                self.assertEqual(authorize.call_args.args[1], "B")
                recover.assert_not_called()
            with patch.object(controller, "_current_session", return_value={"user": {"open_id": "test"}}), patch.object(controller, "_authorized_scope_or_error", return_value="B"):
                self.assertEqual(client.post("/api/repair-management/operations/scope-check").status_code, 200)
                recover.assert_called_once_with("scope-check", recover=True)

    def test_source_end_recovery_preserves_actual_end_time(self):
        config = server.get_field_config("维保通告")
        with patch.object(PortalRuntime, "service", self.service):
            payload = PortalRuntime._source_end_payload({}, {config["actual_end"]: "2026-09-20 12:34:56"}, "维保通告")
            self.assertEqual(payload["ended_at"], "2026-09-20 12:34:56")
            with self.assertRaisesRegex(RuntimeError, "缺少实际结束"):
                PortalRuntime._source_end_payload({}, {}, "维保通告")


if __name__ == "__main__":
    unittest.main()
