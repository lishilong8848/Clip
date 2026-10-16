"""Fault recovery after the original Qt executor has returned its outcome."""
import copy
import sqlite3
from concurrent.futures import Future
from contextlib import ExitStack, contextmanager
from unittest import TestCase
from unittest.mock import patch

from bin import test_event_notice_priority as fixtures
from lan_bitable_template_portal import event_priority as ep
from lan_bitable_template_portal.portal_service import STATE_NS_ACTION_JOB
from lan_bitable_template_portal.server import PortalRuntime


class EventNoticeCompletionRecoveryTests(TestCase):
    setUp = fixtures.EventNoticePriorityTests.setUp
    tearDown = fixtures.EventNoticePriorityTests.tearDown
    bare_service = fixtures.EventNoticePriorityTests.bare_service
    qt_command = fixtures.EventNoticePriorityTests.qt_command

    def ready_job(self):
        service = self.bare_service()
        event = fixtures._event_payload("fixture event", target="rec-recovery-event")
        ep.record_result(self.store, event, "upload")
        command = self.qt_command()
        job = service.create_qt_event_wait_job(command, ep.blockers(self.store))
        ep.record_result(self.store, event, "end")
        service.release_event_waiting_jobs()
        return service, command, job["job_id"]

    @contextmanager
    def runtime(self, service, result):
        with ExitStack() as stack:
            stack.enter_context(patch.object(PortalRuntime, "state_store", self.store))
            stack.enter_context(patch.object(PortalRuntime, "service", service))
            stack.enter_context(patch.object(PortalRuntime, "ensure_action_worker"))
            execute = stack.enter_context(patch.object(PortalRuntime, "_execute_local_notice_upload", return_value=result))
            yield execute

    def assert_result(self, service, job_id, result):
        phase = "success" if result["ok"] else "failed"
        job = service.get_job(job_id)
        stored = self.store.get_document(STATE_NS_ACTION_JOB, job_id)
        self.assertEqual((job["phase"], stored["phase"]), (phase, phase))
        self.assertEqual(job["qt_result"], result)
        self.assertEqual(stored["qt_result"], result)
        receipts = self.store.list_outbox_events("qt_action")
        self.assertEqual(len(receipts), 1)
        self.assertEqual(receipts[0]["payload"]["payload"]["result"], result)

    def test_queue_mark_failure_keeps_original_success_or_failure_and_retries_only_locally(self):
        for ok in (True, False):
            with self.subTest(ok=ok):
                service, command, job_id = self.ready_job()
                result = {"ok": ok, "real_record_id": "rec-result", "message": "original outcome"}
                with fixtures.closing(sqlite3.connect(self.store.db_path)) as conn:
                    conn.execute("CREATE TRIGGER fail_terminal_queue BEFORE UPDATE ON runtime_task_queue "
                                 "WHEN NEW.status IN ('done', 'failed') BEGIN SELECT RAISE(ABORT, 'fixture queue'); END")
                with self.runtime(service, result) as execute:
                    with self.assertLogs(level="ERROR"):
                        PortalRuntime._process_maintenance_action_job(job_id)
                    self.assert_result(service, job_id, result)
                    self.assertTrue(self.store.get_document(STATE_NS_ACTION_JOB, job_id)["qt_completion_pending"])
                    with fixtures.closing(sqlite3.connect(self.store.db_path)) as conn:
                        conn.execute("DROP TRIGGER fail_terminal_queue")
                    # A late bookkeeping exception cannot replace a committed outcome.
                    PortalRuntime._finish_qt_event_wait_job(job_id, command, {"ok": not ok, "message": "late error"})
                    self.assertTrue(PortalRuntime._runtime_queue_job_processable("qt_action", job_id))
                    PortalRuntime._process_maintenance_action_job(job_id)
                    self.assertEqual(PortalRuntime.execute_local_notice_upload(command), result)
                    self.assert_result(service, job_id, result)
                    execute.assert_called_once()
                queue = next(row for row in self.store.list_runtime_queue_items("qt_action", statuses=("done", "failed")) if row["job_id"] == job_id)
                self.assertEqual(queue["status"], "done" if ok else "failed")
                # Keep each subtest's receipt count independent without touching business data.
                self.store.delete_document(STATE_NS_ACTION_JOB, job_id)
                with fixtures.closing(self.store._connect()) as conn:
                    conn.execute("DELETE FROM event_outbox")
                    conn.commit()

    def test_receipt_failure_restart_and_new_event_reconcile_without_uploading_again(self):
        service, command, job_id = self.ready_job()
        result = {"ok": True, "real_record_id": "rec-result", "record_version": "version-1"}
        with self.runtime(service, result) as execute:
            with (patch.object(self.store, "enqueue_outbox_event", side_effect=OSError("fixture receipt unavailable")),
                  self.assertLogs(level="ERROR")):
                PortalRuntime._process_maintenance_action_job(job_id)
            execute.assert_called_once()
            pending = self.store.get_document(STATE_NS_ACTION_JOB, job_id)
            self.assertEqual(pending["phase"], "upload_waiting")
            self.assertTrue(pending["qt_completion_pending"])
            self.assertEqual(pending["qt_result"], result)
            self.assertFalse(self.store.list_outbox_events("qt_action"))
            self.assertEqual(PortalRuntime.execute_local_notice_upload(command)["job_id"], job_id)
        restored = self.bare_service()
        restored._jobs = restored._load_action_jobs_from_state()
        self.assertIn(job_id, restored.recoverable_action_job_ids())
        ep.record_result(self.store, fixtures._event_payload("new event", target="rec-new-event"), "upload")
        with self.runtime(restored, result) as execute:
            self.assertTrue(PortalRuntime._runtime_queue_job_processable("qt_action", job_id))
            PortalRuntime._process_maintenance_action_job(job_id)
            self.assert_result(restored, job_id, result)
            self.assertEqual(PortalRuntime.execute_local_notice_upload(command), result)
            execute.assert_not_called()
        self.assertEqual(restored.get_job(job_id)["message_warning"], "")

    def test_unavailable_receipt_and_pending_storage_and_requeue_recover_from_original_lease(self):
        service, _, job_id = self.ready_job()
        result = {"ok": True, "real_record_id": "rec-result"}
        self.store.lease_runtime_queue_items("qt_action", limit=1, lease_seconds=30)
        with self.runtime(service, result) as execute:
            with (patch.object(self.store, "enqueue_outbox_event", side_effect=OSError("fixture receipt")),
                  patch.object(self.store, "put_document", side_effect=OSError("fixture pending")),
                  patch.object(service, "_persist_action_job_locked"),
                  patch.object(self.store, "requeue_runtime_queue_item", side_effect=OSError("fixture requeue")),
                  self.assertLogs(level="ERROR")):
                PortalRuntime._process_maintenance_action_job(job_id)
            self.assertTrue(service.get_job(job_id)["qt_completion_pending"])
            self.assertEqual(self.store.list_runtime_queue_items("qt_action", statuses=("processing",))[0]["status"], "processing")
            self.assertFalse(service.delete_action_job(job_id))
            with patch("lan_bitable_template_portal.state_store.time.time", return_value=fixtures.time.time() + 31):
                self.assertTrue(self.store.lease_runtime_queue_items("qt_action", limit=1))
            PortalRuntime._process_maintenance_action_job(job_id)
            self.assert_result(service, job_id, result)
            execute.assert_called_once()
            self.assertEqual(service.cleanup_action_jobs(success_retention_seconds=60)["removed_total"], 0)

    def test_audit_failure_restart_does_not_change_committed_business_outcome(self):
        service, command, job_id = self.ready_job()
        service.mark_job(job_id, business_audit_id="fixture-audit")
        result = {"ok": True, "real_record_id": "rec-result"}
        with self.runtime(service, result) as execute:
            with (patch("lan_bitable_template_portal.operation_audit.finish_business_audit", side_effect=OSError("fixture audit")),
                  self.assertLogs(level="ERROR")):
                PortalRuntime._process_maintenance_action_job(job_id)
            self.assert_result(service, job_id, result)
            execute.assert_called_once()
        restored = self.bare_service()
        restored._jobs = restored._load_action_jobs_from_state()
        self.store.reset_runtime_queue_incomplete()
        self.assertIn(job_id, restored.recoverable_action_job_ids())
        self.store.upsert_runtime_queue_item("qt_action", job_id)
        with self.runtime(restored, result) as execute:
            with patch("lan_bitable_template_portal.operation_audit.finish_business_audit", return_value={"status": "success"}) as audit:
                self.assertTrue(PortalRuntime._runtime_queue_job_processable("qt_action", job_id))
                PortalRuntime._process_maintenance_action_job(job_id)
                audit.assert_called_once()
                self.assertTrue(audit.call_args.kwargs["success"])
            self.assertEqual(PortalRuntime.execute_local_notice_upload(command), result)
            self.assert_result(restored, job_id, result)
            execute.assert_not_called()

    def test_audit_helper_empty_result_is_retried_without_losing_receipt(self):
        service, _, job_id = self.ready_job()
        service.mark_job(job_id, business_audit_id="fixture-audit")
        result = {"ok": False, "message": "original failure"}
        with self.runtime(service, result) as execute:
            with (patch("lan_bitable_template_portal.operation_audit.finish_business_audit", return_value={}),
                  self.assertLogs(level="ERROR")):
                PortalRuntime._process_maintenance_action_job(job_id)
            self.assert_result(service, job_id, result)
            with patch("lan_bitable_template_portal.operation_audit.finish_business_audit", return_value={"status": "failed"}):
                PortalRuntime._process_maintenance_action_job(job_id)
            self.assert_result(service, job_id, result)
            execute.assert_called_once()

    def test_failed_business_result_survives_receipt_failure_and_restart(self):
        service, _, job_id = self.ready_job()
        result = {"ok": False, "message": "fixture original validation failure", "needs_target_selection": True}
        with self.runtime(service, result) as execute:
            with patch.object(self.store, "enqueue_outbox_event", side_effect=OSError("fixture receipt")), self.assertLogs(level="ERROR"):
                PortalRuntime._process_maintenance_action_job(job_id)
            self.assertEqual(service.get_job(job_id)["phase"], "upload_waiting")
            self.assertEqual(service.get_job(job_id)["qt_result"], result)
            execute.assert_called_once()
        restored = self.bare_service()
        restored._jobs = restored._load_action_jobs_from_state()
        with self.runtime(restored, {"ok": True}) as execute:
            PortalRuntime._process_maintenance_action_job(job_id)
            self.assert_result(restored, job_id, result)
            execute.assert_not_called()

    def test_pending_bookkeeping_is_not_pruned_and_delivered_receipt_is_not_reissued(self):
        service, _, job_id = self.ready_job()
        result = {"ok": True, "real_record_id": "rec-result"}
        with self.runtime(service, result) as execute:
            with patch.object(self.store, "put_action_job_with_queue", side_effect=OSError("fixture queue")), self.assertLogs(level="ERROR"):
                PortalRuntime._process_maintenance_action_job(job_id)
            self.assert_result(service, job_id, result)
            self.assertTrue(service.get_job(job_id)["qt_completion_pending"])
            receipt = self.store.list_outbox_events("qt_action")[0]
            self.store.mark_outbox_event(receipt["id"], "done")
            self.assertFalse(service.delete_action_job(job_id))
            with patch.object(service, "_job_epoch", return_value=fixtures.time.time() - 86400):
                self.assertEqual(service.cleanup_action_jobs(success_retention_seconds=60)["removed_total"], 0)
            with patch("lan_bitable_template_portal.portal_service.ACTION_JOB_MAX_RETAINED", 0):
                service._trim_jobs_locked()
                self.assertIn(job_id, service._jobs)
                restored = self.bare_service()
                restored._jobs = restored._load_action_jobs_from_state()
                self.assertIn(job_id, restored.recoverable_action_job_ids())
            PortalRuntime._process_maintenance_action_job(job_id)
            self.assertFalse(self.store.list_outbox_events("qt_action"))
            receipts = self.store.list_outbox_events("qt_action", status="done")
            self.assertEqual([row["id"] for row in receipts], [receipt["id"]])
            self.assertEqual(receipts[0]["payload"]["payload"]["result"], result)
            self.assertFalse(service.get_job(job_id)["qt_completion_pending"])
            execute.assert_called_once()

    def test_future_error_preserves_committed_outcome_instead_of_marking_failed(self):
        service, _, job_id = self.ready_job()
        result = {"ok": True, "real_record_id": "rec-result"}
        with self.runtime(service, result) as execute:
            PortalRuntime._process_maintenance_action_job(job_id)
            failed_future = Future()
            failed_future.set_exception(OSError("fixture late callback"))
            with self.assertLogs(level="ERROR"):
                PortalRuntime._action_future_done(job_id, failed_future, service, self.store)
            self.assert_result(service, job_id, result)
            PortalRuntime._process_maintenance_action_job(job_id)
            execute.assert_called_once()

    def test_compaction_preserves_pending_outcome_and_settled_qt_replay(self):
        for ok in (True, False):
            with self.subTest(ok=ok):
                service, _, job_id = self.ready_job()
                result = {"ok": ok, "message": "fixture outcome"}
                with self.runtime(service, result) as execute:
                    with patch.object(self.store, "put_action_job_with_queue", side_effect=OSError("fixture queue")), self.assertLogs(level="ERROR"):
                        PortalRuntime._process_maintenance_action_job(job_id)
                    pending = copy.deepcopy(service.get_job(job_id))
                    compact = service._compact_completed_job_locked if ok else service._compact_failed_job_locked
                    with service._jobs_lock:
                        compact(job_id)
                    self.assertEqual(service.get_job(job_id), pending)
                    self.assertEqual(self.store.get_document(STATE_NS_ACTION_JOB, job_id), pending)
                    PortalRuntime._process_maintenance_action_job(job_id)
                    with service._jobs_lock:
                        compact(job_id)
                    self.assertEqual(service.get_job(job_id)["qt_request"], self.qt_command())
                    PortalRuntime._process_maintenance_action_job(job_id)
                    self.assertEqual(service.get_job(job_id)["qt_result"], result)
                    execute.assert_called_once()

    def test_pre_execution_storage_failures_do_not_execute_or_report_business_failure(self):
        service, _, job_id = self.ready_job()
        initial = copy.deepcopy(service.get_job(job_id))
        with self.runtime(service, {"ok": True}) as execute:
            with (patch.object(service, "_persist_action_job_locked", side_effect=OSError("fixture state write")),
                  patch.object(self.store, "requeue_runtime_queue_item", side_effect=OSError("fixture queue write")),
                  self.assertLogs(level="ERROR")):
                PortalRuntime._process_maintenance_action_job(job_id)
            self.assertNotIn(service.get_job(job_id)["phase"], {"success", "failed"})
            self.assertEqual(self.store.get_document(STATE_NS_ACTION_JOB, job_id), initial)
            execute.assert_not_called()
            PortalRuntime._process_maintenance_action_job(job_id)
            execute.assert_called_once()
            self.assert_result(service, job_id, {"ok": True})

    def test_read_failure_is_not_treated_as_missing_or_cancelled_job(self):
        service, _, job_id = self.ready_job()
        with self.runtime(service, {"ok": True}) as execute:
            with patch.object(service, "get_job", side_effect=OSError("fixture read")), self.assertLogs(level="ERROR"):
                self.assertFalse(PortalRuntime._runtime_queue_job_processable("qt_action", job_id))
            self.assertEqual(self.store.list_runtime_queue_items("qt_action")[0]["status"], "queued")
            PortalRuntime._process_maintenance_action_job(job_id)
            execute.assert_called_once()
            self.assert_result(service, job_id, {"ok": True})
