"""Tests for event notice priority.

Covers the stable API contract of ``lan_bitable_template_portal.event_priority``
and the ``MaintenancePortalService`` event-waiting helpers:
  * draft/local events and real IDs bound without any successful remote write
    must never block;
  * a successful ``upload`` record result arms a global blocker;
  * blockers are independent per distinct real target ID;
  * ending/deleting removes only the matching blocker;
  * an active journal survives a missing local Qt row so no false release
    happens;
  * reopening a temporary SQLite store retains blockers;
  * event summaries carry title / occurrence time / source / level / progress;
  * ``put_action_job_with_queue`` atomically persists the job and an
    ``event_waiting`` queue row with no leasable entry while held;
  * non-terminal notice jobs pin referenced attachments past ``expires_at``
    while a terminal job releases the pin;
  * ``public_job`` strips unauthorized event details without mutating the
    stored job.

Tests use a temporary ``LanPortalStateStore`` and an uninitialized bare
``MaintenancePortalService`` (``object.__new__``) whose methods only need
``_jobs``, ``_jobs_lock`` and ``_state_store``.  No application service is
started and no production state is written.
"""
import copy
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from contextlib import closing
from unittest.mock import patch

BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal import event_priority as ep
from lan_bitable_template_portal.portal_service import (
    MaintenancePortalService,
    STATE_NS_ACTION_JOB,
)
from lan_bitable_template_portal.state_store import LanPortalStateStore


def _event_text(occurrence_time="2026-08-31 10:00", source="BMS", progress="处理中"):
    return (
        "【事件通告】状态：开始\n"
        f"【事件发生时间】{occurrence_time}\n"
        f"【来源】{source}\n"
        f"【进展】{progress}"
    )


def _event_payload(
    title,
    *,
    target="",
    active_item_id="",
    occurrence_time="2026-08-31 10:00",
    source="BMS",
    level="I3",
    progress="处理中",
):
    """Build an event payload; ``target`` selects a real (non-local) ID."""
    payload = {
        "notice_type": "事件通告",
        "title": title,
        "text": _event_text(occurrence_time, source, progress),
        "time_str": occurrence_time,
        "event_source": source,
        "level": level,
        "progress": progress,
        "buildings": ["A楼"],
        "building_codes": ["A"],
    }
    if target:
        payload.update(
            active_item_id=target,
            record_id=target,
            target_record_id=target,
        )
    else:
        active = active_item_id or "local-draft"
        payload.update(active_item_id=active, record_id=active)
    return payload


def _maintenance_request(
    *,
    operation_id="qt-op-maintenance",
    record_id="rec-maintenance",
):
    """A non-event notice request that is eligible for event-wait holding."""
    return {
        "action": "start",
        "notice_type": "维保通告",
        "work_type": "maintenance",
        "operation_id": operation_id,
        "record_id": record_id,
        "active_item_id": record_id,
    }


class EventNoticePriorityTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.store = LanPortalStateStore(Path(self._tmp.name) / "state.sqlite3")

    def tearDown(self):
        try:
            self.store.shutdown_write_worker(timeout=2.0)
        finally:
            self._tmp.cleanup()

    def bare_service(self):
        """Bare service without running the heavy production constructor."""
        service = object.__new__(MaintenancePortalService)
        service._jobs_lock = threading.RLock()
        service._jobs = {}
        service._state_store = self.store
        return service

    # ------------------------------------------------------------------
    # Blocking semantics for drafts / real IDs without success
    # ------------------------------------------------------------------
    def test_drafts_and_bound_real_id_without_success_do_not_block(self):
        draft = _event_payload("Draft event", active_item_id="local-draft")
        self.assertTrue(
            self.store.upsert_qt_active_item(draft, section="event", origin="test")
        )

        # A real ID bound in the local UI but with no confirmed remote write.
        bound_without_success = _event_payload(
            "Bound but not uploaded", target="rec-bound-without-success"
        )
        self.assertTrue(
            self.store.upsert_qt_active_item(
                bound_without_success, section="event", origin="test"
            )
        )

        self.assertEqual(ep.blockers(self.store), [])

    def test_real_id_confirmed_by_remote_write_arms_blocker(self):
        payload = _event_payload("Confirmed event", target="rec-confirmed")
        payload["last_remote_write_at"] = 1234567890.0
        self.assertTrue(
            self.store.upsert_qt_active_item(payload, section="event", origin="test")
        )
        blockers = ep.blockers(self.store)
        self.assertEqual(len(blockers), 1)
        self.assertEqual(blockers[0]["title"], "Confirmed event")

    # ------------------------------------------------------------------
    # record_result arms a global blocker
    # ------------------------------------------------------------------
    def test_record_result_upload_arms_global_blocker(self):
        payload = _event_payload("Uploaded event", target="rec-A")
        ep.record_result(self.store, payload, "upload", "rec-A")
        blockers = ep.blockers(self.store)
        self.assertEqual(len(blockers), 1)
        self.assertEqual(blockers[0]["title"], "Uploaded event")

    # ------------------------------------------------------------------
    # Distinct real IDs remain independent
    # ------------------------------------------------------------------
    def test_two_real_events_same_title_distinct_ids_remain_independent(self):
        ev_a = _event_payload("Same title", target="rec-A")
        ev_b = _event_payload("Same title", target="rec-B")
        ep.record_result(self.store, ev_a, "upload", "rec-A")
        ep.record_result(self.store, ev_b, "upload", "rec-B")

        self.assertEqual({event["title"] for event in ep.blockers(self.store)}, {"Same title"})
        self.assertEqual(len(ep.blockers(self.store)), 2)

        # Ending A removes only A's blocker while B remains active.
        ep.record_result(self.store, ev_a, "end", "rec-A")
        remaining = ep.blockers(self.store)
        self.assertEqual(len(remaining), 1)
        self.assertEqual(
            self.store.get_document(ep.NAMESPACE, "rec-A")["state"], "ended"
        )
        self.assertEqual(
            self.store.get_document(ep.NAMESPACE, "rec-B")["state"], "active"
        )

    # ------------------------------------------------------------------
    # Successful end/delete removes only the matching blocker; drafts are harmless
    # ------------------------------------------------------------------
    def test_successful_end_delete_removes_only_matching_blocker_and_draft_harmless(self):
        draft = _event_payload("Draft stays harmless", active_item_id="local-draft")
        self.assertTrue(
            self.store.upsert_qt_active_item(draft, section="event", origin="test")
        )

        ev = _event_payload("Real event", target="rec-delete-me")
        ep.record_result(self.store, ev, "upload", "rec-delete-me")
        self.assertEqual(len(ep.blockers(self.store)), 1)

        ep.record_result(self.store, ev, "delete", "rec-delete-me")
        self.assertEqual(ep.blockers(self.store), [])
        self.assertEqual(
            self.store.get_document(ep.NAMESPACE, "rec-delete-me")["state"], "ended"
        )

    # ------------------------------------------------------------------
    # Active journal persists through missing local Qt row (no false release)
    # ------------------------------------------------------------------
    def test_active_journal_persists_missing_local_qt_row_no_false_release(self):
        # Arm a blocker purely through the persisted journal with no Qt row.
        ep.record_result(
            self.store,
            _event_payload("Only journal", target="rec-missing-qt"),
            "upload",
            "rec-missing-qt",
        )
        self.assertEqual(self.store.list_qt_active_items(), [])
        self.assertNotEqual(ep.blockers(self.store), [])

        service = self.bare_service()
        request = _maintenance_request()
        job = service._base_job(request)
        service._jobs[job["job_id"]] = job
        self.assertEqual(job["phase"], "event_waiting")

        # Blocker exists even without the local Qt row -> no false release.
        self.assertEqual(service.release_event_waiting_jobs(), [])
        self.assertEqual(job["phase"], "event_waiting")

        # After the journal is ended the waiting job is released.
        ep.record_result(
            self.store,
            _event_payload("Only journal", target="rec-missing-qt"),
            "delete",
            "rec-missing-qt",
        )
        released = service.release_event_waiting_jobs()
        self.assertIn(job["job_id"], released)
        self.assertEqual(job["phase"], "accepted")

    # ------------------------------------------------------------------
    # Reopening a temporary store retains blockers
    # ------------------------------------------------------------------
    def test_reopening_temp_store_retains_blockers(self):
        ep.record_result(
            self.store,
            _event_payload("Persistent blocker", target="rec-persist"),
            "upload",
            "rec-persist",
        )
        path = self.store.db_path
        reopened = LanPortalStateStore(path)
        self.addCleanup(reopened.shutdown_write_worker)
        blockers = ep.blockers(reopened)
        self.assertEqual(len(blockers), 1)
        self.assertEqual(blockers[0]["title"], "Persistent blocker")

    # ------------------------------------------------------------------
    # Summary fields
    # ------------------------------------------------------------------
    def test_event_summary_includes_title_occurrence_time_source_level_progress(self):
        ep.record_result(
            self.store,
            _event_payload(
                "Full summary",
                target="rec-summary",
                occurrence_time="2026-08-31 09:30",
                source="火警系统",
                level="I2",
                progress="已到场处置",
            ),
            "upload",
            "rec-summary",
        )
        event = ep.blockers(self.store)[0]
        self.assertEqual(event["title"], "Full summary")
        self.assertEqual(event["occurrence_time"], "2026-08-31 09:30")
        self.assertEqual(event["source"], "火警系统")
        self.assertEqual(event["level"], "I2")
        self.assertEqual(event["progress"], "已到场处置")

    # ------------------------------------------------------------------
    # hold_action_job_for_event / release_event_waiting_jobs / create_qt_event_wait_job
    # ------------------------------------------------------------------
    def test_hold_action_job_for_event_waits_only_when_events_block(self):
        service = self.bare_service()
        request = _maintenance_request()
        job = service._base_job(request)
        service._jobs[job["job_id"]] = job
        self.assertEqual(job["phase"], "accepted")

        # No blocker -> hold does nothing.
        self.assertFalse(service.hold_action_job_for_event(job["job_id"]))
        self.assertEqual(job["phase"], "accepted")
        self.assertNotIn("event_wait", job)

        # Arm a blocker -> hold moves the job into event_waiting.
        ep.record_result(
            self.store,
            _event_payload("Armed blocker", target="rec-arm"),
            "upload",
            "rec-arm",
        )
        self.assertTrue(service.hold_action_job_for_event(job["job_id"]))
        self.assertEqual(job["phase"], "event_waiting")
        self.assertEqual(job["wait_reason"], "active_event")
        self.assertEqual(job["event_resume_phase"], "accepted")
        self.assertTrue(job["was_event_waiting"])
        self.assertEqual(len(job["event_wait"]["events"]), 1)

    def test_release_event_waiting_jobs_only_releases_when_no_blockers(self):
        service = self.bare_service()
        request = _maintenance_request()
        job = service._base_job(request)
        service._jobs[job["job_id"]] = job
        self.assertEqual(job["phase"], "accepted")

        ep.record_result(
            self.store,
            _event_payload("Wait event", target="rec-wait"),
            "upload",
            "rec-wait",
        )
        service.hold_action_job_for_event(job["job_id"])
        self.assertEqual(job["phase"], "event_waiting")
        # Still blocked.
        self.assertEqual(service.release_event_waiting_jobs(), [])

        ep.record_result(
            self.store,
            _event_payload("Wait event", target="rec-wait"),
            "end",
            "rec-wait",
        )
        released = service.release_event_waiting_jobs()
        self.assertIn(job["job_id"], released)
        self.assertEqual(job["phase"], "accepted")
        self.assertEqual(job.get("wait_reason"), "")
        self.assertTrue(job.get("event_released_at"))

    def test_create_qt_event_wait_job_creates_held_job_and_persists(self):
        service = self.bare_service()
        ep.record_result(
            self.store,
            _event_payload("QT wait event", target="rec-qt-event"),
            "upload",
            "rec-qt-event",
        )
        command = {
            "action_type": "upload",
            "operation_id": "qt-op-wait",
            "response_time": "2026-08-31 11:00",
            "data_dict": {
                "notice_type": "维保通告",
                "record_id": "rec-maintenance",
                "active_item_id": "rec-maintenance",
            },
        }
        job = service.create_qt_event_wait_job(command, ep.blockers(self.store))
        self.assertIn(job["job_id"], service._jobs)
        self.assertEqual(job["phase"], "event_waiting")
        self.assertEqual(job["execution_origin"], "qt_local")
        self.assertEqual(job["request"]["action"], "start")
        self.assertEqual(len(job["event_wait"]["events"]), 1)

        # Job persisted to the notice_action_job table.
        self.assertEqual(self.store.get_document(STATE_NS_ACTION_JOB, job["job_id"]), job)
        # Queue row holds event_waiting and is not leasable while held.
        queue = self.store.list_runtime_queue_items(
            "qt_action", statuses=("event_waiting",)
        )
        self.assertEqual([item["job_id"] for item in queue], [job["job_id"]])
        self.assertEqual(queue[0]["status"], "event_waiting")
        self.assertEqual(self.store.lease_runtime_queue_items("qt_action"), [])

    # ------------------------------------------------------------------
    # put_action_job_with_queue writes atomically + no lease while held
    # ------------------------------------------------------------------
    def test_put_action_job_with_queue_atomic_write_and_no_lease_while_held(self):
        job_id = "notice-action-job-atomic"
        job = {
            "job_id": job_id,
            "phase": "event_waiting",
            "accepted_at": time.time(),
        }
        self.store.put_action_job_with_queue(job)

        persisted = self.store.get_document(STATE_NS_ACTION_JOB, job_id)
        self.assertEqual(persisted, job)

        queue = self.store.list_runtime_queue_items(
            "qt_action", statuses=("event_waiting",)
        )
        self.assertEqual(len(queue), 1)
        self.assertEqual(queue[0]["job_id"], job_id)
        self.assertEqual(queue[0]["status"], "event_waiting")
        self.assertEqual(queue[0]["lease_until"], 0.0)

        # While held the item must never be leased.
        self.assertEqual(self.store.lease_runtime_queue_items("qt_action", limit=5), [])

    # ------------------------------------------------------------------
    # Attachment pinning by non-terminal notice jobs
    # ------------------------------------------------------------------
    def test_nonterminal_job_pins_attachment_past_expiry_and_terminal_releases(self):
        attachment = self.store.put_notice_upload_attachment(
            open_id="open-1",
            file_name="photo.png",
            mime_type="image/png",
            content=b"some bytes",
            ttl_seconds=60,
        )
        upload_id = attachment["upload_id"]
        future = attachment["expires_at"] + 10.0

        # Non-terminal job references the attachment -> pin survives cleanup.
        job_id = "pin-attachment-job"
        nonterminal = {
            "job_id": job_id,
            "phase": "accepted",
            "accepted_at": time.time(),
            "prepared": {"images": [upload_id]},
            "text": f"正在处理 {upload_id}",
        }
        self.store.put_action_job_with_queue(nonterminal)
        self.assertEqual(
            self.store.cleanup_notice_upload_attachments(now=future), 0
        )
        self.assertIsNotNone(self.store.get_notice_upload_attachment(upload_id))

        # Same job becomes terminal -> the pin is released.
        terminal = {
            "job_id": job_id,
            "phase": "success",
            "accepted_at": time.time(),
            "prepared": {"images": [upload_id]},
            "text": f"完成 {upload_id}",
        }
        self.store.put_action_job_with_queue(terminal)
        self.assertEqual(
            self.store.cleanup_notice_upload_attachments(now=future), 1
        )
        self.assertIsNone(self.store.get_notice_upload_attachment(upload_id))

    # ------------------------------------------------------------------
    # permission-safe public_job
    # ------------------------------------------------------------------
    def test_public_job_strips_unauthorized_event_details_without_mutating_stored_job(self):
        class _FakeAuth:
            def __init__(self, *, admin=False, modules=(), scopes=()):
                self.admin = admin
                self.modules = set(modules)
                self.scopes = set(scopes)

            def is_admin(self, session):
                return self.admin

            def scope_allowed(self, session, scope):
                return scope in self.scopes

            def account_profile(self, open_id):
                return {"modules": list(self.modules)}

        base_job = {
            "job_id": "public-safe-job",
            "event_wait": {
                "since": 1234.0,
                "events": [
                    {
                        "building_codes": ["A"],
                        "building": "A楼",
                        "title": "Allowed A",
                        "occurrence_time": "2026-08-31 10:00",
                        "source": "BMS",
                        "level": "I3",
                        "progress": "处理中",
                    },
                    {
                        "building_codes": ["B"],
                        "building": "B楼",
                        "title": "Hidden B",
                        "occurrence_time": "2026-08-31 10:05",
                        "source": "烟感",
                        "level": "I2",
                        "progress": "待处理",
                    },
                ],
                "message": "original message",
            },
        }
        stored = copy.deepcopy(base_job)

        # User may see scope A but not scope B.
        auth = _FakeAuth(admin=False, modules=["maintenance", "event"], scopes={"A"})
        session = {"user": {"open_id": "ou-test"}}
        safe = ep.public_job(base_job, auth, session)

        visible = safe["event_wait"]["events"]
        self.assertEqual([event["title"] for event in visible], ["Allowed A"])
        self.assertIn("另有无权查看详情的事件正在处理中。", safe["event_wait"]["message"])
        # The stored job must not be mutated by the public projection.
        self.assertEqual(base_job, stored)
        self.assertEqual(len(stored["event_wait"]["events"]), 2)

    def test_atomic_queue_failure_rolls_back_job_document(self):
        import sqlite3
        self.store.put_document("test", "initialize", {})
        with closing(sqlite3.connect(self.store.db_path)) as conn:
            conn.execute("CREATE TRIGGER fail_wait BEFORE INSERT ON runtime_task_queue BEGIN SELECT RAISE(ABORT, 'fixture'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.put_action_job_with_queue({"job_id": "rollback", "phase": ep.WAITING})
        self.assertIsNone(self.store.get_document(STATE_NS_ACTION_JOB, "rollback"))

    def test_expired_attachment_remains_readable_only_while_pending(self):
        image = self.store.put_notice_upload_attachment(content=b"photo", ttl_seconds=60)
        job = {"job_id": "pin-read", "phase": ep.WAITING, "request": {"upload_id": image["upload_id"]}}
        self.store.put_action_job_with_queue(job)
        with patch("lan_bitable_template_portal.state_store.time.time", return_value=image["expires_at"] + 100):
            self.assertEqual(self.store.get_notice_upload_attachment(image["upload_id"])["content"], b"photo")
            self.store.put_document(STATE_NS_ACTION_JOB, job["job_id"], {**job, "phase": "success"})
            self.assertIsNone(self.store.get_notice_upload_attachment(image["upload_id"]))

    def qt_command(self, operation="qt-start", action="upload", active="local-notice"):
        return {"operation_id": operation, "action_type": action,
                "response_time": "2026-10-16 10:00", "robot_group_choice": "skip",
                "data_dict": {"notice_type": "维保通告", "active_item_id": active,
                              "record_id": active, "text": "已确认的通告正文", "buildings": ["B楼"]}}

    def test_qt_and_web_wait_survive_restart_without_becoming_failed(self):
        service = self.bare_service()
        event = _event_payload("A event", target="rec-restart-event")
        ep.record_result(self.store, event, "upload")
        qt_job = service.create_qt_event_wait_job(self.qt_command(), ep.blockers(self.store))
        web_job = service._base_job({**_maintenance_request(), "work_type": "maintenance", "scope": "E"})
        service._jobs[web_job["job_id"]] = web_job
        service._persist_action_job_locked(web_job)
        restored = self.bare_service()
        restored._jobs = restored._load_action_jobs_from_state()
        self.assertEqual(set(restored.recoverable_action_job_ids()), {qt_job["job_id"], web_job["job_id"]})
        self.assertTrue(all(job["phase"] == ep.WAITING for job in restored._jobs.values()))
        ep.record_result(self.store, event, "end")
        self.assertEqual(set(restored.release_event_waiting_jobs()), set(restored._jobs))
        again = self.bare_service()
        again._jobs = again._load_action_jobs_from_state()
        self.assertTrue(all(job["phase"] == "accepted" for job in again._jobs.values()))

    def test_qt_delays_original_executor_and_replays_result_without_new_upload(self):
        from lan_bitable_template_portal.server import PortalRuntime
        service = self.bare_service()
        event = _event_payload("A event", target="rec-runtime-event")
        ep.record_result(self.store, event, "upload")
        command = self.qt_command()
        result = {"ok": True, "name": "上传", "record_id": "local-notice", "real_record_id": "rec-sent"}
        with (patch.object(PortalRuntime, "state_store", self.store), patch.object(PortalRuntime, "service", service),
              patch.object(PortalRuntime, "ensure_action_worker"),
              patch.object(PortalRuntime, "_execute_local_notice_upload", return_value=result) as execute):
            queued = PortalRuntime.execute_local_notice_upload(command)
            repeated = PortalRuntime.execute_local_notice_upload(command)
            self.assertEqual(queued["job_id"], repeated["job_id"])
            self.assertTrue(queued["event_waiting"])
            execute.assert_not_called()
            self.assertFalse(PortalRuntime._runtime_queue_job_processable("qt_action", queued["job_id"]))
            ep.record_result(self.store, event, "delete")
            service.release_event_waiting_jobs()
            PortalRuntime._process_maintenance_action_job(queued["job_id"])
            self.assertEqual(service.get_job(queued["job_id"])["phase"], "success")
            self.assertEqual(PortalRuntime.execute_local_notice_upload(command), result)
            execute.assert_called_once()
            sent = execute.call_args.args[0]
            self.assertEqual(sent["data_dict"]["text"], command["data_dict"]["text"])
            self.assertEqual(sent["response_time"], command["response_time"])
            self.assertEqual(self.store.list_outbox_events("qt_action")[-1]["payload"]["kind"], "notice_wait_result")

    def test_same_notice_start_update_end_chain_and_failed_predecessor(self):
        from lan_bitable_template_portal.server import PortalRuntime
        from unittest.mock import Mock
        service = self.bare_service()
        event = _event_payload("A event", target="rec-chain-event")
        ep.record_result(self.store, event, "upload")
        jobs = [service.create_qt_event_wait_job(self.qt_command("chain-" + action, action), ep.blockers(self.store))
                for action in ("upload", "update", "end")]
        self.assertEqual(jobs[1]["depends_on_job_id"], jobs[0]["job_id"])
        self.assertEqual(jobs[2]["depends_on_job_id"], jobs[1]["job_id"])
        ep.record_result(self.store, event, "end")
        service.release_event_waiting_jobs()
        with (patch.object(PortalRuntime, "state_store", self.store), patch.object(PortalRuntime, "service", service),
              patch.object(PortalRuntime, "_execute_local_notice_upload", new=Mock()) as execute):
            self.assertFalse(PortalRuntime._runtime_queue_job_processable("qt_action", jobs[1]["job_id"]))
            service.mark_job(jobs[0]["job_id"], phase="failed", error="fixture previous upload failed")
            PortalRuntime._process_maintenance_action_job(jobs[1]["job_id"])
            execute.assert_not_called()
            failed = service.get_job(jobs[1]["job_id"])
            self.assertEqual(failed["phase"], "failed")
            self.assertIn("前序", failed["error"])
            self.assertFalse(failed["qt_result"]["ok"])

    def test_failed_event_upload_or_end_never_changes_confirmed_state(self):
        from lan_bitable_template_portal.server import PortalRuntime
        event = _event_payload("Real event", target="rec-failed-event")
        with patch.object(PortalRuntime, "state_store", self.store):
            PortalRuntime._record_event_notice_operation_result(event, action_type="upload", success=False)
            self.assertFalse(ep.blockers(self.store))
            PortalRuntime._record_event_notice_operation_result(event, action_type="upload", success=True, record_id="rec-failed-event")
            PortalRuntime._record_event_notice_operation_result(event, action_type="end", success=False)
            self.assertEqual(len(ep.blockers(self.store)), 1)
            PortalRuntime._record_event_notice_operation_result(event, action_type="end", success=True, record_id="rec-failed-event")
            self.assertFalse(ep.blockers(self.store))

    def test_state_read_failure_keeps_queue_and_does_not_execute(self):
        from lan_bitable_template_portal.server import PortalRuntime
        service = self.bare_service()
        job = service._base_job(_maintenance_request())
        service._jobs[job["job_id"]] = job
        self.store.put_action_job_with_queue(job)
        with (patch.object(PortalRuntime, "state_store", self.store), patch.object(PortalRuntime, "service", service),
              patch("lan_bitable_template_portal.event_priority.blockers", side_effect=OSError("fixture unavailable")),
              self.assertLogs(level="ERROR")):
            self.assertFalse(PortalRuntime._runtime_queue_job_processable("qt_action", job["job_id"]))
        self.assertEqual(self.store.list_runtime_queue_items("qt_action")[0]["status"], "queued")
        self.assertEqual(service.get_job(job["job_id"])["phase"], "accepted")

    def test_waiting_retains_predecessor_results_and_frozen_time(self):
        service = self.bare_service()
        ep.record_result(self.store, _event_payload("A event", target="rec-retention"), "upload")
        first = service.create_qt_event_wait_job(self.qt_command(), ep.blockers(self.store))
        second = service.create_qt_event_wait_job(self.qt_command("next", "update"), ep.blockers(self.store))
        service.mark_job(first["job_id"], phase="success", upload_finished_at=time.time()-86400)
        self.assertFalse(service.delete_action_job(first["job_id"]))
        self.assertEqual(service.cleanup_action_jobs(success_retention_seconds=60)["removed_total"], 0)
        with patch("lan_bitable_template_portal.portal_service.ACTION_JOB_MAX_RETAINED", 1):
            service._trim_jobs_locked()
            self.assertEqual(len(service._jobs), 2)
            self.assertEqual(len(service._load_action_jobs_from_state()), 2)
        self.assertEqual(service.get_job(second["job_id"])["request"]["response_time"], self.qt_command()["response_time"])

    def test_qt_missing_response_time_is_frozen_into_original_command(self):
        service = self.bare_service()
        ep.record_result(self.store, _event_payload("A event", target="rec-time-event"), "upload")
        command = self.qt_command()
        command["response_time"] = ""
        job = service.create_qt_event_wait_job(command, ep.blockers(self.store))
        self.assertEqual(job["qt_request"]["response_time"], job["created_at"])
        self.assertEqual(self.store.get_document(STATE_NS_ACTION_JOB, job["job_id"])["qt_request"]["response_time"], job["created_at"])

    def test_qt_terminal_job_and_completion_receipt_commit_together(self):
        import sqlite3
        from lan_bitable_template_portal.server import PortalRuntime
        service = self.bare_service()
        ep.record_result(self.store, _event_payload("A event", target="rec-receipt"), "upload")
        command = self.qt_command()
        job = service.create_qt_event_wait_job(command, ep.blockers(self.store))
        with closing(sqlite3.connect(self.store.db_path)) as conn:
            conn.execute("CREATE TRIGGER fail_receipt BEFORE INSERT ON event_outbox BEGIN SELECT RAISE(ABORT, 'fixture'); END")
        with (patch.object(PortalRuntime, "state_store", self.store), patch.object(PortalRuntime, "service", service),
              self.assertLogs(level="ERROR")):
            PortalRuntime._finish_qt_event_wait_job(job["job_id"], command, {"ok": True})
        stored = self.store.get_document(STATE_NS_ACTION_JOB, job["job_id"])
        self.assertEqual(stored["phase"], "upload_waiting")
        self.assertTrue(stored["qt_completion_pending"])
        self.assertEqual(stored["qt_result"], {"ok": True})
        self.assertEqual(service.get_job(job["job_id"])["phase"], "upload_waiting")
        self.assertEqual(self.store.list_outbox_events("qt_action"), [])

    def test_concurrent_duplicate_qt_submission_and_new_event_before_execution(self):
        from concurrent.futures import ThreadPoolExecutor
        from lan_bitable_template_portal.server import PortalRuntime
        service = self.bare_service()
        event = _event_payload("A event", target="rec-original-event")
        ep.record_result(self.store, event, "upload")
        events = ep.blockers(self.store)
        with ThreadPoolExecutor(max_workers=4) as pool:
            jobs = list(pool.map(lambda _: service.create_qt_event_wait_job(self.qt_command(), events), range(8)))
        self.assertEqual(len({job["job_id"] for job in jobs}), 1)
        ep.record_result(self.store, event, "end")
        service.release_event_waiting_jobs()
        ep.record_result(self.store, _event_payload("New event", target="rec-new-event"), "upload")
        with patch.object(PortalRuntime, "state_store", self.store), patch.object(PortalRuntime, "service", service):
            self.assertFalse(PortalRuntime._runtime_queue_job_processable("qt_action", jobs[0]["job_id"]))
        self.assertEqual(service.get_job(jobs[0]["job_id"])["phase"], ep.WAITING)

    def test_deleting_notice_cancels_held_and_released_jobs_not_other_notices(self):
        service = self.bare_service()
        event = _event_payload("A event", target="rec-cancel-event")
        ep.record_result(self.store, event, "upload")
        held = service.create_qt_event_wait_job(self.qt_command(), ep.blockers(self.store))
        other = service.create_qt_event_wait_job(self.qt_command("other", active="another-notice"), ep.blockers(self.store))
        ep.record_result(self.store, event, "end")
        service.release_event_waiting_jobs()
        service.cancel_event_waiting_notice({"notice_type": "维保通告", "work_type": "maintenance", "active_item_id": "local-notice"})
        self.assertEqual(service.get_job(held["job_id"])["phase"], "failed")
        self.assertEqual(service.get_job(other["job_id"])["phase"], "accepted")

    def test_qt_target_selection_continues_same_operation_and_has_a_new_receipt(self):
        from lan_bitable_template_portal.server import PortalRuntime
        service = self.bare_service()
        event = _event_payload("A event", target="rec-selection-event")
        ep.record_result(self.store, event, "upload")
        command = self.qt_command()
        with (patch.object(PortalRuntime, "state_store", self.store), patch.object(PortalRuntime, "service", service),
              patch.object(PortalRuntime, "ensure_action_worker"),
              patch.object(PortalRuntime, "_execute_local_notice_upload", side_effect=[
                  {"ok": False, "needs_target_selection": True, "message": "请选择目标"},
                  {"ok": True, "real_record_id": "rec-selected"}]) as execute):
            queued = PortalRuntime.execute_local_notice_upload(command)
            ep.record_result(self.store, event, "end")
            service.release_event_waiting_jobs()
            PortalRuntime._process_maintenance_action_job(queued["job_id"])
            selected = copy.deepcopy(command)
            selected["data_dict"].update(target_record_id="rec-selected", record_id="rec-selected",
                                         _event_wait_resume_job_id=queued["job_id"])
            resumed = PortalRuntime.execute_local_notice_upload(selected)
            self.assertEqual(resumed["job_id"], queued["job_id"])
            self.assertEqual(resumed["event_wait_attempt"], 1)
            self.assertFalse(resumed["event_waiting"])
            self.assertEqual(PortalRuntime.execute_local_notice_upload(selected)["event_wait_attempt"], 1)
            PortalRuntime._process_maintenance_action_job(queued["job_id"])
            self.assertEqual(execute.call_count, 2)
            self.assertEqual(execute.call_args.args[0]["data_dict"]["target_record_id"], "rec-selected")
            self.assertEqual(execute.call_args.args[0]["operation_id"], command["operation_id"])
            receipts = self.store.list_outbox_events("qt_action")
            self.assertEqual([row["payload"]["payload"]["attempt"] for row in receipts], [0, 1])

    def test_same_plan_different_targets_do_not_chain_or_cancel_each_other(self):
        service = self.bare_service()
        ep.record_result(self.store, _event_payload("A event", target="rec-generation-event"), "upload")
        jobs = []
        for target in ("rec-generation-one", "rec-generation-two"):
            command = self.qt_command(target, "update", active=target)
            command["data_dict"].update(source_record_id="rec-same-plan", target_record_id=target)
            jobs.append(service.create_qt_event_wait_job(command, ep.blockers(self.store)))
        self.assertFalse(jobs[1]["depends_on_job_id"])
        service.cancel_event_waiting_notice({**jobs[0]["request"], "work_type": "maintenance"})
        self.assertEqual(service.get_job(jobs[0]["job_id"])["phase"], "failed")
        self.assertEqual(service.get_job(jobs[1]["job_id"])["phase"], ep.WAITING)

    def test_assistant_and_feishu_describe_waiting_not_completion(self):
        from lan_bitable_template_portal.feishu_assistant import FeishuAssistant, _plan_state_hint, _plain_status_line
        from lan_bitable_template_portal.feishu_notice_cards import _status_plain
        job = {"phase": ep.WAITING, "event_wait": {"message": "已排队，等待事件结束后自动发送。"}}
        for receipt in ({"job_result": {"data": job}}, {"job_result": {"_raw": job}}, {"data": {**job, "initial_phase": ep.WAITING}}):
            plan = {"status": "submitted", "title": "隔离测试", "version": 1, "results": [receipt]}
            self.assertEqual(ep.plan_wait_message(plan), job["event_wait"]["message"])
            self.assertIn("已排队", FeishuAssistant._status_text(None, plan))
            self.assertIn("已排队", FeishuAssistant.plan_text(plan))
            self.assertEqual(_plan_state_hint(plan), job["event_wait"]["message"])
            self.assertEqual(_plain_status_line(plan), job["event_wait"]["message"])
            self.assertEqual(_status_plain(plan), "已排队，等待事件结束")
        plan["status"] = "completed"
        self.assertEqual(ep.plan_wait_message(plan), "")

    def test_every_non_event_type_and_action_waits_and_repeated_enqueue_preserves_hold(self):
        from lan_bitable_template_portal.server import PortalRuntime
        service = self.bare_service()
        ep.record_result(self.store, _event_payload("A event", target="rec-all-types"), "upload")
        for kind in ("maintenance", "change", "repair", "polling", "adjust", "power"):
            for action in ("start", "update", "end"):
                with self.subTest(kind=kind, action=action):
                    request = {**_maintenance_request(), "work_type": kind, "action": action,
                               "actual_action_time": "2026-10-16 09:00", "text": "原始确认正文"}
                    job = service._base_job(request)
                    service._jobs[job["job_id"]] = job
                    service._persist_action_job_locked(job)
                    with (patch.object(PortalRuntime, "state_store", self.store),
                          patch.object(PortalRuntime, "ensure_action_worker")):
                        PortalRuntime.enqueue_action_job(job["job_id"])
                    queued = next(row for row in self.store.list_runtime_queue_items("qt_action", statuses=(ep.WAITING,)) if row["job_id"] == job["job_id"])
                    self.assertEqual(queued["status"], ep.WAITING)
                    self.assertEqual(service.get_job(job["job_id"])["request"], request)


class QtEventWaitingUiTests(unittest.TestCase):
    def test_late_queued_response_does_not_override_completed_outbox_result(self):
        from types import SimpleNamespace
        from unittest.mock import Mock
        from upload_event_module.ui.main_window_workflow import MainWindowWorkflowMixin
        class Window(MainWindowWorkflowMixin):
            def __init__(self):
                self.callbacks = []
                self.lan_template_portal_controller = SimpleNamespace(execute_qt_notice_upload=lambda command: {
                    "queued": True, "job_id": "fast", "event_waiting": True})
                self._apply_backend_notice_upload_result = Mock()
                self._find_active_item_by_record_id = Mock(return_value=(None, None))
            def _enqueue_ui_mutation(self, name, callback):
                self.callbacks.append(callback)
        window = Window()
        command = {"record_id": "local-notice", "notice_type": "维保通告", "text": "原始正文"}
        window._delegate_qt_notice_upload_to_backend(data_snapshot=command, screenshot_bytes=None,
            extra_images=[], action_type="upload", response_time="", recover_selected=False, robot_group_choice="skip")
        window._apply_event_wait_result({"job_id": "fast", "command": {"data_dict": command}, "result": {"ok": True}})
        window._find_active_item_by_record_id.reset_mock()
        window.callbacks[0]()
        window._find_active_item_by_record_id.assert_not_called()
        window._apply_backend_notice_upload_result.assert_called_once()

    def test_old_result_keeps_newly_copied_text_and_other_waiting_generation(self):
        from types import SimpleNamespace
        from unittest.mock import Mock
        from upload_event_module.ui.main_window_workflow import MainWindowWorkflowMixin
        current = {"text": "新复制正文", "_event_wait_job_id": "job-old"}
        item = SimpleNamespace(data=lambda role: current.copy(), setData=lambda role, data: current.update(data))
        window = object.__new__(MainWindowWorkflowMixin)
        window._find_active_item_by_record_id = lambda record: (None, item)
        window._is_valid_list_item = lambda item: True
        window._rebuild_active_item_widget = Mock()
        window._apply_backend_notice_upload_result = Mock()
        window.show_message = Mock()
        window.request_active_cache_save = Mock()
        payload = {"job_id": "job-old", "command": {"data_dict": {"record_id": "local", "text": "原正文"}}, "result": {"ok": True}}
        window._apply_event_wait_result(payload)
        window._apply_backend_notice_upload_result.assert_not_called()
        self.assertEqual(current["text"], "新复制正文")
        current["_event_wait_job_id"] = "job-new"
        window._rebuild_active_item_widget.reset_mock()
        self.assertTrue(window._apply_event_wait_result(payload)["ignored"])
        window._rebuild_active_item_widget.assert_not_called()


if __name__ == "__main__":
    unittest.main()
