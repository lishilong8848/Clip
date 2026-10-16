"""Recovery semantics for the event priority queue when persistence fails half-way.

Six tests reuse fixtures without inheriting the sibling suite; each checks
memory, the SQLite job document and the queue stay consistent.
"""
import copy
import sqlite3
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from bin import test_event_notice_priority as fixtures
from lan_bitable_template_portal import event_priority as ep
from lan_bitable_template_portal.portal_service import STATE_NS_ACTION_JOB


class EventNoticePriorityRecoveryTests(unittest.TestCase):
    setUp = fixtures.EventNoticePriorityTests.setUp
    tearDown = fixtures.EventNoticePriorityTests.tearDown
    bare_service = fixtures.EventNoticePriorityTests.bare_service
    qt_command = fixtures.EventNoticePriorityTests.qt_command

    def _raises(self, exc, fn, *a):
        with self.assertRaises(exc):
            fn(*a)

    def _st(self, svc, job_id, **kw):
        memory = svc.get_job(job_id)
        stored = self.store.get_document(STATE_NS_ACTION_JOB, job_id)
        assert stored == memory, job_id
        for k, v in kw.items(): assert memory.get(k) == v, k
        return memory, stored

    def _queue(self, ss, ids=None):
        items = self.store.list_runtime_queue_items("qt_action", statuses=ss)
        if ids is not None: assert sorted(i["job_id"] for i in items) == sorted(ids), ss
        return items

    def test_create_persist_failure_no_residue_then_retry_single(self):
        svc = self.bare_service()
        ep.record_result(self.store, fixtures._event_payload("Recovery event", target="rec-recovery-create"), "upload")
        events = ep.blockers(self.store)
        assert events
        command = self.qt_command("op-recovery-create", "upload")
        attempted = {}
        def fail_once(job):
            attempted["job_id"] = job["job_id"]; raise sqlite3.OperationalError("fixture create")
        with patch.object(self.store, "put_action_job_with_queue", side_effect=fail_once):
            self._raises(sqlite3.OperationalError, svc.create_qt_event_wait_job, command, events)
        job_id = attempted["job_id"]
        assert job_id not in svc._jobs and svc._jobs == {} and \
            self.store.get_document(STATE_NS_ACTION_JOB, job_id) is None and self._queue((ep.WAITING,)) == []
        job = svc.create_qt_event_wait_job(command, events)
        assert self._st(svc, job["job_id"], phase=ep.WAITING)[0] == job
        self._queue((ep.WAITING,), [job["job_id"]])
        again = svc.create_qt_event_wait_job(command, events)
        assert again["job_id"] == job["job_id"]
        assert len([c for c in svc._jobs.values() if c.get("operation_id") == command["operation_id"]]) == 1
        self._queue((ep.WAITING,), [job["job_id"]])

    def test_release_persist_failure_keeps_waiting_then_retry(self):
        svc = self.bare_service()
        job = svc._base_job(fixtures._maintenance_request(operation_id="op-release-recovery", record_id="rec-maintenance"))
        svc._jobs[job["job_id"]] = job
        assert job["phase"] == "accepted"
        event = fixtures._event_payload("Release recovery", target="rec-release-recovery")
        ep.record_result(self.store, event, "upload")
        assert ep.blockers(self.store)
        svc.hold_action_job_for_event(job["job_id"], ep.blockers(self.store))
        assert job["phase"] == ep.WAITING
        ep.record_result(self.store, event, "end")
        def fail_once(_job): raise sqlite3.OperationalError("fixture release")
        with patch.object(self.store, "put_action_job_with_queue", side_effect=fail_once):
            self._raises(sqlite3.OperationalError, svc.release_event_waiting_jobs)
        memory, stored = self._st(svc, job["job_id"], phase=ep.WAITING)
        assert memory["event_resume_phase"] == stored["event_resume_phase"] == "accepted"
        self._queue((ep.WAITING,), [job["job_id"]])
        released = svc.release_event_waiting_jobs()
        assert job["job_id"] in released
        memory, stored = self._st(svc, job["job_id"], phase="accepted")
        assert memory.get("wait_reason") == "" and memory.get("event_released_at")
        assert stored.get("wait_reason") == "" and stored.get("event_released_at")
        self._queue(("queued",), [job["job_id"]])

    def test_release_multi_second_write_failure_first_released_rest_waiting(self):
        svc = self.bare_service()
        jobs = []
        for op in ("op-multi-release-1", "op-multi-release-2", "op-multi-release-3"):
            job = svc._base_job(fixtures._maintenance_request(operation_id=op))
            svc._jobs[job["job_id"]] = job
            jobs.append(job)
        event = fixtures._event_payload("Multi release", target="rec-multi-release")
        ep.record_result(self.store, event, "upload")
        assert ep.blockers(self.store)
        for job in jobs:
            svc.hold_action_job_for_event(job["job_id"], ep.blockers(self.store))
        assert all(job["phase"] == ep.WAITING for job in jobs)
        ep.record_result(self.store, event, "end")
        original_put = self.store.put_action_job_with_queue
        calls = {"n": 0}
        def fail_second(job):
            calls["n"] += 1
            if calls["n"] == 2: raise sqlite3.OperationalError("fixture second write")
            original_put(job)
        with patch.object(self.store, "put_action_job_with_queue", side_effect=fail_second):
            self._raises(sqlite3.OperationalError, svc.release_event_waiting_jobs)
        first, second, third = jobs[0], jobs[1], jobs[2]
        for jid, ph in ((first["job_id"], "accepted"), (second["job_id"], ep.WAITING), (third["job_id"], ep.WAITING)):
            self._st(svc, jid, phase=ph)
        self._queue(("queued",), [first["job_id"]])
        self._queue((ep.WAITING,), [second["job_id"], third["job_id"]])
        released = svc.release_event_waiting_jobs()
        assert set(released) == {second["job_id"], third["job_id"]}
        for job in jobs:
            self._st(svc, job["job_id"], phase="accepted")
        self._queue(("queued",), [job["job_id"] for job in jobs])

    def test_hold_failure_preserves_snapshot_then_retry_freezes_time(self):
        svc = self.bare_service()
        request = fixtures._maintenance_request(operation_id="op-hold-recovery", record_id="rec-maintenance")
        request.pop("response_time", None)
        job = svc._base_job(request)
        svc._jobs[job["job_id"]] = job
        svc._persist_action_job_locked(job)
        snapshot = copy.deepcopy(job)
        assert job["phase"] == "accepted" and "response_time" not in job["request"]
        ep.record_result(self.store, fixtures._event_payload("Hold recovery", target="rec-hold-recovery"), "upload")
        events = ep.blockers(self.store)
        assert events
        def fail_once(_job): raise sqlite3.OperationalError("fixture hold")
        with patch.object(self.store, "put_action_job_with_queue", side_effect=fail_once):
            self._raises(sqlite3.OperationalError, svc.hold_action_job_for_event, job["job_id"], events)
        memory, stored = self._st(svc, job["job_id"])
        assert memory == snapshot and stored == snapshot and "event_wait" not in memory and memory["phase"] == "accepted"
        assert svc.hold_action_job_for_event(job["job_id"], events)
        memory, stored = self._st(svc, job["job_id"])
        assert memory["phase"] == ep.WAITING and memory["request"]["response_time"] == memory["created_at"]
        assert memory["event_wait"]["since"] > 0 and memory["event_resume_phase"] == "accepted"
        assert stored["request"]["response_time"] == stored["created_at"]
        self._queue((ep.WAITING,), [job["job_id"]])
        since = memory["event_wait"]["since"]
        assert svc.hold_action_job_for_event(job["job_id"], events)
        memory, stored = self._st(svc, job["job_id"])
        assert memory["request"]["response_time"] == memory["created_at"]
        assert memory["event_wait"]["since"] == stored["event_wait"]["since"] == since

    def test_resume_failure_keeps_snapshot_then_retry_applies_target_once(self):
        svc = self.bare_service()
        command = self.qt_command("op-resume-recovery", "upload")
        request = {**copy.deepcopy(command["data_dict"]), "action": "start", "operation_id": command["operation_id"],
                   "response_time": command.get("response_time") or ""}
        job = svc._base_job(request)
        svc._jobs[job["job_id"]] = job
        job.update(execution_origin="qt_local", qt_request=copy.deepcopy(command), phase="failed", error="previous failure",
                   qt_result={"ok": False, "needs_target_selection": True, "message": "请选择目标"}, event_wait_attempt=2)
        svc._persist_action_job_locked(job)
        snapshot = copy.deepcopy(job)
        assert job["phase"] == "failed" and job["event_wait_attempt"] == 2
        ep.record_result(self.store, fixtures._event_payload("Resume recovery", target="rec-resume-recovery"), "upload")
        events = ep.blockers(self.store)
        assert events
        retry_command = copy.deepcopy(command)
        retry_command["data_dict"].update(target_record_id="rec-new-target", record_version="v2")
        def fail_once(_job): raise sqlite3.OperationalError("fixture resume")
        with patch.object(self.store, "put_action_job_with_queue", side_effect=fail_once):
            self._raises(sqlite3.OperationalError, svc.resume_qt_event_wait_job, job["job_id"], retry_command, events)
        memory, stored = self._st(svc, job["job_id"])
        assert memory == snapshot and stored == snapshot and memory["phase"] == "failed" and \
            memory["event_wait_attempt"] == 2 and memory["qt_request"] == command and stored["qt_request"] == command
        resumed = svc.resume_qt_event_wait_job(job["job_id"], retry_command, events)
        assert resumed["phase"] == ep.WAITING and resumed["event_wait_attempt"] == 3 and resumed["qt_result"] == {}
        assert resumed["qt_request"]["data_dict"]["target_record_id"] == "rec-new-target"
        assert resumed["qt_request"]["data_dict"]["record_version"] == "v2"
        assert resumed["qt_request"]["data_dict"]["text"] == command["data_dict"]["text"]
        assert self._st(svc, job["job_id"])[0] == resumed
        self._queue((ep.WAITING,), [job["job_id"]])
        again = svc.resume_qt_event_wait_job(job["job_id"], retry_command, events)
        assert again["event_wait_attempt"] == 3 and svc.get_job(job["job_id"])["event_wait_attempt"] == 3 and \
            self.store.get_document(STATE_NS_ACTION_JOB, job["job_id"])["event_wait_attempt"] == 3

    def test_create_without_blockers_persists_and_recoverable_includes_job(self):
        svc = self.bare_service()
        assert ep.blockers(self.store) == []
        command = self.qt_command("op-no-event-recovery", "upload")
        job = svc.create_qt_event_wait_job(command, [])
        assert job["phase"] == "accepted" and job["job_id"] in svc._jobs
        assert self._st(svc, job["job_id"], phase="accepted")[0] == job
        self._queue(("queued",), [job["job_id"]])
        restored = self.bare_service()
        restored._jobs = restored._load_action_jobs_from_state()
        restored_job = restored.get_job(job["job_id"])
        assert restored_job is not None and restored_job["phase"] == "accepted"
        assert restored_job["operation_id"] == command["operation_id"]
        assert job["job_id"] in restored.recoverable_action_job_ids()


if __name__ == "__main__":
    unittest.main()
