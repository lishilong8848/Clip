"""Focused non-blocking coverage for PortalAgent field-options/preview/execute I/O.

These tests verify that the synchronous store/SQLite reads and writes, file
access and ``assistant._lock`` blocks that used to run directly on the event
loop in ``field_options`` / ``preview_notice`` / ``preview_repair`` /
``preview_cabinet_text`` / ``_stage_water_photos`` / ``_execute`` now run in
worker threads via ``asyncio.to_thread`` / ``_durable_save``; that the event
loop stays responsive while that I/O is in flight; and that cancelling a
durable save finishes the atomic write (shielded) then lets the final/cancelled
snapshot win last without any orphaned late worker overwriting it.
"""
import asyncio
import contextvars
import copy
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from fastapi import FastAPI, Request
from lan_bitable_template_portal.lighthouse_ai import LighthouseAssistant
from lan_bitable_template_portal.lighthouse_agent import PortalAgent
from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog
from lan_bitable_template_portal.lighthouse_files import LighthouseFiles

ACTOR = {"id": "fixture-a", "scopes": ["A"], "is_admin": False}
PLAN_NAMESPACE = "lighthouse_agent_plans"

# Isolated child programs (run via sys.executable -B -c in a bounded subprocess)
# so a busy-spin regression fails the parent with a timeout instead of hanging
# the whole packaging process.  No cloud, no real service: PortalAgent is built
# with __new__ and only the mocked target is exercised.
_CHILD_TEARDOWN_SAVE = r'''
import asyncio
import sys
import threading
from lan_bitable_template_portal.lighthouse_agent import PortalAgent

agent = PortalAgent.__new__(PortalAgent)
started, release = threading.Event(), threading.Event()

def blocked_save(actor, plan):
    started.set()
    release.wait(10)
    return plan

agent._save_plan = blocked_save

async def main():
    seeded = {"id": "plan-teardown-cancel-save", "status": "running", "version": 1}
    task = asyncio.create_task(agent._durable_save({"id": "fixture-a"}, seeded))
    await asyncio.to_thread(started.wait, 5)
    assert started.is_set()
    current = asyncio.current_task()
    for t in list(asyncio.all_tasks()):
        if t is not current:
            t.cancel()
    # Not an asyncio Task: teardown cannot cancel the write; the caller stays
    # non-terminal waiting on the shielded write, not busy-spinning.
    assert not task.done()
    release.set()
    try:
        await task
    except asyncio.CancelledError:
        pass
    else:
        print("NO_CANCELLED_ERROR")
        sys.exit(1)
    print("CHILD_OK")
    sys.exit(0)

asyncio.run(main())
'''

_CHILD_WORKER_CANCELLED_SAVE = r'''
import asyncio
import sys
from lan_bitable_template_portal.lighthouse_agent import PortalAgent

agent = PortalAgent.__new__(PortalAgent)

def cancelled_save(actor, plan):
    raise asyncio.CancelledError("worker-cancel")

agent._save_plan = cancelled_save

async def main():
    try:
        await agent._durable_save({"id": "fixture-a"}, {"id": "x", "status": "running", "version": 1})
    except asyncio.CancelledError:
        pass
    else:
        print("NO_CANCELLED_ERROR")
        sys.exit(1)
    print("CHILD_OK")
    sys.exit(0)

asyncio.run(main())
'''


class RecordingStore:
    """In-memory store recording every get/put calling thread.  It can be armed
    to briefly block the next plan-namespace write (or get) so tests can observe
    the event loop staying responsive while the I/O runs off-loop."""

    def __init__(self, path, block_seconds=0.05):
        self.db_path, self.block_seconds = path, block_seconds
        self.docs = {}
        self.get_threads = []
        self.put_threads = []
        self.loop_thread = None
        self.timer = 0
        self._block_put = True
        self._block_get = False
        self.blocked_put_thread = None
        self.blocked_get_thread = None
        self.blocked_delta = 0

    def get_document(self, namespace, key):
        ident = threading.get_ident()
        self.get_threads.append(ident)
        if self._block_get:
            self._block_get = False
            self.blocked_get_thread = ident
            before = self.timer
            time.sleep(self.block_seconds)
            self.blocked_delta = self.timer - before
        return copy.deepcopy(self.docs.get((namespace, key)))

    def put_document(self, namespace, key, value):
        ident = threading.get_ident()
        self.put_threads.append(ident)
        if namespace == PLAN_NAMESPACE and self._block_put:
            self._block_put = False
            self.blocked_put_thread = ident
            before = self.timer
            time.sleep(self.block_seconds)
            self.blocked_delta = self.timer - before
        self.docs[namespace, key] = copy.deepcopy(value)


async def _tick_loop(store):
    store.loop_thread = threading.get_ident()
    while True:
        store.timer += 1
        await asyncio.sleep(0.002)


class PlanIOTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = RecordingStore(Path(self.tmp.name) / "state.sqlite3")
        self._arm_timer = False
        self.model = Mock()
        self.model.settings.return_value = {
            "configured": True, "enabled": True, "active_model_id": "default",
            "models": [{"id": "default", "name": "测试模型", "model": "fixture-model", "configured": True}],
        }
        self.model.profile.return_value = {"id": "default", "name": "测试模型", "model": "fixture-model"}
        self.search = Mock(side_effect=AssertionError("agent must use backend APIs, not local scraping"))
        self.assistant = LighthouseAssistant(self.store, self.search, model=self.model)
        self.files = LighthouseFiles(self.store)
        app = FastAPI()
        self.catalog = PortalAPICatalog(app)
        self.agent = PortalAgent(self.assistant, self.catalog, self.files)
        self.request = Request({"type": "http", "scheme": "http", "server": ("testserver", 80),
                                "client": ("127.0.0.1", 4567), "path": "/api/assistant/agent",
                                "root_path": "", "query_string": b"",
                                "headers": [(b"cookie", b"fixture=a"), (b"origin", b"http://testserver")]})
        self.timer_task = None

    async def asyncSetUp(self):
        if self._arm_timer:
            self.timer_task = asyncio.create_task(_tick_loop(self.store))
            await asyncio.sleep(0.02)

    async def asyncTearDown(self):
        if self.timer_task is not None:
            self.timer_task.cancel()
            try:
                await self.timer_task
            except asyncio.CancelledError:
                pass
        pending = list(self.agent.tasks)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        self.agent.tasks.clear()

    def _run_child(self, program, timeout=6.0):
        # Run an isolated bounded child; a busy-spin regression must fail the
        # parent with a timeout (killing only this owned subprocess) instead of
        # hanging the packaging process.
        code = (
            "import sys\n"
            f"sys.path.insert(0, {str(Path(__file__).resolve().parent)!r})\n"
            + program
        )
        try:
            result = subprocess.run(
                [sys.executable, "-B", "-c", code],
                capture_output=True, text=True, timeout=timeout,
            )
        except subprocess.TimeoutExpired as exc:
            self.fail(
                "child teardown regression timed out (busy-spin?); "
                f"stdout={exc.stdout!r} stderr={exc.stderr!r}")
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("CHILD_OK", result.stdout)

    def _seed_plan(self, plan):
        plan = copy.deepcopy(plan)
        plan["conversation_id"] = self.assistant.conversation(ACTOR)["conversation_id"]
        self.store.put_document(PLAN_NAMESPACE, plan["id"], plan)
        return plan

    async def test_field_options_commit_runs_off_loop_and_loop_stays_responsive(self):
        plan = self._seed_plan({
            "id": "plan-field-options-io",
            "owner": ACTOR["id"], "scopes": ["A"], "turn_id": "turn_field_options_io",
            "conversation_id": "conv", "title": "选项", "explanation": "", "status": "needs_input",
            "risk": "normal", "version": 1, "error": "", "fields": [], "_references": {}, "_queries": {},
            "operations": [{"api_id": "POST /fixture-op", "body": {}}],
            "results": [],
            "fields": [{"name": "target", "path": "target", "operation_index": 0,
                        "options_source": "plan_maintenance", "type": "select", "required": True}],
        })
        self._arm_timer = True
        await self.asyncSetUp()
        self._reset_block(put=True)
        with patch.object(self.agent, "_invoke", new=AsyncMock(return_value={
                "ok": True, "_raw": {"items": [{"record_id": "rec-1", "name": "检修A"}]},
                "data": {"items": [{"record_id": "rec-1", "name": "检修A"}]}})):
            public = await self.agent.field_options(ACTOR, plan["id"], "target", self.request)
        # The atomic commit wrote the store from a worker thread, not the loop.
        self.assertIsNotNone(self.store.blocked_put_thread)
        self.assertNotEqual(self.store.blocked_put_thread, self.store.loop_thread)
        # While that blocking I/O was in flight the loop timer kept ticking.
        self.assertGreater(self.store.blocked_delta, 0)
        # The public reply reflects the persisted options.
        field = next(f for f in public["fields"] if f["path"] == "target")
        self.assertEqual(field["options"][0]["value"], "__empty__")
        self.assertIn("rec-1", [option["value"] for option in field["options"]])
        stored = self.agent.get_plan(ACTOR, plan["id"])
        self.assertEqual(stored["version"], 2)

    async def test_field_options_changed_version_raises_409_in_worker(self):
        plan = self._seed_plan({
            "id": "plan-field-options-version",
            "owner": ACTOR["id"], "scopes": ["A"], "turn_id": "turn_ver", "conversation_id": "conv",
            "title": "版本", "explanation": "", "status": "needs_input", "risk": "normal",
            "version": 1, "error": "", "fields": [], "_references": {}, "_queries": {},
            "operations": [{"api_id": "POST /fixture-op", "body": {}}], "results": [],
            "fields": [{"name": "target", "path": "target", "operation_index": 0,
                        "options_source": "plan_maintenance", "type": "select", "required": True}],
        })
        self._arm_timer = True
        await self.asyncSetUp()
        # Bump the persisted version while the (awaited) native lookup is in flight,
        # simulating another writer changing the plan between field_options reading it
        # and the worker re-reading it for its version check.
        def bump_and_return(actor, operation, request, **kwargs):
            changed = self.agent.get_plan(ACTOR, plan["id"])
            changed["version"] = 99
            self.store.put_document(PLAN_NAMESPACE, plan["id"], changed)
            return {"ok": True, "_raw": {"items": [{"record_id": "rec-1", "name": "x"}]}}
        with patch.object(self.agent, "_invoke", new=AsyncMock(side_effect=bump_and_return)):
            with self.assertRaises(Exception) as ctx:
                await self.agent.field_options(ACTOR, plan["id"], "target", self.request)
        self.assertEqual(getattr(ctx.exception, "status", None), 409)

    async def test_preview_cabinet_text_reads_plan_off_loop_and_produces_rows(self):
        plan = self._seed_plan({
            "id": "plan-cabinet-text-io", "owner": ACTOR["id"], "scopes": ["A"],
            "turn_id": "turn_cabinet_io", "conversation_id": "conv", "title": "机柜文本",
            "explanation": "", "status": "needs_input", "risk": "normal", "version": 3,
            "error": "", "fields": [], "_references": {}, "_queries": {},
            "operations": [{"api_id": "POST /api/cabinet-power/batches/text-preview", "body": {}}],
            "results": [],
            "fields": [{"name": "text_fill", "path": "text_fill", "operation_index": 0,
                        "native_cabinet_text_create": True, "type": "text"}],
        })
        self._arm_timer = True
        await self.asyncSetUp()
        self._reset_block(get=True)
        rows = [{"scope": "A", "text_id": "t-1", "text_row": 1, "row_id": "r1",
                 "issues": [], "actual": "B17", "expected": "B17"}]
        with patch.object(self.agent, "_invoke", new=AsyncMock(return_value={
                "ok": True, "_raw": {"rows": rows}, "data": {"rows": rows}})):
            reply = await self.agent.preview_cabinet_text(
                ACTOR, plan["id"], {"version": 3, "field": "text_fill", "sources": []}, self.request)
        self.assertEqual(reply["rows"][0]["text_id"], "t-1")
        # At least one plan read (initial get_plan) ran off-loop.
        self.assertIsNotNone(self.store.blocked_get_thread)
        self.assertNotEqual(self.store.blocked_get_thread, self.store.loop_thread)
        self.assertGreater(self.store.blocked_delta, 0)

    async def test_preview_notice_reads_plan_off_loop(self):
        plan = self._seed_plan({
            "id": "plan-notice-io", "owner": ACTOR["id"], "scopes": ["A"], "turn_id": "turn_notice_io",
            "conversation_id": "conv", "title": "检修", "explanation": "", "status": "needs_input",
            "risk": "normal", "version": 2, "error": "", "fields": [], "_references": {}, "_queries": {},
            "operations": [{"api_id": "POST /api/workbench-actions",
                            "body": {"action": "start", "work_type": "repair", "scope": "A"}}],
            "results": [],
            "fields": [
                {"name": "source_record_id", "path": "source_record_id", "operation_index": 0,
                 "native_notice_prefill": True, "options": [{"value": "rec-source", "label": "s"}]},
                {"name": "patch", "path": "patch", "operation_index": 0, "native_notice": True,
                 "children": [{"path": "title", "type": "text"}]},
            ],
        })
        self._arm_timer = True
        await self.asyncSetUp()
        self._reset_block(get=True)
        prefill = {"work_type": "repair", "title": "检修A"}
        with patch.object(self.agent, "_invoke", new=AsyncMock(return_value={
                "ok": True, "_raw": {"draft": prefill, "source_record": {"record_id": "rec-source"},
                                     "source_record_id": "rec-source", "repair_management_record_id": "rec-source"},
                "data": {"draft": prefill, "source_record": {"record_id": "rec-source"},
                         "source_record_id": "rec-source", "repair_management_record_id": "rec-source"}})), \
             patch("openclaw_service.assistant.lighthouse_sources.codes", return_value={"A"}), \
             patch("openclaw_service.assistant.lighthouse_sources.record_codes", return_value={"A"}):
            reply = await self.agent.preview_notice(
                ACTOR, plan["id"],
                {"version": 2, "operation_index": 0, "source_record_id": "rec-source", "scope": "A"},
                self.request)
        self.assertEqual(reply["fields"]["title"], "检修A")
        self.assertIsNotNone(self.store.blocked_get_thread)
        self.assertNotEqual(self.store.blocked_get_thread, self.store.loop_thread)

    async def test_preview_repair_reads_plan_off_loop(self):
        plan = self._seed_plan({
            "id": "plan-repair-io", "owner": ACTOR["id"], "scopes": ["A"], "turn_id": "turn_repair_io",
            "conversation_id": "conv", "title": "维修", "explanation": "", "status": "needs_input",
            "risk": "normal", "version": 4, "error": "", "fields": [], "_references": {}, "_queries": {},
            "operations": [{"api_id": "POST /api/repair-management/records", "body": {"scope": "A"}}],
            "results": [],
            "fields": [
                {"name": "f", "path": "fields", "operation_index": 0, "native_repair": True,
                 "children": [{"path": "title", "type": "text"}], "unlinked_children": [{"path": "title"}]},
            ],
        })
        self._arm_timer = True
        await self.asyncSetUp()
        self._reset_block(get=True)
        with patch.object(self.agent.catalog, "get", return_value={"api_id": "POST /api/repair-management/prefill"}), \
             patch("openclaw_service.assistant.lighthouse_model.scoped_operation",
                   return_value={"api_id": "POST /api/repair-management/prefill", "body": {}}):
            reply = await self.agent.preview_repair(
                ACTOR, plan["id"],
                {"version": 4, "operation_index": 0, "source_event_id": "", "source_repair_ids": [],
                 "scope": "A", "source_month": ""},
                self.request)
        self.assertEqual(reply["skip"], False)
        self.assertIsNotNone(self.store.blocked_get_thread)
        self.assertNotEqual(self.store.blocked_get_thread, self.store.loop_thread)

    async def test_execute_durable_saves_off_loop_and_loop_stays_responsive(self):
        plan = self._seed_plan({
            "id": "plan-execute-io", "owner": ACTOR["id"], "scopes": ["A"], "turn_id": "turn_execute_io",
            "conversation_id": "conv", "title": "执行", "explanation": "", "status": "running",
            "risk": "normal", "version": 1, "error": "", "fields": [], "_references": {}, "_queries": {},
            "operations": [{"api_id": "POST /fixture-op", "body": {"scope": "A", "name": "one"}}],
            "results": [],
        })
        self._arm_timer = True
        await self.asyncSetUp()
        self._reset_block(put=True)
        self.agent.executing.add((ACTOR["id"], plan["id"]))
        with patch.object(self.agent, "_validate", return_value=(None, [])), \
             patch.object(self.agent, "_invoke", new=AsyncMock(return_value={
                "ok": True, "_raw": {"record_id": "r1"}, "data": {"record_id": "r1"}})), \
             patch.object(self.agent.catalog, "get", return_value={"read_only": False}):
            await self.agent._execute(ACTOR, plan, self.request)
        # A durable save wrote the store from a worker thread and the loop ticked while it blocked.
        self.assertIsNotNone(self.store.blocked_put_thread)
        self.assertNotEqual(self.store.blocked_put_thread, self.store.loop_thread)
        self.assertGreater(self.store.blocked_delta, 0)
        stored = self.agent.get_plan(ACTOR, plan["id"])
        self.assertEqual(stored["status"], "completed")
        self.assertEqual(len(stored["results"]), 1)
        self.assertNotIn((ACTOR["id"], plan["id"]), self.agent.executing)

    async def test_stage_water_photos_file_check_and_save_off_loop(self):
        water_actor = {**ACTOR, "is_admin": True}
        plan = self._seed_plan({
            "id": "plan-water-io", "owner": water_actor["id"], "scopes": ["A"], "turn_id": "turn_water_io",
            "conversation_id": "conv", "title": "水耗", "explanation": "", "status": "running",
            "risk": "normal", "version": 1, "error": "", "fields": [], "_references": {}, "_queries": {},
            "operations": [{"api_id": "POST /api/capacity/water/records", "body": {"scope": "A"}}],
            "results": [], "_water_files": {"0": ["file-1"]}, "_water_uploads": {},
        })
        self._arm_timer = True
        await self.asyncSetUp()
        self._reset_block(put=True)
        file_threads = []
        def fake_check(actor, operation, identity):
            file_threads.append(threading.get_ident())
        expires = time.time() + 10000
        with patch.object(self.agent, "_check_plan_file", new=fake_check), \
             patch.object(self.agent, "_invoke", new=AsyncMock(return_value={
                "ok": True, "_raw": {"upload_id": "up-1", "expires_at": expires},
                "data": {"upload_id": "up-1", "expires_at": expires}})):
            op = copy.deepcopy(plan["operations"][0])
            await self.agent._stage_water_photos(water_actor, plan, 0, op, self.request)
        # The file check ran in a worker thread, not on the loop.
        self.assertEqual(len(file_threads), 1)
        self.assertNotEqual(file_threads[0], self.store.loop_thread)
        # The durable save wrote the plan off-loop.
        self.assertIsNotNone(self.store.blocked_put_thread)
        self.assertNotEqual(self.store.blocked_put_thread, self.store.loop_thread)
        self.assertEqual(op["body"]["upload_ids"], ["up-1"])
        stored = self.agent.get_plan(ACTOR, plan["id"])
        self.assertEqual(stored["_water_uploads"]["0"]["file-1"]["upload_id"], "up-1")

    async def test_cancel_during_durable_save_finishes_write_then_final_wins(self):
        seeded = self._seed_plan({
            "id": "plan-cancel-save", "owner": ACTOR["id"], "scopes": ["A"], "turn_id": "turn_cancel_save",
            "conversation_id": "conv", "title": "取消", "explanation": "", "status": "running",
            "risk": "normal", "version": 1, "error": "", "fields": [], "operations": [], "results": [],
        })
        # Arm the store so the (shielded) durable write blocks in its worker.
        self._reset_block(put=True)
        original_save = self.agent._save_plan
        started = {"ok": False}
        def blocking_save(actor, plan):
            started["ok"] = True
            return original_save(actor, plan)
        with patch.object(self.agent, "_save_plan", new=blocking_save):
            save_task = asyncio.create_task(self.agent._durable_save(ACTOR, seeded))
            while not started["ok"]:
                await asyncio.sleep(0.001)
            # The worker is now blocked inside the durable write; cancel the caller.
            save_task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await save_task
        # The in-flight atomic snapshot was still durably persisted (shielded/finished).
        self.assertEqual(self.agent.get_plan(ACTOR, seeded["id"])["status"], "running")
        # Persist a final/cancelled snapshot last; it must win and stay.
        final = self.agent.get_plan(ACTOR, seeded["id"])
        final["status"] = "failed"
        final["error"] = "服务关闭导致执行中断"
        await self.agent._durable_save(ACTOR, final)
        await asyncio.sleep(0.05)  # allow any hypothetical orphan late write to appear
        stored = self.agent.get_plan(ACTOR, seeded["id"])
        self.assertEqual(stored["status"], "failed")
        self.assertEqual(stored["error"], "服务关闭导致执行中断")

    async def test_repeated_cancel_during_durable_save_finishes_then_final_wins(self):
        # Deterministic Event barrier: a repeated outer cancellation while the
        # owned write is blocked must NOT make the _durable_save caller terminal
        # before the worker finishes (no orphaned late write), and the final
        # cancelled snapshot must never be overwritten afterward.
        seeded = self._seed_plan({
            "id": "plan-repeated-cancel-save", "owner": ACTOR["id"], "scopes": ["A"],
            "turn_id": "turn_repeated_cancel_save", "conversation_id": "conv", "title": "重复取消",
            "explanation": "", "status": "running", "risk": "normal", "version": 1,
            "error": "", "fields": [], "operations": [], "results": [],
        })
        started, release = threading.Event(), threading.Event()
        original_save = self.agent._save_plan
        def blocked_save(actor, plan):
            # Signals the test then blocks the worker until explicitly released.
            started.set()
            release.wait(10)
            return original_save(actor, plan)
        with patch.object(self.agent, "_save_plan", new=blocked_save):
            save_task = asyncio.create_task(self.agent._durable_save(ACTOR, seeded))
            await asyncio.to_thread(started.wait, 5)
            # Cancel twice while the write is blocked; the caller must survive both.
            save_task.cancel()
            await asyncio.sleep(0.02)
            save_task.cancel()
            await asyncio.sleep(0.02)
            self.assertFalse(save_task.done())           # non-terminal until worker finishes
            self.assertFalse(release.is_set())           # worker genuinely still blocked
            release.set()                                 # let the owned write complete
            with self.assertRaises(asyncio.CancelledError):
                await save_task
        # The in-flight atomic snapshot was still durably persisted.
        self.assertEqual(self.agent.get_plan(ACTOR, seeded["id"])["status"], "running")
        # Persist a final/cancelled snapshot last; it must win and stay forever.
        final = self.agent.get_plan(ACTOR, seeded["id"])
        final["status"] = "failed"
        final["error"] = "服务关闭导致执行中断"
        await self.agent._durable_save(ACTOR, final)
        await asyncio.sleep(0.05)  # allow any hypothetical orphan late write to appear
        stored = self.agent.get_plan(ACTOR, seeded["id"])
        self.assertEqual(stored["status"], "failed")
        self.assertEqual(stored["error"], "服务关闭导致执行中断")

    async def test_teardown_all_task_cancellation_does_not_busy_spin(self):
        # Service-teardown regression, run in an isolated bounded subprocess so
        # a busy-spin (which would prevent asyncio.wait_for's own timeout from
        # firing) fails the parent with a timeout instead of hanging the whole
        # packaging process.  asyncio.run() cancels every asyncio Task via
        # asyncio.all_tasks(); the owned write must be a plain executor future
        # (NOT an asyncio Task) so teardown cannot cancel it, and the caller must
        # stay non-terminal waiting on the shielded write rather than spinning.
        self._run_child(_CHILD_TEARDOWN_SAVE)

    async def test_durable_save_worker_cancelled_error_propagates(self):
        # Worker-raised CancelledError finishes the executor future; the caller
        # must break out of the retry loop on completion (not busy-spin) and
        # propagate the worker outcome.  Bounded in a subprocess so a regression
        # cannot hang the runner.
        self._run_child(_CHILD_WORKER_CANCELLED_SAVE)

    async def test_durable_save_worker_failure_propagates_under_cancellation(self):
        # A failing durable write must not be swallowed even when the caller is
        # cancelled mid-write: the worker exception is observed and propagated
        # (a busy loop must never mask a completed failed future).
        seeded = self._seed_plan({
            "id": "plan-worker-fail-save", "owner": ACTOR["id"], "scopes": ["A"],
            "turn_id": "turn_worker_fail", "conversation_id": "conv", "title": "失败",
            "explanation": "", "status": "running", "risk": "normal", "version": 1,
            "error": "", "fields": [], "operations": [], "results": [],
        })
        started, release = threading.Event(), threading.Event()
        def failing_save(actor, plan):
            started.set()
            release.wait(10)
            raise OSError("durable write failed")
        with patch.object(self.agent, "_save_plan", new=failing_save):
            task = asyncio.create_task(self.agent._durable_save(ACTOR, seeded))
            await asyncio.to_thread(started.wait, 5)
            task.cancel()
            release.set()
            with self.assertRaises(OSError):
                await task

    async def test_durable_save_worker_sees_caller_contextvar(self):
        # copy_context() must be applied when running the worker off-loop so the
        # thread observes the original caller context, not an empty one.
        seeded = self._seed_plan({
            "id": "plan-context-save", "owner": ACTOR["id"], "scopes": ["A"],
            "turn_id": "turn_context_save", "conversation_id": "conv", "title": "上下文",
            "explanation": "", "status": "running", "risk": "normal", "version": 1,
            "error": "", "fields": [], "operations": [], "results": [],
        })
        marker = contextvars.ContextVar("save_marker", default="unset")
        seen = {}
        token = marker.set("from-test")
        def reading_save(actor, plan):
            seen["value"] = marker.get()
        try:
            with patch.object(self.agent, "_save_plan", new=reading_save):
                await self.agent._durable_save(ACTOR, seeded)
        finally:
            marker.reset(token)
        self.assertEqual(seen["value"], "from-test")

    def _reset_block(self, put=False, get=False):
        self.store._block_put = put
        self.store._block_get = get
        self.store.blocked_put_thread = None
        self.store.blocked_get_thread = None
        self.store.blocked_delta = 0


if __name__ == "__main__":
    unittest.main()
