"""Focused non-blocking coverage for PortalAgent.confirm/refresh.

These tests verify that the lock-atomic store reads, version checks, mutations
and saves originally performed on the event loop now run off-loop (in worker
threads); that a loop timer/health task stays responsive while commit I/O is in
flight; that an accepted confirm whose HTTP request is cancelled still schedules
the execution (no orphaned ``executing`` membership); and that duplicate
confirm / duplicate submitted-refresh never start duplicate tasks.

Regression coverage: a post-commit public-snapshot rendering failure must not
lose an already-committed + scheduled operation or schedule it twice; and a
submitted-resume whose persistence write fails must restore the previous
submitted snapshot, never reserve ``executing``, and never schedule execution.
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
_CHILD_TEARDOWN_COMMIT = r'''
import asyncio
import sys
import threading
from lan_bitable_template_portal.lighthouse_agent import PortalAgent

agent = PortalAgent.__new__(PortalAgent)

async def main():
    started, release = threading.Event(), threading.Event()
    calls = []
    def worker():
        started.set()
        release.wait(10)
        return "accepted-result"
    commit = asyncio.create_task(
        agent._run_commit(worker, on_commit=lambda r: calls.append(r))
    )
    await asyncio.to_thread(started.wait, 5)
    assert started.is_set()
    current = asyncio.current_task()
    for t in list(asyncio.all_tasks()):
        if t is not current:
            t.cancel()
    # Not an asyncio Task: teardown cannot cancel the commit; the caller stays
    # non-terminal waiting on the shielded write, not busy-spinning.
    assert not commit.done()
    release.set()
    try:
        await commit
    except asyncio.CancelledError:
        pass
    else:
        print("NO_CANCELLED_ERROR")
        sys.exit(1)
    assert calls == ["accepted-result"], calls
    print("CHILD_OK")
    sys.exit(0)

asyncio.run(main())
'''

_CHILD_WORKER_CANCELLED_COMMIT = r'''
import asyncio
import sys
from lan_bitable_template_portal.lighthouse_agent import PortalAgent

agent = PortalAgent.__new__(PortalAgent)

async def main():
    calls = []
    def worker():
        raise asyncio.CancelledError("worker-cancel")
    try:
        await agent._run_commit(worker, on_commit=lambda r: calls.append(r))
    except asyncio.CancelledError:
        pass
    else:
        print("NO_CANCELLED_ERROR")
        sys.exit(1)
    # Callback must never fire for a commit the worker did not complete.
    assert calls == [], calls
    print("CHILD_OK")
    sys.exit(0)

asyncio.run(main())
'''


class RecordingStore:
    """In-memory store that records the calling thread for every write and can
    briefly block the first plan-namespace write so tests can observe the event
    loop staying responsive while the commit runs off-loop."""

    def __init__(self, path, block_seconds=0.05):
        self.db_path, self.block_seconds = path, block_seconds
        self.docs = {}
        self.call_threads = []
        self.loop_thread = None
        self.timer = 0
        self._measure = True
        self.commit_put_thread = None
        self.commit_delta = 0

    def get_document(self, namespace, key):
        return copy.deepcopy(self.docs.get((namespace, key)))

    def put_document(self, namespace, key, value):
        ident = threading.get_ident()
        self.call_threads.append(ident)
        if namespace == PLAN_NAMESPACE and self._measure:
            self._measure = False
            self.commit_put_thread = ident
            before = self.timer
            time.sleep(self.block_seconds)
            self.commit_delta = self.timer - before
        self.docs[namespace, key] = copy.deepcopy(value)


async def _tick_loop(store):
    store.loop_thread = threading.get_ident()
    while True:
        store.timer += 1
        await asyncio.sleep(0.002)


async def _blocking_execute(actor, plan, request):
    await asyncio.sleep(10)


class NonblockingPlanTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = RecordingStore(Path(self.tmp.name) / "state.sqlite3")
        self.model = Mock()
        self.model.settings.return_value = {
            "configured": True, "enabled": True, "active_model_id": "default",
            "models": [{"id": "default", "name": "测试模型", "model": "fixture-model", "configured": True}],
        }
        self.model.profile.return_value = {"id": "default", "name": "测试模型", "model": "fixture-model"}
        self.search = Mock(side_effect=AssertionError("agent must use backend APIs, not local scraping"))
        self.assistant = LighthouseAssistant(self.store, self.search, model=self.model)
        self.files = LighthouseFiles(self.store)
        self.writes = []
        self.executions = 0
        app = FastAPI()

        @app.post("/api/create")
        async def fixture_create(request: Request):
            self.writes.append("create")
            return {"ok": True, "data": {"id": "created-fixture"}}

        self.catalog = PortalAPICatalog(app)
        self.agent = PortalAgent(self.assistant, self.catalog, self.files)
        self.request = Request({"type": "http", "scheme": "http", "server": ("testserver", 80),
                                "client": ("127.0.0.1", 4567), "path": "/api/assistant/agent",
                                "root_path": "", "query_string": b"",
                                "headers": [(b"cookie", b"fixture=a"), (b"origin", b"http://testserver")]})

    def _arm_measure(self):
        # prepare() already consumed the store's one-shot measurement; re-arm it
        # for the confirm/refresh commit that we actually want to observe on-loop
        # responsiveness for.
        self.store._measure = True
        self.store.commit_put_thread = None
        self.store.commit_delta = 0

    async def _prepare_confirmation_plan(self):
        decision = {"title": "创建一条记录", "explanation": "低风险创建",
                    "operations": [{"api_id": "POST /api/create", "body": {"scope": "A"}}]}
        plan = self.agent.prepare(ACTOR, decision, "op_nonblock_00000001", [])
        self.assertEqual(plan["status"], "awaiting_confirmation")
        self.assertEqual(plan["risk"], "normal")
        return plan

    async def _cancel_tasks(self, predicate=None):
        pending = list(self.agent.tasks)
        for task in pending:
            if predicate is None or predicate(task):
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

    async def test_confirm_commit_is_off_loop_and_loop_stays_responsive(self):
        plan = await self._prepare_confirmation_plan()
        timer = asyncio.create_task(_tick_loop(self.store))
        self.addCleanup(timer.cancel)
        await asyncio.sleep(0.02)  # let the health timer start and set loop_thread
        self._arm_measure()
        # The real _execute would perform its own sync saves; stub it so the only
        # blocking I/O under observation is the confirm commit worker.
        with patch.object(self.agent, "_execute", new=_blocking_execute):
            public = await self.agent.confirm(
                ACTOR, plan["id"], {"version": plan["version"], "stage": "review"}, self.request)
        self.assertEqual(public["status"], "running")
        # A store write during the commit happened in a worker thread, not on the loop.
        self.assertIsNotNone(self.store.commit_put_thread)
        self.assertNotEqual(self.store.commit_put_thread, self.store.loop_thread)
        # While the commit's blocking I/O was in flight the loop timer kept ticking.
        self.assertGreater(self.store.commit_delta, 0)
        await self._cancel_tasks()

    async def test_cancellation_during_commit_still_schedules_execution(self):
        plan = await self._prepare_confirmation_plan()
        self.store.block_seconds = 0.3
        self._arm_measure()
        with patch.object(self.agent, "_execute", new=_blocking_execute):
            confirm_task = asyncio.create_task(self.agent.confirm(
                ACTOR, plan["id"], {"version": plan["version"], "stage": "review"}, self.request))
            await asyncio.sleep(0.05)   # commit worker is now blocked inside its store write
            confirm_task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await confirm_task
            # The accepted execution must not be orphaned: executing membership and a
            # scheduled task must coexist even though the HTTP request was cancelled.
            self.assertIn((ACTOR["id"], plan["id"]), self.agent.executing)
            self.assertGreaterEqual(len(self.agent.tasks), 1)
        await self._cancel_tasks()

    async def test_repeated_cancel_during_commit_schedules_once_on_original_loop(self):
        # Deterministic barrier test for _run_commit: a *repeated* outer
        # cancellation while the commit worker is blocked must not make the caller
        # terminal before the worker finishes; afterward on_commit is invoked
        # exactly once, on the original event-loop thread, and cancellation is
        # then propagated.
        started, release = threading.Event(), threading.Event()
        loop_thread = threading.get_ident()
        on_commit_calls = []
        def worker():
            started.set()
            release.wait(10)   # block the owned commit until the test releases it
            return "accepted-result"
        commit = asyncio.create_task(self.agent._run_commit(
            worker,
            on_commit=lambda result: on_commit_calls.append((threading.get_ident(), result)),
        ))
        await asyncio.to_thread(started.wait, 5)
        commit.cancel()
        await asyncio.sleep(0.02)
        commit.cancel()
        await asyncio.sleep(0.02)
        # Repeated cancellation must not finalize the caller while worker is blocked.
        self.assertFalse(commit.done())
        self.assertFalse(release.is_set())
        self.assertEqual(on_commit_calls, [])
        release.set()   # let the owned commit finish
        with self.assertRaises(asyncio.CancelledError):
            await commit
        # Accepted durable commit scheduled exactly once, on the original loop.
        self.assertEqual(on_commit_calls, [(loop_thread, "accepted-result")])

    async def test_teardown_all_task_cancellation_does_not_busy_spin_commit(self):
        # Service-teardown regression for _run_commit, run in an isolated bounded
        # subprocess so a busy-spin (which would prevent asyncio.wait_for's own
        # timeout from firing) fails the parent with a timeout instead of hanging
        # the whole packaging process.  asyncio.run() cancels every asyncio Task
        # via asyncio.all_tasks(); the commit worker must be a plain executor
        # future (NOT an asyncio Task) so teardown cannot cancel it, and the
        # caller must stay non-terminal waiting on the shielded write.
        self._run_child(_CHILD_TEARDOWN_COMMIT)

    async def test_run_commit_worker_cancelled_error_callback_not_fired(self):
        # Worker-raised CancelledError finishes the executor future; the caller
        # must break out of the retry loop on completion (not busy-spin) and the
        # on_commit callback must never fire for an unaccepted result.  Bounded
        # in a subprocess so a regression cannot hang the runner.
        self._run_child(_CHILD_WORKER_CANCELLED_COMMIT)

    async def test_run_commit_worker_failure_propagates(self):
        # A failing commit worker must propagate its exception and must never
        # invoke on_commit for a result that was not accepted.
        started, release = threading.Event(), threading.Event()
        on_commit_calls = []
        def failing_worker():
            started.set()
            release.wait(10)
            raise OSError("commit failed")
        try:
            with self.assertRaises(OSError):
                await asyncio.wait_for(self.agent._run_commit(
                    failing_worker,
                    on_commit=lambda result: on_commit_calls.append(result),
                ), timeout=10)
        finally:
            release.set()
        self.assertEqual(on_commit_calls, [])

    async def test_run_commit_worker_sees_caller_contextvar(self):
        # copy_context() must apply when the _run_commit worker runs off-loop.
        marker = contextvars.ContextVar("commit_marker", default="unset")
        seen = {}
        token = marker.set("from-test")
        def reading_worker():
            seen["value"] = marker.get()
            return "ok"
        try:
            result = await self.agent._run_commit(reading_worker, on_commit=lambda _r: None)
        finally:
            marker.reset(token)
        self.assertEqual(result, "ok")
        self.assertEqual(seen["value"], "from-test")

    async def test_duplicate_confirm_does_not_schedule_twice(self):
        plan = await self._prepare_confirmation_plan()
        started = []
        async def counting_execute(actor, plan, request):
            started.append(plan["id"])
            await asyncio.sleep(10)
        with patch.object(self.agent, "_execute", new=counting_execute):
            first = await self.agent.confirm(
                ACTOR, plan["id"], {"version": plan["version"], "stage": "review"}, self.request)
            await asyncio.sleep(0.01)   # let the scheduled execution stub start
            self.assertEqual(first["status"], "running")
            self.assertEqual(len(started), 1)
            # A duplicate confirm while running is idempotent and must not spawn a task.
            second = await self.agent.confirm(
                ACTOR, plan["id"], {"version": plan["version"], "stage": "review"}, self.request)
            self.assertEqual(second["status"], "running")
            self.assertEqual(len(started), 1)
            self.assertEqual(len(self.agent.tasks), 1)
        await self._cancel_tasks()

    async def test_confirm_render_failure_still_commits_and_schedules_once(self):
        # Defect regression: the post-commit public snapshot must be rendered only
        # AFTER the accepted action was committed and scheduled.  A rendering
        # failure must not lose the already-accepted (committed + scheduled)
        # operation, nor schedule it twice.
        plan = await self._prepare_confirmation_plan()
        scheduled = []
        async def counting_execute(actor, plan, request):
            scheduled.append(plan["id"])
            await asyncio.sleep(10)
        def failing_public(target, actor=None):
            raise RuntimeError("post-commit render failed")
        with patch.object(self.agent, "public_plan", new=failing_public), \
             patch.object(self.agent, "_execute", new=counting_execute):
            with self.assertRaises(RuntimeError):
                await self.agent.confirm(
                    ACTOR, plan["id"], {"version": plan["version"], "stage": "review"}, self.request)
        # The accepted operation is committed and scheduled exactly once despite
        # the frozen reply render failing after scheduling.
        self.assertEqual(self.agent.get_plan(ACTOR, plan["id"])["status"], "running")
        self.assertIn((ACTOR["id"], plan["id"]), self.agent.executing)
        self.assertEqual(len(scheduled), 1)
        await self._cancel_tasks()

    async def test_confirm_reply_is_frozen_before_execution_changes_plan(self):
        plan = await self._prepare_confirmation_plan()
        async def execute(actor, accepted, request):
            accepted['status'] = 'completed'
        with patch.object(self.agent, '_execute', new=execute):
            reply = await self.agent.confirm(ACTOR, plan['id'],
                {'version': plan['version'], 'stage': 'review'}, self.request)
        self.assertEqual(reply['status'], 'running')
        await self._cancel_tasks()

    async def _seed_submitted_plan(self):
        state = self.assistant._state(ACTOR)
        plan = {
            "id": "plan-submitted-nonblock",
            "owner": ACTOR["id"],
            "scopes": ["A"],
            "turn_id": "turn_submitted_00000002",
            "conversation_id": state["id"],
            "title": "多步提交恢复",
            "explanation": "",
            "status": "submitted",
            "risk": "normal",
            "version": 3,
            "error": "",
            "fields": [],
            "operations": [
                {"api_id": "POST /api/create", "body": {"scope": "A", "name": "one"}},
                {"api_id": "POST /api/create", "body": {"scope": "A", "name": "two"}},
            ],
            "results": [
                {"ok": True, "api_id": "POST /api/create", "data": {"record_id": "rec-1"},
                 "_task": {"operation": {"api_id": "GET /api/create/job"}, "kind": "job"}},
            ],
        }
        self.store.put_document(PLAN_NAMESPACE, plan["id"], copy.deepcopy(plan))
        return plan

    async def test_submitted_recovery_resumes_execution(self):
        seeded = await self._seed_submitted_plan()
        started = []
        async def counting_execute(actor, plan, request):
            started.append(plan["id"])
            await asyncio.sleep(10)
        job_payload = {"ok": True, "data": {"job_id": "job-mid", "finished": True}}
        with patch.object(self.agent, "_task_result", new=AsyncMock(return_value=(job_payload, True))), \
             patch.object(self.agent, "_execute", new=counting_execute):
            public = await self.agent.refresh(ACTOR, copy.deepcopy(seeded), self.request)
        self.assertEqual(public["status"], "running")
        self.assertEqual(len(started), 1)
        self.assertIn((ACTOR["id"], seeded["id"]), self.agent.executing)
        await self._cancel_tasks()

    async def test_submitted_recovery_duplicate_refresh_does_not_double_schedule(self):
        seeded = await self._seed_submitted_plan()
        started = []
        async def counting_execute(actor, plan, request):
            started.append(plan["id"])
            await asyncio.sleep(10)
        job_payload = {"ok": True, "data": {"job_id": "job-mid", "finished": True}}
        with patch.object(self.agent, "_task_result", new=AsyncMock(return_value=(job_payload, True))), \
             patch.object(self.agent, "_execute", new=counting_execute):
            a, b = await asyncio.gather(
                self.agent.refresh(ACTOR, copy.deepcopy(seeded), self.request),
                self.agent.refresh(ACTOR, copy.deepcopy(seeded), self.request),
            )
        # Only one of the two concurrent same-owner refreshes may resume execution.
        self.assertEqual(len(started), 1)
        self.assertEqual(a["status"], "running")
        self.assertEqual(b["status"], "running")
        await self._cancel_tasks()

    async def test_submitted_recovery_save_failure_restores_snapshot_no_reserve_no_schedule(self):
        # Defect regression: executing membership must only be reserved AFTER the
        # resumed plan is durably persisted.  A failed partial resume write must
        # restore the previous submitted snapshot where possible and must never
        # leave the owner running with no scheduled execution task.
        seeded = await self._seed_submitted_plan()
        started = []
        async def counting_execute(actor, plan, request):
            started.append(plan["id"])
            await asyncio.sleep(10)
        job_payload = {"ok": True, "data": {"job_id": "job-mid", "finished": True}}
        original_save = self.agent._save_plan
        state = {"fail_once": True}
        def flaky_save(actor, plan):
            if state["fail_once"]:
                state["fail_once"] = False
                raise OSError("resume write failed")
            return original_save(actor, plan)
        with patch.object(self.agent, "_task_result", new=AsyncMock(return_value=(job_payload, True))), \
             patch.object(self.agent, "_execute", new=counting_execute), \
             patch.object(self.agent, "_save_plan", new=flaky_save):
            with self.assertRaises(OSError):
                await self.agent.refresh(ACTOR, copy.deepcopy(seeded), self.request)
        # Failed partial commit: no membership reservation and no execution task.
        self.assertNotIn((ACTOR["id"], seeded["id"]), self.agent.executing)
        self.assertEqual(len(started), 0)
        # Where possible, the previous submitted snapshot is restored so the plan
        # remains recoverable (still submitted, results unchanged).
        stored = self.agent.get_plan(ACTOR, seeded["id"])
        self.assertEqual(stored["status"], "submitted")
        self.assertEqual(len(stored["results"]), 1)


if __name__ == "__main__":
    unittest.main()
