"""Focused synthetic startup regression tests for OpenClaw warm-start / runtime readiness.

These verify the regressions around the user-reported "first question timed out before
model inference" defect (gateway not prewarmed):

* ``OpenClawRuntime.prepare`` caches a verified runtime/install under ``prepare_lock``
  (default ``startup_timeout=180``) and the canonical tool fingerprint lets the warm run
  and the first real run reuse one Node process (one ``Popen``).
* ``LighthouseOpenClaw.queue_warmup`` dedups per-account, honours the resource cap and
  per-account rate limit, and never spawns for disabled/missing models.
* ``_warmup`` registers the same typed tool schemas via ``LighthouseModel.answer(..., warm_only=True)``
  but runs no model turn, no business query, no file reads and no messages.
* The first real question ``run_stream_events`` calls ``wait_warmup`` before acquiring a
  runtime (joins in-flight preparation), and warmup failures propagate safely.
* ``close()`` cancels in-flight preparation/handshake; a legacy/guest route never spawns.

Everything is synthetic: temporary state roots, mocked subprocess / GatewayClient, no real
Node, model, cloud or credentials. Reuses helpers from ``bin.test_lighthouse_openclaw``
and ``bin.test_lighthouse_runtime``.
"""

from __future__ import annotations

import asyncio
import contextlib
import sys
import tempfile
import time
import unittest
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock, patch

BIN = Path(__file__).resolve().parent
if str(BIN) not in sys.path:
    sys.path.insert(0, str(BIN))
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import httpx
from lan_bitable_template_portal.lighthouse_ai import AssistantError
from lan_bitable_template_portal.lighthouse_gateway import GatewayError
from lan_bitable_template_portal.lighthouse_openclaw import BusinessBridge, LighthouseOpenClaw, OpenClawToolAgent
from lan_bitable_template_portal.lighthouse_runtime import OpenClawRuntime, account_key, AssistantStartupError

from bin.test_lighthouse_openclaw import FakeGatewayClient
from bin.test_lighthouse_runtime import _FakeProcess, _actor, _model, _profile


async def _noop_emit(*_):
    return None


def _blocked_task():
    """A task that never completes by itself; engine.close() cancels it safely."""
    return asyncio.create_task(asyncio.Event().wait())


# ---------------------------------------------------------------------------
# Fakes / builders
# ---------------------------------------------------------------------------


class SpyAgent:
    """Minimal Pydantic-Agent double that records tool registration and never runs."""

    def __init__(self, model, **kwargs):
        self.model = model
        self.kwargs = kwargs
        self.tools = {}
        self.tool_kwargs = {}
        self.validators = []
        self.warmup_called = 0
        self.run_called = 0
        self.message_history = None

    def tool_plain(self, function=None, **kwargs):
        def register(fn):
            self.tools[fn.__name__] = fn
            self.tool_kwargs[fn.__name__] = kwargs
            return fn
        return register(function) if function else register

    def output_validator(self, function):
        self.validators.append(function)
        return function

    async def warmup(self):
        self.warmup_called += 1

    @asynccontextmanager
    async def run_stream_events(self, prompt, *, message_history=None, **_):
        self.run_called += 1
        self.message_history = message_history

        async def empty():
            if False:
                yield None

        yield empty()


class FailingWarmupAgent(SpyAgent):
    async def warmup(self):
        raise AssistantError("助手后台准备未完成，请重试原消息。", 503)


class FakeAssistant:
    """Double for LighthouseAssistant; controlled model selection and profile."""

    def __init__(self, *, enabled=True, profile=None, models=None):
        self._lock = MagicMock()
        self.model = Mock()
        self.enabled = enabled
        self._models = models
        self.profile_base = profile or {
            "model": "fixture-model",
            "name": "Fixture",
            "endpoint": "https://provider.example/v1/chat/completions",
            "key_cipher": "cipher-wrapper-secret",
            "vision_verified": True,
            "context_window": 32000,
        }

    def model_for(self, actor):
        model = Mock()
        configured = (self._models if self._models is not None else
                      [{"id": "t", "name": self.profile_base["name"],
                        "model": self.profile_base["model"], "configured": True}])
        model.settings.return_value = {
            "enabled": self.enabled,
            "active_model_id": "t",
            "models": configured,
        }
        model.profile.side_effect = lambda selected: dict(self.profile_base, id=selected)
        return model

    @staticmethod
    def _state(actor):
        return {"id": "conv-" + actor["id"]}

    @staticmethod
    def _selected(data, settings):
        available = [p for p in settings.get("models", []) if p.get("configured")]
        return next((p for p in available if p["id"] == settings.get("active_model_id")),
                    next(iter(available), None))


def make_engine(tmp, *, enabled=True, profile=None, runtime_root=None, models=None):
    """Real LighthouseOpenClaw with mocked portal/assistant and a real temp manager.

    By default the manager is never used for spawning in these tests (agent_factory is
    swapped for a spy or manager.acquire is replaced), so no Node process can start.
    """
    portal = Mock()
    portal.assistant = FakeAssistant(enabled=enabled, profile=profile, models=models)
    portal.store = Mock()
    portal.store.db_path = str(Path(tmp) / "state.sqlite3")
    portal.catalog = Mock()
    portal.files = Mock()
    engine = LighthouseOpenClaw(
        portal,
        bridge_url=lambda: "http://127.0.0.1:19999/api/assistant/openclaw-tools",
        state_root=Path(tmp) / "openclaw-state",
        runtime_root=runtime_root,
    )
    return engine, portal


# ---------------------------------------------------------------------------
# OpenClawRuntime: prepare caching, canonical fingerprint reuse, one Popen
# ---------------------------------------------------------------------------


class OpenClawRuntimeStartupTests(unittest.IsolatedAsyncioTestCase):
    async def test_startup_timeout_defaults_to_180(self):
        with tempfile.TemporaryDirectory() as tmp:
            runtime = OpenClawRuntime(Path(tmp), runtime_root=Path(tmp) / "runtime")
            try:
                self.assertEqual(runtime.startup_timeout, 180)
            finally:
                await runtime.close()

    async def test_prepare_caches_verified_runtime_across_acquires(self):
        with tempfile.TemporaryDirectory() as tmp:
            runtime = OpenClawRuntime(Path(tmp), runtime_root=Path(tmp) / "runtime")
            calls = {"n": 0}

            def fake_runtime_files(root=None):
                calls["n"] += 1
                return (Path("node.exe"), Path("openclaw.mjs"))

            try:
                with patch.object(runtime, "_ready", new_callable=AsyncMock,
                                  side_effect=lambda item, *a, **k: item), \
                     patch("lan_bitable_template_portal.lighthouse_runtime.runtime_files",
                           side_effect=fake_runtime_files), \
                     patch("lan_bitable_template_portal.lighthouse_runtime.free_port",
                           return_value=18789), \
                     patch("lan_bitable_template_portal.lighthouse_runtime.secrets.token_urlsafe",
                           return_value="test-token"), \
                     patch("lan_bitable_template_portal.lighthouse_runtime.subprocess.Popen",
                           return_value=_FakeProcess()), \
                     patch("upload_event_module.services.process_lifetime.register_child_process",
                           return_value=True):
                    await runtime.prepare()
                    self.assertEqual(calls["n"], 1, "prepare must verify runtime exactly once")
                    await runtime.prepare()
                    self.assertEqual(calls["n"], 1, "prepare must be idempotent/ cached")
                    profile = _profile()
                    profile["key_cipher"] = "cipher"
                    item = await runtime.acquire(_actor("u-prep"), _model(), profile)
                    self.assertEqual(calls["n"], 1, "acquire must reuse cached prepare result")
                    self.assertTrue(item["busy"])
            finally:
                await runtime.close()

    async def test_canonical_fingerprint_warm_real_reuses_one_popen(self):
        with tempfile.TemporaryDirectory() as tmp:
            runtime = OpenClawRuntime(Path(tmp), runtime_root=Path(tmp) / "runtime")
            key = account_key("u-fp")
            plugin = Path(tmp) / key / "plugin"
            plugin.mkdir(parents=True, exist_ok=True)
            for name, content in {
                "index.mjs": b"// fixture",
                "openclaw.plugin.json": b"{}",
                "tools.json": b"[]",
            }.items():
                (plugin / name).write_bytes(content)
            profile = _profile()
            model = _model()
            actor = _actor("u-fp")
            common = dict(tool_names=["query", "prepare_business"],
                          bridge_url="http://bridge", bridge_token="bt")
            try:
                with patch.object(runtime, "_ready", new_callable=AsyncMock,
                                  side_effect=lambda item, *a, **k: item), \
                     patch("lan_bitable_template_portal.lighthouse_runtime.runtime_files",
                           return_value=(Path("node.exe"), Path("openclaw.mjs"))), \
                     patch("lan_bitable_template_portal.lighthouse_runtime.free_port",
                           return_value=18789), \
                     patch("lan_bitable_template_portal.lighthouse_runtime.secrets.token_urlsafe",
                           return_value="test-token"), \
                     patch("lan_bitable_template_portal.lighthouse_runtime.subprocess.Popen",
                           return_value=_FakeProcess()) as popen, \
                     patch("upload_event_module.services.process_lifetime.register_child_process",
                           return_value=True):
                    first = await runtime.acquire(actor, model, profile, plugin=plugin, **common)
                    self.assertTrue(first["busy"])
                    first["busy"] = False  # warmup parks the item idle; real run reuses it
                    second = await runtime.acquire(actor, model, profile, plugin=plugin, **common)
                self.assertIs(second, first,
                              "identical warm/real fingerprint must reuse the same gateway process")
                self.assertEqual(popen.call_count, 1, "warm+real must spawn exactly one Node process")
            finally:
                await runtime.close()

    async def test_ready_probes_models_list_readonly_and_unavailable_retries_to_deadline(self):
        import lan_bitable_template_portal.lighthouse_runtime as lrt
        process = _FakeProcess()
        gateway_calls = []

        class _UnavailableGateway:
            protocol = "fixture"

            def __init__(self, url, token):
                self.calls = []
                gateway_calls.append(self)

            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return False

            async def close(self):
                pass

            async def request(self, method, params=None, timeout=None):
                self.calls.append((method, params))
                if method == "models.list":
                    raise GatewayError("UNAVAILABLE")
                raise AssertionError("readiness probe must never send an agent RPC")

        clock = {"now": 1000.0}

        def monotonic():
            return clock["now"]

        async def advance_clock(*_a, **_k):
            clock["now"] += 5.0

        item = {"process": process, "port": 19999, "token": "gw", "startup_stage": "", 'agent_id': 'lh-fixture'}
        with tempfile.TemporaryDirectory() as tmp:
            runtime = OpenClawRuntime(Path(tmp), runtime_root=Path(tmp) / "runtime")
            try:
                with patch.object(lrt.time, "monotonic", side_effect=monotonic), \
                     patch.object(asyncio, "sleep", new=advance_clock):
                    with self.assertRaises(AssistantStartupError) as caught:
                        await runtime._ready(item, clock["now"] + 1.0, _UnavailableGateway, GatewayError)
                self.assertEqual(caught.exception.status, 503)
                self.assertIn("启动超时", str(caught.exception))
                methods = [method for client in gateway_calls for method, _ in client.calls]
                self.assertEqual(methods, ["models.list"],
                                 "only the local read-only models.list probe may run during readiness")
            finally:
                await runtime.close()


# ---------------------------------------------------------------------------
# LighthouseOpenClaw warmup lifecycle
# ---------------------------------------------------------------------------


class WarmupStartupTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.engine, self.portal = make_engine(self.tmp.name)
        self.actor = {"id": "u-warm", "scopes": ["A"]}

    async def asyncTearDown(self):
        await self.engine.close()

    async def test_warmup_registers_typed_tools_without_model_business_file_or_messages(self):
        spy = SpyAgent(None)

        def spy_factory(model, **kwargs):
            spy.__init__(model, **kwargs)
            return spy

        self.engine.agent_factory = spy_factory
        self.portal.files.context.side_effect = AssertionError("no file reads at warmup")
        self.portal.files.get.side_effect = AssertionError("no file reads at warmup")
        self.portal.files.image_parts.side_effect = AssertionError("no file reads at warmup")
        self.portal.files.upload.side_effect = AssertionError("no file writes at warmup")

        result = await self.engine._warmup(self.actor)

        self.assertIsNone(result, "_warmup must be a silent startup hook")
        self.assertEqual(spy.warmup_called, 1, "warmup must call agent.warmup()")
        self.assertEqual(spy.run_called, 0, "warmup must never run a model turn")
        self.assertIsNone(spy.message_history, "warmup must not build model history")
        for name in ("discover", "query", "notice_sends", "read_query",
                     "repair_overview", "pending_work", "search_local", "read_file",
                     "parse_notice", "search_history", "prepare_business"):
            self.assertIn(name, spy.tools, "identical typed tool schemas must be registered")
        self.portal.files.context.assert_not_called()
        self.portal.files.get.assert_not_called()
        self.portal.files.image_parts.assert_not_called()
        self.portal.files.upload.assert_not_called()
        self.assertEqual(self.engine.warming, {}, "successful warmup must not stay queued")

    async def test_warm_and_real_runs_register_identical_typed_tools(self):
        # The canonical set of typed tools must be identical whether the answer is
        # a warm-only registration or a real user turn.  Both go through the same
        # tool_plain registry before the warm_only branch, so we drive answer()
        # in both modes and compare the captured tool names AND their kwargs.
        def profile_for():
            with self.engine.assistant._lock:
                return self.engine.assistant.model_for(self.actor).profile("t")

        profile = profile_for()
        # Warm path: engine._warmup internally builds turn/_profile/emit/authorize.
        warm_spy = SpyAgent(None)
        self.engine.agent_factory = lambda model, **kw: warm_spy
        await self.engine._warmup(self.actor)

        # Real path: supply a real-looking question and permissive files mocks.
        real_spy = SpyAgent(None)
        self.engine.agent_factory = lambda model, **kw: real_spy
        self.portal.files.context.side_effect = lambda *a, **k: {}
        self.portal.files.image_parts.side_effect = lambda *a, **k: []

        async def authorize():
            return self.actor

        await self.engine.answer(
            self.actor, {"question": "你好", "_profile": profile}, [], None,
            _noop_emit, authorize, {}, warm_only=False)

        self.assertEqual(set(warm_spy.tools), set(real_spy.tools),
                         "warm and real runs must register the same canonical tool set")
        for name in warm_spy.tools:
            self.assertEqual(warm_spy.tool_kwargs[name], real_spy.tool_kwargs[name],
                             f"tool {name} must carry identical registration kwargs")
        required = {"discover", "query", "notice_sends", "read_query", "repair_overview",
                    "event_notices", "pending_work", "search_local", "read_file",
                    "parse_notice", "search_history", "prepare_business"}
        self.assertTrue(required <= set(warm_spy.tools), "canonical registry missing core tools")
        self.assertGreaterEqual(len(warm_spy.tools), 14)

    async def test_queue_warmup_deduplicates_active_account(self):
        key = account_key(self.actor["id"])
        blocked = _blocked_task()
        self.engine.warming[key] = blocked

        self.engine.queue_warmup(self.actor)

        self.assertIs(self.engine.warming[key], blocked, "active warmup must not be replaced")
        self.assertNotIn(key, self.engine.warm_attempts, "dedup must not burn the rate-limit slot")

    async def test_queue_warmup_respects_resource_cap(self):
        self.engine.manager.maximum = 2
        self.engine.warming = {"k1": _blocked_task(), "k2": _blocked_task()}
        key = account_key(self.actor["id"])

        self.engine.queue_warmup(self.actor)

        self.assertNotIn(key, self.engine.warming, "must not exceed the warmup resource cap")
        self.assertNotIn(key, self.engine.warm_attempts)

    async def test_queue_warmup_rate_limits_per_account(self):
        key = account_key(self.actor["id"])
        self.engine.warm_attempts[key] = time.monotonic()

        self.engine.queue_warmup(self.actor)

        self.assertNotIn(key, self.engine.warming, "rate-limited warmup must not reschedule")
        self.assertIn(key, self.engine.warm_attempts, "existing rate-limit slot is retained")

    async def test_queue_warmup_skips_when_process_already_running(self):
        key = account_key(self.actor["id"])
        existing = {
            "key": key,
            "process": Mock(),
            "output": MagicMock(),
            "busy": False,
            "used_at": 0.0,
        }
        existing["process"].poll.return_value = None
        self.engine.manager.accounts[key] = existing

        self.engine.queue_warmup(self.actor)

        self.assertNotIn(key, self.engine.warming)
        self.assertNotIn(key, self.engine.warm_attempts)

    async def test_disabled_model_never_spawns(self):
        engine, portal = make_engine(self.tmp.name, enabled=False)
        engine.agent_factory = lambda model, **kw: (_ for _ in ()).throw(AssertionError("no agent for disabled model"))
        engine.manager.acquire = AsyncMock(side_effect=AssertionError("must not spawn without a model"))
        actor = {"id": "u-off", "scopes": ["A"]}
        try:
            await engine._warmup(actor)
            engine.manager.acquire.assert_not_called()
            self.assertEqual(engine.warming, {})
            self.assertEqual(engine.warm_errors, {})
        finally:
            await engine.close()

    async def test_missing_model_never_spawns(self):
        # Enabled, but the account has no configured/selected model -> no profile.
        engine, portal = make_engine(self.tmp.name, enabled=True, models=[])
        engine.agent_factory = lambda model, **kw: (_ for _ in ()).throw(AssertionError("no agent for missing model"))
        engine.manager.acquire = AsyncMock(side_effect=AssertionError("must not spawn without a model"))
        actor = {"id": "u-missing", "scopes": ["A"]}
        try:
            await engine._warmup(actor)
            engine.manager.acquire.assert_not_called()
            self.assertEqual(engine.warming, {})
            self.assertEqual(engine.warm_errors, {})
        finally:
            await engine.close()

    async def test_wait_warmup_joins_inflight_preparation(self):
        key = account_key(self.actor["id"])
        started = asyncio.Event()
        release = asyncio.Event()

        async def blocking_acquire(*a, **k):
            started.set()
            await release.wait()
            return {"key": key, "port": 19999, "token": "gw", "busy": False,
                    "process": _FakeProcess(), "output": MagicMock(), "used_at": 0.0}

        self.engine.manager.acquire = blocking_acquire
        client = FakeGatewayClient('ws://127.0.0.1:19999', 'gw')
        self.engine.gateway_client = AsyncMock(return_value=client)
        self.engine.queue_warmup(self.actor)
        await asyncio.wait_for(started.wait(), 2)

        emit = AsyncMock()
        waiter = asyncio.create_task(self.engine.wait_warmup(self.actor, emit))
        await asyncio.sleep(0)  # let the waiter start awaiting the in-flight warmup
        release.set()
        await waiter

        emit.assert_awaited_once()
        self.assertEqual(
            emit.await_args.args[1]["label"], "正在准备助手，原消息已保留",
            "first question must join in-flight preparation with a status message")
        self.assertEqual([name for name, _ in client.calls], ['sessions.patch'])
        self.assertTrue(client.calls[0][1]['key'].startswith('agent:lh-'))

    async def test_warmup_failure_propagates_safe_via_wait_warmup(self):
        self.engine.agent_factory = lambda model, **kw: FailingWarmupAgent(model, **kw)
        self.engine.queue_warmup(self.actor)
        key = account_key(self.actor["id"])

        with self.assertRaises(AssistantError) as caught:
            await self.engine.wait_warmup(self.actor, _noop_emit)
        self.assertEqual(caught.exception.status, 503)
        self.assertIn(key, self.engine.warm_errors)

    async def test_cold_session_preparation_does_not_overlap_handshakes(self):
        from lan_bitable_template_portal.lighthouse_openclaw import OpenClawToolAgent
        active, peak, calls = 0, 0, []
        async def connect(actor, item):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(.01)
            async def request(method, params, timeout):
                nonlocal active
                calls.append((actor['id'], method, params))
                await asyncio.sleep(.01)
                active -= 1
                return {}
            return SimpleNamespace(request=request)
        self.engine.gateway_client = connect
        agents = []
        for identity in ('first', 'second', 'third'):
            agent = OpenClawToolAgent(self.engine, {**self.actor, 'id': identity}, {}, _noop_emit, {}, instructions='fixture')
            agent._acquire_runtime = AsyncMock(return_value={'busy': True})
            agents.append(agent)
        await asyncio.gather(*(agent.warmup() for agent in agents))
        self.assertEqual(peak, 1)
        self.assertEqual(len({entry[2]['key'] for entry in calls}), 3)
        self.assertTrue(all(entry[1] == 'sessions.patch' for entry in calls))

    async def test_close_cancels_inflight_warmup_and_clears_pending(self):
        key = account_key(self.actor["id"])
        started = asyncio.Event()

        async def blocking_acquire(*a, **k):
            started.set()
            await asyncio.Event().wait()

        self.engine.manager.acquire = blocking_acquire
        self.engine.queue_warmup(self.actor)
        await asyncio.wait_for(started.wait(), 2)

        await self.engine.close()

        self.assertTrue(self.engine.closing)
        self.assertEqual(self.engine.warming, {}, "close must cancel and clear warmup tasks")
        self.assertEqual(self.engine.manager.accounts, {})


# ---------------------------------------------------------------------------
# First question ordering on the real agent
# ---------------------------------------------------------------------------


class FirstQuestionStartupTests(unittest.IsolatedAsyncioTestCase):
    async def test_real_run_calls_wait_warmup_before_acquire(self):
        engine = SimpleNamespace(
            assistant=SimpleNamespace(
                _state=lambda actor: {"id": "conv-" + actor["id"]},
                model_for=lambda actor: {"id": "fixture-model"},
            ),
            tokens={},
            bridge=BusinessBridge(),
            manager=Mock(resident=False),
            wait_warmup=AsyncMock(),
            bridge_url=lambda: "http://bridge",
        )
        agent = OpenClawToolAgent(
            engine,
            {"id": "u1", "scopes": ["A"]},
            {"operation_id": "op-1", "run_id": ""},
            _noop_emit,
            {"model": "fixture-model"},
            instructions="x",
        )
        agent._acquire_runtime = AsyncMock(return_value={
            "port": 19999, "token": "gw", "busy": False, "used_at": 0.0, "process": Mock()})

        async def empty_events(client, prompt, history):
            if False:
                yield None

        agent._events = empty_events
        fake = FakeGatewayClient("ws://127.0.0.1:19999", "gw-token")
        engine.gateway_client = AsyncMock(return_value=fake)
        engine.keep_gateway = AsyncMock()
        with patch("lan_bitable_template_portal.lighthouse_openclaw.GatewayClient", return_value=fake):
            async with agent.run_stream_events(["hello"], message_history=[]) as events:
                async for _ in events:
                    break

        engine.wait_warmup.assert_awaited_once_with(agent.actor, agent.emit)
        self.assertEqual(agent._acquire_runtime.await_count, 1,
                         "the real question must acquire the runtime only after wait_warmup")


# ---------------------------------------------------------------------------
# Routes: formal accounts can read appearance; guests cannot enter the assistant.
# ---------------------------------------------------------------------------


class StartupRouteTests(unittest.TestCase):
    def setUp(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from lan_bitable_template_portal.lighthouse_appearance import DEFAULT_APPEARANCE
        from tools.lighthouse_test_backend import install_test_backend as install_lighthouse_routes
        from lan_bitable_template_portal.state_store import LanPortalStateStore

        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.store = LanPortalStateStore(db_path=Path(directory.name) / "test.sqlite3")
        self.session = {"open_id": "user-a", "role": "building", "allowed_scopes": ["D"]}
        self.controller = SimpleNamespace(
            _current_session=lambda request: self.session,
            _request_base_url=lambda request: str(request.base_url).rstrip("/"),
        )
        self.runtime = SimpleNamespace(state_store=self.store, auth_manager=SimpleNamespace(
            session_scopes=lambda session: session["allowed_scopes"],
            is_admin=lambda session: session["role"] == "admin"))
        app = FastAPI()
        install_lighthouse_routes(app, self.controller, self.runtime)
        self.client = TestClient(app, headers={"Origin": "http://testserver"})
        self.addCleanup(self.client.close)
        self.DEFAULT_APPEARANCE = DEFAULT_APPEARANCE

    def test_guest_forbidden_without_engine_spawn(self):
        self.session["is_guest"] = True
        with patch("lan_bitable_template_portal.lighthouse_openclaw.LighthouseOpenClaw") as engine_cls:
            response = self.client.get("/api/assistant/appearance")
        self.assertEqual(response.status_code, 403)
        engine_cls.assert_not_called()

    def test_scope_less_formal_account_can_read_appearance_without_engine_spawn(self):
        self.session["allowed_scopes"] = []
        with patch("lan_bitable_template_portal.lighthouse_openclaw.LighthouseOpenClaw") as engine_cls:
            response = self.client.get("/api/assistant/appearance")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["data"], self.DEFAULT_APPEARANCE)
        self.assertEqual(self.session["allowed_scopes"], [])
        engine_cls.assert_not_called()

    def test_appearance_warm_does_not_block_and_does_not_build_engine(self):
        with patch("lan_bitable_template_portal.lighthouse_openclaw.LighthouseOpenClaw") as engine_cls:
            response = self.client.get("/api/assistant/appearance")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["data"], self.DEFAULT_APPEARANCE)
        engine_cls.assert_not_called()


# ---------------------------------------------------------------------------
# Real app lifespan: nonblocking startup, guest never queues, shutdown joins
# ---------------------------------------------------------------------------


class AppLifespanStartupTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        from fastapi import FastAPI
        from tools.lighthouse_test_backend import install_test_backend as install_lighthouse_routes
        from lan_bitable_template_portal.state_store import LanPortalStateStore

        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.store = LanPortalStateStore(db_path=Path(directory.name) / "test.sqlite3")
        self.engine = Mock()
        self.engine.manages_runtime = True
        self.engine.prepare = AsyncMock()
        self.engine.close = AsyncMock()
        self.engine.queue_warmup = Mock()
        self.stream = Mock()
        self.stream.engine = self.engine
        self.stream.close = AsyncMock(side_effect=self.engine.close)
        self.stream.workers = {}
        self.agent = Mock()
        self.agent.tasks = []
        self.service = Mock()
        self.service.model = SimpleNamespace(close=Mock())

        self.session = {"open_id": "user-a", "role": "building", "allowed_scopes": ["D"]}
        controller = SimpleNamespace(
            _current_session=lambda request: self.session,
            _request_base_url=lambda request: str(request.base_url).rstrip("/"),
        )
        runtime = SimpleNamespace(state_store=self.store, auth_manager=SimpleNamespace(
            session_scopes=lambda session: session["allowed_scopes"],
            is_admin=lambda session: session["role"] == "admin"))
        self.app = FastAPI()
        install_lighthouse_routes(self.app, controller, runtime)

        @self.app.get("/api/health")
        async def _health_probe():
            # Connectivity-style probe: must answer immediately, never waiting for
            # OpenClaw preparation to finish.
            return {"ok": True, "service": "lighthouse-startup-test"}

    def _engine_patches(self):
        return (
            patch("openclaw_service.assistant.routes.LighthouseAssistant",
                  return_value=self.service),
            patch("lan_bitable_template_portal.lighthouse_agent.PortalAgent",
                  return_value=self.agent),
            patch("lan_bitable_template_portal.lighthouse_api.PortalAPICatalog",
                  return_value=Mock()),
            patch("lan_bitable_template_portal.lighthouse_files.LighthouseFiles",
                  return_value=Mock()),
            patch("lan_bitable_template_portal.lighthouse_stream.LighthouseStream",
                  return_value=self.stream),
            patch("lan_bitable_template_portal.lighthouse_openclaw.LighthouseOpenClaw",
                  return_value=self.engine),
        )

    async def _wait_for_prepare(self):
        for _ in range(50):
            if self.engine.prepare.await_count:
                return
            await asyncio.sleep(0.001)

    async def test_startup_nonblocking_prepares_without_question_and_shutdown_joins(self):
        with contextlib.ExitStack() as stack:
            for patcher in self._engine_patches():
                stack.enter_context(patcher)
            async with self.app.router.lifespan_context(self.app):
                await self._wait_for_prepare()
                self.engine.prepare.assert_awaited_once()
                # Startup only preps: no warmup/question is queued by the portal.
                self.engine.queue_warmup.assert_not_called()
                transport = httpx.ASGITransport(app=self.app)
                async with httpx.AsyncClient(transport=transport,
                                             base_url="http://testserver") as client:
                    response = await client.get("/api/assistant/appearance")
                    self.assertEqual(response.status_code, 200, response.text)
                # An authenticated appearance queued a warmup for exactly this actor.
                self.engine.queue_warmup.assert_called_once()
                actor = self.engine.queue_warmup.call_args.args[0]
                self.assertEqual(actor["id"], "user-a")
        # Shutdown joined engine.close() and stream.close() before exiting the app.
        self.engine.close.assert_awaited_once()
        self.stream.close.assert_awaited_once()

    async def test_guest_request_never_queues_warmup(self):
        self.session["is_guest"] = True
        with contextlib.ExitStack() as stack:
            for patcher in self._engine_patches():
                stack.enter_context(patcher)
            async with self.app.router.lifespan_context(self.app):
                await self._wait_for_prepare()
                self.engine.prepare.assert_awaited_once()
                transport = httpx.ASGITransport(app=self.app)
                async with httpx.AsyncClient(transport=transport,
                                             base_url="http://testserver") as client:
                    response = await client.get("/api/assistant/appearance")
                    self.assertEqual(response.status_code, 403, response.text)
                # A guest is rejected before warm_authenticated; never queues.
                self.engine.queue_warmup.assert_not_called()
        self.engine.close.assert_awaited_once()

    async def test_framework_preparation_limits_warmups_not_login_accounts(self):
        entered, release = asyncio.Event(), asyncio.Event()
        original = asyncio.to_thread
        async def delayed(function, *args, **kwargs):
            if getattr(function, '__name__', '') == 'get_streams':
                entered.set()
                await release.wait()
            return await original(function, *args, **kwargs)
        with contextlib.ExitStack() as stack:
            for patcher in self._engine_patches():
                stack.enter_context(patcher)
            stack.enter_context(patch('asyncio.to_thread', new=delayed))
            async with self.app.router.lifespan_context(self.app):
                await asyncio.wait_for(entered.wait(), 2)
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app), base_url='http://testserver') as client:
                    for index in range(21):
                        self.session['open_id'] = 'queued-' + str(index)
                        response = await client.get('/api/assistant/appearance')
                        self.assertEqual(response.status_code, 200)
                    self.session.update(open_id='queued-0', allowed_scopes=['A'])
                    self.assertEqual((await client.get('/api/assistant/appearance')).status_code, 200)
                self.engine.queue_warmup.assert_not_called()
                release.set()
                await self._wait_for_prepare()
                actors = [call.args[0] for call in self.engine.queue_warmup.call_args_list]
                self.assertEqual({actor['id'] for actor in actors}, {'queued-0', 'queued-1'})
                self.assertEqual(next(actor['scopes'] for actor in actors if actor['id'] == 'queued-0'), ['A'])

    async def test_lifespan_enters_immediately_and_routes_respond_while_prepare_blocked(self):
        # engine.prepare stays blocked forever on a never-set gate; the lifespan
        # must still start and both health + authenticated appearance must answer
        # on the same event loop without ever waiting for OpenClaw.
        prepare_started = asyncio.Event()
        prepare_cancelled = asyncio.Event()
        prep_task = {}
        gate = asyncio.Event()

        async def blocked_prepare():
            prep_task["task"] = asyncio.current_task()
            prepare_started.set()
            try:
                await gate.wait()
            except asyncio.CancelledError:
                # shutdown must cancel the in-flight prepare instead of leaking it
                prepare_cancelled.set()
                raise

        self.engine.prepare = blocked_prepare

        with contextlib.ExitStack() as stack:
            for patcher in self._engine_patches():
                stack.enter_context(patcher)
            transport = httpx.ASGITransport(app=self.app)
            async with httpx.AsyncClient(transport=transport,
                                         base_url="http://testserver") as client:
                async with asyncio.timeout(5), self.app.router.lifespan_context(self.app):
                    # Startup returned immediately and reached the blocking gate.
                    await asyncio.wait_for(prepare_started.wait(), 2)
                    background = {t for t in asyncio.all_tasks()
                                  if t is not asyncio.current_task()}
                    self.assertTrue(prep_task["task"] in background)
                    self.assertFalse(prep_task["task"].done())

                    health = await asyncio.wait_for(client.get("/api/health"), 2)
                    self.assertEqual(health.status_code, 200, health.text)
                    appearance = await asyncio.wait_for(
                        client.get("/api/assistant/appearance"), 2)
                    self.assertEqual(appearance.status_code, 200, appearance.text)
                    self.engine.queue_warmup.assert_called_once()
                    self.assertFalse(prep_task["task"].done())

        # Shutdown cancelled and joined the in-flight preparation; nothing leaked.
        self.assertTrue(prepare_cancelled.is_set(),
                        "shutdown must cancel the blocked engine.prepare")
        self.assertTrue(prep_task["task"].done(),
                        "preparation task must finish during shutdown")
        for task in background:
            self.assertTrue(task.done(), f"leaked background task: {task}")

    async def test_lifespan_routes_respond_and_shutdown_joins_when_prepare_raises(self):
        prepare_started = asyncio.Event()
        prep_task = {}

        async def failing_prepare():
            prep_task["task"] = asyncio.current_task()
            prepare_started.set()
            raise AssistantError("fixture prepare failure", 503)

        self.engine.prepare = failing_prepare

        with contextlib.ExitStack() as stack:
            for patcher in self._engine_patches():
                stack.enter_context(patcher)
            transport = httpx.ASGITransport(app=self.app)
            async with httpx.AsyncClient(transport=transport,
                                         base_url="http://testserver") as client:
                async with asyncio.timeout(5), self.app.router.lifespan_context(self.app):
                    await asyncio.wait_for(prepare_started.wait(), 2)
                    background = {t for t in asyncio.all_tasks()
                                  if t is not asyncio.current_task()}

                    health = await asyncio.wait_for(client.get("/api/health"), 2)
                    self.assertEqual(health.status_code, 200, health.text)
                    appearance = await asyncio.wait_for(
                        client.get("/api/assistant/appearance"), 2)
                    self.assertEqual(appearance.status_code, 200, appearance.text)
                    self.engine.queue_warmup.assert_called_once()

        task = prep_task["task"]
        self.assertTrue(task.done(), "preparation task must not leak")
        self.assertIsNone(task.exception(),
                          "prepare_runtime must swallow AssistantError at startup")
        for candidate in background:
            self.assertTrue(candidate.done(), f"leaked background task: {candidate}")


if __name__ == "__main__":
    unittest.main()
