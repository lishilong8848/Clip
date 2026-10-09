"""Focused tests for the unlimited account-admission sentinel and schema-valid
OpenClaw ``maxConcurrent`` config.

These verify the regressions around removing the arbitrary 20-account cap:

* ``protocol.MAX_CONCURRENT_ACCOUNTS`` is the unlimited sentinel ``0`` and
  OpenClaw's config value (``OPENCLAW_MAX_CONCURRENT``) is a schema-valid
  positive integer, so the native ``agents.defaults.maxConcurrent`` is never 0.
* ``OpenClawRuntime`` maps the unlimited sentinel to the large schema-valid
  value and preserves explicit positive caps; busy-account checks are
  conditional on a positive maximum so an unlimited runtime never rejects.
* ``LighthouseStream`` never builds ``Semaphore(0)`` (which would deadlock); an
  unlimited ``max_parallel`` skips the global cap entirely while an explicit
  positive cap still yields a real semaphore.
* Preparatory warmup stays bounded independently (``WARMUP_CONCURRENCY``) of
  unlimited account admission, and an explicit positive injected maximum is
  still honored.

Everything is synthetic: temporary state roots, mocked portal/assistant/engine,
no Node, model, cloud or credentials.
"""

from __future__ import annotations

import asyncio
import copy
import sys
import tempfile
import threading
import unittest
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

BIN = Path(__file__).resolve().parent
if str(BIN) not in sys.path:
    sys.path.insert(0, str(BIN))
ROOT = BIN.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from openclaw_service.protocol import MAX_CONCURRENT_ACCOUNTS, OPENCLAW_MAX_CONCURRENT
from lan_bitable_template_portal import lighthouse_runtime as lrt
from lan_bitable_template_portal.lighthouse_ai import AssistantError
from lan_bitable_template_portal.lighthouse_openclaw import BusinessBridge, LighthouseOpenClaw, OpenClawToolAgent
from lan_bitable_template_portal.lighthouse_runtime import OpenClawRuntime, account_key, build_configuration
from lan_bitable_template_portal.lighthouse_stream import LighthouseStream
from test_lighthouse_runtime import _FakeProcess, _actor, _model, _profile


def _mem_store():
    class Store:
        def __init__(self):
            self.docs = {}

        def get_document(self, namespace, key):
            return copy.deepcopy(self.docs.get((namespace, key)))

        def put_document(self, namespace, key, value):
            self.docs[namespace, key] = copy.deepcopy(value)

    return Store()


class UnlimitedSentinelTests(unittest.TestCase):
    def test_protocol_sentinel_is_zero_and_openclaw_value_is_schema_valid(self):
        self.assertEqual(MAX_CONCURRENT_ACCOUNTS, 0)
        self.assertIsInstance(OPENCLAW_MAX_CONCURRENT, int)
        self.assertGreaterEqual(OPENCLAW_MAX_CONCURRENT, 1)
        self.assertNotEqual(OPENCLAW_MAX_CONCURRENT, 0)

    def test_build_configuration_writes_schema_valid_max_concurrent(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            key = account_key("fixture-account")
            profile = {
                "model": "fixture",
                "context_window": 64000,
                "vision_verified": False,
                "model": "fixture",
                "endpoint": "http://127.0.0.1:1/v1/chat/completions",
            }
            accounts = {
                key: {
                    "profile": profile,
                    "root": root,
                    "agent_id": "lh-" + key,
                    "config_fingerprint": "f" * 64,
                }
            }
            config = build_configuration(
                root, accounts, 18745,
                model_url="http://127.0.0.1:1/api/assistant/openclaw-models",
                plugin=root,
            )
            value = config["agents"]["defaults"]["maxConcurrent"]
            self.assertIsInstance(value, int)
            self.assertNotEqual(value, 0)
            self.assertEqual(value, OPENCLAW_MAX_CONCURRENT)


class RuntimeMappingTests(unittest.TestCase):
    def test_runtime_maps_positive_and_unlimited(self):
        with tempfile.TemporaryDirectory() as directory:
            limited = OpenClawRuntime(Path(directory) / "limited", max_accounts=2)
            self.assertEqual(limited.maximum, 2)
            self.assertEqual(limited._openclaw_max_concurrent, 2)

            unlimited = OpenClawRuntime(Path(directory) / "unlimited", max_accounts=MAX_CONCURRENT_ACCOUNTS)
            self.assertEqual(unlimited.maximum, 0)
            self.assertEqual(unlimited._openclaw_max_concurrent, OPENCLAW_MAX_CONCURRENT)
            # The unlimited mapping no longer imposes the old arbitrary 20-cap.
            self.assertGreater(unlimited._openclaw_max_concurrent, 20)


class StreamSlotsTests(unittest.TestCase):
    def _stream(self, max_parallel):
        portal = SimpleNamespace(assistant=SimpleNamespace(store=_mem_store()))
        engine = SimpleNamespace(max_parallel=max_parallel)
        return LighthouseStream(portal, engine=engine)

    def test_unlimited_max_parallel_skips_deadlocking_semaphore(self):
        stream = self._stream(0)
        self.assertIsNone(stream.slots)

        stream_neg = self._stream(-1)
        self.assertIsNone(stream_neg.slots)

    def test_positive_max_parallel_still_creates_bounded_semaphore(self):
        stream = self._stream(2)
        self.assertIsNotNone(stream.slots)
        self.assertEqual(stream.slots._value, 2)


class StreamUnlimitedExecutionTests(unittest.IsolatedAsyncioTestCase):
    """Directly drive _execute with a slots=None stream to prove no Semaphore(0)
    deadlock: the run still completes and the model is still asked."""

    class Engine:
        max_parallel = 0
        manages_context = False

        def __init__(self):
            self.calls = []

        async def answer(self, actor, turn, history, request, emit, authorize, context):
            self.calls.append(actor["id"])
            await emit("text", {"delta": "ok"})
            return {"answer": "ok", "sources": [], "files": []}

    def _portal(self, actor, operation_id, run_id):
        assistant = Mock()
        assistant._lock = threading.Lock()
        assistant.store = _mem_store()
        state = {
            "id": "state-" + actor["id"],
            "turns": [{
                "operation_id": operation_id,
                "run_id": run_id,
                "status": "pending",
                "question": "hi",
                "answer": "",
                "process": [],
                "scopes": actor["scopes"],
            }],
            "active_run_id": run_id,
            "phase": "queued",
            "revision": 0,
            "tokens": 0,
        }
        assistant._state = Mock(return_value=state)
        assistant._key = Mock(return_value="state-" + actor["id"])
        assistant._active = set()
        assistant._allowed = Mock(return_value=True)
        return SimpleNamespace(assistant=assistant)

    async def test_unlimited_execute_completes_without_semaphore(self):
        actor = {"id": "fixture-u", "scopes": ["D"]}
        operation_id = "op" + "x" * 22
        run_id = "a" * 31 + "b"
        engine = self.Engine()
        portal = self._portal(actor, operation_id, run_id)
        stream = LighthouseStream(portal, engine=engine)
        stream.workers[actor["id"]] = asyncio.current_task()
        run = {
            "id": run_id,
            "revision": 0,
            "operation_id": operation_id,
            "conversation_id": "state-" + actor["id"],
            "owner": actor["id"],
            "scopes": ["D"],
            "answer": "",
            "label": "",
            "process": [],
            "events": [],
            "status": "queued",
            "question": "hi",
            "_turn": {
                "question": "hi",
                "_profile": {
                    "id": "m", "model": "m", "name": "m",
                    "endpoint": "http://127.0.0.1:1/v1/chat/completions",
                    "key_cipher": "k",
                },
            },
            "_history": [],
            "_context": {},
        }
        async def _authorize():
            return copy.deepcopy(actor)
        request = SimpleNamespace()
        await stream._execute(actor, run, request, _authorize, None)
        self.assertIsNone(stream.slots)
        self.assertEqual(run["status"], "completed")
        self.assertEqual(engine.calls, [actor["id"]])


class WarmupBoundedTests(unittest.IsolatedAsyncioTestCase):
    async def test_queue_warmup_bounded_independently_when_unlimited(self):
        with tempfile.TemporaryDirectory() as directory:
            portal = Mock()
            portal.assistant = Mock()
            portal.assistant.store = Mock()
            portal.catalog = Mock()
            portal.files = Mock()
            engine = LighthouseOpenClaw(
                portal,
                bridge_url=lambda: "http://127.0.0.1:1/api/assistant/openclaw-tools",
                state_root=Path(directory) / "openclaw-state",
            )
            blocked1 = asyncio.create_task(asyncio.Event().wait())
            blocked2 = asyncio.create_task(asyncio.Event().wait())
            try:
                actor = {"id": "u-warm-3", "scopes": ["A"]}
                akey = account_key(actor["id"])

                # Unlimited sentinel (maximum == 0) still bounds queued warmups
                # to WARMUP_CONCURRENCY (2), so it is not an unbounded burst.
                engine.manager.maximum = 0
                engine.warming = {"k1": blocked1, "k2": blocked2}
                engine.queue_warmup(actor)
                self.assertNotIn(akey, engine.warming)
                self.assertNotIn(akey, engine.warm_attempts)

                # An explicit positive injected maximum is still honored.
                engine.manager.maximum = 2
                engine.warming = {"k1": blocked1, "k2": blocked2}
                engine.queue_warmup(actor)
                self.assertNotIn(akey, engine.warming)
                self.assertNotIn(akey, engine.warm_attempts)
            finally:
                blocked1.cancel()
                blocked2.cancel()
                await asyncio.gather(blocked1, blocked2, return_exceptions=True)
                await engine.close()


class UnlimitedSharedRuntimeAcceptanceTests(unittest.IsolatedAsyncioTestCase):
    """Mock-runtime acceptance proving the unlimited sentinel really removes the
    account-admission volume cap on one shared gateway, while per-account locks,
    explicit positive caps and account-model bridge tokens stay isolated.

    The mocked-process setup mirrors ``SharedRuntimeTests`` from
    ``test_lighthouse_runtime.py`` (reused by inspection, not modified): a fake
    node gateway process, scripted ``_ready``/``_configured`` and empty state
    roots keep everything synthetic. No cloud, Node, model or credentials.
    """

    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.runtime = OpenClawRuntime(Path(self.directory.name))
        self.process = _FakeProcess()
        self.mocks = ExitStack()
        self.mocks.enter_context(patch.object(self.runtime, '_ready', AsyncMock(side_effect=lambda item, *_: item)))
        self.configured = self.mocks.enter_context(patch.object(self.runtime, '_configured', AsyncMock()))
        self.mocks.enter_context(patch.object(lrt, 'runtime_files', return_value=(Path('node.exe'), Path('openclaw.mjs'))))
        self.popen = self.mocks.enter_context(patch.object(lrt.subprocess, 'Popen', return_value=self.process))
        self.mocks.enter_context(patch('upload_event_module.services.process_lifetime.register_child_process', return_value=True))
        self.model = _model()

    async def asyncTearDown(self):
        await self.runtime.close()
        self.mocks.close()
        self.directory.cleanup()

    async def acquire(self, identity='u-default', profile=None, scopes=('A',)):
        return await self.runtime.acquire(_actor(identity, scopes), self.model, profile or _profile())

    async def test_at_least_24_distinct_accounts_acquire_one_shared_gateway(self):
        items = await asyncio.gather(*(self.acquire('actor-' + str(i), {**_profile(), 'model': 'model-' + str(i)}) for i in range(24)))
        self.popen.assert_called_once()
        # One shared gateway: identical port, token and process for every account.
        for field in ('port', 'token', 'process'):
            self.assertEqual(len({item[field] for item in items}), 1)
        # Each distinct account keeps its own isolated identity/config root.
        for field in ('key', 'agent_id', 'root'):
            self.assertEqual(len({item[field] for item in items}), 24)
        # Unlimited sentinel keeps the runtime admission cap off (maximum == 0),
        # so all 24 can be busy on the same gateway without a volume rejection.
        self.assertEqual(self.runtime.maximum, 0)
        self.assertTrue(all(item['busy'] for item in items))

    async def test_same_busy_account_rejected_when_unlimited(self):
        first = await self.acquire('same-actor', {**_profile(), 'model': 'same-model'})
        self.assertTrue(first['busy'])
        # Unlimited admission cap does not weaken the per-account busy lock.
        with self.assertRaises(AssistantError) as error:
            await self.acquire('same-actor', {**_profile(), 'model': 'same-model'})
        self.assertEqual(error.exception.status, 409)

    async def test_positive_max_accounts_two_rejects_third(self):
        limited = OpenClawRuntime(Path(self.directory.name) / 'limited-2', max_accounts=2)
        self.mocks.enter_context(patch.object(limited, '_ready', AsyncMock(side_effect=lambda item, *_: item)))
        self.mocks.enter_context(patch.object(limited, '_configured', AsyncMock()))
        async def acquire_limited(identity):
            return await limited.acquire(_actor(identity), self.model, {**_profile(), 'model': 'model-' + identity})
        first = await acquire_limited('actor-1')
        second = await acquire_limited('actor-2')
        self.assertTrue(first['busy'])
        self.assertTrue(second['busy'])
        # The explicit positive cap is preserved even when an unlimited runtime is
        # also available, so a third distinct account is still rejected.
        with self.assertRaises(AssistantError) as error:
            await acquire_limited('actor-3')
        self.assertEqual(error.exception.status, 503)
        self.assertEqual(len(limited.accounts), 2)
        await limited.close()

    def test_account_model_bridge_tokens_are_isolated_per_account(self):
        engine = SimpleNamespace(
            assistant=SimpleNamespace(
                _state=lambda actor: {"id": "conv-" + actor["id"]},
                model_for=lambda actor: {"id": "fixture-model", "provider": "openai", "model": "gpt-4o-mini"},
            ),
            tokens={},
            bridge=BusinessBridge(),
            manager=Mock(),
            wait_warmup=AsyncMock(),
            bridge_url=lambda: "http://bridge",
            gateways={}, closing=False,
        )
        for name in ('gateway_ready', 'gateway_client', 'keep_gateway'):
            setattr(engine, name, getattr(LighthouseOpenClaw, name).__get__(engine))
        a1 = OpenClawToolAgent(engine, {"id": "u1", "scopes": ["A"]}, {}, None, {}, instructions="x")
        a2 = OpenClawToolAgent(engine, {"id": "u1", "scopes": ["A"]}, {}, None, {}, instructions="x")
        b1 = OpenClawToolAgent(engine, {"id": "u2", "scopes": ["B"]}, {}, None, {}, instructions="x")
        # Same account reuses one stable model-bridge token across runs...
        self.assertEqual(a1.token, a2.token)
        # ...but a distinct account sharing the gateway gets its own token, so the
        # account-model tool bridge stays isolated and cannot be impersonated.
        self.assertNotEqual(a1.token, b1.token)
        self.assertNotEqual(a2.token, b1.token)
        self.assertEqual(len(engine.tokens), 2)


if __name__ == "__main__":
    unittest.main()