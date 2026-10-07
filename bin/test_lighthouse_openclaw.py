"""Isolated unit tests for the OpenClaw-backed BusinessBridge, OpenClawToolAgent
token/session binding, and the LighthouseModel agent_factory hook.

All tests are synthetic: they use fake GatewayClient / OpenClawRuntime processes
and never contact a real model, Node runtime, or cloud service.  The agent_factory
test reuses a real PortalAPICatalog (realoriginalcatalog) so scope / read-only /
confirmation semantics are verified against the actual route descriptors.

Abort targets the exact account session and run. If its acknowledgement is
lost, only that account is revoked; the shared process must remain available.
Shared process startup and shutdown are covered in test_lighthouse_runtime.
"""

from __future__ import annotations

import asyncio
import copy
import os
import sys
import tempfile
import unittest
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from fastapi import FastAPI, Request
from pydantic_ai import Tool
from pydantic_ai.exceptions import ModelRetry

from lan_bitable_template_portal.lighthouse_ai import AssistantError, LighthouseAssistant
from lan_bitable_template_portal.lighthouse_agent import PortalAgent
from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog
from lan_bitable_template_portal.lighthouse_files import LighthouseFiles
from lan_bitable_template_portal.lighthouse_gateway import GatewayError
from lan_bitable_template_portal.lighthouse_model import LighthouseModel
from lan_bitable_template_portal.lighthouse_openclaw import BusinessBridge, LighthouseOpenClaw, OpenClawToolAgent
from lan_bitable_template_portal.lighthouse_queries import PortalOperation

ACTOR_D = {"id": "fixture-d", "scopes": ["D"], "is_admin": False}


class DefaultEngineTests(unittest.TestCase):
    def test_portal_defaults_to_native_and_explicit_legacy_remains_available(self):
        from fastapi.testclient import TestClient
        from tools.lighthouse_test_backend import install_test_backend as install_lighthouse_routes
        for mode in (None, "legacy"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                store = Store(Path(directory) / "fixture.sqlite3")
                session = {"user": {"open_id": "fixture-d"}, "allowed_scopes": ["D"], "role": "building"}
                controller = SimpleNamespace(_current_session=lambda _: session,
                    _request_base_url=lambda request: str(request.base_url).rstrip("/"), bound_port=19004, preferred_port=19004)
                runtime = SimpleNamespace(state_store=store, auth_manager=SimpleNamespace(
                    session_scopes=lambda value: value["allowed_scopes"], is_admin=lambda _: False))
                app = FastAPI()
                engine = Mock(max_parallel=2, close=AsyncMock())
                with patch.dict(os.environ), patch("lan_bitable_template_portal.lighthouse_openclaw.LighthouseOpenClaw", return_value=engine) as native:
                    os.environ.pop("LIGHTHOUSE_AGENT_ENGINE", None)
                    if mode:
                        os.environ["LIGHTHOUSE_AGENT_ENGINE"] = mode
                    install_lighthouse_routes(app, controller, runtime)
                    with TestClient(app) as client:
                        self.assertEqual(client.get("/api/assistant/conversation").status_code, 200)
                    if mode is None:
                        native.assert_called_once()
                        self.assertEqual(native.call_args.kwargs["bridge_url"](), "http://127.0.0.1:19004/api/assistant/openclaw-tools")
                        engine.close.assert_awaited_once()
                    else:
                        native.assert_not_called()


class Store:
    def __init__(self, path):
        self.db_path, self.docs = path, {}

    def get_document(self, namespace, key):
        return copy.deepcopy(self.docs.get((namespace, key)))

    def put_document(self, namespace, key, value):
        self.docs[namespace, key] = copy.deepcopy(value)

    def list_documents(self, namespace, *, key_prefix=""):
        return [{"key": key, "payload": copy.deepcopy(value)}
                for (space, key), value in self.docs.items()
                if space == namespace and key.startswith(key_prefix)]


# ---------------------------------------------------------------------------
# BusinessBridge helpers
# ---------------------------------------------------------------------------

class FakeActive:
    """Minimal stand-in for an OpenClawToolAgent that BusinessBridge.call reads."""

    def __init__(self, session_key, tools, *, running=True, results=None):
        self.running = running
        self.session_key = session_key
        self.tools = tools
        self.calls = 0
        self.results = results if results is not None else {}
        self.call_lock = asyncio.Lock()
        self.events = asyncio.Queue()


def _tool(async_fn, *, timeout=None):
    return Tool(async_fn, timeout=timeout)


def _bridge_active(bridge, token, active):
    bridge.active[token] = active
    return active


async def _async_ok(scope: str):
    return {"ok": True, "scope": scope}


class BusinessBridgeTests(unittest.IsolatedAsyncioTestCase):
    async def test_shared_gateway_routes_only_trusted_agent_session_and_run(self):
        from lan_bitable_template_portal.lighthouse_runtime import agent_id
        bridge = BusinessBridge()
        manager = SimpleNamespace(bridge_token='shared-private-fixture')
        for owner in ('first', 'second'):
            active = FakeActive('agent:' + agent_id(owner) + ':conversation', {'lighthouse_probe': _tool(_async_ok)})
            active.actor, active.token, active.run_id, active.item = {'id': owner}, 'private-' + owner, 'run-' + owner, {}
            bridge.active[active.token] = active
        first = bridge.active['private-first']
        payload = {'agent_id': agent_id('first'), 'session_key': first.session_key, 'run_id': first.run_id,
                   'tool': 'lighthouse_probe', 'call_id': 'call-1', 'params': {'scope': 'A'}}
        result = await bridge.call_shared(manager.bridge_token, payload, manager)
        self.assertEqual(result['scope'], 'A')
        for invalid in ({**payload, 'agent_id': agent_id('second')},
                        {**payload, 'session_key': bridge.active['private-second'].session_key},
                        {**payload, 'run_id': 'run-second'}):
            with self.assertRaises(AssistantError):
                await bridge.call_shared(manager.bridge_token, invalid, manager)
        with self.assertRaises(AssistantError):
            await bridge.call_shared('invalid', payload, manager)
        self.assertEqual(bridge.active['private-second'].calls, 0)
        first.item['blocked'] = True
        with self.assertRaises(AssistantError):
            await bridge.call_shared(manager.bridge_token, payload, manager)

    async def test_token_binds_live_callback_and_session_key(self):
        bridge = BusinessBridge()
        valid_token = "tok-1"
        active = FakeActive("session-1", {"lighthouse_probe": _tool(_async_ok, timeout=5)})
        _bridge_active(bridge, valid_token, active)

        # Correct session key is accepted.
        result = await bridge.call(valid_token, {
            "session_key": "session-1", "tool": "lighthouse_probe", "call_id": "c1",
            "params": {"scope": "A"},
        })
        self.assertEqual(result, {"ok": True, "scope": "A"})

        # Unknown token is rejected even if a matching active existed earlier.
        with self.assertRaises(AssistantError) as err:
            await bridge.call("missing-token", {
                "session_key": "session-1", "tool": "lighthouse_probe", "call_id": "c2",
                "params": {"scope": "A"},
            })
        self.assertEqual(err.exception.status, 403)

        # Right token but wrong session key is rejected.
        with self.assertRaises(AssistantError) as err:
            await bridge.call(valid_token, {
                "session_key": "session-OTHER", "tool": "lighthouse_probe", "call_id": "c3",
                "params": {"scope": "A"},
            })
        self.assertEqual(err.exception.status, 403)

    async def test_stale_callback_removal_rejects_token(self):
        bridge = BusinessBridge()
        token = "tok-stale"
        active = FakeActive("session-1", {"lighthouse_probe": _tool(_async_ok, timeout=5)})
        _bridge_active(bridge, token, active)
        await bridge.call(token, {"session_key": "session-1", "tool": "lighthouse_probe",
                                  "call_id": "s1", "params": {"scope": "A"}})

        # The token->callback registration is removed (as run_stream_events finally does).
        bridge.active.pop(token, None)
        with self.assertRaises(AssistantError) as err:
            await bridge.call(token, {"session_key": "session-1", "tool": "lighthouse_probe",
                                      "call_id": "s2", "params": {"scope": "A"}})
        self.assertEqual(err.exception.status, 403)

        # A stopped callback (running False) is also treated as stale.
        stopped = FakeActive("session-1", {"lighthouse_probe": _tool(_async_ok, timeout=5)}, running=False)
        bridge.active["tok-stopped"] = stopped
        with self.assertRaises(AssistantError) as err:
            await bridge.call("tok-stopped", {"session_key": "session-1", "tool": "lighthouse_probe",
                                              "call_id": "s3", "params": {"scope": "A"}})
        self.assertEqual(err.exception.status, 403)

    async def test_named_tool_json_schema_validation_and_unregistered_name(self):
        bridge = BusinessBridge()
        tool = _tool(_async_ok, timeout=5)
        active = FakeActive("session-1", {"lighthouse_probe": tool})
        _bridge_active(bridge, "tok-json", active)

        # An unregistered tool name is refused.
        with self.assertRaises(AssistantError) as err:
            await bridge.call("tok-json", {"session_key": "session-1", "tool": "lighthouse_unknown",
                                           "call_id": "j0", "params": {"scope": "A"}})
        self.assertEqual(err.exception.status, 403)

        # Params that fail the named-tool JSON schema become a sanitized result.
        invalid = await bridge.call("tok-json", {"session_key": "session-1", "tool": "lighthouse_probe",
                                                 "call_id": "j1", "params": {"unexpected_key": 1}})
        self.assertIs(invalid["ok"], False)
        self.assertIn("工具参数或响应无效", invalid["error"])

    async def test_call_id_replay_with_changed_args_conflict(self):
        bridge = BusinessBridge()
        active = FakeActive("session-1", {"lighthouse_probe": _tool(_async_ok, timeout=5)})
        _bridge_active(bridge, "tok-replay", active)

        first = await bridge.call("tok-replay", {"session_key": "session-1", "tool": "lighthouse_probe",
                                                 "call_id": "r1", "params": {"scope": "A"}})
        self.assertEqual(first["scope"], "A")

        # Same call id with identical args returns the cached result.
        replay_same = await bridge.call("tok-replay", {"session_key": "session-1", "tool": "lighthouse_probe",
                                                       "call_id": "r1", "params": {"scope": "A"}})
        self.assertEqual(replay_same, first)

        # Same call id with different args is a 409 replay conflict.
        with self.assertRaises(AssistantError) as err:
            await bridge.call("tok-replay", {"session_key": "session-1", "tool": "lighthouse_probe",
                                             "call_id": "r1", "params": {"scope": "B"}})
        self.assertEqual(err.exception.status, 409)

    async def test_effective_timeout_is_never_zero(self):
        bridge = BusinessBridge()
        # timeout=0 is falsy; the bridge must fall back to the default 45 and
        # never pass a zero/absent timeout to asyncio.wait_for.  (pydantic_ai
        # rejects Tool(timeout=0) at construction, so we simulate the malformed
        # config by overriding the attribute directly.)
        tool = _tool(_async_ok, timeout=None)
        tool.timeout = 0
        active = FakeActive("session-1", {"lighthouse_probe": tool})
        _bridge_active(bridge, "tok-zero", active)
        result = await bridge.call("tok-zero", {"session_key": "session-1", "tool": "lighthouse_probe",
                                                "call_id": "t1", "params": {"scope": "A"}})
        self.assertEqual(result["scope"], "A")

        # A non-zero small timeout is honored: a slow tool is cut with a sanitized time-out.
        async def slow(scope: str):
            await asyncio.sleep(1)
            return {"ok": True}

        bridge.active["tok-slow"] = FakeActive("session-1", {"lighthouse_probe": _tool(slow, timeout=0.05)})
        timeout_result = await bridge.call("tok-slow", {"session_key": "session-1", "tool": "lighthouse_probe",
                                                        "call_id": "t2", "params": {"scope": "A"}})
        self.assertIs(timeout_result["ok"], False)
        self.assertIn("超时", timeout_result["error"])

    async def test_unexpected_failure_is_sanitized(self):
        bridge = BusinessBridge()
        secret = "SECRET-MODEL-KEY-must-not-leak"

        async def explode(scope: str):
            raise RuntimeError(secret)

        active = FakeActive("session-1", {"lighthouse_probe": _tool(explode, timeout=5)})
        _bridge_active(bridge, "tok-fail", active)
        result = await bridge.call("tok-fail", {"session_key": "session-1", "tool": "lighthouse_probe",
                                                "call_id": "f1", "params": {"scope": "A"}})
        self.assertIs(result["ok"], False)
        self.assertNotIn(secret, result["error"])
        self.assertIn("工具参数或响应无效", result["error"])


# ---------------------------------------------------------------------------
# OpenClawToolAgent token / session binding
# ---------------------------------------------------------------------------

def _fake_engine():
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
    return engine


class OpenClawTokenTests(unittest.TestCase):
    def test_per_account_tokens_are_stable_and_distinct_per_account(self):
        engine = _fake_engine()
        a1 = OpenClawToolAgent(engine, {"id": "u1", "scopes": ["A"]}, {}, None, {}, instructions="x")
        a2 = OpenClawToolAgent(engine, {"id": "u1", "scopes": ["A"]}, {}, None, {}, instructions="x")
        b1 = OpenClawToolAgent(engine, {"id": "u2", "scopes": ["B"]}, {}, None, {}, instructions="x")
        self.assertEqual(a1.token, a2.token)
        self.assertNotEqual(a1.token, b1.token)

    def test_session_key_embeds_scope_fingerprint(self):
        engine = _fake_engine()
        same_scope_a = OpenClawToolAgent(engine, {"id": "u1", "scopes": ["A", "B"]}, {}, None, {}, instructions="x")
        same_scope_b = OpenClawToolAgent(engine, {"id": "u1", "scopes": ["B", "A"]}, {}, None, {}, instructions="x")
        narrower = OpenClawToolAgent(engine, {"id": "u1", "scopes": ["A"]}, {}, None, {}, instructions="x")
        self.assertEqual(same_scope_a.session_key, same_scope_b.session_key)
        self.assertNotEqual(same_scope_a.session_key, narrower.session_key)


# ---------------------------------------------------------------------------
# LighthouseOpenClaw lifecycle / stale callback removal
# ---------------------------------------------------------------------------

class FakeGatewayClient:
    """Scripted stand-in for the loopback GatewayClient.

    ``request`` records every RPC, reuses the exact accepted run id, and only
    ever answers ``chat.history`` / ``agent`` / ``agent.wait`` / ``chat.abort``.
    ``next_event`` pops queued frames or a queued exception; the single
    exception ``GatewayError`` (used to trigger ``_recover``) is raised instead
    of returned so the WS-disconnect recovery path can be exercised.

    New instances created by the agent factory take class-level defaults
    (``class_fail`` / ``class_wait`` / ``class_history`` / ``class_events``)
    so recovery/abort tests can script the second, freshly-authenticated
    connection independently.  ``close`` is always implemented so the agent's
    teardown path never touches the real transport.
    """

    instances: list = []
    class_fail = {}
    class_wait = None
    class_history = None
    class_events = []

    @classmethod
    def _reset(cls):
        cls.instances.clear()
        cls.class_fail = {}
        cls.class_wait = None
        cls.class_history = None
        cls.class_events = []

    def __init__(self, url, token, *, events=None, fail=None, wait=None, history=None, run_id="run-1"):
        self.url, self.token = url, token
        self.closed = False
        self.entered = False
        self.calls = []
        self.run_id = run_id
        self.events = list(events if events is not None else type(self).class_events)
        self.fail = dict(fail or {})
        self.fail.update(type(self).class_fail)
        self.wait = wait if wait is not None else type(self).class_wait
        self.history = (
            history
            if history is not None
            else (type(self).class_history if type(self).class_history is not None else {"messages": []})
        )
        type(self).instances.append(self)

    async def request(self, method, params=None, timeout=None):
        self.calls.append((method, params))
        exc = self.fail.get(method)
        if exc:
            raise exc() if isinstance(exc, type) and issubclass(exc, BaseException) else exc
        if method == "chat.history":
            return copy.deepcopy(self.history)
        if method == "sessions.patch":
            assert set(params) == {'key'}
            return {'key': params['key']}
        if method == "agent":
            previous = self.run_id
            self.run_id = params['idempotencyKey']
            for frame in self.events:
                if isinstance(frame, dict) and frame.get('payload', {}).get('runId') == previous:
                    frame['payload']['runId'] = self.run_id
            return {"runId": self.run_id}
        if method == "agent.wait":
            if self.wait is not None:
                return copy.deepcopy(self.wait)
            return {"status": "ok", "terminalReply": {"disposition": "visible", "text": "fixture reply"}}
        if method == "chat.abort":
            return {}
        raise AssertionError(f"unexpected request {method}")

    async def next_event(self, timeout):
        if self.events:
            step = self.events.pop(0)
            if isinstance(step, (GatewayError, OSError)):
                raise step
            return step
        return {
            "event": "agent",
            "payload": {"runId": self.run_id, "stream": "lifecycle", "data": {"phase": "end"}},
        }

    async def __aenter__(self):
        self.entered = True
        return self

    async def __aexit__(self, *exc):
        return False

    @property
    def connected(self):
        return self.entered and not self.closed

    def discard_events(self):
        self.events.clear()

    def select_run(self, run_id=None):
        pass

    async def close(self):
        self.closed = True


class OpenClawLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_prepare_overlaps_transport_and_runtime_without_inference(self):
        engine = _fake_engine()
        runtime_started, transport_started = asyncio.Event(), asyncio.Event()
        async def runtime():
            runtime_started.set()
            await transport_started.wait()
        async def transport():
            transport_started.set()
            await runtime_started.wait()
        engine.manager.prepare = AsyncMock(side_effect=runtime)
        engine.manager.model_client = AsyncMock(side_effect=transport)
        await asyncio.wait_for(LighthouseOpenClaw.prepare(engine), 1)
        engine.manager.prepare.assert_awaited_once()
        engine.manager.model_client.assert_awaited_once()
        engine.manager.acquire.assert_not_called()

    async def test_compaction_progress_binds_current_run_and_is_not_duplicated(self):
        for current in (False, True):
            with self.subTest(current=current):
                engine = _fake_engine()
                item = {'port': 19999, 'token': 'fixture-token'}
                emitted = []
                async def emit(kind, value): emitted.append((kind, value))
                agent = OpenClawToolAgent(engine, ACTOR_D, {'operation_id': 'compaction-test'}, emit, {}, instructions='fixture')
                agent._acquire_runtime = AsyncMock(return_value=item)
                client = FakeGatewayClient('ws://127.0.0.1:19999', 'fixture-token')
                await client.__aenter__()
                original_request = client.request
                async def request(method, params=None, timeout=None):
                    result = await original_request(method, params, timeout)
                    if method == 'agent':
                        item['compaction_run_id'] = client.run_id if current else 'previous-run'
                        if current:
                            client.events.append({'event': 'agent', 'payload': {'runId': client.run_id,
                                'stream': 'compaction', 'data': {'phase': 'start'}}})
                    return result
                client.request = request
                engine.gateway_client = AsyncMock(return_value=client)
                engine.keep_gateway = AsyncMock()
                async with agent.run_stream_events(['hello'], message_history=[]) as events:
                    results = [event async for event in events]
                self.assertEqual(results[-1].result.output, 'fixture reply')
                self.assertEqual(sum(value.get('label') == '正在自动整理上下文' for _, value in emitted), int(current))
                await client.close()

    async def test_consecutive_turns_reuse_socket_and_do_not_reannounce_connection(self):
        FakeGatewayClient._reset()
        engine = _fake_engine()
        engine.manager.root = Path(tempfile.gettempdir()) / 'lighthouse-openclaw-test'
        item = {'port': 19999, 'token': 'fixture-token', 'busy': False}
        engine.manager.acquire = AsyncMock(side_effect=lambda *_args, **_kwargs: dict(item))
        emitted = []
        async def emit(kind, value):
            emitted.append((kind, value))
        with patch('lan_bitable_template_portal.lighthouse_openclaw.GatewayClient', FakeGatewayClient):
            for number in range(2):
                agent = OpenClawToolAgent(engine, {'id': 'fixture-d', 'scopes': ['D']},
                    {'operation_id': 'op-' + str(number)}, emit, {'model': 'fixture-model'}, instructions='fixture')
                async with agent.run_stream_events(['hello'], message_history=[]) as events:
                    results = [event async for event in events]
                self.assertEqual(results[-1].result.output, 'fixture reply')
                self.assertFalse(engine.bridge.active)
        self.assertEqual(len(FakeGatewayClient.instances), 1)
        client = FakeGatewayClient.instances[0]
        self.assertFalse(client.closed)
        self.assertEqual(sum(method == 'agent' for method, _ in client.calls), 2)
        self.assertEqual(sum(value.get('label') == '正在连接助手' for _, value in emitted), 1)
        await client.close()

    async def test_gateway_rotation_disconnect_and_account_isolation(self):
        FakeGatewayClient._reset()
        engine = _fake_engine()
        a, b = {'id': 'fixture-a'}, {'id': 'fixture-b'}
        first_item = {'port': 19991, 'token': 'fixture-a'}
        with patch('lan_bitable_template_portal.lighthouse_openclaw.GatewayClient', FakeGatewayClient):
            first = await engine.gateway_client(a, first_item)
            self.assertIs(first, await engine.gateway_client(a, dict(first_item)))
            other = await engine.gateway_client(b, {'port': 19992, 'token': 'fixture-b'})
            self.assertIsNot(first, other)
            rotated = await engine.gateway_client(a, {'port': 19991, 'token': 'fixture-a-new'})
            self.assertTrue(first.closed)
            self.assertFalse(other.closed)
            await rotated.close()
            replacement = await engine.gateway_client(a, {'port': 19991, 'token': 'fixture-a-new'})
            self.assertIsNot(rotated, replacement)
            self.assertEqual(len(FakeGatewayClient.instances), 4)
            self.assertFalse(any(method == 'agent' for client in FakeGatewayClient.instances for method, _ in client.calls))
            await other.close(); await replacement.close()

    async def test_active_reader_cannot_be_replaced_by_a_second_connection(self):
        engine = _fake_engine()
        actor, item = {'id': 'fixture'}, {'port': 19999, 'token': 'fixture'}
        client = FakeGatewayClient('ws://127.0.0.1:19999', 'fixture')
        await client.__aenter__()
        await engine.keep_gateway(actor, item, client)
        with patch.object(client, 'discard_events', side_effect=RuntimeError('active reader')), patch('lan_bitable_template_portal.lighthouse_openclaw.GatewayClient') as factory:
            with self.assertRaises(AssistantError) as error:
                await engine.gateway_client(actor, item)
            self.assertEqual(error.exception.status, 409)
            factory.assert_not_called()
            self.assertFalse(client.closed)
        await client.close()

    async def test_run_stream_events_finally_removes_stale_callback(self):
        engine = _fake_engine()
        engine.bridge_url = lambda: "http://bridge"
        manager = Mock()
        manager.root = Path(tempfile.gettempdir()) / "lighthouse-openclaw-test"
        manager.acquire = AsyncMock(return_value={"port": 19999, "token": "gw-token", "busy": False, "used_at": 0.0})
        engine.manager = manager
        emitted = []

        async def _emit(kind, value):
            emitted.append((kind, value))

        with patch("lan_bitable_template_portal.lighthouse_openclaw.GatewayClient", FakeGatewayClient):
            agent = OpenClawToolAgent(engine, {"id": "u1", "scopes": ["A"]}, {"operation_id": "op-0001", "run_id": ""},
                                      _emit, {'model': 'fixture-model'}, instructions="x")
            token = agent.token
            self.assertNotIn(token, engine.bridge.active)

            async with agent.run_stream_events(["hello"], message_history=[]) as events:
                # Inside the run, the token->callback binding is live.
                self.assertIn(token, engine.bridge.active)
                async for _ in events:
                    break

        # The finally block removed the stale callback, so the token no longer works.
        self.assertNotIn(token, engine.bridge.active)
        with self.assertRaises(AssistantError) as err:
            await engine.bridge.call(token, {"session_key": agent.session_key, "tool": "lighthouse_probe",
                                             "call_id": "c1", "params": {}})
        self.assertEqual(err.exception.status, 403)

    async def test_close_drives_production_shutdown_and_calls_manager_close(self):
        # Drive the real LighthouseOpenClaw.close() shutdown path.  Construction
        # is kept light with a minimal portal stub; the manager is replaced by
        # an AsyncMock afterwards so we assert close() actually calls it.
        portal = Mock()
        portal.assistant = SimpleNamespace(_state=lambda actor: {"id": "conv-" + actor["id"]})
        store = Mock()
        store.db_path = str(Path(tempfile.gettempdir()) / "lighthouse-openclaw-test" / "state.sqlite3")
        portal.store = store
        engine = LighthouseOpenClaw(portal, bridge_url="http://bridge")
        manager = AsyncMock()
        engine.manager = manager

        agent = OpenClawToolAgent(engine, {"id": "u1", "scopes": ["A"]}, {}, None, {}, instructions="x")
        token = agent.token
        engine.bridge.active[token] = agent
        engine.bridge.active["stale-other"] = agent
        socket = FakeGatewayClient('ws://127.0.0.1:19999', 'fixture')
        await socket.__aenter__()
        engine.gateways['fixture'] = ({}, socket)

        await engine.close()

        # Both active callbacks were torn down through the real close() body.
        self.assertEqual(engine.bridge.active, {})
        self.assertTrue(socket.closed)
        self.assertFalse(engine.gateways)
        # Production close() reached the runtime manager cleanup.
        manager.close.assert_awaited_once()


# ---------------------------------------------------------------------------
# Focused recovery / abort tests (no process, no cloud; pure fakes + mocks)
# ---------------------------------------------------------------------------

async def _noop_emit(kind, value):
    pass


def _recovery_agent(engine=None, *, turn=None):
    engine = engine if engine is not None else _fake_engine()
    agent = OpenClawToolAgent(
        engine,
        {"id": "u-rec", "scopes": ["A"]},
        turn or {"operation_id": "op-rec", "run_id": ""},
        _noop_emit,
        {'model': 'fixture-model'},
        instructions="x",
    )
    agent.item = {'port': 19999, 'token': 'fixture-token'}
    return agent


class OpenClawRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        FakeGatewayClient._reset()

    async def test_native_model_failures_are_actionable_without_exposing_provider_details(self):
        from lan_bitable_template_portal.lighthouse_stream import failure_detail
        cases = (('Model provider authentication failed', 'model_auth'),
                 ('Model provider rate limit exceeded', 'model_busy'),
                 ('Model provider model not found', 'model_request'),
                 ('Model provider timed out', 'timeout'),
                 ('Model provider unavailable', 'model_connection'),
                 ('Context overflow', 'model_context'),
                 ('private-provider-detail only', 'model_protocol'))
        for marker, category in cases:
            with self.subTest(category=category):
                agent = _recovery_agent()
                fake = FakeGatewayClient('ws://127.0.0.1:19999', 'fixture-token', events=[
                    {'event': 'agent', 'payload': {'runId': 'run-1', 'stream': 'lifecycle',
                     'data': {'phase': 'error', 'error': marker + ': sk-test-secret-private private-provider-detail'}}}])
                agent.client = fake
                with self.assertRaises(AssistantError) as raised:
                    _ = [event async for event in agent._events(fake, ['hello'], [])]
                self.assertEqual(raised.exception.category, category)
                self.assertEqual(failure_detail(raised.exception)[0], category)
                self.assertNotIn('sk-test-secret-private', str(raised.exception))
                self.assertNotIn('private-provider-detail', str(raised.exception))
                self.assertEqual(sum(method == 'agent' for method, _ in fake.calls), 1)

    async def test_completed_acceptance_reads_original_reply_without_waiting_for_new_events(self):
        agent = _recovery_agent()
        fake = FakeGatewayClient('ws://127.0.0.1:19999', 'fixture-token',
            wait={'status': 'ok', 'terminalReply': {'disposition': 'visible', 'text': '原回答'}})
        original = fake.request
        async def request(method, params=None, timeout=None):
            value = await original(method, params, timeout)
            return {**value, 'status': 'ok'} if method == 'agent' else value
        fake.request = request
        fake.next_event = AsyncMock(side_effect=AssertionError('Completed replay must not wait for new events'))
        agent.client = fake
        events = [event async for event in agent._events(fake, ['hello'], [])]
        self.assertEqual(events[-1].result.output, '原回答')
        self.assertEqual(sum(method == 'agent' for method, _ in fake.calls), 1)
        self.assertEqual([params for method, params in fake.calls if method == 'agent.wait'],
                         [{'runId': fake.run_id, 'timeoutMs': 0}])
        fake.next_event.assert_not_called()

    async def test_failed_terminal_acceptance_does_not_restart_model_or_wait_for_events(self):
        for status in ('error', 'timeout'):
            with self.subTest(status=status):
                agent = _recovery_agent()
                fake = FakeGatewayClient('ws://127.0.0.1:19999', 'fixture-token', wait={'status': status})
                original = fake.request
                async def request(method, params=None, timeout=None):
                    value = await original(method, params, timeout)
                    return {**value, 'status': status} if method == 'agent' else value
                fake.request = request
                fake.next_event = AsyncMock(side_effect=AssertionError('Terminal replay must not wait for new events'))
                agent.client = fake
                with self.assertRaises(AssistantError):
                    _ = [event async for event in agent._events(fake, ['hello'], [])]
                self.assertEqual(sum(method == 'agent' for method, _ in fake.calls), 1)
                fake.next_event.assert_not_called()

    async def test_completed_acceptance_rejects_missing_or_wrong_run_reply_without_inference(self):
        for reply in ({'status': 'ok', 'terminalReply': {'disposition': 'suppressed'}},
                      {'status': 'ok', 'runId': 'another-run', 'terminalReply': {'disposition': 'visible', 'text': '其他回答'}},
                      {'status': 'in_progress'}):
            with self.subTest(reply=reply):
                agent = _recovery_agent()
                fake = FakeGatewayClient('ws://127.0.0.1:19999', 'fixture-token', wait=reply)
                original = fake.request
                async def request(method, params=None, timeout=None):
                    value = await original(method, params, timeout)
                    return {**value, 'status': 'ok'} if method == 'agent' else value
                fake.request = request
                fake.next_event = AsyncMock(side_effect=AssertionError('Completed replay must not wait for new events'))
                agent.client = fake
                with self.assertRaises(AssistantError):
                    _ = [event async for event in agent._events(fake, ['hello'], [])]
                self.assertEqual(sum(method == 'agent' for method, _ in fake.calls), 1)
                fake.next_event.assert_not_called()

    async def test_tool_commentary_and_final_are_separate_native_messages(self):
        agent = _recovery_agent(turn={"operation_id": "run-parts", "run_id": ""})
        final = "今日用水112吨。"
        fake = FakeGatewayClient("ws://127.0.0.1:19999", "gw-token", run_id="run-parts",
            history={"messages": [{"role": "user", "content": "prev"}]},
            wait={"status": "ok", "terminalReply": {"disposition": "visible", "text": final}},
            events=[
                {"event": "agent", "payload": {"runId": "run-parts", "seq": 1, "stream": "assistant",
                    "data": {"text": "先读取资料。", "phase": "commentary", "itemId": "assistant-1"}}},
                {"event": "agent", "payload": {"runId": "run-parts", "seq": 2, "stream": "assistant",
                    "data": {"text": final, "phase": "final", "itemId": "assistant-2"}}},
                {"event": "agent", "payload": {"runId": "run-parts", "seq": 3, "stream": "lifecycle", "data": {"phase": "end"}}},
            ])
        agent.client = fake
        events = [event async for event in agent._events(fake, ["用水多少"], [])]
        self.assertEqual(events[-1].result.output, final)
        self.assertNotIn("先读取资料", str(events))

    async def test_explicit_native_snapshot_replacement_does_not_concatenate_old_text(self):
        agent = _recovery_agent(turn={"operation_id": "run-replace", "run_id": ""})
        fake = FakeGatewayClient("ws://127.0.0.1:19999", "gw-token", run_id="run-replace",
            history={"messages": [{"role": "user", "content": "prev"}]},
            wait={"status": "ok", "terminalReply": {"disposition": "visible", "text": "最终回答"}},
            events=[
                {"event": "agent", "payload": {"runId": "run-replace", "seq": 1, "stream": "assistant",
                    "data": {"text": "初始片段", "itemId": "assistant-1"}}},
                {"event": "agent", "payload": {"runId": "run-replace", "seq": 2, "stream": "assistant",
                    "data": {"text": "最终回答", "replace": True, "itemId": "assistant-1"}}},
            ])
        agent.client = fake
        events = [event async for event in agent._events(fake, ["hello"], [])]
        self.assertEqual(events[-1].result.output, "最终回答")

    async def test_new_message_item_resets_text_even_without_phase_metadata(self):
        agent = _recovery_agent(turn={"operation_id": "run-items", "run_id": ""})
        fake = FakeGatewayClient("ws://127.0.0.1:19999", "gw-token", run_id="run-items",
            history={"messages": [{"role": "user", "content": "prev"}]},
            wait={"status": "ok", "terminalReply": {"disposition": "visible", "text": "最终回答"}},
            events=[
                {"event": "agent", "payload": {"runId": "run-items", "seq": 1, "stream": "assistant",
                    "data": {"text": "原片段", "itemId": "assistant-1"}}},
                {"event": "agent", "payload": {"runId": "run-items", "seq": 2, "stream": "assistant",
                    "data": {"text": "最终回答", "itemId": "assistant-2"}}},
            ])
        agent.client = fake
        events = [event async for event in agent._events(fake, ["hello"], [])]
        self.assertEqual(events[-1].result.output, "最终回答")

    async def test_late_commentary_phase_drops_an_early_unclassified_prefix(self):
        agent = _recovery_agent(turn={"operation_id": "run-phase", "run_id": ""})
        fake = FakeGatewayClient("ws://127.0.0.1:19999", "gw-token", run_id="run-phase",
            history={"messages": [{"role": "user", "content": "prev"}]},
            wait={"status": "ok", "terminalReply": {"disposition": "visible", "text": "最终回答"}},
            events=[
                {"event": "agent", "payload": {"runId": "run-phase", "seq": 1, "stream": "assistant",
                    "data": {"text": "先读", "itemId": "assistant-1"}}},
                {"event": "agent", "payload": {"runId": "run-phase", "seq": 2, "stream": "assistant",
                    "data": {"text": "先读取资料", "phase": "commentary", "itemId": "assistant-1"}}},
                {"event": "agent", "payload": {"runId": "run-phase", "seq": 3, "stream": "assistant",
                    "data": {"text": "最终回答", "phase": "final", "itemId": "assistant-1"}}},
            ])
        agent.client = fake
        events = [event async for event in agent._events(fake, ["hello"], [])]
        self.assertEqual(events[-1].result.output, "最终回答")

    async def test_same_item_unmarked_corruption_still_stops(self):
        agent = _recovery_agent(turn={"operation_id": "run-bad", "run_id": ""})
        fake = FakeGatewayClient("ws://127.0.0.1:19999", "gw-token", run_id="run-bad",
            history={"messages": [{"role": "user", "content": "prev"}]},
            events=[
                {"event": "agent", "payload": {"runId": "run-bad", "seq": 1, "stream": "assistant",
                    "data": {"text": "前缀", "itemId": "assistant-1"}}},
                {"event": "agent", "payload": {"runId": "run-bad", "seq": 2, "stream": "assistant",
                    "data": {"text": "损坏内容", "itemId": "assistant-1"}}},
            ])
        agent.client = fake
        with self.assertRaisesRegex(AssistantError, "内容不连续"):
            _ = [event async for event in agent._events(fake, ["hello"], [])]

    async def test_native_adapter_without_item_ids_relays_authoritative_snapshots(self):
        agent = _recovery_agent(turn={"operation_id": "run-adapter", "run_id": ""})
        fake = FakeGatewayClient("ws://127.0.0.1:19999", "gw-token", run_id="run-adapter",
            history={"messages": [{"role": "user", "content": "prev"}]},
            wait={"status": "ok", "terminalReply": {"disposition": "visible", "text": "最终回答"}},
            events=[
                {"event": "agent", "payload": {"runId": "run-adapter", "seq": 1, "stream": "assistant", "data": {"text": "查询提示"}}},
                {"event": "agent", "payload": {"runId": "run-adapter", "seq": 2, "stream": "assistant", "data": {"text": "最终回答"}}},
            ])
        agent.client = fake
        events = [event async for event in agent._events(fake, ["hello"], [])]
        self.assertEqual(events[-1].result.output, "最终回答")

    async def test_disconnect_recovers_owned_final_message_without_resubmitting(self):
        agent = _recovery_agent(turn={"operation_id": "run-terminal", "run_id": ""})
        agent.item, agent.item_url = {"token": "gw-token"}, "ws://127.0.0.1:19999"
        FakeGatewayClient.class_wait = {"status": "ok", "terminalReply": {"disposition": "visible", "text": "最终回答"}}
        fake = FakeGatewayClient(agent.item_url, "gw-token", run_id="run-terminal",
            history={"messages": [{"role": "user", "content": "prev"}]},
            events=[{"event": "agent", "payload": {"runId": "run-terminal", "seq": 1, "stream": "assistant",
                     "data": {"text": "暂时片段", "itemId": "assistant-1"}}}, OSError("disconnect")])
        agent.client = fake
        with patch("lan_bitable_template_portal.lighthouse_openclaw.GatewayClient", FakeGatewayClient):
            events = [event async for event in agent._events(fake, ["hello"], [])]
        self.assertEqual(events[-1].result.output, "最终回答")
        self.assertEqual(sum(method == "agent" for client in FakeGatewayClient.instances for method, _ in client.calls), 1)

    async def test_terminal_observer_timeout_recovers_original_run_without_resubmitting(self):
        agent = _recovery_agent(turn={'operation_id': 'run-observer-timeout', 'run_id': ''})
        agent.item, agent.item_url = {'token': 'gw-token'}, 'ws://127.0.0.1:19999'
        FakeGatewayClient.class_wait = {'runId': 'run-observer-timeout', 'status': 'ok',
            'terminalReply': {'disposition': 'visible', 'text': '原运行的最终回答'}}
        fake = FakeGatewayClient(agent.item_url, 'gw-token', run_id='run-observer-timeout',
            history={'messages': [{'role': 'user', 'content': 'prev'}]},
            events=[{'event': 'agent', 'payload': {'runId': 'run-observer-timeout', 'seq': 1,
                'stream': 'lifecycle', 'data': {'phase': 'end'}}}])
        original = fake.request
        async def request(method, params=None, timeout=None):
            value = await original(method, params, timeout)
            if method == 'agent.wait':
                raise asyncio.TimeoutError()
            if method == 'agent':
                FakeGatewayClient.class_wait['runId'] = value['runId']
            return value
        fake.request = request
        agent.client = fake
        with patch('lan_bitable_template_portal.lighthouse_openclaw.GatewayClient', FakeGatewayClient):
            events = [event async for event in agent._events(fake, ['hello'], [])]
        self.assertEqual(events[-1].result.output, '原运行的最终回答')
        self.assertEqual(sum(method == 'agent' for client in FakeGatewayClient.instances for method, _ in client.calls), 1)
        self.assertTrue(fake.closed)

    async def test_recovery_rejects_another_run_reply(self):
        agent = _recovery_agent()
        agent.item, agent.item_url, agent.run_id = {'token': 'gw-token'}, 'ws://127.0.0.1:19999', 'run-owned'
        agent.client = FakeGatewayClient(agent.item_url, 'gw-token')
        FakeGatewayClient.class_wait = {'runId': 'another-run', 'status': 'ok',
            'terminalReply': {'disposition': 'visible', 'text': '其他会话的内容'}}
        with patch('lan_bitable_template_portal.lighthouse_openclaw.GatewayClient', FakeGatewayClient):
            with self.assertRaises(AssistantError):
                await agent._recover('')
        self.assertEqual(sum(method == 'agent' for client in FakeGatewayClient.instances for method, _ in client.calls), 0)

    async def test_recover_authenticates_new_connection_and_queries_owned_run_without_resubmitting(self):
        agent = _recovery_agent()
        item = {"key": "k", "token": "gw-token", "port": 19999}
        agent.item = item
        agent.item_url = "ws://127.0.0.1:19999"
        agent.run_id = "run-owned"

        first = FakeGatewayClient(
            "ws://127.0.0.1:19999", "gw-token",
            wait={"status": "in_progress"},
            history={"inFlightRun": {"runId": "run-owned", "text": "partial-answer"}},
        )
        agent.client = first
        # The freshly-authenticated retry connection is created by _recover via
        # the patched factory, so it must inherit the same wait/history script.
        FakeGatewayClient.class_wait = {"status": "in_progress"}
        FakeGatewayClient.class_history = {"inFlightRun": {"runId": "run-owned", "text": "partial-answer"}}

        with patch("lan_bitable_template_portal.lighthouse_openclaw.GatewayClient", FakeGatewayClient):
            restored, terminal = await agent._recover("original-answer")

        self.assertEqual(FakeGatewayClient.class_wait, {"status": "in_progress"})
        self.assertEqual(FakeGatewayClient.class_history,
                         {"inFlightRun": {"runId": "run-owned", "text": "partial-answer"}})

        second = FakeGatewayClient.instances[-1]
        self.assertIsNot(second, first, "recovery must open a new connection")
        self.assertTrue(second.entered, "recovery must authenticate anew via __aenter__")
        self.assertTrue(first.closed, "old client must be closed first")
        # Exact owned run is observed; the agent is never re-submitted.
        methods = [m for m, _ in second.calls]
        self.assertIn("agent.wait", methods)
        self.assertIn("chat.history", methods)
        self.assertNotIn("agent", methods)
        wait_params = [p for m, p in second.calls if m == "agent.wait"][0]
        self.assertEqual(wait_params, {"runId": "run-owned", "timeoutMs": 0})
        hist_params = [p for m, p in second.calls if m == "chat.history"][0]
        self.assertEqual(hist_params["sessionKey"], agent.session_key)
        self.assertEqual(hist_params["limit"], 1)
        self.assertEqual(restored, "partial-answer")
        self.assertFalse(terminal, "inFlight recovery must not be terminal")

    async def test_recover_does_not_merge_wrong_run_history(self):
        agent = _recovery_agent()
        item = {"key": "k", "token": "gw-token", "port": 19999}
        agent.item = item
        agent.item_url = "ws://127.0.0.1:19999"
        agent.run_id = "run-owned"

        first = FakeGatewayClient(
            "ws://127.0.0.1:19999", "gw-token",
            wait={"status": "in_progress"},
            history={"inFlightRun": {"runId": "run-OTHER", "text": "someone-elses-answer"}},
        )
        agent.client = first
        FakeGatewayClient.class_wait = {"status": "in_progress"}
        FakeGatewayClient.class_history = {"inFlightRun": {"runId": "run-OTHER", "text": "someone-elses-answer"}}

        with patch("lan_bitable_template_portal.lighthouse_openclaw.GatewayClient", FakeGatewayClient):
            restored, terminal = await agent._recover("original-answer")
        self.assertEqual(restored, "original-answer")
        self.assertFalse(terminal)

    async def test_recover_terminal_ok_returns_terminal_reply(self):
        agent = _recovery_agent()
        item = {"key": "k", "token": "gw-token", "port": 19999}
        agent.item = item
        agent.item_url = "ws://127.0.0.1:19999"
        agent.run_id = "run-owned"

        first = FakeGatewayClient(
            "ws://127.0.0.1:19999", "gw-token",
            wait={"status": "ok", "terminalReply": {"disposition": "visible", "text": "finished"}},
        )
        agent.client = first
        FakeGatewayClient.class_wait = {"status": "ok", "terminalReply": {"disposition": "visible", "text": "finished"}}

        with patch("lan_bitable_template_portal.lighthouse_openclaw.GatewayClient", FakeGatewayClient):
            restored, terminal = await agent._recover("original-answer")
        self.assertEqual(restored, "finished")
        self.assertTrue(terminal)

    async def test_same_seq_frame_is_ignored(self):
        agent = _recovery_agent(turn={'operation_id': 'run-seq', 'run_id': ''})
        agent.validators = []
        run_id = "run-seq"
        fake = FakeGatewayClient(
            "ws://127.0.0.1:19999", "gw-token",
            run_id=run_id,
            history={"messages": [{"role": "user", "content": "prev"}]},
            wait={"status": "ok", "terminalReply": {"disposition": "visible", "text": "hello"}},
            events=[
                {"event": "agent", "payload": {"runId": run_id, "stream": "assistant", "data": {"delta": "hello"}, "seq": 1}},
                # Same seq must be ignored (duplicate relay frame).
                {"event": "agent", "payload": {"runId": run_id, "stream": "assistant", "data": {"delta": "world"}, "seq": 1}},
                {"event": "agent", "payload": {"runId": run_id, "stream": "lifecycle", "data": {"phase": "end"}, "seq": 2}},
            ],
        )
        agent.client = fake
        deltas = []
        final_output = None
        async for event in agent._events(fake, ["hello"], []):
            if event.event_kind == "part_delta":
                deltas.append(event.delta.content_delta)
            elif event.event_kind == "agent_run_result":
                final_output = event.result.output
        self.assertEqual(deltas, ["hello"], "duplicate seq frame must not be delivered as text")
        self.assertEqual(final_output, "hello", "duplicate relay frame must not merge into the answer")

    async def test_abort_sends_chat_abort_for_owned_run_when_first_connection_alive(self):
        engine = _fake_engine()
        agent = _recovery_agent(engine)
        item = {"key": "k", "token": "gw-token", "port": 19999}
        agent.item = item
        agent.item_url = "ws://127.0.0.1:19999"
        agent.run_id = "run-abort"
        first = FakeGatewayClient("ws://127.0.0.1:19999", "gw-token")
        agent.client = first
        engine.manager._stop = AsyncMock()

        await agent._abort()

        abort_calls = [p for m, p in first.calls if m == "chat.abort"]
        self.assertEqual(len(abort_calls), 1)
        self.assertEqual(abort_calls[0]["runId"], "run-abort")
        engine.manager._stop.assert_not_awaited()

    async def test_abort_revokes_only_owned_account_when_both_connections_fail(self):
        engine = _fake_engine()
        agent = _recovery_agent(engine)
        item = {"key": "k", "token": "gw-token", "port": 19999, "process": Mock()}
        agent.item = item
        agent.item_url = "ws://127.0.0.1:19999"
        agent.run_id = "run-abort-fail"
        engine.manager._stop = AsyncMock()

        FakeGatewayClient.class_fail = {"chat.abort": GatewayError("DISCONNECTED")}
        try:
            first = FakeGatewayClient("ws://127.0.0.1:19999", "gw-token")
            agent.client = first
            with patch("lan_bitable_template_portal.lighthouse_openclaw.GatewayClient", FakeGatewayClient):
                await agent._abort()
        finally:
            FakeGatewayClient.class_fail = {}

        # Runtime revokes this account without terminating the shared process.
        engine.manager._stop.assert_awaited_once_with(item)
        item['process'].terminate.assert_not_called()


# ---------------------------------------------------------------------------
# LighthouseModel.agent_factory hook preservation
# ---------------------------------------------------------------------------

class FakeAgent:
    """Minimal Pydantic-Agent-shaped fake that records registration and history."""

    def __init__(self, model, **kwargs):
        self.model = model
        self.kwargs = kwargs
        self.tools = {}
        self.tool_kwargs = {}
        self.validators = []
        self.message_history = None
        self.prompt = None
        self.ran = False

    def tool_plain(self, function=None, **kwargs):
        def register(fn):
            self.tools[fn.__name__] = fn
            self.tool_kwargs[fn.__name__] = kwargs
            return fn
        return register(function) if function else register

    def output_validator(self, function):
        self.validators.append(function)
        return function

    @asynccontextmanager
    async def run_stream_events(self, prompt, *, message_history=None, **_):
        self.prompt = prompt
        self.message_history = message_history
        self.ran = True

        async def _empty():
            if False:
                yield None
        yield _empty()


class AgentFactoryHookTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = Store(Path(self.tmp.name) / "state.sqlite3")
        self.model = Mock()
        self.model.settings.return_value = {"configured": True, "enabled": True, "active_model_id": "t", "models": [{"id": "t", "name": "测试", "model": "fixture", "configured": True}]}
        self.model.profile.return_value = {"id": "t", "name": "测试", "model": "fixture"}
        self.assistant = LighthouseAssistant(self.store, Mock(return_value=([], [])), model=self.model)
        self.app = FastAPI()
        self.reads, self.writes = [], []

        @self.app.get("/api/repair-management/records")
        async def records(scope: str = "ALL"):
            self.reads.append(scope)
            return {"ok": True, "data": {"scope": scope, "total": 1, "records": [{"record_id": "r1", "scope": scope}]}}

        @self.app.post("/api/fixture-write")
        async def fixture_write(request: Request):
            self.writes.append(await request.json())
            return {"ok": True, "saved": True}

        self.catalog = PortalAPICatalog(self.app)
        self.portal = PortalAgent(self.assistant, self.catalog, LighthouseFiles(self.store))
        self.actor = copy.deepcopy(ACTOR_D)
        self.request = Request({"type": "http", "http_version": "1.1", "method": "GET",
                                "scheme": "http", "server": ("testserver", 80),
                                "client": ("127.0.0.1", 1), "path": "/", "root_path": "",
                                "query_string": b"", "headers": []})
        self.emit_log = []

    async def authorize(self):
        return copy.deepcopy(self.actor)

    def _engine(self, factory):
        @asynccontextmanager
        async def lazy_model_factory(_custom, _profile):
            yield None

        engine = LighthouseModel(self.portal, model_factory=lazy_model_factory, agent_factory=factory)
        return engine

    async def _run_answer(self, factory, question, history):
        engine = self._engine(factory)
        profile = {"id": "fixture", "name": "Fixture", "model": "fixture"}
        turn = {"question": question, "operation_id": "op-0001", "file_ids": [], "_profile": profile}
        await engine.answer(self.actor, turn, history, self.request, self.emit, self.authorize, {})
        return engine

    async def emit(self, kind, value):
        self.emit_log.append((kind, value))

    async def test_agent_factory_preserves_scope_readonly_confirm_with_real_catalog(self):
        fake_agent = FakeAgent(None)
        factory_calls = []

        def factory(model, **kwargs):
            fake_agent.__init__(model, **kwargs)
            factory_calls.append(kwargs)
            return fake_agent

        history = [
            # Broader scoped history must not be imported into this narrower session.
            {"question": "broad 查询", "answer": "broad answer", "scopes": ["A", "D"], "sources": []},
            {"question": "narrow 查询", "answer": "narrow answer", "scopes": ["D"], "sources": []},
        ]
        engine = await self._run_answer(factory, "请查询D楼维修单数量并填写报告", history)

        # Hook wiring: factory received actor/turn/emit plus agent construction kwargs.
        self.assertTrue(engine is not None)
        self.assertTrue(fake_agent.ran)
        self.assertIn("actor", fake_agent.kwargs)
        self.assertIs(fake_agent.kwargs["actor"], self.actor)
        self.assertEqual(fake_agent.kwargs["name"], "lighthouse")
        self.assertEqual(fake_agent.kwargs["retries"], 1)
        self.assertEqual(fake_agent.kwargs["tool_timeout"], 45)

        # Real catalog is attached (realoriginalcatalog), not a mock.
        self.assertIsInstance(self.portal.catalog, PortalAPICatalog)

        # Broad-scope history is excluded from the imported model history.
        prior_text = self._flatten_history(fake_agent.message_history)
        self.assertNotIn("broad 查询", prior_text)
        self.assertIn("narrow 查询", prior_text)

        # Confirmation gate is preserved: with a write intent and no prepared plan,
        # the output validator insists on prepare_business instead of confirming.
        validator = fake_agent.validators[0]
        with self.assertRaises(ModelRetry):
            await validator("好的，已经处理完成。")

        # Scope is preserved: querying an out-of-scope building is refused by the
        # real catalog's scoped_operation path and reported as forbidden.
        query_tool = fake_agent.tools["query"]
        denied = await query_tool(PortalOperation(api_id="GET /api/repair-management/records",
                                                  params={"scope": "E"}))
        self.assertIs(denied["ok"], False)
        self.assertEqual(denied["query_state"], "forbidden")
        self.assertIn("本轮不能查询选择范围之外的楼栋", denied["error"])
        self.assertEqual(self.reads, [])  # the backend was never reached

        # Read-only gate is preserved: a write operation is refused before any
        # backend call happens (all real business writes still require the
        # prepare + confirm path and are not reachable from the read tool).
        write_denied = await query_tool(PortalOperation(api_id="POST /api/fixture-write", body={"x": 1}))
        self.assertIs(write_denied["ok"], False)
        self.assertEqual(write_denied["query_state"], "forbidden")
        self.assertIn("业务写入必须先准备并确认", write_denied["error"])
        self.assertEqual(self.writes, [])  # nothing actually written

    @staticmethod
    def _flatten_history(message_history):
        text = []
        for msg in message_history or []:
            for part in getattr(msg, "parts", []):
                content = getattr(part, "content", "")
                if isinstance(content, str):
                    text.append(content)
        return "\n".join(text)


if __name__ == "__main__":
    unittest.main()
