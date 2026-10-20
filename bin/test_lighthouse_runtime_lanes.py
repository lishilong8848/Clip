"""Offline native lane isolation and enforced text-only streaming checks."""
import asyncio
import copy
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import httpx
from fastapi import FastAPI, Request
from openclaw_service.client import ResidentRuntime
from openclaw_service.protocol import ServiceError, process_stamp
from openclaw_service.server import Host, build_app
from openclaw_service.assistant import lighthouse_openclaw as native
from openclaw_service.assistant import lighthouse_runtime as runtime
from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_context import execution_key, execution_lane
import test_lighthouse_openclaw as agent_fixtures
import test_lighthouse_runtime as runtime_fixtures


def actor(lane=None, **extra):
    value = {'id': 'same-owner', 'scopes': ['A'], 'modules': ['repair']}
    if lane is not None:
        value['_execution_lane'] = lane
    return {**value, **extra}


def agent(engine, owner, profile=None):
    value = native.OpenClawToolAgent(engine, owner, {'operation_id': 'same-operation'},
        agent_fixtures._noop_emit, profile or runtime_fixtures._profile(), instructions='fixture')
    value.tools['lighthouse_probe'] = agent_fixtures._tool(agent_fixtures._async_ok)
    return value


class NativeRuntimeLaneTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.runtime = runtime.OpenClawRuntime(self.directory.name)
        self.process = runtime_fixtures._FakeProcess()
        self.enterContext(patch.object(self.runtime, 'prepare', AsyncMock(
            return_value=(Path('node.exe'), Path('openclaw.mjs')))))
        self.enterContext(patch.object(self.runtime, '_ready', AsyncMock(side_effect=lambda item, *_: item)))
        self.enterContext(patch.object(self.runtime, '_configured', AsyncMock()))
        self.popen = self.enterContext(patch.object(runtime.subprocess, 'Popen', return_value=self.process))
        self.enterContext(patch('upload_event_module.services.process_lifetime.register_child_process', return_value=True))
        self.engine = agent_fixtures._fake_engine()
        self.engine.manager = self.runtime
        self.model = Mock(unprotect=Mock(side_effect=lambda value: 'synthetic-key-' + value))
        self.engine.assistant.model_for = Mock(return_value=self.model)
        self.engine.assistant._state = Mock(side_effect=lambda owner: {'id': 'conversation-' + owner['id']})

    async def asyncTearDown(self):
        for _, client in self.engine.gateways.values():
            await client.close()
        await self.runtime.close()
        self.directory.cleanup()

    def resident(self):
        host = Host(state=self.directory.name, key='synthetic-control-key', manager=self.runtime)
        host.port = 7310
        host.lease = {'id': 'synthetic-lease', 'pid': os.getpid(), 'process_stamp': process_stamp(os.getpid()),
            'callback_url': 'http://127.0.0.1:7312/api/assistant/openclaw-tools'}
        resident = ResidentRuntime(self.directory.name, callback_url=lambda: host.lease['callback_url'], launch=lambda _: None)
        resident.prepare = AsyncMock()
        resident.descriptor, resident.instance = {'port': host.port}, host.instance
        resident.key, resident.lease, resident.disconnected = host.key, host.lease['id'], False
        resident.http = httpx.AsyncClient(transport=httpx.ASGITransport(app=build_app(host)))
        resident._request = AsyncMock(wraps=resident._request)
        self.enterContext(patch('openclaw_service.client.RemoteProcess', side_effect=lambda *_: self.process))
        self.enterContext(patch('openclaw_service.assistant.lighthouse_ai.unprotect_key', return_value='synthetic-model-key'))
        self.engine.manager = resident
        self.addAsyncCleanup(resident.close)
        return resident, host

    async def test_resident_same_actor_lanes_bind_independent_reservations_and_callbacks(self):
        resident, host = self.resident()
        owners = [actor('browser-a'), actor('browser-b'),
            actor('ignored-header', channel='feishu:chat', _feishu_topic={'id': 'topic-a'}),
            actor(channel='feishu:chat', _feishu_topic={'id': 'topic-b'})]
        agents = [agent(self.engine, owner, {**runtime_fixtures._profile(), 'id': 'fixture',
            'model': 'model-' + str(index)}) for index, owner in enumerate(owners)]
        original = copy.deepcopy(owners)
        with patch.object(self.runtime, 'acquire', wraps=self.runtime.acquire) as acquire:
            items = await asyncio.gather(*(value._acquire_runtime() for value in agents))
        self.assertEqual(len({item['key'] for item in items}), len(owners))
        self.assertEqual(len(host.reservations), len(owners))
        self.assertEqual(len(host.account_locks), len(owners))
        self.assertEqual(len({host.bridge_key(item['key']) for item in items}), len(owners))
        self.assertEqual(owners, original)
        for call in acquire.call_args_list:
            self.assertEqual(call.args[0]['id'], 'same-owner')
            self.assertEqual(set(call.args[0]), {'id', 'scopes', '_execution_lane'})
        wire = [call.args[1]['actor'] for call in resident._request.call_args_list if call.args[0] == 'acquire']
        self.assertCountEqual([owner['_execution_lane'] for owner in wire], [execution_lane(owner) for owner in owners])
        first, second = items[:2]
        with self.assertRaises(ServiceError) as caught:
            await resident.begin_run(first, agents[1].session_key, 'wrong-session', agents[0].token)
        self.assertEqual(caught.exception.code, 'invalid_session')
        payloads = []
        for index, (value, item) in enumerate(zip(agents, items)):
            value.running, value.run_id, value.item = True, 'run-' + str(index), item
            self.engine.bridge.active[value.token] = value
            await resident.begin_run(item, value.session_key, value.run_id, value.token)
            payloads.append({'agent_id': runtime.agent_id(execution_key(value.actor)),
                'session_key': value.session_key, 'run_id': value.run_id,
                'tool': 'lighthouse_probe', 'call_id': 'same-call', 'params': {'scope': 'A'}})
        await host.end_run({'lease': resident.lease, 'account': first['key'], 'run_id': 'wrong-run',
            'reservation': second['reservation'], 'final': True})
        self.assertIn(second['reservation'], host.reservations)
        self.assertTrue(self.runtime.accounts[second['key']]['busy'])
        portal = FastAPI()
        received = []
        @portal.post('/api/assistant/openclaw-tools')
        async def callback(request: Request):
            payload = await request.json()
            token = request.headers['authorization'].removeprefix('Bearer ')
            current = self.engine.bridge.active[token]
            received.append((current.actor['id'], execution_key(current.actor)))
            return await self.engine.bridge.call(token, payload)
        real_client = httpx.AsyncClient
        with patch.object(httpx, 'AsyncClient', side_effect=lambda **options:
                real_client(transport=httpx.ASGITransport(app=portal), **options)) as callback_client:
            for payload in payloads:
                self.assertTrue((await host.bridge(payload, self.runtime.bridge_token))['ok'])
            self.assertCountEqual(received, [('same-owner', execution_key(owner)) for owner in owners])
            calls = callback_client.call_count
            with self.assertRaises(ServiceError):
                await host.bridge({**payloads[0], 'agent_id': payloads[1]['agent_id']}, self.runtime.bridge_token)
            self.assertEqual(callback_client.call_count, calls)
            await resident._stop(first)
            await resident.release(first)
            self.assertIn(second['key'], host.runs)
            self.assertTrue(self.runtime.accounts[second['key']]['busy'])
            self.assertTrue((await host.bridge({**payloads[1], 'call_id': 'next-call'}, self.runtime.bridge_token))['ok'])
        for item in items[1:]:
            await resident.release(item)
        self.assertFalse(host.reservations)
        self.assertFalse(host.runs)
        self.popen.assert_called_once()

    async def test_resident_text_only_uses_full_catalog_and_blocks_both_dispatch_layers(self):
        resident, host = self.resident()
        profile = {**runtime_fixtures._profile(), 'id': 'fixture'}
        text = agent(self.engine, actor('text'), profile)
        text._live_text_only = True
        item = await text._acquire_runtime()
        native_item = self.runtime.accounts[item['key']]
        self.assertTrue(native_item['text_only'])
        plugin = native_item['root'] / 'plugin'
        self.assertEqual(json.loads((plugin / 'tools.json').read_text(encoding='utf-8')), [])
        catalog = json.loads((plugin.with_name('plugin-catalog') / 'tools.json').read_text(encoding='utf-8'))
        self.assertEqual([value['name'] for value in catalog], ['lighthouse_probe'])
        sent = next(call.args[1] for call in resident._request.call_args_list if call.args[0] == 'acquire')
        self.assertTrue(sent['text_only'])
        self.assertEqual(sent['definitions'], catalog)
        text.running, text.run_id, text.item = True, 'text-run', item
        self.engine.bridge.active[text.token] = text
        await resident.begin_run(item, text.session_key, text.run_id, text.token)
        payload = {'agent_id': native_item['agent_id'], 'session_key': text.session_key,
            'run_id': text.run_id, 'tool': 'lighthouse_probe', 'call_id': 'blocked', 'params': {'scope': 'A'}}
        with patch.object(httpx, 'AsyncClient') as callback:
            with self.assertRaises(ServiceError) as caught:
                await host.bridge(payload, self.runtime.bridge_token)
            self.assertEqual(caught.exception.status, 403)
            callback.assert_not_called()
        with self.assertRaises(AssistantError):
            await self.engine.bridge.call(text.token, payload)
        self.assertEqual(text.calls, 0)
        warm = agent(self.engine, actor('warm'), profile)
        warm._live_text_only = True
        with patch.object(native, 'GatewayClient', agent_fixtures.FakeGatewayClient):
            await warm.warmup()
        warm_item = self.runtime.accounts[runtime.account_key(execution_key(warm.actor))]
        self.assertFalse(warm_item['text_only'])
        self.assertFalse(warm_item['busy'])
        self.assertEqual(len(host.reservations), 1)
        business = agent(self.engine, actor('business'), profile)
        business_item = await business._acquire_runtime()
        self.assertFalse(self.runtime.accounts[business_item['key']]['text_only'])
        await resident.release(business_item)
        await resident.release(item)

    async def test_resident_legacy_identity_and_scope_payload_remain_unchanged(self):
        resident, host = self.resident()
        owner = actor(allowed_scopes=['D'], private_permission='must-not-cross-wire')
        original = copy.deepcopy(owner)
        profile = {**runtime_fixtures._profile(), 'id': 'fixture'}
        item = await resident.acquire(owner, None, profile,
            definitions=[{'name': 'lighthouse_probe', 'parameters': {'type': 'object'}}], warm_only=True)
        self.assertEqual(item['key'], runtime.account_key(owner['id']))
        self.assertEqual(self.runtime.accounts[item['key']]['agent_id'], runtime.agent_id(owner['id']))
        sent = next(call.args[1] for call in resident._request.call_args_list if call.args[0] == 'acquire')
        self.assertEqual(sent['actor'], {'id': 'same-owner', 'scopes': ['D']})
        self.assertFalse(sent['text_only'])
        self.assertEqual(owner, original)
        self.assertFalse(host.reservations)
        legacy = {key: value for key, value in sent.items() if key != 'text_only'}
        legacy['actor'].update(channel='feishu:untrusted', _feishu_topic={'id': 'untrusted'})
        reused = await host.acquire({**legacy, 'lease': resident.lease})
        self.assertEqual(reused['key'], item['key'])
        await host.acquire({**legacy, 'lease': resident.lease, 'text_only': True, 'warm_only': True})
        self.assertFalse(self.runtime.accounts[item['key']]['text_only'])

    async def test_resident_host_rejects_bad_lanes_and_flags_before_native_work(self):
        resident, host = self.resident()
        payload = {'lease': resident.lease, 'actor': actor(), 'request_id': 'fixture',
            'profile': {**runtime_fixtures._profile(), 'id': 'fixture'},
            'definitions': [{'name': 'lighthouse_probe', 'parameters': {'type': 'object'}}]}
        with patch.object(self.runtime, 'acquire', AsyncMock()) as acquire, \
                patch('openclaw_service.server.prepare_tool_plugin') as plugin:
            for lane in (None, True, 1, [], {}, '', '   ', 'x' * 513,
                    'a\n', 'a\t', 'a\x00', 'a\x7f', 'a\x85', 'a\u200b'):
                with self.subTest(lane=repr(lane)), self.assertRaises(ServiceError) as caught:
                    await host.acquire({**payload, 'actor': {**actor(), '_execution_lane': lane}})
                self.assertEqual(caught.exception.status, 400)
            for flag in (None, 'true', 1, []):
                with self.subTest(flag=flag), self.assertRaises(ServiceError) as caught:
                    await host.acquire({**payload, 'text_only': flag})
                self.assertEqual(caught.exception.status, 400)
            for definitions in ([], payload['definitions'] * 65):
                with self.assertRaises(ServiceError) as caught:
                    await host.acquire({**payload, 'definitions': definitions})
                self.assertEqual(caught.exception.status, 400)
            acquire.assert_not_awaited()
            plugin.assert_not_called()

    async def test_resident_agent_runs_overlap_and_release_independently(self):
        resident, host = self.resident()
        profile = {**runtime_fixtures._profile(), 'id': 'fixture'}
        agents = [agent(self.engine, actor(lane), profile) for lane in ('a', 'b')]
        agents[0]._live_text_only = True
        started, both_started = set(), asyncio.Event()
        async def connect(owner, item):
            client = agent_fixtures.FakeGatewayClient('ws://127.0.0.1:19999', item['token'])
            await client.__aenter__()
            original = client.request
            async def request(method, params=None, timeout=None):
                result = await original(method, params, timeout)
                if method == 'agent':
                    started.add(item['key'])
                    if len(started) == 2:
                        both_started.set()
                    await asyncio.wait_for(both_started.wait(), 2)
                return result
            client.request = request
            await self.engine.keep_gateway(owner, item, client)
            return client
        self.engine.gateway_client = connect
        async def run(value):
            async with value.run_stream_events(['hello'], message_history=[]) as events:
                return [event async for event in events]
        results = await asyncio.wait_for(asyncio.gather(*(run(value) for value in agents)), 5)
        self.assertEqual([events[-1].result.output for events in results], ['fixture reply'] * 2)
        self.assertEqual(len(started), 2)
        self.assertFalse(host.runs)
        self.assertFalse(host.reservations)
        self.assertFalse(self.engine.bridge.active)
        self.assertTrue(all(not item['busy'] for item in self.runtime.accounts.values()))
        self.assertEqual(len(self.engine.gateways), 2)
        self.assertTrue(all(client.connected for _, client in self.engine.gateways.values()))

    async def test_resident_real_typed_tool_catalog_warms_without_inference(self):
        resident, host = self.resident()
        fixture = agent_fixtures.AgentFactoryHookTests()
        await fixture.asyncSetUp()
        created = []
        profile = {**runtime_fixtures._profile(), 'id': 'fixture'}
        def factory(_profile, *, actor, turn, emit, **kwargs):
            value = native.OpenClawToolAgent(self.engine, actor, turn, emit, profile, **kwargs)
            created.append(value)
            return value
        engine = fixture._engine(factory)
        try:
            with patch.object(native, 'GatewayClient', agent_fixtures.FakeGatewayClient):
                await engine.answer(fixture.actor, {'question': '', 'operation_id': 'warm',
                    'file_ids': [], '_profile': profile}, [], fixture.request, fixture.emit,
                    fixture.authorize, {}, warm_only=True)
            self.assertEqual(len(created), 1)
            value = created[0]
            self.assertGreater(len(value.tools), 40)
            self.assertLessEqual(len(value.tools), 64)
            item = self.runtime.accounts[runtime.account_key(execution_key(fixture.actor))]
            self.assertFalse(item['text_only'])
            self.assertFalse(item['busy'])
            self.assertFalse(host.reservations)
            client = self.engine.gateways[item['key']][1]
            self.assertEqual([method for method, _ in client.calls], ['sessions.patch'])
        finally:
            await engine.close()
            fixture.doCleanups()

    async def test_same_actor_browser_feishu_topics_and_private_are_concurrent(self):
        owners = [actor('browser-a'), actor('browser-b'),
            actor(channel='feishu:chat-1', _feishu_topic={'id': 'topic-1'}),
            actor(channel='feishu:chat-1', _feishu_topic={'id': 'topic-2'}),
            actor(channel='feishu:private-1')]
        original = copy.deepcopy(owners)
        agents = [agent(self.engine, owner, {**runtime_fixtures._profile(),
            'model': 'model-' + str(index), 'key_cipher': str(index)}) for index, owner in enumerate(owners)]
        items = await asyncio.gather(*(value._acquire_runtime() for value in agents))
        self.popen.assert_called_once()
        for field in ('key', 'agent_id', 'root'):
            self.assertEqual(len({item[field] for item in items}), len(owners))
        self.assertEqual(len({value.token for value in agents}), len(owners))
        self.assertEqual(len({value.session_key for value in agents}), len(owners))
        self.assertEqual(len(self.runtime.locks), len(owners))
        for index, (owner, item) in enumerate(zip(owners, items)):
            key = runtime.account_key(execution_key(owner))
            self.assertIs(self.runtime.accounts[key], item)
            self.assertEqual(item['agent_id'], runtime.agent_id(execution_key(owner)))
            self.assertEqual(item['profile']['model'], 'model-' + str(index))
            self.assertEqual(item['model_key'], 'synthetic-key-' + str(index))
            self.assertTrue(item['busy'])
        self.assertEqual(owners, original)
        for callback in (self.engine.assistant.model_for, self.engine.assistant._state):
            self.assertTrue(callback.call_args_list)
            self.assertTrue(all(call.args[0]['id'] == 'same-owner' for call in callback.call_args_list))
        config = json.loads((self.runtime.gateway['root'] / 'openclaw.json').read_text(encoding='utf-8'))
        self.assertEqual(set(config['agents']['entries']), {item['agent_id'] for item in items})
        self.assertNotIn('synthetic-key-', json.dumps(config))

    async def test_same_lane_busy_cancel_and_explicit_cap_remain_bounded(self):
        self.runtime.maximum = 2
        owners = [actor('a'), actor('b')]
        first, second = await asyncio.gather(*(self.runtime.acquire(owner, self.model,
            runtime_fixtures._profile()) for owner in owners))
        for owner, status in ((owners[0], 409), (actor('c'), 503)):
            with self.assertRaises(AssistantError) as caught:
                await self.runtime.acquire(owner, self.model, runtime_fixtures._profile())
            self.assertEqual(caught.exception.status, status)
        async with self.runtime.locks[second['key']]:
            waiter = asyncio.create_task(self.runtime.acquire(owners[1], self.model, runtime_fixtures._profile()))
            await asyncio.sleep(0)
            waiter.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await waiter
        self.assertTrue(first['busy'])
        self.assertTrue(second['busy'])
        await self.runtime._stop(first)
        self.assertTrue(first['blocked'])
        self.assertTrue(second['busy'])
        self.assertFalse(second.get('blocked'))
        self.assertEqual(self.process.terminate_count, 0)

    async def test_model_proxy_and_profile_changes_stay_with_their_lane(self):
        owners = [actor('a'), actor('b')]
        profiles = [{**runtime_fixtures._profile(), 'model': 'model-' + lane,
            'key_cipher': lane, 'endpoint': 'https://' + lane + '.invalid/v1/chat/completions'} for lane in ('a', 'b')]
        items = await asyncio.gather(*(self.runtime.acquire(owner, self.model, profile)
            for owner, profile in zip(owners, profiles)))
        sent = []
        async def provider(request):
            sent.append((request.url.host, request.headers['authorization'], json.loads(request.content)['model']))
            return httpx.Response(200, json={'ok': True})
        self.runtime.http = httpx.AsyncClient(transport=httpx.MockTransport(provider))
        app = FastAPI()
        runtime.install_model_route(app, self.runtime)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app, client=('127.0.0.1', 1)),
                base_url='http://fixture') as client:
            async def invoke(item):
                return await client.post('/api/assistant/openclaw-models/chat/completions',
                    headers={'Authorization': 'Bearer ' + item['token']},
                    json={'model': item['agent_id'], 'messages': [{'role': 'user', 'content': 'hello'}]})
            responses = await asyncio.gather(*(invoke(item) for item in items))
            self.assertEqual([response.status_code for response in responses], [200, 200])
            self.assertCountEqual(sent, [('a.invalid', 'Bearer synthetic-key-a', 'model-a'),
                ('b.invalid', 'Bearer synthetic-key-b', 'model-b')])
            await self.runtime._stop(items[0])
            self.assertEqual((await invoke(items[0])).status_code, 403)
            self.assertEqual((await invoke(items[1])).status_code, 200)
        items[1]['busy'] = False
        replacement = await self.runtime.acquire(owners[1], self.model, {**profiles[1], 'model': 'replacement'})
        self.assertEqual(replacement['profile']['model'], 'replacement')
        self.assertEqual(items[0]['profile']['model'], 'model-a')

    async def test_text_only_first_boot_disables_tools_without_poisoning_business_or_warmup(self):
        text = agent(self.engine, actor('text'))
        text._live_text_only = True
        text._buffered_reply = True
        text_item = await text._acquire_runtime()
        self.assertFalse(text._buffered_reply)
        self.assertTrue(text_item['text_only'])
        plugin = text_item['root'] / 'plugin'
        self.assertEqual(json.loads((plugin / 'tools.json').read_text(encoding='utf-8')), [])
        self.assertEqual(json.loads((plugin / 'openclaw.plugin.json').read_text(encoding='utf-8'))['contracts']['tools'], [])
        business = agent(self.engine, actor('business'))
        business_item = await business._acquire_runtime()
        warm = agent(self.engine, actor('warm'))
        warm._live_text_only = True
        warm_item = await warm._acquire_runtime(warm_only=True)
        self.assertFalse(business_item['text_only'])
        self.assertFalse(warm_item['text_only'])
        config = json.loads((self.runtime.gateway['root'] / 'openclaw.json').read_text(encoding='utf-8'))
        self.assertEqual(config['agents']['entries'][text_item['agent_id']]['tools'], {'deny': ['*']})
        self.assertNotIn('tools', config['agents']['entries'][business_item['agent_id']])
        self.assertEqual(config['tools']['allow'], ['lighthouse_probe'])
        shared = json.loads((self.runtime.gateway['plugin'] / 'tools.json').read_text(encoding='utf-8'))
        self.assertEqual([item['name'] for item in shared], ['lighthouse_probe'])
        self.popen.assert_called_once()
        text_item['busy'] = False
        restored = await agent(self.engine, text.actor)._acquire_runtime()
        self.assertFalse(restored['text_only'])
        self.assertEqual(restored['agent_id'], text_item['agent_id'])

    async def test_business_first_text_only_model_proxy_strips_all_tool_options(self):
        business = agent(self.engine, actor('business'))
        await business._acquire_runtime()
        text = agent(self.engine, actor('text'))
        text._live_text_only = True
        item = await text._acquire_runtime()
        sent = []
        async def provider(request):
            sent.append(json.loads(request.content))
            return httpx.Response(200, json={'ok': True})
        self.runtime.http = httpx.AsyncClient(transport=httpx.MockTransport(provider))
        app = FastAPI()
        runtime.install_model_route(app, self.runtime)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app, client=('127.0.0.1', 1)),
                base_url='http://fixture') as client:
            response = await client.post('/api/assistant/openclaw-models/chat/completions',
                headers={'Authorization': 'Bearer ' + item['token']}, json={'model': item['agent_id'],
                    'messages': [{'role': 'user', 'content': 'hello'}], 'tools': [{'type': 'function'}],
                    'tool_choice': 'required', 'parallel_tool_calls': True})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(sent), 1)
        for field in ('tools', 'tool_choice', 'parallel_tool_calls'):
            self.assertNotIn(field, sent[0])
        self.assertTrue(self.runtime.accounts[runtime.account_key(execution_key(business.actor))]['busy'])


class NativeAgentLaneTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = agent_fixtures._fake_engine()
        agent_fixtures.FakeGatewayClient._reset()

    async def asyncTearDown(self):
        for _, client in self.engine.gateways.values():
            await client.close()

    async def test_legacy_tokens_sessions_and_health_keep_true_identity(self):
        legacy = agent(self.engine, actor())
        same = agent(self.engine, actor())
        a, b = agent(self.engine, actor('a')), agent(self.engine, actor('b'))
        self.assertEqual(legacy.token, same.token)
        self.assertEqual(legacy.session_key, same.session_key)
        self.assertEqual(legacy.token, self.engine.tokens[runtime.account_key('same-owner')])
        self.assertTrue(legacy.session_key.startswith('agent:' + runtime.agent_id('same-owner') + ':'))
        self.assertEqual(len({value.token for value in (legacy, a, b)}), 3)
        self.assertEqual(a.token, agent(self.engine, actor('a')).token)
        a.health.record(a._health_key(a.profile), category='model_auth')
        self.assertEqual(b.health.snapshot(b._health_key(b.profile)), {})
        self.assertTrue(all(value.actor['id'] == 'same-owner' for value in (legacy, a, b)))

    async def test_gateway_rotation_and_native_history_cache_are_lane_local(self):
        owners = [actor('a'), actor('b')]
        items = [{'port': 19999, 'token': 'synthetic-gateway', 'busy': True,
            'fingerprint': 'same', 'native_history_session': lane} for lane in ('a', 'b')]
        with patch.object(native, 'GatewayClient', agent_fixtures.FakeGatewayClient):
            first, second = await asyncio.gather(*(self.engine.gateway_client(owner, item)
                for owner, item in zip(owners, items)))
            self.assertIsNot(first, second)
            self.assertTrue(all(self.engine.gateway_ready(owner) for owner in owners))
            rotated = await self.engine.gateway_client(owners[0], {**items[0], 'token': 'rotated'})
            self.assertTrue(first.closed)
            self.assertFalse(second.closed)
            replacement = {**items[1]}
            replacement.pop('native_history_session')
            self.assertIs(second, await self.engine.gateway_client(owners[1], replacement))
            self.assertEqual(replacement['native_history_session'], 'b')
            await rotated.close()
            self.assertFalse(self.engine.gateway_ready(owners[0]))
            self.assertTrue(self.engine.gateway_ready(owners[1]))

    async def test_ending_one_lane_preserves_other_callback_socket_and_busy_state(self):
        a, b = [agent(self.engine, actor(lane)) for lane in ('a', 'b')]
        for value in (a, b):
            value._acquire_runtime = AsyncMock(return_value={'port': 19999, 'token': 'synthetic'})
        with patch.object(native, 'GatewayClient', agent_fixtures.FakeGatewayClient):
            async with b.run_stream_events(['hello'], message_history=[]) as other:
                async with a.run_stream_events(['hello'], message_history=[]):
                    a.run_id = 'owned-run-a'
                    first_client = a.client
                    self.assertEqual(set(self.engine.bridge.active), {a.token, b.token})
                self.assertNotIn(a.token, self.engine.bridge.active)
                self.assertIs(self.engine.bridge.active[b.token], b)
                self.assertTrue(b.item['busy'])
                self.assertTrue(b.client.connected)
                self.assertIn(('chat.abort', {'sessionKey': a.session_key, 'runId': 'owned-run-a'}), first_client.calls)
                self.assertFalse(any(method == 'chat.abort' for method, _ in b.client.calls))
                results = [event async for event in other]
                self.assertEqual(results[-1].result.output, 'fixture reply')
        self.assertFalse(self.engine.bridge.active)

    async def test_bridge_checks_lane_agent_session_run_and_text_only_before_dispatch(self):
        agents = [agent(self.engine, actor(lane)) for lane in ('a', 'b')]
        for value in agents:
            value.running, value.run_id, value.item = True, 'same-run', {}
            async def probe(scope: str, owner=value.actor):
                return {'ok': True, 'identity': owner['id'], 'scope': scope}
            value.tools['lighthouse_probe'] = agent_fixtures._tool(probe)
            self.engine.bridge.active[value.token] = value
        manager = SimpleNamespace(bridge_token='synthetic-bridge')
        payload = {'agent_id': runtime.agent_id(execution_key(agents[0].actor)),
            'session_key': agents[0].session_key, 'run_id': 'same-run',
            'tool': 'lighthouse_probe', 'call_id': 'same-call', 'params': {'scope': 'A'}}
        result = await self.engine.bridge.call_shared(manager.bridge_token, payload, manager)
        self.assertEqual(result['identity'], 'same-owner')
        self.assertEqual(agents[1].calls, 0)
        for invalid in ({**payload, 'agent_id': runtime.agent_id(execution_key(agents[1].actor))},
                {**payload, 'session_key': agents[1].session_key}, {**payload, 'run_id': 'other-run'}):
            with self.assertRaises(AssistantError):
                await self.engine.bridge.call_shared(manager.bridge_token, invalid, manager)
        agents[0]._live_text_only = True
        with self.assertRaises(AssistantError) as caught:
            await self.engine.bridge.call_shared(manager.bridge_token, payload, manager)
        self.assertEqual(caught.exception.status, 403)
        self.assertEqual(agents[0].calls, 1)

    async def test_live_stream_gate_run_ids_and_cross_lane_events(self):
        for live in (False, True):
            with self.subTest(live=live):
                value = agent(self.engine, actor('live-' + str(live)))
                value._live_text_only = live
                value._acquire_runtime = AsyncMock(return_value={'port': 19999, 'token': 'synthetic'})
                client = agent_fixtures.FakeGatewayClient('ws://127.0.0.1:19999', 'synthetic')
                await client.__aenter__()
                self.engine.gateway_client = AsyncMock(return_value=client)
                self.engine.keep_gateway = AsyncMock()
                partial_seen = []
                async def frame(timeout):
                    if not partial_seen:
                        partial_seen.append(True)
                        return {'event': 'agent', 'payload': {'runId': client.run_id,
                            'agentId': runtime.agent_id(execution_key(actor('other'))),
                            'stream': 'assistant', 'data': {'text': 'wrong lane'}}}
                    if len(partial_seen) == 1:
                        partial_seen.append(True)
                        return {'event': 'agent', 'payload': {'runId': client.run_id,
                            'agentId': runtime.agent_id(execution_key(value.actor)),
                            'stream': 'assistant', 'data': {'text': 'fixture reply', 'itemId': 'reply-1'}}}
                    return {'event': 'agent', 'payload': {'runId': client.run_id,
                        'stream': 'lifecycle', 'data': {'phase': 'end'}}}
                client.next_event = frame
                async with value.run_stream_events(['hello'], message_history=[]) as events:
                    first = await anext(events)
                    self.assertEqual(first.part.content, 'fixture reply')
                    self.assertEqual(value.running, True)
                    self.assertEqual(bool(value.run_id), live)
                    rest = [event async for event in events]
                self.assertEqual(rest[-1].result.output, 'fixture reply')
                params = next(params for method, params in client.calls if method == 'agent')
                expected = runtime.agent_id(execution_key(value.actor))
                self.assertEqual(params['agentId'], expected)
                self.assertEqual(params['idempotencyKey'], expected + ':same-operation')
                await client.close()

    async def test_visible_text_terminal_validation_and_provider_failures_never_retry(self):
        from pydantic_ai import ModelRetry
        for failure in ('terminal', 'validation', 'provider', 'missing-receipt', 'published-tool'):
            with self.subTest(failure=failure):
                value = agent(self.engine, actor('failed-' + failure))
                value._live_text_only = True
                value._acquire_runtime = AsyncMock(return_value={'port': 19999, 'token': 'synthetic'})
                value._next_profile = Mock(return_value=None)
                if failure in {'missing-receipt', 'published-tool'}:
                    value.calls = 1
                if failure == 'published-tool':
                    value.results['receipt'] = ('signature', {'ok': True, 'job_id': 'accepted'})
                if failure == 'validation':
                    async def reject(_):
                        raise ModelRetry('Evidence missing')
                    value.validators.append(reject)
                frames = [] if failure == 'missing-receipt' else [
                    {'event': 'agent', 'payload': {'runId': 'run-1', 'stream': 'assistant',
                        'data': {'delta': 'partial original answer'}}}]
                if failure in {'provider', 'missing-receipt', 'published-tool'}:
                    frames.append({'event': 'agent', 'payload': {'runId': 'run-1',
                        'stream': 'lifecycle', 'data': {'phase': 'error', 'error': 'Model provider unavailable'}}})
                client = agent_fixtures.FakeGatewayClient('ws://127.0.0.1:19999', 'synthetic', events=frames,
                    wait={'status': 'ok', 'terminalReply': {'disposition': 'visible',
                        'text': 'NO_REPLY' if failure == 'terminal' else 'fixture reply'}})
                await client.__aenter__()
                self.engine.gateway_client = AsyncMock(return_value=client)
                self.engine.keep_gateway = AsyncMock()
                events = []
                with self.assertRaises(AssistantError):
                    async with value.run_stream_events(['hello'], message_history=[]) as stream:
                        async for event in stream:
                            events.append(event)
                value._next_profile.assert_not_called()
                self.assertEqual(sum(method == 'agent' for method, _ in client.calls), 1)
                self.assertFalse(any(event.event_kind == 'agent_run_result' for event in events))
                if failure != 'missing-receipt':
                    self.assertEqual(events[0].delta.content_delta, 'partial original answer')
                if failure == 'validation':
                    self.assertEqual(value.health.snapshot(value._health_key(value.profile)), {})
                await client.close()

    async def test_warmup_queues_failures_and_waits_are_lane_local_and_bounded(self):
        engine = self.engine
        engine.warming, engine.warm_attempts, engine.warm_errors = {}, {}, {}
        engine.manager.accounts, engine.manager.maximum = {}, 0
        engine.WARMUP_CONCURRENCY = 2
        release = asyncio.Event()
        started = []
        async def warm(owner):
            started.append(owner)
            await release.wait()
        engine._warmup = warm
        owners = [actor(lane) for lane in ('a', 'b', 'c')]
        for owner in owners:
            native.LighthouseOpenClaw.queue_warmup(engine, owner)
        tasks = list(engine.warming.values())
        try:
            self.assertEqual(set(engine.warming), {runtime.account_key(execution_key(owner)) for owner in owners[:2]})
            await asyncio.sleep(0)
            self.assertEqual(len(started), 2)
            emit = AsyncMock()
            await native.LighthouseOpenClaw.wait_warmup(engine, owners[2], emit)
            emit.assert_not_awaited()
            first_key = runtime.account_key(execution_key(owners[0]))
            engine.warm_errors[first_key] = AssistantError('fixture failed', 503)
            first = asyncio.create_task(native.LighthouseOpenClaw.wait_warmup(engine, owners[0], emit))
            await asyncio.sleep(0)
            second = asyncio.create_task(native.LighthouseOpenClaw.wait_warmup(engine, owners[1], emit))
            await asyncio.sleep(0)
            release.set()
            with self.assertRaises(AssistantError):
                await first
            await second
        finally:
            release.set()
            await asyncio.gather(*tasks, return_exceptions=True)
        self.assertFalse(engine.warming)


if __name__ == '__main__':
    unittest.main()
