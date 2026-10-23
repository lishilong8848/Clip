"""Native mixed-query phases cannot advertise or replay mutating tools."""
import asyncio
import copy
import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pydantic_ai import Tool
from openclaw_service.assistant import lighthouse_openclaw as native
from openclaw_service.assistant.lighthouse_ai import AssistantError
from bin import test_lighthouse_model_failover as fixtures


BLOCKED = ('lighthouse_prepare_business', 'lighthouse_prepare_planned_notice',
           'lighthouse_create_text_file', 'lighthouse_create_document', 'lighthouse_create_chart',
           'lighthouse_personal_workspace', 'lighthouse_submit', 'lighthouse_prepare_image')


class MixedNativeGuardTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.fixture = fixtures.ModelFailoverTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.agent = self.fixture.agent
        self.agent.turn.update(_read_only_phase=True, _profile=copy.deepcopy(self.agent.profile))
        self.effects = []

        async def read():
            self.effects.append('read')
            return {'ok': True, 'query_ref': 'authorized-snapshot'}

        async def write():
            self.effects.append('write')
            return {'ok': True, 'form': 'must-not-be-returned'}

        self.agent.tools = {name: Tool(read) for name in native.READ_RECOVERY_TOOLS}
        self.agent.tools.update({name: Tool(write) for name in BLOCKED})

    def activate(self):
        self.agent.running = True
        self.agent.run_id = 'active-run'
        self.fixture.engine.bridge.active[self.agent.token] = self.agent

    def payload(self, name, identity='call'):
        return {'session_key': self.agent.session_key, 'run_id': self.agent.run_id,
                'tool': name, 'call_id': identity, 'params': {}}

    async def call(self, name, identity='call'):
        return await self.fixture.engine.bridge.call(self.agent.token, self.payload(name, identity))

    async def test_resident_advertisement_filters_every_acquisition_and_warmup(self):
        self.agent.resident = True
        self.fixture.engine.manager.acquire = AsyncMock(return_value={'fixture': True})
        for read_only in (True, False):
            self.agent.turn['_read_only_phase'] = read_only
            for warm_only in (True, False):
                for recovery in (True, False):
                    self.agent._read_recovery = recovery
                    self.agent._evidence_only = recovery
                    await native.OpenClawToolAgent._acquire_runtime(self.agent, warm_only=warm_only)
                    kwargs = self.fixture.engine.manager.acquire.await_args.kwargs
                    names = {item['name'] for item in kwargs['definitions']}
                    self.assertEqual(names, native.READ_RECOVERY_TOOLS if read_only else set(self.agent.tools))
                    self.assertEqual(kwargs['warm_only'], warm_only)
        self.assertEqual(set(self.agent.tools), native.READ_RECOVERY_TOOLS | set(BLOCKED))

    async def test_local_plugin_catalog_and_runtime_names_share_the_filtered_definitions(self):
        with tempfile.TemporaryDirectory() as directory:
            self.fixture.engine.manager.root = Path(directory)
            self.fixture.engine.manager.acquire = AsyncMock(return_value={'fixture': True})
            for text_only in (False, True):
                self.agent._live_text_only = text_only
                self.agent._auto_tools = False
                with patch('openclaw_service.protocol.prepare_tool_plugin') as prepare:
                    await native.OpenClawToolAgent._acquire_runtime(self.agent)
                    self.assertEqual(prepare.call_count, 2 if text_only else 1)
                    advertised = prepare.call_args_list[0].args[2]
                    self.assertEqual({item['name'] for item in advertised}, native.READ_RECOVERY_TOOLS)
                    kwargs = self.fixture.engine.manager.acquire.await_args.kwargs
                    self.assertEqual(set(kwargs['tool_names']), set() if text_only else native.READ_RECOVERY_TOOLS)
                    if text_only:
                        self.assertEqual(prepare.call_args_list[1].args[2], [])

    async def test_phase_guard_precedes_cached_results_and_every_recovery_state(self):
        self.activate()
        for evidence_only in (False, True):
            for read_recovery in (False, True):
                self.agent._evidence_only, self.agent._read_recovery = evidence_only, read_recovery
                for cached in (False, True):
                    for name in BLOCKED:
                        with self.subTest(name=name, cached=cached, evidence=evidence_only, recovery=read_recovery):
                            self.agent.results.clear()
                            if cached:
                                signature = hashlib.sha256(json.dumps([name, {}], sort_keys=True).encode()).hexdigest()
                                self.agent.results['call'] = (signature, {'ok': True, 'form': 'cached-form'})
                            before = copy.deepcopy(self.agent.results)
                            with self.assertRaises(AssistantError) as raised:
                                await self.call(name)
                            self.assertEqual(raised.exception.status, 403)
                            self.assertEqual(self.agent.results, before)
        self.assertEqual(self.effects, [])
        self.assertEqual(self.agent.calls, 0)
        self.assertEqual(self.agent.called_tools, set())
        self.assertTrue(self.agent.events.empty())

    async def test_waiting_call_checks_phase_after_acquiring_lock(self):
        self.activate()
        self.agent.turn['_read_only_phase'] = False
        await self.agent.call_lock.acquire()
        task = asyncio.create_task(self.call('lighthouse_prepare_business'))
        try:
            await asyncio.sleep(0)
            self.assertFalse(task.done())
            self.agent.turn['_read_only_phase'] = True
        finally:
            self.agent.call_lock.release()
        with self.assertRaises(AssistantError) as raised:
            await task
        self.assertEqual(raised.exception.status, 403)
        self.assertEqual(self.effects, [])

    async def test_authorized_reads_and_read_cache_remain_available(self):
        self.activate()
        first = await self.call('lighthouse_query')
        replay = await self.call('lighthouse_query')
        self.assertEqual(first, replay)
        self.assertEqual(self.effects, ['read'])
        self.assertEqual(self.agent.calls, 1)
        self.assertEqual(self.agent.called_tools, {'lighthouse_query'})

    async def test_phase_guard_survives_real_fallback_and_recovery_flag_resets(self):
        self.agent._auto_tools = True
        visited = []

        async def hook(client):
            identity = self.agent.profile['id']
            if identity in visited:
                return
            visited.append(identity)
            if identity == 'backup':
                self.assertTrue(self.agent._read_recovery)
                self.assertTrue(self.agent._evidence_only)
                self.agent._read_recovery = self.agent._evidence_only = False
            for name in BLOCKED:
                with self.assertRaises(AssistantError) as raised:
                    await self.call(name, identity + name)
                self.assertEqual(raised.exception.status, 403)
            await self.call('lighthouse_query', identity + '-read')

        self.fixture.fail(hook=hook)
        self.fixture.scripts['backup'] = {'hook': hook}
        original = copy.deepcopy(self.agent.turn)
        await self.fixture.run_agent()
        self.assertEqual(visited, ['current', 'backup'])
        self.assertEqual(self.fixture.selected, ['current', 'backup'])
        self.assertEqual(self.effects, ['read', 'read'])
        self.assertEqual(self.agent.turn, original)
        self.assertTrue(self.agent.turn['_read_only_phase'])
        self.assertFalse(self.agent._read_recovery)
        self.assertFalse(self.agent._evidence_only)
        self.assertEqual(self.agent.profile['id'], 'backup')
        self.assertEqual(self.agent.turn['_profile']['id'], 'current')
        self.assertFalse(self.fixture.engine.bridge.active)

    async def test_non_phase_turn_keeps_existing_dispatch_behavior(self):
        self.activate()
        for flag in (None, False):
            if flag is None:
                self.agent.turn.pop('_read_only_phase')
            else:
                self.agent.turn['_read_only_phase'] = flag
            result = await self.call('lighthouse_prepare_business', str(flag))
            self.assertTrue(result['ok'])
        self.assertEqual(self.effects, ['write', 'write'])


if __name__ == '__main__':
    unittest.main()
