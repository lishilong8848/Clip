"""Gateway readiness must verify actual private-agent tools, not manifest names."""
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock

sys.path.insert(0, str(Path(__file__).resolve().parent))
from openclaw_service.assistant.lighthouse_runtime import OpenClawRuntime, AssistantStartupError
from openclaw_service.assistant.lighthouse_gateway import GatewayError


class ToolReadinessTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.runtime = OpenClawRuntime(self.tmp.name)
        self.runtime.tool_names = ['lighthouse_query', 'lighthouse_review_answer']
        self.item = {'agent_id': 'lh-fixture', 'profile': {'tool_calls': True}}
        self.client = AsyncMock()
        self.result = {'agentId': 'lh-fixture', 'groups': [{'tools': [
            {'id': name, 'pluginId': 'lighthouse-tools'} for name in self.runtime.tool_names]}]}

    async def test_catalog_activation_then_effective_verification_without_inference(self):
        self.client.request.side_effect = [{}, {}, self.result]
        await self.runtime._verify_tools(self.client, self.item)
        calls = self.client.request.call_args_list
        self.assertEqual([call.args[0] for call in calls], ['tools.catalog', 'sessions.patch', 'tools.effective'])
        self.assertEqual(calls[-1].args[1], {'agentId': 'lh-fixture',
            'sessionKey': 'agent:lh-fixture:lighthouse-readiness'})

    async def test_declared_but_not_callable_wrong_agent_and_denied_tools_fail_closed(self):
        for result in ({'agentId': 'lh-fixture', 'groups': []},
                       {**self.result, 'agentId': 'lh-other'},
                       {'agentId': 'lh-fixture', 'groups': [{'tools': [
                           {'id': name, 'pluginId': 'lighthouse-tools', 'deniedBySession': True}
                           for name in self.runtime.tool_names]}]}):
            with self.subTest(result=result):
                self.client.request.side_effect = [{'groups': self.result['groups']}, {}, result]
                with self.assertRaises(AssistantStartupError) as raised:
                    await self.runtime._verify_tools(self.client, self.item)
                self.assertEqual(raised.exception.category, 'runtime_tools')

    async def test_explicit_text_only_modes_do_not_require_tools(self):
        for item in ({**self.item, 'text_only': True}, {**self.item, 'profile': {'tool_calls': False}}):
            await self.runtime._verify_tools(self.client, item)
        self.client.request.assert_not_called()

    async def test_readonly_lane_checks_only_its_enabled_subset(self):
        self.client.request.side_effect = [{}, {}, {'agentId': 'lh-fixture', 'groups': [{'tools': [
            {'id': 'lighthouse_query', 'pluginId': 'lighthouse-tools'}]}]}]
        await self.runtime._verify_tools(self.client, {**self.item, 'tool_names': ['lighthouse_query']})

    async def test_native_details_are_not_exposed_as_model_failures(self):
        self.client.request.side_effect = GatewayError('INVALID_REQUEST', reason='private native content')
        with self.assertRaises(AssistantStartupError) as raised:
            await self.runtime._verify_tools(self.client, self.item)
        self.assertEqual(raised.exception.category, 'runtime_tools')
        self.assertNotIn('private', str(raised.exception))

    async def test_transient_readiness_error_keeps_existing_bounded_startup_retry(self):
        self.client.request.side_effect = GatewayError('TIMEOUT')
        with self.assertRaises(GatewayError) as raised:
            await self.runtime._verify_tools(self.client, self.item)
        self.assertEqual(raised.exception.code, 'TIMEOUT')


if __name__ == '__main__':
    unittest.main()
