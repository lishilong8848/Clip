"""Isolated MCP tests; no production store, model calls or cloud writes."""
import asyncio
import copy
import json
import sys
import tempfile
import unittest
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

sys.path.insert(0, str(Path(__file__).resolve().parent))
from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_extensions import ExtensionTools, check_schema
from openclaw_service.assistant.lighthouse_mcp_builtin import convert_table, text_lines
from openclaw_service.assistant.lighthouse_mcp_office import compare_tables, convert_units, table_statistics, working_days

ADMIN = {'id': 'fixture-admin', 'is_admin': True, 'scopes': ['ALL']}
USER = {'id': 'fixture-user', 'is_admin': False, 'scopes': ['D']}


class Store:
    def __init__(self):
        self.docs = {}

    def get_document(self, namespace, key):
        return copy.deepcopy(self.docs.get((namespace, key)))

    def put_document(self, namespace, key, value):
        self.docs[namespace, key] = copy.deepcopy(value)


class RegistryTests(unittest.TestCase):
    def setUp(self):
        self.tools = ExtensionTools(Store(), protect=lambda value: 'cipher:' + value, unprotect=lambda value: value.removeprefix('cipher:'))

    def add_remote(self):
        result = self.tools.registry.save(ADMIN, {'revision': 0, 'name': 'fixture', 'transport': 'http',
            'endpoint': 'https://example.com/mcp', 'secret': 'fixture-secret', 'admin_only': True})
        return result['servers'][-1]['id']

    def test_only_admin_edits_and_public_never_returns_credentials(self):
        with self.assertRaises(AssistantError):
            self.tools.registry.save(USER, {'revision': 0, 'id': 'lighthouse-utilities', 'enabled': False})
        identity = self.add_remote()
        visible = self.tools.registry.public(ADMIN)
        self.assertNotIn('fixture-secret', json.dumps(visible))
        self.assertNotIn(identity, json.dumps(self.tools.registry.public(USER)))
        self.assertTrue(visible['servers'][-1]['has_secret'])

    def test_stale_save_and_unapproved_tools_are_rejected(self):
        identity = self.add_remote()
        with self.assertRaises(AssistantError) as raised:
            self.tools.registry.save(ADMIN, {'revision': 0, 'id': identity, 'delete': True})
        self.assertEqual(raised.exception.status, 409)
        prior = self.tools.registry.server(ADMIN, identity, testing=True)
        with self.assertRaises(AssistantError):
            self.tools.registry.save(ADMIN, {**prior, 'revision': 1, 'allowed_tools': ['unknown'], 'enabled': True})

    def test_credential_change_disables_old_tool_approvals(self):
        identity = self.add_remote()
        prior = self.tools.registry.server(ADMIN, identity, testing=True)
        self.tools.registry.remember_tools(identity, prior, [{'name': 'search', 'description': 'Search', 'schema': {'type': 'object'}, 'read_only': True}])
        result = self.tools.registry.save(ADMIN, {**prior, 'revision': 2, 'enabled': True, 'allowed_tools': ['search']})
        updated = result['servers'][-1]
        result = self.tools.registry.save(ADMIN, {**updated, 'revision': 3, 'secret': 'changed-secret'})
        self.assertFalse(result['servers'][-1]['enabled'])
        self.assertEqual(result['servers'][-1]['allowed_tools'], [])

    def test_forbidden_schema_references_do_not_fetch_urls(self):
        with self.assertRaises(AssistantError):
            check_schema({'type': 'object', 'properties': {'q': {'$ref': 'https://example.com/schema'}}})

    def test_common_tools_keep_order_and_escape_csv_formulas(self):
        self.assertEqual(text_lines('B\nA\nB')['text'], 'B\nA')
        self.assertEqual(text_lines('B\nA\nB', sort=True)['text'], 'A\nB')
        self.assertIn("'=1+1", convert_table('[{"name":"=1+1"}]', 'json_to_csv')['text'])
        self.assertEqual(convert_table('[{"=1+1":"@SUM(A1)","name":"A"},{"name":"B"}]', 'json_to_csv')['text'],
                         "'=1+1,name\r\n'@SUM(A1),A\r\n,B\r\n")
        with self.assertRaises(ValueError):
            convert_table('a,a\n1,2')

    def test_office_units_statistics_and_table_differences_are_precise(self):
        self.assertEqual(convert_units('3.5', 'kW', 'W')['result'], '3500.0')
        with self.assertRaises(ValueError):
            convert_units('1', 'kW', 'kWh')
        value = table_statistics('[{"x":"0.1","group":"A"},{"x":"0.2","group":"A"}]', 'x', 'group')
        self.assertEqual(value['total'], '0.3')
        self.assertEqual(value['groups'], {'A': 2})
        value = compare_tables('[{"id":1,"x":2}]', '[{"id":1,"x":3},{"id":2,"x":4}]', 'id')
        self.assertEqual(value['changed_count'], 1)
        self.assertEqual(value['added_count'], 1)
        with self.assertRaises(ValueError):
            compare_tables('[{"id":1},{"id":1}]', '[]', 'id')
        self.assertEqual(working_days('2026-10-12', '2026-10-18', holidays=['2026-10-13'], work_dates=['2026-10-17'])['working_days'], 5)


class MCPIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tools = ExtensionTools(Store())

    async def asyncTearDown(self):
        await self.tools.pool.close()

    async def test_packaged_stdio_mcp_reuses_session_and_handles_concurrent_people(self):
        self.assertEqual(len(self.tools.pool.connections), 0)
        found = await self.tools.discover(USER)
        self.assertEqual(found['total'], 20)
        self.assertEqual(len(self.tools.pool.connections), 0)
        schema = await self.tools.describe(USER, 'lighthouse-utilities', 'text_lines')
        self.assertIn('text', schema['schema']['properties'])
        results = await asyncio.gather(*(self.tools.call({**USER, 'id': 'fixture-' + str(i)},
            'lighthouse-utilities', 'text_lines', {'text': f'{i}\n{i}'}, '文本去重') for i in range(20)))
        for i, result in enumerate(results):
            self.assertTrue(result['ok'])
            self.assertEqual(result['structured']['text'], str(i))
        self.assertEqual(len(self.tools.pool.connections), 1)
        self.assertEqual(len(self.tools.registry.public(USER)['recent_calls']), 0)

    async def test_invalid_arguments_and_disabled_tool_do_not_execute(self):
        self.tools.pool.call = AsyncMock()
        with self.assertRaises(AssistantError):
            await self.tools.call(USER, 'lighthouse-utilities', 'text_lines', {'text': 123}, '去重')
        self.tools.pool.call.assert_not_awaited()
        self.tools.registry.save(ADMIN, {'revision': 0, 'id': 'lighthouse-utilities', 'enabled': False})
        with self.assertRaises(AssistantError):
            await self.tools.describe(USER, 'lighthouse-utilities', 'text_lines')

    async def test_office_stdio_runs_offline_without_opening_public_services(self):
        value = await self.tools.call(USER, 'lighthouse-office', 'convert_units', {'value': '500', 'from_unit': 'kW', 'to_unit': 'W'}, '单位换算')
        self.assertTrue(value['ok'])
        self.assertEqual(value['structured']['result'], '500000')
        self.assertEqual(len(self.tools.pool.connections), 1)

    async def test_public_preset_rejects_unknown_parameters_before_network(self):
        self.tools.pool.list_tools = AsyncMock()
        self.tools.pool.call = AsyncMock()
        with self.assertRaises(AssistantError):
            await self.tools.call(USER, 'frankfurter-rates', 'get_rates', {'base': 'USD', 'secret': 'fixture-private'}, '美元汇率')
        self.tools.pool.list_tools.assert_not_awaited()
        self.tools.pool.call.assert_not_awaited()

    async def test_search_mcp_uses_host_question_and_rejects_internal_queries(self):
        descriptor = {'name': 'public_search', 'schema': {'type': 'object', 'properties': {'query': {'type': 'string'}, 'question': {'type': 'string'}}, 'required': ['query', 'question']}}
        self.tools.pool.list_tools = AsyncMock(return_value=[descriptor])
        self.tools.pool.call = AsyncMock(return_value={'ok': True, 'content': {'items': []}, 'structured': None, 'truncated': False})
        await self.tools.call(USER, 'lighthouse-public', 'public_search', {'query': 'Python official'}, 'Python official')
        self.assertEqual(self.tools.pool.call.await_args.args[-1]['question'], 'Python official')
        shown = await self.tools.describe(USER, 'lighthouse-public', 'public_search')
        self.assertNotIn('question', shown['schema']['properties'])
        with self.assertRaises(AssistantError):
            await self.tools.call(USER, 'lighthouse-public', 'public_search', {'query': 'Python'}, '查询E楼未结束通告')
        self.assertEqual(self.tools.pool.call.await_count, 1)

    async def test_successful_protocol_response_with_unavailable_source_is_not_success(self):
        item = self.tools.registry.server(USER, 'lighthouse-public')
        result = SimpleNamespace(isError=False, structuredContent=None,
            content=[SimpleNamespace(type='text', text='{"ok":false,"unavailable":true,"note":"暂不可用"}')])
        self.tools.pool.session = AsyncMock(return_value={'session': SimpleNamespace(call_tool=AsyncMock(return_value=result)), 'busy': 0})
        response = await self.tools.pool.call(USER, item, 'public_search', {'query': 'Python', 'question': 'Python'})
        self.assertFalse(response['ok'])
        self.assertIn('暂不可用', response['error'])

    async def test_remote_business_input_is_rejected_before_io(self):
        tools = ExtensionTools(Store(), protect=lambda value: value, unprotect=lambda value: value)
        config = tools.registry.save(ADMIN, {'revision': 0, 'name': 'public-search', 'transport': 'http',
            'endpoint': 'https://example.com/mcp', 'admin_only': False})
        identity = config['servers'][-1]['id']
        item = tools.registry.server(ADMIN, identity, testing=True)
        tools.registry.remember_tools(identity, item, [{'name': 'search', 'schema': {'type': 'object'}, 'description': 'Search', 'read_only': True}])
        tools.registry.save(ADMIN, {**item, 'revision': 2, 'enabled': True, 'allowed_tools': ['search']})
        tools.pool.call = AsyncMock()
        with self.assertRaises(AssistantError):
            await tools.call(USER, identity, 'search', {'q': 'E楼未结束通告'}, 'E楼未结束通告')
        tools.pool.call.assert_not_awaited()
        await tools.pool.close()


class AutoMCPTests(unittest.IsolatedAsyncioTestCase):
    async def test_large_tool_result_is_delivered_whole_and_only_to_its_owner(self):
        from bin import test_lighthouse_stream as fixtures
        from pydantic_ai.models.function import DeltaToolCall, FunctionModel
        from openclaw_service.assistant.lighthouse_model import LighthouseModel
        await fixtures.StreamTests.asyncSetUp(self)
        text = '\n'.join('Row-%04d' % i for i in range(900))
        turn = {'question': '将这些文本行去重并提供完整结果', '_profile': self.model.profile()}
        calls, artifacts = [], []

        async def stream(messages, info):
            calls.append(1)
            if len(calls) == 1:
                yield {0: DeltaToolCall(name='call_extension', json_args=json.dumps({
                    'server_id': 'lighthouse-utilities', 'tool': 'text_lines', 'arguments': {'text': text}}))}
            else:
                returned = [part.content for message in messages for part in message.parts
                            if getattr(part, 'tool_name', '') == 'call_extension' and hasattr(part, 'content')]
                value = json.loads(returned[-1]) if isinstance(returned[-1], str) else returned[-1]
                self.assertTrue(value['truncated'])
                self.assertNotIn('_full_text', value)
                self.assertEqual(value['file']['delivery'], 'conversation')
                yield '完整结果已附在当前会话。'

        @asynccontextmanager
        async def model(*_):
            yield FunctionModel(stream_function=stream)

        async def authorize():
            return self.actor

        async def emit(event, data):
            if event == 'artifact':
                artifacts.append(data['file'])

        engine = LighthouseModel(self.portal, model_factory=model)
        try:
            await engine.answer(self.actor, turn, [], self.request, emit, authorize, {})
            self.assertEqual(len(artifacts), 1)
            identity = artifacts[0]['id']
            file = self.portal.files.get(self.actor, identity)
            self.assertEqual(Path(file['path']).read_text(encoding='utf-8'), text)
            self.assertIn(identity, turn['file_ids'])
            with self.assertRaises(AssistantError):
                self.portal.files.get({**self.actor, 'id': 'other-person', 'is_admin': True}, identity)
        finally:
            await engine.close()
            await self.runtime.close()

    async def test_model_uses_mcp_from_conversation_without_manual_selection(self):
        from bin import test_lighthouse_stream as fixtures
        from pydantic_ai.models.function import DeltaToolCall, FunctionModel
        from openclaw_service.assistant.lighthouse_model import LighthouseModel
        await fixtures.StreamTests.asyncSetUp(self)
        calls = []
        async def stream(messages, info):
            calls.append(1)
            names = {tool.name for tool in info.function_tools}
            self.assertTrue({'discover_extensions', 'describe_extension', 'call_extension'} <= names)
            if len(calls) == 1:
                yield {0: DeltaToolCall(name='discover_extensions', json_args='{"keyword":"去重"}')}
            elif len(calls) == 2:
                yield {0: DeltaToolCall(name='describe_extension', json_args='{"server_id":"lighthouse-utilities","tool":"text_lines"}')}
            elif len(calls) == 3:
                yield {0: DeltaToolCall(name='call_extension', json_args='{"server_id":"lighthouse-utilities","tool":"text_lines","arguments":{"text":"Alpha\\nBeta\\nAlpha"}}')}
            else:
                returned = [part.content for message in messages for part in message.parts if getattr(part, 'tool_name', '') == 'call_extension' and hasattr(part, 'content')]
                value = json.loads(returned[-1]) if isinstance(returned[-1], str) else returned[-1]
                self.assertEqual(value['structured']['text'], 'Alpha\nBeta')
                yield '去重后的内容：Alpha、Beta。'
        @asynccontextmanager
        async def model(*_):
            yield FunctionModel(stream_function=stream)
        async def authorize():
            return self.actor
        async def emit(*_):
            pass
        engine = LighthouseModel(self.portal, model_factory=model)
        try:
            result = await engine.answer(self.actor, {'question': '帮我把 Alpha、Beta、Alpha 这几行去重', '_profile': self.model.profile()}, [], self.request, emit, authorize, {})
            self.assertIn('去重后的内容', result['answer'])
            self.assertEqual(len(calls), 4)
            self.assertEqual(self.reads, [])
            self.assertIn('扩展工具', result['sources'][0]['title'])
        finally:
            await engine.close()
            await self.runtime.close()


class ExtensionRoutesTests(unittest.TestCase):
    def test_configuration_is_authenticated_admin_only_and_lazy(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from openclaw_service.store import AssistantStore
        from openclaw_service.assistant.lighthouse_api import PortalAPICatalog
        from openclaw_service.assistant.routes import install_assistant_routes
        with tempfile.TemporaryDirectory() as directory:
            app = FastAPI()

            async def authorize(request):
                identity = request.headers.get('x-test-account')
                if identity not in {'admin', 'member'}:
                    raise AssistantError('Not authorized', 403)
                return {**USER, 'id': identity, 'is_admin': identity == 'admin'}

            host = SimpleNamespace(store=AssistantStore(directory), state=Path(directory),
                catalog=PortalAPICatalog(app), authorize=authorize,
                portal_bridge=SimpleNamespace(search=lambda *args: ([], [])))
            install_assistant_routes(app, host)
            with TestClient(app) as client:
                path = '/api/assistant/extensions'
                member, admin = {'x-test-account': 'member'}, {'x-test-account': 'admin'}
                self.assertEqual(client.get(path).status_code, 403)
                response = client.get(path, headers=member)
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.headers['cache-control'], 'no-store')
                self.assertFalse(response.json()['data']['can_manage'])
                payload = {'revision': 0, 'id': 'lighthouse-utilities', 'enabled': False}
                self.assertEqual(client.put(path, headers=member, json=payload).status_code, 403)
                self.assertEqual(client.post(path + '/lighthouse-utilities/test', headers=member).status_code, 403)
                response = client.put(path, headers=admin, json=payload)
                self.assertEqual(response.status_code, 200, response.text)
                self.assertEqual(response.json()['data']['revision'], 1)
                self.assertEqual(client.put(path, headers=admin, json=payload).status_code, 409)


if __name__ == '__main__':
    unittest.main()
