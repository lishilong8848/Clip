"""Source discovery/recovery uses isolated native reads, never production services."""
import json
import unittest
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

from fastapi import Request
from pydantic_ai import ModelRetry, Tool
from pydantic_ai.models.function import DeltaToolCall, FunctionModel

from bin import test_lighthouse_openclaw as fixtures
from bin import test_lighthouse_model_failover as failover
from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_api import PortalAPICatalog
from openclaw_service.assistant.lighthouse_model import LighthouseModel
from openclaw_service.assistant.lighthouse_queries import PortalOperation
from lan_bitable_template_portal.feishu_assistant_questions import prepare_question_card


class SourceRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.fixture = fixtures.AgentFactoryHookTests()
        await self.fixture.asyncSetUp()
        self.addCleanup(self.fixture.tmp.cleanup)

    async def tools(self, question='查询D楼维修项目有几条'):
        agent = fixtures.FakeAgent(None)
        await self.fixture._run_answer(lambda *a, **kw: agent, question, [])
        return agent

    async def test_failed_collector_is_a_read_receipt_not_a_fatal_model_exception(self):
        agent = await self.tools('查看本周都有哪些人参与了答题')
        self.fixture.portal.learning_participation_answer = AsyncMock(side_effect=AssistantError('A楼快照未就绪', 503))
        result = await agent.tools['learning_participants'](period='week')
        self.assertFalse(result['ok'])
        self.assertEqual(result['status'], 503)
        self.assertEqual(result['query_conditions'], {'scope': 'ALL', 'period': 'week'})
        self.assertIn('discover', result['guidance'])
        with self.assertRaises(ModelRetry):
            await agent.validators[0]('未取得名单，请到页面查看。')
        self.assertEqual(self.fixture.writes, [])

    async def test_failed_turn_cannot_reissue_a_stale_clarification_card(self):
        for status in ('failed', 'stopped', 'cancelled', 'superseded', 'pending'):
            self.assertIsNone(prepare_question_card(None, {}, {}, {}, {
                'status': status, 'clarification': {'options': ['类别甲']}}))

    async def test_unavailable_sources_trigger_bounded_readonly_recovery_not_premature_final(self):
        agent = await self.tools()
        self.fixture.portal._invoke = AsyncMock(side_effect=AssistantError('读取超时', 503))
        await agent.tools['query'](PortalOperation(api_id='GET /api/repair-management/records', params={'scope': 'D'}))
        for _ in range(2):
            with self.assertRaisesRegex(ModelRetry, '继续只读查证'):
                await agent.validators[0]('本轮没有取得资料，请自行查看。')
        limited = await agent.validators[0]('本次无法核实数量。')
        self.assertIn('读取超时', limited)
        self.assertNotIn('0条', limited)
        self.assertTrue(agent._answer_repair)
        self.assertEqual(self.fixture.writes, [])

    async def test_complete_empty_data_is_not_refetched_or_rejected(self):
        agent = await self.tools()
        self.fixture.portal._invoke = AsyncMock(return_value={'ok': True, '_raw': {
            'scope': 'D', 'records': [], 'total': 0, 'complete': True}})
        operation = PortalOperation(api_id='GET /api/repair-management/records', params={'scope': 'D'})
        await agent.tools['query'](operation)
        await agent.tools['query'](operation)
        self.assertEqual(self.fixture.portal._invoke.await_count, 1)
        self.assertEqual(await agent.validators[0]('D楼已核实为0条维修记录。'), 'D楼已核实为0条维修记录。')

    async def test_incomplete_snapshot_can_be_reread_while_complete_snapshot_is_reused(self):
        agent = await self.tools()
        self.fixture.portal._invoke = AsyncMock(side_effect=[
            {'ok': True, '_raw': {'scope': 'D', 'complete': False, 'records': [], 'total': 2}},
            {'ok': True, '_raw': {'scope': 'D', 'complete': True, 'records': [
                {'record_id': 'r1', 'scope': 'D'}, {'record_id': 'r2', 'scope': 'D'}], 'total': 2}}])
        operation = PortalOperation(api_id='GET /api/repair-management/records', params={'scope': 'D'})
        first = await agent.tools['query'](operation)
        self.assertEqual(first['query_state'], 'partial')
        with self.assertRaises(ModelRetry):
            await agent.validators[0]('资料不完整，不能回答。')
        second = await agent.tools['query'](operation)
        await agent.tools['query'](operation)
        self.assertEqual(second['query_state'], 'found')
        self.assertEqual(self.fixture.portal._invoke.await_count, 2)
        self.assertEqual(await agent.validators[0]('已查全2条维修记录。'), '已查全2条维修记录。')

    async def test_permission_denial_does_not_request_alternate_source_bypass(self):
        agent = await self.tools()
        self.fixture.portal._invoke = AsyncMock(side_effect=AssistantError('无权查看此资料', 403))
        await agent.tools['query'](PortalOperation(api_id='GET /api/repair-management/records', params={'scope': 'D'}))
        result = await agent.validators[0]('无权读取，无法核实。')
        self.assertIn('无权查看', result)
        self.assertFalse(getattr(agent, '_answer_repair', False))

    async def test_one_complete_building_does_not_hide_another_buildings_missing_data(self):
        self.fixture.actor['scopes'] = ['C', 'D']
        agent = await self.tools('查询C楼和D楼的维修项目')
        self.fixture.portal._invoke = AsyncMock(side_effect=[
            {'ok': True, '_raw': {'scope': 'D', 'complete': True, 'total': 1, 'records': [{'id': 'd', 'scope': 'D'}]}},
            {'ok': True, '_raw': {'scope': 'C', 'complete': False, 'total': 1, 'records': []}},
            {'ok': True, '_raw': {'scope': 'C', 'complete': True, 'total': 1, 'records': [{'id': 'c', 'scope': 'C'}]}}])
        for scope in ('D', 'C'):
            await agent.tools['query'](PortalOperation(api_id='GET /api/repair-management/records', params={'scope': scope}))
        with self.assertRaisesRegex(ModelRetry, '只读查证'):
            await agent.validators[0]('D楼1项，C楼暂未核实。')
        await agent.tools['query'](PortalOperation(api_id='GET /api/repair-management/records', params={'scope': 'C'}))
        self.assertEqual(await agent.validators[0]('C楼和D楼各1项，已完整查得全部记录。'),
                         'C楼和D楼各1项，已完整查得全部记录。')
        self.assertEqual(self.fixture.portal._invoke.await_count, 3)

    async def test_table_read_consent_stops_source_recovery(self):
        self.fixture.portal.catalog.bridge = SimpleNamespace(acall=AsyncMock(return_value={
            'confirmation_required': True, 'message': '请确认查询此来源。'}))
        agent = await self.tools()
        result = await agent.tools['table_records']('test_source')
        self.assertTrue(result['confirmation_required'])
        self.assertIn('请确认查询此来源', await agent.validators[0]('请确认查询。'))
        self.assertFalse(getattr(agent, '_answer_repair', False))
        self.fixture.portal.catalog.bridge.acall.assert_awaited_once()

    async def test_weekly_question_can_recover_from_collector_to_registered_table(self):
        self.fixture.portal.learning_participation_answer = AsyncMock(side_effect=AssistantError('本地历史未就绪', 503))
        calls, steps = [], []
        async def read(action, params, context):
            calls.append((action, params))
            if action == 'table_catalog':
                return {'sources': [{'table_id': 'registered_learning', 'name': '学练历史'}]}
            self.assertEqual(params['query']['table_id'], 'registered_learning')
            if params['query']['metadata_only']:
                return {'name': '学练历史', 'fields': [{'name': field} for field in ('姓名', '日期', '已答题数')],
                        'source_url': '/learning'}
            self.assertTrue(params['query']['complete'])
            self.assertEqual(params['query']['filters'][0]['field_name'], '日期')
            return {'name': '学练历史', 'complete': True, 'truncated': False, 'total': 2,
                    'source_url': '/learning', 'records': [
                        {'fields': {'姓名': '测试甲', '日期': '2026-10-22', '已答题数': 3}},
                        {'fields': {'姓名': '测试乙', '日期': '2026-10-23', '已答题数': 2}}]}
        self.fixture.portal.catalog.bridge = SimpleNamespace(acall=read)
        async def script(messages, info):
            step = len(steps)
            steps.append(step)
            tools = [
                ('learning_participants', {'period': 'week'}), None,
                ('table_catalog', {'keyword': '学练'}),
                ('table_records', {'table_id': 'registered_learning', 'metadata_only': True}),
                ('table_records', {'table_id': 'registered_learning', 'fields': ['姓名', '日期', '已答题数'],
                    'filters': [{'field_name': '日期', 'operator': 'isGreater', 'value': ['2026-10-18']}], 'complete': True})]
            if step < len(tools) and tools[step]:
                name, params = tools[step]
                yield {0: DeltaToolCall(name=name, json_args=json.dumps(params))}
            elif step == 1:
                yield '本周名单未完整取得，请到页面自行查看。'
            else:
                yield '本周已参与答题的人员有测试甲、测试乙，共2人。[2]'
        @asynccontextmanager
        async def model(*args):
            yield FunctionModel(stream_function=script)
        engine = LighthouseModel(self.fixture.portal, model_factory=model)
        self.addAsyncCleanup(engine.close)
        result = await engine.answer(self.fixture.actor, {'question': '查看本周都有哪些人参与了答题',
            'operation_id': 'weekly-source-recovery', 'file_ids': [], '_profile': {'model': 'fixture', 'name': 'fixture'}},
            [], self.fixture.request, self.fixture.emit, self.fixture.authorize, {})
        self.assertIn('测试甲、测试乙', result['answer'])
        self.assertNotIn('自行查看', result['answer'])
        self.assertEqual([action for action, _ in calls], ['table_catalog', 'table_records', 'table_records'])
        self.assertEqual(len(steps), 6)
        self.assertEqual(self.fixture.writes, [])

    async def test_real_model_recovers_using_discovered_native_shift_source(self):
        self.fixture.actor['learning_scopes'] = ['D']
        @self.fixture.app.get('/api/learning/completion')
        async def completion(request: Request):
            self.assertEqual(dict(request.query_params), {'scope': 'D', 'date': '2026-10-22', 'shift': '夜'})
            return {'ok': True, 'data': {'complete': True, 'total': 2, 'page': 1, 'page_size': 30,
                'items': [{'person_id': 'p1', 'person': {'name': '测试甲'}, 'scopes': ['D'],
                           'tasks': [{'shift': '夜', 'answered': 3}]},
                          {'person_id': 'p2', 'person': {'name': '测试乙'}, 'scopes': ['D'],
                           'tasks': [{'shift': '夜', 'answered': 0}]}]}}
        self.fixture.portal.catalog = PortalAPICatalog(self.fixture.app)
        self.fixture.portal.learning_participation_answer = AsyncMock(side_effect=AssistantError('快照未完整取得', 503))
        steps, retried = [], []
        async def script(messages, info):
            step = len(steps)
            steps.append(step)
            if step == 0:
                yield {0: DeltaToolCall(name='learning_participants', json_args=json.dumps({
                    'start_date': '2026-10-22', 'end_date': '2026-10-22'}))}
            elif step == 1:
                yield '资料不全，请在页面查看。'
            elif step == 2:
                retried.extend(str(part.content) for msg in messages for part in msg.parts
                               if getattr(part, 'part_kind', '') == 'retry-prompt')
                yield {0: DeltaToolCall(name='discover', json_args=json.dumps({'group': '画像学练'}))}
            elif step == 3:
                catalogue = messages[-1].parts[0].content
                self.assertIn('/api/learning/completion', json.dumps(catalogue))
                yield {0: DeltaToolCall(name='query', json_args=json.dumps({'operation': {
                    'api_id': 'GET /api/learning/completion',
                    'params': {'scope': 'D', 'date': '2026-10-22', 'shift': '夜'}}}))}
            else:
                yield '2026-10-22夜班D楼实际参与1人：测试甲。测试乙尚未作答。[1]'
        @asynccontextmanager
        async def model(*args):
            yield FunctionModel(stream_function=script)
        engine = LighthouseModel(self.fixture.portal, model_factory=model)
        self.addAsyncCleanup(engine.close)
        result = await engine.answer(self.fixture.actor, {'question': '昨天夜班D楼有多少人参与答题',
            'operation_id': 'recovery-test', 'file_ids': [], '_profile': {'model': 'fixture', 'name': 'fixture'}},
            [], self.fixture.request, self.fixture.emit, self.fixture.authorize, {})
        self.assertIn('参与1人', result['answer'])
        self.assertNotIn('请在页面', result['answer'])
        self.assertTrue(retried)
        self.assertEqual(len(steps), 5)
        self.assertEqual(self.fixture.writes, [])

    async def test_completion_is_discoverable_with_shift_filters_but_not_writable(self):
        @self.fixture.app.get('/api/learning/completion')
        async def completion():
            return {'ok': True}
        @self.fixture.app.post('/api/learning/completion')
        async def forbidden():
            self.fail('completion writes are not available to an assistant')
        catalog = PortalAPICatalog(self.fixture.app)
        api = catalog.get('GET /api/learning/completion')
        self.assertTrue(api['read_only'])
        for field in ('scope', 'date', 'from', 'to', 'shift', 'page', 'page_size'):
            self.assertIn(field, api['schema']['query'])
        self.assertEqual(api['query_enums']['shift'], ['', '白', '夜'])
        with self.assertRaises(AssistantError):
            catalog.validate_operation({'api_id': api['id'], 'params': {'scope': 'D', 'shift': '夜班'}})
        with self.assertRaises(AssistantError):
            catalog.get('POST /api/learning/completion')

    async def test_complete_query_reads_all_completion_pages_preserving_shift_date_and_scope(self):
        self.fixture.actor['learning_scopes'] = ['D']
        reads = []
        @self.fixture.app.get('/api/learning/completion')
        async def completion(scope: str, date: str, shift: str, page: int = 1, page_size: int = 1):
            reads.append((scope, date, shift, page, page_size))
            return {'ok': True, 'data': {'from': date, 'to': date, 'total': 2, 'page': page, 'page_size': page_size,
                'items': [{'id': 'daily-' + str(page), 'person_id': 'p' + str(page), 'scopes': [scope],
                           'tasks': [{'shift': shift, 'answered': page}]}]}}
        self.fixture.portal.catalog = PortalAPICatalog(self.fixture.app)
        agent = await self.tools('查询D楼昨天夜班参与答题的完整名单')
        result = await agent.tools['query'](PortalOperation(api_id='GET /api/learning/completion', params={
            'scope': 'D', 'date': '2026-10-22', 'shift': '夜', 'page': 1, 'page_size': 1}), complete=True)
        self.assertEqual(result['query_state'], 'found')
        self.assertTrue(result['data']['query_completeness']['complete'])
        self.assertEqual(len(result['data']['items']), 2)
        self.assertEqual(reads, [('D', '2026-10-22', '夜', page, 1) for page in (1, 2)])
        self.assertEqual(self.fixture.writes, [])


class NativeReadRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_empty_second_read_reply_can_continue_without_replaying_a_write(self):
        flow = failover.ModelFailoverTests()
        flow.setUp()
        self.addCleanup(flow.tearDown)
        agent, reads = flow.agent, []
        agent._auto_tools = True
        agent._buffered_reply = True
        async def query(page: int):
            reads.append(page)
            return {'ok': True, 'data': {'page': page, 'has_more': page < 3}}
        async def write():
            self.fail('read recovery must not execute business writes')
        agent.tools.update(lighthouse_query=Tool(query), lighthouse_prepare_business=Tool(write))
        async def validate(answer):
            if answer == '只有第一页':
                raise ModelRetry('请继续读取原条件的后续页。')
            return answer
        agent.validators.append(validate)
        async def hook(client):
            page = len([p for m, p in client.calls if m == 'agent'])
            if page not in reads:
                await flow.engine.bridge.call(agent.token, {'session_key': agent.session_key,
                    'run_id': agent.run_id, 'tool': 'lighthouse_query', 'call_id': 'page-' + str(page), 'params': {'page': page}})
                if page > 1:
                    with self.assertRaises(AssistantError):
                        await flow.engine.bridge.call(agent.token, {'session_key': agent.session_key,
                            'run_id': agent.run_id, 'tool': 'lighthouse_prepare_business', 'call_id': 'no-write', 'params': {}})
            client.wait = {'status': 'ok', 'terminalReply': {'disposition': 'visible',
                'text': {1: '只有第一页', 2: '', 3: '已取得三页并核实。'}[page]}}
        flow.scripts['current'] = {'hook': hook, 'events': []}
        events = await flow.run_agent()
        self.assertEqual(events[-1].result.output, '已取得三页并核实。')
        self.assertEqual(reads, [1, 2, 3])
        self.assertEqual(flow.selected, ['current'])
        self.assertFalse(agent._read_recovery)


if __name__ == '__main__':
    unittest.main()
