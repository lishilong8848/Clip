"""General assistance and local tools use no cloud/business test writes."""
import datetime as dt
import asyncio
import copy
import json
import unittest
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, Mock

from bin import test_lighthouse_stream as fixtures
from pydantic_ai.models.function import DeltaToolCall, FunctionModel
from openclaw_service.assistant.lighthouse_basics import calculate, CalculationError, date_time, calculation_request, time_request, public_capability_request
from openclaw_service.assistant.lighthouse_model import LighthouseModel, equipment_knowledge_question, instructions_for_question
from openclaw_service.assistant.lighthouse_ai import is_business_query
from openclaw_service.assistant.lighthouse_ai import AssistantError, CustomModel
from openclaw_service.assistant.lighthouse_openclaw import LighthouseOpenClaw
from openclaw_service.assistant.lighthouse_queries import business_domains


class CalculationTests(unittest.TestCase):
    def test_precise_common_operations(self):
        for expression, answer in [('0.1+0.2', '0.3'), ('(4000*12)/1000', '48'), ('1200*15%', '180'),
            ('-3//2', '-2'), ('-3%2', '1'), ('3%-2', '-1'), ('3%(-2)', '-1'), ('-3%-2', '-1'),
            ('1e-3%', '0.00001'), ('2^10', '1024'), ('(-2)^5', '-32'), ('1e-10*2', '0.0000000002')]:
            with self.subTest(expression=expression):
                self.assertEqual(calculate(expression)['result'], answer)

    def test_unsafe_and_unbounded_expressions_are_rejected(self):
        for expression in ["__import__('os').system('x')", 'a+b', '[1][0]', '1/0', '2**9999999', '1e10000000',
                           '1e-10000000', '9'*41, '1+'*100+'1', '1+2#comment', '1 << 2', '(-2)**0.5']:
            with self.subTest(expression=expression), self.assertRaises(CalculationError):
                calculate(expression)

    def test_native_routing_is_not_a_business_action(self):
        self.assertEqual(calculation_request('请计算 1200*15%是多少？'), '1200*15%')
        self.assertEqual(calculation_request('0.1+0.2'), '0.1+0.2')
        self.assertIsNone(calculation_request('今天有多少事件'))
        self.assertIsNone(calculation_request('请修改机柜功率为4000'))
        self.assertTrue(time_request('现在几点？'))
        self.assertTrue(time_request('今天是星期几？'))
        self.assertFalse(time_request('今天通告开始时间是多少'))

    def test_beijing_time_and_date_difference(self):
        result = date_time('2026-10-01', '2026-10-05')
        self.assertEqual(result['days'], 4)
        self.assertEqual(dt.datetime.fromisoformat(result['now']).utcoffset(), dt.timedelta(hours=8))
        for args in [('2026-02-30', '2026-10-05'), ('', '2026-10-05')]:
            with self.assertRaises(ValueError): date_time(*args)

    def test_network_capability_is_not_a_request_for_facts(self):
        for question in ('你能联网查询吗？简单说明。', '你支持联网吗', '是否支持上网搜索？',
                         'Can you browse the web?', 'Can you search the internet?'):
            self.assertTrue(public_capability_request(question), question)
        for question in ('请联网查南通明天天气', '你能联网查询今天E楼通告吗', '你能查今天的新闻吗',
                         'Can you browse https://example.com?', 'Can you search for Python docs?'):
            self.assertFalse(public_capability_request(question), question)


class GeneralModelTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await fixtures.StreamTests.asyncSetUp(self)

    async def asyncTearDown(self):
        await self.runtime.close()

    async def authorize(self):
        return self.actor

    def test_equipment_knowledge_policy_is_narrow_and_permission_aware(self):
        for question in ('UPS是什么？', 'what is UPS?', 'HVDC原理是什么', '柴油发电机有哪些常见故障',
                         '机柜正式电和测试电的区别'):
            self.assertTrue(equipment_knowledge_question(question), question)
            self.assertIn('设备原理', instructions_for_question(question))
            self.assertNotIn('设备原理', instructions_for_question(question, knowledge_access=False))
        for question in ('汽车维修有哪些问题', 'Python原理是什么', 'D楼这台UPS当前状态是什么', 'MYUPSTOOL是什么'):
            self.assertFalse(equipment_knowledge_question(question), question)

    async def test_simple_calculation_and_time_skip_model_and_business(self):
        @asynccontextmanager
        async def forbidden(*_):
            raise AssertionError('Local tools must not wait for the gateway/model')
            yield
        engine = LighthouseModel(self.portal, model_factory=forbidden)
        self.portal.catalog.get = Mock(side_effect=AssertionError('Do not query business APIs'))
        for question in ['0.1+0.2', '现在几点？', '1/0']:
            result = await engine.answer(self.actor, {'question': question, '_profile': self.model.profile()},
                [], self.request, AsyncMock(), self.authorize, {})
            self.assertTrue(result['answer'])
            self.assertEqual(result['sources'], [])
        self.assertEqual(self.reads, [])

    async def test_network_capability_uses_registration_not_model_or_live_queries(self):
        @asynccontextmanager
        async def forbidden(*_):
            raise AssertionError('Capability question must not need inference or real data')
            yield
        public = Mock(weather=AsyncMock(side_effect=AssertionError('No weather was asked for')),
                      search=AsyncMock(side_effect=AssertionError('No search topic was given')))
        for source, commands in ((public, []), (public, [{'kind': 'tool', 'name': 'public_search'}]), (None, [])):
            engine = LighthouseModel(self.portal, model_factory=forbidden, public_sources=source)
            result = await engine.answer(self.actor,
                {'question': '你能联网查询吗？简单说明。', '_profile': self.model.profile(), '_command_context': commands},
                [], self.request, AsyncMock(), self.authorize, {})
            self.assertEqual(result['sources'], [])
            self.assertIsNone(result.get('plan'))
            if source:
                self.assertIn('支持公开搜索', result['answer'])
            else:
                self.assertIn('未接入', result['answer'])
        self.assertEqual(self.reads, [])

    async def test_general_question_uses_model_without_unrelated_business_reads(self):
        async def stream(messages, info):
            self.assertIn('calculate', {tool.name for tool in info.function_tools})
            self.assertIn('date_time', {tool.name for tool in info.function_tools})
            yield 'Hello 的中文是你好。'
        @asynccontextmanager
        async def factory(*_):
            yield FunctionModel(stream_function=stream)
        engine = LighthouseModel(self.portal, model_factory=factory)
        result = await engine.answer(self.actor, {'question': '把 Hello 翻译成中文', '_profile': self.model.profile()},
            [], self.request, AsyncMock(), self.authorize, {})
        self.assertEqual(result['answer'], 'Hello 的中文是你好。')
        self.assertEqual(self.reads, [])
        self.assertEqual(result['sources'], [])

    async def test_chat_identifier_does_not_become_a_building_or_business_update(self):
        question = '将聊天代号改为 BETA-PUBLIC-82，后续以这个为准。'
        self.assertFalse(is_business_query(question))
        for label in ('ALPHA-PUBLIC-47', 'TOPIC-FAB-12', 'NAMED-31'):
            self.assertFalse(is_business_query('本次聊天代号为 ' + label), label)
        for question_with_scope in ('查询C-247当前机柜状态', 'E楼维修单有几条', '本机房D-201上下电记录',
                                    'EA118-D-346检修通告有哪些'):
            self.assertTrue(is_business_query(question_with_scope), question_with_scope)
        async def stream(*_):
            yield '聊天代号已改为 BETA-PUBLIC-82。'
        @asynccontextmanager
        async def factory(*_):
            yield FunctionModel(stream_function=stream)
        engine = LighthouseModel(self.portal, model_factory=factory,
            cached_reader=Mock(side_effect=AssertionError('Do not query unrelated records')))
        result = await engine.answer(self.actor, {'question': question, '_profile': self.model.profile()},
            [], self.request, AsyncMock(), self.authorize, {})
        self.assertEqual(result['answer'], '聊天代号已改为 BETA-PUBLIC-82。')
        self.assertEqual(result['sources'], [])
        self.assertIsNone(result.get('plan'))
        self.assertEqual(self.reads, [])

    async def test_model_can_call_real_calculator_callback(self):
        async def stream(messages, info):
            returns = [part for message in messages for part in message.parts if getattr(part, 'part_kind', '') == 'tool-return']
            if not returns:
                yield {0: DeltaToolCall(name='calculate', json_args=json.dumps({'expression': '350*15%'}))}
            else:
                self.assertEqual(returns[-1].content['result'], '52.5')
                yield '350 的15%是52.5。'
        @asynccontextmanager
        async def factory(*_):
            yield FunctionModel(stream_function=stream)
        engine = LighthouseModel(self.portal, model_factory=factory)
        result = await engine.answer(self.actor, {'question': '350的百分之十五是多少', '_profile': self.model.profile()},
            [], self.request, AsyncMock(), self.authorize, {})
        self.assertEqual(result['answer'], '350 的15%是52.5。')
        self.assertEqual(self.reads, [])

    async def test_general_knowledge_writing_and_code_do_not_prepare_business(self):
        for question in ['如何提高工作效率', '解释Python事件循环', '创建一个Python任务调度示例', '翻译这段故事',
                         '如何提高汽车维修效率', '给我一份汽车维修工单模板', '电脑设备怎么设置密码',
                         'UPS是什么', 'what is UPS?', '机柜正式电和测试电的区别', '柴油发电机有哪些常见故障',
                         '在Word里怎么插入签名', 'Excel中B17单元格怎么求和', '汽车维修还有多少未完成的']:
            with self.subTest(question=question):
                self.assertFalse(is_business_query(question))
                self.assertEqual(business_domains(question), set())
                async def stream(messages, info):
                    yield '普通问题的简洁回答。'
                @asynccontextmanager
                async def factory(*_):
                    yield FunctionModel(stream_function=stream)
                result = await LighthouseModel(self.portal, model_factory=factory).answer(self.actor,
                    {'question': question, '_profile': self.model.profile()}, [], self.request, AsyncMock(), self.authorize, {})
                self.assertEqual(result['answer'], '普通问题的简洁回答。')
                self.assertIsNone(result.get('plan'))
        self.assertEqual(self.reads, [])
        self.assertTrue(is_business_query('今天E楼发生哪些事件'))
        self.assertEqual(business_domains('今天E楼发生哪些事件'), {'events'})
        for question in ['今天E楼的维修项目有几条', '维修单怎么关联检修通告', '解释D楼这台UPS当前状态',
                         '灯塔助手中怎么设置密码', 'A-201包间B17机柜的实际时间是什么',
                         '创建一个演练示例', '新建维修单示例', '解释本周事件统计']:
            self.assertTrue(is_business_query(question), question)

    async def test_general_question_rejects_an_unrelated_business_tool_call(self):
        async def stream(messages, info):
            retries = [part for message in messages for part in message.parts if getattr(part, 'part_kind', '') == 'retry-prompt']
            if not retries:
                yield {0: DeltaToolCall(name='pending_work', json_args='{}')}
            else:
                yield '汽车维修通常应先诊断故障，再核对维修项目。'
        @asynccontextmanager
        async def factory(*_):
            yield FunctionModel(stream_function=stream)
        engine = LighthouseModel(self.portal, model_factory=factory,
            cached_reader=Mock(side_effect=AssertionError('No unrelated local business data')))
        for question in ('汽车维修有哪些常见问题', '你好', 'UPS是什么？'):
            with self.subTest(question=question):
                result = await engine.answer(self.actor, {'question': question, '_profile': self.model.profile()},
                    [], self.request, AsyncMock(), self.authorize, {})
                self.assertIn('汽车维修', result['answer'])
                self.assertEqual(result['sources'], [])
                self.assertIsNone(result.get('plan'))
        self.assertEqual(self.reads, [])

    async def test_news_uses_public_search_without_portal_queries(self):
        async def stream(messages, info):
            returns = [part for message in messages for part in message.parts if getattr(part, 'part_kind', '') == 'tool-return']
            if not returns:
                yield {0: DeltaToolCall(name='public_search', json_args=json.dumps({'query': '国际新闻事件'}))}
            else:
                self.assertTrue(returns[-1].content['ok'])
                yield '公开来源提供了这条新闻[1]，发布日期尚未核实。'
        @asynccontextmanager
        async def factory(*_):
            yield FunctionModel(stream_function=stream)
        public = Mock()
        public.search = AsyncMock(return_value={'ok': True, 'queried_at': '2026-10-05T10:00:00+08:00',
            'results': [{'title': '国际新闻', 'url': 'https://example.com/news', 'snippet': '新闻资料'}]})
        result = await LighthouseModel(self.portal, model_factory=factory, public_sources=public).answer(self.actor,
            {'question': '今天国际新闻事件有哪些', '_profile': self.model.profile()}, [], self.request, AsyncMock(), self.authorize, {})
        public.search.assert_awaited_once_with('国际新闻事件', '今天国际新闻事件有哪些')
        self.assertEqual(len(result['sources']), 1)
        self.assertEqual(self.reads, [])

    async def test_empty_public_search_returns_honest_result_without_retry_loop(self):
        rounds = []
        async def stream(messages, info):
            rounds.append(1)
            if len(rounds) == 1:
                yield {0: DeltaToolCall(name='public_search', json_args=json.dumps({'query': '国际新闻事件'}))}
            else:
                yield '没有匹配的公开搜索结果。'
        @asynccontextmanager
        async def factory(*_):
            yield FunctionModel(stream_function=stream)
        public = Mock()
        public.search = AsyncMock(return_value={'ok': True, 'empty': True, 'count': 0, 'results': [],
                                               'queried_at': '2026-10-06T00:00:00+08:00'})
        result = await LighthouseModel(self.portal, model_factory=factory, public_sources=public).answer(self.actor,
            {'question': '今天国际新闻事件有哪些', '_profile': self.model.profile()},
            [], self.request, AsyncMock(), self.authorize, {})
        self.assertEqual(len(rounds), 2)
        self.assertIn('未找到', result['answer'])
        self.assertNotIn('未查询真实', result['answer'])
        self.assertEqual(result['sources'], [])
        self.assertEqual(self.reads, [])

    async def test_explicit_source_request_cannot_claim_common_knowledge_as_verified(self):
        async def stream(*_):
            yield 'UPS 是不间断电源。出处是常识和任务说明。'
        @asynccontextmanager
        async def factory(*_):
            yield FunctionModel(stream_function=stream)
        self.actor['learning_scopes'] = ['D']
        engine = LighthouseModel(self.portal, model_factory=factory)
        from pydantic_ai.exceptions import UnexpectedModelBehavior
        emitted = []
        async def emit(kind, value):
            emitted.append((kind, value))
        with self.assertRaises(UnexpectedModelBehavior):
            await engine.answer(self.actor, {'question': 'UPS是什么？解释并给出处。',
                '_profile': self.model.profile()}, [], self.request, emit, self.authorize, {})
        self.assertFalse(any(kind == 'text' and '出处是常识' in str(value) for kind, value in emitted))

    async def test_source_code_explanation_does_not_require_web_research(self):
        async def stream(*_):
            yield 'Source code is human-readable program text.'
        @asynccontextmanager
        async def factory(*_):
            yield FunctionModel(stream_function=stream)
        public = Mock(search=AsyncMock(side_effect=AssertionError('No external request was asked for')))
        result = await LighthouseModel(self.portal, model_factory=factory, public_sources=public).answer(self.actor,
            {'question': 'Explain Python source code briefly.', '_profile': self.model.profile()},
            [], self.request, AsyncMock(), self.authorize, {})
        self.assertIn('human-readable', result['answer'])
        self.assertEqual(result['sources'], [])
        public.search.assert_not_awaited()

    async def test_cited_equipment_explanation_cannot_stop_at_its_reference(self):
        @self.app.get('/api/assistant/question-bank')
        async def knowledge(scope: str = 'D', search: str = ''):
            self.reads.append('knowledge')
            return {'ok': True, 'data': {'scope': scope, 'items': [{
                'id': 'fixtureUPS', 'stem': 'UPS的作用是什么？',
                'answer_text': '市电中断时继续供电。', 'version': 1}], 'total': 1}}
        self.actor['learning_scopes'] = ['D']
        self.portal.catalog = fixtures.PortalAPICatalog(self.app)
        answer = 'UPS 是不间断电源,市电中断时继续供电。[1]'
        for first, expected_rounds in (
                ('**出处**：题库中的 UPS 不间断电源说明。[1]', 3),
                ('来源：题库。[1]\n' + answer, 2)):
            with self.subTest(first=first):
                rounds, emitted = [], []
                self.reads.clear()
                async def stream(messages, info):
                    rounds.append(1)
                    if len(rounds) == 1:
                        yield {0: DeltaToolCall(name='query', json_args=json.dumps({'operation': {
                            'api_id': 'GET /api/assistant/question-bank', 'params': {'scope': 'D', 'search': 'UPS'}}}))}
                    elif len(rounds) == 2:
                        yield first
                    else:
                        self.assertTrue(any(getattr(part, 'part_kind', '') == 'retry-prompt'
                            for message in messages for part in message.parts))
                        yield answer
                @asynccontextmanager
                async def factory(*_):
                    yield FunctionModel(stream_function=stream)
                async def emit(kind, value):
                    emitted.append((kind, value))
                result = await LighthouseModel(self.portal, model_factory=factory).answer(self.actor,
                    {'question': 'UPS是什么？解释并给出处。', '_profile': self.model.profile()},
                    [], self.request, emit, self.authorize, {})
                self.assertEqual(len(rounds), expected_rounds)
                self.assertIn(answer, result['answer'])
                self.assertEqual(self.reads, ['knowledge'])
                self.assertEqual(len(result['sources']), 1)
                self.assertEqual([value['delta'] for kind, value in emitted if kind == 'text'], [result['answer']])

    async def test_twenty_accounts_run_in_parallel_with_private_models_and_history(self):
        configured = CustomModel(self.store, client=Mock(), protect=lambda key: 'fixture:' + key,
            unprotect=lambda key: key.removeprefix('fixture:'))
        self.assistant.model = configured
        actors = [{'id': 'account-' + str(n), 'scopes': ['ABCDEH'[n % 6]], 'is_admin': False}
                  for n in range(20)]
        for n, actor in enumerate(actors):
            configured.for_actor(actor['id']).configure({'action': 'upsert', 'profile': {
                'id': 'default', 'name': 'Model ' + str(n), 'model': 'model-' + str(n),
                'endpoint': 'https://example.com/v1/chat/completions', 'api_key': 'synthetic-key-' + str(n)}})
        started, release, calls = asyncio.Event(), asyncio.Event(), {}
        class ParallelEngine:
            max_parallel = LighthouseOpenClaw.max_parallel
            async def answer(engine, actor, turn, history, request, emit, authorize, context):
                calls[actor['id']] = (copy.deepcopy(turn), copy.deepcopy(history))
                if len(calls) == len(actors):
                    started.set()
                await release.wait()
                return {'answer': actor['id'] + ':' + turn['_profile']['model'], 'sources': []}
        self.runtime = fixtures.LighthouseStream(self.portal, engine=ParallelEngine())
        acknowledgements = []
        for n, actor in enumerate(actors):
            async def authorize(owner=actor): return copy.deepcopy(owner)
            state = self.assistant._state(actor)
            acknowledgements.append(await self.runtime.submit(actor, {
                'question': '独立问题' + str(n), 'file_ids': [], 'operation_id': 'parallel_message_%04d' % n,
                'conversation_id': state['id']}, self.request, authorize))
        await asyncio.wait_for(started.wait(), 5)
        self.assertEqual(len({ack['conversation_id'] for ack in acknowledgements}), 20)
        for n, actor in enumerate(actors):
            turn, history = calls[actor['id']]
            self.assertEqual(turn['_profile']['model'], 'model-' + str(n))
            self.assertEqual(turn['question'], '独立问题' + str(n))
            self.assertEqual(history, [])
            self.assertEqual(turn['scopes'], actor['scopes'])
        with self.assertRaises(AssistantError):
            self.runtime.get_run(actors[0], acknowledgements[1]['run_id'])
        with self.assertRaises(AssistantError):
            await self.runtime.stop(actors[0], acknowledgements[1]['run_id'])
        await self.runtime.stop(actors[0], acknowledgements[0]['run_id'])
        self.assertTrue(all(not self.runtime.workers[actor['id']].done() for actor in actors[1:]))
        release.set()
        await asyncio.wait_for(asyncio.gather(*self.runtime.workers.values(), return_exceptions=True), 5)
        for n, actor in enumerate(actors[1:], 1):
            state = await self.runtime.conversation(actor)
            self.assertEqual(len(state['turns']), 1)
            self.assertEqual(state['turns'][0]['answer'], actor['id'] + ':model-' + str(n))


if __name__ == '__main__':
    unittest.main()
