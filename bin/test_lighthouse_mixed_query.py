"""Mixed routing through the real answer wrapper; synthetic APIs/models only."""
import copy
import json
import sys
import unittest
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from pydantic_ai import Tool
from pydantic_ai.models.function import DeltaToolCall, FunctionModel
from openclaw_service.assistant import lighthouse_knowledge_answer as documents
from openclaw_service.assistant import lighthouse_query_intent as classifier
from openclaw_service.assistant.lighthouse_ai import AssistantError, PRIVATE_REPLY, safe_text
from openclaw_service.assistant.lighthouse_model import LighthouseModel
from openclaw_service.assistant.lighthouse_openclaw import BusinessBridge
from openclaw_service.assistant.lighthouse_review import READ_REPAIR_TOOLS
from bin import test_lighthouse_openclaw as fixtures


QUESTION = '核对D楼维修项目与现场规程，逐条比较适用条件'
BUSINESS_QUERY = '核对D楼维修项目的适用条件'
DOCUMENT_QUERY = 'D楼维修项目的现场规程适用条件'
DOCUMENT_BODY = 'PRIVATE_DOCUMENT_BODY_ONLY_FOR_STATELESS_SYNTHESIS'
FINAL = '业务记录与适用规程已分别核对。[1][2]'


def resolved(kind='mixed', *, question=QUESTION, attempts=1, continued=False):
    return {'kind': kind, 'query': question, 'continued': continued, 'attempts': attempts,
            'task_type': {'mixed': 'mixed', 'knowledge': 'document', 'business': 'statistics',
                          'general': 'general', 'unknown': 'unknown'}[kind],
            'business_query': BUSINESS_QUERY if kind == 'mixed' else '',
            'document_query': DOCUMENT_QUERY if kind == 'mixed' else ''}


class MixedQueryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.enterContext(patch('socket.create_connection', side_effect=AssertionError('Network forbidden')))
        self.enterContext(patch('socket.socket.connect', side_effect=AssertionError('Network forbidden')))
        self.fixture = fixtures.AgentFactoryHookTests()
        await self.fixture.asyncSetUp()
        self.addCleanup(self.fixture.doCleanups)
        self.actor = copy.deepcopy(self.fixture.actor)
        self.current_actor = copy.deepcopy(self.actor)
        self.authorize = AsyncMock(side_effect=lambda: copy.deepcopy(self.current_actor))
        self.profile = {'id': 'primary', 'name': 'Synthetic', 'model': 'fixture-primary',
            'endpoint': 'https://fixture.invalid/v1/chat/completions', 'key_cipher': 'synthetic-cipher'}
        self.profiles = [self.profile, {**self.profile, 'id': 'backup', 'model': 'fixture-backup'}]
        self.fixture.model.profile.side_effect = lambda key='': next(
            p for p in self.profiles if p['id'] == (key or 'primary'))
        self.fixture.model.settings.return_value = {'enabled': True, 'configured': True,
            'models': [{**p, 'configured': True} for p in self.profiles]}
        self.item = {'document_id': 'fixture-doc', 'version': 1, 'ordinal': 0,
            'name': '现场规程', 'location': '第1段', 'text': DOCUMENT_BODY,
            'url': '/knowledge-base?document=fixture-doc&version=1&chunk=0'}
        self.knowledge = SimpleNamespace(search=Mock(return_value={'items': [self.item], 'warning': ''}),
                                         evidence_current=Mock(return_value=True))
        self.fixture.portal.get_knowledge = Mock(return_value=self.knowledge)
        self.events, self.transcript, self.tool_sets, self.steps = [], [], [], []
        self.enterContext(patch('openclaw_service.assistant.lighthouse_planned.continue_selection',
                                new_callable=AsyncMock, return_value=None))

        async def stream(messages, info):
            index = len(self.transcript)
            self.transcript.append(copy.deepcopy(messages))
            self.tool_sets.append({tool.name for tool in info.function_tools})
            step = self.steps[index] if index < len(self.steps) else '已核对本轮业务资料。[1]'
            if isinstance(step, tuple):
                name, args = step
                yield {0: DeltaToolCall(name=name, json_args=json.dumps(args, ensure_ascii=False))}
            else:
                yield step

        @asynccontextmanager
        async def model_factory(*_):
            yield FunctionModel(stream_function=stream)

        self.engine = LighthouseModel(self.fixture.portal, model_factory=model_factory)
        self.addAsyncCleanup(self.engine.close)

    def turn(self, question=QUESTION, **changes):
        return {'question': question, 'operation_id': 'mixed-fixture', 'run_id': 'mixed-run',
            'file_ids': [], 'scopes': ['D'], 'execution_lane': '', 'feishu_topic': '',
            '_profile': copy.deepcopy(self.profile), **changes}

    async def emit(self, kind, value):
        self.events.append((kind, copy.deepcopy(value)))

    async def answer(self, turn=None, history=None, **kwargs):
        return await self.engine.answer(self.actor, turn if turn is not None else self.turn(),
            history or [], self.fixture.request, self.emit, self.authorize, {}, **kwargs)

    def texts(self):
        return ''.join(data['delta'] for kind, data in self.events if kind == 'text')

    def patch_intent(self, value=None):
        return self.enterContext(patch.object(classifier, 'resolve_query_intent',
            new_callable=AsyncMock, return_value=value if value is not None else resolved()))

    def business_result(self, **changes):
        rows = [{'record_id': 'fixture-' + str(i), 'scope': 'D', 'title': '维修项目' + str(i)} for i in range(60)]
        full = {'number': 1, 'title': '本轮维修回执', 'url': '/api/repair-management/records',
                'available': True, 'scopes': ['D'], 'query_ref': 'read-fixture',
                'data': {'records': rows, 'total': 60, 'complete': True}}
        return {'answer': '本轮业务结果。[1]', 'sources': [{**full, 'data': {'records': rows[:2]}}],
                '_query_evidence': [full], **changes}

    def combined_result(self):
        source = {'number': 2, 'kind': 'company_knowledge', 'title': '现场规程',
            'url': self.item['url'], 'data': {k: v for k, v in self.item.items() if k not in {'text', 'url'}}}
        return {'answer': FINAL, 'sources': [source],
                '_review_sources': [{**source, 'data': {**source['data'], 'text': DOCUMENT_BODY}}],
                '_knowledge_evidence': [copy.deepcopy(source['data'])]}

    async def test_mixed_phase_uses_business_query_and_passes_full_receipts(self):
        classify = self.patch_intent()
        turn = self.turn(_task_context={'query': '旧条件'}, prompt='原始提示')
        original = copy.deepcopy(turn)
        history = [{'question': '上轮问题', 'answer': '上轮回答', 'scopes': ['D']}]
        before = copy.deepcopy(history)
        business = self.business_result()
        expected_receipts = copy.deepcopy(business['_query_evidence'])
        phases = []
        async def read(actor, phase, old, request, emit, authorize, context):
            self.assertEqual(old, [])
            self.assertEqual(context, {})
            phases.append(copy.deepcopy(phase))
            await emit('reply-mode', {'mode': 'live'})
            await emit('text', {'delta': 'HIDDEN_BUSINESS_DRAFT'})
            return business
        self.engine._answer_candidate = AsyncMock(side_effect=read)
        with patch.object(documents, 'answer_company', new_callable=AsyncMock,
                          return_value=self.combined_result()) as synth:
            result = await self.answer(turn, history)
        classify.assert_awaited_once()
        self.assertEqual(len(phases), 1)
        self.assertTrue(phases[0]['_read_only_phase'])
        self.assertEqual(phases[0]['question'], BUSINESS_QUERY)
        self.assertEqual(phases[0]['prompt'], BUSINESS_QUERY)
        self.assertEqual(phases[0]['_query_intent']['kind'], 'business')
        self.assertFalse(phases[0]['_task_context'])
        self.assertEqual(phases[0]['_query_intent']['query'], BUSINESS_QUERY)
        self.assertEqual(phases[0]['_answer_goals'], [])
        self.assertEqual(synth.await_args.args[3], QUESTION)
        self.assertEqual(synth.await_args.kwargs['document_query'], DOCUMENT_QUERY)
        self.assertEqual(synth.await_args.kwargs['business_sources'], expected_receipts)
        self.assertEqual(len(synth.await_args.kwargs['business_sources'][0]['data']['records']), 60)
        self.assertEqual({k: turn[k] for k in original}, original)
        self.assertEqual(history, before)
        self.assertNotIn(DOCUMENT_BODY, repr(phases))
        self.assertNotIn(DOCUMENT_BODY, repr(result))
        self.assertNotIn('HIDDEN_BUSINESS_DRAFT', self.texts())
        self.assertEqual(self.texts(), FINAL)

    async def test_document_only_equipment_never_reenters_business_phase_in_either_channel(self):
        question = '查询D楼未完成工单数量，同时核对设备X的任务资料归档要求'
        business_query = '查询D楼未完成工单数量'
        document_query = '核对设备X的任务资料归档要求'
        for channel in ('', 'feishu:oc_fixture'):
            with self.subTest(channel=channel):
                self.actor['channel'] = channel
                self.current_actor = copy.deepcopy(self.actor)
                routing = {**resolved(question=question), 'business_query': business_query,
                           'document_query': document_query}
                turn = self.turn(question, _task_context={'query': '设备X的旧条件'},
                                 _answer_goals=[{'id': 'old', 'request': document_query}])
                async def read(actor, phase, old, request, emit, authorize, context):
                    self.assertEqual(phase['question'], business_query)
                    self.assertEqual(phase['_query_intent']['query'], business_query)
                    self.assertNotIn('设备X', repr(phase['_answer_goals']))
                    self.assertEqual(old, [])
                    return self.business_result()
                self.engine._answer_candidate = AsyncMock(side_effect=read)
                with patch.object(classifier, 'resolve_query_intent', new=AsyncMock(return_value=routing)), \
                        patch.object(documents, 'answer_company', new=AsyncMock(return_value=self.combined_result())) as synth:
                    await self.answer(turn, [{'question': '设备X有哪些工单', 'scopes': ['D']}])
                self.assertEqual(synth.await_args.kwargs['document_query'], document_query)
                self.assertEqual(turn['question'], question)
                self.assertEqual(self.fixture.writes, [])

    async def test_semantic_knowledge_followup_uses_resolved_query_with_actual_candidate(self):
        effective = 'D楼昨天UPS巡检规程的适用条件'
        self.patch_intent(resolved('knowledge', question=effective, continued=True))
        turn = self.turn('改成昨天')
        history = [{'question': 'D楼今天UPS巡检规程', 'answer': '旧回答', 'scopes': ['D'],
                    'sources': [{'kind': 'company_knowledge'}]}]
        with patch.object(documents, 'answer_company', new_callable=AsyncMock,
                          return_value={'answer': '已按昨天条件核对。', 'sources': []}) as synth:
            await self.answer(turn, history)
        self.assertEqual(synth.await_args.args[3], effective)
        self.assertEqual(turn['question'], '改成昨天')
        self.assertEqual(self.transcript, [])

    async def test_semantic_general_intent_is_not_overridden_by_business_or_document_words(self):
        cases = [('包间湿度从91%下降到80%，下降了几个百分点？', '按你提供的读数，下降了11个百分点。'),
                 ('已有10条维修工单，完成了6条，完成率是多少？', '按你提供的数据，完成率为60%。'),
                 ('机房EOP这个缩写是什么意思？', 'EOP指紧急操作程序，是对术语的一般解释。'),
                 ('给我一个机房维修工单统计的虚拟示例', '以下仅是虚拟示例，不代表实际业务记录。')]
        for channel in ('', 'feishu:oc_Test'):
            self.actor['channel'] = channel
            self.current_actor = copy.deepcopy(self.actor)
            for question, answer in cases:
                with self.subTest(channel=channel, question=question):
                    self.transcript.clear()
                    self.events.clear()
                    self.steps = [answer]
                    with patch.object(classifier, 'resolve_query_intent', new=AsyncMock(return_value=resolved('general', question=question))), \
                         patch.object(documents, 'answer_company', new_callable=AsyncMock) as synthesis, \
                         patch.object(documents, 'automatic_company_query', new_callable=AsyncMock) as legacy:
                        result = await self.answer(self.turn(question))
                    self.assertEqual(result['answer'], safe_text(answer))
                    self.assertTrue(self.transcript)
                    synthesis.assert_not_awaited()
                    legacy.assert_not_awaited()
                    self.fixture.portal.get_knowledge.assert_not_called()
                    self.assertEqual(self.fixture.reads, [])
                    self.assertEqual(self.fixture.writes, [])

    async def test_observation_question_reclassified_before_any_business_model_or_tools(self):
        question = '包间湿度现在是91%，该用哪个EOP？'
        self.actor['modules'] = ['assistant', 'knowledge']
        for channel in ('', 'feishu:oc_Test'):
            with self.subTest(channel=channel):
                self.actor['channel'] = channel
                self.current_actor = copy.deepcopy(self.actor)
                self.fixture.model.complete.side_effect = [
                    json.dumps({'kind': 'mixed', 'query': question, 'continued': False,
                                'business_query': '查询现场湿度', 'document_query': question}),
                    json.dumps({'kind': 'knowledge', 'query': question, 'continued': False})]
                turn = self.turn(question, execution_lane=classifier.execution_lane(self.actor))
                with patch.object(documents, 'answer_company', new_callable=AsyncMock,
                                  return_value={'answer': '依据已给出的情况查阅规程。', 'sources': []}) as synth:
                    await self.answer(turn)
                synth.assert_awaited_once()
                self.assertEqual(synth.await_args.args[3], question)
                self.assertEqual(turn['question'], question)
                self.assertEqual(turn['_query_intent']['kind'], 'knowledge')
                self.assertEqual(self.transcript, [])
                self.assertEqual(self.fixture.writes, [])

    async def test_legacy_followup_keeps_current_request_once_when_classifier_skips(self):
        base = 'D楼现场巡检规程有哪些适用条件'
        history = [{'question': base, 'answer': '旧文档回答', 'scopes': ['D'],
                    'sources': [{'kind': 'company_knowledge'}]}]
        for followup in ('还有呢', '详细点', '那凭证呢'):
            with self.subTest(followup=followup), \
                    patch.object(classifier, 'resolve_query_intent', new_callable=AsyncMock,
                                 return_value=resolved('unknown', question=followup, attempts=0)), \
                    patch.object(documents, 'answer_company', new_callable=AsyncMock,
                                 return_value={'answer': '补充回答', 'sources': []}) as synth:
                turn = self.turn(followup)
                await self.answer(turn, history)
                query = synth.await_args.args[3]
                self.assertIn(base, query)
                self.assertEqual(query.count(followup), 1)
                self.assertEqual(documents.company_followup(query, history) or query, query)
                self.assertEqual(turn['question'], followup)
        self.assertEqual(self.transcript, [])

    async def test_explicit_skill_and_warmup_skip_classifier(self):
        classify = self.patch_intent()
        with patch.object(documents, 'answer_company', new_callable=AsyncMock,
                          return_value={'answer': '文档回答', 'sources': []}) as synth:
            await self.answer(self.turn('/公司知识库 D楼巡检规程'))
        self.assertEqual(synth.await_args.args[3], 'D楼巡检规程')
        self.engine._answer_candidate = AsyncMock(return_value={'answer': '', 'sources': []})
        await self.answer(warm_only=True)
        classify.assert_not_awaited()
        self.assertTrue(self.engine._answer_candidate.await_args.kwargs['warm_only'])

    async def test_mixed_clarification_and_private_business_reply_do_not_synthesize(self):
        self.patch_intent()
        for answer, fields in (('请明确设备范围。', {'clarification': {'options': []}}), (PRIVATE_REPLY, {})):
            with self.subTest(answer=answer):
                self.events.clear()
                self.engine._answer_candidate = AsyncMock(return_value=self.business_result(answer=answer, **fields))
                with patch.object(documents, 'answer_company', new_callable=AsyncMock) as synth:
                    result = await self.answer()
                synth.assert_not_awaited()
                self.assertEqual(result['answer'], answer)
                self.assertEqual(self.texts(), answer)
                if fields:
                    self.assertEqual(result['clarification'], fields['clarification'])

    async def test_private_document_reply_is_not_prefixed_with_business_answer(self):
        self.patch_intent()
        self.engine._answer_candidate = AsyncMock(return_value=self.business_result())
        with patch.object(documents, 'answer_company', new_callable=AsyncMock,
                          return_value={'answer': PRIVATE_REPLY, 'sources': []}):
            result = await self.answer()
        self.assertEqual(result['answer'], PRIVATE_REPLY)
        self.assertEqual(self.texts(), PRIVATE_REPLY)
        self.assertEqual(result['sources'], [])

    async def test_plan_from_mixed_business_phase_is_rejected(self):
        self.patch_intent()
        self.engine._answer_candidate = AsyncMock(return_value=self.business_result(plan={'id': 'forbidden'}))
        with patch.object(documents, 'answer_company', new_callable=AsyncMock) as synth:
            with self.assertRaises(AssistantError) as caught:
                await self.answer()
        self.assertEqual(caught.exception.status, 403)
        self.assertEqual(self.texts(), '')
        synth.assert_not_awaited()

    async def test_failed_model_read_still_looks_up_documents_with_explicit_unavailability(self):
        self.patch_intent()
        self.engine._answer_candidate = AsyncMock(side_effect=AssistantError('private provider error', 502, category='model_context'))
        with patch.object(documents, 'answer_company', new_callable=AsyncMock,
                          return_value=self.combined_result()) as synth:
            result = await self.answer()
        self.assertTrue(result['answer'].endswith(FINAL))
        self.assertIn('实时业务查询未完成', result['answer'])
        self.assertIs(result['knowledge_retrieval']['complete'], False)
        evidence = synth.await_args.kwargs['business_sources']
        self.assertEqual(len(evidence), 1)
        self.assertIs(evidence[0]['available'], False)
        self.assertIs(evidence[0]['data']['ok'], False)
        self.assertIs(evidence[0]['data']['complete'], False)
        self.assertNotIn('private provider error', str(evidence))
        self.assertEqual(synth.await_args.kwargs['document_query'], DOCUMENT_QUERY)

    async def test_failed_read_document_private_reply_stays_private_without_partial_prefix(self):
        self.patch_intent()
        self.engine._answer_candidate = AsyncMock(side_effect=AssistantError('context failure', 502, category='model_context'))
        with patch.object(documents, 'answer_company', new_callable=AsyncMock,
                          return_value={'answer': PRIVATE_REPLY, 'sources': []}):
            result = await self.answer()
        self.assertEqual(result['answer'], PRIVATE_REPLY)
        self.assertNotIn('knowledge_retrieval', result)

    async def test_failed_read_never_uses_document_recovery_to_bypass_permissions(self):
        self.patch_intent()
        for error in (AssistantError('forbidden', 403, category='model_context'),
                      AssistantError('business validation failed', 409)):
            self.engine._answer_candidate = AsyncMock(side_effect=error)
            with patch.object(documents, 'answer_company', new_callable=AsyncMock) as synth:
                with self.assertRaises(AssistantError):
                    await self.answer()
            synth.assert_not_awaited()

    async def test_document_synthesis_receives_failed_read_as_unavailable_not_an_empty_result(self):
        self.patch_intent()
        self.engine._answer_candidate = AsyncMock(side_effect=AssistantError('context failure', 502, category='model_context'))
        self.fixture.model.complete.return_value = json.dumps({
            'answer': '实时业务尚未核实，以下仅说明文档要求。[2]', 'queries': [], 'gaps': ['实时业务未核实']})
        result = await self.answer()
        packet = next(value for call in self.fixture.model.complete.call_args_list
                      if 'business_evidence' in (value := json.loads(call.args[0][1]['content'])))
        evidence = packet['business_evidence']['evidence']
        self.assertIs(evidence[0]['available'], False)
        self.assertEqual(evidence[0]['state'], 'unavailable')
        self.assertIn('现场规程', packet['documents'][0]['name'])
        self.assertIn('未核实', result['answer'])
        self.knowledge.search.assert_called_once()
        self.assertEqual(self.fixture.writes, [])

    async def test_document_failures_preserve_business_with_limitation_without_quality_profile(self):
        self.patch_intent()
        for failure in (AssistantError('文档服务暂不可用', 503), OSError('provider detail'), TimeoutError()):
            with self.subTest(failure=type(failure).__name__):
                self.events.clear()
                turn = self.turn()
                business = self.business_result(answer_review={'status': 'passed'}, _task_context={'contract': {}})
                self.engine._answer_candidate = AsyncMock(return_value=business)
                async def fail(*args, **kwargs):
                    turn['_actual_profile'] = copy.deepcopy(self.profile)
                    raise failure
                with patch.object(documents, 'answer_company', new_callable=AsyncMock, side_effect=fail):
                    result = await self.answer(turn)
                self.assertIn('本轮业务结果。[1]', result['answer'])
                self.assertGreater(len(result['answer']), len('本轮业务结果。[1]'))
                self.assertRegex(result['answer'], r'文档|资料|知识库')
                self.assertNotIn('provider detail', result['answer'])
                self.assertTrue(result['sources'])
                self.assertNotIn('_actual_profile', result)
                self.assertNotIn('_actual_profile', turn)
                self.assertEqual(result['answer_review']['reason'], 'source_unavailable')
                self.assertNotIn('_task_context', result)
                self.assertEqual(self.texts(), result['answer'])

    async def test_model_requested_documents_preserve_business_when_no_document_is_available(self):
        self.patch_intent(resolved('unknown'))
        business = self.business_result(_requested_company_query=DOCUMENT_QUERY)
        self.engine._answer_candidate = AsyncMock(return_value=business)
        async def unavailable(engine, actor, turn, question, emit, authorize, **kwargs):
            await emit('text', {'delta': '未取得适用文档。'})
            return {'answer': '未取得适用文档。', 'sources': [],
                    'answer_review': {'status': 'original_flow', 'reason': 'source_unavailable'}}
        with patch.object(documents, 'answer_company', new_callable=AsyncMock,
                          side_effect=unavailable):
            result = await self.answer()
        self.assertIn('本轮业务结果。[1]', result['answer'])
        self.assertIn('未取得适用文档', result['answer'])
        self.assertTrue(result['sources'])
        self.assertEqual(result['answer_review']['reason'], 'source_unavailable')
        self.assertEqual(self.texts(), result['answer'])

    async def test_model_requested_document_denial_never_returns_the_business_draft(self):
        self.patch_intent(resolved('unknown'))
        self.engine._answer_candidate = AsyncMock(return_value=self.business_result(_requested_company_query=DOCUMENT_QUERY))
        with patch.object(documents, 'answer_company', new_callable=AsyncMock,
                          side_effect=AssistantError('权限已撤销', 403)):
            with self.assertRaises(AssistantError) as denied:
                await self.answer()
        self.assertEqual(denied.exception.status, 403)
        self.assertEqual(self.texts(), '')

    async def test_model_requested_document_fallback_rechecks_permission_before_emitting(self):
        self.patch_intent(resolved('unknown'))
        self.engine._answer_candidate = AsyncMock(return_value=self.business_result(_requested_company_query=DOCUMENT_QUERY))
        async def unavailable(*args, **kwargs):
            self.current_actor['scopes'] = []
            return {'answer': '文档暂不可用。', 'sources': []}
        with patch.object(documents, 'answer_company', new_callable=AsyncMock, side_effect=unavailable):
            with self.assertRaises(AssistantError) as denied:
                await self.answer()
        self.assertEqual(denied.exception.status, 403)
        self.assertEqual(self.texts(), '')

    async def test_unavailable_documents_do_not_rewrite_reviewed_business_from_old_history(self):
        for routing in ('mixed', 'unknown'):
            with self.subTest(routing=routing):
                self.events.clear()
                self.patch_intent(resolved(routing))
                business = self.business_result(answer_review={'status': 'passed'})
                if routing == 'unknown':
                    business['_requested_company_query'] = DOCUMENT_QUERY
                self.engine._answer_candidate = AsyncMock(return_value=business)
                reviewer = SimpleNamespace(run=AsyncMock(return_value={
                    'answer': 'OLD_RULE_MUST_NOT_RETURN', 'sources': [], 'answer_review': {'status': 'corrected'}}))
                self.engine.answer_reviewer = reviewer
                history = [{'question': '旧规定', 'answer': 'OLD_RULE_MUST_NOT_RETURN', 'scopes': ['D'],
                            'sources': [{'kind': 'company_knowledge'}]}]
                with patch.object(documents, 'answer_company', new_callable=AsyncMock,
                        return_value={'answer': '未取得适用文档。', 'sources': []}):
                    result = await self.answer(history=history)
                reviewer.run.assert_not_awaited()
                self.assertIn('本轮业务结果', result['answer'])
                self.assertIn('未取得适用文档', result['answer'])
                self.assertNotIn('OLD_RULE_MUST_NOT_RETURN', result['answer'])
                self.assertEqual(result['answer_review']['reason'], 'source_unavailable')

    async def test_document_authorization_errors_propagate_without_emission(self):
        self.patch_intent()
        for status in (401, 403):
            with self.subTest(status=status):
                self.events.clear()
                self.engine._answer_candidate = AsyncMock(return_value=self.business_result())
                with patch.object(documents, 'answer_company', new_callable=AsyncMock,
                                  side_effect=AssistantError('资料授权已撤销', status)):
                    with self.assertRaises(AssistantError) as caught:
                        await self.answer()
                self.assertEqual(caught.exception.status, status)
                self.assertEqual(self.texts(), '')

    async def test_document_timeout_still_rechecks_scope_before_business_fallback(self):
        self.patch_intent()
        self.engine._answer_candidate = AsyncMock(return_value=self.business_result())
        async def fail(*args, **kwargs):
            self.current_actor['scopes'] = []
            raise TimeoutError()
        with patch.object(documents, 'answer_company', new_callable=AsyncMock, side_effect=fail):
            with self.assertRaises(AssistantError) as caught:
                await self.answer()
        self.assertEqual(caught.exception.status, 403)
        self.assertEqual(self.texts(), '')

    async def test_scope_revoked_after_business_phase_prevents_synthesis_and_output(self):
        self.patch_intent()
        async def read(*args, **kwargs):
            self.current_actor['scopes'] = []
            return self.business_result()
        self.engine._answer_candidate = AsyncMock(side_effect=read)
        with patch.object(documents, 'answer_company', new_callable=AsyncMock) as synth:
            with self.assertRaises(AssistantError) as caught:
                await self.answer()
        self.assertEqual(caught.exception.status, 403)
        self.assertEqual(self.texts(), '')
        synth.assert_not_awaited()

    async def test_scope_revoked_during_stateless_synthesis_prevents_combined_output(self):
        self.patch_intent()
        self.engine._answer_candidate = AsyncMock(return_value=self.business_result())
        def complete(*args, **kwargs):
            self.current_actor['scopes'] = []
            return json.dumps({'answer': FINAL, 'queries': [], 'gaps': []})
        self.fixture.model.complete.side_effect = complete
        with self.assertRaises(AssistantError) as caught:
            await self.answer()
        self.assertEqual(caught.exception.status, 403)
        self.assertEqual(self.texts(), '')

    async def test_native_bridge_denies_all_write_tools_in_the_mixed_phase(self):
        self.patch_intent()
        bridge = BusinessBridge()
        effects = []
        async def write():
            effects.append('unexpected write')
            return {'ok': True}
        async def read(actor, phase, history, request, emit, authorize, context):
            tools = {name: Tool(write) for name in ('lighthouse_prepare_business',
                'lighthouse_create_text_file', 'lighthouse_call_extension')}
            active = fixtures.FakeActive('mixed-session', tools)
            active.turn = phase
            bridge.active['fixture-token'] = active
            for name in tools:
                with self.assertRaises(AssistantError) as caught:
                    await bridge.call('fixture-token', {'session_key': 'mixed-session',
                        'call_id': name, 'tool': name, 'params': {}})
                self.assertEqual(caught.exception.status, 403)
            return self.business_result()
        self.engine._answer_candidate = AsyncMock(side_effect=read)
        with patch.object(documents, 'answer_company', new_callable=AsyncMock,
                          return_value=self.combined_result()):
            await self.answer()
        self.assertEqual(effects, [])

    async def test_scope_revoked_during_outer_review_prevents_combined_output(self):
        self.patch_intent()
        self.engine._answer_candidate = AsyncMock(return_value=self.business_result())
        async def review(actor, turn, history, result, authorize):
            self.current_actor['scopes'] = []
            return result
        self.engine.answer_reviewer = SimpleNamespace(run=AsyncMock(side_effect=review))
        with patch.object(documents, 'answer_company', new_callable=AsyncMock,
                          return_value=self.combined_result()):
            with self.assertRaises(AssistantError) as caught:
                await self.answer()
        self.assertEqual(caught.exception.status, 403)
        self.assertEqual(self.texts(), '')

    async def test_read_phase_advertises_only_read_tools_and_rejects_forged_write(self):
        self.patch_intent()
        self.steps = [('create_text_file', {'name': 'forbidden.txt', 'content': 'must not write'}),
            ('query', {'operation': {'api_id': 'GET /api/repair-management/records', 'params': {'scope': 'D'}}}),
            '已核对业务记录。[1]']
        with patch.object(self.fixture.portal.files, 'upload', side_effect=AssertionError('Write forbidden')) as upload, \
                patch.object(documents, 'answer_company', new_callable=AsyncMock,
                             return_value=self.combined_result()) as synth:
            await self.answer()
        self.assertTrue(self.tool_sets)
        for tools in self.tool_sets:
            self.assertLessEqual(tools, READ_REPAIR_TOOLS)
            self.assertNotIn('company_documents', tools)
            self.assertNotIn('prepare_business', tools)
            self.assertNotIn('create_text_file', tools)
        upload.assert_not_called()
        self.assertEqual(self.fixture.writes, [])
        self.assertEqual(self.fixture.store.list_documents('lighthouse_agent_plans'), [])
        self.assertEqual(self.fixture.reads, ['D'])
        self.assertEqual(synth.await_count, 1)

    async def test_business_phase_review_does_not_require_document_answer(self):
        self.patch_intent()
        self.steps = [
            ('company_documents', {'query': DOCUMENT_QUERY}),
            ('query', {'operation': {'api_id': 'GET /api/repair-management/records', 'params': {'scope': 'D'}}}),
            '已核对业务记录。[1]']
        reviews = []

        async def review(actor, turn, history, result, authorize, **kwargs):
            reviews.append((turn.get('_read_only_phase', False), kwargs.get('business_only', False)))
            return {**result, 'answer_review': {'status': 'passed', 'repairs': 0}}

        self.engine.answer_reviewer = SimpleNamespace(run=AsyncMock(side_effect=review))
        with patch.object(documents, 'answer_company', new_callable=AsyncMock,
                          return_value=self.combined_result()) as synth:
            await self.answer()
        self.assertIn((True, True), reviews)
        self.assertTrue(all(business_only for phase, business_only in reviews if phase))
        self.assertEqual(synth.await_count, 1)
        self.assertEqual(self.fixture.reads, ['D'])
        self.assertEqual(self.fixture.writes, [])

    async def test_exhausted_classifier_keeps_metadata_document_tool_and_stateless_synthesis(self):
        self.fixture.model.complete.side_effect = [OSError('first failure'), OSError('second failure'),
            json.dumps({'answer': FINAL, 'queries': [], 'gaps': []})]
        self.steps = [
            ('query', {'operation': {'api_id': 'GET /api/repair-management/records', 'params': {'scope': 'D'}}}),
            ('company_documents', {'query': DOCUMENT_QUERY}), 'HIDDEN_METADATA_DRAFT']
        turn = self.turn()
        with patch.object(documents, 'automatic_company_query', new_callable=AsyncMock,
                          side_effect=AssertionError('Third classifier forbidden')) as legacy, \
                patch.object(documents, 'answer_company', wraps=documents.answer_company) as synth:
            result = await self.answer(turn)
        legacy.assert_not_awaited()
        calls = self.fixture.model.complete.call_args_list
        self.assertEqual(sum(call.kwargs.get('max_tokens') == 1800 for call in calls), 2)
        self.assertEqual(turn['_query_intent']['attempts'], 2)
        self.assertEqual(turn['_query_intent']['kind'], 'unknown')
        self.assertEqual(turn['question'], QUESTION)
        self.assertEqual(synth.await_count, 1)
        self.assertTrue(synth.await_args.kwargs['business_sources'])
        self.assertEqual(synth.await_args.kwargs['document_query'], DOCUMENT_QUERY)
        self.assertIn('company_documents', self.tool_sets[0])
        self.assertTrue(DOCUMENT_BODY not in repr(self.transcript), 'Document body entered model transcript')
        self.assertIn(DOCUMENT_BODY, calls[-1].args[0][1]['content'])
        self.assertNotIn(DOCUMENT_BODY, repr(result))
        self.assertNotIn(DOCUMENT_BODY, repr(self.events))
        self.assertNotIn('HIDDEN_METADATA_DRAFT', self.texts())
        self.assertEqual(result['answer'], FINAL)
        self.assertEqual(self.fixture.writes, [])

    async def test_prior_company_answer_is_not_reimported_into_native_prompt(self):
        self.patch_intent()
        history = [{'question': '旧资料问题', 'answer': 'OLD_DOCUMENT_BODY', 'scopes': ['D'],
            'sources': [{'kind': 'company_knowledge', 'data': {'text': 'OLD_DOCUMENT_BODY'}}]}]
        before = copy.deepcopy(history)
        agent = fixtures.FakeAgent(None)
        self.engine.agent_factory = lambda *args, **kwargs: agent
        with patch.object(documents, 'answer_company', new_callable=AsyncMock,
                          return_value=self.combined_result()):
            await self.answer(history=history)
        self.assertTrue(agent.ran)
        self.assertEqual(history, before)
        self.assertTrue('OLD_DOCUMENT_BODY' not in repr((agent.prompt, agent.message_history)),
                        'Old company answer was reimported into native agent input')

    async def test_actual_public_topic_context_does_not_copy_document_answer_into_native_prompt(self):
        from lan_bitable_template_portal.feishu_assistant import Inbox
        inbox = Inbox(Path(self.fixture.tmp.name) / 'topic-inbox.sqlite3')
        message = {'chat_id': 'oc_Test', 'root_id': 'om_Root', 'message_id': 'om_First',
            'chat_type': 'group', 'app_id': 'fixture-app', 'question': 'D楼现场规程的适用条件'}
        inbox.remember_public_reply(message, {'text': DOCUMENT_BODY,
            'thread_route': {'chat_id': 'oc_Test', 'root_id': 'om_Root'},
            'delivery_access': {'scopes': ['D'], 'modules': [], 'is_admin': False}})
        context = inbox.topic_context(message, self.actor, details=True)
        self.assertIn(DOCUMENT_BODY, context['context'])
        self.actor.update(channel='feishu:oc_Test', _feishu_topic={'id': 'oc_Test:om_Root', **context})
        self.current_actor = copy.deepcopy(self.actor)
        self.patch_intent()
        agent = fixtures.FakeAgent(None)
        self.engine.agent_factory = lambda *args, **kwargs: agent
        with patch.object(documents, 'answer_company', new_callable=AsyncMock,
                          return_value=self.combined_result()):
            await self.answer(self.turn(feishu_topic='oc_Test:om_Root', execution_lane='feishu:oc_Test:oc_Test:om_Root'))
        self.assertTrue(agent.ran)
        self.assertTrue(DOCUMENT_BODY not in repr((agent.prompt, agent.message_history)),
                        'Authorized public topic context copied a prior document answer into native input')

    async def test_unknown_after_one_attempt_also_skips_legacy_classifier(self):
        self.fixture.model.settings.return_value['models'] = self.fixture.model.settings.return_value['models'][:1]
        self.fixture.model.complete.side_effect = OSError('only configured classifier failed')
        self.steps = [('query', {'operation': {'api_id': 'GET /api/repair-management/records', 'params': {'scope': 'D'}}}),
                      '已核对业务记录。[1]']
        with patch.object(documents, 'automatic_company_query', new_callable=AsyncMock,
                          side_effect=AssertionError('Legacy classifier forbidden')) as legacy:
            turn = self.turn()
            await self.answer(turn)
        legacy.assert_not_awaited()
        self.assertEqual(self.fixture.model.complete.call_count, 1)
        self.assertEqual(turn['_query_intent']['attempts'], 1)


if __name__ == '__main__':
    unittest.main()
