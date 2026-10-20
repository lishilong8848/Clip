"""Shared web/Feishu task evaluations with real tool schemas and fake read APIs."""
import asyncio
import copy
import json
import sys
import unittest
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fastapi import FastAPI
from pydantic_ai.models.function import DeltaToolCall, FunctionModel
from openclaw_service.assistant.lighthouse_api import PortalAPICatalog
from openclaw_service.assistant.lighthouse_model import LighthouseModel
from openclaw_service.assistant.lighthouse_context import task_context
from openclaw_service.assistant.lighthouse_queries import query_statistics
from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_review import AnswerReviewer


class IndependentReviewTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.actor = {'id': 'fixture-owner', 'scopes': ['D'], 'modules': ['assistant', 'learning']}
        self.profile = {'id': 'writer', 'model': 'writer', 'endpoint': 'https://fixture.example/chat/completions', 'key_cipher': 'fixture'}
        self.model = SimpleNamespace(settings=lambda: {'enabled': True, 'models': [{'id': 'checker', 'configured': True}]},
            profile=lambda _: {**self.profile, 'id': 'checker', 'model': 'checker'}, complete=Mock())
        self.assistant = SimpleNamespace(model_for=lambda _: self.model)
        self.reviewer = AnswerReviewer(self.assistant)
        self.turn = {'question': '人名和人数都告诉我', '_profile': self.profile}
        self.result = {'answer': '共有2人。', 'sources': [{'number': 1, 'available': True, 'scopes': ['D'],
            'data': {'participants': [{'name': '测试甲'}, {'name': '测试乙'}], 'complete': True}}]}
        self.authorize = AsyncMock(side_effect=lambda: copy.deepcopy(self.actor))
        self.passed = json.dumps({'passed': True, 'issues': []})
        self.rejected = json.dumps({'passed': False, 'issues': ['缺少已查到的两个人名']})

    async def test_independent_review_uses_original_question_and_repairs_without_tools(self):
        self.model.complete.side_effect = [self.rejected, '共2人：测试甲、测试乙。', self.passed]
        history = [{'question': '其他话题的私有问题', 'feishu_topic': 'other', 'scopes': ['D']},
                   {'question': '旧范围资料', 'scopes': ['A']}]
        result = await self.reviewer.run(self.actor, self.turn, history, self.result, self.authorize)
        self.assertEqual(result['answer_review'], {'status': 'corrected', 'repairs': 1})
        self.assertIn('测试乙', result['answer'])
        calls = self.model.complete.call_args_list
        self.assertEqual([call.kwargs['profile']['model'] for call in calls], ['checker', 'writer', 'checker'])
        packet = json.loads(calls[0].args[0][1]['content'])
        self.assertEqual(packet['question'], self.turn['question'])
        self.assertEqual(packet['history'], [])
        self.assertNotIn('key_cipher', str(packet))
        self.assertTrue(all('tools' not in call.kwargs and 'messages' not in call.kwargs for call in calls))
        self.assertNotIn('测试乙', self.result['answer'])

    async def test_two_repairs_are_bounded_and_unchecked_candidate_is_not_returned(self):
        self.model.complete.side_effect = [self.rejected, 'still incomplete', self.rejected, 'also incomplete', self.rejected]
        result = await self.reviewer.run(self.actor, self.turn, [], self.result, self.authorize)
        self.assertEqual(result['answer_review'], {'status': 'needs_review', 'repairs': 2})
        self.assertEqual(self.model.complete.call_count, 5)
        self.assertNotIn('still incomplete', result['answer'])
        self.assertNotIn('also incomplete', result['answer'])

    async def test_followup_review_reuses_only_authorized_task_context(self):
        self.turn.update(question='改成上周', _task_context={'version': 1, 'owner': self.actor['id'], 'continued': True,
            'topic_id': '', 'scopes': ['D'], 'modules': ['learning'], 'query': '上周D楼学练参与者及人数'})
        self.model.complete.return_value = self.passed
        await self.reviewer.run(self.actor, self.turn, [], self.result, self.authorize)
        packet = json.loads(self.model.complete.call_args.args[0][1]['content'])
        self.assertEqual(packet['question'], '改成上周')
        self.assertEqual(packet['effective_question'], '上周D楼学练参与者及人数')
        self.turn['_task_context']['topic_id'] = 'other-topic'
        await self.reviewer.run(self.actor, self.turn, [], self.result, self.authorize)
        packet = json.loads(self.model.complete.call_args.args[0][1]['content'])
        self.assertNotIn('effective_question', packet)

    async def test_unavailable_or_malformed_reviewer_never_marks_passed(self):
        for value in (OSError('fixture offline'), 'not JSON', '{"passed":true,"issues":["contradiction"]}'):
            self.model.complete.reset_mock(side_effect=True)
            self.model.complete.side_effect = value if isinstance(value, Exception) else None
            self.model.complete.return_value = value
            result = await self.reviewer.run(self.actor, self.turn, [], self.result, self.authorize)
            self.assertEqual(result['answer_review']['status'], 'unavailable')
            self.assertIn('尚未经独立核验', result['answer'])

    async def test_confirmations_and_security_refusals_do_not_get_model_rewritten(self):
        for result in ({**self.result, 'plan': {'status': 'submitted', 'id': 'original-job'}},
                       {**self.result, 'clarification': {'options': []}}):
            checked = await self.reviewer.run(self.actor, self.turn, [], result, self.authorize)
            self.assertEqual(checked['answer'], result['answer'])
            self.assertEqual(checked['answer_review']['status'], 'original_flow')
        self.model.complete.assert_not_called()

    async def test_permission_revocation_stops_before_rewrite_or_delivery(self):
        self.model.complete.return_value = self.rejected
        self.authorize.side_effect = [copy.deepcopy(self.actor), copy.deepcopy(self.actor), {**self.actor, 'scopes': []}]
        with self.assertRaises(AssistantError):
            await self.reviewer.run(self.actor, self.turn, [], self.result, self.authorize)
        self.assertEqual(self.model.complete.call_count, 1)

    async def test_known_bad_answer_is_not_released_when_rewrite_fails(self):
        self.model.complete.side_effect = [self.rejected, TimeoutError()]
        result = await self.reviewer.run(self.actor, self.turn, [], self.result, self.authorize)
        self.assertEqual(result['answer_review']['status'], 'needs_review')
        self.assertNotEqual(result['answer'], self.result['answer'])

    async def test_oversize_question_is_not_silently_truncated_and_approved(self):
        self.turn['question'] = '问' * 21000
        result = await self.reviewer.run(self.actor, self.turn, [], self.result, self.authorize)
        self.assertEqual(result['answer_review']['status'], 'unavailable')
        self.model.complete.assert_not_called()

    async def test_shared_engine_delivers_only_reviewed_text_without_replaying_candidate(self):
        for channel in ('', 'feishu:oc_fixture'):
            with self.subTest(channel=channel):
                self.model.complete.side_effect = [self.rejected, '共2人：测试甲、测试乙。', self.passed]
                actor = {**self.actor, 'channel': channel}
                engine = LighthouseModel(SimpleNamespace(assistant=self.assistant), model_factory=Mock(), answer_reviewer=self.reviewer)
                async def candidate(*args, **kwargs):
                    await args[4]('text', {'delta': self.result['answer']})
                    return copy.deepcopy(self.result)
                engine._answer_candidate = AsyncMock(side_effect=candidate)
                emitted = []
                async def emit(kind, data):
                    emitted.append((kind, data))
                result = await engine.answer(actor, self.turn, [], None, emit, self.authorize, {})
                engine._answer_candidate.assert_awaited_once()
                texts = [data['delta'] for kind, data in emitted if kind == 'text']
                self.assertEqual(texts, [result['answer']])
                self.assertIn('测试乙', result['answer'])
                self.assertNotIn(self.result['answer'], texts)


class SnapshotStatisticsTests(unittest.TestCase):
    def test_statistics_keep_duplicates_missing_values_and_partial_scope_explicit(self):
        data = {'complete': True, 'items': [{'person': {'id': 'a'}, 'value': .1},
            {'person': {'id': 'a'}, 'value': .2}, {'person': {'id': 'b'}}]}
        self.assertEqual(query_statistics(data, ['items'])['value'], 3)
        self.assertEqual(query_statistics(data, ['items'], 'distinct', ['person', 'id'])['value'], 2)
        result = query_statistics(data, ['items'], 'sum', ['value'])
        self.assertEqual(result['value'], .3)
        self.assertEqual(result['missing_count'], 1)
        self.assertFalse(result['complete'])
        data['has_more'] = True
        self.assertFalse(query_statistics(data, ['items'])['complete'])
        self.assertIsNone(query_statistics({'items': [{}]}, ['items'], 'mean', ['value'])['value'])
        self.assertEqual(query_statistics(data, ['items'], 'group_count', ['person', 'id'])['groups'],
                         [{'value': 'a', 'count': 2}, {'value': 'b', 'count': 1}])

    def test_statistics_cannot_disclose_sensitive_paths_or_guess_numeric_strings(self):
        for field in ('password', 'api_key', 'mobile', 'signature', '_private', 'relative_path'):
            with self.subTest(field=field), self.assertRaises(AssistantError):
                query_statistics({'items': [{field: 'hidden'}]}, ['items'], 'distinct', [field])
        for value in ('30%', '1.5', True, float('nan'), float('inf')):
            with self.subTest(value=value), self.assertRaises(AssistantError):
                query_statistics({'items': [{'value': value}]}, ['items'], 'sum', ['value'])
        with self.assertRaises(AssistantError):
            query_statistics({'items': [{}] * 5001}, ['items'])


class IntelligenceEvaluationTests(unittest.IsolatedAsyncioTestCase):
    async def test_review_rejects_fabricated_sources_without_triggering_business_reads(self):
        for channel in ('', 'feishu:oc_evaluation'):
            step = 0
            async def script(messages, info):
                nonlocal step
                step += 1
                if step == 1:
                    answer, goal = '资料已核实。', {'description': '核实资料', 'kind': 'data',
                        'status': 'complete', 'query_refs': ['not-an-authorized-receipt']}
                elif step == 2:
                    self.assertFalse(messages[-1].parts[0].content['ready'])
                    answer, goal = '尚无可核验来源，不将其认作已核实资料。', {
                        'description': '核实资料', 'kind': 'data', 'status': 'blocked', 'limitation': '没有来源回执'}
                else:
                    self.assertTrue(messages[-1].parts[0].content['ready'])
                    yield 'done'
                    return
                yield {0: DeltaToolCall(name='review_answer', json_args=json.dumps({'answer': answer, 'goals': [goal]}))}
            result, calls, _ = await self.evaluate(channel, '帮我核实这份资料', script, lambda *_: {})
            self.assertIn('尚无可核验来源', result['answer'])
            self.assertEqual(calls, [])

    async def test_snapshot_statistics_and_answer_completion_work_in_both_channels(self):
        for channel in ('', 'feishu:oc_evaluation'):
            step, reference = 0, ''
            async def script(messages, info):
                nonlocal step, reference
                step += 1
                if step == 1:
                    self.assertIn('按整轮语义', info.instructions)
                    yield {0: DeltaToolCall(name='query', json_args=json.dumps({'complete': True,
                        'operation': {'api_id': 'GET /api/assistant/work-orders', 'params': {'scope': 'D'}}}))}
                elif step == 2:
                    receipt = messages[-1].parts[0].content
                    reference = receipt['query_ref']
                    yield {0: DeltaToolCall(name='query_statistics', json_args=json.dumps({
                        'query_ref': receipt['query_ref'], 'path': ['items'],
                        'operation': 'distinct', 'field': ['person', 'id']}))}
                elif step == 3:
                    self.assertEqual(messages[-1].parts[0].content['value'], 2)
                    yield {0: DeltaToolCall(name='review_answer', json_args=json.dumps({
                        'answer': '共有2人：测试甲、测试乙。', 'goals': [{
                            'description': '核实工单参与者并给出人数及姓名', 'kind': 'data', 'status': 'complete',
                            'query_refs': [reference]}]}))}
                else:
                    self.assertTrue(messages[-1].parts[0].content['ready'])
                    yield '已完成。'
            def responder(scope, *_):
                return {'ok': True, 'data': {'complete': True, 'items': [
                    {'id': '1', 'person': {'id': 'a', 'name': '测试甲'}},
                    {'id': '2', 'person': {'id': 'a', 'name': '测试甲'}},
                    {'id': '3', 'person': {'id': 'b', 'name': '测试乙'}}], 'total': 3}}
            result, calls, events = await self.evaluate(channel, 'D楼工单有哪些人员参与，共多少人？', script, responder)
            self.assertEqual(result['answer'], '共有2人:测试甲、测试乙。')
            self.assertEqual(len(calls), 1)
            self.assertEqual(step, 4)
            self.assertEqual(''.join(data['delta'] for kind, data in events if kind == 'text'), result['answer'])

    async def test_business_report_request_cannot_claim_a_file_without_an_artifact(self):
        step = 0
        async def script(messages, info):
            nonlocal step
            step += 1
            if step == 1:
                yield '已生成工单统计Excel文件。'
            else:
                yield '缺少可核验资料，请提供工单明细后生成文件。'
        result, calls, _ = await self.evaluate('', '生成D楼工单统计Excel文件', script, lambda *_: {})
        self.assertIn('缺少', result['answer'])
        self.assertEqual(step, 2)
        self.assertEqual(calls, [])
        self.assertNotIn('output_files', result)

    async def evaluate(self, channel, question, script, responder, *, scopes=('D',), turn=None, model_factory=None):
        app, calls = FastAPI(), []
        @app.get('/api/assistant/work-orders')
        @app.get('/api/learning/papers')
        async def records(scope: str, state: str = 'active', page: int = 1, page_size: int = 20):
            calls.append({'scope': scope, 'state': state, 'page': page, 'page_size': page_size})
            value = responder(scope, page, page_size)
            return await value if asyncio.iscoroutine(value) else value
        @app.post('/api/workbench-actions')
        async def write():
            self.fail('Read evaluation must never execute a business write')
        portal = SimpleNamespace(
            assistant=SimpleNamespace(model=Mock(), _state=lambda _: {'turns': []}),
            catalog=PortalAPICatalog(app), files=SimpleNamespace(context=lambda *_: [], image_parts=lambda *_: []),
            _public_references=lambda result, _: result, _source_url=lambda *_: '/workbench-lite')
        async def invoke(actor, operation, request):
            self.assertIn(operation['api_id'], {'GET /api/assistant/work-orders', 'GET /api/learning/papers'})
            self.assertIn(operation['params']['scope'], actor['scopes'])
            return await records(**operation['params'])
        portal._invoke = invoke
        @asynccontextmanager
        async def factory(*_):
            yield FunctionModel(stream_function=script)
        actor = {'id': 'evaluation-user', 'scopes': list(scopes), 'learning_scopes': list(scopes),
                 'modules': ['assistant', 'polling', 'learning'], 'channel': channel}
        async def auth():
            return copy.deepcopy(actor)
        events = []
        async def emit(kind, data):
            events.append((kind, data))
        engine = LighthouseModel(portal, model_factory=model_factory or factory)
        result = await engine.answer(actor, {'question': question, '_profile': {'name': 'fixture', 'model': 'fixture'},
                                           **(turn or {})}, [], None, emit, auth, {})
        return result, calls, events

    async def test_full_list_is_collected_once_and_remainder_is_read_from_snapshot(self):
        for channel in ('', 'feishu:oc_evaluation'):
            step = 0
            async def script(messages, info):
                nonlocal step
                step += 1
                if step == 1:
                    yield {0: DeltaToolCall(name='query', json_args=json.dumps({'complete': True,
                        'operation': {'api_id': 'GET /api/assistant/work-orders',
                                      'params': {'scope': 'D', 'state': 'active', 'page_size': 20}}}))}
                elif step == 2:
                    response = messages[-1].parts[0].content
                    self.assertEqual(response['query_state'], 'found')
                    self.assertTrue(response['data']['complete'])
                    self.assertEqual(len(response['data']['items']), 40)
                    yield {0: DeltaToolCall(name='read_query', json_args=json.dumps({
                        'query_ref': response['query_ref'], 'path': ['items'], 'offset': 40, 'limit': 20}))}
                else:
                    response = messages[-1].parts[0].content
                    self.assertEqual(response['data']['loaded_count'], 45)
                    self.assertEqual(len(response['data']['items']), 5)
                    yield 'D楼当前共有45项工单，已读取完整名单。'
            def responder(scope, page, page_size):
                start = (page - 1) * page_size
                return {'ok': True, 'data': {'items': [{'id': str(i), 'scope': scope, 'title': '测试工单'}
                        for i in range(start, min(45, start + page_size))],
                        'total': 45, 'page': page, 'page_size': page_size}}
            result, calls, events = await self.evaluate(channel, '列出D楼所有工单明细', script, responder)
            self.assertIn('45项', result['answer'])
            self.assertEqual([call['page'] for call in calls], [1, 2, 3])
            self.assertEqual(step, 3)
            self.assertNotIn('plan', result)
            self.assertEqual(''.join(data['delta'] for kind, data in events if kind == 'text'), result['answer'])

    async def test_complete_query_does_not_mistake_failed_page_for_zero(self):
        for channel in ('', 'feishu:oc_evaluation'):
            step = 0
            async def script(messages, info):
                nonlocal step
                step += 1
                if step == 1:
                    yield {0: DeltaToolCall(name='query', json_args=json.dumps({'complete': True,
                        'operation': {'api_id': 'GET /api/assistant/work-orders',
                                      'params': {'scope': 'D', 'page_size': 20}}}))}
                else:
                    response = messages[-1].parts[0].content
                    self.assertEqual(response['query_state'], 'partial')
                    self.assertFalse(response['data']['complete'])
                    yield '已取得20项，后续分页暂不可用，完整数量未知。'
            def responder(scope, page, page_size):
                if page == 2:
                    return {'ok': False, 'status': 503, 'error': 'fixture unavailable'}
                return {'ok': True, 'data': {'items': [{'id': str(i), 'scope': scope} for i in range(20)],
                        'total': 45, 'page': page, 'page_size': page_size}}
            result, calls, _ = await self.evaluate(channel, '查询D楼工单，提供全部记录', script, responder)
            self.assertIn('完整数量未知', result['answer'])
            self.assertEqual(len(calls), 2)

    async def test_cross_building_fanout_is_at_most_three_and_preserves_scope_order(self):
        active, peak, step = 0, 0, 0
        async def responder(scope, page, page_size):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            try:
                await asyncio.sleep(.01)
                return {'ok': True, 'data': {'items': [], 'total': 0, 'scope': scope}}
            finally:
                active -= 1
        async def script(messages, info):
            nonlocal step
            step += 1
            if step == 1:
                yield {0: DeltaToolCall(name='query', json_args=json.dumps({
                    'operation': {'api_id': 'GET /api/learning/papers', 'params': {'scope': 'ALL'}}}))}
            else:
                self.assertEqual([row['scope'] for row in messages[-1].parts[0].content['data']['buildings']], list('ABCDH'))
                yield '所选5栋楼本次筛选均没有题单。'
        result, calls, _ = await self.evaluate('feishu:oc_evaluation', '查询各楼题单明细', script, responder, scopes=tuple('ABCDH'))
        self.assertEqual(peak, 3)
        self.assertEqual(len(calls), 5)
        self.assertNotIn('plan', result)

    async def test_partial_full_claim_is_corrected_before_any_text_is_delivered(self):
        for channel in ('', 'feishu:oc_evaluation'):
            step = 0
            async def script(messages, info):
                nonlocal step
                step += 1
                if step == 1:
                    yield {0: DeltaToolCall(name='query', json_args=json.dumps({
                        'operation': {'api_id': 'GET /api/assistant/work-orders', 'params': {'scope': 'D'}}}))}
                elif step == 2:
                    yield '以下是完整名单，共1项。'
                else:
                    yield '本页仅取得1项，尚未查全，不能确认完整名单。'
            def responder(scope, page, page_size):
                return {'ok': True, 'data': {'items': [{'id': 'fixture', 'scope': scope}], 'total': 10, 'has_more': True}}
            result, calls, events = await self.evaluate(channel, '查询D楼工单明细', script, responder)
            delivered = ''.join(data['delta'] for kind, data in events if kind == 'text')
            self.assertIn('尚未查全', result['answer'])
            self.assertNotIn('以下是完整名单', delivered)
            self.assertEqual(len(calls), 1)

    async def test_fanout_uses_native_pagination_not_just_display_truncation(self):
        step = 0
        async def script(messages, info):
            nonlocal step
            step += 1
            if step == 1:
                yield {0: DeltaToolCall(name='query', json_args=json.dumps({'operation': {
                    'api_id': 'GET /api/learning/papers', 'params': {'scope': 'ALL'}}}))}
            elif step == 2:
                receipt = messages[-1].parts[0].content
                self.assertFalse(receipt['data']['complete'])
                self.assertTrue(all(row['query_state'] == 'partial' for row in receipt['data']['buildings']))
                yield '已读取全部记录，以下是完整名单。'
            else:
                yield '各楼还有后续页，尚未查全，不能确认完整名单。'
        def responder(scope, *_):
            raw = {'items': [{'id': scope + str(i), 'scope': scope} for i in range(50)], 'total': 100, 'has_more': True}
            return {'ok': True, '_raw': raw, 'data': raw, 'truncated': True}
        result, calls, events = await self.evaluate('feishu:oc_evaluation', '查询各楼题单明细', script, responder, scopes=tuple('ABCDH'))
        self.assertIn('尚未查全', result['answer'])
        self.assertEqual(len(calls), 5)
        self.assertNotIn('已读取全部记录', ''.join(data['delta'] for kind, data in events if kind == 'text'))

    async def test_skipped_page_cannot_be_presented_as_a_complete_list(self):
        step = 0
        async def script(messages, info):
            nonlocal step
            step += 1
            if step in (1, 2):
                yield {0: DeltaToolCall(name='query', json_args=json.dumps({'operation': {
                    'api_id': 'GET /api/assistant/work-orders', 'params': {'scope': 'D', 'page': 1 if step == 1 else 3}}}))}
            elif step == 3:
                self.assertEqual(messages[-1].parts[0].content['query_state'], 'partial')
                yield '以下是完整名单，共60项。'
            else:
                yield '仅取得第1页和第3页，缺少第2页，尚未查全。'
        def responder(scope, page, page_size):
            start = (page - 1) * page_size
            return {'ok': True, 'data': {'items': [{'id': str(i), 'scope': scope} for i in range(start, start + page_size)],
                    'total': 60, 'page': page, 'has_more': page < 3}}
        result, calls, events = await self.evaluate('', '查询D楼工单明细', script, responder)
        self.assertIn('缺少第2页', result['answer'])
        self.assertEqual([call['page'] for call in calls], [1, 3])
        self.assertNotIn('完整名单', ''.join(data['delta'] for kind, data in events if kind == 'text'))

    async def test_selected_record_uses_fresh_details_not_previous_values_or_count_shortcut(self):
        for channel in ('', 'feishu:oc_evaluation'):
            actor = {'id': 'evaluation-user', 'scopes': ['D'], 'modules': ['assistant', 'polling']}
            previous = {'question': '当前未完成工单有几项？', 'scopes': ['D'],
                        'sources': [{'data': {'items': [{'id': 'one', 'title': '工单一'},
                            {'id': 'two', 'title': '工单二', 'progress': '旧进度不应继承'}]}}]}
            structured = task_context('第二条缺什么？', previous, actor)
            step = 0
            async def script(messages, info):
                nonlocal step
                step += 1
                if step == 1:
                    self.assertIn('工单二', str(messages))
                    self.assertNotIn('旧进度不应继承', str(messages))
                    yield {0: DeltaToolCall(name='query', json_args=json.dumps({
                        'operation': {'api_id': 'GET /api/assistant/work-orders', 'params': {'scope': 'D'}}}))}
                else:
                    self.assertIn('waiting_photo', str(messages[-1].parts[0].content))
                    yield '第二条工单当前缺少照片，仍需在原工单中补充。'
            result, calls, _ = await self.evaluate(channel, '第二条缺什么？', script,
                lambda scope, *_: {'ok': True, 'data': {'items': [{'id': 'two', 'scope': scope, 'status': 'waiting_photo'}]}},
                turn={'_task_context': structured})
            self.assertIn('缺少照片', result['answer'])
            self.assertEqual(len(calls), 1)
            self.assertNotIn('plan', result)

    async def test_typed_notice_tool_keeps_notice_count_and_action_count_separate(self):
        for channel in ('', 'feishu:oc_evaluation'):
            step = 0
            async def script(messages, info):
                nonlocal step
                step += 1
                if step == 1:
                    yield {0: DeltaToolCall(name='notice_records', json_args=json.dumps({
                        'criteria': {'status': 'sent', 'work_types': ['maintenance'],
                                     'start_date': '2026-10-19', 'end_date': '2026-10-19'}}))}
                else:
                    receipt = messages[-1].parts[0].content
                    self.assertEqual((receipt['count'], receipt['send_count']), (1, 2))
                    yield '本次为1条维保通告，成功发送2次，二者不能混算。'
            reader = AsyncMock(return_value={'kind': 'notice_query', 'complete': True,
                'count': 1, 'send_count': 2, 'unit': 'distinct_notices', 'groups': []})
            with patch('openclaw_service.assistant.lighthouse_queries.collect_notices', reader):
                result, calls, _ = await self.evaluate(channel, '核对通告分类统计口径并说明来源', script, lambda *_: {})
            self.assertEqual(reader.await_count, 1)
            self.assertEqual(reader.call_args.args[1].status, 'sent')
            self.assertIn('成功发送2次', result['answer'])
            self.assertEqual(calls, [])


if __name__ == '__main__':
    unittest.main()
