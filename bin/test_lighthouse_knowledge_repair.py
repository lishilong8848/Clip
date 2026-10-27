"""Read-repair contract tests use isolated evidence and never call cloud services."""
import copy
import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from openclaw_service.assistant.lighthouse_ai import AssistantError, PRIVATE_REPLY
from openclaw_service.assistant.lighthouse_knowledge_answer import answer_company, _candidate, _previous_candidate


def hit(identity, text, **extra):
    return {'document_id': identity, 'version': 1, 'ordinal': 0, 'name': identity + '.txt',
            'text': text, 'location': '第1段', 'url': '/knowledge-base?document=' + identity, **extra}


def candidate(answer='', queries=(), gaps=()):
    return json.dumps({'answer': answer, 'queries': list(queries), 'gaps': list(gaps)}, ensure_ascii=False)


class KnowledgeRepair(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(patch('socket.socket.connect', side_effect=AssertionError('Network forbidden')))
        self.actor = {'id': 'user-a', 'scopes': ['A'], 'modules': ['knowledge']}
        self.profile = {'endpoint': 'https://fixture.invalid', 'model': 'text'}
        self.first = hit('candidate', '该流程只适用于设备甲。')
        self.second = hit('applicable', '设备乙应先核实现场状态，再联系负责人员。')
        self.kb = SimpleNamespace(search=Mock(return_value={'items': [self.first], 'warning': ''}),
                                  evidence_current=Mock(return_value=True))
        self.model = Mock()
        self.engine = SimpleNamespace(portal=SimpleNamespace(get_knowledge=lambda: self.kb),
                                      assistant=SimpleNamespace(model_for=lambda actor: self.model))
        self.emit = AsyncMock()
        self.auth = AsyncMock(return_value=self.actor)

    async def answer(self, question='设备乙异常应如何处理？'):
        return await answer_company(self.engine, self.actor, {'question': question, '_profile': self.profile},
                                    question, self.emit, self.auth)

    async def test_irrelevant_hits_are_retrieved_again_and_only_final_answer_is_emitted(self):
        self.kb.search.side_effect = [{'items': [item], 'warning': ''} for item in (self.first, self.second)]
        self.model.complete.side_effect = [candidate(queries=['设备乙应急规程'], gaps=['设备不符']),
                                          candidate('先核实现场状态，再联系负责人员。[1]')]
        result = await self.answer()
        self.assertEqual(self.kb.search.call_count, 2)
        self.assertEqual(result['sources'][0]['data']['document_id'], 'applicable')
        self.assertEqual(result['knowledge_retrieval']['repair_rounds'], 1)
        texts = [call.args[1]['delta'] for call in self.emit.call_args_list if call.args[0] == 'text']
        self.assertEqual(texts, [result['answer']])
        self.assertNotIn('设备不符', result['answer'])

    async def test_legacy_no_applicable_marker_also_repairs(self):
        self.kb.search.side_effect = [{'items': [item], 'warning': ''} for item in (self.first, self.second)]
        self.model.complete.side_effect = ['[NO_APPLICABLE_DOCUMENT]', '{"keywords":["设备乙流程"]}',
                                          '先核实现场状态。[1]']
        result = await self.answer()
        self.assertIn('先核实', result['answer'])
        self.assertEqual(result['knowledge_retrieval']['repair_rounds'], 1)

    async def test_same_evidence_under_different_query_does_not_repeat_generation(self):
        self.model.complete.return_value = candidate(queries=['设备乙处理步骤'], gaps=['设备不匹配'])
        result = await self.answer()
        self.model.complete.assert_called_once()
        self.assertEqual(self.kb.search.call_count, 2)
        self.assertEqual(result['sources'], [])

    async def test_incomplete_generation_retries_same_evidence_for_web_and_feishu(self):
        for channel in ('', 'feishu:topic'):
            for failed in ('{"answer":"未完成', AssistantError('output limit', 502, category='model_output')):
                with self.subTest(channel=channel, failed=type(failed).__name__):
                    self.actor['channel'] = channel
                    self.model.complete.reset_mock()
                    self.kb.search.reset_mock()
                    self.emit.reset_mock()
                    self.model.complete.side_effect = [failed, candidate('该流程只适用于设备甲。[1]')]
                    result = await self.answer()
                    self.kb.search.assert_called_once()
                    self.assertEqual(self.model.complete.call_count, 2)
                    self.assertTrue(result['knowledge_retrieval']['complete'])
                    self.assertNotIn('适用对象或条件不符', result['answer'])
                    packets = [json.loads(call.args[0][1]['content']) for call in self.model.complete.call_args_list]
                    self.assertEqual(packets[0]['documents'][0]['text'], packets[1]['documents'][0]['text'])
                    self.assertTrue(packets[1]['review_feedback'])
                    self.assertIsNone(self.model.complete.call_args_list[0].kwargs['max_tokens'])
                    texts = [call.args[1]['delta'] for call in self.emit.call_args_list if call.args[0] == 'text']
                    self.assertEqual(texts, [result['answer']])

    async def test_repeated_invalid_generation_is_not_reported_as_missing_document(self):
        self.model.complete.return_value = '{"answer":"unfinished'
        result = await self.answer()
        self.assertEqual(self.model.complete.call_count, 3)
        self.kb.search.assert_called_once()
        self.assertIn('答复未完整生成', result['answer'])
        self.assertNotIn('适用对象或条件不符', result['answer'])
        self.assertNotIn('请补充文件名称', result['answer'])

    async def test_protocol_or_timeout_failure_switches_model_with_same_evidence_and_no_new_search(self):
        for failure in (AssistantError('incomplete', 502, category='model_protocol'), TimeoutError()):
            with self.subTest(failure=type(failure).__name__):
                alternative = {**self.profile, 'model': 'healthy'}
                self.kb.model_allowed = Mock(return_value=True)
                self.kb.search.reset_mock()
                self.model.complete.reset_mock()
                self.model.complete.side_effect = [failure, candidate('该流程只适用于设备甲。[1]')]
                with patch('openclaw_service.assistant.lighthouse_model_health.select_task_profile', return_value=alternative):
                    result = await self.answer()
                self.assertIn('设备甲', result['answer'])
                self.kb.search.assert_called_once()
                self.assertEqual(self.model.complete.call_args_list[1].kwargs['profile'], alternative)
                bodies = [[{k:v for k,v in doc.items() if k != 'queried_at'}
                           for doc in json.loads(call.args[0][1]['content'])['documents']]
                          for call in self.model.complete.call_args_list]
                self.assertEqual(bodies[0], bodies[1])

    async def test_visual_failures_do_not_discard_text_or_append_unrelated_document_warnings(self):
        base = self.kb
        class TextWithUnavailableImages:
            search = base.search
            evidence_current = base.evidence_current
            def answer_evidence(self, actor, items, *, profile, model):
                return {'items': [{**item, 'coverage': {'missing': ['visual_read_failed']},
                                  'context': [{'kind': 'original_text', 'text': item['text']}],
                                  'visual_evidence': []} for item in items],
                        'warning': '无关候选文件：未获得可靠图片解释'}
        self.kb = TextWithUnavailableImages()
        self.model.complete.return_value = candidate('该流程只适用于设备甲。[1]')
        result = await self.answer()
        self.assertIn('该流程只适用于设备甲', result['answer'])
        self.assertNotIn('无关候选文件', result['answer'])
        packet = json.loads(self.model.complete.call_args.args[0][1]['content'])
        self.assertEqual(packet['documents'][0]['context'][0]['kind'], 'original_text')
        self.assertNotIn('text', packet['documents'][0]['context'][0])
        self.assertIn('visual_read_failed', packet['documents'][0]['coverage']['missing'])
        self.assertIn('图片失败不否定', self.model.complete.call_args.args[0][0]['content'])

    async def test_repeated_queries_cannot_start_infinite_read_loop(self):
        self.model.complete.return_value = candidate(queries=['设备乙异常应如何处理？'], gaps=['条件不明'])
        await self.answer()
        self.kb.search.assert_called_once()

    async def test_partial_supported_answer_survives_unavailable_supplement(self):
        self.model.complete.return_value = candidate('设备甲有明确适用限制。[1]', ['设备乙条件'], ['设备乙处理依据'])
        self.kb.search.side_effect = [{'items': [self.first], 'warning': ''}, AssistantError('资料暂不可用', 503)]
        result = await self.answer()
        self.assertIn('设备甲有明确适用限制', result['answer'])
        self.assertIn('尚未核实', result['answer'])
        self.assertFalse(result['knowledge_retrieval']['complete'])

    async def test_no_gap_array_with_requested_queries_still_marks_partial(self):
        self.model.complete.return_value = candidate('设备甲有明确适用限制。[1]', ['设备乙条件'])
        result = await self.answer()
        self.assertFalse(result['knowledge_retrieval']['complete'])
        self.assertIn('尚未核实', result['answer'])

    async def test_model_failure_during_repair_keeps_supported_answer(self):
        self.kb.search.side_effect = [{'items': [item], 'warning': ''} for item in (self.first, self.second)]
        self.model.complete.side_effect = [candidate('甲流程仅适用于甲。[1]', ['设备乙流程'], ['乙的依据']),
                                          AssistantError('模型暂不可用', 502, category='model_connection')]
        result = await self.answer()
        self.assertIn('甲流程仅适用于甲', result['answer'])
        self.assertEqual(result['sources'][0]['data']['document_id'], 'candidate')
        self.assertFalse(result['knowledge_retrieval']['complete'])
        self.assertIn('乙的依据', result['answer'])

    async def test_failed_supplement_cannot_publish_partial_after_permission_changes(self):
        def search(*args, **kwargs):
            if self.kb.search.call_count == 1:
                return {'items': [self.first], 'warning': ''}
            self.auth.return_value = {**self.actor, 'modules': []}
            raise AssistantError('读取失败', 503)
        self.kb.search.side_effect = search
        self.model.complete.return_value = candidate('甲流程仅适用于甲。[1]', ['设备乙流程'], ['乙的依据'])
        result = await self.answer()
        self.assertNotIn('甲流程仅适用于甲', result['answer'])
        self.assertEqual(result['sources'], [])

    async def test_reviewed_partial_does_not_trigger_second_review_of_gap_notice(self):
        from openclaw_service.assistant.lighthouse_review import AnswerReviewer
        reviewer = Mock(spec=AnswerReviewer)
        reviewer.run = AsyncMock(side_effect=lambda actor, turn, history, result, authorize, **kw: {
            **result, 'answer_review': {'status': 'passed', 'repairs': 0}})
        self.engine.answer_reviewer = reviewer
        self.model.complete.return_value = candidate('甲流程仅适用于甲。[1]')
        self.kb.search.return_value['warning'] = '向量索引尚未就绪，本次仅使用关键词结果。'
        result = await self.answer()
        self.assertIn('甲流程仅适用于甲', result['answer'])
        self.assertEqual(reviewer.run.await_count, 1)

    def test_protocol_parser_rejects_unbounded_response(self):
        with self.assertRaises(AssistantError):
            _candidate(candidate('x' * 20001 + '[1]'))

    async def test_generation_retries_share_a_deadline(self):
        clock = [0]
        def fail(*args, **kwargs):
            clock[0] += 50
            raise AssistantError('invalid response', 502, category='model_protocol')
        self.model.complete.side_effect = fail
        with patch('openclaw_service.assistant.lighthouse_knowledge_answer.time',
                   SimpleNamespace(monotonic=lambda: clock[0])), \
             patch('openclaw_service.assistant.lighthouse_model_health.select_task_profile', return_value=None):
            result = await self.answer()
        self.assertEqual([call.kwargs['timeout'] for call in self.model.complete.call_args_list], [45, 10])
        self.kb.search.assert_called_once()
        self.assertIn('未完整生成', result['answer'])

    async def test_failed_repair_and_review_keep_only_previously_verified_claims(self):
        from openclaw_service.assistant.lighthouse_review import AnswerReviewer
        reviewer = Mock(spec=AnswerReviewer)
        reviewer.run = AsyncMock(side_effect=[
            {'answer': '甲流程仅适用甲。[1] 乙也能照做。[1]', '_review_supported_answer': '甲流程仅适用甲。[1]',
             'answer_review': {'status': 'needs_repair', 'issues': ['乙没有依据'], 'read_queries': ['乙流程']}},
            {'answer': '甲流程仅适用甲。[1] 乙也能照做。[1]', 'answer_review': {'status': 'unavailable'}}])
        self.engine.answer_reviewer = reviewer
        self.kb.search.side_effect = [{'items': [item], 'warning': ''} for item in (self.first, self.second)]
        self.model.complete.side_effect = [candidate('甲流程仅适用甲。[1] 乙也能照做。[1]'),
                                          AssistantError('connection', 502, category='model_connection')]
        with patch('openclaw_service.assistant.lighthouse_model_health.select_task_profile', return_value=None):
            result = await self.answer()
        self.assertIn('甲流程仅适用甲。[1]', result['answer'])
        self.assertNotIn('乙也能照做', result['answer'])
        self.assertEqual(result['answer_review']['status'], 'needs_review')
        self.assertFalse(result['knowledge_retrieval']['complete'])

    async def test_repair_is_bounded_even_when_every_search_finds_new_data(self):
        self.kb.search.side_effect = lambda actor, query, **kwargs: {
            'items': [hit(str(self.kb.search.call_count), query)], 'warning': ''}
        self.model.complete.side_effect = [candidate('本段仅支持部分结论。[1]', [f'条件{i}资料'], ['关键条件'])
                                          for i in range(3)]
        result = await self.answer()
        self.assertEqual(self.kb.search.call_count, 3)
        self.assertEqual(self.model.complete.call_count, 3)
        self.assertFalse(result['knowledge_retrieval']['complete'])

    async def test_denied_supplement_never_uses_other_access_path(self):
        self.kb.search.side_effect = [{'items': [self.first], 'warning': ''}, AssistantError('无权', 403)]
        self.model.complete.return_value = candidate(queries=['设备乙流程'], gaps=['设备不符'])
        with self.assertRaises(AssistantError) as caught:
            await self.answer()
        self.assertEqual(caught.exception.status, 403)
        self.assertFalse(any(call.args[0] == 'text' for call in self.emit.call_args_list))

    async def test_identity_change_before_second_search_is_rejected(self):
        self.auth.side_effect = [self.actor, self.actor, self.actor, {**self.actor, 'id': 'other'}]
        self.model.complete.return_value = candidate(queries=['设备乙流程'], gaps=['设备不符'])
        with self.assertRaises(AssistantError):
            await self.answer()
        self.kb.search.assert_called_once()

    async def test_revocation_after_keyword_rewrite_stops_all_supplement_queries(self):
        self.model.complete.side_effect = ['[NO_APPLICABLE_DOCUMENT]', '{"keywords":["设备乙流程","设备乙条件"]}']
        self.auth.side_effect = [self.actor, self.actor, self.actor, {**self.actor, 'modules': []}]
        with self.assertRaises(AssistantError) as caught:
            await self.answer()
        self.assertEqual(caught.exception.status, 403)
        self.kb.search.assert_called_once()

    async def test_revocation_between_keyword_searches_does_not_start_next_read(self):
        self.model.complete.side_effect = ['[NO_APPLICABLE_DOCUMENT]', '{"keywords":["设备乙流程","设备乙条件"]}']
        self.auth.side_effect = [self.actor] * 4 + [{**self.actor, 'modules': []}]
        with self.assertRaises(AssistantError):
            await self.answer()
        self.assertEqual(self.kb.search.call_count, 2)

    async def test_empty_answer_keeps_specific_missing_evidence_and_actual_read_error(self):
        self.model.complete.return_value = candidate(queries=['设备乙条件'], gaps=['缺少设备乙适用范围'])
        self.kb.search.side_effect = [{'items': [self.first], 'warning': ''}, AssistantError('资料读取暂不可用', 503)]
        result = await self.answer()
        self.assertIn('缺少设备乙适用范围', result['answer'])
        self.assertIn('资料读取暂不可用', result['answer'])

    async def test_privacy_refusal_is_preserved_without_citation_error(self):
        self.model.complete.return_value = PRIVATE_REPLY
        result = await self.answer()
        self.assertEqual(result['answer'], PRIVATE_REPLY)

    async def test_revoked_document_after_repair_is_not_delivered(self):
        self.kb.search.side_effect = [{'items': [item], 'warning': ''} for item in (self.first, self.second)]
        self.model.complete.side_effect = [candidate(queries=['设备乙流程'], gaps=['设备不符']),
                                          candidate('核实现场状态。[1]')]
        self.kb.evidence_current.side_effect = [True, True, True, False]
        result = await self.answer()
        self.assertNotIn('核实现场状态', result['answer'])
        self.assertEqual(result['sources'], [])

    async def test_revocation_during_context_expansion_blocks_answer_model(self):
        base = self.kb
        class ExpandingKnowledge:
            search = base.search
            evidence_current = base.evidence_current
            def answer_evidence(self, actor, items, *, profile, model):
                return {'items': items, 'warning': ''}
        self.kb = ExpandingKnowledge()
        self.auth.side_effect = [self.actor, self.actor, {**self.actor, 'modules': []}]
        with self.assertRaises(AssistantError) as caught:
            await self.answer()
        self.assertEqual(caught.exception.status, 403)
        self.model.complete.assert_not_called()

    async def test_supported_chunks_retained_while_new_evidence_displaces_irrelevant_hits(self):
        initial = [self.first, *[hit(str(i), '无关流程') for i in range(11)]]
        self.kb.search.side_effect = [{'items': initial, 'warning': ''}, {'items': [self.second], 'warning': ''}]
        self.model.complete.side_effect = [candidate('甲流程仅适用甲。[1]', ['乙流程'], ['乙的条件']),
                                          candidate('甲适用限制见[1]；乙先核实现场状态。[2]')]
        result = await self.answer()
        self.assertEqual([row['data']['document_id'] for row in result['sources'][:2]], ['candidate', 'applicable'])
        self.assertLessEqual(len(result['sources']), 12)

    async def test_question_and_profile_unchanged_and_no_source_text_persisted(self):
        self.model.complete.return_value = candidate('甲流程仅适用于甲。[1]')
        before = copy.deepcopy((self.actor, self.profile, self.first))
        result = await self.answer()
        self.assertEqual((self.actor, self.profile, self.first), before)
        self.assertNotIn(self.first['text'], json.dumps(result['sources'], ensure_ascii=False))
        self.assertNotIn(self.first['text'], json.dumps(result['_knowledge_evidence'], ensure_ascii=False))

    async def test_sensitive_queries_not_sent_to_knowledge(self):
        self.model.complete.return_value = candidate(queries=['人员手机号13800138000'], gaps=['不适用'])
        await self.answer()
        self.kb.search.assert_called_once()

    async def test_sensitive_gap_is_not_appended_to_supported_answer(self):
        self.model.complete.return_value = candidate('甲流程仅适用于甲。[1]', ['设备乙条件'], ['联系13800138000核对'])
        result = await self.answer()
        self.assertNotIn('13800138000', result['answer'])

    async def test_version_and_scope_conflict_rules_reach_the_model(self):
        self.model.complete.return_value = candidate('文件适用对象不同，不能合并为同一流程。[1]')
        await self.answer()
        rules = self.model.complete.call_args.args[0][0]['content']
        for requirement in ('设备型号', '例外', '业务批准', '替代关系', '每个关键结论', 'OCR', '先给用户所问的直接结论'):
            self.assertIn(requirement, rules)

    def test_protocol_parser_rejects_malformed_envelopes_and_accepts_legacy_markdown(self):
        for value in ('{"answer":1}', '{"answer":"x","queries":[{}]}', '{bad', '{"answer":"x","gaps":"wrong"}'):
            with self.assertRaises(AssistantError) as caught:
                _candidate(value)
            self.assertEqual(caught.exception.category, 'model_protocol')
        self.assertEqual(_candidate('原文说明。[1]'), ('原文说明。[1]', [], []))
        self.assertEqual(_candidate('```json\n' + candidate('说明。[1]') + '\n```'), ('说明。[1]', [], []))
        with self.assertRaises(AssistantError):
            _candidate('```json\n' + candidate('说明。[1]') + '\n```\n以上为结果')

    def test_previous_candidate_references_are_remapped_without_mutating_saved_partial(self):
        def source(number, identity):
            return {'number': number, 'data': {'document_id': identity, 'version': 1, 'ordinal': 0}}
        previous = [source(1, 'one'), source(2, 'two')]
        current = [source(1, 'two'), source(2, 'new'), source(3, 'one')]
        answer = '第一项。[1] 第二项。[2]'
        result = _previous_candidate(answer, previous, current)
        self.assertEqual(result['answer'], '第一项。[3] 第二项。[1]')
        self.assertEqual(previous[1]['number'], 2)
        result = _previous_candidate(answer, previous, current[:1])
        self.assertIn('[原依据未纳入本轮]', result['answer'])


if __name__ == '__main__':
    unittest.main()
