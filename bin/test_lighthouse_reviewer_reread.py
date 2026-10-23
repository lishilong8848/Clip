"""Synthetic evidence only: reviewer feedback must drive fresh authorized reads."""
import copy
import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from bin import test_lighthouse_knowledge_repair as fixture
from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_knowledge_answer import answer_company, _previous_candidate
from openclaw_service.assistant.lighthouse_model import LighthouseModel
from openclaw_service.assistant.lighthouse_review import AnswerReviewer


class ReviewerReread(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        fixture.KnowledgeRepair.setUp(self)
        self.profile.update(id='writer', name='writer', key_cipher='fixture')
        self.model.settings.return_value = {'enabled': True, 'models': [{'id': 'checker', 'configured': True}]}
        self.model.profile.return_value = {**self.profile, 'id': 'checker'}
        self.engine.answer_reviewer = AnswerReviewer(self.engine.assistant)
        self.turn = {'question': '设备乙异常应如何处理？', '_profile': self.profile}
        self.pass_verdict = json.dumps({'passed': True, 'issues': []})

    async def answer(self, **kwargs):
        return await answer_company(self.engine, self.actor, self.turn, self.turn['question'],
                                    self.emit, self.auth, **kwargs)

    def fail_verdict(self, queries=()):
        return json.dumps({'passed': False, 'issues': ['Need the applicable equipment procedure.'],
                           'read_queries': list(queries)})

    async def test_reviewer_missing_evidence_reads_again_before_final_delivery(self):
        self.kb.search.side_effect = [{'items': [row]} for row in (self.first, self.second)]
        self.model.complete.side_effect = [fixture.candidate('设备甲流程不能直接用于设备乙。[1]'),
            self.fail_verdict(['设备乙应急规程']),
            fixture.candidate('设备乙应先核实现场状态，再联系负责人员。[2]'), self.pass_verdict]
        result = await self.answer()
        self.assertEqual(self.kb.search.call_count, 2)
        self.assertEqual(result['knowledge_retrieval']['repair_rounds'], 1)
        self.assertEqual(result['answer_review']['status'], 'limited')
        self.assertIn('设备乙应先核实现场状态', result['answer'])
        packet = json.loads(self.model.complete.call_args_list[2].args[0][1]['content'])
        self.assertEqual(packet['review_feedback'], ['Need the applicable equipment procedure.'])
        self.assertEqual([c.args[1]['delta'] for c in self.emit.call_args_list if c.args[0] == 'text'], [result['answer']])
        self.assertNotIn('read_queries', json.dumps(result, ensure_ascii=False))
        self.assertNotIn('text', result['sources'][0]['data'])

    async def test_existing_evidence_correction_does_not_reread(self):
        self.model.complete.side_effect = [fixture.candidate('设备甲流程适用于设备乙。[1]'),
            self.fail_verdict(), '该资料只适用于设备甲，不能据此确认设备乙流程。[1]', self.pass_verdict]
        result = await self.answer()
        self.kb.search.assert_called_once()
        self.assertEqual(result['answer_review']['status'], 'limited')
        self.assertEqual(result['answer_review']['repairs'], 1)
        self.assertIn('不能据此确认设备乙流程', result['answer'])

    async def test_rejected_candidate_is_not_delivered_when_reread_stalls_and_final_review_fails(self):
        rejected = '设备甲流程适用于设备乙。[1]'
        self.model.complete.side_effect = [fixture.candidate(rejected),
            self.fail_verdict(['设备乙应急规程']), OSError('synthetic reviewer unavailable')]
        self.kb.search.return_value = {'items': [self.first], 'warning': ''}
        result = await self.answer()
        self.assertEqual(self.kb.search.call_count, 2)
        self.assertEqual(result['knowledge_retrieval']['repair_rounds'], 0)
        texts = [call.args[1]['delta'] for call in self.emit.call_args_list if call.args[0] == 'text']
        self.assertTrue(result['answer'].strip())
        self.assertEqual(texts, [result['answer']])
        with self.subTest(surface='returned answer'):
            self.assertNotIn(rejected, result['answer'])
        with self.subTest(surface='emitted text'):
            self.assertFalse(any(rejected in text for text in texts))

    async def test_verified_support_survives_reread_stall_without_the_rejected_statement(self):
        supported, rejected = '该流程只适用于设备甲。[1]', '设备甲流程适用于设备乙。[1]'
        evidence = [{'source_number': 1, 'quote': self.first['text']}]
        verdict = {'passed': False, 'issues': ['Need the applicable equipment procedure.'],
                   'read_queries': ['设备乙应急规程'], 'claim_assessments': [
                       {'claim': supported, 'status': 'support', 'evidence': evidence},
                       {'claim': rejected, 'status': 'mismatch', 'evidence': evidence,
                        'issue': 'The source applies only to equipment A.'}]}
        self.model.complete.side_effect = [fixture.candidate(supported + ' ' + rejected),
            json.dumps(verdict), OSError('synthetic reviewer unavailable')]
        self.kb.search.return_value = {'items': [self.first], 'warning': ''}
        result = await self.answer()
        texts = [call.args[1]['delta'] for call in self.emit.call_args_list if call.args[0] == 'text']
        self.assertEqual(self.kb.search.call_count, 2)
        self.assertIn(supported, result['answer'])
        self.assertNotIn(rejected, result['answer'])
        self.assertEqual(texts, [result['answer']])
        self.assertFalse(any(rejected in text for text in texts))

    async def test_permission_revoked_by_review_does_not_query_or_deliver(self):
        def complete(*args, **kwargs):
            if self.model.complete.call_count == 1:
                return fixture.candidate('设备甲流程不能直接用于设备乙。[1]')
            self.auth.return_value = {**self.actor, 'modules': []}
            return self.fail_verdict(['设备乙流程'])
        self.model.complete.side_effect = complete
        with self.assertRaises(AssistantError) as caught:
            await self.answer()
        self.assertEqual(caught.exception.status, 403)
        self.kb.search.assert_called_once()
        self.assertFalse(any(c.args[0] == 'text' for c in self.emit.call_args_list))

    async def test_mixed_sources_numbered_separately_and_no_raw_data_persisted(self):
        business = {'number': 8, 'query_ref': 'q1', 'title': 'Latest equipment status', 'available': True,
                    'url': '/api/status', 'data': {'value': 72, 'complete': True}}
        self.model.complete.side_effect = [fixture.candidate('现场值为72。[1] 资料只适用于设备甲。[2]'), self.pass_verdict]
        result = await self.answer(business_sources=[business], document_query='设备甲要求')
        packet = json.loads(self.model.complete.call_args_list[0].args[0][1]['content'])
        self.assertEqual(packet['documents'][0]['number'], 2)
        self.assertEqual(result['sources'][0]['number'], 1)
        self.assertEqual(result['sources'][1]['number'], 2)
        self.assertIn('72', json.dumps(packet['business_evidence']))
        self.assertNotIn('text', result['sources'][1]['data'])

    async def test_unusable_mixed_evidence_never_starts_public_search(self):
        self.engine.public_sources = SimpleNamespace(search=AsyncMock())
        self.model.complete.return_value = fixture.candidate(queries=['设备乙条件'], gaps=['未核实'])
        result = await self.answer(business_sources=[{'number': 1, 'query_ref': 'q1',
            'title': 'Status', 'data': {'value': 72}, 'available': True}])
        self.engine.public_sources.search.assert_not_awaited()
        self.assertEqual(len(result['sources']), 1)

    async def test_explicit_company_mixed_news_with_no_receipts_never_starts_public_search(self):
        question = '我们公司未公开的项目代号SYNTHETIC-PLAN，预算7万元，同时参考新闻解释融资趋势。'
        self.turn['question'] = '/公司知识库 ' + question
        self.kb.search.return_value = {'items': [], 'warning': '知识库暂无相关资料。'}
        self.engine.public_sources = SimpleNamespace(search=AsyncMock(return_value={'ok': False}))
        result = await answer_company(self.engine, self.actor, self.turn, question,
            self.emit, self.auth, business_sources=[], document_query='融资文件要求')
        self.kb.search.assert_called_once()
        self.engine.public_sources.search.assert_not_awaited()
        self.assertEqual(result['sources'], [])

    async def test_clean_public_only_question_keeps_explicit_command_public_fallback(self):
        question = 'Python 列表是什么'
        self.turn['question'] = '/公司知识库 ' + question
        self.kb.search.return_value = {'items': [], 'warning': '知识库暂无相关资料。'}
        self.engine.public_sources = SimpleNamespace(search=AsyncMock(return_value={
            'ok': True, 'queried_at': '2026-10-23', 'results': [
                {'title': 'Public Python reference', 'url': 'https://example.invalid/python',
                 'snippet': 'Lists contain multiple elements.'}]}))
        self.model.complete.return_value = '列表可以保存多个元素。[1]'
        result = await answer_company(self.engine, self.actor, self.turn, question,
                                      self.emit, self.auth, business_sources=[])
        self.engine.public_sources.search.assert_awaited_once_with('Python 列表', question)
        self.assertIn('公开资料参考', result['answer'])
        self.assertIn('列表可以保存多个元素。[1]', result['answer'])
        self.assertEqual(len(result['sources']), 1)

    def test_previous_candidate_accepts_list_receipts_without_repeating_raw_data(self):
        sources = [{'number': 1, 'query_ref': 'q', 'data': [{'private_payload': 'large body'}]}]
        previous = _previous_candidate('Count verified.[1]', sources, sources)
        self.assertEqual(previous['answer'], 'Count verified.[1]')
        self.assertNotIn('large body', json.dumps(previous))

    def test_only_reviewed_actual_model_is_scored(self):
        engine = LighthouseModel(SimpleNamespace(assistant=self.engine.assistant),
                                 answer_reviewer=self.engine.answer_reviewer)
        turn = {**self.turn, '_actual_profile': {**self.profile, 'id': 'fallback'}, 'task_type': 'document'}
        source = {'number': 1, 'data': {'complete': True}}
        with patch('openclaw_service.assistant.lighthouse_model_health.record_task_quality') as record:
            for status in ('unavailable', 'limited', 'live_general', 'skipped'):
                engine._record_answer_quality(self.actor, turn, {'sources': [source], 'answer_review': {'status': status}})
            record.assert_not_called()
            engine._record_answer_quality(self.actor, turn, {'sources': [source], 'answer_review': {'status': 'passed'}})
            self.assertEqual(record.call_args.args[3]['id'], 'fallback')
            self.assertEqual(record.call_args.kwargs['score'], 100)


if __name__ == '__main__':
    unittest.main()
