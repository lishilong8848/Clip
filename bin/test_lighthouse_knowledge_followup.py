"""Document followups use current local originals and bounded, scoped reads."""
import json
import unittest
from unittest.mock import AsyncMock, Mock, patch
from types import SimpleNamespace

from bin import test_lighthouse_knowledge_evidence as evidence_fixtures
from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_knowledge_answer import answer_company

TEXT = evidence_fixtures.TEXT


class DocumentFollowup(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.fixture = evidence_fixtures.EvidenceTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.kb, self.actor = self.fixture.kb, self.fixture.actor
        self.model = Mock()
        self.engine = SimpleNamespace(portal=SimpleNamespace(get_knowledge=lambda: self.kb),
            assistant=SimpleNamespace(model_for=lambda actor: self.model))
        self.emit = AsyncMock()

    async def answer(self, references, question='具体怎么操作？'):
        return await answer_company(self.engine, self.actor, {'question': question, '_profile': TEXT,
            '_query_intent': {'document_references': references}}, question,
            self.emit, AsyncMock(return_value=self.actor))

    async def test_followup_reads_relevant_distant_sections_in_different_documents(self):
        for name, fact in (('设备规程.txt', '操作步骤：先由值班负责人核对授权。'),
                           ('报销手册.txt', '操作步骤：申请人附上合规凭证。')):
            with self.subTest(document=name):
                parts = [{'location': f'第{i}段', 'text': '范围说明及定义。'} for i in range(40)]
                parts[30]['text'] = fact
                ref = self.fixture.upload(name, content=name.encode(), sections=parts)
                self.model.complete.return_value = json.dumps({'answer': fact + '[1]', 'queries': [], 'gaps': []})
                with patch.object(self.kb, 'search', wraps=self.kb.search) as search, \
                     patch.object(self.kb, '_ensure_snapshot', side_effect=AssertionError('No embedding search')):
                    result = await self.answer([ref])
                packet = json.loads(self.model.complete.call_args.args[0][1]['content'])
                self.assertIn(fact, '\n'.join(doc['text'] for doc in packet['documents']))
                self.assertTrue(search.call_args.kwargs['document_refs'])
                self.assertEqual({s['data']['document_id'] for s in result['sources']}, {ref['document_id']})
                self.assertEqual(result['knowledge_retrieval']['searches'], 0)
                self.assertLessEqual(sum(len(doc['text']) for doc in packet['documents']), 24000)

    async def test_stale_reference_stops_without_broad_search_or_model(self):
        ref = self.fixture.upload('policy.txt')
        for invalid in ({**ref, 'version': 999}, {**ref, 'ordinal': 999}):
            with patch.object(self.kb, 'search', wraps=self.kb.search) as search:
                result = await self.answer([invalid])
            self.assertTrue(all(call.kwargs.get('document_refs') for call in search.call_args_list))
            self.assertEqual(result['sources'], [])
            self.model.complete.assert_not_called()

    async def test_followup_repair_stays_with_selected_document(self):
        ref = self.fixture.upload('policy.txt', sections=[{'location': '第1段', 'text': '提交申请需附凭证。'}])
        self.fixture.upload('unrelated.txt', content=b'other', sections=[{'location': '第1段', 'text': '特殊条件采用另一套规则。'}])
        self.model.complete.return_value = json.dumps({'answer': '提交申请需附凭证。[1]',
            'queries': ['特殊条件'], 'gaps': ['特殊条件的依据']}, ensure_ascii=False)
        with patch.object(self.kb, 'search', wraps=self.kb.search) as search:
            result = await self.answer([ref], '还有哪些条件？')
        self.assertTrue(all(call.kwargs.get('document_refs') == [ref] for call in search.call_args_list))
        self.assertIn('提交申请需附凭证', result['answer'])
        self.assertFalse(result['knowledge_retrieval']['complete'])
        self.assertEqual({s['data']['document_id'] for s in result['sources']}, {ref['document_id']})

    async def test_followup_model_retry_reuses_signed_current_evidence(self):
        ref = self.fixture.upload('policy.txt', sections=[{'location': '手续', 'text': '提交申请需附凭证。'}])
        self.model.complete.side_effect = [AssistantError('bad output', 502, category='model_protocol'),
                                          '提交申请需附凭证。[1]']
        with patch.object(self.kb, 'search', wraps=self.kb.search) as search, \
             patch.object(self.kb, 'answer_evidence', wraps=self.kb.answer_evidence) as expand, \
             patch('openclaw_service.assistant.lighthouse_model_health.select_task_profile', return_value=None):
            result = await self.answer([ref], '具体需要什么凭证？')
        self.assertEqual(search.call_count, 1)
        self.assertEqual(self.model.complete.call_count, 2)
        self.assertTrue(expand.call_args_list[1].args[1][0]['_evidence_receipt'])
        self.assertEqual(result['sources'][0]['data']['version'], ref['version'])
        self.assertNotIn('_evidence_receipt', json.dumps(result['sources']))

    async def test_changed_source_revision_never_reaches_followup_model(self):
        ref = self.fixture.upload('managed.txt', managed=True)
        self.fixture.source_version = 2
        result = await self.answer([ref])
        self.assertEqual(result['sources'], [])
        self.model.complete.assert_not_called()

    async def test_mixed_followup_scoped_read_uses_document_branch_only(self):
        ref = self.fixture.upload('policy.txt', sections=[{'location': '手续', 'text': '提交申请需附凭证。'}])
        question = '查询当前记录，并说明规程中的凭证要求'
        self.model.complete.return_value = '提交申请需附凭证。[1]'
        with patch.object(self.kb, 'search', wraps=self.kb.search) as search:
            await answer_company(self.engine, self.actor, {'question': question, '_profile': TEXT,
                '_query_intent': {'document_references': [ref]}}, question, self.emit,
                AsyncMock(return_value=self.actor), document_query='规程中的凭证要求')
        self.assertEqual(search.call_args.args[1], '规程中的凭证要求')

    def test_scoped_search_rejects_invalid_revoked_and_changed_versions(self):
        ref = self.fixture.upload('managed.txt', managed=True)
        for refs in ([], [ref] * 7, [{**ref, 'version': 999}], [{**ref, 'ordinal': 999}]):
            with self.assertRaises(AssistantError):
                self.kb.search(self.actor, 'needle', profile=TEXT, document_refs=refs)
        self.fixture.allowed = False
        with self.assertRaises(AssistantError):
            self.kb.search(self.actor, 'needle', profile=TEXT, document_refs=[ref])


if __name__ == '__main__':
    unittest.main()
