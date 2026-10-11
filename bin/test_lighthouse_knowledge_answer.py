import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from openclaw_service.assistant.lighthouse_knowledge_answer import answer_company, company_followup
from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_model import LighthouseModel


class CompanyAnswers(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.actor = {'id': 'person-a', 'scopes': ['E']}
        self.item = {'name': '员工手册.txt', 'document_id': 'document', 'version': 2, 'ordinal': 0,
                     'location': '第3段', 'text': '出差前提交申请。', 'url': '/knowledge-base?document=document&version=2&chunk=0'}
        self.kb = Mock()
        self.kb.search.return_value = {'items': [self.item], 'warning': ''}
        self.kb.evidence_current.return_value = True
        self.model = Mock(complete=Mock(return_value='按员工手册，出差前提交申请。[1]'))
        self.engine = SimpleNamespace(portal=SimpleNamespace(get_knowledge=lambda: self.kb), assistant=SimpleNamespace(model_for=lambda actor: self.model))
        self.profile = {'endpoint': 'https://approved.example/chat/completions', 'model': 'fixture'}
        self.emit = AsyncMock()

    async def test_web_and_feishu_use_identical_fresh_retrieval_without_business_tools(self):
        with patch('pydantic_ai.Agent') as factory:
            for channel in ('', 'feishu:oc_group'):
                user = {**self.actor, 'channel': channel}
                response = await answer_company(self.engine, user, {'_profile': self.profile}, '公司出差规定', self.emit, AsyncMock(return_value=user))
                self.assertEqual(response['sources'][0]['kind'], 'company_knowledge')
                self.assertEqual(response['sources'][0]['data']['version'], 2)
                self.assertIn('[1]', response['answer'])
                self.kb.search.assert_called_with(user, '公司出差规定', profile=self.profile)
            factory.assert_not_called()
            self.assertEqual(len(self.model.complete.call_args.args[0]), 2)
            self.assertEqual(self.model.complete.call_args.kwargs['profile'], self.profile)

    async def test_deletion_while_model_answers_discards_result_and_citations(self):
        self.kb.evidence_current.side_effect = [True, False]
        self.model.complete.return_value = '过期答案'
        response = await answer_company(self.engine, self.actor, {'_profile': self.profile}, '公司出差规定', self.emit, AsyncMock(return_value=self.actor))
        self.assertNotIn('过期答案', response['answer'])
        self.assertEqual(response['sources'], [])

    async def test_document_revoked_before_generation_is_not_sent_to_model(self):
        self.kb.evidence_current.return_value = False
        response = await answer_company(self.engine, self.actor, {'_profile': self.profile}, '公司出差规定', self.emit, AsyncMock(return_value=self.actor))
        self.model.complete.assert_not_called()
        self.assertEqual(response['sources'], [])

    async def test_direct_hit_uses_one_model_call_without_keyword_expansion(self):
        await answer_company(self.engine, self.actor, {'_profile': self.profile}, '公司出差规定', self.emit, AsyncMock(return_value=self.actor))
        self.model.complete.assert_called_once()
        self.kb.search.assert_called_once()

    async def test_miss_expands_once_using_the_same_account_model_then_cites_evidence(self):
        self.kb.search.side_effect = [{'items': [], 'warning': ''}, {'items': [self.item], 'warning': ''}]
        self.model.complete.side_effect = ['{"keywords":["差旅报销"]}', '出差前提交申请。[1]']
        response = await answer_company(self.engine, self.actor, {'_profile': self.profile}, '公司出差手续', self.emit, AsyncMock(return_value=self.actor))
        self.assertEqual(self.kb.search.call_count, 2)
        self.assertEqual(self.kb.search.call_args.args[1], '差旅报销')
        self.assertEqual(self.model.complete.call_count, 2)
        options = self.model.complete.call_args_list[0].kwargs
        self.assertEqual(options['profile'], self.profile)
        self.assertEqual(options['timeout'], 6)
        self.assertTrue(options['structured'])
        self.assertEqual(response['sources'][0]['data']['version'], 2)

    async def test_empty_library_does_not_call_model(self):
        self.kb.search.return_value = {'items': [], 'warning': '公司知识库暂无已完成入库的文件。'}
        response = await answer_company(self.engine, self.actor, {'_profile': self.profile}, '公司出差手续', self.emit, AsyncMock(return_value=self.actor))
        self.model.complete.assert_not_called()
        self.assertEqual(response['sources'], [])

    async def test_invalid_or_sensitive_keyword_output_does_not_query_or_generate_again(self):
        self.kb.search.return_value = {'items': [], 'warning': ''}
        for rewritten in ('not-json', '[]', '{"keywords":[]}', '{"keywords":["13800138000"]}', '{"keywords":["今天天气"]}'):
            with self.subTest(rewritten=rewritten):
                self.kb.search.reset_mock()
                self.model.complete.reset_mock()
                self.model.complete.return_value = rewritten
                response = await answer_company(self.engine, self.actor, {'_profile': self.profile}, '公司出差手续', self.emit, AsyncMock(return_value=self.actor))
                self.kb.search.assert_called_once()
                self.model.complete.assert_called_once()
                self.assertEqual(response['sources'], [])

    async def test_keyword_model_failure_does_not_fabricate_an_answer(self):
        self.kb.search.return_value = {'items': [], 'warning': ''}
        self.model.complete.side_effect = AssistantError('模型连接未完成', 502)
        response = await answer_company(self.engine, self.actor, {'_profile': self.profile}, '公司出差手续', self.emit, AsyncMock(return_value=self.actor))
        self.model.complete.assert_called_once()
        self.assertEqual(response['sources'], [])

    async def test_keyword_expansion_rechecks_actual_identity(self):
        self.kb.search.return_value = {'items': [], 'warning': ''}
        self.model.complete.return_value = '{"keywords":["差旅报销"]}'
        with self.assertRaises(AssistantError) as caught:
            await answer_company(self.engine, self.actor, {'_profile': self.profile}, '公司出差手续', self.emit, AsyncMock(side_effect=[self.actor, {'id': 'another-person'}]))
        self.assertEqual(caught.exception.status, 403)
        self.kb.search.assert_called_once()

    async def test_unverified_citation_or_no_citation_is_not_presented_as_company_policy(self):
        for answer in ('直接执行即可。', '出差前提交申请。[99]'):
            with self.subTest(answer=answer):
                self.model.complete.return_value = answer
                response = await answer_company(self.engine, self.actor, {'_profile': self.profile}, '公司出差手续', self.emit, AsyncMock(return_value=self.actor))
                self.assertNotEqual(response['answer'], answer)
                self.assertEqual(response['sources'], [])

    async def test_sensitive_model_output_is_refused_without_document_disclosure(self):
        self.model.complete.return_value = '身份证号 110105199003071234。[1]'
        response = await answer_company(self.engine, self.actor, {'_profile': self.profile}, '公司出差手续', self.emit, AsyncMock(return_value=self.actor))
        self.assertNotIn('110105199003071234', response['answer'])
        self.assertEqual(response['sources'], [])

    async def test_company_route_precedes_general_model_and_business(self):
        engine = LighthouseModel(Mock())
        with patch('openclaw_service.assistant.lighthouse_knowledge_answer.answer_company', new_callable=AsyncMock, return_value={'answer': 'source reply'}) as query:
            result = await engine.answer(self.actor, {'question': '我们公司报销规定'}, [], None, self.emit, AsyncMock(return_value=self.actor), {})
        self.assertEqual(result['answer'], 'source reply')
        query.assert_awaited_once()

    def test_only_company_material_questions_enter_knowledge(self):
        history = [{'question': '公司报销规定是什么', 'sources': [{'kind': 'company_knowledge'}]}]
        for question in ('今天E楼有几条通告', '公司今天有多少人在职', '帮我们公司发送维保通告', '南通天气', '微软公司最新消息', 'UPS是什么', '那今天天气怎么样', '那今天通告有多少条'):
            with self.subTest(question=question):
                self.assertEqual(company_followup(question, history), '')
        self.assertIn('补充问题', company_followup('那具体需要哪些凭证？', history))


if __name__ == '__main__':
    unittest.main()
