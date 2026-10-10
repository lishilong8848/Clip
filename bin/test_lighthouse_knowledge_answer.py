import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from openclaw_service.assistant.lighthouse_knowledge_answer import answer_company, company_followup
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
        self.kb.evidence_current.return_value = False
        self.model.complete.return_value = '过期答案'
        response = await answer_company(self.engine, self.actor, {'_profile': self.profile}, '公司出差规定', self.emit, AsyncMock(return_value=self.actor))
        self.assertNotIn('过期答案', response['answer'])
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
