import sys
import unittest
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from openclaw_service.assistant.lighthouse_knowledge_answer import answer_company, company_followup, company_request
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

    async def test_explicit_company_route_precedes_general_model_and_business_in_both_channels(self):
        engine = LighthouseModel(Mock())
        for channel in ('', 'feishu:oc_group'):
            actor = {**self.actor, 'channel': channel}
            for turn in ({'question': '/公司知识库 变压器故障怎么应急?给我一个精简的步骤'},
                         {'question': '变压器故障怎么应急?给我一个精简的步骤',
                          'submitted_commands': [{'kind': 'skill', 'id': 'company-knowledge'}]}):
                with self.subTest(channel=channel, turn=turn), patch(
                        'openclaw_service.assistant.lighthouse_knowledge_answer.answer_company',
                        new_callable=AsyncMock, return_value={'answer': 'source reply'}) as query:
                    result = await engine.answer(actor, turn, [], None, self.emit, AsyncMock(return_value=actor), {})
                    self.assertEqual(result['answer'], 'source reply')
                    query.assert_awaited_once()
                    self.assertEqual(query.call_args.args[3], '变压器故障怎么应急?给我一个精简的步骤')

    def test_only_current_explicit_selection_counts(self):
        selected = [{'kind': 'skill', 'id': 'company-knowledge'}]
        for turn in ({'question': '公司报销规定'}, {'question': '怎么使用 /公司知识库'},
                     {'question': '不要使用/公司知识库'},
                     {'question': '还有呢', 'submitted_commands': [], 'commands': selected,
                      '_command_context': [{'kind': 'guide', 'name': 'company-knowledge'}]},
                     {'question': '变压器故障怎么应急', 'prompt': '/公司知识库 变压器故障怎么应急'}):
            with self.subTest(turn=turn):
                self.assertIsNone(company_request(turn))
        self.assertEqual(company_request({'question': '/公司知识库：变压器故障怎么应急'}), '变压器故障怎么应急')
        self.assertEqual(company_request({'question': '问题', 'commands': selected}), '问题')

    async def test_ordinary_question_and_followup_never_open_knowledge(self):
        @asynccontextmanager
        async def ordinary_model(*args):
            raise RuntimeError('ordinary pipeline reached')
            yield
        portal = Mock()
        engine = LighthouseModel(portal, model_factory=ordinary_model)
        history = [{'question': '/公司知识库 公司报销规定', 'sources': [{'kind': 'company_knowledge'}]}]
        for channel in ('', 'feishu:oc_group'):
            actor = {**self.actor, 'channel': channel}
            for question in ('公司报销规定是什么', '变压器故障怎么应急?给我一个精简的步骤', '那具体需要哪些凭证？'):
                with self.subTest(channel=channel, question=question), patch(
                        'openclaw_service.assistant.lighthouse_planned.continue_selection', new_callable=AsyncMock, return_value=None), patch(
                        'openclaw_service.assistant.lighthouse_knowledge_answer.answer_company', new_callable=AsyncMock) as query:
                    with self.assertRaisesRegex(RuntimeError, 'ordinary pipeline reached'):
                        await engine.answer(actor, {'question': question, '_profile': {**self.profile, 'name': 'test'},
                            'submitted_commands': [], 'commands': [{'kind': 'skill', 'id': 'company-knowledge'}],
                            '_command_context': [{'kind': 'guide', 'name': 'company-knowledge'}]},
                            history, None, self.emit, AsyncMock(return_value=actor), {})
                    query.assert_not_awaited()
                    portal.get_knowledge.assert_not_called()

    async def test_bare_command_asks_question_without_loading_library(self):
        engine = LighthouseModel(Mock())
        result = await engine.answer(self.actor, {'question': '/公司知识库'}, [], None, self.emit, AsyncMock(return_value=self.actor), {})
        self.assertIn('填写要查询的问题', result['answer'])
        engine.portal.get_knowledge.assert_not_called()

    async def test_explicit_miss_uses_only_original_question_for_public_fallback(self):
        self.kb.search.return_value = {'items': [], 'warning': '知识库暂无相关资料。'}
        self.engine.public_sources = SimpleNamespace(search=AsyncMock(return_value={
            'ok': True, 'queried_at': '2026-10-11', 'results': [
                {'title': '公开安全资料', 'url': 'https://example.com/safety', 'snippet': '保持距离，联系专业人员。'}]}))
        self.model.complete.return_value = '保持距离，联系专业人员。[1]'
        turn = {'question': '/公司知识库 变压器故障怎么应急', '_profile': self.profile}
        result = await answer_company(self.engine, self.actor, turn, '内部文档的扩展检索词', self.emit, AsyncMock(return_value=self.actor))
        self.engine.public_sources.search.assert_awaited_once_with('变压器故障怎么应急', '变压器故障怎么应急')
        self.assertNotIn('内部文档', str(self.model.complete.call_args))
        self.assertNotIn('company_knowledge', str(result['sources']))
        self.assertIn('公开资料参考', result['answer'])

    async def test_inapplicable_document_falls_back_without_sending_document_to_search(self):
        self.item['text'] = '只能用于公司内部的无关资料'
        self.model.complete.side_effect = ['[NO_APPLICABLE_DOCUMENT]', '公开参考。[1]']
        self.engine.public_sources = SimpleNamespace(search=AsyncMock(return_value={
            'ok': True, 'queried_at': '2026-10-11', 'results': [
                {'title': '公开资料', 'url': 'https://example.com', 'snippet': '公开内容'}]}))
        result = await answer_company(self.engine, self.actor,
            {'question': '/公司知识库 变压器故障怎么应急', '_profile': self.profile},
            '变压器故障怎么应急', self.emit, AsyncMock(return_value=self.actor))
        self.engine.public_sources.search.assert_awaited_once_with('变压器故障怎么应急', '变压器故障怎么应急')
        self.assertNotIn(self.item['text'], str(self.model.complete.call_args_list[-1]))
        self.assertIn('不适用于', result['answer'])

    async def test_internal_question_and_denied_access_do_not_fall_back_to_public(self):
        self.engine.public_sources = SimpleNamespace(search=AsyncMock())
        for question in ('公司内部报销规定', 'A-177-GPS-05设备怎么应急', '我们公司员工手册'):
            self.kb.search.return_value = {'items': [], 'warning': '无资料'}
            await answer_company(self.engine, self.actor, {'question': '/公司知识库 ' + question, '_profile': self.profile},
                question, self.emit, AsyncMock(return_value=self.actor))
        self.kb.search.side_effect = AssistantError('无权读取', 403)
        await answer_company(self.engine, self.actor, {'question': '/公司知识库 变压器故障怎么应急', '_profile': self.profile},
            '变压器故障怎么应急', self.emit, AsyncMock(return_value=self.actor))
        self.engine.public_sources.search.assert_not_awaited()

    def test_only_company_material_questions_enter_knowledge(self):
        history = [{'question': '公司报销规定是什么', 'sources': [{'kind': 'company_knowledge'}]}]
        for question in ('今天E楼有几条通告', '公司今天有多少人在职', '帮我们公司发送维保通告', '南通天气', '微软公司最新消息', 'UPS是什么', '那今天天气怎么样', '那今天通告有多少条'):
            with self.subTest(question=question):
                self.assertEqual(company_followup(question, history), '')
        self.assertIn('补充问题', company_followup('那具体需要哪些凭证？', history))

    def test_switching_subject_or_declining_knowledge_does_not_inherit_company_topic(self):
        history = [{'question': '公司报销规定是什么', 'sources': [{'kind': 'company_knowledge'}]}]
        for question in ('那你是什么模型？', '那帮我写个冒泡排序', '那谢谢你', '这个UPS是什么',
                         '还有今天学练未答的人', '这个公司知识库怎么上传文件', '知识库是什么？',
                         '不要查知识库，解释向量的原理', '不用公司资料，介绍一下空调',
                         '公司里水耗管理的数据', '我们公司今天的重保任务', '那把报销制度发给我',
                         '将公司报销制度发给李世龙', '向李世龙发送公司资料', '我们公司现在有多少在职人员',
                         '本公司未结束的工单有几条', '公司里所有人员名单', '我们公司总人数'):
            with self.subTest(question=question):
                self.assertEqual(company_followup(question, history), '')

    def test_consecutive_company_followups_keep_topic_but_not_across_new_subjects(self):
        first = {'question': '公司报销规定是什么', 'sources': [{'kind': 'company_knowledge'}]}
        second = {'question': '那具体需要哪些凭证？', 'sources': [{'kind': 'company_knowledge'}]}
        query = company_followup('还有呢？', [first, second])
        self.assertIn(first['question'], query)
        self.assertIn(second['question'], query)
        self.assertTrue(query.endswith('还有呢？'))
        self.assertEqual(company_followup('还有呢？', [first, {'question': '南通天气', 'sources': []}]), '')
        self.assertEqual(company_followup('那具体需要哪些凭证？', []), '')
        self.assertEqual(company_followup('知识库有什么报销规定？', []), '知识库有什么报销规定？')

    async def test_stale_prompt_cannot_force_company_route_in_either_channel(self):
        for channel in ('', 'feishu:oc_group'):
            actor = {**self.actor, 'channel': channel, 'name': '真实登录人'}
            engine = LighthouseModel(Mock())
            with patch('openclaw_service.assistant.lighthouse_knowledge_answer.answer_company', new_callable=AsyncMock) as query:
                result = await engine.answer(actor, {
                    'question': '当前登录人是谁', 'prompt': '我们公司报销流程是什么\n当前登录人是谁',
                }, [{'question': '公司报销流程', 'sources': [{'kind': 'company_knowledge'}]}],
                    None, self.emit, AsyncMock(return_value=actor), {})
            query.assert_not_awaited()
            self.assertIn('真实登录人', result['answer'])

    async def test_new_document_question_does_not_use_old_business_prompt(self):
        engine = LighthouseModel(Mock())
        with patch('openclaw_service.assistant.lighthouse_knowledge_answer.answer_company', new_callable=AsyncMock, return_value={'answer': 'source reply'}) as query:
            await engine.answer(self.actor, {'question': '/公司知识库 我们公司报销规定', 'prompt': '今天的通告\n我们公司报销规定'},
                                [], None, self.emit, AsyncMock(return_value=self.actor), {})
        self.assertEqual(query.call_args.args[3], '我们公司报销规定')


if __name__ == '__main__':
    unittest.main()
