import json
import sys
import tempfile
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
        self.reviewer = SimpleNamespace(run=AsyncMock(side_effect=lambda actor, turn, history, result, authorize: {
            **result, 'answer_review': {'status': 'passed', 'repairs': 0}}))

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

    async def test_compound_faults_retrieve_both_topics_even_when_first_search_hits(self):
        transformer = [{**self.item, 'document_id': 'transformer', 'ordinal': n,
                        'name': '变压器故障流程.xlsx', 'text': '变压器故障按电气应急流程处理。'} for n in range(8)]
        heat = [{**self.item, 'document_id': 'room-heat', 'ordinal': n,
                 'name': '机房局部高温流程.xlsx', 'text': '机房环境高温按暖通应急流程处理。'} for n in range(8)]
        self.kb.search.side_effect = [{'items': rows, 'warning': ''} for rows in (transformer, transformer, heat)]
        self.model.complete.side_effect = ['{"keywords":["变压器故障","机房高温"]}', '分别核实供电与机房温度，按对应流程协调处置。[1][2]']
        question = '根据公司知识库中的内容告诉我变压器故障的同时又机房温度高温了给我一个简单的应急思路。'
        response = await answer_company(self.engine, self.actor, {'_profile': self.profile}, question,
                                        self.emit, AsyncMock(return_value=self.actor))
        self.assertEqual([call.args[1] for call in self.kb.search.call_args_list], [question, '变压器故障', '机房高温'])
        prompt = json.loads(self.model.complete.call_args.args[0][1]['content'])
        self.assertEqual({doc['name'] for doc in prompt['documents']}, {'变压器故障流程.xlsx', '机房局部高温流程.xlsx'})
        self.assertEqual(len(prompt['documents']), 12)
        self.assertEqual([doc['number'] for doc in prompt['documents']], list(range(1, 13)))
        self.assertEqual(len({(s['data']['document_id'], s['data']['ordinal']) for s in response['sources']}), 12)
        self.assertTrue(all(topic['candidate_sources'] for topic in prompt['topics']))
        self.assertIn('逐一覆盖', self.model.complete.call_args.args[0][0]['content'])

    async def test_compound_partial_miss_is_explicit_not_replaced_by_first_topic(self):
        self.kb.search.side_effect = [{'items': rows, 'warning': ''} for rows in ([self.item], [self.item], [])]
        self.model.complete.side_effect = ['{"keywords":["变压器故障","机房高温"]}', '仅找到第一项的资料。[1]']
        response = await answer_company(self.engine, self.actor, {'_profile': self.profile}, '变压器故障同时机房高温',
                                        self.emit, AsyncMock(return_value=self.actor))
        self.assertIn('以下主题尚未检索到公司资料依据：机房高温', response['answer'])
        prompt = json.loads(self.model.complete.call_args.args[0][1]['content'])
        self.assertEqual(prompt['topics'][1]['candidate_sources'], [])

    async def test_compound_answer_uses_two_real_indexed_documents(self):
        from openclaw_service.assistant.lighthouse_knowledge import KnowledgeBase
        from bin.test_lighthouse_knowledge import FakeEmbedder, FakeMemoryIndex, actor, _protect
        with tempfile.TemporaryDirectory() as folder:
            kb = KnowledgeBase(folder, embedder=FakeEmbedder(), memory_index_factory=FakeMemoryIndex,
                               protect=_protect, start_worker=False)
            try:
                for name, content in (('变压器故障流程.txt', '变压器故障：先核实供电状态，再按批准的电气应急流程处理。'),
                                      ('机房局部高温流程.txt', '机房高温：核实机房环境温度，再按批准的暖通应急流程处理。')):
                    kb.upload(actor(admin=True), name, content.encode('utf-8'))
                    self.assertTrue(kb.process_one())
                self.engine.portal.get_knowledge = lambda: kb
                self.model.complete.side_effect = ['{"keywords":["变压器故障","机房高温"]}', '分别核实供电和环境温度，协调处置。[1][2]']
                user = {**actor(), 'scopes': ['E']}
                response = await answer_company(self.engine, user, {'_profile': self.profile},
                    '根据公司知识库中的内容告诉我变压器故障的同时又机房温度高温了给我一个简单的应急思路。',
                    self.emit, AsyncMock(return_value=user))
                names = {source['title'].split(' · ')[0] for source in response['sources']}
                self.assertEqual(names, {'变压器故障流程.txt', '机房局部高温流程.txt'})
                self.assertIn('[2]', response['answer'])
            finally:
                kb.close()

    async def test_compound_planner_failure_still_searches_separate_original_clauses(self):
        self.model.complete.side_effect = [AssistantError('模型暂不可用', 502), '仅提供已核实的安全概述。[1]']
        await answer_company(self.engine, self.actor, {'_profile': self.profile}, '变压器故障同时又机房高温',
                             self.emit, AsyncMock(return_value=self.actor))
        self.assertEqual([call.args[1] for call in self.kb.search.call_args_list],
                         ['变压器故障同时又机房高温', '变压器故障', '机房高温'])

    async def test_failed_topic_is_reported_as_failed_not_as_no_document(self):
        self.kb.search.side_effect = [{'items': [self.item], 'warning': ''},
                                     {'items': [self.item], 'warning': ''}, AssistantError('高温资料读取暂忙', 503)]
        self.model.complete.side_effect = ['{"keywords":["变压器故障","机房高温"]}', '先依据已取得的资料核实。[1]']
        response = await answer_company(self.engine, self.actor, {'_profile': self.profile}, '变压器故障同时机房高温',
                                        self.emit, AsyncMock(return_value=self.actor))
        self.assertIn('高温资料读取暂忙', response['answer'])
        self.assertNotIn('以下主题尚未检索到', response['answer'])

    async def test_topic_permission_failure_stops_generation(self):
        self.kb.search.side_effect = [{'items': [self.item], 'warning': ''}, AssistantError('无权读取', 403)]
        self.model.complete.return_value = '{"keywords":["变压器故障","机房高温"]}'
        with self.assertRaises(AssistantError) as caught:
            await answer_company(self.engine, self.actor, {'_profile': self.profile}, '变压器故障同时机房高温',
                                 self.emit, AsyncMock(return_value=self.actor))
        self.assertEqual(caught.exception.status, 403)
        self.model.complete.assert_called_once()

    async def test_knowledge_module_revocation_during_answer_discards_generated_text(self):
        self.model.complete.return_value = '不应返回的旧文档回答。[1]'
        response = await answer_company(self.engine, self.actor, {'_profile': self.profile}, '公司出差规定',
            self.emit, AsyncMock(side_effect=[self.actor, self.actor, {**self.actor, 'modules': []}]))
        self.assertNotIn('不应返回', response['answer'])
        self.assertEqual(response['sources'], [])

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
        engine = LighthouseModel(Mock(), answer_reviewer=self.reviewer)
        for channel in ('', 'feishu:oc_group'):
            actor = {**self.actor, 'channel': channel}
            for turn in ({'question': '/公司知识库 变压器故障怎么应急?给我一个精简的步骤'},
                         {'question': '变压器故障怎么应急?给我一个精简的步骤',
                          'submitted_commands': [{'kind': 'skill', 'id': 'company-knowledge'}]}):
                with self.subTest(channel=channel, turn=turn), patch(
                        'openclaw_service.assistant.lighthouse_knowledge_answer.answer_company',
                        new_callable=AsyncMock, return_value={'answer': 'source reply'}) as query:
                    self.reviewer.run.reset_mock()
                    result = await engine.answer(actor, turn, [], None, self.emit, AsyncMock(return_value=actor), {})
                    self.assertEqual(result['answer'], 'source reply')
                    self.reviewer.run.assert_awaited_once()
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

    async def test_ordinary_new_question_never_opens_knowledge(self):
        @asynccontextmanager
        async def ordinary_model(*args):
            raise RuntimeError('ordinary pipeline reached')
            yield
        portal = Mock()
        engine = LighthouseModel(portal, model_factory=ordinary_model)
        history = [{'question': '/公司知识库 公司报销规定', 'sources': [{'kind': 'company_knowledge'}]}]
        for channel in ('', 'feishu:oc_group'):
            actor = {**self.actor, 'channel': channel}
            for question in ('公司报销规定是什么', '变压器故障怎么应急?给我一个精简的步骤',
                             '那今天天气怎么样', '不用公司资料，再详细点', '还有今天学练未答的人'):
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

    async def test_related_knowledge_correction_resumes_fresh_retrieval_in_both_channels(self):
        first = {'question': '/公司知识库 变压器故障同时机房高温，给我简单的应急思路',
                 'answer': '只回答了变压器。', 'sources': [{'kind': 'company_knowledge'}], 'scopes': ['E']}
        unrelated = {'question': '鱼缸养什么鱼', 'answer': '旧的无关话题。', 'sources': []}
        question = '还有个机房高温的你这里只说了一个变压器的'
        for channel in ('', 'feishu:oc_group'):
            actor = {**self.actor, 'channel': channel}
            engine = LighthouseModel(Mock(), answer_reviewer=self.reviewer)
            with patch('openclaw_service.assistant.lighthouse_knowledge_answer.answer_company',
                       new_callable=AsyncMock, return_value={'answer': '两种故障的安全概述'}) as query:
                self.reviewer.run.reset_mock()
                result = await engine.answer(actor, {'question': question, 'submitted_commands': []}, [unrelated, first],
                    None, self.emit, AsyncMock(return_value=actor), {'summary': '以前聊鱼缸。'})
            self.assertEqual(result['answer'], '两种故障的安全概述')
            self.reviewer.run.assert_awaited_once()
            query.assert_awaited_once()
            combined = query.call_args.args[3]
            self.assertIn('变压器故障同时机房高温', combined)
            self.assertIn(question, combined)
            self.assertNotIn('鱼缸', combined)
            self.assertNotIn('只回答了变压器', combined)

    async def test_knowledge_followup_does_not_cross_permission_or_topic_boundaries(self):
        @asynccontextmanager
        async def ordinary_model(*args):
            raise RuntimeError('ordinary pipeline reached')
            yield
        first = {'question': '/公司知识库 公司报销规定', 'sources': [{'kind': 'company_knowledge'}]}
        for history in ([], [first, {'question': '今天天气', 'sources': []}], [{**first, 'scopes': ['A']} ]):
            with self.subTest(history=history), patch(
                'openclaw_service.assistant.lighthouse_planned.continue_selection', new_callable=AsyncMock, return_value=None), patch(
                'openclaw_service.assistant.lighthouse_knowledge_answer.answer_company', new_callable=AsyncMock) as query:
                engine = LighthouseModel(Mock(), model_factory=ordinary_model)
                with self.assertRaisesRegex(RuntimeError, 'ordinary pipeline reached'):
                    await engine.answer(self.actor, {'question': '那具体需要哪些凭证？', '_profile': self.profile},
                        history, None, self.emit, AsyncMock(return_value=self.actor), {})
                query.assert_not_awaited()

    async def test_bare_command_asks_question_without_loading_library(self):
        engine = LighthouseModel(Mock(), answer_reviewer=self.reviewer)
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
            engine = LighthouseModel(Mock(), answer_reviewer=self.reviewer)
            with patch('openclaw_service.assistant.lighthouse_knowledge_answer.answer_company', new_callable=AsyncMock) as query:
                result = await engine.answer(actor, {
                    'question': '当前登录人是谁', 'prompt': '我们公司报销流程是什么\n当前登录人是谁',
                }, [{'question': '公司报销流程', 'sources': [{'kind': 'company_knowledge'}]}],
                    None, self.emit, AsyncMock(return_value=actor), {})
            query.assert_not_awaited()
            self.assertIn('真实登录人', result['answer'])

    async def test_new_document_question_does_not_use_old_business_prompt(self):
        engine = LighthouseModel(Mock(), answer_reviewer=self.reviewer)
        with patch('openclaw_service.assistant.lighthouse_knowledge_answer.answer_company', new_callable=AsyncMock, return_value={'answer': 'source reply'}) as query:
            await engine.answer(self.actor, {'question': '/公司知识库 我们公司报销规定', 'prompt': '今天的通告\n我们公司报销规定'},
                                [], None, self.emit, AsyncMock(return_value=self.actor), {})
        self.assertEqual(query.call_args.args[3], '我们公司报销规定')


if __name__ == '__main__':
    unittest.main()
