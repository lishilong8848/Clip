"""Automatic document routing and transient review evidence, without network calls."""
import asyncio
import copy
import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from openclaw_service.assistant import lighthouse_knowledge_answer as answers
from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_model import LighthouseModel


class AutomaticCompanyKnowledge(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(patch('socket.create_connection', side_effect=AssertionError('Network forbidden')))
        self.enterContext(patch('socket.socket.connect', side_effect=AssertionError('Network forbidden')))
        self.actor = {'id': 'person-a', 'scopes': ['E'], 'modules': ['knowledge']}
        self.profile = {'endpoint': 'https://approved.example/chat/completions', 'model': 'fixture'}
        self.model = Mock(settings=Mock(return_value={'enabled': True, 'configured': True}),
                          complete=Mock(return_value='true'))
        self.assistant = SimpleNamespace(model_for=Mock(return_value=self.model))
        self.item = {'name': '南通现场规程.txt', 'document_id': 'document', 'version': 2, 'ordinal': 0,
                     'location': '第3段', 'text': '仅供本轮核验的文档正文。',
                     'url': '/knowledge-base?document=document&version=2&chunk=0'}
        self.kb = Mock(search=Mock(return_value={'items': [self.item], 'warning': ''}),
                       evidence_current=Mock(return_value=True))
        self.portal = SimpleNamespace(get_knowledge=Mock(return_value=self.kb), assistant=self.assistant)
        self.engine = SimpleNamespace(portal=self.portal, assistant=self.assistant,
                                      public_sources=SimpleNamespace(search=AsyncMock()))
        self.authorize = AsyncMock(return_value=self.actor)
        self.emit = AsyncMock()

    async def classify(self, question, *, history=None, turn=None):
        return await answers.automatic_company_query(self.engine, self.actor,
            turn or {'question': question, '_profile': self.profile}, question, history or [], self.authorize)

    async def test_web_and_feishu_classify_generic_procedures_without_skills_or_keyword_rules(self):
        for channel in ('', 'feishu:oc_group'):
            self.actor['channel'] = channel
            for question in ('机房湿度超过80%怎么应急？', '冷却系统失效后的应急流程是什么？',
                             '设备检修前有哪些隔离确认步骤？', '交接班巡检应核对哪些事项？',
                             '电池维护SOP有哪些安全要求？', '站点技术文档里的告警分级是什么？'):
                with self.subTest(channel=channel, question=question):
                    self.model.complete.reset_mock()
                    self.assertEqual(await self.classify(question), question)
                    messages = self.model.complete.call_args.args[0]
                    self.assertEqual(messages[1], {'role': 'user', 'content': question})
                    self.assertNotIn('80', messages[0]['content'])
                    self.assertNotIn('湿度', messages[0]['content'])
                    self.assertEqual(self.model.complete.call_args.kwargs['profile'], {**self.profile, 'reasoning': False})
                    self.assertLessEqual(self.model.complete.call_args.kwargs['timeout'], 6)
            self.assistant.model_for.assert_called_with(self.actor)
        self.portal.get_knowledge.assert_not_called()
        self.kb.search.assert_not_called()
        self.engine.public_sources.search.assert_not_awaited()

    async def test_company_words_still_require_semantic_classification(self):
        for question in ('公司报销规定是什么？', '南通知识库中交接班规范有哪些？'):
            self.assertEqual(await self.classify(question), question)
        self.model.settings.assert_called()
        self.assertEqual(self.model.complete.call_count, 2)
        self.portal.get_knowledge.assert_not_called()

    async def test_company_terms_in_a_general_question_do_not_force_retrieval(self):
        self.model.complete.return_value = 'false'
        for question in ('公司知识库这个词是什么意思？', '给我一份公司规章制度的虚拟写作示例',
                         'EOP是什么意思？', '读数从91%降到80%变化了几个百分点？'):
            self.assertIsNone(await self.classify(question))
        self.assertEqual(self.model.complete.call_count, 4)
        self.portal.get_knowledge.assert_not_called()

    async def test_semantic_routing_does_not_spend_the_answer_reasoning_budget(self):
        self.profile['reasoning'] = True
        self.model.complete.return_value = 'true'
        await self.classify('现场环境异常应怎样处置？')
        self.assertFalse(self.model.complete.call_args.kwargs['profile']['reasoning'])
        self.assertTrue(self.profile['reasoning'])

    async def test_parent_routes_web_and_feishu_automatic_questions_before_general_agent(self):
        reviewer = SimpleNamespace(run=AsyncMock(side_effect=lambda actor, turn, history, result, authorize: result))
        portal = Mock(assistant=self.assistant, get_knowledge=self.portal.get_knowledge)
        model_factory = Mock(side_effect=AssertionError('General agent must not start'))
        engine = LighthouseModel(portal, model_factory=model_factory, answer_reviewer=reviewer)
        for channel in ('', 'feishu:oc_group'):
            self.actor['channel'] = channel
            for question in ('机房湿度超过80%怎么应急？', '设备检修前有哪些隔离确认步骤？'):
                with self.subTest(channel=channel, question=question), patch(
                        'openclaw_service.assistant.lighthouse_planned.continue_selection',
                        new_callable=AsyncMock, return_value=None):
                    self.model.complete.side_effect = ['true', '依据已核实的现场规程处理。[1]']
                    turn = {'question': question, '_profile': self.profile, 'submitted_commands': []}
                    result = await engine.answer(self.actor, turn, [], None, self.emit, self.authorize, {})
                    self.kb.search.assert_called_with({**self.actor, 'allowed_scopes': self.actor['scopes']},
                                                      question, profile=self.profile)
                    self.assertEqual(result['sources'][0]['kind'], 'company_knowledge')
                    self.assertNotIn('_review_sources', result)
        model_factory.assert_not_called()

    async def test_ordinary_chat_realtime_counts_writing_and_public_topics_do_not_route(self):
        self.model.complete.return_value = 'false'
        for question in ('你好，今天心情如何？', '今天E楼有几条通告？', '公司现在有多少在职人员？',
                         '帮我写一篇公司报销规定的通知', '南通明天天气怎么样？', 'UPS是什么？',
                         '上传公司资料并发送给同事'):
            with self.subTest(question=question):
                self.assertIsNone(await self.classify(question))
        self.portal.get_knowledge.assert_not_called()

    async def test_explicit_optout_never_calls_model_or_factory(self):
        for question in ('不用公司资料，讲一下应急流程', '不要查知识库，介绍现场管理',
                         '不用南通文件，讲一下应急原则',
                         '不要使用/公司知识库，解释空调原理',
                         "Don't use company documents; explain the concept."):
            with self.subTest(question=question):
                self.assertIsNone(await self.classify(question))
        self.assistant.model_for.assert_not_called()
        self.portal.get_knowledge.assert_not_called()

    async def test_absent_or_noncallable_factory_skips_model(self):
        for portal in (SimpleNamespace(), SimpleNamespace(get_knowledge=None),
                       SimpleNamespace(get_knowledge=True)):
            self.engine.portal = portal
            self.assertIsNone(await self.classify('现场应急流程是什么？'))
        self.assistant.model_for.assert_not_called()

    async def test_unconfigured_models_and_implicit_mock_settings_skip_even_direct_company_query(self):
        for settings in (Mock(), None, {}, {'enabled': False, 'configured': True},
                         {'enabled': True, 'configured': False}, {'enabled': Mock(), 'configured': True}):
            self.model.settings.return_value = settings
            self.assertIsNone(await self.classify('公司报销规定是什么？'))
        self.model.complete.assert_not_called()
        self.portal.get_knowledge.assert_not_called()

    async def test_missing_profile_and_sensitive_question_skip_classifier(self):
        for turn in ({'question': '现场应急流程是什么？'}, {'question': '现场应急流程是什么？', '_profile': Mock()}):
            self.assertIsNone(await self.classify(turn['question'], turn=turn))
        self.assertIsNone(await self.classify('请查询密码 password=abcdef123456'))
        self.model.complete.assert_not_called()

    async def test_classifier_accepts_only_json_true(self):
        for decision in ('false', 'null', '1', '"true"', '[]', '{"company":true}', 'true trailing',
                         '```json\ntrue\n```', '', None):
            self.model.complete.return_value = decision
            with self.subTest(decision=decision):
                self.assertIsNone(await self.classify('现场应急流程是什么？'))
        self.portal.get_knowledge.assert_not_called()

    async def test_classifier_errors_fail_gracefully_and_reauthorize(self):
        for error in (TimeoutError(), OSError(), AssistantError('模型不可用', 502), RuntimeError()):
            self.authorize.reset_mock()
            self.model.complete.side_effect = error
            self.assertIsNone(await self.classify('现场应急流程是什么？'))
            self.assertEqual(self.authorize.await_count, 2)
        self.portal.get_knowledge.assert_not_called()

    async def test_wall_clock_timeout_is_enforced_if_model_ignores_timeout_option(self):
        real_timeout = asyncio.timeout
        async def blocked_call(function, *args, **kwargs):
            if function is self.model.complete:
                await asyncio.Event().wait()
            return function(*args, **kwargs)
        with patch.object(answers.asyncio, 'to_thread', side_effect=blocked_call), \
                patch.object(answers.asyncio, 'timeout', side_effect=lambda seconds: real_timeout(.01)) as bound:
            self.assertIsNone(await self.classify('现场应急流程是什么？'))
        bound.assert_called_once_with(6)
        self.assertEqual(self.authorize.await_count, 2)
        self.portal.get_knowledge.assert_not_called()

    async def test_identity_change_before_or_after_classification_stops_routing(self):
        other = {**self.actor, 'id': 'person-b'}
        for authorizations in ([other], [self.actor, other]):
            self.authorize.side_effect = authorizations
            with self.assertRaises(AssistantError) as caught:
                await self.classify('现场应急流程是什么？')
            self.assertEqual(caught.exception.status, 403)
        self.portal.get_knowledge.assert_not_called()

    async def test_permissions_denied_initially_or_revoked_during_classification(self):
        denied = {**self.actor, 'modules': []}
        self.authorize.return_value = denied
        self.assertIsNone(await self.classify('现场应急流程是什么？'))
        self.model.complete.assert_not_called()
        for changed in (denied, {**self.actor, 'scopes': []}):
            self.authorize.side_effect = [self.actor, changed]
            with self.assertRaises(AssistantError):
                await self.classify('现场应急流程是什么？')
        self.portal.get_knowledge.assert_not_called()

    async def test_history_answers_documents_topic_context_and_old_prompt_are_never_classified(self):
        question = '现场应急流程是什么？'
        history = [{'question': '旧问题', 'answer': '已撤销答案', 'scopes': ['A'],
                    'channel': 'feishu:another', 'sources': [{'data': {'text': '撤销的正文'}}]}]
        turn = {'question': question, '_profile': self.profile, 'prompt': '旧提示里的正文',
                '_task_context': {'query': '其他话题的上下文'}}
        before = copy.deepcopy((history, turn))
        self.assertEqual(await self.classify(question, history=history, turn=turn), question)
        self.assertEqual(self.model.complete.call_args.args[0][1]['content'], question)
        self.assertEqual((history, turn), before)
        self.assertNotIn('_model_health', vars(self.assistant))

    def test_automatic_document_followup_preserves_original_question_without_skills(self):
        first = {'question': '机房湿度超过80%怎么应急？', 'sources': [{'kind': 'company_knowledge'}]}
        followup = {'question': '还有呢？', 'sources': [{'kind': 'company_knowledge'}]}
        query = answers.company_followup('详细点', [first, followup], only_followup=True)
        self.assertTrue(query.startswith(first['question']))
        self.assertIn('还有呢？', query)
        self.assertTrue(query.endswith('详细点'))
        self.assertEqual(answers.company_followup('详细点', [first, {'question': '天气如何？'}], only_followup=True), '')
        self.assertEqual(answers.company_followup('不用公司资料，再详细点', [first], only_followup=True), '')

    async def test_empty_keyword_fallback_and_partial_index_warning_allow_one_synonym_retry(self):
        for warning, mode in (('向量索引尚未就绪，本次仅使用关键词结果。', 'keyword'),
                              ('部分索引尚未完成。', 'keyword'), ('partial index', 'keyword'),
                              ('检索已降级。', 'keyword_fallback')):
            with self.subTest(warning=warning):
                self.kb.search.reset_mock()
                self.kb.search.side_effect = [{'items': [], 'warning': warning, 'mode': mode},
                                             {'items': [self.item], 'warning': ''}]
                self.model.complete.side_effect = ['{"keywords":["环境异常处置"]}', '按现场规程核实。[1]']
                result = await answers.answer_company(self.engine, self.actor, {'_profile': self.profile},
                    '环境异常应急流程', self.emit, self.authorize)
                self.assertEqual(self.kb.search.call_count, 2)
                self.assertEqual(self.kb.search.call_args.args[1], '环境异常处置')
                self.assertTrue(result['sources'])

    async def test_authorization_warning_or_denial_never_triggers_synonym_retry(self):
        for failure in ({'items': [], 'warning': '当前模型未获授权，本次仅使用关键词结果。', 'mode': 'keyword_fallback'},
                        AssistantError('无权读取资料', 403)):
            self.kb.search.side_effect = failure if isinstance(failure, Exception) else None
            if isinstance(failure, dict):
                self.kb.search.return_value = failure
            result = await answers.answer_company(self.engine, self.actor, {'_profile': self.profile},
                '公司应急流程', self.emit, self.authorize)
            self.assertNotIn('_review_sources', result)
            self.assertNotIn('_knowledge_evidence', result)
        self.model.complete.assert_not_called()
        self.engine.public_sources.search.assert_not_awaited()

    async def test_success_contains_separate_review_text_and_metadata_only_revalidation(self):
        self.model.complete.return_value = '依据现场规程核实。[1]'
        result = await answers.answer_company(self.engine, self.actor, {'_profile': self.profile},
            '现场应急流程', self.emit, self.authorize)
        self.assertEqual(result['_review_sources'][0]['data']['text'], self.item['text'])
        self.assertNotIn('text', result['sources'][0]['data'])
        self.assertEqual(result['_knowledge_evidence'][0]['document_id'], self.item['document_id'])
        self.assertNotIn(self.item['text'], json.dumps(result['_knowledge_evidence'], ensure_ascii=False))
        self.assertNotIn('profile', json.dumps(result['_knowledge_evidence']))
        result['_review_sources'][0]['data']['name'] = 'changed'
        self.assertEqual(result['sources'][0]['data']['name'], self.item['name'])

    async def test_rejected_answers_never_return_private_review_evidence(self):
        for reply in ('缺少引用', '错误引用。[99]', '[NO_APPLICABLE_DOCUMENT]'):
            self.model.complete.return_value = reply
            result = await answers.answer_company(self.engine, self.actor, {'_profile': self.profile},
                '现场应急流程', self.emit, self.authorize)
            self.assertNotIn('_review_sources', result)
            self.assertNotIn('_knowledge_evidence', result)

    async def test_outer_review_receives_body_but_persistable_result_and_events_do_not(self):
        seen = []
        async def review(actor, turn, history, result, authorize):
            seen.append(copy.deepcopy(result['sources']))
            return result
        engine = LighthouseModel(self.portal, answer_reviewer=SimpleNamespace(run=AsyncMock(side_effect=review)))
        self.model.complete.return_value = '按已核实的规程处理。[1]'
        history = [{'question': '你好', 'answer': '你好'}]
        original = copy.deepcopy(history)
        result = await engine.answer(self.actor, {'question': '/公司知识库 现场应急流程', '_profile': self.profile},
            history, None, self.emit, self.authorize, {})
        self.assertEqual(seen[0][0]['data']['text'], self.item['text'])
        self.assertNotIn('_review_sources', result)
        self.assertNotIn('_knowledge_evidence', result)
        self.assertNotIn(self.item['text'], json.dumps(result, ensure_ascii=False))
        self.assertNotIn(self.item['text'], str(self.emit.call_args_list))
        self.assertEqual(history, original)
        self.assertEqual(self.kb.evidence_current.call_count, 3)

    async def test_document_revoked_during_outer_review_prevents_text_delivery(self):
        async def revoke(actor, turn, history, result, authorize):
            self.kb.evidence_current.return_value = False
            return result
        engine = LighthouseModel(self.portal, answer_reviewer=SimpleNamespace(run=AsyncMock(side_effect=revoke)))
        self.model.complete.return_value = '已被撤销的答案。[1]'
        with self.assertRaises(AssistantError) as caught:
            await engine.answer(self.actor, {'question': '/公司知识库 现场应急流程', '_profile': self.profile},
                [], None, self.emit, self.authorize, {})
        self.assertEqual(caught.exception.status, 403)
        self.assertFalse(any(call.args[0] == 'text' for call in self.emit.call_args_list))

    async def test_no_current_documents_cannot_be_recreated_from_history_by_outer_review(self):
        self.kb.search.return_value = {'items': [], 'warning': '公司知识库中未找到相关资料。'}
        self.model.complete.return_value = '{"keywords":[]}'
        reviewer = SimpleNamespace(run=AsyncMock(return_value={
            'answer': '上次的归档规定仍然有效。', 'sources': [], 'answer_review': {'status': 'corrected'}}))
        engine = LighthouseModel(self.portal, answer_reviewer=reviewer)
        history = [{'question': '公司归档规定', 'answer': '旧规程曾要求三人复核。', 'scopes': ['E']}]
        result = await engine.answer(self.actor, {'question': '/公司知识库 公司归档规定', '_profile': self.profile},
            history, None, self.emit, self.authorize, {})
        reviewer.run.assert_not_awaited()
        self.assertEqual(result['answer_review']['reason'], 'source_unavailable')
        self.assertNotIn('仍然有效', result['answer'])
        self.assertNotIn('三人复核', result['answer'])
        self.assertEqual(result['sources'], [])


if __name__ == '__main__':
    unittest.main()
