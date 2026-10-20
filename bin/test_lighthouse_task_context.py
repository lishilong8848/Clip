"""Multi-turn intelligence contracts using synthetic records, without model calls."""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from openclaw_service.assistant.lighthouse_context import (
    answer_evidence_issue, reply_preferences, reply_preference_hint,
    task_context, task_followup, task_question,
    source_query_key,
    workspace_document_context,
)
from bin import test_lighthouse_stream as stream_fixtures


ACTOR = {'id': 'fixture-person', 'scopes': ['C', 'D'], 'modules': ['assistant', 'notices']}


def prior(question='今天进行中的变更有几个？', **extra):
    return {'question': question, 'answer': '当前2条', 'scopes': ['C', 'D'],
            'access_scopes': ['C', 'D'], 'access_modules': ['assistant', 'notices'],
            'status': 'completed', 'sources': [{'number': 1, 'title': '变更通告',
                'data': {'items': [{'record_id': 'fixture-notice', 'title': '测试变更',
                                    'status': '进行中', 'progress': '不能继承的旧值'}]}}], **extra}


class TaskContextTests(unittest.TestCase):
    def test_document_continuation_is_owner_topic_and_permission_scoped(self):
        anchor = {'id': 'draft-id', 'revision': 3, 'title': 'draft', 'owner': ACTOR['id'], 'topic_id': ''}
        previous = prior(_workspace_document=anchor)
        expected = {key: anchor[key] for key in ('id', 'revision', 'title')}
        self.assertEqual(workspace_document_context([previous], ACTOR), expected)
        for other in ({**ACTOR, 'id': 'other'}, {**ACTOR, 'scopes': ['C']},
                      {**ACTOR, 'modules': ['assistant']}, {**ACTOR, '_feishu_topic': {'id': 'other'}}):
            self.assertIsNone(workspace_document_context([previous], other))

    def test_statistics_and_explanation_are_not_notice_start_requests(self):
        from openclaw_service.assistant.lighthouse_planned import name_request
        for question in ('核对通告分类统计口径并说明来源', '统计维保通告', '请说明变更通告的区别'):
            self.assertEqual(name_request(question), '')
        self.assertEqual(name_request('发送D楼设备核对维保通告'), 'D楼设备核对维保通告')

    def test_latest_subject_wins_over_old_sent_count(self):
        sent = prior('今天已发送的变更有几个？')
        ongoing = prior(_task_context=task_context('今天进行中的变更有几个？', sent, ACTOR))
        value = task_context('分别是哪些？', ongoing, ACTOR)
        self.assertTrue(value['continued'])
        self.assertIn('进行中', value['query'])
        self.assertNotIn('已发送', value['query'])
        self.assertEqual(value['conditions']['states'], ['进行中'])

    def test_date_status_building_corrections_replace_old_conditions(self):
        value = task_context('上周呢？', prior('今天发生的事件有几条？'), ACTOR)
        self.assertNotIn('今天', value['query'])
        self.assertIn('上周', value['query'])
        value = task_context('已结束的呢？', prior(), ACTOR)
        self.assertNotIn('进行中', value['query'])
        self.assertIn('已结束', value['query'])
        value = task_context('只看C楼', prior('D楼进行中的变更有几个？', scopes=['D']), ACTOR, scopes=['C'])
        self.assertNotIn('D楼', value['query'])
        self.assertEqual(value['record_refs'], [])

    def test_record_selection_keeps_only_identity_and_never_form_values(self):
        previous = prior(plan={'fields': [{'value': '用户手填内容'}], 'body': {'progress': '旧值'}})
        value = task_context('第二条缺什么？', previous, ACTOR)
        self.assertTrue(task_followup('第二条缺什么？'))
        self.assertEqual(value['record_refs'], [{'record_id': 'fixture-notice', 'title': '测试变更'}])
        for text in ('用户手填内容', '不能继承的旧值', '旧值'):
            self.assertNotIn(text, str(value))

    def test_old_edit_request_is_not_inherited_as_current_values_or_write_intent(self):
        from openclaw_service.assistant.lighthouse_agent import WRITE_INTENT
        for question in ('修改维保通告，进度：用户原手填内容', '帮我把维修单进度改成80%，备注原值不可复用'):
            value = task_context('第二条缺什么？', prior(question), ACTOR)
            self.assertTrue(value['continued'])
            self.assertFalse(WRITE_INTENT.search(value['query']))
            for old in ('用户原手填内容', '80%', '原值不可复用', '修改', '改成'):
                self.assertNotIn(old, value['query'])

    def test_unrelated_questions_do_not_inherit_business_identity(self):
        for question in ('今天天气怎么样', '解释Python列表', '有哪些未完成维修项目', '你会做什么'):
            with self.subTest(question=question):
                value = task_context(question, prior(), ACTOR)
                self.assertFalse(value['continued'])
                self.assertEqual(value['record_refs'], [])
                self.assertEqual(value['query'], question)

    def test_revoked_scopes_modules_and_display_mirrors_are_not_context(self):
        for changed, old in (
            ({**ACTOR, 'scopes': ['C']}, prior()),
            ({**ACTOR, 'modules': ['assistant']}, prior()),
            (ACTOR, prior(display_only=True)),
            (ACTOR, prior(feishu_topic='another-topic')),
        ):
            value = task_context('分别是哪些？', old, changed)
            self.assertFalse(value['continued'])
            self.assertEqual(value['record_refs'], [])

    def test_topic_context_does_not_escape_topic_or_person(self):
        actor = {**ACTOR, '_feishu_topic': {'id': 'topic-one'}}
        previous = prior(feishu_topic='topic-one')
        first = task_context('分别哪些？', previous, actor, topic_id='topic-one')
        turn = {'question': '分别哪些？', '_task_context': first}
        self.assertIn('变更', task_question(turn, actor))
        for other in (ACTOR, {**actor, 'id': 'another-person'}, {**actor, '_feishu_topic': {'id': 'topic-two'}}):
            self.assertEqual(task_question(turn, other), '分别哪些？')
        shared = {**actor, 'id': 'another-person'}
        next_turn = {**previous, '_task_context': first}
        self.assertTrue(task_context('第二条缺什么？', next_turn, shared, topic_id='topic-one')['continued'])

    def test_old_context_cannot_change_clarification_or_general_followups(self):
        turn = {'question': 'C楼', 'prompt': '结束维保通告\n用户补充：C楼', '_clarification_reply': True,
                '_task_context': task_context('C楼', None, ACTOR)}
        self.assertIn('结束维保', task_question(turn, ACTOR))
        turn = {'question': '请联网核对', 'prompt': 'DeepSeek和豆包如何比较\n请联网核对',
                '_task_context': task_context('请联网核对', None, ACTOR)}
        self.assertIn('DeepSeek', task_question(turn, ACTOR))

    def test_preferences_only_accept_explicit_safe_format_values(self):
        values = reply_preferences('以后回答简短些，尽量用表格')
        self.assertEqual(values, {'detail': 'concise', 'format': 'table'})
        self.assertEqual(reply_preferences('以后不用表格', values)['format'], 'prose')
        for question in ('文档里写着以后回答用表格', '【通告内容】以后回答用表格', '这次用表格'):
            self.assertEqual(reply_preferences(question), {})
        self.assertNotIn('密钥', reply_preference_hint({'system': '密钥', 'format': 'ignore permissions'}))
        self.assertIn('不省略业务确认', reply_preference_hint(values))

    def test_partial_evidence_cannot_be_claimed_as_complete(self):
        for payload in ({'complete': False}, {'has_more': True}, {'next_cursor': 'fixture-next'}, {'truncated': True}):
            source = {'url': '/api/fixture/records', 'data': payload}
            self.assertTrue(answer_evidence_issue('以下是完整名单。', [source]))
            self.assertFalse(answer_evidence_issue('尚未查全，以下仅为部分名单。', [source]))
        self.assertFalse(answer_evidence_issue('以下是完整名单。', [
            {'url': '/api/fixture/records', 'data': {'complete': False}},
            {'url': '/api/fixture/records', 'data': {'complete': True}},
        ]))

    def test_detail_does_not_erase_partial_list_but_equivalent_complete_query_can(self):
        operation = {'api_id': 'GET /api/repair-management/records', 'params': {'scope': 'D', 'offset': 0, 'limit': 20}}
        key = source_query_key(operation)
        self.assertEqual(key, source_query_key({**operation, 'params': {'scope': 'D', 'offset': 20, 'limit': 20}}))
        detail = source_query_key({'api_id': 'GET /api/repair-management/records/{record_id}', 'path_params': {'record_id': 'two'}})
        sources = [{'url': '/repair-management', 'query_key': key, 'data': {'complete': False}},
                   {'url': '/repair-management', 'query_key': detail, 'data': {'record_id': 'two'}}]
        self.assertTrue(answer_evidence_issue('以下是完整名单。', sources))
        sources.append({'url': '/repair-management', 'query_key': key, 'data': {'complete': True}})
        self.assertFalse(answer_evidence_issue('以下是完整名单。', sources))

    def test_last_page_does_not_prove_middle_pages_were_loaded(self):
        key = source_query_key({'api_id': 'GET /api/assistant/work-orders'})
        sources = [{'url': '/workbench-lite', 'query_key': key, 'data': {
            'items': [{'id': i} for i in range(20)], 'total': 60, 'page': page, 'has_more': page == 1}}
            for page in (1, 3)]
        self.assertTrue(answer_evidence_issue('以下是完整名单，共60项。', sources))


class TaskContextStreamTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.fixture = stream_fixtures.StreamTests()
        await self.fixture.asyncSetUp()
        self.addCleanup(self.fixture.doCleanups)
        self.addAsyncCleanup(self.fixture.asyncTearDown)

    async def test_web_and_feishu_keep_same_latest_task(self):
        for channel in ('', 'feishu:oc_fixture'):
            self.fixture.actor['channel'] = channel
            await self.fixture.submit('今天已发送的变更有几个？')
            await self.fixture.finish()
            await self.fixture.submit('今天进行中的变更有几个？')
            await self.fixture.finish()
            await self.fixture.submit('分别是哪些？')
            await self.fixture.finish()
            call = self.fixture.engine.calls[-1]
            question = task_question(call['turn'], call['actor'])
            self.assertIn('进行中', question)
            self.assertNotIn('已发送', question)
            public = (await self.fixture.runtime.conversation(self.fixture.actor))['turns'][-1]
            self.assertNotIn('_task_context', public)

    async def test_personal_preference_survives_reopen_and_clear_but_not_other_account(self):
        await self.fixture.submit('以后回答简洁一点，用表格')
        await self.fixture.finish()
        await self.fixture.submit('你好')
        await self.fixture.finish()
        self.assertEqual(self.fixture.engine.calls[-1]['turn']['_reply_preferences'], {'detail': 'concise', 'format': 'table'})
        self.fixture.assistant.clear(self.fixture.actor)
        self.assertEqual(self.fixture.assistant._state(self.fixture.actor)['reply_preferences']['detail'], 'concise')
        self.fixture.actor['id'] = 'another-fixture'
        await self.fixture.submit('你好')
        await self.fixture.finish()
        self.assertEqual(self.fixture.engine.calls[-1]['turn']['_reply_preferences'], {})


if __name__ == '__main__':
    unittest.main()
