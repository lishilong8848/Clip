"""A bare read retry repeats the authorized question; synthetic and offline.

Only existing mechanisms are exercised: query_refinement, task_context and the stream
prompt. No model, network, cloud write, credential or business record is used.
"""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from bin import test_lighthouse_stream as stream_fixtures
from bin.test_lighthouse_question_intent import _StreamFixture
from openclaw_service.assistant.lighthouse_context import task_context, task_question
from openclaw_service.assistant.lighthouse_queries import (
    DATE_TOKEN, effective_question, query_refinement, read_only_question,
)

RETRIES = ('你查看一下', '再查查', '重新查找', '帮我再核实一下', '请继续查看')
ACTOR = {'id': 'read-followup-person', 'scopes': ['C', 'D'], 'modules': ['assistant', 'notices']}
AUTHORIZED = 'D楼本周进行中的变更有哪些？'


class ReadRetryClassifierTests(unittest.TestCase):
    def test_short_read_retries_are_read_only_followups(self):
        for question in RETRIES + ('查看一下', '再查一下', '再核对一遍', '请再查查吧', '麻烦帮我查看一下。'):
            with self.subTest(question=question):
                self.assertTrue(query_refinement(question))
                self.assertTrue(read_only_question(question))

    def test_new_subjects_commands_and_general_questions_are_not_retries(self):
        for question in ('查看D楼水耗', '重新查找D楼事件', '再核实一下变更是否已发送', '查看一下有哪些',
                         '查看E楼维修单', '请发送D楼维保通告', '帮我把进度改成80%', '我可以查看一下吗',
                         '为什么查看一下', '你好'):
            with self.subTest(question=question):
                self.assertFalse(query_refinement(question))
        # Public search stays the existing 联网 branch, never a business retry.
        self.assertTrue(query_refinement('联网查一下'))

    def test_no_instruction_suffix_or_business_text_is_added(self):
        prompt = '之前的问题：' + AUTHORIZED + '\n本次补充（采用最新条件）：你查看一下'
        value = effective_question({'question': '你查看一下', 'prompt': prompt})
        self.assertEqual(value, prompt)
        self.assertEqual(DATE_TOKEN.findall(value), ['本周'])
        self.assertTrue(read_only_question(value))


class ReadRetryFailedTurnTests(unittest.TestCase):
    def previous(self, **extra):
        return {'question': '今天发生了几条事件', 'answer': '', 'status': 'failed', 'scopes': ['C', 'D'],
                'access_scopes': ['C', 'D'], 'access_modules': ['assistant', 'notices'],
                'error': '这次未能完成回答，原消息已保留。', **extra}

    def test_bare_retry_after_failure_keeps_question_and_date(self):
        for question in RETRIES:
            with self.subTest(question=question):
                context = task_context(question, self.previous(), ACTOR)
                self.assertTrue(context['continued'])
                self.assertIn('今天发生了几条事件', context['query'])
                self.assertEqual(DATE_TOKEN.findall(context['query']), ['今天'])
                self.assertTrue(read_only_question(context['query']))
                self.assertEqual(task_question({'_task_context': context}, ACTOR), context['query'])

    def test_retry_after_failure_keeps_the_night_shift_condition(self):
        night = self.previous(question='D楼昨天夜班进行中的变更有哪些')
        context = task_context('再查查', night, ACTOR)
        self.assertTrue(context['continued'])
        self.assertIn('夜班', context['query'])
        self.assertEqual(DATE_TOKEN.findall(context['query']), ['昨天'])


class ReadRetryStreamTests(unittest.IsolatedAsyncioTestCase):
    async def started(self, scopes=('C', 'D')):
        """A real stream over synthetic in-memory stores, limited to the fixture's C/D scopes."""
        fixture = stream_fixtures.StreamTests()
        await fixture.asyncSetUp()
        self.addCleanup(fixture.doCleanups)
        self.addAsyncCleanup(fixture.asyncTearDown)
        fixture.actor.update(scopes=list(scopes))
        return fixture

    async def submit(self, fixture, question):
        await fixture.submit(question)
        await fixture.finish()
        return fixture.assistant._state(fixture.actor)['turns'][-1]

    async def test_channel_and_topic_switches_do_not_inherit_a_bare_retry(self):
        fixture = _StreamFixture({'id': 'lanes', 'scopes': ['C', 'D'], 'modules': ['assistant', 'notices'],
                                  'is_admin': True})
        fixture.seed_turn({'operation_id': 'lanes_message_0001', 'question': AUTHORIZED, 'status': 'failed',
                           'run_id': 'r' * 32, 'scopes': ['C', 'D'], 'access_scopes': ['C', 'D'],
                           'access_modules': ['assistant', 'notices'], 'feishu_topic': ''})
        run, _ = fixture.accept('再查查', 'lanes_message_0002')
        self.assertTrue(run['_turn']['_task_context']['continued'])
        run, _ = fixture.accept('重新查找', 'lanes_message_0003')
        self.assertEqual(effective_question(run['_turn']).count('D楼'), 1)
        fixture.actor['channel'] = 'feishu:oc_lanes'
        run, _ = fixture.accept('帮我再核实一下', 'lanes_message_0004')
        self.assertNotIn('D楼', effective_question(run['_turn']))

    async def test_public_topic_retry_uses_only_supplied_public_context(self):
        fixture = await self.started()
        await self.submit(fixture, 'D楼本周进行中的变更有哪些？')
        fixture.actor.update(channel='feishu:oc_fixture',
                             _feishu_topic={'id': 'topic-one', 'context': '已公开：D楼变更甲和乙'})
        turn = await self.submit(fixture, '帮我再核实一下')
        self.assertEqual(turn['status'], 'completed')
        self.assertIn('D楼变更甲和乙', turn['prompt'])
        self.assertNotIn('D楼本周进行中的变更有哪些', turn['prompt'])
        self.assertFalse(turn['_task_context']['continued'])


if __name__ == '__main__':
    unittest.main()
