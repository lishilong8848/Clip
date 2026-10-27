"""Offline live-output and same-account execution-lane regressions."""
import asyncio
import copy
import json
import unittest
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, patch

from bin import test_lighthouse_stream as fixtures
from bin import test_lighthouse_learning_bridge as bridge_fixtures
from openclaw_service.assistant.lighthouse_context import execution_key
from openclaw_service.assistant.lighthouse_model import LighthouseModel, PublicText
from pydantic_ai.models.function import FunctionModel
from starlette.requests import Request


class LiveConcurrencyTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = fixtures.StreamTests.asyncSetUp
    asyncTearDown = fixtures.StreamTests.asyncTearDown
    payload = fixtures.StreamTests.payload
    finish = fixtures.StreamTests.finish

    async def authorize(self):
        return copy.deepcopy(self.actor)

    async def launch(self, lane, question='解释冒泡排序的原理', **fields):
        actor = {**self.actor, **lane}
        async def authorize():
            return copy.deepcopy(actor)
        payload = self.payload(question, **fields)
        payload['conversation_id'] = self.assistant._state(actor)['id']
        result = await self.runtime.submit(copy.deepcopy(actor), payload, self.request, authorize)
        return actor, result, authorize

    async def until(self, predicate):
        async with asyncio.timeout(3):
            while not predicate():
                await asyncio.sleep(.01)

    def live_engine(self, *, fail=False):
        reached, release = asyncio.Event(), asyncio.Event()
        async def answer(actor, turn, history, request, emit, authorize, context):
            await emit('reply-mode', {'mode': 'live'})
            await emit('text', {'delta': '第一段，'})
            reached.set()
            await release.wait()
            if fail:
                raise TimeoutError('offline interruption')
            await emit('text', {'delta': '第二段。'})
            return {'answer': '第一段，第二段。', 'sources': []}
        self.engine.answer = answer
        return reached, release

    async def test_text_visible_before_completion_and_replay_has_no_duplicate(self):
        reached, release = self.live_engine()
        actor, ack, _ = await self.launch({'_execution_lane': 'browser-one'})
        await reached.wait()
        view = await self.runtime.conversation(actor)
        self.assertTrue(view['busy'])
        self.assertEqual(view['turns'][-1]['answer'], '第一段，')
        self.assertEqual(view['turns'][-1]['status'], 'pending')
        run = self.runtime.get_run(actor, ack['run_id'])
        self.assertTrue(any(e['type'] == 'text-delta' for e in run['events']))
        release.set()
        await self.finish()
        deltas = [e['delta'] for e in run['events'] if e['type'] == 'text-delta']
        self.assertEqual(''.join(deltas), '第一段，第二段。')

    async def test_business_drafts_are_not_visible_before_validation(self):
        reached, release = asyncio.Event(), asyncio.Event()
        async def answer(actor, turn, history, request, emit, authorize, context):
            await emit('text', {'delta': '未经核验的业务数量999。'})
            reached.set()
            await release.wait()
            return {'answer': '已核验2项。', 'sources': []}
        self.engine.answer = answer
        actor, ack, _ = await self.launch({'_execution_lane': 'browser-one'}, '查询维修项目')
        await reached.wait()
        self.assertEqual((await self.runtime.conversation(actor))['turns'][-1]['answer'], '')
        run = self.runtime.get_run(actor, ack['run_id'])
        self.assertFalse(any(e['type'] == 'text-delta' for e in run['events']))
        release.set()
        await self.finish()
        self.assertEqual(run['answer'], '已核验2项。')

    async def test_two_browsers_and_feishu_topics_do_not_cancel_each_other(self):
        reached, release = self.live_engine()
        lanes = [{'_execution_lane': 'browser-one'}, {'_execution_lane': 'browser-two'},
                 {'channel': 'feishu:oc_Test'},
                 {'channel': 'feishu:oc_Test', '_feishu_topic': {'id': 'topic-one'}},
                 {'channel': 'feishu:oc_Test', '_feishu_topic': {'id': 'topic-two'}}]
        self.runtime.slots = None
        runs = [await self.launch(lane) for lane in lanes]
        await self.until(lambda: all(self.runtime.live[ack['run_id']].get('_candidate_answer') for _, ack, _ in runs))
        self.assertEqual(len({execution_key(actor) for actor, _, _ in runs}), 5)
        for actor, ack, _ in runs:
            view = await self.runtime.conversation(actor)
            self.assertEqual(view['active_run_id'], ack['run_id'])
            self.assertTrue(view['busy'])
            run = self.runtime.get_run(actor, ack['run_id'])
            if actor.get('channel', '').startswith('feishu:'):
                self.assertEqual(run['reply_mode'], 'validated')
                self.assertEqual(run['answer'], '')
                self.assertFalse(any(item['type'] == 'text-delta' for item in run['events']))
            else:
                self.assertEqual(run['answer'], '第一段，')
        self.assertEqual(runs[0][1]['conversation_id'], runs[1][1]['conversation_id'])
        self.assertEqual(len({ack['conversation_id'] for _, ack, _ in runs}), 4)
        release.set()
        await self.finish()
        self.assertTrue(all(self.runtime.get_run(actor, ack['run_id'])['status'] == 'completed' for actor, ack, _ in runs))
        self.assertFalse(self.assistant._active)

    async def test_stop_only_own_lane_and_partial_failure_is_not_success(self):
        reached, release = self.live_engine(fail=True)
        first = await self.launch({'_execution_lane': 'browser-one'})
        second = await self.launch({'_execution_lane': 'browser-two'})
        await self.until(lambda: all(item.get('answer') for item in self.runtime.live.values()))
        with self.assertRaises(fixtures.AssistantError):
            await self.runtime.stop(first[0], second[1]['run_id'])
        await self.runtime.stop(first[0], first[1]['run_id'])
        self.assertTrue((await self.runtime.conversation(second[0]))['busy'])
        release.set()
        await self.finish()
        other = self.runtime.get_run(second[0], second[1]['run_id'])
        self.assertEqual(other['status'], 'failed')
        self.assertEqual(other['answer'], '第一段，')
        self.assertTrue(other['error'])

    async def test_owner_can_stop_own_run_after_revocation_but_not_from_another_channel(self):
        reached, release = self.live_engine()
        actor, ack, _ = await self.launch({'channel': 'feishu:oc_Private'})
        await reached.wait()
        actor['scopes'], actor['modules'] = [], []
        with self.assertRaises(fixtures.AssistantError):
            await self.runtime.stop({**actor, 'channel': ''}, ack['run_id'])
        result = await self.runtime.stop(actor, ack['run_id'])
        self.assertEqual(result['status'], 'stopped')
        release.set()
        await self.finish()

    async def test_other_lane_does_not_cancel_a_business_form_or_refine_its_question(self):
        first, ack, _ = await self.launch({'_execution_lane': 'browser-one'}, '查询D楼的维修项目')
        await self.finish()
        state = self.assistant._state(first)
        turn = state['turns'][0]
        turn['plan'] = {'id': 'retained-plan', 'status': 'needs_input'}
        self.store.put_document('lighthouse_agent_plans', 'retained-plan', turn['plan'])
        self.store.put_document('lighthouse_ai', self.assistant._key(first), state)
        await self.launch({'_execution_lane': 'browser-two'}, '有哪些？')
        await self.finish()
        self.assertEqual(self.store.get_document('lighthouse_agent_plans', 'retained-plan')['status'], 'needs_input')
        self.assertNotIn('查询D楼的维修项目', self.engine.calls[-1]['turn']['prompt'])

    async def test_restart_recovers_only_current_lane(self):
        reached, release = self.live_engine()
        first = await self.launch({'_execution_lane': 'browser-one'})
        second = await self.launch({'_execution_lane': 'browser-two'})
        await self.until(lambda: all(item.get('answer') for item in self.runtime.live.values()))
        await self.runtime.close()
        restored = fixtures.LighthouseStream(self.portal, engine=fixtures.Engine())
        try:
            result = await restored.conversation(first[0])
            self.assertFalse(result['busy'])
            result2 = await restored.conversation(second[0])
            self.assertFalse(result2['busy'])
            self.assertEqual(len(result2['turns']), 2)
        finally:
            await restored.close()

    async def test_legacy_pending_run_and_summary_survive_lane_upgrade(self):
        state = self.assistant._state(self.actor)
        state['context'] = {'summary': '原有共享摘要', 'scopes': ['D']}
        self.store.put_document('lighthouse_ai', self.assistant._key(self.actor), state)
        run, _ = self.runtime._accept(self.actor, self.payload('原有未完成的问题'))
        current = {**self.actor, '_execution_lane': 'browser-one'}
        view = await self.runtime.conversation(current)
        self.assertFalse(view['busy'])
        self.assertEqual(view['turns'][-1]['status'], 'stopped')
        self.assertNotIn(self.actor['id'], self.assistant._active)
        await self.launch({'_execution_lane': 'browser-one'})
        await self.finish()
        self.assertIn('原有共享摘要', json.dumps(self.assistant._state(current), ensure_ascii=False))
        self.assistant.clear(current)

    async def test_completed_legacy_feishu_reply_can_still_be_read_by_operation(self):
        actor, ack, _ = await self.launch({'channel': 'feishu:oc_Test'}, operation_id='feishu_' + 'a' * 40)
        await self.finish()
        result = await self.runtime.conversation({**actor, 'channel': 'feishu:oc_Test'}, operation='feishu_' + 'a' * 40)
        self.assertEqual(result['turns'][0]['run_id'], ack['run_id'])

    async def test_business_pronoun_followup_never_becomes_unvalidated_live_text(self):
        generated, release, events = asyncio.Event(), asyncio.Event(), []
        async def model_stream(messages, info):
            self.assertTrue(info.function_tools)
            yield '上述两项都已处理完。'
            generated.set()
            await release.wait()
        @asynccontextmanager
        async def factory(*_):
            yield FunctionModel(stream_function=model_stream)
        engine = LighthouseModel(self.portal, model_factory=factory)
        async def emit(kind, data):
            events.append((kind, data))
        task = asyncio.create_task(engine.answer(self.actor,
            {'question': '刚才那些都处理完了吗？', 'operation_id': 'pronoun_business_1',
             '_profile': {'name': 'fixture', 'model': 'fixture'}},
            [{'question': '哪些维修未完成', 'answer': '两项维修未完成', 'scopes': ['D']}],
            self.request, emit, self.authorize, {}))
        try:
            await asyncio.wait_for(generated.wait(), 3)
            self.assertIn(('reply-mode', {'mode': 'validated'}), events)
            self.assertFalse(any(kind == 'text' for kind, _ in events))
        finally:
            task.cancel()
            release.set()
            await asyncio.gather(task, return_exceptions=True)

    async def test_live_chunks_check_revoked_permissions(self):
        reached, release = self.live_engine()
        actor, ack, authorize = await self.launch({'_execution_lane': 'browser-one'})
        await reached.wait()
        actor['scopes'] = []
        release.set()
        await self.finish()
        run = self.runtime.live[ack['run_id']]
        self.assertEqual(run['status'], 'failed')
        self.assertEqual(run['answer'], '第一段，')

    async def test_actual_model_stream_is_visible_while_generator_is_blocked(self):
        released = asyncio.Event()
        async def model_stream(messages, info):
            self.assertTrue(info.function_tools, 'ordinary streaming keeps model-selected tools available without forcing a query')
            yield '冒泡排序会逐对比较'
            await released.wait()
            yield '将较大的数移到右侧。'
        @asynccontextmanager
        async def factory(*_):
            yield FunctionModel(stream_function=model_stream)
        reviewer = AsyncMock()
        engine = LighthouseModel(self.portal, model_factory=factory, answer_reviewer=reviewer)
        self.runtime.engine = engine
        self.enterContext(patch('openclaw_service.assistant.lighthouse_query_intent.resolve_query_intent',
            new=AsyncMock(return_value={'kind':'general','task_type':'general','continued':False,'attempts':1})))
        actor, ack, _ = await self.launch({'_execution_lane': 'browser-one'})
        try:
            await self.until(lambda: bool(self.runtime.live[ack['run_id']]['answer']))
            run = self.runtime.get_run(actor, ack['run_id'])
            self.assertEqual(run['status'], 'running')
            self.assertIn('逐对比较', run['answer'])
        finally:
            released.set()
        await self.finish()
        self.assertEqual(run['status'], 'completed')
        reviewer.run.assert_not_awaited()

    async def test_actual_feishu_model_waits_for_complete_answer_while_web_stays_live(self):
        generated, release = asyncio.Event(), asyncio.Event()
        async def model_stream(messages, info):
            yield '冒泡排序会逐对比较'
            generated.set()
            await release.wait()
            yield '将较大的数移到右侧。'
        @asynccontextmanager
        async def factory(*_):
            yield FunctionModel(stream_function=model_stream)
        self.runtime.engine = LighthouseModel(self.portal, model_factory=factory)
        actor, ack, _ = await self.launch({'channel': 'feishu:oc_Test'})
        try:
            await asyncio.wait_for(generated.wait(), 3)
            run = self.runtime.get_run(actor, ack['run_id'])
            self.assertEqual(run['reply_mode'], 'validated')
            self.assertEqual(run['answer'], '')
            self.assertFalse(any(item['type'] == 'text-delta' for item in run['events']))
        finally:
            release.set()
        await self.finish()
        self.assertEqual(run['status'], 'completed')
        self.assertIn('将较大的数移到右侧', run['answer'])
        self.assertEqual(len([item for item in run['events'] if item['type'] == 'text-delta']), 1)

    def test_roster_wording_is_business_but_general_advice_stays_general(self):
        from openclaw_service.assistant.lighthouse_ai import is_business_query
        from openclaw_service.assistant.lighthouse_queries import business_domains
        for question in ('今天白班有多少人上班，分别是谁？', '明天夜班有哪些人？', '本周排班名单', '当班出勤有几人？'):
            with self.subTest(question=question):
                self.assertTrue(is_business_query(question))
                self.assertIn('people', business_domains(question))
        for question in ('写一首上班的诗歌', '上班效率有哪些提升建议？', '解释冒泡排序的原理'):
            with self.subTest(question=question):
                self.assertFalse(is_business_query(question))

    async def test_business_reviewer_must_finish_before_text_is_released(self):
        checking, release = asyncio.Event(), asyncio.Event()
        async def candidate(actor, turn, history, request, emit, authorize, context):
            await emit('reply-mode', {'mode': 'validated'})
            await emit('text', {'delta': '未经核对的草稿。'})
            return {'answer': '未经核对的草稿。', 'sources': []}
        async def review(*args):
            checking.set()
            await release.wait()
            return {'answer': '核对后的结果。', 'sources': []}
        reviewer = AsyncMock()
        reviewer.run.side_effect = review
        engine = LighthouseModel(self.portal, answer_reviewer=reviewer)
        engine._answer_candidate = candidate
        self.runtime.engine = engine
        actor, ack, _ = await self.launch({'_execution_lane': 'browser-one'}, '查询维修项目')
        await checking.wait()
        run = self.runtime.get_run(actor, ack['run_id'])
        self.assertEqual(run['answer'], '')
        self.assertFalse(any(e['type'] == 'text-delta' for e in run['events']))
        release.set()
        await self.finish()
        self.assertEqual(run['answer'], '核对后的结果。')

    def test_live_text_does_not_split_sensitive_identifiers(self):
        text = PublicText(live=True)
        self.assertEqual(text.push('一般说明，'), '一般说明,')
        self.assertNotIn('138', text.push('手机号138'))
        result = text.push('00138000，继续。')
        self.assertNotIn('13800138000', result)

    def test_live_english_spacing_is_retained(self):
        text = PublicText(live=True)
        result = ''.join(text.push(part) for part in ['Bubble ', 'sort ', 'compares ', 'pairs.'])
        result += text.push('', final=True)
        self.assertEqual(result, 'Bubble sort compares pairs.')

    def test_live_label_and_value_split_across_chunks_do_not_leak(self):
        for parts in (['以下是密', '码：', 'unsafe', ' value\n', '继续说明'],
                      ['password', ': ', 'unsafe', ' value\n', '继续说明']):
            text = PublicText(live=True)
            result = ''.join(text.push(part) for part in parts) + text.push('', final=True)
            self.assertNotIn('unsafe', result)
            self.assertNotIn('value', result)
            self.assertIn('继续说明', result)

    def test_live_spaced_identifier_prefix_never_leaks(self):
        text = PublicText(live=True)
        result = ''.join(text.push(part) for part in ['110101 ', '19900101 ', '1234。'])
        self.assertNotIn('110101', result)
        self.assertNotIn('19900101', result)

    def test_history_limit_never_discards_pending_parallel_runs(self):
        state = {'turns': [{'operation_id': str(index), 'status': 'pending' if index % 2 else 'completed'}
                           for index in range(260)]}
        self.assistant._trim_turns(state)
        self.assertEqual(len(state['turns']), 130)
        self.assertTrue(all(turn['status'] == 'pending' for turn in state['turns']))


class ClientLaneAuthorityTests(unittest.IsolatedAsyncioTestCase):
    setUp = bridge_fixtures.LearningBridgeTests.setUp

    async def test_browser_client_header_changes_execution_only_not_account_permissions(self):
        from lan_bitable_template_portal.lighthouse_bridge import actor_for
        actors = []
        for name in ('browser_client_one', 'browser_client_two'):
            request = Request({'type': 'http', 'headers': [(b'x-lighthouse-client', name.encode())], 'state': {}})
            actors.append(await actor_for(self.controller, self.runtime, request))
        self.assertEqual(actors[0]['id'], actors[1]['id'])
        self.assertEqual(actors[0]['scopes'], actors[1]['scopes'])
        self.assertEqual(actors[0]['modules'], actors[1]['modules'])
        self.assertNotEqual(execution_key(actors[0]), execution_key(actors[1]))

    async def test_untrusted_client_header_cannot_set_feishu_channel_and_invalid_header_is_rejected(self):
        from lan_bitable_template_portal.lighthouse_bridge import actor_for
        request = Request({'type': 'http', 'headers': [(b'x-lighthouse-client', b'feishu:oc_forged')], 'state': {}})
        with self.assertRaises(fixtures.AssistantError):
            await actor_for(self.controller, self.runtime, request)
        request = Request({'type': 'http', 'headers': [(b'x-lighthouse-client', b'forged_client_one')],
                           'state': {'lighthouse_channel': 'feishu:oc_Trusted'}})
        actor = await actor_for(self.controller, self.runtime, request)
        self.assertNotIn('_execution_lane', actor)
        self.assertTrue(execution_key(actor).startswith(actor['id'] + ':'))


if __name__ == '__main__':
    unittest.main()
