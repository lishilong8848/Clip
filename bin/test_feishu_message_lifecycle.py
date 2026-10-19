"""Private-message recovery and permission-filtered public topic context, offline."""
import asyncio
import copy
import hashlib
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from fastapi import FastAPI
from bin.test_feishu_assistant import event
from bin.test_lighthouse_stream import Store, Engine
from lan_bitable_template_portal.feishu_assistant import FeishuAssistant, Inbox, parse_message
from openclaw_service.assistant.lighthouse_ai import AssistantError, LighthouseAssistant
from openclaw_service.assistant.lighthouse_api import PortalAPICatalog
from openclaw_service.assistant.lighthouse_agent import PortalAgent
from openclaw_service.assistant.lighthouse_files import LighthouseFiles
from openclaw_service.assistant.lighthouse_stream import LighthouseStream
from openclaw_service.assistant.lighthouse_openclaw import OpenClawToolAgent
from bin.test_lighthouse_openclaw import _fake_engine


class MessageLifecycleTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name)
        self.store = Store(self.path / 'fixture.sqlite3')
        self.bot = FeishuAssistant(SimpleNamespace(), SimpleNamespace(state_store=self.store), AsyncMock())
        self.bot.inbox = Inbox(self.path / 'inbox.sqlite3')
        self.bot.messenger = SimpleNamespace(react=AsyncMock(), send=AsyncMock())
        self.actor = {'id': 'ou_Actual', 'scopes': ['D'], 'modules': ['assistant'], 'is_admin': False}

    def message(self, *, owner='first', root=None, chat='oc_Chat', app='cliTest', group=False, identity='First'):
        raw = event(owner, chat_type='group' if group else 'p2p', message_id=identity, content='original question')
        raw['header']['app_id'] = app
        raw['event']['message']['chat_id'] = chat
        if root:
            raw['event']['message']['root_id'] = root
        message = parse_message(raw, app, 'ouBot', inbox=self.bot.inbox)
        self.assertIsNotNone(message)
        self.bot.inbox.add(message)
        return message

    async def test_private_timeout_sends_unknown_outcome_not_fake_failure_or_success(self):
        message = self.message()
        async def answer(_):
            await asyncio.Event().wait()
        self.bot.answer = AsyncMock(side_effect=answer)
        await self.bot._react(message)
        with patch('lan_bitable_template_portal.feishu_assistant._MESSAGE_TIMEOUT', .1):
            await self.bot._drain(message['union_id'])
        self.bot.messenger.react.assert_awaited_once()
        text = self.bot.messenger.send.await_args.args[1]['text']
        self.assertIn('状态尚未确认', text)
        self.assertNotIn('诊断编号', text)
        self.assertNotIn('操作已完成', text)
        self.assertEqual(self.bot.inbox.owners(), [])

    async def test_cancel_after_reaction_saves_terminal_notice_without_reasking(self):
        message, entered = self.message(), asyncio.Event()
        async def answer(_):
            entered.set()
            await asyncio.Event().wait()
        self.bot.answer = AsyncMock(side_effect=answer)
        task = asyncio.create_task(self.bot._drain(message['union_id']))
        await entered.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.bot.inbox.set(message['message_id'], 'reply', delay=0)
        await self.bot._drain(message['union_id'])
        self.bot.answer.assert_awaited_once()
        self.bot.messenger.send.assert_awaited_once()
        self.assertIn('状态尚未确认', self.bot.messenger.send.await_args.args[1]['text'])

    async def test_cancel_during_delivery_and_restart_preserves_form_and_checkpoints(self):
        message, entered = self.message(), asyncio.Event()
        reply = {'text': 'Original task still running', 'topic_plan_id': 'original-plan',
                 'topic_plan_status': 'submitted', 'sent_parts': ['answer:0'], 'ticket': 'original-ticket'}
        self.bot.answer = AsyncMock(return_value=reply)
        async def send(*args, **kwargs):
            entered.set()
            await asyncio.Event().wait()
        self.bot.messenger.send.side_effect = send
        task = asyncio.create_task(self.bot._drain(message['union_id']))
        await entered.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.bot.inbox = Inbox(self.bot.inbox.path)
        self.assertEqual(self.bot.inbox.reply_for(message['message_id']), reply)
        self.bot.inbox.set(message['message_id'], 'reply', delay=0)
        self.bot.messenger.send.side_effect = None
        await self.bot._drain(message['union_id'])
        self.bot.answer.assert_awaited_once()
        self.assertEqual(self.bot.messenger.send.await_args.args[1]['topic_plan_id'], 'original-plan')

    async def test_restart_does_not_reexecute_an_interrupted_business_message(self):
        message = self.message()
        self.bot.inbox.take(message['union_id'])
        self.bot.inbox = Inbox(self.bot.inbox.path)
        self.bot.answer = AsyncMock()
        await self.bot._drain(message['union_id'])
        self.bot.answer.assert_not_awaited()
        self.assertIn('状态尚未确认', self.bot.messenger.send.await_args.args[1]['text'])

    async def test_failed_turn_cannot_publish_a_partial_success_claim(self):
        message = self.message()
        operation = 'feishu_' + hashlib.sha256(message['message_id'].encode()).hexdigest()[:40]
        request = SimpleNamespace(state=SimpleNamespace())
        self.bot._request_context = AsyncMock(return_value=(request, copy.deepcopy(self.actor)))
        self.bot._call = AsyncMock(side_effect=[{'conversation_id': 'original', 'turns': []},
            {'turns': [{'operation_id': operation, 'status': 'failed', 'answer': '操作已完成。',
                        'error': '模型未完成，原任务结果尚未确认。'}]}])
        with patch('lan_bitable_template_portal.feishu_assistant_files.handle_files', AsyncMock(return_value=None)):
            reply = await self.bot.answer(message)
        self.assertNotIn('操作已完成', reply['text'])
        self.assertIn('原任务结果尚未确认', reply['text'])

    async def test_same_topic_waits_for_previous_delivery_but_other_topics_run_in_parallel(self):
        first = self.message(group=True)
        second = self.message(owner='second', group=True, root=first['message_id'], identity='Second')
        entered, release = asyncio.Event(), asyncio.Event()
        contexts = []
        async def answer(message):
            contexts.append((message['sender_id'], self.bot.inbox.topic_context(message, self.actor)))
            return {'text': 'Public answer ' + message['message_id'], 'delivery_access': self.bot._delivery_access(self.actor),
                'thread_route': {'chat_id': message['chat_id'], 'root_id': message.get('root_id') or message['message_id'],
                                 'thread_id': 'omt_Test', 'chat_type': 'group'}}
        async def send(message, reply, **_):
            if message['message_id'] == first['message_id']:
                entered.set()
                await release.wait()
        self.bot.answer = AsyncMock(side_effect=answer)
        self.bot.messenger.send.side_effect = send
        task1 = asyncio.create_task(self.bot._drain(first['union_id']))
        await entered.wait()
        task2 = asyncio.create_task(self.bot._drain(second['union_id']))
        await asyncio.sleep(.02)
        self.assertEqual(len(contexts), 1)
        other = self.message(owner='third', group=True, identity='Other')
        await self.bot._drain(other['union_id'])
        self.assertEqual(len(contexts), 2)
        release.set()
        await asyncio.gather(task1, task2)
        self.assertIn('Public answer om_First', contexts[-1][1])
        self.assertFalse(self.bot.topic_locks)

    async def test_topic_context_is_app_chat_root_and_current_permission_scoped(self):
        message = self.message(group=True)
        reply = {'text': 'Authorized public document summary', 'delivery_access': self.bot._delivery_access(self.actor),
                 'files': [{'id': 'not-authorized-for-other-person'}], 'thread_route': {
                     'chat_id': message['chat_id'], 'root_id': message['message_id'], 'thread_id': 'omt_Test', 'chat_type': 'group'}}
        self.bot.inbox.remember_public_reply(message, reply)
        follow = self.message(owner='second', group=True, identity='Second', root=message['message_id'])
        self.assertIn('public document', self.bot.inbox.topic_context(follow, self.actor))
        self.assertNotIn('not-authorized', self.bot.inbox.topic_context(follow, self.actor))
        for changes in ({'app_id': 'cliOther'}, {'chat_id': 'oc_Other'}, {'root_id': 'om_Other'}):
            self.assertEqual(self.bot.inbox.topic_context({**follow, **changes}, self.actor), '')
        self.assertEqual(self.bot.inbox.topic_context(follow, {**self.actor, 'scopes': ['A']}), '')
        self.assertEqual(self.bot.inbox.topic_context(follow, {**self.actor, 'modules': []}), '')
        self.assertEqual(self.bot.inbox.topic_context(follow, {**self.actor, 'id': 'ou_Other'}),
                         self.bot.inbox.topic_context(follow, self.actor))
        request1, request2 = SimpleNamespace(state=SimpleNamespace()), SimpleNamespace(state=SimpleNamespace())
        actor1, actor2 = copy.deepcopy(self.actor), {**self.actor, 'id': 'ou_Other'}
        await self.bot._prepare_topic(message, request1, actor1)
        await self.bot._prepare_topic(follow, request2, actor2)
        self.assertEqual(request1.state.feishu_topic['id'], request2.state.feishu_topic['id'])
        self.assertEqual(actor1['id'], self.actor['id'])
        self.assertEqual(actor2['scopes'], self.actor['scopes'])
        self.assertFalse(actor2['is_admin'])

    async def test_unknown_group_topic_never_uses_personal_context_and_foreign_app_is_not_known(self):
        message = self.message(group=True)
        changed = {**message, 'app_id': 'cliOther'}
        self.assertFalse(self.bot.inbox.has_thread(message['chat_id'], message['message_id'], '', app_id='cliOther'))
        request, actor = SimpleNamespace(state=SimpleNamespace()), {**self.actor, 'channel': 'feishu:oc_Chat'}
        unknown = {**message, 'message_id': 'om_UnknownFollowup', 'root_id': 'om_UnknownRoot'}
        await self.bot._prepare_topic(unknown, request, actor)
        self.assertIn('om_UnknownRoot', actor['_feishu_topic']['id'])
        self.assertEqual(actor['_feishu_topic']['context'], '')
        self.assertNotEqual(LighthouseAssistant._key(actor), LighthouseAssistant._key(self.actor))
        self.bot.config = {'app_id': 'cliTest'}
        self.bot._resolve_user = AsyncMock()
        with self.assertRaisesRegex(Exception, '此前的飞书应用'):
            await self.bot._request_context(changed)
        self.bot._resolve_user.assert_not_awaited()

    async def test_admin_public_history_is_not_an_implicit_admin_grant(self):
        message = self.message(group=True)
        reply = {'text': 'Admin-only public fixture', 'delivery_access': {**self.bot._delivery_access(self.actor), 'is_admin': True},
                 'thread_route': {'chat_id': message['chat_id'], 'root_id': message['message_id'],
                                  'thread_id': 'omt_Test', 'chat_type': 'group'}}
        self.bot.inbox.remember_public_reply(message, reply)
        self.assertEqual(self.bot.inbox.topic_context(message, self.actor), '')
        self.assertIn('Admin-only', self.bot.inbox.topic_context(message, {**self.actor, 'is_admin': True}))

    async def test_document_history_requires_document_owner_not_just_equal_building_permissions(self):
        message = self.message(group=True)
        reply = {'text': 'Owner-only document summary',
                 'delivery_access': {**self.bot._delivery_access(self.actor), 'source_owner': self.actor['id']},
                 'thread_route': {'chat_id': message['chat_id'], 'root_id': message['message_id'],
                                  'thread_id': 'omt_Test', 'chat_type': 'group'}}
        self.bot.inbox.remember_public_reply(message, reply)
        self.assertIn('Owner-only', self.bot.inbox.topic_context(message, self.actor))
        self.assertEqual(self.bot.inbox.topic_context(message, {**self.actor, 'id': 'ou_Other'}), '')

    async def test_old_topic_card_lookup_reuses_only_exact_verified_plan_and_does_not_retry_writes(self):
        topic = {'id': 'oc_Chat:om_Root:app:0123456789abcdef', 'context': 'public'}
        request = SimpleNamespace(state=SimpleNamespace(feishu_topic=topic,
            feishu_shared_plan={'id': 'old-plan', 'owner': 'ou_Origin', 'conversation_id': 'old-conversation', 'chat_id': 'oc_Chat'}))
        calls = []
        async def call(request, actor, path, body=None, *_, **kwargs):
            calls.append((path, body, copy.deepcopy(request.state.feishu_topic), copy.deepcopy(actor)))
            if request.state.feishu_topic:
                raise AssistantError('原会话已清空，请在当前会话重新准备操作。', 409)
            return {'id': 'old-plan', 'status': 'submitted'}
        self.bot._call_once = AsyncMock(side_effect=call)
        self.assertEqual((await self.bot._call(request, self.actor, 'plans/old-plan'))['id'], 'old-plan')
        await self.bot._call(request, self.actor, 'plans/old-plan/confirm', {'version': 1}, method='POST')
        self.assertEqual(len(calls), 3)
        self.assertEqual(sum(body is not None for _, body, _, _ in calls), 1)
        self.assertIs(request.state.feishu_topic, topic)
        self.assertTrue(all(actor == self.actor for _, _, _, actor in calls))
        request.state.feishu_legacy_plan = None
        self.bot._call_once = AsyncMock(side_effect=AssistantError('原会话已清空，请在当前会话重新准备操作。', 409))
        with self.assertRaises(AssistantError):
            await self.bot._call(request, self.actor, 'plans/old-plan/confirm', {'version': 1}, method='POST')
        self.bot._call_once.assert_awaited_once()
        self.bot._call_once.reset_mock()
        with self.assertRaises(AssistantError):
            await self.bot._call(request, self.actor, 'plans/unrelated-plan')
        self.bot._call_once.assert_awaited_once()


class TopicInferenceIsolationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = Store(Path(self.tmp.name) / 'fixture.sqlite3')
        model = Mock()
        model.settings.return_value = {'enabled': True, 'active_model_id': 'fixture', 'models': [
            {'id': 'fixture', 'name': 'Fixture', 'model': 'fixture', 'configured': True}]}
        model.profile.return_value = {'id': 'fixture', 'name': 'Fixture', 'model': 'fixture'}
        self.assistant = LighthouseAssistant(self.store, Mock(return_value=([], [])), model=model)
        portal = PortalAgent(self.assistant, PortalAPICatalog(FastAPI()), LighthouseFiles(self.store))
        self.engine = Engine()
        self.stream = LighthouseStream(portal, engine=self.engine)
        self.addAsyncCleanup(self.stream.close)
        self.person = {'id': 'ou_Actual', 'scopes': ['D'], 'modules': ['assistant'], 'is_admin': False}
        self.counter = 0

    def topic_actor(self, owner='ou_Actual', topic='oc_Test:om_Root:app:0123456789abcdef', context=''):
        return {**self.person, 'id': owner, 'channel': 'feishu:oc_Test', '_feishu_topic': {'id': topic, 'context': context}}

    async def ask(self, actor, question):
        self.counter += 1
        async def authorize():
            return copy.deepcopy(actor)
        payload = {'question': question, 'file_ids': [], 'operation_id': 'offline_message_%04d' % self.counter,
                   'conversation_id': self.assistant._state(actor)['id']}
        await self.stream.submit(actor, payload, None, authorize)
        await asyncio.gather(*self.stream.workers.values())

    async def test_group_context_never_uses_private_web_history_or_other_topic_history(self):
        await self.ask(self.person, 'private web secret fixture')
        first = self.topic_actor(context='Topic public answer: D楼变更甲乙')
        await self.ask(first, '分别是哪些？')
        second = self.topic_actor('ou_Second', context='Topic public answer: D楼变更甲乙')
        await self.ask(second, '分别是哪些？')
        for call in self.engine.calls[-2:]:
            self.assertEqual(call['history'], [])
            self.assertIn('D楼变更甲乙', call['turn']['prompt'])
            self.assertNotIn('private web secret', call['turn']['prompt'])
        self.assertEqual(self.engine.calls[-1]['actor']['id'], 'ou_Second')
        other = self.topic_actor(topic='oc_Test:om_Other:app:0123456789abcdef')
        await self.ask(other, '分别是哪些？')
        self.assertNotIn('D楼变更甲乙', self.engine.calls[-1]['turn']['prompt'])
        personal = self.assistant._state(self.person)
        self.assertEqual(len(personal['turns']), 1)
        self.assertEqual(personal['turns'][0]['question'], 'private web secret fixture')
        private_actor = {**self.person, 'channel': 'feishu:oc_Private'}
        self.assertEqual(self.assistant._key(private_actor), self.assistant._key(self.person))
        self.assertNotEqual(self.assistant._key(first), self.assistant._key(self.person))

    async def test_topic_model_sessions_change_with_public_view_and_never_reuse_personal_native_history(self):
        engine = _fake_engine()
        turn = {'operation_id': 'original-operation', 'run_id': 'run-one'}
        actor = self.topic_actor(context='Current allowed document')
        private = OpenClawToolAgent(engine, self.person, turn, AsyncMock(), {}, instructions='policy')
        first = OpenClawToolAgent(engine, actor, turn, AsyncMock(), {}, instructions='policy')
        changed = OpenClawToolAgent(engine, actor, {**turn, 'run_id': 'run-two'}, AsyncMock(), {}, instructions='policy')
        self.assertNotEqual(private.session_key, first.session_key)
        self.assertNotEqual(first.session_key, changed.session_key)

    async def test_topic_scope_or_module_revocation_prevents_old_prompt_reuse(self):
        actor = self.topic_actor(context='private-to-D document')
        await self.ask(actor, 'D楼进行中的变更有哪些？')
        actor['modules'], actor['_feishu_topic']['context'] = [], ''
        await self.ask(actor, '分别是哪些？')
        self.assertNotIn('private-to-D', self.engine.calls[-1]['turn']['prompt'])
        self.assertNotIn('D楼进行中的变更', self.engine.calls[-1]['turn']['prompt'])


if __name__ == '__main__':
    unittest.main()
