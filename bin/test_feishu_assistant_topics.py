"""Topic routing and delivery regression tests. No real Feishu or business writes."""
import asyncio
import copy
import json
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from contextlib import closing
from unittest.mock import AsyncMock, patch

from bin.test_feishu_assistant import Memory, event
from lan_bitable_template_portal.feishu_assistant import FeishuAssistant, FeishuMessenger, Inbox, parse_message, _plan_key
from lan_bitable_template_portal.feishu_notice_cards import _issue_ticket
from openclaw_service.assistant.lighthouse_ai import AssistantError


class TopicTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = Memory(Path(self.tmp.name) / 'portal.sqlite3')
        self.bot = FeishuAssistant(SimpleNamespace(), SimpleNamespace(state_store=self.store), AsyncMock())
        self.bot.inbox = Inbox(Path(self.tmp.name) / 'messages.sqlite3')
        self.actor = {'id': 'ou_Person', 'scopes': ['E'], 'modules': ['assistant', 'maintenance'], 'is_admin': False}
        self.request = SimpleNamespace(state=SimpleNamespace())
        self.message = parse_message(event(chat_type='group', message_id='Root'), 'cliTest', 'ouBot')
        self.bot.inbox.add(self.message)
        self.route = {'chat_id': 'oc_Chat', 'root_id': self.message['message_id'], 'thread_id': 'omt_Topic', 'chat_type': 'group'}
        self.plans = {}
        self.conversation = {'conversation_id': 'own', 'turns': []}
        self.bot._request_context = AsyncMock(return_value=(self.request, self.actor))
        async def call(request, actor, path, body=None, **kwargs):
            if path == 'conversation':
                return copy.deepcopy(self.conversation)
            if path.startswith('plans/'):
                return copy.deepcopy(self.plans[path.split('/')[1]])
            return {}
        self.bot._call = AsyncMock(side_effect=call)

    def followup(self, question='progress: 50'):
        raw = event(chat_type='group', message_id='Followup', content=question)
        raw['event']['message'].update(root_id=self.message['message_id'], thread_id='omt_Topic', mentions=[])
        return parse_message(raw, 'cliTest', 'ouBot', inbox=self.bot.inbox)

    def bind(self, identity, title='Topic plan', card_id=None):
        plan = {'id': identity, 'title': title, 'status': 'needs_input', 'version': 1,
                'fields': [{'name': 'progress', 'type': 'string'}], 'operations': []}
        self.plans[identity] = plan
        ticket = _issue_ticket(self.bot, self.message, self.actor, self.conversation, plan)
        reply = {'text': title, 'ticket': ticket, 'topic_plan_id': identity, 'thread_route': self.route,
                 'card_message_id': card_id or 'om_' + identity, 'thread_messages': [card_id or 'om_' + identity]}
        self.bot.inbox.set(self.message['message_id'], 'reply', reply)
        return ticket

    def test_followup_is_durable_before_model_reply_and_foreign_root_rejected(self):
        message = self.followup('还有一个条件')
        self.assertIsNotNone(message)
        self.assertTrue(self.bot.inbox.add(message))
        self.assertFalse(self.bot.inbox.add(message))
        restored = Inbox(self.bot.inbox.path)
        self.assertTrue(restored.has_thread('oc_Chat', self.message['message_id'], 'omt_Topic'))
        self.assertFalse(restored.has_thread('oc_Other', self.message['message_id'], 'omt_Topic'))

    async def test_field_lines_only_use_this_topic_not_private_plan(self):
        self.bind('topicA')
        self.conversation['turns'] = [{'plan': {'id': 'privateB', 'status': 'needs_input', 'fields': [{'name': 'progress', 'type': 'string'}]}}]
        self.bot._fill_fields = AsyncMock(return_value='stored')
        self.bot._attach_form_card = AsyncMock()
        with patch('lan_bitable_template_portal.feishu_assistant_files.handle_files', AsyncMock(return_value=None)):
            await self.bot.answer(self.followup())
        self.assertEqual(list(self.bot._fill_fields.call_args.args[3]), ['topicA'])

    async def test_unknown_topic_has_no_private_photo_target(self):
        self.conversation['turns'] = [{'plan': {'id': 'privateB', 'status': 'needs_input'}}]
        message = self.followup('请读取附件，结合当前会话继续处理。')
        message['content'] = {'content': [[{'tag': 'img', 'image_key': 'img_fixture'}]]}
        handle = AsyncMock(return_value={'text': 'attachment received'})
        with patch('lan_bitable_template_portal.feishu_assistant_files.handle_files', handle):
            await self.bot.answer(message)
        self.assertEqual(handle.call_args.args[4]['turns'], [])

    async def test_multiple_plans_require_selection_and_card_reference_selects_exactly(self):
        self.bind('A', card_id='om_CardA')
        self.bind('B', card_id='om_CardB')
        message = self.followup('确认执行')
        topic = self.bot.inbox.thread_for(message)
        scoped, prompt = await self.bot._topic_plan(message, self.request, self.actor, self.conversation, topic)
        self.assertIn('多项待办', prompt)
        self.assertFalse(scoped['turns'])
        message['parent_id'] = 'om_CardA'
        scoped, prompt = await self.bot._topic_plan(message, self.request, self.actor, self.conversation, topic)
        self.assertIsNone(prompt)
        self.assertEqual(scoped['turns'][0]['plan']['id'], 'A')

    async def test_number_selection_is_scoped_to_person_and_topic(self):
        self.bind('A'); self.bind('B')
        await self.bot._topic_plan(self.followup('查看当前计划'), self.request, self.actor, self.conversation, self.route)
        message = self.followup('选择计划 2')
        scoped, prompt = await self.bot._topic_plan(message, self.request, self.actor, self.conversation, self.route)
        self.assertIsNone(prompt)
        self.assertEqual(scoped['turns'][0]['plan']['id'], 'B')
        self.assertNotIn(self.actor['id'], self.bot.picked_plan)
        other = {**self.actor, 'id': 'ou_Other'}
        _, prompt = await self.bot._topic_plan(self.followup('确认执行'), self.request, other, self.conversation, self.route)
        self.assertIn('多项待办', prompt)

    async def test_reply_to_old_card_cannot_confirm_newer_version(self):
        self.bind('A', card_id='om_Old')
        self.bind('A', card_id='om_New')
        message = self.followup('确认执行')
        message['parent_id'] = 'om_Old'
        scoped, prompt = await self.bot._topic_plan(message, self.request, self.actor, self.conversation, self.route)
        self.assertIn('已更新或结束', prompt)
        self.assertFalse(scoped['turns'])

    async def test_file_notice_keeps_card_identity_and_explicit_replies_choose_their_plan(self):
        ticket = self.bind('A')
        self.bind('B')
        reply = {'text': 'A计划完整说明', 'card': {'body': {'elements': [{'tag': 'markdown', 'content': 'A表单'}]}},
                 'ticket': ticket, 'topic_plan_id': 'A', 'file_notice': '附件未上传，请检查权限。'}
        receipts = iter(('om_BodyA', 'om_CardA', 'om_NoticeA'))
        messenger = FeishuMessenger({})
        client = SimpleNamespace(post=AsyncMock(side_effect=lambda *a, **kw: SimpleNamespace(json=lambda: {
            'code': 0, 'data': {'message_id': next(receipts), 'chat_id': 'oc_Chat',
                                'root_id': self.route['root_id'], 'thread_id': 'omt_Topic'}})))
        messenger.client = AsyncMock(return_value=client)
        messenger.headers = AsyncMock(return_value={})
        async def checkpoint():
            self.bot.inbox.set(self.message['message_id'], 'reply', reply)
        with patch('lan_bitable_template_portal.portal_service.external_real_write_guard', return_value={'real_write_allowed': True}):
            await messenger.send(self.message, reply, checkpoint=checkpoint)
        self.assertEqual(reply['card_message_id'], 'om_CardA')
        for reference in ('om_CardA', 'om_BodyA', 'om_NoticeA'):
            message = self.followup('确认执行')
            message['parent_id'] = reference
            self.bot.picked_plan[_plan_key(message, self.actor)] = 'B'
            scoped, prompt = await self.bot._topic_plan(message, self.request, self.actor, self.conversation, self.route)
            self.assertIsNone(prompt)
            self.assertEqual(scoped['turns'][0]['plan']['id'], 'A')

    async def test_selection_numbers_use_previously_shown_snapshot(self):
        self.bind('B'); self.bind('C')
        await self.bot._topic_plan(self.followup('查看当前计划'), self.request, self.actor, self.conversation, self.route)
        self.bind('A')
        scoped, prompt = await self.bot._topic_plan(self.followup('选择计划 2'), self.request, self.actor, self.conversation, self.route)
        self.assertEqual(scoped['turns'][0]['plan']['id'], 'C')

    async def test_each_part_rechecks_authority_and_keeps_sent_checkpoint(self):
        messenger = FeishuMessenger({})
        client = SimpleNamespace(post=AsyncMock(return_value=SimpleNamespace(json=lambda: {'code': 0, 'data': {'message_id': 'om_Receipt'}})))
        messenger.client = AsyncMock(return_value=client)
        messenger.headers = AsyncMock(return_value={})
        guard = AsyncMock(side_effect=[None, AssistantError('revoked', 403)])
        reply = {'text': 'allowed before revoke', 'files': [{'id': 'fixture', 'file_key': 'fixture-key'}]}
        with patch('lan_bitable_template_portal.portal_service.external_real_write_guard', return_value={'real_write_allowed': True}):
            with self.assertRaises(AssistantError):
                await messenger.send(self.message, reply, authorize=guard)
        self.assertEqual(client.post.await_count, 1)
        self.assertEqual(reply['sent_parts'], ['answer:0'])

    async def test_completed_plan_is_retired_but_failed_plan_can_retry(self):
        self.bind('A'); self.bind('B')
        self.plans['A']['status'] = 'completed'
        self.plans['B']['status'] = 'failed'
        scoped, prompt = await self.bot._topic_plan(self.followup('重试操作'), self.request, self.actor, self.conversation, self.route)
        self.assertEqual(scoped['turns'][0]['plan']['id'], 'B')
        self.assertEqual([p['plan_id'] for p in self.bot.inbox.topic_plans(self.followup())], ['B'])

    def test_topic_context_only_contains_delivered_permitted_public_content(self):
        reply = {'text': 'Public fixture', 'thread_route': self.route, 'delivery_access': self.bot._delivery_access(self.actor)}
        self.assertEqual(self.bot.inbox.topic_context(self.followup(), self.actor), '')
        self.bot.inbox.remember_public_reply(self.message, reply)
        self.assertIn('Public fixture', self.bot.inbox.topic_context(self.followup(), self.actor))
        self.assertEqual(self.bot.inbox.topic_context(self.followup(), {**self.actor, 'scopes': ['D']}), '')
        self.assertEqual(self.bot.inbox.topic_context({**self.followup(), 'chat_id': 'oc_Other'}, self.actor), '')

    async def test_revoked_or_cleared_reply_never_sends_cached_content(self):
        for change in ('scope', 'disabled', 'conversation'):
            message = {**self.message, 'message_id': 'om_' + change}
            self.bot.inbox.add(message)
            reply = {'text': 'PRIVATE-BUSINESS-FIXTURE', 'actor_id': self.actor['id'], 'conversation_id': 'own',
                     'delivery_access': self.bot._delivery_access(self.actor)}
            self.bot.inbox.set(message['message_id'], 'reply', reply)
            current = {**self.actor, 'scopes': []} if change == 'scope' else self.actor
            self.bot._request_context = AsyncMock(side_effect=AssistantError('disabled', 403)) if change == 'disabled' else AsyncMock(return_value=(self.request, current))
            self.conversation['conversation_id'] = 'changed' if change == 'conversation' else 'own'
            self.bot.messenger = SimpleNamespace(send=AsyncMock())
            await self.bot._drain(message['union_id'])
            sent = [call.args[1]['text'] for call in self.bot.messenger.send.await_args_list]
            self.assertNotIn('PRIVATE-BUSINESS-FIXTURE', sent)
            self.assertTrue(any('停止发送' in text for text in sent))

    async def test_unchanged_context_sends_once_without_reasking_model(self):
        reply = {'text': 'permitted', 'actor_id': self.actor['id'], 'conversation_id': 'own',
                 'delivery_access': self.bot._delivery_access(self.actor)}
        self.bot.inbox.set(self.message['message_id'], 'reply', reply)
        self.bot.answer = AsyncMock()
        self.bot.messenger = SimpleNamespace(send=AsyncMock())
        await self.bot._drain(self.message['union_id'])
        self.bot.answer.assert_not_awaited()
        self.assertEqual(self.bot.messenger.send.call_args.args[1]['text'], 'permitted')

    def test_retries_stop_and_cleanup_preserves_dedup_without_old_text(self):
        for index in range(8):
            stopped = self.bot.inbox.defer_delivery(self.message['message_id'])
            self.assertEqual(stopped, index == 7)
        self.bot.inbox.cleanup(time.time() + 31 * 86400)
        self.assertFalse(self.bot.inbox.add(self.message))
        with self.bot.inbox.connect() as db:
            state, payload, reply = db.execute('SELECT state,payload,reply FROM messages WHERE id=?', (self.message['message_id'],)).fetchone()
        self.assertEqual((state, payload, reply), ('archived', '{}', None))

    def test_migrates_existing_inbox_without_erasing_pending_messages(self):
        old = Path(self.tmp.name) / 'old.sqlite3'
        with closing(sqlite3.connect(old)) as db, db:
            db.execute('CREATE TABLE messages(id TEXT PRIMARY KEY,owner TEXT,payload TEXT,state TEXT,reply TEXT,retry_at REAL DEFAULT 0)')
            db.execute('INSERT INTO messages VALUES(?,?,?,?,?,0)', ('om_old', 'on_Person', json.dumps(self.message), 'pending', None))
        restored = Inbox(old)
        restored.cleanup()
        self.assertIsNotNone(restored.take('on_Person'))

    async def test_permanent_delivery_error_not_retried(self):
        messenger = FeishuMessenger({})
        client = SimpleNamespace(post=AsyncMock(return_value=SimpleNamespace(json=lambda: {'code': 230013})))
        messenger.client = AsyncMock(return_value=client)
        messenger.headers = AsyncMock(return_value={})
        self.bot.messenger = messenger
        self.bot.inbox.set(self.message['message_id'], 'reply', {'text': 'safe fixture'})
        with patch('lan_bitable_template_portal.portal_service.external_real_write_guard', return_value={'real_write_allowed': True}):
            await self.bot._drain(self.message['union_id'])
            await self.bot._drain(self.message['union_id'])
        self.assertEqual(client.post.await_count, 1)
        self.assertEqual(self.bot.inbox.owners(), [])


if __name__ == '__main__':
    unittest.main()
