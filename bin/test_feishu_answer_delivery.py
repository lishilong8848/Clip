"""Offline final-answer delivery, including lost responses and long/card replies."""
import asyncio
import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from bin.test_feishu_assistant import event
from bin.test_lighthouse_stream import Store
from lan_bitable_template_portal.feishu_assistant import FeishuAssistant, FeishuMessenger, Inbox, parse_message
from openclaw_service.assistant.lighthouse_ai import AssistantError, safe_text


class DeliveryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.messenger = FeishuMessenger({})
        self.calls = []
        async def post(url, **kwargs):
            self.calls.append((url, copy.deepcopy(kwargs['json'])))
            return SimpleNamespace(json=lambda: {'code': 0, 'data': {
                'message_id': 'om_Receipt', 'root_id': 'om_Root', 'thread_id': 'omt_Thread', 'chat_id': 'oc_Chat'}})
        self.client = SimpleNamespace(post=AsyncMock(side_effect=post))
        self.messenger.client = AsyncMock(return_value=self.client)
        self.messenger.headers = AsyncMock(return_value={})
        self.messenger.upload_file = AsyncMock(side_effect=AssertionError('No mandatory file upload for a text answer'))
        self.message = parse_message(event(chat_type='group'), 'cliTest', 'ouBot')
        self.message.update(root_id='om_Root', thread_id='omt_Thread')
        self.guard = patch('lan_bitable_template_portal.portal_service.external_real_write_guard', return_value={'real_write_allowed': True})
        self.guard.start()
        self.addCleanup(self.guard.stop)

    async def test_long_answer_is_complete_in_original_topic_not_replaced_by_attachment(self):
        text = '# 上周参与人员\n' + ''.join(f'人员{i}：已答题15题。\n' for i in range(800))
        reply = {'text': text}
        await self.messenger.send(self.message, reply)
        await self.messenger.send(self.message, reply)
        restored = ''.join(json.loads(body['content'])['body']['elements'][0]['content'] for _, body in self.calls)
        self.assertEqual(restored, safe_text(text, limit=120000).strip())
        self.assertGreater(len(self.calls), 1)
        self.assertTrue(all(url.endswith('/om_Root/reply') and body['reply_in_thread'] for url, body in self.calls))
        self.assertTrue(all(len(json.dumps(body).encode()) < 30000 for _, body in self.calls))
        self.assertEqual(len(set(body['uuid'] for _, body in self.calls)), len(self.calls))
        self.messenger.upload_file.assert_not_awaited()

    async def test_html_disclosure_keeps_names_and_basis_readable_in_original_topic(self):
        reply = {'text': '夜班名单\n<details><summary>口径说明</summary>测试甲、测试乙；排班口径。</details>'}
        await self.messenger.send(self.message, reply)
        content = ''.join(json.loads(body['content'])['body']['elements'][0]['content'] for _, body in self.calls)
        self.assertNotIn('<details>', content)
        self.assertNotIn('<summary>', content)
        self.assertIn('口径说明', content)
        self.assertIn('测试甲、测试乙', content)
        self.assertTrue(all(url.endswith('/om_Root/reply') for url, _ in self.calls))

    async def test_question_form_does_not_hide_answer_tail_or_sources(self):
        reply = {'text': '完整统计和来源，不只是表单开头', 'card': {'schema': '2.0', 'body': {
            'elements': [{'tag': 'markdown', 'content': '表单开头'}]}}}
        await self.messenger.send(self.message, reply)
        self.assertEqual(len(self.calls), 2)
        self.assertIn(safe_text(reply['text']), self.calls[0][1]['content'])
        self.assertIn('表单开头', self.calls[1][1]['content'])

    async def test_normalized_question_form_does_not_repeat_same_question_as_separate_reply(self):
        reply = {'text': '请补充范围\n<details><summary>选择回答</summary>甲或乙</details>',
                 'card': {'schema': '2.0', 'body': {'elements': [{'tag': 'markdown',
                    'content': '请补充范围\n<details><summary>选择回答</summary>甲或乙</details>'}]}}}
        await self.messenger.send(self.message, reply)
        await self.messenger.send(self.message, reply)
        self.assertEqual(len(self.calls), 1)

    async def test_form_without_markdown_does_not_hide_empty_answer_failure_notice(self):
        reply = {'text': '', 'card': {'schema': '2.0', 'body': {'elements': []}}}
        await self.messenger.send(self.message, reply)
        self.assertEqual(len(self.calls), 2)
        self.assertIn('本轮暂未生成回答', self.calls[0][1]['content'])

    async def test_mid_answer_disconnect_retries_only_missing_part_with_same_uuid(self):
        reply = {'text': '答' * 8000}
        original = self.client.post.side_effect
        attempts = []
        async def post(url, **kwargs):
            attempts.append(kwargs['json']['uuid'])
            if len(attempts) == 2:
                raise TimeoutError()
            return await original(url, **kwargs)
        self.client.post.side_effect = post
        with self.assertRaises(TimeoutError):
            await self.messenger.send(self.message, reply)
        self.assertEqual(reply['sent_parts'], ['answer:0'])
        await self.messenger.send(self.message, reply)
        self.assertEqual(attempts[1], attempts[2])
        self.assertEqual(len(self.calls), 3)

    async def test_rejected_markdown_falls_back_to_readable_text_not_silent_stop(self):
        self.client.post.side_effect = [SimpleNamespace(json=lambda: {'code': 230099}),
            SimpleNamespace(json=lambda: {'code': 0, 'data': {'message_id': 'om_Receipt'}})]
        await self.messenger.send(self.message, {'text': '| 姓名 | 答题数 |\n| 人员甲 | 15 |'})
        first, second = self.client.post.await_args_list
        self.assertEqual(second.kwargs['json']['msg_type'], 'text')
        self.assertIn('人员甲', second.kwargs['json']['content'])
        self.assertEqual(first.kwargs['json']['uuid'], second.kwargs['json']['uuid'])

    async def test_form_rejection_does_not_resend_already_delivered_complete_answer(self):
        success = SimpleNamespace(json=lambda: {'code': 0, 'data': {'message_id': 'om_Receipt'}})
        self.client.post.side_effect = [success, SimpleNamespace(json=lambda: {'code': 230001}), success]
        reply = {'text': 'Complete answer with all participants', 'card': {'schema': '2.0', 'body': {
            'elements': [{'tag': 'markdown', 'content': 'Short form'}]}}}
        await self.messenger.send(self.message, reply)
        await self.messenger.send(self.message, reply)
        bodies = [json.loads(call.kwargs['json']['content']) for call in self.client.post.await_args_list]
        self.assertEqual(len(bodies), 3)
        self.assertIn(reply['text'], str(bodies[0]))
        self.assertNotIn(reply['text'], str(bodies[2]))
        self.assertIn('交互表单暂未显示', str(bodies[2]))

    async def test_no_receipt_or_wrong_chat_never_marks_answer_delivered(self):
        for data in ({}, {'message_id': 'om_Receipt', 'chat_id': 'oc_Wrong'}):
            self.client.post.side_effect = None
            self.client.post.return_value = SimpleNamespace(json=lambda: {'code': 0, 'data': data})
            reply = {'text': 'private answer'}
            with self.assertRaises(AssistantError):
                await self.messenger.send(self.message, reply)
            self.assertFalse(reply.get('sent_parts'))


class AnswerRecoveryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        path = Path(temp.name)
        self.bot = FeishuAssistant(SimpleNamespace(), SimpleNamespace(state_store=Store(path / 'state.db')), AsyncMock())
        self.bot.inbox = Inbox(path / 'inbox.db')
        self.bot.messenger = SimpleNamespace(react=AsyncMock(), send=AsyncMock())
        self.actor = {'id': 'ou_Actor', 'scopes': ['D'], 'modules': ['assistant', 'learning'], 'is_admin': False}
        self.request = SimpleNamespace(state=SimpleNamespace())
        self.bot._request_context = AsyncMock(return_value=(self.request, self.actor))
        self.bot._validate_delivery = AsyncMock()
        self.bot._sync_reply_history = AsyncMock()
        self.bot._prepare_reply_files = AsyncMock()
        self.message = parse_message(event(content='上周哪些人员参与答题'), 'cliTest', 'ouBot')
        self.bot.inbox.add(self.message)
        self.operation = 'feishu_' + hashlib.sha256(self.message['message_id'].encode()).hexdigest()[:40]
        self.conversation = {'conversation_id': 'original', 'turns': []}
        self.status, self.posts = 'pending', 0
        async def call(request, actor, path, body=None, **kwargs):
            if path == 'chat':
                self.posts += 1
                raise TimeoutError('HTTP response lost, original task continues')
            if kwargs.get('params', {}).get('operation_id'):
                self.assertEqual(kwargs['params']['operation_id'], self.operation)
                return {**self.conversation, 'turns': [{'operation_id': self.operation, 'status': self.status,
                    'answer': '已查全：人员甲、人员乙，共2人。' if self.status == 'completed' else '正在查询第一栋'}]}
            return self.conversation
        self.bot._call = AsyncMock(side_effect=call)
        self.files = patch('lan_bitable_template_portal.feishu_assistant_files.handle_files', AsyncMock(return_value=None))
        self.files.start()
        self.addCleanup(self.files.stop)

    async def test_lost_chat_response_then_final_answer_is_delivered_without_resubmitting(self):
        await self.bot._drain(self.message['union_id'])
        self.assertEqual(self.posts, 1)
        waiting = self.bot.inbox.reply_for(self.message['message_id'])
        self.assertEqual(waiting['awaiting_turn'], self.operation)
        self.assertNotIn('正在查询第一栋', self.bot.messenger.send.await_args.args[1]['text'])
        self.bot._sync_reply_history.assert_not_awaited()
        self.bot.inbox = Inbox(self.bot.inbox.path)
        self.status = 'completed'
        self.bot.inbox.set(self.message['message_id'], 'reply', delay=0)
        await self.bot._drain(self.message['union_id'])
        self.assertEqual(self.posts, 1)
        self.assertIn('人员甲、人员乙,共2人', self.bot.messenger.send.await_args.args[1]['text'])
        self.bot._sync_reply_history.assert_awaited_once()
        self.assertIsNone(self.bot.inbox.reply_for(self.message['message_id']))
        self.assertFalse(self.bot.inbox.add(self.message))

    async def test_cleared_conversation_does_not_read_or_send_old_final_answer(self):
        waiting = await self.bot._answer_message(self.message)
        self.conversation['conversation_id'] = 'new-conversation'
        with self.assertRaisesRegex(AssistantError, '已清空'):
            await self.bot._recover_answer(self.message, waiting)
        self.assertEqual(self.posts, 1)
        self.bot.messenger.send.assert_not_awaited()

    async def test_restarted_processing_keeps_original_operation_and_waiting_parts(self):
        waiting = await self.bot._answer_message(self.message)
        waiting['sent_parts'] = ['waiting:0']
        self.bot.inbox.set(self.message['message_id'], 'processing', waiting)
        self.bot.inbox = Inbox(self.bot.inbox.path)
        self.assertEqual(self.bot.inbox.reply_for(self.message['message_id']), waiting)
        self.status = 'completed'
        await self.bot._drain(self.message['union_id'])
        self.assertEqual(self.posts, 1)
        self.assertNotIn('sent_parts', self.bot.messenger.send.await_args.args[1])

    async def test_failed_original_turn_does_not_send_its_partial_answer(self):
        waiting = await self.bot._answer_message(self.message)
        self.status = 'failed'
        reply = await self.bot._recover_answer(self.message, waiting)
        self.assertNotIn('正在查询第一栋', reply['text'])
        self.assertIn('未完成', reply['text'])

    async def test_legacy_live_turn_never_mirrors_prefix_and_failure_discards_old_card_draft(self):
        from lan_bitable_template_portal.feishu_assistant import _live_card
        waiting = await self.bot._answer_message(self.message)
        original = self.bot._call.side_effect
        async def legacy(*args, **kwargs):
            result = await original(*args, **kwargs)
            if isinstance(result, dict) and result.get('turns'):
                result['turns'][0]['reply_mode'] = 'live'
            return result
        self.bot._call.side_effect = legacy
        waiting['live_reply'] = {'partial': '今天', 'notice': 'old incomplete notice', 'message_id': 'om_Existing'}
        card = _live_card(waiting)
        self.assertNotIn('今天', json.dumps(card, ensure_ascii=False))
        pending = await self.bot._recover_answer(self.message, waiting)
        self.assertNotIn('正在查询第一栋', pending['text'])
        self.assertNotIn('partial', pending['live_reply'])
        self.status = 'failed'
        failed = await self.bot._recover_answer(self.message, pending)
        self.assertNotIn('今天', failed['text'])
        self.assertNotIn('正在查询第一栋', failed['text'])
        self.assertEqual(failed['live_reply']['message_id'], 'om_Existing')
        self.assertFalse(failed['live_reply']['notice'])
        self.assertEqual(self.posts, 1)

    async def test_failed_turn_has_a_safe_run_reference_but_no_rejected_candidate(self):
        reply = await self.bot._turn_reply(self.message, self.request, self.actor, self.conversation,
            {'status': 'failed', 'error': 'Internal details must not be shown', 'error_code': 'model_validation',
             'run_id': 'abc12345' * 4, 'answer': 'Unverified candidate must not be shown'}, self.message['question'])
        self.assertIn('未通过资料核验', reply['text'])
        self.assertIn('诊断编号:abc12345', reply['text'])
        self.assertNotIn('Internal details', reply['text'])
        self.assertNotIn('Unverified candidate', reply['text'])
        reply = await self.bot._turn_reply(self.message, self.request, self.actor, self.conversation,
            {'status': 'failed', 'run_id': 'untrusted reference', 'error_code': 'model_validation'}, self.message['question'])
        self.assertNotIn('诊断编号', reply['text'])

    async def test_temporarily_unreachable_backend_keeps_pending_outcome_for_later(self):
        waiting = await self.bot._answer_message(self.message)
        self.bot._call.side_effect = TimeoutError()
        for _ in range(10):
            self.bot.inbox.set(self.message['message_id'], 'reply', waiting)
            await self.bot._drain(self.message['union_id'])
        self.assertEqual(self.bot.inbox.reply_for(self.message['message_id'])['awaiting_turn'], self.operation)
        self.bot.messenger.send.assert_not_awaited()

    async def test_lost_post_response_preserves_expiring_context_until_original_run_finishes(self):
        from unittest.mock import Mock
        authority = SimpleNamespace(context=Mock(return_value='trusted'), contexts={'trusted': {'expires': 1800}})
        client = SimpleNamespace(request=AsyncMock(side_effect=TimeoutError()))
        current = SimpleNamespace(key='fixture', instance='fixture', lease='fixture', descriptor={'port': 1},
                                  http_client=AsyncMock(return_value=client))
        self.bot.ready = AsyncMock(return_value=(current, authority))
        with self.assertRaises(TimeoutError):
            await self.bot._call_once(self.request, self.actor, 'chat', {'operation_id': self.operation})
        self.assertIn('trusted', authority.contexts)
        client.request.side_effect = None
        client.request.return_value = SimpleNamespace(status_code=200, json=lambda: {'ok': True, 'data': {'turns': [{'status': 'completed'}]}})
        await self.bot._call_once(self.request, self.actor, 'chat', {'operation_id': self.operation})
        self.assertNotIn('trusted', authority.contexts)


if __name__ == '__main__':
    unittest.main()
