"""Offline live Feishu replies: one receipt, checked snapshots, isolated queues."""
import asyncio
import copy
import hashlib
import json
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from bin.test_feishu_assistant import event
from bin.test_lighthouse_stream import Store
from lan_bitable_template_portal.feishu_assistant import (
    AssistantError, FeishuAssistant, FeishuMessenger, Inbox, parse_message, _live_result,
)


class LiveReplyTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        self.bot = FeishuAssistant(SimpleNamespace(), SimpleNamespace(state_store=Store(root / 'state.db')), AsyncMock())
        self.bot.inbox = Inbox(root / 'inbox.db')
        self.bot._react = AsyncMock()
        self.bot._sync_reply_history = AsyncMock()
        self.bot._prepare_reply_files = AsyncMock()
        self.bot._validate_delivery = AsyncMock()
        self.actor = {'id': 'ou_Actual', 'scopes': ['D'], 'modules': ['assistant'], 'is_admin': False}
        self.request = SimpleNamespace(state=SimpleNamespace())
        self.bot._request_context = AsyncMock(return_value=(self.request, self.actor))
        self.bot.messenger = FeishuMessenger({})
        self.bot.messenger.headers = AsyncMock(return_value={})
        self.posts, self.edits = [], []
        self.receipt = {'message_id': 'om_Live', 'chat_id': 'oc_Chat'}

        async def post(url, **kwargs):
            self.posts.append((url, copy.deepcopy(kwargs['json'])))
            return SimpleNamespace(status_code=200, json=lambda: {'code': 0, 'data': self.receipt})

        async def edit(url, **kwargs):
            self.edits.append((url, copy.deepcopy(kwargs['json'])))
            return SimpleNamespace(status_code=200, json=lambda: {'code': 0})

        self.client = SimpleNamespace(post=AsyncMock(side_effect=post), patch=AsyncMock(side_effect=edit))
        self.bot.messenger.client = AsyncMock(return_value=self.client)
        for target, value in (
            ('lan_bitable_template_portal.portal_service.external_real_write_guard', {'real_write_allowed': True}),
            ('lan_bitable_template_portal.feishu_assistant_files.handle_files', None),
        ):
            mocked = patch(target, AsyncMock(return_value=value) if value is None else lambda value=value: value)
            mocked.start()
            self.addCleanup(mocked.stop)

    def message(self, identity='First', *, group=False, root=None, chat='oc_Chat', app='cliTest'):
        raw = event('SamePerson', chat_type='group' if group else 'p2p', message_id=identity, content='解释一下这个概念')
        raw['header']['app_id'] = app
        raw['event']['message']['chat_id'] = chat
        if root:
            raw['event']['message']['root_id'] = root
        message = parse_message(raw, app, 'ouBot', inbox=self.bot.inbox)
        self.assertIsNotNone(message)
        self.bot.inbox.add(message)
        return message

    def waiting(self, message):
        return {'awaiting_turn': 'feishu_' + hashlib.sha256(message['message_id'].encode()).hexdigest()[:40],
                'actor_id': self.actor['id'], 'conversation_id': 'original', 'wait_started': time.time(),
                'query_question': message['question'], 'delivery_access': self.bot._delivery_access(self.actor),
                'text': '原回答仍在处理中。'}

    def backend(self, message, *, status='running', mode='live', answer='第一部分', **extra):
        waiting = self.waiting(message)
        turn = {'operation_id': waiting['awaiting_turn'], 'status': status, 'reply_mode': mode,
                'answer': answer, **extra}
        conversation = {'conversation_id': 'original', 'turns': [turn]}

        async def call(request, actor, path, body=None, **kwargs):
            self.assertEqual(path, 'conversation')
            if kwargs.get('params'):
                self.assertEqual(kwargs['params'], {'operation_id': waiting['awaiting_turn']})
            return copy.deepcopy(conversation)

        self.bot._call = AsyncMock(side_effect=call)
        return waiting, conversation, turn

    def enable_files(self, name='完整回答.md'):
        original = self.bot._call.side_effect

        async def call(request, actor, path, body=None, **kwargs):
            if path.startswith('files/'):
                return {'id': path.split('/')[1], 'name': name}
            return await original(request, actor, path, body, **kwargs)

        self.bot._call.side_effect = call
        self.bot._reply_file_context = AsyncMock(return_value=(self.request, self.actor))
        self.bot._prepare_reply_files = (
            FeishuAssistant._prepare_reply_files.__get__(self.bot, FeishuAssistant))
        self.bot.messenger.upload_file = AsyncMock(return_value='file_FullAnswer')

    async def deliver(self, message, reply):
        self.bot.inbox.set(message['message_id'], 'reply', reply)
        await self.bot._drain(message['union_id'])
        return self.bot.inbox.reply_for(message['message_id'])

    async def test_private_placeholder_hides_snapshots_then_final_edits_one_receipt(self):
        message = self.message()
        waiting, conversation, turn = self.backend(message)
        waiting = await self.deliver(message, waiting)
        self.assertEqual(len(self.posts), 1)
        self.assertEqual(waiting['live_reply']['message_id'], 'om_Live')
        self.assertNotIn('thread_id', self.receipt)
        self.assertEqual(self.edits, [])
        turn['answer'] = '第一部分，第二部分'
        waiting['live_reply']['updated_at'] = 0
        waiting = await self.deliver(message, waiting)
        self.assertEqual(self.edits, [])
        self.assertNotIn('第一部分', str(self.posts))
        turn.update(status='completed', answer='这是经过最终校验的完整回答。')
        self.assertIsNone(await self.deliver(message, waiting))
        self.assertEqual(len(self.posts), 1)
        self.assertEqual(len(self.edits), 1)
        self.assertIn('完整回答', self.edits[-1][1]['content'])
        self.assertNotIn('未完成', self.edits[-1][1]['content'])
        self.assertTrue(all(url.endswith('/om_Live') for url, _ in self.edits))
        self.bot._sync_reply_history.assert_awaited_once()
        self.assertFalse(self.bot.inbox.add(message))

    async def test_business_or_evidence_or_form_draft_is_never_published(self):
        for changes in ({'mode': 'final'}, {'plan': {'id': 'business'}}, {'sources': [{'kind': 'company_knowledge'}]},
                        {'output_files': [{'id': 'artifact'}]}, {'clarification': {'question': 'choose'}}):
            message = self.message('Guard' + str(len(self.posts)))
            waiting, _, _ = self.backend(message, answer='UNREVIEWED-BUSINESS', **changes)
            saved = await self.deliver(message, waiting)
            self.assertNotIn('live_reply', saved)
            self.assertNotIn('UNREVIEWED-BUSINESS', str(self.posts))
            self.assertEqual(self.edits, [])

    async def test_throttle_coalesces_and_final_does_not_wait_for_next_interval(self):
        message = self.message()
        waiting, _, turn = self.backend(message)
        waiting = await self.deliver(message, waiting)
        turn['answer'] = 'NEW-SNAPSHOT'
        waiting = await self.deliver(message, waiting)
        self.assertEqual(self.edits, [])
        turn.update(status='completed', answer='FINAL-SNAPSHOT')
        await self.deliver(message, waiting)
        self.assertEqual(len(self.posts), 1)
        self.assertEqual(len(self.edits), 1)
        self.assertIn('FINAL-SNAPSHOT', self.edits[0][1]['content'])

    async def test_final_patch_lost_response_retries_same_message_after_restart(self):
        message = self.message()
        waiting, _, turn = self.backend(message)
        waiting = await self.deliver(message, waiting)
        turn.update(status='completed', answer='VALIDATED-FINAL')
        self.client.patch.side_effect = TimeoutError()
        final = await self.deliver(message, waiting)
        self.assertNotIn('awaiting_turn', final)
        self.assertEqual(final['live_reply']['message_id'], 'om_Live')
        self.bot.inbox = Inbox(self.bot.inbox.path)
        self.client.patch.side_effect = None
        self.client.patch.return_value = SimpleNamespace(status_code=200, json=lambda: {'code': 0})
        self.bot._call.reset_mock()
        await self.deliver(message, final)
        self.assertEqual(len(self.posts), 1)
        attempts = self.client.patch.await_args_list
        self.assertEqual(attempts[0], attempts[1])
        self.assertFalse(any(call.args[2] == 'chat' for call in self.bot._call.await_args_list))

    async def test_history_failure_after_final_edit_retries_history_without_edit_or_new_message(self):
        message = self.message()
        waiting, _, turn = self.backend(message)
        waiting = await self.deliver(message, waiting)
        turn.update(status='completed', answer='VALIDATED-FINAL')
        self.bot._sync_reply_history.side_effect = TimeoutError()
        final = await self.deliver(message, waiting)
        self.bot.inbox = Inbox(self.bot.inbox.path)
        self.bot._sync_reply_history.side_effect = None
        await self.deliver(message, final)
        self.assertEqual(len(self.posts), 1)
        self.assertEqual(len(self.edits), 1)
        self.assertEqual(self.bot._sync_reply_history.await_count, 2)

    async def test_rejected_update_stops_without_sending_replacement_or_claiming_final_delivery(self):
        message = self.message()
        waiting, _, turn = self.backend(message)
        waiting = await self.deliver(message, waiting)
        turn.update(status='completed', answer='VALIDATED-FINAL')
        self.client.patch.side_effect = None
        self.client.patch.return_value = SimpleNamespace(status_code=400, json=lambda: {'code': 230013})
        await self.deliver(message, waiting)
        with self.bot.inbox.connect() as db:
            state = db.execute('SELECT state FROM messages WHERE id=?', (message['message_id'],)).fetchone()[0]
        self.assertEqual(state, 'stopped')
        self.assertEqual(len(self.posts), 1)
        self.bot._sync_reply_history.assert_not_awaited()

    async def test_wrong_topic_or_missing_receipt_cannot_become_live_update_target(self):
        for index, receipt in enumerate(({}, {'message_id': 'om_Wrong', 'chat_id': 'oc_Other'},
                                         {'message_id': 'om_Wrong', 'root_id': 'om_ForeignTopic'})):
            message = self.message('Receipt' + str(index), group=True)
            self.receipt = receipt
            waiting, _, _ = self.backend(message)
            saved = await self.deliver(message, waiting)
            self.assertFalse(saved['live_reply'].get('message_id'))
            self.assertEqual(self.edits, [])

    async def test_expired_uncertain_placeholder_does_not_create_a_new_message(self):
        message = self.message()
        waiting, _, _ = self.backend(message)
        waiting['live_reply'] = {'create_started': time.time() - 3601}
        await self.deliver(message, waiting)
        self.assertEqual(self.posts, [])
        self.assertEqual(self.edits, [])

    async def test_placeholder_lost_receipt_reuses_uuid_and_fixed_content(self):
        message = self.message()
        waiting, _, turn = self.backend(message)
        original = self.client.post.side_effect
        attempts = []

        async def post(url, **kwargs):
            attempts.append(copy.deepcopy(kwargs['json']))
            if len(attempts) == 1:
                raise TimeoutError()
            return await original(url, **kwargs)

        self.client.post.side_effect = post
        waiting = await self.deliver(message, waiting)
        self.assertNotIn('message_id', waiting['live_reply'])
        turn['answer'] = 'DIFFERENT-SNAPSHOT'
        await self.deliver(message, waiting)
        self.assertEqual(attempts[0], attempts[1])
        self.assertEqual(len(self.posts), 1)

    async def test_failed_or_stopped_answer_discards_partial_and_never_claims_success(self):
        for index, status in enumerate(('failed', 'stopped', 'cancelled', 'superseded')):
            message = self.message('Failure' + str(index))
            waiting, _, turn = self.backend(message, answer='PARTIAL-EXPLANATION')
            waiting = await self.deliver(message, waiting)
            turn.update(status=status, answer='操作已完成。', error='本轮已停止。')
            await self.deliver(message, waiting)
            content = self.edits[-1][1]['content']
            self.assertNotIn('PARTIAL-EXPLANATION', content)
            self.assertIn('已停止', content)
            self.assertNotIn('操作已完成', content)
        self.assertEqual(len(self.posts), 4)

    async def test_revocation_replaces_same_card_with_notice_without_partial(self):
        message = self.message()
        waiting, _, _ = self.backend(message, answer='PREVIOUS-PARTIAL')
        waiting = await self.deliver(message, waiting)
        async def validate(message, reply):
            if reply.get('actor_id'):
                raise AssistantError('revoked', 403, category='delivery_revoked')
        self.bot._validate_delivery.side_effect = validate
        stopped = await self.deliver(message, waiting)
        self.assertIsNone(stopped)
        self.assertEqual(len(self.posts), 1)
        self.assertTrue(self.edits[-1][0].endswith('/om_Live'))
        self.assertIn('停止发送', self.edits[-1][1]['content'])
        self.assertNotIn('PREVIOUS-PARTIAL', self.edits[-1][1]['content'])

    async def test_topic_receipt_and_only_final_public_context_survive_restart(self):
        message = self.message(group=True)
        self.receipt.update(root_id=message['message_id'], thread_id='omt_Topic')
        waiting, _, turn = self.backend(message, answer='PARTIAL-TOPIC')
        waiting = await self.deliver(message, waiting)
        self.assertEqual(self.bot.inbox.topic_context(message, self.actor), '')
        self.bot.inbox = Inbox(self.bot.inbox.path)
        turn.update(status='completed', answer='FINAL-TOPIC')
        await self.deliver(message, waiting)
        self.assertIn('FINAL-TOPIC', self.bot.inbox.topic_context(message, self.actor))
        self.assertNotIn('PARTIAL-TOPIC', self.bot.inbox.topic_context(message, self.actor))
        self.assertEqual(len(self.posts), 1)

    async def test_stopped_long_partial_is_not_delivered(self):
        message = self.message()
        waiting, _, turn = self.backend(message, answer='部分内容' * 2000)
        waiting = await self.deliver(message, waiting)
        turn.update(status='stopped', error='本轮已停止。')
        await self.deliver(message, waiting)
        content = json.loads(self.edits[-1][1]['content'])
        displayed = ''.join(element['content'] for element in content['body']['elements'])
        self.assertNotIn('部分内容', displayed)
        self.assertNotIn('仅展示开头', displayed)
        self.assertIn('已停止', displayed)
        self.assertLess(len(json.dumps(content, ensure_ascii=False).encode()), 30000)

    async def test_3501_and_5000_character_final_replies_are_complete_in_same_card(self):
        for length in (3501, 5000):
            message = self.message('Length' + str(length))
            waiting, _, turn = self.backend(message)
            waiting = await self.deliver(message, waiting)
            answer = '答' * (length - 4) + '最终结尾'
            turn.update(status='completed', answer=answer)
            await self.deliver(message, waiting)
            card = json.loads(self.edits[-1][1]['content'])
            self.assertEqual(''.join(element['content'] for element in card['body']['elements']), answer)
            self.assertNotIn('仅展示开头', str(card))
            self.assertLess(len(json.dumps(card, ensure_ascii=False).encode()), 30000)
        self.assertEqual(len(self.posts), 2)

    def test_failed_result_already_containing_partial_does_not_duplicate_answer(self):
        prior = {'live_reply': {'partial': 'ALREADY-SHOWN-PARTIAL'}}
        result = _live_result(prior, {'text': 'ALREADY-SHOWN-PARTIAL\n\n本轮未完成。'}, incomplete=True)
        self.assertEqual(result['text'].count('ALREADY-SHOWN-PARTIAL'), 1)

    def test_failed_result_without_partial_does_not_claim_generated_content(self):
        from lan_bitable_template_portal.feishu_assistant import _live_card
        result = _live_result({'live_reply': {'partial': ''}}, {'text': '排班视图未完整读取，人数未知。'}, incomplete=True)
        content = _live_card(result)['body']['elements']
        self.assertEqual(len(content), 1)
        self.assertEqual(content[0]['content'], '排班视图未完整读取,人数未知。')

    async def test_oversized_final_creates_complete_file_and_attaches_it_in_original_chat(self):
        message = self.message()
        waiting, _, turn = self.backend(message)
        waiting = await self.deliver(message, waiting)
        answer = '完整内容\n' * 5000 + 'FINAL-TAIL'
        turn.update(status='completed', answer=answer)
        self.enable_files()
        upload = AsyncMock(return_value='b' * 32)
        download = AsyncMock(return_value=answer.encode())
        with patch('lan_bitable_template_portal.feishu_assistant_files._upload', upload), \
             patch('lan_bitable_template_portal.feishu_assistant_files._assistant_download', download):
            self.assertIsNone(await self.deliver(message, waiting))
        self.assertEqual(upload.await_args.args[-1].decode(), answer)
        self.assertEqual(self.bot.messenger.upload_file.await_args.args[-1].decode(), answer)
        self.assertEqual(len(self.posts), 2)
        self.assertEqual(self.posts[-1][1]['msg_type'], 'file')
        self.assertIn('完整回答.md', self.edits[-1][1]['content'])
        self.assertNotIn('仅展示开头', self.edits[-1][1]['content'])
        self.assertTrue(all('/' + message['message_id'] + '/reply' in url for url, _ in self.posts))

    async def test_packaged_full_file_retry_preserves_file_and_does_not_finalize_before_receipt(self):
        message = self.message()
        waiting, _, turn = self.backend(message)
        waiting = await self.deliver(message, waiting)
        fid, answer = 'a' * 32, '完整内容\n' * 5000
        turn.update(status='completed', answer='完整内容已保存为「完整回答.md」，请查看本会话附件。',
                    output_files=[{'id': fid, 'name': '完整回答.md'}])
        self.enable_files()
        original, attempts = self.client.post.side_effect, []

        async def post(url, **kwargs):
            attempts.append(copy.deepcopy(kwargs['json']))
            if len(attempts) == 1:
                raise TimeoutError()
            return await original(url, **kwargs)

        self.client.post.side_effect = post
        with patch('lan_bitable_template_portal.feishu_assistant_files._assistant_download',
                   AsyncMock(return_value=answer.encode())), \
             patch('lan_bitable_template_portal.feishu_assistant_files._upload', AsyncMock()) as upload:
            final = await self.deliver(message, waiting)
            self.assertEqual(self.edits, [])
            self.assertNotIn('awaiting_turn', final)
            self.assertEqual(final['files'][0]['id'], fid)
            self.bot.inbox = Inbox(self.bot.inbox.path)
            await self.deliver(message, final)
        self.assertEqual(attempts[0], attempts[1])
        self.assertEqual(len(self.posts), 2)
        self.assertEqual(len(self.edits), 1)
        self.assertEqual(self.bot.messenger.upload_file.await_args.args[-1].decode(), answer)
        upload.assert_not_awaited()

    async def test_full_file_upload_permission_loss_stops_without_marking_answer_complete(self):
        message = self.message()
        waiting, _, turn = self.backend(message)
        waiting = await self.deliver(message, waiting)
        turn.update(status='completed', answer='完整回答见附件。',
                    output_files=[{'id': 'a' * 32, 'name': '完整回答.md'}])
        self.enable_files()
        self.bot.messenger.upload_file.side_effect = AssistantError('文件权限已撤销，尚未发送。', 403,
                                                                   category='feishu_file_upload_permission')
        with patch('lan_bitable_template_portal.feishu_assistant_files._assistant_download',
                   AsyncMock(return_value=b'full answer')):
            saved = await self.deliver(message, waiting)
        self.assertTrue(saved['files'])
        with self.bot.inbox.connect() as db:
            self.assertEqual(db.execute('SELECT state FROM messages WHERE id=?',
                                        (message['message_id'],)).fetchone()[0], 'stopped')
        self.assertEqual(len(self.posts), 1)
        self.assertEqual(self.edits, [])
        self.bot._sync_reply_history.assert_not_awaited()

    async def test_live_poll_reads_original_operation_while_chat_is_running(self):
        message = self.message()
        operation = self.waiting(message)['awaiting_turn']
        release = asyncio.Event()
        reads = 0
        turn = {'operation_id': operation, 'reply_mode': 'live', 'status': 'running', 'answer': 'LIVE-BEFORE-RETURN'}

        async def call(request, actor, path, body=None, **kwargs):
            nonlocal reads
            if path == 'chat':
                await release.wait()
                turn.update(status='completed', answer='FINAL-AFTER-POLL')
            elif kwargs.get('params'):
                self.assertEqual(kwargs['params']['operation_id'], operation)
                reads += 1
                if reads >= 3:
                    release.set()
            return {'conversation_id': 'original', 'active_run_id': 'FOREIGN-LANE-RUN',
                    'turns': [copy.deepcopy(turn)]}

        self.bot._call = AsyncMock(side_effect=call)
        with patch('lan_bitable_template_portal.feishu_assistant._LIVE_REPLY_INTERVAL', .01):
            reply = await asyncio.wait_for(self.bot.answer(message), 3)
        self.assertEqual(len(self.posts), 1)
        self.assertFalse(any('LIVE-BEFORE-RETURN' in body['content'] for _, body in self.posts + self.edits))
        self.assertIn('FINAL-AFTER-POLL', reply['text'])
        self.assertEqual(sum(call.args[2] == 'chat' for call in self.bot._call.await_args_list), 1)

    async def test_cancelling_poll_keeps_receipt_and_recovery_never_replays_chat(self):
        message = self.message()
        waiting = self.waiting(message)
        never = asyncio.Event()
        status = 'running'

        async def call(request, actor, path, body=None, **kwargs):
            if path == 'chat':
                await never.wait()
            turns = [{'operation_id': waiting['awaiting_turn'], 'status': status, 'reply_mode': 'live',
                      'answer': 'SAVED-PARTIAL', 'error': '本轮已停止。'}] if kwargs.get('params') else []
            return {'conversation_id': 'original', 'turns': turns}

        self.bot._call = AsyncMock(side_effect=call)
        with patch('lan_bitable_template_portal.feishu_assistant._LIVE_REPLY_INTERVAL', .01):
            task = asyncio.create_task(self.bot._answer_message(message))
            try:
                async with asyncio.timeout(3):
                    while not (self.bot.inbox.reply_for(message['message_id']) or {}).get('live_reply', {}).get('message_id'):
                        await asyncio.sleep(.01)
            finally:
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
        self.bot.inbox = Inbox(self.bot.inbox.path)
        saved = self.bot.inbox.reply_for(message['message_id'])
        self.assertEqual(saved['live_reply']['message_id'], 'om_Live')
        status = 'stopped'
        await self.deliver(message, saved)
        self.assertEqual(len(self.posts), 1)
        self.assertEqual(sum(call.args[2] == 'chat' for call in self.bot._call.await_args_list), 1)
        self.assertIn('已停止', self.edits[-1][1]['content'])

    async def test_same_person_other_topics_and_private_chats_run_concurrently(self):
        first = self.message('TopicOne', group=True)
        second = self.message('TopicTwo', group=True)
        private = self.message('PrivateOne')
        other_private = self.message('PrivateTwo', chat='oc_Other')
        entered, release = asyncio.Event(), asyncio.Event()
        running = set()

        async def answer(message):
            running.add(message['message_id'])
            if len(running) == 4:
                entered.set()
            await release.wait()
            return {'text': 'done'}

        self.bot.answer = AsyncMock(side_effect=answer)
        self.bot.messenger = SimpleNamespace(send=AsyncMock())
        queues = self.bot.inbox.queues()
        self.assertEqual(len(queues), 4)
        self.assertEqual(len({owner for owner, _ in queues}), 1)
        runner = asyncio.create_task(self.bot._run())
        try:
            await asyncio.wait_for(entered.wait(), 3)
        finally:
            release.set()
            try:
                await asyncio.gather(*self.bot.tasks.values())
            finally:
                self.bot.closing = True
                self.bot.event.set()
                await asyncio.wait_for(runner, 3)
        self.assertEqual(running, {message['message_id'] for message in (first, second, private, other_private)})
        self.assertFalse(self.bot.topic_locks)

    def test_cancel_recovery_only_changes_its_own_lane(self):
        first, second = self.message(), self.message('Second', chat='oc_Other')
        owner = first['union_id']
        one, two = self.bot.inbox.queue_key(first), self.bot.inbox.queue_key(second)
        self.bot.inbox.take(owner, one)
        self.bot.inbox.take(owner, two)
        self.bot.inbox.recover_processing(owner, one)
        with self.bot.inbox.connect() as db:
            states = dict(db.execute('SELECT id,state FROM messages'))
        self.assertEqual(states[first['message_id']], 'reply')
        self.assertEqual(states[second['message_id']], 'processing')


if __name__ == '__main__':
    unittest.main()
