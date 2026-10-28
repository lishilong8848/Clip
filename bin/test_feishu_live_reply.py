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
        allowed = {'等待处理', '正在准备回答', '正在查询资料', '正在核对结果', '正在整理答复', '正在处理你的问题'}
        for changes in ({'mode': 'final'}, {'plan': {'id': 'business'}}, {'sources': [{'kind': 'company_knowledge'}]},
                        {'output_files': [{'id': 'artifact'}]}, {'clarification': {'question': 'choose'}}):
            seen = len(self.posts)
            message = self.message('Guard' + str(len(self.posts)))
            waiting, _, _ = self.backend(message, answer='UNREVIEWED-BUSINESS', **changes)
            saved = await self.deliver(message, waiting)
            # A pending business turn may show one safe progress card, but never the draft.
            self.assertEqual(saved['live_reply']['message_id'], 'om_Live')
            self.assertEqual(self.edits, [])
            published = [self.card_text(body) for _, body in self.posts[seen:] + self.edits]
            self.assertEqual(len(published), 1)
            self.assertIn(published[0], allowed)
            for text in published:
                for raw in ('UNREVIEWED-BUSINESS', 'business', 'choose', 'company_knowledge', 'artifact'):
                    self.assertNotIn(raw, text)

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
        # A running turn with no process label yet: inline loop shows the generic
        # phase, and a later identical phase must not re-edit the same card.
        self.assertEqual([self.card_text(body) for _, body in self.posts], ['正在处理你的问题'])
        self.assertEqual(self.edits, [])
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

    def card_text(self, body):
        return ''.join(element['content'] for element in json.loads(body['content'])['body']['elements'])

    async def test_phase_change_edits_same_message_and_same_phase_does_not_edit(self):
        message = self.message()
        waiting, _, turn = self.backend(message, mode='validated', plan={'id': 'business'})
        turn['process'] = [{'label': '等待处理'}]
        waiting = await self.deliver(message, waiting)
        self.assertEqual([self.card_text(body) for _, body in self.posts], ['等待处理'])
        turn['process'].append({'label': '正在查询资料'})
        waiting = await self.deliver(message, waiting)
        self.assertEqual(self.edits, [], 'stage changes must retain the existing throttle')
        waiting['live_reply']['updated_at'] = 0
        waiting = await self.deliver(message, waiting)
        self.assertEqual(len(self.edits), 1)
        self.assertEqual(self.card_text(self.edits[-1][1]), '正在查询资料')
        # Same safe phase again (even after the interval elapsed) keeps the card untouched.
        waiting['live_reply']['updated_at'] = 0
        waiting = await self.deliver(message, waiting)
        self.assertEqual(len(self.edits), 1)
        turn['process'].append({'label': '正在核对结果'})
        waiting['live_reply']['updated_at'] = 0
        await self.deliver(message, waiting)
        self.assertEqual([self.card_text(body) for _, body in self.edits], ['正在查询资料', '正在核对结果'])
        self.assertEqual(len(self.posts), 1)
        self.assertTrue(all(url.endswith('/om_Live') for url, _ in self.edits))

    async def test_phase_follows_last_label_even_when_it_moves_back_to_query(self):
        # An early thinking label must not mask later query/review phases.
        message = self.message()
        waiting, _, turn = self.backend(message, mode='validated', plan={'id': 'business'})
        waiting = await self.deliver(message, waiting)
        # A turn with no process entry has no known phase yet, never a stale one.
        self.assertEqual(self.card_text(self.posts[0][1]), '正在处理你的问题')
        expected = []
        for label, phrase in (('正在整理答复', '正在整理答复'), ('正在查询资料', '正在查询资料'),
                              ('正在核对结果', '正在核对结果'), ('正在查询资料', '正在查询资料')):
            turn['process'] = turn.get('process', []) + [{'label': label}]
            waiting['live_reply']['updated_at'] = 0
            waiting = await self.deliver(message, waiting)
            expected.append(phrase)
        self.assertEqual([self.card_text(body) for _, body in self.edits], expected)
        self.assertEqual(len(self.posts), 1)
        turn.update(status='completed', answer='FINAL-AFTER-BACKTRACK', plan=None)
        self.assertIsNone(await self.deliver(message, waiting))
        self.assertEqual(len(self.posts), 1)
        self.assertEqual(self.card_text(self.edits[-1][1]), 'FINAL-AFTER-BACKTRACK')
        self.assertNotIn('live_phase', self.bot.inbox.reply_for(message['message_id']) or {})

    def test_public_phase_uses_latest_label_and_never_leaks_raw_text(self):
        from lan_bitable_template_portal.feishu_assistant import _public_phase
        self.assertEqual(_public_phase({'process': [{'label': '正在整理答复'}, {'label': '正在查询资料'}]}), 'query')
        self.assertEqual(_public_phase({'process': [{'label': '正在查询资料'}, {'label': '正在核对结果'}]}), 'checking')
        self.assertEqual(_public_phase({'process': [{'label': '正在核对结果'}, {'label': '正在查询资料'}]}), 'query')
        # Real backend labels are classified by known prefix family only.
        for label, phase in (('正在思考回答', 'thinking'), ('正在核对答复是否完整', 'checking'),
                             ('正在核对答复与资料', 'checking'), ('正在补查缺失资料', 'query'),
                             ('正在补查缺失或不适用的资料', 'query'), ('正在补查缺失资料 3/5', 'query'),
                             ('正在查询 本月计划通告', 'query'), ('正在读取飞书文档', 'query'),
                             ('正在连接助手', 'connecting'), ('正在准备回答', 'connecting'),
                             ('当前模型暂不可用，正在切换可用模型', 'switching'), ('正在恢复原回答，不重复提交业务', 'connecting')):
            self.assertEqual(_public_phase({'process': [{'label': label}]}), phase)
        # The latest actual label wins; an unknown one yields generic, not a stale phase.
        self.assertEqual(_public_phase({'process': [{'label': '正在查询资料'}, {'label': '调用工具 read_rows'}]}), 'unknown')
        self.assertEqual(_public_phase({'process': [{'label': '正在思考回答'}, {'label': '正在核对答复与资料'}]}), 'checking')
        self.assertEqual(_public_phase({'process': [{'label': 'already thread prepar'}]}), 'unknown')
        self.assertEqual(_public_phase({'process': []}), 'unknown')
        self.assertEqual(_public_phase({}), 'unknown')
        self.assertEqual(_public_phase({'label': '正在整理答复'}), 'finishing')
        self.assertEqual(_public_phase({'label': '正在整理资料答复'}), 'finishing')

    async def test_raw_or_invalid_process_label_is_never_shown(self):
        message = self.message()
        waiting, _, turn = self.backend(message, mode='validated', answer='DRAFT-ONLY')
        turn['process'] = [{'label': '查询 ou_Actual 的所谓“机密”记录 RUN-123'}]
        waiting = await self.deliver(message, waiting)
        turn['process'].append({'label': '调用工具 read_rows 参数 {"b": 1}'})
        waiting['live_reply']['updated_at'] = 0
        waiting = await self.deliver(message, waiting)
        shown = [self.card_text(body) for _, body in self.posts] + [self.card_text(body) for _, body in self.edits]
        # Neither label belongs to a known family, so only the generic phrase is shown.
        self.assertEqual(set(shown), {'正在处理你的问题'})
        self.assertEqual(len(self.posts), 1)
        for text in shown:
            for raw in ('ou_Actual', '机密', 'RUN-123', 'read_rows', '参数', 'DRAFT-ONLY', message['question']):
                self.assertNotIn(raw, text)

    async def test_unknown_process_label_uses_generic_phase_once(self):
        from lan_bitable_template_portal.feishu_assistant import _LIVE_UNKNOWN_PHRASE
        message = self.message()
        waiting, _, turn = self.backend(message, mode='validated')
        waiting = await self.deliver(message, waiting)
        # Once the turn is readable, a wholly unrecognised label shows the generic
        # status text (same fixed phrase either way), never the raw label.
        turn['process'] = [{'label': '加载附件二进制'}]
        waiting['live_reply']['updated_at'] = 0
        waiting = await self.deliver(message, waiting)
        self.assertEqual([self.card_text(body) for _, body in self.posts], [_LIVE_UNKNOWN_PHRASE])
        self.assertEqual(self.edits, [])
        self.assertNotIn('加载附件二进制', str(self.posts + self.edits))
        # An unrelated label after an unknown one still never becomes visible.
        turn['process'].append({'label': 'unknown-label-xyz'})
        waiting['live_reply']['updated_at'] = 0
        await self.deliver(message, waiting)
        self.assertNotIn('unknown-label-xyz', str(self.posts + self.edits))
        self.assertEqual(len(self.posts), 1)

    async def test_final_validated_answer_replaces_progress_in_same_card(self):
        message = self.message()
        waiting, _, turn = self.backend(message, mode='validated', plan={'id': 'business'})
        turn['process'] = [{'label': '正在查询资料'}]
        waiting = await self.deliver(message, waiting)
        self.assertEqual(self.card_text(self.posts[0][1]), '正在查询资料')
        turn['process'].append({'label': '正在整理答复'})
        waiting['live_reply']['updated_at'] = 0
        waiting = await self.deliver(message, waiting)
        self.assertEqual(self.card_text(self.edits[-1][1]), '正在整理答复')
        turn.update(status='completed', answer='VALIDATED-BUSINESS-FINAL', plan=None)
        self.assertIsNone(await self.deliver(message, waiting))
        self.assertEqual(len(self.posts), 1)
        self.assertEqual(self.card_text(self.edits[-1][1]), 'VALIDATED-BUSINESS-FINAL')
        self.assertNotIn('正在整理', self.card_text(self.edits[-1][1]))
        self.assertTrue(all(url.endswith('/om_Live') for url, _ in self.edits))
        # The final output carries the receipt only, never progress state.
        self.assertNotIn('live_phase', self.bot.inbox.reply_for(message['message_id']) or {})
        self.bot._sync_reply_history.assert_awaited_once()

    async def test_business_turn_shows_safe_progress_before_chat_finishes(self):
        message = self.message()
        operation = self.waiting(message)['awaiting_turn']
        release, chat_finished = asyncio.Event(), asyncio.Event()
        process = [{'label': '等待处理'}]
        reads = 0

        async def call(request, actor, path, body=None, **kwargs):
            nonlocal reads
            if path == 'chat':
                await release.wait()
                # Safe progress cards exist before chat returns; drafts/plan never do.
                self.assertTrue(self.posts, 'the seeded card must exist while chat is still running')
                for text in [self.card_text(body) for _, body in self.posts + self.edits]:
                    self.assertNotIn('UNREVIEWED-BUSINESS-DRAFT', text)
                    self.assertNotIn('PLAN-BODY', text)
                self.assertTrue({self.card_text(body) for _, body in self.posts + self.edits}
                                <= {'等待处理', '正在查询资料', '正在整理答复'})
                chat_finished.set()
                return {'conversation_id': 'original', 'turns': [{
                    'operation_id': operation, 'status': 'completed', 'reply_mode': 'validated',
                    'answer': 'BUSINESS-FINAL-ANSWER', 'process': process}]}
            reads += 1
            if reads == 1:
                process.append({'label': '正在查询资料'})
            elif reads >= 3:
                release.set()
            return {'conversation_id': 'original', 'turns': [{
                'operation_id': operation, 'status': 'running', 'reply_mode': 'validated',
                'answer': 'UNREVIEWED-BUSINESS-DRAFT', 'process': copy.deepcopy(process),
                'plan': {'id': 'business', 'status': 'awaiting_confirmation', 'operations': [{'secret': 'PLAN-BODY'}]}}]}

        self.bot._call = AsyncMock(side_effect=call)
        with patch('lan_bitable_template_portal.feishu_assistant._LIVE_REPLY_INTERVAL', .01):
            reply = await asyncio.wait_for(self.bot._answer_message(message), 5)
        self.assertTrue(chat_finished.is_set())
        self.assertIn('BUSINESS-FINAL-ANSWER', reply['text'])
        self.assertEqual(len(self.posts), 1)
        # The inline loop already saw the query phase, so the card opens there.
        self.assertEqual(self.card_text(self.posts[0][1]), '正在查询资料')
        # The inline loop edits the one card for each phase change it observes.
        self.assertTrue(all(url.endswith('/om_Live') for url, _ in self.edits))
        self.bot.inbox.set(message['message_id'], 'reply', reply)
        self.assertIsNone(await self.deliver(message, reply))
        shown = [self.card_text(body) for _, body in self.posts + self.edits]
        self.assertEqual(shown[-1], 'BUSINESS-FINAL-ANSWER')
        self.assertNotIn('等待处理', shown[-1])
        self.assertTrue(set(shown[:-1]) <= {'等待处理', '正在查询资料', '正在整理答复'})
        for text in shown:
            self.assertNotIn('UNREVIEWED-BUSINESS-DRAFT', text)
            self.assertNotIn('PLAN-BODY', text)

    async def test_seeded_waiting_payload_shows_status_before_turn_is_readable(self):
        message = self.message()
        operation = self.waiting(message)['awaiting_turn']
        release = asyncio.Event()
        reads = 0

        async def call(request, actor, path, body=None, **kwargs):
            nonlocal reads
            if path == 'chat':
                await release.wait()
                return {'conversation_id': 'original', 'turns': [{
                    'operation_id': operation, 'status': 'completed', 'reply_mode': 'validated',
                    'answer': 'SEEDED-WAITING-FINAL'}]}
            reads += 1
            if reads >= 2:
                release.set()
            # The turn is not readable yet while the submitted call is still running.
            return {'conversation_id': 'original', 'turns': []}

        self.bot._call = AsyncMock(side_effect=call)
        with patch('lan_bitable_template_portal.feishu_assistant._LIVE_REPLY_INTERVAL', .01):
            reply = await asyncio.wait_for(self.bot._answer_message(message), 5)
        # The seeded payload published one generic status card before the turn existed.
        self.assertEqual(len(self.posts), 1)
        self.assertEqual(self.card_text(self.posts[0][1]), '等待处理')
        self.assertEqual(reply['live_reply']['message_id'], 'om_Live')
        for body in [body for _, body in self.posts + self.edits]:
            self.assertNotIn(message['question'], self.card_text(body))
        self.bot.inbox.set(message['message_id'], 'reply', reply)
        self.assertIsNone(await self.deliver(message, reply))
        self.assertEqual(len(self.posts), 1)
        self.assertEqual(self.card_text(self.edits[-1][1]), 'SEEDED-WAITING-FINAL')

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
