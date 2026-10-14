"""Feishu transport isolation tests: no network, messages or real business writes."""
import asyncio
import copy
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(0, str(Path(__file__).parent))
from lan_bitable_template_portal.feishu_assistant import FeishuAssistant, FeishuMessenger, Inbox, parse_message, _reply_text
from openclaw_service.assistant.lighthouse_ai import LighthouseAssistant, AssistantError
from lan_bitable_template_portal.lighthouse_bridge import actor_for
from starlette.requests import Request


def event(owner="onPerson", *, chat_type="p2p", bot="ouBot", message_id="omMessage", content="今天有哪些通告"):
    return {"header": {"app_id": "cliTest", "event_type": "im.message.receive_v1"}, "event": {
        "sender": {"sender_type": "user", "sender_id": {"open_id": "ou_Person", "union_id": "on_"+owner}},
        "message": {"message_id": "om_"+message_id, "chat_id": "oc_Chat", "chat_type": chat_type,
            "message_type": "text", "content": json.dumps({"text": content}),
            "mentions": [{"key": "@_user_1", "id": {"open_id": bot}}]}}}


class Memory:
    def __init__(self, path=None): self.docs = {}; self.db_path = path
    def get_document(self, ns, key): return copy.deepcopy(self.docs.get((ns, key)))
    def put_document(self, ns, key, value): self.docs[(ns, key)] = copy.deepcopy(value)
    def delete_document(self, ns, key): self.docs.pop((ns, key), None)
    def list_documents(self, ns, *, key_prefix=''):
        return [{'key': key, 'payload': copy.deepcopy(value)} for (namespace, key), value in self.docs.items()
                if namespace == ns and key.startswith(key_prefix)]


class ParsingTests(unittest.TestCase):
    def test_direct_and_mention_only_group(self):
        self.assertEqual(parse_message(event(), "cliTest", "ouBot")["question"], "今天有哪些通告")
        self.assertIsNone(parse_message(event(chat_type="group"), "cliTest", "anotherBot"))
        self.assertIsNotNone(parse_message(event(chat_type="group"), "cliTest", "ouBot"))
        self.assertIsNone(parse_message(event(), "wrongApp", "ouBot"))
        payload = event(); payload["event"]["sender"]["sender_type"] = "app"
        self.assertIsNone(parse_message(payload, "cliTest", "ouBot"))

    def test_requires_verified_union_identity(self):
        payload = event(); del payload["event"]["sender"]["sender_id"]["union_id"]
        self.assertEqual(parse_message(payload, "cliTest", "ouBot")["union_id"], "")

    def test_post_and_mention_removal(self):
        payload = event(content="@_user_1 你好")
        self.assertEqual(parse_message(payload, "cliTest", "ouBot")["question"], "你好")
        payload["event"]["message"].update(message_type="post", content=json.dumps({"title": "问题", "content": [[{"tag": "text", "text": "通告多少条"}]]}))
        self.assertIn("通告多少条", parse_message(payload, "cliTest", "ouBot")["question"])

    def test_private_data_not_persisted(self):
        question = parse_message(event(content="请查询身份证号"), "cliTest", "ouBot")["question"]
        self.assertIn("敏感查询", question)

    def test_resource_message_reaches_shared_file_flow_without_raw_post_text(self):
        payload = event()
        payload['event']['message'].update(message_type='file', content=json.dumps({'file_key': 'file_v3_abc-def', 'file_name': '公司手册.txt'}))
        result = parse_message(payload, 'cliTest', 'ouBot')
        self.assertFalse(result['problem'])
        self.assertEqual(result['content']['file_key'], 'file_v3_abc-def')
        self.assertNotIn('text', result['content'])

    def test_conversation_separates_people_but_reuses_rooms_and_web(self):
        keys = [LighthouseAssistant._key(actor) for actor in [
            {"id": "a"}, {"id": "b"}, {"id": "a", "channel": "feishu:oc_1"},
            {"id": "a", "channel": "feishu:oc_2"}, {"id": "b", "channel": "feishu:oc_1"}]]
        self.assertEqual(len(set(keys)), 2)
        self.assertEqual(keys[0], "conversation:a")
        self.assertEqual(keys[0], keys[2])
        self.assertEqual(keys[2], keys[3])
        self.assertEqual(keys[1], keys[4])


class InboxTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name)/"inbox.sqlite3"
        self.inbox = Inbox(self.path)

    def test_dedup_fifo_and_restart_reply_recovery(self):
        first = parse_message(event(message_id="first"), "cliTest", "ouBot")
        second = parse_message(event(message_id="second"), "cliTest", "ouBot")
        self.assertTrue(self.inbox.add(first)); self.assertFalse(self.inbox.add(first))
        self.inbox.add(second)
        taken = self.inbox.take(first["union_id"])
        self.assertEqual(taken["message_id"], first["message_id"])
        self.inbox.set(first["message_id"], "reply", {"text": "已完成回答"})
        self.inbox.take(first["union_id"])
        restored = Inbox(self.path)
        self.assertEqual(restored.take(first["union_id"])["reply"]["text"], "已完成回答")
        restored.set(first["message_id"], "done")
        self.assertEqual(restored.take(first["union_id"])["message_id"], second["message_id"])

    def test_many_people_not_limited_to_twenty(self):
        for number in range(30):
            self.inbox.add(parse_message(event(str(number), message_id=str(number)), "cliTest", "ouBot"))
        self.assertEqual(len(self.inbox.owners()), 30)

    def test_known_group_topic_accepts_other_members_without_mentions_after_restart(self):
        original = parse_message(event(chat_type='group'), 'cliTest', 'ouBot')
        self.inbox.add(original)
        self.inbox.set(original['message_id'], 'reply', {'thread_route': {'chat_id': 'oc_Chat',
            'root_id': original['message_id'], 'thread_id': 'omt_Topic', 'chat_type': 'group'},
            'thread_messages': ['om_Card'], 'ticket': 'fixture-ticket'})
        restored = Inbox(self.path)
        payload = event('anotherPerson', chat_type='group', message_id='followup')
        payload['event']['message'].update(mentions=[], root_id=original['message_id'], thread_id='omt_Topic')
        received = parse_message(payload, 'cliTest', 'ouBot', inbox=restored)
        self.assertEqual(received['union_id'], 'on_anotherPerson')
        self.assertEqual(received['thread_id'], 'omt_Topic')
        self.assertEqual(restored.thread_for({'chat_id': 'oc_Chat', 'reply_to_message_id': 'om_Card'})['ticket'], 'fixture-ticket')
        for changes in ({'chat_id': 'oc_Other'}, {'thread_id': 'omt_Other'}, {'root_id': 'om_Other'}):
            changed = copy.deepcopy(payload)
            changed['event']['message'].update(changes)
            self.assertIsNone(parse_message(changed, 'cliTest', 'ouBot', inbox=restored))
        self.assertIsNone(parse_message(payload, 'wrong-app', 'ouBot', inbox=restored))


class ChannelTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.store = Memory(Path(self.temp.name)/"portal.sqlite3")
        self.runtime = SimpleNamespace(state_store=self.store, auth_manager=SimpleNamespace(
            _open_id_explicitly_disabled=lambda _:False))
        self.controller = SimpleNamespace(bound_port=18766, preferred_port=18766)
        self.channel = FeishuAssistant(self.controller, self.runtime, AsyncMock())
        self.channel.inbox = Inbox(Path(self.temp.name)/"messages.sqlite3")

    async def test_parallel_people_and_same_person_fifo(self):
        running, maximum, seen = set(), 0, []
        all_entered = asyncio.Event()
        async def answer(message):
            nonlocal maximum
            owner = message["union_id"]
            self.assertNotIn(owner, running)
            running.add(owner); maximum = max(maximum, len(running))
            if len(running) >= 25: all_entered.set()
            await asyncio.wait_for(all_entered.wait(), 10)
            seen.append(message["message_id"]); running.remove(owner)
            return {"text": owner}
        self.channel.answer = answer
        self.channel.messenger = SimpleNamespace(send=AsyncMock())
        for number in range(25):
            self.channel.inbox.add(parse_message(event(str(number), message_id=f"a{number}"), "cliTest", "ouBot"))
        self.channel.inbox.add(parse_message(event("0", message_id="second"), "cliTest", "ouBot"))
        await asyncio.gather(*(self.channel._drain(owner) for owner in self.channel.inbox.owners()))
        self.assertGreater(maximum, 20)
        self.assertGreater(seen.index("om_second"), seen.index("om_a0"))
        self.assertEqual(self.channel.messenger.send.await_count, 26)

    async def test_reply_retry_never_reasks_model(self):
        message = parse_message(event(), "cliTest", "ouBot")
        self.channel.inbox.add(message)
        self.channel.answer = AsyncMock(return_value={"text": "saved"})
        self.channel.messenger = SimpleNamespace(send=AsyncMock(side_effect=TimeoutError()))
        await self.channel._drain(message["union_id"])
        self.channel.inbox.set(message["message_id"], "reply", delay=0)
        self.channel.messenger.send = AsyncMock()
        await self.channel._drain(message["union_id"])
        self.assertEqual(self.channel.answer.await_count, 1)
        self.assertEqual(self.channel.inbox.owners(), [])

    async def test_history_sync_retry_uses_saved_reply_without_reexecuting_business(self):
        message = parse_message(event(content='确认执行', message_id='history'), 'cliTest', 'ouBot')
        self.channel.inbox.add(message)
        reply = {'text': '操作已完成', 'sync_history': True, 'actor_id': 'ou_A', 'conversation_id': 'same'}
        self.channel.answer = AsyncMock(return_value=reply)
        self.channel._validate_delivery = AsyncMock()
        self.channel._request_context = AsyncMock(return_value=(None, {'id': 'ou_A', 'channel': 'feishu:oc_Chat'}))
        self.channel._call = AsyncMock(side_effect=[TimeoutError(), {'recorded': True}])
        self.channel.messenger = SimpleNamespace(send=AsyncMock(), react=AsyncMock())
        await self.channel._drain(message['union_id'])
        self.channel.messenger.send.assert_not_awaited()
        self.channel.inbox.set(message['message_id'], 'reply', delay=0)
        await self.channel._drain(message['union_id'])
        self.channel.answer.assert_awaited_once()
        self.channel.messenger.send.assert_awaited_once()
        calls = self.channel._call.await_args_list
        self.assertEqual(calls[0].args[2], 'channel-replies')
        self.assertEqual(calls[0].args[3], calls[1].args[3])

    async def test_history_sync_after_clear_does_not_restore_previous_conversation(self):
        message = parse_message(event(message_id='historycleared'), 'cliTest', 'ouBot')
        self.channel.inbox.add(message)
        reply = {'text': '旧回复', 'sync_history': True, 'actor_id': 'ou_A', 'conversation_id': 'old'}
        self.channel._request_context = AsyncMock(return_value=(None, {'id': 'ou_A'}))
        self.channel._call = AsyncMock(side_effect=AssistantError('会话已变化', 409))
        await self.channel._sync_reply_history(message, reply)
        self.assertNotIn('sync_history', reply)

    async def test_stopped_attachment_reply_retains_distinct_notice_until_delivery(self):
        message = parse_message(event(message_id='filestopped'), 'cliTest', 'ouBot')
        self.channel.inbox.add(message)
        reply = {'text': '文件已经生成', 'actor_id': 'fixture', 'conversation_id': 'same',
                 'sent_parts': ['answer:0'], 'files': [{'id': 'a' * 32, 'file_key': 'old_key'}]}
        self.channel.inbox.set(message['message_id'], 'reply', reply)
        self.channel.answer = AsyncMock()
        self.channel._validate_delivery = AsyncMock()
        self.channel._prepare_reply_files = AsyncMock(side_effect=AssistantError('文件权限失效', 403))
        self.channel.messenger = SimpleNamespace(send=AsyncMock())
        await self.channel._drain(message['union_id'])
        with self.channel.inbox.connect() as db:
            saved = json.loads(db.execute('SELECT reply FROM messages WHERE id=?', (message['message_id'],)).fetchone()[0])
        self.assertTrue(saved['attachment_stopped'])
        self.assertNotIn('files', saved)
        self.channel.inbox.set(message['message_id'], 'reply', saved, delay=0)
        self.channel._prepare_reply_files = AsyncMock()
        await self.channel._drain(message['union_id'])
        self.channel.answer.assert_not_awaited()
        self.assertTrue(self.channel.messenger.send.call_args.args[1]['attachment_stopped'])
        self.assertEqual(self.channel.inbox.owners(), [])

    async def test_missing_file_upload_scope_sends_a_short_notice_without_reasking_model(self):
        message = parse_message(event(message_id='uploadscope'), 'cliTest', 'ouBot')
        self.channel.inbox.add(message)
        self.channel.answer = AsyncMock(return_value={'text': '完整说明。' * 900})
        denied = AssistantError('飞书应用缺少文件上传权限，请联系管理员。', 403, category='feishu_file_upload_permission')
        self.channel.messenger = SimpleNamespace(send=AsyncMock(side_effect=denied), react=AsyncMock())
        await self.channel._drain(message['union_id'])
        with self.channel.inbox.connect() as db:
            saved = json.loads(db.execute('SELECT reply FROM messages WHERE id=?', (message['message_id'],)).fetchone()[0])
        self.assertTrue(saved.get('attachment_stopped'))
        self.assertLess(len(saved['text']), 400)
        self.assertIn('文件上传权限', saved['text'])
        self.channel.inbox.set(message['message_id'], 'reply', saved, delay=0)
        self.channel.messenger.send = AsyncMock()
        await self.channel._drain(message['union_id'])
        self.assertEqual(self.channel.answer.await_count, 1)
        self.assertEqual(self.channel.inbox.owners(), [])

    async def test_untrusted_callback_cannot_submit(self):
        self.channel.secret = "private"
        for host, headers in [("192.168.1.2", [(b"authorization", b"Bearer private")]), ("127.0.0.1", []),
                              ("127.0.0.1", [(b"authorization", b"Bearer private"), (b"origin", b"http://evil")])]:
            request = Request({"type": "http", "client": (host, 1), "headers": headers})
            self.assertEqual((await self.channel.receive(request)).status_code, 403)

    async def test_topic_without_shared_form_does_not_confirm_private_pending_plan(self):
        message = parse_message(event(chat_type='group', content='确认执行'), 'cliTest', 'ouBot')
        message.update(root_id='om_TopicRoot', thread_id='omt_Topic')
        self.channel._request_context = AsyncMock(return_value=(None, {'id': 'ou_Actual'}))
        self.channel._call = AsyncMock(return_value={'conversation_id': 'own', 'turns': [
            {'plan': {'id': 'private', 'status': 'awaiting_confirmation', 'version': 1}}]})
        result = await self.channel.answer(message)
        self.assertIn('未处理你在其他会话中的待办', result['text'])
        self.channel._call.assert_awaited_once()

    async def test_shared_capability_does_not_replace_actual_authority(self):
        from lan_bitable_template_portal.lighthouse_bridge import PortalAuthority
        actor = {'id': 'ou_Actual', 'scopes': ['E'], 'modules': ['assistant', 'maintenance'],
                 'learning_scopes': [], 'is_admin': False, 'channel': 'feishu:oc_Group'}
        authority = PortalAuthority(None, self.controller, self.runtime, SimpleNamespace(lease='fixture'))
        grant = {'id': 'specific-plan', 'owner': 'ou_OriginAdmin', 'conversation_id': 'origin', 'chat_id': 'oc_Group'}
        key = authority.context(None, actor, plan_id='specific-plan', shared_plan=grant)
        with patch('lan_bitable_template_portal.lighthouse_bridge.actor_for', AsyncMock(return_value=copy.deepcopy(actor))):
            _, actual = await authority.authorize(key)
        self.assertEqual(actual['id'], 'ou_Actual')
        self.assertFalse(actual['is_admin'])
        self.assertEqual(actual['scopes'], ['E'])
        self.assertEqual(actual['_shared_plan']['owner'], 'ou_OriginAdmin')
        revoked = {**actor, 'modules': ['assistant']}
        with patch('lan_bitable_template_portal.lighthouse_bridge.actor_for', AsyncMock(return_value=revoked)):
            with self.assertRaises(AssistantError): await authority.authorize(key)

    async def test_user_mapping_uses_union_not_names_or_sender_open_id(self):
        self.store.put_document("feishu_assistant_identity", "on_user", {"union_id": "on_user", "open_id": "ou_Original", "name": "同名员工"})
        user = self.channel._user("on_user")
        self.assertEqual(user["open_id"], "ou_Original")
        self.runtime.auth_manager._open_id_explicitly_disabled=lambda _:True
        with self.assertRaises(AssistantError): self.channel._user("on_user")

    def sender_directory(self, people):
        import time
        snapshot = {'loaded_at': time.time(), 'people': {person['id']: person for person in people}}
        self.runtime.auth_manager._account_directory_provider = lambda: snapshot
        self.runtime.auth_manager.directory_person = lambda identity: next(
            (person for person in snapshot['people'].values() if identity in person['login_ids']), {})
        self.channel.config = {'app_id': 'cliTest'}
        return snapshot

    async def test_known_sender_open_id_reuses_portal_account_without_union_or_network(self):
        person = {'id': 'recA', 'name': '王彦博', 'login_ids': ['ou_Person'], 'selectable': True}
        self.sender_directory([person])
        self.channel._user = Mock(side_effect=AssertionError('Exact identity needs no union lookup'))
        self.channel.messenger = SimpleNamespace(sender_name=AsyncMock())
        message = parse_message(event(), 'cliTest', 'ouBot')
        message['union_id'] = ''
        user = await self.channel._resolve_user(message)
        self.assertEqual(user['open_id'], 'ou_Person')
        self.channel.messenger.sender_name.assert_not_awaited()

    async def test_unique_feishu_name_binds_once_and_survives_restart_and_chat_change(self):
        person = {'id': 'recA', 'name': '王彦博', 'employee_no': '12345',
                  'login_ids': ['ou_Web'], 'selectable': True}
        self.sender_directory([person])
        self.channel._user = Mock(side_effect=AssistantError('unmapped', 403, category='feishu_identity_unmapped'))
        self.channel.messenger = SimpleNamespace(sender_name=AsyncMock(return_value='王彦博'))
        message = parse_message(event(content='我叫别的人'), 'cliTest', 'ouBot')
        user = await self.channel._resolve_user(message)
        self.assertEqual(user['open_id'], 'ou_Web')
        self.assertEqual(user['employee_no'], '12345')
        restarted = FeishuAssistant(self.controller, self.runtime, AsyncMock())
        restarted.config = dict(self.channel.config)
        restarted._user = Mock(side_effect=AssertionError('Cached relation must avoid network'))
        other_room = {**message, 'chat_id': 'oc_Other', 'chat_type': 'group'}
        self.assertEqual(await restarted._resolve_user(other_room), user)
        self.channel.messenger.sender_name.assert_awaited_once()
        self.assertEqual(LighthouseAssistant._key({'id': user['open_id'], 'channel': 'feishu:oc_Other'}),
                         LighthouseAssistant._key({'id': 'ou_Web'}))
        self.runtime.auth_manager._open_id_explicitly_disabled = lambda _: True
        with self.assertRaisesRegex(AssistantError, '停用'):
            await restarted._resolve_user(other_room)

    async def test_name_fallback_rejects_duplicates_departure_and_identity_conflicts(self):
        message = parse_message(event(), 'cliTest', 'ouBot')
        person = {'id': 'recA', 'name': '王彦博', 'login_ids': ['ou_Web'], 'selectable': True}
        for people in ([person, {**person, 'id': 'recB', 'login_ids': ['ou_Other']}],
                       [{**person, 'inactive': True}], [{**person, 'selectable': False}],
                       [{**person, 'login_ids': ['ou_Web', 'ou_Other']}]):
            with self.subTest(people=people):
                self.sender_directory(people)
                with self.assertRaises(AssistantError):
                    self.channel._sender_user(message, name='王彦博')
                self.assertFalse(self.store.get_document('feishu_assistant_sender', 'cliTest:ou_Person'))
        self.sender_directory([person, {**person, 'id': 'recOld', 'inactive': True, 'login_ids': ['ou_Old']}])
        self.assertEqual(self.channel._sender_user(message, name='王彦博')['open_id'], 'ou_Web')

    async def test_name_bindings_are_scoped_to_bot_application_and_cannot_follow_a_reused_record(self):
        message = parse_message(event(), 'cliTest', 'ouBot')
        person = {'id': 'recA', 'name': '王彦博', 'login_ids': ['ou_Web'], 'selectable': True}
        snapshot = self.sender_directory([person])
        self.channel._sender_user(message, name='王彦博')
        self.channel.config['app_id'] = 'cliOther'
        self.assertIsNone(self.channel._sender_user(message))
        self.channel.config['app_id'] = 'cliTest'
        snapshot['people'] = {'recOther': {**person, 'id': 'recOther'}}
        with self.assertRaisesRegex(AssistantError, '记录发生变化'):
            self.channel._sender_user(message)

    async def test_sender_name_uses_api_identity_and_external_group_pagination(self):
        messenger = FeishuMessenger({})
        messenger.headers = AsyncMock(return_value={})
        client = SimpleNamespace(get=AsyncMock(side_effect=[
            SimpleNamespace(json=lambda: {'code': 41050}),
            SimpleNamespace(json=lambda: {'code': 0, 'data': {'items': [
                {'member_id_type': 'open_id', 'member_id': 'ou_Other', 'name': '不能使用的姓名'}],
                'has_more': True, 'page_token': 'next'}}),
            SimpleNamespace(json=lambda: {'code': 0, 'data': {'items': [
                {'member_id_type': 'open_id', 'member_id': 'ou_Person', 'name': '王彦博'}], 'has_more': False}})]))
        messenger.client = AsyncMock(return_value=client)
        message = parse_message(event(chat_type='group', content='我叫李世龙'), 'cliTest', 'ouBot')
        self.assertEqual(await messenger.sender_name(message), '王彦博')
        self.assertEqual(client.get.call_args.kwargs['params']['page_token'], 'next')
        client.get = AsyncMock(return_value=SimpleNamespace(json=lambda: {'code': 41050}))
        with self.assertRaisesRegex(AssistantError, '读取人员或群成员'):
            await messenger.sender_name({**message, 'chat_type': 'p2p'})

    async def test_filtered_reply_keeps_an_explicit_notice_instead_of_claiming_no_answer(self):
        text = _reply_text('不能提供身份证号或家庭住址。')
        self.assertIn('受保护内容', text)
        self.assertEqual(_reply_text(text), text)
        self.assertIn('暂未生成回答', _reply_text('  '))
        self.assertEqual(_reply_text('可以帮你整理资料。'), '可以帮你整理资料。')

    async def test_actor_uses_original_permission_and_channel_is_server_state_only(self):
        auth = SimpleNamespace(session_scopes=lambda _: ["D"], is_admin=lambda _:False, account_profile=lambda _: {"modules": ["assistant"]})
        ctrl = SimpleNamespace(_current_session=lambda _: {"role": "building", "user": {"open_id": "ou_Original", "name": "D楼"}})
        request = Request({"type": "http", "headers": [(b"x-lighthouse-channel", b"feishu:oc_Evil")], "state": {}})
        actor = await actor_for(ctrl, SimpleNamespace(auth_manager=auth), request)
        self.assertNotIn("channel", actor)
        self.assertEqual(actor["scopes"], ["D"])
        request.state.lighthouse_channel = "feishu:oc_Chat"
        self.assertEqual((await actor_for(ctrl, SimpleNamespace(auth_manager=auth), request))["channel"], "feishu:oc_Chat")

    async def test_confirmation_requires_unique_current_plan(self):
        message = parse_message(event(content="确认执行"), "cliTest", "ouBot")
        self.channel._request_context = AsyncMock(return_value=(None, {"id": "ou_A"}))
        self.channel._call = AsyncMock(return_value={"turns": [{"plan": {"status": "awaiting_confirmation"}}, {"plan": {"status": "awaiting_confirmation"}}]})
        result = await self.channel.answer(message)
        self.assertIn("没有唯一", result["text"])
        self.assertEqual(self.channel._call.await_count, 1)

    async def test_confirmation_preview_includes_values(self):
        text = self.channel.plan_text({"title": "更新通告", "status": "awaiting_confirmation", "operations": [{"body": {"name": "设备维护", "progress": "已检查"}}]})
        self.assertIn("已检查", text)
        self.assertIn("尚未执行", text)

    async def test_cancel_incomplete_form_reuses_original_plan_api(self):
        message = parse_message(event(content="取消操作"), "cliTest", "ouBot")
        self.channel._request_context = AsyncMock(return_value=(None, {"id": "ou_A"}))
        plan = {"id": "pending_form", "status": "needs_input"}
        self.channel._call = AsyncMock(side_effect=[{"turns": [{"plan": plan}]}, {}])
        result = await self.channel.answer(message)
        self.assertIn("已取消", result["text"])
        self.assertEqual(self.channel._call.await_args.args[2:], ("plans/pending_form/cancel", {}))

    async def test_cancel_does_not_guess_or_withdraw_submitted_business(self):
        message = parse_message(event(content="取消操作"), "cliTest", "ouBot")
        self.channel._request_context = AsyncMock(return_value=(None, {"id": "ou_A"}))
        for plans in ([{"id": "a", "status": "needs_input"}, {"id": "b", "status": "awaiting_confirmation"}],
                      [{"id": "a", "status": "submitted"}]):
            with self.subTest(plans=plans):
                self.channel._call = AsyncMock(return_value={"turns": [{"plan": p} for p in plans]})
                result = await self.channel.answer(message)
                self.assertIn("没有唯一", result["text"])
                self.channel._call.assert_awaited_once()

    async def test_group_result_replies_to_original_message_without_private_copy(self):
        messenger = FeishuMessenger({})
        client = SimpleNamespace(post=AsyncMock(return_value=SimpleNamespace(json=lambda:{"code":0})))
        messenger.client = AsyncMock(return_value=client)
        messenger.headers = AsyncMock(return_value={})
        message = parse_message(event(chat_type="group"), "cliTest", "ouBot")
        with patch("lan_bitable_template_portal.portal_service.external_real_write_guard", return_value={"real_write_allowed":True}):
            await messenger.send(message, {"text":"内部业务资料"})
        client.post.assert_awaited_once()
        reply = client.post.call_args
        self.assertTrue(reply.args[0].endswith('/' + message['message_id'] + '/reply'))
        self.assertNotIn('receive_id', reply.kwargs['json'])
        self.assertIn('内部业务资料', reply.kwargs['json']['content'])
        self.assertTrue(reply.kwargs['json']['reply_in_thread'])

    async def test_thread_response_checkpoints_route_and_all_parts_keep_same_root(self):
        messenger = FeishuMessenger({})
        client = SimpleNamespace(post=AsyncMock(return_value=SimpleNamespace(json=lambda: {'code': 0,
            'data': {'message_id': 'om_Delivered', 'root_id': 'om_Root', 'thread_id': 'omt_Topic'}})))
        messenger.client = AsyncMock(return_value=client)
        messenger.headers = AsyncMock(return_value={})
        message = parse_message(event(chat_type='group'), 'cliTest', 'ouBot')
        message.update(root_id='om_Root', thread_id='omt_Topic')
        self.channel.inbox.add(message)
        reply = {'text': '处理结果', 'files': [{'id': 'fixture', 'file_key': 'file_fixture'}]}
        async def checkpoint():
            self.channel.inbox.set(message['message_id'], 'reply', reply)
        with patch('lan_bitable_template_portal.portal_service.external_real_write_guard', return_value={'real_write_allowed': True}):
            await messenger.send(message, reply, checkpoint=checkpoint)
            await messenger.send(message, reply, checkpoint=checkpoint)
        self.assertEqual(client.post.await_count, 2)
        for call in client.post.await_args_list:
            self.assertTrue(call.args[0].endswith('/om_Root/reply'))
            self.assertTrue(call.kwargs['json']['reply_in_thread'])
        self.assertTrue(self.channel.inbox.has_thread('oc_Chat', 'om_Root', 'omt_Topic'))

    async def test_unsupported_topic_falls_back_once_with_same_uuid_but_network_errors_do_not(self):
        for code in (230071, 230072, 99991400):
            messenger = FeishuMessenger({})
            client = SimpleNamespace(post=AsyncMock(side_effect=[SimpleNamespace(json=lambda: {'code': code}),
                                                                 SimpleNamespace(json=lambda: {'code': 0})]))
            messenger.client = AsyncMock(return_value=client)
            messenger.headers = AsyncMock(return_value={})
            message = parse_message(event(), 'cliTest', 'ouBot')
            reply = {'text': '保留的回复'}
            with patch('lan_bitable_template_portal.portal_service.external_real_write_guard', return_value={'real_write_allowed': True}):
                if code == 99991400:
                    with self.assertRaises(AssistantError): await messenger.send(message, reply)
                    self.assertEqual(client.post.await_count, 1)
                    self.assertNotIn('thread_fallback', reply)
                else:
                    await messenger.send(message, reply)
                    calls = client.post.await_args_list
                    self.assertTrue(calls[0].kwargs['json']['reply_in_thread'])
                    self.assertFalse(calls[1].kwargs['json']['reply_in_thread'])
                    self.assertEqual(calls[0].kwargs['json']['uuid'], calls[1].kwargs['json']['uuid'])

    async def test_editable_reply_uses_real_callback_message_and_falls_back_only_on_rejection(self):
        messenger = FeishuMessenger({})
        client = SimpleNamespace(post=AsyncMock(side_effect=[
            SimpleNamespace(json=lambda: {'code': 230001}), SimpleNamespace(json=lambda: {'code': 0})]))
        messenger.client = AsyncMock(return_value=client)
        messenger.headers = AsyncMock(return_value={})
        message = parse_message(event(chat_type='group'), 'cliTest', 'ouBot')
        message.update(message_id='crd_synthetic', reply_to_message_id='om_actualcard')
        reply = {'text': '填写仍保留', 'card': {'schema': '2.0', 'body': {'elements': []}}}
        with patch('lan_bitable_template_portal.portal_service.external_real_write_guard', return_value={'real_write_allowed': True}):
            await messenger.send(message, reply)
        self.assertEqual(client.post.await_count, 2)
        self.assertTrue(all(call.args[0].endswith('/om_actualcard/reply') for call in client.post.await_args_list))
        self.assertNotIn('card', reply)
        self.assertIn('原填写仍保留', json.loads(client.post.await_args.kwargs['json']['content'])['body']['elements'][0]['content'])

    async def test_parallel_group_answers_keep_original_message_and_retry_uuid(self):
        messenger = FeishuMessenger({})
        client = SimpleNamespace(post=AsyncMock(return_value=SimpleNamespace(json=lambda:{"code":0})))
        messenger.client = AsyncMock(return_value=client)
        messenger.headers = AsyncMock(return_value={})
        messages = [parse_message(event(owner=str(i), chat_type='group', message_id=str(i)), 'cliTest', 'ouBot') for i in range(2)]
        with patch('lan_bitable_template_portal.portal_service.external_real_write_guard', return_value={'real_write_allowed':True}):
            await asyncio.gather(*(messenger.send(m, {'text':'answer ' + str(i)}) for i, m in enumerate(messages)))
            await messenger.send(messages[0], {'text':'answer 0'})
        calls = client.post.call_args_list
        self.assertEqual(calls[0].args[0], calls[2].args[0])
        self.assertNotEqual(calls[0].args[0], calls[1].args[0])
        self.assertEqual(calls[0].kwargs['json']['uuid'], calls[2].kwargs['json']['uuid'])
        self.assertNotEqual(calls[0].kwargs['json']['uuid'], calls[1].kwargs['json']['uuid'])

    async def test_message_transport_reuses_one_http_client_during_login_burst(self):
        messenger = FeishuMessenger({})
        fake = SimpleNamespace()
        with patch("httpx.AsyncClient", return_value=fake) as factory:
            clients = await asyncio.gather(*(messenger.client() for _ in range(25)))
        self.assertTrue(all(client is fake for client in clients))
        factory.assert_called_once()

    async def test_typing_reaction_uses_original_message_for_group_and_private(self):
        messenger = FeishuMessenger({})
        client = SimpleNamespace(post=AsyncMock(return_value=SimpleNamespace(json=lambda:{'code':0})))
        messenger.client = AsyncMock(return_value=client)
        messenger.headers = AsyncMock(return_value={})
        with patch('lan_bitable_template_portal.portal_service.external_real_write_guard', return_value={'real_write_allowed':True}):
            for kind in ('group', 'p2p'):
                message = parse_message(event(chat_type=kind, message_id=kind), 'cliTest', 'ouBot')
                await messenger.react(message)
                request = client.post.call_args
                self.assertTrue(request.args[0].endswith('/' + message['message_id'] + '/reactions'))
                self.assertEqual(request.kwargs['json'], {'reaction_type': {'emoji_type':'Typing'}})
        with patch('lan_bitable_template_portal.portal_service.external_real_write_guard', return_value={'real_write_allowed':False}):
            await messenger.react(message)
        self.assertEqual(client.post.await_count, 2)

    async def test_reaction_precedes_answer_once_and_failure_does_not_block(self):
        for kind in ('group', 'p2p'):
            with self.subTest(kind=kind):
                message = parse_message(event(chat_type=kind, message_id=kind), 'cliTest', 'ouBot')
                self.channel.inbox.add(message)
                order = []
                async def react(_):
                    order.append('reaction')
                    if kind == 'group': raise TimeoutError()
                async def answer(_):
                    order.append('answer')
                    return {'text':'answer'}
                self.channel.answer = AsyncMock(side_effect=answer)
                self.channel.messenger = SimpleNamespace(react=AsyncMock(side_effect=react), send=AsyncMock())
                await self.channel._drain(message['union_id'])
                self.assertEqual(order, ['reaction', 'answer'])
                self.channel.messenger.send.assert_awaited_once()
                await self.channel._react(message)
                recovered = Inbox(self.channel.inbox.path)
                self.assertFalse(recovered.claim_reaction(message['message_id']))
                self.assertEqual(self.channel.messenger.react.await_count, 1)

    async def test_reaction_failure_is_reported_without_sensitive_payload(self):
        messenger = FeishuMessenger({})
        client = SimpleNamespace(post=AsyncMock(return_value=SimpleNamespace(json=lambda:{'code':99991672})))
        messenger.client = AsyncMock(return_value=client)
        messenger.headers = AsyncMock(return_value={})
        with patch('lan_bitable_template_portal.portal_service.external_real_write_guard', return_value={'real_write_allowed':True}):
            with self.assertRaisesRegex(AssistantError, '99991672'):
                await messenger.react({'message_id':'om_test'})


if __name__ == "__main__": unittest.main()
