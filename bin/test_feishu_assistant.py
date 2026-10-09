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
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).parent))
from lan_bitable_template_portal.feishu_assistant import FeishuAssistant, FeishuMessenger, Inbox, parse_message
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

    def test_conversation_separates_people_rooms_and_web(self):
        keys = [LighthouseAssistant._key(actor) for actor in [
            {"id": "a"}, {"id": "b"}, {"id": "a", "channel": "feishu:oc_1"},
            {"id": "a", "channel": "feishu:oc_2"}, {"id": "b", "channel": "feishu:oc_1"}]]
        self.assertEqual(len(set(keys)), 5)
        self.assertEqual(keys[0], "conversation:a")


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

    async def test_untrusted_callback_cannot_submit(self):
        self.channel.secret = "private"
        for host, headers in [("192.168.1.2", [(b"authorization", b"Bearer private")]), ("127.0.0.1", []),
                              ("127.0.0.1", [(b"authorization", b"Bearer private"), (b"origin", b"http://evil")])]:
            request = Request({"type": "http", "client": (host, 1), "headers": headers})
            self.assertEqual((await self.channel.receive(request)).status_code, 403)

    async def test_user_mapping_uses_union_not_names_or_sender_open_id(self):
        self.store.put_document("feishu_assistant_identity", "on_user", {"union_id": "on_user", "open_id": "ou_Original", "name": "同名员工"})
        user = self.channel._user("on_user")
        self.assertEqual(user["open_id"], "ou_Original")
        self.runtime.auth_manager._open_id_explicitly_disabled=lambda _:True
        with self.assertRaises(AssistantError): self.channel._user("on_user")

    async def test_actor_uses_original_permission_and_channel_is_server_state_only(self):
        auth = SimpleNamespace(session_scopes=lambda _: ["D"], is_admin=lambda _:False)
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
