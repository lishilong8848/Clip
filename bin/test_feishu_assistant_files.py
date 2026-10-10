"""Feishu attachment parity tests: fake http/resources/temp state, no network."""
import asyncio
import copy
import hashlib
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).parent))
from lan_bitable_template_portal import feishu_assistant_files as files_module
from lan_bitable_template_portal.feishu_assistant_files import (
    NAMESPACE, CONFIRM_READ, CONFIRM_KB, parse_resources, handle_files,
    _identity, _pending_key, _kb_key)
from openclaw_service.assistant.lighthouse_ai import AssistantError

DEFAULT_IMAGE = b"\x89PNG\r\n\x1a\n" + b"pillow-data"


def message(mid="om_m1", owner="ouOwner", chat="oc_chat", content=None, question=""):
    return {"message_id": "om_" + mid, "sender_id": "ou_" + owner, "union_id": "on_" + owner,
            "chat_id": "oc_" + chat, "chat_type": "p2p", "question": question,
            "content": content or {}}


class Memory:
    def __init__(self): self.docs = {}
    def get_document(self, ns, key): return copy.deepcopy(self.docs.get((ns, key)))
    def put_document(self, ns, key, value): self.docs[(ns, key)] = copy.deepcopy(value)
    def delete_document(self, ns, key): self.docs.pop((ns, key), None)
    def list_documents(self, ns, *, key_prefix=""):
        return [{"key": key, "payload": copy.deepcopy(value)} for (namespace, key), value in self.docs.items()
                if namespace == ns and key.startswith(key_prefix)]


class FakeStream:
    def __init__(self, content=DEFAULT_IMAGE, status=200):
        self.content, self.status_code = content, status
    async def __aenter__(self): return self
    async def __aexit__(self, *a): return False
    async def aiter_bytes(self):
        yield self.content


def _entry_name(entry):
    spec = entry[1] if isinstance(entry, (list, tuple)) and len(entry) > 1 else entry
    if isinstance(spec, (list, tuple)) and spec:
        return str(spec[0])
    return str(spec)


class FakeClient:
    def __init__(self, results=None, streams=None):
        self.results = results or {}
        self.streams = streams or {}
        self.calls = []
    def stream(self, *a, **k):
        self.calls.append(("stream", a, k))
        url = a[1] if len(a) > 1 else k.get("url", "")
        for marker, fs in self.streams.items():
            if marker in (url or ""):
                return fs
        return FakeStream(self.results.get("stream", DEFAULT_IMAGE))
    async def post(self, url, **kwargs):
        self.calls.append(("post", url, kwargs))
        uploads = kwargs.get("files") or []
        if url.endswith("/knowledge/files"):
            data = self.results.get("post") if "post" in self.results else None
            if data is not None:
                return SimpleNamespace(json=lambda: data, status_code=200)
            ids = ["kb_" + hashlib.sha1(_entry_name(f).encode()).hexdigest()[:10] for f in uploads]
            return SimpleNamespace(json=lambda: {"ok": True, "data": {"items": [{"id": i, "status": "queued"} for i in ids], "errors": []}}, status_code=200)
        ids = ["f_" + hashlib.sha1(_entry_name(f).encode()).hexdigest()[:10] for f in uploads]
        return SimpleNamespace(json=lambda: {"ok": True, "data": {"files": [{"id": i} for i in ids]}}, status_code=200)
    async def get(self, url, **kwargs):
        self.calls.append(("get", url, kwargs))
        return SimpleNamespace(content=self.results.get("get", b"downloaded-bytes"), status_code=200)


class FakeCurrent:
    def __init__(self, client=None):
        self.key, self.instance, self.lease = "key", "inst", "lease"
        self.descriptor = {"port": 18799}
        self._client = client or FakeClient()
    async def http_client(self): return self._client


class FakeAuthority:
    def __init__(self): self.contexts = {}
    def context(self, request, actor, plan_id=None):
        token = id((request, actor, plan_id))
        self.contexts[token] = True
        return token


class FakeReady:
    def __init__(self, current=None, authority=None):
        self.current = current or FakeCurrent()
        self.authority = authority or FakeAuthority()
    async def __call__(self):
        return self.current, self.authority


def fake_bot(store, *, kbase=None, messenger_client=None):
    bot = SimpleNamespace()
    bot.runtime = SimpleNamespace(state_store=store)
    bot.ready = FakeReady(FakeCurrent(messenger_client))
    messenger = SimpleNamespace(
        client=AsyncMock(return_value=bot.ready.current._client),
        headers=AsyncMock(return_value={"Authorization": "Bearer t"}))
    bot.messenger = messenger
    call = AsyncMock()
    call.side_effect = lambda req, act, path, body=None, method=None, params=None, plan_id=None: \
        ({"items": kbase.get("items", [])} if path == "knowledge" and params else {})
    bot._call = call
    bot._kb = kbase or {"items": []}
    return bot


def make_kb_bot(store, pages, total):
    bot = SimpleNamespace()
    bot.runtime = SimpleNamespace(state_store=store)
    bot.messenger = SimpleNamespace(client=AsyncMock(), headers=AsyncMock())
    bot.ready = FakeReady(FakeCurrent())
    async def _call(request, actor, path, body=None, *, method=None, params=None, plan_id=None):
        if path == "knowledge":
            page = int((params or {}).get("page", "1"))
            return {"items": pages.get(page, []), "total": total}
        return {}
    bot._call = _call
    return bot


class ParseTests(unittest.TestCase):
    def test_image_file_and_post(self):
        msg = message(mid="a1")
        self.assertEqual(len(parse_resources(msg, {"image_key": "img_v2_key1"})), 1)
        self.assertEqual(parse_resources(msg, {"file_key": "file_v2_key1", "file_name": "a.pdf"})[0]["name"], "a.pdf")
        post = {"zh_cn": {"content": [[{"tag": "img", "image_key": "img_v2_k2"}],
                                       [{"tag": "text", "text": "hi"}, {"tag": "img", "image_key": "img_v2_k3"}]]}}
        keys = [r["key"] for r in parse_resources(msg, post)]
        self.assertEqual(keys, ["img_v2_k2", "img_v2_k3"])

    def test_limits_and_validation(self):
        msg = message(mid="a2")
        content = {"content": [[{"tag": "img", "image_key": "img_v2_x" + str(i)}] for i in range(12)]}
        self.assertEqual(len(parse_resources(msg, content)), 12)  # no silent truncation
        bad = {"image_key": "https://evil/x.png"}
        self.assertEqual(parse_resources(msg, bad), [])
        malformed = {"image_key": "img_v2_bad!"}
        self.assertEqual(parse_resources(msg, malformed), [])
        mismatch = {"image_key": "file_v2_wrong"}
        self.assertEqual(parse_resources(msg, mismatch), [])
        invalid = message(mid="not-a-id")
        self.assertEqual(parse_resources(invalid, {"image_key": "img_v2_ok"}), [])

    def test_real_hyphen_and_v3_keys(self):
        msg = message(mid="hyp")
        self.assertEqual(parse_resources(msg, {"file_key": "file_v3_key-with_suffix", "file_name": "a.pdf"})[0]["key"],
                         "file_v3_key-with_suffix")
        self.assertEqual(parse_resources(msg, {"image_key": "img_v3_ab-cd"})[0]["key"], "img_v3_ab-cd")


class ResourceFlowTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.store = Memory()
        self.owner, self.actor_id, self.channel = "ownerhash", "ou_user", "feishu:oc_chat"
        self.request = SimpleNamespace()
        self.actor = {"id": "ou_user", "channel": "feishu:oc_chat"}

    def test_identity_and_pending(self):
        owner, actor_id, channel = _identity(self.actor, message())
        self.assertEqual(channel, "feishu:oc_chat")
        # Stable single pending bundle key per actor+channel (message id ignored)
        self.assertEqual(_pending_key(owner, "m1"), _pending_key(owner, "m2"))

    def test_stable_kb_key_ignores_op(self):
        self.assertEqual(_kb_key(self.owner, {"action": "add", "document_id": "a"}),
                         _kb_key(self.owner, {"action": "delete", "document_id": "b"}))

    async def test_image_triggers_confirm_not_download(self):
        bot = fake_bot(self.store)
        msg = message(mid="img1", content={"image_key": "img_v2_k1"})
        result = await handle_files(bot, msg, self.request, self.actor, {})
        self.assertIn(CONFIRM_READ, result["text"])
        self.assertTrue(bot.messenger.client.await_count == 0)
        self.assertEqual(len(self.store.list_documents(NAMESPACE)), 1)

    async def test_confirm_read_imports_and_reuses(self):
        bot = fake_bot(self.store)
        msg = message(mid="img1", content={"image_key": "img_v2_k1"})
        await handle_files(bot, msg, self.request, self.actor, {})
        confirm = message(mid="c1", question=CONFIRM_READ)
        result = await handle_files(bot, confirm, self.request, self.actor, {})
        self.assertIn("file_ids", result)
        self.assertEqual(len(result["file_ids"]), 1)
        first_calls = len(bot.ready.current._client.calls)

        # Retry / response loss must reuse same imported file id, no second upload.
        result2 = await handle_files(bot, confirm, self.request, self.actor, {})
        self.assertEqual(result2["file_ids"], result["file_ids"])
        self.assertEqual(len(bot.ready.current._client.calls), first_calls)

    async def test_too_many_resources_errors(self):
        bot = fake_bot(self.store)
        content = {"content": [[{"tag": "img", "image_key": "img_v2_x" + str(i)}] for i in range(12)]}
        msg = message(mid="many", content=content)
        result = await handle_files(bot, msg, self.request, self.actor, {})
        self.assertIn("一次最多接收", result["text"])
        self.assertEqual(len(self.store.list_documents(NAMESPACE)), 0)

    async def test_kb_add_prepare_then_confirm(self):
        msg = message(mid="img1", content={"image_key": "img_v2_k1"})
        bot = fake_bot(self.store)
        await handle_files(bot, msg, self.request, self.actor, {})
        await handle_files(bot, message(mid="c1", question=CONFIRM_READ), self.request, self.actor, {})
        row = self.store.list_documents(NAMESPACE)[0]
        res = row["payload"]["resources"][0]
        name = res["name"]
        add_cmd = message(mid="kb1", question=f"将附件加入知识库：{name}")
        prepared = await handle_files(bot, add_cmd, self.request, self.actor, {})
        self.assertIn(CONFIRM_KB, prepared["text"])
        confirmed = await handle_files(bot, message(mid="kb2", question=CONFIRM_KB),
                                       self.request, self.actor, {})
        self.assertIn("正在排队入库", confirmed["text"])

    async def test_total_bytes_includes_already_imported(self):
        owner, actor_id, channel = _identity(self.actor, {})
        key = _pending_key(owner, "tot")
        self.store.put_document(NAMESPACE, key, {
            "owner": owner, "actor": actor_id, "channel": channel, "expires": 9999999999,
            "resources": [
                {"kind": "file", "key": "file_v2_a", "name": "a.pdf", "file_id": "fA", "size": 95,
                 "original_mid": "om_from"},
                {"kind": "file", "key": "file_v2_b", "name": "b.pdf", "original_mid": "om_from"},
            ]})
        bot = fake_bot(self.store)
        with patch.object(files_module, "MAX_BATCH", 100):
            with self.assertRaises(AssistantError) as ctx:
                await handle_files(bot, message(mid="c", question=CONFIRM_READ), self.request, self.actor, {})
        self.assertIn("合计超过100MiB", str(ctx.exception))
        doc = self.store.get_document(NAMESPACE, key)
        self.assertEqual(doc["resources"][0]["file_id"], "fA")
        self.assertIsNone(doc["resources"][1].get("file_id"))

    async def test_partial_import_then_retry(self):
        owner, actor_id, channel = _identity(self.actor, {})
        key = _pending_key(owner, "part")
        self.store.put_document(NAMESPACE, key, {
            "owner": owner, "actor": actor_id, "channel": channel, "expires": 9999999999,
            "resources": [
                {"kind": "file", "key": "file_v2_a", "name": "a.pdf", "original_mid": "om_from"},
                {"kind": "file", "key": "file_v2_b", "name": "b.pdf", "original_mid": "om_from"},
            ]})
        streams = {"file_v2_a": FakeStream(b"AAA", 200), "file_v2_b": FakeStream(b"", 503)}
        client = FakeClient(streams=streams)
        bot = fake_bot(self.store, messenger_client=client)
        with self.assertRaises(AssistantError):
            await handle_files(bot, message(mid="c", question=CONFIRM_READ), self.request, self.actor, {})
        doc = self.store.get_document(NAMESPACE, key)
        self.assertTrue(doc["resources"][0]["file_id"])
        self.assertIsNone(doc["resources"][1].get("file_id"))
        # Retry: early receipt is reused, only the failed file is re-attempted.
        client.streams["file_v2_b"] = FakeStream(b"BBB", 200)
        before = len(client.calls)
        result = await handle_files(bot, message(mid="c2", question=CONFIRM_READ), self.request, self.actor, {})
        self.assertEqual(len(result["file_ids"]), 2)
        doc = self.store.get_document(NAMESPACE, key)
        self.assertTrue(doc["resources"][0]["file_id"])
        self.assertTrue(doc["resources"][1]["file_id"])
        # file A was not re-uploaded: only one "files" post total for A + one for B
        upload_calls = [c for c in client.calls if c[0] == "post"]
        self.assertEqual(len(upload_calls), 2)

    async def test_failed_kb_partial_response(self):
        owner, actor_id, channel = _identity(self.actor, {})
        key = _kb_key(owner, {})
        self.store.put_document(NAMESPACE, key, {
            "action": "add", "file_id": "fA", "file_name": "a.pdf",
            "document_id": None, "owner": owner, "actor": actor_id, "channel": channel,
            "expires": 9999999999, "receipt": None})
        client = FakeClient(results={"post": {"ok": True, "data": {
            "items": [{"id": "ok", "status": "queued"}],
            "errors": [{"name": "a.pdf", "error": "部分文件失败"}]}}})
        bot = fake_bot(self.store, messenger_client=client)
        result = await handle_files(bot, message(mid="c", question=CONFIRM_KB), self.request, self.actor, {})
        self.assertIn("部分文件失败", result["text"])

    async def test_restore_uses_deleted(self):
        store = Memory()
        actorA = {"id": "ou_userR", "channel": "feishu:ocR"}
        deleted_doc = {"id": "docR", "name": "已删文档", "version": 9, "can_edit": True, "status": "deleted"}
        bot = fake_bot(store, kbase={"items": [deleted_doc]})
        result = await handle_files(bot, message(mid="rr", chat="chR", question="恢复知识库：已删文档"),
                                    self.request, actorA, {})
        self.assertIn(CONFIRM_KB, result["text"])
        confirmed = await handle_files(bot, message(mid="rc", chat="chR", question=CONFIRM_KB),
                                       self.request, actorA, {})
        self.assertIn("已恢复", confirmed["text"])

    async def test_duplicate_names_across_pages(self):
        store = Memory()
        actorA = {"id": "ou_userD", "channel": "feishu:ocD"}
        pages = {
            1: [{"id": f"d{i}", "name": "A" if i == 0 else f"doc{i}", "version": 3,
                 "can_edit": True, "status": "queued"} for i in range(20)],
            2: [{"id": "d_dup", "name": "A", "version": 3, "can_edit": True, "status": "queued"}],
        }
        bot = make_kb_bot(store, pages, 40)
        with self.assertRaises(AssistantError) as ctx:
            await handle_files(bot, message(mid="k", chat="chD", question="删除知识库：A"),
                               self.request, actorA, {})
        self.assertIn("多个同名", str(ctx.exception))

    async def test_one_pending_kb_op_latest_and_actor_isolation(self):
        store = Memory()
        actorA = {"id": "ou_userA", "channel": "feishu:ocA"}
        docX = {"id": "docX", "name": "文档X", "version": 3, "can_edit": True, "status": "queued"}
        docY = {"id": "docY", "name": "文档Y", "version": 7, "can_edit": True, "status": "queued"}
        botA1 = fake_bot(store, kbase={"items": [docX]})
        await handle_files(botA1, message(mid="dx", chat="chA", question="删除知识库：文档X"),
                           self.request, actorA, {})
        botA2 = fake_bot(store, kbase={"items": [docY]})
        await handle_files(botA2, message(mid="dy", chat="chA", question="删除知识库：文档Y"),
                           self.request, actorA, {})
        result = await handle_files(botA2, message(mid="ck", chat="chA", question=CONFIRM_KB),
                                    self.request, actorA, {})
        self.assertIn("文档Y", result["text"])
        # Another actor in a different channel must not see A's pending op.
        actorB = {"id": "ou_userB", "channel": "feishu:ocB"}
        botB = fake_bot(store, kbase={"items": []})
        resB = await handle_files(botB, message(mid="cb", chat="chB", question=CONFIRM_KB),
                                  self.request, actorB, {})
        self.assertIn("没有待确认", resB["text"])

    async def test_ambiguous_names_not_picked(self):
        store = Memory()
        actorA = {"id": "ou_userA", "channel": "feishu:ocA"}
        owner, actor_id, channel = _identity(actorA, message(chat="chA"))
        # Same-named attachments can only coexist through conversation turns now
        # (single pending bundle per actor+channel), so derive ambiguity there.
        conversation = {"turns": [{
            "output_files": [{"id": "f1", "name": "同一名字.pdf"}],
            "attachments": [{"id": "f2", "name": "同一名字.pdf"}],
        }]}
        bot = fake_bot(store, kbase={"items": []})
        result = await handle_files(bot, message(mid="k1", chat="chA", question="将附件加入知识库：同一名字.pdf"),
                                    self.request, actorA, conversation)
        self.assertIn("多个同名", result["text"])
        result2 = await handle_files(bot, message(mid="k1b", chat="chA", question="将附件加入知识库：同名a"),
                                     self.request, actorA, conversation)
        self.assertIn("未找到", result2["text"])

    async def test_image_name_detects_real_format(self):
        res = {"kind": "image", "original_mid": "om_from", "key": "img_v3_abc", "name": None}
        from lan_bitable_template_portal.feishu_assistant_files import _upload_name
        self.assertTrue(_upload_name(res, b"\x89PNG\r\n\x1a\n" + b"rest").endswith(".png"))
        self.assertTrue(_upload_name(res, b"\xff\xd8\xff\xe0" + b"..").endswith(".jpg"))
        self.assertTrue(_upload_name(res, b"GIF89a...").endswith(".gif"))
        self.assertTrue(_upload_name(res, b"RIFF\x00\x00\x00\x00WEBPVP8 " + b"..").endswith(".webp"))
        with self.assertRaises(AssistantError):
            _upload_name(res, b"not-an-image-bytes")

    async def test_expired_pending_not_used(self):
        owner, actor_id, channel = _identity(self.actor, {})
        key = _pending_key(owner, "exp")
        self.store.put_document(NAMESPACE, key, {"owner": owner, "actor": actor_id,
            "channel": channel, "expires": 1, "resources": [{"kind": "image", "key": "img_v2_e",
            "name": None, "original_mid": "om_exp"}]})
        bot = fake_bot(self.store)
        result = await handle_files(bot, message(mid="c", question=CONFIRM_READ),
                                    self.request, self.actor, {})
        self.assertIn("没有待确认", result["text"])
        self.assertNotIn((NAMESPACE, key), self.store.docs)

    async def test_new_uploads_reuse_one_pending_record_and_clear_invalidates_confirmation(self):
        bot = fake_bot(self.store)
        for index in range(100):
            await handle_files(bot, message(mid=f'file{index}', content={'file_key': f'file_v3_{index}', 'file_name': 'guide.txt'}),
                               self.request, self.actor, {'conversation_id': 'old'})
        self.assertEqual(len(self.store.list_documents(NAMESPACE)), 1)
        result = await handle_files(bot, message(mid='confirm', question=CONFIRM_READ), self.request, self.actor, {'conversation_id': 'new'})
        self.assertIn('已失效', result['text'])
        bot.messenger.client.assert_not_awaited()

    async def test_heavy_limit_covers_download_through_upload_for_many_people(self):
        bot = fake_bot(self.store)
        active, maximum = 0, 0
        async def download(*args):
            nonlocal active, maximum
            active += 1
            maximum = max(maximum, active)
            await asyncio.sleep(.005)
            return b'fixture'
        async def upload(*args):
            nonlocal active
            await asyncio.sleep(.005)
            active -= 1
            return 'fileid'
        people = [{**self.actor, 'id': f'person{i}'} for i in range(24)]
        for index, person in enumerate(people):
            await handle_files(bot, message(mid=f'u{index}', content={'file_key': f'file_v3_{index}', 'file_name': 'guide.txt'}), self.request, person, {})
        with patch('lan_bitable_template_portal.feishu_assistant_files._download', side_effect=download), patch('lan_bitable_template_portal.feishu_assistant_files._upload', side_effect=upload):
            results = await asyncio.wait_for(asyncio.gather(*(handle_files(bot, message(mid=f'c{i}', question=CONFIRM_READ), self.request, person, {}) for i, person in enumerate(people))), 5)
        self.assertEqual(maximum, 4)
        self.assertEqual(active, 0)
        self.assertTrue(all(result['file_ids'] == ['fileid'] for result in results))


if __name__ == "__main__":
    unittest.main()
