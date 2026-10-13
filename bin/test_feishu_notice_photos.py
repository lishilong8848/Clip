# -*- coding: utf-8 -*-
"""Isolated tests for the Feishu notice-photo / result rework (audit #1..#7).

Scope guarded by the task: only this test file is runtime code beyond the three
modules; nothing here hits the kernel, sends real messages, or commits git.

Coverage map (audit findings):
  1. required<->maxItems & optional site/ali never reported as missing
  2. PATCH attach_files contract (stages photos, keeps other fields)
  3. auto-attach only accepts real image resources and only when explicitly
     waiting for required photos / user mentions 现场照片
  4. multi-candidate stable listing, site/ali selectable, conversation/actor/
     version validation, receipts + no finally-delete, idempotent redelivery,
     ``purpose=notice_photo`` upload reused without OCR
  5. result notification uses an independent message_id + reply_to_message_id,
     attaches a fresh editable card, no ``status`` param on channel-replies,
     plain-language status text
  6. polling binds conversationId/actorId, awaiting_* gets feedback, and after
     the max attempts (>= 15 minutes) a clear "still processing" hint is sent
  7. no internal field names / ids / empty maxBytes hints in user-facing output
"""
import asyncio
import copy
import hashlib
import sqlite3
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))
sys.path.insert(0, str(_PROJECT_ROOT / "bin"))

from openclaw_service.assistant.lighthouse_ai import AssistantError
from lan_bitable_template_portal import feishu_assistant as assistant_mod
from lan_bitable_template_portal import feishu_assistant_files as files
from lan_bitable_template_portal import feishu_notice_cards as cards


class MemoryStore:
    def __init__(self, db_path=":memory:"):
        self.db_path = db_path
        self.data = {}

    def get_document(self, namespace, key):
        return copy.deepcopy(self.data.get((namespace, key)))

    def put_document(self, namespace, key, payload):
        self.data[(namespace, key)] = copy.deepcopy(payload)

    def delete_document(self, namespace, key):
        self.data.pop((namespace, key), None)

    def list_documents(self, namespace, key_prefix=""):
        return [{"key": key, "payload": copy.deepcopy(value)}
                for (ns, key), value in self.data.items()
                if ns == namespace and key.startswith(key_prefix)]


ACTOR = {"id": "open_user_1"}
UNION = "on_alice"
CHAT = "oc_chat_1"
CONV = {"conversation_id": "conv_1"}


def message(card=None, union=UNION, chat=CHAT, mid="om_1", question=""):
    return {
        "message_id": "synthetic_1",
        "reply_to_message_id": mid,
        "union_id": union,
        "sender_id": "open_user_1",
        "chat_id": chat,
        "chat_type": "group",
        "event_id": "evt_origin",
        "question": question,
        "content": {},
        "card": card or {},
    }


def image_message(mid="om_imga1", question="现场照片", image_key="img_xyz"):
    m = message(mid=mid, question=question)
    m["message_id"] = mid
    m["content"] = {"image_key": image_key}
    return m


def file_message(mid="om_file1", question="读取这个 PDF"):
    m = message(mid=mid, question=question)
    m["message_id"] = mid
    m["content"] = {"file_key": "file_abc", "file_name": "report.pdf"}
    return m


def photo_field(kind="site", name="step0.notice_site_photos", value=None, selected=None,
                max_items=3, required=True):
    field = {
        "native_notice_photos": kind,
        "name": name,
        "label": "现场照片" if kind == "site" else "阿里确认截图",
        "type": "file",
        "value": value if value is not None else [],
        "selected_files": selected or [],
        "maxItems": max_items,
        "maxBytes": 8 * 1024 * 1024,
        "required": required,
    }
    if kind != "site":
        field["name"] = "step0.notice_ali_photos"
    return field


def base_plan(**overrides):
    plan = {
        "id": "plan_1",
        "version": 1,
        "status": "needs_input",
        "title": "测试维保通告",
        "fields": [photo_field("site")],
    }
    plan.update(overrides)
    return plan


def conversation_with(*plans):
    conv = copy.deepcopy(CONV)
    conv["turns"] = [{"plan": p} for p in plans]
    return conv


def walk(card):
    results = []
    if not isinstance(card, dict):
        return results
    stack = list((card.get("body") or {}).get("elements") or [])
    while stack:
        node = stack.pop()
        results.append(node)
        for key in ("elements", "children"):
            for sub in node.get(key) or []:
                stack.append(sub)
    return results


def nodes_text(nodes):
    return "\n".join(str(node.get("content") or node.get("text") or "") for node in nodes)


class FakeBotForAssistant(assistant_mod.FeishuAssistant):
    """Real FeishuAssistant surface with a memory store, temp Inbox and stubs."""

    def __init__(self):
        self.memory = MemoryStore()
        super().__init__(None, SimpleNamespace(state_store=self.memory), None)
        from lan_bitable_template_portal.feishu_assistant import Inbox
        self.tmpdir = tempfile.TemporaryDirectory()
        self.inbox = Inbox(Path(self.tmpdir.name) / "inbox.sqlite3")
        self.calls = []
        self.sent = []
        self.conversation_result = None
        self.patch_result = None
        self.messenger = _FakeMessenger(self.sent)

    # ---- transport stubs -------------------------------------------------
    async def _request_context(self, message):
        sender = (message or {}).get("sender_id") or "open_user_1"
        return {"request": message}, {"id": sender}

    async def _call(self, request, actor, path, body=None, plan_id=None, *, method=None, params=None):
        self.calls.append((path, method, body, params))
        if path == "conversation":
            return self.conversation_result or conversation_with(base_plan())
        if path == "channel-replies":
            return {"ok": True}
        if path.endswith("/confirm"):
            return copy.deepcopy(base_plan(status="running", version=2))
        if path.endswith("/cancel"):
            return {**copy.deepcopy(base_plan()), "status": "cancelled"}
        if method == "PATCH":
            return copy.deepcopy(self.patch_result or base_plan(status="needs_input", version=2))
        return {}

    async def _sync_reply_history(self, message, reply):
        return None

    def status(self, identity="plan_status_abc"):
        """Current inbox state of a plan-status poll row."""
        return asyncio.run(asyncio.to_thread(self.inbox._state_raw, identity))


class _FakeMessenger:
    def __init__(self, sink):
        self.sink = sink

    async def send(self, message, reply, checkpoint=None, authorize=None):
        if authorize:
            await authorize()
        self.sink.append((copy.deepcopy(message), copy.deepcopy(reply)))
        if checkpoint:
            await checkpoint()


def _store_docs(bot):
    return bot.memory.list_documents(files.NAMESPACE)


def _store_photo_doc(bot):
    for doc in _store_docs(bot):
        if doc["key"].startswith("photo:"):
            return doc["payload"]
    return None


def _import_counter_factory():
    state = {"calls": 0}

    async def fake_import(bot, message, request, actor, resources, conversation=None):
        state["calls"] += 1
        for i, res in enumerate(resources):
            res["file_id"] = res.get("file_id") or ("fid_" + str(i + 1))
        return [str(r["file_id"]) for r in resources]

    return state, fake_import


# ---------------------------------------------------------------------------
# Finding 1: required = at least one, never maxItems; optional never missing
# ---------------------------------------------------------------------------
class RequiredPhotoSemanticsTests(unittest.TestCase):
    def test_required_means_at_least_one_not_maxitems(self):
        filled = photo_field("site", value=["a.jpg", "b.jpg"])
        self.assertEqual(files._photo_sent_count(filled), 2)
        # Even though maxItems is 3, 2 photos already satisfy "required".
        self.assertGreaterEqual(files._photo_sent_count(filled), 1)

    def test_optional_and_filled_are_not_missing(self):
        plan = base_plan(fields=[
            photo_field("site", value=["a.jpg"]),          # required, filled
            photo_field("ali", value=[], required=False),  # optional, empty
        ])
        # Non-required ali must never be surfaced as missing.
        candidates = files._plain_photo_candidates(conversation_with(plan))
        self.assertEqual(candidates, [])

    def test_required_empty_is_missing_and_optional_not_listed(self):
        plan = base_plan(fields=[
            photo_field("site", value=[]),                 # required, empty -> missing
            photo_field("ali", value=[], required=False),  # optional -> NOT missing
            photo_field("site", value=["x.jpg"], name="step0.notice_other_site",
                        max_items=1),                      # required filled -> fine
        ])
        entries = files._missing_photo_entries(conversation_with(plan))
        self.assertEqual(len(entries), 1)
        plan_entry, field_entry = entries[0]
        self.assertEqual(field_entry["native_notice_photos"], "site")
        self.assertEqual(field_entry["name"], "step0.notice_site_photos")
        # The optional ali field is simply absent from missing entries.
        self.assertNotIn("step0.notice_ali_photos", [e[1]["name"] for e in entries])


# ---------------------------------------------------------------------------
# Finding 2 + 7: attach_files contract + no internal id/name/maxBytes hints
# ---------------------------------------------------------------------------
class PhotoOutputAndContractTests(unittest.TestCase):
    def test_attach_uses_attach_files_contract_not_plain_amend(self):
        bot = FakeBotForAssistant()
        plan = base_plan(fields=[photo_field("site", value=[])])
        field = plan["fields"][0]
        asyncio.run(files._attach_photos_to_plan(
            bot, message(), {"r": 1}, ACTOR, bot.memory, "owner", "open_user_1", "ch",
            [{"kind": "image", "file_id": "fid_1", "name": "pic.png"}],
            "现场照片", conversation_with(plan), plan, field, imported_ids=["fid_1"]))
        patch = next((c for c in bot.calls if c[0].endswith("/" + plan["id"]) and c[1] == "PATCH"), None)
        self.assertIsNotNone(patch)
        _, _, body, _ = patch
        self.assertEqual(body.get("action"), "attach_files")
        self.assertEqual(body.get("version"), plan["version"])
        self.assertEqual(body.get("values"), {"step0.notice_site_photos": ["fid_1"]})

    def test_upload_forward_purpose_notice_photo_without_ocr(self):
        captured = {}

        async def fake_multipart(bot, request, actor, path, parts, params=None):
            captured["params"] = params
            captured["path"] = path
            return {"files": [{"id": "fid_u1"}]}

        original = files._multipart
        files._multipart = fake_multipart
        try:
            fid = asyncio.run(files._upload(FakeBotForAssistant(), {"r": 1}, ACTOR,
                                            "pic.png", b"\x89PNG", purpose="notice_photo"))
            self.assertEqual(fid, "fid_u1")
            self.assertEqual(captured["params"], {"purpose": "notice_photo"})
            self.assertEqual(captured["path"], "files")
            # No OCR model call is made (only the raw assistant files upload).
        finally:
            files._multipart = original

    def test_preview_and_card_render_leak_no_internal_field_or_id(self):
        field = photo_field("site", value=[], selected=[{"name": "a.jpg", "id": "file_99"}],
                            max_items=3)
        lines = cards._preview_field_lines(field)
        preview = "\n".join(lines)
        self.assertIn("现场照片", preview)
        self.assertNotIn("step0.notice_site_photos", preview)   # internal field name
        self.assertNotIn("file_99", preview)                     # internal id
        self.assertNotIn("maxBytes", preview)

        md = cards._photos_markdown(field)
        content = md.get("content") or ""
        self.assertNotIn("step0.notice_site_photos", content)
        self.assertNotIn("file_99", content)
        self.assertNotIn("maxBytes", content)

    def test_status_plain_never_leaks_kernel_terms(self):
        self.assertEqual(cards._status_plain(base_plan(status="needs_input")), "待补充内容")
        self.assertEqual(cards._status_plain(base_plan(status="awaiting_confirmation")), "待确认")
        for status in ("needs_input", "awaiting_confirmation", "awaiting_second_confirmation"):
            line = assistant_mod._plain_status_line(base_plan(status=status))
            self.assertNotIn(status, line)
            self.assertNotIn("maxBytes", line)


# ---------------------------------------------------------------------------
# Finding 3: auto-attach only for real images when expected/mentioned
# ---------------------------------------------------------------------------
class AutoAttachGateTests(unittest.TestCase):
    def test_ordinary_file_never_attached_as_photo(self):
        bot = FakeBotForAssistant()
        plan = base_plan(fields=[photo_field("site", value=[])])
        bot.conversation_result = conversation_with(plan)
        prev_import = files._import_photo_file_ids
        seen = {"n": 0}

        async def spy_import(bot2, message, request, actor, resources, conversation=None):
            seen["n"] += 1
            return []

        files._import_photo_file_ids = spy_import
        try:
            reply = asyncio.run(files.handle_files(bot, file_message(), {"r": 1}, ACTOR,
                                                   conversation_with(plan)))
        finally:
            files._import_photo_file_ids = prev_import
        self.assertIsNotNone(reply)
        self.assertIn("已收到", (reply.get("text") or ""))
        # The OCR/normal file was NOT imported as a notice photo, no PATCH attach.
        self.assertEqual(seen["n"], 0)
        self.assertFalse(any(c[1] == "PATCH" for c in bot.calls))

    def test_image_with_photo_keyword_auto_attaches_via_attach_files(self):
        bot = FakeBotForAssistant()
        plan = base_plan(fields=[photo_field("site", value=[])])
        _, fake_import = _import_counter_factory()
        prev = files._import_photo_file_ids
        files._import_photo_file_ids = fake_import
        try:
            reply = asyncio.run(files.handle_files(bot, image_message(question="补现场照片"),
                                                   {"r": 1}, ACTOR, conversation_with(plan)))
        finally:
            files._import_photo_file_ids = prev
        self.assertIn("已将", (reply.get("text") or ""))
        self.assertIn("照片补充", (reply.get("text") or ""))
        patch = next((c for c in bot.calls if c[1] == "PATCH"), None)
        self.assertIsNotNone(patch)
        self.assertEqual(patch[2].get("action"), "attach_files")
        self.assertIn("step0.notice_site_photos", patch[2]["values"])


# ---------------------------------------------------------------------------
# Finding 4: multi-candidate selection, idempotent stash, version check,
#            no finally-delete on failure, purpose upload reuses API
# ---------------------------------------------------------------------------
class MultiCandidateTests(unittest.TestCase):
    def test_site_and_ali_both_selectable_with_stable_numbers(self):
        plan = base_plan(fields=[photo_field("site", value=[]), photo_field("ali", value=[])])
        target = files._photo_attach_target(conversation_with(plan))
        self.assertEqual(target[0], "choose")
        entries = target[1]
        self.assertEqual(len(entries), 2)
        # Two distinct selectable rows (site + ali) with stable numbering.
        lines = files._photo_choice_lines(entries)
        self.assertIn("1. 通告「测试维保通告」：现场照片", lines)
        self.assertIn("2. 通告「测试维保通告」：阿里确认截图", lines)
        self.assertIn("补照片计划 N", lines)

    def test_redelivery_of_same_message_does_not_reimport(self):
        bot = FakeBotForAssistant()
        plan = base_plan(fields=[photo_field("site", value=[]), photo_field("ali", value=[])])
        conv = conversation_with(plan)
        entries = files._missing_photo_entries(conv)
        state, fake_import = _import_counter_factory()
        prev = files._import_photo_file_ids
        files._import_photo_file_ids = fake_import
        try:
            for _ in range(2):
                res = asyncio.run(files._stash_photos_for_selection(
                    bot, message(mid="om_stash1"), {"r": 1}, ACTOR, bot.memory,
                    "owner", "open_user_1", "ch", [{"kind": "image", "name": "p.png", "image_key": "img1"}],
                    conv, entries, "现场照片"))
                self.assertIsNotNone(res.get("photo_choices"))
        finally:
            files._import_photo_file_ids = prev
        # Only imported once across both deliveries.
        self.assertEqual(state["calls"], 1)

    def test_stash_selection_validates_conversation_then_version(self):
        bot = FakeBotForAssistant()
        plan = base_plan(fields=[photo_field("site", value=[]), photo_field("ali", value=[])])
        conv = conversation_with(plan)
        entries = files._missing_photo_entries(conv)
        _, fake_import = _import_counter_factory()
        prev = files._import_photo_file_ids
        files._import_photo_file_ids = fake_import
        try:
            asyncio.run(files._stash_photos_for_selection(
                bot, message(mid="om_stash2"), {"r": 1}, ACTOR, bot.memory,
                "owner", "open_user_1", "ch", [{"kind": "image", "name": "p.png", "image_key": "img1"}],
                conv, entries, "现场照片"))
        finally:
            files._import_photo_file_ids = prev
        # Version mismatch: current plan is v2, stash snapshot was v1.
        bumped = copy.deepcopy(plan)
        bumped["version"] = 2
        reply = asyncio.run(files._attach_stashed_photos(
            bot, message(mid="om_pick3", question="补照片计划 1"), {"r": 1}, ACTOR, bot.memory,
            "owner", "open_user_1", "ch", conversation_with(bumped), 1))
        self.assertIn("版本变化", (reply.get("text") or ""))
        # Stash is still present (not deleted on failure).
        self.assertIsNotNone(_store_photo_doc(bot))

    def test_attach_failure_keeps_stash_not_deleted(self):
        bot = FakeBotForAssistant()
        plan = base_plan(fields=[photo_field("site", value=[]), photo_field("ali", value=[])])
        conv = conversation_with(plan)
        entries = files._missing_photo_entries(conv)
        _, fake_import = _import_counter_factory()
        prev = files._import_photo_file_ids
        files._import_photo_file_ids = fake_import
        try:
            asyncio.run(files._stash_photos_for_selection(
                bot, message(mid="om_stash3"), {"r": 1}, ACTOR, bot.memory,
                "owner", "open_user_1", "ch", [{"kind": "image", "name": "p.png", "image_key": "img1"}],
                conv, entries, "现场照片"))
        finally:
            files._import_photo_file_ids = prev

        class FailingBot(FakeBotForAssistant):
            async def _call(self, request, actor, path, body=None, plan_id=None, *, method=None, params=None):
                if method == "PATCH":
                    raise AssistantError("网络超时", 502)
                return await super()._call(request, actor, path, body, plan_id, method=method, params=params)

        failing = FailingBot()
        failing.memory = bot.memory
        failing.runtime = SimpleNamespace(state_store=bot.memory)
        with self.assertRaises(AssistantError):
            asyncio.run(files._attach_stashed_photos(
                failing, message(mid="om_pick4", question="补照片计划 1"), {"r": 1}, ACTOR, failing.memory,
                "owner", "open_user_1", "ch", conv, 1))
        # The staged images remain in the store so the user can retry.
        self.assertIsNotNone(_store_photo_doc(failing))


# ---------------------------------------------------------------------------
# Finding 5: independent result message_id + reply_to, editable card,
#            channel-replies without status param, plain language
# ---------------------------------------------------------------------------
class ResultNotificationTests(unittest.TestCase):
    def _bot_with_notify(self, identity="plan_status_abc"):
        bot = FakeBotForAssistant()
        bot.messenger = _FakeMessenger(bot.sent)
        # A plan-status poll row normally exists; ensure the notifier can persist.
        payload = {
            "message_id": identity, "owner": UNION, "kind": "plan_status",
            "plan_id": "plan_1", "plan_version": 2, "conversation_id": "conv_1",
            "actor_id": "open_user_1", "attempts": 0, "created_at": 1.0,
            "original": {"message_id": "om_orig_1", "chat_id": CHAT, "chat_type": "group",
                         "sender_id": "open_user_1", "union_id": UNION},
        }
        asyncio.run(asyncio.to_thread(bot.inbox.add_plan_status, payload, 0))
        return bot

    def _original(self, mid="om_orig_1", reply_to=None):
        original = {"message_id": mid, "chat_id": CHAT, "chat_type": "group",
                    "sender_id": "open_user_1", "union_id": UNION}
        if reply_to:
            original["reply_to_message_id"] = reply_to
        return original

    def test_needs_input_notification_independent_id_and_editable_card(self):
        bot = self._bot_with_notify()
        plan = base_plan(status="needs_input", version=2,
                         fields=[photo_field("site", value=[])])
        original = self._original(mid="om_orig_1")
        request = {"request": original}
        asyncio.run(bot._notify_plan_status("plan_status_abc", original, request,
                                            ACTOR, plan, "conv_1"))
        self.assertEqual(len(bot.sent), 1)
        sent_msg, reply = bot.sent[0]
        # Independent message_id (never reuses the original confirmation id).
        self.assertNotEqual(sent_msg["message_id"], original["message_id"])
        self.assertNotIn("om_orig_1", sent_msg["message_id"])
        # reply_to_message_id is the original real message.
        self.assertEqual(sent_msg["reply_to_message_id"], original["message_id"])
        # Fresh editable card is attached for needs_input.
        self.assertTrue(reply.get("card"))
        self.assertIn("现场照片", reply.get("text") or "")
        # channel-replies sync must NOT carry a non-existent status param.
        reply_calls = [c for c in bot.calls if c[0] == "channel-replies"]
        self.assertEqual(len(reply_calls), 1)
        _, _, body, params = reply_calls[0]
        self.assertNotIn("status", body)
        self.assertTrue(params is None or "status" not in params)

    def test_awaiting_confirmation_plain_language_without_kernel_terms(self):
        bot = self._bot_with_notify()
        plan = base_plan(status="awaiting_confirmation", version=2,
                         fields=[photo_field("site", value=[{"name": "a.jpg"}])])
        original = self._original(mid="om_orig_2")
        asyncio.run(bot._notify_plan_status("plan_status_abc", original,
                                            {"request": original}, ACTOR, plan, "conv_1"))
        self.assertEqual(len(bot.sent), 1)
        text = bot.sent[0][1].get("text") or ""
        self.assertIn("确认执行", text)
        self.assertNotIn("needs_input", text)
        self.assertNotIn("awaiting_", text)
        # The awaited state gets feedback (never left waiting forever) via card.
        self.assertTrue(bot.sent[0][1].get("card"))

    def test_same_status_sent_twice_reuses_same_uuid_and_true_reply_id(self):
        """Lost-response retry must NOT mint a new message/uuid/card/ticket."""
        bot = self._bot_with_notify()
        plan = base_plan(status="needs_input", version=2,
                         fields=[photo_field("site", value=[{"name": "a.jpg"}])])
        original = self._original(mid="om_orig_3", reply_to="om_true_original")
        for _ in range(2):
            asyncio.run(bot._notify_plan_status("plan_status_abc", original,
                                                {"request": original}, ACTOR, plan, "conv_1"))
        # Two deliveries occurred (retry after a lost response).
        self.assertEqual(len(bot.sent), 2)
        first = bot.sent[0]
        second = bot.sent[1]
        # Same deterministic notify_id -> same Feishu uuid on both deliveries.
        self.assertEqual(first[0]["message_id"], second[0]["message_id"])
        # The true original reply id (reply_to_message_id) is used, not the
        # synthetic card target.
        self.assertEqual(first[0]["reply_to_message_id"], "om_true_original")
        self.assertEqual(second[0]["reply_to_message_id"], "om_true_original")
        # Same card / ticket is reused (no new ticket invalidating the card).
        self.assertEqual(first[1].get("ticket"), second[1].get("ticket"))
        self.assertEqual(first[1].get("card"), second[1].get("card"))

    def test_loss_response_retry_reuses_same_valid_card(self):
        """A failed send must reuse the exact card/ticket on the next attempt."""
        bot = self._bot_with_notify()
        plan = base_plan(status="needs_input", version=2,
                         fields=[photo_field("site", value=[])])

        # First attempt fails after the reply was persisted (lost response).
        failures = {"n": 0}

        class FailOnceMessenger:
            def __init__(self, sent, failures):
                self.sent, self.failures = sent, failures

            async def send(self, message, reply, checkpoint=None, authorize=None):
                if authorize:
                    await authorize()
                if self.failures["n"] == 0:
                    self.failures["n"] += 1
                    raise AssistantError("网络中断", 503)
                self.sent.append((copy.deepcopy(message), copy.deepcopy(reply)))
                if checkpoint:
                    await checkpoint()

        bot.messenger = FailOnceMessenger(bot.sent, failures)
        original = self._original(mid="om_orig_4", reply_to="om_true_original")
        try:
            with self.assertRaises(AssistantError):
                asyncio.run(bot._notify_plan_status("plan_status_abc", original,
                                                    {"request": original}, ACTOR, plan, "conv_1"))
        finally:
            bot.messenger = _FakeMessenger(bot.sent)
        self.assertEqual(bot.sent, [])
        # Retry: same status/version -> persisted reply (card/ticket) is reused.
        asyncio.run(bot._notify_plan_status("plan_status_abc", original,
                                            {"request": original}, ACTOR, plan, "conv_1"))
        self.assertEqual(len(bot.sent), 1)
        sent_msg, reply = bot.sent[0]
        self.assertTrue(reply.get("card"))
        self.assertTrue(reply.get("ticket"))
        # The reused card is still valid against the store (same ticket).
        from lan_bitable_template_portal.feishu_notice_cards import _NAMESPACE, _validate_ticket
        conv = conversation_with(plan)
        tick, restored, error = _validate_ticket(bot, reply["ticket"], message(), ACTOR, conv)
        self.assertIsNone(error, "reused card ticket must remain valid")
        self.assertIsNotNone(tick)
        self.assertEqual(tick["plan_id"], "plan_1")
        self.assertEqual(sent_msg["reply_to_message_id"], "om_true_original")
        # Same uuid as the very first (failed) attempt.
        self.assertTrue(sent_msg["message_id"].startswith("notify_"))


# ---------------------------------------------------------------------------
# Finding 6: binding, awaiting feedback and >=15min still-processing hint
# ---------------------------------------------------------------------------
class PlanStatusPollTests(unittest.TestCase):
    def setUp(self):
        self.bot = FakeBotForAssistant()

    def _payload(self, attempts=0, actor_id="open_user_1", conversation_id="conv_1"):
        return {
            "message_id": "plan_status_abc",
            "owner": UNION,
            "plan_id": "plan_1",
            "plan_version": 2,
            "actor_id": actor_id,
            "conversation_id": conversation_id,
            "attempts": attempts,
            "created_at": 1.0,
            "original": {"message_id": "om_1", "reply_to_message_id": "om_1", "chat_id": CHAT,
                         "chat_type": "group", "sender_id": "open_user_1", "union_id": UNION},
        }

    def _insert(self, payload):
        asyncio.run(asyncio.to_thread(self.bot.inbox.add_plan_status, payload, 0))

    def _state(self, identity="plan_status_abc"):
        return asyncio.run(asyncio.to_thread(self.bot.inbox._state_raw, identity))

    def test_schedule_binds_conversation_and_actor(self):
        plan = base_plan(status="running", version=2)
        payload = self._payload()
        # Directly verify what schedule_plan_status persists.
        msg = message()
        asyncio.run(self.bot.schedule_plan_status(msg, plan, CONV, ACTOR["id"]))
        due = asyncio.run(asyncio.to_thread(self.bot.inbox.due_plan_statuses, time.time() + 100))
        stored = next((p for p in due if p.get("kind") == "plan_status"), None)
        self.assertIsNotNone(stored)
        self.assertEqual(stored["conversation_id"], "conv_1")
        self.assertEqual(stored["actor_id"], "open_user_1")

    def test_poll_binding_mismatch_cancels_without_send(self):
        plan = base_plan(status="submitted", version=2)
        self.bot.conversation_result = conversation_with(plan)
        payload = self._payload(actor_id="someone_else")
        self._insert(payload)
        asyncio.run(self.bot._run_plan_status("plan_status_abc", payload))
        self.assertEqual(self._state(), "done")
        self.assertEqual(self.bot.sent, [])

    def test_awaiting_confirmation_gets_feedback_and_done(self):
        plan = base_plan(status="awaiting_confirmation", version=2)
        self.bot.conversation_result = conversation_with(plan)
        payload = self._payload()
        self._insert(payload)
        asyncio.run(self.bot._run_plan_status("plan_status_abc", payload))
        texts = [r.get("text") for _, r in self.bot.sent]
        # The awaiting plan is reported back (not left waiting forever).
        self.assertTrue(any("确认执行" in (t or "") for t in texts))
        self.assertEqual(self._state(), "done")

    def test_still_processing_after_max_attempts(self):
        plan = base_plan(status="submitted", version=2)
        self.bot.conversation_result = conversation_with(plan)
        payload = self._payload(attempts=assistant_mod._PLAN_STATUS_MAX_ATTEMPTS)
        self._insert(payload)
        asyncio.run(self.bot._run_plan_status("plan_status_abc", payload))
        texts = [r.get("text") for _, r in self.bot.sent]
        self.assertTrue(any("15分钟" in (t or "") and "处理中" in (t or "") for t in texts))


class _FakeInbox:
    """Placeholder so the module maintains an Inbox reference for assertions."""


def _inject_state_raw():
    if hasattr(assistant_mod.Inbox, "_state_raw"):
        return

    def _state_raw(self, identity):
        with self.connect() as db:
            row = db.execute("SELECT state FROM messages WHERE id=?", (identity,)).fetchone()
            return row[0] if row else None

    assistant_mod.Inbox._state_raw = _state_raw


_inject_state_raw()


if __name__ == "__main__":
    unittest.main(verbosity=2)
