# -*- coding: utf-8 -*-
"""Strict async tests for the Feishu notice card rework.

Every card interaction goes through the *real* async surface:
    await prepare_card(...)
    await handle_card(...)
and the portal API is represented by an ``async def _call`` (AsyncMock-style) so
the tests exercise the genuine ``await bot._call`` path instead of a sync Mock.
"""
import asyncio
import copy
import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

# Make both the project root and `bin/` importable so `openclaw_service` and
# the `lan_bitable_template_portal` package resolve.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))
sys.path.insert(0, str(_PROJECT_ROOT / "bin"))

from lan_bitable_template_portal import feishu_notice_cards as cards


class MemoryStore:
    """Minimal stand-in for LanPortalStateStore (document API only)."""

    def __init__(self):
        self.data = {}

    def get_document(self, namespace, key):
        return copy.deepcopy(self.data.get((namespace, key)))

    def put_document(self, namespace, key, payload):
        self.data[(namespace, key)] = copy.deepcopy(payload)

    def delete_document(self, namespace, key):
        self.data.pop((namespace, key), None)

    def list_documents(self, namespace, key_prefix=""):
        return [{"key": key, "payload": copy.deepcopy(value)} for (ns, key), value in self.data.items()
                if ns == namespace and key.startswith(key_prefix)]


class FakeBot:
    """Async portal client whose ``_call`` is a real coroutine function."""

    def __init__(self, store=None):
        self.runtime = SimpleNamespace(state_store=store or MemoryStore())
        self.calls = []
        self.store = self.runtime.state_store
        # High-level result templates that mimic the portal plans/{id} API.
        self.plan = None              # the plan the test believes is current
        self.save_override = None     # optional result for a save PATCH
        self.options_result = None    # result for plans/{id}/options
        self.tracked = []

    async def schedule_plan_status(self, message, plan, conversation, actor_id):
        self.tracked.append((message, plan, conversation, actor_id))

    async def _call(self, request, actor, path, body=None, plan_id=None, *, method=None, params=None):
        self.calls.append({
            "path": path, "method": method, "body": body,
            "params": params, "plan_id": plan_id,
        })
        base = copy.deepcopy(self.plan) or {}
        if path.endswith("/confirm"):
            stage = (body or {}).get("stage")
            if stage == "review":
                return {**base, "version": (base.get("version") or 0) + 1,
                        "status": "awaiting_second_confirmation"}
            return {**base, "version": (base.get("version") or 0) + 1,
                    "status": "running", "results": [{"ok": True}]}
        if path.endswith("/options"):
            return self.options_result or {"fields": []}
        if path.endswith("/cancel"):
            return {**base, "status": "cancelled"}
        if method == "PATCH":
            if (body or {}).get("action") == "edit":
                return {**base, "status": "needs_input"}
            if self.save_override is not None:
                return self.save_override
            return {**base, "version": (base.get("version") or 0) + 1,
                    "status": "awaiting_confirmation"}
        return {}


ACTOR = {"id": "open_user_1"}
UNION = "on_alice"
CHAT = "oc_chat_1"
CONV = {"conversation_id": "conv_1"}


def message(card=None, union=UNION, chat=CHAT, mid="om_1"):
    return {
        "message_id": "synthetic_1",
        "reply_to_message_id": mid,
        "union_id": union,
        "chat_id": chat,
        "chat_type": "group",
        "event_id": "evt_origin",
        "card": card or {},
    }


def conversation_with(plan):
    conv = copy.deepcopy(CONV)
    conv.setdefault("turns", []).append({"plan": plan})
    return conv


def notice_field(name="notice", label="通告字段", children=None, value=None, **markers):
    field = {
        "native_notice": True,
        "name": name,
        "label": label,
        "value": value or {},
        "children": children or [],
    }
    field.update(markers)
    return field


def child(path, ftype="text", label=None, value="", options=None, **markers):
    c = {
        "path": path,
        "type": ftype,
        "label": label or path,
        "value": value,
        "options": options or [],
    }
    c.update(markers)
    return c


def sop_field(name="sop", label="SOP/人员选择", loaded=True, **markers):
    field = {
        "native_notice_sop": True,
        "name": name,
        "label": label,
        "sops": [{"id": "s1", "title": "常规SOP"}] if loaded else [],
        "people": [{"id": "ou_a", "name": "甲"}, {"id": "ou_b", "name": "乙"}] if loaded else [],
        "scopes": [{"code": "A1", "name": "A1栋"}, {"code": "B2", "name": "B2栋"}] if loaded else [],
        "directory_scope": "A1",
        "value": {
            "exempt": "否", "scope": "", "sop_id": "", "operator_record_id": "",
            "reviewer_record_id": "", "runs": [], "mode": "normal",
        },
    }
    field.update(markers)
    return field


def base_plan(**overrides):
    fields = [
        notice_field(
            name="notice",
            label="通告字段",
            value={"building": "A1", "job_text": "", "when": "2026-10-12T08:00", "tags": ["x"]},
            children=[
                child("building", "select", label="楼栋", value="A1",
                      options=[{"value": "A1", "label": "A1栋"}, {"value": "B2", "label": "B2栋"}]),
                child("job_text", "text", label="工作内容", value=""),
                child("when", "datetime-local", label="时间", value="2026-10-12T08:00"),
                child("tags", "multiselect", label="标签", value=["x"],
                      options=[{"value": "x", "label": "X"}, {"value": "y", "label": "Y"}]),
            ],
        ),
    ]
    plan = {
        "id": "plan_1",
        "version": 1,
        "status": "needs_input",
        "title": "测试维保通知",
        "fields": fields,
        "planned_notice": {
            "text": "【维保通告】A1栋空调检修\n具体安排见卡片。",
            "stage": "待处理",
            "scope": "A1",
            "month": "2026-10",
            "selected": {"title": "t", "building": "A1", "work_type": "w", "progress": "p"},
            "missing_fields": [],
            "field_sources": [],
            "recommendations": [],
        },
    }
    plan.update(overrides)
    return plan


def leaf(field, leaf_key):
    return cards._leaf_id("nnot", field, leaf_key)


def find_components(card):
    out = []
    for element in (card.get("body") or {}).get("elements") or []:
        if element.get("tag") == "form":
            for sub in element.get("elements") or []:
                if sub.get("tag") in {"input", "select_static", "multi_select_static",
                                      "picker_datetime", "date_picker", "picker_time", "button",
                                      "markdown"}:
                    out.append(sub)
        if element.get("tag") in {"button", "markdown"}:
            out.append(element)
    return out


def walk(card):
    """Flatten every deeply nested element for assertions."""
    results = []
    stack = list((card.get("body") or {}).get("elements") or [])
    while stack:
        node = stack.pop()
        results.append(node)
        for key in ("elements", "children"):
            for sub in node.get(key) or []:
                stack.append(sub)
    return results


def run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


class CardReworkAsyncTest(unittest.IsolatedAsyncioTestCase):

    async def test_01_async_submit_schema_and_real_await(self):
        """Defect #1/#2: prepare/handle are async; submit uses 2.0 schema."""
        plan = base_plan()
        bot = FakeBot()
        bot.plan = plan
        conv = conversation_with(plan)

        prep = await cards.prepare_card(bot, message(), ACTOR, conv, plan)
        ticket = prep["ticket"]
        self.assertTrue(isinstance(prep["card"], dict))
        self.assertEqual((prep["card"] or {}).get("schema"), "2.0")

        # The submitted form button must be schema 2.0 (form_action_type).
        all_nodes = walk(prep["card"])
        submit = next((n for n in all_nodes
                       if n.get("tag") == "button" and n.get("form_action_type") == "submit"), None)
        self.assertIsNotNone(submit, "missing form_action_type=submit button")
        self.assertEqual(submit["behaviors"][0]["type"], "callback")
        self.assertEqual(submit["behaviors"][0]["value"]["kind"], "notice_form")
        self.assertEqual(submit["behaviors"][0]["value"]["action"], "save")

        # Save via the real async handle_card; assert await worked (dict result).
        notice = plan["fields"][0]
        form_value = {
            leaf(notice, "building"): "B2",
            leaf(notice, "job_text"): "检修主机",
        }
        card_msg = message(card={"kind": "notice_form", "action": "save",
                                 "ticket": ticket, "form_value": form_value})
        reply = await cards.handle_card(bot, card_msg, None, ACTOR, conv)
        self.assertEqual(reply["status"], "saved")
        self.assertIsInstance(reply["plan"], dict, "await bot._call must return a dict")
        patch = next(c["body"] for c in bot.calls if c["method"] == "PATCH")
        self.assertEqual(patch["values"]["notice"]["building"], "B2")
        self.assertEqual(patch["values"]["notice"]["job_text"], "检修主机")

    async def test_02_select_initial_option_is_value_not_label(self):
        """Defect #2: select initial_option must be the option VALUE."""
        plan = base_plan()
        bot = FakeBot()
        prep = await cards.prepare_card(bot, message(), ACTOR, conversation_with(plan), plan)
        nodes = walk(prep["card"])
        select = next(n for n in nodes if n.get("tag") == "select_static")
        # A1 is the value; the label is "A1栋".
        self.assertEqual(select["initial_option"], "A1")
        values = [o["value"] for o in select["options"]]
        self.assertIn("A1", values)
        self.assertNotIn("A1栋", values)

    async def test_03_multiselect_is_array_and_datetime_is_picker(self):
        """Defect #2/#12: multi_select_static uses an array; datetime uses picker_datetime."""
        plan = base_plan()
        bot = FakeBot()
        prep = await cards.prepare_card(bot, message(), ACTOR, conversation_with(plan), plan)
        nodes = walk(prep["card"])
        multi = next(n for n in nodes if n.get("tag") == "multi_select_static")
        self.assertIsInstance(multi["selected_values"], list)
        self.assertEqual(multi["selected_values"], ["x"])
        picker = next(n for n in nodes if n.get("tag") == "picker_datetime")
        self.assertEqual(picker["initial_datetime"], "2026-10-12 08:00")

    async def test_04_strict_unknown_form_key_rejected(self):
        """Defect #3: unknown form keys are rejected (no silent fallback)."""
        plan = base_plan()
        bot = FakeBot()
        prep = await cards.prepare_card(bot, message(), ACTOR, conversation_with(plan), plan)
        form_value = {"totally_unknown_key": "x"}
        card_msg = message(card={"kind": "notice_form", "action": "save",
                                 "ticket": prep["ticket"], "form_value": form_value})
        reply = await cards.handle_card(bot, card_msg, None, ACTOR,
                                        conversation_with(plan))
        self.assertIn("未知字段", reply["text"])
        self.assertTrue(reply.get("error"))
        self.assertFalse(any(c["method"] == "PATCH" for c in bot.calls))

    async def test_05_readonly_parent_rejected_on_decode(self):
        """Defect #3: parent read_only makes the leaf non-editable on decode too."""
        plan = base_plan()
        # Mark the whole native notice parent read-only.
        plan["fields"][0]["read_only"] = True
        bot = FakeBot()
        prep = await cards.prepare_card(bot, message(), ACTOR, conversation_with(plan), plan)
        notice = plan["fields"][0]
        form_value = {leaf(notice, "job_text"): "不应被写入"}
        card_msg = message(card={"kind": "notice_form", "action": "save",
                                 "ticket": prep["ticket"], "form_value": form_value})
        reply = await cards.handle_card(bot, card_msg, None, ACTOR,
                                        conversation_with(plan))
        self.assertTrue(reply.get("error"))
        self.assertFalse(any(c["method"] == "PATCH" for c in bot.calls))

    async def test_06_empty_string_clears_field_not_dropped(self):
        """Defect #3: user clearing a filled field must send '' (not be ignored)."""
        plan = base_plan()
        bot = FakeBot()
        prep = await cards.prepare_card(bot, message(), ACTOR, conversation_with(plan), plan)
        notice = plan["fields"][0]
        form_value = {leaf(notice, "building"): ""}
        card_msg = message(card={"kind": "notice_form", "action": "save",
                                 "ticket": prep["ticket"], "form_value": form_value})
        reply = await cards.handle_card(bot, card_msg, None, ACTOR,
                                        conversation_with(plan))
        self.assertEqual(reply["status"], "saved")
        patch = next(c["body"] for c in bot.calls if c["method"] == "PATCH")
        self.assertIn("building", patch["values"]["notice"])
        self.assertEqual(patch["values"]["notice"]["building"], "")

    async def test_07_datetime_timezone_and_multiselect_decode(self):
        """Defect #12: picker timezone suffix is dropped; multiselect stays an array."""
        plan = base_plan()
        bot = FakeBot()
        prep = await cards.prepare_card(bot, message(), ACTOR, conversation_with(plan), plan)
        notice = plan["fields"][0]
        form_value = {
            leaf(notice, "when"): "2026-10-12 08:00 +08:00",
            leaf(notice, "tags"): ["y"],
        }
        card_msg = message(card={"kind": "notice_form", "action": "save",
                                 "ticket": prep["ticket"], "form_value": form_value})
        reply = await cards.handle_card(bot, card_msg, None, ACTOR,
                                        conversation_with(plan))
        self.assertEqual(reply["status"], "saved")
        patch = next(c["body"] for c in bot.calls if c["method"] == "PATCH")
        # The picker's timezone suffix is dropped and the value normalized.
        self.assertNotIn("+08:00", patch["values"]["notice"]["when"])
        self.assertIn("2026-10-12T08:00", patch["values"]["notice"]["when"])
        self.assertEqual(patch["values"]["notice"]["tags"], ["y"])

    async def test_08_ticket_persisted_in_state_store(self):
        """Defect #4: tickets live in bot.runtime.state_store, not a global dict."""
        plan = base_plan()
        bot = FakeBot()
        prep = await cards.prepare_card(bot, message(), ACTOR, conversation_with(plan), plan)
        doc = bot.store.get_document(cards._NAMESPACE, prep["ticket"])
        self.assertIsInstance(doc, dict)
        self.assertEqual(doc["plan_id"], "plan_1")
        self.assertEqual(doc["plan_version"], 1)
        self.assertEqual(doc["owner_open_id"], ACTOR["id"])
        self.assertEqual(doc["owner_union"], UNION)
        self.assertEqual(doc["chat_id"], CHAT)
        self.assertEqual(doc["conversation_id"], "conv_1")

    async def test_09_old_version_is_rejected(self):
        """Defect #4: a ticket minted against version N fails after version changes."""
        plan = base_plan(version=1)
        bot = FakeBot()
        prep = await cards.prepare_card(bot, message(), ACTOR, conversation_with(plan), plan)
        # The plan in conversation advanced to v5 before the user clicks the old card.
        newer = base_plan(version=5)
        card_msg = message(card={"kind": "notice_card", "action": "confirm",
                                 "ticket": prep["ticket"], "value": {}})
        reply = await cards.handle_card(bot, card_msg, None, ACTOR,
                                        conversation_with(newer))
        self.assertIn("版本", reply["text"])
        self.assertFalse(any(c["method"] == "POST" for c in bot.calls if "confirm" in c["path"]))

    async def test_10_new_preview_mints_new_ticket_and_old_is_consumed(self):
        """Defect #4: each successful save mints a distinct ticket and retires the old one."""
        plan = base_plan()
        bot = FakeBot()
        bot.plan = plan
        conv = conversation_with(plan)
        prep = await cards.prepare_card(bot, message(), ACTOR, conv, plan)
        old_ticket = prep["ticket"]
        notice = plan["fields"][0]
        form_value = {leaf(notice, "job_text"): "填好了"}
        card_msg = message(card={"kind": "notice_form", "action": "save",
                                 "ticket": old_ticket, "form_value": form_value})
        reply = await cards.handle_card(bot, card_msg, None, ACTOR, conv)
        new_ticket = reply["ticket"]
        self.assertNotEqual(new_ticket, old_ticket)
        # The old ticket is retired (consumed) from the durable store.
        old_doc = bot.store.get_document(cards._NAMESPACE, old_ticket)
        self.assertTrue(old_doc is None or old_doc.get("consumed"),
                        "old ticket should be retired")
        # The old ticket must no longer be usable for a second action.
        again = message(card={"kind": "notice_card", "action": "confirm",
                              "ticket": old_ticket, "value": {}})
        reply2 = await cards.handle_card(bot, again, None, ACTOR, conv)
        self.assertIn("失效", reply2["text"])

    async def test_11_second_confirm_promotes_and_retires_old(self):
        """Defects #9/#12: awaiting_confirmation -> review promotes; old consumed; dedup."""
        plan = base_plan(status="awaiting_confirmation")
        bot = FakeBot()
        bot.plan = plan
        conv = conversation_with(plan)
        prep = await cards.prepare_card(bot, message(), ACTOR, conv, plan)
        ticket = prep["ticket"]
        card_msg = message(card={"kind": "notice_card", "action": "confirm",
                                 "ticket": ticket, "value": {}})
        reply = await cards.handle_card(bot, card_msg, None, ACTOR, conv)
        self.assertEqual(reply["status"], "promoted")
        self.assertTrue(reply["ticket"])
        # Old ticket retired -> repeated click is rejected (no re-submit).
        confirm_posts = lambda: [c for c in bot.calls if c["method"] == "POST" and "confirm" in c["path"]]
        before = len(confirm_posts())
        reply2 = await cards.handle_card(bot, card_msg, None, ACTOR, conv)
        self.assertTrue(reply2.get("error"), "repeated click on retired ticket must error")
        self.assertEqual(len(confirm_posts()), before, "duplicate confirm must not re-submit")
        # A fresh preview mints another ticket for the actual execute stage.
        promoted = reply["plan"]
        bot.plan = promoted
        conv2 = conversation_with(promoted)
        prep2 = await cards.prepare_card(bot, message(), ACTOR, conv2, promoted)
        exec_msg = message(card={"kind": "notice_card", "action": "confirm",
                                 "ticket": prep2["ticket"], "value": {}})
        reply3 = await cards.handle_card(bot, exec_msg, None, ACTOR, conv2)
        self.assertEqual(reply3["status"], "confirmed")
        self.assertTrue(
            any(c["method"] == "POST" and "confirm" in c["path"] and (c["body"] or {}).get("stage") == "execute"
                for c in bot.calls))

    async def test_12_different_operator_rejected(self):
        """Defect #8/#12: a different operator's union cannot act on someone's card."""
        plan = base_plan(status="awaiting_confirmation")
        bot = FakeBot()
        bot.plan = plan
        conv = conversation_with(plan)
        prep = await cards.prepare_card(bot, message(), ACTOR, conv, plan)
        intruder = message(union="on_mallory")
        card_msg = message(card={"kind": "notice_card", "action": "confirm",
                                 "ticket": prep["ticket"], "value": {}})
        # Build a card message attributed to the intruder union id.
        card_msg["union_id"] = "on_mallory"
        reply = await cards.handle_card(bot, card_msg, None, ACTOR, conv)
        self.assertIn("身份", reply["text"])
        self.assertFalse(any(c["method"] == "POST" for c in bot.calls))

    async def test_13_missing_required_keeps_editable_card(self):
        """Defect #7: save with missing required still returns an editable card (no confirm)."""
        plan = base_plan()
        bot = FakeBot()
        bot.plan = plan
        bot.save_override = {**base_plan(), "version": 1, "status": "needs_input",
                             "missing_fields": ["notice.job_text"]}
        conv = conversation_with(plan)
        prep = await cards.prepare_card(bot, message(), ACTOR, conv, plan)
        notice = plan["fields"][0]
        form_value = {leaf(notice, "job_text"): ""}
        card_msg = message(card={"kind": "notice_form", "action": "save",
                                 "ticket": prep["ticket"], "form_value": form_value})
        reply = await cards.handle_card(bot, card_msg, None, ACTOR, conv)
        self.assertEqual(reply["status"], "needs_more")
        self.assertTrue(reply.get("card"))
        # The re-rendered card is editable (not a confirm preview).
        self.assertTrue(any(n.get("tag") == "form" for n in walk(reply["card"])))

    async def test_14_directory_missing_renders_load_button_and_no_empty_select(self):
        """Defect #8: missing SOP directory renders a load button, never an empty select."""
        plan = base_plan()
        plan["fields"].append(sop_field(loaded=False))
        bot = FakeBot()
        prep = await cards.prepare_card(bot, message(), ACTOR, conversation_with(plan), plan)
        nodes = walk(prep["card"])
        load_btns = [n for n in nodes
                     if n.get("tag") == "button" and n.get("text", {}).get("content") == "读取工单及人员"]
        self.assertTrue(load_btns, "expected a '读取工单及人员' button when directory is missing")
        select_options = [(n, n.get("options") or []) for n in nodes if n.get("tag") == "select_static"]
        for _, opts in select_options:
            self.assertTrue(opts, "must not render a select with empty options")

    async def test_15_non_confirmable_hides_execute(self):
        """Defect #7/#9: only awaiting statuses expose confirm; edit/cancel present."""
        awaiting = base_plan(status="awaiting_confirmation")
        bot = FakeBot()
        prep = await cards.prepare_card(bot, message(), ACTOR, conversation_with(awaiting), awaiting)
        preview = cards.build_preview_card(awaiting, prep["ticket"], just_saved=False)
        nodes = walk(preview)
        confirm = [n for n in nodes if n.get("tag") == "button" and
                   (n.get("behaviors") or [{}])[0].get("value", {}).get("action") == "confirm"]
        self.assertTrue(confirm, "awaiting_confirmation should show confirm")
        edit = [n for n in nodes if (n.get("behaviors") or [{}])[0].get("value", {}).get("action") == "edit"]
        cancel = [n for n in nodes if (n.get("behaviors") or [{}])[0].get("value", {}).get("action") == "cancel"]
        self.assertTrue(edit and cancel)

        # A non-confirmable status (e.g. still running after execute) hides confirm.
        running = base_plan(status="running")
        preview_r = cards.build_preview_card(running, prep["ticket"], just_saved=False)
        nodes_r = walk(preview_r)
        confirm_r = [n for n in nodes_r if n.get("tag") == "button" and
                     (n.get("behaviors") or [{}])[0].get("value", {}).get("action") == "confirm"]
        self.assertFalse(confirm_r, "non-confirmable status must not show confirm")

    async def test_16_recommendations_adopt_only_merges_blank(self):
        """Defect #10: adopt merges only into currently-blank writable native fields, no submit."""
        plan = base_plan()
        plan["planned_notice"]["recommendations"] = [{
            "source_building": "B2",
            "source_title": "B2上次方案",
            "suggested_values": {"job_text": "B2 历史工作内容", "building": "B2"},
        }]
        bot = FakeBot()
        bot.plan = plan
        prep = await cards.prepare_card(bot, message(), ACTOR, conversation_with(plan), plan)
        nodes = walk(prep["card"])
        adopt = [n for n in nodes if n.get("tag") == "button" and
                 (n.get("behaviors") or [{}])[0].get("value", {}).get("action") == "adopt"]
        self.assertTrue(adopt, "recommendations should render adopt buttons")

        rec_val = (adopt[0]["behaviors"][0]["value"])
        card_msg = message(card={"kind": "notice_card", "action": "adopt",
                                 "ticket": prep["ticket"], "value": rec_val})
        reply = await cards.handle_card(bot, card_msg, None, ACTOR,
                                        conversation_with(plan))
        self.assertEqual(reply["status"], "adopted")
        # No business submit happened.
        self.assertFalse(any(c["method"] == "POST" for c in bot.calls))
        # Adopt only fills blank writable leaves; existing non-blank values are untouched.
        editable = reply["plan"]
        notice = editable["fields"][0]
        # job_text was blank -> now filled; building was A1 (filled) -> untouched.
        self.assertEqual(notice["value"].get("job_text"), "B2 历史工作内容")
        self.assertEqual(notice["value"].get("building"), "A1")

    async def test_17_preview_hides_internal_ids_and_shows_text(self):
        """Defect #11: preview shows planned_notice.text + labels, never raw internal IDs."""
        plan = base_plan()
        bot = FakeBot()
        prep = await cards.prepare_card(bot, message(), ACTOR, conversation_with(plan), plan)
        preview = cards.build_preview_card(plan, prep["ticket"], just_saved=True)
        text = json.dumps(preview, ensure_ascii=False)
        self.assertIn("维保通告", text)  # planned_notice.text surfaced
        self.assertNotIn("operations", text)  # no raw internal ops payload leaked
        self.assertNotIn("$query", text)

    async def test_18_edit_and_cancel_actions_and_consume(self):
        """Defect #9: real edit/cancel actions return; consumed ticket is inert."""
        plan = base_plan(status="awaiting_confirmation")
        bot = FakeBot()
        bot.plan = plan
        conv = conversation_with(plan)

        # Edit returns an editable card and retires the old ticket.
        prep = await cards.prepare_card(bot, message(), ACTOR, conv, plan)
        edit_msg = message(card={"kind": "notice_card", "action": "edit",
                                 "ticket": prep["ticket"], "value": {}})
        reply = await cards.handle_card(bot, edit_msg, None, ACTOR, conv)
        self.assertEqual(reply["status"], "editable")
        self.assertTrue(any(n.get("tag") == "form" for n in walk(reply["card"])))
        self.assertTrue(bot.store.get_document(cards._NAMESPACE, prep["ticket"]) is None)

        # Cancel retires the ticket and confirms intent.
        prep2 = await cards.prepare_card(bot, message(), ACTOR, conv, plan)
        cancel_msg = message(card={"kind": "notice_card", "action": "cancel",
                                   "ticket": prep2["ticket"], "value": {}})
        reply2 = await cards.handle_card(bot, cancel_msg, None, ACTOR, conv)
        self.assertEqual(reply2["status"], "cancelled")
        self.assertTrue(bot.store.get_document(cards._NAMESPACE, prep2["ticket"]) is None)

    async def test_19_parse_card_event_validates_and_keeps_reply(self):
        """Defect #5: parse validates kind/ticket/action/shapes/union; keeps reply id."""
        payload = {
            "header": {"app_id": "app_x", "event_type": "card.action.trigger", "event_id": "evt_1234"},
            "event": {
                "operator": {"open_id": "ou_cafebabe", "union_id": "on_cafebabe", "user_id": "u1"},
                "token": "tok",
                "action": {
                    "name": "save", "tag": "button",
                    "value": {"kind": "notice_form", "ticket": "abcdef123456_" + "x" * 22, "action": "save"},
                    "form_value": {"f1": "v"},
                    "timezone": "Asia/Shanghai",
                },
                "context": {"open_message_id": "om_cafebabe", "open_chat_id": "oc_cafebabe"},
                "host": "im_message",
            },
        }
        msg = cards.parse_card_event(payload, "app_x")
        self.assertIsNotNone(msg)
        self.assertTrue(msg["message_id"].startswith("crd_"))
        self.assertEqual(msg["reply_to_message_id"], "om_cafebabe")
        self.assertEqual(msg["union_id"], "on_cafebabe")
        # Deterministic synthetic id (dedup) across identical clicks.
        self.assertEqual(msg["message_id"], cards.parse_card_event(payload, "app_x")["message_id"])

        # Bad kind / ticket / action / shape are all rejected.
        bad1 = copy.deepcopy(payload)
        bad1["event"]["action"]["value"]["kind"] = "evil"
        self.assertIsNone(cards.parse_card_event(bad1, "app_x"))
        bad2 = copy.deepcopy(payload)
        bad2["event"]["action"]["value"]["ticket"] = ""
        self.assertIsNone(cards.parse_card_event(bad2, "app_x"))
        bad3 = copy.deepcopy(payload)
        bad3["event"]["action"]["value"]["action"] = "hack"
        self.assertIsNone(cards.parse_card_event(bad3, "app_x"))
        bad4 = copy.deepcopy(payload)
        bad4["event"]["operator"]["union_id"] = ""
        self.assertIsNone(cards.parse_card_event(bad4, "app_x"))
        bad5 = copy.deepcopy(payload)
        bad5["event"]["action"]["form_value"] = "notadict"
        self.assertIsNone(cards.parse_card_event(bad5, "app_x"))
        bad6 = copy.deepcopy(payload)
        bad6["event"]["context"]["open_message_id"] = "bad_id"
        self.assertIsNone(cards.parse_card_event(bad6, "app_x"))

    async def test_20_load_directory_merges_and_renders(self):
        """Defect #8: '读取工单及人员' reuses the options API and re-renders selects."""
        plan = base_plan()
        plan["fields"].append(sop_field(loaded=False))
        bot = FakeBot()
        bot.plan = plan
        bot.options_result = {"fields": [sop_field(name="sop", label="SOP/人员选择", loaded=True)]}
        prep = await cards.prepare_card(bot, message(), ACTOR, conversation_with(plan), plan)
        field_name = plan["fields"][1]["name"]
        card_msg = message(card={"kind": "notice_card", "action": "load-directory",
                                 "ticket": prep["ticket"],
                                 "value": {"field": field_name}})
        reply = await cards.handle_card(bot, card_msg, None, ACTOR,
                                        conversation_with(plan))
        self.assertEqual(reply["status"], "loaded")
        # Now the directory is loaded so the SOP field renders select options.
        nodes = walk(reply["card"])
        selects = [n for n in nodes if n.get("tag") == "select_static" and n.get("options")]
        self.assertTrue(selects, "selects should render after directory is loaded")

    async def test_generic_nested_fields_preserve_native_shape_and_frozen_values(self):
        from openclaw_service.assistant.lighthouse_agent import _editable_patch
        field = {'name': 'step0.fields', 'type': 'object', 'label': '维修填写',
                 'children': [child('note', 'textarea'), child('unchanged', read_only=True),
                              child('details', 'object', children=[child('count', 'number'), child('literal.key')]),
                              child('rows', 'array', item={'type': 'object', 'children': [child('value')]}),
                              child('signer', 'select', person_picker=True, options=[
                                  {'value': 'recPerson', 'label': '测试操作人', 'person': {'record_id': 'recPerson', 'name': '测试操作人'}}])],
                 'value': {'note': '旧备注', 'unchanged': '不能修改', 'details': {'count': 1, 'literal.key': 'old'},
                           'rows': [{'value': 'one'}], 'signer': {}}}
        plan = base_plan(fields=[field])
        bot = FakeBot(); bot.plan = plan
        prepared = await cards.prepare_card(bot, message(), ACTOR, conversation_with(plan), plan)
        nodes = walk(prepared['card'])
        self.assertTrue(any(node.get('name') == cards._generic_id(field, ('details', 'count')) for node in nodes))
        values = cards._build_values([field], {
            cards._generic_id(field, ('note',)): '新备注',
            cards._generic_id(field, ('details', 'count')): '2',
            cards._generic_id(field, ('details', 'literal.key')): 'new',
            cards._generic_id(field, ('rows', 0, 'value')): 'two',
            cards._generic_id(field, ('signer',)): 'recPerson',
        })[field['name']]
        self.assertEqual(_editable_patch(field, field['value'], values), values)
        self.assertEqual(values['details'], {'count': 2, 'literal.key': 'new'})
        self.assertEqual(values['rows'], [{'value': 'two'}])
        self.assertEqual(values['unchanged'], '不能修改')
        self.assertEqual(values['signer']['name'], '测试操作人')
        with self.assertRaises(cards.AssistantError):
            cards._build_values([field], {cards._generic_id(field, ('unchanged',)): 'forged'})

    async def test_adopt_and_directory_keep_partial_inputs_without_execution(self):
        plan = base_plan()
        field = plan['fields'][0]
        plan['fields'].append(sop_field(loaded=False))
        plan['planned_notice']['recommendations'] = [{'source_building': 'B', 'suggested_values': {'job_text': 'B推荐'}}]
        bot = FakeBot(); bot.plan = plan
        prepared = await cards.prepare_card(bot, message(), ACTOR, conversation_with(plan), plan)
        callback = message(card={'kind': 'notice_form', 'action': 'adopt', 'ticket': prepared['ticket'],
                                'value': {'rec_index': 0}, 'form_value': {
                                    leaf(field, 'job_text'): '我正在填写的内容',
                                    cards._leaf_id('sop', plan['fields'][1], 'exempt'): '否'}})
        adopted = await cards.handle_card(bot, callback, None, ACTOR, conversation_with(plan))
        self.assertEqual(adopted['status'], 'adopted')
        self.assertEqual(adopted['plan']['fields'][0]['value']['job_text'], '我正在填写的内容')
        self.assertEqual(bot.calls, [])
        # The immutable ticket retains the displayed draft even before PATCH.
        _, restored, error = cards._validate_ticket(bot, adopted['ticket'], message(), ACTOR, conversation_with(plan))
        self.assertIsNone(error)
        self.assertEqual(restored['fields'][0]['value']['job_text'], '我正在填写的内容')
        bot.options_result = {**copy.deepcopy(plan), 'version': 2}
        bot.options_result['fields'][1] = sop_field(loaded=True)
        loaded = await cards.handle_card(bot, message(card={'kind': 'notice_form', 'action': 'load-directory',
            'ticket': adopted['ticket'], 'value': {'field': 'sop'}, 'form_value': {}}), None, ACTOR, conversation_with(plan))
        self.assertEqual(loaded['plan']['version'], 2)
        self.assertEqual(loaded['plan']['fields'][0]['value']['job_text'], '我正在填写的内容')
        self.assertTrue(all(call['path'].endswith('/options') for call in bot.calls))

    async def test_timezone_conversion_and_generic_confirmation_preview(self):
        self.assertEqual(cards._coerce_card_string('2026-10-12 08:30 -0500'), '2026-10-12T21:30')
        self.assertEqual(cards._coerce_card_string('2026-10-12 08:30 +0800'), '2026-10-12T08:30')
        card = cards.build_preview_card(base_plan(fields=[], status='awaiting_confirmation', operations=[
            {'name': '更新任务', 'body': {'description': '更换过滤器'}, 'selected_labels': {'recipient_names': '测试人员'}}]), 'ticket')
        rendered = json.dumps(card, ensure_ascii=False)
        self.assertIn('更换过滤器', rendered)
        self.assertIn('测试人员', rendered)
        large = base_plan(status='awaiting_confirmation')
        large['planned_notice']['text'] = '长通告' * 10000
        preview = cards.build_preview_card(large, 'ticket')
        self.assertFalse(any(node.get('name') == 'confirm' for node in walk(preview)))
        self.assertLess(len(json.dumps(preview, ensure_ascii=False).encode('utf8')), 30_000)

    async def test_strict_callback_size_and_identity(self):
        payload = {'header': {'app_id': 'app', 'event_type': 'card.action.trigger', 'event_id': 'fixture-event'},
                   'event': {'operator': {'open_id': 'ou_test', 'union_id': 'on_test'},
                             'context': {'open_chat_id': 'oc_test', 'open_message_id': 'om_test'},
                             'action': {'value': {'kind': 'notice_form', 'action': 'save', 'ticket': 'abcdef123456_' + 'x' * 22},
                                        'form_value': {'simple_test': 'ok'}}}}
        self.assertIsNotNone(cards.parse_card_event(payload, 'app'))
        for key, value in [('union_id', 'not-a-union'), ('open_id', 'forged')]:
            bad = copy.deepcopy(payload); bad['event']['operator'][key] = value
            self.assertIsNone(cards.parse_card_event(bad, 'app'))
        for form in [{'x': 'a' * 100001}, {'x': {'nested': 'forged'}}, {str(i): 'x' for i in range(501)}]:
            bad = copy.deepcopy(payload); bad['event']['action']['form_value'] = form
            self.assertIsNone(cards.parse_card_event(bad, 'app'))

    async def test_generic_empty_array_can_add_fill_and_remove_without_execution(self):
        field = {'name': 'step0.items', 'label': '明细', 'type': 'array', 'value': [], 'maxItems': 2,
                 'item': {'type': 'object', 'children': [child('description', 'textarea')]}}
        plan = base_plan(fields=[field])
        bot = FakeBot(); bot.plan = plan
        prepared = await cards.prepare_card(bot, message(), ACTOR, conversation_with(plan), plan)
        reply = await cards.handle_card(bot, message(card={'kind': 'notice_form', 'action': 'add-row',
            'ticket': prepared['ticket'], 'value': {'array': cards._generic_id(field, ())}}), None, ACTOR, conversation_with(plan))
        self.assertEqual(reply['plan']['fields'][0]['value'], [{}])
        edited = cards._build_values(reply['plan']['fields'], {cards._generic_id(field, (0, 'description')): '待处理明细'})
        self.assertEqual(edited[field['name']], [{'description': '待处理明细'}])
        removed = await cards.handle_card(bot, message(card={'kind': 'notice_form', 'action': 'remove-row',
            'ticket': reply['ticket'], 'value': {'array': cards._generic_id(field, ())}}), None, ACTOR, conversation_with(plan))
        self.assertEqual(removed['plan']['fields'][0]['value'], [])
        self.assertEqual(bot.calls, [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
