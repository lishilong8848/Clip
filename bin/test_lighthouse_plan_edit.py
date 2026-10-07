# -*- coding: utf-8 -*-
"""Bounded tests for PortalAgent.amend(payload={action: "edit", version}).

Covers the "edit my operation plan" workflow on a throwaway in-memory Store and a
minimal fake FastAPI endpoint.  Helpers are reused from existing test modules by
module import (never by TestCase import) so duplicate unittest discovery does not
pick them up again.
"""
import asyncio
import copy
import sys
import tempfile
import unittest
from pathlib import Path
from typing import List, Literal
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

from fastapi import FastAPI, Request
from pydantic import BaseModel

# Reused helpers via module import (never import the TestCase classes).
from test_lighthouse_stream import Store
from lan_bitable_template_portal.lighthouse_ai import AssistantError, LighthouseAssistant
from lan_bitable_template_portal.lighthouse_agent import PLAN_NAMESPACE, PortalAgent, _result_refs
from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog
from lan_bitable_template_portal.lighthouse_files import LighthouseFiles

API_ID = "POST /api/plan-edit/records"
ACTOR = {"id": "plan-edit-owner", "scopes": ["A"], "is_admin": True}


class PlanSub(BaseModel):
    k1: str = ""
    k2: int = 0


class PlanPayload(BaseModel):
    operation_id: str = ""
    name: str = ""
    note: str = ""
    when: str = ""
    tags: List[Literal["a", "b", "c"]] = []
    payload: PlanSub = PlanSub()


_FIELDS = [
    {"name": "step0.name", "path": "name", "label": "名称", "type": "text", "required": True},
    {"name": "step0.when", "path": "when", "label": "日期", "type": "date", "required": True},
    {"name": "step0.tags", "path": "tags", "label": "标签", "type": "multiselect", "required": True,
     "options": [{"value": "a", "label": "甲"}, {"value": "b", "label": "乙"}, {"value": "c", "label": "丙"}]},
    {"name": "step0.payload", "path": "payload", "label": "对象", "type": "object", "required": True},
    {"name": "step0.note", "path": "note", "label": "备注", "type": "textarea", "required": False},
]

_VALUES = {
    "step0.name": "alpha",
    "step0.when": "2026-10-01",
    "step0.tags": ["a", "c"],
    "step0.payload": {"k1": "v1", "k2": 7},
    "step0.note": "",
}


def _request():
    return Request({"type": "http", "scheme": "http", "server": ("testserver", 80),
                    "client": ("127.0.0.1", 1), "path": "/api/assistant/plans",
                    "root_path": "", "query_string": b"",
                    "headers": [(b"cookie", b"fixture=owner"), (b"origin", b"http://testserver")]})


class PlanEditTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.store = Store(root / "assistant.sqlite3")
        self.app = FastAPI()
        self.received = []

        @self.app.post("/api/plan-edit/records")
        async def edit_records(payload: PlanPayload):
            self.received.append(payload.model_dump())
            return {"ok": True, "id": "rec-1", "data": payload.model_dump()}

        self.catalog = PortalAPICatalog(self.app)
        self.assertIn(API_ID, self.catalog._descriptors)
        model = Mock()
        model.settings.return_value = {}
        self.assistant = LighthouseAssistant(self.store, lambda *_: ([], []), model=model)
        self.files = LighthouseFiles(self.store, root=root / "files")
        self.agent = PortalAgent(self.assistant, self.catalog, self.files)
        self.request = _request()

    def _prepare(self):
        return self.agent.prepare(
            ACTOR,
            {"title": "t", "operations": [{"api_id": API_ID}], "fields": copy.deepcopy(_FIELDS)},
            "plan-edit-turn-00001", [], queries={})

    def _fill(self, plan, values=None):
        return self.agent.amend(ACTOR, plan["id"], {
            "version": plan["version"], "values": values if values is not None else copy.deepcopy(_VALUES)})

    def _edit(self, version):
        return self.agent.amend(ACTOR, self.plan["id"], {"action": "edit", "version": version})

    def _raw(self):
        return self.store.get_document(PLAN_NAMESPACE, self.plan["id"])

    def _restart(self):
        # A new PortalAgent over the same assistant/store simulates a process restart.
        return PortalAgent(self.assistant, self.catalog, self.files)

    async def _confirm_twice(self, agent, awaiting_version, request):
        reviewed = await agent.confirm(ACTOR, self.plan["id"], {"version": awaiting_version, "stage": "review"}, request)
        executed = await agent.confirm(ACTOR, self.plan["id"], {"version": reviewed["version"], "stage": "execute"}, request)
        if agent.tasks:
            await asyncio.gather(*tuple(agent.tasks))
        return executed

    def _resolved_review_values(self):
        # Frozen object/array _edit_value entries may carry $query refs that only
        # resolve against the persisted _queries; compare the resolved value instead
        # of requiring one exact private representation.
        raw = self._raw()
        return {field["name"]: _result_refs(field["_edit_value"], [], raw.get("_references"), raw.get("_queries"))
                for field in raw["_review_fields"]}

    def _resolved_public_values(self, public=None):
        # Public field values may retain the frozen $query binding after reopening;
        # resolve against stored _queries so the full restored value is still verified.
        if public is None:
            public = self.agent.public_plan(self.agent.get_plan(ACTOR, self.plan["id"]))
        raw = self._raw()
        return {field["name"]: _result_refs(field.get("value"), [], raw.get("_references"), raw.get("_queries"))
                for field in public["fields"] if "value" in field}

    async def test_scalar_date_multiselect_object_survive_edit_and_restart(self):
        self.plan = self._prepare()
        filled = self._fill(self.plan)
        self.assertEqual(filled["status"], "awaiting_confirmation")
        self.assertTrue(filled["can_edit"])

        raw = self._raw()
        self.assertEqual(self._resolved_review_values(),
                         {**copy.deepcopy(_VALUES), "step0.payload": {"k1": "v1", "k2": 7}})

        edited = self._edit(filled["version"])
        self.assertEqual(edited["status"], "needs_input")
        self.assertFalse(edited["can_edit"])
        resolved = self._resolved_public_values(edited)
        self.assertEqual(resolved["step0.name"], "alpha")
        self.assertEqual(resolved["step0.when"], "2026-10-01")
        self.assertEqual(resolved["step0.tags"], ["a", "c"])
        self.assertEqual(resolved["step0.payload"], {"k1": "v1", "k2": 7})
        self.assertEqual(resolved["step0.note"], "")

        # Simulate a PortalAgent restart: a brand-new agent over the persisted plan.
        restarted = self._restart()
        restored = restarted.get_plan(ACTOR, self.plan["id"])
        self.assertEqual(restored["status"], "needs_input")
        values = self._resolved_public_values(restarted.public_plan(restored))
        self.assertEqual(values["step0.name"], "alpha")
        self.assertEqual(values["step0.when"], "2026-10-01")
        self.assertEqual(values["step0.tags"], ["a", "c"])
        self.assertEqual(values["step0.payload"], {"k1": "v1", "k2": 7})
        self.assertEqual(values["step0.note"], "")

    async def test_changed_values_reach_same_original_endpoint_once_after_confirm_twice(self):
        self.plan = self._prepare()
        opened = self.plan["operations"][0]["body"].get("operation_id")
        filled = self._fill(self.plan)
        self.assertEqual(self._raw()["_review_operations"][0]["body"]["operation_id"], opened)

        edited = self._edit(filled["version"])
        self.assertEqual(edited["status"], "needs_input")
        # Edit restores the pre-amend operations verbatim (same operation ID, empty body).
        restored_ops = self._raw()["operations"]
        self.assertEqual(restored_ops[0]["body"]["operation_id"], opened)
        self.assertEqual(restored_ops[0]["body"].get("name"), "")
        self.assertEqual(restored_ops[0]["body"].get("note"), "")

        changed = {
            "step0.name": "beta", "step0.when": "2026-10-02", "step0.tags": ["b"],
            "step0.payload": {"k1": "v2", "k2": 9}, "step0.note": "正在修改",
        }
        refilled = self.agent.amend(ACTOR, self.plan["id"], {"version": edited["version"], "values": changed})
        self.assertEqual(refilled["status"], "awaiting_confirmation")
        self.assertEqual(self._raw()["operations"][0]["body"]["operation_id"], opened)

        executed = await self._confirm_twice(self.agent, refilled["version"], self.request)
        self.assertEqual(executed["status"], "running")
        finished = self.agent.get_plan(ACTOR, self.plan["id"])
        self.assertEqual(finished["status"], "completed", finished.get("error"))
        self.assertTrue(finished["results"][0]["ok"])

        # Same original endpoint got exactly one call, carrying the changed values.
        self.assertEqual(len(self.received), 1)
        self.assertEqual(self.received[0], {
            "operation_id": opened, "name": "beta", "note": "正在修改", "when": "2026-10-02",
            "tags": ["b"], "payload": {"k1": "v2", "k2": 9}})

    async def test_edit_from_awaiting_second_confirmation_then_confirm_twice(self):
        self.plan = self._prepare()
        filled = self._fill(self.plan)
        reviewed = await self.agent.confirm(ACTOR, self.plan["id"], {"version": filled["version"], "stage": "review"}, self.request)
        self.assertEqual(reviewed["status"], "awaiting_second_confirmation")
        self.assertTrue(reviewed["can_edit"])

        edited = self._edit(reviewed["version"])
        self.assertEqual(edited["status"], "needs_input")
        refilled = self.agent.amend(ACTOR, self.plan["id"], {
            "version": edited["version"],
            "values": {"step0.name": "beta", "step0.when": "2026-10-05", "step0.tags": ["c"],
                       "step0.payload": {"k1": "x", "k2": 3}, "step0.note": "changed"}})
        executed = await self._confirm_twice(self.agent, refilled["version"], self.request)
        finished = self.agent.get_plan(ACTOR, self.plan["id"])
        self.assertEqual(finished["status"], "completed")
        self.assertEqual(len(self.received), 1)
        self.assertEqual(self.received[0]["name"], "beta")
        self.assertEqual(self.received[0]["when"], "2026-10-05")
        self.assertEqual(self.received[0]["note"], "changed")

    async def test_edit_increments_version_and_invalidates_old_confirmation(self):
        self.plan = self._prepare()
        filled = self._fill(self.plan)
        old_version = filled["version"]
        edited = self._edit(old_version)
        self.assertEqual(edited["version"], old_version + 1)

        refilled = self.agent.amend(ACTOR, self.plan["id"], {"version": edited["version"], "values": copy.deepcopy(_VALUES)})
        self.assertEqual(refilled["version"], old_version + 2)

        with self.assertRaises(AssistantError) as ctx:
            await self.agent.confirm(ACTOR, self.plan["id"], {"version": old_version, "stage": "review"}, self.request)
        self.assertEqual(ctx.exception.status, 409)

    async def test_foreign_user_and_scopes_cannot_edit(self):
        self.plan = self._prepare()
        filled = self._fill(self.plan)
        with self.assertRaises(AssistantError):
            self.agent.amend({**ACTOR, "id": "intruder"}, self.plan["id"], {"action": "edit", "version": filled["version"]})
        with self.assertRaises(AssistantError):
            self.agent.amend({**ACTOR, "scopes": ["Z"]}, self.plan["id"], {"action": "edit", "version": filled["version"]})
        # Plan unchanged, still awaiting_confirmation at the same version.
        raw = self._raw()
        self.assertEqual(raw["status"], "awaiting_confirmation")
        self.assertEqual(raw["version"], filled["version"])

    async def test_unknown_action_cannot_edit(self):
        self.plan = self._prepare()
        filled = self._fill(self.plan)
        with self.assertRaises(AssistantError):
            self.agent.amend(ACTOR, self.plan["id"], {"action": "bogus", "version": filled["version"]})
        raw = self._raw()
        self.assertEqual(raw["status"], "awaiting_confirmation")
        self.assertEqual(raw["version"], filled["version"])

    async def test_partial_results_and_terminal_statuses_not_editable(self):
        self.plan = self._prepare()
        filled = self._fill(self.plan)

        # Partial result (results already present while still awaiting) cannot be edited.
        partial = self._raw()
        partial["results"] = [{"ok": True, "api_id": API_ID, "data": {"partial": True}}]
        self.store.put_document(PLAN_NAMESPACE, self.plan["id"], partial)
        self.assertFalse(self.agent.public_plan(self.agent.get_plan(ACTOR, self.plan["id"]))["can_edit"])
        with self.assertRaises(AssistantError):
            self.agent.amend(ACTOR, self.plan["id"], {"action": "edit", "version": filled["version"]})

        # Terminal / active statuses must never expose editing (fresh awaiting plan each time).
        for status in ("submitted", "running", "completed", "cancelled", "failed"):
            with self.subTest(status=status):
                probe = self._raw()
                probe["results"] = []
                probe["status"] = status
                self.store.put_document(PLAN_NAMESPACE, self.plan["id"], probe)
                self.assertFalse(self.agent.public_plan(self.agent.get_plan(ACTOR, self.plan["id"]))["can_edit"])
                with self.assertRaises(AssistantError):
                    self.agent.amend(ACTOR, self.plan["id"], {"action": "edit", "version": filled["version"]})

    async def test_optional_top_level_textarea_empty_actually_clears(self):
        self.plan = self._prepare()
        # Start with a NONEMPTY value, reopen into edit, then clear it so the
        # empty-string result really reflects a user clear rather than a no-op.
        initial_note = "初次填写的一条非空备注"
        filled = self._fill(self.plan, {**copy.deepcopy(_VALUES), "step0.note": initial_note})
        raw = self._raw()
        self.assertEqual(raw["operations"][0]["body"]["note"], initial_note)
        self.assertEqual(next(f for f in raw["_review_fields"] if f["name"] == "step0.note")["_edit_value"], initial_note)

        edited = self._edit(filled["version"])
        self.assertEqual(self._resolved_public_values(edited)["step0.note"], initial_note)
        refilled = self.agent.amend(ACTOR, self.plan["id"], {
            "version": edited["version"],
            "values": {**copy.deepcopy(_VALUES), "step0.note": ""}})
        self.assertEqual(self._raw()["operations"][0]["body"]["note"], "")
        executed = await self._confirm_twice(self.agent, refilled["version"], self.request)
        finished = self.agent.get_plan(ACTOR, self.plan["id"])
        self.assertEqual(finished["status"], "completed")
        self.assertEqual(len(self.received), 1)
        self.assertEqual(self.received[0]["note"], "")

    async def test_read_only_structured_metadata_not_tampered_after_edit(self):
        self.plan = self._prepare()
        # Snapshot the full original (pre-amend) field metadata before any fill.
        originals = {field["name"]: copy.deepcopy(field) for field in self.plan["fields"]}
        filled = self._fill(self.plan)

        # action:edit must reject any extra private/review/values keys and leave the
        # awaiting_confirmation plan completely unchanged.
        before = self._raw()
        for extra in ({"_review_fields": []}, {"_review_operations": []}, {"values": {"step0.name": "injected"}}):
            with self.subTest(extra=tuple(extra)):
                with self.assertRaises(AssistantError):
                    self.agent.amend(ACTOR, self.plan["id"], {"action": "edit", "version": filled["version"], **extra})
                raw = self._raw()
                self.assertEqual(raw["status"], before["status"])
                self.assertEqual(raw["version"], before["version"])
                self.assertEqual(raw["fields"], before["fields"])
                self.assertEqual(raw["operations"], before["operations"])

        edited = self._edit(filled["version"])
        raw = self._raw()

        # Restored fields are the original pre-amend fields plus the private _edit_value;
        # every other key (including the immutable structured `value`) must be untouched.
        for field in raw["fields"]:
            restored_meta = {k: v for k, v in field.items() if k != "_edit_value"}
            self.assertEqual(restored_meta, originals[field["name"]], field["name"])
        payload = next(f for f in raw["fields"] if f["name"] == "step0.payload")
        self.assertEqual(_result_refs(payload["_edit_value"], [], raw.get("_references"), raw.get("_queries")),
                         {"k1": "v1", "k2": 7})
        # The immutable structured value metadata may itself carry a frozen $query
        # binding; it reflects the original baseline and must be untouched.
        self.assertIn("$query", payload["value"])
        # The reopened public field value remains the edited value (with a frozen
        # $query binding); resolving it still yields the full edited payload.
        self.assertEqual(self._resolved_public_values(edited)["step0.payload"], {"k1": "v1", "k2": 7})
        tags = next(f for f in raw["fields"] if f["name"] == "step0.tags")
        self.assertEqual([option["value"] for option in tags["options"]], ["a", "b", "c"])

        # Operations were restored to the pre-amend originals (empty body, same operation id).
        ops = raw["operations"]
        self.assertEqual(ops[0]["body"].get("name"), "")
        self.assertEqual(ops[0]["body"].get("when"), "")
        self.assertEqual(ops[0]["body"].get("tags"), [])
        self.assertEqual(ops[0]["body"].get("payload"), {"k1": "", "k2": 0})
        self.assertEqual(ops[0]["body"]["operation_id"], self.plan["operations"][0]["body"]["operation_id"])

        # An invalid option after reopening must be rejected without changing the plan.
        self.assertEqual(edited["status"], "needs_input")
        needs_input = self._raw()
        with self.assertRaises(AssistantError):
            self.agent.amend(ACTOR, self.plan["id"], {
                "version": needs_input["version"],
                "values": {**copy.deepcopy(_VALUES), "step0.tags": ["z"]}})
        after = self._raw()
        self.assertEqual(after["status"], needs_input["status"])
        self.assertEqual(after["version"], needs_input["version"])
        self.assertEqual(after["fields"], needs_input["fields"])
        self.assertEqual(after["operations"], needs_input["operations"])

    async def test_private_review_state_never_exposed_publicly(self):
        self.plan = self._prepare()
        self._fill(self.plan)
        self._edit(self.raw_version())
        public = self.agent.public_plan(self.agent.get_plan(ACTOR, self.plan["id"]))
        for key in ("_review_fields", "_review_operations", "_edit_value"):
            self.assertNotIn(key, public)
            self.assertNotIn(key, str(public))
        for field in public["fields"]:
            self.assertNotIn("_edit_value", field)
            self.assertNotIn("_initial_form", field)

    async def test_long_top_textarea_survives_reopen_and_refill(self):
        self.plan = self._prepare()
        # ~9000 chars in a top-level textarea; the frozen $query path must not
        # truncate the value on reopening/refilling.
        long_note = "长" * 9000
        filled = self._fill(self.plan, {**copy.deepcopy(_VALUES), "step0.note": long_note})

        edited = self._edit(filled["version"])
        # Public restored field value resolves back to the full long note.
        self.assertEqual(self._resolved_public_values(edited)["step0.note"], long_note)
        self.assertEqual(len(self._resolved_public_values(edited)["step0.note"]), 9000)

        refilled = self.agent.amend(ACTOR, self.plan["id"], {
            "version": edited["version"],
            "values": {**copy.deepcopy(_VALUES), "step0.note": long_note}})
        executed = await self._confirm_twice(self.agent, refilled["version"], self.request)
        finished = self.agent.get_plan(ACTOR, self.plan["id"])
        self.assertEqual(finished["status"], "completed")
        self.assertEqual(len(self.received), 1)
        self.assertEqual(self.received[0]["note"], long_note)
        self.assertEqual(len(self.received[0]["note"]), 9000)

    async def test_nested_object_text_over_20k_survives_reopen_and_refill(self):
        self.plan = self._prepare()
        # The product API schema permits k1/k2 to be edited, and k1 is plain text;
        # a >20,000 char nested value must survive reopening/refilling intact.
        big_text = "值" * 20001
        payload = {"k1": big_text, "k2": 7}
        filled = self._fill(self.plan, {**copy.deepcopy(_VALUES), "step0.payload": payload})

        edited = self._edit(filled["version"])
        self.assertEqual(self._resolved_public_values(edited)["step0.payload"], payload)
        self.assertEqual(len(self._resolved_public_values(edited)["step0.payload"]["k1"]), 20001)

        refilled = self.agent.amend(ACTOR, self.plan["id"], {
            "version": edited["version"],
            "values": {**copy.deepcopy(_VALUES), "step0.payload": payload}})
        executed = await self._confirm_twice(self.agent, refilled["version"], self.request)
        finished = self.agent.get_plan(ACTOR, self.plan["id"])
        self.assertEqual(finished["status"], "completed")
        self.assertEqual(len(self.received), 1)
        self.assertEqual(self.received[0]["payload"], payload)
        self.assertEqual(len(self.received[0]["payload"]["k1"]), 20001)

    def raw_version(self):
        return self._raw()["version"]


if __name__ == "__main__":
    unittest.main()