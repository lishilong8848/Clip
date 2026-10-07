"""Focused isolated tests for the native repair-management "people" field contract.

Scope
-----
* Only `bin/test_lighthouse_repair_people.py` is created/edited.
* Uses the project venv interpreter and reuses existing test helpers
  (``Store``, ``gather_tasks``, ``ACTOR``) by module import -- never by
  importing another test *class*.
* Fake in-process native endpoints + a temp in-memory store.  No network,
  no production server/cloud/credentials, no pip install, no npm/build.

Contract under test (the target being implemented by Codex)
-----------------------------------------------------------
* ``portal_service`` treats ``field_type == 11`` or ``ui_type == "user"`` as
  a person; ``15`` / ``ui_type == "url"`` stays URL/text and is NOT a person.
* ``_repair_frontend_fields(metas, unlinked=..., scope='A')`` emits a
  ``native_repair_people`` control for type-11 / user fields and keeps
  type-15 as a URL/text control.
* Selected people are normalised to ``[{"id": ...}]`` only; names are
  UI/audit labels.  Plain strings, missing IDs, nested identity-spoof payloads
  and readonly edits must fail; an empty list clears an optional field while
  an empty required field must fail.
* Other data, relation IDs and version must be preserved.  Return-to-edit
  keeps both people names and IDs.
* The four native operations POST/PUT `/api/repair-management/records` and
  `/api/repair-management/followups` run through the real ``PortalAgent`` +
  ``PortalAPICatalog`` with the existing Pydantic request models.
* Followup snapshots carry ``summary_record_id`` + ``relation_mode=record_id``;
  project snapshots use ``REPAIR_MANAGEMENT_TABLE_ID``.

The product is still being edited.  Tests failing here are reported exactly
as-is; no product code is modified and no assertion is weakened.
"""
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

from fastapi import FastAPI, Request
from clipflow_backend.api_models import (
    RepairFollowupRecordRequest,
    RepairManagementRecordRequest,
)

from lan_bitable_template_portal.lighthouse_agent import PortalAgent, _result_refs
from lan_bitable_template_portal.lighthouse_ai import AssistantError, LighthouseAssistant
from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog, _repair_frontend_fields
from lan_bitable_template_portal.lighthouse_files import LighthouseFiles
from lan_bitable_template_portal.portal_service import REPAIR_MANAGEMENT_TABLE_ID

from test_lighthouse_agent_workflows import Store, gather_tasks, ACTOR


def _request():
    return Request({
        "type": "http",
        "scheme": "http",
        "server": ("testserver", 80),
        "client": ("127.0.0.1", 4567),
        "path": "/api/assistant/agent",
        "root_path": "",
        "query_string": b"",
        "headers": [(b"cookie", b"fixture=a"), (b"origin", b"http://testserver")],
    })


def _build_app(writes):
    """Fake native endpoints that capture the validated Pydantic payloads."""
    app = FastAPI()

    @app.post("/api/repair-management/records")
    async def create_record(body: RepairManagementRecordRequest):
        writes.append(body.model_dump())
        return {"ok": True, "data": {"record_id": "rec-created"}}

    @app.put("/api/repair-management/records/{record_id}")
    async def update_record(record_id: str, body: RepairManagementRecordRequest):
        writes.append(body.model_dump())
        return {"ok": True, "data": {"updated": True}}

    @app.post("/api/repair-management/followups")
    async def create_followup(body: RepairFollowupRecordRequest):
        writes.append(body.model_dump())
        return {"ok": True, "data": {"record_id": "rec-followup"}}

    @app.put("/api/repair-management/followups/{record_id}")
    async def update_followup(record_id: str, body: RepairFollowupRecordRequest):
        writes.append(body.model_dump())
        return {"ok": True, "data": {"updated": True}}

    return app


def _metas():
    return [
        {"field_name": "维修负责人", "field_type": 11, "editable": True, "options": []},
        {"field_name": "维修链接", "field_type": 15, "editable": True, "options": []},
        {"field_name": "只读文本", "field_type": 1, "editable": False, "options": []},
        {"field_name": "所属专业", "field_type": 3, "editable": True, "options": ["电气", "暖通"]},
    ]


def _raw_fields():
    return {
        "维修负责人": [{"id": "ou_1", "name": "张三"}, {"id": "ou_2", "name": "李四"}],
        "维修链接": {"link": "https://example.com/a", "text": "验收链接"},
        "只读文本": "只读内容不可改",
        "所属专业": "电气",
    }


class RepairPeopleTests(unittest.IsolatedAsyncioTestCase):
    """Focused, isolated tests for the native repair people contract."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = Store(Path(self.tmp.name) / "state.sqlite3")
        model = Mock()
        model.settings.return_value = {
            "configured": True,
            "enabled": True,
            "active_model_id": "default",
            "models": [{"id": "default", "name": "测试模型", "model": "fixture-model", "configured": True}],
        }
        model.profile.return_value = {"id": "default", "name": "测试模型", "model": "fixture-model"}
        search = Mock(side_effect=AssertionError("agent must use backend APIs, not local/DOM scraping"))
        self.assistant = LighthouseAssistant(self.store, search, model=model)
        self.files = LighthouseFiles(self.store)
        self.writes = []
        self.app = _build_app(self.writes)
        self.catalog = PortalAPICatalog(self.app)
        self.agent = PortalAgent(self.assistant, self.catalog, self.files)
        self.request = _request()
        self.operation_id = "repair_people_test_00000001"

    def _agent_app(self, writes):
        """A second independent agent+app pair for multi-plan tests."""
        app = _build_app(writes)
        return PortalAgent(self.assistant, PortalAPICatalog(app), self.files), app

    # ------------------------------------------------------------------ #
    # Direct _repair_frontend_fields classification                        #
    # ------------------------------------------------------------------ #

    def test_repair_frontend_fields_type11_included_as_people_and_type15_not_people(self):
        metas = _metas()
        try:
            control = _repair_frontend_fields(metas, unlinked=True, scope="A")
        except TypeError as exc:  # target signature not yet implemented in product
            self.fail("_repair_frontend_fields must accept scope (target signature has scope='A'): %s" % exc)
        children = {child["path"]: child for child in control["children"]}
        self.assertIn("维修负责人", children, "type-11 / user fields must be surfaced (currently dropped)")
        self.assertTrue(children["维修负责人"].get("native_repair_people"),
                        "type-11 must map to the native repair people picker control")
        self.assertNotEqual(children["维修负责人"].get("type"), "textarea",
                            "type-11 must not fall back to URL/text")
        url_child = children.get("维修链接")
        self.assertIn("维修链接", children, "type-15 URL must remain an editable URL/text control")
        self.assertFalse(url_child.get("native_repair_people"),
                         "type-15 URL must NOT be treated as a people control")
        self.assertNotIn("只读文本", children, "readonly text must stay out of the editable control")

    def test_repair_frontend_fields_ui_type_user_counts_as_people_but_ui_type_url_does_not(self):
        metas = [
            {"field_name": "处理人", "ui_type": "user", "editable": True, "options": []},
            {"field_name": "资料地址", "ui_type": "url", "editable": True, "options": []},
        ]
        try:
            control = _repair_frontend_fields(metas, unlinked=True, scope="A")
        except TypeError as exc:
            self.fail("_repair_frontend_fields must accept scope: %s" % exc)
        children = {child["path"]: child for child in control["children"]}
        self.assertTrue(children["处理人"].get("native_repair_people"),
                        "ui_type=user must be treated as a person field")
        self.assertFalse(children["资料地址"].get("native_repair_people"),
                         "ui_type=url must remain a URL/text field, never a person")

    # ------------------------------------------------------------------ #
    # Project record PUT workflow                                           #
    # ------------------------------------------------------------------ #

    async def test_record_put_people_selection_ids_only_labels_names_and_preservation(self):
        record = {
            "record_id": "rec-project",
            "record_version": "v-project-1",
            "building_codes": ["A"],
            "raw_fields": _raw_fields(),
            "source_event_id": "rec-event-1",
            "source_repair_ids": ["rec-repair-1"],
        }
        queries = {"query_" + "a" * 32: {"table_id": REPAIR_MANAGEMENT_TABLE_ID, "records": [record], "fields": _metas()}}
        operation = {
            "api_id": "PUT /api/repair-management/records/{record_id}",
            "path_params": {"record_id": "rec-project"},
            "body": {"scope": "A"},
        }
        plan = self.agent.prepare(ACTOR, {"operations": [operation]}, self.operation_id, [], queries=queries)
        self.assertEqual(plan["status"], "needs_input")
        field = next(f for f in plan["fields"] if f["path"] == "fields")
        children = {child["path"]: child for child in field["children"]}
        self.assertIn("维修负责人", children, "type-11 people field must be editable in the repair form")
        self.assertTrue(children["维修负责人"].get("native_repair_people"))

        public = self.agent.public_plan(plan)
        value = public["fields"][0]["value"]
        # Original people names are retained for return-to-edit.
        self.assertIn("张三", json.dumps(value, ensure_ascii=False))

        # Selection: keep ou_1, drop ou_2.
        chosen = {**value, "维修负责人": [{"id": "ou_1", "name": "张三"}]}
        amended = self.agent.amend(ACTOR, plan["id"], {"version": plan["version"], "values": {field["name"]: chosen}})
        body = self.agent.get_plan(ACTOR, amended["id"])["operations"][0]["body"]
        # Native payload: people are IDs only -- no names, no nested spoof keys.
        self.assertEqual(body["fields"]["维修负责人"], [{"id": "ou_1"}])
        self.assertNotIn("name", json.dumps(body["fields"]["维修负责人"]))
        # Other data, relations and version preserved.
        self.assertEqual(body["expected_version"], "v-project-1")
        self.assertEqual(body["source_event_id"], "rec-event-1")
        self.assertEqual(body["source_repair_ids"], ["rec-repair-1"])
        self.assertFalse(body.get("replace_source_relations"))
        self.assertEqual(set(body["fields"]), {"维修负责人"})
        merged = {**record["raw_fields"], **body["fields"]}
        self.assertEqual(merged["所属专业"], "电气")
        self.assertEqual(merged["只读文本"], "只读内容不可改")
        self.assertEqual(merged["维修链接"], {"link": "https://example.com/a", "text": "验收链接"})

        public_after = self.agent.public_plan(self.agent.get_plan(ACTOR, amended["id"]))
        labels = public_after["operations"][0]["selected_labels"].get("维修负责人", "")
        self.assertIn("张三", labels, "public selected_labels must show the person's name")
        self.assertNotIn("ou_1", labels, "public selected_labels must not expose the raw ID")
        self.assertNotIn("[{", labels, "public selected_labels must not be a raw ID list JSON")

    async def test_record_put_people_selection_removal_and_clear(self):
        record = {
            "record_id": "rec-project",
            "record_version": "v-project-2",
            "building_codes": ["A"],
            "raw_fields": _raw_fields(),
            "source_event_id": "rec-event-2",
            "source_repair_ids": ["rec-repair-1"],
        }
        queries = {"query_" + "b" * 32: {"table_id": REPAIR_MANAGEMENT_TABLE_ID, "records": [record], "fields": _metas()}}
        operation = {
            "api_id": "PUT /api/repair-management/records/{record_id}",
            "path_params": {"record_id": "rec-project"},
            "body": {"scope": "A"},
        }
        plan = self.agent.prepare(ACTOR, {"operations": [operation]}, self.operation_id, [], queries=queries)
        field = next(f for f in plan["fields"] if f["path"] == "fields")
        value = self.agent.public_plan(plan)["fields"][0]["value"]

        # Removal: keep ou_1, drop ou_2.
        removal = {**value, "维修负责人": [{"id": "ou_1", "name": "张三"}]}
        amended = self.agent.amend(ACTOR, plan["id"], {"version": plan["version"], "values": {field["name"]: removal}})
        self.assertEqual(self.agent.get_plan(ACTOR, amended["id"])["operations"][0]["body"]["fields"]["维修负责人"],
                         [{"id": "ou_1"}])

        # Clear an optional people field: empty list must be accepted. A fresh
        # edit must first return to the form (action=edit/version) before amend.
        edit_plan = self.agent.amend(ACTOR, amended["id"], {"action": "edit", "version": amended["version"]})
        self.assertEqual(edit_plan["status"], "needs_input")
        edit_field = next(f for f in edit_plan["fields"] if f["path"] == "fields")
        cleared = {**edit_field["value"], "维修负责人": []}
        amended_clear = self.agent.amend(ACTOR, edit_plan["id"],
                                         {"version": edit_plan["version"], "values": {edit_field["name"]: cleared}})
        self.assertEqual(self.agent.get_plan(ACTOR, amended_clear["id"])["operations"][0]["body"]["fields"]["维修负责人"], [])

        # The empty selection labels must say "不关联/清空", never expose IDs.
        public_clear = self.agent.public_plan(self.agent.get_plan(ACTOR, amended_clear["id"]))
        labels = public_clear["operations"][0]["selected_labels"].get("维修负责人", "")
        self.assertNotIn("ou_", labels)

    async def test_record_put_people_malformed_selection_and_readonly_fail(self):
        record = {
            "record_id": "rec-project",
            "record_version": "v-project-3",
            "building_codes": ["A"],
            "raw_fields": _raw_fields(),
            "source_event_id": "rec-event-3",
            "source_repair_ids": ["rec-repair-1"],
        }
        queries = {"query_" + "c" * 32: {"table_id": REPAIR_MANAGEMENT_TABLE_ID, "records": [record], "fields": _metas()}}
        operation = {
            "api_id": "PUT /api/repair-management/records/{record_id}",
            "path_params": {"record_id": "rec-project"},
            "body": {"scope": "A"},
        }
        plan = self.agent.prepare(ACTOR, {"operations": [operation]}, self.operation_id, [], queries=queries)
        field = next(f for f in plan["fields"] if f["path"] == "fields")
        base_value = self.agent.public_plan(plan)["fields"][0]["value"]

        # Plain string must fail (people must be structured objects with an id).
        with self.assertRaises(AssistantError):
            self.agent.amend(ACTOR, plan["id"], {"version": plan["version"],
                                                 "values": {field["name"]: {**base_value, "维修负责人": "ou_1"}}})

        # Missing ID (name only) must fail.
        with self.assertRaises(AssistantError):
            self.agent.amend(ACTOR, plan["id"], {"version": plan["version"],
                                                 "values": {field["name"]: {**base_value, "维修负责人": [{"name": "张三"}]}}})

        # Nested identity-spoof fields must fail.
        with self.assertRaises(AssistantError):
            self.agent.amend(ACTOR, plan["id"], {"version": plan["version"],
                                                 "values": {field["name"]: {**base_value,
                                                                           "维修负责人": [{"id": "ou_1", "name": "张三", "users": [{"id": "ou_evil"}]}]}}})

        # Readonly text edits must fail.
        with self.assertRaises(AssistantError):
            self.agent.amend(ACTOR, plan["id"], {"version": plan["version"],
                                                 "values": {field["name"]: {**base_value, "只读文本": "篡改"}}})
        self.assertEqual(self.writes, [])

    # ------------------------------------------------------------------ #
    # Project record POST workflow                                          #
    # ------------------------------------------------------------------ #

    async def test_record_post_people_ids_only_directly(self):
        queries = {"query_" + "d" * 32: {"table_id": REPAIR_MANAGEMENT_TABLE_ID, "records": [], "fields": _metas()}}
        operation = {
            "api_id": "POST /api/repair-management/records",
            "body": {"scope": "A", "fields": {}},
        }
        plan = self.agent.prepare(ACTOR, {"operations": [operation]}, self.operation_id, [], queries=queries)
        field = next(f for f in plan["fields"] if f["path"] == "fields")
        children = {child["path"]: child for child in field["children"]}
        self.assertIn("维修负责人", children)
        value = self.agent.public_plan(plan)["fields"][0]["value"]
        filled = {**value, "维修负责人": [{"id": "ou_9", "name": "王五"}]}
        amended = self.agent.amend(ACTOR, plan["id"], {"version": plan["version"], "values": {field["name"]: filled}})
        body = self.agent.get_plan(ACTOR, amended["id"])["operations"][0]["body"]
        self.assertEqual(body["fields"]["维修负责人"], [{"id": "ou_9"}])
        self.assertNotIn("王五", json.dumps(body["fields"]))

    # ------------------------------------------------------------------ #
    # Followup workflows                                                    #
    # ------------------------------------------------------------------ #

    async def test_followup_put_people_contract_snapshot_and_preservation(self):
        raw = {**_raw_fields(), "维修进度": 0.58, "故障维修总费用": 12}
        metas = _metas() + [
            {"field_name": "维修进度", "field_type": 2, "editable": True, "options": []},
            {"field_name": "故障维修总费用", "field_type": 2, "editable": True, "options": []},
        ]
        snapshot = {
            "summary_record_id": "rec-parent",
            "relation_mode": "record_id",
            "fields": metas,
            "records": [{"record_id": "rec-followup", "record_version": "followup-v1", "raw_fields": raw}],
        }
        queries = {"query_" + "e" * 32: snapshot}
        op = {
            "api_id": "PUT /api/repair-management/followups/{record_id}",
            "path_params": {"record_id": "rec-followup"},
            "body": {"scope": "A", "summary_record_id": "rec-parent", "cmdb_record_ids": []},
        }
        plan = self.agent.prepare(ACTOR, {"operations": [op]}, self.operation_id, [], queries=queries)
        field = next(f for f in plan["fields"] if f["path"] == "fields")
        children = {child["path"]: child for child in field["children"]}
        self.assertIn("维修负责人", children, "followup people field must be editable")
        value = self.agent.public_plan(plan)["fields"][0]["value"]
        self.assertIn("张三", json.dumps(value, ensure_ascii=False))

        filled = {**value, "维修负责人": [{"id": "ou_2", "name": "李四"}], "维修进度": "0.75"}
        amended = self.agent.amend(ACTOR, plan["id"], {"version": plan["version"], "values": {field["name"]: filled}})
        body = self.agent.get_plan(ACTOR, amended["id"])["operations"][0]["body"]
        self.assertEqual(body["fields"]["维修负责人"], [{"id": "ou_2"}])
        # Other snapshot fields preserved.
        self.assertEqual(set(body["fields"]), {"维修负责人", "维修进度"})
        merged = {**raw, **body["fields"]}
        self.assertEqual(merged["故障维修总费用"], 12)
        self.assertEqual(merged["只读文本"], "只读内容不可改")
        self.assertEqual(merged["维修链接"], {"link": "https://example.com/a", "text": "验收链接"})
        self.assertEqual(body["expected_version"], "followup-v1")
        self.assertEqual(body["summary_record_id"], "rec-parent")

        public_after = self.agent.public_plan(self.agent.get_plan(ACTOR, amended["id"]))
        labels = public_after["operations"][0]["selected_labels"].get("维修负责人", "")
        self.assertIn("李四", labels)
        self.assertNotIn("ou_2", labels)

    async def test_followup_post_people_requires_matching_snapshot_and_ids_only(self):
        snapshot = {
            "summary_record_id": "rec-parent",
            "relation_mode": "record_id",
            "fields": _metas(),
            "records": [],
        }
        queries = {"query_" + "f" * 32: snapshot}
        op = {
            "api_id": "POST /api/repair-management/followups",
            "body": {"scope": "A", "summary_record_id": "rec-parent", "fields": {}},
        }
        plan = self.agent.prepare(ACTOR, {"operations": [op]}, self.operation_id, [], queries=queries)
        field = next(f for f in plan["fields"] if f["path"] == "fields")
        value = self.agent.public_plan(plan)["fields"][0]["value"]
        filled = {**value, "维修负责人": [{"id": "ou_77", "name": "新记录人"}]}
        amended = self.agent.amend(ACTOR, plan["id"], {"version": plan["version"], "values": {field["name"]: filled}})
        body = self.agent.get_plan(ACTOR, amended["id"])["operations"][0]["body"]
        self.assertEqual(body["fields"]["维修负责人"], [{"id": "ou_77"}])
        self.assertNotIn("新记录人", json.dumps(body["fields"]))
        self.assertNotEqual(body["fields"], {})

    # ------------------------------------------------------------------ #
    # Required empty, two confirmations, return-to-edit                     #
    # ------------------------------------------------------------------ #

    async def test_record_put_required_empty_fails(self):
        record = {
            "record_id": "rec-project",
            "record_version": "v",
            "building_codes": ["A"],
            "raw_fields": {"维修负责人": [{"id": "ou_1", "name": "张三"}], "只读文本": "x"},
            "source_event_id": "",
            "source_repair_ids": [],
        }
        # The people meta is required: an empty selection must be rejected.
        metas = [dict(meta, required=(meta["field_name"] == "维修负责人")) for meta in _metas()]
        queries = {"query_" + "g" * 32: {"table_id": REPAIR_MANAGEMENT_TABLE_ID, "records": [record], "fields": metas}}
        operation = {
            "api_id": "PUT /api/repair-management/records/{record_id}",
            "path_params": {"record_id": "rec-project"},
            "body": {"scope": "A"},
        }
        plan = self.agent.prepare(ACTOR, {"operations": [operation]}, self.operation_id, [], queries=queries)
        field = next(f for f in plan["fields"] if f["path"] == "fields")
        children = {child["path"]: child for child in field["children"]}
        self.assertTrue(children["维修负责人"].get("required"),
                        "a required type-11 field must be flagged required in the control")
        value = self.agent.public_plan(plan)["fields"][0]["value"]
        # A required empty people selection must be rejected.
        with self.assertRaises(AssistantError):
            self.agent.amend(ACTOR, plan["id"], {"version": plan["version"],
                                                 "values": {field["name"]: {**value, "维修负责人": []}}})
        self.assertEqual(self.writes, [])

    async def test_record_put_two_confirmations_and_return_to_edit_keeps_names_and_ids(self):
        record = {
            "record_id": "rec-project",
            "record_version": "v",
            "building_codes": ["A"],
            "raw_fields": _raw_fields(),
            "source_event_id": "rec-event",
            "source_repair_ids": ["rec-repair"],
        }
        queries = {"query_" + "h" * 32: {"table_id": REPAIR_MANAGEMENT_TABLE_ID, "records": [record], "fields": _metas()}}
        operation = {
            "api_id": "PUT /api/repair-management/records/{record_id}",
            "path_params": {"record_id": "rec-project"},
            "body": {"scope": "A"},
        }
        plan = self.agent.prepare(ACTOR, {"operations": [operation]}, self.operation_id, [], queries=queries)
        field = next(f for f in plan["fields"] if f["path"] == "fields")
        base_value = self.agent.public_plan(plan)["fields"][0]["value"]
        filled = {**base_value, "维修负责人": [{"id": "ou_1", "name": "张三"}]}

        # Fill the form -> awaiting_confirmation.
        confirmed = self.agent.amend(ACTOR, plan["id"],
                                     {"version": plan["version"], "values": {field["name"]: filled}})
        self.assertEqual(confirmed["status"], "awaiting_confirmation")

        # Review: first confirmation -> awaiting_second_confirmation.
        confirmed = await self.agent.confirm(ACTOR, confirmed["id"],
                                             {"version": confirmed["version"], "stage": "review"}, self.request)
        self.assertEqual(confirmed["status"], "awaiting_second_confirmation")

        # Return-to-edit: names AND ids from the previous answer must be retained.
        # The form value is frozen behind $query bindings, so resolve it against
        # the stored plan._queries instead of asserting on exact private form.
        edit_plan = self.agent.amend(ACTOR, confirmed["id"],
                                     {"action": "edit", "version": confirmed["version"]})
        self.assertEqual(edit_plan["status"], "needs_input")
        edit_field = next(f for f in edit_plan["fields"] if f["path"] == "fields")
        edit_value = edit_field["value"]
        edit_plan_raw = self.agent.get_plan(ACTOR, edit_plan["id"])
        resolved_edit = _result_refs(edit_value, [], edit_plan_raw.get("_references", {}), edit_plan_raw.get("_queries", {}))
        self.assertEqual(resolved_edit["维修负责人"], [{"id": "ou_1", "name": "张三"}])

        # Re-submit after editing; confirmations restart with review then execute.
        final = self.agent.amend(ACTOR, edit_plan["id"],
                                 {"version": edit_plan["version"], "values": {edit_field["name"]: {**edit_value, "维修负责人": [{"id": "ou_1", "name": "张三"}]}}})
        self.assertEqual(final["status"], "awaiting_confirmation")
        self.assertEqual(self.agent.get_plan(ACTOR, final["id"])["operations"][0]["body"]["fields"]["维修负责人"],
                         [{"id": "ou_1"}])
        # Fresh confirmations: review first, then execute. No write until both.
        final = await self.agent.confirm(ACTOR, final["id"], {"version": final["version"], "stage": "review"}, self.request)
        self.assertEqual(final["status"], "awaiting_second_confirmation")
        self.assertEqual(self.writes, [])
        final = await self.agent.confirm(ACTOR, final["id"], {"version": final["version"], "stage": "execute"}, self.request)
        await gather_tasks(self.agent)
        self.assertEqual(len(self.writes), 1)
        self.assertEqual(self.writes[0]["fields"]["维修负责人"], [{"id": "ou_1"}])
        self.assertEqual(self.writes[0]["expected_version"], "v")
        self.assertEqual(self.writes[0]["source_event_id"], "rec-event")
        self.assertEqual(self.writes[0]["source_repair_ids"], ["rec-repair"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
