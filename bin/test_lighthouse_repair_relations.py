"""Repair assistant fidelity: repair-management edit relations and form body.

Focused isolated tests using the real PortalAgent/catalog plus fake in-process
Pydantic-validated native endpoints.  Reuses only Store/ACTOR (not any test
class) from test_lighthouse_agent_workflows.  No real cloud/provider access,
no product edits.  Each test asserts a declared fidelity contract and reports
the actual outcome against the current build.
"""
import asyncio
import copy
import json
import sys
import tempfile
import unittest
from pathlib import Path
from urllib.parse import urlencode
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

from fastapi import FastAPI, Request

from clipflow_backend.api_models import (RepairFollowupRecordRequest,
                                         RepairManagementRecordRequest)

from lan_bitable_template_portal.lighthouse_agent import PortalAgent
from lan_bitable_template_portal.lighthouse_ai import (AssistantError,
                                                       LighthouseAssistant)
from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog
from lan_bitable_template_portal.lighthouse_files import LighthouseFiles
from lan_bitable_template_portal.portal_service import (REPAIR_FOLLOWUP_CMDB_FIELD_NAME,
                                                         REPAIR_FOLLOWUP_DEVICE_NAME_FIELD_NAME,
                                                         REPAIR_FOLLOWUP_DEVICE_NUMBER_FIELD_NAME,
                                                         REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME,
                                                         REPAIR_MANAGEMENT_TABLE_ID,
                                                         FieldMeta,
                                                         MaintenancePortalService)

from test_lighthouse_agent_workflows import ACTOR, Store

OPERATION_ID = "repair_relations_00000001"


def make_request(query_string=b"", path="/api/assistant/agent"):
    return Request({
        "type": "http",
        "scheme": "http",
        "server": ("testserver", 80),
        "client": ("127.0.0.1", 4567),
        "path": path,
        "root_path": "",
        "query_string": query_string,
        "headers": [(b"cookie", b"fixture=a"), (b"origin", b"http://testserver")],
    })


async def gather_tasks(agent):
    if agent.tasks:
        await asyncio.gather(*tuple(agent.tasks))


def options_request(q="", selected=None, scope="A", source_event_id=None):
    params = {}
    if q:
        params["q"] = q
    if selected is not None:
        params["selected"] = json.dumps(selected)
    if scope:
        params["scope"] = scope
    if source_event_id is not None:
        params["source_event_id"] = source_event_id
    return make_request(query_string=urlencode(params).encode())


class RepairRelationsBase(unittest.IsolatedAsyncioTestCase):
    """Shared fixture: assistant/files/agent + fake native repair endpoints."""

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

        self.app = FastAPI()
        self.project_writes = []
        self.followup_writes = []
        self.device_calls = []
        self.event_calls = []
        self.repair_calls = []

        @self.app.put("/api/repair-management/records/{record_id}")
        async def update_project(record_id: str, body: RepairManagementRecordRequest):
            self.project_writes.append(body.model_dump())
            return {"ok": True, "data": {"record_id": record_id}}

        @self.app.post("/api/repair-management/records")
        async def create_project(body: RepairManagementRecordRequest):
            self.project_writes.append(body.model_dump())
            return {"ok": True, "data": {"record_id": "rec-created-project"}}

        @self.app.put("/api/repair-management/followups/{record_id}")
        async def update_followup(record_id: str, body: RepairFollowupRecordRequest):
            self.followup_writes.append(body.model_dump())
            return {"ok": True, "data": {"record_id": record_id}}

        @self.app.post("/api/repair-management/followups")
        async def create_followup(body: RepairFollowupRecordRequest):
            self.followup_writes.append(body.model_dump())
            return {"ok": True, "data": {"record_id": "rec-created-followup"}}

        @self.app.get("/api/repair-management/cmdb-candidates")
        async def devices(scope: str, q: str = "", limit: int = 80):
            self.device_calls.append((scope, q))
            rows = [
                {"record_id": "rec-device-0", "name": "柴油发电机", "unique_id": "5.14.0"},
                {"record_id": "rec-device-1", "name": "UPS主机", "unique_id": "5.14.1"},
                {"record_id": "rec-device-old", "name": "旧设备", "unique_id": "5.14.9"},
            ]
            if q:
                rows = [row for row in rows if q in row["name"] or q in row["record_id"]]
            return {"ok": True, "data": {"records": rows, "has_more": False}}

        @self.app.get("/api/repair-management/event-candidates")
        async def events(scope: str, q: str = "", limit: int = 80):
            self.event_calls.append((scope, q))
            rows = [
                {"record_id": "rec-event", "name": "事件A_关联事件单",
                 "display_fields": {"事件简述": "事件A", "标题": "事件A单号"}},
                {"record_id": "rec-event-2", "name": "事件B", "display_fields": {"事件简述": "事件B"}},
            ]
            if q:
                rows = [row for row in rows if q in str(row.get("name")) or q in json.dumps(row.get("display_fields", {}), ensure_ascii=False)]
            return {"ok": True, "data": {"records": rows, "has_more": False}}

        @self.app.get("/api/repair-management/repair-candidates")
        async def repairs(scope: str, q: str = "", limit: int = 80, event_record_id: str = ""):
            self.repair_calls.append((scope, q, event_record_id))
            rows = [
                {"record_id": "rec-repair", "name": "检修通告A_检修通告名称",
                 "display_fields": {"检修通告名称": "检修A", "名称": "检修A名称"}},
                {"record_id": "rec-repair-2", "name": "检修通告B", "display_fields": {"检修通告名称": "检修B"}},
            ]
            if event_record_id and event_record_id not in {"rec-event", "rec-event-2"}:
                rows = []
            if q:
                rows = [row for row in rows if q in str(row.get("name")) or q in json.dumps(row.get("display_fields", {}), ensure_ascii=False)]
            return {"ok": True, "data": {"records": rows, "has_more": False}}

        self.catalog = PortalAPICatalog(self.app)
        self.agent = PortalAgent(self.assistant, self.catalog, self.files)
        self.request = make_request()

    # ---------- helpers ----------
    def _prepare(self, operation, queries=None, extra_fields=None):
        decision = {"operations": [operation]}
        if extra_fields:
            decision["fields"] = extra_fields
        return self.agent.prepare(ACTOR, decision, OPERATION_ID, [], queries=queries or {})

    def _project_queries(self, original, metas):
        return {"query_" + ("a" * 32): {"table_id": REPAIR_MANAGEMENT_TABLE_ID,
                                        "records": [original], "fields": metas}}

    def _followup_queries(self, original, metas, summary_record_id="rec-parent"):
        return {"query_" + ("b" * 32): {"summary_record_id": summary_record_id,
                                        "relation_mode": "record_id",
                                        "fields": metas, "records": [original]}}

    def _field(self, plan, path, idx=0):
        return next(f for f in plan["fields"]
                    if f.get("operation_index", 0) == idx and f.get("path") == path)

    def _public_form(self, plan, idx=0):
        return self.agent.public_plan(plan)["fields"][idx]["value"]


class RepairFidelityTests(RepairRelationsBase):
    """Contract-by-contract fidelity coverage."""

    # --- 1/2. form baseline display vs delta body submit ---------------------
    async def test_project_form_retains_full_baseline_but_submits_only_changed_editables(self):
        metas = [
            {"field_name": "故障发生时间", "field_type": 5, "editable": True, "options": []},
            {"field_name": "故障维修原因", "field_type": 1, "editable": True, "options": []},
            {"field_name": "所属专业", "field_type": 3, "editable": True, "options": ["电气", "暖通"]},
            {"field_name": "证据.说明", "field_type": 17, "editable": False, "options": []},
            {"field_name": "汇总公式", "field_type": 20, "editable": False, "options": []},
        ]
        original = {"record_id": "rec-project", "record_version": "v-original", "building_codes": ["A"],
                    "raw_fields": {"故障发生时间": 1790821800000, "故障维修原因": [{"text": "原原因"}],
                                   "所属专业": "电气", "证据.说明": {"content": "原证明", "file_token": "private-proof"},
                                   "汇总公式": "不编辑"},
                    "source_event_id": "rec-event", "source_repair_ids": ["rec-repair"]}
        plan = self._prepare({"api_id": "PUT /api/repair-management/records/{record_id}",
                              "path_params": {"record_id": "rec-project"},
                              "body": {"scope": "A"}},
                             queries=self._project_queries(original, metas))
        control = self._field(plan, "fields")
        form = self._public_form(plan)
        # Display/reopen must keep the full baseline (including readonly/evidence).
        for key in ("故障发生时间", "故障维修原因", "所属专业", "证据.说明", "汇总公式"):
            self.assertIn(key, form)
        # Submit only the actually changed EDITABLE key.
        value = dict(form)
        value["故障维修原因"] = "已更正"
        amended = self.agent.amend(ACTOR, plan["id"], {"version": plan["version"], "values": {control["name"]: value}})
        submitted = self.agent.get_plan(ACTOR, amended["id"])["operations"][0]["body"]["fields"]
        self.assertEqual(submitted, {"故障维修原因": "已更正"})

    async def test_explicit_model_override_sent_even_if_ui_accepts_unchanged(self):
        metas = [
            {"field_name": "设备型号", "field_type": 1, "editable": True, "options": []},
            {"field_name": "设备品牌", "field_type": 3, "editable": True, "options": ["双登", "华为"]},
        ]
        original = {"record_id": "rec-project", "record_version": "v1", "building_codes": ["A"],
                    "raw_fields": {"设备型号": "旧型号", "设备品牌": "双登"}}
        plan = self._prepare({"api_id": "PUT /api/repair-management/records/{record_id}",
                              "path_params": {"record_id": "rec-project"},
                              "body": {"scope": "A", "fields": {"设备型号": "新型号"}}},
                             queries=self._project_queries(original, metas))
        control = self._field(plan, "fields")
        # UI posts the current form back unchanged; the model override must survive.
        amended = self.agent.amend(ACTOR, plan["id"], {"version": plan["version"], "values": {control["name"]: self._public_form(plan)}})
        submitted = self.agent.get_plan(ACTOR, amended["id"])["operations"][0]["body"]["fields"]
        self.assertEqual(submitted.get("设备型号"), "新型号")

    # --- 3. optional clearing sends empty ------------------------------------
    async def test_optional_clearing_sends_empty(self):
        metas = [{"field_name": "维修进度", "field_type": 2, "editable": True, "options": []}]
        original = {"record_id": "rec-followup", "record_version": "fv1", "raw_fields": {"维修进度": 0.5},
                    "cmdb_record_ids": ["rec-device-0"]}
        plan = self._prepare({"api_id": "PUT /api/repair-management/followups/{record_id}",
                              "path_params": {"record_id": "rec-followup"},
                              "body": {"scope": "A", "summary_record_id": "rec-parent", "cmdb_record_ids": ["rec-device-0"]}},
                             queries=self._followup_queries(original, metas))
        cmdb = self._field(plan, "cmdb_record_ids")
        amended = self.agent.amend(ACTOR, plan["id"], {"version": plan["version"], "values": {cmdb["name"]: []}})
        self.assertEqual(self.agent.get_plan(ACTOR, amended["id"])["operations"][0]["body"]["cmdb_record_ids"], [])

        # Project repair-link clearing via POST (no seeded original) so the field is editable.
        plan2 = self._prepare({"api_id": "POST /api/repair-management/records",
                               "body": {"scope": "A"}},
                              extra_fields=[{"name": "clear_repairs", "path": "source_repair_ids",
                                             "type": "multiselect", "required": False}])
        repair_field = next(f for f in plan2["fields"] if f["name"] == "clear_repairs")
        amended2 = self.agent.amend(ACTOR, plan2["id"], {"version": plan2["version"], "values": {repair_field["name"]: []}})
        self.assertEqual(self.agent.get_plan(ACTOR, amended2["id"])["operations"][0]["body"]["source_repair_ids"], [])

    # --- 4. required / readonly tampering still fails -------------------------
    async def test_required_and_readonly_tampering_still_fails(self):
        metas = [
            {"field_name": "故障维修原因", "field_type": 1, "editable": True, "required": True, "options": []},
            {"field_name": "所属专业", "field_type": 3, "editable": True, "options": ["电气"]},
            {"field_name": "证据.说明", "field_type": 17, "editable": False, "options": []},
        ]
        original = {"record_id": "rec-project", "record_version": "v1", "building_codes": ["A"],
                    "raw_fields": {"故障维修原因": [{"text": "原原因"}], "所属专业": "电气", "证据.说明": {"content": "原证明"}}}
        plan = self._prepare({"api_id": "PUT /api/repair-management/records/{record_id}",
                              "path_params": {"record_id": "rec-project"},
                              "body": {"scope": "A"}},
                             queries=self._project_queries(original, metas))
        control = self._field(plan, "fields")
        form = self._public_form(plan)

        tampered = dict(form)
        tampered["证据.说明"] = {"content": "伪造证明"}
        with self.assertRaises(AssistantError):
            self.agent.amend(ACTOR, plan["id"], {"version": plan["version"], "values": {control["name"]: tampered}})

        cleared = dict(form)
        cleared["故障维修原因"] = None
        with self.assertRaises(AssistantError):
            self.agent.amend(ACTOR, plan["id"], {"version": plan["version"], "values": {control["name"]: cleared}})

        forged = dict(form)
        forged["所属专业"] = "编造专业"
        with self.assertRaises(AssistantError):
            self.agent.amend(ACTOR, plan["id"], {"version": plan["version"], "values": {control["name"]: forged}})

    # --- 5. existing project relations auto-expose + preselect with labels ----
    async def test_project_relations_auto_expose_preselect_with_labels(self):
        metas = [{"field_name": "所属专业", "field_type": 3, "editable": True, "options": ["电气"]}]
        original = {"record_id": "rec-project", "record_version": "v1", "building_codes": ["A"],
                    "raw_fields": {"所属专业": "电气"},
                    "display_fields": {"关联事件单": "事件A_关联事件单", "检修通告名称": "检修A"},
                    "source_event_id": "rec-event", "source_repair_ids": ["rec-repair"]}
        plan = self._prepare({"api_id": "PUT /api/repair-management/records/{record_id}",
                              "path_params": {"record_id": "rec-project"},
                              "body": {"scope": "A"}},
                             queries=self._project_queries(original, metas))
        paths = {f["path"] for f in plan["fields"]}
        self.assertIn("source_event_id", paths, "PUT project should auto-expose 关联事件 control")
        self.assertIn("source_repair_ids", paths, "PUT project should auto-expose 检修通告 control")
        event_field = self._field(plan, "source_event_id")
        repair_field = self._field(plan, "source_repair_ids")
        self.assertEqual(event_field["type"], "select")
        self.assertEqual(repair_field["type"], "multiselect")
        self.assertEqual(repair_field.get("value"), ["rec-repair"])
        # The preselected option label must come from the original 检修通告名称 display field.
        preset = {opt["value"]: opt.get("label", "") for opt in repair_field["options"]}
        self.assertEqual(preset.get("rec-repair"), "检修A")
        # Loading repair candidates must retain the original selection.
        await self.agent.field_options(ACTOR, plan["id"], repair_field["name"], options_request())
        repair_field = self._field(self.agent.get_plan(ACTOR, plan["id"]), "source_repair_ids")
        values = {opt["value"] for opt in repair_field["options"]}
        self.assertIn("rec-repair", values)

    # --- 6. max-one repair relation enforced ---------------------------------
    async def test_max_one_repair_relation_enforced(self):
        plan = self._prepare({"api_id": "POST /api/repair-management/records",
                              "body": {"scope": "A", "source_event_id": "rec-event"}},
                             extra_fields=[{"name": "repair_ids", "path": "source_repair_ids",
                                            "type": "multiselect", "required": False}])
        field = next(f for f in plan["fields"] if f["name"] == "repair_ids")
        await self.agent.field_options(ACTOR, plan["id"], field["name"], options_request())
        raw = self.agent.get_plan(ACTOR, plan["id"])
        field = self._field(raw, "source_repair_ids")
        self.assertEqual(field.get("maxItems"), 1)
        choices = [opt["value"] for opt in field["options"]][:2]
        with self.assertRaises(AssistantError):
            self.agent.amend(ACTOR, plan["id"], {"version": raw["version"], "values": {field["name"]: choices}})

    # --- 7. followup cmdb preselect + parent fixed on PUT --------------------
    async def test_followup_cmdb_preselect_and_parent_fixed_on_put(self):
        metas = [{"field_name": "维修进度", "field_type": 2, "editable": True, "options": []}]
        original = {"record_id": "rec-followup", "record_version": "fv1",
                    "raw_fields": {"维修进度": 0.5}, "cmdb_record_ids": ["rec-device-0"]}
        plan = self._prepare({"api_id": "PUT /api/repair-management/followups/{record_id}",
                              "path_params": {"record_id": "rec-followup"},
                              "body": {"scope": "A", "summary_record_id": "rec-parent", "cmdb_record_ids": ["rec-device-0"]}},
                             queries=self._followup_queries(original, metas))
        paths = {f["path"] for f in plan["fields"]}
        self.assertIn("cmdb_record_ids", paths, "followup PUT should auto-expose CMDB multiselect")
        cmdb = self._field(plan, "cmdb_record_ids")
        self.assertEqual(cmdb["type"], "multiselect")
        self.assertEqual(cmdb.get("value"), ["rec-device-0"])
        # Loading candidates retains the original selected device with a label.
        await self.agent.field_options(ACTOR, plan["id"], cmdb["name"], options_request())
        cmdb = self._field(self.agent.get_plan(ACTOR, plan["id"]), "cmdb_record_ids")
        values = {opt["value"] for opt in cmdb["options"]}
        self.assertIn("rec-device-0", values)
        # Parent summary_record_id is fixed on PUT: no editable field, body unchanged.
        self.assertNotIn("summary_record_id", [f["path"] for f in plan["fields"]])
        raw = self.agent.get_plan(ACTOR, plan["id"])
        self.assertEqual(raw["operations"][0]["body"]["summary_record_id"], "rec-parent")

    # --- 8. CMDB search native + no unchanged old names resubmitted -----------
    async def test_cmdb_search_native_retains_selected_and_skips_unchanged_names(self):
        metas = [
            {"field_name": "维修进度", "field_type": 2, "editable": True, "options": []},
            {"field_name": "设备名称", "field_type": 1, "editable": True, "options": []},
            {"field_name": "设备编号", "field_type": 1, "editable": True, "options": []},
        ]
        original = {"record_id": "rec-followup", "record_version": "fv1",
                    "raw_fields": {"维修进度": 0.5, "设备名称": "旧设备", "设备编号": "OLD-1"},
                    "cmdb_record_ids": ["rec-device-old"]}
        plan = self._prepare({"api_id": "PUT /api/repair-management/followups/{record_id}",
                              "path_params": {"record_id": "rec-followup"},
                              "body": {"scope": "A", "summary_record_id": "rec-parent", "cmdb_record_ids": ["rec-device-old"]}},
                             queries=self._followup_queries(original, metas))
        cmdb = self._field(plan, "cmdb_record_ids")
        # Full load first so options/known set exists, then a native filtered search.
        await self.agent.field_options(ACTOR, plan["id"], cmdb["name"], options_request())
        before = list(self.device_calls)
        await self.agent.field_options(ACTOR, plan["id"], cmdb["name"], options_request(q="device", selected=["rec-device-old"]))
        self.assertEqual(self.device_calls, before + [("A", "device")])
        raw = self.agent.get_plan(ACTOR, plan["id"])
        cmdb = self._field(raw, "cmdb_record_ids")
        values = {opt["value"] for opt in cmdb["options"]}
        self.assertIn("rec-device-old", values)
        self.assertIn("rec-device-0", values)
        # Now select a new device; unchanged old 设备名称/设备编号 must NOT be re-sent.
        fields_control = self._field(raw, "fields")
        fields_value = self._public_form(raw)
        amended = self.agent.amend(ACTOR, plan["id"], {"version": raw["version"], "values": {
            cmdb["name"]: ["rec-device-0"], fields_control["name"]: fields_value}})
        submitted = self.agent.get_plan(ACTOR, amended["id"])["operations"][0]["body"]
        self.assertEqual(submitted["cmdb_record_ids"], ["rec-device-0"])
        self.assertNotIn("旧设备", submitted["fields"].get("设备名称", ""))
        self.assertNotIn("OLD-1", submitted["fields"].get("设备编号", ""))

    # --- 9. clearing CMDB -> empty relation + cleared names -------------------
    async def test_clear_cmdb_submits_empty_and_clears_names(self):
        metas = [
            {"field_name": "维修进度", "field_type": 2, "editable": True, "options": []},
            {"field_name": "设备名称", "field_type": 1, "editable": True, "options": []},
            {"field_name": "设备编号", "field_type": 1, "editable": True, "options": []},
        ]
        original = {"record_id": "rec-followup", "record_version": "fv1",
                    "raw_fields": {"维修进度": 0.5, "设备名称": "旧设备", "设备编号": "OLD-1"},
                    "cmdb_record_ids": ["rec-device-old"]}
        plan = self._prepare({"api_id": "PUT /api/repair-management/followups/{record_id}",
                              "path_params": {"record_id": "rec-followup"},
                              "body": {"scope": "A", "summary_record_id": "rec-parent", "cmdb_record_ids": ["rec-device-old"]}},
                             queries=self._followup_queries(original, metas))
        cmdb = self._field(plan, "cmdb_record_ids")
        fields_control = self._field(plan, "fields")
        amended = self.agent.amend(ACTOR, plan["id"], {"version": plan["version"], "values": {
            cmdb["name"]: [], fields_control["name"]: self._public_form(plan)}})
        submitted = self.agent.get_plan(ACTOR, amended["id"])["operations"][0]["body"]
        self.assertEqual(submitted["cmdb_record_ids"], [])
        self.assertEqual(submitted["fields"].get("设备名称"), "")
        self.assertEqual(submitted["fields"].get("设备编号"), "")

    # --- 10. changed event removes previous repair link ------------------------
    async def test_changed_event_removes_previous_repair_link(self):
        metas = [{"field_name": "所属专业", "field_type": 3, "editable": True, "options": ["电气"]}]
        original = {"record_id": "rec-project", "record_version": "v1", "building_codes": ["A"],
                    "raw_fields": {"所属专业": "电气"},
                    "source_event_id": "rec-event", "source_repair_ids": ["rec-repair"]}
        plan = self._prepare({"api_id": "PUT /api/repair-management/records/{record_id}",
                              "path_params": {"record_id": "rec-project"},
                              "body": {"scope": "A"}},
                             queries=self._project_queries(original, metas))
        paths = {f["path"] for f in plan["fields"]}
        self.assertIn("source_event_id", paths, "project PUT should expose the event control to change it")
        event_field = self._field(plan, "source_event_id")
        await self.agent.field_options(ACTOR, plan["id"], event_field["name"], options_request())
        raw = self.agent.get_plan(ACTOR, plan["id"])
        event_field = self._field(raw, "source_event_id")
        amended = self.agent.amend(ACTOR, plan["id"], {"version": raw["version"], "values": {event_field["name"]: "rec-event-2"}})
        submitted = self.agent.get_plan(ACTOR, amended["id"])["operations"][0]["body"]
        self.assertEqual(submitted["source_event_id"], "rec-event-2")
        self.assertEqual(submitted["source_repair_ids"], [])
        self.assertTrue(submitted["replace_source_relations"])

    # --- 11. options accept source_event_id (event-scoped repair candidates) ---
    async def test_options_accept_source_event_and_scope_repair_candidates(self):
        plan = self._prepare({"api_id": "POST /api/repair-management/records",
                              "body": {"scope": "A", "source_event_id": "rec-event"}},
                             extra_fields=[
                                 {"name": "event_link", "path": "source_event_id", "type": "select", "options_source": "repair_events", "required": False},
                                 {"name": "repairs", "path": "source_repair_ids", "type": "multiselect", "required": False},
                             ])
        repairs = next(f for f in plan["fields"] if f["name"] == "repairs")
        await self.agent.field_options(ACTOR, plan["id"], repairs["name"], options_request())
        raw = self.agent.get_plan(ACTOR, plan["id"])
        self.assertEqual(self.repair_calls[-1][2], "rec-event")
        opts = {opt["value"] for opt in self._field(raw, "source_repair_ids")["options"]}
        self.assertIn("rec-repair", opts)

        # 1) Load event options so the changed event rec-event-2 is a known option.
        event_field = self._field(raw, "source_event_id")
        await self.agent.field_options(ACTOR, plan["id"], event_field["name"], options_request())
        raw = self.agent.get_plan(ACTOR, plan["id"])
        event_field = self._field(raw, "source_event_id")
        # 2) Repair options with the changed event (query-param event2, cleared selection):
        #    native repair-candidates must receive event2.
        repairs = self._field(raw, "source_repair_ids")
        await self.agent.field_options(ACTOR, plan["id"], repairs["name"],
                                       options_request(source_event_id="rec-event-2", selected=[]))
        raw = self.agent.get_plan(ACTOR, plan["id"])
        self.assertEqual(self.repair_calls[-1][2], "rec-event-2")
        opts = {opt["value"] for opt in self._field(raw, "source_repair_ids")["options"]}
        self.assertIn("rec-repair-2", opts)
        repairs = self._field(raw, "source_repair_ids")
        event_field = self._field(raw, "source_event_id")
        # 3) A forged/unread event is still denied even with a cleared selection.
        with self.assertRaises(AssistantError):
            await self.agent.field_options(ACTOR, plan["id"], repairs["name"],
                                           options_request(source_event_id="rec-forged", selected=[]))
        # 4) Old-event options while the plan now targets event2 + a new repair are denied.
        with self.assertRaises(AssistantError):
            await self.agent.field_options(ACTOR, plan["id"], repairs["name"],
                                           options_request(source_event_id="rec-event", selected=["rec-repair-2"]))
        # 5) Pick repair2 and amend event2+repair2 together: must succeed.
        amended_both = self.agent.amend(ACTOR, plan["id"], {"version": raw["version"], "values": {
            event_field["name"]: "rec-event-2", repairs["name"]: ["rec-repair-2"]}})
        submitted = self.agent.get_plan(ACTOR, amended_both["id"])["operations"][0]["body"]
        self.assertEqual(submitted["source_event_id"], "rec-event-2")
        self.assertEqual(submitted["source_repair_ids"], ["rec-repair-2"])

    # --- 12. confirm flow: no writes before confirm, write after --------------
    async def test_confirm_flow_no_write_before_confirm_and_write_after(self):
        metas = [{"field_name": "故障维修原因", "field_type": 1, "editable": True, "options": []}]
        original = {"record_id": "rec-project", "record_version": "v1", "building_codes": ["A"],
                    "raw_fields": {"故障维修原因": [{"text": "原原因"}]},
                    "source_event_id": "rec-event", "source_repair_ids": []}
        plan = self._prepare({"api_id": "PUT /api/repair-management/records/{record_id}",
                              "path_params": {"record_id": "rec-project"},
                              "body": {"scope": "A"}},
                             queries=self._project_queries(original, metas))
        control = self._field(plan, "fields")
        value = dict(self._public_form(plan))
        value["故障维修原因"] = "已更正"
        amended = self.agent.amend(ACTOR, plan["id"], {"version": plan["version"], "values": {control["name"]: value}})
        self.assertEqual(self.project_writes, [], "no write should happen before confirm")
        reviewed = await self.agent.confirm(ACTOR, plan["id"], {"version": amended["version"], "stage": "review"}, self.request)
        if reviewed["status"] == "awaiting_second_confirmation":
            await self.agent.confirm(ACTOR, plan["id"], {"version": reviewed["version"], "stage": "execute"}, self.request)
        await gather_tasks(self.agent)
        self.assertEqual(len(self.project_writes), 1)
        finished = self.agent.get_plan(ACTOR, plan["id"])
        self.assertEqual(finished["status"], "completed", finished.get("error"))

    # --- 13. return-edit keeps full old + modified values and names -----------
    async def test_return_edit_retains_full_values_and_names(self):
        metas = [
            {"field_name": "故障维修原因", "field_type": 1, "editable": True, "options": []},
            {"field_name": "所属专业", "field_type": 3, "editable": True, "options": ["电气", "暖通"]},
        ]
        original = {"record_id": "rec-project", "record_version": "v1", "building_codes": ["A"],
                    "raw_fields": {"故障维修原因": [{"text": "原原因"}], "所属专业": "电气"}}
        plan = self._prepare({"api_id": "PUT /api/repair-management/records/{record_id}",
                              "path_params": {"record_id": "rec-project"},
                              "body": {"scope": "A"}},
                             queries=self._project_queries(original, metas))
        control = self._field(plan, "fields")
        value = dict(self._public_form(plan))
        value["故障维修原因"] = "已更正"
        amended = self.agent.amend(ACTOR, plan["id"], {"version": plan["version"], "values": {control["name"]: value}})
        # Return to the edit form: it must retain full old values plus the edit.
        returned = self.agent.amend(ACTOR, plan["id"], {"action": "edit", "version": amended["version"]})
        returned_form = next(field for field in returned["fields"] if field.get("native_repair"))
        self.assertIn("故障维修原因", returned_form["dirty_fields"])
        self.assertNotIn("所属专业", returned_form["dirty_fields"])
        form = self._public_form(returned)
        self.assertEqual(form.get("故障维修原因"), "已更正")
        self.assertEqual(form.get("所属专业"), "电气")
        # Re-submit and confirm to prove names preserved end to end.
        fields_control = self._field(returned, "fields")
        amended2 = self.agent.amend(ACTOR, plan["id"], {"version": returned["version"], "values": {fields_control["name"]: form}})
        reviewed = await self.agent.confirm(ACTOR, plan["id"], {"version": amended2["version"], "stage": "review"}, self.request)
        if reviewed["status"] == "awaiting_second_confirmation":
            await self.agent.confirm(ACTOR, plan["id"], {"version": reviewed["version"], "stage": "execute"}, self.request)
        await gather_tasks(self.agent)
        # The write is a delta body: only the changed editable key is submitted,
        # while the form above retained the full old+modified values/names.
        self.assertEqual(self.project_writes[0]["fields"], {"故障维修原因": "已更正"})

    # --- 14. REAL backend prep replaces old CMDB names without losing manual edits ---
    async def test_real_prepare_repair_followup_fields_replaces_old_cmdb_with_new_names(self):
        metas = [
            {"field_name": "维修进度", "field_type": 2, "editable": True, "options": []},
            {"field_name": "设备名称", "field_type": 1, "editable": True, "options": []},
            {"field_name": "设备编号", "field_type": 1, "editable": True, "options": []},
            {"field_name": "维修进展描述", "field_type": 1, "editable": True, "options": []},
        ]
        original = {"record_id": "rec-followup", "record_version": "fv1",
                    "raw_fields": {"维修进度": 0.5, "设备名称": "旧设备", "设备编号": "OLD-1",
                                   "维修进展描述": "已检查"},
                    "cmdb_record_ids": ["rec-device-old"]}

        def make_service():
            # Feed the raw assistant delta through the REAL backend prep, mocking
            # ONLY the two external readers.  The service is created via __new__ so
            # no cloud client is constructed.
            service = MaintenancePortalService.__new__(MaintenancePortalService)
            meta_by_name = {
                REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME: FieldMeta("fld_parent", REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME, "Text", 1, False, {}, [], False),
                REPAIR_FOLLOWUP_CMDB_FIELD_NAME: FieldMeta("fld_cmdb", REPAIR_FOLLOWUP_CMDB_FIELD_NAME, "Link", 18, False, {}, [], False),
                "设备名称": FieldMeta("fld_name", "设备名称", "Text", 1, False, {}, [], False),
                "设备编号": FieldMeta("fld_num", "设备编号", "Text", 1, False, {}, [], False),
                "维修进展描述": FieldMeta("fld_desc", "维修进展描述", "Text", 1, False, {}, [], False),
            }
            cmdb_meta_by_name = {
                "设备名称": FieldMeta("fld_name", "设备名称", "Text", 1, False, {}, [], False),
                "设备编号": FieldMeta("fld_num", "设备编号", "Text", 1, False, {}, [], False),
            }
            service._load_repair_management_cmdb_records = lambda: ({}, cmdb_meta_by_name, [])
            service._load_table_records_by_ids = lambda **kwargs: [
                {"record_id": "rec-device-0", "display_fields": {"设备名称": "柴油发电机", "设备编号": "5.14.0"}},
            ]
            return service, meta_by_name

        # ---- First path: the assistant leaves 设备名称/设备编号 untouched in the form
        # and only changes the CMDB ids plus a manual 维修进展描述.  The delta body must
        # therefore omit both device keys, and REAL native prep must autofill BOTH name
        # and number from the selected device NAME (not the unique_id).
        plan = self._prepare({"api_id": "PUT /api/repair-management/followups/{record_id}",
                              "path_params": {"record_id": "rec-followup"},
                              "body": {"scope": "A", "summary_record_id": "rec-parent",
                                       "cmdb_record_ids": ["rec-device-old"]}},
                             queries=self._followup_queries(original, metas))
        cmdb = self._field(plan, "cmdb_record_ids")
        # Full load so the known option set exists, then a native filtered search.
        await self.agent.field_options(ACTOR, plan["id"], cmdb["name"], options_request())
        before = list(self.device_calls)
        await self.agent.field_options(ACTOR, plan["id"], cmdb["name"],
                                       options_request(q="device", selected=["rec-device-old"]))
        self.assertEqual(self.device_calls, before + [("A", "device")])
        raw = self.agent.get_plan(ACTOR, plan["id"])
        cmdb = self._field(raw, "cmdb_record_ids")
        fields_control = self._field(raw, "fields")
        fields_value = self._public_form(raw)
        # Keep 设备名称/设备编号 untouched (still "旧设备"/"OLD-1"); only change CMDB ids
        # and add a manual progress note.  This proves the assistant lets native prep
        # autofill the new device names instead of passing them through explicitly.
        fields_value["维修进展描述"] = "已更换零件"
        amended = self.agent.amend(ACTOR, plan["id"], {"version": raw["version"], "values": {
            cmdb["name"]: ["rec-device-0"], fields_control["name"]: fields_value}})
        submitted = self.agent.get_plan(ACTOR, amended["id"])["operations"][0]["body"]
        self.assertEqual(submitted["cmdb_record_ids"], ["rec-device-0"])
        # Assistant delta must NOT contain the device keys at all.
        self.assertNotIn("设备名称", submitted["fields"])
        self.assertNotIn("设备编号", submitted["fields"])
        self.assertEqual(submitted["fields"].get("维修进展描述"), "已更换零件")

        service, meta_by_name = make_service()
        prepared, warnings = service._prepare_repair_followup_fields(
            summary_record_id="rec-parent",
            fields=dict(submitted["fields"]),
            cmdb_record_ids=submitted["cmdb_record_ids"],
            meta_by_name=meta_by_name,
            existing_record=original,
        )
        self.assertEqual(prepared[REPAIR_FOLLOWUP_CMDB_FIELD_NAME], ["rec-device-0"])
        # Native current behavior fills BOTH name and number from the selected device
        # NAME, not the unique_id, so both become "柴油发电机".
        self.assertEqual(prepared[REPAIR_FOLLOWUP_DEVICE_NAME_FIELD_NAME], "柴油发电机")
        self.assertEqual(prepared[REPAIR_FOLLOWUP_DEVICE_NUMBER_FIELD_NAME], "柴油发电机")
        # The manual progress edit survives unchanged.
        self.assertEqual(prepared["维修进展描述"], "已更换零件")
        self.assertNotIn("旧设备", prepared.get(REPAIR_FOLLOWUP_DEVICE_NAME_FIELD_NAME, ""))
        self.assertNotIn("OLD-1", prepared.get(REPAIR_FOLLOWUP_DEVICE_NUMBER_FIELD_NAME, ""))

        # ---- Second path: an EXPLICIT manual override of 设备名称/设备编号 must remain in
        # the resulting assistant delta AND in native prep (native prep honors submitted
        # names instead of autofilling).  No autofill is faked here.
        plan2 = self._prepare({"api_id": "PUT /api/repair-management/followups/{record_id}",
                               "path_params": {"record_id": "rec-followup"},
                               "body": {"scope": "A", "summary_record_id": "rec-parent",
                                        "cmdb_record_ids": ["rec-device-old"]}},
                              queries=self._followup_queries(original, metas))
        cmdb2 = self._field(plan2, "cmdb_record_ids")
        await self.agent.field_options(ACTOR, plan2["id"], cmdb2["name"], options_request())
        raw2 = self.agent.get_plan(ACTOR, plan2["id"])
        cmdb2 = self._field(raw2, "cmdb_record_ids")
        fields_control2 = self._field(raw2, "fields")
        fields_value2 = self._public_form(raw2)
        # Explicit manual override (distinct from the CMDB display name/unique_id).
        fields_value2["设备名称"] = "手动设备名"
        fields_value2["设备编号"] = "手动编号"
        fields_value2["维修进展描述"] = "已更换零件"
        amended2 = self.agent.amend(ACTOR, plan2["id"], {"version": raw2["version"], "values": {
            cmdb2["name"]: ["rec-device-0"], fields_control2["name"]: fields_value2}})
        submitted2 = self.agent.get_plan(ACTOR, amended2["id"])["operations"][0]["body"]
        self.assertEqual(submitted2["cmdb_record_ids"], ["rec-device-0"])
        self.assertEqual(submitted2["fields"].get("设备名称"), "手动设备名")
        self.assertEqual(submitted2["fields"].get("设备编号"), "手动编号")
        self.assertEqual(submitted2["fields"].get("维修进展描述"), "已更换零件")

        service2, meta_by_name2 = make_service()
        prepared2, _ = service2._prepare_repair_followup_fields(
            summary_record_id="rec-parent",
            fields=dict(submitted2["fields"]),
            cmdb_record_ids=submitted2["cmdb_record_ids"],
            meta_by_name=meta_by_name2,
            existing_record=original,
        )
        self.assertEqual(prepared2[REPAIR_FOLLOWUP_CMDB_FIELD_NAME], ["rec-device-0"])
        # Submitted names are honored; native prep does NOT overwrite the manual override.
        self.assertEqual(prepared2[REPAIR_FOLLOWUP_DEVICE_NAME_FIELD_NAME], "手动设备名")
        self.assertEqual(prepared2[REPAIR_FOLLOWUP_DEVICE_NUMBER_FIELD_NAME], "手动编号")
        self.assertEqual(prepared2["维修进展描述"], "已更换零件")


if __name__ == "__main__":
    unittest.main(verbosity=2)
