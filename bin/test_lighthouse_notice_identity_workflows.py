"""Original notice binding requests through the assistant; fake business API only."""
import asyncio
import copy
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock
from urllib.parse import urlencode

sys.path.insert(0, str(Path(__file__).resolve().parent))

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from clipflow_backend.api_models import NoticeIdentityBindRequest, NoticeTargetLookupRequest
from lan_bitable_template_portal.lighthouse_agent import PortalAgent, PLAN_NAMESPACE
from lan_bitable_template_portal.lighthouse_ai import LighthouseAssistant, AssistantError
from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog
from lan_bitable_template_portal.lighthouse_files import LighthouseFiles
from test_lighthouse_agent_workflows import Store, ACTOR

BIND = "POST /api/notice-identity/bind"
PLAN = {"record_id": "rec-plan-a", "source_record_id": "rec-plan-a", "scope": "A", "work_type": "maintenance",
        "title": "EA118机房A楼配电维护", "reason": "年度维护", "status": "未开始", "start_time": "2026-10-03 09:00", "end_time": "2026-10-03 11:00"}
ONGOING = {**PLAN, "record_id": "rec-target-a", "target_record_id": "rec-target-a", "active_item_id": "active-a", "status": "开始"}
TARGET = {"record_id": "rec-target-new", "target_record_id": "rec-target-new", "scope": "A", "building_codes": ["A"],
          "work_type": "maintenance", "title": "A楼目标维护", "status": "开始", "target_active": True, "target_finished": False}


class NoticeIdentityWorkflowTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        store = Store(Path(temp.name) / "state.sqlite3")
        assistant = LighthouseAssistant(store, Mock(side_effect=AssertionError("No data scraping")))
        self.writes, self.lookups, self.source_reads = [], [], []
        self.targets = [TARGET, {**TARGET, "target_record_id": "rec-ended", "record_id": "rec-ended", "title": "A楼已结束维护", "status": "结束", "target_finished": True, "target_active": False},
                        {**TARGET, "target_record_id": "rec-other-building", "scope": "B", "building_codes": ["B"]}]
        self.sources = [{"source_record_id": "rec-source-new", "title": "A楼新计划", "building": "A楼", "progress": "未开始"}]
        self.on_read = None
        self.read_error = False
        self.bind_error = False
        self.ongoing = [copy.deepcopy(ONGOING), copy.deepcopy(TARGET)]
        app = FastAPI()

        @app.get("/api/workbench")
        async def ongoing(scope: str = 'A', work_type: str = 'maintenance', sections: str = 'ongoing'):
            return {"ok": True, "data": {"ongoing": self.ongoing}}

        @app.post("/api/notice-identity/bind")
        async def bind(body: NoticeIdentityBindRequest):
            self.writes.append(body.model_dump())
            if self.bind_error:
                return JSONResponse({"ok": False, "error": "所选目标已结束，请重新选择"}, status_code=409)
            return {"ok": True, "data": {"source_record_id": body.source_record_id, "target_record_id": body.target_record_id}}

        @app.post("/api/notice-target-candidates")
        async def targets(body: NoticeTargetLookupRequest):
            self.lookups.append(body.model_dump())
            if self.on_read:
                self.on_read()
            if self.read_error:
                return JSONResponse({"ok": False, "error": "候选读取失败"}, status_code=503)
            return {"ok": True, "data": {"candidates": self.targets}}

        @app.get("/api/workbench/source-options")
        async def sources(request: Request):
            self.source_reads.append(dict(request.query_params))
            return {"ok": True, "data": {"items": self.sources}}

        @app.post("/api/workbench-actions")
        async def notice(request: Request):
            raise AssertionError("Binding must never send a notice")

        self.agent = PortalAgent(assistant, PortalAPICatalog(app), LighthouseFiles(store))
        self.store = store
        self.request = self.request_for()

    @staticmethod
    def request_for(**query):
        return Request({"type": "http", "scheme": "http", "server": ("testserver", 80), "client": ("127.0.0.1", 1),
                        "path": "/api/assistant/plans/test/options", "root_path": "", "query_string": urlencode(query).encode(),
                        "headers": [(b"origin", b"http://testserver"), (b"cookie", b"fixture=a")]})

    def prepare(self, mode="planned", **extra):
        body = {"scope": "A", "work_type": "maintenance", "binding_context": "planned" if mode == "planned" else "ongoing"}
        body.update({"source_record_id": PLAN["record_id"]} if mode == "planned" else {"active_item_id": ONGOING["active_item_id"], "target_record_id": ONGOING["target_record_id"], "source_binding_only": mode == "source"})
        if mode == "source":
            body["source_month"] = "9月"
        body.update(extra)
        return self.agent.prepare(ACTOR, {"operations": [{"api_id": BIND, "body": body}]}, "bind-" + mode, [],
                                  queries={"q": {"records": [PLAN], "ongoing": [ONGOING]}})

    @staticmethod
    def selector(plan):
        return next(field for field in plan["fields"] if field.get("native_notice_identity") and field.get("options_source"))

    async def load(self, plan, **params):
        return await self.agent.field_options(ACTOR, plan["id"], self.selector(plan)["name"], self.request_for(**params))

    async def execute(self, ready):
        first = await self.agent.confirm(ACTOR, ready["id"], {"version": ready["version"], "stage": "review"}, self.request)
        self.assertEqual(first["status"], "awaiting_second_confirmation")
        await self.agent.confirm(ACTOR, ready["id"], {"version": first["version"], "stage": "execute"}, self.request)
        await self.agent.confirm(ACTOR, ready["id"], {"version": first["version"], "stage": "execute"}, self.request)
        if self.agent.tasks:
            await asyncio.gather(*tuple(self.agent.tasks))
        return self.agent.get_plan(ACTOR, ready["id"])

    async def test_three_native_binding_modes_write_once_only_after_confirmation(self):
        for mode in ("planned", "target", "source"):
            with self.subTest(mode=mode):
                self.writes.clear()
                plan = self.prepare(mode, title="模型改写不应保存")
                loaded = await self.load(plan)
                field = self.selector(loaded)
                self.assertTrue(field.get("required"), "candidate selection must be visibly required")
                self.assertIn(PLAN["title"], field["question_text"])
                self.assertEqual(self.writes, [])
                selected = "rec-source-new" if mode == "source" else "rec-target-new"
                self.assertIn(selected, [option["value"] for option in field["options"]])
                ready = self.agent.amend(ACTOR, plan["id"], {"version": loaded["version"], "values": {field["name"]: selected}})
                self.assertEqual(self.writes, [])
                self.assertEqual(ready["operations"][0]["selected_labels"]["original_notice"], PLAN["title"])
                completed = await self.execute(ready)
                self.assertEqual(completed["status"], "completed", completed)
                self.assertEqual(len(self.writes), 1)
                body = self.writes[0]
                self.assertEqual(body["title"], PLAN["title"])
                self.assertEqual(body["reason"], PLAN["reason"])
                self.assertEqual(body["source_record_id"], selected if mode == "source" else PLAN["record_id"])
                self.assertEqual(body["target_record_id"], ONGOING["target_record_id"] if mode == "source" else selected)
                self.assertEqual(body["active_item_id"], "" if mode == "planned" else ONGOING["active_item_id"])
                if mode == "source":
                    self.assertEqual(self.source_reads[-1]["month"], "9月")
                    self.assertEqual(body["source_month"], "9月")
                else:
                    self.assertEqual(body["record_id"], selected)
                    self.assertEqual(self.lookups[-1]["lookup_context"], "planned_target_table" if mode == "planned" else "target_table")

    async def test_ended_target_is_not_an_ongoing_binding_candidate(self):
        planned = await self.load(self.prepare())
        ongoing = await self.load(self.prepare("target"))
        self.assertNotIn("rec-ended", [option["value"] for option in self.selector(planned)["options"]])
        self.assertNotIn("rec-ended", [option["value"] for option in self.selector(ongoing)["options"]])
        self.assertNotIn("rec-other-building", [option["value"] for option in self.selector(planned)["options"]])
        self.assertEqual(self.writes, [])

    async def test_month_change_requires_reloading_and_retains_only_same_month_selection(self):
        plan = self.prepare("source")
        loaded = await self.load(plan)
        values = {"step0.source_record_id": "rec-source-new", "step0.source_month": "10月"}
        with self.assertRaisesRegex(AssistantError, "月份已变化"):
            self.agent.amend(ACTOR, plan["id"], {"version": loaded["version"], "values": values})
        with self.assertRaisesRegex(AssistantError, "月份已改变"):
            await self.load(plan, month="10月", selected=json.dumps(["rec-source-new"]))
        loaded = await self.load(plan, month="10月", selected="[]")
        ready = self.agent.amend(ACTOR, plan["id"], {"version": loaded["version"], "values": values})
        editing = self.agent.amend(ACTOR, plan["id"], {"version": ready["version"], "action": "edit"})
        self.assertEqual(next(field for field in editing["fields"] if field["path"] == "source_month")["value"], "10月")
        self.assertEqual(self.selector(editing)["value"], "rec-source-new")
        self.assertEqual(self.writes, [])
        ready = self.agent.amend(ACTOR, plan["id"], {"version": editing["version"], "values": {}})
        completed = await self.execute(ready)
        self.assertEqual(completed["status"], "completed")
        self.assertEqual(self.writes[-1]["source_month"], "10月", "new month must reach original binding endpoint")

    async def test_six_work_types_accept_native_source_option_shape(self):
        for work in ("maintenance", "change", "repair", "power", "polling", "adjust"):
            with self.subTest(work_type=work):
                original = {**ONGOING, "work_type": work, "notice_type": ""}
                plan = self.agent.prepare(ACTOR, {"operations": [{"api_id": BIND, "body": {
                    "scope": "A", "work_type": work, "binding_context": "ongoing", "source_binding_only": True,
                    "active_item_id": original["active_item_id"], "target_record_id": original["target_record_id"]}}]}, "six-" + work, [], queries={"q": {"ongoing": [original]}})
                loaded = await self.load(plan)
                self.assertIn("rec-source-new", [option["value"] for option in self.selector(loaded)["options"]])
        self.assertEqual(self.writes, [])

    async def test_unknown_choice_and_scope_override_never_write(self):
        plan = self.prepare()
        loaded = await self.load(plan)
        with self.assertRaises(AssistantError):
            self.agent.amend(ACTOR, plan["id"], {"version": loaded["version"], "values": {"step0.target_record_id": "forged"}})
        count = len(self.lookups)
        with self.assertRaises(AssistantError):
            await self.load(plan, scope="B")
        self.assertEqual(len(self.lookups), count)
        self.assertEqual(self.writes, [])

    async def test_original_type_and_active_identity_cannot_be_mixed(self):
        decision = {"operations": [{"api_id": BIND, "body": {
            "scope": "A", "work_type": "repair", "binding_context": "planned", "source_record_id": PLAN["record_id"]}}]}
        with self.assertRaises(AssistantError):
            self.agent.prepare(ACTOR, decision, "wrong-original-type", [], queries={"q": {"records": [PLAN]}})
        decision["operations"][0]["body"] = {
            "scope": "A", "work_type": "maintenance", "binding_context": "ongoing", "active_item_id": ONGOING["active_item_id"],
            "target_record_id": "rec-other-active"}
        original = {**ONGOING, "active_item_id": "other-active", "target_record_id": "rec-other-active", "record_id": "rec-other-active"}
        plan = self.agent.prepare(ACTOR, decision, "rebind-active-priority", [], queries={"q": {"ongoing": [ONGOING, original]}})
        self.assertEqual(plan["operations"][0]["body"]["active_item_id"], ONGOING["active_item_id"])
        loaded = await self.load(plan)
        ready = self.agent.amend(ACTOR, plan["id"], {"version": loaded["version"], "values": {"step0.target_record_id": TARGET["target_record_id"]}})
        completed = await self.execute(ready)
        self.assertEqual(completed["status"], "completed")
        self.assertEqual(self.writes[-1]["active_item_id"], ONGOING["active_item_id"])

    async def test_invalid_explicit_active_id_is_not_replaced_by_target_match(self):
        with self.assertRaisesRegex(AssistantError, "已变化"):
            self.prepare("source", active_item_id="missing-active")
        self.assertEqual(self.writes, [])

    async def test_multi_building_anchor_does_not_widen_candidates(self):
        from lan_bitable_template_portal.lighthouse_sources import SCOPES
        actor = {**ACTOR, "scopes": sorted(SCOPES), "is_admin": True}
        original = {**PLAN, "scope": "", "building_codes": ["A", "H"]}
        plan = self.agent.prepare(actor, {"operations": [{"api_id": BIND, "body": {
            "scope": "ALL", "work_type": "maintenance", "binding_context": "planned", "source_record_id": original["record_id"]}}]},
            "multi-identity", [], queries={"q": {"records": [original]}})
        self.assertEqual(plan["operations"][0]["body"]["scope"], "ALL")
        self.targets = [TARGET, {**TARGET, "target_record_id": "rec-h", "scope": "H", "building_codes": ["H"]},
                        {**TARGET, "target_record_id": "rec-d", "scope": "D", "building_codes": ["D"]},
                        {**TARGET, "target_record_id": "rec-unknown", "scope": "", "building_codes": []}]
        loaded = await self.agent.field_options(actor, plan["id"], self.selector(plan)["name"], self.request)
        self.assertEqual({option["value"] for option in self.selector(loaded)["options"]}, {TARGET["target_record_id"], "rec-h"})

    async def test_read_error_incomplete_data_and_stale_plan_preserve_selection_state(self):
        plan = self.prepare()
        before = self.agent.get_plan(ACTOR, plan["id"])
        self.read_error = True
        with self.assertRaisesRegex(AssistantError, "候选读取失败"):
            await self.load(plan)
        self.read_error = False
        self.targets = None
        with self.assertRaisesRegex(AssistantError, "未完整返回"):
            await self.load(plan)
        self.assertEqual(self.agent.get_plan(ACTOR, plan["id"]), before)
        self.targets = [TARGET]
        def cancel():
            changed = copy.deepcopy(before)
            changed["status"] = "cancelled"
            self.store.put_document(PLAN_NAMESPACE, plan["id"], changed)
        self.on_read = cancel
        with self.assertRaisesRegex(AssistantError, "填写已变化"):
            await self.load(plan)
        self.assertEqual(self.agent.get_plan(ACTOR, plan["id"])["status"], "cancelled")
        self.assertEqual(self.writes, [])

    async def test_original_bind_rejection_is_reported_without_notice_or_retry(self):
        plan = self.prepare("target")
        loaded = await self.load(plan)
        ready = self.agent.amend(ACTOR, plan["id"], {"version": loaded["version"], "values": {"step0.target_record_id": "rec-target-new"}})
        self.bind_error = True
        completed = await self.execute(ready)
        self.assertEqual(completed["status"], "failed")
        self.assertIn("目标已结束", completed["error"])
        self.assertEqual(len(self.writes), 1)

    async def test_binding_cannot_process_record_removed_after_form_preparation(self):
        plan = self.prepare('target')
        loaded = await self.load(plan)
        ready = self.agent.amend(ACTOR, plan['id'], {'version': loaded['version'], 'values': {'step0.target_record_id': 'rec-target-new'}})
        self.ongoing = [TARGET]
        finished = await self.execute(ready)
        self.assertEqual(finished['status'], 'failed')
        self.assertEqual(self.writes, [])


if __name__ == "__main__":
    unittest.main()
