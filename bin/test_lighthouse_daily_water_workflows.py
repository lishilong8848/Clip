"""Native schemas and isolated ASGI handlers; no message or Feishu writes."""
import asyncio
import copy
import json
import io
import time
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import test_lighthouse_agent_workflows as workflows
from fastapi import Request
from fastapi.responses import JSONResponse
from clipflow_backend.api_models import DailyTaskSendRequest, WaterConsumptionRecordRequest
from lan_bitable_template_portal.lighthouse_agent import PortalAgent, _result_refs
from lan_bitable_template_portal.lighthouse_ai import AssistantError
from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog

ACTOR = workflows.ACTOR
WATER_ACTOR = {**ACTOR, "is_admin": True}


class DailyWaterWorkflowTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.fixture = workflows.WorkflowTests()
        self.fixture.setUp()
        for callback, args, kwargs in self.fixture._cleanups:
            self.addCleanup(callback, *args, **kwargs)
        self.fixture._cleanups.clear()
        self.sent, self.water_calls, self.uploaded = [], [], []
        self.water_queries = {
            "query_bootstrap": {"scope": "A", "permissions": {"can_create": True}, "options": {"meters": ["中水"], "frequencies": ["每日"], "shifts": ["白班"]}},
            "query_detail": {"record_id": "rec-water", "scope_code": "A", "version": "water-v1", "photos": [], "edit_policy": {"can_edit": True},
                "meter": "中水", "frequency": "每日", "shift": "白班", "statistic_date": "2026-10-02", "meter_value": 200, "corrected_usage": None},
        }
        self.people_queries = []
        self.people = [
            {"user_id": "ou-private-one", "name": "同名人员", "employee_no": "101", "selectable": True},
            {"user_id": "ou-private-two", "name": "同名人员", "employee_no": "102", "selectable": True},
            {"user_id": "", "name": "不可发信人员", "selectable": False}]
        app = self.fixture.catalog.app

        @app.get("/api/repair-management/people")
        async def people(request: Request):
            self.people_queries.append(dict(request.query_params))
            return {"ok": True, "data": {"people": copy.deepcopy(self.people), "total": len(self.people)}}

        @app.post("/api/daily-tasks/send")
        async def daily_send(request: Request, payload: DailyTaskSendRequest):
            self.sent.append(payload.model_dump())
            return {"ok": True, "data": {"sent_count": len(payload.recipient_open_ids), "failed_count": 0}}

        @app.post("/api/capacity/water/uploads")
        async def upload(request: Request):
            self.uploaded.append(await request.body())
            return {"ok": True, "data": {"upload_id": "water-image", "expires_at": time.time() + 86400}}

        async def water(request: Request, payload: WaterConsumptionRecordRequest):
            self.water_calls.append(payload.model_dump())
            if not payload.large_change_confirmed or not payload.abnormal_note.strip():
                return JSONResponse({"ok": False, "error": "水表数值变化较大，请核对。", "error_code": "confirmation_required",
                    "details": {"kind": "water_large_change", "note_required": True, "old_value": 100, "new_value": 200}}, status_code=409)
            return {"ok": True, "data": {"saved": True, "record_id": "rec-water"}}

        app.add_api_route("/api/capacity/water/records", water, methods=["POST"])
        app.add_api_route("/api/capacity/water/records/{record_id}", water, methods=["PATCH"])
        self.agent = PortalAgent(self.fixture.assistant, PortalAPICatalog(app), self.fixture.files)

    def prepare(self, operations):
        actor = WATER_ACTOR if any("/api/capacity/water/" in op["api_id"] for op in operations) else ACTOR
        return self.agent.prepare(actor, {"operations": operations}, "daily-water-fixture-001", [], queries=self.water_queries)

    async def run_plan(self, plan):
        actor = WATER_ACTOR if any("/api/capacity/water/" in op["api_id"] for op in plan["operations"]) else ACTOR
        water = next((field for field in plan.get("fields", []) if field.get("native_water_record")), None)
        if water:
            stored = self.agent.get_plan(actor, plan["id"])
            value = _result_refs(water["value"], [], stored.get("_references"), stored.get("_queries"))
            values = {water["name"]: value}
            if not stored["operations"][water["operation_index"]]["body"].get("upload_ids") and not value.get("retained_image_ids"):
                from PIL import Image
                stream = io.BytesIO()
                Image.new("RGB", (4, 4), "white").save(stream, format="PNG")
                image = self.fixture.files.upload(actor, "meter.png", stream.getvalue(), extract=False)
                values[next(field["name"] for field in plan["fields"] if field.get("native_water_photos"))] = [image["id"]]
            plan = self.agent.amend(actor, plan["id"], {"version": plan["version"], "values": values})
        value = await self.agent.confirm(actor, plan["id"], {"version": plan["version"], "stage": "review"}, self.fixture.request)
        if value["status"] == "awaiting_second_confirmation":
            await self.agent.confirm(actor, plan["id"], {"version": value["version"], "stage": "execute"}, self.fixture.request)
        if self.agent.tasks:
            await asyncio.gather(*tuple(self.agent.tasks))
        return self.agent.get_plan(ACTOR, plan["id"])

    async def test_daily_name_picker_opaque_ids_stable_refresh_and_real_recipients(self):
        plan = self.prepare([{"api_id": "POST /api/daily-tasks/send", "body": {"scope": "A", "date": "2026-10-02"}}])
        field = next(field for field in plan["fields"] if field["path"] == "recipient_open_ids")
        self.assertEqual((field["type"], field["maxItems"]), ("multiselect", 20))
        public = await self.agent.field_options(ACTOR, plan["id"], field["name"], self.fixture.request)
        field = next(item for item in public["fields"] if item["name"] == field["name"])
        self.assertEqual([item["label"] for item in field["options"]], ["同名人员 · 101", "同名人员 · 102"])
        refs = [item["value"] for item in field["options"]]
        self.assertNotIn("ou-private", json.dumps(public))
        public2 = await self.agent.field_options(ACTOR, plan["id"], field["name"], self.fixture.request)
        self.assertEqual(public2["fields"][0]["options"], field["options"])
        self.assertEqual(self.people_queries[0]["scope"], "A")
        for bad in ([], ["ou-private-one"], ["forged"]):
            with self.assertRaises(AssistantError):
                self.agent.amend(ACTOR, plan["id"], {"version": public2["version"], "values": {field["name"]: bad}})
        amended = self.agent.amend(ACTOR, plan["id"], {"version": public2["version"], "values": {field["name"]: refs}})
        self.assertEqual(amended["operations"][0]["selected_labels"]["recipient_names"], "同名人员 · 101、同名人员 · 102")
        self.assertNotIn("ou-private", json.dumps(amended))
        self.assertEqual(self.sent, [])
        done = await self.run_plan(amended)
        self.assertEqual(done["status"], "completed", done["error"])
        self.assertEqual(self.sent[0]["recipient_open_ids"], ["ou-private-one", "ou-private-two"])
        with self.assertRaises(AssistantError):
            _result_refs({"$reference": refs[0]}, [], done["_references"], target_field="source_event_id")

    async def test_daily_outside_scope_cannot_read_recipients_or_send(self):
        plan = self.prepare([{"api_id": "POST /api/daily-tasks/send", "body": {"scope": "B", "date": "2026-10-02"}}])
        with self.assertRaisesRegex(AssistantError, "无权"):
            await self.agent.field_options(ACTOR, plan["id"], plan["fields"][0]["name"], self.fixture.request)
        self.assertEqual(self.people_queries, [])
        self.assertEqual(self.sent, [])

    async def test_daily_defaults_to_single_allowed_building(self):
        plan = self.prepare([{"api_id": "POST /api/daily-tasks/send", "body": {"date": "2026-10-02"}}])
        self.assertEqual(plan["operations"][0]["body"]["scope"], "A")
        await self.agent.field_options(ACTOR, plan["id"], plan["fields"][0]["name"], self.fixture.request)
        self.assertEqual(self.people_queries[-1]["scope"], "A")

    async def test_daily_partial_delivery_is_not_reported_as_all_success(self):
        plan = self.prepare([{"api_id": "POST /api/daily-tasks/send", "body": {"scope": "A", "date": "2026-10-02"}}])
        field = plan["fields"][0]
        loaded = await self.agent.field_options(ACTOR, plan["id"], field["name"], self.fixture.request)
        amended = self.agent.amend(ACTOR, plan["id"], {"version": loaded["version"], "values": {field["name"]: [option["value"] for option in loaded["fields"][0]["options"]]}})
        with patch.object(self.agent, "_invoke", return_value={"ok": True, "status": 200, "_raw": {"sent_count": 1, "failed_count": 1}}) as invoke:
            done = await self.run_plan(amended)
        self.assertEqual(done["status"], "failed")
        self.assertIn("已发送 1 人，1 人未发送", done["error"])
        self.assertEqual(invoke.call_count, 1)

    async def test_water_confirmation_resumes_original_id_after_restart(self):
        for method in ("POST", "PATCH"):
            with self.subTest(method=method):
                self.water_calls.clear()
                op = {"api_id": method + " /api/capacity/water/records" + ("/{record_id}" if method == "PATCH" else ""),
                    "body": {"scope": "A", "meter": "中水", "frequency": "每日", "shift": "白班", "statistic_date": "2026-10-02",
                             "meter_value": 200, "expected_version": "water-v1"}}
                if method == "PATCH":
                    op["path_params"] = {"record_id": "rec-water"}
                plan = self.prepare([op])
                operation_id = plan["operations"][0]["body"]["operation_id"]
                pending = await self.run_plan(plan)
                self.assertEqual(pending["status"], "needs_input", pending["error"])
                self.assertEqual(pending["results"], [])
                self.assertEqual(len(self.water_calls), 1)
                self.agent = PortalAgent(self.fixture.assistant, self.agent.catalog, self.fixture.files)
                field = pending["fields"][0]
                for bad in ("   ", "x" * 1001, {"bad": "type"}):
                    with self.assertRaises(AssistantError):
                        self.agent.amend(ACTOR, pending["id"], {"version": pending["version"], "values": {field["name"]: bad}})
                amended = self.agent.amend(ACTOR, pending["id"], {"version": pending["version"], "values": {field["name"]: "已核对，本次设备补水。"}})
                self.assertEqual(len(self.water_calls), 1)
                done = await self.run_plan(amended)
                self.assertEqual(done["status"], "completed", done["error"])
                self.assertEqual([row["operation_id"] for row in self.water_calls], [operation_id, operation_id])
                self.assertEqual(self.water_calls[-1]["upload_ids"], ["water-image"])
                self.assertEqual(self.water_calls[-1]["expected_version"], "water-v1" if method == "PATCH" else "")
                self.assertTrue(self.water_calls[-1]["large_change_confirmed"])
                self.assertEqual(len(done["results"]), 1)

    async def test_water_confirmation_does_not_repeat_previous_upload(self):
        from PIL import Image
        stream = io.BytesIO()
        Image.new("RGB", (4, 4), "white").save(stream, format="PNG")
        image = self.fixture.files.upload(ACTOR, "water.png", stream.getvalue(), extract=False)
        plan = self.agent.prepare(WATER_ACTOR, {"operations": [
            {"api_id": "POST /api/capacity/water/uploads", "params": {"scope": "A"}, "files": {"file": [image["id"]]}},
            {"api_id": "POST /api/capacity/water/records", "body": {"scope": "A", "meter": "中水", "frequency": "每日", "shift": "白班",
                "statistic_date": "2026-10-02", "meter_value": 200, "upload_ids": [{"$result": {"step": 0, "path": "upload_id"}}]}}
        ]}, "daily-water-fixture-002", [image["id"]], queries=self.water_queries)
        pending = await self.run_plan(plan)
        self.assertEqual(pending["status"], "needs_input", pending["error"])
        self.assertEqual(len(pending["results"]), 1)
        self.assertEqual(len(self.uploaded), 1)
        field = pending["fields"][0]
        amended = self.agent.amend(ACTOR, pending["id"], {"version": pending["version"], "values": {field["name"]: "现场读数已复核"}})
        done = await self.run_plan(amended)
        self.assertEqual(done["status"], "completed", done["error"])
        self.assertEqual(len(done["results"]), 2)
        self.assertEqual(len(self.uploaded), 1)
        self.assertEqual(self.water_calls[-1]["upload_ids"], ["water-image"])

    async def test_water_network_failure_is_not_treated_as_safe_confirmation(self):
        self.water_queries["query_detail"]["photos"] = [{"image_id": "existing-native-photo", "file_name": "原图"}]
        plan = self.prepare([{"api_id": "PATCH /api/capacity/water/records/{record_id}", "path_params": {"record_id": "rec-water"}, "body": {"scope": "A", "meter": "中水", "frequency": "每日",
            "shift": "白班", "statistic_date": "2026-10-02", "meter_value": 200}}])
        with patch.object(self.agent, "_invoke", return_value={"ok": False, "status": 504, "error": "网络超时", "_raw": {}}):
            done = await self.run_plan(plan)
        self.assertEqual(done["status"], "failed")
        self.assertEqual(done["fields"], [])
        self.assertEqual(self.water_calls, [])


if __name__ == "__main__":
    unittest.main()
