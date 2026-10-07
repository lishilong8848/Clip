"""Creation forms call typed native routes only after confirmation; no cloud writes."""
import asyncio
import copy
import datetime as dt
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import test_lighthouse_agent_workflows as workflows
from fastapi import File, Form, Request, UploadFile
from clipflow_backend.api_models import MorningMeetingGenerateRequest
from lan_bitable_template_portal.lighthouse_agent import PortalAgent, _result_refs
from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog
from lan_bitable_template_portal.lighthouse_ai import AssistantError

ACTOR = {"id": "creation-admin", "scopes": list("ABCDEH"), "is_admin": True}
MORNING = "POST /api/daily-tasks/morning-meeting/generate"
DRILL = "POST /api/drills"


class CreationWorkflowTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.fixture = workflows.WorkflowTests()
        self.fixture.setUp()
        for callback, args, kwargs in self.fixture._cleanups:
            self.addCleanup(callback, *args, **kwargs)
        self.fixture._cleanups.clear()
        self.calls = []
        self.today = dt.datetime.now(dt.timezone(dt.timedelta(hours=8))).date().isoformat()
        self.bad_download = False
        app = self.fixture.catalog.app

        @app.post("/api/daily-tasks/morning-meeting/generate")
        async def morning(request: Request, payload: MorningMeetingGenerateRequest):
            self.calls.append(payload.model_dump())
            return {"ok": True, "data": {"date": payload.date, "download_url":
                "https://invalid.test/private" if self.bad_download else "/api/daily-tasks/morning-meeting/download?date=" + payload.date}}

        @app.post("/api/drills")
        async def drill(request: Request, file: UploadFile = File(...), name: str = Form(""),
                        year: str = Form(""), month: str = Form(""), assigned_scopes: str = Form("")):
            self.calls.append({"name": name, "year": year, "month": month, "assigned_scopes": json.loads(assigned_scopes),
                               "file_name": file.filename, "content": await file.read()})
            return {"ok": True, "data": {"id": "draft-fixture", "status": "draft", "version": 1}}

        self.agent = PortalAgent(self.fixture.assistant, PortalAPICatalog(app), self.fixture.files)

    def prepare(self, kind=MORNING, body=None, *, actor=ACTOR, queries=None, files=None, fields=None):
        return self.agent.prepare(actor, {"operations": [{"api_id": kind, "body": body or {}, "files": files or {}}],
            "fields": fields or []}, "creation-fixture", [fid for ids in (files or {}).values() for fid in ids], queries=queries)

    def amend(self, plan, updates=None, file_id=None, actor=ACTOR):
        field = next(f for f in plan["fields"] if f["type"] == "object")
        value = _result_refs(field.get("_edit_value", field["value"]), [], plan.get("_references"), plan.get("_queries"))
        value.update(updates or {})
        values = {field["name"]: value}
        if file_id:
            values[next(f["name"] for f in plan["fields"] if f["type"] == "file")] = [file_id]
        return self.agent.amend(actor, plan["id"], {"version": plan["version"], "values": values})

    async def execute(self, plan, actor=ACTOR):
        result = await self.agent.confirm(actor, plan["id"], {"version": plan["version"], "stage": "review"}, self.fixture.request)
        if result["status"] == "awaiting_second_confirmation":
            await self.agent.confirm(actor, plan["id"], {"version": result["version"], "stage": "execute"}, self.fixture.request)
        if self.agent.tasks:
            await asyncio.gather(*tuple(self.agent.tasks))
        return self.agent.get_plan(actor, plan["id"])

    def test_empty_body_has_native_form_no_model_override(self):
        for kind in (MORNING, DRILL):
            plan = self.prepare(kind, fields=[{"type": "text", "path": "year", "label": "bogus"}])
            self.assertEqual(plan["status"], "needs_input")
            form = next(f for f in plan["fields"] if f["type"] == "object")
            self.assertNotIn("bogus", json.dumps(plan["fields"]))
            self.assertEqual(len(plan["fields"]), 1 if kind == MORNING else 2)
            if kind == DRILL:
                multi = next(f for f in form["children"] if f["path"] == "assigned_scopes")
                self.assertTrue(multi["choice_group"])
                self.assertEqual(multi["type"], "multiselect")
                upload = next(f for f in plan["fields"] if f["type"] == "file")
                self.assertEqual((upload["purpose"], upload["maxItems"], upload["accept"]), ("drill_template", 1, ".xlsx"))
        self.assertEqual(self.calls, [])

    async def test_morning_prefill_null_zero_edit_restart_and_download(self):
        preview = {"date": self.today, "weather_condition": "晴", "dry_bulb_temperature": 0, "wet_bulb_temperature": None}
        plan = self.prepare(queries={"query_preview": preview})
        operation_id = plan["operations"][0]["body"]["operation_id"]
        amended = self.amend(plan, {"weather_condition": "多云", "dry_bulb_temperature": 21.3})
        self.assertEqual(self.calls, [])
        edited = self.agent.amend(ACTOR, plan["id"], {"version": amended["version"], "action": "edit"})
        amended = self.agent.amend(ACTOR, plan["id"], {"version": edited["version"], "values": {}})
        self.agent = PortalAgent(self.fixture.assistant, self.agent.catalog, self.fixture.files)
        done = await self.execute(amended)
        self.assertEqual(done["status"], "completed", done["error"])
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(self.calls[0]["operation_id"], operation_id)
        self.assertEqual(self.calls[0]["weather_condition"], "多云")
        self.assertEqual(self.calls[0]["dry_bulb_temperature"], 21.3)
        self.assertIsNone(self.calls[0]["wet_bulb_temperature"])
        public = self.agent.public_plan(done, ACTOR)
        self.assertEqual(public["results"][0]["download_url"], "/api/daily-tasks/morning-meeting/download?date=" + self.today)
        repeated = await self.execute(amended)
        self.assertEqual(repeated["status"], "completed")
        self.assertEqual(len(self.calls), 1)

    async def test_untrusted_download_link_not_promoted(self):
        self.bad_download = True
        done = await self.execute(self.amend(self.prepare()))
        self.assertEqual(done["status"], "completed")
        self.assertNotIn("download_url", self.agent.public_plan(done, ACTOR)["results"][0])

    def test_morning_stale_preview_not_defaulted_and_explicit_stale_denied(self):
        plan = self.prepare(queries={"query_old": {"date": "2020-01-01", "weather_condition": "旧天气", "dry_bulb_temperature": 30}})
        form = plan["fields"][0]["_initial_form"]
        self.assertEqual(form["date"], self.today)
        self.assertEqual(form["weather_condition"], "")
        self.assertIsNone(form["dry_bulb_temperature"])
        with self.assertRaises(AssistantError):
            self.prepare(body={"date": "2020-01-01"})

    def test_morning_invalid_fields_never_write(self):
        for change in ({"dry_bulb_temperature": 81}, {"wet_bulb_temperature": -51}, {"dry_bulb_temperature": "not-number"},
                       {"dry_bulb_temperature": True}, {"wet_bulb_temperature": float("nan")}, {"weather_condition": "x" * 41},
                       {"date": "2026-02-30"}, {"date": ""}, {"actor_name": "fake"}):
            with self.subTest(change=change), self.assertRaises(AssistantError):
                self.amend(self.prepare(), change)
        self.assertEqual(self.calls, [])

    def test_roles_and_scope_guards(self):
        with self.assertRaises(AssistantError):
            self.prepare(actor={**ACTOR, "scopes": ["A"]})
        with self.assertRaises(AssistantError):
            self.prepare(DRILL, actor={**ACTOR, "is_admin": False})
        for body in ({"assigned_scopes": ["H"]}, {"assigned_scopes": ["A", "A"]}, {"year": 1900, "month": "01"}, {"month": "bad"}):
            with self.subTest(body=body), self.assertRaises(AssistantError):
                self.prepare(DRILL, body)
        with self.assertRaises(AssistantError):
            self.prepare(DRILL, {"assigned_scopes": ["B"]}, actor={**ACTOR, "scopes": ["A"]})

    async def test_drill_draft_upload_month_and_scopes_match_native(self):
        file = self.fixture.files.upload(ACTOR, "演练.xlsx", b"fake-native-template", extract=False)
        plan = self.prepare(DRILL, {"year": 2026, "month": "10", "assigned_scopes": ["A"]}, files={"file": [file["id"]]})
        review = self.amend(plan, {"month": "2027-01", "name": "演练草稿", "assigned_scopes": ["B", "E"]})
        self.assertEqual(self.calls, [])
        done = await self.execute(review)
        self.assertEqual(done["status"], "completed", done["error"])
        self.assertEqual(self.calls, [{"name": "演练草稿", "year": "2027", "month": "2027-01", "assigned_scopes": ["B", "E"],
                                      "file_name": "演练.xlsx", "content": b"fake-native-template"}])
        self.assertIn("尚未发布", done["results"][0]["query_reply"])

    def test_drill_empty_scopes_and_invalid_file_block_before_confirmation(self):
        plan = self.prepare(DRILL, {"assigned_scopes": []})
        with self.assertRaises(AssistantError):
            self.amend(plan)
        bad = self.fixture.files.upload(ACTOR, "wrong.xlsm", b"fake", extract=False)
        with self.assertRaises(AssistantError):
            self.amend(self.prepare(DRILL), file_id=bad["id"])
        with self.assertRaises(AssistantError):
            self.prepare(DRILL, files={"file": [bad["id"]]})
        other = self.fixture.files.upload({**ACTOR, "id": "other"}, "valid.xlsx", b"fake", extract=False)
        with self.assertRaises(AssistantError):
            self.amend(self.prepare(DRILL), file_id=other["id"])
        self.assertEqual(self.calls, [])

    async def test_morning_midnight_before_execute_does_not_write_old_day(self):
        plan = self.amend(self.prepare())
        with patch("lan_bitable_template_portal.lighthouse_agent._creation_form", side_effect=AssistantError("晨会表格只支持生成当天数据。")):
            done = await self.execute(plan)
        self.assertEqual(done["status"], "failed")
        self.assertEqual(self.calls, [])


if __name__ == "__main__":
    unittest.main()
