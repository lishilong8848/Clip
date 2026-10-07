"""Focused workflow checks for PortalAgent: confirmations, multipart refs,
HTTP-200 failures, async job resume and notice_command binding fields.

Fake in-process API + in-memory store only.  No real cloud/provider access.
"""
import asyncio
import copy
import json
import re
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from typing import List
from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from clipflow_backend.api_models import RepairFollowupRecordRequest, RepairManagementRecordRequest, PollingSopRequest, CriticalGuardResponseRequest, CriticalGuardTaskRequest, CriticalGuardScopeTemplateRequest, OngoingDeleteRequest, NoticeUndoApplyRequest

from lan_bitable_template_portal.lighthouse_agent import (
    PLAN_NAMESPACE,
    PortalAgent,
    _result_refs,
)
from lan_bitable_template_portal.lighthouse_ai import AssistantError, LighthouseAssistant
from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog
from lan_bitable_template_portal.lighthouse_files import LighthouseFiles

ACTOR = {"id": "workflow-fixture-a", "scopes": ["A"], "is_admin": False}


class Store:
    def __init__(self, path):
        self.db_path, self.docs = path, {}

    def get_document(self, namespace, key):
        return copy.deepcopy(self.docs.get((namespace, key)))

    def put_document(self, namespace, key, value):
        self.docs[namespace, key] = copy.deepcopy(value)


async def gather_tasks(agent):
    if agent.tasks:
        await asyncio.gather(*tuple(agent.tasks))


class WorkflowTests(unittest.IsolatedAsyncioTestCase):
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

        self.delete_writes = []
        self.upload_writes = []
        self.attach_writes = []
        self.sequential_writes = []
        self.batch_writes = []
        self.job_phase = {"job-1": "processing"}
        self.notice_writes = []
        self.notice_deletes, self.notice_undos = [], []
        self.ongoing = [{"scope": "A", "building_codes": ["A"], "work_type": "maintenance", "notice_type": "维保通告",
                         "title": "A楼测试维护", "status": "开始", "active_item_id": "active-original",
                         "target_record_id": "rec-original", "record_id": "rec-original"}]
        self.ongoing_query_ok = True
        self.records = [
            {"record_id": "plan-1", "title": "A楼调整", "status": "未开始"},
            {"record_id": "plan-2", "title": "B楼调整", "status": "未开始"},
        ]

        app = FastAPI()

        @app.get("/api/workbench")
        async def workbench(request: Request):
            scope = request.query_params.get("scope")
            work_type = request.query_params.get("work_type")
            sections = request.query_params.get("sections")
            search = request.query_params.get("search")
            ongoing_page_size = request.query_params.get("ongoing_page_size")
            return {"ok": self.ongoing_query_ok, "data": {"scope": scope, "ongoing": copy.deepcopy(self.ongoing)}, "error": "fixture unavailable" if not self.ongoing_query_ok else ""}

        @app.post("/api/ongoing-items/delete")
        async def delete_notice(body: OngoingDeleteRequest):
            self.notice_deletes.append(body.model_dump())
            return {"ok": True, "data": {"deleted": True, "remote_deleted": True, "qt_deleted": True}}

        @app.post("/api/notice-undo/{undo_id}/apply")
        async def undo_notice(undo_id: str, body: NoticeUndoApplyRequest):
            self.notice_undos.append(undo_id)
            return {"ok": True, "data": {"restored": True}}

        @app.delete("/api/drills/{drill_id}")
        async def delete_drill(drill_id: str, request: Request):
            self.delete_writes.append({"drill_id": drill_id})
            return {"ok": True, "data": {"deleted": True}}

        @app.post("/api/engineer/mop/upload-local")
        async def upload_notice(file: UploadFile = File(...), request: Request = None):
            raw = await file.read()
            filename = file.filename or "upload"
            self.upload_writes.append((filename, raw))
            return {"ok": True, "data": {"upload_id": "up-1", "file_token": "private-file-token"}}

        @app.post("/api/cabinet-power/batches/{batch_id}/images")
        async def attach_files(batch_id: str, request: Request, file_token: str = Form(""), person: str = Form(""), files: List[UploadFile] = File(...)):
            self.attach_writes.append({
                "file_token": file_token,
                "person": person,
                "batch_id": batch_id,
                "attachment_name": files[0].filename if files else None,
                "attachment_content": await files[0].read() if files else b"",
            })
            return {"ok": True, "data": {"saved": True}}

        @app.post("/api/step-one")
        async def step_one(request: Request):
            self.sequential_writes.append("step-one")
            return JSONResponse({"ok": False, "error": "业务被拒绝"}, status_code=200)

        @app.post("/api/step-two")
        async def step_two(request: Request):
            self.sequential_writes.append("step-two")
            return {"ok": True, "data": {"saved": True}}

        @app.post("/api/step-interrupt")
        async def step_interrupt(request: Request):
            self.sequential_writes.append("step-interrupt")
            raise InterruptedError("模拟第二部写入中断")

        @app.post("/api/batch/submit")
        async def batch_submit(request: Request):
            self.batch_writes.append("submit")
            return {"ok": True, "data": {"job_id": "job-1"}}

        @app.get("/api/jobs/{job_id}")
        async def job_status(job_id: str, request: Request):
            return {"ok": True, "data": {"phase": self.job_phase.get(job_id, "success")}}

        @app.post("/api/batch/finalize")
        async def batch_finalize(request: Request):
            self.batch_writes.append("finalize")
            return {"ok": True, "data": {"saved": True}}

        @app.post("/api/workbench-actions")
        async def workbench_actions(request: Request):
            payload = await request.json()
            self.notice_writes.append(payload)
            return {"ok": True, "data": {"record_id": "rec-notice"}}

        @app.get("/api/workbench/source-options")
        async def records(request: Request):
            return {"ok": True, "data": {"items": self.records[:1]}}

        self.catalog = PortalAPICatalog(app)
        self.agent = PortalAgent(self.assistant, self.catalog, self.files)
        self.request = Request({
            "type": "http",
            "scheme": "http",
            "server": ("testserver", 80),
            "client": ("127.0.0.1", 4567),
            "path": "/api/assistant/agent",
            "root_path": "",
            "query_string": b"",
            "headers": [(b"cookie", b"fixture=a"), (b"origin", b"http://testserver")],
        })
        self.operation_id = "workflow_test_00000001"
        self.conversation = self.assistant.conversation(ACTOR)

    def setUpModel(self, *decisions):
        self.assistant.model.complete.side_effect = [json.dumps(d, ensure_ascii=False) for d in decisions]

    async def test_delete_high_risk_two_confirmations_single_write(self):
        decision = {
            "operations": [{
                "api_id": "DELETE /api/drills/{drill_id}",
                "path_params": {"drill_id": "drill-01"},
            }],
        }
        plan = self.agent.prepare(ACTOR, decision, self.operation_id, [])
        self.assertEqual(plan["status"], "awaiting_confirmation")
        self.assertEqual(plan["risk"], "high")

        plan = await self.agent.confirm(ACTOR, plan["id"], {"version": plan["version"], "stage": "review"}, self.request)
        self.assertEqual(plan["status"], "awaiting_second_confirmation")
        self.assertEqual(self.delete_writes, [])

        await self.agent.confirm(ACTOR, plan["id"], {"version": plan["version"], "stage": "execute"}, self.request)
        # A repeat click on the already-running plan must not start a second job.
        running = self.agent.get_plan(ACTOR, plan["id"])
        self.assertEqual(running["status"], "running")
        await self.agent.confirm(ACTOR, running["id"], {"version": running["version"], "stage": "execute"}, self.request)

        await gather_tasks(self.agent)
        self.assertEqual(self.delete_writes, [{"drill_id": "drill-01"}])
        finished = self.agent.get_plan(ACTOR, plan["id"])
        self.assertEqual(finished["status"], "completed")

    async def test_delete_notice_calls_web_delete_not_undo_and_keeps_target_id(self):
        decision = {"title": "删除维保通告", "operations": [{"api_id": "POST /api/ongoing-items/delete", "body": {
            "scope": "A", "work_type": "maintenance", "notice_type": "维保通告",
            "active_item_id": "active-original", "target_record_id": "rec-original"}}]}
        plan = self.agent.prepare(ACTOR, decision, self.operation_id, [], queries={"q": {"ongoing": self.ongoing}})
        self.assertEqual(plan["risk"], "high")
        self.assertEqual(self.notice_deletes, [])
        plan = await self.agent.confirm(ACTOR, plan["id"], {"version": plan["version"], "stage": "review"}, self.request)
        self.assertEqual(plan["status"], "awaiting_second_confirmation")
        await self.agent.confirm(ACTOR, plan["id"], {"version": plan["version"], "stage": "execute"}, self.request)
        await gather_tasks(self.agent)
        completed = self.agent.get_plan(ACTOR, plan["id"])
        self.assertEqual(completed["status"], "completed", completed.get("error"))
        self.assertEqual(len(self.notice_deletes), 1)
        self.assertEqual(self.notice_deletes[0]["target_record_id"], "rec-original")
        self.assertEqual(self.notice_deletes[0]["active_item_id"], "active-original")
        self.assertEqual(self.notice_undos, [])
        self.assertEqual(self.notice_writes, [])

    async def test_delete_notice_rejects_unqueried_historical_id(self):
        decision = {"operations": [{"api_id": "POST /api/ongoing-items/delete", "body": {
            "scope": "A", "work_type": "maintenance", "active_item_id": "rec-old", "target_record_id": "rec-old",
            "title": self.ongoing[0]["title"]}}]}
        for queries in ({}, {"q": {"ongoing": self.ongoing}}):
            with self.subTest(queries=bool(queries)), self.assertRaisesRegex(AssistantError, "重新查询"):
                self.agent.prepare(ACTOR, decision, self.operation_id, [], queries=queries)
        self.assertFalse(self.notice_deletes)

    async def test_delete_notice_rechecks_before_write_without_retargeting(self):
        original = copy.deepcopy(self.ongoing)
        for mode in ('removed', 'replaced', 'renamed', 'rebound', 'unavailable'):
            with self.subTest(mode=mode):
                self.ongoing, self.ongoing_query_ok = copy.deepcopy(original), True
                decision = {"operations": [{"api_id": "POST /api/ongoing-items/delete", "body": {
                    "scope": "A", "work_type": "maintenance", "active_item_id": "active-original", "target_record_id": "rec-original"}}]}
                plan = self.agent.prepare(ACTOR, decision, self.operation_id, [], queries={"q": {"ongoing": self.ongoing}})
                if mode == 'removed':
                    self.ongoing = []
                elif mode == 'replaced':
                    self.ongoing[0].update(active_item_id='new-active', target_record_id='new-rec', record_id='new-rec')
                elif mode == 'renamed':
                    self.ongoing[0]['title'] = '另一条通告'
                elif mode == 'rebound':
                    self.ongoing[0].update(target_record_id='new-rec', record_id='new-rec')
                else:
                    self.ongoing_query_ok = False
                await self.agent._execute(ACTOR, plan, self.request)
                self.assertEqual(plan['status'], 'failed')
                self.assertFalse(self.notice_deletes)
                self.assertFalse(self.notice_undos)

    async def test_delete_notice_old_plan_cannot_execute_without_anchor(self):
        decision = {"operations": [{"api_id": "POST /api/ongoing-items/delete", "body": {
            "scope": "A", "work_type": "maintenance", "target_record_id": "rec-original"}}]}
        plan = self.agent.prepare(ACTOR, decision, self.operation_id, [], queries={"q": {"ongoing": self.ongoing}})
        plan.pop('_notice_delete_targets')
        self.store.put_document(PLAN_NAMESPACE, plan['id'], plan)
        with self.assertRaisesRegex(AssistantError, '重新查询'):
            await self.agent.confirm(ACTOR, plan['id'], {'version': plan['version'], 'stage': 'review'}, self.request)
        await self.agent._execute(ACTOR, plan, self.request)
        self.assertEqual(plan['status'], 'failed')
        self.assertFalse(self.notice_deletes)

    async def test_delete_notice_intent_rejects_undo_even_with_model_rewritten_title(self):
        decision = {"title": "撤销开始", "operations": [{"api_id": "POST /api/notice-undo/{undo_id}/apply",
            "path_params": {"undo_id": "undo-original"}, "body": {"scope": "A"}}]}
        with self.assertRaisesRegex(AssistantError, "不能代替删除"):
            self.agent.prepare(ACTOR, decision, self.operation_id, [], question="删除刚刚发送的维保通告")
        self.assertEqual(self.notice_undos, [])
        self.ongoing[0].update(undo_id='undo-original', undo_action_type='update')
        plan = self.agent.prepare(ACTOR, decision, self.operation_id, [], question="撤销这次维保通告的更新", queries={'q': {'ongoing': self.ongoing}})
        self.assertEqual(plan["operations"][0]["api_id"], "POST /api/notice-undo/{undo_id}/apply")

    async def test_undo_only_handles_current_ongoing_notice(self):
        decision = {'operations': [{'api_id': 'POST /api/notice-undo/{undo_id}/apply',
                                   'path_params': {'undo_id': 'undo-current'}, 'body': {'scope': 'A'}}]}
        for action in ('end', 'delete'):
            self.ongoing[0].update(undo_id='undo-current', undo_action_type=action)
            with self.assertRaises(AssistantError):
                self.agent.prepare(ACTOR, decision, self.operation_id, [], queries={'q': {'ongoing': self.ongoing}})
        self.ongoing[0].update(undo_action_type='update')
        plan = self.agent.prepare(ACTOR, decision, self.operation_id, [], queries={'q': {'ongoing': self.ongoing}})
        self.ongoing = []
        await self.agent._execute(ACTOR, plan, self.request)
        self.assertEqual(plan['status'], 'failed')
        self.assertEqual(self.notice_undos, [])

    async def test_five_building_exports_generate_native_stable_batch_ids(self):
        app, writes = FastAPI(), []
        @app.post("/api/cabinet-power/export-batches")
        async def export_batch(request: Request):
            body = await request.json()
            self.assertRegex(body["batch_id"], r"^all_[a-f0-9]{32}$")
            writes.append(copy.deepcopy(body))
            return {"ok": True, "data": {**body, "status": "succeeded"}}
        @app.get("/api/cabinet-power/export-batches/{batch_id}")
        async def export_status(batch_id: str):
            return {"ok": True, "data": {"batch_id": batch_id, "status": "succeeded"}}
        agent = PortalAgent(self.assistant, PortalAPICatalog(app), self.files)
        actor = {**ACTOR, "scopes": list("ABCDE")}
        plan = agent.prepare(actor, {"operations": [{"api_id": "POST /api/cabinet-power/export-batches"}] * 2}, self.operation_id, [])
        ids = [op["body"]["batch_id"] for op in plan["operations"]]
        self.assertEqual(len(set(ids)), 2)
        self.assertTrue(all(re.fullmatch(r"all_[a-f0-9]{32}", value) for value in ids))
        plan = await agent.confirm(actor, plan["id"], {"version": plan["version"], "stage": "review"}, self.request)
        if plan["status"] == "awaiting_second_confirmation":
            plan = await agent.confirm(actor, plan["id"], {"version": plan["version"], "stage": "execute"}, self.request)
        await gather_tasks(agent)
        finished = agent.get_plan(actor, plan["id"])
        self.assertEqual(finished["status"], "completed", finished.get("error"))
        await agent.confirm(actor, plan["id"], {"version": finished["version"], "stage": "review"}, self.request)
        self.assertEqual([body["batch_id"] for body in writes], ids)

    async def test_workorder_execution_rejected_in_new_and_saved_plans(self):
        @self.catalog.app.post("/api/polling-work-orders/confirm")
        async def confirm_step(request: Request):
            raise AssertionError("workorder execution must remain in the original interface")
        catalog = PortalAPICatalog(self.catalog.app)
        agent = PortalAgent(self.assistant, catalog, self.files)
        forbidden = {"api_id": "POST /api/polling-work-orders/confirm", "body": {"step_key": "1", "expected_version": 1}}
        with self.assertRaises(AssistantError):
            agent.prepare(ACTOR, {"operations": [forbidden]}, self.operation_id, [])
        plan = agent.prepare(ACTOR, {"operations": [{"api_id": "DELETE /api/drills/{drill_id}", "path_params": {"drill_id": "d-legacy"}}]}, self.operation_id, [])
        plan["operations"] = [forbidden]
        await agent._execute(ACTOR, plan, self.request)
        self.assertEqual(plan["status"], "failed")
        self.assertEqual(plan["results"], [])
        self.assertEqual(self.delete_writes, [])

    async def test_multipart_upload_result_reference_sends_file_token_and_hides_private_token(self):
        file_a = self.files.upload(ACTOR, "a.txt", ("A楼内容\n" * 10).encode())
        file_b = self.files.upload(ACTOR, "b.txt", ("B楼内容\n" * 10).encode())
        decision = {
            "operations": [
                {"api_id": "POST /api/engineer/mop/upload-local", "files": {"file": [file_a["id"]]}},
                {
                    "api_id": "POST /api/cabinet-power/batches/{batch_id}/images",
                    "path_params": {"batch_id": "batch-attach-1"},
                    "body": {
                        "file_token": {"$result": {"step": 0, "path": "file_token"}},
                        "person": "ou_plain",
                    },
                    "files": {"files": [file_b["id"]]},
                },
            ],
        }
        plan = self.agent.prepare(ACTOR, decision, self.operation_id, [file_a["id"], file_b["id"]])
        await self.agent._execute(ACTOR, plan, self.request)

        self.assertEqual(len(self.upload_writes), 1)
        self.assertEqual(len(self.attach_writes), 1)
        self.assertEqual(self.attach_writes[0]["file_token"], "private-file-token")
        self.assertEqual(self.attach_writes[0]["person"], "ou_plain")
        self.assertEqual(self.attach_writes[0]["attachment_name"], "b.txt")

        public = self.agent.public_plan(plan)
        public_json = json.dumps(public, ensure_ascii=False)
        self.assertNotIn("private-file-token", public_json)

    async def test_person_reference_with_attachments_survives_prepare(self):
        # Attachment IDs must not replace the private personnel reference map.
        file_a = self.files.upload(ACTOR, "person.txt", ("人员附件\n" * 10).encode())
        decision = {
            "operations": [{
                "api_id": "POST /api/cabinet-power/batches/{batch_id}/images",
                "path_params": {"batch_id": "batch-attach-1"},
                "body": {"person": {"$reference": "person_zzz"}},
                "files": {"files": [file_a["id"]]},
            }],
        }
        plan = self.agent.prepare(
            ACTOR, decision, self.operation_id, [file_a["id"]],
            references={"person_zzz": "ou_123"},
        )
        self.assertIsInstance(plan["_references"], dict)
        self.assertEqual(plan["_references"]["person_zzz"], "ou_123")

    async def test_http_200_ok_false_stops_sequential_operations(self):
        decision = {
            "operations": [
                {"api_id": "POST /api/step-one"},
                {"api_id": "POST /api/step-two"},
            ],
        }
        plan = self.agent.prepare(ACTOR, decision, self.operation_id, [])
        await self.agent._execute(ACTOR, plan, self.request)

        self.assertEqual(self.sequential_writes, ["step-one"])
        self.assertEqual(plan["status"], "failed")
        self.assertIn("业务被拒绝", plan["error"])

    async def test_repair_record_pickers_use_native_candidates_and_clear_optional_multiselect(self):
        app, calls, writes = FastAPI(), [], []
        @app.get("/api/repair-management/records")
        async def projects(scope: str, q: str = "", limit: int = 80):
            calls.append(("projects", scope, q))
            return {"ok": True, "data": {"records": [{"record_id": "rec-project", "title": "A楼维修项目"}], "total": 1}}
        @app.get("/api/repair-management/cmdb-candidates")
        async def devices(scope: str, q: str = "", limit: int = 80):
            calls.append(("devices", scope, q))
            return {"ok": True, "data": {"records": [{"record_id": f"rec-device-{i}", "name": "柴油发电机", "unique_id": f"5.14.{i}"} for i in range(2)], "has_more": True}}
        @app.post("/api/repair-management/followups")
        async def followup(body: RepairFollowupRecordRequest):
            writes.append(body.model_dump())
            return {"ok": True, "data": {"record_id": "rec-followup"}}
        agent = PortalAgent(self.assistant, PortalAPICatalog(app), self.files)
        plan = agent.prepare(ACTOR, {"operations": [{"api_id": "POST /api/repair-management/followups", "body": {"scope": "A", "fields": {"维修进度": 58}}}]}, self.operation_id, [])
        self.assertEqual(plan["status"], "needs_input")
        fields = {f["path"]: f for f in plan["fields"]}
        self.assertEqual(fields["summary_record_id"]["type"], "select")
        self.assertEqual(fields["cmdb_record_ids"]["type"], "multiselect")
        for path in ("summary_record_id", "cmdb_record_ids"):
            plan = await agent.field_options(ACTOR, plan["id"], fields[path]["name"], self.request)
        raw = agent.get_plan(ACTOR, plan["id"])
        device = next(f for f in raw["fields"] if f["path"] == "cmdb_record_ids")
        self.assertTrue(device["options_has_more"])
        self.assertIn("5.14.0", device["options"][0]["label"])
        self.assertEqual(calls, [("projects", "A", ""), ("devices", "A", "")])
        with self.assertRaises(AssistantError):
            agent.amend(ACTOR, plan["id"], {"version": plan["version"], "values": {fields["summary_record_id"]["name"]: "rec-project", fields["cmdb_record_ids"]["name"]: ["forged-id"]}})
        plan = agent.amend(ACTOR, plan["id"], {"version": plan["version"], "values": {fields["summary_record_id"]["name"]: "rec-project", fields["cmdb_record_ids"]["name"]: ["rec-device-0", "rec-device-1"]}})
        await agent._execute(ACTOR, agent.get_plan(ACTOR, plan["id"]), self.request)
        self.assertEqual(writes[0]["summary_record_id"], "rec-project")
        self.assertEqual(writes[0]["cmdb_record_ids"], ["rec-device-0", "rec-device-1"])
        clear = agent.prepare(ACTOR, {"operations": [{"api_id": "POST /api/repair-management/followups", "body": {"scope": "A", "summary_record_id": "rec-project", "cmdb_record_ids": ["rec-device-0"], "fields": {}}}],
            "fields": [{"name": "clear_devices", "path": "cmdb_record_ids", "section": "body", "type": "multiselect", "required": False, "options_source": "repair_devices", "options": []}]}, self.operation_id, [])
        clear = agent.amend(ACTOR, clear["id"], {"version": clear["version"], "values": {"clear_devices": []}})
        self.assertEqual(agent.get_plan(ACTOR, clear["id"])["operations"][0]["body"]["cmdb_record_ids"], [])

    async def test_repair_native_fields_keep_baseline_version_and_replace_only_selected_relation(self):
        from lan_bitable_template_portal.portal_service import REPAIR_MANAGEMENT_TABLE_ID
        app, writes = FastAPI(), []
        @app.put("/api/repair-management/records/{record_id}")
        async def update(record_id: str, body: RepairManagementRecordRequest):
            writes.append(body.model_dump())
            return {"ok": True}
        agent = PortalAgent(self.assistant, PortalAPICatalog(app), self.files)
        metas = [{"field_name": name, "field_type": kind, "editable": editable, "options": options} for name, kind, editable, options in (
            ("故障发生时间", 5, True, []), ("故障维修原因", 1, True, []), ("所属专业", 3, True, ["电气", "暖通"]),
            ("证据.说明", 17, False, []), ("汇总公式", 20, False, []))]
        original = {"故障发生时间": 1790821800000, "故障维修原因": [{"text": "原原因"}], "所属专业": "电气",
                    "证据.说明": {"content": "原证明", "file_token": "private-proof"}, "汇总公式": "不编辑"}
        record = {"record_id": "rec-project", "record_version": "v-original", "building_codes": ["A"], "raw_fields": original,
                  "source_event_id": "rec-event", "source_repair_ids": ["rec-repair"]}
        queries = {"query_" + "a" * 32: {"table_id": REPAIR_MANAGEMENT_TABLE_ID, "records": [record], "fields": metas}}
        operation = {"api_id": "PUT /api/repair-management/records/{record_id}", "path_params": {"record_id": "rec-project"}, "body": {"scope": "A"}}
        plan = agent.prepare(ACTOR, {"operations": [operation], "fields": [{"name": "clear_relation", "path": "source_repair_ids", "type": "multiselect", "required": False}]}, self.operation_id, [], queries=queries)
        control = next(field for field in plan["fields"] if field["path"] == "fields")
        children = {child["path"]: child for child in control["children"]}
        self.assertEqual(set(children), {"故障发生时间", "故障维修原因", "所属专业"})
        self.assertEqual(children["故障发生时间"]["type"], "datetime-local")
        self.assertEqual(children["所属专业"]["options"], [{"value": "电气", "label": "电气"}, {"value": "暖通", "label": "暖通"}])
        public_value = agent.public_plan(plan)["fields"][0]["value"]
        self.assertNotIn("private-proof", json.dumps(public_value))
        self.assertEqual(_result_refs(public_value, [], queries=plan["_queries"]), original)
        for invalid in ({"所属专业": "编造专业"}, {"故障发生时间": "2026-02-30T10:00"}, {"汇总公式": "覆盖公式"}):
            with self.assertRaises(AssistantError):
                agent.amend(ACTOR, plan["id"], {"version": 1, "values": {control["name"]: {**public_value, **invalid}}})
        self.assertEqual(writes, [])
        amended = agent.amend(ACTOR, plan["id"], {"version": 1, "values": {control["name"]: {**public_value, "故障维修原因": "已更正"}, "clear_relation": []}})
        await agent._execute(ACTOR, agent.get_plan(ACTOR, amended["id"]), self.request)
        self.assertEqual(writes[0]["expected_version"], "v-original")
        self.assertTrue(writes[0]["replace_source_relations"])
        self.assertEqual(writes[0]["source_event_id"], "rec-event")
        self.assertEqual(writes[0]["source_repair_ids"], [])
        self.assertEqual(writes[0]["fields"], {"故障维修原因": "已更正"})
        self.assertEqual({**original, **writes[0]["fields"]}, {**original, "故障维修原因": "已更正"})
        queries[next(iter(queries))]["records"][0]["building_codes"] = ["B"]
        with self.assertRaisesRegex(AssistantError, "无权"):
            agent.prepare(ACTOR, {"operations": [operation]}, self.operation_id, [], queries=queries)

    async def test_repair_search_retains_only_selected_candidates_and_validates_scope(self):
        from urllib.parse import urlencode
        app, calls = FastAPI(), []
        @app.get("/api/repair-management/cmdb-candidates")
        async def devices(scope: str, q: str = "", limit: int = 80):
            calls.append(q)
            return {"ok": True, "data": {"records": [{"record_id": "rec-" + q, "name": q, "building_codes": [scope]}]}}
        @app.post("/api/repair-management/followups")
        async def create(body: RepairFollowupRecordRequest):
            return {"ok": True}
        agent = PortalAgent(self.assistant, PortalAPICatalog(app), self.files)
        actor = {**ACTOR, "scopes": ["A", "B"]}
        plan = agent.prepare(actor, {"operations": [{"api_id": "POST /api/repair-management/followups", "body": {"scope": "A", "summary_record_id": "rec-parent", "fields": {}}}]}, self.operation_id, [])
        field = next(f for f in plan["fields"] if f["path"] == "cmdb_record_ids")
        async def search(q, selected, scope="A"):
            return await agent.field_options(actor, plan["id"], field["name"], Request({**self.request.scope,
                "query_string": urlencode({"q": q, "scope": scope, "selected": json.dumps(selected)}).encode()}))
        await search("one", [])
        loaded = await search("two", ["rec-one"])
        options = next(f for f in loaded["fields"] if f["path"] == "cmdb_record_ids")["options"]
        self.assertEqual([option["value"] for option in options], ["rec-one", "rec-two"])
        for selected in (["forged"], "rec-one", [None], ["rec-one"] * 501):
            with self.assertRaises(AssistantError):
                await search("bad", selected)
        with self.assertRaisesRegex(AssistantError, "楼栋范围已改变"):
            await search("wrong-scope", ["rec-one"], "B")
        with self.assertRaisesRegex(AssistantError, "无权"):
            await search("forbidden", [], "E")
        self.assertEqual(calls, ["one", "two"])
        loaded = await search("three", ["rec-one"])
        options = next(f for f in loaded["fields"] if f["path"] == "cmdb_record_ids")["options"]
        self.assertEqual([option["value"] for option in options], ["rec-one", "rec-three"])
        amended = agent.amend(actor, plan["id"], {"version": loaded["version"], "values": {field["name"]: ["rec-one", "rec-three"]}})
        self.assertEqual(amended["operations"][0]["body"]["cmdb_record_ids"], ["rec-one", "rec-three"])

    async def test_followup_form_uses_only_matching_parent_metadata_and_keeps_other_fields(self):
        app, writes = FastAPI(), []
        @app.put("/api/repair-management/followups/{record_id}")
        async def update(record_id: str, body: RepairFollowupRecordRequest):
            writes.append(body.model_dump())
            return {"ok": True}
        agent = PortalAgent(self.assistant, PortalAPICatalog(app), self.files)
        raw = {"维修进度": 0.58, "设备品牌": "双登", "故障维修总费用": 12, "设备型号": "保持原型号", "维修进展描述": [{"text": "原进展"}]}
        metas = [{"field_name": name, "field_type": kind, "editable": True, "options": options} for name, kind, options in (
            ("维修进度", 2, []), ("设备品牌", 3, ["双登"]), ("故障维修总费用", 2, []), ("设备型号", 1, []), ("维修进展描述", 1, []))]
        matching = {"summary_record_id": "rec-parent", "relation_mode": "record_id", "fields": metas,
                    "records": [{"record_id": "rec-followup", "record_version": "followup-v1", "raw_fields": raw}]}
        queries = {"query_" + "b" * 32: matching, "query_" + "c" * 32: {**matching, "summary_record_id": "rec-other", "fields": [{**metas[1], "options": ["其它项目品牌"]}]}}
        op = {"api_id": "PUT /api/repair-management/followups/{record_id}", "path_params": {"record_id": "rec-followup"}, "body": {"scope": "A", "summary_record_id": "rec-parent", "cmdb_record_ids": []}}
        plan = agent.prepare(ACTOR, {"operations": [op]}, self.operation_id, [], queries=queries)
        field = next(field for field in plan["fields"] if field["path"] == "fields")
        self.assertEqual(next(child for child in field["children"] if child["path"] == "设备品牌")["options"], [{"value": "双登", "label": "双登"}])
        value = {**agent.public_plan(plan)["fields"][0]["value"], "维修进度": "0.75", "维修进展描述": "已处理"}
        amended = agent.amend(ACTOR, plan["id"], {"version": 1, "values": {field["name"]: value}})
        await agent._execute(ACTOR, agent.get_plan(ACTOR, amended["id"]), self.request)
        self.assertEqual(writes[0]["expected_version"], "followup-v1")
        self.assertEqual(writes[0]["fields"], {"维修进度": "0.75", "维修进展描述": "已处理"})
        self.assertEqual({**raw, **writes[0]["fields"]}, {**raw, "维修进度": "0.75", "维修进展描述": "已处理"})
        with self.assertRaisesRegex(AssistantError, "字段定义"):
            agent.prepare(ACTOR, {"operations": [op], "fields": [{"path": "fields", "type": "object"}]}, self.operation_id, [], queries={})

    async def test_repair_query_references_open_original_form_instead_of_losing_controls(self):
        app = FastAPI()
        @app.put("/api/repair-management/followups/{record_id}")
        async def update(record_id: str, body: RepairFollowupRecordRequest):
            return {"ok": True}
        agent = PortalAgent(self.assistant, PortalAPICatalog(app), self.files)
        ref = "query_" + "a" * 32
        snapshot = {"summary_record_id": "rec-parent", "relation_mode": "record_id", "fields": [
            {"field_name": "设备名称", "field_type": 1, "editable": True}], "records": [
                {"record_id": "rec-followup", "record_version": "v1", "raw_fields": {"设备名称": "原设备"}, "cmdb_record_ids": ["rec-device"]}]}
        operation = {"api_id": "PUT /api/repair-management/followups/{record_id}",
            "path_params": {"record_id": {"$query": {"ref": ref, "path": "records.0.record_id"}}},
            "body": {"scope": "A", "summary_record_id": {"$query": {"ref": ref, "path": "summary_record_id"}}}}
        plan = agent.prepare(ACTOR, {"operations": [operation]}, self.operation_id, [], queries={ref: snapshot})
        form = next(field for field in plan["fields"] if field.get("native_repair"))
        self.assertEqual(form["value"]["设备名称"], "原设备")
        self.assertEqual(plan["operations"][0]["path_params"]["record_id"], "rec-followup")
        self.assertEqual(next(field for field in plan["fields"] if field["path"] == "cmdb_record_ids")["value"], ["rec-device"])

    async def test_repair_single_select_uses_resolved_display_label_without_unrequested_write(self):
        app, writes = FastAPI(), []
        @app.put("/api/repair-management/followups/{record_id}")
        async def update(record_id: str, body: RepairFollowupRecordRequest):
            writes.append(body.model_dump())
            return {"ok": True}
        agent = PortalAgent(self.assistant, PortalAPICatalog(app), self.files)
        original = {"record_id": "rec-followup", "record_version": "v1", "raw_fields": {"设备品牌": "optLegacy", "维修进度": 0.5},
                    "display_fields": {"设备品牌": "品牌A"}}
        snapshot = {"summary_record_id": "rec-parent", "relation_mode": "record_id", "records": [original], "fields": [
            {"field_name": "设备品牌", "field_type": 3, "editable": True, "options": ["品牌A", "品牌B"]},
            {"field_name": "维修进度", "field_type": 2, "editable": True}]}
        op = {"api_id": "PUT /api/repair-management/followups/{record_id}", "path_params": {"record_id": "rec-followup"},
              "body": {"scope": "A", "summary_record_id": "rec-parent"}}
        plan = agent.prepare(ACTOR, {"operations": [op]}, self.operation_id, [], queries={"snapshot": snapshot})
        field = next(f for f in plan["fields"] if f.get("native_repair"))
        self.assertEqual(field["value"]["设备品牌"], "品牌A")
        value = {**field["value"], "维修进度": 0.7}
        amended = agent.amend(ACTOR, plan["id"], {"version": plan["version"], "values": {field["name"]: value}})
        self.assertEqual(amended["operations"][0]["body"]["fields"], {"维修进度": 0.7})
        self.assertEqual(original["raw_fields"]["设备品牌"], "optLegacy")
        original["raw_fields"]["设备品牌"] = "品牌B"
        plan = agent.prepare(ACTOR, {"operations": [op]}, self.operation_id, [], queries={"snapshot": snapshot})
        self.assertEqual(next(f for f in plan["fields"] if f.get("native_repair"))["value"]["设备品牌"], "品牌B")

    async def test_changed_event_requires_fresh_selection_even_for_same_repair_record(self):
        from urllib.parse import urlencode
        from lan_bitable_template_portal.portal_service import REPAIR_MANAGEMENT_TABLE_ID
        app = FastAPI()
        @app.put("/api/repair-management/records/{record_id}")
        async def update(record_id: str, body: RepairManagementRecordRequest):
            return {"ok": True}
        @app.get("/api/repair-management/repair-candidates")
        async def candidates(scope: str, event_record_id: str, q: str = "", limit: int = 80):
            self.assertEqual(event_record_id, "rec-new-event")
            return {"ok": True, "data": {"records": [{"record_id": "rec-repair", "title": "可重新绑定的检修"}]}}
        agent = PortalAgent(self.assistant, PortalAPICatalog(app), self.files)
        record = {"record_id": "rec-project", "building_codes": ["A"], "record_version": "v1", "raw_fields": {},
                  "source_event_id": "rec-old-event", "source_repair_ids": ["rec-repair"]}
        decision = {"operations": [{"api_id": "PUT /api/repair-management/records/{record_id}",
            "path_params": {"record_id": "rec-project"}, "body": {"scope": "A", "source_event_id": "rec-new-event"}}]}
        queries = {"snapshot": {"table_id": REPAIR_MANAGEMENT_TABLE_ID, "records": [record], "fields": []}}
        plan = agent.prepare(ACTOR, decision, self.operation_id, [], queries=queries)
        amended = agent.amend(ACTOR, plan["id"], {"version": plan["version"], "values": {}})
        self.assertEqual(amended["operations"][0]["body"]["source_repair_ids"], [])
        plan = agent.prepare(ACTOR, decision, self.operation_id, [], queries=queries)
        field = next(field for field in plan["fields"] if field["path"] == "source_repair_ids")
        loaded = await agent.field_options(ACTOR, plan["id"], field["name"], Request({**self.request.scope,
            "query_string": urlencode({"source_event_id": "rec-new-event", "selected": "[]"}).encode()}))
        amended = agent.amend(ACTOR, plan["id"], {"version": loaded["version"], "values": {field["name"]: ["rec-repair"]}})
        self.assertEqual(amended["operations"][0]["body"]["source_repair_ids"], ["rec-repair"])

    async def test_guard_form_keeps_other_checks_signers_and_version_until_confirmed(self):
        from lan_bitable_template_portal.critical_guard import default_response_cells, normalize_response_cells
        app, writes = FastAPI(), []
        @app.put("/api/critical-guard/responses/{response_id}")
        async def save(response_id: str, body: CriticalGuardResponseRequest):
            writes.append(body.model_dump())
            return {"ok": True}
        agent = PortalAgent(self.assistant, PortalAPICatalog(app), self.files)
        original = default_response_cells("灾害专项", "A", today="2026-10-01", template_items=[
            {"key": "check.1", "category": "供配电", "content": "现场检查1"},
            {"key": "check.2", "category": "空调", "content": "现场检查2"}], template_revision=7, template_customized=True)
        original["checks"]["check.2"] = {"status": "abnormal", "note": "原异常备注"}
        original["weather"] = {"level1": "台风", "level2": "暴雨", "current": "大雨"}
        signers = [{"source": "staff", "record_id": "rec-inspector", "role": "inspector", "name": "检查人"}]
        response = {"response_id": "guard-a", "scope": "A", "version": 4, "sheet_type": "灾害专项", "cells": original, "signatures": signers}
        query = {"query_" + "d" * 32: {"task_id": "guard-task", "responses": [response], "template_outdated": False}}
        op = {"api_id": "PUT /api/critical-guard/responses/{response_id}", "path_params": {"response_id": "guard-a"},
              "body": {"cells": {"checks": {"check.1": {"note": "本次备注"}}, "weather": {"current": "小雨"}}}}
        plan = agent.prepare(ACTOR, {"operations": [op]}, self.operation_id, [], queries=query)
        self.assertEqual(writes, [])
        field = next(field for field in plan["fields"] if field["path"] == "cells")
        self.assertTrue(field["native_guard"])
        value = agent.public_plan(plan)["fields"][0]["value"]
        filled = _result_refs(value, [], queries=plan["_queries"])
        self.assertEqual(filled["checks"]["check.2"], original["checks"]["check.2"])
        self.assertEqual(filled["weather"], {**original["weather"], "current": "小雨"})
        for invalid in ({"machine_room": "其它楼"}, {"template_revision": 999}, {"check_date": "2026-02-30"}, {"checks": {**value["checks"], "check.1": {"status": "编造状态"}}}):
            with self.assertRaises(AssistantError):
                agent.amend(ACTOR, plan["id"], {"version": 1, "values": {field["name"]: {**value, **invalid}}})
        value["checks"]["check.1"].update(status="abnormal", note="")
        with self.assertRaisesRegex(AssistantError, "异常项必须填写备注"):
            agent.amend(ACTOR, plan["id"], {"version": 1, "values": {field["name"]: value, "step0.generate_image": True}})
        self.assertEqual(writes, [])
        draft = agent.amend(ACTOR, plan["id"], {"version": 1, "values": {field["name"]: value, "step0.generate_image": False}})
        raw = agent.get_plan(ACTOR, draft["id"])
        self.assertEqual(raw["operations"][0]["body"]["expected_version"], 4)
        self.assertEqual(raw["operations"][0]["body"]["signatures"], signers)
        normalized = normalize_response_cells("灾害专项", "A", raw["operations"][0]["body"]["cells"])
        self.assertEqual(normalized["template_revision"], 7)
        self.assertEqual(normalized["checks"]["check.2"], original["checks"]["check.2"])
        confirmed = await agent.confirm(ACTOR, plan["id"], {"version": draft["version"], "stage": "review"}, self.request)
        if confirmed["status"] == "awaiting_second_confirmation":
            await agent.confirm(ACTOR, plan["id"], {"version": confirmed["version"], "stage": "execute"}, self.request)
        await gather_tasks(agent)
        self.assertEqual(len(writes), 1)
        self.assertFalse(writes[0]["generate_image"])
        self.assertEqual(writes[0]["cells"]["checks"]["check.1"], {"status": "abnormal", "note": ""})
        with self.assertRaisesRegex(AssistantError, "原重保任务"):
            agent.prepare(ACTOR, {"operations": [op]}, self.operation_id, [], queries={})
        query[next(iter(query))]["template_outdated"] = True
        with self.assertRaisesRegex(AssistantError, "模板已变化"):
            agent.prepare(ACTOR, {"operations": [op]}, self.operation_id, [], queries=query)
        query[next(iter(query))]["template_outdated"] = False
        response["scope"] = "B"
        with self.assertRaisesRegex(AssistantError, "无权"):
            agent.prepare(ACTOR, {"operations": [op]}, self.operation_id, [], queries=query)

    async def test_guard_signer_picker_binds_only_native_people_and_retains_other_selections(self):
        from lan_bitable_template_portal.critical_guard import default_response_cells
        app, calls, writes = FastAPI(), [], []
        @app.get("/api/signatures/people")
        async def people(scope: str, notice_key: str, q: str = "", limit: int = 100):
            calls.append((scope, notice_key, q))
            return {"ok": True, "data": {"people": [{"source": "staff", "record_id": "rec-person", "name": "检查人甲", "has_signature": True,
                "usage_confirmed": False, "open_id": "private-open-id", "image_base64": "private-signature-image"}], "count": 102}}
        @app.get("/api/signatures/temporary/people")
        async def external(scope: str, notice_key: str, q: str = "", limit: int = 100):
            return {"ok": True, "data": {"people": [{"source": "external", "record_id": "rec-external", "name": "检查人乙", "has_signature": False}], "count": 1}}
        @app.put("/api/critical-guard/responses/{response_id}")
        async def save(response_id: str, body: CriticalGuardResponseRequest):
            writes.append(body.model_dump())
            return {"ok": True}
        agent = PortalAgent(self.assistant, PortalAPICatalog(app), self.files)
        response = {"response_id": "guard-signers", "task_id": "native-task", "scope": "A", "version": 3, "sheet_type": "设备安全",
                    "cells": default_response_cells("设备安全", "A", today="2026-10-01", template_items=[{"key": "check.1", "content": "检查"}]),
                    "signatures": [{"source": "staff", "record_id": "rec-original", "role": "inspector", "name": "原检查人"}]}
        op = {"api_id": "PUT /api/critical-guard/responses/{response_id}", "path_params": {"response_id": response["response_id"]}, "body": {"scope": "A"}}
        plan = agent.prepare(ACTOR, {"operations": [op], "fields": [{"path": "signatures", "type": "array"}]}, self.operation_id, [], queries={"original": response})
        signer = next(field for field in plan["fields"] if field["path"] == "signatures")
        original_ref = signer["value"][0]
        self.assertEqual(signer["type"], "multiselect")
        await agent.field_options(ACTOR, plan["id"], signer["name"], self.request)
        raw = agent.get_plan(ACTOR, plan["id"])
        signer = next(field for field in raw["fields"] if field["path"] == "signatures")
        self.assertTrue(signer["options_has_more"])
        self.assertIn(original_ref, [option["value"] for option in signer["options"]])
        self.assertTrue(any("待本人确认" in option["label"] for option in signer["options"]))
        self.assertTrue(any("未签名" in option["label"] for option in signer["options"]))
        self.assertEqual(calls, [("A", "critical_guard:native-task:A", "")])
        self.assertNotIn("private-", json.dumps(agent.public_plan(raw)))
        field = next(field for field in raw["fields"] if field["path"] == "cells")
        value = agent.public_plan(raw)["fields"][0]["value"]
        with self.assertRaises(AssistantError):
            agent.amend(ACTOR, plan["id"], {"version": raw["version"], "values": {field["name"]: value, signer["name"]: ["forged-person"]}})
        choices = [option["value"] for option in signer["options"]]
        amended = agent.amend(ACTOR, plan["id"], {"version": raw["version"], "values": {field["name"]: value, signer["name"]: choices}})
        await agent._execute(ACTOR, agent.get_plan(ACTOR, amended["id"]), self.request)
        self.assertEqual([person["record_id"] for person in writes[0]["signatures"]], ["rec-original", "rec-person", "rec-external"])
        self.assertEqual({key for person in writes[0]["signatures"] for key in person}, {"source", "record_id", "role", "name"})
        fresh = agent.prepare(ACTOR, {"operations": [op]}, self.operation_id, [], queries={"original": response})
        signer = next(field for field in fresh["fields"] if field["path"] == "signatures")
        wrong = Request({**self.request.scope, "query_string": b"scope=B"})
        with self.assertRaisesRegex(AssistantError, "无权"):
            await agent.field_options(ACTOR, fresh["id"], signer["name"], wrong)
        self.assertEqual(len(calls), 1)

    def test_guard_file_form_retains_existing_source_file_without_manual_rows(self):
        from lan_bitable_template_portal.critical_guard import default_response_cells
        app = FastAPI()
        @app.put("/api/critical-guard/responses/{response_id}")
        async def save(response_id: str, body: CriticalGuardResponseRequest):
            raise AssertionError("must not write during preparation")
        agent = PortalAgent(self.assistant, PortalAPICatalog(app), self.files)
        original = default_response_cells("物资检查清单", "A", today="2026-10-01")
        original.update(source_file_id="guard-file", source_file_name="物资.xlsx", source_file_sha256="original-sha")
        response = {"response_id": "file-response", "scope": "A", "version": 2, "sheet_type": "物资检查清单", "cells": original}
        op = {"api_id": "PUT /api/critical-guard/responses/{response_id}", "path_params": {"response_id": "file-response"}}
        plan = agent.prepare(ACTOR, {"operations": [op]}, self.operation_id, [], queries={"snapshot": response})
        field = next(field for field in plan["fields"] if field["path"] == "cells")
        self.assertEqual([child["path"] for child in field["children"]], ["check_date"])
        value = agent.public_plan(plan)["fields"][0]["value"]
        self.assertEqual(_result_refs(value, [], queries=plan["_queries"])["source_file_id"], "guard-file")
        with self.assertRaisesRegex(AssistantError, "只读字段"):
            agent.amend(ACTOR, plan["id"], {"version": 1, "values": {field["name"]: {**value, "source_file_id": "forged-file"}}})

    async def test_guard_upload_then_generate_uses_new_file_and_version_with_user_date(self):
        from lan_bitable_template_portal.critical_guard import default_response_cells
        app, calls = FastAPI(), []
        original = default_response_cells("物资检查清单", "A", today="2026-10-01")
        original.update(source_file_id="old-source", source_file_name="旧物资.xlsx", source_file_sha256="old-hash")
        response = {"response_id": "file-generate", "task_id": "guard-task", "scope": "A", "version": 2, "sheet_type": "物资检查清单", "cells": original}
        @app.post("/api/critical-guard/source-files")
        async def upload(file: UploadFile = File(...), scope: str = Form(""), response_id: str = Form(""), expected_version: str = Form("")):
            calls.append(("upload", response_id, scope, expected_version, await file.read()))
            return {"ok": True, "data": {**response, "version": 3, "cells": {**original, "source_file_id": "new-source", "source_file_name": "新物资.xlsx", "source_file_sha256": "new-hash"}}}
        @app.put("/api/critical-guard/responses/{response_id}")
        async def generate(response_id: str, body: CriticalGuardResponseRequest):
            calls.append(("generate", body.model_dump()))
            return {"ok": True, "data": {"generated": True}}
        agent = PortalAgent(self.assistant, PortalAPICatalog(app), self.files)
        file = self.files.upload(ACTOR, "新物资.xlsx", b"fake-xlsx-test-content", extract=False)
        operations = [{"api_id": "POST /api/critical-guard/source-files", "body": {"scope": "A", "response_id": response["response_id"], "expected_version": "2"}, "files": {"file": [file["id"]]}},
                      {"api_id": "PUT /api/critical-guard/responses/{response_id}", "path_params": {"response_id": response["response_id"]},
                       "body": {"scope": "A", "generate_image": True, "cells": {"$result": {"step": 0, "path": "cells"}}}}]
        plan = agent.prepare(ACTOR, {"operations": operations}, self.operation_id, [file["id"]], queries={"original": response})
        self.assertEqual(calls, [])
        field = next(field for field in plan["fields"] if field["path"] == "cells")
        value = {**agent.public_plan(plan)["fields"][0]["value"], "check_date": "2026-10-07"}
        amended = agent.amend(ACTOR, plan["id"], {"version": 1, "values": {field["name"]: value}})
        await agent._execute(ACTOR, agent.get_plan(ACTOR, amended["id"]), self.request)
        self.assertEqual(calls[0], ("upload", "file-generate", "A", "2", b"fake-xlsx-test-content"))
        self.assertEqual(calls[1][1]["expected_version"], 3)
        self.assertEqual(calls[1][1]["cells"]["source_file_id"], "new-source")
        self.assertEqual(calls[1][1]["cells"]["source_file_sha256"], "new-hash")
        self.assertEqual(calls[1][1]["cells"]["check_date"], "2026-10-07")
        forged = copy.deepcopy(operations)
        forged[0]["body"]["response_id"] = "another-response"
        with self.assertRaisesRegex(AssistantError, "同一填报记录"):
            agent.prepare(ACTOR, {"operations": forged}, self.operation_id, [file["id"]], queries={"original": response})

    def test_guard_task_uses_native_sheet_and_building_options_not_text_arrays(self):
        from lan_bitable_template_portal.critical_guard import CRITICAL_GUARD_SHEET_NAMES, CRITICAL_GUARD_SCOPE_CODES
        app = FastAPI()
        @app.post("/api/critical-guard/tasks")
        async def create(body: CriticalGuardTaskRequest):
            raise AssertionError("cannot write during prepare")
        agent = PortalAgent(self.assistant, PortalAPICatalog(app), self.files)
        actor = {**ACTOR, "scopes": list("ABCDE"), "is_admin": True}
        plan = agent.prepare(actor, {"operations": [{"api_id": "POST /api/critical-guard/tasks", "body": {"name": "重保任务"}}],
            "fields": [{"path": "sheet_types", "type": "array"}, {"path": "target_scopes", "type": "array"}]}, self.operation_id, [])
        fields = {field["path"]: field for field in plan["fields"]}
        self.assertEqual(fields["sheet_types"]["type"], "multiselect")
        self.assertEqual([option["value"] for option in fields["sheet_types"]["options"]], list(CRITICAL_GUARD_SHEET_NAMES))
        self.assertEqual([option["value"] for option in fields["target_scopes"]["options"]], list(CRITICAL_GUARD_SCOPE_CODES))
        self.assertEqual(fields["target_scopes"]["options"][0]["label"], "A楼")
        with self.assertRaises(AssistantError):
            agent.amend(actor, plan["id"], {"version": 1, "values": {fields["sheet_types"]["name"]: ["不存在的表"], fields["target_scopes"]["name"]: ["A"]}})
        narrow = agent.prepare(ACTOR, {"operations": [{"api_id": "POST /api/critical-guard/tasks"}], "fields": [{"path": "target_scopes", "type": "array"}]}, self.operation_id, [])
        self.assertEqual(next(field for field in narrow["fields"] if field["path"] == "target_scopes")["options"], [{"value": "A", "label": "A楼"}])
        with self.assertRaises(AssistantError):
            agent.amend(actor, plan["id"], {"version": 1, "values": {fields["sheet_types"]["name"]: ["设备安全"], fields["target_scopes"]["name"]: []}})

    async def test_guard_template_editor_uses_native_atomic_save_reset_and_conflicts(self):
        from lan_bitable_template_portal.critical_guard import critical_guard_catalog, default_response_cells
        from lan_bitable_template_portal.portal_service import MaintenancePortalService, PortalConflictError
        from lan_bitable_template_portal.state_store import LanPortalStateStore
        state = LanPortalStateStore(Path(self.tmp.name) / "guard.sqlite3")
        cells = default_response_cells("设备安全", "A", today="2026-10-02")
        first, second = [item["key"] for item in cells["template_items"][:2]]
        cells["checks"][first] = {"status": "abnormal", "note": "原异常"}
        cells["checks"][second] = {"status": "abnormal", "note": "保留异常"}
        cells["suggestions"] = "原整改建议"
        state.create_critical_guard_task(task_id="guard-template-task", operation_id="create-guard-fixture", task_name="模板测试", memory_key="模板测试",
            sheet_types=["设备安全"], target_scopes=["A"], template_version=critical_guard_catalog()["template_version"],
            created_by_open_id="fixture-user", created_by_name="测试人",
            responses=[{"response_id": "guard-template-response", "scope": "A", "sheet_type": "设备安全", "cells": cells}])
        service = MaintenancePortalService.__new__(MaintenancePortalService)
        service._state_store = state
        app, writes = FastAPI(), []

        def save(body, reset):
            writes.append(body.model_dump())
            try:
                result = service.update_critical_guard_scope_template(scope=body.scope, sheet_type=body.sheet_type, items=body.items,
                    reset_to_default=reset, expected_revision=body.expected_revision, response_id=body.response_id,
                    response_cells=body.cells, expected_response_version=body.expected_response_version,
                    operation_id=body.operation_id, operator_open_id="fixture-user", operator_name="测试人")
                return {"ok": True, "data": result}
            except PortalConflictError as exc:
                return JSONResponse({"ok": False, "error": str(exc)}, status_code=409)

        @app.put("/api/critical-guard/scope-template")
        async def update(body: CriticalGuardScopeTemplateRequest):
            return save(body, False)
        @app.post("/api/critical-guard/scope-template/reset")
        async def reset(body: CriticalGuardScopeTemplateRequest):
            return save(body, True)

        agent = PortalAgent(self.assistant, PortalAPICatalog(app), self.files)
        def prepare(api_id="PUT /api/critical-guard/scope-template", **body):
            response = state.get_critical_guard_response("guard-template-response")
            template = service.get_critical_guard_scope_template(scope="A", sheet_type="设备安全")
            return agent.prepare(ACTOR, {"operations": [{"api_id": api_id, "body": {"scope": "A", "sheet_type": "设备安全", "response_id": response["response_id"], **body}}]},
                self.operation_id, [], queries={"template": template, "task": {"responses": [response]}})

        plan = prepare(expected_revision=999, expected_response_version=999)
        self.assertEqual(writes, [])
        self.assertEqual(plan["operations"][0]["body"]["expected_revision"], 0)
        field = plan["fields"][0]
        public_items = copy.deepcopy(agent.public_plan(plan)["fields"][0]["value"])
        self.assertEqual(field["path"], "items")
        public_items[0]["content"] = "新的检查内容"
        public_items.append({"key": "new-stable-key", "category": "楼内", "content": "新增检查"})
        with self.assertRaises(AssistantError):
            agent.amend(ACTOR, plan["id"], {"version": 1, "values": {field["name"]: []}})
        forged = copy.deepcopy(public_items)
        forged[1]["private_field"] = "forged"
        with self.assertRaises(AssistantError):
            agent.amend(ACTOR, plan["id"], {"version": 1, "values": {field["name"]: forged}})
        amended = agent.amend(ACTOR, plan["id"], {"version": 1, "values": {field["name"]: public_items}})
        reviewed = await agent.confirm(ACTOR, plan["id"], {"version": amended["version"], "stage": "review"}, self.request)
        self.assertEqual(reviewed["status"], "awaiting_second_confirmation")
        await agent.confirm(ACTOR, plan["id"], {"version": reviewed["version"], "stage": "execute"}, self.request)
        await gather_tasks(agent)
        done = agent.get_plan(ACTOR, plan["id"])
        self.assertEqual(done["status"], "completed", done.get("error"))
        await agent.confirm(ACTOR, plan["id"], {"version": done["version"], "stage": "execute"}, self.request)
        self.assertEqual(len(writes), 1)
        saved = state.get_critical_guard_response("guard-template-response")
        self.assertEqual(saved["cells"]["checks"][first], {"status": "normal", "note": ""})
        self.assertEqual(saved["cells"]["checks"][second], cells["checks"][second])
        self.assertEqual(saved["cells"]["suggestions"], cells["suggestions"])
        self.assertEqual(saved["cells"]["template_items"][-1]["key"], "new-stable-key")
        self.assertFalse(service.get_critical_guard_scope_template(scope="B", sheet_type="设备安全")["customized"])

        restored_plan = prepare("POST /api/critical-guard/scope-template/reset")
        self.assertEqual(restored_plan["fields"], [])
        await agent._execute(ACTOR, restored_plan, self.request)
        restored = state.get_critical_guard_response("guard-template-response")
        self.assertEqual(restored["cells"]["template_items"], cells["template_items"])
        self.assertEqual(restored["cells"]["checks"][second], cells["checks"][second])
        stale = prepare()
        update = writes[0]
        concurrent = service.update_critical_guard_scope_template(scope="A", sheet_type="设备安全", items=update["items"],
            reset_to_default=False, expected_revision=2, response_id=restored["response_id"], response_cells=restored["cells"],
            expected_response_version=restored["version"], operator_open_id="fixture-user", operator_name="测试人", operation_id="concurrent-template-fixture")
        await agent._execute(ACTOR, stale, self.request)
        self.assertEqual(agent.get_plan(ACTOR, stale["id"])["status"], "failed")
        self.assertEqual(state.get_critical_guard_response(restored["response_id"])["version"], concurrent["response"]["version"])
        self.assertEqual(service.get_critical_guard_scope_template(scope="A", sheet_type="设备安全")["items"], update["items"])

    def test_guard_template_requires_original_scope_sheet_and_response(self):
        app = FastAPI()
        @app.put("/api/critical-guard/scope-template")
        async def save(body: CriticalGuardScopeTemplateRequest):
            raise AssertionError("preparation cannot write")
        agent = PortalAgent(self.assistant, PortalAPICatalog(app), self.files)
        body = {"scope": "A", "sheet_type": "设备安全"}
        template = {**body, "revision": 0, "items": [{"key": "a.1", "content": "检查", "category": "设备"}]}
        for changed, queries, message in (({"scope": "B"}, {"template": template}, "无权"), ({}, {}, "先读取"),
            ({"response_id": "missing-response"}, {"template": template}, "先读取原重保任务")):
            with self.subTest(changed=changed), self.assertRaisesRegex(AssistantError, message):
                agent.prepare(ACTOR, {"operations": [{"api_id": "PUT /api/critical-guard/scope-template", "body": {**body, **changed}}]}, self.operation_id, [], queries=queries)

    def test_generated_plan_and_form_ids_with_phone_like_digits_round_trip(self):
        from types import SimpleNamespace
        from unittest.mock import patch
        app = FastAPI()
        @app.put("/api/critical-guard/scope-template")
        async def save(body: CriticalGuardScopeTemplateRequest):
            raise AssertionError("review cannot write")
        agent = PortalAgent(self.assistant, PortalAPICatalog(app), self.files)
        body = {"scope": "A", "sheet_type": "设备安全"}
        identifier = "a289617ab13940249922e985f84575f8"
        original = [{"key": identifier, "content": "设备检查", "category": "设备"}]
        with patch("lan_bitable_template_portal.lighthouse_agent.uuid.uuid4", return_value=SimpleNamespace(hex=identifier)):
            plan = agent.prepare(ACTOR, {"operations": [{"api_id": "PUT /api/critical-guard/scope-template", "body": body}]}, self.operation_id, [],
                queries={"template": {**body, "revision": 0, "items": original}})
        public = agent.public_plan(plan)
        self.assertEqual(public["id"], identifier)
        field = public["fields"][0]
        self.assertEqual(field["value"][0]["$query"]["ref"], "query_form_" + identifier)
        self.assertEqual(field["value"][0]["key"], identifier)
        amended = agent.amend(ACTOR, plan["id"], {"version": 1, "values": {field["name"]: field["value"]}})
        self.assertEqual(amended["status"], "awaiting_confirmation")
        self.assertEqual(agent.get_plan(ACTOR, plan["id"])["operations"][0]["body"]["items"], original)

    def test_calendar_inputs_reject_invalid_dates_without_business_write(self):
        fields = [{"name": key, "path": key, "section": "body", "type": kind, "required": True} for key, kind in (("date", "date"), ("time", "time"), ("month", "month"))]
        plan = self.agent.prepare(ACTOR, {"operations": [{"api_id": "POST /api/step-two"}], "fields": fields}, self.operation_id, [])
        with self.assertRaises(AssistantError):
            self.agent.amend(ACTOR, plan["id"], {"version": plan["version"], "values": {"date": "2026-02-30", "time": "10:00", "month": "2026-10"}})
        self.assertEqual(self.sequential_writes, [])
        amended = self.agent.amend(ACTOR, plan["id"], {"version": plan["version"], "values": {"date": "2026-10-01", "time": "10:00", "month": "2026-10"}})
        self.assertEqual(amended["status"], "awaiting_confirmation")

    async def test_nested_required_fields_and_timestamp_calendar_fill_native_body(self):
        class NestedFields(BaseModel):
            actual: str
        class NestedRequest(BaseModel):
            fields: NestedFields
        app, writes = FastAPI(), []
        @app.post("/api/nested-form")
        async def nested(body: NestedRequest):
            writes.append(body.model_dump())
            return {"ok": True}
        agent = PortalAgent(self.assistant, PortalAPICatalog(app), self.files)
        plan = agent.prepare(ACTOR, {"operations": [{"api_id": "POST /api/nested-form"}]}, self.operation_id, [])
        field = plan["fields"][0]
        self.assertEqual((field["section"], field["path"]), ("body", "fields.actual"))
        plan = agent.amend(ACTOR, plan["id"], {"version": plan["version"], "values": {field["name"]: "2026-10-01T10:30:00"}})
        await agent._execute(ACTOR, agent.get_plan(ACTOR, plan["id"]), self.request)
        self.assertEqual(writes, [{"fields": {"actual": "2026-10-01T10:30:00"}}])
        timestamp = self.agent.prepare(ACTOR, {"operations": [{"api_id": "POST /api/step-two"}], "fields": [
            {"name": "actual", "path": "actual", "label": "实际完成时间", "type": "datetime-local", "value_format": "timestamp_ms", "required": True}
        ]}, self.operation_id, [])
        result = self.agent.amend(ACTOR, timestamp["id"], {"version": timestamp["version"], "values": {"actual": "2026-10-01T10:30:00"}})
        self.assertEqual(self.agent.get_plan(ACTOR, result["id"])["operations"][0]["body"]["actual"], 1790821800000)

    async def test_repair_candidates_reject_other_scope_but_allow_shared_blank_scope(self):
        app, calls = FastAPI(), []
        rows = [{"record_id": "rec-b", "scope": "B", "name": "B楼设备"}]
        @app.get("/api/repair-management/cmdb-candidates")
        async def devices(scope: str, q: str = "", limit: int = 80):
            calls.append(scope)
            return {"ok": True, "data": {"records": copy.deepcopy(rows)}}
        @app.post("/api/repair-management/followups")
        async def followup(body: RepairFollowupRecordRequest):
            raise AssertionError("candidate search must never write")
        agent = PortalAgent(self.assistant, PortalAPICatalog(app), self.files)
        plan = agent.prepare(ACTOR, {"operations": [{"api_id": "POST /api/repair-management/followups", "body": {
            "scope": "A", "summary_record_id": "rec-a", "fields": {}
        }}]}, self.operation_id, [])
        field = next(f for f in plan["fields"] if f["path"] == "cmdb_record_ids")
        with self.assertRaises(AssistantError) as error:
            await agent.field_options(ACTOR, plan["id"], field["name"], self.request)
        self.assertEqual(error.exception.status, 403)
        self.assertEqual(agent.get_plan(ACTOR, plan["id"])["version"], 1)
        rows[:] = [{"record_id": "rec-shared", "scope": "", "name": "共用设备"}]
        refreshed = await agent.field_options(ACTOR, plan["id"], field["name"], self.request)
        raw = agent.get_plan(ACTOR, refreshed["id"])
        self.assertEqual(next(f for f in raw["fields"] if f["path"] == "cmdb_record_ids")["options"][0]["value"], "rec-shared")
        forbidden = Request({**self.request.scope, "query_string": b"scope=B"})
        with self.assertRaises(AssistantError):
            await agent.field_options(ACTOR, plan["id"], field["name"], forbidden)
        self.assertEqual(calls, ["A", "A"])

    def test_structured_inputs_use_native_schema_and_preserve_unedited_values(self):
        class Row(BaseModel):
            content: str
            enabled: bool
            actual: str
            weight: int = 3
        class Rows(BaseModel):
            rows: list[Row]
        app = FastAPI()
        @app.post("/api/structured-form")
        async def submit(body: Rows):
            raise AssertionError("form review must never write")
        agent = PortalAgent(self.assistant, PortalAPICatalog(app), self.files)
        original = {"content": "原步骤", "enabled": False, "actual": "2026-10-01T10:30:00", "weight": 3}
        plan = agent.prepare(ACTOR, {"operations": [{"api_id": "POST /api/structured-form", "body": {"rows": [original]}}], "fields": [
            {"name": "rows", "path": "rows", "label": "操作列表", "type": "array", "required": True, "item": {"type": "text"}}
        ]}, self.operation_id, [])
        field = plan["fields"][0]
        self.assertEqual(field["item"]["type"], "object")
        self.assertEqual({f["path"] for f in field["item"]["children"]}, {"content", "enabled", "actual", "weight"})
        self.assertEqual(_result_refs(field["value"], [], queries=plan["_queries"]), [original])
        public = agent.public_plan(plan)
        self.assertEqual(public["fields"][0]["item"], field["item"])
        with self.assertRaises(AssistantError):
            agent.amend(ACTOR, plan["id"], {"version": 1, "values": {"rows": "not-a-list"}})
        amended = agent.amend(ACTOR, plan["id"], {"version": 1, "values": {"rows": [{**original, "content": "已更正"}]}})
        self.assertEqual(agent.get_plan(ACTOR, amended["id"])["operations"][0]["body"]["rows"], [{**original, "content": "已更正"}])
        clear = agent.prepare(ACTOR, {"operations": [{"api_id": "POST /api/structured-form", "body": {"rows": [original]}}], "fields": [
            {"name": "rows", "path": "rows", "label": "操作列表", "type": "array", "required": True}
        ]}, self.operation_id, [])
        clear = agent.amend(ACTOR, clear["id"], {"version": 1, "values": {"rows": []}})
        self.assertEqual(agent.get_plan(ACTOR, clear["id"])["operations"][0]["body"]["rows"], [])
        with self.assertRaisesRegex(AssistantError, "实际字段"):
            agent.prepare(ACTOR, {"operations": [{"api_id": "POST /api/structured-form", "body": {"rows": [original]}}], "fields": [
                {"name": "forged", "path": "forged", "type": "object", "children": [{"path": "secret"}]}
            ]}, self.operation_id, [])

    async def test_unedited_form_projection_never_overwrites_original_text_or_private_content(self):
        class Row(BaseModel):
            content: str
            note: str
        class Body(BaseModel):
            rows: list[Row]
        app, writes = FastAPI(), []
        @app.post("/api/raw-form")
        async def save(body: Body):
            writes.append(body.model_dump())
            return {"ok": True}
        agent = PortalAgent(self.assistant, PortalAPICatalog(app), self.files)
        original = {"content": "Ａ侧设备（待检查）", "note": "原备注\n身份证：11010519491231002X"}
        plan = agent.prepare(ACTOR, {"operations": [{"api_id": "POST /api/raw-form", "body": {"rows": [original]}}],
            "fields": [{"path": "rows", "type": "array"}]}, self.operation_id, [])
        field = plan["fields"][0]
        value = agent.public_plan(plan)["fields"][0]["value"]
        self.assertNotIn("11010519491231002X", json.dumps(value))
        self.assertNotEqual(value[0]["content"], original["content"])
        self.assertEqual(_result_refs(value, [], queries=plan["_queries"]), [original])
        amended = agent.amend(ACTOR, plan["id"], {"version": 1, "values": {field["name"]: value}})
        await agent._execute(ACTOR, agent.get_plan(ACTOR, amended["id"]), self.request)
        self.assertEqual(writes, [{"rows": [original]}])
        plan = agent.prepare(ACTOR, {"operations": [{"api_id": "POST /api/raw-form", "body": {"rows": [original]}}],
            "fields": [{"path": "rows", "type": "array"}]}, self.operation_id, [])
        field = plan["fields"][0]
        value = agent.public_plan(plan)["fields"][0]["value"]
        value[0]["content"] = "明确更正内容"
        amended = agent.amend(ACTOR, plan["id"], {"version": 1, "values": {field["name"]: value}})
        await agent._execute(ACTOR, agent.get_plan(ACTOR, amended["id"]), self.request)
        self.assertEqual(writes[-1], {"rows": [{**original, "content": "明确更正内容"}]})

    async def test_long_editable_rows_preserve_hidden_proof_when_reordered_or_deleted(self):
        class ProofRow(BaseModel):
            content: str
            file_token: str
        class ProofRows(BaseModel):
            rows: list[ProofRow]
        app, writes = FastAPI(), []
        @app.post("/api/proof-rows")
        async def submit(body: ProofRows):
            writes.append(body.model_dump())
            return {"ok": True}
        agent = PortalAgent(self.assistant, PortalAPICatalog(app), self.files)
        original = [{"content": f"记录{i}", "file_token": f"private-proof-{i}"} for i in range(61)]
        plan = agent.prepare(ACTOR, {"operations": [{"api_id": "POST /api/proof-rows", "body": {"rows": original}}], "fields": [
            {"name": "rows", "path": "rows", "label": "明细", "type": "array", "required": True}
        ]}, self.operation_id, [])
        values = agent.public_plan(plan)["fields"][0]["value"]
        self.assertEqual(len(values), 61)
        self.assertNotIn("private-proof", json.dumps(values))
        self.assertNotIn("file_token", {child["path"] for child in plan["fields"][0]["item"]["children"]})
        forged = copy.deepcopy(values)
        forged[0]["$query"]["ref"] = "query_" + "f" * 32
        with self.assertRaises(AssistantError):
            agent.amend(ACTOR, plan["id"], {"version": 1, "values": {"rows": forged}})
        self.assertEqual(writes, [])
        values = [{**values[-1], "content": "已更正最后一条"}, *values[:-2]]
        amended = agent.amend(ACTOR, plan["id"], {"version": 1, "values": {"rows": values}})
        await agent._execute(ACTOR, agent.get_plan(ACTOR, amended["id"]), self.request)
        self.assertEqual(writes, [{"rows": [{**original[-1], "content": "已更正最后一条"}, *original[:-2]]}])
        self.assertEqual(len(writes[0]["rows"]), 60)

    async def test_sop_editor_uses_original_steps_version_and_native_loop_expansion(self):
        from lan_bitable_template_portal.polling_work_orders import PollingWorkOrderService
        app, writes = FastAPI(), []
        @app.post("/api/polling-sops")
        async def create(body: PollingSopRequest):
            writes.append(body.model_dump())
            return {"ok": True}
        @app.put("/api/polling-sops/{sop_id}")
        async def update(sop_id: str, body: PollingSopRequest):
            writes.append(body.model_dump())
            return {"ok": True}
        agent = PortalAgent(self.assistant, PortalAPICatalog(app), self.files)
        draft = agent.prepare(ACTOR, {"operations": [{"api_id": "POST /api/polling-sops", "body": {"name": "水质循环", "scope": "A", "work_type": "maintenance"}}]}, self.operation_id, [])
        form = next(field for field in draft["fields"] if field["path"] == "steps")
        self.assertEqual((form["type"], form["maxItems"], form["context_source"]), ("array", 30, "sop_steps"))
        children = {child["path"]: child for child in form["item"]["children"]}
        self.assertTrue(children["step_id"]["hidden"])
        self.assertEqual(children["step_id"]["generate"], "uuid")
        self.assertTrue(children["operator_required"]["initial"])
        self.assertTrue(children["delay_reminder_minutes"]["toggle_zero"])
        steps = [{"step_id": f"s{i}", "content": f"步骤{i}", "operator_required": True, "reviewer_required": True, "photo_required": True} for i in range(1, 15)]
        steps[5]["repeat_rules"] = [{"from_step_id": "s1", "to_step_id": "s6", "count": 4}]
        steps[13]["repeat_rules"] = [{"from_step_id": "s11", "to_step_id": "s14", "count": 4}]
        draft = agent.amend(ACTOR, draft["id"], {"version": 1, "values": {form["name"]: steps}})
        await agent._execute(ACTOR, agent.get_plan(ACTOR, draft["id"]), self.request)
        expanded = PollingWorkOrderService._expanded_steps(writes[0]["steps"])
        self.assertEqual(len(expanded), 54)
        self.assertEqual([step["step_id"] for step in expanded[38:]], [f"s{i}" for i in range(11, 15)] * 4)
        queries = {"query_" + "a" * 32: {"items": [{"sop_id": "sop-original", "version": 7, "scope": "A", "work_type": "maintenance", "name": "原名称", "steps": steps}]}}
        edited = agent.prepare(ACTOR, {"operations": [{"api_id": "PUT /api/polling-sops/{sop_id}", "path_params": {"sop_id": "sop-original"}, "body": {"name": "仅更名"}}]}, self.operation_id, [], queries=queries)
        await agent._execute(ACTOR, edited, self.request)
        self.assertEqual(writes[-1]["expected_version"], 7)
        self.assertEqual([step["step_id"] for step in writes[-1]["steps"]], [step["step_id"] for step in steps])
        self.assertEqual(writes[-1]["work_type"], "maintenance")
        with self.assertRaisesRegex(AssistantError, "读取原SOP"):
            agent.prepare(ACTOR, {"operations": [{"api_id": "PUT /api/polling-sops/{sop_id}", "path_params": {"sop_id": "unknown"}, "body": {"name": "仅更名"}}]}, self.operation_id, [])
        invalid = copy.deepcopy(steps)
        invalid[5]["repeat_rules"][0]["to_step_id"] = "s14"
        with self.assertRaisesRegex(AssistantError, "连续步骤区间"):
            agent.prepare(ACTOR, {"operations": [{"api_id": "POST /api/polling-sops", "body": {"name": "无效循环", "scope": "A", "steps": invalid}}]}, self.operation_id, [])
        missing_scope = agent.prepare(ACTOR, {"operations": [{"api_id": "POST /api/polling-sops", "body": {"name": "仅A楼", "steps": steps}}]}, self.operation_id, [])
        choices = next(field for field in missing_scope["fields"] if field["path"] == "scope")["options"]
        self.assertEqual([option["value"] for option in choices], ["A"])

    async def test_async_job_resume_continues_without_resubmitting(self):
        decision = {
            "operations": [
                {"api_id": "POST /api/batch/submit"},
                {"api_id": "POST /api/batch/finalize"},
            ],
        }
        plan = self.agent.prepare(ACTOR, decision, self.operation_id, [])
        # Simulate: first write already ran and returned an async job that was
        # still processing when the agent task exited.
        plan["status"] = "submitted"
        plan["error"] = "后台任务仍在处理，可查询原任务结果；不会重复提交。"
        plan["results"] = [{
            "ok": True,
            "status": 200,
            "data": {"job_id": "job-1"},
            "_raw": {"job_id": "job-1"},
            "api_id": "POST /api/batch/submit",
            "truncated": False,
        }]
        self.store.put_document(PLAN_NAMESPACE, plan["id"], plan)

        # The real *other* process now sees the job succeeded.
        self.job_phase["job-1"] = "success"

        refreshed = await self.agent.refresh(ACTOR, plan, self.request)
        self.assertEqual(refreshed["status"], "running")
        await gather_tasks(self.agent)

        # Submitted write must not be repeated; only the remaining step runs.
        self.assertEqual(self.batch_writes, ["finalize"])
        finished = self.agent.get_plan(ACTOR, plan["id"])
        self.assertEqual(finished["status"], "completed")
        self.assertEqual(len(finished["results"]), 2)

    async def _prepare_notice_plan(self):
        qref = "query_" + "a" * 32
        draft = {
            "title": "A楼设备调整",
            "progress": "已完成60%",
            "start_time": "2026-09-30 09:00",
            "end_time": "2026-09-30 11:00",
            "location": "A楼机房",
            "content": "调整机组参数",
            "reason": "设备更新",
            "impact": "短时停机",
            "specialty": "电气",
            "maintenance_cycle": "每月",
            "execution_party": "厂维",
            "building_codes": ["A"],
        }
        decision = {
            "operations": [{
                "api_id": "POST /api/workbench-actions",
                "body": {
                    "command_format": "notice_command",
                    "scope": "A",
                    "work_type": "maintenance",
                    "action": "start",
                    "manual": True,
                    "polling_work_order_exempt": True,
                    "patch": {"$query": {"ref": qref, "path": "draft"}},
                },
            }],
        }
        queries = {qref: {"draft": draft}}
        return self.agent.prepare(ACTOR, decision, self.operation_id, [], queries=queries)

    async def test_notice_command_start_requires_bind_unbound_and_unbound_needs_no_source(self):
        plan = await self._prepare_notice_plan()
        self.assertEqual(plan["status"], "needs_input")
        names = {f["name"] for f in plan["fields"]}
        self.assertIn("step0.manual_binding_choice", names)
        self.assertIn("step0.source_record_id", names)

        amended = self.agent.amend(ACTOR, plan["id"], {
            "version": plan["version"],
            "values": {"step0.manual_binding_choice": "unbound"},
        })
        self.assertEqual(amended["status"], "awaiting_confirmation")
        stored = self.agent.get_plan(ACTOR, plan["id"])
        body = stored["operations"][0]["body"]
        self.assertEqual(body["manual_binding_choice"], "unbound")
        self.assertNotIn("source_record_id", body)
        # $query overlay must remain intact so original draft survives.
        self.assertIn("$query", json.dumps(body["patch"], ensure_ascii=False))

    async def test_notice_command_start_bind_uses_loaded_options_and_preserves_draft(self):
        plan = await self._prepare_notice_plan()
        options_plan = await self.agent.field_options(ACTOR, plan["id"], "step0.source_record_id", self.request)
        field = next(f for f in options_plan["fields"] if f["name"] == "step0.source_record_id")
        self.assertTrue(field["options"])
        self.assertEqual(field["options"][0]["value"], "plan-1")

        amended = self.agent.amend(ACTOR, plan["id"], {
            "version": options_plan["version"],
            "values": {
                "step0.manual_binding_choice": "bind",
                "step0.source_record_id": "plan-1",
            },
        })
        self.assertEqual(amended["status"], "awaiting_confirmation")
        stored = self.agent.get_plan(ACTOR, plan["id"])
        self.assertEqual(stored["operations"][0]["body"]["source_record_id"], "plan-1")
        self.assertEqual(stored["operations"][0]["body"]["manual_binding_choice"], "bind")

        await self.agent._execute(ACTOR, stored, self.request)
        self.assertEqual(len(self.notice_writes), 1)
        body = self.notice_writes[0]
        self.assertEqual(body["source_record_id"], "plan-1")
        patch = body["patch"]
        self.assertEqual(patch["progress"], "已完成60%")
        self.assertEqual(patch["start_time"], "2026-09-30 09:00")
        self.assertEqual(patch["end_time"], "2026-09-30 11:00")

    async def test_notice_binding_search_preserves_selected_record_until_cleared(self):
        from urllib.parse import urlencode
        plan = await self._prepare_notice_plan()
        await self.agent.field_options(ACTOR, plan["id"], "step0.source_record_id", self.request)
        self.records = [{"record_id": "plan-new", "title": "新计划", "status": "未开始"}]
        request = Request({**self.request.scope, "query_string": urlencode({"q": "新计划", "selected": json.dumps(["plan-1"])}).encode()})
        loaded = await self.agent.field_options(ACTOR, plan["id"], "step0.source_record_id", request)
        field = next(f for f in loaded["fields"] if f["path"] == "source_record_id")
        self.assertEqual([option["value"] for option in field["options"]], ["plan-1", "plan-new"])
        loaded = await self.agent.field_options(ACTOR, plan["id"], "step0.source_record_id", self.request)
        field = next(f for f in loaded["fields"] if f["path"] == "source_record_id")
        self.assertEqual([option["value"] for option in field["options"]], ["plan-new"])

    async def test_notice_source_options_keep_requested_month_and_report_limit(self):
        plan = await self._prepare_notice_plan()
        plan["operations"][0]["body"]["source_month"] = "9月"
        self.agent._save_plan(ACTOR, plan)
        rows = [{"source_record_id": f"plan-{index}", "title": f"九月计划{index}", "progress": "延期未开始"} for index in range(200)]
        with patch.object(self.agent, "_invoke", new=AsyncMock(return_value={"ok": True, "data": {"items": rows}})) as invoke:
            loaded = await self.agent.field_options(ACTOR, plan["id"], "step0.source_record_id", self.request)
        self.assertEqual(invoke.call_args.args[1]["params"]["month"], "9月")
        field = next(item for item in loaded["fields"] if item["path"] == "source_record_id")
        self.assertTrue(field["options_has_more"])
        self.assertIn("延期未开始", field["options"][0]["label"])
        with patch.object(self.agent, "_invoke", new=AsyncMock(return_value={"ok": True, "data": {"items": None}})), self.assertRaises(AssistantError):
            await self.agent.field_options(ACTOR, plan["id"], "step0.source_record_id", self.request)
        self.assertEqual(self.notice_writes, [])

    async def test_concurrent_refresh_resumes_remaining_write_exactly_once(self):
        # Two concurrent refresh() calls race on the same submitted, 2-step plan.
        # A barrier synchronises both calls at _task_result so both reach the
        # atomic post-job check before either persists the resumed state. Only
        # one refresh may resume execution; the remaining finalize runs once.
        decision = {
            "operations": [
                {"api_id": "POST /api/batch/submit"},
                {"api_id": "POST /api/batch/finalize"},
            ],
        }
        plan = self.agent.prepare(ACTOR, decision, self.operation_id, [])
        # First write already ran and returned an async job that was still
        # processing when the agent task exited.
        plan["status"] = "submitted"
        plan["error"] = "后台任务仍在处理，可查询原任务结果；不会重复提交。"
        plan["results"] = [{
            "ok": True,
            "status": 200,
            "data": {"job_id": "job-1"},
            "_raw": {"job_id": "job-1"},
            "api_id": "POST /api/batch/submit",
            "truncated": False,
        }]
        self.store.put_document(PLAN_NAMESPACE, plan["id"], plan)
        self.job_phase["job-1"] = "success"

        barrier = asyncio.Barrier(2)
        original_task_result = self.agent._task_result

        async def synchronized(actor, spec, request):
            result, done = await original_task_result(actor, spec, request)
            await barrier.wait()
            return result, done

        self.agent._task_result = synchronized
        try:
            await asyncio.gather(
                self.agent.refresh(ACTOR, self.agent.get_plan(ACTOR, plan["id"]), self.request),
                self.agent.refresh(ACTOR, self.agent.get_plan(ACTOR, plan["id"]), self.request),
            )
        finally:
            self.agent._task_result = original_task_result

        await gather_tasks(self.agent)
        # The remaining write executes exactly once, not twice.
        self.assertEqual(self.batch_writes, ["finalize"])
        finished = self.agent.get_plan(ACTOR, plan["id"])
        self.assertEqual(finished["status"], "completed")
        self.assertEqual(len(finished["results"]), 2)

    async def test_late_refresh_error_cannot_overwrite_completed_resume(self):
        decision = {"operations": [{"api_id": "POST /api/batch/submit"}, {"api_id": "POST /api/batch/finalize"}]}
        plan = self.agent.prepare(ACTOR, decision, self.operation_id, [])
        plan.update(status="submitted", results=[{"ok": True, "api_id": "POST /api/batch/submit", "data": {"job_id": "job-1"}, "_raw": {"job_id": "job-1"}}])
        self.store.put_document(PLAN_NAMESPACE, plan["id"], plan)
        barrier = asyncio.Barrier(2)
        calls = 0

        async def raced_status(actor, spec, request):
            nonlocal calls
            calls += 1
            current = calls
            await barrier.wait()
            if current == 1:
                await asyncio.sleep(.05)
                raise AssistantError("暂时超时", 503)
            return {"ok": True, "data": {"phase": "success"}}, True

        original = self.agent._task_result
        self.agent._task_result = raced_status
        try:
            await asyncio.gather(*(self.agent.refresh(ACTOR, self.agent.get_plan(ACTOR, plan["id"]), self.request) for _ in range(2)))
            await gather_tasks(self.agent)
        finally:
            self.agent._task_result = original
        self.assertEqual(self.batch_writes, ["finalize"])
        self.assertEqual(self.agent.get_plan(ACTOR, plan["id"])["status"], "completed")

    async def test_interrupted_second_write_marks_failed_not_submitted_no_replay(self):
        # Step 1 is an async job that completes; step 2 is a synchronous write
        # that is interrupted (InterruptedError, the synchronous counterpart of
        # task cancellation). The plan must be left failed, never submitted, and
        # a later refresh must not replay/restart the interrupted write.
        decision = {
            "operations": [
                {"api_id": "POST /api/batch/submit"},
                {"api_id": "POST /api/step-interrupt"},
            ],
        }
        plan = self.agent.prepare(ACTOR, decision, self.operation_id, [])
        self.job_phase["job-1"] = "success"

        original_invoke = self.agent._invoke

        async def interrupting_invoke(actor, op, request, *, uploads=False):
            if op.get("api_id") == "POST /api/step-interrupt":
                raise InterruptedError("模拟第二部写入中断")
            return await original_invoke(actor, op, request, uploads=uploads)

        self.agent._invoke = interrupting_invoke
        try:
            await self.agent._execute(ACTOR, plan, self.request)
        finally:
            self.agent._invoke = original_invoke

        # First async job completed; the interrupted synchronous write failed.
        self.assertEqual(self.batch_writes, ["submit"])
        self.assertEqual(plan["status"], "failed")
        self.assertEqual(len(plan["results"]), 1)
        self.assertIn("未确认", plan["error"])

        # Refresh must not interpret this as a submitted plan or restart it.
        refreshed = await self.agent.refresh(ACTOR, self.agent.get_plan(ACTOR, plan["id"]), self.request)
        self.assertEqual(refreshed["status"], "failed")
        self.assertEqual(self.batch_writes, ["submit"])
        self.assertEqual(len(self.agent.get_plan(ACTOR, plan["id"])["results"]), 1)

    async def test_person_ref_select_resolves_private_id_without_leak_keeps_attachments(self):
        # A select field whose option value is an opaque person_ref is stored as
        # a reference, resolved to the real private ID only at execution, never
        # leaked to the public plan, and uploaded attachments stay attached.
        file_a = self.files.upload(ACTOR, "person-ref.txt", ("人员附件\r\n" * 10).encode())
        decision = {
            "operations": [{
                "api_id": "POST /api/cabinet-power/batches/{batch_id}/images",
                "path_params": {"batch_id": "batch-attach-1"},
                "body": {"file_token": "ignored-token"},
                "files": {"files": [file_a["id"]]},
            }],
            "fields": [{
                "operation_index": 0,
                "section": "body",
                "path": "person",
                "type": "select",
                "label": "责任人员",
                "required": True,
                "options": [{"value": "person_zzz", "label": "王工"}],
            }],
        }
        plan = self.agent.prepare(
            ACTOR, decision, self.operation_id, [file_a["id"]],
            references={"person_zzz": "ou_real_private_id"},
        )
        self.assertEqual(plan["status"], "needs_input")
        self.assertEqual(plan["_references"]["person_zzz"], "ou_real_private_id")

        amended = self.agent.amend(ACTOR, plan["id"], {
            "version": plan["version"],
            "values": {"step0.person": "person_zzz"},
        })
        self.assertEqual(amended["status"], "awaiting_confirmation")
        stored = self.agent.get_plan(ACTOR, plan["id"])
        # The opaque person_ref is kept as a reference marker after selection.
        self.assertEqual(stored["operations"][0]["body"]["person"], {"$reference": "person_zzz"})
        self.assertIn(file_a["id"], stored["file_ids"])

        public = self.agent.public_plan(stored)
        public_json = json.dumps(public, ensure_ascii=False)
        self.assertNotIn("ou_real_private_id", public_json)

        await self.agent._execute(ACTOR, stored, self.request)
        self.assertEqual(len(self.attach_writes), 1)
        write = self.attach_writes[0]
        self.assertEqual(write["person"], "ou_real_private_id")
        self.assertEqual(write["attachment_content"], ("人员附件\r\n" * 10).encode())
        # No credential leakage even after execution.
        self.assertNotIn(
            "ou_real_private_id",
            json.dumps(self.agent.public_plan(self.agent.get_plan(ACTOR, plan["id"])), ensure_ascii=False),
        )
    async def test_clear_rejects_busy_chat_with_running_plan_and_preserves_conversation(self):
        # A chat turn already contains a prepared plan that has been marked
        # running. LighthouseAssistant.clear must reject under its own lock even
        # if an outer HTTP route precheck has not run, and it must never reset
        # the still-referenced conversation.
        decision = {"operations": [{"api_id": "DELETE /api/drills/{drill_id}", "path_params": {"drill_id": "d-00"}}]}
        turn_id = "workflow_busy_turn_0000000000001"
        plan = self.agent.prepare(ACTOR, decision, turn_id, [])
        state = self.assistant._state(ACTOR)
        state["turns"] = [{
            "operation_id": turn_id,
            "question": "删除设备",
            "answer": "请核对操作清单，确认后执行。",
            "status": "completed",
            "scopes": ["A"],
            "plan": self.agent.public_plan(plan),
        }]
        self.store.put_document("lighthouse_ai", self.assistant._key(ACTOR), state)
        plan["status"] = "running"
        self.store.put_document(PLAN_NAMESPACE, plan["id"], plan)
        state["turns"][0]["plan"] = self.agent.public_plan(plan)
        self.store.put_document("lighthouse_ai", self.assistant._key(ACTOR), state)

        # Actor is intentionally NOT in _active: clear must still reject on its
        # own, because the conversation's turn references a running plan.
        with self.assertRaises(AssistantError) as ctx:
            self.assistant.clear(ACTOR)
        self.assertEqual(ctx.exception.status, 409)

        self.assertEqual(self.delete_writes, [])
        stored = self.assistant._state(ACTOR)
        self.assertEqual(stored["id"], state["id"])
        self.assertEqual(len(stored["turns"]), 1)
        self.assertEqual(stored["turns"][0]["operation_id"], turn_id)
        self.assertEqual(stored["turns"][0]["plan"]["id"], plan["id"])
        self.assertEqual(stored["turns"][0]["plan"]["status"], "running")

    async def test_stale_tab_confirm_after_clear_raises_409_no_writes(self):
        # A prepared plan from a conversation that has since been cleared must
        # not be confirmable from another stale tab: confirm must raise 409 and
        # must not perform any business write.
        decision = {"operations": [{"api_id": "DELETE /api/drills/{drill_id}", "path_params": {"drill_id": "d-02"}}]}
        plan = self.agent.prepare(ACTOR, decision, self.operation_id, [])
        self.assertEqual(plan["status"], "awaiting_confirmation")
        self.assertEqual(plan["risk"], "high")

        self.assistant.clear(ACTOR)

        with self.assertRaises(AssistantError) as ctx:
            await self.agent.confirm(ACTOR, plan["id"], {"version": plan["version"], "stage": "review"}, self.request)
        self.assertEqual(ctx.exception.status, 409)
        self.assertEqual(self.delete_writes, [])
        stored = self.store.get_document(PLAN_NAMESPACE, plan["id"])
        self.assertEqual(stored["status"], "awaiting_confirmation")

    async def test_confirm_save_failure_leaves_executing_clear_and_retry_once(self):
        # A one-shot failure persisting the running plan inside confirm must not
        # leave the actor stuck in agent.executing; retrying with the original
        # version must then complete exactly once. The underlying DB is never
        # broken: only the plan-save call is wrapped for one failure. Both the
        # failure on the plan put (plan never persisted running) and the failure
        # on the follow-up conversation put (plan already persisted running) must
        # roll back to the originally confirmed version with no business write.
        for failure_mode in ("plan_save", "conversation_put"):
            with self.subTest(failure_mode=failure_mode):
                self.delete_writes = []
                decision = {"operations": [{"api_id": "DELETE /api/drills/{drill_id}", "path_params": {"drill_id": "d-03"}}]}
                plan = self.agent.prepare(ACTOR, decision, self.operation_id, [])
                # Give the conversation a turn for this plan so _save_plan also
                # reaches the follow-up conversation write (needed by the
                # conversation_put failure mode).
                state = self.assistant._state(ACTOR)
                state["turns"] = [{
                    "operation_id": plan["turn_id"],
                    "question": "删除设备",
                    "answer": "请核对操作清单，确认后执行。",
                    "status": "completed",
                    "scopes": ["A"],
                    "plan": self.agent.public_plan(plan),
                }]
                self.store.put_document("lighthouse_ai", self.assistant._key(ACTOR), state)
                plan = await self.agent.confirm(ACTOR, plan["id"], {"version": plan["version"], "stage": "review"}, self.request)
                self.assertEqual(plan["status"], "awaiting_second_confirmation")
                review_version = plan["version"]
                self.assertEqual(self.delete_writes, [])

                original_put = self.store.put_document
                failed = {"once": False}

                def wrapper(namespace, key, value):
                    if not failed["once"]:
                        if failure_mode == "plan_save":
                            match = namespace == PLAN_NAMESPACE and key == plan["id"] and value.get("status") == "running"
                        else:
                            match = namespace == "lighthouse_ai"
                        if match:
                            failed["once"] = True
                            raise RuntimeError("injected plan save failure")
                    return original_put(namespace, key, value)

                self.store.put_document = wrapper
                try:
                    with self.assertRaises(AssistantError) as ctx:
                        await self.agent.confirm(ACTOR, plan["id"], {"version": review_version, "stage": "execute"}, self.request)
                    self.assertEqual(ctx.exception.status, 503)
                    self.assertIn("尚未启动业务写入", str(ctx.exception))
                finally:
                    self.store.put_document = original_put

                # Actor must never enter (or stay stuck in) the executing set.
                self.assertFalse(any(owner == ACTOR["id"] for owner, _plan_id in self.agent.executing))
                self.assertEqual(self.delete_writes, [])

                # The rollback must restore the original version/status in the
                # audited store, even when the running plan had already persisted.
                persisted = self.store.get_document(PLAN_NAMESPACE, plan["id"])
                self.assertEqual(persisted["status"], "awaiting_second_confirmation")
                self.assertEqual(persisted["version"], review_version)

                # Retry with the original version executes exactly once.
                running = await self.agent.confirm(ACTOR, plan["id"], {"version": review_version, "stage": "execute"}, self.request)
                self.assertEqual(running["status"], "running")
                await gather_tasks(self.agent)
                self.assertEqual(self.delete_writes, [{"drill_id": "d-03"}])
                finished = self.agent.get_plan(ACTOR, plan["id"])
                self.assertEqual(finished["status"], "completed")

    async def test_chat_compress_must_not_revert_completed_business_plan(self):
        # A chat that is parked inside _compress with a stale in-memory
        # conversation must not persist that stale copy after an earlier
        # business plan has been completed via _save_plan. The completed plan's
        # answer/status in the conversation must never be reverted.
        business_op = "workflow_business_0000000000001"
        new_op = "workflow_newchat_00000000000001"

        decision = {"operations": [{"api_id": "DELETE /api/drills/{drill_id}", "path_params": {"drill_id": "d-04"}}]}
        business_plan = self.agent.prepare(ACTOR, decision, business_op, [])
        self.assertEqual(business_plan["status"], "awaiting_confirmation")

        state = self.assistant._state(ACTOR)
        turns = []
        for i in range(10):
            turns.append({
                "operation_id": f"workflow_prev_{i:08d}",
                "question": f"历史问题{i}",
                "answer": "历史回答" * 60,
                "status": "completed",
                "scopes": ["A"],
            })
        turns.append({
            "operation_id": business_op,
            "question": "删除设备",
            "answer": "请核对操作清单，确认后执行。",
            "status": "completed",
            "scopes": ["A"],
            "plan": self.agent.public_plan(business_plan),
        })
        state["turns"] = turns
        self.store.put_document("lighthouse_ai", self.assistant._key(ACTOR), state)
        conversation_id = state["id"]

        entered_compress = threading.Event()
        release_compress = threading.Event()
        complete_calls = {"count": 0}

        def fake_complete(messages, *, profile=None, max_tokens=None, structured=False):
            if complete_calls["count"] == 0:
                complete_calls["count"] += 1
                entered_compress.set()
                release_compress.wait(timeout=10)
                return "旧摘要：之前的历史内容。"
            complete_calls["count"] += 1
            return "好的，我已知晓。"

        self.assistant.model.complete.side_effect = fake_complete

        chat_result = {}

        def run_chat():
            try:
                chat_result["ok"] = self.assistant.chat(ACTOR, {
                    "question": "你好，介绍一下",
                    "operation_id": new_op,
                    "conversation_id": conversation_id,
                })
            except Exception as exc:  # pragma: no cover - surfaced below
                chat_result["error"] = exc

        thread = threading.Thread(target=run_chat)
        thread.start()
        try:
            self.assertTrue(entered_compress.wait(timeout=5))
            # Confirm the earlier business plan while the new chat is parked
            # inside _compress.
            business_plan["status"] = "completed"
            self.agent._save_plan(ACTOR, business_plan)
            completed_before = self.assistant._state(ACTOR)
            business_turn_before = next(t for t in completed_before["turns"] if t["operation_id"] == business_op)
            self.assertEqual(business_turn_before["answer"], "操作已完成。")
            self.assertEqual(business_turn_before["plan"]["status"], "completed")
        finally:
            release_compress.set()
            thread.join(timeout=10)
        self.assertFalse(thread.is_alive())
        self.assertNotIn("error", chat_result)

        stored = self.assistant._state(ACTOR)
        business_turn = next(t for t in stored["turns"] if t["operation_id"] == business_op)
        self.assertEqual(business_turn["answer"], "操作已完成。")
        self.assertEqual(business_turn["plan"]["status"], "completed")


if __name__ == "__main__":
    unittest.main()
