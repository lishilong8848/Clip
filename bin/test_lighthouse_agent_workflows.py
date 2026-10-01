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
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import JSONResponse
from clipflow_backend.api_models import RepairFollowupRecordRequest

from lan_bitable_template_portal.lighthouse_agent import (
    PLAN_NAMESPACE,
    PortalAgent,
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
        self.records = [
            {"record_id": "plan-1", "title": "A楼调整", "status": "未开始"},
            {"record_id": "plan-2", "title": "B楼调整", "status": "未开始"},
        ]

        app = FastAPI()

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

        @app.post("/api/records/attach-files")
        async def attach_files(request: Request, file_token: str = Form(""), person: str = Form(""), attachment: UploadFile = File(...)):
            self.attach_writes.append({
                "file_token": file_token,
                "person": person,
                "attachment_name": attachment.filename,
                "attachment_content": await attachment.read(),
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
                    "api_id": "POST /api/records/attach-files",
                    "body": {
                        "file_token": {"$result": {"step": 0, "path": "file_token"}},
                        "person": "ou_plain",
                    },
                    "files": {"attachment": [file_b["id"]]},
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
                "api_id": "POST /api/records/attach-files",
                "body": {"person": {"$reference": "person_zzz"}},
                "files": {"attachment": [file_a["id"]]},
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

    def test_calendar_inputs_reject_invalid_dates_without_business_write(self):
        fields = [{"name": key, "path": key, "section": "body", "type": kind, "required": True} for key, kind in (("date", "date"), ("time", "time"), ("month", "month"))]
        plan = self.agent.prepare(ACTOR, {"operations": [{"api_id": "POST /api/step-two"}], "fields": fields}, self.operation_id, [])
        with self.assertRaises(AssistantError):
            self.agent.amend(ACTOR, plan["id"], {"version": plan["version"], "values": {"date": "2026-02-30", "time": "10:00", "month": "2026-10"}})
        self.assertEqual(self.sequential_writes, [])
        amended = self.agent.amend(ACTOR, plan["id"], {"version": plan["version"], "values": {"date": "2026-10-01", "time": "10:00", "month": "2026-10"}})
        self.assertEqual(amended["status"], "awaiting_confirmation")

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
                "api_id": "POST /api/records/attach-files",
                "body": {"file_token": "ignored-token"},
                "files": {"attachment": [file_a["id"]]},
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
                self.assertNotIn(ACTOR["id"], self.agent.executing)
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
