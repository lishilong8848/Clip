"""Generated-file delivery uses native protected URLs, without cloud writes."""
import asyncio
import copy
import json
import sys
import unittest
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import test_lighthouse_agent_workflows as workflows
from fastapi import FastAPI, Request
from fastapi.responses import Response
from clipflow_backend.api_models import DrillGenerateRequest
from lan_bitable_template_portal.lighthouse_ai import AssistantError, NAMESPACE
from lan_bitable_template_portal.lighthouse_agent import PortalAgent
from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog
from lan_bitable_template_portal.lighthouse_model import LighthouseModel
from lan_bitable_template_portal.lighthouse_stream import LighthouseStream
from pydantic_ai.models.function import DeltaToolCall, FunctionModel

ACTOR = {"id": "downloads-fixture", "scopes": list("ABCDEH"), "is_admin": True}


class DownloadWorkflowTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.fixture = workflows.WorkflowTests()
        self.fixture.setUp()
        for callback, args, kwargs in self.fixture._cleanups:
            self.addCleanup(callback, *args, **kwargs)
        self.fixture._cleanups.clear()
        self.writes = []
        self.export = {"export_id": "export-E", "scope": "E", "filename": "E楼机柜统计.xlsm", "phase": "completed"}
        self.job = {"job_id": "job-export", "scope": "E", "kind": "export", "status": "succeeded", "result": copy.deepcopy(self.export)}
        self.batch = {"batch_id": "all_" + "a" * 32, "status": "succeeded", "items": {scope: {"scope": scope, "status": "succeeded",
            "result": {**self.export, "scope": scope, "export_id": "export-" + scope, "filename": scope + "楼机柜统计.xlsm"}} for scope in "ABCDE"}}
        self.execution = {"drill_id": "drill-E", "scope": "E", "status": "synced", "execution_version": 3,
            "generated_version": 3, "generation_rule_current": True, "generated": {"name": "E楼演练.xlsx", "size": 256}, "last_error": ""}
        self.guard = {"response_id": "guard-E", "scope": "E", "sheet_type": "电气检查", "check_date": "2026-10-03",
                      "has_image": True, "has_workbook": True}
        app = FastAPI()

        @app.post("/api/cabinet-power/exports")
        async def export(request: Request):
            self.writes.append("export")
            return {"ok": True, "data": {"job_id": "job-export", "scope": "E", "status": "pending"}}

        @app.get("/api/cabinet-power/jobs/{job_id}")
        async def export_job(job_id: str):
            return {"ok": True, "data": copy.deepcopy(self.job)}

        @app.post("/api/cabinet-power/export-batches")
        async def export_all(request: Request):
            self.writes.append("batch")
            self.batch["batch_id"] = (await request.json())["batch_id"]
            return {"ok": True, "data": {"batch_id": self.batch["batch_id"], "status": "running"}}

        @app.get("/api/cabinet-power/export-batches/{batch_id}")
        async def export_batch(batch_id: str):
            return {"ok": True, "data": copy.deepcopy(self.batch)}

        @app.get("/api/cabinet-power/export-history")
        async def history(scope: str):
            return {"ok": True, "data": {"items": [copy.deepcopy(self.export)], "total": 1}}

        @app.post("/api/drills/{drill_id}/generate")
        async def generate(drill_id: str, body: DrillGenerateRequest, scope: str):
            self.writes.append("drill")
            return {"ok": True, "data": {"queued": True, "execution": {"execution_version": 3}}}

        @app.get("/api/drills/{drill_id}/execution")
        async def execution(drill_id: str, scope: str):
            return {"ok": True, "data": {"execution": copy.deepcopy(self.execution)}}

        @app.get("/api/critical-guard/tasks/{task_id}")
        async def guard(task_id: str, scope: str, admin: str = ""):
            return {"ok": True, "data": {"task_id": task_id, "target_scopes": ["E"], "responses": [copy.deepcopy(self.guard)]}}

        @app.get("/api/critical-guard/tasks/{task_id}/download")
        async def guard_zip(task_id: str, sheet_type: str):
            raise AssertionError("ZIP bytes are downloaded by browser only after clicking the protected link")

        @app.post("/api/batch/finalize")
        async def finalize():
            self.writes.append("finalize")
            return {"ok": True}

        @app.get("/api/cabinet-power/exports/{export_id}/download")
        async def export_json(export_id: str, scope: str = "E"):
            return Response(json.dumps({"records": [{"record_id": export_id, "title": "E楼机柜统计", "row": 1}]}, ensure_ascii=False).encode(),
                media_type="application/json", headers={"Content-Disposition": "attachment; filename*=UTF-8''cabinet.json"})

        self.agent = PortalAgent(self.fixture.assistant, PortalAPICatalog(app), self.fixture.files)

    def prepare(self, kind="export", followup=False):
        op = {"api_id": "POST /api/cabinet-power/exports", "body": {"scope": "E"}}
        if kind == "batch":
            op = {"api_id": "POST /api/cabinet-power/export-batches", "body": {}}
        elif kind == "drill":
            op = {"api_id": "POST /api/drills/{drill_id}/generate", "path_params": {"drill_id": "drill-E"},
                  "params": {"scope": "E"}, "body": {"expected_version": 3}}
        ops = [op] + ([{"api_id": "POST /api/batch/finalize"}] if followup else [])
        return self.agent.prepare(ACTOR, {"operations": ops}, "downloads-fixture", [])

    async def execute(self, plan):
        result = await self.agent.confirm(ACTOR, plan["id"], {"version": plan["version"], "stage": "review"}, self.fixture.request)
        if result["status"] == "awaiting_second_confirmation":
            await self.agent.confirm(ACTOR, plan["id"], {"version": result["version"], "stage": "execute"}, self.fixture.request)
        if self.agent.tasks:
            await asyncio.gather(*tuple(self.agent.tasks))
        return self.agent.get_plan(ACTOR, plan["id"])

    def public(self, plan):
        return self.agent.public_plan(plan, ACTOR)

    async def test_single_export_download_after_poll_without_second_export(self):
        plan = self.prepare()
        done = await self.execute(plan)
        self.assertEqual(done["status"], "completed", done["error"])
        self.assertEqual(self.public(done)["results"][0]["downloads"], [{"name": self.export["filename"], "url": "/api/cabinet-power/exports/export-E/download"}])
        await self.execute(plan)
        self.assertEqual(self.writes, ["export"])

    async def test_five_exports_and_failed_archive_keep_files_but_fail_plan(self):
        for fail in (False, True):
            with self.subTest(fail=fail):
                self.batch["status"] = "failed" if fail else "succeeded"
                self.batch["error"] = "隔离归档失败" if fail else ""
                done = await self.execute(self.prepare("batch", followup=fail))
                self.assertEqual(done["status"], "failed" if fail else "completed", done["error"])
                result = self.public(done)["results"][0]
                self.assertEqual(len(result["downloads"]), 5)
                self.assertEqual(result["ok"], not fail)
                self.assertNotIn("finalize", self.writes)

    async def test_partial_building_failure_only_links_completed_buildings(self):
        self.batch.update(status="failed", error="C楼导出失败")
        self.batch["items"]["C"] = {"scope": "C", "status": "failed", "error": "isolated"}
        done = await self.execute(self.prepare("batch"))
        urls = [item["url"] for item in self.public(done)["results"][0]["downloads"]]
        self.assertEqual(len(urls), 4)
        self.assertFalse(any("export-C/" in url for url in urls))
        self.assertEqual(done["status"], "failed")

    async def test_drill_current_file_and_cloud_failure_are_distinct(self):
        for failed in (False, True):
            self.execution.update(status="error" if failed else "synced", last_error="云端同步失败" if failed else "")
            done = await self.execute(self.prepare("drill"))
            self.assertEqual(done["status"], "failed" if failed else "completed", done["error"])
            self.assertEqual(self.public(done)["results"][0]["downloads"], [{"name": "E楼演练.xlsx", "url": "/api/drills/drill-E/download?scope=E"}])

    async def test_changed_drill_version_has_no_old_task_download(self):
        self.execution["execution_version"] = self.execution["generated_version"] = 4
        done = await self.execute(self.prepare("drill"))
        self.assertEqual(done["status"], "failed")
        self.assertEqual(self.public(done)["results"][0]["downloads"], [])

    async def test_restart_refresh_failed_batch_keeps_downloads_and_no_next_write(self):
        plan = self.prepare("batch", followup=True)
        self.batch.update(status="failed", error="isolated archive failure")
        self.batch["batch_id"] = plan["operations"][0]["body"]["batch_id"]
        initial = {"ok": True, "api_id": plan["operations"][0]["api_id"], "_raw": {"batch_id": self.batch["batch_id"], "status": "running"}}
        initial["_task"] = self.agent._task_spec(plan["operations"][0], initial)
        plan.update(status="submitted", results=[initial])
        self.agent._save_plan(ACTOR, plan)
        self.agent = PortalAgent(self.fixture.assistant, self.agent.catalog, self.fixture.files)
        result = await self.agent.refresh(ACTOR, plan, self.fixture.request)
        self.assertEqual(result["status"], "failed")
        self.assertFalse(result["results"][0]["ok"])
        self.assertEqual(len(result["results"][0]["downloads"]), 5)
        self.assertEqual(self.writes, [])

    async def test_authenticated_queries_include_files_without_binary_fetch(self):
        for op, count in (
            ({"api_id": "GET /api/drills/{drill_id}/execution", "path_params": {"drill_id": "drill-E"}, "params": {"scope": "E"}}, 1),
            ({"api_id": "GET /api/cabinet-power/export-history", "params": {"scope": "E"}}, 1),
            ({"api_id": "GET /api/critical-guard/tasks/{task_id}", "path_params": {"task_id": "guard-task"}, "params": {"scope": "E"}}, 3),
        ):
            result = await self.agent._invoke(ACTOR, op, self.fixture.request)
            self.assertTrue(result["ok"], result)
            self.assertEqual(len(result["downloads"]), count)
        self.assertEqual(self.writes, [])

    async def test_guard_archive_uses_native_link_without_buffering_large_zip(self):
        op = {"api_id": "GET /api/critical-guard/tasks/{task_id}/download", "path_params": {"task_id": "guard-task"}, "params": {"sheet_type": "电气检查"}}
        result = await self.agent._invoke(ACTOR, op, self.fixture.request)
        self.assertEqual(len(result["downloads"]), 1)
        self.assertIn("/tasks/guard-task/download?sheet_type=", result["downloads"][0]["url"])
        with self.assertRaises(AssistantError):
            await self.agent._invoke({**ACTOR, "is_admin": False}, op, self.fixture.request)
        with self.assertRaises(AssistantError):
            await self.agent._invoke({**ACTOR, "scopes": ["A"]}, op, self.fixture.request)
        with self.assertRaises(AssistantError):
            await self.agent._invoke(ACTOR, {**op, "params": {"sheet_type": "不存在的类型"}}, self.fixture.request)
        self.guard["task_id"] = "other-task"
        with self.assertRaises(AssistantError):
            await self.agent._invoke(ACTOR, op, self.fixture.request)

    async def test_json_attachment_is_delivered_as_file_not_response_dict(self):
        result = await self.agent._invoke(ACTOR, {"api_id": "GET /api/cabinet-power/exports/{export_id}/download",
                                                  "path_params": {"export_id": "export-E"}, "params": {"scope": "E"}},
                                          self.fixture.request)
        self.assertTrue(result["ok"], result)
        file = result["data"]
        self.assertEqual(file["name"], "cabinet.json")
        self.assertRegex(file["url"], r"^/api/assistant/files/[a-f0-9]{32}$")
        stored = self.fixture.files.get(ACTOR, file["id"])
        self.assertEqual(json.loads(Path(stored["path"]).read_text(encoding="utf-8"))["records"][0]["title"], "E楼机柜统计")
        with self.assertRaises(AssistantError):
            self.fixture.files.get({**ACTOR, "id": "other"}, file["id"])

    async def test_query_files_flow_through_model_and_stream_restart(self):
        async def stream_model(messages, info):
            has_result = any(getattr(part, "part_kind", "") == "tool-return" for message in messages for part in message.parts)
            if not has_result:
                yield {0: DeltaToolCall(name="query", json_args=json.dumps({"operation": {"api_id": "GET /api/drills/{drill_id}/execution",
                    "path_params": {"drill_id": "drill-E"}, "params": {"scope": "E"}}}))}
            else:
                yield "E楼演练文件已生成。"

        @asynccontextmanager
        async def model_factory(*args):
            yield FunctionModel(stream_function=stream_model)

        engine = LighthouseModel(self.agent, model_factory=model_factory)
        runtime = LighthouseStream(self.agent, engine=engine)
        self.addAsyncCleanup(runtime.close)
        authorize = AsyncMock(return_value=ACTOR)
        payload = {"question": "查看E楼演练生成文件", "operation_id": "fixture_downloads_query", "conversation_id": self.fixture.assistant._state(ACTOR)["id"]}
        ack = await runtime.submit(ACTOR, payload, self.fixture.request, authorize)
        await asyncio.wait_for(asyncio.gather(*runtime.workers.values()), 15)
        state = await runtime.conversation(ACTOR)
        self.assertEqual(len(state["turns"][-1]["downloads"]), 1)
        # Simulate terminal SSE saved, but the separate conversation save lost.
        stored = self.fixture.assistant._state(ACTOR)
        stored["active_run_id"] = ack["run_id"]
        stored["turns"][-1]["downloads"] = []
        stored["turns"][-1]["status"] = "pending"
        self.fixture.store.put_document(NAMESPACE, self.fixture.assistant._key(ACTOR), stored)
        fresh = LighthouseStream(self.agent, engine=engine)
        self.addAsyncCleanup(fresh.close)
        restored = await fresh.conversation(ACTOR)
        self.assertEqual(restored["turns"][-1]["downloads"], state["turns"][-1]["downloads"])
        denied = await fresh.conversation({**ACTOR, "scopes": ["A"]})
        self.assertEqual(denied["turns"], [])
        self.assertEqual(self.writes, [])


if __name__ == "__main__":
    unittest.main()
