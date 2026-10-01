"""Typed model calls keep authorized person/file references usable without exposure."""
import asyncio
import copy
import io
import json
import sys
import tempfile
import unittest
import zipfile
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import JSONResponse, Response
from openpyxl import Workbook
from pydantic_ai.models.function import DeltaToolCall, FunctionModel
from pydantic_ai.messages import ModelResponse, TextPart
from clipflow_backend.api_models import DrillExecutionRequest, DrillGenerateRequest, EngineerMopFillRequest
from lan_bitable_template_portal.lighthouse_agent import PortalAgent, _result_refs
from lan_bitable_template_portal.lighthouse_ai import AssistantError, LighthouseAssistant
from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog
from lan_bitable_template_portal.lighthouse_files import LighthouseFiles
from lan_bitable_template_portal.lighthouse_model import LighthouseModel
from test_lighthouse_stream import Store
from test_drill_management import _fixture_xlsx, _signature_png
from lan_bitable_template_portal.drill_management import DrillManagementService, _parse_workbook

ACTOR = {"id": "reference-owner", "scopes": ["A"], "is_admin": False}


class ReferenceWorkflowTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = Store(Path(self.tmp.name) / "state.sqlite3")
        model = Mock()
        model.settings.return_value = {"configured": True, "enabled": True, "active_model_id": "default",
            "models": [{"id": "default", "name": "fixture", "model": "fixture-model", "configured": True}]}
        self.assistant = LighthouseAssistant(self.store, lambda *_: ([], []), model=model)
        self.files = LighthouseFiles(self.store)
        self.writes, self.returned = [], []
        self.request = Request({"type": "http", "scheme": "http", "server": ("testserver", 80),
            "client": ("127.0.0.1", 123), "path": "/api/assistant/messages", "root_path": "", "query_string": b"",
            "headers": [(b"origin", b"http://testserver"), (b"cookie", b"fixture=owner")]})
        self.app = FastAPI()

    async def answer(self, stream, question, *, history=None, turn=None):
        @asynccontextmanager
        async def factory(*_):
            yield FunctionModel(stream_function=stream)
        self.portal = PortalAgent(self.assistant, PortalAPICatalog(self.app), self.files)
        engine = LighthouseModel(self.portal, model_factory=factory)
        async def emit(*_): pass
        async def authorize(): return copy.deepcopy(ACTOR)
        turn = turn or {"question": question, "operation_id": "reference_workflow_00001", "file_ids": [],
            "_profile": {"id": "default", "name": "fixture", "model": "fixture-model"}}
        self.turn = turn
        return await engine.answer(ACTOR, turn, history or [], self.request, emit, authorize, {})

    async def execute_plan(self, result):
        plan = result["plan"]
        plan = await self.portal.confirm(ACTOR, plan["id"], {"version": plan["version"], "stage": "review"}, self.request)
        if plan["status"] == "awaiting_second_confirmation":
            plan = await self.portal.confirm(ACTOR, plan["id"], {"version": plan["version"], "stage": "execute"}, self.request)
        if self.portal.tasks:
            await asyncio.gather(*tuple(self.portal.tasks))
        return self.portal.get_plan(ACTOR, plan["id"])

    @staticmethod
    def tools(messages):
        return [part.content for message in messages for part in message.parts if getattr(part, "part_kind", "") == "tool-return"]

    async def test_person_reference_reaches_model_then_native_signer_request(self):
        @self.app.get("/api/signatures/people")
        async def people(scope: str):
            return {"ok": True, "data": {"items": [{"source": "staff", "record_id": "rec-signer", "name": "测试签署人",
                "open_id": "opaque-person-identity", "has_signature": True, "image_base64": "never-model-signature"}]}}

        @self.app.post("/api/engineer/mop/fill")
        async def fill(body: EngineerMopFillRequest):
            self.writes.append(body.model_dump())
            return {"ok": True, "data": {"filled": True}}

        async def stream(messages, info):
            results = self.tools(messages)
            if not results:
                yield {0: DeltaToolCall(name="query", json_args=json.dumps({"operation": {"api_id": "GET /api/signatures/people", "params": {"scope": "A"}}}))}
            elif len(results) == 1:
                self.returned.append(copy.deepcopy(results[0]))
                person = results[0]["data"]["items"][0]
                self.assertTrue(person["signing_available"])
                yield {0: DeltaToolCall(name="prepare_business", json_args=json.dumps({"title": "填写维护单签名", "operations": [{
                    "api_id": "POST /api/engineer/mop/fill", "body": {"scope": "A", "signatures": [{
                        "source": "staff", "role": "implementer", "record_id": person["record_id"], "name": person["name"],
                        "open_id": {"$reference": person["person_ref"]}}]}}]}))}
            else:
                yield "已准备维护单签名，请核对。"
        result = await self.answer(stream, "查找A楼测试签署人并准备填写维护单签名")
        self.assertEqual(self.writes, [])
        public = json.dumps(self.returned + result["sources"], ensure_ascii=False)
        self.assertIn("person_", public)
        self.assertNotIn("opaque-person-identity", public)
        self.assertNotIn("never-model-signature", public)
        plan = await self.execute_plan(result)
        self.assertEqual(plan["status"], "completed", plan.get("error"))
        self.assertEqual(len(self.writes), 1)
        self.assertEqual(self.writes[0]["signatures"][0]["open_id"], "opaque-person-identity")

    async def test_mop_attachment_and_document_paths_use_bound_references(self):
        path = str(Path(self.tmp.name) / "owner-template.xlsx")
        read_tokens = []
        @self.app.get("/api/engineer/mop/bootstrap")
        async def bootstrap(scope: str):
            return {"ok": True, "data": {"items": [{"record_id": "rec-mop", "title": "测试维护单", "attachments": [{
                "file_token": "opaque-file-identity", "name": "owner-template.xlsx"}]}]}}

        @self.app.get("/api/engineer/mop/preview")
        async def preview(scope: str, mop_record_id: str, file_token: str):
            read_tokens.append(file_token)
            return {"ok": True, "data": {"local_file": {"path": path, "file_name": "owner-template.xlsx"}, "scope": scope}}

        @self.app.post("/api/engineer/mop/fill")
        async def fill(body: EngineerMopFillRequest):
            self.writes.append(body.model_dump())
            return {"ok": True, "data": {"filled": True}}

        async def stream(messages, info):
            results = self.tools(messages)
            self.returned[:] = copy.deepcopy(results)
            if not results:
                operation = {"api_id": "GET /api/engineer/mop/bootstrap", "params": {"scope": "A"}}
                yield {0: DeltaToolCall(name="query", json_args=json.dumps({"operation": operation}))}
            elif len(results) == 1:
                document = results[0]["data"]["items"][0]["documents"][0]
                operation = {"api_id": "GET /api/engineer/mop/preview", "params": {"scope": "A", "mop_record_id": "rec-mop",
                    "file_token": {"$reference": document["business_refs"][0]["ref"]}}}
                yield {0: DeltaToolCall(name="query", json_args=json.dumps({"operation": operation}))}
            elif len(results) == 2:
                document = results[1]["data"]["document"]
                yield {0: DeltaToolCall(name="prepare_business", json_args=json.dumps({"title": "填写维护单", "operations": [{
                    "api_id": "POST /api/engineer/mop/fill", "body": {"scope": "A", "mop_record_id": "rec-mop",
                        "local_file_path": {"$reference": document["business_refs"][0]["ref"]}}}]}))}
            else:
                yield "维护单已准备。"
        result = await self.answer(stream, "读取A楼维护单的附件，预览后准备填写")
        self.assertEqual(read_tokens, ["opaque-file-identity"])
        public = json.dumps(self.returned + result["sources"], ensure_ascii=False)
        self.assertNotIn("opaque-file-identity", public)
        self.assertNotIn(path, public)
        refs = result["_references"]
        path_ref = next(key for key, value in refs.items() if isinstance(value, dict) and value["field"] == "local_file_path")
        with self.assertRaises(AssistantError):
            _result_refs({"progress": {"$reference": path_ref}}, [], refs)
        plan = await self.execute_plan(result)
        self.assertEqual(plan["status"], "completed", plan.get("error"))
        self.assertEqual(self.writes[0]["local_file_path"], path)

    async def test_summary_keeps_ref_names_without_raw_identities_or_paths(self):
        sent = []
        def summarize(messages, info):
            sent.append(str(messages))
            return ModelResponse(parts=[TextPart("授权人员与文件的选择已保留。")])
        @asynccontextmanager
        async def factory(*_):
            yield FunctionModel(function=summarize)
        portal = PortalAgent(self.assistant, PortalAPICatalog(self.app), self.files)
        engine = LighthouseModel(portal, model_factory=factory)
        turns = [{"question": "选择测试人填写原文件", "_references": {
            "person_fixture": "hidden-person-identity", "value_fixture": {"field": "local_file_path", "value": "D:/private/hidden-file.xlsx"}}}]
        await engine.summarize(ACTOR, turns, "", {})
        self.assertIn("person_fixture", sent[0])
        self.assertIn("value_fixture", sent[0])
        self.assertNotIn("hidden-person-identity", sent[0])
        self.assertNotIn("D:/private/hidden-file.xlsx", sent[0])

    async def test_failed_native_read_returns_reason_without_retrying_or_inventing_count(self):
        @self.app.get("/api/repair-management/records")
        async def records(scope: str):
            return JSONResponse({"ok": False, "error": "来源资料读取暂忙。"}, status_code=503)
        attempts = []
        async def stream(messages, info):
            attempts.append(1)
            if not self.tools(messages):
                yield {0: DeltaToolCall(name="query", json_args=json.dumps({"operation": {
                    "api_id": "GET /api/repair-management/records", "params": {"scope": "A"}}}))}
            else:
                yield "A楼相关维修记录有5条。"
        result = await self.answer(stream, "查询A楼柴发相关维修记录共有几条")
        self.assertEqual(len(attempts), 2)
        self.assertIn("来源资料读取暂忙", result["answer"])
        self.assertIn("暂无法确认", result["answer"])
        self.assertNotIn("5条", result["answer"])
        self.assertFalse(result["sources"][0]["available"])

    async def test_downloaded_file_can_be_used_by_later_tool_in_same_run(self):
        data = io.BytesIO()
        workbook = Workbook()
        workbook.active["A1"] = "isolated workbook"
        workbook.save(data)
        workbook.close()
        raw = data.getvalue()

        @self.app.get("/api/drills/{drill_id}/download")
        async def download(drill_id: str, scope: str):
            return Response(raw, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={"Content-Disposition": 'attachment; filename="fixture.xlsx"'})

        @self.app.post("/api/engineer/mop/upload-local")
        async def upload(file: UploadFile = File(...), scope: str = Form(...)):
            self.writes.append({"scope": scope, "name": file.filename, "content": await file.read()})
            return {"ok": True, "data": {"upload_id": "fixture-upload"}}

        async def stream(messages, info):
            results = self.tools(messages)
            if not results:
                yield {0: DeltaToolCall(name="query", json_args=json.dumps({"operation": {"api_id": "GET /api/drills/{drill_id}/download",
                    "params": {"scope": "A"}, "path_params": {"drill_id": "fixture"}}}))}
            elif len(results) == 1:
                file = results[0]["data"]
                yield {0: DeltaToolCall(name="prepare_business", json_args=json.dumps({"title": "上传授权文件", "operations": [{
                    "api_id": "POST /api/engineer/mop/upload-local", "body": {"scope": "A"}, "files": {"file": [file["id"]]}}]}))}
            else:
                yield "文件已取得，准备上传。"
        result = await self.answer(stream, "下载A楼演练fixture文件，并准备作为本地维护单上传")
        self.assertEqual(self.writes, [])
        self.assertEqual(len(result["output_files"]), 1)
        self.assertEqual(result["output_files"][0]["name"], "fixture.xlsx")
        self.assertEqual(result["file_ids"], [result["output_files"][0]["id"]])
        plan = await self.execute_plan(result)
        self.assertEqual(plan["status"], "completed", plan.get("error"))
        self.assertEqual(self.writes, [{"scope": "A", "name": "fixture.xlsx", "content": raw}])

    async def test_confirmed_signer_flow_generates_native_excel_and_waits_for_sync(self):
        service = DrillManagementService(self.store, data_root=Path(self.tmp.name) / "drills")
        definition = service.create_definition(name="隔离签名演练", year=2026, month=10, file_name="fixture.xlsx", source=_fixture_xlsx(with_evaluator=True))
        definition = service.publish(definition["drill_id"], expected_version=definition["version"])
        people = [{"record_id": f"person-{i}", "name": f"测试人员{i}"} for i in range(4)]
        execution = service.save_execution(definition["drill_id"], "A", {
            "drill_date": "2026-10-01", "first_start_time": "09:00", "commander": people[0], "participants": people,
            "evaluator": people[3], "signature_time": "2026-10-01T10:15", "evaluation_time": "2026-10-01T11:32",
            "step_signers": {str(step["row"]): [p["record_id"] for p in people[:step["signature_slots"]]] for step in definition["configuration"]["steps"]}}, expected_version=0)
        generation_calls = []

        @self.app.put("/api/drills/{drill_id}/execution")
        async def save(drill_id: str, body: DrillExecutionRequest, scope: str):
            return {"ok": True, "data": service.save_execution(drill_id, scope, body.model_dump(exclude={"expected_version"}), expected_version=body.expected_version)}

        @self.app.post("/api/drills/{drill_id}/generate")
        async def generate(drill_id: str, body: DrillGenerateRequest, scope: str):
            queued = service.queue_generation(drill_id, scope, expected_version=body.expected_version)
            generation_calls.append((drill_id, scope))
            return JSONResponse({"ok": True, "data": {"queued": True, "execution": queued}}, status_code=202)

        @self.app.get("/api/drills/{drill_id}/execution")
        async def state(drill_id: str, scope: str):
            current = service.get_execution(drill_id, scope)
            if current.get("generation_queue"):
                generated = service.generate(drill_id, scope, signatures={p["record_id"]: _signature_png() for p in people})
                current = service.mark_sync_result(drill_id, scope, generated_version=generated["execution"]["generated_version"], record_id="isolated-cloud-record")
            return {"ok": True, "data": {"execution": current}}

        self.portal = PortalAgent(self.assistant, PortalAPICatalog(self.app), self.files)
        decision = {"operations": [{"api_id": "PUT /api/drills/{drill_id}/execution", "path_params": {"drill_id": definition["drill_id"]}, "params": {"scope": "A"},
                "body": {"signature_time": "2026-10-01T10:30", "evaluation_time": "2026-10-01T11:45"}},
            {"api_id": "POST /api/drills/{drill_id}/generate", "path_params": {"drill_id": definition["drill_id"]}, "params": {"scope": "A"},
                "body": {"expected_version": {"$result": {"step": 0, "path": "version"}}}}]}
        plan = self.portal.prepare(ACTOR, decision, "native_signer_workflow_00001", [], queries={"current": {"execution": execution}})
        self.assertEqual(generation_calls, [])
        self.assertEqual(plan["operations"][0]["body"]["participants"], execution["participants"])
        self.assertEqual(_result_refs({"$query": {"ref": "current", "path": "execution.signature_time"}}, [], queries={"current": {"execution": execution}}), "2026-10-01T10:15")
        with self.assertRaises(AssistantError):
            _result_refs({"$query": {"ref": "current", "path": "execution.signature_image"}}, [], queries={"current": {"execution": {"signature_image": "private"}}})
        plan = await self.execute_plan({"plan": self.portal.public_plan(plan)})
        self.assertEqual(plan["status"], "completed", plan.get("error"))
        self.assertEqual(generation_calls, [(definition["drill_id"], "A")])
        self.assertEqual(plan["results"][1]["job_result"]["_raw"]["execution"]["status"], "synced")
        path, _ = service.generated_file(definition["drill_id"], "A")
        book = _parse_workbook(path)
        record = next(sheet for sheet in book["sheets"] if sheet["name"] == "本月记录")
        assessment = next(sheet for sheet in book["sheets"] if sheet["name"] == "评估表")
        self.assertEqual(record["cells"]["G18"], "2026年10月01日 10时30分")
        self.assertEqual(assessment["cells"]["H12"], "2026年10月01日 11时45分")
        with zipfile.ZipFile(path) as archive:
            self.assertGreater(sum("drill_signature_" in name for name in archive.namelist()), 2)
        public = json.dumps(self.portal.public_plan(plan), ensure_ascii=False)
        self.assertNotIn("data:image", public)
        spec = plan["results"][1]["_task"]
        current = service.get_execution(definition["drill_id"], "A")
        for changes in ({"status": "sync_pending", "last_error": "隔离同步失败"}, {"execution_version": current["execution_version"] + 1}):
            saved = {**current, **changes}
            self.store.put_document("drill_execution", current["key"], saved)
            with self.assertRaises(AssistantError):
                await self.portal._task_result(ACTOR, spec, self.request)


if __name__ == "__main__":
    unittest.main()
