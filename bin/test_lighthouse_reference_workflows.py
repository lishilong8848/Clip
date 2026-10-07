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
from clipflow_backend.api_models import DrillExecutionRequest, DrillGenerateRequest, EngineerMopFillRequest, RepairFollowupRecordRequest
from lan_bitable_template_portal.lighthouse_agent import PortalAgent, _result_refs
from lan_bitable_template_portal.lighthouse_ai import AssistantError, LighthouseAssistant
from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog
from lan_bitable_template_portal.lighthouse_files import LighthouseFiles
from lan_bitable_template_portal.lighthouse_model import LighthouseModel
from test_lighthouse_stream import Store
from test_drill_management import _fixture_xlsx, _signature_png
from lan_bitable_template_portal.drill_management import DrillManagementService, _parse_workbook
from lan_bitable_template_portal.portal_service import MaintenancePortalService

ACTOR = {"id": "reference-owner", "scopes": ["A"], "is_admin": False, "learning_scopes": ["A"]}


def _mop_preview(path, *, record_id="fixture-mop"):
    rows = [["维护实施人", ""], ["维护审核人", ""], ["维护完成时间", "2026-10-02 10:30"]]
    sheet = {"name": "填写页", "rows": rows, "row_count": len(rows), "columns": ["A", "B"],
             **MaintenancePortalService._extract_mop_sheet_targets(sheet_name="填写页", rows=rows)}
    return {"scope": "A", "mop_record_id": record_id, "mop_title": "测试维护单", "mop_file_name": "owner-template.xlsx",
            "local_file": {"path": path, "file_name": "owner-template.xlsx"}, "sheets": [sheet]}


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
        if plan["status"] == "needs_input":
            plan = self.portal.amend(ACTOR, plan["id"], {"version": plan["version"], "values": {field["name"]: field.get("value") for field in plan["fields"]}})
        plan = await self.portal.confirm(ACTOR, plan["id"], {"version": plan["version"], "stage": "review"}, self.request)
        if plan["status"] == "awaiting_second_confirmation":
            plan = await self.portal.confirm(ACTOR, plan["id"], {"version": plan["version"], "stage": "execute"}, self.request)
        if self.portal.tasks:
            await asyncio.gather(*tuple(self.portal.tasks))
        return self.portal.get_plan(ACTOR, plan["id"])

    @staticmethod
    def tools(messages):
        return [part.content for message in messages for part in message.parts if getattr(part, "part_kind", "") == "tool-return"]

    async def test_query_result_pages_reach_water_rows_after_initial_sample_without_refetch(self):
        reads = []
        rows = [{"record_id": f"w{i}", "meter_no": f"M{i}", "scope": "A", "secret": "hidden-value"} for i in range(73)]
        @self.app.get("/api/capacity/water/records")
        async def water(scope: str, month: str):
            reads.append((scope, month))
            return {"ok": True, "data": {"scope": scope, "month": month, "total": 73, "records": copy.deepcopy(rows)}}

        async def stream(messages, info):
            results = self.tools(messages)
            if not results:
                yield {0: DeltaToolCall(name="query", json_args=json.dumps({"operation": {
                    "api_id": "GET /api/capacity/water/records", "params": {"scope": "A", "month": "2026-10"}}}))}
            elif len(results) <= 2:
                if len(results) == 1:
                    self.assertEqual(len(results[0]["data"]["records"]), 40)
                else:
                    self.assertEqual(results[1]["data"]["loaded_count"], 73)
                    self.assertEqual(results[1]["next_offset"], 60)
                yield {0: DeltaToolCall(name="read_query", json_args=json.dumps({
                    "query_ref": results[0]["query_ref"], "path": ["records"], "offset": 40 if len(results) == 1 else 60, "limit": 20}))}
            else:
                self.assertEqual([row["record_id"] for row in results[1]["data"]["items"]], [f"w{i}" for i in range(40, 60)])
                self.assertEqual([row["record_id"] for row in results[2]["data"]["items"]], [f"w{i}" for i in range(60, 73)])
                self.assertIsNone(results[2]["next_offset"])
                self.assertNotIn("hidden-value", json.dumps(results))
                self.assertTrue(results[2]["data"]["snapshot_only"])
                yield "已读取后续水耗记录，末项为w72。[3]"
        result = await self.answer(stream, "查看A楼本月水耗记录的后续明细，不只看前40条")
        self.assertEqual(reads, [("A", "2026-10")])
        self.assertEqual(len(result["sources"]), 3)
        self.assertEqual(len(result["sources"][2]["data"]["items"]), 13)
        self.assertEqual(result["sources"][2]["queried_at"], result["sources"][0]["queried_at"])
        self.assertEqual(rows[0]["secret"], "hidden-value")

    async def test_query_slice_is_not_native_pagination_and_private_paths_are_not_readable(self):
        reads = []
        @self.app.get("/api/repair-management/records")
        async def records(scope: str, page: int = 1, page_size: int = 2):
            reads.append(page)
            return {"ok": True, "data": {"items": [{"id": f"r{page}-{i}", "scope": scope} for i in range(2)],
                "total": 105, "page": page, "page_size": 2,
                "attachments": [{"name": "private-image"}], "address": ["private-home"]}}

        async def stream(messages, info):
            results = self.tools(messages)
            if not results:
                yield {0: DeltaToolCall(name="query", json_args=json.dumps({"operation": {
                    "api_id": "GET /api/repair-management/records", "params": {"scope": "A", "page": 1, "page_size": 2}}}))}
            elif len(results) < 5:
                number = len(results)
                yield {0: DeltaToolCall(name="read_query", json_args=json.dumps({
                    "query_ref": "query_not_this_turn" if number == 4 else results[0]["query_ref"],
                    "path": ["attachments"] if number == 2 else ["address"] if number == 3 else ["items"],
                    "offset": 2 if number == 1 else 0}))}
            else:
                self.assertEqual(results[1]["data"]["loaded_count"], 2)
                self.assertEqual(results[1]["data"]["items"], [])
                self.assertIsNone(results[1]["next_offset"])
                self.assertTrue(all(result["ok"] is False for result in results[2:]))
                self.assertNotIn("private-image", json.dumps(results[1:]))
                self.assertNotIn("private-home", json.dumps(results))
                yield "当前只读取了原接口第一页，不能把它当作105条完整记录。"
        result = await self.answer(stream, "查看A楼维修记录的查询内容")
        self.assertEqual(reads, [1])
        self.assertEqual(len(result["sources"]), 2)

    async def test_query_slice_rechecks_permission_before_reading(self):
        revoked = False
        @self.app.get("/api/capacity/water/records")
        async def water(scope: str):
            return {"ok": True, "data": {"records": [{"record_id": "w1", "scope": "A"}]}}
        async def stream(messages, info):
            nonlocal revoked
            results = self.tools(messages)
            if not results:
                yield {0: DeltaToolCall(name="query", json_args=json.dumps({"operation": {
                    "api_id": "GET /api/capacity/water/records", "params": {"scope": "A"}}}))}
            else:
                revoked = True
                yield {0: DeltaToolCall(name="read_query", json_args=json.dumps({"query_ref": results[0]["query_ref"], "path": ["records"]}))}
        @asynccontextmanager
        async def factory(*_):
            yield FunctionModel(stream_function=stream)
        async def authorize():
            return {**ACTOR, "scopes": [] if revoked else ["A"]}
        async def emit(*_): pass
        self.portal = PortalAgent(self.assistant, PortalAPICatalog(self.app), self.files)
        engine = LighthouseModel(self.portal, model_factory=factory)
        with self.assertRaises(AssistantError):
            await engine.answer(ACTOR, {"question": "查看A楼水耗记录", "_profile": {"id": "default", "name": "fixture", "model": "fixture-model"}}, [], self.request, emit, authorize, {})

    async def test_repair_prepare_reads_missing_native_schema_without_aborting_or_writing(self):
        @self.app.get("/api/repair-management/followups")
        async def followups(scope: str, summary_record_id: str):
            self.assertEqual((scope, summary_record_id), ("A", "rec-parent"))
            return {"ok": True, "data": {"summary_record_id": summary_record_id, "relation_mode": "record_id", "records": [],
                "fields": [{"field_name": "维修进展描述", "field_type": 1, "editable": True, "options": []}]}}
        @self.app.post("/api/repair-management/followups")
        async def create(body: RepairFollowupRecordRequest):
            raise AssertionError("form preparation must never upload")
        operation = {"api_id": "POST /api/repair-management/followups", "body": {"scope": "A", "summary_record_id": "rec-parent", "cmdb_record_ids": []}}
        async def stream(messages, info):
            results = self.tools(messages)
            if not results:
                yield {0: DeltaToolCall(name="prepare_business", json_args=json.dumps({"title": "填写维修跟进", "operations": [operation], "fields": [{"path": "fields", "type": "object"}]}))}
            elif len(results) == 1:
                self.assertFalse(results[0]["ok"])
                self.assertFalse(results[0]["business_written"])
                self.assertIn("字段定义", results[0]["error"])
                yield {0: DeltaToolCall(name="query", json_args=json.dumps({"operation": {"api_id": "GET /api/repair-management/followups", "params": {"scope": "A", "summary_record_id": "rec-parent"}}}))}
            elif len(results) == 2:
                yield {0: DeltaToolCall(name="prepare_business", json_args=json.dumps({"title": "填写维修跟进", "operations": [operation], "fields": [{"path": "fields", "type": "object"}]}))}
            else:
                yield "跟进表单已准备，请填写后核对。"
        result = await self.answer(stream, "为A楼这个维修项目填写新跟进，先给我表单")
        self.assertEqual(result["plan"]["status"], "needs_input")
        field = next(field for field in result["plan"]["fields"] if field["path"] == "fields")
        self.assertTrue(field["native_repair"])
        self.assertEqual(field["children"][0]["path"], "维修进展描述")
        self.assertEqual(self.writes, [])

    async def test_person_reference_reaches_model_then_native_signer_request(self):
        @self.app.get("/api/signatures/people")
        async def people(scope: str):
            return {"ok": True, "data": {"items": [{"source": "staff", "record_id": "rec-signer", "name": "测试签署人",
                "open_id": "opaque-person-identity", "has_signature": True, "image_base64": "never-model-signature"}]}}

        @self.app.post("/api/engineer/mop/fill")
        async def fill(body: EngineerMopFillRequest):
            self.writes.append(body.model_dump())
            return {"ok": True, "data": {"filled": True}}
        @self.app.get("/api/engineer/mop/preview")
        async def preview(scope: str):
            return {"ok": True, "data": _mop_preview(str(Path(self.tmp.name) / "owner-template.xlsx"))}

        async def stream(messages, info):
            results = self.tools(messages)
            if not results:
                yield {0: DeltaToolCall(name="query", json_args=json.dumps({"operation": {"api_id": "GET /api/signatures/people", "params": {"scope": "A"}}}))}
            elif len(results) == 1:
                self.returned.append(copy.deepcopy(results[0]))
                yield {0: DeltaToolCall(name="query", json_args=json.dumps({"operation": {"api_id": "GET /api/engineer/mop/preview", "params": {"scope": "A"}}}))}
            elif len(results) == 2:
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
        self.assertEqual(self.writes[0]["signatures"][0]["record_id"], "rec-signer")
        self.assertNotIn("open_id", self.writes[0]["signatures"][0])

    async def test_company_staff_directory_is_cross_building_but_business_record_is_not(self):
        # 公司人员目录 (GET /api/signatures/people) 的 scope 是签名使用上下文,不是员工记录属主:
        # A 楼账号可跨楼栋检索 B 楼在职员工并取得脱敏后的 name/employee_no/受保护 person_ref;
        # open_id/原始签名/证件号/地址仍不得进入公开输出。业务记录则仍走 scoped_result,越权 403。
        people_calls = []

        @self.app.get("/api/signatures/people")
        async def people(scope: str):
            people_calls.append(scope)
            return {"ok": True, "data": {"items": [{"source": "staff", "record_id": "rec-b-signer",
                "name": "B楼在职工", "employee_no": "EMP-B-1001", "building": "B",
                "open_id": "opaque-b-identity", "has_signature": True, "image_base64": "never-b-signature",
                "id_card": "11010119900307337X", "address": "B楼宿舍"}]}}

        @self.app.get("/api/repair-management/records/{record_id}")
        async def record(record_id: str, scope: str):
            return {"ok": True, "data": {"record": {"record_id": record_id, "building": "B", "title": "B楼设备"}}}

        async def stream(messages, info):
            results = self.tools(messages)
            if not results:
                yield {0: DeltaToolCall(name="query", json_args=json.dumps(
                    {"operation": {"api_id": "GET /api/signatures/people", "params": {"scope": "A"}}}))}
            elif len(results) == 1:
                self.returned.append(copy.deepcopy(results[0]))
                person = results[0]["data"]["items"][0]
                self.assertEqual(person["name"], "B楼在职工")
                self.assertEqual(person["employee_no"], "EMP-B-1001")
                self.assertTrue(person["signing_available"])
                yield {0: DeltaToolCall(name="query", json_args=json.dumps(
                    {"operation": {"api_id": "GET /api/repair-management/records/{record_id}",
                     "params": {"scope": "A"}, "path_params": {"record_id": "rec-b"}}}))}
            else:
                # 第二条(维修明细)查询结果应是被 scoped_result 拒绝且不带 B楼数据。
                repair = results[1]
                self.assertFalse(repair.get("ok"))
                self.assertIn("范围之外的楼栋", repair.get("error", ""))
                self.assertIsNone(repair.get("data"))
                self.assertNotIn("B楼设备", json.dumps(repair, ensure_ascii=False))
                yield "人员目录可跨楼栋查询；B楼维修明细越权，读取失败（范围之外的楼栋）。"
        result = await self.answer(stream, "查找B楼在职员工用于签名，并读取B楼维修明细")
        # native invoke 的原始 cookie/auth 路径仍然执行(people_calls 记录实际请求)。
        self.assertEqual(people_calls, ["A"])
        public = json.dumps(self.returned + result["sources"], ensure_ascii=False)
        self.assertIn("person_", public)          # 受保护的 person_ref 暴露给模型作引用
        self.assertIn("B楼在职工", public)
        self.assertIn("EMP-B-1001", public)
        self.assertNotIn("opaque-b-identity", public)
        self.assertNotIn("never-b-signature", public)
        self.assertNotIn("11010119900307337X", public)
        self.assertNotIn("B楼宿舍", public)
        # 越权业务记录(B楼):仍被 scoped_result 拒绝。单建筑查询失败时 query 提前返回,
        # 不产生该 source(来源里只剩人员目录那一条),并在答复中明确失败原因,
        # 不得泄露 B楼设备数据,不得 prepare/write。
        self.assertEqual(len(result["sources"]), 1)  # 仅人员目录一条来源
        self.assertIn("范围之外的楼栋", result["answer"])
        self.assertNotIn("B楼设备", result["answer"])
        self.assertEqual(self.writes, [])
        self.assertIsNone(result.get("plan"))

    async def test_plan_shared_rules_are_readable_and_unscoped_blocks_are_not_reported_as_zero(self):
        @self.app.get("/api/plan-convergence/blocks")
        async def blocks():
            return {"ok": True, "data": {"items": [{"blockId": "1", "blockName": "A楼屏蔽"},
                {"blockId": "2", "blockName": "B楼私有屏蔽"}, {"blockId": "3", "blockName": "楼栋未标明"}]}}
        @self.app.get("/api/plan-convergence/rulesets/{id}")
        async def shared(id: str):
            return {"ok": True, "data": {"id": id, "name": "B楼适用的共享规则", "items": [{"building": "B", "scope_type": "objtype"}]}}

        async def stream(messages, info):
            results = self.tools(messages)
            if not results:
                yield {0: DeltaToolCall(name="query", json_args=json.dumps({"operation": {"api_id": "GET /api/plan-convergence/blocks"}}))}
            elif len(results) == 1:
                self.assertEqual(len(results[0]["data"]["items"]), 1)
                self.assertFalse(results[0]["complete"])
                self.assertIn("未标明楼栋", results[0]["scope_warning"])
                self.assertNotIn("B楼私有屏蔽", json.dumps(results[0], ensure_ascii=False))
                yield {0: DeltaToolCall(name="query", json_args=json.dumps({"operation": {"api_id": "GET /api/plan-convergence/rulesets/{id}", "path_params": {"id": "set-1"}}}))}
            else:
                self.assertTrue(results[1]["ok"])
                self.assertEqual(results[1]["data"]["name"], "B楼适用的共享规则")
                yield "A楼已核实1条屏蔽；另有记录楼栋未标明，不能判断完整数量。共享规则已读取。"
        result = await self.answer(stream, "查看A楼屏蔽记录和共享规则")
        self.assertFalse(result["sources"][0]["complete"])
        self.assertIn("未标明楼栋", result["sources"][0]["warnings"][0])
        self.assertIn("未标明楼栋", result["warnings"][0])
        self.assertEqual(len(result["sources"]), 2)
        self.assertIsNone(result.get("plan"))

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
            return {"ok": True, "data": _mop_preview(path, record_id=mop_record_id)}

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
                self.assertTrue(results[1].get("ok"), results[1])
                document = results[1]["data"]["document"]
                yield {0: DeltaToolCall(name="prepare_business", json_args=json.dumps({"title": "填写维护单", "operations": [{
                    "api_id": "POST /api/engineer/mop/fill", "body": {"scope": "A", "mop_record_id": "rec-mop",
                        "signatures": [{"source": "staff", "role": "implementer", "record_id": "rec-signer", "name": "测试签署人"}],
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
            "person_fixture": "hidden-person-identity", "value_fixture": {"field": "local_file_path", "value": "D:/private/hidden-file.xlsx"}},
            "plan": {"id": "large-form", "status": "needs_input", "fields": [{"label": "机柜", "children": [{"label": "never-send-descriptor"}] * 500}],
                     "operations": [{"api_id": "PATCH /api/cabinet-power/batches/{batch_id}", "path_params": {"batch_id": "remember-this-target"}}]}}]
        await engine.summarize(ACTOR, turns, "", {})
        self.assertIn("person_fixture", sent[0])
        self.assertIn("value_fixture", sent[0])
        self.assertNotIn("hidden-person-identity", sent[0])
        self.assertNotIn("D:/private/hidden-file.xlsx", sent[0])
        self.assertIn("remember-this-target", sent[0])
        self.assertNotIn("never-send-descriptor", sent[0])

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
            return {"ok": True, "data": {"drill": definition, "execution": current}}

        self.portal = PortalAgent(self.assistant, PortalAPICatalog(self.app), self.files)
        decision = {"operations": [{"api_id": "PUT /api/drills/{drill_id}/execution", "path_params": {"drill_id": definition["drill_id"]}, "params": {"scope": "A"},
                "body": {"signature_time": "2026-10-01T10:30", "evaluation_time": "2026-10-01T11:45"}},
            {"api_id": "POST /api/drills/{drill_id}/generate", "path_params": {"drill_id": definition["drill_id"]}, "params": {"scope": "A"},
                "body": {"expected_version": {"$result": {"step": 0, "path": "version"}}}}]}
        plan = self.portal.prepare(ACTOR, decision, "native_signer_workflow_00001", [], queries={"current": {"drill": definition, "execution": execution}})
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
        self.store.put_document("drill_execution", current["key"], {**current, "status": "sync_pending", "last_error": "隔离同步失败"})
        failed, done = await self.portal._task_result(ACTOR, spec, self.request)
        self.assertTrue(done)
        self.assertEqual(failed["_task_error"], "隔离同步失败")
        self.assertEqual(len(failed["downloads"]), 1)
        self.store.put_document("drill_execution", current["key"], {**current, "execution_version": current["execution_version"] + 1})
        with self.assertRaises(AssistantError):
            await self.portal._task_result(ACTOR, spec, self.request)

    async def test_drill_form_retains_baseline_and_generates_with_commander_in_second_slot(self):
        from clipflow_backend.main import FastAPIPortalController
        service = DrillManagementService(self.store, data_root=Path(self.tmp.name) / "drill-form")
        definition = service.create_definition(name="演练表单隔离测试", year=2026, month=10, file_name="fixture.xlsx", source=_fixture_xlsx(with_evaluator=True))
        configuration = copy.deepcopy(definition["configuration"])
        next(step for step in configuration["steps"] if step["row"] == 13)["signature_slots"] = 2
        definition = service.save_configuration(definition["drill_id"], configuration, expected_version=definition["version"])
        definition = service.publish(definition["drill_id"], expected_version=definition["version"])
        execution = service.get_execution(definition["drill_id"], "A", create=True)
        people = [{"record_id": f"p{i}", "name": f"测试人员{i}", "building": "B", "has_signature": True} for i in range(4)]
        controller = FastAPIPortalController.__new__(FastAPIPortalController)
        controller._drill_people = lambda *_, **kw: people
        fixture_errors = []
        @self.app.middleware("http")
        async def capture_fixture_errors(request, call_next):
            try:
                return await call_next(request)
            except Exception as exc:
                fixture_errors.append(str(exc))
                raise

        @self.app.get("/api/drills/bootstrap")
        async def bootstrap(scope: str):
            self.assertEqual(scope, "A")
            return {"ok": True, "data": {"default_scope": scope, "people": people}}

        @self.app.put("/api/drills/{drill_id}/execution")
        async def save(drill_id: str, body: DrillExecutionRequest, scope: str):
            payload = body.model_dump(exclude={"expected_version"})
            controller._validate_drill_people_payload(scope, definition, payload, require_complete=False, require_signatures=False)
            self.writes.append(payload)
            return {"ok": True, "data": service.save_execution(drill_id, scope, payload, expected_version=body.expected_version)}

        @self.app.post("/api/drills/{drill_id}/generate")
        async def generate(drill_id: str, body: DrillGenerateRequest, scope: str):
            current = service.get_execution(drill_id, scope)
            controller._validate_drill_people_payload(scope, definition, current, require_complete=True, require_signatures=True)
            queued = service.queue_generation(drill_id, scope, expected_version=body.expected_version)
            return JSONResponse({"ok": True, "data": {"queued": True, "execution": queued}}, status_code=202)

        @self.app.get("/api/drills/{drill_id}/execution")
        async def state(drill_id: str, scope: str):
            current = service.get_execution(drill_id, scope)
            if current.get("generation_queue"):
                generated = service.generate(drill_id, scope, signatures={p["record_id"]: _signature_png() for p in people})
                current = service.mark_sync_result(drill_id, scope, generated_version=generated["execution"]["generated_version"], record_id="isolated-only")
            return {"ok": True, "data": {"drill": definition, "execution": current}}

        self.portal = PortalAgent(self.assistant, PortalAPICatalog(self.app), self.files)
        operations = [{"api_id": "PUT /api/drills/{drill_id}/execution", "path_params": {"drill_id": definition["drill_id"]}, "params": {"scope": "A"}, "body": {}},
                      {"api_id": "POST /api/drills/{drill_id}/generate", "path_params": {"drill_id": definition["drill_id"]}, "params": {"scope": "A"}, "body": {"expected_version": {"$result": {"step": 0, "path": "version"}}}}]
        plan = self.portal.prepare(ACTOR, {"operations": operations}, "drill_form_fixture_0001", [], queries={"snapshot": {"drill": definition, "execution": execution}})
        self.assertEqual(len(plan["fields"]), 1)
        field = plan["fields"][0]
        self.assertTrue(field["native_drill"])
        self.assertEqual(self.writes, [])
        foreign = Request({**self.request.scope, "query_string": b"scope=B"})
        with self.assertRaises(AssistantError):
            await self.portal.field_options(ACTOR, plan["id"], field["name"], foreign)
        loaded = await self.portal.field_options(ACTOR, plan["id"], field["name"], self.request)
        field = loaded["fields"][0]
        self.assertEqual(len(next(c for c in field["children"] if c["path"] == "commander")["options"]), 4)
        value = copy.deepcopy(field["value"])
        value.update(drill_date="2026-10-02", first_start_time="09:30", simulation_scenario="只修改演练填写",
                     commander={"record_id": "p1", "name": "伪造名字会被目录更正"}, participants=[{"record_id": "p2"}], evaluator={"record_id": "p3"},
                     signature_time="2026-10-02T10:15", evaluation_time="2026-10-02T11:32")
        value["step_signers"] = {str(step["row"]): (["p2", "p1"] + [""] * step["signature_slots"])[:step["signature_slots"]] for step in definition["configuration"]["steps"]}
        for changes in ({"commander": {"record_id": "not-in-directory"}}, {"step_signers": {"13": ["p1", "p1"]}}, {"expected_version": 999},
                        {"step_signers": {"13": ["p1", "p2", "p3"]}}):
            with self.assertRaises(AssistantError):
                self.portal.amend(ACTOR, plan["id"], {"version": loaded["version"], "values": {field["name"]: {**value, **changes}}})
        amended = self.portal.amend(ACTOR, plan["id"], {"version": loaded["version"], "values": {field["name"]: value}})
        controller._validate_drill_people_payload("A", definition, copy.deepcopy(amended["operations"][0]["body"]), require_complete=False, require_signatures=False)
        completed = await self.execute_plan({"plan": amended})
        self.assertEqual(completed["status"], "completed", (completed.get("error"), fixture_errors))
        saved = service.get_execution(definition["drill_id"], "A")
        self.assertEqual(saved["commander"]["name"], "测试人员1")
        self.assertEqual([p["record_id"] for p in saved["participants"]], ["p1", "p2"])
        self.assertEqual(saved["step_signers"]["13"], ["p2", "p1"])
        self.assertEqual(saved["signature_time"], "2026-10-02T10:15")
        self.assertEqual(saved["evaluation_time"], "2026-10-02T11:32")
        path, _ = service.generated_file(definition["drill_id"], "A")
        book = _parse_workbook(path)
        self.assertEqual(next(s for s in book["sheets"] if s["name"] == "本月记录")["cells"]["G18"], "2026年10月02日 10时15分")
        self.assertEqual(next(s for s in book["sheets"] if s["name"] == "评估表")["cells"]["H12"], "2026年10月02日 11时32分")
        self.assertNotIn("data:image", json.dumps(self.portal.public_plan(completed)))

    async def test_drill_bootstrap_preserves_native_person_directory_without_other_scope_counts(self):
        @self.app.get("/api/drills/bootstrap")
        async def bootstrap(scope: str):
            return {"ok": True, "data": {"default_scope": scope, "scopes": [{"scope": "A", "pending": 1}, {"scope": "B", "pending": 99}],
                "people": [{"record_id": "person-b", "name": "B楼人员", "building": "B", "image_base64": "private-image"}], "drills": []}}
        async def stream(messages, info):
            results = self.tools(messages)
            if not results:
                yield {0: DeltaToolCall(name="query", json_args=json.dumps({"operation": {"api_id": "GET /api/drills/bootstrap", "params": {"scope": "A"}}}))}
            else:
                self.assertEqual(results[0]["data"]["scopes"], [{"scope": "A", "pending": 1}])
                self.assertEqual(results[0]["data"]["people"][0]["name"], "B楼人员")
                self.assertNotIn("private-image", json.dumps(results[0]))
                yield "A楼有1项演练，人员目录已读取。"
        result = await self.answer(stream, "读取A楼演练和签名人员")
        self.assertEqual(len(result["sources"]), 1)

    async def test_learning_domain_returns_only_original_page_link_no_native_calls(self):
        reads = []
        @self.app.get("/api/learning/papers")
        async def papers(scope: str, today: str = "0"):
            reads.append(("papers", scope, today))
            return {"ok": True, "data": {"items": [{"id": "paper-d-1001", "scope": scope}], "total": 1}}
        @self.app.post("/api/learning/papers/{id}/answer")
        async def answer(id: str, body: dict):
            reads.append(("answer", id))
            return {"ok": True, "data": {"id": id}}
        stream_calls = []
        async def stream(messages, info):
            results = self.tools(messages)
            if not results:
                stream_calls.append("query")
                yield {0: DeltaToolCall(name="query", json_args=json.dumps({"operation": {
                    "api_id": "GET /api/learning/papers", "params": {"scope": "A", "today": "0"}}}))}
            else:
                self.assertEqual(len(results), 1)
                self.assertTrue(results[0]["ok"], results[0])
                items = (results[0].get("data") or {}).get("items", [])
                self.assertTrue(any(item.get("id") == "paper-d-1001" for item in items))
                yield "查看A楼今日的题单（paper-d-1001），画像学练见 /learning 学习页。"
        result = await self.answer(stream, "查看A楼今日的题单和学习进度")
        self.assertEqual(stream_calls, ["query"])
        self.assertEqual(reads, [("papers", "A", "0")])
        self.assertIn("/learning", result["answer"])
        self.assertIn("画像学练", result["answer"])
        self.assertIn("paper-d-1001", result["answer"])

    async def test_learning_native_operations_are_not_discoverable_or_executable(self):
        @self.app.get("/api/learning/papers")
        async def papers(scope: str, today: str = "0"):
            return {"ok": True, "data": {"items": [{"id": "paper-d-1001", "scope": scope}], "total": 1}}
        @self.app.post("/api/learning/papers/{id}/answer")
        async def answer(id: str, body: dict):
            raise AssertionError("学习写入端点在排除后不应被调用")
        self.portal = PortalAgent(self.assistant, PortalAPICatalog(self.app), self.files)
        # Learning GET queries are discoverable under the original account permission.
        self.assertIsInstance(self.portal.catalog.get("GET /api/learning/papers"), dict)
        # Learning write operations (answer) remain excluded and non-discoverable.
        with self.assertRaises(AssistantError):
            self.portal.catalog.get("POST /api/learning/papers/d-1/answer")
        # Learning write operations (answer) stay rejected at prepare time.
        with self.assertRaises(AssistantError):
            self.portal.prepare(ACTOR, {"operations": [{"api_id": "POST /api/learning/papers/d-1/answer", "path_params": {"id": "d-1"}, "body": {"choices": []}}]},
                                "learning_write_fixture00001", [])


if __name__ == "__main__":
    unittest.main()
