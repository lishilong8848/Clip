"""Assistant configuration saves use the original drill service in isolation."""
import asyncio
import copy
import json
import sys
import tempfile
import unittest
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic_ai.models.function import FunctionModel, DeltaToolCall
from clipflow_backend.api_models import DrillConfigurationRequest
from lan_bitable_template_portal.drill_management import DrillManagementService, DrillError
from lan_bitable_template_portal.lighthouse_agent import PortalAgent, _set_path, _result_refs
from lan_bitable_template_portal.lighthouse_ai import AssistantError, LighthouseAssistant
from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog
from lan_bitable_template_portal.lighthouse_files import LighthouseFiles
from lan_bitable_template_portal.lighthouse_model import LighthouseModel, read_scope_operations
from test_drill_management import _fixture_xlsx
from test_lighthouse_stream import Store

ACTOR = {"id": "drill-config-admin", "scopes": list("ABCDE"), "is_admin": True}
API = "PUT /api/drills/{drill_id}/configuration"


class DrillConfigurationWorkflowTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.store = Store(root / "state.sqlite3")
        self.service = DrillManagementService(self.store, data_root=root / "drills")
        item = self.service.create_definition(name="模板演练", year=2026, month=10, file_name="fixture.xlsx", source=_fixture_xlsx(with_evaluator=True))
        self.definition = self.service.get_definition(item["drill_id"])
        model = Mock()
        model.settings.return_value = {"configured": True, "enabled": True}
        self.assistant = LighthouseAssistant(self.store, lambda *_: ([], []), model=model)
        self.files = LighthouseFiles(self.store)
        self.app, self.writes, self.reads = FastAPI(), [], []

        @self.app.get("/api/drills")
        async def drills(request: Request):
            self.reads.append(dict(request.query_params))
            rows = self.service.list_definitions(request.query_params.get("month", ""))
            if request.query_params.get("scope"):
                rows = [row for row in rows if row["status"] == "published"]
            return {"ok": True, "data": {"items": rows, "scope": request.query_params.get("scope", "")}}

        @self.app.put("/api/drills/{drill_id}/configuration")
        async def configuration(drill_id: str, body: DrillConfigurationRequest):
            self.writes.append(body.model_dump())
            try:
                data = self.service.save_configuration(drill_id, body.configuration, expected_version=body.expected_version, actor="隔离测试")
            except DrillError as exc:
                return JSONResponse({"ok": False, "error": str(exc)}, status_code=409)
            return {"ok": True, "data": data}

        self.portal = PortalAgent(self.assistant, PortalAPICatalog(self.app), self.files)
        self.request = Request({"type": "http", "scheme": "http", "server": ("testserver", 80), "client": ("127.0.0.1", 1),
            "path": "/api/assistant/messages", "root_path": "", "query_string": b"", "headers": [(b"origin", b"http://testserver")]})

    def prepare(self, *, actor=None, body=None, definition=None, queries=None):
        definition = definition or self.definition
        operation = {"api_id": API, "path_params": {"drill_id": definition["drill_id"]}, "body": body or {}}
        return self.portal.prepare(actor or ACTOR, {"title": "修改演练模板", "operations": [operation]}, "drill_config_fixture", [],
            queries=queries if queries is not None else {"query_snapshot": {"items": [definition]}})

    def values(self, plan, changes=None):
        field = plan["fields"][0]
        values = copy.deepcopy(self.portal.public_plan(plan)["fields"][0]["value"])
        for native_path, value in (changes or {}).items():
            alias = next(alias for alias, path in field["_mapping_paths"].items() if ".".join(path) == native_path)
            _set_path(values, alias, value)
        return {field["name"]: values}

    async def execute(self, plan, changes=None):
        plan = self.portal.amend(ACTOR, plan["id"], {"version": plan["version"], "values": self.values(plan, changes)})
        plan = await self.portal.confirm(ACTOR, plan["id"], {"version": plan["version"], "stage": "review"}, self.request)
        if plan["status"] == "awaiting_second_confirmation":
            plan = await self.portal.confirm(ACTOR, plan["id"], {"version": plan["version"], "stage": "execute"}, self.request)
        if self.portal.tasks:
            await asyncio.gather(*tuple(self.portal.tasks))
        return self.portal.get_plan(ACTOR, plan["id"])

    async def test_native_save_keeps_template_fields_and_is_not_a_publish(self):
        before = copy.deepcopy(self.definition["configuration"])
        plan = self.prepare()
        public = self.portal.public_plan(plan)
        self.assertEqual(plan["status"], "needs_input")
        self.assertEqual(len(public["fields"]), 1)
        self.assertTrue(public["fields"][0]["native_drill_configuration"])
        self.assertNotIn(self.definition["source"]["path"], json.dumps(public))
        self.assertNotIn('"_configuration"', json.dumps(public))
        self.assertEqual(self.writes, [])
        result = await self.execute(plan, {"steps.0.signature_slots": 3, "mapping.review_time": "G18:H18"})
        self.assertEqual(result["status"], "completed", result.get("error"))
        saved = self.service.get_definition(self.definition["drill_id"])
        self.assertEqual(saved["version"], 2)
        self.assertEqual(saved["status"], "draft")
        self.assertEqual(saved["configuration"]["steps"][0]["signature_slots"], 3)
        self.assertEqual(saved["configuration"]["mapping"]["review_time"], "G18:H18")
        self.assertEqual(saved["source"], self.definition["source"])
        for key in ("content", "location", "duration_text", "row"):
            self.assertEqual(saved["configuration"]["steps"][0][key], before["steps"][0][key])
        self.assertEqual(saved["configuration"]["mapping"]["participant_signatures"], before["mapping"]["participant_signatures"])
        self.assertEqual(saved["configuration"]["mapping"]["assessment"], before["mapping"]["assessment"])
        again = await self.portal.confirm(ACTOR, result["id"], {"version": result["version"], "stage": "execute"}, self.request)
        self.assertEqual(again["status"], "completed")
        self.assertEqual(len(self.writes), 1)

    async def test_model_patch_is_visible_but_cannot_change_readonly_steps(self):
        config = copy.deepcopy(self.definition["configuration"])
        config["steps"][0]["signature_slots"] = 4
        plan = self.prepare(body={"configuration": config})
        raw = _result_refs(plan["fields"][0]["value"], [], queries=plan["_queries"])
        self.assertEqual(raw["signers"]["step_0"], 4)
        result = await self.execute(plan)
        self.assertEqual(result["status"], "completed", result.get("error"))
        config["steps"][0]["content"] = "改变原步骤"
        with self.assertRaisesRegex(AssistantError, "只读"):
            self.prepare(body={"configuration": config})

    async def test_rejects_missing_snapshot_lock_scope_and_nonadmin(self):
        cases = [({}, {"queries": {}}, "完整配置"),
                 ({"configuration_locked": True}, {}, "已有楼栋执行"),
                 ({"has_executions": True}, {}, "已有楼栋执行"),
                 ({"version": None}, {}, "完整配置"),
                 ({}, {"actor": {**ACTOR, "is_admin": False}}, "管理员"),
                 ({}, {"actor": {**ACTOR, "scopes": ["A"]}}, "全部分配楼栋"),
                 ({}, {"body": {"expected_version": 0}}, "版本已变化")]
        for patch, kwargs, message in cases:
            with self.subTest(patch=patch, kwargs=kwargs), self.assertRaisesRegex(AssistantError, message):
                self.prepare(definition={**self.definition, **patch}, **kwargs)
        self.assertEqual(self.writes, [])

    async def test_invalid_edits_leave_original_untouched(self):
        for changed in ({"steps.0.signature_slots": 11}, {"steps.0.signature_slots": 1.5}, {"steps.0.signature_slots": False},
                        {"record_sheet": "陌生工作表"}, {"mapping.reviewer_signature": "arbitrary-file-path"}):
            plan = self.prepare()
            with self.subTest(changed=changed), self.assertRaises(AssistantError):
                self.portal.amend(ACTOR, plan["id"], {"version": plan["version"], "values": self.values(plan, changed)})
        self.assertEqual(self.writes, [])
        self.assertEqual(self.service.get_definition(self.definition["drill_id"]), self.definition)

    async def test_native_validation_and_late_execution_lock_remain_authoritative(self):
        plan = self.prepare()
        failed = await self.execute(plan, {"mapping.assessment.score_rows.0.score": 39})
        self.assertEqual(failed["status"], "failed")
        self.assertIn("100", failed["error"])
        self.assertEqual(self.service.get_definition(self.definition["drill_id"])["version"], 1)
        self.service.publish(self.definition["drill_id"], expected_version=1)
        self.definition = self.service.get_definition(self.definition["drill_id"])
        plan = self.prepare()
        self.service.save_execution(self.definition["drill_id"], "A", {}, expected_version=0)
        failed = await self.execute(plan, {"steps.0.signature_slots": 2})
        self.assertEqual(failed["status"], "failed")
        self.assertIn("执行数据", failed["error"])
        self.assertEqual(self.service.get_definition(self.definition["drill_id"])["version"], 2)

    async def test_stale_version_does_not_overwrite_newer_configuration(self):
        plan = self.prepare()
        config = copy.deepcopy(self.definition["configuration"])
        config["steps"][0]["signature_slots"] = 4
        self.service.save_configuration(self.definition["drill_id"], config, expected_version=1)
        failed = await self.execute(plan, {"steps.0.signature_slots": 2})
        self.assertEqual(failed["status"], "failed")
        current = self.service.get_definition(self.definition["drill_id"])
        self.assertEqual(current["configuration"]["steps"][0]["signature_slots"], 4)
        self.assertEqual(current["version"], 2)

    async def test_admin_unscoped_listing_includes_drafts_and_opens_form(self):
        operation = {"api_id": "GET /api/drills", "params": {"month": "2026-10"}}
        descriptor = self.portal.catalog.get(operation["api_id"])
        admin_ops = read_scope_operations(operation, descriptor, ACTOR)
        self.assertEqual(len(admin_ops), 1)
        self.assertNotIn("scope", admin_ops[0]["params"])
        self.assertEqual(read_scope_operations(operation, descriptor, {**ACTOR, "is_admin": False, "scopes": ["A"]})[0]["params"]["scope"], "A")
        self.assertEqual(read_scope_operations({**operation, "params": {"scope": "D"}}, descriptor, ACTOR)[0]["params"]["scope"], "D")

        async def stream(messages, info):
            returns = [part.content for message in messages for part in message.parts if getattr(part, "part_kind", "") == "tool-return"]
            if not returns:
                yield {0: DeltaToolCall(name="query", json_args=json.dumps({"operation": {**operation, "params": {"scope": "A", "month": "2026-10"}}}))}
            elif len(returns) == 1:
                self.assertFalse(returns[-1]["ok"])
                self.assertIn("不传scope", returns[-1]["error"])
                self.assertEqual(self.reads, [])
                yield {0: DeltaToolCall(name="query", json_args=json.dumps({"operation": operation}))}
            else:
                if len(returns) == 2:
                    self.assertTrue(returns[-1]["ok"], returns[-1])
                    edit = returns[-1]["editable_form"]["operation"]
                    self.assertEqual(edit["api_id"], API)
                    edit = {**edit, "path_params": {"drill_id": "mistyped-target"}}
                else:
                    self.assertFalse(returns[-1]["ok"])
                    self.assertFalse(returns[-1]["business_written"])
                    edit = returns[-1]["editable_forms"][0]
                    self.assertEqual(edit["path_params"]["drill_id"], self.definition["drill_id"])
                yield {0: DeltaToolCall(name="prepare_business", json_args=json.dumps({"title": "模板配置", "operations": [edit]}))}

        @asynccontextmanager
        async def factory(*_):
            yield FunctionModel(stream_function=stream)
        async def emit(*_): pass
        async def authorize(): return copy.deepcopy(ACTOR)
        engine = LighthouseModel(self.portal, model_factory=factory)
        result = await engine.answer(ACTOR, {"question": "修改模板演练的模板配置和步骤签名人数", "operation_id": "drill_configuration_model",
            "file_ids": [], "_profile": {"id": "default", "name": "fixture", "model": "fixture"}}, [], self.request, emit, authorize, {})
        self.assertEqual(result["plan"]["status"], "needs_input", result)
        self.assertTrue(result["plan"]["fields"][0]["native_drill_configuration"])
        self.assertEqual(self.reads, [{"month": "2026-10"}])
        self.assertEqual(self.writes, [])

    async def test_single_unrelated_template_does_not_become_the_edit_target(self):
        async def stream(messages, info):
            returns = [part.content for message in messages for part in message.parts if getattr(part, "part_kind", "") == "tool-return"]
            if not returns:
                yield {0: DeltaToolCall(name="query", json_args=json.dumps({"operation": {"api_id": "GET /api/drills", "params": {"month": "2026-10"}}}))}
            else:
                self.assertTrue(returns[-1]["ok"], returns[-1])
                self.assertNotIn("editable_form", returns[-1])
                yield "未找到指定的配电柜故障演练，目前只有模板演练。你要修改哪一份？"

        @asynccontextmanager
        async def factory(*_):
            yield FunctionModel(stream_function=stream)
        async def emit(*_): pass
        async def authorize(): return copy.deepcopy(ACTOR)
        result = await LighthouseModel(self.portal, model_factory=factory).answer(ACTOR, {
            "question": "修改配电柜故障演练的模板配置", "operation_id": "drill_config_wrong_target",
            "file_ids": [], "_profile": {"id": "default", "name": "fixture", "model": "fixture"}}, [], self.request, emit, authorize, {})
        self.assertNotIn("plan", result)
        self.assertIn("未找到", result["answer"])
        self.assertEqual(self.writes, [])


if __name__ == "__main__":
    unittest.main()
