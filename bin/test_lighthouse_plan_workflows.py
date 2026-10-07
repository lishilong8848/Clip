"""Assistant selections exercise native plan routes and isolated SQLite/Excel."""
import asyncio
import copy
import io
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fastapi import FastAPI, Request
from openpyxl import Workbook
from lan_bitable_template_portal import plan_convergence_auth as auth, plan_convergence_points as points, plan_convergence_rules as rules
from lan_bitable_template_portal.lighthouse_agent import PortalAgent, _result_refs
from lan_bitable_template_portal.lighthouse_ai import AssistantError, LighthouseAssistant
from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog
from lan_bitable_template_portal.lighthouse_files import LighthouseFiles
from lan_bitable_template_portal.plan_convergence import PlanConvergenceService
from lan_bitable_template_portal.plan_convergence_routes import install_plan_convergence_routes
from test_lighthouse_stream import Store
from test_plan_convergence import FakeController, FakeRuntime
from test_plan_convergence_points_adapter import _build_databases

ACTOR = {"id": "plan-fixture-owner", "scopes": ["110", *list("ABCDEH")], "is_admin": True}


class PlanWorkflowTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        catalog, point_db = _build_databases(root)
        for module, key, value in ((rules, "RULE_DB", root / "rules.sqlite3"), (rules, "CATALOG_DB", catalog),
            (points, "CATALOG_PATH", catalog), (points, "POINTS_PATH", point_db), (auth, "_store", None)):
            guard = patch.object(module, key, value)
            guard.start()
            self.addCleanup(guard.stop)
        self.store = Store(root / "assistant.sqlite3")
        model = Mock()
        model.settings.return_value = {}
        self.assistant = LighthouseAssistant(self.store, lambda *_: ([], []), model=model)
        self.files = LighthouseFiles(self.store, root=root / "files")
        self.runtime, self.controller = FakeRuntime(), FakeController()
        self.app = FastAPI()
        install_plan_convergence_routes(self.app, self.controller, self.runtime)
        self.agent = PortalAgent(self.assistant, PortalAPICatalog(self.app), self.files)
        self.request = Request({"type": "http", "scheme": "http", "server": ("testserver", 80), "client": ("127.0.0.1", 1),
            "path": "/api/assistant/plans", "root_path": "", "query_string": b"", "headers": [(b"cookie", b"fixture=owner"), (b"origin", b"http://testserver")]})
        self.blocks = [{"blockId": "123", "blockName": "A楼冷机核对", "status": "1", "startTime": "2026-10-02 08:00"},
                       {"blockId": "456", "blockName": "B楼冷机核对", "status": "1", "startTime": "2026-10-02 09:00"}]
        self.details = [{"blockDetailId": "d1", "classifyModel": "冷机", "classifyModelId": "CLS-1", "domainCode": "d1",
            "spaceModel": "设备间", "relateConfig": "高温", "instances": "冷机一号", "instanceIds": "INS-1"}]
        self.remote_calls = []
        def remote(method, path, payload=None, credentials=None, *, deadline=None):
            self.remote_calls.append((method, path, copy.deepcopy(payload)))
            if path.startswith("/getAlarmBlockDetail/"):
                return {"blockId": path.rsplit("/", 1)[1], "blockName": self.blocks[0 if path.endswith("/123") else 1]["blockName"], "alarmBlockDetailResultList": copy.deepcopy(self.details)}
            if path in {"/getBlockInstanceSnapshot", "/getRuleView"}:
                return {"content": [{"insName": "冷机一号", "ruleName": "高温"}], "hasNext": False}
            raise AssertionError("unexpected upstream request: " + path)
        for guard in (patch.object(PlanConvergenceService, "remote", side_effect=remote),
            patch.object(PlanConvergenceService, "blocks", side_effect=lambda refresh=False, **kwargs: {"items": copy.deepcopy(self.blocks)}),
            patch.object(auth, "build_auth", return_value=({}, {}))):
            guard.start()
            self.addCleanup(guard.stop)
        self.set_id = rules.create_set("冷机测试规则集", "原备注")
        rules.save_set(self.set_id, "冷机测试规则集", "原备注", [{"scope_type": "device", "inst_name": "冷机一号", "rule_group_no": 1, "rule_type": "normal"}])

    def prepare(self, operation, *, queries=None, actor=ACTOR):
        return self.agent.prepare(actor, {"operations": [operation]}, "plan-fixture-turn-00001", [], queries=queries)

    async def fill_and_run(self, plan, values, *, actor=ACTOR):
        amended = self.agent.amend(actor, plan["id"], {"version": plan["version"], "values": values})
        reviewed = await self.agent.confirm(actor, plan["id"], {"version": amended["version"], "stage": "review"}, self.request)
        if reviewed["status"] == "awaiting_second_confirmation":
            await self.agent.confirm(actor, plan["id"], {"version": reviewed["version"], "stage": "execute"}, self.request)
        if self.agent.tasks:
            await asyncio.gather(*tuple(self.agent.tasks))
        result = self.agent.get_plan(actor, plan["id"])
        self.assertEqual(result["status"], "completed", result.get("error"))
        return result

    async def test_rule_match_name_pickers_use_native_matching_without_writes(self):
        before = rules.get_set(self.set_id)
        plan = self.prepare({"api_id": "POST /api/plan-convergence/rulesets/{id}/match"})
        self.assertEqual(plan["risk"], "normal")
        self.assertEqual({field["path"] for field in plan["fields"]}, {"id", "block_id"})
        for field in plan["fields"]:
            await self.agent.field_options(ACTOR, plan["id"], field["name"], self.request)
        plan = self.agent.get_plan(ACTOR, plan["id"])
        fields = {field["path"]: field for field in plan["fields"]}
        self.assertEqual(fields["id"]["options"][0]["label"], "冷机测试规则集")
        with self.assertRaises(AssistantError):
            self.agent.amend(ACTOR, plan["id"], {"version": plan["version"], "values": {fields["id"]["name"]: "forged", fields["block_id"]["name"]: "123"}})
        done = await self.fill_and_run(plan, {fields["id"]["name"]: str(self.set_id), fields["block_id"]["name"]: "123"})
        self.assertTrue(done["results"][0]["_raw"]["passed"])
        self.assertIn("核对通过", done["results"][0]["query_reply"])
        self.assertEqual(rules.get_set(self.set_id), before)
        self.assertIn(done["results"][0]["query_ref"], done["_queries"])

    async def test_excel_upload_sheet_selection_preserves_all_rows_and_native_gaps(self):
        workbook, stream = Workbook(), io.BytesIO()
        for number, sheet in enumerate((workbook.active, workbook.create_sheet())):
            sheet.title = "场景" + str(number + 1)
            sheet.append(["设备域", "关联资源", "关联设备", "关联告警规则"])
            for _ in range(121):
                sheet.append(["冷机", "设备间", "冷机一号,冷机二号" if number else "冷机一号", "高温"])
        workbook.save(stream)
        owned = self.files.upload(ACTOR, "场景.xlsx", stream.getvalue(), extract=False)
        parsed = await self.agent._invoke(ACTOR, {"api_id": "POST /api/plan-convergence/excel", "files": {"file": [owned["id"]]}}, self.request, uploads=True)
        self.assertTrue(parsed["ok"], parsed)
        plan = self.prepare({"api_id": "POST /api/plan-convergence/compare"}, queries={"excel": parsed["_raw"]})
        sheet = next(field for field in plan["fields"] if field["path"] == "scenarios")
        block = next(field for field in plan["fields"] if field["path"] == "block_id")
        await self.agent.field_options(ACTOR, plan["id"], block["name"], self.request)
        plan = self.agent.get_plan(ACTOR, plan["id"])
        selected = sheet["options"][1]["value"]
        self.assertEqual(sheet["type"], "select")
        self.assertEqual(sheet["options"][1]["label"], "场景2 · 121 行")
        done = await self.fill_and_run(plan, {block["name"]: "123", sheet["name"]: selected})
        result = done["results"][0]["_raw"]
        self.assertEqual(result["stats"]["expected_row_count"], 121)
        self.assertEqual(result["missing_data_list"][0]["missing_devices"], ["冷机二号"])
        self.assertEqual(result["missing_data_list"][0]["row_numbers"], list(range(2, 123)))
        self.assertIn("存在缺项", done["results"][0]["query_reply"])
        self.assertEqual(_result_refs(done["operations"][0], [], done["_references"], done["_queries"])["body"]["scenarios"][0]["scenario_name"], "场景2")

    async def test_detail_selector_binds_native_identifiers_for_snapshot_and_rule_view(self):
        detail = await self.agent._invoke(ACTOR, {"api_id": "GET /api/plan-convergence/blocks/{id}", "path_params": {"id": "123"}}, self.request)
        for action in ("snapshots", "rule-view"):
            plan = self.prepare({"api_id": "POST /api/plan-convergence/" + action}, queries={"detail": detail["_raw"]})
            field = plan["fields"][0]
            self.assertEqual(field["label"], "屏蔽明细")
            self.assertIn("冷机一号", field["options"][0]["label"])
            done = await self.fill_and_run(plan, {field["name"]: field["options"][0]["value"]})
            request = self.remote_calls[-1][2]
            if action == "snapshots":
                self.assertEqual((request["blockId"], request["blockDetailId"], request["instanceIds"]), ("123", "d1", "INS-1"))
            else:
                self.assertEqual((request["classifyModelId"], request["domainCode"]), ("CLS-1", "d1"))
            self.assertIn("冷机一号", done["results"][0]["query_reply"])

    async def test_scope_filters_candidates_and_checks_without_leaking_other_buildings(self):
        actor = {**ACTOR, "scopes": ["A"], "is_admin": False}
        plan = self.prepare({"api_id": "GET /api/plan-convergence/blocks/{id}"}, actor=actor)
        refreshed = await self.agent.field_options(actor, plan["id"], plan["fields"][0]["name"], self.request)
        self.assertEqual([option["value"] for option in refreshed["fields"][0]["options"]], ["123"])
        with self.assertRaisesRegex(AssistantError, "权限范围之外"):
            await self.agent._invoke(actor, {"api_id": "GET /api/plan-convergence/blocks/{id}", "path_params": {"id": "456"}}, self.request)
        with patch.object(PlanConvergenceService, "maintenance_check", return_value={"records": [
            {"name": "A楼检修", "record_id": "rec-a", "building": "A", "hits": []},
            {"name": "B楼检修", "record_id": "rec-b", "building": "B", "hits": []}],
            "stats": {"maintenance": 2, "blocks": 99, "matched_records": 0}, "orphan_block_ids": ["private-b"]}):
            result = await self.agent._invoke(actor, {"api_id": "POST /api/plan-convergence/maintenance/check"}, self.request)
        self.assertEqual(len(result["_raw"]["records"]), 1)
        self.assertEqual(result["_raw"]["stats"], {"maintenance": 1, "matched_records": 0})
        self.assertNotIn("orphan_block_ids", result["_raw"])

    async def test_shared_ruleset_names_are_not_business_scopes_and_unknown_blocks_are_hidden(self):
        actor = {**ACTOR, "scopes": ["A"], "is_admin": False}
        rules.save_set(self.set_id, "B楼适用的共享规则", "", rules.get_set(self.set_id)["items"])
        plan = self.prepare({"api_id": "POST /api/plan-convergence/rulesets/{id}/match"}, actor=actor)
        field = next(field for field in plan["fields"] if field["path"] == "id")
        refreshed = await self.agent.field_options(actor, plan["id"], field["name"], self.request)
        self.assertEqual(next(field for field in refreshed["fields"] if field["path"] == "id")["options"][0]["label"], "B楼适用的共享规则")
        self.blocks.append({"blockId": "999", "blockName": "未标楼栋的屏蔽", "status": "1"})
        field = next(field for field in plan["fields"] if field["path"] == "block_id")
        refreshed = await self.agent.field_options(actor, plan["id"], field["name"], self.request)
        choices = next(field for field in refreshed["fields"] if field["path"] == "block_id")
        self.assertEqual([option["value"] for option in choices["options"]], ["123"])
        self.assertIn("未标明楼栋", choices["options_warning"])
        for action in ("compare", "snapshots"):
            with self.subTest(action=action), self.assertRaisesRegex(AssistantError, "楼栋无法核实"):
                await self.agent._invoke(actor, {"api_id": "POST /api/plan-convergence/" + action,
                    "body": {"blockId": "999", "block_id": "999"}}, self.request)
        self.assertEqual(self.remote_calls, [])

    async def test_cross_scope_hits_are_hidden_without_failing_authorized_check(self):
        actor = {**ACTOR, "scopes": ["A"], "is_admin": False}
        payload = {"records": [{"name": "A楼检修", "building": "A", "hits": [{"blockName": "B楼屏蔽", "blockId": "456", "reason": "匹配"}]}], "stats": {"maintenance": 1}}
        with patch.object(PlanConvergenceService, "maintenance_check", return_value=payload):
            result = await self.agent._invoke(actor, {"api_id": "POST /api/plan-convergence/maintenance/check"}, self.request)
        self.assertTrue(result["ok"])
        self.assertEqual(result["_raw"]["records"][0]["hits"], [])
        self.assertTrue(result["_raw"]["records"][0]["matches_restricted"])
        self.assertNotIn("B楼屏蔽", str(result))
        from lan_bitable_template_portal.lighthouse_agent import _plan_query_reply
        self.assertIn("受权限限制", _plan_query_reply("POST /api/plan-convergence/maintenance/check", result["_raw"]))
        payload["records"][0]["hits"].append({"blockName": "A楼可查看的屏蔽", "blockId": "123", "reason": "匹配"})
        with patch.object(PlanConvergenceService, "maintenance_check", return_value=payload):
            partial = await self.agent._invoke(actor, {"api_id": "POST /api/plan-convergence/maintenance/check"}, self.request)
        text = _plan_query_reply("POST /api/plan-convergence/maintenance/check", partial["_raw"])
        self.assertIn("A楼可查看的屏蔽", text)
        self.assertIn("部分匹配受权限限制", text)
        self.assertNotIn("B楼屏蔽", text)

    async def test_malformed_check_response_cannot_be_reported_as_empty(self):
        with patch.object(PlanConvergenceService, "maintenance_check", return_value=[]):
            with self.assertRaisesRegex(AssistantError, "不能判断为空"):
                await self.agent._invoke(ACTOR, {"api_id": "POST /api/plan-convergence/maintenance/check"}, self.request)

    def test_rule_writes_require_admin_and_complete_original_details(self):
        operation = {"api_id": "PUT /api/plan-convergence/rulesets/{id}", "path_params": {"id": str(self.set_id)}}
        with self.assertRaisesRegex(AssistantError, "仅管理员"):
            self.prepare(operation, actor={**ACTOR, "is_admin": False})
        with self.assertRaisesRegex(AssistantError, "先读取"):
            self.prepare(operation)
        original = rules.get_set(self.set_id)
        plan = self.prepare(operation, queries={"original": original})
        self.assertEqual(plan["operations"][0]["body"]["items"], original["items"])
        self.assertEqual(plan["operations"][0]["body"]["remark"], "原备注")
        self.assertFalse(any(field["path"] == "id" for field in plan["fields"]))

    def test_excel_and_detail_forms_do_not_guess_missing_sources(self):
        for api_id, queries, message in (("POST /api/plan-convergence/compare", {}, "先上传"),
            ("POST /api/plan-convergence/compare", {"excel": {"sheets": [{"name": "坏表", "headers": [], "rows": [{}]}]}}, "缺少"),
            ("POST /api/plan-convergence/snapshots", {}, "先读取")):
            with self.subTest(api_id=api_id), self.assertRaisesRegex(AssistantError, message):
                self.prepare({"api_id": api_id}, queries=queries)


if __name__ == "__main__":
    unittest.main()
