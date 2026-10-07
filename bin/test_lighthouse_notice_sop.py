"""Notice preparation uses native SOP contracts, never live Feishu or work orders."""
import copy
import json
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.parse import urlencode

from . import test_lighthouse_notice_workflows as fixture
from lan_bitable_template_portal.lighthouse_ai import AssistantError
from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog
from lan_bitable_template_portal.polling_work_orders import PollingWorkOrderService
from lan_bitable_template_portal.portal_service import PortalConflictError
from lan_bitable_template_portal.state_store import LanPortalStateStore


class NoticeSopTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.fx = fixture.NoticeWorkflowTests()
        self.fx.setUp()
        for callback, args, kwargs in self.fx._cleanups:
            self.addCleanup(callback, *args, **kwargs)
        self.fx._cleanups.clear()
        self.agent = self.fx.agent
        self.root = Path(self.fx.tmp.name)
        with patch("lan_bitable_template_portal.polling_work_orders.get_data_file_path", side_effect=lambda name: str(self.root / name)):
            self.service = PollingWorkOrderService(LanPortalStateStore(self.root / "sops.sqlite3"))
        self.sops = {}
        for work, content in (("maintenance", "检查供水温度"), ("polling", "{{from}}切换至{{to}}"), ("adjust", "{{from}}切换运行模式")):
            sop = self.service.save_sop({"scope": "A", "work_type": work, "name": work + " SOP", "steps": [{"content": content, "operator_required": True, "reviewer_required": True}]})
            self.sops[work] = self.service.add_sop_attachment(sop["sop_id"], file_name="fixture.txt", content=b"isolated", expected_version=sop["version"])
        self.people = [{"record_id": "person-one", "name": "测试操作人", "open_id": "private-one"},
                       {"record_id": "person-two", "name": "测试审核人", "open_id": "private-two"}]
        self.reads = []
        app = self.agent.catalog.app

        @app.get("/api/polling-sops")
        async def sops(scope: str, work_type: str):
            self.assertEqual(scope, "A")
            self.reads.append((scope, work_type))
            return {"ok": True, "data": {"items": [self.sops[work_type]]}}

        @app.get("/api/signatures/people")
        async def people(q: str = "", limit: int = 200):
            self.assertEqual(limit, 200)
            return {"ok": True, "data": {"people": [row for row in self.people if q in row["name"]], "count": len(self.people)}}

        self.agent.catalog = PortalAPICatalog(app)

    def prepare(self, work="maintenance", **patch_values):
        decision = self.fx._start_decision(patch={**fixture.VALID_STARTS[work], **patch_values}, work_type=work, extra={"manual_binding_choice": "unbound"})
        decision["operations"][0]["body"].pop("polling_work_order_exempt")
        return self.agent.prepare(fixture.ACTOR, decision, "sop-selection-" + work, [])

    async def load(self, plan, scope="A", q=""):
        request = fixture.Request({**self.fx.request.scope, "query_string": urlencode({"scope": scope, "q": q}).encode()})
        return await self.agent.field_options(fixture.ACTOR, plan["id"], "step0.notice_sop", request)

    def select(self, plan, work="maintenance", **changes):
        field = next(field for field in plan["fields"] if field.get("native_notice_sop"))
        value = {**field["value"], "exempt": False, "scope": "A", "sop_id": self.sops[work]["sop_id"],
                 "operator_record_id": "person-one", "reviewer_record_id": "person-two", "runs": [], **changes}
        return self.agent.amend(fixture.ACTOR, plan["id"], {"version": plan["version"], "values": {field["name"]: value}})

    async def test_three_selections_are_accepted_by_native_start_without_cloud(self):
        for work, runs in (("maintenance", []), ("polling", [{"from_unit": "1#", "to_unit": "3#"}, {"from_unit": "4#", "to_unit": "6#"}]),
                           ("adjust", [{"from_unit": "2#", "to_unit": "4#", "other_unit": "6#"}])):
            with self.subTest(work=work):
                plan = await self.load(self.prepare(work))
                text = json.dumps(plan)
                self.assertNotIn("private-one", text)
                self.assertNotIn("_people", text)
                self.assertNotIn(str(self.root), text)
                control = next(field for field in plan["fields"] if field.get("native_notice_sop"))
                self.assertEqual(control["sops"][0]["documents"], [{"name": "fixture.txt"}])
                amended = self.select(plan, work, runs=runs)
                self.assertEqual(amended["status"], "awaiting_confirmation")
                body = self.agent.get_plan(fixture.ACTOR, plan["id"])["operations"][0]["body"]
                self.assertEqual(body["patch"]["polling_sop_version"], self.sops[work]["version"])
                self.assertEqual(body["patch"]["polling_runs"], runs if runs else [{"label": "维保作业"}])
                native = self.service.prepare_start({**body, **body["patch"], "_web_action_request": True}, job_id="isolated-" + work, people=self.people)
                self.assertTrue(native)
                self.assertEqual(self.fx.notice_writes, [])

    async def test_requires_explicit_selection_or_exemption_and_clears_stale_ids(self):
        plan = self.prepare(polling_sop_id="stale-sop", polling_operator_record_id="stale-person")
        with self.assertRaises(AssistantError):
            self.agent.amend(fixture.ACTOR, plan["id"], {"version": plan["version"], "values": {}})
        field = next(field for field in plan["fields"] if field.get("native_notice_sop"))
        self.assertFalse(field["value"]["exempt"])
        result = self.agent.amend(fixture.ACTOR, plan["id"], {"version": plan["version"], "values": {field["name"]: {**field["value"], "exempt": True}}})
        saved = self.agent.get_plan(fixture.ACTOR, result["id"])["operations"][0]["body"]["patch"]
        self.assertTrue(saved["polling_work_order_exempt"])
        self.assertEqual((saved["polling_sop_id"], saved["polling_operator_record_id"], saved["polling_runs"]), ("", "", []))

    async def test_return_to_edit_keeps_selection_and_stable_operation(self):
        plan = await self.load(self.prepare("adjust"))
        selected = self.select(plan, "adjust", runs=[{"from_unit": "1#", "to_unit": "4#", "other_unit": "6#"}])
        original = copy.deepcopy(self.agent.get_plan(fixture.ACTOR, plan["id"])["operations"])
        editing = self.agent.amend(fixture.ACTOR, plan["id"], {"version": selected["version"], "action": "edit"})
        field = next(field for field in editing["fields"] if field.get("native_notice_sop"))
        self.assertEqual(field["value"]["runs"][0]["other_unit"], "6#")
        self.assertEqual(field["value"]["operator_record_id"], "person-one")
        self.agent.amend(fixture.ACTOR, plan["id"], {"version": editing["version"], "values": {}})
        self.assertEqual(self.agent.get_plan(fixture.ACTOR, plan["id"])["operations"], original)
        self.assertEqual(self.fx.notice_writes, [])

    async def test_wrong_scope_unknown_person_and_same_person_are_blocked(self):
        plan = await self.load(self.prepare())
        for changes in ({"scope": "B"}, {"sop_id": "forged"}, {"operator_record_id": "forged"},
                        {"operator_record_id": "h_duty_account"}, {"reviewer_record_id": "person-one"}):
            with self.subTest(changes=changes), self.assertRaises(AssistantError):
                self.select(plan, **changes)
        with self.assertRaises(AssistantError):
            await self.load(plan, scope="B")
        self.assertEqual(self.reads, [("A", "maintenance")])
        self.people[1]["open_id"] = self.people[0]["open_id"]
        plan = await self.load(plan)
        with self.assertRaisesRegex(AssistantError, "同一人"):
            self.select(plan)
        self.select(plan, reviewer_record_id="h_duty_account")
        self.assertEqual(self.fx.notice_writes, [])

    async def test_run_rules_and_cooling_field_mapping(self):
        plan = await self.load(self.prepare("polling"))
        for runs in ([], [{"from_unit": "1#", "to_unit": "4#"}], [{"from_unit": "1#", "to_unit": "1#"}],
                     [{"from_unit": "1#", "to_unit": "2#"}, {"from_unit": "2#", "to_unit": "3#"}]):
            with self.subTest(runs=runs), self.assertRaises(AssistantError):
                self.select(plan, "polling", runs=runs)
        plan = await self.load(self.prepare("adjust"))
        for runs in ([{"from_unit": "1", "to_unit": "2", "other_unit": "6#"}],
                     [{"from_unit": "2#", "to_unit": "2#", "other_unit": "6#"}],
                     [{"from_unit": "6#", "to_unit": "2#", "other_unit": "4#"}]):
            with self.subTest(runs=runs), self.assertRaises(AssistantError):
                self.select(plan, "adjust", runs=runs)
        self.assertEqual(self.fx.notice_writes, [])

    async def test_directory_refresh_preserves_people_but_never_crosses_version(self):
        plan = await self.load(self.prepare())
        plan = await self.load(plan, q="审核")
        field = next(field for field in plan["fields"] if field.get("native_notice_sop"))
        self.assertEqual({person["record_id"] for person in field["people"]}, {"person-one", "person-two", "h_duty_account"})
        original = self.agent._invoke
        async def concurrent(*args, **kwargs):
            result = await original(*args, **kwargs)
            saved = self.agent.get_plan(fixture.ACTOR, plan["id"])
            saved["version"] += 1
            self.agent._save_plan(fixture.ACTOR, saved)
            return result
        with patch.object(self.agent, "_invoke", side_effect=concurrent), self.assertRaisesRegex(AssistantError, "填写已变化"):
            await self.load(plan)

    async def test_stale_cloud_sop_and_version_stay_blocked(self):
        self.sops["maintenance"].update(cloud_sync_required=True, cloud_sync_status="pending")
        plan = await self.load(self.prepare())
        with self.assertRaisesRegex(AssistantError, "同步"):
            self.select(plan)
        self.sops["maintenance"].update(cloud_sync_required=False)
        plan = await self.load(plan)
        amended = self.select(plan)
        body = self.agent.get_plan(fixture.ACTOR, amended["id"])["operations"][0]["body"]
        current = self.sops["maintenance"]
        self.service.save_sop({**current, "expected_version": current["version"], "name": "后来修改的SOP"})
        with self.assertRaises(PortalConflictError):
            self.service.prepare_start({**body, **body["patch"], "_web_action_request": True}, job_id="stale", people=self.people)


if __name__ == "__main__":
    unittest.main()
