"""Module grants guard real HTTP requests and aggregate data before exposure."""
import copy
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent))
import httpx
from fastapi import FastAPI, Request
from lan_bitable_template_portal.module_access import (
    MODULE_IDS, MODULE_OPTIONS, NOTICE_MODULES, filter_overview, filter_workbench, module_denial, required_modules,
)


class ModuleAccessTests(unittest.IsolatedAsyncioTestCase):
    def test_inspection_entry_label_keeps_existing_module_and_route(self):
        self.assertEqual(next(item['label'] for item in MODULE_OPTIONS if item['value'] == 'risk_inspection'), '专项排查')
        for path in ('/risk-inspections', '/api/risk-inspections', '/api/risk-inspections/people'):
            self.assertEqual(required_modules(path), frozenset({'risk_inspection'}))
        frontend = Path(__file__).parent / 'lan_bitable_template_portal' / 'frontend' / 'src'
        self.assertIn("id: 'risk_inspection', label: '专项排查'", (frontend / 'workspace-v2/modules.ts').read_text(encoding='utf-8-sig'))
        self.assertIn('选择楼栋进入专项排查', (frontend / 'scopeHomeUtils.ts').read_text(encoding='utf-8-sig'))
        self.assertIn('risk_inspection: "专项排查"', (frontend / 'App.vue').read_text(encoding='utf-8-sig'))

    async def asyncSetUp(self):
        self.modules = {"maintenance", "assistant"}
        self.called = []
        self.session = {"user": {"open_id": "ou_fixture"}}
        self.runtime = SimpleNamespace(
            auth_manager=SimpleNamespace(account_profile=lambda _: {"modules": sorted(self.modules)}),
            service=SimpleNamespace(get_job=lambda _: {"request": {"work_type": "change"}}),
            state_store=SimpleNamespace(get_notice_undo_action=lambda _: {"work_type": "change"}),
        )
        async def read_body(request, **kwargs):
            return await request.json()
        controller = SimpleNamespace(_current_session=lambda request: self.session, _read_json_request=read_body)
        app = FastAPI()
        @app.middleware("http")
        async def guard(request, next_call):
            denied = await module_denial(request, controller, self.runtime)
            return denied if denied is not None else await next_call(request)
        @app.api_route("/{path:path}", methods=["GET", "POST", "DELETE"])
        async def endpoint(path: str, request: Request):
            self.called.append(path)
            return {"body": await request.json()} if request.method == "POST" else {"ok": True}
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")
        self.addAsyncCleanup(self.client.aclose)

    async def test_module_revocation_denies_existing_session_and_direct_url(self):
        for path in ("/learning", "/api/learning/today", "/api/signatures/management/preview", "/api/assistant/knowledge/status"):
            self.assertEqual((await self.client.get(path)).status_code, 403, path)
        self.assertEqual(self.called, [])
        self.modules.add("learning")
        self.assertEqual((await self.client.get("/api/learning/today")).status_code, 200)
        self.modules.remove("learning")
        self.assertEqual((await self.client.get("/api/learning/today")).status_code, 403)

    async def test_post_body_routes_by_notice_type_and_remains_readable(self):
        payload = {"work_type": "maintenance", "action": "end"}
        response = await self.client.post("/api/workbench-actions", json=payload)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["body"], payload)
        response = await self.client.post("/api/workbench-actions", json={**payload, "work_type": "change"})
        self.assertEqual(response.status_code, 403)
        self.assertEqual(len(self.called), 1)

    async def test_job_and_undo_use_stored_type_instead_of_forged_query(self):
        self.assertEqual((await self.client.get("/api/jobs/fixture?work_type=maintenance")).status_code, 403)
        self.assertEqual((await self.client.post("/api/notice-undo/fixture/apply?work_type=maintenance", json={})).status_code, 403)
        self.assertEqual(self.called, [])

    async def test_guest_retains_only_cabinet(self):
        self.session = {"role": "guest", "user": {"open_id": "guest_fixture"}}
        self.assertEqual((await self.client.get("/api/cabinet-power/overview")).status_code, 200)
        self.assertEqual((await self.client.get("/api/assistant/conversation")).status_code, 403)

    def test_all_public_module_pages_are_mapped(self):
        self.assertEqual({item["value"] for item in MODULE_OPTIONS if item["group"] == "通告办理"}, NOTICE_MODULES)
        self.assertEqual(required_modules('/signature-management'), frozenset())
        paths = {"/api/signatures/management/preview": "signatures", "/knowledge-base": "knowledge",
                 "/admin/settings": "accounts", "/repair-status": "repair_management",
                 "/drill-management/print": "drills", "/cabinet-power/batches": "cabinet",
                 "/life-guide": "life_guide", "/link-directory": "links", "/plan-convergence": "plan_convergence",
                 "/risk-inspections": "risk_inspection", "/api/risk-inspections/people": "risk_inspection"}
        for path, module in paths.items():
            self.assertEqual(required_modules(path), {module})
        self.assertTrue(set(paths.values()) <= MODULE_IDS)

    def test_aggregate_filter_does_not_mutate_shared_cache_or_leak_titles(self):
        payload = {"scopes": {"E": {"maintenance_pending": 2, "change_pending": 7, "event_total": 19,
                   "maintenance_ongoing": 1, "change_ongoing": 3, "closed_today": 8,
                   "ongoing_titles": [{"work_type": "maintenance", "title": "allowed"}, {"work_type": "change", "title": "hidden"}]}}}
        original = copy.deepcopy(payload)
        result = filter_overview(payload, {"maintenance"})
        row = result["scopes"]["E"]
        self.assertEqual(row["ongoing_title_count"], 1)
        self.assertNotIn("event_total", row)
        self.assertNotIn("change_pending", row)
        self.assertNotIn("closed_today", row)
        self.assertEqual([item["title"] for item in row["ongoing_titles"]], ["allowed"])
        self.assertEqual(payload, original)
        data = filter_workbench({"records": original["scopes"]["E"]["ongoing_titles"],
                                 "record_type_counts": {"maintenance": 2, "change": 7}, "zhihang_change_records": ["hidden"]}, {"maintenance"})
        self.assertEqual(len(data["records"]), 1)
        self.assertEqual(data["record_type_counts"], {"maintenance": 2})
        self.assertNotIn("zhihang_change_records", data)

    def test_history_filter_recounts_days(self):
        data = {"days": [{"date": "2026-10-12", "items": [
            {"work_type": "change", "started_at": "today", "status": "开始"},
            {"work_type": "maintenance", "started_at": "today", "status": "开始"}], "stats": {"started": 2}}]}
        result = filter_workbench(data, {"maintenance"})
        self.assertEqual(len(result["days"][0]["items"]), 1)
        self.assertEqual(result["days"][0]["stats"]["started"], 1)
        self.assertEqual(result["days"][0]["stats"]["ongoing"], 1)
        self.assertEqual(len(data["days"][0]["items"]), 2)
