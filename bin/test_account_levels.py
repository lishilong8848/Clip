"""Unified HR hierarchy, action boundaries and API bypass tests (isolated data)."""
import asyncio
import copy
from types import SimpleNamespace
from unittest.mock import patch, Mock

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .test_personnel_password_login import _TempDirTestCase, PersonnelLoginHarness, _page, _user
from lan_bitable_template_portal.account_management import AccountManagement, install_account_management
from lan_bitable_template_portal.account_policy import MODULE_IDS, account_allowed, business_delete_denial
from lan_bitable_template_portal.personnel_password_login import PasswordLoginError
from lan_bitable_template_portal.portal_auth import BUILDING_OPEN_ID_MAP, LI_SHILONG_OPEN_ID
from lan_bitable_template_portal.portal_service import PortalError


class AccountLevelTests(_TempDirTestCase):
    def setUp(self):
        super().setUp()
        self.h = PersonnelLoginHarness(self.root)
        self.addCleanup(self.h.close)
        self.h.auth._account_directory_provider = lambda: self.h.login.snapshot
        rows = []
        for rid, home, job in (("HM1", "H", "运维经理"), ("HM2", "H", "运维经理"),
                               ("HE1", "H", "电气专业工程师"), ("HE2", "H", "弱电专业工程师"),
                               ("HS", "H", "运维主管"), ("AS", "A", "运维主管"), ("BS", "B", "运维主管"),
                               ("AE", "A", "暖通工程师"), ("AO", "A", "值班员"), ("BE", "B", "电气工程师")):
            fields = {"姓名": "测试" + rid, "员工姓名": _user("测试" + rid, "ou_fixture" + rid),
                      "机楼/专业": [home + "楼"], "体系岗位": job, "岗位": "其他", "账号性质": "外部账号"}
            self.h.service._records["rec" + rid] = fields
            rows.append({"record_id": "rec" + rid, "fields": fields})
        self.h.service._pages = [_page(rows)]
        self.h.login.directory(force=True)
        self.manager = AccountManagement(self.h.login)

    def actor(self, rid):
        return {"open_id": "ou_fixture" + rid, "name": "测试" + rid}

    def item(self, rid):
        return next(item for item in self.manager.accounts()["items"] if item["id"] == "rec" + rid)

    def change(self, rid, **patch):
        item = self.item(rid)
        return {**{key: item[key] for key in ("id", "revision", "role", "scopes", "enabled")}, **patch}

    def test_levels_and_latest_scope_rules_ignore_old_cross_building_admin_grants(self):
        auth = self.h.auth
        self.assertEqual(auth.account_profile(LI_SHILONG_OPEN_ID)["level"], "L0")
        root = auth.account_profile(LI_SHILONG_OPEN_ID)
        self.assertEqual(set(root["modules"]), MODULE_IDS)
        self.assertEqual(root["scope_rules"], auth._admin_scopes())
        self.assertTrue(root["can_manage_accounts"])
        self.assertTrue(root["can_delete_business"])
        for rid, level, scopes in (("HM1", "L1", auth._admin_scopes()), ("HM2", "L1", auth._admin_scopes()),
                                   ("HE1", "L2", ["H"]), ("AS", "L3", ["A"]), ("AE", "L4", ["A"])):
            oid = self.actor(rid)["open_id"]
            self.assertEqual(auth.account_profile(oid)["level"], level)
            self.assertEqual(auth.scopes_for_open_id(oid), scopes)
        previous = auth._load_permissions_locked()
        previous["users"]["ou_fixtureAE"].update(role="admin", scopes=auth._admin_scopes())
        auth._save_permissions_locked(previous)
        self.assertFalse(auth.is_admin({"user": self.actor("AE"), "role": "admin"}))
        self.assertEqual(auth.session_scopes({"user": self.actor("AE"), "allowed_scopes": ["ALL"]}), ["A"])
        for home, oid in BUILDING_OPEN_ID_MAP.items():
            self.assertEqual(auth.scopes_for_open_id(oid), auth._admin_scopes() if home == "H" else [home])
            self.assertEqual(auth.account_profile(oid)["can_manage_accounts"], home == "H")
            self.assertEqual(auth.account_profile(oid)["can_delete_business"], home == "H")
            self.assertEqual(auth.account_profile(oid)["level"], "L1" if home == "H" else "L4")

    def test_h_engineer_edit_selector_keeps_peers_and_lower_view_only_candidates_separate(self):
        actor = self.actor("HE1")
        listing = self.manager.accounts(actor=actor)
        self.assertEqual({item["category"] for item in listing["items"]}, {"engineer", "supervisor"})
        for item in listing["items"]:
            self.assertEqual(item["can_edit"], item['id'] == 'recHS' or item['level'] == 'L4' and item['category'] == 'engineer')
            self.assertEqual(item["can_delete"], item['id'] == 'recHS')
            self.assertNotIn("last_login", item)
            self.assertNotIn("employee_no", item)
            self.assertEqual(account_allowed(listing["policy"], item, "view"), item['is_self'] or item['id'] == 'recHS')
        self.manager.update({"changes": [self.change("AE", enabled=False)]}, actor)
        self.assertTrue(self.h.auth._open_id_explicitly_disabled("ou_fixtureAE"))
        for rid in ("AO", "AS", "HM1"):
            with self.assertRaises(PasswordLoginError):
                self.manager.update({"changes": [self.change(rid, enabled=False)]}, actor)
        with self.assertRaises(PasswordLoginError):
            self.manager.delete("recBE", {"revision": self.item("BE")["revision"]}, actor)

    def test_admin_recipients_exclude_departed_or_disabled_h_managers(self):
        auth = self.h.auth
        departed_id = self.actor("HM1")["open_id"]
        disabled_id = self.actor("HM2")["open_id"]
        self.assertIn(departed_id, auth.admin_open_ids())
        self.assertIn(disabled_id, auth.admin_open_ids())
        self.h.login.snapshot["people"]["recHM1"].update(
            inactive=True, selectable=False, disabled_reason="已离职")
        payload = auth._load_permissions_locked()
        payload["users"][disabled_id]["enabled"] = False
        auth._save_permissions_locked(payload)
        recipients = auth.admin_open_ids()
        self.assertNotIn(departed_id, recipients)
        self.assertNotIn(disabled_id, recipients)
        self.assertIn(LI_SHILONG_OPEN_ID, recipients)
        self.assertIn(BUILDING_OPEN_ID_MAP["H"], recipients)

    def test_supervisor_building_isolation_and_atomic_mixed_batch(self):
        listing = self.manager.accounts(actor=self.actor("AS"))
        self.assertEqual({tuple(item["own_scopes"]) for item in listing["items"]}, {("A",)})
        self.assertEqual({item["category"] for item in listing["items"] if not item['is_self']}, {"engineer", "operator", "duty"})
        before = self.h.auth._load_permissions_locked()
        with self.assertRaises(PasswordLoginError):
            self.manager.update({"changes": [self.change("AE", enabled=False), self.change("BE", enabled=False)]}, self.actor("AS"))
        self.assertEqual(self.h.auth._load_permissions_locked(), before)
        self.assertTrue(self.h.auth._state_store.list_business_operation_audits(domain="accounts", limit=10))
        with self.assertRaises(PasswordLoginError):
            self.manager.password("recBE", {"new_password": "FixturePassword123", "confirm_password": "FixturePassword123"}, self.actor("AS"))
        self.assertFalse(self.h.auth.scope_allowed({"user": self.actor("AS"), "allowed_scopes": ["ALL"]}, "B"))

    def test_delete_preserves_history_and_cannot_delete_fixed_duty(self):
        self.h.auth._state_store.put_document("fixture_learning", "keep", {"score": 15})
        item = self.item("AE")
        self.manager.delete(item["id"], {"revision": item["revision"]}, self.actor("AS"))
        self.h.auth.sync_directory_permissions(self.h.login.snapshot["people"].values())
        self.assertTrue(self.h.auth._open_id_explicitly_disabled(item["open_id"]))
        self.assertEqual(self.h.auth._state_store.get_document("fixture_learning", "keep"), {"score": 15})
        duty = next(item for item in self.manager.accounts()["items"] if item["kind"] == "duty" and item["own_scopes"] == ["A"])
        for actor in (self.actor("AS"), {"open_id": LI_SHILONG_OPEN_ID}):
            with self.assertRaises(PasswordLoginError):
                self.manager.delete(duty["id"], {"revision": duty["revision"]}, actor)
        with self.assertRaises(Exception):
            self.h.auth.remove_permission_user(duty["open_id"], updated_by=LI_SHILONG_OPEN_ID)

    def test_identity_conflict_departure_missing_home_and_disable_fail_closed(self):
        for rid in ("HM1", "HE1", "AS"):
            person = self.h.login.snapshot["people"]["rec" + rid]
            old = copy.deepcopy(person)
            person.update(selectable=False, disabled_reason="多人使用同一OpenID")
            self.h.auth._account_index_cache = (None, {})
            self.assertEqual(self.h.auth.scopes_for_open_id(self.actor(rid)["open_id"]), [])
            self.assertFalse(self.h.auth.account_profile(self.actor(rid)["open_id"])["can_manage_accounts"])
            person.update(old)
        person = self.h.login.snapshot["people"]["recAS"]
        person["scopes"] = ["A", "B"]
        self.h.auth._account_index_cache = (None, {})
        self.assertEqual(self.h.auth.account_profile("ou_fixtureAS")["level"], "L4")
        self.assertEqual(self.h.auth.scopes_for_open_id("ou_fixtureAS"), [])

    def test_no_role_escalation_and_l4_cannot_edit_others(self):
        root = {"open_id": LI_SHILONG_OPEN_ID}
        for actor in (self.actor("AS"), self.actor("HE1"), root):
            with self.assertRaises(PasswordLoginError):
                self.manager.update({"changes": [self.change("AE", role="admin", scopes=["ALL"])]}, actor)
        with self.assertRaises(PasswordLoginError):
            self.manager.update({"changes": [self.change("AO", enabled=False)]}, self.actor("AE"))

    def test_name_alone_never_grants_superuser_and_changed_target_is_rechecked(self):
        self.assertFalse(self.h.auth.is_admin({"user": {**self.actor("AO"), "name": "李世龙"}, "role": "admin"}))
        stale = self.item("AE")
        self.h.login.snapshot["people"]["recAE"]["scopes"] = ["B"]
        with self.assertRaises(PasswordLoginError):
            self.manager.check(self.actor("AS"), stale, "edit")

    def test_scope_options_do_not_log_normal_filtering_as_unauthorized_access(self):
        session = {"user": self.actor("AO")}
        with patch("lan_bitable_template_portal.portal_auth.logging.warning") as log:
            options = self.h.auth.filter_scope_options([
                {"value": "ALL"}, {"value": "A"}, {"value": "B"}], session)
            self.assertEqual(options, [{"value": "A"}])
            log.assert_not_called()
            self.assertFalse(self.h.auth.scope_allowed(session, "B"))
            log.assert_called_once()

    def test_initial_directory_loading_is_distinct_from_no_permission(self):
        session = {"user": self.actor("AO")}
        ready = self.h.login.snapshot
        self.h.login.snapshot = {}
        self.assertTrue(self.h.auth.public_status(session)["directory_pending"])
        self.assertFalse(self.h.auth.public_status({"user": {"open_id": BUILDING_OPEN_ID_MAP["H"]}})["directory_pending"])
        self.h.auth._account_directory_refresh_failed = True
        failed = self.h.auth.public_status(session)
        self.assertFalse(failed["directory_pending"])
        self.assertIn("人员目录暂时无法读取", failed["directory_warning"])
        self.h.login.snapshot = ready
        self.assertFalse(self.h.auth.public_status(session)["directory_pending"])
        self.assertEqual(self.h.auth.public_status(session)["directory_warning"], '')
        self.assertEqual(self.h.auth.public_status(session)["allowed_scopes"], ["A"])

    def test_rejected_legacy_confirmation_does_not_mark_cross_floor_request_approved(self):
        auth = self.h.auth
        request = auth.create_permission_request(open_id="ou_fixtureAO", name="测试AO", scopes=["B"])
        stored = auth._state_store.get_permission_request(request["request_id"])
        stored.update(code_salt="fixture-salt", code_hash=auth._permission_code_hash("FIX123", "fixture-salt"))
        auth._state_store.put_permission_request(stored)
        before = auth._load_permissions_locked()
        with self.assertRaisesRegex(PortalError, "不支持额外跨楼授权"):
            auth.confirm_permission_request(open_id="ou_fixtureAO", request_id=request["request_id"], code="FIX123")
        self.assertEqual(auth._load_permissions_locked(), before)
        self.assertEqual(auth._state_store.get_permission_request(request["request_id"])["status"], "pending")

    def test_plan_notice_checks_filter_actual_buildings_before_query(self):
        from lan_bitable_template_portal.plan_convergence import PlanConvergenceService
        records = [{"record_id": "recA", "building": "南通A楼"},
                   {"record_id": "recB", "building": "B楼"},
                   {"record_id": "recAB", "building": "A楼、B楼"},
                   {"record_id": "recUnknown", "building": ""}]
        self.assertEqual([r["record_id"] for r in PlanConvergenceService._visible_notice_records(records, ["A"])], ["recA"])
        self.assertEqual(PlanConvergenceService._visible_notice_records(records, ["ALL"]), records)

    def test_workbench_hides_destructive_start_rollback_for_ordinary_accounts(self):
        from lan_bitable_template_portal.workbench_lite import render_workbench_lite
        undos = [{"undo_id": "fixture-start", "undo_action_type": "start", "title": "测试开始回退"},
                 {"undo_id": "fixture-update", "undo_action_type": "update", "title": "测试更新回退"}]
        page = render_workbench_lite(payload={}, session={"user": self.actor("AO"), "role": "building", "can_delete_business": False},
                                    scope="A", work_type="maintenance", month="10月", notice_undos=undos)
        self.assertNotIn('data-undo-id="fixture-start"', page)
        self.assertIn('data-undo-id="fixture-update"', page)

    def test_http_account_apis_enforce_target_and_csrf_even_without_admin_page(self):
        auth, manager = self.h.auth, self.manager
        controller = SimpleNamespace(_current_session=lambda request: {"user": self.actor(request.headers.get("x-fixture-user", "AS"))},
            _request_base_url=lambda request: "http://fixture", _clear_read_cache=lambda keys: None)
        async def read_json(request, max_bytes):
            return await request.json()
        controller._read_json_request = read_json
        runtime = SimpleNamespace(auth_manager=auth, state_store=auth._state_store)
        app = FastAPI()
        install_account_management(app, controller, runtime, lambda: self.h.login)
        async def exercise():
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://fixture") as client:
                listed = await client.get("/api/auth/accounts")
                self.assertEqual(listed.status_code, 200)
                self.assertTrue(all(item["own_scopes"] == ["A"] for item in listed.json()["data"]["items"]))
                for rid, code in (("AE", 200), ("BE", 403)):
                    response = await client.post("/api/auth/accounts/permissions", headers={"Origin": "http://fixture"},
                        json={"changes": [self.change(rid, enabled=False)]})
                    self.assertEqual(response.status_code, code, response.text)
                denied = await client.get("/api/auth/accounts", headers={"x-fixture-user": "AO"})
                self.assertEqual(denied.status_code, 403)
                denied = await client.get("/api/auth/accounts/audit", headers={"x-fixture-user": "HE1"})
                self.assertEqual(denied.status_code, 403)
                denied = await client.post("/api/auth/accounts/permissions", headers={"Origin": "https://outside.test"},
                    json={"changes": [self.change("AO", enabled=False)]})
                self.assertEqual(denied.status_code, 403)
        asyncio.run(exercise())

    def test_business_delete_whitelist_and_post_body_survives_middleware(self):
        from clipflow_backend.main import FastAPIPortalController
        controller = SimpleNamespace(_current_session=lambda request: {"user": self.actor(request.headers.get("x-fixture-user", "AO"))},
            _read_json_request=FastAPIPortalController._read_json_request)
        runtime = SimpleNamespace(auth_manager=self.h.auth, state_store=self.h.auth._state_store)
        app = FastAPI()
        @app.middleware("http")
        async def guard(request, call_next):
            denied = await business_delete_denial(request, controller, runtime)
            return denied if denied is not None else await call_next(request)
        @app.api_route("/api/workbench-actions", methods=["POST"])
        async def mutate(request: Request):
            return {"received": await FastAPIPortalController._read_json_request(request)}
        @app.delete("/api/repair-management/records/{rid}")
        async def remove(rid: str):
            return {"deleted": rid}
        @app.post("/api/notice-undo/{undo_id}/apply")
        async def undo(undo_id: str):
            return {"restored": undo_id}
        async def exercise():
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://fixture") as client:
                for action, code in (("start", 200), ("update", 200), ("end", 200), ("delete", 200)):
                    response = await client.post("/api/workbench-actions", json={"action": action, "scope": "A"})
                    self.assertEqual(response.status_code, code, response.text)
                    if code == 200:
                        self.assertEqual(response.json()["received"]["action"], action)
                for actor, code in (("AO", 403), ("HE1", 403), ("AS", 200), ("HM1", 200)):
                    response = await client.delete("/api/repair-management/records/recFixture", headers={"x-fixture-user": actor})
                    self.assertEqual(response.status_code, code, response.text)
                with patch.object(runtime.state_store, "get_notice_undo_action", return_value={"action_type": "start"}):
                    response = await client.post("/api/notice-undo/fixture/apply")
                    self.assertEqual(response.status_code, 403, response.text)
                    response = await client.post("/api/notice-undo/fixture/apply", headers={"x-fixture-user": "AS"})
                    self.assertEqual(response.status_code, 200, response.text)
                with patch.object(runtime.state_store, "get_notice_undo_action", return_value={"action_type": "update"}):
                    response = await client.post("/api/notice-undo/fixture/apply")
                    self.assertEqual(response.status_code, 200, response.text)
        asyncio.run(exercise())

    def test_notice_deletion_matches_handling_grants_without_global_delete_rights(self):
        from lan_bitable_template_portal.account_policy import notice_delete_allowed
        from lan_bitable_template_portal.module_access import NOTICE_MODULES
        from lan_bitable_template_portal.workbench_lite import render_workbench_lite, extract_workbench_lite_fragments
        for kind in NOTICE_MODULES:
            for oid in (self.actor("AO")["open_id"], self.actor("AE")["open_id"], BUILDING_OPEN_ID_MAP["A"]):
                profile = self.h.auth.account_profile(oid)
                self.assertFalse(profile["can_delete_business"])
                self.assertTrue(notice_delete_allowed(profile, kind))
            row = {"active_item_id": "active-fixture", "target_record_id": "rec-fixture", "work_type": kind,
                   "building_codes": ["A"], "title": "A楼隔离测试", "status": "开始"}
            for modules, expected in (([kind], True), ([], False), (["event"], False)):
                page = render_workbench_lite(payload={"ongoing": [row]},
                    session={"user": self.actor("AO"), "role": "building", "can_delete_business": False, "allowed_modules": modules},
                    scope="A", work_type=kind, active_item_id="active-fixture")
                self.assertEqual('data-ongoing-delete-mode="remote"' in extract_workbench_lite_fragments(page)["detail"], expected)
                self.assertEqual(notice_delete_allowed({"modules": modules, "scope_rules": ["A"]}, kind), expected)
        self.assertFalse(notice_delete_allowed({"modules": list(NOTICE_MODULES), "scope_rules": []}, "maintenance"))
        self.assertFalse(notice_delete_allowed(self.h.auth.account_profile(self.actor("AO")["open_id"]), "event"))

    def test_notice_delete_endpoint_checks_login_scope_module_and_resolved_target(self):
        import ast
        from pathlib import Path
        from clipflow_backend.main import FastAPIPortalController
        from clipflow_backend.api_models import OngoingDeleteRequest
        from lan_bitable_template_portal.identity_utils import normalize_notice_identity_payload
        from lan_bitable_template_portal.module_access import module_denial

        def current_session(request):
            who = request.headers.get("x-fixture-user", "AO")
            return {"user": self.actor(who)} if who != "anonymous" else None

        def scope_check(session, scope):
            if not self.h.auth.scope_allowed(session, scope):
                raise PortalError("无权操作此楼栋")
            return scope

        def validate(payload, *, scope):
            if payload.get("target_record_id") == "rec-other-floor":
                raise PortalError("当前账号无权删除该楼栋的进行中通告。")
            if payload.get("target_record_id") == "rec-other-module":
                payload["work_type"] = "change"
            if payload.get("target_record_id") == "rec-event":
                payload["work_type"] = "event"

        controller = SimpleNamespace(_current_session=current_session,
            _read_json_request=FastAPIPortalController._read_json_request,
            _authorized_scope_or_error=scope_check,
            _auth_required_response=lambda: JSONResponse({"ok": False}, status_code=401),
            _portal_error_response=lambda exc, **_: JSONResponse({"ok": False, "error": str(exc)}, status_code=403),
            _json_ok=lambda request, session, data: JSONResponse({"ok": True, "data": data}),
            _record_deletion_audit=Mock(), _deletion_business_type=lambda _: "维保", _deletion_scope=lambda _, scope: scope,
            _deletion_record_name=lambda _: "隔离测试", _clear_read_cache=Mock(), _notify_qt_active_streams=Mock())
        async def read_model(request, model):
            return SimpleNamespace(to_payload=lambda: request.state.payload)
        controller._read_model_request = read_model
        execute = Mock(return_value={"ok": True, "remote_deleted": True, "local_cleanup_completed": True, "record_id": "rec-fixture"})
        runtime = SimpleNamespace(auth_manager=self.h.auth, state_store=self.h.auth._state_store,
            service=SimpleNamespace(validate_ongoing_delete_item=validate), execute_local_delete_active_item=execute,
            clear_payload_cache=Mock())
        source = Path(__file__).parent / "clipflow_backend/main.py"
        node = copy.deepcopy(next(n for n in ast.walk(ast.parse(source.read_text(encoding="utf-8-sig")))
                                 if isinstance(n, ast.AsyncFunctionDef) and n.name == "ongoing_items_delete"))
        node.decorator_list = []
        namespace = {"self": controller, "Request": Request, "JSONResponse": JSONResponse, "Any": object,
                     "asyncio": asyncio, "PortalRuntime": runtime, "OngoingDeleteRequest": OngoingDeleteRequest,
                     "normalize_notice_identity_payload": normalize_notice_identity_payload}
        exec(compile(ast.fix_missing_locations(ast.Module(body=[node], type_ignores=[])), str(source), "exec"), namespace)
        app = FastAPI()
        @app.middleware("http")
        async def guard(request, call_next):
            request.state.payload = await controller._read_json_request(request)
            for policy in (module_denial, business_delete_denial):
                denied = await policy(request, controller, runtime)
                if denied is not None:
                    return denied
            return await call_next(request)
        app.add_api_route("/api/ongoing-items/delete", namespace["ongoing_items_delete"], methods=["POST"])
        permissions = self.h.auth._load_permissions_locked()
        permissions["users"][self.actor("AO")["open_id"]].update(module_access_version=1, modules=["maintenance", "event"])
        self.h.auth._save_permissions_locked(permissions)
        async def exercise():
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://fixture") as client:
                for who, scope, kind, target, code in (
                    ("AO", "A", "maintenance", "rec-fixture", 200),
                    ("AO", "B", "maintenance", "rec-fixture", 403),
                    ("AO", "A", "change", "rec-fixture", 403),
                    ("AO", "A", "maintenance", "rec-other-floor", 403),
                    ("AO", "A", "maintenance", "rec-other-module", 403),
                    ("AO", "A", "maintenance", "rec-event", 403),
                    ("anonymous", "A", "maintenance", "rec-fixture", 401)):
                    before = execute.call_count
                    response = await client.post("/api/ongoing-items/delete", headers={"x-fixture-user": who},
                        json={"scope": scope, "work_type": kind, "target_record_id": target, "active_item_id": "active-fixture"})
                    self.assertEqual(response.status_code, code, response.text)
                    self.assertEqual(execute.call_count - before, int(code == 200))
                permissions["users"][self.actor("AO")["open_id"]]["enabled"] = False
                self.h.auth._save_permissions_locked(permissions)
                denied = await client.post("/api/ongoing-items/delete", json={"scope": "A", "work_type": "maintenance"})
                self.assertEqual(denied.status_code, 403, denied.text)
        asyncio.run(exercise())

    def test_cabinet_patch_and_retry_cannot_bypass_delete_whitelist(self):
        from lan_bitable_template_portal.cabinet_power_routes import install_cabinet_power_routes
        service = Mock()
        service.local.version.return_value = 1
        service.local.document.return_value = {"delete": True}
        service.save_operation.return_value = {"deleted": True}
        controller = SimpleNamespace(_current_session=lambda request: {"user": self.actor(request.headers.get("x-fixture-user", "AO"))},
            _json_ok=lambda request, session, data: JSONResponse({"ok": True, "data": data}))
        async def read_json(request, max_bytes):
            return await request.json()
        controller._read_json_request = read_json
        runtime = SimpleNamespace(auth_manager=self.h.auth, state_store=self.h.auth._state_store)
        app = FastAPI()
        with patch("lan_bitable_template_portal.cabinet_power_routes.CabinetPowerService", return_value=service):
            install_cabinet_power_routes(app, controller, runtime)
        async def exercise():
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://fixture") as client:
                response = await client.patch("/api/cabinet-power/operations/recFixture", json={"scope": "A", "groups": []})
                self.assertEqual(response.status_code, 403, response.text)
                service.save_operation.assert_not_called()
                for stage in ("resume", "reconcile"):
                    response = await client.post("/api/cabinet-power/writes/fixture/" + stage, json={"scope": "A"})
                    self.assertEqual(response.status_code, 403, response.text)
                service.resume_write.assert_not_called()
                service.reconcile_write.assert_not_called()
                response = await client.patch("/api/cabinet-power/operations/recFixture", headers={"x-fixture-user": "AS"},
                    json={"scope": "A", "groups": []})
                self.assertEqual(response.status_code, 200, response.text)
                service.save_operation.assert_called_once()
        asyncio.run(exercise())
