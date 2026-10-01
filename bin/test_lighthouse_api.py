# -*- coding: utf-8 -*-
"""Isolated unit tests for PortalAPICatalog."""
import asyncio
import ast
from datetime import datetime
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Literal

sys.path.insert(0, str(Path(__file__).resolve().parent))

import httpx
from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import JSONResponse, PlainTextResponse, Response
from pydantic import BaseModel, Field

from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog, _frontend_field
from lan_bitable_template_portal.lighthouse_ai import AssistantError


# ---------------------------------------------------------------------------
# Module-level Pydantic models (resolved from endpoint globals via AST)
# ---------------------------------------------------------------------------
class WorkbenchActionRequest(BaseModel):
    scope: str
    work_type: str = "maintenance"
    record_id: str = ""
    operation_id: str = ""


class WaterRecordRequest(BaseModel):
    scope: str
    meter: str
    meter_value: float | int | str | None = None
    upload_ids: list[str] = Field(default_factory=list, max_length=20)


class MemberAssignRequest(BaseModel):
    role: str
    open_id: str
    record_id: str = ""


class StatusRequest(BaseModel):
    scope: str
    status: Literal["open", "closed", "paused"]


class AttachRequest(BaseModel):
    scope: str
    attachment_ids: list[str]


class DateStep(BaseModel):
    actual: datetime


class DateRows(BaseModel):
    groups: list[DateStep]


class CalendarChoiceRequest(BaseModel):
    drill_date: str
    first_start_time: str
    actual: str
    selected: list[Literal[1, 2, 3]] = Field(min_length=1, max_length=2)
    progress: int = Field(ge=0, le=100)


class SecretInput(BaseModel):
    token: str


class MockRequest(BaseModel):
    """Backend admin route used only to assert it is excluded."""
    payload: str


class NestedNullableValue(BaseModel):
    value: int


class NullableAnyOfRequest(BaseModel):
    """Required-yet-nullable union fields (anyOf) for nullability checks."""
    scope: str
    cost: float | int | None
    profile: NestedNullableValue | None


# ---------------------------------------------------------------------------
# Fixture app helpers
# ---------------------------------------------------------------------------

def _build_fixture_app():
    app = FastAPI()

    @app.post("/api/workbench/action")
    async def workbench_action(request: Request):
        # Guard: must have matching cookie + origin like real business controllers.
        if request.headers.get("cookie") != "session=test-session":
            return JSONResponse({"ok": False, "error": "auth required"}, status_code=401)
        if request.headers.get("origin") not in ("http://testserver", "https://portal.test"):
            return JSONResponse({"ok": False, "error": "bad origin"}, status_code=403)
        payload = await _read_model_request(request, WorkbenchActionRequest)
        return {"ok": True, "data": {"scope": payload.scope, "work_type": payload.work_type}}

    @app.post("/api/workbench/soft-fail")
    async def workbench_soft_fail(request: Request):
        # Native envelope reports business failure even though HTTP 200.
        return {"ok": False, "error": "invalid business state"}

    @app.post("/api/capacity/water/records")
    async def create_water(request: Request):
        payload = await _read_model_request(request, WaterRecordRequest)
        return {"ok": True, "data": {"scope": payload.scope, "meter": payload.meter}}

    @app.post("/api/daily/send")
    async def daily_send(request: Request):
        payload = await request.json()
        text = str(payload.get("text") or "")
        recipient = payload.get("recipient") or ""
        return {"ok": True, "data": {"sent_to": recipient, "preview": text[:20]}}

    @app.get("/api/notice/list")
    async def notice_list(request: Request):
        scope = request.query_params.get("scope") or "ALL"
        return {"ok": True, "data": [{"id": "rec_1", "title": "A楼维保", "scope": scope}]}

    @app.get("/api/notice/large")
    async def notice_large(request: Request):
        items = [{"id": f"rec_{i}", "title": f"事项 {i}"} for i in range(60)]
        return {"ok": True, "data": {"items": items, "total": len(items), "page": 1, "count": len(items)}}

    @app.get("/api/notice/refresh")
    async def notice_refresh(request: Request):
        return {"ok": True, "data": {"refreshed": True}}

    @app.post("/api/notice-target-candidates")
    async def notice_target_candidates(request: Request):
        payload = await request.json()
        keyword = str((payload or {}).get("keyword") or "")
        return {"ok": True, "data": [{"record_id": "target-1", "name": keyword or "候选"}]}

    @app.post("/api/change-target-candidates")
    async def change_target_candidates(request: Request):
        payload = await request.json()
        return {"ok": True, "data": [{"record_id": "target-c", "name": (payload or {}).get("title") or "变更候选"}]}

    @app.post("/api/repair-management/prefill")
    async def repair_prefill(request: Request):
        payload = await request.json()
        return {"ok": True, "data": {"prefill": payload or {}}}

    @app.post("/api/repair/records/apply-preview")
    async def misleading_preview(request: Request):
        # Misleading name: this POST actually MUTATES, so it must not be read-only.
        payload = await request.json()
        return {"ok": True, "data": {"result": "applied", "note": payload.get("note")}}

    @app.get("/api/repair/query")
    async def repair_query(request: Request, scope: str):
        return {"ok": True, "data": {"scope": scope}}

    @app.post("/api/repair/assignment")
    async def repair_assignment(request: Request):
        payload = await _read_model_request(request, MemberAssignRequest)
        return {"ok": True, "data": {"role": payload.role, "open_id": payload.open_id}}

    @app.post("/api/status/update")
    async def status_update(request: Request):
        payload = await _read_model_request(request, StatusRequest)
        return {"ok": True, "data": {"status": payload.status}}

    @app.post("/api/attachment/bind")
    async def attachment_bind(request: Request):
        payload = await _read_model_request(request, AttachRequest)
        return {"ok": True, "data": {"count": len(payload.attachment_ids)}}

    @app.post("/api/engineer/mop/upload")
    async def mop_upload(request: Request, note: str = Form(""), file: UploadFile = File(...)):
        raw = await file.read()
        return {"ok": True, "data": {"size": len(raw), "note": note, "name": file.filename}}

    @app.get("/api/learning/task")
    async def learning_task(request: Request):
        return {"ok": True, "data": {"question": "请回答", "choices": ["A", "B", "C"]}}

    @app.get("/api/download-binary")
    async def download_binary():
        return Response(content=b"\x89PNG\r\n\x1a\nbinary-data", media_type="image/png")

    @app.get("/api/big-binary")
    async def big_binary():
        # Anything above 20MiB must be rejected, not silently truncated.
        return Response(content=b"x" * (20 * 1024 * 1024 + 1), media_type="application/octet-stream")

    @app.get("/api/repair/people")
    async def repair_people(request: Request):
        return {"ok": True, "data": {"name": "张三", "employee_no": "E12345",
                                     "phone": "13812345678", "email": "test@example.com",
                                     "身份证号": "110101199001011234"}}

    # Business admin functionality stays available to the agent.
    @app.get("/api/admin/mop-settings")
    async def admin_mop_settings(request: Request):
        return {"ok": True, "data": {"template": "A"}}

    @app.post("/api/admin/mop-settings")
    async def save_admin_mop_settings(request: Request):
        payload = await request.json()
        return {"ok": True, "data": {"saved": payload}}

    # The real _read_model_request helper pattern (async, returns model).
    async def _read_model_request(request: Request, model_cls):
        payload = await request.json()
        return model_cls.model_validate(payload)

    # -- routes that MUST be excluded --------------------------------------
    @app.post("/api/assistant/chat")
    async def assistant_chat(request: Request):
        return {"ok": True, "data": "SHOULD_NOT_EXIST"}

    @app.get("/api/auth/login")
    async def auth_login():
        return {"ok": True}

    @app.get("/api/backend/stats")
    async def backend_stats():
        return {"ok": True}

    @app.get("/api/health")
    async def health():
        return {"ok": True}

    @app.get("/api/signatures/image")
    async def signature_image():
        return Response(content=b"sig", media_type="image/png")

    @app.get("/api/signatures/temporary/session")
    async def sig_temp_session():
        return {"ok": True}

    @app.get("/api/jobs/stream")
    async def jobs_stream():
        return PlainTextResponse("data")

    @app.get("/api/assistant/recursive")
    async def assistant_recursive():
        return {"ok": True}

    @app.get("/api/system/runtime")
    async def system_runtime():
        return {"ok": True}

    # System-level admin route must stay excluded even though business admin isn't.
    @app.get("/api/admin/system/reload")
    async def admin_system_reload():
        return {"ok": True}

    return app


def _dummy_get_app():
    """Isolated app with dummy GET endpoints mirroring real business routes."""
    app = FastAPI()

    @app.get("/api/repair-management/records")
    async def _records(request: Request):
        return {"ok": True}

    @app.get("/api/repair-management/overview")
    async def _overview(request: Request):
        return {"ok": True}

    @app.get("/api/repair-management/event-candidates")
    async def _event_candidates(request: Request):
        return {"ok": True}

    @app.get("/api/repair-management/cmdb-cache/status")
    async def _cmdb_status(request: Request):
        return {"ok": True}

    @app.get("/api/repair-management/sync-status")
    async def _sync_status(request: Request):
        return {"ok": True}

    @app.get("/api/critical-guard")
    async def _critical_guard(request: Request):
        return {"ok": True}

    @app.get("/api/repair-refresh")
    async def _repair_refresh(request: Request):
        return {"ok": True}

    @app.get("/api/maintenance-refresh")
    async def _maintenance_refresh(request: Request):
        return {"ok": True}

    @app.get("/api/change-refresh")
    async def _change_refresh(request: Request):
        return {"ok": True}

    @app.get("/api/notice/refresh")
    async def _notice_refresh(request: Request):
        return {"ok": True}

    return app


def _native_route_catalog():
    """Read registration syntax without starting native services or calling APIs."""
    root = Path(__file__).resolve().parent
    app = FastAPI()

    async def endpoint():
        raise AssertionError("Metadata audit must not invoke native business operations")

    tree = ast.parse((root / "clipflow_backend/main.py").read_text(encoding="utf-8-sig"))
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for decorator in node.decorator_list:
            if not isinstance(decorator, ast.Call) or not isinstance(decorator.func, ast.Attribute):
                continue
            method = decorator.func.attr
            if method not in {"get", "post", "put", "patch", "delete", "api_route"} or not decorator.args:
                continue
            path = ast.literal_eval(decorator.args[0])
            if not path.startswith("/api/"):
                continue
            methods = [method.upper()] if method != "api_route" else next(
                ast.literal_eval(kw.value) for kw in decorator.keywords if kw.arg == "methods")
            app.add_api_route(path, endpoint, methods=methods)

    for filename, prefix in (("learning_routes.py", "learning"),
                             ("plan_convergence_routes.py", "plan-convergence"),
                             ("cabinet_power_routes.py", "cabinet-power")):
        tree = ast.parse((root / "lan_bitable_template_portal" / filename).read_text(encoding="utf-8-sig"))
        for node in ast.walk(tree):
            routes = None
            if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "ROUTES" for t in node.targets):
                routes = node.value
            elif isinstance(node, ast.For) and isinstance(node.iter, ast.Call) and isinstance(node.iter.func, ast.Attribute):
                routes = node.iter.func.value
            if isinstance(routes, ast.Dict):
                for path, methods in ast.literal_eval(routes).items():
                    app.add_api_route(f"/api/{prefix}/{path}", endpoint, methods=list(methods))
    return PortalAPICatalog(app)


class PortalAPICatalogTests(unittest.TestCase):
    def setUp(self):
        self.app = _build_fixture_app()
        self.catalog = PortalAPICatalog(self.app)

    # -- discovery ---------------------------------------------------------
    def test_discovery_include_business_and_exclude_internal(self):
        result = self.catalog.discover(page_size=200)
        ids = {item["id"] for item in result["items"]}
        self.assertIn("POST /api/workbench/action", ids)
        self.assertIn("GET /api/notice/list", ids)
        self.assertIn("POST /api/capacity/water/records", ids)
        self.assertIn("POST /api/notice-target-candidates", ids)
        self.assertIn("POST /api/_assistant/parse-notice", ids)
        # Business admin is included.
        self.assertIn("GET /api/admin/mop-settings", ids)
        self.assertIn("POST /api/admin/mop-settings", ids)
        # excluded endpoints
        self.assertNotIn("POST /api/assistant/chat", ids)
        self.assertNotIn("GET /api/auth/login", ids)
        self.assertNotIn("GET /api/backend/stats", ids)
        self.assertNotIn("GET /api/health", ids)
        self.assertNotIn("GET /api/signatures/image", ids)
        self.assertNotIn("GET /api/signatures/temporary/session", ids)
        self.assertNotIn("GET /api/jobs/stream", ids)
        self.assertNotIn("GET /api/system/runtime", ids)
        self.assertNotIn("POST /api/assistant/recursive", ids)
        self.assertNotIn("GET /api/admin/system/reload", ids)

    def test_discover_groups_and_page_size(self):
        result = self.catalog.discover(page_size=10)
        self.assertIn("page_size", result)
        self.assertIn("groups", result)
        self.assertTrue(isinstance(result["groups"], list) and result["groups"])
        self.assertEqual(result["page_size"], 10)
        # page_size is clamped to <= 50
        self.assertEqual(self.catalog.discover(page_size=999)["page_size"], 50)
        self.assertEqual(self.catalog.discover(page_size=0)["page_size"], 1)

    def test_discover_pagination_and_group_filter(self):
        result = self.catalog.discover(group="通告与事件", page_size=1, page=1)
        for item in result["items"]:
            self.assertEqual(item["group"], "通告与事件")
            self.assertIn("page", item)
        self.assertGreaterEqual(result["total"], 1)

    def test_get_returns_descriptor_and_unknown_raises(self):
        desc = self.catalog.get("POST /api/workbench/action")
        self.assertEqual(desc["id"], "POST /api/workbench/action")
        self.assertEqual(desc["method"], "POST")
        self.assertIn("body", desc["schema"])
        self.assertIn("page", desc)
        with self.assertRaises(AssistantError):
            self.catalog.get("POST /api/unknown/nope")

    def test_native_catalog_labels_aliases_and_pages(self):
        catalog = _native_route_catalog()
        first = catalog.discover(page_size=50)
        items = [item for page in range(1, (first["total"] + 49) // 50 + 1)
                 for item in catalog.discover(page=page, page_size=50)["items"]]
        self.assertGreater(len(items), 200)
        for item in items:
            with self.subTest(api_id=item["id"]):
                self.assertNotRegex(item["name"], r"[a-z]", item["name"])

        cases = (
            ("月度事件", "GET /api/events/monthly", "事件", "/?mode=events"),
            ("事件 月报", "GET /api/events/monthly", "事件", "/?mode=events"),
            ("通告工作台", "POST /api/workbench-actions", "工作台", "/workbench-lite"),
            ("维修跟进", "POST /api/repair-management/followups", "维修单与跟进", "/repair-management"),
            ("维修进度", "GET /api/repair-management/status", "维修单与跟进", "/repair-status"),
            ("机柜 文本填充", "POST /api/cabinet-power/batches/{batch_id}/text-apply", "机柜上下电", "/cabinet-power"),
            ("用水量", "GET /api/capacity/water/records", "容量与水耗", "/water-management"),
            ("学习 试卷", "GET /api/learning/papers", "画像学练", "/learning"),
            ("每日任务清单", "GET /api/daily-tasks", "日常工作", "/daily-tasks"),
            ("晨会 生成", "POST /api/daily-tasks/morning-meeting/generate", "日常工作", "/daily-tasks"),
            ("屏蔽记录", "GET /api/plan-convergence/blocks", "计划收敛审查", "/plan-convergence"),
            ("检修核对", "POST /api/plan-convergence/maintenance/check", "计划收敛审查", "/plan-convergence"),
            ("检修收敛", "GET /api/plan-convergence/maintenance/records", "计划收敛审查", "/plan-convergence"),
            ("应急演练 生成", "POST /api/drills/{drill_id}/generate", "演练", "/drill-management"),
            ("MOP 签名选择", "POST /api/engineer/mop/fill", "维护单", "/engineer/mop"),
            ("签名分配", "PUT /api/drills/{drill_id}/execution", "演练", "/drill-management"),
            ("签名选择", "GET /api/signatures/management/people", "人员与签名管理", "/signature-management"),
            ("重点保障 填报", "PUT /api/critical-guard/responses/{response_id}", "重保管理", "/critical-guard"),
            ("通告历史", "POST /api/admin/notice-memory/history-scan", "通告历史", "/admin/history-memory"),
        )
        for keyword, api_id, group, page in cases:
            with self.subTest(keyword=keyword):
                matches = catalog.discover(keyword=keyword, group=group, page_size=50)
                self.assertIn(api_id, {item["id"] for item in matches["items"]})
                self.assertEqual(catalog.get(api_id)["page"], page)
        self.assertEqual(catalog.get("GET /api/events/monthly")["name"], "查询月度事件")
        self.assertEqual(catalog.get("POST /api/plan-convergence/maintenance/check")["name"], "检修核对")
        for keyword in ("维保核对", "维保收敛"):
            self.assertEqual(catalog.discover(keyword=keyword, group="计划收敛审查")["total"], 0)
        self.assertEqual(catalog.discover(keyword="水耗 不存在的操作")["total"], 0)
        self.assertEqual(catalog.discover(keyword="月报", group="维护单")["total"], 0)

    def test_native_catalog_preserves_classification_and_exclusions(self):
        catalog = _native_route_catalog()
        for api_id in (
            "GET /api/events/monthly", "GET /api/repair-management/followups",
            "GET /api/repair-management/status", "GET /api/daily-tasks",
            "GET /api/signatures/management/people", "GET /api/drills/{drill_id}/execution",
            "POST /api/cabinet-power/batches/{batch_id}/text-preview",
        ):
            with self.subTest(api_id=api_id):
                self.assertTrue(catalog.get(api_id)["read_only"])
                self.assertEqual(catalog.get(api_id)["risk"], "normal")
        for api_id in (
            "GET /api/maintenance-refresh", "POST /api/events/transfer-repair",
            "POST /api/workbench-actions", "POST /api/daily-tasks/send",
            "POST /api/learning/papers/{id}/reveal", "POST /api/plan-convergence/compare",
            "POST /api/cabinet-power/batches/{batch_id}/text-apply",
            "POST /api/engineer/mop/fill", "POST /api/drills/{drill_id}/generate",
            "POST /api/signatures/usage-confirmations/send",
            "PUT /api/critical-guard/responses/{response_id}", "POST /api/signatures/management/merge",
        ):
            with self.subTest(api_id=api_id):
                self.assertFalse(catalog.get(api_id)["read_only"])
                self.assertEqual(catalog.get(api_id)["risk"], "high")
        for api_id in (
            "GET /api/auth/status", "POST /api/backend/shutdown", "GET /api/jobs/stream",
            "GET /api/plan-convergence/settings", "POST /api/plan-convergence/settings/browser-login",
            "GET /api/signatures/image", "POST /api/signatures/save",
            "GET /api/signatures/temporary/session", "GET /api/signatures/temporary/image",
            "GET /api/signatures/management/{operation}", "POST /api/signatures/management/{operation}",
            "GET /api/signatures/management/request", "POST /api/signatures/management/requests",
            "POST /api/signatures/management/submit", "POST /api/signatures/management/temporary",
            "POST /api/signatures/management/people", "GET /api/signatures/management/merge",
        ):
            with self.subTest(api_id=api_id), self.assertRaises(AssistantError):
                catalog.validate_operation({"api_id": api_id, "path_params": {"operation": "submit"}})

    def test_concrete_signature_management_routes_reuse_native_guards(self):
        from types import SimpleNamespace
        from lan_bitable_template_portal.signature_management import dispatch, SignatureManagementError

        app = FastAPI()
        manager = SimpleNamespace(people=lambda payload: {"people": [{"record_id": "person-fixture", "name": "测试人员"}]})

        @app.api_route("/api/signatures/management/{operation}", methods=["GET", "POST"])
        async def management(operation: str, request: Request):
            try:
                actor = "fixture" if request.headers.get("cookie") == "session=test-session" else ""
                data = dispatch(manager, request.method, operation, {}, actor=actor, is_admin=False)
                return {"ok": True, "data": data}
            except SignatureManagementError as exc:
                return JSONResponse({"ok": False, "error": str(exc)}, status_code=exc.status)

        catalog = PortalAPICatalog(app)

        async def run(cookie, api_id):
            request = SimpleNamespace(base_url="http://testserver/", headers={"cookie": cookie, "origin": "http://testserver"}, client=None)
            return await catalog.invoke({"api_id": api_id, "path_params": {"operation": "submit"}}, request)

        api_id = "GET /api/signatures/management/people"
        self.assertEqual(asyncio.run(run("", api_id))["status"], 401)
        result = asyncio.run(run("session=test-session", api_id))
        self.assertEqual(result["data"]["people"][0]["record_id"], "person-fixture")
        self.assertEqual(asyncio.run(run("session=test-session", "POST /api/signatures/management/associate"))["status"], 403)

    def test_safe_person_assignment_schema_keeps_native_field_names(self):
        from clipflow_backend.api_models import (
            CriticalGuardResponseRequest, DrillExecutionRequest,
            EngineerMopFillRequest, EngineerMopUploadSignedRequest,
        )

        cases = (
            ("/api/engineer/mop/fill", "POST", EngineerMopFillRequest, {"signatures", "signature_context_key"}, "high"),
            ("/api/engineer/mop/upload-signed", "POST", EngineerMopUploadSignedRequest, {"signatures", "signature_context_key"}, "normal"),
            ("/api/drills/{drill_id}/execution", "PUT", DrillExecutionRequest,
             {"commander", "evaluator", "participants", "step_signers", "signature_time"}, "high"),
            ("/api/critical-guard/responses/{response_id}", "PUT", CriticalGuardResponseRequest,
             {"signatures", "signature_source", "signature_record_id"}, "high"),
        )
        for path, method, model, fields, risk in cases:
            with self.subTest(path=path):
                app = FastAPI()

                async def endpoint(body: model):
                    raise AssertionError("Schema inspection must not generate signed output")

                app.add_api_route(path, endpoint, methods=[method])
                catalog = PortalAPICatalog(app)
                descriptor = catalog.get(f"{method} {path}")
                properties = descriptor["schema"]["body"]["properties"]
                self.assertTrue(fields <= properties.keys())
                self.assertFalse(descriptor["read_only"])
                self.assertEqual(descriptor["risk"], risk)

        class SelectionInput(BaseModel):
            signatures: list[dict[str, str]]
            signature_png: str = ""
            signature_raw: str = ""
            access_token: str = ""

        app = FastAPI()

        @app.post("/api/engineer/mop/fill")
        async def fill(body: SelectionInput, token: str = "", scope: str = "A"):
            raise AssertionError("Do not generate signatures")

        catalog = PortalAPICatalog(app)
        descriptor = catalog.get("POST /api/engineer/mop/fill")
        self.assertEqual(set(descriptor["schema"]["body"]["properties"]), {"signatures"})
        self.assertEqual(descriptor["schema"]["body"]["required"], ["signatures"])
        self.assertEqual(descriptor["schema"]["query"], ["scope"])
        selection = [{"source": "staff", "role": "implementer", "record_id": "person-fixture"}]
        normalized, missing = catalog.validate_operation({"api_id": descriptor["id"], "body": {"signatures": selection}})
        self.assertEqual(missing, [])
        self.assertEqual(normalized["body"]["signatures"], selection)

    def test_trusted_schema_redaction_does_not_relax_business_data_redaction(self):
        class PersonSelection(BaseModel):
            source: Literal["staff", "external"] = "staff"
            role: Literal["implementer", "auditor"]
            record_id: str = Field(examples=["example-person-fixture"])
            access_token: str

        class OutputSelection(BaseModel):
            signatures: list[PersonSelection]
            signature_time: str = Field(default="default-time-fixture", examples=["example-time-fixture"])
            note: str = Field(default="default-note-fixture", json_schema_extra={"const": "constant-note-fixture"})

        app = FastAPI()

        @app.put("/api/drills/{drill_id}/execution")
        async def execution(body: OutputSelection):
            raise AssertionError("Schema audit must not generate signed output")

        @app.post("/api/engineer/mop/upload-local")
        async def upload(file: UploadFile = File(...), signature_time: str = Form(...), password: str = Form(...)):
            raise AssertionError("Schema audit must not upload files")

        catalog = PortalAPICatalog(app)
        schema = catalog.get("PUT /api/drills/{drill_id}/execution")["schema"]["body"]
        self.assertEqual(catalog.discover(keyword="演练 execution")["items"][0]["schema"]["body"], schema)
        self.assertEqual(schema["properties"]["signature_time"]["type"], "string")
        self.assertEqual(schema["properties"]["signatures"]["items"]["$ref"], "#/$defs/PersonSelection")
        person = schema["$defs"]["PersonSelection"]
        self.assertEqual(person["properties"]["role"]["enum"], ["implementer", "auditor"])
        self.assertEqual(person["required"], ["role", "record_id"])
        for removed in ("access_token", '"default"', '"examples"', '"const"', "fixture"):
            self.assertNotIn(removed, json.dumps(schema))
        form = catalog.get("POST /api/engineer/mop/upload-local")["schema"]["body"]
        self.assertEqual(set(form["properties"]), {"signature_time"})
        self.assertEqual(form["required"], ["signature_time"])

        result = catalog._consume_json(200, {"ok": True, "data": {
            "name": "测试人员", "signatures": [{"access_token": "payload-token-fixture"}],
            "signature_time": "payload-time-fixture", "signature_png": "payload-image-fixture",
            "schema": {"properties": {"signatures": "payload-schema-fixture", "access_token": "payload-token-fixture"}},
        }}, "GET /api/signatures/people")
        self.assertEqual(result["data"]["name"], "测试人员")
        self.assertEqual(result["data"]["schema"]["properties"], {})
        self.assertNotIn("fixture", json.dumps(result["data"]))

    def test_learning_dispatch_schema_describes_native_writes(self):
        catalog = _native_route_catalog()
        cases = (
            ("POST", "papers/{id}/answer", {"question_id", "version", "operation_id"}, {"option_ids", "answer_text", "self_rating", "practice"}),
            ("POST", "papers/{id}/reveal", {"question_id"}, {"kind"}),
            ("POST", "papers/{id}/notes", {"question_id"}, {"note", "favorite"}),
            ("POST", "questions", set(), {"stem", "bank", "options", "correct_option_ids", "new_id"}),
            ("PUT", "questions/{id}", {"version"}, {"stem", "type", "status", "reason", "answer_text"}),
            ("POST", "questions/{id}/status", {"status"}, {"ids", "versions", "version"}),
            ("POST", "questions/{id}/copy", set(), {"version"}),
            ("POST", "issues", {"paper_id", "question_id", "description"}, {"suggestion", "category"}),
            ("PATCH", "issues/{id}", {"version"}, {"status", "remark", "description"}),
            ("PUT", "settings", set(), {"enabled", "publish_time", "reminder_enabled", "reminder_time"}),
            ("POST", "import", {"questions"}, {"preview"}),
            ("POST", "publish", set(), {"date"}),
            ("POST", "refresh", set(), set()),
            ("DELETE", "attachments/{id}", set(), {"version"}),
        )
        for method, path, required, optional in cases:
            with self.subTest(path=path):
                descriptor = catalog.get(f"{method} /api/learning/{path}")
                body = descriptor["schema"]["body"]
                self.assertEqual(set(body["required"]), required)
                self.assertTrue(required | optional <= body["properties"].keys())
                self.assertFalse(descriptor["read_only"])
        answer = catalog.get("POST /api/learning/papers/{id}/answer")["schema"]["body"]
        self.assertEqual(answer["properties"]["version"]["type"], "integer")
        questions = catalog.get("POST /api/learning/import")["schema"]["body"]["properties"]["questions"]
        self.assertEqual(questions["maxItems"], 500)
        self.assertIn("text", questions["items"]["properties"]["options"]["items"]["properties"])
        attachment = catalog.get("POST /api/learning/attachments")
        self.assertTrue(attachment["multipart"])
        self.assertTrue({"question_id", "issue_id", "kind", "version"} <= set(attachment["schema"]["query"]))
        _, missing = catalog.validate_operation({"api_id": "POST /api/learning/papers/{id}/answer",
                                                "path_params": {"id": "paper-fixture"}})
        self.assertEqual({field["path"] for field in missing}, {"question_id", "version", "operation_id"})

    def test_single_scope_metadata_uses_exact_native_routes(self):
        catalog = _native_route_catalog()
        for api_id, buildings, section, required in (
            ("GET /api/capacity/water/records", "ABCDEH", "params", True),
            ("POST /api/capacity/water/records", "ABCDEH", "body", True),
            ("PATCH /api/capacity/water/records/{record_id}", "ABCDEH", "body", True),
            ("GET /api/learning/papers", "ABCDEH", "params", False),
            ("POST /api/learning/issues", "ABCDEH", "body", False),
            ("GET /api/drills", "ABCDE", "params", False),
            ("PUT /api/drills/{drill_id}/execution", "ABCDE", "params", True),
            ("GET /api/critical-guard/tasks", "ABCDE", "params", True),
            ("PUT /api/critical-guard/responses/{response_id}", "ABCDE", "body", True),
            ("POST /api/critical-guard/source-files", "ABCDE", "body", True),
        ):
            with self.subTest(api_id=api_id):
                descriptor = catalog.get(api_id)
                self.assertEqual(descriptor["scope_mode"], "single")
                self.assertEqual(descriptor["scope_values"], list(buildings))
                self.assertEqual(descriptor["scope_section"], section)
                self.assertEqual(descriptor["scope_required"], required)
        found = catalog.discover(keyword="water records", page_size=50)
        self.assertTrue(all(item["scope_mode"] == "single" for item in found["items"]))
        for api_id in (
            "GET /api/capacity/water/buildings", "POST /api/capacity/water/refresh",
            "GET /api/critical-guard/bootstrap", "POST /api/critical-guard/tasks",
            "GET /api/critical-guard/images/{response_id}", "GET /api/learning/questions",
            "GET /api/learning/papers/{id}", "PUT /api/drills/{drill_id}/configuration",
        ):
            self.assertNotIn("scope_mode", catalog.get(api_id), api_id)

    def test_single_scope_rejects_invalid_input_before_native_invocation(self):
        from types import SimpleNamespace

        class DefaultScope(BaseModel):
            scope: str = "A"

        app, calls = FastAPI(), []

        @app.get("/api/capacity/water/records")
        async def water_read(scope: str = "ALL"):
            calls.append(scope)
            return {"ok": True, "data": {"scope": scope}}

        @app.post("/api/capacity/water/records")
        async def water_write(body: DefaultScope):
            calls.append(body.scope)
            return {"ok": True, "data": {"scope": body.scope}}

        catalog = PortalAPICatalog(app)
        request = SimpleNamespace(base_url="http://testserver/", headers={}, client=None)
        for method, section in (("GET", "params"), ("POST", "body")):
            operation = {"api_id": f"{method} /api/capacity/water/records"}
            for invalid in ("ALL", "CAMPUS", "G", "110", "A,B", "ABCDE", ["A", "B"], {"scope": "A"}):
                with self.subTest(method=method, scope=invalid), self.assertRaises(AssistantError):
                    asyncio.run(catalog.invoke({**operation, section: {"scope": invalid}}, request))
            for empty in ({}, {"scope": ""}):
                normalized, missing = catalog.validate_operation({**operation, section: empty})
                self.assertEqual(len(missing), 1)
                self.assertEqual((missing[0]["path"], missing[0]["section"], missing[0]["type"]), ("scope", section, "select"))
                self.assertEqual([option["value"] for option in missing[0]["options"]], list("ABCDEH"))
                self.assertFalse(normalized[section].get("scope"))
                with self.assertRaises(AssistantError):
                    asyncio.run(catalog.invoke(normalized, request))
            wrong_section = "body" if section == "params" else "params"
            _, missing = catalog.validate_operation({**operation, wrong_section: {"scope": "B"}})
            self.assertTrue(any(field["path"] == "scope" and field["section"] == section for field in missing))
        self.assertEqual(calls, [])
        for method, section, scope in (("GET", "params", "H"), ("POST", "body", "B")):
            result = asyncio.run(catalog.invoke({"api_id": f"{method} /api/capacity/water/records", section: {"scope": scope}}, request))
            self.assertEqual(result["data"]["scope"], scope)
        self.assertEqual(calls, ["H", "B"])

    def test_single_scope_preserves_native_actor_and_record_defaults(self):
        catalog = _native_route_catalog()
        for operation in (
            {"api_id": "GET /api/learning/papers"},
            {"api_id": "GET /api/learning/profile"},
            {"api_id": "GET /api/learning/papers/{id}", "path_params": {"id": "paper-fixture"}},
            {"api_id": "POST /api/learning/papers/{id}/reveal", "path_params": {"id": "paper-fixture"}, "body": {"question_id": "question-fixture"}},
            {"api_id": "POST /api/learning/issues", "body": {"paper_id": "paper-fixture", "question_id": "question-fixture", "description": "fixture"}},
            {"api_id": "GET /api/drills"},
            {"api_id": "GET /api/critical-guard/tasks", "params": {"admin": "1"}},
        ):
            with self.subTest(api_id=operation["api_id"]):
                normalized, missing = catalog.validate_operation(operation)
                self.assertEqual(missing, [])
                self.assertNotIn("scope", normalized["body"])
                self.assertNotIn("scope", normalized["params"])
        for api_id, path_params, section in (
            ("GET /api/learning/papers", {}, "params"),
            ("POST /api/learning/issues", {}, "body"),
            ("GET /api/drills", {}, "params"),
            ("PUT /api/drills/{drill_id}/execution", {"drill_id": "fixture"}, "params"),
            ("GET /api/critical-guard/tasks", {}, "params"),
            ("PUT /api/critical-guard/responses/{response_id}", {"response_id": "fixture"}, "body"),
        ):
            with self.subTest(api_id=api_id), self.assertRaises(AssistantError):
                catalog.validate_operation({"api_id": api_id, "path_params": path_params, section: {"scope": "ALL"}})
        for api_id in ("GET /api/drills", "GET /api/critical-guard/tasks"):
            with self.assertRaises(AssistantError):
                catalog.validate_operation({"api_id": api_id, "params": {"scope": "H"}})
        _, missing = catalog.validate_operation({"api_id": "GET /api/critical-guard/tasks/{task_id}",
                                                "path_params": {"task_id": "fixture"}, "params": {"admin": "1"}})
        self.assertTrue(any(field["path"] == "scope" for field in missing))

    def test_plan_dispatch_schema_and_excel_row_metadata(self):
        catalog = _native_route_catalog()
        self.assertEqual(catalog.get("POST /api/plan-convergence/rulesets")["schema"]["body"]["required"], ["name"])
        update = catalog.get("PUT /api/plan-convergence/rulesets/{id}")["schema"]["body"]
        self.assertEqual(update["required"], ["items"])
        item = update["properties"]["items"]["items"]["properties"]
        self.assertTrue({"scope_type", "obj_name", "inst_name", "point_name", "rule_group_no", "rule_type", "rule_label"} <= item.keys())
        _, missing = catalog.validate_operation({"api_id": "PUT /api/plan-convergence/rulesets/{id}", "path_params": {"id": "1"}, "body": {"items": []}})
        self.assertEqual(missing, [])
        match = catalog.get("POST /api/plan-convergence/rulesets/{id}/match")["schema"]["body"]
        self.assertEqual(match["required"], [])
        self.assertEqual(set(match["properties"]), {"block_id", "details"})
        compare = catalog.get("POST /api/plan-convergence/compare")["schema"]["body"]
        scenario = compare["properties"]["scenarios"]
        self.assertEqual(scenario["maxItems"], 1)
        self.assertEqual(scenario["items"]["required"], ["scenario_name", "rows"])
        row = {key: "fixture" for key in ("设备域", "关联资源", "关联设备", "关联告警规则")}
        operation = {"api_id": "POST /api/plan-convergence/compare", "body": {
            "block_id": "1", "scenarios": [{"scenario_name": "场景", "rows": [{**row, "_excel_row": 2}]}]}}
        normalized, missing = catalog.validate_operation(operation)
        self.assertEqual(missing, [])
        self.assertEqual(normalized["body"]["scenarios"][0]["rows"][0]["_excel_row"], 2)
        for key, value in (("_actor", "fake"), ("_excel_row", True), ("_excel_row", 1), ("_excel_row", {"_actor": "fake"})):
            with self.subTest(key=key, value=value), self.assertRaises(AssistantError):
                catalog.validate_operation({**operation, "body": {"block_id": "1", "scenarios": [{"scenario_name": "场景", "rows": [{**row, key: value}]}]}})
        with self.assertRaises(AssistantError):
            catalog.validate_operation({"api_id": "POST /api/learning/questions", "body": {"options": [{"_excel_row": 2}]}})

    def test_excel_raw_transport_uses_owned_file_and_native_parser(self):
        from types import SimpleNamespace
        from urllib.parse import quote, unquote
        from unittest.mock import Mock
        from openpyxl import Workbook
        from lan_bitable_template_portal.lighthouse_files import LighthouseFiles
        from lan_bitable_template_portal.plan_convergence import PlanConvergenceService

        output = io.BytesIO()
        workbook = Workbook()
        workbook.active.title = "场景"
        workbook.active.append(["设备域", "关联资源", "关联设备", "关联告警规则"])
        workbook.active.append(["fixture", "A楼", "测试设备", "测试规则"])
        workbook.save(output)
        workbook.close()
        raw = output.getvalue()
        received = []
        app = FastAPI()

        @app.post("/api/plan-convergence/excel")
        async def excel(request: Request):
            self.assertEqual(request.headers.get("cookie"), "session=test-session")
            self.assertEqual(request.headers.get("origin"), "http://testserver")
            content = await request.body()
            received.append((content, request.headers.get("x-filename"), request.headers.get("content-type")))
            return {"ok": True, "data": PlanConvergenceService.parse_excel(content, unquote(request.headers.get("x-filename", "")))}

        catalog = PortalAPICatalog(app)
        descriptor = catalog.get("POST /api/plan-convergence/excel")
        self.assertTrue(descriptor["read_only"])
        self.assertEqual(descriptor["upload_format"], "raw-excel")
        self.assertFalse(descriptor["multipart"])
        request = SimpleNamespace(base_url="http://testserver/", headers={"cookie": "session=test-session", "origin": "http://testserver"}, client=None)
        actor = {"id": "file-owner", "scopes": ["A"]}
        docs = {}
        store = Mock()
        store.put_document.side_effect = lambda namespace, key, value: docs.__setitem__((namespace, key), value)
        store.get_document.side_effect = lambda namespace, key: docs.get((namespace, key))
        with tempfile.TemporaryDirectory() as root:
            files = LighthouseFiles(store, root=root)
            filename = "收敛 场景%测试.xlsx"
            owned = files.upload(actor, filename, raw, extract=False)
            operation = {"api_id": descriptor["id"], "files": {"file": [owned["id"]]}}
            result = asyncio.run(catalog.invoke(operation, request, file_provider=lambda fid: files.get(actor, fid)))
            self.assertTrue(result["ok"], result)
            self.assertEqual(received, [(raw, quote(filename, safe=""), "application/octet-stream")])
            self.assertEqual(result["_raw"]["sheets"][0]["rows"][0]["_excel_row"], 2)
            foreign = files.upload({"id": "other-owner", "scopes": ["A"]}, filename, raw, extract=False)
            revoked = files.upload(actor, filename, raw, extract=False, source_scopes=["B"])
            for identity in (foreign["id"], revoked["id"], "0" * 32):
                with self.subTest(identity=identity), self.assertRaises(AssistantError):
                    asyncio.run(catalog.invoke({**operation, "files": {"file": [identity]}}, request, file_provider=lambda fid: files.get(actor, fid)))
            oversized = files.upload(actor, filename, b"PK" + b"x" * (8 * 1024 * 1024), extract=False)
            with self.assertRaises(AssistantError) as error:
                asyncio.run(catalog.invoke({**operation, "files": {"file": [oversized["id"]]}}, request, file_provider=lambda fid: files.get(actor, fid)))
            self.assertEqual(error.exception.status, 413)
            self.assertEqual(len(received), 1)

    def test_excel_raw_boundaries_and_exact_endpoint(self):
        from types import SimpleNamespace
        from unittest.mock import Mock

        app = FastAPI()
        received = []

        @app.post("/api/plan-convergence/excel")
        async def excel(request: Request):
            received.append(await request.body())
            return {"ok": True}

        @app.post("/api/fixture/excel")
        async def other(request: Request):
            return {"ok": True, "data": {"content_type": request.headers.get("content-type"), "body": await request.json()}}

        catalog = PortalAPICatalog(app)
        request = SimpleNamespace(base_url="http://testserver/", headers={}, client=None)
        operation = {"api_id": "POST /api/plan-convergence/excel", "files": {"file": ["fixture"]}}
        for fields in ({}, {"file": ["one", "two"]}, {"file": ["one"], "other": ["two"]}, {"other": ["one"]}):
            provider = Mock()
            with self.subTest(files=fields), self.assertRaises(AssistantError):
                asyncio.run(catalog.invoke({**operation, "files": fields}, request, file_provider=provider))
            provider.assert_not_called()
        for metadata in (None, {"name": "a.csv", "bytes": b"PK"}, {"name": "a.xlsx", "bytes": b"text"},
                         {"name": "a.xlsx", "bytes": "PK"}, {"name": "a.xlsx"},
                         {"name": "a.xlsx", "bytes": b"PK" + b"x" * (8 * 1024 * 1024)}):
            with self.subTest(name=metadata.get("name") if metadata else None), self.assertRaises(AssistantError):
                asyncio.run(catalog.invoke(operation, request, file_provider=Mock(return_value=metadata)))
        self.assertEqual(received, [])
        result = asyncio.run(catalog.invoke(operation, request, file_provider=Mock(return_value={"name": "场景.xlsm", "bytes": b"PK-fixture"})))
        self.assertTrue(result["ok"])
        self.assertEqual(received, [b"PK-fixture"])
        provider = Mock()
        result = asyncio.run(catalog.invoke({**operation, "api_id": "POST /api/fixture/excel", "upload_format": "raw-excel", "body": {"value": 1}}, request, file_provider=provider))
        provider.assert_not_called()
        self.assertEqual(result["data"], {"content_type": "application/json", "body": {"value": 1}})

    # -- required-body schema ---------------------------------------------
    def test_required_body_schema_from_pydantic(self):
        desc = self.catalog.get("POST /api/workbench/action")
        schema = desc["schema"]["body"]
        self.assertIn("scope", schema["required"])
        self.assertNotIn("operation_id", schema["required"])  # has default
        # defaults should be stripped from public descriptor
        self.assertNotIn("default", json.dumps(schema, ensure_ascii=False))

    # -- missing fields ----------------------------------------------------
    def test_missing_required_scalars_returned_not_raised(self):
        norm, missing = self.catalog.validate_operation({
            "api_id": "POST /api/workbench/action",
            "body": {"work_type": "maintenance"},
        })
        self.assertTrue(missing)
        field = next((f for f in missing if f["path"] == "scope"), None)
        self.assertIsNotNone(field)
        self.assertTrue(field["required"])
        self.assertEqual(field["label"], "楼栋范围")
        self.assertEqual(norm["body"]["work_type"], "maintenance")

    def test_missing_path_param_returns_missing_field(self):
        desc_app = FastAPI()

        @desc_app.get("/api/repair/records/{record_id}")
        async def get_record(request: Request, record_id: str):
            return {"ok": True, "data": {"id": record_id}}

        catalog = PortalAPICatalog(desc_app)
        norm, missing = catalog.validate_operation({
            "api_id": "GET /api/repair/records/{record_id}",
        })
        self.assertEqual(norm["path"], "/api/repair/records/{record_id}")
        field = next((f for f in missing if f["name"] == "record_id"), None)
        self.assertIsNotNone(field)
        self.assertEqual(field["section"], "path_params")
        self.assertTrue(field["required"])

    def test_required_query_and_file_fields_are_missing(self):
        # Required query param on /api/repair/query
        _, missing_q = self.catalog.validate_operation({"api_id": "GET /api/repair/query"})
        qf = next((f for f in missing_q if f["section"] == "params" and f["name"] == "scope"), None)
        self.assertIsNotNone(qf, missing_q)
        self.assertTrue(qf["required"])

        # Required UploadFile on /api/engineer/mop/upload
        _, missing_f = self.catalog.validate_operation({
            "api_id": "POST /api/engineer/mop/upload",
            "body": {"note": "x"},
        })
        ff = next((f for f in missing_f if f["section"] == "files" and f["name"] == "file"), None)
        self.assertIsNotNone(ff, missing_f)
        self.assertEqual(ff["type"], "file")

    def test_frontend_missing_field_types(self):
        _, missing = self.catalog.validate_operation({
            "api_id": "POST /api/status/update",
            "body": {"scope": "A"},
        })
        status = next(f for f in missing if f["path"] == "status")
        self.assertEqual(status["type"], "select")
        self.assertTrue(isinstance(status["options"], list))
        self.assertIn("value", status["options"][0])
        self.assertIn("label", status["options"][0])

        _, missing_a = self.catalog.validate_operation({
            "api_id": "POST /api/attachment/bind",
            "body": {"scope": "A"},
        })
        ids = next(f for f in missing_a if f["path"] == "attachment_ids")
        self.assertEqual(ids["type"], "textarea")
        self.assertEqual(ids["value_format"], "json")

    def test_full_required_body_passes_validation(self):
        norm, missing = self.catalog.validate_operation({
            "api_id": "POST /api/workbench/action",
            "body": {"scope": "A", "work_type": "maintenance"},
        })
        self.assertEqual(missing, [])
        self.assertEqual(norm["body"]["scope"], "A")

    # -- optional fields ---------------------------------------------------
    def test_optional_fields_not_required(self):
        desc = self.catalog.get("POST /api/capacity/water/records")
        schema = desc["schema"]["body"]
        self.assertIn("meter_value", schema["properties"])
        self.assertNotIn("meter_value", schema["required"])
        norm, missing = self.catalog.validate_operation({
            "api_id": "POST /api/capacity/water/records",
            "body": {"scope": "B", "meter": "M-1"},
        })
        self.assertEqual(missing, [])

    def test_generic_endpoint_fields_are_optional(self):
        desc = self.catalog.get("POST /api/daily/send")
        schema = desc["schema"]["body"]
        self.assertIn("text", schema["properties"])
        self.assertEqual(schema["required"], [])  # must not mark all as required

    # -- read-only vs risk -------------------------------------------------
    def test_read_only_vs_risk_classification(self):
        self.assertTrue(self.catalog.get("GET /api/notice/list")["read_only"])
        self.assertEqual(self.catalog.get("GET /api/notice/list")["risk"], "normal")
        refresh = self.catalog.get("GET /api/notice/refresh")
        self.assertFalse(refresh["read_only"])
        self.assertEqual(refresh["risk"], "high")
        self.assertFalse(self.catalog.get("POST /api/workbench/action")["read_only"])
        self.assertEqual(self.catalog.get("POST /api/workbench/action")["risk"], "high")

    def test_readonly_post_allowlist(self):
        candidate = self.catalog.get("POST /api/notice-target-candidates")
        self.assertTrue(candidate["read_only"])
        self.assertEqual(candidate["risk"], "normal")
        change = self.catalog.get("POST /api/change-target-candidates")
        self.assertTrue(change["read_only"])
        prefill = self.catalog.get("POST /api/repair-management/prefill")
        self.assertTrue(prefill["read_only"])
        # Business writes remain non-read-only.
        self.assertFalse(self.catalog.get("POST /api/capacity/water/records")["read_only"])
        # Unknown POST /.../preview that actually mutates is NOT read-only.
        misleading = self.catalog.get("POST /api/repair/records/apply-preview")
        self.assertFalse(misleading["read_only"])
        self.assertEqual(misleading["risk"], "high")

    def test_low_risk_writes_are_normal(self):
        self.assertEqual(self.catalog.get("POST /api/engineer/mop/upload")["risk"], "normal")
        self.assertFalse(self.catalog.get("POST /api/engineer/mop/upload")["read_only"])

    # -- real GET route regression (business domain != action segment) -------
    def test_real_business_get_paths_are_readonly(self):
        catalog = PortalAPICatalog(_dummy_get_app())
        for api_id in (
                "GET /api/repair-management/records",
                "GET /api/repair-management/overview",
                "GET /api/repair-management/event-candidates",
                "GET /api/repair-management/cmdb-cache/status",
                "GET /api/repair-management/sync-status",
                "GET /api/critical-guard",
        ):
            desc = catalog.get(api_id)
            self.assertTrue(desc["read_only"], api_id)
            self.assertEqual(desc["risk"], "normal", api_id)

    def test_real_get_refresh_variants_are_operations(self):
        catalog = PortalAPICatalog(_dummy_get_app())
        for api_id in (
                "GET /api/repair-refresh",
                "GET /api/maintenance-refresh",
                "GET /api/change-refresh",
                "GET /api/notice/refresh",
        ):
            desc = catalog.get(api_id)
            self.assertFalse(desc["read_only"], api_id)
            self.assertEqual(desc["risk"], "high", api_id)

    def test_refresh_status_tail_stays_readonly(self):
        # ``refresh/status`` must never be treated as the ``refresh`` operation.
        app = FastAPI()

        @app.get("/api/repair-management/cmdb-cache/status")
        async def status_query(request: Request):
            return {"ok": True}

        catalog = PortalAPICatalog(app)
        desc = catalog.get("GET /api/repair-management/cmdb-cache/status")
        self.assertTrue(desc["read_only"])
        self.assertEqual(desc["risk"], "normal")

    def test_real_module_context_names(self):
        catalog = PortalAPICatalog(_dummy_get_app())
        self.assertEqual(catalog.get("GET /api/repair-management/records")["name"], "查询维修单记录")
        self.assertEqual(catalog.get("GET /api/repair-management/overview")["name"], "查询维修单概览")
        self.assertEqual(catalog.get("GET /api/repair-refresh")["name"], "刷新维修")
        self.assertEqual(catalog.get("GET /api/maintenance-refresh")["name"], "刷新维保")

        cab = FastAPI()

        @cab.get("/api/cabinet-power/batches")
        async def batches(request: Request):
            return {"ok": True}

        cab_catalog = PortalAPICatalog(cab)
        self.assertEqual(cab_catalog.get("GET /api/cabinet-power/batches")["name"], "查询机柜批量")

    # -- native FastAPI body model via dependant.body_params ----------------
    def test_native_fastapi_body_model_supported(self):
        class NativeReq(BaseModel):
            scope: str
            note: str = Field(default="")

        app = FastAPI()

        @app.post("/api/native-body")
        async def native(body: NativeReq, request: Request):
            return {"ok": True}

        catalog = PortalAPICatalog(app)
        desc = catalog.get("POST /api/native-body")
        body = desc["schema"]["body"]
        self.assertIn("scope", body["required"])
        self.assertNotIn("note", body["required"])
        self.assertIn("scope", body["properties"])

    # -- shared dispatch routes expose optional body fields ------------------
    def test_shared_dispatch_get_body_is_optional_and_readonly(self):
        app = FastAPI()

        async def learning_endpoint(request: Request):
            payload = {}
            if request.method != "GET":
                payload = await request.json()
            if request.method == "POST":
                return {"ok": True, "data": {"q": payload.get("question")}}
            return {"ok": True, "data": payload.get("scope")}

        async def plan_endpoint(request: Request):
            payload = {}
            if request.method != "GET":
                payload = await request.json()
            if request.method == "POST":
                return {"ok": True, "data": payload.get("items")}
            return {"ok": True, "data": payload.get("name")}

        app.add_api_route("/api/learning/papers", learning_endpoint, methods=["GET"])
        app.add_api_route("/api/learning/refresh", learning_endpoint, methods=["POST"])
        app.add_api_route("/api/plan-convergence/rulesets", plan_endpoint, methods=["GET", "POST"])

        catalog = PortalAPICatalog(app)
        get_learning = catalog.get("GET /api/learning/papers")
        self.assertTrue(get_learning["read_only"])
        self.assertEqual(get_learning["schema"]["body"]["required"], [])
        self.assertIn("scope", get_learning["schema"]["body"]["properties"])

        get_plan = catalog.get("GET /api/plan-convergence/rulesets")
        self.assertTrue(get_plan["read_only"])
        self.assertEqual(get_plan["schema"]["body"]["required"], [])
        # POST refresh remains a write (not read-only, missing fields never fabricated).
        self.assertFalse(catalog.get("POST /api/learning/refresh")["read_only"])

    # -- dynamically composed cabinet add_api_route coverage ----------------
    def test_cabinet_dynamic_add_api_route_covered(self):
        app = FastAPI()

        async def cabinet_endpoint(request: Request):
            return {"ok": True}

        # These are registered through add_api_route just like the real cabinet
        # router; the full literal is not greppable from a single source slice.
        app.add_api_route("/api/cabinet-power/batches/text-preview", cabinet_endpoint, methods=["POST"])
        app.add_api_route("/api/cabinet-power/batches/{batch_id}/status", cabinet_endpoint, methods=["GET"])
        app.add_api_route("/api/cabinet-power/batches", cabinet_endpoint, methods=["GET"])

        catalog = PortalAPICatalog(app)
        text_preview = catalog.get("POST /api/cabinet-power/batches/text-preview")
        self.assertTrue(text_preview["read_only"])  # verified parser-only endpoint
        self.assertEqual(text_preview["name"], "文本预览")
        batch_status = catalog.get("GET /api/cabinet-power/batches/{batch_id}/status")
        self.assertTrue(batch_status["read_only"])
        self.assertEqual(batch_status["risk"], "normal")

    def test_native_multipart_required_fields_are_in_catalog_and_validation(self):
        app = FastAPI()

        @app.post("/api/fixture-upload")
        async def upload(request: Request, expected_version: int = Form(...), file: UploadFile = File(...)):
            return {"ok": True, "data": {"version": expected_version, "content": (await file.read()).decode()}}

        catalog = PortalAPICatalog(app)
        descriptor = catalog.get("POST /api/fixture-upload")
        self.assertEqual(descriptor["schema"]["body"]["required"], ["expected_version"])
        _, missing = catalog.validate_operation({"api_id": descriptor["id"]})
        self.assertEqual({field["path"] for field in missing}, {"file", "expected_version"})
        _, missing = catalog.validate_operation({"api_id": descriptor["id"], "body": {"expected_version": 2}, "files": {"file": ["fixture"]}})
        self.assertEqual(missing, [])

    def test_cabinet_metadata_requires_real_identity_and_preserves_proof_scopes(self):
        app = FastAPI()

        @app.post("/api/cabinet-power/operations")
        async def operation(request: Request):
            return {"ok": True}

        catalog = PortalAPICatalog(app)
        _, missing = catalog.validate_operation({"api_id": "POST /api/cabinet-power/operations"})
        self.assertTrue({"scope", "operation_id", "room", "rack", "rack_type", "action", "actual", "result"} <= {field["path"] for field in missing})
        body = {"scope": "E", "operation_id": "cabinet_operation_0001", "room": "202", "rack": "B17", "rack_type": "", "groups": [{"action": "上测试电", "actual": "2026-09-30 09:00", "result": "成功", "evidence_files": [{"scopes": ["E"], "file_token": "private-proof-token"}]}]}
        normalized, missing = catalog.validate_operation({"api_id": "POST /api/cabinet-power/operations", "body": body})
        self.assertEqual(missing, [])
        self.assertEqual(normalized["body"]["groups"], body["groups"])
        self.assertEqual(normalized["body"]["rack_type"], "")

    # -- cabinet shared batch/rack/export schema (untyped dispatchers) ---------
    def test_cabinet_shared_schema_keeps_real_json_types(self):
        catalog = _native_route_catalog()
        create = catalog.get("POST /api/cabinet-power/batches")
        body_schema = create["schema"]["body"]["properties"]
        self.assertEqual(body_schema["source"]["type"], "string")
        self.assertEqual(sorted(body_schema["source"]["enum"]), ["image", "text"])
        self.assertEqual(body_schema["request_id"]["type"], "string")
        self.assertEqual(body_schema["sources"]["type"], "array")
        self.assertEqual(body_schema["rows"]["type"], "array")
        # Arrays/objects must render as JSON textareas, never plain string.
        self.assertEqual(_frontend_field(body_schema["sources"], body_schema)["type"], "textarea")
        self.assertEqual(_frontend_field(body_schema["sources"], body_schema)["value_format"], "json")

        update = catalog.get("PATCH /api/cabinet-power/batches/{batch_id}")
        update_props = update["schema"]["body"]["properties"]
        self.assertEqual(update_props["version"]["type"], "integer")
        self.assertEqual(update_props["common"]["type"], "object")
        self.assertEqual(update_props["rows"]["type"], "array")
        self.assertEqual(update_props["row_ids"]["type"], "array")
        self.assertEqual(update_props["acknowledge_warnings"]["type"], "boolean")

        confirm = catalog.get("POST /api/cabinet-power/batches/{batch_id}/confirm")
        confirm_props = confirm["schema"]["body"]["properties"]
        self.assertEqual(confirm_props["all"]["type"], "boolean")
        self.assertEqual(confirm_props["row_ids"]["type"], "array")

    def test_cabinet_batch_validate_operation_preserves_json_shapes(self):
        catalog = _native_route_catalog()
        # Real text-create shape: request_id/sources/rows must survive as real shapes.
        body = {
            "source": "text",
            "request_id": "client_req_20261001080000_0001",
            "sources": [{"id": "abcdefgh12345678", "text": "202包间\nB17 新增机柜"}],
            "rows": [{"text_id": "abcdefgh12345678", "text_row": 2, "room": "202", "rack": "B17"}],
        }
        normalized, missing = catalog.validate_operation(
            {"api_id": "POST /api/cabinet-power/batches", "body": body})
        self.assertEqual(missing, [])
        self.assertIsInstance(normalized["body"]["sources"], list)
        self.assertIsInstance(normalized["body"]["rows"], list)
        self.assertEqual(normalized["body"]["request_id"], body["request_id"])

        # confirm: version integer and all boolean stay typed.
        normalized, missing = catalog.validate_operation({
            "api_id": "POST /api/cabinet-power/batches/{batch_id}/confirm",
            "path_params": {"batch_id": "b1"},
            "body": {"version": 5, "all": True}})
        self.assertEqual(missing, [])
        self.assertEqual(normalized["body"]["version"], 5)
        self.assertIs(normalized["body"]["all"], True)

        # rack-power: operation_id pattern + number power are not string-inferred.
        normalized, missing = catalog.validate_operation({
            "api_id": "PATCH /api/cabinet-power/rack-power",
            "body": {"scope": "E", "room": "202", "rack": "B17",
                       "power": 12.5, "operation_id": "clean_rack_power_0001"}})
        self.assertEqual(missing, [])
        self.assertEqual(normalized["body"]["power"], 12.5)
        self.assertEqual(normalized["body"]["operation_id"], "clean_rack_power_0001")

    def test_cabinet_shared_missing_version_prompts_number_field(self):
        catalog = _native_route_catalog()
        # Missing version must prompt a numeric field, not a fabricated string zero.
        _, missing = catalog.validate_operation({
            "api_id": "POST /api/cabinet-power/batches/{batch_id}/confirm",
            "path_params": {"batch_id": "b1"}, "body": {"all": True}})
        version_fields = [field for field in missing if field["path"] == "version"]
        self.assertEqual(len(version_fields), 1)
        self.assertEqual(version_fields[0]["type"], "number")
        self.assertTrue(version_fields[0]["required"])

        _, missing = catalog.validate_operation({
            "api_id": "PATCH /api/cabinet-power/batches/{batch_id}",
            "path_params": {"batch_id": "b1"}, "body": {}})
        version_fields = [field for field in missing if field["path"] == "version"]
        self.assertEqual(len(version_fields), 1)
        self.assertEqual(version_fields[0]["type"], "number")

        # export-batches still requires its stable batch identity.
        _, missing = catalog.validate_operation({"api_id": "POST /api/cabinet-power/export-batches"})
        batch_fields = [field for field in missing if field["path"] == "batch_id"]
        self.assertEqual(len(batch_fields), 1)
        self.assertEqual(batch_fields[0]["type"], "text")
        self.assertTrue(batch_fields[0]["required"])

    def test_cabinet_export_batch_id_pattern_preserved(self):
        # Native start_export_batch only accepts all_[a-f0-9]{32}; the late
        # descriptor override must not erase that pattern.
        catalog = _native_route_catalog()
        export = catalog.get("POST /api/cabinet-power/export-batches")
        batch_id = export["schema"]["body"]["properties"]["batch_id"]
        self.assertEqual(batch_id["pattern"], r"^all_[a-f0-9]{32}$")
        self.assertEqual(export["schema"]["body"]["required"], ["batch_id"])
        # Text batches keep their native stable-identifier constraints (not just a loose string).
        batches = catalog.get("POST /api/cabinet-power/batches")
        request_id = batches["schema"]["body"]["properties"]["request_id"]
        self.assertEqual(request_id["minLength"], 16)
        self.assertEqual(request_id["pattern"], r"^[A-Za-z0-9_-]+$")

    def test_cabinet_rack_power_null_clears_but_absent_still_missing(self):
        catalog = _native_route_catalog()
        # Native save_rack_power accepts power:null to clear the reading.
        _, missing = catalog.validate_operation({
            "api_id": "PATCH /api/cabinet-power/rack-power",
            "body": {"scope": "E", "room": "202", "rack": "B17",
                     "power": None, "operation_id": "clean_rack_power_0001"}})
        self.assertEqual([m["path"] for m in missing if m["path"] == "power"], [])
        # An absent power key is still invalid for this non-redundant required field.
        _, missing = catalog.validate_operation({
            "api_id": "PATCH /api/cabinet-power/rack-power",
            "body": {"scope": "E", "room": "202", "rack": "B17",
                     "operation_id": "clean_rack_power_0001"}})
        self.assertIn("power", [m["path"] for m in missing])
        # Non-nullable required values must NOT be relaxed by the null shortcut.
        _, missing = catalog.validate_operation({
            "api_id": "POST /api/cabinet-power/batches/{batch_id}/confirm",
            "path_params": {"batch_id": "b1"},
            "body": {"version": None, "all": True}})
        self.assertIn("version", [m["path"] for m in missing])

    def test_missing_scalars_nullable_anyof_without_nested_prompt(self):
        app = FastAPI()

        @app.post("/api/fixture-nullable")
        async def nullable(body: NullableAnyOfRequest):
            return {"ok": True}

        catalog = PortalAPICatalog(app)
        # Required but nullable anyOf fields: explicit null is complete, absent is missing.
        # cost is an anyOf scalar union; profile is a nullable object (anyOf + $ref).
        _, missing = catalog.validate_operation({
            "api_id": "POST /api/fixture-nullable", "body": {}})
        self.assertEqual(sorted(m["path"] for m in missing), ["cost", "profile", "scope"])
        # Explicit null on both nullable fields must not prompt or recurse into the object.
        _, missing = catalog.validate_operation({
            "api_id": "POST /api/fixture-nullable",
            "body": {"scope": "E", "cost": None, "profile": None}})
        self.assertEqual(missing, [])
        # Non-nullable scope still rejects an explicit null (validated as a 422 type error).
        with self.assertRaises(AssistantError) as ctx:
            catalog.validate_operation({
                "api_id": "POST /api/fixture-nullable",
                "body": {"scope": None, "cost": None, "profile": None}})
        self.assertEqual(ctx.exception.status, 422)
        # A populated nullable object still validates its own required child.
        _, missing = catalog.validate_operation({
            "api_id": "POST /api/fixture-nullable",
            "body": {"scope": "E", "cost": 3, "profile": {"value": 1}}})
        self.assertEqual(missing, [])

    def test_cabinet_correct_image_requires_fields_and_version(self):
        catalog = _native_route_catalog()
        correct = catalog.get(
            "POST /api/cabinet-power/batches/{batch_id}/images/{image_id}/correct")
        # Native contract: fields + version required; candidate_index optional (default -1).
        self.assertEqual(correct["schema"]["body"]["required"], ["version", "fields"])
        self.assertEqual(correct["schema"]["body"]["properties"]["version"]["type"], "integer")
        self.assertEqual(correct["schema"]["body"]["properties"]["candidate_index"]["type"], "integer")
        # Candidate index is an OPTIONAL integer; the catalogue redacts the default
        # key, so the description carries the -1 / manual-correction semantics.
        self.assertNotIn("candidate_index", correct["schema"]["body"]["required"])
        self.assertIn("candidate_index缺省为-1", correct["schema"]["body"]["description"])
        # Missing version or fields must be prompted; fields needs scope/room/rack.
        missing_paths = []
        _, missing = catalog.validate_operation({
            "api_id": "POST /api/cabinet-power/batches/{batch_id}/images/{image_id}/correct",
            "path_params": {"batch_id": "b1", "image_id": "img1"}, "body": {}})
        missing_paths = [m["path"] for m in missing]
        self.assertIn("version", missing_paths)
        self.assertTrue(any(p.startswith("fields.") for p in missing_paths), missing_paths)
        self.assertLessEqual({"scope", "room", "rack"}, set(p.split(".", 1)[1] for p in missing_paths if p.startswith("fields.")))
        # candidate_index absent with valid fields is allowed (native defaults to -1 / manual correction).
        _, missing = catalog.validate_operation({
            "api_id": "POST /api/cabinet-power/batches/{batch_id}/images/{image_id}/correct",
            "path_params": {"batch_id": "b1", "image_id": "img1"},
            "body": {"version": 5, "fields": {"scope": "E", "room": "202", "rack": "B17"}}})
        self.assertEqual(missing, [])
        # retry/restore keep version-only signature.
        retry = catalog.get("POST /api/cabinet-power/batches/{batch_id}/images/{image_id}/retry")
        self.assertEqual(retry["schema"]["body"]["required"], ["version"])
        restore = catalog.get("POST /api/cabinet-power/batches/{batch_id}/images/{image_id}/restore")
        self.assertEqual(restore["schema"]["body"]["required"], ["version"])

    def test_polling_work_orders_execution_routes_excluded(self):
        catalog = _native_route_catalog()
        # Native execution routes must never be in the catalogue.
        for cid in ("GET /api/polling-work-orders/session",
                    "POST /api/polling-work-orders/photo",
                    "GET /api/polling-work-orders/photos/{photo_id}",
                    "POST /api/polling-work-orders/confirm",
                    "POST /api/polling-work-orders/rollback",
                    "POST /api/polling-work-orders/activate",
                    "POST /api/polling-work-orders/release",
                    "POST /api/polling-work-orders/{group_id}/retry-upload",
                    "POST /api/polling-work-orders/{group_id}/resend-links"):
            with self.assertRaises(AssistantError, msg=cid) as ctx:
                catalog.get(cid)
            self.assertEqual(ctx.exception.status, 404, cid)
        # No execution routes are discoverable at all (iterate every page).
        all_ids = []
        listing = catalog.discover(page=1, page_size=50)
        total_pages = (listing["total"] + listing["page_size"] - 1) // listing["page_size"]
        for page in range(1, total_pages + 1):
            listing = catalog.discover(page=page, page_size=50)
            all_ids.extend(desc["id"] for desc in listing["items"])
        self.assertFalse(any("polling-work-orders" in cid for cid in all_ids))
        # SOP CRUD and delay status stay discoverable (read-only workorder status via local queries unaffected).
        for cid in ("GET /api/polling-sops", "POST /api/polling-sops",
                    "PUT /api/polling-sops/{sop_id}", "GET /api/workbench/polling-delay-status"):
            self.assertIsNotNone(catalog.get(cid), cid)

    def test_array_missing_field_keeps_index_and_dates_are_json_serializable(self):
        app = FastAPI()

        @app.post("/api/fixture-dates")
        async def dates(body: DateRows):
            return {"ok": True}

        catalog = PortalAPICatalog(app)
        _, missing = catalog.validate_operation({"api_id": "POST /api/fixture-dates", "body": {"groups": [{}]}})
        self.assertEqual(missing[0]["path"], "groups.0.actual")
        self.assertEqual(missing[0]["type"], "datetime-local")
        normalized, missing = catalog.validate_operation({"api_id": "POST /api/fixture-dates", "body": {"groups": [{"actual": "2026-09-30T10:00:00"}]}})
        json.dumps(normalized["body"])
        self.assertEqual(missing, [])

    def test_calendar_multiselect_and_numeric_constraints_match_native_types(self):
        app = FastAPI()
        @app.post("/api/calendar-choice")
        async def choice(body: CalendarChoiceRequest):
            return {"ok": True}
        catalog = PortalAPICatalog(app)
        _, missing = catalog.validate_operation({"api_id": "POST /api/calendar-choice"})
        fields = {field["path"]: field for field in missing}
        self.assertEqual(fields["drill_date"]["type"], "date")
        self.assertEqual(fields["first_start_time"]["type"], "time")
        self.assertEqual(fields["actual"]["type"], "datetime-local")
        self.assertEqual(fields["selected"]["type"], "multiselect")
        self.assertEqual([item["value"] for item in fields["selected"]["options"]], [1, 2, 3])
        self.assertEqual(fields["selected"]["maxItems"], 2)
        self.assertEqual((fields["progress"]["min"], fields["progress"]["max"], fields["progress"]["step"]), (0, 100, 1))

    def test_required_credential_never_becomes_a_user_input_field(self):
        app = FastAPI()

        @app.post("/api/fixture-secret")
        async def secret(body: SecretInput):
            return {"ok": True}

        catalog = PortalAPICatalog(app)
        descriptor = catalog.get("POST /api/fixture-secret")
        self.assertNotIn("token", descriptor["schema"]["body"]["properties"])
        self.assertNotIn("token", descriptor["schema"]["body"]["required"])
        with self.assertRaisesRegex(AssistantError, "不会收集"):
            catalog.validate_operation({"api_id": descriptor["id"]})

    # -- path traversal / auth spoof --------------------------------------
    def test_path_traversal_rejected(self):
        desc_app = FastAPI()

        @desc_app.get("/api/repair/records/{record_id}")
        async def get_record(request: Request, record_id: str):
            return {"ok": True, "data": {"id": record_id}}

        catalog = PortalAPICatalog(desc_app)
        with self.assertRaises(AssistantError):
            catalog.validate_operation({
                "api_id": "GET /api/repair/records/{record_id}",
                "path_params": {"record_id": "../../etc/passwd"},
            })
        with self.assertRaises(AssistantError):
            catalog.validate_operation({
                "api_id": "GET /api/repair/records/{record_id}",
                "path_params": {"record_id": "https://evil.example/x"},
            })

    def test_auth_spoof_fields_rejected(self):
        for bad in ("_open_id", "open_id", "role", "authorization"):
            with self.assertRaises(AssistantError):
                self.catalog.validate_operation({
                    "api_id": "POST /api/workbench/action",
                    "body": {bad: "forged", "scope": "A"},
                })

    def test_schema_gated_spoof_allowed_when_declared(self):
        # role/open_id are real business fields on this endpoint, so allowed.
        norm, missing = self.catalog.validate_operation({
            "api_id": "POST /api/repair/assignment",
            "body": {"role": "engineer", "open_id": "ou_123", "record_id": "r1"},
        })
        self.assertEqual(missing, [])
        self.assertEqual(norm["body"]["role"], "engineer")
        self.assertEqual(norm["body"]["open_id"], "ou_123")

    def test_no_arbitrary_endpoint(self):
        with self.assertRaises(AssistantError):
            self.catalog.validate_operation({"api_id": "POST /api/custom/made-up"})

    # -- invoke ------------------------------------------------------------
    def test_invoke_forwards_cookie_and_origin(self):
        from unittest.mock import Mock

        request = Mock()
        request.base_url = "http://testserver/"
        request.headers = {"cookie": "session=test-session", "origin": "http://testserver"}
        request.client = ("127.0.0.1", 5000)

        async def run():
            return await self.catalog.invoke({
                "api_id": "POST /api/workbench/action",
                "body": {"scope": "A", "work_type": "maintenance"},
            }, request)

        result = asyncio.run(run())
        self.assertTrue(result["ok"], result)
        self.assertEqual(result["status"], 200)
        # data is unwrapped from the native {ok,data} envelope
        self.assertEqual(result["data"]["scope"], "A")
        # private _raw preserves unredacted native data for result-reference binding
        self.assertEqual(result["_raw"]["scope"], "A")
        self.assertEqual(result["api_id"], "POST /api/workbench/action")

    def test_invoke_denied_when_cookie_missing(self):
        from unittest.mock import Mock

        request = Mock()
        request.base_url = "http://testserver/"
        request.headers = {"origin": "http://testserver"}   # no cookie
        request.client = ("127.0.0.1", 5000)

        async def run():
            return await self.catalog.invoke({
                "api_id": "POST /api/workbench/action",
                "body": {"scope": "A"},
            }, request)

        result = asyncio.run(run())
        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], 401)

    def test_http200_business_failure_not_success(self):
        from unittest.mock import Mock

        request = Mock()
        request.base_url = "http://testserver/"
        request.headers = {"cookie": "session=test-session", "origin": "http://testserver"}
        request.client = ("127.0.0.1", 5000)

        async def run():
            return await self.catalog.invoke({
                "api_id": "POST /api/workbench/soft-fail",
            }, request)

        result = asyncio.run(run())
        self.assertFalse(result["ok"], result)
        self.assertEqual(result["status"], 200)
        self.assertIn("invalid", result["error"])

    def test_invoke_refuses_missing_fields(self):
        from unittest.mock import Mock

        request = Mock()
        request.base_url = "http://testserver/"
        request.headers = {"cookie": "session=test-session", "origin": "http://testserver"}
        request.client = ("127.0.0.1", 5000)

        async def run():
            return await self.catalog.invoke({
                "api_id": "POST /api/workbench/action",
                "body": {"work_type": "maintenance"},
            }, request)

        with self.assertRaises(AssistantError):
            asyncio.run(run())

    def test_invoke_generic_post_and_redaction(self):
        from unittest.mock import Mock

        request = Mock()
        request.base_url = "http://testserver/"
        request.headers = {"cookie": "session=test-session", "origin": "http://testserver"}
        request.client = ("127.0.0.1", 5000)

        async def run():
            return await self.catalog.invoke({
                "api_id": "POST /api/daily/send",
                "body": {"text": "今日工作完成", "recipient": "E12345"},
            }, request)

        result = asyncio.run(run())
        self.assertTrue(result["ok"])
        self.assertEqual(result["data"]["sent_to"], "E12345")

    def test_binary_reply_returns_private_binary_and_metadata(self):
        from unittest.mock import Mock

        request = Mock()
        request.base_url = "http://testserver/"
        request.headers = {"cookie": "session=test-session", "origin": "http://testserver"}
        request.client = ("127.0.0.1", 5000)

        async def run():
            return await self.catalog.invoke({
                "api_id": "GET /api/download-binary",
            }, request)

        result = asyncio.run(run())
        self.assertTrue(result["ok"])
        data = result["data"]
        self.assertEqual(data["name"], "report.png")
        self.assertEqual(data["mime"], "image/png")
        self.assertEqual(data["size"], len(b"\x89PNG\r\n\x1a\nbinary-data"))
        self.assertNotIn("content", data)
        # raw binary lives only in the private underscore key, never serialized
        self.assertIn("_binary", result)
        self.assertEqual(result["_binary"]["content"], b"\x89PNG\r\n\x1a\nbinary-data")
        self.assertEqual(result["_binary"]["mime"], "image/png")
        # public payload never contains bytes
        serialized = json.dumps(data, ensure_ascii=False)
        self.assertNotIn(b"\x89PNG", serialized.encode())
        self.assertNotIn("content", serialized)

    def test_binary_over_20MiB_rejected(self):
        from unittest.mock import Mock

        request = Mock()
        request.base_url = "http://testserver/"
        request.headers = {"cookie": "session=test-session", "origin": "http://testserver"}
        request.client = ("127.0.0.1", 5000)

        async def run():
            return await self.catalog.invoke({
                "api_id": "GET /api/big-binary",
            }, request)

        with self.assertRaises(AssistantError):
            asyncio.run(run())

    def test_large_list_truncated_but_valid_and_total_retained(self):
        from unittest.mock import Mock

        request = Mock()
        request.base_url = "http://testserver/"
        request.headers = {"cookie": "session=test-session", "origin": "http://testserver"}
        request.client = ("127.0.0.1", 5000)

        async def run():
            return await self.catalog.invoke({
                "api_id": "GET /api/notice/large",
            }, request)

        result = asyncio.run(run())
        self.assertTrue(result["ok"])
        self.assertTrue(result["truncated"])
        # safe_data caps the list at 40 while keeping count/total fields
        self.assertEqual(len(result["data"]["items"]), 40)
        self.assertEqual(result["data"]["total"], 60)
        self.assertEqual(result["data"]["count"], 60)
        # never parse sliced JSON: result is always valid JSON-serializable
        json.dumps(result["data"])

    def test_multipart_owner_provider(self):
        from unittest.mock import Mock

        request = Mock()
        request.base_url = "http://testserver/"
        request.headers = {"cookie": "session=test-session", "origin": "http://testserver"}
        request.client = ("127.0.0.1", 5000)

        provider = Mock()
        provider.side_effect = lambda aid: {"name": "evidence.png",
                                            "mime": "image/png",
                                            "bytes": b"fake-png-data"}

        async def run():
            return await self.catalog.invoke({
                "api_id": "POST /api/engineer/mop/upload",
                "body": {"note": "测试"},
                "files": {"file": ["attachment-1"]},
            }, request, file_provider=provider)

        result = asyncio.run(run())
        self.assertTrue(result["ok"], result)
        self.assertEqual(result["data"]["size"], len(b"fake-png-data"))
        self.assertEqual(result["data"]["note"], "测试")
        provider.assert_called_once_with("attachment-1")

    def test_multipart_large_file_rejected(self):
        from unittest.mock import Mock

        request = Mock()
        request.base_url = "http://testserver/"
        request.headers = {"cookie": "session=test-session", "origin": "http://testserver"}
        request.client = ("127.0.0.1", 5000)

        provider = Mock(return_value={"name": "big.png", "mime": "image/png",
                                      "bytes": b"x" * (20 * 1024 * 1024 + 10)})

        async def run():
            return await self.catalog.invoke({
                "api_id": "POST /api/engineer/mop/upload",
                "body": {"note": "x"},
                "files": {"file": ["attachment-2"]},
            }, request, file_provider=provider)

        with self.assertRaises(AssistantError):
            asyncio.run(run())

    def test_real_typed_post_only_on_invoke(self):
        """validate_operation must never write or break types; invoke performs it."""
        norm, missing = self.catalog.validate_operation({
            "api_id": "POST /api/capacity/water/records",
            "body": {"scope": "A", "meter": "m1", "meter_value": 12},
        })
        self.assertEqual(missing, [])
        self.assertEqual(norm["body"]["meter_value"], 12)
        # No side effect during validation: a plain repro validate does not call the endpoint.
        self.assertEqual(len(self.catalog._descriptors), len(self.catalog.discover(page_size=10 ** 6)["items"]))

    # -- redaction ---------------------------------------------------------
    def test_redaction_scales_to_model(self):
        from unittest.mock import Mock

        request = Mock()
        request.base_url = "http://testserver/"
        request.headers = {"cookie": "session=test-session", "origin": "http://testserver"}
        request.client = ("127.0.0.1", 5000)

        async def run():
            return await self.catalog.invoke({
                "api_id": "GET /api/repair/people",
            }, request)

        result = asyncio.run(run())
        serialized = json.dumps(result["data"], ensure_ascii=False)
        self.assertIn("张三", serialized)
        self.assertIn("E12345", serialized)
        self.assertNotIn("13812345678", serialized)
        self.assertNotIn("110101199001011234", serialized)

    # -- parse_notice helper -----------------------------------------------
    def test_parse_notice_virtual_entry_and_helper(self):
        desc = self.catalog.get("POST /api/_assistant/parse-notice")
        self.assertTrue(desc["assistant_only"])
        self.assertTrue(desc["read_only"])
        parsed = self.catalog.parse_notice("【维保通告】A楼柴发维保\n原因：设备老化")
        self.assertIn("work_type", parsed)
        self.assertIn("action", parsed)
        self.assertIn("draft", parsed)


if __name__ == "__main__":
    unittest.main()
