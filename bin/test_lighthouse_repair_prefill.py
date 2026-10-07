"""Repair assistant prefill / preview-relations fidelity.

Focused isolated tests for ``PortalAgent.preview_repair`` against the real
``PortalAgent`` + ``PortalAPICatalog`` with a fake in-process Pydantic-validated
native ``POST /api/repair-management/prefill`` endpoint plus the existing
repair-management record/followup endpoints.  Uses only Store/ACTOR (not any
test class) from test_lighthouse_agent_workflows as helpers.  No real
cloud/provider access, no plan/business writes, no product edits.

Each test asserts a declared fidelity contract and reports the real outcome
against the current build:

  1. preview is a readonly gate: valid loaded event/repair succeeds, forged /
     other-scope / wrong-version / cancelled / non-record / max-1 / mismatched
     event-context payloads are rejected BEFORE any native read,
  2. native success never mutates the plan and returns exactly the fields that
     the linked/unlinked native repair descriptor declares,
  3. readonly / credential / attachment names are stripped even if the native
     prefill returns them,
  4. native HTTP failures surface as-is,
  5. a stale plan version/status after the awaited native query rejects the
     response,
  6. empty event+repair clears through an empty readonly response with no
     native call.
"""
import copy
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import httpx
sys.path.insert(0, str(Path(__file__).resolve().parent))

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from clipflow_backend.api_models import (RepairFollowupRecordRequest,
                                         RepairManagementPrefillRequest,
                                         RepairManagementRecordRequest)

from lan_bitable_template_portal.lighthouse_agent import PLAN_NAMESPACE, PortalAgent
from lan_bitable_template_portal.lighthouse_ai import AssistantError, LighthouseAssistant
from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog
from lan_bitable_template_portal.lighthouse_files import LighthouseFiles
from tools.lighthouse_test_backend import install_test_backend as install_lighthouse_routes
from lan_bitable_template_portal.portal_service import REPAIR_MANAGEMENT_TABLE_ID

from test_lighthouse_agent_workflows import ACTOR, Store

OPERATION_ID = "repair_prefill_00000001"

DEFAULT_PREFILL = {
    "fields": {
        "设备名称": "油机A",
        "维修开始时间": "2026-10-01 09:30",
        "故障发生时间": 1790821800000,
        "故障维修原因": "绝缘劣化",
        "证据.说明": {"content": "原证明", "file_token": "private-token"},
        "access_token": "secret-token",
        "attachment": "附件.bin",
    },
    "source_field_names": [
        "设备名称", "维修开始时间", "故障发生时间", "故障维修原因",
        "证据.说明", "access_token", "不存在字段",
    ],
    "warnings": ["来源数据含未落库字段"],
    "event_context_missing": False,
}


def make_request(query_string=b"", path="/api/assistant/agent"):
    return Request({
        "type": "http",
        "scheme": "http",
        "server": ("testserver", 80),
        "client": ("127.0.0.1", 4567),
        "path": path,
        "root_path": "",
        "query_string": query_string,
        "headers": [(b"cookie", b"fixture=a"), (b"origin", b"http://testserver")],
    })


class RepairPrefillBase(unittest.IsolatedAsyncioTestCase):
    """Shared fixture: assistant/files/agent + fake native repair endpoints."""

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

        self.prefill_calls = []
        self.prefill_response = None
        self.prefill_status = 200
        self.prefill_side_effect = None
        self.record_writes = []

        self.app = FastAPI()

        @self.app.post("/api/repair-management/prefill")
        async def prefill(body: RepairManagementPrefillRequest):
            self.prefill_calls.append(body.model_dump())
            if self.prefill_side_effect is not None:
                await self.prefill_side_effect()
            if self.prefill_response is not None:
                if self.prefill_status == 200:
                    return {"ok": True, "data": self.prefill_response}
                return JSONResponse({"ok": False, "error": self.prefill_response},
                                    status_code=self.prefill_status)
            return {"ok": True, "data": copy.deepcopy(DEFAULT_PREFILL)}

        @self.app.post("/api/repair-management/records")
        async def create_project(body: RepairManagementRecordRequest):
            self.record_writes.append(body.model_dump())
            return {"ok": True, "data": {"record_id": "rec-created-project"}}

        @self.app.put("/api/repair-management/records/{record_id}")
        async def update_project(record_id: str, body: RepairManagementRecordRequest):
            self.record_writes.append(body.model_dump())
            return {"ok": True, "data": {"record_id": record_id}}

        @self.app.post("/api/repair-management/followups")
        async def create_followup(body: RepairFollowupRecordRequest):
            self.record_writes.append(body.model_dump())
            return {"ok": True, "data": {"record_id": "rec-created-followup"}}

        @self.app.put("/api/repair-management/followups/{record_id}")
        async def update_followup(record_id: str, body: RepairFollowupRecordRequest):
            self.record_writes.append(body.model_dump())
            return {"ok": True, "data": {"record_id": record_id}}

        self.catalog = PortalAPICatalog(self.app)
        self.agent = PortalAgent(self.assistant, self.catalog, self.files)
        self.request = make_request()

    # ---------- fixture data ----------
    def metas(self):
        return [
            {"field_name": "设备名称", "field_type": 1, "editable": True, "options": []},
            {"field_name": "维修开始时间", "field_type": 5, "editable": True, "options": []},
            {"field_name": "故障发生时间", "field_type": 5, "editable": True, "options": []},
            {"field_name": "故障维修原因", "field_type": 1, "editable": True, "options": []},
            {"field_name": "证据.说明", "field_type": 17, "editable": False, "options": []},
        ]

    def original(self):
        return {
            "record_id": "rec-project",
            "record_version": "pv1",
            "building_codes": ["A"],
            "raw_fields": {
                "设备名称": "原设备",
                "维修开始时间": 0,
                "故障发生时间": 0,
                "故障维修原因": "原原因",
                "证据.说明": {"content": "原证明", "file_token": "t"},
            },
            "source_event_id": "rec-event",
            "source_repair_ids": ["rec-repair"],
        }

    def followup_original(self):
        return {
            "record_id": "rec-followup",
            "record_version": "fv1",
            "building_codes": ["A"],
            "cmdb_record_ids": [],
            "raw_fields": {"设备名称": "原设备", "维修开始时间": 0},
        }

    # ---------- prepare helpers ----------
    def _project_queries(self, original, metas):
        return {"query_" + ("a" * 32): {
            "table_id": REPAIR_MANAGEMENT_TABLE_ID,
            "records": [original],
            "fields": metas,
        }}

    def _prepare_project(self, operation_body=None, original=None):
        body = dict(operation_body or {})
        body.setdefault("scope", "A")
        original = original or self.original()
        return self.agent.prepare(
            ACTOR,
            {"operations": [{
                "api_id": "PUT /api/repair-management/records/{record_id}",
                "path_params": {"record_id": "rec-project"},
                "body": body,
            }]},
            OPERATION_ID, [],
            queries=self._project_queries(original, self.metas()),
        )

    def _prepare_followup(self):
        query = {"query_" + ("b" * 32): {
            "summary_record_id": "rec-parent",
            "relation_mode": "record_id",
            "records": [self.followup_original()],
            "fields": self.metas(),
        }}
        return self.agent.prepare(
            ACTOR,
            {"operations": [{
                "api_id": "PUT /api/repair-management/followups/{record_id}",
                "path_params": {"record_id": "rec-followup"},
                "body": {"scope": "A", "summary_record_id": "rec-parent"},
            }]},
            OPERATION_ID, [],
            queries=query,
        )

    def _payload(self, plan, **overrides):
        body = plan["operations"][0].get("body") or {}
        payload = {
            "version": plan["version"],
            "operation_index": 0,
            "source_event_id": body.get("source_event_id", ""),
            "source_repair_ids": body.get("source_repair_ids", []),
        }
        payload.update(overrides)
        return payload

    def _stored(self, plan_id):
        return self.store.get_document(PLAN_NAMESPACE, plan_id)


class PreviewRepairSuccessTests(RepairPrefillBase):
    """Valid, readonly prefill flows against a linked record plan."""

    async def test_valid_loaded_event_and_repair_prefills_against_native(self):
        plan = self._prepare_project()
        payload = self._payload(plan)
        result = await self.agent.preview_repair(ACTOR, plan["id"], payload, self.request)

        self.assertEqual(result["version"], plan["version"])
        self.assertEqual(len(self.prefill_calls), 1)
        self.assertEqual(self.prefill_calls[0]["source_event_id"], "rec-event")
        self.assertEqual(self.prefill_calls[0]["source_repair_ids"], ["rec-repair"])
        # Only fields declared by the linked native repair descriptor survive.
        self.assertEqual(
            set(result["fields"]),
            {"设备名称", "维修开始时间", "故障发生时间", "故障维修原因"},
        )
        # Warnings come straight from the native payload.
        self.assertEqual(result["warnings"], ["来源数据含未落库字段"])
        self.assertFalse(result["skip"])
        # Controlled field set is the native declaration ∩ editable children.
        self.assertEqual(result["controlled_fields"],
                         ["故障发生时间", "故障维修原因", "维修开始时间"])
        # source_field_names are taken from the native prefill declarations.
        self.assertEqual(result["source_field_names"],
                         ["设备名称", "维修开始时间", "故障发生时间", "故障维修原因"])

    async def test_source_field_names_taken_from_native_declarations_not_fabricated(self):
        plan = self._prepare_project()
        self.prefill_response = {
            "fields": {"设备名称": "油机A"},
            "source_field_names": ["设备名称", "虚构字段", "access_token", "证据.说明"],
        }
        result = await self.agent.preview_repair(
            ACTOR, plan["id"], self._payload(plan), self.request)
        self.assertEqual(result["source_field_names"], ["设备名称"])
        # A fabricated native declaration is never echoed back.
        self.assertNotIn("虚构字段", result["fields"])
        self.assertNotIn("虚构字段", result["source_field_names"])

    async def test_readonly_and_credentials_attachments_are_excluded(self):
        plan = self._prepare_project()
        self.prefill_response = {
            "fields": {
                "设备名称": "油机A",
                "证据.说明": {"content": "原证明"},
                "access_token": "secret-token",
                "attachment": "附件.bin",
                "凭证.访问令牌": "secret",
            },
            "source_field_names": ["设备名称", "证据.说明", "access_token", "attachment"],
        }
        result = await self.agent.preview_repair(
            ACTOR, plan["id"], self._payload(plan), self.request)
        # readonly / credential / attachment names returned by native are dropped.
        self.assertEqual(result["fields"], {"设备名称": "油机A"})
        self.assertEqual(result["source_field_names"], ["设备名称"])
        self.assertNotIn("证据.说明", result["fields"])
        self.assertNotIn("access_token", result["fields"])
        self.assertNotIn("attachment", result["fields"])

    async def test_empty_event_and_repair_clears_without_native_call(self):
        plan = self._prepare_project()
        result = await self.agent.preview_repair(
            ACTOR, plan["id"],
            self._payload(plan, source_event_id="", source_repair_ids=[]),
            self.request)
        self.assertEqual(result["fields"], {})
        self.assertEqual(result["source_field_names"], [])
        self.assertEqual(result["warnings"], [])
        self.assertEqual(self.prefill_calls, [], "empty selection must not hit native prefill")
        self.assertEqual(result["version"], plan["version"])

    async def test_empty_event_via_underscore_placeholder_clears(self):
        plan = self._prepare_project()
        result = await self.agent.preview_repair(
            ACTOR, plan["id"],
            self._payload(plan, source_event_id="__empty__", source_repair_ids=[]),
            self.request)
        self.assertEqual(result["fields"], {})
        self.assertEqual(self.prefill_calls, [])


class PreviewRepairRejectionTests(RepairPrefillBase):
    """Payloads that must be rejected before any native read/mutation."""

    async def test_forged_event_id_rejected_before_reading(self):
        plan = self._prepare_project()
        with self.assertRaises(AssistantError) as cm:
            await self.agent.preview_repair(
                ACTOR, plan["id"],
                self._payload(plan, source_event_id="rec-forged"), self.request)
        self.assertIn("请选择已经读取的关联记录", str(cm.exception))
        self.assertEqual(cm.exception.status, 400)
        self.assertEqual(self.prefill_calls, [], "forged id must be rejected before native read")

    async def test_forged_repair_id_rejected_before_reading(self):
        plan = self._prepare_project()
        with self.assertRaises(AssistantError) as cm:
            await self.agent.preview_repair(
                ACTOR, plan["id"],
                self._payload(plan, source_repair_ids=["rec-forged-repair"]), self.request)
        self.assertIn("请选择已经读取的关联记录", str(cm.exception))
        self.assertEqual(self.prefill_calls, [])

    async def test_other_scope_rejected_before_reading(self):
        plan = self._prepare_project()
        with self.assertRaises(AssistantError) as cm:
            await self.agent.preview_repair(
                ACTOR, plan["id"],
                self._payload(plan, scope="B"), self.request)
        self.assertIn("本轮不能查询选择范围之外的楼栋", str(cm.exception))
        self.assertEqual(cm.exception.status, 403)
        self.assertEqual(self.prefill_calls, [])

    async def test_wrong_version_rejected(self):
        plan = self._prepare_project()
        with self.assertRaises(AssistantError) as cm:
            await self.agent.preview_repair(
                ACTOR, plan["id"],
                self._payload(plan, version=plan["version"] + 1), self.request)
        self.assertIn("填写状态已变化", str(cm.exception))
        self.assertEqual(cm.exception.status, 409)
        self.assertEqual(self.prefill_calls, [])

    async def test_cancelled_plan_rejected(self):
        plan = self._prepare_project()
        mutated = self._stored(plan["id"])
        mutated["status"] = "cancelled"
        self.store.put_document(PLAN_NAMESPACE, plan["id"], mutated)
        with self.assertRaises(AssistantError) as cm:
            await self.agent.preview_repair(
                ACTOR, plan["id"], self._payload(plan), self.request)
        self.assertIn("填写状态已变化", str(cm.exception))
        self.assertEqual(cm.exception.status, 409)
        self.assertEqual(self.prefill_calls, [])

    async def test_non_record_operation_rejected(self):
        plan = self._prepare_followup()
        self.assertEqual(plan["status"], "needs_input")
        with self.assertRaises(AssistantError) as cm:
            await self.agent.preview_repair(
                ACTOR, plan["id"], self._payload(plan), self.request)
        self.assertIn("此操作不支持维修来源预填", str(cm.exception))
        self.assertEqual(cm.exception.status, 400)
        self.assertEqual(self.prefill_calls, [])

    async def test_max_one_repair_id_rejected(self):
        plan = self._prepare_project()
        with self.assertRaises(AssistantError) as cm:
            await self.agent.preview_repair(
                ACTOR, plan["id"],
                self._payload(plan, source_repair_ids=["rec-repair", "rec-repair-2"]),
                self.request)
        self.assertIn("请选择有效的事件及一条检修通告", str(cm.exception))
        self.assertEqual(cm.exception.status, 400)
        self.assertEqual(self.prefill_calls, [])

    async def test_mismatched_event_context_rejected_before_reading(self):
        # Original loaded under event rec-event-1; the model already switched the
        # body to rec-event-2 while keeping repair rec-repair.  preview must reject
        # the stale event context without contacting native.
        original = self.original()
        original["source_event_id"] = "rec-event-1"
        original["source_repair_ids"] = ["rec-repair"]
        plan = self._prepare_project(
            operation_body={"scope": "A", "source_event_id": "rec-event-2",
                            "source_repair_ids": ["rec-repair"]},
            original=original,
        )
        with self.assertRaises(AssistantError) as cm:
            await self.agent.preview_repair(
                ACTOR, plan["id"], self._payload(plan), self.request)
        self.assertIn("事件关联已改变，请重新查找对应检修通告", str(cm.exception))
        self.assertEqual(cm.exception.status, 400)
        self.assertEqual(self.prefill_calls, [], "mismatched context must reject before native read")


class PreviewRepairNativeTests(RepairPrefillBase):
    """Native prefill outcome handling."""

    async def test_native_http_error_remains_visible(self):
        plan = self._prepare_project()
        self.prefill_response = "预填服务拒绝"
        self.prefill_status = 500
        with self.assertRaises(AssistantError) as cm:
            await self.agent.preview_repair(
                ACTOR, plan["id"], self._payload(plan), self.request)
        self.assertEqual(cm.exception.status, 500)
        self.assertIn("预填服务拒绝", str(cm.exception))

    async def test_missing_event_context_marks_skip(self):
        plan = self._prepare_project()
        self.prefill_response = {
            "fields": {"设备名称": "油机A"},
            "source_field_names": ["设备名称"],
            "event_context_missing": True,
        }
        result = await self.agent.preview_repair(
            ACTOR, plan["id"],
            self._payload(plan, source_repair_ids=[]), self.request)
        self.assertTrue(result["skip"])
        self.assertEqual(result["fields"], {"设备名称": "油机A"})

    async def test_stale_plan_version_after_native_rejects_response(self):
        plan = self._prepare_project()
        plan_id = plan["id"]

        async def mutate_during_prefill():
            current = self._stored(plan_id)
            current["version"] += 1
            self.store.put_document(PLAN_NAMESPACE, plan_id, current)

        self.prefill_side_effect = mutate_during_prefill
        with self.assertRaises(AssistantError) as cm:
            await self.agent.preview_repair(
                ACTOR, plan_id, self._payload(plan), self.request)
        self.assertIn("填写状态已变化", str(cm.exception))
        self.assertEqual(cm.exception.status, 409)

    async def test_stale_plan_status_after_native_rejects_response(self):
        plan = self._prepare_project()
        plan_id = plan["id"]

        async def mutate_during_prefill():
            current = self._stored(plan_id)
            current["status"] = "cancelled"
            self.store.put_document(PLAN_NAMESPACE, plan_id, current)

        self.prefill_side_effect = mutate_during_prefill
        with self.assertRaises(AssistantError) as cm:
            await self.agent.preview_repair(
                ACTOR, plan_id, self._payload(plan), self.request)
        self.assertIn("填写状态已变化", str(cm.exception))
        self.assertEqual(cm.exception.status, 409)


class PreviewRepairReadonlyTests(RepairPrefillBase):
    """Preview is strictly readonly: no plan mutation, no business write."""

    async def test_success_does_not_mutate_plan_or_write_business(self):
        plan = self._prepare_project()
        before = self._stored(plan["id"])
        result = await self.agent.preview_repair(
            ACTOR, plan["id"], self._payload(plan), self.request)
        after = self._stored(plan["id"])
        self.assertEqual(before, after, "preview must not mutate the stored plan")
        self.assertEqual(after["version"], plan["version"])
        self.assertEqual(after["status"], "needs_input")
        self.assertEqual(self.record_writes, [],
                         "preview must not issue any repair record write")
        self.assertEqual(result["version"], plan["version"])

    async def test_store_version_unchanged_after_readonly_preview(self):
        plan = self._prepare_project()
        original_version = self._stored(plan["id"])["version"]
        await self.agent.preview_repair(
            ACTOR, plan["id"], self._payload(plan), self.request)
        self.assertEqual(self._stored(plan["id"])["version"], original_version)


class RepairPrefillRouteTests(RepairPrefillBase):
    """Real installed Lighthouse route ``POST /api/assistant/plans/{plan_id}/repair-prefill``.

    Drives the actual ``install_lighthouse_routes`` endpoint through an in-process
    ASGI transport (no service/network).  Plans are prepared by the ordinary
    ``PortalAgent`` against the same isolated store the route lazily builds its own
    agent from, and the fake native repair prefill endpoint is registered on the
    same app so the gateway-based preview resolves real native prefill data.  All
    data is mock/isolated; no production writes.
    """

    def setUp(self):
        super().setUp()
        self.actor = dict(ACTOR)
        self.session = {"user": {"open_id": self.actor["id"]},
                        "allowed_scopes": list(self.actor["scopes"]),
                        "role": "building"}
        controller = SimpleNamespace(
            _current_session=lambda request: self.session,
            _request_base_url=lambda request: str(request.base_url).rstrip("/"),
        )
        runtime = SimpleNamespace(
            state_store=self.store,
            auth_manager=SimpleNamespace(
                is_admin=lambda s: s.get("role") == "admin",
                session_scopes=lambda s: list(s["allowed_scopes"]),
            ),
        )
        install_lighthouse_routes(self.app, controller, runtime)

    async def _post_prefill(self, plan, payload, origin="http://testserver"):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.app, client=("127.0.0.1", 123)),
            base_url="http://testserver",
        ) as client:
            return await client.post(
                f"/api/assistant/plans/{plan['id']}/repair-prefill",
                json=payload,
                headers={"Origin": origin},
            )

    async def test_installed_route_successful_dispatch(self):
        plan = self._prepare_project()
        payload = self._payload(plan)

        resp = await self._post_prefill(plan, payload)

        self.assertEqual(resp.status_code, 200, resp.text)
        body = resp.json()
        self.assertTrue(body["ok"])
        data = body["data"]
        self.assertEqual(data["version"], plan["version"])
        # The fake native prefill descriptor was reached and readonly fields filtered out.
        self.assertEqual(len(self.prefill_calls), 1)
        self.assertEqual(self.prefill_calls[0]["source_event_id"], "rec-event")
        self.assertEqual(self.prefill_calls[0]["source_repair_ids"], ["rec-repair"])
        self.assertEqual(set(data["fields"]),
                         {"设备名称", "维修开始时间", "故障发生时间", "故障维修原因"})
        self.assertEqual(data["warnings"], ["来源数据含未落库字段"])
        self.assertFalse(data["skip"])

    async def test_installed_route_foreign_origin_rejected(self):
        plan = self._prepare_project()

        resp = await self._post_prefill(plan, self._payload(plan), origin="http://evil.example")

        self.assertEqual(resp.status_code, 403, resp.text)
        self.assertIn("不允许跨来源提交", resp.json()["error"])
        self.assertEqual(self.prefill_calls, [])

    async def test_installed_route_missing_auth(self):
        plan = self._prepare_project()
        self.session = None

        resp = await self._post_prefill(plan, self._payload(plan))

        self.assertEqual(resp.status_code, 401, resp.text)
        self.assertIn("请重新登录", resp.json()["error"])
        self.assertEqual(self.prefill_calls, [])

    async def test_installed_route_wrong_owner(self):
        plan = self._prepare_project()
        original_open_id = self.session["user"]["open_id"]
        self.session["user"]["open_id"] = "different-owner"
        try:
            resp = await self._post_prefill(plan, self._payload(plan))
        finally:
            self.session["user"]["open_id"] = original_open_id

        self.assertEqual(resp.status_code, 404, resp.text)
        self.assertIn("无权访问", resp.json()["error"])
        self.assertEqual(self.prefill_calls, [])

    async def test_installed_route_stale_version(self):
        plan = self._prepare_project()
        stored = self._stored(plan["id"])
        stored["version"] += 1
        self.store.put_document(PLAN_NAMESPACE, plan["id"], stored)

        resp = await self._post_prefill(plan, self._payload(plan))

        self.assertEqual(resp.status_code, 409, resp.text)
        self.assertIn("已变化", resp.json()["error"])
        self.assertEqual(self.prefill_calls, [])

    async def test_installed_route_preview_readonly_no_writes_no_mutation(self):
        plan = self._prepare_project()
        before = self._stored(plan["id"])

        resp = await self._post_prefill(plan, self._payload(plan))

        self.assertEqual(resp.status_code, 200, resp.text)
        after = self._stored(plan["id"])
        self.assertEqual(before, after, "preview must not mutate the stored plan")
        self.assertEqual(after["version"], plan["version"])
        self.assertEqual(after["status"], "needs_input")
        self.assertEqual(self.record_writes, [],
                         "preview must not issue any repair record write")


if __name__ == "__main__":
    unittest.main(verbosity=2)
