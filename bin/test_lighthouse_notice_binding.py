"""Focused isolated unittest coverage for the manual notice binding / native
repair prefill contract.

Drives the real ``PortalAgent`` + ``PortalAPICatalog`` against in-process fake
FastAPI native endpoints (no LLM, no cloud, no plan/business writes):
  * ``POST /api/workbench-actions``            notice write backend
  * ``GET /api/workbench/source-options``      plan-notice source records (bind)
  * ``GET /api/workbench/repair-event-prefill`` native repair event prefill

Only helper objects (``Store``/``ACTOR``) are reused; no test classes are
imported.  This runs against the CURRENT build and reports real failures.
"""
import copy
import json
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

from lan_bitable_template_portal.lighthouse_agent import PLAN_NAMESPACE, PortalAgent, _result_refs
from lan_bitable_template_portal.lighthouse_ai import AssistantError, LighthouseAssistant
from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog
from lan_bitable_template_portal.lighthouse_files import LighthouseFiles
from tools.lighthouse_test_backend import install_test_backend as install_lighthouse_routes

from test_lighthouse_agent_workflows import ACTOR, Store

OPERATION_ID = "notice_binding_test_00000001"

BASE_DRAFT = {
    "title": "A楼设备调整",
    "start_time": "2026-09-30 09:00",
    "end_time": "2026-09-30 11:00",
    "location": "A楼机房",
    "content": "调整机组参数",
    "reason": "设备更新",
    "impact": "短时停机",
    "specialty": "电气",
    "building_codes": ["A"],
}

# Minimal valid start draft per native work type (mirrors the native forms).
VALID_STARTS = {
    "maintenance": {**BASE_DRAFT, "maintenance_cycle": "每月", "execution_party": "厂维", "progress": "已完成60%"},
    "change": {**BASE_DRAFT, "level": "低", "execution_party": "厂维", "progress": "已完成60%"},
    "repair": {**BASE_DRAFT, "level": "中", "repair_device": "空调1号机", "repair_fault": "故障A",
               "fault_type": "硬件", "repair_mode": "更换", "discovery": "巡检", "symptom": "异响",
               "solution": "更换备件", "progress": "已完成60%"},
    "power": {**BASE_DRAFT, "notice_type": "上电通告", "cabinet": "A-01", "quantity": "2", "progress": "已完成60%"},
    "polling": {**BASE_DRAFT, "device": "水泵1号", "progress": "已完成60%"},
    "adjust": dict(BASE_DRAFT),
}

# The native prefill returns the SAME repair project id in all three identity
# fields, and the draft already carries the workbench start/end keys.
NATIVE_PREFILL = {
    "source_record_id": "src-1",
    "repair_management_record_id": "src-1",
    "target_record_id": "",
    "source_record": {"record_id": "src-1", "scope": "A", "work_type": "repair"},
    "draft": {
        "work_type": "repair",
        "notice_type": "检修通告",
        "title": "A楼设备调整",
        "location": "A楼机房",
        "content": "调整机组参数",
        "start_time": "2026-09-30 09:00",
        "end_time": "2026-09-30 11:00",
        # Never surfaced: raw/native-only data and secrets.
        "access_token": "secret-token",
        "attachment": "附件.bin",
        "raw_record": {"record_id": "raw-inner"},
    },
}

# Frozen photo/SOP refs already present in the prepared operation body, matching
# how a real upload/SOP flow leaves them in the native patch.
FROZEN_PHOTOS = [{"upload_id": "frozen-photo-1", "file_name": "proof.png"}]
FROZEN_SOP_REFS = {"polling_sop": "frozen-sop-1"}


def make_request():
    return Request({
        "type": "http",
        "scheme": "http",
        "server": ("testserver", 80),
        "client": ("127.0.0.1", 123),
        "path": "/api/assistant/plans/x",
        "root_path": "",
        "query_string": b"",
        "headers": [(b"origin", b"http://testserver"), (b"cookie", b"fixture=a")],
    })


class NoticeBindingBase(unittest.IsolatedAsyncioTestCase):
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
        search = Mock(side_effect=AssertionError("agent must use fake native APIs, not local/DOM scraping"))
        self.assistant = LighthouseAssistant(self.store, search, model=model)
        self.files = LighthouseFiles(self.store)

        self.notice_writes = []
        self.prefill_calls = []
        self.prefill_response = None
        self.prefill_status = 200
        self.prefill_side_effect = None
        self.sources = [
            {"source_record_id": "src-1", "repair_management_record_id": "src-1", "record_id": "src-1",
             "title": "A楼调整", "status": "未开始", "scope": "A", "work_type": "repair"},
        ]

        app = self.app = FastAPI()

        @app.post("/api/workbench-actions")
        async def workbench_actions(request: Request):
            payload = await request.json()
            self.notice_writes.append(payload)
            return {"ok": True, "data": {"record_id": "rec-notice"}}

        @app.get("/api/workbench/source-options")
        async def source_options():
            return {"ok": True, "data": {"items": self.sources}}

        @app.get("/api/workbench/repair-event-prefill")
        async def repair_event_prefill(request: Request):
            self.prefill_calls.append({
                "scope": request.query_params.get("scope"),
                "repair_management_record_id": request.query_params.get("repair_management_record_id"),
            })
            if self.prefill_side_effect is not None:
                await self.prefill_side_effect()
            if self.prefill_response is not None:
                if self.prefill_status == 200:
                    return {"ok": True, "data": self.prefill_response}
                return JSONResponse({"ok": False, "error": self.prefill_response},
                                    status_code=self.prefill_status)
            return {"ok": True, "data": copy.deepcopy(NATIVE_PREFILL)}

        self.catalog = PortalAPICatalog(app)
        self.agent = PortalAgent(self.assistant, self.catalog, self.files)
        self.request = make_request()

    # ---------- helpers ----------
    def _start_decision(self, work_type="repair", *, patch=None, body_extra=None):
        body = {
            "command_format": "notice_command",
            "scope": "A",
            "work_type": work_type,
            "action": "start",
            "manual": True,
            "polling_work_order_exempt": True,
            "patch": patch if patch is not None else copy.deepcopy(VALID_STARTS[work_type]),
        }
        body.update(body_extra or {})
        return {"operations": [{"api_id": "POST /api/workbench-actions", "body": body}]}

    def _prepare_start(self, work_type="repair", *, body_extra=None, patch=None, queries=None):
        return self.agent.prepare(
            ACTOR, self._start_decision(work_type, patch=patch, body_extra=body_extra),
            OPERATION_ID + "_" + work_type, [], queries=queries)

    def _stored(self, plan_id):
        return self.store.get_document(PLAN_NAMESPACE, plan_id)

    def _source_options_field(self, plan, index=0):
        return self._field(plan, "step%d.source_record_id" % index, path="source_record_id")

    def _field(self, plan, name, path=None):
        for f in plan["fields"]:
            if f["name"] == name and (path is None or f.get("path") == path):
                return f
        self.fail("missing field %r (path=%r); fields: %r"
                  % (name, path, [(f.get("name"), f.get("path")) for f in plan["fields"]]))

    def _notice_field(self, plan, index=0):
        return self._field(plan, "step%d.patch" % index, path="patch")

    def _notice_values(self, plan, **overrides):
        values = copy.deepcopy(self._notice_field(plan)["_initial_form"])
        values.update(overrides)
        return values

    async def _load_sources(self, plan):
        source = self._source_options_field(plan)
        loaded = await self.agent.field_options(ACTOR, plan["id"], source["name"], self.request)
        self.current_version = loaded["version"]
        return loaded


class NativeNoticeBindingPrepareTests(NoticeBindingBase):
    """Binding controls always appear for native starts; unbound cleans IDs."""

    async def test_start_binding_controls_always_present_for_all_work_types(self):
        for work_type in VALID_STARTS:
            with self.subTest(work_type=work_type):
                plan = self._prepare_start(work_type)
                self.assertEqual(plan["status"], "needs_input")
                binding = self._field(plan, "step0.manual_binding_choice", path="manual_binding_choice")
                self.assertEqual(binding["type"], "select")
                self.assertTrue(binding["required"])
                self.assertEqual([o["value"] for o in binding["options"]], ["bind", "unbound"])
                self.assertTrue(binding.get("native_notice_binding"), "binding select must carry native_notice_binding")

                source = self._field(plan, "step0.source_record_id", path="source_record_id")
                self.assertEqual(source["options_source"], "notice_sources")
                self.assertTrue(source.get("native_notice_binding"), "source select must carry native_notice_binding")
                self.assertEqual(source.get("when", {}).get("equals"), "bind")
                if work_type == "repair":
                    self.assertTrue(source.get("native_notice_prefill"),
                                    "repair source select must carry native_notice_prefill")
                else:
                    self.assertFalse(source.get("native_notice_prefill"),
                                     "only repair source loads native prefill")

        # No business write may happen while preparing.
        self.assertEqual(self.notice_writes, [])

    async def test_unrecognized_raw_prefill_source_not_trusted(self):
        # A raw preselected source that was never loaded through source-options
        # must NOT be echoed as a trusted value.
        plan = self._prepare_start(
            "repair",
            body_extra={"manual_binding_choice": "bind", "source_record_id": "src-lookup-me",
                        "manual_binding_required": True})
        source = self._source_options_field(plan)
        self.assertFalse(source.get("value"))
        self.assertEqual([o["value"] for o in source.get("options", [])], [])

        self.assertEqual(self.notice_writes, [])

    async def test_recognized_queried_source_preselected(self):
        # A truly queried source is a trusted candidate and is echoed as value.
        queries = {"query_sources": {"items": copy.deepcopy(self.sources)}}
        plan = self._prepare_start(
            "repair",
            body_extra={"manual_binding_choice": "bind", "source_record_id": "src-1"},
            queries=queries)
        source = self._source_options_field(plan)
        self.assertEqual(source["value"], "src-1")
        self.assertIn("src-1", [o["value"] for o in source["options"]])

        self.assertEqual(self.notice_writes, [])

    async def test_omitted_choice_still_requires_user_decision(self):
        plan = self._prepare_start("repair")
        with self.assertRaisesRegex(AssistantError, "计划通告关联"):
            self.agent.amend(ACTOR, plan["id"], {"version": plan["version"], "values": {}})
        self.assertEqual(self.notice_writes, [])

    async def test_unbound_clears_query_backed_identity_without_losing_photos(self):
        reference = "query_" + "a" * 32
        draft = {**VALID_STARTS["repair"], "source_record_id": "stale-source", "repair_management_record_id": "stale-repair", "site_photos": FROZEN_PHOTOS}
        plan = self._prepare_start("repair", patch={"$query": {"ref": reference, "path": "draft"}},
                                   body_extra={"manual_binding_choice": "unbound"}, queries={reference: {"draft": draft}})
        self.agent.amend(ACTOR, plan["id"], {"version": plan["version"], "values": {}})
        stored = self._stored(plan["id"])
        body = _result_refs(stored["operations"][0]["body"], [], stored["_references"], stored["_queries"])
        self.assertFalse(body.get("source_record_id"))
        self.assertFalse(body["patch"].get("source_record_id"))
        self.assertFalse(body["patch"].get("repair_management_record_id"))
        self.assertEqual(body["patch"]["site_photos"], FROZEN_PHOTOS)
        self.assertNotIn("stale-", json.dumps(body))

    async def test_amend_unbound_clears_source_ids_but_preserves_notice_and_refs(self):
        # The frozen photo/SOP refs live in the prepared operation patch, exactly
        # as a real pending upload/SOP flow would leave them.
        patch = {**copy.deepcopy(VALID_STARTS["repair"]),
                 "site_photos": copy.deepcopy(FROZEN_PHOTOS),
                 "polling_work_order_refs": copy.deepcopy(FROZEN_SOP_REFS)}
        plan = self._prepare_start(
            "repair",
            patch=patch,
            body_extra={"manual_binding_choice": "bind", "source_record_id": "src-1",
                        "repair_management_record_id": "src-1"})
        loaded = await self._load_sources(plan)

        notice = self._notice_field(plan)
        amended = self.agent.amend(ACTOR, plan["id"], {
            "version": loaded["version"],
            "values": {
                "step0.manual_binding_choice": "unbound",
                "step0.source_record_id": "",
                notice["name"]: self._notice_values(plan, progress="已完成80%"),
            },
        })
        self.assertEqual(amended["status"], "awaiting_confirmation")
        stored = self._stored(plan["id"])
        body = stored["operations"][0]["body"]
        self.assertNotIn("source_record_id", body)
        self.assertNotIn("repair_management_record_id", body)
        # Unbound start must not retain a bound source in the patch either.
        self.assertNotIn("source_record_id", body.get("patch", {}))
        self.assertNotIn("repair_management_record_id", body.get("patch", {}))
        # Notice draft, photos and SOP refs survive the unbound switch.
        patch = body["patch"]
        self.assertEqual(patch["title"], "A楼设备调整")
        self.assertEqual(patch["location"], "A楼机房")
        self.assertEqual(patch["progress"], "已完成80%")
        self.assertEqual(patch["site_photos"], FROZEN_PHOTOS)
        self.assertEqual(patch["polling_work_order_refs"], FROZEN_SOP_REFS)
        # Prepare + amend never write.
        self.assertEqual(self.notice_writes, [])

    async def test_bind_to_unbound_to_edit_retains_choice_and_no_id_in_resolved_body(self):
        patch = {**copy.deepcopy(VALID_STARTS["repair"]),
                 "site_photos": copy.deepcopy(FROZEN_PHOTOS),
                 "polling_work_order_refs": copy.deepcopy(FROZEN_SOP_REFS)}
        plan = self._prepare_start(
            "repair",
            patch=patch,
            body_extra={"manual_binding_choice": "bind", "source_record_id": "src-1",
                        "repair_management_record_id": "src-1"})
        loaded = await self._load_sources(plan)
        notice = self._notice_field(plan)

        def amend_values(version, *, choice, source, progress):
            return self.agent.amend(ACTOR, plan["id"], {
                "version": version,
                "values": {
                    "step0.manual_binding_choice": choice,
                    "step0.source_record_id": source,
                    notice["name"]: self._notice_values(plan, progress=progress),
                },
            })

        # 1. bind to src-1 -> awaiting confirmation.
        bound = amend_values(loaded["version"], choice="bind", source="src-1", progress="已完成70%")
        self.assertEqual(bound["status"], "awaiting_confirmation")
        bound_body = self._stored(plan["id"])["operations"][0]["body"]
        self.assertEqual(bound_body["source_record_id"], "src-1")

        # 2. return to editing, then switch to unbound -> IDs cleared.
        edited_bind = self.agent.amend(ACTOR, plan["id"], {"action": "edit", "version": bound["version"]})
        self.assertEqual(edited_bind["status"], "needs_input")
        unbound = amend_values(edited_bind["version"], choice="unbound", source="", progress="已完成80%")
        self.assertEqual(unbound["status"], "awaiting_confirmation")
        stored = self._stored(plan["id"])
        body = stored["operations"][0]["body"]
        self.assertNotIn("source_record_id", body)
        self.assertNotIn("repair_management_record_id", body)
        self.assertNotIn("source_record_id", body.get("patch", {}))
        self.assertNotIn("repair_management_record_id", body.get("patch", {}))

        # 3. return to edit again: the unbound choice is retained (not reverted).
        edited = self.agent.amend(ACTOR, plan["id"], {"action": "edit", "version": unbound["version"]})
        self.assertEqual(edited["status"], "needs_input")
        binding = self._field(edited, "step0.manual_binding_choice", path="manual_binding_choice")
        self.assertEqual(binding["value"], "unbound")
        source_field = self._field(edited, "step0.source_record_id", path="source_record_id")
        self.assertEqual(source_field["value"], "")

        # 4. final submit as unbound: still no source/repair ids in the body.
        final = amend_values(edited["version"], choice="unbound", source="", progress="已完成90%")
        self.assertEqual(final["status"], "awaiting_confirmation")
        final_body = self._stored(plan["id"])["operations"][0]["body"]
        self.assertNotIn("source_record_id", final_body)
        self.assertNotIn("repair_management_record_id", final_body)
        self.assertNotIn("source_record_id", final_body.get("patch", {}))
        self.assertNotIn("repair_management_record_id", final_body.get("patch", {}))
        # Final body JSON carries no stale ids anywhere.
        self.assertNotIn("src-1", json.dumps(final_body))
        self.assertEqual(self.notice_writes, [])


class PreviewNoticeTests(NoticeBindingBase):
    """Readonly native repair-event prefill via preview_notice."""

    async def _prepare_repair_start(self, body_extra=None, patch=None):
        plan = self._prepare_start("repair", body_extra=body_extra, patch=patch)
        loaded = await self._load_sources(plan)
        return plan, loaded

    def _payload(self, plan, version=None, **overrides):
        payload = {
            "version": version if version is not None else self.current_version,
            "operation_index": 0,
            "source_record_id": "src-1",
            "scope": "A",
        }
        payload.update(overrides)
        return payload

    async def test_success_previews_restricted_native_notice_fields(self):
        plan, loaded = await self._prepare_repair_start({"manual_binding_choice": "bind",
                                                         "source_record_id": "src-1"})
        stored_before = self._stored(plan["id"])
        result = await self.agent.preview_notice(ACTOR, plan["id"], self._payload(plan), self.request)

        self.assertEqual(result["version"], self.current_version)
        self.assertEqual(len(self.prefill_calls), 1)
        self.assertEqual(self.prefill_calls[0]["scope"], "A")
        self.assertEqual(self.prefill_calls[0]["repair_management_record_id"], "src-1")
        fields = result["fields"]
        # Restricted to the actual native_notice children.
        self.assertEqual(fields["title"], "A楼设备调整")
        self.assertEqual(fields["location"], "A楼机房")
        self.assertEqual(fields["content"], "调整机组参数")
        # Dates are normalized and NOT swapped: start=expected, end=fault.
        self.assertEqual(fields["start_time"], "2026-09-30T09:00")
        self.assertEqual(fields["end_time"], "2026-09-30T11:00")
        # No tokens / native-only / raw record values leak.
        self.assertNotIn("secret-token", json.dumps(result, ensure_ascii=False))
        self.assertNotIn("附件.bin", json.dumps(result, ensure_ascii=False))
        self.assertNotIn("raw-inner", json.dumps(result, ensure_ascii=False))
        # Readonly: the stored plan is unchanged and no write ran.
        self.assertEqual(self._stored(plan["id"]), stored_before)
        self.assertEqual(self._stored(plan["id"])["version"], self.current_version)
        self.assertEqual(self._stored(plan["id"])["status"], "needs_input")
        self.assertEqual(self.notice_writes, [])

    async def test_empty_source_clears_without_native_call(self):
        plan, loaded = await self._prepare_repair_start({"manual_binding_choice": "bind"})
        result = await self.agent.preview_notice(
            ACTOR, plan["id"], self._payload(plan, source_record_id=""), self.request)
        self.assertEqual(result["fields"], {})
        self.assertEqual(self.prefill_calls, [], "empty source must not hit native prefill")
        self.assertEqual(result["version"], self.current_version)

    async def test_ongoing_target_rejected(self):
        plan, loaded = await self._prepare_repair_start({"manual_binding_choice": "bind",
                                                         "source_record_id": "src-1"})
        self.prefill_response = {**NATIVE_PREFILL, "target_record_id": "rec-ongoing"}
        with self.assertRaises(AssistantError) as cm:
            await self.agent.preview_notice(ACTOR, plan["id"], self._payload(plan), self.request)
        self.assertIn("未结束", str(cm.exception))

    async def test_wrong_version_rejected_before_native(self):
        plan, loaded = await self._prepare_repair_start({"manual_binding_choice": "bind"})
        with self.assertRaises(AssistantError) as cm:
            await self.agent.preview_notice(
                ACTOR, plan["id"], self._payload(plan, version=self.current_version + 1), self.request)
        self.assertEqual(cm.exception.status, 409)
        self.assertEqual(self.prefill_calls, [])

    async def test_cancelled_plan_rejected_before_native(self):
        plan, loaded = await self._prepare_repair_start({"manual_binding_choice": "bind"})
        current = self._stored(plan["id"])
        current["status"] = "cancelled"
        self.store.put_document(PLAN_NAMESPACE, plan["id"], current)
        with self.assertRaises(AssistantError) as cm:
            await self.agent.preview_notice(ACTOR, plan["id"], self._payload(plan), self.request)
        self.assertEqual(cm.exception.status, 409)
        self.assertEqual(self.prefill_calls, [])

    async def test_wrong_source_not_loaded_rejected_before_native(self):
        plan, loaded = await self._prepare_repair_start({"manual_binding_choice": "bind"})
        with self.assertRaises(AssistantError) as cm:
            await self.agent.preview_notice(
                ACTOR, plan["id"], self._payload(plan, source_record_id="src-forged"), self.request)
        self.assertIn("已读取", str(cm.exception))
        self.assertEqual(self.prefill_calls, [])

    async def test_out_of_scope_rejected_before_native(self):
        plan, loaded = await self._prepare_repair_start({"manual_binding_choice": "bind"})
        with self.assertRaises(AssistantError) as cm:
            await self.agent.preview_notice(
                ACTOR, plan["id"], self._payload(plan, scope="B"), self.request)
        self.assertEqual(cm.exception.status, 403)
        self.assertEqual(self.prefill_calls, [])

    async def test_wrong_owner_rejected(self):
        plan, loaded = await self._prepare_repair_start({"manual_binding_choice": "bind"})
        other = {**ACTOR, "id": "different-owner"}
        with self.assertRaises(AssistantError) as cm:
            await self.agent.preview_notice(other, plan["id"], self._payload(plan), self.request)
        self.assertEqual(cm.exception.status, 404)
        self.assertEqual(self.prefill_calls, [])

    async def test_native_http_error_remains_visible(self):
        plan, loaded = await self._prepare_repair_start({"manual_binding_choice": "bind"})
        self.prefill_response = "预填拒绝"
        self.prefill_status = 500
        with self.assertRaises(AssistantError) as cm:
            await self.agent.preview_notice(ACTOR, plan["id"], self._payload(plan), self.request)
        self.assertEqual(cm.exception.status, 500)
        self.assertIn("预填拒绝", str(cm.exception))

    async def test_mismatched_prefill_rejected(self):
        plan, loaded = await self._prepare_repair_start({"manual_binding_choice": "bind"})
        self.prefill_response = {**NATIVE_PREFILL, "source_record_id": "src-other",
                                 "draft": {"work_type": "change"}}
        with self.assertRaises(AssistantError):
            await self.agent.preview_notice(ACTOR, plan["id"], self._payload(plan), self.request)

    async def test_incomplete_prefill_rejected(self):
        plan, loaded = await self._prepare_repair_start({"manual_binding_choice": "bind"})
        self.prefill_response = {"source_record_id": "src-1"}  # missing draft/source
        with self.assertRaises(AssistantError):
            await self.agent.preview_notice(ACTOR, plan["id"], self._payload(plan), self.request)

    async def test_stale_version_after_native_rejects_response(self):
        plan, loaded = await self._prepare_repair_start({"manual_binding_choice": "bind"})
        plan_id = plan["id"]

        async def mutate_during_prefill():
            current = self._stored(plan_id)
            current["version"] += 1
            self.store.put_document(PLAN_NAMESPACE, plan_id, current)

        self.prefill_side_effect = mutate_during_prefill
        with self.assertRaises(AssistantError) as cm:
            await self.agent.preview_notice(ACTOR, plan_id, self._payload(plan), self.request)
        self.assertEqual(cm.exception.status, 409)

    async def test_stale_status_after_native_rejects_response(self):
        plan, loaded = await self._prepare_repair_start({"manual_binding_choice": "bind"})
        plan_id = plan["id"]

        async def mutate_during_prefill():
            current = self._stored(plan_id)
            current["status"] = "cancelled"
            self.store.put_document(PLAN_NAMESPACE, plan_id, current)

        self.prefill_side_effect = mutate_during_prefill
        with self.assertRaises(AssistantError) as cm:
            await self.agent.preview_notice(ACTOR, plan_id, self._payload(plan), self.request)
        self.assertEqual(cm.exception.status, 409)

    # ---- guards for preview_notice ----

    async def test_wrong_operation_index_rejected(self):
        plan, loaded = await self._prepare_repair_start({"manual_binding_choice": "bind"})
        with self.assertRaises(AssistantError) as cm:
            await self.agent.preview_notice(
                ACTOR, plan["id"], self._payload(plan, operation_index=5), self.request)
        self.assertIn("检修通告", str(cm.exception))
        self.assertEqual(self.prefill_calls, [])

    async def test_wrong_operation_type_rejected(self):
        plan = self._prepare_start("maintenance")
        with self.assertRaises(AssistantError) as cm:
            await self.agent.preview_notice(
                ACTOR, plan["id"],
                {"version": self._stored(plan["id"])["version"], "operation_index": 0,
                 "source_record_id": "src-1", "scope": "A"},
                self.request)
        self.assertIn("此操作不支持检修计划预填", str(cm.exception))
        self.assertEqual(self.prefill_calls, [])

    async def test_missing_binding_block_rejected(self):
        plan = self._prepare_start("repair", body_extra={"manual_binding_choice": "bind",
                                                         "source_record_id": "src-1"})
        stored = self._stored(plan["id"])
        stored["fields"] = [f for f in stored["fields"]
                            if f["path"] not in ("source_record_id", "manual_binding_choice")]
        self.store.put_document(PLAN_NAMESPACE, plan["id"], stored)
        with self.assertRaises(AssistantError) as cm:
            await self.agent.preview_notice(
                ACTOR, plan["id"],
                {"version": stored["version"], "operation_index": 0,
                 "source_record_id": "src-1", "scope": "A"},
                self.request)
        self.assertIn("此操作不支持检修计划预填", str(cm.exception))
        self.assertEqual(self.prefill_calls, [])

    async def test_response_wrong_scope_rejected(self):
        plan, loaded = await self._prepare_repair_start({"manual_binding_choice": "bind"})
        self.prefill_response = {**NATIVE_PREFILL, "source_record": {**NATIVE_PREFILL["source_record"], "scope": "B"}}
        with self.assertRaises(AssistantError) as cm:
            await self.agent.preview_notice(ACTOR, plan["id"], self._payload(plan), self.request)
        self.assertIn("类型或楼栋不一致", str(cm.exception))

    async def test_invalid_date_rejected(self):
        plan, loaded = await self._prepare_repair_start({"manual_binding_choice": "bind"})
        self.prefill_response = {**NATIVE_PREFILL, "draft": {**NATIVE_PREFILL["draft"], "start_time": "not-a-date"}}
        with self.assertRaises(AssistantError) as cm:
            await self.agent.preview_notice(ACTOR, plan["id"], self._payload(plan), self.request)
        self.assertIn("时间无效", str(cm.exception))

    async def test_installed_preview_route_dispatch_and_security(self):
        session = {"user": {"open_id": ACTOR["id"]}, "allowed_scopes": ACTOR["scopes"], "role": "building"}
        controller = SimpleNamespace(_current_session=lambda request: session,
                                     _request_base_url=lambda request: str(request.base_url).rstrip("/"))
        runtime = SimpleNamespace(state_store=self.store, auth_manager=SimpleNamespace(
            is_admin=lambda item: False, session_scopes=lambda item: item["allowed_scopes"]))
        install_lighthouse_routes(self.app, controller, runtime)
        plan, loaded = await self._prepare_repair_start()
        before = self._stored(plan["id"])
        path = f"/api/assistant/plans/{plan['id']}/notice-prefill"
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app), base_url="http://testserver") as client:
            response = await client.post(path, json=self._payload(plan), headers={"Origin": "http://testserver"})
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json()["data"]["fields"]["start_time"], "2026-09-30T09:00")
            response = await client.post(path, json=self._payload(plan), headers={"Origin": "https://foreign.example"})
            self.assertEqual(response.status_code, 403, response.text)
            session["user"]["open_id"] = "another-user"
            response = await client.post(path, json=self._payload(plan), headers={"Origin": "http://testserver"})
            self.assertEqual(response.status_code, 404, response.text)
            session = None
            response = await client.post(path, json=self._payload(plan), headers={"Origin": "http://testserver"})
            self.assertEqual(response.status_code, 401, response.text)
        self.assertEqual(self._stored(plan["id"]), before)
        self.assertEqual(len(self.prefill_calls), 1)
        self.assertEqual(self.notice_writes, [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
