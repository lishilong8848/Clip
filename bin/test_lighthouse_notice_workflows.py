"""Bounded PortalAgent integration tests for the native notice_command form.

These tests drive the real ``PortalAgent.prepare / amend / confirm / _execute``
against an in-process FastAPI app that fakes ONLY the native endpoints (no
assistant/LLM mocking, no cloud access):

  * ``POST /api/workbench-actions``            notice write backend
  * ``POST /api/notice-attachments``           attachment upload (returns upload_id)
  * ``GET /api/workbench/source-options``      plan-notice source records (bind)
  * ``GET /api/workbench``                     ongoing notice targets (update/end)

Covered guarantees:

  1. start retains parsed values for all six native work types;
  2. update/end write only the intended delta (real queried original held
     separately), keep the same target id and preserve body attachment refs;
  3. mandatory blanks, malformed datetimes, out-of-scope buildings and forged
     target_record_id inside the patch object are rejected with zero writes;
  4. an ``event`` operation cannot be misrouted to the ordinary notice backend;
  5. returning to edit never mutates the original ``$query`` source;
  6. an initially-unknown target select loads options and the second amend
     finishes the form without writing before execute;
  7. a pending prior image-upload ``$result`` reference inside a ``site_photos``
     list stays frozen (unresolved) when the notice form is prepared.
"""
import asyncio
import copy
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

from fastapi import FastAPI, Request
from PIL import Image
from lan_bitable_template_portal.lighthouse_agent import PLAN_NAMESPACE, PortalAgent
from lan_bitable_template_portal.lighthouse_ai import AssistantError, LighthouseAssistant
from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog
from lan_bitable_template_portal.lighthouse_files import LighthouseFiles
from lan_bitable_template_portal.workbench_lite import _record_title

ACTOR = {"id": "notice-workflow-a", "scopes": ["A"], "is_admin": False}

DRAFT = {
    "title": "A楼设备调整",
    "progress": "已完成60%",
    "start_time": "2026-09-30 09:00",
    "end_time": "2026-09-30 11:00",
    "location": "A楼机房",
    "content": "调整机组参数",
    "reason": "设备更新",
    "impact": "短时停机",
    "specialty": "电气",
    "maintenance_cycle": "每月",
    "execution_party": "厂维",
    "building_codes": ["A"],
}

# Validation metadata per native work type.
VALID_STARTS = {
    "maintenance": {**DRAFT, "maintenance_cycle": "每月", "execution_party": "厂维"},
    "change": {**DRAFT, "level": "低", "execution_party": "厂维"},
    "repair": {**DRAFT, "level": "中", "repair_device": "空调1号机",
               "repair_fault": "故障A", "fault_type": "硬件", "repair_mode": "更换",
               "discovery": "巡检", "symptom": "异响", "solution": "更换备件"},
    "power": {**DRAFT, "notice_type": "上电通告", "cabinet": "A-01", "quantity": "2"},
    "polling": {**DRAFT, "device": "水泵1号"},
    "adjust": dict(DRAFT),
}

ORIGINAL = {
    "record_id": "rec-orig",
    "target_record_id": "rec-orig",
    "active_item_id": "active-orig",
    "source_record_id": "src-orig",
    "scope": "A",
    "work_type": "maintenance",
    "version": 7,
    "site_photos": [{"upload_id": "orig-photo-1", "file_name": "original.png"}],
    **DRAFT,
}


class Store:
    def __init__(self, path):
        self.db_path, self.docs = path, {}

    def get_document(self, namespace, key):
        return copy.deepcopy(self.docs.get((namespace, key)))

    def put_document(self, namespace, key, value):
        self.docs[namespace, key] = copy.deepcopy(value)


async def gather_tasks(agent):
    if agent.tasks:
        await asyncio.gather(*tuple(agent.tasks))


class NoticeWorkflowTests(unittest.IsolatedAsyncioTestCase):
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
        search = Mock(side_effect=AssertionError("agent must use fake native APIs, not local scraping"))
        self.assistant = LighthouseAssistant(self.store, search, model=model)
        self.files = LighthouseFiles(self.store)

        self.notice_writes = []
        self.upload_writes = []
        self.sources = [
            {"record_id": "plan-1", "target_record_id": "rec-orig", "title": "A楼调整", "status": "未开始",
             "scope": "A", "work_type": "maintenance"},
            {"record_id": "plan-2", "target_record_id": "rec-orig-2", "title": "B楼调整", "status": "未开始",
             "scope": "A", "work_type": "maintenance"},
        ]
        self.ongoing = [ORIGINAL, {**ORIGINAL, "target_record_id": "rec-orig-2", "record_id": "rec-orig-2", "active_item_id": "active-orig-2", "content": "另一个原内容"}]

        app = FastAPI()

        @app.post("/api/workbench-actions")
        async def workbench_actions(request: Request):
            payload = await request.json()
            self.notice_writes.append(payload)
            return {"ok": True, "data": {"record_id": "rec-notice"}}

        @app.post("/api/notice-attachments")
        async def upload_local(request: Request):
            raw = await request.body()
            self.upload_writes.append((request.query_params.get("kind"), raw))
            return {"ok": True, "data": {"upload_id": "up-1", "file_token": "private-file-token"}}

        @app.get("/api/workbench/source-options")
        async def source_options():
            return {"ok": True, "data": {"items": self.sources}}

        @app.get("/api/workbench")
        async def workbench_ongoing(scope: str = 'A', work_type: str = 'maintenance', sections: str = 'ongoing'):
            return {"ok": True, "data": {"ongoing": self.ongoing}}

        self.catalog = PortalAPICatalog(app)
        self.agent = PortalAgent(self.assistant, self.catalog, self.files)
        self.request = Request({
            "type": "http",
            "scheme": "http",
            "server": ("testserver", 80),
            "client": ("127.0.0.1", 123),
            "path": "/api/assistant/messages",
            "root_path": "",
            "query_string": b"",
            "headers": [(b"origin", b"http://testserver"), (b"cookie", b"fixture=a")],
        })

    # -- helpers -----------------------------------------------------------

    def _operation_id(self, suffix):
        return "notice_workflow_" + suffix + "_" + "x" * 20

    def _start_decision(self, qref=None, patch=None, extra=None, work_type="maintenance"):
        body = {
            "command_format": "notice_command",
            "scope": "A",
            "work_type": work_type,
            "action": "start",
            "manual": True,
            "polling_work_order_exempt": True,
            "patch": patch if patch is not None else {"$query": {"ref": qref, "path": "draft"}},
        }
        body.update(extra or {})
        return {"operations": [{"api_id": "POST /api/workbench-actions", "body": body}]}

    def _update_end_decision(self, action, progress, *, work_type="maintenance", scope="A",
                             target_record_id="rec-orig", expected_version=7, extra=None):
        body = {
            "command_format": "notice_command",
            "scope": scope,
            "work_type": work_type,
            "action": action,
            "target_record_id": target_record_id,
            "expected_version": expected_version,
            "patch": {"progress": progress},
        }
        body.update(extra or {})
        return {"operations": [{"api_id": "POST /api/workbench-actions", "body": body}]}

    async def _amend(self, plan, values, actor=ACTOR):
        return self.agent.amend(actor, plan["id"], {"version": plan["version"], "values": values})

    async def _execute(self, plan, actor=ACTOR):
        stored = self.agent.get_plan(actor, plan["id"])
        await self.agent._execute(actor, stored, self.request)
        await gather_tasks(self.agent)
        return self.agent.get_plan(actor, plan["id"])

    # -- 1) start typed form retains parsed values for all six work types ---

    async def test_start_typed_form_retains_parsed_values_for_all_work_types(self):
        for work_type, draft in VALID_STARTS.items():
            with self.subTest(work_type=work_type):
                self.notice_writes.clear()
                qref = "query_" + "a" * 32
                queries = {qref: {"draft": copy.deepcopy(draft)}}
                plan = self.agent.prepare(ACTOR, self._start_decision(qref, work_type=work_type),
                                          self._operation_id(f"start_{work_type}"), [], queries=queries)
                self.assertEqual(plan["status"], "needs_input")
                amended = await self._amend(plan, {"step0.manual_binding_choice": "unbound"})
                self.assertEqual(amended["status"], "awaiting_confirmation")
                finished = await self._execute(amended)
                self.assertEqual(finished["status"], "completed", finished.get("error"))
                self.assertEqual(len(self.notice_writes), 1)
                patch = self.notice_writes[0]["patch"]
                for key, value in draft.items():
                    self.assertEqual(patch.get(key), value, f"{work_type} draft field {key} lost in final patch")

    # -- 2) update/end write only the intended delta ------------------------

    async def test_update_and_end_only_write_progress_delta(self):
        for action in ("update", "end"):
            with self.subTest(action=action):
                self.notice_writes.clear()
                qref = "query_" + "b" * 32
                queries = {qref: {"ongoing": [copy.deepcopy(ORIGINAL)]}}
                new_progress = "已完成80%" if action == "update" else "已结束"
                decision = self._update_end_decision(action, new_progress)
                # Preserve the body attachment reference of the real queried
                # original so a delta-only patch never drops it.
                decision["operations"][0]["body"]["patch"]["site_photos"] = copy.deepcopy(
                    queries[qref]["ongoing"][0]["site_photos"]
                )
                plan = self.agent.prepare(ACTOR, decision, self._operation_id(action), [], queries=queries)
                self.assertEqual(plan["status"], "needs_input")
                patch_field = next(f for f in plan["fields"] if f["path"] == "patch")
                # Hydrated from the real queried original, not a blank form.
                self.assertEqual(patch_field["value"]["title"], _record_title(ORIGINAL))
                self.assertEqual(patch_field["value"]["location"], "A楼机房")

                amended = await self._amend(plan, {
                    "step0.patch": {**patch_field["value"], "progress": new_progress},
                })
                self.assertEqual(amended["status"], "awaiting_confirmation")
                # Nothing is written until execute.
                self.assertEqual(self.notice_writes, [])
                finished = await self._execute(amended)
                self.assertEqual(finished["status"], "completed", finished.get("error"))
                self.assertEqual(len(self.notice_writes), 1)
                write = self.notice_writes[0]
                self.assertEqual(write["target_record_id"], "rec-orig")
                self.assertEqual(write["expected_version"], 7)
                self.assertEqual(write["patch"], {"progress": new_progress, "site_photos": ORIGINAL["site_photos"]})

    async def test_canonical_target_alias_and_selected_scope_remain_bound(self):
        actor = {**ACTOR, "scopes": ["A", "B"]}
        original = {**ORIGINAL, "record_id": "local-record-alias"}
        plan = self.agent.prepare(actor, self._update_end_decision("update", "已完成80%", extra={"record_id": "rec-orig"}),
                                  self._operation_id("alias"), [], queries={"query_" + "c" * 32: {"ongoing": [original]}})
        field = next(field for field in plan["fields"] if field.get("native_notice"))
        buildings = next(child for child in field["children"] if child["path"] == "building_codes")
        self.assertEqual([option["value"] for option in buildings["options"]], ["A"])
        with self.assertRaises(AssistantError):
            await self._amend(plan, {field["name"]: {**field["value"], "building_codes": ["B"]}}, actor=actor)
        amended = await self._amend(plan, {}, actor=actor)
        await self._execute(amended, actor=actor)
        self.assertEqual(self.notice_writes[0]["target_record_id"], "rec-orig")
        self.assertEqual(self.notice_writes[0]["record_id"], "rec-orig")

    # -- 3) validation rejects bad input ------------------------------------

    async def test_update_end_rejects_historical_deleted_or_finished_target_before_form(self):
        for action in ('update', 'end'):
            decision = self._update_end_decision(action, '现场处理')
            for queries in ({'q': {'records': [ORIGINAL]}}, {'q': {'ongoing': [{**ORIGINAL, 'status': '已结束'}]}},
                            {'q': {'ongoing': [{**ORIGINAL, 'deleted_at': 123}]}}, {'q': {'ongoing': []}}):
                with self.subTest(action=action, queries=queries), self.assertRaises(AssistantError):
                    self.agent.prepare(ACTOR, decision, self._operation_id('historical'), [], queries=queries)
        self.assertEqual(self.notice_writes, [])

    async def test_record_leaving_current_list_after_form_prevents_update_and_end(self):
        for action in ('update', 'end'):
            for state in ('removed', 'finished', 'replaced'):
                with self.subTest(action=action, state=state):
                    original = copy.deepcopy(ORIGINAL)
                    self.ongoing = [original]
                    plan = self.agent.prepare(ACTOR, self._update_end_decision(action, '本次进展'), self._operation_id(action + state), [], queries={'q': {'ongoing': self.ongoing}})
                    ready = await self._amend(plan, {})
                    if state == 'removed':
                        self.ongoing = []
                    elif state == 'finished':
                        self.ongoing = [{**original, 'status': '结束'}]
                    else:
                        self.ongoing = [{**original, 'active_item_id': 'new-active', 'target_record_id': 'new-rec', 'record_id': 'new-rec'}]
                    finished = await self._execute(ready)
                    self.assertEqual(finished['status'], 'failed')
                    self.assertEqual(self.notice_writes, [])

    async def test_mandatory_blanks_invalid_datetime_out_of_scope_and_forged_ids_rejected(self):
        qref = "query_" + "c" * 32
        queries = {qref: {"draft": copy.deepcopy(DRAFT)}}
        plan = self.agent.prepare(ACTOR, self._start_decision(qref), self._operation_id("blank"), [],
                                  queries=queries)
        patch_field = next(f for f in plan["fields"] if f["path"] == "patch")
        base = copy.deepcopy(patch_field["value"])

        blank = copy.deepcopy(base)
        blank["location"] = ""
        with self.assertRaises(AssistantError) as ctx:
            await self._amend(plan, {"step0.manual_binding_choice": "unbound", "step0.patch": blank})
        self.assertIn("请填写", str(ctx.exception))

        bad_time = copy.deepcopy(base)
        bad_time["start_time"] = "not-a-datetime"
        with self.assertRaises(AssistantError) as ctx:
            await self._amend(plan, {"step0.manual_binding_choice": "unbound", "step0.patch": bad_time})
        self.assertIn("时间", str(ctx.exception))

        out_scope = copy.deepcopy(base)
        out_scope["building_codes"] = ["B"]
        with self.assertRaises(AssistantError) as ctx:
            await self._amend(plan, {"step0.manual_binding_choice": "unbound", "step0.patch": out_scope})
        self.assertIn("楼栋", str(ctx.exception))
        self.assertEqual(ctx.exception.status, 403)

        # A forged target_record_id inside the patch object is NOT silently
        # ignored: it is a read-only identity field and amend must reject it.
        forged = copy.deepcopy(base)
        forged["target_record_id"] = "rec-evil"
        forged["record_id"] = "rec-forged"
        with self.assertRaises(AssistantError) as ctx:
            await self._amend(plan, {"step0.manual_binding_choice": "unbound", "step0.patch": forged})
        self.assertIn("只读", str(ctx.exception))
        self.assertEqual(self.notice_writes, [])

    # -- 4) event unaffected ------------------------------------------------

    async def test_event_work_type_cannot_use_ordinary_notice_backend(self):
        decision = {"operations": [{"api_id": "POST /api/workbench-actions", "body": {
            "command_format": "notice_command",
            "scope": "A",
            "work_type": "event",
            "action": "start",
            "patch": {"title": "事件A", "location": "A楼", "content": "事件内容"},
        }}]}
        with self.assertRaisesRegex(AssistantError, "Qt 专用链路"):
            self.agent.prepare(ACTOR, decision, self._operation_id("event"), [])
        self.assertEqual(self.notice_writes, [])

    def test_model_parameters_cannot_silently_skip_native_notice_body(self):
        with self.assertRaisesRegex(AssistantError, "不要放入params"):
            self.agent.prepare(ACTOR, {"operations": [{"api_id": "POST /api/workbench-actions",
                "params": {"action": "update", "target_record_id": "rec-orig"}, "body": {"progress": "已完成80%"}}]}, self._operation_id("wrong-envelope"), [])
        decision = self._start_decision(patch=dict(DRAFT))
        decision["operations"][0]["body"]["progress"] = "不能忽略的进度"
        with self.assertRaisesRegex(AssistantError, "body.patch"):
            self.agent.prepare(ACTOR, decision, self._operation_id("flat-progress"), [])
        self.assertEqual(self.notice_writes, [])

    # -- 5) original source unchanged when returning to edit ----------------

    async def test_original_source_unchanged_when_returning_to_edit(self):
        qref = "query_" + "d" * 32
        queries = {qref: {"draft": copy.deepcopy(DRAFT)}}
        plan = self.agent.prepare(ACTOR, self._start_decision(qref), self._operation_id("edit"), [],
                                  queries=queries)
        before = copy.deepcopy(queries[qref])

        amended = await self._amend(plan, {"step0.manual_binding_choice": "unbound"})
        self.assertEqual(amended["status"], "awaiting_confirmation")
        reopened = self.agent.amend(ACTOR, amended["id"], {"action": "edit", "version": amended["version"]})
        self.assertEqual(reopened["status"], "needs_input")
        patch_field = next(f for f in reopened["fields"] if f["path"] == "patch")
        self.assertEqual(patch_field["value"]["location"], "A楼机房")

        stored = self.agent.get_plan(ACTOR, plan["id"])
        self.assertEqual(json.dumps(queries[qref], sort_keys=True), json.dumps(before, sort_keys=True))
        self.assertEqual(json.dumps(stored["_queries"][qref], sort_keys=True), json.dumps(before, sort_keys=True))

    # -- 6) unknown target select -> loaded options -> amend opens form -----

    async def test_unknown_target_select_loads_options_then_finishes_without_premature_write(self):
        broad_actor = {**ACTOR, "scopes": ["A", "B"]}
        qref = "query_" + "e" * 32
        original = {**ORIGINAL, "record_id": "rec-orig", "target_record_id": "rec-orig",
                    "building_codes": ["A"], "scope": "A"}
        queries = {qref: {"ongoing": [copy.deepcopy(original)]}}
        decision = self._update_end_decision("update", "已完成80%")
        # Remove the known target so the agent surfaces a target select.
        decision["operations"][0]["body"].pop("target_record_id")
        plan = self.agent.prepare(broad_actor, decision, self._operation_id("target"), [], queries=queries)
        target_field = next(f for f in plan["fields"] if f["path"] == "target_record_id")
        self.assertEqual(target_field["options_source"], "notice_targets")
        self.assertEqual(target_field["options"], [])

        loaded = await self.agent.field_options(broad_actor, plan["id"], "step0.target_record_id", self.request)
        field = next(f for f in loaded["fields"] if f["path"] == "target_record_id")
        self.assertEqual([option["value"] for option in field["options"]], ["rec-orig", "rec-orig-2"])

        amended = await self._amend(loaded, {"step0.target_record_id": "rec-orig"}, actor=broad_actor)
        self.assertEqual(amended["status"], "needs_input")
        self.assertTrue(any(f["path"] == "patch" and f.get("native_notice") for f in amended["fields"]))
        patch_field = next(f for f in amended["fields"] if f["path"] == "patch")
        self.assertEqual(patch_field["value"]["title"], _record_title(original))
        # The actor is also authorized for B, but the selected A record keeps its
        # building codes constrained to the chosen scope in the hydrated form.
        self.assertEqual(patch_field["value"]["building_codes"], ["A"])

        # Finish the second amend (the actual notice form).
        confirmed = await self._amend(amended, {
            "step0.patch": {**patch_field["value"], "progress": "已完成80%"},
        }, actor=broad_actor)
        self.assertEqual(confirmed["status"], "awaiting_confirmation")
        # No write before execute.
        self.assertEqual(self.notice_writes, [])
        # The queried original record id and title are never mutated.
        original = queries[qref]["ongoing"][0]
        self.assertEqual(original["record_id"], "rec-orig")
        self.assertEqual(original["title"], "A楼设备调整")

        finished = await self._execute(confirmed, actor=broad_actor)
        self.assertEqual(finished["status"], "completed", finished.get("error"))
        self.assertEqual(len(self.notice_writes), 1)
        write = self.notice_writes[0]
        self.assertEqual(write["target_record_id"], "rec-orig")
        self.assertEqual(write["patch"]["progress"], "已完成80%")

    # -- 7) pending image-upload $result stays frozen ------------------------

    async def test_pending_image_upload_result_stays_frozen_in_patch(self):
        png = io.BytesIO()
        Image.new("RGB", (2, 2), "white").save(png, format="PNG")
        image = self.files.upload(ACTOR, "proof.png", png.getvalue(), extract=False)
        decision = {"operations": [
            {"api_id": "POST /api/notice-attachments", "params": {"scope": "A", "kind": "site"}, "files": {"file": [image["id"]]}},
            {"api_id": "POST /api/workbench-actions", "body": {
                "command_format": "notice_command",
                "scope": "A",
                "work_type": "maintenance",
                "action": "start",
                "manual": True,
                "polling_work_order_exempt": True,
                "patch": {
                    **copy.deepcopy(DRAFT),
                    "site_photos": [{"upload_id": {"$result": {"step": 0, "path": "upload_id"}}, "file_name": "proof.png"}],
                },
            }},
        ]}
        plan = self.agent.prepare(ACTOR, decision, self._operation_id("frozen"), [image["id"]])
        self.assertEqual(plan["status"], "needs_input")
        stored = self.agent.get_plan(ACTOR, plan["id"])
        body = stored["operations"][1]["body"]
        # The pending $result reference is frozen in the raw patch and is never
        # resolved at prepare time.
        self.assertIn("$result", json.dumps(body["patch"], ensure_ascii=False))
        self.assertIn("site_photos", body["patch"])
        self.assertEqual(body["patch"]["site_photos"][0]["upload_id"]["$result"]["step"], 0)
        self.assertEqual(body["patch"]["site_photos"][0]["upload_id"]["$result"]["path"], "upload_id")
        # The notice form must not expose the still-unavailable result value.
        patch_field = next(f for f in plan["fields"] if f["path"] == "patch")
        self.assertNotIn("private-file-token", json.dumps(patch_field, ensure_ascii=False))
        self.assertNotIn("up-1", json.dumps(patch_field, ensure_ascii=False))
        self.assertNotIn("$result", json.dumps(patch_field, ensure_ascii=False))
        amended = await self._amend(plan, {"step1.manual_binding_choice": "unbound"})
        self.assertEqual(self.upload_writes, [])
        self.assertEqual(self.notice_writes, [])
        finished = await self._execute(amended)
        self.assertEqual(finished["status"], "completed", finished.get("error"))
        self.assertEqual(self.upload_writes, [("site", png.getvalue())])
        self.assertEqual(self.notice_writes[0]["patch"]["site_photos"], [{"upload_id": "up-1", "file_name": "proof.png"}])


if __name__ == "__main__":
    unittest.main()
