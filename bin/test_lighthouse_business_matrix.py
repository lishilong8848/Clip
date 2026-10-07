"""Bounded protocol/adapter coverage for the assistant's typed read tools and
PortalAgent write confirmation path.

This file only tests adapter/protocol behaviour against isolated Fake FastAPI
native endpoints and an in-memory Store.  It never touches real cloud/provider
services, performs no business writes other than the explicitly confirmed fake
notice-command start / drill signer-save / drill generate, and contains no
secrets.

Covered typed reads (via LighthouseModel driven by pydantic_ai FunctionModel):
  * daily checklist      -> GET /api/daily-tasks        (tasks/categories/stats)
  * water records        -> GET /api/capacity/water/records
  * plan-convergence     -> GET /api/plan-convergence/blocks
  * drills               -> GET /api/drills/{drill_id}/execution
  * critical-guard list  -> GET /api/critical-guard/tasks
  * critical-guard detail-> GET /api/critical-guard/tasks/{task_id}
  * repair detail        -> GET /api/repair-management/records/{record_id}
                             (repair_followup_status; verified progress/followup)

For each read we verify (a) native fields remain available on the source data,
(b) the correct account scope reaches the original endpoint, and (c) denied
scopes do not leak (both before the endpoint and after a buggy native result).

Requested high-risk writes are covered with real request shapes:
  * POST /api/workbench-actions  notice_command start
  * PUT  /api/drills/{drill_id}/execution  (DrillExecutionRequest)
  * POST /api/drills/{drill_id}/generate   (DrillGenerateRequest)
and prove prepare never executes before explicit confirmation, person
selection names/IDs and signature/evaluation times survive the gateway, and
a repeated confirmation does not write twice.
"""
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

from pydantic_ai.models.function import DeltaToolCall, FunctionModel

from clipflow_backend.api_models import DrillExecutionRequest, DrillGenerateRequest
from lan_bitable_template_portal.lighthouse_agent import PortalAgent
from lan_bitable_template_portal.lighthouse_ai import LighthouseAssistant
from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog
from lan_bitable_template_portal.lighthouse_files import LighthouseFiles
from lan_bitable_template_portal.lighthouse_model import LighthouseModel

ACTOR = {"id": "business-matrix-fixture", "scopes": ["D"], "is_admin": False, "learning_scopes": ["D"]}


def _make_request():
    return Request({
        "type": "http",
        "scheme": "http",
        "server": ("testserver", 80),
        "client": ("127.0.0.1", 4567),
        "path": "/api/assistant/agent",
        "root_path": "",
        "query_string": b"",
        "headers": [(b"cookie", b"fixture=a"), (b"origin", b"http://testserver")],
    })


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


def _single_tool_stream(tool_name, tool_args, final_text):
    payload = json.dumps(tool_args, ensure_ascii=False)

    async def model_stream(messages, info):
        has_tool_return = any(getattr(part, "part_kind", "") == "tool-return"
                              for message in messages for part in message.parts)
        if has_tool_return:
            yield final_text
        else:
            yield "正在查询业务接口。"
            yield {0: DeltaToolCall(name=tool_name, json_args=payload)}

    return model_stream


class BusinessMatrixTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = Store(Path(self.tmp.name) / "state.sqlite3")

        model = Mock()
        model.settings.return_value = {
            "configured": True, "enabled": True, "active_model_id": "default",
            "models": [{"id": "default", "name": "测试模型", "model": "fixture-model", "configured": True}],
        }
        model.profile.return_value = {"id": "default", "name": "测试模型", "model": "fixture-model"}
        search = Mock(side_effect=AssertionError("must use backend APIs, not local scraping"))
        self.assistant = LighthouseAssistant(self.store, search, model=model)
        self.files = LighthouseFiles(self.store)

        self.scope_hits = {}          # api_id -> [scope,...]
        self.learning_hits = []       # (scope, today) for GET /api/learning/papers
        self.notice_writes = []
        self.execution_writes = []
        self.generate_writes = []
        self.buggy_water = False
        self.failed_water_scope = ""
        self.operation_id = "matrix_write_00000001"

        app = FastAPI()

        @app.get("/api/daily-tasks")
        async def daily_tasks(scope: str, date: str = "", request: Request = None):
            self.scope_hits.setdefault("GET /api/daily-tasks", []).append(scope)
            return {"ok": True, "data": {
                "scope": scope, "date": date,
                "stats": {"total": 2, "ongoing": 1, "completed": 1, "attention": 0},
                "categories": [
                    {"key": "notice", "label": "通告", "count": 1, "tasks": [
                        {"task_id": "d1", "category": "notice", "title": "消防巡检",
                         "status": "待完成", "status_tone": "ongoing", "time": "09:00",
                         "building": "D楼", "specialty": "消防"}]},
                    {"key": "repair", "label": "检修", "count": 1, "tasks": [
                        {"task_id": "d2", "category": "repair", "title": "水泵保养",
                         "status": "已完成", "status_tone": "completed", "time": "08:30",
                         "building": "D楼", "specialty": "机电"}]},
                ],
                "tasks": [
                    {"task_id": "d1", "category": "notice", "title": "消防巡检",
                     "status": "待完成", "status_tone": "ongoing", "time": "09:00",
                     "building": "D楼", "specialty": "消防"},
                    {"task_id": "d2", "category": "repair", "title": "水泵保养",
                     "status": "已完成", "status_tone": "completed", "time": "08:30",
                     "building": "D楼", "specialty": "机电"},
                ],
                "warnings": [],
            }}

        @app.get("/api/capacity/water/records")
        async def water_records(scope: str, request: Request = None):
            self.scope_hits.setdefault("GET /api/capacity/water/records", []).append(scope)
            if scope == self.failed_water_scope:
                return {"ok": False, "error": "此楼栋资料暂不可用。"}
            record = {"record_id": "w1", "building_codes": [scope], "meter": "M-D-1",
                      "meter_value": 123, "computed_usage": 12.3,
                      "water_current_value": 123, "water_previous_value": 110,
                      "water_change_ratio": 0.118}
            if self.buggy_water:  # simulate a leaky native endpoint ignoring scope
                record = {"record_id": "w2", "building_codes": ["E"], "meter_value": 999,
                          "water_current_value": 999, "water_previous_value": 1, "water_change_ratio": 999}
            return {"ok": True, "data": {"scope": scope, "total": 1, "records": [record]}}

        @app.get("/api/plan-convergence/blocks")
        async def convergence_blocks(scope: str, request: Request = None):
            self.scope_hits.setdefault("GET /api/plan-convergence/blocks", []).append(scope)
            return {"ok": True, "data": {"scope": scope, "total": 1, "items": [
                {"block_id": "b1", "building_codes": [scope], "rule_name": "智航收敛-测点跳变",
                 "obj_name": "D楼冷水机组", "status": "命中"}]}}

        @app.get("/api/drills/{drill_id}/execution")
        async def drill_execution(drill_id: str, scope: str, request: Request = None):
            self.scope_hits.setdefault("GET /api/drills/{drill_id}/execution", []).append(scope)
            return {"ok": True, "data": {
                "drill": {"drill_id": drill_id, "name": "火灾疏散演练", "scope": scope},
                "execution": {
                    "drill_id": drill_id, "scope": scope, "version": 3,
                    "status": "synced" if self.generate_writes else "进行中",
                    "execution_version": 1, "generated_version": 1 if self.generate_writes else 0,
                    "drill_date": "2026-10-01", "first_start_time": "09:00",
                    "commander": {"source": "staff", "record_id": "p-cmd", "name": "张指挥"},
                    "evaluator": {"source": "staff", "record_id": "p-eva", "name": "李评估"},
                    "signature_time": "10:30", "evaluation_time": "11:45",
                    "participants": [{"source": "staff", "record_id": "p-p1", "name": "王参演"}],
                    "step_signers": {"task_confirm": ["p-cmd"]},
                }}}

        @app.get("/api/critical-guard/tasks")
        async def critical_guard_tasks(scope: str, request: Request = None):
            self.scope_hits.setdefault("GET /api/critical-guard/tasks", []).append(scope)
            return {"ok": True, "data": {
                "scope": scope, "count": 2,
                "tasks": [
                    {"task_id": "g1", "operation_id": "op-1", "name": "重点区域检查",
                     "target_scopes": [scope], "template_version": "v1", "status": "active",
                     "response_count": 1, "submitted_count": 0, "draft_count": 1,
                     "pending_count": 0, "complete": False,
                     "current_template_version": "v1", "template_outdated": False,
                     "created_at": 0.0, "updated_at": 0.0},
                    {"task_id": "g2", "operation_id": "op-2", "name": "天气相关检查",
                     "target_scopes": [scope], "template_version": "v1", "status": "active",
                     "response_count": 1, "submitted_count": 1, "draft_count": 0,
                     "pending_count": 0, "complete": True,
                     "current_template_version": "v1", "template_outdated": False,
                     "created_at": 0.0, "updated_at": 0.0},
                ]}}

        @app.get("/api/critical-guard/tasks/{task_id}")
        async def critical_guard_task_detail(task_id: str, scope: str, request: Request = None):
            self.scope_hits.setdefault("GET /api/critical-guard/tasks/{task_id}", []).append(scope)
            return {"ok": True, "data": {
                "task_id": task_id, "scope": scope, "name": "重点区域检查",
                "target_scopes": [scope], "status": "active",
                "responses": [
                    {"response_id": "r1", "task_id": task_id, "scope": scope,
                     "sheet_type": "critical", "status": "draft",
                     "cells": {"A1": "值"}, "version": 1,
                     "signatures": [{"source": "staff", "record_id": "p-1", "name": "检查员"}]}
                ],
                "catalog": {"template_version": "v1"},
                "current_signer": {"record_id": "p-1", "name": "检查员"},
                "current_template_version": "v1", "template_outdated": False,
            }}

        @app.get("/api/repair-management/records/{record_id}")
        async def repair_record(record_id: str, scope: str, request: Request = None):
            self.scope_hits.setdefault("GET /api/repair-management/records/{record_id}", []).append(scope)
            verified = not record_id.startswith("unverified-")
            return {"ok": True, "data": {"record": {
                "record_id": record_id, "scope": scope, "title": "D楼水泵维修",
                "building_codes": [scope], "workflow": "in_progress", "is_completed": False,
                "progress_percent": 75, "followup_count": 3,
                "followup_state_verified": verified, "status": "处理中",
                "display_fields": {"当前维修进度": "75%"}, "raw_fields": {}, "summary_only": False},
                "fields": [], "schema_warnings": []}}

        @app.post("/api/workbench-actions")
        async def workbench_actions(request: Request):
            payload = await request.json()
            self.notice_writes.append(payload)
            return {"ok": True, "data": {"accepted": True, "notice_id": "notice-1"}}

        @app.put("/api/drills/{drill_id}/execution")
        async def drill_execution_save(drill_id: str, body: DrillExecutionRequest, scope: str = "", request: Request = None):
            self.execution_writes.append(body.model_dump())
            return {"ok": True, "data": {"stored": True, "drill_id": drill_id, "scope": scope}}

        @app.post("/api/drills/{drill_id}/generate")
        async def drill_generate(drill_id: str, body: DrillGenerateRequest, scope: str = "", request: Request = None):
            self.generate_writes.append(body.model_dump())
            return {"ok": True, "data": {"queued": True, "drill_id": drill_id, "scope": scope, "execution": {"execution_version": 1, "status": "queued"}}}

        @app.get("/api/learning/papers")
        async def learning_papers(scope: str, today: str = "0"):
            self.learning_hits.append((scope, today))
            return {"ok": True, "data": {"items": [{"id": "paper-d-1001", "scope": scope}], "total": 1}}

        self.catalog = PortalAPICatalog(app)
        self.agent = PortalAgent(self.assistant, self.catalog, self.files)
        self.request = _make_request()

    def _new_engine(self, tool_name, tool_args, final_text, cached=True):
        @asynccontextmanager
        async def factory(custom, profile):
            yield FunctionModel(stream_function=_single_tool_stream(tool_name, tool_args, final_text))

        def _cached(kind, scopes, allowed):
            return [], []

        engine = LighthouseModel(self.agent,
                                 cached_reader=(_cached if cached is True else cached or None),
                                 model_factory=factory)
        return engine

    async def _run_answer(self, engine, question, final_hint="完成。", actor=None):
        actor = copy.deepcopy(actor or ACTOR)
        async def emit(kind, payload=None):
            return None

        async def authorize():
            return {**copy.deepcopy(actor), "allowed_scopes": actor["scopes"]}

        turn = {"question": question, "_profile": {"id": "default", "name": "测试模型", "model": "fixture-model"},
                "operation_id": "matrix_read_00000001", "run_id": "run-1", "file_ids": []}
        return await engine.answer(actor, turn, [], self.request, emit, authorize, {})

    # ---- typed read tools: native fields survive + correct scope reaches endpoint ----

    async def test_daily_checklist_read_native_fields_and_scope(self):
        engine = self._new_engine("query",
                                  {"operation": {"api_id": "GET /api/daily-tasks",
                                                  "params": {"scope": "D", "date": "2026-10-01"}}},
                                  "以下是D楼每日任务清单。")
        result = await self._run_answer(engine, "查看D楼每日任务清单")
        self.assertEqual(self.scope_hits["GET /api/daily-tasks"], ["D"])
        data = result["sources"][0]["data"]
        self.assertEqual(data["stats"]["total"], 2)
        tasks = data["tasks"]
        self.assertEqual(tasks[0]["task_id"], "d1")
        self.assertEqual(tasks[0]["title"], "消防巡检")
        self.assertEqual(tasks[0]["building"], "D楼")
        self.assertEqual(data["categories"][0]["label"], "通告")
        self.assertEqual(data["categories"][0]["tasks"][0]["status_tone"], "ongoing")

    async def test_water_records_read_native_fields_and_scope(self):
        engine = self._new_engine("query",
                                  {"operation": {"api_id": "GET /api/capacity/water/records",
                                                  "params": {"scope": "D"}}},
                                  "D楼水耗记录已返回。")
        result = await self._run_answer(engine, "查看D楼水耗记录")
        self.assertEqual(self.scope_hits["GET /api/capacity/water/records"], ["D"])
        record = result["sources"][0]["data"]["records"][0]
        self.assertEqual(record["water_current_value"], 123)
        self.assertEqual(record["water_previous_value"], 110)
        self.assertAlmostEqual(record["water_change_ratio"], 0.118)
        self.assertEqual(record["building_codes"], ["D"])

    async def test_learning_today_state_returns_only_original_page_link_no_native_read(self):
        engine = self._new_engine("query",
                                  {"operation": {"api_id": "GET /api/learning/papers",
                                                  "params": {"scope": "D", "today": "0"}}},
                                  "今日D楼学练题单已发布（paper-d-1001），画像学练见 /learning 学习页。")
        result = await self._run_answer(engine, "今天D楼学练题单发布了吗")
        self.assertEqual(self.learning_hits, [("D", "0")])
        self.assertEqual(len(result["sources"]), 1)
        self.assertIn("/learning", result["answer"])
        self.assertIn("画像学练", result["answer"])
        self.assertIn("paper-d-1001", result["answer"])

    async def test_all_water_reads_split_by_permission_and_keep_partial_failure(self):
        actor = {**ACTOR, "scopes": ["A", "D"]}
        for failed in ("", "D"):
            with self.subTest(failed=failed):
                self.failed_water_scope = failed
                self.scope_hits.clear()
                engine = self._new_engine("query", {"operation": {"api_id": "GET /api/capacity/water/records", "params": {"scope": "ALL"}}}, "已逐楼查询水耗。")
                result = await self._run_answer(engine, "查询我有权限的全部楼栋水耗记录", actor=actor)
                self.assertEqual(self.scope_hits["GET /api/capacity/water/records"], ["A", "D"])
                data = result["sources"][0]["data"]
                self.assertEqual(data["complete"], not bool(failed))
                self.assertEqual([row["scope"] for row in data["buildings"]], ["A", "D"])
                self.assertEqual(data["buildings"][0]["data"]["records"][0]["water_current_value"], 123)
                if failed:
                    self.assertFalse(data["buildings"][1]["ok"])
                    self.assertIsNone(data["buildings"][1]["data"])

    async def test_plan_convergence_read_native_fields_and_scope(self):
        engine = self._new_engine("query",
                                  {"operation": {"api_id": "GET /api/plan-convergence/blocks",
                                                  "params": {"scope": "D"}}},
                                  "D楼计划收敛屏蔽记录已返回。")
        result = await self._run_answer(engine, "查看D楼计划收敛屏蔽记录")
        self.assertEqual(self.scope_hits["GET /api/plan-convergence/blocks"], ["D"])
        item = result["sources"][0]["data"]["items"][0]
        self.assertEqual(item["block_id"], "b1")
        self.assertEqual(item["building_codes"], ["D"])
        self.assertEqual(item["status"], "命中")

    async def test_drill_execution_read_native_fields_and_scope(self):
        engine = self._new_engine("query",
                                  {"operation": {"api_id": "GET /api/drills/{drill_id}/execution",
                                                  "path_params": {"drill_id": "drill-1"},
                                                  "params": {"scope": "D"}}},
                                  "演练执行进度为进行中。")
        result = await self._run_answer(engine, "查看D楼演练drill-1执行进度")
        self.assertEqual(self.scope_hits["GET /api/drills/{drill_id}/execution"], ["D"])
        data = result["sources"][0]["data"]
        self.assertEqual(data["drill"]["drill_id"], "drill-1")
        execution = data["execution"]
        self.assertEqual(execution["scope"], "D")
        self.assertEqual(execution["version"], 3)
        self.assertEqual(execution["status"], "进行中")
        self.assertEqual(execution["commander"]["record_id"], "p-cmd")
        self.assertEqual(execution["participants"][0]["name"], "王参演")

    async def test_critical_guard_list_read_native_fields_and_scope(self):
        engine = self._new_engine("query",
                                  {"operation": {"api_id": "GET /api/critical-guard/tasks",
                                                  "params": {"scope": "D"}}},
                                  "已返回D楼重保检查任务。")
        result = await self._run_answer(engine, "查看D楼重保检查任务")
        self.assertEqual(self.scope_hits["GET /api/critical-guard/tasks"], ["D"])
        data = result["sources"][0]["data"]
        self.assertEqual(data["count"], 2)
        tasks = data["tasks"]
        self.assertEqual([t["name"] for t in tasks], ["重点区域检查", "天气相关检查"])
        self.assertTrue(all(t["target_scopes"] == ["D"] for t in tasks))
        self.assertEqual(tasks[0]["current_template_version"], "v1")

    async def test_critical_guard_detail_read_native_fields_and_scope(self):
        engine = self._new_engine("query",
                                  {"operation": {"api_id": "GET /api/critical-guard/tasks/{task_id}",
                                                  "path_params": {"task_id": "task-1"},
                                                  "params": {"scope": "D"}}},
                                  "已返回D楼重保任务详情。")
        result = await self._run_answer(engine, "查看D楼重保任务task-1详情")
        self.assertEqual(self.scope_hits["GET /api/critical-guard/tasks/{task_id}"], ["D"])
        data = result["sources"][0]["data"]
        self.assertEqual(data["task_id"], "task-1")
        self.assertEqual(len(data["responses"]), 1)
        self.assertEqual(data["responses"][0]["scope"], "D")
        self.assertEqual(data["responses"][0]["response_id"], "r1")
        self.assertEqual(data["responses"][0]["cells"], {"A1": "值"})
        self.assertEqual(data["current_signer"]["name"], "检查员")
        self.assertEqual(data["catalog"]["template_version"], "v1")

    async def test_repair_followup_native_fields_and_scope(self):
        engine = self._new_engine("repair_followup_status",
                                  {"record_id": "repair-1", "scope": "D"},
                                  "维修项目repair-1跟进3条，进度已完成75%。")
        result = await self._run_answer(engine, "D楼维修项目repair-1的跟进进度")
        self.assertEqual(self.scope_hits["GET /api/repair-management/records/{record_id}"], ["D"])
        record = result["sources"][0]["data"]["record"]
        self.assertEqual(record["progress_percent"], 75)
        self.assertEqual(record["followup_count"], 3)
        self.assertTrue(record["followup_state_verified"])

    async def test_repair_unverified_followup_count_is_nulled(self):
        # followup_state_verified=False must not surface a possibly-stale count.
        engine = self._new_engine("repair_followup_status",
                                  {"record_id": "unverified-repair-1", "scope": "D"},
                                  "跟进状态尚未确认，跟进数量未知。")
        result = await self._run_answer(engine, "D楼维修项目unverified-repair-1的跟进进度")
        record = result["sources"][0]["data"]["record"]
        self.assertIsNone(record["followup_count"])
        self.assertFalse(record["followup_state_verified"])
        self.assertEqual(record["progress_percent"], 75)

    # ---- denied scopes do not leak ----

    async def test_out_of_scope_operation_rejected_before_endpoint(self):
        # Model asks for scope E but the account only has D: must fail before the
        # original endpoint is reached.
        engine = self._new_engine("query",
                                  {"operation": {"api_id": "GET /api/daily-tasks",
                                                  "params": {"scope": "E"}}},
                                  "无法查询该范围。")
        result = await self._run_answer(engine, "D楼水耗记录怎么样")
        self.assertEqual(self.scope_hits.get("GET /api/daily-tasks"), None)
        self.assertEqual(self.scope_hits, {})
        self.assertEqual(result["sources"], [])
        self.assertNotIn("d1", json.dumps(result, ensure_ascii=False))

    async def test_leaked_result_scope_rejected_after_endpoint(self):
        # Native endpoint (buggy) returns another building's record for scope D;
        # the adapter must reject the leaked scope even though the endpoint ran.
        self.buggy_water = True
        engine = self._new_engine("query",
                                  {"operation": {"api_id": "GET /api/capacity/water/records",
                                                  "params": {"scope": "D"}}},
                                  "返回了一条记录但被拒绝使用。")
        result = await self._run_answer(engine, "D楼水耗记录怎么样")
        self.assertEqual(self.scope_hits["GET /api/capacity/water/records"], ["D"])
        self.assertEqual(result["sources"], [])
        serialized = json.dumps(result, ensure_ascii=False)
        self.assertNotIn("w2", serialized)
        self.assertNotIn("D楼水耗记录", serialized)

    # ---- requested writes: notice start, drill signer-save + generate ----

    async def test_notice_command_start_confirm_no_duplicate_write(self):
        qref = "query_" + "a" * 32
        draft = {
            "title": "D楼设备维护", "progress": "已完成60%",
            "start_time": "2026-09-30 09:00", "end_time": "2026-09-30 11:00",
            "location": "D楼机房", "content": "维护检查", "reason": "例行维保", "impact": "无业务影响",
            "specialty": "暖通", "maintenance_cycle": "每月", "execution_party": "自维", "building_codes": ["D"],
        }
        decision = {
            "operations": [{
                "api_id": "POST /api/workbench-actions",
                "body": {
                    "command_format": "notice_command",
                    "scope": "D",
                    "work_type": "maintenance",
                    "action": "start",
                    "manual": True,
                    "polling_work_order_exempt": True,
                    "manual_binding_choice": "unbound",
                    "patch": {"$query": {"ref": qref, "path": "draft"}},
                },
            }],
        }
        queries = {qref: {"draft": draft}}
        plan = self.agent.prepare(ACTOR, decision, self.operation_id, [], queries=queries)
        self.assertEqual(plan["status"], "needs_input")
        self.assertTrue(any(field.get("native_notice") for field in plan["fields"]))
        self.assertEqual(self.notice_writes, [])
        plan = self.agent.amend(ACTOR, plan["id"], {"version": plan["version"], "values": {}})
        self.assertEqual(plan["status"], "awaiting_confirmation")
        self.assertEqual(plan["risk"], "high")
        # Never executed before confirmation.
        self.assertEqual(self.notice_writes, [])

        plan = await self.agent.confirm(ACTOR, plan["id"], {"version": plan["version"], "stage": "review"}, self.request)
        self.assertEqual(plan["status"], "awaiting_second_confirmation")
        self.assertEqual(self.notice_writes, [])

        await self.agent.confirm(ACTOR, plan["id"], {"version": plan["version"], "stage": "execute"}, self.request)
        # A repeat confirm click on the running plan must not start a second write.
        running = self.agent.get_plan(ACTOR, plan["id"])
        self.assertEqual(running["status"], "running")
        await self.agent.confirm(ACTOR, running["id"], {"version": running["version"], "stage": "execute"}, self.request)

        await gather_tasks(self.agent)
        self.assertEqual(len(self.notice_writes), 1)
        body = self.notice_writes[0]
        self.assertEqual(body["command_format"], "notice_command")
        self.assertEqual(body["action"], "start")
        self.assertEqual(body["scope"], "D")
        self.assertEqual(body["work_type"], "maintenance")
        self.assertEqual(body["manual_binding_choice"], "unbound")
        patch = body["patch"]
        self.assertEqual(patch["title"], "D楼设备维护")
        self.assertEqual(patch["progress"], "已完成60%")
        self.assertEqual(patch["start_time"], "2026-09-30 09:00")
        self.assertEqual(patch["end_time"], "2026-09-30 11:00")
        finished = self.agent.get_plan(ACTOR, plan["id"])
        self.assertEqual(finished["status"], "completed")

    async def test_drill_execution_and_generate_preserve_people_times_no_duplicate(self):
        decision = {
            "operations": [
                {
                    "api_id": "PUT /api/drills/{drill_id}/execution",
                    "path_params": {"drill_id": "drill-01"},
                    "params": {"scope": "D"},
                    "body": {
                        "expected_version": 0,
                        "drill_date": "2026-10-01",
                        "first_start_time": "09:00",
                        "simulation_scenario": "模拟B1层电气火灾",
                        "commander": {"source": "staff", "record_id": "p-cmd", "name": "张指挥"},
                        "evaluator": {"source": "staff", "record_id": "p-eva", "name": "李评估"},
                        "signature_time": "10:30",
                        "evaluation_time": "11:45",
                        "participants": [
                            {"source": "staff", "record_id": "p-p1", "name": "王参演1"},
                            {"source": "external", "record_id": "ext-2", "name": "外聘赵"},
                            {"source": "staff", "record_id": "p-eva", "name": "李评估"},
                        ],
                        "step_signers": {"task_confirm": ["p-cmd"], "scene_exec": ["p-p1", "p-eva"]},
                    },
                },
                {
                    "api_id": "POST /api/drills/{drill_id}/generate",
                    "path_params": {"drill_id": "drill-01"},
                    "params": {"scope": "D"},
                    "body": {"expected_version": 0},
                },
            ],
        }
        execution = {**decision["operations"][0]["body"], "drill_id": "drill-01", "scope": "D", "version": 0}
        definition = {"drill_id": "drill-01", "scope": "D", "configuration": {"steps": [
            {"row": "task_confirm", "signature_slots": 1}, {"row": "scene_exec", "signature_slots": 2}]}}
        plan = self.agent.prepare(ACTOR, decision, self.operation_id, [], queries={"current": {"drill": definition, "execution": execution}})
        self.assertEqual(plan["status"], "needs_input")
        public = self.agent.public_plan(plan)
        plan = self.agent.amend(ACTOR, plan["id"], {"version": plan["version"], "values": {field["name"]: field["value"] for field in public["fields"]}})
        self.assertEqual(plan["status"], "awaiting_confirmation")
        preview = plan["operations"][0]["body"]
        self.assertEqual(preview["signature_time"], "10:30")
        self.assertEqual(preview["evaluation_time"], "11:45")
        self.assertEqual(plan["risk"], "high")
        self.assertEqual(self.execution_writes, [])
        self.assertEqual(self.generate_writes, [])

        plan = await self.agent.confirm(ACTOR, plan["id"], {"version": plan["version"], "stage": "review"}, self.request)
        self.assertEqual(plan["status"], "awaiting_second_confirmation")
        self.assertEqual(self.execution_writes, [])
        self.assertEqual(self.generate_writes, [])

        await self.agent.confirm(ACTOR, plan["id"], {"version": plan["version"], "stage": "execute"}, self.request)
        running = self.agent.get_plan(ACTOR, plan["id"])
        self.assertEqual(running["status"], "running")
        await self.agent.confirm(ACTOR, running["id"], {"version": running["version"], "stage": "execute"}, self.request)

        await gather_tasks(self.agent)
        self.assertEqual(len(self.execution_writes), 1)
        self.assertEqual(len(self.generate_writes), 1)
        body = self.execution_writes[0]
        # Person selection survives with stable names/IDs.
        self.assertEqual(body["commander"]["record_id"], "p-cmd")
        self.assertEqual(body["commander"]["name"], "张指挥")
        self.assertEqual(body["evaluator"]["record_id"], "p-eva")
        self.assertEqual(body["evaluator"]["name"], "李评估")
        self.assertEqual(body["participants"][0]["record_id"], "p-cmd")
        self.assertEqual(body["participants"][1]["record_id"], "p-p1")
        self.assertEqual(body["participants"][2]["name"], "外聘赵")
        self.assertEqual(body["step_signers"]["task_confirm"], ["p-cmd"])
        self.assertEqual(body["step_signers"]["scene_exec"], ["p-p1", "p-eva"])
        # Signature and evaluation times stay independent.
        self.assertEqual(body["signature_time"], "10:30")
        self.assertEqual(body["evaluation_time"], "11:45")
        # Generate is dispatched once with the real request body.
        self.assertEqual(self.generate_writes[0]["expected_version"], 0)
        finished = self.agent.get_plan(ACTOR, plan["id"])
        self.assertEqual(finished["status"], "completed")


if __name__ == "__main__":
    unittest.main()
