"""Isolated unfinished-work checks. No live data or cloud writes."""
import asyncio
import copy
import gc
import json
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fastapi import FastAPI, Request
from lan_bitable_template_portal.lighthouse_pending import cached_items, collect_pending, guard_reply, pending_reply
from lan_bitable_template_portal.lighthouse_ai import AssistantError, LighthouseAssistant
from lan_bitable_template_portal.lighthouse_agent import PortalAgent
from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog
from lan_bitable_template_portal.lighthouse_files import LighthouseFiles

ACTOR = {"id": "fixture-a", "scopes": ["A"], "is_admin": False}


class PendingTests(unittest.IsolatedAsyncioTestCase):
    async def test_explicit_learning_without_permission_is_not_zero(self):
        with self.assertRaises(AssistantError) as error:
            await collect_pending(ACTOR, 'A楼学练还有多少未答题', self.invoke, self.cached, groups_only={'learning'})
        self.assertEqual(error.exception.status, 403)
        self.assertEqual(self.calls, [])

    def setUp(self):
        self.calls = []
        self.rows = [{"record_id": "n" + str(i), "title": "A楼旧维保" + str(i), "scope": "A", "status": "进行中", "work_type": "maintenance", "start_time": "2025-09-01"} for i in range(41)]
        self.rows += [dict(self.rows[0]), {"record_id": "ended", "scope": "A", "status": "已结束"},
                      {"record_id": "end-draft", "title": "结束草稿", "scope": "A", "work_type": "change", "status": "结束", "_has_unuploaded_changes": True}]

    async def invoke(self, op):
        self.calls.append(copy.deepcopy(op))
        api, params = op["api_id"], op.get("params") or {}
        if api == "GET /api/workbench":
            page = int(params.get("ongoing_page", 1))
            rows = self.rows[(page - 1) * 20:page * 20]
            data = {"source_snapshot_ready": True, "records": [], "records_pagination": {"total": 0}, "ongoing": rows, "ongoing_pagination": {"total": len(self.rows), "has_more": page * 20 < len(self.rows)}, "daily_summary": {"completed": 999}}
        elif api == "GET /api/repair-management/records":
            data = {"records": [{"record_id": "r1", "title": "A楼光伏检修", "building_codes": ["A"], "workflow": "未开始", "is_completed": False},
                                {"record_id": "r2", "scope": "A", "is_completed": True, "workflow": "维修完成"}], "total": 2, "has_more": False}
        elif api == "GET /api/cabinet-power/batches":
            data = {"items": [{"batch_id": "b1", "scopes": ["A"], "title": "A楼图片批次", "is_todo": True, "pending_rows": 2}], "total": 1}
        elif api == "GET /api/drills":
            data = {"items": [{"drill_id": "d1", "name": "演练", "status": "published", "execution": {"status": "completed"}}]}
        elif api == "GET /api/critical-guard/tasks":
            data = {"tasks": []}
        elif api == 'GET /api/learning/papers':
            data = {'items': [{'id': 'learning_' + params['scope'], 'date': '2026-10-03', 'scope': params['scope'], 'status': 'pending', 'stats': {'total': 10, 'answered': 3}}], 'total': 1}
        else:
            raise AssertionError("Unexpected API " + api)
        # The public samples are deliberately truncated: counting must use raw API data.
        return {"ok": True, "status": 200, "data": {"items": []}, "_raw": data}

    def cached(self, kind, scopes):
        self.assertEqual(scopes, ["A"])
        return ([{"id": "event1", "title": "轮巡发现A-127-2#冷水机组故障", "scopes": ["A"], "status": "处理中", "url": "/?mode=events&scope=A"}] if kind == "events" else []), []

    async def test_all_dates_complete_paging_dedup_and_no_completed_or_daily_rows(self):
        data = await collect_pending(ACTOR, "现在还有哪些未完成工作", self.invoke, self.cached)
        groups = {row["key"]: row for row in data["groups"]}
        self.assertTrue(data["complete"])
        self.assertEqual(groups["notices"]["count"], 42)
        self.assertEqual(groups["notices"]["type_counts"]["维保"], 41)
        self.assertEqual(groups["notices"]["type_counts"]["变更"], 1)
        self.assertEqual(groups["notices"]["remaining"], 32)
        self.assertEqual(groups["repairs"]["count"], 1)
        self.assertEqual(groups["repairs"]["items"][0]["status"], "未开始")
        self.assertNotIn("learning", groups)
        self.assertFalse([c for c in self.calls if "learning" in c["api_id"]])
        self.assertEqual(groups["drills"]["count"], 0)
        board = [call for call in self.calls if call["api_id"] == "GET /api/workbench" and call["params"].get("sections") == "ongoing"]
        self.assertEqual([call["params"]["ongoing_page"] for call in board], [1, 2, 3])
        self.assertTrue(all("month" not in call["params"] and "date" not in call["params"] for call in board))
        self.assertTrue(all("daily" not in call["api_id"] for call in self.calls))
        reply = pending_reply(data)
        self.assertIn("42", reply)
        self.assertIn("冷水机组故障", reply)
        self.assertNotIn("未命名事件", reply)
        self.assertIn("不相加为独立工作总数", reply)

    async def test_unassigned_repair_is_not_a_complete_count_or_a_scope_guess(self):
        async def invoke(operation):
            result = await self.invoke(operation)
            if operation['api_id'] == 'GET /api/repair-management/records':
                result['_raw']['records'].append({'record_id': 'unassigned', 'title': '未填写楼栋的项目',
                    'is_completed': False, 'workflow': '维修中'})
                result['_raw']['total'] += 1
            return result
        actor = {**ACTOR, 'scopes': ['110', 'A', 'B', 'C', 'D', 'E', 'H']}
        data = await collect_pending(actor, '现在未完成的维修项目', invoke, self.cached, groups_only={'repairs'})
        group = data['groups'][0]
        self.assertFalse(group['available'])
        self.assertIsNone(group['count'])
        self.assertEqual(group['known_count'], 1)
        self.assertEqual([row['id'] for row in group['items']], ['r1'])
        self.assertIn('楼栋', '；'.join(group['warnings']))
        reply = pending_reply(data, details=False)
        self.assertIn('已核对 1', reply)
        self.assertNotIn('未填写楼栋的项目', reply)
        self.assertNotIn('为 **0', reply)

    async def test_pending_plans_are_included_without_duplicate_started_plan(self):
        self.rows[0]["source_record_id"] = "started"
        async def more(op):
            result = await self.invoke(op)
            if op["api_id"] == "GET /api/workbench" and op["params"].get("sections") == "records":
                result["_raw"].update(records=[
                    {"record_id": "started", "scope": "A", "work_type": "maintenance", "source_progress": "未开始", "title": "已开始的计划"},
                    {"record_id": "new", "scope": "A", "work_type": "maintenance", "source_progress": "未开始", "title": "尚未发送计划"}], records_pagination={"total": 2})
            return result
        progress = []
        async def emit(label): progress.append(label)
        data = await collect_pending(ACTOR, "今天未结束的工作有哪些", more, self.cached, on_progress=emit)
        groups = {row["key"]: row for row in data["groups"]}
        self.assertEqual(groups["plans"]["count"], 1)
        self.assertNotIn("learning", groups)
        self.assertIn("尚未发送计划", pending_reply(data))
        self.assertFalse(any("学练" in step for step in progress))
        self.assertFalse([call for call in self.calls if "learning" in call["api_id"]])

    async def test_only_ongoing_notice_count_does_not_query_other_modules(self):
        data = await collect_pending(ACTOR, '今天有多少条进行中的通告', self.invoke, self.cached, groups_only={'notices'})
        self.assertEqual({call['api_id'] for call in self.calls}, {'GET /api/workbench'})
        self.assertTrue(all(call['params']['sections'] == 'ongoing' for call in self.calls))
        reply = pending_reply(data, details=False)
        self.assertIn('42 条', reply)
        self.assertIn('维保 41 条', reply)
        self.assertNotIn('未完成工作', reply)
        self.assertNotIn('维修项目', reply)
        self.assertNotIn('学练', reply)

    async def test_full_notice_list_keeps_all_pages_instead_of_ten_row_preview(self):
        data = await collect_pending(ACTOR, '完整未结束通告', self.invoke, None, groups_only={'notices'}, item_limit=None)
        group = data['groups'][0]
        self.assertTrue(data['complete'])
        self.assertEqual(len(group['items']), 42)
        self.assertEqual(group['remaining'], 0)
        self.assertIn('A楼旧维保40', pending_reply(data))
        self.assertNotIn('另有', pending_reply(data))
        self.assertNotIn('ended', [row['id'] for row in group['items']])

    async def test_user_benchmark_250_plans_7_ongoing_and_per_type_counts(self):
        plans = [{"record_id": f"p{i}", "scope": "A", "source_progress": "未开始", "title": f"隔离计划{i}",
                  "work_type": "maintenance" if i < 247 else "change" if i < 249 else "repair"} for i in range(250)]
        ongoing = [{"record_id": f"n{i}", "scope": "A", "status": "进行中", "title": f"隔离通告{i}",
                    "work_type": "maintenance" if i < 2 else "repair"} for i in range(7)]
        async def fixture(op):
            self.assertEqual(op["api_id"], "GET /api/workbench")
            params = op["params"]
            key = "records" if params["sections"] == "records" else "ongoing"
            rows = plans if key == "records" else ongoing
            page = params[key + "_page"]
            return {"ok": True, "_raw": {"source_snapshot_ready": True, key: rows[(page-1)*200:page*200], key + "_pagination": {"total": len(rows)}}}
        data = await collect_pending(ACTOR, "当前待办", fixture, self.cached, groups_only={"plans", "notices"})
        groups = {group["key"]: group for group in data["groups"]}
        self.assertEqual(groups["plans"]["count"], 250)
        self.assertEqual(groups["notices"]["count"], 7)
        self.assertEqual(groups["plans"]["type_counts"]["变更"], 2)
        self.assertEqual(groups["notices"]["type_counts"]["变更"], 0)
        self.assertEqual(groups["plans"]["type_counts"]["检修"], 1)
        self.assertEqual(groups["notices"]["type_counts"]["检修"], 5)

    async def test_learning_scopes_never_queried_or_included_in_pending(self):
        data = await collect_pending(ACTOR, "未答题", self.invoke, self.cached)
        self.assertNotIn("learning", {row["key"] for row in data["groups"]})
        self.assertFalse([c for c in self.calls if "learning" in c["api_id"]])
        self.assertNotIn("学练", pending_reply(data))

    async def test_partial_failure_and_broken_paging_are_unknown_not_zero(self):
        async def broken(op):
            if op["api_id"] == "GET /api/workbench":
                return {"ok": True, "_raw": {"source_snapshot_ready": True, "ongoing": self.rows[:20], "ongoing_pagination": {"total": 42}}}
            if op["api_id"] == "GET /api/repair-management/records":
                return {"ok": False, "status": 503, "error": "读取暂忙"}
            return await self.invoke(op)
        data = await collect_pending(ACTOR, "未完成工作", broken, self.cached)
        groups = {row["key"]: row for row in data["groups"]}
        self.assertFalse(data["complete"])
        self.assertIsNone(groups["notices"]["count"])
        self.assertIsNone(groups["repairs"]["count"])
        self.assertIn("分页", groups["notices"]["error"])
        self.assertIn("[未完成维修项目]", pending_reply(data))
        self.assertIn("| 待确认 |", pending_reply(data))

    async def test_scope_denied_before_any_business_read(self):
        with self.assertRaises(AssistantError) as error:
            await collect_pending(ACTOR, "B楼未完成工作", self.invoke, self.cached)
        self.assertEqual(error.exception.status, 403)
        self.assertEqual(self.calls, [])
        with self.assertRaises(AssistantError):
            await collect_pending(ACTOR, "园区未完成工作", self.invoke, self.cached)
        self.assertEqual(self.calls, [])

    async def test_agent_uses_pending_api_once_and_returns_finished_conversation(self):
        with tempfile.TemporaryDirectory() as directory:
            docs = {}
            class Store:
                db_path = Path(directory) / "state.sqlite3"
                def get_document(self, namespace, key): return copy.deepcopy(docs.get((namespace, key)))
                def put_document(self, namespace, key, value): docs[namespace, key] = copy.deepcopy(value)
            store, model, app = Store(), Mock(), FastAPI()
            model.settings.return_value = {"configured": True, "enabled": True, "active_model_id": "default", "models": [{"id": "default", "name": "测试", "model": "mock", "configured": True}]}
            model.profile.return_value = {"id": "default", "name": "测试", "model": "mock"}
            assistant = LighthouseAssistant(store, Mock(side_effect=AssertionError("No scraping")), model=model)
            reads = []
            @app.get("/api/assistant/pending")
            async def pending(request: Request):
                self.assertEqual(request.cookies.get("fixture"), "a")
                reads.append(request.query_params["q"])
                return {"ok": True, "data": await collect_pending(ACTOR, request.query_params["q"], self.invoke, self.cached)}
            agent = PortalAgent(assistant, PortalAPICatalog(app), LighthouseFiles(store))
            request = Request({"type": "http", "scheme": "http", "server": ("testserver", 80), "client": ("127.0.0.1", 1), "path": "/api/assistant/agent", "root_path": "", "query_string": b"", "headers": [(b"cookie", b"fixture=a"), (b"origin", b"http://testserver")]})
            payload = {"question": "现在有哪些未完成工作", "conversation_id": assistant.conversation(ACTOR)["conversation_id"], "operation_id": "pending_fixture_00001"}
            result = await agent.chat(ACTOR, payload, request)
            self.assertFalse(result["busy"])
            self.assertEqual(result["turns"][-1]["status"], "completed")
            self.assertEqual(result["turns"][-1]["model_name"], "灯塔业务查询")
            self.assertIn("scope=A", result["turns"][-1]["sources"][0]["url"])
            self.assertIn("42", result["turns"][-1]["answer"])
            self.assertEqual(await agent.chat(ACTOR, payload, request), result)
            self.assertEqual(len(reads), 1)
            model.complete.assert_not_called()

    def test_event_names_scope_and_closed_transferred_event(self):
        from lan_bitable_template_portal.portal_service import MaintenancePortalService
        service = MaintenancePortalService.__new__(MaintenancePortalService)
        records = [{"record_id": "event1", "display_fields": {"告警描述": "A楼冷水机组故障", "机楼": "A楼", "最终状态": "进行中"}},
                   {"record_id": "closed", "display_fields": {"告警描述": "A楼已闭环转检修", "机楼": "A楼", "最终状态": "检修中", "事件目前进展": "事件闭环"}},
                   {"record_id": "secret-b", "display_fields": {"告警描述": "B楼故障", "机楼": "B楼", "最终状态": "进行中"}}]
        service._load_repair_management_event_records = Mock()
        service._event_source_config = Mock(return_value=("app", "events_table", "event_notice"))
        service._repair_snapshot_requires_workflow_refresh = Mock(return_value=False)
        service._repair_snapshot_from_local = Mock(return_value=([], {}, records))
        snapshot = {"status": "active", "refreshed_at": time.time(), "app_token": "app",
                    "table_id": "events_table", "fields": [], "records": records}
        store = SimpleNamespace(get_repair_snapshot_meta=lambda _: {"exists": True},
                                get_repair_snapshot=lambda _: snapshot,
                                list_visible_qt_active_items=lambda: [])
        result = cached_items("events", ["A"], SimpleNamespace(service=service, state_store=store))
        self.assertEqual(len(result), 3)
        rows, warnings, metadata = result
        service._load_repair_management_event_records.assert_not_called()
        self.assertEqual(warnings, [])
        self.assertEqual([row["id"] for row in rows], ["event1"])
        self.assertEqual(rows[0]["title"], "A楼冷水机组故障")
        self.assertEqual(rows[0]["scopes"], ["A"])
        self.assertEqual(metadata['source_freshness'][0]['last_cloud_sync_at'], snapshot['refreshed_at'])

    async def test_cached_event_provenance_survives_bridge_json_and_zero_counts(self):
        stamp = 1750000000
        freshness = {'kind': 'local_cache', 'last_cloud_sync_at': stamp, 'includes_qt_changes': True}
        for items in ([{'id': 'event-local', 'title': '缓存事件', 'scopes': ['A'], 'status': '处理中'}], []):
            loaded = json.loads(json.dumps((items, [], {'source_freshness': [freshness]})))
            data = await collect_pending(ACTOR, '未闭环事件', self.invoke, lambda *_: loaded, groups_only={'events'})
            group = data['groups'][0]
            self.assertTrue(group['available'])
            self.assertEqual(group['count'], len(items))
            self.assertEqual(group['source_freshness'], [freshness])
            reply = pending_reply(data)
            self.assertIn('本机已知记录', reply)
            self.assertIn('2025-06-15 23:06:40', reply)
            self.assertIn('未重新同步云端', reply)
            self.assertNotIn('当前**', reply)
        self.assertEqual(self.calls, [])

    def test_events_stale_empty_snapshot_is_unknown_not_zero(self):
        from lan_bitable_template_portal.portal_service import MaintenancePortalService
        service = MaintenancePortalService.__new__(MaintenancePortalService)
        service._event_source_config = Mock(return_value=("app", "events_table", "event_notice"))
        service._repair_snapshot_requires_workflow_refresh = Mock(return_value=False)
        service._repair_snapshot_from_local = Mock()
        snapshot = {"status": "active", "refreshed_at": 0.0, "app_token": "app",
                    "table_id": "events_table", "fields": [], "records": []}
        store = SimpleNamespace(get_repair_snapshot_meta=lambda _: {"exists": True},
                                get_repair_snapshot=lambda _: snapshot,
                                list_visible_qt_active_items=lambda: [])
        service._load_repair_management_event_records = Mock()
        with self.assertRaises(AssistantError) as error:
            cached_items("events", ["A"], SimpleNamespace(service=service, state_store=store))
        self.assertEqual(error.exception.status, 503)
        service._load_repair_management_event_records.assert_not_called()

    # ---- 重保守卫统计（collect_pending.guard + guard_reply）----
    @staticmethod
    def _response(rid, scope, status, cells):
        return {"response_id": rid, "scope": scope, "sheet_type": "综合检查表", "status": status, "cells": cells}

    @staticmethod
    def _task(task_id, *scopes):
        return {"task_id": task_id, "name": "重保任务" + task_id, "target_scopes": list(scopes), "status": "active"}

    def _guard_runner(self, tasks_by_scope, details, fail_detail=None):
        async def invoke(op):
            api, params, path_params = op["api_id"], op.get("params") or {}, op.get("path_params") or {}
            if api == "GET /api/critical-guard/tasks":
                return {"ok": True, "status": 200, "data": {"items": []}, "_raw": {"tasks": tasks_by_scope.get(params.get("scope"), [])}}
            if api == "GET /api/critical-guard/tasks/{task_id}":
                if fail_detail is not None and (path_params.get("task_id"), params.get("scope")) == fail_detail:
                    return {"ok": False, "status": 503, "error": "读取暂忙"}
                return {"ok": True, "status": 200, "data": {"items": []}, "_raw": {"responses": details.get((path_params.get("task_id"), params.get("scope")), [])}}
            raise AssertionError("Unexpected API " + api)
        return invoke

    async def _guard_collect(self, scopes, tasks_by_scope, details, fail_detail=None):
        actor = {"id": "fixture-a", "scopes": list(scopes), "is_admin": False}
        query = "、".join(code + "楼" for code in scopes) + "重保"
        data = await collect_pending(actor, query, self._guard_runner(tasks_by_scope, details, fail_detail),
                                     self.cached, groups_only={"guard"})
        guard = next(group for group in data["groups"] if group["key"] == "guard")
        return data, guard

    async def test_guard_zero_tasks_reports_zero_not_fake(self):
        data, guard = await self._guard_collect(["A"], {"A": []}, {})
        self.assertTrue(guard["available"])
        stats = guard["stats"]
        self.assertEqual(stats["task_count"], 0)
        self.assertEqual(stats["pending_buildings_count"], 0)
        self.assertEqual(stats["unfilled_buildings_count"], 0)
        self.assertEqual(stats["pending_checklist_count"], 0)
        self.assertIn("当前重保任务 0 个，0 栋楼尚未填写，0 栋楼已填写但未提交。", guard_reply(data))

    async def test_guard_same_task_cross_buildings_unfilled_two_filled_one(self):
        tasks = {"A": [self._task("t1", "A", "B", "C")], "B": [self._task("t1", "A", "B", "C")], "C": [self._task("t1", "A", "B", "C")]}
        details = {
            ("t1", "A"): [self._response("r1a", "A", "pending", {})],
            ("t1", "B"): [self._response("r1b", "B", "pending", {"": ""})],
            ("t1", "C"): [self._response("r1c", "C", "pending", {"checks": {"item": 1}, "check_date": "2026-10-01"})],
        }
        data, guard = await self._guard_collect(["A", "B", "C"], tasks, details)
        stats = guard["stats"]
        self.assertEqual(stats["task_count"], 1)
        self.assertEqual(stats["pending_buildings_count"], 3)
        self.assertEqual(stats["pending_scopes"], ["A", "B", "C"])
        self.assertEqual(stats["unfilled_buildings_count"], 2)
        self.assertEqual(stats["unfilled_scopes"], ["A", "B"])
        self.assertEqual(stats["filled_not_submitted_count"], 1)
        self.assertEqual(stats["pending_checklist_count"], 3)
        self.assertIn("当前重保任务 1 个，2 栋楼尚未填写，1 栋楼已填写但未提交。 另有 3 份检查表未提交。", guard_reply(data))

    async def test_guard_multiple_tasks_same_building_dedup(self):
        tasks = {"A": [self._task("t1", "A"), self._task("t2", "A")]}
        details = {
            ("t1", "A"): [self._response("r1a", "A", "pending", {})],
            ("t2", "A"): [self._response("r2a", "A", "pending", {"checks": {"item": 0}})],
        }
        data, guard = await self._guard_collect(["A"], tasks, details)
        stats = guard["stats"]
        self.assertEqual(stats["task_count"], 2)
        self.assertEqual(stats["pending_buildings_count"], 1)
        self.assertEqual(stats["pending_scopes"], ["A"])
        self.assertEqual(stats["unfilled_buildings_count"], 1)
        self.assertEqual(stats["unfilled_scopes"], ["A"])
        self.assertIn("当前重保任务 2 个，1 栋楼尚未填写，", guard_reply(data))

    async def test_guard_submitted_does_not_count_as_pending(self):
        tasks = {"A": [self._task("t1", "A", "B")], "B": [self._task("t1", "A", "B")]}
        details = {
            ("t1", "A"): [self._response("r1a", "A", "submitted", {"checks": {"item": 1}})],
            ("t1", "B"): [self._response("r1b", "B", "pending", {})],
        }
        data, guard = await self._guard_collect(["A", "B"], tasks, details)
        stats = guard["stats"]
        self.assertEqual(stats["task_count"], 1)
        self.assertEqual(stats["pending_buildings_count"], 1)
        self.assertEqual(stats["pending_scopes"], ["B"])
        self.assertEqual(stats["unfilled_buildings_count"], 1)
        self.assertEqual(stats["pending_checklist_count"], 1)
        self.assertIn("当前重保任务 1 个，1 栋楼尚未填写，0 栋楼已填写但未提交。", guard_reply(data))

    async def test_guard_data_failure_is_unknown_not_zero(self):
        tasks = {"A": [self._task("t1", "A")]}
        data, guard = await self._guard_collect(["A"], tasks, {}, fail_detail=("t1", "A"))
        self.assertFalse(guard["available"])
        self.assertIsNone(guard["stats"])
        self.assertIsNone(guard["count"])
        self.assertIn("暂无法确认", guard_reply(data))
        self.assertNotIn("当前重保任务", guard_reply(data))

    async def test_guard_faulty_detail_shape_is_unknown(self):
        async def invoke(op):
            api, params, path_params = op["api_id"], op.get("params") or {}, op.get("path_params") or {}
            if api == "GET /api/critical-guard/tasks":
                return {"ok": True, "status": 200, "data": {"items": []}, "_raw": {"tasks": [self._task("t1", "A")]}}
            return {"ok": True, "status": 200, "data": {"items": []}, "_raw": {"responses": "not-a-list"}}
        actor = {"id": "fixture-a", "scopes": ["A"], "is_admin": False}
        data = await collect_pending(actor, "A楼重保", invoke, self.cached, groups_only={"guard"})
        guard = next(group for group in data["groups"] if group["key"] == "guard")
        self.assertFalse(guard["available"])
        self.assertIsNone(guard["stats"])

    async def test_guard_permission_isolation_only_acting_scope_visible(self):
        # 任务同时发布到 A、B，但当前账号仅 A 可访问；detail 仅回 A 楼，B 楼不计入。
        tasks = {"A": [self._task("t1", "A", "B")]}
        details = {("t1", "A"): [self._response("r1a", "A", "pending", {"checks": {"item": 1}})]}
        data, guard = await self._guard_collect(["A"], tasks, details)
        stats = guard["stats"]
        self.assertEqual(stats["task_count"], 1)
        self.assertEqual(stats["pending_scopes"], ["A"])
        self.assertEqual(stats["pending_buildings_count"], 1)
        self.assertTrue(all(code in "ABCDE" for code in stats["pending_scopes"]))

    async def test_guard_unsupported_scope_H_is_not_zero(self):
        tasks = {"A": [self._task("t1", "A")]}
        details = {("t1", "A"): [self._response("r1a", "A", "pending", {})]}
        actor = {"id": "fixture-a", "scopes": ["A", "H"], "is_admin": False}
        data = await collect_pending(actor, "A楼和H楼重保",
                                     self._guard_runner(tasks, details), self.cached, groups_only={"guard"})
        guard = next(group for group in data["groups"] if group["key"] == "guard")
        stats = guard["stats"]
        self.assertEqual(stats["task_count"], 1)
        self.assertEqual(stats["pending_buildings_count"], 1)
        self.assertEqual(stats["unsupported_scopes"], ["H"])
        reply = guard_reply(data)
        self.assertIn("1 个，1 栋楼尚未填写", reply)
        self.assertIn("H楼重保暂不支持，未计入，不能作为零条", reply)
        self.assertNotIn("H楼尚未填写", reply)

    # ---- 复核修正：H-only 不适用/未知，明细不完整报未知 ----
    async def test_guard_H_only_is_not_applicable_not_zero(self):
        data = await collect_pending({"id": "h", "scopes": ["H"], "is_admin": False}, "H楼重保",
                                     self._guard_runner({}, {}), self.cached, groups_only={"guard"})
        guard = next(group for group in data["groups"] if group["key"] == "guard")
        self.assertFalse(guard["available"])
        self.assertIsNone(guard["stats"])
        self.assertIsNone(guard["count"])
        reply = guard_reply(data)
        self.assertIn("暂无法确认", reply)
        self.assertIn("H楼尚无重保模块支持", reply)
        self.assertNotIn("当前重保任务 0 个", reply)

    async def test_guard_empty_detail_is_incomplete(self):
        data, guard = await self._guard_collect(["A"], {"A": [self._task("t1", "A")]}, {})
        self.assertFalse(guard["available"])
        self.assertIsNone(guard["stats"])
        reply = guard_reply(data)
        self.assertIn("暂无法确认", reply)
        self.assertNotIn("当前重保任务", reply)

    async def test_guard_foreign_only_detail_is_incomplete(self):
        tasks = {"A": [self._task("t1", "A")]}
        details = {("t1", "A"): [self._response("rb", "B", "pending", {})]}
        data, guard = await self._guard_collect(["A"], tasks, details)
        self.assertFalse(guard["available"])
        self.assertIsNone(guard["stats"])
        self.assertNotIn("当前重保任务", guard_reply(data))

    async def test_guard_missing_response_id_is_incomplete(self):
        tasks = {"A": [self._task("t1", "A")]}
        details = {("t1", "A"): [{"scope": "A", "cells": {}}]}
        data, guard = await self._guard_collect(["A"], tasks, details)
        self.assertFalse(guard["available"])
        self.assertIsNone(guard["stats"])

    async def test_guard_missing_cells_key_is_incomplete(self):
        tasks = {"A": [self._task("t1", "A")]}
        details = {("t1", "A"): [{"response_id": "r1", "scope": "A"}]}
        data, guard = await self._guard_collect(["A"], tasks, details)
        self.assertFalse(guard["available"])
        self.assertIsNone(guard["stats"])

    async def test_guard_cells_not_dict_is_incomplete(self):
        tasks = {"A": [self._task("t1", "A")]}
        details = {("t1", "A"): [{"response_id": "r1", "scope": "A", "cells": "nope"}]}
        data, guard = await self._guard_collect(["A"], tasks, details)
        self.assertFalse(guard["available"])
        self.assertIsNone(guard["stats"])

    def test_guard_reply_requires_group_available_and_stats(self):
        reply = guard_reply({"groups": [{"key": "guard", "available": True, "stats": None}]})
        self.assertIn("暂无法确认", reply)
        self.assertNotIn("当前重保任务", reply)
        reply = guard_reply({"groups": [{"key": "guard", "available": False,
                                         "stats": {"available": True, "task_count": 1, "unfilled_buildings_count": 0, "filled_not_submitted_count": 0}}]})
        self.assertIn("暂无法确认", reply)
        self.assertNotIn("当前重保任务", reply)

    async def test_learning_pending_only_reads_own_building_for_broad_h_account(self):
        actor = {"id": "h", "scopes": ["A", "B", "C", "D", "E", "H"],
                 "learning_scopes": ["H"], "is_admin": False}
        data = await collect_pending(actor, "学练未答题", self.invoke, self.cached, groups_only={'learning'})
        self.assertEqual(data['groups'][0]['count'], 1)
        self.assertEqual(data['groups'][0]['items'][0]['status'], '待答 7 题')
        learning_calls = [call for call in self.calls if "learning" in call["api_id"]]
        self.assertEqual([call['params']['scope'] for call in learning_calls], ['H'])
        self.assertTrue(all(call['api_id'].startswith('GET ') for call in self.calls))

    async def test_learning_unpublished_is_not_zero_unanswered(self):
        async def unpublished(op):
            self.assertEqual(op['api_id'], 'GET /api/learning/papers')
            return {'ok': True, '_raw': {'items': [], 'total': 0}}
        data = await collect_pending({**ACTOR, 'learning_scopes': ['A']}, '学练', unpublished, self.cached, groups_only={'learning'})
        self.assertIn('未发布', pending_reply(data))
        self.assertEqual(data['groups'][0]['stats']['unpublished_scopes'], ['A'])


    async def test_slow_early_module_does_not_block_guard_learning_and_order_is_stable(self):
        notices_blocked = asyncio.Event()
        guard_started, learning_started = asyncio.Event(), asyncio.Event()
        actor = {**ACTOR, "learning_scopes": ["A"]}

        async def invoke(op):
            api = op["api_id"]
            params = op.get("params") or {}
            if api == "GET /api/workbench" and params.get("sections") == "ongoing":
                await notices_blocked.wait()
                return {"ok": True, "_raw": {"source_snapshot_ready": True, "ongoing": [], "ongoing_pagination": {"total": 0}}}
            if api == "GET /api/workbench" and params.get("sections") == "records":
                return {"ok": True, "_raw": {"source_snapshot_ready": True, "records": [], "records_pagination": {"total": 0}}}
            if api == "GET /api/critical-guard/tasks":
                guard_started.set()
                return {"ok": True, "_raw": {"tasks": []}}
            if api == "GET /api/learning/papers":
                learning_started.set()
                return {"ok": True, "_raw": {"items": [], "total": 0}}
            raise AssertionError("Unexpected API " + api)

        task = asyncio.ensure_future(collect_pending(actor, "未完成", invoke, self.cached,
                                                     groups_only={"notices", "guard", "learning"}))
        await asyncio.wait_for(asyncio.gather(guard_started.wait(), learning_started.wait()), timeout=2)
        notices_blocked.set()
        data = await asyncio.wait_for(task, timeout=2)
        keys = [group["key"] for group in data["groups"]]
        self.assertEqual(keys, ["notices", "guard", "learning"])
        guard = next(group for group in data["groups"] if group["key"] == "guard")
        self.assertTrue(guard["available"])
        self.assertEqual(guard["stats"]["task_count"], 0)
        learning = next(group for group in data["groups"] if group["key"] == "learning")
        self.assertTrue(learning["available"])
        self.assertEqual(learning["stats"]["unpublished_scopes"], ["A"])

    async def test_notices_plans_share_ongoing_paging_once(self):
        ongoing_calls = []

        async def invoke(op):
            api, params = op["api_id"], op.get("params") or {}
            if api == "GET /api/workbench":
                if params.get("sections") == "ongoing":
                    page = int(params.get("ongoing_page", 1))
                    ongoing_calls.append(page)
                    rows = [{"record_id": "n%d" % i, "scope": "A", "status": "进行中", "work_type": "maintenance"}
                            for i in range(25)]
                    page_rows = rows[(page - 1) * 20: page * 20]
                    return {"ok": True, "_raw": {"source_snapshot_ready": True, "ongoing": page_rows,
                                                 "ongoing_pagination": {"total": 25, "has_more": page * 20 < 25}}}
                return {"ok": True, "_raw": {"source_snapshot_ready": True, "records": [], "records_pagination": {"total": 0}}}
            raise AssertionError("Unexpected API " + api)

        data = await collect_pending(ACTOR, "未完成", invoke, self.cached, groups_only={"notices", "plans"})
        self.assertEqual(len(ongoing_calls), 2)
        self.assertEqual(sorted(ongoing_calls), [1, 2])
        groups = {group["key"]: group for group in data["groups"]}
        self.assertEqual(groups["notices"]["count"], 25)
        self.assertEqual(groups["plans"]["count"], 0)

    async def test_bounded_timeout_marks_hung_cached_unknown(self):
        import bin.openclaw_service.assistant.lighthouse_pending as lp
        captured = {}

        async def raising_wait_for(awaitable, timeout):
            captured["timeout"] = timeout
            if hasattr(awaitable, "close"):
                awaitable.close()
            raise asyncio.TimeoutError()

        def never_success(kind, scopes):
            raise AssertionError("read_cached must not be reached when the wait is bounded")

        with patch.object(lp.asyncio, "wait_for", raising_wait_for):
            data = await collect_pending(ACTOR, "未完成", self.invoke, never_success, groups_only={"events"})
        self.assertIn("timeout", captured)
        # Per-group loader is capped by the round deadline (min(remaining, 10)).
        self.assertLessEqual(captured["timeout"], 10)
        self.assertGreater(captured["timeout"], 0)
        events = next(group for group in data["groups"] if group["key"] == "events")
        self.assertFalse(events["available"])
        self.assertIsNone(events["count"])
        self.assertIn("读取超时", events["error"])

    async def test_cancellation_of_pending_collect_is_prompt(self):
        never = asyncio.Event()

        async def hang(op):
            await never.wait()
            raise AssertionError("unreachable")

        task = asyncio.ensure_future(collect_pending(ACTOR, "A楼未完成", hang, self.cached, groups_only={"drills"}))
        await asyncio.sleep(0.05)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=2)

    async def test_whole_loader_budget_bounds_early_paginated_groups_and_later_guard_learning_run(self):
        # 用模块常量 patch 成毫秒级预算，短时验证“整组 loader 预算”确实限制了
        # 分页数据集占满并发槽位，且后续的 guard/learning 仍会真实执行（非仅源码字符串断言）。
        import openclaw_service.assistant.lighthouse_pending as lp
        guard_called, learning_called = 0, 0
        notices_pages = 0

        async def invoke(op):
            nonlocal guard_called, learning_called, notices_pages
            api, params = op["api_id"], op.get("params") or {}
            if api == "GET /api/workbench":
                # ongoing 永远分页下去（has_more=True），共享读取自身也有预算，会超时；
                # plans 与 notices 复用同一个 ongoing 读取，因此也被该“整体读取超时”挡住。
                # 用极短 sleep 让事件循环正常调度，避免紧循环把其他分组饿死（真实网络调用同理）。
                notices_pages += 1
                await asyncio.sleep(0.001)
                return {"ok": True, "_raw": {"source_snapshot_ready": True,
                    "ongoing": [{"record_id": "n%d" % notices_pages, "scope": "A", "status": "进行中", "work_type": "maintenance"}],
                    "ongoing_pagination": {"total": 10 ** 9, "has_more": True}}}
            if api == "GET /api/repair-management/records":
                return {"ok": True, "_raw": {"records": [], "total": 0, "has_more": False}}
            if api == "GET /api/cabinet-power/batches":
                return {"ok": True, "_raw": {"items": [], "total": 0}}
            if api == "GET /api/drills":
                return {"ok": True, "_raw": {"items": []}}
            if api == "GET /api/critical-guard/tasks":
                guard_called += 1
                return {"ok": True, "_raw": {"tasks": []}}
            if api == "GET /api/learning/papers":
                learning_called += 1
                return {"ok": True, "_raw": {"items": [], "total": 0}}
            raise AssertionError("Unexpected API " + api)

        actor = {**ACTOR, "learning_scopes": ["A"]}
        with patch.object(lp, "WHOLE_LOADER_BUDGET_SECONDS", 0.2), \
             patch.object(lp, "SHARED_BOARD_BUDGET_SECONDS", 0.2), \
             patch.object(lp, "ROUND_DEADLINE_SECONDS", 30):
            data = await collect_pending(actor, "未完成", invoke, self.cached)
        keys = [group["key"] for group in data["groups"]]
        self.assertEqual(keys, ["notices", "plans", "events", "repairs", "batches",
                                "orders", "mops", "drills", "guard", "learning"])
        self.assertGreaterEqual(notices_pages, 1)
        self.assertEqual(guard_called, 1)
        self.assertEqual(learning_called, 1)
        by = {group["key"]: group for group in data["groups"]}
        # 整组/共享读取超时按 unknown（count=None）处理，绝不当作零条。
        for key in ("notices", "plans"):
            self.assertFalse(by[key]["available"])
            self.assertIsNone(by[key]["count"])
            self.assertIn("整体读取超时", by[key]["error"])
        self.assertTrue(by["guard"]["available"])
        self.assertEqual(by["guard"]["stats"]["task_count"], 0)
        self.assertTrue(by["learning"]["available"])
        self.assertEqual(by["learning"]["stats"]["unpublished_scopes"], ["A"])

    async def test_cancellation_closes_in_flight_shared_board_task(self):
        # 共享通告读取在 collect 取消时不能遗留 detached 请求：必须被显式取消并等待。
        ongoing_started = asyncio.Event()
        ongoing_cancelled = asyncio.Event()

        async def invoke(op):
            api, params = op["api_id"], op.get("params") or {}
            if api == "GET /api/workbench" and params.get("sections") == "ongoing":
                if not ongoing_started.is_set():
                    ongoing_started.set()
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    ongoing_cancelled.set()
                    raise
                return {"ok": True, "_raw": {"source_snapshot_ready": True, "ongoing": [], "ongoing_pagination": {"total": 0}}}
            if api == "GET /api/workbench" and params.get("sections") == "records":
                return {"ok": True, "_raw": {"source_snapshot_ready": True, "records": [], "records_pagination": {"total": 0}}}
            raise AssertionError("Unexpected API " + api)

        task = asyncio.ensure_future(collect_pending(ACTOR, "未完成", invoke, self.cached,
                                                     groups_only={"notices", "plans"}))
        await asyncio.wait_for(ongoing_started.wait(), timeout=1)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=2)
        self.assertTrue(ongoing_cancelled.is_set())

    async def test_timed_out_consumer_retrieves_later_shared_task_failure(self):
        import openclaw_service.assistant.lighthouse_pending as lp

        async def invoke(operation):
            await asyncio.Event().wait()

        async def progress(label):
            if '部分资料暂不可用' in label:
                await asyncio.sleep(0.04)

        loop = asyncio.get_running_loop()
        with patch.object(lp, 'WHOLE_LOADER_BUDGET_SECONDS', 0.01), \
                patch.object(lp, 'SHARED_BOARD_BUDGET_SECONDS', 0.02), \
                patch.object(loop, 'call_exception_handler') as errors:
            data = await collect_pending(ACTOR, '未结束通告', invoke, self.cached,
                groups_only={'notices'}, on_progress=progress)
            await asyncio.sleep(0)
            gc.collect()
            errors.assert_not_called()
        self.assertIsNone(data['groups'][0]['count'])
        self.assertIn('读取超时', data['groups'][0]['error'])



if __name__ == "__main__":
    unittest.main()
