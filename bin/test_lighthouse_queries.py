"""Business intent/count regressions; all records are synthetic and read-only."""
import copy
import datetime as dt
import sys
import unittest
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lan_bitable_template_portal.lighthouse_ai import AssistantError
from lan_bitable_template_portal.lighthouse_model import LighthouseModel
from lan_bitable_template_portal.lighthouse_pending import collect_pending, pending_reply
from lan_bitable_template_portal.lighthouse_queries import (
    EventQuery, PortalOperation, TZ, all_pending_modules, business_domains, collect_events, current_pending_query, date_window, effective_question, event_date, event_reply, query_refinement,
)


class QueryTests(unittest.IsolatedAsyncioTestCase):
    def test_common_date_followups_keep_the_previous_business_subject(self):
        for question in ("9月呢", "近一个月呢", "过去7天", "上星期", "2026-09-01呢", "10月4日呢", "还剩几条？", "还有多少呢"):
            with self.subTest(question=question):
                self.assertTrue(query_refinement(question))
                self.assertIn("事件", effective_question({"question": question, "prompt": "今天发生了几条事件\n" + question}))
        self.assertFalse(query_refinement("你是谁，还有什么能力"))
        self.assertFalse(query_refinement("今天南通天气怎么样"))

    def test_public_search_confirmation_keeps_the_original_subject(self):
        original = '讲一讲deepseek和豆包的优劣点'
        for question in ('需要联网', '联网核对一下', '请联网查询', '上网查一下', '需要联网。'):
            with self.subTest(question=question):
                self.assertTrue(query_refinement(question))
                self.assertIn(original, effective_question({'question': question,
                    'prompt': '之前的问题：' + original + '\n本次补充：' + question}))
        self.assertFalse(query_refinement('联网查询南通天气'))
        self.assertFalse(query_refinement('你能联网吗'))

    def test_common_date_ranges_use_beijing_calendar_without_guessing(self):
        today = dt.date(2026, 10, 4)
        cases = {"9月": (dt.date(2026, 9, 1), dt.date(2026, 9, 30)),
                 "近一个月": (dt.date(2026, 9, 5), today),
                 "过去7天": (dt.date(2026, 9, 28), today),
                 "上星期": (dt.date(2026, 9, 21), dt.date(2026, 9, 27)),
                 "10月4日": (today, today),
                 "去年": (dt.date(2025, 1, 1), dt.date(2025, 12, 31))}
        for question, expected in cases.items():
            with self.subTest(question=question):
                self.assertEqual(date_window(question, today), expected)
        with self.assertRaises(AssistantError):
            date_window("13月", today)

    def test_read_and_write_operation_envelopes_share_typed_sections(self):
        operation = PortalOperation.model_validate({"api_id": "POST /api/workbench-actions", "body": {
            "command_format": "notice_command", "patch": {"progress": "已完成60%"}}})
        self.assertEqual(operation.params, {})
        self.assertEqual(operation.model_dump()["body"]["patch"]["progress"], "已完成60%")
        for bad in ({"api_id": "POST /api/workbench-actions", "progress": "错放顶层"},
                    {"api_id": "POST /api/workbench-actions", "body": "invalid"}):
            with self.assertRaises(ValueError):
                PortalOperation.model_validate(bad)

    def setUp(self):
        self.actor = {"id": "fixture", "scopes": ["E"]}
        self.calls = []
        self.rows = [
            {"record_id": "today-open", "scope": "E", "title": "测试事件一", "occurrence_time": "2026/10/01 09:00", "status": "处理中"},
            {"record_id": "today-closed", "scope": "E", "title": "测试事件二", "occurrence_time": "2026-10-01T10:00:00", "end_time": "2026-10-01 12:00", "status": "已结束"},
            {"record_id": "older", "scope": "E", "title": "昨日事件今天更新", "occurrence_time": "2026/09/30 09:00", "progress_update": "2026-10-01 12:00", "status": "处理中"},
            {"record_id": "private", "scope": "A", "title": "不可访问", "occurrence_time": "2026-10-01 09:00", "status": "处理中"},
        ]
        self.query = EventQuery(start_date="2026-10-01", end_date="2026-10-01")

    async def invoke(self, operation):
        self.calls.append(copy.deepcopy(operation))
        self.assertEqual(operation["api_id"], "GET /api/events/monthly")
        return {"ok": True, "_raw": {"scope": "E", "snapshot_exists": True, "records": copy.deepcopy(self.rows),
            "last_refreshed_at": dt.datetime(2026, 10, 1, 12, tzinfo=TZ).timestamp()}}

    async def test_today_includes_closed_excludes_old_updated_other_scope_and_deduplicates(self):
        self.rows.append(copy.deepcopy(self.rows[0]))
        data = await collect_events(self.actor, self.query, self.invoke)
        self.assertEqual(data["count"], 2)
        self.assertEqual(data["known_count"], 2)
        self.assertEqual(self.calls[0]["params"], {"scope": "E", "month": "2026-10"})
        reply = event_reply(data)
        self.assertIn("2 条事件通告", reply)
        self.assertNotIn("测试事件一", reply)
        self.assertNotIn("维修项目", reply)
        self.assertIn("测试事件一", event_reply(data, details=True))

    async def test_cross_month_dedup_and_status_filters(self):
        data = await collect_events(self.actor, EventQuery(start_date="2026-09-30", end_date="2026-10-01"), self.invoke)
        self.assertEqual(data["count"], 3)
        self.assertEqual([op["params"]["month"] for op in self.calls], ["2026-09", "2026-10"])
        data = await collect_events(self.actor, self.query.model_copy(update={"status": "open"}), self.invoke)
        self.assertEqual(data["count"], 1)

    async def test_completion_date_is_not_occurrence_date(self):
        self.rows[2]["end_time"] = "2026-10-01 18:00"
        self.rows[2]["status"] = "已结束"
        query = self.query.model_copy(update={"date_field": "end_time"})
        data = await collect_events(self.actor, query, self.invoke)
        self.assertEqual(data["count"], 2)
        self.assertIn("按事件结束时间统计", event_reply(data))

    async def test_user_benchmark_five_events_two_processing(self):
        self.rows = [{"record_id": f"e{i}", "scope": "E", "title": f"隔离事件{i}", "occurrence_time": "2026-10-01 10:00",
                      "status": "处理中" if i < 2 else "已结束", "end_time": "" if i < 2 else "2026-10-01 12:00"} for i in range(5)]
        data = await collect_events(self.actor, self.query, self.invoke)
        self.assertEqual(data["stats"]["total"], 5)
        self.assertEqual(data["stats"]["processing"], 2)
        self.assertEqual(data["stats"]["ended"], 3)
        reply = event_reply(data, status_details=True)
        self.assertIn("5 条事件通告", reply)
        self.assertIn("处理中 **2 条**", reply)

    async def test_latest_snapshot_selected_before_open_state_filter(self):
        async def changed(operation):
            newer = operation["params"]["month"] == "2026-10"
            row = {"record_id": "same", "scope": "E", "title": "测试事件", "occurrence_time": "2026-09-30 09:00",
                "status": "已结束" if newer else "处理中", "end_time": "2026-10-01 10:00" if newer else ""}
            return {"ok": True, "_raw": {"snapshot_exists": True, "records": [row], "last_refreshed_at": 20 if newer else 10}}
        query = EventQuery(start_date="2026-09-30", end_date="2026-10-01", status="open")
        data = await collect_events(self.actor, query, changed)
        self.assertEqual(data["count"], 0)

    async def test_no_permissions_never_reads_and_model_schema_rejects_unknown_filters(self):
        with self.assertRaises(AssistantError):
            await collect_events({"scopes": []}, self.query, self.invoke)
        self.assertEqual(self.calls, [])
        with self.assertRaises(ValueError):
            EventQuery(start_date="2026-10-01", end_date="2026-10-01", arbitrary_sql="SELECT 1")

    async def test_missing_dates_uninitialized_and_failed_reads_are_not_zero(self):
        self.rows[0]["occurrence_time"] = ""
        data = await collect_events(self.actor, self.query, self.invoke)
        self.assertIsNone(data["count"])
        self.assertEqual(data["known_count"], 1)
        self.assertIn("完整数量暂无法确认", event_reply(data))
        for result in ({"ok": False}, {"ok": True, "data": {"snapshot_exists": False, "records": []}}, {"ok": True, "truncated": True, "data": {"snapshot_exists": True, "records": []}}):
            async def broken(_):
                return result
            data = await collect_events(self.actor, self.query, broken)
            self.assertIsNone(data["count"])

    def test_business_vocabulary_and_latest_explicit_subject(self):
        for question, domain in (
            ("今天E楼发生了几条事件", "events"), ("有多少事件通告", "events"), ("未发检修", "repair_notices"),
            ("未结束的检修通告", "repair_notices"), ("维修单", "repairs"), ("维修跟进记录", "followups"),
            ("未完成维保通告", "notices"), ("未完成维护单", "mops"), ("SOP工单", "orders"),
            ("机柜测试电", "batches"), ("水耗", "water"), ("演练", "drills"), ("重保", "guard"),
            ("学练题单", "learning"), ("题库面试题", "question_bank"), ("工作报告", "daily"), ("收敛核对台", "convergence"), ("工号", "people"),
        ):
            with self.subTest(question=question):
                self.assertEqual(business_domains(question), {domain})
        self.assertEqual(effective_question({"question": "今天E楼发生了几条事件", "prompt": "未完成任务\n今天E楼发生了几条事件"}), "今天E楼发生了几条事件")
        self.assertEqual(effective_question({"question": "只看E楼", "prompt": "今天发生了几条事件\n只看E楼"}), "今天发生了几条事件\n只看E楼")

    def test_beijing_dates_and_invalid_range(self):
        today = dt.date(2026, 10, 1)
        self.assertEqual(date_window("今天不是，要看昨天", today), (dt.date(2026, 9, 30),) * 2)
        self.assertEqual(date_window("2026-09-30至2026-10-01", today), (dt.date(2026, 9, 30), today))
        self.assertEqual(date_window("上月", today), (dt.date(2026, 9, 1), dt.date(2026, 9, 30)))
        self.assertEqual(event_date("2026-09-30T17:00:00Z"), today)
        self.assertIsNone(event_date("2026-99-99"))
        with self.assertRaises(AssistantError):
            date_window("2026年2月30日", today)
        with self.assertRaises(ValueError):
            EventQuery(start_date="2026-10-02", end_date="2026-10-01")

    def test_today_backlog_is_current_but_occurrence_and_historical_cutoff_are_not(self):
        today = dt.date(2026, 10, 1)
        for question in ("今天未结束的工作有哪些", "今日未完成的任务", "现在还有几个未答题题单", "当前还有哪些待办"):
            self.assertTrue(current_pending_query(question, today), question)
        for question in ("今天发生了几条事件", "今天新增但未结束的通告", "今天发布的未答题题单", "昨天未结束的工作"):
            self.assertFalse(current_pending_query(question, today), question)
        self.assertTrue(all_pending_modules("所有功能中未完成、未结束、未答题的"))
        self.assertFalse(all_pending_modules("所有未完成的维修任务"))

    async def test_today_all_pending_bypasses_model_and_preserves_unknown_sources(self):
        @asynccontextmanager
        async def forbidden(*args):
            self.fail("Today's backlog must not depend on the model selecting tools")
            yield
        portal = SimpleNamespace(assistant=SimpleNamespace(model=Mock()))
        engine = LighthouseModel(portal, cached_reader=Mock(), model_factory=forbidden)
        async def emit(*args): pass
        async def auth(): return self.actor
        base = {"available": True, "count": 1, "error": "", "warnings": [], "items": [], "remaining": 0, "url": "/learning"}
        data = {"scopes": ["E"], "queried_at": "2026-10-01T15:00:00+08:00", "groups": [
            {**base, "key": "learning", "label": "学练题单"},
            {**base, "key": "drills", "label": "演练任务", "count": 0},
            {**base, "key": "events", "label": "未闭环事件", "available": False, "count": None, "error": "读取暂忙"}]}
        for question in ("今天未结束的工作有哪些", "所有功能中未完成的未结束的未答题的有哪些"):
            with patch("lan_bitable_template_portal.lighthouse_pending.collect_pending", AsyncMock(return_value=copy.deepcopy(data))) as reader:
                result = await engine.answer(self.actor, {"question": question, "_profile": {}}, [], None, emit, auth, {})
                self.assertIsNone(reader.call_args.kwargs["groups_only"])
                self.assertIn("学练题单", result["answer"])
                self.assertIn("读取暂忙", result["answer"])
                self.assertNotIn("演练任务", result["answer"])
                self.assertNotIn("请选择", result["answer"])

    async def test_change_plan_and_active_counts_are_separate_even_when_active_is_zero(self):
        @asynccontextmanager
        async def forbidden(*args):
            self.fail("Explicit current notice counters should use native data")
            yield
        portal = SimpleNamespace(assistant=SimpleNamespace(model=Mock()))
        engine = LighthouseModel(portal, cached_reader=Mock(), model_factory=forbidden)
        async def emit(*args): pass
        async def auth(): return self.actor
        base = {"available": True, "error": "", "warnings": [], "items": [], "remaining": 0, "url": "/workbench-lite"}
        data = {"scopes": ["E"], "queried_at": "2026-10-01", "groups": [
            {**base, "key": "plans", "label": "未发计划通告", "count": 2}, {**base, "key": "notices", "label": "未结束通告", "count": 0}]}
        with patch("lan_bitable_template_portal.lighthouse_pending.collect_pending", AsyncMock(return_value=data)) as reader:
            result = await engine.answer(self.actor, {"question": "变更待发起和进行中各多少", "_profile": {}}, [], None, emit, auth, {})
            self.assertEqual(reader.call_args.kwargs["groups_only"], {"plans", "notices"})
            self.assertEqual(reader.call_args.kwargs["notice_type"], "change")
            self.assertIn("| 2 |", result["answer"])
            self.assertIn("| 0 |", result["answer"])

    async def test_zero_groups_hidden_but_unknown_retained_and_specific_domain_not_broadened(self):
        async def forbidden(_):
            self.fail("Specific event query must not fetch other modules")
        def cached(kind, scopes):
            self.assertEqual(kind, "events")
            return [], []
        data = await collect_pending(self.actor, "未完成工作", forbidden, cached, groups_only={"events"})
        self.assertEqual([group["key"] for group in data["groups"]], ["events"])
        zero = data["groups"][0]
        data["groups"] += [{**zero, "key": "repairs", "label": "未完成维修项目", "count": 3},
                           {**zero, "key": "drills", "label": "演练任务", "count": None, "available": False, "error": "读取超时"}]
        reply = pending_reply(data, details=False)
        self.assertNotIn("未闭环事件", reply)
        self.assertIn("未完成维修项目", reply)
        self.assertIn("演练任务", reply)
        self.assertIn("读取超时", reply)
        self.assertIn("未闭环事件", pending_reply(data, include_zero=True))

    async def test_guard_zero_tasks_and_unfilled_buildings_use_native_state_without_model(self):
        @asynccontextmanager
        async def forbidden(*args):
            self.fail("Guard counts must use native task/response identities")
            yield
        portal = SimpleNamespace(assistant=SimpleNamespace(model=Mock()),
            catalog=SimpleNamespace(get=lambda _: {"read_only": True, "name": "重保任务", "schema": {}}))
        async def invoke(actor, operation, request):
            self.assertEqual(operation["api_id"], "GET /api/critical-guard/tasks")
            self.assertEqual(operation["params"]["scope"], "E")
            return {"ok": True, "_raw": {"tasks": [], "count": 0}}
        portal._invoke = invoke
        engine = LighthouseModel(portal, model_factory=forbidden)
        async def emit(*args): pass
        async def auth(): return self.actor
        result = await engine.answer(self.actor, {"question": "当前E楼重保有几个任务，多少楼未填写", "_profile": {}}, [], None, emit, auth, {})
        self.assertIn("重保任务 0 个", result["answer"])
        self.assertIn("0 栋楼尚未填写", result["answer"])

    async def test_event_question_bypasses_model_and_previous_work_summary(self):
        # A wrong model/tool choice must never turn an explicit event count into repair totals.
        @asynccontextmanager
        async def forbidden(*args):
            self.fail("Unambiguous count must use the native event snapshot")
            yield
        portal = SimpleNamespace(assistant=SimpleNamespace(model=Mock()),
            catalog=SimpleNamespace(get=lambda _: {"read_only": True, "name": "事件月快照", "schema": {}}))
        async def invoke(actor, operation, request):
            result = await self.invoke(operation)
            result["_raw"]["records"] = [row for row in result["_raw"]["records"] if row["scope"] == "E"]
            return result
        portal._invoke = invoke
        engine = LighthouseModel(portal, model_factory=forbidden)
        async def emit(*args): pass
        async def auth(): return self.actor
        turn = {"question": "2026-10-01E楼发生了几条事件", "prompt": "之前查询未完成任务，用户补充：2026-10-01E楼发生了几条事件", "_profile": {}}
        result = await engine.answer(self.actor, turn, [], None, emit, auth, {})
        self.assertIn("2 条事件通告", result["answer"])
        self.assertEqual(len(self.calls), 1)
        turn["question"] = "有几条事件"
        result = await engine.answer(self.actor, turn, [], None, emit, auth, {})
        self.assertIn("哪个时间范围", result["answer"])
        self.assertEqual(len(self.calls), 1)


if __name__ == "__main__":
    unittest.main()
