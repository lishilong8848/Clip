"""QUERY conversational-intent regressions for the Lighthouse assistant.

Scope: question *intent* only. This module intentionally does not touch
production code, credentials, real runtime/business data, cloud/network, the
frontend, or raw tables. It uses the real intent/date helpers and builds
synthetic FunctionModel fixtures for the paths that legitimately reach the
model. Deterministic paths (events, pending, guard, learning-only, notice
sends) are tested with a *forbidden* model factory so a regression that leaks
into the model is caught.
"""
import copy
import sys
import unittest
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from fastapi import FastAPI, Request  # noqa: E402
from pydantic_ai.models.function import FunctionModel  # noqa: E402

from lan_bitable_template_portal.lighthouse_ai import AssistantError, MODEL_QUESTION  # noqa: E402
from lan_bitable_template_portal.lighthouse_ai import LighthouseAssistant, NAMESPACE  # noqa: E402
from lan_bitable_template_portal.lighthouse_agent import PortalAgent  # noqa: E402
from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog  # noqa: E402
from lan_bitable_template_portal.lighthouse_files import LighthouseFiles  # noqa: E402
from lan_bitable_template_portal.lighthouse_model import LighthouseModel  # noqa: E402
from lan_bitable_template_portal.lighthouse_pending import collect_pending, pending_reply  # noqa: E402
from lan_bitable_template_portal.lighthouse_queries import (  # noqa: E402
    TZ, all_pending_modules, business_domains, collect_notice_sends,
    current_pending_query, date_window, effective_question, notice_sends_reply,
    query_result_state, read_only_question, sent_notice_question,
)
from lan_bitable_template_portal.lighthouse_stream import LighthouseStream  # noqa: E402


class _EngineHarness:
    """A tiny read-only LighthouseModel harness; no network or real service."""

    def __init__(self, routes, *, actor=None, factory=None, cached_reader=None, profile=None):
        self.actor = actor or {"id": "fixture", "scopes": ["D"], "is_admin": False, "learning_scopes": ["D"]}
        self.calls = []
        self.routes = routes or {}
        self.events = []

        async def _invoke(current, operation, request, **kwargs):
            self.calls.append(operation)
            api = operation.get("api_id")
            if api not in self.routes:
                raise AssertionError("unexpected api " + api)
            return {"ok": True, "_raw": copy.deepcopy(self.routes[api])}

        portal = SimpleNamespace(
            assistant=SimpleNamespace(model=Mock()),
            catalog=SimpleNamespace(get=lambda api: {"read_only": True, "name": "查询" + api, "schema": {},
                                                     "scope_mode": "", "scope_values": []}),
            files=SimpleNamespace(get=self._no_file, context=lambda *_: [], image_parts=lambda *_: []),
            _public_references=lambda result, _: result,
            _source_url=lambda *_: "/workbench-lite",
            _invoke=_invoke,
        )
        self.profile = profile or {"name": "独立测试模型", "model": "fixture/auto"}
        self.engine = LighthouseModel(portal, cached_reader=cached_reader, model_factory=factory)

    @staticmethod
    def _no_file(actor, identity):
        raise AssistantError("no-file")

    async def ask(self, question, history=None):
        async def auth():
            return self.actor

        async def emit(kind, data):
            self.events.append((kind, data))

        turn = {"question": question, "_profile": self.profile}
        result = await self.engine.answer(self.actor, turn, history or [], None, emit, auth, {})
        return result, self.calls, self.events


def _forbidden_factory(testcase):
    @asynccontextmanager
    async def factory(*_):
        testcase.fail("model must not be called for this deterministic query")
        yield

    return factory


def _text_factory(text):
    @asynccontextmanager
    async def factory(*_):
        async def stream(messages, info):
            yield text

        yield FunctionModel(stream_function=stream)

    return factory


class _SyntheticStore:
    """Minimal in-memory document store used by the real LighthouseStream fixture."""

    def __init__(self, path=None):
        self.db_path, self.docs = path or "memory://state.sqlite3", {}

    def get_document(self, namespace, key):
        return copy.deepcopy(self.docs.get((namespace, key)))

    def put_document(self, namespace, key, value):
        self.docs[namespace, key] = copy.deepcopy(value)

    def list_documents(self, namespace, *, key_prefix=""):
        return [{"key": key, "payload": copy.deepcopy(value)}
                for (space, key), value in self.docs.items()
                if space == namespace and key.startswith(key_prefix)]


class _DummyStreamEngine:
    max_parallel = 1


class _StreamFixture:
    """A real LighthouseStream over a synthetic Store/model/PortalAgent fixture.

    No real cloud/model, credentials, frontend or network are touched. `_accept`
    is driven synchronously so the resulting turn (including its `prompt`) can be
    inspected with the production `effective_question` helper.
    """

    def __init__(self, actor):
        self.actor = dict(actor)
        self.store = _SyntheticStore()
        self.model = Mock()
        self.model.settings.return_value = {
            "configured": True, "enabled": True, "active_model_id": "test",
            "models": [{"id": "test", "name": "测试", "model": "fixture", "configured": True}],
        }
        self.model.profile.return_value = {"id": "test", "name": "测试", "model": "fixture"}
        self.assistant = LighthouseAssistant(self.store, Mock(return_value=([], [])), model=self.model)
        self.app = FastAPI()
        self.portal = PortalAgent(self.assistant, PortalAPICatalog(self.app), LighthouseFiles(self.store))
        self.runtime = LighthouseStream(self.portal, engine=_DummyStreamEngine())

    def seed_turn(self, turn, *, active_run_id=""):
        state = {"id": "conversation-fixture", "turns": [copy.deepcopy(turn)], "active_run_id": active_run_id}
        self.store.put_document(NAMESPACE, self.assistant._key(self.actor), state)

    def accept(self, question, operation_id):
        return self.runtime._accept(self.actor, {
            "question": question,
            "file_ids": [],
            "operation_id": operation_id,
            "conversation_id": self.assistant._state(self.actor)["id"],
        })


class QueryIntentTests(unittest.TestCase):
    def setUp(self):
        self.today = datetime.now(TZ).date()

    # ---------- domain classification (10 real variants) ----------
    def test_business_domains_for_named_modules_and_plain_backlog(self):
        expect = {
            "水耗本月有没有异常": {"water"},
            "本周把机房机柜的事登记一下": {"batches"},
            "演练任务安排": {"drills"},
            "A楼重保现在什么情况": {"guard"},
            "学练未答题有多少": {"learning"},
            "计划收敛核对台": {"convergence"},
            "当前还有什么活没做完": set(),
            "维修还没好有几项": {"repairs"},
            "本周检修只看D楼": {"repair_notices"},
            "今天一共发了几次通告": {"notices"},
        }
        for text, expected in expect.items():
            with self.subTest(text=text):
                self.assertEqual(business_domains(text), expected)

    # ---------- current backlog vs new records / specific windows ----------
    def test_current_pending_distinguishes_backlog_from_new_or_windowed(self):
        cases = {
            # (text, expected)  expected follows business rule: today's outstanding work.
            "当前还有什么活没做完": True,
            "还有几个任务没做完": True,
            "今天发生了几条事件": False,      # today's new event notices, not outstanding work
            "本周检修只看D楼": False,          # weekly window, not today's backlog
            "昨天未结束的工作": False,          # yesterday's status, not today's backlog
            # Intended business rule: "还没好" is unfinished repair => current backlog.
            "维修还没好有几项": True,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                self.assertEqual(current_pending_query(text), expected)

    def test_all_pending_modules_only_matches_explicit_all(self):
        self.assertTrue(all_pending_modules("把所有的功能未完成的工作查一下"))
        self.assertTrue(all_pending_modules("所有未完成的工作有哪些"))
        self.assertFalse(all_pending_modules("当前还有什么活没做完"))

    # ---------- date windows ----------
    def test_date_windows_realistic_variants(self):
        today = self.today
        # "最近一周" is a realistic Chinese variant meaning the last 7 days inclusive.
        expected_week = (today - timedelta(days=6), today)
        got = date_window("最近一周未结束的工作")
        with self.subTest(case="最近一周_intended_seven_days"):
            self.assertEqual(got, expected_week)
        # "本周" is recognised and rolls back to the current week's Monday.
        given = date_window("本周检修只看D楼")
        self.assertIsNotNone(given)
        self.assertEqual(given[1], today)
        self.assertLessEqual(given[0], today)
        self.assertEqual(given[0], today - timedelta(days=today.weekday()))
        # "今天一共发了几次通告" narrows to today.
        self.assertEqual(date_window("今天一共发了几次通告"), (today, today))

    def test_effective_question_supplement_retains_prior_subject_date_status(self):
        turn = {"question": "只看D楼",
                "prompt": "本周检修仍未结束的通告\n只看D楼"}
        # Scope-only supplement has no business domain => prior prompt (subject) is kept.
        self.assertEqual(effective_question(turn), turn["prompt"])

        turn = {"question": "只要E楼",
                "prompt": "2026-10-01 事件（未闭环）\n只要E楼"}
        self.assertEqual(effective_question(turn), turn["prompt"])

    def test_effective_question_new_explicit_subject_replaces_old_topic(self):
        turn = {"question": "今天发生了几条事件",
                "prompt": "当前还有什么活没做完\n今天发生了几条事件"}
        self.assertEqual(effective_question(turn), "今天发生了几条事件")

        turn = {"question": "请查D楼维修还没好有几项",
                "prompt": "本周检修只看D楼\n请查D楼维修还没好有几项"}
        # "维修" / "检修" are explicit business domains => new subject wins over old topic.
        self.assertEqual(effective_question(turn), "请查D楼维修还没好有几项")

    def test_effective_question_same_object_week_retained_but_new_events_replace(self):
        # Same-object refinement: "只看D楼的检修" only narrows the scope of the prior
        # "本周未开始检修有多少", so the week and 未开始 constraints must be kept.
        turn = {"question": "只看D楼的检修",
                "prompt": "本周未开始检修有多少\n只看D楼的检修"}
        self.assertEqual(effective_question(turn), turn["prompt"])

        # A full new question about today's events is an explicit new subject and must
        # discard the old repair-topic. This guard stays in place too.
        turn = {"question": "今天发生了几条事件",
                "prompt": "本周未开始检修有多少\n今天发生了几条事件"}
        self.assertEqual(effective_question(turn), "今天发生了几条事件")

    def test_how_to_vs_current_data_read_only(self):
        self.assertFalse(read_only_question("导入导出功能怎么用"))
        self.assertFalse(read_only_question("为什么维修单还没结束"))
        self.assertFalse(read_only_question("请帮我把通告发送一下"))
        # current-data questions stay read-only.
        self.assertTrue(read_only_question("今天维保发了多少条"))
        self.assertTrue(read_only_question("学练未答题有多少"))

    def test_sent_notice_question_finds_count_phrasings(self):
        # Already-supported phrasing.
        self.assertTrue(sent_notice_question("今天发了多少条通告"))
        self.assertTrue(sent_notice_question("一共发出过几份检修通告"))
        # HOW-TO / write instructions are never sent-notice counts.
        self.assertFalse(sent_notice_question("怎么发送通告"))
        self.assertFalse(sent_notice_question("请帮我发一条通告"))
        # "发了几次通告" carries the "发了" verb, so the count phrasing is recognised.
        self.assertTrue(sent_notice_question("今天一共发了几次通告"))

    def test_query_result_failure_is_not_zero(self):
        state, guidance = query_result_state({"ok": False, "error": "暂忙", "data": {"records": []}})
        self.assertEqual(state, "unavailable")
        self.assertIn("不代表没有记录", guidance)
        self.assertNotIn("0 条", guidance)


class QueryIntentAsyncTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.today = datetime.now(TZ).date()

    async def test_notice_distinct_vs_send_counts_are_separate(self):
        calls = []
        routes = {
            "GET /api/history-summary": {
                "days": [{
                    "date": self.today.isoformat(),
                    "items": [
                        {"title": "A楼维保通告", "work_type": "maintenance", "record_id": "m1", "scope": "E",
                         "actions": [
                             {"action": "start", "time": f"{self.today} 09:00:00", "text": "开始"},
                             {"action": "update", "time": f"{self.today} 10:00:00", "text": "进展"},
                         ]},
                        {"title": "B楼变更通告", "work_type": "change", "record_id": "c1", "scope": "E",
                         "actions": [
                             {"action": "start", "time": f"{self.today} 11:00:00", "text": "开始"},
                             {"action": "end", "time": f"{self.today} 12:00:00", "text": "结束"},
                         ]},
                    ],
                }],
            },
        }

        async def invoke(op):
            calls.append(op["params"])
            return {"ok": True, "_raw": routes["GET /api/history-summary"]}

        actor = {"id": "fixture", "scopes": ["E"], "is_admin": False}
        data = await collect_notice_sends(actor, self.today, self.today, invoke)
        self.assertTrue(data["complete"])
        self.assertEqual(data["count"], 2)        # distinct notices
        self.assertEqual(data["send_count"], 4)   # start/update/start/end actions
        self.assertEqual(data["actions"], {"start": 2, "update": 1, "end": 1})
        self.assertEqual(data["by_type"]["维保"], 1)
        self.assertEqual(data["by_type"]["变更"], 1)
        self.assertEqual(data["by_type"]["检修"], 0)  # zero types are explicit, not fabricated
        reply = notice_sends_reply(data, details=True)
        self.assertIn("已发送 **2 条通告，共 4 次**", reply)
        self.assertIn("开始 2 次，更新 1 次，结束 1 次。", reply)
        self.assertEqual(len(calls), 1)

    async def test_engine_events_today_despite_pending_modules(self):
        routes = {
            "GET /api/events/monthly": {
                "snapshot_exists": True,
                "scope": "E",
                "records": [
                    {"record_id": "ev1", "title": "A楼冷水机组故障", "scope": "E",
                     "occurrence_time": f"{self.today} 09:00:00", "status": "处理中"},
                    {"record_id": "ev2", "title": "B楼电井冒烟", "scope": "E",
                     "occurrence_time": f"{self.today} 11:00:00", "end_time": f"{self.today} 13:00:00", "status": "已结束"},
                ],
            },
        }
        harness = _EngineHarness(routes, actor={"id": "fixture", "scopes": ["E"], "is_admin": False},
                                 factory=_forbidden_factory(self))
        result, calls, _ = await harness.ask(f"{self.today.isoformat()}E楼发生了几条事件")
        self.assertIn("2 条事件通告", result["answer"])
        self.assertEqual([c["api_id"] for c in calls], ["GET /api/events/monthly"])
        self.assertFalse([c for c in calls if "pending" in c["api_id"]])

    async def test_engine_current_backlog_does_not_become_events(self):
        synthetic = {
            "scopes": ["D"], "queried_at": "2026-10-03T10:00:00+08:00",
            "groups": [{"key": "events", "label": "未闭环事件", "count": 2, "known_count": 2,
                        "available": True, "error": "", "warnings": [],
                        "items": [{"title": "A楼冷水机组故障", "scopes": ["D"], "status": "处理中", "type": "事件"}],
                        "remaining": 0, "url": "/event-management?scope=D"}],
        }
        with patch("lan_bitable_template_portal.lighthouse_pending.collect_pending",
                   new=AsyncMock(return_value=synthetic)) as reader:
            harness = _EngineHarness({}, factory=_forbidden_factory(self),
                                     cached_reader=lambda *_: ([], []))
            result, calls, _ = await harness.ask("当前还有什么活没做完")
        reader.assert_awaited_once()
        self.assertIn("## 未闭环事件", result["answer"])
        self.assertIn("未闭环事件", result["answer"])
        self.assertEqual(calls, [])  # never fetches month/API data

    async def test_engine_repair_plans_vs_ongoing_notices_vs_unfinished(self):
        rep = {
            "kind": "repair", "scopes": ["D"], "queried_at": "2026-10-03T10:00:00+08:00",
            "groups": [
                {"key": "planned_repairs", "label": "待开始检修计划", "count": 3, "known_count": 3,
                 "available": True, "error": "", "warnings": [], "items": [], "remaining": 0,
                 "url": "/repair-management?scope=D"},
                {"key": "repair_notices", "label": "未结束检修通告", "count": 2, "known_count": 2,
                 "available": True, "error": "", "warnings": [], "items": [], "remaining": 0,
                 "url": "/workbench-lite?work_type=repair&scope=D"},
                {"key": "repairs", "label": "未完成维修项目", "count": 4, "known_count": 4,
                 "available": True, "error": "", "warnings": [], "items": [], "remaining": 0,
                 "url": "/repair-management?scope=D"},
            ],
        }
        with patch("lan_bitable_template_portal.lighthouse_pending.collect_repair_overview",
                   new=AsyncMock(return_value=rep)) as overview:
            harness = _EngineHarness({}, factory=_forbidden_factory(self))
            result, _, _ = await harness.ask("未发检修计划有几条")
        overview.assert_awaited_once()
        # Only planned repairs are kept; notices and unfinished projects are not mixed in.
        self.assertIn("待开始检修计划", result["answer"])
        self.assertIn("待开始检修计划 3 条", result["answer"])
        self.assertNotIn("未结束检修通告", result["answer"])
        self.assertNotIn("未完成维修项目", result["answer"])

    async def test_engine_learning_read_query_is_deterministic(self):
        routes = {
            "GET /api/learning/papers": {
                "items": [{"id": "p1", "scope": "D", "date": self.today.isoformat(), "status": "pending",
                           "stats": {"total": 10, "answered": 3}}],
                "total": 1,
            },
        }
        harness = _EngineHarness(routes, actor={"id": "fixture", "scopes": ["D"], "is_admin": False,
                                                "learning_scopes": ["D"]},
                                 factory=_forbidden_factory(self))
        result, calls, _ = await harness.ask("学练未答题明细")
        self.assertIn("今日学练", result["answer"])
        self.assertIn("待答 7 题", result["answer"])
        self.assertEqual([c["api_id"] for c in calls], ["GET /api/learning/papers"])

    async def test_engine_learning_write_is_blocked_not_executed(self):
        harness = _EngineHarness({}, actor={"id": "fixture", "scopes": ["D"], "is_admin": False,
                                            "learning_scopes": ["D"]},
                                 factory=_forbidden_factory(self))
        result, calls, _ = await harness.ask("请填写学练题单")
        self.assertIn("仅查询", result["answer"])
        self.assertIn("/learning", result["answer"])
        self.assertEqual(calls, [])
        self.assertNotIn("已提交", result["answer"])

    async def test_engine_guard_unsupported_scope_is_unknown_not_zero(self):
        harness = _EngineHarness({}, actor={"id": "fixture", "scopes": ["H"], "is_admin": False},
                                 factory=_forbidden_factory(self))
        result, calls, _ = await harness.ask("H楼重保现在什么情况")
        self.assertIn("暂无法确认", result["answer"])
        self.assertIn("尚无重保模块支持", result["answer"])
        self.assertNotIn("当前重保任务 0", result["answer"])
        self.assertEqual(calls, [])

    async def test_engine_partial_repair_failure_is_unknown_not_zero(self):
        synthetic = {
            "scopes": ["D"], "queried_at": "2026-10-03T10:00:00+08:00",
            "groups": [{"key": "repairs", "label": "未完成维修项目", "count": None, "known_count": None,
                        "available": False, "error": "读取暂忙", "warnings": [], "items": [],
                        "remaining": 0, "url": "/repair-management?scope=D"}],
        }
        with patch("lan_bitable_template_portal.lighthouse_pending.collect_pending",
                   new=AsyncMock(return_value=synthetic)):
            harness = _EngineHarness({}, factory=_forbidden_factory(self),
                                     cached_reader=lambda *_: ([], []))
            result, _, _ = await harness.ask("当前还有什么活没做完")
        self.assertIn("待确认", result["answer"])
        self.assertIn("未完成维修项目", result["answer"])  # surfaced as unknown, with 待确认
        self.assertNotIn("未完成维修项目为 **0 项**", result["answer"])  # never a fabricated zero

    async def test_model_question_after_business_uses_model_profile(self):
        self.assertTrue(MODEL_QUESTION.fullmatch("你用的什么模型"))
        harness = _EngineHarness({},
                                 factory=_text_factory("我是灯塔助手，本轮配置为fixture/auto"))
        result, calls, _ = await harness.ask("你用的什么模型")
        self.assertIn("fixture/auto", result["answer"])
        self.assertEqual(calls, [])

    async def test_generic_chat_after_business_topic_does_not_fetch_business(self):
        history = [{"question": "当前还有什么活没做完", "answer": "未完成工作 0 项。", "scopes": ["D"]}]
        harness = _EngineHarness({}, factory=_text_factory("这是日常问候，不涉及业务。"))
        result, calls, _ = await harness.ask("帮我翻译一句欢迎语", history=history)
        self.assertIn("日常问候", result["answer"])
        self.assertEqual(calls, [])  # no business API cracked open for a generic chat

    async def test_stream_yesterday_after_completed_event_keeps_subject_and_date(self):
        # Drive the REAL LighthouseStream._accept over its synthetic Store/model
        # fixture after a completed event question. A deictic follow-up "昨天呢" must
        # preserve the event subject and shift to yesterday.
        fixture = _StreamFixture({"id": "fixture-d", "scopes": ["D"], "is_admin": False})
        fixture.seed_turn({
            "operation_id": "fixture_message_0001",
            "question": "今天发生了几条事件",
            "answer": "今天共 2 条事件通告。",
            "status": "completed",
            "run_id": "r" * 32,
            "scopes": ["D"],
            "at": 1.0,
        })
        run, _ack = fixture.accept("昨天呢", "fixture_message_0002")
        effective = effective_question(run["_turn"])
        self.assertIn("事件", effective)                      # event subject preserved
        yesterday = self.today - timedelta(days=1)
        self.assertEqual(date_window(effective), (yesterday, yesterday))  # yesterday's date

    async def test_stream_model_question_with_haiyou_does_not_inherit_business(self):
        # A self-contained new chat/model question ("你是谁，还有什么能力") asked AFTER an
        # unfinished-business question must not inherit the old business query, even
        # though it contains 还有.
        fixture = _StreamFixture({"id": "fixture-d", "scopes": ["D"], "is_admin": False})
        fixture.seed_turn({
            "operation_id": "fixture_message_0001",
            "question": "当前还有什么活没做完",
            "answer": "未完成工作 0 项。",
            "status": "completed",
            "run_id": "r" * 32,
            "scopes": ["D"],
            "at": 1.0,
        })
        run, _ack = fixture.accept("你是谁，还有什么能力", "fixture_message_0002")
        effective = effective_question(run["_turn"])
        self.assertEqual(effective, run["_turn"]["question"])   # accepted normalized text
        self.assertEqual(business_domains(effective), set())    # no business domain inherited
        self.assertNotIn("活没做完", effective)                  # no inherited unfinished-work text
        self.assertNotIn("未完成", effective)

    async def test_failed_pending_group_counts_are_none_not_zero(self):
        # Collection that cannot be trusted (missing stable identifiers) must surface
        # "待确认", never a fabricated zero.
        async def invoke(op):
            self.fail("no api should be read when events come from the cached reader")

        def cached(kind, scopes):
            return ([] if kind != "events" else []), ([] if kind != "events" else ["部分事件缺少稳定标识"])

        data = await collect_pending({"id": "fixture", "scopes": ["D"], "is_admin": False},
                                     "未完成工作", invoke, cached, groups_only={"events"})
        group = next(group for group in data["groups"] if group["key"] == "events")
        self.assertIsNone(group["count"])
        self.assertFalse(group["available"])
        self.assertIn("待确认", pending_reply(data))
        self.assertNotIn("为 **0 项**", pending_reply(data))


if __name__ == "__main__":
    unittest.main()
