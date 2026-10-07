"""Successful send counts use local business history, never provider guesses."""
import copy
import datetime as dt
import sys
import unittest
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lan_bitable_template_portal.lighthouse_ai import AssistantError
from lan_bitable_template_portal.lighthouse_model import LighthouseModel
from lan_bitable_template_portal.lighthouse_queries import collect_notice_sends, notice_sends_reply, sent_notice_question


def action(kind, job, day="2026-10-03"):
    return {"action": kind, "job_id": job, "time": day + " 10:00:00"}


def notice(identity, actions, kind="maintenance", scope="E"):
    return {"target_record_id": identity, "title": "隔离测试", "work_type": kind, "scope": scope,
            "start_time": "2026-10-03 00:00:00", "actions": actions}


class NoticeSendCountsTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.actor = {"id": "fixture", "scopes": ["E"]}
        self.calls = []
        self.rows = [
            notice("one", [action("start", "a"), action("update", "b"), action("end", "c")]),
            notice("two", [action("update", "d")], "change"),
            notice("three", [action("end", "e")], "repair"),
            notice("one", [action("start", "a")]),
            notice("older", [action("start", "old", "2026-10-02")]),
            notice("event", [action("start", "evt")], "event"),
            notice("private", [action("start", "private")], scope="A"),
            notice("unsent", []),
        ]

    async def invoke(self, op):
        self.calls.append(copy.deepcopy(op))
        self.assertEqual(op["api_id"], "GET /api/history-summary")
        return {"ok": True, "_raw": {"days": [{"date": "2026-10-03", "items": copy.deepcopy(self.rows)}]}}

    async def collect(self, **kwargs):
        return await collect_notice_sends(self.actor, "2026-10-03", "2026-10-03", self.invoke, **kwargs)

    async def test_deduplicates_notices_and_actions_excludes_plans_events_and_other_buildings(self):
        result = await self.collect()
        self.assertTrue(result["complete"])
        self.assertEqual((result["count"], result["send_count"]), (3, 5))
        self.assertEqual(result["actions"], {"start": 1, "update": 2, "end": 2})
        self.assertEqual(self.calls[0]["params"], {"scope": "E", "month": "2026-10", "work_type": "all"})
        reply = notice_sends_reply(result)
        self.assertIn("3 条通告，共 5 次", reply)
        self.assertNotIn("隔离测试", reply)
        self.assertIn("不含事件通告和未发送的计划", reply)

    async def test_work_type_filter(self):
        result = await self.collect(work_types=["change", "repair"])
        self.assertEqual((result["count"], result["send_count"]), (2, 2))
        self.assertEqual(result["by_type"], {"变更": 1, "检修": 1})

    async def test_incomplete_and_missing_actual_time_are_not_zero(self):
        self.rows = [notice("one", [{"action": "start"}])]
        result = await self.collect()
        self.assertFalse(result["complete"])
        self.assertIsNone(result["count"])
        self.assertIn("完整数量暂无法确认", notice_sends_reply(result))
        self.assertNotIn("已发送 **0", notice_sends_reply(result))

    async def test_empty_valid_history_is_zero(self):
        self.rows = []
        result = await self.collect()
        self.assertEqual(result["count"], 0)
        self.assertTrue(result["complete"])

    async def test_failed_or_truncated_source_is_not_zero(self):
        for payload in ({"ok": False}, {"ok": True, "data": {}}, {"ok": True, "truncated": True, "data": {"days": []}}):
            async def bad(_op): return payload
            result = await collect_notice_sends(self.actor, "2026-10-03", "2026-10-03", bad)
            self.assertIsNone(result["count"])
            self.assertFalse(result["complete"])

    async def test_cross_month_and_multi_building_dedup(self):
        shared = notice("shared", [action("start", "common")])
        shared.pop("scope"); shared["building_codes"] = ["A", "B"]
        async def load(op):
            self.calls.append(op)
            return {"ok": True, "_raw": {"days": [{"date": "2026-10-03", "items": [shared]}]}}
        result = await collect_notice_sends({"scopes": ["A", "B"]}, "2026-09-30", "2026-10-03", load)
        self.assertEqual(len(self.calls), 4)
        self.assertEqual((result["count"], result["send_count"]), (1, 1))

    async def test_no_scope_and_invalid_range_do_not_read(self):
        with self.assertRaises(AssistantError):
            await collect_notice_sends({"scopes": []}, "2026-10-03", "2026-10-03", self.invoke)
        with self.assertRaises(AssistantError):
            await collect_notice_sends(self.actor, "2026-10-03", "bad", self.invoke)
        self.assertEqual(self.calls, [])

    def test_count_intent_never_becomes_send_request(self):
        for question in ("今天总共发了多少通告", "今天已经发送了几条通告", "昨天发了多少维保通告", "本月检修通告发了多少"):
            self.assertTrue(sent_notice_question(question), question)
        for question in ("今天未发通告多少", "今天事件发生几条", "帮我发送通告", "今天发了多少通告请再发一条", "今天还有多少待发的变更", "今天和昨天分别发了多少通告"):
            self.assertFalse(sent_notice_question(question), question)

    async def test_user_question_bypasses_provider_even_when_unavailable(self):
        @asynccontextmanager
        async def forbidden(*_args):
            self.fail("Exact notice counts must not depend on the model")
            yield
        portal = SimpleNamespace(assistant=SimpleNamespace(model=Mock()),
            catalog=SimpleNamespace(get=lambda _: {"read_only": True, "name": "查询通告历史汇总", "schema": {}}))
        async def invoke(actor, op, request):
            result = await self.invoke(op)
            result["_raw"]["days"][0]["items"] = [r for r in self.rows if r["scope"] == "E"]
            return result
        portal._invoke = invoke
        labels = []
        async def emit(kind, data): labels.append(data)
        async def auth(): return self.actor
        engine = LighthouseModel(portal, model_factory=forbidden)
        for text in ("今天总共发了多少通告", "今天已经发送了几条通告"):
            with patch('lan_bitable_template_portal.lighthouse_model.date_window', return_value=(dt.date(2026,10,3), dt.date(2026,10,3))):
                result = await engine.answer(self.actor, {"question": text, "_profile": {}}, [], None, emit, auth, {})
            self.assertIn("3 条通告，共 5 次", result["answer"])
            self.assertEqual(result["sources"][0]["title"], "通告实际发送统计")
        self.assertNotIn("正在查询查询", str(labels))


if __name__ == '__main__':
    unittest.main()
