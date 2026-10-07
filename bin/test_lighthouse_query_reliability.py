"""Query routing and general conversation regressions, with isolated data only."""
import json
import sys
import unittest
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, AsyncMock

sys.path.insert(0, str(Path(__file__).resolve().parent))

from fastapi import FastAPI
from pydantic_ai.models.function import DeltaToolCall, FunctionModel

from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog
from lan_bitable_template_portal.lighthouse_model import (
    GENERAL_INSTRUCTIONS, INSTRUCTIONS, LighthouseModel, discover_for_question,
    instructions_for_question, public_catalog,
)
from lan_bitable_template_portal.lighthouse_queries import past_action_question, query_result_state, read_only_question


def catalog():
    app = FastAPI()

    @app.get("/api/workbench")
    def workbench(scope: str = "", search: str = ""):
        return {}

    @app.get("/api/repair-management/records")
    def repairs(scope: str = "", search: str = ""):
        return {}

    @app.get("/api/repair-management/records/{record_id}")
    def detail(record_id: str):
        return {}

    @app.post("/api/workbench-actions")
    def send():
        return {}

    return PortalAPICatalog(app)


class QueryContractTests(unittest.TestCase):
    def test_completed_actions_are_queries_not_mutations(self):
        for text in ("今天更新了几条维保通告", "已发送通告有多少", "删除过哪些维修单", "本月导出了几份表格"):
            with self.subTest(text=text):
                self.assertTrue(past_action_question(text))
                self.assertTrue(read_only_question(text))
        for text in ("帮我发送几条通告", "发送了几条通告，然后删除它们", "请把几条维修单改为已完成", "创建新的维修单", "删除这些记录"):
            with self.subTest(text=text):
                self.assertFalse(past_action_question(text))

    def test_record_title_discovery_offers_read_apis_without_fake_match(self):
        c = catalog()
        found = discover_for_question(c, "查看D楼冷水机组维保通告", keyword="D楼冷水机组维保通告")
        self.assertFalse(found["keyword_matched"])
        suggested = found["items"]
        self.assertIn("GET /api/workbench", [item["id"] for item in suggested])
        self.assertTrue(all(item["read_only"] for item in suggested))
        public = public_catalog(found)
        self.assertEqual(public["items"][0]["schema"], suggested[0]["schema"])
        self.assertIn("不代表没有业务记录", public["note"])

    def test_discovery_handles_multiple_domains_and_unknown_module(self):
        c = catalog()
        found = discover_for_question(c, "维保和维修项目中的冷水机组", keyword="冷水机组")
        ids = [item["id"] for item in found["items"]]
        self.assertIn("GET /api/workbench", ids)
        self.assertIn("GET /api/repair-management/records", ids)
        self.assertLess(ids.index("GET /api/repair-management/records"), ids.index("GET /api/repair-management/records/{record_id}"))
        found = discover_for_question(c, "不明确的内容", keyword="不存在的业务")
        self.assertEqual(found["items"], [])
        self.assertEqual(c.discover(keyword="水耗 不存在的操作")["total"], 0)

    def test_empty_partial_failed_and_found_are_distinct(self):
        cases = (
            ({"ok": True, "data": {"records": [], "total": 300, "page": 99}}, "empty_page"),
            ({"ok": True, "data": {"records": []}, "truncated": True}, "partial"),
            ({"ok": True, "data": {"complete": False, "buildings": []}}, "partial"),
            ({"ok": True, "data": {"records": [{"record_id": "one"}]}}, "found"),
            ({"ok": False, "error": "timeout", "data": {"records": []}}, "unavailable"),
            ({"ok": True, "data": {"options": []}}, "read"),
        )
        for result, expected in cases:
            with self.subTest(result=result):
                self.assertEqual(query_result_state(result)[0], expected)
        self.assertIn("必须保留", query_result_state({"ok": True, "data": {"items": []}})[1])

    def test_general_questions_do_not_receive_business_form_manual(self):
        for text in ("南通今天天气怎么样", "帮我翻译一段英文", "你是智能体还是助手？你是什么模型？", "解释Python列表推导式"):
            with self.subTest(text=text):
                result = instructions_for_question(text)
                self.assertEqual(result, GENERAL_INSTRUCTIONS)
                self.assertNotIn("command_format", result)
                self.assertLess(len(result), len(INSTRUCTIONS) / 5)
                self.assertIn("不要因为问题不属于灯塔业务而拒绝", result)
                self.assertIn("不能编造温度", result)
        self.assertIn("notice_sends", instructions_for_question("今天发了几条通告"))

    def test_general_advice_preserves_uncertainty_and_current_source_requirements(self):
        instructions = instructions_for_question('电脑设备怎么设置登录密码？给三个简单建议。')
        self.assertIn('当前权威来源', instructions)
        self.assertIn('未核实时只给一般原则', instructions)
        self.assertIn('不编造或强推数字阈值、强制周期和标准版本', instructions)
        self.assertIn('不提供密码最短长度、固定轮换周期等安全数值', instructions)
        self.assertIn('不默认建议定期更换密码', instructions)
        self.assertIn('模型记忆不算本轮核验', instructions)
        self.assertIn('不先给数字再加免责声明', instructions)
        self.assertNotIn('command_format', instructions)

    def test_live_probe_detects_unverified_security_thresholds_not_list_numbers(self):
        from tools.check_lighthouse_general import has_security_threshold
        for text in ('至少8~12位', '建议每90天更换', 'minimum 8 characters', '每三个月更换'):
            with self.subTest(text=text):
                self.assertTrue(has_security_threshold(text))
        self.assertFalse(has_security_threshold('Windows 11：1. 使用长且唯一的密码；2. 留好恢复途径。'))


class ModelRoutingTests(unittest.IsolatedAsyncioTestCase):
    async def test_usage_guidance_does_not_require_current_business_records(self):
        async def stream(messages, info):
            yield "在通告管理页面选择对应楼栋和类型，查看进行中列表即可。"

        result, calls, _ = await self.run_question("怎么查看未结束通告？", stream)
        self.assertIn("进行中列表", result["answer"])
        self.assertEqual(calls, [])
        self.assertNotIn("plan", result)

    async def run_question(self, question, stream, *, history=None, public_sources=None):
        portal = SimpleNamespace(
            assistant=SimpleNamespace(model=Mock()), catalog=catalog(),
            files=SimpleNamespace(context=lambda *_: [], image_parts=lambda *_: []),
            _public_references=lambda result, _: result,
            _source_url=lambda *_: "/workbench-lite",
        )
        calls = []

        async def invoke(actor, operation, request):
            calls.append(operation)
            return {"ok": True, "data": {"records": [], "total": 0}}

        portal._invoke = invoke

        @asynccontextmanager
        async def factory(*_):
            yield FunctionModel(stream_function=stream)

        engine = LighthouseModel(portal, model_factory=factory, public_sources=public_sources)
        actor = {"id": "test-user", "scopes": ["D"], "is_admin": False}

        async def auth():
            return actor

        events = []

        async def emit(kind, data):
            events.append((kind, data))

        result = await engine.answer(actor, {"question": question, "_profile": {"name": "独立测试模型", "model": "fixture/auto"}}, history or [], None, emit, auth, {})
        return result, calls, events

    async def test_rejected_weather_argument_returns_public_failure_not_business_retry(self):
        sources = SimpleNamespace(weather=AsyncMock(side_effect=ValueError('fixture city mismatch')))
        async def stream(messages, info):
            returned = [p for m in messages for p in m.parts if getattr(p, 'part_kind', '') == 'tool-return']
            if not returned:
                yield {0: DeltaToolCall(name='weather', json_args=json.dumps({'city': 'Nantong'}))}
            else:
                self.assertFalse(returned[-1].content['ok'])
                yield '天气资料未取得，请明确城市名称。'
        result, calls, _ = await self.run_question('南通今天天气怎么样?', stream, public_sources=sources)
        self.assertIn('实时资料未取得', result['answer'])
        self.assertNotIn('未取得可靠业务依据', result['answer'])
        self.assertEqual(calls, [])
        sources.weather.assert_awaited_once()

    async def test_identity_and_general_chat_keep_real_model_configuration(self):
        async def stream(messages, info):
            self.assertIn("fixture/auto", info.instructions)
            self.assertNotIn("command_format", info.instructions)
            yield "我是灯塔助手，本轮配置为fixture/auto，可以正常聊天并调用已接入的工具。"

        result, calls, _ = await self.run_question("你是智能体还是助手？你是什么模型？", stream)
        self.assertIn("fixture/auto", result["answer"])
        self.assertEqual(calls, [])

    async def test_past_update_question_does_not_force_write_form(self):
        async def stream(messages, info):
            returned = [p for m in messages for p in m.parts if getattr(p, "part_kind", "") == "tool-return"]
            if not returned:
                yield {0: DeltaToolCall(name="query", json_args=json.dumps({"operation": {"api_id": "GET /api/workbench", "params": {"scope": "D"}}}))}
            else:
                self.assertEqual(returned[-1].content["query_state"], "empty_page")
                self.assertIn("日期", returned[-1].content["query_guidance"])
                yield "本次工作台查询没有记录；这不是更新次数的统计，尚不能确认今天的发送次数。"

        result, calls, _ = await self.run_question("更新了哪些维保通告", stream)
        self.assertEqual(len(calls), 1)
        self.assertNotIn("plan", result)
        self.assertIn("尚不能确认", result["answer"])


if __name__ == "__main__":
    unittest.main()
