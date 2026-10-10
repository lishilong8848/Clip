"""Feishu assistant plan-edit command tests.

Deterministic text commands reuse the SAME existing plan APIs the web uses
(PATCH plans/{id}, GET plans/{id}/options, POST confirm/retry/cancel). These
tests use AsyncMock and fake transports only: no server, no real Feishu writes.
"""
import copy
import hashlib
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock

sys.path.insert(0, str(Path(__file__).parent))
from lan_bitable_template_portal.feishu_assistant import FeishuAssistant, Inbox, parse_message
from openclaw_service.assistant.lighthouse_ai import AssistantError


def event(owner="onPerson", *, chat_type="p2p", bot="ouBot", message_id="m0", content="今天有哪些通告"):
    return {"header": {"app_id": "cliTest", "event_type": "im.message.receive_v1"}, "event": {
        "sender": {"sender_type": "user", "sender_id": {"open_id": "ou_Person", "union_id": "on_"+owner}},
        "message": {"message_id": "om_"+message_id, "chat_id": "oc_Chat", "chat_type": chat_type,
            "message_type": "text", "content": json.dumps({"text": content}),
            "mentions": [{"key": "@_user_1", "id": {"open_id": bot}}]}}}


def to_message(content, **kwargs):
    return parse_message(event(content=content, **kwargs), "cliTest", "ouBot")


def field(name, label, ftype, value=None, options=None, options_source=None, **extra):
    data = {"name": name, "label": label, "type": ftype, "value": value,
            "required": bool(extra.pop("required", False)), "options": options or [],
            "options_source": options_source, "path": name}
    data.update(extra)
    return data


def needs_plan(plan_id="P1", title="维保变更设备调整", version=3, fields=None):
    return {"id": plan_id, "title": title, "explanation": "更新设备调整单",
            "status": "needs_input", "version": version, "fields": fields or []}


class Memory:
    def __init__(self, path=None):
        self.docs = {}
        self.db_path = path
    def get_document(self, ns, key):
        return copy.deepcopy(self.docs.get((ns, key)))
    def put_document(self, ns, key, value):
        self.docs[(ns, key)] = copy.deepcopy(value)
    def delete_document(self, ns, key):
        self.docs.pop((ns, key), None)
    def list_documents(self, ns, *, key_prefix=''):
        return [{'key': key, 'payload': copy.deepcopy(value)} for (namespace, key), value in self.docs.items()
                if namespace == ns and key.startswith(key_prefix)]


class PlanCommandTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.runtime = SimpleNamespace(state_store=Memory(Path(self.temp.name)/"portal.sqlite3"),
                                       auth_manager=SimpleNamespace(_open_id_explicitly_disabled=lambda _: False))
        self.controller = SimpleNamespace(bound_port=18766, preferred_port=18766)
        self.channel = FeishuAssistant(self.controller, self.runtime, AsyncMock())
        self.channel.inbox = Inbox(Path(self.temp.name)/"messages.sqlite3")
        self.channel._request_context = AsyncMock(return_value=(None, {"id": "ou_A"}))

    async def answer(self, content, plans, *results):
        calls = [{"conversation_id": "conv1", "turns": [{"plan": plan} for plan in plans]}]
        calls.extend(results)
        self.channel._call = AsyncMock(side_effect=calls)
        message = to_message(content)
        return await self.channel.answer(message), self.channel._call

    async def test_show_current_plan_lists_fields_with_values(self):
        plan = needs_plan(fields=[
            field("step0.device_name", "设备名称", "text", value="空调1#"),
            field("step0.quantity", "数量", "number", value=2),
            field("step0.reason", "原因", "select", value="b",
                  options=[{"value": "a", "label": "老化"}, {"value": "b", "label": "故障"}]),
            field("step0.tags", "标签", "multiselect", value=["x"],
                  options=[{"value": "x", "label": "紧急"}, {"value": "y", "label": "普通"}]),
        ])
        result, calls = await self.answer("查看当前计划", [plan])
        self.assertIn("step0.device_name", result["text"])
        self.assertIn("空调1#", result["text"])
        self.assertIn("故障", result["text"])
        self.assertIn("紧急", result["text"])
        self.assertEqual(calls.await_count, 1)

    async def test_fill_fields_patches_existing_plan_without_autoexecute(self):
        plan = needs_plan(fields=[
            field("step0.device_name", "设备名称", "text"),
            field("step0.quantity", "数量", "number"),
        ])
        result, calls = await self.answer("step0.device_name：空调1#\nstep0.quantity：2.5", [plan],
                                          {"id": "P1", "status": "awaiting_confirmation",
                                           "version": 4, "operations": [], "title": "维保变更设备调整", "explanation": ""})
        self.assertIn("已按你的填写更新操作计划", result["text"])
        self.assertIn("未执行", result["text"])
        self.assertEqual(calls.await_count, 2)
        path = calls.await_args.args[2]
        kwargs = calls.await_args.kwargs
        self.assertEqual(kwargs["method"], "PATCH")
        self.assertEqual(path, "plans/P1")
        self.assertEqual(calls.await_args.args[3], {"version": 3, "values": {"step0.device_name": "空调1#", "step0.quantity": 2.5}})
        # no confirm/execute call was made
        self.assertNotIn("confirm", path)
        self.assertNotIn("confirm", str(calls.await_args_list))

    async def test_fill_typed_validation_rejects_bad_number_and_date(self):
        plan = needs_plan(fields=[
            field("step0.quantity", "数量", "number"),
            field("step0.when", "日期", "date"),
        ])
        result, calls = await self.answer("step0.quantity：不是数字\nstep0.when：2026-10-09", [plan])
        self.assertIn("请填写数字", result["text"])
        self.assertEqual(calls.await_count, 1)  # conversation only, no PATCH

    async def test_fill_select_maps_label_to_option_value(self):
        plan = needs_plan(fields=[
            field("step0.reason", "原因", "select", options=[{"value": "a", "label": "老化"}, {"value": "b", "label": "故障"}]),
        ])
        result, calls = await self.answer("step0.reason：故障", [plan],
                                          {"id": "P1", "status": "awaiting_confirmation", "version": 4,
                                           "operations": [], "title": "维保变更设备调整"})
        self.assertIn("已按你的填写更新操作计划", result["text"])
        self.assertEqual(calls.await_args.args[3]["values"], {"step0.reason": "b"})

    async def test_fill_multiselect_splits_and_validates(self):
        plan = needs_plan(fields=[
            field("step0.tags", "标签", "multiselect",
                  options=[{"value": "x", "label": "紧急"}, {"value": "y", "label": "普通"}]),
        ])
        result, calls = await self.answer("step0.tags：紧急、y", [plan],
                                          {"id": "P1", "status": "awaiting_confirmation", "version": 4,
                                           "operations": [], "title": "维保变更设备调整"})
        self.assertIn("已按你的填写更新操作计划", result["text"])
        self.assertEqual(calls.await_args.args[3]["values"]["step0.tags"], ["x", "y"])

    async def test_options_source_requires_options_loaded_before_fill(self):
        plan = needs_plan(fields=[
            field("step0.target", "目标", "select", options_source="service", options=[]),
        ])
        result, calls = await self.answer("step0.target：abc", [plan])
        self.assertIn("请从候选选项中选择", result["text"])
        self.assertEqual(calls.await_count, 1)

    async def test_complex_field_redirects_to_original_page_not_silent_success(self):
        plan = needs_plan(fields=[
            field("step0.device_name", "设备名称", "text"),
            field("step0.attachment", "附件", "file"),
        ])
        result, calls = await self.answer("step0.device_name：空调1#\nstep0.attachment：不填", [plan])
        self.assertIn("复杂表单", result["text"])
        self.assertIn("灯塔原业务页面", result["text"])
        self.assertEqual(calls.await_count, 1)  # no PATCH

    async def test_unknown_field_line_falls_through_to_chat(self):
        plan = needs_plan(fields=[field("step0.device_name", "设备名称", "text")])
        # "review：请说明流程" is prose with a colon, not a known field
        op = "feishu_" + hashlib.sha256(b"om_m0").hexdigest()[:40]
        result, calls = await self.answer("review：请说明流程", [plan],
                                          {"turns": [{"operation_id": op, "answer": "这是普通问答"}]})
        self.assertEqual(result["text"], "这是普通问答")
        self.assertEqual(calls.await_count, 2)  # conversation then chat

    async def test_ambiguous_pending_asks_choose_and_never_overwrites(self):
        plan_a = needs_plan("A", "计划A")
        plan_b = needs_plan("B", "计划B")
        plan_a["fields"] = [field("step0.device_name", "设备名称", "text")]
        plan_b["fields"] = [field("step0.device_name", "设备名称", "text")]
        result, calls = await self.answer("step0.device_name：空调1#", [plan_a, plan_b])
        self.assertIn("选择计划", result["text"])
        self.assertEqual(calls.await_count, 1)  # conversation only, no PATCH

    async def test_picked_plan_resolves_ambiguity(self):
        plan_a = needs_plan("A", "计划A")
        plan_b = needs_plan("B", "计划B")
        for plan in (plan_a, plan_b):
            plan["fields"] = [field("step0.device_name", "设备名称", "text")]
        message = to_message("选择计划 2")
        self.channel._request_context = AsyncMock(return_value=(None, {"id": "ou_A"}))
        self.channel._call = AsyncMock(side_effect=[{"turns": [{"plan": plan_a}, {"plan": plan_b}]}])
        result = await self.channel.answer(message)
        self.assertIn("计划B", result["text"])
        # now filling picks B without ambiguity
        message = to_message("step0.device_name：空调1#")
        self.channel._call = AsyncMock(side_effect=[
            {"turns": [{"plan": plan_a}, {"plan": plan_b}]},
            {"id": "B", "status": "awaiting_confirmation", "version": 4, "operations": [], "title": "计划B"}])
        result = await self.channel.answer(message)
        self.assertIn("已按你的填写更新操作计划", result["text"])

    async def test_options_command_calls_options_api_and_shows_real_values(self):
        plan = needs_plan(fields=[
            field("step0.target", "目标设备", "select", options_source="device"),
        ])
        updated = needs_plan()
        updated["fields"] = [field("step0.target", "目标设备", "select", options_source="device",
                                   options=[{"value": "dv-001", "label": "1号空调"}, {"value": "dv-002", "label": "2号空调"}])]
        result, calls = await self.answer("查看选项 step0.target 空调", [plan], updated)
        self.assertIn("dv-001", result["text"])
        self.assertIn("1号空调", result["text"])
        self.assertIn("2号空调", result["text"])
        self.assertEqual(calls.await_count, 2)
        option_call = calls.await_args
        self.assertEqual(option_call.args[2], "plans/P1/options")
        self.assertIn("field", option_call.kwargs["params"])
        self.assertEqual(option_call.kwargs["params"]["field"], "step0.target")
        # The real routes.py field_options signature ignores a keyword: no q is
        # sent, the returned page is filtered locally instead.
        self.assertNotIn("q", option_call.kwargs["params"])

    async def test_options_filtered_locally_and_partial_does_not_claim_zero_full_results(self):
        plan = needs_plan(fields=[
            field("step0.target", "目标设备", "select", options_source="device"),
        ])
        updated = needs_plan()
        updated["fields"] = [field("step0.target", "目标设备", "select", options_source="device",
                                   options=[{"value": "dv-001", "label": "1号空调"}, {"value": "dv-002", "label": "2号空调"}],
                                   options_has_more=True, options_total=200)]
        result, calls = await self.answer("查看选项 step0.target 不存在的词", [plan], updated)
        # zero local matches on a partial set must NOT be presented as definitive
        # "no matching options".
        self.assertIn("仍可能包含匹配项", result["text"])
        self.assertNotIn("无匹配候选项", result["text"])
        # show a term that matches only some of the loaded page
        result2, calls2 = await self.answer("查看选项 step0.target 1号", [plan], updated)
        self.assertIn("1号空调", result2["text"])
        self.assertNotIn("2号空调", result2["text"])

    async def test_no_plans_prose_with_colon_falls_through_to_chat(self):
        # No pending plans: ordinary prose containing a colon must NOT be
        # interpreted as a field write.
        op = "feishu_" + hashlib.sha256(b"om_m0").hexdigest()[:40]
        result, calls = await self.answer("请说明：维保已完成", [],
                                          {"turns": [{"operation_id": op, "answer": "普通对话回答"}]})
        self.assertEqual(result["text"], "普通对话回答")
        self.assertEqual(calls.await_count, 2)  # conversation then chat

    async def test_select_duplicate_label_rejected(self):
        plan = needs_plan(fields=[
            field("step0.reason", "原因", "select",
                  options=[{"value": "a", "label": "老化"}, {"value": "b", "label": "老化"}]),
        ])
        result, calls = await self.answer("step0.reason：老化", [plan])
        self.assertIn("多个", result["text"])
        self.assertEqual(calls.await_count, 1)  # no PATCH

    async def test_multiselect_does_not_split_slash(self):
        plan = needs_plan(fields=[
            field("step0.tag", "标签", "multiselect",
                  options=[{"value": "A/B", "label": "A/B类别"}, {"value": "C", "label": "C类"}]),
        ])
        # A value containing "/" is treated literally, not as a separator.
        result, calls = await self.answer("step0.tag：A/B", [plan], {
            "id": "P1", "status": "awaiting_confirmation", "version": 4, "operations": [], "title": "维保变更设备调整"})
        self.assertIn("已按你的填写更新操作计划", result["text"])
        self.assertEqual(calls.await_args.args[3]["values"]["step0.tag"], ["A/B"])

    async def test_float_rejects_nan_and_inf(self):
        plan = needs_plan(fields=[
            field("step0.ratio", "比例", "float"),
        ])
        for bad in ("nan", "inf", "-inf"):
            with self.subTest(bad=bad):
                result, calls = await self.answer("step0.ratio：" + bad, [plan])
                self.assertIn("数字无效", result["text"])
                self.assertEqual(calls.await_count, 1)

    async def test_fill_ambiguous_label_rejects(self):
        plan = needs_plan(fields=[
            field("step0.a", "名称", "text"),
            field("step0.b", "名称", "text"),
        ])
        result, calls = await self.answer("名称：abc", [plan])
        self.assertIn("多个字段", result["text"])
        self.assertEqual(calls.await_count, 1)

    async def test_fill_readonly_field_rejected(self):
        plan = needs_plan(fields=[
            field("step0.device_name", "设备名称", "text"),
            field("step0.fixed", "固定字段", "text", readonly=True),
        ])
        result, calls = await self.answer("step0.fixed：xxx", [plan])
        self.assertIn("只读字段", result["text"])
        self.assertEqual(calls.await_count, 1)

    async def test_selected_needs_input_plan_shows_editable_fields(self):
        plan = needs_plan(fields=[
            field("step0.device_name", "设备名称", "text"),
            field("step0.fixed", "固定字段", "text", readonly=True),
        ])
        message = to_message("选择计划 1")
        self.channel._call = AsyncMock(side_effect=[{"turns": [{"plan": plan}]}])
        result = await self.channel.answer(message)
        self.assertIn("step0.device_name", result["text"])
        self.assertIn("只读", result["text"])
        self.assertIn("不可修改", result["text"])

    async def test_status_command_shows_unique_plan_and_version(self):
        plan = {"id": "F", "title": "维保变更设备调整", "status": "failed", "version": 7,
                "error": "写入超时", "fields": []}
        result, calls = await self.answer("操作状态", [plan])
        self.assertIn("失败", result["text"])
        self.assertIn("版本:7", result["text"])
        self.assertIn("重试操作", result["text"])
        self.assertEqual(calls.await_count, 1)

    async def test_retry_uses_unique_plan_version_and_retains_confirmation_context(self):
        failed = {"id": "F", "title": "维保变更设备调整", "status": "failed", "version": 7, "fields": []}
        retried = {"id": "F", "title": "维保变更设备调整", "status": "running", "version": 8, "fields": []}
        result, calls = await self.answer("重试操作", [failed], retried)
        self.assertIn("已提交原业务流程", result["text"])
        self.assertEqual(calls.await_count, 2)
        retry_call = calls.await_args
        self.assertEqual(retry_call.args[2], "plans/F/retry")
        self.assertEqual(retry_call.args[3], {"version": 7})
        self.assertEqual(retry_call.kwargs["plan_id"], "F")

    async def test_cancel_uses_unique_plan(self):
        plan = needs_plan("P1", "计划A", version=2)
        result, calls = await self.answer("取消操作", [plan], {})
        self.assertIn("已取消", result["text"])
        self.assertEqual(calls.await_args.args[2], "plans/P1/cancel")

    async def test_picked_plan_is_isolated_per_person_and_chat(self):
        plan_a = needs_plan("A")
        plan_b = needs_plan("B")
        for plan in (plan_a, plan_b):
            plan["fields"] = [field("step0.device_name", "设备名称", "text")]
        # The selection hint is keyed by the stable actor id resolved from the
        # session, not the raw event open_id.
        message = to_message("选择计划 2")
        message["sender_id"] = "ou_1"; message["chat_id"] = "oc_1"
        self.channel._request_context = AsyncMock(return_value=(None, {"id": "ou_A"}))
        self.channel._call = AsyncMock(side_effect=[{"turns": [{"plan": plan_a}, {"plan": plan_b}]}])
        await self.channel.answer(message)
        self.assertEqual(self.channel.picked_plan[("ou_A", "oc_1")], "B")
        # The raw event open_id is never used as the ownership key.
        self.assertNotIn(("ou_1", "oc_1"), self.channel.picked_plan)
        # Same actor, different room: no pick yet.
        self.assertNotIn(("ou_A", "oc_2"), self.channel.picked_plan)
        # A different actor in the same room does not inherit the pick.
        self.assertNotIn(("ou_2", "oc_1"), self.channel.picked_plan)


class FakeContexts:
    def __init__(self):
        self.contexts = {}
    def context(self, request, actor, plan_id=None):
        cid = "ctx_" + str(actor["id"]) + "_" + str(plan_id)
        self.contexts[cid] = True
        return cid


class FakeClient:
    def __init__(self, *, ok=True):
        self.calls = []
        self.ok = ok
    async def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        if not self.ok:
            return SimpleNamespace(status_code=500, json=lambda: {"ok": False, "error": "boom"})
        return SimpleNamespace(status_code=200, json=lambda: {"ok": True, "data": {}})


class FakeCurrent:
    key = "k"
    instance = "i"
    lease = "l"
    descriptor = {"port": 18766}
    def __init__(self, client):
        self.client = client
    async def http_client(self):
        return self.client


class CallTransportTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = SimpleNamespace(state_store=Memory(), auth_manager=SimpleNamespace())
        self.controller = SimpleNamespace(bound_port=18766, preferred_port=18766)
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.channel = FeishuAssistant(self.controller, self.runtime, AsyncMock())
        self.channel.inbox = Inbox(Path(self.temp.name)/"messages.sqlite3")
        self.actor = {"id": "ou_A"}

    def build(self, ok=True):
        client = FakeClient(ok=ok)
        authority = FakeContexts()
        self.channel.ready = AsyncMock(return_value=(FakeCurrent(client), authority))
        return client, authority

    async def test_patch_sends_method_and_params(self):
        client, authority = self.build()
        await self.channel._call(None, self.actor, "plans/P1", {"version": 3, "values": {"a": 1}},
                                 method="PATCH", params={"x": "y"})
        method, url, kwargs = client.calls[0]
        self.assertEqual(method, "PATCH")
        self.assertTrue(url.endswith("/api/assistant/plans/P1"))
        self.assertEqual(kwargs["json"], {"version": 3, "values": {"a": 1}})
        self.assertEqual(kwargs["params"], {"x": "y"})
        # non-execution context cleaned up
        self.assertNotIn("ctx_ou_A_None", authority.contexts)

    async def test_conversation_get_cleans_context(self):
        _, authority = self.build()
        await self.channel._call(None, self.actor, "conversation")
        self.assertNotIn("ctx_ou_A_None", authority.contexts)

    async def test_non_execution_context_cleaned_even_on_http_error(self):
        _, authority = self.build(ok=False)
        with self.assertRaises(AssistantError):
            await self.channel._call(None, self.actor, "plans/P1", {}, params={"field": "a"})
        self.assertNotIn("ctx_ou_A_None", authority.contexts)

    async def test_confirmation_context_retained_on_success(self):
        client, authority = self.build()
        await self.channel._call(None, self.actor, "plans/P1/confirm", {"version": 3, "stage": "review"},
                                 plan_id="P1")
        self.assertIn("ctx_ou_A_P1", authority.contexts)

    async def test_confirmation_context_retained_even_on_http_error(self):
        _, authority = self.build(ok=False)
        with self.assertRaises(AssistantError):
            await self.channel._call(None, self.actor, "plans/P1/retry", {"version": 3}, plan_id="P1")
        self.assertIn("ctx_ou_A_P1", authority.contexts)


if __name__ == "__main__":
    unittest.main()