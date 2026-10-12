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
from lan_bitable_template_portal import feishu_notice_forms as notice_forms
from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_agent import _editable_patch
from openclaw_service.assistant.lighthouse_notice_sop import selection_field, selection_payload, update_directory


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

    async def test_picked_plan_follows_person_across_chats(self):
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
        self.assertEqual(self.channel.picked_plan["ou_A"], "B")
        # The raw event open_id is never used as the ownership key.
        self.assertNotIn("ou_1", self.channel.picked_plan)
        message['chat_id'] = 'oc_2'
        picked, _ = self.channel._resolve_plan(message, {'A': plan_a, 'B': plan_b}, {'needs_input'}, {'id': 'ou_A'})
        self.assertEqual(picked['id'], 'B')
        picked, _ = self.channel._resolve_plan(message, {'A': plan_a, 'B': plan_b}, {'needs_input'}, {'id': 'ou_B'})
        self.assertIsNone(picked)


NOTICE_CHILDREN = [
    {"path": "title", "label": "标题", "type": "text", "required": True},
    {"path": "building_codes", "label": "楼栋/范围", "type": "multiselect", "required": True,
     "options": [{"value": "A", "label": "A栋"}, {"value": "B", "label": "B栋"}]},
    {"path": "start_time", "label": "开始时间", "type": "datetime-local", "required": True},
    {"path": "end_time", "label": "结束时间", "type": "datetime-local", "required": True},
    {"path": "specialty", "label": "专业", "type": "select", "required": True,
     "options": [{"value": "hv", "label": "暖通"}, {"value": "el", "label": "电气"}]},
    {"path": "progress", "label": "进度", "type": "number", "required": False, "hidden": True},
]


def notice_patch_field(name="step0.patch", *, value=None, children=None, label="维保通告填写"):
    return {"name": name, "label": label, "type": "object", "path": "patch",
            "options_source": "notice", "required": True, "native_notice": True,
            "value": value, "children": children if children is not None else NOTICE_CHILDREN}


def notice_sop_field(name="step0.notice_sop", *, value=None, sops=None, people=None,
                     scopes=None, directory_scope=None, work_type="maintenance"):
    return {"name": name, "label": "SOP/人员选择", "type": "object", "path": "notice_sop",
            "options_source": "notice_sops", "required": True, "native_notice_sop": True,
            "value": value, "work_type": work_type, "directory_scope": directory_scope,
            "scopes": scopes if scopes is not None else ["A"],
            "sops": sops if sops is not None else [], "people": people if people is not None else []}


class NoticeFormEditTests(unittest.IsolatedAsyncioTestCase):
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

    async def test_native_notice_display_flattens_and_hides_progress(self):
        plan = needs_plan(fields=[notice_patch_field()])
        # simulate a filled value with $query protocol key (as the Agent returns it)
        plan["fields"][0]["value"] = {"$query": {"ref": "q1", "path": []},
                                      "title": "空调维修", "building_codes": ["A", "B"],
                                      "specialty": "hv", "start_time": "2026-10-09T10:00",
                                      "end_time": "2026-10-09T12:00", "progress": 30}
        result, calls = await self.answer("查看当前计划", [plan])
        self.assertIn("step0.patch.标题", result["text"])
        self.assertIn("空调维修", result["text"])
        self.assertIn("楼栋/范围", result["text"])
        self.assertIn("A栋、B栋", result["text"])
        self.assertIn("专业", result["text"])
        # adjustment progress must never be exposed even if present in value
        self.assertNotIn("进度", result["text"])
        self.assertNotIn("step0.patch.progress", result["text"])
        self.assertEqual(calls.await_count, 1)

    async def test_native_notice_fill_merges_only_edited_children_into_parent(self):
        field = notice_patch_field(value={"title": "旧标题", "building_codes": ["A"],
                                          "specialty": "hv", "start_time": "2026-10-09T10:00",
                                          "end_time": "2026-10-09T12:00", "progress": 30})
        plan = needs_plan(fields=[field])
        result, calls = await self.answer(
            "step0.patch.标题：新标题\nstep0.patch.楼栋/范围：A、B",
            [plan],
            {"id": "P1", "status": "awaiting_confirmation", "version": 4,
             "operations": [], "title": "维保变更设备调整"})
        self.assertIn("已按你的填写更新操作计划", result["text"])
        self.assertEqual(calls.await_count, 2)
        payload = calls.await_args.args[3]["values"]["step0.patch"]
        # the parent object schema is a plain dict (no $ protocol keys)
        self.assertNotIn("$query", payload)
        self.assertNotIn("$", payload)
        # unedited/hidden children are preserved, only edited children changed
        self.assertEqual(payload["title"], "新标题")
        self.assertEqual(payload["building_codes"], ["A", "B"])
        self.assertEqual(payload["specialty"], "hv")
        self.assertEqual(payload["start_time"], "2026-10-09T10:00")
        self.assertEqual(payload["end_time"], "2026-10-09T12:00")
        self.assertEqual(payload["progress"], 30)

    async def test_native_notice_fresh_start_forwards_only_edited_children(self):
        plan = needs_plan(fields=[notice_patch_field()])
        result, calls = await self.answer(
            "step0.patch.标题：停机检修\nstep0.patch.专业：暖通",
            [plan],
            {"id": "P1", "status": "awaiting_confirmation", "version": 4,
             "operations": [], "title": "维保变更设备调整"})
        payload = calls.await_args.args[3]["values"]["step0.patch"]
        self.assertEqual(payload["title"], "停机检修")
        self.assertEqual(payload["specialty"], "hv")
        self.assertNotIn("building_codes", payload)
        self.assertNotIn("progress", payload)

    async def test_native_notice_hidden_and_invalid_leaf_rejected(self):
        plan = needs_plan(fields=[notice_patch_field()])
        result, calls = await self.answer("step0.patch.progress：30", [plan])
        self.assertIn("只读字段", result["text"])
        self.assertEqual(calls.await_count, 1)
        result2, calls2 = await self.answer("step0.patch.专业：不存在专业", [plan])
        self.assertIn("不在候选选项", result2["text"])
        self.assertEqual(calls2.await_count, 1)

    async def test_native_notice_whole_field_asks_to_use_leaves(self):
        plan = needs_plan(fields=[notice_patch_field()])
        result, calls = await self.answer("step0.patch：整体", [plan])
        self.assertIn("整体对象", result["text"])
        self.assertIn("叶子字段名", result["text"])
        self.assertEqual(calls.await_count, 1)

    async def test_notice_sop_selected_sop_people_scope(self):
        sop_payload = notice_sop_field(
            directory_scope="A",
            sops=[{"sop_id": "sop-maint", "name": "维保SOP", "mode": "normal", "version": 3}],
            people=[{"record_id": "person-one", "name": "测试操作人", "label": "测试操作人"},
                    {"record_id": "person-two", "name": "测试审核人", "label": "测试审核人"}],
        )
        plan = needs_plan(fields=[sop_payload])
        result, calls = await self.answer(
            "step0.notice_sop.不使用工单：否\nstep0.notice_sop.楼栋：A\n"
            "step0.notice_sop.SOP：维保SOP\nstep0.notice_sop.操作人：测试操作人\n"
            "step0.notice_sop.审核人：测试审核人",
            [plan],
            {"id": "P1", "status": "awaiting_confirmation", "version": 4,
             "operations": [], "title": "维保变更设备调整"})
        self.assertIn("已按你的填写更新操作计划", result["text"])
        selected = calls.await_args.args[3]["values"]["step0.notice_sop"]
        self.assertFalse(selected["exempt"])
        self.assertEqual(selected["scope"], "A")
        self.assertEqual(selected["sop_id"], "sop-maint")
        self.assertEqual(selected["operator_record_id"], "person-one")
        self.assertEqual(selected["reviewer_record_id"], "person-two")
        self.assertEqual(selected["runs"], [])

    async def test_notice_sop_exemption_explicitly_selected(self):
        plan = needs_plan(fields=[notice_sop_field()])
        result, calls = await self.answer("step0.notice_sop.不使用工单：是", [plan],
                                          {"id": "P1", "status": "awaiting_confirmation", "version": 4,
                                           "operations": [], "title": "维保变更设备调整"})
        selected = calls.await_args.args[3]["values"]["step0.notice_sop"]
        self.assertTrue(selected["exempt"])
        self.assertEqual(selected["sop_id"], "")
        self.assertEqual(selected["operator_record_id"], "")
        self.assertEqual(selected["runs"], [])

    async def test_notice_sop_ambiguity_asks_for_selection(self):
        sop_payload = notice_sop_field(
            sops=[{"sop_id": "s1", "name": "同名SOP", "mode": "normal"},
                  {"sop_id": "s2", "name": "同名SOP", "mode": "normal"}],
            people=[{"record_id": "p1", "name": "张三", "label": "张三"}])
        plan = needs_plan(fields=[sop_payload])
        result, calls = await self.answer("step0.notice_sop.SOP：同名SOP", [plan])
        self.assertIn("多个", result["text"])
        self.assertEqual(calls.await_count, 1)

    async def test_notice_sop_unknown_option_needs_loaded_directory_no_invented_id(self):
        plan = needs_plan(fields=[notice_sop_field()])
        result, calls = await self.answer("step0.notice_sop.SOP：编造的SOP", [plan])
        self.assertIn("请先发送", result["text"])  # must force directory load, never invent an ID
        self.assertEqual(calls.await_count, 1)

    async def test_notice_sop_runs_parsed_for_polling_mode(self):
        sop_payload = notice_sop_field(
            directory_scope="A",
            sops=[{"sop_id": "sop-poll", "name": "轮巡SOP", "mode": "polling"}],
            people=[], scopes=["A"],
            value={"exempt": False, "scope": "A", "sop_id": "sop-poll",
                   "operator_record_id": "", "reviewer_record_id": "", "runs": []})
        plan = needs_plan(fields=[sop_payload])
        result, calls = await self.answer("step0.notice_sop.运行：1#→2#、3#→4#", [plan],
                                          {"id": "P1", "status": "awaiting_confirmation", "version": 4,
                                           "operations": [], "title": "维保变更设备调整"})
        self.assertIn("已按你的填写更新操作计划", result["text"])
        selected = calls.await_args.args[3]["values"]["step0.notice_sop"]
        self.assertEqual(selected["runs"], [{"from_unit": "1#", "to_unit": "2#"},
                                            {"from_unit": "3#", "to_unit": "4#"}])

    async def test_notice_sop_options_calls_api_with_scope(self):
        plan = needs_plan(fields=[notice_sop_field()])
        updated = needs_plan()
        updated["fields"] = [notice_sop_field(directory_scope="A",
                                              sops=[{"sop_id": "sop-maint", "name": "维保SOP", "mode": "normal"}],
                                              people=[{"record_id": "p1", "name": "张三"}],
                                              scopes=["A", "B"])]
        result, calls = await self.answer("查看选项 step0.notice_sop A", [plan], updated)
        self.assertIn("维保SOP", result["text"])
        self.assertIn("张三", result["text"])
        call = calls.await_args
        self.assertEqual(call.args[2], "plans/P1/options")
        self.assertEqual(call.kwargs["params"]["field"], "step0.notice_sop")
        self.assertEqual(call.kwargs["params"]["scope"], "A")

    async def test_native_notice_leaf_options_inline(self):
        plan = needs_plan(fields=[notice_patch_field()])
        result, calls = await self.answer("查看选项 step0.patch.专业", [plan])
        self.assertIn("暖通", result["text"])
        self.assertIn("电气", result["text"])
        self.assertEqual(calls.await_count, 1)  # no API call for inline leaf options

    async def test_awaiting_confirmation_edit_returns_to_needs_input_first(self):
        plan = {"id": "P1", "title": "维保变更设备调整", "status": "awaiting_confirmation",
                "version": 6, "fields": [field("step0.device_name", "设备名称", "text")],
                "operations": [], "explanation": ""}
        needs = needs_plan(version=7, fields=[field("step0.device_name", "设备名称", "text")])
        result, calls = await self.answer("step0.device_name：空调2#", [plan], needs,
                                          {"id": "P1", "status": "awaiting_confirmation", "version": 8,
                                           "operations": [], "title": "维保变更设备调整"})
        self.assertIn("已按你的填写更新操作计划", result["text"])
        self.assertEqual(calls.await_count, 3)
        edit_call = calls.await_args_list[1]
        self.assertEqual(edit_call.args[2], "plans/P1")
        self.assertEqual(edit_call.args[3], {"version": 6, "action": "edit"})
        patch_call = calls.await_args_list[2]
        self.assertEqual(patch_call.args[3]["version"], 7)

    async def test_multiple_plans_asks_selection_before_native_edit(self):
        plan_a = needs_plan("A", "计划A")
        plan_b = needs_plan("B", "计划B")
        for plan in (plan_a, plan_b):
            plan["fields"] = [notice_patch_field()]
        result, calls = await self.answer("step0.patch.标题：x", [plan_a, plan_b])
        self.assertIn("选择计划", result["text"])
        self.assertEqual(calls.await_count, 1)

    async def test_complex_native_marker_without_children_still_denied(self):
        # A native notice field must expose children; without them nothing is editable.
        bare = {"name": "step0.patch", "label": "通告", "type": "object", "path": "patch",
                "native_notice": True, "children": []}
        plan = needs_plan(fields=[bare])
        result, calls = await self.answer("step0.patch.标题：x", [plan])
        self.assertIn("未识别字段", result["text"])  # nothing editable => denied, not silently written
        self.assertEqual(calls.await_count, 1)

    async def test_stale_version_error_is_surfaced_not_executed(self):
        plan = needs_plan(fields=[field("step0.device_name", "设备名称", "text")])
        calls = [{"conversation_id": "conv1", "turns": [{"plan": plan}]},
                 AssistantError("版本已变化，请重新读取", 409)]
        self.channel._call = AsyncMock(side_effect=calls)
        message = to_message("step0.device_name：x")
        with self.assertRaises(AssistantError):
            await self.channel.answer(message)

    async def test_agent_editable_patch_accepts_parent_object_schema(self):
        """Realistic end-to-end: the parent object __schema__ built by the assistant
        is accepted by the Agent's _editable_patch merge (preserving unedited)."""
        field = {
            "label": "维保通告填写", "type": "object", "path": "patch", "native_notice": True,
            "value": {"title": "旧标题", "building_codes": ["A"], "specialty": "hv"},
            "children": [{"path": "title", "label": "标题", "type": "text", "required": True},
                         {"path": "building_codes", "label": "楼栋/范围", "type": "multiselect",
                          "options": [{"value": "A", "label": "A栋"}, {"value": "B", "label": "B栋"}]},
                         {"path": "specialty", "label": "专业", "type": "select",
                          "options": [{"value": "hv", "label": "暖通"}, {"value": "el", "label": "电气"}]}],
        }
        old = {"title": "旧标题", "building_codes": ["A"], "specialty": "hv"}
        built = notice_forms.build_native_notice_value(field, {"title": "新标题"})
        # preserves unedited children
        self.assertEqual(built["building_codes"], ["A"])
        self.assertEqual(built["specialty"], "hv")
        merged = _editable_patch(field, old, built)
        self.assertEqual(merged["title"], "新标题")
        self.assertEqual(merged["building_codes"], ["A"])
        self.assertEqual(merged["specialty"], "hv")
        # hidden/read-only injection is rejected by the Agent as well
        with self.assertRaises(AssistantError):
            _editable_patch(field, old, {**built, "not_a_child": "x"})

    def test_real_selection_field_scopes_are_strings_and_display_resolves(self):
        # Real contract: selection_field returns scopes as a list of strings and
        # the enforcement engine (selection_payload) accepts our built payload.
        actor = {"scopes": ["A", "E"]}
        field = selection_field(actor, {"action": "start", "work_type": "maintenance", "scope": "A,E"}, {}, 0)
        self.assertIsNotNone(field)
        self.assertEqual(field["name"], "step0.notice_sop")
        self.assertEqual(sorted(field["scopes"]), ["A", "E"])
        sop_row = {"sop_id": "sop-maint", "name": "维保SOP", "scope": "A", "work_type": "maintenance", "version": 3,
                   "ready": True, "steps": [{"content": "完成维保作业"}], "attachments": [{"name": "附件"}]}
        people = [{"record_id": "p-op", "name": "操作人甲", "source": "staff", "open_id": "ou_op"},
                  {"record_id": "p-rv", "name": "审核人乙", "source": "staff", "open_id": "ou_rv"}]
        update_directory(field, "A", [sop_row], people)
        self.assertEqual(field["directory_scope"], "A")
        self.assertEqual(field["sops"][0]["name"], "维保SOP")
        text = "\n".join(notice_forms.notice_sop_display(field, status="needs_input"))
        self.assertIn(".不使用工单：否", text)
        built = notice_forms.build_notice_sop_value(field, {
            "exempt": "否", "scope": "A", "sop_id": "维保SOP",
            "operator_record_id": "操作人甲", "reviewer_record_id": "审核人乙"})
        self.assertFalse(built["exempt"])
        self.assertEqual(built["sop_id"], "sop-maint")
        selection, labels = selection_payload(field, built, actor, {"A"})
        self.assertFalse(selection["polling_work_order_exempt"])
        self.assertEqual(selection["polling_sop_id"], "sop-maint")
        self.assertEqual(selection["polling_runs"], [{"label": "维保作业"}])
        self.assertEqual(selection["polling_operator_record_id"], "p-op")
        self.assertEqual(selection["polling_reviewer_record_id"], "p-rv")

    def test_real_sop_same_message_new_sop_mode_governs_runs(self):
        # Fix #4: when the same message selects a new SOP and supplies runs, the
        # runs must be parsed against the NEW SOP's mode (here cooling) instead of
        # the previously stored one (normal, which would reject runs entirely).
        actor = {"scopes": ["A"]}
        field = selection_field(actor, {"action": "start", "work_type": "adjust", "scope": "A"}, {}, 0)
        sops = [
            {"sop_id": "sop-adj", "name": "旧普通调整SOP", "scope": "A", "work_type": "adjust", "ready": True,
             "steps": [{"content": "完成调整作业"}], "attachments": [{"name": "a"}]},
            {"sop_id": "sop-cool", "name": "新制冷SOP", "scope": "A", "work_type": "adjust", "ready": True,
             "steps": [{"content": "将空调 {{from}} 切换到制热"}], "attachments": [{"name": "a"}]},
        ]
        update_directory(field, "A", sops, [])
        field["_initial_form"] = {"exempt": False, "scope": "A", "sop_id": "sop-adj",
                                  "operator_record_id": "", "reviewer_record_id": "", "runs": []}
        field["value"] = {"$query": {"ref": "q", "path": []}, "exempt": False, "scope": "A",
                          "sop_id": "sop-adj", "operator_record_id": "", "reviewer_record_id": "", "runs": []}
        built = notice_forms.build_notice_sop_value(field, {"sop_id": "新制冷SOP", "runs": "制冷→制热／1#"})
        self.assertEqual(built["sop_id"], "sop-cool")
        self.assertEqual(built["runs"], [{"from_unit": "制冷", "to_unit": "制热", "other_unit": "1#"}])

    async def test_awaiting_confirmation_view_options_returns_to_needs_input_first(self):
        # Fix #6: the real PortalAgent.field_options only works for needs_input;
        # the assistant must first amend(action="edit") to return to needs_input.
        plan = {"id": "P1", "title": "维保变更设备调整", "status": "awaiting_confirmation", "version": 6,
                "fields": [field("step0.target", "目标设备", "select", options_source="device")],
                "operations": [], "explanation": ""}
        needs = needs_plan(version=7, fields=[field("step0.target", "目标设备", "select", options_source="device")])
        updated = needs_plan(version=8)
        updated["fields"] = [field("step0.target", "目标设备", "select", options_source="device",
                                   options=[{"value": "dv-1", "label": "1号空调"}])]
        result, calls = await self.answer("查看选项 step0.target", [plan], needs, updated)
        self.assertIn("1号空调", result["text"])
        self.assertEqual(calls.await_count, 3)  # conversation + edit + options
        edit_call = calls.await_args_list[1]
        self.assertEqual(edit_call.args[2], "plans/P1")
        self.assertEqual(edit_call.args[3], {"version": 6, "action": "edit"})
        option_call = calls.await_args_list[2]
        self.assertEqual(option_call.args[2], "plans/P1/options")
        self.assertEqual(option_call.kwargs["params"]["field"], "step0.target")

    def test_sop_leaf_ambiguous_across_fields_requires_qualification(self):
        # Fix #2: a bare same-name leaf must never silently pick the first field.
        fields = [notice_sop_field(name="step0.notice_sop"), notice_sop_field(name="step1.notice_sop")]
        with self.assertRaises(AssistantError):
            notice_forms.resolve_notice_leaf(fields, "SOP")
        target, child = notice_forms.resolve_notice_leaf(fields, "step1.notice_sop.SOP")
        self.assertTrue(notice_forms.is_native_notice_sop(target))
        self.assertEqual(target["name"], "step1.notice_sop")
        self.assertEqual(child["key"], "sop_id")

    async def test_exempt_reverse_alias_removed_not_accepted(self):
        # Fix #3: "使用工单" is not a recognized leaf; only explicit
        # "不使用工单：是" can set exemption, so no implicit exemption exists.
        plan = needs_plan(fields=[notice_sop_field()])
        result, calls = await self.answer("step0.notice_sop.使用工单：是", [plan])
        self.assertIn("未识别字段", result["text"])
        self.assertEqual(calls.await_count, 1)  # no PATCH

    async def test_parent_field_hidden_or_only_read_chinese_protected(self):
        # Fix #5: parent field hidden / 只读 must be protected like readonly.
        plan = needs_plan(fields=[field("step0.fixed", "固定字段", "text", hidden=True)])
        result, calls = await self.answer("step0.fixed：xxx", [plan])
        self.assertIn("只读字段", result["text"])
        self.assertEqual(calls.await_count, 1)
        plan2 = needs_plan(fields=[field("step0.fixed2", "固定字段2", "text", **{"只读": True})])
        result2, calls2 = await self.answer("step0.fixed2：yyy", [plan2])
        self.assertIn("只读字段", result2["text"])
        self.assertEqual(calls2.await_count, 1)
        self.assertTrue(notice_forms.child_protected({"hidden": True}))
        self.assertTrue(notice_forms.child_protected({"只读": True}))

    # ----- Defect #1: a protected parent field must block ALL leaf edits -----
    async def test_native_notice_protected_parent_blocks_leaf_edit(self):
        patch = notice_patch_field(children=[{"path": "内容", "label": "内容", "type": "text"},
                                             {"path": "标题", "label": "标题", "type": "text"}])
        patch["read_only"] = True
        plan = needs_plan(fields=[patch])
        # The child 标题 itself is writable, but the parent notice field may not
        # be edited through leaf lines.
        result, calls = await self.answer("step0.patch.标题：xxx", [plan])
        self.assertIn("只读", result["text"])
        self.assertEqual(calls.await_count, 1)  # conversation only, no PATCH

    async def test_native_sop_protected_parent_blocks_leaf_edit(self):
        sop = notice_sop_field(scopes=["A"])
        sop["read_only"] = True
        plan = needs_plan(fields=[sop])
        result, calls = await self.answer("step0.notice_sop.楼栋：A", [plan])
        self.assertIn("只读", result["text"])
        self.assertEqual(calls.await_count, 1)

    # ----- Defect #2: SOP scope accepts the 中文候选标签 labels (within scope) -----
    async def test_notice_sop_scope_accepts_chinese_labels_within_candidates(self):
        sop = notice_sop_field(scopes=["A", "E", "110"],
                               sops=[{"sop_id": "s1", "name": "SOP1", "mode": "normal"}],
                               people=[{"record_id": "p1", "name": "张三"}])
        plan = needs_plan(fields=[sop])
        result, calls = await self.answer("step0.notice_sop.楼栋：E楼", [plan],
                                          {"id": "P1", "status": "awaiting_confirmation",
                                           "version": 4, "operations": [], "title": "维保变更设备调整", "explanation": ""})
        self.assertIn("已按你的填写更新操作计划", result["text"])
        payload = calls.await_args.args[3]["values"]["step0.notice_sop"]
        self.assertEqual(payload["scope"], "E")

    def test_notice_sop_scope_label_reverse_lookup_and_reject(self):
        from openclaw_service.assistant.lighthouse_ai import AssistantError
        field = notice_sop_field(scopes=["A", "E", "110"])
        self.assertEqual(notice_forms.build_notice_sop_value(field, {"scope": "E楼"})["scope"], "E")
        self.assertEqual(notice_forms.build_notice_sop_value(field, {"scope": "110站"})["scope"], "110")
        with self.assertRaises(AssistantError):
            notice_forms.build_notice_sop_value(field, {"scope": "Z楼"})

    # ----- Defect #3: switching SOP type must never silently reuse old runs -----
    def test_sop_type_change_without_runs_prompts_for_device_pointing(self):
        from openclaw_service.assistant.lighthouse_ai import AssistantError
        field = notice_sop_field(
            scopes=["A"],
            sops=[{"sop_id": "old-normal", "name": "旧普通SOP", "mode": "normal"},
                  {"sop_id": "new-poll", "name": "新轮巡SOP", "mode": "polling"}],
            value={"exempt": False, "scope": "A", "sop_id": "old-normal",
                   "operator_record_id": "", "reviewer_record_id": "",
                   "runs": [{"from_unit": "1#", "to_unit": "2#"}]})
        with self.assertRaises(AssistantError) as ctx:
            notice_forms.build_notice_sop_value(field, {"sop_id": "新轮巡SOP"})
        self.assertIn("已更换SOP", str(ctx.exception))
        self.assertIn("运行", str(ctx.exception))

    def test_sop_type_change_to_normal_clears_stale_runs(self):
        field = notice_sop_field(
            scopes=["A"],
            sops=[{"sop_id": "old-poll", "name": "旧轮巡SOP", "mode": "polling"},
                  {"sop_id": "new-norm", "name": "新普通SOP", "mode": "normal"}],
            value={"exempt": False, "scope": "A", "sop_id": "old-poll",
                   "operator_record_id": "", "reviewer_record_id": "",
                   "runs": [{"from_unit": "1#", "to_unit": "2#"}]})
        built = notice_forms.build_notice_sop_value(field, {"sop_id": "新普通SOP"})
        # normal-mode SOP regenerates its own run label, so old runs are cleared.
        self.assertEqual(built["runs"], [])

    # ----- Defect #4: an awaiting-confirmation typo must not issue action=edit PATCH -----
    async def test_awaiting_confirmation_unknown_field_does_not_issue_edit(self):
        plan = {"id": "P1", "title": "维保变更设备调整", "status": "awaiting_confirmation", "version": 6,
                "fields": [field("step0.device_name", "设备名称", "text")],
                "operations": [], "explanation": ""}
        result, calls = await self.answer("step0.device_name：x\n错误字段：y", [plan])
        self.assertIn("未识别字段", result["text"])
        # conversation call only: no NEEDS_INPUT transition, no action=edit PATCH.
        self.assertEqual(calls.await_count, 1)


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
