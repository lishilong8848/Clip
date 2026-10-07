# -*- coding: utf-8 -*-
"""PlanRuleEditor: local draft editor + native plan PUT ruleset workflow.

Composes the PlanWorkflowTests fixture (isolated SQLite) and exercises the
admin prepare of ``PUT /api/plan-convergence/rulesets/{id}`` against a frozen
``rules.get_set`` snapshot: one ``native_plan_rules`` object editor,
identity-keyed item edits (reorder/delete preserve unedited fields by database
id), and rejection guards that run before any create/write.  Malformed text
values are rejected by the shared text-type guard before a create runs.
"""
import asyncio
import copy
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import test_lighthouse_plan_workflows as _plan_workflows
from lan_bitable_template_portal import plan_convergence_rules as rules
from lan_bitable_template_portal.lighthouse_ai import AssistantError

ACTOR = _plan_workflows.ACTOR


class PlanRuleEditorTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.fixture = _plan_workflows.PlanWorkflowTests()
        self.fixture.setUp()
        # IsolatedAsyncioTestCase cleanup runs through _asyncioRunner which is
        # None on an un-started composed fixture; transfer its registered
        # callbacks to this (live) parent test and stop the nested list.
        for callback, args, kwargs in self.fixture._cleanups:
            self.addCleanup(callback, *args, **kwargs)
        self.fixture._cleanups.clear()
        rules.save_set(self.fixture.set_id, "冷机测试规则集", "原备注", [
            {"scope_type": "device", "inst_name": "冷机一号", "rule_group_no": 1,
             "rule_type": "normal", "rule_label": "原标签甲"},
            {"scope_type": "point", "inst_name": "冷机二号", "rule_group_no": 2,
             "rule_type": "common", "rule_label": "原标签乙"},
            {"scope_type": "room", "building": "A", "floor": "F1", "room": "A-F1-1",
             "rule_group_no": 3, "rule_type": "normal", "rule_label": "原标签丙"},
        ])

    def _prepare_put(self, queries=None, actor=ACTOR, path_id=None):
        if path_id is None:
            path_id = {"id": str(self.fixture.set_id)}
        op = {"api_id": "PUT /api/plan-convergence/rulesets/{id}", "path_params": path_id}
        return self.fixture.prepare(op, queries=queries, actor=actor)

    def _editor(self, plan, index=None):
        editors = [f for f in plan["fields"] if f.get("native_plan_rules")]
        self.assertTrue(editors, "缺少 native_plan_rules 编辑器")
        if index is not None:
            editors = [f for f in editors if f.get("operation_index") == index]
            self.assertTrue(editors, f"步骤 {index} 缺少 native_plan_rules 编辑器")
        self.assertEqual(len(editors), 1, "应只有一个 native_plan_rules 编辑器")
        return editors[0]

    def _create_configure_plan(self):
        return self.fixture.agent.prepare(ACTOR, {
            "operations": [
                {"api_id": "POST /api/plan-convergence/rulesets"},
                {"api_id": "PUT /api/plan-convergence/rulesets/{id}",
                 "path_params": {"id": {"$result": {"step": 0, "path": "id"}}}},
            ]
        }, "plan-fixture-turn-00001", [], queries={})

    def test_put_editor_carries_full_snapshot_without_raw_id(self):
        original = rules.get_set(self.fixture.set_id)
        plan = self._prepare_put(queries={"original": original})
        self.assertEqual(plan["risk"], "high")
        field = self._editor(plan, index=0)
        self.assertEqual(field["path"], "")
        self.assertEqual(field["section"], "body")
        self.assertEqual(field["type"], "object")
        self.assertFalse(any(f["path"] == "id" for f in plan["fields"]))
        self.assertFalse(any(f.get("options_source") == "plan_rulesets" for f in plan["fields"]))
        self.assertIn("$query", field["value"])
        self.assertEqual(set(field["value"]) - {"$query"}, {"name", "remark", "items", "expected_version"})
        self.assertEqual(field['value']['expected_version'], original['version'])
        self.assertNotIn('expected_version', {child['path'] for child in field['children']})
        self.assertEqual(field["value"]["name"], original["name"])
        self.assertEqual(field["value"]["remark"], original["remark"])
        self.assertEqual(len(field["value"]["items"]), len(original["items"]))

    async def test_amend_reorder_delete_preserves_unedited_fields_by_database_id(self):
        original = rules.get_set(self.fixture.set_id)
        plan = self._prepare_put(queries={"original": original})
        field = self._editor(plan, index=0)
        value = copy.deepcopy(field["value"])
        value["name"] = "重排删除后的规则集"
        value["remark"] = "仅保留两条并按新顺序"
        item_c, item_a, item_b = original["items"][2], original["items"][0], original["items"][1]
        # Reorder [room, device] and drop the point row.  Only id + edited fields
        # are re-submitted (group_no reassigned like the native UI); unedited
        # fields must come from the row matched by database id, not by position.
        value["items"] = [
            {"id": item_c["id"], "scope_type": "room", "rule_group_no": 1, "rule_label": "新标签丙"},
            {"id": item_a["id"], "scope_type": "device", "rule_group_no": 2, "rule_label": "原标签甲"},
        ]

        done = await self.fixture.fill_and_run(plan, {field["name"]: value})

        body = done["operations"][0]["body"]
        self.assertEqual(len(body["items"]), 2)
        # Order matches the resubmitted order (room first, device second).
        room = body["items"][0]
        self.assertEqual(room["id"], original["items"][2]["id"])
        self.assertEqual(room["rule_group_no"], 1)
        self.assertEqual(room["building"], "A")
        self.assertEqual(room["floor"], "F1")
        self.assertEqual(room["room"], "A-F1-1")
        self.assertEqual(room["rule_label"], "新标签丙")
        self.assertEqual(room["rule_type"], "normal")
        device = body["items"][1]
        self.assertEqual(device["id"], original["items"][0]["id"])
        self.assertEqual(device["rule_group_no"], 2)
        self.assertEqual(device["inst_name"], "冷机一号")
        self.assertEqual(device["rule_label"], "原标签甲")
        self.assertEqual(device["rule_type"], "normal")
        # The deleted point row is gone.
        self.assertNotIn(item_b["id"], {row["id"] for row in body["items"]})
        self.assertNotIn("冷机二号", [row.get("inst_name") for row in body["items"]])

        saved = rules.get_set(self.fixture.set_id)
        self.assertEqual(saved["name"], "重排删除后的规则集")
        self.assertEqual(len(saved["items"]), 2)
        saved_by_group = {row["rule_group_no"]: row for row in saved["items"]}
        self.assertEqual(saved_by_group[1]["building"], "A")
        self.assertEqual(saved_by_group[1]["room"], "A-F1-1")
        self.assertEqual(saved_by_group[1]["rule_label"], "新标签丙")
        self.assertEqual(saved_by_group[2]["inst_name"], "冷机一号")
        self.assertEqual(saved_by_group[2]["rule_label"], "原标签甲")
        self.assertTrue(done["results"][0]["ok"], done["results"][0])

    def test_nonadmin_and_missing_snapshot_are_rejected(self):
        op = {"api_id": "PUT /api/plan-convergence/rulesets/{id}",
              "path_params": {"id": str(self.fixture.set_id)}}
        with self.assertRaisesRegex(AssistantError, "仅管理员"):
            self.fixture.prepare(op, actor={**ACTOR, "is_admin": False})
        with self.assertRaisesRegex(AssistantError, "先读取"):
            self.fixture.prepare(op)

    async def test_malformed_unknown_and_over_500_items_rejected_before_write(self):
        before = rules.get_set(self.fixture.set_id)
        plan = self._prepare_put(queries={"original": before})
        field = self._editor(plan, index=0)

        def amend(value):
            return self.fixture.agent.amend(
                ACTOR, plan["id"],
                {"version": plan["version"], "values": {field["name"]: value}})

        too_many = copy.deepcopy(field["value"])
        too_many["items"] = [
            {"id": index, "scope_type": "device", "inst_name": "设备" + str(index),
             "rule_group_no": 1} for index in range(501)]
        with self.assertRaises(AssistantError):
            amend(too_many)
        self.assertEqual(rules.get_set(self.fixture.set_id), before)

        unknown = copy.deepcopy(field["value"])
        unknown["mystery_field"] = True
        with self.assertRaises(AssistantError):
            amend(unknown)
        self.assertEqual(rules.get_set(self.fixture.set_id), before)

        malformed = copy.deepcopy(field["value"])
        malformed["items"] = "not-a-list"
        with self.assertRaises(AssistantError):
            amend(malformed)
        self.assertEqual(rules.get_set(self.fixture.set_id), before)

    async def test_create_then_configure_malformed_text_rejected_before_any_create(self):
        before = rules.list_sets()
        plan = self._create_configure_plan()
        field = self._editor(plan, index=1)
        bad = {"name": "坏文本规则集", "remark": "inst_name 应为字符串",
               "items": [{"scope_type": "device", "inst_name": {"wrong": "type"},
                          "rule_group_no": 1, "rule_type": "normal"}]}
        with self.assertRaisesRegex(AssistantError, "有效文本"):
            self.fixture.agent.amend(ACTOR, plan["id"],
                {"version": plan["version"], "values": {field["name"]: bad}})
        self.assertEqual(rules.list_sets(), before)
        self.assertFalse(any(row["name"] == "坏文本规则集" for row in rules.list_sets()))

    async def test_create_then_configure_no_write_before_second_confirmation(self):
        plan = self._create_configure_plan()
        field = self._editor(plan, index=1)
        value = {"name": "延迟写入规则集", "remark": "二次确认后才创建",
                 "items": [{"scope_type": "device", "inst_name": "冷机一号",
                            "rule_group_no": 1, "rule_type": "normal"}]}
        before = rules.list_sets()

        amended = self.fixture.agent.amend(ACTOR, plan["id"],
            {"version": plan["version"], "values": {field["name"]: value}})
        reviewed = await self.fixture.agent.confirm(ACTOR, plan["id"],
            {"version": amended["version"], "stage": "review"}, self.fixture.request)
        self.assertEqual(reviewed["status"], "awaiting_second_confirmation")
        self.assertEqual(rules.list_sets(), before)
        self.assertNotIn("延迟写入规则集", {row["name"] for row in rules.list_sets()})

        await self.fixture.agent.confirm(ACTOR, plan["id"],
            {"version": reviewed["version"], "stage": "execute"}, self.fixture.request)
        if self.fixture.agent.tasks:
            await asyncio.gather(*tuple(self.fixture.agent.tasks))
        created = next((row for row in rules.list_sets() if row["name"] == "延迟写入规则集"), None)
        self.assertIsNotNone(created)

    async def test_create_then_configure_resolves_id_and_copies_name(self):
        plan = self._create_configure_plan()
        self.assertEqual([f.get("operation_index") for f in plan["fields"]], [1])
        field = self._editor(plan, index=1)
        self.assertEqual(field["operation_index"], 1)

        value = {"name": "新建配置规则集", "remark": "创建后配置",
                 "items": [{"scope_type": "device", "inst_name": "冷机一号",
                            "rule_group_no": 1, "rule_type": "normal"}]}
        done = await self.fixture.fill_and_run(plan, {field["name"]: value})
        self.assertEqual(done["operations"][0]["body"]["name"], "新建配置规则集")
        self.assertEqual(done["operations"][0]["body"]["remark"], "创建后配置")
        created_id = done["results"][0]["_raw"]["id"]
        saved = rules.get_set(created_id)
        self.assertEqual(saved["name"], "新建配置规则集")
        self.assertEqual(saved["remark"], "创建后配置")
        self.assertEqual([item["inst_name"] for item in saved["items"]], ["冷机一号"])
        self.assertTrue(done["results"][1]["ok"], done["results"][1])
        self.assertEqual(done["results"][1]["_raw"]["id"], created_id)
        self.assertIn(created_id, {row["id"] for row in rules.list_sets()})

    def test_forward_result_links_are_strict(self):
        def prepare_with(link, step=0):
            return self.fixture.agent.prepare(ACTOR, {
                "operations": [
                    {"api_id": "POST /api/plan-convergence/rulesets"},
                    {"api_id": "PUT /api/plan-convergence/rulesets/{id}",
                     "path_params": {"id": {"$result": {"step": step, "path": link}}}},
                ]
            }, "plan-fixture-turn-00001", [], queries={})

        with self.assertRaises(AssistantError):
            prepare_with("wrong")
        with self.assertRaises(AssistantError):
            prepare_with("id", step=99)


if __name__ == "__main__":
    unittest.main()
