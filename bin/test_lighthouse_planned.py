import asyncio
import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fastapi import FastAPI, Request
from clipflow_backend.api_models import WorkbenchActionRequest
from lan_bitable_template_portal.state_store import LanPortalStateStore
from openclaw_service.assistant.lighthouse_ai import LighthouseAssistant, AssistantError
from openclaw_service.assistant.lighthouse_agent import PortalAgent
from openclaw_service.assistant.lighthouse_api import PortalAPICatalog
from openclaw_service.assistant.lighthouse_files import LighthouseFiles
from openclaw_service.assistant import lighthouse_planned as workflow
from test_lighthouse_notice_workflows import VALID_STARTS
from test_lighthouse_notice_binding import make_request


class PlannedAssistantTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = LanPortalStateStore(Path(self.tmp.name) / "state.sqlite3")
        self.model = Mock()
        self.model.settings.return_value = {"configured": True, "enabled": True, "models": []}
        self.model.profile.return_value = {"id": "fixture", "name": "fixture", "model": "fixture"}
        self.model.complete.side_effect = AssistantError("模型暂不可用")
        self.assistant = LighthouseAssistant(self.store, Mock(side_effect=AssertionError("No cloud")), model=self.model)
        self.actor = {"id": "account-A", "scopes": list("ABCDEH"), "home_scope": "A", "is_admin": False}
        self.request = make_request()
        self.turn = {"operation_id": "planned-turn-0000001"}
        self.rows = [self.row()]
        self.reads, self.writes = [], []
        self.complete = True
        self.app = app = FastAPI()

        @app.get("/api/workbench/source-options")
        async def options(request: Request):
            scope = request.query_params.get("scope")
            month = request.query_params.get("month")
            work_type = request.query_params.get("work_type")
            planned_match = request.query_params.get("planned_match")
            page = int(request.query_params.get("page") or 1)
            size = int(request.query_params.get("page_size") or 500)
            self.reads.append(dict(request.query_params))
            start = (page - 1) * size
            return {"ok": True, "data": {"items": self.rows[start:start + size], "total": len(self.rows),
                "page": page, "has_more": start + size < len(self.rows), "complete": self.complete}}

        @app.get("/api/workbench/planned-notice-prefill")
        async def prefill(request: Request):
            scope = request.query_params.get("scope")
            month = request.query_params.get("month")
            kind = request.query_params.get("work_type")
            identity = request.query_params.get("source_record_id")
            row = next(row for row in self.rows if row["source_record_id"] == identity)
            draft = {**VALID_STARTS[kind], "title": row["title"], "building_codes": [scope], "progress": ""}
            return {"ok": True, "data": {"draft": draft, "version": "v1", "field_sources": {"content": "history"},
                "missing_fields": ["progress"] if kind != "adjust" else [], "selected": row,
                "source_record": {"source_record_id": identity, "record_id": identity, "work_type": kind,
                                  "building_codes": [scope], "title": row["title"]}}}

        @app.post("/api/workbench-actions")
        async def send(body: WorkbenchActionRequest):
            self.writes.append(body.model_dump())
            return {"ok": True, "data": {"record_id": "fixture-target"}}

        self.agent = PortalAgent(self.assistant, PortalAPICatalog(app), LighthouseFiles(self.store))

    def row(self, identity="source-1", title="冷水机组月度维护", kind="maintenance"):
        return {"source_record_id": identity, "title": title, "work_type": kind, "building": "A楼", "progress": "未开始"}

    async def begin(self, question="发送冷水机组月度维护", actor=None):
        return await workflow.begin(self.agent, actor or self.actor, self.turn, question, self.request)

    async def amend(self, plan, values=None, **kwargs):
        return await workflow.amend_selection(self.agent, self.actor, plan["id"],
            {"version": plan["version"], **({"values": values or {}} if not kwargs.get("action") else {}), **kwargs}, self.request)

    def fill(self, plan):
        saved = self.agent.get_plan(self.actor, plan["id"])
        values = {}
        for field in saved["fields"]:
            value = copy.deepcopy(field.get("_initial_form", field.get("value", "")))
            if field.get("native_notice"):
                value["progress"] = "本次现场已核对"
            if field.get("native_notice_sop"):
                value["exempt"] = True
            values[field["name"]] = value
        return values

    async def test_home_building_not_view_scope_and_no_write_before_confirm(self):
        plan = await self.begin()
        self.assertEqual(plan["planned_notice"]["stage"], "preview")
        self.assertTrue(all(item["scope"] == "A" for item in self.reads))
        self.assertFalse(any(field.get("native_notice_binding") for field in plan["fields"]))
        self.assertEqual(self.writes, [])
        self.model.complete.assert_not_called()

    async def test_admin_asks_even_page_default_scope_and_uses_current_explicit_scope(self):
        admin = {**self.actor, "is_admin": True, "scopes": ["E"], "allowed_scopes": list("ABCDEH")}
        plan = await self.begin(actor=admin)
        self.assertEqual(plan["planned_notice"]["stage"], "scope")
        self.assertEqual(len(plan["fields"][0]["options"]), 6)
        self.assertEqual(self.reads, [])
        plan = await self.begin("发送A楼冷水机组月度维护", actor=admin)
        self.assertEqual(plan["planned_notice"]["scope"], "A")

    async def test_h_duty_only_uses_home(self):
        plan = await self.begin(actor={**self.actor, "home_scope": "H"})
        self.assertEqual(plan["planned_notice"]["scope"], "H")
        with self.assertRaises(AssistantError):
            await self.begin("发送E楼冷水机组月度维护")

    async def test_unknown_home_asks(self):
        plan = await self.begin(actor={**self.actor, "home_scope": ""})
        self.assertEqual(plan["planned_notice"]["stage"], "scope")

    async def test_duplicate_choice_not_silently_auto_selected(self):
        self.rows.append(self.row("source-2"))
        plan = await self.begin()
        self.assertEqual(plan["planned_notice"]["stage"], "candidate")
        self.assertEqual(len(plan["fields"][0]["options"]), 2)
        selected = await self.amend(plan, {"planned.source": "source-2"})
        self.assertEqual(selected["planned_notice"]["stage"], "preview")
        self.assertEqual(self.writes, [])

    async def test_no_match_asks_name_not_independent(self):
        plan = await self.begin("消防泵季度维护")
        self.assertEqual(plan["fields"][0]["path"], "query")
        self.assertEqual(plan["operations"], [])

    async def test_failed_source_not_reported_as_no_match(self):
        self.complete = False
        with self.assertRaisesRegex(AssistantError, "未读取完整"):
            await self.begin()

    async def test_only_native_write_after_form_and_confirmation(self):
        plan = await self.begin()
        plan = await self.amend(plan, self.fill(plan))
        self.assertEqual(plan["status"], "awaiting_confirmation")
        self.assertIn("本次现场已核对", plan["planned_notice"]["text"])
        self.assertEqual(self.writes, [])
        plan = await self.agent.confirm(self.actor, plan["id"], {"version": plan["version"], "stage": "review"}, self.request)
        if plan["status"] == "awaiting_second_confirmation":
            plan = await self.agent.confirm(self.actor, plan["id"], {"version": plan["version"], "stage": "execute"}, self.request)
        if self.agent.tasks:
            await asyncio.gather(*tuple(self.agent.tasks))
        self.assertEqual(len(self.writes), 1)
        self.assertEqual(self.writes[0]["source_record_id"], "source-1")
        self.assertEqual(self.writes[0]["planned_notice_version"], "v1")
        self.assertFalse(self.writes[0].get("manual"))

    async def test_all_non_event_type_forms(self):
        names = {"maintenance": "冷水机组月度维护", "change": "冷水机组软件变更", "repair": "冷水机组检修",
                 "polling": "冷水机组轮巡", "adjust": "冷水机组设备调整", "power": "机柜上电"}
        for kind, title in names.items():
            with self.subTest(kind=kind):
                self.rows = [self.row(kind, title, kind)]
                plan = await self.begin(title)
                self.assertEqual(plan["planned_notice"]["stage"], "preview")
                field = next(field for field in plan["fields"] if field.get("native_notice"))
                self.assertEqual(field["value"]["progress"], "")
                if kind == "adjust":
                    self.assertFalse(next(child for child in field["children"] if child["path"] == "progress")["required"])
        self.assertIsNone(await self.begin("发送事件通告"))

    async def test_document_read_and_admin_requests_do_not_start_notice_matching(self):
        for question in ('生成A楼维护单', '读取A楼维护单的附件，预览后准备填写',
                         '下载A楼演练文件，并准备作为本地维护单上传', '上传已签名的维护单',
                         '在维护单中使用我的签名', '维护单MOP配置', '导入通告记忆',
                         '扫描通告记忆', '管理通告历史记忆', '读取A楼维保通告'):
            with self.subTest(question=question):
                self.assertEqual(workflow.name_request(question), '')
                self.assertEqual(workflow.name_request(question, allow_plain=True), '')
                self.assertIsNone(await self.begin(question))
        self.assertEqual(self.reads, [])
        self.assertEqual(self.writes, [])

    async def test_edits_survive_review_and_cancel_invalidates_preview(self):
        plan = await self.begin()
        values = self.fill(plan)
        values["step0.patch"]["content"] = "手工修改"
        plan = await self.amend(plan, values)
        plan = await self.amend(plan, action="edit")
        field = next(field for field in plan["fields"] if field.get("native_notice"))
        self.assertEqual(field["value"]["content"], "手工修改")
        self.agent.cancel(self.actor, plan["id"])
        with self.assertRaises(AssistantError):
            await self.amend(plan, self.fill(plan))
        self.assertEqual(self.writes, [])

    async def test_switch_requires_reset_and_clears_form(self):
        plan = await self.begin()
        with self.assertRaisesRegex(AssistantError, "先确认重置"):
            await self.amend(plan, action="planned-reset")
        reset = await self.amend(plan, action="planned-reset", reset_confirmed=True)
        self.assertEqual(reset["operations"], [])
        self.assertNotIn("selected", reset["planned_notice"])
        self.assertEqual(reset["fields"][0]["path"], "query")

    async def test_foreign_choice_rejected(self):
        self.rows.append(self.row("source-2"))
        plan = await self.begin()
        with self.assertRaises(AssistantError):
            await self.amend(plan, {"planned.source": "not-recalled"})

    async def test_scope_can_be_answered_in_chat_without_another_business_write(self):
        admin = {**self.actor, "is_admin": True}
        plan = await self.begin(actor=admin)
        state = self.assistant._state(admin)
        state["turns"] = [{"operation_id": self.turn["operation_id"], "question": "发送冷水机组月度维护", "status": "completed", "plan": plan}]
        self.store.put_document("lighthouse_ai", self.assistant._key(admin), state)
        result = await workflow.continue_selection(self.agent, admin, "A楼", self.request)
        self.assertIn("已更新", result)
        saved = self.agent.get_plan(admin, plan["id"])
        self.assertEqual(saved["_planned"]["stage"], "preview")
        self.assertEqual(self.writes, [])

    async def test_clear_conversation_invalidates_unsubmitted_selection(self):
        plan = await self.begin()
        state = self.assistant._state(self.actor)
        state["id"] = "a-new-conversation"
        state["turns"] = []
        self.store.put_document("lighthouse_ai", self.assistant._key(self.actor), state)
        with self.assertRaises(AssistantError):
            self.agent.get_plan(self.actor, plan["id"])


if __name__ == "__main__":
    unittest.main()
