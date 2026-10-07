"""Native approval requests through assistant controls, without sending real messages."""
import asyncio
import copy
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fastapi import FastAPI, Request
from clipflow_backend.api_models import SignatureUsageConfirmationSendRequest
from lan_bitable_template_portal.lighthouse_agent import PortalAgent, _SIGNATURE_USAGE, _result_refs
from lan_bitable_template_portal.lighthouse_ai import LighthouseAssistant, AssistantError
from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog
from lan_bitable_template_portal.lighthouse_files import LighthouseFiles
from lan_bitable_template_portal.portal_service import MaintenancePortalService
from lan_bitable_template_portal.state_store import LanPortalStateStore
from test_lighthouse_stream import Store

ACTOR = {"id": "ou_operator", "scopes": ["A"], "is_admin": False}
PERSON = {"source": "staff", "record_id": "person-a", "name": "签名人员甲", "employee_no": "1001", "building": "A楼",
          "has_signature": True, "usage_confirmed": False, "can_receive_message": True, "open_id": "ou_person", "image_base64": "NO-IMAGE-IN-CHAT"}
MOP = {"sheets": [], "mop_record_id": "mop-a", "mop_file_name": "维护单.xlsx", "attachment": {"file_token": "private-file-token", "name": "维护单.xlsx"}}
NOTICE = {"notice_key": "notice-a", "title": "A楼维护", "building_codes": ["A"]}
TASK = {"task_id": "guard-a", "task_name": "节日重保", "responses": [{"response_id": "response-a", "scope": "A", "sheet_type": "设备安全", "version": 1, "cells": {}}]}


class SignatureUsageWorkflowTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.store = Store(root / "assistant.sqlite3")
        self.assistant = LighthouseAssistant(self.store, Mock(), model=Mock())
        self.service = object.__new__(MaintenancePortalService)
        self.service._state_store = LanPortalStateStore(str(root / "native.sqlite3"))
        self.people = [PERSON, {**PERSON, "record_id": "self", "open_id": ACTOR["id"]},
                       {**PERSON, "record_id": "no-image", "has_signature": False},
                       {**PERSON, "record_id": "approved", "usage_confirmed": True},
                       {**PERSON, "record_id": "external", "source": "external"},
                       {**PERSON, "record_id": "no-recipient", "open_id": ""}]
        self.service._load_signature_people = Mock(side_effect=lambda **_: copy.deepcopy(self.people))
        self.service._signature_public_base_url = Mock(return_value="http://testserver")
        self.service.signature_image_bytes = Mock(side_effect=AssertionError("No signature images may be read"))
        self.app, self.reads, self.writes, self.messages = FastAPI(), [], [], []
        self.fail_send = False

        @self.app.get("/api/signatures/people")
        async def people(scope: str, notice_key: str, q: str = "", limit: int = 100):
            self.reads.append((scope, notice_key))
            return {"ok": True, "data": {"people": self.people, "count": len(self.people)}}

        @self.app.post("/api/signatures/usage-confirmations/send")
        async def send(payload: SignatureUsageConfirmationSendRequest):
            self.writes.append(payload.model_dump())
            data = self.service.build_signature_usage_confirmation_messages(**payload.model_dump(), operator_open_id=ACTOR["id"], operator_name="隔离测试操作人")
            self.messages.extend(data["messages"])
            return {"ok": True, "data": {"sent_count": 0 if self.fail_send else len(data["messages"]),
                "failed_count": len(data["messages"]) if self.fail_send else 0, "skipped": data["skipped"]}}

        self.portal = PortalAgent(self.assistant, PortalAPICatalog(self.app), LighthouseFiles(self.store))
        self.request = Request({"type": "http", "path": "/api/assistant/messages", "root_path": "", "scheme": "http", "server": ("testserver", 80),
                                "client": ("127.0.0.1", 1), "headers": [(b"origin", b"http://testserver")], "query_string": b""})

    def prepare(self, *, body=None, queries=None, fields=None):
        return self.portal.prepare(ACTOR, {"title": "发送签名使用确认", "operations": [{"api_id": _SIGNATURE_USAGE, "body": body or {"scope": "A", "notice_key": "notice-a"}}], "fields": fields or []},
            "approval-fixture", [], queries=queries if queries is not None else {"bootstrap": {"notices": [NOTICE]}, "preview": MOP})

    async def ready(self, plan):
        public = await self.portal.field_options(ACTOR, plan["id"], "step0.signatures", self.request)
        self.assertNotIn("NO-IMAGE-IN-CHAT", json.dumps(public))
        self.assertNotIn("ou_person", json.dumps(public))
        self.assertNotIn("private-file-token", json.dumps(public))
        self.assertTrue(public["operations"][0]["selected_labels"]["selected_document"])
        return self.portal.amend(ACTOR, plan["id"], {"version": public["version"], "values": {"step0.signatures": [item["value"] for item in public["fields"][0]["options"]]}})

    async def execute(self, plan):
        reviewed = await self.portal.confirm(ACTOR, plan["id"], {"version": plan["version"], "stage": "review"}, self.request)
        self.assertEqual(reviewed["status"], "awaiting_second_confirmation")
        self.assertEqual(self.writes, [])
        await self.portal.confirm(ACTOR, plan["id"], {"version": reviewed["version"], "stage": "execute"}, self.request)
        if self.portal.tasks:
            await asyncio.gather(*tuple(self.portal.tasks))
        return self.portal.get_plan(ACTOR, plan["id"])

    async def test_mop_roles_use_original_context_and_native_deduplication(self):
        plan = self.prepare(fields=[{"path": "notice_key", "type": "text"}, {"path": "request_base_url", "type": "text"}])
        self.assertEqual(len(plan["fields"]), 1)
        plan = await self.ready(plan)
        stored = self.portal.get_plan(ACTOR, plan["id"])
        body = _result_refs(stored["operations"][0]["body"], [], stored.get("_references"), stored.get("_queries"))
        self.assertEqual(body["notice_key"], "notice-a|mop:mop-a|attachment:private-file-token")
        self.assertEqual({row["role"] for row in body["signatures"]}, {"implementer", "auditor"})
        self.assertEqual({row["record_id"] for row in body["signatures"]}, {"person-a"})
        result = await self.execute(plan)
        self.assertEqual(result["status"], "completed", result.get("error"))
        self.assertEqual(len(self.messages), 1)
        self.assertIn("维护实施人、维护审核人", self.messages[0]["text"])
        self.assertIn("A楼维护", self.messages[0]["text"])
        self.assertIn("http://testserver/api/signatures/usage-confirm?token=", self.messages[0]["text"])
        self.service.signature_image_bytes.assert_not_called()
        await self.portal.confirm(ACTOR, plan["id"], {"version": result["version"], "stage": "execute"}, self.request)
        self.assertEqual(len(self.writes), 1)

    async def test_guard_uses_task_scope_and_only_inspector_role(self):
        plan = self.prepare(body={"scope": "A", "context_type": "critical_guard", "notice_key": "guard-a", "mop_attachment_name": "设备安全"}, queries={"task": TASK})
        plan = await self.ready(plan)
        result = await self.execute(plan)
        self.assertEqual(result["status"], "completed", result.get("error"))
        self.assertEqual(self.reads, [("A", "critical_guard:guard-a:A")])
        self.assertEqual(self.writes[0]["notice_key"], "critical_guard:guard-a:A")
        self.assertEqual(self.writes[0]["mop_attachment_name"], "本任务全部检查表")
        self.assertEqual(self.writes[0]["signatures"][0]["role"], "inspector")
        self.assertIn("【重保检查签名使用确认】", self.messages[0]["text"])

    async def test_partial_send_is_failure_without_automatic_retry(self):
        self.fail_send = True
        result = await self.execute(await self.ready(self.prepare()))
        self.assertEqual(result["status"], "failed")
        self.assertIn("1 人发送失败", result["results"][0]["query_reply"])
        with self.assertRaises(AssistantError):
            await self.portal.confirm(ACTOR, result["id"], {"version": result["version"], "stage": "execute"}, self.request)
        self.assertEqual(len(self.writes), 1)

    async def test_unknown_context_scope_recipient_and_custom_url_rejected(self):
        for body in ({"scope": "D", "notice_key": "notice-a"}, {"scope": "A", "notice_key": "unknown"},
                     {"scope": "A", "notice_key": "notice-a", "request_base_url": "https://attacker.invalid"},
                     {"scope": "A", "context_type": "work_order"}):
            with self.assertRaises(AssistantError):
                self.prepare(body=body)
        with self.assertRaises(AssistantError):
            self.prepare(queries={})
        plan = self.prepare()
        with self.assertRaises(AssistantError):
            self.portal.amend(ACTOR, plan["id"], {"version": plan["version"], "values": {"step0.signatures": ["invented-person"]}})
        self.assertEqual(self.writes, [])


if __name__ == "__main__":
    unittest.main()
