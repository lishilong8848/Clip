"""Agent acceptance on isolated APIs and files, never real business writes."""
import copy
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from PIL import Image
from lan_bitable_template_portal.lighthouse_ai import AssistantError, LighthouseAssistant, PRIVATE_REPLY
from lan_bitable_template_portal.lighthouse_agent import PortalAgent, parse_decision, _result_refs, _set_path
from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog
from lan_bitable_template_portal.lighthouse_files import LighthouseFiles, MAX_FILE_BYTES

ACTOR = {"id": "fixture-a", "scopes": ["A"], "is_admin": False}


class NoticeInput(BaseModel):
    scope: str
    title: str
    action: str = "start"
    target_record_id: str = ""
    operation_id: str = ""


class Store:
    def __init__(self, path):
        self.db_path, self.docs = path, {}

    def get_document(self, namespace, key):
        return copy.deepcopy(self.docs.get((namespace, key)))

    def put_document(self, namespace, key, value):
        self.docs[namespace, key] = copy.deepcopy(value)


async def _read_model_request(request, model):
    return model.model_validate(await request.json())


class AgentTests(unittest.IsolatedAsyncioTestCase):
    def test_signer_preview_keeps_identity_not_images(self):
        plan = {"operations": [{"api_id": "POST /api/engineer/mop/fill", "body": {
            "signature_time": "10:30", "signatures": [{"source": "staff", "name": "测试人员", "record_id": "rec-test",
                "image": "private-image", "signature_url": "private-url", "base64": "private-base64"}]}}]}
        body = self.agent.public_plan(plan)["operations"][0]["body"]
        self.assertEqual(body["signature_time"], "10:30")
        self.assertEqual(body["signatures"], [{"source": "staff", "name": "测试人员", "record_id": "rec-test"}])
        plan["operations"][0]["body"]["signature_time"] = "2026-10-01T10:30"
        self.assertEqual(self.agent.public_plan(plan)["operations"][0]["body"]["signature_time"], "2026-10-01T10:30")
        plan["operations"][0]["body"]["signature_time"] = "invalid or private payload"
        self.assertNotIn("signature_time", self.agent.public_plan(plan)["operations"][0]["body"])

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = Store(Path(self.tmp.name) / "state.sqlite3")
        self.model = Mock()
        self.model.settings.return_value = {"configured": True, "enabled": True, "active_model_id": "default", "models": [{"id": "default", "name": "测试模型", "model": "fixture-model", "configured": True}]}
        self.model.profile.return_value = {"id": "default", "name": "测试模型", "model": "fixture-model"}
        self.search = Mock(side_effect=AssertionError("agent must use backend APIs, not local/DOM scraping"))
        self.assistant = LighthouseAssistant(self.store, self.search, model=self.model)
        self.files = LighthouseFiles(self.store)
        self.writes, self.reads = [], []
        app = FastAPI()

        @app.get("/api/repair-management/records")
        async def repair_records(request: Request):
            self.reads.append(dict(request.query_params))
            if request.cookies.get("fixture") != "a" or request.query_params.get("scope", "A") != "A":
                return JSONResponse({"ok": False, "error": "无权查询该楼栋"}, status_code=403)
            return {"ok": True, "data": {"total": 1, "items": [{"record_id": "rec-test-a", "scope": "A", "title": "A楼柴发维修", "status": "维修中"}]}}

        @app.get("/api/cabinet-power/batches")
        async def cabinet_batches(request: Request):
            return {"ok": True, "data": {"total": 2, "items": [{"batch_id": "batch-a", "scope": "A", "status": "pending"}]}}

        @app.post("/api/workbench-actions")
        async def submit_notice(request: Request):
            data = await _read_model_request(request, NoticeInput)
            if request.cookies.get("fixture") != "a" or data.scope != "A":
                return JSONResponse({"ok": False, "error": "无权写入"}, status_code=403)
            self.writes.append(data.model_dump())
            return {"ok": True, "data": {"record_id": data.target_record_id or "rec-created", "saved": True}}

        @app.delete("/api/drills/{drill_id}")
        async def delete_drill(drill_id: str, request: Request):
            self.writes.append({"deleted": drill_id})
            return {"ok": True, "data": {"deleted": True}}

        @app.post("/api/notice-attachments")
        async def upload_notice(request: Request):
            form = await request.form()
            file = form["file"]
            self.writes.append({"upload": file.filename, "content": await file.read()})
            return {"ok": True, "data": {"upload_id": "upload-fixture", "file_token": "private-file-token"}}

        self.catalog = PortalAPICatalog(app)
        self.agent = PortalAgent(self.assistant, self.catalog, self.files)
        self.request = Request({"type": "http", "scheme": "http", "server": ("testserver", 80), "client": ("127.0.0.1", 4567), "path": "/api/assistant/agent", "root_path": "", "query_string": b"", "headers": [(b"cookie", b"fixture=a"), (b"origin", b"http://testserver")]})
        self.payload = {"question": "查询A楼维修和机柜数据", "conversation_id": self.assistant.conversation(ACTOR)["conversation_id"], "operation_id": "agent_test_00000001"}

    def decisions(self, *values):
        self.model.complete.side_effect = [json.dumps(value, ensure_ascii=False) for value in values]

    async def test_multi_source_api_query_and_context_without_dom_or_cache_reads(self):
        self.decisions({"action": "query", "operation": {"api_id": "GET /api/repair-management/records", "params": {"scope": "A"}}},
                       {"action": "query", "operation": {"api_id": "GET /api/cabinet-power/batches"}},
                       {"action": "answer", "text": "A楼有1个维修项目[1]、2个机柜待办批次[2]。"})
        result = await self.agent.chat(ACTOR, self.payload, self.request)
        self.assertEqual(len(result["turns"][-1]["sources"]), 2)
        self.assertIn("1个维修项目", result["turns"][-1]["answer"])
        self.assertEqual(self.reads, [{"scope": "A"}])
        self.assertEqual(self.writes, [])
        self.search.assert_not_called()
        await self.agent.chat(ACTOR, self.payload, self.request)
        self.assertEqual(self.model.complete.call_count, 3)

    async def test_query_cannot_execute_mutation_even_if_model_requests_it(self):
        operation = {"api_id": "DELETE /api/drills/{drill_id}", "path_params": {"drill_id": "x"}}
        self.decisions({"action": "query", "operation": operation}, {"action": "prepare", "operations": [operation]})
        result = await self.agent.chat(ACTOR, {**self.payload, "question": "删除演练"}, self.request)
        self.assertEqual(result["turns"][-1]["plan"]["status"], "awaiting_confirmation")
        self.assertEqual(self.writes, [])

    async def test_explicit_action_with_missing_fields_returns_form_not_plain_question(self):
        self.decisions({"action": "answer", "text": "请提供完整原文。"}, {"action": "prepare", "operations": [{"api_id": "POST /api/workbench-actions", "body": {"scope": "A"}}]})
        result = await self.agent.chat(ACTOR, {**self.payload, "question": "帮我准备一条通告，先不实际发送。"}, self.request)
        self.assertEqual(result["turns"][-1]["plan"]["status"], "needs_input")
        self.assertTrue(result["turns"][-1]["plan"]["fields"])
        self.assertEqual(self.writes, [])

    async def test_missing_input_first_confirmation_second_confirmation_and_repeat_click(self):
        self.decisions({"action": "prepare", "title": "发送A楼通告", "operations": [{"api_id": "POST /api/workbench-actions", "body": {"scope": "A"}}]})
        result = await self.agent.chat(ACTOR, self.payload, self.request)
        plan = result["turns"][-1]["plan"]
        self.assertEqual(plan["status"], "needs_input")
        self.assertEqual(self.writes, [])
        value = {field["name"]: "A楼维护通告" for field in plan["fields"]}
        plan = self.agent.amend(ACTOR, plan["id"], {"version": plan["version"], "values": value})
        plan = await self.agent.confirm(ACTOR, plan["id"], {"version": plan["version"], "stage": "review"}, self.request)
        self.assertEqual(plan["status"], "awaiting_second_confirmation")
        self.assertEqual(self.writes, [])
        with self.assertRaises(AssistantError):
            await self.agent.confirm(ACTOR, plan["id"], {"version": plan["version"], "stage": "review"}, self.request)
        await self.agent.confirm(ACTOR, plan["id"], {"version": plan["version"], "stage": "execute"}, self.request)
        await self.agent.confirm(ACTOR, plan["id"], {"version": plan["version"], "stage": "execute"}, self.request)
        await asyncio_gather(self.agent)
        self.assertEqual(len(self.writes), 1)
        self.assertEqual(self.writes[0]["title"], "A楼维护通告")
        self.assertTrue(self.writes[0]["operation_id"])
        finished = self.agent.get_plan(ACTOR, plan["id"])
        self.assertEqual(finished["status"], "completed")
        self.assertIn("操作已完成", self.assistant.conversation(ACTOR)["turns"][-1]["answer"])

    async def test_current_user_scope_is_enforced_by_original_api(self):
        self.decisions({"action": "query", "operation": {"api_id": "GET /api/repair-management/records", "params": {"scope": "B"}}}, {"action": "answer", "text": "当前账号无权查询B楼。"})
        result = await self.agent.chat(ACTOR, self.payload, self.request)
        self.assertEqual(result["turns"][-1]["sources"][0]["data"]["status"], 403)
        self.assertIn("无权查询", result["turns"][-1]["answer"])

    async def test_cancelled_and_foreign_plans_cannot_execute(self):
        decision = {"operations": [{"api_id": "DELETE /api/drills/{drill_id}", "path_params": {"drill_id": "drill1"}}]}
        plan = self.agent.prepare(ACTOR, decision, self.payload["operation_id"], [])
        with self.assertRaises(AssistantError):
            self.agent.get_plan({**ACTOR, "id": "fixture-b"}, plan["id"])
        self.agent.cancel(ACTOR, plan["id"])
        with self.assertRaises(AssistantError):
            await self.agent.confirm(ACTOR, plan["id"], {"version": 2, "stage": "execute"}, self.request)
        self.assertEqual(self.writes, [])

    async def test_attachment_ownership_extraction_and_model_context(self):
        file = self.files.upload(ACTOR, "记录.txt", ("A楼机柜B01 上测试电\n" * 800).encode())
        self.assertTrue(self.files.text(ACTOR, file["id"], offset=6500)["text"])
        with self.assertRaises(AssistantError):
            self.files.get({**ACTOR, "id": "fixture-b"}, file["id"])
        self.decisions({"action": "read_file", "file_id": file["id"], "offset": 6000}, {"action": "answer", "text": "附件包含A楼B01机柜上测试电信息。"})
        await self.agent.chat(ACTOR, {**self.payload, "question": "读这个文件", "file_ids": [file["id"]]}, self.request)
        self.assertIn("A楼机柜B01", json.dumps(self.model.complete.call_args.args[0], ensure_ascii=False))

    async def test_followup_can_read_prior_attachment_without_reupload(self):
        file = self.files.upload(ACTOR, "previous.txt", b"first row\nsecond row")
        self.decisions({"action": "answer", "text": "附件已读取。"})
        await self.agent.chat(ACTOR, {**self.payload, "question": "读取文件", "file_ids": [file["id"]]}, self.request)
        self.model.complete.reset_mock()
        self.decisions({"action": "read_file", "file_id": file["id"]}, {"action": "answer", "text": "还有第二行。"})
        await self.agent.chat(ACTOR, {**self.payload, "question": "刚才文件还有什么内容？", "operation_id": "followup_file_00000001"}, self.request)
        initial_messages = self.model.complete.call_args_list[0].args[0]
        self.assertIn(file["id"], json.dumps(initial_messages, ensure_ascii=False))
        self.assertIn("second row", json.dumps(self.model.complete.call_args.args[0], ensure_ascii=False))
        self.assertEqual(self.writes, [])

    async def test_image_text_and_visual_input_are_provided_together(self):
        output = io.BytesIO(); Image.new("RGB", (20, 20), "white").save(output, "PNG")
        with patch("lan_bitable_template_portal.lighthouse_files.recognize_text", return_value="E楼202包间B17 上测试电 2026-07-18 15:51:55"):
            file = self.files.upload(ACTOR, "确认截图.png", output.getvalue())
        self.decisions({"action": "answer", "text": "这是一张机柜确认截图。"})
        await self.agent.chat(ACTOR, {**self.payload, "question": "识别截图", "file_ids": [file["id"]]}, self.request)
        parts = self.model.complete.call_args.args[0][-1]["content"]
        self.assertEqual(parts[1]["type"], "image_url")
        self.assertIn("B17", parts[0]["text"])
        self.assertNotIn("path", file)

    def test_windows_ocr_accepts_native_awaitable_not_only_coroutines(self):
        import asyncio
        from types import SimpleNamespace
        from lan_bitable_template_portal.lighthouse_files import _ocr_worker
        class NativeAwaitable:
            def __await__(self):
                return asyncio.sleep(0, result=SimpleNamespace(lines=[SimpleNamespace(text="B17 实际完成09:15")])).__await__()
        output = io.BytesIO(); Image.new("RGB", (20, 20), "white").save(output, "PNG")
        sender = Mock()
        with patch.dict(sys.modules, {"winocr": SimpleNamespace(recognize_pil=lambda *_: NativeAwaitable())}), patch("ctypes.WinDLL", return_value=Mock(), create=True):
            _ocr_worker(output.getvalue(), sender)
        sender.send.assert_called_once_with((True, "B17 实际完成09:15"))

    async def test_image_stays_on_original_question_when_planning_hints_are_appended(self):
        output = io.BytesIO(); Image.new("RGB", (20, 20), "white").save(output, "PNG")
        with patch("lan_bitable_template_portal.lighthouse_files.recognize_text", return_value="截图字段"):
            file = self.files.upload(ACTOR, "截图.png", output.getvalue())
        self.decisions({"action": "prepare", "operations": [{"api_id": "POST /api/workbench-actions", "body": {"scope": "A"}}]})
        await self.agent.chat(ACTOR, {**self.payload, "question": "帮我准备一条通告", "file_ids": [file["id"]]}, self.request)
        messages = self.model.complete.call_args.args[0]
        multimodal = [m for m in messages if isinstance(m["content"], list)]
        self.assertEqual(len(multimodal), 1)
        self.assertIn("帮我准备一条通告", multimodal[0]["content"][0]["text"])
        self.assertIn("本轮文件", multimodal[0]["content"][0]["text"])
        self.assertIsInstance(messages[-1]["content"], str)

    async def test_transient_model_error_does_not_claim_vision_is_unsupported(self):
        output = io.BytesIO(); Image.new("RGB", (20, 20), "white").save(output, "PNG")
        with patch("lan_bitable_template_portal.lighthouse_files.recognize_text", return_value="可靠识别文字"):
            file = self.files.upload(ACTOR, "截图.png", output.getvalue())
        self.model.complete.side_effect = AssistantError("模型连接超时", 502)
        with self.assertRaises(AssistantError):
            await self.agent.chat(ACTOR, {**self.payload, "question": "识别截图", "file_ids": [file["id"]]}, self.request)
        self.assertEqual(self.model.complete.call_count, 1)
        self.assertNotIn("未接受图片", json.dumps(self.model.complete.call_args.args[0], ensure_ascii=False))

    async def test_explicit_unsupported_vision_uses_ocr_without_losing_original_image(self):
        output = io.BytesIO(); Image.new("RGB", (20, 20), "white").save(output, "PNG")
        with patch("lan_bitable_template_portal.lighthouse_files.recognize_text", return_value="E楼202包间 B17"):
            file = self.files.upload(ACTOR, "截图.png", output.getvalue())
        self.model.complete.side_effect = [AssistantError("图片输入不支持", 502, category="vision_unsupported"), '{"action":"answer","text":"OCR识别到E楼202包间B17，其他图像内容需人工核对。"}']
        result = await self.agent.chat(ACTOR, {**self.payload, "question": "识别截图", "file_ids": [file["id"]]}, self.request)
        self.assertIn("B17", result["turns"][-1]["answer"])
        self.assertEqual(result["turns"][-1]["attachments"][0]["id"], file["id"])

    async def test_private_question_never_reaches_model_or_file_processing(self):
        result = await self.agent.chat(ACTOR, {**self.payload, "question": "张三的身份证号"}, self.request)
        self.assertEqual(result["turns"][-1]["answer"], PRIVATE_REPLY)
        self.model.complete.assert_not_called()
        self.assertEqual(self.reads, [])

    async def test_generated_file_download_is_authorized_and_survives_public_plan(self):
        from fastapi.responses import Response

        @self.catalog.app.get("/api/fixture-export-download")
        async def download():
            return Response(content=b"fixture-file", media_type="application/pdf", headers={"content-disposition": 'attachment; filename="export.pdf"'})

        self.agent.catalog = PortalAPICatalog(self.catalog.app)
        result = await self.agent._invoke(ACTOR, {"api_id": "GET /api/fixture-export-download"}, self.request)
        public = self.agent.public_plan({"operations": [], "results": [result]})
        file = public["results"][0]["data"]
        self.assertEqual(file["url"], "/api/assistant/files/" + file["id"])
        self.assertNotIn(str(self.tmp.name), json.dumps(public))
        with self.assertRaises(AssistantError):
            self.files.get({**ACTOR, "id": "another-user"}, file["id"])

    def test_complete_history_reference_can_prepend_without_dropping_proofs(self):
        old = [{"id": "old", "action": "上测试电", "expected": "", "evidence_files": [{"scopes": ["E"], "file_token": "private-proof-token"}]}]
        result = _result_refs({"$concat": [[{"action": "下测试电", "actual": "2026-09-30 12:00"}], {"$query": {"ref": "query-fixture", "path": "groups"}}]}, [], queries={"query-fixture": {"groups": old}})
        self.assertEqual(result[1:], old)
        _set_path({"groups": result}, "groups.0.expected", "2026-09-30 12:05")
        self.assertEqual(result[0]["expected"], "2026-09-30 12:05")
        self.assertEqual(old[0]["expected"], "")

    def test_unsafe_file_types_and_oversize_are_rejected(self):
        with self.assertRaises(AssistantError): self.files.upload(ACTOR, "run.exe", b"program")
        with self.assertRaises(AssistantError): self.files.upload(ACTOR, "large.txt", b"x" * (MAX_FILE_BYTES + 1))
        self.assertEqual(parse_decision("普通回答")["action"], "answer")
        with self.assertRaises(AssistantError): parse_decision('{"action":"execute"}')


async def asyncio_gather(agent):
    import asyncio
    if agent.tasks:
        await asyncio.gather(*tuple(agent.tasks))


if __name__ == "__main__":
    unittest.main()
