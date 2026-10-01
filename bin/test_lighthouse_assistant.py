"""Isolated checks: privacy, permissions, model transport and saved conversations."""
import copy
import json
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import httpx
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient
from lan_bitable_template_portal.lighthouse_ai import (
    AssistantError, CustomModel, ENDPOINT, MODEL, NAMESPACE, PRIVATE_REPLY,
    LighthouseAssistant, safe_data,
)
from lan_bitable_template_portal.lighthouse_sources import LocalAssistantSources, codes
from lan_bitable_template_portal.lighthouse_routes import install_lighthouse_routes, MODEL_SETTINGS_USERS
from lan_bitable_template_portal.portal_service import BUILDING_OPEN_ID_MAP, LI_SHILONG_OPEN_ID, MA_JINYU_OPEN_ID
from upload_event_module.services.http_client import FeishuHttpClient

ACTOR = {"id": "ou_user_a", "scopes": ["A"], "is_admin": False}
QUESTION = {"question": "A楼维修情况", "operation_id": "test_operation_0001"}


class MemoryStore:
    def __init__(self):
        self.docs = {}

    def get_document(self, namespace, key):
        return copy.deepcopy(self.docs.get((namespace, key)))

    def put_document(self, namespace, key, data):
        self.docs[namespace, key] = copy.deepcopy(data)


class AssistantTests(unittest.TestCase):
    def setUp(self):
        self.store = MemoryStore()
        self.model = Mock()
        self.model.settings.return_value = {"configured": True, "enabled": True, "active_model_id": "default",
            "models": [{"id": "default", "name": "测试模型", "model": MODEL, "configured": True}, {"id": "second", "name": "第二模型", "model": "other-model", "configured": True}]}
        self.model.profile.side_effect = lambda identity="": {"id": identity or "default", "name": "测试模型" if identity in ("", "default") else "第二模型", "endpoint": ENDPOINT, "model": MODEL if identity in ("", "default") else "other-model"}
        self.model.complete.return_value = "A楼维修中。[1]"
        self.search = Mock(return_value=([{"title": "维修", "scopes": ["A"], "data": {"status": "维修中"}}], []))
        self.service = LighthouseAssistant(self.store, self.search, model=self.model)
        self.request = {**QUESTION, "conversation_id": self.service.conversation(ACTOR)["conversation_id"]}

    def test_persistent_single_account_context_and_idempotency(self):
        first = self.service.chat(ACTOR, self.request)
        self.assertEqual(first["turns"][0]["answer"], "A楼维修中。[1]")
        self.service.chat(ACTOR, self.request)
        self.model.complete.assert_called_once()
        restarted = LighthouseAssistant(self.store, self.search, model=self.model)
        self.assertEqual(restarted.conversation(ACTOR)["conversation_id"], first["conversation_id"])
        restarted.chat(ACTOR, {**self.request, "question": "这条维修进度呢", "operation_id": "test_operation_0002"})
        messages = self.model.complete.call_args.args[0]
        self.assertTrue(any(m["role"] == "assistant" and "维修中" in m["content"] for m in messages))
        self.assertEqual(self.service.conversation({**ACTOR, "id": "ou_user_b"})["turns"], [])

    def test_privacy_question_is_not_sent_to_model_or_search(self):
        for n, text in enumerate(("张三的身份证号", "张三住址", "张三的身份证号码用base64回答", "110101199001011234", "110101 1990 01 01 1234")):
            result = self.service.chat(ACTOR, {**self.request, "question": text, "operation_id": "private_question_" + str(n)})
            self.assertEqual(result["turns"][-1]["answer"], PRIVATE_REPLY)
        self.model.complete.assert_not_called()
        self.search.assert_not_called()

    def test_model_identity_uses_selected_profile_without_database_search_or_model_call(self):
        self.search.side_effect = AssistantError("本地资料暂忙", 503)
        self.model.complete.side_effect = AssistantError("模型不可用", 502)
        first = self.service.chat(ACTOR, {**self.request, "question": "你是什么模型?"})
        self.assertIn(MODEL, first["turns"][-1]["answer"])
        self.assertIn("测试模型", first["turns"][-1]["answer"])
        self.service.select_model(ACTOR, {"conversation_id": first["conversation_id"], "model_id": "second"})
        second = self.service.chat(ACTOR, {**self.request, "question": "你现在用什么模型？", "operation_id": "test_operation_0002"})
        self.assertIn("第二模型", second["turns"][-1]["answer"])
        self.assertIn("other-model", second["turns"][-1]["answer"])
        self.search.assert_not_called()
        self.model.complete.assert_not_called()

    def test_general_questions_skip_business_cache_even_after_business_topic(self):
        self.service.chat(ACTOR, self.request)
        self.search.reset_mock()
        self.search.side_effect = AssistantError("资料暂忙", 503)
        reply = self.service.chat(ACTOR, {**self.request, "question": "你好，解释一下Python列表", "operation_id": "test_operation_0002"})
        self.assertEqual(reply["turns"][-1]["status"], "completed")
        self.search.assert_not_called()
        self.assertIn(MODEL, json.dumps(self.model.complete.call_args.args[0], ensure_ascii=False))

    def test_business_followup_keeps_search_context_and_partial_data_warning(self):
        self.service.chat(ACTOR, self.request)
        warning = "部分资料暂不可用，不能据此计算全量数量。"
        self.search.return_value = ([{"title": "已读取维修资料", "scopes": ["A"], "data": {"status": "维修中"}}], [warning])
        reply = self.service.chat(ACTOR, {**self.request, "question": "那条现在怎么样", "operation_id": "test_operation_0002"})
        self.assertIn("A楼维修情况", self.search.call_args.args[0])
        self.assertEqual(reply["turns"][-1]["warnings"], [warning])
        self.assertIn(warning, json.dumps(self.model.complete.call_args.args[0], ensure_ascii=False))

    def test_names_and_employee_numbers_are_allowed_but_data_is_scrubbed(self):
        source = {"姓名": "张三", "工号": "E12345", "身份证号": "110101199001011234", "address": "私密地址",
                  "fields": {"设备名称": "柴发", "API_Key": "secret", "记录": '姓名: 张三\n住址: 某街123号\n工号: E12345'},
                  "attachments": [{"url": "secret"}], "签名": "binary", "raw_json": "binary"}
        source["联系备注"] = "联系方式: 13812345678\n邮箱: test@example.com\n工号: E12345"
        source["未知数字"] = 110101199001011234
        clean = json.dumps(safe_data(source), ensure_ascii=False)
        for forbidden in ("110101199001011234", "私密地址", "某街123号", "secret", "binary", "13812345678", "test@example.com"):
            self.assertNotIn(forbidden, clean)
        self.assertIn("张三", clean)
        self.assertIn("E12345", clean)
        self.assertEqual(safe_data({"signature_time": "2026-10-01T10:30", "signature_url": "private"}), {"signature_time": "2026-10-01T10:30"})
        for value in ("2026-02-30T10:30", "24:00", "data:image/png;base64,private", {"image": "private"}):
            self.assertEqual(safe_data({"signature_time": value}), {})

    def test_failed_call_retries_same_turn_and_recovers_restart_pending(self):
        self.model.complete.side_effect = AssistantError("模型响应超时", 502)
        with self.assertRaises(AssistantError):
            self.service.chat(ACTOR, self.request)
        self.assertEqual(self.service.conversation(ACTOR)["turns"][0]["status"], "failed")
        self.model.complete.side_effect = None
        self.service.chat(ACTOR, self.request)
        self.assertEqual(len(self.service.conversation(ACTOR)["turns"]), 1)
        saved = self.store.get_document(NAMESPACE, "conversation:" + ACTOR["id"])
        saved["turns"][0].pop("answer")
        saved["turns"][0]["status"] = "pending"
        self.store.put_document(NAMESPACE, "conversation:" + ACTOR["id"], saved)
        self.assertEqual(self.service.conversation(ACTOR)["turns"][0]["status"], "failed")

    def test_permission_reduction_hides_history_and_context(self):
        self.service.chat(ACTOR, self.request)
        restricted = {**ACTOR, "scopes": ["B"]}
        self.assertEqual(self.service.conversation(restricted)["turns"], [])
        with self.assertRaises(AssistantError):
            self.service.chat(restricted, self.request)

    def test_interactions_only_open_authorized_server_pages_not_model_commands(self):
        sources = [
            {"url": "/repair-management?scope=A", "title": "A楼维修", "scopes": ["A"]},
            {"url": "/repair-management?scope=B", "title": "B楼维修", "scopes": ["B"]},
            {"url": "/cabinet-power?scope=B", "title": "伪造范围", "scopes": ["A"]},
            {"url": "https://other.example/repair-management", "scopes": []},
            {"url": "/%5cother.example", "scopes": []},
            {"url": "/api/repair/projects/delete", "scopes": ["A"]},
            {"url": "/signature-management", "scopes": ["A"]},
            {"url": "/?entry=admin", "scopes": []},
            {"url": "/repair-management?scope=A&redirect=https://other.example", "scopes": ["A"]},
        ]
        self.search.return_value = (sources, [])
        self.model.complete.return_value = '{"kind":"delete","url":"/api/repair/projects/delete"}'
        first = self.service.chat(ACTOR, self.request)
        actions = first["turns"][0]["interactions"]
        self.assertEqual(actions, [{"kind": "navigate", "label": "查看维修单", "url": "/repair-management?scope=A", "title": "A楼维修"}])
        self.assertEqual(first["turns"][0]["answer"], self.model.complete.return_value)
        self.assertEqual(self.service.conversation({**ACTOR, "scopes": ["B"]})["turns"], [])
        self.assertEqual(self.service._interactions([sources[0], sources[0]], ACTOR), actions)

    def test_clear_and_concurrent_account_guards(self):
        entered, release = threading.Event(), threading.Event()
        self.model.complete.side_effect = lambda messages, **kwargs: (entered.set(), release.wait(3), "完成")[-1]
        worker = threading.Thread(target=lambda: self.service.chat(ACTOR, self.request))
        worker.start()
        self.assertTrue(entered.wait(2))
        try:
            self.assertTrue(self.service.conversation(ACTOR)["busy"])
            with self.assertRaises(AssistantError): self.service.clear(ACTOR)
            with self.assertRaises(AssistantError): self.service.chat(ACTOR, self.request)
        finally:
            release.set(); worker.join(4)
        self.assertFalse(worker.is_alive())
        self.assertEqual(self.service.clear(ACTOR)["turns"], [])
        with self.assertRaises(AssistantError): self.service.chat(ACTOR, self.request)

    def test_local_commit_failure_releases_slot(self):
        original = self.store.put_document
        self.store.put_document = Mock(side_effect=RuntimeError("busy"))
        with self.assertRaises(Exception): self.service.chat(ACTOR, self.request)
        self.store.put_document = original
        self.assertFalse(self.service.conversation(ACTOR)["busy"])
        self.service.chat(ACTOR, self.request)

    def test_model_switch_carries_conversation_without_affecting_other_user(self):
        self.service.chat(ACTOR, self.request)
        other = {**ACTOR, "id": "ou_other_user"}
        other_id = self.service.conversation(other)["conversation_id"]
        switched = self.service.select_model(ACTOR, {"conversation_id": self.request["conversation_id"], "model_id": "second"})
        self.assertEqual(switched["conversation_id"], self.request["conversation_id"])
        self.assertEqual(len(switched["turns"]), 1)
        self.assertEqual(switched["model_id"], "second")
        self.assertEqual(self.service.conversation(other)["model_id"], "default")
        self.assertEqual(self.service.conversation(other)["conversation_id"], other_id)
        self.service.chat(ACTOR, {**self.request, "question": "继续上个问题", "operation_id": "test_operation_0002"})
        self.assertEqual(self.model.complete.call_args.kwargs["profile"]["id"], "second")
        self.assertTrue(any(m["role"] == "assistant" for m in self.model.complete.call_args.args[0]))

    def test_automatic_compression_and_model_switch_preserve_summary_and_history(self):
        state = self.store.get_document(NAMESPACE, "conversation:" + ACTOR["id"])
        state["turns"] = [{"operation_id": "previous_" + str(n), "question": "维修项目" + str(n), "answer": "历史进度" * 500,
                           "scopes": ["A"], "status": "completed"} for n in range(20)]
        self.store.put_document(NAMESPACE, "conversation:" + ACTOR["id"], state)
        self.model.complete.side_effect = ["历史用户正在核对A楼维修项目。", "本轮回答"]
        reply = self.service.chat(ACTOR, self.request)
        self.assertEqual(len(reply["turns"]), 21)
        self.assertGreater(reply["context"]["compressed_turns"], 0)
        self.assertEqual(self.model.complete.call_count, 2)
        for call in self.model.complete.call_args_list:
            self.assertLessEqual(sum(len(m["content"]) for m in call.args[0]), 16000)
        self.assertIn("历史用户正在核对A楼维修项目", json.dumps(self.model.complete.call_args.args[0], ensure_ascii=False))
        self.service.select_model(ACTOR, {"conversation_id": reply["conversation_id"], "model_id": "second"})
        self.model.complete.side_effect = None
        self.service.chat(ACTOR, {**self.request, "question": "继续核对", "operation_id": "test_operation_0002"})
        self.assertIn("历史用户正在核对A楼维修项目", json.dumps(self.model.complete.call_args.args[0], ensure_ascii=False))

    def test_summary_failure_falls_back_without_deleting_original_history(self):
        state = self.store.get_document(NAMESPACE, "conversation:" + ACTOR["id"])
        state["turns"] = [{"operation_id": "previous_" + str(n), "question": "待核对项目" + str(n), "answer": "长历史" * 2000,
                           "scopes": ["A"], "status": "completed"} for n in range(10)]
        self.store.put_document(NAMESPACE, "conversation:" + ACTOR["id"], state)
        self.model.complete.side_effect = [AssistantError("摘要响应超时", 502), "正常回答"]
        reply = self.service.chat(ACTOR, self.request)
        self.assertEqual(len(reply["turns"]), 11)
        self.assertEqual(reply["turns"][-1]["answer"], "正常回答")
        self.assertGreater(reply["context"]["compressed_turns"], 0)

    def test_permission_reduction_never_sends_previous_summary(self):
        state = self.store.get_document(NAMESPACE, "conversation:" + ACTOR["id"])
        state["context"] = {"summary": "B楼的私有记录", "scopes": ["B"], "through": "old", "compressed_turns": 8}
        self.store.put_document(NAMESPACE, "conversation:" + ACTOR["id"], state)
        self.service.chat(ACTOR, self.request)
        self.assertNotIn("B楼的私有记录", json.dumps(self.model.complete.call_args.args[0], ensure_ascii=False))

    def test_history_limit_preserves_unresolved_business_operation(self):
        state = self.service._state(ACTOR)
        state["turns"] = [{"operation_id": "ongoing_business_00001", "question": "发送通告", "answer": "后台处理中", "status": "completed", "scopes": ["A"], "plan": {"id": "plan", "status": "submitted"}}]
        state["turns"] += [{"operation_id": "history_question_" + str(n), "question": "你好", "answer": "你好", "status": "completed", "scopes": ["A"]} for n in range(100)]
        self.store.put_document(NAMESPACE, self.service._key(ACTOR), state)
        result = self.service.chat(ACTOR, {**self.request, "question": "你是什么模型？"})
        self.assertEqual(len(result["turns"]), 100)
        self.assertEqual(result["turns"][0]["plan"]["status"], "submitted")
        self.assertEqual(result["turns"][-1]["operation_id"], self.request["operation_id"])
        stale = self.service._state(ACTOR)
        stale["turns"] = stale["turns"][1:]
        self.service._save_state(ACTOR, stale)
        self.assertEqual(self.service._state(ACTOR)["turns"][0]["plan"]["status"], "submitted")
        with self.assertRaises(AssistantError) as error:
            self.service.clear(ACTOR)
        self.assertEqual(error.exception.status, 409)


class ModelTests(unittest.TestCase):
    def model(self, response):
        def handle(request):
            self.assertEqual(str(request.url), ENDPOINT)
            self.assertEqual(request.headers["Authorization"], "Bearer test-key")
            body = json.loads(request.content)
            self.assertEqual(body["model"], MODEL)
            self.assertFalse(body["stream"])
            return response(request)
        client = FeishuHttpClient(timeout=2, retries=0, transport=httpx.MockTransport(handle))
        self.addCleanup(client.close)
        model = CustomModel(MemoryStore(), client=client, protect=lambda key: "encrypted:" + key, unprotect=lambda key: key.split(":", 1)[1])
        model.configure({"api_key": "test-key"})
        return model

    def test_success_settings_blank_retention_and_delete(self):
        model = self.model(lambda request: httpx.Response(200, json={"choices": [{"message": {"content": "<think>private thoughts</think>你好"}}]}))
        self.assertEqual(model.complete([{ "role": "user", "content": "hello"}]), "你好")
        public = model.configure({"api_key": "", "enabled": True})
        self.assertTrue(public["configured"])
        self.assertNotIn("test-key", json.dumps(public))
        self.assertFalse(model.configure({"clear_key": True})["configured"])
        with self.assertRaises(AssistantError): model.complete([])

    def test_remote_errors_are_redacted_and_no_automatic_retry(self):
        calls = []
        def fail(request):
            calls.append(1)
            return httpx.Response(401, json={"error": {"message": "test-key-sensitive"}})
        model = self.model(fail)
        with self.assertRaises(AssistantError) as captured: model.complete([])
        self.assertNotIn("test-key", str(captured.exception))
        self.assertEqual(len(calls), 1)

    def test_remote_failure_categories_are_clear_without_vendor_body_or_key(self):
        for status, expected in ((400, "配置无效"), (404, "配置无效"), (429, "请求较多"), (503, "服务暂时不可用")):
            model = self.model(lambda request: httpx.Response(status, json={"error": {"message": "test-key-sensitive"}}))
            with self.assertRaises(AssistantError) as captured:
                model.complete([])
            self.assertIn(expected, str(captured.exception))
            self.assertNotIn("test-key", str(captured.exception))

    def test_sensitive_or_invalid_reply(self):
        for text in ("身份证号110101199001011234", "北京市某街123号", "家庭住址见备注", "110101 1990 01 01 1234", "联系13812345678"):
            model = self.model(lambda request: httpx.Response(200, json={"choices": [{"message": {"content": text}}]}))
            self.assertEqual(model.complete([]), PRIVATE_REPLY)
        for data in ({"error": "secret"}, {"choices": []}, {"choices": [{"message": {"content": None}}]}):
            model = self.model(lambda request: httpx.Response(200, json=data))
            with self.assertRaises(AssistantError): model.complete([])

    @unittest.skipUnless(sys.platform == "win32", "Windows credential protection")
    def test_real_windows_credential_roundtrip(self):
        from lan_bitable_template_portal.lighthouse_ai import protect_key, unprotect_key
        value = "test-only-credential"
        encrypted = protect_key(value)
        self.assertNotIn(value, encrypted)
        self.assertEqual(unprotect_key(encrypted), value)

    def test_multiple_profiles_edit_switch_delete_and_legacy_migration(self):
        calls = []
        client = FeishuHttpClient(retries=0, transport=httpx.MockTransport(lambda request: (calls.append(request) or httpx.Response(200, json={"choices": [{"message": {"content": "ok"}}]}))))
        self.addCleanup(client.close)
        store = MemoryStore()
        store.put_document(NAMESPACE, "model", {"key_cipher": "encrypted:old-key", "enabled": True})
        model = CustomModel(store, client=client, protect=lambda key: "encrypted:" + key, unprotect=lambda key: key.split(":", 1)[1])
        self.assertTrue(model.settings()["configured"])
        self.assertNotIn("old-key", json.dumps(model.settings()))
        model.configure({"action": "upsert", "profile": {"id": "another", "name": "第二模型", "endpoint": "https://other.example/v1/chat/completions", "model": "model-b", "api_key": "new-key"}})
        model.configure({"action": "select", "id": "another"})
        model.complete([])
        self.assertEqual(str(calls[-1].url), "https://other.example/v1/chat/completions")
        self.assertEqual(calls[-1].headers["authorization"], "Bearer new-key")
        self.assertEqual(json.loads(calls[-1].content)["model"], "model-b")
        model.configure({"action": "upsert", "profile": {"id": "another", "name": "修改名称", "endpoint": "https://other.example/v1/chat/completions", "model": "model-c", "api_key": ""}})
        model.complete([])
        self.assertEqual(calls[-1].headers["authorization"], "Bearer new-key")
        with self.assertRaises(AssistantError):
            model.configure({"action": "upsert", "profile": {"id": "another", "name": "改接口", "endpoint": "https://new.example/v1/chat/completions", "model": "model-c", "api_key": ""}})
        model.configure({"action": "delete", "id": "another"})
        self.assertEqual(model.settings()["active_model_id"], "default")

    def test_invalid_endpoint_and_duplicate_model_names_do_not_overwrite_config(self):
        model = self.model(lambda request: httpx.Response(200, json={"choices": [{"message": {"content": "ok"}}]}))
        initial = model.settings()
        for url in ("http://example.com/v1/chat/completions", "https://user:pass@example.com/v1/chat/completions", "https://example.com/v1/chat/completions?key=x", "https://169.254.169.254/v1/chat/completions", "https://example.com/wrong-path"):
            with self.assertRaises(AssistantError):
                model.configure({"action": "upsert", "profile": {"id": "new", "name": "新模型", "endpoint": url, "model": "model", "api_key": "new-key"}})
        self.assertEqual(model.settings(), initial)
        with self.assertRaises(AssistantError):
            model.configure({"action": "upsert", "profile": {"id": "new", "name": initial["models"][0]["name"], "endpoint": ENDPOINT, "model": MODEL, "api_key": "test-key"}})


class SourcesAndRoutesTests(unittest.TestCase):
    def setUp(self):
        from lan_bitable_template_portal.state_store import LanPortalStateStore
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = LanPortalStateStore(db_path=Path(self.tmp.name) / "portal.sqlite3")
        self.store.put_document("polling_sop", "sop-a", {"name": "A楼柴发维保", "scope": "A", "steps": [{"content": "复核"}]})
        self.store.put_document("polling_sop", "sop-b", {"name": "B楼柴发维保", "scope": "B"})
        self.store.put_document("plan_convergence", "credentials", {"token": "never-send-this"})

    def test_local_sources_allowlist_and_permissions_before_limit(self):
        self.store.replace_repair_snapshot("repair_projects", records=[{"record_id": "r-b" + str(n), "scope_codes": ["B"], "fields": {"楼栋": "B楼", "名称": "柴发"}} for n in range(40)] + [{"record_id": "r-a", "scope_codes": ["A"], "fields": {"楼栋": "A楼", "名称": "柴发"}}])
        self.store.replace_repair_snapshot("repair_cmdb", records=[{"record_id": "global-device", "fields": {"设备名称": "柴发", "楼栋": ""}}])
        sources = LocalAssistantSources(self.store)
        result, warnings = sources("A楼柴发维保", ACTOR)
        serialized = json.dumps(result, ensure_ascii=False)
        self.assertIn("A楼柴发维保", serialized)
        self.assertIn("r-a", serialized)
        self.assertIn("global-device", serialized)
        self.assertNotIn("B楼柴发维保", serialized)
        self.assertNotIn("never-send-this", serialized)
        with self.assertRaises(AssistantError): sources("B楼柴发", ACTOR)
        self.assertEqual(codes("园区"), set("ABCDE"))
        self.assertEqual(codes("110站"), {"110"})
        self.assertEqual(codes("设备110功率"), set())

    def test_authenticated_routes_and_admin_settings(self):
        session = {"user": {"open_id": ACTOR["id"]}, "allowed_scopes": ["A"], "role": "building"}
        controller = SimpleNamespace(_current_session=lambda request: session,
            _auth_required_response=lambda: JSONResponse({"ok": False}, status_code=401),
            _request_base_url=lambda request: str(request.base_url).rstrip("/"))
        runtime = SimpleNamespace(state_store=self.store, auth_manager=SimpleNamespace(
            is_admin=lambda s: s.get("role") == "admin", session_scopes=lambda s: s["allowed_scopes"]))
        app = FastAPI()
        install_lighthouse_routes(app, controller, runtime)
        with TestClient(app, headers={"Origin": "http://testserver"}) as client:
            history = client.get("/api/assistant/conversation")
            self.assertEqual(history.status_code, 200, history.text)
            self.assertEqual(history.headers["cache-control"], "no-store")
            self.assertEqual(client.get("/api/assistant/settings").status_code, 403)
            self.assertEqual(client.delete("/api/assistant/conversation", headers={"Origin": "https://other.example"}).status_code, 403)
            self.assertEqual(client.delete("/api/assistant/conversation", headers={"Origin": ""}).status_code, 403)
            self.assertEqual(client.post("/api/assistant/chat", content='"not object"').status_code, 400)
            self.assertEqual(client.post("/api/assistant/chat", content="x" * 16001).status_code, 413)
            self.assertEqual(client.post("/api/assistant/compress").status_code, 404)
            session["role"] = "admin"
            self.assertEqual(client.get("/api/assistant/settings").status_code, 403)
            self.assertFalse(client.get("/api/assistant/conversation").json()["data"]["can_manage_settings"])
            for open_id in (LI_SHILONG_OPEN_ID, MA_JINYU_OPEN_ID, BUILDING_OPEN_ID_MAP["H"]):
                session["user"] = {"open_id": open_id}
                session["role"] = "building" if open_id == BUILDING_OPEN_ID_MAP["H"] else "admin"
                self.assertEqual(client.get("/api/assistant/settings").status_code, 200)
                self.assertTrue(client.get("/api/assistant/conversation").json()["data"]["can_manage_settings"])
                self.assertEqual(client.put("/api/assistant/settings", json={"action": "toggle", "enabled": True}).status_code, 200)
            session["user"] = {"open_id": "not-a-whitelisted-user", "name": "李世龙"}
            session["role"] = "admin"
            self.assertEqual(client.get("/api/assistant/settings").status_code, 403)
            self.assertEqual(client.put("/api/assistant/settings", json={"action": "toggle", "enabled": False}).status_code, 403)
            self.assertEqual(MODEL_SETTINGS_USERS, frozenset((LI_SHILONG_OPEN_ID, MA_JINYU_OPEN_ID, BUILDING_OPEN_ID_MAP["H"])))
            session["is_guest"] = True
            self.assertEqual(client.get("/api/assistant/conversation").status_code, 403)
            session.clear()
            controller._current_session = lambda request: None
            self.assertEqual(client.get("/api/assistant/conversation").status_code, 401)

    def test_multipart_limits_precede_file_storage_and_close_all_parts(self):
        from starlette.datastructures import UploadFile
        session = {"user": {"open_id": ACTOR["id"]}, "allowed_scopes": ["A"], "role": "building"}
        controller = SimpleNamespace(_current_session=lambda request: session,
            _auth_required_response=lambda: JSONResponse({"ok": False}, status_code=401),
            _request_base_url=lambda request: str(request.base_url).rstrip("/"))
        runtime = SimpleNamespace(state_store=self.store, auth_manager=SimpleNamespace(
            is_admin=lambda s: False, session_scopes=lambda s: s["allowed_scopes"]))
        app = FastAPI()
        install_lighthouse_routes(app, controller, runtime)
        closed = []
        original_close = UploadFile.close
        async def close(upload):
            closed.append(upload.filename)
            await original_close(upload)
        with TestClient(app, headers={"Origin": "http://testserver"}) as client, patch("lan_bitable_template_portal.lighthouse_files.LighthouseFiles.upload", return_value={"id": "a" * 32}) as save, patch.object(UploadFile, "close", close):
            response = client.post("/api/assistant/files", files=[("files", (str(n) + ".txt", b"a")) for n in range(11)])
            self.assertEqual(response.status_code, 413, response.text)
            save.assert_not_called()
            with patch("lan_bitable_template_portal.lighthouse_files.MAX_FILE_BYTES", 8):
                response = client.post("/api/assistant/files", files=[("files", ("small.txt", b"a")), ("files", ("large.txt", b"a" * 9))])
                self.assertEqual(response.status_code, 413, response.text)
                save.assert_not_called()
                self.assertIn("small.txt", closed)
                self.assertIn("large.txt", closed)
            with patch("lan_bitable_template_portal.lighthouse_files.MAX_BATCH_BYTES", 8):
                response = client.post("/api/assistant/files", files={"files": ("stream.txt", b"a" * 66000)})
                self.assertEqual(response.status_code, 413, response.text)
                save.assert_not_called()
            response = client.post("/api/assistant/files", files={"files": ("valid.txt", b"valid")})
            self.assertEqual(response.status_code, 200, response.text)
            save.assert_called_once()
            self.assertIn("valid.txt", closed)

    def test_cabinet_sources_match_frozen_floor_plan_and_use_real_storage_path(self):
        from lan_bitable_template_portal.cabinet_power_store import CabinetStore
        local = CabinetStore(Path(self.tmp.name) / "cabinet_power" / "buildings")
        config = {"scope": "A", "rooms": [{"id": "201", "total": 1}],
                  "inventory": [{"room": "201", "rack": "B01", "rack_type": "网络机柜", "state": "off", "positions": []}],
                  "power_baseline": {"201/B01": {"state": "test", "last_operation": "上测试电", "event_hashes": []}}}
        local.replace("A", config, [], [])
        result, _ = LocalAssistantSources(self.store)("A楼机柜上电数量", ACTOR)
        summary = next(x for x in result if "库存汇总" in x["title"])
        self.assertEqual(summary["data"]["counts"]["test"], 1)
        self.assertEqual(summary["data"]["counts"]["off"], 0)
        self.assertTrue(any("201 / B01" in x["title"] for x in result))

    def test_general_question_does_not_query_any_business_database(self):
        reader = LocalAssistantSources(self.store)
        with patch.object(reader, "_read", side_effect=AssertionError("must not query caches")):
            self.assertEqual(reader("你是什么模型?", ACTOR), ([], []))
            self.assertEqual(reader("你好", ACTOR), ([], []))

    def test_one_unavailable_table_does_not_block_other_authorized_sources(self):
        reader = LocalAssistantSources(self.store)
        original = reader._read
        def unavailable(path, sql, params=()):
            if "ongoing_items" in sql:
                raise AssistantError("查询超时", 503)
            return original(path, sql, params)
        with patch.object(reader, "_read", side_effect=unavailable):
            results, warnings = reader("A楼柴发维保", ACTOR)
        self.assertTrue(any("A楼柴发维保" in x["title"] for x in results))
        self.assertTrue(any("不能据此计算全量数量" in w for w in warnings))
        self.assertNotIn("B楼柴发维保", json.dumps(results, ensure_ascii=False))

    def test_incomplete_cabinet_snapshot_never_becomes_zero_or_baseline_only_count(self):
        reader = LocalAssistantSources(self.store)
        original = reader._read
        def unavailable(path, sql, params=()):
            if "FROM inventory" in sql or "FROM records" in sql:
                self.assertIn("UNION ALL", sql)
                raise AssistantError("操作历史读取超时", 503)
            return original(path, sql, params)
        with patch.object(reader, "_read", side_effect=unavailable):
            results, warnings = reader("A楼机柜上电数量", ACTOR)
        self.assertFalse(any("库存汇总" in x["title"] for x in results))
        self.assertTrue(any("未使用该楼机柜状态或数量" in w for w in warnings))

    def test_person_scope_is_checked_before_projection_and_unknown_scope_is_not_shared(self):
        self.store.put_document("signature_management_person", "known-person", {"name": "范围测试人员", "scope_codes": ["B"], "工号": "E12345"})
        self.store.put_document("signature_management_person", "unknown-person", {"name": "区域测试人员", "area": "未知站点", "工号": "E54321"})
        self.store.put_document("signature_management_person", "global-person", {"name": "公开测试人员", "工号": "E99999"})
        result, _ = LocalAssistantSources(self.store)("测试人员", ACTOR)
        text = json.dumps(result, ensure_ascii=False)
        self.assertNotIn("范围测试人员", text)
        self.assertNotIn("区域测试人员", text)
        self.assertIn("公开测试人员", text)


if __name__ == "__main__":
    unittest.main()
