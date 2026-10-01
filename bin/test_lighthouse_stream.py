"""Stream, permission and SDK checks using isolated stores and synthetic APIs."""
import asyncio
import copy
import json
import sys
import tempfile
import unittest
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fastapi import FastAPI, Request
from pydantic_ai.models.function import DeltaToolCall, FunctionModel
from lan_bitable_template_portal.lighthouse_ai import AssistantError, LighthouseAssistant
from lan_bitable_template_portal.lighthouse_agent import PortalAgent
from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog
from lan_bitable_template_portal.lighthouse_files import LighthouseFiles
from lan_bitable_template_portal.lighthouse_model import LighthouseModel, PublicText, public_catalog, read_scope_operations, scoped_operation, scoped_result
from lan_bitable_template_portal.lighthouse_stream import LighthouseStream, failure_detail
from lan_bitable_template_portal.lighthouse_sources import SCOPES


ACTOR = {"id": "fixture-d", "scopes": ["D"], "is_admin": False}


class Store:
    def __init__(self, path):
        self.db_path, self.docs = path, {}
    def get_document(self, namespace, key):
        return copy.deepcopy(self.docs.get((namespace, key)))
    def put_document(self, namespace, key, value):
        self.docs[namespace, key] = copy.deepcopy(value)
    def list_documents(self, namespace, *, key_prefix=""):
        return [{"key": key, "payload": copy.deepcopy(value)} for (space, key), value in self.docs.items() if space == namespace and key.startswith(key_prefix)]


class Engine:
    def __init__(self):
        self.calls = []
        self.gate = asyncio.Event()
        self.started = asyncio.Event()
    async def answer(self, actor, turn, history, request, emit, authorize, context):
        self.calls.append({"actor": actor, "turn": turn, "history": history})
        await emit("status", {"label": "正在查询测试数据"})
        await emit("text", {"delta": "处理中。"})
        self.started.set()
        if "等待" in turn["question"]:
            await self.gate.wait()
        await emit("text", {"delta": "范围：" + ",".join(actor["scopes"])})
        return {"answer": "范围：" + ",".join(actor["scopes"]), "sources": []}
    async def summarize(self, actor, turns, previous, profile):
        return "测试摘要，未更改业务数据。"


class StreamTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = Store(Path(self.tmp.name) / "state.sqlite3")
        self.model = Mock()
        self.model.settings.return_value = {"configured": True, "enabled": True, "active_model_id": "test", "models": [{"id": "test", "name": "测试", "model": "fixture", "configured": True}]}
        self.model.profile.return_value = {"id": "test", "name": "测试", "model": "fixture"}
        self.assistant = LighthouseAssistant(self.store, Mock(return_value=([], [])), model=self.model)
        self.app = FastAPI()
        self.reads = []
        @self.app.get("/api/repair-management/records")
        async def records(scope: str = "ALL"):
            self.reads.append(scope)
            return {"ok": True, "data": {"scope": scope, "total": 2, "records": [{"record_id": "repair1", "scope": scope, "title": "测试维修"}]}}
        self.portal = PortalAgent(self.assistant, PortalAPICatalog(self.app), LighthouseFiles(self.store))
        self.engine = Engine()
        self.runtime = LighthouseStream(self.portal, engine=self.engine)
        self.actor = copy.deepcopy(ACTOR)
        self.request = Request({"type": "http", "scheme": "http", "server": ("testserver", 80), "client": ("127.0.0.1", 1), "path": "/api/assistant/messages", "root_path": "", "query_string": b"", "headers": [(b"origin", b"http://testserver")]})
        self.counter = 0
    async def asyncTearDown(self):
        await self.runtime.close()
    async def authorize(self):
        return copy.deepcopy(self.actor)
    def payload(self, question, **kwargs):
        self.counter += 1
        return {"question": question, "file_ids": [], "operation_id": "fixture_message_%04d" % self.counter,
                "conversation_id": self.assistant._state(self.actor)["id"], **kwargs}
    async def submit(self, question, **kwargs):
        return await self.runtime.submit(self.actor, self.payload(question, **kwargs), self.request, self.authorize)
    async def finish(self):
        await asyncio.wait_for(asyncio.gather(*self.runtime.workers.values(), return_exceptions=True), 8)

    async def test_default_scope_is_actor_scope_and_wire_is_sdk_stream(self):
        ack = await self.submit("你好")
        await self.finish()
        self.assertEqual(self.engine.calls[0]["actor"]["scopes"], ["D"])
        chunks = [chunk async for chunk in self.runtime.stream(self.actor, ack["run_id"], self.authorize)]
        text = "".join(chunks)
        self.assertIn('"type":"text-delta"', text)
        self.assertIn('"type":"data-turn"', text)
        self.assertTrue(text.endswith("data: [DONE]\n\n"))
        self.assertFalse((await self.runtime.conversation(self.actor))["busy"])

    async def test_supplement_narrows_all_to_d_and_old_reply_cannot_win(self):
        self.actor["scopes"] = sorted(SCOPES)
        first = await self.submit("等待查询多少未发检修")
        await asyncio.wait_for(self.engine.started.wait(), 5)
        second = await self.submit("只看D楼")
        await self.finish()
        state = await self.runtime.conversation(self.actor)
        self.assertEqual(self.engine.calls[-1]["actor"]["scopes"], ["D"])
        self.assertIn("多少未发检修", self.engine.calls[-1]["turn"]["prompt"])
        self.assertEqual(state["turns"][0]["status"], "superseded")
        self.assertEqual(state["turns"][-1]["answer"], "范围：D")
        self.assertNotEqual(first["run_id"], second["run_id"])

    async def test_denied_named_building_never_calls_engine(self):
        with self.assertRaises(AssistantError) as error:
            await self.submit("只看E楼")
        self.assertEqual(error.exception.status, 403)
        self.assertEqual(self.engine.calls, [])
        with self.assertRaises(AssistantError):
            self.actor["scopes"] = ["H"]
            await self.submit("只看园区")

    async def test_private_question_is_refused_without_model_or_business_read(self):
        @asynccontextmanager
        async def forbidden_factory(*args):
            raise AssertionError("Private question must not contact the model")
            yield
        self.runtime.engine = LighthouseModel(self.portal, model_factory=forbidden_factory)
        payload = self.payload("查询人员的身份证号和家庭住址")
        first = await self.runtime.submit(self.actor, payload, self.request, self.authorize)
        await self.finish()
        reply = (await self.runtime.conversation(self.actor))["turns"][-1]
        self.assertEqual(reply["status"], "completed")
        self.assertIn("不能提供", reply["answer"])
        self.assertEqual(self.reads, [])
        again = await self.runtime.submit(self.actor, payload, self.request, self.authorize)
        self.assertEqual(again["run_id"], first["run_id"])

    async def test_terminal_turn_rejects_stale_flush(self):
        ack = await self.submit("你好")
        await self.finish()
        run = self.runtime.get_run(self.actor, ack["run_id"])
        self.assertFalse(self.runtime._patch_turn(self.actor, run, {"status": "stopped", "answer": "stale"}, finish=True))
        self.assertEqual((await self.runtime.conversation(self.actor))["turns"][-1]["answer"], "范围：D")

    async def test_restart_recovers_downloads_without_losing_input_files(self):
        uploaded = self.portal.files.upload(self.actor, "source.txt", b"original source")
        downloaded = self.portal.files.upload(self.actor, "result.txt", b"authorized result")
        run, _ = self.runtime._accept(self.actor, self.payload("处理附件", file_ids=[uploaded["id"]]))
        public_file = self.portal.files.public(self.portal.files.get(self.actor, downloaded["id"]))
        run.update(status="completed", answer="文件已生成", events=[{"type": "data-turn", "data": {"output_files": [public_file], "sources": []}}])
        self.runtime._save_run(run)
        self.runtime = LighthouseStream(self.portal, engine=self.engine)
        recovered = await self.runtime.conversation(self.actor)
        self.assertEqual(recovered["turns"][-1]["output_files"], [public_file])
        internal = self.assistant._state(self.actor)["turns"][-1]
        self.assertEqual(internal["file_ids"], [uploaded["id"], downloaded["id"]])
        self.assertEqual(internal["status"], "completed")
        self.assertFalse(recovered["active_run_id"])

    async def test_permission_reduction_removes_old_context_and_stream_access(self):
        self.actor["scopes"] = sorted(SCOPES)
        first = await self.submit("只看D楼")
        await self.finish()
        self.actor["scopes"] = ["D"]
        self.assertEqual((await self.runtime.conversation(self.actor))["turns"], [])
        with self.assertRaises(AssistantError):
            self.runtime.get_run(self.actor, first["run_id"])
        await self.submit("你好")
        await self.finish()
        self.assertEqual(self.engine.calls[-1]["history"], [])

    async def test_duplicate_submission_and_unknown_retry_reuse_attempt(self):
        payload = self.payload("你好")
        first = await self.runtime.submit(self.actor, payload, self.request, self.authorize)
        await self.finish()
        second = await self.runtime.submit(self.actor, payload, self.request, self.authorize)
        self.assertEqual(first["run_id"], second["run_id"])
        self.assertEqual(len(self.engine.calls), 1)
        payload["attempt_id"] = "another_attempt_0001"
        third = await self.runtime.submit(self.actor, payload, self.request, self.authorize)
        await self.finish()
        self.assertNotEqual(third["run_id"], first["run_id"])
        self.assertEqual(len((await self.runtime.conversation(self.actor))["turns"]), 1)

    async def test_stop_keeps_partial_and_stale_stop_does_not_cancel_new_run(self):
        first = await self.submit("等待")
        await asyncio.wait_for(self.engine.started.wait(), 5)
        await self.runtime.stop(self.actor, first["run_id"])
        self.assertEqual((await self.runtime.conversation(self.actor))["turns"][-1]["status"], "stopped")
        self.assertIn("处理中", (await self.runtime.conversation(self.actor))["turns"][-1]["answer"])
        second = await self.submit("等待第二个")
        await self.runtime.stop(self.actor, first["run_id"])
        self.assertFalse(self.runtime.workers[self.actor["id"]].done())
        self.engine.gate.set()
        await self.finish()
        self.assertEqual(self.runtime.get_run(self.actor, second["run_id"])["status"], "completed")

    async def test_reconnect_replays_without_running_model_twice_and_owner_checked(self):
        ack = await self.submit("你好")
        await self.finish()
        first = [chunk async for chunk in self.runtime.stream(self.actor, ack["run_id"], self.authorize)]
        second = [chunk async for chunk in self.runtime.stream(self.actor, ack["run_id"], self.authorize)]
        self.assertEqual(first, second)
        self.assertEqual(len(self.engine.calls), 1)
        with self.assertRaises(AssistantError):
            self.runtime.get_run({**self.actor, "id": "other"}, ack["run_id"])
        with self.assertRaises(AssistantError):
            self.runtime.get_run({**self.actor, "scopes": ["A"]}, ack["run_id"])

    async def test_restart_marks_interrupted_and_never_replays_business(self):
        ack = await self.submit("等待")
        await asyncio.wait_for(self.engine.started.wait(), 5)
        self.runtime.workers[self.actor["id"]].cancel()
        await self.finish()
        state = self.assistant._state(self.actor)
        state["active_run_id"] = ack["run_id"]
        state["turns"][-1]["status"] = "pending"
        self.store.put_document("lighthouse_ai", self.assistant._key(self.actor), state)
        runtime = LighthouseStream(self.portal, engine=Engine())
        self.assistant._active.clear()
        restored = await runtime.conversation(self.actor)
        self.assertEqual(restored["turns"][-1]["status"], "stopped")
        self.assertEqual(runtime.engine.calls, [])

    async def test_model_switch_changes_next_run_not_current_profile(self):
        await self.submit("等待")
        await asyncio.wait_for(self.engine.started.wait(), 5)
        self.model.profile.return_value = {"id": "second", "name": "第二模型", "model": "second"}
        result = self.assistant.select_model(self.actor, {"conversation_id": self.assistant._state(self.actor)["id"], "model_id": "second"})
        self.assertTrue(result["busy"])
        self.assertEqual(self.engine.calls[0]["turn"]["_profile"]["id"], "test")

    async def test_real_pydantic_function_tools_and_streaming(self):
        seen = []
        async def model_stream(messages, info):
            seen.append(messages)
            if not any(getattr(p, "part_kind", "") == "tool-return" for m in messages for p in m.parts):
                yield "让我先查接口，用户的问题还需要分类。"
                yield {0: DeltaToolCall(name="query", json_args=json.dumps({"operation": {"api_id": "GET /api/repair-management/records", "params": {"scope": "ALL"}}}))}
            else:
                yield "D楼共有2项维修。"
        @asynccontextmanager
        async def factory(custom, profile):
            yield FunctionModel(stream_function=model_stream)
        self.runtime.engine = LighthouseModel(self.portal, model_factory=factory)
        errors = []
        original_answer = self.runtime.engine.answer
        async def checked_answer(*args):
            try:
                return await original_answer(*args)
            except Exception as exc:
                errors.append(exc)
                raise
        self.runtime.engine.answer = checked_answer
        ack = await self.submit("查询D楼维修记录")
        await self.finish()
        if errors:
            raise errors[0]
        turn = (await self.runtime.conversation(self.actor))["turns"][-1]
        self.assertEqual(turn["status"], "completed", turn)
        self.assertIn("2项", turn["answer"])
        self.assertEqual(self.reads, ["D"])
        self.assertEqual(len(seen), 2)
        self.assertTrue(turn["sources"])
        self.assertNotIn("让我先", turn["answer"])
        wire = "".join([chunk async for chunk in self.runtime.stream(self.actor, ack["run_id"], self.authorize)])
        self.assertNotIn("让我先", wire)
        self.assertTrue(turn["process"])

    async def test_safe_process_is_bounded_persisted_and_replayed_without_reasoning(self):
        async def answer(actor, turn, history, request, emit, authorize, context):
            for i in range(30):
                await emit("status", {"label": "已核对模块" + str(i)})
            await emit("status", {"label": "已核对模块29"})
            return {"answer": "查询完成。", "sources": []}
        self.engine.answer = answer
        ack = await self.submit("查询未完成工作")
        await self.finish()
        turn = (await self.runtime.conversation(self.actor))["turns"][-1]
        self.assertEqual(len(turn["process"]), 24)
        self.assertEqual(turn["process"][-1]["label"], "已核对模块29")
        restored = LighthouseStream(self.portal, engine=self.engine)
        self.assertEqual((await restored.conversation(self.actor))["turns"][-1]["process"], turn["process"])
        wire = "".join([chunk async for chunk in restored.stream(self.actor, ack["run_id"], self.authorize)])
        self.assertIn('"process":', wire)
        await restored.close()

    async def test_failure_is_actionable_and_does_not_log_provider_body(self):
        async def fail(*args):
            raise RuntimeError("api_key=must-not-appear-in-log")
        self.engine.answer = fail
        with self.assertLogs("lan_bitable_template_portal.lighthouse_stream", level="WARNING") as log:
            ack = await self.submit("你好")
            await self.finish()
        turn = (await self.runtime.conversation(self.actor))["turns"][-1]
        self.assertEqual(turn["status"], "failed")
        self.assertEqual(turn["error_code"], "query_adapter")
        self.assertIn(ack["run_id"][:8], turn["error"])
        self.assertNotIn("must-not-appear", "\n".join(log.output) + turn["error"])
        from pydantic_ai.exceptions import UnexpectedModelBehavior, UsageLimitExceeded
        for exc, expected in ((UnexpectedModelBehavior("bad"), "model_protocol"), (UsageLimitExceeded("too many"), "model_protocol"), (TimeoutError(), "timeout")):
            self.assertEqual(failure_detail(exc)[0], expected)

    def test_private_identifiers_never_cross_stream_chunks(self):
        text = PublicText()
        self.assertEqual(text.push("身份证号：110101199"), "")
        self.assertNotIn("110101", text.push("003079876。"))
        self.assertEqual(text.push("**普通"), "")
        self.assertIn("普通内容", text.push("内容**。"))

    def test_model_catalog_keeps_signer_schema_not_images_or_credentials(self):
        from clipflow_backend.api_models import DrillExecutionRequest, CriticalGuardResponseRequest
        schema = DrillExecutionRequest.model_json_schema()
        schema["properties"]["api_key"] = {"type": "string", "default": "must-not-leak"}
        schema["properties"]["evaluator"]["examples"] = [{"private": "must-not-leak"}]
        public = public_catalog({"items": [{"schema": {"body": schema}}, {"schema": {"body": CriticalGuardResponseRequest.model_json_schema()}}]})
        props = public["items"][0]["schema"]["body"]["properties"]
        self.assertIn("signature_time", props)
        self.assertIn("step_signers", props)
        self.assertIn("signatures", public["items"][1]["schema"]["body"]["properties"])
        self.assertNotIn("must-not-leak", json.dumps(public))
        self.assertNotIn("api_key", props)

    def test_scoped_operation_injects_scope_and_rejects_out_of_scope(self):
        desc = {"schema": {"query": {"properties": {"scope": {"type": "string"}}}}}
        self.assertEqual(scoped_operation({"api_id": "GET /x"}, desc, self.actor)["params"]["scope"], "D")
        with self.assertRaises(AssistantError):
            scoped_operation({"params": {"scope": "E"}}, desc, self.actor)
        for alias in ("ALL", "CAMPUS"):
            with self.subTest(alias=alias), self.assertRaises(AssistantError):
                scoped_operation({"params": {"scope": alias}}, desc, {**self.actor, "scopes": ["A", "D"]})

    def test_shared_record_must_intersect_query_and_be_fully_authorised(self):
        shared = {"building_codes": ["D", "E"], "title": "跨楼事项"}
        scoped_result(shared, {**self.actor, "allowed_scopes": list(SCOPES)})
        with self.assertRaises(AssistantError):
            scoped_result(shared, self.actor)
        with self.assertRaises(AssistantError):
            scoped_result({"building_codes": ["E"]}, {**self.actor, "allowed_scopes": list(SCOPES)})

    def test_single_building_reads_split_but_writes_and_details_do_not(self):
        desc = {"scope_mode": "single", "scope_values": list("ABCDEH"), "schema": {"query": ["scope"]}}
        actor = {**self.actor, "scopes": ["A", "D"]}
        operations = read_scope_operations({"params": {"scope": "ALL", "date": "2026-10-01"}}, desc, actor)
        self.assertEqual([op["params"] for op in operations], [{"scope": "A", "date": "2026-10-01"}, {"scope": "D", "date": "2026-10-01"}])
        self.assertEqual(read_scope_operations({}, desc, self.actor)[0]["params"]["scope"], "D")
        with self.assertRaises(AssistantError):
            read_scope_operations({"params": {"scope": "E"}}, desc, actor)
        with self.assertRaises(AssistantError):
            read_scope_operations({"path_params": {"id": "rec1"}}, desc, actor)
        with self.assertRaises(AssistantError):
            scoped_operation({"body": {"scope": "ALL"}}, desc, {**actor, "scopes": list(SCOPES)})
        write_desc = {**desc, "scope_section": "body", "schema": {"query": ["scope"], "body": {"properties": {"scope": {"type": "string"}}}}}
        write = scoped_operation({"body": {"scope": "D"}}, write_desc, {**actor, "scopes": list(SCOPES)})
        self.assertEqual(write["body"]["scope"], "D")
        self.assertNotIn("scope", write["params"])

    def test_learning_scopes_are_narrower_than_general_building_permissions(self):
        actor = {**self.actor, "scopes": sorted(SCOPES), "learning_scopes": ["H"]}
        desc = {"scope_mode": "single", "scope_values": list("ABCDEH"), "schema": {"query": ["scope"]}}
        operations = read_scope_operations({"api_id": "GET /api/learning/papers", "params": {"scope": "ALL"}}, desc, actor)
        self.assertEqual([op["params"]["scope"] for op in operations], ["H"])
        with self.assertRaises(AssistantError):
            scoped_operation({"api_id": "POST /api/learning/papers/{id}/answer", "params": {"scope": "D"}}, desc, actor)
        with self.assertRaises(AssistantError):
            read_scope_operations({"api_id": "GET /api/learning/papers"}, desc, {**actor, "learning_scopes": []})


if __name__ == "__main__":
    unittest.main()
