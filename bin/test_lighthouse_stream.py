"""Stream, permission and SDK checks using isolated stores and synthetic APIs."""
import asyncio
import copy
import json
import sys
import tempfile
import unittest
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

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

    async def test_completed_runs_release_private_history_and_checkpoint_less_often(self):
        async def answer(actor, turn, history, request, emit, authorize, context):
            for value in range(10):
                await emit('text', {'delta': str(value)})
                await asyncio.sleep(.08)
            return {'answer': '0123456789', 'sources': []}
        self.engine.answer = answer
        writes = []
        save = self.runtime._save_run
        def observe(run):
            writes.append(run['revision'])
            return save(run)
        with patch.object(self.runtime, '_save_run', side_effect=observe):
            ack = await self.submit('测试流式回答')
            await self.finish()
        run = self.runtime.get_run(self.actor, ack['run_id'])
        self.assertEqual(len(writes), 3)  # accepted, first partial, terminal
        self.assertEqual(run['answer'], '0123456789')
        self.assertTrue(all(key not in run for key in ('_turn', '_history', '_context')))
        saved = self.store.get_document('lighthouse_runs', ack['run_id'])
        self.assertEqual(saved['answer'], '0123456789')
        replay = ''.join([chunk async for chunk in self.runtime.stream(self.actor, ack['run_id'], self.authorize)])
        self.assertIn('0123456789', replay)

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

    async def test_network_confirmation_keeps_completed_public_question(self):
        question = '讲一讲deepseek和豆包的优劣点'
        await self.submit(question)
        await self.finish()
        await self.submit('需要联网')
        await self.finish()
        turn = self.engine.calls[-1]['turn']
        self.assertEqual(turn['question'], '需要联网')
        self.assertIn(question, turn['prompt'])
        self.assertEqual(self.engine.calls[-1]['actor']['scopes'], ['D'])
        self.assertEqual(self.reads, [])

    async def test_denied_named_building_never_calls_engine(self):
        with self.assertRaises(AssistantError) as error:
            await self.submit("只看E楼")
        self.assertEqual(error.exception.status, 403)
        self.assertEqual(self.engine.calls, [])
        with self.assertRaises(AssistantError):
            self.actor["scopes"] = ["H"]
            await self.submit("只看园区")

    async def test_new_question_does_not_inherit_previous_notice_scope(self):
        self.actor.update(scopes=sorted(SCOPES), is_admin=True)
        await self.submit("查询E楼维保通告")
        await self.finish()
        self.assertEqual(self.engine.calls[-1]["actor"]["scopes"], ["E"])
        await self.submit("今天有多少条进行中的通告")
        await self.finish()
        self.assertEqual(self.engine.calls[-1]["actor"]["scopes"], sorted(SCOPES))

    async def test_explicit_followup_keeps_scope_but_new_subject_resets_it(self):
        self.actor.update(scopes=sorted(SCOPES), is_admin=True)
        await self.submit("查询D楼未结束通告")
        await self.finish()
        await self.submit("看明细")
        await self.finish()
        self.assertEqual(self.engine.calls[-1]["actor"]["scopes"], ["D"])
        await self.submit("今天发生了多少事件")
        await self.finish()
        self.assertEqual(self.engine.calls[-1]["actor"]["scopes"], sorted(SCOPES))

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

    async def test_selected_commands_freeze_and_retries_do_not_change_input(self):
        payload = self.payload('你好', commands=[{'kind': 'skill', 'id': 'lighthouse-repairs'}])
        first = await self.runtime.submit(self.actor, payload, self.request, self.authorize)
        await self.finish()
        self.assertEqual(first['turn']['commands'][0]['id'], 'lighthouse-repairs')
        self.assertNotIn('_command_context', first['turn'])
        self.assertTrue(self.engine.calls[-1]['turn']['_command_context'][0]['content'])
        again = await self.runtime.submit(self.actor, payload, self.request, self.authorize)
        self.assertEqual(again['run_id'], first['run_id'])
        with self.assertRaises(AssistantError):
            await self.runtime.submit(self.actor, {**payload, 'commands': [{'kind': 'tool', 'id': 'weather'}]}, self.request, self.authorize)

    async def test_refinement_inherits_command_but_scope_uses_plain_question(self):
        self.actor['scopes'] = sorted(SCOPES)
        await self.submit('等待查询未发检修', commands=[{'kind': 'skill', 'id': 'lighthouse-repairs'}])
        await asyncio.wait_for(self.engine.started.wait(), 5)
        await self.submit('只看D楼')
        await self.finish()
        self.assertEqual(self.engine.calls[-1]['actor']['scopes'], ['D'])
        self.assertEqual(self.engine.calls[-1]['turn']['commands'][0]['id'], 'lighthouse-repairs')
        self.assertEqual(self.engine.calls[-1]['turn']['submitted_commands'], [])

    async def test_unknown_command_does_not_supersede_pending_turn(self):
        first = await self.submit('等待')
        await asyncio.wait_for(self.engine.started.wait(), 5)
        with self.assertRaises(AssistantError):
            await self.submit('新问题', commands=[{'kind': 'tool', 'id': 'DELETE /api/bitable'}])
        self.assertEqual(self.assistant._state(self.actor)['active_run_id'], first['run_id'])
        self.assertEqual(len(self.engine.calls), 1)

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

    async def test_public_weather_followup_passes_only_the_prior_user_question(self):
        public = SimpleNamespace(weather=AsyncMock(return_value={"ok": True, "city": "南通",
            "sourceURL": "https://wttr.in/Nantong", "queried_at": "2026-10-03T10:00:00+08:00",
            "capability": {"structured_weather": True}, "daily": [{"date": "2026-10-04", "max": 22, "min": 18}]}))
        seen = []
        async def model_stream(messages, info):
            seen.append(messages)
            if not any(getattr(part, "part_kind", "") == "tool-return" for message in messages for part in message.parts):
                yield {0: DeltaToolCall(name="weather", json_args=json.dumps({"city": "南通"}))}
            else:
                yield "南通明天18至22度。[1]"
        @asynccontextmanager
        async def factory(custom, profile):
            yield FunctionModel(stream_function=model_stream)
        engine = LighthouseModel(self.portal, model_factory=factory, public_sources=public)
        async def emit(*_):
            pass
        result = await engine.answer(self.actor, {"question": "明天呢", "operation_id": "fixture-weather-2",
            "_profile": self.model.profile.return_value}, [{"question": "南通今天天气怎么样", "answer": "旧回答不是实时事实",
                "scopes": ["D"], "status": "completed"}], self.request, emit, self.authorize, {})
        public.weather.assert_awaited_once_with("南通", "明天呢", previous_question="南通今天天气怎么样")
        self.assertEqual(self.reads, [])
        self.assertEqual(seen, [], 'Simple weather must not wait for a model/gateway round')
        self.assertTrue(result["sources"])

    async def test_selected_weather_tool_receives_context_without_rewriting_question(self):
        public = SimpleNamespace(weather=AsyncMock(return_value={'ok': True, 'city': '南通',
            'sourceURL': 'https://example.com/weather', 'queried_at': '2026-10-05T10:00:00+08:00',
            'daily': [{'date': '2026-10-05', 'max': 22, 'min': 18}]}))
        seen = []
        async def model_stream(messages, info):
            seen.append(messages)
            if not any(getattr(part, 'part_kind', '') == 'tool-return' for message in messages for part in message.parts):
                yield {0: DeltaToolCall(name='weather', json_args=json.dumps({'city': '南通'}))}
            else:
                yield '南通18至22度。[1]'
        @asynccontextmanager
        async def factory(custom, profile):
            yield FunctionModel(stream_function=model_stream)
        engine = LighthouseModel(self.portal, model_factory=factory, public_sources=public)
        async def emit(*_):
            pass
        result = await engine.answer(self.actor, {'question': '南通', 'operation_id': 'fixture-selected-weather',
            'commands': [{'kind': 'tool', 'id': 'weather'}], '_command_context': [{'kind': 'tool', 'name': 'weather'}],
            '_profile': self.model.profile.return_value}, [], self.request, emit, self.authorize, {})
        public.weather.assert_awaited_once_with('南通', '南通')
        self.assertTrue(result['sources'])
        self.assertEqual(seen, [], 'Selecting the weather tool must keep the native fast path')
        self.assertEqual(self.reads, [])

    async def test_network_followup_can_search_its_original_public_subject(self):
        from openclaw_service.assistant.lighthouse_public import _ensure_public_query
        original = '讲一讲deepseek和豆包的优劣点'
        captured = []
        async def search(query, question):
            _ensure_public_query(query, question)
            captured.append(question)
            return {'ok': True, 'queried_at': '2026-10-06T10:00:00+08:00', 'results': [
                {'url': 'https://example.com/models', 'title': '测试公开资料', 'snippet': '模型比较测试资料'}]}
        async def model_stream(messages, info):
            if not any(getattr(part, 'part_kind', '') == 'tool-return' for message in messages for part in message.parts):
                yield {0: DeltaToolCall(name='public_search', json_args=json.dumps({'query': 'DeepSeek Doubao latest model comparison 2026'}))}
            else:
                yield '已取得本轮公开比较资料。[1]'
        @asynccontextmanager
        async def factory(*_):
            yield FunctionModel(stream_function=model_stream)
        engine = LighthouseModel(self.portal, model_factory=factory, public_sources=SimpleNamespace(search=search))
        result = await engine.answer(self.actor, {'question': '需要联网',
            'prompt': '之前的问题：' + original + '\n本次补充（采用最新条件）：需要联网',
            'operation_id': 'network-followup-test', '_profile': self.model.profile.return_value},
            [], self.request, AsyncMock(), self.authorize, {})
        self.assertEqual(len(captured), 1)
        self.assertIn(original, captured[0])
        self.assertTrue(result['sources'])
        self.assertNotIn('本轮实时资料未取得', result['answer'])
        self.assertEqual(self.reads, [])

    async def test_public_search_reuses_results_and_bounds_each_turn_independently(self):
        public = SimpleNamespace(search=AsyncMock(return_value={'ok': True, 'queried_at': '2026-10-06T10:00:00+08:00',
            'results': [{'url': 'https://docs.python.org/3/tutorial/controlflow.html',
                        'title': 'Python Control Flow Tools', 'snippet': 'Python if and for statements.'}]}))
        queries = ['Python control flow', '  PYTHON   control flow ', 'Python control flow docs',
                   'Python control flow official', 'Python control flow tutorial']
        replies = []
        async def model_stream(messages, info):
            responses = [part for message in messages for part in message.parts
                         if getattr(part, 'part_kind', '') == 'tool-return']
            if responses:
                replies.append(responses[-1].content)
            if len(responses) < len(queries):
                yield {0: DeltaToolCall(name='public_search', json_args=json.dumps({'query': queries[len(responses)]}))}
            else:
                yield 'Python 控制流包含 if 和 for。[1]'
        @asynccontextmanager
        async def factory(*_):
            yield FunctionModel(stream_function=model_stream)
        engine = LighthouseModel(self.portal, model_factory=factory, public_sources=public)
        async def emit(*_): pass
        for run in range(2):
            replies.clear()
            before = public.search.await_count
            result = await engine.answer(self.actor, {'question': '请联网搜索 Python control flow 的官方文档',
                'operation_id': f'public-fixture-{run}', '_profile': self.model.profile.return_value},
                [], self.request, emit, self.authorize, {})
            self.assertIn('if', result['answer'])
            self.assertEqual(len(result['sources']), 1)
            self.assertEqual(public.search.await_count - before, 3)
            self.assertEqual(len(replies), 5)
            self.assertTrue(replies[1]['no_new_results'])
            self.assertTrue(replies[2]['no_new_results'])
            self.assertTrue(replies[-1]['stop_searching'])
            self.assertFalse(replies[-1]['ok'])
        self.assertEqual(self.reads, [])

    async def test_public_search_timeout_cancels_request_and_prevents_another_fetch(self):
        cancelled = []
        async def slow_search(*_):
            try:
                await asyncio.sleep(5)
            finally:
                cancelled.append(True)
        public = SimpleNamespace(search=AsyncMock(side_effect=slow_search))
        replies = []
        async def model_stream(messages, info):
            responses = [part for message in messages for part in message.parts
                         if getattr(part, 'part_kind', '') == 'tool-return']
            if responses:
                replies.append(responses[-1].content)
            if len(responses) < 2:
                yield {0: DeltaToolCall(name='public_search', json_args='{"query":"Python control flow"}')}
            else:
                yield '资料未取得，无法核实。'
        @asynccontextmanager
        async def factory(*_):
            yield FunctionModel(stream_function=model_stream)
        engine = LighthouseModel(self.portal, model_factory=factory, public_sources=public)
        async def emit(*_): pass
        with patch('openclaw_service.assistant.lighthouse_public.MAX_NETWORK_SECONDS', .03):
            result = await engine.answer(self.actor, {'question': '请联网搜索 Python control flow 的官方文档',
                'operation_id': 'public-timeout-fixture', '_profile': self.model.profile.return_value},
                [], self.request, emit, self.authorize, {})
        self.assertEqual(public.search.await_count, 1)
        self.assertEqual(cancelled, [True])
        self.assertTrue(all(reply['stop_searching'] for reply in replies))
        self.assertIn('时限', result['answer'])
        self.assertFalse(result['sources'])
        self.assertEqual(self.reads, [])

    async def test_public_page_navigates_actual_links_and_caches_the_original_url(self):
        index, chapter = 'https://docs.python.org/3/contents.html', 'https://docs.python.org/3/tutorial/controlflow.html'
        async def read_page(url, question):
            return {'ok': True, 'sourceURL': url, 'queried_at': '2026-10-06T10:00:00+08:00',
                'title': 'Control Flow Tools' if url == chapter else 'Python contents',
                'text': 'Use if and for.' if url == chapter else 'Contents index, not chapter text.',
                'links': [{'label': 'Control Flow Tools', 'link': chapter}] if url == index else []}
        public = SimpleNamespace(search=AsyncMock(return_value={'ok': False,
            'navigation': [{'title': 'Python contents', 'link': index}]}), page=AsyncMock(side_effect=read_page))
        steps = [('public_search', {'query': 'site:docs.python.org Python control flow documentation'}),
                 ('public_page', {'url': index}), ('public_page', {'url': chapter}),
                 ('public_page', {'url': index}), ('public_page', {'url': 'https://other.example/not-returned'})]
        responses = []
        async def model_stream(messages, info):
            parts = [part for message in messages for part in message.parts
                     if getattr(part, 'part_kind', '') == 'tool-return']
            if parts:
                responses.append(parts[-1].content)
            if len(parts) < len(steps):
                name, args = steps[len(parts)]
                yield {0: DeltaToolCall(name=name, json_args=json.dumps(args))}
            else:
                yield 'Python 控制流包括 if 和 for。[2]'
        @asynccontextmanager
        async def factory(*_):
            yield FunctionModel(stream_function=model_stream)
        engine = LighthouseModel(self.portal, model_factory=factory, public_sources=public)
        async def emit(*_): pass
        result = await engine.answer(self.actor, {'question': '请联网搜索 Python control flow documentation 的官方文档',
            'operation_id': 'public-page-fixture', '_profile': self.model.profile.return_value},
            [], self.request, emit, self.authorize, {})
        self.assertEqual(public.page.await_count, 2)
        self.assertTrue(responses[0]['ok'])
        self.assertEqual(responses[0]['resolved_page']['source_number'], 2)
        self.assertEqual([call.args[0] for call in public.page.await_args_list], [index, chapter])
        self.assertTrue(responses[3]['cached'])
        self.assertEqual(responses[3]['link'], index)
        self.assertFalse(responses[-1]['ok'])
        self.assertEqual([source['url'] for source in result['sources']], [index, chapter])
        self.assertEqual(self.reads, [])

    async def test_explicit_public_url_with_chinese_punctuation_can_be_read(self):
        url = 'https://docs.python.org/3/tutorial/controlflow.html'
        public = SimpleNamespace(page=AsyncMock(return_value={'ok': True, 'sourceURL': url,
            'queried_at': '2026-10-06T10:00:00+08:00', 'title': 'Control Flow Tools', 'text': 'Use if and for.',
            'truncated': True, 'links': []}))
        async def model_stream(messages, info):
            if any(getattr(part, 'part_kind', '') == 'tool-return' for message in messages for part in message.parts):
                yield '已获取Python官方文档全文。Python 控制流包括 if 和 for。[1]'
            else:
                yield {0: DeltaToolCall(name='public_page', json_args=json.dumps({'url': url}))}
        @asynccontextmanager
        async def factory(*_):
            yield FunctionModel(stream_function=model_stream)
        engine = LighthouseModel(self.portal, model_factory=factory, public_sources=public)
        async def emit(*_): pass
        result = await engine.answer(self.actor, {'question': '请阅读 ' + url + '，说明 Python 控制流',
            'operation_id': 'public-page-url-fixture', '_profile': self.model.profile.return_value},
            [], self.request, emit, self.authorize, {})
        public.page.assert_awaited_once()
        self.assertEqual(result['sources'][0]['url'], url)
        self.assertTrue(result['answer'].startswith('已读取公开页面节选，并非全文。'))
        self.assertEqual(self.reads, [])

    async def test_public_page_recovers_one_transient_read_without_replaying_success(self):
        index, chapter = 'https://docs.python.org/3/contents.html', 'https://docs.python.org/3/tutorial/controlflow.html'
        for failed_url in (index, chapter):
            with self.subTest(failed_url=failed_url):
                reads, replies = [], []
                async def read_page(url, question):
                    reads.append(url)
                    if url == failed_url and reads.count(url) == 1:
                        return {'ok': False, 'retryable': True, 'note': '公开页面读取未完成。'}
                    return {'ok': True, 'sourceURL': url, 'queried_at': '2026-10-06T10:00:00+08:00',
                        'title': 'Control Flow Tools', 'text': 'Use if and for.',
                        'links': [{'label': 'Control Flow Tools', 'link': chapter}] if url == index else []}
                public = SimpleNamespace(search=AsyncMock(return_value={'ok': False,
                    'navigation': [{'title': 'Python contents', 'link': index}]}), page=AsyncMock(side_effect=read_page))
                steps = [('public_search', {'query': 'Python control flow documentation site:docs.python.org'}),
                         ('public_page', {'url': chapter}), ('public_page', {'url': index})]
                async def model_stream(messages, info):
                    parts = [part for message in messages for part in message.parts
                             if getattr(part, 'part_kind', '') == 'tool-return']
                    if parts:
                        replies.append(parts[-1].content)
                    if len(parts) < len(steps):
                        name, args = steps[len(parts)]
                        yield {0: DeltaToolCall(name=name, json_args=json.dumps(args))}
                    else:
                        yield 'Python 控制流包括 if 和 for。[2]'
                @asynccontextmanager
                async def factory(*_):
                    yield FunctionModel(stream_function=model_stream)
                engine = LighthouseModel(self.portal, model_factory=factory, public_sources=public)
                async def emit(*_): pass
                result = await engine.answer(self.actor, {'question': '请联网搜索 Python control flow documentation 的官方文档',
                    'operation_id': 'page-retry-' + str(failed_url == chapter), '_profile': self.model.profile.return_value},
                    [], self.request, emit, self.authorize, {})
                self.assertTrue(replies[0]['ok'])
                self.assertEqual(reads, [index, index, chapter] if failed_url == index else [index, chapter, chapter])
                self.assertTrue(all(reply.get('cached') for reply in replies[1:]))
                self.assertEqual([source['url'] for source in result['sources']], [index, chapter])
                self.assertEqual(self.reads, [])

    async def test_public_page_retry_budget_and_permanent_failure_cache(self):
        urls = ['https://docs.python.org/' + suffix for suffix in ('3/contents.html', '3/tutorial/', '3/library/')]
        for retryable, count in ((True, 3), (False, 2)):
            with self.subTest(retryable=retryable):
                public = SimpleNamespace(page=AsyncMock(return_value={'ok': False,
                    'retryable': retryable, 'note': '公开页面未取得。'}))
                steps, replies = [urls[0], urls[0], urls[1], urls[1]], []
                async def model_stream(messages, info):
                    parts = [part for message in messages for part in message.parts
                             if getattr(part, 'part_kind', '') == 'tool-return']
                    if parts:
                        replies.append(parts[-1].content)
                    if len(parts) < len(steps):
                        yield {0: DeltaToolCall(name='public_page', json_args=json.dumps({'url': steps[len(parts)]}))}
                    else:
                        yield '公开资料未取得，无法核实。'
                @asynccontextmanager
                async def factory(*_):
                    yield FunctionModel(stream_function=model_stream)
                engine = LighthouseModel(self.portal, model_factory=factory, public_sources=public)
                async def emit(*_): pass
                result = await engine.answer(self.actor, {'question': '请阅读 ' + ' '.join(urls),
                    'operation_id': 'page-retry-limit-' + str(retryable), '_profile': self.model.profile.return_value},
                    [], self.request, emit, self.authorize, {})
                self.assertEqual(public.page.await_count, count)
                self.assertTrue(replies[1]['cached'])
                self.assertFalse(any(reply['ok'] for reply in replies))
                self.assertFalse(result['sources'])
                if retryable:
                    self.assertTrue(replies[-1]['stop_reading'])

    async def test_explicit_page_refresh_bypasses_turn_cache_but_not_fetch_budget(self):
        url, replies = 'https://docs.python.org/3/tutorial/controlflow.html', []
        public = SimpleNamespace(page=AsyncMock(return_value={'ok': True, 'sourceURL': url,
            'queried_at': 'original-read-time', 'title': 'Control flow', 'text': 'if and for', 'links': []}))
        refreshes = [False, True, False, True, True]
        async def model_stream(messages, info):
            parts = [part for message in messages for part in message.parts
                     if getattr(part, 'part_kind', '') == 'tool-return']
            if parts:
                replies.append(parts[-1].content)
            if len(parts) < len(refreshes):
                yield {0: DeltaToolCall(name='public_page', json_args=json.dumps({
                    'url': url, 'refresh': refreshes[len(parts)]}))}
            else:
                yield 'Python 控制流包括 if 和 for。[1]'
        @asynccontextmanager
        async def factory(*_):
            yield FunctionModel(stream_function=model_stream)
        engine = LighthouseModel(self.portal, model_factory=factory, public_sources=public)
        async def emit(*_): pass
        await engine.answer(self.actor, {'question': '请阅读并刷新 ' + url,
            'operation_id': 'page-refresh-fixture', '_profile': self.model.profile.return_value},
            [], self.request, emit, self.authorize, {})
        self.assertEqual(public.page.await_count, 3)
        self.assertEqual([call.kwargs for call in public.page.await_args_list], [{}, {'refresh': True}, {'refresh': True}])
        self.assertTrue(replies[2]['cached'])
        self.assertEqual(replies[2]['queried_at'], 'original-read-time')
        self.assertTrue(replies[-1]['stop_reading'])

    async def test_failure_is_actionable_and_does_not_log_provider_body(self):
        async def fail(*args):
            raise RuntimeError("api_key=must-not-appear-in-log")
        self.engine.answer = fail
        with self.assertLogs("openclaw_service.assistant.lighthouse_stream", level="WARNING") as log:
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
        operations = read_scope_operations({"api_id": "GET /api/assistant/question-bank", "params": {"scope": "ALL"}}, desc, actor)
        self.assertEqual([op["params"]["scope"] for op in operations], ["H"])
        with self.assertRaises(AssistantError):
            scoped_operation({"api_id": "GET /api/assistant/question-bank", "params": {"scope": "D"}}, desc, actor)
        with self.assertRaises(AssistantError):
            read_scope_operations({"api_id": "GET /api/assistant/question-bank"}, desc, {**actor, "learning_scopes": []})


if __name__ == "__main__":
    unittest.main()
