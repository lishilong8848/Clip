"""Regression: Feishu assistant conversations isolate by the authenticated person.

The Feishu transport routes a message under ``lighthouse_channel == "feishu:" + chat_id``
and the assistant keys persistent state with ``LighthouseAssistant._key(actor)`` using
``actor["id"]`` plus a hash of that channel.  These tests verify isolation at the real
storage/stream/plan/file boundaries when two people with the same display name share one
Feishu group.  Everything runs against an in-memory Store and a local temporary directory
with a fake engine; nothing touches the network or real business data.
"""
import asyncio
import copy
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

from fastapi import FastAPI, Request
from lan_bitable_template_portal.lighthouse_ai import AssistantError, LighthouseAssistant
from lan_bitable_template_portal.lighthouse_agent import PortalAgent
from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog
from lan_bitable_template_portal.lighthouse_files import LighthouseFiles
from lan_bitable_template_portal.lighthouse_stream import LighthouseStream

from bin.test_lighthouse_stream import Store


class Engine:
    """Fake model engine that records identity/history and can gate concurrency."""

    def __init__(self):
        self.calls = []
        self.gate = asyncio.Event()
        self.started = asyncio.Event()
        self.inflight = 0
        self.target = 0
        self.block = False

    async def answer(self, actor, turn, history, request, emit, authorize, context):
        self.calls.append({"actor": actor["id"], "turn": turn,
                           "question": turn.get("question", ""),
                           "history": [copy.deepcopy(item) for item in history]})
        await emit("status", {"label": "处理隔离测试"})
        await emit("text", {"delta": "处理中。"})
        if self.block:
            self.inflight += 1
            if self.target and self.inflight >= self.target:
                self.started.set()
            await self.gate.wait()
        await emit("text", {"delta": "完成。"})
        return {"answer": "回答:" + actor["id"], "sources": []}

    async def summarize(self, actor, turns, previous, profile):
        return "隔离测试摘要。"


def _feishu_actor(open_id, chat_id, *, is_admin=False, name="张三", employee_no="E1001"):
    return {"id": open_id, "name": name, "employee_no": employee_no,
            "scopes": ["E"], "allowed_scopes": ["E"],
            "channel": "feishu:" + chat_id, "is_admin": is_admin,
            "can_manage_settings": True}


def _web_actor(open_id):
    return {"id": open_id, "name": "张三", "employee_no": "E1001",
            "scopes": ["E"], "allowed_scopes": ["E"], "is_admin": False,
            "can_manage_settings": True}


class FeishuIsolationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = Store(Path(self.tmp.name) / "state.sqlite3")
        self.suite = self._build(self.store)

    async def asyncTearDown(self):
        await self.suite.runtime.close()

    def _build(self, store, engine=None):
        model = Mock()
        model.settings.return_value = {"configured": True, "enabled": True,
                                       "active_model_id": "test",
                                       "models": [{"id": "test", "name": "测试", "model": "fixture",
                                                   "configured": True}]}
        model.profile.return_value = {"id": "test", "name": "测试", "model": "fixture"}
        assistant = LighthouseAssistant(store, Mock(return_value=([], [])), model=model)
        app = FastAPI()

        @app.delete("/api/drills/{drill_id}")
        async def delete_drill(drill_id: str):
            return {"ok": True, "data": {"deleted": True}}

        catalog = PortalAPICatalog(app)
        files = LighthouseFiles(store)
        portal = PortalAgent(assistant, catalog, files)
        engine = engine or Engine()
        runtime = LighthouseStream(portal, engine=engine)
        request = Request({"type": "http", "scheme": "http", "server": ("testserver", 80),
                           "client": ("127.0.0.1", 1), "path": "/api/assistant/messages",
                           "root_path": "", "query_string": b"",
                           "headers": [(b"origin", b"http://testserver")]})
        return SimpleNamespace(store=store, assistant=assistant, portal=portal, files=files,
                               engine=engine, runtime=runtime, request=request)

    async def _submit(self, suite, actor, question, operation_id):
        conversation_id = suite.assistant._state(actor)["id"]
        payload = {"question": question, "file_ids": [], "operation_id": operation_id,
                   "conversation_id": conversation_id}

        async def authorize():
            return copy.deepcopy(actor)

        return await suite.runtime.submit(actor, payload, suite.request, authorize)

    async def _finish(self, suite):
        await asyncio.wait_for(asyncio.gather(*suite.runtime.workers.values(),
                                              return_exceptions=True), 8)

    async def test_same_group_same_name_separate_states_and_reconstruct_isolated(self):
        suite = self.suite
        A = _feishu_actor("ou_A_AAAA", "oc_GRP1")
        B = _feishu_actor("ou_B_BBBB", "oc_GRP1")
        self.assertEqual(A["name"], B["name"])
        self.assertNotEqual(A["id"], B["id"])
        self.assertEqual(A["channel"], B["channel"])

        await self._submit(suite, A, "A的第一个问题", "isolation_op_A1_0001")
        await self._finish(suite)
        await self._submit(suite, B, "B的第一个问题", "isolation_op_B1_0001")
        await self._finish(suite)

        key_a = LighthouseAssistant._key(A)
        key_b = LighthouseAssistant._key(B)
        self.assertNotEqual(key_a, key_b)
        state_a = suite.store.get_document("lighthouse_ai", key_a)
        state_b = suite.store.get_document("lighthouse_ai", key_b)
        self.assertNotEqual(state_a["id"], state_b["id"])
        self.assertEqual([turn["question"] for turn in state_a["turns"]], ["A的第一个问题"])
        self.assertEqual([turn["question"] for turn in state_b["turns"]], ["B的第一个问题"])

        suite.assistant._save_state(
            A, {**state_a, "context": {"summary": "A的专属摘要", "through": "",
                                       "scopes": ["E"]}})
        suite.assistant._save_state(
            B, {**state_b, "context": {"summary": "B的专属摘要", "through": "",
                                       "scopes": ["E"]}})

        rebuilt = self._build(suite.store)
        try:
            conv_a = await rebuilt.runtime.conversation(A)
            conv_b = await rebuilt.runtime.conversation(B)
            self.assertEqual([turn["question"] for turn in conv_a["turns"]], ["A的第一个问题"])
            self.assertEqual([turn["question"] for turn in conv_b["turns"]], ["B的第一个问题"])
            self.assertEqual(rebuilt.store.get_document("lighthouse_ai", rebuilt.assistant._key(A))["context"]["summary"], "A的专属摘要")
            self.assertEqual(rebuilt.store.get_document("lighthouse_ai", rebuilt.assistant._key(B))["context"]["summary"], "B的专属摘要")

            web_a = _web_actor(A["id"])
            group2_a = _feishu_actor(A["id"], "oc_GRP2")
            self.assertNotEqual(LighthouseAssistant._key(web_a), key_a)
            self.assertNotEqual(LighthouseAssistant._key(group2_a), key_a)
            self.assertEqual(rebuilt.assistant._state(web_a)["turns"], [])
            self.assertTrue(all(turn["question"] != "A的第一个问题"
                                for turn in rebuilt.assistant._state(group2_a)["turns"]))
        finally:
            await rebuilt.runtime.close()

    async def test_group_detail_followup_inherits_only_the_same_person_latest_question(self):
        suite = self.suite
        person_a = _feishu_actor("ou_A_AAAA", "oc_GRP1")
        person_b = _feishu_actor("ou_B_BBBB", "oc_GRP1")
        await self._submit(suite, person_a, "今天有几个变更？", "followup_person_A1_0001")
        await self._finish(suite)
        await self._submit(suite, person_a, "今天进行中的变更有几个？", "followup_person_A2_0001")
        await self._finish(suite)
        await self._submit(suite, person_b, "今天发生了几条事件？", "followup_person_B1_0001")
        await self._finish(suite)
        await self._submit(suite, person_a, "分别是哪些？", "followup_person_A3_0001")
        await self._finish(suite)
        call = suite.engine.calls[-1]
        self.assertEqual(call["actor"], person_a["id"])
        self.assertIn("进行中的变更", call["turn"]["prompt"])
        self.assertNotIn("今天有几个变更", call["turn"]["prompt"])
        self.assertNotIn("事件", call["turn"]["prompt"])
        self.assertTrue(all("事件" not in turn["question"] for turn in call["history"]))

    async def test_200_people_relogin_reuse_persistent_context_and_summary(self):
        from openclaw_service.store import AssistantStore
        from concurrent.futures import ThreadPoolExecutor
        store = AssistantStore(Path(self.tmp.name) / 'persistent')
        first = self._build(store)
        people = [_feishu_actor(f'ou_person{i}', 'oc_group200') for i in range(200)]
        try:
            saved = {}
            for person in people:
                state = first.assistant._state(person)
                state['context'] = {'summary': person['id'] + '的摘要', 'scopes': ['E']}
                first.assistant._save_state(person, state)
                saved[person['id']] = state['id']
            # Recreate the service from disk, not the in-memory test store.
            second = self._build(AssistantStore(Path(self.tmp.name) / 'persistent'))
            try:
                for person in people * 3:
                    relogged = {**person, 'name': '更新姓名', 'session_token': 'new-login'}
                    state = second.assistant._state(relogged)
                    self.assertEqual(state['id'], saved[person['id']])
                    self.assertEqual(state['context']['summary'], person['id'] + '的摘要')
                self.assertEqual(len(store.list_documents('lighthouse_ai', key_prefix='conversation:')), 200)
                newcomer = _feishu_actor('ou_newcomer', 'oc_group200')
                with ThreadPoolExecutor(max_workers=8) as pool:
                    identities = list(pool.map(lambda _: second.assistant._state(newcomer)['id'], range(40)))
                self.assertEqual(len(set(identities)), 1)
                self.assertEqual(len(store.list_documents('lighthouse_ai', key_prefix='conversation:')), 201)
            finally:
                await second.runtime.close()
        finally:
            await first.runtime.close()

    async def test_formal_person_without_building_permissions_can_ask_shared_company_docs_only(self):
        person = _feishu_actor('ou_no_building', 'oc_group')
        person.update(scopes=[], allowed_scopes=[])
        result = await self._submit(self.suite, person, '查公司员工手册的报销规定', 'company_no_scope_0001')
        await self._finish(self.suite)
        self.assertTrue(result['run_id'])
        with self.assertRaises(AssistantError) as error:
            await self._submit(self.suite, person, '今天E楼有几条通告', 'business_no_scope_0001')
        self.assertEqual(error.exception.status, 403)

    async def test_web_and_group_channels_and_different_groups_are_distinct_storage(self):
        suite = self.suite
        person_a = "ou_A_AAAA"
        group1 = _feishu_actor(person_a, "oc_GRP1")
        group2 = _feishu_actor(person_a, "oc_GRP2")
        web = _web_actor(person_a)

        keys = {LighthouseAssistant._key(actor) for actor in (group1, group2, web)}
        self.assertEqual(len(keys), 3)

        await self._submit(suite, group1, "群里的问题", "channel_op_G1_0001")
        await self._finish(suite)
        for actor in (group2, web):
            state = suite.store.get_document("lighthouse_ai", LighthouseAssistant._key(actor))
            self.assertTrue(state is None or all(turn["question"] != "群里的问题"
                                                 for turn in state["turns"]))
        state1 = suite.store.get_document("lighthouse_ai", LighthouseAssistant._key(group1))
        self.assertEqual([turn["question"] for turn in state1["turns"]], ["群里的问题"])

    async def test_same_group_concurrent_runs_isolate_and_deny_foreign_admin_run(self):
        suite = self.suite
        A = _feishu_actor("ou_A_AAAA", "oc_GRP1")
        B = _feishu_actor("ou_B_BBBB", "oc_GRP1", is_admin=True)

        suite.engine.block = True
        suite.engine.target = 2
        first_a = asyncio.create_task(self._submit(suite, A, "我的并发问题一", "concurrency_op_A1_0001"))
        first_b = asyncio.create_task(self._submit(suite, B, "我的并发问题二", "concurrency_op_B1_0001"))
        ack_a1, ack_b1 = await asyncio.gather(first_a, first_b)
        self.assertNotEqual(ack_a1["run_id"], ack_b1["run_id"])
        await asyncio.wait_for(suite.engine.started.wait(), 5)
        suite.engine.gate.set()
        await self._finish(suite)

        in_flight = {call["actor"]: call for call in suite.engine.calls}
        self.assertEqual(in_flight[A["id"]]["question"], "我的并发问题一")
        self.assertEqual(in_flight[B["id"]]["question"], "我的并发问题二")
        self.assertEqual(in_flight[A["id"]]["history"], [])
        self.assertEqual(in_flight[B["id"]]["history"], [])

        suite.engine.block = False
        suite.engine.calls = []
        ack_a2 = await self._submit(suite, A, "我的并发问题一继续", "concurrency_op_A2_0001")
        ack_b2 = await self._submit(suite, B, "我的并发问题二继续", "concurrency_op_B2_0001")
        await self._finish(suite)
        self.assertEqual(ack_a2["turn"]["question"], "我的并发问题一继续")
        self.assertEqual(ack_b2["turn"]["question"], "我的并发问题二继续")
        calls2 = {call["actor"]: call for call in suite.engine.calls}
        self.assertEqual([turn["question"] for turn in calls2[A["id"]]["history"]], ["我的并发问题一"])
        self.assertEqual([turn["question"] for turn in calls2[B["id"]]["history"]], ["我的并发问题二"])

        # Own access succeeds first.
        self.assertEqual(suite.runtime.get_run(A, ack_a1["run_id"])["id"], ack_a1["run_id"])
        with self.assertRaises(AssistantError) as error:
            suite.runtime.get_run(B, ack_a1["run_id"])
        self.assertEqual(error.exception.status, 404)
        with self.assertRaises(AssistantError) as error:
            await suite.runtime.stop(B, ack_a1["run_id"])
        self.assertEqual(error.exception.status, 404)

    async def test_portal_plan_rejects_foreign_and_other_conversation_but_allows_owner(self):
        suite = self.suite
        A = _feishu_actor("ou_A_AAAA", "oc_GRP1")
        B = _feishu_actor("ou_B_BBBB", "oc_GRP1", is_admin=True)

        decision = {"operations": [{"api_id": "DELETE /api/drills/{drill_id}",
                                    "path_params": {"drill_id": "drill1"}}]}
        plan = suite.portal.prepare(A, decision, "agent_isolation_op_0001", [])
        plan_id = plan["id"]

        self.assertEqual(suite.portal.get_plan(A, plan_id)["owner"], A["id"])

        with self.assertRaises(AssistantError) as error:
            suite.portal.get_plan(B, plan_id)
        self.assertEqual(error.exception.status, 404)
        with self.assertRaises(AssistantError) as error:
            suite.portal.cancel(B, plan_id)
        self.assertEqual(error.exception.status, 404)

        other_group = _feishu_actor(A["id"], "oc_GRP2")
        with self.assertRaises(AssistantError) as error:
            suite.portal.get_plan(other_group, plan_id)
        self.assertEqual(error.exception.status, 409)
        with self.assertRaises(AssistantError) as error:
            suite.portal.cancel(other_group, plan_id)
        self.assertEqual(error.exception.status, 409)

        cancelled = suite.portal.cancel(A, plan_id)
        self.assertEqual(cancelled["status"], "cancelled")

    async def test_files_are_private_and_foreign_submission_rejected_without_model(self):
        suite = self.suite
        A = _feishu_actor("ou_A_AAAA", "oc_GRP1")
        B = _feishu_actor("ou_B_BBBB", "oc_GRP1", is_admin=True)

        uploaded = suite.files.upload(A, "私人.txt", "A的私人文件正文".encode("utf-8"))
        fid = uploaded["id"]
        # Own access succeeds first.
        item = suite.files.get(A, fid)
        self.assertEqual(item["owner"], A["id"])
        self.assertIn("A的私人文件正文", suite.files.text(A, fid)["text"])

        # The peer cannot retrieve the attachment directly either.
        with self.assertRaises(AssistantError) as error:
            suite.files.get(B, fid)
        self.assertEqual(error.exception.status, 404)

        # A real LighthouseStream submit from B carrying A's valid file_id must reject
        # with 404 before any model call or turn is recorded.
        conversation_id = suite.assistant._state(B)["id"]
        payload = {"question": "处理附件", "file_ids": [fid], "operation_id": "foreign_attach_op_0001",
                   "conversation_id": conversation_id}

        async def authorize():
            return copy.deepcopy(B)

        with self.assertRaises(AssistantError) as error:
            await suite.runtime.submit(B, payload, suite.request, authorize)
        self.assertEqual(error.exception.status, 404)
        self.assertEqual(suite.engine.calls, [])
        state_b = suite.store.get_document("lighthouse_ai", LighthouseAssistant._key(B))
        self.assertTrue(all(turn["question"] != "处理附件" for turn in state_b["turns"]))


if __name__ == "__main__":
    unittest.main()
