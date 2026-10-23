"""Regression: Feishu assistant conversations isolate by the authenticated person.

The Feishu transport retains channel authorization while private, topic and web
conversations have independent state. These tests verify isolation at the real
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

    async def test_mixed_history_is_partitioned_once_without_deleting_originals(self):
        suite = self.suite
        person = _web_actor('ou_A_AAAA')
        private = _feishu_actor(person['id'], 'private')
        turns = [dict(operation_id='web_original_0001', question='网页问题', source_channel='web'),
                 dict(operation_id='feishu_private_0001', question='私聊问题', source_channel='feishu'),
                 dict(operation_id='topic_mirror_old', question='旧话题镜像', source_channel='feishu',
                      display_only=True, source_topic='old-topic')]
        for index, turn in enumerate(turns):
            turn.update(answer='原回答', status='completed', at=index + 1, scopes=['E'])
        mixed = {'id': 'web-original', 'turns': turns, 'model_id': 'test',
                 'context': {'summary': '旧混合摘要'}, 'execution_lanes': {'old': {'context': {'summary': '旧分支摘要'}}}}
        suite.store.put_document('lighthouse_ai', 'conversation:' + person['id'], mixed)
        legacy_key = 'conversation:' + person['id'] + ':legacy-hash'
        suite.store.put_document('lighthouse_ai', legacy_key, {'id': 'old-channel', 'turns': []})
        first = suite.assistant._state(person)
        self.assertEqual(first['id'], 'web-original')
        self.assertEqual(first['turns'], turns)
        self.assertEqual(first['context'], {})
        self.assertEqual(first['_legacy_context'], mixed['context'])
        self.assertEqual(first['execution_lanes']['old']['context'], {})
        copied = suite.assistant._state(private)
        self.assertEqual(copied['id'], first['id'])
        self.assertEqual(copied['turns'], [turns[1]])
        self.assertEqual(copied['model_id'], 'test')
        self.assertEqual(copied['legacy_conversation_ids'], [])
        self.assertEqual([t['question'] for t in (await suite.runtime.conversation(person))['turns']], ['网页问题'])
        self.assertEqual([t['question'] for t in (await suite.runtime.history(private, 100))['turns']], ['私聊问题'])
        self.assertEqual(len(suite.store.list_documents('lighthouse_messages', key_prefix='web-original:')), 3)
        self.assertEqual(suite.store.get_document('lighthouse_ai', legacy_key), {'id': 'old-channel', 'turns': []})
        self.assertEqual(suite.assistant._state(private), copied)
        suite.assistant.clear(person)
        self.assertEqual(suite.assistant._state(private)['turns'], [turns[1]])
        self.assertNotEqual(suite.assistant._state(person)['id'], first['id'])
        suite.assistant.clear(private)
        self.assertEqual(suite.assistant._state(private)['turns'], [])
        rebuilt = self._build(suite.store)
        try:
            self.assertEqual(rebuilt.assistant._state(private)['turns'], [])
            self.assertEqual(rebuilt.assistant._state(person)['turns'], [])
        finally:
            await rebuilt.runtime.close()

    async def test_direct_feishu_commands_are_saved_once_without_model_execution(self):
        suite = self.suite
        actor = _feishu_actor('ou_A_AAAA', 'group1')
        state = suite.assistant._state(actor)
        payload = {'operation_id': 'feishu_' + 'a' * 40, 'conversation_id': state['id'],
                   'question': '进度:处理完成', 'answer': '已更新填写,请确认后发送。'}
        suite.assistant.record_channel_reply(actor, payload)
        suite.assistant.record_channel_reply(actor, payload)
        turns = (await suite.runtime.conversation(actor))['turns']
        self.assertEqual(len(turns), 1)
        self.assertEqual(turns[0]['question'], payload['question'])
        self.assertEqual(turns[0]['answer'], payload['answer'])
        self.assertEqual(suite.engine.calls, [])
        self.assertEqual((await suite.runtime.conversation(_web_actor(actor['id'])))['turns'], [])
        stale = copy.deepcopy(state)
        suite.assistant._save_state(actor, stale)
        self.assertEqual(suite.assistant._state(actor)['turns'][0]['operation_id'], payload['operation_id'])
        with self.assertRaises(AssistantError):
            suite.assistant.record_channel_reply(_web_actor(actor['id']), payload)
        with self.assertRaises(AssistantError):
            suite.assistant.record_channel_reply(_feishu_actor('ou_B_BBBB', 'group1'), payload)

    async def test_upgrade_preserves_private_active_run_regardless_of_first_channel(self):
        for index, order in enumerate(('web-first', 'private-first')):
            with self.subTest(order=order):
                web = _web_actor('ou_upgrade_' + str(index))
                private = _feishu_actor(web['id'], 'private')
                run_id = str(index) * 32
                turn = {'operation_id': 'feishu_upgrade_0001', 'run_id': run_id, 'question': '原私聊问题',
                        'status': 'pending', 'source_channel': 'feishu', 'scopes': ['E']}
                self.store.put_document('lighthouse_ai', 'conversation:' + web['id'], {
                    'id': 'existing-' + str(index), 'turns': [turn], 'active_run_id': run_id,
                    'query_scopes': ['E'], 'phase': '正在处理', 'streaming': True})
                for actor in ([web, private] if order == 'web-first' else [private, web]):
                    self.suite.assistant._state(actor)
                self.assertEqual(self.suite.assistant._state(private)['active_run_id'], run_id)
                self.assertEqual(self.suite.assistant._state(private)['turns'], [turn])
                self.assertEqual(self.suite.assistant._state(web)['active_run_id'], '')

    async def test_archived_private_history_remains_available_when_recent_turns_are_web_only(self):
        web = _web_actor('ou_A_AAAA')
        private = _feishu_actor(web['id'], 'private')
        self.store.put_document('lighthouse_ai', 'conversation:' + web['id'], {'id': 'old-archive', 'turns': []})
        self.store.put_document('lighthouse_messages', 'old-archive:feishu_archived_0001', {
            'operation_id': 'feishu_archived_0001', 'source_channel': 'feishu', 'at': 1,
            'question': '原私聊历史', 'answer': '原回答', 'status': 'completed', 'scopes': ['E']})
        self.assertEqual((await self.suite.runtime.history(web, 100))['turns'], [])
        self.assertEqual((await self.suite.runtime.history(private, 100))['turns'][0]['question'], '原私聊历史')
        self.suite.assistant.clear(private)
        self.assertEqual((await self.suite.runtime.history(private, 100))['turns'], [])

    async def test_partition_never_overwrites_newer_private_receipts_with_old_web_snapshot(self):
        web = _web_actor('ou_A_AAAA')
        private = _feishu_actor(web['id'], 'private')
        old = {'operation_id': 'feishu_original_0001', 'source_channel': 'feishu', 'status': 'pending',
               'question': '原问题', 'scopes': ['E'], 'at': 1}
        latest = {**old, 'status': 'completed', 'answer': '已核实的最终回执'}
        key = 'old-shared:' + old['operation_id']
        self.store.put_document('lighthouse_ai', 'conversation:' + web['id'], {'id': 'old-shared', 'turns': [old]})
        self.store.put_document('lighthouse_messages', key, latest)
        self.assertEqual(self.suite.assistant._state(private)['turns'], [latest])
        self.suite.assistant._state(web)
        self.assertEqual(self.store.get_document('lighthouse_messages', key), latest)

    async def test_card_followup_binds_to_current_question_and_preserves_context(self):
        suite = self.suite
        actor = _feishu_actor('ou_A_AAAA', 'group1')
        state = suite.assistant._state(actor)
        state['turns'] = [{'operation_id': 'original_question_0001', 'question': '帮我写一篇文章',
                          'answer': '面向什么读者?', 'status': 'completed', 'scopes': ['E'], 'source_channel': 'feishu',
                          'clarification': {'options': ['专业读者', '新手']}, 'at': 1}]
        suite.store.put_document('lighthouse_ai', suite.assistant._key(actor), state)
        async def authorize():
            return actor
        body = {'conversation_id': state['id'], 'question': '面向新员工,侧重实际例子',
                'operation_id': 'card_reply_0000001', 'clarification_operation_id': 'stale_question'}
        with self.assertRaises(AssistantError) as error:
            await suite.runtime.submit(actor, body, suite.request, authorize)
        self.assertEqual(error.exception.status, 409)
        body['clarification_operation_id'] = 'original_question_0001'
        await suite.runtime.submit(actor, body, suite.request, authorize)
        await self._finish(suite)
        saved = suite.assistant._state(actor)['turns'][-1]
        self.assertTrue(saved['_clarification_reply'])
        self.assertIn('帮我写一篇文章', saved['prompt'])
        self.assertIn(body['question'], saved['prompt'])

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
            self.assertEqual(LighthouseAssistant._key(group2_a), key_a)
            self.assertEqual(rebuilt.assistant._state(web_a)["turns"], [])
            self.assertEqual(rebuilt.assistant._state(group2_a)["turns"], state_a['turns'])
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
                self.assertEqual(len(store.list_documents('lighthouse_ai', key_prefix='feishu-conversation:')), 200)
                newcomer = _feishu_actor('ou_newcomer', 'oc_group200')
                with ThreadPoolExecutor(max_workers=8) as pool:
                    identities = list(pool.map(lambda _: second.assistant._state(newcomer)['id'], range(40)))
                self.assertEqual(len(set(identities)), 1)
                self.assertEqual(len(store.list_documents('lighthouse_ai', key_prefix='feishu-conversation:')), 201)
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

    async def test_web_private_and_topics_have_independent_history_and_context(self):
        suite = self.suite
        person_a = "ou_A_AAAA"
        group1 = _feishu_actor(person_a, "oc_GRP1")
        group2 = _feishu_actor(person_a, "oc_GRP2")
        group1['_feishu_topic'] = {'id': 'group1:root1'}
        group2['_feishu_topic'] = {'id': 'group2:root2'}
        private = _feishu_actor(person_a, 'private')
        web = _web_actor(person_a)

        keys = {LighthouseAssistant._key(actor) for actor in (group1, group2, private, web)}
        self.assertEqual(len(keys), 4)

        await self._submit(suite, group1, "群里的问题", "channel_op_G1_0001")
        await self._finish(suite)
        for actor in (group2, private, web):
            self.assertEqual(suite.assistant._state(actor)['turns'], [])
        state1 = suite.store.get_document("lighthouse_ai", LighthouseAssistant._key(group1))
        self.assertEqual([turn["question"] for turn in state1["turns"]], ["群里的问题"])
        await self._submit(suite, web, '接着刚才的问题', 'channel_op_WEB_0001')
        await self._finish(suite)
        self.assertEqual(suite.engine.calls[-1]['history'], [])
        await self._submit(suite, private, '私聊的问题', 'channel_op_PRIVATE_0001')
        await self._finish(suite)
        self.assertEqual(suite.engine.calls[-1]['history'], [])
        await self._submit(suite, group1, '接着群里的问题', 'channel_op_G1_0002')
        await self._finish(suite)
        self.assertEqual(suite.engine.calls[-1]['history'], [])
        self.assertNotEqual(suite.assistant._state(group2)['id'], state1['id'])

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

    async def test_same_person_can_run_web_private_and_two_topics_concurrently(self):
        suite = self.suite
        web = _web_actor('ou_A_AAAA')
        private = _feishu_actor(web['id'], 'private')
        topics = [{**_feishu_actor(web['id'], 'group'), '_feishu_topic': {'id': 'group:' + root}}
                  for root in ('root1', 'root2')]
        actors = [web, private, *topics]
        suite.engine.block, suite.engine.target = True, 3
        acks = await asyncio.gather(*[self._submit(suite, actor, '独立问题' + str(index), 'parallel_channel_%04d' % index)
                                     for index, actor in enumerate(actors)])
        await asyncio.wait_for(suite.engine.started.wait(), 5)
        suite.engine.gate.set()
        await self._finish(suite)
        for index, actor in enumerate(actors):
            self.assertEqual(suite.runtime.get_run(actor, acks[index]['run_id'])['status'], 'completed')
            for other in actors:
                if other is not actor:
                    with self.assertRaises(AssistantError):
                        suite.runtime.get_run(other, acks[index]['run_id'])
            self.assertEqual([t['question'] for t in (await suite.runtime.conversation(actor))['turns']], ['独立问题' + str(index)])
        self.assertTrue(all(call['history'] == [] for call in suite.engine.calls))

    async def test_existing_private_plan_survives_partition_without_exposing_web_runs(self):
        suite = self.suite
        private = _feishu_actor('ou_A_AAAA', 'private')
        web = _web_actor(private['id'])
        run_id, web_run_id, plan_id = 'a' * 32, 'b' * 32, 'c' * 32
        private_turn = {'operation_id': 'feishu_private_0001', 'run_id': run_id, 'status': 'completed',
                        'source_channel': 'feishu', 'question': '原私聊待办', 'answer': '请确认', 'scopes': ['E'],
                        'plan': {'id': plan_id, 'status': 'awaiting_confirmation'}}
        web_turn = {**private_turn, 'operation_id': 'web_original_0001', 'run_id': web_run_id,
                    'source_channel': 'web', 'plan': None}
        suite.store.put_document('lighthouse_ai', 'conversation:' + private['id'],
                                 {'id': 'existing-shared', 'turns': [private_turn, web_turn]})
        original = {'id': plan_id, 'owner': private['id'], 'conversation_id': 'existing-shared',
                    'assistant_run_id': run_id, 'turn_id': private_turn['operation_id'],
                    'scopes': ['E'], 'status': 'awaiting_confirmation'}
        suite.store.put_document('lighthouse_agent_plans', plan_id, original)
        for identity, turn in ((run_id, private_turn), (web_run_id, web_turn)):
            suite.store.put_document('lighthouse_runs', identity, {'id': identity, 'owner': private['id'],
                'conversation_id': 'existing-shared', 'operation_id': turn['operation_id'], 'scopes': ['E'], 'status': 'completed'})
        self.assertEqual(suite.portal.get_plan(private, plan_id), original)
        self.assertEqual(suite.runtime.get_run(private, run_id)['id'], run_id)
        for actor, identity in ((web, run_id), (private, web_run_id)):
            with self.assertRaises(AssistantError):
                suite.runtime.get_run(actor, identity)
        with self.assertRaises(AssistantError):
            suite.portal.get_plan(web, plan_id)
        new_plan = suite.portal.prepare(web, {'operations': [{'api_id': 'DELETE /api/drills/{drill_id}',
            'path_params': {'drill_id': 'drill1'}}]}, 'web_new_plan_0001', [])
        with self.assertRaises(AssistantError):
            suite.portal.get_plan(private, new_plan['id'])
        suite.assistant.clear(web)
        self.assertEqual(suite.portal.get_plan(private, plan_id), original)
        self.assertEqual(suite.store.get_document('lighthouse_agent_plans', plan_id), original)

    async def test_portal_plan_stays_in_original_channel_and_survives_web_clear(self):
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
        self.assertEqual(suite.portal.get_plan(other_group, plan_id)['id'], plan_id)
        with self.assertRaises(AssistantError):
            suite.portal.get_plan(_web_actor(A['id']), plan_id)
        cancelled = suite.portal.cancel(other_group, plan_id)
        self.assertEqual(cancelled["status"], "cancelled")
        suite.assistant.clear(_web_actor(A['id']))
        self.assertEqual(suite.portal.get_plan(A, plan_id)['id'], plan_id)
        suite.assistant.clear(A)
        with self.assertRaises(AssistantError) as error:
            suite.portal.get_plan(A, plan_id)
        self.assertEqual(error.exception.status, 409)

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
