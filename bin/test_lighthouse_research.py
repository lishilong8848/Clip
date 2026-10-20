"""Research resumes successful reads without touching business records or other topics."""
import copy
from contextlib import closing
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock

sys.path.insert(0, str(Path(__file__).resolve().parent))
from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_research import Research
from bin import test_lighthouse_stream as fixtures


class ResearchTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.store = fixtures.Store('unused')
        self.research = Research(self.store)
        self.actor = {'id': 'user', 'scopes': ['C'], 'modules': ['assistant']}
        self.sources = AsyncMock()
        self.sources.search.return_value = {'ok': True, 'results': [
            {'url': 'https://docs.python.org/3/', 'title': 'Python', 'snippet': 'documented facts'}]}
        self.sources.page.return_value = {'ok': True, 'text': 'verified original text'}
        self.emit = AsyncMock()
        self.auth = AsyncMock(side_effect=lambda: copy.deepcopy(self.actor))

    async def collect(self, **kwargs):
        return await self.research.collect(self.actor, 'operation', ['Python'], self.sources,
            self.auth, self.emit, question='Python', **kwargs)

    async def test_resume_reuses_successful_reads(self):
        first = await self.collect()
        second = await self.collect()
        self.assertEqual(first['items'], second['items'])
        self.assertEqual(self.sources.search.await_count, 1)
        self.assertEqual(self.sources.page.await_count, 1)
        self.assertEqual(second['distinct_domains'], 1)

    async def test_migration_archived_checkpoints_reject_reads_and_collection_without_overwrite(self):
        for migration in ({'blocked': True}, {'archived': True}, {'resume_allowed': False}):
            with self.subTest(migration=migration):
                key = self.research.key(self.actor, 'operation')
                saved = {'queries': ['original query'], 'searches': {'original query': {'ok': True, 'results': []}},
                         'pages': {}, 'scopes': ['C'], 'modules': ['assistant'], 'identity_migration': migration,
                         'historical_answer': 'original historical answer'}
                self.store.put_document('lighthouse_ai', key, saved)
                before = copy.deepcopy(self.store.docs)
                with self.assertRaises(AssistantError) as read_error:
                    self.research.read(self.actor, 'operation')
                self.assertEqual(read_error.exception.status, 409)
                with self.assertRaises(AssistantError) as collect_error:
                    await self.collect()
                self.assertEqual(collect_error.exception.status, 409)
                self.assertEqual(collect_error.exception.category, 'research_checkpoint_blocked')
                self.assertEqual(self.store.docs, before)
        self.sources.search.assert_not_awaited()
        self.sources.page.assert_not_awaited()
        self.emit.assert_not_awaited()

    async def test_actual_transformer_opaque_checkpoint_is_not_resumed(self):
        from lan_bitable_template_portal.feishu_identity_documents import transform_document
        key = self.research.key(self.actor, 'operation')
        previous = {'queries': ['original query'], 'searches': {}, 'pages': {},
                    'scopes': ['C'], 'modules': ['assistant']}
        identity, saved = transform_document('lighthouse_ai', key, previous,
                                             {'ou_FixturePrevious': 'ou_FixtureCurrent'})
        self.assertTrue(saved['identity_migration']['archived'])
        self.store.put_document('lighthouse_ai', identity, saved)
        with self.assertRaises(AssistantError) as caught:
            await self.collect()
        self.assertEqual(caught.exception.status, 409)
        self.assertEqual(self.store.get_document('lighthouse_ai', identity), saved)
        self.sources.search.assert_not_awaited()
        self.sources.page.assert_not_awaited()

    async def test_migrated_owner_hash_miss_cannot_restart_the_old_operation(self):
        from lan_bitable_template_portal.feishu_identity_migration import MARKER_TABLE
        with tempfile.TemporaryDirectory() as directory:
            self.store.db_path = Path(directory) / 'fixture.sqlite3'
            with closing(sqlite3.connect(self.store.db_path)) as db:
                db.execute(f'CREATE TABLE {MARKER_TABLE}(mappings_json TEXT NOT NULL)')
                db.execute(f'INSERT INTO {MARKER_TABLE} VALUES(?)',
                           (json.dumps({'ou_FixturePrevious': self.actor['id']}),))
                db.commit()
            old_key = self.research.key({**self.actor, 'id': 'ou_FixturePrevious'}, 'operation')
            saved = {'queries': ['original query'], 'searches': {}, 'pages': {}, 'scopes': ['C'],
                     'modules': ['assistant'], 'identity_migration': {'archived': True, 'resume_allowed': False}}
            self.store.put_document('lighthouse_ai', old_key, saved)
            current_key = self.research.key(self.actor, 'operation')
            self.assertNotEqual(old_key, current_key)
            with self.assertRaises(AssistantError) as caught:
                await self.collect()
            self.assertEqual(caught.exception.status, 409)
            self.sources.search.assert_not_awaited()
            self.sources.page.assert_not_awaited()
            self.assertIsNone(self.store.get_document('lighthouse_ai', current_key))
            self.assertEqual(self.store.get_document('lighthouse_ai', old_key), saved)
            shadow = {**saved, 'identity_migration': {}, 'queries': ['a new shadow query']}
            self.store.put_document('lighthouse_ai', current_key, shadow)
            with self.assertRaises(AssistantError) as caught:
                await self.collect()
            self.assertEqual(caught.exception.status, 409)
            self.sources.search.assert_not_awaited()
            self.sources.page.assert_not_awaited()
            self.assertEqual(self.store.get_document('lighthouse_ai', current_key), shadow)
            self.assertIsNone(self.research.read({**self.actor, 'id': 'another'}, 'operation'))
            self.assertIsNone(self.research.read({**self.actor, '_feishu_topic': {'id': 'other-topic'}}, 'operation'))
            await self.research.collect(self.actor, 'different-operation', ['Python'], self.sources,
                                        self.auth, self.emit, question='Python')
            self.sources.search.assert_awaited_once()
            self.sources.page.assert_awaited_once()
            self.assertEqual(self.store.get_document('lighthouse_ai', old_key), saved)

    async def test_unverifiable_migration_marker_never_falls_back_to_new_research(self):
        from lan_bitable_template_portal.feishu_identity_migration import MARKER_TABLE
        with tempfile.TemporaryDirectory() as directory:
            self.store.db_path = Path(directory) / 'fixture.sqlite3'
            with closing(sqlite3.connect(self.store.db_path)) as db:
                db.execute(f'CREATE TABLE {MARKER_TABLE}(mappings_json TEXT NOT NULL)')
                db.execute(f'INSERT INTO {MARKER_TABLE} VALUES(?)', ('invalid-json',))
                db.commit()
            with self.assertRaises(AssistantError) as caught:
                await self.collect()
            self.assertEqual(caught.exception.status, 409)
            self.sources.search.assert_not_awaited()
            self.sources.page.assert_not_awaited()
            self.assertEqual(self.store.docs, {})

    async def test_new_checkpoints_record_verifiable_identity_without_changing_the_key(self):
        await self.collect()
        saved = self.store.get_document('lighthouse_ai', self.research.key(self.actor, 'operation'))
        self.assertEqual((saved['owner'], saved['topic_id'], saved['operation_id']),
                         (self.actor['id'], '', 'operation'))

    async def test_verifiable_checkpoint_migrates_without_repeating_successful_reads(self):
        from lan_bitable_template_portal.feishu_identity_documents import transform_document
        first = await self.collect()
        old_key = self.research.key(self.actor, 'operation')
        saved = self.store.get_document('lighthouse_ai', old_key)
        current = {**self.actor, 'id': 'ou_FixtureCurrent'}
        key, migrated = transform_document('lighthouse_ai', old_key, saved, {self.actor['id']: current['id']})
        self.assertEqual(key, self.research.key(current, 'operation'))
        self.assertNotIn('identity_migration', migrated)
        self.store.docs.pop(('lighthouse_ai', old_key))
        self.store.put_document('lighthouse_ai', key, migrated)
        self.auth.side_effect = lambda: copy.deepcopy(current)
        second = await self.research.collect(current, 'operation', ['Python'], self.sources,
                                              self.auth, self.emit, question='Python')
        self.assertEqual(first['items'], second['items'])
        self.sources.search.assert_awaited_once()
        self.sources.page.assert_awaited_once()

    async def test_failed_original_page_is_retried_not_search(self):
        self.sources.page.side_effect = [TimeoutError(), {'ok': True, 'text': 'recovered'}]
        self.assertFalse((await self.collect())['complete'])
        self.assertTrue((await self.collect())['complete'])
        self.assertEqual(self.sources.search.await_count, 1)
        self.assertEqual(self.sources.page.await_count, 2)

    async def test_scope_and_topic_are_not_shared(self):
        await self.collect()
        with self.assertRaises(AssistantError):
            self.research.read({**self.actor, 'scopes': []}, 'operation')
        self.assertIsNone(self.research.read({**self.actor, 'id': 'another'}, 'operation'))
        self.assertIsNone(self.research.read({**self.actor, '_feishu_topic': {'id': 'another-topic'}}, 'operation'))

    async def test_revoke_between_reads_keeps_checkpoint_but_stops(self):
        async def search(*args):
            self.auth.side_effect = lambda: {**self.actor, 'scopes': []}
            return self.sources.search.return_value
        self.sources.search.side_effect = search
        with self.assertRaises(AssistantError):
            await self.collect()
        self.sources.page.assert_not_awaited()

    async def test_internal_queries_rejected_before_external_call(self):
        with self.assertRaises(AssistantError):
            await self.research.collect(self.actor, 'op', ['C楼维修记录'], self.sources,
                self.auth, self.emit, question='C楼维修记录')
        self.sources.search.assert_not_awaited()

    async def test_checkpoint_cancellation_keeps_completed_search(self):
        import asyncio
        self.sources.page.side_effect = asyncio.CancelledError()
        with self.assertRaises(asyncio.CancelledError):
            await self.collect()
        self.sources.page.side_effect = None
        await self.collect()
        self.assertEqual(self.sources.search.await_count, 1)

    async def test_long_page_excerpt_is_never_labeled_full_text(self):
        self.sources.page.return_value = {'ok': True, 'text': '文' * 8000, 'truncated': False}
        page = (await self.collect())['items'][0]['page']
        self.assertTrue(page['truncated'])
        self.assertEqual(len(page['text']), 5000)


class ResearchRecoveryTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = fixtures.StreamTests.asyncSetUp
    asyncTearDown = fixtures.StreamTests.asyncTearDown
    authorize = fixtures.StreamTests.authorize
    payload = fixtures.StreamTests.payload
    submit = fixtures.StreamTests.submit
    finish = fixtures.StreamTests.finish
    async def test_research_restart_can_resume_original_operation(self):
        ack = await self.submit('/深入研究 Python 等待')
        await self.engine.started.wait()
        await self.runtime.stop(self.actor, ack['run_id'])
        state = await self.runtime.conversation(self.actor)
        self.assertTrue(state['turns'][-1]['resume_available'])
        self.engine.gate.set()
        resumed = await self.runtime.resume_research(self.actor, ack['run_id'], self.request, self.authorize)
        await self.finish()
        final = await self.runtime.conversation(self.actor)
        self.assertNotEqual(resumed['run_id'], ack['run_id'])
        self.assertEqual(len(final['turns']), 1)
        self.assertEqual(final['turns'][0]['operation_id'], ack['turn']['operation_id'])
        self.assertEqual(final['turns'][0]['status'], 'completed')

    async def test_business_turn_cannot_use_research_recovery(self):
        ack = await self.submit('维修等待')
        await self.engine.started.wait()
        await self.runtime.stop(self.actor, ack['run_id'])
        with self.assertRaises(AssistantError):
            await self.runtime.resume_research(self.actor, ack['run_id'], self.request, self.authorize)

    async def test_repeated_resume_click_reuses_accepted_attempt(self):
        import asyncio
        ack = await self.submit('/深入研究 Python 等待')
        await self.engine.started.wait()
        await self.runtime.stop(self.actor, ack['run_id'])
        first, second = await asyncio.gather(*[self.runtime.resume_research(
            self.actor, ack['run_id'], self.request, self.authorize) for _ in range(2)])
        self.assertEqual(first['run_id'], second['run_id'])
        self.assertEqual(len(self.engine.calls), 2)
        self.engine.gate.set()
        await self.finish()

    async def test_restart_without_conversation_read_marks_old_run_terminal(self):
        import asyncio
        from openclaw_service.assistant.lighthouse_stream import LighthouseStream
        ack = await self.submit('/深入研究 Python 等待')
        await self.engine.started.wait()
        await self.runtime.close()
        stale = self.store.get_document('lighthouse_runs', ack['run_id'])
        stale.update(status='running', revision=stale['revision'] + 1)
        self.store.put_document('lighthouse_runs', ack['run_id'], stale)
        self.runtime = LighthouseStream(self.portal, engine=self.engine)
        self.engine.gate.set()
        resumed = await self.runtime.resume_research(self.actor, ack['run_id'], self.request, self.authorize)
        await self.finish()
        self.assertEqual(self.runtime.get_run(self.actor, ack['run_id'])['status'], 'stopped')
        self.assertEqual(self.runtime.get_run(self.actor, resumed['run_id'])['status'], 'completed')

    async def test_topic_research_cannot_resume_from_personal_context(self):
        self.actor.update(channel='feishu:oc_Research', _feishu_topic={'id': 'topic-fixture'})
        ack = await self.submit('/深入研究 Python 等待')
        await self.engine.started.wait()
        await self.runtime.stop(self.actor, ack['run_id'])
        self.actor.pop('_feishu_topic')
        self.actor['channel'] = ''
        with self.assertRaises(AssistantError):
            await self.runtime.resume_research(self.actor, ack['run_id'], self.request, self.authorize)

    async def test_automatically_selected_research_keeps_recovery_after_restart(self):
        import asyncio
        async def research(actor, turn, history, request, emit, authorize, context):
            await emit('research', {'label': '正在深入研究'})
            self.engine.started.set()
            await self.engine.gate.wait()
            return {'answer': '已完成研究', 'sources': []}
        self.engine.answer = research
        ack = await self.submit('请比较Python的不同方案')
        await self.engine.started.wait()
        await self.runtime.stop(self.actor, ack['run_id'])
        state = await self.runtime.conversation(self.actor)
        self.assertTrue(state['turns'][-1]['resume_available'])
        self.engine.gate.set()
        await self.runtime.resume_research(self.actor, ack['run_id'], self.request, self.authorize)
        await self.finish()
        self.assertTrue(self.engine.calls == [])
        self.assertEqual((await self.runtime.conversation(self.actor))['turns'][-1]['answer'], '已完成研究')

