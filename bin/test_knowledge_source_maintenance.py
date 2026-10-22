"""Offline maintenance tests for the existing KB, with no worker or model loads."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch
import uuid

from bin.test_lighthouse_knowledge import FakeEmbedder, FakeMemoryIndex, actor
from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_knowledge import KnowledgeBase


class SourceMaintenanceTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(patch('socket.socket.connect', side_effect=AssertionError('No network')))
        self.enterContext(patch('openclaw_service.assistant.lighthouse_knowledge._load_local', return_value=None))
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.current = set()
        self.kb = self.open_kb()

    def open_kb(self):
        kb = KnowledgeBase(self.root / 'knowledge', embedder=FakeEmbedder(),
            memory_index_factory=FakeMemoryIndex, start_worker=False,
            source_current=lambda source: (source['table_key'], source['record_id']) in self.current,
            source_acl=lambda user, source: user['id'] == 'u1' and (source['table_key'], source['record_id']) in self.current)
        self.addCleanup(kb.close)
        return kb

    def add_source(self, key='manuals/recOne/record', content=b'Alpha maintenance procedure', *, index=True):
        table, record, _ = key.split('/', 2)
        source = {'table_key': table, 'record_id': record, 'origin': 'fixture-origin',
            'record_revision': 'record-v1', 'config_revision': 'config-v1', 'scopes': ['A']}
        self.current.add((table, record))
        doc = self.kb.upsert_source(key, source, 'guide.txt', content)
        if index:
            self.assertTrue(self.kb.process_one())
        return key, source, doc['id']

    def row(self, identity):
        with self.kb.connect() as db:
            row = db.execute('SELECT * FROM documents WHERE id=?', (identity,)).fetchone()
            return dict(row) if row else None

    def versions(self, identity):
        with self.kb.connect() as db:
            return [dict(row) for row in db.execute('SELECT * FROM versions WHERE document_id=? ORDER BY version', (identity,))]

    def revoke(self, source):
        self.current.discard((source['table_key'], source['record_id']))

    def assert_removed(self, identity, chunk_ids=()):
        self.assertIsNone(self.row(identity))
        with self.kb.connect() as db:
            for table in ('chunks', 'index_staging', 'source_retry', 'doc_group_members', 'source_versions', 'versions', 'document_sources'):
                self.assertEqual(db.execute(f'SELECT count(*) FROM {table} WHERE document_id=?', (identity,)).fetchone()[0], 0, table)
            for chunk_id in chunk_ids:
                self.assertIsNone(db.execute('SELECT 1 FROM chunks_fts WHERE rowid=?', (chunk_id,)).fetchone())
                self.assertIsNone(db.execute('SELECT 1 FROM local_vectors WHERE chunk_id=?', (chunk_id,)).fetchone())

    def test_ready_same_content_is_reembedded_without_new_version_or_file_write(self):
        key, source, identity = self.add_source()
        before = self.versions(identity)
        original = self.kb.root / before[0]['path']
        bytes_before, mtime = original.read_bytes(), original.stat().st_mtime_ns
        with self.kb.connect() as db:
            vector = db.execute('SELECT embedding FROM local_vectors').fetchone()[0]
        old_calls = len(self.kb.embedder.calls)
        self.kb.upsert_source(key, source, 'guide.txt', bytes_before)
        self.assertEqual(len(self.kb.embedder.calls), old_calls)
        request = str(uuid.uuid4())
        queued = self.kb.reindex_source(key, source, request)
        self.assertEqual(queued['status'], 'queued')
        self.assertFalse(queued['duplicate'])
        real_vector = self.kb.embedder.vector_for
        with patch.object(self.kb.embedder, 'vector_for', side_effect=lambda text: list(reversed(real_vector(text)))):
            self.assertTrue(self.kb.process_one())
        self.assertEqual(len(self.kb.embedder.calls), old_calls + 1)
        self.assertEqual(self.row(identity)['status'], 'ready')
        self.assertEqual(self.versions(identity), before)
        self.assertEqual((original.read_bytes(), original.stat().st_mtime_ns), (bytes_before, mtime))
        with self.kb.connect() as db:
            self.assertNotEqual(db.execute('SELECT embedding FROM local_vectors').fetchone()[0], vector)
        self.assertTrue(self.kb.reindex_source(key, source, request)['duplicate'])
        self.assertFalse(self.kb.process_one())

    def test_receipt_survives_restart_before_and_after_indexing(self):
        key, source, identity = self.add_source()
        request = str(uuid.uuid4())
        self.kb.reindex_source(key, source, request)
        queued = self.row(identity)
        self.kb.close()
        self.kb = self.open_kb()
        self.assertTrue(self.kb.reindex_source(key, source, request.upper())['duplicate'])
        self.assertEqual(self.row(identity), queued)
        self.assertTrue(self.kb.process_one())
        self.assertEqual(len(self.kb.embedder.calls), 1)
        self.kb.close()
        self.kb = self.open_kb()
        self.assertTrue(self.kb.reindex_source(key, source, request)['duplicate'])
        self.assertFalse(self.kb.process_one())
        self.assertEqual(self.kb.embedder.calls, [])
        self.kb.reindex_source(key, source, str(uuid.uuid4()))
        self.assertTrue(self.kb.process_one())

    def test_duplicate_request_does_not_interrupt_inflight_index(self):
        key, source, identity = self.add_source()
        request = str(uuid.uuid4())
        self.kb.reindex_source(key, source, request)
        entered, release = threading.Event(), threading.Event()
        real_embed = self.kb.embedder.embed

        def blocked(*args, **kwargs):
            entered.set()
            self.assertTrue(release.wait(10))
            return real_embed(*args, **kwargs)

        with patch.object(self.kb.embedder, 'embed', side_effect=blocked), ThreadPoolExecutor(1) as pool:
            future = pool.submit(self.kb.process_one)
            try:
                self.assertTrue(entered.wait(10))
                indexing = self.row(identity)
                self.assertTrue(self.kb.reindex_source(key, source, request)['duplicate'])
                self.assertEqual(self.row(identity), indexing)
            finally:
                release.set()
            self.assertTrue(future.result(timeout=10))
        self.assertEqual(self.row(identity)['status'], 'ready')
        self.assertFalse(self.kb.process_one())

    def test_failed_request_recovers_on_existing_retry_queue_after_restart(self):
        key, source, identity = self.add_source()
        request = str(uuid.uuid4())
        self.kb.reindex_source(key, source, request)
        self.kb.embedder.fail = True
        self.assertTrue(self.kb.process_one())
        failed = self.row(identity)
        self.assertEqual(failed['status'], 'failed')
        with self.kb.connect() as db:
            retry = tuple(db.execute('SELECT * FROM source_retry').fetchone())
        self.assertTrue(self.kb.reindex_source(key, source, request)['duplicate'])
        self.assertEqual(self.row(identity), failed)
        self.kb.close()
        self.kb = self.open_kb()
        self.assertTrue(self.kb.reindex_source(key, source, request)['duplicate'])
        with self.kb.connect() as db:
            self.assertEqual(tuple(db.execute('SELECT * FROM source_retry').fetchone()), retry)
            db.execute('UPDATE source_retry SET next_attempt=0')
        self.assertTrue(self.kb.process_one())
        self.assertEqual(self.row(identity)['status'], 'ready')
        self.assertEqual(len(self.versions(identity)), 1)

    def test_restart_backfills_legacy_failed_source_without_retry_receipt(self):
        _, _, identity = self.add_source(index=False)
        with self.kb.connect() as db:
            db.execute("UPDATE documents SET status='failed',error='fixture old failure' WHERE id=?", (identity,))
            self.assertEqual(db.execute('SELECT count(*) FROM source_retry').fetchone()[0], 0)
        self.kb.close()
        self.kb = self.open_kb()
        with self.kb.connect() as db:
            self.assertEqual(tuple(db.execute('SELECT * FROM source_retry').fetchone()), (identity, 0, 0))
        self.assertTrue(self.kb.process_one())
        self.assertEqual(self.row(identity)['status'], 'ready')
        self.assertEqual(len(self.versions(identity)), 1)
        self.kb.close()
        self.kb = self.open_kb()
        self.assertFalse(self.kb.process_one())
        with self.kb.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM source_retry').fetchone()[0], 0)

    def test_legacy_retry_backfill_preserves_backoff_and_ignores_ineligible_documents(self):
        ids = {kind: self.add_source('manuals/rec' + kind + '/record', index=False)[2]
            for kind in ('Backoff', 'Blocked', 'Disabled', 'Deleted', 'NoPending', 'Manual')}
        with self.kb.connect() as db:
            db.execute("UPDATE documents SET status='failed'")
            db.execute('INSERT INTO source_retry VALUES(?,?,?)', (ids['Backoff'], 5, 9999999999))
            db.execute("UPDATE documents SET status='blocked' WHERE id=?", (ids['Blocked'],))
            db.execute('UPDATE document_sources SET enabled=0 WHERE document_id=?', (ids['Disabled'],))
            db.execute('UPDATE documents SET deleted=1 WHERE id=?', (ids['Deleted'],))
            db.execute('UPDATE documents SET pending_version=NULL WHERE id=?', (ids['NoPending'],))
            db.execute("UPDATE documents SET source_key='',owner='u1' WHERE id=?", (ids['Manual'],))
            db.execute('DELETE FROM document_sources WHERE document_id=?', (ids['Manual'],))
        self.kb.close()
        self.kb = self.open_kb()
        with self.kb.connect() as db:
            self.assertEqual([tuple(row) for row in db.execute('SELECT * FROM source_retry')], [(ids['Backoff'], 5, 9999999999)])
        self.assertFalse(self.kb.process_one())

    def test_restart_recovers_interrupted_indexing_without_requeueing_duplicate_request(self):
        key, source, identity = self.add_source()
        request = str(uuid.uuid4())
        self.kb.reindex_source(key, source, request)
        real_embed = self.kb.embedder.embed

        def interrupted(*args, **kwargs):
            result = real_embed(*args, **kwargs)
            self.kb._stop.set()
            return result

        with patch.object(self.kb.embedder, 'embed', side_effect=interrupted):
            self.assertFalse(self.kb.process_one())
        self.assertEqual(self.row(identity)['status'], 'indexing')
        self.kb.close()
        self.kb = self.open_kb()
        recovered = self.row(identity)
        self.assertEqual(recovered['status'], 'queued')
        self.assertTrue(self.kb.reindex_source(key, source, request)['duplicate'])
        self.assertEqual(self.row(identity), recovered)
        self.assertTrue(self.kb.process_one())
        self.assertEqual(self.row(identity)['status'], 'ready')
        self.assertEqual(len(self.kb.embedder.calls), 1)

    def test_receipt_and_queue_commit_atomically(self):
        key, source, identity = self.add_source()
        request, before = str(uuid.uuid4()), self.row(identity)
        with patch.object(self.kb, '_bump', side_effect=RuntimeError('rollback fixture')):
            with self.assertRaises(RuntimeError):
                self.kb.reindex_source(key, source, request)
        self.assertEqual(self.row(identity), before)
        self.assertFalse(self.kb.reindex_source(key, source, request)['duplicate'])
        self.assertTrue(self.kb.process_one())

    def test_latest_pending_version_is_reindexed_and_receipts_are_source_scoped(self):
        key, source, identity = self.add_source()
        self.kb.upsert_source(key, source, 'guide.txt', b'Updated maintenance procedure')
        before = self.versions(identity)
        request = str(uuid.uuid4())
        self.kb.reindex_source(key, source, request)
        self.assertEqual(self.row(identity)['pending_version'], 2)
        self.assertTrue(self.kb.process_one())
        self.assertEqual(self.row(identity)['active_version'], 2)
        self.assertEqual(self.versions(identity), before)
        other, provenance, _ = self.add_source('other/recOne/record')
        self.assertFalse(self.kb.reindex_source(other, provenance, request)['duplicate'])

    def test_reindex_rejects_invalid_request_and_stale_or_unmapped_provenance(self):
        key, source, identity = self.add_source()
        before = self.row(identity)
        for request in (None, '', 'not-a-uuid', uuid.uuid4(), uuid.uuid4().hex):
            with self.subTest(request=request), self.assertRaises(AssistantError):
                self.kb.reindex_source(key, source, request)
        for altered in ({**source, 'scopes': ['B']}, {**source, 'record_revision': 'stale'}):
            with self.assertRaises(AssistantError):
                self.kb.reindex_source(key, altered, str(uuid.uuid4()))
        self.revoke(source)
        with self.assertRaises(AssistantError):
            self.kb.reindex_source(key, source, str(uuid.uuid4()))
        self.assertEqual(self.row(identity), before)
        with self.kb.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM source_reindex_receipts').fetchone()[0], 0)
            db.execute('DELETE FROM document_sources WHERE document_id=?', (identity,))
        with self.assertRaises(AssistantError):
            self.kb.reindex_source(key, source, str(uuid.uuid4()))

    def test_remove_all_history_preserves_manual_and_other_tables_and_invalidates_cache(self):
        key, source, identity = self.add_source()
        self.kb.upsert_source(key, source, 'guide.txt', b'Alpha revised maintenance procedure')
        self.assertTrue(self.kb.process_one())
        originals = [self.kb.root / v['path'] for v in self.versions(identity)]
        _, _, other = self.add_source('manuals_extra/recOne/record')
        manual = self.kb.upload(actor(), 'manual.txt', b'Manual knowledge untouched')['id']
        self.assertTrue(self.kb.process_one())
        unaffected = {doc: self.row(doc) for doc in (other, manual)}
        hit = next(item for item in self.kb.search(actor(), 'Alpha')['items'] if item['document_id'] == identity)
        turn = {'sources': [{'kind': 'company_knowledge', 'data': hit}], 'output_files': [{'id': 'fixture-artifact'}]}
        self.kb.bind_source_artifacts(turn)
        result = self.kb.search(actor(), 'Alpha')
        with self.kb.connect() as db:
            ids = [row[0] for row in db.execute('SELECT id FROM chunks WHERE document_id=?', (identity,))]
            self.assertEqual(len(ids), 2)
            blob = db.execute('SELECT embedding FROM local_vectors LIMIT 1').fetchone()[0]
            for chunk_id in ids:
                db.execute('INSERT OR REPLACE INTO local_vectors VALUES(?,?)', (chunk_id, blob))
            db.execute('INSERT INTO index_staging VALUES(?,?,?)', (identity, 0, blob))
            db.execute('INSERT INTO source_retry VALUES(?,?,?)', (identity, 1, 0))
            db.execute('INSERT INTO doc_group_members VALUES(?,?)', (identity, 'fixture-group'))
            revision, vector_revision = self.kb._revision(db), self.kb._vector_revision(db)
        self.revoke(source)
        self.assertEqual(self.kb.remove_sources('manuals/'), {'removed': 1, 'skipped': 0})
        self.assert_removed(identity, ids)
        self.assertTrue(all(not path.exists() for path in originals))
        self.assertEqual({doc: self.row(doc) for doc in unaffected}, unaffected)
        self.assertEqual(self.kb._snapshot.count, 2)
        self.assertFalse(self.kb.result_current(actor(), result))
        self.assertFalse(self.kb.cached_turn_current(actor(), turn))
        self.assertFalse(self.kb.artifact_current(actor(), 'fixture-artifact'))
        self.assertFalse(self.kb.search(actor('unauthorized'), 'Alpha')['items'])
        self.assertNotIn(identity, {item['document_id'] for item in self.kb.search(actor(), 'Alpha')['items']})
        with self.kb.connect() as db:
            self.assertEqual((self.kb._revision(db), self.kb._vector_revision(db)), (revision + 1, vector_revision + 1))
        self.assertEqual(self.kb.remove_sources('manuals/'), {'removed': 0, 'skipped': 0})
        with self.kb.connect() as db:
            self.assertEqual((self.kb._revision(db), self.kb._vector_revision(db)), (revision + 1, vector_revision + 1))

    def test_prefix_is_bounded_and_record_boundary_is_literal(self):
        _, first, one = self.add_source('manuals/recOne/record')
        _, second, two = self.add_source('manuals/recOneMore/record')
        self.revoke(first)
        self.revoke(second)
        for prefix in ('', '/', 'manuals', 'man', '%/', 'manuals/rec', 'manuals//', 'manuals/recOne/record/', '../', 'manuals/../'):
            with self.subTest(prefix=prefix), self.assertRaises(AssistantError):
                self.kb.remove_sources(prefix)
        self.assertEqual(self.kb.remove_sources('_/')['removed'], 0)
        self.assertEqual(self.kb.remove_sources('manuals/recOne/')['removed'], 1)
        self.assert_removed(one)
        self.assertIsNotNone(self.row(two))

    def test_current_sources_are_never_removed_even_when_disabled_or_deleted(self):
        _, source, identity = self.add_source()
        with self.kb.connect() as db:
            db.execute('UPDATE document_sources SET enabled=0 WHERE document_id=?', (identity,))
            db.execute('UPDATE documents SET deleted=1 WHERE id=?', (identity,))
        before = self.row(identity)
        self.assertEqual(self.kb.remove_sources('manuals/'), {'removed': 0, 'skipped': 1})
        self.assertEqual(self.row(identity), before)
        with patch.object(self.kb, 'source_current', None), self.assertRaises(AssistantError):
            self.kb.remove_sources('manuals/')
        with patch.object(self.kb, 'source_current', return_value=None):
            self.assertEqual(self.kb.remove_sources('manuals/')['removed'], 0)
        with patch.object(self.kb, 'source_current', side_effect=RuntimeError('unavailable')):
            with self.assertRaises(RuntimeError):
                self.kb.remove_sources('manuals/')
        self.revoke(source)
        self.assertEqual(self.kb.remove_sources('manuals/')['removed'], 1)
        self.assert_removed(identity)

    def test_removed_source_can_be_upserted_again_without_deleted_tombstone(self):
        key, source, identity = self.add_source()
        request = str(uuid.uuid4())
        self.kb.reindex_source(key, source, request)
        self.revoke(source)
        self.kb.remove_sources('manuals/')
        _, _, replacement = self.add_source(key, b'Completely new content')
        self.assertEqual(replacement, identity)
        self.assertEqual(self.row(identity)['active_version'], 1)
        self.assertEqual(len(self.versions(identity)), 1)
        self.assertTrue(self.kb.reindex_source(key, source, request)['duplicate'])
        self.assertFalse(self.kb.process_one())
        self.assertFalse(self.kb.reindex_source(key, source, str(uuid.uuid4()))['duplicate'])
        self.assertTrue(self.kb.process_one())

    def test_inflight_embedding_or_failure_cannot_revive_removed_or_recreated_source(self):
        for reattach in (False, True):
            for fail in (False, True):
                with self.subTest(reattach=reattach, fail=fail):
                    key = f'table{int(reattach)}{int(fail)}/recOne/record'
                    key, source, identity = self.add_source(key, index=False)
                    entered, release = threading.Event(), threading.Event()
                    real_embed = self.kb.embedder.embed

                    def blocked(*args, **kwargs):
                        entered.set()
                        self.assertTrue(release.wait(10))
                        if fail:
                            raise AssistantError('fixture failure', 503)
                        return real_embed(*args, **kwargs)

                    with patch.object(self.kb.embedder, 'embed', side_effect=blocked), ThreadPoolExecutor(1) as pool:
                        future = pool.submit(self.kb.process_one)
                        try:
                            self.assertTrue(entered.wait(10))
                            self.revoke(source)
                            self.kb.remove_sources(source['table_key'] + '/')
                            self.assert_removed(identity)
                            if reattach:
                                self.add_source(key, b'New replacement content', index=False)
                                replacement = self.row(identity)
                        finally:
                            release.set()
                        self.assertTrue(future.result(timeout=10))
                    if reattach:
                        self.assertEqual(self.row(identity), replacement)
                        with self.kb.connect() as db:
                            self.assertEqual(db.execute('SELECT count(*) FROM source_retry WHERE document_id=?', (identity,)).fetchone()[0], 0)
                        self.assertTrue(self.kb.process_one())
                        with self.kb.connect() as db:
                            texts = [row[0] for row in db.execute('SELECT text FROM chunks WHERE document_id=?', (identity,))]
                        self.assertEqual(texts, ['New replacement content'])
                    else:
                        self.assert_removed(identity)

    def test_removal_after_staging_fences_final_commit(self):
        key, source, identity = self.add_source(index=False)
        entered, release = threading.Event(), threading.Event()
        calls = 0

        def before_commit():
            nonlocal calls
            calls += 1
            if calls == 2:
                entered.set()
                self.assertTrue(release.wait(10))
            return False

        with patch.object(self.kb._stop, 'is_set', side_effect=before_commit), ThreadPoolExecutor(1) as pool:
            future = pool.submit(self.kb.process_one)
            try:
                self.assertTrue(entered.wait(10))
                with self.kb.connect() as db:
                    self.assertEqual(db.execute('SELECT count(*) FROM index_staging').fetchone()[0], 1)
                self.revoke(source)
                self.kb.remove_sources('manuals/')
                self.add_source(key, b'Replacement after staging', index=False)
                replacement = self.row(identity)
            finally:
                release.set()
            self.assertTrue(future.result(timeout=10))
        self.assertEqual(self.row(identity), replacement)
        self.assertTrue(self.kb.process_one())
        self.assertEqual(self.row(identity)['status'], 'ready')

    def test_old_faiss_search_is_filtered_after_removal_and_rowid_reuse(self):
        _, _, keep = self.add_source('keep/recOne/record', b'Unrelated retained source')
        key, source, identity = self.add_source()
        old_snapshot = self.kb._snapshot
        real_search = old_snapshot.search

        def remove_during_search(*args, **kwargs):
            hits = real_search(*args, **kwargs)
            self.revoke(source)
            self.kb.remove_sources('manuals/')
            self.add_source(key, b'Entirely different replacement')
            return hits

        with patch.object(old_snapshot, 'search', side_effect=remove_during_search):
            result = self.kb.search(actor(), 'Alpha maintenance procedure')
        self.assertEqual(result['items'], [])
        self.assertFalse(result['indexready'])
        self.assertIsNotNone(self.row(keep))
        self.assertIsNotNone(self.row(identity))

    def test_snapshot_build_failure_excludes_source_and_zero_removal_retry_republishes(self):
        self.add_source('keep/recOne/record', b'Other searchable source')
        _, source, identity = self.add_source()
        old = self.kb._snapshot
        self.revoke(source)
        with patch.object(self.kb, '_build_snapshot', side_effect=RuntimeError('fixture snapshot unavailable')) as build:
            with self.assertRaises(AssistantError) as failure:
                self.kb.remove_sources('manuals/')
            self.assertEqual(failure.exception.status, 503)
            self.assertIs(self.kb._snapshot, old)
            self.assert_removed(identity)
            with self.kb.connect() as db:
                revisions = self.kb._revision(db), self.kb._vector_revision(db)
            self.assertEqual(build.call_count, 1)
            with self.assertRaises(AssistantError):
                self.kb.remove_sources('manuals/')
            self.assertEqual(build.call_count, 2)
            result = self.kb.search(actor(), 'Alpha maintenance procedure')
        self.assertEqual(result['items'], [])
        self.assertFalse(result['indexready'])
        self.assertEqual(self.kb.remove_sources('manuals/'), {'removed': 0, 'skipped': 0})
        self.assertIsNot(self.kb._snapshot, old)
        self.assertEqual(self.kb._snapshot.count, 1)
        self.assertEqual(self.kb._snapshot.revision, revisions[1])
        with self.kb.connect() as db:
            self.assertEqual((self.kb._revision(db), self.kb._vector_revision(db)), revisions)

    def test_snapshot_publication_exception_recovers_after_committed_deletion(self):
        _, source, identity = self.add_source()
        original = self.kb.root / self.versions(identity)[0]['path']
        self.revoke(source)
        with patch.object(self.kb, '_publish_snapshot', side_effect=RuntimeError('fixture publish failure')):
            with self.assertRaises(RuntimeError):
                self.kb.remove_sources('manuals/')
        self.assert_removed(identity)
        self.assertFalse(original.exists())
        self.assertEqual(self.kb.remove_sources('manuals/'), {'removed': 0, 'skipped': 0})
        with self.kb.connect() as db:
            self.assertEqual(self.kb._snapshot.revision, self.kb._vector_revision(db))
        self.assertEqual(self.kb._snapshot.count, 0)

    def test_original_cleanup_preserves_shared_or_uncontrolled_paths(self):
        for shared in (False, True):
            with self.subTest(shared=shared):
                key, source, identity = self.add_source(f'table{int(shared)}/recOne/record')
                version = self.versions(identity)[0]
                original = self.kb.root / version['path']
                if shared:
                    manual = self.kb.upload(actor(), 'manual.txt', b'Shared path fixture')['id']
                    with self.kb.connect() as db:
                        db.execute('UPDATE versions SET path=? WHERE document_id=?', (version['path'], manual))
                else:
                    original = self.root / 'external.txt'
                    original.write_bytes(b'Uncontrolled original must survive')
                    with self.kb.connect() as db:
                        db.execute('UPDATE versions SET path=? WHERE document_id=?', (str(original), identity))
                content = original.read_bytes()
                self.revoke(source)
                self.kb.remove_sources(source['table_key'] + '/')
                self.assert_removed(identity)
                self.assertEqual(original.read_bytes(), content)

    def test_original_cleanup_rechecks_reference_after_concurrent_reattach(self):
        key, source, identity = self.add_source()
        original = self.kb.root / self.versions(identity)[0]['path']
        self.revoke(source)
        connect = self.kb.connect
        pending = True

        @contextmanager
        def reattach_after_commit(*args, **kwargs):
            nonlocal pending
            with connect(*args, **kwargs) as db:
                yield db
            if pending:
                pending = False
                self.add_source(key, b'Concurrent replacement original', index=False)

        with patch.object(self.kb, 'connect', side_effect=reattach_after_commit):
            self.kb.remove_sources('manuals/')
        self.assertEqual(original.read_bytes(), b'Concurrent replacement original')
        self.assertTrue(self.kb.process_one())
        self.assertEqual(self.row(identity)['status'], 'ready')


if __name__ == '__main__':
    unittest.main()
