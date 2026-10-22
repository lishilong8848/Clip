"""Offline coverage for full source ingestion into the existing shared index."""
import copy
import io
import json
import unittest
from unittest.mock import patch

from bin.test_document_library import LibraryFixture, actor, configuration
from bin.test_lighthouse_knowledge import FakeEmbedder, FakeMemoryIndex
from lan_bitable_template_portal.document_library import DocumentLibraryService
from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_knowledge import KnowledgeBase


class FullSourceKnowledgeTests(LibraryFixture):
    def setUp(self):
        super().setUp()
        self.enterContext(patch('socket.socket.connect', side_effect=AssertionError('No network')))

    def sync(self):
        self.service.request_sync(actor(), 'manuals')
        self.drain()

    def record_hit(self, term, user=None):
        hits = self.kb.search(user or actor(), term)['items']
        return next(item for item in hits if item['name'] == 'recOne.txt')

    def test_listing_checks_index_status_only_for_the_authorized_page(self):
        with patch.object(self.service, '_public', wraps=self.service._public) as render:
            first = self.service.list_records(self.admin, 'manuals', {'page_size': 1})
            self.assertEqual(render.call_count, 1)
            self.assertEqual((first['total'], first['has_more'], first['cursor']), (2, True, '1'))
            self.assertEqual(first['items'][0]['record_id'], 'recOne')
            render.reset_mock()
            second = self.service.list_records(self.admin, 'manuals', {'page_size': 1, 'cursor': '1'})
            self.assertEqual(render.call_count, 1)
            self.assertEqual(second['items'][0]['record_id'], 'recTwo')
            self.assertFalse(second['has_more'])
            for query in ({'cursor': '2'}, {'q': 'nomatchingfixture'}, {'scope': 'B'}):
                render.reset_mock()
                result = self.service.list_records(actor(), 'manuals', query)
                self.assertEqual(render.call_count, 0)
                self.assertEqual(result['items'], [])

    def test_field_only_records_include_unselected_structured_fields_and_false_values(self):
        fields = self.remote.rows['recOne']['fields']
        fields.update(Title='Fieldonlyneedle', Files=[], Opaque={'nested': ['Hiddenneedle', False, 0]},
            Formula='Calculatedneedle')
        self.sync()
        hit = self.record_hit('Hiddenneedle')
        self.assertEqual(hit['source_kind'], 'document_library')
        text = self.kb.file(actor(), hit['document_id'])[0].read_text(encoding='utf-8')
        for value in ('Opaque (fldOpaque)', 'Hiddenneedle', 'false', '0', 'Calculatedneedle'):
            self.assertIn(value, text)
        self.assertEqual(self.record()['knowledge_status'], 'ready')
        self.assertFalse(any(item['name'] == 'recOne.txt' for item in self.kb.search(actor(('B',)), 'Hiddenneedle')['items']))
        self.assertEqual((self.remote.updates, self.remote.creates, self.remote.uploads), (0, 0, 0))

    def test_display_and_attachment_selection_do_not_remove_source_knowledge(self):
        source = self.service._table('manuals')
        self.service.save_table(self.admin, configuration(display_fields=['fldNumber'], attachment_fields=[],
            expected_revision=source['revision']), key='manuals')
        self.drain()
        self.assertTrue(self.record_hit('Alpha'))
        self.assertTrue(self.source_items())
        public = self.record()
        self.assertNotIn('file_token', json.dumps(public))
        attachment = public['attachments'][0]
        self.assertEqual(self.service.download(actor(), 'manuals', 'recOne', attachment['attachment_id'])[0].read_bytes(),
            b'Alpha policy service steps')
        with self.assertRaises(AssistantError):
            self.service.upload_attachment(actor(), 'manuals', 'recOne', {
                'field_id': 'fldFiles', 'expected_revision': public['revision'],
                'operation_id': 'fixture-disabled-upload', 'reason': 'fixture'}, 'file.txt', io.BytesIO(b'new'))
        self.assertEqual((self.remote.updates, self.remote.uploads), (0, 0))

    def test_every_attachment_field_uses_existing_ocr_and_has_explicit_status(self):
        schema = [*self.remote.fields(), {'field_id': 'fldPictures', 'field_name': 'Pictures', 'type': 17}]
        self.enterContext(patch.object(self.remote, 'fields', return_value=schema))
        self.remote.files['tokenImageFixture'] = b'fixture image bytes'
        self.remote.files['tokenZipFixture'] = b'fixture archived original'
        self.remote.rows['recOne']['fields']['Pictures'] = [
            {'file_token': 'tokenImageFixture', 'name': 'drawing.png', 'size': 19},
            {'file_token': 'tokenZipFixture', 'name': 'archive.zip', 'size': 99}]
        with patch('openclaw_service.assistant.lighthouse_knowledge_extract.recognize_text',
                return_value='Imageocrneedle wiring steps') as ocr:
            self.sync()
        ocr.assert_called_once_with(b'fixture image bytes')
        public = self.record()
        states = {item['name']: item for item in public['knowledge_items']}
        self.assertEqual(states['drawing.png']['status'], 'ready')
        self.assertEqual(states['archive.zip']['status'], 'unsupported')
        self.assertTrue(states['archive.zip']['error'])
        self.assertEqual(public['knowledge_status'], 'unsupported')
        self.assertEqual(len(public['attachments']), 3)
        self.assertNotIn('file_token', json.dumps(public))
        hit = next(item for item in self.kb.search(actor(), 'Imageocrneedle')['items'] if item['name'] == 'drawing.png')
        self.assertIn('OCR', hit['location'])
        self.assertIn('Imageocrneedle', hit['text'])
        embedded = '\n'.join(text for texts, _, query in self.kb.embedder.calls if not query for text in texts)
        self.assertNotIn('fixture image bytes', embedded)
        self.assertNotIn('tokenImageFixture', embedded)
        self.assertNotIn('tokenZipFixture', embedded)

    def test_empty_image_ocr_reports_failure_without_claiming_image_is_indexed(self):
        self.remote.rows['recOne']['fields']['Files'] = [
            {'file_token': 'tokenNoTextImage', 'name': 'empty.png', 'size': 12}]
        self.remote.files['tokenNoTextImage'] = b'empty image'
        with patch('openclaw_service.assistant.lighthouse_knowledge_extract.recognize_text', return_value=''):
            self.sync()
        item = next(item for item in self.record()['knowledge_items'] if item['name'] == 'empty.png')
        self.assertEqual(item['status'], 'failed')
        self.assertTrue(item['error'])
        self.assertFalse(any(item['name'] == 'empty.png' for item in self.kb.search(actor(), 'Alpha')['items']))

    def test_digest_dedup_updates_body_but_reuses_attachment_version(self):
        attachment = self.source_items()[0]
        original = self.record_hit('Alpha')
        before_calls = len(self.kb.embedder.calls)
        attachment_embeddings = sum(texts == ['Alpha policy service steps'] and not query
            for texts, _, query in self.kb.embedder.calls)
        before = self.record()['revision']
        self.remote.rows['recOne']['fields']['Files'][0]['tmp_url'] = 'https://fixture.invalid/rotated'
        self.sync()
        self.assertEqual(self.record()['revision'], before)
        self.assertEqual(len(self.kb.embedder.calls), before_calls)
        self.remote.rows['recOne']['fields']['Opaque'] = 'Changedfieldneedle'
        self.service.record(actor(), 'manuals', 'recOne')
        self.assertFalse(self.kb.evidence_current([original], {'endpoint': 'https://company.example/v1'}, actor=actor()))
        self.drain()
        updated = self.record_hit('Changedfieldneedle')
        self.assertEqual(updated['document_id'], original['document_id'])
        self.assertGreater(updated['version'], original['version'])
        self.assertEqual(sum(texts == ['Alpha policy service steps'] and not query
            for texts, _, query in self.kb.embedder.calls), attachment_embeddings)
        restored = next(item for item in self.kb.search(actor(), 'Alpha policy service steps')['items']
            if item['document_id'] == attachment['id'])
        self.assertIn('vector', restored['matches'])
        with self.kb.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM versions WHERE document_id=?',
                (attachment['id'],)).fetchone()[0], 1)
            self.assertEqual(db.execute('SELECT count(*) FROM versions WHERE document_id=?',
                (original['document_id'],)).fetchone()[0], 2)

    def test_synced_tasks_recover_when_the_knowledge_store_is_empty(self):
        self.kb = KnowledgeBase(self.service.root / 'replacement-knowledge', embedder=FakeEmbedder(),
            memory_index_factory=FakeMemoryIndex, start_worker=False)
        self.addCleanup(self.kb.close)
        self.service.bind_knowledge(self.kb)
        public = self.service.record(actor(), 'manuals', 'recOne', refresh=False)
        self.assertEqual(public['knowledge_status'], 'queued')
        self.assertTrue(all(item['status'] == 'queued' and item['error'] for item in public['knowledge_items']))
        with self.service.connect() as db:
            self.assertEqual({row[0] for row in db.execute('SELECT status FROM ingestion')}, {'synced'})
        self.sync()
        self.assertEqual(self.record()['knowledge_status'], 'ready')
        self.assertTrue(self.record_hit('Alpha'))
        self.assertTrue(self.source_items())

    def test_missing_source_mapping_or_vectors_are_repaired_without_duplicate_versions(self):
        identity = self.source_items()[0]['id']
        for missing in ('mapping', 'vectors', 'chunks'):
            with self.subTest(missing=missing):
                with self.kb.connect() as db:
                    before = db.execute('SELECT count(*) FROM versions WHERE document_id=?', (identity,)).fetchone()[0]
                    if missing == 'mapping':
                        db.execute('DELETE FROM document_sources WHERE document_id=?', (identity,))
                    else:
                        db.execute('DELETE FROM local_vectors WHERE chunk_id IN (SELECT id FROM chunks WHERE document_id=?)', (identity,))
                        if missing == 'chunks':
                            db.execute('DELETE FROM chunks_fts WHERE rowid IN (SELECT id FROM chunks WHERE document_id=?)', (identity,))
                            db.execute('DELETE FROM chunks WHERE document_id=?', (identity,))
                public = self.service.record(actor(), 'manuals', 'recOne', refresh=False)
                self.assertEqual(public['knowledge_status'], 'queued')
                self.sync()
                self.assertEqual(self.record()['knowledge_status'], 'ready')
                self.assertTrue(self.source_items())
                with self.kb.connect() as db:
                    self.assertEqual(db.execute('SELECT count(*) FROM versions WHERE document_id=?', (identity,)).fetchone()[0], before)

    def test_long_fields_keep_the_tail_and_oversized_fields_fail_without_truncation(self):
        from openclaw_service.assistant.lighthouse_knowledge_extract import MAX_CHARS
        fields = self.remote.rows['recOne']['fields']
        fields['Opaque'] = ('ordinary field text ' * 300) + 'Longfieldtailneedle'
        self.sync()
        hit = self.record_hit('Longfieldtailneedle')
        self.assertIn('Longfieldtailneedle', hit['text'])
        fields['Opaque'] = 'x' * MAX_CHARS + 'Oversizedtailneedle'
        self.sync()
        public = self.record()
        item = next(item for item in public['knowledge_items'] if item['kind'] == 'record')
        self.assertEqual(item['status'], 'failed')
        self.assertTrue(item['error'])
        self.assertTrue(public['fields']['fldOpaque'].endswith('Oversizedtailneedle'))
        self.assertGreater(len(public['fields']['fldOpaque']), MAX_CHARS)
        self.assertFalse(any(item['name'] == 'recOne.txt' for item in self.kb.search(actor(), 'Longfieldtailneedle')['items']))

    def test_removed_attachment_and_record_are_isolated_before_new_indexing(self):
        old = self.source_items()[0]
        self.remote.rows['recOne']['fields']['Files'] = []
        self.record()
        with self.assertRaises(AssistantError):
            self.kb.file(actor(), old['id'])
        self.drain()
        body = self.record_hit('Alpha')
        self.assertFalse(self.source_items())
        del self.remote.rows['recOne']
        with self.assertRaises(AssistantError):
            self.record()
        with self.assertRaises(AssistantError):
            self.kb.file(actor(), body['document_id'])
        self.assertFalse(self.kb.search(actor(), 'Alpha policy')['items'])

    def test_scope_move_revokes_body_citation_artifact_and_historical_file(self):
        hit = self.record_hit('Alpha')
        turn = {'sources': [{'kind': 'company_knowledge', 'data': hit}],
            'output_files': [{'id': 'record-artifact'}]}
        self.kb.bind_source_artifacts(turn)
        self.remote.rows['recOne']['fields']['Building'] = 'B'
        with self.assertRaises(AssistantError):
            self.record()
        self.assertFalse(self.kb.cached_turn_current(actor(), turn))
        self.assertFalse(self.kb.artifact_current(actor(), 'record-artifact'))
        self.drain()
        self.assertTrue(self.record_hit('Alpha', actor(('B',))))
        for user in (actor(), actor(('B',))):
            with self.assertRaises(AssistantError):
                self.kb.file(user, hit['document_id'], hit['version'])

    def test_legacy_cache_backfills_on_normal_scan_without_losing_history(self):
        source = self.service._table('manuals')
        raw = self.remote.get('recOne')
        normalized = self.service._normalize(source, raw, self.remote.fields(), json.loads(self.service._cached('manuals', 'recOne')['payload']))
        # Model the pre-upgrade cache: all fields stored, only selected attachments queued.
        legacy = copy.deepcopy(normalized)
        legacy.pop('field_names')
        with self.service.connect() as db:
            db.execute('UPDATE records SET payload=? WHERE table_key=? AND id=?', (json.dumps(legacy), 'manuals', 'recOne'))
            db.execute("DELETE FROM ingestion WHERE source_key='manuals/recOne/record'")
            history = db.execute('SELECT count(*) FROM versions').fetchone()[0]
        before = self.source_items()[0]['id']
        reopened = DocumentLibraryService(self.temp, remote_factory=lambda _: self.remote, clock=lambda: self.now)
        reopened.bind_knowledge(self.kb)
        self.service = reopened
        with patch.object(self.kb, 'upsert_source', wraps=self.kb.upsert_source) as upsert:
            self.now += 301
            self.drain()
        self.assertIn('manuals/recOne/record', [call.args[0] for call in upsert.call_args_list])
        self.assertTrue(self.record_hit('Alpha'))
        with self.service.connect() as db:
            self.assertEqual(db.execute("SELECT status FROM ingestion WHERE source_key='manuals/recOne/record'").fetchone()[0], 'synced')
            self.assertGreaterEqual(db.execute('SELECT count(*) FROM versions').fetchone()[0], history)
        self.assertEqual(self.source_items()[0]['id'], before)

    def test_schema_rename_refreshes_record_text(self):
        fields = copy.deepcopy(self.remote.fields())
        fields[2]['field_name'] = 'Renamedmetricneedle'
        value = self.remote.rows['recOne']['fields'].pop('Number')
        self.remote.rows['recOne']['fields']['Renamedmetricneedle'] = value
        with patch.object(self.remote, 'fields', return_value=fields):
            self.sync()
        self.assertTrue(self.record_hit('Renamedmetricneedle'))

    def test_sensitive_field_body_remains_blocked_and_reports_state(self):
        self.remote.rows['recOne']['fields']['Opaque'] = 'password: fixture-secret-for-test'
        self.sync()
        item = next(item for item in self.record()['knowledge_items'] if item['kind'] == 'record')
        self.assertEqual(item['status'], 'blocked')
        self.assertTrue(item['error'])
        self.assertFalse(any(item['name'] == 'recOne.txt' for item in self.kb.search(actor(), 'Alpha')['items']))


if __name__ == '__main__':
    unittest.main()
