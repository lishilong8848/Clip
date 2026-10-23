"""Isolated recovery, cache maintenance and SQL paging; no live provider writes."""
import copy
import hashlib
import json
import os
from pathlib import Path
from unittest.mock import patch

from bin import test_document_library as fixture
from lan_bitable_template_portal.document_library import DocumentLibraryService
from openclaw_service.assistant.lighthouse_ai import AssistantError


class LibraryRecoveryTests(fixture.LibraryFixture):
    def test_uncertain_update_remains_findable_after_restart_and_never_resends(self):
        self.remote.fail_write_before_commit = True
        payload = self.mutation({'fldNumber': 18})
        with self.assertRaises(AssistantError):
            self.service.save_record(fixture.actor(), 'manuals', payload, identity='recOne')
        reopened = DocumentLibraryService(self.temp, remote_factory=lambda _: self.remote, clock=lambda: self.now)
        pending = reopened.pending_operations(fixture.actor(), 'manuals')['items']
        self.assertEqual([item['operation_id'] for item in pending], [payload['operation_id']])
        result = reopened.verify_operation(fixture.actor(), 'manuals', payload['operation_id'], {})
        self.assertEqual(result['status'], 'uncertain')
        self.assertEqual(self.remote.updates, 1)
        self.remote.rows['recOne']['fields']['Number'] = 18
        result = reopened.verify_operation(fixture.actor(), 'manuals', payload['operation_id'], {})
        self.assertEqual(result['status'], 'completed')
        self.assertEqual(reopened.pending_operations(fixture.actor(), 'manuals')['items'], [])
        self.assertEqual(self.remote.updates, 1)

    def test_create_response_loss_requires_verified_record_not_second_create(self):
        original = self.remote.create
        def uncertain(*args):
            original(*args)
            raise OSError('synthetic response loss')
        payload = {'fields': {'fldScope': 'A', 'fldTitle': 'new document'}, 'reason': 'fixture', 'operation_id': 'create-recovery-123'}
        with patch.object(self.remote, 'create', side_effect=uncertain), self.assertRaises(AssistantError):
            self.service.save_record(fixture.actor(), 'manuals', payload)
        self.assertEqual(self.service.verify_operation(fixture.actor(), 'manuals', payload['operation_id'], {})['status'], 'uncertain')
        with self.assertRaises(AssistantError):
            self.service.verify_operation(fixture.actor(), 'manuals', payload['operation_id'], {'record_id': 'recTwo'})
        result = self.service.verify_operation(fixture.actor(), 'manuals', payload['operation_id'], {'record_id': 'recCreated1'})
        self.assertEqual(result['status'], 'completed')
        self.assertEqual(self.remote.creates, 1)

    def test_operation_owner_scope_and_readonly_checks(self):
        self.remote.fail_write_before_commit = True
        payload = self.mutation({'fldNumber': 18})
        with self.assertRaises(AssistantError):
            self.service.save_record(fixture.actor(), 'manuals', payload, identity='recOne')
        for actor in ({**fixture.actor(), 'id': 'someone-else'}, fixture.actor(('B',))):
            self.assertEqual(self.service.pending_operations(actor, 'manuals')['items'], [])
            with self.assertRaises(AssistantError):
                self.service.verify_operation(actor, 'manuals', payload['operation_id'], {})
        with self.assertRaises(AssistantError):
            self.service.verify_operation({**fixture.actor(), 'read_only': True}, 'manuals', payload['operation_id'], {})
        self.assertEqual(self.remote.updates, 1)

    def test_concurrent_create_verification_cannot_rebind_a_different_record(self):
        payload = {'fields': {'fldScope': 'A', 'fldTitle': 'new document'}, 'reason': 'fixture', 'operation_id': 'create-cas-123'}
        original = self.remote.create
        def uncertain(*args):
            original(*args)
            raise OSError('synthetic response loss')
        with patch.object(self.remote, 'create', side_effect=uncertain), self.assertRaises(AssistantError):
            self.service.save_record(fixture.actor(), 'manuals', payload)
        verify = self.service._verify_fields
        def concurrent(*args):
            with self.service.connect() as db:
                db.execute('UPDATE operations SET record_id=? WHERE id=?', ('recOtherVerified', payload['operation_id']))
            return verify(*args)
        with patch.object(self.service, '_verify_fields', side_effect=concurrent), self.assertRaises(AssistantError) as caught:
            self.service.verify_operation(fixture.actor(), 'manuals', payload['operation_id'], {'record_id': 'recCreated1'})
        self.assertEqual(caught.exception.status, 409)
        with self.service.connect(readonly=True) as db:
            self.assertEqual(db.execute('SELECT record_id FROM operations WHERE id=?', (payload['operation_id'],)).fetchone()[0], 'recOtherVerified')
        self.assertEqual(self.remote.creates, 1)

    def test_sql_paging_has_one_index_read_and_preserves_filters_and_scope(self):
        from lan_bitable_template_portal import document_library_index
        for n in range(70):
            self.remote.rows['recPage' + str(n).zfill(3)] = {'record_id': 'recPage' + str(n).zfill(3),
                'fields': {'Title': 'Large-' + str(n), 'Building': 'A', 'Number': n}}
        self.service.request_sync(self.admin, 'manuals')
        self.service.process_sync('manuals')
        with patch.object(document_library_index, 'read_index_states', wraps=document_library_index.read_index_states) as indexed:
            value = self.service.list_records(fixture.actor(), 'manuals', {'page_size': 10, 'cursor': 20, 'q': 'large'})
        self.assertEqual((value['total'], len(value['items']), value['cursor']), (70, 10, '30'))
        indexed.assert_called_once()
        value = self.service.list_records(self.admin, 'manuals', {'filters': [{'field': 'fldNumber', 'op': 'gte', 'value': 60}]})
        self.assertEqual(value['total'], 10)
        self.assertEqual(self.service.list_records(fixture.actor(('B',)), 'manuals', {'q': 'large'})['total'], 0)

    def test_reindex_only_failed_or_missing_skips_ready_files(self):
        revision = self.service._table('manuals')['revision']
        source = 'manuals/recOne/'
        with self.service.connect() as db:
            key = db.execute('SELECT source_key FROM ingestion WHERE source_key LIKE ? ORDER BY source_key LIMIT 1', (source + '%',)).fetchone()[0]
            db.execute("UPDATE ingestion SET status='blocked',error_code='too_large' WHERE source_key=?", (key,))
        result = self.service.reindex(fixture.actor(), 'manuals', {'operation_id': '41257c9b-8112-449d-bd7a-219ae32d8e01',
            'expected_revision': revision, 'record_ids': ['recOne'], 'mode': 'failed'})
        self.assertEqual((result['records'], result['items']), (1, 1))
        self.drain()
        result = self.service.reindex(fixture.actor(), 'manuals', {'operation_id': '41257c9b-8112-449d-bd7a-219ae32d8e02',
            'expected_revision': revision, 'record_ids': ['recOne'], 'mode': 'missing'})
        self.assertEqual(result['items'], 0)

    def test_permanent_file_error_is_blocked_but_network_error_retries(self):
        with self.service.connect() as db:
            db.execute("UPDATE ingestion SET status='queued',stage='prepare',next_attempt=0,lease_until=0 WHERE record_id='recOne'")
        task = self.service._claim('ingestion', stage='prepare')
        self.service._retry('ingestion', task, AssistantError('file too large', 413, category='too_large'))
        with self.service.connect(readonly=True) as db:
            self.assertEqual(db.execute('SELECT status FROM ingestion WHERE source_key=?', (task['source_key'],)).fetchone()[0], 'blocked')
        task = self.service._claim('ingestion', stage='prepare')
        self.service._retry('ingestion', task, AssistantError('network', 503, category='transport'))
        with self.service.connect(readonly=True) as db:
            self.assertEqual(db.execute('SELECT status FROM ingestion WHERE source_key=?', (task['source_key'],)).fetchone()[0], 'retrying')

    def test_paged_source_yields_to_another_queued_table(self):
        self.service.save_table(self.admin, fixture.configuration(id='other', table_id='tblOther'))
        self.service.request_sync(self.admin, 'manuals')
        first = self.service._claim('sync_jobs', table_key='manuals')
        with patch.object(self.remote, 'records', return_value={'items': [], 'has_more': True, 'page_token': 'page-two'}):
            self.service._scan_page(first)
        second = self.service._claim('sync_jobs')
        self.assertEqual(second['table_key'], 'other')
        resumed = self.service._claim('sync_jobs')
        self.assertEqual((resumed['table_key'], resumed['cursor']), ('manuals', 'page-two'))

    def route_client(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from lan_bitable_template_portal.document_library_routes import install_document_library_routes
        self.current_actor = self.admin
        async def authorize(request):
            return self.current_actor
        app = FastAPI()
        install_document_library_routes(app, authorize, lambda: self.service)
        return self.enterContext(TestClient(app))

    def test_recovery_routes_preserve_ownership_and_do_not_repeat_write(self):
        client = self.route_client()
        self.remote.fail_write_before_commit = True
        payload = self.mutation({'fldNumber': 18})
        with self.assertRaises(AssistantError):
            self.service.save_record(fixture.actor(), 'manuals', payload, identity='recOne')
        prefix = '/api/document-library/tables/manuals/operations'
        self.current_actor = fixture.actor()
        pending = client.get(prefix).json()['data']['items']
        self.assertEqual(pending[0]['operation_id'], payload['operation_id'])
        response = client.post(prefix + '/' + payload['operation_id'] + '/verify', json={})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()['data']['status'], 'uncertain')
        self.current_actor = {**fixture.actor(), 'id': 'other-person'}
        self.assertEqual(client.get(prefix).json()['data']['items'], [])
        self.assertEqual(client.post(prefix + '/' + payload['operation_id'] + '/verify', json={}).status_code, 404)
        self.assertEqual(client.get('/api/document-library/storage').status_code, 403)
        self.current_actor = self.admin
        self.assertEqual(client.get('/api/document-library/storage').status_code, 200)
        self.assertEqual(client.post('/api/document-library/storage', json={}).status_code, 400)
        self.assertEqual(self.remote.updates, 1)

    def test_inline_preview_whitelist_and_current_access_checks(self):
        client = self.route_client()
        for name, inline in [('photo.png', True), ('report.pdf', True), ('active.svg', False), ('active.html', False)]:
            with self.subTest(name=name):
                self.remote.rows['recOne']['fields']['Files'][0]['name'] = name
                record = self.service.record(self.admin, 'manuals', 'recOne')
                url = record['attachments'][0]['download_url'] + '?preview=1'
                response = client.get(url)
                self.assertEqual(response.status_code, 200, response.text)
                self.assertTrue(response.headers['content-disposition'].startswith('inline;' if inline else 'attachment;'))
                self.assertEqual(response.headers['x-content-type-options'], 'nosniff')
                self.assertIn('sandbox', response.headers['content-security-policy'])
                self.current_actor = fixture.actor(('B',))
                self.assertEqual(client.get(url).status_code, 404)
                self.current_actor = self.admin

    def test_storage_cleanup_preserves_current_historical_and_recent_files(self):
        directory = self.service.root / 'files'
        protected = list(directory.iterdir())
        orphan = directory / hashlib.sha256(b'orphan-fixture').hexdigest()
        recent = directory / hashlib.sha256(b'recent-fixture').hexdigest()
        orphan.write_bytes(b'orphan'); recent.write_bytes(b'recent')
        for path in [*protected, orphan]:
            os.utime(path, (1, 1))
        self.now = 100000
        status = self.service.storage(self.admin)
        self.assertEqual(status['orphan_files'], 1)
        with self.assertRaises(AssistantError):
            self.service.storage(fixture.actor())
        result = self.service.storage(self.admin, {'expected_revision': status['revision']})
        self.assertEqual(result['removed'], 1)
        self.assertFalse(orphan.exists())
        self.assertTrue(recent.exists())
        self.assertTrue(all(path.exists() for path in protected))
        self.assertEqual((self.remote.creates, self.remote.updates, self.remote.uploads), (0, 0, 0))

    def test_new_cache_reference_invalidates_cleanup_confirmation(self):
        token = 'orphan-becomes-referenced'
        orphan = self.service.root / 'files' / hashlib.sha256(token.encode()).hexdigest()
        orphan.write_bytes(b'keep'); os.utime(orphan, (1, 1)); self.now = 100000
        status = self.service.storage(self.admin)
        with self.service.connect() as db:
            row = db.execute("SELECT payload FROM versions WHERE table_key='manuals' LIMIT 1").fetchone()
            data = json.loads(row[0]); data['protect_test'] = {'file_token': token}
            db.execute("UPDATE versions SET payload=? WHERE table_key='manuals'", (json.dumps(data),))
        with self.assertRaises(AssistantError):
            self.service.storage(self.admin, {'expected_revision': status['revision']})
        self.assertTrue(orphan.exists())

    def test_source_summary_only_counts_authorized_records(self):
        summary = self.service.tables(fixture.actor())['items'][0]['summary']
        self.assertEqual(summary['records'], 1)
        self.assertEqual(self.service.tables(self.admin)['items'][0]['summary']['records'], 2)


if __name__ == '__main__':
    import unittest
    unittest.main()
