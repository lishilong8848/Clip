"""Local maintenance and cache recovery; no live Feishu writes or model calls."""
import copy
import json
from unittest.mock import patch

from bin.test_document_library import LibraryFixture, actor, configuration
from bin.test_document_library_sync import SyncFixture
from lan_bitable_template_portal.document_library import DocumentLibraryService
from openclaw_service.assistant.lighthouse_ai import AssistantError


class MaintenanceTests(LibraryFixture):
    def request(self, **values):
        return {'operation_id': '41257c9b-8112-449d-bd7a-219ae32d8e01',
            'expected_revision': self.service._table('manuals')['revision'], **values}

    def test_selected_reindex_recomputes_all_parts_and_duplicate_is_not_requeued(self):
        request = self.request(record_ids=['recOne'])
        before_downloads = self.remote.downloads
        with patch.object(self.kb.embedder, 'embed', wraps=self.kb.embedder.embed) as embed:
            first = self.service.reindex(actor(), 'manuals', request)
            self.assertEqual((first['records'], first['items']), (1, 2))
            self.assertFalse(self.service.cached_record(actor(), 'manuals', 'recOne')['knowledge_summary']['vectorized'])
            self.drain()
            calls = embed.call_count
            self.assertGreater(calls, 0)
            self.assertEqual(self.service.reindex(actor(), 'manuals', request), first)
            self.drain()
            self.assertEqual(embed.call_count, calls)
        self.assertEqual(self.remote.downloads, before_downloads)
        result = self.service.cached_record(actor(), 'manuals', 'recOne')
        self.assertTrue(result['knowledge_summary']['vectorized'])
        self.assertEqual(result['knowledge_status'], 'ready')

    def test_all_respects_building_query_and_never_writes_remote(self):
        result = self.service.reindex(actor(), 'manuals', self.request(all=True, q='policy'))
        self.assertEqual(result['records'], 1)
        with self.service.connect(readonly=True) as db:
            ids = {r[0] for r in db.execute("SELECT record_id FROM ingestion WHERE reindex_request<>''")}
        self.assertEqual(ids, {'recOne'})
        self.drain()
        self.assertEqual((self.remote.creates, self.remote.updates, self.remote.uploads), (0, 0, 0))

    def test_field_filters_match_listing_and_cross_page_selection_offline(self):
        filters = [{'field': 'fldNumber', 'op': 'gte', 'value': 5},
            {'field': 'fldTitle', 'op': 'contains', 'value': 'policy'}]
        with patch.object(self.remote, 'records', side_effect=AssertionError('Use local cache')), \
                patch.object(self.remote, 'fields', side_effect=AssertionError('Use cached fields')):
            listed = self.service.list_records(self.admin, 'manuals', {'filters': json.dumps(filters), 'page_size': 1})
            self.assertEqual((listed['total'], listed['has_more']), (1, False))
            self.assertEqual(listed['items'][0]['record_id'], 'recTwo')
            result = self.service.reindex(self.admin, 'manuals', self.request(all=True, filters=filters))
        self.assertEqual(result['records'], listed['total'])
        with self.service.connect(readonly=True) as db:
            ids = {row[0] for row in db.execute("SELECT record_id FROM ingestion WHERE reindex_request<>''")}
        self.assertEqual(ids, {'recTwo'})
        self.assertEqual(self.service.list_records(actor(), 'manuals', {'filters': json.dumps(filters)})['total'], 0)

    def test_filters_invalid_fields_or_values_cannot_broaden_selection(self):
        invalid = [False, {}, '[', [{'field': 'missing', 'op': 'empty'}],
            [{'field': 'fldNumber', 'op': 'gte', 'value': 'NaN'}],
            [{'field': 'fldTitle', 'op': 'lte', 'value': 'x'}],
            [{'field': ['fldTitle'], 'op': 'eq', 'value': 'x'}]]
        for filters in invalid:
            with self.subTest(filters=filters):
                with self.assertRaises(AssistantError):
                    self.service.list_records(self.admin, 'manuals', {'filters': filters})
                with self.assertRaises(AssistantError):
                    self.service.reindex(self.admin, 'manuals', self.request(all=True, filters=filters))
        with self.service.connect(readonly=True) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM maintenance').fetchone()[0], 0)

    def test_native_field_filter_dates_names_false_zero_and_attachments(self):
        from lan_bitable_template_portal.document_library_filters import matches_filters, parse_filters
        fields = {fid: {'type': kind} for fid, kind in [('date', 5), ('people', 11), ('valid', 7), ('count', 2), ('files', 17)]}
        values = {'date': 1792425600000, 'people': [{'id': 'ou_fixture', 'name': '测试人员'}],
            'valid': False, 'count': 0, 'files': [{'file_token': 'not-a-label', 'name': '运维规程.pdf'}]}
        filters = [{'field': 'date', 'op': 'eq', 'value': '2026-10-20'},
            {'field': 'people', 'op': 'eq', 'value': '测试人员'}, {'field': 'valid', 'op': 'eq', 'value': False},
            {'field': 'count', 'op': 'eq', 'value': 0}, {'field': 'files', 'op': 'contains', 'value': '规程'}]
        self.assertTrue(matches_filters(values, parse_filters(filters, fields)))
        self.assertFalse(matches_filters(values, parse_filters([{'field': 'valid', 'op': 'empty'}], fields)))
        self.assertFalse(matches_filters(values, parse_filters([{'field': 'date', 'op': 'lte', 'value': '2026-10-19'}], fields)))
        self.assertTrue(matches_filters({}, parse_filters([{'field': 'files', 'op': 'empty'}], fields)))
        for date in ('2026-02-31', 'yesterday'):
            with self.assertRaises(AssistantError):
                parse_filters([{'field': 'date', 'op': 'eq', 'value': date}], fields)

    def test_inflight_ingest_cannot_consume_newer_reindex_request(self):
        self.service.reindex(actor(), 'manuals', self.request(record_ids=['recOne']))
        self.service.prepare_pending(limit=8)
        task = self.service._claim('ingestion', stage='index')
        second = self.request(record_ids=['recOne'], operation_id='41257c9b-8112-449d-bd7a-219ae32d8e06')
        self.service.reindex(actor(), 'manuals', second)
        with self.service.connect(readonly=True) as db:
            lease = db.execute('SELECT lease FROM ingestion WHERE source_key=?', (task['source_key'],)).fetchone()[0]
        self.assertEqual(lease, task['lease'])
        self.service._ingest(task)
        with self.service.connect(readonly=True) as db:
            state = db.execute('SELECT status,reindex_request FROM ingestion WHERE source_key=?', (task['source_key'],)).fetchone()
        self.assertEqual(tuple(state), ('queued', second['operation_id']))
        self.drain()
        self.assertTrue(self.service.cached_record(actor(), 'manuals', 'recOne')['knowledge_summary']['vectorized'])

    def test_cross_scope_selection_rejected_atomically_and_readonly_denied(self):
        for who, request in ((actor(), self.request(record_ids=['recOne', 'recTwo'])),
                ({**actor(), 'read_only': True}, self.request(record_ids=['recOne'])),
                (actor(), self.request(record_ids=['recOne'], expected_revision='stale'))):
            with self.assertRaises(AssistantError):
                self.service.reindex(who, 'manuals', request)
        with self.service.connect(readonly=True) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM maintenance').fetchone()[0], 0)

    def test_missing_task_and_unsupported_attachment_never_report_success(self):
        with self.service.connect() as db:
            db.execute("DELETE FROM ingestion WHERE table_key='manuals' AND record_id='recOne' AND source_key LIKE '%/record'")
        result = self.service.cached_record(actor(), 'manuals', 'recOne')
        self.assertFalse(result['knowledge_summary']['vectorized'])
        self.assertEqual(result['knowledge_summary']['total'], 2)
        self.assertEqual(result['knowledge_status'], 'queued')
        self.remote.rows['recOne']['fields']['Files'][0]['name'] = 'unsupported.bin'
        self.service.record(actor(), 'manuals', 'recOne')
        self.drain()
        result = self.service.cached_record(actor(), 'manuals', 'recOne')
        self.assertFalse(result['knowledge_summary']['vectorized'])
        self.assertIn('unsupported', [i['status'] for i in result['knowledge_items']])

    def test_remove_retries_after_restart_and_preserves_other_source_and_remote(self):
        self.service.save_table(self.admin, configuration(id='manuals-other', table_id='tblOther'))
        self.drain()
        remote_before = copy.deepcopy(self.remote.rows)
        request = self.request()
        with patch.object(self.kb, 'remove_sources', side_effect=OSError('fixture interruption')):
            result = self.service.remove_table(self.admin, 'manuals', request)
            self.assertEqual(result['cleanup_status'], 'pending')
            self.assertTrue(self.service.prepare_pending(limit=1))
        self.assertEqual([r['id'] for r in self.service.tables(self.admin)['items']], ['manuals-other'])
        self.assertTrue(self.service.tables(self.admin)['cleanup_pending'])
        with self.assertRaises(AssistantError):
            self.service.save_table(self.admin, configuration())
        self.service = DocumentLibraryService(self.temp, remote_factory=lambda _: self.remote,
            knowledge=self.kb, clock=lambda: self.now)
        self.now += 6
        self.drain()
        self.assertEqual(self.service.remove_table(self.admin, 'manuals', request)['cleanup_status'], 'completed')
        with self.kb.connect() as db:
            keys = [r[0] for r in db.execute('SELECT source_key FROM document_sources')]
        self.assertTrue(keys)
        self.assertTrue(all(k.startswith('manuals-other/') for k in keys))
        with self.service.connect(readonly=True) as db:
            for table in ('records', 'versions', 'ingestion', 'sync_jobs', 'schema_cache'):
                self.assertEqual(db.execute('SELECT count(*) FROM ' + table + ' WHERE table_key=?', ('manuals',)).fetchone()[0], 0)
        self.assertEqual(self.remote.rows, remote_before)
        self.assertEqual((self.remote.creates, self.remote.updates, self.remote.uploads), (0, 0, 0))
        self.service.save_table(self.admin, configuration())
        self.drain()
        self.assertTrue(self.service.cached_record(actor(), 'manuals', 'recOne')['knowledge_summary']['vectorized'])

    def test_removal_permission_pending_write_and_operation_fingerprint(self):
        request = self.request()
        with self.assertRaises(AssistantError):
            self.service.remove_table(actor(), 'manuals', request)
        self.remote.fail_write_before_commit = True
        with self.assertRaises(AssistantError):
            self.service.save_record(actor(), 'manuals', self.mutation({'fldNumber': 55}), identity='recOne')
        with self.assertRaises(AssistantError):
            self.service.remove_table(self.admin, 'manuals', request)
        self.assertEqual(len(self.service.tables(self.admin)['items']), 1)

    def test_ready_keyword_index_without_vectors_is_not_vectorized(self):
        with self.kb.connect() as db:
            config = json.loads(db.execute("SELECT payload FROM config WHERE key='embedding'").fetchone()[0])
            config['mode'] = 'keyword'
            db.execute("UPDATE config SET payload=? WHERE key='embedding'", (json.dumps(config),))
            db.execute('DELETE FROM local_vectors')
        result = self.service.cached_record(actor(), 'manuals', 'recOne')
        self.assertEqual(result['knowledge_status'], 'text_only')
        self.assertFalse(result['knowledge_summary']['vectorized'])


class CacheTests(SyncFixture):
    def test_all_remote_pages_are_cached_and_paging_survives_restart_offline(self):
        rows = []
        for number in range(75):
            row = copy.deepcopy(self.remote.rows['recOne'])
            row['record_id'] = f'rec{number:03d}'
            row['fields'].update(Title=f'Policy {number}', Number=number, Files=[])
            rows.append(row)
        self.save()
        def page(cursor='', page_size=100):
            offset = int(cursor or 0)
            return {'items': rows[offset:offset + 25], 'has_more': offset + 25 < len(rows),
                'page_token': str(offset + 25) if offset + 25 < len(rows) else ''}
        with patch.object(self.remote, 'records', side_effect=page) as read:
            for _ in range(3):
                self.assertTrue(self.service.process_sync('manuals'))
            self.assertEqual(read.call_count, 3)
        self.service = self.reopen()
        with patch.object(self.remote, 'records', side_effect=AssertionError('Offline cache must serve every page')), \
                patch.object(self.remote, 'fields', side_effect=AssertionError('Use cached schema')):
            first = self.service.list_records(actor(), 'manuals', {'page_size': 30})
            last = self.service.list_records(actor(), 'manuals', {'page_size': 30, 'cursor': '60'})
            self.assertEqual((first['total'], first['cache']['complete']), (75, True))
            self.assertEqual((len(last['items']), last['has_more']), (15, False))
            self.assertEqual(last['items'][-1]['record_id'], 'rec074')

    def test_restart_opens_schema_records_details_offline(self):
        self.save()
        self.service.process_sync('manuals')
        self.service = self.reopen()
        with patch.object(self.remote, 'fields', side_effect=AssertionError('cached schema should not fetch')), \
                patch.object(self.remote, 'get', side_effect=AssertionError('cached record should not fetch')), \
                patch.object(self.remote, 'records', side_effect=AssertionError('cached listing should not fetch')):
            self.assertTrue(self.service.schema(actor(), 'manuals')['fields'])
            self.assertEqual(self.service.list_records(actor(), 'manuals', {})['total'], 1)
            self.assertEqual(self.service.cached_record(actor(), 'manuals', 'recOne')['record_id'], 'recOne')
        with patch.object(self.remote, 'get', wraps=self.remote.get) as get:
            self.service.cached_record(actor(), 'manuals', 'recOne', refresh=True)
            get.assert_called_once_with('recOne')

    def test_partial_cache_visible_but_select_all_requires_completion(self):
        self.save()
        with patch.object(self.remote, 'records', return_value={'items': [self.remote.rows['recOne']], 'has_more': True, 'page_token': 'next'}):
            self.service.process_sync('manuals')
        result = self.service.list_records(actor(), 'manuals', {})
        self.assertFalse(result['cache']['complete'])
        self.assertEqual(result['total'], 1)
        payload = {'operation_id': '41257c9b-8112-449d-bd7a-219ae32d8e02', 'expected_revision': self.service._table('manuals')['revision']}
        with self.assertRaises(AssistantError):
            self.service.reindex(actor(), 'manuals', {**payload, 'all': True})
        self.assertEqual(self.service.reindex(actor(), 'manuals', {**payload, 'record_ids': ['recOne']})['records'], 1)

    def test_due_refresh_deduplicates_and_stale_cache_obeys_current_permissions(self):
        self.save()
        self.service.process_sync('manuals')
        self.assertIsNone(self.service.refresh_cache_if_due(actor(), 'manuals'))
        self.now += 301
        result = self.service.refresh_cache_if_due(actor(), 'manuals')
        self.assertEqual(result['status'], 'queued')
        self.assertIsNone(self.service.refresh_cache_if_due(actor(), 'manuals'))
        with self.assertRaises(AssistantError):
            self.service.cached_record(actor(('B',)), 'manuals', 'recOne')
        self.assertEqual(self.service.list_records(actor(('B',)), 'manuals', {})['items'][0]['record_id'], 'recTwo')


class MaintenanceRoutesTests(SyncFixture):
    def setUp(self):
        super().setUp()
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from lan_bitable_template_portal.document_library_routes import install_document_library_routes
        self.save()
        self.service.process_sync('manuals')
        self.user = self.admin
        async def authorize(request):
            return copy.deepcopy(self.user)
        app = FastAPI()
        install_document_library_routes(app, authorize, lambda: self.service)
        self.client = self.enterContext(TestClient(app))
        self.url = '/api/document-library/tables/manuals'
        self.request = {'operation_id': '41257c9b-8112-449d-bd7a-219ae32d8e03',
            'expected_revision': self.service._table('manuals')['revision']}

    def test_cached_detail_and_schema_do_not_wait_for_network(self):
        with patch.object(self.remote, 'fields', side_effect=OSError('offline')), patch.object(self.remote, 'get', side_effect=OSError('offline')):
            self.assertEqual(self.client.get(self.url + '/schema').status_code, 200)
            self.assertEqual(self.client.get(self.url + '/records/recOne').status_code, 200)
            self.assertEqual(self.client.get(self.url + '/records/recOne?refresh=1').status_code, 503)
            self.assertEqual(self.client.get(self.url + '/schema?refresh=1').status_code, 503)
        self.assertEqual(self.client.get(self.url + '/records').json()['data']['total'], 2)

    def test_reindex_retry_and_removal_routes(self):
        request = {**self.request, 'all': True}
        first = self.client.post(self.url + '/reindex', json=request)
        self.assertEqual(first.status_code, 200, first.text)
        self.assertEqual(first.json()['data']['records'], 2)
        self.assertEqual(self.client.post(self.url + '/reindex', json=request).json(), first.json())
        remove = {**self.request, 'operation_id': '41257c9b-8112-449d-bd7a-219ae32d8e04'}
        self.user = actor()
        self.assertEqual(self.client.request('DELETE', self.url, json=remove).status_code, 403)
        self.user = self.admin
        result = self.client.request('DELETE', self.url, json=remove)
        self.assertEqual(result.status_code, 200, result.text)
        self.assertTrue(result.json()['data']['removed'])
        self.assertEqual(self.client.request('DELETE', self.url, json=remove).status_code, 200)
        self.assertEqual(self.client.get(self.url + '/records').status_code, 404)
        self.assertEqual(len(self.remote.rows), 2)
