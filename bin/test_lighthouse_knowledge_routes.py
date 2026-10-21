import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import httpx
from fastapi import FastAPI

from openclaw_service.assistant.lighthouse_knowledge import KnowledgeBase
from openclaw_service.assistant.lighthouse_knowledge_routes import install_knowledge_routes
from test_lighthouse_knowledge import FakeEmbedder, FakeMemoryIndex, actor, _protect, _text


class KnowledgeRoutes(unittest.IsolatedAsyncioTestCase):
    """Web/Feishu integration tests backed by the REAL KnowledgeBase service.

    The service runs in the canonical ``local_faiss`` mode (DIMENSIONS=512) with the
    deterministic 512-dim FakeEmbedder and a real FAISS index, so search / delete /
    restore / cross-user mutation and version restrictions are exercised through the
    actual service layer, not through scripted route fakes.
    """

    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.kb = KnowledgeBase(
            self.tmp.name,
            embedder=FakeEmbedder(),  # 512-dim, canonical local_faiss
            protect=_protect,
            start_worker=False,
            memory_index_factory=FakeMemoryIndex,
        )
        app = FastAPI()

        async def authorize(request):
            return {**actor(request.headers.get('user', 'owner'), admin=request.headers.get('admin') == '1',
                            guest=request.headers.get('guest') == '1', role=None),
                    'channel': request.headers.get('channel', '')}

        install_knowledge_routes(app, authorize, lambda: self.kb)
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://fixture')

    async def asyncTearDown(self):
        await self.client.aclose()
        self.kb.close()
        self.tmp.cleanup()

    async def test_manual_management_excludes_sources_but_both_channels_search_and_download(self):
        base = '/api/assistant/knowledge'
        self.kb.source_current = lambda source: True
        self.kb.source_acl = lambda user, source: user['id'] == 'owner'
        managed = self.kb.upsert_source('fixture/record/body', {'scopes': ['A']},
            'managed.txt', b'Unifiedneedle source content')
        manual = self.kb.upload(actor('owner'), 'manual.txt', b'Unifiedneedle manual content')
        while self.kb.process_one():
            pass
        for params in ({}, {'source': 'all', 'select_all': '1'}, {'q': 'managed'}, {'deleted': '1'}):
            response = await self.client.get(base, params=params, headers={'admin': '1'})
            self.assertEqual(response.status_code, 200, response.text)
            data = response.json()['data']
            self.assertNotIn(managed['id'], [item['id'] for item in data['items']])
            self.assertEqual(data['total'], 0 if params.get('q') or params.get('deleted') else 1)
            if 'selection' in data:
                self.assertEqual(data['selection']['count'], 1)
        for channel in ('web', 'feishu'):
            found = (await self.client.get(base + '/search', params={'q': 'Unifiedneedle'},
                headers={'channel': channel})).json()['data']['items']
            self.assertEqual({item['document_id'] for item in found}, {managed['id'], manual['id']})
            other = (await self.client.get(base + '/search', params={'q': 'Unifiedneedle'},
                headers={'channel': channel, 'user': 'other'})).json()['data']['items']
            self.assertEqual({item['document_id'] for item in other}, {manual['id']})
        path = base + '/documents/' + managed['id']
        self.assertEqual((await self.client.get(path)).status_code, 200)
        self.assertEqual((await self.client.get(path + '/file')).content, b'Unifiedneedle source content')
        self.assertEqual((await self.client.get(path + '/file', headers={'user': 'other'})).status_code, 404)
        self.assertEqual(self.kb.settings(actor('owner'))['vectors'], 1)
        self.kb.upsert_source('fixture/record/pending', {'scopes': ['A']}, 'queued.txt', b'Awaiting index')
        self.assertEqual(self.kb.settings(actor('owner'))['queued'], 0)

    async def test_manual_folder_counts_selection_and_ancestors_exclude_managed_members(self):
        base = '/api/assistant/knowledge'
        self.kb.source_current = self.kb.source_acl = lambda *args: True
        managed = self.kb.upsert_source('fixture/record/body', {}, 'managed.txt', b'Source fixture')
        manual = self.kb.upload(actor('owner'), 'manual.txt', b'Manual fixture')
        parent = self.kb.create_group(actor('owner'), 'Parent')
        child = self.kb.create_group(actor('owner'), 'Child', parent_id=parent['id'])
        hidden = self.kb.create_group(actor('owner'), 'Managed only')
        with self.kb.connect() as db:
            db.execute('INSERT INTO doc_group_members VALUES(?,?)', (managed['id'], parent['id']))
        self.kb.set_documents_group(actor('owner'), child['id'], [manual['id']])
        groups = (await self.client.get(base + '/groups')).json()['data']['groups']
        by_id = {item['id']: item for item in groups}
        self.assertEqual(by_id[parent['id']]['count'], 0)
        self.assertEqual(by_id[child['id']]['count'], 1)
        self.assertEqual(by_id[child['id']]['parent_id'], parent['id'])
        with self.kb.connect() as db:
            db.execute('UPDATE doc_group_members SET group_id=? WHERE document_id=?', (hidden['id'], managed['id']))
        groups = (await self.client.get(base + '/groups')).json()['data']['groups']
        self.assertNotIn(hidden['id'], [item['id'] for item in groups])
        selection = (await self.client.get(base, params={'select_all': '1'}, headers={'admin': '1'})).json()['data']['selection']
        moved = await self.client.put(base + '/groups/' + parent['id'] + '/documents',
            headers={'admin': '1'}, json={'selection': selection})
        self.assertEqual(moved.status_code, 200, moved.text)
        self.assertEqual(moved.json()['data']['updated'], 1)
        renamed = await self.client.patch(base + '/groups/' + parent['id'], json={'name': 'Renamed'})
        self.assertEqual(renamed.json()['data']['count'], 1)

    async def test_web_upload_route_seeds_document(self):
        base = '/api/assistant/knowledge'
        response = await self.client.post(base + '/files', files={
            'files': ('公司手册.txt', '公司报销需要真实凭证。'.encode('utf-8'))})
        self.assertEqual(response.status_code, 200, response.text)
        document = response.json()['data']['items'][0]
        self.assertEqual(document['name'], '公司手册.txt')
        # the uploaded file is really stored through the service (not a scripted fake):
        # the service returns it in list and document reads
        listing = (await self.client.get(base, headers={'admin': '1'})).json()['data']
        self.assertEqual(listing['items'][0]['id'], document['id'])
        doc = (await self.client.get(base + '/documents/' + document['id'],
                                     headers={'admin': '1'})).json()['data']['document']
        self.assertEqual(doc['id'], document['id'])

    async def test_filtered_all_selection_moves_only_editable_documents(self):
        base = '/api/assistant/knowledge'
        mine = self.kb.upload(actor('owner'), *_text('公司指南。', '指南.txt'))
        self.kb.upload(actor('other'), *_text('共享指南。', '指南-他人.txt'))
        root = (await self.client.post(base + '/groups', json={'name': '目录'})).json()['data']
        child = (await self.client.post(base + '/groups', json={'name': '目录', 'parent_id': root['id']})).json()['data']
        response = await self.client.get(base, params={'q': '指南', 'select_all': '1'})
        self.assertEqual(response.status_code, 200)
        selection = response.json()['data']['selection']
        self.assertEqual(selection['count'], 1)
        moved = await self.client.put(base + '/groups/' + child['id'] + '/documents', json={'selection': selection})
        self.assertEqual(moved.status_code, 200, moved.text)
        self.assertEqual(moved.json()['data']['updated'], 1)
        items = (await self.client.get(base, params={'group': child['id']})).json()['data']['items']
        self.assertEqual([item['id'] for item in items], [mine['id']])

    async def test_guests_and_invalid_multipart_do_not_store(self):
        base = '/api/assistant/knowledge'
        # guests are rejected before any service call
        self.assertEqual((await self.client.get(base, headers={'guest': '1'})).status_code, 403)
        # unknown multipart field is rejected before any service call
        response = await self.client.post(base + '/files', files={'wrong': ('a.txt', b'data')})
        self.assertEqual(response.status_code, 400)

    async def test_cross_user_read_delete_restore_are_real_service(self):
        base = '/api/assistant/knowledge'
        # seed through the REAL service (kb.upload), not a fake response
        admin = actor('admin', admin=True, name='Admin')
        document = self.kb.upload(admin, *_text('公司报销流程', '报销.txt'))
        self.assertTrue(self.kb.process_one())
        path = base + '/documents/' + document['id']

        with self.kb.connect() as db:
            row = db.execute('SELECT revision FROM documents WHERE id=?', (document['id'],)).fetchone()
            revision = row['revision']

        # another signed-in user can read (shared)
        other = {'headers': {'user': 'bob'}}
        listing = (await self.client.get(base, **other)).json()['data']
        self.assertEqual(listing['items'][0]['id'], document['id'])
        self.assertFalse(listing['items'][0]['can_edit'])

        # other user cannot delete (real service permission check -> 403)
        response = await self.client.request('DELETE', path, headers={'user': 'bob'},
                                             json={'version': revision})
        self.assertEqual(response.status_code, 403)
        # owner/admin can delete
        response = await self.client.request('DELETE', path, headers={'admin': '1'},
                                             json={'version': revision})
        self.assertEqual(response.status_code, 200)
        # file endpoint reflects the deletion (410)
        self.assertEqual((await self.client.get(path + '/file')).status_code, 410)
        # restore works
        response = await self.client.post(path + '/restore', headers={'admin': '1'}, json={'version': 2})
        self.assertEqual(response.status_code, 200)

    async def test_version_conflict_returns_409_through_route(self):
        base = '/api/assistant/knowledge'
        admin = actor('admin', admin=True, name='Admin')
        document = self.kb.upload(admin, *_text('再融资规则', '融资规则.txt'))
        self.assertTrue(self.kb.process_one())
        path = base + '/documents/' + document['id']

        with self.kb.connect() as db:
            row = db.execute('SELECT revision FROM documents WHERE id=?', (document['id'],)).fetchone()
            latest = row['revision']

        # stale version conflicts surface as 409 through the real service
        response = await self.client.request('DELETE', path, headers={'admin': '1'}, json={'version': latest - 1})
        self.assertEqual(response.status_code, 409)
        # matching version succeeds
        response = await self.client.request('DELETE', path, headers={'admin': '1'}, json={'version': latest})
        self.assertEqual(response.status_code, 200)

    async def test_group_routes_create_assign_filter_and_delete(self):
        base = '/api/assistant/knowledge'
        admin_headers = {'admin': '1'}
        # create group through the HTTP route (real service)
        response = await self.client.post(base + '/groups', headers=admin_headers, json={'name': '财务组'})
        self.assertEqual(response.status_code, 200, response.text)
        group = response.json()['data']
        self.assertEqual(group['name'], '财务组')

        # GET /groups must expose the exact HTTP contract the frontend consumes:
        # data.groups[] plus data.total. Keeping this in sync with the JS fixture
        # (check-knowledge-browser.mjs) prevents a mock-only mismatch from masking a
        # real API-contract regression.
        listed = await self.client.get(base + '/groups', headers=admin_headers)
        self.assertEqual(listed.status_code, 200, listed.text)
        groups_payload = listed.json()['data']
        self.assertIsInstance(groups_payload['groups'], list)
        self.assertEqual(groups_payload['total'], 1)
        first = groups_payload['groups'][0]
        for key in ('id', 'name', 'owner_name', 'created_at', 'count', 'can_manage'):
            self.assertIn(key, first, f'GET /groups group object missing "{key}"')
        self.assertEqual(first['id'], group['id'])

        # upload a document through the service, then assign via route
        document = self.kb.upload(actor('admin', admin=True, name='Admin'), *_text('公司报销', '报销.txt'))
        self.assertTrue(self.kb.process_one())
        assign = await self.client.put(f"{base}/groups/{group['id']}/documents", headers=admin_headers,
                                       json={'document_ids': [document['id']]})
        self.assertEqual(assign.status_code, 200, assign.text)
        self.assertEqual(assign.json()['data']['updated'], 1)

        # filter listing returns the grouped document
        listing = await self.client.get(base, headers=admin_headers, params={'group': group['id']})
        data = listing.json()['data']
        self.assertEqual(data['total'], 1)
        self.assertEqual(data['items'][0]['id'], document['id'])
        self.assertEqual(data['items'][0]['group_name'], '财务组')

        # ungrouped filter no longer contains it
        ungrouped = (await self.client.get(base, headers=admin_headers, params={'group': 'ungrouped'})).json()['data']
        self.assertEqual(ungrouped['total'], 0)

        # delete group through route; document returns to ungrouped and file/index intact
        removed = await self.client.request('DELETE', base + '/groups/' + group['id'], headers=admin_headers)
        self.assertEqual(removed.status_code, 200)
        ungrouped2 = (await self.client.get(base, headers=admin_headers, params={'group': 'ungrouped'})).json()['data']
        self.assertEqual(ungrouped2['total'], 1)
        # original file still downloadable
        file_resp = await self.client.get(base + '/documents/' + document['id'] + '/file')
        self.assertEqual(file_resp.status_code, 200)


if __name__ == '__main__':
    unittest.main()
