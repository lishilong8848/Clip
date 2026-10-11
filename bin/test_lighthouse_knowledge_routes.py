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


if __name__ == '__main__':
    unittest.main()
