import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import httpx
from fastapi import FastAPI
from openclaw_service.assistant.lighthouse_knowledge import KnowledgeBase
from openclaw_service.assistant.lighthouse_knowledge_routes import install_knowledge_routes
from bin.test_lighthouse_knowledge import FakeEmbedder, actor, _protect


class KnowledgeRoutes(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.kb = KnowledgeBase(self.tmp.name, embedder=FakeEmbedder(), protect=_protect, start_worker=False)
        app = FastAPI()
        async def authorize(request):
            return {**actor(request.headers.get('user', 'owner'), admin=request.headers.get('admin') == '1',
                            guest=request.headers.get('guest') == '1'), 'channel': request.headers.get('channel', '')}
        install_knowledge_routes(app, authorize, lambda: self.kb)
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://fixture')

    async def asyncTearDown(self):
        await self.client.aclose()
        self.kb.close()
        self.tmp.cleanup()

    async def test_web_upload_feishu_read_same_document_and_mutation_permissions(self):
        base = '/api/assistant/knowledge'
        response = await self.client.put(base + '/settings', headers={'admin': '1'}, json={
            'endpoint': 'https://fixture.example/embeddings', 'model': 'test', 'api_key': 'fixture',
            'approved_origins': ['https://fixture.example']})
        self.assertEqual(response.status_code, 200, response.text)
        response = await self.client.post(base + '/files', files={'files': ('公司手册.txt', '公司报销需要真实凭证。'.encode())})
        document = response.json()['data']['items'][0]
        self.assertTrue(self.kb.process_one())
        response = await self.client.get(base + '/search', params={'q': '公司报销'}, headers={'user': 'other', 'channel': 'feishu:oc_group'})
        self.assertEqual(response.json()['data']['items'][0]['document_id'], document['id'])
        path = base + '/documents/' + document['id']
        response = await self.client.request('DELETE', path, headers={'user': 'other'}, json={'version': 1})
        self.assertEqual(response.status_code, 403)
        response = await self.client.request('DELETE', path, json={'version': 1})
        self.assertEqual(response.status_code, 200)
        self.assertEqual((await self.client.get(path + '/file')).status_code, 410)
        response = await self.client.post(path + '/restore', json={'version': 2})
        self.assertEqual(response.status_code, 200)
        self.assertFalse((await self.client.get(base + '/search', params={'q': '公司报销'})).json()['data']['items'])
        self.assertTrue(self.kb.process_one())
        self.assertEqual((await self.client.get(path + '/file')).status_code, 200)

    async def test_guests_and_invalid_multipart_do_not_store_documents(self):
        base = '/api/assistant/knowledge'
        self.assertEqual((await self.client.get(base, headers={'guest': '1'})).status_code, 403)
        response = await self.client.post(base + '/files', files={'wrong': ('a.txt', b'data')})
        self.assertEqual(response.status_code, 400)
        response = await self.client.post(base + '/files', files=[('files', ('a.txt', b'data')), ('files', ('bad.exe', b'bad'))])
        self.assertEqual(len(response.json()['data']['items']), 1)
        self.assertEqual(len(response.json()['data']['errors']), 1)


if __name__ == '__main__':
    unittest.main()
