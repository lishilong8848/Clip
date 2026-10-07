import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).parent))
from fastapi import FastAPI
from fastapi.testclient import TestClient
from openclaw_service.store import AssistantStore
from openclaw_service.assistant.lighthouse_api import PortalAPICatalog
from openclaw_service.assistant.routes import install_assistant_routes
from openclaw_service.assistant.lighthouse_ai import AssistantError


class SkillRoutesTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.store = AssistantStore(directory.name)
        app = FastAPI()

        @app.get('/api/repair-management/records')
        def original(scope: str = 'ALL'):
            raise AssertionError('Selecting a tool must never call a business handler')

        async def authorize(request):
            identity = request.headers.get('x-test-account')
            if identity not in {'one', 'two'}:
                raise AssistantError('测试账号未授权', 403)
            return {'id': identity, 'scopes': ['D'], 'is_admin': False}

        host = SimpleNamespace(store=self.store, state=Path(directory.name), catalog=PortalAPICatalog(app),
            authorize=authorize, portal_bridge=SimpleNamespace(search=lambda *args: ([], [])))
        install_assistant_routes(app, host)
        self.client = TestClient(app)
        self.headers = {'x-test-account': 'one'}

    def test_upload_shared_read_and_owner_delete_are_authenticated(self):
        body = b'\xef\xbb\xbf---\r\nname: route-example\r\ndisplay_name: Example skill\r\ndescription: Native queries only\r\n---\r\nDo not run scripts.'
        response = self.client.post('/api/assistant/skills/install', headers=self.headers, files={'file': ('SKILL.md', body, 'text/markdown')})
        self.assertEqual(response.status_code, 200, response.text)
        name = response.json()['data']['name']
        self.assertFalse(response.json()['data']['duplicate'])
        duplicate = self.client.post('/api/assistant/skills/install', headers=self.headers, files={'file': ('SKILL.md', body, 'text/markdown')})
        self.assertTrue(duplicate.json()['data']['duplicate'])
        self.assertEqual(self.client.get('/api/assistant/skills/' + name, headers={'x-test-account': 'two'}).status_code, 200)
        self.assertEqual(self.client.delete('/api/assistant/skills/' + name, headers={'x-test-account': 'two'}).status_code, 403)
        self.assertEqual(self.client.get('/api/assistant/skills/' + name, headers=self.headers).json()['data']['display_name'], 'Example skill')
        listing = self.client.get('/api/assistant/commands?keyword=Example', headers=self.headers)
        self.assertEqual(listing.json()['data']['items'][0]['name'], name)
        self.assertEqual(self.client.delete('/api/assistant/skills/' + name, headers=self.headers).status_code, 200)
        self.assertEqual(self.client.get('/api/assistant/skills/' + name, headers=self.headers).status_code, 404)

    def test_install_shape_size_and_auth_errors(self):
        unauth = self.client.get('/api/assistant/skills')
        self.assertEqual(unauth.status_code, 403)
        body = b'---\nname: test\ndescription: test\n---\nExample'
        for files in ({'files': ('SKILL.md', body)}, [('file', ('SKILL.md', body)), ('file', ('other.md', body))]):
            self.assertIn(self.client.post('/api/assistant/skills/install', headers=self.headers, files=files).status_code, {400, 413})
        invalid = self.client.post('/api/assistant/skills/install', headers=self.headers, files={'file': ('SKILL.zip', b'not a zip')})
        self.assertEqual(invalid.status_code, 400)
        self.assertEqual(self.client.get('/api/assistant/commands?kind=shell', headers=self.headers).status_code, 400)
        self.assertEqual(self.client.get('/api/assistant/skills/lighthouse-repairs?offset=bad', headers=self.headers).status_code, 400)
        self.assertIn(self.client.delete('/api/assistant/skills/lighthouse-repairs', headers=self.headers).status_code, {400, 404})

    def test_tools_are_native_catalogue_only_and_selection_is_not_execution(self):
        data = self.client.get('/api/assistant/commands?kind=tools&keyword=维修', headers=self.headers).json()['data']
        identities = {item['id'] for item in data['items']}
        self.assertIn('GET /api/repair-management/records', identities)
        self.assertFalse(any('DELETE /api/bitable' in item for item in identities))
        response = self.client.get('/api/assistant/skills', headers=self.headers)
        self.assertEqual(response.headers['cache-control'], 'no-store')


if __name__ == '__main__':
    unittest.main()
