"""Exercise the public ingress and alarm delivery with all Feishu calls mocked."""
import asyncio
import hashlib
import hmac
import json
from pathlib import Path
import sys
import tempfile
import unittest

import httpx
from fastapi import FastAPI

sys.path.insert(0, str(Path(__file__).resolve().parent))
from alarm_scan.routes import install_alarm_scan_routes, PUBLIC_PAGE
from public_access.config import Settings as PublicSettings
from public_access.ingress import MARKER, PublicGuard


class AlarmScanPublicTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.folder = tempfile.TemporaryDirectory(prefix='alarm_scan_public_')
        self.addCleanup(self.folder.cleanup)
        self.config_path = Path(self.folder.name) / 'config.json'
        self.config_path.write_text(json.dumps({
            'enabled': True,
            'FEISHU_APP_ID': 'cli_offline_fixture',
            'FEISHU_APP_SECRET': 'offline-only-secret',
            'FEISHU_CHAT_ID': 'oc_offline_fixture',
            'ALARM_RECEIVER_TOKEN': 'offline-receiver-token',
            'SCAN_RELAY_SIGNING_KEY': 'offline-signing-key',
        }), encoding='utf-8')
        self.calls = []

        def feishu(request):
            self.calls.append(str(request.url))
            if '/tenant_access_token/internal' in str(request.url):
                return httpx.Response(200, json={'code': 0, 'tenant_access_token': 'offline-token', 'expire': 7200})
            if '/im/v1/messages' in str(request.url):
                if getattr(self, 'timeout_message', False):
                    raise httpx.ReadTimeout('offline timeout', request=request)
                return httpx.Response(200, json={'code': 0, 'data': {'message_id': 'om_offline_fixture'}})
            raise AssertionError('Unexpected outbound HTTP')

        app = FastAPI()

        @app.get('/api/private-business')
        async def private_business():
            return {'unexpected': True}

        install_alarm_scan_routes(app, self.config_path, httpx.MockTransport(feishu))
        settings = PublicSettings(enabled=True, public_url='https://dt.sjhl.online',
                                  server_addr='192.0.2.2', token='offline-frp-token')
        self.guard = PublicGuard(app, 'offline-ingress-marker', settings, lambda request: None)
        self.app = app
        self.lifespan = app.router.lifespan_context(app)
        await self.lifespan.__aenter__()
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.guard, client=('127.0.0.1', 10000)),
            base_url='https://dt.sjhl.online',
            headers={MARKER.decode(): 'offline-ingress-marker'},
        )

    async def asyncTearDown(self):
        await self.client.aclose()
        await self.lifespan.__aexit__(None, None, None)

    def payload(self, stamp):
        building, text = 'E', '【事件通告】离线扫码，勿发送真实消息'
        body = json.dumps([building, text], ensure_ascii=False, separators=(',', ':')).encode('utf-8')
        return {'louhao': building, 'content': text, 'ts': stamp,
                'signature': hmac.new(b'offline-signing-key', body, hashlib.sha256).hexdigest()}

    async def test_public_scan_sends_each_new_scan_without_portal_login(self):
        page = await self.client.get('/alarm-scan/scan', params={'target': PUBLIC_PAGE})
        self.assertEqual(page.status_code, 200)
        self.assertIn("fetch('/api/alarm-scan/events'", page.text)
        self.assertIn('data-dingtalk-managed="false"', page.text)
        self.assertEqual((await self.client.get('/api/alarm-scan/health')).status_code, 200)
        headers = {'Origin': 'https://dt.sjhl.online'}
        first = await self.client.post('/api/alarm-scan/events', json=self.payload(1), headers=headers)
        repeat = await self.client.post('/api/alarm-scan/events', json=self.payload(1), headers=headers)
        second = await self.client.post('/api/alarm-scan/events', json=self.payload(2), headers=headers)
        self.assertEqual([r.json()['feishu']['status'] for r in (first, repeat, second)],
                         ['sent', 'duplicate', 'sent'])
        self.assertEqual(sum('/im/v1/messages' in call for call in self.calls), 2)

    async def test_public_boundary_rejects_unsigned_and_private_routes(self):
        headers = {'Origin': 'https://dt.sjhl.online'}
        bad = self.payload(3)
        bad['content'] += ' altered'
        self.assertEqual((await self.client.post('/api/alarm-scan/events', json=bad, headers=headers)).status_code, 403)
        self.assertEqual((await self.client.post('/api/alarm-scan/events', json=self.payload(3),
                                                 headers={'Origin': 'https://example.org'})).status_code, 403)
        self.assertEqual((await self.client.get('/api/private-business')).status_code, 403)
        self.assertEqual((await self.client.get('/api/qt/status')).status_code, 403)
        self.assertFalse(self.calls)

    async def test_uncertain_send_is_not_reported_success_or_repeated_after_restart(self):
        self.timeout_message = True
        headers = {'Origin': 'https://dt.sjhl.online'}
        first = await self.client.post('/api/alarm-scan/events', json=self.payload(10), headers=headers)
        self.assertEqual(first.status_code, 503)
        self.assertEqual(first.json()['status'], 'failed')
        await self.lifespan.__aexit__(None, None, None)
        self.lifespan = self.app.router.lifespan_context(self.app)
        await self.lifespan.__aenter__()
        second = await self.client.post('/api/alarm-scan/events', json=self.payload(10), headers=headers)
        self.assertEqual(second.status_code, 409)
        self.assertEqual(second.json()['status'], 'uncertain')
        self.assertEqual(sum('/im/v1/messages' in call for call in self.calls), 1)


if __name__ == '__main__':
    unittest.main()
