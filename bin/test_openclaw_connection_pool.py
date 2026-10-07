"""Local transports are pooled, created off-loop, and never replay failures."""
import asyncio
from pathlib import Path
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import httpx
from openclaw_service.bridge import PortalBridge
from openclaw_service.client import ResidentRuntime
from openclaw_service.protocol import ServiceError

REAL_ASYNC = httpx.AsyncClient


class ConnectionPoolTests(unittest.IsolatedAsyncioTestCase):
    async def test_runtime_reuses_one_client_and_keeps_per_request_timeouts(self):
        instances, threads, requests = [], [], []
        loop_thread = threading.get_ident()

        def handler(request):
            requests.append(request)
            return httpx.Response(200, json={'ok': True, 'data': {'accepted': True}})

        class Client(REAL_ASYNC):
            def __init__(self, **kwargs):
                threads.append(threading.get_ident())
                time.sleep(.08)
                super().__init__(**kwargs, transport=httpx.MockTransport(handler))
                instances.append(self)

        with tempfile.TemporaryDirectory() as directory, patch('httpx.AsyncClient', Client):
            runtime = ResidentRuntime(directory, callback_url=lambda: '')
            runtime.descriptor, runtime.key, runtime.instance = {'port': 7310}, 'fixture-key', 'fixture-instance'
            ticked = False
            async def tick():
                nonlocal ticked
                await asyncio.sleep(.02)
                ticked = not instances
            await asyncio.gather(tick(), runtime._request('health', timeout=3), runtime._request('health', timeout=9))
            self.assertTrue(ticked)
            self.assertEqual(len(instances), 1)
            self.assertNotIn(loop_thread, threads)
            self.assertEqual([request.extensions['timeout']['read'] for request in requests], [3, 9])
            self.assertTrue(all(request.headers['authorization'] == 'Bearer fixture-key' for request in requests))
            await runtime.close()
            self.assertTrue(instances[0].is_closed)

    async def test_transport_timeout_does_not_replay_control_request(self):
        requests = []
        def handler(request):
            requests.append(request)
            raise httpx.ReadTimeout('synthetic dropped response', request=request)
        with tempfile.TemporaryDirectory() as directory:
            runtime = ResidentRuntime(directory, callback_url=lambda: '')
            runtime.descriptor, runtime.key, runtime.instance = {'port': 7310}, 'fixture-key', 'fixture-instance'
            runtime.http = REAL_ASYNC(transport=httpx.MockTransport(handler), trust_env=False)
            with self.assertRaises(ServiceError):
                await runtime._request('acquire')
            self.assertEqual(len(requests), 1)
            self.assertTrue(runtime.disconnected)
            await runtime.close()

    async def test_bridge_reuses_client_but_rechecks_each_lease(self):
        instances, threads, seen = [], [], []
        lease = {'id': 'lease', 'callback_url': 'http://127.0.0.1:7311/api/assistant/service-bridge'}
        active = True
        def current(payload):
            if not active or payload['lease'] != 'lease':
                raise ServiceError('expired', 409, 'lease_expired')
            return lease
        host = SimpleNamespace(key='fixture-key', instance='fixture-instance', lease=lease, current_lease=current)
        def handler(request):
            seen.append(request)
            return httpx.Response(200, json={'ok': True, 'data': {'accepted': True}})
        class Client(REAL_ASYNC):
            def __init__(self, **kwargs):
                threads.append(threading.get_ident())
                super().__init__(**kwargs, transport=httpx.MockTransport(handler))
                instances.append(self)
        bridge = PortalBridge(host)
        with patch('httpx.AsyncClient', Client):
            for _ in range(3):
                await bridge.acall('search', context={'id': 'context', 'lease': 'lease'})
            active = False
            with self.assertRaises(ServiceError):
                await bridge.acall('search', context={'id': 'context', 'lease': 'lease'})
            self.assertEqual(len(seen), 3)
            self.assertEqual(len(instances), 1)
            self.assertNotIn(threading.get_ident(), threads)
            await bridge.close()
            self.assertTrue(instances[0].is_closed)


if __name__ == '__main__':
    unittest.main()
