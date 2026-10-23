"""Offline checks for request-owned model transports and stream cleanup."""
import asyncio
import json
import sys
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

import httpx
from fastapi import FastAPI, Request
from openclaw_service.assistant.lighthouse_ai import AssistantError, CustomModel
from openclaw_service.assistant.lighthouse_context import execution_key
from openclaw_service.assistant.lighthouse_runtime import OpenClawRuntime, account_key, install_model_route
from test_lighthouse_account_models import MemoryStore


class AsyncTransport(httpx.MockTransport):
    def __init__(self, handler):
        super().__init__(handler)
        self.closes = 0

    async def aclose(self):
        await asyncio.sleep(0)
        self.closes += 1


class Stream(httpx.AsyncByteStream):
    def __init__(self, read):
        self.read = read
        self.closed = False

    async def __aiter__(self):
        yield await self.read()

    async def aclose(self):
        await asyncio.sleep(0)
        self.closed = True


class RuntimeConnectionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.runtime = OpenClawRuntime(self.directory.name)
        self.runtime.gateway = {'token': 'synthetic-broker', 'stopped': True}
        app = FastAPI()
        install_model_route(app, self.runtime)
        self.caller = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1')
        self.clients, self.transports = [], []

    async def asyncTearDown(self):
        await self.caller.aclose()
        await self.runtime.close()
        self.directory.cleanup()

    def account(self, owner, **lane):
        key = account_key(execution_key({'id': owner, **lane}))
        item = {'agent_id': 'lh-' + key, 'busy': True,
                'model_key': 'synthetic-' + key,
                'profile': {'endpoint': 'https://provider.invalid/v1/chat/completions',
                            'model': 'model-' + key, 'context_window': 32000}}
        self.runtime.accounts[key] = item
        return item

    def provider(self, handler):
        real_client = httpx.AsyncClient

        def create(**kwargs):
            transport = AsyncTransport(handler)
            client = real_client(**kwargs, transport=transport)
            self.transports.append(transport)
            self.clients.append(client)
            return client

        return patch.object(httpx, 'AsyncClient', side_effect=create)

    async def invoke(self, item):
        return await self.caller.post('/api/assistant/openclaw-models/chat/completions',
            headers={'Authorization': 'Bearer synthetic-broker', 'Cookie': 'browser=private'},
            json={'model': item['agent_id'], 'messages': [{'role': 'user', 'content': 'hello'}]})

    def assert_closed(self):
        self.assertTrue(self.clients)
        self.assertTrue(all(client.is_closed for client in self.clients))
        self.assertEqual([transport.closes for transport in self.transports], [1] * len(self.clients))

    async def test_users_browsers_and_topics_overlap_with_distinct_clients_and_credentials(self):
        items = [self.account('first', _execution_lane='browser-a'),
                 self.account('second', _execution_lane='browser-a'),
                 self.account('first', _execution_lane='browser-b'),
                 self.account('first', channel='feishu:chat', _feishu_topic={'id': 'one'}),
                 self.account('first', channel='feishu:chat', _feishu_topic={'id': 'two'})]
        self.assertEqual(len({item['agent_id'] for item in items}), len(items))
        requests, ready = [], asyncio.Event()

        async def upstream(request):
            requests.append(request)
            if len(requests) == len(items):
                ready.set()
            await asyncio.wait_for(ready.wait(), 3)
            return httpx.Response(200, json={'ok': True}, headers={
                'Set-Cookie': 'provider=private; Path=/; Secure'})

        with self.provider(upstream):
            responses = await asyncio.gather(*(self.invoke(item) for item in items))
            # A later request to the same host must also get a fresh connection.
            responses.append(await self.invoke(items[0]))
        self.assertEqual([response.status_code for response in responses], [200] * 6)
        self.assertEqual(len({id(client) for client in self.clients}), 6)
        self.assertEqual(len({id(client._transport) for client in self.clients}), 6)
        expected = {'Bearer ' + item['model_key']: item['profile']['model'] for item in items}
        self.assertCountEqual([request.headers['authorization'] for request in requests],
                              ['Bearer ' + item['model_key'] for item in items + items[:1]])
        for request in requests:
            self.assertEqual(json.loads(request.content)['model'], expected[request.headers['authorization']])
            self.assertFalse(request.headers.get('cookie'))
        self.assertTrue(all(not list(client.cookies.items()) for client in self.clients))
        self.assertTrue(all('synthetic-' not in response.text for response in responses))
        self.assert_closed()

    async def test_cancel_one_during_headers_or_stream_keeps_other_request_alive(self):
        await self.runtime.model_client()
        for phase in ('headers', 'stream'):
            with self.subTest(phase=phase):
                items = [self.account('first'), self.account('second')]
                ready, release = asyncio.Event(), asyncio.Event()
                arrivals, streams = [], []

                async def read():
                    arrivals.append(True)
                    if len(arrivals) == 2:
                        ready.set()
                    await release.wait()
                    return b'{"ok":true}'

                async def upstream(request):
                    if phase == 'headers':
                        return httpx.Response(200, content=await read())
                    stream = Stream(read)
                    streams.append(stream)
                    return httpx.Response(200, stream=stream)

                offset = len(self.clients)
                with self.provider(upstream):
                    tasks = [asyncio.create_task(self.invoke(item)) for item in items]
                    try:
                        await asyncio.wait_for(ready.wait(), 3)
                        tasks[0].cancel()
                        with self.assertRaises(asyncio.CancelledError):
                            await tasks[0]
                        self.assertTrue(self.clients[offset].is_closed)
                        self.assertFalse(self.clients[offset + 1].is_closed)
                        self.assertFalse(tasks[1].done())
                        release.set()
                        self.assertEqual((await tasks[1]).json(), {'ok': True})
                    finally:
                        release.set()
                        for task in tasks:
                            if not task.done():
                                task.cancel()
                        await asyncio.gather(*tasks, return_exceptions=True)
                self.assertTrue(all(stream.closed for stream in streams))
                self.assert_closed()

    async def test_request_budget_cancels_error_body_and_closes_connection(self):
        item = self.account('first')
        entered = asyncio.Event()

        async def read():
            entered.set()
            await asyncio.Future()

        stream = Stream(read)
        async with asyncio.timeout(None) as budget:
            async def expire():
                await entered.wait()
                budget.reschedule(asyncio.get_running_loop().time())

            timer = asyncio.create_task(expire())
            try:
                with self.provider(lambda _: httpx.Response(400, stream=stream)):
                    with self.assertRaises(asyncio.CancelledError):
                        await self.invoke(item)
            finally:
                await timer
        self.assertTrue(budget.expired())
        self.assertTrue(stream.closed)
        self.assert_closed()

    async def test_injected_transport_is_not_closed_by_an_individual_request(self):
        item = self.account('first')
        transport = AsyncTransport(lambda _: httpx.Response(200, json={'ok': True}))
        self.runtime.http = httpx.AsyncClient(transport=transport)
        for _ in range(2):
            self.assertEqual((await self.invoke(item)).status_code, 200)
            self.assertFalse(self.runtime.http.is_closed)
            self.assertEqual(transport.closes, 0)
        await self.runtime.close()
        self.assertEqual(transport.closes, 1)

    async def test_rejections_redirect_timeout_and_connection_error_close_their_clients(self):
        item = self.account('first')
        for failure, expected in [(400, 400), (401, 401), (429, 429), (503, 503),
                                  (302, 302), (httpx.ReadTimeout, 504), (httpx.ConnectError, 502)]:
            with self.subTest(failure=failure):
                calls = []

                async def upstream(request):
                    calls.append(request)
                    if isinstance(failure, type):
                        raise failure('synthetic-private-detail')
                    return httpx.Response(failure, json={'error': {'message': item['model_key']}},
                        headers={'Location': 'https://other.invalid/steal'})

                with self.provider(upstream):
                    response = await self.invoke(item)
                self.assertEqual(response.status_code, expected)
                self.assertEqual(len(calls), 1)
                self.assertNotIn('synthetic-', response.text)
                self.assert_closed()

    async def test_stream_failure_or_disconnect_before_first_chunk_closes_response_and_client(self):
        item = self.account('first')
        for phase in ('read', 'send'):
            with self.subTest(phase=phase):
                async def read():
                    raise httpx.ReadTimeout('synthetic-private-detail')

                stream = Stream(read)
                body = json.dumps({'model': item['agent_id'], 'messages': []}).encode()

                async def receive():
                    return {'type': 'http.request', 'body': body, 'more_body': False}

                async def send(message):
                    if phase == 'send':
                        raise asyncio.CancelledError()

                scope = {'type': 'http', 'method': 'POST', 'path': '/',
                         'client': ('127.0.0.1', 1), 'asgi': {'spec_version': '2.4'},
                         'headers': [(b'authorization', b'Bearer synthetic-broker')]}
                with self.provider(lambda _: httpx.Response(200, stream=stream)):
                    response = await self.runtime.forward_model(Request(scope, receive))
                    self.assertFalse(self.clients[-1].is_closed)
                    with self.assertRaises(httpx.ReadTimeout if phase == 'read' else asyncio.CancelledError):
                        await response(scope, receive, send)
                self.assertTrue(stream.closed)
                self.assert_closed()


class SyncTransport(httpx.MockTransport):
    def __init__(self, handler):
        super().__init__(handler)
        self.closes = 0

    def close(self):
        self.closes += 1


class CustomModelConnectionTests(unittest.TestCase):
    def setUp(self):
        self.base = CustomModel(MemoryStore(), protect=lambda key: key, unprotect=lambda key: key)
        self.models = [self.base.for_actor(owner) for owner in ('first', 'second')]
        for index, model in enumerate(self.models):
            model.configure({'action': 'upsert', 'profile': {'id': 'fixture', 'name': 'Fixture',
                'model': 'model-' + str(index), 'endpoint': 'https://provider.invalid/v1/chat/completions',
                'api_key': 'synthetic-' + str(index)}})
            model.configure({'action': 'select', 'id': 'fixture'})
        self.clients, self.transports, self.options = [], [], []

    def provider(self, handler):
        real_client = httpx.Client

        def create(**kwargs):
            transport = SyncTransport(handler)
            client = real_client(**kwargs, transport=transport)
            self.options.append(kwargs)
            self.clients.append(client)
            self.transports.append(transport)
            return client

        return patch.object(httpx, 'Client', side_effect=create)

    def assert_closed(self):
        self.assertTrue(all(client.is_closed for client in self.clients))
        self.assertEqual([transport.closes for transport in self.transports], [1] * len(self.clients))

    def test_parallel_bound_accounts_and_topics_use_separate_clients(self):
        barrier = threading.Barrier(4, timeout=3)
        requests = []

        def upstream(request):
            requests.append(request)
            barrier.wait()
            return httpx.Response(200, json={'choices': [{'message': {'content': 'ok'}}]},
                headers={'Set-Cookie': 'provider=private; Path=/; Secure'})

        with self.provider(upstream), ThreadPoolExecutor(max_workers=4) as workers:
            tasks = [workers.submit(model.complete, [{'role': 'user', 'content': topic}], timeout=7)
                     for model in self.models for topic in ('one', 'two')]
            self.assertEqual([task.result(timeout=5) for task in tasks], ['ok'] * 4)
        self.assertEqual(len({id(client) for client in self.clients}), 4)
        for request in requests:
            model_id = json.loads(request.content)['model']
            self.assertEqual(request.headers['authorization'], 'Bearer synthetic-' + model_id[-1])
            self.assertFalse(request.headers.get('cookie'))
            self.assertEqual(set(request.extensions['timeout'].values()), {7})
        for client, options in zip(self.clients, self.options):
            self.assertFalse(list(client.cookies.items()))
            self.assertFalse(options['trust_env'])
            self.assertFalse(options['follow_redirects'])
            self.assertEqual(options['limits'].max_connections, 1)
            self.assertEqual(options['limits'].max_keepalive_connections, 0)
            self.assertEqual(options['timeout'].as_dict(), {'connect': 5, 'read': 75, 'write': 20, 'pool': 5})
        self.assert_closed()

    def test_timeout_invalid_response_and_cancellation_close_the_owned_client(self):
        for failure in (httpx.ReadTimeout, asyncio.CancelledError, ValueError, 302, 401, 503, 'invalid', 'empty'):
            with self.subTest(failure=failure):
                def upstream(request):
                    if isinstance(failure, type):
                        raise failure('synthetic-private-detail')
                    if isinstance(failure, int):
                        return httpx.Response(failure, json={'error': {'message': 'synthetic-private-detail'}},
                            headers={'Location': 'https://other.invalid/steal'})
                    return httpx.Response(200, json={} if failure == 'invalid' else {'choices': []})

                with self.provider(upstream):
                    with self.assertRaises(asyncio.CancelledError if failure is asyncio.CancelledError else AssistantError) as caught:
                        self.models[0].complete([])
                if failure is not asyncio.CancelledError:
                    self.assertNotIn('synthetic-', str(caught.exception))
                self.assert_closed()

    def test_injected_client_is_borrowed_even_when_a_bound_call_fails(self):
        client = Mock()
        client.request_json.side_effect = [{'choices': [{'message': {'content': 'ok'}}]}, {}]
        self.base.client = client
        model = self.base.for_actor('first')
        self.assertEqual(model.complete([]), 'ok')
        with self.assertRaises(AssistantError):
            model.complete([])
        model.close()
        self.base.close()
        client.close.assert_not_called()


if __name__ == '__main__':
    unittest.main()
