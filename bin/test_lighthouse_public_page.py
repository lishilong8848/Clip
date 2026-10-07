"""Public-page parsing and outbound safety with isolated HTTP/DNS substitutes."""
import asyncio
import datetime as dt
import gzip
import socket
import ssl
import sys
import tempfile
import time
import unittest
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import patch

import httpx

sys.path.insert(0, str(Path(__file__).parent))
from openclaw_service.assistant import lighthouse_public_page as page
from openclaw_service.assistant.lighthouse_public import MAX_BYTES, PublicQueryError
from openclaw_service.assistant.lighthouse_ai import CONTACT, safe_text

URL = 'https://docs.python.org/3/contents.html'
QUESTION = '请阅读 ' + URL + '，说明 Python 控制流'
HTML = b'''<html><head><title>Python documentation</title><script>SECRET_SCRIPT</script></head>
<body><nav><a href="/navigation">Navigation</a>NOT_BODY</nav>
<div hidden><div>HIDDEN_INNER</div>HIDDEN_OUTER</div><main><h1>Control flow</h1>
<p>Use if and for.</p><a href="tutorial/controlflow.html">Control Flow Tools</a>
<a href="https://evil.example/private">External</a><a href="/secret?token=secret">Secret</a>
<a href="javascript:alert(1)">JS</a></main></body></html>'''


def dns_rows(*addresses):
    return [(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, '', (address, 443)) for address in addresses]


class PublicPageTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.requests, self.clients = [], []
        self.enterContext(patch.object(page, '_verified_tls_context', return_value=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)))
        self.dns = self.enterContext(patch.object(socket, 'getaddrinfo', return_value=dns_rows('1.1.1.1')))
        self.proxies = self.enterContext(patch.object(page, 'getproxies', create=True, return_value={}))
        self.bypass = self.enterContext(patch.object(page, 'proxy_bypass', create=True, return_value=False))

    def factory(self, handler=None):
        def create():
            def respond(request):
                self.requests.append(request)
                return handler(request) if handler else httpx.Response(200, headers={'content-type': 'text/html'}, content=HTML)
            client = httpx.AsyncClient(transport=httpx.MockTransport(respond), follow_redirects=False)
            self.clients.append(client)
            return client
        return create

    async def read(self, **kwargs):
        return await page.read_page(URL, QUESTION, client_factory=self.factory(), **kwargs)

    async def test_public_text_links_and_dns_pinning(self):
        self.dns.side_effect = [dns_rows('1.1.1.1'), dns_rows('127.0.0.1')]
        value = await self.read()
        self.assertEqual(self.dns.call_count, 1)
        request = self.requests[0]
        self.assertEqual(request.url.host, '1.1.1.1')
        self.assertEqual(request.headers['host'], 'docs.python.org')
        self.assertEqual(request.extensions['sni_hostname'], 'docs.python.org')
        self.assertNotIn('cookie', request.headers)
        self.assertNotIn('authorization', request.headers)
        self.assertIn('if and for', value['text'])
        for hidden in ('SECRET_SCRIPT', 'NOT_BODY', 'HIDDEN_INNER', 'HIDDEN_OUTER'):
            self.assertNotIn(hidden, value['text'])
        self.assertEqual(value['links'][0]['link'], 'https://docs.python.org/3/tutorial/controlflow.html')
        self.assertTrue(all('evil' not in link['link'] and 'token' not in link['link'] for link in value['links']))
        self.assertTrue(self.clients[0].is_closed)

    def test_public_page_cache_policy_is_bounded_and_fail_closed(self):
        for headers, expected in (({}, 0), ({'cache-control': 'max-age=600, public'}, 60),
                ({'cache-control': 'max-age="30"', 'age': '5'}, 25),
                ({'cache-control': 'max-age=30', 'age': '31'}, 0),
                ({'cache-control': 'max-age=30, max-age=60'}, 0),
                ({'cache-control': 'max-age="30'}, 0),
                ({'cache-control': 'max-age=60', 'age': 'invalid'}, 0),
                ({'cache-control': 'max-age=60', 'set-cookie': 'anonymous-session'}, 0),
                ({'cache-control': 'max-age=60', 'vary': 'Cookie'}, 0),
                ({'cache-control': 'max-age=60', 'vary': '*'}, 0)):
            with self.subTest(headers=headers):
                self.assertEqual(page._page_cache_seconds(URL, httpx.Headers(headers)), expected)
        for directive in ('private', 'no-store', 'no-cache'):
            self.assertEqual(page._page_cache_seconds(URL, httpx.Headers({
                'cache-control': 'max-age=60, ' + directive})), 0)
        self.assertEqual(page._page_cache_seconds(URL + '?temporary=opaque', httpx.Headers({
            'cache-control': 'max-age=60'})), 0)

    async def test_read_response_retains_server_cache_lifetime(self):
        value = await page.read_page(URL, QUESTION, client_factory=self.factory(lambda _: httpx.Response(200,
            headers={'content-type': 'text/html', 'cache-control': 'public, max-age=30', 'age': '5'}, content=HTML)))
        self.assertGreater(value['cache_seconds'], 0)
        self.assertLessEqual(value['cache_seconds'], 25)
        self.assertTrue(self.clients[-1].is_closed)

    async def test_download_time_cannot_extend_server_freshness(self):
        async def delayed(_):
            await asyncio.sleep(.05)
            return httpx.Response(200, headers={'content-type': 'text/html', 'cache-control': 'max-age=1'}, content=HTML)
        value = await page.read_page(URL, QUESTION, client_factory=self.factory(delayed))
        self.assertEqual(value['cache_seconds'], 0)
        self.assertTrue(self.clients[-1].is_closed)

    async def test_invalid_or_private_inputs_never_resolve_or_connect(self):
        for url, question in [(URL, '读取本系统D楼维修单'), (URL, '读取身份证和住址'),
                ('https://localhost/a', '阅读公开页面'), ('https://127.0.0.1/a', '阅读公开页面'),
                ('https://example.com:8443/a', '阅读公开页面'), ('http://example.com/a', '阅读公开页面'),
                ('https://example.com/?token=secret', '阅读公开页面'),
                ('https://example.com/recAbc123456789', '阅读公开页面'),
                ('https://example.com/%73%6b-abcdefghijklm', '阅读公开页面'),
                ('https://example.com/%2573%256b-abcdefghijklm', '阅读公开页面')]:
            with self.subTest(url=url, question=question), self.assertRaises(PublicQueryError):
                await page.read_page(url, question, client_factory=self.factory())
        self.dns.assert_not_called()
        self.assertEqual(self.requests, [])

    async def test_private_mixed_mapped_and_tunnel_dns_fail_closed(self):
        for values in [('127.0.0.1',), ('1.1.1.1', '10.0.0.2'), ('169.254.169.254',),
                ('::ffff:127.0.0.1',), ('2002:7f00:1::',), ('2606:4700:4700::1111%eth0',), ('224.0.0.1',)]:
            with self.subTest(values=values):
                self.dns.return_value = dns_rows(*values)
                with self.assertRaises(PublicQueryError):
                    await self.read()
        self.assertEqual(self.requests, [])

    async def test_proxy_fake_dns_uses_fixed_public_dns_and_only_hostname(self):
        self.dns.return_value = dns_rows('198.18.0.2')
        requests = []
        def handler(request):
            requests.append(request)
            return httpx.Response(200, json={'Status': 0, 'TC': False,
                'Question': {'name': 'docs.python.org.', 'type': 1},
                'Answer': [{'name': 'docs.python.org.', 'type': 5, 'data': 'cdn.example.'},
                           {'name': 'cdn.example.', 'type': 1, 'data': '1.1.1.1'}]})
        @asynccontextmanager
        async def dns_client():
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                yield client
        value = await self.read(dns_client=dns_client)
        self.assertTrue(value['ok'])
        self.assertEqual(self.requests[0].url.host, '1.1.1.1')
        self.assertEqual(requests[0].url.host, 'dns.alidns.com')
        self.assertEqual(dict(requests[0].url.params), {'name': 'docs.python.org', 'type': 'A'})

    async def test_public_dns_invalid_answers_cannot_authorize_page_connection(self):
        self.dns.return_value = dns_rows('198.18.0.2')
        for data in ({'Status': 0, 'Question': [{'name': 'other.example.', 'type': 1}], 'Answer': []},
                {'Status': 0, 'TC': True, 'Question': [{'name': 'docs.python.org.', 'type': 1}], 'Answer': []},
                {'Status': 0, 'Question': [{'name': 'docs.python.org.', 'type': 1}],
                 'Answer': [{'name': 'docs.python.org.', 'type': 1, 'data': '127.0.0.1'}]},
                {'Status': 0, 'Question': [{'name': 'docs.python.org.', 'type': 1}],
                 'Answer': [{'name': 'other.example.', 'type': 1, 'data': '1.1.1.1'}]}):
            @asynccontextmanager
            async def dns_client():
                async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200, json=data))) as client:
                    yield client
            with self.subTest(data=data), self.assertRaises(PublicQueryError):
                await self.read(dns_client=dns_client)
        self.assertEqual(self.requests, [])

    async def test_public_dns_http_failure_uses_next_fixed_provider(self):
        self.dns.return_value = dns_rows('198.18.0.2')
        hosts = []
        def handler(request):
            hosts.append(request.url.host)
            if len(hosts) == 1:
                return httpx.Response(503)
            return httpx.Response(200, json={'Status': 0,
                'Question': [{'name': 'docs.python.org.', 'type': 1}],
                'Answer': [{'name': 'docs.python.org.', 'type': 1, 'data': '1.1.1.1'}]})
        @asynccontextmanager
        async def dns_client():
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                yield client
        self.assertTrue((await self.read(dns_client=dns_client))['ok'])
        self.assertEqual(hosts, ['dns.alidns.com', 'cloudflare-dns.com'])

    async def test_redirect_binary_and_compression_are_not_read(self):
        for response in (httpx.Response(302, headers={'location': 'https://127.0.0.1/'}),
                httpx.Response(200, headers={'content-type': 'application/pdf'}, content=b'PDF'),
                httpx.Response(200, headers={'content-type': 'text/html', 'content-encoding': 'gzip'}, content=gzip.compress(HTML))):
            with self.subTest(response=response), self.assertRaises(PublicQueryError):
                await page.read_page(URL, QUESTION, client_factory=self.factory(lambda _: response))
        self.assertEqual(len(self.requests), 3)
        self.assertTrue(all(client.is_closed for client in self.clients))

    async def test_large_page_is_bounded_and_explicitly_partial(self):
        value = await page.read_page(URL, QUESTION, client_factory=self.factory(lambda _: httpx.Response(200,
            headers={'content-type': 'text/plain'}, content=b'a' * (MAX_BYTES + 1))))
        self.assertTrue(value['truncated'])
        self.assertEqual(len(value['text']), page.MAX_TEXT)
        self.assertTrue(self.clients[0].is_closed)

    async def test_cancelled_stream_closes_client(self):
        entered, closed = asyncio.Event(), []
        class Body(httpx.AsyncByteStream):
            async def __aiter__(self):
                entered.set()
                yield b'<p>Start</p>'
                await asyncio.sleep(20)
            async def aclose(self):
                closed.append(True)
        task = asyncio.create_task(page.read_page(URL, QUESTION, client_factory=self.factory(
            lambda _: httpx.Response(200, headers={'content-type': 'text/html'}, stream=Body()))))
        await asyncio.wait_for(entered.wait(), 2)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(closed)
        self.assertTrue(self.clients[0].is_closed)

    async def test_production_constructor_keeps_tls_and_disables_environment_proxy(self):
        original = httpx.AsyncClient
        arguments = []
        def create(**kwargs):
            arguments.append(kwargs)
            return original(transport=httpx.MockTransport(lambda _: httpx.Response(200,
                headers={'content-type': 'text/plain'}, content=b'Public text.')))
        with patch.object(httpx, 'AsyncClient', side_effect=create):
            value = await page.read_page(URL, QUESTION)
        self.assertTrue(value['ok'])
        self.assertFalse(arguments[0]['trust_env'])
        self.assertFalse(arguments[0]['follow_redirects'])
        self.assertEqual(arguments[0]['verify'].verify_mode, ssl.CERT_REQUIRED)
        self.assertTrue(arguments[0]['verify'].check_hostname)

    async def test_configured_proxy_keeps_pinned_ip_and_original_tls_hostname(self):
        self.proxies.return_value = {'https': 'http://fixture-user:private-proxy-password@127.0.0.1:7890'}
        original, arguments = httpx.AsyncClient, []
        def create(**kwargs):
            arguments.append(kwargs)
            return original(transport=httpx.MockTransport(lambda request: (self.requests.append(request) or
                httpx.Response(200, headers={'content-type': 'text/plain'}, content=b'Public text.'))))
        with patch.object(httpx, 'AsyncClient', side_effect=create):
            value = await page.read_page(URL, QUESTION)
        self.assertTrue(value['ok'])
        self.assertEqual(arguments[0]['proxy'].url.host, '127.0.0.1')
        self.assertFalse(arguments[0]['trust_env'])
        self.assertFalse(arguments[0]['follow_redirects'])
        context = arguments[0]['verify']
        self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)
        self.assertTrue(context.check_hostname)
        self.assertEqual(context.wrap_bio(ssl.MemoryBIO(), ssl.MemoryBIO(), server_hostname='1.1.1.1').server_hostname, 'docs.python.org')
        self.assertEqual(context.wrap_bio(ssl.MemoryBIO(), ssl.MemoryBIO(), server_hostname='proxy.example').server_hostname, 'proxy.example')
        self.assertEqual(self.requests[0].url.host, '1.1.1.1')
        self.assertEqual(self.requests[0].headers['host'], 'docs.python.org')
        self.assertNotIn('authorization', self.requests[0].headers)
        self.assertNotIn('private-proxy-password', str(value))

    async def test_no_proxy_bypass_never_selects_configured_proxy(self):
        self.bypass.return_value = True
        self.proxies.return_value = {'https': 'http://127.0.0.1:7890'}
        original, arguments = httpx.AsyncClient, []
        def create(**kwargs):
            arguments.append(kwargs)
            return original(transport=httpx.MockTransport(lambda _: httpx.Response(200,
                headers={'content-type': 'text/plain'}, content=b'Public text.')))
        with patch.object(httpx, 'AsyncClient', side_effect=create):
            self.assertTrue((await page.read_page(URL, QUESTION))['ok'])
        self.assertIsNone(arguments[0].get('proxy'))
        self.proxies.assert_not_called()

    def test_long_unbroken_text_filter_is_bounded_and_contacts_still_match(self):
        start = time.monotonic()
        self.assertEqual(safe_text('a' * MAX_BYTES, limit=6000), 'a' * 6000)
        self.assertLess(time.monotonic() - start, 2)
        for value in ('person@example.com', 'prefix+person@example.com', '中文person@example.com',
                      'path/person@example.com', 'a' * 1000 + '@example.com', '+86 13812345678'):
            with self.subTest(value=value[:50]):
                self.assertIsNotNone(CONTACT.search(value))


class PinnedTLSVerificationTests(unittest.TestCase):
    def test_actual_handshake_checks_original_hostname_and_trusted_ca(self):
        from cryptography import x509
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.x509.oid import NameOID
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'expected.example')])
        now = dt.datetime.now(dt.timezone.utc)
        certificate = (x509.CertificateBuilder().subject_name(name).issuer_name(name)
            .public_key(key.public_key()).serial_number(x509.random_serial_number())
            .not_valid_before(now - dt.timedelta(days=1)).not_valid_after(now + dt.timedelta(days=1))
            .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
            .add_extension(x509.SubjectAlternativeName([x509.DNSName('expected.example')]), critical=False)
            .sign(key, hashes.SHA256()).public_bytes(serialization.Encoding.PEM))
        source = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        source.load_verify_locations(cadata=certificate.decode())
        with tempfile.TemporaryDirectory() as directory:
            cert_file, key_file = Path(directory) / 'certificate.pem', Path(directory) / 'key.pem'
            cert_file.write_bytes(certificate)
            key_file.write_bytes(key.private_bytes(serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
            server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            server_context.load_cert_chain(cert_file, key_file)
            def handshake(context):
                incoming, outgoing, server_in, server_out = [ssl.MemoryBIO() for _ in range(4)]
                client = context.wrap_bio(incoming, outgoing, server_hostname='1.1.1.1')
                server = server_context.wrap_bio(server_in, server_out, server_side=True)
                for _ in range(10):
                    client_ready = server_ready = False
                    try:
                        client.do_handshake(); client_ready = True
                    except ssl.SSLWantReadError:
                        pass
                    data = outgoing.read()
                    if data: server_in.write(data)
                    try:
                        server.do_handshake(); server_ready = True
                    except ssl.SSLWantReadError:
                        pass
                    data = server_out.read()
                    if data: incoming.write(data)
                    if client_ready and server_ready: return
                self.fail('TLS memory handshake did not finish')
            handshake(page._PinnedHostnameContext(source, '1.1.1.1', 'expected.example'))
            with self.assertRaises(ssl.SSLCertVerificationError):
                handshake(page._PinnedHostnameContext(source, '1.1.1.1', 'wrong.example'))
            with self.assertRaises(ssl.SSLCertVerificationError):
                handshake(page._PinnedHostnameContext(ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT), '1.1.1.1', 'expected.example'))


if __name__ == '__main__':
    unittest.main()
