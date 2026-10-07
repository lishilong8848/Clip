"""Read bounded public text pages, pinning DNS without weakening TLS checks."""
from __future__ import annotations

import asyncio
import ipaddress
import json
import math
import re
import socket
import ssl
from html.parser import HTMLParser
from urllib.parse import unquote, urlencode, urljoin, urlsplit, urlunsplit
from urllib.request import getproxies, proxy_bypass

import httpx

from .lighthouse_ai import API_KEY, CONTACT, private_identifier, safe_text
from .lighthouse_public import (MAX_BYTES, PublicQueryError, _forbidden_inputs, _now_iso,
    _read_bounded, _verified_public_url, _verified_tls_context)

MAX_TEXT = 6000
MAX_LINKS = 40
VOID_TAGS = frozenset({'area', 'base', 'br', 'col', 'embed', 'hr', 'img', 'input', 'link', 'meta', 'param', 'source', 'track', 'wbr'})
FAKE_DNS = ipaddress.ip_network('198.18.0.0/15')
DOH_ENDPOINTS = ('https://dns.alidns.com/resolve', 'https://cloudflare-dns.com/dns-query', 'https://dns.google/resolve')


class _PinnedHostnameContext(ssl.SSLContext):
    def __new__(cls, source, address, host):
        return super().__new__(cls, ssl.PROTOCOL_TLS_CLIENT)

    def __init__(self, source, address, host):
        self._address, self._host = address, host
        self.options = source.options
        self.minimum_version, self.maximum_version = source.minimum_version, source.maximum_version
        self.verify_mode, self.check_hostname = source.verify_mode, source.check_hostname
        self.verify_flags = source.verify_flags
        self.set_ciphers(':'.join(cipher['name'] for cipher in source.get_ciphers()))
        certificates = ''.join(ssl.DER_cert_to_PEM_cert(cert) for cert in source.get_ca_certs(binary_form=True))
        if certificates:
            self.load_verify_locations(cadata=certificates)

    def wrap_bio(self, incoming, outgoing, server_side=False, server_hostname=None, session=None):
        # HTTPcore's proxy tunnel ignores the request's SNI extension. Keep the
        # CONNECT destination pinned, but verify the original site's hostname.
        if server_hostname == self._address:
            server_hostname = self._host
        return super().wrap_bio(incoming, outgoing, server_side, server_hostname, session)


def _page_proxy(host):
    if proxy_bypass(host):
        return None
    proxies = getproxies()
    value = proxies.get('https') or proxies.get('all')
    if not value:
        return None
    value = value if '://' in value else 'http://' + value
    if urlsplit(value).scheme not in {'http', 'https'}:
        raise PublicQueryError('公开页面当前代理类型不受支持，未尝试绕过代理。')
    return value


def page_url(value):
    url = _verified_public_url(value)
    if not url:
        return None
    try:
        parts = urlsplit(url)
        decoded = url
        for _ in range(3):
            decoded = unquote(decoded)
        if parts.port not in (None, 443) or parts.scheme != 'https' or any(ord(ch) < 32 for ch in decoded):
            return None
        if API_KEY.search(decoded) or CONTACT.search(decoded) or private_identifier(decoded) or re.search(
                r'\brec[A-Za-z0-9]{8,}\b|(?:token|password|secret|api.?key|authorization)\s*[:=]', decoded, re.I):
            return None
        host = parts.hostname.encode('idna').decode('ascii').lower().rstrip('.')
        if '.' not in host or not re.fullmatch(r'[a-z0-9.-]+', host):
            return None
        return urlunsplit(('https', host, parts.path or '/', parts.query, ''))
    except (ValueError, UnicodeError):
        return None


def question_urls(question):
    return {url for match in re.findall(r'https://[^\s<>\[\]"\'()，。；！？]+', question)
            if (url := page_url(match.rstrip('。，；！？,;!?')))}


def _safe_address(values):
    resolved = []
    for value in values:
        if '%' in value:
            raise PublicQueryError('公开页面地址不能包含网络接口标识。')
        ip = ipaddress.ip_address(value)
        if not ip.is_global or ip.is_multicast or ip.is_reserved or getattr(ip, 'ipv4_mapped', None) \
                or getattr(ip, 'sixtofour', None) or getattr(ip, 'teredo', None):
            raise PublicQueryError('公开页面域名指向非公网地址，未访问。')
        resolved.append(ip)
    if not resolved:
        raise PublicQueryError('公开页面域名未取得有效公网地址。')
    # Literal IP transport prevents a second, potentially different DNS lookup.
    return str(next((ip for ip in resolved if ip.version == 4), resolved[0]))


async def _public_address(host, dns_client):
    addresses = await asyncio.to_thread(socket.getaddrinfo, host, 443,
        type=socket.SOCK_STREAM, proto=socket.IPPROTO_TCP)
    values = [address[4][0] for address in addresses]
    if not values or not all(ipaddress.ip_address(value) in FAKE_DNS for value in values):
        return _safe_address(values)
    # Some local proxies return only benchmark IPs. Resolve this public hostname
    # through fixed DoH endpoints, never send the page path or business context.
    async with dns_client() as client:
        for endpoint in DOH_ENDPOINTS:
            try:
                async with asyncio.timeout(3):
                    async with client.stream('GET', endpoint + '?' + urlencode({'name': host, 'type': 'A'}),
                            headers={'Accept': 'application/dns-json', 'Accept-Encoding': 'identity'}) as response:
                        if response.status_code != 200 or response.headers.get('content-encoding', 'identity') != 'identity':
                            continue
                        data = json.loads(await _read_bounded(response, '公网域名核验'))
            except (httpx.HTTPError, asyncio.TimeoutError):
                continue
            question = data.get('Question') if isinstance(data, dict) else None
            if endpoint == DOH_ENDPOINTS[0] and isinstance(question, dict):
                question = [question]
            if not isinstance(data, dict) or data.get('Status') != 0 or data.get('TC') \
                    or question != [{'name': host + '.', 'type': 1}] or not isinstance(data.get('Answer'), list):
                raise PublicQueryError('公网域名核验内容无效，未访问页面。')
            names, rows = {host + '.'}, data['Answer'][:100]
            for _ in range(16):
                names.update(row['data'].lower() for row in rows if isinstance(row, dict)
                    and row.get('type') == 5 and row.get('name', '').lower() in names and isinstance(row.get('data'), str))
            return _safe_address([row['data'] for row in rows if isinstance(row, dict) and row.get('type') == 1
                and row.get('name', '').lower() in names and isinstance(row.get('data'), str)])
    raise PublicQueryError('公网域名核验暂不可用，未访问页面。')


class _PageText(HTMLParser):
    def __init__(self, url):
        super().__init__(convert_charrefs=True)
        self.url, self.title, self.text, self.links, self.seen_links = url, [], [], [], set()
        self.stack, self.in_title, self.anchor = [], False, None

    @property
    def hidden(self):
        return bool(self.stack and self.stack[-1][1])

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == 'title':
            self.in_title = True
        hidden = self.hidden or tag in {'script', 'style', 'noscript', 'svg', 'nav', 'header', 'footer', 'form'} \
            or 'hidden' in attrs or attrs.get('aria-hidden') == 'true'
        if tag not in VOID_TAGS:
            self.stack.append((tag, hidden))
        if tag == 'a':
            link = page_url(urljoin(self.url, attrs.get('href', '')))
            if link and link != self.url and urlsplit(link).hostname == urlsplit(self.url).hostname:
                self.anchor = {'label': [], 'link': link, 'visible': not hidden}
        if tag in {'p', 'div', 'li', 'pre', 'br', 'h1', 'h2', 'h3', 'h4', 'tr'} and not self.hidden:
            self.text.append('\n')

    def handle_endtag(self, tag):
        if tag == 'title':
            self.in_title = False
        for index in range(len(self.stack) - 1, -1, -1):
            if self.stack[index][0] == tag:
                del self.stack[index:]
                break
        if tag == 'a' and self.anchor:
            label = safe_text(' '.join(' '.join(self.anchor['label']).split()), limit=160)
            if label and self.anchor['link'] not in self.seen_links:
                self.links.append({'label': label, 'link': self.anchor['link'], 'visible': self.anchor['visible']})
                self.seen_links.add(self.anchor['link'])
            self.anchor = None
        if tag in {'p', 'div', 'li', 'pre', 'h1', 'h2', 'h3', 'h4', 'tr'} and not self.hidden:
            self.text.append('\n')

    def handle_data(self, text):
        if self.in_title:
            self.title.append(text)
        elif not self.hidden:
            self.text.append(text)
        if self.anchor:
            self.anchor['label'].append(text)


async def _page_bytes(response):
    chunks, size = [], 0
    async for chunk in response.aiter_bytes(chunk_size=16 * 1024):
        chunks.append(chunk)
        size += len(chunk)
        if size >= MAX_BYTES:
            return b''.join(chunks)[:MAX_BYTES], True
    return b''.join(chunks), False


def _page_contents(text, kind, target, byte_truncated):
    title, links = urlsplit(target).hostname, []
    if kind != 'text/plain':
        parser = _PageText(target)
        parser.feed(text)
        title = ' '.join(' '.join(parser.title).split()) or title
        text = ''.join(parser.text)
        links = [{key: link[key] for key in ('label', 'link')}
                 for link in sorted(parser.links, key=lambda link: not link['visible'])[:MAX_LINKS]]
    text = safe_text('\n'.join(line.strip() for line in text.splitlines() if line.strip()), limit=20000)
    if not text:
        raise PublicQueryError('公开页面没有可读取正文。')
    partial = byte_truncated or len(text) > MAX_TEXT
    return {'ok': True, 'source': 'public_page', 'sourceURL': target, 'queried_at': _now_iso(),
        'title': safe_text(title, limit=200), 'text': text[:MAX_TEXT], 'truncated': partial,
        'note': '已读取的是正文节选，不是全文；未取得的段落不能声称已核验。' if partial else '已读取当前页面正文。',
        'links': links, 'external_untrusted': True}


def _page_cache_seconds(target, headers):
    directives = [part.strip().lower().partition('=') for part in headers.get('cache-control', '').split(',')]
    if urlsplit(target).query or headers.get('set-cookie') or any(name.strip() in {'private', 'no-store', 'no-cache'}
            for name, _, _ in directives) or {'*', 'cookie', 'authorization'} & {
                part.strip().lower() for part in headers.get('vary', '').split(',')}:
        return 0
    ages = [value.strip() for name, _, value in directives if name.strip() == 'max-age']
    value = ages[0] if len(ages) == 1 else ''
    if value.startswith('"') and value.endswith('"'):
        value = value[1:-1]
    age = headers.get('age', '0').strip()
    if not re.fullmatch(r'[0-9]{1,10}', value) or not re.fullmatch(r'[0-9]{1,10}', age):
        return 0
    return max(0, min(60, int(value) - int(age)))


async def read_page(url, question, *, client_factory=None, dns_client=None, timeout=10):
    target = page_url(url)
    if not target or not question.strip() or _forbidden_inputs(question):
        raise PublicQueryError('只能读取公开问题中的安全 HTTPS 页面，不能发送内部业务或私密信息。')
    async with asyncio.timeout(timeout):
        host = urlsplit(target).hostname
        if dns_client is None:
            from .lighthouse_public import PublicSources
            dns_source = PublicSources()
            try:
                address = await _public_address(host, dns_source._client)
            finally:
                await dns_source.close()
        else:
            address = await _public_address(host, dns_client)
        context = await asyncio.to_thread(_verified_tls_context, trust_env=False)
        proxy = None
        if client_factory is None:
            proxy_url = await asyncio.to_thread(_page_proxy, host)
            if proxy_url:
                proxy = httpx.Proxy(proxy_url, ssl_context=context if urlsplit(proxy_url).scheme == 'https' else None)
                context = await asyncio.to_thread(_PinnedHostnameContext, context, address, host)
        factory = client_factory or (lambda: httpx.AsyncClient(verify=context, trust_env=False,
            proxy=proxy, follow_redirects=False, timeout=httpx.Timeout(timeout, connect=5)))
        client = factory()
        try:
            # A fresh one-request client cannot reuse an IP-keyed TLS connection
            # across different hosts. CA loading is still cached in the worker.
            requested_at = asyncio.get_running_loop().time()
            async with client.stream('GET', httpx.URL(target).copy_with(host=address),
                    headers={'Host': host, 'Accept': 'text/html, application/xhtml+xml, text/plain', 'Accept-Encoding': 'identity'},
                    extensions={'sni_hostname': host}) as response:
                if response.status_code != 200:
                    raise PublicQueryError(f'公开页面返回 HTTP {response.status_code}，未读取或跟随跳转。')
                kind = response.headers.get('content-type', '').split(';')[0].strip().lower()
                if kind not in {'text/html', 'application/xhtml+xml', 'text/plain'}:
                    raise PublicQueryError('公开页面不是可读取的 HTML 或文本。')
                if response.headers.get('content-encoding', 'identity').lower() != 'identity':
                    raise PublicQueryError('公开页面忽略了非压缩请求，未读取压缩内容。')
                raw, byte_truncated = await _page_bytes(response)
                text = raw.decode(response.encoding or 'utf-8', errors='replace')
                cache_seconds = _page_cache_seconds(target, response.headers)
        finally:
            await client.aclose()
        value = await asyncio.to_thread(_page_contents, text, kind, target, byte_truncated)
        return {**value, 'cache_seconds': max(0, cache_seconds - math.ceil(asyncio.get_running_loop().time() - requested_at))}
