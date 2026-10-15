"""Public-only policy and streaming proxy. LAN requests never enter this policy."""
import asyncio
import hmac
import time
from urllib.parse import urlsplit

from starlette.requests import Request
from starlette.responses import JSONResponse

MARKER = b'x-clipflow-public-ingress'
HOP = {b'connection', b'keep-alive', b'proxy-authenticate', b'proxy-authorization',
       b'te', b'trailer', b'transfer-encoding', b'upgrade'}
DENIED = ('/api/qt/', '/api/backend/', '/api/auth/guest', '/api/assistant/feishu-event',
          '/api/notice-cards/', '/docs', '/redoc', '/openapi.json')
API_ROOTS = frozenset(('auth assistant admin bootstrap scope-overview records workbench '
    'workbench-actions maintenance-actions maintenance-refresh change-refresh repair-refresh '
    'refresh source-refresh-status history-summary engineer signatures events repair-management '
    'capacity cabinet-power critical-guard drills daily-tasks learning plan-convergence '
    'link-directory pre-shift-checks pre-shift notice-panel notice-alert-tags notice-memory '
    'notice-attachments notice-image-drafts notice-images notice-undo notice-target-candidates '
    'notice-identity ongoing-items polling-sops polling-work-orders change-confirmations '
    'change-target-candidates jobs qt-active-items handover-links handover-links-auth '
    'handover-password-reset generate send-generated message-delivery').split())
ANONYMOUS = {
    ('GET', '/api/auth/status'), ('GET', '/api/auth/login'), ('GET', '/api/auth/feishu/callback'),
    ('GET', '/api/auth/password/people'), ('POST', '/api/auth/password/login'),
    ('POST', '/api/auth/password/reset'), ('GET', '/api/auth/logout'), ('POST', '/api/auth/logout'),
}
# These original endpoints verify their own scoped, expiring link tokens.
SIGNED = {
    ('GET', '/api/signatures/people'),
    ('GET', '/api/signatures/usage-confirm'), ('POST', '/api/signatures/usage-confirm'),
    ('GET', '/api/signatures/temporary/session'), ('POST', '/api/signatures/temporary/save'),
    ('POST', '/api/signatures/save'), ('GET', '/api/polling-work-orders/session'),
    ('POST', '/api/polling-work-orders/photo'), ('POST', '/api/polling-work-orders/confirm'),
    ('POST', '/api/polling-work-orders/rollback'), ('POST', '/api/polling-work-orders/activate'),
    ('POST', '/api/polling-work-orders/release'),
}


async def reject(scope, receive, send, status, message):
    await JSONResponse({'ok': False, 'error': message, **({'auth_required': True} if status == 401 else {})},
                       status_code=status, headers={'Cache-Control': 'no-store'})(scope, receive, send)


def clean_headers(headers):
    nominated = {part.strip().lower() for key, value in headers if key.lower() == b'connection'
                 for part in value.split(b',')}
    return [(key.lower(), value) for key, value in headers if key.lower() not in HOP | nominated
            and not key.lower().startswith((b'x-forwarded-', b'x-clipflow-'))
            and key.lower() not in {b'forwarded', b'x-real-ip'}]


class PublicGuard:
    def __init__(self, app, token, settings, session_reader):
        self.app, self.token, self.settings, self.session_reader = app, token.encode(), settings, session_reader
        self.routes = [(route.path_regex, getattr(route, 'methods', set())) for route in app.routes
                       if hasattr(route, 'path_regex')]
        self.auth_times = []

    async def __call__(self, scope, receive, send):
        if scope['type'] != 'http':
            return await self.app(scope, receive, send)
        markers = [value for key, value in scope['headers'] if key.lower() == MARKER]
        if not markers:
            return await self.app(scope, receive, send)
        if len(markers) != 1 or not hmac.compare_digest(markers[0], self.token) \
                or (scope.get('client') or ('',))[0] not in {'127.0.0.1', '::1'}:
            return await reject(scope, receive, send, 403, '公网入口凭证无效。')
        path, method = scope['path'], scope['method']
        if '%' in path or '\\' in path or '//' in path or any(p in {'.', '..'} for p in path.split('/')) \
                or any(ord(ch) < 32 for ch in path) or path.startswith(DENIED):
            return await reject(scope, receive, send, 403, '该接口不对公网开放。')
        headers = dict(scope['headers'])
        if headers.get(b'host', b'').decode('latin1').lower() != urlsplit(self.settings.origin).netloc:
            return await reject(scope, receive, send, 400, '公网访问地址无效。')
        if path == '/api/health':
            # A public probe must never disclose internal queues, config or local URLs.
            return await JSONResponse({'ok': True, 'service': 'clipflow_backend'},
                                      headers={'Cache-Control': 'no-store'})(scope, receive, send)
        if not any(regex.fullmatch(path) and method in methods for regex, methods in self.routes):
            return await reject(scope, receive, send, 404, '公网入口不存在。')
        if path.startswith('/api/') and path.split('/')[2] not in API_ROOTS:
            return await reject(scope, receive, send, 403, '此模块尚未开放公网访问。')
        if method not in {'GET', 'HEAD', 'OPTIONS'}:
            source = headers.get(b'origin') or headers.get(b'referer') or b''
            try:
                parsed = urlsplit(source.decode('latin1'))
                origin = f'{parsed.scheme}://{parsed.netloc}'.lower()
            except ValueError:
                origin = ''
            if origin != self.settings.origin or headers.get(b'sec-fetch-site') == b'cross-site':
                return await reject(scope, receive, send, 403, '不允许跨来源提交。')
        if path.startswith('/api/auth/') and method == 'POST':
            now = time.monotonic()
            self.auth_times = [value for value in self.auth_times if now - value < 60]
            if len(self.auth_times) >= 30:
                return await reject(scope, receive, send, 429, '公网登录请求较多，请稍后重试。')
            self.auth_times.append(now)
        scope = {**scope, 'client': ('192.0.2.1', 0), 'scheme': 'https',
                 'headers': clean_headers(scope['headers']) + [(b'x-forwarded-proto', b'https')]}
        needs_session = path.startswith('/api/') and (method, path) not in ANONYMOUS
        session = await asyncio.to_thread(self.session_reader, Request(scope)) if needs_session else None
        if isinstance(session, dict) and session.get('role') == 'guest' and path.startswith('/api/') \
                and (method, path) not in ANONYMOUS:
            return await reject(scope, receive, send, 403, '公网入口不开放机柜访客数据。')
        signed_photo = method == 'GET' and path.startswith('/api/polling-work-orders/photos/')
        if path.startswith('/api/') and (method, path) not in ANONYMOUS | SIGNED and not signed_photo and not session:
            return await reject(scope, receive, send, 401, '请先登录灯塔。')

        async def secure_send(message):
            if message['type'] == 'http.response.start':
                safe = []
                for key, value in message.get('headers', []):
                    if key.lower() == b'set-cookie' and b'; secure' not in value.lower():
                        value += b'; Secure'
                    safe.append((key, value))
                message = {**message, 'headers': safe + [(b'x-content-type-options', b'nosniff')]}
            await send(message)
        await self.app(scope, receive, secure_send)


class StreamingIngress:
    def __init__(self, upstream, token, settings, client=None):
        self.upstream, self.token, self.settings = upstream, token, settings
        self.client, self.active, self.uploads = client, 0, 0

    async def __call__(self, scope, receive, send):
        import httpx
        if scope['type'] == 'websocket':
            return await send({'type': 'websocket.close', 'code': 1008})
        if scope['type'] != 'http':
            return
        headers = list(scope['headers'])
        host = [value for key, value in headers if key.lower() == b'host']
        if len(host) != 1 or host[0].decode('latin1').lower() != urlsplit(self.settings.origin).netloc:
            return await reject(scope, receive, send, 400, '请使用配置的 HTTPS 公网域名访问。')
        try:
            length = int(dict(headers).get(b'content-length', b'0'))
            if length < 0 or length > 300 * 1024 ** 2:
                raise ValueError()
        except ValueError:
            return await reject(scope, receive, send, 413, '上传内容超过公网入口限制。')
        upload = length > 1024 ** 2 or b'multipart/' in dict(headers).get(b'content-type', b'')
        if self.active >= self.settings.max_requests or upload and self.uploads >= 2:
            return await reject(scope, receive, send, 503, '公网访问繁忙，请稍后继续原任务。')
        if self.client is None:
            self.client = httpx.AsyncClient(trust_env=False, follow_redirects=False,
                timeout=httpx.Timeout(300, connect=5, pool=5),
                limits=httpx.Limits(max_connections=self.settings.max_requests, max_keepalive_connections=8))
        self.active += 1
        self.uploads += int(upload)
        response, started = None, False

        async def body():
            size = 0
            async for chunk in Request(scope, receive).stream():
                size += len(chunk)
                if size > 300 * 1024 ** 2:
                    raise ValueError('Public upload limit exceeded.')
                yield chunk
        try:
            raw_path = scope.get('raw_path') or scope['path'].encode('utf-8')
            raw_path += b'?' + scope['query_string'] if scope.get('query_string') else b''
            url = httpx.URL(self.upstream).copy_with(raw_path=raw_path)
            forwarded = clean_headers(headers) + [(MARKER, self.token.encode())]
            # Do not use build_request: a shared client's cookie jar must never mix users.
            request = httpx.Request(scope['method'], url, headers=forwarded, content=body())
            response = await self.client.send(request, stream=True)
            await send({'type': 'http.response.start', 'status': response.status_code,
                        'headers': clean_headers(response.headers.raw)})
            started = True
            async for chunk in response.aiter_raw():
                await send({'type': 'http.response.body', 'body': chunk, 'more_body': True})
            await send({'type': 'http.response.body', 'body': b'', 'more_body': False})
        except (httpx.HTTPError, ValueError, OSError):
            if not started:
                await reject(scope, receive, send, 502, '公网连接中断；提交结果可能尚未确认，请查询原任务，勿重复新增。')
            else:
                raise
        finally:
            self.active -= 1
            self.uploads -= int(upload)
            if response:
                await response.aclose()

    async def close(self):
        if self.client:
            await self.client.aclose()
