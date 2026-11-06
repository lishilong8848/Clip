"""Public QR endpoints with their own signature check, hosted on the portal's port."""
import asyncio
from contextlib import asynccontextmanager
import hashlib
import hmac
import json
import time
from collections import deque
from pathlib import Path
from urllib.parse import urlsplit

import httpx
from fastapi import Request
from fastapi.responses import HTMLResponse, JSONResponse

from .receiver import Settings, create_app as create_receiver

DATA_DIR = Path(__file__).resolve().parents[1] / 'data' / 'alarm_scan'
PUBLIC_PAGE = 'https://lishilong8848.github.io/NanTongJiFangDianChi/copy.html'
LEGACY_PAGE = 'https://lsldiyige-sys.github.io/ntsjhl/copy.html'
MAX_REQUEST_BYTES = 96 * 1024


def install_alarm_scan_routes(app, config_path=None, transport=None):
    config_path = Path(config_path) if config_path is not None else DATA_DIR / 'config.json'
    state = {'ready': False}
    attempts = deque()
    app.state.alarm_scan = state
    original_lifespan = app.router.lifespan_context

    @asynccontextmanager
    async def receiver_lifespan():
        try:
            config = json.loads(config_path.read_text(encoding='utf-8-sig'))
            if not isinstance(config, dict) or config.get('enabled') is not True:
                config = {}
            required = ('FEISHU_APP_ID', 'FEISHU_APP_SECRET', 'FEISHU_CHAT_ID',
                        'ALARM_RECEIVER_TOKEN', 'SCAN_RELAY_SIGNING_KEY')
            if not all(isinstance(config.get(key), str) and config[key].strip() for key in required):
                config = None
        except (OSError, ValueError):
            config = None
        if config is None:
            yield
            return
        settings = Settings(
            app_id=config['FEISHU_APP_ID'], app_secret=config['FEISHU_APP_SECRET'],
            chat_id=config['FEISHU_CHAT_ID'], token=config['ALARM_RECEIVER_TOKEN'],
            db_path=str(config_path.parent / 'delivered.sqlite3'), dedup_mode='scan',
        )
        receiver = create_receiver(settings=settings, transport=transport)
        async with receiver.router.lifespan_context(receiver):
            if not receiver.state.receiver.get('ready'):
                yield
                return
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=receiver),
                                         base_url='http://internal.receiver', trust_env=False) as client:
                state.update(ready=True, signing_key=config['SCAN_RELAY_SIGNING_KEY'],
                             token=settings.token, client=client, semaphore=asyncio.Semaphore(8))
                try:
                    yield
                finally:
                    state.clear()
                    state['ready'] = False

    @asynccontextmanager
    async def lifespan(application):
        async with original_lifespan(application), receiver_lifespan():
            yield

    app.router.lifespan_context = lifespan

    def reply(data, status=200):
        return JSONResponse(data, status_code=status,
                            headers={'Cache-Control': 'no-store', 'Referrer-Policy': 'no-referrer'})

    @app.get('/alarm-scan/scan')
    async def scan_page(request: Request):
        if request.query_params.get('target', PUBLIC_PAGE) not in {PUBLIC_PAGE, LEGACY_PAGE}:
            return reply({'error': '钉钉网页地址不受支持'}, 400)
        return HTMLResponse(Path(__file__).with_name('scan.html').read_text(encoding='utf-8'),
                            headers={'Cache-Control': 'no-store', 'Referrer-Policy': 'no-referrer'})

    @app.get('/api/alarm-scan/health')
    async def health():
        return reply({'ok': state['ready'], 'service': 'alarm_scan',
                      'feishu_configured': state['ready'], 'dingtalk_managed': False},
                     200 if state['ready'] else 503)

    @app.post('/api/alarm-scan/events')
    async def receive(request: Request):
        if not state['ready']:
            return reply({'error': '告警扫码模块尚未配置'}, 503)
        now = time.monotonic()
        while attempts and now - attempts[0] >= 60:
            attempts.popleft()
        if len(attempts) >= 60:
            return reply({'error': '扫码请求较多，请一分钟后重试'}, 429)
        attempts.append(now)
        source = urlsplit(request.headers.get('origin') or request.headers.get('referer') or '')
        if (source.scheme, source.netloc) != (request.url.scheme, request.url.netloc) or request.headers.get('sec-fetch-site') == 'cross-site':
            return reply({'error': '不允许跨来源提交'}, 403)
        raw = bytearray()
        async for part in request.stream():
            raw.extend(part)
            if len(raw) > MAX_REQUEST_BYTES:
                return reply({'error': '告警内容过长'}, 413)
        try:
            payload = json.loads(raw)
            if not isinstance(payload, dict):
                raise ValueError()
            building, content = payload.get('louhao', ''), payload.get('content')
            stamp, signature = payload.get('ts'), payload.get('signature')
            if not isinstance(building, str) or len(building) > 64:
                raise ValueError()
            if not isinstance(content, str) or not content.strip() or len(content) > 12000:
                raise ValueError()
            if type(stamp) is not int or stamp <= 0:
                raise ValueError()
            if not isinstance(signature, str) or len(signature) != 64:
                return reply({'error': '二维码签名无效，请重新生成'}, 403)
            signed = json.dumps([building, content], ensure_ascii=False, separators=(',', ':')).encode('utf-8')
            expected = hmac.new(state['signing_key'].encode('utf-8'), signed, hashlib.sha256).hexdigest()
            if not hmac.compare_digest(expected.encode('ascii'), signature.encode('utf-8')):
                return reply({'error': '二维码签名无效，请重新生成'}, 403)
        except (ValueError, TypeError, UnicodeError):
            return reply({'error': '告警数据格式错误'}, 400)
        if state['semaphore'].locked():
            return reply({'error': '正在处理其他扫码，请稍后重试'}, 429)
        async with state['semaphore']:
            try:
                response = await state['client'].post('/api/alarms',
                    json={'louhao': building, 'content': content, 'ts': stamp},
                    headers={'Authorization': 'Bearer ' + state['token']})
                result = response.json()
                if response.status_code == 200 and result.get('status') in {'sent', 'duplicate'}:
                    return reply({'status': 'accepted', 'feishu': result, 'dingtalk_managed': False})
                if result.get('code') == 'delivery_uncertain':
                    return reply({'status': 'uncertain', 'error': '上次发送结果待核对，未重复发送。', 'dingtalk_managed': False}, 409)
            except (httpx.HTTPError, ValueError):
                pass
        return reply({'status': 'failed', 'error': '飞书发送未确认，请核对原消息后重试。', 'dingtalk_managed': False}, 503)
