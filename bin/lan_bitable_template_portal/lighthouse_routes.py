"""Authenticated bridge to the assistant worker owned by this portal."""
from __future__ import annotations

import asyncio
import hmac
import json
import logging
import re
from pathlib import Path
from urllib.parse import urlsplit

from fastapi import Request
from fastapi.responses import JSONResponse, StreamingResponse

from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.protocol import ServiceError
from .lighthouse_bridge import PortalAuthority, actor_for


def install_lighthouse_routes(app, controller, runtime):
    from .lighthouse_icon_weather import install_icon_weather
    install_icon_weather(app, controller, runtime)
    client, authority = None, None
    lock = asyncio.Lock()
    preparation = None
    diagnostic_reporter = None
    closing = False

    async def ready():
        nonlocal client, authority
        async with lock:
            if closing:
                raise AssistantError('助手连接正在关闭。', 503)
            if client is None:
                from openclaw_service.client import ResidentRuntime
                root = Path(runtime.state_store.db_path).parent / 'lighthouse_openclaw'
                client = await asyncio.to_thread(ResidentRuntime, root, callback_url=lambda:
                    'http://127.0.0.1:' + str(controller.bound_port or controller.preferred_port) + '/api/assistant/service-bridge',
                    legacy_db=runtime.state_store.db_path, assistant_backend=True)
                authority = PortalAuthority(app, controller, runtime, client)
        await client.prepare()
        return client, authority

    async def startup():
        nonlocal preparation, diagnostic_reporter
        from openclaw_service.diagnostics import configure, DiagnosticReporter
        from openclaw_service.protocol import PROJECT
        try:
            diagnostic_store = await asyncio.to_thread(configure,
                Path(runtime.state_store.db_path).parent / 'lighthouse_openclaw', PROJECT)
            diagnostic_reporter = DiagnosticReporter(diagnostic_store)
            diagnostic_reporter.start()
        except Exception:
            logging.getLogger(__name__).warning('Diagnostic local storage unavailable')
        async def connect():
            from openclaw_service.assistant.lighthouse_startup_log import emit
            emit('service_connecting')
            try:
                current, _ = await ready()
                emit('service_ready', pid=current.descriptor['pid'], port=current.descriptor['port'])
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                emit('service_failed', error=type(exc).__name__)
                logging.getLogger(__name__).warning('Assistant service connection not ready: type=%s', type(exc).__name__)
        preparation = asyncio.create_task(connect())
        scheduler.start()
        public_sender.start()
        await feishu_channel.start()

    async def shutdown():
        nonlocal closing
        closing = True
        await scheduler.close()
        await public_sender.close()
        await feishu_channel.close()
        if diagnostic_reporter is not None:
            await diagnostic_reporter.close()
        if preparation:
            preparation.cancel()
            await asyncio.gather(preparation, return_exceptions=True)
        if authority is not None:
            await authority.close()
        if client is not None:
            await client.close()

    from .feishu_assistant import install_feishu_assistant
    feishu_channel = install_feishu_assistant(app, controller, runtime, ready)
    runtime.feishu_assistant_channel = feishu_channel
    from .assistant_schedule_runtime import AssistantScheduler
    scheduler = AssistantScheduler(controller, runtime, ready)
    runtime.assistant_scheduler = scheduler
    from .feishu_delivery import PublicFeishuSender
    public_sender = PublicFeishuSender(runtime)
    from .feishu_workpartner_routes import install_feishu_workpartner_routes
    install_feishu_workpartner_routes(app, controller, runtime, feishu_channel)
    app.add_event_handler('startup', startup)
    app.add_event_handler('shutdown', shutdown)

    async def recommend_notice_tags(owner, notices):
        current, _ = await ready()
        http = await current.http_client()
        # A background model timeout must not mark the interactive connection disconnected.
        response = await http.post(f"http://127.0.0.1:{current.descriptor['port']}/recommend-notice-tags",
            headers={'Authorization': 'Bearer ' + current.key}, timeout=80,
            json={'instance': current.instance, 'lease': current.lease, 'owner': owner, 'notices': notices})
        response.raise_for_status()
        result = response.json()
        if not result.get('ok'):
            raise AssistantError('后台推荐标签未完成。', 503)
        return result['data']

    async def generate_sharing_questions(owner, source, previous, count, question_types=None,
                                         images=None, source_images=None, mode=None, repair=None):
        payload = {'owner': owner, 'source': source, 'previous': previous, 'count': count,
                   'question_types': question_types}
        payload.update({key: value for key, value in (('images', images), ('source_images', source_images),
                                                     ('mode', mode), ('repair', repair))
                        if value is not None})
        # Reserve space for the instance and lease added to the 8MiB IPC body.
        if mode == 'image_sources' and len(json.dumps(payload, ensure_ascii=False).encode('utf-8')) > 8 * 1024 * 1024 - 4096:
            raise AssistantError('图片识别请求须小于助手通道8MiB上限，请缩小JPEG或减少本批图片。', 413)
        current, _ = await ready()
        http = await current.http_client()
        response = await http.post(f"http://127.0.0.1:{current.descriptor['port']}/generate-sharing-questions",
            headers={'Authorization': 'Bearer ' + current.key}, timeout=100,
            json={'instance': current.instance, 'lease': current.lease, **payload})
        result = response.json()
        if not result.get('ok'):
            raise AssistantError(str(result.get('error') or '分享会出题未完成，请核对本人模型配置及纪要内容后重试。')[:500], 503)
        return result['data'] if mode in {'image_sources', 'question_import'} else result['data']['questions']

    runtime.generate_sharing_questions = generate_sharing_questions

    def failure(exc):
        category = getattr(exc, 'category', '')
        details = {'category': category} if isinstance(category, str) and re.fullmatch(r'[a-z][a-z_]{0,63}', category) else {}
        return JSONResponse({'ok': False, 'error': str(exc), **details}, status_code=exc.status,
            headers={'Cache-Control': 'no-store'})

    async def business_bridge(request: Request):
        try:
            if not request.client or request.client.host != '127.0.0.1' or request.headers.get('origin'):
                raise AssistantError('不允许访问内部业务通道。', 403)
            header = request.headers.get('authorization', '')
            token = header[7:] if header.startswith('Bearer ') else ''
            if authority is None or client is None or not client.key or not token.isascii() or not hmac.compare_digest(token, client.key):
                raise AssistantError('内部业务通道认证未通过。', 403)
            body = bytearray()
            async for chunk in request.stream():
                if len(body) + len(chunk) > 8 * 1024 * 1024:
                    raise AssistantError('业务请求过大。', 413)
                body.extend(chunk)
            try:
                message = json.loads(body)
            except (ValueError, UnicodeError, RecursionError):
                raise AssistantError('业务请求格式无效。') from None
            result = await authority.dispatch(message)
            return JSONResponse({'ok': True, 'data': result}, headers={'Cache-Control': 'no-store'})
        except AssistantError as exc:
            return failure(exc)
        except Exception:
            return failure(AssistantError('业务桥接未完成，未自动重发。', 503))

    app.add_api_route('/api/assistant/service-bridge', business_bridge, methods=['POST'], name='lighthouse_service_bridge')

    async def endpoint(request: Request):
        import httpx
        upstream = connection = None
        try:
            actor = await actor_for(controller, runtime, request)
            if request.method != 'GET':
                source = request.headers.get('origin') or request.headers.get('referer')
                expected = urlsplit(controller._request_base_url(request))
                actual = urlsplit(source) if source else None
                if not source or request.headers.get('sec-fetch-site', '').lower() == 'cross-site' or (actual.scheme, actual.netloc) != (expected.scheme, expected.netloc):
                    raise AssistantError('不允许跨来源提交。', 403)
            current, gateway = await ready()
            action = request.url.path.removeprefix('/api/assistant/')
            parts = action.split('/')
            # Detail refresh may continue remaining steps of a previously approved
            # submitted plan. The service verifies owner, scopes and stored status;
            # reading an unconfirmed plan never schedules its execution.
            plan_id = None
            if parts[0] == 'plans' and (
                    len(parts) == 3 and parts[2] in {'confirm', 'retry'} and request.method == 'POST'
                    or len(parts) == 2 and request.method == 'GET'):
                plan_id = parts[1]
            context_id = gateway.context(request, actor, plan_id=plan_id)
            headers = {'Authorization': 'Bearer ' + current.key, 'x-clipflow-instance': current.instance,
                'x-clipflow-lease': current.lease, 'x-clipflow-context': context_id}
            for name in ('content-type', 'range'):
                if request.headers.get(name):
                    headers[name] = request.headers[name]
            # knowledge/files streams large multipart batches (300MiB + framing overhead); ordinary
            # assistant attachments keep their original 100MiB ceiling with the same auth.
            limit = 300 * 1024 * 1024 + 65536 if action == 'knowledge/files' else 100 * 1024 * 1024 + 65536 if action == 'files' else 10 * 1024 * 1024 + 65536 if action == 'skills/install' else 4 * 1024 * 1024 if action.startswith('plans/') else 512 * 1024 if action == 'workspace' else 128000 if action in {'messages', 'agent', 'chat'} else 16000
            async def content():
                size = 0
                async for chunk in request.stream():
                    size += len(chunk)
                    if size > limit:
                        raise AssistantError('提交内容过大。', 413)
                    yield chunk
            connection = await current.http_client()
            url = f"http://127.0.0.1:{current.descriptor['port']}" + request.url.path
            call = connection.build_request(request.method, url, params=list(request.query_params.multi_items()),
                headers=headers, content=content() if request.method != 'GET' else None,
                timeout=httpx.Timeout(connect=3, read=None if action.endswith('/stream') else 150, write=150, pool=3))
            upstream = await connection.send(call, stream=True)
            response_headers = {key: value for key, value in upstream.headers.items() if key.lower() in {
                'content-type', 'cache-control', 'content-disposition', 'x-content-type-options',
                'x-vercel-ai-ui-message-stream', 'x-accel-buffering', 'content-range', 'accept-ranges'}}
            reply = upstream
            async def output():
                try:
                    async for chunk in reply.aiter_bytes():
                        yield chunk
                finally:
                    await reply.aclose()
            response = StreamingResponse(output(), status_code=reply.status_code, headers=response_headers)
            upstream = connection = None
            return response
        except (AssistantError, ServiceError) as exc:
            return failure(exc)
        except httpx.HTTPError:
            return failure(AssistantError('助手连接暂时中断，原消息已保留；其他业务不受影响。', 503))
        except Exception:
            return failure(AssistantError('助手暂时不可用，原消息已保留；其他业务可继续使用。', 503))
        finally:
            if upstream is not None:
                await upstream.aclose()

    paths = (('appearance', ['GET', 'PUT']), ('conversation', ['GET', 'DELETE', 'PATCH']), ('conversation/stamp', ['GET']),
        ('chat', ['POST']), ('settings', ['GET', 'PUT']), ('question-bank', ['GET']),
        ('question-material', ['GET']), ('work-orders', ['GET']), ('messages', ['POST']),
        ('history', ['GET']), ('runs/{run_id}/stream', ['GET']), ('runs/{run_id}/cancel', ['POST']),
        ('agent', ['POST']), ('pending', ['GET']), ('capabilities', ['GET']), ('files', ['POST']),
        ('files/{file_id}', ['GET']), ('plans/{plan_id}', ['GET', 'PATCH']), ('plans/{plan_id}/confirm', ['POST']),
        ('plans/{plan_id}/cancel', ['POST']), ('plans/{plan_id}/retry', ['POST']), ('plans/{plan_id}/options', ['GET']),
        ('plans/{plan_id}/cabinet-text-preview', ['POST']), ('plans/{plan_id}/repair-prefill', ['POST']),
        ('plans/{plan_id}/notice-prefill', ['POST']), ('commands', ['GET']), ('skills', ['GET']),
        ('skills/install', ['POST']), ('skills/{skill_name}', ['GET', 'DELETE']))
    paths += (('extensions', ['GET', 'PUT']), ('extensions/{server_id}/test', ['POST']),
              ('runs/{run_id}/resume', ['POST']), ('workspace', ['GET', 'POST']), ('voice', ['GET', 'POST', 'PUT']),
              ('intelligence-capabilities', ['GET']))
    from openclaw_service.assistant.lighthouse_knowledge_routes import KNOWLEDGE_ROUTES
    for path, methods in (*paths, *KNOWLEDGE_ROUTES):
        app.add_api_route('/api/assistant/' + path, endpoint, methods=methods, name='lighthouse_' + path.replace('/', '_'))
    return recommend_notice_tags
