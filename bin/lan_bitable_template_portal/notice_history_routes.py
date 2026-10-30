"""Authenticated history viewing and record-only corrections for both UIs."""
from __future__ import annotations

import asyncio
import json
import logging
import threading
from urllib.parse import quote, urlsplit

from fastapi import Request
from fastapi.responses import JSONResponse, Response

from openclaw_service.assistant.lighthouse_ai import AssistantError
from .notice_history import NoticeHistory


def install_notice_history_routes(app, controller, runtime):
    manager = None
    lock = threading.Lock()

    def get_manager():
        nonlocal manager
        with lock:
            if manager is None:
                manager = NoticeHistory(controller, runtime)
        return manager

    async def endpoint(request: Request):
        try:
            session = controller._current_session(request)
            if not session:
                return controller._auth_required_response()
            service = await asyncio.to_thread(get_manager)
            payload = {}
            if request.method == 'POST':
                origin = urlsplit(request.headers.get('origin') or request.headers.get('referer') or '')
                if origin.scheme != request.url.scheme or origin.netloc != request.url.netloc:
                    raise AssistantError('请求来源无效。', 403)
                body = bytearray()
                async for chunk in request.stream():
                    body.extend(chunk)
                    if len(body) > 256 * 1024:
                        raise AssistantError('请求内容过大。', 413)
                try:
                    payload = json.loads(body or b'{}')
                except (ValueError, UnicodeDecodeError):
                    raise AssistantError('请求格式无效。') from None
                if not isinstance(payload, dict):
                    raise AssistantError('请求格式无效。')
            path, query = request.url.path, request.query_params
            if path.endswith('/attachment'):
                content, name = await asyncio.to_thread(service.attachment, session,
                    **{key: query.get(key, '') for key in ('work_type', 'record_id', 'group', 'field_id', 'file_token')})
                fresh = controller._current_session(request)
                if not fresh or (fresh.get('user') or {}).get('open_id') != (session.get('user') or {}).get('open_id'):
                    return controller._auth_required_response()
                await asyncio.to_thread(service._user, fresh, query.get('work_type', ''))
                return Response(content, media_type='application/octet-stream', headers={
                    'Content-Disposition': "attachment; filename*=UTF-8''" + quote(name, safe=''),
                    'Cache-Control': 'private, no-store', 'X-Content-Type-Options': 'nosniff'})
            if path.endswith('/detail'):
                data = await asyncio.to_thread(service.detail, session,
                    **{key: query.get(key, '') for key in ('work_type', 'record_id', 'scope')})
            elif path.endswith('/save'):
                data = await asyncio.to_thread(service.save, session, payload)
            elif path.endswith('/retry'):
                data = await asyncio.to_thread(service.retry, session, request.path_params['operation_id'])
            elif path.endswith('/abandon'):
                data = await asyncio.to_thread(service.abandon, session, request.path_params['operation_id'])
            else:
                try:
                    page, page_size = int(query.get('page', 1)), int(query.get('page_size', 20))
                except ValueError:
                    raise AssistantError('分页参数无效。') from None
                data = await asyncio.to_thread(service.list, session, month=query.get('month', ''),
                    scope=query.get('scope', 'ALL'), work_type=query.get('work_type', 'all'),
                    q=query.get('q', '')[:200], page=page, page_size=page_size)
            return JSONResponse({'ok': True, 'data': data}, headers={'Cache-Control': 'private, no-store'})
        except AssistantError as exc:
            return JSONResponse({'ok': False, 'error': str(exc)}, status_code=exc.status,
                                headers={'Cache-Control': 'private, no-store'})
        except Exception as exc:
            from .portal_service import PortalError
            if isinstance(exc, PortalError):
                return controller._portal_error_response(exc, default_status=403)
            logging.getLogger(__name__).exception('Notice history request failed')
            return JSONResponse({'ok': False, 'error': '历史通告处理未完成，请重试原操作。'}, status_code=503)

    for path in ('', '/detail', '/attachment'):
        app.add_api_route('/api/notice-history' + path, endpoint, methods=['GET'])
    for path in ('/save', '/operations/{operation_id}/retry', '/operations/{operation_id}/abandon'):
        app.add_api_route('/api/notice-history' + path, endpoint, methods=['POST'])
