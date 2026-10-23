"""Document-library routes; the host supplies the existing session/CSRF authority."""
import asyncio
import json
import logging
from pathlib import Path

from fastapi import Request
from fastapi.responses import FileResponse, JSONResponse
from starlette.formparsers import MultiPartException, MultiPartParser
from starlette.requests import ClientDisconnect

from openclaw_service.assistant.lighthouse_ai import AssistantError
from .document_library import MAX_FILE, _member

PREFIX = '/api/document-library'
_UPLOADS = asyncio.Semaphore(2)
_INITIAL_WAIT_SECONDS = 20


def install_document_library_routes(app, authorize, get_service):
    """authorize(request) must validate the session, knowledge grant and write CSRF."""
    scans = {}
    scan_slot = asyncio.Semaphore(2)
    closing = False

    def start_scan(service, key):
        if closing:
            raise AssistantError('资料同步正在关闭，请稍后重新读取。', 503)
        task = scans.get(key)
        if task is None or task.done():
            async def advance():
                while not closing:
                    async with scan_slot:
                        worked = await asyncio.to_thread(service.prepare_pending, table_key=key, limit=1)
                    if not worked:
                        break
                    await asyncio.sleep(0)
            task = scans[key] = asyncio.create_task(advance())
            def finished(done):
                if scans.get(key) is done:
                    scans.pop(key, None)
                if not done.cancelled() and done.exception() is not None:
                    logging.getLogger(__name__).warning('Document library scan unavailable: type=%s', type(done.exception()).__name__)
            task.add_done_callback(finished)
        return task

    async def close_scans():
        nonlocal closing
        closing = True
        # Do not cancel a running thread while it holds a durable scan lease.
        await asyncio.gather(*scans.values(), return_exceptions=True)

    app.add_event_handler('shutdown', close_scans)

    async def initial_records(request, service, actor, key):
        query = dict(request.query_params)
        deadline = asyncio.get_running_loop().time() + _INITIAL_WAIT_SECONDS
        task = None
        while True:
            try:
                return await asyncio.to_thread(service.list_records, actor, key, query)
            except AssistantError as exc:
                if exc.category != 'document_library_sync_pending':
                    raise
                # Even an expired first read must wake the durable job before returning pending.
                if task is not None and asyncio.get_running_loop().time() >= deadline:
                    raise
            if await request.is_disconnected():
                raise AssistantError('资料读取已取消，后台同步仍会继续。', 503, category='document_library_read_cancelled')
            if task is None or task.done():
                if task is not None and not task.cancelled() and task.exception() is not None:
                    raise AssistantError('资料同步队列暂时不可用，请重试；持续失败时请联系管理员检查运行目录。', 503,
                        category='document_library_sync_failed')
                await asyncio.to_thread(service.ensure_initial_sync, actor, key)
                task = start_scan(service, key)
            await asyncio.wait({task}, timeout=min(1, max(0, deadline - asyncio.get_running_loop().time())))
            if task.done() and not task.cancelled() and task.exception() is None:
                # Another process may hold the lease. This bounded read waits, not a new scheduler.
                try:
                    return await asyncio.to_thread(service.list_records, actor, key, query)
                except AssistantError as exc:
                    if exc.category != 'document_library_sync_pending':
                        raise
                await asyncio.sleep(min(1, max(0, deadline - asyncio.get_running_loop().time())))

    async def endpoint(request: Request):
        try:
            actor = await authorize(request)
            _member(actor)
            service = await asyncio.to_thread(get_service)
            key = request.path_params.get('table_key')
            identity = request.path_params.get('record_id')
            action = request.scope['route'].path.removeprefix(PREFIX).strip('/')
            method = request.method
            payload = {}
            if method not in {'GET', 'HEAD'} and not action.endswith('/attachments'):
                body = bytearray()
                async for part in request.stream():
                    body.extend(part)
                    if len(body) > 256 * 1024:
                        raise AssistantError('请求内容超过大小限制。', 413)
                payload = json.loads(body or b'{}')
                if not isinstance(payload, dict):
                    raise AssistantError('请求内容必须是对象。')
            if action == 'preview':
                data = await asyncio.to_thread(service.preview_table, actor, payload)
            elif action == 'storage':
                data = await asyncio.to_thread(service.storage, actor, payload if method == 'POST' else None)
            elif action == 'tables':
                data = await asyncio.to_thread(service.tables, actor) if method == 'GET' else await asyncio.to_thread(service.save_table, actor, payload)
            elif action.endswith('/schema'):
                data = await asyncio.to_thread(service.schema, actor, key, refresh=request.query_params.get('refresh') == '1')
            elif action.endswith('/file'):
                attachment = request.path_params['attachment_id']
                version = request.query_params.get('version')
                await asyncio.to_thread(service.download, actor, key, identity, attachment, version=version)
                fresh = await authorize(request)
                _same_actor(actor, fresh)
                path, name = await asyncio.to_thread(service.download, fresh, key, identity, attachment, version=version, refresh=False)
                media_type = {'.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg',
                    '.gif': 'image/gif', '.webp': 'image/webp', '.pdf': 'application/pdf'}.get(Path(name).suffix.lower(), 'application/octet-stream')
                inline = request.query_params.get('preview') == '1' and media_type != 'application/octet-stream'
                return FileResponse(path, filename=name, media_type=media_type, content_disposition_type='inline' if inline else 'attachment', headers={
                    'Cache-Control': 'private, no-store', 'X-Content-Type-Options': 'nosniff',
                    'Content-Security-Policy': "sandbox; default-src 'none'; frame-ancestors 'self'"})
            elif action.endswith('/operations'):
                data = await asyncio.to_thread(service.pending_operations, actor, key)
            elif action.endswith('/verify'):
                data = await asyncio.to_thread(service.verify_operation, actor, key, request.path_params['operation_id'], payload)
            elif action.endswith('/versions'):
                data = await asyncio.to_thread(service.versions, actor, key, identity)
            elif action.endswith('/attachments'):
                if _UPLOADS._value < 1:
                    raise AssistantError('正在上传的文件过多，请稍后重试。', 503)
                await _UPLOADS.acquire()
                form = None
                parser = None
                try:
                    size = 0
                    async def bounded():
                        nonlocal size
                        try:
                            async for part in request.stream():
                                size += len(part)
                                if size > MAX_FILE + 64 * 1024:
                                    raise MultiPartException('上传内容超过大小限制。')
                                yield part
                        except ClientDisconnect:
                            raise MultiPartException('上传连接已断开。') from None
                    parser = MultiPartParser(request.headers, bounded(), max_files=1, max_fields=5)
                    form = await parser.parse()
                    keys = {'file', 'field_id', 'expected_revision', 'operation_id', 'reason', 'replace_attachment_id'}
                    if set(form) - keys or any(len(form.getlist(k)) != 1 for k in form):
                        raise AssistantError('上传字段无效。')
                    file = form.get('file')
                    if not file or not hasattr(file, 'file'):
                        raise AssistantError('请选择一个文件上传。')
                    fields = {k: form[k] for k in form if k != 'file'}
                    data = await asyncio.to_thread(service.upload_attachment, actor, key, identity, fields, file.filename, file.file)
                except MultiPartException:
                    raise AssistantError('上传内容无效或超过大小限制。', 413) from None
                finally:
                    if form is not None:
                        await form.close()
                    elif parser is not None:
                        for handle in parser._files_to_close_on_error:
                            handle.close()
                    _UPLOADS.release()
            elif '/sync/' in action:
                data = await asyncio.to_thread(service.sync_status, actor, key, request.path_params['job_id'])
            elif action.endswith('/sync'):
                data = await asyncio.to_thread(service.request_sync, actor, key)
            elif action.endswith('/reindex'):
                data = await asyncio.to_thread(service.reindex, actor, key, payload)
            elif method == 'DELETE':
                data = await asyncio.to_thread(service.remove_table, actor, key, payload)
            elif identity:
                data = await asyncio.to_thread(service.cached_record, actor, key, identity,
                    refresh=request.query_params.get('refresh') == '1') if method == 'GET' else await asyncio.to_thread(service.save_record, actor, key, payload, identity=identity)
            elif action.endswith('/records'):
                data = await initial_records(request, service, actor, key) if method == 'GET' else await asyncio.to_thread(service.save_record, actor, key, payload)
            else:
                data = await asyncio.to_thread(service.save_table, actor, payload, key=key)
            fresh = await authorize(request)
            _same_actor(actor, fresh)
            if action == 'storage' and (not fresh.get('is_admin') or fresh.get('read_only')):
                raise AssistantError('缓存管理权限已变化。', 403)
            await asyncio.to_thread(service.check_response, fresh, key, data,
                configuration=method in {'POST', 'PUT'} and action in {'tables', 'tables/{table_key}'},
                preview=action == 'preview', removal=method == 'DELETE')
            if method in {'POST', 'PUT'} and action in {'tables', 'tables/{table_key}'} and data.get('enabled'):
                start_scan(service, data['id'])
            elif action.endswith('/sync') or action.endswith('/reindex'):
                start_scan(service, key)
            elif key and method == 'GET':
                await asyncio.to_thread(service.refresh_cache_if_due, fresh, key)
                start_scan(service, key)
            return JSONResponse({'ok': True, 'data': data}, headers={'Cache-Control': 'no-store'})
        except AssistantError as exc:
            return JSONResponse({'ok': False, 'error': str(exc), 'code': exc.category or 'document_library_error'}, status_code=exc.status, headers={'Cache-Control': 'no-store'})
        except (ValueError, TypeError, KeyError):
            return JSONResponse({'ok': False, 'error': '资料库请求参数无效。'}, status_code=400, headers={'Cache-Control': 'no-store'})
        except Exception:
            return JSONResponse({'ok': False, 'error': '资料库暂时不可用，系统不会自动重复提交。'}, status_code=503, headers={'Cache-Control': 'no-store'})

    routes = (
        ('/preview', ['POST']),
        ('/storage', ['GET', 'POST']),
        ('/tables', ['GET', 'POST']), ('/tables/{table_key}', ['PUT', 'DELETE']),
        ('/tables/{table_key}/operations', ['GET']),
        ('/tables/{table_key}/operations/{operation_id}/verify', ['POST']),
        ('/tables/{table_key}/reindex', ['POST']),
        ('/tables/{table_key}/schema', ['GET']), ('/tables/{table_key}/records', ['GET', 'POST']),
        ('/tables/{table_key}/records/{record_id}', ['GET', 'PATCH']),
        ('/tables/{table_key}/records/{record_id}/attachments', ['POST']),
        ('/tables/{table_key}/records/{record_id}/versions', ['GET']),
        ('/tables/{table_key}/records/{record_id}/attachments/{attachment_id}/file', ['GET']),
        ('/tables/{table_key}/sync', ['POST']), ('/tables/{table_key}/sync/{job_id}', ['GET']),
    )
    for path, methods in routes:
        app.add_api_route(PREFIX + path, endpoint, methods=methods)


def _same_actor(before, after):
    _member(after)
    if (before.get('id') != after.get('id') or set(before.get('scopes', [])) - set(after.get('scopes', []))
            or set(before.get('modules', [])) - set(after.get('modules', []))
            or before.get('is_admin') and not after.get('is_admin')):
        raise AssistantError('请求过程中访问权限已变化，请重新读取。', 403)
