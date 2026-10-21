"""Document-library routes; the host supplies the existing session/CSRF authority."""
import asyncio
import json
from pathlib import Path

from fastapi import Request
from fastapi.responses import FileResponse, JSONResponse
from starlette.formparsers import MultiPartException, MultiPartParser
from starlette.requests import ClientDisconnect

from openclaw_service.assistant.lighthouse_ai import AssistantError
from .document_library import MAX_FILE, _member

PREFIX = '/api/document-library'
_UPLOADS = asyncio.Semaphore(2)


def install_document_library_routes(app, authorize, get_service):
    """authorize(request) must validate the session, knowledge grant and write CSRF."""
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
            elif action == 'tables':
                data = await asyncio.to_thread(service.tables, actor) if method == 'GET' else await asyncio.to_thread(service.save_table, actor, payload)
            elif action.endswith('/schema'):
                data = await asyncio.to_thread(service.schema, actor, key)
            elif action.endswith('/file'):
                attachment = request.path_params['attachment_id']
                version = request.query_params.get('version')
                await asyncio.to_thread(service.download, actor, key, identity, attachment, version=version)
                fresh = await authorize(request)
                _same_actor(actor, fresh)
                path, name = await asyncio.to_thread(service.download, fresh, key, identity, attachment, version=version, refresh=False)
                media_type = {'.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg',
                    '.gif': 'image/gif', '.webp': 'image/webp'}.get(Path(name).suffix.lower(), 'application/octet-stream')
                return FileResponse(path, filename=name, media_type=media_type, content_disposition_type='attachment', headers={
                    'Cache-Control': 'private, no-store', 'X-Content-Type-Options': 'nosniff'})
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
            elif identity:
                data = await asyncio.to_thread(service.record, actor, key, identity) if method == 'GET' else await asyncio.to_thread(service.save_record, actor, key, payload, identity=identity)
            elif action.endswith('/records'):
                data = await asyncio.to_thread(service.list_records, actor, key, dict(request.query_params)) if method == 'GET' else await asyncio.to_thread(service.save_record, actor, key, payload)
            else:
                data = await asyncio.to_thread(service.save_table, actor, payload, key=key)
            fresh = await authorize(request)
            _same_actor(actor, fresh)
            await asyncio.to_thread(service.check_response, fresh, key, data,
                configuration=method in {'POST', 'PUT'} and action in {'tables', 'tables/{table_key}'}, preview=action == 'preview')
            return JSONResponse({'ok': True, 'data': data}, headers={'Cache-Control': 'no-store'})
        except AssistantError as exc:
            return JSONResponse({'ok': False, 'error': str(exc), 'code': exc.category or 'document_library_error'}, status_code=exc.status, headers={'Cache-Control': 'no-store'})
        except (ValueError, TypeError, KeyError):
            return JSONResponse({'ok': False, 'error': '资料库请求参数无效。'}, status_code=400, headers={'Cache-Control': 'no-store'})
        except Exception:
            return JSONResponse({'ok': False, 'error': '资料库暂时不可用，系统不会自动重复提交。'}, status_code=503, headers={'Cache-Control': 'no-store'})

    routes = (
        ('/preview', ['POST']),
        ('/tables', ['GET', 'POST']), ('/tables/{table_key}', ['PUT']),
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
