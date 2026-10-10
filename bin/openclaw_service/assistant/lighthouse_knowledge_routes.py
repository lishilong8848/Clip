"""Authenticated shared knowledge endpoints, common to both assistant channels."""
import asyncio
import json

from fastapi import Request
from fastapi.responses import FileResponse, JSONResponse
from starlette.formparsers import MultiPartException, MultiPartParser

from .lighthouse_ai import AssistantError
from .lighthouse_knowledge import check_actor, MAX_FILE_BYTES


KNOWLEDGE_ROUTES = (
    ('knowledge', ['GET']), ('knowledge/files', ['POST']),
    ('knowledge/search', ['GET']), ('knowledge/settings', ['GET', 'PUT']),
    ('knowledge/documents/{document_id}', ['GET', 'DELETE']),
    ('knowledge/documents/{document_id}/file', ['GET']),
    ('knowledge/documents/{document_id}/restore', ['POST']),
    ('knowledge/documents/{document_id}/retry', ['POST']),
)


def install_knowledge_routes(app, authorize, get_knowledge):
    async def endpoint(request: Request):
        try:
            actor = await authorize(request)
            check_actor(actor)
            service = await asyncio.to_thread(get_knowledge)
            action = request.url.path.removeprefix('/api/assistant/knowledge').strip('/')
            identity = request.path_params.get('document_id', '')
            query = dict(request.query_params)
            if action == 'files':
                total = 0
                async def bounded():
                    nonlocal total
                    async for chunk in request.stream():
                        total += len(chunk)
                        if total > 100 * 1024 * 1024 + 65536:
                            raise MultiPartException('文件合计不得超过100MiB。')
                        yield chunk
                try:
                    form = await MultiPartParser(request.headers, bounded(), max_files=10, max_fields=0).parse()
                except MultiPartException:
                    raise AssistantError('每次最多10个文件，合计不超过100MiB。', 413) from None
                try:
                    files = form.getlist('files')
                    target = query.get('document_id', '')
                    if set(form) != {'files'} or not files or any(not hasattr(file, 'read') for file in files) or target and len(files) != 1:
                        raise AssistantError('请选择文件；替换时只能上传一个文件。')
                    revision = int(query['version']) if target and 'version' in query else None
                    items, errors = [], []
                    for file in files:
                        content = await file.read(MAX_FILE_BYTES + 1)
                        try:
                            items.append(await asyncio.to_thread(service.upload, actor, file.filename, content,
                                document_id=target, revision=revision))
                        except AssistantError as exc:
                            errors.append({'name': file.filename, 'error': str(exc)})
                    data = {'items': items, 'errors': errors}
                finally:
                    await form.close()
            else:
                payload = {}
                if request.method not in {'GET', 'HEAD'}:
                    body = bytearray()
                    async for chunk in request.stream():
                        body.extend(chunk)
                        if len(body) > 16000:
                            raise AssistantError('提交内容过大。', 413)
                    payload = json.loads(body or b'{}')
                    if not isinstance(payload, dict):
                        raise AssistantError('提交格式无效。')
                if not action:
                    data = await asyncio.to_thread(service.list, actor, query)
                elif action == 'search':
                    data = await asyncio.to_thread(service.search, actor, query.get('q', ''))
                elif action == 'settings':
                    data = await asyncio.to_thread(service.settings, actor) if request.method == 'GET' else await asyncio.to_thread(service.save_settings, actor, payload)
                elif action.endswith('/file'):
                    path, name = await asyncio.to_thread(service.file, actor, identity, query.get('version'))
                    return FileResponse(path, filename=name, media_type='application/octet-stream',
                        headers={'Cache-Control': 'private, no-store', 'X-Content-Type-Options': 'nosniff'})
                elif request.method == 'GET':
                    data = await asyncio.to_thread(service.document, actor, identity, query)
                else:
                    operation = 'delete' if request.method == 'DELETE' else action.rsplit('/', 1)[-1]
                    data = await asyncio.to_thread(service.change, actor, identity, operation, payload.get('version'))
            return JSONResponse({'ok': True, 'data': data}, headers={'Cache-Control': 'no-store'})
        except AssistantError as exc:
            return JSONResponse({'ok': False, 'error': str(exc)}, status_code=exc.status)
        except (ValueError, TypeError):
            return JSONResponse({'ok': False, 'error': '知识库请求参数无效。'}, status_code=400)
        except Exception:
            return JSONResponse({'ok': False, 'error': '知识库暂不可用，已保存的文件不受影响。'}, status_code=503)

    for path, methods in KNOWLEDGE_ROUTES:
        app.add_api_route('/api/assistant/' + path, endpoint, methods=methods, name='lighthouse_' + path.replace('/', '_'))
