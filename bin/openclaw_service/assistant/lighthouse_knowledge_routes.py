"""Authenticated shared knowledge endpoints, common to both assistant channels."""
import asyncio
import json

from fastapi import Request
from fastapi.responses import FileResponse, JSONResponse
from starlette.formparsers import MultiPartException, MultiPartParser
from starlette.requests import ClientDisconnect

from .lighthouse_ai import AssistantError
from .lighthouse_knowledge import check_actor


MAX_PER_FILE = 100 * 1024 * 1024
MAX_BATCH = 300 * 1024 * 1024
MAX_FILES = 10
MULTIPART_OVERHEAD = 64 * 1024
# Bound concurrent multipart upload parsing so a burst of large uploads cannot
# open hundreds of spooled temp file handles at once.  When both slots are full
# the request fails fast instead of queueing behind open file handles.
_UPLOAD_SLOTS = 2
_UPLOAD_SEMAPHORE = asyncio.Semaphore(_UPLOAD_SLOTS)


async def _try_acquire_upload_slot():
    if _UPLOAD_SEMAPHORE._value < 1:
        return False
    await _UPLOAD_SEMAPHORE.acquire()
    return True


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
                if not await _try_acquire_upload_slot():
                    raise AssistantError('同时上传的文件较多，请稍后重试。', 503)
                form = None
                try:
                    total = 0
                    async def bounded():
                        nonlocal total
                        try:
                            async for chunk in request.stream():
                                total += len(chunk)
                                if total > MAX_BATCH + MULTIPART_OVERHEAD:
                                    raise MultiPartException('文件合计不得超过300MiB。')
                                yield chunk
                        except asyncio.CancelledError:
                            raise
                        except ClientDisconnect:
                            # Surface a disconnect as a parse error so Starlette's
                            # MultiPartParser closes every spooled temp file it had
                            # already allocated (zero-file-handle leak on a dropped upload).
                            raise MultiPartException('客户端中断上传。') from None
                    parser = MultiPartParser(request.headers, bounded(), max_files=MAX_FILES, max_fields=0)
                    try:
                        form = await parser.parse()
                    except MultiPartException:
                        raise AssistantError(f'每次最多{MAX_FILES}个文件，合计不超过300MiB。', 413) from None
                    except BaseException:
                        # Starlette only closes partial uploads for MultiPartException.
                        for handle in parser._files_to_close_on_error:
                            handle.close()
                        raise
                    files = form.getlist('files')
                    target = query.get('document_id', '')
                    if set(form) != {'files'} or not files or any(not hasattr(file, 'read') for file in files) or target and len(files) != 1:
                        raise AssistantError('请选择文件；替换时只能上传一个文件。')
                    if len(files) > MAX_FILES:
                        raise AssistantError(f'每次最多上传{MAX_FILES}个文件。', 413)
                    oversized = [file for file in files if (getattr(file, 'size', 0) or 0) > MAX_PER_FILE]
                    if oversized:
                        raise AssistantError('单文件最大100MiB。', 413)
                    batch = sum(getattr(file, 'size', 0) or 0 for file in files)
                    if batch > MAX_BATCH:
                        raise AssistantError('一次上传合计不超过300MiB。', 413)
                    revision = int(query['version']) if target and 'version' in query else None
                    items, errors = [], []
                    for file in files:
                        try:
                            # file.file is the SpooledTemporaryFile parsed by MultiPartParser and is
                            # seekable; service.upload_file streams from it instead of buffering RAM.
                            items.append(await asyncio.to_thread(service.upload_file, actor, file.filename, file.file,
                                document_id=target, revision=revision))
                        except AssistantError as exc:
                            errors.append({'name': file.filename, 'error': str(exc)})
                    data = {'items': items, 'errors': errors}
                finally:
                    if form is not None:
                        await form.close()
                    _UPLOAD_SEMAPHORE.release()
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
