"""Authenticated pre-shift routes and an independent lightweight worker."""
import asyncio
import threading
from pathlib import Path
from urllib.parse import urlsplit

from fastapi import Request
from fastapi.responses import JSONResponse, FileResponse

from .pre_shift_checks import CheckError, CheckService, scopes_for

_IMAGE_TYPES = {'.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.png': 'image/png', '.webp': 'image/webp'}


def install_pre_shift_routes(app, controller, runtime):
    service = None
    lock = threading.Lock()
    from .pre_shift_cloud import CheckSender
    sender = CheckSender(runtime)

    def get_service():
        nonlocal service
        with lock:
            if service is None:
                from .pre_shift_cloud import CheckCloud, read_roster
                service = CheckService(roster=lambda day: read_roster(runtime.service, day),
                    cloud=CheckCloud(runtime.service), sender=sender, photo_sender=sender.photo,
                    portal_url=lambda: runtime.service._critical_guard_public_base_url())
            return service

    async def startup():
        sender.loop = asyncio.get_running_loop()
        await asyncio.to_thread(lambda: get_service().start())

    async def shutdown():
        if service:
            await asyncio.to_thread(service.stop)

    app.add_event_handler('startup', startup)
    app.add_event_handler('shutdown', shutdown)

    def actor_for(request):
        session = controller._current_session(request)
        if not session:
            raise CheckError('请先登录。', 401)
        from .cabinet_guest import is_cabinet_guest
        if is_cabinet_guest(session):
            raise CheckError('访客不能使用班前检查。', 403)
        user = runtime.auth_manager.public_user(session)
        oid = user.get('open_id')
        if not oid or not user.get('modules') or not scopes_for(user):
            raise CheckError('当前账号没有班前检查权限。', 403)
        return {**user, 'id': oid, 'name': user.get('name') or oid}

    def origin(value):
        u = urlsplit(value)
        return u.scheme, u.hostname, u.port or (443 if u.scheme == 'https' else 80)

    def assert_same_origin(request):
        source = request.headers.get('origin') or request.headers.get('referer')
        try:
            same_origin = not source or origin(source) == origin(controller._request_base_url(request))
        except ValueError:
            same_origin = False
        if request.headers.get('sec-fetch-site') == 'cross-site' or not same_origin:
            raise CheckError('不允许跨来源提交。', 403)

    async def body_for(request):
        assert_same_origin(request)
        body = await controller._read_json_request(request, max_bytes=128 * 1024)
        if not isinstance(body, dict):
            raise CheckError('提交格式无效。')
        return body

    async def handle(request, operation):
        try:
            actor = await asyncio.to_thread(actor_for, request)
            body = await body_for(request) if request.method != 'GET' else {}
            svc = await asyncio.to_thread(get_service)
            scope = request.query_params.get('scope') or actor.get('home_scope') or scopes_for(actor)[0]
            if operation == 'state':
                data = await asyncio.to_thread(svc.state, actor, scope)
            elif operation == 'history':
                data = await asyncio.to_thread(svc.history, actor, scope,
                    request.query_params.get('page', '1'), request.query_params.get('page_size', '20'))
            elif operation == 'definition':
                if set(body) != {'version', 'items'}:
                    raise CheckError('清单参数无效。')
                data = await asyncio.to_thread(svc.save_definition, actor, scope, body['version'], body['items'])
            else:
                if set(body) != {'task_id', 'version', 'answers'} or not isinstance(body['task_id'], str):
                    raise CheckError('提交参数无效。')
                data = await asyncio.to_thread(svc.submit, actor, body['task_id'], body['version'], body['answers'])
            return JSONResponse({'ok': True, 'data': data}, headers={'Cache-Control': 'no-store'})
        except CheckError as exc:
            return JSONResponse({'ok': False, 'error': str(exc)}, status_code=exc.status)

    @app.get('/api/pre-shift-checks')
    async def state(request: Request):
        return await handle(request, 'state')

    @app.put('/api/pre-shift-checks/definition')
    async def definition(request: Request):
        return await handle(request, 'definition')

    @app.get('/api/pre-shift-checks/history')
    async def history(request: Request):
        return await handle(request, 'history')

    @app.post('/api/pre-shift-checks/submit')
    async def submit(request: Request):
        return await handle(request, 'submit')

    @app.post('/api/pre-shift-checks/files')
    async def upload_file(request: Request):
        try:
            actor = await asyncio.to_thread(actor_for, request)
            assert_same_origin(request)
            svc = await asyncio.to_thread(get_service)
            task_id = request.query_params.get('task_id') or ''
            item_id = request.query_params.get('item_id') or ''
            kind = request.query_params.get('kind') or ''
            name = request.query_params.get('name') or ''
            session = await asyncio.to_thread(svc.stage_upload, actor, task_id, item_id, kind, name)
            stage = Path(session['path'])
            size = 0
            try:
                fp = await asyncio.to_thread(stage.open, 'wb')
                try:
                    async for chunk in request.stream():
                        size += len(chunk)
                        if size > session['max_bytes']:
                            raise CheckError('单个附件不能超过20MiB。', 413)
                        await asyncio.to_thread(fp.write, chunk)
                finally:
                    await asyncio.to_thread(fp.close)
                if kind == 'handover':
                    weather = getattr(app.state, 'lighthouse_icon_weather', None)
                    if weather is not None:
                        try:
                            session['photo_weather'] = await asyncio.wait_for(weather.get('南通'), timeout=3)
                        except Exception:
                            pass  # A forecast outage must not block saving the handover photo.
                # Reload actor: permissions may have been revoked during the stream/weather read.
                actor = await asyncio.to_thread(actor_for, request)
                result = await asyncio.to_thread(svc.commit_upload, actor, session, size)
            finally:
                # Successful rename leaves an already-moved (nonexistent) stage and
                # dedup leaves an unused stage, so always abort the staging path.
                await asyncio.to_thread(svc.abort_upload, session)
            return JSONResponse({'ok': True, 'data': result}, headers={'Cache-Control': 'no-store'})
        except CheckError as exc:
            return JSONResponse({'ok': False, 'error': str(exc)}, status_code=exc.status)

    @app.get('/api/pre-shift-checks/files/{file_id}')
    async def get_file(request: Request, file_id: str):
        try:
            actor = await asyncio.to_thread(actor_for, request)
            svc = await asyncio.to_thread(get_service)
            meta = await asyncio.to_thread(svc.get_file, actor, file_id)
            headers = {'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff'}
            suffix = Path(meta['name']).suffix.lower()
            if meta['is_image'] and suffix in _IMAGE_TYPES:
                return FileResponse(meta['path'], media_type=_IMAGE_TYPES[suffix], headers=headers)
            return FileResponse(meta['path'], filename=meta['name'],
                                media_type='application/octet-stream', headers=headers)
        except CheckError as exc:
            return JSONResponse({'ok': False, 'error': str(exc)}, status_code=exc.status)

    @app.delete('/api/pre-shift-checks/files/{file_id}')
    async def delete_file(request: Request, file_id: str):
        try:
            actor = await asyncio.to_thread(actor_for, request)
            assert_same_origin(request)
            svc = await asyncio.to_thread(get_service)
            result = await asyncio.to_thread(svc.delete_file, actor, file_id)
            return JSONResponse({'ok': True, 'data': result}, headers={'Cache-Control': 'no-store'})
        except CheckError as exc:
            return JSONResponse({'ok': False, 'error': str(exc)}, status_code=exc.status)
