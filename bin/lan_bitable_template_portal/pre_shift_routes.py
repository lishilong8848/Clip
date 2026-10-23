"""Authenticated pre-shift routes and an independent lightweight worker."""
import asyncio
import json
import logging
import threading
from contextlib import suppress
from pathlib import Path
from urllib.parse import urlsplit, unquote

from fastapi import Request
from fastapi.responses import JSONResponse, FileResponse

from .pre_shift_checks import CheckError, CheckService, scopes_for

_IMAGE_TYPES = {'.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.png': 'image/png', '.webp': 'image/webp'}
_LOCATION_RETRY_SECONDS = 300


def install_pre_shift_routes(app, controller, runtime):
    service = None
    location_service = None
    location_bootstrap = None
    lock = threading.Lock()
    from .pre_shift_cloud import CheckSender
    sender = CheckSender(runtime)

    def get_service():
        nonlocal service
        with lock:
            if service is None:
                from .pre_shift_cloud import CheckCloud, read_roster, fixed_recipient_options
                service = CheckService(roster=lambda day: read_roster(runtime.service, day, scopes=get_service().configured_scopes(roster_only=True)),
                    cloud=CheckCloud(runtime.service), sender=sender, photo_sender=sender.photo,
                    recipient_directory=lambda scope: fixed_recipient_options(runtime, scope),
                    portal_url=lambda: runtime.service._critical_guard_public_base_url())
            return service

    def get_location():
        nonlocal location_service
        svc = get_service()
        with lock:
            if location_service is None:
                from .pre_shift_location import PhotoLocation
                location_service = PhotoLocation(svc.db.parent / 'location_settings.json')
            return location_service

    async def bootstrap_location():
        while True:
            try:
                from .pre_shift_location_distribution import provision_location
                mapper = await asyncio.to_thread(get_location)
                if mapper.closed or await asyncio.to_thread(provision_location, mapper):
                    return
            except Exception:
                logging.warning('地点解析自动配置暂未完成，保留本机原配置，5分钟后自动重试。')
            await asyncio.sleep(_LOCATION_RETRY_SECONDS)

    async def startup():
        nonlocal location_bootstrap
        sender.loop = asyncio.get_running_loop()
        await asyncio.to_thread(lambda: get_service().start())
        if location_bootstrap is None or location_bootstrap.done():
            location_bootstrap = asyncio.create_task(bootstrap_location())

    async def shutdown():
        if location_service:
            await asyncio.to_thread(location_service.close)
        if location_bootstrap:
            location_bootstrap.cancel()
            with suppress(asyncio.CancelledError):
                await location_bootstrap
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

    def revision_for(request):
        value = request.query_params.get('revision')
        if value is None:
            return None
        if not value.isascii() or not value.isdecimal() or len(value) > 12 or int(value) < 1:
            raise CheckError('检查版本无效。', 409)
        return int(value)

    async def handle(request, operation):
        try:
            actor = await asyncio.to_thread(actor_for, request)
            body = await body_for(request) if request.method != 'GET' else {}
            svc = await asyncio.to_thread(get_service)
            scope = request.query_params.get('scope') or actor.get('home_scope') or scopes_for(actor)[0]
            if operation == 'state':
                data = await asyncio.to_thread(svc.state, actor, scope)
                data['can_configure_location'] = actor.get('level') in ('L0', 'L1')
            elif operation == 'recipients':
                from .pre_shift_checks import authorized
                from .pre_shift_cloud import fixed_recipient_options
                await asyncio.to_thread(authorized, actor, scope, True)
                data = {'items': await asyncio.to_thread(fixed_recipient_options, runtime, scope)}
            elif operation == 'location-settings':
                if actor.get('level') not in ('L0', 'L1'):
                    raise CheckError('仅 L0/L1 管理员可配置全局地点解析。', 403)
                mapper = await asyncio.to_thread(get_location)
                if request.method == 'GET':
                    data = await asyncio.to_thread(mapper.settings)
                else:
                    data = await asyncio.to_thread(mapper.save, body)
            elif operation == 'history':
                data = await asyncio.to_thread(svc.history, actor, scope,
                    request.query_params.get('page', '1'), request.query_params.get('page_size', '20'))
            elif operation == 'definition':
                if set(body) != {'version', 'items'}:
                    raise CheckError('清单参数无效。')
                data = await asyncio.to_thread(svc.save_definition, actor, scope, body['version'], body['items'])
            elif operation == 'task':
                task_id = request.path_params['task_id']
                if request.method == 'GET':
                    data = await asyncio.to_thread(svc.task, actor, task_id)
                else:
                    if set(body) != {'revision', 'answers'}:
                        raise CheckError('编辑参数无效。')
                    data = await asyncio.to_thread(svc.edit, actor, task_id, body['revision'], body['answers'])
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

    @app.get('/api/pre-shift-checks/recipients')
    async def recipients(request: Request):
        return await handle(request, 'recipients')

    @app.api_route('/api/pre-shift-checks/location-settings', methods=['GET', 'PUT'])
    async def location_settings(request: Request):
        return await handle(request, 'location-settings')

    @app.get('/api/pre-shift-checks/history')
    async def history(request: Request):
        return await handle(request, 'history')

    @app.post('/api/pre-shift-checks/submit')
    async def submit(request: Request):
        return await handle(request, 'submit')

    @app.api_route('/api/pre-shift-checks/tasks/{task_id}', methods=['GET', 'PUT'])
    async def task(request: Request, task_id: str):
        return await handle(request, 'task')

    @app.delete('/api/pre-shift-checks/photo-preview')
    async def discard_photo(request: Request):
        try:
            actor = await asyncio.to_thread(actor_for, request)
            body = await body_for(request)
            if set(body) != {'preview_id'}:
                raise CheckError('照片预览参数无效。')
            svc = await asyncio.to_thread(get_service)
            result = await asyncio.to_thread(svc.discard_photo_preview, actor, body['preview_id'])
            return JSONResponse({'ok': True, 'data': result}, headers={'Cache-Control': 'no-store'})
        except CheckError as exc:
            return JSONResponse({'ok': False, 'error': str(exc)}, status_code=exc.status)

    @app.post('/api/pre-shift-checks/photo-preview')
    @app.post('/api/pre-shift-checks/files')
    async def upload_file(request: Request):
        try:
            actor = await asyncio.to_thread(actor_for, request)
            assert_same_origin(request)
            svc = await asyncio.to_thread(get_service)
            if (request.url.path.endswith('/files') and not request.query_params.get('task_id')
                    and request.headers.get('content-type', '').split(';')[0] == 'application/json'):
                body = await body_for(request)
                if set(body) != {'preview_id', 'watermark', 'remember_style'}:
                    raise CheckError('照片确认参数无效。')
                result = await asyncio.to_thread(svc.commit_photo_preview, actor, body['preview_id'], body['watermark'], body['remember_style'])
                return JSONResponse({'ok': True, 'data': result}, headers={'Cache-Control': 'no-store'})
            task_id = request.query_params.get('task_id') or ''
            item_id = request.query_params.get('item_id') or ''
            kind = request.query_params.get('kind') or ''
            name = request.query_params.get('name') or ''
            capture = request.query_params.get('capture', '0')
            retain = request.query_params.get('retain', '0')
            if retain not in ('0', '1') or retain == '1' and not request.url.path.endswith('/photo-preview'):
                raise CheckError('照片预览暂存参数无效。')
            if capture not in ('0', '1'):
                raise CheckError('拍照参数无效。')
            raw_context = request.headers.get('X-Photo-Context', '{}')
            try:
                if len(raw_context) > 16384:
                    raise ValueError()
                raw_context = unquote(raw_context) if raw_context.startswith('%') else raw_context
                if len(raw_context) > 4096:
                    raise ValueError()
                photo_capture = json.loads(raw_context)
                if not isinstance(photo_capture, dict):
                    raise ValueError()
            except (ValueError, TypeError):
                raise CheckError('拍照信息格式无效。') from None
            if capture != '1':
                photo_capture.pop('captured_at', None)
            photo_capture.pop('_time_audit', None)
            photo_capture.pop('_location_name', None)
            session = await asyncio.to_thread(svc.stage_upload, actor, task_id, item_id, kind, name,
                captured=capture == '1', revision=revision_for(request), replace_file_id=request.query_params.get('replace_file_id'))
            session['photo_capture'] = photo_capture
            stage = Path(session['path'])
            size = 0
            retained = False
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
                if kind == 'handover' or capture == '1' or request.url.path.endswith('/photo-preview') or 'watermark' in photo_capture:
                    mapper = await asyncio.to_thread(get_location)
                    photo_capture['_location_name'] = await asyncio.to_thread(mapper.resolve, photo_capture)
                    try:
                        state = await asyncio.to_thread(runtime.service._state_store.get_critical_guard_weather_state)
                        if isinstance(state, dict) and isinstance(state.get('snapshot'), dict):
                            session['photo_weather'] = dict(state['snapshot'])
                    except Exception:
                        pass  # Missing current observations leave the weather line out.
                    weather = getattr(app.state, 'lighthouse_icon_weather', None)
                    if weather is not None:
                        try:
                            session.setdefault('photo_weather', {})['forecast'] = await asyncio.wait_for(weather.get('南通'),timeout=3)
                        except Exception:
                            pass
                # Reload actor: permissions may have been revoked during the stream/weather read.
                actor = await asyncio.to_thread(actor_for, request)
                if retain == '1':
                    result = await asyncio.to_thread(svc.retain_photo_preview, actor, session, size)
                    retained = True
                else:
                    result = await asyncio.to_thread(svc.commit_upload, actor, session, size,
                                                    preview=request.url.path.endswith('/photo-preview'))
            finally:
                # Successful rename leaves an already-moved (nonexistent) stage and
                # dedup leaves an unused stage, so always abort the staging path.
                if not retained:
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
                return FileResponse(meta['path'], media_type=_IMAGE_TYPES[suffix], filename=meta['name'],
                                    content_disposition_type='inline', headers=headers)
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
            result = await asyncio.to_thread(svc.delete_file, actor, file_id,
                revision=revision_for(request), task_id=request.query_params.get('task_id'))
            return JSONResponse({'ok': True, 'data': result}, headers={'Cache-Control': 'no-store'})
        except CheckError as exc:
            return JSONResponse({'ok': False, 'error': str(exc)}, status_code=exc.status)
