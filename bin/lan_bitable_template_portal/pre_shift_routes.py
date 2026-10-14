"""Authenticated pre-shift routes and an independent lightweight worker."""
import asyncio
import threading
from urllib.parse import urlsplit

from fastapi import Request
from fastapi.responses import JSONResponse

from .pre_shift_checks import CheckError, CheckService, scopes_for


def install_pre_shift_routes(app, controller, runtime):
    service = None
    lock = threading.Lock()

    def get_service():
        nonlocal service
        with lock:
            if service is None:
                from .pre_shift_cloud import CheckCloud, read_roster
                from .learning_reminders import send_reminder
                service = CheckService(roster=lambda day: read_roster(runtime.service, day),
                    cloud=CheckCloud(runtime.service), sender=send_reminder,
                    portal_url=lambda: runtime.service._critical_guard_public_base_url())
            return service

    async def startup():
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

    async def body_for(request):
        def origin(value):
            u = urlsplit(value)
            return u.scheme, u.hostname, u.port or (443 if u.scheme == 'https' else 80)
        source = request.headers.get('origin') or request.headers.get('referer')
        try:
            same_origin = not source or origin(source) == origin(controller._request_base_url(request))
        except ValueError:
            same_origin = False
        if request.headers.get('sec-fetch-site') == 'cross-site' or not same_origin:
            raise CheckError('不允许跨来源提交。', 403)
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

    @app.post('/api/pre-shift-checks/submit')
    async def submit(request: Request):
        return await handle(request, 'submit')
