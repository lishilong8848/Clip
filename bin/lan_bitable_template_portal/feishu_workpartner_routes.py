"""Native assistant catalogue routes; writes require the original confirmed plan."""
import asyncio
from urllib.parse import urlsplit

from fastapi import Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from openclaw_service.assistant.lighthouse_ai import AssistantError
from .feishu_workpartner import CONFIRMED_EXECUTION, Workpartner, reference
from .feishu_workpartner_models import (MessageSend, TaskCreate, TaskUpdate, EventCreate,
    EventUpdate, EventAttendees, DocumentCreate, DocumentAppend, TableRecordWrite, ResourceGrant)
from .lighthouse_bridge import actor_for


def install_feishu_workpartner_routes(app, controller, runtime, channel):
    service = None

    def get_service():
        nonlocal service
        if service is None:
            service = Workpartner(runtime, channel)
        return service

    async def actor(request, *, write=False, admin=False):
        from .assistant_shared_read import original_actor
        current = original_actor(await actor_for(controller, runtime, request))
        if admin and not current.get('is_admin'):
            raise AssistantError('仅管理员可配置协作资源授权。', 403)
        if write:
            expected = urlsplit(controller._request_base_url(request))
            actual = urlsplit(request.headers.get('origin') or request.headers.get('referer') or '')
            if ((actual.scheme, actual.netloc) != (expected.scheme, expected.netloc)
                    or request.headers.get('sec-fetch-site') == 'cross-site'):
                raise AssistantError('不允许跨来源提交。', 403)
            if not admin:
                confirmed = CONFIRMED_EXECUTION.get()
                if not confirmed or confirmed[0] != current['id']:
                    raise AssistantError('请先在助手中核对表单并确认操作，未发送或写入。', 403)
        return current

    def bound(model):
        # Plan/step identity, not model-supplied randomness, owns retry deduplication.
        confirmed = CONFIRMED_EXECUTION.get()
        return model.model_copy(update={'operation_id': reference(list(confirmed))})

    def failure(exc, *, read=False):
        from .portal_service import PortalError
        if isinstance(exc, ValidationError) or isinstance(exc, PortalError) and getattr(exc, 'status_code', None) == 400:
            return JSONResponse({'ok': False, 'error': '填写内容无效，请核对必填项、人员和日期格式；未执行操作。'}, status_code=422)
        known = isinstance(exc, AssistantError)
        confirmed = CONFIRMED_EXECUTION.get()
        operation = {'operation_id': reference(list(confirmed))} if confirmed else {}
        return JSONResponse({'ok': False, 'error': str(exc) if known else (
            '资料暂未完整读取，请稍后重试；不能当成没有数据。' if read else '操作结果尚未确认，请查询原操作，不要重复提交。'),
            'error_category': exc.category if known else 'feishu_workpartner_unavailable' if read else 'feishu_workpartner_uncertain', **operation},
            status_code=exc.status if known else 503, headers={'Cache-Control': 'no-store'})

    async def respond(request, run):
        try:
            value = await run()
            return JSONResponse({'ok': True, 'data': value}, headers={'Cache-Control': 'no-store'})
        except Exception as exc:
            return failure(exc, read=request.method == 'GET')

    @app.get('/api/feishu-workpartner/capabilities')
    async def capabilities(request: Request):
        async def run():
            await actor(request)
            return await get_service().capabilities()
        return await respond(request, run)

    @app.get('/api/feishu-workpartner/people')
    async def people(request: Request, q: str = '', page: int = 1):
        async def run():
            current = await actor(request)
            if not 1 <= page <= 10000:
                raise AssistantError('页码无效。', 422)
            rows = await get_service().people(current, q[:120])
            start = (page - 1) * 30
            return {'people': [{key: row.get(key, '') for key in ('id', 'name', 'employee_no', 'building')}
                               for row in rows[start:start + 30]], 'total': len(rows), 'page': page, 'has_more': start + 30 < len(rows)}
        return await respond(request, run)

    @app.get('/api/feishu-workpartner/resources')
    async def resources(request: Request, kind: str = '', q: str = '', page: int = 1):
        async def run():
            return await get_service().resources(await actor(request), kind[:20], q[:120], page)
        return await respond(request, run)

    @app.get('/api/feishu-workpartner/chats')
    async def chats(request: Request, q: str = '', cursor: str = ''):
        async def run():
            return await get_service().chats(await actor(request), q[:120], cursor[:2048])
        return await respond(request, run)

    @app.post('/api/admin/feishu-workpartner/resources')
    async def grant_resource(request: Request):
        async def run():
            current = await actor(request, write=True, admin=True)
            payload = await controller._read_model_request(request, ResourceGrant)
            return await get_service().register(current, payload)
        return await respond(request, run)

    @app.post('/api/feishu-workpartner/resource-grants')
    async def confirmed_resource_grant(request: Request):
        async def run():
            current = await actor(request, write=True)
            if not current.get('is_admin'):
                raise AssistantError('仅管理员可确认协作资源授权，不扩大现有人员权限。', 403)
            payload = await controller._read_model_request(request, ResourceGrant)
            service = get_service()
            model = bound(payload)
            return await service.write(current, 'resource.grant', model, lambda: service.register(current, model))
        return await respond(request, run)

    @app.get('/api/feishu-workpartner/operations/{operation_id}')
    async def operation_status(operation_id: str, request: Request):
        async def run():
            current = await actor(request)
            return await asyncio.to_thread(get_service().status, current, operation_id[:128])
        return await respond(request, run)

    @app.post('/api/feishu-workpartner/messages/send')
    async def send_message(request: Request):
        async def run():
            current = await actor(request, write=True)
            payload = await controller._read_model_request(request, MessageSend)
            return await get_service().send(current, bound(payload))
        return await respond(request, run)

    @app.post('/api/feishu-workpartner/tasks')
    async def create_task(request: Request):
        async def run():
            current = await actor(request, write=True)
            payload = await controller._read_model_request(request, TaskCreate)
            return await get_service().create_task(current, bound(payload))
        return await respond(request, run)

    @app.get('/api/feishu-workpartner/tasks/{task_id}')
    async def task(task_id: str, request: Request):
        async def run():
            return await get_service().task(await actor(request), task_id)
        return await respond(request, run)

    @app.patch('/api/feishu-workpartner/tasks/{task_id}')
    async def update_task(task_id: str, request: Request):
        async def run():
            current = await actor(request, write=True)
            payload = await controller._read_model_request(request, TaskUpdate)
            return await get_service().update_task(current, task_id, bound(payload))
        return await respond(request, run)

    @app.get('/api/feishu-workpartner/events')
    async def events(request: Request, calendar_id: str, start_time: str, end_time: str, cursor: str = ''):
        async def run():
            return await get_service().events(await actor(request), calendar_id, start_time, end_time, cursor[:2048])
        return await respond(request, run)

    @app.post('/api/feishu-workpartner/events')
    async def create_event(request: Request):
        async def run():
            current = await actor(request, write=True)
            payload = await controller._read_model_request(request, EventCreate)
            return await get_service().write_event(current, bound(payload))
        return await respond(request, run)

    @app.patch('/api/feishu-workpartner/events/{event_id}')
    async def update_event(event_id: str, request: Request):
        async def run():
            current = await actor(request, write=True)
            payload = await controller._read_model_request(request, EventUpdate)
            return await get_service().write_event(current, bound(payload), event_id)
        return await respond(request, run)

    @app.post('/api/feishu-workpartner/events/{event_id}/attendees')
    async def add_attendees(event_id: str, request: Request):
        async def run():
            current = await actor(request, write=True)
            payload = await controller._read_model_request(request, EventAttendees)
            return await get_service().add_attendees(current, event_id, bound(payload))
        return await respond(request, run)

    @app.post('/api/feishu-workpartner/documents')
    async def create_document(request: Request):
        async def run():
            current = await actor(request, write=True)
            payload = await controller._read_model_request(request, DocumentCreate)
            return await get_service().create_document(current, bound(payload))
        return await respond(request, run)

    @app.get('/api/feishu-workpartner/documents/{document_id}')
    async def document(document_id: str, request: Request, offset: int = 0):
        async def run():
            return await get_service().document(await actor(request), document_id, offset)
        return await respond(request, run)

    @app.post('/api/feishu-workpartner/documents/{document_id}/append')
    async def append_document(document_id: str, request: Request):
        async def run():
            current = await actor(request, write=True)
            payload = await controller._read_model_request(request, DocumentAppend)
            return await get_service().append_document(current, document_id, bound(payload))
        return await respond(request, run)

    @app.get('/api/feishu-workpartner/tables/{table_id}/records')
    async def table_records(table_id: str, request: Request, cursor: str = ''):
        async def run():
            return await get_service().table_records(await actor(request), table_id, cursor[:2048])
        return await respond(request, run)

    @app.post('/api/feishu-workpartner/tables/{table_id}/records')
    async def create_record(table_id: str, request: Request):
        async def run():
            current = await actor(request, write=True)
            payload = await controller._read_model_request(request, TableRecordWrite)
            return await get_service().write_record(current, table_id, bound(payload))
        return await respond(request, run)

    @app.patch('/api/feishu-workpartner/tables/{table_id}/records/{record_id}')
    async def update_record(table_id: str, record_id: str, request: Request):
        async def run():
            current = await actor(request, write=True)
            payload = await controller._read_model_request(request, TableRecordWrite)
            return await get_service().write_record(current, table_id, bound(payload), record_id)
        return await respond(request, run)
