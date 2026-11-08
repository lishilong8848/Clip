"""Tool-check routes reuse the risk workspace session and origin guards."""

import asyncio
import logging
from urllib.parse import quote

from fastapi import Request
from fastapi.responses import FileResponse, Response

from .risk_inspection import RiskError
from .tool_management import MAX_REPORT, ToolManagement
from .tool_self_check import MAX_FILE


def install_tool_self_check_routes(app, controller, service, read_actor, same_origin, response):
    base = '/api/tool-self-check'

    async def endpoint(request: Request):
        try:
            actor = await asyncio.to_thread(read_actor, request)
            ToolManagement.access(actor)
            svc = await asyncio.to_thread(service)
            if not await asyncio.to_thread(svc.available_to, actor):
                raise RiskError('当前账号没有工具管理权限。', 403)
            params = request.path_params
            month = params.get('month', '')
            identity = params.get('identity', '')
            path = request.url.path.removeprefix(base).strip('/')
            if request.method == 'GET':
                if not path:
                    result = await asyncio.to_thread(svc.list, actor, request.query_params.get('month', ''))
                elif path == 'inventory':
                    result = await asyncio.to_thread(svc.inventory, actor)
                elif path == 'order-candidates':
                    result = await asyncio.to_thread(svc.order_candidates, actor)
                elif path == 'inventory/fields':
                    result = await asyncio.to_thread(svc.new_inventory_fields, actor)
                elif path.startswith('archive/') or path.endswith('/download'):
                    content = await asyncio.to_thread(svc.download, actor, month, identity)
                    filename = quote(f'{month or identity}-工具管理归档.zip')
                    headers = {'Content-Disposition': f"attachment; filename*=UTF-8''{filename}", 'Cache-Control': 'no-store'}
                    if not identity:
                        return FileResponse(content, media_type='application/zip', headers=headers)
                    return Response(content, media_type='application/zip', headers=headers)
                else:
                    result = await asyncio.to_thread(svc.detail, actor, identity)
                return response(result)

            same_origin(request)
            if path.endswith('/report'):
                revision = request.query_params.get('revision', '')
                if not revision.isascii() or not revision.isdecimal() or len(revision) > 9:
                    raise RiskError('工单版本无效。')
                payload = bytearray()
                async for part in request.stream():
                    if len(payload) + len(part) > MAX_REPORT:
                        raise RiskError('送检报告不能超过 12 MB。', 413)
                    payload.extend(part)
                return response(await asyncio.to_thread(svc.attach, actor, identity, int(revision),
                    request.query_params.get('name', ''), bytes(payload)))
            if path == 'templates':
                svc.access(actor)
                if not svc.administrator(actor):
                    raise RiskError('仅管理员可维护工具自检模板。', 403)
                payload = bytearray()
                async for part in request.stream():
                    if len(payload) + len(part) > MAX_FILE:
                        raise RiskError('自检模板不能超过 2 MB。', 413)
                    payload.extend(part)
                version = request.query_params.get('version', '')
                if version and (not version.isascii() or not version.isdecimal() or len(version) > 9):
                    raise RiskError('模板版本无效。')
                result = await asyncio.to_thread(svc.save_template, actor, request.query_params.get('name', ''),
                    bytes(payload), request.query_params.get('id', ''), int(version) if version else None)
                return response(result)
            body = await controller._read_json_request(request, max_bytes=512 * 1024)
            if not isinstance(body, dict):
                raise RiskError('工具自检请求格式无效。')
            if path == 'orders':
                return response(await asyncio.to_thread(svc.create_order, actor, body))
            if path == 'inventory/new':
                return response(await asyncio.to_thread(svc.create_inventory, actor, body))
            if path.endswith('/delete') and path.startswith('orders/'):
                return response(await asyncio.to_thread(svc.cancel_order, actor, identity, body))
            if path.endswith('/clone') and path.startswith('templates/'):
                return response(await asyncio.to_thread(svc.clone_template, actor, params['identity'], body))
            if path.startswith('templates/'):
                await asyncio.to_thread(svc.availability, actor, params['identity'], body)
                return response({'saved': True})
            if path.startswith('inventory/'):
                return response(await asyncio.to_thread(svc.configure, actor, identity, body))
            return response(await asyncio.to_thread(svc.action, actor, identity, params['action'], body))
        except RiskError as exc:
            return response(error=exc)
        except Exception:
            logging.exception('Tool self-check request failed')
            return response(error=RiskError('操作结果暂未确认，请刷新工单核对已保存内容。', 503))

    for path, methods in (('', ['GET']), ('/archive/{month}', ['GET']), ('/inventory', ['GET']),
                          ('/inventory/fields', ['GET']), ('/inventory/{identity}', ['POST']), ('/order-candidates', ['GET']), ('/orders', ['POST']), ('/orders/{identity}', ['GET']),
                          ('/orders/{identity}/download', ['GET']), ('/orders/{identity}/report', ['POST']),
                          ('/orders/{identity}/{action}', ['POST']), ('/templates', ['POST']),
                          ('/templates/{identity}/availability', ['POST']), ('/templates/{identity}/clone', ['POST'])):
        app.add_api_route(base + path, endpoint, methods=methods)
