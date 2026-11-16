"""Tool-check routes reuse the risk workspace session and origin guards."""

import asyncio
import logging
import mimetypes
from urllib.parse import quote

from fastapi import Request
from fastapi.responses import FileResponse, Response

from .risk_inspection import RiskError
from .tool_management import MAX_REPORT, ToolManagement
from .tool_self_check import MAX_FILE


def report_response(attachment):
    name, content = attachment
    mime = mimetypes.guess_type(name)[0] or 'application/octet-stream'
    disposition = 'inline' if mime in {'application/pdf', 'image/png', 'image/jpeg'} else 'attachment'
    return Response(content, media_type=mime, headers={
        'Content-Disposition': f"{disposition}; filename*=UTF-8''{quote(name)}",
        'Cache-Control': 'private, no-store', 'X-Content-Type-Options': 'nosniff',
        'Content-Security-Policy': 'sandbox',
    })


def compact_workspace(data):
    layouts = {}
    for item in data.get('items') or ():
        layout = item.pop('template', None)
        if layout:
            key = item['template_version']
            item['layout_key'] = key
            layouts[key] = layout
    if layouts:
        data['layouts'] = layouts
    return data


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
            if path in {'order-candidates', 'orders', 'orders/batch-create', 'orders/batch-delete', 'orders/clear-month'} or (
                path.startswith('orders/') and path.endswith(('/delete', '/restore'))
            ):
                raise RiskError('月度工单已停用，请在工具工作台办理。', 410)
            if path == 'workspace' or path.startswith('workspace/'):
                def workspace_response(data):
                    return response(compact_workspace(data) if request.query_params.get('compact') == '1' else data)

                if request.method == 'GET':
                    if path == 'workspace':
                        return response(await asyncio.to_thread(svc.workspace_list, actor))
                    if path == 'workspace/history':
                        query = request.query_params
                        return response(await asyncio.to_thread(svc.workspace_history, actor, query.get('before', ''),
                                                               query.get('id', ''), query.get('kind', ''), query.get('asset_id', '')))
                    if path.endswith('/report-file'):
                        return report_response(await asyncio.to_thread(svc.workspace_report_attachment, actor, identity))
                    if path.endswith('/people'):
                        return response(await asyncio.to_thread(svc.workspace_people, actor, identity))
                    if path.endswith('/download'):
                        content = await asyncio.to_thread(svc.workspace_download, actor, identity)
                        filename = quote(f'{identity}-工具办理归档.zip')
                        return Response(content, media_type='application/zip', headers={
                            'Content-Disposition': f"attachment; filename*=UTF-8''{filename}",
                            'Cache-Control': 'no-store',
                        })
                    if path.endswith('/status'):
                        return response(await asyncio.to_thread(svc.workspace_status, actor, identity, request.query_params.get('known', '')))
                    return workspace_response(await asyncio.to_thread(svc.workspace_get, actor, identity))
                same_origin(request)
                if path == 'workspace/refresh':
                    return response(await asyncio.to_thread(svc.workspace_refresh, actor))
                if path.endswith('/resume'):
                    body = await controller._read_json_request(request, max_bytes=4096) if request.headers.get('content-type') else {}
                    if not isinstance(body, dict):
                        raise RiskError('工具办理请求格式无效。')
                    return workspace_response(await asyncio.to_thread(svc.workspace_resume, actor, identity, body))
                if path.endswith('/report'):
                    revision = request.query_params.get('revision', '')
                    if not revision.isascii() or not revision.isdecimal() or len(revision) > 9:
                        raise RiskError('办理会话版本无效。')
                    payload = bytearray()
                    async for part in request.stream():
                        if len(payload) + len(part) > MAX_REPORT:
                            raise RiskError('送检报告不能超过 12 MB。', 413)
                        payload.extend(part)
                    return response(await asyncio.to_thread(
                        svc.workspace_report, actor, identity, int(revision),
                        request.query_params.get('name', ''), bytes(payload),
                    ))
                body = await controller._read_json_request(request, max_bytes=12 * 1024 * 1024)
                if not isinstance(body, dict):
                    raise RiskError('工具办理请求格式无效。')
                if path == 'workspace/open':
                    return workspace_response(await asyncio.to_thread(svc.workspace_open, actor, body))
                if path == 'workspace/transfer':
                    return response(await asyncio.to_thread(svc.workspace_transfer, actor, body))
                if path.endswith('/save'):
                    return workspace_response(await asyncio.to_thread(svc.workspace_save, actor, identity, body))
                if path.endswith('/review'):
                    return workspace_response(await asyncio.to_thread(svc.workspace_review, actor, identity, body))
                if path.endswith('/manage'):
                    return workspace_response(await asyncio.to_thread(svc.workspace_manage_item, actor, identity, body))
                if path.endswith('/binding'):
                    return workspace_response(await asyncio.to_thread(svc.workspace_resolve_binding, actor, identity, body))
                if path.endswith('/reconcile'):
                    return response(await asyncio.to_thread(svc.workspace_reconcile, actor, identity, body))
                return workspace_response(await asyncio.to_thread(svc.workspace_submit, actor, identity, body, require_report_check=True))
            if request.method == 'GET':
                if not path:
                    result = await asyncio.to_thread(svc.list, actor, request.query_params.get('month', ''))
                elif path == 'inventory':
                    result = await asyncio.to_thread(svc.inventory, actor)
                elif path == 'inventory/fields':
                    result = await asyncio.to_thread(svc.new_inventory_fields, actor)
                elif path == 'templates':
                    result = await asyncio.to_thread(svc.template_library, actor)
                elif path.startswith('inventory/') and path.endswith('/history'):
                    result = await asyncio.to_thread(svc.tool_history, actor, identity)
                elif path.endswith('/report-file'):
                    return report_response(await asyncio.to_thread(svc.report_attachment, actor, identity))
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
                template_id = request.query_params.get('id', '')
                legacy = bool(template_id and not template_id.startswith('rec'))
                save_template = svc.save_template if legacy else svc.save_library_template
                result = await asyncio.to_thread(save_template, actor, request.query_params.get('name', ''),
                    bytes(payload), template_id, int(version) if version else None)
                return response(result)
            body = await controller._read_json_request(request, max_bytes=512 * 1024)
            if not isinstance(body, dict):
                raise RiskError('工具自检请求格式无效。')
            if path == 'inventory/new':
                return response(await asyncio.to_thread(svc.create_inventory, actor, body))
            if path == 'inventory/refresh':
                return response(await asyncio.to_thread(svc.refresh_inventory, actor))
            if path.endswith('/clone') and path.startswith('templates/'):
                return response(await asyncio.to_thread(svc.clone_library_template, actor, identity, body))
            if path.endswith('/rebind') and path.startswith('templates/'):
                return response(await asyncio.to_thread(svc.rebind_library_template, actor, identity, body))
            if path.endswith('/delete') and path.startswith('templates/'):
                delete_template = svc.delete_library_template if identity.startswith('rec') else svc.delete_template
                return response(await asyncio.to_thread(delete_template, actor, identity, body))
            if path.startswith('templates/'):
                if identity.startswith('rec'):
                    return response(await asyncio.to_thread(svc.library_template_availability, actor, identity, body))
                await asyncio.to_thread(svc.availability, actor, params['identity'], body)
                return response({'saved': True})
            if path.startswith('inventory/'):
                return response(await asyncio.to_thread(svc.configure, actor, identity, body))
            return response(await asyncio.to_thread(svc.action, actor, identity, params['action'], body, require_report_check=True))
        except RiskError as exc:
            return response(error=exc)
        except Exception:
            logging.exception('Tool self-check request failed')
            if request.url.path.startswith(base + '/workspace'):
                return response(error=RiskError('操作结果暂未确认，请打开「办理记录」核对已保存内容。', 503))
            return response(error=RiskError('操作结果暂未确认，请刷新工单核对已保存内容。', 503))

    for path, methods in (('', ['GET']), ('/archive/{month}', ['GET']), ('/inventory', ['GET']),
                          ('/inventory/fields', ['GET']),
                          # Must precede the /inventory/{identity} catch-all below.
                          ('/inventory/refresh', ['POST']), ('/inventory/{identity}', ['POST']), ('/order-candidates', ['GET']),
                          ('/orders', ['POST']), ('/orders/batch-create', ['GET', 'POST']), ('/orders/batch-delete', ['POST']),
                          ('/orders/clear-month', ['GET', 'POST']), ('/orders/{identity}', ['GET']),
                          ('/orders/{identity}/download', ['GET']), ('/orders/{identity}/report', ['POST']),
                          ('/orders/{identity}/report-file', ['GET']),
                          ('/orders/{identity}/{action}', ['POST']), ('/templates', ['GET', 'POST']),
                          ('/templates/{identity}/availability', ['POST']), ('/templates/{identity}/clone', ['POST']),
                          ('/templates/{identity}/delete', ['POST']),
                          ('/templates/{identity}/rebind', ['POST']),
                          ('/workspace', ['GET']), ('/workspace/open', ['POST']), ('/workspace/refresh', ['POST']),
                          ('/workspace/transfer', ['POST']),
                          ('/workspace/history', ['GET']),
                          ('/workspace/{identity}', ['GET']), ('/workspace/{identity}/status', ['GET']),
                          ('/workspace/{identity}/people', ['GET']),
                          ('/workspace/{identity}/review', ['POST']), ('/workspace/{identity}/binding', ['POST']),
                          ('/workspace/{identity}/manage', ['POST']),
                          ('/workspace/{identity}/reconcile', ['POST']),
                          ('/workspace/{identity}/save', ['POST']),
                          ('/workspace/{identity}/resume', ['POST']),
                          ('/workspace/{identity}/submit', ['POST']), ('/workspace/{identity}/report', ['POST']),
                          ('/workspace/{identity}/download', ['GET']), ('/workspace/{identity}/report-file', ['GET']),
                          ('/inventory/{identity}/history', ['GET'])):
        app.add_api_route(base + path, endpoint, methods=methods)
