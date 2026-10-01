"""Native authenticated FastAPI endpoints for plan-convergence review."""
import asyncio
import json
import logging
import sqlite3
import threading
from urllib.parse import unquote, urlencode, urlsplit

import requests
from fastapi import Request
from fastapi.responses import JSONResponse, Response

ROUTES = {
    'bootstrap': ['GET'], 'blocks': ['GET'], 'blocks/{id}': ['GET'],
    'snapshots': ['POST'], 'rule-view': ['POST'], 'catalog': ['GET'],
    'points': ['GET'], 'points/{id}': ['GET'], 'excel': ['POST'], 'compare': ['POST'],
    'rulesets': ['GET', 'POST'], 'rulesets/{id}': ['GET', 'PUT', 'DELETE'],
    'rulesets/{id}/expand': ['GET'], 'rulesets/{id}/match': ['POST'],
    'maintenance/records': ['GET'], 'maintenance/check': ['POST'],
    'settings': ['GET', 'PUT'], 'settings/test': ['POST'],
    'settings/browser-login': ['GET', 'POST'], 'settings/browser-login/cancel': ['POST'],
}


def install_plan_convergence_routes(app, controller, runtime):
    service = None
    service_lock = threading.Lock()

    def get_service():
        nonlocal service
        with service_lock:
            if service is None:
                from .plan_convergence import PlanConvergenceService
                service = PlanConvergenceService(runtime.state_store, controller._get_ongoing)
            return service

    def dispatch(action, method, query, payload, admin):
        from . import plan_convergence_auth as auth
        from . import plan_convergence_rules as rules
        from . import plan_convergence_points as points

        current = get_service()
        ident = query.get('id')
        if action == 'bootstrap':
            return current.bootstrap(admin)
        if action == 'blocks':
            return current.blocks(query.get('refresh') == '1')
        if action == 'blocks/{id}':
            return current.block(ident)
        if action in {'snapshots', 'rule-view'}:
            return current.remote_rows(action, payload)
        if action == 'catalog':
            return current.catalog(query)
        if action == 'points':
            name = str(query.get('name') or '').strip()
            if not name or len(name) > 200:
                raise ValueError('请输入有效的设备名称')
            return points.points_by_inst_name(name)
        if action == 'points/{id}':
            return points.list_points(str(ident))
        if action == 'excel':
            return current.parse_excel(payload['content'], payload['filename'])
        if action == 'compare':
            return current.compare(payload)
        if action == 'rulesets':
            return rules.list_sets() if method == 'GET' else {'id': rules.create_set(payload.get('name'), payload.get('remark', ''))}
        if action.startswith('rulesets/{id}'):
            set_id = int(ident)
            saved = rules.get_set(set_id)
            if not saved:
                raise FileNotFoundError('规则集不存在')
            if action.endswith('/expand'):
                return current.expand(set_id)
            if action.endswith('/match'):
                return current.match(set_id, payload)
            if method == 'GET':
                return saved
            if method == 'DELETE':
                rules.delete_set(set_id)
                return {'deleted': True}
            if 'items' not in payload:
                raise ValueError('保存内容缺少规则项，已保留原规则集')
            rules.save_set(set_id, payload.get('name', saved['name']), payload.get('remark', saved['remark']), payload['items'])
            return rules.get_set(set_id)
        if action == 'maintenance/records':
            return current.maintenance_records()
        if action == 'maintenance/check':
            return current.maintenance_check(payload.get('record_id') or None)
        if action == 'settings':
            if method == 'PUT':
                auth.save_config(payload)
            return auth.settings_view()
        if action == 'settings/test':
            return auth.test_connection()
        if action == 'settings/browser-login':
            return current.browser_login.start() if method == 'POST' else current.browser_login.status()
        if action == 'settings/browser-login/cancel':
            return current.browser_login.cancel(payload.get('job_id'))
        raise ValueError('操作无效')

    async def body(request, maximum):
        content = bytearray()
        async for chunk in request.stream():
            if len(content) + len(chunk) > maximum:
                raise ValueError('上传内容过大，请拆分文件或规则项')
            content.extend(chunk)
        return bytes(content)

    async def endpoint(request: Request):
        try:
            session = await asyncio.to_thread(controller._current_session, request)
            if not session:
                return controller._auth_required_response()
            if session.get('is_guest'):
                return JSONResponse({'ok': False, 'error': '请登录后使用计划收敛审查'}, status_code=403)
            admin = bool(runtime.auth_manager.is_admin(session))
            action = request.scope['route'].path.removeprefix('/api/plan-convergence/')
            writing_rules = action.startswith('rulesets') and request.method != 'GET' and not action.endswith('/match')
            if (action.startswith('settings') or writing_rules) and not admin:
                return JSONResponse({'ok': False, 'error': '仅管理员可修改规则或平台设置'}, status_code=403)
            if request.method != 'GET':
                source = request.headers.get('origin') or request.headers.get('referer')
                expected = urlsplit(controller._request_base_url(request))
                actual = urlsplit(source) if source else None
                if request.headers.get('sec-fetch-site', '').lower() == 'cross-site' or (
                    actual and (actual.scheme, actual.netloc) != (expected.scheme, expected.netloc)
                ):
                    return JSONResponse({'ok': False, 'error': '不允许跨来源提交'}, status_code=403)
            query = {**dict(request.query_params), **request.path_params}
            payload = {}
            if action == 'excel':
                payload = {'content': await body(request, 8 * 1024 * 1024),
                           'filename': unquote(request.headers.get('x-filename', ''))}
            elif request.method != 'GET':
                payload = json.loads(await body(request, 2 * 1024 * 1024) or b'{}')
                if not isinstance(payload, dict):
                    raise ValueError('提交内容须为对象')
            data = await asyncio.to_thread(dispatch, action, request.method, query, payload, admin)
            return await asyncio.to_thread(controller._json_response, request, session, {'ok': True, 'data': data})
        except FileNotFoundError as exc:
            return JSONResponse({'ok': False, 'error': str(exc)}, status_code=404)
        except (ValueError, KeyError, TypeError, sqlite3.IntegrityError) as exc:
            return JSONResponse({'ok': False, 'error': str(exc)}, status_code=400)
        except requests.RequestException:
            return JSONResponse({'ok': False, 'error': '智航连接失败，请检查内网或 VPN 连接后重试'}, status_code=502)
        except Exception:
            logging.getLogger(__name__).exception('Plan-convergence request failed')
            return JSONResponse({'ok': False, 'error': '核对请求失败，请查看程序日志后重试'}, status_code=500)

    for path, methods in ROUTES.items():
        app.add_api_route('/api/plan-convergence/' + path, endpoint, methods=methods, name='plan_convergence_' + path)

    @app.get('/plan-convergence', include_in_schema=False)
    async def page(request: Request):
        session = await asyncio.to_thread(controller._current_session, request)
        if not session or session.get('is_guest'):
            target = request.url.path + ('?' + request.url.query if request.url.query else '')
            return Response(status_code=302, headers={'Location': '/api/auth/login?' + urlencode({'next': target})})
        from .server import portal_index_file
        return await asyncio.to_thread(controller._static_file_response, request, portal_index_file(), html=True)

    @app.get('/plan-convergence/legacy/{path:path}', include_in_schema=False)
    async def previous_page(path: str):
        tab = 'rules' if path == 'ruleset' else 'maintenance' if path == 'maintenance' else 'review'
        return Response(status_code=302, headers={'Location': '/plan-convergence?tab=' + tab})
