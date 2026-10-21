"""Bounded administrator sessions; never replace a person's credentials/session."""
import asyncio
import hashlib
import secrets
import time
import re
from urllib.parse import urlsplit

from fastapi import Request
from fastapi.responses import JSONResponse

from .account_policy import account_allowed
from .impersonation_context import CURRENT_IMPERSONATION, audit_metadata
from .personnel_password_login import PasswordLoginError
from .portal_auth import AUTH_COOKIE_NAME, BUILDING_OPEN_ID_MAP, REQUIRED_ADMIN_USERS

COOKIE = 'lan_portal_impersonation'
NS = 'admin_impersonation'
TTL = 20 * 60

# Reviewed business reads only. Permissions and record ownership still run in the original routes.
READ_ROUTES = re.compile(r'/api/(?:' + '|'.join((
    r'bootstrap|scope-overview|records|history-summary|source-refresh-status|link-directory',
    r'workbench(?:/(?:lite-fragment|lite-detail|closed|repair-event-candidates|repair-event-prefill|polling-delay-status|source-options|planned-notice-prefill))?',
    r'jobs/(?:recent|stream|batch|[a-f0-9]{32})|qt-active-items/stream',
    r'daily-tasks(?:/morning-meeting/(?:preview|download|print-model))?',
    r'engineer/mop/(?:bootstrap|preview)',
    r'capacity/water/(?:buildings|bootstrap|records(?:/[^/]+)?|images/[^/]+)',
    r'drills(?:/bootstrap|/[^/]+/(?:execution|preview|print-model|download))?',
    r'critical-guard/(?:bootstrap|weather-status|template|scope-template|tasks(?:/[^/]+(?:/download)?)?|source-files/[^/]+(?:/preview)?|images/[^/]+|workbooks/[^/]+)',
    r'events/(?:monthly|overview)',
    r'repair-management/(?:event-candidates|event-prefill|repair-candidates|cmdb-candidates|ledger-candidates|cmdb-cache/status|people|followups|followup-bind-candidates|overview|status|records(?:/[^/]+)?|health|failures|integrity|stream|changes|sync-status)',
    r'polling-sops(?:/[^/]+/attachments/[^/]+)?',
    r'notice-image-drafts|notice-images/[^/]+|notice-alert-tags|notice-undo/available|notice-memory',
    r'change-confirmations(?:/[^/]+/screenshot/preview)?',
    r'pre-shift-checks(?:/recipients|/history|/files/[^/]+)?',
    r'learning/(?:bootstrap|papers(?:/[^/]+)?|people|history|attempts|review|profile|issues|questions(?:/[^/]+)?|attachments/[^/]+|export|sharing(?:/[^/]+)?)',
    r'plan-convergence/(?:bootstrap|blocks(?:/[^/]+)?|catalog|points(?:/[^/]+)?|rulesets(?:/[^/]+(?:/expand)?)?|(?:maintenance|change)/(?:records|points))',
    r'risk-inspections(?:/people|/template|/(?:templates|base-templates)(?:/[^/]+)?|/[a-f0-9]{32}(?:/files/[^/]+)?)?',
    r'work-handovers(?:/people|/sources|/[a-f0-9]{32}|/files/[a-f0-9]{32})?',
    r'cabinet-power/(?:buildings|overview|rooms|racks|rooms/[^/]+/layout|operations(?:/[^/]+/(?:evidence|documents)/[^/]+)?|jobs/[^/]+|exports/[^/]+/download|export-batches/[^/]+|export-history|batches(?:/[^/]+(?:/status|/files/[^/]+|/images/[^/]+)?)?|writes(?:/[^/]+)?|bootstrap)',
)) + r')')


def fingerprint(value):
    return hashlib.sha256(str(value).encode()).hexdigest()


def draft_identity(session):
    grant = (session or {}).get('impersonation')
    return 'impersonation:' + grant['id'] if grant else str((session.get('user') or {}).get('open_id') or '')


def can_impersonate(auth, operator_id, target_id):
    if not operator_id or not target_id or operator_id == target_id or target_id in REQUIRED_ADMIN_USERS:
        return False
    operator, target = auth.account_profile(operator_id), auth.account_profile(target_id)
    if not operator.get('can_manage_accounts') or not target.get('modules') or not auth.scopes_for_open_id(target_id):
        return False
    if auth.directory_block_reason(target_id) or auth._open_id_explicitly_disabled(target_id):
        return False
    if target_id not in BUILDING_OPEN_ID_MAP.values() and target_id not in REQUIRED_ADMIN_USERS:
        person = auth.directory_person(target_id)
        if not person.get('selectable') or person.get('inactive') or person.get('login_ids') != [target_id]:
            return False
    if operator.get('is_fixed_superuser'):
        return True
    candidate = {**target, 'own_scopes': [target['home_scope']] if target.get('home_scope') else []}
    # L2's edit-only personnel selector is not permission to view another account.
    return (account_allowed(operator, candidate, 'view') and account_allowed(operator, candidate, 'edit')
            and set(target['scope_rules']).issubset(operator['scope_rules'])
            and set(target['modules']).issubset(operator['modules']))


def bind_job(store, grant, job_id):
    key = str(job_id)
    value = {'grant_id': grant['id'], 'target_open_id': grant['target_open_id']}
    with store._lock:
        old = store.get_document(NS + '_jobs', key)
        if old and old != value:
            raise PasswordLoginError('任务已有其他代办身份，未重新提交。', 409)
        store.put_document(NS + '_jobs', key, value)


def resume_job(auth, store, request, job_id):
    grant_id = request.get('_admin_impersonation_id')
    if not grant_id:
        return None
    grant = store.get_document(NS, grant_id) or {}
    bound = store.get_document(NS + '_jobs', str(job_id)) or {}
    if (bound.get('grant_id') != grant_id or bound.get('target_open_id') != request.get('_auth_open_id')
            or grant.get('target_open_id') != request.get('_auth_open_id')
            or not can_impersonate(auth, grant.get('operator_open_id'), grant.get('target_open_id'))):
        raise PasswordLoginError('代办任务权限已变化，未继续执行，请以本人账号核对原任务。', 403)
    session = {'user': {'open_id': grant['target_open_id']}}
    from .module_access import require_module
    require_module(auth, session, request.get('work_type') or 'maintenance')
    if not auth.scope_allowed(session, request.get('scope') or 'ALL'):
        raise PasswordLoginError('代办任务楼栋权限已变化，未继续执行。', 403)
    # Leaving the UI does not cancel an already accepted, individually bound task.
    return grant


def current(auth, store, parent_id, token):
    parent = auth.get_session(parent_id)
    grant = store.get_document(NS, fingerprint(token)) or {}
    if not parent or not grant or grant.get('parent_hash') != fingerprint(parent_id):
        return parent, {}, None
    valid = (not grant.get('ended_at') and time.time() < grant['expires_at']
             and grant['operator_open_id'] == (parent.get('user') or {}).get('open_id')
             and can_impersonate(auth, grant['operator_open_id'], grant['target_open_id']))
    if not valid:
        return parent, grant, None
    target = auth.directory_person(grant['target_open_id'])
    session = {'source': 'admin_impersonation', 'created_at_ts': grant['created_at'],
               'expires_at': grant['expires_at'], 'allowed_scopes': auth.scopes_for_open_id(grant['target_open_id']),
               'role': auth.role_for_open_id(grant['target_open_id']),
               'user': {'open_id': grant['target_open_id'], 'name': target.get('name') or grant['target_name'],
                        'employee_no': target.get('employee_no', ''), 'personnel_record_id': target.get('id', '')},
               'impersonation': {'id': grant['id'], 'operator_name': grant['operator_name'], 'target_name': target.get('name') or grant['target_name'],
                                 'expires_at': grant['expires_at'], 'active': True}}
    return parent, grant, session


def response(data=None, error='', status=200):
    return JSONResponse({'ok': not error, **({'error': error} if error else {'data': data})},
                        status_code=status, headers={'Cache-Control': 'no-store'})


def check_origin(request, controller):
    origin = urlsplit(request.headers.get('origin') or request.headers.get('referer') or '')
    base = urlsplit(controller._request_base_url(request))
    if request.headers.get('sec-fetch-site') == 'cross-site' or (origin.scheme, origin.netloc) != (base.scheme, base.netloc):
        raise PasswordLoginError('不允许跨来源进行代登录操作。', 403)


def request_allowed(path, method):
    if path == '/api/auth/status' or path == '/api/health':
        return True
    if path == '/api/workbench/draft':
        return method in {'GET', 'HEAD', 'PUT', 'DELETE'}
    if path == '/workbench-lite/parse':
        return method == 'POST'
    if path.startswith('/api/workspace-layouts/'):
        return method in {'GET', 'HEAD'}
    if not path.startswith('/api/'):
        return method in {'GET', 'HEAD'} and not path.startswith(('/signature', '/polling-work-order', '/handover'))
    # Also covers token-only routes which otherwise bypass session checks.
    if path.startswith(('/api/auth/', '/api/admin', '/api/backend', '/api/qt/', '/api/assistant',
                        '/api/signatures', '/api/polling-work-orders', '/api/handover', '/api/message-delivery',
                        '/api/feishu-workpartner', '/api/workspace-layout')):
        return False
    if method in {'GET', 'HEAD'}:
        return bool(READ_ROUTES.fullmatch(path))
    # Explicit business edits only; new endpoints never inherit impersonation access.
    if path in {'/api/workbench-actions', '/api/maintenance-actions', '/api/notice-attachments',
                '/api/notice-target-candidates', '/api/change-target-candidates', '/api/notice-identity/bind',
                '/api/workbench/repair-event-bind',
                '/api/capacity/water/uploads', '/api/capacity/water/records',
                '/api/repair-management/records', '/api/repair-management/followups',
                '/api/repair-management/followups/bind', '/api/repair-management/prefill'}:
        return method == 'POST'
    return bool(re.fullmatch(r'/api/repair-management/(?:records|followups)/[^/]+', path) and method == 'PUT'
                or re.fullmatch(r'/api/capacity/water/records/[^/]+', path) and method == 'PATCH')


class ImpersonationMiddleware:
    def __init__(self, app, controller, runtime):
        self.app, self.controller, self.runtime = app, controller, runtime

    async def __call__(self, scope, receive, send):
        if scope['type'] != 'http':
            return await self.app(scope, receive, send)
        request = Request(scope, receive)
        token = request.cookies.get(COOKIE)
        path = request.url.path
        mutating = request.method not in {'GET', 'HEAD', 'OPTIONS'}
        expected = request.headers.get('x-clipflow-impersonation', '')
        context_check = path.startswith('/api/') and path not in {'/api/auth/status', '/api/auth/impersonation/stop'}
        if context_check and not token and expected:
            return await response(error='页面登录身份已变化，请刷新页面后再操作。', status=409)(scope, receive, send)
        if not token or path == '/api/auth/impersonation/stop' or path.startswith('/assets/') or path == '/favicon.ico':
            return await self.app(scope, receive, send)
        auth, store = self.runtime.auth_manager, self.runtime.state_store
        parent_id = request.cookies.get(AUTH_COOKIE_NAME, '')
        try:
            parent, grant, session = await asyncio.to_thread(current, auth, store, parent_id, token)
            scope.setdefault('state', {})['admin_impersonation_session'] = session
            if session is None:
                if path == '/api/auth/status':
                    data = {'logged_in': bool(parent), 'scope_options': [], 'allowed_scopes': [], 'default_scope': '',
                            'user': {'open_id': grant.get('target_open_id', ''), 'name': grant.get('target_name', ''), 'modules': [],
                                     'impersonation': {'id': grant.get('id', ''), 'operator_name': (parent or {}).get('user', {}).get('name', ''),
                                         'target_name': grant.get('target_name', ''), 'expires_at': grant.get('expires_at', 0),
                                         'active': False, 'reason': '代登录已到期或权限已变化，请退出代登录。'}}}
                    return await response(data)(scope, receive, send)
                return await response(error='代登录已到期或权限已变化，请退出代登录后重试。', status=403)(scope, receive, send)
            if not request_allowed(path, request.method):
                return await response(error='代登录不开放此操作或私人资料，请退出代登录后以本人账号办理。', status=403)(scope, receive, send)
            if context_check and 'x-clipflow-impersonation' in request.headers and expected != grant['id']:
                raise PasswordLoginError('页面登录身份已变化，请刷新页面后再操作。', 409)
            if mutating:
                if expected != grant['id']:
                    raise PasswordLoginError('页面登录身份已变化，请刷新页面后再操作。', 409)
                check_origin(request, self.controller)
        except PasswordLoginError as exc:
            return await response(error=str(exc), status=exc.status)(scope, receive, send)
        except Exception:
            return await response(error='代登录身份校验暂未完成，未执行操作，请稍后重试或退出代登录。', status=503)(scope, receive, send)
        context = CURRENT_IMPERSONATION.set(grant)
        audit, http_status = None, 500
        last_check = time.monotonic()

        async def guarded_send(message):
            nonlocal http_status, last_check
            if message['type'] == 'http.response.start':
                http_status = message['status']
            if message['type'] == 'http.response.body' and not mutating and (time.time() >= grant['expires_at'] or time.monotonic() - last_check >= 5):
                last_check = time.monotonic()
                _, _, live = await asyncio.to_thread(current, auth, store, parent_id, token)
                if live is None:
                    raise PermissionError('Impersonation ended during streaming')
            await send(message)
        try:
            if mutating:
                # Persist attribution before any side effect; audit failure fails closed.
                try:
                    audit = await asyncio.to_thread(store.record_business_operation_audit,
                        domain='accounts', action='impersonation_request', status='started',
                        actor_open_id=grant['operator_open_id'], actor_name=grant['operator_name'],
                        metadata={'path': path, 'method': request.method, 'target_open_id': grant['target_open_id']})
                except Exception:
                    return await response(error='代办审计暂未保存，未执行操作，请稍后重试。', status=503)(scope, receive, send)
            await self.app(scope, receive, guarded_send)
        finally:
            try:
                if audit:
                    await asyncio.to_thread(store.record_business_operation_audit, audit_id=audit['audit_id'],
                        status='request_finished', metadata={'http_status': http_status})
            finally:
                CURRENT_IMPERSONATION.reset(context)


def install(app, controller, runtime, get_directory):
    app.add_middleware(ImpersonationMiddleware, controller=controller, runtime=runtime)

    @app.post('/api/auth/accounts/{record_id}/impersonate')
    async def start(request: Request):
        from .account_management import AccountManagement
        try:
            check_origin(request, controller)
            if request.cookies.get(COOKIE):
                raise PasswordLoginError('请先退出当前代登录，不能连续嵌套代登录。', 409)
            if await controller._read_json_request(request, max_bytes=1024) != {}:
                raise PasswordLoginError('代登录参数无效。')
            auth, store = runtime.auth_manager, runtime.state_store
            parent_id = request.cookies.get(AUTH_COOKIE_NAME, '')
            parent = await asyncio.to_thread(auth.get_session, parent_id)
            if not parent or parent.get('source') == 'admin_impersonation':
                raise PasswordLoginError('请先以本人管理员账号登录。', 401)
            operator = parent['user']
            if not auth.account_profile(operator.get('open_id'))['can_manage_accounts']:
                raise PasswordLoginError('当前账号没有人员管理权限。', 403)
            directory = await asyncio.to_thread(get_directory)
            listing = await asyncio.to_thread(AccountManagement(directory).accounts, False, operator)
            target = next((item for item in listing['items'] if item['id'] == request.path_params['record_id']), None)
            if not target or not can_impersonate(auth, operator['open_id'], target['open_id']):
                raise PasswordLoginError('无权代登录此账号；仅支持管理范围内的启用账号。', 403)
            token = secrets.token_urlsafe(32)
            now = time.time()
            grant = {'id': fingerprint(token), 'parent_hash': fingerprint(parent_id),
                     'operator_open_id': operator['open_id'], 'operator_name': operator.get('name', ''),
                     'target_open_id': target['open_id'], 'target_name': target['name'], 'record_id': target['id'],
                     'created_at': now, 'expires_at': now + TTL}
            audit = await asyncio.to_thread(store.record_business_operation_audit, domain='accounts', action='impersonation_start',
                status='started', actor_open_id=operator['open_id'], actor_name=operator.get('name', ''),
                target_record_id=target['id'], metadata={'impersonation': audit_metadata(grant), 'target_open_id': target['open_id']})
            await asyncio.to_thread(store.put_document, NS, grant['id'], grant)
            await asyncio.to_thread(store.record_business_operation_audit, audit_id=audit['audit_id'], status='success')
            result = response({'redirect_url': '/'})
            # Keep the expired marker until explicit exit so stale tabs cannot fall back to admin privileges.
            result.set_cookie(COOKIE, token, httponly=True, samesite='strict', secure=request.url.scheme == 'https', path='/')
            return result
        except PasswordLoginError as exc:
            return response(error=str(exc), status=exc.status)
        except Exception:
            return response(error='代登录未建立，原账号保持不变，请稍后重试。', status=503)

    @app.post('/api/auth/impersonation/stop')
    async def stop(request: Request):
        try:
            check_origin(request, controller)
            if await controller._read_json_request(request, max_bytes=1024) != {}:
                raise PasswordLoginError('退出代登录参数无效。')
            token = request.cookies.get(COOKIE, '')
            store = runtime.state_store
            grant = await asyncio.to_thread(store.get_document, NS, fingerprint(token)) if token else None
            expected = request.headers.get('x-clipflow-impersonation', '')
            if grant and expected and expected != grant['id']:
                raise PasswordLoginError('页面登录身份已变化，请刷新页面后再退出。', 409)
            if grant and grant.get('parent_hash') == fingerprint(request.cookies.get(AUTH_COOKIE_NAME, '')):
                with runtime.auth_manager._lock:
                    grant = {**grant, 'ended_at': grant.get('ended_at') or time.time()}
                    store.put_document(NS, grant['id'], grant)
                await asyncio.to_thread(store.record_business_operation_audit,
                    audit_id='impersonation-stop:' + grant['id'], domain='accounts', action='impersonation_stop', status='success',
                    actor_open_id=grant['operator_open_id'], actor_name=grant['operator_name'], target_record_id=grant['record_id'],
                    metadata={'impersonation': audit_metadata(grant), 'target_open_id': grant['target_open_id']})
            result = response({'redirect_url': '/?admin=accounts'})
            result.delete_cookie(COOKIE, path='/', httponly=True, samesite='strict', secure=request.url.scheme == 'https')
            return result
        except PasswordLoginError as exc:
            return response(error=str(exc), status=exc.status)
        except Exception:
            return response(error='退出代登录暂未完成，请重试；未恢复管理员操作。', status=503)
