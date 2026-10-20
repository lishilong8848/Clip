"""Independent impersonation boundary regressions, with SQLite/ASGI only."""
import ast
import asyncio
import copy
import json
import socket
import time
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

from .test_personnel_password_login import (
    PersonnelLoginHarness, _TempDirTestCase, _page, _user,
)
from lan_bitable_template_portal import account_impersonation as imp
from lan_bitable_template_portal.account_management import AccountManagement
from lan_bitable_template_portal.account_policy import account_allowed
from lan_bitable_template_portal.impersonation_context import CURRENT_IMPERSONATION
from lan_bitable_template_portal.module_access import require_module
from lan_bitable_template_portal.personnel_password_login import PasswordLoginError
from lan_bitable_template_portal.portal_auth import (
    AUTH_COOKIE_NAME, AUTH_SESSION_TTL_SECONDS, BUILDING_OPEN_ID_MAP, LI_SHILONG_OPEN_ID,
)
from lan_bitable_template_portal.portal_service import PortalError
from public_access.config import Settings
from public_access.ingress import MARKER, PublicGuard


ORIGIN = 'https://impersonation.example'
MARKER_TOKEN = 'fixture-ingress-marker-not-a-credential'
HEADER = 'X-Clipflow-Impersonation'
MAIN_PATH = Path(__file__).parent / 'clipflow_backend' / 'main.py'
MAIN_AST = ast.parse(MAIN_PATH.read_text(encoding='utf-8-sig'))


def controller_from_current_source(runtime):
    """Compile just these real methods, without importing/starting PortalRuntime."""
    cls = next(node for node in MAIN_AST.body if isinstance(node, ast.ClassDef)
               and node.name == 'FastAPIPortalController')
    names = {'_current_session', '_workbench_draft_identity'}
    body = [copy.deepcopy(node) for node in cls.body if isinstance(node, ast.FunctionDef) and node.name in names]
    if {node.name for node in body} != names:
        raise AssertionError('Current controller must expose the session and draft identity helpers')
    extracted = ast.Module(body=[ast.ClassDef(name='Controller', bases=[], keywords=[], body=body,
                                            decorator_list=[])], type_ignores=[])
    namespace = {'copy': copy, 'Request': Request, 'PortalRuntime': runtime, 'AUTH_COOKIE_NAME': AUTH_COOKIE_NAME}
    exec(compile(ast.fix_missing_locations(extracted), str(MAIN_PATH), 'exec'), namespace)
    controller = namespace['Controller']()
    controller._request_base_url = lambda request: str(request.base_url).rstrip('/')

    async def read_json(request, *, max_bytes):
        body = await request.body()
        if len(body) > max_bytes:
            raise PasswordLoginError('Request too large', 413)
        try:
            return json.loads(body)
        except ValueError:
            raise PasswordLoginError('Invalid JSON', 400) from None
    controller._read_json_request = read_json
    return controller


class AdminImpersonationTests(_TempDirTestCase, unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.enterContext(patch('lan_bitable_template_portal.portal_auth.config',
            SimpleNamespace(app_id='cli_fixture_impersonation', app_secret='synthetic-not-real')))
        self.enterContext(patch.object(socket.socket, 'connect', side_effect=AssertionError('No real network')))
        self.enterContext(patch('requests.Session.request', side_effect=AssertionError('No Feishu network')))
        self.h = PersonnelLoginHarness(self.root)
        self.addCleanup(self.h.close)
        self.auth, self.store = self.h.auth, self.h.auth._state_store
        self.ids = {'l0': LI_SHILONG_OPEN_ID, 'h': BUILDING_OPEN_ID_MAP['H'],
                    'a_duty': BUILDING_OPEN_ID_MAP['A']}
        self.rids = {}
        specs = [('manager', 'H', '经理'), ('manager2', 'H', '经理'), ('engineer_h', 'H', '电气工程师'),
                 ('supervisor', 'A', '主管'), ('supervisor_b', 'B', '主管'),
                 ('a', 'A', '运维值班员'), ('b', 'B', '电气工程师')]
        for key, scope, position in specs:
            identity = 'ou_fixtureimp' + key.replace('_', '')
            self.ids[key] = identity
            self.rids[key] = 'rec' + key.replace('_', '')
            self.h.service._records[self.rids[key]] = {'姓名': 'fixture-' + key, '机楼/专业': scope + '楼',
                '体系岗位': position, '员工姓名': _user('fixture-' + key, identity), '账号性质': '外部账号',
                '员工工号': 'TEST-' + key, '密码': 'synthetic-password-hash-never-used'}
        self.h.service._pages = [_page([{'record_id': rid, 'fields': fields}
            for rid, fields in self.h.service._records.items()])]
        self.h.login.directory(force=True)
        self.auth.sync_directory_permissions(list(self.h.login.snapshot['people'].values()), on_login=True)
        self.configure('a', modules=['maintenance', 'repair_management'])
        self.configure('b', modules=['maintenance', 'repair_management'])
        self.runtime = SimpleNamespace(auth_manager=self.auth, state_store=self.store)
        self.controller = controller_from_current_source(self.runtime)
        self.ops = []
        self.slow_hook = None
        self.app = self.make_app()
        self.parent = self.session('l0')
        self.client = self.client_for(self.parent)

    def configure(self, key, **changes):
        identity = self.ids.get(key, key)
        profile = self.auth.account_profile(identity)
        with self.auth._lock:
            permissions = self.auth._load_permissions_locked()
            old = permissions['users'].get(identity, {})
            permissions['users'][identity] = {**old, 'enabled': True, 'access_version': 1,
                'level': profile['level'], 'category': profile['category'], 'scopes': profile['scope_rules'],
                'modules': profile['modules'], **changes}
            self.auth._save_permissions_locked(permissions)

    def roster_change(self, key, **changes):
        snapshot = copy.deepcopy(self.h.login.snapshot)
        snapshot['people'][self.rids[key]].update(changes)
        self.h.login.snapshot = snapshot

    def session(self, key):
        identity = self.ids[key]
        now = time.time()
        token = 'fixture-parent-' + uuid.uuid4().hex
        session = {'source': 'feishu', 'user': {'open_id': identity, 'name': 'fixture-' + key},
                   'created_at_ts': now, 'expires_at': now + AUTH_SESSION_TTL_SECONDS,
                   'allowed_scopes': self.auth.scopes_for_open_id(identity),
                   'role': self.auth.role_for_open_id(identity)}
        self.auth._sessions[token] = copy.deepcopy(session)
        self.store.put_auth_session(self.auth._secret_hash(token), session)
        return token

    def client_for(self, parent=None, *, public=False, raise_errors=True):
        app = self.app
        headers = {'Origin': ORIGIN}
        if public:
            settings = Settings(enabled=True, public_url=ORIGIN, server_addr='192.0.2.2', token='fixture-only')
            app = PublicGuard(app, MARKER_TOKEN, settings, self.controller._current_session)
            headers[MARKER.decode()] = MARKER_TOKEN
        client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app, client=('127.0.0.1', 1),
            raise_app_exceptions=raise_errors), base_url=ORIGIN, headers=headers)
        if parent:
            client.cookies.set(AUTH_COOKIE_NAME, parent)
        self.addAsyncCleanup(client.aclose)
        return client

    def make_app(self):
        app = FastAPI()
        imp.install(app, self.controller, self.runtime, lambda: self.h.login)

        @app.get('/api/auth/status')
        async def status(request: Request):
            session = self.controller._current_session(request)
            return {'ok': True, 'data': {'logged_in': bool(session), 'user': self.auth.public_user(session)}}

        @app.get('/api/workbench')
        @app.get('/api/repair-management/records')
        async def read_business(request: Request):
            session = self.controller._current_session(request)
            if self.slow_hook:
                self.slow_hook()
            return {'ok': True, 'actor': (session or {}).get('user', {}).get('open_id')}

        async def mutation(request: Request):
            session = self.controller._current_session(request)
            if not session:
                return JSONResponse({'ok': False}, status_code=401)
            body = await request.json()
            module = 'repair_management' if '/repair-management/' in request.url.path else body.get('work_type', 'maintenance')
            try:
                require_module(self.auth, session, module)
            except PortalError:
                return JSONResponse({'ok': False}, status_code=403)
            if not self.auth.scope_allowed(session, body.get('scope', 'A')):
                return JSONResponse({'ok': False}, status_code=403)
            if self.slow_hook:
                self.slow_hook()
            actor = session['user']['open_id']
            audit = self.store.record_business_operation_audit(domain='fixture_business', action='fixture', status='success',
                actor_open_id=actor, actor_name=session['user']['name'], metadata={'fixture': True})
            self.ops.append({'actor': actor, 'grant': copy.deepcopy(CURRENT_IMPERSONATION.get()), 'audit': audit})
            return {'ok': True, 'actor': actor}

        for path in ('/api/workbench-actions', '/api/maintenance-actions', '/api/repair-management/records',
                     '/api/repair-management/followups'):
            app.add_api_route(path, mutation, methods=['POST'])

        @app.api_route('/api/workbench/draft', methods=['GET', 'PUT', 'DELETE'])
        async def draft(request: Request):
            session = self.controller._current_session(request)
            owner = self.controller._workbench_draft_identity(session)
            if request.method == 'PUT':
                self.store.put_document('fixture_drafts', owner, await request.json())
            elif request.method == 'DELETE':
                self.store.delete_document('fixture_drafts', owner)
            return {'ok': True, 'owner': owner, 'data': self.store.get_document('fixture_drafts', owner)}

        @app.get('/api/learning/history')
        async def learning_history(request: Request):
            session = self.controller._current_session(request)
            if not session:
                return JSONResponse({'ok': False}, status_code=401)
            try:
                require_module(self.auth, session, 'learning')
            except PortalError:
                return JSONResponse({'ok': False}, status_code=403)
            scope = request.query_params.get('scope', 'A')
            if not self.auth.scope_allowed(session, scope):
                return JSONResponse({'ok': False}, status_code=403)
            return {'ok': True, 'actor': session['user']['open_id'], 'scope': scope, 'items': []}

        async def sentinel(request: Request):
            self.ops.append({'forbidden_path': request.url.path})
            return {'should_never_reach': True}
        for path in ('/api/assistant/conversation', '/api/assistant/files/private-id', '/api/assistant/settings',
                     '/api/signatures/management/request', '/api/signatures/management/submit', '/signature',
                     '/api/signatures/usage-confirm', '/api/polling-work-orders/session',
                     '/api/polling-work-orders/confirm', '/api/change-confirmations/record/confirm',
                     '/api/learning/papers/fixture/answer', '/api/learning/answers',
                     '/api/auth/permissions', '/api/auth/accounts', '/api/auth/password/change',
                     '/api/auth/logout', '/api/admin/test', '/api/qt/test', '/api/unreviewed-private',
                     '/api/repair-management/unreviewed-private'):
            app.add_api_route(path, sentinel, methods=['GET', 'POST', 'PUT', 'DELETE'])

        @app.get('/api/repair-management/stream')
        async def stream():
            hook = self.slow_hook
            class Stream(Response):
                async def __call__(self, scope, receive, send):
                    await send({'type': 'http.response.start', 'status': 200,
                                'headers': [(b'content-type', b'text/event-stream')]})
                    await send({'type': 'http.response.body', 'body': b'data: first\n\n', 'more_body': True})
                    if hook:
                        hook()
                    await send({'type': 'http.response.body', 'body': b'data: forbidden-after-revoke\n\n', 'more_body': False})
            return Stream()
        return app

    def record_id(self, key):
        return 'account:' + self.ids[key] if key in {'l0', 'h', 'a_duty'} else self.rids[key]

    async def start(self, key='a', client=None, *, expected=200):
        client = client or self.client
        result = await client.post('/api/auth/accounts/' + self.record_id(key) + '/impersonate', json={})
        self.assertEqual(result.status_code, expected, result.text)
        if expected != 200:
            return None
        status = await client.get('/api/auth/status')
        self.assertEqual(status.status_code, 200, status.text)
        user = status.json()['data']['user']
        self.assertEqual(user['open_id'], self.ids[key])
        grant_id = user['impersonation']['id']
        self.assertEqual(grant_id, imp.fingerprint(client.cookies.get(imp.COOKIE)))
        return grant_id

    async def post(self, grant_id, *, client=None, path='/api/workbench-actions', **body):
        return await (client or self.client).post(path, json={'scope': 'A', 'work_type': 'maintenance', **body},
                                                headers={HEADER: grant_id})

    def grant(self, client=None):
        return self.store.get_document(imp.NS, imp.fingerprint((client or self.client).cookies.get(imp.COOKIE)))

    async def test_l0_can_enter_normal_and_fixed_h_duty_without_replacing_parent_cookie(self):
        for key in ('a', 'h'):
            with self.subTest(target=key):
                client = self.client_for(self.parent)
                grant_id = await self.start(key, client)
                self.assertEqual(client.cookies.get(AUTH_COOKIE_NAME), self.parent)
                grant = self.grant(client)
                self.assertEqual(grant['expires_at'] - grant['created_at'], 1200)
                self.assertEqual(grant_id, grant['id'])
                self.assertIsNone(CURRENT_IMPERSONATION.get())

    async def test_l1_cannot_enter_peer_or_l0_but_can_enter_lower_level(self):
        client = self.client_for(self.session('manager'))
        self.assertEqual(self.auth.account_profile(self.ids['manager'])['level'], 'L1')
        for key in ('l0', 'h', 'manager2'):
            await self.start(key, client, expected=403)
        await self.start('a', client)

    async def test_l3_requires_lower_level_home_and_full_target_scope_module_subset(self):
        parent = self.session('supervisor')
        await self.start('a', self.client_for(parent))
        for key in ('b', 'supervisor_b', 'manager'):
            await self.start(key, self.client_for(parent), expected=403)
        self.configure('a', scopes=['A', 'B'])
        await self.start('a', self.client_for(parent), expected=403)
        self.configure('a', scopes=['A'])
        self.configure('supervisor', modules=['accounts', 'maintenance'])
        await self.start('a', self.client_for(parent), expected=403)

    async def test_l2_cross_building_edit_does_not_grant_impersonation_read(self):
        self.configure('a', category='engineer')
        target = self.auth.account_profile(self.ids['a'])
        operator = self.auth.account_profile(self.ids['engineer_h'])
        self.assertTrue(account_allowed(operator, target, 'edit'))
        self.assertFalse(account_allowed(operator, target, 'view'))
        await self.start('a', self.client_for(self.session('engineer_h')), expected=403)

    async def test_disabled_departed_and_ambiguous_target_are_denied(self):
        client = self.client_for(self.parent)
        self.configure('a', enabled=False)
        await self.start('a', client, expected=403)
        self.configure('a', enabled=True)
        self.roster_change('a', inactive=True, selectable=False)
        await self.start('a', client, expected=403)
        self.roster_change('a', inactive=False, selectable=True, login_ids=[self.ids['a'], 'ou_fixture_other_identity'])
        await self.start('a', client, expected=403)

    async def test_original_admin_loses_accounts_module_immediately(self):
        client = self.client_for(self.session('manager'))
        grant_id = await self.start('a', client)
        self.configure('manager', modules=['maintenance'])
        self.assertEqual((await self.post(grant_id, client=client)).status_code, 403)
        self.assertEqual(self.ops, [])

    async def test_target_module_revocation_is_enforced_without_relogin(self):
        client = self.client_for(self.session('supervisor'))
        grant_id = await self.start('a', client)
        self.configure('a', modules=[])
        self.assertEqual((await self.post(grant_id, client=client)).status_code, 403)
        self.assertEqual(self.ops, [])

    async def test_partial_module_and_scope_revocation_cannot_use_the_previous_overlay(self):
        grant_id = await self.start()
        self.configure('a', modules=['maintenance'])
        result = await self.post(grant_id, path='/api/repair-management/followups')
        self.assertEqual(result.status_code, 403)
        self.configure('a', scopes=['B'])
        self.assertEqual((await self.post(grant_id)).status_code, 403)
        self.assertEqual(self.ops, [])

    async def test_existing_child_stops_on_departure_disable_or_identity_ambiguity(self):
        grant_id = await self.start()
        baseline = copy.deepcopy(self.h.login.snapshot)
        for changes in ({'inactive': True, 'selectable': False},
                        {'login_ids': [self.ids['a'], 'ou_fixture_ambiguous']}):
            self.roster_change('a', **changes)
            self.assertEqual((await self.post(grant_id)).status_code, 403)
            self.h.login.snapshot = copy.deepcopy(baseline)
        self.configure('a', enabled=False)
        self.assertEqual((await self.post(grant_id)).status_code, 403)
        self.assertEqual(self.ops, [])

    async def test_personal_session_password_permissions_and_history_remain_unchanged(self):
        person = self.h.login.snapshot['people'][self.rids['a']]
        personal_token = self.auth.create_personnel_session(person)
        personal_before = self.store.get_auth_session(self.auth._secret_hash(personal_token), now=time.time())
        permissions = copy.deepcopy(self.auth._load_permissions_locked())
        records = copy.deepcopy(self.h.service._records)
        self.store.put_document('lighthouse_assistant', 'conversation:' + self.ids['a'], {'private': 'personal-history'})
        grant_id = await self.start()
        self.assertEqual((await self.post(grant_id)).status_code, 200)
        self.assertEqual((await self.client.post('/api/auth/impersonation/stop', json={})).status_code, 200)
        self.assertEqual(self.store.get_auth_session(self.auth._secret_hash(personal_token), now=time.time()), personal_before)
        self.assertEqual(self.auth._load_permissions_locked(), permissions)
        self.assertEqual(self.h.service._records, records)
        self.assertEqual(self.store.get_document('lighthouse_assistant', 'conversation:' + self.ids['a']),
                         {'private': 'personal-history'})
        self.assertEqual(self.h.service._patch_calls, 0)

    async def test_absolute_expiry_never_slides_or_falls_back_to_admin(self):
        grant_id = await self.start()
        grant = self.grant()
        before = grant['expires_at']
        self.assertEqual((await self.client.get('/api/workbench')).status_code, 200)
        self.assertEqual(self.grant()['expires_at'], before)
        grant['expires_at'] = time.time() - 1
        self.store.put_document(imp.NS, grant['id'], grant)
        self.assertEqual((await self.client.get('/api/workbench')).status_code, 403)
        self.assertEqual((await self.post(grant_id)).status_code, 403)
        status = (await self.client.get('/api/auth/status')).json()['data']
        self.assertFalse(status['user']['impersonation']['active'])
        self.assertNotEqual(status['user']['open_id'], self.ids['l0'])
        self.assertEqual(self.ops, [])

    async def test_stop_restores_parent_and_parent_logout_invalidates_child(self):
        grant_id = await self.start()
        child = self.client.cookies.get(imp.COOKIE)
        self.assertEqual((await self.client.post('/api/auth/impersonation/stop', json={})).status_code, 200)
        status = (await self.client.get('/api/auth/status')).json()['data']
        self.assertEqual(status['user']['open_id'], self.ids['l0'])
        self.assertIsNone(self.client.cookies.get(imp.COOKIE))
        self.client.cookies.set(imp.COOKIE, child)
        self.assertEqual((await self.post(grant_id)).status_code, 403)
        self.auth.clear_session(self.parent)
        self.assertEqual((await self.client.get('/api/workbench')).status_code, 403)

    async def test_two_admins_parallel_requests_have_independent_overlay_and_audit(self):
        other = self.client_for(self.session('manager'))
        ids = await asyncio.gather(self.start('a'), self.start('b', other))
        replies = await asyncio.gather(self.post(ids[0]), self.post(ids[1], client=other, scope='B'))
        self.assertEqual([reply.status_code for reply in replies], [200, 200])
        self.assertEqual({row['actor'] for row in self.ops}, {self.ids['a'], self.ids['b']})
        by_target = {row['actor']: row['audit'] for row in self.ops}
        self.assertEqual(by_target[self.ids['a']]['actor_open_id'], self.ids['l0'])
        self.assertEqual(by_target[self.ids['b']]['actor_open_id'], self.ids['manager'])
        self.assertNotEqual(ids[0], ids[1])
        self.assertIsNone(CURRENT_IMPERSONATION.get())

    async def test_parent_logout_invalidates_an_active_child_before_any_stop(self):
        grant_id = await self.start()
        self.auth.clear_session(self.parent)
        self.assertEqual((await self.client.get('/api/workbench')).status_code, 403)
        self.assertEqual((await self.post(grant_id)).status_code, 403)
        self.assertEqual(self.ops, [])

    async def test_csrf_nested_and_forged_start_body_are_rejected(self):
        path = '/api/auth/accounts/reca/impersonate'
        for body in ({'open_id': self.ids['a']}, {'role': 'admin'}, {'scope': 'ALL'}, {'_auth_open_id': self.ids['l0']}, []):
            result = await self.client.post(path, json=body)
            self.assertGreaterEqual(result.status_code, 400, result.text)
        for headers in ({'Origin': 'https://evil.example'}, {'Origin': '', 'Referer': ''},
                        {'Sec-Fetch-Site': 'cross-site'}):
            self.assertEqual((await self.client.post(path, json={}, headers=headers)).status_code, 403)
        await self.start()
        self.assertGreaterEqual((await self.client.post(path, json={})).status_code, 400)

    async def test_stale_admin_tab_requires_matching_hash_when_child_cookie_exists(self):
        grant_id = await self.start()
        for header in ({}, {HEADER: 'forged'}, {HEADER: self.ids['l0']}):
            result = await self.client.post('/api/workbench-actions', json={'scope': 'A'}, headers=header)
            self.assertEqual(result.status_code, 409, result.text)
        self.assertEqual(self.ops, [])
        self.assertEqual((await self.post(grant_id)).status_code, 200)

    async def test_stale_child_tab_with_hash_but_no_cookie_cannot_submit_as_admin(self):
        grant_id = await self.start()
        await self.client.post('/api/auth/impersonation/stop', json={})
        self.assertEqual((await self.post(grant_id)).status_code, 409)
        self.assertEqual(self.ops, [])

    async def test_stale_get_wrong_or_empty_header_cannot_read_active_target_but_status_can(self):
        for public in (False, True):
            with self.subTest(public=public):
                client = self.client_for(self.parent, public=public)
                grant_id = await self.start('a', client)
                for expected in ('old-grant-id', ''):
                    response = await client.get('/api/workbench', headers={HEADER: expected})
                    self.assertEqual(response.status_code, 409, response.text)
                    self.assertNotIn('actor', response.json())
                    status = await client.get('/api/auth/status', headers={HEADER: expected})
                    self.assertEqual(status.status_code, 200, status.text)
                    self.assertEqual(status.json()['data']['user']['open_id'], self.ids['a'])
                response = await client.get('/api/workbench', headers={HEADER: grant_id})
                self.assertEqual(response.status_code, 200, response.text)
                self.assertEqual(response.json()['actor'], self.ids['a'])
                # Native image/download-style reads have no identity header, not an explicit empty one.
                native = await client.get('/api/workbench')
                self.assertEqual(native.status_code, 200, native.text)
                self.assertEqual(native.json()['actor'], self.ids['a'])
        self.assertEqual(self.ops, [])

    async def test_stale_get_after_stop_cannot_read_admin_but_status_can(self):
        for public in (False, True):
            with self.subTest(public=public):
                client = self.client_for(self.parent, public=public)
                grant_id = await self.start('a', client)
                stop = await client.post('/api/auth/impersonation/stop', json={}, headers={HEADER: grant_id})
                self.assertEqual(stop.status_code, 200, stop.text)
                self.assertIsNone(client.cookies.get(imp.COOKIE))
                response = await client.get('/api/workbench', headers={HEADER: grant_id})
                self.assertEqual(response.status_code, 409, response.text)
                self.assertNotIn('actor', response.json())
                status = await client.get('/api/auth/status', headers={HEADER: grant_id})
                self.assertEqual(status.status_code, 200, status.text)
                self.assertEqual(status.json()['data']['user']['open_id'], self.ids['l0'])
                normal = await client.get('/api/workbench', headers={HEADER: ''})
                self.assertEqual(normal.status_code, 200, normal.text)
                self.assertEqual(normal.json()['actor'], self.ids['l0'])
        self.assertEqual(self.ops, [])

    async def test_marker_does_not_authorize_without_matching_parent_cookie(self):
        await self.start()
        for parent in (None, self.session('manager')):
            client = self.client_for(parent)
            client.cookies.set(imp.COOKIE, self.client.cookies.get(imp.COOKIE))
            self.assertEqual((await client.get('/api/workbench')).status_code, 403)

    async def test_private_signature_token_learning_submit_auth_and_unknown_paths_are_blocked(self):
        grant_id = await self.start()
        cases = [('GET', '/api/assistant/conversation'), ('GET', '/api/assistant/files/private-id'),
                 ('GET', '/api/assistant/settings'), ('GET', '/api/signatures/management/request?token=valid'),
                 ('POST', '/api/signatures/management/submit'), ('GET', '/signature?token=valid'),
                 ('POST', '/api/signatures/usage-confirm'), ('GET', '/api/polling-work-orders/session?token=valid'),
                 ('POST', '/api/polling-work-orders/confirm'), ('POST', '/api/change-confirmations/record/confirm'),
                 ('POST', '/api/learning/papers/fixture/answer'), ('POST', '/api/learning/answers'),
                 ('GET', '/api/auth/permissions'), ('POST', '/api/auth/password/change'),
                 ('POST', '/api/auth/logout'), ('GET', '/api/admin/test'), ('GET', '/api/qt/test'),
                 ('GET', '/api/unreviewed-private')]
        for method, path in cases:
            with self.subTest(method=method, path=path):
                result = await self.client.request(method, path, json={}, headers={HEADER: grant_id})
                self.assertEqual(result.status_code, 403, result.text)
        self.assertEqual(self.ops, [])

    async def test_unknown_read_under_existing_module_is_not_implicitly_trusted(self):
        await self.start()
        result = await self.client.get('/api/repair-management/unreviewed-private')
        self.assertEqual(result.status_code, 403, result.text)
        self.assertEqual(self.ops, [])

    async def test_authorized_learning_history_is_readable_but_answers_cannot_be_submitted(self):
        self.configure('a', modules=['maintenance', 'repair_management', 'learning'])
        grant_id = await self.start()
        result = await self.client.get('/api/learning/history?scope=A')
        self.assertEqual(result.status_code, 200, result.text)
        self.assertEqual(result.json()['actor'], self.ids['a'])
        self.assertEqual(result.json()['scope'], 'A')
        self.assertEqual((await self.client.get('/api/learning/history?scope=B')).status_code, 403)
        for path in ('/api/learning/papers/fixture/answer', '/api/learning/answers'):
            result = await self.client.post(path, json={'answer': 'fixture'}, headers={HEADER: grant_id})
            self.assertEqual(result.status_code, 403, result.text)
        self.configure('a', modules=['maintenance', 'repair_management'])
        self.assertEqual((await self.client.get('/api/learning/history?scope=A')).status_code, 403)
        self.assertEqual(self.ops, [])

    async def test_notice_repair_mutations_attribute_real_admin_and_target_without_request_secrets(self):
        grant_id = await self.start()
        for path in ('/api/workbench-actions', '/api/maintenance-actions', '/api/repair-management/records',
                     '/api/repair-management/followups'):
            self.assertEqual((await self.post(grant_id, path=path, password='fixture-secret-password',
                identity_number='synthetic-private-identity', attachment='private-body-text')).status_code, 200)
        audits = self.store.list_business_operation_audits(limit=1000)
        self.assertEqual(len(self.ops), 4)
        for row in self.ops:
            self.assertEqual(row['actor'], self.ids['a'])
            self.assertEqual(row['audit']['actor_open_id'], self.ids['l0'])
            self.assertEqual(row['audit']['metadata']['impersonation']['target_open_id'], self.ids['a'])
        text = json.dumps(audits)
        for secret in ('fixture-secret-password', 'synthetic-private-identity', 'private-body-text',
                       self.client.cookies.get(imp.COOKIE), self.parent):
            self.assertNotIn(secret, text)

    async def test_audit_start_failure_fails_closed_before_mutation_and_clears_context(self):
        grant_id = await self.start()
        with patch.object(self.store, 'record_business_operation_audit', side_effect=OSError('fixture audit failure')):
            result = await self.post(grant_id)
        self.assertEqual(result.status_code, 503, result.text)
        self.assertEqual(self.ops, [])
        self.assertIsNone(CURRENT_IMPERSONATION.get())

    async def test_start_audit_failure_does_not_issue_child_cookie_or_grant(self):
        with patch.object(self.store, 'record_business_operation_audit', side_effect=OSError('fixture audit failure')):
            result = await self.client.post('/api/auth/accounts/reca/impersonate', json={})
        self.assertEqual(result.status_code, 503, result.text)
        self.assertIsNone(self.client.cookies.get(imp.COOKIE))
        self.assertEqual(self.store.list_documents(imp.NS), [])

    async def test_stop_audit_failure_does_not_restore_admin_for_stale_marker(self):
        grant_id = await self.start()
        with patch.object(self.store, 'record_business_operation_audit', side_effect=OSError('fixture audit failure')):
            result = await self.client.post('/api/auth/impersonation/stop', json={})
        self.assertEqual(result.status_code, 503, result.text)
        self.assertIsNotNone(self.client.cookies.get(imp.COOKIE))
        self.assertEqual((await self.post(grant_id)).status_code, 403)

    async def test_final_audit_failure_never_replays_business_and_does_not_leak_context(self):
        grant_id = await self.start()
        record = self.store.record_business_operation_audit
        def audit(**kwargs):
            if kwargs.get('status') == 'request_finished':
                raise OSError('fixture final attribution failure')
            return record(**kwargs)
        with patch.object(self.store, 'record_business_operation_audit', side_effect=audit):
            with self.assertRaises(OSError):
                await self.post(grant_id)
        self.assertEqual(len(self.ops), 1)
        self.assertEqual(self.ops[0]['audit']['actor_open_id'], self.ids['l0'])
        self.assertEqual(self.ops[0]['audit']['status'], 'success')
        self.assertIsNone(CURRENT_IMPERSONATION.get())

    async def test_public_guard_start_stop_and_expired_parent_cleanup(self):
        client = self.client_for(self.parent, public=True)
        await self.start('a', client)
        self.auth.clear_session(self.parent)
        result = await client.post('/api/auth/impersonation/stop', json={})
        self.assertEqual(result.status_code, 200, result.text)
        self.assertIsNone(client.cookies.get(imp.COOKIE))

    async def test_public_guard_preserves_identity_header_for_legitimate_business_mutation(self):
        client = self.client_for(self.parent, public=True)
        grant_id = await self.start('a', client)
        result = await self.post(grant_id, client=client)
        self.assertEqual(result.status_code, 200, result.text)
        self.assertEqual(self.ops[-1]['actor'], self.ids['a'])

    async def test_public_guard_does_not_strip_stale_child_header_and_fall_back_to_admin(self):
        grant_id = await self.start()
        await self.client.post('/api/auth/impersonation/stop', json={})
        client = self.client_for(self.parent, public=True)
        result = await self.post(grant_id, client=client)
        self.assertEqual(result.status_code, 409, result.text)
        self.assertEqual(self.ops, [])

    async def test_public_stop_without_parent_still_requires_same_origin(self):
        client = self.client_for(self.parent, public=True)
        await self.start('a', client)
        self.auth.clear_session(self.parent)
        result = await client.post('/api/auth/impersonation/stop', json={}, headers={'Origin': 'https://evil.example'})
        self.assertEqual(result.status_code, 403, result.text)
        self.assertIsNotNone(client.cookies.get(imp.COOKIE))

    async def test_old_stop_hash_cannot_end_a_new_grant_in_lan_or_public_path(self):
        for public in (False, True):
            with self.subTest(public=public):
                client = self.client_for(self.parent, public=public)
                old = await self.start('a', client)
                response = await client.post('/api/auth/impersonation/stop', json={}, headers={HEADER: old})
                self.assertEqual(response.status_code, 200, response.text)
                new = await self.start('b', client)
                response = await client.post('/api/auth/impersonation/stop', json={}, headers={HEADER: old})
                self.assertEqual(response.status_code, 409, response.text)
                self.assertEqual(self.grant(client)['id'], new)
                self.assertFalse(self.grant(client).get('ended_at'))
                response = await client.post('/api/auth/impersonation/stop', json={}, headers={HEADER: new})
                self.assertEqual(response.status_code, 200, response.text)

    async def test_draft_owners_isolate_two_grants_from_personal_draft(self):
        own = {'user': {'open_id': self.ids['a']}}
        self.assertEqual(self.controller._workbench_draft_identity(own), self.ids['a'])
        self.store.put_document('fixture_drafts', self.ids['a'], {'private': 'target-personal'})
        other = self.client_for(self.session('manager'))
        first, second = await self.start(), await self.start('a', other)
        self.assertNotEqual(first, second)
        for client, grant_id in ((self.client, first), (other, second)):
            response = await client.put('/api/workbench/draft', json={'value': grant_id}, headers={HEADER: grant_id})
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json()['owner'], 'impersonation:' + grant_id)
        for client, grant_id in ((self.client, first), (other, second)):
            response = await client.get('/api/workbench/draft')
            self.assertEqual(response.json()['data'], {'value': grant_id})
        self.assertEqual(self.store.get_document('fixture_drafts', self.ids['a']), {'private': 'target-personal'})

    def test_all_three_real_draft_routes_use_isolated_owner_helper(self):
        routes = {}
        for node in ast.walk(MAIN_AST):
            if not isinstance(node, ast.AsyncFunctionDef):
                continue
            for decorator in node.decorator_list:
                if (isinstance(decorator, ast.Call) and isinstance(decorator.func, ast.Attribute)
                        and decorator.args and isinstance(decorator.args[0], ast.Constant)
                        and decorator.args[0].value == '/api/workbench/draft'):
                    routes[decorator.func.attr] = node
        self.assertEqual(set(routes), {'get', 'put', 'delete'})
        for method, node in routes.items():
            with self.subTest(method=method):
                self.assertTrue(any(isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute)
                    and call.func.attr == '_workbench_draft_identity' for call in ast.walk(node)))

    async def test_account_listing_and_public_user_expose_indicator_not_credentials(self):
        listing = AccountManagement(self.h.login).accounts(False, {'open_id': self.ids['l0']})
        target = next(row for row in listing['items'] if row['id'] == self.rids['a'])
        self.assertTrue(target['can_impersonate'])
        grant_id = await self.start()
        user = (await self.client.get('/api/auth/status')).json()['data']['user']
        self.assertEqual(user['login_method'], 'impersonation')
        self.assertFalse(user['can_manage_accounts'])
        self.assertFalse(user['can_view_accounts'])
        self.assertTrue({'assistant', 'signatures', 'accounts'}.isdisjoint(user['modules']))
        self.assertEqual(user['impersonation']['id'], grant_id)
        self.assertNotIn(self.client.cookies.get(imp.COOKIE), json.dumps(user))
        self.assertNotIn('parent_hash', json.dumps(user))

    async def test_long_read_revalidates_before_releasing_body(self):
        await self.start()
        clock = {'value': 0}
        def revoke():
            self.configure('a', enabled=False)
            clock['value'] += 6
        self.slow_hook = revoke
        with patch.object(imp, 'time', SimpleNamespace(time=time.time, monotonic=lambda: clock['value'])):
            with self.assertRaises(PermissionError):
                await self.client.get('/api/workbench')
        self.assertIsNone(CURRENT_IMPERSONATION.get())

    async def test_sse_revocation_stops_subsequent_body_and_clears_context(self):
        await self.start()
        clock = {'value': 0}
        def revoke():
            self.configure('a', enabled=False)
            clock['value'] += 6
        self.slow_hook = revoke
        with patch.object(imp, 'time', SimpleNamespace(time=time.time, monotonic=lambda: clock['value'])):
            with self.assertRaises(PermissionError):
                await self.client.get('/api/repair-management/stream')
        self.assertIsNone(CURRENT_IMPERSONATION.get())

    async def test_sse_absolute_expiry_blocks_next_chunk_even_before_five_second_recheck(self):
        await self.start()
        expires = self.grant()['expires_at']
        wall = {'value': expires - 1}
        self.slow_hook = lambda: wall.update(value=expires)
        with patch.object(imp, 'time', SimpleNamespace(time=lambda: wall['value'], monotonic=lambda: 0)):
            with self.assertRaises(PermissionError):
                await self.client.get('/api/repair-management/stream')
        self.assertIsNone(CURRENT_IMPERSONATION.get())


if __name__ == '__main__':
    unittest.main()
