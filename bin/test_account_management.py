"""Account changes against isolated SQLite and synthetic Feishu personnel only."""
import asyncio
import copy
import json
from types import SimpleNamespace
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

import httpx
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from .test_personnel_password_login import (
    _TempDirTestCase, PersonnelLoginHarness, _page, _user, _allow_real_writes, IDENTITY_NUMBER,
)
from lan_bitable_template_portal.account_management import AccountManagement, install_account_management
from lan_bitable_template_portal.personnel_password_login import (
    PasswordLoginError, verify_password, install_personnel_password_login,
)
from lan_bitable_template_portal.portal_auth import BUILDING_OPEN_ID_MAP, REQUIRED_ADMIN_USERS


class AccountManagementTests(_TempDirTestCase):
    def setUp(self):
        super().setUp()
        self.enterContext(patch(
            'lan_bitable_template_portal.portal_auth.config',
            SimpleNamespace(app_id='cli_fixture_accounts', app_secret='fixture-only-not-a-real-secret'),
        ))
        self.h = PersonnelLoginHarness(self.root)
        self.addCleanup(self.h.close)
        self.h.auth._account_directory_provider = lambda: self.h.login.snapshot
        self.fields = {'姓名': '测试外部', '员工工号': 'TEST-001', '机楼/专业': 'A楼',
                       '员工姓名': _user('测试外部', 'ou_fixtureAccount'), '账号性质': '外部账号',
                       '岗位': '运维值班员', '身份证号': IDENTITY_NUMBER}
        self.h.service._records['recAccount'] = self.fields
        self.refresh()
        self.manager = AccountManagement(self.h.login)
        self.actor = {'open_id': next(iter(REQUIRED_ADMIN_USERS)), 'name': '测试管理员'}

    def refresh(self, extra=()):
        self.h.service._pages = [_page([{'record_id': rid, 'fields': fields}
                                       for rid, fields in self.h.service._records.items()] + list(extra))]
        self.h.login.directory(force=True)

    def item(self, rid='recAccount'):
        return next(x for x in self.manager.accounts()['items'] if x['id'] == rid)

    def update(self, item=None, **changes):
        item = item or self.item()
        data = {key: item[key] for key in ('id', 'revision', 'role', 'scopes', 'enabled')}
        return self.manager.update({'changes': [{**data, **changes}]}, self.actor)

    def test_unified_directory_includes_duty_positions_and_rejects_scope_change(self):
        before = copy.deepcopy(self.h.auth._load_permissions_locked())
        history_doc = {'do_not_touch': True}
        self.h.auth._state_store.put_document('fixture_history', 'history', history_doc)
        listing = self.manager.accounts()
        self.assertTrue(any(x['id'] == 'recAccount' for x in listing['items']))
        self.assertEqual(set(x['open_id'] for x in listing['items'] if x['kind'] == 'duty'), set(BUILDING_OPEN_ID_MAP.values()))
        self.assertEqual(self.h.login.people()['items'][0]['name'], self.item()['name'])
        self.assertEqual(len(self.h.login.permission_people()['items']), 1)
        self.assertEqual(self.h.login.permission_people()['items'][0]['position'], '运维值班员')
        self.assertEqual(self.h.service._permission_directory_cache['items'][0]['position'], '运维值班员')
        # A read-only listing never mutates the persisted permissions.
        self.assertEqual(self.h.auth._load_permissions_locked(), before)
        self.assertNotIn('password_revision', json.dumps(listing))
        self.assertNotIn(IDENTITY_NUMBER, json.dumps(listing))
        # HR scope/role is read-only: a cross-building escalation is rejected.
        with self.assertRaisesRegex(PasswordLoginError, '不能单独更改或跨楼授权'):
            self.update(scopes=['B'])
        # An enabled-only edit is still permitted and preserves every other account
        # plus the pre-upgrade history snapshot.
        self.update(enabled=False)
        after = self.h.auth._load_permissions_locked()
        for oid, user in before['users'].items():
            if oid == 'ou_fixtureAccount':
                continue
            self.assertEqual(after['users'][oid], user)
        # The edited account only flipped enabled=False; HR scope/role stayed put.
        edited = after['users']['ou_fixtureAccount']
        self.assertFalse(edited['enabled'])
        self.assertEqual(edited['role'], 'building')
        self.assertEqual(edited['scopes'], ['A'])
        self.assertEqual(self.h.auth._state_store.get_document('fixture_history', 'history'), history_doc)
        self.assertEqual(self.h.auth._state_store.get_document('account_management', 'permissions_before_upgrade'), before)

    def test_manual_revocation_tombstone_is_not_reactivated_by_login(self):
        person = self.h.login.snapshot['people']['recAccount']
        self.h.auth.sync_directory_permissions([person], on_login=True)
        self.assertEqual(self.item()['scopes'], ['A'])
        # Explicit revocation is an enabled=False tombstone; HR scope/role stays
        # read-only so the auto-scope is retained but the account is disabled.
        self.update(enabled=False)
        # A roster refresh/login sync must not re-enable the revoked account.
        self.h.auth.sync_directory_permissions([person], on_login=True)
        self.refresh()
        self.assertFalse(self.item()['enabled'])
        # A disabled account resolves to zero scopes - revocation is real.
        self.assertEqual(self.h.auth.scopes_for_open_id('ou_fixtureAccount'), [])

    def test_legacy_partial_permission_save_preserves_unsubmitted_people_and_duty_accounts(self):
        # Modernized update() rejects role/scope changes, so seed a legacy
        # extra-scope grant directly to exercise the migration/save path.
        payload = self.h.auth._load_permissions_locked()
        payload.setdefault('users', {})['ou_fixtureAccount'] = {
            'name': '测试外部', 'role': 'building', 'scopes': ['A', 'C'],
            'enabled': True, 'locked': False,
        }
        self.h.auth._save_permissions_locked(payload)
        self.refresh()
        # ou_other is a real roster identity with home D so the save validates against
        # the trusted directory rather than being treated as an unknown open_id.
        self.h.login.snapshot['people']['recOther'] = {
            'id': 'recOther', 'name': '另一人员', 'building': 'D楼', 'scopes': ['D'],
            'position': '运维值班员', 'selectable': True, 'inactive': False,
            'needs_setup': False, 'password_revision': 'rev-1', 'account_nature': '外部账号',
            'login_ids': ['ou_other'],
        }
        before = copy.deepcopy(self.h.auth._load_permissions_locked())
        result, changed = self.h.auth.save_permissions_payload([
            {'open_id': 'ou_other', 'name': '另一人员', 'role': 'building', 'scopes': ['D'], 'enabled': True}],
            updated_by=self.actor['open_id'])
        after = self.h.auth._load_permissions_locked()
        for oid, user in before['users'].items():
            for key, value in user.items():
                self.assertEqual(after['users'][oid][key], value)
        # The partial save adds only ou_other; the required admin (Li) and the
        # fixed duty/building accounts are auto-managed by _ensure_required_admins.
        fixed = set(REQUIRED_ADMIN_USERS) | set(BUILDING_OPEN_ID_MAP.values())
        self.assertEqual(set(changed) - fixed, {'ou_other'})
        self.assertEqual(after['default_scopes'], before['default_scopes'])
        self.assertTrue(any(item['open_id'] == 'ou_other' for item in result['users']))

    def test_admin_password_api_then_new_password_login_keeps_hr_home_scopes_and_history(self):
        # Seed a legacy extra cross-building grant directly (the modernized API
        # rejects scope changes); after a fresh login only the HR home scope A
        # remains authoritative - the legacy C grant is not preserved.
        payload = self.h.auth._load_permissions_locked()
        payload.setdefault('users', {})['ou_fixtureAccount'] = {
            'name': '测试外部', 'role': 'building', 'scopes': ['A', 'C'],
            'enabled': True, 'locked': False,
        }
        self.h.auth._save_permissions_locked(payload)
        self.refresh()
        # Compare persisted defaults, independent of the 5s permission-cache lifetime.
        self.h.auth._permissions_cache_until = 0
        before = copy.deepcopy(self.h.auth._load_permissions_locked())
        self.h.auth._state_store.put_document('fixture_history', 'history', {'keep': True})
        with _allow_real_writes():
            self.manager.password('recAccount', {'new_password': 'BeforeChange123', 'confirm_password': 'BeforeChange123'}, self.actor)
        redirect = 'http://fixture/api/auth/feishu/callback'
        state = parse_qs(urlsplit(self.h.auth.start_login(redirect_uri=redirect)).query)['state'][0]
        with patch.object(self.h.auth, '_exchange_login_code', return_value=self.actor):
            admin_sid = self.h.auth.complete_login(code='fixture', state=state, redirect_uri=redirect)[0]

        def current_session(request):
            return self.h.auth.get_session(request.cookies.get('lan_portal_session', ''))
        def require_admin(request):
            session = current_session(request)
            return (None, session) if self.h.auth.is_admin(session) else (JSONResponse({'ok': False}, status_code=403), {})
        async def read_json(request, max_bytes):
            return await request.json()
        controller = SimpleNamespace(_current_session=current_session, _require_admin_response=require_admin,
            _request_base_url=lambda request: 'http://fixture', _read_json_request=read_json,
            _clear_read_cache=lambda keys: None)
        runtime = SimpleNamespace(service=self.h.service, auth_manager=self.h.auth, state_store=self.h.auth._state_store)
        app = FastAPI()
        install_personnel_password_login(app, controller, runtime)

        async def exercise():
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://fixture',
                    headers={'Origin': 'http://fixture'}) as client:
                first = await client.post('/api/auth/password/login', json={'person_id': 'recAccount', 'password': 'BeforeChange123'})
                self.assertEqual(first.status_code, 200, first.text)
                old_sid = client.cookies['lan_portal_session']
                client.cookies.set('lan_portal_session', admin_sid, domain='fixture.local', path='/')
                changed = await client.post('/api/auth/accounts/recAccount/password',
                    json={'new_password': 'AfterChange456', 'confirm_password': 'AfterChange456'})
                self.assertEqual(changed.status_code, 200, changed.text)
                self.assertTrue(changed.json()['data']['password_changed'])
                self.assertIsNone(self.h.auth.get_session(old_sid))
                client.cookies.clear()
                rejected = await client.post('/api/auth/password/login', json={'person_id': 'recAccount', 'password': 'BeforeChange123'})
                self.assertEqual(rejected.status_code, 401, rejected.text)
                second = await client.post('/api/auth/password/login', json={'person_id': 'recAccount', 'password': 'AfterChange456'})
                self.assertEqual(second.status_code, 200, second.text)
                session = self.h.auth.get_session(client.cookies['lan_portal_session'])
                self.assertEqual(session['user']['open_id'], 'ou_fixtureAccount')
                self.assertEqual(session['user']['personnel_record_id'], 'recAccount')
                self.assertEqual(session['source'], 'personnel_password')
                self.assertEqual(set(session['allowed_scopes']), {'A'})
                self.assertNotIn('requires_password_change', second.json()['data'])
        with _allow_real_writes():
            asyncio.run(exercise())
        after = self.h.auth._load_permissions_locked()
        for oid, cfg in before['users'].items():
            if oid != 'ou_fixtureAccount':
                self.assertEqual(after['users'][oid], cfg)
        # The legacy extra C grant was normalized away on login - the HR home A
        # is now the sole authoritative scope, with role/enabled unchanged.
        self.assertEqual(after['users']['ou_fixtureAccount']['scopes'], ['A'])
        self.assertEqual(after['users']['ou_fixtureAccount']['role'], before['users']['ou_fixtureAccount']['role'])
        self.assertEqual(self.h.auth._state_store.get_document('fixture_history', 'history'), {'keep': True})

    def test_move_floor_updates_home_and_drops_extra_cross_building_scope(self):
        person = self.h.login.snapshot['people']['recAccount']
        self.h.auth.sync_directory_permissions([person], on_login=True)
        # Seed a legacy extra cross-building grant (A plus C) directly; the
        # modernized API rejects such a scope change outright.
        payload = self.h.auth._load_permissions_locked()
        payload.setdefault('users', {})['ou_fixtureAccount'] = {
            'name': '测试外部', 'role': 'building', 'scopes': ['A', 'C'],
            'enabled': True, 'locked': False,
        }
        self.h.auth._save_permissions_locked(payload)
        # HR home moves to B楼: the identity's scope follows the home and the
        # legacy extra C is no longer effective (no cross-building grant).
        self.fields['机楼/专业'] = 'B楼'
        self.refresh()
        # Re-read the person now that HR home is B, then sync so the saved grant
        # follows the new home; the legacy extra C is dropped.
        person = self.h.login.snapshot['people']['recAccount']
        self.h.auth.sync_directory_permissions([person], on_login=True)
        self.assertEqual(set(self.item()['scopes']), {'B'})
        self.assertEqual(set(self.h.auth.scopes_for_open_id('ou_fixtureAccount')), {'B'})
        self.assertEqual(set(self.h.auth._load_permissions_locked()['users']['ou_fixtureAccount']['scopes']), {'B'})

    def test_stale_save_rejected_without_touching_other_accounts(self):
        old = self.item()
        # An enabled-only edit is allowed and commits a new revision...
        self.update(enabled=False)
        # ...so re-submitting the stale revision (old enabled=True) is rejected as
        # already-changed, and no other account is touched.
        with self.assertRaisesRegex(PasswordLoginError, '已变化'):
            self.update({**old, 'enabled': True})
        self.assertFalse(self.item()['enabled'])

    def test_fixed_admin_self_disable_invalid_scopes_and_batch_atomicity(self):
        admin = next(x for x in self.manager.accounts()['items'] if x['open_id'] == self.actor['open_id'])
        with self.assertRaisesRegex(PasswordLoginError, '固定管理员'):
            self.update(admin, enabled=False)
        before = copy.deepcopy(self.h.auth._load_permissions_locked())
        external = {k: self.item()[k] for k in ('id', 'revision', 'role', 'scopes', 'enabled')}
        protected = {k: admin[k] for k in ('id', 'revision', 'role', 'scopes', 'enabled')}
        with self.assertRaises(PasswordLoginError):
            self.manager.update({'changes': [{**external, 'scopes': ['A']}, protected]}, self.actor)
        self.assertEqual(self.h.auth._load_permissions_locked(), before)
        with self.assertRaises(PasswordLoginError):
            self.update(scopes=['INVALID'])

    def test_disabled_offboarded_duplicate_and_missing_people_cannot_gain_access(self):
        self.update(scopes=['A'], enabled=False)
        self.h.auth.sync_directory_permissions(self.h.login.snapshot['people'].values(), on_login=True)
        self.assertFalse(self.h.login.people()['items'][0]['selectable'])
        self.update(enabled=True, scopes=['A'])
        self.fields['离职/异动情况'] = True
        self.refresh()
        self.assertEqual(self.h.auth.scopes_for_open_id('ou_fixtureAccount'), [])
        self.fields['离职/异动情况'] = False
        self.refresh(extra=[{'record_id': 'recDuplicate', 'fields': dict(self.fields)}])
        with self.assertRaises(PasswordLoginError):
            self.update(scopes=['B'])
        self.h.service._records.clear()
        self.refresh()
        self.assertTrue(self.h.auth._open_id_explicitly_disabled('ou_fixtureAccount'))

    def test_directory_failure_keeps_complete_cache_not_empty_or_removed(self):
        self.update(scopes=['A'])
        self.h.service._pages = []
        data = self.manager.accounts(refresh=True)
        self.assertTrue(data['warning'])
        self.assertTrue(any(x['id'] == 'recAccount' for x in data['items']))
        self.assertEqual(self.h.auth.scopes_for_open_id('ou_fixtureAccount'), ['A'])

    def test_feishu_actual_identity_keeps_hr_home_and_outage_keeps_cached_home(self):
        self.fields['账号性质'] = 'VNET'
        self.refresh()
        self.h.auth._account_directory_loader = lambda: self.h.login.directory()
        redirect = 'http://fixture/api/auth/feishu/callback'
        def login():
            state = parse_qs(urlsplit(self.h.auth.start_login(redirect_uri=redirect)).query)['state'][0]
            with patch.object(self.h.auth, '_exchange_login_code', return_value={'open_id': 'ou_fixtureAccount', 'name': '测试外部'}):
                return self.h.auth.complete_login(code='fixture', state=state, redirect_uri=redirect)[0]
        # A VNET (Feishu-only) identity resolves to its HR home A with no password
        # account created - the actual identity gets the same scope as the roster.
        sid = login()
        self.assertEqual(self.h.auth.get_session(sid)['allowed_scopes'], ['A'])
        self.assertEqual(self.h.auth.get_session(sid)['user']['open_id'], 'ou_fixtureAccount')
        # On a directory outage the complete cached roster is preserved (never
        # dropped), so the already-verified home A is still served - there is no
        # blanket all-scopes auto-grant on failure.
        self.h.auth._account_directory_loader = lambda: (_ for _ in ()).throw(OSError('offline'))
        sid = login()
        self.assertEqual(self.h.auth.get_session(sid)['allowed_scopes'], ['A'])

    def test_restored_session_rejects_old_password_and_inactive_person(self):
        with _allow_real_writes():
            self.manager.password('recAccount', {'new_password': 'BeforeRestart123', 'confirm_password': 'BeforeRestart123'}, self.actor)
            _, sid = self.h.login.login({'person_id': 'recAccount', 'password': 'BeforeRestart123'}, 'fixture')
            self.manager.password('recAccount', {'new_password': 'AfterRestart456', 'confirm_password': 'AfterRestart456'}, self.actor)
        self.h.auth._sessions.clear()
        self.assertIsNone(self.h.auth.get_session(sid))

    def test_admin_password_reuses_format_invalidates_sessions_and_audit_has_no_secrets(self):
        first, second = 'FixtureFirst123', 'FixtureSecond456'
        with _allow_real_writes():
            self.manager.password('recAccount', {'new_password': first, 'confirm_password': first}, self.actor)
            _, sid = self.h.login.login({'person_id': 'recAccount', 'password': first}, 'fixture')
            self.assertIsNotNone(self.h.auth.get_session(sid))
            self.manager.password('recAccount', {'new_password': second, 'confirm_password': second}, self.actor)
        stored = self.h.service._records['recAccount']['密码']
        self.assertTrue(verify_password(second, stored))
        self.assertFalse(verify_password(first, stored))
        self.assertIsNone(self.h.auth.get_session(sid))
        self.assertFalse(self.h.login.people()['items'][0]['needs_setup'])
        audits = self.h.auth._state_store.list_business_operation_audits(domain='accounts')
        for secret in (first, second, stored, IDENTITY_NUMBER):
            self.assertNotIn(secret, json.dumps(audits))

    def test_password_validation_and_feishu_account_protection(self):
        for new, confirmation in [('short', 'short'), ('ValidPass123', 'other')]:
            with self.assertRaises(PasswordLoginError), _allow_real_writes():
                self.manager.password('recAccount', {'new_password': new, 'confirm_password': confirmation}, self.actor)
        self.fields['账号性质'] = 'VNET'
        self.refresh()
        with self.assertRaisesRegex(PasswordLoginError, '飞书'):
            self.manager.password('recAccount', {'new_password': 'ValidPass123', 'confirm_password': 'ValidPass123'}, self.actor)
        self.assertEqual(self.h.service._patch_calls, 0)

    def test_disabled_external_password_can_change_without_reenabling(self):
        self.update(enabled=False, scopes=['A'])
        with _allow_real_writes():
            self.manager.password('recAccount', {'new_password': 'FixturePass123', 'confirm_password': 'FixturePass123'}, self.actor)
        self.assertFalse(self.item()['enabled'])

    def test_routes_reject_non_admin_cross_origin_and_unknown_password_fields(self):
        # Real admin (Li, L0) and ordinary building (ou_fixtureAccount, L4)
        # Feishu sessions so the dispatcher's _current_session + runtime.auth_manager
        # authorization path is exercised end to end.
        redirect = 'http://fixture/api/auth/feishu/callback'
        def login_identity(identity):
            state = parse_qs(urlsplit(self.h.auth.start_login(redirect_uri=redirect)).query)['state'][0]
            with patch.object(self.h.auth, '_exchange_login_code', return_value=identity):
                return self.h.auth.complete_login(code='fixture', state=state, redirect_uri=redirect)[0]
        admin_sid = login_identity(self.actor)
        building_sid = login_identity({'open_id': 'ou_fixtureAccount', 'name': '测试外部'})

        async def read_json(request, max_bytes):
            return await request.json()
        def current_session(request):
            return self.h.auth.get_session(request.cookies.get('lan_portal_session', ''))
        controller = SimpleNamespace(
            _current_session=current_session,
            _request_base_url=lambda request: 'http://fixture', _read_json_request=read_json,
            _clear_read_cache=lambda keys: None)
        runtime = SimpleNamespace(state_store=self.h.auth._state_store, auth_manager=self.h.auth)
        app = FastAPI()
        install_account_management(app, controller, runtime, lambda: self.h.login)

        async def exercise():
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://fixture') as client:
                # not logged in
                self.assertEqual((await client.get('/api/auth/accounts')).status_code, 401)
                # logged in but not a manager (ordinary building identity)
                self.assertEqual((await client.get('/api/auth/accounts', cookies={'lan_portal_session': building_sid})).status_code, 403)
                # a manager can list accounts
                self.assertEqual((await client.get('/api/auth/accounts', cookies={'lan_portal_session': admin_sid})).status_code, 200)
                # cross-origin write rejected even for a manager
                self.assertEqual((await client.post('/api/auth/accounts/permissions', json={'changes': []}, headers={'Origin': 'https://other'}, cookies={'lan_portal_session': admin_sid})).status_code, 403)
                # unknown password field rejected
                self.assertEqual((await client.post('/api/auth/accounts/recAccount/password', json={'password': 'DoNotAccept'}, headers={'Origin': 'http://fixture'}, cookies={'lan_portal_session': admin_sid})).status_code, 400)
        asyncio.run(exercise())
        self.assertEqual(self.h.service._patch_calls, 0)
