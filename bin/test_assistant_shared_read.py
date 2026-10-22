"""Shared assistant reads never grant browser writes or expose private sessions."""
import asyncio
import copy
import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).parent))
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
import httpx
from lan_bitable_template_portal.assistant_shared_read import (
    SHARED_READ, SharedRead, original_actor, reader_actor, shared_operation, shared_query_actor,
)
from lan_bitable_template_portal.lighthouse_bridge import PortalAuthority, actor_for
from lan_bitable_template_portal.module_access import allowed_modules, module_denial
from lan_bitable_template_portal.portal_auth import PortalAuthManager
from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_api import PortalAPICatalog
from openclaw_service.assistant.lighthouse_sources import SCOPES


class Auth(PortalAuthManager):
    def __init__(self):
        self.profile = {'role': 'building', 'level': 'L4', 'modules': ['event'], 'scope_rules': ['A']}
        self.session = {'user': {'open_id': 'ou_self', 'name': '本人'}, 'role': 'person'}

    def _account_profile(self, identity):
        return copy.deepcopy(self.profile)

    def _account_read_state(self):
        return None

    def _permission_user(self, identity):
        return {'enabled': True}

    def directory_block_reason(self, identity):
        return ''

    def get_session(self, _):
        return copy.deepcopy(self.session)


class PolicyTests(unittest.TestCase):
    def test_query_identity_roundtrip_does_not_grant_write_or_admin(self):
        original = {'id': 'ou_self', 'scopes': ['A'], 'modules': ['event'], 'is_admin': False}
        query = shared_query_actor(original)
        self.assertEqual(set(query['scopes']), SCOPES)
        self.assertIn('knowledge', query['modules'])
        self.assertFalse(query['is_admin'])
        self.assertTrue(reader_actor(query)['is_admin'])
        self.assertEqual(original_actor(reader_actor(query)), original)
        self.assertEqual(original_actor({**query, 'allowed_scopes': sorted(SCOPES)}), original)
        query['scopes'].append('invalid')
        self.assertEqual(original['scopes'], ['A'])

    def test_shared_query_does_not_restore_a_plan_outside_original_write_scope(self):
        from openclaw_service.assistant.lighthouse_agent import PortalAgent
        portal = object.__new__(PortalAgent)
        portal.store = SimpleNamespace(get_document=lambda *_: {'owner': 'ou_self', 'scopes': ['B']})
        actor = shared_query_actor({'id': 'ou_self', 'scopes': ['A'], 'modules': ['repair_management'], 'is_admin': False})
        with self.assertRaisesRegex(AssistantError, '无权访问此操作计划'):
            portal.get_plan(actor, 'previous-plan')

    def test_private_paths_writes_and_unclassified_endpoints_are_not_shared(self):
        for path in ('/api/assistant/history', '/api/assistant/files/other', '/api/auth/accounts',
                     '/api/admin/certificates', '/api/signatures/image/other', '/api/qt/settings',
                     '/api/feishu-workpartner/status', '/api/handover-links', '/api/release-journal', '/api/unknown'):
            with self.subTest(path=path):
                self.assertFalse(shared_operation({'method': 'GET', 'path': path, 'read_only': True}))
        for method, read_only in (('POST', True), ('PUT', False), ('DELETE', False), ('GET', False)):
            self.assertFalse(shared_operation({'method': method, 'path': '/api/events', 'read_only': read_only}))
        self.assertTrue(shared_operation({'api_id': 'POST /api/plan-convergence/compare',
            'method': 'POST', 'path': '/api/plan-convergence/compare', 'read_only': True}))

    def test_document_source_vectors_are_shared_without_relaxing_writes_or_stale_provenance(self):
        from lan_bitable_template_portal.document_library import DocumentLibraryService, _digest
        library = object.__new__(DocumentLibraryService)
        library.refresh_actor = library.authorize_source = None
        source = {'enabled': True, 'base_token': 'baseFixture', 'table_id': 'tblFixture',
                  'read_scopes': ['B'], 'write_scopes': ['B'], 'revision': 1}
        record = {'deleted': False, 'pending_operation': '', 'revision': 'r1',
                  'payload': json.dumps({'scopes': ['B']})}
        library._table = lambda _: source
        library._cached = lambda *_: record
        provenance = {'table_key': 'manuals', 'record_id': 'recFixture', 'scopes': ['B'],
            'origin': _digest(['baseFixture', 'tblFixture']), 'config_revision': 1, 'record_revision': 'r1'}
        original = {'id': 'ou_self', 'modules': ['knowledge'], 'scopes': ['A'], 'is_admin': False}
        self.assertFalse(library.can_read_source(original, provenance))
        self.assertTrue(library.can_read_source(shared_query_actor(original), provenance))
        self.assertFalse(library.can_read_source(shared_query_actor(original), {**provenance, 'record_revision': 'old'}))
        operation = {'method': 'GET', 'path': '/api/document-library/tables', 'read_only': True}
        token = SHARED_READ.set(SharedRead(object(), 'ou_self', operation, lambda: None))
        try:
            self.assertTrue(library._allowed(original, source, ['B']))
            self.assertFalse(library._allowed(original, source, ['B'], write=True))
            source['enabled'] = False
            self.assertFalse(library._allowed(original, source, ['B']))
            self.assertFalse(library.can_read_source(shared_query_actor(original), provenance))
        finally:
            SHARED_READ.reset(token)


class SharedReadTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        from clipflow_backend.main import FastAPIPortalController
        self.auth = Auth()
        self.runtime = SimpleNamespace(auth_manager=self.auth, state_store=None, service=SimpleNamespace())
        self.enterContext(patch('clipflow_backend.main.PortalRuntime', self.runtime))
        self.controller = object.__new__(FastAPIPortalController)
        self.app = FastAPI()
        self.writes = 0
        self.background = None
        self.release_background = asyncio.Event()

        def permitted(request):
            session = self.controller._current_session(request)
            return (session and 'repair_management' in allowed_modules(self.auth, session)
                    and self.auth.scope_allowed(session, 'B'))

        @self.app.get('/api/repair-management')
        async def read(request: Request, background: str = ''):
            if not permitted(request):
                return JSONResponse({'error': 'denied'}, status_code=403)
            if request.query_params.get('background'):
                async def later():
                    await self.release_background.wait()
                    return self.auth.account_profile('ou_self')['role']
                self.background = asyncio.create_task(later())
            await asyncio.sleep(0)
            return {'items': [{'scope': 'B', 'name': '共享维修', 'phone': '13900000000',
                'note': 'repair\nAppSecret=FIXTURE_PRIVATE_VALUE'}]}

        @self.app.post('/api/repair-management')
        async def write(request: Request):
            if not permitted(request):
                return JSONResponse({'error': 'denied'}, status_code=403)
            self.writes += 1
            return {'ok': True}

        @self.app.get('/api/repair-management/download')
        async def export(request: Request):
            return Response(b'original private attachment', media_type='application/pdf')

        self.catalog = PortalAPICatalog(self.app)
        self.authority = PortalAuthority(self.app, self.controller, self.runtime,
            SimpleNamespace(instance='fixture', lease='fixture', root=Path('.')))
        self.authority.catalog = self.catalog
        self.request = Request({'type': 'http', 'method': 'GET', 'path': '/', 'headers': [],
                                'query_string': b'', 'scheme': 'http', 'server': ('test', 80)})
        self.actor = await actor_for(self.controller, self.runtime, self.request)
        self.context = {'request': self.request}

    async def test_shared_foreign_read_does_not_change_real_profile_browser_or_writes(self):
        profile = copy.deepcopy(self.auth.profile)
        operation = {'api_id': 'GET /api/repair-management'}
        result = await self.authority.invoke(self.context, self.actor, {'operation': operation})
        self.assertTrue(result['ok'], result)
        self.assertEqual(result['_raw']['items'][0]['name'], '共享维修')
        self.assertNotIn('13900000000', str(result))
        self.assertNotIn('FIXTURE_PRIVATE_VALUE', str(result))
        self.assertIsNone(SHARED_READ.get())
        self.assertEqual(self.auth.profile, profile)
        self.assertEqual(self.auth.account_profile('ou_self'), profile)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app), base_url='http://test') as client:
            self.assertEqual((await client.get('/api/repair-management')).status_code, 403)
            self.assertEqual((await client.post('/api/repair-management')).status_code, 403)
        with self.assertRaises(AssistantError):
            await self.authority.invoke(self.context, self.actor, {'operation': {'api_id': 'POST /api/repair-management'}})
        self.assertEqual(self.writes, 0)

    async def test_chat_entry_without_module_grant_does_not_open_message_write_apis(self):
        self.assertNotIn('assistant', allowed_modules(self.auth, self.auth.session))
        chat = Request({**self.request.scope, 'path': '/api/assistant/messages'})
        self.assertIsNone(await module_denial(chat, self.controller, self.runtime))
        message = Request({**self.request.scope, 'method': 'POST', 'path': '/api/message-delivery/send'})
        denial = await module_denial(message, self.controller, self.runtime)
        self.assertEqual(denial.status_code, 403)

    async def test_expired_guest_or_changed_user_cannot_use_shared_context(self):
        for session in (None, {'role': 'guest', 'user': {'open_id': 'ou_self'}},
                        {'user': {'open_id': 'ou_other'}}):
            self.auth.session = session
            with self.subTest(session=session), self.assertRaises(AssistantError):
                await self.authority.invoke(self.context, self.actor,
                                            {'operation': {'api_id': 'GET /api/repair-management'}})
            self.assertIsNone(SHARED_READ.get())

    async def test_grant_is_bound_to_exact_read_and_context_is_concurrent_safe(self):
        operation, _ = self.catalog.validate_operation({'api_id': 'GET /api/repair-management'})
        grant = SharedRead(self.auth, 'ou_self', operation, lambda: None)
        self.assertFalse(grant.matches('POST', '/api/repair-management'))
        self.assertFalse(grant.matches_operation({**operation, 'params': {'scope': 'B'}}))
        async def scoped():
            token = SHARED_READ.set(grant)
            try:
                await asyncio.sleep(.01)
                self.assertEqual(self.auth.account_profile('ou_self')['role'], 'admin')
                self.assertEqual(self.auth.account_profile('ou_other')['role'], 'building')
            finally:
                SHARED_READ.reset(token)
        async def ordinary():
            await asyncio.sleep(.005)
            self.assertEqual(self.auth.account_profile('ou_self')['role'], 'building')
            self.assertFalse(self.auth.scope_allowed(self.auth.session, 'B'))
        await asyncio.gather(scoped(), ordinary())
        request = Request({**self.request.scope, 'state': {'assistant_shared_business_read': grant}})
        with self.assertRaises(AssistantError):
            await self.catalog.invoke({'api_id': 'POST /api/repair-management'}, request)
        self.assertIsNone(SHARED_READ.get())

    async def test_unredacted_binary_is_not_shared_to_model_or_export_tool(self):
        with self.assertRaisesRegex(AssistantError, '未脱敏原文件'):
            await self.authority.invoke(self.context, self.actor,
                {'operation': {'api_id': 'GET /api/repair-management/download'}})

    async def test_shared_native_roster_read_needs_login_but_no_extra_module_or_consent(self):
        self.authority.authorize = AsyncMock(return_value=(self.context, self.actor))
        self.authority.tables = SimpleNamespace(roster_count=lambda actor, day, shift: {
            'scopes': actor['scopes'], 'assignment_count': 3})
        result = await self.authority.dispatch({'instance': 'fixture', 'lease': 'fixture',
            'context_id': 'trusted', 'action': 'roster_count',
            'payload': {'scopes': ['B'], 'date': '2026-10-22'}})
        self.assertEqual(result['assignment_count'], 3)
        self.assertEqual(result['scopes'], ['B'])

    async def test_background_tasks_cannot_keep_the_finished_read_projection(self):
        await self.authority.invoke(self.context, self.actor,
            {'operation': {'api_id': 'GET /api/repair-management', 'params': {'background': '1'}}})
        self.assertIsNotNone(self.background)
        self.release_background.set()
        self.assertEqual(await self.background, 'building')


if __name__ == '__main__':
    unittest.main()
