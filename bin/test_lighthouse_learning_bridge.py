"""Local identity hints must match native self-only learning authorization."""
import sqlite3
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(0, str(Path(__file__).parent))
from starlette.requests import Request
from lan_bitable_template_portal.lighthouse_bridge import actor_for, PortalAuthority
from lan_bitable_template_portal.portal_service import BUILDING_OPEN_ID_MAP
from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_model import read_scope_operations


class LearningBridgeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.session = {'user': {'open_id': 'ou_self'}, 'role': 'person'}
        self.resolve = Mock(return_value={'person_id': 'person_1', 'person': {'scopes': ['A']}})
        self.controller = SimpleNamespace(_current_session=lambda _: self.session)
        self.runtime = SimpleNamespace(
            state_store=None,
            auth_manager=SimpleNamespace(session_scopes=lambda _: ['A', 'H'], is_admin=lambda _: False),
            learning_service=SimpleNamespace(resolve_self=self.resolve))
        self.request = Request({'type': 'http', 'headers': [], 'state': {}})

    async def test_personal_hints_use_local_identity_without_widening_business_scope(self):
        actor = await actor_for(self.controller, self.runtime, self.request)
        self.assertEqual(actor['scopes'], ['A', 'H'])
        self.assertEqual(actor['learning_scopes'], ['A'])
        self.assertEqual(actor['learning_person_id'], 'person_1')
        self.resolve.assert_called_once_with('ou_self')
        # Default lookup lets the native owner filter include the person's old papers.
        descriptor = {'schema': {'query': ['scope']}, 'scope_mode': ''}
        operation = {'api_id': 'GET /api/assistant/question-bank'}
        result = read_scope_operations(operation, descriptor, actor)
        self.assertNotIn('scope', result[0]['params'])
        with self.assertRaises(AssistantError):
            read_scope_operations({**operation, 'params': {'scope': 'H'}}, descriptor, actor)

    async def test_shared_account_does_not_gain_personal_scope(self):
        self.session['user']['open_id'] = BUILDING_OPEN_ID_MAP['H']
        actor = await actor_for(self.controller, self.runtime, self.request)
        self.assertEqual(actor['learning_scopes'], ['H'])
        self.assertNotIn('learning_person_id', actor)
        self.resolve.assert_not_called()

    async def test_missing_or_busy_learning_cache_does_not_break_other_modules(self):
        for error in (None, sqlite3.OperationalError('busy')):
            with self.subTest(error=error):
                self.resolve.return_value = {'person_id': ''}
                self.resolve.side_effect = error
                actor = await actor_for(self.controller, self.runtime, self.request)
                self.assertEqual(actor['scopes'], ['A', 'H'])
                self.assertEqual(actor['learning_scopes'], [])
                self.assertNotIn('learning_person_id', actor)

    async def test_mapping_change_revokes_inflight_context(self):
        prior = await actor_for(self.controller, self.runtime, self.request)
        authority = PortalAuthority(None, self.controller, self.runtime, SimpleNamespace(lease='test'))
        context = authority.context(self.request, prior)
        current = {**prior, 'learning_person_id': 'other_person'}
        with patch('lan_bitable_template_portal.lighthouse_bridge.actor_for', AsyncMock(return_value=current)):
            with self.assertRaisesRegex(AssistantError, '人员身份已变化'):
                await authority.authorize(context)

    async def test_busy_learning_cache_keeps_nonlearning_authorization(self):
        prior = await actor_for(self.controller, self.runtime, self.request)
        authority = PortalAuthority(None, self.controller, self.runtime, SimpleNamespace(lease='test'))
        context = authority.context(self.request, prior)
        self.resolve.side_effect = sqlite3.OperationalError('busy')
        _, current = await authority.authorize(context)
        self.assertEqual(current['scopes'], ['A', 'H'])
        self.assertEqual(current['learning_scopes'], [])

    async def test_real_person_lookup_reaches_only_own_historical_questions(self):
        from lan_bitable_template_portal.learning import LearningService
        from openclaw_service.assistant.lighthouse_sources import question_bank
        import test_learning as fixtures
        with tempfile.TemporaryDirectory() as directory:
            cloud = fixtures.FakeCloud(enabled=False)
            service = LearningService(Path(directory), cloud, fixtures.FakeSender())
            own = fixtures.LearningTests.question(self, 1)
            other = fixtures.LearningTests.question(self, 2)
            with service.transaction() as conn:
                service._put('person', 'person_1', {'id': 'person_1', 'name': '本人',
                    'employee_no': '1', 'scopes': ['A'], 'active': True, 'login_ids': ['ou_self']}, conn, False)
                for person, question in (('person_1', own), ('person_2', other)):
                    service._put('paper', person, {'id': person, 'scope': 'H', 'person_id': person,
                        'date': '2026-10-01', 'shortage': {}, 'created_at': '2026-10-01',
                        'questions': [question]}, conn, False)
                    service._put('record', person, {'id': person, 'entries': {
                        question['id']: {'revealed': 'yes'}}}, conn, False)
                service._put('local', 'refresh', {'at': '2026-10-09T08:00:00+08:00'}, conn, False)
            self.runtime.learning_service = service
            actor = await actor_for(self.controller, self.runtime, self.request)
            operation = read_scope_operations({'api_id': 'GET /api/assistant/question-bank'},
                {'schema': {'query': ['scope']}, 'scope_mode': ''}, actor)[0]
            result = question_bank(service, actor, operation['params'])
            self.assertEqual([question['id'] for question in result['items']], [own['id']])
            self.assertIn('answer', result['items'][0])
            self.assertEqual(cloud.calls, [])


if __name__ == '__main__':
    unittest.main()
