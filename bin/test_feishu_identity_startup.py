"""Identity migration gates run before any service accepts or resumes work."""
from __future__ import annotations

import asyncio
from pathlib import Path
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))


class PortalMigrationGateTests(unittest.TestCase):
    def test_pending_migration_is_rejected_before_writes_while_task_is_running(self):
        from clipflow_backend.main import FastAPIPortalController, PortalRuntime
        controller = FastAPIPortalController.__new__(FastAPIPortalController)
        controller._server = controller._thread = None
        controller._initialize_portal_handler_state = Mock()
        with patch.object(PortalRuntime, 'action_running_job_ids', {'running-fixture'}), patch(
            'lan_bitable_template_portal.feishu_identity_migration.identity_migration_pending', return_value=True,
        ), patch('lan_bitable_template_portal.feishu_identity_migration.migrate_identity_databases') as migrate:
            with self.assertRaisesRegex(RuntimeError, '后台任务尚未结束'):
                controller.start()
            migrate.assert_not_called()
            controller._initialize_portal_handler_state.assert_not_called()

    def test_changed_ownership_does_not_reuse_pre_migration_service_cache(self):
        import clipflow_backend.main as backend
        made = []

        class Service:
            def __init__(self, **kwargs):
                self.app_token, self.table_id = 'fixture-base', 'fixture-table'
                self._repair_snapshots_enabled = True
                self.ensure_snapshot_loaded = self.resume_repair_link_tasks_async = lambda: None
                made.append(self)

        controller = backend.FastAPIPortalController.__new__(backend.FastAPIPortalController)
        controller.app_token, controller.table_id = 'fixture-base', 'fixture-table'
        controller._identity_migration_requires_reload = True
        previous = Service()
        with patch.object(backend.PortalRuntime, 'service', previous), patch.object(
            backend, 'MaintenancePortalService', Service,
        ), patch.object(backend, 'PortalAuthManager', side_effect=RuntimeError('stop before auth')):
            with self.assertRaisesRegex(RuntimeError, 'stop before auth'):
                controller._initialize_portal_handler_state()
            self.assertEqual(len(made), 2)
            self.assertIs(backend.PortalRuntime.service, made[-1])
            self.assertIsNot(backend.PortalRuntime.service, previous)

    def test_failed_migration_never_initializes_auth_or_workers(self):
        from clipflow_backend.main import FastAPIPortalController, PortalRuntime
        controller = FastAPIPortalController.__new__(FastAPIPortalController)
        controller._server = controller._thread = None
        controller._initialize_portal_handler_state = Mock()
        with tempfile.TemporaryDirectory() as directory, patch.object(
            PortalRuntime, 'state_store', SimpleNamespace(db_path=Path(directory) / 'lan_portal_state.sqlite3')
        ), patch(
            'lan_bitable_template_portal.feishu_identity_migration.migrate_identity_databases',
            side_effect=RuntimeError('isolated migration conflict'),
        ) as migration, patch.object(PortalRuntime, 'ensure_action_worker') as workers:
            with self.assertRaisesRegex(RuntimeError, 'isolated migration conflict'):
                controller.start()
            migration.assert_called_once_with(Path(directory))
            controller._initialize_portal_handler_state.assert_not_called()
            workers.assert_not_called()

    def test_migration_finishes_before_auth_initialization(self):
        from clipflow_backend.main import FastAPIPortalController, PortalRuntime
        controller = FastAPIPortalController.__new__(FastAPIPortalController)
        controller._server = controller._thread = None
        events = []

        def initialize():
            events.append('auth')
            raise RuntimeError('stop before network')

        controller._initialize_portal_handler_state = initialize
        with patch(
            'lan_bitable_template_portal.feishu_identity_migration.migrate_identity_databases',
            side_effect=lambda root: events.append('migration'),
        ), patch.object(PortalRuntime, 'ensure_action_worker') as workers:
            with self.assertRaisesRegex(RuntimeError, 'stop before network'):
                controller.start()
            self.assertEqual(events, ['migration', 'auth'])
            workers.assert_not_called()


class ResidentMigrationGateTests(unittest.IsolatedAsyncioTestCase):
    async def test_failed_migration_stays_closed_and_retry_checks_it_again(self):
        from openclaw_service.server import Host
        host = Host.__new__(Host)
        with tempfile.TemporaryDirectory() as directory:
            host.state = Path(directory) / 'lighthouse_openclaw'
            host.store = SimpleNamespace()
            host.backend_lock = asyncio.Lock()
            host.backend_lease = None
            host.backend_hooks = None
            host.current_lease = Mock(return_value={'id': 'fixture-lease', 'assistant_backend': True})
            host.portal_bridge = SimpleNamespace(acall=AsyncMock(return_value=[]))
            host.catalog = SimpleNamespace(replace=Mock())
            with patch(
                'lan_bitable_template_portal.feishu_identity_migration.migrate_identity_databases',
                side_effect=[RuntimeError('isolated migration conflict'), {}],
            ) as migration:
                with self.assertRaisesRegex(RuntimeError, 'isolated migration conflict'):
                    await host.setup_backend({})
                self.assertIsNone(host.backend_lease)
                host.portal_bridge.acall.assert_not_awaited()
                self.assertEqual(await host.setup_backend({}), {'ready': True})
                self.assertEqual(migration.call_count, 2)
                self.assertEqual(host.backend_lease, 'fixture-lease')


class MigratedAccountIntegrationTests(unittest.TestCase):
    def test_temporary_auth_read_failure_returns_retry_without_clearing_cookie(self):
        from bin.test_portal_access_boundaries import PortalAccessBoundaries
        from lan_bitable_template_portal.portal_auth import PortalAuthUnavailableError, AUTH_COOKIE_NAME

        fixture = PortalAccessBoundaries()
        self.addCleanup(fixture.doCleanups)
        fixture.setUp()
        fixture.login()
        before = fixture.client.cookies.get(AUTH_COOKIE_NAME)
        with patch.object(fixture.controller, '_current_session', side_effect=PortalAuthUnavailableError('fixture failure')):
            response = fixture.client.get('/api/auth/status')
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.headers.get('cache-control'), 'no-store')
        self.assertEqual(response.headers.get('retry-after'), '2')
        self.assertNotIn('set-cookie', response.headers)
        self.assertNotIn('auth_required', response.json())
        self.assertEqual(fixture.client.cookies.get(AUTH_COOKIE_NAME), before)
        self.assertEqual(fixture.client.get('/api/auth/status').status_code, 200)

    def test_same_cookie_retains_scopes_and_reads_original_chat_under_new_id(self):
        self._check_migrated_account('feishu', 'VNET')

    def test_password_cookie_with_feishu_id_retains_original_grants_and_history(self):
        self._check_migrated_account('personnel_password', 'external')

    def test_password_cookie_without_feishu_id_keeps_record_based_principal(self):
        self._check_migrated_account('personnel_password', 'external', with_feishu=False)

    def _check_migrated_account(self, source, nature, *, with_feishu=True):
        from lan_bitable_template_portal.state_store import LanPortalStateStore
        from lan_bitable_template_portal.portal_auth import PortalAuthManager
        from lan_bitable_template_portal.feishu_identity_migration import migrate_identity_databases
        from lan_bitable_template_portal.personnel_password_login import DIRECTORY_NS, DIRECTORY_KEY
        from openclaw_service.store import AssistantStore

        old, new = ('ou_PreviousFixture', 'ou_CurrentFixture') if with_feishu else (
            'person:recFixture', 'person:recFixture')
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = LanPortalStateStore(root / 'lan_portal_state.sqlite3')
            self.addCleanup(store.shutdown_write_worker)
            grant = {'name': 'Migration Fixture', 'role': 'building', 'enabled': True,
                     'level': 'L4', 'category': 'engineer', 'scopes': ['C'],
                     'modules': ['assistant', 'learning'], 'access_version': 1}
            store.put_auth_permissions({'version': 2, 'default_scopes': [], 'users': {old: grant}})
            store.put_document(DIRECTORY_NS, DIRECTORY_KEY, {
                'schema': 3, 'app_id': 'cli_previousFixture', 'loaded_at': time.time(),
                'people': {'recFixture': {'id': 'recFixture', 'name': 'Migration Fixture',
                    'login_ids': [old] if with_feishu else [], 'scopes': ['C'], 'position': 'engineer',
                    'password_revision': 'fixture-revision',
                    'selectable': True, 'inactive': False, 'account_nature': nature}},
            })
            cookie = 'unchanged-session-cookie'
            store.put_auth_session(PortalAuthManager._secret_hash(cookie), {
                'user': {'open_id': old, 'name': 'Migration Fixture', 'personnel_record_id': 'recFixture'},
                'source': source, 'password_revision': 'fixture-revision', 'role': 'building',
                'allowed_scopes': ['C'], 'expires_at': time.time() + 3600,
                'created_at_ts': time.time() - 60,
            })
            conversation = {'id': 'unchanged-conversation', 'unified_person': True,
                            'turns': [{'operation_id': 'unchanged-operation', 'text': 'Original answer', 'at': 1}]}
            store.put_document('lighthouse_ai', 'conversation:' + old, conversation)
            migrate_identity_databases(root, manifest={
                'version': 1, 'target_app_id': 'cli_aa42dc770a381cb3', 'unresolved': [],
                'mappings': [{'old_open_id': old if with_feishu else 'ou_UnrelatedPrevious',
                              'new_open_id': new if with_feishu else 'ou_UnrelatedCurrent', 'record_ids': ['recFixture'],
                              'evidence': ['personnel_snapshot', 'target_hr_person_field']}],
            })
            with patch('lan_bitable_template_portal.portal_auth.get_data_file_path',
                       side_effect=lambda name: str(root / name)), patch(
                'lan_bitable_template_portal.portal_auth.config',
                SimpleNamespace(app_id='cli_aa42dc770a381cb3', app_secret='isolated-fixture-secret'),
            ):
                auth = PortalAuthManager()
                try:
                    snapshot = store.get_document(DIRECTORY_NS, DIRECTORY_KEY)
                    auth._account_directory_provider = lambda: snapshot
                    session = auth.get_session(cookie)
                    self.assertIsNotNone(session)
                    self.assertEqual(session['user']['open_id'], new)
                    self.assertEqual(session['allowed_scopes'], ['C'])
                    self.assertEqual(auth._permission_user(new)['modules'], grant['modules'])
                    if old != new:
                        self.assertEqual(auth._permission_user(old), {})
                finally:
                    auth.shutdown()
                    auth._cleanup_executor.shutdown(wait=True)
                    auth._state_store.shutdown_write_worker()
            legacy_files = root / 'lighthouse_assistant' / 'files'
            legacy_files.mkdir(parents=True)
            assistant = AssistantStore(root / 'lighthouse_openclaw', legacy_db=store.db_path,
                                       legacy_files=legacy_files)
            self.assertEqual(assistant.get_document('lighthouse_ai', 'conversation:' + new), conversation)
            if old != new:
                self.assertIsNone(assistant.get_document('lighthouse_ai', 'conversation:' + old))


if __name__ == '__main__':
    unittest.main()
