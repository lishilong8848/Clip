"""Build/apply a real runtime patch against an isolated, already-used client."""
import asyncio
import datetime as dt
import hashlib
import importlib.util
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import time
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import AsyncMock, Mock, patch
from uuid import NAMESPACE_URL, uuid5
from zipfile import ZipFile

from bin.test_learning import FakeCloud, learning
from upload_event_module.ui.main_window_patch import PatchUpdateMixin
import package_portable as packaging
from lan_bitable_template_portal.feishu_identity import TARGET_APP_ID
from upload_event_module.services.feishu_app_migration import MIGRATION_MARKER


class LearningUpdateFlowTests(TestCase):
    def setUp(self):
        self.enterContext(patch('lan_bitable_template_portal.portal_auth.config', SimpleNamespace(
            app_id=TARGET_APP_ID, app_secret='synthetic-secret', **{MIGRATION_MARKER: 2})))
        self.enterContext(patch('upload_event_module.services.feishu_token_manager.token_manager._credentials',
                                return_value=(TARGET_APP_ID, 'synthetic-secret')))

    def test_full_patch_preserves_people_config_receipts_and_shift_delivery(self):
        with TemporaryDirectory(prefix='learning_update_') as directory:
            root = Path(directory)
            client, output = root / 'client', root / 'build'
            data = client / 'bin/data'
            protected = {
                'bin/data/feishu_assistant.json': json.dumps({'app_id': TARGET_APP_ID,
                    'enabled': True, 'secret_cipher': 'fixture-cipher', 'bridge_cipher': 'fixture-bridge'}).encode(),
                'bin/data/feishu_assistant_active.json': json.dumps({'app_id': TARGET_APP_ID}).encode(),
                'bin/data/feishu_assistant.sqlite3': b'fixture-inbox',
                'bin/data/config.json': json.dumps({'feishu_app_id': TARGET_APP_ID,
                                                   MIGRATION_MARKER: 2}).encode(),
            }
            for relative, content in protected.items():
                path = client / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(content)
            from bin.test_personnel_password_login import PersonnelLoginHarness, _page, _user
            from openclaw_service.assistant.lighthouse_ai import LighthouseAssistant
            from openclaw_service.store import AssistantStore
            self.enterContext(patch.dict(os.environ, {'CLIPFLOW_DISABLE_SQLITE_WRITE_WORKER': '1'}))
            identity = sender_id = 'ou_fixtureIdentity'
            harness = PersonnelLoginHarness(data)
            try:
                harness.service._pages = [_page([{'record_id': 'recFixture', 'fields': {
                    '姓名': '测试人员', '员工姓名': _user('测试人员', identity),
                    '机楼/专业': 'E楼', '体系岗位': '值班员', '账号性质': 'VNET'}}])]
                harness.login.directory(force=True)
                # Match an already-used client, including normalized duty accounts and module restrictions.
                harness.auth._permissions_cache_until = 0
                existing = harness.auth._load_permissions_locked()
                existing['users'][identity].update(modules=['assistant', 'learning'], module_access_version=1)
                harness.auth._save_permissions_locked(existing)
                profile = harness.auth.account_profile(identity)
                cookie = 'fixture-existing-cookie'
                session = {'user': {'open_id': identity, 'name': '测试人员'}, 'source': 'feishu',
                    'role': profile['role'], 'allowed_scopes': harness.auth.scopes_for_open_id(identity),
                    'created_at_ts': time.time(), 'expires_at': time.time() + 3600}
                harness.auth._state_store.put_auth_session(harness.auth._secret_hash(cookie), session)
                permissions = harness.auth._state_store.get_auth_permissions()
            finally:
                harness.close()
            history = {'id': 'fixture-conversation', 'unified_person': True,
                'summary': '更新前的个人摘要', 'turns': [{'operation_id': 'fixture-operation',
                    'question': '更新前的问题', 'answer': '更新前的回答', 'scopes': ['E']}]}
            assistant_root = data / 'lighthouse_openclaw'
            AssistantStore(assistant_root).put_document('lighthouse_ai', 'conversation:' + identity, history)
            for relative in ('bin/data/lan_portal_state.sqlite3',
                             'bin/data/lighthouse_openclaw/assistant.sqlite3'):
                protected[relative] = (client / relative).read_bytes()
            directory_rows = {'schema': 4, 'app_id': TARGET_APP_ID, 'loaded_at': time.time(), 'people': {
                'white': {'id': 'white', 'name': '人员甲', 'scopes': ['A'], 'selectable': True,
                          'inactive': False, 'login_ids': ['ou_white']},
                'night': {'id': 'night', 'name': '人员乙', 'scopes': ['A'], 'selectable': True,
                          'inactive': False, 'login_ids': ['ou_night']},
            }}
            roster = Mock(return_value=[
                {'name': '人员甲', 'scope': 'A', 'shift': '白', 'open_id': ''},
                {'name': '人员乙', 'scope': 'A', 'shift': '夜', 'open_id': 'ou_night'},
            ])
            sender = Mock(return_value=(True, 'ok', [{'message_id': 'om_fixture', 'chat_id': 'oc_fixture'}]))

            def service():
                return learning.LearningService(root=data / 'learning', cloud=FakeCloud(), send_message=sender,
                    get_login_people=lambda: directory_rows, get_shift_roster=roster,
                    get_portal_url=lambda: 'https://portal.test')

            before = service()
            day = '2026-10-10'
            white_key = f'shift:{day}:白:ou_white'
            night_key = f'shift:{day}:夜:ou_night'
            white_receipt = [{'open_id': 'ou_white', 'message_id': 'om_white', 'chat_id': 'oc_white',
                              'sender_app_id': 'cli_login_fixture', 'ok': True}]
            with before.transaction() as conn:
                before._put('settings', 'main', {'enabled': True, 'publish_time': '13:00',
                    'night_publish_time': '08:00', 'reminder_enabled': False}, conn, False)
                before._put('notification', white_key, {'id': white_key, 'kind': 'shift', 'date': day,
                    'scope': '', 'open_id': 'ou_white', 'shift': '白', 'status': 'sent',
                    'delivery': white_receipt}, conn, False)
                before._put('notification', night_key, {'id': night_key, 'kind': 'shift', 'date': day,
                    'scope': '', 'open_id': 'ou_night', 'shift': '夜', 'status': 'pending'}, conn, False)
                before._put('local', 'shift_roster', {'date': day, 'version': 2, 'people': []}, conn, False)
            database = before.db.read_bytes()

            with patch.object(packaging, 'BUILD_DIR', output):
                packaging.build_patch(root / 'dist', client, 'fixture_update', force_ui_update=True)
            payload = output / 'fixture_update_patch_only'
            expected = (
                'bin/upload_event_module/config.py',
                'bin/upload_event_module/services/remote_patch_updater.py',
                'bin/upload_event_module/services/patch_mirror.py',
                'bin/openclaw_service/assistant/lighthouse_distribution.py',
                'bin/lan_bitable_template_portal/learning.py',
                'bin/lan_bitable_template_portal/learning_reminders.py',
                'bin/lan_bitable_template_portal/learning_routes.py',
                'bin/upload_event_module/services/robot_webhook.py',
                'bin/lan_bitable_template_portal/feishu_assistant.py',
                'bin/lan_bitable_template_portal/feishu_assistant_settings.py',
                'bin/lan_bitable_template_portal/feishu_assistant_worker.py',
                'bin/lan_bitable_template_portal/feishu_delivery.py',
                'bin/lan_bitable_template_portal/learning_weekly_cards.py',
                'bin/lan_bitable_template_portal/plan_convergence_notifications.py',
                'bin/lan_bitable_template_portal/portal_auth.py',
                'bin/lan_bitable_template_portal/module_access.py',
                'bin/lan_bitable_template_portal/lighthouse_bridge.py',
                'bin/lan_bitable_template_portal/server.py',
                'bin/clipflow_backend/main.py',
                'bin/lan_bitable_template_portal/workbench_lite.py',
                'bin/lan_bitable_template_portal/portal_service.py',
                'bin/openclaw_service/assistant/lighthouse_ai.py',
                'bin/openclaw_service/assistant/lighthouse_api.py',
                'bin/openclaw_service/assistant/lighthouse_files.py',
                'bin/openclaw_service/assistant/lighthouse_openclaw.py',
                'bin/openclaw_service/assistant/lighthouse_runtime.py',
                'bin/openclaw_service/assistant/lighthouse_stream.py',
                'bin/openclaw_service/assistant/lighthouse_agent.py',
                'bin/openclaw_service/bridge.py',
                'bin/lan_bitable_template_portal/frontend/dist/index.html',
                'bin/lan_bitable_template_portal/frontend/dist/assets/workspace-v2/entry.json',
            )
            for relative in expected:
                self.assertEqual((payload / relative).read_bytes(), (packaging.PROJECT_ROOT / relative).read_bytes())
            self.assertFalse((payload / 'bin/data').exists())
            meta = json.loads((payload / 'bin/patch_meta.json').read_text(encoding='utf-8'))
            self.assertTrue(meta['restart_required'])
            self.assertEqual(meta['min_version'], '')
            identity_path = 'bin/lan_bitable_template_portal/feishu_assistant.py'
            self.assertEqual(meta['file_sha256'][identity_path],
                             hashlib.sha256((packaging.PROJECT_ROOT / identity_path).read_bytes()).hexdigest())
            # Compression has separate coverage; verify the actual archive without recompressing it twice.
            with patch.object(packaging, 'BUILD_DIR', output), \
                 patch.object(packaging, '_compact_patch_zip', return_value=False):
                archive = packaging._zip_patch_dir(payload)
            with ZipFile(archive) as zipped:
                self.assertEqual(zipped.read(payload.name + '/' + identity_path),
                                 (packaging.PROJECT_ROOT / identity_path).read_bytes())
                self.assertFalse(any(name.startswith(payload.name + '/bin/data/') for name in zipped.namelist()))
            packaging._assert_no_runtime_data_in_output(payload, '学练隔离补丁')

            # Even a stale patch containing runtime files/deletions must not replace user data.
            for relative in (*protected, 'bin/data/learning/learning.sqlite3'):
                path = payload / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(b'must-not-install')
            with (payload / 'patch_manifest.txt').open('a', encoding='utf-8') as manifest:
                manifest.write('\n' + '\n'.join((*protected, 'bin/data/learning/learning.sqlite3')) + '\n')

            class Client(PatchUpdateMixin):
                def _get_app_root_dir(self):
                    return client

            updater = Client()
            updater._last_patch_meta, updater._last_patch_source = meta, 'isolated-test'
            updater.patch_update_finished = SimpleNamespace(emit=Mock())
            guard = Mock()
            guard.finish.return_value = ''
            with patch('openclaw_service.update.AssistantUpdateGuard', return_value=guard), \
                 patch('upload_event_module.services.process_lifetime.lower_current_thread_priority'), \
                 patch('upload_event_module.ui.main_window_patch.config.auto_install_dependencies', False):
                updater._apply_patch_worker(payload)
            self.assertTrue(updater.patch_update_finished.emit.call_args.args[0],
                            updater.patch_update_finished.emit.call_args)
            self.assertTrue(updater._last_patch_requires_restart)
            for relative in expected:
                self.assertEqual((client / relative).read_bytes(), (packaging.PROJECT_ROOT / relative).read_bytes())
            for relative, content in protected.items():
                self.assertEqual((client / relative).read_bytes(), content)
            self.assertEqual((data / 'learning/learning.sqlite3').read_bytes(), database)

            # Load the installed file, with a new auth manager and the preserved databases.
            spec = importlib.util.spec_from_file_location('lan_bitable_template_portal._updated_feishu',
                                                         client / identity_path)
            installed = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(installed)
            harness = PersonnelLoginHarness(data, migrated_grants=False)
            try:
                self.assertEqual(harness.auth.get_session(cookie)['user'], session['user'])
                channel = installed.FeishuAssistant(SimpleNamespace(), SimpleNamespace(
                    state_store=harness.auth._state_store, auth_manager=harness.auth), AsyncMock())
                channel.config = json.loads((data / 'feishu_assistant.json').read_text(encoding='utf-8'))
                channel.messenger = installed.FeishuMessenger(channel.config)
                channel.messenger.headers = AsyncMock(return_value={'Authorization': 'Bearer fixture'})
                transport = SimpleNamespace(post=AsyncMock(return_value=SimpleNamespace(json=lambda: {
                    'code': 0, 'data': {'has_more': False, 'items': [{'record_id': 'recFixture',
                        'fields': {'员工姓名': [{'id': sender_id}]}}]}})))
                channel.messenger.client = AsyncMock(return_value=transport)
                channel._user = Mock(side_effect=AssertionError('Must not require another OAuth login'))
                channel.messenger.sender_name = AsyncMock(side_effect=AssertionError('Must not require contact names'))
                user = asyncio.run(channel._resolve_user({'sender_id': sender_id, 'union_id': ''}))
                self.assertEqual(user['open_id'], identity)
                self.assertEqual(harness.auth.account_profile(identity), profile)
                self.assertEqual(harness.auth._state_store.get_auth_permissions(), permissions)
                assistant = LighthouseAssistant(AssistantStore(assistant_root), Mock(), model=Mock())
                for actor in ({'id': identity}, {'id': user['open_id'], 'channel': 'feishu:oc_Fixture'}):
                    self.assertEqual(assistant._state(actor), history)
                recovered = installed.FeishuAssistant(SimpleNamespace(), channel.runtime, AsyncMock())
                recovered.config = dict(channel.config)
                self.assertEqual(asyncio.run(recovered._resolve_user({'sender_id': sender_id})), user)
                self.assertEqual(harness.auth.get_session(cookie)['user'], session['user'])
            finally:
                harness.auth._cleanup_executor.shutdown(wait=True, cancel_futures=True)
                harness.close()

            after = service()
            self.assertEqual(after._get('notification', white_key)['delivery'], white_receipt)
            self.assertEqual((after.settings()['publish_time'], after.settings()['night_publish_time']), ('13:00', '20:00'))
            for hour, minute in ((8, 0), (13, 0), (19, 59)):
                after.send_notifications(dt.datetime(2026, 10, 10, hour, minute, tzinfo=learning.TZ))
            sender.assert_not_called()
            after.send_notifications(dt.datetime(2026, 10, 10, 20, 0, tzinfo=learning.TZ))
            sender.assert_called_once()
            self.assertEqual(sender.call_args.args[0], 'ou_night')
            self.assertEqual(sender.call_args.args[2], night_key)
            restarted = service()
            restarted.send_notifications(dt.datetime(2026, 10, 10, 20, 1, tzinfo=learning.TZ))
            sender.assert_called_once()
            roster.assert_called_once_with(day)
            self.assertEqual(restarted._get('notification', white_key)['status'], 'sent')
            self.assertEqual(restarted._get('notification', night_key)['status'], 'sent')

    def test_legacy_pending_reminder_reuses_original_uuid_through_real_adapter(self):
        from bin.test_learning_shift_reminders import ShiftReminderTests
        from bin.test_feishu_single_app_delivery import standalone_delivery_fixture
        from upload_event_module.services import robot_webhook
        fixture = ShiftReminderTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        service = fixture.service
        service._sender = None
        key = 'shift:2026-10-10:白:ou_external'
        with service.transaction() as conn:
            service._put('notification', key, {'id': key, 'kind': 'shift', 'date': '2026-10-10',
                'scope': '', 'open_id': 'ou_external', 'shift': '白', 'status': 'pending'}, conn, False)
        with standalone_delivery_fixture('ou_external'), \
             patch('lan_bitable_template_portal.portal_service.external_real_write_guard', return_value={'real_write_allowed': True}), \
             patch.object(robot_webhook, '_get_tenant_access_token', return_value=('fixture', '')), \
             patch.object(robot_webhook, '_request_json', return_value={'code': 0,
                 'data': {'message_id': 'om_original', 'chat_id': 'oc_original'}}) as transport:
            fixture.run_at(13)
        transport.assert_called_once()
        self.assertEqual(transport.call_args.kwargs['json_payload']['uuid'], str(uuid5(NAMESPACE_URL, key)))
        self.assertEqual(transport.call_args.kwargs['json_payload']['receive_id'], 'ou_external')
        self.assertEqual(service._get('notification', key)['status'], 'sent')
        self.assertEqual(service._get('notification', key)['delivery'][0]['message_id'], 'om_original')
