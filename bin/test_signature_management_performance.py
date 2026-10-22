"""Request-scoped signature reads against synthetic directories and isolated SQLite."""
import copy
import json
from pathlib import Path
import socket
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from bin.test_signature_management import FakeService
from bin.test_personnel_password_login import PersonnelLoginHarness
from lan_bitable_template_portal.signature_management import dispatch, SignatureManagementError
from lan_bitable_template_portal.portal_auth import LI_SHILONG_OPEN_ID


class SignatureReadPerformanceTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(patch('socket.socket.connect', side_effect=AssertionError('No network in signature tests')))
        self.folder = tempfile.TemporaryDirectory(); self.addCleanup(self.folder.cleanup)
        self.enterContext(patch('lan_bitable_template_portal.portal_auth.config',
            SimpleNamespace(app_id='cli_signature_fixture', app_secret='fixture-only')))
        self.h = PersonnelLoginHarness(Path(self.folder.name)); self.addCleanup(self.h.close)
        self.auth = self.h.auth
        self.service = FakeService(self.auth._state_store); self.manager = self.service.manager
        people, permissions = {}, self.auth._load_permissions_locked()
        for i in range(1200):
            oid, rid = f'ou_signature{i}', f'recSignature{i}'
            self.service.add(rid, name=f'Fixture {i:04}', open_id=oid, building='A楼')
            people[rid] = {'id': rid, 'name': f'Fixture {i:04}', 'login_ids': [oid], 'scopes': ['A'],
                           'selectable': True, 'inactive': False, 'account_nature': 'VNET', 'position': '运维值班员'}
            permissions['users'][oid] = {'name': f'Fixture {i:04}', 'access_version': 1, 'enabled': True,
                'level': 'L4', 'category': 'operator', 'modules': ['signatures'], 'scopes': ['A'], 'role': 'building'}
        self.snapshot = {'schema': 4, 'app_id': 'cli_signature_fixture', 'people': people, 'removed_identities': {}}
        self.h.login.snapshot = self.snapshot
        self.auth._account_directory_provider = Mock(return_value=self.snapshot)
        self.auth._save_permissions_locked(permissions)
        self.manager.directory()

    def read(self, actor=LI_SHILONG_OPEN_ID, payload=None, operation='people'):
        return dispatch(self.manager, 'GET', operation, payload or {}, actor=actor, auth=self.auth)

    def test_large_warm_directory_has_one_permission_snapshot_and_page_only_enrichment(self):
        with patch.object(self.auth, '_load_permissions_locked', wraps=self.auth._load_permissions_locked) as permissions, \
             patch.object(self.manager, '_identity_version', wraps=self.manager._identity_version) as versions, \
             patch.object(self.auth, '_account_profile', wraps=self.auth._account_profile) as profiles:
            start = time.perf_counter()
            result = self.read()
            elapsed = time.perf_counter() - start
        self.assertEqual(result['count'], 1200)
        self.assertEqual(len(result['people']), 50)
        self.assertEqual(permissions.call_count, 1)
        self.assertEqual(self.auth._account_directory_provider.call_count, 1)
        self.assertEqual(versions.call_count, 50)
        self.assertEqual(profiles.call_count, 1201)
        self.assertIsNone(self.auth._account_read_state())
        self.assertEqual(self.h.service._request_payload_calls, 0)
        for private in ('raw_fields', 'signature_file_token', 'signature_version', 'signature_preview_url', '密钥'):
            self.assertNotIn(private, json.dumps(result, ensure_ascii=False))
        print(f'SIGNATURE_FAKE_BENCH: 1200 people, {elapsed:.3f}s, permissions=1, identity_enrichment=50, remote_reads=0')

    def test_each_read_rechecks_actor_and_revocation_and_does_not_reuse_admin_result(self):
        self.read()
        own = self.read('ou_signature0')
        self.assertEqual(own['count'], 1200)
        self.assertEqual(len(own['people']), 50)
        self.assertEqual([row['record_id'] for row in own['people'] if not row['directory_only']], ['recSignature0'])
        permissions = self.auth._load_permissions_locked()
        permissions['users']['ou_signature0']['modules'] = []
        self.auth._save_permissions_locked(permissions)
        with self.assertRaises(SignatureManagementError) as error:
            self.read('ou_signature0')
        self.assertEqual(error.exception.status, 403)
        self.assertIsNone(self.auth._account_read_state())
        self.assertEqual(self.read('ou_signature1', {'q': 'Fixture 0001'})['people'][0]['record_id'], 'recSignature1')

    def test_failed_refresh_keeps_safe_previous_metadata_but_live_permission_filter(self):
        before = self.read('ou_signature0')
        self.service.staff_error = True
        result = self.read('ou_signature0', {'refresh': True})
        self.assertFalse(result['sources']['staff']['ok'])
        self.assertEqual(result['people'], before['people'])
        self.snapshot['people']['recSignature0'].update(selectable=False, disabled_reason='Fixture revoked')
        with self.assertRaises(SignatureManagementError) as error: self.read('ou_signature0')
        self.assertEqual(error.exception.status, 403)
        self.service.staff_error = False
        self.assertTrue(self.read(payload={'refresh': True})['sources']['staff']['ok'])

    def test_all_recipient_picker_and_mutations_keep_live_checks(self):
        self.service.add('recTemporary', source='external', name='Shared fixture', signature=False)
        self.manager.directory(refresh=True)
        result = self.read('ou_signature0', {'source': 'external', 'record_id': 'recTemporary'}, 'recipients')
        self.assertEqual(len(result['people']), 1200)
        self.assertEqual(set(result['people'][0]), {'record_id', 'name', 'employee_no', 'building', 'position', 'team', 'specialty'})
        self.snapshot['people']['recSignature1199'].update(selectable=False, disabled_reason='Fixture revoked')
        result = self.read('ou_signature0', {'source': 'external', 'record_id': 'recTemporary'}, 'recipients')
        self.assertEqual(len(result['people']), 1199)
        with patch.object(self.auth, 'account_listing_read', side_effect=AssertionError('Mutation must check live permissions')):
            with self.assertRaises(SignatureManagementError):
                dispatch(self.manager, 'POST', 'requests', {'source': 'staff', 'record_id': 'recSignature1'},
                    actor='ou_signature0', auth=self.auth, send_text=Mock(side_effect=AssertionError('No send')))


if __name__ == '__main__':
    unittest.main()
