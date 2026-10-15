"""Personal signatures and delegated management, with isolated data and senders."""
import base64
import copy
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlsplit

from bin.test_signature_management import FakeService, signature_bytes
from lan_bitable_template_portal.signature_management import dispatch, SignatureManagementError
from lan_bitable_template_portal.state_store import LanPortalStateStore


class SignatureAccessTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = LanPortalStateStore(Path(self.temp.name) / 'state.sqlite3')
        self.service = FakeService(self.store)
        self.manager = self.service.manager
        self.profiles = {}
        self.blocked = set()
        for oid, level, scope, category in (
            ('super', 'L0', 'H', 'manager'), ('manager', 'L1', 'H', 'manager'),
            ('peerManager', 'L1', 'H', 'manager'), ('crossEngineer', 'L2', 'H', 'engineer'),
            ('supervisorE', 'L3', 'E', 'supervisor'), ('supervisorA', 'L3', 'A', 'supervisor'),
            ('operatorE', 'L4', 'E', 'operator'), ('operatorA', 'L4', 'A', 'operator'),
            ('engineerE', 'L4', 'E', 'engineer'), ('engineerA', 'L4', 'A', 'engineer')):
            self.profiles[oid] = {'level': level, 'home_scope': scope, 'category': category,
                'scope_rules': list('ABCDEH') + ['110'] if level in {'L0', 'L1'} else [scope],
                'role': 'admin' if level in {'L0', 'L1'} else 'building',
                'modules': ['signatures', 'accounts'], 'can_manage_accounts': level != 'L4'}
            self.service.add('rec' + oid, name='同名测试人员', open_id=oid, building=scope + '楼')
        self.auth = SimpleNamespace(account_profile=lambda oid: copy.deepcopy(self.profiles.get(oid, {})),
                                    directory_block_reason=lambda oid: '已离职' if oid in self.blocked else '')
        self.sender = Mock(return_value=(True, '', []))

    def call(self, operation, payload=None, *, actor='operatorE', method='GET'):
        return dispatch(self.manager, method, operation, payload or {}, actor=actor, auth=self.auth,
                        send_text=self.sender, base_url='http://localhost:18766')

    def test_personal_directory_and_account_use_identity_not_name(self):
        result = self.call('people')
        self.assertEqual([p['record_id'] for p in result['people'] if not p['invite_only']], ['recoperatorE'])
        self.assertEqual({p['record_id'] for p in result['people'] if p['invite_only']},
                         {'recsuper', 'recmanager', 'recpeerManager', 'recsupervisorE'})
        self.assertEqual(result['counts']['signed'], 1)
        person = self.call('account')['person']
        self.assertEqual(person['record_id'], 'recoperatorE')
        self.assertTrue(person['is_self'])
        self.assertTrue(person['can_preview'])
        self.assertTrue(person['can_request'])
        for key in ('raw_fields', 'signature_file_token', 'signature_version', 'signature_preview_url'):
            self.assertNotIn(key, person)

    def test_supervisor_only_sees_own_lower_level_people(self):
        listing = self.call('people', actor='supervisorE')
        self.assertEqual({p['record_id'] for p in listing['people'] if not p['invite_only']},
                         {'recsupervisorE', 'recoperatorE', 'recengineerE'})
        self.assertEqual([row['value'] for row in listing['recipients']], ['E'])
        self.assertEqual(listing['counts']['signed'], 3)
        self.assertEqual(self.call('people', {'scope': 'A'}, actor='supervisorE')['count'], 0)
        refreshed = self.call('refresh', actor='supervisorE', method='POST')
        self.assertNotIn('resolved', refreshed)
        self.assertNotIn('staff', refreshed)
        self.assertEqual(sum(not p['invite_only'] for p in refreshed['people']), 3)

    def test_crafted_preview_account_and_request_cannot_cross_buildings_or_levels(self):
        for actor, target in (('operatorE', 'operatorA'), ('supervisorE', 'operatorA'),
                              ('supervisorE', 'supervisorA'), ('manager', 'peerManager')):
            for operation, method, payload in (
                ('preview', 'GET', {'source': 'staff', 'record_id': 'rec' + target}),
                ('account', 'GET', {'open_id': target}),
                ('requests', 'POST', {'source': 'staff', 'record_id': 'rec' + target, 'operation_id': 'attempt'})):
                with self.subTest(actor=actor, target=target, operation=operation):
                    with self.assertRaises(SignatureManagementError) as error:
                        self.call(operation, payload, actor=actor, method=method)
                    self.assertEqual(error.exception.status, 403)
        self.sender.assert_not_called()
        self.assertEqual(self.service.uploads, 0)

    def test_h_engineer_can_request_lower_engineers_but_not_view_their_ink(self):
        listing = self.call('people', actor='crossEngineer')
        self.assertEqual({p['record_id'] for p in listing['people'] if not p['invite_only']},
                         {'reccrossEngineer', 'recengineerA', 'recengineerE'})
        for row in listing['people']:
            self.assertTrue(row['can_request'])
            self.assertEqual(row['can_preview'], row['is_self'])
        result = self.call('requests', {'source': 'staff', 'record_id': 'recengineerA', 'operation_id': 'engineer-request'},
                           actor='crossEngineer', method='POST')
        self.assertEqual(result['status'], 'sent')
        self.assertEqual(self.sender.call_args.args[1], ['engineerA'])

    def test_subordinate_can_invite_actual_superiors_without_ink_or_history_access(self):
        for actor, superior in (('operatorE', 'supervisorE'), ('operatorE', 'manager'),
                                ('operatorE', 'super'), ('manager', 'super'), ('engineerA', 'crossEngineer')):
            with self.subTest(actor=actor, superior=superior):
                candidate = next(row for row in self.call('people', actor=actor)['people']
                                 if row['record_id'] == 'rec' + superior)
                self.assertTrue(candidate['invite_only'])
                self.assertTrue(candidate['can_request'])
                self.assertFalse(candidate['can_preview'])
                for key in ('has_signature', 'signature_count', 'request', 'effective_has_signature', 'identity_version'):
                    self.assertNotIn(key, candidate)
                result = self.call('requests', {'source': 'staff', 'record_id': candidate['record_id'],
                    'operation_id': actor + superior}, actor=actor, method='POST')
                self.assertEqual(result['status'], 'sent')
                self.assertEqual(self.sender.call_args.args[1], [superior])
                with self.assertRaises(SignatureManagementError):
                    self.call('preview', {'source': 'staff', 'record_id': candidate['record_id']}, actor=actor)
                with self.assertRaises(SignatureManagementError):
                    self.call('account', {'open_id': superior}, actor=actor)
        self.assertEqual(self.service.uploads, 0)

    def test_upward_invitation_never_delivers_superior_signing_token_to_requester_or_relay(self):
        for recipient in ('duty:E', 'staff:recoperatorE', 'staff:recoperatorA'):
            with self.subTest(recipient=recipient), self.assertRaises(SignatureManagementError) as error:
                self.call('requests', {'source': 'staff', 'record_id': 'recsupervisorE',
                    'operation_id': 'upward-relay', 'recipient': recipient}, method='POST')
            self.assertEqual(error.exception.status, 403)
        self.sender.assert_not_called()
        self.assertEqual(self.store.list_documents('signature_management_request'), [])

    def test_self_re_sign_uses_one_use_flow_and_keeps_old_signature_until_saved(self):
        before = copy.deepcopy(self.service.staff['recoperatorE'])
        result = self.call('self-start', method='POST')
        url = urlsplit(result['link_url'])
        self.assertEqual(url.path, '/signature')
        self.assertFalse(url.netloc)
        token = parse_qs(url.fragment)['token'][0]
        request_id = parse_qs(url.query)['request_id'][0]
        self.assertEqual(self.service.staff['recoperatorE'], before)
        self.assertEqual(self.service.uploads, 0)
        self.sender.assert_not_called()
        saved = self.store.get_document('signature_management_request', request_id)
        self.assertNotIn(token, json.dumps(saved))
        submit = {'request_id': request_id, 'token': token, 'confirmed_self': True,
                  'signature_png': 'data:image/png;base64,' + base64.b64encode(signature_bytes()).decode()}
        result = dispatch(self.manager, 'POST', 'submit', submit)
        self.assertEqual(result['status'], 'completed')
        self.assertEqual(dispatch(self.manager, 'POST', 'submit', submit)['status'], 'completed')
        self.assertEqual(self.service.uploads, 1)
        self.assertEqual(self.call('account')['person']['request']['status'], 'completed')
        self.assertEqual(self.service.staff['recoperatorA']['手写签名'][0]['file_token'], 'old-recoperatorA')

    def test_self_start_rejects_other_identity_and_duplicate_identity(self):
        with self.assertRaises(SignatureManagementError):
            self.call('self-start', {'open_id': 'operatorA'}, method='POST')
        self.service.add('recDuplicate', open_id='operatorE')
        self.manager.invalidate()
        self.assertIsNone(self.call('account')['person'])
        with self.assertRaises(SignatureManagementError):
            self.call('self-start', method='POST')
        self.assertEqual(self.store.list_documents('signature_management_request'), [])

    def test_explicit_temporary_mapping_supports_owner_but_not_another_user(self):
        self.service.add('recoperatorE', signature=False, open_id='operatorE')
        self.service.add('recLinked', source='external', origin='recoperatorE')
        person = self.call('account')['person']
        self.assertEqual(person['record_id'], 'recLinked')
        self.assertTrue(person['can_preview'])
        self.assertEqual(self.call('self-start', method='POST')['person_key'], 'external:recLinked')
        with self.assertRaises(SignatureManagementError):
            self.call('preview', {'source': 'external', 'record_id': 'recLinked'}, actor='operatorA')

    def test_module_revocation_and_departure_take_effect_on_next_request(self):
        self.call('account')
        self.profiles['operatorE']['modules'] = []
        with self.assertRaises(SignatureManagementError):
            self.call('self-start', method='POST')
        self.blocked.add('operatorA')
        self.assertNotIn('recoperatorA', {p['record_id'] for p in self.call('people', actor='super')['people']})

    def test_shared_duty_account_is_not_a_signer(self):
        duty = self.manager.d.BUILDING_OPEN_ID_MAP['H']
        self.profiles[duty] = self.profiles['manager']
        result = self.call('account', actor=duty)
        self.assertIsNone(result['person'])
        self.assertIn('共享账号', result['reason'])
        with self.assertRaises(SignatureManagementError):
            self.call('self-start', actor=duty, method='POST')

    def test_manager_request_is_idempotent_and_does_not_remove_ink(self):
        payload = {'source': 'staff', 'record_id': 'recoperatorE', 'operation_id': 'manager-request'}
        before = copy.deepcopy(self.service.staff['recoperatorE'])
        first = self.call('requests', payload, actor='supervisorE', method='POST')
        self.assertEqual(self.call('requests', payload, actor='supervisorE', method='POST'), first)
        self.assertEqual(self.sender.call_count, 1)
        self.assertEqual(self.sender.call_args.args[1], ['operatorE'])
        self.assertEqual(self.service.staff['recoperatorE'], before)

    def test_non_vnet_linked_person_can_receive_request_and_sign_on_web(self):
        self.service.add('recoperatorE', open_id='operatorE', account_nature='外部账号')
        self.assertTrue(self.call('account')['person']['can_receive_message'])
        result = self.call('requests', {'source': 'staff', 'record_id': 'recoperatorE', 'operation_id': 'external-request'},
                           actor='supervisorE', method='POST')
        self.assertEqual(result['status'], 'sent')
        self.assertEqual(self.sender.call_args.args[1], ['operatorE'])
        self.assertEqual(self.call('self-start', method='POST')['status'], 'ready')

    def test_relay_cannot_send_a_link_outside_managed_scope(self):
        for recipient in ('duty:A', 'staff:recoperatorA', 'staff:recpeerManager'):
            with self.subTest(recipient=recipient), self.assertRaises(SignatureManagementError):
                self.call('requests', {'source': 'staff', 'record_id': 'recoperatorE', 'recipient': recipient,
                                      'operation_id': 'relay'}, actor='supervisorE', method='POST')
        self.sender.assert_not_called()

    def test_failure_or_unloaded_directory_is_not_reported_as_unsigned(self):
        self.service.staff_error = True
        with self.assertRaises(SignatureManagementError) as error:
            self.call('account')
        self.assertEqual(error.exception.status, 503)

    def test_public_signed_link_works_without_login_but_management_stays_protected(self):
        from fastapi.testclient import TestClient
        from clipflow_backend.main import FastAPIPortalController, PortalRuntime
        from public_access.ingress import PublicGuard, MARKER
        from bin.test_public_access import SETTINGS, SECRET
        result = self.call('self-start', method='POST')
        parsed = urlsplit(result['link_url'])
        request_id, token = parse_qs(parsed.query)['request_id'][0], parse_qs(parsed.fragment)['token'][0]
        controller = FastAPIPortalController(host='127.0.0.1', port=18766)
        with patch.object(PortalRuntime.service, '_signature_management', self.manager, create=True), \
                patch.object(controller, '_current_session', return_value=None):
            app = PublicGuard(controller._build_app(), SECRET, SETTINGS, lambda request: None)
            client = TestClient(app, base_url=SETTINGS.origin, client=('127.0.0.1', 1234))
            try:
                client.headers.update({MARKER.decode(): SECRET, 'Origin': SETTINGS.origin})
                endpoint = '/api/signatures/management/'
                valid = client.get(endpoint + 'request', params={'request_id': request_id}, headers={'X-Signature-Token': token})
                self.assertEqual(valid.status_code, 200, valid.text)
                self.assertEqual(client.get(endpoint + 'request', params={'request_id': request_id}).status_code, 403)
                self.assertEqual(client.get(endpoint + 'people').status_code, 401)
                self.assertEqual(client.get(endpoint + 'account').status_code, 401)
                self.assertEqual(client.post(endpoint + 'self-start', json={}).status_code, 401)
                saved = client.post(endpoint + 'submit', json={'request_id': request_id, 'token': token,
                    'confirmed_self': True, 'signature_png': 'data:image/png;base64,' + base64.b64encode(signature_bytes()).decode()})
                self.assertEqual(saved.status_code, 200, saved.text)
                self.assertEqual(saved.json()['data']['status'], 'completed')
                self.assertEqual(self.service.uploads, 1)
                self.sender.assert_not_called()
            finally:
                client.close()


if __name__ == '__main__':
    unittest.main()
