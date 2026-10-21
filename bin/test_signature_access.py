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

    def c_signer_fixture(self):
        self.hr = {}
        for oid, scope, level, category in (('signerC', 'C', 'L4', 'operator'),
                ('sameNameD', 'D', 'L4', 'operator'), ('supervisorC', 'C', 'L3', 'supervisor'),
                ('supervisorD', 'D', 'L3', 'supervisor')):
            self.profiles[oid] = {'level': level, 'home_scope': scope, 'category': category,
                'scope_rules': [scope], 'role': 'building', 'modules': ['signatures', 'accounts'],
                'can_manage_accounts': level == 'L3'}
            self.hr[oid] = {'scopes': [scope]}
            self.service.add('rec' + oid, name='杨占山' if level == 'L4' else oid,
                             open_id=oid, building='D楼')
        self.auth.directory_person = Mock(side_effect=lambda oid: copy.deepcopy(self.hr.get(oid, {})))

    def test_signer_presentation_and_directory_filters_use_account_c_not_cached_d(self):
        from lan_bitable_template_portal.signature_access import SignatureAccess
        self.c_signer_fixture()
        before = copy.deepcopy(self.service.staff)
        access = SignatureAccess(self.manager, self.auth, 'super')
        access.bind(self.manager.directory())
        raw = self.manager.person('staff', 'recsignerC')
        self.assertEqual(raw['building'], 'D楼')
        presented = access.presentation(raw)
        self.assertEqual(presented['building'], 'C楼')
        self.assertEqual(presented['scope_text'], 'C楼')
        for key in ('name', 'source', 'record_id', 'person_key'):
            self.assertEqual(presented[key], raw[key])
        listing = self.call('people', {'scope': 'C', 'q': '杨占山'}, actor='super')
        self.assertEqual([row['record_id'] for row in listing['people']], ['recsignerC'])
        self.assertEqual(listing['people'][0]['building'], 'C楼')
        other = self.call('people', {'scope': 'D', 'q': '杨占山'}, actor='super')
        self.assertEqual([row['record_id'] for row in other['people']], ['recsameNameD'])
        self.assertEqual(self.service.staff, before)
        self.assertEqual(self.service.uploads, 0)
        self.sender.assert_not_called()

    def test_presentation_falls_back_to_home_scope_without_directory_person_interface(self):
        self.c_signer_fixture()
        del self.auth.directory_person
        self.profiles['signerC']['scope_rules'] = list('ABCDEH') + ['110']
        person = self.call('account', actor='signerC')['person']
        self.assertEqual(person['building'], 'C楼')
        self.assertEqual(person['scope_text'], 'C楼')

    def test_admin_signer_multi_home_presentation_keeps_all_trusted_scopes_not_admin_grants(self):
        self.c_signer_fixture()
        self.hr['manager'] = {'scopes': ['C', 'D']}
        self.service.staff['recmanager']['楼栋'] = 'D楼'
        before = copy.deepcopy(self.profiles['manager'])
        listing = self.call('people', {'scope': 'C'}, actor='super')
        row = next(row for row in listing['people'] if row['record_id'] == 'recmanager')
        self.assertEqual(set(self.service._building_codes_from_value(row['building'])), {'C', 'D'})
        self.assertEqual(row['scope_text'], row['building'])
        self.assertNotIn('recmanager', {row['record_id'] for row in self.call('people', {'scope': 'H'}, actor='super')['people']})
        sent = self.call('requests', {'source': 'staff', 'record_id': 'recmanager',
            'operation_id': 'multiHomePresentation'}, actor='super', method='POST')
        self.assertEqual(set(self.service._building_codes_from_value(sent['building'])), {'C', 'D'})
        self.assertIn('楼栋：' + sent['building'], self.sender.call_args.args[0].splitlines())
        self.assertEqual(self.profiles['manager'], before)

    def test_linked_temporary_signer_uses_c_but_unlinked_same_name_keeps_own_building(self):
        self.c_signer_fixture()
        self.service.add('recsignerC', name='杨占山', signature=False, open_id='signerC', building='D楼')
        self.service.add('recLinkedC', source='external', origin='recsignerC', name='杨占山', building='D楼')
        self.service.add('recUnlinkedD', source='external', name='杨占山', building='D楼')
        person = self.call('account', actor='signerC')['person']
        self.assertEqual((person['source'], person['record_id'], person['building']), ('external', 'recLinkedC', 'C楼'))
        listing = self.call('people', {'q': '杨占山'}, actor='super')['people']
        rows = {row['record_id']: row for row in listing}
        self.assertEqual(rows['recLinkedC']['building'], 'C楼')
        self.assertEqual(rows['recUnlinkedD']['building'], 'D楼')
        self.assertEqual(rows['recsameNameD']['building'], 'D楼')
        with self.assertRaises(SignatureManagementError) as error:
            self.call('preview', {'source': 'external', 'record_id': 'recLinkedC'}, actor='sameNameD')
        self.assertEqual(error.exception.status, 403)
        self.sender.assert_not_called()
        sent = self.call('requests', {'source': 'external', 'record_id': 'recLinkedC',
            'operation_id': 'linkedHomePresentation'}, actor='super', method='POST')
        self.assertEqual(sent['building'], 'C楼')
        self.assertEqual(self.sender.call_args.args[1], ['signerC'])
        self.assertIn('楼栋：C楼', self.sender.call_args.args[0].splitlines())

    def test_self_start_uses_presented_c_without_changing_signature_or_password_fields(self):
        self.c_signer_fixture()
        self.service.staff['recsignerC']['密码'] = 'synthetic-existing-password-hash'
        before = copy.deepcopy(self.service.staff)
        result = self.call('self-start', actor='signerC', method='POST')
        self.assertEqual(result['building'], 'C楼')
        self.assertEqual(result['name'], '杨占山')
        saved = self.store.get_document('signature_management_request', result['request_id'])
        self.assertEqual((saved['source'], saved['record_id'], saved['person_key']), ('staff', 'recsignerC', 'staff:recsignerC'))
        self.assertEqual(saved['person']['scope_text'], 'C楼')
        self.assertEqual(self.service.staff, before)
        self.assertEqual(self.service.uploads, 0)
        self.sender.assert_not_called()

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


class MopSignatureInvitationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = LanPortalStateStore(Path(self.temp.name) / 'state.sqlite3')
        self.service = FakeService(self.store)
        self.manager = self.service.manager
        self.service.add('recReceiver', name='接收人', open_id='receiver', building='E楼')
        self.service.add('recOther', name='其他楼接收人', open_id='other', building='A楼')
        # Eligible staff outside the actor's building stay selectable as MOP recipients.
        self.service.add('recCross', name='跨楼正式人员', open_id='cross', building='A楼')
        self.service.add('recExternal', name='非VNET人员', open_id='nonVnet', building='A楼', account_nature='外部账号')
        self.service.add('recNoOpenId', name='缺少身份人员', open_id='', building='A楼')
        self.service._engineer_month_maintenance_notices = Mock(return_value=[
            {'notice_key': 'noticeE', 'building': 'E楼', 'building_codes': ['E']},
            {'notice_key': 'noticeA', 'building': 'A楼', 'building_codes': ['A']}])
        self.modules = ['engineer_mop']
        self.blocked = set()
        self.auth = SimpleNamespace(account_profile=lambda actor: {'modules': self.modules, 'scope_rules': ['E']},
                                    directory_block_reason=lambda actor: '已离职' if actor in self.blocked else '')
        self.sender = Mock(return_value=(True, '', []))
        self.payload = {'scope': 'E', 'notice_key': 'noticeE', 'role': 'implementer', 'name': '新增外部人员',
                        'specialty': '暖通', 'recipient': 'staff:recReceiver',
                        'operation_id': 'createTest', 'send_operation_id': 'sendTest'}

    def call(self, operation, payload=None, *, actor='mopOperator', method=None):
        return dispatch(self.manager, method or ('POST' if operation == 'mop-invite' else 'GET'), operation,
                        self.payload if payload is None else payload, actor=actor, auth=self.auth,
                        send_text=self.sender, base_url='http://localhost:18766')

    def recipient_values(self, payload=None, *, actor='mopOperator'):
        options = self.call('mop-people', payload, actor=actor)
        for item in options['recipients']:
            self.assertEqual(set(item), {'value', 'label'})
            self.assertNotIn('open_id', str(item))
        return {item['value'] for item in options['recipients']}

    def test_mop_only_user_can_create_invite_and_sign_without_directory_management(self):
        options = self.call('mop-people')
        self.assertEqual(options['scope_options'], [{'value': 'E', 'label': 'E楼'}])
        # Anyone eligible can receive the invitation, not only the sender's own building.
        self.assertEqual({p['value'] for p in options['recipients']},
                         {'duty:E', 'staff:recReceiver', 'staff:recCross', 'staff:recOther'})
        self.assertNotIn('open_id', str(options))
        result = self.call('mop-invite')
        self.assertEqual(result['request']['status'], 'sent')
        self.assertFalse(result['person']['has_signature'])
        self.assertEqual(self.sender.call_args.args[1], ['receiver'])
        self.assertEqual(self.service.creates, 1)
        pending = self.call('mop-list')['items']
        self.assertEqual(len(pending), 1)
        self.assertFalse(pending[0]['has_signature'])
        token = self.sender.call_args.args[0].rsplit('#token=', 1)[-1]
        saved = dispatch(self.manager, 'POST', 'submit', {
            'request_id': result['request']['request_id'], 'token': token, 'confirmed_self': True,
            'signature_png': 'data:image/png;base64,' + base64.b64encode(signature_bytes()).decode()})
        self.assertEqual(saved['status'], 'completed')
        signed = self.call('mop-list')['items'][0]
        self.assertEqual(signed['status'], 'signed')
        self.assertTrue(signed['has_signature'])
        self.assertEqual(signed['record_id'], result['person']['record_id'])
        self.assertEqual(self.service.uploads, 1)
        repeated = self.call('mop-invite')
        self.assertEqual(repeated['request']['status'], 'completed')
        self.assertTrue(repeated['person']['has_signature'])
        self.assertEqual(self.service.creates, 1)
        self.assertEqual(self.sender.call_count, 1)
        with self.assertRaises(SignatureManagementError):
            self.call('temporary', {'name': 'other', 'building': 'E楼', 'operation_id': 'forbidden'}, method='POST')

    def test_duplicate_clicks_and_concurrent_retries_do_not_create_or_send_twice(self):
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=3) as pool:
            results = list(pool.map(lambda _: self.call('mop-invite'), range(3)))
        self.assertEqual(len({r['person']['record_id'] for r in results}), 1)
        self.assertEqual(self.service.creates, 1)
        self.assertEqual(self.sender.call_count, 1)
        self.assertEqual(len(self.call('mop-list')['items']), 1)

    def test_failed_delivery_explicit_retry_reuses_person(self):
        self.sender.return_value = (False, '发送失败', [])
        first = self.call('mop-invite')
        self.assertEqual(first['request']['status'], 'send_failed')
        self.assertFalse(self.call('mop-list')['items'][0]['has_signature'])
        self.sender.return_value = (True, '', [])
        second = self.call('mop-invite', {**self.payload, 'send_operation_id': 'retrySend',
                                        'expected_request_id': first['request']['request_id']})
        self.assertEqual(second['request']['status'], 'sent')
        self.assertEqual(second['person']['record_id'], first['person']['record_id'])
        self.assertEqual(self.service.creates, 1)
        self.assertEqual(self.sender.call_count, 2)

    def test_unconfirmed_delivery_rechecks_original_request_without_resending(self):
        self.sender.side_effect = TimeoutError('response lost')
        first = self.call('mop-invite')
        self.assertEqual(first['request']['status'], 'send_unknown')
        self.assertEqual(self.call('mop-invite')['request']['status'], 'send_unknown')
        self.assertEqual(self.sender.call_count, 1)
        self.assertEqual(self.service.creates, 1)

    def test_module_scope_notice_recipient_and_actor_checks_precede_writes(self):
        for changes in ({'scope': 'A'}, {'scope': 'C'}, {'notice_key': 'noticeA'}, {'notice_key': 'missing'},
                        {'recipient': 'duty:A'}, {'recipient': 'duty:missing'}, {'recipient': 'staff:missing'},
                        {'recipient': 'staff:recExternal'}, {'recipient': 'staff:recNoOpenId'},
                        {'recipient': 'ou_arbitrary'}, {'recipient': ''},
                        {'role': 'inspector'}, {'name': ''}, {'operation_id': 'x' * 128}):
            with self.subTest(changes=changes), self.assertRaises(SignatureManagementError):
                self.call('mop-invite', {**self.payload, **changes})
        # A valid recipient from another building is accepted only when every other
        # guard passes; the notice/building scoping still rejects it first.
        self.assertTrue({'staff:recOther', 'staff:recCross'} <= self.recipient_values())
        self.modules = ['signatures']
        with self.assertRaises(SignatureManagementError):
            self.call('mop-invite')
        self.modules = ['engineer_mop']
        # A blocked actor is rejected at the entry check, on a notice that would otherwise
        # resolve, so the denial cannot be mistaken for a notice/building failure.
        self.blocked = {'mopOperator'}
        with self.assertRaises(SignatureManagementError) as error:
            self.call('mop-invite', {**self.payload, 'recipient': 'staff:recCross'})
        self.assertEqual(error.exception.status, 403)
        # Blocking one recipient does not remove the other eligible recipients.
        self.blocked = {'receiver', 'cross'}
        self.assertEqual(self.staff_recipients(), ['staff:recOther'])
        self.blocked = set()
        self.assertEqual(self.service.creates, 0)
        self.assertEqual(self.store.list_documents('signature_management_request'), [])
        self.sender.assert_not_called()

    def test_invitation_lists_remain_account_and_notice_isolated_after_reload(self):
        self.call('mop-invite')
        self.assertEqual(self.call('mop-list', actor='anotherOperator')['items'], [])
        self.assertEqual(self.call('mop-list', {**self.payload, 'notice_key': 'other'})['items'], [])
        from lan_bitable_template_portal.signature_management import SignatureManagement
        self.manager = SignatureManagement(self.service)
        self.assertEqual(self.call('mop-list')['items'][0]['name'], self.payload['name'])

    def test_directory_failure_never_creates_or_claims_signed(self):
        self.service.staff_error = True
        with self.assertRaises(SignatureManagementError) as error:
            self.call('mop-invite')
        self.assertEqual(error.exception.status, 503)
        self.assertEqual(self.service.creates, 0)
        self.sender.assert_not_called()

    def test_new_context_operations_do_not_grant_signature_management(self):
        from lan_bitable_template_portal.module_access import required_modules
        for operation in ('mop-people', 'mop-list', 'mop-invite'):
            self.assertEqual(required_modules('/api/signatures/management/' + operation), {'engineer_mop'})
        self.assertEqual(required_modules('/api/signatures/management/temporary'), {'signatures'})

    def test_failed_request_can_resume_after_restart_without_creating_another_person(self):
        self.sender.return_value = (False, '发送失败', [])
        first = self.call('mop-invite')
        from lan_bitable_template_portal.signature_management import SignatureManagement
        self.manager = SignatureManagement(self.service)
        row = self.call('mop-list')['items'][0]
        self.assertEqual(row['operation_id'], self.payload['operation_id'])
        self.assertEqual(row['send_operation_id'], self.payload['send_operation_id'])
        self.assertEqual(row['recipient'], self.payload['recipient'])
        self.assertEqual(row['request_status'], 'send_failed')
        self.sender.return_value = (True, '', [])
        resumed = self.call('mop-invite', {**self.payload, 'operation_id': row['operation_id'],
            'send_operation_id': 'afterRestart', 'expected_request_id': row['request_id']})
        self.assertEqual(resumed['request']['status'], 'sent')
        self.assertEqual(resumed['person']['record_id'], first['person']['record_id'])
        self.assertEqual(self.service.creates, 1)
        self.assertEqual(self.sender.call_count, 2)

    def test_old_attempt_cannot_replace_newer_active_request(self):
        self.sender.return_value = (False, '发送失败', [])
        first = self.call('mop-invite')
        self.sender.return_value = (True, '', [])
        current = self.call('mop-invite', {**self.payload, 'send_operation_id': 'newerAttempt',
            'expected_request_id': first['request']['request_id']})
        repeated = self.call('mop-invite')
        self.assertEqual(repeated['request']['request_id'], current['request']['request_id'])
        self.assertEqual(repeated['request']['status'], 'sent')
        row = self.call('mop-list')['items'][0]
        self.assertEqual(row['request_id'], current['request']['request_id'])
        self.assertEqual(row['request_status'], 'sent')
        self.assertEqual(self.sender.call_count, 2)
        self.assertEqual(self.service.creates, 1)

    def test_simultaneous_retries_with_different_ids_send_only_one_new_link(self):
        from concurrent.futures import ThreadPoolExecutor
        self.sender.return_value = (False, '发送失败', [])
        first = self.call('mop-invite')
        self.sender.return_value = (True, '', [])
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda name: self.call('mop-invite', {**self.payload,
                'send_operation_id': name, 'expected_request_id': first['request']['request_id']}),
                ['retryTabOne', 'retryTabTwo']))
        self.assertEqual(len({r['request']['request_id'] for r in results}), 1)
        self.assertEqual(self.sender.call_count, 2)
        self.assertEqual(self.service.creates, 1)

    def test_expired_link_is_labelled_and_explicit_resend_reuses_original_person(self):
        import time
        from lan_bitable_template_portal.signature_management import REQUEST_NS
        first = self.call('mop-invite')
        request_id = first['request']['request_id']
        request = self.store.get_document(REQUEST_NS, request_id)
        request['expires_at'] = time.time() - 60
        self.store.put_document(REQUEST_NS, request_id, request)
        row = self.call('mop-list')['items'][0]
        self.assertEqual(row['request_status'], 'expired')
        self.assertIn('过期', row['signature_reason'])
        result = self.call('mop-invite', {**self.payload, 'send_operation_id': 'renewed',
            'expected_request_id': request_id})
        self.assertEqual(result['request']['status'], 'sent')
        self.assertEqual(result['person']['record_id'], first['person']['record_id'])
        self.assertEqual(self.service.creates, 1)
        self.assertEqual(self.sender.call_count, 2)

    def test_unknown_result_never_becomes_an_implicit_resend(self):
        self.sender.side_effect = TimeoutError('response lost')
        first = self.call('mop-invite')
        row = self.call('mop-list')['items'][0]
        self.assertEqual(row['request_status'], 'send_unknown')
        same = self.call('mop-invite', {**self.payload, 'expected_request_id': row['request_id']})
        changed = self.call('mop-invite', {**self.payload, 'send_operation_id': 'doNotSend',
            'expected_request_id': row['request_id']})
        for result in (same, changed):
            self.assertEqual(result['request']['request_id'], first['request']['request_id'])
            self.assertEqual(result['request']['status'], 'send_unknown')
        self.assertEqual(self.sender.call_count, 1)
        self.assertEqual(self.service.creates, 1)

    def test_legacy_rows_without_resume_metadata_retain_their_creation_id(self):
        from lan_bitable_template_portal.mop_signature_invitations import NAMESPACE
        self.sender.return_value = (False, '发送失败', [])
        first = self.call('mop-invite')
        stored = self.store.list_documents(NAMESPACE)[0]
        value = stored['payload']
        value.pop('operation_id', None)
        value.pop('recipient', None)
        self.store.put_document(NAMESPACE, stored['key'], value)
        row = self.call('mop-list')['items'][0]
        self.assertEqual(row['operation_id'], self.payload['operation_id'])
        self.assertEqual(row['recipient'], '')
        self.assertEqual(row['request_id'], first['request']['request_id'])

    def test_resume_cannot_attach_another_accounts_person(self):
        first = self.call('mop-invite')
        with self.assertRaises(SignatureManagementError):
            self.call('mop-invite', {**self.payload, 'send_operation_id': 'forged',
                'expected_request_id': first['request']['request_id']}, actor='anotherOperator')
        self.assertEqual(self.service.creates, 1)
        self.assertEqual(self.sender.call_count, 1)

    def test_repeated_same_attempt_never_rewrites_saved_recipient(self):
        first = self.call('mop-invite')
        repeated = self.call('mop-invite', {**self.payload, 'recipient': 'duty:E'})
        self.assertEqual(repeated['request']['request_id'], first['request']['request_id'])
        self.assertEqual(self.call('mop-list')['items'][0]['recipient'], self.payload['recipient'])
        self.assertEqual(self.sender.call_count, 1)

    def staff_recipients(self, payload=None, *, actor='mopOperator'):
        return sorted(value for value in self.recipient_values(payload, actor=actor)
                      if value.startswith('staff:'))

    def test_recipients_include_every_eligible_staff_member_regardless_of_building(self):
        eligible = ['staff:recCross', 'staff:recOther', 'staff:recReceiver']
        for actor in ('mopOperator', 'otherBuildingOperator'):
            with self.subTest(actor=actor):
                self.assertEqual(self.staff_recipients(actor=actor), eligible)
        # Missing identity and non-message-capable staff stay out even though they are VNET rows.
        self.assertEqual(self.service.staff['recNoOpenId']['fixture_open_id'], '')
        self.assertEqual(self.service.staff['recExternal']['账号性质'], '外部账号')
        # Blocking is per person: one blocked colleague does not hide the others.
        for blocked, remaining in (('receiver', ['staff:recCross', 'staff:recOther']),
                                   ('cross', ['staff:recOther', 'staff:recReceiver']),
                                   ('other', ['staff:recCross', 'staff:recReceiver'])):
            self.blocked = {blocked}
            with self.subTest(blocked=blocked):
                self.assertEqual(self.staff_recipients(), remaining)
        self.blocked = set()
        # The signer's agent stays scoped to the sender's own building.
        self.assertEqual(self.call('mop-people')['scope_options'], [{'value': 'E', 'label': 'E楼'}])

    def test_cross_building_recipient_receives_invitation_but_subject_building_stays_scoped(self):
        result = self.call('mop-invite', {**self.payload, 'recipient': 'staff:recCross',
                                          'building': 'A楼', 'operation_id': 'crossBuilding'})
        self.assertEqual(result['request']['status'], 'sent')
        self.assertEqual(self.sender.call_args.args[1], ['cross'])
        self.assertEqual(result['person']['building'], 'E楼')
        self.assertEqual(self.sender.call_args.args[0].splitlines()[2], '楼栋：E楼')
        self.assertEqual((result['person']['scope'], result['person']['building']), ('E', 'E楼'))
        self.assertEqual(result['request']['building'], 'E楼')
        row = self.call('mop-list')['items'][0]
        self.assertEqual((row['recipient'], row['building'], row['scope']),
                         ('staff:recCross', 'E楼', 'E'))
        self.assertEqual(len(self.store.list_documents('mop_signature_invitation')), 1)
        # Inviting does not turn the recipient into a signer agent for this scope.
        self.assertEqual(self.staff_recipients(),
                         ['staff:recCross', 'staff:recOther', 'staff:recReceiver'])
        self.assertEqual(self.service.creates, 1)

    def test_ineligible_recipients_are_not_listed_or_invited_across_buildings(self):
        listed = self.staff_recipients()
        for record_id in ('recExternal', 'recNoOpenId'):
            with self.subTest(record_id=record_id):
                self.assertNotIn('staff:' + record_id, listed)
        self.blocked = {'cross'}
        self.assertNotIn('staff:recCross', self.staff_recipients())
        self.blocked = set()
        for recipient in ('staff:recExternal', 'staff:recNoOpenId', 'staff:missing'):
            with self.subTest(recipient=recipient), self.assertRaises(SignatureManagementError) as error:
                self.call('mop-invite', {**self.payload, 'recipient': recipient})
            self.assertEqual(error.exception.status, 403)
        # A blocked colleague is rejected as a recipient without hiding the others.
        self.blocked = {'receiver'}
        with self.assertRaises(SignatureManagementError) as error:
            self.call('mop-invite')
        self.assertEqual(error.exception.status, 403)
        self.assertIn('staff:recCross', self.staff_recipients())
        self.assertEqual(self.service.creates, 0)
        self.assertEqual(self.store.list_documents('signature_management_request'), [])
        self.sender.assert_not_called()

    def test_cross_building_recipient_does_not_bypass_notice_or_building_authorization(self):
        for changes in ({'scope': 'A'}, {'scope': 'C'}, {'notice_key': 'noticeA'}, {'notice_key': 'missing'},
                        {'recipient': 'duty:A'}):
            with self.subTest(changes=changes), self.assertRaises(SignatureManagementError) as error:
                self.call('mop-invite', {**self.payload, 'recipient': 'staff:recCross', **changes})
            self.assertEqual(error.exception.status, 403)
        for modules in ([], ['engineer_mop']):
            self.modules = modules
            self.blocked = {'mopOperator'}
            with self.subTest(modules=modules), self.assertRaises(SignatureManagementError) as error:
                self.call('mop-invite', {**self.payload, 'recipient': 'staff:recCross'})
            self.assertEqual(error.exception.status, 403)
        self.modules = ['engineer_mop']
        self.blocked = set()
        self.service.creates = 0
        self.sender.reset_mock()
        result = self.call('mop-invite', {**self.payload, 'recipient': 'staff:recCross',
                                          'operation_id': 'afterDenied'})
        self.assertEqual(result['request']['status'], 'sent')
        self.assertEqual((self.service.creates, self.sender.call_count), (1, 1))
        # The invitation stays bound to its account and notice even though the
        # recipient lives in another building.
        self.assertEqual(len(self.call('mop-list')['items']), 1)
        self.assertEqual(self.call('mop-list', actor='otherBuildingOperator')['items'], [])
        self.assertEqual(self.call('mop-list', {**self.payload, 'notice_key': 'noticeA'})['items'], [])
        with self.assertRaises(SignatureManagementError) as error:
            self.call('mop-people', {**self.payload, 'notice_key': 'other'})
        self.assertEqual(error.exception.status, 403)

    def test_cross_building_duplicate_retry_reuses_one_person_and_one_message(self):
        payload = {**self.payload, 'recipient': 'staff:recCross', 'operation_id': 'crossDuplicate'}
        first = self.call('mop-invite', payload)
        repeated = self.call('mop-invite', payload)
        self.assertEqual(repeated['request']['request_id'], first['request']['request_id'])
        self.assertEqual(repeated['person']['record_id'], first['person']['record_id'])
        rows = self.call('mop-list', payload)['items']
        self.assertEqual((len(rows), rows[0]['recipient'], rows[0]['building']),
                         (1, 'staff:recCross', 'E楼'))
        self.assertEqual((self.service.creates, self.sender.call_count), (1, 1))

    def test_cross_building_failed_retry_reuses_person_and_keeps_saved_recipient(self):
        payload = {**self.payload, 'recipient': 'staff:recCross', 'operation_id': 'crossFailed'}
        self.sender.return_value = (False, '发送失败', [])
        first = self.call('mop-invite', payload)
        self.assertEqual(first['request']['status'], 'send_failed')
        self.sender.return_value = (True, '', [])
        # The same retry is repeated (double click on "retry"): one new link, one message.
        retry = {**payload, 'send_operation_id': 'crossResend',
                 'expected_request_id': first['request']['request_id']}
        retried = self.call('mop-invite', retry)
        self.assertEqual(self.call('mop-invite', retry)['request']['request_id'],
                         retried['request']['request_id'])
        self.assertEqual(retried['request']['status'], 'sent')
        self.assertEqual(retried['person']['record_id'], first['person']['record_id'])
        self.assertEqual(self.sender.call_count, 2)
        self.assertEqual(self.sender.call_args.args[1], ['cross'])
        row = self.call('mop-list', payload)['items'][0]
        self.assertEqual((row['recipient'], row['request_id'], row['building']),
                         ('staff:recCross', retried['request']['request_id'], 'E楼'))
        self.assertEqual(self.service.creates, 1)


if __name__ == '__main__':
    unittest.main()
