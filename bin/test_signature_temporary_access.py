"""Shared temporary signatures do not grant access to protected staff signatures."""
import copy
import unittest

from bin import test_signature_access as fixtures
from lan_bitable_template_portal.signature_access import SignatureAccess
from lan_bitable_template_portal.signature_management import SignatureManagementError


class TemporarySignatureAccessTests(unittest.TestCase):
    setUp = fixtures.SignatureAccessTests.setUp
    call = fixtures.SignatureAccessTests.call

    def add_visitor(self):
        self.service.add('recVisitor', source='external', name='共享临时人员', building='A楼')
        return {'source': 'external', 'record_id': 'recVisitor'}

    def test_all_signature_users_can_create_in_their_authorized_buildings(self):
        listing = self.call('people')
        self.assertTrue(listing['can_create_temporary'])
        self.assertFalse(listing['can_manage_directory'])
        self.assertEqual(listing['create_scope_options'], [{'value': 'E', 'label': 'E楼'}])
        payload = {'name': '新建临时人员', 'building': 'E楼', 'operation_id': 'newVisitor'}
        result = self.call('temporary', payload, method='POST')
        self.assertEqual(result['source'], 'external')
        self.assertEqual(result['building'], 'E楼')
        self.assertEqual(self.call('temporary', payload, method='POST')['record_id'], result['record_id'])
        self.assertEqual(self.service.creates, 1)
        for actor in ('operatorE', 'operatorA', 'supervisorA', 'manager'):
            with self.subTest(actor=actor):
                people = self.call('people', {'source': 'external'}, actor=actor)['people']
                person = next(p for p in people if p['record_id'] == result['record_id'])
                self.assertTrue(person['can_request'])
                self.assertFalse(person['can_preview'])
        self.sender.assert_not_called()

    def test_unauthorized_building_and_revoked_permissions_cannot_create(self):
        payload = {'name': '临时人员', 'building': 'A楼', 'operation_id': 'outside'}
        with self.assertRaises(SignatureManagementError):
            self.call('temporary', payload, method='POST')
        self.profiles['operatorE']['scope_rules'] = []
        self.assertFalse(self.call('people')['can_create_temporary'])
        with self.assertRaises(SignatureManagementError):
            self.call('temporary', {**payload, 'building': 'E楼'}, method='POST')
        self.profiles['operatorE']['modules'] = []
        with self.assertRaises(SignatureManagementError):
            self.call('temporary', payload, method='POST')
        self.assertEqual(self.service.creates, 0)

    def test_existing_other_building_temporary_signature_is_shared_without_account_privileges(self):
        payload = self.add_visitor()
        rows = self.call('people', {'source': 'external'})['people']
        visitor = next(row for row in rows if row['record_id'] == 'recVisitor')
        self.assertTrue(visitor['can_preview'])
        self.assertTrue(visitor['can_request'])
        self.assertFalse(visitor['invite_only'])
        image, mime = self.call('preview', payload)
        self.assertTrue(image)
        self.assertEqual(mime, 'image/png')
        for op, method in (('duplicates', 'GET'), ('associate', 'POST'), ('merge', 'POST'), ('migrate', 'POST')):
            with self.subTest(operation=op), self.assertRaises(SignatureManagementError) as error:
                self.call(op, payload, method=method)
            self.assertEqual(error.exception.status, 403)
        self.sender.assert_not_called()

    def test_recipient_options_include_other_buildings_without_signature_or_private_fields(self):
        payload = self.add_visitor()
        for i in range(510):
            self.service.add('relay' + str(i), open_id='ou-relay' + str(i), building='A楼')
        self.blocked.add('operatorA')
        self.service.add('missingId', open_id='')
        self.service.add('notMessageCapable', account_nature='外部账号')
        rows = self.call('recipients', payload)['people']
        ids = {row['record_id'] for row in rows}
        self.assertIn('relay509', ids)
        self.assertIn('recpeerManager', ids)
        self.assertNotIn('recoperatorA', ids)
        self.assertNotIn('missingId', ids)
        self.assertNotIn('notMessageCapable', ids)
        for row in rows:
            self.assertEqual(set(row), {'record_id', 'name', 'employee_no', 'building', 'position', 'team', 'specialty'})
        with self.assertRaises(SignatureManagementError):
            self.call('preview', {'source': 'staff', 'record_id': 'recpeerManager'})

    def test_cross_building_relay_sends_once_without_overwriting_signature(self):
        payload = {**self.add_visitor(), 'recipient': 'staff:recoperatorA', 'operation_id': 'crossRelay'}
        before = copy.deepcopy(self.service.external)
        first = self.call('requests', payload, method='POST')
        self.assertEqual(first['status'], 'sent')
        self.assertEqual(first['building'], 'A楼')
        self.assertEqual(self.call('requests', payload, method='POST'), first)
        self.assertEqual(self.sender.call_count, 1)
        self.assertEqual(self.sender.call_args.args[1], ['operatorA'])
        self.assertEqual(self.service.external, before)
        self.assertEqual(self.service.uploads, 0)

    def test_invalid_or_blocked_recipients_do_not_send(self):
        payload = {**self.add_visitor(), 'operation_id': 'rejected'}
        self.blocked.add('operatorA')
        for recipient in ('staff:recoperatorA', 'staff:not-found', 'duty:A', 'forged'):
            with self.subTest(recipient=recipient), self.assertRaises(SignatureManagementError):
                self.call('requests', {**payload, 'recipient': recipient}, method='POST')
        self.sender.assert_not_called()
        self.assertEqual(self.store.list_documents('signature_management_request'), [])

    def test_linked_and_merged_signatures_do_not_become_shared(self):
        self.service.add('recLinked', source='external', origin='recoperatorA', aliases=['recAlias'])
        self.service.add('recAlias', source='external')
        rows = self.call('people')['people']
        self.assertFalse({'recLinked', 'recAlias'} & {p['record_id'] for p in rows})
        for rid in ('recLinked', 'recAlias'):
            for op in ('preview', 'recipients'):
                with self.subTest(record_id=rid, operation=op), self.assertRaises(SignatureManagementError):
                    self.call(op, {'source': 'external', 'record_id': rid})
        self.sender.assert_not_called()

    def test_upward_invitation_cannot_enumerate_relays(self):
        with self.assertRaises(SignatureManagementError) as error:
            self.call('recipients', {'source': 'staff', 'record_id': 'recsupervisorE'})
        self.assertEqual(error.exception.status, 403)

    def test_recipient_endpoint_requires_login_and_keeps_formal_scope_restrictions(self):
        payload = self.add_visitor()
        with self.assertRaises(SignatureManagementError) as error:
            self.call('recipients', payload, actor='')
        self.assertEqual(error.exception.status, 401)
        rows = self.call('recipients', {'source': 'staff', 'record_id': 'recoperatorE'}, actor='supervisorE')['people']
        self.assertIn('recoperatorE', {row['record_id'] for row in rows})
        self.assertNotIn('recoperatorA', {row['record_id'] for row in rows})
        self.assertNotIn('recpeerManager', {row['record_id'] for row in rows})

    def test_directory_failure_and_account_revocation_fail_closed(self):
        payload = self.add_visitor()
        self.service.staff_error = True
        with self.assertRaises(SignatureManagementError) as error:
            self.call('recipients', payload)
        self.assertEqual(error.exception.status, 503)
        self.service.staff_error = False
        self.blocked.add('operatorE')
        with self.assertRaises(SignatureManagementError):
            self.call('people')
        self.sender.assert_not_called()

    def test_send_rechecks_recipient_after_selection(self):
        payload = self.add_visitor()
        access = SignatureAccess(self.manager, self.auth, 'operatorE')
        person = access.require('external', 'recVisitor', 'request')
        access.recipient('staff:recoperatorA', person=person)
        self.blocked.add('operatorA')
        with self.assertRaises(SignatureManagementError):
            self.manager.send({**payload, 'recipient': 'staff:recoperatorA', 'operation_id': 'revokedRelay'},
                              actor='operatorE', base_url='http://localhost', send_text=self.sender, access=access)
        self.sender.assert_not_called()


if __name__ == '__main__':
    unittest.main()
