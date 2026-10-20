"""Identity-migrated operations and app-scoped receipts, with no real sends.

Transformer contract: keep id/fingerprint/receipts; record previous_owner and
source_app_id in identity_migration; rekey recipient parts while retaining their
original uuid5 in message_uuid. previous_owner is not a login permission alias.
"""
import copy
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
import uuid
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).parent))
from bin.test_message_delivery import fake_service, PEOPLE
from lan_bitable_template_portal.message_delivery import MessageDelivery, DeliveryError, NS, digest
from lan_bitable_template_portal.feishu_identity_documents import transform_document
from lan_bitable_template_portal.state_store import LanPortalStateStore

PREVIOUS_APP = 'cli_fixturePrevious'
CURRENT_APP = 'cli_fixtureCurrent'
PREVIOUS_OWNER = 'ou_a'
CURRENT_OWNER = 'ou_currentOwner'


class IdentityDeliveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = LanPortalStateStore(Path(self.temp.name) / 'fixture.sqlite3')
        self.service = fake_service(self.store)
        self.transport = SimpleNamespace(upload=Mock(return_value='file-current-app'),
                                         send=Mock(return_value='om_FixtureReceipt'))
        self.credentials = self.enterContext(patch(
            'upload_event_module.services.feishu_token_manager.token_manager._credentials',
            return_value=(PREVIOUS_APP, 'fixture-previous-secret')))
        self.delivery = MessageDelivery(self.service, self.transport, clock=lambda: 1100.)
        self.payload = {'operation_id': 'fixture-operation-1', 'recipient_ids': ['__self__'], 'text': 'original complete message'}

    def legacy(self, *, files=(), parts=None, status='queued', text=None):
        payload = self.payload if text is None else {**self.payload, 'text': text}
        result = self.delivery.prepare(PREVIOUS_OWNER, payload, list(files))
        job = self.store.get_document(NS, result['delivery_id'])
        job.pop('operation_id')
        job.pop('origin_app_id')
        job['parts'] = copy.deepcopy(parts or {})
        job['status'] = status
        job['receipts'] = {'historical': {'message_id': 'om_KeepHistory'}}
        self.store.put_document(NS, job['id'], job)
        return job

    def migrate(self, job, *, origin=PREVIOUS_APP, current=CURRENT_APP, rekey=True):
        job = copy.deepcopy(job)
        job['owner'] = CURRENT_OWNER
        job['identity_migration'] = {'previous_owner': PREVIOUS_OWNER, 'source_app_id': origin,
                                     'target_app_id': current}
        for person in job['recipients']:
            previous = person['open_id']
            if previous == PREVIOUS_OWNER:
                person['open_id'] = CURRENT_OWNER
            if rekey:
                labels = (['text'] if job['text'].strip() else []) + ['file:' + file['sha256'] for file in job['files']]
                for label in labels:
                    old_key = digest([previous, label])
                    if old_key in job['parts']:
                        part = job['parts'].pop(old_key)
                        part.setdefault('message_uuid', str(uuid.uuid5(uuid.NAMESPACE_URL, job['id'] + old_key)))
                        job['parts'][digest([person['open_id'], label])] = part
        self.store.put_document(NS, job['id'], job)
        rows = copy.deepcopy(PEOPLE)
        rows[0]['open_id'] = CURRENT_OWNER
        self.service._load_signature_people.return_value = rows
        self.service.fields['rec-a']['openid'] = CURRENT_OWNER
        self.credentials.return_value = (current, 'fixture-current-secret')
        self.delivery._directory = None
        return job

    def stored(self, job):
        return self.store.get_document(NS, job['id'])

    def test_new_jobs_persist_the_operation_and_origin_without_changing_digest(self):
        public = self.delivery.prepare(PREVIOUS_OWNER, self.payload, [])
        job = self.store.get_document(NS, public['delivery_id'])
        self.assertEqual(job['id'], digest([PREVIOUS_OWNER, self.payload['operation_id']]))
        self.assertEqual(job['operation_id'], self.payload['operation_id'])
        self.assertEqual(job['origin_app_id'], PREVIOUS_APP)

    def test_old_operation_retry_returns_original_job_and_preserves_receipts(self):
        job = self.migrate(self.legacy(status='completed'))
        before = self.stored(job)
        with patch.object(self.delivery, '_selected', side_effect=AssertionError('must not create a second job')):
            result = self.delivery.prepare(CURRENT_OWNER, self.payload, [])
        self.assertEqual(result['delivery_id'], job['id'])
        self.assertNotEqual(result['delivery_id'], digest([CURRENT_OWNER, self.payload['operation_id']]))
        self.assertEqual(self.stored(job), before)
        self.assertEqual(len(self.store.list_documents(NS)), 1)
        self.transport.send.assert_not_called()

    def test_actual_transformer_completed_job_and_original_operation_remain_addressable(self):
        key = digest([PREVIOUS_OWNER, 'text'])
        previous = self.legacy(parts={key: {'status': 'sent', 'message_id': 'om_Historical'}}, status='completed')
        identity, migrated = transform_document(NS, previous['id'], previous, {PREVIOUS_OWNER: CURRENT_OWNER})
        self.store.put_document(NS, identity, migrated)
        result = self.delivery.prepare(CURRENT_OWNER, self.payload, [])
        self.assertEqual(result['delivery_id'], previous['id'])
        self.assertEqual(result['sent_count'], 1)
        self.delivery.run(CURRENT_OWNER, identity)
        self.assertEqual(self.stored(migrated), migrated)
        self.transport.send.assert_not_called()

    def test_actual_transformer_blocks_uncertain_job_even_when_app_matches(self):
        key = digest([PREVIOUS_OWNER, 'text'])
        previous = self.legacy(parts={key: {'status': 'sending', 'attempted_at': 1000.}}, status='failed')
        identity, migrated = transform_document(NS, previous['id'], previous, {PREVIOUS_OWNER: CURRENT_OWNER})
        self.assertTrue(migrated['identity_migration']['blocked'])
        self.store.put_document(NS, identity, migrated)
        self.delivery.run(CURRENT_OWNER, identity)
        self.assertEqual(self.stored(migrated)['status'], 'migration_blocked')
        self.transport.send.assert_not_called()
        self.transport.upload.assert_not_called()

    def test_actual_transformer_queued_job_reuploads_original_file_in_new_app(self):
        previous = self.legacy(files=[('report.txt', b'original')], text='')
        previous['files'][0]['file_key'] = 'file-previous-app'
        self.migrate(previous)
        identity, migrated = transform_document(NS, previous['id'], previous, {PREVIOUS_OWNER: CURRENT_OWNER})
        self.assertNotIn('file_key', migrated['files'][0])
        self.store.put_document(NS, identity, migrated)
        self.delivery.run(CURRENT_OWNER, identity)
        self.assertEqual(self.stored(migrated)['status'], 'completed')
        self.transport.upload.assert_called_once()
        self.assertEqual(Path(migrated['files'][0]['path']).read_bytes(), b'original')

    def test_old_owner_proof_does_not_grant_old_owner_or_other_users_access(self):
        job = self.migrate(self.legacy())
        for owner in (PREVIOUS_OWNER, 'ou_otherOwner'):
            with self.subTest(owner=owner), self.assertRaises(DeliveryError) as error:
                self.delivery.get(owner, job['id'])
            self.assertEqual(error.exception.status, 404)
        with self.assertRaises(DeliveryError):
            self.delivery.prepare(PREVIOUS_OWNER, self.payload, [])
        other = self.delivery.prepare('ou_otherOwner', {**self.payload, 'recipient_ids': ['rec-b']}, [])
        self.assertNotEqual(other['delivery_id'], job['id'])
        self.assertEqual(self.stored(job)['owner'], CURRENT_OWNER)

    def test_different_operation_with_identical_content_creates_distinct_job(self):
        job = self.migrate(self.legacy())
        result = self.delivery.prepare(CURRENT_OWNER, {**self.payload, 'operation_id': 'fixture-operation-2'}, [])
        self.assertNotEqual(result['delivery_id'], job['id'])
        self.assertEqual(len(self.store.list_documents(NS)), 2)

    def test_same_operation_cannot_change_text_or_file_content(self):
        job = self.migrate(self.legacy(files=[('report.txt', b'original file')]))
        for payload, files in (({**self.payload, 'text': 'changed text'}, [('report.txt', b'original file')]),
                               (self.payload, [('report.txt', b'changed file')])):
            with self.subTest(payload=payload), self.assertRaises(DeliveryError) as error:
                self.delivery.prepare(CURRENT_OWNER, payload, files)
            self.assertEqual(error.exception.status, 409)
        self.assertEqual(self.stored(job)['receipts'], {'historical': {'message_id': 'om_KeepHistory'}})
        self.assertEqual(len(self.store.list_documents(NS)), 1)

    def test_only_verified_operation_hash_can_match_a_migrated_job(self):
        job = self.migrate(self.legacy())
        job['identity_migration']['previous_owner'] = 'ou_somebodyElse'
        self.store.put_document(NS, job['id'], job)
        result = self.delivery.prepare(CURRENT_OWNER, self.payload, [])
        self.assertNotEqual(result['delivery_id'], job['id'])

    def test_conflicting_recorded_operation_is_rejected_not_recreated(self):
        job = self.migrate(self.legacy())
        job['operation_id'] = 'fixture-another-operation'
        self.store.put_document(NS, job['id'], job)
        with self.assertRaises(DeliveryError) as error:
            self.delivery.prepare(CURRENT_OWNER, self.payload, [])
        self.assertEqual(error.exception.status, 409)
        self.assertEqual(len(self.store.list_documents(NS)), 1)

    def test_migrated_sent_part_and_original_message_id_are_never_resent(self):
        key = digest([PREVIOUS_OWNER, 'text'])
        job = self.migrate(self.legacy(parts={key: {'status': 'sent', 'message_id': 'om_Original', 'attempted_at': 1000.}}, status='failed'))
        part = copy.deepcopy(job['parts'][digest([CURRENT_OWNER, 'text'])])
        self.delivery.run(CURRENT_OWNER, job['id'])
        self.assertEqual(self.stored(job)['parts'][digest([CURRENT_OWNER, 'text'])], part)
        self.assertEqual(self.delivery.get(CURRENT_OWNER, job['id'])['status'], 'completed')
        self.transport.send.assert_not_called()
        self.transport.upload.assert_not_called()
        self.service.signature_management._fields.assert_not_called()

    def test_old_app_sending_or_uncertain_is_blocked_before_reads_uploads_or_sends(self):
        original = self.legacy(status='failed')
        for status in ('sending', 'uncertain'):
            with self.subTest(status=status):
                key = digest([PREVIOUS_OWNER, 'text'])
                original['parts'] = {key: {'status': status, 'attempted_at': 1000.}}
                job = self.migrate(original)
                parts = copy.deepcopy(job['parts'])
                self.delivery.run(CURRENT_OWNER, job['id'])
                self.delivery.run(CURRENT_OWNER, job['id'])
                self.assertEqual(self.stored(job)['status'], 'migration_blocked')
                self.assertEqual(self.stored(job)['parts'], parts)
                self.assertEqual(self.delivery.prepare(CURRENT_OWNER, self.payload, [])['delivery_id'], job['id'])
        self.transport.send.assert_not_called()
        self.transport.upload.assert_not_called()
        self.service.signature_management._fields.assert_not_called()

    def test_unknown_origin_cannot_treat_target_app_as_proof_of_original_send(self):
        key = digest([PREVIOUS_OWNER, 'text'])
        job = self.migrate(self.legacy(parts={key: {'status': 'sending', 'attempted_at': 1000.}}, status='failed'))
        job.pop('origin_app_id', None)
        job['identity_migration'].pop('source_app_id')
        self.store.put_document(NS, job['id'], job)
        self.delivery.run(CURRENT_OWNER, job['id'])
        self.assertEqual(self.stored(job)['status'], 'migration_blocked')
        self.transport.send.assert_not_called()

    def test_unmapped_historical_part_key_fails_closed_instead_of_resending(self):
        key = digest([PREVIOUS_OWNER, 'text'])
        job = self.migrate(self.legacy(parts={key: {'status': 'sent', 'message_id': 'om_Original'}}), rekey=False)
        self.delivery.run(CURRENT_OWNER, job['id'])
        self.assertEqual(self.stored(job)['status'], 'migration_blocked')
        self.transport.send.assert_not_called()

    def test_legacy_sending_with_no_origin_is_blocked_even_without_migration_marker(self):
        key = digest([PREVIOUS_OWNER, 'text'])
        job = self.legacy(parts={key: {'status': 'sending', 'attempted_at': 1000.}}, status='failed')
        self.delivery.run(PREVIOUS_OWNER, job['id'])
        self.assertEqual(self.stored(job)['status'], 'migration_blocked')
        self.transport.send.assert_not_called()

    def test_known_same_app_retry_preserves_original_part_uuid_after_rekey(self):
        key = digest([PREVIOUS_OWNER, 'text'])
        job = self.migrate(self.legacy(parts={key: {'status': 'sending', 'attempted_at': 1000.}}), current=PREVIOUS_APP)
        original_uuid = str(uuid.uuid5(uuid.NAMESPACE_URL, job['id'] + key))
        self.delivery.run(CURRENT_OWNER, job['id'])
        self.assertEqual(self.stored(job)['status'], 'completed')
        self.transport.send.assert_called_once_with(CURRENT_OWNER, 'text', {'text': self.payload['text']}, original_uuid)
        self.assertNotEqual(original_uuid, str(uuid.uuid5(uuid.NAMESPACE_URL, job['id'] + digest([CURRENT_OWNER, 'text']))))

    def test_completed_history_is_readable_even_when_current_credentials_are_unavailable(self):
        job = self.migrate(self.legacy(status='completed'))
        before = self.stored(job)
        self.credentials.side_effect = AssertionError('completed history must not authenticate again')
        self.assertEqual(self.delivery.get(CURRENT_OWNER, job['id'])['status'], 'completed')
        self.assertEqual(self.delivery.prepare(CURRENT_OWNER, self.payload, [])['delivery_id'], job['id'])
        self.delivery.run(CURRENT_OWNER, job['id'])
        self.assertEqual(self.stored(job), before)
        self.transport.send.assert_not_called()

    def test_queued_old_app_file_key_is_reuploaded_without_altering_original_content(self):
        content = b'original complete file bytes'
        job = self.legacy(files=[('report.txt', content)], text='')
        job['files'][0]['file_key'] = 'file-previous-app'
        job = self.migrate(job)
        original = copy.deepcopy(job['files'][0])
        self.delivery.run(CURRENT_OWNER, job['id'])
        stored = self.stored(job)
        self.assertEqual(stored['status'], 'completed')
        self.assertEqual(stored['files'][0]['file_key'], 'file-current-app')
        self.assertEqual(stored['files'][0]['file_key_app_id'], CURRENT_APP)
        for key in ('name', 'path', 'size', 'sha256'):
            self.assertEqual(stored['files'][0][key], original[key])
        self.assertEqual(Path(original['path']).read_bytes(), content)
        self.transport.upload.assert_called_once()
        self.transport.send.assert_called_once()
        self.assertEqual(self.transport.send.call_args.args[2], {'file_key': 'file-current-app'})
        self.delivery.run(CURRENT_OWNER, job['id'])
        self.assertEqual(self.transport.send.call_count, 1)
        self.assertEqual(self.transport.upload.call_count, 1)

    def test_partial_migration_keeps_sent_text_receipt_and_only_uploads_unsent_file(self):
        key = digest([PREVIOUS_OWNER, 'text'])
        job = self.legacy(files=[('report.txt', b'original')],
                          parts={key: {'status': 'sent', 'message_id': 'om_Original'}}, status='failed')
        job['files'][0]['file_key'] = 'file-previous-app'
        job = self.migrate(job)
        self.delivery.run(CURRENT_OWNER, job['id'])
        self.assertEqual(self.stored(job)['parts'][digest([CURRENT_OWNER, 'text'])]['message_id'], 'om_Original')
        self.transport.send.assert_called_once()
        self.assertEqual(self.transport.send.call_args.args[1], 'file')

    def test_changed_local_attachment_cannot_replace_original_migrated_message(self):
        job = self.legacy(files=[('report.txt', b'original')])
        job['files'][0]['file_key'] = 'file-previous-app'
        job = self.migrate(job)
        Path(job['files'][0]['path']).write_bytes(b'tampered')
        self.delivery.run(CURRENT_OWNER, job['id'])
        self.assertEqual(self.stored(job)['status'], 'failed')
        self.assertIn('内容已变化', self.stored(job)['error'])
        self.transport.upload.assert_not_called()
        self.transport.send.assert_not_called()


if __name__ == '__main__':
    unittest.main()
