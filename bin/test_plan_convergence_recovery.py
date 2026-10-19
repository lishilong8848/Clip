"""Offline recovery tests using temporary SQLite databases and fake Feishu receipts."""
import asyncio
import copy
import hashlib
import sys
import tempfile
import threading
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lan_bitable_template_portal import plan_convergence_notifications as notifications
from lan_bitable_template_portal import plan_convergence_auth as auth
from lan_bitable_template_portal.plan_convergence import PlanConvergenceService
from lan_bitable_template_portal.plan_convergence_maintenance import business_fingerprint
from lan_bitable_template_portal.portal_service import BUILDING_OPEN_ID_MAP
from lan_bitable_template_portal.state_store import LanPortalStateStore


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.path = Path(folder.name) / 'state.sqlite3'
        self.store = LanPortalStateStore(self.path)
        self.addCleanup(self.store.shutdown_write_worker)
        self.addCleanup(auth.bind_store, auth._store)
        self.clock = patch.object(notifications.time, 'time', return_value=1800000000.0).start()
        self.addCleanup(patch.stopall)
        self.service = SimpleNamespace(
            _building_codes_from_value=lambda value: [code for code in ('110', 'A', 'B', 'C', 'D', 'E', 'H') if code in (value or '')],
            _recipients_for_building_codes=Mock(side_effect=lambda codes, **_: ('', [code + '-duty' for code in codes], '')))
        self.check = Mock(side_effect=lambda record: {'records': [{**record, 'hits': []}]})
        self.send = Mock(side_effect=self.accept)
        self.worker = notifications.NoticePlanChecks(self.service, self.store, self.check, self.send)

    @staticmethod
    def accept(text, recipients, *, message_uuid):
        return True, 'ok', [{'open_id': recipients[0], 'ok': True, 'message_id': 'om_fixture',
            'chat_id': 'oc_fixture', 'sender_app_id': 'cli_fixture', 'message_uuid': message_uuid,
            'accepted_at': 1800000000.0}]

    def advance(self, seconds=notifications.RETRY_SECONDS + 1):
        self.clock.return_value += seconds

    def enqueue(self, ident='one', *, kind='repair', code='B', target='recB', action='start'):
        notice = {'notice_type': '变更通告' if kind == 'change' else '设备检修',
            'work_type': kind, 'action': action, 'title': code + '楼设备作业',
            'building': code + '楼', 'building_codes': [code], 'location': code + '-124',
            'repair_device': code + '-124-BAS-101', 'repair_fault': '指示灯故障', 'content': '控制器变更'}
        self.worker.enqueue(notice, operation_id=ident, target_record_id=target)
        return self.store.get_document(notifications.CHANNEL,
            ('latest:change:' if kind == 'change' else 'latest:') + target)['job_id']

    def rows(self, status='pending'):
        return self.store.list_outbox_events(notifications.CHANNEL, status=status, limit=500)

    def drain(self):
        while rows := self.store.lease_outbox_events(notifications.CHANNEL, limit=20,
                lease_seconds=notifications.RETRY_SECONDS):
            for row in rows:
                self.worker.process(row)

    def job(self, ident):
        return self.store.get_document(notifications.CHANNEL, ident)

    def h_messages(self):
        return [call for call in self.send.call_args_list if call.args[1] == [BUILDING_OPEN_ID_MAP['H']]]

    def restart(self):
        self.store.shutdown_write_worker()
        self.store = LanPortalStateStore(self.path)
        self.addCleanup(self.store.shutdown_write_worker)
        self.worker = notifications.NoticePlanChecks(self.service, self.store, self.check, self.send)
        self.worker.recover()

    def test_timeout_waits_persistently_without_unmatched_result_then_recovers(self):
        ident = self.enqueue()
        self.check.side_effect = TimeoutError('private credential must not appear')
        self.drain()
        self.assertEqual(self.job(ident)['status'], 'pending')
        self.assertNotIn('result', self.job(ident))
        self.assertNotIn('checked_at', self.job(ident))
        self.assertEqual(len(self.h_messages()), 1)
        text = self.h_messages()[0].args[0]
        self.assertIn('VPN', text)
        self.assertIn('自动重新核对', text)
        self.assertNotIn('未找到匹配', text)
        self.assertNotIn('private credential', text)
        for _ in range(8):
            self.advance()
            self.drain()
        self.assertEqual(len(self.h_messages()), 1)
        self.assertEqual(len(self.rows('failed')), 0)
        self.assertTrue(self.rows('leased'))
        self.assertTrue(all(row['attempts'] == 0 for row in self.rows('leased')))
        self.restart()
        self.check.side_effect = lambda record: {'records': [{**record, 'hits': []}]}
        self.advance()
        self.drain()
        self.assertEqual(self.job(ident)['status'], 'ready')
        self.assertFalse(self.store.get_document(notifications.CHANNEL, notifications.OUTAGE_KEY)['active'])
        self.assertTrue(self.job(ident)['receipts']['B-duty']['sent'])
        self.assertEqual(self.send.call_count, 2)
        self.assertIn('未找到匹配', self.send.call_args.args[0])

    def test_shared_outage_backoff_does_not_probe_per_job_or_starve_queue(self):
        self.enqueue(target='recB')
        self.enqueue('two', target='recC', code='C', kind='change')
        self.check.side_effect = ConnectionError('VPN offline')
        self.drain()
        self.assertEqual(self.check.call_count, 1)
        self.assertEqual(len(self.h_messages()), 1)
        self.assertEqual(len(self.rows('leased')), 2)
        for _ in range(4):
            self.advance()
            self.drain()
        self.assertEqual(self.check.call_count, 5)
        self.assertEqual(len(self.h_messages()), 1)
        self.check.side_effect = lambda record: {'records': [{**record, 'hits': []}]}
        self.advance()
        self.drain()
        self.assertEqual(len(self.rows('leased')), 0)
        self.assertEqual(self.send.call_count, 3)

    def test_recovered_then_new_outage_creates_new_alert_uuid(self):
        self.check.side_effect = TimeoutError()
        self.enqueue()
        self.drain()
        self.advance()
        self.check.side_effect = lambda record: {'records': [{**record, 'hits': []}]}
        self.drain()
        self.restart()
        self.check.side_effect = ConnectionError()
        self.enqueue('second', target='recC', code='C')
        self.drain()
        alerts = self.h_messages()
        self.assertEqual(len(alerts), 2)
        self.assertNotEqual(alerts[0].kwargs['message_uuid'], alerts[1].kwargs['message_uuid'])

    def test_malformed_or_explicit_failed_result_is_never_empty_success(self):
        bad_results = (None, {}, {'records': []}, {'records': [{}]}, {'records': [{'hits': []}], 'error': 'offline'},
            {'records': [{'hits': [], 'check_status': 'failed'}]}, {'records': [{'hits': [], 'check_error': 'timeout'}]},
            {'records': [{'hits': [], 'record_id': 'another'}]}, {'records': [{'hits': [{}]}]})
        for index, result in enumerate(bad_results):
            with self.subTest(result=result):
                self.advance()
                ident = self.enqueue(str(index), target='rec' + str(index))
                self.check.side_effect = None
                self.check.return_value = result
                row = next(row for row in self.rows() if row['payload'].get('job_id') == ident)
                self.worker.process(row)
                self.assertEqual(self.job(ident)['status'], 'pending')
                self.assertNotIn('result', self.job(ident))
        self.assertEqual(len(self.h_messages()), 0)

    def test_one_incomplete_record_does_not_block_other_records(self):
        bad = self.enqueue(target='bad')
        good = self.enqueue('next', target='good', kind='change', code='C')
        self.check.side_effect = lambda record: {'records': [{}]} if record['record_id'] == 'bad' else {
            'records': [{**record, 'hits': []}]}
        self.drain()
        self.assertEqual(self.job(bad)['status'], 'pending')
        self.assertEqual(self.job(good)['status'], 'ready')
        self.assertEqual(self.check.call_count, 2)
        self.assertEqual(self.send.call_count, 1)
        self.assertEqual(len(self.h_messages()), 0)

    def test_change_and_repair_with_same_ids_are_isolated_and_change_is_visible(self):
        repair_id = self.enqueue('same', target='same-record')
        change_id = self.enqueue('same', kind='change', target='same-record')
        self.assertNotEqual(repair_id, change_id)
        self.drain()
        self.assertEqual(self.check.call_count, 2)
        self.assertEqual({call.args[0]['work_type'] for call in self.check.call_args_list}, {'repair', 'change'})
        self.assertEqual(self.job(repair_id)['record']['device'], 'B-124-BAS-101')
        self.assertEqual(self.job(change_id)['record']['fault'], '控制器变更')
        cached = self.store.get_document('plan_convergence_checks', 'change:same-record')
        self.assertEqual(cached['check_source'], 'auto')
        self.assertEqual(cached['fingerprint'], business_fingerprint(self.job(change_id)['record']))
        convergence = PlanConvergenceService(self.store, lambda _: [])
        self.addCleanup(convergence.close)
        record = copy.deepcopy(self.job(change_id)['record'])
        convergence._overlay_check_results(record, 'change')
        self.assertEqual(record['check_status'], 'ready')
        self.assertEqual(record['check_source'], 'auto')
        self.assertEqual(record['hits'], [])
        self.assertTrue(any('变更通告自动核对' in call.args[0] for call in self.send.call_args_list))

    def test_newer_manual_result_is_not_overwritten(self):
        ident = self.enqueue(kind='change')
        newer = {'check_source': 'manual', 'checked_at': self.clock() + 1, 'hits': [{'blockId': 'newer'}]}
        self.store.put_document('plan_convergence_checks', 'change:recB', newer)
        self.drain()
        self.assertEqual(self.store.get_document('plan_convergence_checks', 'change:recB'), newer)
        self.assertEqual(self.job(ident)['status'], 'ready')

    def test_de_exclusion_and_non_start_actions_never_check_or_notify(self):
        for kind in ('change', 'repair'):
            for code in ('D', 'E'):
                self.worker.enqueue({'work_type': kind, 'action': 'start', 'title': code + '楼作业',
                    'building_codes': [code]}, operation_id=code + kind, target_record_id='rec' + code)
        for action in ('end', 'undo', 'delete'):
            self.worker.enqueue({'work_type': 'change', 'action': action, 'building_codes': ['B']},
                operation_id=action, target_record_id='recB')
        self.assertEqual(self.rows(), [])
        self.service._recipients_for_building_codes.assert_not_called()
        self.check.assert_not_called()
        self.send.assert_not_called()

    def test_duplicate_enqueue_keeps_legacy_repair_identity_and_delivery_uuid(self):
        ident = self.enqueue()
        self.enqueue()
        self.assertEqual(ident, hashlib.sha256(b'recB:one').hexdigest())
        self.assertEqual(len(self.rows()), 1)
        self.drain()
        self.assertEqual(self.send.call_count, 1)
        self.assertEqual(self.send.call_args.kwargs['message_uuid'],
            str(uuid.uuid5(uuid.NAMESPACE_URL, notifications.CHANNEL + ident + 'B-duty')))

    def test_recovered_legacy_de_task_remains_excluded(self):
        ident = self.enqueue()
        old = self.job(ident)
        old.pop('version')
        old.update(status='failed')
        old['record'].update(building='D楼', name='D楼设备作业', location='D-124', rooms=['D-124'])
        self.store.put_document(notifications.CHANNEL, ident, old)
        self.store.mark_outbox_event(self.rows()[0]['id'], 'done')
        self.restart()
        self.drain()
        self.assertEqual(self.job(ident)['status'], 'skipped')
        self.check.assert_not_called()
        self.send.assert_not_called()

    def test_pending_superseded_job_cannot_publish_stale_check(self):
        first = self.enqueue('older')
        second = self.enqueue('newer', action='update')
        self.drain()
        self.assertEqual(self.job(first)['status'], 'superseded')
        self.assertEqual(self.job(second)['status'], 'ready')
        self.assertEqual(self.check.call_count, 1)
        self.assertEqual(self.send.call_count, 1)

    def test_rejection_retries_more_than_three_times_and_keeps_receipt(self):
        ident = self.enqueue()
        self.send.side_effect = lambda text, recipients, **_: (False, 'not available', [
            {'open_id': recipients[0], 'ok': False, 'failure_kind': 'bot_unavailable'}])
        for _ in range(15):
            self.drain()
            self.advance()
        self.assertEqual(self.check.call_count, 1)
        self.assertEqual(self.send.call_count, 15)
        self.assertEqual(self.rows('failed'), [])
        uuids = {call.kwargs['message_uuid'] for call in self.send.call_args_list}
        self.assertEqual(len(uuids), 1)
        self.restart()
        self.send.side_effect = self.accept
        self.drain()
        receipt = self.job(ident)['receipts']['B-duty']
        self.assertTrue(receipt['sent'])
        self.assertEqual(receipt['message_id'], 'om_fixture')
        self.assertEqual(receipt['chat_id'], 'oc_fixture')
        self.assertEqual(receipt['sender_app_id'], 'cli_fixture')
        self.assertNotIn('failure_kind', receipt)
        self.assertEqual(self.check.call_count, 1)

    def test_lost_send_response_reuses_same_uuid_across_restart(self):
        ident = self.enqueue()
        self.send.side_effect = TimeoutError('response lost')
        self.drain()
        original_uuid = self.send.call_args.kwargs['message_uuid']
        self.restart()
        self.send.side_effect = self.accept
        self.drain()
        self.assertEqual(self.send.call_args.kwargs['message_uuid'], original_uuid)
        self.assertEqual(self.check.call_count, 1)
        self.assertTrue(self.job(ident)['receipts']['B-duty']['sent'])
        self.assertNotIn('error', self.job(ident)['receipts']['B-duty'])

    def test_definite_block_does_not_expire_but_cannot_clear_prior_unknown_send(self):
        blocked = lambda *_args, **_kwargs: (False, 'blocked', [
            {'open_id': 'B-duty', 'ok': False, 'failure_kind': 'blocked'}])
        document = {'id': 'blocked-alert', 'recipients': ['B-duty']}
        self.send.side_effect = blocked
        self.assertFalse(self.worker._deliver(document, 'fixture'))
        self.assertNotIn('attempted_at', document['receipts']['B-duty'])
        self.advance(notifications.DELIVERY_WINDOW + 1)
        self.send.side_effect = self.accept
        self.assertTrue(self.worker._deliver(document, 'fixture'))

        unknown = {'id': 'unknown-alert', 'recipients': ['B-duty']}
        self.send.side_effect = TimeoutError()
        self.assertFalse(self.worker._deliver(unknown, 'fixture'))
        attempted_at = unknown['receipts']['B-duty']['attempted_at']
        self.send.side_effect = blocked
        self.assertFalse(self.worker._deliver(unknown, 'fixture'))
        self.assertEqual(unknown['receipts']['B-duty']['attempted_at'], attempted_at)
        self.assertTrue(unknown['receipts']['B-duty']['uncertain'])

    def test_expired_uncertain_send_is_retained_without_risking_duplicate(self):
        ident = self.enqueue()
        self.send.side_effect = TimeoutError()
        self.drain()
        self.advance(notifications.DELIVERY_WINDOW + 1)
        self.restart()
        self.send.side_effect = self.accept
        self.drain()
        self.assertEqual(self.send.call_count, 1)
        receipt = self.job(ident)['receipts']['B-duty']
        self.assertFalse(receipt['sent'])
        self.assertTrue(receipt['delivery_uncertain'])
        self.assertTrue(self.rows('leased'))
        self.assertEqual(self.rows('failed'), [])

    def test_later_rejection_cannot_erase_an_earlier_unknown_delivery(self):
        ident = self.enqueue()
        self.send.side_effect = TimeoutError()
        self.drain()
        self.advance()
        self.send.side_effect = lambda text, recipients, **_: (False, 'rejected', [
            {'open_id': recipients[0], 'ok': False, 'code': 230013}])
        self.drain()
        receipt = self.job(ident)['receipts']['B-duty']
        self.assertTrue(receipt['uncertain'])
        self.assertIn('attempted_at', receipt)
        self.advance(notifications.DELIVERY_WINDOW + 1)
        self.drain()
        self.assertEqual(self.send.call_count, 2)
        self.assertTrue(self.job(ident)['receipts']['B-duty']['delivery_uncertain'])

    def test_crash_after_check_persisted_does_not_recheck_on_restart(self):
        ident = self.enqueue()
        with patch.object(self.worker, '_deliver', side_effect=RuntimeError('crash')):
            with self.assertRaises(RuntimeError):
                self.drain()
        self.assertEqual(self.job(ident)['status'], 'ready')
        self.restart()
        self.drain()
        self.assertEqual(self.check.call_count, 1)
        self.assertEqual(self.send.call_count, 1)

    def test_crash_after_remote_acceptance_before_receipt_save_reuses_uuid(self):
        ident = self.enqueue()
        original = self.store.put_document
        def fail_after_send(namespace, key, value):
            if key == ident and value.get('receipts', {}).get('B-duty', {}).get('sent'):
                raise RuntimeError('disk response lost')
            original(namespace, key, value)
        with patch.object(self.store, 'put_document', side_effect=fail_after_send):
            with self.assertRaises(RuntimeError):
                self.drain()
        original_uuid = self.send.call_args.kwargs['message_uuid']
        self.restart()
        self.drain()
        self.assertEqual(self.send.call_count, 2)
        self.assertEqual(self.send.call_args.kwargs['message_uuid'], original_uuid)
        self.assertTrue(self.job(ident)['receipts']['B-duty']['sent'])
        self.assertEqual(self.check.call_count, 1)

    def test_receipt_persisted_but_outbox_ack_lost_does_not_send_again(self):
        ident = self.enqueue()
        with patch.object(self.store, 'mark_outbox_event', side_effect=RuntimeError('disk unavailable')):
            with self.assertRaises(RuntimeError):
                self.drain()
        self.assertTrue(self.job(ident)['receipts']['B-duty']['sent'])
        self.restart()
        self.drain()
        self.assertEqual(self.send.call_count, 1)
        self.assertEqual(self.check.call_count, 1)
        self.assertEqual(len(self.rows('done')), 1)

    def test_h_alert_retries_durably_without_losing_successful_check(self):
        self.enqueue()
        self.check.side_effect = TimeoutError()
        self.send.side_effect = ConnectionError()
        self.drain()
        alert_uuid = self.send.call_args.kwargs['message_uuid']
        self.restart()
        self.check.side_effect = lambda record: {'records': [{**record, 'hits': []}]}
        self.send.side_effect = self.accept
        self.advance()
        self.drain()
        alerts = self.h_messages()
        self.assertEqual(len(alerts), 2)
        self.assertEqual(alerts[-1].kwargs['message_uuid'], alert_uuid)
        self.assertEqual(self.send.call_count, 3)
        self.restart()
        self.drain()
        self.assertEqual(self.send.call_count, 3)

    def test_missing_recipients_do_not_drop_check_and_can_recover(self):
        self.service._recipients_for_building_codes.side_effect = RuntimeError('directory offline')
        ident = self.enqueue()
        self.drain()
        self.assertEqual(self.job(ident)['status'], 'ready')
        self.send.assert_not_called()
        self.assertTrue(self.rows('leased'))
        self.service._recipients_for_building_codes.side_effect = lambda codes, **_: ('', ['B-duty'], '')
        self.advance()
        self.drain()
        self.assertTrue(self.job(ident)['receipts']['B-duty']['sent'])
        self.assertEqual(self.check.call_count, 1)

    def test_old_failed_and_exhausted_jobs_migrate_once_preserving_prior_receipts(self):
        ident = self.enqueue()
        old = self.job(ident)
        old.pop('version')
        old['status'] = 'failed'
        old['receipts'] = {'B-duty': {'sent': True, 'message_id': 'om_oldFailure'}}
        self.store.put_document(notifications.CHANNEL, ident, old)
        event_id = self.rows()[0]['id']
        for _ in range(3):
            self.store.mark_outbox_event(event_id, 'pending')
        self.assertEqual(len(self.rows('failed')), 1)
        self.restart()
        self.worker.recover()
        self.assertEqual(len(self.rows()), 1)
        self.drain()
        updated = self.job(ident)
        self.assertEqual(updated['status'], 'ready')
        self.assertEqual(updated['failure_receipts'], old['receipts'])
        self.assertEqual(updated['receipts']['B-duty']['message_id'], 'om_fixture')
        old_uuid = str(uuid.uuid5(uuid.NAMESPACE_URL, notifications.CHANNEL + ident + 'B-duty'))
        self.assertNotEqual(self.send.call_args.kwargs['message_uuid'], old_uuid)
        self.restart()
        self.drain()
        self.assertEqual(self.check.call_count, 1)
        self.assertEqual(self.send.call_count, 1)

    def test_old_successful_results_are_not_changed_or_resent(self):
        ident = self.enqueue()
        self.drain()
        old = self.job(ident)
        old.pop('version')
        self.store.put_document(notifications.CHANNEL, ident, old)
        self.restart()
        self.drain()
        self.assertEqual(self.job(ident), old)
        self.assertEqual(self.send.call_count, 1)
        self.assertEqual(self.check.call_count, 1)

    def test_old_ready_job_with_exhausted_delivery_resumes_without_recheck(self):
        ident = self.enqueue()
        self.send.side_effect = lambda text, recipients, **_: (False, '', [
            {'open_id': recipients[0], 'failure_kind': 'not_sent'}])
        self.drain()
        old = self.job(ident)
        old.pop('version')
        self.store.put_document(notifications.CHANNEL, ident, old)
        row = self.rows('leased')[0]
        self.store.mark_outbox_event(row['id'], 'failed')
        self.restart()
        self.send.side_effect = self.accept
        self.drain()
        self.assertEqual(self.check.call_count, 1)
        self.assertEqual(self.send.call_count, 2)
        self.assertTrue(self.job(ident)['receipts']['B-duty']['sent'])

    def test_concurrent_duplicate_handoff_cannot_reset_saved_receipts(self):
        barrier = threading.Barrier(2)
        release_second = threading.Event()
        first_completed = threading.Event()
        def recipients(*_, **__):
            index = barrier.wait(timeout=5)
            if index:
                release_second.wait(timeout=5)
            return '', ['B-duty'], ''
        self.service._recipients_for_building_codes.side_effect = recipients
        errors = []
        def submit():
            try:
                self.enqueue()
                first_completed.set()
            except Exception as exc:
                errors.append(exc)
        threads = [threading.Thread(target=submit) for _ in range(2)]
        for thread in threads:
            thread.start()
        try:
            # One handoff finishes while the duplicate still resolves recipients.
            self.assertTrue(first_completed.wait(timeout=5))
            self.drain()
        finally:
            release_second.set()
            for thread in threads:
                thread.join(timeout=5)
        self.assertFalse(errors)
        self.assertTrue(all(not thread.is_alive() for thread in threads))
        self.assertEqual(self.check.call_count, 1)
        self.assertEqual(self.send.call_count, 1)
        ident = hashlib.sha256(b'recB:one').hexdigest()
        self.assertTrue(self.job(ident)['receipts']['B-duty']['sent'])

    def test_installer_uses_existing_guarded_sender_and_correct_check_method(self):
        handlers = {}
        app = SimpleNamespace(add_event_handler=lambda event, callback: handlers.update({event: callback}))
        convergence = SimpleNamespace(maintenance_check=Mock(return_value={'records': []}),
            change_check=Mock(return_value={'records': []}), _persist_lock=threading.Lock())
        runtime = SimpleNamespace(service=self.service, state_store=self.store)
        fake_server = SimpleNamespace(_send_text_to_open_ids_guarded=self.send)
        with patch.dict(sys.modules, {'lan_bitable_template_portal.server': fake_server}):
            notifications.install_notice_plan_checks(app, runtime, lambda: convergence)
        try:
            runtime.notice_plan_checks.check({'work_type': 'change'})
            convergence.change_check.assert_called_once_with(records=[{'work_type': 'change'}])
            runtime.notice_plan_checks.check({'work_type': 'repair'})
            convergence.maintenance_check.assert_called_once_with(records=[{'work_type': 'repair'}])
            self.assertIs(runtime.notice_plan_checks.send, self.send)
            self.assertTrue(callable(self.service._notice_plan_guard))
        finally:
            asyncio.run(handlers['shutdown']())

    def test_lease_delays_real_attempts_without_new_timer_or_retry_exhaustion(self):
        self.enqueue()
        self.check.side_effect = TimeoutError()
        self.drain()
        self.advance(notifications.RETRY_SECONDS - 1)
        self.drain()
        self.assertEqual(self.check.call_count, 1)
        self.advance(2)
        self.drain()
        self.assertEqual(self.check.call_count, 2)

    def test_stop_prevents_check_and_send_leaving_lease_for_next_start(self):
        self.enqueue()
        self.worker.stopped = True
        self.drain()
        self.check.assert_not_called()
        self.send.assert_not_called()
        self.restart()
        self.drain()
        self.assertEqual(self.check.call_count, 1)


if __name__ == '__main__':
    unittest.main()
