"""Isolated source snapshots and Feishu receipts; no live sends or cloud writes."""
import copy
import datetime as dt
from pathlib import Path
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lan_bitable_template_portal import planned_notice_deadlines as deadlines
from lan_bitable_template_portal import notice_recipients
from lan_bitable_template_portal.portal_service import MaintenancePortalService, BUILDING_OPEN_ID_MAP
from lan_bitable_template_portal.state_store import LanPortalStateStore


class DeadlineTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.store = LanPortalStateStore(Path(temp.name) / 'state.sqlite3')
        self.addCleanup(self.store.shutdown_write_worker)
        self.current = dt.datetime(2026, 10, 22, 9, tzinfo=deadlines.ZONE)
        self.clock_patch = patch.object(notice_recipients.time, 'time', side_effect=lambda: self.current.timestamp())
        self.clock_patch.start()
        self.addCleanup(self.clock_patch.stop)
        self.records, self.people, self.disabled = [], {}, set()
        self.snapshot = {'schema': 4, 'app_id': 'cli_fixture', 'loaded_at': self.current.timestamp(), 'people': self.people}
        self.statuses = {kind: {'status': 'success', 'refreshed_at': self.current.timestamp()}
                         for kind in ('maintenance', 'change', 'repair')}
        self.service = object.__new__(MaintenancePortalService)
        self.service.app_token, self.service.table_id = 'fixture_base', 'fixture_table'
        self.service._source_refresh_status = self.statuses
        self.service.refresh_if_interval_elapsed = Mock()
        self.service._workbench_records = Mock(side_effect=lambda **_: copy.deepcopy(self.records))
        self.service._work_status_by_records = Mock(return_value={})
        self.service._signature_public_base_url = Mock(return_value='https://fixture.invalid')
        self.store.get_source_scope_snapshot = Mock(side_effect=lambda _: {'exists': True,
            'meta': {'source_refresh_status': copy.deepcopy(self.statuses)}})
        self.auth = SimpleNamespace(current_feishu_app_id=lambda: 'cli_fixture',
            _account_directory_provider=lambda: self.snapshot, _account_directory_loader=Mock(),
            account_profile=Mock(return_value={'modules': list(deadlines.TYPES), 'scope_rules': ['ALL']}),
            current_feishu_open_id=self.verify)
        self.sender = Mock(side_effect=self.accept)
        self.worker = deadlines.PlannedNoticeDeadlines(self.service, self.store, self.auth, self.sender, clock=lambda: self.current)

    def verify(self, identity, app_id):
        if identity in self.disabled or app_id != self.auth.current_feishu_app_id():
            return None
        if identity in BUILDING_OPEN_ID_MAP.values():
            return identity
        matches = [person for person in self.people.values() if identity in person.get('login_ids', [])]
        if len(matches) == 1 and matches[0]['selectable'] and not matches[0].get('inactive') and matches[0]['login_ids'] == [identity]:
            return identity
        return None

    @staticmethod
    def accept(message, recipients, *, message_uuid):
        return True, 'accepted', [{'open_id': recipients[0], 'message_id': 'om_fixture', 'sender_app_id': 'cli_fixture'}]

    def advance(self, minutes=5):
        self.current += dt.timedelta(minutes=minutes)
        self.snapshot['loaded_at'] = self.current.timestamp()
        for status in self.statuses.values():
            status['refreshed_at'] = self.current.timestamp()

    def engineer(self, key='engineer', scopes=None, **values):
        self.people[key] = {'id': key, 'name': key, 'scopes': scopes or ['A'],
            'position': '工程师', 'specialties': ['暖通'], 'login_ids': ['ou_' + key],
            'selectable': True, 'inactive': False, **values}

    def plan(self, rid='recOne', scopes=None, kind='maintenance', status='未开始', **fields):
        scopes = scopes or ['A']
        values = {'楼栋': '、'.join(scope + '楼' for scope in scopes), '专业': '暖通',
            '维护总项': rid, '维护实施状态': status, '计划开始维护时间': '2026-10-01',
            '计划结束维护时间': '2026-10-22 18:30', **fields}
        if kind == 'change':
            values.update(变更简述=rid, 变更进度=status, 计划开始时间='2026-10-01', 计划结束时间='2026-10-22 18:30')
        record = {'record_id': rid, 'work_type': kind, 'display_fields': values}
        self.records.append(record)
        return record

    def recipients(self):
        return [call.args[1][0] for call in self.sender.call_args_list]

    def state(self):
        return self.store.get_document(deadlines.NS, self.current.date().isoformat()) or {}

    def test_before_nine_does_not_read_or_send(self):
        self.current = self.current.replace(hour=8, minute=59)
        self.plan()
        self.worker.tick()
        self.sender.assert_not_called()
        self.service.refresh_if_interval_elapsed.assert_not_called()
        self.store.get_source_scope_snapshot.assert_not_called()

    def test_nine_aggregates_per_recipient_and_preserves_all_buildings(self):
        for code in BUILDING_OPEN_ID_MAP:
            self.plan('rec' + code, scopes=[code])
            self.engineer('engineer' + code, scopes=[code])
        self.plan('recA2')
        self.engineer('wrongmajor', specialties=['电气'])
        self.engineer('supervisor', position='主管')
        self.worker.tick()
        self.assertEqual(set(self.recipients()), set(BUILDING_OPEN_ID_MAP.values()) | {'ou_engineer' + code for code in BUILDING_OPEN_ID_MAP})
        # A second tick must not repeat any successful daily reminders.
        self.advance()
        self.worker.tick()
        bodies = {call.args[1][0]: call.args[0] for call in self.sender.call_args_list}
        self.assertIn('recA2', bodies[BUILDING_OPEN_ID_MAP['A']])
        self.assertIn('recA2', bodies['ou_engineerA'])
        self.assertNotIn('recA2', bodies[BUILDING_OPEN_ID_MAP['E']])
        self.assertIn('scope=E', bodies[BUILDING_OPEN_ID_MAP['E']])
        self.assertIn('/workbench-lite?', bodies[BUILDING_OPEN_ID_MAP['A']])
        self.assertEqual(len(self.recipients()), len(set(self.recipients())))
        self.assertTrue(self.state()['complete'])

    def test_only_final_planned_day_and_unfinished_states_are_selected(self):
        self.plan('eligible')
        self.plan('ongoing', status='进行中')
        self.plan('finished', status='正常结束')
        self.plan('cancelled', status='已取消，未完成')
        self.plan('unknown', status='')
        self.plan('tomorrow', 计划结束维护时间='2026-10-23')
        self.plan('yesterday', 计划结束维护时间='2026-10-21')
        self.plan('badwindow', 计划开始维护时间='2026-10-24')
        self.plan('actualonly', 计划结束维护时间='', 实际结束时间='2026-10-22')
        self.plan('bad', 计划结束维护时间=True)
        self.assertEqual({item['source_record_id'] for item in self.worker.due(self.current).values()}, {'eligible', 'ongoing'})

    def test_cross_month_and_millisecond_timezone_deadlines(self):
        self.plan('crossmonth', 计划开始维护时间='2026-09-20')
        self.plan('milliseconds', 计划结束维护时间=int(dt.datetime(2026, 10, 21, 16, tzinfo=dt.timezone.utc).timestamp() * 1000))
        self.plan('offset', 计划结束维护时间='2026-10-21T16:30:00Z')
        self.assertEqual(len(self.worker.due(self.current)), 3)

    def test_change_deadline_uses_change_fields_and_repair_ignores_actual_end(self):
        self.plan('change', kind='change')
        record = self.plan('repair', kind='repair')
        record['display_fields'].update(维修结束时间='2026-10-22', 故障发生时间='2026-10-22')
        self.assertEqual(deadlines.plan_times(self.service, record), (None, None))
        record['display_fields']['期望完成时间'] = '2026-10-22'
        self.assertEqual(deadlines.plan_times(self.service, record)[1].date(), self.current.date())
        self.assertEqual({item['source_record_id'] for item in self.worker.due(self.current).values()}, {'change'})

    def test_confirmed_end_in_current_period_is_not_reminded_while_source_lags(self):
        self.plan()
        self.service._work_status_by_records.return_value = {'recOne': {'status': '已结束', 'ended_at': '2026-10-21 16:00'}}
        self.assertFalse(self.worker.due(self.current))
        self.service._work_status_by_records.return_value = {'recOne': {'status': '已结束', 'ended_at': '2026-09-21 16:00'}}
        self.assertTrue(self.worker.due(self.current))

    def test_starting_late_catches_up_without_resending_after_restart(self):
        self.current = self.current.replace(hour=11)
        self.advance(0)
        self.plan()
        self.worker.tick()
        self.assertEqual(len(self.recipients()), 1)
        restarted = deadlines.PlannedNoticeDeadlines(self.service, self.store, self.auth, self.sender, clock=lambda: self.current)
        self.advance(20)
        restarted.tick()
        self.assertEqual(len(self.recipients()), 1)
        self.service.refresh_if_interval_elapsed.assert_called_once()

    def test_failure_retries_only_failed_recipient_with_same_uuid(self):
        self.plan()
        self.engineer()
        def fail_engineer(message, people, **kwargs):
            if people == ['ou_engineer']:
                raise TimeoutError('private diagnostic')
            return self.accept(message, people, **kwargs)
        self.sender.side_effect = fail_engineer
        self.worker.tick()
        first = self.sender.call_args
        self.advance()
        self.sender.side_effect = self.accept
        self.worker.tick()
        self.assertEqual(self.recipients(), [BUILDING_OPEN_ID_MAP['A'], 'ou_engineer', 'ou_engineer'])
        self.assertEqual(self.sender.call_args.kwargs, first.kwargs)
        self.assertEqual(self.sender.call_args.args[0], first.args[0])
        self.assertNotIn('private diagnostic', str(self.state()))
        self.assertTrue(self.state()['complete'])

    def test_unconfirmed_delivery_outside_idempotency_window_is_not_repeated(self):
        self.plan()
        self.sender.side_effect = TimeoutError()
        self.worker.tick()
        self.advance(60)
        self.worker.tick()
        self.assertEqual(self.sender.call_count, 1)
        self.assertEqual(next(iter(self.state()['deliveries'].values()))['state'], 'delivery_uncertain')
        self.assertFalse(self.state()['complete'])

    def test_completed_between_recipients_is_not_sent_to_engineer(self):
        record = self.plan()
        self.engineer()
        def complete(message, people, **kwargs):
            record['display_fields']['维护实施状态'] = '已完成'
            return self.accept(message, people, **kwargs)
        self.sender.side_effect = complete
        self.worker.tick()
        self.assertEqual(self.recipients(), [BUILDING_OPEN_ID_MAP['A']])

    def test_transfer_and_permission_revocation_are_rechecked(self):
        self.plan()
        self.engineer()
        def transfer(message, people, **kwargs):
            self.people['engineer']['scopes'] = ['B']
            return self.accept(message, people, **kwargs)
        self.sender.side_effect = transfer
        self.worker.tick()
        self.assertEqual(self.recipients(), [BUILDING_OPEN_ID_MAP['A']])
        self.people['engineer']['scopes'] = ['A']
        self.auth.account_profile.return_value = {'modules': [], 'scope_rules': ['ALL']}
        _, audience, _ = self.worker.audience(self.worker.due(self.current))
        self.assertNotIn('ou_engineer', audience)

    def test_hr_failure_preserves_duty_send_then_resumes_engineer(self):
        self.plan()
        self.engineer()
        self.auth._account_directory_refresh_failed = True
        self.worker.tick()
        self.assertEqual(self.recipients(), [BUILDING_OPEN_ID_MAP['A']])
        self.assertFalse(self.state()['complete'])
        self.auth._account_directory_refresh_failed = False
        self.advance()
        self.worker.tick()
        self.assertEqual(self.recipients(), [BUILDING_OPEN_ID_MAP['A'], 'ou_engineer'])

    def test_invalid_identity_and_unrelated_specialty_do_not_widen_recipients(self):
        self.plan()
        self.engineer('disabled')
        self.disabled.add('ou_disabled')
        self.engineer('inactive', inactive=True)
        self.engineer('ambiguous', login_ids=['ou_1', 'ou_2'])
        self.engineer('wrong', scopes=['B'])
        self.engineer('major', specialties=['电气'])
        self.engineer('all', scopes=['ALL'])
        _, recipients, pending = self.worker.audience(self.worker.due(self.current))
        self.assertEqual(set(recipients), {BUILDING_OPEN_ID_MAP['A']})
        self.assertTrue(pending)

    def test_source_failure_does_not_mean_empty_or_complete(self):
        self.plan()
        self.statuses['maintenance']['status'] = 'retained'
        self.worker.tick()
        self.sender.assert_not_called()
        self.assertFalse(self.state().get('complete'))
        self.statuses['maintenance']['status'] = 'success'
        self.advance()
        self.worker.tick()
        self.assertTrue(self.state()['complete'])

    def test_refresh_finishing_after_tick_start_is_not_rejected_as_future_snapshot(self):
        self.plan()
        self.service.refresh_if_interval_elapsed.side_effect = lambda **_: self.advance(1)
        self.worker.tick()
        self.assertEqual(len(self.recipients()), 1)

    def test_known_rejection_can_regroup_without_resending_successful_items(self):
        one = self.plan('one')
        self.plan('two')
        self.sender.return_value = (False, 'rejected', [{'open_id': BUILDING_OPEN_ID_MAP['A'], 'code': 230013}])
        self.sender.side_effect = None
        self.worker.tick()
        one['display_fields']['维护实施状态'] = '已完成'
        self.sender.side_effect = self.accept
        self.advance()
        self.worker.tick()
        self.assertEqual(self.sender.call_count, 2)
        self.assertNotIn('one', self.sender.call_args.args[0])
        self.assertIn('two', self.sender.call_args.args[0])

    def test_next_day_does_not_send_yesterdays_failed_message(self):
        self.plan()
        self.sender.side_effect = TimeoutError()
        self.worker.tick()
        self.advance(24 * 60)
        self.worker.tick()
        self.assertEqual(self.sender.call_count, 1)
        self.assertTrue(self.state()['complete'])

    def test_local_lock_rejects_overlapping_ticks(self):
        self.plan()
        self.worker.lock.acquire()
        try:
            self.worker.tick()
            self.sender.assert_not_called()
        finally:
            self.worker.lock.release()

    def test_all_six_plan_types_use_existing_status_and_specialty_mapping(self):
        for kind in deadlines.TYPES:
            record = self.plan('rec' + kind, kind=kind)
            if kind == 'repair':
                record['building_codes'] = ['A']
                record['title'] = 'A楼设备检修'
                record['display_fields'].update(所属专业='暖通', 期望完成时间='2026-10-22')
        items = self.worker.due(self.current)
        self.assertEqual({item['kind'] for item in items.values()}, set(deadlines.TYPES))

    def test_unknown_specialty_still_notifies_duty_without_guessing_engineers(self):
        self.plan(专业='未核对专业')
        self.engineer()
        self.worker.tick()
        self.assertEqual(self.recipients(), [BUILDING_OPEN_ID_MAP['A']])

    def test_reused_record_next_period_has_independent_reminder(self):
        record = self.plan()
        self.worker.tick()
        self.current = self.current.replace(month=11)
        self.advance(0)
        record['display_fields'].update(计划开始维护时间='2026-11-01', 计划结束维护时间='2026-11-22')
        self.worker.tick()
        self.assertEqual(self.sender.call_count, 2)
        self.assertNotEqual(self.sender.call_args_list[0].kwargs['message_uuid'], self.sender.call_args_list[1].kwargs['message_uuid'])

    def test_crash_after_remote_acceptance_reuses_persisted_uuid(self):
        self.plan()
        accepted = set()
        def transport(message, recipients, **kwargs):
            accepted.add(kwargs['message_uuid'])
            return self.accept(message, recipients, **kwargs)
        self.sender.side_effect = transport
        original = self.store.put_document
        def crash(namespace, key, document):
            if namespace == deadlines.NS and any(item.get('sent') for item in document.get('deliveries', {}).values()):
                raise OSError('simulated receipt write failure')
            return original(namespace, key, document)
        with patch.object(self.store, 'put_document', side_effect=crash):
            self.worker.tick()
        self.assertEqual(len(accepted), 1)
        self.advance()
        restarted = deadlines.PlannedNoticeDeadlines(self.service, self.store, self.auth, self.sender, clock=lambda: self.current)
        restarted.tick()
        self.assertEqual(self.sender.call_count, 2)
        self.assertEqual(len(accepted), 1)
        self.assertTrue(self.state()['complete'])

    def test_failed_sending_intent_save_never_calls_transport(self):
        self.plan()
        original = self.store.put_document
        def crash(namespace, key, document):
            if namespace == deadlines.NS and any(item.get('attempted_at') for item in document.get('deliveries', {}).values()):
                raise OSError('intent write failed')
            return original(namespace, key, document)
        with patch.object(self.store, 'put_document', side_effect=crash):
            self.worker.tick()
        self.sender.assert_not_called()
        self.advance()
        self.worker.tick()
        self.assertEqual(self.sender.call_count, 1)

    def test_program_shutdown_stops_remaining_recipients(self):
        self.plan()
        self.engineer()
        def stop(message, recipients, **kwargs):
            self.worker.closed = True
            return self.accept(message, recipients, **kwargs)
        self.sender.side_effect = stop
        self.worker.tick()
        self.assertEqual(self.sender.call_count, 1)
        self.assertFalse(self.state()['complete'])

    def test_day_change_during_send_does_not_dispatch_previous_day_to_next_person(self):
        self.plan()
        self.engineer()
        def next_day(message, recipients, **kwargs):
            self.advance(24 * 60)
            return self.accept(message, recipients, **kwargs)
        self.sender.side_effect = next_day
        self.worker.tick()
        self.assertEqual(self.sender.call_count, 1)

    def test_large_summary_is_split_without_losing_or_duplicating_plans(self):
        for index in range(45):
            self.plan(f'plan{index:03}')
        self.worker.tick()
        self.assertEqual(self.sender.call_count, 3)
        state = self.state()
        keys = [key for receipt in state['deliveries'].values() for key in receipt['items']]
        self.assertEqual(len(keys), 45)
        self.assertEqual(len(set(keys)), 45)
        self.assertTrue(state['complete'])


class SchedulerTests(unittest.TestCase):
    def test_scheduler_uses_shanghai_nine_and_recovers_without_new_threads(self):
        from clipflow_backend.main import FastAPIPortalController
        from apscheduler.schedulers.background import BackgroundScheduler
        controller = object.__new__(FastAPIPortalController)
        controller._scheduler = None
        controller._write_runtime_heartbeat = Mock()
        with patch.object(BackgroundScheduler, 'start'):
            controller._start_scheduler()
        scheduler = controller._scheduler
        try:
            job = next(job for job in scheduler.get_jobs() if job.id == 'planned_notice_deadlines')
            self.assertEqual(job.max_instances, 1)
            before = dt.datetime(2026, 10, 22, 8, 59, 59, tzinfo=deadlines.ZONE)
            self.assertEqual(job.trigger.get_next_fire_time(None, before).astimezone(deadlines.ZONE).hour, 9)
            self.assertIsNotNone(job.next_run_time)
            self.assertEqual(job.executor, 'default')
        finally:
            scheduler._executors['maintenance']._pool.shutdown(wait=False)

    def test_mock_and_stopping_runtime_cannot_send(self):
        from clipflow_backend.main import FastAPIPortalController
        controller = object.__new__(FastAPIPortalController)
        controller._stopping_event = threading.Event()
        controller._plan_deadlines = Mock()
        with patch('clipflow_backend.main._mock_external_enabled', return_value=True):
            controller._run_scheduled_plan_deadlines()
        controller._stopping_event.set()
        with patch('clipflow_backend.main._mock_external_enabled', return_value=False):
            controller._run_scheduled_plan_deadlines()
        controller._plan_deadlines.tick.assert_not_called()


if __name__ == '__main__':
    unittest.main()
