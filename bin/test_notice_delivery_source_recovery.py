"""Offline source-finalization and per-recipient recovery regressions."""
import copy
import datetime as dt
import sys
import tempfile
import threading
import time
import unittest
from concurrent.futures import Future
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lan_bitable_template_portal import portal_service as portal, server
from lan_bitable_template_portal.feishu_delivery import PublicFeishuSender
from lan_bitable_template_portal.state_store import LanPortalStateStore


def field(name, *, formula=False, options=()):
    return portal.FieldMeta('field-test', name, 'Formula' if formula else 'SingleSelect',
                            20 if formula else 3, False, {}, list(options), formula)


class ManualExecutor:
    """Keep submitted work pending without creating any worker thread."""
    def __init__(self):
        self.calls = []

    def submit(self, fn, *args, **kwargs):
        future = Future()
        self.calls.append((future, fn, args, kwargs))
        return future

    def run(self, index=0):
        future, fn, args, kwargs = self.calls[index]
        future.set_running_or_notify_cancel()
        try:
            future.set_result(fn(*args, **kwargs))
        except Exception as exc:
            future.set_exception(exc)


class NoticeDeliverySourceRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = LanPortalStateStore(Path(self.tmp.name) / 'state.sqlite3')
        self.service = self.new_service()
        self.auth = SimpleNamespace(scope_allowed=Mock(return_value=True),
                                    account_profile=Mock(return_value={'modules': ['maintenance']}))
        self.runtime = type('IsolatedRecoveryRuntime', (server.PortalRuntime,), {
            'service': self.service, 'state_store': self.store, 'auth_manager': self.auth,
            'clear_payload_cache': Mock(), '_reconcile_source_ongoing_after_target_terminal': Mock(),
        })
        self.guard = patch.object(portal, 'external_real_write_guard', return_value={
            'mock_external': False, 'real_write_allowed': True})
        self.guard.start()
        self.network = patch('requests.sessions.Session.request', side_effect=AssertionError('network forbidden'))
        self.network.start()
        self.addCleanup(self.guard.stop)
        self.addCleanup(self.network.stop)
        self.prepared = {'job_id': 'job-end', 'text': '110 maintenance end',
                         'recipients': ['duty110', 'operator'], 'message_signature': 'signature'}
        self.seed_job()

    def new_service(self):
        service = object.__new__(portal.MaintenancePortalService)
        service._state_store = self.store
        service._jobs_lock = threading.RLock()
        service._refresh_lock = threading.RLock()
        service._jobs = {}
        service._records = []
        service._change_records = []
        service.app_token, service.table_id = 'source-app', 'source-table'
        service._field_meta_by_name = {'维护实施状态': field('维护实施状态', options=['进行中', '已结束'])}
        service._change_field_meta_by_name = {'变更进度': field('变更进度', options=['进行中', '已结束'])}
        service._touch_state_cache_version = Mock()
        service._load_table_fields = Mock(side_effect=AssertionError('unexpected metadata network read'))
        return service

    def seed_job(self, job_id='job-end'):
        job = {'job_id': job_id, 'phase': 'success', 'remote_record_id': 'rec-target',
               'target_record_id': 'rec-target', 'message_signature': 'signature',
               'request': {'_auth_open_id': 'owner', 'scope': '110', 'work_type': 'maintenance',
                           'action': 'end', 'source_record_id': 'rec-source'}}
        self.service._jobs[job_id] = job
        self.store.put_document(portal.STATE_NS_ACTION_JOB, job_id, job)
        return job

    def tearDown(self):
        self.store.shutdown_write_worker(timeout=2)
        self.tmp.cleanup()

    def source(self, *, formula=False, work_type='maintenance'):
        status = '维护实施状态' if work_type == 'maintenance' else '变更进度'
        remote = {status: '进行中', '实际开始时间': '2026-10-19 09:30',
                  '计划开始维护时间': '2026-10-19 09:30'}
        row = {'record_id': 'rec-source', 'work_type': work_type, 'display_fields': copy.deepcopy(remote)}
        setattr(self.service, '_records' if work_type == 'maintenance' else '_change_records', [row])
        metadata = getattr(self.service, '_field_meta_by_name' if work_type == 'maintenance' else '_change_field_meta_by_name')
        metadata[status] = field(status, formula=formula, options=['进行中', '已结束'])
        self.service._request_json = Mock(side_effect=lambda *a, **k: {
            'data': {'record': {'record_id': 'rec-source', 'fields': copy.deepcopy(remote)}}})
        self.service._patch_record_fields = Mock(side_effect=lambda **kw: remote.update(kw['fields']))
        payload = {'source_record_id': 'rec-source', 'target_record_id': 'rec-target',
                   'work_type': work_type, 'ended_at': '2026-10-19 18:30'}
        return payload, remote, row

    def receipt(self, recipient, *, ok=True, **extra):
        return {'open_id': recipient, 'ok': ok, 'message_id': 'message-' + recipient if ok else '',
                'message': 'ok' if ok else 'rejected', 'failure_kind': '' if ok else 'not_sent', **extra}

    def deliver(self, side_effect):
        with patch.object(portal, 'send_text_to_open_ids', side_effect=side_effect) as sender:
            result = self.service.send_action_personal_message(self.prepared)
        return result, sender

    def send_ok(self, text, recipients, **kw):
        return True, 'ok', [self.receipt(recipients[0], message_uuid=kw['message_uuid'])]

    def personal_doc(self):
        return self.store.get_document('notice_personal_delivery', 'job-end')

    def set_due(self, channel):
        events = (self.store.list_outbox_events(channel) + self.store.list_outbox_events(channel, status='leased')
                  + self.store.list_outbox_events(channel, status='deferred'))
        for event in events:
            self.store.mark_outbox_event(event['id'], 'pending', count_attempt=False)
        if channel == 'notice_personal':
            document = self.personal_doc()
            document['next_check_at'] = 0
            self.store.put_document('notice_personal_delivery', 'job-end', document)
        else:
            for event in events:
                key = event['payload'].get('_source_recovery_id')
                document = self.store.get_document('notice_source_recovery', key)
                document['next_check_at'] = 0
                self.store.put_document('notice_source_recovery', key, document)

    def test_writable_maintenance_status_and_exact_end_are_confirmed(self):
        payload, remote, row = self.source()
        self.assertTrue(self.service.sync_notice_source_ended_fields(payload))
        sent = self.service._patch_record_fields.call_args.kwargs
        self.assertEqual(sent['record_id'], 'rec-source')
        self.assertEqual(sent['fields']['维护实施状态'], '已结束')
        self.assertEqual(sent['fields']['实际结束时间'], int(dt.datetime(2026, 10, 19, 18, 30).timestamp() * 1000))
        self.assertEqual(row['display_fields']['维护实施状态'], '已结束')

    def test_change_source_uses_change_metadata_and_table(self):
        payload, remote, _ = self.source(work_type='change')
        self.assertTrue(self.service.sync_notice_source_ended_fields(payload))
        sent = self.service._patch_record_fields.call_args.kwargs
        self.assertEqual(sent['table_id'], portal.CHANGE_SOURCE_TABLE_ID)
        self.assertEqual(sent['fields']['变更进度'], '已结束')
        self.assertNotIn('维护实施状态', sent['fields'])

    def change_lookup_source(self):
        payload, remote, row = self.source(formula=True, work_type='change')
        remote.update({
            '变更进度': {'type': 3, 'value': ['进行中']},
            '实际开始时间': {'type': 5, 'value': [1792976400000]},
            '实际结束时间': {'type': 5, 'value': [1793092632123]},
            '开始/结束状态打标': {'type': 3, 'value': ['正常结束']},
            '计划开始日期（阿里）': 1792976400000,
            '计划结束日期（阿里）': 1793199600000,
            '变更楼栋': 'A楼', '变更简述': 'HVDC防护罩更换',
        })
        self.service._change_field_meta_by_name['实际结束时间'] = portal.FieldMeta(
            'lookup-end', '实际结束时间', 'Lookup', 19, False, {}, [], False)
        row['raw_fields'] = copy.deepcopy(remote)
        row['display_fields'] = copy.deepcopy(remote)
        for name in ('实际开始时间', '实际结束时间', '计划开始日期（阿里）'):
            row['display_fields'][name] = self.service._format_source_datetime(
                self.service._repair_management_datetime_ms(remote[name]))
        payload['ended_at'] = '2026-10-27 17:17:12.123'
        return payload, remote, row

    def change_option_source(self):
        payload, remote, row = self.change_lookup_source()
        remote.update({'变更进度': ['opt-progress'], '开始/结束状态打标': ['opt-finished'],
                       '实际开始时间': 46321.375, '实际结束时间': 46322.7201388889})
        for name, options in (('变更进度', {'opt-progress': '进行中'}),
                              ('开始/结束状态打标', {'opt-finished': '正常结束'})):
            self.service._change_field_meta_by_name[name] = portal.FieldMeta(
                'field-' + name, name, 'Formula', 20, False, options, list(options.values()), True)
        row.update(self.service._normalize_record({'record_id': 'rec-source', 'fields': copy.deepcopy(remote)},
            meta_by_name=self.service._change_field_meta_by_name, work_type='change', notice_type='变更通告'))
        payload['ended_at'] = '2026-10-27 17:17:00'
        return payload, remote, row

    def test_formula_option_ids_and_excel_dates_confirm_without_cloud_writes(self):
        payload, remote, row = self.change_option_source()
        for wrapped in (False, True):
            with self.subTest(wrapped=wrapped):
                remote['开始/结束状态打标'] = ({'type': 3, 'value': ['opt-finished']}
                                             if wrapped else ['opt-finished'])
                self.assertTrue(self.service.sync_notice_source_ended_fields(payload))
                self.assertEqual(row['display_fields']['开始/结束状态打标'], '正常结束')
                self.assertEqual(self.service._change_progress_value(row), '已结束')
                refreshed = self.service._normalize_record({'record_id': 'rec-source', 'fields': remote},
                    meta_by_name=self.service._change_field_meta_by_name, work_type='change', notice_type='变更通告')
                self.assertEqual(self.service._change_progress_value(refreshed), '已结束')
        self.service._patch_record_fields.assert_not_called()

    def test_cold_source_metadata_is_retained_for_plan_status_after_binding(self):
        payload, _, row = self.change_option_source()
        metadata = self.service._change_field_meta_by_name
        self.service._change_field_meta_by_name = {}
        self.service._load_table_fields = Mock(return_value=(list(metadata.values()), metadata))
        self.assertTrue(self.service.sync_notice_source_ended_fields(payload))
        self.assertEqual(self.service._change_progress_value(row), '已结束')
        self.assertNotIn(row, self.service._filter_change_records(scope='A'))
        self.service._load_table_fields.assert_called_once()
        self.service._patch_record_fields.assert_not_called()

    def test_unknown_execution_option_id_stays_pending(self):
        payload, remote, _ = self.change_option_source()
        remote['开始/结束状态打标'] = ['opt-unknown']
        with self.assertRaises(portal.NoticeSourcePending):
            self.service.sync_notice_source_ended_fields(payload)
        self.service._patch_record_fields.assert_not_called()

    def test_writable_maintenance_option_id_already_ended_does_not_repeat_write(self):
        payload, remote, _ = self.source()
        remote.update({'维护实施状态': ['opt-ended'], '实际结束时间': payload['ended_at']})
        self.service._field_meta_by_name['维护实施状态'] = portal.FieldMeta(
            'field-state', '维护实施状态', 'SingleSelect', 3, False,
            {'opt-ended': '已结束'}, ['进行中', '已结束'], False)
        self.assertTrue(self.service.sync_notice_source_ended_fields(payload))
        self.service._patch_record_fields.assert_not_called()

    def test_native_planned_binding_commits_ended_identity_and_keeps_independent_second_notice(self):
        from clipflow_backend.main import FastAPIPortalController
        from fastapi.testclient import TestClient

        payload, _, row = self.change_option_source()
        target = self.change_target()
        target['raw_fields']['变更结束时间'] = 1793092620000
        validation = self.validate_change_target(target)
        self.service._summary_lock = threading.RLock()
        self.service._work_status_dir = Path(self.tmp.name) / 'work-status'
        self.service._summary_dir = Path(self.tmp.name) / 'summaries'
        self.service._legacy_summary_migrated = True
        self.store.upsert_qt_active_item({'active_item_id': 'independent-two', 'target_record_id': 'rec-two',
            'record_id': 'rec-two', 'work_type': 'change', 'notice_type': '变更通告', 'status': '开始',
            'title': 'HVDC防护罩更换（二）', 'building_codes': ['A']}, section='other', origin='portal')
        session = {'user': {'open_id': 'fixture-owner'}, 'role': 'building', 'allowed_scopes': ['A']}
        with (
            patch.object(server.PortalRuntime, 'state_store', self.store),
            patch.object(server.PortalRuntime, 'service', self.service),
            patch('clipflow_backend.main.LanPortalStateStore', return_value=self.store),
            patch.object(server.PortalRuntime.auth_manager, 'account_profile', return_value={'modules': ['change']}),
            patch.object(server.PortalRuntime.auth_manager, 'scope_allowed', return_value=True),
            patch.object(server.PortalRuntime.auth_manager, 'is_admin', return_value=False),
            patch.object(self.service, 'validate_notice_identity_binding', return_value=validation),
        ):
            controller = FastAPIPortalController(host='127.0.0.1', port=18766)
            with patch.object(controller, '_current_session', return_value=session):
                response = TestClient(controller._build_app()).post('/api/notice-identity/bind', json={
                    **payload, 'scope': 'A', 'binding_context': 'planned', 'notice_type': '变更通告',
                    'title': 'HVDC防护罩更换', 'building': 'A楼'})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertTrue(response.json()['data']['source_ended_synced'])
        identity = self.store.resolve_notice_identity(work_type='change', source_record_id='rec-source')
        self.assertEqual(identity['target_record_id'], 'rec-target')
        self.assertEqual(identity['payload']['ended_at'], '2026-10-27 17:17:00.000')
        self.assertEqual(self.service._change_progress_value(row), '已结束')
        self.assertEqual([item['payload']['target_record_id'] for item in self.store.list_visible_qt_active_items()], ['rec-two'])
        self.service._patch_record_fields.assert_not_called()

    def validate_change_target(self, target):
        self.service._source_record_in_scope_snapshot = Mock(return_value={
            'record_id': 'rec-source', 'work_type': 'change', 'display_fields': {}})
        self.service._load_table_fields = Mock(return_value=([], {'变更状态': field('变更状态')}))
        self.service._load_table_records_by_ids = Mock(return_value=[target])
        self.service._target_record_form_fields = Mock(return_value={})
        self.service._target_snapshot_active_payload = Mock(return_value={'active_item_id': 'active-target'})
        with patch.object(portal.config, 'app_token', 'target-app'), \
                patch.object(portal.config, 'get_table_id', return_value='target-table'):
            return self.service.validate_notice_identity_binding(
                scope='A', work_type='change', notice_type='变更通告',
                source_record_id='rec-source', target_record_id='rec-target',
                allow_finished=True, fresh_target=True)

    def change_target(self):
        return {
            'record_id': 'rec-target', 'last_modified_time': 1793092692000,
            'created_time': 1792974275000,
            'raw_fields': {'变更状态': '结束', '变更结束时间': {'type': 5, 'value': [1793092632123]}},
            'display_fields': {'变更状态': '结束', '变更结束时间': '2026-10-27 17:17', '楼栋': 'A楼'},
        }

    def test_binding_uses_raw_actual_end_instead_of_display_or_modification_time(self):
        target = self.change_target()
        for modified in (1793092692000, 1793199600000):
            with self.subTest(modified=modified):
                target['last_modified_time'] = modified
                result = self.validate_change_target(target)
                self.assertEqual(result['target_ended_at'], '2026-10-27 17:17:12.123')
                self.assertTrue(result['target_finished'])
        self.assertEqual(self.service._target_record_finished_at(
            work_type='change', target_record=target, lifecycle={'ended_at': '2026-10-27 17:17'}),
            '2026-10-28 23:00', 'candidate recency keeps its existing metadata fallback')

    def test_binding_without_actual_end_never_falls_back_to_metadata(self):
        target = self.change_target()
        target['raw_fields'].pop('变更结束时间')
        result = self.validate_change_target(target)
        self.assertEqual(result['target_ended_at'], '')
        payload, _, _ = self.source(work_type='change')
        with self.assertRaisesRegex(portal.PortalError, '缺少实际结束时间'):
            self.service.sync_notice_source_ended_fields({**payload, 'ended_at': result['target_ended_at']})
        self.service._patch_record_fields.assert_not_called()

    def test_raw_missing_or_empty_end_is_authoritative_over_stale_display(self):
        target = self.change_target()
        target['display_fields']['变更结束时间'] = 1793092632123
        for raw in ({}, {'变更状态': '结束'}):
            target['raw_fields'] = raw
            self.assertEqual(self.validate_change_target(target)['target_ended_at'], '')
        for empty in (None, '', {'type': 5, 'value': []}):
            with self.subTest(empty=empty):
                target['raw_fields']['变更结束时间'] = empty
                self.assertEqual(self.validate_change_target(target)['target_ended_at'], '')
        target.pop('raw_fields')
        self.assertEqual(self.validate_change_target(target)['target_ended_at'], '2026-10-27 17:17:12.123')

    def test_datetime_precision_in_epoch_lookup_fraction_and_explicit_timezones(self):
        for value in (1793092632123, '1793092632123', 1793092632.123, '1793092632.123',
                      '1793092632123.0', {'type': 5, 'value': [1793092632123]},
                      '[1793092632123]', '2026-10-27 17:17:12.123',
                      '2026-10-27T17:17:12.123+08:00', '2026-10-27T09:17:12.123Z'):
            with self.subTest(value=value):
                self.assertEqual(self.service._repair_management_datetime_ms(value), 1793092632123)
        for milliseconds in range(1000):
            text = f'2026-10-27 17:17:12.{milliseconds:03d}'
            seconds_text = f'1793092632.{milliseconds:03d}'
            expected = 1793092632000 + milliseconds
            for value in (text, float(seconds_text), seconds_text, expected, str(expected)):
                with self.subTest(milliseconds=milliseconds, value=value):
                    self.assertEqual(self.service._repair_management_datetime_ms(value), expected)
        self.assertEqual(self.service._repair_management_datetime_ms(1793092632), 1793092632000)
        self.assertEqual(self.service._parse_notice_datetime('2026年10月27日17时17分'), dt.datetime(2026, 10, 27, 17, 17))

    def test_active_update_with_stale_prior_end_is_not_a_finished_binding(self):
        target = self.change_target()
        target['raw_fields']['变更状态'] = target['display_fields']['变更状态'] = '更新'
        result = self.validate_change_target(target)
        self.assertTrue(result['target_active'])
        self.assertFalse(result['target_finished'])
        self.assertEqual(result['target_ended_at'], '')

    def test_lookup_exact_milliseconds_and_execution_marker_confirm_without_writes(self):
        payload, remote, row = self.change_lookup_source()
        for value in (1793092632123, '1793092632123', {'type': 5, 'value': [1793092632123]}):
            with self.subTest(value=value):
                remote['实际结束时间'] = value
                self.assertTrue(self.service.sync_notice_source_ended_fields(payload))
        for expected in (1793092632123, '2026-10-27T17:17:12.123+08:00', '2026-10-27T09:17:12.123Z'):
            with self.subTest(expected=expected):
                self.assertTrue(self.service.sync_notice_source_ended_fields({**payload, 'ended_at': expected}))
        self.service._patch_record_fields.assert_not_called()
        self.assertEqual(row['display_fields']['变更进度'], '进行中', 'do not overwrite the schedule formula')
        self.assertEqual(self.service._change_progress_value(row), '已结束')

    def test_lookup_one_millisecond_difference_stays_pending_without_writes(self):
        payload, remote, _ = self.change_lookup_source()
        remote['实际结束时间'] = {'type': 5, 'value': [1793092632122]}
        for _ in range(2):
            with self.assertRaisesRegex(portal.NoticeSourcePending, '实际结束时间尚未准确读回'):
                self.service.sync_notice_source_ended_fields(payload)
        self.service._patch_record_fields.assert_not_called()

    def test_writable_source_preserves_seconds_and_milliseconds_without_repeat_patch(self):
        for kind in ('maintenance', 'change'):
            with self.subTest(kind=kind):
                payload, remote, _ = self.source(work_type=kind)
                payload['ended_at'] = '2026-10-27 17:17:12.123'
                self.assertTrue(self.service.sync_notice_source_ended_fields(payload))
                self.assertEqual(remote['实际结束时间'], 1793092632123)
                self.assertTrue(self.service.sync_notice_source_ended_fields(payload))
                self.service._patch_record_fields.assert_called_once()

    def test_delayed_lookup_then_execution_marker_recovers_with_only_reads(self):
        payload, remote, _ = self.change_lookup_source()
        remote['实际结束时间'] = {'type': 5, 'value': []}
        remote['开始/结束状态打标'] = {'type': 3, 'value': ['正常开始']}
        with self.assertRaisesRegex(portal.NoticeSourcePending, '实际结束时间尚未准确读回'):
            self.service.sync_notice_source_ended_fields(payload)
        remote['实际结束时间'] = {'type': 5, 'value': [1793092632123]}
        with self.assertRaisesRegex(portal.NoticeSourcePending, '只读状态尚未结束'):
            self.service.sync_notice_source_ended_fields(payload)
        remote['开始/结束状态打标'] = {'type': 3, 'value': ['正常结束']}
        self.assertTrue(self.service.sync_notice_source_ended_fields(payload))
        self.service._patch_record_fields.assert_not_called()

    def test_missing_or_conflicting_execution_marker_cannot_confirm_formula_end(self):
        for marker in (None, '', '正常开始', '未开始', '流程终止', ['正常结束', '正常开始']):
            for progress in ('进行中', '已结束'):
                with self.subTest(marker=marker, progress=progress):
                    payload, remote, _ = self.change_lookup_source()
                    if marker is None:
                        remote.pop('开始/结束状态打标')
                    else:
                        remote['开始/结束状态打标'] = marker
                    remote['变更进度'] = progress
                    with self.assertRaises(portal.NoticeSourcePending):
                        self.service.sync_notice_source_ended_fields(payload)
                    self.service._patch_record_fields.assert_not_called()

    def test_newer_actual_or_planned_start_cannot_be_closed_by_old_lookup_end(self):
        for name in ('实际开始时间', '计划开始日期（阿里）'):
            with self.subTest(field=name):
                payload, remote, row = self.change_lookup_source()
                remote[name] = {'type': 5, 'value': [1793092632124]}
                row['raw_fields'] = copy.deepcopy(remote)
                row['display_fields'][name] = '2026-10-27 17:17'
                self.assertFalse(self.service.sync_notice_source_ended_fields(payload))
                self.assertEqual(self.service._change_progress_value(row), '进行中')
                self.service._patch_record_fields.assert_not_called()

    def test_verified_execution_updates_plan_status_and_source_ongoing_counts(self):
        _, _, completed = self.change_lookup_source()
        ongoing = copy.deepcopy(completed)
        ongoing['record_id'] = 'rec-newer-cycle'
        ongoing['raw_fields']['实际开始时间'] = 1793092632124
        ongoing['display_fields']['实际开始时间'] = '2026-10-27 17:17'
        self.service._change_records = [completed, ongoing]
        self.service._get_record_memory = Mock(return_value={})
        serialized = self.service._serialize_record(completed)
        self.assertEqual(serialized['source_progress'], '已结束')
        self.assertEqual(serialized['source_status'], '已结束')
        self.assertEqual(self.service._serialize_record(ongoing)['source_progress'], '进行中')
        self.assertEqual([r['record_id'] for r in self.service._filter_change_records(scope='A')], ['rec-newer-cycle'])
        self.assertEqual(len(self.service._filter_change_records(scope='A', progress_values={'已结束'})), 1)
        self.service._summary_lock = threading.RLock()
        self.service.ensure_snapshot_loaded = Mock()
        self.service._load_work_status_items_locked = Mock(return_value=[])
        self.service._repair_records = self.service._zhihang_change_records = []
        sources, zhihang = self.service._ongoing_source_snapshot_records_locked()
        self.assertEqual([r['record_id'] for r in sources], ['rec-newer-cycle'])
        self.assertEqual(zhihang, [])

    def test_confirmed_completion_survives_snapshot_reload_with_stale_raw_fields(self):
        payload, remote, row = self.change_lookup_source()
        for fields in (row['raw_fields'], row['display_fields']):
            fields['实际结束时间'] = ''
            fields['开始/结束状态打标'] = '正常开始'
        self.store.replace_source_table_snapshot('change', {'A': [row]})
        self.assertTrue(self.service.sync_notice_source_ended_fields(payload))
        reloaded = self.store.get_source_table_scope_snapshot('change', 'A')['records'][0]
        self.assertEqual(reloaded['raw_fields']['实际结束时间'], '', 'exercise the unchanged raw snapshot')
        self.assertEqual(reloaded['display_fields']['实际结束时间'], '2026-10-27 17:17:12.123')
        restarted = self.new_service()
        restarted._change_field_meta_by_name = self.service._change_field_meta_by_name
        self.assertEqual(restarted._change_progress_value(reloaded), '已结束')
        self.assertFalse(restarted._source_record_is_ongoing(reloaded))
        self.service._patch_record_fields.assert_not_called()

    def test_normalized_lookup_record_keeps_verified_execution_status(self):
        _, remote, _ = self.change_lookup_source()
        normalized = self.service._normalize_record({'record_id': 'rec-source', 'fields': remote},
            meta_by_name=self.service._change_field_meta_by_name, work_type='change', notice_type='变更通告')
        self.assertEqual(self.service._change_progress_value(normalized), '已结束')

    def test_writable_active_progress_is_not_overridden_by_stale_execution_marker(self):
        _, _, row = self.change_lookup_source()
        self.service._change_field_meta_by_name['变更进度'] = field('变更进度', options=['进行中', '已结束'])
        self.assertEqual(self.service._change_progress_value(row), '进行中')

    def test_legacy_readonly_execution_status_can_confirm_without_separate_marker(self):
        for status in ('正常结束', '延期结束', '已结束'):
            with self.subTest(status=status):
                payload, remote, _ = self.source(formula=True, work_type='change')
                remote.update({'变更进度': status, '实际结束时间': payload['ended_at']})
                if status != '已结束':
                    remote['计划开始日期（阿里）'] = '2026-10-19 09:30'
                self.assertTrue(self.service.sync_notice_source_ended_fields(payload))
                self.service._patch_record_fields.assert_not_called()

    def test_formula_lag_reads_without_repeating_patch_then_completes(self):
        payload, remote, row = self.source(formula=True)
        for _ in range(3):
            with self.assertRaises(portal.NoticeSourcePending):
                self.service.sync_notice_source_ended_fields(payload)
        self.service._patch_record_fields.assert_called_once()
        self.assertNotIn('维护实施状态', self.service._patch_record_fields.call_args.kwargs['fields'])
        self.assertEqual(row['display_fields']['维护实施状态'], '进行中')
        remote['维护实施状态'] = '已结束'
        self.assertTrue(self.service.sync_notice_source_ended_fields(payload))
        self.service._patch_record_fields.assert_called_once()

    def test_readonly_actual_end_is_not_patched(self):
        payload, remote, _ = self.source(formula=True)
        self.service._field_meta_by_name['实际结束时间'] = field('实际结束时间', formula=True)
        with self.assertRaises(portal.NoticeSourcePending):
            self.service.sync_notice_source_ended_fields(payload)
        self.service._patch_record_fields.assert_not_called()

    def test_wrong_end_readback_does_not_report_source_success(self):
        payload, remote, row = self.source()
        self.service._patch_record_fields.side_effect = lambda **kw: remote.update({'维护实施状态': '已结束'})
        with self.assertRaises(portal.NoticeSourcePending):
            self.service.sync_notice_source_ended_fields(payload)
        self.assertEqual(row['display_fields']['维护实施状态'], '进行中')

    def test_later_cycle_cannot_be_closed_by_old_end(self):
        payload, remote, _ = self.source()
        remote['实际开始时间'] = '2026-11-19 09:30'
        self.assertFalse(self.service.sync_notice_source_ended_fields(payload))
        self.service._patch_record_fields.assert_not_called()

    def test_changed_target_binding_is_rejected(self):
        payload, _, _ = self.source()
        with patch.object(self.store, 'resolve_notice_identity', return_value={'target_record_id': 'rec-new'}):
            with self.assertRaisesRegex(portal.PortalError, '关联.*变化'):
                self.service.sync_notice_source_ended_fields(payload)
        self.service._patch_record_fields.assert_not_called()

    def test_missing_metadata_is_visible_not_guessed_status_write(self):
        payload, _, _ = self.source()
        self.service._field_meta_by_name = {}
        self.service._load_table_fields = Mock(return_value=([], {}))
        with self.assertRaisesRegex(portal.NoticeSourcePending, '元信息缺失'):
            self.service.sync_notice_source_ended_fields(payload)
        self.service._patch_record_fields.assert_not_called()
        self.service._load_table_fields.assert_called_once()

    def test_change_end_recovers_missing_metadata_after_restart_without_resending_target(self):
        payload, remote, row = self.source(work_type='change')
        self.service._change_field_meta_by_name = {}
        self.service._load_table_fields = Mock(side_effect=OSError('fields offline'))
        self.runtime._queue_source_end_sync(payload, 'rec-target', '变更通告')
        end_fields = {'变更状态': '结束', '变更结束时间': payload['ended_at']}
        with patch.object(server, 'query_record_by_id', return_value=(True, {'fields': end_fields})), \
                patch.object(server, 'update_bitable_record_by_payload') as target_write, \
                patch.object(portal, 'send_text_to_open_ids') as sender:
            self.runtime.process_source_end_sync()
            self.service._patch_record_fields.assert_not_called()
            self.assertEqual(row['display_fields']['变更进度'], '进行中')
            pending = self.store.list_outbox_events('notice_source_finalize', status='deferred')
            self.assertEqual(len(pending), 1)
            self.assertIn('fields offline', pending[0]['last_error'])
            restarted = self.new_service()
            restarted._change_field_meta_by_name = {}
            restarted._change_records = [row]
            restarted._request_json = self.service._request_json
            restarted._patch_record_fields = self.service._patch_record_fields
            restarted._load_table_fields = Mock(return_value=([], {
                '变更进度': field('变更进度', options=['进行中', '已结束'])}))
            self.runtime.service = restarted
            self.set_due('notice_source_finalize')
            self.runtime.process_source_end_sync()
            self.runtime.process_source_end_sync()
        self.assertEqual(remote['变更进度'], '已结束')
        self.assertEqual(row['display_fields']['变更进度'], '已结束')
        restarted._patch_record_fields.assert_called_once()
        self.assertEqual(len(self.store.list_outbox_events('notice_source_finalize', status='done')), 1)
        target_write.assert_not_called()
        sender.assert_not_called()

    def test_reloaded_change_formula_is_not_written_or_falsely_marked_ended(self):
        payload, remote, row = self.source(work_type='change')
        self.service._change_field_meta_by_name = {}
        self.service._load_table_fields = Mock(return_value=([], {'变更进度': field('变更进度', formula=True)}))
        for _ in range(2):
            with self.assertRaises(portal.NoticeSourcePending):
                self.service.sync_notice_source_ended_fields(payload)
        self.service._patch_record_fields.assert_called_once()
        self.assertNotIn('变更进度', self.service._patch_record_fields.call_args.kwargs['fields'])
        self.assertEqual(row['display_fields']['变更进度'], '进行中')
        remote['变更进度'] = '已结束'
        self.assertTrue(self.service.sync_notice_source_ended_fields(payload))

    def test_formula_pending_survives_retries_restart_and_does_not_change_target_success(self):
        payload, remote, _ = self.source(formula=True)
        self.runtime._queue_source_end_sync({**payload, 'job_id': 'job-end'}, 'rec-target', '维保通告')
        event = self.store.list_outbox_events('notice_source_finalize')[0]
        key = event['payload']['_source_recovery_id']
        document = self.store.get_document('notice_source_recovery', key)
        document['created_at'] = time.time() - 1900
        self.store.put_document('notice_source_recovery', key, document)
        with patch.object(self.runtime, '_sync_confirmed_source_end', side_effect=lambda p: self.service.sync_notice_source_ended_fields(p)):
            for _ in range(10):
                self.set_due('notice_source_finalize')
                self.runtime.process_source_end_sync()
            self.assertEqual(self.store.get_document('notice_source_recovery', key)['state'], 'attention_required')
            self.assertIn('30分钟', self.service.get_job('job-end')['source_sync_warning'])
            self.assertEqual(self.service.get_job('job-end')['phase'], 'success')
            self.service._jobs.clear()
            remote['维护实施状态'] = '已结束'
            self.set_due('notice_source_finalize')
            self.runtime.process_source_end_sync()
        self.assertEqual(len(self.store.list_outbox_events('notice_source_finalize', status='done')), 1)
        self.service._patch_record_fields.assert_called_once()
        self.assertEqual(self.service.get_job('job-end')['source_sync_state'], 'done')

    def test_qt_without_action_job_still_has_durable_source_recovery(self):
        payload, _, _ = self.source()
        self.runtime._queue_source_end_sync(payload, 'rec-target', '维保通告')
        self.runtime._queue_source_end_sync(payload, 'rec-target', '维保通告')
        events = self.store.list_outbox_events('notice_source_finalize')
        self.assertEqual(len(events), 1)
        self.assertTrue(self.store.get_document('notice_source_recovery', events[0]['payload']['_source_recovery_id']))
        with patch.object(self.runtime, '_sync_confirmed_source_end', return_value=True):
            self.runtime.process_source_end_sync()
        self.assertEqual(len(self.store.list_outbox_events('notice_source_finalize', status='done')), 1)

    def test_source_network_failure_is_not_dropped_after_eight_attempts(self):
        payload, _, _ = self.source()
        self.runtime._queue_source_end_sync(payload, 'rec-target', '维保通告')
        with patch.object(self.runtime, '_sync_confirmed_source_end', side_effect=OSError('offline')):
            for _ in range(10):
                self.set_due('notice_source_finalize')
                self.runtime.process_source_end_sync()
        self.assertEqual(len(self.store.list_outbox_events('notice_source_finalize', status='failed')), 0)
        with patch.object(self.runtime, '_sync_confirmed_source_end', return_value=True):
            self.set_due('notice_source_finalize')
            self.runtime.process_source_end_sync()
        self.assertEqual(len(self.store.list_outbox_events('notice_source_finalize', status='done')), 1)

    def test_qt_source_followup_never_changes_confirmed_remote_operation_status(self):
        document = {'operation_id': 'operation-test'}
        with patch.object(self.store, 'get_notice_remote_operation', return_value={'status': 'completed'}), \
                patch.object(self.runtime, '_mark_notice_remote_operation') as mark:
            self.runtime._source_sync_result({}, document, 'pending', 'formula lag')
        mark.assert_called_once_with('operation-test', status='completed', result={
            'source_sync_state': 'pending', 'source_sync_warning': 'formula lag'})

    def test_original_delivery_document_and_outbox_commit_atomically(self):
        encode = self.store._json
        def interrupted(value):
            if isinstance(value, dict) and value.get('fingerprint'):
                raise OSError('transaction interrupted')
            return encode(value)
        with patch.object(self.store, '_json', side_effect=interrupted):
            with patch.object(portal, 'send_text_to_open_ids') as sender:
                with self.assertRaises(OSError):
                    self.service.send_action_personal_message(self.prepared)
        sender.assert_not_called()
        self.assertIsNone(self.personal_doc())
        self.assertEqual(self.store.list_outbox_events('notice_personal'), [])
        self.assertTrue(self.service.get_job('job-end')['_notice_personal_staging_pending'])

    def test_failed_initial_staging_survives_compaction_and_recovers_without_upload(self):
        with patch.object(self.store, 'enqueue_outbox_event', side_effect=OSError('staging unavailable')):
            with patch.object(portal, 'send_text_to_open_ids') as sender:
                with self.assertRaises(OSError):
                    self.service.send_action_personal_message(self.prepared)
        sender.assert_not_called()
        self.service._compact_completed_job_locked('job-end')
        self.assertTrue(self.service._impersonation_followups_pending(self.service.get_job('job-end')))
        snapshot = self.service.get_job('job-end')['_notice_personal_staging_pending']
        snapshot['next_check_at'] = 0
        self.service.mark_job('job-end', _notice_personal_staging_pending=snapshot)
        with patch.object(portal, 'send_text_to_open_ids', side_effect=self.send_ok) as sender:
            self.runtime.process_notice_personal_messages()
            self.runtime.process_notice_personal_messages()
        self.assertEqual(sender.call_count, 2)
        self.assertTrue(self.service.get_job('job-end')['message_sent'])
        self.assertFalse(self.service.get_job('job-end').get('_notice_personal_staging_pending'))
        self.assertEqual(len(self.store.list_outbox_events('notice_personal', status='done')), 1)

    def test_message_followup_does_not_block_next_notice_operation(self):
        self.deliver(lambda *a, **k: (False, 'no', [self.receipt(a[1][0], ok=False)]))
        following = self.seed_job('next-job')
        self.service.mark_job('next-job', phase='accepted', depends_on_job_id='job-end')
        with patch.object(self.runtime, '_hold_for_active_event', return_value=False):
            self.assertTrue(self.runtime._runtime_queue_job_processable('qt_action', following['job_id']))

    def test_blocked_staging_tasks_back_off_without_starving_other_recipients(self):
        self.deliver(lambda *a, **k: (False, 'no', [self.receipt(a[1][0], ok=False)]))
        self.set_due('notice_personal')
        for n in range(3):
            job_id = 'blocked-' + str(n)
            job = self.seed_job(job_id)
            job['request']['_auth_open_id'] = 'revoked'
            self.service.mark_job(job_id, request=job['request'], _notice_personal_staging_pending={
                **self.prepared, 'job_id': job_id, 'signature': 'signature', 'request': job['request']})
        self.auth.scope_allowed.side_effect = lambda session, scope: session['user']['open_id'] != 'revoked'
        with patch.object(portal, 'send_text_to_open_ids', side_effect=self.send_ok) as sender:
            self.runtime.process_notice_personal_messages()
            sender.assert_not_called()
            self.assertEqual(self.service.get_job('blocked-0')['message_delivery_state'], 'blocked')
            self.runtime.process_notice_personal_messages()
        self.assertEqual(sender.call_count, 2)
        self.assertTrue(self.service.get_job('job-end')['message_sent'])

    def test_expected_recipients_all_must_have_success_receipts(self):
        outcomes = [[], [self.receipt('unrelated')], [self.receipt('duty110'), self.receipt('duty110')],
                    [self.receipt('duty110', message_id='')]]
        for n, receipts in enumerate(outcomes):
            with self.subTest(outcome=n):
                self.prepared['job_id'] = 'bad-' + str(n)
                self.seed_job(self.prepared['job_id'])
                result, sender = self.deliver(lambda *a, **k: (True, 'ok', receipts))
                self.assertFalse(result[0])
                self.assertFalse(self.service.get_job(self.prepared['job_id'])['message_sent'])

    def test_partial_failure_retries_only_failed_recipient_with_original_uuid(self):
        def first(text, recipients, **kw):
            recipient = recipients[0]
            return recipient != 'duty110', 'rejected', [self.receipt(recipient, ok=recipient != 'duty110')]
        result, first_sender = self.deliver(first)
        self.assertFalse(result[0])
        self.assertEqual(self.service.get_job('job-end')['phase'], 'success')
        result, second_sender = self.deliver(self.send_ok)
        self.assertTrue(result[0])
        second_sender.assert_called_once()
        self.assertEqual(second_sender.call_args.args[1], ['duty110'])
        self.assertEqual(first_sender.call_args_list[0].kwargs['message_uuid'], second_sender.call_args.kwargs['message_uuid'])
        self.assertFalse(self.service.get_job('job-end')['message_delivery_warning'])

    def test_uncertain_result_not_resent_without_definite_receipt(self):
        self.deliver(lambda *a, **k: (False, 'timeout', []))
        result, sender = self.deliver(self.send_ok)
        self.assertFalse(result[0])
        sender.assert_not_called()
        self.assertEqual(self.service.get_job('job-end')['message_delivery_state'], 'verification_required')

    def test_persisted_public_receipt_recovers_after_restart_without_send(self):
        self.deliver(lambda *a, **k: (False, 'timeout', []))
        for recipient, result in self.personal_doc()['results'].items():
            key = PublicFeishuSender.route_key(recipient, 'open_id', result['message_uuid'])
            self.store.put_document('feishu_public_delivery', key, {'status': 'sent', 'receipt': self.receipt(recipient)})
        self.service = self.new_service()
        result, sender = self.deliver(self.send_ok)
        self.assertTrue(result[0])
        sender.assert_not_called()
        self.assertTrue(self.service.get_job('job-end')['message_sent'])

    def test_definitive_not_sent_receipt_allows_same_uuid_retry(self):
        self.deliver(lambda *a, **k: (False, 'timeout', []))
        originals = copy.deepcopy(self.personal_doc()['results'])
        for recipient, result in originals.items():
            self.store.put_document('feishu_public_delivery', PublicFeishuSender.route_key(recipient, 'open_id', result['message_uuid']), {'status': 'not_sent'})
        result, sender = self.deliver(self.send_ok)
        self.assertTrue(result[0])
        self.assertEqual([call.kwargs['message_uuid'] for call in sender.call_args_list],
                         [originals[r]['message_uuid'] for r in self.prepared['recipients']])

    def test_storage_failure_before_posting_never_sends(self):
        put = self.store.put_document
        def fail_before_post(namespace, key, document):
            if namespace == 'notice_personal_delivery':
                raise OSError('disk unavailable')
            return put(namespace, key, document)
        with patch.object(self.store, 'put_document', side_effect=fail_before_post), patch.object(portal, 'send_text_to_open_ids') as sender:
            with self.assertRaises(OSError):
                self.service.send_action_personal_message(self.prepared)
        sender.assert_not_called()
        self.assertTrue(self.personal_doc(), 'atomic original snapshot is still recoverable')

    def test_crash_after_send_before_result_store_reuses_public_receipt(self):
        put = self.store.put_document
        def fail_after_send(namespace, key, document):
            if namespace == 'notice_personal_delivery' and any(v.get('ok') for v in document['results'].values()):
                raise OSError('result store interrupted')
            return put(namespace, key, document)
        def sent(text, recipients, **kw):
            recipient = recipients[0]
            put('feishu_public_delivery', PublicFeishuSender.route_key(recipient, 'open_id', kw['message_uuid']),
                {'status': 'sent', 'receipt': self.receipt(recipient)})
            return self.send_ok(text, recipients, **kw)
        with patch.object(self.store, 'put_document', side_effect=fail_after_send):
            with self.assertRaises(OSError):
                self.deliver(sent)
        self.service = self.new_service()
        result, sender = self.deliver(self.send_ok)
        self.assertTrue(result[0])
        self.assertEqual([call.args[1] for call in sender.call_args_list], [['operator']])

    def test_snapshot_fingerprint_cannot_be_replaced(self):
        self.deliver(self.send_ok)
        self.prepared['text'] = 'another notice'
        with patch.object(portal, 'send_text_to_open_ids') as sender:
            with self.assertRaisesRegex(portal.PortalError, '已变化'):
                self.service.send_action_personal_message(self.prepared)
        sender.assert_not_called()

    def test_legacy_partial_receipts_preserve_success_and_uncertainty(self):
        self.service.mark_job('job-end', recipient_results=[
            self.receipt('operator', message_uuid='original-success-uuid'),
            self.receipt('duty110', ok=False, failure_kind='uncertain', message_uuid='original-unknown-uuid')])
        result, sender = self.deliver(self.send_ok)
        self.assertFalse(result[0])
        sender.assert_not_called()
        self.assertEqual(self.personal_doc()['results']['duty110']['message_uuid'], 'original-unknown-uuid')

    def test_receipts_without_message_ids_cannot_seed_success(self):
        self.service.mark_job('job-end', recipient_results=[self.receipt('duty110', message_id='')])
        result, sender = self.deliver(self.send_ok)
        self.assertFalse(result[0])
        self.assertEqual([call.args[1] for call in sender.call_args_list], [['operator']])

    def test_crash_between_complete_receipts_and_job_summary_is_local_only_recovery(self):
        with patch.object(self.service, 'mark_job', side_effect=OSError('summary interrupted')):
            with self.assertRaises(OSError):
                self.deliver(self.send_ok)
        self.assertEqual(self.personal_doc()['state'], 'sent')
        self.assertFalse(self.service.get_job('job-end').get('message_sent'))
        self.service._jobs.clear()
        with patch('time.time', return_value=time.time() + 65), patch.object(portal, 'send_text_to_open_ids') as sender:
            self.runtime.process_notice_personal_messages()
        sender.assert_not_called()
        self.assertTrue(self.service.get_job('job-end')['message_sent'])
        self.assertEqual(len(self.store.list_outbox_events('notice_personal', status='done')), 1)

    def test_legacy_source_outbox_retains_original_snapshot_across_deferred_retry(self):
        payload, _, _ = self.source()
        source_patch = self.service._patch_record_fields
        payload.update(notice_type='维保通告', plan_month='10月', maintenance_cycle='每月')
        event_id = self.store.enqueue_outbox_event('notice_source_finalize', payload)
        with patch.object(self.runtime, '_sync_confirmed_source_end', side_effect=portal.NoticeSourcePending('formula pending')):
            self.runtime.process_source_end_sync()
        document = self.store.list_documents('notice_source_recovery')[0]['payload']
        self.assertEqual(document['outbox_id'], event_id)
        self.assertEqual(document['payload']['_source_period']['plan_month'], '10月')
        self.assertEqual(len(self.store.list_outbox_events('notice_source_finalize', status='deferred')), 1)
        self.service = self.new_service()
        self.runtime.service = self.service
        with patch('time.time', return_value=time.time() + 65), \
                patch.object(self.runtime, '_sync_confirmed_source_end', return_value=True) as sync:
            self.runtime.process_source_end_sync()
        self.assertEqual(sync.call_args.args[0]['_source_period']['plan_month'], '10月')
        done = self.store.list_outbox_events('notice_source_finalize', status='done')
        self.assertEqual([item['id'] for item in done], [event_id])
        self.assertEqual(len(self.store.list_documents('notice_source_recovery')), 1)
        source_patch.assert_not_called()

    def test_recovery_removes_only_its_old_warning_after_compaction(self):
        self.deliver(lambda *a, **k: (False, 'no', [self.receipt(a[1][0], ok=False)]))
        self.service.set_notice_followup('job-end', 'source_sync', 'pending', 'source formula pending')
        with patch.object(self.service, '_build_action_frontend_patch', return_value={}), \
                patch.object(self.service, '_record_successful_action'):
            self.service.mark_action_upload_result('job-end', success=True, record_id='rec-target', message='target uploaded')
        self.deliver(self.send_ok)
        job = self.service.get_job('job-end')
        self.assertEqual(job['phase'], 'success')
        self.assertEqual(job['message_warning'], 'source formula pending')
        self.assertNotIn('个人消息', job['upload_message'])

    def test_personal_worker_rechecks_permissions_without_repeating_upload(self):
        self.deliver(lambda *a, **k: (False, 'no', [self.receipt(a[1][0], ok=False)]))
        self.set_due('notice_personal')
        self.auth.scope_allowed.return_value = False
        with patch.object(portal, 'send_text_to_open_ids') as sender:
            self.runtime.process_notice_personal_messages()
        sender.assert_not_called()
        self.assertEqual(self.service.get_job('job-end')['phase'], 'success')
        self.assertEqual(self.service.get_job('job-end')['message_delivery_state'], 'blocked')

    def test_background_recovery_keeps_original_task_and_receipts(self):
        self.deliver(lambda *a, **k: (False, 'no', [self.receipt(a[1][0], ok=False)]))
        self.service._jobs.clear()
        self.set_due('notice_personal')
        with patch.object(portal, 'send_text_to_open_ids', side_effect=self.send_ok) as sender:
            self.runtime.process_notice_personal_messages()
        self.assertEqual(sender.call_count, 2)
        self.assertEqual(self.service.get_job('job-end')['phase'], 'success')
        self.assertTrue(self.service.get_job('job-end')['message_sent'])
        self.assertEqual(len(self.store.list_outbox_events('notice_personal', status='done')), 1)

    def test_pending_outbox_pins_completed_job_until_followups_done(self):
        self.deliver(lambda *a, **k: (False, 'timeout', []))
        self.assertTrue(self.service._impersonation_followups_pending(self.service.get_job('job-end')))

    def test_empty_recipient_list_is_not_all_delivered(self):
        self.prepared['recipients'] = []
        result, sender = self.deliver(self.send_ok)
        self.assertFalse(result[0])
        sender.assert_not_called()

    def test_missing_original_job_never_creates_another_notice_or_sends(self):
        self.prepared['job_id'] = 'missing'
        result, sender = self.deliver(self.send_ok)
        self.assertFalse(result[0])
        sender.assert_not_called()

    def period_source(self):
        payload, remote, row = self.source()
        remote.update({'计划维护月份': '10月', '维护周期': '每月'})
        row['display_fields'].update(remote)
        payload['_source_period'] = self.service.notice_source_period_snapshot(payload, use_local=True)
        return payload, remote, row

    def test_changed_month_with_unchanged_dates_never_ends_next_period(self):
        payload, remote, _ = self.period_source()
        remote['计划维护月份'] = '11月'
        with self.assertRaisesRegex(portal.NoticeSourceSuperseded, '计划月份'):
            self.service.sync_notice_source_ended_fields(payload)
        self.service._patch_record_fields.assert_not_called()

    def test_changed_cycle_does_not_reuse_prior_monthly_end(self):
        payload, remote, _ = self.period_source()
        remote['维护周期'] = '每季'
        with self.assertRaisesRegex(portal.NoticeSourceSuperseded, '维护周期'):
            self.service.sync_notice_source_ended_fields(payload)
        self.service._patch_record_fields.assert_not_called()

    def test_missing_original_period_is_pending_not_guessed_from_end_date(self):
        payload, remote, _ = self.period_source()
        payload.pop('_source_period')
        with self.assertRaisesRegex(portal.NoticeSourcePending, '原计划月份'):
            self.service.sync_notice_source_ended_fields(payload)
        self.service._patch_record_fields.assert_not_called()

    def test_late_end_in_next_month_keeps_original_explicit_period(self):
        payload, _, _ = self.period_source()
        payload['ended_at'] = '2026-11-01 18:30'
        self.assertTrue(self.service.sync_notice_source_ended_fields(payload))
        self.service._patch_record_fields.assert_called_once()

    def test_equivalent_cycle_labels_are_not_false_rollover(self):
        payload, remote, _ = self.period_source()
        remote['维护周期'] = '月度'
        self.assertTrue(self.service.sync_notice_source_ended_fields(payload))

    def test_staged_original_month_survives_queue_failure_restart_and_reenqueue_only(self):
        payload, remote, _ = self.period_source()
        payload.update(job_id='job-end', operation_id='end-operation')
        with patch.object(self.store, 'enqueue_outbox_event', side_effect=OSError('queue rollback')):
            warning = self.runtime._queue_source_end_sync(payload, 'rec-target', '维保通告')
        self.assertIn('原源表同步快照已保存', warning)
        docs = self.store.list_documents('notice_source_recovery')
        self.assertEqual(len(docs), 1)
        original = docs[0]['payload']
        self.assertEqual(original['state'], 'staging_pending')
        self.assertEqual(original['payload']['_source_period']['plan_month'], '10月')
        self.assertEqual(self.service.get_job('job-end')['phase'], 'success')
        self.assertEqual(self.service.get_job('job-end')['source_sync_state'], 'staging_pending')
        remote['计划维护月份'] = '11月'
        original['next_check_at'] = 0
        self.store.put_document('notice_source_recovery', docs[0]['key'], original)
        self.service = self.new_service()
        self.runtime.service = self.service
        with patch.object(server, 'query_record_by_id', side_effect=AssertionError('reenqueue must not read cloud')), \
                patch.object(self.runtime, '_execute_backend_prepared_upload', side_effect=AssertionError('no second target write')):
            self.runtime._resume_source_end_staging()
            self.runtime._resume_source_end_staging()
        events = self.store.list_outbox_events('notice_source_finalize')
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]['payload']['_source_period']['plan_month'], '10月')
        self.assertEqual(self.service.get_job('job-end')['phase'], 'success')

    def test_qt_without_action_job_keeps_source_intent_if_outbox_insert_fails(self):
        payload, _, _ = self.period_source()
        payload['operation_id'] = 'qt-end'
        with patch.object(self.store, 'enqueue_outbox_event', side_effect=OSError('insert interrupted')):
            self.runtime._queue_source_end_sync(payload, 'rec-target', '维保通告')
        doc = self.store.list_documents('notice_source_recovery')[0]
        self.assertFalse(doc['payload']['job_id'])
        doc['payload']['next_check_at'] = 0
        self.store.put_document('notice_source_recovery', doc['key'], doc['payload'])
        self.runtime._resume_source_end_staging()
        event = self.store.list_outbox_events('notice_source_finalize')[0]
        self.assertEqual(event['payload']['_source_period']['plan_month'], '10月')
        self.assertEqual(event['payload']['source_record_id'], 'rec-source')

    def test_prewrite_source_intent_waits_for_confirmed_target_operation(self):
        payload, _, _ = self.period_source()
        payload['operation_id'] = 'qt-awaiting'
        recovery_id, doc = self.runtime._stage_source_end_sync(payload, 'rec-target', '维保通告', awaiting_target=True)
        with patch.object(self.store, 'get_notice_remote_operation', return_value={'status': 'executing'}):
            self.runtime._resume_source_end_staging()
        self.assertEqual(self.store.list_outbox_events('notice_source_finalize'), [])
        doc = self.store.get_document('notice_source_recovery', recovery_id)
        doc['next_check_at'] = 0
        self.store.put_document('notice_source_recovery', recovery_id, doc)
        with patch.object(self.store, 'get_notice_remote_operation', return_value={'status': 'completed'}), \
                patch.object(self.runtime, '_mark_notice_remote_operation'):
            self.runtime._resume_source_end_staging()
        self.assertEqual(len(self.store.list_outbox_events('notice_source_finalize')), 1)

    def test_unqueued_source_intent_pins_original_job_without_an_outbox_row(self):
        payload, _, _ = self.period_source()
        payload.update(job_id='job-end', operation_id='source-pin')
        self.runtime._stage_source_end_sync(payload, 'rec-target', '维保通告', awaiting_target=True)
        self.assertEqual(self.store.list_outbox_events('notice_source_finalize'), [])
        self.service._compact_completed_job_locked('job-end')
        self.assertTrue(self.service._impersonation_followups_pending(self.service.get_job('job-end')))

    def test_qt_lost_end_receipt_can_only_read_verify_and_reenqueue_original_intent(self):
        payload, _, _ = self.period_source()
        payload['operation_id'] = 'qt-lost-receipt'
        key, doc = self.runtime._stage_source_end_sync(payload, 'rec-target', '维保通告', awaiting_target=True)
        doc.update(created_at=time.time() - 181, next_check_at=0)
        self.store.put_document('notice_source_recovery', key, doc)
        config = server.get_field_config('维保通告')
        with patch.object(self.store, 'get_notice_remote_operation', return_value={'status': 'executing'}), \
                patch.object(server, 'query_record_by_id', return_value=(True, {'record_id': 'rec-target', 'fields': {
                    config['status']: '结束', config['actual_end']: '2026-10-19 18:30'}})) as query, \
                patch.object(self.runtime, '_mark_notice_remote_operation'):
            self.runtime._resume_source_end_staging()
        query.assert_called_once_with('rec-target', '维保通告')
        self.service._patch_record_fields.assert_not_called()
        event = self.store.list_outbox_events('notice_source_finalize')[0]
        self.assertEqual(event['payload']['_source_period']['plan_month'], '10月')

    def test_real_qt_end_entry_persists_period_before_target_write(self):
        from upload_event_module.services.handlers import get_notice_handler
        _, _, row = self.period_source()
        config = server.get_field_config('维保通告')
        remote = {config['status']: '开始', config['actual_start']: '2026-10-19 09:30'}
        request = {'action_type': 'end', 'operation_id': 'qt-real-end', 'response_time': '2026-10-19 18:30',
                   'robot_group_choice': 'skip', 'data_dict': {
                       'record_id': 'rec-target', 'target_record_id': 'rec-target', 'source_record_id': 'rec-source',
                       'notice_type': '维保通告', 'work_type': 'maintenance', 'buildings': ['A楼'],
                       'text': '【维保通告】状态：结束\n【名称】A楼测试\n【时间】2026-10-19 09:30~2026-10-19 18:30\n【进度】完成'}}
        def update(record_id, notice_type, notice):
            intent = self.store.list_documents('notice_source_recovery')[0]['payload']
            self.assertEqual(intent['state'], 'awaiting_target')
            self.assertEqual(intent['payload']['_source_period']['plan_month'], '10月')
            fields = get_notice_handler(notice_type).build_update_fields(notice)
            remote.update(fields)
            notice._clipflow_written_fields = fields
            return True, record_id
        with patch.object(self.runtime, '_work_order_end_error', return_value=''), \
                patch.object(self.runtime, '_has_cumulative_site_photo_for_notice', return_value=True), \
                patch.object(self.runtime, '_create_backend_undo_checkpoint', return_value=''), \
                patch.object(server, 'query_record_by_id', side_effect=lambda *args: (True, {'record_id': 'rec-target', 'fields': copy.deepcopy(remote)})), \
                patch.object(server, 'update_bitable_record_by_payload', side_effect=update) as write:
            result = self.runtime.execute_local_notice_upload(request)
        self.assertTrue(result['ok'], result)
        write.assert_called_once()
        self.assertEqual(len(self.store.list_documents('notice_source_recovery')), 1)
        self.assertEqual(len(self.store.list_outbox_events('notice_source_finalize')), 1)

    def test_snapshot_staging_failure_prevents_initial_target_write(self):
        payload, _, _ = self.period_source()
        payload.update(action='end', notice_type='维保通告')
        with patch.object(self.runtime, '_acquire_event_operation_lock', return_value=('', '', '')), \
                patch.object(self.runtime, '_notice_remote_operation_request', return_value={}), \
                patch.object(self.runtime, '_begin_notice_remote_operation', return_value={'created': True, 'status': 'accepted'}), \
                patch.object(self.runtime, '_stage_source_end_sync', side_effect=OSError('snapshot unavailable')), \
                patch.object(self.runtime, '_execute_backend_prepared_upload') as upload:
            with self.assertRaisesRegex(OSError, 'snapshot unavailable'):
                self.runtime._execute_durable_prepared_upload(payload, operation_id='web-end', operation_type='end')
        upload.assert_not_called()

    def test_period_rollover_finishes_source_task_as_superseded_without_patch(self):
        payload, remote, _ = self.period_source()
        self.runtime._queue_source_end_sync(payload, 'rec-target', '维保通告')
        remote['计划维护月份'] = '11月'
        with patch.object(self.runtime, '_sync_confirmed_source_end', side_effect=lambda p: self.service.sync_notice_source_ended_fields({**p, 'ended_at': payload['ended_at']})):
            self.runtime.process_source_end_sync()
        self.service._patch_record_fields.assert_not_called()
        doc = self.store.list_documents('notice_source_recovery')[0]['payload']
        self.assertEqual(doc['state'], 'superseded')
        self.assertIn('计划月份已变化', doc['reason'])
        self.assertEqual(len(self.store.list_outbox_events('notice_source_finalize', status='done')), 1)

    def test_robot_scheduler_does_not_wait_for_personal_recovery(self):
        executor = ManualExecutor()
        self.runtime.action_executor = executor
        self.runtime.action_worker_stop = False
        self.runtime.notice_personal_future = None
        with patch.object(self.runtime, 'process_notice_personal_messages') as process, \
                patch.object(self.store, 'lease_outbox_events', return_value=[]) as lease:
            self.runtime.process_notice_robot_messages()
            lease.assert_called_once()
            self.assertEqual(lease.call_args.args[0], 'notice_robot')
            process.assert_not_called()
            for _ in range(10):
                self.assertFalse(self.runtime.dispatch_notice_personal_messages())
            self.assertEqual(len(executor.calls), 1)
            executor.run()
            process.assert_called_once_with(limit=1, recipient_limit=1)

    def test_dispatch_reuses_executor_and_never_starts_one_when_unavailable(self):
        self.runtime.action_executor = None
        self.runtime.action_worker_stop = False
        self.runtime.notice_personal_future = None
        with patch.object(server, 'ThreadPoolExecutor', side_effect=AssertionError('no new executor')):
            self.assertFalse(self.runtime.dispatch_notice_personal_messages())

    def test_limited_personal_retry_rotates_past_a_permanently_failed_recipient(self):
        self.deliver(lambda *a, **k: (False, 'no', [self.receipt(a[1][0], ok=False)]))
        original = copy.deepcopy(self.personal_doc()['results'])
        def response(text, recipients, **kw):
            return recipients[0] != 'duty110', 'result', [self.receipt(recipients[0], ok=recipients[0] != 'duty110')]
        with patch.object(portal, 'send_text_to_open_ids', side_effect=response) as sender:
            self.service.send_action_personal_message(self.prepared, recipient_limit=1)
            self.service.send_action_personal_message(self.prepared, recipient_limit=1)
        self.assertEqual([call.args[1] for call in sender.call_args_list], [['duty110'], ['operator']])
        self.assertEqual([call.kwargs['message_uuid'] for call in sender.call_args_list],
                         [original[person]['message_uuid'] for person in ('duty110', 'operator')])
        self.assertTrue(self.personal_doc()['results']['operator']['ok'])

    def test_many_future_personal_retries_do_not_hide_a_fresh_due_job(self):
        original = copy.deepcopy(self.prepared)
        for n in range(14):
            self.prepared['job_id'] = 'old-personal-' + str(n)
            self.seed_job(self.prepared['job_id'])
            self.deliver(lambda *a, **k: (False, 'timeout', []))
        self.prepared = original
        self.deliver(lambda *a, **k: (False, 'not sent', [self.receipt(a[1][0], ok=False)]))
        document = self.personal_doc()
        document['next_check_at'] = 0
        self.store.put_document('notice_personal_delivery', 'job-end', document)
        now = time.time()
        with patch.object(server.time, 'time', return_value=now + 65), \
                patch.object(portal, 'send_text_to_open_ids', side_effect=self.send_ok) as sender:
            self.runtime.process_notice_personal_messages(limit=1, recipient_limit=1)
        sender.assert_called_once()
        self.assertEqual(sender.call_args.args[1], ['duty110'])

    def test_many_future_source_retries_do_not_hide_a_fresh_due_job(self):
        for n in range(14):
            self.runtime._queue_source_end_sync({'work_type': 'maintenance', 'source_record_id': 'old-source-' + str(n),
                'operation_id': 'old-end-' + str(n)}, 'old-target-' + str(n), '维保通告')
        with patch.object(self.runtime, '_sync_confirmed_source_end', side_effect=portal.NoticeSourcePending('formula lag')):
            for _ in range(5):
                self.runtime.process_source_end_sync()
        now = time.time()
        for row in self.store.list_documents('notice_source_recovery'):
            row['payload']['next_check_at'] = now + 300
            self.store.put_document('notice_source_recovery', row['key'], row['payload'])
        self.runtime._queue_source_end_sync({'work_type': 'maintenance', 'source_record_id': 'fresh-source',
            'operation_id': 'fresh-end'}, 'fresh-target', '维保通告')
        with patch.object(server.time, 'time', return_value=now + 65), \
                patch.object(self.runtime, '_sync_confirmed_source_end', return_value=True) as sync:
            self.runtime.process_source_end_sync()
        sync.assert_called_once()
        self.assertEqual(sync.call_args.args[0]['source_record_id'], 'fresh-source')

    def test_busy_delivery_lock_defers_without_losing_retry_or_starving_others(self):
        self.deliver(lambda *a, **k: (False, 'no', [self.receipt(a[1][0], ok=False)]))
        self.set_due('notice_personal')
        self.store.acquire_notice_operation_lock('notice_personal:job-end', owner='other-worker', action='delivery', ttl_seconds=300)
        with patch.object(portal, 'send_text_to_open_ids') as sender:
            self.runtime.process_notice_personal_messages(limit=1, recipient_limit=1)
        sender.assert_not_called()
        self.assertGreater(self.service.get_job('job-end')['_notice_personal_retry_after'], time.time())
        self.assertEqual(len(self.store.list_outbox_events('notice_personal', status='deferred')), 1)
        self.store.release_notice_operation_lock('notice_personal:job-end', 'other-worker')
        self.service.mark_job('job-end', _notice_personal_retry_after=0)
        with patch.object(portal, 'send_text_to_open_ids', side_effect=self.send_ok) as sender:
            self.runtime.process_notice_personal_messages(limit=1, recipient_limit=1)
        sender.assert_called_once()

    def test_explicit_period_formula_lag_retains_actual_end_and_only_reads_on_retry(self):
        payload, remote, row = self.period_source()
        self.service._field_meta_by_name['维护实施状态'] = field('维护实施状态', formula=True)
        for _ in range(2):
            with self.assertRaises(portal.NoticeSourcePending):
                self.service.sync_notice_source_ended_fields(payload)
        self.service._patch_record_fields.assert_called_once()
        self.assertEqual(row['display_fields']['实际结束时间'], '2026-10-19 18:30')

    def test_explicit_period_still_checks_expired_target_lock_before_source_patch(self):
        payload, _, _ = self.period_source()
        self.runtime._queue_source_end_sync(payload, 'rec-target', '维保通告')
        config = server.get_field_config('维保通告')
        with patch.object(server, 'query_record_by_id', return_value=(True, {'record_id': 'rec-target', 'fields': {
                config['status']: '结束', config['actual_end']: payload['ended_at']}})), \
                patch.object(self.store, 'renew_notice_operation_lock', return_value=False):
            self.runtime.process_source_end_sync()
        self.service._patch_record_fields.assert_not_called()
        event = self.store.list_outbox_events('notice_source_finalize', status='deferred')[0]
        self.assertIn('操作锁已过期', event['last_error'])


class ChangeEndConfirmationTests(unittest.TestCase):
    def setUp(self):
        from upload_event_module.services import feishu_service
        from upload_event_module.services.handlers.change_notice import ChangeNoticeHandler
        from upload_event_module.services.handlers.base import NoticePayload
        self.api = feishu_service
        self.handler = ChangeNoticeHandler()
        self.payload = NoticePayload(
            text='【变更通告】状态：结束\n【名称】110站UPS切换测试\n'
                 '【时间】2026-10-23 09:00~2026-10-23 18:00\n【进度】工作完成',
            response_time='2026-10-23 18:00',
        )
        self.enterContext(patch('requests.sessions.Session.request', side_effect=AssertionError('network forbidden')))
        self.enterContext(patch.object(self.api, 'check_token_status', return_value=True))
        self.enterContext(patch.object(self.api, '_resolve_handler', return_value=(self.handler, 'test-table', '')))
        self.enterContext(patch.object(self.api, '_filter_missing_optional_fields', side_effect=lambda n, f: f))
        self.enterContext(patch.object(self.api, '_info_logging_enabled', return_value=False))
        self.enterContext(patch.object(self.api, '_log_record_action'))
        self.write = self.enterContext(patch.object(self.api, '_execute_bitable_write', return_value=SimpleNamespace(
            success=lambda: True, data=SimpleNamespace(record=SimpleNamespace(record_id='rec-target')))))
        self.sender = self.enterContext(patch.object(self.api, '_send_robot_message'))
        self.query = self.enterContext(patch.object(self.api, 'query_record_by_id', return_value=(True, {
            'record_id': 'rec-target', 'fields': {'变更状态': '开始'}})))

    def test_missing_ali_screenshot_blocks_end_even_with_site_photos_and_cached_tokens(self):
        self.payload.extra_file_tokens = ['site-photo']
        self.payload.existing_ali_confirmation_file_tokens = ['stale-other-target']
        for images in (None, [], [{'name': 'not-uploaded.png'}], [{'file_token': ''}]):
            with self.subTest(images=images):
                self.query.return_value = (True, {'record_id': 'rec-target', 'fields': {
                    '变更状态': '开始', '阿里确认截图': images, '现场图片': [{'file_token': 'site-photo'}]}})
                ok, error = self.api.update_bitable_record_by_payload('rec-target', '变更通告', self.payload)
                self.assertFalse(ok)
                self.assertIn('阿里确认截图', error)
        self.write.assert_not_called()
        self.sender.assert_not_called()

    def test_existing_target_screenshot_allows_end_without_h_confirmation(self):
        self.query.return_value = (True, {'record_id': 'rec-target', 'fields': {
            '变更状态': '开始', '阿里确认截图': [{'file_token': 'ali-old'}], 'H楼确认': False}})
        self.assertTrue(self.api.update_bitable_record_by_payload('rec-target', '变更通告', self.payload)[0])
        self.query.assert_called_once_with('rec-target', '变更通告')
        self.write.assert_called_once()
        self.sender.assert_called_once()
        self.assertEqual(self.payload._clipflow_written_fields['变更状态'], '结束')
        self.assertNotIn('阿里确认截图', self.payload._clipflow_written_fields)

    def test_new_ali_image_is_preserved_and_merged_once_on_end(self):
        self.payload.ali_confirmation_file_tokens = ['ali-new', 'ali-new']
        self.payload.existing_ali_confirmation_file_tokens = ['ali-old']
        self.assertTrue(self.api.update_bitable_record_by_payload('rec-target', '变更通告', self.payload)[0])
        fields = self.payload._clipflow_written_fields
        self.assertEqual(fields['阿里确认截图'], [{'file_token': 'ali-old'}, {'file_token': 'ali-new'}])
        self.assertFalse(fields['H楼确认'])
        self.assertEqual(fields['变更状态'], '结束')
        self.query.assert_not_called()

    def test_unreadable_or_wrong_target_is_not_reported_as_success(self):
        for response in ((False, 'offline'), (True, {}), (True, {'fields': {}}), (True, {
                'record_id': 'rec-other', 'fields': {'阿里确认截图': [{'file_token': 'other'}]}})):
            with self.subTest(response=response):
                self.query.return_value = response
                ok, error = self.api.update_bitable_record_by_payload('rec-target', '变更通告', self.payload)
                self.assertFalse(ok)
                self.assertIn('未发送结束', error)
        self.write.assert_not_called()
        self.sender.assert_not_called()

    def test_network_exception_never_writes_end_or_sends_notification(self):
        self.query.side_effect = OSError('offline')
        with self.assertRaises(OSError):
            self.api.update_bitable_record_by_payload('rec-target', '变更通告', self.payload)
        self.write.assert_not_called()
        self.sender.assert_not_called()

    def test_batch_end_and_create_cannot_bypass_screenshot_requirement(self):
        with patch.object(self.api, '_build_client') as client:
            results = self.api.batch_update_bitable_records_by_payload('变更通告', [('rec-target', self.payload)])
            self.assertFalse(results[0][0])
            self.assertIn('阿里确认截图', results[0][1])
            self.assertFalse(self.api.create_bitable_record_by_payload('变更通告', self.payload)[0])
            self.assertFalse(self.api.batch_create_bitable_records_by_payload('变更通告', [self.payload])[0][0])
        client.assert_not_called()
        self.write.assert_not_called()
        self.sender.assert_not_called()

    def test_non_end_change_and_other_notice_types_are_unchanged(self):
        for notice, status in (('变更通告', '开始'), ('变更通告', '更新'), ('检修通告', '结束'), ('维保通告', '结束')):
            with self.subTest(notice=notice, status=status):
                self.assertEqual(self.api._change_end_confirmation_error('rec-target', notice, {'变更状态': status}), '')
        self.query.assert_not_called()

    def test_backend_end_verifies_new_screenshot_before_writing_end(self):
        fields = {'变更状态': '开始', '阿里确认截图': [{'file_token': 'ali-old'}], 'H楼确认': True}
        prepared = {'action': 'end', 'work_type': 'change', 'notice_type': '变更通告',
                    'target_record_id': 'rec-target', 'text': self.payload.text,
                    'ali_confirmation_images': [{'file_token': 'ali-new'}]}
        runtime = type('IsolatedChangeEndRuntime', (server.PortalRuntime,), {
            '_has_cumulative_site_photo_for_notice': Mock(return_value=True),
            '_create_backend_undo_checkpoint': Mock(return_value=''),
            '_stage_notice_robot_delivery': Mock(), '_check_current_notice_actor': Mock(),
            '_mark_local_notice_images_target_written': Mock(),
        })
        self.query.side_effect = lambda *a: (True, {'record_id': 'rec-target', 'fields': copy.deepcopy(fields)})
        with patch.object(server, 'external_real_write_guard', return_value={'mock_external': False, 'real_write_allowed': True}), \
                patch.object(server, 'query_record_by_id', side_effect=self.query.side_effect), \
                patch.object(server, 'update_bitable_record_fields', side_effect=lambda rid, typ, patch: (fields.update(patch) or (True, rid))) as image_write, \
                patch.object(server, 'update_bitable_record_by_payload', side_effect=self.api.update_bitable_record_by_payload):
            ok, _, rid = runtime._execute_backend_prepared_upload(prepared)
        self.assertTrue(ok)
        self.assertEqual(rid, 'rec-target')
        self.assertEqual(fields['阿里确认截图'], [{'file_token': 'ali-old'}, {'file_token': 'ali-new'}])
        image_write.assert_called_once()
        self.write.assert_called_once()

    def test_backend_missing_or_failed_image_blocks_target_end(self):
        runtime = type('IsolatedMissingChangeImageRuntime', (server.PortalRuntime,), {
            '_has_cumulative_site_photo_for_notice': Mock(return_value=True),
            '_create_backend_undo_checkpoint': Mock(return_value=''),
        })
        prepared = {'action': 'end', 'work_type': 'change', 'notice_type': '变更通告',
                    'target_record_id': 'rec-target', 'text': self.payload.text}
        with patch.object(server, 'external_real_write_guard', return_value={'mock_external': False, 'real_write_allowed': True}), \
                patch.object(server, 'query_record_by_id', return_value=(True, {'fields': {'变更状态': '开始'}})), \
                patch.object(runtime, '_upload_extra_images_for_notice', return_value=(True, '', [], [])), \
                patch.object(runtime, '_upload_change_confirmation_images', return_value=(False, '截图上传失败', [], [])), \
                patch.object(server, 'update_bitable_record_by_payload') as target_write:
            self.assertIn('阿里确认截图', runtime._execute_backend_prepared_upload(prepared)[1])
            prepared['ali_confirmation_images'] = [{'upload_id': 'failed-image'}]
            self.assertFalse(runtime._execute_backend_prepared_upload(prepared)[0])
        target_write.assert_not_called()

    def test_backend_unconfirmed_screenshot_write_does_not_end_notice(self):
        runtime = type('IsolatedUnconfirmedChangeImageRuntime', (server.PortalRuntime,), {
            '_has_cumulative_site_photo_for_notice': Mock(return_value=True),
            '_create_backend_undo_checkpoint': Mock(return_value=''),
        })
        prepared = {'action': 'end', 'work_type': 'change', 'notice_type': '变更通告',
                    'target_record_id': 'rec-target', 'text': self.payload.text,
                    'ali_confirmation_images': [{'file_token': 'new-image'}]}
        with patch.object(server, 'external_real_write_guard', return_value={'mock_external': False, 'real_write_allowed': True}), \
                patch.object(server, 'query_record_by_id', return_value=(True, {'fields': {'变更状态': '开始'}})), \
                patch.object(runtime, '_upload_extra_images_for_notice', return_value=(True, '', [], [])), \
                patch.object(runtime, '_repair_pending_change_confirmation_remote_fields', side_effect=RuntimeError('截图尚未确认')), \
                patch.object(server, 'update_bitable_record_by_payload') as target_write:
            ok, error, _ = runtime._execute_backend_prepared_upload(prepared)
        self.assertFalse(ok)
        self.assertIn('尚未确认', error)
        target_write.assert_not_called()


if __name__ == '__main__':
    unittest.main()
