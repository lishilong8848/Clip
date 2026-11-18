"""Isolated delivery-before-archive checks with durable queues and no network."""
import copy
import threading
import unittest
from unittest.mock import Mock, patch

from bin import test_notice_delivery_source_recovery as fixtures
from lan_bitable_template_portal.portal_service import STATE_NS_ACTION_JOB
from lan_bitable_template_portal.workbench_lite import _detail_form


class NoticeMessageFirstTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.NoticeDeliverySourceRecoveryTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.addCleanup(self.fixture.tearDown)
        self.service, self.store, self.runtime = self.fixture.service, self.fixture.store, self.fixture.runtime
        self.enterContext(patch('lan_bitable_template_portal.server.external_real_write_guard',
            return_value={'mock_external': False, 'real_write_allowed': True}))
        self.fixture.auth.account_profile.return_value = {'modules': ['adjust']}
        self.runtime.enqueue_message_job = Mock()
        self.runtime.enqueue_action_job = Mock()
        self.runtime._hold_for_active_event = Mock(return_value=False)
        self.runtime.action_queue_event = threading.Event()
        self.prepared = {**self.fixture.prepared, 'work_type': 'adjust', 'notice_type': '设备调整',
                         'action': 'update', 'status': '更新', 'scope': '110', 'building': '110站',
                         'target_record_id': 'rec-target', 'active_item_id': 'active-fixture'}
        job = self.service.get_job('job-end')
        job.update(phase='uploading', prepared=copy.deepcopy(self.prepared), remote_written=False,
                   request={**job['request'], '_web_action_request': True, 'work_type': 'adjust', 'action': 'update'})
        self.service._jobs['job-end'] = job
        self.store.put_document(STATE_NS_ACTION_JOB, 'job-end', job)
        self.operation = self.store.begin_notice_remote_operation(
            operation_id='notice_action:job-end', operation_type='update',
            target_record_id='rec-target', request={'action': 'update', 'dingtalk_robot_ids': []})

    def stage(self):
        with patch('lan_bitable_template_portal.dingtalk_notices.DingTalkNotices.snapshot', return_value=[]), patch(
                'lan_bitable_template_portal.server.query_record_by_id', return_value=(True, {'fields': {}})):
            self.runtime._stage_message_first_notice('job-end', self.prepared, self.operation)

    def test_finished_or_changed_target_is_rejected_before_delivery_is_queued(self):
        for fields, version in (({'状态': '结束'}, ''), ({}, 'new-version')):
            with self.subTest(fields=fields), patch(
                    'lan_bitable_template_portal.server.external_real_write_guard', return_value={'mock_external': False, 'real_write_allowed': True}), patch(
                    'lan_bitable_template_portal.server.query_record_by_id', return_value=(True, {'fields': fields, 'record_version': version})), patch.object(
                    self.service, '_target_record_lifecycle', return_value={'finished': bool(fields)}):
                prepared = {**self.prepared, 'expected_record_version': 'old-version'}
                with self.assertRaises(Exception):
                    self.runtime._stage_message_first_notice('job-end', prepared, self.operation)
                self.assertFalse(self.store.list_outbox_events('notice_robot'))
                self.assertFalse(self.store.list_outbox_events('notice_personal'))

    def test_stage_commits_delivery_queues_without_upload_or_send(self):
        with patch('lan_bitable_template_portal.portal_service.send_text_to_open_ids') as send:
            self.stage()
        send.assert_not_called()
        self.assertEqual(self.service.get_job('job-end')['phase'], 'uploading')
        operation = self.store.get_notice_remote_operation(self.operation['operation_id'])
        self.assertNotEqual(operation['status'], 'completed')
        self.assertTrue(operation['result']['message_first'])
        self.assertEqual(operation['result']['robot_payload']['text'], self.prepared['text'])
        self.assertEqual(len(self.store.list_outbox_events('notice_robot')), 1)
        self.assertEqual(len(self.store.list_outbox_events('notice_personal')), 1)
        self.runtime.enqueue_message_job.assert_called_once_with('job-end')

    def test_personal_send_precedes_archive_and_restart_does_not_resend(self):
        self.stage()
        self.store.upsert_runtime_queue_item('message', 'job-end')
        self.runtime.process_notice_robot_messages = Mock()
        self.runtime._release_message_scope = Mock()
        with patch('lan_bitable_template_portal.portal_service.send_text_to_open_ids', side_effect=self.fixture.send_ok) as send:
            self.assertEqual(self.runtime._dequeue_runtime_job('message'), 'job-end')
            self.runtime._process_initial_message_job('job-end')
            self.assertEqual(send.call_count, 2)
            self.assertEqual(self.store.list_runtime_queue_items('message', statuses=('done',))[0]['job_id'], 'job-end')
            original = self.store.get_document('notice_personal_delivery', 'job-end')
            self.assertTrue(self.service.get_job('job-end')['message_sent'])
            self.assertFalse(self.service.get_job('job-end')['remote_written'])
            self.service._jobs.clear()
            self.runtime.process_notice_personal_messages(limit=5)
            self.assertEqual(send.call_count, 2)
            self.assertEqual(self.store.get_document('notice_personal_delivery', 'job-end')['results'], original['results'])

    def test_group_send_does_not_mark_archive_complete_or_repeat(self):
        self.stage()
        self.runtime.dispatch_notice_personal_messages = Mock()
        with patch('lan_bitable_template_portal.event_notice_delivery.dispatch'), patch(
                'lan_bitable_template_portal.dingtalk_notices.dispatch'), patch(
                'lan_bitable_template_portal.server.send_robot_message_by_payload',
                return_value={'robot_sent': True, 'robot_skipped': False}) as send:
            self.runtime.process_notice_robot_messages()
            self.runtime.process_notice_robot_messages()
        send.assert_called_once()
        operation = self.store.get_notice_remote_operation(self.operation['operation_id'])
        self.assertNotEqual(operation['status'], 'completed')
        self.assertEqual(operation['result']['robot_delivery_state'], 'sent')

    def test_transient_archive_error_defers_original_task_and_keeps_message_receipts(self):
        self.stage()
        self.store.upsert_runtime_queue_item('qt_action', 'job-end')
        self.assertTrue(self.runtime._defer_message_first_archive('job-end', self.prepared, 'HTTP 503 unavailable'))
        job = self.service.get_job('job-end')
        self.assertEqual(job['phase'], 'archive_pending')
        self.assertEqual(job['prepared'], self.prepared)
        self.assertEqual(job['archive_retry_count'], 1)
        self.assertFalse(job['remote_written'])
        self.assertEqual(len(self.store.list_outbox_events('notice_robot')), 1)
        self.assertFalse(self.runtime._defer_message_first_archive('job-end', self.prepared, 'forbidden 403'))
        self.assertFalse(self.runtime._defer_message_first_archive('job-end', self.prepared,
            '目标多维记录已被其他用户修改，请刷新后重新提交。'))

    def test_message_queue_temporary_failure_preserves_independent_archive_task(self):
        self.stage()
        self.store.upsert_runtime_queue_item('message', 'job-end')
        self.runtime.process_notice_robot_messages = Mock(side_effect=RuntimeError('isolated queue failure'))
        self.runtime._release_message_scope = Mock()
        self.runtime._process_initial_message_job('job-end')
        job = self.service.get_job('job-end')
        self.assertEqual(job['phase'], 'uploading')
        self.assertFalse(job['remote_written'])
        queued = self.store.list_runtime_queue_items('message', statuses=('queued',), due_only=False)
        self.assertEqual([item['job_id'] for item in queued], ['job-end'])

    def test_delivery_queue_positions_cannot_rewind_archive_phase(self):
        self.stage()
        self.store.upsert_runtime_queue_item('message', 'job-end')
        for phase in ('uploading', 'archive_pending', 'success', 'failed'):
            with self.subTest(phase=phase):
                self.service.mark_job('job-end', phase=phase)
                self.runtime._update_message_queue_positions_locked()
                job = self.service.get_job('job-end')
                self.assertEqual(job['phase'], phase)
                self.assertEqual(job['message_queue_position'], 1)

    def test_late_delivery_receipt_only_patches_result_preserving_archive_status(self):
        self.stage()
        self.store.mark_notice_remote_operation(self.operation['operation_id'], status='remote_written')
        self.store.mark_notice_remote_operation(self.operation['operation_id'], status=None,
                                                result={'robot_delivery_state': 'sent'})
        self.assertEqual(self.store.get_notice_remote_operation(self.operation['operation_id'])['status'], 'remote_written')

    def test_start_archive_retries_only_the_original_frozen_create_token(self):
        operation_id = 'notice_action:start-replay'
        self.store.begin_notice_remote_operation(operation_id=operation_id, operation_type='start', request={})
        fields = {'名称': '隔离工具调整', '通告状态': '开始'}
        token = 'c2a24d24-7118-4aa7-8a2b-bc7a5c392ca1'
        self.store.mark_notice_remote_operation(operation_id, status='executing', result={
            'message_first': True, 'create_fields': fields, 'create_client_token': token})
        prepared = {**self.prepared, 'action': 'start', 'target_record_id': '', 'record_id': ''}
        with patch.object(self.runtime, '_existing_target_for_prepared_start', return_value=''), patch.object(
                self.runtime, '_find_replayed_start_target', return_value=('', '')), patch.object(
                self.runtime, '_check_current_notice_actor'), patch.object(
                self.runtime, '_remote_record_matches_prepared', return_value=True), patch(
                'lan_bitable_template_portal.server.external_real_write_guard', return_value={'mock_external': False, 'real_write_allowed': True}), patch(
                'lan_bitable_template_portal.server.query_record_by_id', return_value=(True, {'fields': fields})), patch(
                'upload_event_module.services.feishu_service.create_bitable_record_fields',
                side_effect=[(False, 'HTTP 503'), (True, 'rec-created')]) as create:
            for expected in (False, True):
                operation = self.store.get_notice_remote_operation(operation_id)
                result = self.runtime._reconcile_notice_remote_intent(prepared, operation)
                self.assertEqual(result[0], expected)
            result = self.runtime._reconcile_notice_remote_intent(prepared, self.store.get_notice_remote_operation(operation_id))
            self.assertTrue(result[0])
        self.assertEqual(create.call_count, 2)
        for call in create.call_args_list:
            self.assertEqual(call.args[1], fields)
            self.assertEqual(call.kwargs['client_token'], token)
        self.assertEqual(self.store.get_notice_remote_operation(operation_id)['target_record_id'], 'rec-created')

    def test_message_staging_alone_is_not_an_uncertain_cloud_create(self):
        operation = {'operation_type': 'start', 'status': 'executing',
                     'result': {'message_first': True, 'robot_payload': {'text': '隔离通告'}}}
        self.assertFalse(self.runtime._remote_operation_outcome_uncertain(operation))
        operation['result']['create_client_token'] = 'persisted-token'
        self.assertTrue(self.runtime._remote_operation_outcome_uncertain(operation))

    def test_event_and_qt_requests_keep_their_existing_flow(self):
        self.runtime._stage_message_first_notice('job-end', {**self.prepared, 'work_type': 'event'}, self.operation)
        job = self.service.get_job('job-end')
        job['request'].pop('_web_action_request')
        self.service._jobs['job-end'] = job
        self.runtime._stage_message_first_notice('job-end', self.prepared, self.operation)
        self.runtime.enqueue_message_job.assert_not_called()

    def test_external_write_guard_prevents_message_staging(self):
        for guard in ({'real_write_allowed': False, 'mock_external': False},
                      {'real_write_allowed': False, 'mock_external': True}):
            with self.subTest(guard=guard), patch('lan_bitable_template_portal.server.external_real_write_guard', return_value=guard):
                self.stage()
            self.assertFalse(self.service.get_job('job-end').get('message_first'))
            self.assertFalse(self.store.list_outbox_events('notice_robot'))
            self.runtime.enqueue_message_job.assert_not_called()

    def test_prepared_text_follows_requested_action_for_all_regular_notice_types(self):
        methods = {'maintenance': 'prepare_maintenance_action', 'change': 'prepare_change_action',
                   'repair': 'prepare_repair_action', 'adjust': 'prepare_simple_manual_notice_action',
                   'polling': 'prepare_simple_manual_notice_action', 'power': 'prepare_simple_manual_notice_action'}
        for work, method in methods.items():
            for action, status in (('update', '更新'), ('end', '结束')):
                with self.subTest(work=work, action=action), patch.object(self.service, method,
                        return_value={**self.prepared, 'work_type': work, 'action': action, 'status': '开始'}), patch(
                        'lan_bitable_template_portal.dingtalk_notices.DingTalkNotices.snapshot', return_value=[]):
                    prepared = self.service.prepare_workbench_action(
                        {'work_type': work, 'action': action, 'dingtalk_robot_ids': ['isolated']}, job_id='job-end')
                    self.assertEqual(prepared['action'], action)
                    self.assertEqual(prepared['status'], status)
                    self.assertIn('状态：' + status, prepared['text'])

    def test_linked_ongoing_change_never_renders_as_a_new_start(self):
        ongoing = {'work_type': 'change', 'notice_type': '变更通告', 'title': '隔离变更',
                   'target_record_id': 'rec-target', 'source_record_id': 'rec-plan',
                   'active_item_id': 'active-fixture', 'building': 'D楼', 'status': '开始'}
        html = _detail_form(record=None, ongoing_item=ongoing, scope='D', work_type='change',
                            manual=True, parsed_action='start')
        self.assertIn('data-action="update"', html)
        self.assertNotIn('name="submit_action" value="start"', html)
        self.assertIn('name="submit_action" value="update"', html)


if __name__ == '__main__':
    unittest.main()
