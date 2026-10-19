"""Offline notice queue integration against the account-impersonation contract."""
import asyncio
import copy
import socket
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

BIN = Path(__file__).resolve().parent
sys.path.insert(0, str(BIN))
from lan_bitable_template_portal import notice_actions as actions
from lan_bitable_template_portal import server
from lan_bitable_template_portal.portal_service import MaintenancePortalService, PortalError, STATE_NS_ACTION_JOB
from lan_bitable_template_portal.server import PortalRuntime
from lan_bitable_template_portal.state_store import LanPortalStateStore


class NoticeImpersonationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='notice_impersonation_')
        self.addCleanup(self.temp.cleanup)
        self.store = LanPortalStateStore(Path(self.temp.name) / 'state.sqlite3')
        self.addCleanup(lambda: self.store.shutdown_write_worker(timeout=2))
        self.service = self.bare_service()
        self.auth = SimpleNamespace(operators={'ou_admin'}, targets={'ou_target'}, scopes={'A'}, modules={'maintenance'})
        self.module = actions.impersonation
        self.current = self.module.CURRENT_IMPERSONATION
        self.auth.scope_allowed = lambda session, scope: scope in self.auth.scopes
        self.grant = dict(id='grant-one', operator_open_id='ou_admin', operator_name='真实管理员',
            target_open_id='ou_target', target_name='被代登录人员', parent_hash='opaque', expires_at=time.time()+1200)
        self.save_grant(self.grant)
        policy = patch.object(self.module, 'can_impersonate', side_effect=lambda auth, operator, target:
                              operator in auth.operators and target in auth.targets)
        policy.start()
        self.addCleanup(policy.stop)
        self.remote = Mock()
        self.runtime = type('IsolatedNoticeRuntime', (PortalRuntime,), {
            'service': self.service, 'state_store': self.store, 'auth_manager': self.auth,
            '_get_notice_remote_operation': classmethod(lambda cls, key: None),
            'clear_payload_cache': classmethod(lambda cls: None),
            'ensure_action_worker': classmethod(lambda cls: None),
        })
        self.runtime.enqueue_initial_message_or_upload_job = Mock()
        self.controller = SimpleNamespace(_authorized_scope_or_error=lambda session, scope: scope,
            _clear_read_cache=Mock())
        self.session = {'user': {'open_id': 'ou_target', 'name': '被代登录人员'}}
        def require_module(auth, session, module):
            if module not in auth.modules:
                raise PortalError('module revoked')
        module_access = patch('lan_bitable_template_portal.module_access.require_module', side_effect=require_module)
        module_access.start()
        self.addCleanup(module_access.stop)
        # Real business validation is covered by the sibling suites; isolate identity/queue boundaries here.
        for name in ('validate_source_scope', 'validate_submission'):
            guard = patch('lan_bitable_template_portal.planned_notices.' + name, return_value=None)
            guard.start()
            self.addCleanup(guard.stop)
        connect = socket.socket.connect
        def local_only(sock, address):
            if address[0] not in {'127.0.0.1', '::1', 'localhost'}:
                raise AssertionError('no network in notice tests')
            return connect(sock, address)
        network = patch('socket.socket.connect', local_only)
        network.start()
        self.addCleanup(network.stop)

    def bare_service(self):
        service = object.__new__(MaintenancePortalService)
        service._jobs_lock = threading.RLock()
        service._jobs = {}
        service._state_store = self.store
        return service

    def save_grant(self, grant):
        self.store.put_document(self.module.NS, grant['id'], copy.deepcopy(grant))

    def payload(self, operation='op-one', **kw):
        return {'action': 'start', 'work_type': 'maintenance', 'notice_type': '维保通告',
            'record_id': 'rec-one', 'scope': 'A', '_auth_open_id': 'ou_target',
            'operation_id': operation, **kw}

    def create(self, *, grant=None, payload=None, bind=True):
        token = self.current.set(self.grant if grant is None else grant)
        try:
            job_id, _ = self.service.create_action_job(payload or self.payload())
            if bind:
                actions.bind_notice_job(self.runtime, job_id)
            return job_id
        finally:
            self.current.reset(token)

    def worker(self, job_id):
        def execute(cls, identity):
            self.assertEqual(identity, job_id)
            self.assertEqual(self.current.get()['operator_open_id'], 'ou_admin')
            self.remote(identity)
            cls.service.mark_job(identity, phase='success', remote_written=True,
                                 target_record_id='rec-target', _persist=True)
        with patch.object(self.runtime, '_process_maintenance_action_job_impl', classmethod(execute)):
            self.runtime._process_maintenance_action_job(job_id)

    def audits(self, job_id):
        return self.store.list_business_operation_audits(domain='notice', operation_id=job_id)

    def test_forged_markers_are_removed_recursively_without_mutating_client(self):
        payload = self.payload(_admin_impersonation_id='fake', _admin_actor='ou_other',
            patch={'_admin_override': True, 'progress': 'unchanged'}, _impersonation_grant={'id': 'fake'})
        before = copy.deepcopy(payload)
        job_id, _ = self.service.create_action_job(payload)
        request = self.service.get_job(job_id)['request']
        self.assertNotIn('_admin_impersonation_id', request)
        self.assertNotIn('_admin_actor', request)
        self.assertNotIn('_impersonation_grant', request)
        self.assertEqual(request['patch'], {'progress': 'unchanged'})
        self.assertEqual(payload, before)
        self.assertFalse(self.service.get_job(job_id).get('impersonation_binding_pending'))

    def test_only_server_context_can_stamp_target_and_id(self):
        job_id = self.create(bind=False, payload=self.payload(_admin_impersonation_id='spoofed'))
        request = self.service.get_job(job_id)['request']
        self.assertEqual(request['_admin_impersonation_id'], self.grant['id'])
        self.assertTrue(self.service.get_job(job_id)['impersonation_binding_pending'])
        with self.assertRaises(PortalError):
            self.create(payload=self.payload('op-wrong-target', _auth_open_id='ou_another'))

    def test_same_operation_cannot_be_shared_with_self_another_admin_or_new_grant(self):
        job_id = self.create()
        other = {**self.grant, 'id': 'grant-two', 'operator_open_id': 'ou_other'}
        self.save_grant(other)
        for grant in ({}, other, {**self.grant, 'id': 'new-session'}):
            with self.subTest(grant=grant), self.assertRaises(PortalError):
                self.create(grant=grant)
        self.assertEqual(self.service.get_job(job_id)['request']['_admin_impersonation_id'], 'grant-one')
        self.assertEqual(len(self.service._jobs), 1)

    def test_native_task_cannot_be_adopted_by_impersonation(self):
        job_id, _ = self.service.create_action_job(self.payload())
        with self.assertRaises(PortalError):
            self.create()
        self.assertNotIn('_admin_impersonation_id', self.service.get_job(job_id)['request'])

    def test_same_grant_idempotency_and_target_deduplication_preserve_original_owner(self):
        job_id = self.create()
        self.assertEqual(self.create(), job_id)
        other = {**self.grant, 'id': 'grant-two', 'operator_open_id': 'ou_other'}
        self.save_grant(other)
        with self.assertRaises(PortalError):
            self.create(grant=other, payload=self.payload('different-operation'))
        self.assertEqual(len(self.audits(job_id)), 1)

    def test_submit_binds_and_audits_before_enqueue(self):
        def enqueue(job_id):
            binding = self.store.get_document(self.module.NS + '_jobs', job_id)
            self.assertEqual(binding['grant_id'], self.grant['id'])
            self.assertEqual(self.audits(job_id)[0]['actor_open_id'], 'ou_admin')
            self.assertFalse(self.service.get_job(job_id)['impersonation_binding_pending'])
        self.runtime.enqueue_initial_message_or_upload_job.side_effect = enqueue
        token = self.current.set(self.grant)
        try:
            response = asyncio.run(actions.submit_notice_action(self.controller, self.runtime,
                self.session, self.payload()))
        finally:
            self.current.reset(token)
        self.assertEqual(response.status_code, 202)
        self.assertEqual(len(self.service._jobs), 1)
        self.runtime.enqueue_initial_message_or_upload_job.assert_called_once()

    def test_bind_disk_failure_recovers_automatically_without_user_resubmission(self):
        token = self.current.set(self.grant)
        try:
            with patch.object(self.module, 'bind_job', side_effect=sqlite3.OperationalError('busy')):
                response = asyncio.run(actions.submit_notice_action(self.controller, self.runtime, self.session, self.payload()))
                self.assertEqual(response.status_code, 202)
        finally:
            self.current.reset(token)
        job_id = next(iter(self.service._jobs))
        self.assertIsNone(self.store.get_document(self.module.NS + '_jobs', job_id))
        queue = self.store.list_runtime_queue_items('qt_action', statuses=('queued',))
        self.assertEqual([row['job_id'] for row in queue], [job_id])
        leased = self.store.lease_runtime_queue_items('qt_action', limit=1, lease_seconds=30)
        self.assertEqual([row['job_id'] for row in leased], [job_id])
        self.remote.assert_not_called()
        self.worker(job_id)
        self.remote.assert_called_once_with(job_id)
        self.assertEqual(self.service.get_job(job_id)['phase'], 'success')
        self.assertEqual(self.store.get_document(self.module.NS + '_jobs', job_id)['grant_id'], self.grant['id'])

    def test_crash_between_job_persistence_and_bind_recovers_original_job_after_restart(self):
        job_id = self.create(bind=False)
        restarted = self.bare_service()
        restarted._jobs = restarted._load_action_jobs_from_state()
        self.assertEqual(restarted.recoverable_action_job_ids(), [job_id])
        self.service = self.runtime.service = restarted
        self.assertEqual(restarted.get_job(job_id)['phase'], 'accepted')
        self.worker(job_id)
        self.remote.assert_called_once_with(job_id)
        self.assertEqual(len(restarted._jobs), 1)
        self.assertEqual(self.audits(job_id)[0]['actor_open_id'], 'ou_admin')

    def test_missing_or_contradictory_grant_evidence_never_rebinds(self):
        job_id = self.create(bind=False)
        self.store.delete_document(self.module.NS, self.grant['id'])
        self.worker(job_id)
        self.remote.assert_not_called()
        self.assertEqual(self.service.get_job(job_id)['phase'], 'cancelled')
        self.assertIsNone(self.store.get_document(self.module.NS + '_jobs', job_id))

    def test_recovery_cannot_replace_existing_binding_even_when_pending(self):
        job_id = self.create(bind=False)
        self.store.put_document(self.module.NS + '_jobs', job_id,
            {'grant_id': 'another-grant', 'target_open_id': 'ou_target'})
        self.worker(job_id)
        self.remote.assert_not_called()
        self.assertEqual(self.service.get_job(job_id)['phase'], 'cancelled')
        self.assertEqual(self.store.get_document(self.module.NS + '_jobs', job_id)['grant_id'], 'another-grant')

    def test_pending_recovery_rechecks_current_operator_permission(self):
        job_id = self.create(bind=False)
        self.auth.operators.clear()
        self.worker(job_id)
        self.remote.assert_not_called()
        self.assertEqual(self.service.get_job(job_id)['phase'], 'cancelled')
        self.assertIsNone(self.store.get_document(self.module.NS + '_jobs', job_id))

    def test_recovery_binding_disk_failure_retries_same_job_after_storage_recovers(self):
        job_id = self.create(bind=False)
        with patch.object(self.module, 'bind_job', side_effect=sqlite3.OperationalError('still busy')):
            self.worker(job_id)
        self.remote.assert_not_called()
        self.assertEqual(self.service.get_job(job_id)['phase'], 'accepted')
        self.worker(job_id)
        self.remote.assert_called_once_with(job_id)
        self.assertEqual(len(self.service._jobs), 1)

    def test_job_and_queue_persist_atomically_before_binding(self):
        with patch.object(self.store, 'put_action_job_with_queue', side_effect=sqlite3.OperationalError('commit failed')):
            with self.assertRaises(sqlite3.OperationalError):
                self.create(bind=False)
        self.assertEqual(self.service._jobs, {})
        self.assertEqual(self.store.list_documents(STATE_NS_ACTION_JOB), [])
        self.assertEqual(self.store.list_runtime_queue_items('qt_action'), [])

    def test_current_target_permissions_are_rechecked_for_people_scopes_and_modules(self):
        for attribute in ('targets', 'scopes', 'modules'):
            with self.subTest(attribute=attribute):
                self.service = self.bare_service()
                self.runtime.service = self.service
                job_id = self.create(payload=self.payload('op-' + attribute, record_id='rec-' + attribute))
                old = getattr(self.auth, attribute)
                setattr(self.auth, attribute, set())
                self.worker(job_id)
                setattr(self.auth, attribute, old)
                self.assertEqual(self.service.get_job(job_id)['phase'], 'cancelled')
                self.assertTrue(self.service.get_job(job_id)['impersonation_blocked'])
        self.remote.assert_not_called()

    def test_current_operator_revocation_blocks_remote_action_and_preserves_audit_identity(self):
        job_id = self.create()
        self.auth.operators.clear()
        self.worker(job_id)
        self.remote.assert_not_called()
        job = self.service.get_job(job_id)
        self.assertEqual(job['phase'], 'cancelled')
        audit = self.audits(job_id)[0]
        self.assertEqual(audit['actor_open_id'], 'ou_admin')
        self.assertEqual(audit['metadata']['impersonation']['target_open_id'], 'ou_target')

    def test_waiting_restart_stop_and_expiry_keep_accepted_job_and_real_operator(self):
        job_id = self.create()
        self.service.mark_job(job_id, was_event_waiting=True, phase='event_waiting', _persist=True)
        self.grant.update(stopped=True, expires_at=time.time()-3600)
        self.save_grant(self.grant)
        restarted = self.bare_service()
        restarted._jobs[job_id] = self.store.get_document(STATE_NS_ACTION_JOB, job_id)
        self.service = self.runtime.service = restarted
        self.worker(job_id)
        self.assertIsNone(self.current.get())
        self.remote.assert_called_once_with(job_id)
        self.assertEqual(self.service.get_job(job_id)['phase'], 'success')
        audit = self.audits(job_id)[0]
        self.assertEqual(audit['status'], 'success')
        self.assertEqual(audit['actor_open_id'], 'ou_admin')
        self.assertEqual(audit['metadata']['impersonation']['target_open_id'], 'ou_target')

    def test_audit_begin_failure_recovers_without_changing_operation_or_binding(self):
        job_id = self.create(bind=False)
        self.module.bind_job(self.store, self.grant, job_id)
        with patch.object(self.store, 'record_business_operation_audit', side_effect=sqlite3.OperationalError('busy')):
            self.worker(job_id)
        self.remote.assert_not_called()
        self.worker(job_id)
        self.remote.assert_called_once_with(job_id)
        self.assertEqual(len(self.audits(job_id)), 1)

    def test_audit_finish_failure_only_replays_bookkeeping_not_remote_send(self):
        job_id = self.create()
        original = self.store.record_business_operation_audit
        def record(**kw):
            if kw.get('status') == 'success':
                raise sqlite3.OperationalError('finish busy')
            return original(**kw)
        with patch.object(self.store, 'record_business_operation_audit', side_effect=record):
            self.worker(job_id)
        job = self.service.get_job(job_id)
        self.assertEqual(job['phase'], 'success')
        self.assertTrue(job['impersonation_audit_pending'])
        restarted = self.bare_service()
        restarted._jobs = restarted._load_action_jobs_from_state()
        self.assertEqual(restarted.recoverable_action_job_ids(), [job_id])
        self.service = self.runtime.service = restarted
        self.auth.operators.clear()
        self.assertTrue(self.runtime._runtime_queue_job_processable('qt_action', job_id))
        self.worker(job_id)
        self.remote.assert_called_once_with(job_id)
        self.assertFalse(self.service.get_job(job_id)['impersonation_audit_pending'])
        self.assertEqual(self.audits(job_id)[0]['status'], 'success')

    def test_compaction_keeps_binding_and_audit_recovery_flags(self):
        job_id = self.create()
        self.service.mark_job(job_id, phase='success', impersonation_audit_pending=True)
        with self.service._jobs_lock:
            self.service._compact_completed_job_locked(job_id)
        job = self.service.get_job(job_id)
        self.assertEqual(job['request']['_admin_impersonation_id'], self.grant['id'])
        self.assertTrue(job['impersonation_audit_pending'])
        self.assertEqual(actions.check_notice_job_impersonation(self.runtime, job_id)['operator_open_id'], 'ou_admin')
        self.service.mark_job(job_id, phase='failed', error='timeout', error_retryable=True)
        with self.service._jobs_lock:
            self.service._compact_failed_job_locked(job_id)
        job = self.service.get_job(job_id)
        self.assertEqual(job['request']['_admin_impersonation_id'], self.grant['id'])
        self.assertEqual(job['retry_request']['_admin_impersonation_id'], self.grant['id'])

    def test_qt_terminal_audit_recovery_finishes_saved_receipt_without_uploading_again(self):
        job_id = self.create()
        command = {'action_type': 'upload', 'operation_id': 'qt-finish', 'data_dict': self.payload()}
        def execute(cls, identity):
            self.remote(identity)
            cls.service.mark_job(identity, phase='success', execution_origin='qt_local',
                qt_request=command, qt_result={'ok': True, 'real_record_id': 'rec-qt-finish'},
                qt_completion_pending=True, remote_written=True)
        original = self.store.record_business_operation_audit
        def record(**kw):
            if kw.get('status') == 'success':
                raise sqlite3.OperationalError('finish busy')
            return original(**kw)
        with patch.object(self.runtime, '_process_maintenance_action_job_impl', classmethod(execute)), \
                patch.object(self.store, 'record_business_operation_audit', side_effect=record):
            self.runtime._process_maintenance_action_job(job_id)
        self.assertTrue(self.service.get_job(job_id)['impersonation_audit_pending'])
        self.auth.operators.clear()
        self.runtime._process_maintenance_action_job(job_id)
        self.remote.assert_called_once_with(job_id)
        self.assertFalse(self.service.get_job(job_id)['qt_completion_pending'])
        results = self.store.list_outbox_events('qt_action')
        self.assertEqual(sum(row['payload'].get('kind') == 'notice_wait_result' for row in results), 1)
        self.assertEqual(self.audits(job_id)[0]['actor_open_id'], 'ou_admin')

    def test_qt_deferred_upload_resumes_bound_context_and_stops_revoked_target(self):
        token = self.current.set(self.grant)
        command = {'action_type': 'upload', 'operation_id': 'qt-op', 'data_dict': self.payload()}
        try:
            job = self.service.create_qt_event_wait_job(command, [])
            actions.bind_notice_job(self.runtime, job['job_id'])
        finally:
            self.current.reset(token)
        job_id = job['job_id']
        def upload(request):
            self.assertEqual(self.current.get()['operator_open_id'], 'ou_admin')
            self.assertTrue(request['data_dict']['_event_deferred_upload'])
            return {'ok': True, 'real_record_id': 'rec-qt-target'}
        with patch.object(self.runtime, '_execute_local_notice_upload', side_effect=upload) as remote:
            result = self.runtime.execute_local_notice_upload(command, _queued_job_id=job_id)
            self.assertTrue(result['ok'])
            self.auth.targets.clear()
            with self.assertRaises(actions.NoticeJobPermissionDenied):
                self.runtime.execute_local_notice_upload(command, _queued_job_id=job_id)
            remote.assert_called_once()
        self.assertIsNone(self.current.get())

    def test_followup_source_write_restores_context_and_rechecks_current_grant(self):
        job_id = self.create()
        payload = {'_admin_notice_job_id': job_id}
        def sync(value):
            self.assertEqual(self.current.get()['target_open_id'], 'ou_target')
            return True
        with patch.object(self.runtime, '_sync_confirmed_source_end_impl', side_effect=sync) as write:
            self.assertTrue(self.runtime._sync_confirmed_source_end(payload))
            self.auth.operators.clear()
            with self.assertRaises(actions.NoticeJobPermissionDenied):
                self.runtime._sync_confirmed_source_end(payload)
            write.assert_called_once()

    def robot_operation(self, job_id):
        operation_id = 'notice_action:' + job_id
        self.store.begin_notice_remote_operation(operation_id=operation_id, operation_type='start')
        with actions.notice_job_impersonation(self.runtime, job_id):
            self.runtime._mark_notice_remote_operation(operation_id, status='completed', target_record_id='rec-target',
                result={'robot_background': True, 'robot_delivery_state': 'pending',
                        'robot_payload': {'text': '隔离测试通告'}, 'robot_notice_type': '维保通告',
                        'robot_message_uuid': '00000000-0000-4000-8000-000000000001'})
        self.service.mark_job(job_id, phase='success', remote_written=True, target_record_id='rec-target')
        return operation_id

    def test_actual_robot_outbox_resumes_original_actor_and_sends_once(self):
        job_id = self.create()
        operation_id = self.robot_operation(job_id)
        def send(*args, **kw):
            self.assertEqual(self.current.get()['operator_open_id'], 'ou_admin')
            self.assertEqual(kw['message_uuid'], '00000000-0000-4000-8000-000000000001')
            return {'robot_sent': True}
        with patch.object(self.runtime, '_get_notice_remote_operation', side_effect=self.store.get_notice_remote_operation), \
                patch.object(server, 'send_robot_message_by_payload', side_effect=send) as outbound:
            self.runtime.process_notice_robot_messages()
            self.runtime.process_notice_robot_messages()
        outbound.assert_called_once()
        result = self.store.get_notice_remote_operation(operation_id)['result']
        self.assertEqual(result['_admin_notice_job_id'], job_id)
        self.assertEqual(result['robot_delivery_state'], 'sent')
        self.assertIsNone(self.current.get())

    def test_robot_permission_revocation_keeps_already_written_notice_and_never_sends(self):
        job_id = self.create()
        operation_id = self.robot_operation(job_id)
        self.auth.targets.clear()
        with patch.object(self.runtime, '_get_notice_remote_operation', side_effect=self.store.get_notice_remote_operation), \
                patch.object(server, 'send_robot_message_by_payload') as outbound:
            self.runtime.process_notice_robot_messages()
        outbound.assert_not_called()
        self.assertEqual(self.service.get_job(job_id)['phase'], 'success')
        self.assertTrue(self.service.get_job(job_id)['remote_written'])
        operation = self.store.get_notice_remote_operation(operation_id)
        self.assertEqual(operation['status'], 'completed')
        self.assertEqual(operation['result']['robot_delivery_state'], 'blocked_permission')

    def test_pending_robot_receipt_pins_bound_job_until_delivery_finishes(self):
        job_id = self.create()
        self.robot_operation(job_id)
        with patch('lan_bitable_template_portal.portal_service.time.time', return_value=time.time()+3600):
            result = self.service.cleanup_action_jobs(success_retention_seconds=60)
        self.assertEqual(result['removed_success'], 0)
        self.assertIsNotNone(self.store.get_document(STATE_NS_ACTION_JOB, job_id))
        self.assertFalse(self.service.delete_action_job(job_id))

    def test_revocation_after_remote_write_stops_next_stage_without_falsifying_saved_result(self):
        job_id = self.create()
        following_send = Mock()
        def execute(cls, identity):
            cls.service.mark_job(identity, remote_written=True, target_record_id='rec-written')
            self.auth.operators.clear()
            cls._check_current_notice_actor()
            following_send()
        with patch.object(self.runtime, '_process_maintenance_action_job_impl', classmethod(execute)):
            self.runtime._process_maintenance_action_job(job_id)
        following_send.assert_not_called()
        job = self.service.get_job(job_id)
        self.assertEqual(job['phase'], 'cancelled')
        self.assertTrue(job['partial'])
        self.assertTrue(job['remote_written'])
        self.assertEqual(job['target_record_id'], 'rec-written')
        self.assertEqual(job['error_category'], 'permission_denied')
        self.auth.operators.add('ou_admin')
        with self.assertRaises(PortalError):
            self.create()
        self.assertEqual(len(self.service._jobs), 1)

    def test_topic_delegation_cannot_be_combined_with_impersonation(self):
        with self.assertRaises(PortalError):
            self.create(payload=self.payload(_topic_delegation_id='topic-grant'))

    def test_normal_job_never_uses_impersonation_resume_or_audit(self):
        job_id, _ = self.service.create_action_job(self.payload())
        with patch.object(self.module, 'resume_job', side_effect=AssertionError('native job must be unchanged')):
            with actions.notice_job_impersonation(self.runtime, job_id) as grant:
                self.assertIsNone(grant)
        self.assertEqual(self.audits(job_id), [])
        self.assertIsNone(self.current.get())

    def test_normal_job_portal_error_propagates_without_changing_phase(self):
        job_id, _ = self.service.create_action_job(self.payload())
        before = copy.deepcopy(self.service.get_job(job_id))
        error = PortalError('ordinary business failure')
        with patch.object(self.runtime, '_process_maintenance_action_job_impl', side_effect=error), \
             patch.object(self.service, 'mark_job', wraps=self.service.mark_job) as mark_job, \
             patch.object(self.store, 'mark_runtime_queue_item', wraps=self.store.mark_runtime_queue_item) as mark_queue:
            with self.assertRaises(PortalError) as raised:
                self.runtime._process_maintenance_action_job(job_id)
        self.assertIs(raised.exception, error)
        mark_job.assert_not_called()
        mark_queue.assert_not_called()
        self.assertEqual(self.service.get_job(job_id), before)
        self.assertEqual(self.store.get_document(STATE_NS_ACTION_JOB, job_id), before)
        self.assertEqual(self.audits(job_id), [])


if __name__ == '__main__':
    unittest.main()
