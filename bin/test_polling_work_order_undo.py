"""End/undo/end uses real local stores and never calls business transports."""
import copy
import hashlib
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_lan_template_work_status import _TestMaintenancePortalService
from bin import test_polling_work_orders as fixtures
from lan_bitable_template_portal.polling_work_orders import PollingWorkOrderService, POLLING_WORK_ORDER_NAMESPACE
from lan_bitable_template_portal.server import PortalRuntime


class WorkOrderUndoTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(patch('socket.socket.connect', side_effect=AssertionError('No network')))
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.root = Path(folder.name)
        for module in ('portal_service', 'state_store', 'polling_work_orders'):
            self.enterContext(patch(f'lan_bitable_template_portal.{module}.get_data_file_path',
                                    side_effect=lambda name: str(self.root / name)))
        self.service = _TestMaintenancePortalService()
        self.store = self.service._state_store
        self.addCleanup(self.store.shutdown_write_worker, timeout=2)
        self.orders = PollingWorkOrderService(self.store)
        self.enterContext(patch.object(PortalRuntime, 'service', self.service))
        self.enterContext(patch.object(PortalRuntime, 'state_store', self.store))
        self.enterContext(patch.object(PortalRuntime, 'polling_work_orders', side_effect=lambda: self.orders))
        self.enterContext(patch('lan_bitable_template_portal.server.external_real_write_guard',
            return_value={'mock_external': True, 'real_write_allowed': False, 'reason': 'isolated test'}))
        for name in ('query_record_by_id', 'update_bitable_record_fields', 'create_bitable_record_fields', 'delete_bitable_record'):
            self.enterContext(patch('lan_bitable_template_portal.server.' + name,
                                    side_effect=AssertionError('No business transport')))

    def group(self, work_type='polling', target='rec-target', *, finished=True, uploaded=True):
        staged = self.orders.work_order_root / 'pending' / target / 'manual.txt'
        staged.parent.mkdir(parents=True, exist_ok=True)
        staged.write_bytes(b'fixture instruction')
        spec = {'work_type': work_type, 'scope': 'A', 'sop_id': 'fixture-sop', 'sop_version': 1,
            'sop_name': 'Same SOP', 'runs': [{'run_index': 1}, {'run_index': 2}],
            'steps': [{'step_id': 's1', 'content': '核实状态', 'operator_required': True,
                       'reviewer_required': True, 'photo_required': True}],
            'operator': {'record_id': 'op1', 'name': 'operator one', 'open_id': 'ou_one'},
            'operators': [{'record_id': 'op1', 'name': 'operator one', 'open_id': 'ou_one'},
                          {'record_id': 'op2', 'name': 'operator two', 'open_id': 'ou_two'}],
            'reviewer': {'record_id': 'reviewer', 'name': 'reviewer', 'open_id': 'ou_reviewer'},
            'attachments': [{'attachment_id': 'manual', 'name': 'manual.txt', 'staged_path': str(staged),
                             'sha256': hashlib.sha256(staged.read_bytes()).hexdigest()}]}
        self.orders.create_group({'polling_work_order_required': True, 'polling_work_order_spec': spec},
            target_record_id=target, title='Same title', public_base_url='https://fixture.invalid')
        if finished:
            for run in (1, 2):
                operator = self.orders.role_token(target, 'operator')
                reviewer = self.orders.role_token(target, 'reviewer')
                current = self.orders.get_group(target)
                self.orders.activate(operator, run_index=run, expected_version=current['version'])
                self.orders.add_step_photo(operator, step_key=f'{run}:1',
                    expected_version=self.orders.get_group(target)['version'],
                    file_name='photo.png', mime_type='image/png', content=fixtures._png_bytes())
                self.orders.confirm(operator, step_key=f'{run}:1',
                    expected_version=self.orders.get_group(target)['version'])
                self.orders.confirm(reviewer, step_key=f'{run}:1',
                    expected_version=self.orders.get_group(target)['version'])
            if uploaded:
                self.orders.mark_upload_result(target, success=True, file_tokens=['fixture-workbook'],
                                               photo_file_tokens=['fixture-photo'])
        return self.orders.get_group(target)

    def context(self, target, work_type):
        return {'target_record_id': target, 'record_id': target, 'active_item_id': 'active-' + target,
                'source_record_id': 'same-plan', 'work_type': work_type, 'title': 'Same title',
                'notice_type': {'polling': '设备轮巡', 'maintenance': '维保通告', 'adjust': '设备调整'}[work_type],
                'scope': 'A', 'building_codes': ['A'], 'status': '进行中'}

    def checkpoint(self, target, work_type, action='end', *, required=True):
        context = self.context(target, work_type)
        self.store.upsert_qt_active_item(context, section='other', origin='test')
        fields = {'工单附件': [{'file_token': 'fixture-workbook'}]}
        if required is not None:
            fields['是否涉及重要操作'] = required
        return self.service.create_notice_undo_checkpoint(action, context, scope='A',
            remote_fields=fields)

    def end_error(self, target, work_type):
        return PortalRuntime._work_order_end_error(target_record_id=target, work_type=work_type,
            fields={'是否涉及重要操作': True, '工单附件': [{'file_token': 'fixture-workbook'}]})

    def stop_notice(self, target, work_type):
        PortalRuntime._enqueue_active_delete_for_ended_notice(self.context(target, work_type), remote_record_id=target)

    def test_completed_end_undo_end_preserves_steps_people_and_files_for_all_types(self):
        for work_type in ('polling', 'maintenance', 'adjust'):
            with self.subTest(work_type=work_type):
                target = 'rec-' + work_type
                before = self.group(work_type, target)
                files = [Path(a['path']) for a in before['attachments']]
                files += [Path(p['path']) for step in before['steps'] for p in step['photos']]
                hashes = {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in files}
                for _ in range(2):
                    self.assertEqual(self.end_error(target, work_type), '')
                    undo_id = self.checkpoint(target, work_type)
                    self.stop_notice(target, work_type)
                    self.assertEqual(self.orders.get_group(target)['state'], 'cancelled')
                    self.orders = PollingWorkOrderService(self.store)
                    result = PortalRuntime.execute_notice_undo(undo_id)
                    self.assertTrue(result['ok'])
                    self.assertEqual(self.end_error(target, work_type), '')
                    restored = self.orders.get_group(target)
                    for key in ('steps', 'runs', 'current_index', 'attachments', 'operator', 'operators',
                                'reviewer', 'uploaded_file_tokens', 'uploaded_photo_file_tokens', 'token_hashes'):
                        self.assertEqual(restored[key], before[key], key)
                    self.assertEqual({str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in files}, hashes)

    def test_local_restore_failure_retries_without_repeating_remote_undo(self):
        for work_type in ('polling', 'maintenance', 'adjust'):
            target = 'rec-failure-' + work_type
            self.group(work_type, target)
            undo_id = self.checkpoint(target, work_type)
            self.stop_notice(target, work_type)
            original = self.store.put_document
            def fail(namespace, key, payload):
                if namespace == POLLING_WORK_ORDER_NAMESPACE and key == target:
                    raise OSError('synthetic local write failure')
                return original(namespace, key, payload)
            with patch('lan_bitable_template_portal.server.external_real_write_guard',
                       return_value={'mock_external': False, 'real_write_allowed': True, 'reason': ''}), \
                 patch('lan_bitable_template_portal.server.update_bitable_record_fields', return_value=(True, target)) as update:
                with patch.object(self.store, 'put_document', side_effect=fail):
                    with self.assertRaisesRegex(OSError, 'local write failure'):
                        PortalRuntime.execute_notice_undo(undo_id)
                self.assertEqual(self.store.get_notice_undo_action(undo_id)['status'], 'available')
                self.assertEqual(self.orders.get_group(target)['state'], 'cancelled')
                self.orders = PollingWorkOrderService(self.store)
                PortalRuntime.execute_notice_undo(undo_id)
                update.assert_called_once()
            self.assertEqual(self.store.get_notice_undo_action(undo_id)['status'], 'undone')
            self.assertEqual(self.end_error(target, work_type), '')

    def test_final_undo_marker_failure_retries_local_repair_idempotently(self):
        target = 'rec-marker'
        self.group(target=target)
        undo_id = self.checkpoint(target, 'polling')
        self.stop_notice(target, 'polling')
        original = self.store.mark_notice_undo_action
        def fail(identity, status, **kwargs):
            return False if status == 'undone' else original(identity, status, **kwargs)
        with patch('lan_bitable_template_portal.server.external_real_write_guard',
                   return_value={'mock_external': False, 'real_write_allowed': True, 'reason': ''}), \
             patch('lan_bitable_template_portal.server.update_bitable_record_fields', return_value=(True, target)) as update:
            with patch.object(self.store, 'mark_notice_undo_action', side_effect=fail):
                with self.assertRaisesRegex(Exception, '回退状态保存未完成'):
                    PortalRuntime.execute_notice_undo(undo_id)
            before = self.orders.get_group(target)
            PortalRuntime.execute_notice_undo(undo_id)
            self.assertEqual(self.orders.get_group(target), before)
            update.assert_called_once()

    def test_legacy_cancelled_completion_and_pending_upload_are_recovered(self):
        for work_type in ('polling', 'maintenance', 'adjust'):
            for uploaded in (False, True):
                target = f'rec-legacy-{work_type}-{uploaded}'
                before = self.group(work_type, target, uploaded=uploaded)
                undo_id = self.checkpoint(target, work_type)
                legacy = {**before, 'state': 'cancelled', 'cancel_reason': 'target_terminal'}
                if work_type == 'polling':
                    legacy.pop('work_type')
                self.store.put_document(POLLING_WORK_ORDER_NAMESPACE, target, legacy)
                PortalRuntime.execute_notice_undo(undo_id)
                self.assertEqual(self.end_error(target, work_type), '')
                self.assertEqual(self.orders.get_group(target)['state'], 'completed' if uploaded else 'upload_pending')

    def test_incomplete_legacy_and_current_orders_stay_blocked(self):
        for legacy in (False, True):
            for work_type in ('polling', 'maintenance', 'adjust'):
                target = f'rec-incomplete-{work_type}-{legacy}'
                before = self.group(work_type, target, finished=False)
                before.update(uploaded_file_tokens=['old-workbook'], upload_completed_at='old timestamp')
                self.store.put_document(POLLING_WORK_ORDER_NAMESPACE, target, before)
                undo_id = self.checkpoint(target, work_type)
                self.stop_notice(target, work_type)
                if legacy:
                    cancelled = self.orders.get_group(target)
                    cancelled.pop('state_before_cancel', None)
                    self.store.put_document(POLLING_WORK_ORDER_NAMESPACE, target, cancelled)
                PortalRuntime.execute_notice_undo(undo_id)
                self.assertIn('尚未全部完成', self.end_error(target, work_type))
                self.assertEqual(self.orders.get_group(target)['steps'], before['steps'])

    def test_legacy_automatic_steps_use_original_completion_rule_and_finished_cursor(self):
        with self.assertRaisesRegex(Exception, '至少需要'):
            self.orders._normalized_steps([{'content': 'new unsigned', 'operator_required': False, 'reviewer_required': False}])
        for finished_cursor in (False, True):
            target = f'rec-auto-{finished_cursor}'
            group = self.group(target=target)
            group['steps'].append({'step_key': '2:2', 'operator_required': False, 'reviewer_required': False,
                                   'content': 'legacy automatic step'})
            group.update(current_index=len(group['steps']) if finished_cursor else 0,
                         state='cancelled', cancel_reason='target_terminal')
            self.store.put_document(POLLING_WORK_ORDER_NAMESPACE, target, group)
            undo_id = self.checkpoint(target, 'polling')
            PortalRuntime.execute_notice_undo(undo_id)
            self.assertEqual(self.end_error(target, 'polling') == '', finished_cursor)

    def test_restore_is_bound_to_target_type_and_terminal_reason(self):
        first = self.group(target='rec-first')
        second = self.group(target='rec-second')
        undo_id = self.checkpoint('rec-first', 'polling')
        self.stop_notice('rec-first', 'polling')
        self.stop_notice('rec-second', 'polling')
        other = self.orders.get_group('rec-second')
        PortalRuntime.execute_notice_undo(undo_id)
        self.assertEqual(self.orders.get_group('rec-second'), other)
        for changes in ({'cancel_reason': 'target_deleted'}, {'cancel_reason': 'work_order_exempt'},
                        {'work_type': 'maintenance'}, {'target_record_id': 'rec-other'},
                        {'group_id': 'rec-other'}, {'state': 'stopped'}):
            candidate = {**first, 'state': 'cancelled', 'cancel_reason': 'target_terminal', **changes}
            self.store.put_document(POLLING_WORK_ORDER_NAMESPACE, 'rec-first', candidate)
            self.orders.restore_ended_group('rec-first', work_type='polling')
            self.assertEqual(self.orders.get_group('rec-first'), candidate)

    def test_pending_attachment_and_delay_guards_are_preserved(self):
        for state in ('upload_pending', 'completed'):
            target = 'rec-delay-' + state
            before = self.group(target=target, uploaded=state == 'completed')
            undo_id = self.checkpoint(target, 'polling')
            self.stop_notice(target, 'polling')
            group = self.orders.get_group(target)
            group['steps'][0].update(delay_reminder_minutes=1, delay_reminder={'due_at_ts': time.time() + 120})
            self.store.put_document(POLLING_WORK_ORDER_NAMESPACE, target, group)
            PortalRuntime.execute_notice_undo(undo_id)
            self.assertEqual(self.orders.get_group(target)['state'], state)
            self.assertIn('延时提醒尚未到期', self.end_error(target, 'polling'))
            self.assertEqual(self.orders.get_group(target)['uploaded_file_tokens'], before['uploaded_file_tokens'])

    def test_legacy_already_applied_undo_recovers_only_from_current_exact_notice(self):
        for finished in (False, True):
            target = f'rec-already-undone-{finished}'
            self.group(target=target, finished=finished)
            undo_id = self.checkpoint(target, 'polling')
            self.stop_notice(target, 'polling')
            legacy = self.orders.get_group(target)
            legacy.pop('state_before_cancel')
            self.store.put_document(POLLING_WORK_ORDER_NAMESPACE, target, legacy)
            undo = self.store.get_notice_undo_action(undo_id)
            self.service.restore_notice_undo_local(undo, target_record_id=target)
            self.store.mark_notice_undo_action(undo_id, 'undone', payload_patch={'restored_target_record_id': target})
            self.orders = PollingWorkOrderService(self.store)
            self.assertEqual(self.end_error(target, 'polling') == '', finished)
            self.assertEqual(self.orders.get_group(target)['state'], 'completed' if finished else 'active')

    def test_cancelled_order_is_not_recovered_from_unapplied_or_other_notice_undo(self):
        target = 'rec-unconfirmed'
        self.group(target=target)
        undo_id = self.checkpoint(target, 'polling')
        self.stop_notice(target, 'polling')
        legacy = self.orders.get_group(target)
        legacy.pop('state_before_cancel')
        self.store.put_document(POLLING_WORK_ORDER_NAMESPACE, target, legacy)
        undo = self.store.get_notice_undo_action(undo_id)
        self.service.restore_notice_undo_local(undo, target_record_id=target)
        self.assertIn('尚未全部完成', self.end_error(target, 'polling'))
        self.assertEqual(self.orders.get_group(target), legacy)
        other_id = self.checkpoint('rec-other-undo', 'polling')
        self.store.mark_notice_undo_action(other_id, 'undone')
        row = next(row for row in self.store.list_qt_active_items() if row['record_id'] == target)
        self.store.upsert_qt_active_item({**row['payload'], 'undo_restored_id': other_id}, section='other', origin='test')
        self.assertIn('尚未全部完成', self.end_error(target, 'polling'))
        self.assertEqual(self.orders.get_group(target), legacy)

    def test_unknown_remote_receipt_does_not_repeat_remote_or_mark_undo_applied(self):
        target = 'rec-unknown-remote'
        self.group(target=target)
        undo_id = self.checkpoint(target, 'polling')
        self.stop_notice(target, 'polling')
        original = self.store.mark_notice_undo_action
        def fail(identity, status, **kwargs):
            receipt = (kwargs.get('payload_patch') or {}).get('work_order_end_remote_receipt') or {}
            return False if receipt.get('state') == 'restored' else original(identity, status, **kwargs)
        with patch('lan_bitable_template_portal.server.external_real_write_guard',
                   return_value={'mock_external': False, 'real_write_allowed': True, 'reason': ''}), \
             patch('lan_bitable_template_portal.server.update_bitable_record_fields', return_value=(True, target)) as update:
            with patch.object(self.store, 'mark_notice_undo_action', side_effect=fail):
                with self.assertRaisesRegex(Exception, '远端回执保存失败'):
                    PortalRuntime.execute_notice_undo(undo_id)
            with self.assertRaisesRegex(Exception, '远端回退结果尚未确认'):
                PortalRuntime.execute_notice_undo(undo_id)
            update.assert_called_once()
        self.assertEqual(self.store.get_notice_undo_action(undo_id)['status'], 'available')
        self.assertEqual(self.orders.get_group(target)['state'], 'cancelled')

    def test_no_work_order_keeps_original_remote_failure_retry_without_new_receipt(self):
        for work_type in ('polling', 'maintenance', 'adjust'):
            for required in (False, None):
                target = f'rec-no-order-{work_type}-{required}'
                undo_id = self.checkpoint(target, work_type, required=required)
                with patch('lan_bitable_template_portal.server.external_real_write_guard',
                           return_value={'mock_external': False, 'real_write_allowed': True, 'reason': ''}), \
                     patch('lan_bitable_template_portal.server.update_bitable_record_fields',
                           side_effect=[(False, 'fixture original failure'), (True, target)]) as update, \
                     patch.object(self.orders, 'restore_ended_group', side_effect=AssertionError('No work order to restore')):
                    with self.assertRaisesRegex(Exception, 'fixture original failure'):
                        PortalRuntime.execute_notice_undo(undo_id)
                    self.assertNotIn('work_order_end_remote_receipt', self.store.get_notice_undo_action(undo_id))
                    self.assertTrue(PortalRuntime.execute_notice_undo(undo_id)['ok'])
                    self.assertEqual(update.call_count, 2)
                self.assertNotIn('work_order_end_remote_receipt', self.store.get_notice_undo_action(undo_id))

    def test_receipt_requires_matching_target_type_or_explicit_required_flag(self):
        for changes in ({'work_type': 'maintenance'}, {'target_record_id': 'rec-other'}, {'group_id': 'rec-other'}):
            target = 'rec-mismatch-' + next(iter(changes))
            self.group(target=target)
            group = self.orders.get_group(target)
            group.update(changes)
            self.store.put_document(POLLING_WORK_ORDER_NAMESPACE, target, group)
            undo_id = self.checkpoint(target, 'polling', required=False)
            PortalRuntime.execute_notice_undo(undo_id)
            self.assertNotIn('work_order_end_remote_receipt', self.store.get_notice_undo_action(undo_id))
            self.assertEqual(self.orders.get_group(target), group)
        target = 'rec-matching-local'
        self.group(target=target)
        undo_id = self.checkpoint(target, 'polling', required=False)
        self.stop_notice(target, 'polling')
        PortalRuntime.execute_notice_undo(undo_id)
        self.assertEqual(self.store.get_notice_undo_action(undo_id)['work_order_end_remote_receipt']['state'], 'restored')
        target = 'rec-required-missing-local'
        undo_id = self.checkpoint(target, 'polling', required=True)
        PortalRuntime.execute_notice_undo(undo_id)
        self.assertEqual(self.store.get_notice_undo_action(undo_id)['work_order_end_remote_receipt']['state'], 'restored')
        self.assertIn('无法核验', self.end_error(target, 'polling'))

    def test_untyped_remote_failure_for_work_order_is_not_assumed_safe_to_replay(self):
        target = 'rec-untyped-rejection'
        self.group(target=target)
        undo_id = self.checkpoint(target, 'polling')
        self.stop_notice(target, 'polling')
        with patch('lan_bitable_template_portal.server.external_real_write_guard',
                   return_value={'mock_external': False, 'real_write_allowed': True, 'reason': ''}), \
             patch('lan_bitable_template_portal.server.update_bitable_record_fields',
                   return_value=(False, 'fixture provider rejection text')) as update:
            with self.assertRaisesRegex(Exception, 'provider rejection'):
                PortalRuntime.execute_notice_undo(undo_id)
            with self.assertRaisesRegex(Exception, '尚未确认'):
                PortalRuntime.execute_notice_undo(undo_id)
            update.assert_called_once()


if __name__ == '__main__':
    unittest.main()
