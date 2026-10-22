"""Maintenance-only advisory checks and existing message delivery recovery."""
import copy
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from upload_event_module.services.notice_cycle_warnings import (
    maintenance_cycle_rules, maintenance_cycle_warnings, maintenance_reminder_text,
)
from upload_event_module.services.handlers.base import NoticePayload
from upload_event_module.services.handlers.maintenance_notice import MaintenanceNoticeHandler


def notice(action='开始', kind='维保通告', content='季度设备维护', reason='按半年度维保计划执行'):
    return f'【{kind}】状态：{action}\n【名称】测试维保\n【内容】{content}\n【原因】{reason}\n【进度】等待核对'


class CycleWarningTests(unittest.TestCase):
    def test_monthly_conflicts_are_field_specific_and_read_only(self):
        values = {'content': '季度设备维护', 'reason': '按半年度、年度维保计划执行'}
        before = copy.deepcopy(values)
        result = maintenance_cycle_warnings('维保通告', '月度', **values)
        self.assertEqual([(r['field'], r['terms']) for r in result], [('content', ['季度']), ('reason', ['半年度', '年度'])])
        self.assertTrue(all(r['cycle'] == '每月' and '不阻止发送' in r['message'] for r in result))
        self.assertEqual(values, before)

    def test_longest_alias_prevents_half_year_and_multi_year_false_positives(self):
        for selected, text in [('半年', '半年度维护，每半年执行，每六个月核对'), ('每两年', '每两年维护，每2年和两年一次'),
                ('每年', '每年度维护，年度和每年年检'), ('冬季保温每日', '冬季保温每日，每天按日巡检')]:
            with self.subTest(selected=selected):
                self.assertEqual(maintenance_cycle_warnings('维保通告', selected, text), [])
        result = maintenance_cycle_warnings('维保通告', '每年', '半年度维护')
        self.assertEqual(result[0]['terms'], ['半年度'])

    def test_only_maintenance_and_only_content_and_reason(self):
        for kind in ('变更通告', '设备检修', '检修通告', '事件通告', '设备调整', '设备轮巡', '上电通告', '下电通告', '', None):
            self.assertEqual(maintenance_cycle_warnings(kind, '每月', '季度', '年度'), [])
            body = notice(kind=kind or '')
            self.assertEqual(maintenance_reminder_text(kind, '每月', body), body)
        body = notice(content='维护过滤器', reason='按计划执行') + '\n【影响】年度运行正常'
        self.assertEqual(maintenance_reminder_text('维保通告', '每月', body), body)

    def test_missing_custom_cycle_or_corrected_content_has_no_warning(self):
        for cycle in ('', '/', None, '每18个月', '按设备说明书'):
            self.assertEqual(maintenance_cycle_warnings('维保通告', cycle, '季度', '年度'), [])
        self.assertEqual(maintenance_cycle_warnings('维保通告', '每月', '月度维护', '按每月计划'), [])
        self.assertEqual(maintenance_cycle_warnings('维保通告', '每季', '季度维护', '按每季计划'), [])

    def test_shared_rules_cover_every_alias_and_do_not_expose_mutable_constants(self):
        for rule in maintenance_cycle_rules():
            for alias in rule['aliases']:
                self.assertEqual(maintenance_cycle_warnings('维保通告', alias, alias, alias), [])
        rules = maintenance_cycle_rules()
        rules[0]['aliases'].append('fixture changed')
        self.assertNotIn('fixture changed', maintenance_cycle_rules()[0]['aliases'])

    def test_qt_robot_warning_never_changes_upload_fields_or_payload(self):
        handler = MaintenanceNoticeHandler()
        for action in ('开始', '更新', '结束'):
            payload = NoticePayload(text=notice(action), maintenance_cycle='每月')
            original = copy.deepcopy(payload)
            fields = handler.build_create_fields(payload) if action == '开始' else handler.build_update_fields(payload)
            title, text, kind, level = handler.build_robot_message(payload)
            self.assertTrue(text.startswith(payload.text))
            self.assertEqual(text.count('【维保周期核对提醒】'), 1)
            self.assertEqual(payload, original)
            self.assertNotIn('【维保周期核对提醒】', str(fields))


class PersonalWarningTests(unittest.TestCase):
    def setUp(self):
        from bin.test_notice_delivery_source_recovery import NoticeDeliverySourceRecoveryTests
        self.fx = NoticeDeliverySourceRecoveryTests('runTest')
        self.fx.setUp()
        self.addCleanup(self.fx.doCleanups)
        self.addCleanup(self.fx.tearDown)

    def prepared(self, action='end', kind='维保通告'):
        return {**self.fx.prepared, 'notice_type': kind, 'action': action, 'maintenance_cycle': '每月',
            'text': notice({'start': '开始', 'update': '更新', 'end': '结束'}[action], kind)}

    def test_start_update_end_send_warning_to_original_recipients_without_new_messages(self):
        from lan_bitable_template_portal import portal_service as portal
        for action in ('start', 'update', 'end'):
            job_id = 'job-' + action
            self.fx.seed_job(job_id)
            prepared = {**self.prepared(action), 'job_id': job_id}
            before = copy.deepcopy(prepared)
            with patch.object(portal, 'send_text_to_open_ids', side_effect=self.fx.send_ok) as send:
                self.assertTrue(self.fx.service.send_action_personal_message(prepared)[0])
                self.assertTrue(self.fx.service.send_action_personal_message(prepared)[0])
            self.assertEqual(send.call_count, 2)
            self.assertEqual([call.args[1] for call in send.call_args_list], [['duty110'], ['operator']])
            self.assertTrue(all(call.args[0].count('【维保周期核对提醒】') == 1 for call in send.call_args_list))
            doc = self.fx.store.get_document('notice_personal_delivery', job_id)
            self.assertEqual(doc['text'], prepared['text'])
            self.assertEqual(doc['delivery_text'], send.call_args.args[0])
            self.assertEqual(prepared, before)

    def test_partial_send_recovery_reuses_warning_and_receipts(self):
        from lan_bitable_template_portal import portal_service as portal
        self.fx.prepared = self.prepared()
        def send_first(text, recipients, **kwargs):
            if recipients == ['duty110']:
                return False, 'not sent', [self.fx.receipt('duty110', ok=False)]
            return self.fx.send_ok(text, recipients, **kwargs)
        self.fx.deliver(send_first)
        original = self.fx.personal_doc()
        self.fx.set_due('notice_personal')
        with patch.object(portal, 'send_text_to_open_ids', side_effect=self.fx.send_ok) as send:
            self.fx.runtime.process_notice_personal_messages()
        self.assertEqual(send.call_count, 1)
        self.assertEqual(send.call_args.args, (original['delivery_text'], ['duty110']))
        self.assertEqual(send.call_args.kwargs['message_uuid'], original['results']['duty110']['message_uuid'])

    def test_staging_failure_keeps_warning_after_job_compaction(self):
        from lan_bitable_template_portal import portal_service as portal
        prepared = self.prepared()
        with patch.object(self.fx.store, 'enqueue_outbox_event', side_effect=OSError('fixture interruption')):
            with self.assertRaises(OSError):
                self.fx.service.send_action_personal_message(prepared)
        self.fx.service._compact_completed_job_locked('job-end')
        snapshot = self.fx.service.get_job('job-end')['_notice_personal_staging_pending']
        self.assertIn('【维保周期核对提醒】', snapshot['delivery_text'])
        snapshot['next_check_at'] = 0
        self.fx.service.mark_job('job-end', _notice_personal_staging_pending=snapshot)
        with patch.object(portal, 'send_text_to_open_ids', side_effect=self.fx.send_ok) as send:
            self.fx.runtime.process_notice_personal_messages()
        self.assertEqual(send.call_count, 2)
        self.assertTrue(all(call.args[0] == snapshot['delivery_text'] for call in send.call_args_list))

    def test_existing_delivery_body_unchanged_after_upgrade(self):
        from lan_bitable_template_portal import portal_service as portal
        self.fx.prepared = {**self.prepared(), 'maintenance_cycle': ''}
        self.fx.deliver(lambda text, ids, **kw: (False, 'not sent', [self.fx.receipt(ids[0], ok=False)]))
        doc = self.fx.personal_doc()
        doc.pop('delivery_text')
        self.fx.store.put_document('notice_personal_delivery', 'job-end', doc)
        with patch.object(portal, 'send_text_to_open_ids', side_effect=self.fx.send_ok) as send:
            self.fx.service.send_action_personal_message(self.prepared())
        self.assertTrue(all(call.args[0] == doc['text'] for call in send.call_args_list))

    def test_other_notice_type_has_no_warning_even_with_cycle(self):
        from lan_bitable_template_portal import portal_service as portal
        prepared = self.prepared(kind='变更通告')
        with patch.object(portal, 'send_text_to_open_ids', side_effect=self.fx.send_ok) as send:
            self.fx.service.send_action_personal_message(prepared)
        self.assertTrue(all(call.args[0] == prepared['text'] for call in send.call_args_list))
