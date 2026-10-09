"""Local native-draft regression; no Feishu calls or business writes."""
import copy
import datetime as dt
import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).parent))
import test_planned_notices as fixtures
from lan_bitable_template_portal import notice_panel_data as data
from lan_bitable_template_portal.portal_service import PortalError


class PanelDataTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.PlannedNativeTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.service = self.fixture.service
        self.current = data.now()
        self.fixture.records[0]['display_fields'].update({
            '计划开始维护时间': self.current.date().isoformat(),
            '计划结束维护时间': (self.current.date() + dt.timedelta(days=2)).isoformat(),
        })
        self.snapshot = {'exists': True, 'meta': {}, 'records': self.fixture.records}
        self.service._state_store.get_source_scope_snapshot.return_value = self.snapshot

    def items(self):
        return data.build_items(self.service, 'A', 'all', [], self.current)

    def test_one_shared_local_snapshot_and_history_without_old_progress(self):
        self.fixture.remember()
        result = self.items()
        self.assertEqual(len(result), 1)
        self.service._state_store.get_source_scope_snapshot.assert_called_once_with('A')
        self.service._workbench_records.assert_called_once()
        self.assertEqual(result[0]['draft']['content'], '历史内容')
        self.assertEqual(result[0]['draft']['execution_party'], '厂维')
        self.assertEqual(result[0]['draft']['progress'], '')
        self.assertNotIn('old', result[0]['draft'].get('target_record_id', ''))

    def test_no_history_is_not_a_ready_to_send_notice(self):
        item = self.items()[0]
        with self.assertRaisesRegex(PortalError, '请填写'):
            data.submission(self.service, item)
        self.assertEqual(item['draft']['content'], '')

    def test_expired_plans_excluded_but_unknown_dates_blocked(self):
        fields = self.fixture.records[0]['display_fields']
        fields.update({'计划开始维护时间': '2000-01-01', '计划结束维护时间': '2000-01-02'})
        self.assertEqual(self.items(), [])
        fields['计划结束维护时间'] = ''
        self.assertTrue(self.items()[0]['blocked'])

    def test_incomplete_source_fails_instead_of_empty(self):
        self.snapshot['meta'] = {'source_refresh_status': {'maintenance': {'status': 'partial'}}}
        with self.assertRaisesRegex(PortalError, '读取尚未完成'):
            self.items()

    def test_ongoing_retains_real_target_and_excludes_progress_for_adjust(self):
        active = {'active_item_id': 'rec-active', 'target_record_id': 'rec-target',
                  'source_record_id': 'rec-source', 'title': 'A楼空调运行模式调整',
                  'building': 'A楼', 'building_codes': ['A'], 'content': '调整模式',
                  'progress': '不应复用', 'status': '开始'}
        item = data.ongoing_item(self.service, 'A', 'adjust', active)
        fields = data.fields_for('adjust', item['draft'], 'update')
        self.assertNotIn('progress', {f['key'] for f in fields})
        self.assertEqual(item['draft']['target_record_id'], 'rec-target')
        self.assertEqual(item['draft']['source_record_id'], 'rec-source')
        self.assertIn('notice_action', {f['key'] for f in fields})

    def test_sop_and_end_photos_not_bypassed(self):
        self.fixture.remember()
        item = self.items()[0]
        item['draft'].update(progress='工程师现场核对', specialty='暖通', work_order_choice='使用网页已配置工单')
        with self.assertRaisesRegex(PortalError, 'SOP'):
            data.submission(self.service, item)
        item['draft']['work_order_choice'] = '本次无需工单'
        item['draft']['notice_action'] = '结束'
        item['action'] = 'update'
        item['fields'] = data.fields_for('maintenance', item['draft'], 'update')
        with patch.object(self.service, '_require_end_site_photo_cumulative', side_effect=PortalError('缺少现场照片')) as guard:
            with self.assertRaisesRegex(PortalError, '现场照片'):
                data.submission(self.service, item)
            guard.assert_called_once()

    def test_plain_preview_does_not_save_memory(self):
        self.fixture.remember()
        item = self.items()[0]
        item['draft'].update(progress='现场已核对', specialty='暖通', work_order_choice='本次无需工单')
        before = copy.deepcopy(self.service._get_record_memory(self.fixture.records[0]))
        body, preview = data.submission(self.service, item)
        self.assertEqual(body['source_record_id'], 'source-1')
        self.assertEqual(body['action'], 'start')
        self.assertIn('现场已核对', preview)
        self.assertEqual(self.service._get_record_memory(self.fixture.records[0]), before)

    def test_events_and_old_card_ui_absent(self):
        with self.assertRaisesRegex(PortalError, 'Qt'):
            data.build_items(self.service, 'A', 'event', [])
        from lan_bitable_template_portal.workbench_lite import render_workbench_lite
        html = render_workbench_lite(payload={}, session={'user': {'name': 'Fixture'}}, scope='A', work_type='maintenance', manual=True)
        self.assertNotIn('/api/notice-cards', html)
        self.assertNotIn('立即发送测试卡片', html)


if __name__ == '__main__':
    unittest.main()
