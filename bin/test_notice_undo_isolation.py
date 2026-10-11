"""Notice rollback must not borrow another execution of the same plan."""
import copy
import sys
import tempfile
import unittest
from html.parser import HTMLParser
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_lan_template_work_status import _TestMaintenancePortalService
from lan_bitable_template_portal.portal_service import STATE_NS_WORK_STATUS
from lan_bitable_template_portal.workbench_lite import render_workbench_lite


class NoticeFormParser(HTMLParser):
    in_notice = False
    record_id = ''

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == 'form':
            self.in_notice = attrs.get('id') == 'lite-notice-form'
        if self.in_notice and tag == 'input' and attrs.get('name') == 'record_id':
            self.record_id = attrs.get('value', '')

    def handle_endtag(self, tag):
        if tag == 'form':
            self.in_notice = False


class NoticeUndoIsolationTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        for module in ('portal_service', 'state_store'):
            patcher = patch(f'lan_bitable_template_portal.{module}.get_data_file_path',
                            side_effect=lambda name: str(Path(temp.name) / name))
            patcher.start()
            self.addCleanup(patcher.stop)
        self.service = _TestMaintenancePortalService()
        self.addCleanup(self.service._state_store.shutdown_write_worker, timeout=2)
        self.ba = dict(work_type='maintenance', notice_type='维保通告', scope='C',
                       source_record_id='rec-shared-source', active_item_id='rec-ba',
                       target_record_id='rec-ba', record_id='rec-ba',
                       title='BA系统软件维护', text='【维保通告】状态：更新\n【名称】BA系统软件维护',
                       building='C楼', building_codes=['C'], status='进行中')
        self.svg = {**self.ba, 'active_item_id': 'rec-svg', 'target_record_id': 'rec-svg',
                    'record_id': 'rec-svg', 'title': 'SVG维护', 'text': 'SVG维护'}

    def test_shared_source_does_not_override_distinct_targets_or_active_ids(self):
        keys = self.service._work_status_identity_keys
        matches = self.service._items_identity_intersects
        self.assertFalse(matches(keys(self.ba), keys(self.svg)))
        self.assertFalse(matches(keys(self.ba), keys({**self.svg, 'active_item_id': 'rec-ba'})))
        self.assertTrue(matches(keys(self.ba), keys({**self.ba, 'active_item_id': 'old-ba'})))
        self.assertTrue(matches(keys(self.ba), {'maintenance:source:rec-shared-source'}))

    def test_snapshot_prefers_target_and_never_falls_back_to_conflicting_target(self):
        row = lambda payload: dict(payload=payload, active_item_id=payload['active_item_id'],
                                   record_id=payload['record_id'], updated_at=10)
        stale = {**self.svg, 'active_item_id': 'rec-ba'}
        self.assertIsNone(self.service._find_qt_active_snapshot(self.ba, [row(stale)]))
        found = self.service._find_qt_active_snapshot(self.ba, [row(stale), row(self.ba)])
        self.assertEqual(found['payload']['title'], 'BA系统软件维护')

    def test_checkpoint_does_not_capture_another_execution(self):
        store = self.service._state_store
        store.upsert_qt_active_item(self.svg)
        store.put_document(STATE_NS_WORK_STATUS, 'C', {'items': [self.svg]})
        checkpoint = self.service.create_notice_undo_checkpoint('update', self.ba, scope='C')
        undo = store.get_notice_undo_action(checkpoint)
        self.assertIsNone(undo['local']['qt_active'])
        self.assertEqual(undo['local']['work_items'], [])

    def test_old_contaminated_checkpoint_does_not_restore_svg_into_ba(self):
        store = self.service._state_store
        store.upsert_qt_active_item(self.svg)
        store.put_document(STATE_NS_WORK_STATUS, 'C', {'items': [self.svg]})
        undo = {**self.ba, 'undo_id': 'undo-old', 'action_type': 'end',
                'identity_keys': sorted(self.service._work_status_identity_keys(self.svg)),
                'context': copy.deepcopy(self.ba),
                'local': {'daily_item': self.svg, 'qt_active': {'payload': self.svg},
                          'work_items': [{'document_key': 'C', 'item': self.svg}]}}
        result = self.service.restore_notice_undo_local(undo, target_record_id='rec-ba')
        self.assertEqual(result['active_payload']['title'], 'BA系统软件维护')
        self.assertEqual(result['active_payload']['target_record_id'], 'rec-ba')
        self.assertEqual(store.get_document(STATE_NS_WORK_STATUS, 'C')['items'][0]['title'], 'SVG维护')

    def test_undo_start_does_not_revive_an_unrelated_old_notice(self):
        undo = {**self.ba, 'undo_id': 'undo-start', 'action_type': 'start',
                'identity_keys': sorted(self.service._work_status_identity_keys(self.svg)),
                'context': copy.deepcopy(self.ba), 'local': {'qt_active': {'payload': self.svg}}}
        result = self.service.restore_notice_undo_local(undo, target_record_id='rec-ba')
        self.assertFalse(result['restored_active'])
        self.assertEqual(result['active_payload'], {})

    def test_missing_selected_notice_never_opens_first_plan_instead(self):
        for selection in ({'active_item_id': 'rec-deleted'}, {'record_id': 'rec-missing'}):
            with self.subTest(selection=selection):
                html = render_workbench_lite(
                    payload={'records': [{'record_id': 'rec-other', 'work_type': 'maintenance',
                                          'display_fields': {'楼栋': 'C楼', '维护总项': 'SVG维护'}}],
                             'ongoing': [], 'daily_summary': {'stats': {}}},
                    session={'user': {'name': '测试'}}, scope='C', work_type='maintenance', **selection)
                parser = NoticeFormParser()
                parser.feed(html)
                self.assertNotEqual(parser.record_id, 'rec-other')


if __name__ == '__main__':
    unittest.main()
