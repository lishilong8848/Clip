"""Notice defaults and memories use isolated stores and never send messages."""
import copy
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from bin.test_notice_memory_history import _HistoryMemoryService, _source_record
from lan_bitable_template_portal.portal_service import MaintenancePortalService, STATE_NS_MEMORY
from lan_bitable_template_portal.state_store import LanPortalStateStore
from lan_bitable_template_portal.workbench_lite import _draft_from_record, _detail_form


class NoticeMemoryIsolationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        store = LanPortalStateStore(root / 'state.sqlite3')
        with (patch('lan_bitable_template_portal.portal_service.LanPortalStateStore', return_value=store),
              patch('lan_bitable_template_portal.portal_service.get_data_file_path', side_effect=lambda name: str(root / name)),
              patch.object(MaintenancePortalService, '_ensure_signature_crypto_ready')):
            self.service = _HistoryMemoryService(root / 'state.sqlite3')
        self.service._memory_dir = root / 'memory'

    def tearDown(self):
        self.service._state_store.shutdown_write_worker(timeout=2)
        self.temp.cleanup()

    def source(self, name='冷站Y型过滤器检查', building='C楼', cycle='每月', work='maintenance'):
        return _source_record('recFilter', work, {'楼栋': building, '维护总项': name,
            '名称': name, '标题/补充内容': name if work == 'change' else '', '维护周期': cycle})

    def remember(self, name='冷站Y型过滤器检查', building='C楼', cycle='每月', work='maintenance'):
        self.service._remember_draft_fields(building=building, maintenance_total=name, item_name=name,
            work_type=work, maintenance_cycle=cycle, location='冷站', content='检查过滤器', reason='例行检查',
            impact='无影响', extra_fields={'execution_party': '自维'})

    def test_full_name_building_type_and_cycle_are_all_isolated(self):
        self.remember()
        self.assertEqual(self.service._get_record_memory(self.source())['execution_party'], '自维')
        for source in (self.source(name='柴发空载测试'), self.source(building='A楼'),
                       self.source(cycle='每季'), self.source(work='change')):
            with self.subTest(source=source):
                self.assertFalse(self.service._get_record_memory(source).get('content'))
        self.assertEqual(self.service._get_record_memory(self.source(cycle='月度'))['content'], '检查过滤器')

    def test_corrupt_payload_identity_does_not_inherit_its_dictionary_key(self):
        self.remember()
        original = self.service._state_store.get_document(STATE_NS_MEMORY, 'C楼')
        key = self.service._memory_item_key('maintenance', '冷站Y型过滤器检查', '每月')
        for wrong in ({'memory_name': '柴发空载测试'}, {'building': 'A楼'},
                      {'work_type': 'change'}, {'maintenance_cycle': '每季'}):
            payload = copy.deepcopy(original)
            payload['items'][key].update(wrong)
            self.service._state_store.put_document(STATE_NS_MEMORY, 'C楼', payload)
            self.assertFalse(self.service._get_record_memory(self.source()).get('content'), wrong)

    def test_current_and_user_fields_win_for_all_non_event_channels(self):
        for work in ('maintenance', 'change', 'repair', 'polling', 'adjust', 'power'):
            record = {'record_id': 'recFilter', 'work_type': work, 'title': '冷站Y型过滤器检查',
                'location': '本楼现场位置', 'content': '当前内容', 'reason': '当前原因',
                'memory': {'location': '柴发位置', 'content': '柴发空载测试', 'reason': '柴发原因',
                           'impact': '无影响', 'execution_party': '自维'},
                'submitted_draft': {'content': '用户手填内容', 'impact': ''}}
            before = copy.deepcopy(record)
            draft = _draft_from_record(record, work_type=work)
            self.assertEqual(draft['location'], '本楼现场位置')
            self.assertEqual(draft['reason'], '当前原因')
            self.assertEqual(draft['content'], '用户手填内容')
            self.assertEqual(draft['impact'], '')
            self.assertEqual(draft['execution_party'], '自维')
            self.assertEqual(record, before)
            draft = _draft_from_record({**record, 'active_item_id': 'recActive'}, work_type=work)
            self.assertEqual(draft['content'], '用户手填内容')

    def test_import_and_restart_preserve_execution_party(self):
        source = {'work_type': 'maintenance', 'building': 'C楼', 'memory_name': '冷站Y型过滤器检查', 'maintenance_cycle': '每月'}
        result = self.service.save_historical_notice_memory_matches(matches=[{'source_item': source,
            'selected': True, 'fields': {'location': '冷站', 'content': '检查过滤器', 'execution_party': '厂维'}}])
        self.assertEqual(result['saved_count'], 1)
        self.assertEqual(self.service._get_record_memory(self.source())['execution_party'], '厂维')
        self.service._state_store = LanPortalStateStore(Path(self.temp.name) / 'state.sqlite3')
        draft = _draft_from_record(self.service._serialize_record(self.source()))
        self.assertEqual(draft['execution_party'], '厂维')

    def test_candidate_import_checks_server_scan_not_browser_identity(self):
        source = {'id': 'maintenance:recFilter', 'work_type': 'maintenance', 'building': 'C楼',
            'building_codes': ['C'], 'memory_name': '冷站Y型过滤器检查', 'canonical_title': '冷站y型过滤器检查', 'maintenance_cycle': '每月'}
        valid = {**source, 'id': 'maintenance:recHistory'}
        for wrong in ({'canonical_title': '柴发空载测试'}, {'building_codes': ['A']},
                      {'work_type': 'change'}, {'maintenance_cycle': '每季'}):
            self.service._history_memory_scan = {'at': time.monotonic(), 'sources': {source['id']: source},
                'candidates': {valid['id']: {**valid, **wrong}}}
            result = self.service.save_historical_notice_memory_matches(matches=[{'source_item': source,
                'candidate_id': valid['id'], 'field_origin': 'candidate', 'fields': {'content': '不应写入'}}])
            self.assertEqual(result['saved_count'], 0, wrong)
        self.service._history_memory_scan['candidates'][valid['id']] = valid
        result = self.service.save_historical_notice_memory_matches(matches=[{'source_item': {**source, 'building': 'A楼'},
            'candidate_id': valid['id'], 'field_origin': 'candidate', 'fields': {'content': '已核对', 'execution_party': '自维'}}])
        self.assertEqual(result['saved_count'], 1)
        self.assertEqual(self.service._get_record_memory(self.source())['content'], '已核对')
        self.assertFalse(self.service._get_record_memory(self.source(building='A楼')).get('content'))
        self.service._history_memory_scan['at'] -= 3601
        result = self.service.save_historical_notice_memory_matches(matches=[{'source_item': source,
            'candidate_id': valid['id'], 'field_origin': 'candidate', 'fields': {'content': '过期'}}])
        self.assertEqual(result['saved_count'], 0)
        self.assertEqual(self.service._get_record_memory(self.source())['content'], '已核对')

    def test_form_context_changes_with_source_building_cycle_and_type(self):
        import re
        def context(record, work='maintenance'):
            html = _detail_form(record=record, ongoing_item=None, scope='C', work_type=work, manual=False)
            return re.search(r'data-draft-context="([a-f0-9]+)"', html).group(1)
        baseline = context(self.source())
        self.assertEqual(context(self.source()), baseline)
        for source in (self.source(name='柴发空载测试'), self.source(building='A楼'), self.source(cycle='每季')):
            self.assertNotEqual(context(source), baseline)
        self.assertNotEqual(context(self.source(work='change'), 'change'), baseline)


if __name__ == '__main__':
    unittest.main()
