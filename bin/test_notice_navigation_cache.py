"""Local-only notice reads; no cloud calls or business writes."""
import copy
import ast
import asyncio
import hashlib
import json
import sys
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lan_bitable_template_portal.portal_service import MaintenancePortalService
from lan_bitable_template_portal.workbench_lite import _draft_from_record
from test_lighthouse_notice_command_regression import VALID_STARTS


class NoticeNavigationCacheTests(unittest.TestCase):
    def service(self):
        service = object.__new__(MaintenancePortalService)
        service._jobs_lock = threading.RLock()
        service._jobs = {}
        return service

    def test_raw_clipboard_notices_recover_fields_without_cloud_queries(self):
        samples = {
            'maintenance': '【维保通告】状态：开始\n【名称】E楼维保\n【位置】E楼\n【内容】刷新电池\n【原因】周期\n【影响】无',
            'change': '【变更通告】状态：开始\n【名称】E楼变更\n【位置】E楼\n【内容】换电池',
            'repair': '【设备检修】状态：开始\n【标题】E楼设备检修\n【地点】E楼\n【维修设备】空调\n【故障现象】异响',
            'power': '【下电通告】状态：开始\n【名称】E楼下电\n【柜号】E-402包间B15\n【数量】1',
            'polling': '【设备轮巡】状态：开始\n【标题】E楼轮巡\n【设备】水泵\n【内容】轮巡',
            'adjust': '【设备调整】状态：开始\n【名称】E楼调整\n【位置】E楼\n【内容】调整参数',
        }
        keys = {'maintenance': 'content', 'change': 'content', 'repair': 'repair_device',
                'power': 'cabinet', 'polling': 'device', 'adjust': 'content'}
        for kind, text in samples.items():
            with self.subTest(kind=kind):
                record = {'active_item_id': 'active-1', 'work_type': kind, 'text': text}
                before = copy.deepcopy(record)
                draft = _draft_from_record(record, work_type=kind)
                self.assertTrue(draft[keys[kind]])
                self.assertEqual(record, before)
                record[keys[kind]] = 'Explicit local field'
                self.assertEqual(_draft_from_record(record, work_type=kind)[keys[kind]], 'Explicit local field')

    def test_frozen_job_fields_are_owner_and_target_bound(self):
        service = self.service()
        request = {**VALID_STARTS['maintenance'], '_auth_open_id': 'owner', 'work_type': 'maintenance',
                   'active_item_id': 'active-1', 'target_record_id': 'rec-1', 'title': 'Frozen title'}
        service._jobs['job-1'] = {'request': request, 'accepted_at': 20, 'phase': 'uploading'}
        item = {'work_type': 'maintenance', 'active_item_id': 'active-1', 'target_record_id': 'rec-1', '_local_updated_at': 10}
        self.assertEqual(service.submitted_notice_draft(item, 'owner')['title'], 'Frozen title')
        self.assertEqual(service.submitted_notice_draft(item, 'other'), {})
        self.assertEqual(service.submitted_notice_draft({**item, 'target_record_id': 'rec-other'}, 'owner'), {})
        self.assertEqual(service.submitted_notice_draft({**item, 'work_type': 'event'}, 'owner'), {})
        self.assertEqual(service.submitted_notice_draft({**item, '_local_updated_at': 30}, 'owner')['title'], 'Frozen title')
        service._jobs['job-1']['phase'] = 'failed'
        self.assertEqual(service.submitted_notice_draft({**item, '_local_updated_at': 30}, 'owner'), {})
        service._jobs['job-1'] = {'retry_request': request, 'accepted_at': 20, 'phase': 'failed'}
        self.assertEqual(service.submitted_notice_draft(item, 'owner')['title'], 'Frozen title')
        service._jobs['job-2'] = {'request': {**request, 'title': 'Latest title'}, 'accepted_at': 25, 'phase': 'remote_written'}
        self.assertEqual(service.submitted_notice_draft(item, 'owner')['title'], 'Latest title')
        service._jobs['job-2']['phase'] = 'success'
        self.assertEqual(service.submitted_notice_draft(item, 'owner'), {})

    def test_ongoing_only_skips_unrelated_reads_but_stats_keep_plan_counts(self):
        service = self.service()
        row = {'record_id': 'plan-1', 'work_type': 'maintenance'}
        service.ensure_snapshot_loaded = Mock()
        service._load_warnings = []
        service._last_loaded_at = 'fixture'
        service._state_store = SimpleNamespace(get_source_scope_snapshot=lambda scope: {'exists': True})
        service._project_ongoing_items = Mock(return_value=[])
        service._workbench_records = Mock(return_value=[row])
        service._linked_zhihang_record_ids = Mock(return_value=set())
        service._filter_zhihang_change_records = Mock(return_value=[])
        service._pending_work_type_counts = lambda records, _: {'maintenance': len(records)}
        service._record_work_type = lambda record: record['work_type']
        service.get_daily_summary = Mock(return_value={'stats': {}, 'items': []})
        service._annotate_undo_items = Mock(side_effect=lambda rows, **_: rows)
        service._work_type_counts = Mock(return_value={})
        service._payload_version = Mock(return_value='fixture')
        service._scope_label = Mock(return_value='E楼')
        service._source_cache_ttl_seconds = Mock(return_value=60)
        service._current_load_warnings = Mock(return_value=[])
        service._maintenance_options_for_records = Mock(return_value=[])
        service._recent_month_labels = Mock(return_value=['10月'])
        service._sorted_unique_work_specialties = Mock(return_value=[])
        result = service.query_records(scope='E', month='10月', sections={'ongoing'}, ongoing_items_authoritative=True)
        self.assertEqual(result['ongoing'], [])
        service._workbench_records.assert_not_called()
        service._filter_zhihang_change_records.assert_not_called()
        service.get_daily_summary.assert_not_called()
        service._annotate_undo_items.assert_not_called()
        result = service.query_records(scope='E', month='10月', sections={'stats'}, ongoing_items_authoritative=True)
        self.assertEqual(result['record_type_counts']['maintenance'], 1)
        self.assertEqual(result['records'], [])
        service._workbench_records.assert_called_once()


class NoticeStreamResponsivenessTests(unittest.IsolatedAsyncioTestCase):
    async def test_slow_sqlite_reads_do_not_block_page_or_health_requests(self):
        tree = ast.parse((Path(__file__).parent / 'clipflow_backend/main.py').read_text(encoding='utf-8-sig'))
        method = next(node for node in ast.walk(tree) if isinstance(node, ast.AsyncFunctionDef) and node.name == '_qt_active_items_stream')
        read_threads = []
        def slow_read(value):
            def read(*args, **kwargs):
                read_threads.append(threading.get_ident())
                time.sleep(0.06)
                return value
            return read
        store = SimpleNamespace(
            get_ongoing_snapshot_meta=slow_read({'snapshot_id': 'local', 'count': 1}),
            active_source_snapshot_meta=slow_read({'snapshot_id': 'source'}),
            qt_active_items_meta=slow_read({'active': 1}),
            get_ongoing_snapshot=slow_read({'snapshot_id': 'local', 'count': 1, 'items': []}),
            qt_active_items_stats=slow_read({'active': 1, 'checked_at': 'volatile'}))
        controller = SimpleNamespace(_current_session=lambda _: {}, _authorized_scope_or_error=lambda *args: 'E',
            _register_sse=lambda *args: ('key', 1), _register_qt_active_stream_waker=lambda _: asyncio.Event(),
            _sse_active=lambda *args: True, _unregister_qt_active_stream_waker=Mock(), _unregister_sse=Mock(),
            _scoped_ongoing_signature=lambda *args: ('ongoing', 1),
            _scoped_qt_active_signature=slow_read(('qt', 1)),
            _scoped_qt_active_identities=slow_read([{'active_item_id': 'same-id'}]))
        namespace = {'asyncio': asyncio, 'json': json, 'time': time, 'hashlib': hashlib, 'Request': object,
                     'AsyncIterator': object, 'PortalRuntime': SimpleNamespace(state_store=store),
                     '_env_float': lambda name, default, **kwargs: default}
        module = ast.Module(body=[ast.ImportFrom(module='__future__', names=[ast.alias(name='annotations')], level=0), method], type_ignores=[])
        exec(compile(ast.fix_missing_locations(module), '<notice-stream>', 'exec'), namespace)
        request = SimpleNamespace(query_params={'scope': 'E'}, is_disconnected=AsyncMock(return_value=False))
        stream = namespace['_qt_active_items_stream'](controller, request)
        ticks = []
        async def heartbeat():
            while True:
                ticks.append(time.perf_counter())
                await asyncio.sleep(0.01)
        pulse = asyncio.create_task(heartbeat())
        await asyncio.sleep(0)
        try:
            result = (await anext(stream)).decode()
            self.assertIn('event: qt_active_items', result)
            self.assertIn('"scope": "E"', result)
            self.assertIn('same-id', result)
            self.assertNotIn('volatile', result)
            self.assertTrue(read_threads)
            self.assertTrue(all(thread != threading.get_ident() for thread in read_threads))
            self.assertGreater(len(ticks), 10)
        finally:
            pulse.cancel()
            await asyncio.gather(pulse, return_exceptions=True)
            await stream.aclose()
        controller._unregister_sse.assert_called_once_with('key', 1)


if __name__ == '__main__':
    unittest.main()
