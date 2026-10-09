# -*- coding: utf-8 -*-
"""Isolated regression checks for plan-convergence manual change checks and
cached result persistence (local-only listing, manual persist, overlay rules,
no cross-type id collisions, no telemetry/Feishu on reads)."""
import copy
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_plan_convergence import FakeController, FakeRuntime
from lan_bitable_template_portal import plan_convergence_auth as auth
from lan_bitable_template_portal import plan_convergence_maintenance as maintenance
from lan_bitable_template_portal.plan_convergence import PlanConvergenceService
from lan_bitable_template_portal.plan_convergence_routes import install_plan_convergence_routes
from lan_bitable_template_portal.state_store import LanPortalStateStore


class _Provider:
    def __init__(self, items):
        self.items = items

    def __call__(self, scope):
        return copy.deepcopy(self.items)


def _change_item(record_id='recChange', title='B-124-BAS-101/103/104/105/401、B-150-BAS-101/102',
                 building='B楼', location='B-124', content='更换 BAS 前端控制器', device=None):
    return {
        'active_item_id': record_id,
        'target_record_id': record_id,
        'work_type': 'change',
        'notice_type': '变更通告',
        'status': '开始',
        'title': title,
        'building': building,
        'building_codes': ['B'],
        'location': location,
        'device': device if device is not None else title,
        'content': content,
        'specialty': '楼宇自控',
    }


def _repair_item(record_id='recRepair'):
    return {
        'active_item_id': record_id,
        'target_record_id': record_id,
        'work_type': 'repair',
        'notice_type': '设备检修',
        'status': '开始',
        'title': 'B楼空调检修',
        'building': 'B楼',
        'building_codes': ['B'],
        'location': 'B-124',
        'repair_device': 'B-124-CRAC-01',
        'repair_fault': '压力传感器失效',
        'specialty': '暖通',
    }


def _blocks(pairs):
    return {'items': [{'blockId': bid, 'status': 1, 'blockName': name} for bid, name in pairs]}


def _block_detail():
    return {'alarmBlockDetailResultList': []}


class ChangeCheckPersistenceTests(unittest.TestCase):
    def setUp(self):
        self.previous_store = auth._store
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        path = Path(self.temp.name) / 'state.sqlite3'
        self.store = LanPortalStateStore(path)
        self.addCleanup(self.store.shutdown_write_worker)

    def tearDown(self):
        auth.bind_store(self.previous_store)

    def _service(self, items):
        return PlanConvergenceService(self.store, _Provider(items))

    def _route_client(self, items):
        controller = FakeController()
        controller.ongoing = items
        runtime = FakeRuntime()
        runtime.state_store = self.store
        app = FastAPI()
        install_plan_convergence_routes(app, controller, runtime)
        client = TestClient(app)
        self.addCleanup(client.close)
        return client

    def test_change_route_list_and_manual_check_persist_after_reopen(self):
        items = [_change_item()]
        client = self._route_client(items)
        with patch('lan_bitable_template_portal.plan_convergence_auth.request',
                   side_effect=AssertionError('must not request remote on listing')):
            listed = client.get('/api/plan-convergence/change/records')
        self.assertEqual(listed.status_code, 200)
        rows = listed.json()['data']
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['record_id'], 'recChange')
        self.assertNotIn('check_status', rows[0])

        with patch.object(PlanConvergenceService, 'blocks', return_value=_blocks([('b1', 'B楼 B-124空调屏蔽')])), \
                patch.object(PlanConvergenceService, 'block', return_value=_block_detail()):
            checked = client.post('/api/plan-convergence/change/check', json={'record_id': 'recChange'})
        self.assertEqual(checked.status_code, 200)
        data = checked.json()['data']
        self.assertEqual(data['stats']['change'], 1)
        row = data['records'][0]
        self.assertEqual(row['check_status'], 'ready')
        self.assertEqual(row['check_source'], 'manual')
        self.assertIsInstance(row['checked_at'], (int, float))
        self.assertEqual([hit['blockId'] for hit in row['hits']], ['b1'])

        # Reopen the store on a fresh service instance -> cached result returned
        # with no Zhihang/Feishu request.
        durable = self.store.get_document('plan_convergence_checks', 'change:recChange')
        self.assertIsNotNone(durable)
        self.assertEqual(durable['check_source'], 'manual')
        self.assertEqual([hit['blockId'] for hit in durable['hits']], ['b1'])
        recreated = PlanConvergenceService(LanPortalStateStore(Path(self.temp.name) / 'state.sqlite3'),
                                           _Provider(items))
        self.addCleanup(recreated.close)
        with patch('lan_bitable_template_portal.plan_convergence_auth.request',
                   side_effect=AssertionError('must not request remote on reopen')):
            overlay = recreated.change_records('recChange')[0]
        self.assertEqual(overlay['check_status'], 'ready')
        self.assertEqual(overlay['check_source'], 'manual')
        self.assertEqual([hit['blockId'] for hit in overlay['hits']], ['b1'])

    def test_change_unmatched_persists_empty_hits_still_ready(self):
        item = _change_item()
        service = self._service([item])
        with patch.object(PlanConvergenceService, 'blocks', return_value=_blocks([('e1', 'E楼 E-101空调屏蔽')])), \
                patch.object(PlanConvergenceService, 'block', return_value=_block_detail()):
            result = service.change_check('recChange')
        row = result['records'][0]
        self.assertEqual(row['hits'], [])
        self.assertEqual(row['check_status'], 'ready')
        cached = self.store.get_document('plan_convergence_checks', 'change:recChange')
        self.assertEqual(cached['hits'], [])
        self.assertEqual(cached['check_status'], 'ready')

    def test_change_cross_building_excluded_same_b_room_matches(self):
        items = [_change_item()]
        service = self._service(items)
        blocks = _blocks([
            ('b1', 'B楼 B-124空调屏蔽'),
            ('e1', 'E楼 E-101空调屏蔽'),
        ])
        with patch.object(PlanConvergenceService, 'blocks', return_value=blocks), \
                patch.object(PlanConvergenceService, 'block', return_value=_block_detail()):
            result = service.change_check('recChange')
        hits = result['records'][0]['hits']
        self.assertEqual([hit['blockId'] for hit in hits], ['b1'])
        self.assertIn('B-124', hits[0]['reason'])

    def test_record_source_changed_invalidates_cache_to_stale(self):
        service = self._service([_change_item()])
        with patch.object(PlanConvergenceService, 'blocks', return_value=_blocks([('b1', 'B楼 B-124空调屏蔽')])), \
                patch.object(PlanConvergenceService, 'block', return_value=_block_detail()):
            service.change_check('recChange')
        cached = self.store.get_document('plan_convergence_checks', 'change:recChange')
        old_checked_at = cached['checked_at']

        changed = _change_item(title='B-150-BAS-202/203变更（标题已改）', location='B-150', content='更换网关')
        service2 = self._service([changed])
        overlay = service2.change_records('recChange')[0]
        self.assertEqual(overlay['check_status'], 'stale')
        self.assertEqual(overlay['check_source'], 'manual')
        self.assertEqual(overlay['checked_at'], old_checked_at)
        self.assertNotIn('hits', overlay)

    def test_change_and_repair_ids_do_not_collide(self):
        repair = _repair_item('recShared')
        change = _change_item('recShared')
        service = self._service([repair, change])
        with patch.object(PlanConvergenceService, 'blocks', return_value=_blocks([('b1', 'B楼 B-124空调屏蔽')])), \
                patch.object(PlanConvergenceService, 'block', return_value=_block_detail()):
            service.change_check('recShared')
        self.assertIsNotNone(self.store.get_document('plan_convergence_checks', 'change:recShared'))
        self.assertIsNone(self.store.get_document('plan_convergence_checks', 'repair:recShared'))
        # Maintenance listing never sees the change-only cached result.
        with patch('lan_bitable_template_portal.plan_convergence_auth.request',
                   side_effect=AssertionError('maintenance list must stay local')):
            maintenance_rows = service.maintenance_records('recShared')
        self.assertEqual(len(maintenance_rows), 1)
        self.assertNotIn('check_source', maintenance_rows[0])

    def test_source_gone_hides_cache_even_if_persisted(self):
        service = self._service([_change_item()])
        with patch.object(PlanConvergenceService, 'blocks', return_value=_blocks([('b1', 'B楼 B-124空调屏蔽')])), \
                patch.object(PlanConvergenceService, 'block', return_value=_block_detail()):
            service.change_check('recChange')
        self.assertIsNotNone(self.store.get_document('plan_convergence_checks', 'change:recChange'))
        # Source ended/deleted -> no longer in ongoing projection -> hidden even
        # though a persisted cache document still exists.
        removed = self._service([])
        self.assertEqual(removed.change_records(), [])
        with self.assertRaises(FileNotFoundError):
            removed.change_records('recChange')
        self.assertIsNotNone(self.store.get_document('plan_convergence_checks', 'change:recChange'))

    def test_failed_and_partial_remote_check_preserve_previous_success(self):
        service = self._service([_change_item()])
        with patch.object(PlanConvergenceService, 'blocks', return_value=_blocks([('b1', 'B楼 B-124空调屏蔽')])), \
                patch.object(PlanConvergenceService, 'block', return_value=_block_detail()):
            service.change_check('recChange')
        before = self.store.get_document('plan_convergence_checks', 'change:recChange')
        self.assertEqual([h['blockId'] for h in before['hits']], ['b1'])

        # A partial remote failure (a detail read fails) must raise and must not
        # overwrite the last successful result with empty hits.
        with patch.object(PlanConvergenceService, 'blocks', return_value=_blocks([('b1', 'B楼 B-124空调屏蔽')])), \
                patch.object(PlanConvergenceService, 'block', side_effect=ValueError('broken detail')):
            with self.assertRaisesRegex(ValueError, 'broken detail'):
                service.change_check('recChange')
        after = self.store.get_document('plan_convergence_checks', 'change:recChange')
        self.assertEqual(after, before)
        self.assertEqual([h['blockId'] for h in after['hits']], ['b1'])

        # The previously saved ready result keeps surfacing after the failure.
        overlay = service.change_records('recChange')[0]
        self.assertEqual(overlay['check_status'], 'ready')
        self.assertEqual([h['blockId'] for h in overlay['hits']], ['b1'])

    def test_local_get_never_requests_remote_after_cache(self):
        service = self._service([_change_item()])
        with patch.object(PlanConvergenceService, 'blocks', return_value=_blocks([('b1', 'B楼 B-124空调屏蔽')])), \
                patch.object(PlanConvergenceService, 'block', return_value=_block_detail()):
            service.change_check('recChange')
        with patch('lan_bitable_template_portal.plan_convergence_auth.request',
                   side_effect=AssertionError('local GET must not touch remote')):
            rows = service.change_records('recChange')
        self.assertEqual(rows[0]['check_status'], 'ready')

    def test_manual_vs_auto_precedence_by_timestamp(self):
        rid = 'recRepair'
        record = maintenance.ongoing_records([_repair_item(rid)])[0]
        now = time.time()
        # Auto ready (older) in notice_plan_checks.
        self.store.put_document('notice_plan_checks', 'latest:' + rid, {'job_id': 'auto-job'})
        self.store.put_document('notice_plan_checks', 'auto-job', {
            'id': 'auto-job', 'status': 'ready', 'checked_at': now - 100,
            'record': record,
            'result': {'records': [{'hits': [{'blockId': 'auto-hit'}]}]},
        })
        # Newer manual ready in plan_convergence_checks.
        self.store.put_document('plan_convergence_checks', 'repair:' + rid, {
            'work_type': 'repair', 'source': 'manual', 'check_source': 'manual',
            'check_status': 'ready', 'checked_at': now,
            'hits': [{'blockId': 'manual-hit'}],
            'fingerprint': maintenance.business_fingerprint(record),
        })
        service = self._service([_repair_item(rid)])
        row = service.maintenance_records(rid)[0]
        self.assertEqual(row['check_source'], 'manual')
        self.assertEqual(row['check_status'], 'ready')
        self.assertEqual([h['blockId'] for h in row['hits']], ['manual-hit'])
        self.assertEqual(row['auto_check_status'], 'ready')
        self.assertEqual(row['auto_checked_at'], now - 100)

        # Flip timestamps: newer auto ready wins over older manual.
        self.store.put_document('plan_convergence_checks', 'repair:' + rid, {
            'work_type': 'repair', 'source': 'manual', 'check_source': 'manual',
            'check_status': 'ready', 'checked_at': now - 200,
            'hits': [{'blockId': 'manual-hit'}],
            'fingerprint': maintenance.business_fingerprint(record),
        })
        self.store.put_document('notice_plan_checks', 'auto-job', {
            **self.store.get_document('notice_plan_checks', 'auto-job'), 'checked_at': now,
        })
        row = service.maintenance_records(rid)[0]
        self.assertEqual(row['check_source'], 'auto')
        self.assertEqual([h['blockId'] for h in row['hits']], ['auto-hit'])

    def test_auto_ready_stale_when_source_changed_no_false_hits(self):
        rid = 'recRepair'
        snapshot = maintenance.ongoing_records([_repair_item(rid)])[0]
        self.store.put_document('notice_plan_checks', 'latest:' + rid, {'job_id': 'auto-job'})
        self.store.put_document('notice_plan_checks', 'auto-job', {
            'id': 'auto-job', 'status': 'ready', 'checked_at': 123,
            'record': snapshot,
            'result': {'records': [{'hits': [{'blockId': 'auto-hit'}]}]},
        })
        changed = _repair_item(rid)
        changed['title'] = 'B楼空调检修（标题已改）'
        service = self._service([changed])
        row = service.maintenance_records(rid)[0]
        self.assertEqual(row['auto_check_status'], 'ready')
        self.assertEqual(row['check_source'], 'auto')
        self.assertEqual(row['check_status'], 'stale')
        self.assertEqual(row['checked_at'], 123)
        self.assertNotIn('hits', row)

    def test_auto_failed_or_pending_exposes_no_hits(self):
        rid = 'recRepair'
        snapshot = maintenance.ongoing_records([_repair_item(rid)])[0]
        for status in ('failed', 'pending'):
            with self.subTest(status=status):
                store = LanPortalStateStore(Path(self.temp.name) / ('state-' + status + '.sqlite3'))
                self.addCleanup(store.shutdown_write_worker)
                store.put_document('notice_plan_checks', 'latest:' + rid, {'job_id': 'auto-job'})
                store.put_document('notice_plan_checks', 'auto-job', {
                    'id': 'auto-job', 'status': status, 'checked_at': 123,
                    'record': snapshot, 'result': {'records': [{'hits': [{'blockId': 'leak'}]}]},
                })
                service = PlanConvergenceService(store, _Provider([_repair_item(rid)]))
                self.addCleanup(service.close)
                row = service.maintenance_records(rid)[0]
                self.assertEqual(row['auto_check_status'], status)
                self.assertNotIn('check_status', row)
                self.assertNotIn('hits', row)

    def test_stats_keys_match_original_contract(self):
        service = self._service([_repair_item('recR'), _change_item('recC')])
        with patch.object(PlanConvergenceService, 'blocks', return_value=_blocks([('b1', 'B楼 B-124空调屏蔽')])), \
                patch.object(PlanConvergenceService, 'block', return_value=_block_detail()):
            maintenance_row = service.maintenance_check('recR')
            change_row = service.change_check('recC')
        self.assertEqual(set(maintenance_row['stats']),
                         {'maintenance', 'blocks', 'matched_records', 'orphan_blocks'})
        self.assertEqual(maintenance_row['stats']['maintenance'], 1)
        self.assertNotIn('repair', maintenance_row['stats'])
        self.assertEqual(set(change_row['stats']),
                         {'change', 'blocks', 'matched_records', 'orphan_blocks'})
        self.assertEqual(change_row['stats']['change'], 1)

    def test_native_change_projection_maps_direct_fields_and_keeps_b_building(self):
        item = _change_item(title='B-124-BAS-101/103/104/105/401、B-150-BAS-101/102',
                            content='更换 BAS 前端控制器', device='B-124-BAS-CTRL-01')
        rec = maintenance.ongoing_records([item], work_type='change')[0]
        self.assertEqual(rec['device'], 'B-124-BAS-CTRL-01')
        self.assertEqual(rec['fault'], '更换 BAS 前端控制器')
        self.assertEqual(rec['building'], 'B楼')
        self.assertIn('B-124', rec['rooms'])
        self.assertIn('B-150', rec['rooms'])

    def test_change_projection_falls_back_to_direct_alias_fields_change_device_change_content(self):
        item = _change_item()
        # Native projection fields absent; the direct alias keys carry the device/content.
        item.pop('device', None)
        item.pop('content', None)
        item['change_device'] = 'B-124-BAS-CTRL-01'
        item['change_content'] = '更换 BAS 前端控制器'
        rec = maintenance.ongoing_records([item], work_type='change')[0]
        self.assertEqual(rec['device'], 'B-124-BAS-CTRL-01')
        self.assertEqual(rec['fault'], '更换 BAS 前端控制器')

    def test_repair_projection_prefers_nested_field_over_top_level_chinese_alias(self):
        # repair_device/repair_fault are absent at top level. The nested
        # fields['维修设备'/'维修故障'] carry the real value, while conflicting
        # Chinese alias keys exist directly on the item. The restored value()
        # helper must keep using the nested field (repair precedence unchanged).
        item = dict(_repair_item('recRepair'))
        item.pop('repair_device', None)
        item.pop('repair_fault', None)
        item['维修设备'] = 'WRONG-top-level-device'
        item['维修故障'] = 'WRONG-top-level-fault'
        item['fields'] = {'维修设备': 'RIGHT-nested-device', '维修故障': 'RIGHT-nested-fault'}
        rec = maintenance.ongoing_records([item])[0]
        self.assertEqual(rec['device'], 'RIGHT-nested-device')
        self.assertEqual(rec['fault'], 'RIGHT-nested-fault')

    def test_content_change_invalidates_manual_cache_to_stale(self):
        service = self._service([_change_item()])
        with patch.object(PlanConvergenceService, 'blocks', return_value=_blocks([('b1', 'B楼 B-124空调屏蔽')])), \
                patch.object(PlanConvergenceService, 'block', return_value=_block_detail()):
            service.change_check('recChange')
        cached = self.store.get_document('plan_convergence_checks', 'change:recChange')
        old_checked_at = cached['checked_at']

        changed = _change_item(content='更换消防主机')
        service2 = self._service([changed])
        overlay = service2.change_records('recChange')[0]
        self.assertEqual(overlay['check_status'], 'stale')
        self.assertEqual(overlay['checked_at'], old_checked_at)
        self.assertNotIn('hits', overlay)

    def test_auto_ready_with_missing_result_is_failed_without_hits(self):
        rid = 'recRepair'
        snapshot = maintenance.ongoing_records([_repair_item(rid)])[0]
        for label, result in [('empty-result', {}), ('result-none', None), ('result-list', [{'records': [{'hits': []}]}]),
                              ('empty-records', {'records': []}), ('none-hits', {'records': [{'hits': None}]})]:
            with self.subTest(label=label):
                store = LanPortalStateStore(Path(self.temp.name) / ('state-' + label + '.sqlite3'))
                self.addCleanup(store.shutdown_write_worker)
                store.put_document('notice_plan_checks', 'latest:' + rid, {'job_id': 'auto-job'})
                store.put_document('notice_plan_checks', 'auto-job', {
                    'id': 'auto-job', 'status': 'ready', 'checked_at': 123,
                    'record': snapshot, 'result': result,
                })
                service = PlanConvergenceService(store, _Provider([_repair_item(rid)]))
                self.addCleanup(service.close)
                row = service.maintenance_records(rid)[0]
                self.assertEqual(row['auto_check_status'], 'ready')
                self.assertEqual(row['check_status'], 'failed')
                self.assertNotIn('hits', row)

    def test_auto_incomplete_preserves_previous_valid_manual(self):
        for label, auto_result in [('empty-result', {}), ('result-none', None)]:
            with self.subTest(label=label):
                rid = 'recRepair'
                record = maintenance.ongoing_records([_repair_item(rid)])[0]
                now = time.time()
                # Valid manual ready result.
                self.store.put_document('plan_convergence_checks', 'repair:' + rid, {
                    'work_type': 'repair', 'source': 'manual', 'check_source': 'manual',
                    'check_status': 'ready', 'checked_at': now,
                    'hits': [{'blockId': 'manual-hit'}],
                    'fingerprint': maintenance.business_fingerprint(record),
                })
                # Malformed/incomplete auto ready job.
                self.store.put_document('notice_plan_checks', 'latest:' + rid, {'job_id': 'auto-job'})
                self.store.put_document('notice_plan_checks', 'auto-job', {
                    'id': 'auto-job', 'status': 'ready', 'checked_at': now + 500,
                    'record': record, 'result': auto_result,
                })
                service = self._service([_repair_item(rid)])
                row = service.maintenance_records(rid)[0]
                # The valid manual result is preserved and preferred over the incomplete auto.
                self.assertEqual(row['check_source'], 'manual')
                self.assertEqual(row['check_status'], 'ready')
                self.assertEqual([h['blockId'] for h in row['hits']], ['manual-hit'])
                self.assertEqual(row['auto_check_status'], 'ready')

    def test_manual_cached_malformed_hits_marked_failed_no_hits(self):
        record = maintenance.ongoing_records([_change_item()], work_type='change')[0]
        self.store.put_document('plan_convergence_checks', 'change:recChange', {
            'work_type': 'change', 'source': 'manual', 'check_source': 'manual',
            'check_status': 'ready', 'checked_at': 123, 'hits': None,
            'fingerprint': maintenance.business_fingerprint(record),
        })
        service = self._service([_change_item()])
        row = service.change_records('recChange')[0]
        self.assertEqual(row['check_status'], 'failed')
        self.assertNotIn('hits', row)

    def test_empty_local_records_still_queries_blocks_and_reports_orphans(self):
        client = self._route_client([])
        # GET records stays purely local - no remote request at all.
        with patch('lan_bitable_template_portal.plan_convergence_auth.request',
                   side_effect=AssertionError('must not dial remote on records listing')):
            listed = client.get('/api/plan-convergence/change/records')
        self.assertEqual(listed.status_code, 200)
        self.assertEqual(listed.json()['data'], [])

        # An explicit check-all on an empty local list still queries the remote
        # blocking records (zero ongoing notices does not imply zero blocks).
        # Every block is an orphan because there is nothing to match.
        blocks = _blocks([('b1', 'B楼 B-101空调屏蔽'), ('b2', 'B楼 B-102空调屏蔽')])
        with patch.object(PlanConvergenceService, 'blocks', return_value=blocks) as remote, \
                patch.object(PlanConvergenceService, 'block', return_value=_block_detail()):
            change = client.post('/api/plan-convergence/change/check', json={})
            maintenance_result = client.post('/api/plan-convergence/maintenance/check', json={})
        # Each empty-records check refreshes the cached block listing (blocks()
        # is invoked twice: once for the cached copy, once for the refresh),
        # so the two check requests drive four remote block fetches total.
        self.assertEqual(remote.call_count, 4)
        self.assertEqual(change.status_code, 200)
        change_data = change.json()['data']
        self.assertEqual(change_data['records'], [])
        self.assertEqual(sorted(change_data['orphan_block_ids']), ['b1', 'b2'])
        self.assertEqual(change_data['stats']['blocks'], 2)
        self.assertEqual(change_data['stats']['orphan_blocks'], 2)
        self.assertEqual(maintenance_result.status_code, 200)
        maintenance_data = maintenance_result.json()['data']
        self.assertEqual(maintenance_data['records'], [])
        self.assertEqual(sorted(maintenance_data['orphan_block_ids']), ['b1', 'b2'])
        self.assertEqual(maintenance_data['stats']['maintenance'], 0)
        self.assertEqual(maintenance_data['stats']['blocks'], 2)
        self.assertEqual(maintenance_data['stats']['orphan_blocks'], 2)

    def test_change_de_record_skipped_even_with_matching_de_block(self):
        items = [_change_item(title='D-124-BAS-101/103/104/105/401、D-150-BAS-101/102',
                              building='D楼', location='D-124')]
        service = self._service(items)
        with patch.object(PlanConvergenceService, 'blocks', return_value=_blocks([('de1', 'D楼 D-124空调屏蔽')])), \
                patch.object(PlanConvergenceService, 'block', return_value=_block_detail()):
            result = service.change_check('recChange')
        row = result['records'][0]
        self.assertEqual(row['check_status'], 'skipped')
        self.assertEqual(row['hits'], [])
        self.assertIn('D/E楼', row['check_error'])
        # The source change notice is still counted, but it is skipped and no
        # D/E blocks are ever matched or reported as orphans.
        self.assertEqual(result['stats']['change'], 1)
        self.assertEqual(result['orphan_block_ids'], [])
        self.assertEqual(result['stats']['blocks'], 0)

    def test_repair_de_record_skipped_and_de_block_filtered(self):
        item = dict(_repair_item())
        item['building'] = 'D楼'
        item['building_codes'] = ['D']
        item['title'] = 'D楼空调检修'
        item['location'] = 'D-124'
        item['repair_device'] = 'D-124-CRAC-01'
        service = self._service([item])
        with patch.object(PlanConvergenceService, 'blocks',
                          return_value=_blocks([('de1', 'D楼 D-124空调屏蔽')])), \
                patch.object(PlanConvergenceService, 'block', return_value=_block_detail()):
            result = service.maintenance_check('recRepair')
        row = result['records'][0]
        self.assertEqual(row['check_status'], 'skipped')
        self.assertEqual(row['hits'], [])
        self.assertIn('D/E楼', row['check_error'])

    def test_empty_local_records_de_blocks_are_excluded_orphan_suppressed(self):
        # Production regression fixture: with zero local records the only remote
        # blocks are D/E-named. The D/E exclusion filters the remote block
        # listing, so orphan reporting is suppressed (0 blocks / 0 orphans)
        # even though those remote blocks genuinely exist. This is reported to
        # the product team; production is intentionally not altered here.
        client = self._route_client([])
        blocks = _blocks([('de1', 'D楼 D-101空调屏蔽'), ('de2', 'D楼 D-102空调屏蔽')])
        with patch.object(PlanConvergenceService, 'blocks', return_value=blocks), \
                patch.object(PlanConvergenceService, 'block', return_value=_block_detail()):
            change = client.post('/api/plan-convergence/change/check', json={})
        self.assertEqual(change.status_code, 200)
        change_data = change.json()['data']
        self.assertEqual(change_data['records'], [])
        self.assertEqual(sorted(change_data['orphan_block_ids']), [])
        self.assertEqual(change_data['stats']['blocks'], 0)
        self.assertEqual(change_data['stats']['orphan_blocks'], 0)

    def test_persist_uses_put_documents_and_older_result_does_not_overwrite_newer(self):
        service = self._service([_change_item()])
        # A newer manual result already persisted.
        newer = {'checked_at': time.time(), 'hits': [{'blockId': 'newer-hit'}]}
        self.store.put_document('plan_convergence_checks', 'change:recChange', {
            'work_type': 'change', 'source': 'manual', 'check_source': 'manual',
            'check_status': 'ready', 'checked_at': newer['checked_at'],
            'hits': newer['hits'], 'fingerprint': {},
        })
        with patch.object(PlanConvergenceService, 'blocks', return_value=_blocks([('b1', 'B楼 B-124空调屏蔽')])), \
                patch.object(PlanConvergenceService, 'block', return_value=_block_detail()):
            with patch('lan_bitable_template_portal.plan_convergence.time.time',
                       return_value=newer['checked_at'] - 100):
                service.change_check('recChange')
        stored = self.store.get_document('plan_convergence_checks', 'change:recChange')
        self.assertEqual(stored['checked_at'], newer['checked_at'])
        self.assertEqual(stored['hits'], newer['hits'])

    def test_persist_batch_is_single_put_documents_call(self):
        items = [_change_item('recA'), _change_item('recB'), _change_item('recC')]
        service = self._service(items)
        with patch.object(PlanConvergenceService, 'blocks', return_value=_blocks([('e1', 'E楼 E-101空调屏蔽')])), \
                patch.object(PlanConvergenceService, 'block', return_value=_block_detail()):
            with patch.object(self.store, 'put_document', wraps=self.store.put_document) as single, \
                    patch.object(self.store, 'put_documents', wraps=self.store.put_documents) as batch:
                service.change_check()
        for rid in ('recA', 'recB', 'recC'):
            self.assertIsNotNone(self.store.get_document('plan_convergence_checks', 'change:' + rid))
        self.assertEqual(batch.call_count, 1)
        self.assertEqual(len(batch.call_args[0][1]), 3)
        single.assert_not_called()

    def test_put_documents_failure_leaves_previous_result_intact(self):
        service = self._service([_change_item()])
        with patch.object(PlanConvergenceService, 'blocks', return_value=_blocks([('b1', 'B楼 B-124空调屏蔽')])), \
                patch.object(PlanConvergenceService, 'block', return_value=_block_detail()):
            service.change_check('recChange')
        before = self.store.get_document('plan_convergence_checks', 'change:recChange')
        with patch.object(PlanConvergenceService, 'blocks', return_value=_blocks([('b1', 'B楼 B-124空调屏蔽')])), \
                patch.object(PlanConvergenceService, 'block', return_value=_block_detail()), \
                patch.object(self.store, 'put_documents', side_effect=RuntimeError('commit failed')):
            with self.assertRaisesRegex(RuntimeError, 'commit failed'):
                service.change_check('recChange')
        after = self.store.get_document('plan_convergence_checks', 'change:recChange')
        self.assertEqual(after, before)

    def test_automatic_notice_check_snapshot_not_saved_as_manual(self):
        rid = 'recRepair'
        record = maintenance.ongoing_records([_repair_item(rid)])[0]
        service = self._service([_repair_item(rid)])
        # This is how NoticePlanChecks invokes the maintenance check: explicit
        # records argument. It must not create a manual plan_convergence_checks doc.
        with patch.object(PlanConvergenceService, 'blocks', return_value=_blocks([('b1', 'B楼 B-124空调屏蔽')])), \
                patch.object(PlanConvergenceService, 'block', return_value=_block_detail()):
            service.maintenance_check(records=[record])
        self.assertIsNone(self.store.get_document('plan_convergence_checks', 'repair:' + rid))


if __name__ == '__main__':
    unittest.main()