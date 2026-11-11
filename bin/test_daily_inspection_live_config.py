"""Regressions for live point-configuration propagation into daily inspection.

The panorama producer syncs point-configuration changes to the Feishu config
table immediately.  The portal must pick them up without waiting for a fixed
cache window, and open inspection rounds must re-derive their automatic
positions/values while carrying human reviews over by stable identity.
"""
import copy
import datetime as dt
import sys
import tempfile
import unittest
from pathlib import Path

BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal.daily_inspection import CN, SOURCES, DailyInspection
from lan_bitable_template_portal.state_store import LanPortalStateStore

try:
    from bin.test_daily_inspection import InspectionCloud
except ImportError:  # 也支持在 bin 目录内直接运行
    from test_daily_inspection import InspectionCloud

BATCH = 1793695864749
DAY = {'scope': 'D', 'date': '2026-11-03', 'shift': '白'}


class DailyInspectionLiveConfigTests(unittest.TestCase):
    def setUp(self):
        folder = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.store = LanPortalStateStore(folder / 'state.sqlite3')
        self.cloud = InspectionCloud()
        self.service = DailyInspection(self.store, self.cloud)
        self.source_table, self.config_table = SOURCES['D'][0][0], SOURCES['D'][1][0]

    def tearDown(self):
        self.store.shutdown_write_worker(timeout=2)

    def point(self, batch):
        device = next(item for item in batch['devices'] if item['points'])
        return device, device['points'][0]

    def expire_probe(self):
        for scope in SOURCES:
            cached = self.service.source_cache.get(scope)
            if cached:
                cached['probed_at'] = 0

    def test_config_change_is_visible_on_next_capture(self):
        first = self.service.capture(['D'])['D']
        _, point = self.point(first)
        self.assertEqual(point['cell'], 'B2')
        self.cloud.tables[self.config_table][0]['fields']['目标单元格'] = 'C3'
        self.expire_probe()
        second = self.service.capture(['D'])['D']
        _, moved = self.point(second)
        self.assertEqual(moved['cell'], 'C3')

    def test_probe_throttle_bounds_remote_reads(self):
        self.service.capture(['D'])
        self.cloud.tables[self.config_table][0]['fields']['目标单元格'] = 'C3'
        second = self.service.capture(['D'])['D']
        _, still = self.point(second)
        self.assertEqual(still['cell'], 'B2')

    def test_new_source_batch_is_visible_on_next_capture(self):
        first = self.service.capture(['D'])['D']
        _, point = self.point(first)
        self.assertEqual(str(point['value']), '0')
        new_stamp = BATCH + 60_000
        for row in self.cloud.tables[self.source_table]:
            row['fields']['源文件时间'] = new_stamp
            row['fields']['上传日期'] = new_stamp
        target = next(row for row in self.cloud.tables[self.source_table]
                        if row['fields'].get('记录类型') == '测点明细')
        target['fields']['测点值'] = 7
        self.expire_probe()
        second = self.service.capture(['D'])['D']
        _, updated = self.point(second)
        self.assertEqual(str(updated['value']), '7')

    def test_sync_active_live_updates_positions_and_carries_reviews(self):
        cutoff_ms = int(self.service.round_cutoff(DAY, 1).timestamp() * 1000)
        ok_stamp = cutoff_ms - 120_000
        for row in self.cloud.tables[self.source_table]:
            row['fields']['源文件时间'] = ok_stamp
            row['fields']['上传日期'] = ok_stamp
        batches = self.service.capture(['D'])
        state = {'active': None, 'history': []}
        device, point = self.point(batches['D'])
        state['active'] = self.service.new_round(state, DAY, 1, batches, '测试员', dt.datetime.now(CN))
        state['active']['reviews'] = {device['id']: {'values': {'p:' + point['id']: '5'},
                                                     'result': '', 'note': '', 'revision': 0}}
        self.store.put_document('daily_de_inspection', 'building:D', state)
        self.cloud.tables[self.config_table][0]['fields']['目标单元格'] = 'C3'
        self.cloud.tables[self.source_table][2] = {
            'record_id': 'recD3b',
            'fields': {**copy.deepcopy(self.cloud.tables[self.source_table][2]['fields'])},
        }
        self.expire_probe()
        synced = self.service.sync_active_live('D')
        active = synced['active']
        _, moved = self.point(active['batches']['D'])
        self.assertEqual(moved['cell'], 'C3')
        self.assertEqual(moved['id'], 'recD3b')
        self.assertEqual(active['reviews'][device['id']]['values'], {'p:recD3b': '5'})
        self.assertEqual(active['review_version'], 1)
        self.assertEqual(active['live_fp']['batch'], ok_stamp)
        persisted = self.service.load('D')
        self.assertEqual(persisted['active']['live_fp'], active['live_fp'])

    def test_sync_active_live_keeps_round_without_changes(self):
        cutoff_ms = int(self.service.round_cutoff(DAY, 1).timestamp() * 1000)
        ok_stamp = cutoff_ms - 120_000
        for row in self.cloud.tables[self.source_table]:
            row['fields']['源文件时间'] = ok_stamp
            row['fields']['上传日期'] = ok_stamp
        batches = self.service.capture(['D'])
        state = {'active': None, 'history': []}
        state['active'] = self.service.new_round(state, DAY, 1, batches, '测试员', dt.datetime.now(CN))
        self.store.put_document('daily_de_inspection', 'building:D', state)
        synced = self.service.sync_active_live('D')
        self.assertEqual(synced['active'].get('review_version', 0), 0)
        self.assertEqual(synced['active']['live_fp']['batch'], ok_stamp)

    def test_completed_round_keeps_frozen_snapshot(self):
        batches = self.service.capture(['D'])
        state = {'active': None, 'history': []}
        state['active'] = self.service.new_round(state, DAY, 1, batches, '测试员', dt.datetime.now(CN))
        state['active']['completed'] = 123
        self.store.put_document('daily_de_inspection', 'building:D', state)
        self.cloud.tables[self.config_table][0]['fields']['目标单元格'] = 'C3'
        self.expire_probe()
        synced = self.service.sync_active_live('D')
        _, frozen = self.point(synced['active']['batches']['D'])
        self.assertEqual(frozen['cell'], 'B2')

    def test_manual_round_rejects_batch_sealed_after_cutoff(self):
        from lan_bitable_template_portal.daily_inspection_reminders import shift_bounds
        cutoff_ms = int(self.service.round_cutoff(DAY, 1).timestamp() * 1000)
        shift_end_ms = int(shift_bounds(DAY['date'], DAY['shift'])[1] * 1000)
        self.assertLess(cutoff_ms, shift_end_ms)
        batches = self.service.capture(['D'])
        state = {'active': None, 'history': []}
        state['active'] = self.service.new_round(state, DAY, 1, batches, '测试员', dt.datetime.now(CN))
        self.store.put_document('daily_de_inspection', 'building:D', state)
        # 收尾时点之后封印的批次属于下一轮，即使班次尚未结束也必须拒绝。
        late = cutoff_ms + 60_000
        self.assertLess(late, shift_end_ms)
        self.assertFalse(self.service.live_batch_belongs_to_round(
            state['active'], {'batch': late, 'rows': [], 'devices': []}))
        next_shift = shift_end_ms + 60_000
        self.assertFalse(self.service.live_batch_belongs_to_round(
            state['active'], {'batch': next_shift, 'rows': [], 'devices': []}))
        self.assertTrue(self.service.live_batch_belongs_to_round(
            state['active'], {'batch': cutoff_ms - 60_000, 'rows': [], 'devices': []}))

    def test_scheduled_round_rejects_other_slot_batch(self):
        from lan_bitable_template_portal.daily_inspection_reminders import slot_time
        service = DailyInspection(self.store, self.cloud, scheduled=True)
        duty = {'scope': 'D', 'date': '2026-11-03', 'shift': '白'}
        slot = slot_time(duty['date'], duty['shift'], 1)
        bucket_start = slot.replace(hour=slot.hour - slot.hour % 2, minute=0)
        stamp = int(bucket_start.timestamp() * 1000) + 60_000
        inside = {'batch': stamp + 60_000, 'rows': [{'fields': {'源文件时间': stamp}}], 'devices': []}
        self.assertTrue(service.live_batch_belongs_to_round(
            {'duty': duty, 'round': 1}, inside))
        next_bucket = stamp + 2 * 3600 * 1000
        outside = {'batch': next_bucket + 60_000, 'rows': [{'fields': {'源文件时间': next_bucket}}], 'devices': []}
        self.assertFalse(service.live_batch_belongs_to_round(
            {'duty': duty, 'round': 1}, outside))

    def test_probe_uses_single_bounded_request(self):
        self.service.capture(['D'])
        calls = []
        original = self.cloud.request
        def request(method, path, **kwargs):
            if path == self.source_table + '/records/search':
                calls.append(kwargs.get('params', {}).get('page_size'))
            return original(method, path, **kwargs)
        self.cloud.request = request
        self.expire_probe()
        self.service.capture(['D'])
        self.assertEqual(calls, [1])

    def test_renamed_point_keeps_saved_review_values(self):
        from lan_bitable_template_portal.daily_inspection import remap_review_values
        old = [{'id': 'D:电气:144-AH-109', 'sections': [], 'points': [
            {'id': 'recX', 'point_id': 'p1', 'name': '电流', 'value': 1}]}]
        renamed = [{'id': 'D:电气:144-AH-109', 'sections': [], 'points': [
            {'id': 'recX2', 'point_id': 'p1', 'name': '电流（改名）', 'value': 2}]}]
        reviews = {'D:电气:144-AH-109': {'values': {'p:recX': '5'}, 'result': '', 'note': '', 'revision': 1}}
        carried, ledger = remap_review_values(old, renamed, reviews)
        self.assertEqual(carried['D:电气:144-AH-109']['values'], {'p:recX2': '5'})
        self.assertEqual(ledger, {})

    def test_removed_point_values_survive_for_revival(self):
        from lan_bitable_template_portal.daily_inspection import remap_review_values
        old = [{'id': 'dev', 'sections': [], 'points': [
            {'id': 'recX', 'point_id': 'p1', 'name': '电流', 'value': 1}]}]
        removed = [{'id': 'dev', 'sections': [], 'points': []}]
        reviews = {'dev': {'values': {'p:recX': '5'}, 'result': '', 'note': '', 'revision': 0}}
        carried, ledger = remap_review_values(old, removed, reviews)
        self.assertEqual(carried['dev']['values'], {'p:recX': '5'})
        self.assertEqual(ledger['dev']['p:recX'], ['point', 'p1'])
        revived, ledger2 = remap_review_values(removed, old, carried, ledger)
        self.assertEqual(revived['dev']['values'], {'p:recX': '5'})
        self.assertEqual(ledger2, {})

    def test_removed_point_revives_with_fresh_record_id(self):
        from lan_bitable_template_portal.daily_inspection import remap_review_values
        with_point = [{'id': 'dev', 'sections': [], 'points': [
            {'id': 'recX', 'point_id': 'p1', 'name': '电流', 'value': 1}]}]
        removed = [{'id': 'dev', 'sections': [], 'points': []}]
        readded = [{'id': 'dev', 'sections': [], 'points': [
            {'id': 'recY', 'point_id': 'p1', 'name': '电流', 'value': 3}]}]
        reviews = {'dev': {'values': {'p:recX': '5'}, 'result': '', 'note': '', 'revision': 0}}
        carried, ledger = remap_review_values(with_point, removed, reviews)
        revived, ledger2 = remap_review_values(removed, readded, carried, ledger)
        self.assertEqual(revived['dev']['values'], {'p:recY': '5'})
        self.assertEqual(ledger2, {})

    def test_probe_finds_marker_via_device_filter_when_first_row_lacks_it(self):
        table = self.cloud.tables[self.source_table]
        point_row = next(row for row in table if row['fields'].get('记录类型') == '测点明细')
        table.remove(point_row)
        table.insert(0, point_row)
        self.service.capture(['D'])
        calls = []
        original = self.cloud.request
        def request(method, path, **kwargs):
            if path == self.source_table + '/records/search':
                calls.append((kwargs.get('params', {}).get('page_size'), kwargs.get('json', {})))
            return original(method, path, **kwargs)
        self.cloud.request = request
        self.expire_probe()
        batch = self.service.capture(['D'])['D']['batch']
        self.assertEqual(batch, BATCH)
        self.assertEqual(len(calls), 1)
        page_size, query = calls[0]
        self.assertEqual(page_size, 1)
        self.assertEqual(query.get('filter', {}).get('conjunction'), 'and')
        self.assertEqual(query.get('filter', {}).get('conditions'), [
            {'field_name': '记录类型', 'operator': 'is', 'value': ['设备巡检']},
            {'field_name': '备注', 'operator': 'contains', 'value': ['INSPECTION_BATCH_V1:']}])

    def test_profession_confirmation_invalidated_by_live_value_change(self):
        from lan_bitable_template_portal.daily_inspection import all_professions_confirmed, profession_version
        batches = self.service.capture(['D'])
        state = {'active': None, 'history': []}
        state['active'] = self.service.new_round(state, DAY, 1, batches, '测试员', dt.datetime.now(CN))
        active = state['active']
        active['professions'] = ['电气']
        version = profession_version(active, 'D', '电气')
        confirmations = {'电气': {'version': version, 'actor': '测试员', 'confirmed': 1}}
        self.assertTrue(all_professions_confirmed(active, 'D', confirmations))
        device, point = self.point(active['batches']['D'])
        point['value'] = 99
        self.assertNotEqual(profession_version(active, 'D', '电气'), version)
        self.assertFalse(all_professions_confirmed(active, 'D', confirmations))


if __name__ == '__main__':
    unittest.main()
