"""D/E HVAC switch regression. All writes go to a temporary test directory.

Set HVAC_ACCEPT_CSV_DIR and HVAC_ACCEPT_TEMPLATE_DIR for the optional independent
CSV/original-XLSX comparison. The ordinary suite uses only packaged resources.
"""
import copy
import csv
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from collections import Counter, defaultdict
from functools import wraps
from pathlib import Path
from unittest.mock import Mock, patch

from openpyxl import load_workbook
from openpyxl.cell.cell import MergedCell
from openpyxl.utils.cell import coordinate_to_tuple

SLOTS = ('白1', '白2', '夜1', '夜2')
DAY = '2026-11-26'
SIGNER = '隔离签名甲'
CHILD = __name__ == '__main__' and sys.argv[1:2] == ['--isolated-case']


def isolated_case(check):
    """Keep import-time config migration and logging out of the user's data and test discovery process."""
    @wraps(check)
    def run(self):
        if CHILD:
            return check(self)
        with tempfile.TemporaryDirectory() as root:
            env = dict(os.environ, CLIPFLOW_DATA_DIR=root, LOCALAPPDATA=root, PYTHONUTF8='1')
            subprocess.run([sys.executable, str(Path(__file__).resolve()), '--isolated-case', check.__name__],
                           env=env, cwd=root, check=True, timeout=180)
    return run


def temperature(slot, item_index):
    # Distinct across fields and rounds; a stale or transposed reading fails.
    return f'{18 + SLOTS.index(slot)}.{item_index:03d}'


def sheet_name(title):
    return '原-暖-' + title.strip()


def rows_from_csv(file):
    with file.open(encoding='utf-8-sig', newline='') as stream:
        return list(csv.DictReader(stream))


class HvacSwitchTests(unittest.TestCase):
    def setUp(self):
        if not CHILD:
            return
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        store = LanPortalStateStore(self.root / 'isolated.sqlite3')
        self.service = inspection.DailyInspection(store, Mock(spec_set=[]))

    def make_round(self, scope, slot, controls, *, point_ids=None, before_freeze=None, legacy=False):
        mapping = hvac_switch_map(scope)
        duty = dict(scope=scope, date=DAY, shift=slot[0], shift_names={'白': [SIGNER], '夜': ['隔离签名乙']})
        when = slot_time(DAY, slot[0], int(slot[1]))
        stamp = int(when.timestamp() * 1000)
        rows, values = [], {}
        humidity_ids = {f"{scope}-{item['target_device']}": item['point_id']
                        for item in inspection.confirmed_humidity_cells().values() if item['scope'] == scope}

        def point(name, identity, label, value, **fields):
            common = {'记录类型': '测点明细', '专业': '暖通', '楼栋': scope + '楼', '设备编号': name,
                      '测点ID': identity, '测点名称': label, '源文件时间': stamp, '采样时间': stamp, '上传日期': stamp,
                      '测点值': value, '测点原始值': str(value) if value is not None else ''}
            rows.append({'record_id': f'fixture_{len(rows)}', 'fields': {**common, **fields}})

        for identity, item in mapping.items():
            if point_ids is not None and identity not in point_ids:
                continue
            name, sheet = item['device'], item['sheet']
            rows.append({'record_id': 'device_' + identity, 'fields': {
                '记录类型': '设备巡检', '专业': '暖通', '楼栋': scope + '楼', '设备编号': name,
                '源文件时间': stamp, '上传日期': stamp}})
            for index, cell in enumerate(item['slots'][slot]):
                value = temperature(slot, len(values) + 1)
                values[sheet, cell] = value
                coordinates = {field: item['slots'][s][index] for s, field in inspection.ROUND_LOCATION_FIELDS.items()}
                point(name, humidity_ids.get(name, f'sensor_{identity}_{index}'), ('回风温度', '送风温度')[index] if '精密' in sheet else '湿度显示',
                      value, 目标巡检表=item['template'], 目标工作表=sheet, 目标单元格=item['slots']['白1'][index], **coordinates)
            for value in controls.get(identity, ()):
                fan = inspection.E_212_FAN_IDS.get(name) if scope == 'E' and not legacy else None
                if not fan:
                    point(name, identity, '开关机状态' if '精密' in sheet else '运行状态', value)
                else:
                    speed = {'关闭': '0.00', '运行': '35.00', '1': '35.00', '1.00': '35.00'}.get(str(value), value)
                    try:
                        status = '△' if float(speed) < 5 else '√' if float(speed) > 5 else '待核对'
                    except (TypeError, ValueError):
                        status = '待核对'
                    point(name, fan, '室内风机转速（%）', speed, 状态判定=status)
        ids = sorted(row['record_id'] for row in rows)
        device_count = sum(r['fields']['记录类型'] == '设备巡检' for r in rows)
        receipt = dict(status='completed', building=scope + '楼', upload_time=stamp, device_count=device_count,
                       point_count=len(rows) - device_count,
                       record_ids_sha256=hashlib.sha256(json.dumps(ids, separators=(',', ':')).encode()).hexdigest())
        rows[0]['fields']['备注'] = inspection.INSPECTION_BATCH_PREFIX + json.dumps(receipt, ensure_ascii=False)
        self.assertIsNotNone(inspection.validate_source_batch(scope, rows))
        if before_freeze:
            before_freeze(rows)
        template = next(iter(mapping.values()))['template']
        devices = inspection.devices_for(scope, rows, [], {(scope, '暖通'): [template]})
        batch = {'batch': stamp, 'rows': rows, 'devices': devices}
        original_rows = copy.deepcopy(rows)
        if legacy:
            original_freeze = self.service.freeze_hvac_switches

            def freeze_old(active, started):
                active.pop('manual_humidifiers', None)
                original_freeze(active, started)

            with patch.object(self.service, 'freeze_hvac_switches', side_effect=freeze_old):
                active = self.service.new_round({}, duty, int(slot[1]), {scope: batch}, SIGNER, when, professions=['暖通'])
        else:
            active = self.service.new_round({}, duty, int(slot[1]), {scope: batch}, SIGNER, when, professions=['暖通'])
        active['profession_confirmations'] = {'暖通': {'actor': SIGNER, 'confirmed': stamp}}
        self.assertEqual(batch['rows'], original_rows)
        actual_sheets = {sheet.strip(): sheet for sheet, _ in values}
        for device in batch['devices']:
            for sample in device['points']:
                if sample['point_id'].startswith('sensor_'):
                    expected = values[actual_sheets[sample['sheet'].strip()], sample['round_cells'][slot]]
                    self.assertEqual((sample['raw_value'], sample['value']), (expected, expected))
        if not legacy:
            values = {(sheet, cell): None if sheet == '恒湿机巡检表' else value for (sheet, cell), value in values.items()}
        return active, values

    def export_round(self, active):
        scope = active['duty']['scope']
        path = self.root / (active['id'] + '.xlsx')
        path.write_bytes(inspection.workbook(active, {scope: []}))
        return path

    def assert_copy(self, path, expected, *, confirmed=True):
        book = load_workbook(path, data_only=False)
        try:
            for (sheet, address), value in expected.items():
                actual = book[sheet_name(sheet)][address].value
                self.assertEqual(actual, value, f'{sheet!r}/{address}')
            if confirmed:
                self.assertEqual(book['专业确认']['B2'].value, SIGNER)
            self.assertIn(SIGNER, book[sheet_name('2F包间精密空调巡检表')]['E2'].value)
        finally:
            book.close()

    @isolated_case
    def test_packaged_coordinates_and_template_identity(self):
        seen = set()
        for scope in ('D', 'E'):
            template, = [t for t in inspection.original_templates() if t['scope'] == scope and t['profession'] == '暖通']
            layouts = {s['name']: s for s in template['sheets']}
            mapping = hvac_switch_map(scope)
            self.assertEqual(len(mapping), 166)
            self.assertEqual(len({item['device'] for item in mapping.values()}), 166)
            for item in mapping.values():
                self.assertEqual(item['template'], template['name'])
                sheet = layouts[item['sheet']]
                self.assertEqual(set(item['slots']), set(SLOTS))
                for cells in item['slots'].values():
                    for cell in cells:
                        key = (scope, sheet['name'], cell)
                        self.assertNotIn(key, seen)
                        seen.add(key)
                        r, c = coordinate_to_tuple(cell)
                        self.assertLessEqual(r, len(sheet['rows']))
                        self.assertLessEqual(c, len(sheet['rows'][r - 1]))
        self.assertEqual(len(seen), 2288)
        e = {v['device']: v for v in hvac_switch_map('E').values()}
        self.assertEqual(set(e['E-212-CRAH-06']['slots']['白1']), {'R17', 'R18'})
        self.assertEqual(e['E-240-HUM-01']['slots']['白1'], ('C5',))

    @isolated_case
    def test_full_rounds_and_frozen_history(self):
        oldest = None
        for scope in ('D', 'E'):
            mapping = hvac_switch_map(scope)
            history = []
            all_values = {}
            for i, slot in enumerate(SLOTS):
                controls, stopped = {}, set()
                for j, (identity, item) in enumerate(mapping.items()):
                    off = (i + j + (scope == 'D')) % 2 == 0
                    migrated = item['device'] in inspection.E_212_FAN_IDS
                    controls[identity] = [(4.99 if off else 5.01) if migrated else
                                          ('0.00', '关闭', 0)[j % 3] if off else (1, '运行')[j % 2]]
                    if off and item['sheet'] != '恒湿机巡检表':
                        stopped.update((item['sheet'], cell) for cell in item['slots'][slot])
                active, expected = self.make_round(scope, slot, controls)
                path = self.export_round(active)
                expected.update(dict.fromkeys(stopped, '△'))
                self.assertEqual(len(expected), 286)
                self.assert_copy(path, expected)
                all_values.update(expected)
                _, frozen = inspection.archived_report_content.__wrapped__(
                    path.read_bytes(), scope, active['id'], slot[0], int(slot[1]), True)
                history.append(dict(shift=slot[0], round=int(slot[1]), **frozen))
                if oldest is None:
                    oldest = path, hashlib.sha256(path.read_bytes()).digest(), expected
            report = {'scope': scope, 'date': DAY, 'rounds': history, 'original_templates': [
                t for t in inspection.original_templates() if t['scope'] == scope and t['profession'] == '暖通']}
            path = self.root / (scope + '-archived-day.xlsx')
            path.write_bytes(inspection.inspection_report_workbook(report))
            daybook = load_workbook(path)
            try:
                self.assertEqual(len(all_values), 1144)
                for (sheet, cell), value in all_values.items():
                    self.assertEqual(daybook[sheet_name(sheet)][cell].value, value, f'{scope}/{sheet}/{cell}')
            finally:
                daybook.close()
        path, digest, expected = oldest
        self.assertEqual(hashlib.sha256(path.read_bytes()).digest(), digest)
        self.assert_copy(path, expected)

    @isolated_case
    def test_missing_conflicting_and_invalid_controls_keep_readings(self):
        scope = 'E'
        mapping = hvac_switch_map(scope)
        devices = {item['device']: key for key, item in mapping.items()}
        ids = [devices['E-212-CRAH-06'], devices['E-240-HUM-01']]
        for controls in ({}, {ids[0]: ['0', '1'], ids[1]: ['运行', '关闭']}, {ids[0]: ['5.00']},
                         {ids[0]: [''], ids[1]: ['故障']}):
            active, expected = self.make_round(scope, '白1', controls, point_ids=ids)
            self.assert_copy(self.export_round(active), expected)

    @isolated_case
    def test_unsealed_mixed_upload_and_old_round_controls_do_not_mark(self):
        scope = 'E'
        key = next(key for key, item in hvac_switch_map(scope).items() if item['device'] == 'E-212-CRAH-06')
        fan = inspection.E_212_FAN_IDS['E-212-CRAH-06']

        def stale(rows, field):
            row = next(row['fields'] for row in rows if row['fields'].get('测点ID') == fan)
            row[field] -= 24 * 3600 * 1000

        for mutate in (lambda rows: rows[0]['fields'].pop('备注'), lambda rows: stale(rows, '上传日期'),
                       lambda rows: stale(rows, '采样时间'), lambda rows: stale(rows, '源文件时间'),
                       lambda rows: rows[-1]['fields'].update(测点ID=key, 测点名称='开关机状态'),
                       lambda rows: rows[-1]['fields'].update(采样时间=rows[-1]['fields']['采样时间'] - 1)):
            active, expected = self.make_round(scope, '白2', {key: [0]}, point_ids=[key], before_freeze=mutate)
            self.assert_copy(self.export_round(active), expected)

    @isolated_case
    def test_humidifier_uses_only_real_humidity_for_the_device_and_round(self):
        for scope in ('D', 'E'):
            item_id = next(point for point, item in hvac_switch_map(scope).items()
                           if item['device'] == f'{scope}-240-HUM-01')
            active, expected = self.make_round(scope, '白1', {item_id: ['关闭']}, point_ids=[item_id])
            active['worksheet_mode'] = True
            duty = active['duty']
            state_key = 'building:' + scope
            self.service.store.put_document(inspection.NS, state_key, {'active': active, 'history': []})
            positions = inspection.humidifier_positions(active, scope)
            self.assertEqual(len(positions), 46)
            with self.assertRaisesRegex(inspection.RiskError, '46 格未选启停'):
                inspection.require_humidifiers(active, scope, ['暖通'])
            self.assert_copy(self.export_round(active), expected)

            edits = [{'template': template, 'sheet': sheet, 'cell': cell, 'value': '△',
                      'humidifier_state': 'off'} for template, sheet, cell in positions]
            on = next(edit for edit in edits if edit['cell'] == 'C5')
            on.update(value='无需采信的客户端数字', humidifier_state='on')
            missing = next(edit for edit in edits if edit['cell'] == 'D5')
            missing.update(value='0', humidifier_state='on')
            self.service.change('fixture', SIGNER, duty, 'draft', {
                'id': active['id'], 'review_version': 0, 'sheet_cells': edits})
            saved = self.service.load(scope)['active']
            with self.assertRaisesRegex(inspection.RiskError, '缺少本轮湿度实测.*240-HUM-02 D5'):
                inspection.require_humidifiers(saved, scope, ['暖通'])
            self.assert_copy(self.export_round(saved), {('恒湿机巡检表', 'C5'): '18.001',
                                                       ('恒湿机巡检表', 'D5'): None}, confirmed=False)
            self.service.change('fixture', SIGNER, duty, 'draft', {
                'id': active['id'], 'review_version': saved['review_version'],
                'sheet_cells': [{**missing, 'humidifier_state': 'off'}]})
            saved = self.service.load(scope)['active']
            inspection.require_humidifiers(saved, scope, ['暖通'])
            self.assert_copy(self.export_round(saved), {('恒湿机巡检表', 'C5'): '18.001',
                                                       ('恒湿机巡检表', 'D5'): '△', ('恒湿机巡检表', 'C6'): None}, confirmed=False)

    @isolated_case
    def test_locked_e_round_keeps_manual_review_signature_and_original_sensor_values(self):
        scope = 'E'
        controls = {k: ['关闭'] for k, v in hvac_switch_map(scope).items()
                    if v['device'] in ('E-212-CRAH-06', 'E-240-HUM-01')}
        active, readings = self.make_round(scope, '白1', controls, point_ids=controls)
        self.assertEqual({cell for _, cell in readings}, {'C5', 'R17', 'R18'})
        device = next(d for d in active['batches'][scope]['devices'] if '212-CRAH-06' in d['name'])
        point = next(p for p in device['points'] if p['point_id'].startswith('sensor_'))
        parameter = next(p for p in inspection.parameters(device) if p.get('point') == point['id'])
        active['reviews'] = {device['id']: {'values': {parameter['id']: '23.45'}, 'actor': SIGNER,
                                           'result': '正常', 'note': '隔离手工复核', 'revision': 1}}
        state = {'active': active, 'history': []}
        self.service.store.put_document(inspection.NS, 'building:E', state)
        with patch.object(self.service, 'cached_source', side_effect=AssertionError('Locked snapshot was refreshed')):
            saved = self.service.sync_active_live(scope, cached_only=True)
        self.assertEqual(saved, state)
        path = self.export_round(saved['active'])
        stopped = {(sheet, cell): '△' if sheet != '恒湿机巡检表' else None for sheet, cell in readings}
        self.assert_copy(path, stopped)
        book = load_workbook(path)
        try:
            def records(title):
                header, *data = book[title].values
                return [dict(zip(header, row)) for row in data]
            review, = [r for r in records(inspection.REVIEW_SHEET) if r['来源'] == point['point_id']]
            self.assertEqual((review['现场读数'], review['填写人']), ('23.45', SIGNER))
            original, = [r for r in records('E楼源表快照') if r['测点ID'] == point['point_id']]
            self.assertEqual((original['测点原始值'], original['测点值']), (point['raw_value'], point['value']))
        finally:
            book.close()
        # The next round's sealed input has readings but no switch state. Never borrow the prior stop.
        later, expected = self.make_round(scope, '白2', {}, point_ids=controls)
        self.assert_copy(self.export_round(later), expected)
        self.assert_copy(path, stopped)

    @unittest.skipUnless(os.environ.get('HVAC_ACCEPT_CSV_DIR') and os.environ.get('HVAC_ACCEPT_TEMPLATE_DIR'),
                         'optional local authoritative inputs')
    @isolated_case
    def test_authoritative_csv_and_original_workbook_copies(self):
        source = Path(os.environ['HVAC_ACCEPT_CSV_DIR'])
        devices = rows_from_csv(source / '空调和恒湿机启停清单.csv')
        fields = rows_from_csv(source / '启停控制与四轮数据格.csv')
        self.assertEqual((len(devices), len(fields)), (332, 572))
        totals = Counter()
        for scope in ('D', 'E'):
            inventory = {r['point_id']: r for r in devices if r['building'] == scope + '楼'}
            mapping = hvac_switch_map(scope)
            self.assertEqual(set(mapping), set(inventory))
            rows = [r for r in fields if r['楼栋'] == scope + '楼']
            template, = {r['目标巡检表'] for r in rows}
            original = Path(os.environ['HVAC_ACCEPT_TEMPLATE_DIR']) / template
            original_copy = self.root / original.name
            shutil.copyfile(original, original_copy)
            book = load_workbook(original_copy, data_only=False)
            try:
                by_point = defaultdict(list)
                for row in rows:
                    key = row['状态测点ID']
                    item = mapping[key]
                    self.assertEqual(item['device'], inventory[key]['device'])
                    self.assertEqual((item['device'], item['template'], item['sheet']),
                                     (row['设备'], template, row['工作表']))
                    self.assertEqual(row['状态点'], inventory[key]['metric'])
                    for slot in SLOTS:
                        sheet = book[row['工作表']]
                        r, c = coordinate_to_tuple(row[slot])
                        self.assertLessEqual(r, sheet.max_row)
                        self.assertLessEqual(c, sheet.max_column)
                        self.assertNotIsInstance(sheet.cell(r, c), MergedCell)
                    by_point[key].append(row)
                self.assertEqual(set(by_point), set(mapping))
                for key, group in by_point.items():
                    for slot in SLOTS:
                        self.assertCountEqual(mapping[key]['slots'][slot], [r[slot] for r in group])
            finally:
                book.close()
            controls = {k: [r['sample']] for k, r in inventory.items()}
            self.assertLessEqual({r['sample'] for r in inventory.values()}, {'0.00', '1.00'})
            for slot in SLOTS:
                active, readings = self.make_round(scope, slot, controls, legacy=True)
                expected = {}
                for row in rows:
                    address = row['工作表'], row[slot]
                    off = inventory[row['状态测点ID']]['sample'] == '0.00'
                    expected[address] = '△' if off else readings[address]
                    totals[scope, 'off' if off else 'on'] += 1
                self.assertEqual(len(expected), 286)
                self.assert_copy(self.export_round(active), expected)
        self.assertEqual(sum(totals.values()), 2288)
        print('Authoritative copy readback:', dict(totals))


if CHILD:
    from contextlib import ExitStack
    with ExitStack() as guard:
        for target in ('socket.socket.connect', 'socket.socket.connect_ex', 'socket.getaddrinfo'):
            guard.enter_context(patch(target, side_effect=AssertionError('Network forbidden in HVAC regression')))
        # The legacy migrator also searches next to the executable. Use a blank portable root.
        with patch.object(sys, 'frozen', True, create=True), patch.object(sys, 'executable', str(Path.cwd() / 'python.exe')):
            from upload_event_module import utils
        assert Path(utils.DATA_DIR) == Path.cwd() == Path(utils.BASE_DIR)
        from lan_bitable_template_portal import daily_inspection as inspection
        from lan_bitable_template_portal.daily_inspection_hvac_mapping import hvac_switch_map
        from lan_bitable_template_portal.daily_inspection_reminders import slot_time
        from lan_bitable_template_portal.state_store import LanPortalStateStore
        unittest.main(argv=[__file__, 'HvacSwitchTests.' + sys.argv[2]])
elif __name__ == '__main__':
    unittest.main()
