import io
import sys
import tempfile
import unittest
from pathlib import Path
from openpyxl import load_workbook

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lan_bitable_template_portal import daily_inspection as inspection


def point_row(scope, book, address, source, value, stamp):
    return {'record_id': f'recFixture{scope}{address}', 'fields': {
        '楼栋': scope + '楼', '专业': '暖通', '记录类型': '测点明细',
        '测点来源': source, '测点ID': 'local_' + 'a' * 32, '采样时间': stamp,
        '测点值': value, '目标巡检表': book, '目标工作表': inspection.CHILLER_SHEET,
        '目标单元格': address, **dict.fromkeys(inspection.ROUND_LOCATION_FIELDS.values(), address),
    }}


def scope_fixture(scope):
    selected = {'K7', 'K8', 'K9', 'B24'}
    devices = {}
    names = {}
    for (floor, equipment, profession, location, form, name), item in inspection.catalog_locations().items():
        address = item['cells']['白1']
        if floor != scope + '楼' or item['sheet'].strip() != inspection.CHILLER_SHEET or address not in selected:
            continue
        book = item['template']
        device = devices.setdefault(equipment, {'id': f'{scope}:{equipment}', 'name': equipment, 'scope': scope,
                                                'profession': profession, 'sections': [], 'points': []})
        if not device['sections']:
            device['sections'].append({'location': location, 'form': form, 'values': {}, 'integrity': '', 'missing': '', 'note': ''})
        device['sections'][0]['values'][name] = ''
        names[address] = name
    assert set(names) == selected
    return book, names, {'devices': list(devices.values())}


class HandoverInspectionTests(unittest.TestCase):
    def test_primary_and_cooling_follow_unit_only_without_pump_feedback(self):
        from lan_bitable_template_portal.daily_inspection_operating import unit_pump_marks

        marks = unit_pump_marks({'K4': '√', 'L4': '△', 'B17': '', 'B26': '△', 'B24': '/',
                                 'C21': '√', 'K26': '', 'L26': '√'})
        self.assertEqual(marks['B21'], '√')
        self.assertNotIn('B17', marks)
        self.assertEqual([marks[cell] for cell in ('B26', 'B24', 'C21', 'C26', 'C24', 'K26', 'L26')],
                         ['△', '/', '√', '△', '/', '', '√'])
        self.assertNotIn('D21', marks)
        self.assertEqual(unit_pump_marks(marks), marks)

    def test_auxiliary_status_defaults_are_exposed_for_the_current_workbook(self):
        for scope in 'DE':
            forms = inspection.supplemental_form_devices(scope)
            device = next(d for d in forms if any(p['cell'] == 'K17' and
                          p['sheet'].strip() == inspection.CHILLER_SHEET for p in d['points']))
            point = next(p for p in device['points'] if p['cell'] == 'K17')
            parameter = next(p for p in inspection.parameters(device) if p.get('point') == point['id'])
            self.assertEqual(parameter['reference'], '√')
            self.assertEqual(parameter['configured_target']['round_cells'], dict.fromkeys(('白1','白2','夜1','夜2'), 'K17'))

    def test_opening_snapshot_is_supplemented_before_start_only(self):
        import datetime as dt
        from lan_bitable_template_portal.state_store import LanPortalStateStore

        root = self.enterContext(tempfile.TemporaryDirectory())
        store = LanPortalStateStore(Path(root) / 'state.sqlite3')
        service = inspection.DailyInspection(store, None, scheduled=True)
        duty = {'scope': 'D', 'date': '2026-11-14', 'shift': '白'}
        template, _, original = scope_fixture('D')
        ts = lambda hour, minute: int(dt.datetime(2026, 11, 14, hour, minute,
                                                   tzinfo=inspection.CN).timestamp() * 1000)
        original.update(batch=ts(9, 54), rows=[{'record_id': 'recInitial', 'fields': {'源文件时间': ts(9, 45)}}])
        active = service.new_round({}, duty, 1, {'D': original}, inspection.SYSTEM_INSPECTION_ACTOR,
                                   dt.datetime.fromtimestamp(ts(9, 54) / 1000, inspection.CN))
        active['worksheet_mode'] = True
        store.put_document(inspection.NS, 'building:D', {'active': active, 'history': []})

        def batch(sealed, sampled, value):
            point = {'cell': 'K22', 'value': value, 'sampled': sampled, 'enabled': True, 'template': template,
                     'sheet': inspection.CHILLER_SHEET, 'round_cells': dict.fromkeys(('白1','白2','夜1','夜2'), 'K22')}
            device = {'scope': 'D', 'profession': '暖通', 'points': [point]}
            return {'batch': sealed, 'rows': [{'fields': {'源文件时间': sampled}}], 'devices': [device]}

        service.source_cache['D'] = {'batch': batch(ts(10, 33), ts(10, 10), '19.30')}
        state = service.sync_active_live('D', cached_only=True)
        current = state['active']
        self.assertEqual(current['batches']['D']['batch'], ts(9, 54))
        self.assertEqual(current['batches']['D']['auxiliary_levels'], {'K22': '19.30'})
        self.assertEqual(service.sync_active_live('D', cached_only=True)['active']['review_version'],
                         current['review_version'])
        _, report = service.active_report_content(current, 'D')
        values = {entry['cell']: entry['value'] for entry in report['extra_cells']}
        self.assertEqual(values['K22'], '19.30')
        service.source_cache['D'] = {'batch': batch(ts(12, 0), ts(11, 59), '21.00')}
        self.assertEqual(service.sync_active_live('D', cached_only=True)['active']['batches']['D']['auxiliary_levels'],
                         {'K22': '19.30'})
        current['source_locked'] = True
        store.put_document(inspection.NS, 'building:D', state)
        service.source_cache['D'] = {'batch': batch(ts(11, 0), ts(10, 50), '20.00')}
        self.assertEqual(service.sync_active_live('D', cached_only=True)['active']['batches']['D']['auxiliary_levels'],
                         {'K22': '19.30'})

    def test_compressor_current_above_twenty_only_uses_same_round_matching_unit(self):
        book, _, batch = scope_fixture('D')
        from lan_bitable_template_portal.daily_inspection_operating import compressor_running_marks

        def point(cell, value, sampled=150):
            return {'template':book, 'sheet':inspection.CHILLER_SHEET, 'cell':cell,
                    'round_cells':dict.fromkeys(('白1','白2','夜1','夜2'), cell),
                    'raw_value':value, 'sampled':sampled}
        batch['devices'].extend([
            {'scope':'D', 'profession':'暖通', 'points':[point('B4','20.01%'), point('C4','20'), point('D4','34',99)]},
            {'scope':'E', 'profession':'暖通', 'points':[point('E4','80')]},
        ])
        stopped = {'B4':'△', 'B6':'/', 'K4':'△', 'K7':'/', 'K8':'/', 'K9':'/', 'B21':'√', 'K26':'△'}
        marks = compressor_running_marks('D', batch, book, 100, 200, 180, stopped)
        self.assertEqual((marks['K4'], marks['K7'], marks['B21'], marks['K26']), ('√', '', '√', '△'))
        self.assertTrue({'B4', 'B6', 'L4', 'M4', 'N4'}.isdisjoint(marks))

    def test_stopped_plate_modes_are_slashes_with_old_or_current_batch(self):
        for scope, old_value in (('D', '△'), ('E', '/')):
            book, names, batch = scope_fixture(scope)
            rows = [point_row(scope, book, address, inspection.HANDOVER_MODE, old_value, 100)
                    for address in ('K7', 'K8', 'K9')]
            self.assertTrue(inspection.apply_handover_readings(scope, batch, rows, 90, 120, 200, 175))
            rendered = {p['name']: p['reference'] for device in batch['devices'] for p in inspection.parameters(device)}
            self.assertEqual([rendered[names[cell]] for cell in ('K7', 'K8', 'K9')], ['/', '/', '/'])
            self.assertEqual(batch['operating_marks'], {'K4': '△', 'K14': '/', 'K15': '/'})
            newer = [point_row(scope, book, address, inspection.HANDOVER_MODE, value, 110)
                     for address, value in zip(('K7', 'K8', 'K9'), ('/', '√', '/'))]
            self.assertTrue(inspection.apply_handover_readings(scope, batch, newer, 90, 120, 200, 175))
            self.assertEqual(batch['operating_marks'], {'K4': '√'})
            self.assertFalse(inspection.apply_handover_readings(scope, batch, rows, 90, 120, 200, 175))
            self.assertEqual(batch['operating_marks'], {'K4': '√'})

    def test_remote_control_does_not_require_mode_to_be_known(self):
        from lan_bitable_template_portal.daily_inspection_reminders import slot_time

        duty = {'scope': 'D', 'date': '2026-11-14', 'shift': '白'}
        _, _, batch = scope_fixture('D')
        batch.update(batch=int(slot_time(duty['date'], duty['shift'], 1).timestamp() * 1000), rows=[{'fields': {}}])
        service = inspection.DailyInspection(None, None)
        active = {'duty': duty, 'round': 1}
        self.assertTrue(service.fill_handover_points('D', active, batch, batch))
        self.assertEqual(batch['operating_marks'], {f'{column}5': '√' for column in 'KLMNOP'})
        self.assertFalse(service.fill_handover_points('D', active, batch, batch))

    def test_last_manual_pump_pressures_follow_only_the_running_pump(self):
        from lan_bitable_template_portal.daily_inspection_operating import remember_pump_pressures

        template, names, d_batch = scope_fixture('D')
        _, _, e_batch = scope_fixture('E')
        state = {}
        pump = next(item for item in d_batch['devices'] if names['B24'] in item['sections'][0]['values'])
        active = {'duty': {'scope': 'D'}, 'batches': {'D': d_batch}}

        def edit(cell, value):
            return {'template': template, 'sheet': inspection.CHILLER_SHEET, 'cell': cell, 'value': value}

        inspection.apply_operating_marks('D', d_batch, {'B26': '√', 'C26': '△', 'K26': '√'}, template=template)
        remember_pump_pressures(state, active, [edit('B24', '0.31'), edit('B25', '0.39'),
                                                edit('C24', '9.99'), edit('K24', '0')])
        self.assertEqual(state['pump_pressures'], {'cooling:1:inlet': '0.31', 'cooling:1:outlet': '0.39',
                                                   'secondary:1:inlet': '0'})

        inspection.apply_operating_marks('D', d_batch, {'B26': '√', 'K26': ''}, template=template,
                                         saved_pressures=state['pump_pressures'])
        self.assertEqual(d_batch['carried_pump_pressures'], {'B24': '0.31', 'B25': '0.39'})
        self.assertEqual(inspection.parameters(pump)[0]['reference'], '0.31')
        inspection.apply_operating_marks('E', e_batch, {'B26': '√'}, template='E暖通')
        self.assertEqual(e_batch['carried_pump_pressures'], {})

        pump['sections'][0]['values'][names['B24']] = '0.37'
        inspection.apply_operating_marks('D', d_batch, {'B26': '√'}, template=template,
                                         saved_pressures=state['pump_pressures'])
        self.assertEqual(inspection.parameters(pump)[0]['reference'], '0.37')
        remember_pump_pressures(state, active, [edit('B24', '')])
        self.assertNotIn('cooling:1:inlet', state['pump_pressures'])

        inspection.apply_operating_marks('D', d_batch, {'B26': '△', 'B25': '/', 'C26': '√'}, template=template,
                                         saved_pressures=state['pump_pressures'])
        self.assertEqual(d_batch['carried_pump_pressures'], {})
        self.assertEqual(state['pump_pressures']['cooling:1:outlet'], '0.39')
        remember_pump_pressures(state, active, [edit('C24', '0.41')])
        inspection.apply_operating_marks('D', d_batch, {'B26': '△', 'C26': '√', 'K26': '√'}, template=template,
                                         saved_pressures=state['pump_pressures'])
        self.assertEqual(d_batch['carried_pump_pressures'], {'C24': '0.41', 'K24': '0'})

    def test_operating_marks_drive_live_and_frozen_forms_and_keep_manual_value(self):
        book, names, batch = scope_fixture('D')
        marker = {f'{right}5': '√' for right in 'KLMNOP'}
        for left, right in zip('BCDEFG', 'KLMNOP'):
            marker.update({f'{left}{line}': '/' for line in (5, 6, 7, 8, 9, 10, 11, 12, 14, 15, 17, 19, 24, 25)})
            marker.update({f'{right}{line}': '/' for line in (7, 8, 9, 10, 11, 12, 13, 14, 15, 24, 25)})
            marker.update({cell: '△' for cell in (f'{left}4', f'{left}21', f'{left}26', f'{right}4', f'{right}26')})
        rows = [point_row('D', book, address, inspection.OPERATING_SOURCE, value, 140)
                for address, value in marker.items()]
        self.assertFalse(inspection.collect_operating_marks('E', {'rows': rows}, book, 100, 200, 170, inspection.plain))
        self.assertFalse(inspection.collect_operating_marks('D', {'rows': rows}, book, 141, 200, 170, inspection.plain))
        self.assertFalse(inspection.collect_operating_marks('D', {'rows': rows[:-1]}, book, 100, 200, 170, inspection.plain))
        marks = inspection.collect_operating_marks('D', {'rows': rows}, book, 100, 200, 170, inspection.plain)
        self.assertEqual(marks, marker)
        pump = next(device for device in batch['devices'] if names['B24'] in device['sections'][0]['values'])
        pump['sections'][0]['values'][names['B24']] = '0.35'
        inspection.apply_operating_marks('D', batch, marks)
        self.assertEqual(inspection.parameters(pump)[0]['reference'], '/')
        self.assertEqual(pump['sections'][0]['values'][names['B24']], '0.35')
        active = {'id': 'operation', 'actor': '测试人', 'started': 175, 'round': 1,
                  'duty': {'scope': 'D', 'date': '2026-11-15', 'shift': '白'},
                  'batches': {'D': {**batch, 'rows': rows, 'batch': 170}}, 'reviews': {}}
        _, preview = inspection.DailyInspection(None, None).active_report_content(active, 'D')
        entries = {entry['cell']: entry['value'] for entry in preview['original_cells'] + preview['extra_cells']}
        self.assertEqual([entries[cell] for cell in ('B4', 'B24', 'K4', 'K5', 'K7')], ['△', '/', '△', '√', '/'])
        pid = 's:0:' + names['B24']
        manual = {pump['id']: {'values': {pid: '0.32'}, 'result': '', 'note': ''}}
        active['reviews'] = inspection.finalize_reviews(batch['devices'], manual, '现场')
        active['completed'] = 190
        content = inspection.workbook(active, {'D': []}, presentation=False)
        with inspection.archived_workbook(content, active['id'], 'D') as archived:
            sheet = archived['原-暖-白1-暖通冷机系统巡检表']
            self.assertEqual([sheet[cell].value for cell in ('B4', 'B24', 'K4', 'K5', 'K7')],
                             ['△', '0.32', '△', '√', '/'])

    def test_modes_and_pressure_use_existing_owners_then_archive_slash(self):
        for scope in 'DE':
            with self.subTest(scope=scope):
                book, names, batch = scope_fixture(scope)
                rows = [point_row(scope, book, address, inspection.HANDOVER_MODE, value, 100)
                        for address, value in zip(('K7', 'K8', 'K9'), ('/', '√', '/'))]
                rows.append(point_row(scope, book, 'B24', inspection.HANDOVER_PRESSURE, '0', 150))
                self.assertTrue(inspection.apply_handover_readings(scope, batch, rows, 90, 120, 200, 175))
                checked = inspection.finalize_reviews(batch['devices'], {}, '测试人')
                for device in batch['devices']:
                    for item in inspection.parameters(device):
                        self.assertEqual(checked[device['id']]['values'][item['id']], item['reference'])
                readings = {row['name']: row['reference'] for device in batch['devices'] for row in inspection.parameters(device)}
                self.assertEqual([readings[names[cell]] for cell in ('K7', 'K8', 'K9', 'B24')], ['/', '√', '/', '0'])
                self.assertFalse(inspection.apply_handover_readings(scope, batch, rows, 90, 120, 200, 175))
                active = {'id': 'case', 'actor': '测试人', 'started': 175, 'completed': 190,
                          'duty': {'scope': scope, 'date': '2026-11-15', 'shift': '白'}, 'round': 1,
                          'batches': {scope: {**batch, 'rows': rows, 'batch': 175}}, 'reviews': checked}
                artifact = load_workbook(io.BytesIO(inspection.workbook(
                    active, {scope: []}, strict_original=False, presentation=False)))
                try:
                    sheet = artifact['原-暖-白1-暖通冷机系统巡检表']
                    self.assertEqual([sheet[cell].value for cell in ('K7', 'K8', 'K9')], ['/', '√', '/'])
                finally:
                    artifact.close()

    def test_previous_round_and_cross_building_values_are_ignored(self):
        book, _, batch = scope_fixture('D')
        rows = [point_row('D', book, 'B24', inspection.HANDOVER_PRESSURE, '0.31', 119),
                point_row('E', book, 'B24', inspection.HANDOVER_PRESSURE, '0.42', 150),
                point_row('D', book, 'K7', inspection.HANDOVER_MODE, '√', 100)]
        self.assertFalse(inspection.apply_handover_readings('D', batch, rows, 90, 120, 200, 175))

    def test_manual_review_and_existing_device_pressure_take_precedence(self):
        book, names, batch = scope_fixture('D')
        pump = next(d for d in batch['devices'] if names['B24'] in d['sections'][0]['values'])
        pump['sections'][0]['values'][names['B24']] = '0.35'
        row = point_row('D', book, 'B24', inspection.HANDOVER_PRESSURE, '0.28', 150)
        self.assertFalse(inspection.apply_handover_readings('D', batch, [row], 90, 120, 200, 175))
        pid = 's:0:' + names['B24']
        pump['sections'][0]['values'][names['B24']] = ''
        self.assertTrue(inspection.apply_handover_readings('D', batch, [row], 90, 120, 200, 175))
        saved = {pump['id']: {'values': {pid: '0.32'}, 'result': '', 'note': ''}}
        self.assertEqual(inspection.finalize_reviews(batch['devices'], saved, '现场')[pump['id']]['values'][pid], '0.32')

    def test_supplement_rows_do_not_change_primary_source_time(self):
        row = point_row('D', 'template', 'B24', inspection.HANDOVER_PRESSURE, '0.28', 150)
        self.assertEqual(list(inspection.sampled_source_times([row, {'fields': {'源文件时间': 120}}])), [120])


if __name__ == '__main__':
    unittest.main()
