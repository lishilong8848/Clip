"""Apply operating marks from the same sealed warm-HVAC survey snapshot."""

import re
from decimal import Decimal, InvalidOperation

from .daily_inspection_form_fields import CHILLER_SHEET
from .daily_inspection_original import catalog_locations, cell_excluded


OPERATING_SOURCE = 'inspection_operating'
AUXILIARY_STATE_CELLS = frozenset(f'{col}{row}' for col in 'KL' for row in range(17, 22))
AUXILIARY_LEVEL_CELLS = frozenset(('K22', 'L22'))
_BASIC = re.compile(r'[B-G](?:[4-9]|1[0124579]|2[1456])')
_RIGHT = re.compile(r'[K-P](?:[45789]|1[0-5]|2[456])')
_STATE = re.compile(r'(?:[B-G](?:21|26)|[K-P](?:[45789]|26))')
_TEXT = re.compile(r'local_[0-9a-f]{32}')
_NUMERIC_ROWS = (4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 15)
_PRESSURE_GROUPS = (('primary', 'BCDEFG', 21, (17, 19)), ('cooling', 'BCDEFG', 26, (24, 25)),
                    ('secondary', 'KLMNOP', 26, (24, 25)))
_PRESSURE_CELLS = {f'{col}{row}': (f'{col}{status_row}', f'{pump}:{number}:{side}')
                   for pump, columns, status_row, rows in _PRESSURE_GROUPS
                   for number, col in enumerate(columns, 1)
                   for row, side in zip(rows, ('inlet', 'outlet'))}


def configured_mark(sheet, cell, round_cells, marks):
    """Each chiller worksheet uses the same address on four separate copies."""
    if (str(sheet or '').strip() == CHILLER_SHEET and cell in marks
            and round_cells and len(round_cells) == 4 and set(round_cells.values()) == {cell}):
        return marks[cell]
    return None


def collect_operating_marks(scope, batch, template, start, end, sealed, plain):
    marks, samples, conflicts = {}, set(), set()
    for row in batch.get('rows', ()):
        fields = row['fields']
        if plain(fields.get('测点来源')) != OPERATING_SOURCE:
            continue
        if (plain(fields.get('楼栋')) != scope + '楼' or plain(fields.get('专业')) != '暖通'
                or plain(fields.get('记录类型')) != '测点明细'
                or not _TEXT.fullmatch(plain(fields.get('测点ID')))
                or plain(fields.get('目标巡检表')) != template
                or plain(fields.get('目标工作表')).strip() != CHILLER_SHEET):
            continue
        sampled = fields.get('采样时间')
        cell = plain(fields.get('目标单元格'))
        value = plain(fields.get('测点值'))
        if (type(sampled) is not int or not start <= sampled < end or sampled > sealed
                or not (_BASIC.fullmatch(cell) or _RIGHT.fullmatch(cell))
                or any(plain(fields.get(f'{slot}目标单元格')) != cell for slot in ('白1', '白2', '夜1', '夜2'))
                or value not in ('', '/', '√', '△')):
            continue
        samples.add(sampled)
        if cell in marks:
            conflicts.add(cell)
        marks[cell] = value
    if len(samples) != 1 or conflicts:
        return {}
    for left, right in zip('BCDEFG', 'KLMNOP'):
        modes = [marks.get(f'{right}{line}') for line in (7, 8, 9)]
        if modes == ['', '', '']:
            status = ''
        elif modes == ['/', '/', '/']:
            status = '△'
        elif None not in modes and sorted(modes) == ['/', '/', '√']:
            status = '√'
        else:
            return {}
        if marks.get(f'{right}4') != status or marks.get(f'{right}5') != '√':
            return {}
        numeric = [f'{left}{line}' for line in _NUMERIC_ROWS] + [f'{right}{line}' for line in range(10, 16)]
        if status == '√':
            if any(cell in marks for cell in numeric):
                return {}
        else:
            filler = '/' if status == '△' else ''
            if marks.get(numeric[0]) != status or any(marks.get(cell) != filler for cell in numeric[1:]):
                return {}
    for pressure, (status_cell, _) in _PRESSURE_CELLS.items():
        pump_status = marks.get(status_cell)
        if pump_status not in ('', '√', '△'):
            return {}
        if pump_status == '√':
            if pressure in marks:
                return {}
        elif marks.get(pressure) != ('/' if pump_status else ''):
            return {}
    return marks


def pressure_value(value):
    text = str(value).strip() if value is not None else ''
    if not 0 < len(text) <= 24 or not re.fullmatch(r'[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)', text):
        return ''
    try:
        return text if Decimal(text).is_finite() else ''
    except InvalidOperation:
        return ''


def compressor_running_marks(scope, batch, template, start, end, sealed, marks):
    """Use only configured, in-round compressor current for the same building and unit."""
    readings = {}
    for device in batch.get('devices', ()):
        if device.get('scope') != scope or device.get('profession') != '暖通':
            continue
        for point in device.get('points', ()):
            cell, sample = point.get('cell'), point.get('sampled')
            if (not isinstance(cell, str) or not re.fullmatch(r'[B-G]4', cell) or type(sample) is not int
                    or not start <= sample < end or sample > sealed or point.get('enabled') is False
                    or point.get('template') != template or str(point.get('sheet') or '').strip() != CHILLER_SHEET
                    or not point.get('round_cells') or set(point['round_cells'].values()) != {cell}
                    or point.get('source_kind') in ('fixed_value', 'fixed_status', 'manual_status')):
                continue
            raw = point.get('corrected_value')
            if raw is None:
                raw = point.get('raw_value')
            number = pressure_value(str(raw or '').strip().removesuffix('%'))
            if number:
                readings.setdefault(cell, set()).add(Decimal(number))
    updated = dict(marks)
    for left, right in zip('BCDEFG', 'KLMNOP'):
        values = readings.get(f'{left}4', set())
        if len(values) != 1 or next(iter(values)) <= 20:
            continue
        updated[f'{right}4'] = '√'
        # Keep a verified mode. A former stop marker cannot identify which mode is active now.
        modes = tuple(f'{right}{line}' for line in (7, 8, 9))
        if [updated.get(cell) for cell in modes].count('√') != 1:
            for cell in modes:
                updated[cell] = ''
        for cell in [*(f'{left}{line}' for line in _NUMERIC_ROWS), *(f'{right}{line}' for line in range(10, 16))]:
            updated.pop(cell, None)
    return updated


def unit_pump_marks(marks):
    """Use the same-number unit only when a primary or cooling pump has no feedback."""
    updated = dict(marks)
    for left, right in zip('BCDEFG', 'KLMNOP'):
        unit = marks.get(f'{right}4')
        if unit not in ('√', '△'):
            continue
        for state_row, pressure_rows in ((21, (17, 19)), (26, (24, 25))):
            status_cell = f'{left}{state_row}'
            if updated.get(status_cell) in ('√', '△'):
                continue
            updated[status_cell] = unit
            for row in pressure_rows:
                address = f'{left}{row}'
                if unit == '△':
                    updated[address] = '/'
                else:
                    updated.pop(address, None)
    return updated


def auxiliary_levels(scope, batch, template, start, end, sealed):
    """Select verified auxiliary level readings from this building and round."""
    candidates = {}
    for device in batch.get('devices', ()):
        if device.get('scope') != scope or device.get('profession') != '暖通':
            continue
        for point in device['points']:
            cell, sampled = point.get('cell'), point.get('sampled')
            if (cell not in AUXILIARY_LEVEL_CELLS or point.get('template') != template
                    or str(point.get('sheet') or '').strip() != CHILLER_SHEET
                    or point.get('enabled') is not True or point.get('mapping_issue')
                    or not point.get('round_cells') or len(point['round_cells']) != 4
                    or set(point['round_cells'].values()) != {cell}):
                continue
            value = pressure_value(point.get('value'))
            valid = type(sampled) is int and start <= sampled < end and sampled <= sealed and value
            candidates.setdefault(cell, []).append(value if valid else '')
    return {cell: values[0] for cell, values in candidates.items() if len(values) == 1 and values[0]}


def remember_pump_pressures(state, active, edits):
    scope = active['duty']['scope']
    batch = active['batches'][scope]
    marks = batch.get('operating_marks', {})
    template = batch.get('operating_template')
    saved = dict(state.get('pump_pressures') or {})
    for edit in edits:
        target = _PRESSURE_CELLS.get(edit['cell'])
        if (edit['template'] != template or edit['sheet'].strip() != CHILLER_SHEET
                or not target or marks.get(target[0]) != '√'):
            continue
        _, key = target
        text = edit['value']
        if value := pressure_value(text):
            saved[key] = value
        elif text is None or not str(text).strip():
            saved.pop(key, None)
    if saved or 'pump_pressures' in state:
        state['pump_pressures'] = saved


def apply_operating_marks(scope, batch, marks, *, template=None, saved_pressures=None):
    batch['operating_marks'] = marks
    if not marks:
        return
    batch['operating_template'] = template
    catalogue = catalog_locations()
    devices = [*batch.get('devices', ()), *batch.get('form_devices', ())]
    has_live_value = set()
    for device in devices:
        if device.get('scope') != scope or device.get('profession') != '暖通':
            continue
        for point in device['points']:
            cell = point.get('cell')
            if (cell in _PRESSURE_CELLS and point.get('template') == template
                    and str(point.get('sheet') or '').strip() == CHILLER_SHEET
                    and point.get('enabled') is not False
                    and (pressure_value(point.get('value')) or pressure_value(point.get('handover_value')))):
                has_live_value.add(cell)
        for section in device['sections']:
            prefix = (scope + '楼', device['name'], '暖通', section['location'], section['form'].strip())
            for name, value in section['values'].items():
                target = catalogue.get((*prefix, name))
                if target and target['template'] == template:
                    cell = target['cells'].get('白1')
                    fallback = section.get('handover_readings', {}).get(name, {}).get('value')
                    if cell in _PRESSURE_CELLS and (pressure_value(value) or pressure_value(fallback)):
                        has_live_value.add(cell)
    carried = {}
    for cell, (status_cell, key) in _PRESSURE_CELLS.items():
        if cell not in has_live_value and marks.get(status_cell) == '√':
            if value := pressure_value((saved_pressures or {}).get(key)):
                carried[cell] = value
    batch['carried_pump_pressures'] = carried
    effective = {**marks, **carried}
    for device in devices:
        if device.get('scope') != scope or device.get('profession') != '暖通':
            continue
        for point in device['points']:
            mark = configured_mark(point.get('sheet'), point.get('cell'), point.get('round_cells'), effective)
            if mark is not None and point.get('enabled') is not False:
                point['operating_value'] = mark
            else:
                point.pop('operating_value', None)
        for section in device['sections']:
            prefix = (scope + '楼', device['name'], '暖通', section['location'], section['form'].strip())
            values = {}
            missing = re.split(r'[、,，;；\n]+', section.get('missing') or '')
            for name in dict.fromkeys([*section['values'], *(item.strip() for item in missing)]):
                target = catalogue.get((*prefix, name))
                if target is None:
                    continue
                mark = configured_mark(target['sheet'], target['cells'].get('白1'), target['cells'], effective)
                if mark is not None:
                    values[name] = mark
            if values:
                section['operating_values'] = values
            else:
                section.pop('operating_values', None)


def unbound_operating_cells(marks, template, layout, occupied):
    """Retain statuses for stopped equipment that has no bound sensor or form row."""
    for cell, value in marks.items():
        if not value or cell in occupied or cell_excluded(layout, cell):
            continue
        col, row = ord(cell[0]) - ord('A'), int(cell[1:]) - 1
        if row < len(layout['rows']) and col < len(layout['rows'][row]) and layout['rows'][row][col] in ('', None):
            yield {'template': template, 'sheet': layout['name'], 'cell': cell, 'label': '运行联动',
                   'value': value, 'value_type': '状态' if _STATE.fullmatch(cell) else '数值'}
