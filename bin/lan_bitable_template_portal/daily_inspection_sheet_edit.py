"""Validated edits of frozen inspection form cells; never changes the base template."""
import datetime as dt
from openpyxl.cell.cell import MergedCell
from openpyxl.utils.cell import coordinate_from_string, column_index_from_string
from .daily_inspection_history import (REVIEW_SHEET, archived_devices, edit_archived_devices, valid_cell_text)
from .daily_inspection_original import (ORIGINAL_INDEX_SHEET, ORIGINAL_MAPPING_SHEET, refresh_archived_original)
from .risk_inspection import RiskError


def json_cell_value(value):
    return value.isoformat() if isinstance(value,(dt.datetime,dt.date,dt.time)) else value


def form_index(book):
    if ORIGINAL_INDEX_SHEET not in book:
        raise RiskError('历史附件缺少原表目录，无法安全修改，请核对原记录。', 409)
    return {(row[1], row[2]): row[0] for row in book[ORIGINAL_INDEX_SHEET].iter_rows(min_row=2, values_only=True) if row[0]}


def apply_cells(book, scope, edits, actor, *, enforce_exclusions=False):
    if not isinstance(edits, list) or not edits or len(edits) > 2000:
        raise RiskError('请提交有效的单元格修改。', 400)
    index = form_index(book)
    from .daily_inspection_original import original_templates,cell_excluded
    layouts={(t['name'],s['name']):s for t in original_templates() if t['scope']==scope for s in t['sheets']} if enforce_exclusions else {}
    mappings = {}
    for row in book[ORIGINAL_MAPPING_SHEET].iter_rows(min_row=2, values_only=True):
        mappings.setdefault((row[0], row[1]), []).append(row)
    devices = archived_devices(book, scope)
    rows = {d['id']: d for d in devices}
    changes, seen, direct = {}, set(), []
    for edit in edits:
        if not isinstance(edit, dict) or not valid_cell_text(edit.get('value'), 1000):
            raise RiskError('单元格内容无效或过长。', 400)
        name = index.get((edit.get('template'), edit.get('sheet')))
        address = edit.get('cell')
        if not name or not isinstance(address, str):
            raise RiskError('原表位置已变化，请刷新后核对。', 409)
        try:
            col, row = coordinate_from_string(address)
            col = column_index_from_string(col)
            target = book[name]
            if row < 1 or col < 1 or row > target.max_row or col > target.max_column:
                raise ValueError()
            cell = target.cell(row, col)
            if isinstance(cell, MergedCell) or (name, address) in seen:
                raise ValueError()
        except (ValueError, TypeError, KeyError):
            raise RiskError('单元格坐标无效或重复。', 400) from None
        layout=layouts.get((edit.get('template'),edit.get('sheet')))
        if layout and cell_excluded(layout,address):
            raise RiskError('该区域已设为本次无需填写，原有历史记录保留。',409)
        seen.add((name, address))
        for _, _, identity, kind, point, parameter, location, form in mappings.get((name, address), []):
            device = rows.get(identity)
            matches = [r for r in (device or {}).get('rows', []) if r['name'] == parameter and
                       (r['source'] == point if kind == 'point' else (r['location'] == location and r['source'] == form))]
            if len(matches) != 1 or matches[0]['read_only']:
                raise RiskError('该单元格的测点映射无法核验或已停用。', 409)
            change = changes.setdefault(identity, {'values': {}, 'result': device['result'], 'note': device['note']})
            change['values'][matches[0]['id']] = edit['value']
            if edit.get('note'):
                change['note'] = edit['note']
        direct.append((cell, edit['value']))
    if changes:
        edit_archived_devices(book, devices, changes, actor)
        refresh_archived_original(book, changes, actor)
    for cell, value in direct:
        cell.value = value or None
        cell.data_type = 's'  # Text must never execute as an Excel formula.
    return changes


def form_extras(book, templates, entries):
    """Read non-mapped corrections from frozen forms, including explicit blanks."""
    index = form_index(book)
    mapped = {(c['template'], c['sheet'], c['cell']): c for c in entries}
    extras = []
    for template in templates:
        for sheet in template['sheets']:
            name = index.get((template['name'], sheet['name']))
            if not name or name not in book:
                continue
            for ri, row in enumerate(sheet['rows'], 1):
                for ci, original in enumerate(row, 1):
                    cell = book[name].cell(ri, ci)
                    if isinstance(cell, MergedCell):
                        continue
                    key = (template['name'], sheet['name'], cell.coordinate)
                    if key in mapped:
                        mapped[key]['value'] = json_cell_value(cell.value)
                    elif ('' if cell.value is None else str(cell.value)) != ('' if original is None else str(original)):
                        extras.append({'template':key[0], 'sheet':key[1], 'cell':key[2], 'value':json_cell_value(cell.value)})
    return extras


def overlay_views(views, edits):
    for view in views:
        for edit in edits:
            if (edit['template'], edit['sheet']) != (view['template'], view['sheet']):
                continue
            col, row = coordinate_from_string(edit['cell'])
            view['rows'][row-1][column_index_from_string(col)-1] = edit['value']
    return views


def frozen_readings(book, scope):
    """History uses its saved coordinates, never today's mapping configuration."""
    index = {name: key for key, name in form_index(book).items()}
    known = {device['id']: device for device in archived_devices(book, scope)}
    grouped = {}
    for name, address, identity, kind, point, parameter, location, form in book[ORIGINAL_MAPPING_SHEET].iter_rows(min_row=2, values_only=True):
        if name not in index or name not in book:
            raise RiskError('历史巡检原表目录无法核验。', 409)
        members = known.get(identity, {}).get('rows', [])
        matched = [row for row in members if row['name'] == parameter and
                   (row['source'] == point if kind == 'point' else row['location'] == location and row['source'] == form)]
        if len(matched) != 1:
            raise RiskError('历史巡检测点关联不唯一，请核对原附件。', 409)
        template, sheet = index[name]
        grouped.setdefault((name,address), []).append({'template':template,'sheet':sheet,'cell':address,
            'value':json_cell_value(book[name][address].value),'device':identity,'kind':kind,'point_id':point or '',
            'parameter':parameter,'location':location or '', 'form':form or '', 'read_only':matched[0]['read_only']})
    return [items[0] if len(items)==1 else {**items[0],'kind':'paired','members':items,
        'read_only':any(item['read_only'] for item in items)} for items in grouped.values()]
