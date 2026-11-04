"""Read-only original form layout and strictly identified per-round readings."""

import json
import copy
import re
from collections import defaultdict
from decimal import Decimal, InvalidOperation
from functools import lru_cache
from pathlib import Path

from openpyxl.utils.cell import coordinate_from_string, column_index_from_string
from openpyxl.cell.cell import MergedCell

ORIGINAL_MAPPING_SHEET = '原表映射'
ORIGINAL_MAPPING_COLUMNS = ('工作表', '单元格', '设备标识', '类型', '测点ID', '参数', '位置', '来源表')
ORIGINAL_INDEX_SHEET = '原表目录'
ORIGINAL_INDEX_COLUMNS = ('工作表', '原文件', '原工作表')


def same_reading(left, right):
    left = '' if left is None else str(left).strip()
    right = '' if right is None else str(right).strip()
    if left == right:
        return True
    if not left or not right:
        return False
    try:
        a, b = Decimal(left), Decimal(right)
        return a.is_finite() and b.is_finite() and a == b
    except InvalidOperation:
        return False


@lru_cache(maxsize=1)
def original_templates():
    path = Path(__file__).resolve().parents[1] / 'resources' / 'daily_inspection_templates.json'
    data = json.loads(path.read_text(encoding='utf-8'))
    if data.get('version') != 1 or not isinstance(data.get('templates'), list):
        raise ValueError('巡检原表资料格式不正确')
    return data['templates']


@lru_cache(maxsize=1)
def catalog_locations():
    path = Path(__file__).resolve().parents[1] / 'resources' / 'daily_inspection_catalog_mapping.json'
    data = json.loads(path.read_text(encoding='utf-8'))
    if data.get('version') != 1 or not isinstance(data.get('items'), list):
        raise ValueError('巡检目录坐标格式不正确')
    result = {}
    for item in data['items']:
        key = (item['scope'] + '楼', item['device'], item['profession'], item['location'],
               item['sheet'].strip(), item['field'])
        # Ambiguous directory entries must never be resolved by order.
        result[key] = item if key not in result else None
    return result


def original_cells(sheets, templates, shift, round_number):
    """Match a verified four-round target with the exact archived review row.

    Existing coordinates without all four round locations are intentionally not
    guessed. Ambiguous identities or destinations are omitted rather than merged.
    """
    if shift not in ('白', '夜') or round_number not in (1, 2):
        return []
    source = {sheet['name']: sheet['rows'] for sheet in sheets}
    points, reviews = source.get('测点读数', []), source.get('现场核对', [])
    if not reviews:
        return []
    def records(rows):
        keys = rows[0]
        return [dict(zip(keys, row)) for row in rows[1:]]
    valid_sheets = {}
    for template in templates:
        for sheet in template['sheets']:
            key = (template['name'], sheet['name'].strip())
            if key in valid_sheets:
                valid_sheets[key] = None
            else:
                valid_sheets[key] = sheet
    observed = defaultdict(list)
    for row in records(reviews):
        key = (row.get('楼栋'), row.get('设备'), row.get('专业'), row.get('来源'), row.get('参数'))
        observed[key].append(row)
    targets = defaultdict(list)
    slot = ('白班' if shift == '白' else '夜班') + str(round_number) + '单元格'
    def add_target(template_name, sheet, address, data):
        if not sheet or not isinstance(address, str) or not re.fullmatch(r'[A-Z]{1,3}[1-9]\d{0,5}', address):
            return
        try:
            column, index = coordinate_from_string(address)
            column = column_index_from_string(column)
            if index > len(sheet['rows']) or column > len(sheet['rows'][index - 1]):
                return
            if sheet['rows'][index - 1][column - 1] not in ('', None):
                return
        except (ValueError, IndexError, TypeError):
            return
        target = (template_name, sheet['name'], address)
        targets[target].append({'template': target[0], 'sheet': target[1], 'cell': address, **data})

    for row in records(points) if points else []:
        key = (row.get('楼栋'), row.get('设备'), row.get('专业'), row.get('测点ID'), row.get('测点名称'))
        matched = observed.get(key, [])
        sheet = valid_sheets.get((row.get('模板'), str(row.get('页签') or '').strip()))
        address = row.get(slot)
        if len(matched) != 1:
            continue
        review = matched[0]
        if review.get('参数状态') == '配置停用':
            continue
        add_target(row.get('模板'), sheet, address,
                   {'value': review.get('现场读数'), 'device': review.get('设备标识'),
                    'point_id': row.get('测点ID'), 'parameter': row.get('测点名称'),
                    'kind': 'point', 'read_only': review.get('参数状态') == '配置停用'})
    locations = catalog_locations()
    for review in records(reviews):
        key = (review.get('楼栋'), review.get('设备'), review.get('专业'), review.get('位置'),
               str(review.get('来源') or '').strip(), review.get('参数'))
        mapping = locations.get(key)
        if not mapping:
            continue
        sheet = valid_sheets.get((mapping['template'], mapping['sheet'].strip()))
        address = mapping.get('cells', {}).get(shift + str(round_number))
        add_target(mapping['template'], sheet, address,
                   {'value': review.get('现场读数'), 'device': review.get('设备标识'),
                    'point_id': '', 'parameter': review.get('参数'), 'kind': 'section',
                    'location': review.get('位置'), 'form': review.get('来源'),
                    'read_only': review.get('参数状态') in ('配置停用', '无可填参数')})
    result = []
    for entries in targets.values():
        if len(entries) == 1:
            result.append(entries[0])
        elif len(entries) == 2 and {entry['kind'] for entry in entries} == {'point', 'section'}:
            point = next(entry for entry in entries if entry['kind'] == 'point')
            section = next(entry for entry in entries if entry['kind'] == 'section')
            oil = re.fullmatch(r'([1-9]\d*)#油箱体积', str(point['parameter'] or ''))
            if (oil and section['parameter'] == '柴油液位(cm)'
                    and str(section['device']).endswith(':' + oil.group(1) + '#柴发')
                    and str(point['device']).split(':')[-1] in ('燃油自控系统','178-燃油自控系统')):
                # The point remains in original volume units. The device field is
                # the agreed rounded liquid level; the conversion is not invertible.
                result.append(section)
                continue
            # Both catalog and live point may describe the same physical cell.
            # Keep both identities so one correction updates both stored values.
            same = same_reading(entries[0]['value'], entries[1]['value'])
            result.append({**entries[0], 'kind': 'paired', 'members': entries,
                           'value': entries[0]['value'] if same else None, 'conflict': not same,
                           'read_only': any(entry['read_only'] for entry in entries)})
    return result


def original_views(templates, rounds, *, include_empty_rounds=False):
    """Compose one daily form, except for the per-round chiller form."""
    output = []
    slots = (('白',1),('白',2),('夜',1),('夜',2))
    by_slot = defaultdict(list)
    for item in rounds:
        by_slot[(item.get('shift'),item.get('round'))].append(item)
    if any(len(items)>1 for items in by_slot.values()):
        raise ValueError('同一巡检轮次出现重复记录')
    for template in templates:
        for sheet in template['sheets']:
            per_round = sheet['name'].strip() == '暖通冷机系统巡检表'
            groups = ([(slot, by_slot.get(slot, [])) for slot in slots
                       if include_empty_rounds or slot in by_slot]
                      if per_round else [(None, rounds)])
            for slot,group in groups:
                rows = copy.deepcopy(sheet['rows'])
                occupied = set()
                for item in group:
                    for cell in item.get('original_cells', []):
                        if cell['template'] != template['name'] or cell['sheet'] != sheet['name']:
                            continue
                        if cell.get('conflict'):
                            raise ValueError('同一原表位置的来源读数不一致')
                        address = cell['cell']
                        if address in occupied:
                            raise ValueError('不同巡检轮次对应了同一原表单元格')
                        occupied.add(address)
                        col, row = coordinate_from_string(address)
                        rows[row - 1][column_index_from_string(col) - 1] = cell.get('value')
                round_label = f'{slot[0]}{slot[1]}-' if slot else ''
                output.append({'name': f"原-{template['profession'][0]}-{round_label}{sheet['name'].strip()}",
                               'template': template['name'], 'sheet': sheet['name'],
                               'rows': rows, 'merges': sheet['merges']})
    return output


def refresh_archived_original(book, changes=None, actor=''):
    """Update only frozen, explicitly recorded mappings after a history edit."""
    if ORIGINAL_MAPPING_SHEET not in book:
        return  # Older archives did not contain original form sheets.
    mapping = book[ORIGINAL_MAPPING_SHEET]
    if tuple(cell.value for cell in mapping[1][:len(ORIGINAL_MAPPING_COLUMNS)]) != ORIGINAL_MAPPING_COLUMNS:
        raise ValueError('原表映射格式无效')
    review = book['现场核对']
    header = [cell.value for cell in review[1]]
    records = defaultdict(list)
    for number, values in enumerate(review.iter_rows(min_row=2, values_only=True), 2):
        row = dict(zip(header, values))
        records[(row.get('设备标识'), 'point', row.get('来源'), row.get('参数'), '', '')].append((number,row))
        records[(row.get('设备标识'), 'section', '', row.get('参数'), row.get('位置') or '', row.get('来源') or '')].append((number,row))
    changed = {int(row) for change in (changes or {}).values() for row in change.get('values', {})}
    targets = defaultdict(list)
    for values in mapping.iter_rows(min_row=2, max_col=len(ORIGINAL_MAPPING_COLUMNS), values_only=True):
        name, address, device, kind, point, parameter, location, form = values
        if not name and not address:
            continue
        if name not in book or not isinstance(address, str) or kind not in ('point', 'section'):
            raise ValueError('原表映射目标无效')
        key = (device, kind, point or '', parameter, location or '', form or '')
        matching = records.get(key, [])
        if len(matching) != 1:
            raise ValueError('原表对应的历史读数不唯一')
        targets[(name,address)].append((kind,*matching[0]))
    for (name,address), entries in targets.items():
        if len(entries)>2 or len({entry[0] for entry in entries})!=len(entries):
            raise ValueError('原表映射目标重复')
        selected = [entry for entry in entries if entry[1] in changed]
        values = [entry[2].get('现场读数') for entry in (selected or entries)]
        if any(not same_reading(values[0],value) for value in values[1:]):
            raise ValueError('同一原表位置修改了不同读数')
        value = values[0]
        if selected:
            for _,number,row in entries:
                if not same_reading(row.get('现场读数'),value):
                    review.cell(number,10).value = value
                    if isinstance(value,str):
                        review.cell(number,10).data_type = 's'
                    review.cell(number,14).value = actor
                    if isinstance(actor,str):
                        review.cell(number,14).data_type = 's'
        elif any(not same_reading(value,entry[2].get('现场读数')) for entry in entries):
            raise ValueError('同一原表位置的历史读数不一致')
        cell = book[name][address]
        if isinstance(cell, MergedCell):
            raise ValueError('原表映射落在合并单元格内')
        cell.value = value
        if isinstance(cell.value, str):
            cell.data_type = 's'
