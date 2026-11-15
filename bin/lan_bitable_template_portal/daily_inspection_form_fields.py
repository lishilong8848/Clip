"""Identification and rendering of non-measurement fields on inspection forms."""

import datetime as dt
import re
from functools import lru_cache

from openpyxl.utils.cell import get_column_letter, range_boundaries


CHILLER_SHEET = '暖通冷机系统巡检表'
_REMARK = re.compile(r'\s*备注\s*([：:]?)\s*')
_DATE = re.compile(r'(?:日\s*期\s*[:：]\s*)?(?:[0-9]{4}\s*)?年\s*(?:[0-9]{1,2}\s*)?月\s*(?:[0-9]{1,2}\s*)?日')
_DATE_PART = re.compile(r'\s*(?:[0-9]+\s*)?([年月日])\s*')
_SHIFT_NAME = re.compile(r'([白夜])班值班员\s*[:：][ \u3000]*')


def remark_cells(sheet):
    """Use the text anchor of a remark area, including vertically merged fields."""
    merge_ends = {}
    for area in sheet.get('merges', []):
        col, row, _, last = range_boundaries(area)
        merge_ends[row, col] = last
    result = []
    for row, values in enumerate(sheet['rows'], 1):
        for col, value in enumerate(values, 1):
            match = _REMARK.fullmatch(str(value or ''))
            if not match:
                continue
            bottom = row if match[1] else merge_ends.get((row, col), row) + 1
            if bottom <= len(sheet['rows']) and col <= len(sheet['rows'][bottom - 1]):
                target = sheet['rows'][bottom - 1][col - 1]
                if match[1] or not str(target or '').strip():
                    result.append(get_column_letter(col) + str(bottom))
    return result


@lru_cache(maxsize=2)
def remark_positions(scope):
    from .daily_inspection_original import original_templates

    return frozenset((template['name'], sheet['name'], cell)
                     for template in original_templates() if template['scope'] == scope
                     for sheet in template['sheets'] for cell in remark_cells(sheet))


def form_header(sheet, duty, *, inspector='', started=None):
    """Fill the original header anchors without changing any merges."""
    if len(sheet['rows']) < 2:
        return {}
    source = sheet['rows'][1]
    date = duty.get('date')
    if not date:
        return {}
    day = dt.date.fromisoformat(date)
    changes = {}
    chiller = sheet['name'].strip() == CHILLER_SHEET
    if chiller:
        if started:
            day = started.date()
            changes['F2'] = started.strftime('%H:%M')
        if duty.get('shift') in ('白', '夜'):
            changes['D2'] = duty['shift'] + '班'
        if inspector:
            changes['H2'] = inspector

    parts = dict(zip('年月日', (day.year, day.month, day.day)))
    people = duty.get('shift_names') or {}
    for col, raw in enumerate(source, 1):
        if not isinstance(raw, str):
            continue
        if part := _DATE_PART.fullmatch(raw):
            value = str(parts[part[1]]) + part[1]
        else:
            value = _DATE.sub(f'日期：{day:%Y-%m-%d}', raw)

            def add_names(match):
                names = people.get(match[1]) or []
                return match[0] if not names else f'{match[1]}班值班员：{"、".join(names)}  '

            if not chiller:
                value = _SHIFT_NAME.sub(add_names, value)
        if value != raw:
            changes[get_column_letter(col) + '2'] = value
    return changes
