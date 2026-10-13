"""Small office/engineering utilities for the packaged local MCP service."""
import datetime as dt
import decimal
import difflib
import json
import re

from .lighthouse_mcp_builtin import checked_text

TOOLS = {
    'table_statistics': 'Excel/CSV表格统计：按列求和、均值、最小最大值及分类数量',
    'compare_tables': '两份表格按唯一键核对新增、删除和字段变动',
    'convert_units': '电压、电流、功率、电量、长度、容量和压力单位换算',
    'working_days': '计算两个日期间工作日，支持指定休息日和补班日',
    'document_outline': '提取Markdown标题目录及行号，便于阅读工作文档',
    'compare_text': '对比两份文本或通告草稿的逐行差异，不写业务数据',
}


def table_rows(text):
    rows = json.loads(checked_text(text))
    if not isinstance(rows, list) or len(rows) > 1000 or any(not isinstance(row, dict) or len(row) > 100 for row in rows):
        raise ValueError('请提供最多1000条、100列的JSON对象数组。')
    return rows


def number(value):
    if isinstance(value, bool) or isinstance(value, (dict, list)):
        raise ValueError('请提供有效数值。')
    try:
        value = decimal.Decimal(str(value).replace(',', '').strip())
    except decimal.InvalidOperation:
        raise ValueError('请提供有效数值。') from None
    if not value.is_finite() or not value.is_zero() and abs(value.adjusted()) > 30:
        raise ValueError('数值超出范围。')
    return value


def table_statistics(text: str, numeric_column: str = '', group_column: str = '') -> dict:
    rows = table_rows(text)
    result = {'row_count': len(rows)}
    if group_column:
        if any(group_column not in row for row in rows):
            raise ValueError('分类列不存在于所有数据行。')
        groups = {}
        for row in rows:
            key = str(row[group_column])
            groups[key] = groups.get(key, 0) + 1
        result['groups'] = groups
    if numeric_column:
        if rows and not any(numeric_column in row for row in rows):
            raise ValueError('数值列不存在。')
        values, missing = [], 0
        for row in rows:
            if row.get(numeric_column) in (None, ''):
                missing += 1
                continue
            values.append(number(row[numeric_column]))
        result.update(valid_count=len(values), missing_count=missing)
        if values:
            with decimal.localcontext() as context:
                context.prec = 50
                total = sum(values)
                result.update(total=str(total), average=str(total / len(values)), minimum=str(min(values)), maximum=str(max(values)))
    return result


def compare_tables(before: str, after: str, key_column: str) -> dict:
    def keyed(text):
        result = {}
        for row in table_rows(text):
            if key_column not in row or isinstance(row[key_column], (dict, list)):
                raise ValueError('唯一键字段缺失或格式无效。')
            key = json.dumps(row[key_column], ensure_ascii=False, sort_keys=True)
            if key in result:
                raise ValueError('唯一键存在重复，请先核对原数据。')
            result[key] = row
        return result
    old, new = keyed(before), keyed(after)
    added = [row for key, row in new.items() if key not in old]
    removed = [row for key, row in old.items() if key not in new]
    changed = [{'key': new[key][key_column], 'before': old[key], 'after': new[key]} for key in new if key in old and old[key] != new[key]]
    return {'added': added, 'removed': removed, 'changed': changed,
            'added_count': len(added), 'removed_count': len(removed), 'changed_count': len(changed),
            'unchanged_count': sum(old[key] == new[key] for key in old.keys() & new.keys())}


UNITS = {
    'V': ('voltage', '1'), 'kV': ('voltage', '1000'),
    'A': ('current', '1'), 'mA': ('current', '.001'), 'kA': ('current', '1000'),
    'W': ('power', '1'), 'kW': ('power', '1000'), 'MW': ('power', '1000000'),
    'Wh': ('energy', '1'), 'kWh': ('energy', '1000'), 'MWh': ('energy', '1000000'),
    'mm': ('length', '.001'), 'cm': ('length', '.01'), 'm': ('length', '1'), 'km': ('length', '1000'),
    'mL': ('volume', '.000001'), 'L': ('volume', '.001'), 'm3': ('volume', '1'),
    'Pa': ('pressure', '1'), 'kPa': ('pressure', '1000'), 'MPa': ('pressure', '1000000'),
}


def convert_units(value: str, from_unit: str, to_unit: str) -> dict:
    if from_unit not in UNITS or to_unit not in UNITS or UNITS[from_unit][0] != UNITS[to_unit][0]:
        raise ValueError('仅支持同一物理量的单位换算；功率与电量不能直接互换。')
    with decimal.localcontext() as context:
        context.prec = 50
        result = number(value) * decimal.Decimal(UNITS[from_unit][1]) / decimal.Decimal(UNITS[to_unit][1])
    return {'input': value, 'from_unit': from_unit, 'to_unit': to_unit, 'result': str(result)}


def working_days(start_date: str, end_date: str, holidays: list[str] = [], work_dates: list[str] = []) -> dict:
    start, end = dt.date.fromisoformat(start_date), dt.date.fromisoformat(end_date)
    if start > end or (end - start).days > 3660 or len(holidays) + len(work_dates) > 1000:
        raise ValueError('日期范围须按先后顺序，最多10年、1000个特殊日期。')
    rest = {dt.date.fromisoformat(value) for value in holidays}
    work = {dt.date.fromisoformat(value) for value in work_dates}
    if rest & work:
        raise ValueError('同一天不能同时设置休息与补班。')
    count = 0
    for offset in range((end - start).days + 1):
        day = start + dt.timedelta(days=offset)
        count += day in work or day.weekday() < 5 and day not in rest
    return {'start_date': start_date, 'end_date': end_date, 'working_days': count,
            'note': '包含起止日期，默认周一至周五；仅使用本次提供的休息日/补班日，未核验法定节假日或人员排班。'}


def document_outline(text: str) -> dict:
    entries = []
    fenced = False
    for index, line in enumerate(checked_text(text).splitlines(), 1):
        if re.match(r'^\s*(```|~~~)', line):
            fenced = not fenced
        if not fenced:
            match = re.match(r'^\s{0,3}(#{1,6})\s+(.+?)\s*#*\s*$', line)
            if match:
                entries.append({'level': len(match[1]), 'title': match[2], 'line': index})
    return {'headings': entries, 'count': len(entries)}


def compare_text(before: str, after: str) -> dict:
    checked_text(before)
    checked_text(after)
    if len(before.splitlines()) + len(after.splitlines()) > 2000:
        raise ValueError('两份文本合计最多2000行。')
    diff = '\n'.join(difflib.unified_diff(before.splitlines(), after.splitlines(), fromfile='before', tofile='after', lineterm=''))
    return {'changed': before != after, 'diff': diff}


def functions():
    return {name: globals()[name] for name in TOOLS}
