"""Maintenance cycle validation; never rewrite the user's notice text."""
import re


_CYCLES = (
    ('每月', ('每月', '月度', '按月', '月检')),
    ('每季', ('每季度', '每季', '季度', '按季', '季检')),
    ('每年', ('每年度', '每年', '年度', '按年', '年检')),
    ('半年', ('每半年度', '每半年', '半年度', '半年', '每六个月', '每6个月')),
    ('每两年', ('每两年', '每二年', '每2年', '两年一次', '二年一次', '2年一次', '双年度', '双年')),
    ('每三年', ('每三年', '每3年', '三年一次', '3年一次')),
    ('每五年', ('每五年', '每5年', '五年一次', '5年一次')),
    ('冬季保温每日', ('冬季保温每日', '每日', '每天', '日度', '按日', '日检')),
    ('每周', ('每星期', '每周', '周度', '按周', '周检')),
    ('非计划性', ('非计划性', '非计划')),
)
_OWNER = {alias: value for value, aliases in _CYCLES for alias in aliases}
_INTERVALS = {
    '每月': ('month', 1), '每季': ('month', 3), '每年': ('month', 12), '半年': ('month', 6),
    '每两年': ('month', 24), '每三年': ('month', 36), '每五年': ('month', 60),
    '冬季保温每日': ('day', 1), '每周': ('day', 7), '非计划性': ('unplanned', 1),
}
_DIGITS = dict(zip('零〇一二两三四五六七八九', (0, 0, 1, 2, 2, 3, 4, 5, 6, 7, 8, 9)))
MAINTENANCE_CYCLE_DURATION_PATTERN = r'(?<![\d零〇一二两三四五六七八九十百千年月日号])(?:每隔|每|按|隔)?\s*([0-9]+(?:\.[0-9]+)?|[零〇一二两三四五六七八九十百千]+|半)\s*(?:个\s*)?(季度|年度|星期|个月|月|季|年|周|天|日)(?:\s*(?:一次|1次|/次))?(?![\d])'
MAINTENANCE_CYCLE_DATE_PATTERN = r'(?<!\d)(?:\d{4}\s*年(?:\s*\d{1,2}\s*月)?(?:\s*\d{1,2}\s*[日号])?|\d{1,2}\s*月\s*\d{1,2}\s*[日号])'
_DURATION_RE = re.compile(MAINTENANCE_CYCLE_DURATION_PATTERN)
_PATTERN = re.compile('|'.join(re.escape(term) for term in sorted(_OWNER, key=len, reverse=True)) + '|' + MAINTENANCE_CYCLE_DURATION_PATTERN)


def _cycle_interval(term):
    if term in _OWNER:
        return _INTERVALS[_OWNER[term]]
    match = _DURATION_RE.fullmatch(term)
    if not match:
        return None
    quantity, unit = match.groups()
    if unit == '月' and '个' not in term and not term.startswith(('每', '按', '隔')) and not term.endswith(('一次', '1次', '/次')):
        return None
    if quantity == '半':
        count = 0.5
    elif quantity[0].isdigit():
        count = float(quantity)
    else:
        count, digit = 0, 0
        for char in quantity:
            if char in _DIGITS:
                digit = _DIGITS[char]
            else:
                count += (digit or 1) * {'十': 10, '百': 100, '千': 1000}[char]
                digit = 0
        count += digit
    # Calendar years are dates, not maintenance intervals; months are not fixed-day durations.
    if count <= 0 or (unit in ('年', '年度') and count >= 1000):
        return None
    factor = {'季度': 3, '季': 3, '年': 12, '年度': 12, '星期': 7, '周': 7}.get(unit, 1)
    return ('day' if unit in ('星期', '周', '天', '日') else 'month', count * factor)


def maintenance_cycle_rules():
    return [{'value': value, 'aliases': list(aliases), 'interval': list(_INTERVALS[value])} for value, aliases in _CYCLES]


def maintenance_cycle_warnings(notice_type, cycle, content='', reason=''):
    if notice_type != '维保通告':
        return []
    selected_text = str(cycle or '').strip()
    selected = _cycle_interval(selected_text)
    if not selected:
        return []
    warnings = []
    for field, label, value in (('content', '内容', content), ('reason', '原因', reason)):
        scanned = re.sub(MAINTENANCE_CYCLE_DATE_PATTERN, ' ', str(value or ''))
        terms = list(dict.fromkeys(match[0].strip() for match in _PATTERN.finditer(scanned)
            if _cycle_interval(match[0].strip()) not in (None, selected)))
        if terms:
            label_cycle = _OWNER.get(selected_text, selected_text)
            warnings.append({'field': field, 'label': label, 'cycle': label_cycle, 'terms': terms,
                'message': f'已选维保周期“{label_cycle}”，【{label}】含不一致的周期“{"、".join(terms)}”，请修改一致后再发送开始、更新或结束。'})
    return warnings


def maintenance_warnings_from_notice(notice_type, cycle, text):
    if notice_type != '维保通告':
        return []
    from .handlers.maintenance_notice import MaintenanceNoticeHandler
    handler = MaintenanceNoticeHandler()
    return maintenance_cycle_warnings(notice_type, cycle,
        handler._extract_section(str(text or ''), '内容'), handler._extract_section(str(text or ''), '原因'))


def maintenance_reminder_text(notice_type, cycle, text):
    warnings = maintenance_warnings_from_notice(notice_type, cycle, text)
    if not warnings:
        return text
    return str(text).rstrip() + '\n\n【维保周期核对提醒】\n' + '\n'.join(w['message'] for w in warnings)


def maintenance_cycle_error(notice_type, cycle, text):
    return '\n'.join(w['message'] for w in maintenance_warnings_from_notice(notice_type, cycle, text))
