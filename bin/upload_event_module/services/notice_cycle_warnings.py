"""Advisory-only maintenance cycle checks; never change notice text or upload fields."""
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
_PATTERN = re.compile('|'.join(re.escape(term) for term in sorted(_OWNER, key=len, reverse=True)))


def maintenance_cycle_rules():
    return [{'value': value, 'aliases': list(aliases)} for value, aliases in _CYCLES]


def maintenance_cycle_warnings(notice_type, cycle, content='', reason=''):
    if notice_type != '维保通告':
        return []
    selected = _OWNER.get(str(cycle or '').strip())
    if not selected:
        return []
    warnings = []
    for field, label, value in (('content', '内容', content), ('reason', '原因', reason)):
        # Longest alternatives consume e.g. 半年度 before 年度, and 每两年 before 年检.
        terms = list(dict.fromkeys(match[0] for match in _PATTERN.finditer(str(value or ''))
            if _OWNER[match[0]] != selected))
        if terms:
            warnings.append({'field': field, 'label': label, 'cycle': selected, 'terms': terms,
                'message': f'已选维保周期“{selected}”，【{label}】含其它周期字样“{"、".join(terms)}”，请核对（仅提醒，不阻止发送）。'})
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
