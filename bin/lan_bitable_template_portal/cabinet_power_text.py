"""Parse copied confirmation-table text; never persist or upload business data."""
import csv
import datetime as dt
import re
import unicodedata

from .cabinet_power_excel import CabinetError, OPS, system_name

MAX_TEXT_BYTES = 200_000
ROOM = re.compile(r"(?<![A-Z0-9])(?:EA118\s*[-_]\s*(?P<full>[A-E](?:[1-4]\s*[-_]\s*\d{1,2}|\s*[-_]\s*[1-4]\d{2})(?:运营商机房)?)|(?P<short>[A-E][1-4]\s*[-_]\s*\d{1,2})\s*[.]\s*EA118)(?![A-Z0-9])", re.I)
RACK = re.compile(r"(?<![A-Z0-9])([A-Z]\d{2})(?![A-Z0-9])", re.I)
ACTION = re.compile('|'.join(re.escape(value) for value in sorted(OPS, key=len, reverse=True)))
DATE = re.compile(r"20\d{2}[-/.]\d{1,2}[-/.]\d{1,2}[ T\n]+\d{1,2}:\d{2}(?::\d{2})?(?![\d:])")
RESULT = re.compile(r"(?<![A-Za-z])(Success|Failed|Failure|Fail)(?![A-Za-z])|成功|失败", re.I)


def parse_confirmation_text(raw):
    if not isinstance(raw, str) or not raw.strip():
        raise CabinetError('请粘贴机柜确认信息', 400)
    if len(raw.encode('utf-8')) > MAX_TEXT_BYTES:
        raise CabinetError('单次粘贴文本不能超过200KB，请分次粘贴', 413)
    text = unicodedata.normalize('NFKC', raw).replace('\r', '').replace('\u200b', '')
    anchors = list(ROOM.finditer(text))
    rows = []
    problems = []
    for index, anchor in enumerate(anchors):
        end = anchors[index + 1].start() if index + 1 < len(anchors) else len(text)
        tail = text[anchor.end():end]
        room_text = re.sub(r'\s+', '', anchor.group('full') or anchor.group('short')).upper()
        parts = re.fullmatch(r'([A-E])(?:([1-4])[-_](\d{1,2})|[-_]([1-4]\d{2})(?:运营商机房)?)', room_text)
        if not parts: continue
        action = ACTION.search(tail)
        rack = RACK.search(tail[:action.start()] if action else tail)
        dates = list(DATE.finditer(tail))
        # The package and system-name columns often repeat the same room before the rack.
        if not rack and not action and not dates: continue
        if len(dates)>2 and len(list(ACTION.finditer(tail)))>1:
            raise CabinetError('部分机柜行缺少包间系统名称，无法区分对应记录，请重新粘贴完整机柜行',400)
        expected = actual = ''
        fields = None
        line = text[anchor.start():end].split('\n', 1)[0]
        delimiter = '\t' if '\t' in line else '|' if '|' in line else None
        if delimiter:
            fields = next(csv.reader([line.strip().strip('|')], delimiter=delimiter))
            fields = [value.strip() for value in fields]
        if fields and len(fields) >= 5 and ROOM.search(fields[0]):
            # Keep empty or malformed date cells in their original column, never shift them.
            expected, actual = fields[3], fields[4]
        elif len(dates) >= 2:
            expected, actual = dates[0].group(), dates[1].group()
        header = text[:anchor.start()]
        expected_header, actual_header = header.rfind('期望完成时间'), header.rfind('实际完成时间')
        if 0 <= actual_header < expected_header and '\n\n' not in header[min(actual_header, expected_header):]:
            expected, actual = actual, expected
        def normalize_date(value):
            match = DATE.fullmatch(value.strip())
            if not match: return value.strip()
            value = re.sub(r'[ T\n]+', ' ', value.strip()).replace('/', '-').replace('.', '-')
            try: return dt.datetime(*(int(part) for part in re.findall(r'\d+', value))).isoformat(sep=' ', timespec='seconds')
            except ValueError: return value
        result_tail = tail[dates[1].end():] if len(dates) >= 2 else ''
        result = RESULT.search(result_tail)
        supplier = RACK.search(result_tail[:result.start()] if result else result_tail)
        value = result.group().lower() if result else ''
        room = parts[4] or f'{parts[2]}{int(parts[3]):02d}'
        rows.append({'scope':parts[1], 'room':room, 'system_name':system_name(parts[1],room),
                     'rack':rack[1].upper() if rack else '', 'action':action.group() if action else '',
                     'expected':normalize_date(expected), 'actual':normalize_date(actual),
                     'supplier_rack':supplier[1].upper() if supplier else '',
                     'result':'成功' if value in ('success', '成功') else '失败' if value else '',
                     'raw_text':text[anchor.start():end].strip()[:2000]})
        labels={'rack':'机柜编号','action':'操作类型','expected':'期望完成时间','actual':'实际完成时间'}
        missing=[label for key,label in labels.items() if not rows[-1][key]]
        if missing: problems.append(f"第{len(rows)}条缺少"+'、'.join(missing))
        for key in ('expected','actual'):
            if rows[-1][key]:
                try: dt.datetime.fromisoformat(rows[-1][key])
                except ValueError: problems.append(f"第{len(rows)}条{labels[key]}不完整或格式无效")
        if len(rows) > 2000: raise CabinetError('单批最多2000条机柜记录', 413)
    if problems: raise CabinetError('；'.join(problems[:5])+'，请重新复制完整内容后粘贴',400)
    if not rows:
        missing=[]
        if not anchors: missing.append('包间系统名称（如 EA118-E2-2）')
        if not RACK.search(text): missing.append('机柜编号（如 A11）')
        if not ACTION.search(text): missing.append('操作类型')
        if len(DATE.findall(text))<2: missing.append('期望完成时间和实际完成时间')
        raise CabinetError(('缺少'+ '、'.join(missing) if missing else '内容无法对应到完整机柜行')+'，请重新复制完整内容后粘贴', 400)
    return rows
