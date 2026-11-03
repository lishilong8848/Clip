"""Read and correct the field-review worksheet in an archived inspection."""

import io
import re
from contextlib import contextmanager
from zipfile import BadZipFile, ZipFile

from openpyxl import load_workbook

from .risk_inspection import RiskError

REVIEW_SHEET = '现场核对'
REVIEW_COLUMNS = ('楼栋', '顺序', '设备标识', '设备', '专业', '位置', '来源', '参数',
                  '参考值', '现场读数', '参数状态', '核对结论', '现场备注', '填写人')
MAX_ARCHIVE = 20 * 1024 * 1024
MAX_ROWS = 100_000
UNSUPPORTED_CELL_TEXT = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f\ud800-\udfff\ufffe\uffff]')


def valid_cell_text(value, limit):
    return isinstance(value, str) and len(value) <= limit and not UNSUPPORTED_CELL_TEXT.search(value)


@contextmanager
def archived_workbook(content, archive_id, scope):
    try:
        with ZipFile(io.BytesIO(content)) as zipped:
            if sum(item.file_size for item in zipped.infolist()) > 80 * 1024 * 1024:
                raise ValueError('archive too large')
        book = load_workbook(io.BytesIO(content), keep_links=False)
    except (BadZipFile, ValueError, KeyError, OSError) as error:
        raise RiskError('归档附件无法读取为巡检表。', 422) from error
    try:
        if ('巡检信息' not in book or book['巡检信息']['A2'].value != archive_id or
                book['巡检信息']['I2'].value != scope + '楼'):
            raise RiskError('附件与原巡检记录不匹配，请核对飞书记录。', 409)
        if REVIEW_SHEET not in book:
            add_legacy_review_sheet(book, scope)
        yield book
    finally:
        book.close()


def add_legacy_review_sheet(book, scope):
    # Older exports still have the ordered device and point references.
    rows = []
    for name in ('DE巡检表', '测点读数'):
        if name not in book or book[name].max_row > MAX_ROWS:
            raise RiskError('历史附件缺少可核验的设备参数。', 422)
        for row in book[name].iter_rows(min_row=2, max_col=8, values_only=True):
            floor, order, device, profession, *values = row
            if floor != scope + '楼' or type(order) is not int or not device:
                raise RiskError('历史附件的设备顺序或楼栋无法核验。', 422)
            location, source, parameter, reference = values
            if name == '测点读数':
                location, source, parameter, reference = '', location, source, parameter
            rows.append((floor, order, f'{scope}:{order}', device, profession, location, source,
                         parameter, reference, None, None, None, None, None))
    if not rows:
        raise RiskError('历史附件没有可填写的设备参数。', 422)
    sheet = book.create_sheet(REVIEW_SHEET)
    sheet.freeze_panes = 'A2'
    for number, row in enumerate([REVIEW_COLUMNS, *sorted(rows, key=lambda row: row[1])], 1):
        for column, value in enumerate(row, 1):
            write_cell(sheet, number, column, value)


def write_cell(sheet, row, column, value):
    cell = sheet.cell(row, column)
    cell.value = value
    if isinstance(value, str):
        cell.data_type = 's'


def archived_devices(book, scope):
    sheet = book[REVIEW_SHEET]
    if sheet.max_row > MAX_ROWS or tuple(cell.value for cell in sheet[1][:len(REVIEW_COLUMNS)]) != REVIEW_COLUMNS:
        raise RiskError('现场核对工作表格式无法识别。', 422)
    devices = {}
    for number, row in enumerate(sheet.iter_rows(min_row=2, max_col=len(REVIEW_COLUMNS), values_only=True), 2):
        floor, order, identity, name, profession, location, source, parameter, reference, value, status, result, note, actor = row
        if floor != scope + '楼' or not isinstance(identity, str) or type(order) is not int or not name:
            raise RiskError('现场核对记录的设备或楼栋无法核验。', 422)
        result, note, actor = ('' if item is None else str(item) for item in (result, note, actor))
        device = devices.setdefault(identity, {'id': identity, 'name': name, 'order': order, 'profession': profession,
                                              'rows': [], 'result': result, 'note': note, 'actor': actor})
        if (device['name'], device['order'], device['profession'], device['result'], device['note']) != (
                name, order, profession, result, note):
            raise RiskError('同一设备的历史结论不一致，请先核对附件。', 409)
        device['rows'].append({'id': str(number), 'name': parameter, 'location': location, 'source': source,
                               'reference': reference, 'value': '' if value is None else str(value),
                               'read_only': status in ('配置停用', '无可填参数') or not parameter})
    if not devices:
        raise RiskError('归档附件没有可核验的现场设备。', 422)
    return sorted(devices.values(), key=lambda device: device['order'])


def edit_archived_devices(book, devices, changes, actor):
    known = {device['id']: device for device in devices}
    if not isinstance(changes, dict) or not changes or changes.keys() - known.keys():
        raise RiskError('请选择需要修改的历史设备。', 400)
    sheet = book[REVIEW_SHEET]
    for identity, change in changes.items():
        device = known[identity]
        values = change.get('values') if isinstance(change, dict) else None
        editable = {row['id'] for row in device['rows'] if not row['read_only']}
        if (not isinstance(values, dict) or values.keys() - editable or
                any(not valid_cell_text(value, 160) for value in values.values())):
            raise RiskError('历史设备读数格式无效。', 400)
        result, note = change.get('result'), change.get('note')
        if (result not in ('', '正常', '异常') or not valid_cell_text(note, 1000)
                or (result == '异常' and not note.strip())):
            raise RiskError(f'请核对「{device["name"]}」的结论及异常备注。', 400)
        for row in device['rows']:
            number = int(row['id'])
            if row['id'] in values:
                row['value'] = values[row['id']].strip()
                write_cell(sheet, number, 10, row['value'] or None)
            write_cell(sheet, number, 12, result or None)
            write_cell(sheet, number, 13, note.strip() or None)
            write_cell(sheet, number, 14, actor)
        device.update(result=result, note=note.strip(), actor=actor)
    completed = sum(device['result'] in ('正常', '异常') and (device['result'] != '异常' or bool(device['note'].strip()))
                    and all(row['read_only'] or row['value'].strip() for row in device['rows'])
                    for device in devices)
    return completed, sum(device['result'] == '异常' for device in devices)
