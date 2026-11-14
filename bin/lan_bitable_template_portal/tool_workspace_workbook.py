"""Describe and fill immutable XLSX tool self-check template snapshots.

Persist the original bytes and ``template_sha256`` when a session is opened.
Use that same snapshot for all edits and export, including retries.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import io
import math
import re
from collections.abc import Mapping
from copy import copy, deepcopy
from datetime import date, datetime, time
from unicodedata import east_asian_width
from zipfile import ZipFile

from openpyxl import load_workbook
from openpyxl.cell.cell import MergedCell
from openpyxl.drawing.image import Image as WorkbookImage
from openpyxl.drawing.spreadsheet_drawing import AnchorMarker, OneCellAnchor
from openpyxl.drawing.xdr import XDRPositiveSize2D
from openpyxl.styles.colors import COLOR_INDEX
from openpyxl.utils.cell import column_index_from_string, get_column_letter
from openpyxl.utils.units import EMU_to_pixels, pixels_to_EMU, points_to_pixels
from PIL import Image, ImageChops, UnidentifiedImageError

from .drill_management import _theme_colors
from .risk_inspection import RiskError, text
from .tool_self_check import RESULTS, parse_template, timestamp


_SIGNATURE_PREFIX = 'data:image/png;base64,'
_SIGNATURE_BYTES = 2 * 1024 * 1024
_SIGNATURE_DIMENSION = 2048
_SIGNATURE_PIXELS = 2_000_000
_ADDRESS = re.compile(r'[A-Z]{1,3}[1-9][0-9]{0,6}', re.ASCII)
_BAD_TEXT = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f\ud800-\udfff\ufffe\uffff]')
_ASSET_LABELS = {
    'name': {'仪器名称', '工具名称', '设备名称'},
    'model': {'规格/型号', '规格／型号', '型号', '规格型号'},
    'brand': {'制造厂商', '生产厂商', '制造商', '品牌'},
}


def _snapshot_schema(content, expected_sha256):
    if not isinstance(content, bytes):
        raise RiskError('请使用原始 XLSX 模板字节。')
    digest = hashlib.sha256(content).hexdigest()
    if expected_sha256 is not None and expected_sha256 != digest:
        raise RiskError('自检模板与工单保存的快照不一致。', 409)
    return deepcopy(parse_template(content)), digest


def _key(sheet, address):
    return f'{sheet.title}!{address}'


def _layout(sheet, schema):
    hvac = schema.get('layout') == 'hvac'
    assets = {'scope': 'C5' if hvac else 'C3', 'location': 'D11' if hvac else 'D8'}
    inspector, checked_at = ('G6', 'G11') if hvac else ('G4', 'G8')
    editable = {inspector, checked_at, 'C8' if hvac else 'C6'}
    editable.update(schema.get('fields', ()))
    for item in schema.get('calibration', ()):
        editable.update(item['fields'])
    merged = {(item.min_row, item.min_col): item for item in sheet.merged_cells.ranges}
    if hvac:
        assets.update(name='C3', model='C4', brand='G4')
    else:
        # Only use explicitly labeled slots in the header of a derived electrical template.
        for row in sheet.iter_rows(min_row=3, max_row=9, max_col=9):
            for cell in row:
                if not isinstance(cell.value, str):
                    continue
                label = cell.value.strip().rstrip('：:').strip()
                for field, labels in _ASSET_LABELS.items():
                    if field in assets or label not in labels:
                        continue
                    region = merged.get((cell.row, cell.column))
                    target_column = (region.max_col if region else cell.column) + 1
                    if target_column > 9:
                        continue
                    target = sheet.cell(cell.row, target_column)
                    if not isinstance(target, MergedCell) and target.value in (None, '') and target.coordinate not in editable | set(assets.values()):
                        assets[field] = target.coordinate
    editable.update(assets.values())
    checks = {f'I{entry["row"]}' for entry in schema['checks']}
    for address in editable | checks:
        if isinstance(sheet[address], MergedCell) or sheet[address].data_type == 'f':
            raise RiskError(f'模板的 {address} 不是可填写单元格。')
    return editable, checks, assets


def _review_addresses(schema):
    row = schema['review_row']
    return {'reviewer_address': f'E{row}', 'reviewed_at_address': f'G{row}', 'signature_address': f'E{row}'}


def _column_width(sheet, index):
    width = sheet.sheet_format.defaultColWidth
    if width is None:
        width = sheet.sheet_format.baseColWidth or 8.43
    for key, dimension in sheet.column_dimensions.items():
        first = dimension.min or column_index_from_string(key)
        if first <= index <= (dimension.max or first):
            width = dimension.width
            if dimension.hidden:
                return 0
    return int(width * (12 if width < 1 else 7) + (0 if width < 1 else 5) + 0.5)


def _row_height(sheet, index):
    dimension = sheet.row_dimensions.get(index)
    if dimension and dimension.hidden:
        return 0
    height = dimension.height if dimension and dimension.height is not None else sheet.sheet_format.defaultRowHeight
    return points_to_pixels(15 if height is None else height)


def _color(color, theme):
    if color is None:
        return ''
    if color.type == 'theme':
        return theme.get(color.theme, '')
    rgb = color.rgb if color.type == 'rgb' else None
    if color.type == 'indexed' and 0 <= color.indexed < len(COLOR_INDEX):
        rgb = COLOR_INDEX[color.indexed]
    return f'#{rgb[-6:]}' if isinstance(rgb, str) and re.fullmatch(r'[0-9a-fA-F]{8}', rgb) else ''


def _invisible_input_ink(cell, theme):
    # Native HVAC name/model/brand slots inherit white title text on white cells.
    background = (_color(cell.fill.fgColor, theme) if cell.fill.patternType == 'solid'
                  else '#ffffff' if cell.fill.patternType is None else '')
    return _color(cell.font.color, theme).lower() == background.lower() == '#ffffff'


def _style(cell, theme):
    font, alignment = cell.font, cell.alignment
    style = {'whiteSpace': 'pre-wrap' if alignment.wrap_text else 'pre'}
    if font.name:
        name = font.name.replace('\\', '').replace('"', '').replace('\n', '')
        style['fontFamily'] = f'"{name}", sans-serif'
    if font.sz:
        style['fontSize'] = f'{font.sz:g}pt'
    if font.b:
        style['fontWeight'] = 'bold'
    if font.i:
        style['fontStyle'] = 'italic'
    decoration = [value for enabled, value in ((font.u, 'underline'), (font.strike, 'line-through')) if enabled]
    if decoration:
        style['textDecoration'] = ' '.join(decoration)
    if color := _color(font.color, theme):
        style['color'] = color
    if cell.fill.patternType == 'solid' and (color := _color(cell.fill.fgColor, theme)):
        style['backgroundColor'] = color
    horizontal = alignment.horizontal
    if horizontal in {'left', 'center', 'right', 'justify', 'centerContinuous', 'distributed'}:
        style['textAlign'] = {'centerContinuous': 'center', 'distributed': 'justify'}.get(horizontal, horizontal)
    else:
        style['textAlign'] = 'right' if cell.data_type == 'n' and cell.value is not None else 'left'
    style['verticalAlign'] = {'center': 'middle', 'top': 'top'}.get(alignment.vertical, 'bottom')
    for edge in ('top', 'right', 'bottom', 'left'):
        side = getattr(cell.border, edge)
        if side and side.style:
            width = 3 if side.style in {'thick', 'double'} else 2 if side.style.startswith('medium') else 1
            line = 'double' if side.style == 'double' else 'dotted' if side.style == 'dotted' else 'dashed' if 'dash' in side.style.lower() else 'solid'
            style[f'border{edge.title()}'] = f'{width}px {line} {_color(side.color, theme) or "#000000"}'
    return style


def _preview_value(value):
    if isinstance(value, (date, datetime, time)):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _images(sheet, columns, heights):
    images = []
    for image in sheet._images:
        anchor = image.anchor
        marker = getattr(anchor, '_from', None)
        if marker is None or marker.col >= len(columns) or marker.row >= len(heights):
            continue
        if image.format not in {'png', 'jpeg', 'gif'}:
            continue
        offset_x, offset_y = EMU_to_pixels(marker.colOff), EMU_to_pixels(marker.rowOff)
        if end := getattr(anchor, 'to', None):
            width = sum(columns[marker.col:end.col]) + EMU_to_pixels(end.colOff) - offset_x
            height = sum(heights[marker.row:end.row]) + EMU_to_pixels(end.rowOff) - offset_y
        elif size := getattr(anchor, 'ext', None):
            width, height = EMU_to_pixels(size.cx), EMU_to_pixels(size.cy)
        else:
            width, height = image.width, image.height
        if width <= 0 or height <= 0:
            continue
        encoded = base64.b64encode(image._data()).decode('ascii')
        images.append({'address': f'{get_column_letter(marker.col + 1)}{marker.row + 1}',
                       'src': f'data:image/{image.format};base64,{encoded}',
                       'offsetX': offset_x, 'offsetY': offset_y, 'width': width, 'height': height})
    return images


def _describe_sheet(sheet, editable, checks, theme):
    populated = [cell for cell in sheet._cells.values()
                 if cell.value is not None and (not isinstance(cell.value, str) or cell.value.strip())]
    max_row = max((cell.row for cell in populated), default=1)
    max_col = max((cell.column for cell in populated), default=1)
    merges = {}
    covered = set()
    for region in sheet.merged_cells.ranges:
        if region.max_col > 24 or region.max_row > 113:
            raise RiskError('模板合并单元格超出可预览范围。')
        first = (region.min_row, region.min_col)
        merges[first] = (region.size['rows'], region.size['columns'])
        covered.update(point for point in region.cells if point != first)
        max_row, max_col = max(max_row, region.max_row), max(max_col, region.max_col)
    if max_row > 113 or max_col > 24:
        raise RiskError('模板数据超出可预览范围。')
    columns = [_column_width(sheet, index) for index in range(1, max_col + 1)]
    heights = [_row_height(sheet, index) for index in range(1, max_row + 1)]
    rows = []
    for index, height in enumerate(heights, 1):
        cells = []
        for column in range(1, max_col + 1):
            if (index, column) in covered:
                continue
            cell = sheet.cell(index, column)
            rowspan, colspan = merges.get((index, column), (1, 1))
            check = cell.coordinate in checks
            can_edit = check or cell.coordinate in editable
            style = _style(cell, theme)
            if can_edit and _invisible_input_ink(cell, theme):
                style['color'] = '#000000'
            cells.append({'address': cell.coordinate, 'value': _preview_value(cell.value),
                          'rowspan': rowspan, 'colspan': colspan, 'style': style,
                          'editable': can_edit, **({'check': True} if check else {})})
        rows.append({'index': index, 'height': height, 'cells': cells})
    return {'name': sheet.title, 'columns': columns, 'rows': rows, 'images': _images(sheet, columns, heights)}


def describe(content):
    """Return JSON-safe original grids, template schema and the snapshot digest."""
    schema, digest = _snapshot_schema(content, None)
    with ZipFile(io.BytesIO(content)) as package:
        theme = _theme_colors(package)
    book = load_workbook(io.BytesIO(content), keep_links=False)
    try:
        editable, checks, _ = _layout(book[schema['sheet']], schema)
        sheets = []
        for sheet in book.worksheets:
            if sheet.title == schema['sheet']:
                sheets.append(_describe_sheet(sheet, editable, checks, theme) | {'review': _review_addresses(schema)})
            elif sheet.sheet_state == 'visible':
                sheets.append(_describe_sheet(sheet, set(), set(), theme))
        return {'sheets': sheets, 'schema': schema, 'template_sha256': digest}
    finally:
        book.close()


def _asset_values(asset, addresses):
    if not isinstance(asset, Mapping):
        raise RiskError('工具资料格式不正确。')
    values = {}
    for field, address in addresses.items():
        value = _literal(asset.get(field) or '', 500)
        if field == 'scope':
            if value in {'A', 'B', 'C', 'D', 'E', 'H'}:
                value += '楼'
            elif value == '110':
                value = '110站'
        if value:
            values[address] = value
    return values


def prefill(content, asset):
    """Return safe qualified cell keys for the asset fields present in this template."""
    schema, _ = _snapshot_schema(content, None)
    book = load_workbook(io.BytesIO(content), keep_links=False)
    try:
        sheet = book[schema['sheet']]
        _, _, addresses = _layout(sheet, schema)
        return {_key(sheet, address): value for address, value in _asset_values(asset, addresses).items()}
    finally:
        book.close()


def _literal(value, limit):
    if not isinstance(value, str) or _BAD_TEXT.search(value) or value.lstrip().startswith('='):
        raise RiskError('自检单元格只接受普通文字，不能填写公式或控制字符。')
    if len(value) > limit:
        raise RiskError(f'自检单元格内容不能超过 {limit} 字。')
    return value


def _validated_map(sheet, allowed, entries, *, is_check):
    if not isinstance(entries, Mapping) or len(entries) > len(allowed):
        raise RiskError('自检填写项目格式或数量不正确。')
    result = {}
    for key, value in entries.items():
        title, separator, address = key.rpartition('!') if isinstance(key, str) else ('', '', '')
        if not separator or title != sheet.title or not _ADDRESS.fullmatch(address) or address not in allowed:
            raise RiskError('自检数据中包含无效或不可编辑的单元格。')
        if is_check:
            if value == '':
                continue
            if value not in RESULTS:
                raise RiskError('自检项目只能填写“是”或“否”。')
            result[address] = value
        else:
            result[address] = _literal(value, 1500)
    return result


def _validate_sheet(sheet, schema, values, checks, complete):
    editable, check_cells, asset_fields = _layout(sheet, schema)
    values = _validated_map(sheet, editable, values, is_check=False)
    answers = _validated_map(sheet, check_cells, checks, is_check=True)
    if complete and answers.keys() != check_cells:
        raise RiskError('请完成表格中的所有“是／否”自检项目。')
    return values, answers, asset_fields


def validate_inputs(content, values, checks, *, complete=False, expected_sha256=None):
    """Validate draft cells, optionally requiring every original check cell."""
    schema, _ = _snapshot_schema(content, expected_sha256)
    book = load_workbook(io.BytesIO(content), keep_links=False)
    try:
        _validate_sheet(book[schema['sheet']], schema, values, checks, complete)
    finally:
        book.close()


def _signature(signature):
    if not isinstance(signature, str) or not signature.startswith(_SIGNATURE_PREFIX):
        raise RiskError('请提供 PNG 格式的审核签名。')
    encoded = signature[len(_SIGNATURE_PREFIX):]
    if not encoded or len(encoded) > 4 * ((_SIGNATURE_BYTES + 2) // 3):
        raise RiskError('审核签名为空或超过 2 MB。')
    try:
        data = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError):
        raise RiskError('审核签名的图片编码无效。') from None
    if not data or len(data) > _SIGNATURE_BYTES:
        raise RiskError('审核签名为空或超过 2 MB。')
    if not data.startswith(b'\x89PNG\r\n\x1a\n'):
        raise RiskError('请提供 PNG 格式的审核签名。')
    try:
        with Image.open(io.BytesIO(data), formats=['PNG']) as image:
            width, height = image.size
            if max(width, height) > _SIGNATURE_DIMENSION or width * height > _SIGNATURE_PIXELS:
                raise RiskError('审核签名图片尺寸过大。')
            if getattr(image, 'is_animated', False):
                raise RiskError('请提供静态 PNG 审核签名。')
            image.verify()
        # verify() checks the PNG structure, load() also checks compressed image data.
        with Image.open(io.BytesIO(data), formats=['PNG']) as image:
            rgba = image.convert('RGBA')
            ink = ImageChops.multiply(ImageChops.invert(rgba.convert('L')), rgba.getchannel('A'))
            if not ink.getbbox():
                raise RiskError('审核签名没有可见笔迹，请重新签名。')
    except (UnidentifiedImageError, Image.DecompressionBombError, OSError, SyntaxError, ValueError):
        raise RiskError('审核签名图片损坏或无法读取。') from None
    return data, width, height


def validate_signature(signature):
    """Raise RiskError unless signature is a bounded static PNG data URL."""
    _signature(signature)


def _embed_signature(sheet, cell, signature):
    data, width, height = signature
    region = next((item for item in sheet.merged_cells.ranges if cell.coordinate in item), None)
    last_column = region.max_col if region else cell.column
    last_row = region.max_row if region else cell.row
    column_width = sum(_column_width(sheet, col) for col in range(cell.column, last_column + 1))
    row_height = sum(_row_height(sheet, row) for row in range(cell.row, last_row + 1))
    available_width = column_width - 6
    available_height = row_height - 6
    if available_width <= 0 or available_height <= 0:
        raise RiskError('审核人单元格被隐藏，无法嵌入签名。')
    font_size = points_to_pixels(cell.font.sz or 11)
    text_width = sum(font_size if east_asian_width(char) in {'W', 'F', 'A'} else font_size * 0.6
                     for char in cell.value)
    text_height = (font_size + 4) * max(1, math.ceil(text_width / available_width))
    alignment = copy(cell.alignment)
    # Fit inside the original reviewer span; changing row heights also moves the
    # native date field and print layout. Short rows use name/signature side by side.
    if available_height >= text_height + 19:
        signature_width, signature_height = available_width, min(48, available_height - text_height - 3)
        left, top = 3, 3 + text_height + 3
        alignment.horizontal = 'center'
        alignment.vertical = 'top'
        alignment.wrap_text = True
    else:
        signature_width, signature_height = available_width - text_width - 6, min(48, available_height)
        if signature_width < 16 or available_height < font_size:
            raise RiskError('审核人姓名和签名无法放入原表审核区域，请缩短姓名或调整模板。')
        left, top = 3 + text_width + 6, 3
        alignment.horizontal = 'left'
        alignment.vertical = 'center'
        alignment.wrap_text = False
    scale = min(1, signature_width / width, signature_height / height)
    width, height = max(1, int(width * scale)), max(1, int(height * scale))
    left += (signature_width - width) / 2
    top += (signature_height - height) / 2
    alignment.indent = 0
    cell.alignment = alignment
    image = WorkbookImage(io.BytesIO(data))
    image.anchor = OneCellAnchor(
        _from=AnchorMarker(col=cell.column - 1, row=cell.row - 1,
                           colOff=pixels_to_EMU(left), rowOff=pixels_to_EMU(top)),
        ext=XDRPositiveSize2D(pixels_to_EMU(width), pixels_to_EMU(height)),
    )
    sheet.add_image(image)


def fill(content, values, reviewer, reviewed_at, signature, checks, asset, *, expected_sha256=None):
    """Fill the exact template snapshot and return a signed XLSX file as bytes."""
    schema, _ = _snapshot_schema(content, expected_sha256)
    reviewer = text(_literal(reviewer, 120), 120, required=True)
    if '\n' in reviewer or '\r' in reviewer:
        raise RiskError('审核人姓名应填写在同一行。')
    reviewed_at = timestamp(reviewed_at)
    if not reviewed_at:
        raise RiskError('请填写审核时间。')
    signed_image = _signature(signature)
    book = load_workbook(io.BytesIO(content), keep_links=False)
    try:
        sheet = book[schema['sheet']]
        values, answers, asset_fields = _validate_sheet(sheet, schema, values, checks, True)
        with ZipFile(io.BytesIO(content)) as package:
            theme = _theme_colors(package)
        fields = _asset_values(asset, asset_fields) | values
        for address, answer in answers.items():
            fields[address] = '是[√]  否[ ]' if answer == '是' else '是[ ]  否[√]'
        completed = all(answer == '是' for answer in answers.values())
        fields[f'E{schema["result_row"]}'] = '已完成[√]            未完成[ ]' if completed else '已完成[ ]            未完成[√]'
        review = _review_addresses(schema)
        fields[review['reviewer_address']] = reviewer
        fields[review['reviewed_at_address']] = reviewed_at.replace('T', ' ')
        for address, value in fields.items():
            cell = sheet[address]
            cell.value = value
            cell.data_type = 's'
            if _invisible_input_ink(cell, theme):
                font = copy(cell.font)
                font.color = '000000'
                cell.font = font
        _embed_signature(sheet, sheet[review['signature_address']], signed_image)
        for original_sheet in book.worksheets:
            # The writer derives this from dimensions that have not been grouped
            # in memory; retain the template's existing outline metadata.
            original_sheet.column_dimensions.max_outline = original_sheet.sheet_format.outlineLevelCol
        output = io.BytesIO()
        book.save(output)
        return output.getvalue()
    finally:
        book.close()
