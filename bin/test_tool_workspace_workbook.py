import base64
from copy import copy
import hashlib
import io
import json
from pathlib import Path
import unittest
from xml.etree import ElementTree as ET
from zipfile import ZipFile

from openpyxl import load_workbook
from openpyxl.utils.units import EMU_to_pixels, points_to_pixels
from PIL import Image, ImageDraw

from lan_bitable_template_portal.risk_inspection import RiskError
from lan_bitable_template_portal.tool_self_check import TEMPLATES, parse_template
from lan_bitable_template_portal.tool_workspace_workbook import (
    describe,
    fill,
    prefill,
    validate_inputs,
    validate_signature,
)


ASSET = {'name': '仪器001', 'model': '001.20', 'brand': '测试品牌', 'scope': 'D', 'location': 'D-178 机房'}
SAMPLES = TEMPLATES / 'folder_imports'


def png_data_url(size=(240, 60), image_format='PNG', background='white', ink=True):
    image = Image.new('RGBA' if isinstance(background, tuple) else 'RGB', size, background)
    if ink:
        ImageDraw.Draw(image).line([(1, 1), (size[0] - 1, size[1] - 1)], fill='black', width=2)
    out = io.BytesIO()
    image.save(out, format=image_format)
    return 'data:image/png;base64,' + base64.b64encode(out.getvalue()).decode('ascii')


def cells(sheet):
    return {cell['address']: cell for row in sheet['rows'] for cell in row['cells']}


def answers(description, result='是'):
    return {f'{sheet["name"]}!{cell["address"]}': result for sheet in description['sheets']
            for cell in cells(sheet).values() if cell.get('check')}


def sheet_model(description):
    return next(sheet for sheet in description['sheets'] if sheet['name'] == description['schema']['sheet'])


def load(content):
    return load_workbook(io.BytesIO(content), keep_links=False)


class ToolWorkspaceWorkbookTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.electrical = (SAMPLES / '电气自检记录单' / '电能质量分析仪自检记录.xlsx').read_bytes()
        cls.hvac = (SAMPLES / '暖通自检记录单' / '温湿度仪自检记录.xlsx').read_bytes()
        cls.signature = png_data_url()

    def export(self, content, values=None, checks=None, **kwargs):
        description = describe(content)
        return fill(content, {} if values is None else values, '审核人甲', '2026-11-13T15:30', self.signature,
                    answers(description) if checks is None else checks, ASSET, **kwargs)

    def test_all_original_samples_describe_and_fill(self):
        samples = sorted(SAMPLES.glob('*/*.xlsx'))
        self.assertEqual(len(samples), 15)
        for sample in samples:
            with self.subTest(sample=sample.name):
                content = sample.read_bytes()
                description = describe(content)
                self.assertEqual(description['schema'], parse_template(content))
                self.assertEqual(description['template_sha256'], hashlib.sha256(content).hexdigest())
                json.dumps(description, ensure_ascii=False, allow_nan=False)
                cover, form = description['sheets']
                self.assertEqual(cover['name'], '封面')
                self.assertTrue(all(not cell['editable'] for cell in cells(cover).values()))
                self.assertEqual(len(answers(description)), len(description['schema']['checks']))
                self.assertLessEqual(len(form['columns']), 24)
                self.assertTrue(form['images'][0]['src'].startswith('data:image/'))
                result = self.export(content, expected_sha256=description['template_sha256'])
                source, target = load(content), load(result)
                try:
                    self.assertEqual(source.sheetnames, target.sheetnames)
                    for before, after in zip(source, target, strict=True):
                        self.assertEqual(set(before.merged_cells.ranges), set(after.merged_cells.ranges))
                        self.assertEqual(before.print_area, after.print_area)
                        self.assertEqual(before.print_options, after.print_options)
                        self.assertEqual(before.page_setup, after.page_setup)
                        self.assertEqual(before.page_margins, after.page_margins)
                        self.assertEqual(before.sheet_format, after.sheet_format)
                        for attribute in ('column_dimensions', 'row_dimensions'):
                            self.assertEqual({key: (dict(value), value._style) for key, value in getattr(before, attribute).items() if dict(value)},
                                             {key: (dict(value), value._style) for key, value in getattr(after, attribute).items() if dict(value)})
                        review_cell = f'E{description["schema"]["review_row"]}'
                        allowed = {cell['address'] for cell in cells(form).values() if cell['editable']}
                        allowed.update((review_cell, f'G{description["schema"]["review_row"]}',
                                        f'E{description["schema"]["result_row"]}'))
                        for coordinate, cell in before._cells.items():
                            saved = after.cell(*coordinate)
                            if before.title != form['name'] or cell.coordinate not in allowed:
                                self.assertEqual(cell.value, saved.value, cell.coordinate)
                            if before.title != form['name'] or cell.coordinate != review_cell:
                                expected_style = copy(cell._style)
                                if (before.title == form['name'] and description['schema'].get('layout') == 'hvac'
                                        and cell.coordinate in {'C3', 'C4', 'G4'}):
                                    expected_font = copy(cell.font)
                                    expected_font.color = '000000'
                                    self.assertEqual(saved.font, expected_font)
                                    expected_style.fontId = saved._style.fontId
                                self.assertEqual(expected_style, saved._style, cell.coordinate)
                    before, after = source[form['name']], target[form['name']]
                    self.assertEqual(before['B2']._style, after['B2']._style)
                    self.assertEqual(len(after._images), len(before._images) + 1)
                    original = next(item for item in after._images if item.anchor._from.row == 1)
                    self.assertEqual(original._data(), before._images[0]._data())
                    review = description['schema']['review_row']
                    self.assertEqual(after[f'E{review}'].value, '审核人甲')
                    self.assertEqual(after[f'G{review}'].value, '2026-11-13 15:30')
                    image = next(item for item in after._images if item.anchor._from.row == review - 1)
                    self.assertEqual((image.anchor._from.col, image.anchor._from.row), (4, review - 1))
                    self.assertGreater(image.anchor._from.rowOff, 0)
                    self.assertLessEqual(EMU_to_pixels(image.anchor.ext.cx), form['columns'][4])
                    self.assertLessEqual(EMU_to_pixels(image.anchor.ext.cy + image.anchor._from.rowOff),
                                         form['rows'][review - 1]['height'])
                    self.assertEqual(image._data(), base64.b64decode(self.signature.split(',', 1)[1]))
                finally:
                    source.close()
                    target.close()

    def test_downloaded_originals_match_all_fifteen_bundled_snapshots(self):
        originals = Path('D:/下载')
        folders = ('电气自检记录单', '暖通自检记录单')
        if not all((originals / folder).is_dir() for folder in folders):
            self.skipTest('Local original XLSX folders are not available on this host')
        for folder, count in zip(folders, (11, 4), strict=True):
            files = sorted((originals / folder).glob('*.xlsx'))
            self.assertEqual(len(files), count)
            for sample in files:
                with self.subTest(sample=sample.name):
                    self.assertEqual(hashlib.sha256(sample.read_bytes()).hexdigest(),
                                     hashlib.sha256((SAMPLES / folder / sample.name).read_bytes()).hexdigest())

    def test_description_matches_original_merges_and_known_edit_cells(self):
        for sample in sorted(SAMPLES.glob('*/*.xlsx')):
            with self.subTest(sample=sample.name):
                content = sample.read_bytes()
                description = describe(content)
                schema = description['schema']
                expected = {'C5', 'G6', 'C8', 'D11', 'G11', 'C3', 'C4', 'G4', 'C10'} if schema.get('layout') == 'hvac' else {'C3', 'G4', 'C6', 'D8', 'G8'}
                expected.update(f'I{check["row"]}' for check in schema['checks'])
                expected.update(address for entry in schema.get('calibration', ()) for address in entry['fields'])
                book = load(content)
                try:
                    for model in description['sheets']:
                        sheet = book[model['name']]
                        grid = cells(model)
                        self.assertEqual({cell['address'] for cell in grid.values() if cell['editable']},
                                         expected if sheet.title == schema['sheet'] else set())
                        for region in sheet.merged_cells.ranges:
                            anchor = sheet.cell(region.min_row, region.min_col).coordinate
                            self.assertEqual((grid[anchor]['rowspan'], grid[anchor]['colspan']),
                                             (region.size['rows'], region.size['columns']))
                            self.assertEqual(grid[anchor]['value'], sheet[anchor].value)
                finally:
                    book.close()

    def test_prefill_uses_real_slots_and_text_only_export(self):
        description = describe(self.hvac)
        sheet = sheet_model(description)

        def key(address):
            return f'{sheet["name"]}!{address}'

        expected = {'C3': '仪器001', 'C4': '001.20', 'G4': '测试品牌', 'C5': 'D楼', 'D11': 'D-178 机房'}
        for address in ('C3', 'C4', 'G4'):
            self.assertEqual(cells(sheet)[address]['style']['color'], '#000000')
        self.assertEqual(prefill(self.hvac, ASSET), {key(address): value for address, value in expected.items()})
        electric = sheet_model(describe(self.electrical))['name']
        self.assertEqual(prefill(self.electrical, ASSET), {f'{electric}!C3': 'D楼', f'{electric}!D8': 'D-178 机房'})
        self.assertEqual((cells(sheet)['C3']['rowspan'], cells(sheet)['C3']['colspan']), (1, 7))
        self.assertNotIn('D3', cells(sheet))
        check_map = answers(description)
        first_check = next(iter(check_map))
        check_map[first_check] = '否'
        calibration = next(iter(description['schema']['calibration'][0]['fields']))
        content = self.export(self.hvac, {key('C4'): '0007', key(calibration): '0.25', key('C8'): '#N/A'}, check_map)
        book = load(content)
        try:
            form = book[sheet['name']]
            for address, value in (expected | {'C4': '0007', calibration: '0.25', 'C8': '#N/A'}).items():
                self.assertEqual((form[address].value, form[address].data_type), (value, 's'))
            self.assertEqual(form[first_check.rsplit('!', 1)[1]].value, '是[ ]  否[√]')
            self.assertIn('未完成[√]', form[f'E{description["schema"]["result_row"]}'].value)
        finally:
            book.close()

    def test_rejects_noneditable_formula_and_incomplete_checks(self):
        description = describe(self.hvac)
        name = description['schema']['sheet']
        check_map = answers(description)
        review = description['schema']['review_row']
        for key in ('C3', f'{name}!$C$3', f'{name}!c3', f'{name}!I14', f'{name}!D3', f'{name}!XFD1',
                    f'{name}!E{review}', f'{name}!G{review}', '封面!C3', f'{name}!E14', f'{name}!C03'):
            with self.subTest(key=key), self.assertRaises(RiskError):
                validate_inputs(self.hvac, {key: 'x'}, {})
        for value in ('=1+1', ' \n=1+1', '\x01', '\ud800', 45, 'x' * 1501):
            with self.subTest(value=repr(value)[:30]), self.assertRaises(RiskError):
                validate_inputs(self.hvac, {f'{name}!C3': value}, {})
        first = next(iter(check_map))
        for value in ('√', '否 ', True, [], None):
            with self.subTest(value=value), self.assertRaises(RiskError):
                validate_inputs(self.hvac, {}, {first: value})
        validate_inputs(self.hvac, {}, {first: '是'})
        del check_map[first]
        with self.assertRaisesRegex(RiskError, '所有'):
            self.export(self.hvac, checks=check_map)

    def test_cleared_check_can_be_saved_as_draft_but_cannot_be_submitted(self):
        description = describe(self.hvac)
        check_map = answers(description)
        first = next(iter(check_map))
        check_map[first] = ''
        validate_inputs(self.hvac, {}, check_map, complete=False)
        validate_inputs(self.hvac, {}, {key: '' for key in check_map}, complete=False)
        with self.assertRaisesRegex(RiskError, '所有'):
            validate_inputs(self.hvac, {}, check_map, complete=True)
        with self.assertRaisesRegex(RiskError, '所有'):
            self.export(self.hvac, checks=check_map)
        for invalid in (None, False, 0, [], {}):
            with self.subTest(invalid=invalid), self.assertRaises(RiskError):
                validate_inputs(self.hvac, {}, {first: invalid})
        with self.assertRaisesRegex(RiskError, '无效或不可编辑'):
            validate_inputs(self.hvac, {}, {f'{description["schema"]["sheet"]}!B2': ''})

    def test_png_validation_and_snapshot_mismatch(self):
        self.assertIsNone(validate_signature(self.signature))
        for signature in ('', 'data:image/jpeg;base64,AAAA', 'data:image/png;base64,%%%%',
                          png_data_url(image_format='JPEG'), png_data_url((2049, 20)), png_data_url((1500, 1500)),
                          png_data_url(ink=False), png_data_url(background=(0, 0, 0, 0), ink=False),
                          self.signature[:-20], 'data:image/png;base64,' + 'A' * 3_000_000):
            with self.subTest(signature=signature[:35]), self.assertRaises(RiskError):
                validate_signature(signature)
        digest = describe(self.hvac)['template_sha256']
        with self.assertRaises(RiskError) as error:
            self.export(self.electrical, expected_sha256=digest)
        self.assertEqual(error.exception.status, 409)

    def test_signature_fits_combined_merged_width_and_height(self):
        book = load(self.electrical)
        try:
            schema = parse_template(self.electrical)
            form = book[schema['sheet']]
            row = schema['review_row']
            # Use a narrow anchor column and a wider neighbor to detect single-column sizing.
            form.column_dimensions['E'].width = 1
            form.column_dimensions['F'].width = 26
            form[f'F{row}'] = None
            form.merge_cells(f'E{row}:F{row + 1}')
            form.row_dimensions[row].height = 26
            form.row_dimensions[row + 1].height = 44
            out = io.BytesIO()
            book.save(out)
        finally:
            book.close()
        content = out.getvalue()
        model = sheet_model(describe(content))
        self.assertEqual(model['review'], {'reviewer_address': f'E{row}', 'reviewed_at_address': f'G{row}',
                                           'signature_address': f'E{row}'})
        self.assertEqual((cells(model)[f'E{row}']['rowspan'], cells(model)[f'E{row}']['colspan']), (2, 2))
        result = load(self.export(content))
        try:
            form = result[schema['sheet']]
            image = next(item for item in form._images if item.anchor._from.row == row - 1)
            width = EMU_to_pixels(image.anchor.ext.cx)
            self.assertGreater(width, model['columns'][4])
            self.assertLessEqual(EMU_to_pixels(image.anchor._from.colOff) + width, sum(model['columns'][4:6]))
            height = EMU_to_pixels(image.anchor.ext.cy + image.anchor._from.rowOff)
            self.assertLessEqual(height, sum(points_to_pixels(form.row_dimensions[index].height) for index in (row, row + 1)))
            self.assertEqual(form.row_dimensions[row].height, 26)
            self.assertEqual(form.row_dimensions[row + 1].height, 44)
        finally:
            result.close()

    def test_signature_does_not_resize_a_short_merged_reviewer_span(self):
        book = load(self.electrical)
        try:
            schema = parse_template(self.electrical)
            form = book[schema['sheet']]
            row = schema['review_row']
            form.column_dimensions['E'].width = 1
            form.column_dimensions['F'].width = 26
            form[f'F{row}'] = None
            form.merge_cells(f'E{row}:F{row}')
            form.row_dimensions[row].height = 18
            out = io.BytesIO()
            book.save(out)
        finally:
            book.close()
        content = out.getvalue()
        model = sheet_model(describe(content))
        result = load(self.export(content))
        try:
            form = result[schema['sheet']]
            image = next(item for item in form._images if item.anchor._from.row == row - 1)
            self.assertEqual(form.row_dimensions[row].height, 18)
            self.assertGreater(EMU_to_pixels(image.anchor.ext.cx), model['columns'][4])
            self.assertLessEqual(EMU_to_pixels(image.anchor.ext.cx + image.anchor._from.colOff), sum(model['columns'][4:6]))
            self.assertLessEqual(EMU_to_pixels(image.anchor.ext.cy + image.anchor._from.rowOff), 24)
        finally:
            result.close()

    def test_name_too_long_for_native_reviewer_cell_is_rejected(self):
        description = describe(self.electrical)
        with self.assertRaisesRegex(RiskError, '原表审核区域'):
            fill(self.electrical, {}, '审核人员' * 10, '2026-11-13T15:30', self.signature,
                 answers(description), ASSET)

    def test_hidden_or_zero_height_reviewer_cell_is_rejected(self):
        for hidden in (False, True):
            book = load(self.electrical)
            try:
                schema = parse_template(self.electrical)
                form = book[schema['sheet']]
                dimension = form.row_dimensions[schema['review_row']]
                sheet_path = f'xl/worksheets/sheet{book.index(form) + 1}.xml'
                if hidden:
                    dimension.hidden = True
                else:
                    dimension.height = 0
                out = io.BytesIO()
                book.save(out)
            finally:
                book.close()
            if not hidden:
                # openpyxl omits ht=0 on save. Model an actual zero-height XLSX
                # row explicitly instead of accidentally testing default height.
                rewritten = io.BytesIO()
                with ZipFile(io.BytesIO(out.getvalue())) as source, ZipFile(rewritten, 'w') as target:
                    for part in source.infolist():
                        data = source.read(part.filename)
                        if part.filename == sheet_path:
                            root = ET.fromstring(data)
                            ns = '{http://schemas.openxmlformats.org/spreadsheetml/2006/main}'
                            root.find(f'{ns}sheetData/{ns}row[@r="{schema["review_row"]}"]').set('ht', '0')
                            data = ET.tostring(root)
                        target.writestr(part, data)
                out = rewritten
            with self.subTest(hidden=hidden), self.assertRaisesRegex(RiskError, '被隐藏'):
                self.export(out.getvalue())


if __name__ == '__main__':
    unittest.main()
