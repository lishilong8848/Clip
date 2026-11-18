import io
import json
import unittest
from openpyxl import load_workbook
from openpyxl.cell.cell import MergedCell
from lan_bitable_template_portal.daily_inspection import inspection_report_workbook
from lan_bitable_template_portal.daily_inspection_original import original_templates, original_views, native_style
from lan_bitable_template_portal.daily_inspection_template_art import read_artwork


class TemplateFormatTests(unittest.TestCase):
    def test_export_preserves_every_original_sheet_style_and_dimensions(self):
        for scope in ('D','E'):
            templates=[t for t in original_templates() if t['scope']==scope]
            fixed = {(t['name'], s['name']): set(s.get('slash_cells', ())) for t in templates for s in t['sheets']}
            self.assertEqual(sum(map(len, fixed.values())), {'D':308, 'E':280}[scope])
            report={'scope':scope,'date':'2026-11-05','rounds':[],'original_templates':templates}
            views=original_views(templates,[],include_empty_rounds=True)
            content=inspection_report_workbook(report)
            drawings=read_artwork(io.BytesIO(content))
            book=load_workbook(io.BytesIO(content))
            try:
                self.assertEqual(len(book.worksheets),len(views))
                for ws,source in zip(book.worksheets,views):
                    slashes = fixed[source['template'], source['sheet']]
                    if source.get('artwork'):
                        self.assertEqual(drawings[ws.title]['xml'],source['artwork']['xml'])
                        self.assertEqual(drawings[ws.title]['assets'],source['artwork']['assets'])
                    self.assertEqual(set(map(str,ws.merged_cells.ranges)),set(source['merges']))
                    native=[native_style(json.dumps(s,sort_keys=True)) for s in source['excel_styles']]
                    for ri,row in enumerate(source['rows']):
                        for ci,_ in enumerate(row):
                            cell=ws.cell(ri+1,ci+1)
                            if isinstance(cell,MergedCell):continue
                            expected=native[source['style_ids'][ri][ci]]
                            for key,value in expected.items():
                                if cell.coordinate in slashes and key in ('font', 'alignment'):
                                    continue
                                self.assertEqual(getattr(cell,key),value,f'{scope} {ws.title} {cell.coordinate} {key}')
                    for address in slashes:
                        cell = ws[address]
                        self.assertEqual((cell.value, cell.data_type, cell.alignment.horizontal), ('/', 's', 'center'))
                    for index,values in source['excel_settings']['row_dimensions'].items():
                        for key,value in values.items():self.assertEqual(getattr(ws.row_dimensions[int(index)],key),value)
                    for index,values in source['excel_settings']['column_dimensions'].items():
                        for key,value in values.items():self.assertEqual(getattr(ws.column_dimensions[index],key),value)
                    self.assertTrue(ws.page_margins)
                    self.assertEqual(len([s for s in book.sheetnames if '暖通冷机系统' in s]),4)
            finally:book.close()

    def test_slash_cells_win_over_frozen_readings_and_manual_overrides(self):
        templates = [t for t in original_templates() if t['scope'] == 'D' and t['profession'] == '电气']
        target = {'template': templates[0]['name'], 'sheet': 'SVG巡检表'}
        rounds = [{'shift':'白', 'round':1, 'original_cells': [
            {**target, 'cell':'H7', 'value':'旧测点值', 'conflict':True},
            {**target, 'cell':'G7', 'value':'相邻测点'}],
            'extra_cells': [{**target, 'cell':'I7', 'value':'旧手填值'}]}]
        view = next(s for s in original_views(templates, rounds) if s['sheet'] == target['sheet'])
        self.assertEqual(view['rows'][6][6:9], ['相邻测点', '/', '/'])

    def test_values_are_same_as_page_overlay_and_cannot_become_formulas(self):
        templates=[t for t in original_templates() if t['scope']=='D' and t['profession']=='暖通']
        t=templates[0];sheet=next(s for s in t['sheets'] if s['name'].strip()=='暖通冷机系统巡检表')
        rounds=[{'shift':shift,'round':number,'original_cells':[
            {'template':t['name'],'sheet':sheet['name'],'cell':'B4','value':str(i+10)}],
            'extra_cells':[{'template':t['name'],'sheet':sheet['name'],'cell':'C4','value':'=1+2'}]}
            for i,(shift,number) in enumerate((('白',1),('白',2),('夜',1),('夜',2)))]
        report={'scope':'D','date':'2026-11-05','rounds':rounds,'original_templates':templates}
        views=original_views(templates,rounds,include_empty_rounds=True)
        book=load_workbook(io.BytesIO(inspection_report_workbook(report)))
        try:
            for ws,source in zip(book.worksheets,views):
                if '暖通冷机系统' not in ws.title:continue
                self.assertEqual(ws['B4'].value,source['rows'][3][1])
                self.assertEqual(ws['C4'].value,'=1+2')
                self.assertEqual(ws['C4'].data_type,'s')
                for cell in (ws['B4'], ws['C4']):
                    self.assertEqual(cell.font.sz,9)
                    self.assertEqual(cell.alignment.horizontal,'center')
                    self.assertEqual(cell.alignment.vertical,'center')
                    self.assertTrue(cell.alignment.shrink_to_fit)
                    self.assertFalse(cell.alignment.wrap_text)
        finally:book.close()


if __name__=='__main__':unittest.main()
