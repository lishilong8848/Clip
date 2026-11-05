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
            report={'scope':scope,'date':'2026-11-05','rounds':[],'original_templates':templates}
            views=original_views(templates,[],include_empty_rounds=True)
            content=inspection_report_workbook(report)
            drawings=read_artwork(io.BytesIO(content))
            book=load_workbook(io.BytesIO(content))
            try:
                self.assertEqual(len(book.worksheets),len(views))
                for ws,source in zip(book.worksheets,views):
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
                                self.assertEqual(getattr(cell,key),value,f'{scope} {ws.title} {cell.coordinate} {key}')
                    for index,values in source['excel_settings']['row_dimensions'].items():
                        for key,value in values.items():self.assertEqual(getattr(ws.row_dimensions[int(index)],key),value)
                    for index,values in source['excel_settings']['column_dimensions'].items():
                        for key,value in values.items():self.assertEqual(getattr(ws.column_dimensions[index],key),value)
                    self.assertTrue(ws.page_margins)
                    self.assertEqual(len([s for s in book.sheetnames if '暖通冷机系统' in s]),4)
            finally:book.close()

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
                expected=native_style(json.dumps(source['excel_styles'][source['style_ids'][3][1]],sort_keys=True))
                self.assertEqual(ws['B4'].font,expected['font'])
        finally:book.close()


if __name__=='__main__':unittest.main()
