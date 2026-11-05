import copy
import concurrent.futures
import io
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient
from openpyxl import load_workbook

from lan_bitable_template_portal.daily_inspection import (
    DailyInspection, RiskError, inspection_report, inspection_report_workbook, install_daily_inspection,
)
from lan_bitable_template_portal.module_access import module_denial
from lan_bitable_template_portal.daily_inspection_original import (catalog_locations, original_cells,
                                                                    original_templates, original_views)
from openpyxl.utils.cell import coordinate_from_string, column_index_from_string
from lan_bitable_template_portal.portal_service import LI_SHILONG_OPEN_ID
from lan_bitable_template_portal.state_store import LanPortalStateStore
import test_daily_inspection as fixtures

DAY, NIGHT, InspectionCloud = fixtures.DAY, fixtures.NIGHT, fixtures.InspectionCloud


class InspectionReportTests(unittest.TestCase):
    def setUp(self):
        self.folder = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.store = LanPortalStateStore(self.folder / 'state.sqlite3')
        self.cloud = InspectionCloud()
        self.service = DailyInspection(self.store, self.cloud)
        self.service.download_archive = lambda attachment: self.cloud.files[attachment['file_token']]

    def test_original_form_uses_exact_identity_and_four_distinct_rounds(self):
        templates = [{'name':'原表.xlsx','scope':'D','profession':'电气','sheets':[
            {'name':'原表','rows':[['设备',None],['时间',None],['时间',None],['时间',None],['时间',None]],'merges':[]}]}]
        points = [['楼栋','设备','专业','测点ID','测点名称','模板','页签','白班1单元格','白班2单元格','夜班1单元格','夜班2单元格'],
                  ['D楼','设备甲','电气','p1','电压','原表.xlsx','原表','B2','B3','B4','B5']]
        reviews = [['楼栋','设备','专业','来源','参数','设备标识','现场读数','参数状态'],
                   ['D楼','设备甲','电气','p1','电压','D:电气:甲','235','']]
        sheets = [{'name':'测点读数','rows':points},{'name':'现场核对','rows':reviews}]
        entries = original_cells(sheets, templates, '白', 1)
        self.assertEqual((entries[0]['cell'], entries[0]['value'], entries[0]['device']), ('B2','235','D:电气:甲'))
        second = original_cells(sheets, templates, '夜', 2)
        self.assertEqual(second[0]['cell'],'B5')
        points[1][7]=''
        self.assertEqual(original_cells(sheets, templates, '白', 1), [])
        points[1][7]='B2'
        views = original_views(templates, [{'shift':'白','round':1,'original_cells':entries},
                                           {'shift':'夜','round':2,'original_cells':second}])
        self.assertEqual((views[0]['rows'][1][1], views[0]['rows'][4][1]),('235','235'))
        reviews.append(reviews[1][:])
        self.assertEqual(original_cells(sheets, templates, '白', 1), [])
        reviews.pop()
        points.append(points[1][:])
        self.assertEqual(original_cells(sheets, templates, '白', 1), [])

    def test_original_catalog_cells_require_exact_location_and_use_saved_reading(self):
        templates=[{'name':'原表.xlsx','scope':'D','profession':'电气','sheets':[
            {'name':'M1','rows':[['设备',None],['时间',None]],'merges':[]}]}]
        mapping={'scope':'D','profession':'电气','device':'设备甲','location':'M1房间','field':'电压',
                 'template':'原表.xlsx','sheet':'M1','cells':{'白1':'B2'}}
        key=('D楼','设备甲','电气','M1房间','M1','电压')
        reviews=[['楼栋','设备','专业','位置','来源','参数','设备标识','现场读数','参数状态'],
                 ['D楼','设备甲','电气','M1房间','M1','电压','D:甲','236','']]
        with patch('lan_bitable_template_portal.daily_inspection_original.catalog_locations',return_value={key:mapping}):
            cells=original_cells([{'name':'现场核对','rows':reviews}],templates,'白',1)
            self.assertEqual((cells[0]['cell'],cells[0]['value'],cells[0]['kind']),('B2','236','section'))
            reviews[1][3]='其他房间'
            self.assertEqual(original_cells([{'name':'现场核对','rows':reviews}],templates,'白',1),[])

    def test_original_cell_with_point_and_device_source_is_linked_and_flags_conflicts(self):
        templates=[{'name':'原表.xlsx','scope':'D','profession':'电气','sheets':[
            {'name':'M1','rows':[['设备',None],['时间',None]],'merges':[]}]}]
        mapping={'scope':'D','profession':'电气','device':'设备甲','location':'M1','field':'输出电压',
                 'template':'原表.xlsx','sheet':'M1','cells':{'白1':'B2'}}
        key=('D楼','设备甲','电气','M1','M1','输出电压')
        points=[['楼栋','设备','专业','测点ID','测点名称','模板','页签','白班1单元格'],
                ['D楼','设备甲','电气','p1','直流电压','原表.xlsx','M1','B2']]
        reviews=[['楼栋','设备','专业','位置','来源','参数','设备标识','现场读数','参数状态'],
                 ['D楼','设备甲','电气','M1','M1','输出电压','D:甲','235',''],
                 ['D楼','设备甲','电气','M1','p1','直流电压','D:甲','235.0','']]
        sheets=[{'name':'测点读数','rows':points},{'name':'现场核对','rows':reviews}]
        with patch('lan_bitable_template_portal.daily_inspection_original.catalog_locations',return_value={key:mapping}):
            cell=original_cells(sheets,templates,'白',1)[0]
            self.assertEqual((cell['kind'],cell['conflict'],len(cell['members'])),('paired',False,2))
            reviews[2][7]='240'
            cell=original_cells(sheets,templates,'白',1)[0]
            self.assertTrue(cell['conflict'])
            with self.assertRaises(ValueError):
                original_views(templates,[{'shift':'白','round':1,'original_cells':[cell]}])

    def test_diesel_level_uses_converted_device_field_without_overwriting_raw_volume(self):
        templates=[{'name':'原表.xlsx','scope':'D','profession':'电气','sheets':[
            {'name':'柴发巡检表','rows':[['设备',None],['时间',None]],'merges':[]}]}]
        mapping={'scope':'D','profession':'电气','device':'1#柴发','location':'柴发巡检表',
                 'field':'柴油液位(cm)','template':'原表.xlsx','sheet':'柴发巡检表','cells':{'白1':'B2'}}
        key=('D楼','1#柴发','电气','柴发巡检表','柴发巡检表','柴油液位(cm)')
        points=[['楼栋','设备','专业','测点ID','测点名称','模板','页签','白班1单元格'],
                ['D楼','燃油自控系统','电气','oil1','1#油箱体积','原表.xlsx','柴发巡检表','B2']]
        reviews=[['楼栋','设备','专业','位置','来源','参数','设备标识','现场读数','参数状态'],
                 ['D楼','1#柴发','电气','柴发巡检表','柴发巡检表','柴油液位(cm)','D:电气:1#柴发','55.43',''],
                 ['D楼','燃油自控系统','电气','', 'oil1','1#油箱体积','D:电气:178-燃油自控系统','554.29','']]
        with patch('lan_bitable_template_portal.daily_inspection_original.catalog_locations',return_value={key:mapping}):
            cells=original_cells([{'name':'测点读数','rows':points},{'name':'现场核对','rows':reviews}],templates,'白',1)
            self.assertEqual((len(cells),cells[0]['kind'],cells[0]['value']),(1,'section','55.43'))
            self.assertNotIn('members',cells[0])

    def test_catalog_mapping_points_only_to_empty_unique_original_cells(self):
        templates = {(item['scope'],item['profession'],item['name'],sheet['name']):sheet
                     for item in original_templates() for sheet in item['sheets']}
        locations = catalog_locations()
        self.assertGreater(len(locations),1900)
        occupied = set()
        for mapping in locations.values():
            self.assertIsNotNone(mapping)
            key=(mapping['scope'],mapping['profession'],mapping['template'],mapping['sheet'])
            sheet=templates[key]
            per_round = sheet['name'].strip()=='暖通冷机系统巡检表'
            for slot,address in mapping['cells'].items():
                col,row=coordinate_from_string(address)
                self.assertIn(sheet['rows'][row-1][column_index_from_string(col)-1],(None,''))
                destination=(key,address,slot if per_round else '')
                self.assertNotIn(destination,occupied)
                occupied.add(destination)

    def test_manual_form_cells_do_not_overlap_automatic_catalog_coordinates(self):
        from lan_bitable_template_portal.daily_inspection import (supplemental_form_devices, confirmed_chiller_cells,
            confirmed_svg_cells, confirmed_hvdc_cells, confirmed_humidity_cells, confirmed_return_air_cells,
            confirmed_electrical_environment_cells, confirmed_dc_screen_cells)
        locations=catalog_locations()
        occupied={(item['scope'],item['template'],item['sheet'].strip(),slot,address)
                  for item in locations.values() if item for slot,address in item['cells'].items()}
        for loader in (confirmed_chiller_cells,confirmed_svg_cells,confirmed_hvdc_cells,
                       confirmed_humidity_cells,confirmed_return_air_cells,confirmed_electrical_environment_cells,
                       confirmed_dc_screen_cells):
            occupied.update((item['scope'],item['template'],item['sheet'].strip(),slot,address)
                            for item in loader().values() for slot,address in item['cells'].items())
        count=0
        for scope in ('D','E'):
            for device in supplemental_form_devices(scope):
                self.assertTrue(device['form_only'])
                for point in device['points']:
                    count+=1
                    for slot,address in point['round_cells'].items():
                        self.assertNotIn((scope,point['template'],point['sheet'].strip(),slot,address),occupied)
        self.assertGreater(count,300)

    def test_chiller_form_stays_separate_for_each_round_and_exports_merges(self):
        template = {'name':'暖通.xlsx','scope':'E','profession':'暖通','sheets':[
            {'name':'暖通冷机系统巡检表 ', 'rows':[['冷机',None],[None,None]],'merges':['A1:B1']},
            {'name':'恒湿机巡检表', 'rows':[['恒湿机',None],[None,None]],'merges':['A1:B1']}]}
        rounds = [{'round_id':str(i),'shift':shift,'round':number,'status':'已归档','sheets':[],'local_draft':False,
                   'original_cells':[]} for i,(shift,number) in enumerate((('白',1),('白',2),('夜',1),('夜',2)))]
        views = original_views([template], rounds)
        self.assertEqual(len(views),5)
        self.assertEqual(sum('冷机系统' in item['name'] for item in views),4)
        book = load_workbook(io.BytesIO(inspection_report_workbook({'scope':'E','date':'2026-11-04',
                         'rounds':rounds,'original_templates':[template]})))
        try:
            names=[name for name in book.sheetnames if '冷机系统' in name]
            self.assertEqual(len(names),4)
            self.assertIn('A1:B1',str(book[names[0]].merged_cells))
        finally:
            book.close()

    def test_daily_export_has_four_chiller_sheets_even_when_rounds_missing(self):
        template = {'name':'暖通.xlsx','scope':'E','profession':'暖通','sheets':[
            {'name':'暖通冷机系统巡检表', 'rows':[['冷机',None],[None,None]], 'merges':[]},
            {'name':'恒湿机巡检表', 'rows':[['恒湿机',None],[None,None]], 'merges':[]}]}
        rounds=[{'shift':'白','round':1,'original_cells':[
            {'template':'暖通.xlsx','sheet':'暖通冷机系统巡检表','cell':'B2','value':'12'}]}]
        self.assertEqual(len(original_views([template],rounds)),2)
        book=load_workbook(io.BytesIO(inspection_report_workbook({'scope':'E','date':'2026-11-04',
                            'rounds':rounds,'original_templates':[template]})))
        try:
            self.assertEqual(len(book.sheetnames),5)
            for slot in ('白1','白2','夜1','夜2'):
                self.assertEqual(book[f'原-暖-{slot}-暖通冷机系统巡检表']['B2'].value,
                                 '12' if slot=='白1' else None)
            self.assertIn('原-暖-恒湿机巡检表',book.sheetnames)
        finally:
            book.close()

    def test_daily_export_keeps_four_round_values_in_separate_chiller_sheets(self):
        template={'name':'暖通.xlsx','scope':'E','profession':'暖通','sheets':[
            {'name':'暖通冷机系统巡检表','rows':[['冷机',None],[None,None]],'merges':[]},
            {'name':'恒湿机巡检表','rows':[['设备',None],[None,None],[None,None],[None,None],[None,None]],'merges':[]}]}
        rounds=[]
        for index,(shift,number) in enumerate((('白',1),('白',2),('夜',1),('夜',2))):
            value=str(index+1)
            rounds.append({'shift':shift,'round':number,'original_cells':[
                {'template':'暖通.xlsx','sheet':'暖通冷机系统巡检表','cell':'B2','value':value},
                {'template':'暖通.xlsx','sheet':'恒湿机巡检表','cell':f'B{index+2}','value':value}]})
        book=load_workbook(io.BytesIO(inspection_report_workbook({'scope':'E','date':'2026-11-04',
                            'rounds':rounds,'original_templates':[template]})))
        try:
            self.assertEqual(len(book.sheetnames),5)
            for index,slot in enumerate(('白1','白2','夜1','夜2')):
                self.assertEqual(book[f'原-暖-{slot}-暖通冷机系统巡检表']['B2'].value,str(index+1))
                self.assertEqual(book['原-暖-恒湿机巡检表'][f'B{index+2}'].value,str(index+1))
        finally:
            book.close()

    def test_history_edit_updates_frozen_original_cell_with_review(self):
        source = self.cloud.tables[fixtures.SOURCES['D'][0][0]][0]['fields']
        source.update(位置='电气系统西区', 巡检表单='电气系统西区')
        active = self.service.change('a','甲',DAY,'start',{'duty':DAY})['active']
        self.service.change('a','甲',DAY,'finish',fixtures.DailyInspectionTests.submission(active))
        detail = self.service.archive_detail('D','recArchive1')
        device = next(item for item in detail['devices_data'] if item['name']=='144-AH-109')
        row = next(item for item in device['rows'] if item['name']=='电流(A)')
        self.service.edit_archive('D','recArchive1',{'version':detail['version'],
            'changes':{device['id']:{'values':{row['id']:'123'},'result':'正常','note':''}}},'乙')
        book=load_workbook(io.BytesIO(self.cloud.file))
        try:
            self.assertEqual(book['原-电-电气系统西区']['C5'].value,'123')
            self.assertEqual(book['原-电-电气系统西区']['C6'].value,None)
        finally:
            book.close()

    def test_history_edit_keeps_shared_point_and_device_value_in_sync(self):
        rows=self.cloud.tables[fixtures.SOURCES['D'][0][0]]
        fields=rows[0]['fields']
        fields.update(设备编号='245-HVDC-111',位置='M1HVDC巡检表',巡检表单='M1HVDC巡检表',**{'输出电压(V)':234})
        rows[2]['fields'].update(设备编号='D-245-HVDC-112',测点ID='13_0_133_1_653_0',测点名称='直流电压_V',测点值=234)
        active=self.service.change('a','甲',DAY,'start',{'duty':DAY})['active']
        self.service.change('a','甲',DAY,'finish',fixtures.DailyInspectionTests.submission(active))
        detail=self.service.archive_detail('D','recArchive1')
        device=next(item for item in detail['devices_data'] if item['name']=='245-HVDC-111')
        section=next(item for item in device['rows'] if item['name']=='输出电压(V)')
        self.service.edit_archive('D','recArchive1',{'version':detail['version'],
            'changes':{device['id']:{'values':{section['id']:'256'},'result':'正常','note':''}}},'乙')
        book=load_workbook(io.BytesIO(self.cloud.file))
        try:
            self.assertEqual(book['原-电-M1HVDC巡检表']['B6'].value,'256')
            values=list(book['现场核对'].values)
            self.assertEqual({r[7]:r[9] for r in values[1:] if r[7] in ('输出电压(V)','直流电压_V')},
                             {'输出电压(V)':'256','直流电压_V':'256'})
        finally:
            book.close()

    def test_inconsistent_device_and_point_are_visible_but_cannot_be_archived_as_one_value(self):
        rows=self.cloud.tables[fixtures.SOURCES['D'][0][0]]
        rows[0]['fields'].update(设备编号='245-HVDC-111',位置='M1HVDC巡检表',巡检表单='M1HVDC巡检表',**{'输出电压(V)':234})
        rows[2]['fields'].update(设备编号='D-245-HVDC-112',测点ID='13_0_133_1_653_0',测点名称='直流电压_V',测点值=235)
        active=self.service.change('a','甲',DAY,'start',{'duty':DAY})['active']
        report=inspection_report(self.service,'D',DAY['date'],'a','甲',include_active=True,drafts={})
        cell=next(item for item in report['rounds'][0]['original_cells'] if item['cell']=='B6')
        self.assertTrue(cell['conflict'])
        with self.assertRaisesRegex(RiskError,'读数不一致'):
            self.service.change('a','甲',DAY,'finish',{'id':active['id'],'review_version':0,'reviews':{}})
        self.assertEqual(self.cloud.creates,0)

    def test_unmapped_original_cell_autosaves_and_archives_without_extra_device_count(self):
        active=self.service.change('a','甲',DAY,'start',{'duty':DAY})['active']
        form=next(item for item in active['batch']['form_devices'] if item['profession']=='电气' and item['points'])
        point=form['points'][0]; parameter='p:'+point['id']
        saved=self.service.change('a','甲',DAY,'draft',{'id':active['id'],'reviews':{
            form['id']:{'values':{parameter:'手填值'},'result':'','note':'','revision':0}}})['active']
        self.assertEqual(saved['reviews'][form['id']]['values'][parameter],'手填值')
        restarted=DailyInspection(self.store,self.cloud)
        restarted.change('a','甲',DAY,'finish',{'id':saved['id'],'review_version':saved['review_version'],'reviews':{}})
        self.assertEqual(self.cloud.tables[fixtures.ARCHIVE][0]['fields']['设备数'],2)
        book=load_workbook(io.BytesIO(self.cloud.file))
        try:
            header,*rows=list(book['原表映射'].values)
            item=next(dict(zip(header,row)) for row in rows if row[2]==form['id'] and row[4]==point['point_id'])
            self.assertEqual(book[item['工作表']][item['单元格']].value,'手填值')
        finally:
            book.close()

    def test_concurrent_original_form_edits_do_not_silently_overwrite_each_other(self):
        active=self.service.change('a','甲',DAY,'start',{'duty':DAY})['active']
        form=next(item for item in active['batch']['form_devices'] if len(item['points'])>=2)
        first,second=('p:'+item['id'] for item in form['points'][:2])
        a={'values':{first:'甲'},'result':'','note':'','revision':0}
        b={'values':{second:'乙'},'result':'','note':'','revision':0}
        latest=self.service.change('a','甲',DAY,'draft',{'id':active['id'],'reviews':{form['id']:a}})['active']
        with self.assertRaisesRegex(RiskError,'已被其他页面更新'):
            self.service.change('b','乙',DAY,'draft',{'id':active['id'],'reviews':{form['id']:b}})
        merged={'values':{first:'甲',second:'乙'},'result':'','note':'','revision':latest['reviews'][form['id']]['revision']}
        result=self.service.change('b','乙',DAY,'draft',{'id':active['id'],'reviews':{form['id']:merged}})['active']
        self.assertEqual(result['reviews'][form['id']]['values'],{first:'甲',second:'乙'})

    def test_day_and_overnight_rounds_export_same_cells_and_keep_local_drafts_local(self):
        active = self.service.change('a', '甲', DAY, 'start', {'duty': DAY})['active']
        self.service.change('a', '甲', DAY, 'finish', fixtures.DailyInspectionTests.submission(active))
        self.cloud.tables[fixtures.SOURCES['D'][0][0]][1]['fields']['湿度'] = ''
        night = self.service.change('b', '乙', NIGHT, 'start', {'duty': NIGHT})['active']
        first = night['batch']['devices'][0]
        first_row = first['parameters'][0]
        drafts = {first['id']: {'values': {first_row['id']: '0'}, 'result': '', 'note': '', 'revision': 0}}
        before = copy.deepcopy(self.service.load('D'))
        report = inspection_report(self.service, 'D', DAY['date'], 'b', '乙', include_active=True,
                                   drafts=drafts, active_id=night['id'])
        self.assertEqual([(item['shift'], item['status']) for item in report['rounds']], [('白', '已归档'), ('夜', '进行中')])
        self.assertTrue(report['rounds'][1]['local_draft'])
        self.assertFalse(any(sheet['name'].startswith('原-') or sheet['name'] in ('原表映射','原表目录')
                             for item in report['rounds'] for sheet in item['sheets']))
        self.assertEqual(self.service.load('D'), before)
        sheet = next(sheet for sheet in report['rounds'][1]['sheets'] if sheet['name'] == '现场核对')
        header, *rows = sheet['rows']
        value_column = header.index('现场读数')
        self.assertEqual(rows[0][value_column], '0')
        self.assertTrue(any(row[value_column] is None for row in rows[1:]))
        book = load_workbook(io.BytesIO(inspection_report_workbook(report)), read_only=True)
        try:
            self.assertEqual(sum(name=='原-电-电气系统西区' for name in book.sheetnames),1)
            self.assertEqual(sum('冷机系统巡检表' in name and name.startswith('原-暖-') for name in book.sheetnames),4)
            self.assertFalse(any('现场核对' in name or '源表快照' in name for name in book.sheetnames))
        finally:
            book.close()
        only_history = inspection_report(self.service, 'D', DAY['date'], 'c', '丙', drafts={})
        self.assertEqual(len(only_history['rounds']), 1)
        self.assertFalse(inspection_report(self.service, 'D', '2026-11-04', 'b', '乙', include_active=True, drafts={})['rounds'])

    def test_stale_draft_or_failed_archive_is_never_silently_omitted(self):
        active = self.service.change('a', '甲', DAY, 'start', {'duty': DAY})['active']
        device = active['batch']['devices'][0]
        draft = {device['id']: {'values': {device['parameters'][0]['id']: '1'}, 'result': '', 'note': '', 'revision': 3}}
        with self.assertRaisesRegex(RiskError, '设备|更新'):
            inspection_report(self.service, 'D', DAY['date'], 'a', '甲', include_active=True,
                              drafts=draft, active_id=active['id'])
        self.service.change('a', '甲', DAY, 'finish', fixtures.DailyInspectionTests.submission(active))
        def fail(_attachment):
            raise RiskError('模拟附件下载失败', 503)
        self.service.download_archive = fail
        with self.assertRaisesRegex(RiskError, '模拟附件下载失败'):
            inspection_report(self.service, 'D', DAY['date'], 'a', '甲', include_active=True, drafts={})

    def test_unconfirmed_archive_is_not_reported_as_completed(self):
        active = self.service.change('a', '甲', DAY, 'start', {'duty': DAY})['active']
        self.cloud.lose_receipt = True
        with self.assertRaisesRegex(RiskError, '模拟响应丢失'):
            self.service.change('a', '甲', DAY, 'finish', fixtures.DailyInspectionTests.submission(active))
        report = inspection_report(self.service, 'D', DAY['date'], 'a', '甲', include_active=True, drafts={})
        self.assertEqual(len(report['rounds']), 1)
        self.assertEqual(report['rounds'][0]['status'], '待归档核验')
        self.assertEqual(inspection_report(self.service, 'D', DAY['date'], 'b', '乙', drafts={})['rounds'][0]['status'], '待归档核验')
        self.cloud.tables[fixtures.ARCHIVE][0]['fields']['巡检快照'] = [{'file_token': 'different'}]
        with self.assertRaisesRegex(RiskError, '附件尚未核验'):
            inspection_report(self.service, 'D', DAY['date'], 'a', '甲', include_active=True, drafts={})


class InspectionProfessionTests(unittest.TestCase):
    def setUp(self):
        folder = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.store = LanPortalStateStore(folder / 'state.sqlite3')
        self.cloud = InspectionCloud()
        self.cloud.tables[fixtures.SOURCES['D'][0][0]][1]['fields']['专业'] = '暖通'
        self.service = DailyInspection(self.store, self.cloud)

    def start(self, choices):
        return self.service.change('a', '甲', DAY, 'start', {'duty': DAY, 'professions': choices})['active']

    def confirm(self, active, profession, actor):
        return self.service.change(actor, actor, DAY, 'confirm-profession', {'id': active['id'],
            'profession': profession, 'profession_version': active['profession_versions'][profession], 'reviews': {}})

    def test_concurrent_professions_share_one_round_and_archive_once(self):
        active = self.start(['电气', '暖通'])
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(self.confirm, active, profession, actor)
                       for profession, actor in [('电气', '甲'), ('暖通', '乙')]]
            for future in futures:
                future.result()
        ready = self.service.view(self.service.load('D'), 'a', DAY)['active']
        self.assertEqual((self.cloud.creates, self.cloud.uploads), (0, 0))
        self.service.change('a', '甲', DAY, 'finish', {'id': active['id'], 'review_version': ready['review_version'], 'reviews': {}})
        self.assertIsNone(self.service.load('D')['active'])
        self.assertEqual((self.cloud.creates, self.cloud.uploads), (1, 1))
        self.assertEqual(self.cloud.tables[fixtures.ARCHIVE][0]['fields']['设备数'], 2)
        book = load_workbook(io.BytesIO(self.cloud.file), read_only=True)
        try:
            self.assertEqual({(row[0], row[1]) for row in list(book['专业确认'].values)[1:]}, {('电气', '甲'), ('暖通', '乙')})
            columns, *rows = list(book['现场核对'].values)
            self.assertEqual({(row[columns.index('专业')], row[columns.index('填写人')]) for row in rows}, {('电气', '甲'), ('暖通', '乙')})
        finally:
            book.close()

    def test_change_after_confirmation_requires_that_profession_to_confirm_again(self):
        active = self.start(['电气', '暖通'])
        first = self.confirm(active, '电气', '甲')['active']
        electrical = next(device for device in first['batch']['devices'] if device['profession'] == '电气')
        change = {'values': {electrical['parameters'][0]['id']: '12'}, 'result': '', 'note': '', 'revision': 0}
        updated = self.service.change('b', '乙', DAY, 'draft', {'id': active['id'], 'reviews': {electrical['id']: change}})['active']
        self.assertNotIn('电气', updated['profession_confirmations'])
        with self.assertRaisesRegex(RiskError, '已变化'):
            self.confirm(active, '电气', '甲')
        after_other = self.confirm(updated, '暖通', '乙')['active']
        self.assertEqual(self.cloud.creates, 0)
        with self.assertRaisesRegex(RiskError, '尚未确认'):
            self.service.change('a', '甲', DAY, 'finish', {'id': active['id'], 'review_version': after_other['review_version'], 'reviews': {}})
        ready = self.confirm(after_other, '电气', '甲')['active']
        self.assertEqual(self.cloud.creates, 0)
        self.service.change('a', '甲', DAY, 'finish', {'id': active['id'], 'review_version': ready['review_version'], 'reviews': {}})
        self.assertEqual(self.cloud.creates, 1)

    def test_two_people_can_edit_and_confirm_different_professions_concurrently(self):
        active = self.start(['电气', '暖通'])
        def inspect(profession, actor, value):
            device = next(item for item in active['batch']['devices'] if item['profession'] == profession)
            edit = {'values': {device['parameters'][0]['id']: value}, 'result': '', 'note': '', 'revision': 0}
            saved = self.service.change(actor, actor, DAY, 'draft', {'id': active['id'], 'reviews': {device['id']: edit}})['active']
            return self.confirm(saved, profession, actor)
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            for future in [pool.submit(inspect, '电气', '甲', '11'), pool.submit(inspect, '暖通', '乙', '22')]:
                future.result()
        ready = self.service.view(self.service.load('D'), 'a', DAY)['active']
        self.service.change('a', '甲', DAY, 'finish', {'id': active['id'], 'review_version': ready['review_version'], 'reviews': {}})
        self.assertEqual((self.cloud.creates, self.cloud.uploads), (1, 1))
        book = load_workbook(io.BytesIO(self.cloud.file), read_only=True)
        try:
            columns, *rows = list(book['现场核对'].values)
            self.assertIn(('电气', '11', '甲'), {(row[columns.index('专业')], row[columns.index('现场读数')], row[columns.index('填写人')]) for row in rows})
            self.assertIn(('暖通', '22', '乙'), {(row[columns.index('专业')], row[columns.index('现场读数')], row[columns.index('填写人')]) for row in rows})
        finally:
            book.close()

    def test_one_profession_excludes_other_devices_and_preserves_other_order(self):
        all_active = self.start(['电气', '暖通'])
        identities = [device['id'] for device in all_active['batch']['devices']]
        self.service.change('a', '甲', DAY, 'order', {'id': all_active['id'], 'order': list(reversed(identities))})
        self.service.change('a', '甲', DAY, 'cancel', {'id': all_active['id'], 'review_version': 0})
        single = self.start(['电气'])
        self.assertEqual({item['profession'] for item in single['batch']['devices']}, {'电气'})
        self.service.change('a', '甲', DAY, 'order', {'id': single['id'], 'order': [single['batch']['devices'][0]['id']]})
        self.assertEqual(set(self.service.order('a', 'D')), set(identities))
        ready = self.confirm(single, '电气', '甲')['active']
        self.service.change('a', '甲', DAY, 'finish', {'id': single['id'], 'review_version': ready['review_version'], 'reviews': {}})
        self.assertEqual(self.cloud.tables[fixtures.ARCHIVE][0]['fields']['设备数'], 1)
        book = load_workbook(io.BytesIO(self.cloud.file), read_only=True)
        try:
            rows = list(book['现场核对'].values)[1:]
            self.assertEqual({row[3] for row in rows if not str(row[3]).startswith('原表补填 · ')}, {'144-AH-109'})
            self.assertEqual({row[4] for row in rows}, {'电气'})
            self.assertEqual(len(list(book['D楼源表快照'].values)), 4)
        finally:
            book.close()

    def test_all_and_each_profession_keep_independent_per_person_orders(self):
        source = self.cloud.tables[fixtures.SOURCES['D'][0][0]]
        extra = copy.deepcopy(source[0])
        extra['record_id'] = 'recDextra'
        extra['fields']['设备编号'] = '144-AH-111'
        source.append(extra)
        active = self.start(['电气', '暖通'])
        ids = [item['id'] for item in active['batch']['devices']]
        full = list(reversed(ids))
        self.service.change('a', '甲', DAY, 'order', {'id': active['id'], 'view': 'ALL', 'order': full})
        electrical = [item['id'] for item in active['batch']['devices'] if item['profession'] == '电气']
        desired = list(reversed([item for item in full if item in electrical]))
        view = self.service.change('a', '甲', DAY, 'order', {'id': active['id'], 'view': '电气', 'order': desired})['active']
        self.assertEqual([item for item in view['orders']['ALL'] if item in ids], full)
        self.assertEqual([item for item in view['orders']['电气'] if item in electrical], desired)
        coworker = self.service.view(self.service.load('D'), 'b', DAY)['active']
        self.assertEqual(coworker['orders']['ALL'], [])
        self.assertEqual(coworker['orders']['电气'], [])
        imported = self.service.change('b', '乙', DAY, 'order', {'id': active['id'], 'view': '电气', 'order': desired})['active']
        self.assertEqual([item for item in imported['orders']['电气'] if item in electrical], desired)
        self.assertEqual(self.service.order('b', 'D'), [])
        self.assertEqual([item for item in self.service.order('a', 'D') if item in ids], full)
        with self.assertRaisesRegex(RiskError, '排序专业'):
            self.service.change('a', '甲', DAY, 'order', {'id': active['id'], 'view': '不属于本轮', 'order': []})

    def test_last_confirmation_recovers_lost_archive_receipt_without_duplicate(self):
        active = self.start(['电气', '暖通'])
        after_first = self.confirm(active, '电气', '甲')['active']
        ready = self.confirm(after_first, '暖通', '乙')['active']
        self.cloud.lose_receipt = True
        with self.assertRaisesRegex(RiskError, '模拟响应丢失'):
            self.service.change('b', '乙', DAY, 'finish', {'id': active['id'], 'review_version': ready['review_version'], 'reviews': {}})
        self.cloud.lose_receipt = False
        result = DailyInspection(self.store, self.cloud).change('b', '乙', DAY, 'finish', {'id': active['id']})
        self.assertIsNone(result['active'])
        self.assertEqual((self.cloud.creates, self.cloud.uploads), (1, 1))


class InspectionAccessTests(unittest.TestCase):
    def setUp(self):
        self.folder = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.store = LanPortalStateStore(self.folder / 'state.sqlite3')
        self.cloud = InspectionCloud()
        self.service = DailyInspection(self.store, self.cloud)
        self.service.change('a', '甲', DAY, 'start', {'duty': DAY})
        self.session = {'user': {'open_id': 'ou_person', 'name': '测试'}, 'role': 'user'}
        self.modules = set()
        self.scopes = ['D']
        self.home = 'D'
        self.duty = None
        owner = self

        class Auth:
            def account_profile(self, _owner): return {'enabled': True, 'home_scope': owner.home}
            def session_scopes(self, _session): return owner.scopes

        class Controller:
            def _current_session(self, _request): return owner.session
            def _request_base_url(self, _request): return 'http://testserver'
            def _auth_required_response(self): return JSONResponse({'error': '请登录'}, status_code=401)
            async def _read_json_request(self, request, **_kwargs): return await request.json()

        self.runtime = SimpleNamespace(auth_manager=Auth(), state_store=self.store, service=object())
        self.controller = Controller()
        app = FastAPI()
        install_daily_inspection(app, self.controller, self.runtime)

        @app.middleware('http')
        async def permission(request, call_next):
            denied = await module_denial(request, self.controller, self.runtime)
            return denied or await call_next(request)

        self.enterContext(patch('lan_bitable_template_portal.daily_inspection.RiskCloud', return_value=self.cloud))
        self.enterContext(patch('lan_bitable_template_portal.daily_inspection.allowed_modules', side_effect=lambda *_: self.modules))
        self.enterContext(patch('lan_bitable_template_portal.module_access.allowed_modules', side_effect=lambda *_: self.modules))
        self.enterContext(patch('lan_bitable_template_portal.daily_inspection.resolve_duty', side_effect=lambda *_: self.duty))
        # Access tests use a fixed in-shift clock; rollover boundaries have dedicated tests.
        import datetime as dt
        from lan_bitable_template_portal.daily_inspection import CN
        rollover = DailyInspection.rollover_due
        self.enterContext(patch.object(DailyInspection, 'rollover_due',
            lambda service, scope: rollover(service, scope, dt.datetime(2026,11,3,10,tzinfo=CN))))
        self.client = self.enterContext(TestClient(app))

    def post(self, action, data):
        return self.client.post('/api/daily-inspection/' + action, json=data, headers={'origin': 'http://testserver'})

    def test_form_only_read_omits_raw_sheets_and_keeps_building_permissions(self):
        body={'scope':'D','date':DAY['date'],'drafts':{},'form_only':True}
        result=self.post('day-sheet',body)
        self.assertEqual(result.status_code,200)
        data=result.json()['data']
        self.assertTrue(data['original_templates'])
        self.assertTrue(data['rounds'][0]['original_cells'])
        self.assertNotIn('sheets',data['rounds'][0])
        self.assertEqual(self.post('day-sheet',{**body,'scope':'E'}).status_code,403)

    def test_home_building_can_inspect_without_daily_module_or_roster(self):
        page = self.client.get('/api/daily-inspection').json()['data']
        self.assertEqual(page['history_scopes'], ['D'])
        self.assertEqual(page['active']['duty']['scope'], 'D')
        self.assertTrue(page['can_edit'])
        own = self.post('day-sheet', {'scope': 'D', 'date': DAY['date'], 'drafts': {}})
        self.assertEqual(own.status_code, 200)
        self.assertEqual(len(own.json()['data']['rounds']), 1)
        self.assertEqual(self.post('day-sheet', {'scope': 'E', 'date': DAY['date'], 'drafts': {}}).status_code, 403)
        device = page['active']['batch']['devices'][0]
        review = {'values': {device['parameters'][0]['id']: '8'}, 'result': '', 'note': '', 'revision': 0}
        saved = self.post('draft', {'id': page['active']['id'], 'reviews': {device['id']: review}})
        self.assertEqual(saved.status_code, 200)
        self.assertEqual(self.post('history-edit', {'id': 'recX', 'changes': {}}).status_code, 403)
        self.assertEqual(self.client.get('/api/daily-inspection?view_scope=E').status_code, 403)
        self.session['source'] = 'admin_impersonation'
        self.assertEqual(self.post('draft', {'id': page['active']['id'], 'reviews': {}}).status_code, 403)

    def test_history_is_readable_for_local_users_and_drafts_are_account_isolated(self):
        active = self.service.view(self.service.load('D'), 'a', DAY)['active']
        self.service.change('a', '甲', DAY, 'finish', fixtures.DailyInspectionTests.submission(active))
        rid = self.service.archive_history('D', DAY['date'])[0]['id']
        with patch.object(DailyInspection, 'download_archive', side_effect=lambda attachment: self.cloud.files[attachment['file_token']]):
            first = self.client.get('/api/daily-inspection/history-record', params={'scope': 'D', 'id': rid}).json()['data']
            first_recent = self.client.get('/api/daily-inspection').json()['data']['history'][0]['draft_key']
            self.session['user']['open_id'] = 'ou_other'
            second = self.client.get('/api/daily-inspection/history-record', params={'scope': 'D', 'id': rid}).json()['data']
            second_recent = self.client.get('/api/daily-inspection').json()['data']['history'][0]['draft_key']
        self.assertTrue(first['can_edit'])
        self.assertTrue(second['can_edit'])
        self.assertNotEqual(first['draft_key'], second['draft_key'])
        self.assertNotEqual(first_recent, second_recent)

    def test_local_non_roster_member_can_edit_own_history_only(self):
        active = self.service.view(self.service.load('D'), 'a', DAY)['active']
        submission = fixtures.DailyInspectionTests.submission(active)
        original = next(iter(submission['reviews'].values()))
        original.update(result='异常', note='旧备注保留')
        self.service.change('a', '甲', DAY, 'finish', submission)
        rid = self.service.archive_history('D', DAY['date'])[0]['id']
        self.scopes = ['D', 'E']
        with patch.object(DailyInspection, 'download_archive', side_effect=lambda attachment: self.cloud.files[attachment['file_token']]):
            detail = self.client.get('/api/daily-inspection/history-record', params={'scope': 'D', 'id': rid}).json()['data']
            device = detail['devices_data'][0]
            item = device['rows'][0]
            body = {'id': rid, 'version': detail['version'], 'changes': {device['id']: {
                'values': {item['id']: '12'}, 'result': device['result'], 'note': device['note']}}}
            edit = lambda scope: self.client.post('/api/daily-inspection/history-edit', params={'scope': scope},
                json=body, headers={'origin': 'http://testserver'})
            self.assertEqual(edit('E').status_code, 403)
            self.session['source'] = 'admin_impersonation'
            self.assertEqual(edit('D').status_code, 403)
            self.session.pop('source')
            result = edit('D')
            self.assertEqual(result.status_code, 200)
            self.assertTrue(result.json()['data']['can_edit'])
            self.assertEqual(result.json()['data']['devices_data'][0]['rows'][0]['value'], '12')
            self.assertEqual(result.json()['data']['devices_data'][0]['note'], '旧备注保留')
            self.assertEqual(result.json()['data']['devices_data'][0]['result'], '异常')
        self.assertEqual(self.cloud.updates, 1)

    def test_historical_value_edit_does_not_require_an_exception_note(self):
        active = self.service.view(self.service.load('D'), 'a', DAY)['active']
        submission = fixtures.DailyInspectionTests.submission(active)
        next(iter(submission['reviews'].values())).update(result='异常', note='')
        self.service.change('a', '甲', DAY, 'finish', submission)
        rid = self.service.archive_history('D', DAY['date'])[0]['id']
        with patch.object(DailyInspection, 'download_archive', side_effect=lambda attachment: self.cloud.files[attachment['file_token']]):
            detail = self.client.get('/api/daily-inspection/history-record', params={'scope': 'D', 'id': rid}).json()['data']
            device = detail['devices_data'][0]
            row = device['rows'][0]
            body = {'id': rid, 'version': detail['version'], 'changes': {device['id']: {
                'values': {row['id']: '18'}, 'result': device['result'], 'note': device['note']}}}
            response = self.client.post('/api/daily-inspection/history-edit', params={'scope': 'D'},
                json=body, headers={'origin': 'http://testserver'})
            self.assertEqual(response.status_code, 200)
            edited = response.json()['data']['devices_data'][0]
            self.assertEqual((edited['rows'][0]['value'], edited['result'], edited['note']), ('18', '异常', ''))

    def test_non_roster_building_member_can_start_and_confirm_a_profession(self):
        current = self.client.get('/api/daily-inspection').json()['data']
        cancel = self.post('cancel', {'id': current['active']['id'], 'review_version': current['active']['review_version']})
        self.assertEqual(cancel.status_code, 200)
        duty = cancel.json()['data']['duty']
        started = self.post('start', {'duty': duty, 'professions': ['电气']})
        self.assertEqual(started.status_code, 200)
        active = started.json()['data']['active']
        result = self.post('confirm-profession', {'id': active['id'], 'profession': '电气',
            'profession_version': active['profession_versions']['电气'], 'reviews': {}})
        self.assertEqual(result.status_code, 200)
        ready = result.json()['data']['active']
        self.assertIsNotNone(ready)
        self.assertEqual(self.cloud.creates, 0)
        finished = self.post('finish', {'id': ready['id'], 'review_version': ready['review_version'], 'reviews': {}})
        self.assertEqual(finished.status_code, 200)
        self.assertIsNone(finished.json()['data']['active'])
        self.assertEqual(self.cloud.creates, 1)

    def test_reference_preview_returns_the_current_users_independent_orders(self):
        page = self.client.get('/api/daily-inspection').json()['data']
        active = page['active']
        ids = [device['id'] for device in active['batch']['devices']]
        response = self.post('order', {'id': active['id'], 'view': '电气', 'order': list(reversed(ids))})
        self.assertEqual(response.status_code, 200)
        preview = self.client.get('/api/daily-inspection/source', params={'view_scope': 'D'}).json()['data']
        self.assertEqual(preview['orders']['电气'], list(reversed(ids)))
        self.assertEqual(preview['orders']['ALL'], [])
        self.session['user']['open_id'] = 'ou_other'
        other = self.client.get('/api/daily-inspection/source', params={'view_scope': 'D'}).json()['data']
        self.assertEqual(other['orders']['电气'], [])

    def test_li_shilong_can_edit_de_without_expanding_other_users(self):
        self.modules, self.scopes = {'daily'}, ['D', 'E']
        self.home = 'H'
        self.session['user']['open_id'] = LI_SHILONG_OPEN_ID
        data = self.client.get('/api/daily-inspection?view_scope=D').json()['data']
        self.assertTrue(data['can_edit'])
        self.assertEqual(data['active']['duty']['scope'], 'D')
        identities = [item['id'] for item in data['active']['batch']['devices']]
        sorted_view = self.client.post('/api/daily-inspection/order?view_scope=D',
            json={'id': data['active']['id'], 'view': 'ALL', 'order': list(reversed(identities))},
            headers={'origin': 'http://testserver'})
        self.assertEqual(sorted_view.status_code, 200)
        self.assertTrue(sorted_view.json()['data']['can_edit'])
        report = self.post('day-sheet', {'scope': 'D', 'date': DAY['date'], 'drafts': {}}).json()['data']
        self.assertEqual(len(report['rounds']), 1)
        device = data['active']['batch']['devices'][0]
        parameter = device['parameters'][0]
        updated = self.client.post('/api/daily-inspection/draft?view_scope=D',
            json={'id': data['active']['id'], 'reviews': {device['id']: {'revision': 0,
                  'values': {parameter['id']: '29'}, 'result': '', 'note': ''}}},
            headers={'origin': 'http://testserver'})
        self.assertEqual(updated.status_code, 200)
        self.assertEqual(updated.json()['data']['active']['reviews'][device['id']]['values'][parameter['id']], '29')
        other_building = self.client.get('/api/daily-inspection?view_scope=E')
        self.assertEqual(other_building.status_code, 200)
        self.assertEqual(other_building.json()['data']['duty']['scope'], 'E')
        self.assertTrue(other_building.json()['data']['can_edit'])
        self.session['user']['open_id'] = 'ou_person'
        self.home = 'D'
        self.duty = DAY
        report = self.post('day-sheet', {'scope': 'D', 'date': DAY['date'], 'drafts': {}}).json()['data']
        self.assertEqual(len(report['rounds']), 1)
        self.assertEqual(self.client.get('/api/daily-inspection?view_scope=E').status_code, 403)


if __name__ == '__main__':
    unittest.main()
