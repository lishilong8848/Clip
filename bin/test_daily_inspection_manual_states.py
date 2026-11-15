import copy
import datetime as dt
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from lan_bitable_template_portal.daily_inspection import (CN,DailyInspection,RiskError,inspection_report,
    ROUND_LOCATION_FIELDS, configured_form_devices, parameters, review_devices,
    manual_state_key, inherit_manual_states, adopt_manual_state_overrides, state_reference)
from lan_bitable_template_portal.state_store import LanPortalStateStore
from test_daily_inspection import InspectionCloud,DAY,NIGHT


class ManualStateTests(unittest.TestCase):
    def setUp(self):
        folder=self.enterContext(tempfile.TemporaryDirectory())
        self.store=LanPortalStateStore(Path(folder)/'state.sqlite3')
        self.cloud=InspectionCloud();self.service=DailyInspection(self.store,self.cloud)
        self.service.change('a','甲',DAY,'start',{'duty':DAY})
        self.device=self.param=None
        for device in review_devices(self.service.load('D')['active'],'D'):
            for param in parameters(device):
                if manual_state_key(device,param):self.device,self.param=device,param;break
            if self.device:break
        self.assertIsNotNone(self.param)
        self.point=next(p for p in self.device['points'] if p['id']==self.param['point'])
        self.key=manual_state_key(self.device,self.param)

    def edit(self,value,note=''):
        active=self.service.load('D')['active']
        cell={'template':self.point['template'],'sheet':self.point['sheet'],
              'cell':self.point['round_cells'][active['duty']['shift']+str(active['round'])],'value':value,'note':note}
        return self.service.change('a','甲',active['duty'],'draft',{'id':active['id'],
                                    'review_version':active.get('review_version',0),'sheet_cells':[cell]})

    def test_empty_status_cell_metadata_and_round_shift_restart_reuse(self):
        report=inspection_report(self.service,'D',DAY['date'],'a','甲',include_active=True,drafts={})
        cell=next(c for c in report['rounds'][0]['original_cells'] if c.get('point_id')==self.point['point_id'])
        self.assertEqual(cell['value_type'],'状态')
        self.edit('√')
        self.service.rollover_due('D',dt.datetime(2026,11,3,12,tzinfo=CN))
        self.service=DailyInspection(self.store,self.cloud)
        second=self.service.change('b','乙',DAY,'start',{'duty':DAY})['active']
        review=second['reviews'][self.device['id']]
        self.assertEqual(review['values'][self.param['id']],'√')
        self.assertEqual(review['actor'],'')
        self.assertFalse(second['profession_confirmations'])
        self.edit('△')
        self.service.rollover_due('D',dt.datetime(2026,11,3,20,tzinfo=CN))
        night=self.service.change('c','丙',NIGHT,'start',{'duty':NIGHT})['active']
        self.assertEqual(night['reviews'][self.device['id']]['values'][self.param['id']],'△')
        e_duty={**DAY,'scope':'E'}
        self.service.change('e','戊',e_duty,'start',{'duty':e_duty})
        self.assertFalse(self.service.load('E').get('manual_states'))
        self.assertFalse(self.service.load('E')['active'].get('reviews'))

    def test_open_chiller_round_updates_old_manual_type_without_replacing_its_batch(self):
        state = self.service.load('D')
        active = state['active']
        batch = active['batches']['D']
        device, point = next((device, point) for device in batch['form_devices'] for point in device['points']
                             if point['sheet'].strip() == '暖通冷机系统巡检表' and point['cell'] == 'K17')
        point['value_type'] = ''  # Snapshot created before K17 was declared a status cell.
        self.store.put_document('daily_de_inspection', 'building:D', state)
        report = inspection_report(self.service, 'D', DAY['date'], 'a', '甲', include_active=True, drafts={})
        cell = next(item for item in report['rounds'][0]['original_cells']
                    if item['sheet'] == point['sheet'] and item['cell'] == 'K17')
        self.assertEqual(cell['value_type'], '状态')

        self.point = point
        with patch.object(self.service, 'live_batch_belongs_to_round', return_value=False):
            with self.assertRaises(RiskError):
                self.edit('42')
            self.edit('√')
        saved = self.service.load('D')
        self.assertEqual(saved['active']['batches']['D']['batch'], batch['batch'])
        self.assertEqual(saved['active']['batches']['D']['form_devices'], batch['form_devices'])
        next_round = self.service.new_round(saved, DAY, 2, saved['active']['batches'], '乙',
                                            dt.datetime(2026, 11, 3, 12, tzinfo=CN))
        self.assertEqual(next_round['reviews'][device['id']]['values']['p:' + point['id']], '√')
        self.assertFalse(self.service.load('E').get('manual_states'))

    def test_clear_invalid_and_failed_save_do_not_leave_wrong_defaults(self):
        self.edit('×','接头异常')
        saved=copy.deepcopy(self.service.load('D')['manual_states'])
        self.assertEqual(saved[self.key]['note'],'接头异常')
        with self.assertRaises(RiskError):self.edit('x')
        self.assertEqual(self.service.load('D')['manual_states'],saved)
        with patch.object(self.store,'put_document',side_effect=OSError('模拟存储失败')):
            with self.assertRaises(OSError):self.edit('√')
        self.assertEqual(self.service.load('D')['manual_states'],saved)
        self.edit('')
        self.assertNotIn(self.key,self.service.load('D')['manual_states'])

    def test_real_mapping_or_changed_device_target_does_not_reuse_manual_value(self):
        device=copy.deepcopy(self.device);point=next(p for p in device['points'] if p['id']==self.param['point'])
        point['status_normal']='["1"]';point['raw_value']='1'
        param=next(p for p in parameters(device) if p['id']==self.param['id'])
        self.assertEqual(param['reference'],'√')
        self.assertIsNone(manual_state_key(device,param))
        point['status_normal']='[]';point['round_cells']['白1']='A999'
        param=next(p for p in parameters(device) if p['id']==self.param['id'])
        self.assertIsNotNone(manual_state_key(device,param))
        self.assertNotEqual(manual_state_key(device,param),self.key)
        device['scope']='E'
        self.assertNotEqual(manual_state_key(device,param),self.key)

    def test_configured_status_uses_fixed_default_and_preserves_manual_history(self):
        identity='local_'+'1'*32
        point=self.point
        fields={'测点ID':identity,'测点名称':'自检状态','专业':self.device['profession'],
                '测点来源':'manual_status','启用':True,'目标巡检表':point['template'],'目标工作表':point['sheet'],
                **{field:point['round_cells'][slot] for slot,field in ROUND_LOCATION_FIELDS.items()}}
        batch=self.service.load('D')['active']['batches']['D']
        devices=copy.deepcopy(batch['devices'])
        cfg=[{'record_id':'recConfigState','fields':fields}]
        forms=configured_form_devices('D',devices,cfg)
        selected=next(p for d in forms for p in d['points'] if p.get('configured_point_id')==identity)
        self.assertEqual(selected['point_id'],point['point_id'])
        self.assertEqual(selected['source_kind'],'manual_status')
        fixed=configured_form_devices('D',devices,[{'fields':fields | {'测点来源':'fixed_status','固定值':'△'}}])
        matched=next((d,p) for d in fixed for p in parameters(d)
                     if any(row.get('configured_point_id')==identity and row['id']==p.get('point') for row in d['points']))
        self.assertEqual(matched[1]['reference'],'△')
        self.assertIsNone(manual_state_key(*matched))

    def test_range_status_uses_corrected_number_and_rejects_nonmatch(self):
        rules=[{'min':'-2','max':'0','status':'not_running'}, {'min':'1','max':'10','status':'normal'}]
        point={'status_intervals':json.dumps(rules), 'raw_value':'9', 'corrected_value':'0'}
        self.assertEqual(state_reference('',point),'△')
        point['corrected_value']='1e1'
        self.assertEqual(state_reference('',point),'√')
        point['corrected_value']='0.5'
        self.assertEqual(state_reference('',point),'')
        point.update(raw_value=None, corrected_value=None)
        self.assertEqual(state_reference('',point),'')
        point['raw_value']='0'
        point['status_intervals']=json.dumps(rules+[{'min':'0','max':'3','status':'abnormal'}])
        self.assertEqual(state_reference('',point),'')

    def test_edit_on_newly_mapped_state_becomes_the_default_on_later_rounds(self):
        state=self.service.load('D')
        active=state['active']
        active['sheet_overrides']=[{'template':self.point['template'],'sheet':self.point['sheet'],
            'cell':self.point['round_cells']['白1'],'value':'×','note':'待更换','actor':'甲','at':5}]
        adopt_manual_state_overrides(state,active,'D')
        self.assertFalse(active['sheet_overrides'])
        self.assertEqual(active['reviews'][self.device['id']]['values'][self.param['id']],'×')
        self.assertEqual(state['manual_states'][self.key]['note'],'待更换')
        later={**active,'round':2,'reviews':{}}
        inherit_manual_states(state,later,'D')
        self.assertEqual(later['reviews'][self.device['id']]['values'][self.param['id']],'×')
        self.assertEqual(later['reviews'][self.device['id']]['note'],'待更换')
        automatic=copy.deepcopy(later)
        point=next(p for d in automatic['batches']['D']['form_devices'] if d['id']==self.device['id']
                   for p in d['points'] if p['id']==self.point['id'])
        point['source_kind']='fixed_status'
        point['fixed_value']='√'
        inherit_manual_states(state,automatic,'D')
        review=automatic['reviews'][self.device['id']]
        self.assertNotIn(self.param['id'],review['values'])
        self.assertEqual((review['note'],review['result']),('',''))


if __name__=='__main__':unittest.main()
