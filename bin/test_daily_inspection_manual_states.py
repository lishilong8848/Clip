import copy
import datetime as dt
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from lan_bitable_template_portal.daily_inspection import (CN,DailyInspection,RiskError,inspection_report,
    parameters,review_devices,manual_state_key,inherit_manual_states)
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


if __name__=='__main__':unittest.main()
