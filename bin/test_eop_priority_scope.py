import copy
import tempfile
import unittest
from pathlib import Path
from lan_bitable_template_portal.eop_scenario_editor import EopEditor,MAIN,LONG,SELECTS,STEPS,IMAGES,LINK,RiskError
from lan_bitable_template_portal.state_store import LanPortalStateStore

class Cloud:
    def __init__(self):
        self.rows={}
        for rid,priority in [('recHigh','P0-高'),('recOther','P1-中'),('recBlank','')]:
            self.rows[rid]={**dict.fromkeys((*MAIN,*LONG),''),'场景名称':rid,'专业':'电气','系统/设备':'变压器','原优先级':priority,STEPS:'检查设备',IMAGES:[{'file_token':'photo1','name':'现场图片'}]}
    def table_path(self,*args):return 'table'
    def all(self,path):
        kinds=dict.fromkeys((*LONG,MAIN[0],STEPS,LINK),1)|dict.fromkeys(SELECTS,3)|{IMAGES:17}
        return [{'field_name':name,'field_id':'field'+str(i),'type':kind,'property':{'options':[{'name':n} for n in ['电气','变压器','P0-高','P1-中','P2-低']]}} for i,(name,kind) in enumerate(kinds.items())]
    def request(self,method,path,**kw):
        if path.endswith('/search'):return {'items':[{'record_id':rid,'fields':copy.deepcopy(f)} for rid,f in self.rows.items()],'has_more':False}
        rid=path.rsplit('/',1)[1]
        if method=='PUT':self.rows[rid].update(copy.deepcopy(kw['json']['fields']))
        return {'record':{'record_id':rid,'fields':copy.deepcopy(self.rows[rid])}}

class EopPriorityTests(unittest.TestCase):
    def setUp(self):
        self.folder=self.enterContext(tempfile.TemporaryDirectory())
        self.store=LanPortalStateStore(Path(self.folder)/'state.sqlite3');self.cloud=Cloud();self.editor=EopEditor(self.cloud,self.store)
    def test_home_is_p0_and_other_contains_unset_after_cache_upgrade(self):
        self.store.put_document('eop_scenario_cache','snapshot',{'records':{'recHigh':self.cloud.rows['recHigh']}})
        self.assertEqual([r['id'] for r in self.editor.cached_list()['items']],['recHigh'])
        self.assertEqual({r['id'] for r in self.editor.cached_list('other')['items']},{'recOther','recBlank'})
        self.assertFalse(self.editor.cached_detail('recOther','reader',False)['editable'])
        with self.assertRaises(RiskError):self.editor.cached_list('invalid')
    def test_edit_other_and_move_to_p0_preserves_images_and_revision_guard(self):
        detail=self.editor.cached_detail('recOther','admin',True)
        body={key:detail[key] for key in ['revision','fields','steps','images']}
        body['fields']['原优先级']='P0-高';body['fields']['原通告模板']='新版资料'
        result=self.editor.save('recOther','admin',body)
        self.assertTrue(result['saved'])
        self.assertEqual(result['record']['images'][0]['token'],'photo1')
        self.assertIn('recOther',[r['id'] for r in self.editor.cached_list()['items']])
        self.assertNotIn('recOther',[r['id'] for r in self.editor.cached_list('other')['items']])
        with self.assertRaisesRegex(RiskError,'记录已变化'):self.editor.save('recOther','admin',body)

if __name__=='__main__':unittest.main()
