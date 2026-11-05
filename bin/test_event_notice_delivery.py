import copy
import tempfile
import threading
import unittest
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from lan_bitable_template_portal.event_notice_delivery import EventNoticeDelivery,NS,OPS,CHANNEL,feishu_destination
from lan_bitable_template_portal.dingtalk_notices import DingTalkNotices
from lan_bitable_template_portal.portal_service import MaintenancePortalService,PortalError
from lan_bitable_template_portal.state_store import LanPortalStateStore
from lan_bitable_template_portal.event_priority import blockers


class EventDeliveryTests(unittest.TestCase):
    def setUp(self):
        folder=self.enterContext(tempfile.TemporaryDirectory());self.store=LanPortalStateStore(Path(folder)/'state.sqlite3')
        service=object.__new__(MaintenancePortalService);service.clear_target_record_cache=lambda:None
        self.runtime=SimpleNamespace(state_store=self.store,service=service,clear_payload_cache=lambda:None)
        self.manager=EventNoticeDelivery(self.runtime);self.calls=[]
        self.manager.ding.transport=SimpleNamespace(send=lambda token,secret,text:self.send(token,text))
        self.enterContext(patch('lan_bitable_template_portal.event_notice_delivery.dispatch'))
        self.enterContext(patch('lan_bitable_template_portal.event_notice_delivery.external_real_write_guard',return_value={'real_write_allowed':True}))
        self.enterContext(patch('lan_bitable_template_portal.event_notice_delivery.config.group_name_event_i3','测试I3群'))
        self.event='web_'+'a'*32
        self.robots=self.manager.ding.save({'revision':0,'robots':[{'name':'测试群'+c,'remark':'','enabled':True,'types':[],'scopes':[],
            'webhook':'https://oapi.dingtalk.com/robot/send?access_token='+c*64,'secret':'SEC'+'s'*64} for c in ('a','b')]})['robots']
        self.manager.save_preference('user',[r['id'] for r in self.robots],0)
    def send(self,token,text):self.calls.append((token,text));return {'state':'sent','reason':''}
    def accept(self,action='start',text=None,operation=None):
        return self.manager.accept(self.event,operation or {'start':'1','update':'2','end':'3'}[action]*32,'user',['D'],action,text or '【事件通告】状态：'+action+'\n【标题】D楼I2测试\n原文不拆分')
    def process(self):self.manager.process_messages()

    def test_no_metadata_required_raw_text_and_parallel_defaults(self):
        barrier=threading.Barrier(2,timeout=3)
        def send(token,secret,text):self.calls.append((token,text));barrier.wait();return {'state':'sent','reason':''}
        self.manager.ding.transport.send=send
        text='【事件通告】状态：新增\n【标题】D楼I2测试\n  保留空格及换行\n'
        self.accept(text=text);self.assertEqual(blockers(self.store),[])
        self.process()
        self.assertEqual(len(self.calls),2);self.assertTrue(all(call[1]==text for call in self.calls))
        self.assertEqual(self.manager.get(self.event)['metadata'],{})
        self.assertEqual(len(blockers(self.store)),1)
        self.assertEqual(self.manager.public(self.manager.get(self.event))['delivery']['state'],'sent')

    def test_default_change_does_not_retarget_an_accepted_task(self):
        self.accept();self.manager.save_preference('user',[self.robots[1]['id']],1);self.process()
        self.assertEqual(len(self.calls),2)
        self.manager.ding.store.put_document('dingtalk_notices','rate:'+self.robots[1]['id'],{'next_at':0})
        self.accept('update');self.process();self.assertEqual(len(self.calls),3)
        self.assertTrue(self.calls[-1][0].startswith('b'))

    def test_partial_failure_does_not_prevent_first_success_priority_or_repeat_success(self):
        self.manager.ding.transport.send=lambda token,secret,text:{'state':'failed','reason':'模拟拒绝'} if token.startswith('a') else self.send(token,text)
        self.accept();self.process();self.assertEqual(len(blockers(self.store)),1)
        self.assertEqual(len(self.calls),1)
        self.manager.ding.transport.send=lambda token,secret,text:self.send(token,text)
        for r in self.robots:self.store.put_document('dingtalk_notices','rate:'+r['id'],{'next_at':0})
        self.manager.retry(self.event,'1'*32);self.process();self.assertEqual(len(self.calls),2)
        self.assertTrue(self.calls[-1][0].startswith('a'))

    def test_end_success_releases_only_this_event_even_when_metadata_missing(self):
        self.accept();self.process()
        other='web_'+'b'*32
        self.manager.accept(other,'4'*32,'user',['D'],'start','【标题】另一个I2事件')
        for r in self.robots:self.store.put_document('dingtalk_notices','rate:'+r['id'],{'next_at':0})
        self.process();self.assertEqual(len(blockers(self.store)),2)
        self.accept('end')
        for r in self.robots:self.store.put_document('dingtalk_notices','rate:'+r['id'],{'next_at':0})
        self.process();self.assertEqual(len(blockers(self.store)),1)
        self.assertEqual(self.manager.get(self.event)['state'],'ended')
        self.assertEqual(len(self.manager.public(self.manager.get(self.event))['missing']),6)

    def test_i3_only_feishu_and_verbatim_text_without_field_autofill(self):
        self.assertIsNone(feishu_destination('【标题】D楼I2故障\n【概述】以前为I3'))
        text='【事件通告】状态：新增\n【标题】D楼I3事件\n【概述】实际现场信息'
        self.accept(text=text)
        def feishu(kind,content,recipients,typ,identity):
            self.assertEqual(content,{'text':text});return True,'ok',[{'ok':True,'message_id':'om_test','chat_id':'oc_test'}]
        with patch('upload_event_module.services.robot_webhook._find_chat_id_by_name',return_value=('oc_test','')),patch('upload_event_module.services.robot_webhook._deliver_many',side_effect=feishu) as call:
            self.process();self.assertEqual(call.call_count,1)
        self.assertEqual(self.manager.get(self.event)['metadata'],{})

    def test_duplicate_accept_and_restart_do_not_repeat_delivery(self):
        self.accept();self.accept();self.process();self.assertEqual(len(self.calls),2)
        manager=EventNoticeDelivery(self.runtime);manager.ding.transport=self.manager.ding.transport
        manager.process_messages();self.assertEqual(len(self.calls),2)
        with self.assertRaises(PortalError):self.accept(text='不同原文')

    def test_metadata_half_filled_and_post_end_updates_only_table(self):
        self.accept();self.process()
        result=self.manager.save_metadata(self.event,'user',['D'],{'summary':'手填概述'},{})
        self.assertEqual(len(result['missing']),5)
        records={};creates=[];updates=[]
        def create(kind,fields,**kw):creates.append((copy.deepcopy(fields),kw));records['rec_meta']=copy.deepcopy(fields);return True,'rec_meta'
        def read(*args):
            fields=copy.deepcopy(records['rec_meta'])
            if '告警描述' in fields:fields['告警描述']=[{'text':fields['告警描述'],'type':'text'}]
            return True,{'fields':fields}
        def update(rec,kind,fields):updates.append(copy.deepcopy(fields));records[rec].update(fields);return True,{}
        with patch('upload_event_module.services.service_registry.create_bitable_record_fields',side_effect=create),patch('upload_event_module.services.service_registry.query_record_by_id',side_effect=read),patch('upload_event_module.services.service_registry.update_bitable_record_fields',side_effect=update):
            self.manager.process_metadata();self.assertEqual(len(creates),1)
            self.assertEqual(self.manager.get(self.event)['sync_state'],'synced')
            self.assertEqual(len(blockers(self.store)),1)
            self.accept('end')
            for r in self.robots:self.store.put_document('dingtalk_notices','rate:'+r['id'],{'next_at':0})
            self.process();self.assertFalse(blockers(self.store))
            count=len(self.calls)
            self.manager.save_metadata(self.event,'user',['D'],{'summary':'结束后补充完整概述','building':'D'},{'summary':'手填概述','building':''})
            self.manager.process_metadata();self.assertEqual(len(creates),1);self.assertEqual(len(self.calls),count)
            self.assertEqual(records['rec_meta']['告警描述'],'结束后补充完整概述');self.assertIn('事件结束时间',records['rec_meta'])

    def test_failed_table_upload_does_not_block_messages_or_lose_partial_fields(self):
        self.accept();self.manager.save_metadata(self.event,'user',['D'],{'summary':'保留的资料'},{})
        self.process()
        with patch('upload_event_module.services.service_registry.create_bitable_record_fields',return_value=(False,'模拟拒绝')):
            self.manager.process_metadata()
        self.assertEqual(self.manager.get(self.event)['sync_state'],'retry')
        self.accept('update')
        for r in self.robots:self.store.put_document('dingtalk_notices','rate:'+r['id'],{'next_at':0})
        self.process();self.assertEqual(len(self.calls),4)
        self.assertEqual(self.manager.get(self.event)['metadata']['summary'],'保留的资料')

    def test_existing_automatic_dates_are_not_resent_by_metadata_edits(self):
        from upload_event_module.config import EVENT_NOTICE_FIELDS
        self.accept();self.process()
        row=self.manager.get(self.event);row.update(target_record_id='rec_meta',auto_dirty=[],dirty_keys=[])
        self.store.put_document(NS,self.event,row)
        self.manager.save_metadata(self.event,'user',['D'],{'summary':'补充资料'},{})
        fields=self.manager.metadata_fields(self.manager.get(self.event))
        self.assertEqual(fields,{EVENT_NOTICE_FIELDS['alarm_desc']:'补充资料'})

    def test_failed_create_reuses_identical_body_and_token_after_restart(self):
        self.accept();self.process();calls=[]
        def create(kind,fields,**kwargs):calls.append((copy.deepcopy(fields),kwargs));return False,'暂未确认'
        with patch('upload_event_module.services.service_registry.create_bitable_record_fields',side_effect=create):
            self.manager.process_metadata()
            self.manager.save_metadata(self.event,'user',['D'],{'summary':'稍后补充'},{})
            EventNoticeDelivery(self.runtime).process_metadata()
        self.assertEqual(len(calls),2);self.assertEqual(calls[0],calls[1])

    def test_metadata_conflicts_and_other_buildings_do_not_overwrite(self):
        self.manager.save_metadata(self.event,'user',['D'],{'summary':'第一份资料'},{})
        result=self.manager.save_metadata(self.event,'user',['D'],{'summary':'过期页面资料'},{})
        self.assertIn('summary',result['metadata_issues'])
        self.assertEqual(result['metadata']['summary'],'第一份资料')
        with self.assertRaises(PortalError):self.manager.save_metadata(self.event,'other',['E'],{'summary':'越权'},{})


if __name__=='__main__':unittest.main()
