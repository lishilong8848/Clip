import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock,patch
from types import SimpleNamespace
from lan_bitable_template_portal.portal_service import MaintenancePortalService,PortalError
from lan_bitable_template_portal.state_store import LanPortalStateStore
from lan_bitable_template_portal.dingtalk_notice_time import sync_start_time


class WebEventTests(unittest.TestCase):
    def setUp(self):
        self.tmp=self.enterContext(tempfile.TemporaryDirectory())
        self.service=object.__new__(MaintenancePortalService)
        self.service._state_store=LanPortalStateStore(Path(self.tmp)/'state.sqlite3')
        self.service._recipients_for_building_codes=lambda *a,**kw:('D',['test-recipient'],'')
        self.request={'scope':'D','work_type':'event','notice_type':'事件通告','action':'start','manual':True,
            'manual_id':'manual_event_test','record_id':'manual_event_test','operation_id':'web_event_test',
            'title':'D楼隔离验证事件','content':'隔离测试概述','impact':'隔离测试影响','progress':'隔离测试进展',
            'time_str':'2026-11-05 09:00','event_source':'巡检发现','level':'I3','specialty':'电气','building_codes':['D']}

    def test_pasted_event_autofill_uses_explicit_fields_without_guessing(self):
        from lan_bitable_template_portal.event_notice_web import parse_event_metadata
        text=('【事件通告】状态：新增\n【标题】D楼I3隔离事件\n【概述】设备告警\n'
              '【事件发生时间】2026-11-05 09:30\n【来源】BMS\n【专业】电气')
        self.assertEqual(parse_event_metadata(self.service,text,['D']),{
            'summary':'设备告警','building':'D','level':'I3','event_source':'BMS动环系统告警',
            'specialty':'电气','occurrence_time':'2026-11-05 09:30'})
        result=parse_event_metadata(self.service,'【标题】D楼和E楼事件\n【发生时间】9:30',['D','E'])
        self.assertNotIn('building',result);self.assertNotIn('occurrence_time',result)
        self.assertNotIn('building',parse_event_metadata(self.service,text,['E']))

    def test_mention_uses_actual_event_buildings_not_actor_scope(self):
        from lan_bitable_template_portal.event_notice_web import event_mention_buildings
        scopes=['D','E']
        self.assertEqual(event_mention_buildings(self.service,'【标题】D楼事件',scopes,{}, {}, []),['D'])
        self.assertEqual(event_mention_buildings(self.service,'【标题】D楼和E楼事件',scopes,{}, {}, []),['D','E'])
        self.assertEqual(event_mention_buildings(self.service,'【位置】发电机房\n【标题】E楼事件',scopes,{}, {}, []),['E'])
        self.assertEqual(event_mention_buildings(self.service,'【位置】D楼\n【标题】E楼事件',scopes,{}, {}, []),[])
        self.assertEqual(event_mention_buildings(self.service,'【标题】事件',scopes,{}, {}, []),[])
        self.assertEqual(event_mention_buildings(self.service,'【标题】D楼事件',scopes,{}, {'building':'E'}, ['building']),['E'])
        self.assertEqual(event_mention_buildings(self.service,'【标题】D楼事件',['E'],{}, {}, []),[])

    def test_api_immediate_send_fills_missing_data_and_preserves_manual_values(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from lan_bitable_template_portal.event_notice_web import install_event_notice_web
        from lan_bitable_template_portal.event_notice_delivery import EventNoticeDelivery
        store=self.service._state_store
        auth=SimpleNamespace(scope_allowed=lambda session,scope:scope=='D',
            account_profile=lambda _: {'can_delete_business':False,'modules':['event'],'scope_rules':['D']})
        async def read_json(request,max_bytes):return await request.json()
        controller=SimpleNamespace(_current_session=lambda _: {'user':{'open_id':'fixture-user'}},
            _authorized_scope_or_error=lambda session,scope:scope,_request_base_url=lambda _: 'http://testserver',
            _read_json_request=read_json)
        runtime=SimpleNamespace(service=self.service,state_store=store,auth_manager=auth)
        manager=EventNoticeDelivery(runtime)
        robot=manager.ding.save({'revision':0,'robots':[{'name':'隔离群','enabled':True,'scopes':[],'types':[],
            'webhook':'https://oapi.dingtalk.com/robot/send?access_token='+'a'*64,'secret':'SEC'+'s'*64}]})['robots'][0]
        manager.save_preference('fixture-user',[robot['id']],0)
        app=FastAPI()
        with patch('lan_bitable_template_portal.event_notice_delivery.dispatch'):
            install_event_notice_web(app,controller,runtime)
        raw='【事件通告】状态：新增\n【标题】D楼I3事件\n【概述】原文概述\n【发生时间】2026-11-05 09:30\n【来源】BMS\n【专业】电气'
        body={'scope':'D','event_id':'web_'+'a'*32,'operation_id':'1'*32,'action':'start','text':raw,
            'metadata_patch':{'summary':'人工概述'},'metadata_base':{},'manual_fields':['summary']}
        with TestClient(app) as client,patch('lan_bitable_template_portal.module_access.allowed_modules',return_value=['event']),patch('lan_bitable_template_portal.event_notice_delivery.dispatch'):
            response=client.post('/api/workbench-events/send',json=body,headers={'origin':'http://testserver'})
        self.assertEqual(response.status_code,200,response.text)
        data=response.json()['data'];self.assertEqual(data['text'],raw)
        self.assertEqual(data['metadata']['summary'],'人工概述');self.assertEqual(data['metadata']['building'],'D')
        self.assertEqual(data['metadata']['occurrence_time'],'2026-11-05 09:30')
        second='web_'+'b'*32
        manager.save_metadata(second,'fixture-user',['D'],{'summary':'上一段自动概述'},{})
        body.update(event_id=second,operation_id='2'*32,metadata_patch={},manual_fields=[],auto_fields={'summary':'上一段自动概述'})
        with TestClient(app) as client,patch('lan_bitable_template_portal.module_access.allowed_modules',return_value=['event']),patch('lan_bitable_template_portal.event_notice_delivery.dispatch'):
            response=client.post('/api/workbench-events/send',json=body,headers={'origin':'http://testserver'})
        self.assertEqual(response.status_code,200,response.text)
        self.assertEqual(response.json()['data']['metadata']['summary'],'原文概述')

    def test_event_start_update_end_use_existing_text_and_upload_without_screenshot(self):
        from lan_bitable_template_portal.server import PortalRuntime
        for action,label in [('start','新增'),('update','更新'),('end','结束')]:
            payload={**self.request,'action':action}
            if action!='start':payload.update(target_record_id='rec_event_test',record_id='rec_event_test')
            prepared=self.service.prepare_workbench_action(payload,job_id='event-test')
            self.assertIn('状态：'+label,prepared['text'])
            self.assertIn('【概述】隔离测试概述',prepared['text'])
            self.assertFalse(prepared['source_record_id'])
            self.assertEqual(prepared['extra_images'],[])
            with patch('lan_bitable_template_portal.server.external_real_write_guard',return_value={'mock_external':True,'real_write_allowed':False}):
                ok,_,_=PortalRuntime._execute_backend_prepared_upload(prepared)
            self.assertTrue(ok)

    def test_invalid_level_missing_target_and_cross_building_rejected(self):
        for changes in ({'level':'invalid'},{'action':'update'},{'building_codes':['E']}):
            with self.assertRaises(PortalError):self.service.prepare_workbench_action({**self.request,**changes},job_id='event-test')

    def test_public_event_routes_use_authenticated_business_allowlist(self):
        from public_access.ingress import API_ROOTS,ANONYMOUS,DENIED
        self.assertIn('workbench-events',API_ROOTS)
        self.assertIn('dingtalk-notices',API_ROOTS)
        self.assertNotIn(('GET','/api/workbench-events'),ANONYMOUS)
        self.assertNotIn(('GET','/api/dingtalk-notices'),ANONYMOUS)
        self.assertIn('/api/qt/',DENIED)

    def test_receipt_time_readback_retry_never_repeats_confirmed_patch(self):
        from contextlib import nullcontext
        from upload_event_module.services.handlers import NoticePayload,get_notice_handler
        payload={'text':'【维保通告】状态：开始\n【名称】隔离测试\n【时间】2026-11-05 09:00~2026-11-05 18:00','response_time':'2026-11-05 09:00'}
        baseline=get_notice_handler('维保通告').build_create_fields(NoticePayload(**payload))['实际开始时间']
        record={'实际开始时间':baseline};writes=[]
        runtime=SimpleNamespace(service=SimpleNamespace(get_job=lambda _:{}),_notice_robot_impersonation=lambda _:nullcontext(),clear_payload_cache=lambda:None)
        operation={'operation_id':'op','operation_type':'start','target_record_id':'rec_test','result':{'robot_notice_type':'维保通告','robot_payload':payload}}
        def write(rec,kind,value):writes.append(value);record.update(value);return True,{}
        with patch('lan_bitable_template_portal.dingtalk_notice_time.query_record_by_id',side_effect=lambda *a:(True,{'fields':dict(record)})),patch('lan_bitable_template_portal.dingtalk_notice_time.update_bitable_record_fields',side_effect=write):
            self.assertEqual(sync_start_time(runtime,operation,1800000000),'synced')
            self.assertEqual(sync_start_time(runtime,operation,1800000000),'synced')
            self.assertEqual(len(writes),1)
            record['实际开始时间']=baseline+1000
            self.assertEqual(sync_start_time(runtime,operation,1800000000),'conflict')
            self.assertEqual(len(writes),1)

    def test_listing_reuses_saved_full_text_and_enforces_module_and_building(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from lan_bitable_template_portal.event_notice_web import install_event_notice_web
        service=self.service
        service._target_records_for_notice_type=lambda *a,**kw:[{'record_id':'rec_'+code,'display_fields':{'机楼':code+'楼','告警描述':code+'楼事件','事件发生时间':'2026-11-05 09:00'}} for code in ['D','E']]
        store=service._state_store
        store.begin_notice_remote_operation(operation_id='event-web',operation_type='start',request={})
        store.mark_notice_remote_operation('event-web',status='completed',target_record_id='rec_D',result={'robot_notice_type':'事件通告','robot_payload':{'text':'【事件通告】状态：新增\n【标题】D楼事件\n【概述】原概述\n【影响】原影响\n【进展】原进展'}})
        from lan_bitable_template_portal.account_policy import business_delete_denial
        auth=SimpleNamespace(scope_allowed=lambda session,scope:scope=='D',
            account_profile=lambda _: {'can_delete_business':False,'modules':['event'],'scope_rules':['D']})
        async def read_json(request,max_bytes):return await request.json()
        controller=SimpleNamespace(_current_session=lambda _: {'user':{'open_id':'fixture-user'}},
            _get_ongoing=lambda _:[],_authorized_scope_or_error=lambda session,scope:scope,
            _request_base_url=lambda _: 'http://testserver',_read_json_request=read_json)
        delete=Mock(return_value={'ok':True})
        runtime=SimpleNamespace(service=service,state_store=store,auth_manager=auth,execute_local_delete_active_item=delete)
        app=FastAPI()
        with patch('lan_bitable_template_portal.event_notice_delivery.dispatch'):
            install_event_notice_web(app,controller,runtime)
        @app.middleware('http')
        async def guard(request,call_next):
            denial=await business_delete_denial(request,controller,runtime)
            return denial if denial is not None else await call_next(request)
        with TestClient(app) as client,patch('lan_bitable_template_portal.module_access.allowed_modules',return_value=['event']):
            result=client.get('/api/workbench-events?scope=D').json()
            self.assertTrue(result['ok']);self.assertEqual(len(result['data']['items']),1)
            self.assertIn('原进展',result['data']['items'][0]['text'])
            self.assertTrue(result['data']['can_delete'])
            with patch('lan_bitable_template_portal.event_notice_delivery.EventNoticeDelivery.get',
                       return_value={'event_id':'remote_rec_E','access_scopes':['E']}):
                denied=client.post('/api/workbench-events/delete',json={'scope':'D','event_id':'remote_rec_E'},
                                   headers={'origin':'http://testserver'})
            self.assertEqual(denied.status_code,409,denied.text)
            delete.assert_not_called()
            response=client.post('/api/workbench-events/delete',json={'scope':'D','event_id':'remote_rec_D'},
                                 headers={'origin':'http://testserver'})
            self.assertEqual(response.status_code,200,response.text)
            self.assertEqual(delete.call_args.args[0]['data_dict']['target_record_id'],'rec_D')
        with TestClient(app) as client,patch('lan_bitable_template_portal.module_access.allowed_modules',return_value=[]):
            self.assertEqual(client.get('/api/workbench-events?scope=D').status_code,403)


if __name__=='__main__':unittest.main()
