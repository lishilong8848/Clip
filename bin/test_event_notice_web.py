import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
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
        auth=SimpleNamespace(scope_allowed=lambda session,scope:scope=='D',account_profile=lambda _: {'can_delete_business':False})
        controller=SimpleNamespace(_current_session=lambda _: {'user':{'open_id':'fixture-user'}},_get_ongoing=lambda _:[],_authorized_scope_or_error=lambda session,scope:scope)
        app=FastAPI();install_event_notice_web(app,controller,SimpleNamespace(service=service,state_store=store,auth_manager=auth))
        with TestClient(app) as client,patch('lan_bitable_template_portal.module_access.allowed_modules',return_value=['event']):
            result=client.get('/api/workbench-events?scope=D').json()
            self.assertTrue(result['ok']);self.assertEqual(len(result['data']['items']),1)
            self.assertIn('原进展',result['data']['items'][0]['text'])
            self.assertFalse(result['data']['can_delete'])
        with TestClient(app) as client,patch('lan_bitable_template_portal.module_access.allowed_modules',return_value=[]):
            self.assertEqual(client.get('/api/workbench-events?scope=D').status_code,403)


if __name__=='__main__':unittest.main()
