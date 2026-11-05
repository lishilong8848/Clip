import copy,datetime as dt,tempfile,time,unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock,patch
from lan_bitable_template_portal import change_screenshot_cards as cards
from lan_bitable_template_portal.state_store import LanPortalStateStore
from lan_bitable_template_portal.portal_service import BUILDING_OPEN_ID_MAP,MaintenancePortalService
from lan_bitable_template_portal.server import PortalRuntime,CHANGE_NOTICE_FIELDS

class CardTests(unittest.TestCase):
 def setUp(self):
  self.folder=self.enterContext(tempfile.TemporaryDirectory());self.store=LanPortalStateStore(Path(self.folder)/'state.sqlite3')
  self.task={'target_record_id':'recCard','screenshot_fingerprint':'f1','title':[{'type':'text','text':'柴油发电机组月度测试变更'}],'building':'D楼'}
  self.runtime=SimpleNamespace(state_store=self.store,auth_manager=SimpleNamespace(_open_id_explicitly_disabled=lambda _:False,role_for_open_id=lambda _:'user',is_admin=lambda _:False),confirm_change_confirmation=Mock(return_value=self.task),clear_payload_cache=Mock())
  self.enterContext(patch('upload_event_module.services.robot_webhook.current_app_credentials',return_value=('cli_test','secret-fixture')))
  self.enterContext(patch('lan_bitable_template_portal.feishu_delivery.current_recipient_open_id',side_effect=lambda r,o,a:o))
  self.card,self.uuid=cards.prepare(self.runtime,self.task,BUILDING_OPEN_ID_MAP['H'],'https://example.test/workbench')
  self.token=self.card['body']['elements'][2]['behaviors'][0]['value']['token']
 def event(self,sender=None):return {'header':{'app_id':'cli_test','event_type':'card.action.trigger'},'event':{'operator':{'open_id':sender or BUILDING_OPEN_ID_MAP['H']},'action':{'value':{'kind':'change_screenshot_confirm','token':self.token}}}}
 def test_plain_title_stable_card_and_authorization(self):
  self.assertNotIn("[{'",self.card['body']['elements'][0]['text']['content'])
  self.assertEqual(cards.prepare(self.runtime,self.task,BUILDING_OPEN_ID_MAP['H'],'https://example.test/workbench')[1],self.uuid)
  self.assertIn('柴油发电机组',MaintenancePortalService._clean_source_text(self.task['title']))
  self.assertTrue(cards.apply(self.runtime,self.event(),'cli_test')['ok'])
  self.assertEqual(self.runtime.confirm_change_confirmation.call_args.kwargs['expected_fingerprint'],'f1')
  with self.assertRaisesRegex(ValueError,'管理员或H楼'):cards.apply(self.runtime,self.event('ou_other'),'cli_test')
  with self.assertRaises(ValueError):cards.apply(self.runtime,self.event(),'cli_wrong')
 def test_two_hours_from_actual_end_not_creation(self):
  end=dt.datetime(2026,11,5,12,tzinfo=dt.timezone(dt.timedelta(hours=8)))
  lifecycle={'finished':True,'ended_at':'2026-11-05 12:00:00'}
  cards.check_lifetime(lifecycle,end.timestamp()+7199)
  with self.assertRaisesRegex(ValueError,'过期'):cards.check_lifetime(lifecycle,end.timestamp()+7200)
  with self.assertRaisesRegex(ValueError,'未核实'):cards.check_lifetime({'finished':True})
  cards.check_lifetime({'active':True},end.timestamp()+86400*30)
 def test_ended_record_grace_is_checked_in_shared_runtime(self):
  end=dt.datetime(2026,11,5,12,tzinfo=dt.timezone(dt.timedelta(hours=8)))
  lifecycle={'finished':True,'active':False,'ended_at':'2026-11-05 12:00:00'}
  runtime=SimpleNamespace(service=SimpleNamespace(_target_record_lifecycle=lambda **_:lifecycle),
      _change_confirmation_fields=lambda r:r['fields'],stop_change_confirmation=Mock())
  query=lambda:PortalRuntime._query_active_change_confirmation_record.__func__(runtime,'recCard',allow_recently_ended=True)
  with patch('lan_bitable_template_portal.server.query_record_by_id',return_value=(True,{'fields':{}})):
   with patch.object(cards.time,'time',return_value=end.timestamp()+7199):self.assertEqual(query(),({'fields':{}},{}))
   with patch.object(cards.time,'time',return_value=end.timestamp()+7200):
    with self.assertRaisesRegex(Exception,'过期'):query()

 def test_runtime_rejects_changed_screenshot_and_is_idempotent(self):
  fields={CHANGE_NOTICE_FIELDS['ali_confirmation_snapshot']:[{'file_token':'newphoto'}],CHANGE_NOTICE_FIELDS['h_confirmation']:False}
  class Runtime:
   _acquire_event_operation_lock=staticmethod(lambda *a,**k:('lock','owner',''))
   _release_event_operation_lock=staticmethod(lambda *a:None)
   _query_active_change_confirmation_record=staticmethod(lambda *a,**k:({'fields':fields},fields))
   _change_confirmation_fields=staticmethod(lambda r:r.get('fields',{}))
   _change_confirmation_fingerprint=PortalRuntime._change_confirmation_fingerprint
   _change_confirmation_attachment_count=PortalRuntime._change_confirmation_attachment_count
   _change_confirmation_checked=PortalRuntime._change_confirmation_checked
   _change_confirmation_task_from_record=staticmethod(lambda *a,**k:{'title':'变更'})
   _put_change_confirmation_task=staticmethod(lambda t:t)
   _patch_change_confirmation_projection=staticmethod(lambda *a,**k:None)
   _change_confirmation_screenshot_is_fresh=staticmethod(lambda t:False)
  Runtime.state_store=self.store
  call=lambda fingerprint:PortalRuntime.confirm_change_confirmation.__func__(Runtime,'recCard',actor_open_id='ou_h',actor_name='H楼',expected_fingerprint=fingerprint,allow_recently_ended=True)
  def update(*args):
   fields[CHANGE_NOTICE_FIELDS['h_confirmation']]=True
   return True,{}
  with patch('lan_bitable_template_portal.server.update_bitable_record_fields',side_effect=update) as write,patch('lan_bitable_template_portal.server.query_record_by_id',return_value=(True,{'fields':fields})):
   with self.assertRaisesRegex(Exception,'截图已变更'):call('old')
   current=Runtime._change_confirmation_fingerprint(fields[CHANGE_NOTICE_FIELDS['ali_confirmation_snapshot']])
   call(current);call(current);self.assertEqual(write.call_count,1)

if __name__=='__main__':unittest.main()
