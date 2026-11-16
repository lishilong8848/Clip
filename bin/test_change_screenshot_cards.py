import asyncio,copy,datetime as dt,json,tempfile,time,unittest,uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock,Mock,patch
from lan_bitable_template_portal import change_screenshot_cards as cards
from lan_bitable_template_portal.feishu_delivery import PublicFeishuSender
from lan_bitable_template_portal.state_store import LanPortalStateStore
from lan_bitable_template_portal.portal_service import BUILDING_OPEN_ID_MAP,MaintenancePortalService
from lan_bitable_template_portal.server import PortalRuntime,CHANGE_NOTICE_FIELDS

class CardTests(unittest.TestCase):
 def setUp(self):
  self.folder=self.enterContext(tempfile.TemporaryDirectory());self.store=LanPortalStateStore(Path(self.folder)/'state.sqlite3')
  self.task={'target_record_id':'recCard','screenshot_fingerprint':'f1','title':[{'type':'text','text':'柴油发电机组月度测试变更'}],'building':'D楼',
      'started_at':'2026-11-05T04:30:00Z','screenshot_tokens':['file-one','file-two']}
  self.runtime=SimpleNamespace(state_store=self.store,auth_manager=SimpleNamespace(_open_id_explicitly_disabled=lambda _:False,role_for_open_id=lambda _:'user',is_admin=lambda _:False),confirm_change_confirmation=Mock(return_value=self.task),clear_payload_cache=Mock())
  self.runtime.get_change_confirmation_screenshot_bytes=Mock(side_effect=lambda _, **kw:
      (kw['file_token'].encode(), 'image/png', 'ali.png'))
  self.enterContext(patch('upload_event_module.services.robot_webhook.current_app_credentials',return_value=('cli_test','secret-fixture')))
  self.enterContext(patch('lan_bitable_template_portal.feishu_delivery.current_recipient_open_id',side_effect=lambda r,o,a:o))
  self.enterContext(patch.object(cards.token_manager,'get_tenant_token',return_value='test-token'))
  self.upload=self.enterContext(patch.object(cards._HTTP,'request',side_effect=lambda *a, **kw:
      self.image_receipt('img_' + kw['files']['image'][1].decode())))
  self.card,self.uuid=cards.prepare(self.runtime,self.task,BUILDING_OPEN_ID_MAP['H'],'https://example.test/workbench')
  self.token=next(el['behaviors'][0]['value']['token'] for el in self.card['body']['elements']
      if el['tag']=='button' and el['behaviors'][0]['type']=='callback')
 @staticmethod
 def image_receipt(key):return SimpleNamespace(status_code=200,json=lambda:{'code':0,'data':{'image_key':key}})
 @staticmethod
 def images(card):return [item for item in card['body']['elements'] if item['tag']=='img']
 def event(self,sender=None):return {'header':{'app_id':'cli_test','event_type':'card.action.trigger'},'event':{'operator':{'open_id':sender or BUILDING_OPEN_ID_MAP['H']},'action':{'value':{'kind':'change_screenshot_confirm','token':self.token}}}}
 def test_plain_title_stable_card_and_authorization(self):
  self.assertNotIn("[{'",self.card['body']['elements'][0]['text']['content'])
  self.assertEqual(cards.prepare(self.runtime,self.task,BUILDING_OPEN_ID_MAP['H'],'https://example.test/workbench')[1],self.uuid)
  retry=dict(self.task,title='更新后的标题',screenshot_tokens=list(reversed(self.task['screenshot_tokens'])))
  self.assertEqual(cards.prepare(self.runtime,retry,BUILDING_OPEN_ID_MAP['H'],'https://example.test/new'),
                   (self.card,self.uuid))
  self.assertIn('柴油发电机组',MaintenancePortalService._clean_source_text(self.task['title']))
  self.assertIn('开始时间：2026-11-05 12:30（北京时间）',self.card['body']['elements'][0]['text']['content'])
  self.assertEqual([im['img_key'] for im in self.images(self.card)],['img_file-one','img_file-two'])
  self.assertTrue(all(im['preview'] for im in self.images(self.card)))
  self.assertEqual(self.upload.call_count,2)
  confirmed=cards.apply(self.runtime,self.event(),'cli_test')
  self.assertTrue(confirmed['ok'])
  self.assertEqual(self.images(confirmed['card']),self.images(self.card))
  self.assertEqual(confirmed['card']['header']['template'],'green')
  self.assertEqual(self.upload.call_count,2)
  self.assertEqual(self.runtime.confirm_change_confirmation.call_args.kwargs['expected_fingerprint'],'f1')
  with self.assertRaisesRegex(ValueError,'管理员或H楼'):cards.apply(self.runtime,self.event('ou_other'),'cli_test')
  with self.assertRaises(ValueError):cards.apply(self.runtime,self.event(),'cli_wrong')

 def test_retry_prepares_only_missing_images_before_sending_card(self):
  changed=dict(self.task,screenshot_fingerprint='f2',screenshot_tokens=['file-three','file-four'])
  with patch('lan_bitable_template_portal.server.external_real_write_guard',return_value={'real_write_allowed':True}), \
       patch('upload_event_module.services.robot_webhook.send_interactive_to_open_ids',return_value=(True,'ok',[])) as deliver:
   self.upload.reset_mock(side_effect=True)
   self.upload.side_effect=[self.image_receipt('img_three'),OSError('temporary failure')]
   self.assertFalse(cards.send(self.runtime,changed,BUILDING_OPEN_ID_MAP['H'],'https://example.test/workbench')[0])
   deliver.assert_not_called()
   restarted=SimpleNamespace(**vars(self.runtime))
   restarted.state_store=LanPortalStateStore(Path(self.folder)/'state.sqlite3')
   self.upload.reset_mock(side_effect=True)
   self.upload.return_value=self.image_receipt('img_four')
   self.assertTrue(cards.send(restarted,changed,BUILDING_OPEN_ID_MAP['H'],'https://example.test/workbench')[0])
   self.upload.assert_called_once()
   self.assertEqual(self.upload.call_args.kwargs['files']['image'][1],b'file-four')
   card=deliver.call_args.args[0]
   self.assertEqual([item['img_key'] for item in self.images(card)],['img_three','img_four'])
   self.assertNotEqual(deliver.call_args.kwargs['message_uuid'],self.uuid)

 def test_old_card_recovery_respects_existing_delivery(self):
  recipient=BUILDING_OPEN_ID_MAP['H']
  task=dict(self.task,screenshot_fingerprint='legacy')
  identity=cards._identity('cli_test',task)
  old_uuid=str(uuid.uuid5(uuid.NAMESPACE_URL,'change-screenshot:'+identity))
  route=PublicFeishuSender.route_key(recipient,'open_id',old_uuid)
  self.store.put_document(cards.NS,'target:'+identity,{'token':self.token,'record_id':'recCard',
      'fingerprint':'legacy','app_id':'cli_test','recipient':recipient,'link':'https://example.test/workbench'})
  with patch('lan_bitable_template_portal.server.external_real_write_guard',return_value={'real_write_allowed':True}), \
       patch('upload_event_module.services.robot_webhook.send_interactive_to_open_ids',return_value=(True,'ok',[])) as deliver:
   self.store.put_document('feishu_public_delivery',route,{'status':'sent'})
   self.assertTrue(cards.send(self.runtime,task,recipient,'https://example.test/workbench')[0])
   self.store.put_document('feishu_public_delivery',route,{'status':'uncertain'})
   self.assertFalse(cards.send(self.runtime,task,recipient,'https://example.test/workbench')[0])
   deliver.assert_not_called()
   self.assertEqual(self.upload.call_count,2)
   self.store.put_document('feishu_public_delivery',route,{'status':'not_sent'})
   self.assertTrue(cards.send(self.runtime,task,recipient,'https://example.test/workbench')[0])
   deliver.assert_called_once()
   self.assertNotEqual(deliver.call_args.kwargs['message_uuid'],old_uuid)

 def test_task_reads_real_start_from_target_record(self):
  start=dt.datetime(2026,11,5,12,30,tzinfo=dt.timezone(dt.timedelta(hours=8)))
  record={'fields':{CHANGE_NOTICE_FIELDS['title']:self.task['title'],'楼栋':'D楼',
                    CHANGE_NOTICE_FIELDS['status']:'开始',CHANGE_NOTICE_FIELDS['start_time']:int(start.timestamp()*1000)}}
  runtime=SimpleNamespace(service=MaintenancePortalService())
  for name in ('_change_confirmation_fields','_change_confirmation_attachment_count',
               '_change_confirmation_attachment_tokens','_change_confirmation_fingerprint',
               '_change_confirmation_checked','_change_confirmation_active_item_id'):
   setattr(runtime,name,getattr(PortalRuntime,name))
  task=PortalRuntime._change_confirmation_task_from_record.__func__(runtime,'recCard',record)
  self.assertEqual(task['started_at'],'2026-11-05 12:30')

 def test_loopback_confirmation_returns_a_renderable_raw_card(self):
  from lark_oapi import JSON
  from lark_oapi.event.callback.model.p2_card_action_trigger import CallBackCard, P2CardActionTriggerResponse
  from starlette.requests import Request
  from lan_bitable_template_portal.feishu_assistant import FeishuAssistant

  channel=SimpleNamespace(inbox=object(),secret='isolated-bridge',config={'app_id':'cli_test'},
      runtime=self.runtime,_configuration_current=AsyncMock(return_value=True))
  async def receive():
   return {'type':'http.request','body':json.dumps(self.event()).encode(),'more_body':False}

  req=Request({'type':'http','method':'POST','scheme':'http','path':'/callback',
      'client':('127.0.0.1',12345),'headers':[(b'authorization',b'Bearer isolated-bridge')]},receive)
  response=asyncio.run(FeishuAssistant.receive(channel,req))
  self.assertEqual(response.status_code,200)
  result=json.loads(response.body)
  self.assertTrue(result['ok'])
  wire_response=P2CardActionTriggerResponse()
  wire_response.card=CallBackCard({'type':'raw','data':result['card']})
  wire=json.loads(JSON.marshal(wire_response))
  self.assertEqual(wire['card']['type'],'raw')
  confirmed=wire['card']['data']
  self.assertEqual(confirmed['header']['title']['content'],'变更阿里确认截图已确认')
  self.assertEqual(self.images(confirmed),self.images(self.card))
  self.assertTrue(all(item['preview'] for item in self.images(confirmed)))
  self.assertFalse(any(behavior['type']=='callback' for item in confirmed['body']['elements']
                       for behavior in item.get('behaviors',())))
  self.runtime.confirm_change_confirmation.assert_called_once()

 def test_pending_receipt_fits_inside_worker_budget(self):
  async def expire(awaitable, *, timeout):
   awaitable.close()
   raise asyncio.TimeoutError

  pending=AsyncMock(side_effect=expire)
  with patch.object(cards.asyncio,'wait_for',pending):
   result=asyncio.run(cards.callback(self.runtime,self.event(),'cli_test'))
  self.assertLess(pending.call_args.kwargs['timeout'],2)
  self.assertIn('尚在核对',result['toast'])
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
