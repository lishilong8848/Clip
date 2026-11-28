import copy
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from lan_bitable_template_portal.eop_scenario_editor import (
    EopEditor, MAIN, LONG, SELECTS, STEPS, IMAGES, LINK, SCENARIO_CODE, RiskError,
    disposition_cards, install_eop_scenario_routes,
)
from lan_bitable_template_portal.state_store import LanPortalStateStore

class Cloud:
    def __init__(self):
        self.rows={}
        self.code_kind = None
        for rid,priority in [('recHigh','P0-高'),('recOther','P1-中'),('recBlank','')]:
            self.rows[rid]={**dict.fromkeys((*MAIN,*LONG),''),'场景名称':rid,'专业':'电气','系统/设备':'变压器','原优先级':priority,STEPS:'检查设备',IMAGES:[{'file_token':'photo1','name':'现场图片'}]}
    def table_path(self,*args):return 'table'
    def all(self,path):
        kinds=dict.fromkeys((*LONG,MAIN[0],STEPS,LINK),1)|dict.fromkeys(SELECTS,3)|{IMAGES:17}
        if self.code_kind is not None:
            kinds[SCENARIO_CODE] = self.code_kind
        return [{'field_name':name,'field_id':'field'+str(i),'type':kind,'property':{'options':[{'name':n} for n in ['电气','变压器','P0-高','P1-中','P2-低']]}} for i,(name,kind) in enumerate(kinds.items())]
    def request(self,method,path,**kw):
        if path.endswith('/search'):return {'items':[{'record_id':rid,'fields':copy.deepcopy(f)} for rid,f in self.rows.items()],'has_more':False}
        if method == 'POST' and path.endswith('/fields'):
            assert kw['json'] == {'field_name': SCENARIO_CODE, 'type': 1}
            self.code_kind = 1
            return {}
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

    def test_scene_code_can_be_saved_on_legacy_table_and_preserves_record_identity(self):
        detail = self.editor.cached_detail('recHigh', 'admin', True)
        body = {key: detail[key] for key in ('revision', 'fields', 'steps', 'images')}
        body['fields'][SCENARIO_CODE] = 'EOP-001'
        result = self.editor.save('recHigh', 'admin', body)['record']
        self.assertEqual(result['id'], 'recHigh')
        self.assertEqual(result['fields'][SCENARIO_CODE], 'EOP-001')
        self.assertEqual(result['images'][0]['token'], 'photo1')
        self.assertEqual(self.editor.cached_list()['items'][0]['fields'][SCENARIO_CODE], 'EOP-001')
        with self.assertRaisesRegex(RiskError, '记录已变化'):
            self.editor.save('recHigh', 'admin', body)

    def test_readonly_scene_code_is_not_overwritten(self):
        self.cloud.code_kind = 1005
        self.cloud.rows['recHigh'][SCENARIO_CODE] = '0001'
        detail = self.editor.cached_detail('recHigh', 'admin', True)
        self.assertIn(SCENARIO_CODE, detail['readonly_fields'])
        body = {key: detail[key] for key in ('revision', 'fields', 'steps', 'images')}
        body['fields'][SCENARIO_CODE] = '0002'
        with self.assertRaisesRegex(RiskError, '不是可编辑的文本字段'):
            self.editor.save('recHigh', 'admin', body)
        self.assertEqual(self.cloud.rows['recHigh'][SCENARIO_CODE], '0001')

    def test_numbered_substeps_keep_line_breaks_and_image_groups(self):
        detail = self.editor.cached_detail('recHigh', 'admin', True)
        body = {key: detail[key] for key in ('revision', 'fields', 'images')}
        body['steps'] = [
            {'text': '检查设备\r\n1. 记录读数\r\n\r\n第二行说明\r\n2、核对现场', 'images': ['photo1']},
            {'text': '通知值班人员', 'images': []},
        ]
        saved = self.editor.save('recHigh', 'admin', body)['record']
        expected = [step | {'text': step['text'].replace('\r\n', '\n')} for step in body['steps']]
        self.assertEqual(saved['steps'], expected)
        self.assertEqual(self.editor.detail('recHigh', 'admin', True)['steps'], expected)
        self.assertTrue(self.cloud.rows['recHigh'][STEPS].startswith('1、检查设备\n1. 记录读数'))
        self.assertEqual(json.loads(self.cloud.rows['recHigh'][LINK])['step_lines'], [5, 1])
        self.cloud.rows['recHigh'][STEPS] = '1、云端更正\n2、核对现场'
        self.assertEqual(self.editor.detail('recHigh', 'admin', True)['steps'],
                         [{'text': '云端更正', 'images': []}, {'text': '核对现场', 'images': []}])

    def test_cards_keep_long_text_and_order_within_feishu_limits(self):
        content = '确认状态\n' + '观察数据' * 4000
        cards = disposition_cards({'场景名称': '测试', STEPS: '1、' + content + '\n2、完成复核'})
        self.assertGreater(len(cards), 1)
        rendered = []
        for index, card in enumerate(cards, 1):
            self.assertEqual(card['schema'], '2.0')
            self.assertFalse(card['config']['enable_forward'])
            self.assertIn(f'{index}/{len(cards)}', card['header']['title']['content'])
            wire = json.dumps({'content': json.dumps(card, ensure_ascii=False)}, ensure_ascii=False)
            self.assertLess(len(wire.encode('utf-8')), 30 * 1024)
            for panel in card['body']['elements'][1:]:
                self.assertEqual(panel['tag'], 'collapsible_panel')
                rendered.append(panel['elements'][0]['text']['content'])
        self.assertEqual(''.join(rendered), content + '完成复核')

    def test_send_route_checks_permission_and_reuses_delivery_ids_for_fixed_recipients(self):
        from lan_bitable_template_portal.feishu_delivery import FeishuDeliveryError
        from lan_bitable_template_portal.portal_service import BUILDING_OPEN_ID_MAP, LI_SHILONG_OPEN_ID

        session = {'user': {'open_id': 'test-user'}, 'admin': True}
        async def read_json(request, **_):
            return await request.json()
        controller = SimpleNamespace(_current_session=lambda _: session,
                                     _request_base_url=lambda _: 'http://testserver', _read_json_request=read_json)
        runtime = SimpleNamespace(state_store=self.store, auth_manager=SimpleNamespace(is_admin=lambda s: s['admin']))
        app = FastAPI()
        install_eop_scenario_routes(app, controller, runtime)
        client = TestClient(app)
        path = '/api/eop-scenarios/recHigh/send-card'
        body = {'revision': self.editor.revision(self.cloud.rows['recHigh']), 'delivery_id': 'a' * 32}
        headers = {'Origin': 'http://testserver'}
        previous, delivered = {}, set()
        fail_once = {LI_SHILONG_OPEN_ID}

        async def send_fake(_runtime, recipient, card, *, message_uuid):
            if recipient in previous:
                self.assertEqual(previous[recipient], message_uuid)
            previous[recipient] = message_uuid
            self.assertNotIn('img', {element['tag'] for element in card['body']['elements']})
            if recipient in fail_once:
                fail_once.remove(recipient)
                raise FeishuDeliveryError('测试暂未发送')
            delivered.add(recipient)
            return {'message_id': 'om_fixture'}

        with (patch('lan_bitable_template_portal.eop_scenario_editor.RiskCloud', return_value=self.cloud),
              patch('lan_bitable_template_portal.eop_scenario_editor.allowed_modules', return_value={'eop_scenarios'}),
              patch('lan_bitable_template_portal.module_access.profession_query', return_value={}),
              patch('lan_bitable_template_portal.feishu_delivery.send_agent_message', side_effect=send_fake) as send):
            self.assertEqual(client.post(path, headers={'Origin': 'https://outside.invalid'}, json=body).status_code, 403)
            session['admin'] = False
            self.assertEqual(client.post(path, headers=headers, json=body).status_code, 403)
            session.update(admin=True, impersonation=True)
            self.assertEqual(client.post(path, headers=headers, json=body).status_code, 403)
            session.pop('impersonation')
            self.assertEqual(client.post(path, headers=headers, json=body | {'revision': 'old'}).status_code, 409)
            send.assert_not_awaited()
            response = client.post(path, headers=headers, json=body).json()
            self.assertTrue(response['ok'])
            self.assertEqual(response['data']['recipients'], [
                {'name': 'H楼值班账号', 'sent': True}, {'name': '李世龙', 'sent': False, 'error': '测试暂未发送'},
            ])
            retry = client.post(path, headers=headers, json=body).json()
            self.assertTrue(all(result['sent'] for result in retry['data']['recipients']))
        self.assertEqual(set(previous), {BUILDING_OPEN_ID_MAP['H'], LI_SHILONG_OPEN_ID})
        self.assertEqual(delivered, set(previous))

if __name__=='__main__':unittest.main()
