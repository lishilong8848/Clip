import io
import json
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from zipfile import ZipFile

from bin.test_tool_self_check import actor
from bin.lan_bitable_template_portal.risk_inspection import RiskError, RiskInspections
from bin.lan_bitable_template_portal.tool_management import ToolManagement, clock
from bin.lan_bitable_template_portal.tool_management_cloud import DETAIL, INVENTORY, KEY, STATE, TABLES, ToolCloud


class LedgerFixture:
    def __init__(self):
        self.assets = [{'id': f'recTool{index}', 'name': f'验收工具{index}', 'scope': 'A', 'location': '工具间',
                        'model': '测试型号', 'quantity': 2, 'retired': False, 'self_cycle': '每月',
                        'external_cycle': '一年' if index == 0 else '', 'self_last': '', 'external_last': ''}
                       for index in range(3)]
        self.writes = []
        self.files = []

    def inventory(self):
        return self.assets, {'A': ['ou_leader']}

    def write(self, dest, order, asset, job, person_for, save_file):
        self.writes.append((dest, order['outcome'], job['remote_id']))
        if job.get('record_content'):
            self.files.append((dest,job['record_name'],job['record_content']))
        return job['remote_id'] or 'rec' + dest


class ToolManagementTest(TestCase):
    def full_actor(self,name):
        return actor(scope='H',identity=name) | {'name':name,'level':'L4','category':'engineer',
             'modules':[],'scope_rules':['H'],'_tool_management_full_access':True}

    def test_designated_people_can_manage_all_buildings_without_expanding_risk_permissions(self):
        from bin.lan_bitable_template_portal.risk_inspection import can_scope
        for name in ('李世龙','刘小杏'):
            who=self.full_actor(name)
            self.assertTrue(self.svc.available_to(who))
            board=self.svc.list(who)
            self.assertEqual(len(board['orders']),4)
            self.assertTrue(board['can_manage_templates'])
            self.assertTrue(board['can_download_month'])
            self.assertEqual(len(self.svc.inventory(who)['items']),3)
            self.assertTrue(all(row['can_claim'] for row in board['orders']))
            self.assertFalse(can_scope(who,'A'),'工具专用授权不能改变专项排查权限')
        ordinary=self.full_actor('李世龙') | {'_tool_management_full_access':False}
        with self.assertRaises(RiskError):self.svc.list(ordinary)

    def test_designated_people_can_handle_claimed_report_and_approve_scrap(self):
        li=self.full_actor('李世龙');liu=self.full_actor('刘小杏')
        external=next(row for row in self.orders if row['kind']=='external')
        claimed=self.svc.action(self.leader,external['id'],'claim',{'revision':external['revision']})
        self.assertTrue(self.svc.detail(liu,external['id'])['can_edit'])
        attached=self.svc.attach(li,external['id'],claimed['revision'],'报告.pdf',b'%PDF-1.7\nfixture')
        body={'revision':attached['revision'],'actual':self.date}
        submitted=self.svc.action(liu,external['id'],'submit',body)
        self.assertEqual(submitted['owner_name'],'刘小杏')
        self.assertEqual(self.svc.action(liu,external['id'],'submit',body),submitted)
        self.assertEqual(self.svc.detail(li,external['id'])['history'][-1]['name'],'刘小杏')
        scrap=self.submit_self(next(row for row in self.orders if row['kind']=='self'),'scrap')
        self.assertTrue(self.svc.detail(li,scrap['id'])['can_approve'])
        self.assertTrue(self.svc.detail(liu,scrap['id'])['can_approve'])
        self.assertFalse(self.svc.detail(self.admin,scrap['id'])['can_approve'])
        approved=self.svc.action(liu,scrap['id'],'approve',{'revision':scrap['revision'],'reason':'核对报废'})
        self.assertEqual(approved['outcome'],'scrapped')

    def test_full_access_requires_current_unique_directory_identity(self):
        from types import SimpleNamespace
        from bin.lan_bitable_template_portal.tool_management import trusted_full_access
        person={'id':'recLiu','name':'刘小杏','selectable':True,'login_ids':['ou_new']}
        snapshot={'schema':4,'app_id':'current','people':{'recLiu':person}}
        auth=SimpleNamespace(_account_read_state=lambda:None,_account_directory_provider=lambda:snapshot,current_feishu_app_id=lambda:'current')
        self.assertTrue(trusted_full_access(auth,'ou_new',person))
        self.assertFalse(trusted_full_access(auth,'ou_someone_else',person))
        self.assertFalse(trusted_full_access(auth,'ou_new',{}))
        self.assertFalse(trusted_full_access(auth,'ou_new',person|{'inactive':True}))
        snapshot['people']['duplicate']={**person,'id':'duplicate'}
        self.assertFalse(trusted_full_access(auth,'ou_new',person))
        del snapshot['people']['duplicate'];snapshot['app_id']='outdated'
        self.assertFalse(trusted_full_access(auth,'ou_new',person))

    def test_module_gate_opens_tools_without_granting_risk_writes(self):
        import asyncio
        from types import SimpleNamespace
        from unittest.mock import patch
        from starlette.requests import Request
        from bin.lan_bitable_template_portal.module_access import module_denial
        who=self.full_actor('刘小杏')
        session={'user':{'open_id':who['open_id'],'name':who['name']},'role':'user'}
        runtime=SimpleNamespace(tool_management=lambda:self.svc,risk_inspection_actor=lambda *_:who,
                                auth_manager=SimpleNamespace(public_user=lambda s:dict(s['user'])))
        controller=SimpleNamespace(_current_session=lambda _:session)
        def check(path,method='GET'):
            return asyncio.run(module_denial(Request({'type':'http','path':path,'query_string':b'','headers':[],
                        'method':method,'scheme':'http','server':('testserver',80)}),controller,runtime))
        with patch('bin.lan_bitable_template_portal.module_access.allowed_modules',return_value=set()):
            self.assertIsNone(check('/api/tool-self-check'))
            self.assertIsNone(check('/api/tool-self-check/orders/fixture/submit','POST'))
            self.assertIsNone(check('/api/risk-inspections/people'))
            self.assertEqual(check('/api/risk-inspections/template','POST').status_code,403)

    def setUp(self):
        temp = TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.admin = actor(identity='admin', admin=True)
        self.leader = actor() | {'category': 'supervisor', 'position': '楼长'}
        self.supervisor = actor(identity='professional') | {'category': 'supervisor', 'position': '专业主管'}
        self.park = actor(identity='manager', admin=True) | {'level': 'L2', 'category': 'manager'}
        self.people = {person['id']: person for person in (self.admin, self.leader, self.supervisor, self.park)}
        self.cloud = LedgerFixture()
        self.svc = ToolManagement(RiskInspections(Path(temp.name) / 'tool.db', self.people.__getitem__), self.cloud)
        self.svc.refresh()
        self.svc.configure(self.admin, 'park-manager', {'manager_id': 'manager'})
        self.svc.configure(self.admin, 'recTool0', {'revision': 0, 'supervisor': 'professional'})
        self.svc.schedule()
        self.svc.schedule()
        self.month = clock().strftime('%Y-%m')
        self.date = clock().date().isoformat()
        self.orders = self.svc.list(self.leader)['orders']
        self.template = self.svc.list(self.admin)['templates'][0]

    def submit_self(self, order, outcome):
        claimed = self.svc.action(self.leader, order['id'], 'claim', {'revision': order['revision']})
        records = [{'entry_id': '0' * 32, 'template_id': self.template['id'], 'version': self.template['version'],
                    'checks': ['是'] * len(self.template['schema']['checks']), 'location': '工具间',
                    'inspector': '验收楼长', 'checked_at': self.date + 'T09:00'}]
        body = {'revision': claimed['revision'], 'records': records, 'outcome': outcome, 'reason': '发现磨损', 'quantity': 1}
        result = self.svc.action(self.leader, order['id'], 'submit', body)
        self.assertEqual(result, self.svc.action(self.leader, order['id'], 'submit', body))
        self.assertEqual(result['status'], 'review_pending')
        self.assertFalse(result['sync'])
        with self.assertRaises(RiskError):
            self.svc.action(self.leader, order['id'], 'submit', body | {'outcome': 'normal' if outcome != 'normal' else 'repair'})
        with self.assertRaises(RiskError):
            self.svc.action(self.supervisor, order['id'], 'review-approve', {'revision': result['revision']})
        return self.svc.action(self.leader, order['id'], 'review-approve', {'revision': result['revision']})

    def test_routes_to_all_ledgers_and_archives_both_types(self):
        self.assertEqual(len(self.orders), 4)
        external = next(row for row in self.orders if row['kind'] == 'external')
        self.assertTrue(external['can_claim'])
        self.assertEqual([row['id'] for row in self.svc.list(self.supervisor)['orders']], [external['id']])
        claim = self.svc.action(self.supervisor, external['id'], 'claim', {'revision': external['revision']})
        with self.assertRaises(RiskError):
            self.svc.action(self.leader, external['id'], 'claim', {'revision': claim['revision']})
        with self.assertRaisesRegex(RiskError, '先上传'):
            self.svc.action(self.supervisor, external['id'], 'submit', {'revision': claim['revision'], 'actual': self.date})
        report = b'%PDF-1.7\nfixture'
        attached = self.svc.attach(self.supervisor, external['id'], claim['revision'], '检测报告.pdf', report)
        self.svc.action(self.supervisor, external['id'], 'submit', {'revision': attached['revision'], 'actual': self.date})
        normal, repair, scrap = [self.submit_self(row, outcome) for row, outcome in zip(
            [row for row in self.orders if row['kind'] == 'self'], ['normal', 'repair', 'scrap'], strict=True)]
        self.assertFalse(self.svc.detail(self.admin, scrap['id'])['can_approve'])
        self.assertTrue(self.svc.detail(self.park, scrap['id'])['can_approve'])
        while self.svc.sync_one():
            pass
        self.assertEqual(sorted(dest for dest, _, _ in self.cloud.writes),
                         ['change', 'change', 'inspection', 'maintenance', 'repair', 'scrap'])
        self.assertEqual(sorted(dest for dest,_,_ in self.cloud.files),['maintenance','repair','scrap'])
        for _,name,content in self.cloud.files:
            self.assertTrue(name.endswith('.xlsx'))
            with ZipFile(__import__('io').BytesIO(content)) as record:
                self.assertIn('xl/workbook.xml',record.namelist())
        repair = self.svc.action(self.leader, repair['id'], 'repair-scrap',
                                 {'revision': repair['revision'], 'reason': '无法修复', 'quantity': 1})
        returned = self.svc.action(self.park, repair['id'], 'reject',
                                   {'revision': repair['revision'], 'reason': '继续检修'})
        self.assertEqual(returned['outcome'], 'repair')
        self.assertTrue(self.svc.detail(self.leader, repair['id'])['can_repair'])
        repair = self.svc.action(self.leader, repair['id'], 'repair-scrap',
                                 {'revision': returned['revision'], 'reason': '复核无法修复', 'quantity': 1})
        approved = self.svc.action(self.park, repair['id'], 'approve', {'revision': repair['revision'], 'reason': ''})
        self.assertEqual(approved['outcome'], 'scrapped')
        rejected = self.svc.action(self.park, scrap['id'], 'reject', {'revision': scrap['revision'], 'reason': '继续使用'})
        self.assertEqual(rejected['outcome'], 'rejected')
        while self.svc.sync_one():
            pass
        self.assertIn(('repair', 'scrapped', 'recrepair'), self.cloud.writes)
        self.assertIn(('scrap', 'rejected', 'recscrap'), self.cloud.writes)
        self.svc.archive_ready()
        with ZipFile(self.svc.download(self.admin, self.month)) as package:
            self.assertIsNone(package.testzip())
            manifest = json.loads(package.read('归档清单.json'))
            self.assertEqual(len(manifest['orders']), 4)
            self.assertEqual(package.read(next(name for name in package.namelist() if name.endswith('.pdf'))), report)
            self.assertEqual(sum(name.endswith('.xlsx') for name in package.namelist()), 3)
        with self.assertRaises(RiskError):
            self.svc.download(self.leader, self.month)

    def test_malformed_report_and_incomplete_month(self):
        with self.assertRaisesRegex(RiskError, '有效'):
            self.svc.attach(self.leader, self.orders[0]['id'], 0, '损坏.docx', b'invalid')
        self.svc.archive_ready()
        with self.assertRaisesRegex(RiskError, '尚未生成'):
            self.svc.download(self.admin, self.month)
        with self.assertRaises(RiskError):
            self.svc.detail(actor(scope='B'), self.orders[0]['id'])

    def test_review_rejection_preserves_draft_and_does_not_create_ledgers(self):
        order = next(row for row in self.orders if row['kind'] == 'self')
        claim = self.svc.action(self.leader, order['id'], 'claim', {'revision': order['revision']})
        records = [{'entry_id': '1' * 32, 'template_id': self.template['id'], 'version': self.template['version'],
                    'checks': ['是'] * len(self.template['schema']['checks']), 'location': '工具间',
                    'inspector': '验收人员', 'checked_at': self.date + 'T09:00'}]
        pending = self.svc.action(self.leader, order['id'], 'submit', {'revision': claim['revision'],
            'records': records, 'outcome': 'normal', 'reason': '', 'quantity': 1})
        self.assertEqual(pending['status'], 'review_pending')
        self.assertEqual(self.svc.detail(self.leader, order['id'])['draft']['outcome'], 'normal')
        self.assertFalse(self.svc.sync_one())
        with self.assertRaises(RiskError):
            self.svc.action(self.supervisor, order['id'], 'review-approve', {'revision': pending['revision']})
        rejected = self.svc.action(self.leader, order['id'], 'review-reject',
                                   {'revision': pending['revision'], 'reason': '请核对工具信息'})
        detail = self.svc.detail(self.leader, order['id'])
        self.assertTrue(rejected['can_edit'])
        self.assertEqual(detail['records'][0]['inspector'], '验收人员')
        self.assertEqual(detail['draft']['outcome'], 'normal')
        self.assertEqual(detail['review_note'], '请核对工具信息')
        self.assertEqual(self.cloud.writes, [])

    def test_delete_template_keeps_pinned_draft_and_download(self):
        with self.svc.connect() as db:
            content = self.svc._template(db, self.template['id'], self.template['version'])['content']
        template = self.svc.save_template(self.admin, '待删除测试模板', content)
        identity = template['id']
        version = {'version': template['version']}
        order = next(row for row in self.orders if row['kind'] == 'self')
        claim = self.svc.action(self.leader, order['id'], 'claim', {'revision': order['revision']})
        record = {'entry_id': '2' * 32, 'template_id': identity, **version,
                  'checks': ['是'] * len(template['schema']['checks']), 'location': '工具间',
                  'inspector': '验收人员', 'checked_at': self.date + 'T09:00'}
        body = {'revision': claim['revision'], 'records': [record], 'outcome': 'normal', 'quantity': 1}
        draft = self.svc.action(self.leader, order['id'], 'draft', body)
        with self.assertRaisesRegex(RiskError, '仅管理员'):
            self.svc.delete_template(self.leader, identity, version)
        self.assertEqual(self.svc.delete_template(self.admin, identity, version), {'deleted': True})
        self.assertEqual(self.svc.delete_template(self.admin, identity, version), {'deleted': True})
        for viewer in (self.admin, self.leader):
            self.assertNotIn(identity, [item['id'] for item in self.svc.list(viewer)['templates']])
        with self.assertRaises(RiskError):
            self.svc.availability(self.admin, identity, version | {'active': True})
        with self.assertRaises(RiskError):
            self.svc.clone_template(self.admin, identity, version | {'name': '不可复制'})

        body['revision'] = draft['revision']
        with self.assertRaises(RiskError):
            self.svc.action(self.leader, order['id'], 'draft', body | {'records': [record | {'entry_id': '3' * 32}]})
        self.assertEqual(self.svc.detail(self.leader, order['id'])['records'][0]['template'], template)
        pending = self.svc.action(self.leader, order['id'], 'submit', body)
        self.svc.action(self.leader, order['id'], 'review-approve', {'revision': pending['revision']})
        with ZipFile(io.BytesIO(self.svc.download(self.leader, identity=order['id']))) as archive:
            self.assertTrue(any(name.endswith('待删除测试模板.xlsx') for name in archive.namelist()))
        reimported = self.svc.save_template(self.admin, template['name'], content)
        self.assertEqual((reimported['id'], reimported['version']), (identity, template['version'] + 1))
        with self.assertRaisesRegex(RiskError, '模板已更新'):
            self.svc.delete_template(self.admin, identity, version)

    def test_manual_order_creation_and_safe_soft_delete(self):
        asset = self.cloud.assets[1]
        self.assertTrue(any(item['id'] == asset['id'] for item in self.svc.order_candidates(self.leader)['items']))
        order = self.svc.create_order(self.leader, {'asset_id': asset['id'], 'kind': 'external'})
        self.assertTrue(order['can_delete'])
        with self.assertRaises(RiskError):
            self.svc.create_order(self.leader, {'asset_id': asset['id'], 'kind': 'external'})
        with self.assertRaises(RiskError):
            self.svc.cancel_order(self.supervisor, order['id'], {'revision': order['revision']})
        deleted = self.svc.cancel_order(self.leader, order['id'], {'revision': order['revision']})
        self.assertEqual(deleted['status'], 'cancelled')
        self.assertFalse(deleted['can_claim'])
        with self.svc.connect() as db:
            self.assertTrue(db.execute("SELECT 1 FROM tool_work_audit WHERE order_id=? AND action='删除工单'", (order['id'],)).fetchone())

    def test_batch_create_uses_current_month_plan_and_retries_without_duplicates(self):
        first_day = clock().date().replace(day=1)
        due = (first_day + timedelta(days=14)).isoformat()
        asset = self.cloud.assets[0] | {'id': 'recBatch', 'self_first': due, 'external_first': due}
        next_month = (first_day + timedelta(days=32)).replace(day=1).isoformat()
        future = asset | {'id': 'recFuture', 'self_first': next_month, 'external_first': next_month}
        other_scope = asset | {'id': 'recOther', 'scope': 'B'}
        self.cloud.assets.extend([asset, future, other_scope])
        self.svc.refresh()

        plan = self.svc.planned_order_candidates(self.leader)
        self.assertEqual(plan['month'], self.month)
        self.assertEqual({(row['asset_id'], row['kind'], row['due']) for row in plan['items']},
                         {('recBatch', 'self', due), ('recBatch', 'external', due)})
        selection = [{'asset_id': row['asset_id'], 'kind': row['kind']} for row in plan['items']]
        body = {'month': self.month, 'orders': selection}
        for invalid in ('recFuture', 'recOther'):
            with self.assertRaises(RiskError):
                self.svc.create_planned_orders(self.leader, body | {'orders': [*selection, {'asset_id': invalid, 'kind': 'self'}]})
        with self.assertRaises(RiskError):
            self.svc.create_planned_orders(self.supervisor, body)
        with self.assertRaises(RiskError):
            self.svc.create_planned_orders(self.leader, body | {'month': '2000-01'})
        self.assertEqual(len(self.svc.list(self.leader)['orders']), len(self.orders))
        self.assertEqual(self.svc.create_planned_orders(self.leader, body),
                         {'month': self.month, 'created_count': 2, 'existing_count': 0})
        self.assertEqual(self.svc.create_planned_orders(self.leader, body),
                         {'month': self.month, 'created_count': 0, 'existing_count': 2})
        self.svc.schedule()
        new_orders = [row for row in self.svc.list(self.leader)['orders'] if row['id'] not in {item['id'] for item in self.orders}]
        self.assertEqual(sorted((row['kind'], row['due']) for row in new_orders), [('external', due), ('self', due)])
        self.assertEqual(self.svc.planned_order_candidates(self.leader)['items'], [])

    def test_batch_delete_is_atomic_and_prevents_regeneration(self):
        first, second, untouched = self.orders[:3]
        entries = [{key: order[key] for key in ('id', 'revision')} for order in (first, second)]
        body = {'month': self.month, 'orders': entries}
        with self.assertRaises(RiskError):
            self.svc.cancel_orders(self.leader, body | {'orders': [entries[0], entries[1] | {'revision': second['revision'] + 1}]})
        with self.assertRaises(RiskError):
            self.svc.cancel_orders(self.leader, body | {'orders': [entries[0], entries[0]]})
        for order in (first, second):
            detail = self.svc.detail(self.leader, order['id'])
            self.assertEqual(detail['status'], 'pending')
            self.assertEqual(detail['revision'], order['revision'])
            self.assertNotIn('删除工单', [entry['action'] for entry in detail['history']])

        self.assertEqual(self.svc.cancel_orders(self.leader, body), {'deleted_ids': [first['id'], second['id']]})
        with self.assertRaises(RiskError):
            self.svc.cancel_orders(self.leader, body)
        self.svc.schedule()
        for order in (first, second):
            detail = self.svc.detail(self.leader, order['id'])
            self.assertEqual(detail['status'], 'cancelled')
            self.assertFalse(detail['can_delete'])
            self.assertEqual([entry['action'] for entry in detail['history']].count('删除工单'), 1)
        self.assertEqual(self.svc.detail(self.leader, untouched['id'])['status'], 'pending')
        self.assertEqual(len(self.svc.list(self.leader)['orders']), len(self.orders))
        self.assertEqual(self.cloud.writes, [])

    def test_batch_delete_respects_delete_permission(self):
        order = next(row for row in self.orders if row['kind'] == 'external')
        self.assertFalse(self.svc.detail(self.supervisor, order['id'])['can_delete'])
        with self.assertRaises(RiskError):
            self.svc.cancel_orders(self.supervisor, {'month': self.month, 'orders': [{'id': order['id'], 'revision': order['revision']}]})
        self.assertEqual(self.svc.detail(self.leader, order['id'])['status'], 'pending')

    def test_inventory_create_idempotence_and_unconfirmed_result(self):
        schema=[{'name':'楼栋','type':3,'options':['南通A楼','南通B楼']},
                {'name':'工具名称','type':1,'options':[]},{'name':'一级分类','type':3,'options':['B工具类']},
                {'name':'区域对应工具数量','type':2,'options':[]}]
        calls=[]
        self.cloud.inventory_fields=lambda:schema
        def create(fields,operation):
            calls.append(operation)
            if operation.startswith('b'):raise RuntimeError('fixture: connection lost')
            return 'recNewFixture'
        self.cloud.create_inventory=create
        fields={'楼栋':'南通A楼','工具名称':'工具验收','一级分类':'B工具类','区域对应工具数量':1}
        body={'operation_id':'a'*32,'fields':fields}
        self.assertEqual(self.svc.create_inventory(self.leader,body)['record_id'],'recNewFixture')
        self.assertEqual(self.svc.create_inventory(self.leader,body)['record_id'],'recNewFixture')
        with self.assertRaises(RiskError):self.svc.create_inventory(self.leader,body|{'fields':fields|{'楼栋':'南通B楼'}})
        ambiguous=body|{'operation_id':'b'*32}
        with self.assertRaises(RiskError):self.svc.create_inventory(self.leader,ambiguous)
        with self.assertRaises(RiskError):self.svc.create_inventory(self.leader,ambiguous)
        self.assertEqual(len(calls),2)

    def test_report_receipt_reused_after_unconfirmed_create(self):
        class Transport:
            uploads = 0
            record = None

            @staticmethod
            def table_path(base, table):
                return f'{base}/{table}'

            def all(self, path):
                self_case.assertIn(TABLES['inspection'], path)
                return [{'field_name': name, 'type': kind, 'property': props} for name, kind, props in
                        [(KEY, 1, {}), (STATE, 1, {}), (DETAIL, 1, {}), ('楼栋', 3, {}),
                         ('送检工具', 21, {'table_id': INVENTORY}), ('计划检测日期', 5, {}),
                         ('实际检测日期', 5, {}), ('送检报告', 17, {})]]

            def request(self, method, path, **kwargs):
                if path.endswith('/upload_all'):
                    self.uploads += 1
                    return {'file_token': 'report-token'}
                if path.endswith('/search'):
                    return {'items': [self.record] if self.record else [], 'has_more': False}
                if method == 'POST':
                    self.record = {'record_id': 'recRemote', 'fields': kwargs['json']['fields']}
                    raise RiskError('连接中断，新增结果待确认。')
                self_case.assertEqual(method, 'PUT')
                self_case.assertTrue(path.endswith('/recRemote'))
                return {'record': self.record}

        self_case = self
        transport = Transport()
        cloud = ToolCloud(transport)
        external = next(row for row in self.orders if row['kind'] == 'external')
        claim = self.svc.action(self.leader, external['id'], 'claim', {'revision': external['revision']})
        attached = self.svc.attach(self.leader, external['id'], claim['revision'], '报告.pdf', b'%PDF-1.7\nfixture')
        self.svc.action(self.leader, external['id'], 'submit', {'revision': attached['revision'], 'actual': self.date})
        self.svc.cloud = cloud
        self.svc.sync_one()
        with self.svc.connect(True) as db:
            db.execute('UPDATE tool_work_sync SET next_try=0')
        self.svc.sync_one()
        self.assertEqual(transport.uploads, 1)
        self.assertEqual(self.svc.detail(self.leader, external['id'])['sync'][0]['state'], 'done')

    def test_self_check_attachment_uses_existing_maintenance_field(self):
        class Transport:
            def __init__(self):self.uploads=0;self.fields=None
            def table_path(self,base,table):return f'{base}/{table}'
            def all(self,path):
                return [{'field_name':name,'type':kind,'property':props} for name,kind,props in [
                    (KEY,1,{}),(STATE,1,{}),(DETAIL,1,{}),('楼栋',3,{}),('维护工具',21,{'table_id':INVENTORY}),
                    ('计划维护日期',5,{}),('实际维护日期',5,{}),('工具检测是否合格',3,{}),('维护记录单',17,{})]]
            def request(self,method,path,**kwargs):
                if path.endswith('/upload_all'):
                    self.uploads+=1;return {'file_token':'fixture-file'}
                if path.endswith('/search'):return {'items':[],'has_more':False}
                self.fields=kwargs['json']['fields'];return {'record':{'record_id':'recFixture'}}
        transport=Transport();cloud=ToolCloud(transport)
        order=next(row for row in self.orders if row['kind']=='self')
        submitted=self.submit_self(order,'normal')
        with self.svc.connect() as db:
            row=db.execute('SELECT * FROM tool_work_orders WHERE id=?',(submitted['id'],)).fetchone()
        saved=[]
        cloud.write('maintenance',row,json.loads(row['asset']),{'file_token':'','remote_id':'',
            'record_name':'记录.xlsx','record_content':b'isolated'},self.svc.person_for,saved.append)
        self.assertEqual(transport.fields['维护记录单'],[{'file_token':'fixture-file'}])
        self.assertEqual(saved,['fixture-file'])
        self.assertEqual(transport.uploads,1)
