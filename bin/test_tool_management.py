import json
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

    def inventory(self):
        return self.assets, {'A': ['ou_leader']}

    def write(self, dest, order, asset, job, person_for, save_file):
        self.writes.append((dest, order['outcome'], job['remote_id']))
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
        with self.assertRaises(RiskError):
            self.svc.action(self.leader, order['id'], 'submit', body | {'outcome': 'normal' if outcome != 'normal' else 'repair'})
        return result

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
