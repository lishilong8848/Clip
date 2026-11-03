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
