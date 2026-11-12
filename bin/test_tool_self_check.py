import datetime as dt
import hashlib
import io
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch
from zipfile import ZipFile

from fastapi import FastAPI
from openpyxl import load_workbook

sys.path.insert(0, str(Path(__file__).resolve().parent))

from bin.lan_bitable_template_portal.risk_inspection import MODULE, RiskError, RiskInspections
from bin.lan_bitable_template_portal.risk_inspection_routes import install_risk_inspection_routes, tool_management_enabled
from bin.lan_bitable_template_portal.tool_self_check import BUILDINGS, TEMPLATES, ToolSelfChecks, fill_template


def actor(scope='A', identity='leader', *, admin=False):
    return {'id': identity, 'open_id': 'ou_' + identity, 'name': '验收人员', 'home_scope': scope,
            'level': 'L0' if admin else 'L3', 'scope_rules': ['ALL'] if admin else [scope], 'modules': [MODULE]}


class ToolSelfCheckTest(TestCase):
    def setUp(self):
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.db = RiskInspections(Path(temporary.name) / 'portal.db', lambda _identity: {})
        self.service = ToolSelfChecks(self.db)
        self.admin = actor(admin=True)
        self.month = self.service.issue()
        self.templates = self.service.list(self.admin)['templates']

    def test_enabled_by_default_and_explicit_disable_still_works(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertTrue(tool_management_enabled())
            app = FastAPI()
            runtime = SimpleNamespace()
            install_risk_inspection_routes(app, SimpleNamespace(), runtime)
            self.assertTrue(any(route.path.startswith('/api/tool-self-check') for route in app.routes))
            self.assertTrue(callable(runtime.tool_management))
        with patch.dict(os.environ, {'CLIPFLOW_TOOL_SELF_CHECK_ENABLED': ''}):
            app = FastAPI()
            install_risk_inspection_routes(app, SimpleNamespace(), SimpleNamespace())
        self.assertFalse(any(route.path.startswith('/api/tool-self-check') for route in app.routes))

    def test_clone_template_preserves_original_and_checks_permissions(self):
        original = self.templates[0]
        checks = [dict(item) for item in original['schema']['checks']]
        checks[0]['name'] = '新的检查项目'
        body = {'version': original['version'], 'name': '独立新模板', 'checks': checks}
        created = self.service.clone_template(self.admin, original['id'], body)
        self.assertNotEqual(created['id'], original['id'])
        self.assertEqual(created['schema']['checks'][0]['name'], '新的检查项目')
        unchanged = next(item for item in self.service.list(self.admin)['templates'] if item['id'] == original['id'])
        self.assertEqual(unchanged['schema']['checks'], original['schema']['checks'])
        with self.assertRaises(RiskError):
            self.service.clone_template(actor(), original['id'], body)

    def test_scope_and_concurrent_claim(self):
        self.assertEqual([row['scope'] for row in self.service.list(actor())['orders']], ['A'])
        with self.assertRaisesRegex(RiskError, '本楼楼长'):
            self.service.detail(actor(), self.month, 'B')
        engineer = actor() | {'level': 'L4'}
        with self.assertRaisesRegex(RiskError, '有权限的管理员'):
            self.service.list(engineer)

        def claim(identity):
            try:
                return self.service.action(actor(identity=identity), self.month, 'A', 'claim', {'revision': 0})
            except RiskError as exc:
                return exc.status

        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes = list(pool.map(claim, ('first', 'second')))
        self.assertEqual(sum(isinstance(result, dict) for result in outcomes), 1)
        self.assertIn(409, outcomes)

    def test_month_boundary_and_recovery(self):
        with self.db.connect(True) as db:
            db.execute('DELETE FROM tool_check_orders')
        self.assertEqual(self.service.issue(dt.datetime(2026, 11, 30, 15, 59, tzinfo=dt.timezone.utc)), '2026-11')
        self.assertEqual(self.service.issue(dt.datetime(2026, 12, 31, 16, tzinfo=dt.timezone.utc)), '2027-01')
        self.service.issue(dt.datetime(2026, 12, 31, 16, tzinfo=dt.timezone.utc))
        with self.db.connect() as db:
            self.assertEqual([tuple(row) for row in db.execute(
                'SELECT month,COUNT(*) FROM tool_check_orders GROUP BY month ORDER BY month')],
                [(month, len(BUILDINGS)) for month in ('2026-11', '2026-12', '2027-01')])

    def test_template_version_and_archive_round_trip(self):
        legacy_ids = {hashlib.sha256(path.name.encode()).hexdigest()[:24] for path in TEMPLATES.glob('*.xlsx')}
        legacy = [template for template in self.templates if template['id'] in legacy_ids]
        self.assertEqual(len(legacy), 13)
        for scope in BUILDINGS:
            owner = actor(scope)
            claim = self.service.action(owner, self.month, scope, 'claim', {'revision': 0})
            self.assertEqual(claim, self.service.action(owner, self.month, scope, 'claim', {'revision': 0}))
            selected = legacy if scope == 'A' else legacy[1:2]
            records = [
                {'entry_id': f'{index:032x}', 'template_id': template['id'], 'version': 1, 'location': '=1+1',
                 'inspector': '验收楼长', 'checked_at': self.month + '-01T08:30', 'reviewer': '验收审核人',
                 'reviewed_at': self.month + '-01T09:30', 'notes': '补充说明',
                 'checks': ['否'] + ['是'] * (len(template['schema']['checks']) - 1)}
                for index, template in enumerate(selected)
            ]
            if scope == 'A':
                draft = self.service.action(owner, self.month, scope, 'draft', {'revision': claim['revision'], 'records': records})
                template = selected[0]
                with self.db.connect() as db:
                    original = self.service._template(db, template['id'], 1)['content']
                self.service.save_template(self.admin, template['name'], original, template['id'], 1)
                self.assertEqual(self.service.detail(owner, self.month, scope)['records'][0]['template']['version'], 1)
                claim = draft
                self.service.tick()
                with self.assertRaisesRegex(RiskError, '尚未全部提交'):
                    self.service.download(self.admin, self.month)
            payload = {'revision': claim['revision'], 'records': records}
            submitted = self.service.action(owner, self.month, scope, 'submit', payload)
            self.assertEqual(submitted, self.service.action(owner, self.month, scope, 'submit', payload))
            self.assertEqual(submitted['status'], 'submitted')

        self.service.tick()
        archive = self.service.download(self.admin, self.month)
        self.service.tick()
        self.assertEqual(archive, self.service.download(self.admin, self.month))
        with self.assertRaisesRegex(RiskError, '无权下载'):
            self.service.download(actor(), self.month)
        with ZipFile(io.BytesIO(archive)) as package:
            self.assertIsNone(package.testzip())
            manifest = json.loads(package.read('归档清单.json'))
            self.assertEqual([row['scope'] for row in manifest['orders']], list(BUILDINGS))
            for entry in manifest['orders'][0]['files']:
                self.assertEqual(entry['version'], 1)
                workbook = load_workbook(io.BytesIO(package.read(entry['file'])))
                try:
                    sheet = workbook.active
                    self.assertEqual((sheet['C3'].value, sheet['D8'].value, sheet['D8'].data_type), ('A楼', '=1+1', 's'))
                    self.assertEqual(sheet['I11'].value, '是[ ]  否[√]')
                    self.assertTrue(sheet.merged_cells.ranges)
                    self.assertEqual(len(sheet._images), 1)
                finally:
                    workbook.close()

    def test_bundled_folders_import_without_overwriting_existing_versions(self):
        paths = list((TEMPLATES / 'folder_imports').rglob('*.xlsx'))
        ids = {self.service.import_identity(path.stem, path.read_bytes()) for path in paths}
        added = [template for template in self.templates if template['id'] in ids]
        self.assertEqual(len(added), 15)
        self.assertEqual(sum(t['schema'].get('layout') == 'hvac' for t in added), 4)
        template = next(t for t in added if t['schema'].get('calibration'))
        with self.db.connect() as db:
            content = self.service._template(db, template['id'], 1)['content']
        exported = fill_template(content, template['schema'], {'inspector': '测试人员', 'location': '测试位置',
            'checked_at': self.month + '-01T09:00', 'notes': '', 'reviewer': '', 'reviewed_at': '',
            'checks': ['是'] * len(template['schema']['checks']),
            'template_values': {'C3': '温湿度仪', 'E20': '45', 'G20': '46', 'I20': '-1'}}, 'A')
        with io.BytesIO(exported) as stream:
            book = load_workbook(stream)
            try:
                self.assertEqual(book['封面']['A1'].value, '自检记录单')
                sheet = book[template['schema']['sheet']]
                self.assertEqual([sheet[cell].value for cell in ('C3', 'C5', 'E20', 'G20', 'I20')],
                                 ['温湿度仪', 'A楼', '45', '46', '-1'])
            finally:
                book.close()
        self.service.delete_template(self.admin, template['id'], {'version': 1})
        reopened = ToolSelfChecks(self.db)
        self.assertNotIn(template['id'], [row['id'] for row in reopened.list(self.admin)['templates']])

    def test_one_bad_bundled_template_preserves_library(self):
        with TemporaryDirectory() as temporary:
            imports = Path(temporary) / 'folder_imports'
            imports.mkdir()
            (imports / '损坏.xlsx').write_bytes(b'invalid workbook')
            source = next(TEMPLATES.glob('*.xlsx'))
            name, content = '新增有效模板', source.read_bytes()
            (imports / (name + '.xlsx')).write_bytes(content)
            with self.assertLogs(level='WARNING'):
                reopened = ToolSelfChecks(self.db, Path(temporary))
            self.assertEqual(len(reopened.list(self.admin)['templates']), len(self.templates) + 1)
