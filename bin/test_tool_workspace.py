import base64
import copy
import datetime as dt
import io
import tempfile
import unittest
from pathlib import Path
from threading import Event
from types import SimpleNamespace
from unittest.mock import patch
from zipfile import ZipFile

from PIL import Image, ImageDraw
from openpyxl import load_workbook

from bin.test_tool_self_check import actor
from bin.lan_bitable_template_portal.risk_inspection import RiskError, RiskInspections
from bin.lan_bitable_template_portal.tool_management import ToolManagement
from bin.lan_bitable_template_portal.tool_self_check import TEMPLATES, TZ


class WorkspaceCloud:
    def __init__(self):
        self.content = next(TEMPLATES.glob('*.xlsx')).read_bytes()
        self.assets = [dict(id=f'recTool{i}', name=f'工具{i}', scope='A', model='测试型号', brand='测试品牌',
                            location='工具间', quantity=1, retired=False, template_id='',
                            self_cycle='每月' if i < 2 else '', external_cycle='一年' if i >= 2 else '')
                       for i in range(4)]
        self.bindings, self.writes, self.tokens = [], {}, []
        self.failures = set()

    def inventory(self):
        return copy.deepcopy(self.assets), {'A': ['ou_leader'], 'B': ['ou_other']}

    def templates(self):
        return [dict(id='recTemplate', name='记录单', file_token='templateToken', digest='fixture')]

    def download_template(self, template):
        return self.content

    def bind_template(self, asset_id, template_id, expected):
        row = next(row for row in self.assets if row['id'] == asset_id)
        if row['template_id'] == template_id:
            return template_id
        if row['template_id'] != expected:
            raise RiskError('关联冲突', 409)
        row['template_id'] = template_id
        self.bindings.append(asset_id)
        return template_id

    def write_workspace_item(self, session_id, item, name, report, actual, **kwargs):
        key = (session_id, item['asset_id'])
        token = kwargs['file_token']
        if not token:
            token = f'upload{len(self.tokens)}'
            self.tokens.append((token, name, report))
            kwargs['save_file'](token)
        if not kwargs['attempted_at']:
            kwargs['save_attempt'](1_790_000_000)
        if item['asset_id'] in self.failures:
            raise RiskError('临时网络失败', 503)
        self.writes.setdefault(key, (copy.deepcopy(item), name, report, actual, token))
        return {'remote_id': 'recArchive' + item['asset_id'], 'file_token': token}


class WorkspaceTests(unittest.TestCase):
    def setUp(self):
        folder = self.enterContext(tempfile.TemporaryDirectory())
        self.path = Path(folder) / 'tools.db'
        self.cloud = WorkspaceCloud()
        self.admin = actor(identity='admin', admin=True)
        self.service = self.restart()
        self.service.refresh()

    def restart(self):
        return ToolManagement(RiskInspections(self.path, lambda _: self.admin), self.cloud)

    def open(self, kind='self', operation='open-one'):
        ids = ['recTool0', 'recTool1'] if kind == 'self' else ['recTool2', 'recTool3']
        return self.service.workspace_open(self.admin, dict(
            kind=kind, asset_ids=ids, template_id='recTemplate' if kind == 'self' else '', operation_id=operation))

    def complete(self, session):
        picture = Image.new('RGBA', (180, 60), 'white')
        ImageDraw.Draw(picture).line((10, 40, 50, 10, 140, 45), fill='black', width=3)
        image = io.BytesIO()
        picture.save(image, format='PNG')
        for item in session['items']:
            item.update(reviewer='验收人员', reviewed_at=dt.datetime.now(TZ).strftime('%Y-%m-%dT%H:%M'),
                        signature='data:image/png;base64,' + base64.b64encode(image.getvalue()).decode(), qualified=True)
            item['checks'] = {sheet['name'] + '!' + cell['address']: '是'
                              for sheet in item['template']['sheets'] for row in sheet['rows']
                              for cell in row['cells'] if cell.get('check')}
        return self.service.workspace_save(self.admin, session['id'], {
            'revision': session['revision'], 'items': session['items']})

    def test_template_relation_and_open_retry_are_scoped_and_persistent(self):
        first = self.open()
        self.assertEqual(self.open()['id'], first['id'])
        self.assertEqual(self.cloud.bindings, ['recTool0', 'recTool1'])
        self.service = self.restart()
        board = self.service.workspace_list(self.admin)
        self.assertEqual([r['template_id'] for r in board['assets'][:2]], ['recTemplate'] * 2)
        second = self.service.workspace_open(self.admin, dict(kind='self', asset_ids=['recTool0'],
                                                              template_id='', operation_id='next-one'))
        self.assertEqual(second['items'][0]['template_id'], 'recTemplate')
        with self.assertRaises(RiskError):
            self.service.workspace_open(self.admin, dict(kind='external', asset_ids=['recTool0'], operation_id='bad'))
        with self.assertRaises(RiskError):
            self.service.workspace_get(actor(scope='B', identity='other'), first['id'])

    def test_individual_signed_files_and_failed_item_do_not_block_others(self):
        session = self.complete(self.open())
        body = {'revision': session['revision']}
        submitted = self.service.workspace_submit(self.admin, session['id'], body)
        self.assertEqual(submitted['status'], 'pending')
        self.assertEqual(self.service.workspace_submit(self.admin, session['id'], body)['status'], 'pending')
        self.cloud.failures.add('recTool0')
        self.service.workspace_sync_one()
        self.service.workspace_sync_one()
        self.assertEqual(len(self.cloud.writes), 1)
        self.service = self.restart()
        self.cloud.failures.clear()
        with self.service.connect(True) as db:
            db.execute('UPDATE tool_workspace_items SET next_try=0')
        self.service.workspace_sync_one()
        self.assertEqual(self.service.workspace_get(self.admin, session['id'])['status'], 'archived')
        self.assertFalse(self.service.workspace_sync_one())
        self.assertEqual(len(self.cloud.writes), 2)
        self.assertEqual(len(self.cloud.tokens), 2)
        original = load_workbook(io.BytesIO(self.cloud.content))
        image_count = sum(len(sheet._images) for sheet in original)
        original.close()
        for _, name, data, _, _ in self.cloud.writes.values():
            self.assertTrue(name.endswith('.xlsx'))
            book = load_workbook(io.BytesIO(data))
            self.assertEqual(sum(len(sheet._images) for sheet in book), image_count + 1)
            book.close()
        with ZipFile(io.BytesIO(self.service.workspace_download(self.admin, session['id']))) as package:
            self.assertEqual(len(package.namelist()), 2)

    def test_external_one_upload_attaches_same_report_to_every_tool(self):
        session = self.open('external')
        report = b'%PDF-1.7\nvalidation report'
        session = self.service.workspace_report(self.admin, session['id'], session['revision'], '检验报告.pdf', report)
        self.service.workspace_submit(self.admin, session['id'], {'revision': session['revision']})
        self.service.workspace_sync_one()
        self.service.workspace_sync_one()
        self.assertEqual(len(self.cloud.tokens), 1)
        self.assertEqual(len(self.cloud.writes), 2)
        self.assertTrue(all(data == report for _, _, data, _, _ in self.cloud.writes.values()))

    def test_interrupted_mapping_is_visible_and_can_resume_after_restart(self):
        real_bind = self.cloud.bind_template
        def fail_second(asset, template, old):
            if asset == 'recTool1':
                raise RiskError('临时断网', 503)
            return real_bind(asset, template, old)
        with patch.object(self.cloud, 'bind_template', side_effect=fail_second):
            with self.assertRaises(RiskError):
                self.open()
        self.service = self.restart()
        pending = self.service.workspace_list(self.admin)['sessions'][0]
        self.assertEqual(pending['status'], 'opening')
        restored = self.service.workspace_resume(self.admin, pending['id'])
        self.assertEqual(restored['status'], 'draft')
        self.assertEqual(self.cloud.bindings, ['recTool0', 'recTool1'])
        self.assertEqual(self.service.workspace_resume(self.admin, pending['id'])['id'], pending['id'])

    def test_stale_draft_incomplete_checks_and_moved_scope_are_rejected(self):
        original = self.open()
        complete = self.complete(original)
        with self.assertRaises(RiskError):
            self.service.workspace_save(self.admin, original['id'], {'revision': 0, 'items': []})
        item = complete['items'][0]
        item['checks'].pop(next(iter(item['checks'])))
        incomplete = self.service.workspace_save(self.admin, original['id'], {
            'revision': complete['revision'], 'items': [item]})
        with self.assertRaisesRegex(RiskError, '自检项目'):
            self.service.workspace_submit(self.admin, original['id'], {'revision': incomplete['revision']})
        complete = self.complete(incomplete)
        self.cloud.assets[0]['scope'] = 'B'
        self.service.refresh()
        with self.assertRaisesRegex(RiskError, '归属楼栋'):
            self.service.workspace_submit(self.admin, original['id'], {'revision': complete['revision']})

    def test_routes_enforce_actor_and_preserve_report_bytes(self):
        from fastapi import FastAPI
        from fastapi.responses import JSONResponse
        from fastapi.testclient import TestClient
        from bin.lan_bitable_template_portal.tool_self_check_routes import install_tool_self_check_routes
        class Controller:
            async def _read_json_request(self, request, **kwargs):
                return await request.json()
        def read_actor(request):
            if request.headers.get('actor') != 'admin':
                raise RiskError('请登录', 401)
            return self.admin
        def same_origin(request):
            if request.headers.get('origin') not in (None, 'http://testserver'):
                raise RiskError('来源不允许', 403)
        def response(data=None, error=None):
            return JSONResponse({'ok': error is None, 'data': data, 'error': str(error) if error else ''},
                                status_code=error.status if error else 200)
        app = FastAPI()
        install_tool_self_check_routes(app, Controller(), lambda: self.service, read_actor, same_origin, response)
        with TestClient(app) as client:
            base = '/api/tool-self-check/workspace'
            self.assertEqual(client.get(base).status_code, 401)
            headers = {'actor': 'admin'}
            self.assertEqual(client.get(base, headers=headers).status_code, 200)
            self.assertEqual(client.post(base + '/refresh', headers={**headers, 'origin': 'https://wrong.invalid'}).status_code, 403)
            opened = client.post(base + '/open', headers=headers, json={
                'kind': 'external', 'asset_ids': ['recTool2'], 'operation_id': 'api-report',
            }).json()['data']
            saved = client.post(base + '/' + opened['id'] + '/report?name=report.pdf&revision=0',
                                headers=headers, content=b'%PDF-1.7 report').json()
            self.assertTrue(saved['ok'], saved)
            self.assertEqual(saved['data']['report_name'], 'report.pdf')

    def test_background_loop_runs_workspace_when_monthly_queue_is_empty(self):
        from bin.lan_bitable_template_portal.tool_management_cloud import run_tool_jobs
        stop = Event()
        calls = []
        def archive():
            calls.append('workspace')
            stop.set()
            return True
        service = SimpleNamespace(refresh=lambda: None, schedule=lambda: None, archive_ready=lambda: None,
                                  sync_one=lambda: False, workspace_sync_one=archive)
        run_tool_jobs(service, stop)
        self.assertEqual(calls, ['workspace'])

