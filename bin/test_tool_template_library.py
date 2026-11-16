"""Tool template publication and pinning against an in-memory Feishu transport."""

import hashlib
import io
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch
from urllib.parse import urlsplit
from zipfile import ZipFile

from bin.test_tool_self_check import actor
from bin.test_tool_workspace_cloud import FakeFeishu
from bin.lan_bitable_template_portal.risk_inspection import RiskError, RiskInspections
from bin.lan_bitable_template_portal.tool_management import ToolManagement
from bin.lan_bitable_template_portal.tool_management_cloud import (
    INVENTORY, TEMPLATE_FILE, TEMPLATE_HASH, TEMPLATE_LINK, TEMPLATE_NAME, TEMPLATE_TABLE,
    WORKSPACE_CREATE_RETRY_SECONDS, ToolCloud,
)
from bin.lan_bitable_template_portal.tool_self_check import TEMPLATES, dumps
from bin.lan_bitable_template_portal.tool_template_library import ToolTemplateLibrary


class TemplateFeishu(FakeFeishu):
    def __init__(self, content):
        super().__init__()
        self.media = {'fixture-media': content}
        self.hide_new = False
        self.fail_list = False
        self.upload_hook = None
        row = self.records[(TEMPLATE_TABLE, 'recTemplate')]
        row[TEMPLATE_FILE][0]['size'] = len(content)
        row[TEMPLATE_HASH] = hashlib.sha256(content).hexdigest()
        first = self.records[(INVENTORY, 'recAsset')]
        first['楼栋'] = 'A楼'
        self.records[(INVENTORY, 'recAssetB')] = first | {'楼栋': 'B楼'}

    def all(self, path, **params):
        result = super().all(path, **params)
        if path.endswith(f'{TEMPLATE_TABLE}/fields'):
            return result + [{'field_name': field, 'type': 1} for field in (TEMPLATE_NAME, TEMPLATE_HASH)]
        if path.endswith(f'{TEMPLATE_TABLE}/records'):
            if self.fail_list:
                raise RiskError('模拟目录不可用', 503)
            if self.hide_new:
                return [row for row in result if not row['record_id'].startswith('recLedger')]
        return result

    def request(self, method, path, **kwargs):
        if path == 'drive/v1/medias/upload_all':
            token = f'template-file-{len(self.media)}'
            part = kwargs['files']['file']
            self.uploads.append(part)
            self.media[token] = part[1]
            if self.upload_hook:
                self.upload_hook()
            return {'file_token': token}
        return super().request(method, path, **kwargs)

    def download_to_file(self, url, output, *, headers, max_bytes):
        token = urlsplit(url).path.split('/')[-2]
        output.write_bytes(self.media[token])


class ToolTemplateLibraryTests(TestCase):
    def setUp(self):
        temp = TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.content = next(TEMPLATES.glob('*.xlsx')).read_bytes()
        self.transport = TemplateFeishu(self.content)
        self.cloud = ToolCloud(self.transport)
        inspection = RiskInspections(Path(temp.name) / 'tool.db', lambda _: {})
        cls = ToolManagement if issubclass(ToolManagement, ToolTemplateLibrary) else type(
            'LibraryService', (ToolTemplateLibrary, ToolManagement), {})
        self.svc = cls(inspection, self.cloud)
        self.svc.refresh()
        self.admin = actor(admin=True)
        self.leader = actor() | {'category': 'supervisor'}

    def test_version_and_delete_preserve_links_saved_checks_and_archive(self):
        before, = self.svc.template_library(self.admin)['templates']
        identity = before['id']
        entry = {'entry_id': '1' * 32, 'template_id': identity, 'version': 1,
                 'checks': ['是'] * len(before['schema']['checks'])}
        with self.svc.connect() as db:
            pinned = self.svc._records(db, [entry], [], submit=False)
        raw, = self.svc.selectable_feishu_templates(self.leader)
        key, _, _ = self.svc._workspace_snapshot(raw)

        replacement = next(path.read_bytes() for path in sorted(TEMPLATES.glob('*.xlsx'))
                           if path.read_bytes() != self.content)
        after = self.svc.save_library_template(self.admin, '改版工作簿', replacement, identity, 1)
        self.assertEqual((after['id'], after['version']), (identity, 2))
        self.assertEqual(self.transport.records[(INVENTORY, 'recAsset')][TEMPLATE_LINK], [{'text': identity}])
        with self.assertRaisesRegex(RiskError, '已更新'):
            self.svc.library_template_availability(self.admin, identity, {'active': False, 'version': 1})
        self.svc.library_template_availability(self.admin, identity, {'active': False, 'version': 2})
        self.assertEqual(self.svc.selectable_feishu_templates(self.leader), [])
        self.svc.library_template_availability(self.admin, identity, {'active': True, 'version': 2})
        self.assertEqual(len(self.svc.selectable_feishu_templates(self.leader)), 1)
        with self.assertRaisesRegex(RiskError, '仍有工具关联'):
            self.svc.delete_library_template(self.admin, identity, {'version': 2})
        target = self.svc.save_library_template(self.admin, '替换模板', self.content)['id']
        result = self.svc.rebind_library_template(self.admin, identity, {'version': 2, 'target_id': target})
        self.assertEqual((result['updated'], result['failed']), (2, []))
        self.assertEqual(self.transport.records[(INVENTORY, 'recAsset')][TEMPLATE_LINK], target)
        self.svc.delete_library_template(self.admin, identity, {'version': 2})
        self.assertTrue((TEMPLATE_TABLE, identity) in self.transport.records)
        with self.svc.connect() as db:
            self.assertEqual(self.svc._records(db, [entry], pinned, submit=False), pinned)
            self.assertEqual([row['id'] for row in self.svc.list_templates(db, True)], [target])
            self.assertEqual(db.execute('SELECT content FROM tool_workspace_templates WHERE cache_key=?', (key,))
                             .fetchone()[0], self.content)
            with self.assertRaises(RiskError):
                self.svc._records(db, [entry | {'entry_id': '2' * 32, 'version': 2}], [], submit=False)
        order = {'scope': 'A', 'owner_name': '测试人', 'submitted_at': None, 'records': dumps(pinned)}
        with ZipFile(io.BytesIO(self.svc.package('2026-11', [order]))) as archive:
            version, = json.loads(archive.read('归档清单.json'))['orders'][0]['files']
            self.assertEqual((version['template_id'], version['version'], version['template_sha256']),
                             (identity, 1, hashlib.sha256(self.content).hexdigest()))
        with patch.object(self.transport, 'fail_list', True):
            self.assertTrue(self.svc.template_library(self.admin)['warning'])
            self.assertEqual(self.svc.filter_selectable_feishu_templates([raw]), [])

    def test_legacy_clone_publishes_and_inventory_counts_follow_scope(self):
        catalog = self.svc.template_library(self.admin)
        self.assertEqual(catalog['templates'][0]['linked_tools_count'], 2)
        scoped_admin = self.admin | {'scope_rules': ['A']}
        self.assertEqual(self.svc.template_library(scoped_admin)['templates'][0]['linked_tools_count'], 1)
        legacy = catalog['legacy_templates'][0]
        self.assertEqual(self.svc.template_library(self.leader)['legacy_templates'], [])
        with self.assertRaises(RiskError) as unauthorized:
            self.svc.clone_library_template(self.leader, legacy['id'], {'name': '副本', 'version': legacy['version']})
        self.assertEqual(unauthorized.exception.status, 403)
        published = self.svc.clone_library_template(self.admin, legacy['id'], {'name': legacy['name'], 'version': legacy['version']})
        self.assertTrue(published['id'].startswith('rec'))
        self.assertNotEqual(published['id'], legacy['id'])
        with self.svc.connect() as db:
            old = self.svc._template(db, legacy['id'], legacy['version'])
            new = self.svc._template(db, published['id'], 1)
            self.assertEqual(old['content'], new['content'])
            with self.assertRaises(RiskError):
                self.svc._select_template(db, legacy['id'], legacy['version'])

    def test_lost_create_is_reconciled_even_after_retry_window(self):
        current, = self.svc.template_library(self.admin)['templates']
        request = {'name': '编辑后的副本', 'version': current['version'], 'checks': current['schema']['checks'],
                   'description': '副本说明'}

        def lose_response():
            self.transport.hide_new = True
            raise TimeoutError('fixture only')

        self.transport.created_hook = lose_response
        first_attempt = 1_790_000_000
        with patch('bin.lan_bitable_template_portal.tool_management_cloud.time.time', return_value=first_attempt):
            with self.assertRaises(TimeoutError):
                self.svc.clone_library_template(self.admin, current['id'], request)
        self.transport.created_hook = None
        with patch('bin.lan_bitable_template_portal.tool_management_cloud.time.time',
                   return_value=first_attempt + WORKSPACE_CREATE_RETRY_SECONDS + 1):
            with self.assertRaisesRegex(RiskError, '不会重复创建'):
                self.svc.clone_library_template(self.admin, current['id'], request)
            self.transport.hide_new = False
            recovered = self.svc.clone_library_template(self.admin, current['id'], request)
        self.assertEqual((len(self.transport.creates), len(self.transport.uploads)), (1, 1))
        self.assertEqual(recovered['schema']['description'], '副本说明')
        self.assertEqual(self.svc.clone_library_template(self.admin, current['id'], request), recovered)

    def test_remote_edit_during_attachment_upload_is_not_overwritten(self):
        current, = self.svc.template_library(self.admin)['templates']
        def rename():
            self.transport.records[(TEMPLATE_TABLE, current['id'])][TEMPLATE_NAME] = '人工修改'

        self.transport.upload_hook = rename
        with self.assertRaisesRegex(RiskError, '飞书模板已变化'):
            self.svc.save_library_template(self.admin, '系统改版', self.content, current['id'], current['version'])
        self.assertEqual(self.transport.updates, [])
        renamed, = self.svc.template_library(self.admin)['templates']
        self.assertEqual((renamed['name'], renamed['version']), ('人工修改', 2))
