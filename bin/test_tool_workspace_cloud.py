"""Exercise the tool workspace cloud contracts without a live Feishu transport."""

import hashlib
import json
from copy import deepcopy
from threading import RLock
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from bin.lan_bitable_template_portal.risk_inspection import RiskError
from bin.lan_bitable_template_portal.tool_management_cloud import (
    BASE, DETAIL, INVENTORY, KEY, ORGANIZATION, STATE, TABLES, TEMPLATE_FILE, TEMPLATE_HASH,
    TEMPLATE_LINK, TEMPLATE_NAME, TEMPLATE_TABLE, WORKSPACE_CREATE_RETRY_SECONDS, ToolCloud,
)
from upload_event_module.services.http_client import FeishuHTTPError


WORKBOOK = b'fixture workbook'
DAY = '2026-11-13'
ASSET = {'asset_id': 'recAsset', 'scope': 'D', 'kind': 'self', 'due': DAY, 'qualified': False}


class FakeFeishu:
    def __init__(self):
        self.lock = RLock()
        self.http = self
        self.records = {}
        self.uploads = []
        self.creates = []
        self.updates = []
        self.downloads = []
        self.created_hook = None
        self.updated_hook = None
        self.read_hook = None
        self.hide_search = False
        self.records[(INVENTORY, 'recAsset')] = {
            '楼栋': '南通D楼', '工具名称': '测试仪', '一级分类': 'B工具类',
            '品牌': {'name': '测试品牌'}, TEMPLATE_LINK: [{'text': 'recTemplate'}],
        }
        self.records[(TEMPLATE_TABLE, 'recTemplate')] = {
            TEMPLATE_NAME: '测试模板', TEMPLATE_HASH: hashlib.sha256(WORKBOOK).hexdigest(),
            TEMPLATE_FILE: [{'file_token': 'fixture-media', 'name': '模板.xlsx', 'size': len(WORKBOOK),
                             'url': 'https://unrelated.invalid/'}],
        }
        self.records[(TEMPLATE_TABLE, 'recPlaceholder')] = {TEMPLATE_NAME: '', TEMPLATE_FILE: []}

    @staticmethod
    def table_path(base, table):
        assert base == BASE
        return f'bitable/v1/apps/{base}/tables/{table}'

    @staticmethod
    def headers():
        return {'Authorization': 'Bearer fixture-token'}

    def rows(self, table):
        return [{'record_id': rid, 'fields': deepcopy(fields)}
                for (table_id, rid), fields in self.records.items() if table_id == table]

    def all(self, path, **params):
        table, endpoint = path.split('/')[-2:]
        if endpoint == 'records':
            return self.rows(table)
        if table == TEMPLATE_TABLE:
            return [{'field_name': TEMPLATE_FILE, 'field_id': 'fldTemplate', 'type': 17}]
        common = [(KEY, 1, {}), (STATE, 1, {}), (DETAIL, 1, {}), ('楼栋', 3, {})]
        if table == TABLES['maintenance']:
            fields = common + [
                ('维护工具', 21, {'table_id': INVENTORY}), ('计划维护日期', 5, {}),
                ('实际维护日期', 5, {}), ('维护记录单', 17, {}),
                ('工具检测是否合格', 3, {'options': [{'name': '合格'}, {'name': '工具不合格'}]}),
            ]
        else:
            fields = common + [
                ('送检工具', 18, {'table_id': INVENTORY}), ('计划检测日期', 5, {}),
                ('实际检测日期', 5, {}), ('送检报告', 17, {}),
            ]
        return [{'field_name': name, 'type': kind, 'property': props} for name, kind, props in fields]

    def request(self, method, path, **kwargs):
        if path == 'drive/v1/medias/upload_all':
            self.uploads.append(kwargs['files']['file'])
            return {'file_token': 'fixture-report'}
        table, endpoint = path.split('/')[-2:]
        if endpoint == 'search':
            table = path.split('/')[-3]
            if table == INVENTORY:
                assert '品牌' in kwargs['json']['field_names'] and TEMPLATE_LINK in kwargs['json']['field_names']
                return {'items': self.rows(table), 'has_more': False}
            key = kwargs['json']['filter']['conditions'][0]['value'][0]
            rows = [row for row in self.rows(table) if row['fields'].get(KEY) == key]
            return {'items': [] if self.hide_search else rows, 'has_more': False}
        if method == 'GET' and table == 'records':
            table = path.split('/')[-3]
            if self.read_hook:
                self.read_hook(table, endpoint)
            return {'record': {'record_id': endpoint, 'fields': deepcopy(self.records[(table, endpoint)])}}
        if method == 'PUT' and table == 'records':
            table = path.split('/')[-3]
            fields = kwargs['json']['fields']
            self.updates.append((table, endpoint, fields))
            self.records[(table, endpoint)].update(fields)
            if self.updated_hook:
                self.updated_hook()
            return {}
        if method == 'POST' and endpoint == 'records':
            fields = deepcopy(kwargs['json']['fields'])
            self.creates.append((table, fields, kwargs['params']))
            number = sum(table_id == table for table_id, _ in self.records) + 1
            rid = 'recLedger' if number == 1 else f'recLedger{number}'
            self.records[(table, rid)] = fields
            if self.created_hook:
                self.created_hook()
            return {'record': {'record_id': rid}}
        raise AssertionError((method, path))

    def download_to_file(self, url, output, *, headers, max_bytes):
        self.downloads.append((url, headers, max_bytes))
        output.write_bytes(WORKBOOK)


class ToolWorkspaceCloudTests(unittest.TestCase):
    def setUp(self):
        self.transport = FakeFeishu()
        self.tool = ToolCloud(self.transport)

    def test_monthly_linked_write_rechecks_returned_id_when_search_is_delayed(self):
        order = {'id': 'order1', 'outcome': 'normal', 'due': DAY, 'actual': DAY,
                 'reason': '', 'resolution': '', 'scrap_reason': ''}
        asset = {'id': 'recAsset', 'scope': 'D'}
        job = {'verify_write': True, 'remote_id': '', 'file_token': '', 'attempted_at': 0,
               'record_name': '记录.xlsx', 'record_content': WORKBOOK,
               'save_attempt': lambda value: job.update(attempted_at=value),
               'save_candidate': lambda value: job.update(candidate_id=value)}
        def fail_read(*_):
            raise RiskError('写入后暂时无法回读', 503)
        self.transport.read_hook = fail_read
        save_file = lambda value: job.update(file_token=value)
        with self.assertRaisesRegex(RiskError, '写入后暂时无法回读'):
            self.tool.write('maintenance', order, asset, job, lambda _: {}, save_file)
        self.assertEqual(job['remote_id'], '')
        self.assertEqual(job['candidate_id'], 'recLedger')
        self.transport.read_hook = None
        self.transport.hide_search = True
        self.assertEqual(self.tool.write('maintenance', order, asset, job, lambda _: {}, save_file), 'recLedger')
        self.assertEqual(len(self.transport.creates), 1)
        self.assertEqual(len(self.transport.uploads), 1)

    def test_inventory_and_template_catalog_omit_invalid_links_and_placeholders(self):
        self.transport.records[(INVENTORY, 'recBadLink')] = {
            '楼栋': 'D楼', '工具名称': '旧工具', '一级分类': 'B工具类', TEMPLATE_LINK: 'recOne, recTwo',
        }
        rows, leaders = self.tool.inventory()
        self.assertEqual((rows[0]['brand'], rows[0]['template_id'], rows[1]['template_id']),
                         ('测试品牌', 'recTemplate', ''))
        self.assertEqual(leaders, {})
        row, = self.tool.templates()
        self.assertEqual(row, {'id': 'recTemplate', 'name': '测试模板', 'file_token': 'fixture-media',
                               'filename': '模板.xlsx', 'size': len(WORKBOOK),
                               'digest': hashlib.sha256(WORKBOOK).hexdigest()})
        self.assertEqual(self.tool.download_template(row), WORKBOOK)
        url, headers, cap = self.transport.downloads[0]
        parsed = urlsplit(url)
        self.assertEqual(parsed.netloc, 'open.feishu.cn')
        self.assertTrue(parsed.path.endswith('/medias/fixture-media/download'))
        self.assertEqual(json.loads(parse_qs(parsed.query)['extra'][0])['bitablePerm'],
                         {'tableId': TEMPLATE_TABLE, 'attachments': {'fldTemplate': {'recTemplate': ['fixture-media']}}})
        self.assertEqual(headers, self.transport.headers())
        self.assertEqual(cap, 2 * 1024 * 1024)

    def test_download_detects_changed_cache_digest_size_and_permission_failure(self):
        row, = self.tool.templates()
        with self.assertRaises(RiskError) as changed:
            self.tool.download_template(row | {'file_token': 'stale-token'})
        self.assertEqual(changed.exception.status, 409)
        with patch.object(self.transport, 'download_to_file',
                          side_effect=FeishuHTTPError('unsafe detail', category='permission')):
            with self.assertRaisesRegex(RiskError, '当前 Qt 飞书应用') as forbidden:
                self.tool.download_template(row)
        self.assertNotIn('unsafe detail', str(forbidden.exception))
        fields = self.transport.records[(TEMPLATE_TABLE, 'recTemplate')]
        fields[TEMPLATE_HASH] = '0' * 64
        with self.assertRaisesRegex(RiskError, '文件摘要不符'):
            self.tool.download_template(self.tool.templates()[0])
        fields[TEMPLATE_FILE][0]['size'] = 2 * 1024 * 1024 + 1
        with self.assertRaises(RiskError) as large:
            self.tool.download_template(self.tool.templates()[0])
        self.assertEqual(large.exception.status, 413)

    def test_bind_compare_and_verify_with_lost_receipt(self):
        fields = self.transport.records[(INVENTORY, 'recAsset')]
        fields[TEMPLATE_LINK] = ''
        self.transport.updated_hook = lambda: (_ for _ in ()).throw(TimeoutError())
        with self.assertRaises(TimeoutError):
            self.tool.bind_template('recAsset', 'recTemplate', '')
        self.assertEqual(self.tool.bind_template('recAsset', 'recTemplate', ''), 'recTemplate')
        self.assertEqual(self.transport.updates, [(INVENTORY, 'recAsset', {TEMPLATE_LINK: 'recTemplate'})])
        for raw in ('人工写入的旧关联', 'recOther', [{'text': 'recA'}, {'text': 'recB'}]):
            fields[TEMPLATE_LINK] = raw
            with self.subTest(raw=raw), self.assertRaises(RiskError) as conflict:
                self.tool.bind_template('recAsset', 'recTemplate', '')
            self.assertEqual(conflict.exception.status, 409)
            self.assertEqual(fields[TEMPLATE_LINK], raw)
        self.assertEqual(len(self.transport.updates), 1)

    def test_binding_conflict_during_template_validation(self):
        fields = self.transport.records[(INVENTORY, 'recAsset')]
        fields[TEMPLATE_LINK] = ''

        def edit_during_read(table, rid):
            if table == TEMPLATE_TABLE:
                fields[TEMPLATE_LINK] = 'recConcurrent'

        self.transport.read_hook = edit_during_read
        with self.assertRaises(RiskError) as conflict:
            self.tool.bind_template('recAsset', 'recTemplate', '')
        self.assertEqual(conflict.exception.status, 409)
        self.assertEqual(self.transport.updates, [])

    def test_writes_one_verified_ledger_row_per_tool_with_correct_report_and_result(self):
        for kind in ('self', 'external'):
            receipt = {}
            item = ASSET.copy()
            if kind == 'external':
                item['kind'] = kind
                item.pop('qualified')

            def save_attempt(value):
                self.assertEqual(receipt['file_token'], 'fixture-report')
                receipt['attempted_at'] = value

            with patch('bin.lan_bitable_template_portal.tool_management_cloud.time.time', return_value=1000):
                result = self.tool.write_workspace_item(
                    'fixture', item, '报告.xlsx', WORKBOOK, DAY,
                    save_file=lambda token: receipt.update(file_token=token), save_attempt=save_attempt,
                )
            self.assertEqual(result, {'remote_id': 'recLedger', 'file_token': 'fixture-report'})
            self.assertEqual(receipt['attempted_at'], 1000)
            table, fields, params = self.transport.creates[-1]
            self.assertEqual(fields[KEY], 'workspace:fixture:recAsset')
            self.assertEqual(len(params['client_token']), 36)
            if kind == 'self':
                self.assertEqual(table, TABLES['maintenance'])
                self.assertEqual(fields['工具检测是否合格'], '工具不合格')
                self.assertEqual(fields['维护记录单'], [{'file_token': 'fixture-report'}])
            else:
                self.assertEqual(table, TABLES['inspection'])
                self.assertEqual(fields[STATE], '已提交')
                self.assertNotIn('工具检测是否合格', fields)
                self.assertEqual(fields['送检报告'], [{'file_token': 'fixture-report'}])
        self.assertEqual(len(self.transport.creates), 2)
        self.assertEqual(len(self.transport.uploads), 2)

    def test_timeout_persists_first_attempt_and_reconciles_even_after_window(self):
        receipt = {}

        def timeout():
            self.assertEqual(receipt, {'attempted_at': 1000, 'file_token': 'fixture-report'})
            raise TimeoutError

        self.transport.created_hook = timeout
        with patch('bin.lan_bitable_template_portal.tool_management_cloud.time.time', return_value=1000):
            with self.assertRaises(TimeoutError):
                self.tool.write_workspace_item(
                    'fixture', ASSET, '报告.xlsx', WORKBOOK, DAY,
                    save_file=lambda value: receipt.update(file_token=value),
                    save_attempt=lambda value: receipt.update(attempted_at=value),
                )
        self.transport.hide_search = True
        later = 1000 + WORKSPACE_CREATE_RETRY_SECONDS
        with patch('bin.lan_bitable_template_portal.tool_management_cloud.time.time', return_value=later):
            restarted = ToolCloud(self.transport)
            with self.assertRaisesRegex(RiskError, '新增结果仍待核验'):
                restarted.write_workspace_item('fixture', ASSET, '报告.xlsx', WORKBOOK, DAY, **receipt)
            self.transport.hide_search = False
            result = restarted.write_workspace_item('fixture', ASSET, '报告.xlsx', WORKBOOK, DAY, **receipt)
        self.assertEqual(result['remote_id'], 'recLedger')
        self.assertEqual(len(self.transport.creates), 1)
        self.assertEqual(len(self.transport.uploads), 1)

    def test_retry_within_window_reuses_the_same_client_token_and_attachment(self):
        receipt = {'file_token': 'fixture-report'}

        def lose_unapplied_create():
            self.transport.records.pop((TABLES['maintenance'], 'recLedger'))
            raise TimeoutError

        self.transport.created_hook = lose_unapplied_create
        with patch('bin.lan_bitable_template_portal.tool_management_cloud.time.time', return_value=1000):
            with self.assertRaises(TimeoutError):
                self.tool.write_workspace_item('fixture', ASSET, '报告.xlsx', WORKBOOK, DAY,
                    file_token=receipt['file_token'], save_attempt=lambda value: receipt.update(attempted_at=value))
        self.transport.created_hook = None
        with patch('bin.lan_bitable_template_portal.tool_management_cloud.time.time', return_value=1010):
            restarted = ToolCloud(self.transport)
            restarted.write_workspace_item('fixture', ASSET, '报告.xlsx', WORKBOOK, DAY, **receipt)
        self.assertEqual(len(self.transport.creates), 2)
        self.assertEqual(self.transport.creates[0], self.transport.creates[1])
        self.assertEqual(self.transport.uploads, [])

    def test_failed_attempt_persistence_prevents_create_and_unmatched_attachment_stays_pending(self):
        def persist_error(_):
            raise OSError('store unavailable')

        with self.assertRaises(OSError):
            self.tool.write_workspace_item('fixture', ASSET, '报告.xlsx', WORKBOOK, DAY,
                                          file_token='fixture-report', save_attempt=persist_error)
        self.assertEqual(self.transport.creates, [])
        self.tool.write_workspace_item('fixture', ASSET, '报告.xlsx', WORKBOOK, DAY,
                                      file_token='fixture-report', save_attempt=lambda _: None)
        with self.assertRaisesRegex(RiskError, '回读未一致'):
            self.tool.write_workspace_item('fixture', ASSET, '报告.xlsx', WORKBOOK, DAY,
                                          file_token='another-token', remote_id='recLedger')
        self.assertEqual(len(self.transport.creates), 1)
        self.assertEqual(self.transport.updates, [])

    def test_existing_record_requires_the_persisted_attachment_receipt(self):
        self.tool.write_workspace_item('fixture', ASSET, '报告.xlsx', WORKBOOK, DAY,
                                      file_token='fixture-report', save_attempt=lambda _: None)
        self.transport.records[(TABLES['maintenance'], 'recLedger')]['维护记录单'] = [
            {'file_token': 'unrelated-report'}]
        for remote_id in ('', 'recLedger'):
            with self.subTest(remote_id=remote_id), self.assertRaisesRegex(RiskError, '附件回执'):
                self.tool.write_workspace_item('fixture', ASSET, '报告.xlsx', WORKBOOK, DAY,
                                              remote_id=remote_id)
        self.assertEqual(len(self.transport.creates), 1)
        self.assertEqual(self.transport.uploads, [])
        self.assertEqual(self.transport.updates, [])

    def test_distinct_tools_and_sessions_keep_separate_idempotent_records(self):
        receipt = {'file_token': 'fixture-report', 'save_attempt': lambda _: None}
        jobs = [('first', ASSET), ('first', ASSET | {'asset_id': 'recAnother'}),
                ('second', ASSET | {'qualified': True})]
        for session, item in jobs:
            result = self.tool.write_workspace_item(session, item, '报告.xlsx', WORKBOOK, DAY, **receipt)
            restarted = ToolCloud(self.transport)
            self.assertEqual(restarted.write_workspace_item(session, item, '报告.xlsx', WORKBOOK, DAY,
                             remote_id=result['remote_id'], **receipt), result)
        self.assertEqual(len(self.transport.creates), 3)
        self.assertEqual(len({params['client_token'] for _, _, params in self.transport.creates}), 3)
        self.assertEqual([fields[KEY] for _, fields, _ in self.transport.creates],
                         ['workspace:first:recAsset', 'workspace:first:recAnother', 'workspace:second:recAsset'])
        self.assertEqual(self.transport.creates[-1][1]['工具检测是否合格'], '合格')
        self.assertEqual(self.transport.uploads, [])
        self.assertEqual(self.transport.updates, [])

    def test_file_receipt_is_saved_before_attempt_and_reused_after_persistence_failure(self):
        receipt = {}

        def fail_attempt(_):
            self.assertEqual(receipt['file_token'], 'fixture-report')
            self.assertEqual(self.transport.creates, [])
            raise OSError('store unavailable')

        with self.assertRaises(OSError):
            self.tool.write_workspace_item('fixture', ASSET, '报告.xlsx', WORKBOOK, DAY,
                save_file=lambda token: receipt.update(file_token=token), save_attempt=fail_attempt)
        self.assertEqual(len(self.transport.uploads), 1)
        self.assertEqual(self.transport.creates, [])
        ToolCloud(self.transport).write_workspace_item('fixture', ASSET, '报告.xlsx', WORKBOOK, DAY,
                                                      **receipt, save_attempt=lambda _: None)
        self.assertEqual(len(self.transport.uploads), 1)
        self.assertEqual(len(self.transport.creates), 1)

    def test_file_persistence_failure_and_missing_callbacks_prevent_record_creation(self):
        for kwargs in ({}, {'save_attempt': lambda _: None}):
            with self.subTest(kwargs=tuple(kwargs)), self.assertRaises(RiskError):
                self.tool.write_workspace_item('fixture', ASSET, '报告.xlsx', WORKBOOK, DAY, **kwargs)
        self.assertEqual(self.transport.uploads, [])

        def fail_file(_):
            raise OSError('store unavailable')

        def unexpected_attempt(_):
            self.fail('Attempt must not be saved before the file receipt')

        with self.assertRaises(OSError):
            self.tool.write_workspace_item('fixture', ASSET, '报告.xlsx', WORKBOOK, DAY,
                                          save_file=fail_file, save_attempt=unexpected_attempt)
        self.assertEqual(self.transport.creates, [])

    def test_changed_readback_fields_never_overwrite_the_existing_record(self):
        self.tool.write_workspace_item('fixture', ASSET, '报告.xlsx', WORKBOOK, DAY,
                                      file_token='fixture-report', save_attempt=lambda _: None)
        original = deepcopy(self.transport.records[(TABLES['maintenance'], 'recLedger')])
        changes = [(KEY, 'workspace:other:recAsset'), (STATE, '合格'), ('楼栋', '南通A楼'),
                   ('维护工具', ['recOther']), ('维护工具', ['recAsset', 'recOther']),
                   ('工具检测是否合格', '合格'), ('计划维护日期', 1), ('实际维护日期', 1),
                   ('维护记录单', []), ('维护记录单', [{'file_token': 'other'}]),
                   ('维护记录单', [{'file_token': 'fixture-report'}] * 2)]
        for name, value in changes:
            with self.subTest(field=name, value=value):
                self.transport.records[(TABLES['maintenance'], 'recLedger')] = original | {name: value}
                with self.assertRaisesRegex(RiskError, '回读未一致'):
                    self.tool.write_workspace_item('fixture', ASSET, '报告.xlsx', WORKBOOK, DAY,
                                                  file_token='fixture-report', remote_id='recLedger')
        self.assertEqual(len(self.transport.creates), 1)
        self.assertEqual(self.transport.uploads, [])
        self.assertEqual(self.transport.updates, [])

    def test_duplicate_key_and_conflicting_remote_receipt_block_new_writes(self):
        self.tool.write_workspace_item('fixture', ASSET, '报告.xlsx', WORKBOOK, DAY,
                                      file_token='fixture-report', save_attempt=lambda _: None)
        with self.assertRaises(RiskError) as conflict:
            self.tool.write_workspace_item('fixture', ASSET, '报告.xlsx', WORKBOOK, DAY,
                                          file_token='fixture-report', remote_id='recDifferent')
        self.assertEqual(conflict.exception.status, 409)
        self.transport.records[(TABLES['maintenance'], 'recDuplicate')] = deepcopy(
            self.transport.records[(TABLES['maintenance'], 'recLedger')])
        with self.assertRaisesRegex(RiskError, '重复或未完整返回'):
            self.tool.write_workspace_item('fixture', ASSET, '报告.xlsx', WORKBOOK, DAY,
                                          file_token='fixture-report', save_attempt=lambda _: None)
        self.assertEqual(len(self.transport.creates), 1)
        self.assertEqual(self.transport.uploads, [])

    def test_binding_conflict_on_post_write_readback_is_not_overwritten(self):
        fields = self.transport.records[(INVENTORY, 'recAsset')]
        fields[TEMPLATE_LINK] = ''
        self.transport.updated_hook = lambda: fields.update({TEMPLATE_LINK: 'recConcurrent'})
        with self.assertRaises(RiskError) as conflict:
            self.tool.bind_template('recAsset', 'recTemplate', '')
        self.assertEqual(conflict.exception.status, 409)
        self.assertEqual(fields[TEMPLATE_LINK], 'recConcurrent')
        self.assertEqual(len(self.transport.updates), 1)


if __name__ == '__main__':
    unittest.main()
