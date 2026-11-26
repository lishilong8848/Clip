"""Isolated counter export tests. No project credentials, HTTP connections or local metric store."""
import copy
import json
import sys
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lan_bitable_template_portal import usage_bitable as usage


class FakeBitable:
    path = 'bitable/v1/apps/baseFixture/tables/' + usage.TABLE

    def __init__(self):
        self.fields = {usage.KEY: {'field_name': usage.KEY, 'type': 1, 'is_primary': True}}
        self.rows = {'recBlank': {}}
        self.tokens = {}
        self.calls = []
        self.lose_reply = ''
        self.hide_search = False
        self.search_pages = None

    def handle(self, method, path, token, *, body=None, params=None):
        assert token == 'fixture'
        params = params or {}
        self.calls.append((method, path, copy.deepcopy(body), params.copy()))
        code = 0
        if method == 'GET' and path == 'wiki/v2/spaces/get_node':
            assert params == {'token': usage.WIKI, 'obj_type': 'wiki'}
            data = {'node': {'node_token': usage.WIKI, 'obj_type': 'bitable', 'obj_token': 'baseFixture'}}
        elif method == 'GET' and path == self.path + '/views/' + usage.VIEW:
            data = {'view': {'view_id': usage.VIEW}}
        elif path == self.path + '/fields' and method == 'GET':
            data = {'items': list(self.fields.values()), 'has_more': False}
        elif path == self.path + '/fields' and method == 'POST':
            assert body['field_name'] not in self.fields
            self.fields[body['field_name']] = body.copy()
            data = {'field': body}
        elif path == self.path + '/records/search' and method == 'POST':
            field, = body['filter']['conditions']
            key, = field['value']
            assert field['field_name'] == usage.KEY and field['operator'] == 'is'
            assert 'view_id' not in body
            items = [{'record_id': rid, 'fields': {usage.KEY: [{'text': key}]}}
                     for rid, fields in self.rows.items() if fields.get(usage.KEY) == key]
            data = {'items': [] if self.hide_search else items, 'has_more': False}
            if self.search_pages is not None:
                data = self.search_pages[params.get('page_token', '')]
        elif path == self.path + '/records' and method == 'POST':
            client_token = params['client_token']
            assert uuid.UUID(client_token).version == 4
            if client_token not in self.tokens:
                rid = f'recTest{len(self.tokens) + 1}'
                self.tokens[client_token] = rid, copy.deepcopy(body)
                self.rows[rid] = body['fields'].copy()
            rid, original = self.tokens[client_token]
            assert original == body, 'same client_token received a different body'
            data = {'record': {'record_id': rid, 'fields': original['fields']}}
        elif path.startswith(self.path + '/records/') and method in ('GET', 'PUT'):
            rid = path.rsplit('/', 1)[-1]
            if rid not in self.rows:
                code, data = 1254043, {}
            else:
                if method == 'PUT':
                    assert set(body['fields']) == {usage.COUNT}
                    self.rows[rid].update(body['fields'])
                data = {'record': {'record_id': rid, 'fields': self.rows[rid]}}
        else:
            raise AssertionError('unexpected request')
        if self.lose_reply and method == self.lose_reply and not path.endswith(('/search', '/fields')):
            self.lose_reply = ''
            raise TimeoutError('transport detail must stay private')
        return SimpleNamespace(code=code, success=lambda: code == 0,
                               raw=SimpleNamespace(content=json.dumps({'data': data}).encode()))

    def requests(self, method, suffix):
        return [r for r in self.calls if r[0] == method and r[1].endswith(suffix)]


class UsageBitableTests(unittest.TestCase):
    def setUp(self):
        self.cloud = FakeBitable()
        transport = SimpleNamespace(
            ensure_feishu_token=Mock(return_value='fixture'), _feishu_request=self.cloud.handle,
            TOKEN_ERROR_CODES=set(), _stable_uuid4_client_token=lambda seed: str(
                uuid.UUID(bytes=uuid.uuid5(uuid.NAMESPACE_URL, seed).bytes, version=4)))
        self.guard = Mock(return_value={'real_write_allowed': True})
        self.enterContext(patch.dict(sys.modules, {
            'upload_event_module.services': SimpleNamespace(feishu_service=transport),
            'upload_event_module.services.feishu_service': transport,
            'lan_bitable_template_portal.portal_service': SimpleNamespace(external_real_write_guard=self.guard)}))
        self.sink = usage.UsageBitableSink()
        self.row = {'key': str(uuid.uuid4()), 'day': '2026-11-26', 'client': 'web_v2',
                    'module_label': '学习', 'page_label': '答题', 'count': 3, 'record_id': ''}

    def send(self, **changes):
        return self.sink.upsert({**self.row, **changes})

    def test_create_update_and_replay_only_touch_owned_row_and_count(self):
        rid = self.send()
        self.assertEqual(self.cloud.rows[rid], {usage.KEY: self.row['key'], usage.DAY: 1795622400000,
                         usage.MODULE: '学习', usage.PAGE: '答题', usage.CLIENT: '新版网页', usage.COUNT: 3})
        self.cloud.rows[rid]['手工备注'] = '保留'
        self.assertEqual(self.send(count=15, record_id=rid), rid)
        self.assertEqual(self.send(count=2), rid)
        self.assertEqual(self.cloud.rows[rid][usage.COUNT], 15)
        self.assertEqual(self.cloud.rows[rid]['手工备注'], '保留')
        self.assertEqual(self.cloud.rows['recBlank'], {})
        self.assertEqual(len(self.cloud.requests('POST', '/records')), 1)
        self.assertEqual(len(self.cloud.requests('GET', '/fields')), 1)
        self.assertEqual(len(self.cloud.requests('POST', '/fields')), 5)

    def test_distinct_day_and_clients_are_analyzable_rows(self):
        first = self.send()
        for code, label in [('web_v2', '新版网页'), ('web_legacy', '旧版网页'),
                            ('web_standalone', '独立网页'), ('qt', 'Qt 桌面')]:
            rid = self.send(key=str(uuid.uuid4()), day='2026-11-27', client=code)
            self.assertEqual(self.cloud.rows[rid][usage.CLIENT], label)
            self.assertEqual(self.cloud.rows[rid][usage.DAY] - self.cloud.rows[first][usage.DAY], 86400000)
        self.assertEqual(len(self.cloud.rows), 6)

    def test_lost_create_receipt_with_growing_count_and_stale_search(self):
        self.cloud.lose_reply = 'POST'
        with self.assertRaisesRegex(RuntimeError, '未确认'):
            self.send()
        self.cloud.hide_search = True
        self.sink = usage.UsageBitableSink()
        rid = self.send(count=8)
        creates = self.cloud.requests('POST', '/records')
        self.assertEqual([r[3]['client_token'] for r in creates], [self.row['key']] * 2)
        self.assertEqual(creates[0][2], creates[1][2])
        self.assertEqual(len(self.cloud.rows), 2)
        self.assertEqual(self.cloud.rows[rid][usage.COUNT], 8)

    def test_lost_update_receipt_and_restart_recovers_by_key(self):
        self.cloud.lose_reply = 'PUT'
        with self.assertRaisesRegex(RuntimeError, '未确认'):
            self.send()
        self.sink = usage.UsageBitableSink()
        rid = self.send(count=9)
        self.assertEqual(len(self.cloud.requests('POST', '/records')), 1)
        self.assertEqual(self.cloud.rows[rid][usage.COUNT], 9)

    def test_deleted_record_replaced_once_even_if_recreation_reply_is_lost(self):
        old = self.send()
        del self.cloud.rows[old]
        self.cloud.lose_reply = 'POST'
        with self.assertRaisesRegex(RuntimeError, '未确认'):
            self.send(count=7, record_id=old)
        self.cloud.hide_search = True
        new = self.send(count=10, record_id=old)
        self.cloud.hide_search = False
        self.assertNotEqual(new, old)
        self.assertEqual(self.send(count=11, record_id=old), new)
        self.assertEqual(self.cloud.rows[new][usage.COUNT], 11)
        tokens = [r[3]['client_token'] for r in self.cloud.requests('POST', '/records')]
        self.assertEqual(tokens[1], tokens[2])
        self.assertNotEqual(tokens[0], tokens[1])
        self.assertEqual(len(self.cloud.rows), 2)

    def test_wrong_receipt_and_reused_key_never_overwrite_history(self):
        rid = self.send()
        for changes in ({'record_id': 'recBlank'}, {'record_id': rid, 'key': str(uuid.uuid4())},
                        {'day': '2026-11-27'}, {'module_label': '其他'}, {'page_label': '其他'}, {'client': 'qt'}):
            with self.subTest(changes=changes), self.assertRaises(RuntimeError):
                self.send(count=99, **changes)
        self.assertEqual(self.cloud.rows[rid][usage.COUNT], 3)
        self.assertEqual(self.cloud.rows['recBlank'], {})

    def test_duplicate_key_on_next_page_blocks_even_with_receipt(self):
        rid = self.send()
        fields = {usage.KEY: self.row['key']}
        self.cloud.search_pages = {
            '': {'items': [{'record_id': rid, 'fields': fields}], 'has_more': True, 'page_token': 'again'},
            'again': {'items': [{'record_id': 'recDuplicate', 'fields': fields}], 'has_more': False}}
        with self.assertRaisesRegex(RuntimeError, '重复统计键'):
            self.send(record_id=rid, count=18)
        self.assertEqual(self.cloud.rows[rid][usage.COUNT], 3)

    def test_unconfirmed_search_cannot_authorize_creation(self):
        for page in ({'items': []}, {'items': [], 'has_more': True},
                     {'items': [{'record_id': 'recBlank', 'fields': {usage.KEY: 'other'}}], 'has_more': False}):
            self.cloud.search_pages = {'': page}
            with self.subTest(page=page), self.assertRaises(RuntimeError):
                self.send()
        self.assertFalse(self.cloud.requests('POST', '/records'))
        self.assertEqual(self.cloud.rows, {'recBlank': {}})

    def test_conflicting_type_stops_before_any_field_is_created(self):
        self.cloud.fields[usage.COUNT] = {'field_name': usage.COUNT, 'type': 1}
        with self.assertRaisesRegex(RuntimeError, '统计数量.*类型冲突'):
            self.send()
        self.assertFalse(self.cloud.requests('POST', '/fields'))
        self.assertFalse(self.cloud.requests('POST', '/records'))
        self.cloud.fields[usage.COUNT].update(type=2, ui_type='Progress')
        with self.assertRaisesRegex(RuntimeError, '统计数量.*类型冲突'):
            self.send()
        self.assertFalse(self.cloud.requests('POST', '/fields'))
        self.cloud.fields[usage.COUNT]['ui_type'] = 'Number'
        self.send()
        self.assertEqual(len(self.cloud.requests('POST', '/fields')), 4)
        self.cloud.fields[usage.DAY]['type'] = 1
        self.sink = usage.UsageBitableSink()
        with self.assertRaisesRegex(RuntimeError, '统计日期.*类型冲突'):
            self.send()

    def test_concurrent_same_sink_reuses_schema_and_preserves_maximum(self):
        with ThreadPoolExecutor(max_workers=6) as pool:
            receipts = list(pool.map(lambda count: self.send(count=count), (15, 1, 22, 33, 2, 10)))
        self.assertEqual(len(set(receipts)), 1)
        self.assertEqual(self.cloud.rows[receipts[0]][usage.COUNT], 33)
        self.assertEqual(len(self.cloud.requests('GET', '/fields')), 1)
        self.assertEqual(len(self.cloud.requests('POST', '/records')), 1)

    def test_invalid_input_and_inactive_writer_make_no_requests(self):
        for changes in ({'day': '20261126'}, {'key': str(uuid.uuid1())}, {'count': True}, {'count': -1},
                        {'client': 'unknown'}, {'record_id': 'recA/other'}, {'page_label': ''}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.send(**changes)
        self.guard.return_value = {'real_write_allowed': False}
        with self.assertRaisesRegex(RuntimeError, '未启用'):
            self.send()
        self.assertFalse(self.cloud.calls)


if __name__ == '__main__':
    unittest.main()
