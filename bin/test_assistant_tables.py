"""Offline read-only table, scope, pagination and Feishu consent checks."""
import asyncio
import copy
import json
from pathlib import Path
import sys
import time
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

sys.path.insert(0, str(Path(__file__).parent))
from bin.test_feishu_assistant import Memory
from lan_bitable_template_portal.assistant_tables import AssistantTables, PERSONNEL_TABLE, PERSONNEL_URL
from lan_bitable_template_portal.assistant_read_consent import ReadConsent
from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_sources import SCOPES

ADMIN = {'id': 'admin', 'scopes': sorted(SCOPES), 'is_admin': True}


class TableTests(unittest.TestCase):
    def setUp(self):
        self.store = Memory()
        self.store.put_document('link_directory', 'snapshot', {'updated_at': time.time(), 'items': [
            {'name': '人员表', 'id': 'rec1', 'url': PERSONNEL_URL, 'sort': 1, 'category': '人员', 'purpose': '在岗人员'},
            {'name': '设备台账', 'id': 'rec2', 'url': 'https://vnet.feishu.cn/wiki/nodeABC?table=tblEquipment', 'sort': 2, 'category': '设备', 'purpose': ''},
            {'name': '平台门户', 'id': 'rec3', 'url': 'https://www.sm.sjhl.online:3001/nantong-base', 'sort': 3, 'category': '网页', 'purpose': ''}]})
        self.service = SimpleNamespace(_request_payload=self.request, _auth_headers=lambda: {'Authorization': 'fake'})
        self.tables = AssistantTables(self.service, self.store)
        self.calls = []
        self.fields = [('员工姓名', 1), ('楼栋', 1), ('机楼/专业', 1), ('员工工号', 1), ('openid', 1),
                       ('离职/异动情况', 7), ('身份证号', 1), ('家庭住址', 1), ('密钥', 1), ('签名图片', 17)]
        self.records = [{'record_id': 'recA', 'fields': {'员工姓名': '示例A', '楼栋': 'A楼', 'openid': 'ou_testA', '离职/异动情况': False}},
                        {'record_id': 'recD', 'fields': {'员工姓名': '示例D', '楼栋': 'D楼', '离职/异动情况': True}}]
        self.more = False

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        if url.endswith('get_node'):
            data = {'node': {'obj_type': 'bitable', 'obj_token': 'baseABC'}}
        elif url.endswith('/fields'):
            data = {'items': [{'field_name': n, 'type': t} for n, t in self.fields], 'has_more': False}
        elif url.endswith('/records/search'):
            chosen = kwargs['json_payload']['field_names']
            data = {'items': [{**r, 'fields': {k: v for k, v in r['fields'].items() if k in chosen}} for r in self.records],
                    'has_more': self.more, 'total': len(self.records)}
            if self.more: data['page_token'] = 'next'
        else:
            raise AssertionError('Unexpected endpoint: ' + url)
        return {'code': 0, 'data': data}

    def test_catalog_excludes_websites_no_network(self):
        self.assertEqual(self.tables.catalog()['total'], 2)
        self.assertEqual(self.tables.catalog('在岗人员')['items'][0]['table_id'], PERSONNEL_TABLE)
        self.assertEqual(self.calls, [])

    def test_unregistered_and_write_rejected(self):
        for table in ('tblOther', '../../x', None):
            with self.assertRaises(AssistantError): self.tables.read(ADMIN, {'table_id': table})
        for method in ('PUT', 'DELETE', 'POST'):
            with self.assertRaises(AssistantError): self.tables.request(method, 'bitable/v1/apps/a/tables/tblA/records')
        self.assertEqual(self.calls, [])

    def test_forbidden_fields_never_requested_or_returned(self):
        result = self.tables.read(ADMIN, {'table_id': PERSONNEL_TABLE})
        self.assertEqual(result['total'], 2)
        for name in ('openid', '身份证号', '家庭住址', '密钥', '签名图片'):
            self.assertNotIn(name, self.calls[-1][2]['json_payload']['field_names'])
            with self.assertRaises(AssistantError): self.tables.read(ADMIN, {'table_id': PERSONNEL_TABLE, 'fields': [name]})
        self.assertNotIn('ou_testA', json.dumps(result))

    def test_paging_cache_and_scope_filter(self):
        actor = {'id': 'dutyA', 'scopes': ['A'], 'is_admin': False}
        result = self.tables.read(actor, {'table_id': PERSONNEL_TABLE})
        self.assertIsNone(result['total'])
        self.assertEqual([r['record_id'] for r in result['items']], ['recA'])
        self.tables.read(ADMIN, {'table_id': PERSONNEL_TABLE})
        self.assertEqual(len(self.calls), 2)
        self.tables.read(ADMIN, {'table_id': PERSONNEL_TABLE, 'cursor': 'next'})
        self.assertEqual(len(self.calls), 3)

    def test_blank_scope_and_narrowed_admin_fail_closed(self):
        self.records += [{'record_id': 'recBlank', 'fields': {'员工姓名': '无楼栋'}}]
        result = self.tables.read({**ADMIN, 'scopes': ['A']}, {'table_id': PERSONNEL_TABLE})
        self.assertEqual(result['visible_page_count'], 1)
        self.fields = [('员工姓名', 1)]
        other = AssistantTables(self.service, Memory())
        other.directory_rows = self.tables.directory_rows
        with self.assertRaises(AssistantError): other.read({**ADMIN, 'scopes': ['A']}, {'table_id': PERSONNEL_TABLE})

    def test_multibuilding_record_cannot_leak_to_one_building(self):
        self.records = [{'record_id': 'recAB', 'fields': {'员工姓名': '多楼人员', '楼栋': 'A/B楼'}}]
        result = self.tables.read({'id': 'b', 'scopes': ['B']}, {'table_id': PERSONNEL_TABLE})
        self.assertEqual(result['items'], [])

    def test_wiki_resolves_only_fixed_official_endpoint(self):
        self.tables.read(ADMIN, {'table_id': 'tblEquipment', 'metadata_only': True})
        self.assertEqual(len(self.calls), 2)
        self.assertTrue(self.calls[1][1].endswith('/apps/baseABC/tables/tblEquipment/fields'))

    def test_invalid_page_not_cached(self):
        self.service._request_payload = lambda *_a, **_k: {'code': 0, 'data': {'items': [], 'has_more': True}}
        with self.assertRaises(AssistantError): self.tables.read(ADMIN, {'table_id': PERSONNEL_TABLE})
        self.assertFalse(any(ns == 'assistant_table_cache' for ns, _key in self.store.docs))

    def test_personnel_full_snapshot_minimal_fields_and_reuse(self):
        result = self.tables.staff_count(ADMIN)
        self.assertEqual(result['unique_people'], 1)
        self.assertEqual(result['source_url'], PERSONNEL_URL)
        self.assertNotIn('签名图片', self.calls[-1][2]['json_payload']['field_names'])
        count = len(self.calls)
        self.tables.staff_count(ADMIN)
        self.assertEqual(len(self.calls), count)
        self.assertNotIn('ou_testA', str(self.store.docs))

    def test_personnel_repeated_cursor_never_reports_partial_count(self):
        self.more = True
        with self.assertRaises(AssistantError): self.tables.staff_count(ADMIN)
        self.assertIsNone(self.store.get_document('assistant_table_cache', 'staff-count'))

    def test_sensitive_filter_and_malformed_request_rejected(self):
        with self.assertRaises(AssistantError):
            self.tables.read(ADMIN, {'table_id': PERSONNEL_TABLE, 'filters': [{'field_name': 'openid', 'operator': 'is', 'value': ['x']}]})
        with self.assertRaises(AssistantError):
            self.tables.read(ADMIN, {'table_id': PERSONNEL_TABLE, 'limit': True})

    def test_secret_configuration_tables_blocked_even_for_admin(self):
        doc = self.store.get_document('link_directory', 'snapshot')
        doc['items'][0]['name'] = '模型配置及凭证'
        self.store.put_document('link_directory', 'snapshot', doc)
        with self.assertRaises(AssistantError): self.tables.read(ADMIN, {'table_id': PERSONNEL_TABLE})
        self.assertEqual(self.calls, [])


class ConsentTests(unittest.TestCase):
    def setUp(self):
        self.store = Memory()
        self.consent = ReadConsent(self.store)
        self.actor = {**ADMIN, 'channel': 'feishu:oc_private'}
        self.payload = {'table_id': 'tblA', 'scopes': ['A']}

    def require(self, actor=None, payload=None):
        return self.consent.require(actor or self.actor, 'read', payload or self.payload, '人员统计', '在职多少人')

    def test_web_does_not_add_confirmation(self):
        self.assertIsNone(self.require(ADMIN))
        self.assertEqual(self.store.docs, {})

    def test_only_owner_same_chat_exact_request_can_approve(self):
        code = self.require()['confirmation_code']
        for actor in ({**self.actor, 'id': 'someone'}, {**self.actor, 'channel': 'feishu:oc_other'}):
            with self.assertRaises(AssistantError): self.consent.approve(actor, code)
        self.assertEqual(self.consent.approve(self.actor, code), '在职多少人')
        self.assertIsNone(self.require())
        self.assertTrue(self.require(payload={**self.payload, 'scopes': ['A', 'B']})['confirmation_required'])
        with self.assertRaises(AssistantError): self.consent.approve(self.actor, code)

    def test_expired_and_changed_question_not_approved(self):
        code = self.require()['confirmation_code']
        for item in self.store.docs.values(): item['expires'] = 0
        with self.assertRaises(AssistantError): self.consent.approve(self.actor, code)
        new = self.require()
        self.assertNotEqual(code, new['confirmation_code'])
        self.consent.approve(self.actor, new['confirmation_code'])
        self.assertTrue(self.consent.require(self.actor, 'read', self.payload, '人员统计', '查询姓名')['confirmation_required'])


class ModelCountTests(unittest.IsolatedAsyncioTestCase):
    async def test_headcount_uses_tool_without_model_and_retains_source(self):
        from contextlib import asynccontextmanager
        from bin import test_lighthouse_stream as fixtures
        from openclaw_service.assistant.lighthouse_model import LighthouseModel
        holder = fixtures.StreamTests()
        await holder.asyncSetUp()
        self.addAsyncCleanup(holder.runtime.close)
        self.addCleanup(holder.tmp.cleanup)
        @asynccontextmanager
        async def forbidden(*_):
            raise AssertionError('Simple headcount must not start a model/gateway')
            yield
        bridge = SimpleNamespace(acall=AsyncMock(return_value={'unique_people': 92, 'raw_active_records': 92,
            'observed_at': 1791532689, 'source_url': PERSONNEL_URL}))
        holder.portal.catalog.bridge = bridge
        engine = LighthouseModel(holder.portal, model_factory=forbidden)
        result = await engine.answer(ADMIN, {'question': '南通基地总共有多少人在职？', '_profile': holder.model.profile()},
            [], holder.request, AsyncMock(), AsyncMock(return_value=ADMIN), {})
        self.assertIn('92', result['answer'])
        self.assertEqual(bridge.acall.await_args.args[0], 'staff_count')
        self.assertEqual(result['sources'][0]['url'], PERSONNEL_URL)
        bridge.acall.return_value = {'confirmation_required': True, 'message': '确认查询 abcdef12'}
        result = await engine.answer(ADMIN, {'question': '在职人数', '_profile': holder.model.profile()},
            [], holder.request, AsyncMock(), AsyncMock(return_value=ADMIN), {})
        self.assertEqual(result['answer'], '确认查询 abcdef12')
        self.assertEqual(result['sources'], [])


class BridgeReadTests(unittest.IsolatedAsyncioTestCase):
    async def test_channel_consent_blocks_read_before_data_is_loaded(self):
        from lan_bitable_template_portal.lighthouse_bridge import PortalAuthority
        from starlette.requests import Request
        store = Memory()
        runtime = SimpleNamespace(state_store=store)
        authority = PortalAuthority(None, None, runtime, SimpleNamespace(instance='i', lease='l'))
        actor = {**ADMIN, 'channel': 'feishu:oc_private'}
        request = Request({'type': 'http', 'state': {'feishu_question': '在职人数'}})
        authority.authorize = AsyncMock(return_value=({'request': request}, actor))
        authority.tables = SimpleNamespace(staff_count=Mock(return_value={'total': 5}))
        message = {'instance': 'i', 'lease': 'l', 'context_id': 'trusted', 'action': 'staff_count',
                   'payload': {'scopes': actor['scopes']}}
        result = await authority.dispatch(message)
        self.assertTrue(result['confirmation_required'])
        authority.tables.staff_count.assert_not_called()
        authority.read_consent.approve(actor, result['confirmation_code'])
        result = await authority.dispatch(message)
        self.assertEqual(result['total'], 5)
        authority.authorize.return_value = ({'request': request}, {**actor, 'scopes': ['A']})
        with self.assertRaises(AssistantError): await authority.dispatch(message)


if __name__ == '__main__':
    unittest.main()
