"""Offline read-only table, scope, pagination and Feishu consent checks."""
import asyncio
import copy
import json
from pathlib import Path
import sys
import time
from concurrent.futures import ThreadPoolExecutor
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

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

    def test_shared_read_cache_only_writes_assistant_namespace_and_coalesces_missing_pages(self):
        before = copy.deepcopy(self.store.docs)
        self.tables.read(ADMIN, {'table_id': PERSONNEL_TABLE, 'metadata_only': True})
        self.calls.clear()
        actors = [{'id': str(i), 'scopes': ['A' if i % 2 else 'D'], 'is_admin': False} for i in range(20)]
        with ThreadPoolExecutor(max_workers=10) as pool:
            results = list(pool.map(lambda actor: self.tables.read(actor, {'table_id': PERSONNEL_TABLE}), actors))
        self.assertEqual(len(self.calls), 1)
        for actor, result in zip(actors, results):
            self.assertEqual([row['record_id'] for row in result['items']], ['rec' + actor['scopes'][0]])
        self.assertEqual({key: value for key, value in self.store.docs.items() if key[0] != 'assistant_table_cache'}, before)
        self.assertTrue(all(url.endswith('/records/search') and method == 'POST' for method, url, _ in self.calls))

    def test_business_data_version_invalidates_read_copy_without_changing_business(self):
        revision = [1]
        self.service.state_cache_version = lambda: revision[0]
        query = {'table_id': PERSONNEL_TABLE}
        self.tables.read(ADMIN, query)
        count = len(self.calls)
        self.tables.read(ADMIN, query)
        self.assertEqual(len(self.calls), count)
        self.records[0]['fields']['员工姓名'] = '更新后人员'
        revision[0] += 1
        refreshed = self.tables.read(ADMIN, query)
        self.assertEqual(refreshed['items'][0]['fields']['员工姓名'], '更新后人员')
        self.assertEqual(len(self.calls), count + 2)
        self.tables.read(ADMIN, query)
        self.assertEqual(len(self.calls), count + 2)

    def test_missing_new_field_reloads_only_schema_then_caches_new_projection(self):
        self.tables.read(ADMIN, {'table_id': PERSONNEL_TABLE, 'metadata_only': True})
        self.store.docs[('assistant_table_cache', 'schema:' + PERSONNEL_TABLE)]['observed_at'] -= 11
        self.fields.append(('专业', 1))
        self.records[0]['fields']['专业'] = '电气'
        query = {'table_id': PERSONNEL_TABLE, 'fields': ['员工姓名', '专业']}
        result = self.tables.read(ADMIN, query)
        self.assertEqual(result['items'][0]['fields']['专业'], '电气')
        count = len(self.calls)
        self.tables.read(ADMIN, query)
        self.assertEqual(len(self.calls), count)

    def test_failed_fill_preserves_good_read_copy_without_claiming_it_is_current(self):
        query = {'table_id': PERSONNEL_TABLE}
        self.tables.read(ADMIN, query)
        key = next(key for key in self.store.docs if key[0] == 'assistant_table_cache' and key[1].startswith('page:'))
        self.store.docs[key]['observed_at'] -= 121
        original = copy.deepcopy(self.store.docs[key])
        self.service._request_payload = Mock(side_effect=AssistantError('read failed', 503))
        with self.assertRaises(AssistantError):
            self.tables.read(ADMIN, query)
        self.assertEqual(self.store.docs[key], original)

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

    def test_consent_discloses_reply_in_current_conversation(self):
        message = self.require()['message']
        self.assertIn('当前会话回复', message)
        self.assertIn('群成员可见', message)
        self.assertNotIn('私信返回', message)

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


class RosterCountTests(unittest.TestCase):
    def test_employee_snapshot_query_includes_long_day_and_preserves_reminder_state(self):
        from lan_bitable_template_portal import personnel_source
        store=Memory()
        store.put_document('local','shift_roster',{'original':True})
        service=SimpleNamespace(_last_error='original')
        tables=AssistantTables(service,store)
        snapshot={'people':[{'name':name,'scope':scope,'shift':shift,'duty_kind':kind,'open_id':oid}
            for name,scope,shift,kind,oid in [('甲','A','白','白','ou_one'),('乙','A','夜','夜','ou_two'),
                ('站员','110','白','白','ou_station'),('长白','A','白','长白','ou_day')]],'issues':[]}
        with patch.object(personnel_source,'daily_roster',return_value=snapshot) as read:
            value=tables.roster_count({**ADMIN,'scopes':['A','110']},'2026-10-26')
            self.assertEqual(value['assignment_count'],4)
            self.assertNotIn('ou_',str(value))
            tables.roster_count({**ADMIN,'scopes':['A','110']},'2026-10-26')
            self.assertEqual(read.call_count,1)
        self.assertEqual(store.get_document('local','shift_roster'),{'original':True})
        self.assertEqual(service._last_error,'original')

    def test_incomplete_snapshot_fails_closed_and_does_not_poison_cache(self):
        from lan_bitable_template_portal import personnel_source
        tables=AssistantTables(SimpleNamespace(),Memory())
        with patch.object(personnel_source,'daily_roster',side_effect=ValueError('incomplete')):
            with self.assertRaisesRegex(AssistantError,'未完整读取'):
                tables.roster_count({**ADMIN,'scopes':['A']},'2026-10-26')
        for day,shift in [('bad',''),('2026-10-26','早')]:
            with self.assertRaises(AssistantError): tables.roster_count(ADMIN,day,shift)


class CompleteTableReadTests(TableTests):
    def test_complete_read_preserves_projection_filters_and_gathers_pages(self):
        original = self.service._request_payload
        queries = []
        def request(method, url, **kwargs):
            if not url.endswith('/records/search'):
                return original(method, url, **kwargs)
            queries.append(kwargs)
            last = kwargs['params'].get('page_token') == 'next'
            return {'code': 0, 'data': {'items': [{'record_id': 'r2' if last else 'r1',
                'fields': {'员工姓名': '第二人' if last else '第一人', '楼栋': 'A楼'}}],
                'has_more': not last, **({'page_token': 'next'} if not last else {})}}
        self.service._request_payload = request
        filters = [{'field_name': '楼栋', 'operator': 'is', 'value': ['A楼']}]
        query = {'table_id': PERSONNEL_TABLE, 'fields': ['员工姓名', '楼栋'], 'filters': filters, 'complete': True}
        result = self.tables.read(ADMIN, query)
        self.assertTrue(result['complete'])
        self.assertEqual([r['record_id'] for r in result['items']], ['r1', 'r2'])
        self.assertEqual(queries[0]['json_payload'], queries[1]['json_payload'])
        self.assertEqual(queries[0]['json_payload']['filter']['conditions'], filters)
        self.assertNotIn('cursor', query)

    def test_incomplete_repeating_cursor_is_not_reported_complete(self):
        self.more = True
        with self.assertRaisesRegex(AssistantError, '游标未前进'):
            self.tables.read(ADMIN, {'table_id': PERSONNEL_TABLE, 'complete': True})

    def test_time_budget_keeps_partial_rows_explicit(self):
        self.more = True
        with patch('lan_bitable_template_portal.assistant_tables.time.monotonic', side_effect=[0, 26]):
            result = self.tables.read(ADMIN, {'table_id': PERSONNEL_TABLE, 'complete': True})
        self.assertFalse(result['complete'])
        self.assertTrue(result['has_more'])
        self.assertEqual(result['cursor'], 'next')


class ModelCountTests(unittest.IsolatedAsyncioTestCase):
    async def test_admin_revocation_stops_publish_after_reading_a_source(self):
        from contextlib import asynccontextmanager
        from pydantic_ai.models.function import DeltaToolCall, FunctionModel
        from bin import test_lighthouse_stream as fixtures
        from openclaw_service.assistant.lighthouse_model import LighthouseModel
        holder = fixtures.StreamTests()
        await holder.asyncSetUp()
        self.addAsyncCleanup(holder.runtime.close)
        self.addCleanup(holder.tmp.cleanup)
        current = dict(ADMIN)
        async def stream_model(messages, info):
            receipts = [part for message in messages for part in message.parts if getattr(part, 'part_kind', '') == 'tool-return']
            if not receipts:
                yield {0: DeltaToolCall(name='staff_headcount', json_args='{}')}
            else:
                current['is_admin'] = False
                yield '不得发布撤权前的旧结果'
        @asynccontextmanager
        async def factory(*_):
            yield FunctionModel(stream_function=stream_model)
        holder.portal.catalog.bridge = SimpleNamespace(acall=AsyncMock(return_value={'unique_people': 3}))
        engine = LighthouseModel(holder.portal, model_factory=factory)
        async def authorize():
            return dict(current)
        with self.assertRaisesRegex(AssistantError, '权限已变化'):
            await engine.answer(ADMIN, {'question': '查询在职人数', '_profile': holder.model.profile()},
                [], holder.request, AsyncMock(), authorize, {})

    async def test_semantic_tool_selection_without_a_question_match_preserves_consent_and_errors(self):
        from contextlib import asynccontextmanager
        from pydantic_ai.models.function import DeltaToolCall, FunctionModel
        from bin import test_lighthouse_stream as fixtures
        from openclaw_service.assistant.lighthouse_model import LighthouseModel
        holder = fixtures.StreamTests()
        await holder.asyncSetUp()
        self.addAsyncCleanup(holder.runtime.close)
        self.addCleanup(holder.tmp.cleanup)
        bridge = SimpleNamespace(acall=AsyncMock(return_value={'basis': '当天排班合计6人次，去重3人。', 'source_url': PERSONNEL_URL}))
        holder.portal.catalog.bridge = bridge
        async def stream_model(messages, info):
            receipts = [part.content for message in messages for part in message.parts if getattr(part, 'part_kind', '') == 'tool-return']
            self.assertIn('duty_roster', {tool.name for tool in info.function_tools})
            if not receipts:
                yield {0: DeltaToolCall(name='table_catalog', json_args='{"keyword":"当日安排"}')}
            elif len(receipts) == 1:
                yield {0: DeltaToolCall(name='duty_roster', json_args='{"date":"2026-10-22"}')}
            else:
                receipt = receipts[-1]
                yield receipt.get('message') or receipt.get('basis') or receipt['error']
        @asynccontextmanager
        async def factory(*_):
            yield FunctionModel(stream_function=stream_model)
        async def read(action, *args):
            return {'items': []} if action == 'table_catalog' else bridge.result
        bridge.acall.side_effect = read
        bridge.result = bridge.acall.return_value
        engine = LighthouseModel(holder.portal, model_factory=factory)
        async def ask():
            return await engine.answer(ADMIN, {'question': '帮我归并刚才说的两班安排', '_profile': holder.model.profile()},
                [], holder.request, AsyncMock(), AsyncMock(return_value=ADMIN), {})
        result = await ask()
        self.assertIn('6人次', result['answer'])
        self.assertEqual(bridge.acall.call_args.args[0], 'roster_count')
        bridge.result = {'confirmation_required': True, 'message': '确认查询 roster123'}
        result = await ask()
        self.assertEqual(result['answer'], '确认查询 roster123')
        self.assertEqual(result['sources'], [])
        bridge.acall.side_effect = AssistantError('排班视图未完整读取，人数未知。', 503)
        result = await ask()
        self.assertIn('人数未知', result['answer'])
        self.assertEqual(result['sources'], [])

    async def test_headcount_native_tool_retains_source_and_consent(self):
        from contextlib import asynccontextmanager
        from pydantic_ai.models.function import DeltaToolCall, FunctionModel
        from bin import test_lighthouse_stream as fixtures
        from openclaw_service.assistant.lighthouse_model import LighthouseModel
        holder = fixtures.StreamTests()
        await holder.asyncSetUp()
        self.addAsyncCleanup(holder.runtime.close)
        self.addCleanup(holder.tmp.cleanup)
        async def stream_model(messages, info):
            receipts = [part.content for message in messages for part in message.parts
                        if getattr(part, 'part_kind', '') == 'tool-return']
            if not receipts:
                yield {0: DeltaToolCall(name='staff_headcount', json_args='{}')}
            else:
                receipt = receipts[-1]
                yield receipt['message'] if receipt.get('confirmation_required') else f"在职人员共{receipt['unique_people']}人。"

        @asynccontextmanager
        async def model_factory(*_):
            yield FunctionModel(stream_function=stream_model)
        bridge = SimpleNamespace(acall=AsyncMock(return_value={'unique_people': 92, 'raw_active_records': 92,
            'observed_at': 1791532689, 'source_url': PERSONNEL_URL}))
        holder.portal.catalog.bridge = bridge
        engine = LighthouseModel(holder.portal, model_factory=model_factory)
        result = await engine.answer(ADMIN, {'question': '南通基地总共有多少人在职？', '_profile': holder.model.profile()},
            [], holder.request, AsyncMock(), AsyncMock(return_value=ADMIN), {})
        self.assertIn('92', result['answer'])
        bridge.acall.assert_awaited_once_with('staff_count', {'scopes': ADMIN['scopes']}, None)
        self.assertEqual(result['sources'][0]['url'], PERSONNEL_URL)
        bridge.acall.reset_mock()
        bridge.acall.return_value = {'confirmation_required': True, 'message': '确认查询 abcdef12'}
        result = await engine.answer(ADMIN, {'question': '在职人数', '_profile': holder.model.profile()},
            [], holder.request, AsyncMock(), AsyncMock(return_value=ADMIN), {})
        self.assertEqual(result['answer'], '确认查询 abcdef12')
        self.assertEqual(result['sources'], [])
        bridge.acall.assert_awaited_once_with('staff_count', {'scopes': ADMIN['scopes']}, None)


class BridgeReadTests(unittest.IsolatedAsyncioTestCase):
    async def test_roster_read_needs_module_scope_and_feishu_consent(self):
        from lan_bitable_template_portal.lighthouse_bridge import PortalAuthority
        from starlette.requests import Request
        store = Memory()
        authority = PortalAuthority(None, None, SimpleNamespace(state_store=store), SimpleNamespace(instance='i', lease='l'))
        actor = {**ADMIN, 'scopes': ['A'], 'channel': 'feishu:oc_private', 'modules': ['assistant', 'links']}
        request = Request({'type': 'http', 'state': {'feishu_question': '今天有多少人上班？'}})
        authority.authorize = AsyncMock(return_value=({'request': request}, actor))
        authority.tables = SimpleNamespace(roster_count=Mock(return_value={'assignment_count': 3}))
        message = {'instance': 'i', 'lease': 'l', 'context_id': 'trusted', 'action': 'roster_count',
                   'payload': {'scopes': ['A'], 'date': '2026-10-22', 'shift': ''}}
        result = await authority.dispatch(message)
        self.assertTrue(result['confirmation_required'])
        authority.tables.roster_count.assert_not_called()
        authority.read_consent.approve(actor, result['confirmation_code'])
        self.assertEqual((await authority.dispatch(message))['assignment_count'], 3)
        authority.tables.roster_count.assert_called_once_with(actor, '2026-10-22', '')
        with self.assertRaises(AssistantError): await authority.dispatch({**message, 'payload': {**message['payload'], 'scopes': ['B']}})
        authority.authorize.return_value = ({'request': request}, {**actor, 'modules': ['assistant']})
        with self.assertRaises(AssistantError): await authority.dispatch(message)

    async def test_channel_consent_blocks_read_before_data_is_loaded(self):
        from lan_bitable_template_portal.lighthouse_bridge import PortalAuthority
        from starlette.requests import Request
        store = Memory()
        runtime = SimpleNamespace(state_store=store)
        authority = PortalAuthority(None, None, runtime, SimpleNamespace(instance='i', lease='l'))
        actor = {**ADMIN, 'channel': 'feishu:oc_private', 'modules': ['assistant', 'links']}
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
