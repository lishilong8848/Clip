"""Isolated collaboration contracts: no real credentials, servers or Feishu writes."""
import asyncio
from contextlib import contextmanager
import json
from pathlib import Path
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import FastAPI
from starlette.requests import Request

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lan_bitable_template_portal import feishu_workpartner as wp
from lan_bitable_template_portal.feishu_workpartner_models import (MessageSend, TaskCreate, TaskUpdate,
    EventCreate, EventUpdate, EventAttendees, DocumentCreate, DocumentAppend, ResourceGrant, TableRecordWrite)
from lan_bitable_template_portal.feishu_workpartner_routes import install_feishu_workpartner_routes
from lan_bitable_template_portal.lighthouse_bridge import PortalAuthority
from lan_bitable_template_portal.state_store import LanPortalStateStore
from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_api import PortalAPICatalog

ALICE = {'id': 'ou_portalAlice', 'name': '测试甲', 'scopes': ['A'], 'modules': ['assistant'], 'is_admin': False}
BOB = {**ALICE, 'id': 'ou_portalBob', 'name': '测试乙'}
CHARLIE = {**ALICE, 'id': 'ou_portalCharlie', 'name': '测试丙', 'scopes': ['B']}
ADMIN = {**ALICE, 'is_admin': True, 'scopes': ['A', 'B']}


@contextmanager
def confirmed(actor=ALICE, step='plan:test:0'):
    token = wp.CONFIRMED_EXECUTION.set((actor['id'], step))
    try:
        yield
    finally:
        wp.CONFIRMED_EXECUTION.reset(token)


class WorkpartnerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = LanPortalStateStore(Path(self.temp.name) / 'state.sqlite3')
        self.requests = []
        self.response = None
        self.remote_revision = 4
        self.scopes = sorted(set().union(*wp.SCOPES.values()))
        self.http = httpx.AsyncClient(transport=httpx.MockTransport(self.handle))
        self.addAsyncCleanup(self.http.aclose)
        self.addCleanup(self.temp.cleanup)
        people = {rid: {'id': rid, 'name': person['name'], 'building': person['scopes'][0],
            'scopes': person['scopes'], 'employee_no': str(index), 'selectable': True,
            'login_ids': [person['id']]}
            for index, (rid, person) in enumerate(zip(['recAlice', 'recBob', 'recCharlie'], [ALICE, BOB, CHARLIE]))}
        self.auth = SimpleNamespace(_account_directory_provider=lambda: {'people': people, 'loaded_at': time.time()},
            _open_id_explicitly_disabled=lambda _: False)
        messenger = SimpleNamespace(client=AsyncMock(return_value=self.http),
            headers=AsyncMock(return_value={'Authorization': 'Bearer test-only'}),
            sender_record=AsyncMock(return_value=None),
            directory_records={f'ou_bot{name}': {f'rec{name}'} for name in ('Alice', 'Bob', 'Charlie')})
        self.channel = SimpleNamespace(config={'enabled': True, 'app_id': 'cli_testAgent'}, messenger=messenger)
        self.runtime = SimpleNamespace(state_store=self.store, auth_manager=self.auth)
        self.service = wp.Workpartner(self.runtime, self.channel)
        self.guard = patch('lan_bitable_template_portal.portal_service.external_real_write_guard',
                           return_value={'real_write_allowed': True})
        self.guard.start()
        self.addCleanup(self.guard.stop)

    def handle(self, request):
        self.requests.append(request)
        if self.response:
            result = self.response(request)
            if result is not None:
                return result
        path, method = request.url.path, request.method
        data = {}
        if path.endswith('/application/v6/scopes'):
            data = {'scopes': [{'scope_name': name, 'grant_status': 1} for name in self.scopes]}
        elif path.endswith('/im/v1/chats'):
            data = {'items': [{'chat_id': 'oc_test', 'name': '测试工作群'}], 'has_more': False}
        elif path.endswith('/members'):
            data = {'items': [{'member_id_type': 'open_id', 'member_id': 'ou_bot' + name}
                             for name in ('Alice', 'Bob')], 'has_more': False}
        elif '/im/v1/messages/' in path and method == 'GET':
            data = {'items': [{'chat_id': 'oc_test'}]}
        elif '/im/v1/messages' in path:
            data = {'message_id': 'om_receipt'}
        elif '/task/v2/tasks' in path:
            data = {'task': {'guid': 'task-guid', 'summary': '任务', 'completed_at': '0'}}
        elif path.endswith('/attendees'):
            body = json.loads(request.content)
            data = {'attendees': [{'attendee_id': str(i)} for i, _ in enumerate(body['attendees'])]}
        elif '/calendar/v4/' in path:
            if path.endswith('/events') and method == 'GET':
                data = {'items': [], 'has_more': False}
            else:
                data = {'event': {'event_id': 'event-test', 'start_time': {'timestamp': '1800000000'},
                                  'end_time': {'timestamp': '1800003600'}}}
        elif path.endswith('/raw_content'):
            data = {'content': '已授权正文'}
        elif '/docx/v1/' in path:
            if '/children' in path:
                data = {'children': json.loads(request.content)['children'], 'document_revision_id': 5}
            else:
                data = {'document': {'document_id': 'docTest', 'revision_id': self.remote_revision, 'title': '测试文档'}}
        elif path.endswith('/fields'):
            data = {'items': [{'field_name': name, 'type': kind} for name, kind in
                [('标题', 1), ('数量', 2), ('完成', 7), ('密码', 1)]], 'has_more': False}
        elif '/bitable/' in path:
            record = {'record_id': 'recResult', 'fields': {'标题': '原内容', '数量': 3, '密码': 'never-return-this'}}
            data = {'items': [record], 'has_more': False} if path.endswith('/records') and method == 'GET' else {'record': record}
        else:
            raise AssertionError(f'Unexpected isolated request {method} {path}')
        return httpx.Response(200, json={'code': 0, 'data': data})

    def writes(self):
        return [r for r in self.requests if r.method != 'GET']

    async def grant(self, kind='calendar', identity='cal_test', **extra):
        return await self.service.register(ADMIN, ResourceGrant(operation_id='register-test', kind=kind,
            resource_id=identity, name='已授权资源', building_scopes=['A'], writable=True, **extra))

    def message(self, **extra):
        return MessageSend(**{'operation_id': 'message-test', 'text': '确认后的消息', 'recipient': '我', **extra})

    async def test_capabilities_are_cached_without_credentials(self):
        first = await self.service.capabilities()
        self.assertTrue(all(row['enabled'] for row in first['items']))
        self.assertEqual(first, await self.service.capabilities())
        self.assertEqual(len(self.requests), 1)
        self.assertNotIn('Bearer', json.dumps(first))

    async def test_missing_permission_fails_before_mutation(self):
        self.scopes = []
        with self.assertRaisesRegex(AssistantError, '尚未获授'):
            await self.service.send(ALICE, self.message())
        self.assertFalse(self.writes())

    async def test_malformed_permissions_are_not_treated_as_full_authorization(self):
        self.response = lambda r: httpx.Response(200, json={'code': 0, 'data': {'scopes': [{'scope_name': 'im:message'}]}})
        with self.assertRaisesRegex(AssistantError, '未完整返回'):
            await self.service.capabilities()
        self.assertIsNone(self.service.permission_cache)

    async def test_wrong_bot_and_invalid_raw_api_path_are_blocked(self):
        with self.assertRaises(AssistantError):
            await self.service.call('GET', '//attacker.test/api')
        self.channel.config['app_id'] = 'cli_a9d1c9fe9d381cee'
        with self.assertRaisesRegex(AssistantError, '专用应用'):
            await self.service.capabilities()
        self.assertFalse(self.requests)

    async def test_people_exact_hr_identity_and_building_boundaries(self):
        self.assertEqual(len(await self.service.people(ALICE)), 2)
        self.assertEqual((await self.service.person(ALICE, '我'))['app_open_id'], 'ou_botAlice')
        with self.assertRaises(AssistantError):
            await self.service.person(ALICE, '测试丙')
        self.channel.messenger.directory_records['ou_duplicate'] = {'recAlice'}
        with self.assertRaisesRegex(AssistantError, '唯一关联'):
            await self.service.person(ALICE, '我')

    async def test_private_send_uses_agent_openid_and_idempotent_receipt(self):
        model = self.message()
        first, second = await asyncio.gather(self.service.send(ALICE, model), self.service.send(ALICE, model))
        self.assertEqual(first, second)
        self.assertEqual(len(self.writes()), 1)
        self.assertEqual(json.loads(self.writes()[0].content)['receive_id'], 'ou_botAlice')
        self.assertEqual(first['operation_id'], model.operation_id)
        restarted = wp.Workpartner(self.runtime, self.channel)
        self.assertEqual(await restarted.send(ALICE, model), first)
        self.assertEqual(len(self.writes()), 1)

    async def test_real_group_mentions_and_thread_reply(self):
        model = MessageSend(operation_id='group-message', text='请核对', chat_id='oc_test',
                            mention_names=['测试乙'], reply_message_id='om_original')
        await self.service.send(ALICE, model)
        body = json.loads(self.writes()[0].content)
        self.assertIn('<at user_id="ou_botBob">测试乙</at>', json.loads(body['content'])['text'])
        self.assertTrue(body['reply_in_thread'])
        self.assertTrue(str(self.writes()[0].url).endswith('/om_original/reply'))

    async def test_group_sender_and_original_message_are_checked(self):
        with self.assertRaisesRegex(AssistantError, '不是该群成员'):
            await self.service.send(CHARLIE, MessageSend(operation_id='wrong-group', text='消息', chat_id='oc_test'))
        with self.assertRaisesRegex(AssistantError, '不属于选中的群'):
            await self.service.send(ALICE, MessageSend(operation_id='wrong-thread', text='消息',
                                                       chat_id='oc_other', reply_message_id='om_original'))
        self.assertFalse(self.writes())

    async def test_group_discovery_only_returns_verified_membership(self):
        rows = await self.service.chats(ALICE, '测试')
        self.assertEqual(rows['items'], [{'chat_id': 'oc_test', 'name': '测试工作群'}])
        self.assertEqual((await self.service.chats(CHARLIE))['items'], [])
        self.assertFalse(self.writes())

    async def test_incomplete_group_members_never_allow_send(self):
        self.response = lambda r: httpx.Response(200, json={'code': 0, 'data': {'items': [], 'has_more': True}}) if r.url.path.endswith('/members') else None
        with self.assertRaisesRegex(AssistantError, '未完整读取'):
            await self.service.send(ALICE, MessageSend(operation_id='member-check', text='消息', chat_id='oc_test'))
        self.assertFalse(self.writes())

    async def test_timeout_is_uncertain_and_never_resent_after_restart(self):
        def timeout(request):
            if request.method == 'POST':
                raise httpx.ReadTimeout('private token must never surface', request=request)
        self.response = timeout
        model = self.message()
        with self.assertRaisesRegex(AssistantError, '结果尚未确认'):
            await self.service.send(ALICE, model)
        self.assertEqual(self.service.status(ALICE, model.operation_id)['status'], 'uncertain')
        restarted = wp.Workpartner(self.runtime, self.channel)
        with self.assertRaises(AssistantError):
            await restarted.send(ALICE, model)
        self.assertEqual(len(self.writes()), 1)
        self.assertNotIn('private token', json.dumps(self.service.status(ALICE, model.operation_id)))
        with self.assertRaises(AssistantError):
            self.service.status(BOB, model.operation_id)

    async def test_missing_success_receipt_is_not_success(self):
        self.response = lambda r: httpx.Response(200, json={'code': 0, 'data': {}}) if r.method == 'POST' else None
        with self.assertRaises(AssistantError):
            await self.service.send(ALICE, self.message())
        self.assertEqual(self.service.status(ALICE, 'message-test')['status'], 'uncertain')

    async def test_duplicate_inflight_response_cannot_trigger_duplicate_create(self):
        self.response = lambda r: httpx.Response(400, json={'code': 1254608}) if r.method != 'GET' else None
        model = self.message()
        for _ in range(2):
            with self.assertRaises(AssistantError):
                await self.service.send(ALICE, model)
        self.assertEqual(self.service.status(ALICE, model.operation_id)['status'], 'uncertain')
        self.assertEqual(len(self.writes()), 1)

    async def test_missing_document_receipt_remains_uncertain_not_recreated(self):
        self.response = lambda r: httpx.Response(200, json={'code': 0, 'data': {'document': {}}}) if r.method != 'GET' else None
        model = DocumentCreate(operation_id='lost-create', title='不重复创建')
        for _ in range(2):
            with self.assertRaises(AssistantError):
                await self.service.create_document(ALICE, model)
        self.assertEqual(len(self.writes()), 1)

    async def test_rejected_request_can_continue_same_operation(self):
        self.response = lambda r: httpx.Response(429) if r.method == 'POST' else None
        with self.assertRaises(AssistantError):
            await self.service.send(ALICE, self.message())
        self.assertEqual(self.service.status(ALICE, 'message-test')['status'], 'rejected')
        self.response = None
        self.assertTrue((await self.service.send(ALICE, self.message()))['sent'])

    async def test_sqlite_claim_is_atomic_across_service_instances(self):
        model = self.message()
        other = wp.Workpartner(self.runtime, self.channel)
        first, second = await asyncio.gather(asyncio.to_thread(self.service._claim, ALICE, 'send', model),
            asyncio.to_thread(other._claim, ALICE, 'send', model), return_exceptions=True)
        self.assertEqual(sum(isinstance(r, tuple) for r in (first, second)), 1)
        self.assertEqual(sum(isinstance(r, AssistantError) for r in (first, second)), 1)
        with self.assertRaisesRegex(AssistantError, '不同内容'):
            self.service._claim(ALICE, 'send', self.message(text='不同内容'))

    async def test_cancellation_does_not_mark_remote_mutation_failed(self):
        entered = asyncio.Event()
        async def pending():
            entered.set()
            await asyncio.Event().wait()
        task = asyncio.create_task(self.service.write(ALICE, 'cancel-test', self.message(), pending))
        await entered.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(self.service.status(ALICE, 'message-test')['status'], 'uncertain')

    async def test_task_creation_assignment_permissions_and_completion(self):
        model = TaskCreate(operation_id='create-task', summary='测试任务', assignees=['我', '我'], followers=['测试乙'])
        result = await self.service.create_task(ALICE, model)
        members = json.loads(self.writes()[0].content)['members']
        self.assertEqual(members, [{'id': 'ou_botAlice', 'type': 'user', 'role': 'assignee'},
                                   {'id': 'ou_botBob', 'type': 'user', 'role': 'follower'}])
        self.assertEqual((await self.service.task(BOB, result['task_id']))['guid'], 'task-guid')
        with self.assertRaises(AssistantError):
            await self.service.update_task(BOB, 'task-guid', TaskUpdate(operation_id='complete-task', completed=True))
        await self.service.update_task(ALICE, 'task-guid', TaskUpdate(operation_id='complete-task', completed=True))
        body = json.loads(self.writes()[-1].content)
        self.assertEqual(body['update_fields'], ['completed_at'])
        self.assertGreater(int(body['task']['completed_at']), 0)
        with self.assertRaises(AssistantError):
            await self.service.task(CHARLIE, 'task-guid')

    async def test_optional_blank_update_fields_do_not_clear_existing_task_values(self):
        self.service._created(ALICE, 'task', 'task-guid', '原任务')
        await self.service.update_task(ALICE, 'task-guid', TaskUpdate(operation_id='rename-task',
            summary='新名称', description=None, due_time=None, completed=None))
        self.assertEqual(json.loads(self.writes()[0].content), {'task': {'summary': '新名称'}, 'update_fields': ['summary']})

    async def test_resource_directory_has_real_pagination(self):
        for index in range(31):
            self.service._created(ALICE, 'task', f'task-{index}', f'任务{index}')
        first = await self.service.resources(ALICE, 'task')
        second = await self.service.resources(ALICE, 'task', page_number=2)
        self.assertEqual((len(first['items']), first['total'], first['has_more']), (30, 31, True))
        self.assertEqual((len(second['items']), second['has_more']), (1, False))
        self.assertFalse({row['resource_id'] for row in first['items']} & {row['resource_id'] for row in second['items']})

    async def test_resource_registration_is_scoped_and_revision_checked(self):
        saved = await self.grant()
        self.assertEqual(saved['revision'], 1)
        with self.assertRaises(AssistantError):
            await self.grant()
        with self.assertRaises(AssistantError):
            await self.service.register(ALICE, ResourceGrant(operation_id='not-admin', kind='calendar',
                resource_id='calOther', name='不可授权', building_scopes=['A']))
        with self.assertRaises(AssistantError):
            self.service.resource(CHARLIE, 'calendar', 'cal_test')
        self.assertEqual((await self.service.resources(ALICE))['total'], 1)
        await self.grant(expected_revision=1, enabled=False)
        self.assertEqual((await self.service.resources(ALICE))['total'], 0)
        self.assertEqual((await self.service.resources(ADMIN))['items'][0]['revision'], 2)

    async def test_personal_resource_cannot_be_granted_to_other_users_by_overwriting(self):
        self.service._created(ALICE, 'document', 'docPrivate', '私有文档')
        with self.assertRaises(AssistantError):
            self.service.resource(BOB, 'document', 'docPrivate')
        with self.assertRaisesRegex(AssistantError, '不能通过登记转换'):
            await self.grant('document', 'docPrivate', expected_revision=1)

    async def test_calendar_create_update_and_invitation_share_original_identity(self):
        await self.grant()
        create = EventCreate(operation_id='new-event', calendar_id='cal_test', summary='检查会议',
                             start_time='2026-10-16T09:00', end_time='2026-10-16T10:00')
        await self.service.write_event(ALICE, create)
        self.assertEqual(json.loads(self.writes()[0].content)['start_time']['timestamp'], str(wp.timestamp(create.start_time)))
        self.assertIn('idempotency_key', self.writes()[0].url.params)
        await self.service.write_event(ALICE, EventUpdate(operation_id='edit-event', calendar_id='cal_test', summary='新名称'), 'event-test')
        self.assertEqual(json.loads(self.writes()[-1].content), {'summary': '新名称'})
        await self.service.add_attendees(ALICE, 'event-test', EventAttendees(operation_id='invite-event',
            calendar_id='cal_test', attendees=['我', '测试乙']))
        invited = json.loads(self.writes()[-1].content)
        self.assertTrue(invited['need_notification'])
        self.assertEqual(invited['attendees'][1]['user_id'], 'ou_botBob')

    async def test_calendar_invalid_range_and_incomplete_list_are_not_empty(self):
        await self.grant()
        with self.assertRaises(AssistantError):
            await self.service.events(ALICE, 'cal_test', '2026-10-17', '2026-10-16')
        self.response = lambda r: httpx.Response(200, json={'code': 0, 'data': {}}) if '/events' in r.url.path else None
        with self.assertRaisesRegex(AssistantError, '未完整返回'):
            await self.service.events(ALICE, 'cal_test', '2026-10-16', '2026-10-17')

    async def test_renaming_all_day_event_does_not_overwrite_original_dates(self):
        await self.grant()
        self.response = lambda r: httpx.Response(200, json={'code': 0, 'data': {'event': {
            'event_id': 'event-test', 'start_time': {'date': '2026-10-16'}, 'end_time': {'date': '2026-10-17'}}}}) if r.method == 'GET' and r.url.path.endswith('/event-test') else None
        await self.service.write_event(ALICE, EventUpdate(operation_id='rename-all-day', calendar_id='cal_test', summary='全天检查'), 'event-test')
        self.assertEqual(json.loads(self.writes()[0].content), {'summary': '全天检查'})

    async def test_partial_calendar_invitation_is_uncertain(self):
        await self.grant()
        self.response = lambda r: httpx.Response(200, json={'code': 0, 'data': {'attendees': []}}) if r.method == 'POST' else None
        model = EventAttendees(operation_id='partial-invite', calendar_id='cal_test', attendees=['我'])
        with self.assertRaisesRegex(AssistantError, '回执不完整'):
            await self.service.add_attendees(ALICE, 'event-test', model)
        self.assertEqual(self.service.status(ALICE, model.operation_id)['status'], 'uncertain')

    async def test_document_revision_checks_and_replay_does_not_append_twice(self):
        await self.service.create_document(ALICE, DocumentCreate(operation_id='new-document', title='测试文档'))
        self.assertEqual((await self.service.document(ALICE, 'docTest'))['revision'], 4)
        with self.assertRaises(AssistantError):
            await self.service.document(BOB, 'docTest')
        model = DocumentAppend(operation_id='append-doc', text='第一段\n第二段', expected_revision=4)
        result = await self.service.append_document(ALICE, 'docTest', model)
        self.remote_revision = 5
        self.assertEqual(await self.service.append_document(ALICE, 'docTest', model), result)
        self.assertEqual(len([r for r in self.writes() if r.url.path.endswith('/children')]), 1)
        with self.assertRaisesRegex(AssistantError, '已被修改'):
            await self.service.append_document(ALICE, 'docTest', model.model_copy(update={'operation_id': 'new-append'}))

    async def test_long_document_can_be_read_completely_in_safe_chunks(self):
        self.service._created(ALICE, 'document', 'docTest', '长文档')
        text = '已核验正文' * 1500
        self.response = lambda r: httpx.Response(200, json={'code': 0, 'data': {'content': text}}) if r.url.path.endswith('/raw_content') else None
        first = await self.service.document(ALICE, 'docTest')
        self.assertEqual(len(first['content']), 6000)
        self.assertTrue(first['truncated'])
        last = await self.service.document(ALICE, 'docTest', first['next_offset'])
        self.assertEqual(first['content'] + last['content'], text)
        self.assertEqual(last['next_offset'], 0)

    async def test_next_page_without_cursor_is_an_error_not_a_complete_list(self):
        await self.grant()
        self.response = lambda r: httpx.Response(200, json={'code': 0, 'data': {'items': [], 'has_more': True}}) if '/events' in r.url.path else None
        with self.assertRaisesRegex(AssistantError, '分页未完整'):
            await self.service.events(ALICE, 'cal_test', '2026-10-16', '2026-10-17')

    async def test_table_sensitive_fields_and_original_business_tables_are_blocked(self):
        from lan_bitable_template_portal.portal_service import PERMISSION_DIRECTORY_TABLE_ID
        with self.assertRaisesRegex(AssistantError, '原灯塔业务表'):
            await self.grant('table', PERMISSION_DIRECTORY_TABLE_ID, base_id='baseTest')
        await self.grant('table', 'tblTest', base_id='baseTest')
        rows = await self.service.table_records(ALICE, 'tblTest')
        self.assertNotIn('never-return-this', json.dumps(rows))
        self.assertEqual(rows['items'][0]['version_id'], wp.reference({'标题': '原内容', '数量': 3}))
        with self.assertRaises(AssistantError):
            await self.service.write_record(ALICE, 'tblTest', TableRecordWrite(operation_id='sensitive-write',
                fields=[{'name': '密码', 'value': 'secret'}]))
        model = TableRecordWrite(operation_id='edit-record', fields=[{'name': '数量', 'value': 4}],
                                  expected_version_id=rows['items'][0]['version_id'])
        await self.service.write_record(ALICE, 'tblTest', model, 'recResult')
        self.assertEqual(json.loads(self.writes()[-1].content), {'fields': {'数量': 4}})
        with self.assertRaisesRegex(AssistantError, '版本'):
            await self.service.write_record(ALICE, 'tblTest', model.model_copy(update={
                'operation_id': 'changed-record', 'expected_version_id': '0' * 32}), 'recResult')

    async def test_editable_table_form_text_is_parsed_against_real_column_types(self):
        await self.grant('table', 'tblTest', base_id='baseTest')
        await self.service.write_record(ALICE, 'tblTest', TableRecordWrite(operation_id='typed-form',
            fields=[{'name': '数量', 'value': '4'}, {'name': '完成', 'value': 'false'}]))
        self.assertEqual(json.loads(self.writes()[0].content), {'fields': {'数量': 4, '完成': False}})
        with self.assertRaises(AssistantError):
            await self.service.write_record(ALICE, 'tblTest', TableRecordWrite(operation_id='bad-number',
                fields=[{'name': '数量', 'value': 'NaN'}]))
        self.assertEqual(len(self.writes()), 1)

    async def install(self):
        self.app = FastAPI()
        async def read_model(request, model):
            return model.model_validate(await request.json())
        self.controller = SimpleNamespace(_read_model_request=read_model,
            _request_base_url=lambda request: str(request.base_url).rstrip('/'))
        install_feishu_workpartner_routes(self.app, self.controller, self.runtime, self.channel)
        self.portal = httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app), base_url='http://portal.test')
        self.addAsyncCleanup(self.portal.aclose)

    async def test_routes_require_confirmed_plan_owner_and_same_origin(self):
        await self.install()
        with patch('lan_bitable_template_portal.feishu_workpartner_routes.actor_for', AsyncMock(return_value=ALICE)):
            kwargs = {'json': self.message().model_dump(), 'headers': {'origin': 'http://portal.test'}}
            response = await self.portal.post('/api/feishu-workpartner/messages/send', **kwargs)
            self.assertEqual(response.status_code, 403)
            with confirmed(BOB):
                self.assertEqual((await self.portal.post('/api/feishu-workpartner/messages/send', **kwargs)).status_code, 403)
            with confirmed():
                response = await self.portal.post('/api/feishu-workpartner/messages/send', **kwargs)
                self.assertTrue(response.json()['data']['sent'])
                expected = wp.reference(['ou_portalAlice', 'plan:test:0'])
                self.assertEqual(response.json()['data']['operation_id'], expected)
                kwargs['headers']['origin'] = 'https://untrusted.test'
                self.assertEqual((await self.portal.post('/api/feishu-workpartner/messages/send', **kwargs)).status_code, 403)
        self.assertEqual(len(self.writes()), 1)

    async def test_invalid_form_and_guest_are_blocked_before_external_requests(self):
        await self.install()
        with patch('lan_bitable_template_portal.feishu_workpartner_routes.actor_for', AsyncMock(return_value=ALICE)), confirmed():
            response = await self.portal.post('/api/feishu-workpartner/tasks', json={'operation_id': 'bad-form'},
                                              headers={'origin': 'http://portal.test'})
            self.assertEqual(response.status_code, 422)
            self.assertNotIn('成功', response.json()['error'])
        self.controller._current_session = lambda _: {'open_id': 'ou_guest', 'is_guest': True}
        self.assertEqual((await self.portal.get('/api/feishu-workpartner/capabilities')).status_code, 403)
        self.assertFalse(self.requests)

    async def test_public_people_result_does_not_expose_login_identifiers(self):
        await self.install()
        with patch('lan_bitable_template_portal.feishu_workpartner_routes.actor_for', AsyncMock(return_value=ALICE)):
            response = await self.portal.get('/api/feishu-workpartner/people')
        row = response.json()['data']['people'][0]
        self.assertEqual(set(row), {'id', 'name', 'employee_no', 'building'})
        self.assertNotIn('ou_', json.dumps(response.json()))

    async def test_resource_grant_form_requires_real_admin_confirmation_and_replays(self):
        await self.install()
        body = ResourceGrant(operation_id='grant-form', kind='calendar', resource_id='calForm',
            name='管理员明确授权', building_scopes=['A'], writable=True).model_dump()
        path, headers = '/api/feishu-workpartner/resource-grants', {'origin': 'http://portal.test'}
        with patch('lan_bitable_template_portal.feishu_workpartner_routes.actor_for', AsyncMock(return_value=ALICE)), confirmed():
            self.assertEqual((await self.portal.post(path, json=body, headers=headers)).status_code, 403)
        with patch('lan_bitable_template_portal.feishu_workpartner_routes.actor_for', AsyncMock(return_value=ADMIN)):
            self.assertEqual((await self.portal.post(path, json=body, headers=headers)).status_code, 403)
            with confirmed(ADMIN):
                first = await self.portal.post(path, json=body, headers=headers)
                self.assertEqual(first.status_code, 200, first.text)
                self.assertEqual(first.json(), (await self.portal.post(path, json=body, headers=headers)).json())
        catalog = PortalAPICatalog(self.app)
        normalized, missing = catalog.validate_operation({'api_id': 'POST ' + path, 'body': body})
        self.assertFalse(missing)
        self.assertEqual(normalized['body']['building_scopes'], ['A'])

    async def test_catalog_discovers_typed_forms_excludes_grants_and_keeps_patch_omissions(self):
        await self.install()
        catalog = PortalAPICatalog(self.app)
        desc = catalog.get('POST /api/feishu-workpartner/tasks')
        self.assertEqual(desc['group'], '飞书工作伙伴')
        self.assertFalse(desc['read_only'])
        self.assertIn('assignees', desc['schema']['body']['properties'])
        with self.assertRaises(AssistantError):
            catalog.get('POST /api/admin/feishu-workpartner/resources')
        normalized, missing = catalog.validate_operation({'api_id': 'PATCH /api/feishu-workpartner/tasks/{task_id}',
            'path_params': {'task_id': 'task-guid'}, 'body': {'operation_id': 'complete-task', 'completed': True}})
        self.assertFalse(missing)
        self.assertNotIn('due_time', normalized['body'])
        self.assertNotIn('description', normalized['body'])

    async def test_authority_propagates_confirmation_only_for_verified_plan(self):
        await self.install()
        authority = PortalAuthority(self.app, self.controller, self.runtime, SimpleNamespace(root=Path(self.temp.name)))
        authority.catalog = PortalAPICatalog(self.app)
        request = Request({'type': 'http', 'method': 'POST', 'path': '/', 'scheme': 'http',
            'server': ('portal.test', 80), 'headers': [(b'host', b'portal.test')]})
        payload = {'operation': {'api_id': 'POST /api/feishu-workpartner/messages/send',
            'body': self.message().model_dump()}, 'operation_id': 'plan:test:0'}
        context = {'request': request, 'plan_id': 'test'}
        with patch('lan_bitable_template_portal.feishu_workpartner_routes.actor_for', AsyncMock(return_value=ALICE)):
            response = await authority.invoke(context, ALICE, payload)
            self.assertTrue(response['ok'], response)
            self.assertIsNone(wp.CONFIRMED_EXECUTION.get())
            with self.assertRaises(AssistantError):
                await authority.invoke({'request': request}, ALICE, payload)
        self.assertEqual(len(self.writes()), 1)

    def test_packager_keeps_runtime_and_skill_but_excludes_test_file(self):
        import package_portable as package
        root = Path(__file__).resolve().parent.parent
        runtime_paths = ['bin/lan_bitable_template_portal/feishu_workpartner.py',
            'bin/lan_bitable_template_portal/feishu_workpartner_models.py',
            'bin/lan_bitable_template_portal/feishu_workpartner_routes.py',
            'bin/openclaw_service/assistant/openclaw/skills/feishu-workpartner/SKILL.md',
            'bin/openclaw_service/assistant/openclaw/skills/registry.json']
        for relative in runtime_paths:
            path = root / relative
            self.assertTrue(path.is_file())
            self.assertFalse(package._is_excluded(path, root=root, exclude_venv=True), relative)
        self.assertTrue(package._is_excluded(Path(__file__).resolve(), root=root, exclude_venv=True))

    def test_explicit_feishu_collaboration_is_not_routed_to_daily_checklists(self):
        from openclaw_service.assistant.lighthouse_model import workpartner_question, discover_for_question
        self.assertTrue(workpartner_question('今天还有多少飞书任务没完成？'))
        self.assertTrue(workpartner_question('在飞书中为我创建一个日程'))
        self.assertFalse(workpartner_question('今天还有多少未结束的维保通告？'))
        catalog = SimpleNamespace(discover=lambda **kw: {'total': 0, 'items': [], 'groups': [{'name': '飞书工作伙伴'}]})
        result = discover_for_question(catalog, '今天还有多少飞书任务没完成？', keyword='找不到的具体任务标题')
        self.assertIn('飞书工作伙伴', [row['name'] for row in result['groups']])

    def test_issued_references_survive_redaction_without_allowing_personal_phone_text(self):
        from openclaw_service.assistant.lighthouse_ai import safe_data
        task_id = '00000000-0000-4000-8000-13812345678a'
        value = safe_data({'task_id': task_id, 'content': '联系13812345678'})
        self.assertEqual(value['task_id'], task_id)
        self.assertNotIn('13812345678', value['content'])
        ref = wp.reference(['actor', 'plan:one:0'])
        self.assertEqual(safe_data({'operation_id': ref, 'version_id': ref})['version_id'], ref)
        self.assertEqual(wp.reference(['actor', 'plan:one:0']), ref)
        self.assertNotEqual(wp.reference(['other', 'plan:one:0']), ref)
        calendar_id = 'feishu.cn_test@group.calendar.feishu.cn'
        self.assertEqual(safe_data({'calendar_id': calendar_id})['calendar_id'], calendar_id)
        self.assertEqual(safe_data({'resource_id': calendar_id})['resource_id'], calendar_id)
        self.assertNotIn('someone@example.com', safe_data({'resource_id': 'someone@example.com'})['resource_id'])


if __name__ == '__main__':
    unittest.main()
