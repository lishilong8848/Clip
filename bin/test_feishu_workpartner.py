"""Isolated collaboration contracts: no real credentials, servers or Feishu writes."""
import asyncio
from contextlib import contextmanager
import copy
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
from upload_event_module.services import feishu_token_manager as qt_auth

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
        config = {'enabled': True, 'app_id': 'cli_testAgent', 'secret_cipher': 'fixture-cipher'}
        messenger.config = dict(config)
        self.channel = SimpleNamespace(config=config, messenger=messenger)
        self.qt_credentials = ('cli_testAgent', 'fixture-secret')
        self.bot_secret = 'fixture-secret'
        for setting in (
            patch.object(qt_auth, 'token_manager', SimpleNamespace(_credentials=lambda: self.qt_credentials)),
            patch('lan_bitable_template_portal.feishu_assistant.unprotect_key', side_effect=lambda _: self.bot_secret),
        ):
            setting.start()
            self.addCleanup(setting.stop)
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

    def reference_message(self, **extra):
        row = {'message_id': 'om_card', 'chat_id': 'oc_test', 'msg_type': 'interactive',
               'update_time': '1', 'body': {'content': json.dumps({'schema': '2.0',
                   'header': {'title': {'tag': 'plain_text', 'content': '测试任务'}},
                   'body': {'elements': [{'tag': 'markdown', 'content': '请核对最新进度'},
                       {'tag': 'person', 'user_id': 'ou_botBob'},
                       {'tag': 'button', 'text': {'tag': 'plain_text', 'content': '查看记录'},
                        'url': 'https://example.test/record', 'value': {'private': 'must-not-read'}}]}})}, **extra}
        self.response = lambda request: httpx.Response(200, json={'code': 0, 'data': {'items': [row]}}) if '/im/v1/messages/' in request.url.path else None
        route = {'app_id': 'cli_testAgent', 'chat_id': 'oc_test', 'chat_type': 'group',
                 'message_id': 'om_current', 'parent_id': 'om_card', 'root_id': 'om_root', 'sender_id': 'ou_botAlice'}
        return {**ALICE, 'channel': 'feishu:oc_test'}, route, row

    async def test_referenced_card_has_real_people_visible_content_and_no_hidden_actions(self):
        actor, route, _ = self.reference_message()
        result = await self.service.message_context(actor, route)
        self.assertEqual(result['message_id'], 'om_card')
        self.assertEqual(result['people'], [{'id': 'recBob', 'name': BOB['name'], 'employee_no': '1', 'building': 'A'}])
        self.assertIn('最新进度', result['content'])
        self.assertEqual(result['links'], ['https://example.test/record'])
        self.assertNotIn('must-not-read', json.dumps(result))
        self.assertNotIn('ou_botBob', json.dumps(result))
        request = next(r for r in self.requests if '/im/v1/messages/' in r.url.path)
        self.assertEqual(request.url.params['card_msg_content_type'], 'user_card_content')
        self.assertFalse(self.writes())

    async def test_message_context_is_bound_to_app_chat_sender_and_original_reference(self):
        actor, route, row = self.reference_message()
        for changed_actor, changed_route in ((ALICE, route), (actor, {**route, 'app_id': 'cli_other'}),
                (actor, {**route, 'sender_id': 'ou_botBob'})):
            with self.subTest(route=changed_route), self.assertRaises(AssistantError):
                await self.service.message_context(changed_actor, changed_route)
        self.assertFalse(any('/im/v1/messages/' in r.url.path for r in self.requests))
        row['chat_id'] = 'oc_foreign'
        with self.assertRaisesRegex(AssistantError, '不属于当前会话'):
            await self.service.message_context(actor, route)
        row['chat_id'], row['deleted'] = 'oc_test', True
        with self.assertRaisesRegex(AssistantError, '已撤回'):
            await self.service.message_context(actor, route)

    async def test_card_pages_are_versioned_and_cannot_mix_updated_messages(self):
        actor, route, row = self.reference_message(msg_type='text', body={'content': json.dumps({'text': '原文' * 2500})})
        first = await self.service.message_context(actor, route)
        second = await self.service.message_context(actor, route, offset=first['next_offset'], version=first['version'])
        self.assertEqual(first['content'] + second['content'], '原文' * 2500)
        self.assertFalse(second['has_more'])
        row['update_time'] = '2'
        with self.assertRaisesRegex(AssistantError, '已变化'):
            await self.service.message_context(actor, route, offset=4000, version=first['version'])

    async def test_unresolved_and_out_of_scope_card_people_are_not_guessed(self):
        actor, route, _ = self.reference_message(body={'content': json.dumps({'elements': [
            {'tag': 'person', 'user_id': 'ou_unknown'}, {'tag': 'person', 'user_id': 'ou_botCharlie'}]})})
        result = await self.service.message_context(actor, route)
        self.assertEqual(result['people'], [])
        self.assertEqual(result['unresolved_people'], 2)
        self.assertNotIn(CHARLIE['name'], json.dumps(result, ensure_ascii=False))

    async def test_reference_reads_need_group_scope_and_cannot_cross_app_rotation(self):
        actor, route, _ = self.reference_message()
        self.scopes.remove('im:message.group_msg')
        with self.assertRaisesRegex(AssistantError, '尚未获授'):
            await self.service.message_context(actor, route)
        self.assertFalse(any('/im/v1/messages/' in r.url.path for r in self.requests))
        self.scopes.append('im:message.group_msg')
        self.service.permission_cache = None
        original = self.response
        def rotate(request):
            result = original(request)
            if result:
                self.rotate_identity('app')
            return result
        self.response = rotate
        with self.assertRaises(AssistantError):
            await self.service.message_context(actor, route)

    async def test_empty_template_and_malformed_reference_are_not_successful_reads(self):
        actor, route, row = self.reference_message(body={'content': json.dumps({'type': 'template', 'data': {'value': 'hidden'}})})
        with self.assertRaisesRegex(AssistantError, '没有返回.*可读正文'):
            await self.service.message_context(actor, route)
        row['body']['content'] = 'not-json'
        with self.assertRaisesRegex(AssistantError, '格式无效'):
            await self.service.message_context(actor, route)

    def test_old_cards_post_mentions_table_numbers_and_media_keep_their_limits(self):
        result = wp.message_projection(json.dumps({'title': '旧卡片', 'elements': [[
            {'tag': 'text', 'text': '联系<at id="ou_botBob"></at>'},
            {'tag': 'table', 'rows': [{'数量': 3, '密码': 'must-not-read'}]},
            {'tag': 'img', 'img_key': 'private-image', 'alt': {'content': '设备照片'}}]]}))
        self.assertIn('3', result['text'])
        self.assertNotIn('must-not-read', result['text'])
        self.assertEqual(result['people'], {'ou_botBob'})
        self.assertEqual(result['unread_media'], ['img'])
        self.assertNotIn('private-image', json.dumps({**result, 'people': list(result['people'])}))
        huge = wp.message_projection(json.dumps({'elements': [{'text': '一行'}] * 4500}))
        self.assertTrue(huge['limited'])

    def rotate_identity(self, field):
        app, secret = self.qt_credentials
        if field == 'app':
            app = 'cli_fixtureNext'
        elif field == 'secret':
            secret = 'fixture-rotated-secret'
        self.qt_credentials = app, secret
        self.bot_secret = secret
        self.channel.config = {**self.channel.config, 'app_id': app,
                               'secret_cipher': 'fixture-rotated-cipher' if field == 'secret' else self.channel.config['secret_cipher']}
        messenger = SimpleNamespace(**vars(self.channel.messenger))
        messenger.config = dict(self.channel.config)
        messenger.headers = AsyncMock(return_value={'Authorization': 'Bearer fixture-rotated-token'})
        self.channel.messenger = messenger

    async def assert_rotation_after_claim(self, field):
        model = DocumentCreate(operation_id='rotation-after-claim', title='synthetic document')
        original_app = self.qt_credentials[0]
        original_claim = self.service._claim

        def claim(actor, action, model):
            value = original_claim(actor, action, model)
            self.rotate_identity(field)
            return value

        with patch.object(self.service, '_claim', side_effect=claim), self.assertRaises(AssistantError) as caught:
            await self.service.create_document(ALICE, model)
        self.assertEqual(caught.exception.category, 'feishu_workpartner_rejected')
        self.assertFalse(self.writes())
        original_key = 'operation:' + wp.digest([original_app, ALICE['id'], model.operation_id])
        saved = self.store.get_document(wp.NS, original_key)
        self.assertEqual((saved['app'], saved['status']), (original_app, 'rejected'))
        self.assertNotIn('fixture-secret', json.dumps(saved))
        self.assertNotIn('fixture-rotated-secret', json.dumps(saved))
        self.assertIsNone(wp._OPERATION.get())
        restarted = wp.Workpartner(self.runtime, self.channel)
        self.assertEqual(restarted.status(ALICE, model.operation_id)['status'], 'rejected')
        before = len(self.requests)
        if field == 'app':
            for service in (self.service, restarted):
                with self.assertRaises(AssistantError) as retry:
                    await service.create_document(ALICE, model)
                self.assertEqual(retry.exception.status, 409)
            self.assertEqual(len(self.requests), before)
            self.assertEqual(self.store.get_document(wp.NS, original_key), saved)
            self.assertEqual(len(self.store.list_documents(wp.NS)), 1)
        else:
            result = await restarted.create_document(ALICE, model)
            self.assertTrue(result['created'])
            self.assertEqual(len(self.writes()), 1)
            before = len(self.requests)
            self.assertEqual(await restarted.create_document(ALICE, model), result)
            self.assertEqual(len(self.requests), before)
            self.assertEqual(self.store.get_document(wp.NS, original_key)['status'], 'completed')

    async def test_application_rotation_after_claim_never_executes_under_new_application(self):
        await self.assert_rotation_after_claim('app')

    async def test_secret_rotation_after_claim_stops_then_safely_retries_same_application(self):
        await self.assert_rotation_after_claim('secret')

    async def test_bot_replacement_after_claim_stops_before_execute(self):
        await self.assert_rotation_after_claim('bot')

    async def test_rotation_while_getting_write_headers_stops_before_http(self):
        model = DocumentCreate(operation_id='rotation-in-headers', title='synthetic document')
        original_claim = self.service._claim

        async def headers():
            self.rotate_identity('secret')
            return {'Authorization': 'Bearer fixture-old-token'}

        def claim(actor, action, model):
            value = original_claim(actor, action, model)
            self.channel.messenger.headers = AsyncMock(side_effect=headers)
            return value

        with patch.object(self.service, '_claim', side_effect=claim), self.assertRaises(AssistantError):
            await self.service.create_document(ALICE, model)
        self.assertFalse(self.writes())
        self.assertEqual(self.service.status(ALICE, model.operation_id)['status'], 'rejected')

    async def assert_rotation_during_acknowledged_write(self, field):
        model = DocumentCreate(operation_id='rotation-with-receipt', title='synthetic document')
        original_app = self.qt_credentials[0]

        def acknowledge(request):
            if request.method == 'POST':
                self.rotate_identity(field)
                return httpx.Response(200, json={'code': 0, 'data': {'document': {'document_id': 'docOriginal'}}})

        self.response = acknowledge
        result = await self.service.create_document(ALICE, model)
        self.assertTrue(result['created'])
        original_key = 'operation:' + wp.digest([original_app, ALICE['id'], model.operation_id])
        receipt = self.store.get_document(wp.NS, original_key)
        self.assertEqual((receipt['status'], receipt['app'], receipt['result']), ('completed', original_app, result))
        resource_key = 'resource:' + wp.digest([original_app, 'document', 'docOriginal'])
        self.assertEqual(self.store.get_document(wp.NS, resource_key)['app'], original_app)
        before = len(self.requests)
        restarted = wp.Workpartner(self.runtime, self.channel)
        for service in (self.service, restarted):
            self.assertEqual(service.status(ALICE, model.operation_id)['result'], result)
            self.assertEqual(await service.create_document(ALICE, model), result)
        self.assertEqual(len(self.requests), before)
        self.assertEqual(len(self.writes()), 1)
        self.assertEqual(self.store.get_document(wp.NS, original_key), receipt)
        if field == 'app':
            new_key = 'operation:' + wp.digest([self.qt_credentials[0], ALICE['id'], model.operation_id])
            self.assertIsNone(self.store.get_document(wp.NS, new_key))
        self.assertIsNone(wp._OPERATION.get())

    async def test_success_receipt_survives_application_rotation_during_http(self):
        await self.assert_rotation_during_acknowledged_write('app')

    async def test_success_receipt_survives_secret_rotation_during_http(self):
        await self.assert_rotation_during_acknowledged_write('secret')

    async def test_completed_history_does_not_require_current_bot_or_credentials(self):
        model = self.message()
        result = await self.service.send(ALICE, model)
        before = len(self.requests)
        self.channel.messenger = None
        self.qt_credentials = ('', '')
        self.assertEqual(self.service.status(ALICE, model.operation_id)['result'], result)
        self.assertEqual(await self.service.send(ALICE, model), result)
        self.assertEqual(len(self.requests), before)

    async def assert_rotation_with_uncertain_result(self, field, *, incomplete=False):
        model = DocumentCreate(operation_id='rotation-unknown-result', title='synthetic document')
        original_app = self.qt_credentials[0]

        def fail(request):
            if request.method == 'POST':
                self.rotate_identity(field)
                if incomplete:
                    return httpx.Response(200, json={'code': 0, 'data': {}})
                raise httpx.ReadTimeout('fixture-private-detail', request=request)

        self.response = fail
        with self.assertRaises(AssistantError) as caught:
            await self.service.create_document(ALICE, model)
        self.assertEqual(caught.exception.category, 'feishu_workpartner_uncertain')
        original_key = 'operation:' + wp.digest([original_app, ALICE['id'], model.operation_id])
        receipt = self.store.get_document(wp.NS, original_key)
        self.assertEqual((receipt['app'], receipt['status']), (original_app, 'uncertain'))
        self.assertNotIn('fixture-private-detail', json.dumps(receipt))
        before = len(self.requests)
        restarted = wp.Workpartner(self.runtime, self.channel)
        for service in (self.service, restarted):
            self.assertEqual(service.status(ALICE, model.operation_id)['status'], 'uncertain')
            with self.assertRaises(AssistantError) as retry:
                await service.create_document(ALICE, model)
            self.assertEqual(retry.exception.category, 'feishu_workpartner_uncertain')
        self.assertEqual(len(self.requests), before)
        self.assertEqual(len(self.writes()), 1)
        self.assertEqual(self.store.get_document(wp.NS, original_key), receipt)
        if field == 'app':
            self.assertIsNone(self.store.get_document(wp.NS, 'operation:' + wp.digest(
                [self.qt_credentials[0], ALICE['id'], model.operation_id])))
        self.assertIsNone(wp._OPERATION.get())

    async def test_timeout_during_application_rotation_remains_original_uncertain_without_replay(self):
        await self.assert_rotation_with_uncertain_result('app')

    async def test_timeout_during_secret_rotation_remains_original_uncertain_without_replay(self):
        await self.assert_rotation_with_uncertain_result('secret')

    async def test_incomplete_receipt_during_application_rotation_is_not_success_or_retried(self):
        await self.assert_rotation_with_uncertain_result('app', incomplete=True)

    async def test_rotation_after_read_prevents_the_following_mutation(self):
        self.service._created(ALICE, 'document', 'docTest', 'synthetic document')
        model = DocumentAppend(operation_id='rotation-between-calls', text='original paragraph', expected_revision=4)

        def rotate_after_read(request):
            if request.url.path.endswith('/docTest') and request.method == 'GET':
                self.rotate_identity('app')
                return httpx.Response(200, json={'code': 0, 'data': {'document': {'document_id': 'docTest', 'revision_id': 4}}})

        self.response = rotate_after_read
        with self.assertRaises(AssistantError):
            await self.service.append_document(ALICE, 'docTest', model)
        self.assertFalse(self.writes())
        self.assertEqual(self.service.status(ALICE, model.operation_id)['status'], 'rejected')
        before = len(self.requests)
        with self.assertRaises(AssistantError) as caught:
            await self.service.append_document(ALICE, 'docTest', model)
        self.assertEqual(caught.exception.status, 409)
        self.assertEqual(len(self.requests), before)

    async def test_rotation_during_recipient_resolution_stops_before_claim_or_send(self):
        async def rotate_sender(*args):
            self.rotate_identity('app')
        self.channel.messenger.sender_record = AsyncMock(side_effect=rotate_sender)
        with self.assertRaises(AssistantError) as caught:
            await self.service.send(ALICE, self.message())
        self.assertEqual(caught.exception.category, 'feishu_workpartner_rejected')
        self.assertFalse(self.writes())
        self.assertFalse([row for row in self.store.list_documents(wp.NS) if row['key'].startswith('operation:')])
        self.assertIsNone(wp._OPERATION.get())

    async def test_cross_application_history_matches_exact_owner_operation_and_payload(self):
        model = DocumentCreate(operation_id='original-document-op', title='synthetic document')

        def receipt(request):
            if request.method == 'POST':
                return httpx.Response(200, json={'code': 0, 'data': {
                    'document': {'document_id': 'FixtureDoc' + str(len(self.writes()))}}})

        self.response = receipt
        original = await self.service.create_document(ALICE, model)
        self.rotate_identity('app')
        before = len(self.requests)
        with self.assertRaises(AssistantError) as caught:
            await self.service.create_document(ALICE, model.model_copy(update={'title': 'different payload'}))
        self.assertEqual(caught.exception.status, 409)
        self.assertEqual(len(self.requests), before)
        self.assertEqual(await self.service.create_document(ALICE, model), original)
        self.assertEqual(len(self.requests), before)
        other_operation = await self.service.create_document(ALICE, model.model_copy(update={'operation_id': 'another-document-op'}))
        other_owner = await self.service.create_document(BOB, model)
        self.assertEqual(len(self.writes()), 3)
        self.assertNotEqual(original['document_id'], other_operation['document_id'])
        self.assertNotEqual(original['document_id'], other_owner['document_id'])
        self.assertEqual(self.service.status(ALICE, model.operation_id)['result'], original)
        self.assertEqual(self.service.status(BOB, model.operation_id)['result'], other_owner)

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
        self.channel.config['app_id'] = 'cli_fixturePrevious'
        with self.assertRaisesRegex(AssistantError, 'Qt 设置中的飞书鉴权') as caught:
            await self.service.capabilities()
        self.assertEqual(caught.exception.status, 503)
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

    async def test_unknown_operation_history_stays_404_on_an_empty_store(self):
        with self.assertRaises(AssistantError) as caught:
            self.service.status(ALICE, 'unknown-operation')
        self.assertEqual(caught.exception.status, 404)
        self.assertFalse(self.requests)

    async def test_migration_blocked_operations_keep_history_without_claim_or_execute(self):
        model = self.message()
        key, original, _ = self.service._claim(ALICE, 'message.send', model)
        execute = AsyncMock(return_value={'sent': True})
        restarted = wp.Workpartner(self.runtime, self.channel)
        for marker in ({'blocked': True}, {'archived': True}, {'resume_allowed': False}):
            for status in ('rejected', 'completed'):
                with self.subTest(marker=marker, status=status):
                    saved = {**original, 'status': status, 'identity_migration': marker,
                             'result': {'message_id': 'om_OriginalReceipt'}, 'error': 'original history'}
                    self.store.put_document(wp.NS, key, saved)
                    before = copy.deepcopy(saved)
                    for service in (self.service, restarted):
                        with self.assertRaises(AssistantError) as caught:
                            await service.write(ALICE, 'message.send', model, execute)
                        self.assertEqual(caught.exception.status, 409)
                        self.assertEqual(caught.exception.category, 'feishu_workpartner_uncertain')
                        history = service.status(ALICE, model.operation_id)
                        self.assertEqual(history['status'], status)
                        self.assertEqual(history['result'], before['result'])
                    self.assertEqual(self.store.get_document(wp.NS, key), before)
        execute.assert_not_awaited()
        self.assertFalse(self.requests)

    async def test_actual_transformer_blocked_rejected_target_key_cannot_execute(self):
        from lan_bitable_template_portal.feishu_identity_documents import transform_document
        model = self.message()
        key, previous, _ = self.service._claim(ALICE, 'message.send', model)
        previous.update(owner=BOB['id'], app='cli_fixturePrevious', status='rejected',
                        result={'message_id': 'om_Historical'})
        identity, saved = transform_document(wp.NS, key, previous, {BOB['id']: ALICE['id']})
        self.assertTrue(saved['identity_migration']['blocked'])
        self.assertTrue(saved['identity_migration']['opaque_key'])
        self.store.put_document(wp.NS, identity, saved)
        execute = AsyncMock()
        with self.assertRaises(AssistantError) as caught:
            await self.service.write(ALICE, 'message.send', model, execute)
        self.assertEqual(caught.exception.status, 409)
        execute.assert_not_awaited()
        self.assertEqual(self.store.get_document(wp.NS, identity), saved)
        self.assertEqual(self.service.status(ALICE, model.operation_id)['result'], saved['result'])
        self.assertFalse(self.requests)

    async def test_opaque_blocked_key_remains_a_guard_for_exact_owner_and_operation(self):
        model = self.message()
        target, previous, _ = self.service._claim(ALICE, 'message.send', model)
        self.store.delete_document(wp.NS, target)
        identity = 'operation:' + 'f' * 64
        saved = {**previous, 'status': 'rejected', 'identity_migration': {'blocked': True, 'opaque_key': True},
                 'result': {'message_id': 'om_Historical'}}
        self.store.put_document(wp.NS, identity, saved)
        execute = AsyncMock(return_value={'sent': True})
        with self.assertRaises(AssistantError) as caught:
            await self.service.write(ALICE, 'message.send', model, execute)
        self.assertEqual(caught.exception.status, 409)
        execute.assert_not_awaited()
        self.assertIsNone(self.store.get_document(wp.NS, target))
        self.assertEqual(self.service.status(ALICE, model.operation_id)['result'], saved['result'])
        shadow = {**previous, 'status': 'rejected', 'result': {'message_id': 'om_Shadow'}}
        self.store.put_document(wp.NS, target, shadow)
        with self.assertRaises(AssistantError) as caught:
            await self.service.write(ALICE, 'message.send', model, execute)
        self.assertEqual(caught.exception.status, 409)
        execute.assert_not_awaited()
        self.assertEqual(self.store.get_document(wp.NS, target), shadow)
        self.assertEqual(self.service.status(ALICE, model.operation_id)['result'], saved['result'])
        with self.assertRaises(AssistantError):
            self.service.status(BOB, model.operation_id)
        self.assertTrue((await self.service.write(BOB, 'message.send', model, execute))['sent'])
        other = self.message(operation_id='different-operation')
        self.assertTrue((await self.service.write(ALICE, 'message.send', other, execute))['sent'])
        self.assertEqual(execute.await_count, 2)
        self.assertEqual(self.store.get_document(wp.NS, identity), saved)
        self.assertFalse(self.requests)

    async def test_opaque_blocked_ledger_without_operation_id_fails_closed_without_hiding_history(self):
        model = self.message()
        key, previous, _ = self.service._claim(ALICE, 'message.send', model)
        history = {**previous, 'status': 'completed', 'result': {'message_id': 'om_Original'}}
        self.store.put_document(wp.NS, key, history)
        unknown = {**previous, 'status': 'rejected', 'identity_migration': {'blocked': True, 'opaque_key': True}}
        execute = AsyncMock(return_value={'sent': True})
        for operation in (None, '', 7):
            with self.subTest(operation=operation):
                unknown['operation_id'] = operation
                if operation is None:
                    unknown.pop('operation_id')
                self.store.put_document(wp.NS, 'operation:' + 'f' * 64, unknown)
                with self.assertRaises(AssistantError) as caught:
                    await self.service.write(ALICE, 'message.send', self.message(operation_id='another-operation'), execute)
                self.assertEqual(caught.exception.status, 409)
                execute.assert_not_awaited()
                self.assertEqual(self.store.get_document(wp.NS, 'operation:' + 'f' * 64), unknown)
        self.assertEqual(self.service.status(ALICE, model.operation_id)['result'], history['result'])
        self.assertEqual(self.store.get_document(wp.NS, key), history)
        self.assertEqual(len(self.store.list_documents(wp.NS)), 2)
        self.assertTrue((await self.service.write(BOB, 'message.send', model, execute))['sent'])
        execute.assert_awaited_once()
        self.assertFalse(self.requests)

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

    async def test_current_message_reference_survives_internal_authority_without_model_ids(self):
        from lan_bitable_template_portal.feishu_assistant import FeishuAssistant
        actor = {**ALICE, 'channel': 'feishu:oc_test', 'learning_scopes': []}
        fake = SimpleNamespace(config={'app_id': 'cli_testAgent'},
            controller=SimpleNamespace(bound_port=18766), runtime=self.runtime,
            _require_current_config=AsyncMock(), _resolve_user=AsyncMock(return_value={'open_id': ALICE['id']}),
            _session=lambda _: 'test-session', _prepare_topic=AsyncMock())
        message = {'app_id': 'cli_testAgent', 'message_id': 'om_current', 'chat_id': 'oc_test', 'chat_type': 'group',
                   'parent_id': 'om_card', 'root_id': 'om_root', 'sender_id': 'ou_botAlice', 'question': '通知卡片中的人'}
        with patch('lan_bitable_template_portal.lighthouse_bridge.actor_for', AsyncMock(return_value=actor)):
            request, _ = await FeishuAssistant._request_context(fake, message)
            self.assertEqual(request.state.feishu_message_route['parent_id'], 'om_card')
            authority = PortalAuthority(FastAPI(), fake.controller, self.runtime,
                                        SimpleNamespace(root=Path(self.temp.name), lease='test'))
            _, current = await authority.authorize(authority.context(request, actor))
        self.assertEqual(current['_feishu_message_context'], {'has_reference': True, 'has_root': True, 'chat_type': 'group'})
        self.assertNotIn('om_card', json.dumps(current))

    async def test_original_confirmed_message_can_retry_rejection_but_not_duplicate_success(self):
        await self.install()
        authority = PortalAuthority(self.app, self.controller, self.runtime, SimpleNamespace(root=Path(self.temp.name)))
        authority.catalog = PortalAPICatalog(self.app)
        request = Request({'type': 'http', 'method': 'POST', 'path': '/', 'scheme': 'http',
            'server': ('portal.test', 80), 'headers': [(b'host', b'portal.test'), (b'origin', b'http://portal.test')]})
        payload = {'operation': {'api_id': 'POST /api/feishu-workpartner/messages/send',
            'body': self.message().model_dump()}, 'operation_id': 'plan:retry-test:0'}
        context = {'request': request, 'plan_id': 'retry-test'}
        self.response = lambda r: httpx.Response(400, json={'code': 230013}) if r.method == 'POST' else None
        with patch('lan_bitable_template_portal.feishu_workpartner_routes.actor_for', AsyncMock(return_value=ALICE)):
            failed = await authority.invoke(context, ALICE, payload)
            self.assertFalse(failed['ok'])
            self.response = None
            sent = await authority.invoke(context, ALICE, payload)
            self.assertTrue(sent['ok'], sent)
            self.assertEqual(await authority.invoke(context, ALICE, payload), sent)
        self.assertEqual(len(self.writes()), 2)
        self.assertEqual(json.loads(self.writes()[0].content)['uuid'], json.loads(self.writes()[1].content)['uuid'])

    async def test_message_context_route_has_no_arbitrary_message_id_and_all_five_groups_are_discoverable(self):
        await self.install()
        catalog = PortalAPICatalog(self.app)
        descriptor = catalog.get('GET /api/feishu-workpartner/messages/context')
        self.assertTrue(descriptor['read_only'])
        self.assertIn('读取', descriptor['name'])
        self.assertNotIn('message_id', json.dumps(descriptor['schema']))
        for endpoint in ('messages/send', 'tasks', 'events', 'documents', 'tables/{table_id}/records'):
            self.assertEqual(catalog.get('POST /api/feishu-workpartner/' + endpoint)['group'], '飞书工作伙伴')
        with patch('lan_bitable_template_portal.feishu_workpartner_routes.actor_for', AsyncMock(return_value=ALICE)):
            result = await self.portal.get('/api/feishu-workpartner/messages/context?message_id=om_foreign')
        self.assertEqual(result.status_code, 403)
        self.assertFalse(self.writes())

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


class WorkpartnerPlanTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        from test_lighthouse_agent import AgentTests
        AgentTests.setUp(self)
        install_feishu_workpartner_routes(self.catalog.app, SimpleNamespace(), None, None)
        self.catalog = self.agent.catalog = PortalAPICatalog(self.catalog.app)

    def plan(self, recipients=('测试乙', '测试甲'), **extra):
        return self.agent.prepare(ALICE, {'title': '通知本卡片涉及人员', 'operations': [
            {'api_id': 'POST /api/feishu-workpartner/messages/send', 'body': {'recipient': name, 'text': '请核对进度'}}
            for name in recipients]}, 'card-notification', [], queries={'query_card': {'people': [
                {'id': 'recAlice', 'name': ALICE['name'], 'employee_no': '0'},
                {'id': 'recBob', 'name': BOB['name'], 'employee_no': '1'}]}},
            question='给卡片涉及到的人发送消息，请核对进度', **extra)

    def test_card_people_prefill_stable_identity_and_human_preview_still_requires_confirmation(self):
        plan = self.plan()
        self.assertEqual(plan['status'], 'awaiting_confirmation')
        self.assertEqual([op['body']['recipient'] for op in plan['operations']], ['recBob', 'recAlice'])
        public = self.agent.public_plan(plan, ALICE)
        self.assertIn('测试乙', public['operations'][0]['selected_labels']['recipient'])
        self.assertIn('工号 1', public['operations'][0]['selected_labels']['recipient'])
        self.assertEqual(plan['results'], [])
        self.assertEqual(self.writes, [])

    def test_editing_recipient_updates_identity_and_preview_together(self):
        plan = self.plan(('测试乙',))
        edited = self.agent.amend(ALICE, plan['id'], {'action': 'edit', 'version': plan['version']})
        recipient = next(field for field in edited['fields'] if field['path'] == 'recipient')
        self.assertEqual(recipient['type'], 'select')
        values = {field['name']: field['value'] for field in edited['fields']}
        values[recipient['name']] = 'recAlice'
        updated = self.agent.amend(ALICE, plan['id'], {'version': edited['version'], 'values': values})
        self.assertEqual(updated['operations'][0]['body']['recipient'], 'recAlice')
        self.assertIn('测试甲', updated['operations'][0]['selected_labels']['recipient'])
        self.assertNotIn('测试乙', updated['operations'][0]['selected_labels']['recipient'])
        self.assertEqual(updated['status'], 'awaiting_confirmation')

    async def test_one_rejection_does_not_block_others_and_retry_keeps_success_receipts(self):
        plan = self.plan()
        attempts = []
        async def invoke(actor, op, request, **kwargs):
            target = op['body']['recipient']
            attempts.append(target)
            failed = target == 'recBob' and attempts.count(target) == 1
            return {'ok': not failed, 'api_id': op['api_id'], 'status': 400 if failed else 200,
                    'error': '飞书拒绝发送' if failed else '', 'data': {} if failed else {'message_id': 'om_' + target, 'sent': True}}
        self.agent._invoke = AsyncMock(side_effect=invoke)
        await self.agent._execute(ALICE, plan, self.request)
        self.assertEqual(attempts, ['recBob', 'recAlice'])
        self.assertEqual(plan['status'], 'failed')
        self.assertTrue(self.agent.public_plan(plan, ALICE)['can_retry'])
        receipt = copy.deepcopy(plan['results'][1])
        with patch.object(self.agent, '_schedule_execution'):
            await self.agent.retry_notice(ALICE, plan['id'], {'version': plan['version']}, self.request)
        resumed = self.agent.get_plan(ALICE, plan['id'])
        self.assertNotIn('retry_attempt', resumed['operations'][0]['body'])
        await self.agent._execute(ALICE, resumed, self.request)
        self.assertEqual(resumed['status'], 'completed')
        self.assertEqual(attempts, ['recBob', 'recAlice', 'recBob'])
        self.assertEqual(resumed['results'][1], receipt)

    async def test_duplicate_same_recipient_and_text_reuses_receipt_without_second_send(self):
        plan = self.plan(('测试乙', '1'))
        self.agent._invoke = AsyncMock(return_value={'ok': True, 'status': 200,
            'api_id': 'POST /api/feishu-workpartner/messages/send', 'data': {'message_id': 'om_once', 'sent': True}})
        await self.agent._execute(ALICE, plan, self.request)
        self.assertEqual(plan['status'], 'completed')
        self.assertEqual(self.agent._invoke.await_count, 1)
        self.assertEqual(plan['results'][0], plan['results'][1])

    def test_conflicting_names_need_selection_and_dependent_or_mixed_writes_are_not_auto_continued(self):
        query = {'people': [{'id': 'recOne', 'name': '重名', 'employee_no': '10'},
                            {'id': 'recTwo', 'name': '重名', 'employee_no': '11'}]}
        with self.assertRaisesRegex(AssistantError, '重名'):
            self.agent.prepare(ALICE, {'operations': [{'api_id': 'POST /api/feishu-workpartner/messages/send',
                'body': {'recipient': '重名', 'text': '正文'}}]}, 'ambiguous', [], queries={'query_people': query})
        self.assertFalse(self.agent._independent_messages({'operations': [
            {'api_id': 'POST /api/feishu-workpartner/tasks', 'body': {'summary': '任务'}}]}))
        self.assertFalse(self.agent._independent_messages({'operations': [
            {'api_id': 'POST /api/feishu-workpartner/messages/send', 'body': {'recipient': 'recBob', 'text': {'$result': {'step': 0, 'path': 'title'}}}}]}))

    def test_combined_business_query_and_collaboration_keep_both_instructions(self):
        from openclaw_service.assistant.lighthouse_model import instructions_for_question
        instructions = instructions_for_question('查询未结束的维保通告，给相关人员发送飞书消息')
        self.assertIn('飞书工作伙伴', instructions)
        self.assertIn('通告原文先用', instructions)
        self.assertIn('任务、日程', instructions)


if __name__ == '__main__':
    unittest.main()
