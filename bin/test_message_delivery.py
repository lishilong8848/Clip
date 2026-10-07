"""Delivery checks use a temporary store and transport; never send real messages."""
import asyncio
import copy
import json
from pathlib import Path
from contextlib import asynccontextmanager
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
from pydantic_ai.models.function import DeltaToolCall, FunctionModel
import httpx
from lan_bitable_template_portal.message_delivery import MessageDelivery, DeliveryError, NS
from lan_bitable_template_portal.message_delivery_routes import install_message_delivery_routes
from lan_bitable_template_portal.state_store import LanPortalStateStore
from lan_bitable_template_portal.portal_service import MaintenancePortalService, SIGNATURE_NAME_FIELD, SIGNATURE_INACTIVE_FIELD
from openclaw_service.assistant.lighthouse_agent import PortalAgent
from openclaw_service.assistant.lighthouse_api import PortalAPICatalog
from openclaw_service.assistant.lighthouse_ai import LighthouseAssistant, AssistantError
from openclaw_service.assistant.lighthouse_files import LighthouseFiles
from openclaw_service.assistant.lighthouse_model import LighthouseModel

PEOPLE = [
    {'record_id': 'rec-a', 'name': '测试甲', 'employee_no': '1001', 'open_id': 'ou_a', 'account_nature': 'VNET', 'can_receive_message': True},
    {'record_id': 'rec-b', 'name': '测试乙', 'employee_no': '1002', 'open_id': 'ou_b', 'account_nature': 'VNET', 'can_receive_message': True},
    {'record_id': 'rec-external', 'name': '外部人员', 'employee_no': '1003', 'open_id': 'ou_external', 'account_nature': '外部', 'can_receive_message': False},
]


def fake_service(store):
    fields = {p['record_id']: {SIGNATURE_NAME_FIELD: p['name'], '员工工号': p['employee_no'], 'openid': p['open_id'], '账号性质': p['account_nature']} for p in PEOPLE}
    return SimpleNamespace(_state_store=store, _signature_people_cache=None,
        _load_signature_people=Mock(return_value=copy.deepcopy(PEOPLE)),
        signature_management=SimpleNamespace(_fields=Mock(side_effect=lambda source, rid: fields[rid])), fields=fields,
        _signature_user_info=MaintenancePortalService._signature_user_info,
        _signature_open_id_from_any=MaintenancePortalService._signature_open_id_from_any,
        _signature_person_inactive=MaintenancePortalService._signature_person_inactive,
        _mop_field_text=MaintenancePortalService._mop_field_text)


class DeliveryTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.store = LanPortalStateStore(Path(temp.name) / 'state.sqlite3')
        self.service = fake_service(self.store)
        self.transport = SimpleNamespace(upload=Mock(return_value='file-private'), send=Mock(return_value='message-id'))
        self.delivery = MessageDelivery(self.service, self.transport)
        self.payload = {'operation_id': 'delivery-fixture-1', 'recipient_ids': ['__self__'], 'text': '完整消息'}

    def test_directory_search_uses_one_load_and_survives_restart(self):
        for _ in range(20):
            result = self.delivery.recipients('ou_a', '测试甲 1001')
            self.assertEqual([p['record_id'] for p in result['people']], ['rec-a'])
            self.assertNotIn('open_id', json.dumps(result))
        MessageDelivery(self.service, self.transport).recipients('ou_a')
        self.service._load_signature_people.assert_called_once_with(force=False)
        self.assertNotIn('rec-external', str(self.delivery.recipients('ou_a')))

    def test_text_and_files_use_self_identity_and_dedupe(self):
        job = self.delivery.prepare('ou_a', self.payload, [('proof.pdf', b'%PDF-fixture')])
        self.delivery.run('ou_a', job['delivery_id'])
        self.assertEqual(self.delivery.get('ou_a', job['delivery_id'])['status'], 'completed')
        self.assertEqual(self.transport.send.call_count, 2)
        self.assertEqual(self.transport.send.call_args_list[0].args[0], 'ou_a')
        self.assertEqual(self.transport.send.call_args_list[0].args[2], {'text': '完整消息'})
        self.assertEqual(self.transport.send.call_args_list[1].args[2], {'file_key': 'file-private'})
        self.delivery.prepare('ou_a', self.payload, [('proof.pdf', b'%PDF-fixture')])
        self.delivery.run('ou_a', job['delivery_id'])
        self.assertEqual(self.transport.send.call_count, 2)
        self.service.signature_management._fields.assert_called_once_with('staff', 'rec-a')
        self.service._load_signature_people.assert_called_once()

    def test_external_unknown_and_other_owner_are_blocked(self):
        for selected in ('rec-external', 'ou_a', '测试甲'):
            with self.assertRaises(DeliveryError):
                self.delivery.prepare('ou_a', {**self.payload, 'recipient_ids': [selected]}, [])
        job = self.delivery.prepare('ou_a', self.payload, [])
        with self.assertRaises(DeliveryError):
            self.delivery.get('ou_b', job['delivery_id'])
        self.transport.send.assert_not_called()

    def test_qualification_rechecked_for_selected_person_without_reloading_all(self):
        job = self.delivery.prepare('ou_a', self.payload, [])
        self.service.fields['rec-a'][SIGNATURE_INACTIVE_FIELD] = True
        self.delivery.run('ou_a', job['delivery_id'])
        self.assertEqual(self.delivery.get('ou_a', job['delivery_id'])['status'], 'failed')
        self.transport.send.assert_not_called()
        self.service._load_signature_people.assert_called_once()

    def test_partial_retry_preserves_uploaded_key_and_successful_parts(self):
        self.transport.send.side_effect = ['message-text', TimeoutError(), 'message-file']
        job = self.delivery.prepare('ou_a', self.payload, [('proof.pdf', b'file')])
        self.delivery.run('ou_a', job['delivery_id'])
        self.assertEqual(self.delivery.get('ou_a', job['delivery_id'])['sent_count'], 1)
        restarted = MessageDelivery(self.service, self.transport)
        restarted.run('ou_a', job['delivery_id'])
        self.assertEqual(restarted.get('ou_a', job['delivery_id'])['status'], 'completed')
        self.transport.upload.assert_called_once()
        self.assertEqual(self.transport.send.call_count, 3)
        self.assertEqual(self.transport.send.call_args_list[1].args[-1], self.transport.send.call_args_list[2].args[-1])

    def test_unknown_result_past_dedupe_window_is_not_resent(self):
        self.delivery.clock = Mock(return_value=1000)
        self.transport.send.side_effect = TimeoutError()
        job = self.delivery.prepare('ou_a', self.payload, [])
        self.delivery.run('ou_a', job['delivery_id'])
        self.delivery.clock.return_value = 5000
        self.delivery.run('ou_a', job['delivery_id'])
        self.assertIn('去重时限', self.delivery.get('ou_a', job['delivery_id'])['error'])
        self.transport.send.assert_called_once()

    def test_changed_payload_same_operation_and_blank_content_rejected(self):
        self.delivery.prepare('ou_a', self.payload, [])
        with self.assertRaises(DeliveryError):
            self.delivery.prepare('ou_a', {**self.payload, 'text': 'changed'}, [])
        with self.assertRaises(DeliveryError):
            self.delivery.prepare('ou_a', {**self.payload, 'text': ''}, [])

    def test_upload_failure_sends_nothing_and_retries_original_job(self):
        self.transport.upload.side_effect = [TimeoutError(), 'file-ready']
        job = self.delivery.prepare('ou_a', self.payload, [('proof.pdf', b'file')])
        self.delivery.run('ou_a', job['delivery_id'])
        self.transport.send.assert_not_called()
        self.assertEqual(self.delivery.get('ou_a', job['delivery_id'])['status'], 'failed')
        self.delivery.run('ou_a', job['delivery_id'])
        self.assertEqual(self.delivery.get('ou_a', job['delivery_id'])['status'], 'completed')


class MessageWorkflowTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.store = LanPortalStateStore(Path(temp.name) / 'state.sqlite3')
        self.service = fake_service(self.store)
        self.actor = {'id': 'ou_a', 'scopes': ['A'], 'is_admin': False}
        controller = SimpleNamespace(_current_session=lambda request: {'open_id': 'ou_a'},
            _json_ok=lambda request, session, data: JSONResponse({'ok': True, 'data': data}),
            _submit_background=Mock(return_value=True))
        async def read_model(request, model):
            return model.model_validate(await request.json())
        controller._read_model_request = read_model
        app = FastAPI()
        install_message_delivery_routes(app, controller, SimpleNamespace(service=self.service, auth_manager=SimpleNamespace(session_scopes=lambda session: ['A'])))
        self.controller = controller
        self.app = app
        self.catalog = PortalAPICatalog(app)
        self.assistant = LighthouseAssistant(self.store, Mock())
        self.files = LighthouseFiles(self.store)
        self.agent = PortalAgent(self.assistant, self.catalog, self.files)
        self.request = Request({'type': 'http', 'scheme': 'http', 'server': ('testserver', 80), 'client': ('127.0.0.1', 1),
            'path': '/api/assistant/messages', 'root_path': '', 'query_string': b'', 'headers': [(b'origin', b'http://testserver')]})

    async def test_native_schema_and_text_only_submission(self):
        descriptor = self.catalog.get('POST /api/message-delivery/send')
        self.assertIn('recipient_ids', descriptor['schema']['body']['properties'])
        self.assertEqual(descriptor['file_required'], {'files': False})
        plan = self.agent.prepare(self.actor, {'operations': [{'api_id': descriptor['id'], 'body': {'recipient_ids': ['__self__'], 'text': '完整文字'}}]}, 'test_message_00001', [])
        self.assertEqual(plan['status'], 'needs_input')
        ready = self.agent.amend(self.actor, plan['id'], {'version': plan['version'], 'values': {'step0.recipient_ids': ['__self__']}})
        op = self.agent.get_plan(self.actor, plan['id'])['operations'][0]
        self.assertEqual(op['body']['text'], '完整文字')
        result = await self.agent._invoke(self.actor, op, self.request, uploads=True)
        self.assertTrue(result['ok'], result)
        self.assertIn('delivery_id', result['data'])
        self.controller._submit_background.assert_called_once()

    async def test_older_content_and_file_multiselect_preserve_full_text(self):
        state = self.assistant._state(self.actor)
        state['turns'] = [{'operation_id': 'older', 'status': 'completed', 'answer': '较早的完整答复', 'question': '先前报告', 'scopes': ['A'], 'at': 1000},
                          {'operation_id': 'newer', 'status': 'completed', 'answer': '最新的无关答复', 'question': '天气', 'scopes': ['A'], 'at': 2000}]
        self.assistant._save_state(self.actor, state)
        file = self.files.upload(self.actor, '旧报告.txt', b'report', extract=False)
        plan = self.agent.prepare(self.actor, {'operations': [{'api_id': 'POST /api/message-delivery/send', 'body': {'recipient_ids': ['__self__']}}]}, 'test_message_00002', [file['id']])
        content = next(f for f in plan['fields'] if f.get('native_message_content'))
        self.assertIn('turn_older', content['_contents'])
        self.assertEqual(content['value'], [])
        self.agent.amend(self.actor, plan['id'], {'version': plan['version'], 'values': {'step0.recipient_ids': ['__self__'], content['name']: ['turn_older', 'file_' + file['id']]}})
        op = self.agent.get_plan(self.actor, plan['id'])['operations'][0]
        self.assertEqual(op['body']['text'], '较早的完整答复')
        self.assertEqual(op['files']['files'], [file['id']])
        result = await self.agent._invoke(self.actor, op, self.request, uploads=True)
        self.assertTrue(result['ok'], result)
        self.assertEqual(result['data']['files'][0]['name'], '旧报告.txt')

    async def test_explicit_text_and_verified_recipient_need_only_confirmation(self):
        state = self.assistant._state(self.actor)
        state['turns'] = [{'operation_id': 'weather', 'status': 'completed', 'question': '天气',
                           'answer': '与消息无关的天气答复', 'scopes': ['A'], 'at': 1000}]
        self.assistant._save_state(self.actor, state)
        people = MessageDelivery(self.service).recipients('ou_a')
        for recipient, name in [('__self__', '测试甲'), ('rec-b', '测试乙'), ('__self__', '自己')]:
            with self.subTest(recipient=recipient):
                question = f'发给{name}工号' + ('1001' if recipient == '__self__' else '1002') + '一条消息:\n这是一条测试消息,接收到了吗?'
                if name == '自己':
                    question = '发给自己：\n这是一条测试消息,接收到了吗?'
                text = question.split('\n')[1]
                plan = self.agent.prepare(self.actor, {'operations': [{'api_id': 'POST /api/message-delivery/send',
                    'body': {'recipient_ids': [recipient], 'text': text}}]}, 'message_complete_1', [], queries={'people': people}, question=question)
                self.assertEqual(plan['status'], 'awaiting_confirmation')
                self.assertEqual(plan['fields'], [])
                self.assertEqual(plan['operations'][0]['body']['text'], text)
                public = self.agent.public_plan(plan, self.actor)
                self.assertIn('测试甲' if recipient == '__self__' else name, public['operations'][0]['selected_labels']['recipient_names'])
                self.assertNotIn('天气', json.dumps(public, ensure_ascii=False))
                self.assertNotIn('multiselect', json.dumps(public))
                self.assertTrue(public['can_edit'])
                edited = self.agent.amend(self.actor, plan['id'], {'action': 'edit', 'version': plan['version']})
                self.assertEqual([field['path'] for field in edited['fields']], ['recipient_ids', 'text'])
                self.assertEqual(edited['fields'][1]['value'], text)
                self.agent.amend(self.actor, plan['id'], {'version': edited['version'], 'values': {'step0.recipient_ids': [recipient], 'step0.text': '修改后正文'}})
                saved = self.agent.get_plan(self.actor, plan['id'])
                self.assertEqual(saved['operations'][0]['body']['text'], '修改后正文')
        self.controller._submit_background.assert_not_called()

    async def test_duplicate_name_and_number_require_recipient_choice(self):
        people = MessageDelivery(self.service).recipients('ou_a')
        people['people'].append({**people['people'][1], 'record_id': 'rec-duplicate'})
        plan = self.agent.prepare(self.actor, {'operations': [{'api_id': 'POST /api/message-delivery/send',
            'body': {'recipient_ids': ['rec-b'], 'text': '完整正文'}}]}, 'message_duplicate', [], queries={'people': people},
            question='发给测试乙工号1002一条消息：完整正文')
        self.assertEqual(plan['status'], 'needs_input')
        self.assertEqual(plan['operations'][0]['body']['text'], '完整正文')
        self.controller._submit_background.assert_not_called()

    async def test_explicit_message_does_not_silently_drop_unknown_recipient(self):
        people = MessageDelivery(self.service).recipients('ou_a')
        for chosen, queries in [(['__self__'], {}), (['rec-b', 'unknown'], {'people': people})]:
            plan = self.agent.prepare(self.actor, {'operations': [{'api_id': 'POST /api/message-delivery/send',
                'body': {'recipient_ids': chosen, 'text': '待发文字'}}]}, 'message_incomplete', [], queries=queries, question='发给他们：待发文字')
            self.assertEqual(plan['status'], 'needs_input')
            self.assertFalse(any(field.get('native_message_content') for field in plan['fields']))
            self.assertEqual(next(field for field in plan['fields'] if field['path'] == 'text')['value'], '待发文字')
        self.controller._submit_background.assert_not_called()

    async def test_no_content_is_not_replaced_with_latest_reply_and_no_button(self):
        plan = self.agent.prepare(self.actor, {'operations': [{'api_id': 'POST /api/message-delivery/send', 'body': {}}]}, 'test_message_00003', [])
        with self.assertRaises(AssistantError):
            self.agent.amend(self.actor, plan['id'], {'version': plan['version'], 'values': {'step0.recipient_ids': ['__self__']}})
        vue = (Path(__file__).parent / 'lan_bitable_template_portal/frontend/src/components/LighthouseAssistant.vue').read_text(encoding='utf-8')
        self.assertNotIn('forwardTurn', vue)

    async def test_archived_topic_search_keeps_full_answer_and_filters_other_scope(self):
        from openclaw_service.assistant.lighthouse_model import instructions_for_question
        self.assertIn('发送内容不一定是上一条', instructions_for_question('把前面的内容发给我'))
        self.assertIn('/api/message-delivery/send', instructions_for_question('把前面的天气发给我'))
        state = self.assistant._state(self.actor)
        full_text = '旧报告正文。' * 2000
        for identity, scope in [('older-report', 'A'), ('hidden-report', 'B')]:
            self.store.put_document('lighthouse_messages', state['id'] + ':' + identity,
                {'operation_id': identity, 'question': '上周巡检报告', 'answer': full_text,
                 'status': 'completed', 'scopes': [scope], 'at': 1000})
        state['turns'] = [{'operation_id': 'latest', 'question': '天气', 'answer': '最新无关答复',
                           'status': 'completed', 'scopes': ['A'], 'at': 2000}]
        self.assistant._save_state(self.actor, state)
        async def stream(messages, info):
            results = [part.content for message in messages for part in message.parts if getattr(part, 'part_kind', '') == 'tool-return']
            if not results:
                yield {0: DeltaToolCall(name='search_history', json_args=json.dumps({'keyword': '上周 巡检报告'}))}
            elif len(results) == 1:
                self.assertEqual(results[0]['total'], 1)
                self.assertEqual(results[0]['items'][0]['operation_id'], 'older-report')
                yield {0: DeltaToolCall(name='prepare_business', json_args=json.dumps({'title': '发送上周巡检报告', 'operations': [
                    {'api_id': 'POST /api/message-delivery/send', 'body': {'recipient_ids': ['__self__'],
                     'text': {'$query': {'ref': results[0]['query_ref'], 'path': 'items.0.answer'}}}}]}))}
            else:
                yield '已找到上周巡检报告，请核对收件人和内容。'
        @asynccontextmanager
        async def factory(*_):
            yield FunctionModel(stream_function=stream)
        async def emit(*_): pass
        async def authorize(): return self.actor.copy()
        result = await LighthouseModel(self.agent, model_factory=factory).answer(self.actor,
            {'question': '把前面上周的巡检报告发给我，不是最新一条', 'operation_id': 'archived_message_01',
             'file_ids': [], '_profile': {'model': 'fixture', 'name': 'fixture'}}, [], self.request, emit, authorize, {})
        self.assertIn('plan', result, result)
        plan = self.agent.get_plan(self.actor, result['plan']['id'])
        self.agent.amend(self.actor, plan['id'], {'version': plan['version'], 'values': {'step0.recipient_ids': ['__self__']}})
        self.assertEqual(self.agent.get_plan(self.actor, plan['id'])['operations'][0]['body']['text'], full_text)
        self.controller._submit_background.assert_not_called()

    async def test_native_generated_file_is_downloaded_with_auth_then_sent(self):
        reads = []
        @self.app.get('/api/cabinet-power/exports/{job_id}/download')
        async def download(job_id: str, scope: str, request: Request):
            reads.append((job_id, scope))
            return Response(b'fixture-workbook', media_type='application/octet-stream',
                            headers={'Content-Disposition': 'attachment; filename="report.xlsm"'})
        self.agent.catalog = PortalAPICatalog(self.app)
        state = self.assistant._state(self.actor)
        state['turns'] = [{'operation_id': 'old-export', 'status': 'completed', 'scopes': ['A'], 'plan': {'results': [
            {'downloads': [{'name': 'report.xlsm', 'url': '/api/cabinet-power/exports/old/download?scope=A'}]}]}}]
        self.assistant._save_state(self.actor, state)
        plan = self.agent.prepare(self.actor, {'operations': [{'api_id': 'POST /api/message-delivery/send',
            'body': {'recipient_ids': ['__self__']}}]}, 'native_file_send_01', [])
        field = next(f for f in plan['fields'] if f.get('native_message_content'))
        ready = self.agent.amend(self.actor, plan['id'], {'version': plan['version'], 'values': {
            'step0.recipient_ids': ['__self__'], field['name']: [field['options'][0]['value']]}})
        transport = SimpleNamespace(upload=Mock(return_value='file-ready'), send=Mock(side_effect=[TimeoutError(), TimeoutError(), 'message-ready']))
        invoke, cached_writes = self.agent.catalog.invoke, {}
        async def cached_invoke(operation, *args, **kwargs):
            if operation['api_id'].startswith('POST '):
                key = json.dumps(operation, sort_keys=True)
                if key not in cached_writes:
                    cached_writes[key] = await invoke(operation, *args, **kwargs)
                return copy.deepcopy(cached_writes[key])
            return await invoke(operation, *args, **kwargs)
        self.agent.catalog.invoke = cached_invoke
        self.controller._submit_background.side_effect = lambda name, fn, *args: (fn(*args), True)[1]
        with patch('lan_bitable_template_portal.message_delivery.FeishuDeliveryTransport', return_value=transport):
            result = await self.agent.confirm(self.actor, plan['id'], {'version': ready['version'], 'stage': 'review'}, self.request)
            if result['status'] == 'awaiting_second_confirmation':
                await self.agent.confirm(self.actor, plan['id'], {'version': result['version'], 'stage': 'execute'}, self.request)
            if self.agent.tasks:
                await asyncio.gather(*tuple(self.agent.tasks))
            for _ in range(2):
                failed = self.agent.get_plan(self.actor, plan['id'])
                self.assertEqual(failed['status'], 'failed', failed)
                await self.agent.retry_notice(self.actor, plan['id'], {'version': failed['version']}, self.request)
                if self.agent.tasks:
                    await asyncio.gather(*tuple(self.agent.tasks))
        final = self.agent.get_plan(self.actor, plan['id'])
        self.assertEqual(final['status'], 'completed', final.get('error'))
        self.assertEqual(reads, [('old', 'A')])
        transport.upload.assert_called_once()
        self.assertEqual(transport.send.call_count, 3)
        self.assertEqual(len({call.args[-1] for call in transport.send.call_args_list}), 1)
        self.assertEqual(len(cached_writes), 3)
        self.assertEqual(transport.send.call_args.args[2], {'file_key': 'file-ready'})

    async def test_native_delivery_rejects_guest_and_cross_origin(self):
        payload = {'operation_id': 'auth_fixture_001', 'recipient_ids': ['__self__'], 'text': 'test'}
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app), base_url='http://testserver') as client:
            result = await client.post('/api/message-delivery/send', json=payload, headers={'Origin': 'http://other-host'})
            self.assertEqual(result.status_code, 403)
            self.controller._current_session = lambda request: {'open_id': 'ou_a', 'role': 'guest'}
            result = await client.post('/api/message-delivery/send', json=payload, headers={'Origin': 'http://testserver'})
            self.assertEqual(result.status_code, 401)
        self.controller._submit_background.assert_not_called()


if __name__ == '__main__':
    unittest.main()
