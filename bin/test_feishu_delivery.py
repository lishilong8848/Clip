"""Isolated public delivery tests: no real HTTP, credentials or business data."""
from __future__ import annotations

import asyncio
import copy
from contextlib import ExitStack, contextmanager
from pathlib import Path
import sys
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lan_bitable_template_portal.feishu_delivery import (
    FeishuDeliveryError, PublicFeishuSender, send_agent_message,
)
from upload_event_module.services import robot_webhook as robot

TARGET_APP_ID = 'cli_fixtureCurrent'
PREVIOUS_APP_ID = 'cli_fixturePrevious'
FIXTURE_SECRET = 'fixture-app-secret'


def active_config(app_id=TARGET_APP_ID, secret=FIXTURE_SECRET):
    return {'app_id': app_id, 'secret_cipher': 'fixture:' + secret}


@contextmanager
def qt_credentials_fixture():
    def unprotect(value):
        if not isinstance(value, str) or not value.startswith('fixture:'):
            raise ValueError('invalid fixture cipher')
        return value[len('fixture:'):]
    with ExitStack() as stack:
        stack.enter_context(patch.object(robot.config, 'app_id', TARGET_APP_ID))
        stack.enter_context(patch.object(robot.config, 'app_secret', FIXTURE_SECRET))
        stack.enter_context(patch.object(robot.config, 'refresh_credentials',
            side_effect=lambda: (robot.config.app_id, robot.config.app_secret)))
        stack.enter_context(patch('openclaw_service.assistant.lighthouse_ai.unprotect_key', side_effect=unprotect))
        yield


STATIC = {'schema': '2.0', 'body': {'elements': [
    {'tag': 'button', 'url': 'https://example.invalid/review', 'text': {'tag': 'plain_text', 'content': '查看'}}]}}
CALLBACKS = [
    {'elements': [{'tag': 'action', 'actions': [{'tag': 'button', 'value': {'kind': 'review'}}]}]},
    {'schema': '2.0', 'body': {'elements': [{'tag': 'button', 'behaviors': [{'type': 'callback', 'value': {}}]}]}},
    {'body': {'elements': [{'tag': 'form', 'elements': []}]}},
    {'elements': [{'tag': 'select_static', 'options': []}]},
    {'body': {'elements': [{'tag': 'date_picker'}]}},
]


class Store:
    def __init__(self):
        self.data = {}
        self.lock = threading.Lock()
        self.db_path = Path('nonexistent-offline-fixture.sqlite3')

    def get_document(self, ns, key):
        with self.lock:
            return copy.deepcopy(self.data.get((ns, key)))

    def put_document(self, ns, key, value):
        with self.lock:
            self.data[ns, key] = copy.deepcopy(value)

    def list_documents(self, ns, *, key_prefix=''):
        with self.lock:
            return [{'key': key, 'payload': copy.deepcopy(value)}
                    for (namespace, key), value in self.data.items()
                    if namespace == ns and key.startswith(key_prefix)]


def response(payload=None, status=200):
    if payload is None:
        payload = {'code': 0, 'data': {'message_id': 'om_agent', 'chat_id': 'oc_original'}}
    return SimpleNamespace(status_code=status, json=lambda: payload)


class FeishuDeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.enterContext(qt_credentials_fixture())
        self.guard = patch('lan_bitable_template_portal.portal_service.external_real_write_guard',
                           return_value={'real_write_allowed': True})
        self.guard_mock = self.guard.start()
        self.addCleanup(self.guard.stop)
        self.request_json = robot._request_json
        # Any unintended transport escape fails the test instead of reaching Feishu.
        self.wire = patch.object(robot, '_request_json', side_effect=AssertionError('real HTTP forbidden'))
        self.legacy_wire = self.wire.start()
        self.addCleanup(self.wire.stop)
        self.token_patch = patch.object(robot, '_get_tenant_access_token', return_value=('fixture', ''))
        self.token = self.token_patch.start()
        self.addCleanup(self.token_patch.stop)
        self.post = AsyncMock(return_value=response())
        self.messenger = SimpleNamespace(client=AsyncMock(return_value=SimpleNamespace(post=self.post)),
            headers=AsyncMock(return_value={'Authorization': 'fixture'}),
            recipient_open_id=AsyncMock(return_value='ou_agent'))
        self.auth = SimpleNamespace(directory_person=Mock(return_value={'id': 'recPerson', 'selectable': True}))
        self.bot = SimpleNamespace(config=active_config(), messenger=self.messenger, closing=False,
            handover_cards=SimpleNamespace(recipient_id=Mock(return_value=('union_id', 'on_verified'))),
            permission_cards=SimpleNamespace(union_id=Mock(return_value='on_verified')))
        self.runtime = SimpleNamespace(auth_manager=self.auth, state_store=Store(), feishu_assistant_channel=self.bot)
        for app_id in (TARGET_APP_ID, 'cli_future'):
            for open_id in ('ou_portal', 'ou_one', 'ou_two'):
                self.runtime.state_store.put_document('feishu_assistant_sender', app_id + ':' + open_id,
                    {'open_id': open_id, 'record_id': 'recPerson', 'matched_by': 'record_id'})
        self.sender = PublicFeishuSender(self.runtime, timeout=1)
        self.sender.start()
        self.addAsyncCleanup(self.sender.close)
        direct = patch('lan_bitable_template_portal.feishu_delivery.standalone_sender', return_value=self.sender)
        direct.start()
        self.addCleanup(direct.stop)

    async def text(self, identity='stable', people=None):
        return await asyncio.to_thread(robot.send_text_to_open_ids, '测试', people or ['ou_portal'], message_uuid=identity)

    async def test_agent_preferred_and_keeps_receipt_uuid_and_original_identity(self):
        ok, _, rows = await self.text()
        self.assertTrue(ok)
        self.token.assert_not_called()
        self.bot.handover_cards.recipient_id.assert_not_called()
        self.messenger.recipient_open_id.assert_not_awaited()
        sent = self.post.call_args.kwargs
        self.assertEqual(sent['params'], {'receive_id_type': 'open_id'})
        self.assertEqual(sent['json']['receive_id'], 'ou_portal')
        self.assertEqual(sent['json']['uuid'], 'stable')
        self.assertEqual(rows[0]['open_id'], 'ou_portal')
        self.assertEqual(rows[0]['sender_app_id'], TARGET_APP_ID)
        self.assertEqual(rows[0]['message_id'], 'om_agent')
        self.assertEqual(rows[0]['chat_id'], 'oc_original')
        self.assertGreater(rows[0]['accepted_at'], 0)

    async def test_current_employee_directory_overrides_historical_sender_after_clear_or_rebind(self):
        from lan_bitable_template_portal.personnel_password_login import DIRECTORY_NS, DIRECTORY_KEY
        for ids in ([],['ou_new']):
            self.runtime.state_store.put_document(DIRECTORY_NS,DIRECTORY_KEY,{
                'schema':4,'app_id':TARGET_APP_ID,'people':{'recPerson':{
                    'id':'recPerson','name':'当前人员','login_ids':ids,'selectable':True}}})
            ok,message,_=await self.text('cleared-or-rebound-'+str(len(ids)))
            self.assertFalse(ok)
            self.assertIn('员工姓名为空或账号已更换',message)
        self.post.assert_not_awaited()
        self.token.assert_not_called()
        ok,_,receipts=await self.text('new-account',['ou_new'])
        self.assertTrue(ok)
        self.assertEqual(self.post.call_args.kwargs['json']['receive_id'],'ou_new')
        self.assertEqual(receipts[0]['open_id'],'ou_new')

    async def test_agent_accepts_future_qt_app_but_rejects_stale_app_or_secret(self):
        with patch.object(robot.config, 'app_id', 'cli_future'):
            ok, _, _ = await self.text('stale')
            self.assertFalse(ok)
            self.post.assert_not_awaited()
            self.bot.config['app_id'] = 'cli_future'
            ok, _, rows = await self.text('future')
            self.assertTrue(ok)
            self.assertEqual(rows[0]['sender_app_id'], 'cli_future')
        self.post.reset_mock()
        self.bot.config = active_config(PREVIOUS_APP_ID)
        self.assertFalse((await self.text('stale-bot'))[0])
        self.bot.config = active_config(secret='fixture-independent-secret')
        self.assertFalse((await self.text('stale-secret'))[0])
        self.post.assert_not_awaited()
        self.token.assert_not_called()

    async def test_secret_rotation_requires_current_bot_and_transport_before_sending(self):
        with patch.object(robot.config, 'app_secret', 'fixture-rotated-secret'):
            self.assertFalse((await self.text('rotated-stale-bot'))[0])
            self.bot.config = active_config(secret='fixture-rotated-secret')
            self.messenger.config = active_config()
            self.assertFalse((await self.text('rotated-stale-transport'))[0])
            self.post.assert_not_awaited()
            self.messenger.config = dict(self.bot.config)
            self.assertTrue((await self.text('rotated-ready'))[0])
        self.post.assert_awaited_once()
        self.token.assert_not_called()

    async def test_authoritative_qt_read_failure_blocks_agent_before_headers(self):
        with patch.object(robot.config, 'refresh_credentials', side_effect=OSError('fixture Qt unavailable')):
            self.assertFalse((await self.text())[0])
        self.post.assert_not_awaited()
        self.messenger.headers.assert_not_awaited()

    async def test_secret_change_during_agent_post_preserves_unconfirmed_route(self):
        async def send(*args, **kwargs):
            robot.config.app_secret = 'fixture-rotated-secret'
            return response()
        self.post.side_effect = send
        ok, _, rows = await self.text()
        self.assertFalse(ok)
        self.assertEqual(rows[0]['failure_kind'], 'uncertain')
        self.post.assert_awaited_once()
        key = self.sender.route_key('ou_portal', 'open_id', 'stable')
        self.assertEqual(self.sender.route(key)['status'], 'posting')

    async def test_multirecipient_dedup_and_stable_individual_uuids(self):
        ok, _, first = await self.text('batch', ['ou_one', 'ou_one', ' ou_two ', ''])
        self.assertTrue(ok)
        self.assertEqual(self.post.await_count, 2)
        self.assertNotEqual(first[0]['message_uuid'], first[1]['message_uuid'])
        ok, _, second = await self.text('batch', ['ou_two', 'ou_one'])
        self.assertTrue(ok)
        self.assertEqual([r['message_uuid'] for r in first], list(reversed([r['message_uuid'] for r in second])))
        self.assertEqual(self.post.await_count, 2)
        await self.text(first[0]['message_uuid'], ['ou_one'])
        self.assertEqual(self.post.await_count, 2)

    async def test_group_keeps_exact_chat_id_without_person_or_group_lookup(self):
        ok, _ = await asyncio.to_thread(robot.send_interactive_to_chat_id, STATIC, 'oc_original', message_uuid='group')
        self.assertTrue(ok)
        self.auth.directory_person.assert_not_called()
        self.token.assert_not_called()
        self.assertEqual(self.post.call_args.kwargs['params'], {'receive_id_type': 'chat_id'})
        self.assertEqual(self.post.call_args.kwargs['json']['receive_id'], 'oc_original')

    async def test_current_fixed_duty_open_id_is_sent_unchanged_without_union_lookup(self):
        from lan_bitable_template_portal.portal_service import BUILDING_OPEN_ID_MAP
        open_id = BUILDING_OPEN_ID_MAP['A']
        self.auth.directory_person.return_value = {}
        with patch('lan_bitable_template_portal.feishu_identity.TARGET_APP_ID', TARGET_APP_ID):
            ok, _, _ = await self.text(people=[open_id])
        self.assertTrue(ok)
        self.messenger.recipient_open_id.assert_not_awaited()
        self.assertEqual(self.post.call_args.kwargs['json']['receive_id'], open_id)
        self.assertEqual(self.post.call_args.kwargs['params']['receive_id_type'], 'open_id')

    async def test_existing_binding_used_when_directory_temporarily_unavailable(self):
        from openclaw_service.assistant.lighthouse_ai import AssistantError
        self.messenger.recipient_open_id.side_effect = AssistantError('fixture', category='feishu_sender_directory_unavailable')
        self.auth.directory_person.return_value = {}
        ok, _, _ = await self.text()
        self.assertTrue(ok)
        self.assertEqual(self.post.call_args.kwargs['json']['receive_id'], 'ou_portal')
        self.messenger.recipient_open_id.assert_not_awaited()

    async def test_unready_unmapped_and_explicit_rejection_never_fallback(self):
        for mode in ('unready', 'unmapped', 'rejected'):
            with self.subTest(mode=mode):
                self.bot.messenger = None if mode == 'unready' else self.messenger
                self.auth.directory_person.side_effect = ValueError('fixture') if mode == 'unmapped' else None
                self.messenger.recipient_open_id.return_value = None if mode == 'unmapped' else 'ou_agent'
                self.bot.handover_cards.recipient_id.side_effect = ValueError('fixture') if mode == 'unmapped' else None
                self.post.return_value = response({'code': 230013}, 400) if mode == 'rejected' else response()
                with patch.object(robot, '_send_message_to_receive_id') as direct:
                    ok, _, rows = await self.text(mode)
                self.assertFalse(ok)
                direct.assert_not_called()
                self.assertEqual(rows[0]['failure_kind'], 'not_sent')
                self.assertEqual(rows[0].get('code'), 230013 if mode == 'rejected' else None)
        self.token.assert_not_called()

    async def test_static_url_card_never_fallback_after_explicit_rejection(self):
        self.post.return_value = response({'code': 230013}, 400)
        with patch.object(robot, '_send_interactive_to_receive_id') as direct:
            ok, _, rows = await asyncio.to_thread(robot.send_interactive_to_open_ids, STATIC, ['ou_portal'], message_uuid='static')
        self.assertFalse(ok)
        self.assertEqual(rows[0]['code'], 230013)
        direct.assert_not_called()
        self.token.assert_not_called()

    async def test_callback_forms_never_use_legacy_even_when_explicitly_rejected(self):
        self.post.return_value = response({'code': 230013}, 400)
        for index, card in enumerate(CALLBACKS):
            with self.subTest(card=card):
                ok, message, rows = await asyncio.to_thread(robot.send_interactive_to_open_ids,
                    card, ['ou_portal'], message_uuid='card' + str(index))
                self.assertFalse(ok)
                self.assertIn('必须由飞书智能体', message)
                self.assertEqual(rows[0]['failure_kind'], 'agent_required')
        self.token.assert_not_called()

    async def test_callback_card_unready_retains_failure(self):
        self.bot.messenger = None
        ok, _, rows = await asyncio.to_thread(robot.send_interactive_to_open_ids, CALLBACKS[0], ['ou_portal'])
        self.assertFalse(ok)
        self.assertEqual(rows[0]['failure_kind'], 'agent_required')
        self.token.assert_not_called()

    async def test_received_without_valid_receipt_never_falls_back(self):
        invalid = [({}, 200), ({'code': False}, 200), ({'code': 0}, 200),
            ({'code': 0, 'data': {'message_id': 'om_ok'}}, 200),
            ({'code': 0, 'data': {'message_id': 'om_ok', 'chat_id': 'bad'}}, 200),
            ({'code': 500}, 503), ({'code': 408}, 408), ({'code': 0}, 400)]
        for index, (payload, status) in enumerate(invalid):
            with self.subTest(payload=payload, status=status):
                self.post.return_value = response(payload, status)
                ok, _, rows = await self.text('invalid' + str(index))
                self.assertFalse(ok)
                self.assertEqual(rows[0]['failure_kind'], 'uncertain')
        self.token.assert_not_called()

    async def test_network_timeout_then_unready_retry_remains_agent_bound(self):
        self.post.side_effect = TimeoutError('fixture')
        ok, _, rows = await self.text()
        self.assertFalse(ok)
        self.assertEqual(rows[0]['failure_kind'], 'uncertain')
        self.bot.messenger = None
        ok, message, _ = await self.text()
        self.assertFalse(ok)
        self.assertIn('原智能体发送结果', message)
        self.token.assert_not_called()

    async def test_restart_preserves_agent_pin_and_completed_receipt(self):
        self.post.side_effect = TimeoutError('fixture')
        await self.text()
        await self.sender.close()
        self.sender = PublicFeishuSender(self.runtime)
        self.sender.start()
        self.addAsyncCleanup(self.sender.close)
        self.bot.messenger = None
        ok, _, _ = await self.text()
        self.assertFalse(ok)
        self.token.assert_not_called()
        self.bot.messenger = self.messenger
        self.post.side_effect = None
        ok, _, _ = await self.text()
        self.assertTrue(ok)
        self.post.reset_mock()
        await self.text()
        self.post.assert_not_awaited()

    async def test_legacy_timeout_retry_does_not_switch_back_to_agent(self):
        key = self.sender.route_key('ou_portal', 'open_id', 'stable')
        original = {'channel': 'legacy', 'app_id': PREVIOUS_APP_ID, 'status': 'posting', 'attempted_at': 10000.}
        self.sender.save_route(key, original)
        ok, _, rows = await self.text()
        self.assertFalse(ok)
        self.assertEqual(rows[0]['failure_kind'], 'uncertain')
        self.assertEqual(self.sender.route(key), original)
        self.post.assert_not_awaited()
        self.token.assert_not_called()

    async def test_async_agent_only_contract_for_group_and_text(self):
        receipt = await send_agent_message(self.runtime, 'oc_original', STATIC,
            receive_id_type='chat_id', message_uuid='weekly')
        self.assertEqual(receipt['message_uuid'], 'weekly')
        self.assertEqual(receipt['sender_app_id'], TARGET_APP_ID)
        await send_agent_message(self.runtime, 'ou_portal', {'text': 'text'}, msg_type='text', message_uuid='asyncText')
        self.assertEqual(self.post.call_args.kwargs['json']['msg_type'], 'text')
        self.post.return_value = response({'code': 230013}, 400)
        with self.assertRaises(FeishuDeliveryError) as caught:
            await send_agent_message(self.runtime, 'oc_original', STATIC, receive_id_type='chat_id', message_uuid='rejected')
        self.assertFalse(caught.exception.uncertain)
        self.token.assert_not_called()

    async def test_async_interface_reports_uncertain_without_fallback(self):
        self.post.return_value = response({})
        with self.assertRaises(FeishuDeliveryError) as caught:
            await send_agent_message(self.runtime, 'oc_original', STATIC, receive_id_type='chat_id')
        self.assertTrue(caught.exception.uncertain)
        self.token.assert_not_called()

    async def test_ambiguous_and_inactive_person_are_blocked_not_fallback(self):
        from openclaw_service.assistant.lighthouse_ai import AssistantError
        self.messenger.recipient_open_id.side_effect = AssistantError('fixture', category='feishu_recipient_ambiguous')
        from lan_bitable_template_portal.personnel_password_login import DIRECTORY_NS, DIRECTORY_KEY
        self.runtime.state_store.put_document(DIRECTORY_NS, DIRECTORY_KEY, {'app_id': TARGET_APP_ID, 'people': {
            'recOne': {'login_ids': ['ou_portal'], 'selectable': True},
            'recTwo': {'login_ids': ['ou_portal'], 'selectable': True}}})
        ok, _, rows = await self.text()
        self.assertFalse(ok)
        self.assertEqual(rows[0]['failure_kind'], 'blocked')
        self.bot.handover_cards.recipient_id.assert_not_called()
        self.auth.directory_person.return_value = {'id': 'recPerson', 'selectable': False}
        ok, _, _ = await self.text('inactive')
        self.assertFalse(ok)
        self.token.assert_not_called()
        self.post.assert_not_awaited()

    async def test_shutdown_cancels_and_unregisters_without_closing_owned_messenger(self):
        started = asyncio.Event()
        async def slow(*args, **kwargs):
            started.set()
            await asyncio.Event().wait()
        self.post.side_effect = slow
        waiting = asyncio.create_task(self.text())
        await started.wait()
        await self.sender.close()
        ok, _, rows = await waiting
        self.assertFalse(ok)
        self.assertEqual(rows[0]['failure_kind'], 'uncertain')
        self.assertIsNone(self.runtime.feishu_public_sender)
        self.assertEqual(self.sender.tasks, set())
        self.assertIsNone(robot._AGENT_SENDER)
        self.token.assert_not_called()

    async def test_sync_call_on_main_loop_is_rejected_without_deadlock_or_legacy(self):
        ok, message, rows = robot.send_text_to_open_ids('text', ['ou_portal'])
        self.assertFalse(ok)
        self.assertIn('不可阻塞', message)
        self.assertEqual(rows[0]['failure_kind'], 'blocked')
        self.post.assert_not_awaited()
        self.token.assert_not_called()

    async def test_guard_blocks_agent_and_legacy_and_async(self):
        self.guard_mock.return_value = {'real_write_allowed': False, 'reason': 'isolated'}
        self.assertFalse((await self.text())[0])
        with self.assertRaises(FeishuDeliveryError):
            await send_agent_message(self.runtime, 'ou_portal', STATIC)
        self.post.assert_not_awaited()
        self.token.assert_not_called()

    async def test_wrong_group_receipt_is_uncertain_not_redirected(self):
        self.post.return_value = response({'code': 0, 'data': {'message_id': 'om_ok', 'chat_id': 'oc_wrong'}})
        with self.assertRaises(FeishuDeliveryError) as caught:
            await send_agent_message(self.runtime, 'oc_original', STATIC, receive_id_type='chat_id')
        self.assertTrue(caught.exception.uncertain)
        self.assertEqual(self.post.await_count, 1)
        self.token.assert_not_called()

    async def test_no_runtime_qt_scope_is_explicit_and_callbacks_cannot_send_direct(self):
        await self.sender.close()
        ok, message, _ = await asyncio.to_thread(robot.send_interactive_to_open_ids, CALLBACKS[0], ['ou_portal'])
        self.assertFalse(ok)
        self.assertIn('独立Qt进程', message)
        self.token.assert_not_called()
        self.legacy_wire.side_effect = None
        self.legacy_wire.return_value = {'code': 0, 'data': {'message_id': 'om_direct', 'chat_id': 'oc_private'}}
        ok, _, rows = await self.text()
        self.assertTrue(ok)
        self.assertEqual(rows[0]['delivery_channel'], 'direct')
        self.assertEqual(rows[0]['sender_app_id'], TARGET_APP_ID)
        self.assertEqual(self.legacy_wire.call_args.kwargs['json_payload']['receive_id'], 'ou_portal')
        self.post.assert_not_awaited()

    async def test_old_lifecycle_cleanup_cannot_unregister_new_sender(self):
        other = PublicFeishuSender(self.runtime)
        other.start()
        try:
            await self.sender.close()
            self.assertIs(robot._AGENT_SENDER, other)
            self.assertIs(self.runtime.feishu_public_sender, other)
        finally:
            await other.close()

    async def test_worker_timeout_does_not_block_loop_or_use_legacy(self):
        self.sender.timeout = .03
        async def slow(*args, **kwargs):
            await asyncio.sleep(1)
        self.post.side_effect = slow
        ticks = 0
        async def heartbeat():
            nonlocal ticks
            for _ in range(10):
                ticks += 1
                await asyncio.sleep(.005)
        reply, _ = await asyncio.gather(self.text(), heartbeat())
        self.assertFalse(reply[0])
        self.assertGreater(ticks, 1)
        self.token.assert_not_called()

    async def test_concurrent_identical_operations_share_one_delivery(self):
        replies = await asyncio.gather(*(self.text() for _ in range(8)))
        self.assertTrue(all(reply[0] for reply in replies))
        self.assertEqual(self.post.await_count, 1)
        self.token.assert_not_called()

    async def test_same_uuid_with_changed_content_is_not_falsely_reported_success(self):
        await self.text()
        ok, message, rows = await asyncio.to_thread(robot.send_text_to_open_ids,
            'different body', ['ou_portal'], message_uuid='stable')
        self.assertFalse(ok)
        self.assertIn('内容已变化', message)
        self.assertEqual(rows[0]['failure_kind'], 'blocked')
        self.assertEqual(self.post.await_count, 1)

    async def test_uncertain_old_operation_expired_dedup_window_requires_manual_check(self):
        import time
        self.post.side_effect = TimeoutError('fixture')
        await self.text()
        key = self.sender.route_key('ou_portal', 'open_id', 'stable')
        stored = self.sender.route(key)
        stored['attempted_at'] = time.time() - 3600
        self.sender.save_route(key, stored)
        self.post.reset_mock()
        ok, message, _ = await self.text()
        self.assertFalse(ok)
        self.assertIn('超出去重安全时限', message)
        self.post.assert_not_awaited()
        self.token.assert_not_called()

    async def test_unknown_delivery_cannot_switch_app_or_mapped_person(self):
        self.post.side_effect = TimeoutError('fixture')
        await self.text()
        self.post.reset_mock()
        self.bot.config['app_id'] = 'cli_other'
        self.assertFalse((await self.text())[0])
        self.bot.config['app_id'] = TARGET_APP_ID
        self.messenger.recipient_open_id.return_value = 'ou_other'
        self.runtime.state_store.put_document('feishu_assistant_sender', TARGET_APP_ID + ':ou_portal',
            {'open_id': 'ou_other', 'matched_by': 'record_id'})
        self.assertFalse((await self.text())[0])
        self.post.assert_not_awaited()
        self.token.assert_not_called()

    async def test_token_rejection_invalidates_agent_token_for_retry(self):
        self.messenger.expires = 12345
        self.post.return_value = response({'code': 99991663}, 401)
        with self.assertRaises(FeishuDeliveryError):
            await send_agent_message(self.runtime, 'ou_portal', CALLBACKS[0], message_uuid='token')
        self.assertEqual(self.messenger.expires, 0)
        self.token.assert_not_called()

    async def test_receipt_persistence_failure_remains_uncertain_and_agent_pinned(self):
        original = self.runtime.state_store.put_document
        def fail_on_receipt(ns, key, value):
            if value.get('status') == 'sent':
                raise OSError('fixture')
            original(ns, key, value)
        with patch.object(self.runtime.state_store, 'put_document', side_effect=fail_on_receipt):
            self.assertFalse((await self.text())[0])
        self.bot.messenger = None
        self.assertFalse((await self.text())[0])
        self.token.assert_not_called()

    async def test_async_agent_only_cannot_adopt_legacy_receipt(self):
        key = self.sender.route_key('ou_portal', 'open_id', 'stable')
        receipt = {'delivery_channel': 'legacy', 'sender_app_id': PREVIOUS_APP_ID, 'message_id': 'om_old'}
        self.sender.save_route(key, {'channel': 'legacy', 'app_id': PREVIOUS_APP_ID, 'status': 'sent', 'receipt': receipt})
        ok, _, rows = await self.text()
        self.assertTrue(ok)
        self.assertEqual(rows[0]['sender_app_id'], PREVIOUS_APP_ID)
        with self.assertRaises(FeishuDeliveryError):
            await send_agent_message(self.runtime, 'ou_portal', {'text': '测试'}, msg_type='text', message_uuid='stable')
        self.post.assert_not_awaited()
        self.token.assert_not_called()

    async def test_legacy_token_failure_is_definitely_not_sent(self):
        self.bot.messenger = None
        self.token.return_value = ('', 'fixture authentication unavailable')
        ok, _, rows = await self.text()
        self.assertFalse(ok)
        self.assertEqual(rows[0]['failure_kind'], 'not_sent')
        self.assertNotIn('code', rows[0])
        self.legacy_wire.assert_not_called()
        route = self.sender.route(self.sender.route_key('ou_portal', 'open_id', 'stable'))
        self.assertEqual(route['status'], 'not_sent')
        self.assertNotIn('attempted_at', route)

    async def test_direct_claim_refused_before_any_post_is_uncertain(self):
        await self.sender.close()
        with patch.object(self.sender, 'claim', return_value=False):
            ok, _, rows = await self.text()
        self.assertFalse(ok)
        self.assertEqual(rows[0]['failure_kind'], 'uncertain')
        self.legacy_wire.assert_not_called()

    async def test_guard_denials_are_blocked_not_legacy_failed(self):
        allow, deny = {'real_write_allowed': True}, {'real_write_allowed': False}
        self.guard_mock.return_value = deny
        ok, _, rows = await self.text('blockedBefore')
        self.assertFalse(ok)
        self.assertEqual(rows[0]['failure_kind'], 'blocked')
        self.guard_mock.side_effect = [allow, allow, deny]
        ok, _, rows = await self.text('blockedFallback')
        self.assertFalse(ok)
        self.assertEqual(rows[0]['failure_kind'], 'blocked')
        self.legacy_wire.assert_not_called()

    async def test_direct_explicit_text_and_static_card_rejections_reset_routes(self):
        await self.sender.close()
        self.legacy_wire.side_effect = None
        self.legacy_wire.return_value = {'code': 230013, 'msg': 'fixture rejected'}
        for kind in ('text', 'interactive'):
            with self.subTest(kind=kind):
                call = self.text(kind) if kind == 'text' else asyncio.to_thread(
                    robot.send_interactive_to_open_ids, STATIC, ['ou_portal'], message_uuid=kind)
                ok, _, rows = await call
                self.assertFalse(ok)
                self.assertEqual(rows[0]['failure_kind'], 'not_sent')
                self.assertEqual(rows[0]['code'], 230013)
                route = self.sender.route(self.sender.route_key('ou_portal', 'open_id', kind))
                self.assertEqual(route['status'], 'not_sent')
                self.assertEqual(route['code'], 230013)
                self.assertNotIn('attempted_at', route)
        self.assertTrue(all(call.kwargs['retries'] == 0 for call in self.legacy_wire.call_args_list))

    async def test_direct_token_refresh_failure_after_rejection_retains_code(self):
        await self.sender.close()
        self.legacy_wire.side_effect = None
        self.legacy_wire.return_value = {'code': 99991663, 'msg': 'fixture expired'}
        self.token.side_effect = [('fixture', ''), ('', 'fixture unavailable')]
        ok, _, rows = await self.text()
        self.assertFalse(ok)
        self.assertEqual(rows[0]['failure_kind'], 'not_sent')
        self.assertEqual(rows[0]['code'], 99991663)
        self.assertEqual(self.legacy_wire.call_count, 1)

    async def test_primary_rejection_keeps_code_without_attempting_direct(self):
        self.post.return_value = response({'code': 230013}, 400)
        ok, _, rows = await self.text()
        self.assertFalse(ok)
        self.assertEqual(rows[0]['failure_kind'], 'not_sent')
        self.assertEqual(rows[0]['code'], 230013)
        route = self.sender.route(self.sender.route_key('ou_portal', 'open_id', 'stable'))
        self.assertEqual(route['status'], 'not_sent')
        self.legacy_wire.assert_not_called()
        self.token.assert_not_called()

    async def test_later_legacy_rejection_cannot_erase_prior_unknown_delivery(self):
        key = self.sender.route_key('ou_portal', 'open_id', 'stable')
        original = {'channel': 'legacy', 'app_id': PREVIOUS_APP_ID, 'status': 'posting', 'attempted_at': 10000.}
        self.sender.save_route(key, original)
        self.post.return_value = response({'code': 230013}, 400)
        ok, _, rows = await self.text()
        self.assertFalse(ok)
        self.assertEqual(rows[0]['failure_kind'], 'uncertain')
        self.assertNotIn('code', rows[0])
        self.assertEqual(self.sender.route(key), original)
        self.post.assert_not_awaited()
        self.legacy_wire.assert_not_called()

    async def test_later_token_failure_cannot_erase_prior_unknown_legacy_delivery(self):
        key = self.sender.route_key('ou_portal', 'open_id', 'stable')
        original = {'channel': 'legacy', 'app_id': PREVIOUS_APP_ID, 'status': 'posting', 'attempted_at': 10000.}
        self.sender.save_route(key, original)
        self.token.return_value = ('', 'fixture unavailable')
        ok, _, rows = await self.text()
        self.assertFalse(ok)
        self.assertEqual(rows[0]['failure_kind'], 'uncertain')
        self.assertNotIn('code', rows[0])
        self.assertEqual(self.sender.route(key), original)
        self.token.assert_not_called()
        self.legacy_wire.assert_not_called()

    async def test_accepted_direct_message_then_receipt_save_failure_stays_uncertain(self):
        await self.sender.close()
        self.legacy_wire.side_effect = None
        self.legacy_wire.return_value = {'code': 0, 'data': {'message_id': 'om_accepted', 'chat_id': 'oc_original'}}
        finish = self.sender.save_route
        for committed in (False, True):
            with self.subTest(committed=committed):
                identity = 'saved' if committed else 'unsaved'
                def fail(key, value):
                    if value.get('status') != 'sent' or committed:
                        finish(key, value)
                    if value.get('status') == 'sent':
                        raise OSError('fixture receipt store unavailable')
                with patch.object(self.sender, 'save_route', side_effect=fail):
                    ok, _, rows = await self.text(identity)
                self.assertFalse(ok)
                self.assertEqual(rows[0]['failure_kind'], 'uncertain')
                self.assertEqual(rows[0]['message_id'], 'om_accepted')
                self.assertGreater(rows[0]['accepted_at'], 0)
                self.assertNotIn('code', rows[0])
                route = self.sender.route(self.sender.route_key('ou_portal', 'open_id', identity))
                self.assertEqual(route['status'], 'sent' if committed else 'posting')
                before = self.legacy_wire.call_count
                self.assertTrue((await self.text(identity))[0])
                self.assertEqual(self.legacy_wire.call_count - before, 0 if committed else 1)
        self.post.assert_not_awaited()

    async def test_rejection_route_save_failure_is_uncertain_without_code(self):
        self.post.return_value = response({'code': 230013}, 400)
        save = self.sender.save_route
        def fail(key, value):
            if value.get('status') == 'not_sent':
                raise OSError('fixture')
            save(key, value)
        with patch.object(self.sender, 'save_route', side_effect=fail):
            ok, _, rows = await self.text()
        self.assertFalse(ok)
        self.assertEqual(rows[0]['failure_kind'], 'uncertain')
        self.assertNotIn('code', rows[0])
        route = self.sender.route(self.sender.route_key('ou_portal', 'open_id', 'stable'))
        self.assertEqual(route['status'], 'posting')

    async def test_agent_only_error_reports_explicit_rejection_code(self):
        self.post.return_value = response({'code': 230013}, 400)
        with self.assertRaises(FeishuDeliveryError) as caught:
            await send_agent_message(self.runtime, 'ou_portal', CALLBACKS[0], message_uuid='rejected')
        self.assertFalse(caught.exception.uncertain)
        self.assertEqual(caught.exception.code, 230013)
        route = self.sender.route(self.sender.route_key('ou_portal', 'open_id', 'rejected'))
        self.assertEqual(route['status'], 'not_sent')
        self.assertEqual(route['code'], 230013)
        self.token.assert_not_called()

    async def test_convergence_safe_window_resets_for_auth_failures_and_remote_rejections(self):
        from lan_bitable_template_portal.plan_convergence_notifications import NoticePlanChecks
        checks = NoticePlanChecks(None, self.runtime.state_store, None, robot.send_text_to_open_ids)
        for mode in ('auth', 'reject'):
            with self.subTest(mode=mode):
                clock = [10000.]
                document = {'id': 'alert-' + mode, 'recipients': ['ou_portal']}
                self.bot.messenger = None if mode == 'auth' else self.messenger
                self.post.return_value = response({'code': 230013}, 400)
                with patch('lan_bitable_template_portal.plan_convergence_notifications.time.time', side_effect=lambda: clock[0]):
                    for _ in range(2):
                        self.assertFalse(await asyncio.to_thread(checks._deliver, document, '检查提醒'))
                        saved = document['receipts']['ou_portal']
                        self.assertNotIn('attempted_at', saved)
                        self.assertFalse(saved['uncertain'])
                        self.assertEqual(saved['failure_kind'], 'not_sent')
                        clock[0] += 4000
                    self.bot.messenger = self.messenger
                    self.post.return_value = response()
                    self.assertTrue(await asyncio.to_thread(checks._deliver, document, '检查提醒'))
                    self.assertEqual(document['receipts']['ou_portal']['message_id'], 'om_agent')
        self.token.assert_not_called()

    async def test_definitely_rejected_legacy_route_can_return_to_agent_after_an_hour(self):
        key = self.sender.route_key('ou_portal', 'open_id', 'stable')
        self.sender.save_route(key, {'status': 'not_sent', 'code': 230013})
        with patch('lan_bitable_template_portal.feishu_delivery.time.time', return_value=14000.):
            ok, _, rows = await self.text()
        self.assertTrue(ok)
        self.assertEqual(rows[0]['delivery_channel'], 'agent')
        self.assertNotIn('code', rows[0])
        self.legacy_wire.assert_not_called()
        self.assertEqual(self.post.call_args.kwargs['json']['uuid'], 'stable')

    async def test_later_agent_rejection_cannot_export_a_definite_code_for_unknown_write(self):
        self.post.side_effect = TimeoutError('fixture')
        await self.text()
        self.post.side_effect = None
        self.post.return_value = response({'code': 230013}, 400)
        ok, _, rows = await self.text()
        self.assertFalse(ok)
        self.assertEqual(rows[0]['failure_kind'], 'uncertain')
        self.assertNotIn('code', rows[0])
        self.token.assert_not_called()

    async def test_convergence_keeps_window_when_direct_was_accepted_but_receipt_save_failed(self):
        from lan_bitable_template_portal.plan_convergence_notifications import NoticePlanChecks
        checks = NoticePlanChecks(None, self.runtime.state_store, None, robot.send_text_to_open_ids)
        await self.sender.close()
        self.legacy_wire.side_effect = None
        self.legacy_wire.return_value = {'code': 0, 'data': {'message_id': 'om_accepted', 'chat_id': 'oc_original'}}
        document = {'id': 'accepted-alert', 'recipients': ['ou_portal']}
        save = self.sender.save_route
        def fail(key, value):
            if value.get('status') == 'sent':
                raise OSError('fixture')
            save(key, value)
        with patch.object(self.sender, 'save_route', side_effect=fail):
            self.assertFalse(await asyncio.to_thread(checks._deliver, document, '检查提醒'))
        receipt = document['receipts']['ou_portal']
        self.assertTrue(receipt['uncertain'])
        self.assertFalse(receipt['sent'])
        self.assertIn('attempted_at', receipt)
        self.assertEqual(receipt['failure_kind'], 'uncertain')
        self.assertEqual(receipt['message_id'], 'om_accepted')
        self.assertNotIn('code', receipt)

    async def test_agent_http408_is_uncertain_even_with_rejection_or_success_body(self):
        bodies = [{'code': 230013}, {'code': 99991663},
                  {'code': 0, 'data': {'message_id': 'om_possible', 'chat_id': 'oc_original'}}]
        for index, body in enumerate(bodies):
            with self.subTest(body=body):
                self.post.return_value = response(body, 408)
                identity = 'agent408' + str(index)
                ok, _, rows = await self.text(identity)
                self.assertFalse(ok)
                self.assertEqual(rows[0]['failure_kind'], 'uncertain')
                self.assertNotIn('code', rows[0])
                route = self.sender.route(self.sender.route_key('ou_portal', 'open_id', identity))
                self.assertEqual(route['status'], 'posting')
                self.assertEqual(route['channel'], 'agent')
        self.token.assert_not_called()
        self.legacy_wire.assert_not_called()

    async def test_direct_http408_preserves_pin_for_text_and_card_without_token_retry(self):
        await self.sender.close()
        bodies = [{'code': 230013, 'msg': 'Bot has NO availability'},
                  {'code': 99991663, 'msg': 'token expired'},
                  {'code': 0, 'data': {'message_id': 'om_possible', 'chat_id': 'oc_original'}}]
        for kind in ('text', 'interactive'):
            for index, body in enumerate(bodies):
                with self.subTest(kind=kind, body=body), \
                     patch.object(robot, '_request_json', self.request_json), \
                     patch.object(robot._HTTP_CLIENT, 'request', return_value=response(body, 408)) as wire, \
                     patch.object(robot._HTTP_CLIENT, 'request_json', side_effect=AssertionError('JSON retry forbidden')):
                    self.token.reset_mock()
                    identity = 'legacy408' + kind + str(index)
                    if kind == 'text':
                        ok, _, rows = await self.text(identity)
                    else:
                        ok, _, rows = await asyncio.to_thread(robot.send_interactive_to_open_ids,
                            STATIC, ['ou_portal'], message_uuid=identity)
                    self.assertFalse(ok)
                    self.assertEqual(rows[0]['failure_kind'], 'uncertain')
                    self.assertNotIn('code', rows[0])
                    self.assertNotIn('message_id', rows[0])
                    route = self.sender.route(self.sender.route_key('ou_portal', 'open_id', identity))
                    self.assertEqual(route['status'], 'posting')
                    self.assertEqual(route['channel'], 'direct')
                    wire.assert_called_once()
                    self.token.assert_called_once()
        self.post.assert_not_awaited()

    async def test_shared_direct_transport_distinguishes_400_rejection_from_uncertain_http(self):
        await self.sender.close()
        with patch.object(robot, '_request_json', self.request_json):
            for status in (400, 408, 500, 503):
                with self.subTest(status=status), patch.object(robot._HTTP_CLIENT, 'request',
                        return_value=response({'code': 230013, 'msg': 'fixture rejected'}, status)):
                    ok, _, rows = await self.text('http' + str(status))
                    self.assertFalse(ok)
                    self.assertEqual(rows[0]['failure_kind'], 'not_sent' if status == 400 else 'uncertain')
                    self.assertEqual(rows[0].get('code'), 230013 if status == 400 else None)

    async def test_old_app_http408_cannot_switch_back_to_agent_on_recovery(self):
        key = self.sender.route_key('ou_portal', 'open_id', 'stable')
        original = {'status': 'posting', 'channel': 'legacy', 'app_id': PREVIOUS_APP_ID, 'attempted_at': 10000.}
        self.sender.save_route(key, original)
        ok, _, rows = await self.text()
        self.assertFalse(ok)
        self.assertEqual(rows[0]['failure_kind'], 'uncertain')
        self.assertEqual(self.sender.route(key), original)
        self.post.assert_not_awaited()
        self.token.assert_not_called()


if __name__ == '__main__':
    unittest.main()
