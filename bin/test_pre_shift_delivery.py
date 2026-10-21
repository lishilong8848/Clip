"""Offline reminder diagnostics, durable retry and scope isolation regressions."""
import asyncio
import datetime as dt
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lan_bitable_template_portal.pre_shift_checks import CheckService, CheckError, CheckDeliveryUncertain, TZ
from lan_bitable_template_portal.pre_shift_cloud import CheckSender
from test_pre_shift_checks import receipt

NOW = dt.datetime(2026, 10, 19, 8, 50, tzinfo=TZ)
ADMIN = {'id': 'ou_admin', 'name': '管理员', 'level': 'L1', 'category': 'manager', 'scope_rules': ['ALL']}
LOCAL_ID = 'ou_localAccount1234'
AGENT_ID = 'ou_agentAccount5678'


def response(status=400, code=230013, headers=None):
    return SimpleNamespace(status_code=status, headers=headers or {}, json=lambda: {
        'code': code, 'msg': 'Bearer private-token raw server payload',
        'error': {'log_id': 'BODY_LOG_123', 'details': 'private-token'},
        'data': {'message_id': 'om_delivered', 'private': 'private-token'}})


class SenderDiagnosticTests(unittest.TestCase):
    def setUp(self):
        self.client = SimpleNamespace(post=AsyncMock(return_value=response()))
        self.bot = SimpleNamespace(config={'app_id': 'cli_agent'}, messenger=SimpleNamespace(
            client=AsyncMock(return_value=self.client), headers=AsyncMock(return_value={'Authorization': 'private-token'}),
            recipient_open_id=AsyncMock(return_value=AGENT_ID)),
            handover_cards=SimpleNamespace(recipient_id=Mock(return_value=('open_id', AGENT_ID))))
        self.auth = SimpleNamespace(directory_person=Mock(return_value={
            'id': 'recPerson', 'name': '测试检查员', 'selectable': True}))
        self.sender = CheckSender(SimpleNamespace(feishu_assistant_channel=self.bot, auth_manager=self.auth))
        guard = patch('lan_bitable_template_portal.portal_service.external_real_write_guard',
                      return_value={'real_write_allowed': True})
        guard.start()
        self.addCleanup(guard.stop)

    def error(self, expected=CheckError):
        with self.assertRaises(expected) as caught:
            asyncio.run(self.sender.send(LOCAL_ID, {}, 'same-reminder'))
        self.assertIs(type(caught.exception), expected)
        message = str(caught.exception)
        for forbidden in ('private-token', 'raw server payload', 'Bearer', LOCAL_ID, AGENT_ID):
            self.assertNotIn(forbidden, message)
        self.assertIn('应用ID=cli_agent', message)
        self.assertIn('接收人=测试检查员', message)
        self.assertIn('本账号=ou_***1234', message)
        self.assertIn('投递账号=ou_***5678', message)
        return message

    def test_230013_is_specific_safe_definite_refusal_with_log_id(self):
        for code in (230013, '230013'):
            with self.subTest(code=code):
                self.client.post.return_value = response(code=code, headers={
                    'X-Tt-Logid': 'HEADER_LOG_123', 'Authorization': 'private-token'})
                message = self.error()
                self.assertIn('接收人不在当前应用机器人的可用范围', message)
                self.assertIn('授权生效后将自动重试', message)
                self.assertIn('HTTP 400', message)
                self.assertIn('code=230013', message)
                self.assertIn('request-logid=HEADER_LOG_123', message)
                self.assertNotIn('BODY_LOG_123', message)
                for wrong_diagnosis in ('多维', '消息权限'):
                    self.assertNotIn(wrong_diagnosis, message)
        self.bot.handover_cards.recipient_id.assert_not_called()

    def test_other_api_errors_do_not_claim_bot_availability_problem(self):
        self.client.post.return_value = response(code=230001)
        message = self.error()
        self.assertIn('code=230001', message)
        self.assertIn('request-logid=BODY_LOG_123', message)
        self.assertNotIn('可用范围', message)
        self.assertNotIn('消息权限', message)

    def test_non_json_400_remains_definite_and_unsafe_log_ids_are_omitted(self):
        self.client.post.return_value = SimpleNamespace(status_code=400,
            headers={'X-Tt-Logid': 'Bearer private-token\r\n'}, json=Mock(side_effect=ValueError('private-token')))
        message = self.error()
        self.assertIn('HTTP 400', message)
        self.assertIn('code=未知', message)
        self.assertNotIn('request-logid', message)

    def test_network_5xx_and_malformed_success_remain_uncertain(self):
        self.client.post.side_effect = TimeoutError('Bearer private-token')
        self.error(CheckDeliveryUncertain)
        self.client.post.side_effect = None
        cases = [response(503, 230013), SimpleNamespace(status_code=200, headers={},
                 json=Mock(side_effect=ValueError('private-token')))]
        cases.extend(SimpleNamespace(status_code=200, headers={}, json=lambda value=value: value)
                     for value in ([], {}, {'code': True}, {'code': 0, 'data': []}, {'code': 0, 'data': {'message_id': ''}}))
        for value in cases:
            with self.subTest(value=value):
                self.client.post.return_value = value
                message = self.error(CheckDeliveryUncertain)
                if value.status_code == 503:
                    self.assertIn('HTTP 503', message)
                    self.assertNotIn('可用范围', message)

    def test_untrusted_code_and_log_values_are_not_stringified(self):
        self.client.post.return_value = SimpleNamespace(status_code=400, headers={}, json=lambda: {
            'code': {'token': 'private-token'}, 'error': {'log_id': {'token': 'private-token'}}})
        self.assertIn('code=未知', self.error())

    def test_fixed_account_without_directory_name_has_masked_diagnostic(self):
        self.auth.directory_person.return_value = {}
        self.auth.account_profile = Mock(return_value={'modules': ['maintenance']})
        with patch('lan_bitable_template_portal.portal_auth.BUILDING_OPEN_ID_MAP', {'A': LOCAL_ID}):
            with self.assertRaises(CheckError) as caught:
                asyncio.run(self.sender.send(LOCAL_ID, {}, 'fixed-reminder'))
        self.assertIs(type(caught.exception), CheckError)
        self.assertIn('接收人=接收账号', str(caught.exception))
        self.assertIn('本账号=ou_***1234', str(caught.exception))
        self.assertNotIn(LOCAL_ID, str(caught.exception))

    def test_real_sender_refusal_recovers_after_hour_without_resending_success(self):
        async def post(url, **kwargs):
            self.assertTrue(url.endswith('/im/v1/messages'))
            return response() if kwargs['json']['receive_id'] == AGENT_ID else response(200, 0)
        self.client.post.side_effect = post
        self.auth.directory_person.side_effect = lambda oid: {'id': 'recPerson' if oid == LOCAL_ID else 'recOther',
                                                              'name': '测试检查员', 'selectable': True}
        self.bot.messenger.recipient_open_id.side_effect = lambda rid: AGENT_ID if rid == 'recPerson' else 'ou_otherAgent'
        with tempfile.TemporaryDirectory() as root:
            roster = lambda _: {'people': [{'scope': 'A', 'shift': '白', 'open_id': oid, 'name': '测试检查员'}
                                           for oid in (LOCAL_ID, 'ou_otherAccount')], 'warnings': {}}
            service = CheckService(root, roster=roster, sender=self.sender, portal_url=lambda: 'https://fixture.invalid')
            with patch('lan_bitable_template_portal.pre_shift_checks.now', return_value=NOW):
                service.save_definition(ADMIN, 'A', 0, [{'id': 'check', 'text': '检查'}])
            async def run():
                self.sender.loop = asyncio.get_running_loop()
                await asyncio.to_thread(service.tick, NOW)
                self.assertEqual(self.client.post.await_count, 2)
                self.assertIn('code=230013', service.state(ADMIN, 'A', NOW)['roster_error'])
                first_uuid = self.client.post.call_args_list[0].kwargs['json']['uuid']
                with service.connection() as conn:
                    failed = conn.execute('SELECT * FROM deliveries WHERE sent=0').fetchone()
                    self.assertEqual(failed['first_attempt'], 0)
                self.client.post.side_effect = None
                self.client.post.return_value = response(200, 0)
                restarted = CheckService(root, roster=roster, sender=self.sender, portal_url=lambda: 'https://fixture.invalid')
                later = NOW + dt.timedelta(hours=1)
                self.assertIn('code=230013', restarted.state(ADMIN, 'A', later)['roster_error'])
                await asyncio.to_thread(restarted.tick, later)
                self.assertEqual(self.client.post.await_count, 3)
                self.assertEqual(self.client.post.call_args.kwargs['json']['uuid'], first_uuid)
                self.assertEqual(restarted.state(ADMIN, 'A', later)['roster_error'], '')
                await asyncio.to_thread(restarted.tick, later + dt.timedelta(minutes=5))
                self.assertEqual(self.client.post.await_count, 3)
                with restarted.connection() as conn:
                    self.assertEqual(conn.execute("SELECT COUNT(*) FROM deliveries WHERE sent=1 AND error='' AND message_id!=''").fetchone()[0], 2)
            asyncio.run(run())


class ScopedFailureTests(unittest.TestCase):
    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.sender = Mock(side_effect=receipt)
        self.roster = Mock(return_value={'people': [{'scope': scope, 'shift': shift, 'open_id': oid, 'name': '测试人员'}
            for scope in ('A', 'B') for shift in ('白', '夜') for oid in (LOCAL_ID, 'ou_peer')], 'warnings': {}})
        self.service = CheckService(folder.name, roster=self.roster, sender=self.sender, portal_url=lambda: 'https://fixture.invalid')
        clock = patch('lan_bitable_template_portal.pre_shift_checks.now', return_value=NOW)
        clock.start()
        self.addCleanup(clock.stop)
        for scope in ('A', 'B'):
            self.service.save_definition(ADMIN, scope, 0, [{'id': 'check', 'text': '检查'}])

    def error(self, scope='A', current=NOW):
        return self.service.state(ADMIN, scope, current)['roster_error']

    def fail_a(self, error=None):
        def send(recipient, card, identity):
            if ':A:' in identity and recipient == LOCAL_ID:
                raise error or CheckError('A楼接收人拒绝 code=230013')
            return receipt(recipient)
        self.sender.side_effect = send
        self.service.tick(NOW)

    def test_one_failure_does_not_block_peers_or_leak_across_buildings(self):
        self.fail_a()
        self.assertEqual(self.sender.call_count, 4)
        self.assertIn('230013', self.error())
        self.assertEqual(self.error('B'), '')
        self.assertEqual(self.error('110'), '')
        self.service.last_error = 'unrelated global error'
        self.assertNotIn('unrelated', self.error())
        self.assertEqual(self.error('B'), '')
        with self.service.connection() as conn:
            self.assertEqual(conn.execute('SELECT SUM(sent) FROM deliveries').fetchone()[0], 3)
        self.service.notify(NOW + dt.timedelta(minutes=1))
        self.assertEqual(self.sender.call_count, 4)
        self.assertIn('230013', self.error())

    def test_direct_notify_success_clears_only_corresponding_failure(self):
        self.sender.side_effect = CheckError('待恢复')
        self.service.tick(NOW)
        def send(oid, card, key):
            if ':B:' in key:
                raise CheckError('B待恢复')
            return receipt(oid)
        self.sender.side_effect = send
        later = NOW + dt.timedelta(minutes=5)
        self.service.notify(later)
        self.assertEqual(self.error(current=later), '')
        self.assertIn('B待恢复', self.error('B', later))

    def test_latest_success_hides_older_round_failure_but_keeps_queue_history(self):
        self.fail_a()
        with self.service.connection(True) as conn:
            conn.execute('INSERT INTO deliveries(id) VALUES(?)', (NOW.date().isoformat() + ':A:白:1:' + LOCAL_ID,))
        self.assertIn('230013', self.error(current=NOW + dt.timedelta(hours=2)))
        self.sender.side_effect = receipt
        later = NOW + dt.timedelta(hours=2)
        self.service.notify(later)
        self.assertEqual(self.error(current=later), '')
        with self.service.connection() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM deliveries WHERE sent=0 AND error!=''").fetchone()[0], 1)
        self.sender.side_effect = CheckError('新一轮失败')
        self.service.notify(NOW + dt.timedelta(hours=4))
        self.assertIn('新一轮失败', self.error(current=NOW + dt.timedelta(hours=4)))
        self.assertNotIn('230013', self.error(current=NOW + dt.timedelta(hours=4)))

    def test_submitted_and_expired_tasks_do_not_show_old_errors(self):
        self.fail_a()
        with self.service.connection() as conn:
            task = conn.execute("SELECT * FROM tasks WHERE scope='A' AND due<=?", (NOW.timestamp(),)).fetchone()
        self.service.submit(ADMIN, task['id'], task['definition_version'], [{'id': 'check', 'status': 'normal', 'note': ''}], NOW)
        self.assertEqual(self.error(), '')
        self.assertEqual(self.error(current=NOW.replace(hour=18)), '')
        self.service.notify(NOW + dt.timedelta(minutes=5))
        self.assertEqual(self.sender.call_count, 4)

    def test_expired_unsubmitted_and_future_tasks_hide_errors(self):
        self.fail_a()
        self.assertEqual(self.error(current=NOW.replace(hour=18)), '')
        self.assertEqual(self.error(current=NOW - dt.timedelta(minutes=11)), '')

    def test_removed_recipient_unassigned_items_and_stale_definition_hide_errors(self):
        self.fail_a()
        with self.service.connection(True) as conn:
            task = dict(conn.execute("SELECT * FROM tasks WHERE scope='A' AND due<=?", (NOW.timestamp(),)).fetchone())
            conn.execute('UPDATE tasks SET people=? WHERE id=?', (json.dumps([{'open_id': 'ou_peer'}]), task['id']))
        self.assertEqual(self.error(), '')
        with self.service.connection(True) as conn:
            conn.execute('UPDATE tasks SET people=?,items=? WHERE id=?', (task['people'], json.dumps([
                {'id': 'check', 'text': '检查', 'recipient_ids': ['ou_peer']}]), task['id']))
        self.assertEqual(self.error(), '')
        with self.service.connection(True) as conn:
            conn.execute('UPDATE tasks SET items=? WHERE id=?', (task['items'], task['id']))
            conn.execute("UPDATE definitions SET version=version+1 WHERE scope='A'")
        self.assertEqual(self.error(), '')
        self.service.save_definition(ADMIN, 'A', 2, [])
        self.assertEqual(self.error(), '')

    def test_network_uncertainty_survives_restart_and_keeps_dedup_limit(self):
        self.fail_a(CheckDeliveryUncertain('网络回执不确定'))
        restarted = CheckService(self.service.db.parent, sender=self.sender, portal_url=lambda: 'https://fixture.invalid')
        self.assertIn('网络回执不确定', restarted.state(ADMIN, 'A', NOW)['roster_error'])
        restarted.notify(NOW + dt.timedelta(minutes=5))
        self.assertEqual(self.sender.call_count, 5)
        self.assertEqual(self.sender.call_args_list[0].args[2], self.sender.call_args.args[2])
        restarted.notify(NOW + dt.timedelta(minutes=50))
        self.assertEqual(self.sender.call_count, 5)
        self.assertIn('原消息回执未确认', restarted.state(ADMIN, 'A', NOW + dt.timedelta(minutes=50))['roster_error'])
        self.assertEqual(restarted.state(ADMIN, 'B', NOW)['roster_error'], '')

    def test_invalid_portal_url_is_scoped_to_pending_tasks_and_recovers(self):
        self.service.prepare(NOW)
        self.service.submit(ADMIN, NOW.date().isoformat() + ':B:白', 1,
                            [{'id': 'check', 'status': 'normal', 'note': ''}], NOW)
        self.service.portal_url = lambda: ''
        self.service.notify(NOW)
        self.sender.assert_not_called()
        self.assertIn('访问地址未配置', self.error())
        self.assertEqual(self.error('B'), '')
        self.service.portal_url = lambda: 'https://fixture.invalid'
        self.service.notify(NOW + dt.timedelta(minutes=5))
        self.assertEqual(self.error(), '')

    def test_roster_warning_and_delivery_failure_both_remain_scope_local(self):
        self.roster.return_value['warnings'] = {'A': 'A排班待补齐'}
        self.fail_a()
        self.assertIn('A排班待补齐', self.error())
        self.assertIn('230013', self.error())
        self.assertEqual(self.error('B'), '')


if __name__ == '__main__':
    unittest.main()
