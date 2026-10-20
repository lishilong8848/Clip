"""Offline credential reload and owned-worker lifecycle checks; no cloud calls."""
import asyncio
import copy
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(0, str(Path(__file__).parent))
from lan_bitable_template_portal.feishu_assistant import FeishuAssistant, FeishuMessenger, Inbox
from lan_bitable_template_portal.feishu_assistant_settings import read_config
from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.protocol import atomic_json
from starlette.requests import Request


def config(app='cli_fixtureA', secret='fixture-secret-A', *, enabled=True):
    return {'app_id': app, 'enabled': enabled, 'secret_cipher': 'fixture:' + secret,
            'bridge_cipher': 'fixture:bridge-' + app}


def decrypt(value):
    if not value.startswith('fixture:'):
        raise ValueError('invalid fixture cipher')
    return value[len('fixture:'):]


class Process:
    def __init__(self, events, pid):
        self.events, self.pid, self.running = events, pid, True
        self.stubborn, self.unkillable, self.killed = False, False, False

    def poll(self):
        return None if self.running else 0

    def terminate(self):
        self.events.append(('terminate', self.pid))
        if not self.stubborn:
            self.running = False

    def kill(self):
        self.events.append(('kill', self.pid))
        self.killed = True
        if not self.unkillable:
            self.running = False

    def wait(self, timeout):
        self.events.append(('wait', self.pid))
        if self.running:
            raise subprocess.TimeoutExpired('fixture-worker', timeout)
        return 0


class ConfigReloadTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.db_path = self.root / 'portal.sqlite3'
        with closing(sqlite3.connect(self.db_path)) as db, db:
            db.execute('CREATE TABLE preserved(kind TEXT PRIMARY KEY,payload TEXT)')
            db.executemany('INSERT INTO preserved VALUES(?,?)', [
                ('session', '{"user":"fixture-person"}'),
                ('permissions', '{"scopes":["D"]}'),
                ('submitted-plan', '{"status":"submitted"}')])
        self.runtime = SimpleNamespace(state_store=SimpleNamespace(db_path=self.db_path),
                                       close=AsyncMock(), cancel=AsyncMock())
        self.bot = FeishuAssistant(SimpleNamespace(bound_port=18766, preferred_port=18766),
                                   self.runtime, AsyncMock())
        self.events, self.processes = [], []
        self.popen = self.enterContext(patch(
            'lan_bitable_template_portal.feishu_assistant.subprocess.Popen', side_effect=self.spawn))
        self.register = self.enterContext(patch(
            'upload_event_module.services.process_lifetime.register_child_process', return_value=True))
        self.enterContext(patch('lan_bitable_template_portal.feishu_assistant.unprotect_key', side_effect=decrypt))
        self.enterContext(patch('lan_bitable_template_portal.portal_service.external_real_write_guard',
                                return_value={'real_write_allowed': True}))
        # Unexpected network activity is a test failure, including token requests.
        self.enterContext(patch('httpx.AsyncClient', side_effect=AssertionError('network forbidden')))
        self.run = self.enterContext(patch.object(self.bot, '_run', AsyncMock(side_effect=self.park)))
        self.addAsyncCleanup(self.bot.close)
        self.write(config())

    async def park(self):
        await asyncio.Event().wait()

    def spawn(self, command, **kwargs):
        if any(p.running for p in self.processes):
            raise AssertionError('overlapping owned workers')
        process = Process(self.events, len(self.processes) + 4200)
        self.processes.append(process)
        snapshot = Path(command[command.index('--config') + 1])
        self.events.append(('spawn', process.pid, json.loads(snapshot.read_text(encoding='utf-8'))))
        return process

    def write(self, value):
        atomic_json(self.bot.config_path, value)

    def message(self, identity='om_Fixture', *, state='pending', reply=None):
        message = {'app_id': self.bot.config['app_id'], 'message_id': identity,
                   'chat_id': 'oc_Fixture', 'chat_type': 'p2p', 'sender_id': 'ou_Fixture',
                   'union_id': 'on_Fixture', 'question': 'fixture question'}
        self.bot.inbox.add(message)
        if state != 'pending' or reply is not None:
            self.bot.inbox.set(identity, state, reply)
        return message

    def rows(self, inbox=None):
        with (inbox or self.bot.inbox).connect() as db:
            return {row[0]: row[1:] for row in db.execute('SELECT id,state,payload,reply FROM messages')}

    def preserved(self):
        with closing(sqlite3.connect(self.db_path)) as db:
            return db.execute('SELECT * FROM preserved ORDER BY kind').fetchall()

    def request(self, bridge, body=None):
        async def receive():
            return {'type': 'http.request', 'body': json.dumps(body or {
                'kind': 'ready', 'bot_id': 'ou_Ready'}).encode(), 'more_body': False}
        return Request({'type': 'http', 'method': 'POST', 'path': '/', 'client': ('127.0.0.1', 1),
                        'headers': [(b'authorization', ('Bearer ' + bridge).encode())]}, receive)

    async def test_concurrent_start_and_worker_start_create_only_one_connection(self):
        await asyncio.gather(self.bot.start(), self.bot.start(), self.bot.start())
        await asyncio.sleep(0)
        self.assertEqual(len(self.processes), 1)
        self.run.assert_awaited_once()
        runner = self.bot.runner
        await asyncio.gather(self.bot._start_worker(), self.bot._start_worker(), self.bot.start())
        self.assertIs(self.bot.runner, runner)
        self.assertEqual(len(self.processes), 1)
        self.register.assert_called_once_with(self.processes[0].pid)

    async def test_secret_rotation_preserves_inbox_sessions_and_business_but_clears_caches(self):
        await self.bot.start()
        inbox, old = self.bot.inbox, self.bot.messenger
        self.message(reply={'text': 'saved result', 'sent_parts': ['answer:0']}, state='reply')
        self.bot.sessions['ou_Fixture'] = 'fixture-session-token'
        self.bot.bot_id = 'ou_OldBot'
        old.token, old.expires = 'fixture-old-token', float('inf')
        old.directory_records, old.directory_expires = {'ou_Old': {'recOld'}}, float('inf')
        before, preserved = self.rows(), self.preserved()
        next_config = config(secret='fixture-secret-B')
        self.write(next_config)
        self.assertTrue(await self.bot._refresh_config(force=True))
        self.assertIs(self.bot.inbox, inbox)
        self.assertEqual(self.rows(), before)
        self.assertEqual(self.preserved(), preserved)
        self.assertEqual(self.bot.sessions, {'ou_Fixture': 'fixture-session-token'})
        self.assertEqual(self.bot.bot_id, '')
        self.assertTrue(old.closed)
        self.assertEqual((old.token, old.directory_records), ('', {}))
        for call in (old.headers, old.client, old.personnel_directory):
            with self.assertRaises(AssistantError):
                await call()
        new = self.bot.messenger
        self.assertIsNot(new, old)
        self.assertEqual((new.token, new.directory_records, new.expires), ('', {}, 0))
        new.http = SimpleNamespace(post=AsyncMock(return_value=SimpleNamespace(json=lambda: {
            'code': 0, 'tenant_access_token': 'fixture-new-token', 'expire': 7200})), aclose=AsyncMock())
        headers = await asyncio.gather(new.headers(), new.headers())
        self.assertEqual(headers, [{'Authorization': 'Bearer fixture-new-token'}] * 2)
        new.http.post.assert_awaited_once()
        self.assertEqual(new.http.post.await_args.kwargs['json'], {
            'app_id': next_config['app_id'], 'app_secret': 'fixture-secret-B'})
        self.assertEqual([e[0] for e in self.events], ['spawn', 'terminate', 'wait', 'spawn'])
        self.runtime.close.assert_not_awaited()
        self.runtime.cancel.assert_not_awaited()

    async def test_app_rotation_stops_old_pending_and_preserves_history_and_bridge_boundary(self):
        await self.bot.start()
        old_inbox, old_bridge = self.bot.inbox, self.bot.secret
        saved = {'text': 'uncertain old-app result', 'sent_parts': ['answer:0'], 'awaiting_turn': 'run-fixture'}
        self.message('om_Pending')
        self.message('om_Processing', state='processing', reply=saved)
        self.message('om_Reply', state='reply', reply=saved)
        self.message('om_Done', state='done', reply={'text': 'historical delivered result'})
        before = self.rows(old_inbox)
        preserved = self.preserved()
        self.write({**config('cli_fixtureB', 'fixture-secret-B'), 'isolated_inbox': True})
        await self.bot._refresh_config(force=True)
        suffix = hashlib.sha256(b'cli_fixtureB').hexdigest()[:24]
        self.assertEqual(self.bot.inbox.path.name, 'feishu_assistant_' + suffix + '.sqlite3')
        self.assertEqual(self.rows(), {})
        after = self.rows(old_inbox)
        for key in ('om_Pending', 'om_Processing', 'om_Reply'):
            self.assertEqual(after[key][0], 'stopped')
            self.assertEqual(after[key][1:], before[key][1:])
        self.assertEqual(after['om_Done'], before['om_Done'])
        self.assertEqual(self.preserved(), preserved)
        self.assertEqual((await self.bot.receive(self.request(old_bridge))).status_code, 403)
        self.assertEqual((await self.bot.receive(self.request(self.bot.secret))).status_code, 200)
        self.assertEqual(self.bot.bot_id, 'ou_Ready')
        self.assertNotEqual(self.bot.secret, old_bridge)

    async def test_changed_revision_blocks_every_round_and_cached_token_before_reload(self):
        await self.bot.start()
        old = self.bot.messenger
        old.token, old.expires = 'fixture-cached-token', float('inf')
        old.directory_records, old.directory_expires = {'ou_Old': {'recOld'}}, float('inf')
        old.http = SimpleNamespace(post=AsyncMock(), aclose=AsyncMock())
        message = self.message()
        self.bot.answer = AsyncMock()
        self.write(config(secret='fixture-updated-secret'))
        self.assertFalse(await self.bot._configuration_current())
        self.assertTrue(self.bot.event.is_set())
        for call in (old.headers, old.personnel_directory,
                     lambda: self.bot._request_context(message),
                     lambda: self.bot._validate_delivery(message, {'text': 'notice without actor'}),
                     lambda: self.bot._process_message(message),
                     lambda: self.bot._call_once(None, {}, 'plans/fixture/submit', {})):
            with self.assertRaises(AssistantError):
                await call()
        old.http.post.assert_not_awaited()
        self.bot.answer.assert_not_awaited()
        self.assertEqual((await self.bot.receive(self.request(self.bot.secret))).status_code, 503)
        await self.bot._refresh_config(force=True)
        self.assertTrue(await self.bot._configuration_current())

    async def test_revision_change_during_token_request_never_publishes_old_token(self):
        await self.bot.start()
        messenger = self.bot.messenger
        async def token(*args, **kwargs):
            self.write(config(secret='fixture-rotated-during-request'))
            return SimpleNamespace(json=lambda: {'code': 0, 'tenant_access_token': 'fixture-stale', 'expire': 7200})
        messenger.http = SimpleNamespace(post=AsyncMock(side_effect=token), aclose=AsyncMock())
        with self.assertRaises(AssistantError):
            await messenger.headers()
        self.assertEqual(messenger.token, '')

    async def test_late_old_worker_ready_cannot_overwrite_new_bot_id(self):
        await self.bot.start()
        entered, release = asyncio.Event(), asyncio.Event()
        async def body():
            entered.set()
            await release.wait()
            return {'type': 'http.request', 'body': b'{"kind":"ready","bot_id":"ou_OldReady"}',
                    'more_body': False}
        request = Request({'type': 'http', 'method': 'POST', 'path': '/', 'client': ('127.0.0.1', 1),
                           'headers': [(b'authorization', ('Bearer ' + self.bot.secret).encode())]}, body)
        callback = asyncio.create_task(self.bot.receive(request))
        await asyncio.wait_for(entered.wait(), 3)
        self.write(config('cli_fixtureB', 'fixture-B'))
        await self.bot._refresh_config(force=True)
        self.bot.bot_id = 'ou_NewReady'
        release.set()
        self.assertEqual((await callback).status_code, 503)
        self.assertEqual(self.bot.bot_id, 'ou_NewReady')

    async def test_disabled_channel_stops_owned_worker_and_can_enable_in_existing_loop(self):
        await self.bot.start()
        original_inbox, runner = self.bot.inbox, self.bot.runner
        self.write(config(enabled=False))
        self.assertFalse(await self.bot._refresh_config(force=True))
        self.assertIsNone(self.bot.process)
        self.assertIsNone(self.bot.messenger)
        self.assertIs(self.bot.runner, runner)
        self.assertFalse(await self.bot._refresh_config(force=True))
        self.assertEqual(len(self.processes), 1)
        self.write(config(secret='fixture-new-secret'))
        self.assertTrue(await self.bot._refresh_config(force=True))
        self.assertIs(self.bot.inbox, original_inbox)
        self.assertEqual(len(self.processes), 2)

    async def test_app_change_while_disabled_still_isolates_old_inbox(self):
        await self.bot.start()
        inbox = self.bot.inbox
        self.message()
        self.write(config('cli_fixtureB', 'fixture-B', enabled=False))
        await self.bot._refresh_config(force=True)
        self.assertEqual(self.rows(inbox)['om_Fixture'][0], 'stopped')
        self.assertIsNone(self.bot.inbox)
        self.write(config('cli_fixtureB', 'fixture-B'))
        await self.bot._refresh_config(force=True)
        self.assertNotEqual(self.bot.inbox.path, inbox.path)
        self.assertEqual(self.rows(), {})

    async def test_invalid_json_and_cipher_fail_closed_then_valid_config_recovers(self):
        await self.bot.start()
        inbox = self.bot.inbox
        self.message(reply={'text': 'saved'}, state='reply')
        before = self.rows()
        atomic_json(self.bot.config_path, ['invalid object'])
        self.assertFalse(await self.bot._refresh_config(force=True))
        self.assertIsNone(self.bot.process)
        self.assertIsNone(self.bot.messenger)
        self.assertEqual(self.rows(inbox), before)
        self.write({**config(), 'secret_cipher': 'invalid fixture cipher'})
        self.assertFalse(await self.bot._refresh_config(force=True))
        self.assertEqual(len(self.processes), 1)
        self.write(config(secret='fixture-fixed'))
        self.assertTrue(await self.bot._refresh_config(force=True))
        self.assertIs(self.bot.inbox, inbox)
        self.assertEqual(len(self.processes), 2)

    async def test_metadata_only_change_does_not_restart_and_periodic_check_is_throttled(self):
        await self.bot.start()
        process, messenger = self.bot.process, self.bot.messenger
        self.write({**config(), 'fixture_metadata': 'changed'})
        self.assertTrue(await self.bot._refresh_config(force=True))
        self.assertIs(self.bot.process, process)
        self.assertIs(self.bot.messenger, messenger)
        self.assertEqual(messenger.config_revision, read_config(self.bot.config_path)[1])
        with patch('lan_bitable_template_portal.feishu_assistant_settings.read_config', wraps=read_config) as read:
            for _ in range(50):
                self.assertTrue(await self.bot._refresh_config())
            read.assert_not_called()

    async def test_transient_inbox_failure_retries_same_revision_and_leaves_healthy_transport_alone(self):
        await self.bot.start()
        old_inbox, old_revision = self.bot.inbox, self.bot._config_revision
        self.message()
        next_config = config('cli_fixtureB', 'fixture-B')
        self.write(next_config)
        with patch('lan_bitable_template_portal.feishu_assistant.Inbox',
                   side_effect=[sqlite3.OperationalError('fixture transient lock'), Inbox(
                       self.root / ('feishu_assistant_' + hashlib.sha256(b'cli_fixtureB').hexdigest()[:24] + '.sqlite3'))]) as create:
            self.assertFalse(await self.bot._refresh_config(force=True))
            self.assertEqual(self.bot.config, next_config)
            self.assertEqual(self.bot._config_revision, old_revision)
            self.assertIsNone(self.bot.messenger)
            self.assertIsNone(self.bot.process)
            self.assertTrue(await self.bot._refresh_config(force=True))
            process, messenger = self.bot.process, self.bot.messenger
            self.assertTrue(await self.bot._refresh_config(force=True))
            self.assertIs(self.bot.process, process)
            self.assertIs(self.bot.messenger, messenger)
            self.assertEqual(create.call_count, 2)
        self.assertEqual(self.rows(old_inbox)['om_Fixture'][0], 'stopped')
        self.assertEqual(len(self.processes), 2)

    async def test_transient_messenger_failure_retries_unchanged_revision_with_original_inbox(self):
        await self.bot.start()
        inbox, old_revision = self.bot.inbox, self.bot._config_revision
        self.message(state='reply', reply={'text': 'preserved saved result'})
        before = self.rows()
        next_config = config(secret='fixture-next-secret')
        self.write(next_config)
        with patch('lan_bitable_template_portal.feishu_assistant.FeishuMessenger',
                   side_effect=[RuntimeError('fixture transient setup failure'), FeishuMessenger(next_config)]) as create:
            self.assertFalse(await self.bot._refresh_config(force=True))
            self.assertEqual(self.bot.config, next_config)
            self.assertEqual(self.bot._config_revision, old_revision)
            self.assertTrue(await self.bot._refresh_config(force=True))
            process = self.bot.process
            self.assertTrue(await self.bot._refresh_config(force=True))
            self.assertIs(self.bot.process, process)
            self.assertEqual(create.call_count, 2)
        self.assertIs(self.bot.inbox, inbox)
        self.assertEqual(self.rows(), before)
        self.assertEqual(self.bot._config_revision, read_config(self.bot.config_path)[1])
        self.assertEqual(len(self.processes), 2)

    async def test_owned_worker_timeout_kills_only_owned_handle_before_replacement(self):
        await self.bot.start()
        old = self.bot.process
        old.stubborn = True
        unrelated = Process([], 9999)
        self.write(config(secret='fixture-rotated'))
        self.assertTrue(await self.bot._refresh_config(force=True))
        self.assertEqual([e[0] for e in self.events], ['spawn', 'terminate', 'wait', 'kill', 'wait', 'spawn'])
        self.assertFalse(old.running)
        self.assertTrue(unrelated.running)
        self.assertEqual(unrelated.events, [])

    async def test_unconfirmed_worker_exit_never_starts_replacement(self):
        await self.bot.start()
        old = self.bot.process
        old.stubborn = old.unkillable = True
        self.write(config(secret='fixture-rotated'))
        self.assertFalse(await self.bot._refresh_config(force=True))
        self.assertIs(self.bot.process, old)
        self.assertIsNone(self.bot.messenger)
        self.assertEqual(len(self.processes), 1)
        old.unkillable = False
        self.assertTrue(await self.bot._refresh_config(force=True))
        self.assertEqual(len(self.processes), 2)

    async def test_inflight_delivery_keeps_original_reply_without_resubmitting_business(self):
        await self.bot.start()
        message = self.message()
        reply = {'text': 'submitted task result', 'awaiting_turn': 'fixture-running-turn',
                 'sent_parts': ['live:0'], 'live_reply': {'message_id': 'om_Original'}}
        self.bot.answer = AsyncMock(return_value=copy.deepcopy(reply))
        self.bot._react = AsyncMock()
        self.bot._recover_answer = AsyncMock(side_effect=lambda _, saved: saved)
        entered = asyncio.Event()
        async def send(*args, **kwargs):
            entered.set()
            await asyncio.Event().wait()
        self.bot.messenger.send = AsyncMock(side_effect=send)
        lane = self.bot.inbox.queue_key(message)
        task = asyncio.create_task(self.bot._drain(message['union_id'], lane))
        self.bot.tasks[(message['union_id'], lane)] = task
        await asyncio.wait_for(entered.wait(), 3)
        self.write(config(secret='fixture-next-secret'))
        await self.bot._refresh_config(force=True)
        self.assertTrue(task.cancelled())
        self.assertEqual(self.bot.inbox.reply_for(message['message_id']), reply)
        self.bot.answer.assert_awaited_once()
        self.runtime.cancel.assert_not_awaited()
        self.runtime.close.assert_not_awaited()

    async def test_app_switch_during_answer_preserves_uncertain_old_outcome_only_in_old_inbox(self):
        await self.bot.start()
        message, inbox = self.message(), self.bot.inbox
        entered = asyncio.Event()
        async def answer(_):
            entered.set()
            await asyncio.Event().wait()
        self.bot._react = AsyncMock()
        self.bot.answer = AsyncMock(side_effect=answer)
        lane = self.bot.inbox.queue_key(message)
        task = asyncio.create_task(self.bot._drain(message['union_id'], lane))
        self.bot.tasks[(message['union_id'], lane)] = task
        await asyncio.wait_for(entered.wait(), 3)
        self.write(config('cli_fixtureB', 'fixture-secret-B'))
        await self.bot._refresh_config(force=True)
        old = self.rows(inbox)[message['message_id']]
        self.assertEqual(old[0], 'stopped')
        self.assertIn('状态尚未确认', json.loads(old[2])['text'])
        self.assertEqual(self.rows(), {})
        self.bot.answer.assert_awaited_once()

    async def test_existing_run_loop_detects_revision_change_without_a_second_service(self):
        await self.bot.start()
        self.bot.runner.cancel()
        await asyncio.gather(self.bot.runner, return_exceptions=True)
        self.bot.runner = asyncio.create_task(FeishuAssistant._run(self.bot))
        self.write(config(secret='fixture-loop-secret'))
        self.bot._next_config_check = 0
        self.bot.event.set()
        async def changed():
            while len(self.processes) < 2:
                await asyncio.sleep(.01)
        await asyncio.wait_for(changed(), 3)
        self.assertEqual(self.bot.config['secret_cipher'], 'fixture:fixture-loop-secret')
        self.assertIsNotNone(self.bot.runner)
        self.assertFalse(self.bot.runner.done())

    async def test_close_during_popen_waits_and_stops_child_without_orphan(self):
        started, release = threading.Event(), threading.Event()
        spawn = self.spawn
        def slow_spawn(*args, **kwargs):
            started.set()
            release.wait(5)
            return spawn(*args, **kwargs)
        self.popen.side_effect = slow_spawn
        start = asyncio.create_task(self.bot.start())
        self.addCleanup(release.set)
        self.assertTrue(await asyncio.to_thread(started.wait, 3))
        start.cancel()
        closing = asyncio.create_task(self.bot.close())
        await asyncio.sleep(.02)
        self.assertFalse(closing.done())
        release.set()
        await asyncio.gather(start, closing, return_exceptions=True)
        self.assertEqual(len(self.processes), 1)
        self.assertFalse(self.processes[0].running)
        self.assertIsNone(self.bot.process)
        self.assertIsNone(self.bot.messenger)


if __name__ == '__main__':
    unittest.main()
