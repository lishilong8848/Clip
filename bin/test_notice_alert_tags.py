"""Isolated tag recommendations: never call a real model or send a real message."""
import asyncio
import copy
import json
import re
import subprocess
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
import httpx
from fastapi import FastAPI
from fastapi.responses import JSONResponse

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lan_bitable_template_portal.state_store import LanPortalStateStore
from lan_bitable_template_portal.notice_alert_tags import NoticeAlertTags, NS, CHANNEL, install_notice_alert_tag_routes
from openclaw_service.assistant.lighthouse_alert_tagging import clean_tags, fallback_text, recommend, rules, tag_text
from openclaw_service.assistant import lighthouse_skills


def tags(name):
    return [{'label': '维护', 'content': name, 'basis': '已发维护通告，包含维护操作。', 'notes': '窗口及实际告警触发关系待现场核对。'}]


def minimal_tags(name):
    return [{'label': '维护', 'content': name}]


class AlertTagsTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.store = LanPortalStateStore(Path(temp.name) / 'test.sqlite3')
        self.calls = []
        async def model(owner, notices):
            self.calls.append((owner, copy.deepcopy(notices)))
            return {item['id']: tags(item['title']) for item in notices}
        def recipients(scopes, **_):
            return '', list(dict.fromkeys(['ou_' + scope for scope in scopes] + ['ou_li'])), ''
        self.service = SimpleNamespace(_state_store=self.store, _recipients_for_building_codes=Mock(side_effect=recipients))
        self.sender = Mock(return_value=(True, 'ok', []))
        self.tags = NoticeAlertTags(self.service, model, self.sender)

    def enqueue(self, operation='one', *, target='recE', owner='ou_owner', scopes=None, action='start'):
        scopes = scopes or ['E']
        self.tags.enqueue({'notice_type': '维保通告', 'title': operation, 'text': '完整通告：' + operation,
                           'building_codes': scopes, 'action': action}, operation_id=operation,
                          target_record_id=target, request={'_auth_open_id': owner})

    async def test_whole_notice_batch_keeps_recipients_separate_and_copies_li_once(self):
        self.enqueue('E楼维护')
        self.enqueue('ABC楼维护', target='recABC', scopes=['A', 'B', 'C'])
        self.assertFalse(self.calls)
        self.sender.assert_not_called()
        await self.tags.tick()
        self.assertEqual(len(self.calls), 1)
        self.assertEqual([n['text'] for n in self.calls[0][1]], ['完整通告：E楼维护', '完整通告：ABC楼维护'])
        sent = {call.args[1][0]: call.args[0] for call in self.sender.call_args_list}
        self.assertEqual(set(sent), {'ou_E', 'ou_A', 'ou_B', 'ou_C', 'ou_li'})
        self.assertNotIn('ABC楼维护', sent['ou_E'])
        self.assertNotIn('E楼维护', sent['ou_A'])
        self.assertIn('ABC楼维护', sent['ou_li'])
        self.assertIn('E楼维护', sent['ou_li'])
        self.assertEqual(self.tags.latest('recE', 'maintenance', ['E'])['status'], 'ready')
        self.assertIsNone(self.tags.latest('recE', 'maintenance', ['A']))
        self.assertEqual(self.tags.latest('recABC', 'maintenance', ['A'])['status'], 'ready')
        self.assertIsNone(self.tags.latest('recABC', 'maintenance', ['D']))

    async def test_dedupe_and_old_results_cannot_replace_latest_update(self):
        self.enqueue('start')
        first = self.tags.latest('recE', 'maintenance', ['E'])['id']
        self.enqueue('update', action='update')
        latest = self.tags.latest('recE', 'maintenance', ['E'])['id']
        self.enqueue('start')
        self.enqueue('end', action='end')
        self.assertNotEqual(first, latest)
        self.assertEqual(self.tags.latest('recE', 'maintenance', ['E'])['id'], latest)
        await self.tags.tick()
        self.assertEqual(self.tags.latest('recE', 'maintenance', ['E'])['tags'][0]['content'], 'update')
        count = self.sender.call_count
        self.enqueue('update', action='update')
        await self.tags.tick()
        self.assertEqual(self.sender.call_count, count)

    async def test_accounts_are_grouped_separately_with_bounded_concurrency(self):
        active = peak = 0
        together = asyncio.Event()
        async def model(owner, notices):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            self.assertEqual(len({row['title'].split('-')[0] for row in notices}), 1)
            if active == 2:
                together.set()
            await asyncio.wait_for(together.wait(), 2)
            active -= 1
            self.calls.append(owner)
            return {row['id']: tags(row['title']) for row in notices}
        self.tags.recommend = model
        for i in range(5):
            for j in range(2):
                self.enqueue(f'user{i}-{j}', target=f'rec{i}', owner=f'ou_{i}')
        await self.tags.tick()
        self.assertEqual(len(self.calls), 5)
        self.assertEqual(peak, 2)

    async def test_model_failure_sends_short_concise_fallback_to_floor_and_li(self):
        attempts = []
        async def fail(*args):
            attempts.append(args)
            raise TimeoutError()
        self.tags.recommend = fail
        self.enqueue()
        await self.tags.tick()
        self.assertEqual(len(attempts), 2)
        self.assertEqual({call.args[1][0] for call in self.sender.call_args_list}, {'ou_E', 'ou_li'})
        for call in self.sender.call_args_list:
            body = call.args[0]
            self.assertEqual(body.count('推荐标签获取失败'), 1)
            self.assertIn(fallback_text('维保通告'), body)
            self.assertNotIn('易错点速查', body)
            self.assertNotIn('name: alert-tagging', body)
            self.assertNotIn('【检修】', body)
            self.assertNotIn('依据', body)
            self.assertNotIn('注意', body)
            self.assertEqual(len(body.split('\n\n')[1].splitlines()), 2)
        visible = self.tags.latest('recE', 'maintenance', ['E'])
        self.assertEqual(visible['status'], 'failed')
        self.assertEqual(visible['error'], fallback_text('维保通告'))

    def test_fallback_is_one_concise_failure_sentence_for_each_notice_type_without_reading_skill(self):
        kinds = ['维保通告', '变更通告', '设备检修', '设备轮巡', '设备调整',
                 '上电通告', '下电通告', '事件通告', '未识别类型']
        with patch('openclaw_service.assistant.lighthouse_alert_tagging.rules', side_effect=AssertionError('no full skill in fallback')):
            for kind in kinds:
                with self.subTest(kind=kind):
                    body = fallback_text(kind)
                    self.assertEqual(body, '推荐标签获取失败，通告业务不受影响。')
                    self.assertEqual(len(body.splitlines()), 1)
                    self.assertLess(len(body), 200)

    def test_mixed_failed_batch_uses_each_notice_type_and_preserves_ready_tags(self):
        self.enqueue('maintenance', scopes=['E'])
        self.tags.enqueue({'notice_type': '事件通告', 'title': 'event', 'text': '事件原文',
                           'building_codes': ['A'], 'action': 'update'}, operation_id='event',
                          target_record_id='recEvent', request={'_auth_open_id': 'ou_owner'})
        self.enqueue('ready', target='recReady', scopes=['E'])
        jobs = [row['payload'] for row in self.store.list_documents(NS, key_prefix='job:')]
        for job in jobs:
            job.update(status='ready' if job['title'] == 'ready' else 'failed',
                       tags=tags('已生成的正常标签') if job['title'] == 'ready' else [],
                       error='旧的失败提示：' + rules())
            self.store.put_document(NS, 'job:' + job['id'], job)
        batch = self.tags.batch(jobs)
        self.tags.notify(batch, jobs)
        sent = {call.args[1][0]: call.args[0] for call in self.sender.call_args_list}
        self.assertIn(fallback_text('事件通告'), sent['ou_A'])
        self.assertNotIn('【维护】', sent['ou_A'])
        self.assertIn(fallback_text('维保通告'), sent['ou_E'])
        self.assertNotIn('事件通告需按实际根因', sent['ou_E'])
        self.assertIn('已生成的正常标签', sent['ou_E'])
        self.assertEqual(sent['ou_li'].count('推荐标签获取失败'), 2)
        self.assertNotIn('现场告警打标规则（完整参考）', sent['ou_li'])
        self.assertEqual(self.tags.latest('recEvent', 'event', ['A'])['error'], fallback_text('事件通告'))
        self.assertFalse(self.calls)

    async def test_notification_retry_does_not_regenerate_or_resend_success(self):
        self.sender.side_effect = [(True, 'ok', []), TimeoutError(), (True, 'ok', [])]
        self.enqueue()
        await self.tags.tick()
        restarted = NoticeAlertTags(self.service, self.tags.recommend, self.sender)
        await restarted.tick()
        self.assertEqual(len(self.calls), 1)
        self.assertEqual([call.args[1][0] for call in self.sender.call_args_list], ['ou_E', 'ou_li', 'ou_li'])
        self.assertEqual(self.sender.call_args_list[1].kwargs['message_uuid'], self.sender.call_args_list[2].kwargs['message_uuid'])

    async def test_old_failure_regenerates_for_display_only_without_resending(self):
        self.enqueue()
        await self.tags.tick()
        original = self.store.list_documents(NS, key_prefix='job:')[0]
        job = {**original['payload'], 'status': 'failed', 'tags': [], 'finished_at': 1, 'error': 'old failure'}
        self.store.put_document(NS, original['key'], job)
        receipts = copy.deepcopy(self.store.list_documents(NS, key_prefix='batch:'))
        self.sender.reset_mock()
        self.calls.clear()
        self.assertIsNone(self.tags.latest('recE', 'maintenance', ['A']))
        self.assertEqual(self.store.list_outbox_events(CHANNEL), [])
        for _ in range(3):
            self.assertEqual(self.tags.latest('recE', 'maintenance', ['E'])['status'], 'pending')
        self.assertEqual(len(self.store.list_outbox_events(CHANNEL)), 1)
        self.store.lease_outbox_events(CHANNEL)
        self.store.release_outbox_leases(CHANNEL)
        restarted = NoticeAlertTags(self.service, self.tags.recommend, self.sender)
        await restarted.tick()
        visible = restarted.latest('recE', 'maintenance', ['E'])
        self.assertEqual(visible['status'], 'ready')
        self.assertEqual(visible['tags'], clean_tags(tags('one')))
        self.assertEqual(len(self.calls), 1)
        self.sender.assert_not_called()
        self.assertEqual(self.store.get_document(NS, original['key']), job)
        self.assertEqual(self.store.list_documents(NS, key_prefix='batch:'), receipts)

    async def test_display_failure_backs_off_and_superseded_update_is_not_overwritten(self):
        self.enqueue()
        await self.tags.tick()
        original = self.store.list_documents(NS, key_prefix='job:')[0]
        self.store.put_document(NS, original['key'], {**original['payload'], 'status': 'failed', 'finished_at': 1})
        now = [1000]
        self.tags.clock = lambda: now[0]
        async def fail(*_): raise TimeoutError()
        self.tags.recommend = fail
        self.sender.reset_mock()
        self.assertEqual(self.tags.latest('recE', 'maintenance', ['E'])['status'], 'pending')
        await self.tags.tick()
        self.assertEqual(self.tags.latest('recE', 'maintenance', ['E'])['status'], 'failed')
        self.assertEqual(self.store.list_outbox_events(CHANNEL), [])
        now[0] += 61
        self.assertEqual(self.tags.latest('recE', 'maintenance', ['E'])['status'], 'pending')
        self.enqueue('new', action='update')
        async def success(owner, notices): return {row['id']: tags(row['title']) for row in notices}
        self.tags.recommend = success
        await self.tags.tick()
        self.assertEqual(self.tags.latest('recE', 'maintenance', ['E'])['tags'], clean_tags(tags('new')))
        self.assertEqual(self.sender.call_count, 2)  # New update only, never the display retry.

    async def test_failed_display_overrides_cannot_restore_legacy_long_error(self):
        self.enqueue()
        await self.tags.tick()
        job_key = self.store.list_documents(NS, key_prefix='job:')[0]
        job = {**job_key['payload'], 'status': 'failed', 'tags': [], 'finished_at': 1, 'error': 'legacy job error'}
        self.store.put_document(NS, job_key['key'], job)
        now = [1000]
        self.tags.clock = lambda: now[0]
        legacy_error = 'old rules\n依据：旧依据\n注意：旧注意'
        display = {'status': 'failed', 'error': legacy_error, 'retry_after': now[0] + 5000, 'attempts': 1}
        self.store.put_document(NS, 'display:' + job['id'], display)
        receipts = copy.deepcopy(self.store.list_documents(NS, key_prefix='batch:'))
        self.sender.reset_mock()
        self.calls.clear()
        visible = self.tags.latest('recE', 'maintenance', ['E'])
        self.assertEqual(visible['status'], 'failed')
        self.assertEqual(visible['error'], fallback_text('维保通告'))
        self.assertNotIn('依据', visible['error'])
        self.assertNotIn('注意', visible['error'])
        # The legacy display override must not regenerate now (still backing off)
        # and must neither resend nor invoke the model.
        self.assertEqual(self.store.list_outbox_events(CHANNEL), [])
        self.assertEqual(self.calls, [])
        self.sender.assert_not_called()
        # Old display/job storage and notification receipts are immutable.
        self.assertEqual(self.store.get_document(NS, job_key['key']), job)
        self.assertEqual(self.store.get_document(NS, 'display:' + job['id']), display)
        self.assertEqual(self.store.list_documents(NS, key_prefix='batch:'), receipts)
        # A pending retry must not keep reporting a previous failed attempt.
        now[0] += 5001
        self.assertEqual(self.tags.latest('recE', 'maintenance', ['E'])['status'], 'pending')
        self.assertEqual(self.tags.latest('recE', 'maintenance', ['E'])['error'], '')
        self.assertEqual(len(self.store.list_outbox_events(CHANNEL)), 1)
        # Sanitization preserves ready status and tags even when the job node is
        # still marked failed (legacy inconsistent state).
        self.store.put_document(NS, 'display:' + job['id'],
            {'status': 'ready', 'tags': tags('已就绪标签'), 'error': legacy_error, 'retry_after': now[0] + 5000})
        ready = self.tags.latest('recE', 'maintenance', ['E'])
        self.assertEqual(ready['status'], 'ready')
        self.assertEqual(ready['tags'], clean_tags(tags('已就绪标签')))
        self.assertEqual(ready['error'], '')

    async def test_restart_recovers_leased_tag_jobs_only(self):
        self.enqueue()
        self.store.lease_outbox_events(CHANNEL)
        other_id = self.store.enqueue_outbox_event('other', {'idempotency_key': 'other'})
        self.store.lease_outbox_events('other')
        self.store.release_outbox_leases(CHANNEL)
        await self.tags.tick()
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(self.store.list_outbox_events('other', status='leased')[0]['id'], other_id)

    def test_skill_installed_and_structured_result_checks_ids_and_all_labels(self):
        self.assertIn('alert-tagging', {row['name'] for row in lighthouse_skills.catalog()})
        self.assertIn('打标', lighthouse_skills.read('alert-tagging')['content'])
        self.assertIn('通讯故障', rules())
        self.assertIn('易错点', lighthouse_skills.read('alert-tagging', reference='alert-tagging/references/rules.md')['content'])
        with self.assertRaises(Exception):
            lighthouse_skills._parse_builtin([{'name': 'alert-tagging', 'description': 'test', 'references': ['../private.md']}])
        model = Mock()
        model.complete.return_value = json.dumps({'items': [{'id': 'notice_1', 'tags': tags('内阻刷新')}]}, ensure_ascii=False)
        result = recommend(model, [{'id': 'one', 'text': '整条通告', 'action': 'start'}])['one']
        self.assertEqual(result[0]['label'], '维护')
        self.assertEqual(result[0]['content'], '内阻刷新')
        self.assertEqual(sorted(result[0]), ['content', 'label'])
        messages = model.complete.call_args.args[0]
        self.assertIn('待现场核对', messages[0]['content'])
        self.assertIn('整条通告', messages[1]['content'])
        self.assertEqual(json.loads(messages[1]['content'])[0]['id'], 'notice_1')
        prompt = messages[0]['content']
        self.assertNotIn('basis', prompt)
        self.assertNotIn('notes', prompt)
        self.assertIn('"content"', prompt)
        self.assertNotIn('"basis"', prompt)
        self.assertNotIn('"notes"', prompt)
        model.complete.return_value = json.dumps({'items': [{'id': 'unknown', 'tags': tags('测试')}]})
        with self.assertRaises(ValueError):
            recommend(model, [{'id': 'other', 'text': 'another'}])

    async def test_exact_polling_message_uses_concise_shared_heading(self):
        async def polling_model(owner, notices):
            return {row['id']: [{'label': '设备轮巡', 'content': '制冷单元及二次泵轮巡'}] for row in notices}
        self.tags.recommend = polling_model
        self.tags.enqueue(
            {'notice_type': '设备轮巡', 'title': 'EA118机房A楼制冷单元及二次泵轮巡通告',
             'text': '轮巡通告原文', 'building_codes': ['E'], 'action': 'start'},
            operation_id='polling', target_record_id='recPoll', request={'_auth_open_id': 'ou_owner'})
        await self.tags.tick()
        body = self.sender.call_args.args[0]
        expected = (
            '通告推荐标签（仅供现场核对，不代表已给告警打标）\n\n'
            'EA118机房A楼制冷单元及二次泵轮巡通告 · 开始\n'
            '【设备轮巡】制冷单元及二次泵轮巡'
        )
        self.assertEqual(body, expected)

    def test_model_minimal_json_works_without_explanations(self):
        model = Mock()
        model.complete.return_value = json.dumps(
            {'items': [{'id': 'notice_1', 'tags': minimal_tags('内阻刷新')}]}, ensure_ascii=False)
        result = recommend(model, [{'id': 'one', 'text': '整条通告', 'action': 'start'}])['one']
        self.assertEqual(result, clean_tags(minimal_tags('内阻刷新')))
        # Legacy extra explanation keys are ignored for this generation and never propagated.
        model.complete.return_value = json.dumps(
            {'items': [{'id': 'notice_1', 'tags': tags('内阻刷新')}]}, ensure_ascii=False)
        result = recommend(model, [{'id': 'one', 'text': '整条通告', 'action': 'start'}])['one']
        self.assertEqual(result, clean_tags(minimal_tags('内阻刷新')))
        self.assertNotIn('basis', result[0])
        self.assertNotIn('notes', result[0])

    def test_legacy_stored_rich_tags_strip_in_send_and_api(self):
        self.enqueue('legacy', scopes=['E'])
        jobs = [row['payload'] for row in self.store.list_documents(NS, key_prefix='job:')]
        for job in jobs:
            job.update(status='ready', tags=tags('旧标签'))
        batch = self.tags.batch(jobs)
        self.tags.notify(batch, jobs)
        for call in self.sender.call_args_list:
            body = call.args[0]
            self.assertIn('【维护】旧标签', body)
            self.assertNotIn('依据', body)
            self.assertNotIn('注意', body)
        visible = self.tags.latest('recE', 'maintenance', ['E'])
        self.assertEqual(visible['tags'], clean_tags(tags('旧标签')))
        self.assertNotIn('basis', visible['tags'][0])
        self.assertNotIn('notes', visible['tags'][0])

    def test_handoff_failure_never_changes_notice_or_cabinet_followup(self):
        from lan_bitable_template_portal.server import PortalRuntime
        with patch.object(PortalRuntime, 'notice_alert_tags', SimpleNamespace(enqueue=Mock(side_effect=RuntimeError('disk busy'))), create=True), \
             patch('lan_bitable_template_portal.server.external_real_write_guard', return_value={'mock_external': False}), \
             patch.object(PortalRuntime, '_enqueue_qt_power_notice') as cabinet:
            PortalRuntime._enqueue_qt_notice_followups({'notice_type': '维保通告'}, {}, 'operation', 'rec', 'start')
            cabinet.assert_called_once()
        self.sender.assert_not_called()

    def test_queue_and_visible_document_commit_atomically(self):
        original = self.store._json
        def serialize(value):
            if value.get('fail_fixture'):
                raise ValueError('fixture serialization failure')
            return original(value)
        with patch.object(self.store, '_json', side_effect=serialize):
            with self.assertRaises(ValueError):
                self.store.enqueue_outbox_event(CHANNEL, {'idempotency_key': 'atomic'}, documents={
                    (NS, 'first'): {'ok': True}, (NS, 'second'): {'fail_fixture': True}})
        self.assertEqual(self.store.list_outbox_events(CHANNEL), [])
        self.assertIsNone(self.store.get_document(NS, 'first'))

    async def test_native_status_is_scoped_and_does_not_call_model(self):
        self.enqueue()
        await self.tags.tick()
        scope = ['A']
        controller = SimpleNamespace(_current_session=lambda request: {'open_id': 'ou_viewer'},
            _json_ok=lambda request, session, data: JSONResponse({'ok': True, 'data': data}))
        runtime = SimpleNamespace(service=self.service, state_store=self.store,
            auth_manager=SimpleNamespace(session_scopes=lambda session: scope))
        app = FastAPI()
        install_notice_alert_tag_routes(app, controller, runtime, self.tags.recommend)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://testserver') as client:
            url = '/api/notice-alert-tags?target_record_id=recE&work_type=maintenance'
            self.assertIsNone((await client.get(url)).json()['data'])
            scope[:] = ['E']
            result = (await client.get(url)).json()['data']
            self.assertEqual(result['status'], 'ready')
            self.assertEqual(result['scopes'], ['E'])
            self.assertEqual(result['title'], 'one')
            self.assertEqual(result['tags'], clean_tags(tags('one')))
            self.assertNotIn('basis', result['tags'][0])
            self.assertNotIn('notes', result['tags'][0])
            self.assertNotIn('recipients', result)
            controller._current_session = lambda request: None
            self.assertEqual((await client.get(url)).status_code, 401)
        self.assertEqual(len(self.calls), 1)

    def test_paired_notice_is_queued_only_when_its_upload_succeeded(self):
        from lan_bitable_template_portal.server import PortalRuntime
        queued = Mock()
        notice = {'paired_upload_status': 'success', 'paired_maintenance_target_record_id': 'recPair',
                  'paired_maintenance_upload': {'notice_type': '维保通告', 'action': 'start'}}
        with patch.object(PortalRuntime, 'notice_alert_tags', SimpleNamespace(enqueue=queued), create=True), \
             patch('lan_bitable_template_portal.server.external_real_write_guard', return_value={'mock_external': False}):
            PortalRuntime.enqueue_notice_alert_tags(notice, operation_id='operation', target_record_id='recMain')
            self.assertEqual(queued.call_count, 2)
            self.assertEqual(queued.call_args.kwargs['target_record_id'], 'recPair')
            notice['paired_upload_status'] = 'failed'
            queued.reset_mock()
            PortalRuntime.enqueue_notice_alert_tags(notice, operation_id='operation', target_record_id='recMain')
            queued.assert_called_once()

    async def test_background_model_rpc_is_authenticated_and_does_not_use_chat_or_gateway(self):
        from .test_openclaw_backend_proxy import World, ALICE
        world = World(self.store.db_path.parent / 'proxy')
        async with world.activate():
            try:
                await world.warm(world.portal_client())
                before = world.host.store.list_documents('lighthouse_messages')
                body = {'instance': world.host.instance, 'lease': world.host.lease['id'], 'owner': ALICE,
                        'notices': [{'id': 'one', 'text': '测试维保通告', 'action': 'start'}]}
                answer = json.dumps({'items': [{'id': 'notice_1', 'tags': tags('测试维护')}]}, ensure_ascii=False)
                with patch('openclaw_service.assistant.lighthouse_ai.CustomModel.complete', autospec=True, return_value=answer) as complete:
                    denied = await world.service_client(with_key=False).post('/recommend-notice-tags', json=body)
                    self.assertEqual(denied.status_code, 403)
                    complete.assert_not_called()
                    response = await world.service_client().post('/recommend-notice-tags', json=body)
                    self.assertEqual(response.status_code, 200, response.text)
                    self.assertEqual(complete.call_args.args[0]._config_key, 'model:' + ALICE)
                    self.assertEqual(response.json()['data']['one'][0]['label'], '维护')
                self.assertEqual(world.manager.spawns, 0)
                self.assertEqual(world.host.store.list_documents('lighthouse_messages'), before)
                self.assertEqual(world.host.runs, {})
            finally:
                await world.host.close()

    def test_rendered_notice_script_is_valid_and_tag_text_is_not_html(self):
        from lan_bitable_template_portal.workbench_lite import render_workbench_lite
        html = render_workbench_lite(payload={}, session={'user': {'name': 'Fixture'}}, scope='E', work_type='maintenance', manual=True)
        script = '\n'.join(re.findall(r'<script>(.*?)</script>', html, re.S))
        checked = subprocess.run(['node', '--check'], input=script, encoding='utf-8', capture_output=True, timeout=30)
        self.assertEqual(checked.returncode, 0, checked.stderr)
        self.assertIn('id="lite-alert-tags"', html)
        start = script.index('async function refreshNoticeTags()')
        end = script.index('function setSubmitButtons', start)
        tags_script = script[start:end]
        self.assertIn('node.textContent = text', tags_script)
        self.assertIn('sequence === noticeTagsSequence && panel.isConnected', tags_script)
        self.assertIn("target === String(previewValue(form, 'target_record_id')", tags_script)
        self.assertIn("workType === String(previewValue(form, 'work_type')", tags_script)
        self.assertEqual(tags_script.count('if (!stillCurrent()) return;'), 2)
        self.assertIn('通告推荐标签（仅供现场核对，不代表已给告警打标）', tags_script)
        self.assertNotIn('已发通告推荐标签', tags_script)
        self.assertNotIn('tag.basis', tags_script)
        self.assertNotIn('tag.notes', tags_script)
        self.assertNotIn('innerHTML', tags_script)


if __name__ == '__main__':
    unittest.main()
