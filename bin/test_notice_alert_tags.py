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
from openclaw_service.assistant.lighthouse_alert_tagging import fallback_text, recommend, rules
from openclaw_service.assistant import lighthouse_skills


def tags(name):
    return [{'label': '维护', 'content': name, 'basis': '已发维护通告，包含维护操作。', 'notes': '窗口及实际告警触发关系待现场核对。'}]


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

    async def test_model_failure_sends_short_type_specific_fallback_to_floor_and_li(self):
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
            self.assertEqual(len(body.split('\n\n')[1].splitlines()), 4)
        visible = self.tags.latest('recE', 'maintenance', ['E'])
        self.assertEqual(visible['status'], 'failed')
        self.assertEqual(visible['error'], fallback_text('维保通告'))

    def test_fallback_is_three_lines_for_each_notice_type_without_reading_skill(self):
        expected = {'维保通告': '【维护】', '变更通告': '【变更】', '设备检修': '【检修】',
                    '设备轮巡': '【设备轮巡】', '设备调整': '【设备调整】',
                    '上电通告': '【上下电】', '下电通告': '【上下电】',
                    '事件通告': '按实际根因', '未识别类型': '通告类型未识别'}
        with patch('openclaw_service.assistant.lighthouse_alert_tagging.rules', side_effect=AssertionError('no full skill in fallback')):
            for kind, text in expected.items():
                with self.subTest(kind=kind):
                    body = fallback_text(kind)
                    self.assertIn(text, body)
                    self.assertEqual(len(body.splitlines()), 3)
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
        self.assertEqual(recommend(model, [{'id': 'one', 'text': '整条通告', 'action': 'start'}])['one'][0]['label'], '维护')
        messages = model.complete.call_args.args[0]
        self.assertIn('待现场核对', messages[0]['content'])
        self.assertIn('整条通告', messages[1]['content'])
        self.assertEqual(json.loads(messages[1]['content'])[0]['id'], 'notice_1')
        model.complete.return_value = json.dumps({'items': [{'id': 'unknown', 'tags': tags('测试')}]})
        with self.assertRaises(ValueError):
            recommend(model, [{'id': 'other', 'text': 'another'}])

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
        self.assertIn('sequence !== noticeTagsSequence', tags_script)
        self.assertNotIn('innerHTML', tags_script)


if __name__ == '__main__':
    unittest.main()
