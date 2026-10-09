"""Offline regression checks for plan-review identity, concurrency and connection reuse."""
import asyncio
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import Mock, patch
from types import SimpleNamespace

import httpx
from fastapi import FastAPI
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_plan_convergence import FakeController, FakeRuntime, MemoryStore
from test_plan_convergence_points_adapter import _build_databases
from lan_bitable_template_portal import plan_convergence_auth as auth, plan_convergence_rules as rules
from lan_bitable_template_portal.plan_convergence import PlanConvergenceService
from lan_bitable_template_portal.plan_convergence_routes import install_plan_convergence_routes


class PlanStabilityTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.catalog, _ = _build_databases(self.root)
        for module, key, value in [(rules, 'RULE_DB', self.root / 'rules.sqlite3'), (rules, 'CATALOG_DB', self.catalog), (auth, '_store', MemoryStore())]:
            guard = patch.object(module, key, value)
            guard.start()
            self.addCleanup(guard.stop)
        self.ident = rules.create_set('测试规则')
        self.details = [{'instances': '冷机一号', 'instanceIds': 'INS-1'}]

    def configure(self, items):
        return rules.save_set(self.ident, '测试规则', '', items)

    def test_unknown_ambiguous_and_empty_groups_never_pass(self):
        for name in ['不存在的设备', '冷机', '%']:
            with self.subTest(name=name):
                self.configure([{'scope_type': 'device', 'inst_name': name}])
                with self.assertRaises(ValueError):
                    rules.match_record_to_set(self.ident, self.details)
        self.configure([{'scope_type': 'device', 'inst_name': '冷机一号'}, {'scope_type': 'exclude_device', 'inst_name': '冷机一号'}])
        result = rules.match_record_to_set(self.ident, self.details)
        self.assertFalse(result['passed'])
        self.assertFalse(result['rules']['groups'][0]['matched'])
        self.configure([{'scope_type': 'room'}])
        with self.assertRaisesRegex(ValueError, '缺少必要条件'):
            rules.match_record_to_set(self.ident, self.details)

    def test_stable_device_id_and_unknown_remote_id(self):
        with closing(sqlite3.connect(self.catalog)) as conn, conn:
            conn.execute("UPDATE zh_device SET inst_name='冷机一号' WHERE ins_id='INS-2'")
        self.configure([{'scope_type': 'device', 'inst_name': '冷机一号', 'ins_id': 'INS-1'}])
        self.assertTrue(rules.match_record_to_set(self.ident, self.details)['passed'])
        self.assertTrue(rules.match_record_to_set(self.ident, [{'instanceIds': 'INSTANCE-6-INS-1'}])['passed'])
        for detail in [{'instanceIds': 'INSTANCE-6-unknown'}, {'instances': 'all', 'instanceIds': 'all', 'spaceModelName': '不存在的空间'}]:
            result = rules.match_record_to_set(self.ident, self.details + [detail])
            self.assertFalse(result['passed'])
            self.assertTrue(result['not_found_devices'])

    def test_version_conflict_and_identical_replay(self):
        original = rules.get_set(self.ident)
        items = [{'scope_type': 'device', 'inst_name': '冷机一号', 'ins_id': 'INS-1'}]
        saved = rules.save_set(self.ident, '测试规则', '', items, expected_version=original['version'])
        self.assertNotEqual(original['version'], saved['version'])
        replay = rules.save_set(self.ident, '测试规则', '', items, expected_version=original['version'])
        self.assertEqual(replay, saved)
        with self.assertRaises(rules.RuleConflictError):
            rules.save_set(self.ident, '旧编辑', '', [], expected_version=original['version'])
        self.assertEqual(rules.get_set(self.ident), saved)
        unordered = [dict(items[0], rule_group_no=2), dict(items[0], rule_group_no=1)]
        saved = rules.save_set(self.ident, '测试规则', '', unordered, expected_version=saved['version'])
        self.assertEqual(rules.save_set(self.ident, '测试规则', '', unordered, expected_version=original['version']), saved)

    def test_one_database_connection_for_repeated_rules(self):
        self.configure([{'scope_type': 'device', 'inst_name': '冷机一号', 'rule_group_no': i + 1} for i in range(100)])
        with patch.object(rules, '_conn', wraps=rules._conn) as connect:
            self.assertTrue(rules.match_record_to_set(self.ident, self.details)['passed'])
        self.assertEqual(connect.call_count, 1)

    def test_route_requires_version_and_rejects_stale_save(self):
        app = FastAPI()
        install_plan_convergence_routes(app, FakeController(), FakeRuntime())
        with TestClient(app) as client:
            url = '/api/plan-convergence/rulesets/' + str(self.ident)
            body = {'name': '测试规则', 'remark': '', 'items': []}
            self.assertEqual(client.put(url, json=body).status_code, 409)
            body['expected_version'] = rules.get_set(self.ident)['version']
            self.configure([{'scope_type': 'device', 'inst_name': '冷机一号'}])
            self.assertEqual(client.put(url, json=body).status_code, 409)
            self.assertEqual(len(rules.get_set(self.ident)['items']), 1)

    def test_identical_queries_share_work_and_do_not_block_local_data(self):
        app = FastAPI()
        install_plan_convergence_routes(app, FakeController(), FakeRuntime())
        entered, release = threading.Event(), threading.Event()
        def slow(*args, **kwargs):
            entered.set()
            release.wait(3)
            return {'records': []}
        async def exercise():
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://testserver') as client:
                tasks = [asyncio.create_task(client.post('/api/plan-convergence/maintenance/check', json={})) for _ in range(6)]
                try:
                    self.assertTrue(await asyncio.to_thread(entered.wait, 2))
                    response = await asyncio.wait_for(client.get('/api/plan-convergence/rulesets'), 1)
                    self.assertEqual(response.status_code, 200)
                    await asyncio.sleep(.05)
                    self.assertEqual(work.call_count, 1)
                finally:
                    release.set()
                self.assertTrue(all(row.status_code == 200 for row in await asyncio.gather(*tasks)))
            await app.router.shutdown()
        with patch.object(PlanConvergenceService, 'maintenance_check', side_effect=slow) as work:
            asyncio.run(exercise())

    def test_failed_detail_stops_scheduling_and_deadline_is_enforced(self):
        service = PlanConvergenceService(MemoryStore(), lambda _: [])
        self.addCleanup(service.close)
        blocks = {'items': [{'blockId': str(i), 'status': '1'} for i in range(100)]}
        with patch.object(service, 'blocks', return_value=blocks), patch.object(service, 'block', side_effect=ValueError('broken detail')) as detail:
            with self.assertRaisesRegex(ValueError, 'broken detail'):
                service.maintenance_check()
            self.assertLessEqual(detail.call_count, 4)
        with patch.object(auth, 'request') as network, patch.object(auth, 'build_auth', return_value=({}, {})):
            with self.assertRaises(TimeoutError):
                service.block('1', deadline=time.monotonic() - 1)
            network.assert_not_called()

    def test_http_client_is_reused_without_retaining_response_cookies(self):
        seen = []
        def reply(request):
            seen.append(request.headers.get('cookie', ''))
            return httpx.Response(200, json={}, headers={'set-cookie': 'stale=secret; Path=/'})
        client = httpx.Client(transport=httpx.MockTransport(reply))
        self.addCleanup(client.close)
        with patch.object(auth, '_client', None), patch('httpx.Client', return_value=client) as create, patch('upload_event_module.services.http_client.verified_tls_context', return_value=True) as tls:
            auth.request('GET', 'https://fixture.invalid', cookies={'token': 'first'})
            auth.request('GET', 'https://fixture.invalid', cookies={'token': 'second'})
            self.assertEqual(create.call_count, 1)
            self.assertEqual(tls.call_count, 1)
            self.assertFalse(create.call_args.kwargs['trust_env'])
        self.assertIn('token=second', seen[1])
        self.assertNotIn('first', seen[1])
        self.assertNotIn('stale', seen[1])


class NoticeCheckTests(unittest.TestCase):
    def test_post_send_check_is_once_and_delivery_retries_without_rechecking(self):
        from lan_bitable_template_portal.plan_convergence_notifications import NoticePlanChecks, CHANNEL
        from lan_bitable_template_portal.state_store import LanPortalStateStore
        with tempfile.TemporaryDirectory() as folder:
            store = LanPortalStateStore(Path(folder) / 'state.sqlite3')
            service = SimpleNamespace(_recipients_for_building_codes=lambda scopes, **_: ('B', ['B-duty', 'Li'], ''))
            check = Mock(return_value={'records': [{'hits': []}]})
            send = Mock(side_effect=[(True, '', []), (False, '', []), (True, '', [])])
            worker = NoticePlanChecks(service, store, check, send)
            notice = {'notice_type': '设备检修', 'action': 'start', 'name': 'B楼设备检修', 'building_codes': ['B']}
            for _ in range(2): worker.enqueue(notice, operation_id='one', target_record_id='recB')
            row = store.lease_outbox_events(CHANNEL, limit=1, lease_seconds=300)[0]
            worker.process(row)
            worker.process(row)
            self.assertEqual(check.call_count, 1)
            self.assertEqual(send.call_count, 3)
            self.assertEqual(send.call_args_list[1].kwargs, send.call_args_list[2].kwargs)
            self.assertIn('B楼设备检修', send.call_args_list[0].args[0])
            self.assertNotIn('核对通过', send.call_args_list[0].args[0])
            check.side_effect = TimeoutError('VPN offline')
            worker.enqueue({**notice, 'action': 'update'}, operation_id='two', target_record_id='recB')
            send.side_effect = None; send.return_value = (True, '', [])
            row = store.lease_outbox_events(CHANNEL, limit=1, lease_seconds=300)[0]
            worker.process(row)
            self.assertIn('通告已正常发送', send.call_args.args[0])
            self.assertEqual(check.call_count, 2)
            worker.enqueue({**notice, 'action': 'end'}, operation_id='end', target_record_id='recB')
            worker.enqueue({**notice, 'notice_type': '维保通告'}, operation_id='maintenance', target_record_id='recM')
            self.assertEqual(store.lease_outbox_events(CHANNEL, limit=1, lease_seconds=300), [])

    def test_common_site_identifier_is_not_a_device_match(self):
        from lan_bitable_template_portal.plan_convergence_maintenance import _device_keywords, _longest_common_substring
        self.assertEqual(_device_keywords('EA118 C01 BMS I3 E-201-UPS-01'), ['E-201-UPS-01'])
        self.assertEqual(_longest_common_substring('EA118_C01机房E楼水泵检修', 'EA118_C01机房E楼烟感故障'), '')

    def test_de_steps_are_not_auto_enqueued_for_post_send_checks(self):
        from lan_bitable_template_portal.plan_convergence_notifications import NoticePlanChecks, CHANNEL
        from lan_bitable_template_portal.state_store import LanPortalStateStore
        with tempfile.TemporaryDirectory() as folder:
            store = LanPortalStateStore(Path(folder) / 'state.sqlite3')
            service = SimpleNamespace(_recipients_for_building_codes=lambda scopes, **_: (scopes[0], [], ''))
            worker = NoticePlanChecks(service, store, Mock(), Mock())
            # Both D and E building notices are excluded from the plan-convergence
            # auto post-send flow and must never be queued.
            for code in ('D', 'E'):
                notice = {'notice_type': '设备检修', 'action': 'start', 'name': f'{code}楼设备检修',
                          'building_codes': [code]}
                worker.enqueue(notice, operation_id='one-' + code, target_record_id='rec' + code)
            self.assertEqual(store.lease_outbox_events(CHANNEL, limit=10, lease_seconds=300), [])


if __name__ == '__main__':
    unittest.main()
