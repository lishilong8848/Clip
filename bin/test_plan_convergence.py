import copy
import base64
import io
import json
import sqlite3
import sys
import threading
import time
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import Mock, patch
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from fastapi import FastAPI
from fastapi.responses import JSONResponse, Response
from fastapi.testclient import TestClient
from openpyxl import Workbook

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lan_bitable_template_portal import plan_convergence_auth as auth
from lan_bitable_template_portal import plan_convergence_maintenance as maintenance
from lan_bitable_template_portal import plan_convergence_routes as routes
from lan_bitable_template_portal import plan_convergence_rules as rule_sets
from lan_bitable_template_portal.plan_convergence import PlanConvergenceService
from lan_bitable_template_portal import plan_convergence_browser_login as browser_login
from tools.export_plan_convergence_catalog import export_catalog, import_rule_sets


class MemoryStore:
    def __init__(self):
        self.documents = {}

    def get_document(self, namespace, key):
        return copy.deepcopy(self.documents.get((namespace, key)))

    def put_document(self, namespace, key, payload):
        self.documents[(namespace, key)] = copy.deepcopy(payload)


def fixture_token(owner='test-user', expiry=None):
    meta = {'UserName': owner, 'jti': 'test-session', 'exp': expiry if expiry is not None else int(time.time()) + 3600}
    encoded = base64.urlsafe_b64encode(json.dumps(meta).encode()).decode().rstrip('=')
    return 'eyJ0eXAiOiJKV1QifQ.' + encoded + '.test-only'


class FakeController:
    def __init__(self):
        self.ongoing = []

    def _get_ongoing(self, scope):
        assert scope == 'ALL'
        return copy.deepcopy(self.ongoing)

    def _current_session(self, request):
        if request.headers.get('x-test-anonymous'):
            return None
        return {'is_guest': bool(request.headers.get('x-test-guest')),
                'user': {'role': request.headers.get('x-test-role', 'admin')}}

    def _auth_required_response(self):
        return JSONResponse({'ok': False, 'auth_required': True}, status_code=401)

    def _json_response(self, request, session, payload):
        return JSONResponse(payload)

    def _request_base_url(self, request):
        return 'http://testserver'

    def _static_file_response(self, request, path, html=False):
        return Response('portal', media_type='text/html')


class FakeRuntime:
    auth_manager = type('Auth', (), {'is_admin': lambda self, session: session['user']['role'] == 'admin'})()

    def __init__(self):
        self.state_store = MemoryStore()
        self.service = type('Service', (), {'_http_client': Mock()})()


class SourceCursor:
    def __init__(self, rows):
        self.rows = rows

    def execute(self, sql):
        self.table = sql.split('FROM ')[-1]

    def fetchmany(self, count):
        rows = self.rows.get(self.table, [])
        self.rows[self.table] = rows[count:]
        return rows[:count]

    def fetchall(self):
        return self.rows.get(self.table, [])

    def close(self):
        pass


class Source:
    def __init__(self, rows):
        self.rows = rows

    def cursor(self):
        return SourceCursor(self.rows)


class PlanConvergenceTests(unittest.TestCase):
    def setUp(self):
        self.previous_store = auth._store
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.legacy_path = patch.object(auth, 'CONFIG_FILE', Path(self.temp.name) / 'auth.json')
        self.legacy_path.start()
        self.addCleanup(self.legacy_path.stop)
        self.runtime = FakeRuntime()
        auth.bind_store(self.runtime.state_store)
        app = FastAPI()
        self.controller = FakeController()
        routes.install_plan_convergence_routes(app, self.controller, self.runtime)
        self.client = TestClient(app)
        self.addCleanup(self.client.close)

    def tearDown(self):
        auth.bind_store(self.previous_store)

    def test_catalog_export(self):
        path = Path(self.temp.name) / 'catalog.sqlite3'
        export_catalog(Source({'zh_device': [(1, '设备A', '', 'INS-1', '冷机', '', '', '', '', 'A楼', '')],
                               'zh_rules': [(1, 'ALM-1', '高温', '', '冷机', '', '', '')]}), path)
        with closing(sqlite3.connect(path)) as conn:
            self.assertEqual(conn.execute('SELECT ins_id FROM zh_device').fetchone()[0], 'INS-1')
            self.assertEqual(conn.execute('SELECT alarm_config_id FROM zh_rules').fetchone()[0], 'ALM-1')
        self.assertFalse(Path(str(path) + '.pending').exists())

    def test_rule_import_refuses_overwrite(self):
        path = Path(self.temp.name) / 'rule_sets.sqlite3'
        import_rule_sets(Source({'rule_set': [(5, '已有规则', '', '2026-01-01', '2026-01-01')],
                                 'rule_set_item': [(9, 5, 'device', '', '', '', '', '', '设备A', '', '', '', 1, 'normal', '', '2026-01-01')]}), path)
        with closing(sqlite3.connect(path)) as conn:
            self.assertEqual(conn.execute('SELECT count(*) FROM rule_set_item').fetchone()[0], 1)
        with self.assertRaisesRegex(ValueError, '拒绝覆盖'):
            import_rule_sets(Source({}), path)

    def test_rule_save_is_atomic(self):
        with patch.object(rule_sets, 'RULE_DB', Path(self.temp.name) / 'rule_sets.sqlite3'):
            ident = rule_sets.create_set('原名称')
            rule_sets.save_set(ident, '新名称', '备注', [{'scope_type': 'device', 'inst_name': '设备A', 'rule_group_no': 1}])
            with self.assertRaisesRegex(ValueError, '规则范围无效'):
                rule_sets.save_set(ident, '错误名称', '', [{'scope_type': 'invalid'}])
            self.assertEqual(rule_sets.get_set(ident)['name'], '新名称')
            self.assertEqual(len(rule_sets.get_set(ident)['items']), 1)

    def test_native_auth_and_write_boundaries(self):
        prefix = '/api/plan-convergence'
        self.assertEqual(self.client.get(prefix + '/rulesets', headers={'x-test-anonymous': '1'}).status_code, 401)
        self.assertEqual(self.client.get(prefix + '/rulesets', headers={'x-test-guest': '1'}).status_code, 403)
        self.assertEqual(self.client.get(prefix + '/settings', headers={'x-test-role': 'viewer'}).status_code, 403)
        self.assertEqual(self.client.post(prefix + '/rulesets', json={'name': '不应创建'}, headers={'origin': 'https://evil.example'}).status_code, 403)
        self.assertEqual(self.client.post(prefix + '/rulesets', json={'name': '不应创建'}, headers={'x-test-role': 'viewer'}).status_code, 403)
        with patch.object(rule_sets, 'get_set', return_value={'id': 1}), patch.object(PlanConvergenceService, 'match', return_value={'passed': True}):
            result = self.client.post(prefix + '/rulesets/1/match', json={'details': []}, headers={'x-test-role': 'viewer'})
        self.assertEqual(result.status_code, 200)
        self.assertTrue(result.json()['data']['passed'])
        self.assertEqual(self.client.get('/plan-convergence/legacy/ruleset', follow_redirects=False).headers['location'], '/plan-convergence?tab=rules')

    def test_large_rule_set_expands_in_batches(self):
        ids = [str(index) for index in range(901)]
        with patch.object(rule_sets, 'get_set', return_value={'id': 1}), patch.object(rule_sets, 'expand_set_items', return_value=ids), patch.object(
            rule_sets, '_exec', side_effect=lambda sql, args: [{'ins_id': value, 'inst_name': value} for value in args]
        ) as execute:
            result = self.client.get('/api/plan-convergence/rulesets/1/expand').json()['data']
        self.assertEqual(result['count'], 901)
        self.assertEqual(len(result['devices']), 901)
        self.assertEqual(execute.call_count, 2)
        with patch.object(rule_sets, '_exec', side_effect=lambda sql, args: [
            {'ins_id': value, 'inst_name': value, 'obj_name': '设备', 'position': ''} for value in args
        ]) as execute:
            self.assertEqual(len(rule_sets._dev_info(ids)), 901)
        self.assertEqual(execute.call_count, 2)

    def test_native_excel_comparison(self):
        workbook = Workbook()
        sheet = workbook.active
        sheet.title = '场景一'
        sheet.append(['设备域', '关联资源', '关联设备', '关联告警规则'])
        sheet.append(['冷却塔', '机房1', '设备A,设备B', '规则1'])
        content = io.BytesIO()
        workbook.save(content)
        upload = self.client.post('/api/plan-convergence/excel', content=content.getvalue(), headers={'x-filename': 'rules.xlsx'})
        self.assertEqual(upload.status_code, 200)
        rows = upload.json()['data']['sheets'][0]['rows']
        detail = {'blockDetailId': 'd1', 'classifyModel': '冷却塔', 'spaceModel': '机房1', 'relateConfig': '规则1', 'alarmConfigId': '1', 'instances': '设备A'}
        with patch.object(PlanConvergenceService, 'block', return_value={'blockId': '123', 'alarmBlockDetailResultList': [detail]}):
            compared = self.client.post('/api/plan-convergence/compare', json={'block_id': '123', 'scenarios': [{'scenario_name': '场景一', 'rows': rows}]})
        self.assertEqual(compared.status_code, 200)
        self.assertEqual(compared.json()['data']['missing_data_list'][0]['missing_devices'], ['设备B'])

    def test_pagination_saves_only_complete_snapshot(self):
        first = {'content': [{'blockId': '1'}], 'hasNext': True, 'searchAfter': ['1']}
        second = {'content': [{'blockId': '1'}, {'blockId': '2'}], 'hasNext': False}
        service = PlanConvergenceService(self.runtime.state_store)
        with patch.object(auth, 'build_auth', return_value=({}, {})), patch.object(service, 'remote', side_effect=[first, second]):
            self.assertEqual([row['blockId'] for row in service.blocks(True)['items']], ['1', '2'])
        previous = service.blocks()
        with patch.object(auth, 'build_auth', return_value=({}, {})), patch.object(service, 'remote', side_effect=[first, first]):
            with self.assertRaisesRegex(ValueError, '重复返回'):
                service.blocks(True)
        self.assertEqual(service.blocks(), previous)

    def test_credentials_migrate_once_without_exposing_secrets(self):
        auth.CONFIG_FILE.write_text(json.dumps({'token': 'test-secret', 'password': 'test-password', 'login_name': 'test'}), encoding='utf-8')
        self.assertEqual(auth.load_config()['token'], 'test-secret')
        auth.CONFIG_FILE.write_text('{}', encoding='utf-8')
        self.assertEqual(auth.load_config()['token'], 'test-secret')
        view = auth.settings_view()
        self.assertNotIn('token', view)
        self.assertNotIn('password', view)
        auth.save_config({'display_name': '用户', 'token': ''})
        self.assertEqual(auth.load_config()['token'], 'test-secret')
        with self.assertRaisesRegex(ValueError, '格式无效'):
            auth.save_config({'token': 'bad\r\nInjected: header'})
        self.assertNotIn('password', auth.load_config())
        auth.save_config({'clear_token': True})
        self.assertFalse(auth.load_config()['token'])

    def test_browser_login_routes_are_admin_only_and_hide_credentials(self):
        for method, path in [('GET', 'settings/browser-login'), ('POST', 'settings/browser-login'),
                             ('POST', 'settings/browser-login/cancel')]:
            self.assertEqual(self.client.request(method, '/api/plan-convergence/' + path,
                                                headers={'x-test-role': 'user'}).status_code, 403)
        self.assertEqual(self.client.get('/api/plan-convergence/settings/browser-login').json()['data']['status'], 'idle')
        with patch.object(browser_login.BrowserLogin, 'start', return_value={
            'job_id': 'fixture-job', 'status': 'waiting', 'message': '等待主机智航登录',
        }):
            self.assertEqual(self.client.post('/api/plan-convergence/settings/browser-login').json()['data']['job_id'], 'fixture-job')
        self.assertEqual(self.client.post('/api/plan-convergence/settings/browser-login',
                                         headers={'origin': 'https://elsewhere.invalid'}).status_code, 403)
        self.assertEqual(self.client.post('/api/plan-convergence/settings/browser-login/cancel', json={'job_id': 'old-job'}).status_code, 400)

    def test_browser_token_is_only_accepted_from_zhihang_api_requests(self):
        token = fixture_token()
        message = {'method': 'Network.requestWillBeSent', 'params': {'request': {
            'url': auth.ZH_BASE + '/api/alarm/alarmBlock/getAlarmBlock',
            'headers': {'Authorization': 'Bearer ' + token},
        }}}
        self.assertEqual(browser_login.request_token(message), token)
        message['params']['request']['url'] = 'https://elsewhere.invalid/api/login'
        self.assertIsNone(browser_login.request_token(message))
        message['params']['request']['url'] = auth.ZH_BASE + '/some-other-resource'
        self.assertIsNone(browser_login.request_token(message))
        extra = {'method': 'Network.requestWillBeSentExtraInfo', 'params': {
            '_request_url': auth.ZH_BASE + '/api/login',
            'headers': {'Authorization': 'Bearer fixture-token', 'Origin': 'https://elsewhere.invalid'},
        }}
        self.assertIsNone(browser_login.request_token(extra))
        extra['params']['headers']['Origin'] = auth.ZH_BASE
        self.assertEqual(browser_login.request_token(extra), 'fixture-token')

    def test_browser_login_saves_only_verified_tokens_and_reuses_active_job(self):
        login = browser_login.BrowserLogin(self.runtime.state_store)
        gate, cleanup = threading.Event(), threading.Event()
        token = fixture_token()

        def capture(stop, deadline):
            try:
                gate.wait(2)
                yield token
            finally:
                cleanup.set()

        with patch.object(login, '_tokens', side_effect=capture) as captured, patch.object(auth, 'test_connection', return_value={'connected': True}):
            first = login.start()
            second = login.start()
            self.assertEqual(first['job_id'], second['job_id'])
            gate.set()
            login._thread.join(3)
            self.assertFalse(login._thread.is_alive())
            self.assertEqual(login.status()['status'], 'success')
            self.assertTrue(cleanup.is_set())
            captured.assert_called_once()
        self.assertEqual(auth.load_config()['token'], token)
        self.assertNotIn(token, json.dumps(login.status()))
        self.assertEqual(auth.settings_view()['token_owner'], 'test-user')
        self.assertNotIn('token', auth.settings_view())
        self.assertEqual(login.cancel(first['job_id'])['status'], 'success')

    def test_browser_login_cancel_and_failed_validation_preserve_original_token(self):
        auth.save_config({'token': 'previous-test-token'})
        login = browser_login.BrowserLogin(self.runtime.state_store)

        def capture(stop, deadline):
            yield fixture_token()

        with patch.object(login, '_tokens', side_effect=capture), patch.object(auth, 'test_connection', side_effect=ValueError('invalid candidate')):
            login.start()
            login._thread.join(3)
            self.assertEqual(login.status()['status'], 'failed')
        self.assertEqual(auth.load_config()['token'], 'previous-test-token')

        def wait_for_cancel(stop, deadline):
            stop.wait(2)
            yield fixture_token()

        with patch.object(login, '_tokens', side_effect=wait_for_cancel), patch.object(auth, 'test_connection', return_value={'connected': True}):
            job = login.start()
            with self.assertRaisesRegex(ValueError, '任务已变化'):
                login.cancel('different-job')
            self.assertEqual(login.cancel(job['job_id'])['status'], 'cancelled')
            login._thread.join(3)
            self.assertFalse(login._thread.is_alive())
        self.assertEqual(auth.load_config()['token'], 'previous-test-token')

    def test_expired_token_never_attempts_password_login(self):
        auth.save_config({'token': fixture_token(expiry=int(time.time()) - 1)})
        self.assertTrue(auth.settings_view()['expired'])
        with patch('requests.post', side_effect=AssertionError('must not probe guessed password endpoints')):
            with self.assertRaisesRegex(ValueError, '已过期'):
                auth.build_auth()

    def test_local_browser_capture_and_cleanup(self):
        profile = Path(self.temp.name) / 'browser-data'
        try:
            args = browser_login.browser_command(profile / 'browser_profile')
        except ValueError:
            self.skipTest('Edge/Chrome is not installed')
        token = fixture_token()

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path == '/api/test':
                    body = b'{"code":200,"success":true}'
                else:
                    body = ('<html><body>Isolated authentication fixture<script>fetch("/api/test",'
                            '{headers:{Authorization:"Bearer ' + token + '"}})</script></body></html>').encode()
                self.send_response(200)
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

            def do_POST(self):
                if self.headers.get('Authorization') == 'Bearer ' + token:
                    body = b'{"code":200,"success":true}'
                else:
                    body = b'{"code":401,"success":false}'
                self.send_response(200)
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()
        login = browser_login.BrowserLogin(self.runtime.state_store)
        origin = f'http://127.0.0.1:{server.server_port}'
        args.insert(1, '--headless=new')
        args.insert(2, '--disable-gpu')
        args.insert(3, '--disable-background-networking')
        try:
            with patch.object(auth, 'ZH_BASE', origin), patch.object(browser_login, 'browser_command', return_value=args), patch.object(
                browser_login, 'get_data_file_path', return_value=str(profile),
            ):
                job = login.start()
                try:
                    login._thread.join(25)
                    self.assertFalse(login._thread.is_alive())
                    self.assertEqual(login.status()['status'], 'success', login.status())
                    self.assertEqual(auth.load_config()['token'], token)
                    self.assertNotIn(token, json.dumps(login.status()))
                finally:
                    login.cancel(job['job_id'])
                    login.close()
            no_debug_args = [arg for arg in args if arg != '--remote-debugging-port=0']
            with patch.object(auth, 'ZH_BASE', origin), patch.object(browser_login, 'browser_command', return_value=no_debug_args), patch.object(
                browser_login, 'get_data_file_path', return_value=str(profile),
            ):
                captured = login._tokens(threading.Event(), time.monotonic() + .5)
                try:
                    with self.assertRaisesRegex(ValueError, '启动超时'):
                        next(captured)
                finally:
                    captured.close()
        finally:
            server.shutdown()
            server.server_close()
            server_thread.join(2)

    def test_maintenance_reads_local_unfinished_notices_for_all_buildings(self):
        for code in 'ABCDEH':
            self.controller.ongoing.append({
                'active_item_id': 'qt-' + code, 'notice_type': '设备检修', 'work_type': 'repair',
                'title': code + '楼空调检修', 'status': '更新', 'building_codes': [code],
                'location': code + '-346', 'repair_device': code + '-346-CRAC-02',
                'repair_fault': '压力传感器失效', 'fault_time': '2026-09-12 17:19',
                'started_at': '2026-09-12 17:27', 'start_time': '2026-10-01 23:59',
                'specialty': '暖通',
            })
        self.controller.ongoing.extend([
            {'record_id': 'recEnded', 'work_type': 'repair', 'status': '结束'},
            {'record_id': 'recEvent', 'work_type': 'event'},
            {'record_id': 'recMaintenance', 'work_type': 'maintenance'},
            {'active_item_id': 'qt-end-draft', 'work_type': 'repair', 'status': '结束',
             '_has_unuploaded_changes': True, 'title': '未发送的结束草稿'},
        ])
        with patch('requests.request', side_effect=AssertionError('must not query remote data')), patch(
            'upload_event_module.services.feishu_token_manager.token_manager.get_tenant_token',
            side_effect=AssertionError('must not request Feishu token'),
        ):
            response = self.client.get('/api/plan-convergence/maintenance/records')
        self.assertEqual(response.status_code, 200)
        records = response.json()['data']
        self.assertEqual(len(records), 7)
        self.assertEqual({row['building'] for row in records[:6]}, {code + '楼' for code in 'ABCDEH'})
        self.assertEqual(records[3]['rooms'], ['D-346'])
        self.assertEqual(records[3]['device'], 'D-346-CRAC-02')
        self.assertEqual(records[0]['start_time'], '2026-09-12 17:27')
        self.assertEqual(records[-1]['record_id'], 'qt-end-draft')
        self.assertNotIn('recEnded', {row['record_id'] for row in records})
        self.controller.ongoing = []
        self.assertEqual(self.client.get('/api/plan-convergence/maintenance/records').json()['data'], [])

    def test_local_maintenance_fields_and_single_check(self):
        self.controller.ongoing = [{
            'active_item_id': 'qt-D', 'work_type': 'repair', 'status': '开始',
            'title': '当前标题', 'display_fields': {
                '名称（标题）': '旧标题', '位置': 'D-346', '维修设备': 'D-346-CRAC-02',
                '维修故障': '压力传感器失效', '楼栋': 'D楼', '专业': '暖通',
                '发生故障时间': '2026-09-12 17:19', '实际开始时间': 1790334000000,
            },
        }]
        service = PlanConvergenceService(self.runtime.state_store, self.controller._get_ongoing)
        records = service.maintenance_records('qt-D')
        self.assertEqual(records[0]['name'], '当前标题')
        self.assertEqual(records[0]['major'], '暖通')
        self.assertEqual(records[0]['fault_time'], '2026-09-12 17:19')
        with patch.object(PlanConvergenceService, 'blocks', return_value={
            'items': [{'blockId': '1', 'status': 1, 'blockName': 'D楼 D-346空调屏蔽'}],
        }), patch.object(PlanConvergenceService, 'block', return_value={'alarmBlockDetailResultList': []}):
            result = self.client.post('/api/plan-convergence/maintenance/check', json={'record_id': 'qt-D'})
            self.assertEqual(result.status_code, 200)
            self.assertEqual(result.json()['data']['records'][0]['hits'][0]['blockId'], '1')
        self.controller.ongoing = []
        with patch.object(PlanConvergenceService, 'blocks') as remote:
            missing = self.client.post('/api/plan-convergence/maintenance/check', json={'record_id': 'qt-D'})
            self.assertEqual(missing.status_code, 404)
            remote.assert_not_called()

    def test_local_maintenance_uses_the_same_projection_as_the_portal(self):
        from clipflow_backend.main import FastAPIPortalController
        from lan_bitable_template_portal.portal_service import MaintenancePortalService
        from lan_bitable_template_portal.server import PortalRuntime
        from lan_bitable_template_portal.state_store import LanPortalStateStore

        store = LanPortalStateStore(Path(self.temp.name) / 'local-notices.sqlite3')
        for code in 'ABCDEH':
            store.upsert_qt_active_item({
                'active_item_id': 'qt-' + code, 'target_record_id': 'rec' + code,
                'work_type': 'repair', 'notice_type': '设备检修', 'status': '开始',
                'building_codes': [code],
                'text': f'【设备检修】状态：开始\n【标题】{code}楼 {code}-346设备检修\n'
                        f'【地点】{code}-346\n【维修设备】{code}-346-CRAC-02\n【维修故障】传感器失效',
            }, section='other')
        store.upsert_qt_active_item({
            'active_item_id': 'qt-ended', 'target_record_id': 'recEnded', 'work_type': 'repair',
            'notice_type': '设备检修', 'status': '结束', 'text': '【设备检修】状态：结束\n【标题】已结束检修',
        }, section='other')
        store.delete_qt_active_item(active_item_id='qt-B')
        self.controller._get_ongoing = FastAPIPortalController._get_ongoing
        with patch.object(PortalRuntime, 'state_store', store), patch.object(
            PortalRuntime, 'service', object.__new__(MaintenancePortalService),
        ), patch('requests.request', side_effect=AssertionError('unexpected remote request')):
            result = self.client.get('/api/plan-convergence/maintenance/records')
        self.assertEqual(result.status_code, 200)
        records = result.json()['data']
        self.assertEqual({row['record_id'] for row in records}, {'rec' + code for code in 'ACDEH'})
        self.assertEqual(next(row for row in records if row['record_id'] == 'recE')['device'], 'E-346-CRAC-02')

    def test_missing_remote_detail_stops_maintenance_audit(self):
        service = PlanConvergenceService(self.runtime.state_store, self.controller._get_ongoing)
        with patch.object(service, 'blocks', return_value={'items': [{'blockId': '1', 'status': 1}]}), patch.object(
            service, 'block', side_effect=ValueError('明细不完整')
        ), patch.object(service, 'maintenance_records') as read:
            with self.assertRaisesRegex(ValueError, '明细不完整'):
                service.maintenance_check()
            read.assert_called_once_with(None)


if __name__ == '__main__':
    unittest.main()
