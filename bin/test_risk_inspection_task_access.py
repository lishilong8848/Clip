import ast
import asyncio
import copy
import io
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient
from openpyxl import Workbook

from bin import test_risk_inspection as fixtures
from bin.lan_bitable_template_portal.module_access import module_denial
from bin.lan_bitable_template_portal.risk_inspection_routes import install_risk_inspection_routes, risk_task_access


class TaskAccessTests(unittest.TestCase):
    setUp = fixtures.RiskTests.setUp

    def client(self):
        self.disabled = set()
        rows = [{'record_id': p['id'], 'name': p['name'], 'open_id': p['open_id'], 'selectable': True}
                for p in self.people.values()]
        auth = SimpleNamespace(
            directory_person=lambda oid: {'id': oid[3:], 'name': oid[3:], 'selectable': True},
            _open_id_explicitly_disabled=lambda oid: oid[3:] in self.disabled,
            account_profile=lambda oid: copy.deepcopy(self.people[oid[3:]]),
            public_user=lambda session: copy.deepcopy(session['user']))
        async def read_body(request, **kwargs):
            return await request.json()
        controller = SimpleNamespace(
            _current_session=lambda req: {'user': {'open_id': 'ou_' + req.headers['x-person']}} if req.headers.get('x-person') else None,
            _request_base_url=lambda req: 'http://testserver', _read_json_request=read_body)
        runtime = SimpleNamespace(auth_manager=auth, state_store=SimpleNamespace(db_path=self.store.db_path),
            service=SimpleNamespace(get_permission_directory_people=lambda: {'items': rows},
                                    _critical_guard_public_base_url=lambda: 'http://testserver'))
        app = FastAPI()
        install_risk_inspection_routes(app, controller, runtime)
        @app.middleware('http')
        async def guard(request, next_call):
            denial = await module_denial(request, controller, runtime)
            return denial if denial is not None else await next_call(request)
        @app.get('/api/events')
        def events():
            return {'ok': True}
        self.runtime, self.controller = runtime, controller
        client = TestClient(app)
        self.addCleanup(client.close)
        return client

    def publish(self):
        self.people['alice']['modules'] = []
        body = {**self.body, 'reviewer_ids': ['alice', 'bob'],
                'people': [{'id': 'alice', 'scopes': ['A', 'B']}]}
        body.pop('reviewer_id')
        data = self.store.publish(self.people['admin'], body, self.template)
        self.store.activate(data['id'])
        with self.store.connect(True) as db:
            db.execute("UPDATE risk_jobs SET status='done' WHERE kind='publish'")
        return data['id']

    def auth_status(self, modules, granted):
        source = Path(__file__).parent / 'clipflow_backend' / 'main.py'
        handler = copy.deepcopy(next(node for node in ast.walk(ast.parse(source.read_text(encoding='utf-8-sig')))
                                    if isinstance(node, ast.AsyncFunctionDef) and node.name == 'auth_status'))
        handler.decorator_list = []
        original = {'user': {'open_id': 'ou_alice', 'modules': modules, 'scope_rules': ['C']}, 'allowed_scopes': ['C']}
        session = {'user': {'open_id': 'ou_alice'}}
        controller = SimpleNamespace(_current_session=lambda request: session,
            _request_target=lambda request: '/', _request_base_url=lambda request: 'http://testserver',
            _json_response=lambda request, session, payload: SimpleNamespace(headers={}, payload=payload))
        runtime = SimpleNamespace(auth_manager=SimpleNamespace(public_status=lambda *args, **kwargs: copy.deepcopy(original)))
        namespace = {'asyncio': asyncio, 'Request': object, 'PortalRuntime': runtime, 'self': controller}
        exec(compile(ast.Module(body=[handler], type_ignores=[]), str(source), 'exec'), namespace)
        access = Mock(return_value=granted)
        with patch.dict(sys.modules, {'lan_bitable_template_portal.risk_inspection_routes': SimpleNamespace(risk_task_access=access)}):
            response = asyncio.run(namespace['auth_status'](SimpleNamespace(query_params={})))
        access.assert_called_once_with(runtime, session)
        self.assertEqual(response.headers['Cache-Control'], 'no-store')
        self.assertEqual(response.payload['data']['allowed_scopes'], ['C'])
        self.assertEqual(response.payload['data']['user']['scope_rules'], ['C'])
        self.assertEqual(original['user'], {'open_id': 'ou_alice', 'modules': modules, 'scope_rules': ['C']})
        return response.payload['data']['user']

    def test_auth_status_exposes_only_task_grant_even_when_existing_building_permission_is_present(self):
        for modules in ([], ['risk_inspection']):
            with self.subTest(modules=modules):
                user = self.auth_status(modules, True)
                self.assertTrue(user['risk_inspection_assigned'])
                self.assertEqual(user['modules'], ['risk_inspection'])

    def test_auth_status_without_assignment_does_not_add_permissions(self):
        user = self.auth_status([], False)
        self.assertEqual(user['modules'], [])
        self.assertNotIn('risk_inspection_assigned', user)

    def test_directory_contains_people_without_module_or_matching_building(self):
        self.people['other']['modules'] = []
        self.people['other']['scope_rules'] = []
        client = self.client()
        reply = client.get('/api/risk-inspections/people', headers={'x-person': 'admin'})
        self.assertEqual(reply.status_code, 200)
        people = reply.json()['data']['items']
        other = next(p for p in people if p['id'] == 'other')
        self.assertTrue(other['can_review'])
        self.assertIn('A', other['scopes'])
        self.assertIn('B', other['scopes'])
        self.assertNotIn('open_id', reply.text)

    def test_assignment_bypasses_only_risk_module_and_only_its_own_release(self):
        rid = self.publish()
        client = self.client()
        headers = {'x-person': 'alice'}
        for path in ('/api/risk-inspections', '/api/risk-inspections/' + rid):
            self.assertEqual(client.get(path, headers=headers).status_code, 200, path)
        self.assertEqual(client.get('/api/risk-inspections/' + 'b' * 32, headers=headers).status_code, 403)
        self.assertEqual(client.get('/api/events', headers=headers).status_code, 403)
        self.assertEqual(self.people['alice']['modules'], [])
        self.assertEqual(self.people['alice']['scope_rules'], ['A'])

    def test_assigned_non_manager_cannot_publish_or_read_template_library(self):
        rid = self.publish()
        client = self.client()
        headers = {'x-person': 'alice'}
        for path in ('/api/risk-inspections/people', '/api/risk-inspections/template', '/api/risk-inspections/base-templates'):
            self.assertEqual(client.get(path, headers=headers).status_code, 403, path)
        self.assertEqual(client.post('/api/risk-inspections', json={}, headers=headers).status_code, 403)
        self.assertEqual(client.request('DELETE', '/api/risk-inspections/' + rid, json={}, headers=headers).status_code, 403)

    def test_cross_building_assignee_can_upload_and_submit_but_not_spoof_an_item(self):
        rid = self.publish()
        client = self.client()
        headers = {'x-person': 'alice', 'content-type': 'application/octet-stream'}
        detail = client.get('/api/risk-inspections/' + rid, headers=headers).json()['data']
        person = detail['people'][0]
        item = next(i for i in person['items'] if i['scope'] == 'B')
        url = '/api/risk-inspections/' + rid + '/files?item_id=' + item['id'] + '&field_id=photo&name=scene.pdf'
        reply = client.post(url, content=b'%PDF-isolated-fixture', headers=headers)
        self.assertEqual(reply.status_code, 200, reply.text)
        fid = reply.json()['data']['id']
        action = client.post('/api/risk-inspections/' + rid + '/people/alice/actions', headers={'x-person': 'alice'},
            json={'action': 'submit', 'item_id': item['id'], 'version': person['version'], 'values': {'result': 'B楼正常', 'photo': [fid]}})
        self.assertEqual(action.status_code, 200, action.text)
        self.assertEqual(client.get('/api/risk-inspections/files/' + fid, headers=headers).status_code, 200)
        self.assertEqual(client.post(url.replace(item['id'], 'not-an-item'), content=b'%PDF', headers=headers).status_code, 409)

    def test_selected_non_manager_reviewer_can_open_and_review_own_task_once(self):
        rid = self.publish()
        client = self.client()
        detail = client.get('/api/risk-inspections/' + rid, headers={'x-person': 'alice'}).json()['data']
        person = detail['people'][0]
        item = person['items'][0]
        path = '/api/risk-inspections/' + rid + '/people/alice/actions'
        submit = client.post(path, headers={'x-person': 'alice'}, json={
            'action': 'submit', 'version': person['version'], 'item_id': item['id'], 'values': {'result': '正常'}})
        self.assertEqual(submit.status_code, 200, submit.text)
        version = submit.json()['data']['people'][0]['version']
        body = {'action': 'approve', 'version': version, 'item_id': item['id']}
        approve = client.post(path, headers={'x-person': 'alice'}, json=body)
        self.assertEqual(approve.status_code, 200, approve.text)
        stale = client.post(path, headers={'x-person': 'bob'}, json=body)
        self.assertEqual(stale.status_code, 409)

    def test_disabled_account_loses_task_grant_without_changing_stored_task(self):
        rid = self.publish()
        client = self.client()
        before = self.store.raw(rid)
        self.disabled.add('alice')
        session = {'user': {'open_id': 'ou_alice'}}
        self.assertFalse(risk_task_access(self.runtime, session, rid))
        self.assertEqual(client.get('/api/risk-inspections/' + rid, headers={'x-person': 'alice'}).status_code, 403)
        self.assertEqual(self.store.raw(rid), before)

    def test_legacy_manager_reviewer_management_is_retained_without_promoting_new_reviewers(self):
        rid = self.publish()
        release, _ = self.store.raw(rid)
        self.assertFalse(self.store.controls(self.people['alice'], release))
        release.pop('task_grants_version')
        release.pop('reviewers')
        release['reviewer'] = self.store.brief(self.people['reviewer'])
        self.assertTrue(self.store.controls(self.people['reviewer'], release))

    def test_partial_building_manager_cannot_read_other_floor_files_or_audit_values(self):
        self.people['admin']['level'] = 'L1'
        self.people['superA'] = fixtures.actor('superA', 'L0', 'A')
        rid = self.publish()
        client = self.client()
        detail = client.get('/api/risk-inspections/' + rid, headers={'x-person': 'alice'}).json()['data']
        person = detail['people'][0]
        item = next(i for i in person['items'] if i['scope'] == 'B')
        url = '/api/risk-inspections/' + rid + '/files?item_id=' + item['id'] + '&field_id=photo&name=B.pdf'
        upload = client.post(url, content=b'%PDF-B-scope', headers={'x-person': 'alice'})
        self.assertEqual(upload.status_code, 200, upload.text)
        fid = upload.json()['data']['id']
        reply = client.post('/api/risk-inspections/' + rid + '/people/alice/actions', headers={'x-person': 'alice'},
            json={'action': 'submit', 'item_id': item['id'], 'version': person['version'],
                  'values': {'result': 'B-SCOPE-MUST-NOT-LEAK', 'photo': [fid]}})
        self.assertEqual(reply.status_code, 200, reply.text)
        scoped = client.get('/api/risk-inspections/' + rid, headers={'x-person': 'superA'})
        self.assertEqual(scoped.status_code, 200, scoped.text)
        self.assertNotIn('B-SCOPE-MUST-NOT-LEAK', scoped.text)
        self.assertTrue(all(i['scope'] == 'A' for p in scoped.json()['data']['people'] for i in p['items']))
        self.assertFalse(scoped.json()['data']['can_manage_release'])
        self.assertEqual(scoped.json()['data']['url'], '')
        self.assertEqual(client.get('/api/risk-inspections/files/' + fid, headers={'x-person': 'superA'}).status_code, 403)

    def test_import_preview_does_not_publish_and_requires_manager(self):
        self.publish()
        client = self.client()
        workbook = Workbook()
        workbook.active.append(['排查事项', '排查结果'])
        workbook.active.append(['检查设备', '现场核对'])
        stream = io.BytesIO()
        workbook.save(stream)
        workbook.close()
        url = '/api/risk-inspections/base-templates/import-file?name=checks.xlsx'
        with self.store.connect() as db:
            before = db.execute('SELECT COUNT(*) FROM risk_releases').fetchone()[0]
        reply = client.post(url, content=stream.getvalue(), headers={'x-person': 'admin', 'content-type': 'application/octet-stream'})
        self.assertEqual(reply.status_code, 200, reply.text)
        self.assertEqual(len(reply.json()['data']['items']), 1)
        self.assertEqual(client.post(url, content=stream.getvalue(), headers={'x-person': 'alice'}).status_code, 403)
        with self.store.connect() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM risk_releases').fetchone()[0], before)

    def test_import_bitable_permission_and_cross_origin_checked_before_cloud_read(self):
        self.publish()
        client = self.client()
        path = '/api/risk-inspections/base-templates/import-bitable'
        remote = Mock()
        with patch('bin.lan_bitable_template_portal.risk_inspection_imports.preview_bitable', remote):
            for headers in ({'x-person': 'alice'}, {'x-person': 'admin', 'Origin': 'https://foreign.test'}):
                reply = client.post(path, json={'url': 'https://vnet.feishu.cn/base/example?table=tblExample'}, headers=headers)
                self.assertEqual(reply.status_code, 403)
            remote.assert_not_called()


if __name__ == '__main__':
    unittest.main()
