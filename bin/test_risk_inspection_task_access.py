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

    def test_import_and_save_base_template_keeps_existing_role_permissions(self):
        self.people['publisher'] = fixtures.actor('publisher', 'L1')
        self.people['no_module'] = {**fixtures.actor('no_module', 'L3'), 'modules': []}
        content = '排查事项,排查结果\n检查设备,现场核对\n'.encode('utf-8-sig')
        base = '/api/risk-inspections/base-templates'
        remote = Mock()
        # No lifespan/worker is started; all state is in the fixture's temporary database.
        with patch('bin.lan_bitable_template_portal.risk_inspection_routes.RiskCloud', return_value=remote):
            client = self.client()
            for identity, can_edit in [('publisher', True), ('reviewer', False)]:
                with self.subTest(identity=identity):
                    headers = {'x-person': identity, 'content-type': 'application/octet-stream'}
                    permissions = client.get('/api/risk-inspections', headers=headers).json()['data']
                    self.assertTrue(permissions['is_manager'])
                    self.assertEqual(permissions['can_edit_template'], can_edit)
                    preview = client.post(base + '/import-file?name=checks.csv', content=content, headers=headers)
                    self.assertEqual(preview.status_code, 200, preview.text)
                    draft = preview.json()['data']
                    with self.store.connect() as db:
                        self.assertEqual(db.execute('SELECT COUNT(*) FROM risk_releases').fetchone()[0], 0)
                        self.assertEqual(db.execute('SELECT COUNT(*) FROM risk_history_templates WHERE json_extract(data,\'$.creator.id\')=?', (identity,)).fetchone()[0], 0)
                    body = {'id': ('b' if identity == 'publisher' else 'c') * 32, 'name': draft['name'],
                            'enabled': True, 'fields': draft['fields'], 'items': draft['items']}
                    saved = client.post(base, json=body, headers={'x-person': identity})
                    self.assertEqual(saved.status_code, 200, saved.text)
                    self.assertEqual(saved.json()['data']['scopes'], ['A'])
                    self.assertEqual(saved.json()['data']['items'], draft['items'])
                    retry = client.post(base, json=body, headers={'x-person': identity})
                    self.assertEqual(retry.status_code, 200, retry.text)
                    self.assertEqual(retry.json()['data']['id'], body['id'])
            for identity in ['alice', 'no_module']:
                for endpoint, kwargs in [('/import-file?name=checks.csv', {'content': content}),
                                         ('', {'json': {**body, 'id': 'd' * 32}})]:
                    with self.subTest(identity=identity, endpoint=endpoint):
                        reply = client.post(base + endpoint, headers={'x-person': identity}, **kwargs)
                        self.assertEqual(reply.status_code, 403, reply.text)
                        self.assertTrue(reply.json().get('error'))
            remote.assert_not_called()
            self.assertEqual(remote.mock_calls, [], 'Import/save cannot read cloud data or send messages')
        with self.store.connect() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM risk_history_templates').fetchone()[0], 2)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM risk_releases').fetchone()[0], 0)

    def publish_role_task(self, rid='a' * 32, reviewers=None, recipients=None):
        body = {**self.body, 'id': rid, 'reviewer_ids': reviewers or ['alice'],
                'people': recipients or [{'id': 'alice', 'scopes': ['A', 'B']}, {'id': 'bob', 'scopes': ['B']}]}
        body.pop('reviewer_id')
        self.store.publish(self.people['admin'], body, self.template)
        self.store.activate(rid)
        with self.store.connect(True) as db:
            db.execute("UPDATE risk_jobs SET status='done' WHERE kind='publish'")
        return rid

    def test_dual_role_lists_have_independent_person_counts_and_same_task(self):
        rid = self.publish_role_task()
        before = self.store.raw(rid)
        mine = self.store.list(self.people['alice'], view='mine')
        review = self.store.list(self.people['alice'], view='review')
        self.assertEqual((mine['view'], review['view']), ('mine', 'review'))
        self.assertEqual((mine['total'], review['total']), (1, 1))
        self.assertEqual((mine['items'][0]['id'], review['items'][0]['id']), (rid, rid))
        self.assertEqual((mine['items'][0]['people_count'], mine['items'][0]['total']), (1, 4))
        self.assertEqual((review['items'][0]['people_count'], review['items'][0]['total']), (2, 6))
        self.assertEqual(self.store.list(self.people['alice'], view='mine', scope='B')['items'][0]['total'], 2)
        self.assertEqual(self.store.list(self.people['alice'], view='review', scope='B')['items'][0]['total'], 4)
        self.assertEqual(self.store.raw(rid), before, 'Role views must never rewrite stored tasks')

    def test_assignee_reviewer_and_publisher_lists_do_not_mix_roles(self):
        rid = self.publish_role_task(reviewers=['reviewer'])
        client = self.client()
        for identity, expected in [('alice', (1, 0)), ('reviewer', (0, 1)), ('other', (0, 0)), ('admin', (0, 0))]:
            with self.subTest(identity=identity):
                counts = []
                for view in ['mine', 'review']:
                    result = client.get('/api/risk-inspections?view=' + view, headers={'x-person': identity})
                    self.assertEqual(result.status_code, 200, result.text)
                    counts.append(result.json()['data']['total'])
                self.assertEqual(tuple(counts), expected)
        published = self.store.list(self.people['admin'], view='published')
        self.assertEqual(published['total'], 1)
        self.assertEqual(self.store.list(self.people['admin'], view='all')['total'], 1)
        self.assertEqual(self.store.list(self.people['admin'], view='review')['total'], 0)
        self.assertTrue(published['items'][0]['can_claim'])
        self.assertTrue(self.store.get(self.people['admin'], rid)['can_claim'])
        self.assertEqual(client.get('/api/risk-inspections?view=bad', headers={'x-person': 'alice'}).status_code, 400)

    def test_publisher_mine_requires_own_assignment_in_selected_scope(self):
        unassigned = self.publish_role_task(reviewers=['admin'])
        assigned = self.publish_role_task('b' * 32, reviewers=['admin'],
                                         recipients=[{'id': 'admin', 'scopes': ['A']}, {'id': 'bob', 'scopes': ['B']}])
        before = {rid: self.store.raw(rid) for rid in (unassigned, assigned)}
        client = self.client()
        for scope, expected in [('', [assigned]), ('ALL', [assigned]), ('A', [assigned]), ('B', [])]:
            with self.subTest(scope=scope):
                response = client.get('/api/risk-inspections?view=mine&scope=' + scope, headers={'x-person': 'admin'})
                self.assertEqual(response.status_code, 200, response.text)
                data = response.json()['data']
                self.assertEqual(data['total'], len(expected))
                self.assertEqual([row['id'] for row in data['items']], expected)
                for row in data['items']:
                    self.assertEqual((row['people_count'], row['total']), (1, 2))
                    self.assertFalse(row['can_claim'])
        for view in ['all', 'published', 'review']:
            data = self.store.list(self.people['admin'], view=view, scope='B')
            self.assertEqual(data['total'], 2)
            self.assertEqual({row['id'] for row in data['items']}, {unassigned, assigned})
        self.assertEqual({rid: self.store.raw(rid) for rid in before}, before)

    def test_role_filter_is_applied_before_pagination(self):
        expected = {'mine': set(), 'review': set()}
        for index in range(44):
            rid = format(index + 1, '032x')
            is_assignee, is_reviewer = index <= 22, index >= 22
            self.publish_role_task(rid, ['alice' if is_reviewer else 'reviewer'],
                                   [{'id': 'alice' if is_assignee else 'bob', 'scopes': ['B']}])
            for view, included in [('mine', is_assignee), ('review', is_reviewer)]:
                if included:
                    expected[view].add(rid)
        for view, ids in expected.items():
            with self.subTest(view=view):
                first = self.store.list(self.people['alice'], page=1, view=view)
                second = self.store.list(self.people['alice'], page=2, view=view)
                self.assertEqual((first['total'], second['total']), (len(ids), len(ids)))
                self.assertEqual((len(first['items']), len(second['items'])), (20, len(ids) - 20))
                self.assertEqual({row['id'] for result in (first, second) for row in result['items']}, ids)
                self.assertEqual(self.store.list(self.people['alice'], page=3, view=view)['items'], [])

    def test_legacy_single_reviewer_and_single_scope_are_read_without_migration(self):
        rid = self.publish_role_task(reviewers=['reviewer'], recipients=[{'id': 'bob', 'scopes': ['A']}])
        release, people = self.store.raw(rid)
        release.pop('reviewers')
        release.pop('task_grants_version')
        for person in people:
            person.pop('scopes')
            for item in person['items']:
                item.pop('scope')
        with self.store.connect(True) as db:
            self.store._put_release(db, release)
            for person in people:
                self.store._put_person(db, rid, person)
        before = self.store.raw(rid)
        self.assertEqual(self.store.list(self.people['reviewer'], view='review')['items'][0]['total'], 2)
        self.assertEqual(self.store.list(self.people['reviewer'], view='mine')['total'], 0)
        self.assertEqual(self.store.list(self.people['admin'], view='mine')['total'], 0)
        self.assertEqual(self.store.list(self.people['admin'], view='published')['total'], 1)
        self.assertEqual(self.store.list(self.people['bob'], view='mine')['items'][0]['total'], 2)
        self.assertEqual(self.store.list(self.people['bob'], view='mine', scope='A')['items'][0]['total'], 2)
        self.assertTrue(self.store.get(self.people['reviewer'], rid)['is_reviewer'])
        self.assertEqual(self.store.raw(rid), before)

    def test_reviewer_replacement_and_revocation_remove_only_the_corresponding_right(self):
        rid = self.publish_role_task()
        client = self.client()
        self.store.change_reviewer(self.people['admin'], rid, {'version': 1, 'reviewer_ids': ['bob']})
        self.assertEqual(self.store.list(self.people['alice'], view='review')['total'], 0)
        self.assertEqual(self.store.list(self.people['alice'], view='mine')['total'], 1)
        self.assertEqual(self.store.list(self.people['bob'], view='review')['total'], 1)
        detail = self.store.get(self.people['alice'], rid)
        person = detail['people'][0]
        path = '/api/risk-inspections/' + rid + '/people/alice/actions'
        submission = client.post(path, headers={'x-person': 'alice'}, json={'action': 'submit',
            'version': person['version'], 'item_id': person['items'][0]['id'], 'values': {'result': '无异常'}})
        self.assertEqual(submission.status_code, 200, submission.text)
        version = submission.json()['data']['people'][0]['version']
        denied = client.post(path, headers={'x-person': 'alice'}, json={'action': 'approve',
            'version': version, 'item_id': person['items'][0]['id']})
        self.assertEqual(denied.status_code, 403, denied.text)
        revoked = self.store.action(self.people['admin'], rid, 'alice',
                                   {'action': 'revoke_person', 'version': version, 'reason': '调整分配'})
        person = next(p for p in revoked['people'] if p['id'] == 'alice')
        write = client.post(path, headers={'x-person': 'alice'}, json={'action': 'submit',
            'version': person['version'], 'item_id': person['items'][1]['id'], 'values': {'result': '无异常'}})
        self.assertEqual(write.status_code, 409, write.text)
        self.assertEqual(self.store.list(self.people['alice'], view='mine')['items'][0]['total'], 0)
        self.disabled.add('bob')
        for view in ['mine', 'review']:
            self.assertEqual(client.get('/api/risk-inspections?view=' + view, headers={'x-person': 'bob'}).status_code, 403)

    def test_task_grant_routes_do_not_grant_published_or_other_modules(self):
        self.people['alice']['modules'] = []
        self.publish_role_task()
        client = self.client()
        for view in ['mine', 'review']:
            response = client.get('/api/risk-inspections?view=' + view, headers={'x-person': 'alice'})
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json()['data']['total'], 1)
        self.assertEqual(client.get('/api/risk-inspections?view=published', headers={'x-person': 'alice'}).json()['data']['view'], 'mine')
        self.assertEqual(client.get('/api/events', headers={'x-person': 'alice'}).status_code, 403)
        self.assertEqual(client.post('/api/risk-inspections', json={}, headers={'x-person': 'alice'}).status_code, 403)
        self.assertEqual(self.people['alice']['modules'], [])


if __name__ == '__main__':
    unittest.main()
