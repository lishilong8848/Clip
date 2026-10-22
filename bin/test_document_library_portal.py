"""Production bridge integration with temporary stores and offline providers."""
import asyncio
import base64
import copy
import json
import os
from pathlib import Path
import sys
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from fastapi import FastAPI
from fastapi.testclient import TestClient
from lan_bitable_template_portal.document_library import DocumentLibraryService
from lan_bitable_template_portal.document_library_portal import install_document_library_portal
from lan_bitable_template_portal.portal_auth import PortalAuthManager
from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_knowledge import KnowledgeBase
from bin.test_document_library import LibraryFixture, actor, configuration
from bin.test_lighthouse_knowledge import FakeEmbedder, FakeMemoryIndex


class DocumentLibraryPortalTests(LibraryFixture):
    def setUp(self):
        super().setUp()
        self.profile = {'role': 'building', 'modules': ['knowledge'], 'scopes': ['A']}
        self.session = {'user': {'open_id': actor()['id'], 'name': 'Fixture reader'},
                        'is_admin': True, 'modules': ['assistant'], 'allowed_scopes': ['ALL']}
        auth = SimpleNamespace(
            account_profile=lambda identity: copy.deepcopy(self.profile),
            scopes_for_open_id=lambda identity: list(self.profile['scopes']),
            _scopes_from_raw=lambda values: values or [])
        auth.is_admin = lambda session: PortalAuthManager.is_admin(auth, session)
        auth.session_scopes = lambda session: PortalAuthManager.session_scopes(auth, session)
        self.read_session = Mock(side_effect=lambda request: copy.deepcopy(self.session))
        controller = SimpleNamespace(_current_session=self.read_session,
            _request_base_url=lambda request: str(request.base_url).rstrip('/'))
        self.runtime = SimpleNamespace(auth_manager=auth,
            state_store=SimpleNamespace(db_path=Path(self.temp) / 'portal.sqlite3'))
        self.bridge_service = None

        def build(root, **kwargs):
            self.bridge_service = DocumentLibraryService(root, remote_factory=lambda _: self.remote,
                knowledge=self.kb, clock=lambda: self.now, **kwargs)
            return self.bridge_service

        self.factory = self.enterContext(patch(
            'lan_bitable_template_portal.document_library_portal.DocumentLibraryService', side_effect=build))
        app = FastAPI()
        install_document_library_portal(app, controller, self.runtime)
        self.client = self.enterContext(TestClient(app, base_url='https://portal.example'))
        self.prefix = '/api/document-library/tables/manuals'
        self.origin = {'origin': 'https://portal.example'}
        for target in ('socket.socket.connect', 'socket.socket.connect_ex', 'socket.getaddrinfo'):
            self.enterContext(patch(target, side_effect=AssertionError('Network is forbidden')))

    def data(self, path):
        response = self.client.get(path)
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.headers['cache-control'], 'no-store')
        return response.json()['data']

    def person_fixture(self, *, multiple=False):
        self.directory = {'schema': 4, 'app_id': 'cliFixture', 'loaded_at': time.time(), 'people': {
            'recPersonA': {'name': 'Person A', 'login_ids': ['ou_PersonA'], 'scopes': ['A'],
                'selectable': True, 'inactive': False, 'employee_no': 'private fixture',
                'password_revision': 'private fixture', 'phone': 'private fixture'},
            'recPersonAB': {'name': 'Person AB', 'login_ids': ['ou_PersonAB'], 'scopes': ['A', 'B'],
                'selectable': True, 'inactive': False},
            'recPersonB': {'name': 'Person B', 'login_ids': ['ou_PersonB'], 'scopes': ['B'],
                'selectable': True, 'inactive': False}}}
        auth = self.runtime.auth_manager
        self.current_app_id = 'cliFixture'
        auth.current_feishu_app_id = lambda: self.current_app_id
        auth._account_directory_provider = Mock(side_effect=lambda: self.directory)
        auth._open_id_explicitly_disabled = Mock(return_value=False)
        auth._account_directory_loader = Mock(side_effect=AssertionError('No directory refresh'))
        auth.sync_directory_permissions = Mock(side_effect=AssertionError('No permission changes'))
        self.addCleanup(auth._account_directory_loader.assert_not_called)
        self.addCleanup(auth.sync_directory_permissions.assert_not_called)
        self.person_schema = {'field_id': 'fldOwner', 'field_name': 'Owner', 'type': 11,
            'property': {'multiple': multiple}}
        self.enterContext(patch.object(self.remote, 'fields', return_value=[*self.remote.fields(), self.person_schema]))
        self.remote.rows['recOne']['fields']['Owner'] = []

    def person_schema_response(self):
        return next(field for field in self.data(self.prefix + '/schema')['fields'] if field['field_id'] == 'fldOwner')

    def save_person(self, value, operation='fixture-person-edit'):
        current = self.data(self.prefix + '/records/recOne')
        return self.client.patch(self.prefix + '/records/recOne', headers=self.origin,
            json={'fields': {'fldOwner': value}, 'expected_revision': current['revision'],
                'operation_id': operation, 'reason': 'fixture person edit'})

    def test_person_options_use_scoped_current_directory_and_only_id_name(self):
        self.person_fixture()
        field = self.person_schema_response()
        self.assertEqual(field['property'], {'multiple': False, 'options': [
            {'id': 'ou_PersonA', 'name': 'Person A'}, {'id': 'ou_PersonAB', 'name': 'Person AB'}]})
        self.assertFalse(field['read_only'])
        self.assertNotIn('private fixture', json.dumps(field))
        self.assertEqual(self.person_schema['property'], {'multiple': False})
        self.assertEqual(self.profile['scopes'], ['A'])

    def test_person_options_intersect_actor_source_write_and_fixed_scopes(self):
        self.person_fixture()
        self.profile['scopes'] = ['A', 'B']
        for change, expected in (({'write_scopes': ['A']}, ['ou_PersonA', 'ou_PersonAB']),
                ({'read_scopes': ['B'], 'write_scopes': ['B']}, ['ou_PersonAB', 'ou_PersonB']),
                ({'fixed_scopes': ['A']}, ['ou_PersonA', 'ou_PersonAB'])):
            with self.subTest(change=change):
                source = self.service._table('manuals')
                self.service.save_table(self.admin, configuration(expected_revision=source['revision'], **change), key='manuals')
                options = self.person_schema_response()['property']['options']
                self.assertEqual([person['id'] for person in options], expected)

    def test_person_options_exclude_inactive_ambiguous_and_invalid_login_ids(self):
        self.person_fixture()
        original = copy.deepcopy(self.directory['people']['recPersonA'])
        for change in ({'selectable': False}, {'inactive': True}, {'disabled_reason': 'pending'},
                {'login_ids': []}, {'login_ids': ['ou_PersonA', 'ou_Other']}, {'login_ids': 'ou_PersonA'},
                {'login_ids': [7]}, {'login_ids': ['old-app-id']}, {'name': ''}, {'scopes': ['C']},
                {'login_ids': [], 'feishu_open_ids': {'cliFixture': 'ou_PersonA'}}):
            with self.subTest(change=change):
                self.directory['people']['recPersonA'] = {**original, **change}
                options = self.person_schema_response()['property']['options']
                self.assertEqual(options, [{'id': 'ou_PersonAB', 'name': 'Person AB'}])
        self.directory['people']['recPersonA'] = original
        self.directory['people']['recDuplicate'] = {**original, 'scopes': ['B']}
        self.assertEqual(self.person_schema_response()['property']['options'], [{'id': 'ou_PersonAB', 'name': 'Person AB'}])
        self.directory['people'].pop('recDuplicate')
        self.runtime.auth_manager._open_id_explicitly_disabled.side_effect = lambda oid: oid == 'ou_PersonA'
        self.assertEqual(self.person_schema_response()['property']['options'], [{'id': 'ou_PersonAB', 'name': 'Person AB'}])

    def test_missing_stale_or_wrong_app_directory_explicitly_disables_person_field(self):
        self.person_fixture()
        original = copy.deepcopy(self.directory)
        for changes in (None, {}, {'schema': 3}, {'app_id': 'cliOld'}, {'people': []},
                {'loaded_at': time.time() - 301}, {'loaded_at': 'invalid'}):
            with self.subTest(changes=changes):
                self.directory = None if changes is None else {} if changes == {} else {**original, **changes}
                field = self.person_schema_response()
                self.assertTrue(field['read_only'])
                self.assertTrue(field['read_only_reason'].startswith('人员目录暂未就绪'))
                self.assertEqual(field['property'], {'multiple': False, 'options': []})
        self.directory = original
        self.runtime.auth_manager._account_directory_refresh_failed = True
        self.assertTrue(self.person_schema_response()['read_only'])
        self.runtime.auth_manager._account_directory_refresh_failed = False
        self.runtime.auth_manager._account_directory_provider.side_effect = OSError('private directory failure')
        field = self.person_schema_response()
        self.assertNotIn('private directory failure', json.dumps(field))
        self.assertTrue(field['read_only'])

    def test_directory_app_change_during_lookup_fails_closed(self):
        self.person_fixture()
        def rotate(identity):
            self.current_app_id = 'cliChanged'
            return False
        self.runtime.auth_manager._open_id_explicitly_disabled.side_effect = rotate
        field = self.person_schema_response()
        self.assertTrue(field['read_only'])
        self.assertEqual(field['property']['options'], [])

    def test_empty_scoped_directory_does_not_silently_offer_person_editing(self):
        self.person_fixture()
        self.directory['people'] = {}
        field = self.person_schema_response()
        self.assertTrue(field['read_only'])
        self.assertEqual(field['read_only_reason'], '当前楼栋权限范围内没有可选人员。')

    def test_person_write_validates_single_count_openids_and_directory_membership(self):
        self.person_fixture()
        for value, status, message in (([{'id': 'ou_PersonA'}, {'id': 'ou_PersonAB'}], 400, '此人员字段仅允许选择一人。'),
                ([{'id': 'person:recOne'}], 400, '人员字段必须使用有效的飞书 OpenID 列表。'),
                (['ou_PersonA'], 400, '人员字段必须使用有效的飞书 OpenID 列表。'),
                ([{'id': 7}], 400, '人员字段必须使用有效的飞书 OpenID 列表。'),
                ([{'id': 'ou_PersonA', 'phone': 'private fixture'}], 400, '人员字段必须使用有效的飞书 OpenID 列表。'),
                ([{'id': 'ou_PersonB'}], 403, '所选人员已不可用或超出当前楼栋权限，请重新选择。'),
                ([{'id': 'ou_Unlisted'}], 403, '所选人员已不可用或超出当前楼栋权限，请重新选择。')):
            with self.subTest(value=value):
                response = self.save_person(value)
                self.assertEqual(response.status_code, status, response.text)
                self.assertEqual(response.json()['error'], message)
        self.assertEqual(self.remote.updates, 0)
        with self.service.connect(readonly=True) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM operations').fetchone()[0], 0)

    def test_person_write_sends_only_ids_and_accepts_enriched_readback(self):
        self.person_fixture()
        original = self.remote.update
        def enriched(identity, fields):
            self.assertEqual(fields['Owner'], [{'id': 'ou_PersonAB'}])
            result = original(identity, fields)
            self.remote.rows[identity]['fields']['Owner'][0]['name'] = 'Person AB'
            return result
        with patch.object(self.remote, 'update', side_effect=enriched):
            response = self.save_person([{'id': 'ou_PersonAB', 'name': 'untrusted label'}])
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(self.remote.updates, 1)
        self.assertEqual(response.json()['data']['fields']['fldOwner'], [{'id': 'ou_PersonAB', 'name': 'Person AB'}])

    def test_person_create_and_completed_retry_do_not_resend_or_require_directory(self):
        self.person_fixture()
        payload = {'fields': {'fldScope': 'A', 'fldOwner': [{'id': 'ou_PersonA', 'name': 'Person A'}]},
            'operation_id': 'fixture-person-create', 'reason': 'fixture person create'}
        response = self.client.post(self.prefix + '/records', headers=self.origin, json=payload)
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()['data']['fields']['fldOwner'], [{'id': 'ou_PersonA'}])
        self.directory = None
        repeated = self.client.post(self.prefix + '/records', headers=self.origin, json=payload)
        self.assertEqual(repeated.status_code, 200, repeated.text)
        self.assertEqual(repeated.json()['data']['record_id'], response.json()['data']['record_id'])
        self.assertEqual(self.remote.creates, 1)

    def test_service_without_people_provider_explicitly_disables_person_editing(self):
        self.person_fixture()
        schema = self.service.schema(actor(), 'manuals')
        field = next(field for field in schema['fields'] if field['field_id'] == 'fldOwner')
        self.assertTrue(field['read_only'])
        self.assertTrue(field['read_only_reason'].startswith('人员目录暂未就绪'))
        with self.assertRaises(AssistantError) as error:
            self.service.save_record(actor(), 'manuals', self.mutation({'fldOwner': [{'id': 'ou_PersonA'}]}), identity='recOne')
        self.assertEqual(error.exception.category, 'document_library_people_unavailable')
        self.assertEqual(self.remote.updates, 0)

    def test_person_multiple_and_clear_keep_native_list_contract(self):
        self.person_fixture(multiple=True)
        self.assertTrue(self.person_schema_response()['property']['multiple'])
        response = self.save_person([{'id': 'ou_PersonA'}, {'id': 'ou_PersonAB'}])
        self.assertEqual(response.status_code, 200, response.text)
        response = self.save_person([], operation='fixture-person-clear')
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()['data']['fields']['fldOwner'], [])
        response = self.save_person([{'id': 'ou_PersonA'}, {'id': 'ou_PersonA'}], operation='fixture-person-duplicate')
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()['error'], '人员字段不能重复选择同一人。')

    def test_person_directory_revocation_after_schema_blocks_write_but_not_other_fields(self):
        self.person_fixture()
        self.person_schema_response()
        self.directory['people']['recPersonA']['selectable'] = False
        self.assertEqual(self.save_person([{'id': 'ou_PersonA'}]).status_code, 403)
        self.directory = None
        response = self.save_person([{'id': 'ou_PersonAB'}])
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()['code'], 'document_library_people_unavailable')
        self.assertEqual(self.remote.updates, 0)
        response = self.client.patch(self.prefix + '/records/recOne', headers=self.origin,
            json=self.mutation({'fldNumber': 21}))
        self.assertEqual(response.status_code, 200, response.text)

    def test_raster_download_mime_and_active_files_keep_attachment_protection(self):
        png = base64.b64decode('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVR4nGP4z8AAAAMBAQDJ/pLvAAAAAElFTkSuQmCC')
        for name, content, expected in (('fixture.PNG', png, 'image/png'),
                ('fixture.svg', b'<svg xmlns="http://www.w3.org/2000/svg"></svg>', 'application/octet-stream'),
                ('fixture.html', b'<html>fixture</html>', 'application/octet-stream'),
                ('fixture.bin', b'fixture', 'application/octet-stream')):
            with self.subTest(name=name):
                token = 'tokenFixture' + name
                self.remote.files[token] = content
                self.remote.rows['recOne']['fields']['Files'] = [{'file_token': token, 'name': name, 'size': len(content)}]
                record = self.data(self.prefix + '/records/recOne')
                url = record['attachments'][0]['download_url']
                response = self.client.get(url)
                self.assertEqual(response.status_code, 200, response.text)
                self.assertEqual(response.content, content)
                self.assertEqual(response.headers['content-type'], expected)
                self.assertTrue(response.headers['content-disposition'].startswith('attachment;'))
                self.assertEqual(response.headers['x-content-type-options'], 'nosniff')
                self.assertEqual(response.headers['cache-control'], 'private, no-store')
                self.session, original = None, self.session
                self.assertEqual(self.client.get(url).status_code, 401)
                self.session = original

    def test_knowledge_only_uses_same_database_and_live_profile(self):
        tables = self.data('/api/document-library/tables')
        self.assertEqual([table['id'] for table in tables['items']], ['manuals'])
        self.assertFalse(tables['can_configure'])
        self.assertEqual(self.bridge_service.db_path, self.service.db_path)
        records = self.data(self.prefix + '/records')
        self.assertEqual([record['record_id'] for record in records['items']], ['recOne'])
        self.assertEqual(self.factory.call_count, 1)

    def test_anonymous_guest_and_temporary_sessions_never_open_service(self):
        for session, status in ((None, 401), ({}, 401),
                ({**self.session, 'is_guest': True}, 403),
                ({**self.session, 'role': 'guest'}, 403),
                ({**self.session, 'role': 'temporary'}, 403)):
            with self.subTest(session=session):
                self.session = session
                response = self.client.get('/api/document-library/tables')
                self.assertEqual(response.status_code, status, response.text)
                self.assertFalse(response.json()['ok'])
        self.factory.assert_not_called()

    def test_assistant_and_admin_do_not_replace_knowledge_grant(self):
        for role in ('building', 'admin'):
            self.profile.update(role=role, modules=['assistant', 'accounts'])
            response = self.client.get('/api/document-library/tables')
            self.assertEqual(response.status_code, 403)
            self.assertEqual(response.json()['error'], '当前账号没有共享知识库权限。')
        self.factory.assert_not_called()

    def test_only_live_admin_can_configure_and_cannot_bypass_record_scope(self):
        response = self.client.post('/api/document-library/tables', headers=self.origin,
            json=configuration(id='other', table_id='tblOther'))
        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.json()['error'], '仅管理员可配置资料表。')
        self.profile['role'] = 'admin'
        response = self.client.post('/api/document-library/tables', headers=self.origin,
            json=configuration(id='other', table_id='tblOther'))
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(self.client.get(self.prefix + '/records/recTwo').status_code, 404)

    def test_admin_can_disable_or_remove_all_read_scopes_with_successful_receipt(self):
        self.profile['role'] = 'admin'
        for changes in ({'enabled': False}, {'read_scopes': [], 'write_scopes': []}):
            with self.subTest(changes=changes):
                current = self.service._table('manuals')
                response = self.client.put(self.prefix, headers=self.origin,
                    json=configuration(expected_revision=current['revision'], **changes))
                self.assertEqual(response.status_code, 200, response.text)
                for key, value in changes.items():
                    self.assertEqual(response.json()['data'][key], value)
                self.assertTrue(self.data('/api/document-library/tables')['can_configure'])
                self.assertEqual(self.client.get(self.prefix + '/records/recOne').status_code, 404)

    def test_table_keys_matching_actions_still_update_configuration(self):
        self.profile['role'] = 'admin'
        for key in ('schema', 'records', 'sync', 'attachments', 'versions', 'file'):
            with self.subTest(key=key):
                payload = configuration(id=key, table_id='tblReserved' + key)
                response = self.client.post('/api/document-library/tables', headers=self.origin, json=payload)
                self.assertEqual(response.status_code, 200, response.text)
                response = self.client.put('/api/document-library/tables/' + key, headers=self.origin,
                    json={**payload, 'name': 'Renamed fixture', 'expected_revision': response.json()['data']['revision']})
                self.assertEqual(response.status_code, 200, response.text)
                self.assertEqual(response.json()['data']['name'], 'Renamed fixture')

    def test_write_requires_same_origin_or_same_origin_referer(self):
        payload = self.mutation({'fldNumber': 17})
        for headers in ({}, {'origin': 'https://other.example'}, {'origin': 'null'},
                {'origin': 'http://portal.example'}, {'origin': 'https://portal.example:444'},
                {**self.origin, 'sec-fetch-site': 'cross-site'}):
            with self.subTest(headers=headers):
                response = self.client.patch(self.prefix + '/records/recOne', json=payload, headers=headers)
                self.assertEqual(response.status_code, 403)
                self.assertEqual(response.json()['error'], '不允许跨来源提交资料。')
        self.assertEqual(self.remote.updates, 0)
        response = self.client.patch(self.prefix + '/records/recOne', json=payload,
            headers={'referer': 'https://portal.example/document-library'})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(self.remote.updates, 1)

    def test_impersonation_is_readonly_in_capabilities_and_all_mutations(self):
        self.session['source'] = 'admin_impersonation'
        self.profile['role'] = 'admin'
        table = self.data('/api/document-library/tables')
        self.assertFalse(table['can_configure'])
        self.assertFalse(table['items'][0]['capabilities']['create'])
        record = self.data(self.prefix + '/records/recOne')
        self.assertFalse(record['capabilities']['edit'])
        self.assertFalse(record['capabilities']['upload'])
        for method, path in (('post', '/api/document-library/tables'), ('put', self.prefix),
                ('post', self.prefix + '/records'), ('patch', self.prefix + '/records/recOne'),
                ('post', self.prefix + '/records/recOne/attachments'), ('post', self.prefix + '/sync')):
            response = getattr(self.client, method)(path, headers=self.origin, json={})
            self.assertEqual(response.status_code, 403, response.text)
            self.assertEqual(response.json()['error'], '临时代登录仅可查看资料，不能修改或上传。')
        self.assertEqual((self.remote.creates, self.remote.updates, self.remote.uploads), (0, 0, 0))

    def test_campus_and_all_expand_only_to_known_buildings(self):
        self.data('/api/document-library/tables')
        for grant, expected in ((['CAMPUS'], set('ABCDE')),
                (['ALL'], {'A', 'B', 'C', 'D', 'E', 'H', '110'}), (['A', 'unknown'], {'A'})):
            self.profile['scopes'] = grant
            refreshed = self.bridge_service.refresh_actor(actor())
            self.assertEqual(set(refreshed['scopes']), expected)

    def test_unknown_source_scope_denies_record_search_and_download(self):
        current = self.data(self.prefix + '/records/recOne')
        self.remote.rows['recOne']['fields']['Building'] = 'A/unknown'
        self.assertEqual(self.client.get(self.prefix + '/records/recOne').status_code, 404)
        self.assertEqual(self.client.get(current['attachments'][0]['download_url']).status_code, 404)
        self.assertEqual(self.kb.search(actor(), 'Alpha policy')['items'], [])

    def test_read_and_write_scopes_only_narrow_actor_grants(self):
        current = self.service._table('manuals')
        self.service.save_table(self.admin, configuration(write_scopes=['B'],
            expected_revision=current['revision']), key='manuals')
        record = self.data(self.prefix + '/records/recOne')
        self.assertFalse(record['capabilities']['edit'])
        response = self.client.patch(self.prefix + '/records/recOne', headers=self.origin,
            json={'fields': {'fldNumber': 42}, 'expected_revision': record['revision'],
                  'operation_id': 'fixture-denied-write', 'reason': 'fixture'})
        self.assertEqual(response.status_code, 404)
        self.assertEqual(self.remote.updates, 0)

    def test_revocation_during_read_blocks_response_and_cached_knowledge(self):
        self.data('/api/document-library/tables')
        found = self.kb.search(actor(), 'Alpha policy')['items']
        turn = {'answer': 'Protected alpha body', 'sources': [{'kind': 'company_knowledge', 'data': found[0]}],
                'output_files': [{'id': 'fixture-derived'}]}
        self.kb.bind_source_artifacts(turn)
        original = self.remote.get
        def revoke(identity):
            result = original(identity)
            self.profile['modules'] = []
            return result
        with patch.object(self.remote, 'get', side_effect=revoke):
            response = self.client.get(self.prefix + '/records/recOne')
        self.assertEqual(response.status_code, 403)
        self.assertNotIn('Alpha policy', response.text)
        self.assertFalse(self.kb.evidence_current(found, {'endpoint': 'https://company.example/v1'}, actor=actor()))
        self.assertFalse(self.kb.cached_turn_current(actor(), turn))
        self.assertFalse(self.kb.artifact_current(actor(), 'fixture-derived'))
        self.assertNotIn('Protected alpha body', json.dumps(self.kb.filter_cached_turn(actor(), turn)))

    def test_scope_and_session_revocation_at_response_boundary(self):
        original = self.service.list_records
        for change in ('scopes', 'identity', 'logout'):
            with self.subTest(change=change):
                self.profile['scopes'] = ['A']
                self.session = {'user': {'open_id': actor()['id']}}
                self.data('/api/document-library/tables')
                def revoke(*args):
                    result = original(*args)
                    if change == 'scopes':
                        self.profile['scopes'] = ['B']
                    elif change == 'identity':
                        self.session['user']['open_id'] = 'ouOther'
                    else:
                        self.session = None
                    return result
                with patch.object(self.bridge_service, 'list_records', side_effect=revoke):
                    response = self.client.get(self.prefix + '/records')
                self.assertIn(response.status_code, (401, 403))
                self.assertNotIn('Alpha policy', response.text)

    def test_download_rechecks_grants_even_with_cached_bytes(self):
        current = self.data(self.prefix + '/records/recOne')
        url = current['attachments'][0]['download_url']
        self.assertEqual(self.client.get(url).content, b'Alpha policy service steps')
        original = self.bridge_service._content_path
        def revoke(*args, **kwargs):
            path = original(*args, **kwargs)
            self.profile['scopes'] = ['B']
            return path
        with patch.object(self.bridge_service, '_content_path', side_effect=revoke):
            response = self.client.get(url)
        self.assertEqual(response.status_code, 404)
        self.assertNotIn('Alpha policy service steps', response.text)

    def test_download_rejects_configuration_change_during_byte_read(self):
        record = self.data(self.prefix + '/records/recOne')
        original = self.bridge_service._content_path
        def remove_attachment_field(*args, **kwargs):
            path = original(*args, **kwargs)
            source = self.service._table('manuals')
            self.service.save_table(self.admin, configuration(attachment_fields=[],
                expected_revision=source['revision']), key='manuals')
            return path
        with patch.object(self.bridge_service, '_content_path', side_effect=remove_attachment_field):
            response = self.client.get(record['attachments'][0]['download_url'])
        self.assertEqual(response.status_code, 409, response.text)
        self.assertNotIn('Alpha policy service steps', response.text)

    def test_attachment_selection_does_not_revoke_authorized_historical_download(self):
        versions = self.data(self.prefix + '/records/recOne/versions')
        url = versions['items'][0]['attachments'][0]['download_url']
        source = self.service._table('manuals')
        self.service.save_table(self.admin, configuration(attachment_fields=[],
            expected_revision=source['revision']), key='manuals')
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.content, b'Alpha policy service steps')

    def test_vector_query_rechecks_revocation_after_embedding(self):
        self.data('/api/document-library/tables')
        original = self.kb.embedder.embed
        def revoke(*args, **kwargs):
            result = original(*args, **kwargs)
            if kwargs.get('query'):
                self.profile['scopes'] = []
            return result
        with patch.object(self.kb.embedder, 'embed', side_effect=revoke) as embedding:
            result = self.kb.search(actor(), 'Alpha policy')
        self.assertTrue(any(call.kwargs.get('query') for call in embedding.call_args_list))
        self.assertEqual(result['items'], [])

    def test_service_validation_errors_are_localized(self):
        from lan_bitable_template_portal.document_library import validate_source
        for changes, message in (({'write_scopes': ['C']}, '编辑范围不能超出读取范围。'),
                ({'read_scopes': ['unknown']}, '楼栋范围配置无效。'),
                ({'module': 'assistant'}, '资料库仅使用共享知识库权限。')):
            with self.subTest(changes=changes), self.assertRaises(AssistantError) as error:
                validate_source(configuration(**changes))
            self.assertEqual(str(error.exception), message)

    def test_revoked_context_and_generated_answer_never_escape_assistant(self):
        from openclaw_service.assistant.lighthouse_knowledge_answer import answer_company
        self.data('/api/document-library/tables')
        for stage in ('before_generation', 'after_generation'):
            with self.subTest(stage=stage):
                self.profile['scopes'] = ['A']
                checks, emitted = [], []
                def complete(*args, **kwargs):
                    self.profile['scopes'] = ['B']
                    return 'Protected alpha answer [1]'
                model = SimpleNamespace(complete=Mock(side_effect=complete))
                engine = SimpleNamespace(portal=SimpleNamespace(get_knowledge=lambda: self.kb),
                    assistant=SimpleNamespace(model_for=Mock(return_value=model)))
                async def authorize():
                    checks.append(True)
                    if stage == 'before_generation' and len(checks) == 2:
                        self.profile['scopes'] = ['B']
                    return self.bridge_service.refresh_actor(actor())
                async def emit(kind, data):
                    emitted.append((kind, data))
                async def answer():
                    return await answer_company(engine, actor(),
                        {'_profile': {'endpoint': 'https://company.example/v1'}},
                        'Alpha policy', emit, authorize)
                result = self.client.portal.call(answer)
                self.assertEqual(result['sources'], [])
                self.assertNotIn('Protected alpha answer', json.dumps([result, emitted]))
                if stage == 'before_generation':
                    model.complete.assert_not_called()
                else:
                    model.complete.assert_called_once()

    def test_config_response_rechecks_admin_revocation(self):
        self.profile['role'] = 'admin'
        self.data('/api/document-library/tables')
        original = self.bridge_service.save_table
        def revoke(*args, **kwargs):
            result = original(*args, **kwargs)
            self.profile['role'] = 'building'
            return result
        with patch.object(self.bridge_service, 'save_table', side_effect=revoke):
            response = self.client.post('/api/document-library/tables', headers=self.origin,
                json=configuration(id='other', table_id='tblOther'))
        self.assertEqual(response.status_code, 403)
        self.assertNotIn('baseFixture123', response.text)

    def test_frontend_contract_preserves_native_values_receipts_and_versions(self):
        schema = self.data(self.prefix + '/schema')
        self.assertEqual(schema['display_fields'], ['fldTitle', 'fldScope', 'fldNumber'])
        self.assertEqual(schema['attachment_fields'], ['fldFiles'])
        self.assertTrue(all('field_id' in field and 'field_name' in field and 'read_only' in field for field in schema['fields']))
        record = self.data(self.prefix + '/records/recOne')
        payload = {'fields': {'fldNumber': 19, 'fldTitle': 'Updated policy'},
            'operation_id': 'fixture-contract-write', 'reason': 'fixture edit', 'expected_revision': record['revision']}
        response = self.client.patch(self.prefix + '/records/recOne', headers=self.origin, json=payload)
        self.assertEqual(response.status_code, 200, response.text)
        saved = response.json()['data']
        self.assertEqual(saved['operation_id'], payload['operation_id'])
        self.assertEqual(saved['fields']['fldNumber'], 19)
        self.assertNotEqual(saved['revision'], record['revision'])
        response = self.client.post(self.prefix + '/records/recOne/attachments', headers=self.origin,
            data={'field_id': 'fldFiles', 'expected_revision': saved['revision'],
                'operation_id': 'fixture-contract-upload', 'reason': 'fixture upload'},
            files={'file': ('new.txt', b'New offline document', 'text/plain')})
        self.assertEqual(response.status_code, 200, response.text)
        uploaded = response.json()['data']
        self.assertEqual(len(uploaded['attachments']), 2)
        self.assertTrue(all(file['attachment_id'] and file['field_id'] for file in uploaded['attachments']))
        versions = self.data(self.prefix + '/records/recOne/versions')
        self.assertEqual(versions['revision'], uploaded['revision'])
        self.assertEqual(versions['items'][0]['created_by'], 'Fixture reader')
        self.assertTrue(all(isinstance(item['number'], int) for item in versions['items']))
        self.assertNotIn('file_token', json.dumps(versions))

    def test_localized_invalid_request_and_unavailable_errors_are_not_cacheable(self):
        for body, message in (('[1]', '请求内容必须是对象。'), ('{', '资料库请求参数无效。')):
            response = self.client.patch(self.prefix + '/records/recOne', headers=self.origin, content=body)
            self.assertEqual(response.status_code, 400)
            self.assertEqual(response.json()['error'], message)
            self.assertEqual(response.headers['cache-control'], 'no-store')
        self.data('/api/document-library/tables')
        with patch.object(self.bridge_service, 'schema', side_effect=OSError('fixture private detail')):
            response = self.client.get(self.prefix + '/schema')
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()['error'], '资料库暂时不可用，系统不会自动重复提交。')
        self.assertEqual(response.headers['cache-control'], 'no-store')


class DocumentLibraryWorkerTests(LibraryFixture):
    def test_worker_startup_indexes_portal_database_without_assistant_requests(self):
        from openclaw_service.assistant.routes import install_assistant_routes
        workers, libraries = [], []
        def build_knowledge(root):
            kb = KnowledgeBase(root, embedder=FakeEmbedder(), memory_index_factory=FakeMemoryIndex)
            workers.append(kb)
            return kb
        def build_library(root):
            service = DocumentLibraryService(root, remote_factory=lambda _: self.remote)
            libraries.append(service)
            return service
        source = self.service._table('manuals')
        self.service.save_table(self.admin, configuration(name='Worker fixture',
            expected_revision=source['revision']), key='manuals')
        host = SimpleNamespace(state=Path(self.temp) / 'lighthouse_openclaw')
        with patch.dict(os.environ, {'LIGHTHOUSE_AGENT_ENGINE': 'legacy'}), \
                patch('lan_bitable_template_portal.document_library.DocumentLibraryService', side_effect=build_library), \
                patch('openclaw_service.assistant.lighthouse_knowledge.KnowledgeBase', side_effect=build_knowledge), \
                patch('openclaw_service.assistant.routes.LighthouseAssistant', side_effect=AssertionError('No real model')):
            hooks = install_assistant_routes(FastAPI(), host)
            async def exercise():
                try:
                    self.enterContext(patch('socket.socket.connect', side_effect=AssertionError('No network')))
                    await hooks['startup']()
                    deadline = time.monotonic() + 10
                    while time.monotonic() < deadline:
                        if workers and libraries and workers[0].search(actor(), 'Alpha policy')['items']:
                            break
                        await asyncio.sleep(.02)
                    else:
                        self.fail('Startup worker did not index the portal source')
                    self.assertEqual(libraries[0].db_path, self.service.db_path)
                    self.assertTrue(workers[0]._worker.is_alive())
                    self.assertTrue(workers[0].source_acl)
                    self.assertTrue(workers[0].source_current)
                finally:
                    await hooks['shutdown']()
            asyncio.run(exercise())
        self.assertFalse(workers[0]._worker.is_alive())


if __name__ == '__main__':
    unittest.main()
