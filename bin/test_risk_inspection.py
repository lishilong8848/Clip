import copy
from concurrent.futures import ThreadPoolExecutor
import io
import json
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest
import uuid
import httpx
from unittest.mock import Mock, patch
from types import SimpleNamespace
from fastapi import FastAPI
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parent))

from bin.lan_bitable_template_portal.risk_inspection import RiskInspections, RiskError, schema_fields, validate_values
from bin.lan_bitable_template_portal.risk_inspection_cloud import CloudRejected, RiskCloud, TEMPLATE_APP, field_spec
from upload_event_module.services.http_client import FeishuHttpClient


def actor(identity, level='L4', scope='A'):
    return {'id': identity, 'name': identity, 'open_id': 'ou_' + identity, 'level': level,
            'role': 'admin' if level in {'L0', 'L1'} else 'building', 'modules': ['risk_inspection'], 'scope_rules': [scope]}


class RiskTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.people = {'admin': actor('admin', 'L0', 'ALL'), 'reviewer': actor('reviewer', 'L3'),
                       'alice': actor('alice'), 'bob': actor('bob'), 'other': actor('other', scope='B')}
        self.store = RiskInspections(Path(self.temp.name) / 'state.sqlite3', lambda p: self.people[p])
        self.fields = schema_fields([{'field_id': 'primary', 'field_name': '排查任务编号', 'type': 1, 'is_primary': True},
            {'field_id': 'result', 'field_name': '排查结果', 'type': 1}, {'field_id': 'photo', 'field_name': '现场附件', 'type': 17},
            {'field_id': 'check', 'field_name': '正常', 'type': 7},
            {'field_id': 'select', 'field_name': '等级', 'type': 3, 'property': {'options': [{'name': '正常'}, {'name': '异常'}]}}])
        self.template = {'fields': self.fields, 'version': 'v1'}
        self.body = {'id': 'a'*32, 'name': '电气风险排查', 'deadline': (time.time()+3600)*1000,
                     'reviewer_id': 'reviewer', 'template_version': 'v1', 'people': [{'id': 'alice', 'scope': 'A'}, {'id': 'bob', 'scope': 'A'}],
                     'items': [{'title': '检查配电设备', 'values': {}}, {'title': '检查环境', 'values': {}}]}

    def publish(self):
        result = self.store.publish(self.people['admin'], self.body, self.template)
        self.store.activate(result['id'])
        with self.store.connect(True) as db:
            db.execute("UPDATE risk_jobs SET status='done' WHERE kind='publish'")
        return self.store.get(self.people['admin'], result['id'])

    def act(self, who, action, *, pid='alice', version=None, values=None, reason=''):
        data = self.store.get(self.people[who], self.body['id'])
        person = next(p for p in data['people'] if p['id'] == pid)
        return self.store.action(self.people[who], self.body['id'], pid, {'action': action, 'version': person['version'] if version is None else version,
                  'item_id': person['items'][0]['id'], 'values': values if values is not None else {'result': '无异常'}, 'reason': reason})

    def test_publish_snapshot_idempotency_and_restart(self):
        first = self.store.publish(self.people['admin'], self.body, self.template)
        self.fields[1]['field_name'] = '修改后的模板'
        again = self.store.publish(self.people['admin'], self.body, self.template)
        self.assertEqual(first['fields'][1]['field_name'], again['fields'][1]['field_name'])
        restarted = RiskInspections(self.store.db_path, self.store.person_for)
        self.assertEqual(restarted.list(self.people['alice'])['total'], 1)
        with self.store.connect() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM risk_jobs WHERE kind='publish'").fetchone()[0], 1)

    def test_changed_idempotency_payload_rejected(self):
        self.publish()
        self.body['name'] = '不同任务'
        with self.assertRaises(RiskError):
            self.store.publish(self.people['admin'], self.body, self.template)

    def test_building_filter_applies_to_list_totals_and_visible_people(self):
        self.publish()
        for identity, recipients in [('b' * 32, [{'id': 'other', 'scope': 'B'}]),
                                     ('c' * 32, [{'id': 'alice', 'scope': 'A'}, {'id': 'other', 'scope': 'B'}])]:
            body = {**self.body, 'id': identity, 'reviewer_id': 'admin', 'people': recipients}
            self.store.publish(self.people['admin'], body, self.template)
            self.store.activate(identity)
        a = self.store.list(self.people['admin'], scope='A')
        b = self.store.list(self.people['admin'], scope='B')
        self.assertEqual({row['id'] for row in a['items']}, {'a' * 32, 'c' * 32})
        self.assertEqual({row['id'] for row in b['items']}, {'b' * 32, 'c' * 32})
        self.assertEqual((a['total'], b['total']), (2, 2))
        self.assertTrue(all(row['people_count'] == 1 and row['total'] == 2 for row in b['items']))
        self.assertEqual(self.store.list(self.people['admin'], scope='ALL')['total'], 3)
        self.assertEqual(self.store.list(self.people['alice'], scope='A')['total'], 2)
        with self.assertRaises(RiskError) as error:
            self.store.list(self.people['alice'], scope='B')
        self.assertEqual(error.exception.status, 403)

    def test_schema_and_scopes_required(self):
        for change in ({'template_version':'old'}, {'people':[{'id':'other','scope':'A'}]}, {'reviewer_id':'alice'}):
            with self.subTest(change=change), self.assertRaises(RiskError):
                self.store.publish(self.people['admin'], {**self.body, **change}, self.template)

    def test_cannot_assign_reviewer_to_self(self):
        self.body['people'] = [{'id':'reviewer','scope':'A'}]
        with self.assertRaises(RiskError):
            self.store.publish(self.people['admin'], self.body, self.template)

    def test_person_isolation_no_other_results(self):
        self.publish()
        self.act('bob','submit',pid='bob',values={'result':'私有反馈'})
        alice = self.store.get(self.people['alice'], self.body['id'])
        self.assertEqual([p['id'] for p in alice['people']], ['alice'])
        self.assertNotIn('私有反馈', json.dumps(alice, ensure_ascii=False))
        with self.assertRaises(RiskError):
            self.store.get(self.people['other'], self.body['id'])

    def test_submit_return_resubmit_approve(self):
        self.publish()
        self.act('alice','submit')
        with self.assertRaises(RiskError):
            self.act('alice','save')
        with self.assertRaises(RiskError):
            self.act('reviewer','return')
        self.act('reviewer','return',reason='请补充照片')
        self.act('alice','submit',values={'result':'已再次检查'})
        data = self.act('reviewer','approve',reason='核实完成')
        self.assertEqual(data['people'][0]['items'][0]['status'], 'approved')
        self.assertEqual(len(data['audit']), 5)

    def test_validation_failure_never_commits(self):
        self.publish()
        with self.assertRaises(RiskError):
            self.act('alice','submit',values={})
        data = self.store.get(self.people['alice'], self.body['id'])
        self.assertEqual(data['people'][0]['version'],1)

    def test_whitespace_is_not_a_required_answer(self):
        self.publish()
        with self.assertRaisesRegex(RiskError, '排查结果'):
            self.act('alice', 'submit', values={'result': ' \n\t\u3000'})
        self.assertEqual(self.store.get(self.people['alice'], self.body['id'])['people'][0]['version'], 1)

    def test_optional_field_can_be_saved_submitted_and_approved_empty(self):
        self.fields = schema_fields(self.fields, {'result': {'required': False}})
        self.template['fields'] = self.fields
        self.assertFalse(self.fields[1]['required'])
        self.publish()
        self.fields[1]['required'] = True
        self.act('alice', 'save', values={})
        self.act('alice', 'submit', values={})
        approved = self.act('reviewer', 'approve', values={})
        self.assertFalse(approved['fields'][1]['required'])
        self.assertEqual(approved['people'][0]['items'][0]['status'], 'approved')

    def test_required_snapshot_still_blocks_empty_submit_after_template_change(self):
        self.publish()
        self.fields[1]['required'] = False
        saved = self.act('alice', 'save', values={})
        self.assertTrue(saved['fields'][1]['required'])
        with self.assertRaisesRegex(RiskError, '排查结果'):
            self.act('alice', 'submit', values={})

    def test_required_select_without_options_cannot_publish(self):
        self.fields[-1].update(required=True, property={'options': []})
        with self.assertRaisesRegex(RiskError, '选项'):
            self.store.publish(self.people['admin'], self.body, self.template)
        self.assertEqual(self.store.list(self.people['admin'])['total'], 0)

    def test_invalid_system_field_cannot_start_cloud_publication(self):
        self.fields[0]['type'] = 2
        with self.assertRaises(RiskError):
            self.store.publish(self.people['admin'], self.body, self.template)

    def test_resubmission_clears_old_verdict_but_keeps_audit(self):
        self.publish()
        self.act('alice', 'submit')
        self.act('reviewer', 'return', reason='补充设备编号')
        data = self.act('alice', 'submit', values={'result': '设备A-001正常'})
        item = data['people'][0]['items'][0]
        self.assertNotIn('reviewed_at', item)
        self.assertFalse(item.get('reason'))
        self.assertTrue(any('补充设备编号' in row['data'] for row in data['audit']))

    def test_old_worker_cannot_finish_reclaimed_job(self):
        self.store.publish(self.people['admin'], self.body, self.template)
        first = self.store.claim_job()
        with self.store.connect(True) as db:
            db.execute('UPDATE risk_jobs SET lease=0 WHERE id=?', (first['id'],))
        second = self.store.claim_job()
        self.store.finish_job(first)
        with self.store.connect() as db:
            self.assertEqual(db.execute('SELECT status FROM risk_jobs WHERE id=?', (first['id'],)).fetchone()[0], 'running')
        self.store.finish_job(second)
        with self.store.connect() as db:
            self.assertEqual(db.execute('SELECT status FROM risk_jobs WHERE id=?', (first['id'],)).fetchone()[0], 'done')

    def test_worker_renews_lease_and_rejects_lost_ownership(self):
        self.store.publish(self.people['admin'], self.body, self.template)
        job = self.store.claim_job()
        self.assertEqual(job['status'], 'running')
        old = job['lease']
        self.store.renew_job(job)
        self.assertGreater(job['lease'], old)
        with self.store.connect(True) as db:
            db.execute('UPDATE risk_jobs SET lease=0 WHERE id=?', (job['id'],))
        with self.assertRaises(RiskError):
            self.store.renew_job(job)

    def test_only_one_cloud_worker_owns_work_at_a_time(self):
        self.publish()
        with self.store.connect(True) as db:
            db.execute("UPDATE risk_jobs SET status='done' WHERE kind='template_sync'")
        first = self.store.claim_job()
        self.assertIsNone(self.store.claim_job())
        self.store.finish_job(first)
        self.assertIsNotNone(self.store.claim_job())

    def test_reassign_reviewer_requeues_pending_reviews_without_changing_answers(self):
        self.publish(); self.act('alice', 'submit')
        before = self.store.raw(self.body['id'])[1]
        self.people['reviewer']['level'] = 'L4'
        data = self.store.change_reviewer(self.people['admin'], self.body['id'], {'reviewer_id': 'admin', 'version': 1})
        self.assertEqual(data['reviewer'], 'admin')
        self.assertTrue(data['is_reviewer'])
        self.assertEqual(self.store.raw(self.body['id'])[1], before)
        with self.assertRaises(RiskError):
            self.store.change_reviewer(self.people['admin'], self.body['id'], {'reviewer_id': 'alice', 'version': 1})
        data = self.act('admin', 'approve')
        self.assertEqual(data['people'][0]['items'][0]['status'], 'approved')

    def test_reviewer_reassignment_rejects_assignee_and_partial_scope_manager(self):
        self.people['alice']['level'] = 'L3'
        self.publish()
        with self.assertRaises(RiskError):
            self.store.change_reviewer(self.people['admin'], self.body['id'], {'reviewer_id': 'alice', 'version': 1})
        self.people['reviewer']['scope_rules'] = ['ALL']
        self.people['superA'] = actor('superA', 'L3')
        self.body['id'] = 'b'*32
        self.body['people'].append({'id': 'other', 'scope': 'B'})
        self.publish()
        with self.assertRaises(RiskError):
            self.store.change_reviewer(self.people['superA'], self.body['id'], {'reviewer_id': 'reviewer', 'version': 1})

    def test_same_person_concurrent_submit_once(self):
        self.publish()
        def submit(_):
            try:
                self.act('alice','submit',version=1)
                return True
            except RiskError:
                return False
        with ThreadPoolExecutor(2) as executor:
            self.assertEqual(sum(executor.map(submit, range(2))), 1)
        with self.store.connect() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM risk_jobs WHERE kind='sync'").fetchone()[0],1)

    def test_manager_cannot_impersonate_assignee_or_reviewer(self):
        self.publish()
        with self.assertRaises(RiskError):
            self.act('admin','submit')
        self.act('alice','submit')
        with self.assertRaises(RiskError):
            self.act('admin','approve')

    def test_revoke_person_retains_audit_excludes_denominator(self):
        self.publish()
        self.act('alice','submit')
        data = self.act('admin','revoke_person',reason='本次无需参与')
        self.assertTrue(data['people'][0]['revoked'])
        self.assertEqual(self.store.list(self.people['admin'])['items'][0]['total'],2)
        with self.assertRaises(RiskError):
            self.act('alice','save')

    def test_revoke_single_item_only(self):
        self.publish()
        data = self.act('admin','revoke_item',reason='已调整范围')
        self.assertEqual([i['status'] for i in data['people'][0]['items']], ['revoked','pending'])

    def test_type_validation_and_cross_person_files(self):
        for values in ({'primary':'fake'}, {'result':42}, {'check':'yes'}, {'select':'未知'}, {'photo':['f'*32]}):
            with self.subTest(values=values),self.assertRaises(RiskError):
                validate_values(self.fields,values,file_check=lambda f,i:False)
        self.assertEqual(validate_values(self.fields,{'check':False}),{'check':False})

    def test_failed_earlier_sync_blocks_newer_same_person(self):
        self.publish()
        self.act('alice','submit')
        self.act('reviewer','return',reason='补充')
        with self.store.connect(True) as db:
            db.execute("UPDATE risk_jobs SET status='done' WHERE kind IN ('notify','review_notice','template_sync')")
            first = db.execute("SELECT id FROM risk_jobs WHERE kind='sync' ORDER BY created LIMIT 1").fetchone()[0]
            db.execute("UPDATE risk_jobs SET status='failed' WHERE id=?", (first,))
        self.assertIsNone(self.store.claim_job())
        self.store.retry(self.people['admin'],self.body['id'])
        self.assertEqual(self.store.claim_job()['id'],first)

    def test_stale_lease_recovers_same_job(self):
        self.store.publish(self.people['admin'],self.body,self.template)
        job = self.store.claim_job()
        self.assertIsNone(self.store.claim_job())
        with self.store.connect(True) as db:
            db.execute('UPDATE risk_jobs SET lease=0 WHERE id=?',(job['id'],))
        self.assertEqual(self.store.claim_job()['id'],job['id'])

    def test_live_revocation_blocks_old_session(self):
        self.publish()
        self.people['alice']['modules']=[]
        with self.assertRaises(RiskError):
            self.store.get(self.people['alice'],self.body['id'])

    def test_reviewer_demotion_blocks_approval(self):
        self.publish();self.act('alice','submit')
        self.people['reviewer']['level']='L4'
        with self.assertRaises(RiskError):self.act('reviewer','approve')

    def test_demoted_reviewer_cannot_keep_reading_others_feedback(self):
        self.publish(); self.act('alice', 'submit')
        self.people['reviewer']['level'] = 'L4'
        with self.assertRaises(RiskError):
            self.store.get(self.people['reviewer'], self.body['id'])

    def test_other_users_audit_volume_does_not_hide_own_history(self):
        self.publish(); self.act('alice', 'submit')
        with self.store.connect(True) as db:
            for _ in range(110):
                self.store._audit(db, self.body['id'], 'bob', self.people['bob'], 'save', {})
        data = self.store.get(self.people['alice'], self.body['id'])
        self.assertEqual([row['action'] for row in data['audit']], ['submit'])

    def test_summary_does_not_load_full_feedback_attachments_and_audit(self):
        self.publish(); self.act('alice', 'submit')
        with patch.object(self.store, 'get', side_effect=AssertionError('summary must not load every detailed form')):
            data = self.store.list(self.people['alice'])
        self.assertEqual(data['items'][0]['total'], 2)
        self.assertEqual(data['items'][0]['submitted'], 1)
        self.assertEqual(data['items'][0]['people_count'], 1)

    def test_sync_order_uses_versions_even_if_clock_moves_backward(self):
        self.publish();self.act('alice','submit');self.act('reviewer','return',reason='补充')
        with self.store.connect(True) as db:
            db.execute("UPDATE risk_jobs SET status='done' WHERE kind!='sync'")
            first=db.execute("SELECT id FROM risk_jobs WHERE kind='sync' ORDER BY created LIMIT 1").fetchone()[0]
            db.execute('UPDATE risk_jobs SET created=? WHERE id=?',(time.time()+3600,first))
        self.assertEqual(self.store.claim_job()['id'],first)

    def test_local_supervisor_cannot_read_other_building_audit_or_table_link(self):
        self.people['reviewer']['scope_rules']=['ALL']
        self.people['admin']=actor('admin','L1','ALL')
        self.people['superA']=actor('superA','L0')
        self.body['people'].append({'id':'other','scope':'B'})
        self.publish();self.act('other','submit',pid='other',values={'result':'B楼独立反馈'})
        self.store.checkpoint(self.body['id'],url='https://example.test/all-buildings')
        data=self.store.get(self.people['superA'],self.body['id'])
        self.assertNotIn('B楼独立反馈',json.dumps(data,ensure_ascii=False))
        self.assertEqual(data['url'],'')

    def test_job_errors_and_review_notifications_follow_subject_scope(self):
        self.people['reviewer']['scope_rules'] = ['ALL']
        self.people['admin'] = actor('admin', 'L1', 'ALL')
        self.people['superA'] = actor('superA', 'L0')
        self.body['people'].append({'id': 'other', 'scope': 'B'})
        self.publish()
        self.act('other', 'submit', pid='other')
        with self.store.connect(True) as db:
            db.execute("UPDATE risk_jobs SET error='B楼人员隐私错误' WHERE json_extract(payload,'$.person_id')='other' OR json_extract(payload,'$.subject_id')='other'")
        self.assertNotIn('B楼人员隐私错误', json.dumps(self.store.get(self.people['superA'], self.body['id']), ensure_ascii=False))


class CloudTests(unittest.TestCase):
    setUp = RiskTests.setUp
    publish = RiskTests.publish
    act = RiskTests.act
    def remote(self):
        cloud = RiskCloud(self.store,Path(self.temp.name)/'not-configured',lambda p:'on_'+p['id'],lambda:'https://example.test')
        self.addCleanup(cloud.close)
        return cloud

    def test_unified_transport_preserves_token_cache_search_and_multipart(self):
        calls = []
        def handle(request):
            calls.append(request)
            if request.url.path.endswith('/tenant_access_token/internal'):
                return httpx.Response(200, json={'code': 0, 'tenant_access_token': 'fixture-token', 'expire': 7200})
            return httpx.Response(200, json={'code': 0, 'data': {'file_token': 'fixture-file'}})
        remote = self.remote()
        self.assertIsInstance(remote.http, FeishuHttpClient)
        self.assertEqual(remote.http.retries, 0)
        remote.http = FeishuHttpClient(timeout=remote.http.timeout, transport=httpx.MockTransport(handle))
        remote.config_path.write_text(json.dumps({'app_id': 'cli_aa42dc770a381cb3', 'secret_cipher': 'fixture-cipher'}))
        with patch('openclaw_service.assistant.lighthouse_ai.unprotect_key', return_value='fixture-secret'), \
                patch('bin.lan_bitable_template_portal.portal_service.external_real_write_guard', return_value={'real_write_allowed': True}):
            remote.request('POST', 'bitable/v1/apps/test/tables/test/records/search', params={'page_token': 'page-2'}, json={'field_names': ['员工姓名']})
            result = remote.request('POST', 'drive/v1/medias/upload_all', data={'size': '7'}, files={'file': ('note.txt', io.BytesIO(b'fixture'), 'text/plain')})
        self.assertEqual(len(calls), 3, 'token is reused for both business requests')
        self.assertEqual(json.loads(calls[0].content)['app_secret'], 'fixture-secret')
        self.assertEqual(calls[0].extensions['timeout']['read'], 25)
        self.assertEqual(calls[1].extensions['timeout']['read'], 30)
        self.assertEqual(calls[1].url.params['page_token'], 'page-2')
        self.assertEqual(json.loads(calls[1].content)['field_names'], ['员工姓名'])
        self.assertEqual(calls[2].headers['authorization'], 'Bearer fixture-token')
        self.assertIn('multipart/form-data', calls[2].headers['content-type'])
        self.assertIn(b'fixture', calls[2].content)
        self.assertEqual(result['file_token'], 'fixture-file')

    def test_transport_failure_retains_create_checkpoint_without_automatic_retry(self):
        for failure in ('timeout', 'server_error'):
            with self.subTest(failure=failure):
                self.body['id'] = ('a' if failure == 'timeout' else 'b') * 32
                self.store.publish(self.people['admin'], self.body, self.template)
                calls = []
                def handle(request):
                    calls.append(request)
                    if failure == 'timeout':
                        raise httpx.ReadTimeout('fixture timeout', request=request)
                    return httpx.Response(503, json={})
                remote = self.remote()
                remote.http = FeishuHttpClient(transport=httpx.MockTransport(handle), retries=3)
                remote.headers = Mock(return_value={})
                remote.all = Mock(return_value=[])
                with patch('bin.lan_bitable_template_portal.portal_service.external_real_write_guard', return_value={'real_write_allowed': True}):
                    with self.assertRaises(httpx.HTTPError):
                        remote.create_release(*self.store.raw(self.body['id']))
                    self.assertTrue(self.store.raw(self.body['id'])[0]['cloud']['create_attempted'])
                    with self.assertRaises(RiskError):
                        remote.create_release(*self.store.raw(self.body['id']))
                self.assertEqual(len(calls), 1, 'uncertain creation must not be sent twice')

    def test_isolated_write_is_blocked_before_transport_or_credentials(self):
        remote = self.remote()
        remote.headers = Mock()
        remote.http.request = Mock()
        with patch('bin.lan_bitable_template_portal.portal_service.external_real_write_guard', return_value={'real_write_allowed': False}):
            with self.assertRaises(CloudRejected):
                remote.request('POST', 'im/v1/messages', json={})
        remote.headers.assert_not_called()
        remote.http.request.assert_not_called()

    def test_cloud_uses_correct_field_name_and_attachment_column(self):
        self.publish()
        release, people = self.store.raw(self.body['id'])
        release['cloud']={'app':'app','table':'tbl'}
        people[0]['items'][0].update(status='submitted',values={'result':'检查反馈','photo':['f'*32]})
        remote=self.remote()
        remote.attachment=Mock(return_value={'file_token':'file'})
        remote.upsert=Mock()
        remote.sync(release,people[0])
        fields=remote.upsert.call_args.args[3]
        self.assertEqual(fields['排查结果'],'检查反馈')
        self.assertEqual(fields['现场附件'],[{'file_token':'file'}])
        self.assertEqual(fields['灯塔人员标识'],'alice')

    def test_create_timeout_does_not_create_second_table(self):
        self.store.publish(self.people['admin'],self.body,self.template)
        remote=self.remote()
        remote.all=Mock(return_value=[])
        remote.request=Mock(side_effect=TimeoutError())
        with self.assertRaises(TimeoutError):
            remote.create_release(*self.store.raw(self.body['id']))
        with self.assertRaises(RiskError):
            remote.create_release(*self.store.raw(self.body['id']))
        self.assertEqual(sum(call.args[0]=='POST' for call in remote.request.call_args_list),1)

    def test_publish_creates_table_inside_template_base_not_a_wiki_node(self):
        self.store.publish(self.people['admin'],self.body,self.template)
        remote=self.remote()
        remote.all=Mock(return_value=[])
        remote.request=Mock(return_value={'table_id':'tblnew','default_view_id':'viewnew'})
        remote.ensure_fields=Mock()
        remote.registry=Mock()
        remote.create_release(*self.store.raw(self.body['id']))
        call=remote.request.call_args
        self.assertEqual(call.args,('POST',f'bitable/v1/apps/{TEMPLATE_APP}/tables'))
        self.assertEqual(call.kwargs['json']['table']['fields'][0]['field_name'],'排查任务编号')
        data=self.store.raw(self.body['id'])[0]
        self.assertEqual(data['cloud']['app'],TEMPLATE_APP)
        self.assertIn('?table=tblnew&view=viewnew',data['cloud']['url'])

    def test_create_response_lost_recovers_existing_table(self):
        self.store.publish(self.people['admin'],self.body,self.template)
        self.store.checkpoint(self.body['id'],create_attempted=True)
        remote=self.remote()
        remote.all=Mock(return_value=[{'table_id':'tblrecovered','name':self.body['name']+' · '+self.body['id'][:12]}])
        remote.request=Mock()
        remote.ensure_fields=Mock()
        remote.registry=Mock()
        remote.create_release(*self.store.raw(self.body['id']))
        remote.request.assert_not_called()
        self.assertEqual(self.store.raw(self.body['id'])[0]['cloud']['table'],'tblrecovered')

    def test_same_named_result_table_is_not_adopted_without_a_creation_attempt(self):
        self.store.publish(self.people['admin'], self.body, self.template)
        remote = self.remote(); remote.request = Mock(); remote.ensure_fields = Mock(); remote.registry = Mock()
        remote.all = Mock(return_value=[{'table_id': 'unrelated', 'name': self.body['name']+' · '+self.body['id'][:12]}])
        with self.assertRaises(RiskError):
            remote.create_release(*self.store.raw(self.body['id']))
        remote.ensure_fields.assert_not_called()

    def test_incomplete_create_response_keeps_uncertain_marker(self):
        self.store.publish(self.people['admin'],self.body,self.template)
        remote=self.remote();remote.all=Mock(return_value=[])
        remote.request=Mock(side_effect=RiskError('response incomplete',503))
        with self.assertRaises(RiskError):remote.create_release(*self.store.raw(self.body['id']))
        self.assertTrue(self.store.raw(self.body['id'])[0]['cloud']['create_attempted'])

    def test_definitive_rejection_can_retry_creation(self):
        self.store.publish(self.people['admin'],self.body,self.template)
        remote=self.remote();remote.all=Mock(return_value=[])
        remote.request=Mock(side_effect=CloudRejected('permission denied',503))
        with self.assertRaises(RiskError):remote.create_release(*self.store.raw(self.body['id']))
        self.assertFalse(self.store.raw(self.body['id'])[0]['cloud']['create_attempted'])

    def test_http_permission_rejection_can_retry_but_server_error_stays_uncertain(self):
        for status, uncertain in ((403, False), (503, True)):
            with self.subTest(status=status):
                self.body['id'] = f'{status:032x}'
                self.store.publish(self.people['admin'], self.body, self.template)
                remote = self.remote(); remote.all = Mock(return_value=[]); remote.headers = Mock(return_value={})
                response = httpx.Response(status, json={}, request=httpx.Request('POST', 'https://fixture.test/tables'))
                remote.http.request = Mock(return_value=response)
                with patch('bin.lan_bitable_template_portal.portal_service.external_real_write_guard', return_value={'real_write_allowed': True}):
                    with self.assertRaises((RiskError, httpx.HTTPStatusError)):
                        remote.create_release(*self.store.raw(self.body['id']))
                self.assertEqual(self.store.raw(self.body['id'])[0]['cloud']['create_attempted'], uncertain)

    def test_registry_keeps_fraction_not_percent(self):
        self.publish()
        release, people=self.store.raw(self.body['id'])
        release['cloud']={'url':'https://vnet.feishu.cn/wiki/test'}
        people[0]['items'][0]['status']='approved'
        remote=self.remote()
        remote.ensure_fields=Mock()
        remote.upsert=Mock()
        remote.registry(release,people)
        fields=remote.upsert.call_args.args[3]
        self.assertEqual(fields['任务完成情况'],.25)
        self.assertEqual(fields['现场排查人'],[{'id':'on_alice'},{'id':'on_bob'}])

    def test_no_message_id_is_not_success(self):
        self.publish()
        release,_=self.store.raw(self.body['id'])
        remote=self.remote()
        remote.request=Mock(return_value={})
        with self.assertRaises(RiskError):
            remote.notify({'id':'job'},release,self.people['alice'])
        self.assertNotIn('message_id',self.store.raw(release['id'])[0]['cloud']['messages']['job'])

    def test_paginated_list_never_accepts_incomplete_data(self):
        remote=self.remote()
        remote.request=Mock(return_value={'items':[],'has_more':True,'page_token':'same'})
        with self.assertRaises(RiskError):
            remote.all('example')

    def test_option_ids_not_copied_between_tables(self):
        spec=field_spec({'field_name':'等级','type':3,'property':{'options':[{'id':'old','name':'高'}]}})
        self.assertEqual(spec['property'],{'options':[{'name':'高'}]})

    def test_replayed_sync_updates_same_record(self):
        remote=self.remote()
        remote.request=Mock(side_effect=[{'items':[{'record_id':'record','fields':{'灯塔排查标识':'key'}}],'has_more':False},{'record':{'record_id':'record'}}])
        self.assertEqual(remote.upsert('app','table','key',{'排查结果':'更正'}),'record')
        self.assertEqual(remote.request.call_args_list[-1].args[0],'PUT')

    def test_duplicate_cloud_records_are_not_overwritten(self):
        remote=self.remote()
        remote.request=Mock(return_value={'items':[{'record_id':'one','fields':{'灯塔排查标识':'key'}},{'record_id':'two','fields':{'灯塔排查标识':'key'}}],'has_more':False})
        with self.assertRaises(RiskError):remote.upsert('app','table','key',{})
        self.assertEqual(remote.request.call_count,1)

    def test_personnel_identity_uses_agent_union_id_and_caches_only_minimal_fields(self):
        remote=self.remote()
        remote.request=Mock(return_value={'items':[{'record_id':'recOne','fields':{'员工姓名':[{'id':'on_exact'}]}}],'has_more':False})
        for _ in range(2):self.assertEqual(remote.identity({'id':'recOne','name':'同名人员','open_id':'ou_other_app'}),'on_exact')
        self.assertEqual(remote.request.call_count,1)
        self.assertEqual(remote.request.call_args.kwargs['params']['user_id_type'],'union_id')
        self.assertEqual(remote.request.call_args.kwargs['json']['field_names'],['员工姓名','离职/异动情况'])

    def test_duplicate_identity_is_never_used_for_delivery(self):
        remote=self.remote()
        remote.request=Mock(return_value={'items':[{'record_id':rid,'fields':{'员工姓名':[{'id':'on_same'}]}} for rid in ('recOne','recTwo')],'has_more':False})
        with self.assertRaises(RiskError):remote.identity({'id':'recOne','name':'重复人员'})

    def review_jobs(self):
        with self.store.connect() as db:
            return [{**dict(r), 'payload': json.loads(r['payload'])} for r in db.execute("SELECT * FROM risk_jobs WHERE kind='review_notice' ORDER BY created")]

    def test_submitting_another_item_does_not_suppress_first_review_notice(self):
        self.publish()
        data = self.act('alice', 'submit')
        person = data['people'][0]
        self.store.action(self.people['alice'], self.body['id'], 'alice', {
            'action': 'submit', 'version': person['version'], 'item_id': person['items'][1]['id'], 'values': {'result': '第二项完成'}})
        remote = self.remote(); remote.notify = Mock()
        for job in self.review_jobs():
            remote.process(job)
        self.assertEqual(remote.notify.call_count, 2)

    def test_old_review_cycle_does_not_notify_after_resubmission(self):
        self.publish(); self.act('alice', 'submit')
        self.act('reviewer', 'return', reason='补充')
        self.act('alice', 'submit')
        remote = self.remote(); remote.notify = Mock()
        for job in self.review_jobs():
            remote.process(job)
        self.assertEqual(remote.notify.call_count, 1)
        self.assertEqual(remote.notify.call_args.args[0]['payload']['version'], 4)

    def test_zero_remaining_items_are_revoked_not_completed(self):
        self.publish()
        self.act('admin', 'revoke_person', reason='不再需要')
        self.act('admin', 'revoke_person', pid='bob', reason='不再需要')
        self.store.checkpoint(self.body['id'], url='https://example.test/table')
        with self.store.connect(True) as db:
            db.execute("UPDATE risk_jobs SET status='done'")
        remote = self.remote(); remote.ensure_fields = Mock(); remote.upsert = Mock()
        remote.registry(*self.store.raw(self.body['id']))
        fields = remote.upsert.call_args.args[3]
        self.assertEqual(fields['灯塔任务状态'], '已撤销')
        self.assertEqual(fields['任务完成情况'], 0)

    def test_reassigned_reviewer_receives_pending_review_only(self):
        self.publish(); self.act('alice', 'submit')
        self.store.change_reviewer(self.people['admin'], self.body['id'], {'reviewer_id': 'admin', 'version': 1})
        remote = self.remote(); remote.notify = Mock()
        for job in self.review_jobs():
            remote.process(job)
        self.assertEqual(remote.notify.call_count, 1)
        self.assertEqual(remote.notify.call_args.args[2]['id'], 'admin')

    def test_admin_retry_can_authorize_preserved_feedback_after_permission_change(self):
        self.publish(); self.act('alice', 'submit')
        with self.store.connect(True) as db:
            db.execute("UPDATE risk_jobs SET status='failed' WHERE kind='sync'")
        self.people['alice']['modules'] = []
        self.store.retry(self.people['admin'], self.body['id'])
        with self.store.connect() as db:
            row = db.execute("SELECT * FROM risk_jobs WHERE kind='sync'").fetchone()
        job = {**dict(row), 'payload': json.loads(row['payload'])}
        self.store.checkpoint(self.body['id'], url='https://example.test/table')
        original = self.store.person_for
        def current(identity):
            if identity == 'alice':
                raise RiskError('人员权限已撤销', 403)
            return original(identity)
        self.store.person_for = current
        remote = self.remote(); remote.sync = Mock(); remote.ensure_fields = Mock(); remote.upsert = Mock()
        remote.process(job)
        remote.sync.assert_called_once()
        self.assertEqual(job['payload']['actor']['id'], 'alice')
        self.assertEqual(remote.sync.call_args.args[1]['items'][0]['values'], {'result': '无异常'})


class HistoryTemplateTests(unittest.TestCase):
    setUp=RiskTests.setUp
    publish=RiskTests.publish
    act=RiskTests.act
    remote=CloudTests.remote
    def test_history_uses_original_prefill_not_submitted_answers(self):
        self.body['items'][0]['values']={'result':'检查电源指示灯并记录实际情况'}
        self.publish();self.act('alice','submit',values={'result':'灯不亮，实际反馈'})
        history=self.store.history.get(self.people['admin'],self.body['id'])
        self.assertEqual(history['items'][0]['values']['result'],'检查电源指示灯并记录实际情况')
        serialized=json.dumps(history,ensure_ascii=False)
        self.assertNotIn('灯不亮，实际反馈',serialized)
        self.assertNotIn('reviewer_id',history)
        self.assertNotIn('people',history)

    def test_template_not_created_until_publication_activated_and_only_once(self):
        self.store.publish(self.people['admin'],self.body,self.template)
        self.assertEqual(self.store.history.list(self.people['admin'])['total'],0)
        self.store.activate(self.body['id']);self.store.activate(self.body['id'])
        self.assertEqual(self.store.history.list(self.people['admin'])['total'],1)
        with self.store.connect() as db:self.assertEqual(db.execute("SELECT COUNT(*) FROM risk_jobs WHERE kind='template_sync'").fetchone()[0],1)

    def test_history_edit_does_not_change_release_or_basic_template(self):
        self.publish()
        history=self.store.history.get(self.people['admin'],self.body['id'])
        body={k:copy.deepcopy(history[k]) for k in ('version','name','enabled','fields','items')}
        body['name']='新模板名称';body['fields'][1]['field_name']='检查说明';body['items'][0]['title']='新的检查要求'
        edited=self.store.history.update(self.people['admin'],history['id'],body)
        old=self.store.raw(history['id'])[0]
        self.assertEqual(edited['revision'],2)
        self.assertEqual(old['fields'][1]['field_name'],'排查结果')
        self.assertEqual(old['original_items'][0]['title'],'检查配电设备')
        with self.assertRaises(RiskError):self.store.history.update(self.people['admin'],history['id'],body)

    def test_reuse_keeps_historical_schema_when_basic_schema_changed(self):
        self.publish()
        history=self.store.history.get(self.people['admin'],self.body['id'])
        changed=copy.deepcopy(self.template);changed['fields'][1]['field_name']='全新基础字段';changed['version']='v2'
        body={**self.body,'id':'b'*32,'history_template_id':history['id'],'template_version':history['version'],'items':history['items']}
        created=self.store.publish(self.people['admin'],body,changed)
        self.assertEqual(created['fields'][1]['field_name'],'排查结果')
        self.assertEqual(created['people'][0]['items'][0]['status'],'pending')
        self.assertNotEqual(created['people'][0]['items'][0]['id'],self.store.get(self.people['admin'],history['id'])['people'][0]['items'][0]['id'])
        self.assertEqual(self.store.raw(created['id'])[0]['source_template']['revision'],1)

    def test_disabled_template_and_old_revision_cannot_publish(self):
        self.publish();h=self.store.history.get(self.people['admin'],self.body['id'])
        body={k:copy.deepcopy(h[k]) for k in ('version','name','enabled','fields','items')};body['enabled']=False
        self.store.history.update(self.people['admin'],h['id'],body)
        with self.assertRaises(RiskError):self.store.publish(self.people['admin'],{**self.body,'id':'b'*32,'history_template_id':h['id'],'template_version':h['version']},None)

    def test_old_task_backfill_never_uses_feedback(self):
        self.publish();self.act('alice','submit',values={'result':'不要复制此反馈'})
        with self.store.connect(True) as db:
            release=self.store._release(db,self.body['id']);release.pop('original_items');self.store._put_release(db,release)
            db.execute('DELETE FROM risk_history_templates');db.execute("DELETE FROM risk_jobs WHERE kind='template_sync'")
        self.store.history.backfill();h=self.store.history.get(self.people['admin'],self.body['id'])
        self.assertTrue(h['legacy']);self.assertTrue(all(not i['values'] for i in h['items']))
        self.assertNotIn('不要复制此反馈',json.dumps(h,ensure_ascii=False))

    def test_history_scope_and_role_guards(self):
        self.publish()
        with self.assertRaises(RiskError):self.store.history.get(self.people['alice'],self.body['id'])
        self.people['other']['level']='L3'
        self.assertEqual(self.store.history.list(self.people['other'])['total'],0)
        with self.assertRaises(RiskError):self.store.history.get(self.people['other'],self.body['id'])

    def test_dates_photos_and_system_values_are_not_reusable(self):
        from bin.lan_bitable_template_portal.risk_inspection_templates import reusable_items
        fields=self.fields+[{'key':'date','field_name':'排查日期','role':'input','type':5}]
        items=reusable_items(fields,[{'title':'检查项','values':{'result':'检查要求','date':1792150000000,'photo':['f'*32],'primary':'旧任务ID'}}])
        self.assertEqual(items,[{'title':'检查项','values':{'result':'检查要求'}}])

    def test_cloud_snapshot_contains_definition_only_and_upload_receipt_reused(self):
        self.body['items'][0]['values']={'result':'发布时的检查要求'}
        self.publish();self.act('alice','submit',values={'result':'实际排查反馈不能复制'})
        remote=self.remote();remote.history_table=Mock(return_value='tblHistory')
        remote.request=Mock(return_value={'file_token':'snapshotToken'})
        remote.upsert=Mock(side_effect=[RiskError('network',503),'record'])
        job={'payload':{'template_id':self.body['id'],'revision':1}}
        with self.assertRaises(RiskError):remote.sync_history(job)
        remote.sync_history(job)
        self.assertEqual(remote.request.call_count,1)
        content=remote.request.call_args.kwargs['files']['file'][1].getvalue().decode('utf-8')
        self.assertIn('发布时的检查要求',content);self.assertNotIn('实际排查反馈不能复制',content)
        self.assertNotIn('reviewer',json.loads(content))
        self.assertTrue(self.store.history.get(self.people['admin'],self.body['id'])['cloud']['synced'])

    def test_old_cloud_retry_cannot_overwrite_new_template(self):
        self.publish();h=self.store.history.get(self.people['admin'],self.body['id'])
        body={k:copy.deepcopy(h[k]) for k in ('version','name','enabled','fields','items')};body['name']='新版'
        self.store.history.update(self.people['admin'],h['id'],body)
        remote=self.remote();remote.request=Mock();remote.history_table=Mock()
        remote.sync_history({'payload':{'template_id':h['id'],'revision':1}})
        remote.request.assert_not_called();remote.history_table.assert_not_called()

    def test_template_validation_keeps_primary_and_rejects_duplicate_fields(self):
        self.publish();h=self.store.history.get(self.people['admin'],self.body['id'])
        body={k:copy.deepcopy(h[k]) for k in ('version','name','enabled','fields','items')}
        body['fields']=body['fields'][1:]
        with self.assertRaises(RiskError):self.store.history.update(self.people['admin'],h['id'],body)
        body['fields']=copy.deepcopy(h['fields']);body['fields'][1]['field_name']=body['fields'][0]['field_name']
        with self.assertRaises(RiskError):self.store.history.update(self.people['admin'],h['id'],body)

    def test_backup_retry_does_not_restart_notifications_or_feedback(self):
        self.publish(); self.act('alice', 'submit')
        with self.store.connect(True) as db:
            db.execute("UPDATE risk_jobs SET status='failed' WHERE status='queued'")
        self.store.retry(self.people['admin'], self.body['id'], template_only=True)
        with self.store.connect() as db:
            states = [(r['kind'], r['status']) for r in db.execute("SELECT kind,status FROM risk_jobs WHERE kind!='publish'")]
        self.assertTrue(all(status == ('queued' if kind == 'template_sync' else 'failed') for kind, status in states))
    def test_same_named_unrelated_cloud_table_is_not_adopted(self):
        remote=self.remote();remote.request=Mock()
        remote.all=Mock(side_effect=[[{'table_id':'unrelated','name':'风险排查模板库'}],[{'field_name':'普通业务字段','type':1}]])
        with self.assertRaises(RiskError):remote.history_table()
        remote.request.assert_not_called()


class RouteTests(unittest.TestCase):
    setUp = RiskTests.setUp
    publish = RiskTests.publish

    def client(self):
        from bin.lan_bitable_template_portal.risk_inspection_routes import install_risk_inspection_routes
        rows=[{'record_id':p['id'],'open_id':p['open_id'],'name':p['name'],'selectable':True} for p in self.people.values()]
        auth=SimpleNamespace(directory_person=lambda oid: {'id':oid[3:],'name':oid[3:],'selectable':True},
                             account_profile=lambda oid:copy.deepcopy(self.people[oid[3:]]),public_user=lambda session:session['user'])
        async def body(request,**kwargs):return await request.json()
        controller=SimpleNamespace(_current_session=lambda request:None if not request.headers.get('x-person') else {'user':{'open_id':'ou_'+request.headers['x-person']}},
                                   _request_base_url=lambda request:'http://testserver',_read_json_request=body)
        runtime=SimpleNamespace(auth_manager=auth,state_store=SimpleNamespace(db_path=self.store.db_path),service=SimpleNamespace(get_permission_directory_people=lambda:{'items':rows},_critical_guard_public_base_url=lambda:'http://testserver'))
        app=FastAPI();install_risk_inspection_routes(app,controller,runtime)
        client=TestClient(app)
        self.addCleanup(client.close)
        return client

    def test_unauthenticated_and_cross_origin_denied(self):
        client=self.client()
        self.assertEqual(client.get('/api/risk-inspections').status_code,401)
        self.assertEqual(client.post('/api/risk-inspections',json={},headers={'x-person':'admin','Origin':'https://untrusted.test'}).status_code,403)

    def test_people_directory_minimizes_personal_information(self):
        client=self.client()
        self.assertEqual(client.get('/api/risk-inspections/people',headers={'x-person':'alice'}).status_code,403)
        result=client.get('/api/risk-inspections/people',headers={'x-person':'admin'})
        self.assertEqual(result.status_code,200)
        self.assertNotIn('open_id',result.text)
        self.assertNotIn('secret',result.text)

    def test_history_api_requires_manager_and_preserves_source(self):
        self.publish();client=self.client()
        self.assertEqual(client.get('/api/risk-inspections/templates',headers={'x-person':'alice'}).status_code,403)
        path='/api/risk-inspections/templates/'+self.body['id']
        reply=client.get(path,headers={'x-person':'admin'})
        self.assertEqual(reply.status_code,200)
        history=reply.json()['data']
        payload={k:history[k] for k in ('version','name','enabled','fields','items')};payload['name']='历史模板改名'
        saved=client.put(path,json=payload,headers={'x-person':'admin','Origin':'http://testserver'})
        self.assertEqual(saved.status_code,200,saved.text)
        self.assertEqual(saved.json()['data']['revision'],2)
        self.assertEqual(self.store.raw(self.body['id'])[0]['name'],self.body['name'])

    def test_publish_retry_after_lost_response_does_not_need_cloud(self):
        self.publish(); client = self.client()
        with patch('bin.lan_bitable_template_portal.risk_inspection_routes.RiskCloud.read_template', side_effect=TimeoutError()) as read:
            response = client.post('/api/risk-inspections', json=self.body, headers={'x-person': 'admin'})
        self.assertEqual(response.status_code, 200, response.text)
        read.assert_not_called()

    def test_base_template_endpoints_preserve_old_schema_and_publish_selected_snapshot(self):
        client = self.client(); headers = {'x-person': 'admin', 'Origin': 'http://testserver'}
        with patch('bin.lan_bitable_template_portal.risk_inspection_routes.RiskCloud.read_template', return_value=self.template) as source:
            reply = client.get('/api/risk-inspections/base-templates', headers=headers)
            self.assertEqual(reply.status_code, 200, reply.text)
            again = client.get('/api/risk-inspections/base-templates', headers=headers)
            self.assertEqual(again.json()['data']['total'], 1)
            self.assertEqual(source.call_count, 1, 'seed reads the original once, later reads stay local')
        self.assertEqual(client.get('/api/risk-inspections/base-templates', headers={'x-person': 'alice'}).status_code, 403)
        body = {'id': 'd' * 32, 'name': '第二个基础模板', 'enabled': True, 'fields': self.fields, 'items': []}
        saved = client.post('/api/risk-inspections/base-templates', headers=headers, json=body)
        self.assertEqual(saved.status_code, 200, saved.text)
        chosen = saved.json()['data']
        self.assertEqual(client.get('/api/risk-inspections/templates/' + chosen['id'], headers=headers).status_code, 409)
        publication = {**self.body, 'base_template_id': chosen['id'], 'template_version': chosen['version']}
        with patch('bin.lan_bitable_template_portal.risk_inspection_routes.RiskCloud.read_template', side_effect=AssertionError('unexpected cloud schema read')):
            accepted = client.post('/api/risk-inspections', headers=headers, json=publication)
        self.assertEqual(accepted.status_code, 200, accepted.text)
        self.assertEqual(self.store.raw(self.body['id'])[0]['source_template']['id'], chosen['id'])

    def test_claim_endpoint_cannot_spoof_another_person_and_mine_list_is_personal(self):
        self.publish(); client = self.client()
        rid = self.body['id']; url = '/api/risk-inspections/' + rid + '/claim'
        self.assertEqual(client.post(url, headers={'x-person': 'bob'}, json={'scope': 'A'}).status_code, 403)
        self.assertEqual(client.post(url, headers={'x-person': 'admin'}, json={'scope': 'A', 'person_id': 'bob'}).status_code, 400)
        result = client.post(url, headers={'x-person': 'admin'}, json={'scope': 'A'})
        self.assertEqual(result.status_code, 200, result.text)
        mine = client.get('/api/risk-inspections?view=mine', headers={'x-person': 'admin'}).json()['data']
        self.assertEqual((mine['items'][0]['people_count'], mine['items'][0]['total']), (1, 2))
        self.assertEqual(client.post(url, headers={'x-person': 'admin', 'Origin': 'https://untrusted.test'}, json={'scope': 'A'}).status_code, 403)

    def test_reviewer_route_permission_and_version_checks(self):
        self.publish(); client = self.client()
        url = '/api/risk-inspections/'+self.body['id']+'/reviewer'
        body = {'reviewer_id': 'admin', 'version': 1}
        self.assertEqual(client.post(url, json=body, headers={'x-person': 'alice'}).status_code, 403)
        self.assertEqual(client.post(url, json=body, headers={'x-person': 'admin'}).status_code, 200)
        self.assertEqual(client.post(url, json=body, headers={'x-person': 'admin'}).status_code, 409)

    def test_worker_drains_ready_queue_without_five_second_gaps(self):
        self.publish()
        finished = threading.Event()
        calls = []
        def process(job):
            calls.append(job['id'])
            if len(calls) == 3:
                finished.set()
        with patch('bin.lan_bitable_template_portal.risk_inspection_routes.RiskCloud.process', side_effect=process):
            with self.client():
                self.assertTrue(finished.wait(4), 'Ready jobs should not wait five seconds between each operation')

    def test_worker_survives_failure_to_save_the_failure_receipt(self):
        self.publish()
        recovered = threading.Event()
        original = RiskInspections.finish_job
        count = 0
        def finish(store, job, error=''):
            nonlocal count
            count += 1
            if count <= 2:
                with store.connect(True) as db:
                    db.execute('UPDATE risk_jobs SET lease=0 WHERE id=?', (job['id'],))
                raise RuntimeError('isolated temporary receipt failure')
            original(store, job, error)
            recovered.set()
        with self.assertLogs(level='ERROR'), patch('bin.lan_bitable_template_portal.risk_inspection_routes.RiskCloud.process'), patch.object(RiskInspections, 'finish_job', finish):
            with self.client():
                self.assertTrue(recovered.wait(4), 'The worker must survive a failed failure-receipt write')

    def test_file_ownership_and_revoked_task(self):
        data=self.publish();rid=data['id'];item=data['people'][0]['items'][0]
        client=self.client()
        response=client.post(f'/api/risk-inspections/{rid}/files',params={'name':'photo.png','item_id':item['id'],'field_id':'photo'},content=b'fixture',headers={'x-person':'alice','Origin':'http://testserver'})
        self.assertEqual(response.status_code,200,response.text)
        fid=response.json()['data']['id']
        self.assertEqual(client.get('/api/risk-inspections/files/'+fid,headers={'x-person':'bob'}).status_code,403)
        self.assertEqual(client.get('/api/risk-inspections/files/'+fid,headers={'x-person':'reviewer'}).content,b'fixture')
        self.store.action(self.people['admin'],rid,'alice',{'action':'revoke_person','version':1,'reason':'范围调整'})
        response=client.post(f'/api/risk-inspections/{rid}/files',params={'name':'photo.png','item_id':item['id'],'field_id':'photo'},content=b'fixture',headers={'x-person':'alice'})
        self.assertEqual(response.status_code,409)


class PublicationIsolationTests(unittest.TestCase):
    setUp = RiskTests.setUp

    def publish_as(self, owner='publisher', rid='a' * 32):
        self.people.setdefault(owner, actor(owner, 'L3'))
        body = {**self.body, 'id': rid, 'reviewer_id': 'admin'}
        result = self.store.publish(self.people[owner], body, self.template)
        self.store.activate(rid)
        return result

    def test_same_level_publications_are_isolated_but_assignment_and_review_remain(self):
        self.publish_as()
        self.publish_as('peer', 'b' * 32)
        self.assertEqual(self.store.list(self.people['publisher'], view='published')['total'], 1)
        for operation in (lambda: self.store.get(self.people['peer'], 'a' * 32),
                          lambda: self.store.retry(self.people['peer'], 'a' * 32),
                          lambda: self.store.change_reviewer(self.people['peer'], 'a' * 32, {'version': 1, 'reviewer_id': 'reviewer'})):
            with self.assertRaises(RiskError) as error:
                operation()
            self.assertEqual(error.exception.status, 403)
        self.assertEqual(self.store.list(self.people['admin'], view='published')['total'], 2)
        own = self.store.list(self.people['alice'], view='mine')
        self.assertEqual((own['total'], own['items'][0]['people_count']), (2, 1))
        self.assertEqual([p['id'] for p in self.store.get(self.people['alice'], 'a' * 32)['people']], ['alice'])
        self.assertTrue(self.store.get(self.people['admin'], 'a' * 32)['is_reviewer'])

    def test_superior_visibility_stays_inside_existing_building_scope(self):
        self.publish_as()
        self.people['manager'] = actor('manager', 'L1', 'B')
        self.assertEqual(self.store.list(self.people['manager'])['total'], 0)
        with self.assertRaises(RiskError):
            self.store.get(self.people['manager'], 'a' * 32)
        self.people['manager']['scope_rules'] = ['A']
        self.assertEqual(self.store.list(self.people['manager'])['total'], 1)
        self.people['manager']['level'] = 'L3'
        self.assertEqual(self.store.list(self.people['manager'])['total'], 0)

    def test_owner_claim_is_idempotent_uses_original_prefill_and_is_fillable(self):
        self.body['items'][0]['values'] = {'result': '原始排查要求'}
        self.publish_as()
        data = self.store.list(self.people['publisher'], view='mine')
        self.assertTrue(data['items'][0]['can_claim'])
        self.assertEqual(data['items'][0]['total'], 0)
        def claim(_):
            return self.store.claim(self.people['publisher'], 'a' * 32, {'scope': 'A'})
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(claim, range(2)))
        person = next(p for p in results[0]['people'] if p['id'] == 'publisher')
        self.assertEqual(person['items'][0]['values'], {'result': '原始排查要求'})
        self.assertEqual(len([p for p in results[1]['people'] if p['id'] == 'publisher']), 1)
        self.assertFalse(results[1]['can_claim'])
        self.store.action(self.people['publisher'], 'a' * 32, 'publisher', {'action': 'submit', 'version': 1,
            'item_id': person['items'][0]['id'], 'values': {'result': '本人现场排查无异常'}})
        with self.store.connect() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM risk_audit WHERE action='领取本人任务'").fetchone()[0], 1)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM risk_jobs WHERE id LIKE 'claim-registry:%'").fetchone()[0], 1)
        mine = self.store.list(self.people['publisher'], view='mine')['items'][0]
        self.assertEqual((mine['people_count'], mine['total'], mine['submitted']), (1, 2, 1))

    def test_claim_rejects_peer_reviewer_unpublished_and_wrong_scope(self):
        self.people['publisher'] = actor('publisher', 'L3')
        self.store.publish(self.people['publisher'], {**self.body, 'reviewer_id': 'admin'}, self.template)
        with self.assertRaises(RiskError) as error:
            self.store.claim(self.people['publisher'], 'a' * 32, {'scope': 'A'})
        self.assertEqual(error.exception.status, 409)
        self.store.activate('a' * 32)
        self.people['peer'] = actor('peer', 'L3')
        for who, scope in [('peer', 'A'), ('admin', 'A'), ('publisher', 'B')]:
            with self.assertRaises(RiskError):
                self.store.claim(self.people[who], 'a' * 32, {'scope': scope})

    def test_multiple_base_templates_and_publication_versions_are_independent(self):
        history = self.store.history
        base = history.seed_base(self.people['admin'], self.template)
        self.assertFalse(history.needs_base(self.people['admin']))
        self.assertEqual(history.seed_base(self.people['admin'], self.template)['id'], base['id'])
        custom = history.add_base(self.people['admin'], {'id': 'd' * 32, 'name': '暖通排查', 'enabled': True,
            'fields': self.fields, 'items': [{'title': '检查冷却系统', 'values': {'result': '检查要求'}}]})
        self.assertEqual(history.list(self.people['admin'], kind='base')['total'], 2)
        self.assertEqual(history.list(self.people['admin'])['total'], 0)
        body = {**self.body, 'base_template_id': custom['id'], 'template_version': custom['version'], 'items': custom['items']}
        self.store.publish(self.people['admin'], body, None)
        changed = history.update(self.people['admin'], custom['id'], {'version': custom['version'], 'name': '暖通新版',
            'enabled': False, 'fields': custom['fields'], 'items': []})
        self.assertNotEqual(changed['version'], custom['version'])
        release = self.store.raw(self.body['id'])[0]
        self.assertEqual(release['source_template']['name'], '暖通排查')
        self.assertEqual(release['original_items'][0]['title'], '检查冷却系统')
        with self.assertRaises(RiskError):
            self.store.publish(self.people['admin'], {**body, 'id': 'e' * 32}, None)

    def test_base_template_creation_and_backups_are_idempotent_and_owner_scoped(self):
        history = self.store.history
        self.people['publisher'] = actor('publisher', 'L3')
        self.people['peer'] = actor('peer', 'L3')
        base = history.seed_base(self.people['publisher'], self.template)
        history.seed_base(self.people['publisher'], self.template)
        self.assertEqual(history.list(self.people['peer'], kind='base')['total'], 0)
        with self.assertRaises(RiskError):
            history.get(self.people['peer'], base['id'])
        self.assertEqual(history.list(self.people['admin'], kind='base')['total'], 1)
        with self.store.connect() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM risk_history_templates').fetchone()[0], 1)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM risk_jobs WHERE kind='template_sync'").fetchone()[0], 1)


class CloudRecoveryTests(unittest.TestCase):
    setUp = RiskTests.setUp
    remote = CloudTests.remote

    def test_record_upsert_uses_stable_uuid4_and_existing_record_after_response_loss(self):
        remote = self.remote()
        tokens, saved = [], {}
        def handle(req):
            if req.url.path.endswith('/search'):
                return httpx.Response(200, json={'code': 0, 'data': {'items': list(saved.values()), 'has_more': False}})
            body = json.loads(req.content)
            if req.method == 'POST':
                token = req.url.params['client_token']; tokens.append(token)
                self.assertEqual(uuid.UUID(token).version, 4)
                saved['record'] = {'record_id': 'recFixture', 'fields': body['fields']}
                raise httpx.ReadTimeout('response lost', request=req)
            self.assertEqual(req.method, 'PUT')
            return httpx.Response(200, json={'code': 0, 'data': {'record': saved['record']}})
        remote.headers = Mock(return_value={})
        remote.http = FeishuHttpClient(transport=httpx.MockTransport(handle))
        with patch('bin.lan_bitable_template_portal.portal_service.external_real_write_guard', return_value={'real_write_allowed': True}):
            with self.assertRaises(httpx.ReadTimeout):
                remote.upsert('appFixture', 'tblFixture', 'identity', {'文本': 'fixture'})
            self.assertEqual(remote.upsert('appFixture', 'tblFixture', 'identity', {'文本': 'fixture'}), 'recFixture')
        self.assertEqual(len(tokens), 1, 'retry finds the original row, not a second create')

    def test_http400_identifies_stage_and_code_without_exposing_response_sensitive_values(self):
        remote = self.remote(); remote.headers = Mock(return_value={})
        remote.http = FeishuHttpClient(transport=httpx.MockTransport(lambda req: httpx.Response(400,
            json={'code': 1254037, 'msg': 'SECRET-MUST-NOT-LEAK', 'error': {'token': 'SENSITIVE'}})))
        from bin.lan_bitable_template_portal.risk_inspection_cloud import REGISTRY_APP, REGISTRY_TABLE
        with patch('bin.lan_bitable_template_portal.portal_service.external_real_write_guard', return_value={'real_write_allowed': True}):
            with self.assertRaises(CloudRejected) as error:
                remote.request('POST', remote.table_path(REGISTRY_APP, REGISTRY_TABLE) + '/records', json={})
        message = str(error.exception)
        self.assertIn('登记排查任务', message); self.assertIn('1254037', message)
        self.assertNotIn('SECRET', message); self.assertNotIn('SENSITIVE', message)

    def test_created_table_is_reused_and_notifications_wait_for_confirmed_registry(self):
        self.store.publish(self.people['admin'], self.body, self.template)
        remote = self.remote(); remote.all = Mock(return_value=[])
        remote.request = Mock(return_value={'table_id': 'tblExisting', 'default_view_id': 'viewExisting'})
        remote.ensure_fields = Mock()
        remote.registry = Mock(side_effect=[CloudRejected('HTTP 400: 1254037', 503), None, None])
        with self.assertRaises(CloudRejected):
            remote.create_release(*self.store.raw(self.body['id']))
        self.assertEqual(self.store.raw(self.body['id'])[0]['status'], 'publishing')
        with self.store.connect() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM risk_jobs WHERE kind='notify'").fetchone()[0], 0)
        remote.create_release(*self.store.raw(self.body['id']))
        remote.create_release(*self.store.raw(self.body['id']))
        self.assertEqual(remote.request.call_count, 1, 'already-created table must never be duplicated')
        self.assertEqual(self.store.raw(self.body['id'])[0]['status'], 'active')
        with self.store.connect() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM risk_jobs WHERE kind='notify'").fetchone()[0], 2)


if __name__=='__main__':
    unittest.main()
