import copy
import json
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from bin.lan_bitable_template_portal.risk_inspection import (
    RiskError, RiskInspections, SYSTEM_NAMES, completion_key, digest, schema_fields,
)
from bin.lan_bitable_template_portal.risk_inspection_cloud import (
    CloudRejected, KEY, RiskCloud, TEMPLATE_APP,
)


def actor(pid, scope='ALL', level='L3', category='manager'):
    return {'id': pid, 'name': pid, 'open_id': 'ou_' + pid, 'level': level,
            'category': category, 'modules': ['risk_inspection'], 'scope_rules': [scope]}


class FinalPushTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.people = {'owner': actor('owner', level='L0'), 'reviewer': actor('reviewer'),
                       'alice': actor('alice', 'A', 'L4', 'employee'),
                       'bob': actor('bob', 'B', 'L4', 'employee'),
                       'ma': actor('ma', 'A'), 'mb': actor('mb', 'B'),
                       'all': actor('all', level='L0')}
        self.store = RiskInspections(Path(temporary.name) / 'test.sqlite3', lambda pid: copy.deepcopy(self.people[pid]))
        self.fields = schema_fields([
            {'field_id': 'primary', 'field_name': '排查任务编号', 'type': 1, 'is_primary': True},
            {'field_id': 'answer', 'field_name': '排查结果', 'type': 1},
            {'field_id': 'photo', 'field_name': '现场附件', 'type': 17},
            {'field_id': 'flag', 'field_name': '已确认', 'type': 7},
        ])
        self.body = {'id': 'a' * 32, 'name': '设备风险排查', 'deadline': (time.time() + 3600) * 1000,
                     'template_version': 'fixture', 'reviewer_id': 'reviewer', 'manager_ids': ['ma', 'mb'],
                     'people': [{'id': 'alice', 'scope': 'A'}, {'id': 'bob', 'scope': 'B'}],
                     'items': [{'title': '配电设备检查', 'values': {}}, {'title': '环境检查', 'values': {}}]}
        self.store.publish(self.people['owner'], self.body, {'version': 'fixture', 'fields': self.fields})
        self.rid = self.body['id']
        self.cloud = RiskCloud(self.store, Path(temporary.name) / 'never-read-config',
                               lambda person: 'on_' + person['id'], lambda: 'https://example.test')
        self.addCleanup(self.cloud.close)
        self.cloud.http.request = Mock(side_effect=AssertionError('real transport must never run'))
        self.cloud.request = Mock(side_effect=self.request)
        self.cloud.registry = Mock()
        self.schema, self.rows, self.messages, self.attempts = [], {}, {}, []
        self.message_hook, self.row_hook = None, None

    def request(self, method, path, **kwargs):
        if method == 'GET' and path.endswith('/tables'):
            return {'items': [], 'has_more': False}
        if method == 'POST' and path.endswith('/tables'):
            self.schema = copy.deepcopy(kwargs['json']['table']['fields'])
            return {'table_id': 'tblResult', 'default_view_id': 'viewResult'}
        if path.endswith('/fields'):
            if method == 'GET':
                return {'items': copy.deepcopy(self.schema), 'has_more': False}
            self.assertEqual(method, 'POST', 'existing columns must never be converted')
            field = {**kwargs['json'], 'field_id': 'field_' + str(len(self.schema))}
            self.schema.append(field)
            return {'field': field}
        if path.endswith('/records/search'):
            key = kwargs['json']['filter']['conditions'][0]['value'][0]
            return {'items': [copy.deepcopy(self.rows[key])] if key in self.rows else [], 'has_more': False}
        if '/records' in path:
            fields = kwargs['json']['fields']
            if self.row_hook:
                self.row_hook(fields)
            key = fields[KEY]
            row = self.rows.setdefault(key, {'record_id': 'record_' + str(len(self.rows)), 'fields': {}})
            row['fields'].update(copy.deepcopy(fields))
            return {'record': copy.deepcopy(row)}
        self.assertEqual((method, path), ('POST', 'im/v1/messages'))
        body = copy.deepcopy(kwargs['json'])
        self.attempts.append(body)
        pending = self.store.raw(self.rid)[0]['cloud']['final_push']
        self.assertEqual(pending['status'], 'pending', 'checkpoint must precede sending')
        self.assertLess(len(body['content'].encode('utf-8')), 25000)
        self.assertLess(len(json.loads(body['content'])['elements']), 40)
        if self.message_hook:
            self.message_hook(body)
        saved = self.messages.setdefault(body['uuid'], {'message_id': 'msg_' + str(len(self.messages)), 'body': body})
        return {'message_id': saved['message_id']}

    def raw(self):
        return self.store.raw(self.rid)

    def put(self, release=None, people=None):
        with self.store.connect(True) as db:
            if release is not None:
                self.store._put_release(db, release)
            for person in people or []:
                self.store._put_person(db, self.rid, person)

    def ready(self, managers=None):
        if managers is not None:
            release, _ = self.raw()
            release['final_managers'] = [{**self.store.brief(self.people[pid]),
                'scopes': [s for s in ('A', 'B') if s in self.people[pid]['scope_rules'] or 'ALL' in self.people[pid]['scope_rules']]}
                for pid in managers]
            self.put(release)
        self.cloud.create_release(*self.raw())
        _, people = self.raw()
        for person in people:
            for index, item in enumerate(person['items']):
                item.update(status='approved', submitted_at=1000 + index, reviewed_at=2000 + index,
                            reviewed_by='reviewer', reason='复核完成-' + person['scope'],
                            values={'answer': person['scope'] + '-反馈-' + str(index), 'flag': False})
        self.put(people=people)
        return self.job()

    def job(self):
        batch = completion_key(self.raw()[1])
        return {'id': 'final:' + self.rid + ':' + batch, 'kind': 'final_push',
                'release_id': self.rid, 'payload': {'batch': batch}}

    def field_reads(self):
        return sum(call.args[0] == 'GET' and call.args[1].endswith('/fields')
                   for call in self.cloud.request.call_args_list)

    def test_publish_initializes_pending_rows_before_activation_and_reuses_fresh_checkpoint(self):
        stale = self.raw()
        self.store.checkpoint(self.rid, messages={'other-job': {'message_id': 'keep-me'}})
        original_activate = self.store.activate

        def activate(rid):
            self.assertEqual(len(self.rows), 4)
            for row in self.rows.values():
                fields = row['fields']
                self.assertEqual(fields['排查进度'], '0%（0/2项已反馈）')
                self.assertEqual(fields['复核人'], 'reviewer')
                self.assertIsNone(fields['排查日期'])
                self.assertIsNone(fields['复核日期'])
                self.assertNotIn('排查结果', fields)
            original_activate(rid)

        with patch.object(self.store, 'activate', side_effect=activate):
            self.cloud.create_release(*stale)
            self.cloud.create_release(*stale)
        self.assertEqual(self.raw()[0]['cloud']['messages']['other-job']['message_id'], 'keep-me')
        self.assertEqual(self.field_reads(), 1)
        self.assertEqual(sum(c.args[0] == 'POST' and c.args[1].endswith('/tables')
                             for c in self.cloud.request.call_args_list), 1)

    def test_pending_initialization_failure_stays_unactivated_and_upserts_original_keys(self):
        count = 0

        def fail(fields):
            nonlocal count
            count += 1
            if count == 2:
                raise CloudRejected('row failed', 503)

        self.row_hook = fail
        with self.assertRaises(CloudRejected):
            self.cloud.create_release(*self.raw())
        self.assertEqual(self.raw()[0]['status'], 'publishing')
        with self.store.connect() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM risk_jobs WHERE kind='notify'").fetchone()[0], 0)
        self.cloud.create_release(*self.raw())
        self.assertEqual(len(self.rows), 4)
        self.assertEqual(self.raw()[0]['status'], 'active')
        self.assertEqual(self.field_reads(), 1)

    def test_sync_refreshes_pending_progress_without_leaking_draft_or_photos(self):
        self.cloud.create_release(*self.raw())
        release, people = self.raw()
        alice = people[0]
        alice['items'][0].update(status='submitted', submitted_at=1234.5, values={'answer': '已提交'})
        alice['items'][1]['values'] = {'answer': '私人草稿', 'photo': ['private-file']}
        self.cloud.attachment = Mock(side_effect=AssertionError('draft attachment must stay local'))
        self.cloud.sync(release, alice)
        rows = [self.rows[f'{self.rid}:alice:{item["id"]}']['fields'] for item in alice['items']]
        self.assertEqual([row['排查进度'] for row in rows], ['50%（1/2项已反馈）'] * 2)
        self.assertEqual(rows[0]['排查日期'], 1234500)
        self.assertIsNone(rows[1]['排查日期'])
        self.assertNotIn('排查结果', rows[1])
        self.assertNotIn('现场附件', rows[1])
        self.assertNotIn('私人草稿', json.dumps(self.rows, ensure_ascii=False))
        self.assertEqual(rows[0]['复核人'], 'reviewer')
        self.assertNotEqual(rows[0]['最终推送'], '已落表')

    def test_historical_sync_adds_missing_system_fields_once_without_changing_types(self):
        self.cloud.create_release(*self.raw())
        release, people = self.raw()
        release['fields'] = copy.deepcopy(self.fields)
        release['cloud'].pop('result_fields')
        self.put(release)
        self.schema = [f for f in self.schema if f['field_name'] not in SYSTEM_NAMES or f['field_name'] == '排查任务编号']
        self.cloud.request.reset_mock()
        job = {'id': 'sync-fixture', 'kind': 'sync', 'release_id': self.rid,
               'payload': {'actor': {'id': 'alice'}, 'person': people[0]}}
        self.cloud.process(job)
        self.cloud.process(job)
        self.assertEqual(self.field_reads(), 1)
        self.assertTrue(set(SYSTEM_NAMES) <= {f['field_name'] for f in self.schema})
        self.assertFalse(any(c.args[0] == 'PUT' and '/fields' in c.args[1] for c in self.cloud.request.call_args_list))

    def test_ensure_fields_rejects_existing_wrong_type_without_conversion(self):
        self.schema = [{'field_name': '排查日期', 'type': 1}]
        with self.assertRaisesRegex(RiskError, '类型不一致'):
            self.cloud.ensure_fields(TEMPLATE_APP, 'tblResult', [{'field_name': '排查日期', 'type': 5}])
        self.assertEqual(self.schema, [{'field_name': '排查日期', 'type': 1}])
        self.assertEqual(self.cloud.request.call_count, 1)

    def test_read_template_exposes_fixed_fields_and_edit_protects_system_roles(self):
        self.schema = copy.deepcopy(self.fields)
        current = self.cloud.read_template()
        self.assertEqual({f['field_name'] for f in current['fields'] if f['role'] != 'input'}, set(SYSTEM_NAMES))
        for field in current['fields']:
            if field['role'] == 'input':
                continue
            for action in ('update', 'delete'):
                with self.subTest(field=field['field_name'], action=action), self.assertRaises(RiskError):
                    self.cloud.edit_field(self.people['owner'], {'version': current['version'], 'action': action,
                        'field_id': field['key'], 'field_name': '自定义', 'type': 1, 'confirmed': True})
        for name, role in [('排查日期', 'input'), ('灯塔伪造', 'input'), ('自定义', 'reviewer'), ('自定义', [])]:
            with self.subTest(name=name, role=role), self.assertRaises(RiskError):
                self.cloud.edit_field(self.people['owner'], {'version': current['version'], 'action': 'add',
                    'field_name': name, 'type': 1, 'role': role})
        self.assertTrue(all(c.args[0] == 'GET' for c in self.cloud.request.call_args_list))

    def test_registry_reviewer_change_updates_only_system_fields_including_pending(self):
        self.cloud.create_release(*self.raw())
        release, people = self.raw()
        release['reviewer'] = self.store.brief(self.people['all'])
        release['reviewers'] = [release['reviewer']]
        people[0]['items'][0]['values'] = {'answer': 'private unsent draft', 'photo': ['local-file']}
        for row in self.rows.values():
            row['fields']['排查结果'] = 'preserve queued cloud snapshot'
        self.put(release, people)
        self.cloud.process({'id': 'reviewer-change', 'kind': 'registry', 'release_id': self.rid,
                            'payload': {'actor': {'id': 'owner'}}})
        for row in self.rows.values():
            self.assertEqual(row['fields']['复核人'], 'all')
            self.assertEqual(row['fields']['排查结果'], 'preserve queued cloud snapshot')
        self.assertNotIn('private unsent draft', json.dumps(self.rows))

    def test_success_isolates_buildings_contains_actual_results_and_writes_final_fields(self):
        job = self.ready()
        self.cloud.process(job)
        self.assertEqual(len(self.messages), 2)
        for message in self.messages.values():
            body = message['body']
            card = json.loads(body['content'])
            scope = 'A' if body['receive_id'] == 'on_ma' else 'B'
            other = 'B' if scope == 'A' else 'A'
            self.assertIn(scope + '-反馈-0', body['content'])
            self.assertIn(scope + '-反馈-1', body['content'])
            self.assertNotIn(other + '-反馈', body['content'])
            self.assertIn('复核完成-' + scope, body['content'])
            self.assertFalse(any(e['tag'] == 'action' for e in card['elements']))
        final = self.raw()[0]['cloud']['final_push']
        self.assertEqual(final['batch'], job['payload']['batch'])
        self.assertEqual(final['status'], 'sent')
        self.assertTrue(final['sent_at'])
        self.assertEqual(final['delivered'], ['ma', 'mb'])
        for person in self.raw()[1]:
            self.assertEqual(final['person_versions'][person['id']], digest(sorted(
                (item['id'], item['reviewed_at']) for item in person['items'])))
            for item in person['items']:
                fields = self.rows[f'{self.rid}:{person["id"]}:{item["id"]}']['fields']
                self.assertEqual(fields['最终推送'], '已推送：' + ('ma' if person['scope'] == 'A' else 'mb'))
                self.assertEqual(fields['排查日期'], int(item['submitted_at'] * 1000))
        before = len(self.attempts)
        sent_at = final['sent_at']
        self.cloud.process(job)
        self.assertEqual(len(self.attempts), before)
        self.assertEqual(self.raw()[0]['cloud']['final_push']['sent_at'], sent_at)

    def test_partial_failure_checkpoints_delivered_and_retries_only_failed_manager(self):
        job = self.ready(['all', 'ma'])

        def fail(body):
            if body['receive_id'] == 'on_ma':
                raise CloudRejected('delivery failed', 503)

        self.message_hook = fail
        with self.assertRaises(CloudRejected):
            self.cloud.process(job)
        final = self.raw()[0]['cloud']['final_push']
        self.assertEqual(final['delivered'], ['all'])
        self.assertEqual(final['status'], 'pending')
        self.assertIsNone(final['sent_at'])
        alice = self.raw()[1][0]
        self.assertEqual(self.rows[f'{self.rid}:alice:{alice["items"][0]["id"]}']['fields']['最终推送'], '部分已推送，等待重试')
        self.message_hook = None
        self.cloud.process(job)
        self.assertEqual(len(self.messages), 2)
        self.assertEqual(sum(b['receive_id'] == 'on_all' for b in self.attempts), 1)
        self.assertEqual(self.raw()[0]['cloud']['final_push']['status'], 'sent')

    def test_cloud_backfill_failure_retries_without_sending_any_message_again(self):
        job = self.ready()
        self.row_hook = Mock(side_effect=CloudRejected('cloud write failed', 503))
        with self.assertRaises(CloudRejected):
            self.cloud.process(job)
        self.assertEqual(self.raw()[0]['cloud']['final_push']['status'], 'sent')
        self.assertEqual(len(self.messages), 2)
        self.row_hook = None
        self.cloud.process(job)
        self.assertEqual(len(self.attempts), 2)

    def test_receipt_survives_final_checkpoint_failure(self):
        job = self.ready()
        original = self.store.checkpoint
        failed = False

        def checkpoint(rid, **values):
            nonlocal failed
            if values.get('final_push', {}).get('delivered') == ['ma'] and not failed:
                failed = True
                raise OSError('checkpoint failed')
            original(rid, **values)

        with patch.object(self.store, 'checkpoint', side_effect=checkpoint), self.assertRaises(OSError):
            self.cloud.process(job)
        self.assertEqual(self.raw()[0]['cloud']['final_push']['delivered'], [])
        self.assertTrue(self.raw()[0]['cloud']['messages'][job['id'] + ':ma:1']['message_id'])
        self.cloud.process(job)
        self.assertEqual(len(self.attempts), 2)
        self.assertEqual(len(self.messages), 2)

    def test_unknown_message_outcome_reuses_uuid_and_expired_attempt_requires_manual_check(self):
        job = self.ready()

        def timeout(body):
            self.messages[body['uuid']] = {'message_id': 'accepted-before-timeout', 'body': body}
            raise TimeoutError('response lost')

        self.message_hook = timeout
        with self.assertRaises(TimeoutError):
            self.cloud.process(job)
        self.message_hook = None
        self.cloud.process(job)
        self.assertEqual(self.attempts[0]['uuid'], self.attempts[1]['uuid'])
        self.assertEqual(len(self.messages), 2)
        release, _ = self.raw()
        release['cloud']['final_push']['delivered'] = []
        receipt = release['cloud']['messages'][job['id'] + ':ma:1']
        receipt.pop('message_id')
        receipt['attempted_at'] = time.time() - 21 * 3600
        self.put(release)
        attempts = len(self.attempts)
        with self.assertRaisesRegex(RiskError, '人工核验'):
            self.cloud.process(job)
        self.assertEqual(len(self.attempts), attempts)

    def test_stale_batch_unapproved_or_historical_without_managers_does_not_send(self):
        job = self.ready()
        original = self.raw()
        for change in ('batch', 'unapproved', 'timestamp', 'historical'):
            with self.subTest(change=change):
                release, people = copy.deepcopy(original)
                queued = copy.deepcopy(job)
                if change == 'batch':
                    queued['payload']['batch'] = 'old'
                elif change == 'unapproved':
                    people[0]['items'][0]['status'] = 'submitted'
                elif change == 'timestamp':
                    people[0]['items'][0]['reviewed_at'] += 1
                elif change == 'historical':
                    release.pop('final_managers')
                self.put(release, people)
                self.cloud.process(queued)
                self.assertNotIn('final_push', self.raw()[0]['cloud'])
                self.assertEqual(len(self.attempts), 0)

    def test_permission_changes_throw_and_never_mark_sent(self):
        job = self.ready()
        original = copy.deepcopy(self.people)
        for pid, update in [('owner', {'modules': []}), ('owner', {'scope_rules': ['A']}),
                            ('ma', {'category': 'employee'}), ('ma', {'modules': []}),
                            ('ma', {'level': 'L4'}), ('ma', {'scope_rules': ['B']})]:
            with self.subTest(pid=pid, update=update):
                self.people = copy.deepcopy(original)
                self.people[pid].update(update)
                with self.assertRaises(RiskError):
                    self.cloud.process(job)
                self.assertEqual(len(self.attempts), 0)
                self.assertNotEqual(self.raw()[0]['cloud'].get('final_push', {}).get('status'), 'sent')

    def test_narrowed_saved_scopes_pause_whole_manager_and_expanded_scopes_do_not_leak(self):
        job = self.ready(['all'])
        self.people['all']['scope_rules'] = ['A']
        with self.assertRaisesRegex(RiskError, '权限已变化'):
            self.cloud.process(job)
        self.assertEqual(len(self.attempts), 0)
        self.assertEqual(self.raw()[0]['cloud']['final_push']['delivered'], [])
        self.people['all']['scope_rules'] = ['ALL']
        self.cloud.process(job)
        self.assertFalse(any(e['tag'] == 'action' for e in json.loads(self.attempts[0]['content'])['elements']))
        release, _ = self.raw()
        release['cloud'].pop('final_push')
        release['cloud'].pop('messages')
        release['final_managers'] = [{**self.store.brief(self.people['ma']), 'scopes': ['A']}]
        self.put(release)
        self.people['ma']['scope_rules'] = ['ALL']
        self.attempts.clear()
        self.messages.clear()
        self.cloud.process(job)
        self.assertIn('A-反馈', self.attempts[0]['content'])
        self.assertNotIn('B-反馈', self.attempts[0]['content'])

    def test_detail_button_requires_original_controls(self):
        job = self.ready(['owner'])
        self.cloud.process(job)
        card = json.loads(self.attempts[0]['content'])
        self.assertTrue(any(e['tag'] == 'action' for e in card['elements']))

    def test_revoked_building_gets_empty_summary_without_leaking_withdrawn_feedback(self):
        self.ready()
        _, people = self.raw()
        people[1]['revoked'] = True
        for item in people[1]['items']:
            item['status'] = 'revoked'
        self.put(people=people)
        self.cloud.process(self.job())
        message = next(b for b in self.attempts if b['receive_id'] == 'on_mb')
        self.assertIn('共0人', message['content'])
        self.assertNotIn('B-反馈', message['content'])
        final = self.raw()[0]['cloud']['final_push']
        self.assertEqual(final['status'], 'sent')
        self.assertEqual(final['delivered'], ['ma', 'mb'])
        self.assertNotIn('bob', final['person_versions'])

    def test_new_batch_ignores_old_receipts_and_digests_exclude_revoked_items(self):
        first = self.ready()
        self.cloud.process(first)
        release, people = self.raw()
        people[0]['items'][0]['status'] = 'revoked'
        self.put(people=people)
        self.cloud.sync(release, people[0], system_only=True)
        fields = self.rows[f'{self.rid}:alice:{people[0]["items"][1]["id"]}']['fields']
        self.assertNotIn('已推送', fields['最终推送'])
        second = self.job()
        self.assertNotEqual(first['payload']['batch'], second['payload']['batch'])
        self.cloud.process(second)
        self.assertEqual(len(self.messages), 4)
        final = self.raw()[0]['cloud']['final_push']
        self.assertEqual(final['person_versions']['alice'], digest([
            (people[0]['items'][1]['id'], people[0]['items'][1]['reviewed_at'])]))

    def test_permission_narrows_between_parts_then_restores_without_resending_completed_parts(self):
        self.ready(['all'])
        _, people = self.raw()
        people[0]['items'][0]['values']['answer'] = '长反馈' * 5000
        self.put(people=people)
        job = self.job()

        def narrow(body):
            self.people['all']['scope_rules'] = ['A']

        self.message_hook = narrow
        with self.assertRaisesRegex(RiskError, '权限已变化'):
            self.cloud.process(job)
        self.assertEqual(len(self.messages), 1)
        self.assertEqual(self.raw()[0]['cloud']['final_push']['delivered'], [])
        first_uuid = self.attempts[0]['uuid']
        self.message_hook = None
        self.people['all']['scope_rules'] = ['ALL']
        self.cloud.process(job)
        self.assertEqual(sum(b['uuid'] == first_uuid for b in self.attempts), 1)
        self.assertEqual(self.raw()[0]['cloud']['final_push']['status'], 'sent')

    def test_batch_change_during_delivery_stops_remaining_managers_and_old_backfill(self):
        job = self.ready()

        def changed(body):
            _, people = self.raw()
            people[0]['items'][0]['status'] = 'submitted'
            self.put(people=people)

        self.message_hook = changed
        self.cloud.process(job)
        self.assertEqual(len(self.messages), 1)
        self.assertEqual(self.raw()[0]['cloud']['final_push']['status'], 'pending')
        self.assertTrue(all('已推送' not in row['fields']['最终推送'] for row in self.rows.values()))

    def test_large_report_cards_are_bounded_complete_and_resume_from_failed_part(self):
        job = self.ready(['all'])
        release, people = self.raw()
        template = people[0]['items'][0]
        people = [{**people[0], 'id': 'person_' + str(p), 'name': 'person_' + str(p),
                   'items': [{**copy.deepcopy(template), 'id': 'item_' + str(i),
                              'values': {'answer': f'UNIQUE-{p}-{i}-' + '风险反馈' * 1000 + '-END', 'flag': False}}
                             for i in range(25)]} for p in range(3)]
        with self.store.connect(True) as db:
            db.execute('DELETE FROM risk_people WHERE release_id=?', (self.rid,))
        self.put(people=people)
        job = self.job()
        failed_uuid = None

        def fail_third(body):
            nonlocal failed_uuid
            if len(self.attempts) == 3:
                failed_uuid = body['uuid']
                raise CloudRejected('third part failed', 503)

        self.message_hook = fail_third
        with self.assertRaises(CloudRejected):
            self.cloud.process(job)
        self.assertEqual(self.raw()[0]['cloud']['final_push']['delivered'], [])
        previous = {body['uuid'] for body in self.attempts[:2]}
        self.message_hook = None
        self.cloud.process(job)
        self.assertEqual(sum(body['uuid'] in previous for body in self.attempts), 2)
        self.assertEqual(sum(body['uuid'] == failed_uuid for body in self.attempts), 2)
        joined = '\n'.join(message['body']['content'] for message in self.messages.values())
        for p in range(3):
            for i in range(25):
                self.assertIn(f'UNIQUE-{p}-{i}-', joined)
        self.assertEqual(joined.count('-END'), 75)
        self.assertIn('共3人', joined)
        self.assertIn('75项', joined)
        self.assertEqual(self.raw()[0]['cloud']['final_push']['status'], 'sent')

    def test_maximum_assignment_generator_limits_count_and_bytes_without_controls(self):
        self.ready()
        release, people = self.raw()
        template = people[0]['items'][0]
        people = [{**people[0], 'id': f'p{p}', 'name': f'p{p}',
                   'items': [{**template, 'id': f'i{i}', 'values': {'answer': f'RESULT-{p}-{i}'}}
                             for i in range(100)]} for p in range(500)]
        items = 0
        for card in self.cloud.final_cards(release, people, self.people['ma']):
            self.assertLess(len(json.dumps(card, ensure_ascii=False).encode('utf-8')), 25000)
            self.assertLess(len(card['elements']), 40)
            self.assertFalse(any(e['tag'] == 'action' for e in card['elements']))
            items += sum('RESULT-' in e['text']['content'] for e in card['elements'])
        self.assertEqual(items, 50000)


if __name__ == '__main__':
    unittest.main()
