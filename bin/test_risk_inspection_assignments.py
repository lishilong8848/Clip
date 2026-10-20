"""Assignment grants and cloud isolation, using temporary databases and fake transports."""
import copy
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

from bin import test_risk_inspection as fixtures
from bin import test_risk_inspection_final_push as cloud_fixtures
from bin.lan_bitable_template_portal.risk_inspection import (
    RiskError, RiskInspections, completion_key, digest, item_scope, person_scopes,
    reviewer_list, scoped_people, system_values,
)
from bin.lan_bitable_template_portal.risk_inspection_cloud import RiskCloud


class AssignmentTests(unittest.TestCase):
    publish = fixtures.RiskTests.publish

    def setUp(self):
        fixtures.RiskTests.setUp(self)
        self.people['admin']['level'] = 'L1'
        self.people['superA'] = fixtures.actor('superA', 'L0', 'A')
        self.people['alice'].update(modules=[], scope_rules=[], home_scope='C')
        self.people['reviewer'].update(modules=[], scope_rules=['H'], level='L4')
        self.body.pop('reviewer_id')
        self.body.update(reviewer_ids=['alice', 'reviewer'],
                         people=[{'id': 'alice', 'scopes': ['A', 'B']}, {'id': 'bob', 'scopes': ['B']}])

    def action(self, who, action, index=0, pid='alice', **body):
        person = next(p for p in self.store.raw(self.body['id'])[1] if p['id'] == pid)
        return self.store.action(self.people[who], self.body['id'], pid, {
            'action': action, 'item_id': person['items'][index]['id'], 'version': person['version'],
            'values': {'result': 'answer-' + str(index)}, **body})

    def remote(self):
        cloud = RiskCloud(self.store, Path(self.temp.name) / 'never-read',
                          lambda p: 'on_' + p['id'], lambda: 'https://example.test')
        self.addCleanup(cloud.close)
        cloud.http.request = Mock(side_effect=AssertionError('real transport forbidden'))
        cloud.request = Mock(side_effect=AssertionError('cloud access forbidden'))
        return cloud

    def review_jobs(self):
        with self.store.connect() as db:
            return [{**dict(r), 'payload': json.loads(r['payload'])} for r in db.execute(
                "SELECT * FROM risk_jobs WHERE kind='review_notice' ORDER BY created,id")]

    def test_contract_has_one_person_row_distinct_item_ids_and_compatibility_fields(self):
        self.publish()
        release, people = self.store.raw(self.body['id'])
        self.assertEqual(release['task_grants_version'], 2)
        self.assertEqual(release['reviewer'], release['reviewers'][0])
        self.assertEqual([p['id'] for p in reviewer_list(release)], ['alice', 'reviewer'])
        alice = people[0]
        self.assertEqual(person_scopes(alice), ['A', 'B'])
        self.assertEqual([i['scope'] for i in alice['items']], ['A', 'A', 'B', 'B'])
        self.assertEqual(len({i['id'] for p in people for i in p['items']}), 6)
        expanded = scoped_people(people)
        self.assertEqual([(p['id'], p['scope'], len(p['items'])) for p in expanded],
                         [('alice', 'A', 2), ('alice', 'B', 2), ('bob', 'B', 2)])
        with self.store.connect() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM risk_people').fetchone()[0], 2)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM risk_jobs WHERE kind='notify'").fetchone()[0], 2)

    def test_legacy_fallback_keeps_ids_and_completion_key(self):
        release = {'reviewer': {'id': 'old', 'name': 'old'}}
        person = {'id': 'legacy', 'scope': 'B', 'revoked': False,
                  'items': [{'id': 'original', 'status': 'approved', 'reviewed_at': 100}]}
        self.assertEqual(reviewer_list(release), [release['reviewer']])
        self.assertEqual(person_scopes(person), ['B'])
        self.assertEqual(item_scope(person, person['items'][0]), 'B')
        self.assertEqual(scoped_people([person])[0]['items'][0]['id'], 'original')
        self.assertEqual(completion_key([person]), digest([('legacy', 'original', 100)])[:24])

    def test_legacy_saved_task_is_readable_actionable_and_keeps_original_ids(self):
        self.body['people'] = [{'id': 'bob', 'scope': 'A'}]
        self.body['reviewer_ids'] = ['reviewer']
        self.publish()
        release, people = self.store.raw(self.body['id'])
        release.pop('reviewers')
        release.pop('task_grants_version')
        person = people[0]
        person.pop('scopes')
        ids = [i['id'] for i in person['items']]
        for item in person['items']:
            item.pop('scope')
        with self.store.connect(True) as db:
            self.store._put_release(db, release)
            self.store._put_person(db, self.body['id'], person)
        self.assertTrue(self.store.has_task_access(self.people['reviewer']))
        self.assertEqual(self.store.list(self.people['reviewer'], view='mine')['items'][0]['total'], 2)
        self.action('bob', 'submit', pid='bob')
        detail = self.action('reviewer', 'approve', pid='bob')
        self.assertEqual([i['id'] for i in detail['people'][0]['items']], ids)
        self.assertNotIn('task_grants_version', self.store.raw(self.body['id'])[0])

    def test_no_module_self_review_and_first_reviewer_wins_with_version_guard(self):
        self.publish()
        self.action('alice', 'submit')
        person = self.store.raw(self.body['id'])[1][0]
        old_version = person['version']
        approved = self.action('alice', 'approve')['people'][0]['items'][0]
        self.assertEqual(approved['status'], 'approved')
        self.assertEqual(approved['reviewed_by'], 'alice')
        self.assertTrue(approved['reviewed_at'])
        with self.assertRaises(RiskError) as error:
            self.action('reviewer', 'approve', version=old_version)
        self.assertEqual(error.exception.status, 409)
        self.assertEqual(self.store.raw(self.body['id'])[1][0]['items'][0]['reviewed_by'], 'alice')

    def test_two_reviewers_concurrently_only_one_commits(self):
        self.publish()
        self.action('alice', 'submit')
        person = self.store.raw(self.body['id'])[1][0]

        def approve(who):
            try:
                self.action(who, 'approve', version=person['version'])
                return 200
            except RiskError as error:
                return error.status

        with ThreadPoolExecutor(max_workers=2) as pool:
            self.assertEqual(sorted(pool.map(approve, ['alice', 'reviewer'])), [200, 409])

    def test_multiple_buildings_keep_state_and_attachments_separate(self):
        self.publish()
        person = self.store.raw(self.body['id'])[1][0]
        a, b = person['items'][0], person['items'][2]
        fid = 'f' * 32
        with self.store.connect(True) as db:
            db.execute('INSERT INTO risk_files VALUES(?,?,?,?,?,?,?,?)',
                       (fid, self.body['id'], 'alice', a['id'], 'photo', 'A-only.txt', 1, 100))
        self.action('alice', 'submit', values={'result': 'A answer', 'photo': [fid]})
        self.action('alice', 'approve')
        with self.assertRaises(RiskError) as error:
            self.action('alice', 'submit', index=2, values={'result': 'B answer', 'photo': [fid]})
        self.assertEqual(error.exception.status, 403)
        person = self.store.raw(self.body['id'])[1][0]
        self.assertEqual([i['status'] for i in person['items']], ['approved', 'pending', 'pending', 'pending'])
        self.assertEqual(person['items'][2]['id'], b['id'])
        self.assertNotEqual(system_values(self.store.raw(self.body['id'])[0], person, a)['status'],
                            system_values(self.store.raw(self.body['id'])[0], person, b)['status'])
        self.assertEqual(person['items'][2]['values'], {})

    def test_mine_includes_reviewer_all_people_and_second_scope_without_original_scope(self):
        self.publish()
        for who in ('alice', 'reviewer'):
            result = self.store.list(self.people[who], view='mine', scope='B')
            self.assertEqual(result['total'], 1)
            self.assertEqual((result['items'][0]['people_count'], result['items'][0]['total']), (2, 4))
        all_items = self.store.list(self.people['reviewer'], view='mine')['items'][0]
        self.assertEqual((all_items['people_count'], all_items['total']), (2, 6))
        with self.assertRaises(RiskError):
            self.store.list(self.people['reviewer'], scope='H')
        self.assertEqual(self.store.list(self.people['bob'], scope='A')['total'], 0)

    def test_assignee_only_sees_all_own_buildings_and_no_other_task_grants(self):
        self.body['reviewer_ids'] = ['reviewer']
        self.publish()
        detail = self.store.get(self.people['alice'], self.body['id'])
        self.assertEqual([(p['id'], len(p['items'])) for p in detail['people']], [('alice', 4)])
        counts = self.store.list(self.people['alice'], view='mine')['items'][0]
        self.assertEqual((counts['people_count'], counts['total']), (1, 4))
        self.assertEqual(self.store.list(self.people['alice'], scope='B')['items'][0]['total'], 2)
        body = {**self.body, 'id': 'b' * 32, 'reviewer_ids': ['admin'],
                'people': [{'id': 'bob', 'scopes': ['B']}]}
        self.store.publish(self.people['admin'], body, self.template)
        self.store.activate(body['id'])
        self.assertTrue(self.store.has_task_access(self.people['alice'], self.body['id']))
        self.assertFalse(self.store.has_task_access(self.people['alice'], body['id']))
        with self.assertRaises(RiskError):
            self.store.get(self.people['alice'], body['id'])
        self.assertEqual(self.people['alice']['modules'], [])
        self.assertEqual(self.people['alice']['scope_rules'], [])

    def test_reviewer_gets_no_manage_delete_reassign_template_publish_or_retry_permission(self):
        self.publish()
        reviewer = self.people['reviewer']
        for data in (self.store.get(reviewer, self.body['id']), self.store.list(reviewer)['items'][0]):
            self.assertTrue(data['is_reviewer'])
            self.assertFalse(data['can_manage_release'])
            self.assertFalse(data['can_delete_release'])
        self.assertFalse(self.store.list(reviewer)['can_edit_template'])
        operations = [lambda: self.store.delete(reviewer, self.body['id'], {}),
                      lambda: self.store.retry(reviewer, self.body['id']),
                      lambda: self.store.change_reviewer(reviewer, self.body['id'], {'reviewer_ids': ['bob'], 'version': 1}),
                      lambda: self.action('reviewer', 'revoke_person', reason='no'),
                      lambda: self.store.publish(reviewer, {**self.body, 'id': 'b' * 32}, self.template)]
        for operation in operations:
            with self.subTest(operation=operation), self.assertRaises(RiskError):
                operation()

    def test_partial_manager_cannot_see_other_items_audit_jobs_or_manage_whole_person(self):
        self.body['people'] = [{'id': 'alice', 'scopes': ['A', 'B']}]
        self.publish()
        self.action('alice', 'submit', index=2, values={'result': 'B-PRIVATE-ANSWER'})
        self.action('reviewer', 'return', index=2, reason='B-PRIVATE-REASON')
        self.action('alice', 'submit', values={'result': 'A-PUBLIC'})
        release, people = self.store.raw(self.body['id'])
        with self.store.connect(True) as db:
            db.execute("UPDATE risk_jobs SET error='B-PRIVATE-JOB' WHERE kind='sync'")
            self.store._audit(db, self.body['id'], 'alice', self.people['admin'], 'revoke_person',
                              {'reason': 'B-PRIVATE-REVOKE', 'items': people[0]['items']})
        self.store.checkpoint(self.body['id'], url='https://example.test/whole-table')
        detail = self.store.get(self.people['superA'], self.body['id'])
        self.assertEqual(len(detail['people']), 1)
        self.assertEqual([i['scope'] for i in detail['people'][0]['items']], ['A', 'A'])
        self.assertEqual(detail['people'][0]['scopes'], ['A'])
        self.assertEqual(detail['url'], '')
        self.assertFalse(detail['can_manage_release'])
        self.assertNotIn('B-PRIVATE', json.dumps(detail))
        self.assertEqual(self.store.view_scopes(self.people['superA'], release, people[0]), ['A'])
        self.assertFalse(self.store._visible(self.people['superA'], release, people[0], item=people[0]['items'][2]))
        self.assertTrue(self.store._visible(self.people['superA'], release, people[0], item=people[0]['items'][0]))
        for operation in (lambda: self.action('superA', 'revoke_person', reason='no'),
                          lambda: self.action('superA', 'revoke_item', index=2, reason='no'),
                          lambda: self.store.delete(self.people['superA'], self.body['id'], {}),
                          lambda: self.store.retry(self.people['superA'], self.body['id']),
                          lambda: self.store.change_reviewer(self.people['superA'], self.body['id'], {'reviewer_ids': ['bob'], 'version': 1})):
            with self.assertRaises(RiskError):
                operation()
        self.action('superA', 'revoke_item', index=1, reason='A only')
        self.assertEqual(self.store.list(self.people['superA'])['items'][0]['total'], 1)
        with self.assertRaises(RiskError):
            self.store.list(self.people['superA'], scope='B')

    def test_disabled_guest_temporary_identities_and_deleted_or_revoked_task_grants(self):
        self.body['reviewer_ids'] = ['reviewer']
        self.publish()
        for change in ({'explicit_disabled': True}, {'inactive': True}, {'role': 'guest'}, {'role': 'temporary'}, {'open_id': ''}):
            actor = {**self.people['alice'], **change}
            self.assertFalse(self.store.has_task_access(actor))
            with self.assertRaises(RiskError):
                self.store.get(actor, self.body['id'])
        self.action('admin', 'revoke_person', reason='withdraw')
        self.assertFalse(self.store.has_task_access(self.people['alice']))
        self.assertTrue(self.store.has_task_access(self.people['reviewer']))
        self.store.delete(self.people['admin'], self.body['id'], {})
        self.assertFalse(self.store.has_task_access(self.people['reviewer']))
        self.assertEqual(self.store.list(self.people['reviewer'], view='mine')['total'], 0)

    def test_change_reviewers_replaces_task_grant_and_suppresses_old_generation_notices(self):
        self.publish()
        self.action('alice', 'submit')
        before = copy.deepcopy(self.store.raw(self.body['id'])[1])
        self.store.change_reviewer(self.people['admin'], self.body['id'], {'reviewer_ids': ['bob'], 'version': 1})
        self.assertFalse(self.store.has_task_access(self.people['reviewer']))
        with self.assertRaises(RiskError):
            self.store.get(self.people['reviewer'], self.body['id'])
        self.store.change_reviewer(self.people['admin'], self.body['id'], {'reviewer_ids': ['reviewer', 'alice'], 'version': 2})
        self.assertEqual(before, self.store.raw(self.body['id'])[1])
        remote = self.remote()
        remote.notify = Mock()
        for job in self.review_jobs():
            remote.process(job)
        self.assertEqual(remote.notify.call_count, 2)
        self.assertEqual({c.args[2]['id'] for c in remote.notify.call_args_list}, {'alice', 'reviewer'})
        self.assertTrue(all(c.args[0]['payload']['reviewer_version'] == 3 for c in remote.notify.call_args_list))
        with self.assertRaises(RiskError) as error:
            self.store.change_reviewer(self.people['admin'], self.body['id'], {'reviewer_ids': ['bob'], 'version': 2})
        self.assertEqual(error.exception.status, 409)

    def test_each_reviewer_notified_and_completed_review_suppresses_pending_notices(self):
        self.publish()
        self.action('alice', 'submit')
        remote = self.remote()
        remote.notify = Mock()
        for job in self.review_jobs():
            remote.process(job)
        self.assertEqual(remote.notify.call_count, 2)
        self.action('reviewer', 'approve')
        remote.notify.reset_mock()
        for job in self.review_jobs():
            remote.process(job)
        self.assertEqual(remote.notify.call_count, 1)
        self.assertEqual(remote.notify.call_args.args[2]['id'], 'alice')

    def test_invalid_assignment_inputs_and_owner_scope_rejected_before_writes(self):
        for updates in ({'people': [{'id': 'alice', 'scopes': []}]},
                        {'people': [{'id': 'alice', 'scopes': ['A', 'A']}]},
                        {'people': [{'id': 'alice', 'scopes': 'A'}]},
                        {'reviewer_ids': []}, {'reviewer_ids': ['alice', 'alice']},
                        {'reviewer_ids': [None]}):
            with self.subTest(updates=updates), self.assertRaises(RiskError):
                self.store.publish(self.people['admin'], {**self.body, **updates}, self.template)
        self.people['admin']['scope_rules'] = ['A']
        with self.assertRaises(RiskError):
            self.store.publish(self.people['admin'], self.body, self.template)
        self.people['admin']['scope_rules'] = ['ALL']
        self.people['reviewer']['explicit_disabled'] = True
        with self.assertRaises(RiskError):
            self.store.publish(self.people['admin'], self.body, self.template)
        with self.store.connect() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM risk_releases').fetchone()[0], 0)

    def test_owner_can_claim_and_self_review_with_independent_item_ids(self):
        self.body['reviewer_ids'] = ['admin']
        self.publish()
        result = self.store.claim(self.people['admin'], self.body['id'], {'scope': 'B'})
        claimed = next(p for p in result['people'] if p['id'] == 'admin')
        self.assertEqual(claimed['scopes'], ['B'])
        self.assertEqual(len(claimed['items']), 2)
        self.action('admin', 'submit', pid='admin')
        result = self.action('admin', 'approve', pid='admin')
        self.assertEqual(next(p for p in result['people'] if p['id'] == 'admin')['items'][0]['reviewed_by'], 'admin')


class AssignmentCloudTests(unittest.TestCase):
    request = cloud_fixtures.FinalPushTests.request
    raw = cloud_fixtures.FinalPushTests.raw
    put = cloud_fixtures.FinalPushTests.put
    job = cloud_fixtures.FinalPushTests.job

    def setUp(self):
        cloud_fixtures.FinalPushTests.setUp(self)
        self.people['alice'].update(modules=[], scope_rules=[], home_scope='C')
        self.people['reviewer'].update(modules=[], level='L4', scope_rules=['H'])
        self.body = {**self.body, 'id': 'b' * 32, 'reviewer_ids': ['alice', 'reviewer'],
                     'people': [{'id': 'alice', 'scopes': ['A', 'B']}]}
        self.store.publish(self.people['owner'], self.body, {'version': 'fixture', 'fields': self.fields})
        self.rid = self.body['id']

    def ready(self):
        self.cloud.create_release(*self.raw())
        _, people = self.raw()
        for person in people:
            for index, item in enumerate(person['items']):
                scope = item_scope(person, item)
                item.update(status='approved', submitted_at=1000 + index, reviewed_at=2000 + index,
                            reviewed_by='actual-' + scope, reason='done-' + scope,
                            values={'answer': scope + '-PRIVATE-' + str(index)})
        self.put(people=people)
        return self.job()

    def test_create_release_allows_task_grants_and_syncs_each_item_building(self):
        self.cloud.create_release(*self.raw())
        self.assertEqual(len(self.rows), 4)
        _, people = self.raw()
        for item in people[0]['items']:
            row = self.rows[f'{self.rid}:alice:{item["id"]}']['fields']
            self.assertEqual(row['楼栋'], item['scope'] + '楼')
            self.assertEqual(row['复核人'], 'alice、reviewer')
            self.assertEqual(row['排查进度'], '0%（0/2项已反馈）')
        with self.store.connect() as db:
            jobs = [json.loads(r[0]) for r in db.execute("SELECT payload FROM risk_jobs WHERE release_id=? AND kind='notify'", (self.rid,))]
        self.assertEqual(jobs, [{'person_id': 'alice'}])
        self.cloud.notify = Mock()
        self.cloud.process({'id': 'initial', 'release_id': self.rid, 'kind': 'notify', 'payload': jobs[0]})
        self.cloud.notify.assert_called_once()

    def test_create_rechecks_owner_second_building_and_disabled_targets_before_transport(self):
        cases = [('owner', {'scope_rules': ['A']}), ('owner', {'modules': []}),
                 ('reviewer', {'explicit_disabled': True}), ('alice', {'explicit_disabled': True})]
        original = copy.deepcopy(self.people)
        for pid, update in cases:
            self.people = copy.deepcopy(original)
            self.people[pid].update(update)
            with self.subTest(pid=pid), self.assertRaises(RiskError):
                self.cloud.create_release(*self.raw())
            self.assertEqual(self.cloud.request.call_count, 0)

    def test_progress_and_dates_are_scope_local_and_actual_reviewer_overrides_default(self):
        self.cloud.create_release(*self.raw())
        release, people = self.raw()
        person = people[0]
        person['items'][0].update(status='approved', submitted_at=12.5, reviewed_at=13.5, reviewed_by='actual')
        self.cloud.sync(release, person)
        for index, item in enumerate(person['items']):
            row = self.rows[f'{self.rid}:alice:{item["id"]}']['fields']
            self.assertEqual(row['排查进度'], '50%（1/2项已反馈）' if index < 2 else '0%（0/2项已反馈）')
            self.assertEqual(row['排查日期'], 12500 if index == 0 else None)
            self.assertEqual(row['复核日期'], 13500 if index == 0 else None)
            self.assertEqual(row['复核人'], 'actual' if index == 0 else 'alice、reviewer')

    def test_registry_deduplicates_people_but_tracks_each_building_completion(self):
        self.cloud.create_release(*self.raw())
        release, people = self.raw()
        for item in people[0]['items'][:2]:
            item['status'] = 'approved'
        with patch.object(self.cloud, 'ensure_fields'), patch.object(self.cloud, 'upsert') as upsert:
            RiskCloud.registry(self.cloud, release, people)
        fields = upsert.call_args.args[3]
        self.assertEqual(fields['现场排查人'], [{'id': 'on_alice'}])
        self.assertEqual(fields['楼栋'], ['A楼', 'B楼'])
        self.assertEqual(fields['已完成楼栋'], ['A楼'])
        self.assertEqual(fields['灯塔提交人数'], 1)
        self.assertEqual(fields['灯塔通过人数'], 0)
        self.assertEqual(fields['任务完成情况'], .5)
        self.assertEqual(fields['灯塔审核人'], 'alice、reviewer')

    def test_final_push_isolates_same_person_items_by_manager_and_scope_versions(self):
        job = self.ready()
        self.cloud.process(job)
        self.assertEqual(len(self.attempts), 2)
        for body in self.attempts:
            scope = 'A' if body['receive_id'] == 'on_ma' else 'B'
            other = 'B' if scope == 'A' else 'A'
            self.assertIn(scope + '-PRIVATE', body['content'])
            self.assertNotIn(other + '-PRIVATE', body['content'])
            self.assertIn('共1人', body['content'])
            self.assertIn('2项', body['content'])
        release, people = self.raw()
        final = release['cloud']['final_push']
        self.assertEqual(set(final['scope_versions']), {'alice:A', 'alice:B'})
        self.assertEqual(set(final['person_versions']), {'alice'})
        for item in people[0]['items']:
            scope = item['scope']
            row = self.rows[f'{self.rid}:alice:{item["id"]}']['fields']
            self.assertEqual(row['最终推送'], '已推送：m' + scope.lower())
        before = len(self.attempts)
        self.cloud.process(job)
        self.assertEqual(len(self.attempts), before)
        people[0]['items'][2]['reviewed_at'] += 1
        self.assertIn('已推送', system_values(release, people[0], people[0]['items'][0])['sync'])
        self.assertNotIn('已推送', system_values(release, people[0], people[0]['items'][2])['sync'])
        self.assertNotEqual(completion_key(people), job['payload']['batch'])

    def test_final_all_scope_card_counts_person_once_and_groups_by_scope(self):
        self.ready()
        release, people = self.raw()
        release['final_managers'] = [{**self.store.brief(self.people['all']), 'scopes': ['A', 'B']}]
        cards = list(self.cloud.final_cards(release, people, self.people['all']))
        content = json.dumps(cards, ensure_ascii=False)
        self.assertIn('共1人', content)
        self.assertIn('4项', content)
        self.assertIn('A楼：1人 / 2项', content)
        self.assertIn('B楼：1人 / 2项', content)
        release['final_managers'][0]['scopes'] = ['A']
        content = json.dumps(list(self.cloud.final_cards(release, people, self.people['all'])))
        self.assertNotIn('B-PRIVATE', content)

    def test_legacy_final_card_order_and_completion_are_stable_for_existing_receipts(self):
        self.ready()
        release, people = self.raw()
        release.pop('task_grants_version')
        legacy_people = []
        for expanded in reversed(scoped_people(people)):
            expanded['id'] += '-' + expanded['scope']
            expanded.pop('scopes')
            expanded['items'] = copy.deepcopy(expanded['items'])
            for item in expanded['items']:
                item.pop('scope')
            legacy_people.append(expanded)
        cards = list(self.cloud.final_cards(release, legacy_people, self.people['all']))
        elements = [e['text']['content'] for card in cards for e in card['elements'] if e['tag'] == 'div'][1:]
        self.assertTrue(elements[0].startswith('B楼'))
        expected = digest(sorted((p['id'], i['id'], i.get('reviewed_at'))
                                for p in legacy_people for i in p['items']))[:24]
        self.assertEqual(completion_key(legacy_people), expected)

    def test_task_grants_never_replace_final_manager_module_or_original_scope(self):
        job = self.ready()
        self.people['mb'].update(modules=[])
        self.cloud.notify = Mock()
        with self.assertRaises(RiskError):
            self.cloud.process(job)
        self.assertNotIn('mb', self.raw()[0]['cloud']['final_push']['delivered'])
        self.assertEqual(self.cloud.notify.call_count, 1)

    def test_no_module_assignee_and_reviewer_sync_but_disabled_identity_is_blocked(self):
        self.cloud.create_release(*self.raw())
        person = self.raw()[1][0]
        self.store.action(self.people['alice'], self.rid, 'alice', {'action': 'submit', 'version': person['version'],
                          'item_id': person['items'][0]['id'], 'values': {'answer': 'submitted'}})
        person = self.raw()[1][0]
        self.store.action(self.people['reviewer'], self.rid, 'alice', {'action': 'approve', 'version': person['version'],
                          'item_id': person['items'][0]['id']})
        with self.store.connect() as db:
            jobs = [{**dict(r), 'payload': json.loads(r['payload'])} for r in db.execute(
                "SELECT * FROM risk_jobs WHERE release_id=? AND kind='sync' ORDER BY created", (self.rid,))]
        for job in jobs:
            self.cloud.process(job)
        item = self.raw()[1][0]['items'][0]
        self.assertEqual(self.rows[f'{self.rid}:alice:{item["id"]}']['fields']['复核人'], 'reviewer')
        self.people['reviewer']['explicit_disabled'] = True
        with self.assertRaises(RiskError):
            self.cloud.process(jobs[-1])


if __name__ == '__main__':
    unittest.main()
