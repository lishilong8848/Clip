import copy
import unittest
from unittest.mock import patch

from bin import test_risk_inspection as fixtures
from bin.lan_bitable_template_portal.risk_inspection import (
    RiskError, SYSTEM_NAMES, completion_key, digest, schema_fields, system_values,
)
from bin.lan_bitable_template_portal.risk_inspection_templates import clean_fields


class SystemFieldTests(unittest.TestCase):
    setUp = fixtures.RiskTests.setUp
    publish = fixtures.RiskTests.publish
    act = fixtures.RiskTests.act

    def values(self, who='alice', item_index=0):
        detail = self.store.get(self.people[who], self.body['id'])
        person = next(p for p in detail['people'] if p['id'] == who)
        values = person['items'][item_index]['system_values']
        return {f['role']: values[f['key']] for f in detail['fields'] if f['role'] != 'input'}

    def add_managers(self):
        self.people['manager'] = {**fixtures.actor('manager', 'L1', 'ALL'), 'category': 'manager'}
        self.body['manager_ids'] = ['manager']

    def test_defaults_are_present_non_editable_and_cannot_be_remapped(self):
        self.fields += schema_fields([{'field_id': 'date', 'field_name': '排查日期', 'type': 5}],
                                     {'date': {'role': 'input', 'required': True}})
        self.assertEqual(self.fields[-1]['role'], 'inspected_at')
        data = self.publish()
        for role in SYSTEM_NAMES.values():
            fields = [f for f in data['fields'] if f['role'] == role]
            self.assertEqual(len(fields), 1)
            self.assertFalse(fields[0]['required'])
        self.assertEqual(self.values()['reviewer'], 'reviewer')
        self.assertIsNone(self.values()['inspected_at'])
        self.assertEqual(self.values()['review'], '')
        self.assertIsNone(self.values()['reviewed_at'])

    def test_new_publication_defaults_reviewer_to_initiator(self):
        self.body.pop('reviewer_id')
        data = self.publish()
        self.assertEqual(data['reviewer_id'], 'admin')
        self.assertEqual(self.values()['reviewer'], 'admin')

    def test_invalid_role_and_field_metadata_are_validation_errors(self):
        for value in ([], {}, 1, None):
            fields = copy.deepcopy(self.fields)
            fields[1]['role'] = value
            with self.subTest(value=value), self.assertRaises(RiskError):
                clean_fields(fields)

    def test_recipient_can_be_assigned_other_buildings_without_changing_home_scope(self):
        self.people['alice'].update(home_scope='C', scope_rules=['ALL'])
        self.people['reviewer']['scope_rules'] = ['ALL']
        self.body['people'][0] = {'id': 'alice', 'scopes': ['A', 'C']}
        self.publish()
        self.assertEqual(self.values()['scope'], 'A楼')
        self.assertEqual(self.values(item_index=2)['scope'], 'C楼')
        self.assertEqual(self.values('bob')['scope'], 'A楼')
        self.assertEqual(self.people['alice']['home_scope'], 'C')

    def test_date_uses_submission_time_not_draft_or_manual_value(self):
        self.publish()
        self.act('alice', 'save')
        self.assertIsNone(self.values()['inspected_at'])
        with patch('bin.lan_bitable_template_portal.risk_inspection.time.time', return_value=1800000000.125):
            self.act('alice', 'submit')
        self.assertEqual(self.values()['inspected_at'], 1800000000125)
        with self.assertRaises(RiskError):
            self.act('bob', 'submit', pid='bob', values={'result': '正常', 'system_inspected_at': 1})
        self.assertIsNone(self.values('bob')['inspected_at'])

    def test_progress_tracks_feedback_not_review_approval(self):
        self.publish()
        self.assertEqual(self.values()['status'], '0%（0/2项已反馈）')
        self.act('alice', 'submit')
        self.assertEqual(self.values()['status'], '50%（1/2项已反馈）')
        self.act('reviewer', 'approve')
        self.assertEqual(self.values()['status'], '50%（1/2项已反馈）')
        self.assertEqual(self.values('bob')['status'], '0%（0/2项已反馈）')

    def test_return_and_resubmit_replace_verdict_date_without_erasing_audit(self):
        self.publish()
        self.act('alice', 'submit')
        self.act('reviewer', 'return', reason='需要补充')
        self.assertEqual(self.values()['status'], '0%（0/2项已反馈）')
        self.assertEqual(self.values()['review'], '退回修改：需要补充')
        self.assertIsNotNone(self.values()['reviewed_at'])
        data = self.act('alice', 'submit')
        self.assertEqual(self.values()['review'], '')
        self.assertIsNone(self.values()['reviewed_at'])
        self.assertTrue(any('需要补充' in row['data'] for row in data['audit']))

    def test_legacy_task_get_is_readonly_and_hides_old_manually_entered_date(self):
        self.publish()
        with self.store.connect(True) as db:
            release = self.store._release(db, self.body['id'])
            release['fields'] = copy.deepcopy(self.fields) + [{'key': 'old_date', 'field_name': '排查日期', 'type': 5, 'role': 'input', 'required': True}]
            self.store._put_release(db, release)
            person = self.store._person(db, self.body['id'], 'alice')
            person['items'][0]['values']['old_date'] = 1
            self.store._put_person(db, self.body['id'], person)
        before = self.store.raw(self.body['id'])
        detail = self.store.get(self.people['alice'], self.body['id'])
        self.assertNotIn('old_date', detail['people'][0]['items'][0]['values'])
        self.assertIsNone(detail['people'][0]['items'][0]['system_values']['old_date'])
        self.assertEqual(self.store.raw(self.body['id']), before)
        self.act('alice', 'submit')

    def test_history_cannot_delete_or_rename_system_defaults(self):
        self.publish()
        history = self.store.history.get(self.people['admin'], self.body['id'])
        body = {k: copy.deepcopy(history[k]) for k in ('version', 'name', 'enabled', 'fields', 'items')}
        field = next(f for f in body['fields'] if f['role'] == 'reviewer')
        field['field_name'] = '另一个复核人'
        with self.assertRaisesRegex(RiskError, '系统默认字段'):
            self.store.history.update(self.people['admin'], history['id'], body)

    def test_selected_managers_must_cover_each_building_and_be_real_managers(self):
        self.body['manager_ids'] = []
        with self.assertRaisesRegex(RiskError, '接收经理'):
            self.store.publish(self.people['admin'], self.body, self.template)
        self.body['manager_ids'] = ['admin']
        with self.assertRaisesRegex(RiskError, '接收经理'):
            self.store.publish(self.people['admin'], self.body, self.template)
        self.add_managers()
        self.body['people'].append({'id': 'other', 'scope': 'B'})
        self.people['reviewer']['scope_rules'] = ['ALL']
        self.people['manager']['scope_rules'] = ['A']
        with self.assertRaisesRegex(RiskError, '每个接收楼栋'):
            self.store.publish(self.people['admin'], self.body, self.template)

    def test_final_push_enqueued_only_after_every_active_item_approved(self):
        self.add_managers()
        self.publish()
        for pid in ['alice', 'bob']:
            for index in range(2):
                person = next(p for p in self.store.get(self.people[pid], self.body['id'])['people'] if p['id'] == pid)
                self.store.action(self.people[pid], self.body['id'], pid, {'action': 'submit', 'version': person['version'], 'item_id': person['items'][index]['id'], 'values': {'result': '已核对'}})
                current = next(p for p in self.store.get(self.people['reviewer'], self.body['id'])['people'] if p['id'] == pid)
                self.store.action(self.people['reviewer'], self.body['id'], pid, {'action': 'approve', 'version': current['version'], 'item_id': current['items'][index]['id']})
                with self.store.connect() as db:
                    count = db.execute("SELECT COUNT(*) FROM risk_jobs WHERE kind='final_push'").fetchone()[0]
                self.assertEqual(count, int(pid == 'bob' and index == 1))
        self.assertNotIn('已推送', self.values()['sync'])
        with self.store.connect(True) as db:
            db.execute("UPDATE risk_jobs SET status='done' WHERE kind NOT IN ('sync','final_push')")
            db.execute("UPDATE risk_jobs SET status='failed' WHERE kind='sync'")
        self.assertIsNone(self.store.claim_job(), 'A failed result write must hold the final manager push')

    def test_old_receipts_cannot_mark_new_work_successful(self):
        self.add_managers()
        self.publish()
        release, people = self.store.raw(self.body['id'])
        person = people[0]
        for item in person['items']:
            item.update(status='approved', reviewed_at=1800000000)
        release['cloud']['final_push'] = {'delivered': ['manager'], 'person_versions': {'alice': digest(sorted((i['id'], i['reviewed_at']) for i in person['items']))}}
        self.assertEqual(system_values(release, person, person['items'][0])['sync'], '已推送：manager')
        person['items'][0]['reviewed_at'] += 1
        self.assertNotIn('已推送', system_values(release, person, person['items'][0])['sync'])
        self.assertFalse(completion_key(people))


if __name__ == '__main__':
    unittest.main()
