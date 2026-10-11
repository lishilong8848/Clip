"""Independent authorization checks using isolated learning records."""
import copy
import unittest
from bin import test_learning_self_service as support
from lan_bitable_template_portal.learning import LearningError


class ScopeBoundaryTests(unittest.TestCase):
    setUp = support.SelfServiceTests.setUp
    seed = support.SelfServiceTests.seed
    dispatch = support.SelfServiceTests.dispatch
    question = support.SelfServiceTests.question
    seed_service = support.SelfServiceTests.seed_service
    actor = support.SelfServiceTests.actor

    def papers(self):
        self.seed_service()
        own = self.actor(id='oid-p1', person_id='p1', can_answer=True)
        paper = self.dispatch('paper.claim', {}, own)
        self.dispatch('paper.answer', {'id': paper['id'], 'person_id': 'p1',
            'question_id': paper['questions'][0]['id'], 'option_ids': ['o0'], 'operation_id': 'same-operation'}, own)
        with self.service.transaction() as conn:
            old = copy.deepcopy(self.service._get('paper', paper['id'], conn))
            old.update(id='old-B-paper', scope='B', date='2026-09-30')
            self.service._put('paper', old['id'], old, conn)
            record = copy.deepcopy(self.service._get('record', paper['id'], conn))
            record.update(id=old['id'], scope='B', date='2026-09-30')
            self.service._put('record', old['id'], record, conn)
        return own, paper

    def test_admin_can_read_another_person_without_answering_for_them(self):
        _, paper = self.papers()
        admin = self.actor(id='admin', is_admin=True)
        self.assertEqual(self.service._paper(paper['id'], admin)['person_id'], 'p1')
        with self.assertRaises(LearningError): self.service._paper(paper['id'], admin, write=True)

    def test_duty_cannot_query_other_building_profile(self):
        self.seed_service()
        duty = self.actor(id='duty-A', shared_account=True, scope='A')
        with self.assertRaises(LearningError) as caught:
            self.service.profile(duty, {'person_id': 'p2'})
        self.assertEqual(caught.exception.status, 403)

    def test_duty_person_lists_keep_historical_building_scope(self):
        own, _ = self.papers()
        duty = self.actor(id='duty-A', shared_account=True, scope='A')
        query = {'person_id': 'p1', 'from': '2026-09-01', 'to': '2026-12-31', 'kind': 'all'}
        for action in ('history', 'review', 'attempts'):
            result = self.dispatch(action, {}, duty, query)
            self.assertEqual({p['scope'] for p in result['items']}, {'A'}, action)
        profile = self.service.profile(duty, query)
        self.assertEqual(profile['summary']['papers'], 1)
        self.assertEqual(self.service.attempt_history(own, query)['total'], 2,
                         'same operation id in distinct papers must not lose historical attempts')

    def test_admin_can_handle_other_person_issue(self):
        own, paper = self.papers()
        item = self.service.create_issue({'paper_id': paper['id'], 'person_id': 'p1',
            'question_id': paper['questions'][0]['id'], 'description': '需要核对答案'}, own)
        result = self.service.update_issue({'id': item['id'], 'version': item['version'],
            'status': 'resolved', 'remark': '管理员已核对'}, self.actor(id='admin', is_admin=True))
        self.assertEqual(result['status'], 'resolved')

    # ---- admin all-people full portrait -------------------------------------
    @staticmethod
    def _person_h3():
        return support._person('p3', '人员3', 'H', 'ou_p3')

    def _seed_p3_plus_p1_p2_answer(self):
        """A's p1 first-attempt correct twice, B's p2 first-attempt wrong once."""
        self.seed_service()
        with self.service.transaction() as conn:
            self.service._put('person', 'p3', self._person_h3(), conn)
        p1 = self.actor(id='oid-p1', person_id='p1', can_answer=True)
        paper1 = self.dispatch('paper.claim', {'person_id': 'p1'}, p1)
        for pos in range(2):
            q = paper1['questions'][pos]
            self.dispatch('paper.answer', {'id': paper1['id'], 'person_id': 'p1',
                'question_id': q['id'], 'option_ids': ['o0'], 'operation_id': 'p1-c' + str(pos)}, p1)
        p2 = self.actor(id='oid-p2', person_id='p2', can_answer=True)
        paper2 = self.dispatch('paper.claim', {'person_id': 'p2'}, p2)
        self.dispatch('paper.answer', {'id': paper2['id'], 'person_id': 'p2',
            'question_id': paper2['questions'][0]['id'], 'option_ids': ['o1'],
            'operation_id': 'p2-w'}, p2)
        return p1, p2

    def test_admin_all_people_profile_includes_all_buildings_and_people_count_weighted(self):
        self._seed_p3_plus_p1_p2_answer()
        admin = self.actor(id='admin', is_admin=True)
        prof = self.service.profile(admin, {'scope': '', 'all_people': '1'})
        people = {p['person_id']: p for p in prof['people']}
        self.assertEqual(set(people), {'p1', 'p2', 'p3'})
        # H 楼 p3 is active but has never claimed: no attempt, nothing assigned.
        self.assertIsNone(people['p3']['summary']['accuracy'])
        self.assertEqual(people['p3']['summary']['assigned'], 0)
        self.assertIsNone(people['p3']['today'])
        # Count-weighted aggregate, never a mean of per-person percentages.
        s = prof['summary']
        self.assertEqual(s['answered'], 3)
        self.assertEqual(s['correct'], 2)
        self.assertEqual(s['wrong'], 1)
        # Count-weighted (2/3), never a mean of per-person percentages.
        self.assertEqual(s['accuracy'], 66.7)
        self.assertEqual({b['scope'] for b in prof['buildings']}, set('ABCDEH'))

    def test_all_people_flag_does_not_widen_ordinary_or_duty_views(self):
        self._seed_p3_plus_p1_p2_answer()
        # Ordinary p1 with all_people still sees only self, never p2/p3.
        ordinary_prof = self.service.profile(
            self.actor(id='oid-p1', person_id='p1', can_answer=True), {'all_people': '1'})
        self.assertEqual({p['person_id'] for p in ordinary_prof['people']}, {'p1'})
        # A 楼值班 with all_people stays read-only scoped to A, no B/H people.
        duty_a = self.actor(id='duty-A', shared_account=True, scope='A')
        duty_prof = self.service.profile(duty_a, {'scope': 'A', 'all_people': '1'})
        self.assertEqual({p['person_id'] for p in duty_prof['people']}, {'p1'})
        self.assertEqual({p['person_id'] for p in duty_prof['people']} & {'p2', 'p3'}, set())

    def test_admin_all_people_not_injected_in_single_building_scope(self):
        self._seed_p3_plus_p1_p2_answer()
        admin = self.actor(id='admin', is_admin=True)
        scoped = self.service.profile(admin, {'scope': 'A', 'all_people': '1'})
        self.assertEqual({p['person_id'] for p in scoped['people']}, {'p1'})
        self.assertEqual({p['person_id'] for p in scoped['people']} & {'p2', 'p3'}, set())
        self.assertEqual({b['scope'] for b in scoped['buildings']}, {'A'})


if __name__ == '__main__': unittest.main()
