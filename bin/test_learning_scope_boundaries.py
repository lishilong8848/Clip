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


if __name__ == '__main__': unittest.main()
