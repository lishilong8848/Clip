"""Personal learning contracts, with temporary SQLite and no external writes.

These fixtures are aligned to the new self-service authorization: personal
accounts act as themselves (single building each), building duty accounts are
read-only, and admin may read but not answer for others.  No old proxy
(duty-claims-for-others) guard has been weakened.
"""
import concurrent.futures
import copy
import datetime as dt
import json
import unittest
from tempfile import TemporaryDirectory
from unittest.mock import patch

from bin import test_learning as support
from bin.test_learning import ADMIN, DAY, CURRENT
from lan_bitable_template_portal import learning, learning_personal as personal

BUILDING = {"p1": "A", "p2": "A", "p3": "A", "p4": "A", "p5": "A", "p6": "B"}


class PersonalLearningTests(unittest.TestCase):
    setUp = support.LearningTests.setUp
    seed = support.LearningTests.seed
    question = support.LearningTests.question
    dispatch = support.LearningTests.dispatch
    assert_status = support.LearningTests.assert_status

    def prepare(self, count=120):
        self.seed(self.question(i, bank, 'single' if bank in {'written', 'supplemental'} else 'interview')
                  for bank in learning.BANKS for i in range(count))
        with self.service.transaction() as conn:
            for n in range(1, 7):
                pid = f'p{n}'
                building = BUILDING[pid]
                self.service._put('person', pid, {'id': pid, 'person_id': pid, 'name': f'人员{n}',
                    'employee_no': str(n), 'scopes': [building], 'active': True,
                    'aliases': [f'staff:rec{n}'], 'login_ids': [f'staff:rec{n}', f'oid-{pid}']}, conn)
        self.service._restored = True
        self.service.publish(DAY)

    def self_actor(self, who='p1'):
        return {'id': 'oid-' + who, 'scope': '', 'is_admin': False, 'person_id': who,
                'shared_account': False, 'can_answer': True, 'identity_issue': ''}

    def duty(self, scope):
        return {'id': f'duty-{scope}', 'scope': scope, 'is_admin': False, 'person_id': '',
                'shared_account': True, 'can_answer': False, 'identity_issue': ''}

    def claim(self, who='p1', scope='A'):
        return self.dispatch('paper.claim', {'person_id': who}, self.self_actor(who))

    def answer(self, paper, wrong=False):
        q = next(q for q in paper['questions'] if q['type'] != 'interview')
        return self.dispatch('paper.answer', {'id': paper['id'], 'person_id': paper['person_id'], 'question_id': q['id'],
            'option_ids': ['o1' if wrong else 'o0'], 'operation_id': 'test-' + paper['id']}, self.self_actor(paper['person_id']))

    def test_reserves_four_then_four_and_resume_without_consuming(self):
        self.prepare()
        self.assertEqual(len(self.service._all('reserve')), 24)
        self.assertEqual(self.service.list_papers(ADMIN, {'today': '1'})['total'], 0)
        self.assertEqual(self.service.profile(ADMIN, {})['summary']['assigned'], 0)
        first = self.claim()
        self.assertEqual(len(first['questions']), 15)
        self.assertEqual({b: sum(q['bank'] == b for q in first['questions']) for b in learning.BANKS}, learning.QUOTAS)
        self.answer(first)
        resumed = self.claim()
        self.assertEqual(resumed['stats']['answered'], 1)
        self.assertEqual(len(self.service._all('reserve')), 24)
        for n in range(2, 6):
            self.claim(f'p{n}')
        self.assertEqual(len(self.service._all('reserve')), 28)
        self.assertEqual(len(self.service._documents('reserve', scope='A')), 8)
        self.assertEqual(self.claim('p1', 'H')['id'], first['id'])

    def test_seven_days_per_person_independent_and_shortage(self):
        self.prepare(count=56)
        history = []
        for day in range(8):
            with patch.object(learning, 'now', return_value=CURRENT + dt.timedelta(days=day)):
                self.service.publish(DAY + dt.timedelta(days=day))
                paper = self.claim()
                families = {q['family_id'] for q in self.service._get('paper', paper['id'])['questions']}
                for previous in history[-6:]:
                    self.assertTrue(families.isdisjoint(previous))
                history.append(families)
                self.assertEqual(paper['stats']['shortage'], 0)
        with patch.object(learning, 'now', return_value=CURRENT + dt.timedelta(days=1)):
            other = self.claim('p2')
            self.assertEqual(len(other['questions']), 15)

    def test_concurrent_claim_is_atomic_and_restart_reuses(self):
        self.prepare()
        # Only the bounded test executor may start threads; no service workers.
        with patch('threading.Thread.start', self._real_thread_start):
            with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
                papers = list(pool.map(lambda _: self.claim(), range(12)))
        self.assertEqual(len({p['id'] for p in papers}), 1)
        self.assertEqual(sum(bool(r.get('person_id')) for r in self.service._all('reserve')), 1)
        self.service = learning.LearningService(self.root, self.cloud, self.sender)
        self.assert_status(503, self.claim)
        self.service.restore()
        self.assertEqual(self.claim()['id'], papers[0]['id'])

    def test_grade_identity_permissions_and_old_history(self):
        self.prepare()
        first = self.claim()
        wrong = self.answer(first, wrong=True)
        second = self.claim('p2')
        self.answer(second)
        # Personal self-accounts see only their own accuracy.
        self.assertEqual(self.service.profile(self.self_actor('p1'), {})['summary']['accuracy'], 0)
        self.assertEqual(self.service.profile(self.self_actor('p2'), {})['summary']['accuracy'], 100)
        # Building duty sees only its own building aggregate.
        self.assertEqual(self.service.profile(self.duty('A'), {'scope': 'A'})['summary']['accuracy'], 50)
        # Stale-version write still conflicts for the owner (guard not weakened).
        self.assert_status(409, self.dispatch, 'paper.notes',
                           {'id': first['id'], 'person_id': first['person_id'],
                            'question_id': first['questions'][0]['id'], 'version': first['version']},
                           self.self_actor('p1'))
        self.assertEqual(self.service.bootstrap('A', self.duty('A'))['scope'], 'A')
        self.assert_status(403, self.dispatch, 'people', actor={'id': 'guest', 'scope': '', 'is_admin': False, 'person_id': ''})
        legacy = copy.deepcopy(self.service._get('paper', first['id']))
        legacy.pop('person_id'); legacy.pop('person'); legacy['id'] = 'legacy'
        with self.service.transaction() as conn:
            self.service._put('paper', 'legacy', legacy, conn, False)
        # Duty/admin retain legacy (anonymous-era) paper visibility; duty stays read-only.
        legacy_list = self.service.list_papers(self.duty('A'), {'legacy': '1'})
        self.assertEqual(legacy_list['items'][0]['legacy'], True)
        self.assertEqual(self.service.profile(self.duty('A'), {'scope': 'A'})['summary']['answered'], 2)
        self.assert_status(403, self.dispatch, 'paper.answer', {'id': 'legacy'}, self.duty('A'))
        self.assertEqual(wrong['questions'][0]['attempt']['operator_id'], 'oid-p1')

    def test_roster_aliases_and_conflicts_do_not_merge_names_or_read_signatures(self):
        self.service._people_reader = lambda: {'sources': {'staff': {'ok': True}}, 'people': [
            {'person_key': 'external:e1', 'record_aliases': ['staff:s1', 'external:e1'], 'name': '同名', 'employee_no': '100', 'building': 'A楼', 'signature_token': 'secret'},
            {'person_key': 'staff:s2', 'record_aliases': ['staff:s2'], 'name': '同名', 'employee_no': '100', 'building': 'B楼'},
            {'person_key': 'staff:s3', 'name': '无楼栋', 'building': ''}]}
        personal.refresh_people(self.service)
        people = personal.people(self.service, ADMIN, {})
        self.assertEqual(people['total'], 2)
        self.assertEqual(len(people['issues']), 1)
        self.assertNotIn('secret', json.dumps(self.service._all('person')))
        identity = personal.people(self.service, ADMIN, {'scope': 'A'})['items'][0]['id']
        self.service._people_reader = lambda: {'people': [{'person_key': 'staff:s1', 'record_aliases': ['staff:s1'], 'name': '改名', 'building': 'C楼'}]}
        personal.refresh_people(self.service)
        self.assertEqual(personal.people(self.service, ADMIN, {'scope': 'C'})['items'][0]['id'], identity)

    def test_new_bank_normalizes_only_reliable_answers(self):
        def parse(**fields):
            return learning.normalize_question({'record_id': 'q', 'bank': 'supplemental', 'fields': {'题目': '设备测试', '专业': '暖通', **fields}})
        q = parse(选项='A. Alpha\nB. Beta', 答案='答：Alpha')
        self.assertEqual(q['type'], 'single'); self.assertEqual(q['problems'], [])
        self.assertEqual(q['specialty'], '暖通')
        self.assertEqual(parse(选项='A. Alpha\nB. Beta', 答案='BA')['type'], 'multiple')
        self.assertEqual(parse(选项='', 答案='参考内容')['type'], 'interview')
        self.assertFalse(parse(答案附件=[{'file_token': 'test', 'name': '答案.png'}])['problems'])
        self.assertTrue(parse(选项='无法解析的选项', 答案='Alpha')['problems'])
        self.assertTrue(parse(选项='A. Alpha\nB. Alpha', 答案='Alpha')['problems'])

    def test_partial_cloud_restore_does_not_reassign_consumed_reserve(self):
        self.prepare()
        first = self.claim()
        self.service.sync_pending(limit=100, force=True)
        self.cloud.entities.pop(('paper', first['id']))
        with TemporaryDirectory() as folder:
            restored = learning.LearningService(folder, self.cloud, self.sender)
            self.assert_status(503, restored.restore)
            self.assertFalse(restored._restored)

    def test_deleted_paper_preserves_dedup_window_and_results_audit(self):
        self.prepare(count=8)
        first = self.claim()
        self.answer(first)
        self.dispatch('paper.delete', {'id': first['id']}, ADMIN)
        self.assert_status(409, self.claim)
        self.assertEqual(self.service.profile(self.self_actor('p1'), {})['summary']['assigned'], 0)
        self.assertIsNotNone(self.service._get('record', first['id']))
        with patch.object(learning, 'now', return_value=CURRENT + dt.timedelta(days=1)):
            self.service.publish(DAY + dt.timedelta(days=1))
            self.assertEqual(self.claim()['shortage']['written'], 8)

    def run(self, *args, **kwargs):
        import threading
        self._real_thread_start = threading.Thread.start
        return super().run(*args, **kwargs)


if __name__ == '__main__':
    unittest.main()