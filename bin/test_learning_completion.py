"""Synthetic daily projections; no workers, sockets, personnel data or real sends."""
import copy
import datetime as dt
import json
import sys
import unittest
from contextlib import closing
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import Mock, patch

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

from bin import test_learning as support
from bin import test_learning_cloud as cloud_support
from lan_bitable_template_portal import learning, learning_completion as completion

DATE = support.DAY.isoformat()
AFTERNOON = support.CURRENT.replace(hour=14)
ADMIN = {**support.ADMIN, 'person_id': ''}


class CompletionTests(unittest.TestCase):
    setUp = support.LearningTests.setUp
    question = support.LearningTests.question
    seed = support.LearningTests.seed
    dispatch = support.LearningTests.dispatch

    def put(self, kind, value, dirty=False):
        with self.service.transaction() as conn:
            self.service._put(kind, value['id'], value, conn, dirty)
        return value

    def person(self, pid='p1', scopes=('A',), **extra):
        return self.put('person', {'id': pid, 'name': 'Synthetic ' + pid, 'scopes': list(scopes),
                                  'active': True, 'login_ids': ['oid-' + pid], **extra})

    def roster(self, people=(), date=DATE):
        from lan_bitable_template_portal.learning_reminders import ROSTER_VERSION
        with self.service.transaction() as conn:
            self.service._put('local', 'shift_roster:' + date,
                              {'date': date, 'version': ROSTER_VERSION, 'people': list(people)}, conn, False)

    def paper(self, pid='p1', scope='A', shift='白', date=DATE, mode='daily', identity='paper', count=2):
        person = self.service._get('person', pid) or self.person(pid, (scope,))
        questions = [self.question(n) for n in range(count)]
        self.seed(questions)
        return self.put('paper', {'id': identity, 'person_id': pid, 'person': person, 'scope': scope,
            'shift': shift, 'mode': mode, 'date': date, 'questions': questions, 'shortage': {},
            'created_at': date + 'T08:00:00+08:00'})

    def record(self, paper, *, answered=2, viewed=False, legacy=False, submitted=None, practice=()):
        entries = {}
        for question in paper['questions'][:answered]:
            first = {'correct': True, 'submitted_at': submitted or DATE + 'T14:00:00+08:00'}
            if not legacy:
                first['answer_view_snapshot'] = {'version': 1, 'answer_before_submit': viewed}
            entries[question['id']] = {'attempt': first, 'practice': list(practice)}
        return self.put('record', {'id': paper['id'], 'person_id': paper['person_id'], 'date': paper['date'],
                                   'scope': paper['scope'], 'version': 1, 'entries': entries})

    def rows(self, current=AFTERNOON, **kwargs):
        return {v['person_id']: v for v in completion.project(self.service, DATE, current=current, **kwargs)}

    def actor(self, pid='p1'):
        return {'id': 'oid-' + pid, 'person_id': pid, 'is_admin': False, 'shared_account': False, 'can_answer': True}

    def test_all_people_empty_verified_roster_not_reserve_assignment(self):
        self.person(); self.person('p2', ('B',)); self.person('inactive', active=False)
        self.put('publication', {'id': DATE, 'date': DATE, 'reserve_ids': ['reserve-all'], 'paper_ids': []})
        self.put('reserve', {'id': 'reserve-all', 'scope': 'A', 'date': DATE, 'questions': [self.question(1)]})
        self.assertEqual(set(self.rows()), {'p1', 'p2'})
        self.assertEqual(self.rows()['p1']['status'], '待核对')
        self.roster()
        for row in self.rows().values():
            self.assertEqual((row['status'], row['assigned']), ('未安排', 0))

    def test_roster_states_and_duplicate_rows_do_not_duplicate_required_work(self):
        self.person()
        duty = {'person_id': 'p1', 'scope': 'A', 'shift': '白'}
        self.roster([duty, duty])
        self.assertEqual(self.rows(current=support.CURRENT)['p1']['status'], '未到时')
        row = self.rows()['p1']
        self.assertEqual((row['status'], row['assigned'], len(row['tasks'])), ('待答', 15, 1))
        self.assertEqual(self.rows(current=AFTERNOON.replace(hour=19))['p1']['status'], '逾期')

    def test_name_only_and_duplicate_login_identity_never_assign(self):
        first = self.person()
        self.person('p2', name=first['name'], login_ids=first['login_ids'])
        self.roster([{'open_id': 'oid-p1', 'name': first['name'], 'scope': 'A', 'shift': '白'}])
        for row in self.rows().values():
            self.assertEqual((row['status'], row['assigned']), ('待核对', 0))
        self.roster([{'name': first['name'], 'scope': 'A', 'shift': '白'}])
        self.assertEqual(self.rows()['p1']['assigned'], 0)
        self.roster([{'person_id': 'p1', 'shift': '白'}])
        self.assertEqual(self.rows()['p1']['status'], '待核对')

    def test_two_scopes_or_shifts_never_share_completion_credit(self):
        self.person(scopes=('A', 'B'))
        self.roster([{'person_id': 'p1', 'scope': s, 'shift': '白'} for s in ('A', 'B')])
        row = self.rows()['p1']
        self.assertEqual(row['assigned'], 30)
        self.assertEqual(len({t['task_id'] for t in row['tasks']}), 2)
        paper = self.paper(scope='A', count=2); self.record(paper)
        self.assertEqual((self.rows()['p1']['assigned'], self.rows()['p1']['answered']), (17, 2))
        frozen = self.rows()['p1']
        self.put(completion.KIND, frozen)
        with self.service.transaction() as conn:
            conn.execute("DELETE FROM documents WHERE kind='local'")
        self.assertEqual(self.rows()['p1']['assigned'], 17)
        self.roster([{'person_id': 'p1', 'scope': 'A', 'shift': shift} for shift in ('白', '夜')])
        self.put('paper', {**paper, 'shift': ''})
        row = self.rows()['p1']
        self.assertEqual((row['status'], row['assigned'], row['answered']), ('待核对', 30, 0))

    def test_night_shift_roster_day_and_autonomous_actual_day(self):
        self.roster()
        paper = self.paper(shift='夜')
        next_day = (support.DAY + dt.timedelta(days=1)).isoformat()
        self.record(paper, submitted=next_day + 'T02:00:00+08:00')
        practice = self.paper(shift='', mode='practice', identity='practice')
        self.record(practice, answered=1, submitted=next_day + 'T03:00:00+08:00',
                    practice=[{'submitted_at': next_day + 'T04:00:00+08:00', 'correct': False}])
        row = self.rows(current=AFTERNOON + dt.timedelta(days=1))['p1']
        self.assertEqual((row['assigned'], row['answered'], row['status'], row['practice_questions']), (2, 2, '已完成', 0))
        following = completion.project(self.service, next_day, current=AFTERNOON + dt.timedelta(days=1))[0]
        self.assertEqual((following['assigned'], following['practice_questions'], following['practice_attempts'], following['review_count']), (0, 1, 2, 1))

    def test_first_results_not_recounted_by_reviews_and_send_failure_separate(self):
        self.roster()
        paper = self.paper()
        self.record(paper, answered=1, practice=[{'submitted_at': DATE + 'T15:00:00+08:00', 'correct': False}])
        row = self.rows()['p1']
        self.assertEqual((row['status'], row['answered'], row['correct'], row['review_count']), ('答题中', 1, 1, 1))
        self.record(paper)
        self.put('notification', {'id': 'notice', 'date': DATE, 'paper_id': paper['id'], 'status': 'failed'})
        row = self.rows()['p1']
        self.assertEqual((row['status'], row['accuracy'], row['delivery_failed']), ('已完成', 100.0, True))

    def test_server_start_marks_in_progress_without_inventing_answers(self):
        self.roster(); self.paper()
        self.dispatch('paper.start', {'id': 'paper', 'person_id': 'p1'}, self.actor())
        row = self.rows()['p1']
        self.assertEqual((row['status'], row['answered'], row['correct']), ('答题中', 0, 0))

    def test_legacy_assisted_unknown_not_answer_viewed_and_snapshots_immutable(self):
        self.roster()
        paper = self.paper(count=1)
        actor = self.actor()
        qid = paper['questions'][0]['id']
        record = {'id': paper['id'], 'date': DATE, 'scope': 'A', 'person_id': 'p1', 'version': 0,
                  'entries': {qid: {'assisted': True, 'revealed': DATE + 'T08:00:00+08:00'}}}
        self.put('record', record)
        self.dispatch('paper.reveal', {'id': paper['id'], 'question_id': qid, 'kind': 'hint'}, actor)
        self.dispatch('paper.answer', {'id': paper['id'], 'question_id': qid, 'option_ids': ['o0'], 'operation_id': 'first'}, actor)
        first = copy.deepcopy(self.service._get('record', paper['id'])['entries'][qid]['attempt'])
        self.assertTrue(first['answer_view_snapshot']['legacy_unknown'])
        self.assertEqual((self.rows()['p1']['answer_viewed'], self.rows()['p1']['answer_view_unknown']), (0, 1))
        self.dispatch('paper.reveal', {'id': paper['id'], 'question_id': qid, 'kind': 'answer'}, actor)
        self.dispatch('paper.answer', {'id': paper['id'], 'question_id': qid, 'option_ids': ['o0'], 'operation_id': 'redo', 'practice': True}, actor)
        saved = self.service._get('record', paper['id'])['entries'][qid]
        self.assertEqual(saved['attempt'], first)
        self.assertTrue(saved['practice'][0]['answer_view_snapshot']['answer_before_submit'])

    def test_new_explicit_answer_view_is_counted_not_analysis_or_hint(self):
        self.roster()
        paper = self.paper(count=2)
        for index, kind in enumerate(('analysis', 'answer')):
            qid = paper['questions'][index]['id']
            self.dispatch('paper.reveal', {'id': paper['id'], 'question_id': qid, 'kind': kind}, self.actor())
            self.dispatch('paper.answer', {'id': paper['id'], 'question_id': qid, 'option_ids': ['o0'], 'operation_id': kind}, self.actor())
        row = self.rows()['p1']
        self.assertEqual((row['answer_viewed'], row['answer_view_unknown']), (1, 0))

    def test_building_scope_filters_before_aggregation_and_no_raw_identity(self):
        self.roster()
        self.person(scopes=('A', 'B'))
        a = self.paper(scope='A', identity='a', count=1)
        self.record(a, answered=1)
        self.sharing(submitted=True, scope='B')
        duty = {'id': 'duty-a', 'scope': 'A', 'shared_account': True, 'is_admin': False}
        response = completion.report(self.service, duty, {'date': DATE})
        row = response['items'][0]
        self.assertEqual((row['assigned'], row['answered'], row['scopes']), (1, 1, ['A']))
        self.assertEqual(row['person']['scopes'], ['A'])
        self.assertNotIn('login_ids', json.dumps(response))
        self.assertNotIn('REFERENCE-SECRET', json.dumps(response))
        self.assertNotIn('oid-p1', json.dumps(response))
        with self.assertRaises(learning.LearningError):
            completion.report(self.service, duty, {'date': DATE, 'scope': 'B'})

    def test_self_only_and_read_is_side_effect_free(self):
        self.person(); self.person('p2', ('B',)); self.roster()
        before = self.service._all('local')
        self.service._shift_roster_reader = Mock(side_effect=AssertionError('GET must not refresh'))
        response = self.service.dispatch('completion', {}, self.actor(), {'date': DATE})
        self.assertEqual([r['person_id'] for r in response['items']], ['p1'])
        self.assertEqual(self.service._all('local'), before)
        self.assertEqual(self.cloud.calls, []); self.assertEqual(self.sender.calls, [])
        with self.assertRaises(learning.LearningError):
            completion.report(self.service, self.actor(), {'date': DATE, 'person_id': 'p2'})
        for query in ({'date': '2026-02-30'}, {'from': '2026-01-01', 'to': '2026-03-01'}, {'page': 'bad'}, {'scope': 'Z'}):
            with self.subTest(query=query), self.assertRaises(learning.LearningError):
                completion.report(self.service, ADMIN, query)

    def sharing(self, *, submitted=False, scope='A'):
        self.person(scopes=('A', 'B'))
        batch = self.put('sharing_batch', {'id': 'batch', 'status': 'published', 'date': DATE,
            'published_at': DATE + 'T13:00:00+08:00', 'questions': [{'id': 'q1', 'type': 'single'}, {'id': 'q2', 'type': 'interview'}]})
        self.put('sharing_attempt', {'id': 'sharing', 'batch_id': batch['id'], 'person_id': 'p1', 'scope': scope,
            'open_id': 'oid-p1', 'question_ids': ['q1', 'q2'], 'draft_answers': {'q1': ['A']},
            'submitted_at': DATE + 'T14:00:00+08:00' if submitted else '', 'correct': 1 if submitted else None,
            'delivery_status': 'sent'})

    def test_sharing_real_assignment_draft_not_score_and_no_invented_deadline(self):
        self.roster(); self.sharing()
        row = self.rows(current=AFTERNOON + dt.timedelta(days=2))['p1']
        self.assertEqual((row['status'], row['assigned'], row['answered'], row['accuracy']), ('答题中', 2, 0, None))
        self.sharing(submitted=True)
        row = self.rows()['p1']
        self.assertEqual((row['status'], row['answered'], row['objective_answered'], row['accuracy'], row['answer_viewed']), ('已完成', 2, 1, 100.0, 0))
        self.assertEqual(row['answer_view_unknown'], 2)
        attempt = self.service._get('sharing_attempt', 'sharing')
        self.put('sharing_attempt', {**attempt, 'answer_view_snapshot': {'version': 1, 'answer_before_submit': False}})
        self.assertEqual(self.rows()['p1']['answer_view_unknown'], 0)
        self.put('sharing_attempt', {**attempt, 'answer_view_snapshot': {'version': 1, 'answer_before_submit': False, 'legacy_unknown': True}})
        self.assertEqual(self.rows()['p1']['answer_view_unknown'], 2)
        self.assertEqual(self.rows(scope='B')['p1']['assigned'], 0)
        batch = self.service._get('sharing_batch', 'batch')
        self.put('sharing_batch', {**batch, 'status': 'draft'})
        self.assertEqual(self.rows()['p1']['assigned'], 0)

    def test_conflicting_sharing_assignments_or_scores_are_unknown_not_merged(self):
        self.roster(); self.sharing(submitted=True)
        attempt = self.service._get('sharing_attempt', 'sharing')
        self.put('sharing_attempt', {**attempt, 'correct': 99})
        self.assertEqual((self.rows()['p1']['status'], self.rows()['p1']['correct']), ('待核对', 0))
        self.put('sharing_attempt', attempt)
        self.put('sharing_attempt', {**attempt, 'id': 'conflict', 'open_id': 'another-id'})
        row = self.rows()['p1']
        self.assertEqual((row['status'], row['answered'], len(row['tasks'])), ('待核对', 0, 1))

    def test_explicit_sharing_close_is_cutoff_not_an_invented_daily_deadline(self):
        self.roster(); self.sharing()
        batch = self.service._get('sharing_batch', 'batch')
        self.put('sharing_batch', {**batch, 'status': 'closed', 'closed_at': DATE + 'T13:30:00+08:00'})
        self.assertEqual(self.rows()['p1']['status'], '逾期')
        self.put('sharing_batch', {**batch, 'status': 'closed'})
        self.assertEqual(self.rows()['p1']['status'], '待核对')

    def test_sharing_inactive_history_keeps_stable_person_id(self):
        self.roster(); self.sharing(submitted=True)
        self.person(active=False)
        self.assertEqual(self.rows()['p1']['answered'], 2)

    def test_stable_upsert_retries_and_daily_restore_not_score_index(self):
        self.roster(); paper = self.paper(); self.record(paper)
        self.service._restored = True; self.service._completion_enabled = True
        completion.refresh(self.service, AFTERNOON)
        row = self.service._get(completion.KIND, completion.key(DATE, 'p1'))
        self.assertTrue(completion.valid(row))
        completion.refresh(self.service, AFTERNOON + dt.timedelta(minutes=1))
        self.assertEqual(self.service._get(completion.KIND, row['id'])['_revision'], row['_revision'])
        self.cloud.fail_after_commit = 'upsert_entity'
        self.service.sync_pending(force=True)
        self.service.sync_pending(force=True)
        calls = [args for method, args in self.cloud.calls if method == 'upsert_entity' and args[0] == completion.KIND]
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0][-1], calls[1][-1])
        with closing(self.service._connect()) as conn:
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM learning_results').fetchone()[0], 2)
        other = learning.LearningService(self.root / 'restore', self.cloud, self.sender)
        other.restore()
        self.assertEqual(other._get(completion.KIND, row['id'])['answered'], 2)
        with closing(other._connect()) as conn:
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM learning_results').fetchone()[0], 0)

    def test_invalid_daily_restore_rolls_back_and_validates_nested_tasks(self):
        self.roster(); self.paper()
        row = self.rows()['p1']
        self.assertTrue(completion.valid(row))
        for field, value in (('tasks', [{}]), ('scopes', ['Z']), ('answered', -1), ('updated_at', 'bad')):
            bad = {**row, field: value}
            self.assertFalse(completion.valid(bad), field)
            self.cloud.entities = {('person', 'restore-only'): {'id': 'restore-only'}, (completion.KIND, bad['id']): bad}
            with self.assertRaises(learning.LearningError):
                self.service.restore()
            self.assertIsNone(self.service._get('person', 'restore-only'))

    def test_daily_upload_waits_for_raw_paper_and_record_confirmation(self):
        self.roster(); paper = self.paper(); record = self.record(paper)
        self.service._restored = True; self.service._completion_enabled = True
        completion.refresh(self.service, AFTERNOON)
        row = self.service._get(completion.KIND, completion.key(DATE, 'p1'))
        self.put('record', record, dirty=True)
        self.assertFalse(self.service._sync_document((completion.KIND, row['id'], row['_revision'])))
        self.assertEqual(self.cloud.calls, [])
        saved = self.service._get('record', record['id'])
        self.service._sync_document(('record', saved['id'], saved['_revision']))
        self.service._sync_document((completion.KIND, row['id'], row['_revision']))
        self.assertFalse(self.service._get(completion.KIND, row['id'])['_dirty'])

    def test_frozen_roster_survives_restore_missing_cache_and_no_unknown_backfill(self):
        self.person(); self.person('p2')
        self.roster([{'person_id': 'p1', 'scope': 'A', 'shift': '夜'}])
        row = self.rows()['p1']
        self.put(completion.KIND, row)
        with self.service.transaction() as conn:
            conn.execute("DELETE FROM documents WHERE kind='local'")
        self.assertEqual(self.rows()['p1']['assigned'], 15)
        self.service._restored = True; self.service._completion_enabled = True
        self.paper(date='2026-09-20')
        completion.refresh(self.service, AFTERNOON)
        self.assertIsNone(self.service._get(completion.KIND, completion.key('2026-09-20', 'p2')))

    def test_failed_roster_read_never_reuses_frozen_not_scheduled_verdict(self):
        self.person(); self.roster()
        frozen = self.rows()['p1']
        self.assertEqual(frozen['status'], '未安排')
        self.put(completion.KIND, frozen)
        with self.service.transaction() as conn:
            conn.execute("DELETE FROM documents WHERE kind='local'")
        self.service._restored = True; self.service._completion_enabled = True
        self.service._shift_roster_reader = Mock(side_effect=OSError('synthetic roster unavailable'))
        completion.refresh(self.service, AFTERNOON)
        row = completion.report(self.service, self.actor(), {'date': DATE})['items'][0]
        self.assertEqual(row['status'], '待核对')
        self.assertFalse(row['roster_verified'])
        self.assertEqual(self.cloud.calls, [])
        self.assertEqual(self.sender.calls, [])
        self.roster()
        self.assertEqual(self.rows()['p1']['status'], '未安排')

    def test_pending_hook_no_cloud_and_answer_requests_same_transaction(self):
        self.paper(count=1)
        self.service._completion_enabled = True
        self.service.completion_changed(DATE, 'bad')
        self.assertEqual(self.service._get('local', 'completion_pending:' + DATE)['date'], DATE)
        self.assertIsNone(self.service._get('local', 'completion_pending:bad'))
        self.assertEqual(self.cloud.calls, [])
        self.dispatch('paper.answer', {'id': 'paper', 'question_id': 'written:0', 'option_ids': ['o0'], 'operation_id': 'answer'}, self.actor())
        self.assertEqual(self.service._get('local', 'completion_pending:' + DATE)['date'], DATE)

    def test_refresh_does_not_lose_concurrent_historical_recompute_request(self):
        self.paper(count=1); self.roster()
        self.service._completion_enabled = True; self.service._restored = True
        self.service.completion_changed(DATE)
        original = completion.project

        def concurrent(*args, **kwargs):
            rows = original(*args, **kwargs)
            if args[1] == DATE:
                self.service.completion_changed(DATE)
            return rows

        with patch.object(completion, 'project', side_effect=concurrent):
            completion.refresh(self.service, AFTERNOON + dt.timedelta(days=3))
        self.assertIsNotNone(self.service._get('local', 'completion_pending:' + DATE))
        completion.refresh(self.service, AFTERNOON + dt.timedelta(days=3))
        self.assertIsNone(self.service._get('local', 'completion_pending:' + DATE))


class CompletionRouteDateTests(unittest.TestCase):
    def setUp(self):
        from lan_bitable_template_portal.learning_routes import install_learning_routes

        folder = TemporaryDirectory(prefix='completion_dates_')
        self.addCleanup(folder.cleanup)
        self.cloud, self.sender = support.FakeCloud(enabled=False), support.FakeSender()
        self.service = learning.LearningService(folder.name, self.cloud, self.sender)
        self.session = {'user': {'open_id': 'synthetic-admin'}, 'role': 'admin'}
        controller = SimpleNamespace(
            _current_session=lambda request: self.session,
            _auth_required_response=lambda: JSONResponse({'ok': False}, status_code=401),
            _json_ok=lambda request, session, data: JSONResponse({'ok': True, 'data': data}),
        )
        runtime = SimpleNamespace(auth_manager=SimpleNamespace(is_admin=lambda session: session.get('role') == 'admin'))
        for guard in (
            patch.object(learning, 'LearningService', return_value=self.service),
            patch.object(learning, 'now', return_value=AFTERNOON),
            patch.object(self.service, 'start', side_effect=AssertionError('No service workers')),
            patch('socket.create_connection', side_effect=AssertionError('No network')),
            patch('socket.getaddrinfo', side_effect=AssertionError('No DNS')),
            patch.dict(sys.modules, {'lan_bitable_template_portal.portal_service': SimpleNamespace(
                BUILDING_OPEN_ID_MAP={scope: 'synthetic-building-' + scope for scope in 'ABCDEH'})}),
        ):
            guard.start()
            self.addCleanup(guard.stop)
        app = FastAPI()
        install_learning_routes(app, controller, runtime)
        self.client = TestClient(app)  # No lifespan, therefore no workers or cloud startup.
        self.addCleanup(self.client.close)

    def get(self, params):
        return self.client.get('/api/learning/completion', params=params)

    def test_real_route_passes_scalar_query_to_real_service(self):
        query = {'from': '2024-02-29', 'to': '2024-02-29', 'scope': 'A', 'person_id': 'synthetic-p1',
                 'status': '已完成', 'shift': '白', 'q': 'Synthetic', 'page': '2', 'page_size': '10'}
        with patch.object(self.service, 'dispatch', wraps=self.service.dispatch) as dispatch:
            response = self.get(query)
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(dispatch.call_args.args[3], query)
        data = response.json()['data']
        self.assertEqual((data['from'], data['to'], data['page'], data['page_size']),
                         ('2024-02-29', '2024-02-29', 2, 10))
        self.assertEqual(self.cloud.calls, [])
        self.assertEqual(self.sender.calls, [])
        self.assertEqual(self.service._all('local'), [])

    def test_real_route_rejects_invalid_and_explicit_empty_dates(self):
        for value in ('', '02/29/2024', '2024-2-29', '2026-02-29', '1900-02-29', '2100-02-29',
                      '2024-04-31', '0000-01-01', '2024-02-29T00:00:00Z', ' 2024-02-29'):
            for field in ('from', 'to', 'date'):
                with self.subTest(value=value, field=field):
                    response = self.get({field: value})
                    self.assertEqual(response.status_code, 400, response.text)
                    self.assertEqual(response.json()['error'], '请选择有效统计日期。')

    def test_real_route_keeps_inclusive_31_day_limit(self):
        for end, status in (('2024-03-02', 200), ('2024-03-03', 400), ('2024-01-31', 400)):
            with self.subTest(end=end):
                response = self.get({'from': '2024-02-01', 'to': end})
                self.assertEqual(response.status_code, status, response.text)

    def test_real_route_keeps_missing_date_default_and_last_query_value(self):
        response = self.get({})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()['data']['from'], DATE)
        response = self.get([('from', 'bad'), ('from', '2024-02-29'), ('to', '2024-02-29')])
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()['data']['from'], '2024-02-29')
        response = self.get([('from', '2024-02-29'), ('from', 'bad'), ('to', '2024-02-29')])
        self.assertEqual(response.status_code, 400, response.text)

    def test_real_route_keeps_permissions(self):
        for session, query, status in (
            (None, {'date': DATE}, 401),
            ({'user': {'open_id': 'synthetic-guest'}, 'role': 'guest'}, {'date': DATE}, 403),
            ({'user': {'open_id': 'synthetic-building-A'}}, {'date': DATE, 'scope': 'B'}, 403),
            ({'user': {'open_id': 'synthetic-unmapped'}}, {'date': DATE}, 403),
        ):
            with self.subTest(session=session):
                self.session = session
                response = self.get(query)
                self.assertEqual(response.status_code, status, response.text)


class CompletionCloudTests(unittest.TestCase):
    new_remote = cloud_support.LearningCloudTests.new_remote

    def setUp(self):
        cloud_support.LearningCloudTests.setUp(self)
        self.cloud = self.remote

    def test_schema_explicit_only_original_json_and_idempotent_projection(self):
        self.cloud.enabled = True
        self.cloud.ensure_schema()
        table = self.cloud._entity_table_id
        original = {f['field_name'] for f in self.fake.fields[table]}
        self.assertEqual(original, set(cloud_support.cloud.ENTITY_FIELDS))
        start = len(self.fake.mutations())
        self.assertFalse(self.cloud.ensure_completion_schema()['ready'])
        self.assertEqual(len(self.fake.mutations()), start)
        with self.assertRaises(cloud_support.cloud.LearningCloudError):
            self.cloud.ensure_completion_schema(create=True, expected_table_id='wrong-table')
        self.assertEqual(len(self.fake.mutations()), start)
        self.assertTrue(self.cloud.ensure_completion_schema(create=True, expected_table_id=table)['ready'])
        self.assertTrue(all(method == 'POST' and resource == f'tables/{table}/fields'
                            for method, resource, *_ in self.fake.mutations()[start:]))
        self.assertEqual(self.fake.records[table], [])
        self.assertTrue(original <= {f['field_name'] for f in self.fake.fields[table]})
        value = {'schema_version': 1, 'id': completion.key(DATE, 'p'), 'person_id': 'p', 'date': DATE,
            'person': {'id': 'p', 'name': 'Synthetic', 'scopes': ['A'], 'active': True}, 'scopes': ['A'],
            'tasks': [], 'status': '未安排', 'assigned': 0, 'answered': 0, 'correct': 0, 'objective_answered': 0,
            'answer_viewed': 0, 'answer_view_unknown': 0, 'practice_questions': 0, 'practice_attempts': 0,
            'review_count': 0, 'accuracy': None, 'updated_at': AFTERNOON.isoformat()}
        for _ in range(2):
            self.cloud.upsert_entity(completion.KIND, value['id'], value, 'same-operation')
        rows = self.fake.records[table]
        self.assertEqual(len(rows), 1)
        fields = rows[0]['fields']
        self.assertEqual(json.loads(fields['内容']), value)
        self.assertEqual(fields['类别'], completion.KIND)
        self.assertEqual(fields['人员标识'], 'p')
        self.assertEqual(fields['完成状态'], '未安排')
        restored = self.cloud.load_entities()
        self.assertEqual(restored[0]['payload'], value)

    def test_daily_cloud_missing_columns_retains_entity_and_bad_types_fail_closed(self):
        self.cloud.enabled = True
        self.cloud.ensure_schema()
        with self.assertRaises(cloud_support.cloud.LearningCloudError):
            self.cloud.upsert_entity(completion.KIND, 'daily', {'id': 'daily'}, 'op')
        self.assertFalse(any(self.fake.records.values()))
        self.fake.fields[self.cloud._entity_table_id].append({'field_name': '日期', 'type': 1})
        with self.assertRaises(cloud_support.cloud.LearningCloudError):
            self.cloud.ensure_completion_schema()

    def test_material_manual_bank_sync_keeps_origin_without_required_meeting_link(self):
        from bin.test_learning_sharing import typed_questions
        from lan_bitable_template_portal.learning_sharing import validate_questions
        questions = typed_questions(['single'], 5)
        for question in questions:
            question.update(origin='manual', evidence='')
        questions = validate_questions(questions, '')
        batch = {'id': '1' * 32, 'status': 'published', 'title': 'Synthetic material', 'url': '',
                 'questions': questions, 'source': {'text': '', 'sources': [{'id': 'file1', 'title': 'Synthetic.pdf',
                      'sha256': 'a' * 64, 'text': 'private source text not copied to metadata'}]}}
        cloud = cloud_support.cloud
        self.fake.fields[cloud.SHARING_TABLE_ID] = [{'field_name': n, 'type': t} for n, t in cloud.SHARING_FIELDS.items()]
        self.fake.records[cloud.SHARING_TABLE_ID] = []
        self.cloud.save_sharing_question(batch, questions[0], 'manual-operation')
        self.cloud.save_sharing_question(batch, questions[0], 'manual-operation')
        rows = self.fake.records[cloud.SHARING_TABLE_ID]
        self.assertEqual(len(rows), 1)
        metadata = json.loads(rows[0]['fields'][cloud.META_FIELD])
        self.assertEqual(metadata['origin'], 'manual')
        self.assertEqual(metadata['sources'][0]['id'], 'file1')
        self.assertNotIn('private source text', json.dumps(metadata))


if __name__ == '__main__':
    unittest.main()
