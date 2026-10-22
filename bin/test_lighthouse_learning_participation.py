"""Offline complete participation: real SQLite, native permissions and assistant reads."""
import asyncio
import copy
import datetime as dt
import hashlib
import json
import sqlite3
import socket
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from contextlib import closing
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

BIN = Path(__file__).resolve().parent
sys.path.insert(0, str(BIN))

import test_learning as fixtures
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient
from lan_bitable_template_portal import learning, learning_personal, learning_routes
from openclaw_service.assistant.lighthouse_agent import PortalAgent
from openclaw_service.assistant.lighthouse_api import PortalAPICatalog
from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_sources import (
    collect_learning_participation, learning_participation_reply, learning_participation_request,
)
from bin import test_lighthouse_stream as streaming

MONDAY = dt.datetime(2026, 10, 19, 10, tzinfo=learning.TZ)
ADMIN = {'id': 'ou_admin', 'is_admin': True, 'scope': '', 'person_id': '', 'shared_account': False}


class ParticipationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='learning_participation_')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.cloud = fixtures.FakeCloud(enabled=False)
        self.sender = fixtures.FakeSender()
        self.service = learning.LearningService(self.root, self.cloud, self.sender)
        connect = socket.socket.connect

        def local_only(sock, address):
            if address[0] not in {'127.0.0.1', '::1', 'localhost'}:
                raise AssertionError('tests must remain offline')
            return connect(sock, address)

        guard = patch('socket.socket.connect', local_only)
        guard.start()
        self.addCleanup(guard.stop)

    def prepare(self):
        self.service._restored = True
        self.clock = patch('lan_bitable_template_portal.learning.now', return_value=MONDAY)
        self.now = self.clock.start()
        self.addCleanup(self.clock.stop)

    def person(self, pid, name=None, scopes=('A',)):
        value = {'id': pid, 'name': name or pid, 'scopes': list(scopes), 'active': True,
                 'login_ids': ['ou_' + pid]}
        with self.service.transaction() as conn:
            self.service._put('person', pid, value, conn, False)
        return value

    def paper(self, identity, pid, *, assigned='2026-10-12', submitted='2026-10-14',
              scope='A', correct=True, practice=(), pending=False, invalid=False, needs_review=False, interview=False):
        question = fixtures.LearningTests.question(self, identity, kind='interview' if interview else 'single', invalid=invalid)
        person = self.service._get('person', pid) or {'id': pid, 'name': pid, 'scopes': [scope]}
        value = {'id': identity, 'date': assigned, 'mode': 'practice', 'person_id': pid, 'person': person,
                 'scope': scope, 'questions': [question], 'shortage': {}}
        with self.service.transaction() as conn:
            self.service._put('paper', identity, value, conn, False)
            if not pending:
                attempt = {'submitted_at': submitted + 'T09:00:00+08:00', 'correct': None if interview else correct}
                self.service._put('record', identity, {'id': identity, 'entries': {question['id']: {
                    'attempt': attempt, 'needs_review': needs_review,
                    'practice': [{'submitted_at': day + 'T11:00:00+08:00', 'operation_id': str(i), 'correct': True}
                                 for i, day in enumerate(practice)]}}}, conn, False)

    def report(self, actor=None, **query):
        return learning_personal.participation(self.service, actor or ADMIN, query)

    def test_full_week_over_40_papers_and_pending_are_not_participants(self):
        self.prepare()
        for n in range(15):
            self.person('p' + str(n), '人员' + str(n), scopes=('A' if n < 8 else 'B',))
            for i in range(5):
                self.paper(f'p{n}-{i}', 'p' + str(n), scope='A' if n < 8 else 'B')
        for i in range(21):
            self.paper('pending' + str(i), 'pending_person', pending=True)
        self.paper('today', 'today_person', submitted='2026-10-19')
        report = self.report(page=1, page_size=40)
        self.assertEqual((report['from'], report['to']), ('2026-10-12', '2026-10-18'))
        self.assertEqual(report['summary']['participants'], 15)
        self.assertEqual(report['summary']['answered'], 75)
        self.assertEqual(len(report['participants']), 15)
        self.assertTrue(report['complete'])
        self.assertFalse(report['truncated'])
        self.assertEqual(self.cloud.calls, [])
        self.assertEqual(self.sender.calls, [])

    def test_submit_date_not_paper_date_and_repeat_attempts(self):
        self.prepare()
        self.person('first')
        self.person('repeat')
        self.paper('older-paper', 'first', assigned='2026-10-01', submitted='2026-10-12',
                   correct=False, practice=('2026-10-15', '2026-10-19'))
        self.paper('future-answer', 'first', submitted='2026-10-19')
        self.paper('practice-only', 'repeat', assigned='2026-10-01', submitted='2026-10-02', practice=('2026-10-18',))
        self.paper('pending', 'never', pending=True)
        self.paper('invalid', 'invalid-person', invalid=True)
        self.paper('review', 'review-person', needs_review=True)
        report = self.report()
        self.assertEqual(report['summary']['participants'], 2)
        self.assertEqual(report['summary']['answered'], 1)
        self.assertEqual(report['summary']['practice'], 2)
        self.assertEqual(report['summary']['answer_attempts'], 3)
        self.assertEqual(report['summary']['correct'], 0)
        self.assertEqual(report['summary']['wrong'], 1)
        self.assertEqual(report['summary']['accuracy'], 0)

    def test_same_person_across_buildings_is_one_person_and_names_do_not_merge(self):
        self.prepare()
        self.person('p1', '同名')
        self.person('p2', '同名')
        self.paper('a', 'p1', scope='A')
        self.paper('b', 'p1', scope='B')
        self.paper('other', 'p2', scope='A', interview=True)
        report = self.report()
        self.assertEqual(report['summary']['participants'], 2)
        self.assertEqual(report['summary']['answered'], 3)
        self.assertEqual(report['summary']['choices'], 2)
        self.assertEqual(report['summary']['interviews'], 1)
        self.assertEqual(report['participants'][0]['scopes'], ['A'])
        self.assertEqual(report['participants'][1]['scopes'], ['A', 'B'])

    def test_native_self_and_duty_permissions(self):
        self.prepare()
        self.person('self', scopes=('A',))
        self.person('other', scopes=('B',))
        self.paper('self-history', 'self', scope='B')
        self.paper('other-history', 'other', scope='B')
        actor = {'id': 'ou_self', 'person_id': 'self', 'scope': '', 'shared_account': False}
        report = self.report(actor)
        self.assertEqual([row['person_id'] for row in report['participants']], ['self'])
        self.assertTrue(report['self_only'])
        with self.assertRaises(learning.LearningError):
            self.report(actor, person_id='other')
        duty = {'id': 'dutyA', 'scope': 'A', 'shared_account': True}
        self.assertEqual(self.report(duty)['summary']['participants'], 0)
        with self.assertRaises(learning.LearningError):
            self.report(duty, scope='B')
        with self.assertRaises(learning.LearningError):
            self.report({'id': 'unbound'})

    def test_missing_person_uses_snapshot_without_removing_historical_answers(self):
        self.prepare()
        self.person('old', '历史人员')
        self.paper('history', 'old')
        with self.service.transaction() as conn:
            conn.execute("DELETE FROM documents WHERE kind='person'")
        report = self.report()
        self.assertEqual(report['participants'][0]['name'], '历史人员')
        self.assertTrue(report['names_complete'])
        with self.service.transaction() as conn:
            paper = self.service._get('paper', 'history', conn)
            paper['person'] = {}
            self.service._put('paper', 'history', paper, conn, False)
        self.cloud.entities['person', 'old'] = {'id': 'old', 'name': '', 'scopes': ['A']}
        report = self.report()
        self.assertEqual(report['summary']['participants'], 1)
        self.assertFalse(report['names_complete'])
        self.assertEqual(report['summary']['unresolved_names'], 1)

    def test_date_boundaries_and_bad_ranges(self):
        self.prepare()
        for date, expected in (
            ('2026-10-19', ('2026-10-12', '2026-10-18')),
            ('2026-10-25', ('2026-10-12', '2026-10-18')),
            ('2027-01-04', ('2026-12-28', '2027-01-03')),
        ):
            self.now.return_value = dt.datetime.fromisoformat(date).replace(tzinfo=learning.TZ)
            report = self.report()
            self.assertEqual((report['from'], report['to']), expected)
        for query in ({'from': '2026-10-12'}, {'period': 'yesterday'},
                      {'from': '2026-10-20', 'to': '2026-10-12'},
                      {'from': '2026-01-01', 'to': '2028-01-01'}):
            with self.subTest(query=query), self.assertRaises(learning.LearningError):
                self.report(**query)

    def test_read_failure_and_unready_cache_are_not_zero_reports(self):
        self.prepare()
        self.service._restored = False
        with patch.object(self.cloud, 'load_entities', side_effect=OSError('offline')):
            with self.assertRaisesRegex(learning.LearningError, '未按零人统计'):
                self.report()
        self.service._restored = True
        with patch.object(learning_personal, '_participation_readonly', side_effect=sqlite3.OperationalError('busy')):
            with self.assertRaises(sqlite3.OperationalError):
                self.report()

    def test_read_is_side_effect_free_and_snapshot_survives_restart(self):
        self.prepare()
        self.person('p1')
        self.paper('p1-paper', 'p1')
        with self.service.transaction() as conn:
            self.service._put('local', 'refresh', {'at': '2026-10-19T09:00:00+08:00'}, conn, False)
        with closing(self.service._connect()) as conn:
            before = list(conn.iterdump())
        original = self.report()
        self.service = learning.LearningService(self.root, self.cloud, self.sender)
        restored = self.report()
        self.assertEqual(original, restored)
        with closing(self.service._connect()) as conn:
            self.assertEqual(before, list(conn.iterdump()))
        self.assertEqual(self.cloud.calls, [])

    def test_route_reuses_authenticated_identity_and_reports_complete_local_data(self):
        self.prepare()
        self.person('p1')
        self.paper('p1-paper', 'p1')
        session = {'user': {'open_id': 'ou_admin'}, 'role': 'admin'}
        controller = SimpleNamespace(_current_session=lambda request: session,
            _auth_required_response=lambda: JSONResponse({'ok': False}, status_code=401),
            _json_ok=lambda request, session, data: JSONResponse({'ok': True, 'data': data}))
        runtime = SimpleNamespace(auth_manager=SimpleNamespace(is_admin=lambda value: value['role'] == 'admin'))
        app = FastAPI()
        with patch.object(learning, 'LearningService', return_value=self.service), patch.dict(sys.modules, {
                'lan_bitable_template_portal.portal_service': SimpleNamespace(BUILDING_OPEN_ID_MAP={}),
                'lan_bitable_template_portal.learning_sharing_routes': SimpleNamespace(install=Mock())}):
            learning_routes.install_learning_routes(app, controller, runtime)
            client = TestClient(app)
            self.addCleanup(client.close)
            response = client.get('/api/learning/profile', params={'period': 'last_week', 'view': 'participation'})
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json()['data']['summary']['answered'], 1)
            descriptor = PortalAPICatalog(app).get('GET /api/learning/profile')
            self.assertTrue(descriptor['read_only'])
            self.assertIn('view', descriptor['schema']['query'])
            session['role'] = 'guest'
            self.assertEqual(client.get('/api/learning/profile?view=participation').status_code, 403)
        self.assertEqual(self.cloud.calls, [])

    def test_assistant_reads_all_buildings_without_pagination_and_deduplicates(self):
        self.prepare()
        for n in range(55):
            pid = 'p' + str(n)
            self.person(pid, '姓名' + str(n))
            self.paper(pid, pid, scope='A' if n < 48 else 'B')
        self.paper('shared-person', 'p0', scope='B')
        calls = []

        async def invoke(operation):
            calls.append(operation)
            raw = self.report(**operation['params'])
            return {'ok': True, '_raw': raw}

        report = asyncio.run(collect_learning_participation(
            {'modules': ['learning'], 'scopes': ['A', 'B'], 'learning_scopes': ['A', 'B']}, {'period': 'last_week', 'scope': 'ALL'}, invoke))
        self.assertEqual(len(calls), 2)
        self.assertTrue(all(call['api_id'] == 'GET /api/learning/profile' and call['params']['view'] == 'participation' for call in calls))
        self.assertEqual(report['summary']['participants'], 55)
        self.assertEqual(report['summary']['answered'], 56)
        reply = learning_participation_reply(report)
        for n in range(55):
            self.assertIn('| 姓名' + str(n) + ' |', reply)
        self.assertNotIn('翻页', reply)
        result = PortalAgent._public_references({'ok': True, 'api_id': calls[0]['api_id'], '_raw': report}, {})
        self.assertEqual(len(result['data']['participants']), 55)
        legacy = PortalAgent._public_references({'ok': True, '_raw': {'participants': report['participants']}}, {})
        self.assertEqual(len(legacy['data']['participants']), 40)

    def test_personal_assistant_does_not_repeat_same_native_self_history(self):
        self.prepare()
        self.person('p1')
        self.paper('p1-paper', 'p1', scope='B')
        calls = []

        async def invoke(operation):
            calls.append(operation)
            return {'ok': True, '_raw': self.report({'id': 'ou_p1', 'person_id': 'p1'}, **operation['params'])}

        report = asyncio.run(collect_learning_participation({'modules': ['learning'], 'scopes': ['A', 'B'], 'learning_scopes': ['A', 'B']},
                                {'period': 'last_week', 'scope': 'ALL'}, invoke))
        self.assertEqual(len(calls), 1)
        self.assertEqual(report['summary']['answered'], 1)
        self.assertIn('本人', learning_participation_reply(report))

    def test_partial_scope_failure_and_duplicate_or_truncated_list_fail_closed(self):
        self.prepare()
        self.person('p1')
        self.paper('p1-paper', 'p1')
        actor = {'modules': ['learning'], 'scopes': ['A', 'B'], 'learning_scopes': ['A', 'B']}
        for bad in ({'ok': False, 'status': 504}, {'ok': True, '_raw': {**self.report(), 'complete': False}},
                    {'ok': True, '_raw': {**self.report(), 'participants': []}},
                    {'ok': True, '_raw': {**self.report(), 'summary': {**self.report()['summary'], 'answered': 99}}}):
            async def invoke(operation):
                return {'ok': True, '_raw': self.report(scope='A')} if operation['params']['scope'] == 'A' else bad
            with self.subTest(bad=bad), self.assertRaises(AssistantError):
                asyncio.run(collect_learning_participation(actor, {'scope': 'ALL'}, invoke))
        no_call = Mock(side_effect=AssertionError('must reject before reading'))
        with self.assertRaises(AssistantError):
            asyncio.run(collect_learning_participation(actor, {'scope': 'H'}, no_call))

    def test_question_routing_is_narrow_and_supports_multiple_buildings(self):
        self.assertEqual(learning_participation_request('上周哪些人员参与答题 每周学练'),
                         {'period': 'last_week', 'scope': 'ALL'})
        self.assertEqual(learning_participation_request('本周A楼和B楼有哪些人参与答题')['scope'], 'A,B')
        self.assertEqual(learning_participation_request('上星期全楼哪些人参与答题')['scope'], 'ALL')
        for question in ('上周哪些人未答题', '上周哪些人没做题', '发送上周每周学练汇总', '上周有哪些事件', '开始今日答题'):
            with self.subTest(question=question):
                self.assertIsNone(learning_participation_request(question))

    def test_participation_rejects_partial_transport_and_pagination_metadata(self):
        self.prepare()
        self.person('p1')
        self.paper('p1-paper', 'p1')
        report = self.report(scope='A')
        actor = {'modules': ['learning'], 'scopes': ['A'], 'learning_scopes': ['A']}
        results = [
            {'ok': True, 'data': report, 'truncated': True},
            {'ok': True, '_raw': report, 'scope_warning': 'incomplete'},
            {'ok': True, '_raw': {**report, 'has_more': True}},
            {'ok': True, '_raw': {**report, 'total': 55}},
            {'ok': True, '_raw': {**report, 'total': 0}},
            {'ok': True, '_raw': {**self.report(scope='B'), 'scope': 'A', 'total': 55}},
        ]
        for result in results:
            with self.subTest(result=result), self.assertRaises(AssistantError):
                asyncio.run(collect_learning_participation(actor, {'scope': 'A'}, AsyncMock(return_value=result)))
        # Display truncation is harmless when the full raw report is retained.
        result = asyncio.run(collect_learning_participation(actor, {'scope': 'A'},
            AsyncMock(return_value={'ok': True, '_raw': report, 'truncated': True})))
        self.assertEqual(result['summary']['participants'], 1)

    def test_pending_and_inconsistent_attempt_counts_cannot_be_participation(self):
        self.prepare()
        self.person('p1')
        self.paper('p1-paper', 'p1')
        actor = {'modules': ['learning'], 'scopes': ['A'], 'learning_scopes': ['A']}
        for changes in (
            {'answered': 0, 'practice': 0, 'answer_attempts': 0, 'choices': 0, 'correct': 0},
            {'answer_attempts': 99}, {'choices': 2}, {'correct': 0, 'wrong': 0},
        ):
            report = self.report(scope='A')
            report['participants'][0].update(changes)
            report['summary'].update(changes)
            with self.subTest(changes=changes), self.assertRaises(AssistantError):
                asyncio.run(collect_learning_participation(actor, {'scope': 'A'},
                    AsyncMock(return_value={'ok': True, '_raw': report})))

    def test_relative_day_uses_beijing_calendar_for_offset_clock(self):
        clock = dt.datetime(2026, 10, 18, 17, tzinfo=dt.timezone.utc)
        self.assertEqual(learning_participation_request('今天哪些人答题', now=clock),
                         {'from': '2026-10-19', 'to': '2026-10-19', 'scope': 'ALL'})

    def test_local_offset_history_is_normalized_without_changing_business_index(self):
        self.prepare()
        for pid, submitted in (('last-week', '2026-10-11T16:30:00Z'),
                               ('next-week', '2026-10-18T16:30:00+00:00')):
            self.person(pid)
            self.paper(pid, pid, practice=('2026-10-15',))
            with self.service.transaction() as conn:
                record = self.service._get('record', pid, conn)
                entry = next(iter(record['entries'].values()))
                entry['attempt']['submitted_at'] = submitted
                entry['practice'][0]['submitted_at'] = submitted
                self.service._put('record', pid, record, conn, False)
        with closing(learning_personal._participation_readonly(self.service.db)) as conn:
            before = list(conn.iterdump())
        report = self.report()
        self.assertEqual([row['person_id'] for row in report['participants']], ['last-week'])
        self.assertEqual(report['summary']['answered'], 1)
        self.assertEqual(report['summary']['practice'], 1)
        with closing(learning_personal._participation_readonly(self.service.db)) as conn:
            self.assertEqual(list(conn.iterdump()), before)
        self.assertEqual(self.cloud.calls, [])

    def test_missing_record_document_does_not_erase_authoritative_local_index(self):
        self.prepare()
        self.person('p1')
        self.paper('p1-paper', 'p1')
        with self.service.transaction() as conn:
            conn.execute("DELETE FROM documents WHERE kind='record'")
        report = self.report()
        self.assertEqual(report['summary']['answered'], 1)
        self.assertEqual(report['summary']['participants'], 1)
        self.assertEqual(self.cloud.calls, [])

    def test_legacy_chat_first_question_is_complete_without_calling_model(self):
        self.prepare()
        self.person('p1', '测试人员')
        self.paper('p1-paper', 'p1')
        for channel in ('', 'feishu:fixture'):
            state = {'id': 'conversation', 'turns': []}
            saved = {}
            assistant = Mock()
            assistant._lock = threading.RLock()
            assistant._state.return_value = state
            assistant._selected.return_value = None
            assistant.model_for.return_value.profile.return_value = {'id': 'test', 'name': 'test'}
            assistant._finish.side_effect = lambda actor, value: saved.update(copy.deepcopy(value))
            assistant.conversation.side_effect = lambda actor: copy.deepcopy(saved)
            files = Mock()
            files.context.return_value = []
            portal = PortalAgent(assistant, Mock(), files)

            async def invoke(actor, operation, request):
                return {'ok': True, '_raw': self.report(**operation['params'])}

            portal._invoke = AsyncMock(side_effect=invoke)
            answer = asyncio.run(portal.chat({'id': 'admin', 'is_admin': True, 'channel': channel,
                'modules': ['learning'], 'scopes': ['A'], 'learning_scopes': ['A']}, {
                'question': '上周哪些人员参与答题 每周学练', 'conversation_id': 'conversation',
                'operation_id': 'participation_test_0001'}, None))
            self.assertEqual(answer['turns'][0]['status'], 'completed')
            self.assertIn('测试人员', answer['turns'][0]['answer'])
            self.assertIn('2026-10-12 至 2026-10-18', answer['turns'][0]['answer'])
            assistant._finish.assert_called_once()
            assistant._compress.assert_not_called()
            assistant.model_for.return_value.respond.assert_not_called()

    def test_legacy_failure_preserves_question_and_finishes_without_success(self):
        portal = PortalAgent(Mock(), Mock(), Mock())
        portal.learning_participation_answer = AsyncMock(side_effect=AssistantError('资料不完整', 503))
        portal._save_chat = Mock()
        turn = {'question': '上周哪些人员参与答题 每周学练'}
        with self.assertRaises(AssistantError):
            asyncio.run(portal._answer_learning_participation({'id': 'p1'}, {}, turn, {}, None))
        self.assertEqual(turn['status'], 'failed')
        self.assertEqual(turn['question'], '上周哪些人员参与答题 每周学练')
        self.assertNotIn('answer', turn)
        portal._save_chat.assert_called_once_with({'id': 'p1'}, {}, finish=True)


class ParticipationReadCacheTests(unittest.TestCase):
    setUp = ParticipationTests.setUp
    prepare = ParticipationTests.prepare
    person = ParticipationTests.person
    paper = ParticipationTests.paper
    report = ParticipationTests.report

    def cloud_snapshot(self, *, clear=True):
        with self.service.transaction() as conn:
            self.cloud.entities = {(row['kind'], row['key']): json.loads(row['payload']) for row in conn.execute(
                "SELECT kind,key,payload FROM documents WHERE kind IN ('person','paper','record')")}
            if clear:
                for table in ('documents', 'learning_usage', 'learning_results', 'learning_practice'):
                    conn.execute('DELETE FROM ' + table)
        self.service._restored = False

    def native_dump(self):
        with closing(learning_personal._participation_readonly(self.service.db)) as conn:
            return list(conn.iterdump())

    def test_complete_local_and_authoritative_zero_never_read_cloud(self):
        self.prepare()
        self.person('p1')
        self.paper('p1-paper', 'p1')
        with patch.object(self.cloud, 'load_entities', side_effect=AssertionError('local completeness must short circuit')):
            self.assertEqual(self.report()['summary']['answered'], 1)
            self.assertEqual(self.report(scope='B')['summary']['participants'], 0)
        self.assertFalse((self.root / 'assistant-participation.sqlite3').exists())

    def test_readonly_cloud_projection_uses_native_stats_without_business_writes(self):
        self.prepare()
        self.person('p1', '人员甲')
        self.paper('p1-paper', 'p1', assigned='2026-10-01', correct=False, practice=('2026-10-16',))
        self.paper('pending', 'never', pending=True)
        self.cloud_snapshot()
        person = self.cloud.entities['person', 'p1']
        person.update(password='PASSWORD-MUST-NOT-PERSIST', phone='PHONE-MUST-NOT-PERSIST',
                      login_ids=['OPENID-MUST-NOT-PERSIST'], secret='SECRET-MUST-NOT-PERSIST')
        paper = self.cloud.entities['paper', 'p1-paper']
        paper['questions'][0].update(stem='QUESTION-CONTENT-MUST-NOT-PERSIST',
            attachments=[{'file_token': 'ATTACHMENT-MUST-NOT-PERSIST'}])
        record = self.cloud.entities['record', 'p1-paper']
        record['entries'][paper['questions'][0]['id']]['attempt']['text'] = 'ANSWER-TEXT-MUST-NOT-PERSIST'
        before = self.native_dump()
        database_paths = [Path(str(self.service.db) + suffix) for suffix in ('', '-wal')]
        hashes = {path: hashlib.sha256(path.read_bytes()).digest() for path in database_paths if path.exists()}
        with patch.object(self.service, 'restore', side_effect=AssertionError('no restore')), \
                patch.object(self.service, 'tick', side_effect=AssertionError('no tick')), \
                patch.object(self.service, 'start', side_effect=AssertionError('no start')), \
                patch.object(self.service, '_put', side_effect=AssertionError('no business/outbox writes')), \
                patch.object(self.cloud, 'ensure_schema', side_effect=AssertionError('no schema writes')):
            report = self.report()
        self.assertEqual(report['summary']['participants'], 1)
        self.assertEqual(report['summary']['answered'], 1)
        self.assertEqual(report['summary']['practice'], 1)
        self.assertEqual(report['summary']['wrong'], 1)
        self.assertEqual(report['participants'][0]['name'], '人员甲')
        self.assertEqual(before, self.native_dump())
        self.assertEqual(hashes, {path: hashlib.sha256(path.read_bytes()).digest()
                                 for path in database_paths if path.exists()})
        self.assertFalse(self.service._restored)
        self.assertFalse(self.service._restore_requested)
        self.assertEqual([call[0] for call in self.cloud.calls], ['load_entities'])
        content = (self.root / 'assistant-participation.sqlite3').read_bytes()
        for marker in (b'PASSWORD-', b'PHONE-', b'OPENID-', b'SECRET-', b'QUESTION-CONTENT-', b'ATTACHMENT-', b'ANSWER-TEXT-'):
            self.assertNotIn(marker, content)
        with closing(learning_personal._participation_readonly(self.root / 'assistant-participation.sqlite3')) as cached:
            self.assertEqual(self.service._get('paper', 'pending', cached)['questions'], [])
        self.service = learning.LearningService(self.root, self.cloud, self.sender)
        self.assertEqual(self.report()['summary'], report['summary'])
        self.assertEqual(len(self.cloud.calls), 1)

    def test_name_only_gap_does_not_import_remote_answer_counts(self):
        self.prepare()
        self.person('p1', '云端姓名')
        self.paper('p1-paper', 'p1')
        self.cloud_snapshot(clear=False)
        self.service._restored = True
        with self.service.transaction() as conn:
            conn.execute("DELETE FROM documents WHERE kind='person'")
            paper = self.service._get('paper', 'p1-paper', conn)
            paper['person'] = {}
            self.service._put('paper', 'p1-paper', paper, conn, False)
        other = copy.deepcopy(self.cloud.entities['paper', 'p1-paper'])
        other.update(id='remote-only', person_id='other')
        self.cloud.entities['paper', 'remote-only'] = other
        self.cloud.entities['record', 'remote-only'] = {**copy.deepcopy(self.cloud.entities['record', 'p1-paper']), 'id': 'remote-only'}
        before = self.native_dump()
        report = self.report()
        self.assertEqual(report['summary']['participants'], 1)
        self.assertEqual(report['summary']['answered'], 1)
        self.assertEqual(report['participants'][0]['name'], '云端姓名')
        self.assertEqual(before, self.native_dump())

    def test_local_new_record_empty_record_and_tombstone_override_cached_history(self):
        self.prepare()
        self.person('p1', '旧姓名')
        self.paper('p1-paper', 'p1')
        self.cloud_snapshot()
        self.assertEqual(self.report()['summary']['answered'], 1)
        paper = copy.deepcopy(self.cloud.entities['paper', 'p1-paper'])
        record = copy.deepcopy(self.cloud.entities['record', 'p1-paper'])
        paper['scope'] = 'B'
        record['entries'][paper['questions'][0]['id']]['attempt']['correct'] = False
        with self.service.transaction() as conn:
            self.service._put('person', 'p1', {'id': 'p1', 'name': '本地新姓名', 'scopes': ['B']}, conn, False)
            self.service._put('paper', 'p1-paper', paper, conn, False)
            self.service._put('record', 'p1-paper', record, conn, False)
        report = self.report(scope='B')
        self.assertEqual(report['participants'][0]['name'], '本地新姓名')
        self.assertEqual(report['summary']['wrong'], 1)
        self.assertEqual(self.report(scope='A')['summary']['participants'], 0)
        with self.service.transaction() as conn:
            self.service._put('record', 'p1-paper', {'id': 'p1-paper', 'entries': {}}, conn, False)
        self.assertEqual(self.report()['summary']['answered'], 0)
        with self.service.transaction() as conn:
            paper['deleted_at'] = 'deleted'
            self.service._put('paper', 'p1-paper', paper, conn, False)
        self.assertEqual(self.report()['summary']['answered'], 0)
        self.assertEqual(len(self.cloud.calls), 1)

    def test_name_fill_failure_keeps_reliable_local_counts_and_known_names(self):
        self.prepare()
        self.person('known', '已知姓名')
        self.paper('known-paper', 'known')
        self.person('unknown', '缺姓名', scopes=('B',))
        self.paper('unknown-paper', 'unknown', scope='B', practice=('2026-10-15',))
        with self.service.transaction() as conn:
            conn.execute("DELETE FROM documents WHERE kind='person' AND key='unknown'")
            paper = self.service._get('paper', 'unknown-paper', conn)
            paper['person'] = {}
            self.service._put('paper', 'unknown-paper', paper, conn, False)
        before = self.native_dump()
        with patch.object(self.cloud, 'load_entities', side_effect=OSError('offline')) as fetch:
            report = self.report()
            self.assertTrue(report['complete'])
            self.assertFalse(report['names_complete'])
            self.assertTrue(report['name_fill_failed'])
            self.assertEqual(report['summary']['participants'], 2)
            self.assertEqual(report['summary']['answered'], 2)
            self.assertEqual(report['summary']['answer_attempts'], 3)
            self.assertEqual(report['summary']['unresolved_names'], 1)
            self.assertIn('已知姓名', [row['name'] for row in report['participants']])

            async def invoke(operation):
                return {'ok': True, '_raw': self.report(**operation['params'])}

            merged = asyncio.run(collect_learning_participation(
                {'modules': ['learning'], 'scopes': ['A', 'B'], 'learning_scopes': ['A', 'B']},
                {'period': 'last_week', 'scope': 'ALL'}, invoke))
            reply = learning_participation_reply(merged)
            self.assertIn('实际参与 **2 人**', reply)
            self.assertIn('已知姓名', reply)
            self.assertIn('补名暂未成功', reply)
            self.assertIn('暂不能提供完整实名名单', reply)
            self.assertEqual(fetch.call_count, 1)
            self.service._restored = False
            with self.assertRaisesRegex(learning.LearningError, '未按零人统计'):
                self.report()
        self.assertEqual(before, self.native_dump())
        self.assertFalse((self.root / 'assistant-participation.sqlite3').exists())

    def test_unknown_empty_cloud_can_only_degrade_names_not_unready_statistics(self):
        self.prepare()
        self.person('unknown')
        self.paper('unknown-paper', 'unknown')
        with self.service.transaction() as conn:
            conn.execute("DELETE FROM documents WHERE kind='person'")
            paper = self.service._get('paper', 'unknown-paper', conn)
            paper['person'] = {}
            self.service._put('paper', 'unknown-paper', paper, conn, False)
        self.assertEqual(self.cloud.entities, {})
        report = self.report()
        self.assertTrue(report['complete'])
        self.assertTrue(report['name_fill_failed'])
        self.assertFalse(report['names_complete'])
        self.assertEqual(report['summary']['answered'], 1)
        self.service._restored = False
        with self.assertRaisesRegex(learning.LearningError, '未按零人统计'):
            self.report()

    def test_concurrent_gaps_share_one_read_and_reuse_snapshot_even_when_names_missing(self):
        self.prepare()
        self.person('p1')
        self.paper('p1-paper', 'p1')
        self.cloud_snapshot()
        self.cloud.entities['person', 'p1']['name'] = ''
        self.cloud.entities['paper', 'p1-paper']['person']['name'] = ''
        original = self.cloud.load_entities
        entered, release = threading.Event(), threading.Event()

        def load():
            entered.set()
            if not release.wait(10):
                raise AssertionError('test release timeout')
            return original()

        with patch.object(self.cloud, 'load_entities', side_effect=load) as fetch, ThreadPoolExecutor(max_workers=8) as pool:
            futures = [pool.submit(self.report) for _ in range(8)]
            self.assertTrue(entered.wait(10))
            release.set()
            reports = [future.result(timeout=20) for future in futures]
        self.assertEqual(fetch.call_count, 1)
        self.assertTrue(all(report['summary']['answered'] == 1 for report in reports))
        self.assertTrue(all(not report['names_complete'] for report in reports))

    def test_failed_fill_keeps_old_cache_and_does_not_retry_for_each_waiter(self):
        self.prepare()
        self.person('p1')
        self.paper('p1-paper', 'p1')
        self.cloud_snapshot()
        self.report()
        path = self.root / 'assistant-participation.sqlite3'
        before = path.read_bytes()
        self.now.return_value = MONDAY + dt.timedelta(minutes=10)
        self.cloud.fail_once = 'load_entities'
        for _ in range(3):
            with self.assertRaisesRegex(learning.LearningError, '未按零人统计'):
                self.report()
        self.assertEqual(len(self.cloud.calls), 2)
        self.assertEqual(path.read_bytes(), before)
        later = time.monotonic() + 61
        with patch('lan_bitable_template_portal.learning_personal.time.monotonic', return_value=later):
            self.assertEqual(self.report()['summary']['answered'], 1)
        self.assertEqual(len(self.cloud.calls), 3)

    def test_cloud_partial_answer_snapshot_is_not_a_valid_zero(self):
        self.prepare()
        self.person('p1')
        self.paper('p1-paper', 'p1')
        self.cloud_snapshot()
        del self.cloud.entities['paper', 'p1-paper']
        with self.assertRaisesRegex(learning.LearningError, '未按零人统计'):
            self.report()

    def test_verified_remote_zero_is_reusable_but_never_overrides_new_local_data(self):
        self.prepare()
        self.person('pending-person')
        self.paper('pending-paper', 'pending-person', pending=True)
        self.cloud_snapshot()
        self.assertEqual(self.report()['summary']['participants'], 0)
        self.assertEqual(self.report()['summary']['participants'], 0)
        self.assertEqual(len(self.cloud.calls), 1)
        self.person('p1', '本地新增')
        self.paper('p1-paper', 'p1')
        self.assertEqual(self.report()['summary']['answered'], 1)
        with self.service.transaction() as conn:
            conn.execute("DELETE FROM documents WHERE kind='paper'")
        self.assertEqual(self.report()['summary']['answered'], 1)
        self.assertEqual(len(self.cloud.calls), 1)

    def test_empty_cloud_response_without_entity_table_is_unknown_not_zero(self):
        self.prepare()
        self.service._restored = False
        self.assertEqual(self.cloud.entities, {})
        with self.assertRaisesRegex(learning.LearningError, '未按零人统计'):
            self.report()
        self.assertFalse((self.root / 'assistant-participation.sqlite3').exists())

    def test_missing_answered_question_preserves_previous_valid_cache(self):
        self.prepare()
        self.person('p1')
        self.paper('p1-paper', 'p1')
        self.cloud_snapshot()
        self.report()
        path = self.root / 'assistant-participation.sqlite3'
        before = path.read_bytes()
        self.now.return_value = MONDAY + dt.timedelta(minutes=10)
        other_question = {**self.cloud.entities['paper', 'p1-paper']['questions'][0], 'id': 'different-question'}
        later = time.monotonic() + 61
        for index, questions in enumerate(([], [other_question])):
            with self.subTest(questions=questions), patch(
                    'lan_bitable_template_portal.learning_personal.time.monotonic', return_value=later + index * 61):
                self.cloud.entities['paper', 'p1-paper']['questions'] = questions
                with self.assertRaisesRegex(learning.LearningError, '未按零人统计'):
                    self.report()
                self.assertEqual(before, path.read_bytes())

    def test_local_becoming_complete_during_remote_read_wins_over_remote_snapshot(self):
        self.prepare()
        self.person('old', '旧云端人员')
        self.paper('old-paper', 'old')
        self.cloud_snapshot()
        original = self.cloud.load_entities

        def load():
            result = original()
            self.person('new', '本地新人员')
            self.paper('new-paper', 'new')
            self.service._restored = True
            return result

        with patch.object(self.cloud, 'load_entities', side_effect=load):
            report = self.report()
        self.assertEqual(report['summary']['participants'], 1)
        self.assertEqual(report['participants'][0]['name'], '本地新人员')

    def test_readthrough_cache_still_obeys_native_person_and_building_filters(self):
        self.prepare()
        self.person('own', '本人', scopes=('A',))
        self.person('other', '其他人', scopes=('B',))
        self.paper('own-paper', 'own')
        self.paper('other-paper', 'other', scope='B')
        self.cloud_snapshot()
        actor = {'id': 'ou_self', 'person_id': 'own', 'shared_account': False}
        report = self.report(actor)
        self.assertEqual([row['name'] for row in report['participants']], ['本人'])
        report = self.report({'id': 'ou_duty', 'shared_account': True, 'scope': 'B'})
        self.assertEqual([row['name'] for row in report['participants']], ['其他人'])
        self.assertEqual(len(self.cloud.calls), 1)
        with self.assertRaises(learning.LearningError):
            self.report(actor, person_id='other')

    def test_readonly_projection_normalizes_answer_and_practice_dates_to_beijing(self):
        self.prepare()
        self.person('p1')
        self.paper('p1-paper', 'p1', practice=('2026-10-15', '2026-10-18'))
        self.person('next-week')
        self.paper('next-week-paper', 'next-week')
        self.cloud_snapshot()
        first = next(iter(self.cloud.entities['record', 'p1-paper']['entries'].values()))
        first['attempt']['submitted_at'] = '2026-10-11T16:30:00Z'
        first['practice'][0]['submitted_at'] = '2026-10-11T16:45:00+00:00'
        first['practice'][1]['submitted_at'] = '2026-10-18T16:30:00Z'
        last = next(iter(self.cloud.entities['record', 'next-week-paper']['entries'].values()))
        last['attempt']['submitted_at'] = '2026-10-18T16:30:00Z'
        before = self.native_dump()
        report = self.report()
        self.assertEqual([row['person_id'] for row in report['participants']], ['p1'])
        self.assertEqual(report['summary']['answered'], 1)
        self.assertEqual(report['summary']['practice'], 1)
        self.assertEqual(self.native_dump(), before)

    def test_old_projection_cache_is_refetched_before_reuse(self):
        self.prepare()
        self.person('p1')
        self.paper('p1-paper', 'p1')
        self.cloud_snapshot()
        self.report()
        path = self.root / 'assistant-participation.sqlite3'
        for version in ('2', None):
            with closing(sqlite3.connect(path)) as conn, conn:
                if version is None:
                    conn.execute("DELETE FROM metadata WHERE key='projection_version'")
                else:
                    conn.execute("UPDATE metadata SET value=? WHERE key='projection_version'", (version,))
            self.assertEqual(self.report()['summary']['participants'], 1)
        self.assertEqual(len(self.cloud.calls), 3)

    def test_native_paginated_reader_and_shared_cache_keep_all_55_people(self):
        from lan_bitable_template_portal.learning_cloud import LearningCloud

        self.prepare()
        for n in range(55):
            pid = 'p' + str(n)
            self.person(pid)
            self.paper(pid, pid, scope='A' if n < 48 else 'B')
        for n in range(20):
            self.paper('pending' + str(n), 'pending', pending=True)
        self.cloud_snapshot()
        entities = [{'record_id': 'rec' + str(n), 'fields': {'类别': kind, '业务键': key, '内容': json.dumps(payload)}}
                    for n, ((kind, key), payload) in enumerate(self.cloud.entities.items())]
        pages = [{'items': entities[:150], 'has_more': True, 'page_token': 'fixture-next', 'total': len(entities)},
                 {'items': entities[150:], 'has_more': False, 'total': len(entities)}]
        native = LearningCloud(enabled=False, http_client=Mock())
        before = self.native_dump()
        with patch.object(self.service, '_cloud', native), \
                patch.object(native, '_find_entity_table', return_value='fixture'), \
                patch.object(native, '_headers', side_effect=AssertionError('must not access credentials')), \
                patch.object(native, '_request', side_effect=pages) as read:
            async def invoke(operation):
                return {'ok': True, '_raw': self.report(**operation['params'])}
            report = asyncio.run(collect_learning_participation(
                {'modules': ['learning'], 'scopes': ['A', 'B'], 'learning_scopes': ['A', 'B']},
                {'period': 'last_week', 'scope': 'ALL'}, invoke))
            self.assertEqual(report['summary']['participants'], 55)
            self.assertEqual(report['summary']['answered'], 55)
            self.assertEqual(len(report['participants']), 55)
            self.assertEqual(read.call_count, 2)
            self.assertEqual(read.call_args_list[1].args[3]['page_token'], 'fixture-next')
            path = self.root / 'assistant-participation.sqlite3'
            cached = path.read_bytes()
            failures = [
                [pages[0], OSError('fixture second page failed')],
                [{'items': entities[:1], 'has_more': False, 'total': 55}],
                [pages[0], {**pages[1], 'total': len(entities) + 1}],
                [pages[0], {**pages[1], 'items': [{**entities[150], 'record_id': entities[0]['record_id']}, *entities[151:]]}],
            ]
            tick = time.monotonic()
            for n, failure in enumerate(failures):
                self.now.return_value = MONDAY + dt.timedelta(minutes=10 + n)
                read.side_effect = failure
                with self.subTest(failure=n), patch.object(learning_personal.time, 'monotonic', return_value=tick + 61 * n), \
                        self.assertRaisesRegex(learning.LearningError, '未按零人统计'):
                    self.report()
                self.assertEqual(path.read_bytes(), cached)
        self.assertEqual(before, self.native_dump())
        self.assertEqual(self.sender.calls, [])


class ParticipationStreamTests(unittest.IsolatedAsyncioTestCase):
    person = ParticipationTests.person
    paper = ParticipationTests.paper

    async def asyncSetUp(self):
        from bin.test_feishu_topic_delegation import Auth
        from clipflow_backend.main import FastAPIPortalController
        from contextlib import asynccontextmanager
        from lan_bitable_template_portal import portal_auth
        from lan_bitable_template_portal.lighthouse_bridge import actor_for, PortalAuthority
        from openclaw_service.assistant.lighthouse_model import LighthouseModel
        from upload_event_module.services.feishu_app_migration import TARGET_APP_ID

        await streaming.StreamTests.asyncSetUp(self)
        self.addAsyncCleanup(self.runtime.close)
        self.settings = SimpleNamespace(app_id=TARGET_APP_ID, app_secret='fixture-current-secret')
        self.enterContext(patch.object(portal_auth, 'config', self.settings))
        self.root = Path(self.tmp.name) / 'learning'
        self.cloud, self.sender = fixtures.FakeCloud(enabled=False), fixtures.FakeSender()
        self.service = learning.LearningService(self.root, self.cloud, self.sender)
        self.service._restored = True
        clock = patch('lan_bitable_template_portal.learning.now', return_value=MONDAY)
        clock.start()
        self.addCleanup(clock.stop)
        class LearningAuth(Auth):
            _account_profile = Auth.account_profile
            account_profile = portal_auth.PortalAuthManager.account_profile

            def _account_read_state(self):
                return None

        self.auth = LearningAuth()
        for identity in ('ou_Admin', 'ou_User', 'ou_Duty'):
            self.auth.add(identity, ['A', 'B'] if identity == 'ou_Admin' else ['A'], ['assistant', 'learning'])
        self.auth.profiles['ou_Admin']['role'] = 'admin'
        self.auth.profiles['ou_Duty']['category'] = 'duty'
        self.auth._account_directory_provider = lambda: {
            'schema': 4, 'app_id': TARGET_APP_ID, 'loaded_at': time.time(),
            'people': {'rec' + identity: {
                'id': 'rec' + identity, 'name': identity, 'login_ids': [identity],
                'selectable': identity not in self.auth.inactive, 'inactive': identity in self.auth.inactive,
            } for identity in self.auth.profiles},
        }
        self.auth.directory_person = portal_auth.PortalAuthManager.directory_person.__get__(self.auth)
        from lan_bitable_template_portal import portal_service
        building = patch.object(portal_service, 'BUILDING_OPEN_ID_MAP', {'A': 'ou_Duty'})
        building.start()
        self.addCleanup(building.stop)
        self.identity = 'ou_Admin'
        self.guest = False
        self.backend = SimpleNamespace(auth_manager=self.auth, learning_service=self.service,
                                       state_store=self.store, service=SimpleNamespace())
        self.enterContext(patch('clipflow_backend.main.PortalRuntime', self.backend))
        self.controller = object.__new__(FastAPIPortalController)
        self.controller._json_ok = lambda request, session, data: JSONResponse({'ok': True, 'data': data})

        def session(_token):
            if self.identity not in self.auth.profiles or self.auth._open_id_explicitly_disabled(self.identity):
                return None
            return {'user': {'open_id': self.identity}, 'is_guest': self.guest,
                    'role': self.auth.account_profile(self.identity)['role']}

        self.auth.get_session = session
        factory = patch.object(learning, 'LearningService', return_value=self.service)
        factory.start()
        self.addCleanup(factory.stop)
        learning_routes.install_learning_routes(self.app, self.controller, self.backend)
        self.portal.catalog = PortalAPICatalog(self.app)

        self.participation_params = {'period': 'last_week'}
        @asynccontextmanager
        async def scripted_model(*args):
            from pydantic_ai.models.function import DeltaToolCall, FunctionModel
            report, reference = None, ''
            async def script(messages, info):
                nonlocal report, reference
                if report is None:
                    if not any(getattr(part, 'tool_name', '') == 'learning_participants' for message in messages for part in message.parts):
                        yield {0: DeltaToolCall(name='learning_participants', json_args=json.dumps(self.participation_params))}
                        return
                    receipt = messages[-1].parts[0].content
                    report, reference = copy.deepcopy(receipt['data']), receipt['query_ref']
                elif getattr(messages[-1].parts[0], 'tool_name', '') == 'read_query':
                    report['participants'].extend(messages[-1].parts[0].content['data']['items'])
                if len(report['participants']) < report['summary']['participants']:
                    yield {0: DeltaToolCall(name='read_query', json_args=json.dumps({
                        'query_ref': reference, 'path': ['participants'], 'offset': len(report['participants']), 'limit': 40}))}
                    return
                yield learning_participation_reply(report)
            yield FunctionModel(stream_function=script)

        self.engine = LighthouseModel(self.portal, model_factory=scripted_model)
        self.runtime.engine = self.engine
        self.actor_for = actor_for
        self.actor = await actor_for(self.controller, self.backend, self.request)
        self.authority = PortalAuthority(self.app, self.controller, self.backend,
                                        SimpleNamespace(root=self.root, lease='fixture', instance='fixture'))

        async def invoke(actor, operation, request, **kwargs):
            return await self.authority.invoke({'request': request}, actor, {'operation': operation})

        self.portal._invoke = AsyncMock(side_effect=invoke)
        self.person('self', '本人')
        with self.service.transaction() as conn:
            person = self.service._get('person', 'self', conn)
            person['login_ids'] = ['ou_User']
            self.service._put('person', 'self', person, conn, False)

    payload = streaming.StreamTests.payload
    finish = streaming.StreamTests.finish

    async def authorize(self):
        return await self.actor_for(self.controller, self.backend, self.request)

    async def submit(self, question):
        return await self.runtime.submit(self.actor, self.payload(question), self.request, self.authorize)

    async def test_today_repeat_and_short_followup_use_model_selected_authoritative_tool(self):
        self.participation_params = {'start_date': '2026-10-19', 'end_date': '2026-10-19'}
        from openclaw_service.assistant import lighthouse_sources
        original = lighthouse_sources.learning_participation_request
        self.person('today', '今天的参与人')
        self.paper('today-paper', 'today', submitted='2026-10-19')
        self.person('yesterday', '昨天的参与人')
        self.paper('yesterday-paper', 'yesterday', submitted='2026-10-18')
        with patch.object(lighthouse_sources, 'learning_participation_request',
                          side_effect=lambda question, previous=None: original(question, previous, now=MONDAY)):
            for channel in ('', 'feishu:oc_Group'):
                self.request.state.lighthouse_channel = channel
                self.actor = await self.authorize()
                for question in ('今天答题的都有哪些?', '有哪些?', '今天答题的都有哪些?'):
                    ack = await self.submit(question)
                    await self.finish()
                    run = self.runtime.get_run(self.actor, ack['run_id'])
                    self.assertEqual(run['status'], 'completed', run.get('error'))
                    self.assertIn('实际参与 **1 人**', run['answer'])
                    self.assertIn('今天的参与人', run['answer'])
                    self.assertNotIn('昨天的参与人', run['answer'])
        self.assertFalse(self.sender.calls)

    async def test_today_topic_followup_uses_same_authorized_complete_query(self):
        self.participation_params = {'start_date': '2026-10-19', 'end_date': '2026-10-19'}
        from openclaw_service.assistant import lighthouse_sources
        original = lighthouse_sources.learning_participation_request
        self.person('today-topic', '话题中的答题人员')
        self.paper('today-topic-paper', 'today-topic', submitted='2026-10-19')
        await self.topic()
        with patch.object(lighthouse_sources, 'learning_participation_request',
                          side_effect=lambda question, previous=None: original(question, previous, now=MONDAY)):
            for question in ('今天答题的都有哪些?', '有哪些?'):
                ack = await self.submit(question)
                await self.finish()
                run = self.runtime.get_run(self.actor, ack['run_id'])
                self.assertEqual(run['status'], 'completed', run.get('error'))
                self.assertIn('实际参与 **1 人**', run['answer'])
                self.assertIn('话题中的答题人员', run['answer'])

    async def test_web_and_feishu_first_question_via_real_stream_and_native_profile(self):
        for n in range(55):
            pid = 'p' + str(n)
            self.person(pid, '人员' + str(n))
            self.paper(pid, pid, scope='A' if n < 48 else 'B')
        self.paper('pending', 'never', pending=True)
        self.paper('today', 'today', submitted='2026-10-19')
        for channel in ('', 'feishu:oc_Group'):
            self.request.state.lighthouse_channel = channel
            self.actor = await self.authorize()
            self.assertEqual(self.actor['learning_scopes'], list('ABCDEH'))
            self.assertEqual(self.actor['_business_write_access']['learning_scopes'], ['A', 'B'])
            ack = await self.submit('上周哪些人员参与答题 每周学练')
            await self.finish()
            run = self.runtime.get_run(self.actor, ack['run_id'])
            self.assertEqual(run['status'], 'completed', run.get('error'))
            answer = (await self.runtime.conversation(self.actor))['turns'][-1]['answer']
            self.assertIn('实际参与 **55 人**', answer)
            self.assertIn('2026-10-12 至 2026-10-18', answer)
            for n in range(55):
                self.assertIn('| 人员' + str(n) + ' |', answer)
            self.assertNotIn('翻页', answer)
            source = self.assistant._state(self.actor)['turns'][-1]['sources'][0]
            self.assertEqual(len(source['data']['participants']), 55)
        self.assertEqual(self.cloud.calls, [])
        self.assertEqual(self.sender.calls, [])

    async def test_stream_personal_and_duty_share_reads_but_retain_native_grants(self):
        self.person('other', '他人', scopes=('B',))
        self.paper('own-answer', 'self')
        self.paper('other-answer', 'other', scope='B')
        for identity in ('ou_User', 'ou_Duty'):
            self.identity = identity
            before = copy.deepcopy(self.auth.profiles)
            self.actor = await self.authorize()
            self.assertEqual(self.actor['learning_scopes'], list('ABCDEH'))
            self.assertEqual(self.actor['_business_write_access']['learning_scopes'], ['A'])
            if identity == 'ou_User':
                self.assertEqual(self.actor['learning_person_id'], 'self')
            ack = await self.submit('上周哪些人员参与答题 每周学练')
            await self.finish()
            run = self.runtime.get_run(self.actor, ack['run_id'])
            self.assertEqual(run['status'], 'completed', run.get('error'))
            self.assertIn('本人', run['answer'])
            self.assertIn('他人', run['answer'])
            self.assertIn('实际参与 **2 人**', run['answer'])
            self.assertEqual(self.auth.profiles, before)

    async def test_real_stream_fills_missing_index_without_business_restore_or_writes(self):
        self.person('p1', '云端答题人员')
        self.paper('p1-paper', 'p1')
        ParticipationReadCacheTests.cloud_snapshot(self)
        before = ParticipationReadCacheTests.native_dump(self)
        with patch.object(self.service, 'restore', side_effect=AssertionError('no business restore')), \
                patch.object(self.service, '_put', side_effect=AssertionError('no business writes')):
            ack = await self.submit('上周哪些人员参与答题 每周学练')
            await self.finish()
        run = self.runtime.get_run(self.actor, ack['run_id'])
        self.assertEqual(run['status'], 'completed', run.get('error'))
        self.assertIn('云端答题人员', run['answer'])
        self.assertIn('助手只读补缺缓存', run['answer'])
        self.assertEqual(before, ParticipationReadCacheTests.native_dump(self))
        self.assertEqual([call[0] for call in self.cloud.calls], ['load_entities'])
        self.assertFalse(self.service._restored)

    async def test_real_stream_name_fill_failure_keeps_local_answer_but_unready_data_fails(self):
        self.person('known', '可核实名字')
        self.paper('known-paper', 'known')
        self.paper('missing-name-paper', 'unknown', scope='B')
        with self.service.transaction() as conn:
            paper = self.service._get('paper', 'missing-name-paper', conn)
            paper['person'] = {}
            self.service._put('paper', paper['id'], paper, conn, False)
        before = ParticipationReadCacheTests.native_dump(self)
        with patch.object(self.cloud, 'load_entities', side_effect=OSError('offline')) as fetch:
            self.request.state.lighthouse_channel = 'feishu:oc_Group'
            self.actor = await self.authorize()
            ack = await self.submit('上周哪些人员参与答题 每周学练')
            await self.finish()
            run = self.runtime.get_run(self.actor, ack['run_id'])
            self.assertEqual(run['status'], 'completed', run.get('error'))
            self.assertIn('实际参与 **2 人**', run['answer'])
            self.assertIn('首次作答 **2 题**', run['answer'])
            self.assertIn('可核实名字', run['answer'])
            self.assertIn('补名暂未成功', run['answer'])
            source = self.assistant._state(self.actor)['turns'][-1]['sources'][0]['data']
            self.assertFalse(source['names_complete'])
            self.assertTrue(source['name_fill_failed'])
            self.service._restored = False
            ack = await self.submit('上周哪些人员参与答题 每周学练')
            await self.finish()
            run = self.runtime.get_run(self.actor, ack['run_id'])
            self.assertEqual(run['status'], 'failed')
            self.assertNotIn('实际参与', run.get('answer', ''))
            self.assertEqual(fetch.call_count, 1)
        self.assertEqual(before, ParticipationReadCacheTests.native_dump(self))

    async def test_linked_accounts_without_learning_grant_or_own_paper_can_read_shared_results(self):
        self.paper('own', 'self')
        for identity, modules in (('ou_Admin', ['assistant']), ('ou_Low', [])):
            self.identity = identity
            self.auth.profiles[identity]['modules'] = modules
            self.actor = await self.authorize()
            ack = await self.submit('上周哪些人员参与答题 每周学练')
            await self.finish()
            run = self.runtime.get_run(self.actor, ack['run_id'])
            self.assertEqual(run['status'], 'completed', run.get('error'))
            self.assertIn('实际参与 **1 人**', run['answer'])
            self.assertEqual(self.auth.account_profile(identity)['modules'], modules)
            self.assertEqual((await self.runtime.conversation(self.actor))['turns'][-1]['question'], '上周哪些人员参与答题 每周学练')

    async def test_missing_guest_and_disabled_login_cannot_start_shared_learning_query(self):
        for identity, guest, disabled in (('', False, False), ('ou_Unknown', False, False),
                                          ('ou_User', True, False), ('ou_User', False, True)):
            self.identity, self.guest = identity, guest
            if disabled:
                self.auth.disabled.add(identity)
            with self.subTest(identity=identity, guest=guest, disabled=disabled), self.assertRaises(AssistantError):
                await self.authorize()
        self.portal._invoke.assert_not_awaited()

    async def topic(self):
        from bin.test_feishu_topic_delegation import TopicDelegationTests
        from lan_bitable_template_portal.lighthouse_bridge import PortalAuthority
        from lan_bitable_template_portal.portal_auth import TopicDelegations

        self.identity = 'ou_Low'
        self.topic_id = TopicDelegationTests.topic_id(app=self.settings.app_id)
        self.request.state.lighthouse_channel = 'feishu:oc_Group'
        self.request.state.feishu_topic = {'id': self.topic_id, 'context': ''}
        self.registry = self.auth._topic_delegations = TopicDelegations(self.auth, self.store)
        for identity in ('ou_Low', 'ou_Duty'):
            self.assertTrue(self.registry.register(self.topic_id, {'id': identity, 'channel': 'feishu:oc_Group'}, {
                'app_id': self.settings.app_id, 'chat_id': 'oc_Group', 'root_id': 'om_Root',
                'chat_type': 'group', 'message_id': 'om_' + identity.replace('_', ''),
                'sender_id': 'ou_Agent' + identity.replace('_', '')}))
        self.authority = PortalAuthority(self.app, self.controller, self.backend,
                                         SimpleNamespace(root=self.root, lease='fixture', instance='fixture'))
        original = await self.authorize()
        self.context = self.authority.context(self.request, original)

        async def authorize():
            return (await self.authority.authorize(self.context))[1]

        self.authorize = authorize
        self.actor = await authorize()

        async def invoke(actor, operation, request, **kwargs):
            return await self.authority.dispatch({'instance': 'fixture', 'lease': 'fixture',
                'context_id': self.context, 'action': 'invoke', 'payload': {'operation': operation}})

        self.portal._invoke = AsyncMock(side_effect=invoke)

    async def test_topic_real_execution_uses_shared_reads_without_borrowing_write_permissions(self):
        self.person('a', 'A楼人员')
        self.person('b', 'B楼人员', scopes=('B',))
        self.paper('a-paper', 'a')
        self.paper('b-paper', 'b', scope='B')
        await self.topic()
        self.assertEqual(self.actor['learning_scopes'], list('ABCDEH'))
        self.assertEqual(self.actor['_business_write_access']['learning_scopes'], [])
        ack = await self.submit('上周哪些人员参与答题 每周学练')
        await self.finish()
        run = self.runtime.get_run(self.actor, ack['run_id'])
        self.assertEqual(run['status'], 'completed', run.get('error'))
        answer = (await self.runtime.conversation(self.actor))['turns'][-1]['answer']
        self.assertIn('A楼人员', answer)
        self.assertIn('B楼人员', answer)
        self.assertIn('实际参与 **2 人**', answer)
        source = self.assistant._state(self.actor)['turns'][-1]['sources'][0]['data']
        self.assertEqual(source['kind'], 'learning_participation')
        self.assertEqual(self.authority.contexts[self.context]['topic_access'], [{'shared_business_read': True}])
        self.assertFalse(self.store.list_documents('lighthouse_topic_audit'))

    async def test_topic_person_parameters_and_revoked_sponsor_cannot_be_used(self):
        from lan_bitable_template_portal.portal_auth import topic_capability
        from lan_bitable_template_portal.portal_service import PortalError

        await self.topic()
        operation, missing = self.portal.catalog.validate_operation({'api_id': 'GET /api/learning/profile',
            'params': {'scope': 'A', 'view': 'participation', 'period': 'last_week'}})
        self.assertFalse(missing)
        cap = topic_capability(operation)
        execution = self.registry.issue(self.topic_id, 'ou_Low', cap)
        altered = copy.deepcopy(operation)
        altered['params']['scope'] = 'B'
        with self.assertRaises(AssistantError):
            self.authority.learning_summary(execution, altered)
        with self.assertRaises(PortalError):
            topic_capability({**operation, 'params': {**operation['params'], 'person_id': 'self'}})
        with self.assertRaises(PortalError):
            topic_capability({**operation, 'params': {**operation['params'], 'view': 'personal'}})
        self.auth.disabled.add('ou_Duty')
        with self.assertRaises(PortalError):
            self.authority.learning_summary(execution, operation)

    async def test_topic_shared_query_does_not_require_combining_participant_grants(self):
        self.person('b', 'B楼答题人员', scopes=('B',))
        self.paper('b-paper', 'b', scope='B')
        self.auth.add('ou_EngineerB', ['B'], ['assistant', 'learning'])
        self.auth.add('ou_OtherDutyB', ['B'], ['assistant', 'event'])
        self.auth.profiles['ou_OtherDutyB']['category'] = 'duty'
        await self.topic()
        for identity in ('ou_EngineerB', 'ou_OtherDutyB'):
            self.assertTrue(self.registry.register(self.topic_id, {'id': identity, 'channel': 'feishu:oc_Group'}, {
                'app_id': self.settings.app_id, 'chat_id': 'oc_Group', 'root_id': 'om_Root', 'chat_type': 'group',
                'message_id': 'om_' + identity.replace('_', ''), 'sender_id': 'ou_Agent' + identity.replace('_', '')}))
        self.actor = await self.authorize()
        self.assertIn('B', self.actor['scopes'])
        ack = await self.submit('上周B楼哪些人员参与答题 每周学练')
        await self.finish()
        run = self.runtime.get_run(self.actor, ack['run_id'])
        self.assertEqual(run['status'], 'completed', run.get('error'))
        self.assertIn('B楼答题人员', run['answer'])
        self.assertEqual(self.auth.account_profile('ou_Low')['modules'], ['assistant', 'event'])
        self.assertEqual(self.auth.account_profile('ou_Low')['scope_rules'], ['D'])

    async def test_shared_learning_read_in_another_topic_keeps_topic_context_separate(self):
        from bin.test_feishu_topic_delegation import TopicDelegationTests
        from starlette.requests import Request

        await self.topic()
        operation = {'api_id': 'GET /api/learning/profile',
                     'params': {'scope': 'A', 'view': 'participation', 'period': 'last_week'}}
        result = await self.authority.dispatch({'instance': 'fixture', 'lease': 'fixture',
            'context_id': self.context, 'action': 'invoke', 'payload': {'operation': operation}})
        self.assertTrue(result['ok'])
        other = Request({**self.request.scope, 'state': {**self.request.scope.get('state', {}),
            'feishu_topic': {'id': TopicDelegationTests.topic_id(app=self.settings.app_id, root='om_Other'), 'context': ''}}})
        actor = await self.actor_for(self.controller, self.backend, other)
        context = self.authority.context(other, actor)
        with self.assertRaises(AssistantError):
            await self.authority.dispatch({'instance': 'fixture', 'lease': 'fixture',
                'context_id': context, 'action': 'invoke', 'payload': {'operation': operation}})
        self.assertTrue(self.registry.register(other.state.feishu_topic['id'],
            {'id': self.identity, 'channel': 'feishu:oc_Group'}, {
                'app_id': self.settings.app_id, 'chat_id': 'oc_Group', 'root_id': 'om_Other', 'chat_type': 'group',
                'message_id': 'om_OtherLow', 'sender_id': 'ou_AgentLow'}))
        result = await self.authority.dispatch({'instance': 'fixture', 'lease': 'fixture',
            'context_id': context, 'action': 'invoke', 'payload': {'operation': operation}})
        self.assertTrue(result['ok'], result)
        other_actor = (await self.authority.authorize(context))[1]
        self.assertNotEqual(other_actor['_feishu_topic']['id'], self.actor['_feishu_topic']['id'])
        self.assertNotEqual(self.assistant._state(other_actor)['id'], self.assistant._state(self.actor)['id'])

    async def test_stream_rechecks_login_before_emitting_completed_statistics(self):
        self.paper('own', 'self')
        original = self.portal.learning_participation_answer

        async def query(*args, **kwargs):
            result = await original(*args, **kwargs)
            self.auth.disabled.add('ou_Admin')
            return result

        self.portal.learning_participation_answer = AsyncMock(side_effect=query)
        ack = await self.submit('上周哪些人员参与答题 每周学练')
        await self.finish()
        run = self.runtime.get_run(self.actor, ack['run_id'])
        self.assertEqual(run['status'], 'failed')
        self.assertNotIn('实际参与', run.get('answer', ''))
        self.portal.learning_participation_answer.assert_awaited_once()


if __name__ == '__main__':
    unittest.main()
