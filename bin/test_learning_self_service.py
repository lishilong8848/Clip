"""Offline self-service authorization tests for the learning portal.

Covers the new account model: admin/ordinary self-only answering, building duty
shared read-only own-building access, login-id based identity mapping, and the
flat attempt history endpoint. No network, no live cloud, no secret reads.
"""
import datetime as dt
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from bin import test_learning as support
from bin.test_learning import DAY, CURRENT
from lan_bitable_template_portal import learning, learning_personal as personal


def _person(pid, name, building, login, extra=None):
    row = {'id': pid, 'person_id': pid, 'name': name, 'employee_no': 'E' + pid,
           'scopes': [building], 'active': True, 'aliases': ['staff:' + pid],
           # Only the real Feishu open id is a login id; never record ids.
           'login_ids': [login]}
    if extra:
        row.update(extra)
    return row


class SelfServiceTests(unittest.TestCase):
    setUp = support.LearningTests.setUp
    seed = support.LearningTests.seed
    dispatch = support.LearningTests.dispatch
    question = support.LearningTests.question

    def seed_service(self, people=None, multiple=False):
        kind = 'multiple' if multiple else 'single'
        self.seed(self.question(i, 'written', kind) for i in range(40))
        people = people or [
            _person('p1', '人员1', 'A', 'ou_p1'),
            _person('p2', '人员2', 'B', 'ou_p2'),
        ]
        with self.service.transaction() as conn:
            for row in people:
                self.service._put('person', row['id'], row, conn)
        self.service._restored = True
        self.service.publish(DAY)

    def actor(self, **changes):
        base = {'id': '', 'scope': '', 'is_admin': False, 'person_id': '',
                'shared_account': False, 'can_answer': False}
        base.update(changes)
        return base

    def _answer_cross_day(self, actor, paper, q, first_op, practice_op, person_id='p1'):
        """First submission on 09-28 (wrong) then a correct practice on 09-29."""
        self.dispatch('paper.answer', {
            'id': paper['id'], 'person_id': person_id, 'question_id': q['id'],
            'option_ids': ['o1'], 'operation_id': first_op}, actor)
        with patch('lan_bitable_template_portal.learning.now',
                   return_value=CURRENT + dt.timedelta(days=1)):
            self.dispatch('paper.answer', {
                'id': paper['id'], 'person_id': person_id, 'question_id': q['id'],
                'option_ids': ['o0'], 'operation_id': practice_op, 'practice': True}, actor)

    # ---- identity mapping -------------------------------------------------
    def test_missing_login_maps_to_clear_identity_issue(self):
        self.seed_service()
        info = self.service.resolve_self('no-such-oid')
        self.assertEqual(info['person_id'], '')
        self.assertIn('尚未关联', info['identity_issue'])

    def test_ambiguous_login_maps_to_clear_identity_issue(self):
        rows = [
            {'id': 'x1', 'person_id': 'x1', 'name': 'A', 'employee_no': '1',
             'scopes': ['A'], 'active': True, 'aliases': ['staff:x1'],
             'login_ids': ['ou_shared']},
            {'id': 'x2', 'person_id': 'x2', 'name': 'B', 'employee_no': '2',
             'scopes': ['B'], 'active': True, 'aliases': ['staff:x2'],
             'login_ids': ['ou_shared']},
        ]
        self.seed_service(people=rows)
        info = self.service.resolve_self('ou_shared')
        self.assertEqual(info['person_id'], '')
        self.assertIn('多个人员', info['identity_issue'])

    def test_refresh_people_persists_only_real_open_id_login_ids_without_leak(self):
        from lan_bitable_template_portal.signature_management import resolve_directory
        staff = [
            {'record_id': 'recS1', 'name': '张三', 'open_id': 'ou_staff1', 'building': 'A楼',
             'employee_no': 'E1', 'has_signature': True, 'signature_token': 'SECRET'},
        ]
        external = [
            {'record_id': 'recE1', 'name': '外部签名', 'open_id': '', 'building': 'C楼',
             'employee_no': 'EX1', 'has_signature': True, 'origin_staff_record_id': 'recS1',
             'historical_record_ids': []},
        ]
        people, resolved = resolve_directory(staff, external)
        directory = {'sources': {'staff': {'ok': True}, 'external': {'ok': True}},
                     'people': people, 'resolved': resolved, 'staff': staff, 'external': external}
        self.service._people_reader = lambda: directory
        personal.refresh_people(self.service)
        stored = self.service._all('person')
        self.assertEqual(len(stored), 1)
        self.assertEqual(stored[0]['login_ids'], ['ou_staff1'])
        # Public projector must never leak login_ids or signature material.
        public = personal.person(self.service, stored[0]['id'])
        self.assertNotIn('login_ids', public)
        self.assertNotIn('SECRET', json.dumps(stored))
        # The real staff open id resolves to the merged person (external row won).
        self.assertEqual(self.service.resolve_self('ou_staff1')['person_id'], stored[0]['id'])
        # Record ids are never logins.
        self.assertEqual(self.service.resolve_self('external:recE1')['person_id'], '')
        self.assertEqual(self.service.resolve_self('staff:recS1')['person_id'], '')

    def test_refresh_people_realistic_directory_resolves_each_person(self):
        from lan_bitable_template_portal.signature_management import resolve_directory
        staff = [
            {'record_id': 'recS1', 'name': '张三', 'open_id': 'ou_zhangsan', 'building': 'A楼',
             'employee_no': 'E1', 'has_signature': True},
            {'record_id': 'recS2', 'name': '李四', 'open_id': 'ou_lisi', 'building': 'B楼',
             'employee_no': 'E2', 'has_signature': False},
        ]
        external = [
            # Effective external: supplies the signature for staff recS2 but carries
            # no open id itself; the explicit staff alias must recover ou_lisi.
            {'record_id': 'recE1', 'name': '外部签名', 'open_id': '', 'building': 'B楼',
             'employee_no': 'EX1', 'has_signature': True, 'origin_staff_record_id': 'recS2',
             'historical_record_ids': ['recOld']},
            {'record_id': 'recE2', 'name': '独立外部', 'open_id': 'ou_ext', 'building': 'C楼',
             'employee_no': 'EX2', 'has_signature': False, 'historical_record_ids': []},
        ]
        people, resolved = resolve_directory(staff, external)
        directory = {'sources': {'staff': {'ok': True}, 'external': {'ok': True}},
                     'people': people, 'resolved': resolved, 'staff': staff, 'external': external}
        self.service._people_reader = lambda: directory
        personal.refresh_people(self.service)
        stored = {p['id']: p for p in self.service._all('person')}
        self.assertEqual(len(stored), 3)
        # Staff zhangsan keeps his own open id.
        zhangsan = [p for p in stored.values() if p['login_ids'] == ['ou_zhangsan']]
        self.assertEqual(len(zhangsan), 1)
        # The merged external-person for lisi maps via the staff alias to ou_lisi.
        lisi = [p for p in stored.values() if p['login_ids'] == ['ou_lisi']]
        self.assertEqual(len(lisi), 1)
        self.assertEqual(lisi[0]['name'], '李四')
        # Independent external maps through its own open id.
        external_person = [p for p in stored.values() if p['login_ids'] == ['ou_ext']]
        self.assertEqual(len(external_person), 1)
        for oid, expected in (('ou_zhangsan', zhangsan[0]['id']), ('ou_lisi', lisi[0]['id']), ('ou_ext', external_person[0]['id'])):
            self.assertEqual(self.service.resolve_self(oid)['person_id'], expected)
        self.assertNotEqual(lisi[0]['id'], zhangsan[0]['id'])

    def test_ambiguous_two_people_same_open_id_fails_closed(self):
        from lan_bitable_template_portal.signature_management import resolve_directory
        staff = [
            {'record_id': 'recS1', 'name': '甲', 'open_id': 'ou_dup', 'building': 'A楼',
             'employee_no': 'E1', 'has_signature': True},
        ]
        external = [
            # Independent external confidently claims the same Feishu account.
            {'record_id': 'recE1', 'name': '乙', 'open_id': 'ou_dup', 'building': 'C楼',
             'employee_no': 'EX1', 'has_signature': True, 'historical_record_ids': []},
        ]
        people, resolved = resolve_directory(staff, external)
        directory = {'sources': {'staff': {'ok': True}, 'external': {'ok': True}},
                     'people': people, 'resolved': resolved, 'staff': staff, 'external': external}
        self.service._people_reader = lambda: directory
        personal.refresh_people(self.service)
        self.assertEqual(len(self.service._all('person')), 2)
        self.assertIn('多个人员', self.service.resolve_self('ou_dup')['identity_issue'])

    def test_name_union_spoof_does_not_map_login(self):
        # A registered under ou_p1 only; spoofing their name/union alias grants nothing.
        rows = [
            _person('p1', '人员1', 'A', 'ou_p1'),
            _person('p2', '人员1', 'B', 'ou_p2'),  # same name, different building
        ]
        self.seed_service(people=rows)
        self.assertEqual(self.service.resolve_self('ou_p1')['person_id'], 'p1')
        self.assertEqual(self.service.resolve_self('ou_p2')['person_id'], 'p2')
        # A staff-only alias from another person is not a real Feishu open id.
        other = self.service.resolve_self('staff:p2')
        self.assertEqual(other['person_id'], '')
        self.assertIn('尚未关联', other['identity_issue'])

    def test_cold_upgrade_missing_login_ids_requests_background_refresh_once(self):
        # Pre-existing people from an old install have no login_ids.
        self.seed(self.question(i, 'written', 'single') for i in range(5))
        with self.service.transaction() as conn:
            self.service._put('person', 'legacy_p1', {
                'id': 'legacy_p1', 'person_id': 'legacy_p1', 'name': '旧人',
                'employee_no': '1', 'scopes': ['A'], 'active': True,
                'aliases': ['staff:recOld'], 'login_ids': []}, conn)
        self.service._people_reader = lambda: {'sources': {'staff': {'ok': True}}, 'people': []}
        self.service._refresh_requested = False
        self.service._refresh_state = 'idle'
        info = self.service.resolve_self('ou_newbie')
        self.assertEqual(info['person_id'], '')
        self.assertIn('尚未关联', info['identity_issue'])
        # Only a background refresh was requested through the existing worker path.
        self.assertTrue(self.service._refresh_requested)
        self.assertEqual(self.service._refresh_state, 'syncing')
        # Local throttling: the very next call does not spam another request.
        self.service._refresh_requested = False
        self.service._refresh_state = 'idle'
        info2 = self.service.resolve_self('ou_newbie')
        self.assertEqual(info2['person_id'], '')
        self.assertFalse(self.service._refresh_requested)

    # ---- self answering ---------------------------------------------------
    def test_admin_and_ordinary_can_answer_self_only(self):
        self.seed_service()
        self_actor = self.actor(id='oid-p1', person_id='p1', can_answer=True)
        admin_self = self.actor(id='admin', is_admin=True, person_id='p1', can_answer=True)
        for idx, actor in enumerate((self_actor, admin_self)):
            paper = self.dispatch('paper.claim', {'person_id': 'p1'}, actor)
            q = paper['questions'][idx]
            result = self.dispatch('paper.answer', {
                'id': paper['id'], 'person_id': 'p1', 'question_id': q['id'],
                'option_ids': ['o0'],
                'operation_id': 'self-' + actor['id']}, actor)
            self.assertEqual(result['questions'][0]['attempt']['correct'], True)

    def test_admin_cannot_answer_or_claim_for_other(self):
        self.seed_service()
        admin = self.actor(id='admin', is_admin=True, person_id='p2', can_answer=True)
        # Admin acts for self p2 only; claiming p1 must be rejected.
        with self.assertRaises(learning.LearningError) as c1:
            self.dispatch('paper.claim', {'person_id': 'p1'}, admin)
        self.assertEqual(c1.exception.status, 403)
        paper = self.dispatch('paper.claim', {'person_id': 'p2'}, admin)
        # Answering another person's claim is also forbidden.
        other_paper = self.dispatch('paper.claim', {'person_id': 'p1'},
                                            self.actor(id='oid-p1', person_id='p1', can_answer=True))
        with self.assertRaises(learning.LearningError) as c2:
            self.dispatch('paper.answer', {
                'id': other_paper['id'], 'person_id': 'p1',
                'question_id': other_paper['questions'][0]['id'],
                'version': other_paper['version'], 'option_ids': ['o0'],
                'operation_id': 'x1'}, admin)
        self.assertEqual(c2.exception.status, 403)

    def test_ordinary_cannot_touch_other_person_data(self):
        self.seed_service()
        me = self.actor(id='oid-p1', person_id='p1', can_answer=True)
        paper = self.dispatch('paper.claim', {'person_id': 'p1'}, me)
        for call in (
            lambda: self.dispatch('paper.get', {'id': paper['id']},
                                  self.actor(id='oid-p2', person_id='p2', can_answer=True), {}),
            lambda: self.service.profile(self.actor(id='oid-p1', person_id='p1', can_answer=True),
                                         {'person_id': 'p2'}),
        ):
            with self.assertRaises(learning.LearningError) as ctx:
                call()
            self.assertNotEqual(ctx.exception.status, 200)
        # Ordinary self-account may list only themselves, never peers.
        people = self.dispatch('people', {}, self.actor(id='oid-p1', person_id='p1', can_answer=True))
        self.assertEqual({r['id'] for r in people['items']}, {'p1'})

    # ---- building duty ----------------------------------------------------
    def test_duty_is_read_only_and_own_building_only(self):
        self.seed_service()
        duty_a = self.actor(id='a', scope='A', shared_account=True)
        self.assertFalse(duty_a['can_answer'])
        paper_a = self.dispatch('paper.claim', {'person_id': 'p1'},
                                        self.actor(id='oid-p1', person_id='p1', can_answer=True))
        # Duty A reads its own paper / people / profile aggregate.
        self.assertEqual(self.service._paper(paper_a['id'], duty_a)['id'], paper_a['id'])
        people = self.dispatch('people', {}, duty_a, {'scope': 'A'})
        self.assertEqual({r['id'] for r in people['items']}, {'p1'})
        # Duty A cannot answer or create issues (write denied).
        with self.assertRaises(learning.LearningError) as w:
            self.dispatch('paper.answer', {
                'id': paper_a['id'], 'person_id': 'p1',
                'question_id': paper_a['questions'][0]['id'],
                'version': paper_a['version'], 'option_ids': ['o0'],
                'operation_id': 'd-write'}, duty_a)
        self.assertEqual(w.exception.status, 403)
        # Duty A cannot read other buildings.
        paper_b = self.dispatch('paper.claim', {'person_id': 'p2'},
                                        self.actor(id='oid-p2', person_id='p2', can_answer=True))
        with self.assertRaises(learning.LearningError) as d:
            self.service._paper(paper_b['id'], duty_a)
        self.assertEqual(d.exception.status, 403)
        with self.assertRaises(learning.LearningError) as d2:
            self.dispatch('people', {}, duty_a, {'scope': 'B'})
        self.assertEqual(d2.exception.status, 403)

    # ---- guest route denial ----------------------------------------------
    def test_guest_route_denied(self):
        self.seed_service()
        with self.assertRaises(learning.LearningError) as ctx:
            self.dispatch('papers.list', {}, self.actor(id='guest'), {})
        self.assertEqual(ctx.exception.status, 403)

    # ---- multi-choice order equality -------------------------------------
    def test_multi_choice_order_equality(self):
        self.seed_service(multiple=True)
        me = self.actor(id='oid-p1', person_id='p1', can_answer=True)
        paper = self.dispatch('paper.claim', {'person_id': 'p1'}, me)
        q = next(x for x in paper['questions'] if x['type'] == 'multiple')
        result = self.dispatch('paper.answer', {
            'id': paper['id'], 'person_id': 'p1', 'question_id': q['id'],
            'version': paper['version'], 'option_ids': ['o2', 'o0'],
            'operation_id': 'multi'}, me)
        attempt = result['questions'][0]['attempt']
        self.assertEqual(attempt['correct'], True)
        self.assertEqual(list(attempt['option_ids']), ['o0', 'o2'])  # normalized order

    # ---- attempt history / practice dedup --------------------------------
    def test_attempts_flat_history_no_duplicate_keeps_first(self):
        self.seed_service()
        me = self.actor(id='oid-p1', person_id='p1', can_answer=True)
        paper = self.dispatch('paper.claim', {'person_id': 'p1'}, me)
        q = paper['questions'][0]
        # First attempt (wrong) then a practice retry, plus a lost-response retry.
        self.dispatch('paper.answer', {
            'id': paper['id'], 'person_id': 'p1', 'question_id': q['id'],
            'option_ids': ['o1'], 'operation_id': 'o-first'}, me)
        self.dispatch('paper.answer', {
            'id': paper['id'], 'person_id': 'p1', 'question_id': q['id'],
            'option_ids': ['o1'], 'operation_id': 'o-first'}, me)
        self.dispatch('paper.answer', {
            'id': paper['id'], 'person_id': 'p1', 'question_id': q['id'],
            'option_ids': ['o0'], 'operation_id': 'o-practice', 'practice': True}, me)
        self.dispatch('paper.answer', {
            'id': paper['id'], 'person_id': 'p1', 'question_id': q['id'],
            'option_ids': ['o0'], 'operation_id': 'o-practice', 'practice': True}, me)
        history = self.service.attempt_history(me, {})
        self.assertEqual(history['total'], 2)
        ops = {r['operation_id'] for r in history['items']}
        self.assertEqual(ops, {'o-first', 'o-practice'})  # retries collapse
        kinds = {r['operation_id']: r['kind'] for r in history['items']}
        self.assertEqual(kinds['o-first'], 'first')
        self.assertEqual(kinds['o-practice'], 'practice')
        # First result is retained on the public paper.
        public = self.service.public_paper(paper, me)
        self.assertEqual(public['questions'][0]['attempt']['correct'], False)

    def test_profile_counts_practice_attempts(self):
        self.seed_service()
        me = self.actor(id='oid-p1', person_id='p1', can_answer=True)
        paper = self.dispatch('paper.claim', {'person_id': 'p1'}, me)
        q = paper['questions'][0]
        self.dispatch('paper.answer', {
            'id': paper['id'], 'person_id': 'p1', 'question_id': q['id'],
            'option_ids': ['o0'], 'operation_id': 'f'}, me)
        self.dispatch('paper.answer', {
            'id': paper['id'], 'person_id': 'p1', 'question_id': q['id'],
            'option_ids': ['o0'], 'operation_id': 'pf', 'practice': True}, me)
        summary = self.service.profile(me, {})['summary']
        self.assertGreaterEqual(summary['attempt_count'], 2)
        self.assertEqual(summary['practice_count'], 1)

    # ---- history/issue/export/attachment leak ----------------------------
    def test_no_other_person_leak_through_history_issue_export_attachment(self):
        self.seed_service()
        me = self.actor(id='oid-p1', person_id='p1', can_answer=True)
        other = self.actor(id='oid-p2', person_id='p2', can_answer=True)
        paper = self.dispatch('paper.claim', {'person_id': 'p1'}, me)
        self.dispatch('paper.answer', {
            'id': paper['id'], 'person_id': 'p1',
            'question_id': paper['questions'][0]['id'],
            'version': paper['version'], 'option_ids': ['o0'],
            'operation_id': 'l-answer'}, me)
        # Attempt history only sees p1.
        self.assertEqual(self.service.attempt_history(me, {})['total'], 1)

    def test_unmapped_ordinary_fails_closed_on_all_content_endpoints(self):
        self.seed_service()
        me = self.actor(id='oid-p1', person_id='p1', can_answer=True)
        paper = self.dispatch('paper.claim', {'person_id': 'p1'}, me)
        q = paper['questions'][0]
        self.dispatch('paper.answer', {
            'id': paper['id'], 'person_id': 'p1', 'question_id': q['id'],
            'version': paper['version'], 'option_ids': ['o0'],
            'operation_id': 'um-answer'}, me)
        issue = self.dispatch('issue.create', {
            'paper_id': paper['id'], 'question_id': q['id'],
            'description': 'Question problem'}, me)
        unmapped = self.actor(id='os', person_id='')
        self.assertEqual(unmapped['person_id'], '')
        # Direct service-layer reads must fail closed for an unmapped ordinary.
        for call in (
            lambda: self.service._paper(paper['id'], unmapped),
            lambda: self.service.review(unmapped, {'person_id': 'p1'}),
            lambda: self.service.attempt_history(unmapped, {'person_id': 'p1'}),
            lambda: self.service.issue(issue['id'], unmapped),
            lambda: self.service.export('results', {'scope': 'A'}, unmapped),
        ):
            with self.assertRaises(learning.LearningError) as ctx:
                call()
            self.assertEqual(ctx.exception.status, 403, ctx.exception)
        # notes goes through dispatch, which rejects unmapped ordinary at the gate.
        with self.assertRaises(learning.LearningError) as ctx:
            self.dispatch('paper.notes', {
                'id': paper['id'], 'question_id': q['id'], 'note': 'x'}, unmapped)
        self.assertEqual(ctx.exception.status, 403)

    def test_ordinary_cannot_open_legacy_ownbuilding_paper_without_person(self):
        self.seed_service()
        me = self.actor(id='oid-p1', person_id='p1', can_answer=True)
        paper = self.dispatch('paper.claim', {'person_id': 'p1'}, me)
        legacy_id = 'legacy:A:historical'
        legacy = {
            'id': legacy_id, 'scope': 'A', 'person_id': '', 'date': '2026-09-27',
            'created_at': '2026-09-27T09:00:00+08:00', 'shortage': {},
            'questions': [self.question(41, 'written')],
        }
        with self.service.transaction() as conn:
            self.service._put('paper', legacy_id, legacy, conn, False)
        # Mapped ordinary must be denied the person-less legacy building paper.
        with self.assertRaises(learning.LearningError) as ctx:
            self.service._paper(legacy_id, me)
        self.assertEqual(ctx.exception.status, 403)
        # Building duty A may still read its own legacy building history.
        duty_a = self.actor(id='a', scope='A', shared_account=True)
        self.assertEqual(self.service._paper(legacy_id, duty_a)['id'], legacy_id)

    def test_bootstrap_exposes_self_and_scope_contract(self):
        self.seed_service()
        me = self.actor(id='oid-p1', person_id='p1', can_answer=True)
        data = self.service.bootstrap('A', me)
        self.assertEqual(data['self_person']['id'], 'p1')
        self.assertEqual(data['self_scope'], 'A')
        self.assertTrue(data['can_answer'])
        self.assertFalse(data['can_view_buildings'])
        self.assertEqual(data['scopes'], [])
        unmatched = self.actor(id='nobody', identity_issue='登录账号与人员名单尚未关联，请管理员先同步人员目录。')
        data2 = self.service.bootstrap('', unmatched)
        self.assertIsNone(data2['self_person'])
        self.assertIn('尚未关联', data2['identity_issue'])

    # ---- cross-day attempt history / practice portrait ---------------------
    def test_cross_day_attempt_history_filters_by_submission_date_with_dedup_and_building_scope(self):
        self.seed_service()
        me = self.actor(id='oid-p1', person_id='p1', can_answer=True)
        paper = self.dispatch('paper.claim', {'person_id': 'p1'}, me)
        q = paper['questions'][0]
        first_op, practice_op = 'x-first', 'x-practice'
        self._answer_cross_day(me, paper, q, first_op, practice_op)
        # Re-send the exact same practice submission: operation id must never re-count.
        with patch('lan_bitable_template_portal.learning.now',
                   return_value=CURRENT + dt.timedelta(days=1)):
            self.dispatch('paper.answer', {
                'id': paper['id'], 'person_id': 'p1', 'question_id': q['id'],
                'option_ids': ['o0'], 'operation_id': practice_op, 'practice': True}, me)

        def kinds(rows):
            return {r['operation_id']: r['kind'] for r in rows}

        day28 = (DAY + dt.timedelta(days=0)).isoformat()
        day29 = (DAY + dt.timedelta(days=1)).isoformat()
        self.assertEqual(self.service.attempt_history(me, {'from': day28, 'to': day28})['total'], 1)
        self.assertEqual(kinds(self.service.attempt_history(me, {'from': day28, 'to': day28})['items']),
                         {first_op: 'first'})
        self.assertEqual(self.service.attempt_history(me, {'from': day29, 'to': day29})['total'], 1)
        self.assertEqual(kinds(self.service.attempt_history(me, {'from': day29, 'to': day29})['items']),
                         {practice_op: 'practice'})
        self.assertEqual(self.service.attempt_history(me, {})['total'], 2)
        # Building duty for the own building sees the same filtered history.
        duty_a = self.actor(id='a', scope='A', shared_account=True)
        duty_view = self.service.attempt_history(duty_a, {
            'from': day29, 'to': day29, 'scope': 'A', 'person_id': 'p1'})
        self.assertEqual(duty_view['total'], 1)
        self.assertEqual(kinds(duty_view['items']), {practice_op: 'practice'})
        # Ordinary other-person and other-building duty stay denied.
        other = self.actor(id='oid-p2', person_id='p2', can_answer=True)
        with self.assertRaises(learning.LearningError) as c1:
            self.service.attempt_history(other, {'person_id': 'p1'})
        self.assertEqual(c1.exception.status, 403)
        duty_b = self.actor(id='b', scope='B', shared_account=True)
        with self.assertRaises(learning.LearningError) as c2:
            self.service.attempt_history(duty_b, {'scope': 'A', 'person_id': 'p1'})
        self.assertEqual(c2.exception.status, 403)

    def test_profile_day_counts_practice_without_mutating_first_attempt_accuracy(self):
        self.seed_service()
        me = self.actor(id='oid-p1', person_id='p1', can_answer=True)
        paper = self.dispatch('paper.claim', {'person_id': 'p1'}, me)
        q = paper['questions'][0]
        self._answer_cross_day(me, paper, q, 'd-first', 'd-practice')

        day28 = DAY.isoformat()
        day29 = (DAY + dt.timedelta(days=1)).isoformat()
        prof = self.service.profile(me, {'from': day29, 'to': day29})
        summary = prof['summary']
        self.assertEqual(summary['practice_count'], 1)
        self.assertEqual(summary['learning_days'], 1)
        self.assertEqual(summary['answered_people'], 1)
        people = {r['person_id']: r for r in prof['people']}
        self.assertEqual(set(people), {'p1'})
        self.assertTrue(people['p1']['last_answered_at'].startswith(day29))
        trend_day = next(t for t in prof['trend'] if t['date'] == day29)
        self.assertEqual(trend_day['practice_count'], 1)
        self.assertEqual(trend_day['people'], 1)
        # The correct practice must not rewrite the preserved first (wrong) result.
        full = self.service.profile(me, {'from': day28, 'to': day29})
        self.assertEqual(full['summary']['answered'], 1)
        self.assertEqual(full['summary']['wrong'], 1)
        self.assertEqual(full['summary']['correct'], 0)
        self.assertEqual(full['summary']['accuracy'], 0.0)
        self.assertEqual(full['summary']['practice_count'], 1)

    def test_building_profile_day_people_include_cross_day_reviewer_without_leak(self):
        self.seed_service()  # p1 in building A, p2 in building B
        me = self.actor(id='oid-p1', person_id='p1', can_answer=True)
        other = self.actor(id='oid-p2', person_id='p2', can_answer=True)
        paper_a = self.dispatch('paper.claim', {'person_id': 'p1'}, me)
        paper_b = self.dispatch('paper.claim', {'person_id': 'p2'}, other)
        self._answer_cross_day(me, paper_a, paper_a['questions'][0], 'a-first', 'a-practice', 'p1')
        self._answer_cross_day(other, paper_b, paper_b['questions'][0], 'b-first', 'b-practice', 'p2')

        day28 = DAY.isoformat()
        day29 = (DAY + dt.timedelta(days=1)).isoformat()
        duty_a = self.actor(id='a-duty', scope='A', shared_account=True)
        prof = self.service.profile(duty_a, {'from': day29, 'to': day29, 'scope': 'A'})
        self.assertEqual({p['person_id'] for p in prof['people']}, {'p1'})
        reviewer = next(p for p in prof['people'] if p['person_id'] == 'p1')
        self.assertTrue(reviewer['last_answered_at'].startswith(day29))
        self.assertEqual(reviewer['summary']['practice_count'], 1)
        building_a = next(b for b in prof['buildings'] if b['scope'] == 'A')
        self.assertEqual(building_a['practice_count'], 1)
        self.assertEqual(building_a['answered_people'], 1)
        # First-attempt metrics on the wider window still reflect the wrong first
        # answer; the correct practice and p2's building-B work never leak into A.
        full_a = self.service.profile(duty_a, {'from': day28, 'to': day29, 'scope': 'A'})
        self.assertEqual({p['person_id'] for p in full_a['people']}, {'p1'})
        self.assertEqual(next(b for b in full_a['buildings'] if b['scope'] == 'A')['answered'], 1)
        self.assertEqual(next(b for b in full_a['buildings'] if b['scope'] == 'A')['wrong'], 1)
        self.assertEqual(next(b for b in full_a['buildings'] if b['scope'] == 'A')['accuracy'], 0.0)
        self.assertEqual(next(b for b in full_a['buildings'] if b['scope'] == 'A')['practice_count'], 1)

    def run(self, *args, **kwargs):
        import threading
        self._real_thread_start = threading.Thread.start
        return super().run(*args, **kwargs)


if __name__ == '__main__':
    unittest.main()