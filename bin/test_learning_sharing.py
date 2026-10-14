"""Offline sharing-exercise contracts; no production DB, model or Feishu writes."""
import asyncio
import copy
import datetime as dt
import json
import itertools
import random
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lan_bitable_template_portal.learning import LearningService, LearningError, TZ
from lan_bitable_template_portal import learning_sharing as sharing
from lan_bitable_template_portal.learning_sharing_routes import SharingRuntime
from openclaw_service.assistant.lighthouse_sharing import generate

URL = 'https://vnet.feishu.cn/docx/TestMeeting'
SOURCE = '运行参数核验后发现冷却水压差水流开关内部细小水流孔脏堵，需由获准人员按审批流程处理。' * 10
ADMIN = {'id': 'ou_admin', 'is_admin': True}
TODAY = dt.datetime(2026, 10, 14, 12, tzinfo=TZ)


def questions(count=20):
    return [{'stem': f'知识点{i}，纪要记录的故障原因为何？', 'options': ['水流孔脏堵', '设备未供电', '通讯中断', '尚未确认'],
             'correct': [0, 2] if i % 2 else [0], 'explanation': '按纪要核验原因，不自行操作。',
             'evidence': '运行参数核验后发现冷却水压差水流开关内部细小水流孔脏堵'} for i in range(count)]


def typed_questions(types, count=20):
    values = questions(count)
    for q, kind in zip(values, sharing.question_mix(count, types)):
        q['type'] = kind
        q['correct'] = [0] if kind == 'single' else [0, 2]
        if kind == 'interview':
            q.update(options=[], correct=[], reference_answer='按审批流程核验水流孔脏堵，交获准人员处理。')
    return sharing.validate_questions(values, SOURCE, types)


class SharingTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory(); self.addCleanup(self.folder.cleanup)
        self.learning = LearningService(self.folder.name)
        self.learning._restored = True
        self.core = sharing.SharingExercises(self.learning)
        self.clock = patch.object(sharing, 'now', return_value=TODAY); self.clock.start(); self.addCleanup(self.clock.stop)
        self.stamp = patch.object(sharing, 'stamp', return_value=TODAY.isoformat(timespec='seconds')); self.stamp.start(); self.addCleanup(self.stamp.stop)
        self.bid = self.core.create(URL, 'operation-fixture-01', ADMIN, ['single', 'multiple'])['id']
        with self.learning.transaction() as conn:
            batch = self.core.batch(self.bid, conn)
            batch.update(title='冷机故障分享会', status='draft', source={'text': SOURCE}, questions=sharing.validate_questions(questions(), SOURCE),
                         participants=[{'name': f'人员{i}', 'open_id': f'ou_person{i}', 'union_id': f'on_person{i}', 'person_id': f'p{i}', 'scope': 'A' if i == 0 else 'B'} for i in range(3)])
            self.learning._put('sharing_batch', self.bid, batch, conn)

    def publish(self):
        return self.core.publish(self.bid, 1, ADMIN, 'cli_bot')

    def submit(self, attempt, **kwargs):
        batch = self.core.batch(self.bid)
        qs = {q['id']: q for q in batch['questions']}
        fields = {qid: '已核验，按审批流程处理。' if qs[qid]['type'] == 'interview' else [str(i) for i in reversed(qs[qid]['correct'])] for qid in attempt['question_ids']}
        return self.core.submit({'attempt_id': attempt['id'], 'ticket': attempt['ticket']}, kwargs.get('fields', fields),
            {'union_id': kwargs.get('uid', attempt['union_id'])}, kwargs.get('app_id', 'cli_bot'), kwargs.get('allowed', lambda oid: True))

    def test_create_idempotency_and_admin_gate(self):
        self.assertEqual(self.core.create(URL, 'operation-fixture-01', ADMIN, ['single', 'multiple'])['id'], self.bid)
        with self.assertRaises(LearningError): self.core.create(URL, 'operation-fixture-01', {'id': 'ou_person0'})
        with self.assertRaises(LearningError): self.core.create('http://localhost/docx/Test', 'operation-fixture-02', ADMIN)

    def test_question_contracts(self):
        for mutation in ('too_few', 'too_many', 'duplicate', 'evidence', 'correct', 'duplicate_options', 'empty'):
            with self.subTest(mutation=mutation):
                qs = questions()
                if mutation == 'too_few': qs = questions(4)
                if mutation == 'too_many': qs = questions(41)
                if mutation == 'duplicate': qs[1]['stem'] = qs[0]['stem']
                if mutation == 'evidence': qs[0]['evidence'] = '原文没有这段内容，禁止生成。'
                if mutation == 'correct': qs[0]['correct'] = [True]
                if mutation == 'duplicate_options': qs[0]['options'][1] = qs[0]['options'][0]
                if mutation == 'empty': qs[0]['stem'] = ' '
                with self.assertRaises(LearningError): sharing.validate_questions(qs, SOURCE)

    def test_five_question_batch_can_edit_publish_and_grade(self):
        detail = self.core.edit(self.bid, {'version': 1, 'questions': questions(5)}, ADMIN)
        self.assertEqual(detail['question_count'], 5)
        self.core.publish(self.bid, detail['version'], ADMIN, 'cli_bot')
        expected = {q['id'] for q in self.core.batch(self.bid)['questions']}
        for attempt in self.core.attempts(self.bid):
            self.assertEqual(set(attempt['question_ids']), expected)
            self.assertEqual(len(attempt['question_ids']), 5)
            self.submit(attempt)
            self.assertEqual(self.learning._get('sharing_attempt', attempt['id'])['correct'], 5)
        self.assertEqual(self.core.detail(self.bid, ADMIN)['completed'], 3)

    def test_publish_restoration_and_version_guards(self):
        self.learning._restored = False
        with self.assertRaises(LearningError): self.publish()
        self.learning._restored = True
        with self.assertRaises(LearningError): self.core.publish(self.bid, 99, ADMIN, 'cli_bot')
        with self.assertRaises(LearningError): self.core.publish(self.bid, 1, ADMIN, '')

    def test_exempt_attendee_is_kept_but_not_assigned_reminded_or_counted(self):
        detail = self.core.edit(self.bid, {'version': 1, 'questions': questions(), 'excluded_open_ids': ['ou_person1']}, ADMIN)
        self.assertEqual((detail['attendee_total'], detail['total']), (3, 2))
        self.assertTrue(next(p for p in detail['participants'] if p['open_id'] == 'ou_person1')['exempted'])
        self.core.publish(self.bid, detail['version'], ADMIN, 'cli_bot')
        self.assertEqual({p['open_id'] for p in self.core.attempts(self.bid)}, {'ou_person0', 'ou_person2'})
        self.assertEqual(len([j for j in self.learning._all('sharing_delivery') if j['kind'] == 'paper']), 2)
        self.assertFalse(self.core.detail(self.bid, ADMIN)['error'])
        exempted = self.core.detail(self.bid, {'id': 'ou_person1'})
        self.assertEqual((exempted['total'], exempted['questions']), (0, []))
        self.core.schedule(TODAY.replace(hour=13))
        self.assertEqual(len([j for j in self.learning._all('sharing_delivery') if j['kind'] == 'reminder']), 2)
        self.submit(self.core.attempts(self.bid)[0])
        self.core.schedule(dt.datetime(2026, 10, 19, 0, tzinfo=TZ))
        job = next(j for j in self.learning._all('sharing_delivery') if j['kind'] == 'weekly')
        self.assertEqual(len(job['items']), 2)
        text = json.dumps(sharing.weekly_card(job, self.core.batch(self.bid), 'http://portal.test'), ensure_ascii=False)
        self.assertIn('参会 3 人 · 需答 2 人 · 免做 1 人', text)
        self.assertNotIn('人员1', text)
        with self.assertRaises(LearningError):
            self.core.edit(self.bid, {'version': self.core.batch(self.bid)['version'], 'questions': questions(), 'excluded_open_ids': []}, ADMIN)

    def test_exemptions_validate_membership_version_admin_and_empty_assignment(self):
        for excluded in ('ou_person0', ['ou_unknown'], ['ou_person0', 'ou_person0'], [None], [{}]):
            with self.subTest(excluded=excluded), self.assertRaises(LearningError):
                self.core.edit(self.bid, {'version': 1, 'questions': questions(), 'excluded_open_ids': excluded}, ADMIN)
        with self.assertRaises(LearningError):
            self.core.edit(self.bid, {'version': 1, 'questions': questions(), 'excluded_open_ids': ['ou_person0']}, {'id': 'ou_person0'})
        with self.assertRaises(LearningError):
            self.core.edit(self.bid, {'version': 0, 'questions': questions(), 'excluded_open_ids': ['ou_person0']}, ADMIN)
        detail = self.core.edit(self.bid, {'version': 1, 'questions': questions(), 'excluded_open_ids': [f'ou_person{i}' for i in range(3)]}, ADMIN)
        with self.assertRaisesRegex(LearningError, '至少保留一位'):
            self.core.publish(self.bid, detail['version'], ADMIN, 'cli_bot')
        self.assertEqual(self.core.attempts(self.bid), [])
        self.assertEqual(self.learning._all('sharing_delivery'), [])

    def test_exemptions_roundtrip_and_cloud_marker_count_only_assignments(self):
        detail = self.core.edit(self.bid, {'version': 1, 'questions': questions(), 'excluded_open_ids': ['ou_person1']}, ADMIN)
        self.core.publish(self.bid, detail['version'], ADMIN, 'cli_bot')
        batch = self.learning._get('sharing_batch', self.bid)
        self.learning._cloud = Mock()
        self.assertFalse(self.learning._sync_document(('sharing_batch', self.bid, batch['_revision'])))
        with self.learning.transaction() as conn:
            for item in self.core.attempts(self.bid, conn):
                self.learning._put('sharing_attempt', item['id'], item, conn, False)
        self.learning._sync_document(('sharing_batch', self.bid, batch['_revision']))
        self.learning._cloud.upsert_entity.assert_called_once()
        entities = [{'kind': kind, 'key': value['id'], 'payload': value}
                    for kind in sharing.KINDS for value in self.learning._all(kind)]
        with tempfile.TemporaryDirectory() as restored_root:
            recovered = LearningService(restored_root, cloud=SimpleNamespace(load_entities=lambda: entities))
            recovered.restore()
            core = sharing.SharingExercises(recovered)
            self.assertEqual((core.detail(self.bid, ADMIN)['total'], core.detail(self.bid, ADMIN)['error']), (2, ''))
            self.assertEqual(core.batch(self.bid)['excluded_open_ids'], ['ou_person1'])
            core.schedule(TODAY.replace(hour=13))
            self.assertEqual(len([j for j in recovered._all('sharing_delivery') if j['kind'] == 'reminder']), 2)

    def test_concurrent_publish_and_restart_keep_same_five(self):
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda _: self.publish(), range(8)))
        attempts = self.core.attempts(self.bid)
        self.assertEqual(len(attempts), 3)
        self.assertTrue(all(len(set(a['question_ids'])) == 5 for a in attempts))
        self.assertEqual(len([j for j in self.learning._all('sharing_delivery') if j['kind'] == 'paper']), 3)
        self.assertEqual(len([j for j in self.learning._all('sharing_delivery') if j['kind'] == 'bank']), 20)
        restored = sharing.SharingExercises(LearningService(self.folder.name))
        self.assertEqual(attempts, restored.attempts(self.bid))
        self.assertFalse(self.learning._all('paper'))

    def test_submission_set_grading_immutable_and_result_outbox_once(self):
        self.publish(); attempt = self.core.attempts(self.bid)[0]
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda _: self.submit(attempt), range(8)))
        current = self.learning._get('sharing_attempt', attempt['id'])
        self.assertEqual(current['correct'], 5)
        self.submit(attempt, fields={})
        self.assertEqual(current['answers'], self.learning._get('sharing_attempt', attempt['id'])['answers'])
        self.assertEqual(len([j for j in self.learning._all('sharing_delivery') if j['kind'] == 'result']), 1)

    def test_forged_forwarded_revoked_or_partial_card_rejected(self):
        self.publish(); attempt = self.core.attempts(self.bid)[0]
        for kwargs in ({'uid': 'on_other'}, {'app_id': 'cli_other'}, {'allowed': lambda _: False}, {'fields': {}}):
            with self.subTest(kwargs=kwargs), self.assertRaises(LearningError): self.submit(attempt, **kwargs)
        with self.assertRaises(LearningError):
            self.core.submit({'attempt_id': attempt['id'], 'ticket': 'wrong'}, {}, {'union_id': attempt['union_id']}, 'cli_bot', lambda _: True)
        self.assertFalse(self.learning._get('sharing_attempt', attempt['id'])['submitted_at'])

    def test_closed_or_published_batch_cannot_edit(self):
        self.publish()
        with self.assertRaises(LearningError): self.core.edit(self.bid, {'version': 2, 'questions': questions()}, ADMIN)
        with self.learning.transaction() as conn:
            batch = self.core.batch(self.bid, conn); batch['status'] = 'closed'
            self.learning._put('sharing_batch', self.bid, batch, conn)
        with self.assertRaises(LearningError): self.submit(self.core.attempts(self.bid)[0])

    def test_answers_hidden_until_own_submission_and_roles_isolated(self):
        self.publish()
        actor = {'id': 'ou_person0'}
        detail = self.core.detail(self.bid, actor)
        self.assertEqual(detail['total'], 1)
        self.assertEqual(len(detail['questions']), 5)
        self.assertNotIn('correct', detail['questions'][0])
        self.assertNotIn('source_text', detail)
        self.assertNotIn('ticket', json.dumps(detail))
        self.assertNotIn('union_id', json.dumps(detail))
        with self.assertRaises(LearningError): self.core.detail(self.bid, {'id': 'ou_other'})
        duty = self.core.detail(self.bid, {'id': 'ou_dutyA', 'shared_account': True, 'scope': 'A'})
        self.assertEqual(duty['total'], 1); self.assertEqual(duty['questions'], [])
        attempt = next(p for p in self.core.attempts(self.bid) if p['open_id'] == actor['id'])
        self.submit(attempt)
        self.assertIn('correct', self.core.detail(self.bid, actor)['questions'][0])

    def test_daily_reminders_at_13_only_pending_people(self):
        self.publish(); self.submit(self.core.attempts(self.bid)[0])
        self.core.schedule(TODAY)
        self.assertFalse([j for j in self.learning._all('sharing_delivery') if j['kind'] == 'reminder'])
        self.core.schedule(TODAY.replace(hour=13)); self.core.schedule(TODAY.replace(hour=14))
        self.assertEqual(len([j for j in self.learning._all('sharing_delivery') if j['kind'] == 'reminder']), 2)
        self.core.schedule((TODAY + dt.timedelta(days=1)).replace(hour=13))
        self.assertEqual(len([j for j in self.learning._all('sharing_delivery') if j['kind'] == 'reminder']), 4)

    def test_weekly_snapshot_pending_and_zero_not_conflated(self):
        self.publish(); self.submit(self.core.attempts(self.bid)[0])
        monday = dt.datetime(2026, 10, 19, 9, tzinfo=TZ)
        self.core.schedule(monday); self.core.schedule(monday)
        jobs = [j for j in self.learning._all('sharing_delivery') if j['kind'] == 'weekly']
        self.assertEqual(len(jobs), 1)
        self.assertEqual(sum(bool(p['choices']) for p in jobs[0]['items']), 1)
        card = sharing.weekly_card(jobs[0], self.core.batch(self.bid), 'http://portal.test')
        text = json.dumps(card, ensure_ascii=False)
        self.assertIn('参会 3 人 · 已答 1 人 · 未答 2 人', text)
        self.assertIn('暂无选择题首答成绩', text)
        self.assertIn('分享会练习 · 每周汇总', text)

    def test_weekly_late_restart_counts_cutoff_not_mondays_answers(self):
        self.publish()
        with patch.object(sharing, 'stamp', return_value='2026-10-19T08:00:00+08:00'):
            for attempt in self.core.attempts(self.bid): self.submit(attempt)
        self.core.schedule(dt.datetime(2026, 10, 20, 9, tzinfo=TZ))
        job = next(j for j in self.learning._all('sharing_delivery') if j['kind'] == 'weekly')
        self.assertTrue(all(item['choices'] == 0 for item in job['items']))

    def test_cloud_roundtrip_preserves_papers_grades_and_tickets(self):
        self.publish(); self.submit(self.core.attempts(self.bid)[0])
        entities = [{'kind': kind, 'key': value['id'], 'payload': value}
                    for kind in sharing.KINDS for value in self.learning._all(kind)]
        cloud = SimpleNamespace(load_entities=lambda: entities)
        with tempfile.TemporaryDirectory() as restored_root:
            restored = LearningService(restored_root, cloud=cloud)
            restored.restore()
            recovered = sharing.SharingExercises(restored)
            self.assertEqual(self.core.attempts(self.bid), recovered.attempts(self.bid))
            self.assertEqual(len(recovered.detail(self.bid, ADMIN)['questions']), 20)

    def test_normalized_punctuation_evidence_and_partial_restore(self):
        qs = questions(); qs[0]['evidence'] = '注意:仅核验,不得操作。'
        sharing.validate_questions(qs, SOURCE + '注意：仅核验，不得操作。')
        self.publish()
        with self.learning.transaction() as conn:
            conn.execute("DELETE FROM documents WHERE kind='sharing_attempt' AND key=?", (self.core.attempts(self.bid, conn)[0]['id'],))
        self.assertIn('恢复不完整', self.core.detail(self.bid, ADMIN)['error'])
        self.core.schedule(TODAY.replace(hour=13))
        self.assertFalse([j for j in self.learning._all('sharing_delivery') if j['kind'] == 'reminder'])

    def test_cloud_publication_marker_waits_for_all_assignments(self):
        self.publish()
        batch = self.learning._get('sharing_batch', self.bid)
        self.learning._cloud = Mock()
        self.assertFalse(self.learning._sync_document(('sharing_batch', self.bid, batch['_revision'])))
        self.learning._cloud.upsert_entity.assert_not_called()
        with self.learning.transaction() as conn:
            for item in self.core.attempts(self.bid, conn): self.learning._put('sharing_attempt', item['id'], item, conn, False)
        self.learning._sync_document(('sharing_batch', self.bid, batch['_revision']))
        self.learning._cloud.upsert_entity.assert_called_once()
        with self.learning.transaction() as conn:
            batch = self.core.batch(self.bid, conn); batch.update(status='draft', version=1)
            self.learning._put('sharing_batch', self.bid, batch, conn)
        with self.assertRaises(LearningError): self.publish()

    def test_large_cards_are_rejected_atomically_before_any_assignment(self):
        with self.learning.transaction() as conn:
            batch = self.core.batch(self.bid, conn)
            for i, q in enumerate(batch['questions']):
                q['stem'] = str(i) + '题' * 490
                q['options'] = [str(n) + '选' * 290 for n in range(6)]
                q['explanation'] = '析' * 590
                q['evidence'] = '据' * 790
            batch['source']['text'] += '据' * 790
            self.learning._put('sharing_batch', self.bid, batch, conn)
        with self.assertRaises(LearningError): self.publish()
        self.assertEqual(self.core.attempts(self.bid), [])
        self.assertEqual(self.learning._all('sharing_delivery'), [])

    def test_card_five_questions_and_no_answers_before_submission(self):
        self.publish(); batch = self.core.batch(self.bid); attempt = self.core.attempts(self.bid)[0]
        card = sharing.exercise_card(batch, attempt)
        form = card['body']['elements'][-1]
        selectors = [e for e in form['elements'] if e['tag'] in {'select_static', 'multi_select_static'}]
        self.assertEqual(len(selectors), 5)
        self.assertNotIn('正确答案', json.dumps(card, ensure_ascii=False))
        self.assertNotIn('正确选项', json.dumps(card, ensure_ascii=False))
        self.submit(attempt)
        result = sharing.exercise_card(batch, self.learning._get('sharing_attempt', attempt['id']), result=True)
        self.assertEqual(json.dumps(result, ensure_ascii=False).count('正确答案'), 5)

    def test_model_generation_is_structured_and_no_attendance_leak(self):
        model = Mock(); model.complete.return_value = json.dumps({'questions': questions()[:5]})
        result = generate(model, {'source': SOURCE + '\n参会人：@保密人员\n', 'previous': [], 'count': 5})
        self.assertEqual(len(result['questions']), 5)
        self.assertNotIn('保密人员', json.dumps(model.complete.call_args.args, ensure_ascii=False))
        model.complete.return_value = '{bad json'
        with self.assertRaises(ValueError): generate(model, {'source': SOURCE, 'previous': [], 'count': 5})

    def test_all_type_combinations_and_weighted_personal_papers(self):
        for size in range(1, 4):
            for types in itertools.combinations(sharing.QUESTION_TYPES, size):
                types = list(types)
                for count in (5, 10, 20, 40):
                    with self.subTest(types=types, count=count):
                        qs = typed_questions(types, count)
                        self.assertEqual({q['type'] for q in qs}, set(types))
                        selected = sharing.sample_questions(qs, random.Random(1))
                        self.assertEqual(len(set(selected)), 5)
                        self.assertEqual({q['type'] for q in qs if q['id'] in selected}, set(types))
        mix = sharing.question_mix(20, list(sharing.QUESTION_TYPES))
        self.assertEqual([mix.count(k) for k in sharing.QUESTION_TYPES], [12, 6, 2])

    def test_question_type_input_and_reference_validation(self):
        for values in ([], None, 'single', ['single', 'single'], ['unknown'], [{}]):
            with self.subTest(values=values), self.assertRaises(LearningError): sharing.selected_types(values)
        qs = typed_questions(['interview'], 5)
        for bad in ('', '答' * 1001, []):
            invalid = copy.deepcopy(qs); invalid[0]['reference_answer'] = bad
            with self.assertRaises(LearningError): sharing.validate_questions(invalid, SOURCE, ['interview'])
        with self.assertRaises(LearningError): sharing.validate_questions(qs, SOURCE, ['single'])
        with self.assertRaises(LearningError): sharing.validate_questions(qs, SOURCE, ['single', 'interview'])
        with self.assertRaises(LearningError): self.core.create(URL, 'operation-fixture-01', ADMIN, ['interview'])

    def test_new_batches_default_single_and_legacy_drafts_remain_publishable(self):
        bid = self.core.create(URL, 'default-type-operation', ADMIN)['id']
        self.assertEqual(self.core.batch(bid)['question_types'], ['single'])
        with self.learning.transaction() as conn:
            batch = self.core.batch(self.bid, conn); batch.pop('question_types')
            self.learning._put('sharing_batch', self.bid, batch, conn)
        self.publish()
        self.assertEqual(len(self.core.attempts(self.bid)), 3)
        self.assertEqual(self.core.detail(self.bid, ADMIN)['question_types'], ['single', 'multiple'])

    def test_mixed_and_all_text_cards_keep_answers_without_fabricating_grades(self):
        for types, choice_count in ((list(sharing.QUESTION_TYPES), 4), (['interview'], 0)):
            with self.subTest(types=types):
                bid = self.core.create(URL, 'new-batch-' + '-'.join(types), ADMIN, types)['id']
                batch = self.core.batch(self.bid)
                batch.update(id=bid, question_types=types, questions=typed_questions(types), version=1)
                with self.learning.transaction() as conn: self.learning._put('sharing_batch', bid, batch, conn)
                self.core.publish(bid, 1, ADMIN, 'cli_bot')
                batch = self.core.batch(bid); attempt = self.core.attempts(bid)[0]
                qmap = {q['id']: q for q in batch['questions']}
                form = sharing.exercise_card(batch, attempt)['body']['elements'][-1]['elements']
                self.assertEqual(len([e for e in form if e['tag'] == 'input']), 5 - choice_count)
                hidden = self.core.detail(bid, {'id': attempt['open_id']})['questions']
                self.assertTrue(all('reference_answer' not in q for q in hidden))
                raw = '先按审批流程核验。' * 50
                fields = {qid: raw if qmap[qid]['type'] == 'interview' else [str(i) for i in reversed(qmap[qid]['correct'])] for qid in attempt['question_ids']}
                text_id = next(qid for qid in attempt['question_ids'] if qmap[qid]['type'] == 'interview')
                for invalid in (' ', '答' * 1001, ['0']):
                    with self.assertRaises(LearningError):
                        self.core.submit({'attempt_id': attempt['id'], 'ticket': attempt['ticket']}, {**fields, text_id: invalid},
                                         {'union_id': attempt['union_id']}, 'cli_bot', lambda _: True)
                response = self.core.submit({'attempt_id': attempt['id'], 'ticket': attempt['ticket']}, fields,
                                           {'union_id': attempt['union_id']}, 'cli_bot', lambda _: True)
                self.assertTrue(response['ok'])
                saved = self.learning._get('sharing_attempt', attempt['id'])
                self.assertEqual(saved['correct'], choice_count)
                self.assertEqual(saved['answers'][text_id], raw)
                card = json.dumps(sharing.exercise_card(batch, saved, result=True), ensure_ascii=False)
                self.assertIn('参考答案', card)
                self.assertIn('不计入选择题正确率', card)
                self.assertIn('完整作答已保存', card)
                self.core.schedule(dt.datetime(2026, 10, 19, tzinfo=TZ))
                weekly = next(j for j in self.learning._all('sharing_delivery') if j['kind'] == 'weekly' and j['batch_id'] == bid)
                self.assertEqual(sum(p['choices'] for p in weekly['items']), choice_count)
                self.assertEqual(sum(p['answered'] for p in weekly['items']), 5)
                sync = self.core.detail(bid, ADMIN)['bank_sync']
                self.assertEqual(sync, {'total': 20, 'synced': 0, 'failed': 0, 'error': ''})

    def test_model_obeys_each_requested_type_and_rejects_mismatch(self):
        qs = typed_questions(list(sharing.QUESTION_TYPES), 5)
        payload = {'source': SOURCE, 'previous': [], 'count': 5, 'question_types': [q['type'] for q in qs]}
        model = Mock(); model.complete.return_value = json.dumps({'questions': qs})
        self.assertEqual(generate(model, payload)['questions'], qs)
        model.complete.return_value = json.dumps({'questions': questions(5)})
        with self.assertRaises(ValueError): generate(model, payload)


class SourceTests(unittest.TestCase):
    def portal(self, *, repeated=False, changed=False):
        def call(method, url, **kwargs):
            if url.endswith('/raw_content'):
                data = {'content': SOURCE + '\n参会人：@人员甲@人员乙\n'}
            elif url.endswith('/blocks'):
                prefix = 'on_' if kwargs['params']['user_id_type'] == 'union_id' else 'ou_'
                data = {'items': [{'text': {'elements': [{'text_run': {'content': '参会人：'}}, {'mention_user': {'user_id': prefix + 'one'}}, {'mention_user': {'user_id': prefix + 'two'}}]}},
                    {'text': {'elements': [{'text_run': {'content': '待办负责人'}}, {'mention_user': {'user_id': prefix + 'notattending'}}]}}],
                    'has_more': repeated, 'page_token': 'same'}
            else:
                calls[0] += 1
                data = {'document': {'revision_id': calls[0] if changed else 1, 'title': '纪要'}}
            return {'code': 0, 'data': data}
        calls = [0]
        return SimpleNamespace(_auth_headers=lambda: {}, _request_payload=call)

    def test_explicit_attendance_only_and_both_app_identity_types(self):
        source = sharing.read_source(self.portal(), URL)
        self.assertEqual([p['open_id'] for p in source['participants']], ['ou_one', 'ou_two'])
        self.assertEqual(source['participants'][0]['union_id'], 'on_one')

    def test_pagination_loops_and_moving_revision_rejected(self):
        for portal in (self.portal(repeated=True), self.portal(changed=True)):
            with self.assertRaises(LearningError): sharing.read_source(portal, URL)


class RuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_new_answer_results_take_priority_over_older_bank_uploads(self):
        setup = SharingTests(); setup.setUp(); self.addCleanup(setup.doCleanups)
        setup.publish(); setup.submit(setup.core.attempts(setup.bid)[0])
        with setup.learning.transaction() as conn:
            for job in setup.learning._all('sharing_delivery', conn):
                if job['kind'] == 'paper':
                    job['status'] = 'sent'; setup.learning._put('sharing_delivery', job['id'], job, conn)
        worker = SharingRuntime(SimpleNamespace(), lambda: setup.learning); worker.core = setup.core
        with patch('lan_bitable_template_portal.learning_sharing_routes.now', return_value=TODAY):
            jobs = worker.pending('sharing_delivery', 5)
        self.assertEqual(jobs[0]['kind'], 'result')
        self.assertTrue(all(job['kind'] == 'bank' for job in jobs[1:]))

    async def test_bank_retry_does_not_send_cards_and_persists_record_receipt(self):
        setup = SharingTests(); setup.setUp(); self.addCleanup(setup.doCleanups)
        setup.publish()
        jobs = setup.learning._all('sharing_delivery')
        paper_jobs = [j for j in jobs if j['kind'] == 'paper']
        bank_job = next(j for j in jobs if j['kind'] == 'bank')
        worker = SharingRuntime(SimpleNamespace(), lambda: setup.learning); worker.core = setup.core
        worker.bot = Mock(side_effect=AssertionError('Bank sync must not send messages'))
        from lan_bitable_template_portal.learning_cloud import LearningCloudWriteUncertain
        worker.question_bank = Mock()
        worker.question_bank.save_sharing_question.side_effect = [LearningCloudWriteUncertain('写入结果未知'), 'recSaved']
        guard = SimpleNamespace(external_real_write_guard=lambda: {'real_write_allowed': True})
        with patch.dict(sys.modules, {'lan_bitable_template_portal.portal_service': guard}):
            await worker.send(bank_job)
            current = setup.learning._get('sharing_delivery', bank_job['id'])
            self.assertEqual(current['status'], 'pending')
            self.assertEqual(current['failures'], 1)
            self.assertIn('写入结果未知', setup.core.detail(setup.bid, ADMIN)['bank_sync']['error'])
            worker.update_batch(setup.bid, status='closed')
            await worker.send(current)
        receipt = setup.learning._get('sharing_delivery', bank_job['id'])
        self.assertEqual((receipt['status'], receipt['record_id']), ('sent', 'recSaved'))
        self.assertEqual([c.args[-1] for c in worker.question_bank.save_sharing_question.call_args_list], [bank_job['id']] * 2)
        self.assertEqual([j for j in setup.learning._all('sharing_delivery') if j['kind'] == 'paper'], paper_jobs)
        self.assertEqual(setup.core.detail(setup.bid, ADMIN)['bank_sync']['synced'], 1)
        await worker.close(); worker.question_bank.close.assert_called_once()

    async def test_bank_guard_blocks_remote_write(self):
        setup = SharingTests(); setup.setUp(); self.addCleanup(setup.doCleanups)
        setup.publish()
        worker = SharingRuntime(SimpleNamespace(), lambda: setup.learning); worker.core = setup.core
        worker.question_bank = Mock()
        guard = SimpleNamespace(external_real_write_guard=lambda: {'real_write_allowed': False})
        job = next(j for j in setup.learning._all('sharing_delivery') if j['kind'] == 'bank')
        with patch.dict(sys.modules, {'lan_bitable_template_portal.portal_service': guard}): await worker.send(job)
        worker.question_bank.save_sharing_question.assert_not_called()
        self.assertEqual(setup.learning._get('sharing_delivery', job['id'])['status'], 'pending')

    async def test_meeting_questions_reach_non_duty_attendees_only(self):
        setup = SharingTests(); setup.setUp(); self.addCleanup(setup.doCleanups)
        setup.learning._login_people_reader = lambda: {'people': {
            str(i): {'scopes': ['A'], 'selectable': True, 'position': '工程师', 'login_ids': [f'ou_person{i}']}
            for i in range(4)}}
        setup.publish()
        post = AsyncMock(return_value=SimpleNamespace(json=lambda: {'code': 0, 'data': {'message_id': 'om_fixture'}}))
        bot = SimpleNamespace(config={'app_id': 'cli_bot'}, messenger=SimpleNamespace(
            client=AsyncMock(return_value=SimpleNamespace(post=post)), headers=AsyncMock(return_value={})))
        worker = SharingRuntime(SimpleNamespace(feishu_assistant_channel=bot), lambda: setup.learning)
        worker.core = setup.core; worker.allowed = lambda _: True
        guard = SimpleNamespace(BUILDING_OPEN_ID_MAP={}, external_real_write_guard=lambda: {'real_write_allowed': True})
        with patch.dict(sys.modules, {'lan_bitable_template_portal.portal_service': guard}):
            from lan_bitable_template_portal.learning_reminders import duty_recipients
            self.assertEqual(duty_recipients(setup.learning), set())
            for job in setup.learning._all('sharing_delivery'):
                if job['kind'] != 'paper': continue
                await worker.send(job)
        self.assertEqual({call.kwargs['json']['receive_id'] for call in post.await_args_list},
                         {'on_person0', 'on_person1', 'on_person2'})
        self.assertTrue(all(job['status'] == 'sent' for job in setup.learning._all('sharing_delivery') if job['kind'] == 'paper'))

    async def test_generation_scales_from_five_to_forty_questions(self):
        setup = SharingTests(); setup.setUp(); self.addCleanup(setup.doCleanups)
        for text, expected in ((SOURCE, 5), (SOURCE + '补充内容' * 800, 10), (SOURCE + '补' * (24000 - len(SOURCE)), 40)):
            with self.subTest(expected=expected):
                async def generate_questions(owner, source, previous, count, question_types):
                    self.assertEqual(owner, ADMIN['id'])
                    self.assertEqual(source, text)
                    values = typed_questions(['single', 'multiple'], expected)[len(previous):len(previous) + count]
                    self.assertEqual([q['type'] for q in values], question_types)
                    return values
                generate_mock = AsyncMock(side_effect=generate_questions)
                worker = SharingRuntime(SimpleNamespace(service=object(), generate_sharing_questions=generate_mock), lambda: setup.learning)
                worker.core = setup.core
                source = {'text': text, 'title': '纪要', 'participants': []}
                with patch('lan_bitable_template_portal.learning_sharing_routes.read_source', return_value=source):
                    await worker.generate(setup.core.batch(setup.bid))
                batch = setup.core.batch(setup.bid)
                self.assertEqual(batch['status'], 'draft', batch.get('error'))
                self.assertEqual(len(batch['questions']), expected)
                self.assertEqual(generate_mock.await_count, expected // 5)
                self.assertEqual(setup.learning._all('sharing_attempt'), [])
                self.assertEqual(setup.learning._all('sharing_delivery'), [])

    async def test_callback_stays_local_and_enforces_app_identity(self):
        setup = SharingTests(); setup.setUp(); self.addCleanup(setup.doCleanups)
        setup.publish(); attempt = setup.core.attempts(setup.bid)[0]
        worker = SharingRuntime(SimpleNamespace(), lambda: setup.learning)
        worker.core = setup.core; worker.allowed = lambda _: True
        batch = setup.core.batch(setup.bid); qs = {q['id']: q for q in batch['questions']}
        payload = {'header': {'app_id': 'cli_bot', 'event_type': 'card.action.trigger'}, 'event': {
            'operator': {'union_id': attempt['union_id']}, 'action': {'value': {'kind': 'sharing_exercise', 'attempt_id': attempt['id'], 'ticket': attempt['ticket']},
                'form_value': {qid: [str(i) for i in qs[qid]['correct']] for qid in attempt['question_ids']}}}}
        self.assertFalse((await worker.callback(payload, 'cli_other'))['ok'])
        self.assertTrue((await worker.callback(payload, 'cli_bot'))['ok'])
        self.assertTrue(worker.wake.is_set())

    async def test_completed_person_skips_queued_reminder(self):
        setup = SharingTests(); setup.setUp(); self.addCleanup(setup.doCleanups)
        setup.publish(); attempt = setup.core.attempts(setup.bid)[0]
        setup.core.schedule(TODAY.replace(hour=13))
        setup.submit(attempt)
        job = next(j for j in setup.learning._all('sharing_delivery') if j['kind'] == 'reminder' and j['attempt_id'] == attempt['id'])
        worker = SharingRuntime(SimpleNamespace(), lambda: setup.learning); worker.core = setup.core
        guard = SimpleNamespace(external_real_write_guard=Mock(side_effect=AssertionError('Must not send')))
        with patch.dict(sys.modules, {'lan_bitable_template_portal.portal_service': guard}):
            await worker.send(job)
        self.assertEqual(setup.learning._get('sharing_delivery', job['id'])['status'], 'skipped')

    async def test_send_preserves_concurrent_answers_and_retries_with_same_identity(self):
        setup = SharingTests(); setup.setUp()
        self.addCleanup(setup.doCleanups)
        setup.publish()
        learning, core = setup.learning, setup.core
        attempt = core.attempts(setup.bid)[0]
        job = next(j for j in learning._all('sharing_delivery') if j.get('attempt_id') == attempt['id'])
        calls = []
        async def send(url, **kwargs):
            calls.append(kwargs)
            if len(calls) == 1: raise TimeoutError()
            setup.submit(attempt)
            return SimpleNamespace(json=lambda: {'code': 0, 'data': {'message_id': 'om_fixture'}})
        bot = SimpleNamespace(config={'app_id': 'cli_bot'}, messenger=SimpleNamespace(client=AsyncMock(return_value=SimpleNamespace(post=send)), headers=AsyncMock(return_value={})))
        runtime = SimpleNamespace(feishu_assistant_channel=bot)
        worker = SharingRuntime(runtime, lambda: learning); worker.core = core; worker.allowed = lambda oid: True
        guard = SimpleNamespace(external_real_write_guard=lambda: {'real_write_allowed': True})
        with patch.dict(sys.modules, {'lan_bitable_template_portal.portal_service': guard}):
            await worker.send(job)
            await worker.send(learning._get('sharing_delivery', job['id']))
        self.assertEqual(calls[0]['json']['uuid'], calls[1]['json']['uuid'])
        self.assertEqual(calls[0]['params']['receive_id_type'], 'union_id')
        self.assertEqual(calls[0]['json']['receive_id'], attempt['union_id'])
        self.assertEqual(learning._get('sharing_attempt', attempt['id'])['correct'], 5)
        self.assertEqual(learning._get('sharing_delivery', job['id'])['status'], 'sent')


class SharingRouteTests(unittest.TestCase):
    def setUp(self):
        from fastapi import FastAPI
        from fastapi.responses import JSONResponse
        from fastapi.testclient import TestClient
        from lan_bitable_template_portal.learning_sharing_routes import install
        self.fixture = SharingTests(); self.fixture.setUp(); self.addCleanup(self.fixture.doCleanups)
        self.actor = ADMIN
        self.session = {'user': {'open_id': 'ou_admin'}}
        self.app = FastAPI()
        controller = SimpleNamespace(_auth_required_response=lambda: JSONResponse({'ok': False}, status_code=401),
            _json_ok=lambda request, session, data: JSONResponse({'ok': True, 'data': data}))
        def identity(request): return self.session, self.actor
        async def body(request): return await request.json()
        def origin(request):
            if request.headers.get('origin') == 'https://foreign.test': raise LearningError('跨来源请求', 403)
        def error(exc): return JSONResponse({'ok': False, 'error': str(exc)}, status_code=getattr(exc, 'status', 500))
        runtime = SimpleNamespace(feishu_assistant_channel=SimpleNamespace(config={'app_id': 'cli_bot'}, messenger=object()))
        install(self.app, controller, runtime, lambda: self.fixture.learning, identity, body, origin, error)
        self.client = TestClient(self.app); self.addCleanup(self.client.close)

    def test_unauthenticated_and_nonadmin_cannot_create_edit_publish(self):
        self.session = None
        self.assertEqual(self.client.get('/api/learning/sharing').status_code, 401)
        self.session = {'user': {'open_id': 'ou_user'}}; self.actor = {'id': 'ou_user'}
        for method, path in [('POST', ''), ('PUT', '/' + self.fixture.bid), ('POST', '/' + self.fixture.bid + '/publish')]:
            with self.subTest(path=path):
                self.assertEqual(self.client.request(method, '/api/learning/sharing' + path, json={}).status_code, 403)

    def test_csrf_revision_and_real_publish_routing(self):
        base = '/api/learning/sharing/' + self.fixture.bid
        self.assertEqual(self.client.post(base + '/publish', json={'version': 1}, headers={'origin': 'https://foreign.test'}).status_code, 403)
        self.assertEqual(self.client.put(base, json={'version': 99, 'questions': questions()}).status_code, 409)
        self.assertEqual(self.client.post(base + '/publish', json={'version': 1}).status_code, 200)
        self.assertEqual(len(self.fixture.core.attempts(self.fixture.bid)), 3)
        self.assertEqual(self.client.get(base).json()['data']['question_count'], 20)

    def test_create_types_and_bank_retry_leave_sent_cards_untouched(self):
        root = '/api/learning/sharing'
        payload = {'url': URL, 'operation_id': 'new-type-operation-01', 'question_types': ['multiple', 'interview']}
        response = self.client.post(root, json=payload)
        self.assertEqual(response.status_code, 200)
        bid = response.json()['data']['id']
        self.assertEqual(self.fixture.core.batch(bid)['question_types'], payload['question_types'])
        self.assertEqual(self.client.post(root, json={**payload, 'operation_id': 'invalid-type-operation', 'question_types': []}).status_code, 400)
        self.fixture.publish()
        with self.fixture.learning.transaction() as conn:
            for job in self.fixture.learning._all('sharing_delivery', conn):
                job.update(status='failed' if job['kind'] == 'bank' else 'sent', failures=5)
                self.fixture.learning._put('sharing_delivery', job['id'], job, conn)
        response = self.client.post(root + '/' + self.fixture.bid + '/retry-delivery', json={'version': 2})
        self.assertEqual(response.status_code, 200)
        jobs = self.fixture.learning._all('sharing_delivery')
        self.assertTrue(all(j['status'] == 'sent' for j in jobs if j['kind'] == 'paper'))
        self.assertTrue(all(j['status'] == 'pending' and j['failures'] == 0 for j in jobs if j['kind'] == 'bank'))


if __name__ == '__main__': unittest.main()
