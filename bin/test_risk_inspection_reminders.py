"""Two-hour reminders: temporary SQLite, a fake clock and an idempotent fake Feishu."""
import copy
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import httpx

from bin import test_risk_inspection as fixtures
from bin.lan_bitable_template_portal import risk_inspection as risk
from bin.lan_bitable_template_portal import risk_inspection_cloud as cloud


class ReminderTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(patch('socket.socket.connect', side_effect=AssertionError('No network')))
        fixtures.RiskTests.setUp(self)
        self.now = 2_000_000_000.0
        clock = SimpleNamespace(time=lambda: self.now)
        self.enterContext(patch.object(risk, 'time', clock))
        self.enterContext(patch.object(cloud, 'time', clock))
        self.enterContext(patch('bin.lan_bitable_template_portal.portal_service.external_real_write_guard',
                                return_value={'real_write_allowed': True}))
        self.body['deadline'] = (self.now + 3600) * 1000
        self.sent, self.calls, self.hook = {}, [], None
        self.restart()

    def restart(self):
        self.store = risk.RiskInspections(self.store.db_path, lambda pid: copy.deepcopy(self.people[pid]))
        self.remote = cloud.RiskCloud(self.store, Path(self.temp.name) / 'never-read-config',
            lambda target: 'on_' + target['id'], lambda: 'https://fixture.invalid')
        self.addCleanup(self.remote.close)
        self.remote.headers = Mock(return_value={'Authorization': 'Bearer synthetic'})
        self.remote.http.request = Mock(side_effect=self.transport)

    def transport(self, method, url, **kwargs):
        self.assertEqual((method, url), ('POST', cloud.API + 'im/v1/messages'))
        body = copy.deepcopy(kwargs['json'])
        self.calls.append(body)
        if self.hook:
            self.hook(body)
        result = self.sent.setdefault(body['uuid'], {'message_id': 'message-' + str(len(self.sent)), 'body': body})
        return httpx.Response(200, json={'code': 0, 'data': {'message_id': result['message_id']}},
                              request=httpx.Request(method, url))

    def publish(self):
        result = fixtures.RiskTests.publish(self)
        self.quiet_other_jobs()
        return result

    def quiet_other_jobs(self):
        with self.store.connect(True) as db:
            db.execute("UPDATE risk_jobs SET status='done' WHERE kind!='reminder'")

    def jobs(self, status=None):
        with self.store.connect() as db:
            rows = db.execute("SELECT * FROM risk_jobs WHERE kind='reminder' ORDER BY created,id").fetchall()
        return [{**dict(row), 'payload': json.loads(row['payload'])} for row in rows if status is None or row['status'] == status]

    def tick(self):
        self.store.enqueue_reminders()

    def drain(self):
        for _ in range(20):
            self.quiet_other_jobs()
            job = self.store.claim_job()
            if not job:
                return
            try:
                self.remote.process(job)
            except Exception:
                self.store.finish_job(job, 'synthetic delivery failure')
            else:
                self.store.finish_job(job)
        self.fail('Unbounded retry loop')

    def act(self, pid, action, index=0, *, who=None):
        person = next(p for p in self.store.raw(self.body['id'])[1] if p['id'] == pid)
        self.store.action(self.people[who or pid], self.body['id'], pid, {'action': action,
            'version': person['version'], 'item_id': person['items'][index]['id'],
            'values': {'result': 'fixture answer'}, 'reason': 'fixture reason'})

    def submit_all(self, pid):
        person = next(p for p in self.store.raw(self.body['id'])[1] if p['id'] == pid)
        for index, item in enumerate(person['items']):
            if item['status'] in {'pending', 'returned'}:
                self.act(pid, 'submit', index)

    def test_exact_two_hours_not_two_minutes_and_repeated_concurrent_ticks(self):
        self.publish()
        for elapsed in (120, 7199.999):
            self.now = 2_000_000_000 + elapsed
            self.tick()
            self.assertEqual(self.jobs(), [])
        self.now = 2_000_000_000 + 7200
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda _: self.tick(), range(8)))
        ids = [j['id'] for j in self.jobs()]
        self.assertEqual(len(ids), 2)
        self.assertEqual({j['payload']['person_id'] for j in self.jobs()}, {'alice', 'bob'})
        self.restart()
        self.tick()
        self.assertEqual([j['id'] for j in self.jobs()], ids)

    def test_next_reminder_is_two_hours_after_delivery_and_no_downtime_burst(self):
        self.publish()
        self.now += 9 * 3600
        self.tick()
        self.drain()
        self.assertEqual(len(self.sent), 2)
        self.restart()
        self.tick()
        self.now += 7199.999
        self.tick()
        self.assertEqual(len(self.jobs()), 2)
        self.now += .001
        self.tick()
        self.drain()
        self.assertEqual(len(self.sent), 4)
        self.assertEqual(len({j['id'] for j in self.jobs()}), 4)

    def test_draft_does_not_stop_or_postpone_reminder(self):
        self.publish()
        self.now += 7000
        self.act('alice', 'save')
        self.now += 200
        self.tick()
        self.drain()
        self.assertEqual({m['body']['receive_id'] for m in self.sent.values()}, {'on_alice', 'on_bob'})

    def test_submitted_and_any_reviewer_approved_stop_return_resumes_after_two_hours(self):
        self.body.pop('reviewer_id')
        self.body['reviewer_ids'] = ['reviewer', 'other']
        self.publish()
        self.now += 7200
        self.tick()
        self.submit_all('alice')
        self.submit_all('bob')
        self.act('alice', 'approve', who='other')
        self.tick()
        self.drain()
        self.assertEqual(self.calls, [])
        self.now += 100
        returned = self.now
        self.act('alice', 'return', 1, who='reviewer')
        self.tick()
        self.now += 7000
        self.act('alice', 'save', 1)
        self.now = returned + 7199.999
        self.tick()
        self.drain()
        self.assertEqual(self.calls, [])
        self.now = returned + 7200
        self.tick()
        self.drain()
        self.assertEqual(len(self.sent), 1)
        body = next(iter(self.sent.values()))['body']
        self.assertEqual(body['receive_id'], 'on_alice')
        self.assertIn('退回待补交1项', body['content'])
        self.act('alice', 'submit', 1)
        self.now += 7200
        self.tick()
        self.drain()
        self.assertEqual(len(self.sent), 1)

    def test_multi_building_assignment_aggregates_only_outstanding_items(self):
        self.body['people'][0] = {'id': 'alice', 'scopes': ['A', 'B']}
        self.people['alice'].update(modules=[], scope_rules=[], home_scope='C')
        self.publish()
        self.act('alice', 'submit', 0)
        self.act('alice', 'submit', 2)
        self.act('alice', 'return', 2, who='reviewer')
        self.act('alice', 'revoke_item', 3, who='admin')
        self.submit_all('bob')
        self.now += 7200
        self.tick()
        self.drain()
        self.assertEqual(len(self.sent), 1)
        content = next(iter(self.sent.values()))['body']['content']
        self.assertIn('仍有2项', content)
        self.assertIn('A楼：1项', content)
        self.assertIn('B楼：1项', content)
        self.assertNotIn('C楼', content)

    def test_releases_never_merge_or_notify_unassigned_reviewers(self):
        self.body['items'] = [{'title': 'FIRST_ONLY', 'values': {}}]
        self.publish()
        self.body = {**self.body, 'id': 'b' * 32, 'name': 'second',
                     'items': [{'title': 'SECOND_ONLY', 'values': {}}]}
        self.publish()
        self.now += 7200
        self.tick()
        self.drain()
        alice = [m['body'] for m in self.sent.values() if m['body']['receive_id'] == 'on_alice']
        self.assertEqual(len(alice), 2)
        self.assertEqual(len(self.sent), 4)
        self.assertTrue(all(('FIRST_ONLY' in b['content']) != ('SECOND_ONLY' in b['content']) for b in alice))
        self.assertEqual({b['receive_id'] for b in self.calls}, {'on_alice', 'on_bob'})

    def test_deletion_cancellation_completion_and_revocation_suppress_queued_reminders(self):
        for status in ('deleted', 'cancelled', 'completed', 'publishing', 'revoked_person'):
            with self.subTest(status=status):
                self.body = {**self.body, 'id': f'{len(self.jobs()) + 1:032x}', 'deadline': (self.now + 3600) * 1000}
                self.publish()
                self.now += 7200
                self.tick()
                if status == 'deleted':
                    self.store.delete(self.people['admin'], self.body['id'], {})
                elif status == 'revoked_person':
                    for pid in ('alice', 'bob'):
                        self.act(pid, 'revoke_person', who='admin')
                else:
                    with self.store.connect(True) as db:
                        release = self.store._release(db, self.body['id'])
                        release['status'] = status
                        self.store._put_release(db, release)
                self.tick()
                self.drain()
                self.assertEqual(self.calls, [])

    def test_existing_releases_backfill_without_changing_ids_or_draft_age(self):
        self.publish()
        with self.store.connect(True) as db:
            release = self.store._release(db, self.body['id'])
            release.pop('activated_at', None)
            release['created'] = self.now - 3 * 3600
            self.store._put_release(db, release)
            for pid in ('alice', 'bob'):
                person = self.store._person(db, self.body['id'], pid)
                person.pop('_reminder', None)
                person.pop('scopes', None)
                for item in person['items']:
                    item.pop('scope', None)
                self.store._put_person(db, self.body['id'], person)
        ids = [[i['id'] for i in p['items']] for p in self.store.raw(self.body['id'])[1]]
        self.restart()
        self.tick()
        self.drain()
        self.assertEqual(len(self.sent), 2)
        self.assertEqual([[i['id'] for i in p['items']] for p in self.store.raw(self.body['id'])[1]], ids)

    def test_failure_one_person_does_not_block_others_and_retry_id_is_stable(self):
        self.publish()
        self.now += 7200
        self.tick()
        self.hook = lambda body: (_ for _ in ()).throw(TimeoutError()) if body['receive_id'] == 'on_alice' else None
        self.drain()
        self.assertEqual({m['body']['receive_id'] for m in self.sent.values()}, {'on_bob'})
        job_id = next(j['id'] for j in self.jobs('queued'))
        first_uuid = next(b['uuid'] for b in self.calls if b['receive_id'] == 'on_alice')
        self.restart()
        self.hook = None
        self.now += 30
        self.tick()
        self.drain()
        self.assertEqual(next(j['id'] for j in self.jobs() if j['payload']['person_id'] == 'alice'), job_id)
        self.assertEqual([b['uuid'] for b in self.calls if b['receive_id'] == 'on_alice'], [first_uuid] * 2)
        self.assertEqual(len(self.sent), 2)

    def test_unknown_delivery_and_receipt_write_failure_reuse_provider_uuid(self):
        self.publish()
        self.submit_all('bob')
        self.now += 7200
        self.tick()
        checkpoint = self.store.checkpoint
        def fail_receipt(rid, **values):
            if any(r.get('message_id') for r in values.get('messages', {}).values()):
                raise OSError('receipt unavailable')
            checkpoint(rid, **values)
        with patch.object(self.store, 'checkpoint', side_effect=fail_receipt):
            self.drain()
        self.assertEqual(len(self.sent), 1)
        self.restart()
        self.now += 30
        self.tick()
        self.drain()
        self.assertEqual(len(self.sent), 1)
        self.assertEqual(len(self.calls), 2)
        self.assertEqual(self.calls[0]['uuid'], self.calls[1]['uuid'])

    def test_successful_receipt_survives_crash_before_job_completion(self):
        self.publish()
        self.submit_all('bob')
        self.quiet_other_jobs()
        self.now += 7200
        self.tick()
        job = self.store.claim_job()
        self.remote.process(job)
        self.restart()
        self.tick()
        self.now += 901
        self.drain()
        self.assertEqual(len(self.sent), 1)
        self.assertEqual(len(self.calls), 1)

    def test_submit_during_identity_or_auth_resolution_cancels_before_send(self):
        self.publish()
        self.submit_all('bob')
        for stage in ('identity', 'headers'):
            with self.subTest(stage=stage):
                self.now += 7200
                self.tick()
                original = getattr(self.remote, stage)
                def submit(*args, **kwargs):
                    self.submit_all('alice')
                    return original(*args, **kwargs)
                with patch.object(self.remote, stage, side_effect=submit):
                    self.drain()
                self.assertEqual(self.calls, [])
                self.act('alice', 'return', who='reviewer')

    def test_partial_submit_during_auth_refreshes_aggregated_card(self):
        self.publish()
        self.submit_all('bob')
        self.now += 7200
        self.tick()
        def headers():
            self.act('alice', 'submit', 0)
            return {'Authorization': 'Bearer synthetic'}
        self.remote.headers.side_effect = headers
        self.drain()
        self.assertEqual(len(self.sent), 1)
        content = self.calls[0]['content']
        self.assertIn('仍有1项', content)
        self.assertNotIn('检查配电设备', content)

    def test_unknown_outcome_past_provider_dedupe_window_never_gets_new_job_id(self):
        self.publish()
        self.submit_all('bob')
        self.now += 7200
        self.tick()
        self.hook = Mock(side_effect=TimeoutError())
        self.drain()
        job_id = self.jobs()[0]['id']
        self.now += 21 * 3600
        self.restart()
        self.tick()
        self.drain()
        self.assertEqual(len(self.calls), 1)
        self.assertEqual([j['id'] for j in self.jobs()], [job_id])

    def test_repeated_delivery_failure_does_not_abandon_recipient_or_change_job(self):
        self.publish()
        self.submit_all('bob')
        self.now += 7200
        self.tick()
        self.hook = Mock(side_effect=TimeoutError())
        for _ in range(7):
            self.drain()
            self.now += 600
            self.restart()
            self.tick()
        self.assertEqual(len(self.jobs()), 1)
        self.assertEqual(self.jobs()[0]['attempts'], 7)
        self.assertEqual(self.jobs()[0]['status'], 'queued')
        self.assertEqual(len({c['uuid'] for c in self.calls}), 1)
        self.hook = None
        self.drain()
        self.assertEqual(len(self.sent), 1)

    def test_enqueue_failure_rolls_back_cycle_and_job_together(self):
        self.publish()
        with self.store.connect(True) as db:
            for pid in ('alice', 'bob'):
                person = self.store._person(db, self.body['id'], pid)
                person.pop('_reminder')
                self.store._put_person(db, self.body['id'], person)
        self.now += 7200
        with patch.object(self.store, 'enqueue', side_effect=OSError('disk unavailable')):
            with self.assertRaises(OSError):
                self.tick()
        self.assertEqual(self.jobs(), [])
        self.assertTrue(all('_reminder' not in p for p in self.store.raw(self.body['id'])[1]))
        self.tick()
        self.assertEqual(len(self.jobs()), 2)

    def test_legacy_returned_items_wait_two_hours_from_return(self):
        self.publish()
        with self.store.connect(True) as db:
            release = self.store._release(db, self.body['id'])
            release.pop('activated_at')
            release['created'] = self.now - 86400
            self.store._put_release(db, release)
            for pid in ('alice', 'bob'):
                person = self.store._person(db, self.body['id'], pid)
                person.pop('_reminder')
                for item in person['items']:
                    item.update(status='returned' if pid == 'alice' else 'submitted', reviewed_at=self.now - 100)
                self.store._put_person(db, self.body['id'], person)
        self.tick()
        self.assertEqual(self.jobs(), [])
        self.now += 7100
        self.tick()
        self.drain()
        self.assertEqual(len(self.sent), 1)
        self.assertEqual(self.calls[0]['receive_id'], 'on_alice')

    def test_newly_claimed_assignment_waits_two_hours_and_repeat_activation_does_not_reset(self):
        self.publish()
        self.now += 100
        self.store.activate(self.body['id'])
        self.now += 7100
        self.tick()
        self.drain()
        self.assertEqual(len(self.sent), 2)
        self.store.claim(self.people['admin'], self.body['id'], {'scope': 'A'})
        self.tick()
        self.assertEqual({j['payload']['person_id'] for j in self.jobs()}, {'alice', 'bob'})
        self.now += 7200
        self.tick()
        self.drain()
        self.assertEqual(sum(m['body']['receive_id'] == 'on_admin' for m in self.sent.values()), 1)

    def test_disabled_recipient_and_forged_unassigned_target_never_receive(self):
        self.publish()
        self.people['alice']['explicit_disabled'] = True
        self.now += 7200
        self.tick()
        self.drain()
        self.assertEqual({m['body']['receive_id'] for m in self.sent.values()}, {'on_bob'})
        job = next(j for j in self.jobs() if j['payload']['person_id'] == 'alice')
        job = {**job, 'payload': {'person_id': 'other'}}
        self.remote.process(job)
        self.assertEqual(len(self.sent), 1)

    def test_public_response_keeps_existing_notification_label_and_hides_timer_state(self):
        self.publish()
        self.now += 7200
        self.tick()
        detail = self.store.get(self.people['alice'], self.body['id'])
        self.assertTrue(all('_reminder' not in p for p in detail['people']))
        self.assertEqual({j['kind'] for j in detail['jobs']}, {'notify'})
        self.submit_all('alice')
        self.quiet_other_jobs()
        self.assertEqual(self.store.get(self.people['alice'], self.body['id'])['jobs'], [])

    def test_known_pre_send_rejection_does_not_become_an_unknown_delivery(self):
        self.publish()
        self.submit_all('bob')
        self.now += 7200
        self.tick()
        def headers():
            self.people['alice']['inactive'] = True
            return {'Authorization': 'Bearer synthetic'}
        self.remote.headers.side_effect = headers
        self.drain()
        self.assertEqual(self.calls, [])
        self.people['alice'].pop('inactive')
        self.now += 21 * 3600
        self.restart()
        self.tick()
        self.drain()
        self.assertEqual(len(self.sent), 1)

    def delayed_initial(self, *, queued_reminder=True):
        self.publish()
        self.submit_all('bob')
        self.quiet_other_jobs()
        initial_id = f'notify:{self.body["id"]}:alice'
        with self.store.connect(True) as db:
            db.execute("UPDATE risk_jobs SET status='queued',next_at=? WHERE id=?", (self.now + 10800, initial_id))
        self.now += 7200
        if queued_reminder:
            self.tick()
        person = next(p for p in self.store.raw(self.body['id'])[1] if p['id'] == 'alice')
        cycle_id = person['_reminder']['id']
        self.now += 3600
        return initial_id, cycle_id

    def test_late_initial_delivery_defers_queued_cycle_before_next_scan_and_across_restart(self):
        initial_id, cycle_id = self.delayed_initial()
        initial = self.store.claim_job()
        self.assertEqual(initial['id'], initial_id)
        self.remote.process(initial)
        self.store.finish_job(initial)
        delivered_at = self.now
        self.assertIsNone(self.store.claim_job(), 'Late initial receipt must postpone the already queued reminder')
        self.assertEqual([(j['id'], j['status'], j['next_at']) for j in self.jobs()],
                         [(cycle_id, 'queued', delivered_at + 7200)])
        self.restart()
        self.tick()
        release = self.store.raw(self.body['id'])[0]
        self.remote.notify(initial, release, self.people['alice'])
        self.assertEqual(len(self.calls), 1, 'Completed initial notice must not be sent again')
        self.now = delivered_at + 7199.999
        self.act('alice', 'save')
        self.tick()
        self.assertIsNone(self.store.claim_job())
        self.now = delivered_at + 7200
        reminder = self.store.claim_job()
        self.assertEqual(reminder['id'], cycle_id)
        self.remote.process(reminder)
        self.store.finish_job(reminder)
        self.tick()
        next_id = next(p for p in self.store.raw(self.body['id'])[1] if p['id'] == 'alice')['_reminder']['id']
        self.now += 7000
        self.act('alice', 'save')
        self.restart()
        self.tick()
        self.assertIsNone(self.store.claim_job())
        self.now += 200
        self.tick()
        self.assertEqual(self.store.claim_job()['id'], next_id)
        self.assertEqual(len(self.calls), 2)

    def test_preexisting_late_receipt_rebases_existing_queue_on_restart(self):
        initial_id, cycle_id = self.delayed_initial()
        with self.store.connect(True) as db:
            release = self.store._release(db, self.body['id'])
            release['cloud']['messages'] = {initial_id: {'message_id': 'saved-initial', 'sent_at': self.now}}
            self.store._put_release(db, release)
            db.execute("UPDATE risk_jobs SET status='done' WHERE id=?", (initial_id,))
        self.restart()
        self.tick()
        self.assertIsNone(self.store.claim_job())
        self.assertEqual([(j['id'], j['status'], j['next_at']) for j in self.jobs()],
                         [(cycle_id, 'queued', self.now + 7200)])
        self.now += 7200
        self.tick()
        self.drain()
        self.assertEqual(len(self.calls), 1)
        self.assertIn('尚未提交', self.calls[0]['content'])

    def test_late_initial_receipt_defers_cycle_that_is_not_yet_queued(self):
        initial_id, cycle_id = self.delayed_initial(queued_reminder=False)
        initial = self.store.claim_job()
        self.assertEqual(initial['id'], initial_id)
        self.remote.process(initial)
        self.store.finish_job(initial)
        self.restart()
        self.tick()
        self.assertEqual(self.jobs(), [])
        self.now += 7200
        self.tick()
        self.assertEqual(self.store.claim_job()['id'], cycle_id)

    def test_late_initial_receipt_and_timer_postponement_commit_together(self):
        initial_id, cycle_id = self.delayed_initial()
        initial = self.store.claim_job()
        with patch.object(self.store, '_put_person', side_effect=OSError('temporary write failure')):
            with self.assertRaises(OSError):
                self.remote.process(initial)
        self.store.finish_job(initial, 'synthetic checkpoint failure')
        release, people = self.store.raw(self.body['id'])
        self.assertNotIn('message_id', release['cloud']['messages'][initial_id])
        self.assertLess(next(p for p in people if p['id'] == 'alice')['_reminder']['due'], self.now)
        self.assertEqual(self.jobs()[0]['next_at'], 0)
        self.restart()
        self.assertIsNone(self.store.claim_job(), 'Reminder must wait for the original initial receipt retry')
        self.now += 30
        retry = self.store.claim_job()
        self.assertEqual(retry['id'], initial_id)
        self.remote.process(retry)
        self.store.finish_job(retry)
        self.assertEqual([(j['id'], j['next_at']) for j in self.jobs()], [(cycle_id, self.now + 7200)])
        self.assertEqual(len(self.sent), 1)
        self.assertEqual(self.calls[0]['uuid'], self.calls[1]['uuid'])
        self.assertIsNone(self.store.claim_job())

    def test_delayed_initial_blocks_only_its_own_recipient_reminder(self):
        self.publish()
        with self.store.connect(True) as db:
            db.execute("UPDATE risk_jobs SET status='queued',next_at=? WHERE id=?",
                       (self.now + 10800, f'notify:{self.body["id"]}:alice'))
        self.now += 7200
        self.tick()
        job = self.store.claim_job()
        self.assertEqual((job['kind'], job['payload']['person_id']), ('reminder', 'bob'))
        self.remote.process(job)
        self.store.finish_job(job)
        self.assertIsNone(self.store.claim_job())
        self.assertEqual([body['receive_id'] for body in self.calls], ['on_bob'])

    def test_legacy_failed_initial_recovers_with_original_id_and_keeps_others_running(self):
        self.publish()
        initial_id = f'notify:{self.body["id"]}:alice'
        with self.store.connect(True) as db:
            db.execute("UPDATE risk_jobs SET status='failed',attempts=6,next_at=? WHERE id=?",
                       (self.now + 7260, initial_id))
        self.now += 7200
        self.restart()
        self.tick()
        bob = self.store.claim_job()
        self.assertEqual((bob['kind'], bob['payload']['person_id']), ('reminder', 'bob'))
        self.remote.process(bob)
        self.store.finish_job(bob)
        self.assertIsNone(self.store.claim_job())
        self.now += 60
        self.people['alice']['inactive'] = True
        initial = self.store.claim_job()
        self.assertIsNotNone(initial)
        self.assertEqual(initial['id'], initial_id)
        with self.assertRaises(risk.RiskError):
            self.remote.process(initial)
        self.store.finish_job(initial, 'recipient temporarily inactive')
        self.people['alice'].pop('inactive')
        self.now += 600
        self.restart()
        retry = self.store.claim_job()
        self.assertEqual(retry['id'], initial_id)
        self.assertEqual(retry['attempts'], 7)
        self.remote.process(retry)
        self.store.finish_job(retry)
        self.assertEqual([body['receive_id'] for body in self.calls], ['on_bob', 'on_alice'])
        self.assertIsNone(self.store.claim_job())
        self.now += 7200
        self.tick()
        self.drain()
        self.assertEqual(sum(body['receive_id'] == 'on_alice' for body in self.calls), 2)

    def test_initial_retry_preserves_unknown_delivery_twenty_hour_guard(self):
        initial_id, _ = self.delayed_initial()
        initial = self.store.claim_job()
        self.hook = Mock(side_effect=TimeoutError())
        with self.assertRaises(TimeoutError):
            self.remote.process(initial)
        self.store.finish_job(initial, 'unknown delivery')
        with self.store.connect(True) as db:
            db.execute("UPDATE risk_jobs SET status='failed',attempts=6 WHERE id=?", (initial_id,))
        self.now += 21 * 3600
        self.restart()
        self.tick()
        initial = self.store.claim_job()
        self.assertEqual(initial['id'], initial_id)
        with self.assertRaisesRegex(risk.RiskError, '人工核验'):
            self.remote.process(initial)
        self.store.finish_job(initial, 'unknown beyond provider dedupe window')
        self.assertEqual(len(self.calls), 1)
        self.assertIsNone(self.store.claim_job())

    def test_failed_initial_is_not_revived_after_submission(self):
        self.publish()
        initial_id = f'notify:{self.body["id"]}:alice'
        with self.store.connect(True) as db:
            db.execute("UPDATE risk_jobs SET status='failed',attempts=6 WHERE id=?", (initial_id,))
        self.submit_all('alice')
        self.now += 7200
        self.tick()
        with self.store.connect() as db:
            self.assertEqual(db.execute('SELECT status FROM risk_jobs WHERE id=?', (initial_id,)).fetchone()[0], 'failed')


if __name__ == '__main__':
    unittest.main()
