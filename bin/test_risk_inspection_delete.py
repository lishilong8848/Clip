import copy
from concurrent.futures import ThreadPoolExecutor
import json
import time
import unittest

from . import test_risk_inspection as fixtures
from .lan_bitable_template_portal.risk_inspection import RiskError, RiskInspections


class RiskDeletionTests(unittest.TestCase):
    setUp = fixtures.RiskTests.setUp
    publish = fixtures.RiskTests.publish
    act = fixtures.RiskTests.act
    client = fixtures.RouteTests.client

    def test_delete_is_idempotent_and_preserves_evidence_after_restart(self):
        self.publish()
        self.act('alice', 'submit', values={'result': '已有现场反馈'})
        rid = self.body['id']
        self.store.checkpoint(rid, table='original-result-table', url='https://example.test/original')
        with self.store.connect(True) as db:
            db.execute('INSERT INTO risk_files VALUES(?,?,?,?,?,?,?,?)', ('file', rid, 'alice', 'item', 'photo', 'evidence.png', 1, time.time()))
        before, people = self.store.raw(rid)
        self.assertTrue(self.store.get(self.people['admin'], rid)['can_delete_release'])
        for _ in range(2):
            self.assertEqual(self.store.delete(self.people['admin'], rid, {}), {'id': rid, 'status': 'deleted'})
        restarted = RiskInspections(self.store.db_path, self.store.person_for)
        for who in ('admin', 'reviewer', 'alice'):
            for view in ('all', 'published', 'mine'):
                self.assertEqual(restarted.list(self.people[who], view=view)['total'], 0)
            with self.assertRaises(RiskError) as error:
                restarted.get(self.people[who], rid)
            self.assertEqual(error.exception.status, 410)
        with self.store.connect() as db:
            saved = self.store._release(db, rid, include_deleted=True)
            self.assertEqual(saved['cloud'], before['cloud'])
            self.assertEqual([json.loads(row[0]) for row in db.execute('SELECT data FROM risk_people WHERE release_id=? ORDER BY scope,person_id', (rid,))], people)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM risk_files').fetchone()[0], 1)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM risk_audit WHERE action='删除排查任务'").fetchone()[0], 1)
            self.assertTrue(all(row[0] in {'done', 'cancelled'} for row in db.execute("SELECT status FROM risk_jobs WHERE kind!='template_sync'")))
        self.assertEqual(restarted.history.get(self.people['admin'], rid)['name'], self.body['name'])
        with self.assertRaises(RiskError):
            restarted.publish(self.people['admin'], self.body, self.template)

    def test_only_existing_full_release_managers_can_delete(self):
        self.publish()
        for who in ('alice', 'bob', 'other'):
            if who != 'other':
                self.assertFalse(self.store.list(self.people[who])['items'][0]['can_delete_release'])
            with self.assertRaises(RiskError) as error:
                self.store.delete(self.people[who], self.body['id'], {})
            self.assertEqual(error.exception.status, 403)
        self.people['admin']['scope_rules'] = ['B']
        with self.assertRaises(RiskError) as error:
            self.store.delete(self.people['admin'], self.body['id'], {})
        self.assertEqual(error.exception.status, 403)
        self.people['admin']['scope_rules'] = ['ALL']
        self.assertEqual(self.store.get(self.people['admin'], self.body['id'])['status'], 'active')

    def test_concurrent_delete_does_not_repeat_audit_or_leave_runnable_jobs(self):
        self.publish()
        with ThreadPoolExecutor(max_workers=2) as workers:
            results = list(workers.map(lambda _: self.store.delete(self.people['admin'], self.body['id'], {}), range(2)))
        self.assertEqual(results, [{'id': self.body['id'], 'status': 'deleted'}] * 2)
        with self.store.connect() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM risk_audit WHERE action='删除排查任务'").fetchone()[0], 1)
            self.assertFalse(db.execute("SELECT 1 FROM risk_jobs WHERE kind!='template_sync' AND status IN ('queued','running')").fetchone())

    def test_partial_scope_reviewer_cannot_delete_other_buildings(self):
        self.body['reviewer_id'] = 'admin'
        self.body['people'].append({'id': 'other', 'scope': 'B'})
        self.publish()
        limited = copy.deepcopy(self.people['admin'])
        limited['scope_rules'] = ['A']
        self.assertFalse(self.store.get(limited, self.body['id'])['can_delete_release'])
        with self.assertRaises(RiskError):
            self.store.delete(limited, self.body['id'], {})

    def test_live_worker_blocks_delete_but_expired_job_is_cancelled(self):
        self.publish()
        with self.store.connect(True) as db:
            db.execute("UPDATE risk_jobs SET status='done' WHERE kind='template_sync'")
        job = self.store.claim_job()
        self.assertIsNotNone(job)
        with self.assertRaises(RiskError) as error:
            self.store.delete(self.people['admin'], self.body['id'], {})
        self.assertEqual(error.exception.status, 409)
        self.assertEqual(self.store.get(self.people['admin'], self.body['id'])['status'], 'active')
        with self.store.connect(True) as db:
            db.execute("UPDATE risk_jobs SET lease=0 WHERE status='running'")
        self.store.delete(self.people['admin'], self.body['id'], {})
        with self.assertRaises(RiskError):
            self.store.renew_job(job)
        claimed = self.store.claim_job()
        self.assertTrue(claimed is None or claimed['kind'] == 'template_sync')
        with self.assertRaises(RiskError):
            self.store.activate(self.body['id'])
        with self.assertRaises(RiskError):
            self.store.retry(self.people['admin'], self.body['id'])

    def test_delete_route_requires_auth_origin_and_valid_body(self):
        self.publish()
        client = self.client()
        path = '/api/risk-inspections/' + self.body['id']
        self.assertEqual(client.request('DELETE', path, json={}).status_code, 401)
        self.assertEqual(client.request('DELETE', path, json={}, headers={'x-person': 'admin', 'Origin': 'https://other.test'}).status_code, 403)
        self.assertEqual(client.request('DELETE', path, json={}, headers={'x-person': 'alice'}).status_code, 403)
        self.assertEqual(client.request('DELETE', path, json={'scope': 'ALL'}, headers={'x-person': 'admin'}).status_code, 400)
        response = client.request('DELETE', path, json={}, headers={'x-person': 'admin'})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()['data']['status'], 'deleted')
        self.assertEqual(client.get(path, headers={'x-person': 'alice'}).status_code, 410)


if __name__ == '__main__':
    unittest.main()
