"""Checklist detail backfills and paginated history, using isolated local data."""
import datetime as dt
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lan_bitable_template_portal.pre_shift_checks import CheckService, CheckError, CLOUD_DETAIL_VERSION, TZ
from lan_bitable_template_portal.pre_shift_cloud import CheckCloud, SCHEMA


class HistoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.current = dt.datetime(2026, 10, 14, 9, tzinfo=TZ)
        clock = patch('lan_bitable_template_portal.pre_shift_checks.now', return_value=self.current)
        clock.start()
        self.addCleanup(clock.stop)
        self.admin = {'id': 'ou_admin', 'name': '管理员', 'level': 'L1', 'scope_rules': ['ALL'], 'category': 'manager'}
        self.worker = {'id': 'ou_one', 'name': '检查人一', 'level': 'L4', 'scope_rules': ['A'], 'home_scope': 'A', 'category': 'duty', 'modules': ['maintenance']}
        self.cloud, self.sender = Mock(), Mock()
        self.cloud.upsert.return_value = 'recExisting'
        self.service = CheckService(self.temp.name, cloud=self.cloud, sender=self.sender)
        self.service.save_definition(self.admin, 'A', 0, [{'id': 'x', 'text': '检查设备'}, {'id': 'y', 'text': '检查环境'}])
        self.task_id = '2026-10-14:A:白'
        self.service.submit(self.worker, self.task_id, 1, [
            {'id': 'x', 'status': 'normal', 'note': ''},
            {'id': 'y', 'status': 'abnormal', 'note': '环境温度偏高\n已通知工程师'}], self.current)

    def test_expired_and_changed_checklists_retain_all_item_results_and_notes(self):
        later = self.current + dt.timedelta(days=40)
        self.service.save_definition(self.admin, 'A', 1, [{'id': 'new', 'text': '新的检查内容'}])
        self.assertNotIn(self.task_id, [t['id'] for t in self.service.state(self.worker, 'A', later)['tasks']])
        history = self.service.history(self.worker, 'A', current=later)
        self.assertEqual(history['total'], 1)
        task = history['items'][0]
        self.assertEqual([(a['text'], a['status'], a['note']) for a in task['answers']], [
            ('检查设备', 'normal', ''), ('检查环境', 'abnormal', '环境温度偏高\n已通知工程师')])
        self.assertEqual((task['normal_count'], task['abnormal_count'], task['actor_name']), (1, 1, '检查人一'))
        self.assertEqual(task['sync_status'], 'pending')
        public = json.dumps(history)
        for private in ('cloud_key', 'actor_id', 'disk_name', self.temp.name):
            self.assertNotIn(private, public)
        self.cloud.upsert.assert_not_called()
        self.sender.assert_not_called()

    def test_all_history_is_reachable_with_stable_local_pagination(self):
        with self.service.connection(True) as db:
            original = dict(db.execute('SELECT * FROM tasks WHERE id=?', (self.task_id,)).fetchone())
            columns = ','.join(original)
            placeholders = ','.join('?' for _ in original)
            for i in range(45):
                row = {**original, 'id': 'history-' + str(i), 'submitted': (self.current + dt.timedelta(days=i+1)).isoformat()}
                db.execute('INSERT INTO tasks(' + columns + ') VALUES(' + placeholders + ')', tuple(row.values()))
        records = [self.service.history(self.worker, 'A', p) for p in (1, 2, 3)]
        self.assertEqual([len(p['items']) for p in records], [20, 20, 6])
        self.assertTrue(all(p['total'] == 46 for p in records))
        self.assertEqual(len({row['id'] for p in records for row in p['items']}), 46)
        self.assertEqual(self.service.history(self.worker, 'A', 100)['page'], 3)

    def test_history_scope_and_pagination_are_checked(self):
        for scope in ('B', 'ALL', "A' OR 1=1 --"):
            with self.subTest(scope=scope), self.assertRaises(CheckError) as error:
                self.service.history(self.worker, scope)
            self.assertEqual(error.exception.status, 403)
        self.assertEqual(self.service.history(self.admin, 'B')['total'], 0)
        for page, size in ((0, 20), (1, 0), (-1, 20), (1, 51), ('bad', 20), (True, 20)):
            with self.subTest(page=page, size=size), self.assertRaises(CheckError):
                self.service.history(self.worker, 'A', page, size)

    def test_old_synced_rows_are_backfilled_once_without_new_records_or_notifications(self):
        self.service.sync(self.current)
        self.cloud.reset_mock()
        with self.service.connection(True) as db:
            db.execute('ALTER TABLE outbox DROP COLUMN detail_version')
        restarted = CheckService(self.temp.name, cloud=self.cloud, sender=self.sender)
        restarted_again = CheckService(self.temp.name, cloud=self.cloud, sender=self.sender)
        with restarted.connection() as db:
            row = db.execute('SELECT * FROM outbox').fetchone()
            self.assertEqual((row['revision'], row['synced'], row['record_id']), (2, 1, 'recExisting'))
        restarted.sync(self.current)
        self.cloud.upsert.assert_called_once()
        self.assertEqual(self.cloud.upsert.call_args.args[2], 'recExisting')
        self.assertEqual(json.loads(self.cloud.upsert.call_args.args[1][0]['answers'])[1]['note'], '环境温度偏高\n已通知工程师')
        again = CheckService(self.temp.name, cloud=self.cloud, sender=self.sender)
        again.sync(self.current)
        restarted_again.sync(self.current)
        self.cloud.upsert.assert_called_once()
        with again.connection() as db:
            row = db.execute('SELECT * FROM outbox').fetchone()
            self.assertEqual((row['revision'], row['synced'], row['detail_version']), (2, 2, CLOUD_DETAIL_VERSION))
        self.sender.assert_not_called()

    def test_backfill_failure_keeps_local_results_and_persistent_retry_backoff(self):
        self.service.sync(self.current)
        self.cloud.reset_mock()
        with self.service.connection(True) as db:
            db.execute('UPDATE outbox SET detail_version=0')
        fresh = CheckService(self.temp.name, cloud=self.cloud)
        self.cloud.upsert.side_effect = TimeoutError('isolated timeout')
        fresh.sync(self.current)
        retry = CheckService(self.temp.name, cloud=self.cloud)
        retry.sync(self.current + dt.timedelta(minutes=1))
        self.cloud.upsert.assert_called_once()
        history = retry.history(self.worker, 'A')
        self.assertTrue(history['items'][0]['sync_error'])
        self.assertEqual(history['items'][0]['answers'][1]['status'], 'abnormal')
        self.cloud.upsert.side_effect = None
        retry.sync(self.current + dt.timedelta(minutes=6))
        self.assertEqual(self.cloud.upsert.call_count, 2)
        self.assertEqual(retry.history(self.worker, 'A')['items'][0]['sync_status'], 'synced')

    def test_cloud_fields_show_every_result_and_its_own_note(self):
        with self.service.connection() as db:
            tasks = [dict(db.execute('SELECT * FROM tasks WHERE id=?', (self.task_id,)).fetchone())]
        cloud = CheckCloud(None)
        cloud.schema_ready = True
        cloud.request = Mock(return_value={})
        cloud.upsert('existing-key', tasks, 'recExisting')
        fields = cloud.request.call_args.args[2]['fields']
        self.assertEqual(fields['检查结果'], '存在异常')
        self.assertIn('检查设备：正常（无异常）', fields['正常检查项'])
        self.assertIn('检查环境：异常；环境温度偏高\n已通知工程师', fields['异常检查项'])
        self.assertIn(fields['异常检查项'].split('班 · ', 1)[1], fields['检查清单'])
        self.assertIn('检查设备：正常（无异常）', fields['检查清单'])
        self.assertEqual(fields['检查明细'], fields['检查清单'])
        self.assertEqual((fields['检查项数'], fields['无异常项数'], fields['异常项数']), (2, 1, 1))
        answers = json.loads(tasks[0]['answers'])
        answers[1].update(status='normal', note='已复核现场')
        tasks[0]['answers'] = json.dumps(answers)
        cloud.upsert('existing-key', tasks, 'recExisting')
        fields = cloud.request.call_args.args[2]['fields']
        self.assertEqual(fields['检查结果'], '全部正常')
        self.assertEqual(fields['异常检查项'], '')
        self.assertIn('已复核现场', fields['正常检查项'])

    def test_schema_adds_only_missing_detail_fields(self):
        cloud = CheckCloud(None)
        new_fields = {'检查结果', '正常检查项', '异常检查项'}
        cloud.all = Mock(return_value=[{'field_name': name, 'type': kind} for name, kind in SCHEMA.items() if name not in new_fields])
        cloud.request = Mock(return_value={})
        cloud.ensure_schema()
        self.assertEqual({call.args[2]['field_name'] for call in cloud.request.call_args_list}, new_fields)
        self.assertTrue(all(call.args[:2] == ('POST', 'fields') for call in cloud.request.call_args_list))

    def test_history_api_requires_login_and_keeps_original_scope_permissions(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from lan_bitable_template_portal import pre_shift_routes
        user = {**self.worker, 'open_id': self.worker['id']}
        session = {'user': user}
        controller = SimpleNamespace(_current_session=lambda _: session)
        runtime = SimpleNamespace(service=Mock(), auth_manager=SimpleNamespace(public_user=lambda _: user))
        app = FastAPI()
        with patch.object(pre_shift_routes, 'CheckService', return_value=self.service), patch.object(self.service, 'start'), patch.object(self.service, 'stop'):
            pre_shift_routes.install_pre_shift_routes(app, controller, runtime)
            with TestClient(app) as client:
                response = client.get('/api/pre-shift-checks/history?scope=A')
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json()['data']['items'][0]['answers'][1]['status'], 'abnormal')
                self.assertIn('no-store', response.headers['cache-control'])
                self.assertEqual(client.get('/api/pre-shift-checks/history?scope=B').status_code, 403)
                self.assertEqual(client.get('/api/pre-shift-checks/history?scope=A&page=-1').status_code, 400)
                session = None
                self.assertEqual(client.get('/api/pre-shift-checks/history?scope=A').status_code, 401)


if __name__ == '__main__':
    unittest.main()
