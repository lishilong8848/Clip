"""Isolated scheduling, authorization, submission and cloud-retry regression tests."""
import datetime as dt
import json
import sys
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lan_bitable_template_portal.pre_shift_checks import CheckService, CheckError, TZ, windows
from lan_bitable_template_portal.pre_shift_cloud import CheckCloud, read_roster

DAY = '2026-10-14'
def at(value):
    return dt.datetime.fromisoformat(value).replace(tzinfo=TZ)


class CheckTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.admin = {'id': 'ou_admin', 'name': '管理员', 'level': 'L1', 'scope_rules': ['ALL'], 'category': 'manager'}
        self.worker = {'id': 'ou_staff1', 'name': '值班员一', 'level': 'L4', 'scope_rules': ['A'], 'home_scope': 'A', 'category': 'operator'}
        self.peer = {**self.worker, 'id': 'ou_staff2', 'name': '值班长二', 'category': 'shift_leader'}
        self.roster = Mock(return_value={'people': [{'scope': 'A', 'shift': shift, 'open_id': f'ou_staff{i}', 'name': f'人员{i}'} for shift in ('白', '夜') for i in (1, 2)], 'warnings': {}})
        self.sender = Mock(return_value=(True, '', {}))
        self.cloud = Mock()
        self.cloud.upsert.return_value = 'rec_test'
        self.service = CheckService(self.temp.name, roster=self.roster, cloud=self.cloud, sender=self.sender, portal_url=lambda: 'http://portal:18766')
        clock = patch('lan_bitable_template_portal.pre_shift_checks.now', return_value=at(DAY + 'T08:50:00'))
        clock.start()
        self.addCleanup(clock.stop)
        self.service.save_definition(self.admin, 'A', 0, [{'id': 'x', 'text': '检查测试项一'}, {'id': 'y', 'text': '检查测试项二'}])
        self.current = at(DAY + 'T08:50:00')
        self.answers = [{'id': 'x', 'status': 'normal', 'note': ''}, {'id': 'y', 'status': 'abnormal', 'note': '测试异常说明'}]

    def submit(self, actor=None, shift='白', current=None):
        return self.service.submit(actor or self.worker, DAY + ':A:' + shift, 1, self.answers, current or self.current)

    def test_schedule_and_stop_after_either_person_submits(self):
        self.service.tick(self.current - dt.timedelta(seconds=1))
        self.assertEqual(self.sender.call_count, 0)
        self.service.tick(self.current)
        self.assertEqual(self.sender.call_count, 2)
        self.service.tick(self.current + dt.timedelta(minutes=119))
        self.assertEqual(self.sender.call_count, 2)
        self.service.tick(self.current + dt.timedelta(hours=2))
        self.assertEqual(self.sender.call_count, 4)
        self.submit(self.peer, current=self.current + dt.timedelta(hours=2, minutes=1))
        self.service.tick(self.current + dt.timedelta(hours=4))
        self.assertEqual(self.sender.call_count, 4)
        self.assertEqual(self.roster.call_count, 1)
        self.assertEqual(self.service.state(self.worker, 'A', self.current)['tasks'][0]['actor_name'], '值班长二')
        self.assertEqual(self.cloud.upsert.call_count, 1)

    def test_day_reminders_end_at_18_and_night_at_next_08(self):
        for hour in [8, 10, 12, 14, 16, 18]:
            self.service.tick(at(f'{DAY}T{hour:02d}:50:00'))
        day_calls = [c for c in self.sender.call_args_list if ':白:' in c.args[2]]
        self.assertEqual(len(day_calls), 10)
        self.service.tick(at(DAY + 'T19:50:00'))
        self.service.tick(at('2026-10-15T07:50:00'))
        night_before = len([c for c in self.sender.call_args_list if f'{DAY}:A:夜:' in c.args[2]])
        self.service.tick(at('2026-10-15T08:00:00'))
        self.assertEqual(night_before, len([c for c in self.sender.call_args_list if f'{DAY}:A:夜:' in c.args[2]]))
        self.assertEqual(windows(DAY, '夜')[1], at('2026-10-15T08:00:00'))

    def test_duty_submission_stops_both_people_but_not_other_shifts_or_days(self):
        self.service.tick(self.current)
        duty = {**self.worker, 'id': 'ou_building_duty', 'category': 'duty'}
        self.submit(duty)
        self.service.tick(self.current + dt.timedelta(hours=2))
        self.assertEqual(self.sender.call_count, 2)
        self.assertTrue(self.service.state(self.peer, 'A', self.current)['tasks'][0]['submitted'])
        night = at(DAY + 'T17:50:00')
        self.service.tick(night)
        self.assertEqual(self.sender.call_count, 4, 'white completion must not silence the night shift')
        self.submit(duty, shift='夜', current=night)
        self.service.tick(at('2026-10-15T01:50:00'))
        self.assertEqual(self.sender.call_count, 4, 'the same overnight shift stays completed after midnight')
        self.service.tick(at('2026-10-15T08:50:00'))
        self.assertEqual(self.sender.call_count, 6, 'the next day is an independent checklist')
        self.assertTrue(all('2026-10-15:A:白' in c.args[2] for c in self.sender.call_args_list[-2:]))

    def test_each_due_group_is_independent_without_faking_answers(self):
        items = [{'id': 'x', 'text': '检查一'}, {'id': 'y', 'text': '检查二'},
                 {'id': 'later', 'text': '稍后检查', 'frequency': {'kind': 'daily', 'time': '11:00'}}]
        self.service.save_definition(self.admin, 'A', 1, items)
        current = at(DAY + 'T11:00:00')
        self.service.prepare(current)
        self.service.submit(self.worker, DAY + ':A:白', 2, self.answers, current)
        self.service.notify(current + dt.timedelta(hours=2))
        self.assertEqual(self.sender.call_count, 2, 'a different due time is still a separate check')
        self.assertTrue(all(':白:1100:' in c.args[2] for c in self.sender.call_args_list))
        later = next(t for t in self.service.state(self.peer, 'A', current)['tasks'] if t['id'].endswith(':1100'))
        self.assertEqual(later['submitted'], '', 'unanswered items are not silently marked complete')
        self.assertEqual(later['answers'], [])
        self.sender.reset_mock()
        self.service.save_definition(self.admin, 'B', 0, [{'id': 'b', 'text': 'B楼检查'}])
        self.roster.return_value['people'] += [{**p, 'scope': 'B'} for p in list(self.roster.return_value['people'])]
        with self.service.connection(True) as conn:
            conn.execute('DELETE FROM rosters')
        self.service.prepare(current)
        self.service.notify(current)
        self.assertTrue(any(':B:白:' in c.args[2] for c in self.sender.call_args_list), 'A completion must not silence B')

    def test_submit_during_first_delivery_stops_the_second_person(self):
        self.service.prepare(self.current)
        def send(*_):
            self.submit(self.peer)
            return True, '', {}
        self.sender.side_effect = send
        self.service.notify(self.current)
        self.assertEqual(self.sender.call_count, 1)
        self.service.notify(self.current + dt.timedelta(hours=2))
        self.assertEqual(self.sender.call_count, 1)

    def test_upcoming_weekly_monthly_checks_can_be_completed_before_their_due_time(self):
        self.service.save_definition(self.admin, 'A', 1, [
            {'id': 'week', 'text': '每周检查', 'frequency': {'kind': 'weekly', 'weekday': 0, 'time': '10:00'}},
            {'id': 'month', 'text': '月初检查', 'frequency': {'kind': 'month_start', 'time': '09:00'}},
        ])
        self.service.prepare(self.current)
        tasks = self.service.state(self.worker, 'A', self.current)['tasks']
        self.assertEqual([task['day'] for task in tasks], ['2026-10-19', '2026-11-01'])
        self.assertTrue(all(task['due'] > self.current.timestamp() and task['can_submit'] for task in tasks))
        self.assertEqual(self.roster.call_count, 1, 'future roster days are not downloaded to render plans')
        for task in tasks:
            results = [{'id': item['id'], 'status': 'normal', 'note': ''} for item in task['items']]
            self.service.submit(self.peer, task['id'], task['definition_version'], results, self.current)
        self.service.notify(at('2026-10-19T10:00:00'))
        self.service.notify(at('2026-11-01T09:00:00'))
        self.sender.assert_not_called()
        completed = self.service.state(self.worker, 'A', self.current)['tasks']
        self.assertTrue(all(t['actor_name'] == self.peer['name'] and t['submitted'] == self.current.isoformat() for t in completed))
        later = self.service.state(self.worker, 'A', at('2026-10-19T18:00:00'))['tasks']
        self.assertEqual([t['day'] for t in later], ['2026-11-01'], 'completed checks disappear at the end of their own window')

    def test_overdue_check_keeps_its_snapshot_and_can_be_completed_late(self):
        self.service.prepare(self.current)
        late = at('2026-10-16T09:00:00')
        with patch('lan_bitable_template_portal.pre_shift_checks.now', return_value=late):
            self.service.save_definition(self.admin, 'A', 1, [{'id': 'new', 'text': '之后的新清单'}])
        state = self.service.state(self.peer, 'A', late)
        row = next(t for t in state['tasks'] if t['id'] == DAY + ':A:白')
        self.assertTrue(row['can_submit'])
        self.assertEqual(row['definition_version'], 1, 'late work retains the original checklist')
        self.submit(self.peer, current=late)
        self.assertNotIn(row['id'], [t['id'] for t in self.service.state(self.worker, 'A', late)['tasks']])
        with self.service.connection() as conn:
            saved = conn.execute('SELECT * FROM tasks WHERE id=?', (row['id'],)).fetchone()
            self.assertEqual(saved['submitted'], late.isoformat())
            self.assertEqual(json.loads(saved['answers'])[0]['id'], 'x')

    def test_downtime_does_not_replay_every_missed_reminder(self):
        self.service.tick(at(DAY + 'T15:10:00'))
        self.assertEqual(self.sender.call_count, 2)
        self.assertTrue(all(':3:' in c.args[2] for c in self.sender.call_args_list))
        self.service.tick(at(DAY + 'T15:11:00'))
        self.assertEqual(self.sender.call_count, 2)

    def test_restart_reuses_roster_tasks_and_completed_deliveries(self):
        self.service.tick(self.current)
        new = CheckService(self.temp.name, roster=self.roster, cloud=self.cloud, sender=self.sender, portal_url=lambda: 'http://portal')
        new.tick(self.current + dt.timedelta(minutes=1))
        self.assertEqual(self.sender.call_count, 2)
        self.assertEqual(self.roster.call_count, 1)
        self.submit()
        new.tick(self.current + dt.timedelta(hours=2))
        self.assertEqual(self.sender.call_count, 2)

    def test_concurrent_and_repeated_submissions_keep_one_winner(self):
        self.service.prepare(self.current)
        barrier = threading.Barrier(2)
        def send(actor):
            barrier.wait()
            return self.submit(actor)
        with ThreadPoolExecutor(2) as pool:
            results = list(pool.map(send, [self.worker, self.peer]))
        self.assertEqual(sum(not r['already_completed'] for r in results), 1)
        with self.service.connection() as conn:
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM outbox').fetchone()[0], 1)
        self.assertTrue(self.submit()['already_completed'])

    def test_mutual_exclusivity_missing_notes_and_identity(self):
        self.service.prepare(self.current)
        for answers in [[], self.answers[:1], [{'id': 'x', 'status': 'both', 'note': ''}, self.answers[1]],
                        [self.answers[0], {'id': 'y', 'status': 'abnormal', 'note': ''}], [self.answers[0], self.answers[0]]]:
            with self.assertRaises(CheckError):
                self.service.submit(self.worker, DAY + ':A:白', 1, answers, self.current)
        with self.assertRaises(CheckError):
            self.submit({**self.worker, 'id': 'ou_unassigned', 'category': 'manager'})
        with self.assertRaises(CheckError):
            self.submit({**self.worker, 'home_scope': 'B'})
        self.assertFalse(self.submit({**self.worker, 'id': 'ou_shared', 'category': 'duty'})['already_completed'])

    def test_definition_edit_authority_conflict_and_historical_snapshot(self):
        with self.assertRaises(CheckError):
            self.service.save_definition(self.worker, 'A', 1, [])
        supervisor = {**self.worker, 'level': 'L3', 'category': 'supervisor'}
        with self.assertRaises(CheckError):
            self.service.save_definition(supervisor, 'B', 0, [])
        self.service.prepare(self.current)
        self.submit()
        with patch('lan_bitable_template_portal.pre_shift_checks.now', return_value=self.current):
            self.service.save_definition(supervisor, 'A', 1, [{'id': 'new', 'text': '新的测试项'}])
        with self.assertRaises(CheckError):
            self.service.save_definition(self.admin, 'A', 1, [])
        self.assertEqual(len(self.service.state(self.worker, 'A', self.current)['tasks'][0]['items']), 2)
        self.assertTrue(self.submit()['already_completed'])

    def test_open_check_updates_immediately_and_stale_draft_cannot_submit(self):
        self.service.prepare(self.current)
        with patch('lan_bitable_template_portal.pre_shift_checks.now', return_value=self.current):
            self.service.save_definition(self.admin, 'A', 1, [{'id': 'new', 'text': '新的测试项'}])
        task = self.service.state(self.worker, 'A', self.current)['tasks'][0]
        self.assertEqual(task['items'], [{'id': 'new', 'text': '新的测试项'}])
        self.assertEqual(task['definition_version'], 2)
        with self.assertRaises(CheckError) as error:
            self.submit()
        self.assertEqual(error.exception.status, 409)
        self.roster.assert_called_once()
        self.sender.assert_not_called()

    def test_clear_hides_pending_and_add_uses_cached_roster_without_worker_tick(self):
        self.service.tick(self.current)
        with patch('lan_bitable_template_portal.pre_shift_checks.now', return_value=self.current):
            self.service.save_definition(self.admin, 'A', 1, [])
            self.assertEqual(self.service.state(self.worker, 'A', self.current)['tasks'], [])
            self.service.notify(self.current + dt.timedelta(hours=2))
            self.assertEqual(self.sender.call_count, 2)
            self.service.save_definition(self.admin, 'A', 2, [{'id': 'new', 'text': '恢复测试项'}])
        task = self.service.state(self.worker, 'A', self.current)['tasks'][0]
        self.assertEqual(task['definition_version'], 3)
        self.assertEqual(task['items'][0]['text'], '恢复测试项')
        self.roster.assert_called_once()

    def test_first_definition_creates_task_immediately_with_cached_roster(self):
        with self.service.connection(True) as conn:
            people = [{**p, 'scope': 'B'} for p in self.roster.return_value['people']]
            conn.execute('INSERT INTO rosters(day,people) VALUES(?,?)', (DAY, json.dumps(people)))
        with patch('lan_bitable_template_portal.pre_shift_checks.now', return_value=self.current):
            self.service.save_definition(self.admin, 'B', 0, [{'id': 'b', 'text': 'B楼测试项'}])
        state = self.service.state({**self.worker, 'home_scope': 'B'}, 'B', self.current)
        self.assertEqual(state['tasks'][0]['items'][0]['text'], 'B楼测试项')
        self.roster.assert_not_called()

    def test_past_shift_keeps_unsubmitted_snapshot_after_edit(self):
        self.service.prepare(self.current)
        with patch('lan_bitable_template_portal.pre_shift_checks.now', return_value=at('2026-10-15T09:00:00')):
            self.service.save_definition(self.admin, 'A', 1, [])
        with self.service.connection() as conn:
            row = conn.execute('SELECT * FROM tasks WHERE id=?', (DAY + ':A:白',)).fetchone()
        self.assertEqual(len(json.loads(row['items'])), 2)

    def test_pre_upgrade_cleared_definition_hides_stale_pending_snapshot(self):
        self.service.prepare(self.current)
        with self.service.connection(True) as conn:
            conn.execute("UPDATE definitions SET items='[]' WHERE scope='A'")
        self.assertEqual(self.service.state(self.worker, 'A', self.current)['tasks'], [])
        with self.assertRaises(CheckError) as error:
            self.submit()
        self.assertEqual(error.exception.status, 409)
        self.service.notify(self.current)
        self.sender.assert_not_called()

    def test_cloud_failure_does_not_lose_submission_or_restart_reminders(self):
        self.service.tick(self.current)
        self.submit()
        self.cloud.upsert.side_effect = TimeoutError()
        self.service.tick(self.current + dt.timedelta(minutes=1))
        state = self.service.state(self.worker, 'A', self.current)
        self.assertTrue(state['tasks'][0]['submitted'])
        self.assertTrue(state['tasks'][0]['sync_error'])
        self.service.tick(self.current + dt.timedelta(minutes=2))
        self.assertEqual(self.cloud.upsert.call_count, 1)
        self.cloud.upsert.side_effect = None
        self.service.tick(self.current + dt.timedelta(hours=2))
        self.assertEqual(self.sender.call_count, 2)
        self.assertEqual(self.service.state(self.worker, 'A', self.current)['tasks'][0]['sync_status'], 'synced')

    def test_two_shifts_by_same_person_upsert_one_cloud_row(self):
        self.service.prepare(self.current)
        self.submit()
        self.service.sync(self.current)
        night = at(DAY + 'T17:50:00')
        self.service.prepare(night)
        self.submit(shift='夜', current=night)
        self.service.sync(night)
        self.assertEqual(self.cloud.upsert.call_args.args[2], 'rec_test')
        self.assertEqual(len(self.cloud.upsert.call_args.args[1]), 2)

    def test_roster_failure_backoff_and_empty_definition_never_completes(self):
        self.roster.side_effect = ValueError('incomplete')
        self.service.tick(self.current)
        self.service.tick(self.current + dt.timedelta(minutes=1))
        self.assertEqual(self.roster.call_count, 1)
        self.assertEqual(self.sender.call_count, 0)
        self.assertTrue(self.service.state(self.worker, 'A', self.current)['roster_error'])
        self.service.save_definition(self.admin, 'A', 1, [])
        self.service.tick(self.current + dt.timedelta(hours=1))
        self.assertEqual(self.roster.call_count, 1)

    def test_cloud_search_reconciles_unknown_create_and_preserves_one_record(self):
        self.service.prepare(self.current)
        self.submit()
        with self.service.connection() as conn:
            tasks = [dict(row) for row in conn.execute("SELECT * FROM tasks WHERE submitted!=''")]
        cloud = CheckCloud(None)
        cloud.schema_ready = True
        cloud.all = Mock(return_value=[{'record_id': 'rec_already_created'}])
        cloud.request = Mock(return_value={})
        self.assertEqual(cloud.upsert('key', tasks), 'rec_already_created')
        self.assertEqual(cloud.request.call_args.args[:2], ('PUT', 'records/rec_already_created'))
        fields = cloud.request.call_args.args[2]['fields']
        self.assertEqual(fields['检查项数'], 2)
        self.assertEqual(fields['异常项数'], 1)
        self.assertIn('测试异常说明', fields['异常备注'])
        cloud.all.return_value *= 2
        with self.assertRaises(CheckError):
            cloud.upsert('key', tasks)

    def test_each_abnormal_item_retains_its_own_note(self):
        self.service.prepare(self.current)
        self.answers = [{'id': 'x', 'status': 'abnormal', 'note': '第一项测试异常'},
                        {'id': 'y', 'status': 'abnormal', 'note': '第二项测试异常'}]
        self.submit()
        task = self.service.state(self.worker, 'A', self.current)['tasks'][0]
        self.assertEqual([(a['id'], a['note']) for a in task['answers']], [('x', '第一项测试异常'), ('y', '第二项测试异常')])
        with self.service.connection() as conn:
            rows = [dict(row) for row in conn.execute("SELECT * FROM tasks WHERE submitted!=''")]
        cloud = CheckCloud(None)
        cloud.schema_ready = True
        cloud.request = Mock(return_value={})
        cloud.upsert('key', rows, 'rec_existing')
        fields = cloud.request.call_args.args[2]['fields']
        self.assertIn('检查测试项一：异常；第一项测试异常', fields['检查明细'])
        self.assertIn('检查测试项二：异常；第二项测试异常', fields['检查明细'])
        self.assertEqual(fields['异常项数'], 2)

    def test_roster_pagination_filters_date_long_day_and_uses_real_person_ids(self):
        fields = {'排班日期': int(self.current.timestamp() * 1000), '班组': 'A班', '机楼': 'A楼', '班次': '白',
                  '人员（文本）':'一、二',
                  '人员': {'users': [{'id': 'ou_one', 'name': '一'}, {'id': 'ou_two', 'name': '二'}]}}
        reader = SimpleNamespace(_request_json=Mock(side_effect=[
            {'code': 0, 'data': {'items': [{'fields': {**fields, '班组': '长白'}}], 'has_more': True, 'page_token': 'p2'}},
            {'code': 0, 'data': {'items': [{'fields': fields}], 'has_more': False}}]),
            get_permission_directory_people=lambda:{'items':[{'record_id':oid,'name':name,'open_id':oid,'scopes':['A'],'selectable':True} for oid,name in [('ou_one','一'),('ou_two','二')]]})
        self.assertEqual([p['open_id'] for p in read_roster(reader, DAY)['people']], ['ou_one', 'ou_two'])
        reader._request_json = Mock(return_value={'code': 0, 'data': {'items': [{'fields': fields}], 'has_more': True, 'page_token': 'same'}})
        with self.assertRaises(CheckError):
            read_roster(reader, DAY)

    def test_ambiguous_building_roster_does_not_block_other_buildings(self):
        fields = {'排班日期': int(self.current.timestamp() * 1000), '班组': 'A班', '机楼': 'A楼', '班次': '白',
                  '人员（文本）':'一、二',
                  '人员': {'users': [{'id': 'ou_one'}, {'id': 'ou_two'}]}}
        data = {'code': 0, 'data': {'items': [{'fields': fields}, {'fields': {**fields, '机楼': '110站', '人员（文本）':'站员', '人员': {'users': [{'id': 'ou_station','name':'站员'}]}}}], 'has_more': False}}
        directory=[{'record_id':oid,'name':name,'open_id':oid,'scopes':[scope],'selectable':True} for oid,name,scope in [('ou_one','一','A'),('ou_two','二','A'),('ou_station','站员','110')]]
        result = read_roster(SimpleNamespace(_request_json=lambda *a, **kw: data,get_permission_directory_people=lambda:{'items':directory}), DAY)
        self.assertEqual(len(result['people']), 2)
        self.assertIn('110', result['warnings'])
        self.roster.return_value = result
        self.service.tick(self.current)
        self.assertEqual(self.sender.call_count, 2)
        self.assertFalse(self.service.state(self.worker, 'A', self.current)['roster_error'])
        self.assertIn('1/2人', self.service.state(self.admin, '110', self.current)['roster_error'])


class RouteTests(unittest.TestCase):
    def test_streamed_files_ownership_dedup_download_and_mid_upload_revocation(self):
        import asyncio
        import httpx
        from fastapi import FastAPI
        from lan_bitable_template_portal import pre_shift_routes
        from lan_bitable_template_portal.account_policy import business_delete_denial
        current = at(DAY + 'T09:00:00')
        user = {'open_id': 'ou_one', 'name': '上传人', 'home_scope': 'A', 'scope_rules': ['A'],
                'category': 'operator', 'modules': ['maintenance'], 'level': 'L4', 'can_delete_business': False}
        async def body(request, **kwargs): return await request.json()
        controller = SimpleNamespace(_current_session=lambda _: {'user': user}, _read_json_request=body,
                                     _request_base_url=lambda _: 'http://fixture')
        runtime = SimpleNamespace(service=Mock(), auth_manager=SimpleNamespace(public_user=lambda _: user,
            account_profile=lambda _: user), state_store=SimpleNamespace(record_business_operation_audit=Mock()))
        with tempfile.TemporaryDirectory() as root, patch('lan_bitable_template_portal.pre_shift_checks.now', return_value=current):
            service = CheckService(root, roster=lambda _: {'people': [{'scope': 'A', 'shift': '白', 'open_id': x, 'name': x} for x in ('ou_one', 'ou_two')]})
            service.save_definition({'id': 'admin', 'level': 'L1', 'scope_rules': ['ALL']}, 'A', 0, [{'id': 'x', 'text': '文件检查'}])
            service.prepare(current)
            app = FastAPI()
            with patch.object(pre_shift_routes, 'CheckService', return_value=service):
                pre_shift_routes.install_pre_shift_routes(app, controller, runtime)
                @app.middleware('http')
                async def deletion_guard(request, call_next):
                    result = await business_delete_denial(request, controller, runtime)
                    return result if result is not None else await call_next(request)
                async def exercise():
                    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://fixture') as client:
                        params = {'task_id': DAY + ':A:白', 'item_id': 'x', 'kind': 'item', 'name': '检查文件.txt'}
                        response = await client.post('/api/pre-shift-checks/files', params=params, content=b'first text')
                        self.assertEqual(response.status_code, 200, response.text)
                        file = response.json()['data']
                        again = await client.post('/api/pre-shift-checks/files', params=params, content=b'first text')
                        self.assertEqual(again.json()['data']['id'], file['id'])
                        self.assertFalse(list(service.staging_dir.iterdir()))
                        download = await client.get(file['url'])
                        self.assertEqual(download.content, b'first text')
                        self.assertIn('filename*=', download.headers['content-disposition'])
                        self.assertEqual(download.headers['x-content-type-options'], 'nosniff')
                        user.update(home_scope='B', scope_rules=['B'])
                        self.assertEqual((await client.get(file['url'])).status_code, 403)
                        user.update(home_scope='A', scope_rules=['A'], open_id='ou_two')
                        self.assertEqual((await client.delete(file['url'])).status_code, 403)
                        user['open_id'] = 'ou_one'
                        denied = await client.delete(file['url'], headers={'origin': 'http://evil.test'})
                        self.assertEqual(denied.status_code, 403)
                        self.assertEqual((await client.delete(file['url'])).status_code, 200)
                        self.assertEqual((await client.get(file['url'])).status_code, 404)
                        async def stream():
                            yield b'part one'
                            user['modules'] = []
                            yield b'part two'
                        revoked = await client.post('/api/pre-shift-checks/files', params=params, content=stream())
                        self.assertEqual(revoked.status_code, 403, revoked.text)
                        self.assertFalse(list(service.staging_dir.iterdir()))
                        with service.connection() as conn:
                            self.assertEqual(conn.execute('SELECT COUNT(*) FROM files').fetchone()[0], 0)
                asyncio.run(exercise())

    def test_session_scope_csrf_and_actor_are_server_derived(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from lan_bitable_template_portal import pre_shift_routes
        user = {'open_id': 'ou_fixture', 'name': '值班员', 'home_scope': 'A', 'category': 'operator', 'modules': ['maintenance']}
        session = {'user': user}
        async def body(request, **kwargs):
            return await request.json()
        controller = SimpleNamespace(_current_session=lambda r: session, _read_json_request=body, _request_base_url=lambda r: str(r.base_url))
        svc = Mock()
        svc.state.return_value = {}
        svc.submit.return_value = {'completed': True}
        runtime = SimpleNamespace(service=Mock(), auth_manager=SimpleNamespace(public_user=lambda s: user))
        with patch.object(pre_shift_routes, 'CheckService', return_value=svc):
            app = FastAPI()
            pre_shift_routes.install_pre_shift_routes(app, controller, runtime)
            with TestClient(app) as client:
                response = client.get('/api/pre-shift-checks?scope=A')
                self.assertEqual(response.status_code, 200)
                self.assertEqual(svc.state.call_args.args[0]['id'], 'ou_fixture')
                payload = {'task_id': 'task', 'version': 1, 'answers': []}
                self.assertEqual(client.post('/api/pre-shift-checks/submit', json=payload, headers={'Origin': 'https://evil.test'}).status_code, 403)
                self.assertEqual(client.post('/api/pre-shift-checks/submit', json={**payload, 'id': 'ou_other'}).status_code, 400)
                user['category'] = 'other'
                self.assertEqual(client.get('/api/pre-shift-checks').status_code, 403)
                session = None
                self.assertEqual(client.get('/api/pre-shift-checks').status_code, 401)


if __name__ == '__main__':
    unittest.main()
