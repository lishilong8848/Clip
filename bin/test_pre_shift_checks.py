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
from lan_bitable_template_portal.pre_shift_checks import CheckService, CheckError, CheckDeliveryUncertain, TZ, windows
from lan_bitable_template_portal.pre_shift_cloud import CheckCloud, read_roster

DAY = '2026-10-14'
def at(value):
    return dt.datetime.fromisoformat(value).replace(tzinfo=TZ)

def receipt(recipient, *_):
    return True, 'ok', [{'open_id': recipient, 'message_id': 'om_test', 'sender_app_id': 'cli_agent'}]


class CheckTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.admin = {'id': 'ou_admin', 'name': '管理员', 'level': 'L1', 'scope_rules': ['ALL'], 'category': 'manager'}
        self.worker = {'id': 'ou_staff1', 'name': '值班员一', 'level': 'L4', 'scope_rules': ['A'], 'home_scope': 'A', 'category': 'operator'}
        self.peer = {**self.worker, 'id': 'ou_staff2', 'name': '值班长二', 'category': 'shift_leader'}
        self.roster = Mock(return_value={'people': [{'scope': 'A', 'shift': shift, 'open_id': f'ou_staff{i}', 'name': f'人员{i}'} for shift in ('白', '夜') for i in (1, 2)], 'warnings': {}})
        self.sender = Mock(side_effect=receipt)
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
        self.service.tick(self.current - dt.timedelta(minutes=10, seconds=1))
        self.assertEqual(self.sender.call_count, 0)
        self.service.tick(self.current - dt.timedelta(minutes=10))
        self.assertEqual(self.sender.call_count, 2)
        self.service.tick(self.current + dt.timedelta(minutes=119))
        self.assertEqual(self.sender.call_count, 2)
        self.service.tick(self.current + dt.timedelta(hours=2))
        self.assertEqual(self.sender.call_count, 4)
        self.submit(self.peer, current=self.current + dt.timedelta(hours=2, minutes=1))
        self.service.tick(self.current + dt.timedelta(hours=4))
        self.assertEqual(self.sender.call_count, 4)
        self.assertEqual(self.roster.call_count, 2, 'initial read plus one pre-reminder roster recheck')
        self.assertEqual(self.service.state(self.worker, 'A', self.current)['tasks'][0]['actor_name'], '值班长二')
        self.assertEqual(self.cloud.upsert.call_count, 1)

    def test_night_prealert_groups_items_and_does_not_repeat_at_due_or_restart(self):
        due = at(DAY + 'T17:50:00')
        self.service.prepare(due)
        self.service.submit(self.worker, DAY + ':A:白', 1, self.answers, due)
        self.service.notify(due - dt.timedelta(minutes=10, seconds=1))
        self.sender.assert_not_called()
        self.service.notify(due - dt.timedelta(minutes=10))
        self.assertEqual(self.sender.call_count, 2)
        for call in self.sender.call_args_list:
            self.assertIn(':夜:0:', call.args[2])
            content = call.args[1]['elements'][0]['text']['content']
            self.assertIn('共2项', content)
            self.assertIn('检查测试项一', content)
            self.assertIn('检查测试项二', content)
        restarted = CheckService(self.temp.name, roster=self.roster, sender=self.sender, portal_url=lambda:'https://fixture.invalid')
        restarted.tick(due)
        self.assertEqual(self.sender.call_count, 2)
        self.submit(self.peer, shift='夜', current=due)
        restarted.tick(due + dt.timedelta(hours=2))
        self.assertEqual(self.sender.call_count, 2)

    def test_prealert_partial_roster_sends_resolved_person_then_only_missing_person(self):
        full = self.roster.return_value['people']
        self.roster.side_effect = [{'people':[p for p in full if p['open_id']=='ou_staff1'], 'warnings':{'A':'一人待核对'}},
                                  {'people':full,'warnings':{}}]
        current = self.current - dt.timedelta(minutes=10)
        self.service.tick(current)
        self.assertEqual(self.sender.call_count, 1)
        self.service.tick(current + dt.timedelta(minutes=5))
        self.assertEqual([c.args[0] for c in self.sender.call_args_list], ['ou_staff1','ou_staff2'])

    def test_uncertain_receipts_are_persisted_and_not_retried_beyond_dedup_window(self):
        self.sender.side_effect = CheckDeliveryUncertain('回执丢失')
        self.service.tick(self.current)
        self.service.tick(self.current + dt.timedelta(minutes=5))
        self.assertEqual(self.sender.call_count, 4)
        self.assertEqual(self.sender.call_args_list[0].args[2], self.sender.call_args_list[2].args[2])
        restarted = CheckService(self.temp.name, roster=self.roster, sender=self.sender, portal_url=lambda:'https://fixture.invalid')
        restarted.tick(self.current + dt.timedelta(minutes=50))
        self.assertEqual(self.sender.call_count, 4)
        with self.service.connection() as db:
            rows = db.execute('SELECT sent,message_id,error FROM deliveries').fetchall()
        self.assertTrue(all(row['sent']==0 and row['message_id']=='' and row['error'] for row in rows))

    def test_definite_failure_can_retry_later_and_requires_valid_receipt(self):
        self.sender.side_effect = CheckError('应用暂未就绪')
        self.service.tick(self.current)
        self.sender.side_effect = lambda *_:(True, 'ok', [])
        self.service.tick(self.current + dt.timedelta(hours=1))
        with self.service.connection() as db:
            self.assertEqual(db.execute('SELECT SUM(sent) FROM deliveries').fetchone()[0],0)
        self.sender.side_effect = receipt
        self.service.tick(self.current + dt.timedelta(hours=1, minutes=5))
        with self.service.connection() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM deliveries WHERE sent=1 AND message_id!=\'\'').fetchone()[0],2)

    def test_unconfigured_scope_hides_cached_roster_errors_and_never_sends(self):
        self.service.prepare(self.current)
        with self.service.connection(True) as db:
            db.execute('UPDATE rosters SET error=?', (json.dumps({'110':'未找到身份'}),))
        self.service.last_error = '后台发送暂未完成'
        self.assertEqual(self.service.state(self.admin,'110',self.current)['roster_error'],'')
        self.service.save_definition(self.admin,'A',1,[])
        self.service.tick(self.current)
        self.sender.assert_not_called()
        self.assertEqual(self.service.state(self.admin,'A',self.current)['roster_error'],'')

    def test_due_reminders_run_before_cloud_attachment_sync(self):
        order = []
        with patch.object(self.service,'prepare',side_effect=lambda *_:order.append('prepare')), patch.object(
            self.service,'notify',side_effect=lambda *_:order.append('notify')), patch.object(
            self.service,'sync',side_effect=lambda *_:order.append('sync')):
            self.service.tick(self.current)
        self.assertEqual(order[:3],['prepare','notify','sync'])

    def test_missing_or_cleared_checklist_never_sends_initial_or_repeat_reminders(self):
        self.service.prepare(self.current)
        for remove in (False, True):
            with self.service.connection(True) as conn:
                if remove:
                    conn.execute("DELETE FROM definitions WHERE scope='A'")
                else:
                    conn.execute("UPDATE definitions SET items='[]' WHERE scope='A'")
            for hours in (0, 2, 4):
                self.service.notify(self.current + dt.timedelta(hours=hours))
        self.sender.assert_not_called()

    def test_clearing_definition_during_first_send_stops_remaining_recipient(self):
        self.service.prepare(self.current)
        def send(*_):
            self.service.save_definition(self.admin, 'A', 1, [])
            return receipt(*_)
        self.sender.side_effect = send
        self.service.notify(self.current)
        self.service.notify(self.current + dt.timedelta(hours=2))
        self.assertEqual(self.sender.call_count, 1)

    def test_superseded_definition_does_not_send_stale_task(self):
        self.service.prepare(self.current)
        with self.service.connection(True) as conn:
            conn.execute("UPDATE definitions SET version=version+1 WHERE scope='A'")
        self.service.notify(self.current)
        self.sender.assert_not_called()

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
            return receipt(*_)
        self.sender.side_effect = send
        self.service.notify(self.current)
        self.assertEqual(self.sender.call_count, 1)
        self.service.notify(self.current + dt.timedelta(hours=2))
        self.assertEqual(self.sender.call_count, 1)

    def test_supervisor_completion_isolated_by_building_shift_due_and_restart(self):
        items=[{'id':'x','text':'检查一'},{'id':'y','text':'检查二'},
               {'id':'later','text':'另一时段','frequency':{'kind':'daily','time':'11:00'}}]
        self.service.save_definition(self.admin,'A',1,items)
        self.service.save_definition(self.admin,'B',0,items)
        self.roster.return_value['people'] += [{**p,'scope':'B'} for p in list(self.roster.return_value['people'])]
        current=at(DAY+'T11:05:00');self.service.prepare(current)
        supervisor={**self.worker,'id':'ou_supervisor','name':'A楼主管','category':'supervisor','level':'L3'}
        self.service.notify(current)
        self.service.submit(supervisor,DAY+':A:白',2,self.answers,current)
        with self.assertRaises(CheckError):
            self.service.submit(supervisor,DAY+':B:白',1,self.answers,current)
        self.sender.reset_mock();self.service.notify(current+dt.timedelta(hours=2))
        keys=[call.args[2] for call in self.sender.call_args_list]
        self.assertEqual(len(keys),6)
        self.assertFalse(any(key.startswith('pre-shift:'+DAY+':A:白:2:') for key in keys))
        self.assertTrue(any(':A:白:1100:' in key for key in keys))
        self.assertTrue(any(':B:白:' in key for key in keys))
        restarted=CheckService(self.temp.name,roster=self.roster,sender=self.sender,portal_url=lambda:'https://fixture.invalid')
        self.sender.reset_mock();restarted.tick(current+dt.timedelta(hours=2,minutes=1))
        self.sender.assert_not_called()
        later=DAY+':A:白:1100'
        restarted.submit(supervisor,later,2,[{'id':'later','status':'normal','note':''}],current+dt.timedelta(hours=2))
        self.sender.reset_mock();restarted.notify(current+dt.timedelta(hours=4))
        self.assertTrue(self.sender.called)
        self.assertTrue(all(':B:白:' in call.args[2] for call in self.sender.call_args_list))
        self.sender.reset_mock();night=at(DAY+'T17:50:00');restarted.tick(night)
        self.assertTrue(any(':A:夜:' in call.args[2] for call in self.sender.call_args_list))
        restarted.submit(supervisor,DAY+':A:夜',2,self.answers,night)
        self.sender.reset_mock();restarted.tick(at('2026-10-15T01:50:00'))
        self.assertTrue(self.sender.called)
        self.assertTrue(all(':B:夜:' in call.args[2] for call in self.sender.call_args_list))
        self.sender.reset_mock();restarted.tick(at('2026-10-15T08:50:00'))
        self.assertTrue(any('2026-10-15:A:白:' in call.args[2] for call in self.sender.call_args_list))

    def test_overview_attention_keeps_other_due_and_overdue_periods(self):
        self.service.prepare(self.current)
        tomorrow=at('2026-10-15T09:00:00');self.service.prepare(tomorrow)
        # Finish today's work, but leave yesterday's day shift for a late check.
        self.service.submit(self.admin,DAY+':A:夜',1,self.answers,tomorrow)
        self.service.submit(self.admin,'2026-10-15:A:白',1,self.answers,tomorrow)
        state=self.service.state(self.admin,'A',tomorrow)
        self.assertFalse(any(row['status'] in ('待完成','逾期待补做') for row in state['overview']['rows']))
        self.assertEqual(state['overview']['pending'],1,'an old unfinished period still needs attention in the global view')
        self.service.submit(self.admin,DAY+':A:白',1,self.answers,tomorrow)
        self.assertEqual(self.service.state(self.admin,'A',tomorrow)['overview']['pending'],0)

    def test_real_patch_install_preserves_checks_and_receipts_and_enables_supervisor(self):
        import importlib.util
        import package_portable as packaging
        from upload_event_module.ui.main_window_patch import PatchUpdateMixin
        with tempfile.TemporaryDirectory(prefix='pre_shift_update_') as directory:
            root=Path(directory);client=root/'client';data=client/'bin/data/pre_shift_checks'
            sender=Mock(side_effect=receipt)
            before=CheckService(data,roster=self.roster,sender=sender,portal_url=lambda:'https://fixture.invalid')
            before.save_definition(self.admin,'A',0,[{'id':'x','text':'更新前的检查'}]);before.prepare(self.current)
            session=before.stage_upload(self.worker,DAY+':A:白','x','item','检查记录.txt',self.current)
            Path(session['path']).write_bytes(b'original evidence')
            photo=before.commit_upload(self.worker,session,None,self.current)
            before.submit(self.worker,DAY+':A:白',1,[{'id':'x','status':'normal','note':''}],self.current)
            night=at(DAY+'T17:50:00');before.notify(night-dt.timedelta(minutes=10))
            self.assertEqual(sender.call_count,2)
            location_config=data/'location_settings.json';location_config.write_bytes(b'{"key_cipher":"isolated fixture"}')
            protected={str(data.relative_to(client)/'checks.sqlite3'):before.db.read_bytes(),
                       str(location_config.relative_to(client)):location_config.read_bytes(),
                       str(Path(before.get_file(self.worker,photo['id'])['path']).relative_to(client)):b'original evidence',
                       'bin/data/config.json':b'{"fixture":"preserve account settings"}'}
            config=client/'bin/data/config.json';config.write_bytes(protected['bin/data/config.json'])
            name='pre_shift_isolated_update';output=root/'build'
            with patch.object(packaging,'BUILD_DIR',output):
                packaging.build_patch(root/'unused',client,name,force_ui_update=True)
            payload=output/(name+'_patch_only')
            expected=['bin/lan_bitable_template_portal/'+part+'.py' for part in
                      ('pre_shift_checks','pre_shift_schedule','pre_shift_routes','pre_shift_cloud','pre_shift_photo','pre_shift_location','pre_shift_location_distribution')]
            location_spec='bin/lan_bitable_template_portal/photo-location-distribution.json'
            if (packaging.PROJECT_ROOT/location_spec).exists():
                expected.append(location_spec)
            expected+=['bin/clipflow_backend/main.py','bin/lan_bitable_template_portal/frontend/dist/index.html',
                       'bin/lan_bitable_template_portal/frontend/dist/assets/workspace-v2/index.html']
            for relative in expected:
                self.assertEqual((payload/relative).read_bytes(),(packaging.PROJECT_ROOT/relative).read_bytes())
            self.assertFalse((payload/'bin/data').exists())
            meta=json.loads((payload/'bin/patch_meta.json').read_text(encoding='utf-8'))
            self.assertTrue(meta['restart_required'])
            # A stale update must not overwrite or delete the installed checklist database/evidence.
            for relative in protected:
                target=payload/relative;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(b'must not install')
            with (payload/'patch_manifest.txt').open('a',encoding='utf-8') as manifest:
                manifest.write('\n'+'\n'.join(protected)+'\n')
            class Client(PatchUpdateMixin):
                def _get_app_root_dir(self):
                    return client
            updater=Client();updater._last_patch_meta=meta;updater._last_patch_source='isolated-test'
            updater.patch_update_finished=SimpleNamespace(emit=Mock())
            guard=Mock();guard.finish.return_value=''
            with patch('openclaw_service.update.AssistantUpdateGuard',return_value=guard),patch(
                'upload_event_module.services.process_lifetime.lower_current_thread_priority'),patch(
                'upload_event_module.ui.main_window_patch.config.auto_install_dependencies',False),patch(
                'upload_event_module.ui.main_window_patch.send_system_alert') as alert:
                updater._apply_patch_worker(payload)
                alert.assert_not_called()
            self.assertTrue(updater.patch_update_finished.emit.call_args.args[0],updater.patch_update_finished.emit.call_args)
            self.assertTrue(updater._last_patch_requires_restart)
            for relative,content in protected.items():
                self.assertEqual((client/relative).read_bytes(),content)
            spec=importlib.util.spec_from_file_location('lan_bitable_template_portal._updated_pre_shift',client/expected[0])
            installed=importlib.util.module_from_spec(spec);spec.loader.exec_module(installed)
            after=installed.CheckService(data,roster=self.roster,sender=sender,portal_url=lambda:'https://fixture.invalid')
            sender.reset_mock();after.tick(night-dt.timedelta(minutes=5));sender.assert_not_called()
            supervisor={**self.worker,'id':'ou_supervisor','name':'A楼主管','category':'supervisor','level':'L3'}
            row=next(t for t in after.state(supervisor,'A',night)['tasks'] if t['id']==DAY+':A:夜')
            self.assertTrue(row['can_submit'])
            after.submit(supervisor,row['id'],1,[{'id':'x','status':'normal','note':''}],night)
            after.tick(at('2026-10-15T01:50:00'));sender.assert_not_called()
            self.assertEqual(after.state(self.worker,'A',night)['overview'],None)
            self.assertEqual(after.state(self.admin,'A',night)['overview']['pending'],0)
            after.tick(at('2026-10-15T08:50:00'))
            self.assertEqual(sender.call_count,2,'new day must not inherit the previous completion')

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
        barrier = threading.Barrier(3)
        def send(actor):
            barrier.wait()
            return self.submit(actor)
        with ThreadPoolExecutor(3) as pool:
            results = list(pool.map(send, [self.worker, self.peer, self.admin]))
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
            self.submit({**self.worker, 'id': 'ou_unassigned', 'category': 'engineer'})
        with self.assertRaises(CheckError):
            self.submit({**self.worker, 'home_scope': 'B'})
        self.assertFalse(self.submit({**self.worker, 'id': 'ou_shared', 'category': 'duty'})['already_completed'])

    def test_superiors_submit_only_in_their_managed_buildings_and_stop_reminders(self):
        cases = [
            ('root', 'L0', 'manager', 'H', ['ALL'], True),
            ('manager', 'L1', 'manager', 'H', ['ALL'], True),
            ('h_duty', 'L1', 'duty', 'H', ['ALL'], True),
            ('supervisor', 'L3', 'supervisor', 'A', ['A'], True),
            ('other_supervisor', 'L3', 'supervisor', 'B', ['B'], False),
            ('limited_manager', 'L1', 'manager', 'B', ['B'], False),
            ('h_engineer', 'L2', 'engineer', 'H', ['H'], False),
            ('engineer', 'L4', 'engineer', 'A', ['A'], False),
            ('operator', 'L4', 'operator', 'A', ['A'], True),
            ('other_operator', 'L4', 'operator', 'B', ['B'], False),
        ]
        for name,level,category,home,scopes,allowed in cases:
            with self.subTest(name=name):
                sender=Mock(side_effect=receipt)
                service=CheckService(Path(self.temp.name)/name,roster=self.roster,sender=sender)
                service.save_definition(self.admin,'A',0,[{'id':'x','text':'检查'}])
                service.prepare(self.current)
                actor={'id':'ou_'+name,'name':name,'level':level,'category':category,'home_scope':home,'scope_rules':scopes}
                with service.connection() as conn:
                    row=conn.execute('SELECT * FROM tasks WHERE id=?',(DAY+':A:白',)).fetchone()
                self.assertEqual(service.can_submit(actor,row,self.current),allowed)
                answers=[{'id':'x','status':'normal','note':''}]
                if not allowed:
                    with self.assertRaises(CheckError) as denied:
                        service.submit(actor,row['id'],1,answers,self.current)
                    self.assertEqual(denied.exception.status,403)
                    with self.assertRaises(CheckError):
                        service.stage_upload(actor,row['id'],'','handover','交接班.jpg',self.current)
                    continue
                task=next(t for t in service.state(actor,'A',self.current)['tasks'] if t['id']==row['id'])
                self.assertTrue(task['can_submit']);self.assertTrue(task['can_upload_handover'])
                for kind,item,file_name in [('item','x','记录.txt'),('handover','','交接班.jpg')]:
                    session=service.stage_upload(actor,row['id'],item,kind,file_name,self.current)
                    service.abort_upload(session)
                result=service.submit(actor,row['id'],1,answers,self.current)
                self.assertFalse(result['already_completed']);self.assertEqual(result['actor_name'],name)
                service.notify(self.current+dt.timedelta(hours=2))
                sender.assert_not_called()
                peer=next(t for t in service.state(self.worker,'A',self.current)['tasks'] if t['id']==row['id'])
                self.assertFalse(peer['can_submit']);self.assertEqual(peer['actor_name'],actor['name'])

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
        self.assertEqual(len(result['people']), 3)
        self.assertIn('夜班排班未读取到', result['warnings']['110'])
        self.roster.return_value = result
        self.service.tick(self.current)
        self.assertEqual(self.sender.call_count, 2)
        self.assertIn('夜班排班未读取到', self.service.state(self.worker, 'A', self.current)['roster_error'])
        self.assertEqual(self.service.state(self.admin, '110', self.current)['roster_error'], '')


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
                svc.start.assert_called_once()
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
            svc.stop.assert_called_once()


if __name__ == '__main__':
    unittest.main()
