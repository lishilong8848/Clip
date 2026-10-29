"""Isolated unit coverage for the roster reader + identity resolver of shift reminders.

Tests only the current implementation in lan_bitable_template_portal.learning_reminders
(read_roster / resolve_roster / enqueue_shift_reminders).  No network, no config,
no service startup; every feed is a synthetic row / Mock.  Defects in the current
implementation surface as failing assertions and are reported separately.

Run:  bin/.venv/Scripts/python.exe -m unittest bin.test_learning_roster_resolution
"""
import datetime as dt
from contextlib import nullcontext
from unittest import TestCase
from unittest.mock import Mock, patch

from bin.test_learning import learning  # noqa: F401  (bootstraps sys.path + TZ)
from lan_bitable_template_portal import learning_reminders as reminders

DAY = '2026-10-10'
DAY_TS = int(dt.datetime(2026, 10, 10, tzinfo=learning.TZ).timestamp() * 1000)


def hr_row(name, scope, oid, *, selectable=True, inactive=False, position='值班员',
           scopes=None, login_ids=None):
    """One local HR directory row shaped for _hr_active/_hr_learning_scopes."""
    return {'name': name, 'scopes': scopes if scopes is not None else [scope],
            'selectable': selectable, 'inactive': inactive,
            'login_ids': login_ids if login_ids is not None else ([oid] if oid else []),
            'position': position}


class ReadRosterTests(TestCase):
    def test_employee_pool_snapshot_filters_reminders_but_not_queries(self):
        from lan_bitable_template_portal import personnel_source
        snapshot = {'people': [
            {'record_id':'rec1', 'name':'值班甲', 'open_id':'ou_a', 'scope':'A', 'shift':'白', 'duty_kind':'白'},
            {'record_id':'rec2', 'name':'长白乙', 'open_id':'ou_b', 'scope':'A', 'shift':'白', 'duty_kind':'长白'},
            {'record_id':'rec3', 'name':'站员丙', 'open_id':'ou_c', 'scope':'110', 'shift':'夜', 'duty_kind':'夜'}]}
        with patch.object(personnel_source, 'daily_roster', return_value=snapshot) as read:
            self.assertEqual([p['name'] for p in reminders.read_roster(Mock(), DAY)], ['值班甲'])
            self.assertEqual(len(reminders.read_roster(Mock(), DAY, scopes=['A','110'], include_long_day=True)), 3)
            self.assertEqual(read.call_args.args[1], DAY)

    def test_incomplete_employee_pool_never_becomes_empty_roster(self):
        from lan_bitable_template_portal import personnel_source
        with patch.object(personnel_source, 'daily_roster', side_effect=ValueError('incomplete')):
            with self.assertRaises(ValueError): reminders.read_roster(Mock(), DAY)


class ResolveRosterTests(TestCase):
    def setUp(self):
        # Keep shared-duty map empty unless a test specifically wants it populated.
        self.shared_patch = patch('lan_bitable_template_portal.portal_service.BUILDING_OPEN_ID_MAP', {})
        self.shared_patch.start()
        self.addCleanup(self.shared_patch.stop)

    def resolve(self, people, directory):
        return reminders.resolve_roster(people, directory)

    def test_exact_single_same_building_active_resolves(self):
        people = [{'open_id': '', 'shift': '白', 'scope': 'A', 'name': '张三'}]
        resolved, warnings = self.resolve(people, {'p': hr_row('张三', 'A', 'ou_z')})
        self.assertEqual(resolved, [{'open_id': 'ou_z', 'shift': '白', 'scope': 'A', 'name': '张三'}])
        self.assertEqual(warnings, [])

    def test_employee_link_cleared_never_uses_cached_account_or_namesake(self):
        person={'record_id':'recA','name':'张三','open_id':'ou_old','scope':'A','shift':'白','employee_linked':False}
        directory={'recA':hr_row('张三','A','ou_old'),'recB':hr_row('张三','A','ou_other')}
        self.assertEqual(self.resolve([person],directory),([],[]))
        person['employee_linked']=True
        directory['recA']['login_ids']=[]
        self.assertEqual(self.resolve([person],directory),([],[]))

    def test_same_person_record_uses_new_account_without_name_based_substitution(self):
        person={'record_id':'recA','name':'张三','open_id':'ou_old','scope':'A','shift':'白'}
        directory={'recA':hr_row('张三','A','ou_new'),'recB':hr_row('张三','A','ou_other')}
        resolved,warnings=self.resolve([person],directory)
        self.assertEqual([p['open_id'] for p in resolved],['ou_new'])
        self.assertEqual(warnings,[])
        directory.pop('recA')
        resolved,warnings=self.resolve([person],directory)
        self.assertEqual(resolved,[])
        self.assertEqual(len(warnings),1)
        directory['recA']=hr_row('张三','A','',login_ids=['ou_one','ou_two'])
        self.assertEqual(self.resolve([person],directory)[0],[])

    def test_blank_position_is_allowed_no_role_filter(self):
        row = hr_row('张三', 'A', 'ou_z', position='')
        row.pop('position')
        resolved, warnings = self.resolve(
            [{'open_id': '', 'shift': '白', 'scope': 'A', 'name': '张三'}], {'p': row})
        self.assertEqual([p['open_id'] for p in resolved], ['ou_z'])
        self.assertEqual(warnings, [])

    def test_inactive_person_not_resolved(self):
        directory = {'p': hr_row('张三', 'A', 'ou_z', inactive=True)}
        resolved, warnings = self.resolve(
            [{'open_id': '', 'shift': '白', 'scope': 'A', 'name': '张三'}], directory)
        self.assertEqual(resolved, [])
        self.assertEqual(len(warnings), 1)

    def test_multi_building_person_never_matches_single_scope(self):
        # [_hr_learning_scopes] returns [] for two declared buildings, so never a candidate.
        row = hr_row('张三', 'A', 'ou_z', scopes=['A', 'B'])
        resolved, warnings = self.resolve(
            [{'open_id': '', 'shift': '白', 'scope': 'A', 'name': '张三'}], {'p': row})
        self.assertEqual(resolved, [])

    def test_same_name_different_building_is_not_ambiguous_for_this_scope(self):
        people = [{'open_id': '', 'shift': '白', 'scope': 'A', 'name': '张三'}]
        directory = {
            'a': hr_row('张三', 'A', 'ou_a'),
            'h': hr_row('张三', 'H', 'ou_h'),
        }
        resolved, warnings = self.resolve(people, directory)
        self.assertEqual([p['open_id'] for p in resolved], ['ou_a'])

    def test_two_same_name_same_scope_is_ambiguous_unresolved(self):
        people = [{'open_id': '', 'shift': '白', 'scope': 'A', 'name': '张三'}]
        directory = {
            'a1': hr_row('张三', 'A', 'ou_a1'),
            'a2': hr_row('张三', 'A', 'ou_a2'),
        }
        resolved, warnings = self.resolve(people, directory)
        self.assertEqual(resolved, [])
        self.assertEqual(len(warnings), 1)

    def test_exact_linked_id_disambiguates_same_name(self):
        people = [{'open_id': 'ou_a2', 'shift': '白', 'scope': 'A', 'name': '张三'}]
        directory = {
            'a1': hr_row('张三', 'A', 'ou_a1'),
            'a2': hr_row('张三', 'A', 'ou_a2'),
        }
        resolved, warnings = self.resolve(people, directory)
        self.assertEqual([p['open_id'] for p in resolved], ['ou_a2'])
        self.assertEqual(warnings, [])

    def test_shared_duty_openid_rejected(self):
        with patch('lan_bitable_template_portal.portal_service.BUILDING_OPEN_ID_MAP',
                   {'A': 'ou_duty'}):
            people = [{'open_id': 'ou_duty', 'shift': '白', 'scope': 'A', 'name': '张三'}]
            resolved, warnings = self.resolve(people, {'p': hr_row('张三', 'A', 'ou_duty')})
        self.assertEqual(resolved, [])
        self.assertEqual(len(warnings), 1)

    def test_duplicate_owned_openid_rejected(self):
        # The open id is owned by two distinct HR rows (one inactive), so owners>1.
        directory = {
            'active': {'name': '张三', 'scopes': ['A'], 'selectable': True,
                       'inactive': False, 'login_ids': ['ou_dup']},
            'other': {'name': '李四', 'scopes': ['H'], 'selectable': True,
                      'inactive': True, 'login_ids': ['ou_dup']},
        }
        people = [{'open_id': 'ou_dup', 'shift': '白', 'scope': 'A', 'name': '张三'}]
        resolved, warnings = self.resolve(people, directory)
        self.assertEqual(resolved, [])
        self.assertTrue(warnings)

    def test_stale_linked_stranger_never_substituted_for_active_namesake(self):
        # Regression: a stale/inactive linked identity must never pull in an active namesake.
        directory = {
            'old': {'name': '张三', 'scopes': ['A'], 'selectable': True,
                    'inactive': True, 'login_ids': ['ou_x']},
            'cur': {'name': '张三', 'scopes': ['A'], 'selectable': True,
                    'inactive': False, 'login_ids': ['ou_z']},
        }
        people = [{'open_id': 'ou_x', 'shift': '白', 'scope': 'A', 'name': '张三'}]
        resolved, warnings = self.resolve(people, directory)
        self.assertEqual(resolved, [])
        self.assertEqual(len(warnings), 1)

    def test_linked_different_name_stranger_never_overrides_full_name(self):
        # Linked ou_l belongs to 李四, but text 张三 must resolve to the unique active HR 张三.
        people = [{'open_id': 'ou_l', 'shift': '白', 'scope': 'A', 'name': '张三'}]
        resolved, warnings = self.resolve(people, {
            'z': hr_row('张三', 'A', 'ou_z'),
            'l': hr_row('李四', 'A', 'ou_l'),
        })
        self.assertEqual([p['open_id'] for p in resolved], ['ou_z'])
        self.assertEqual(warnings, [])

    def test_partial_no_and_wrong_linked_rows_resolve_only_present_hr(self):
        people = [{'name': name, 'open_id': oid, 'shift': '白', 'scope': 'A'}
                  for name, oid in [('张三','ou_l'),('李四',''),('王五',''),('赵六',''),('甲乙','ou_x'),('丙丁','ou_y')]]
        resolved, warnings = self.resolve(people, {
            'z': hr_row('张三', 'A', 'ou_z'),
            'l': hr_row('李四', 'A', 'ou_l'),
            'w': hr_row('王五', 'A', 'ou_w'),
            'j': hr_row('甲乙', 'A', 'ou_j'),
            's': hr_row('戊己', 'A', 'ou_s'),  # stranger not in roster
        })
        self.assertEqual([p['name'] for p in resolved],
                         ['张三', '李四', '王五', '甲乙'])
        self.assertEqual({p['open_id'] for p in resolved},
                         {'ou_z', 'ou_l', 'ou_w', 'ou_j'})
        self.assertNotIn('戊己', [p['name'] for p in resolved])
        self.assertIn('赵六', '；'.join(warnings))
        self.assertIn('丙丁', '；'.join(warnings))

    def test_recovery_when_directory_updated(self):
        people = [{'open_id': '', 'shift': '白', 'scope': 'A', 'name': '张三'}]
        resolved, _ = self.resolve(people, {})
        self.assertEqual(resolved, [])
        resolved, warnings = self.resolve(people, {'p': hr_row('张三', 'A', 'ou_z')})
        self.assertEqual([p['open_id'] for p in resolved], ['ou_z'])
        self.assertEqual(warnings, [])


class EnqueueShiftRemindersTests(TestCase):
    class FakeService:
        def __init__(self, reader, settings=None, local=None, notif=None):
            self._shift_roster_reader = reader
            self._settings = settings or {}
            self._local = dict(local or {})
            self._notif = dict(notif or {})
            self._last_error = None
            self._writes = []

        def settings(self):
            return self._settings

        def _get(self, kind, key, conn=None):
            bucket = self._notif if kind == 'notification' else self._local
            return bucket.get(key)

        def _put(self, kind, key, value, conn=None, dirty=True):
            bucket = self._notif if kind == 'notification' else self._local
            bucket[key] = value
            self._writes.append((kind, key, value))

        def transaction(self):
            return nullcontext()

    def test_before_earliest_shift_reads_nothing(self):
        service = self.FakeService(reader=Mock())
        with patch('lan_bitable_template_portal.learning_personal._login_directory',
                   return_value=({}, True)):
            result = reminders.enqueue_shift_reminders(
                service, dt.datetime(2026, 10, 10, 10, 0, tzinfo=learning.TZ))
        self.assertIsNone(result)
        service._shift_roster_reader.assert_not_called()

    def test_queues_pending_reminder_for_resolved_person_after_shift_time(self):
        directory = {'p': hr_row('张三', 'A', 'ou_z')}
        people = [{'open_id': '', 'shift': '白', 'scope': 'A', 'name': '张三'}]
        service = self.FakeService(reader=lambda day: people, settings={})
        now = dt.datetime(2026, 10, 10, 14, 0, tzinfo=learning.TZ)
        with patch('lan_bitable_template_portal.learning_personal._login_directory',
                   return_value=(directory, True)), \
             patch.object(reminders.time, 'time', return_value=1000):
            result = reminders.enqueue_shift_reminders(service, now)
        self.assertEqual(result, {('白', 'ou_z')})
        key = 'shift:2026-10-10:白:ou_z'
        self.assertEqual(service._get('notification', key)['status'], 'pending')
        self.assertEqual(service._get('notification', key)['open_id'], 'ou_z')
        self.assertEqual(service._get('local', 'shift_roster')['date'], DAY)

    def test_night_waits_until_night_time(self):
        directory = {'a': hr_row('甲', 'A', 'ou_a')}
        people = [{'open_id': '', 'shift': '夜', 'scope': 'A', 'name': '甲'}]
        service = self.FakeService(reader=lambda day: people, settings={})
        at_day = dt.datetime(2026, 10, 10, 14, 0, tzinfo=learning.TZ)
        with patch('lan_bitable_template_portal.learning_personal._login_directory',
                   return_value=(directory, True)), \
             patch.object(reminders.time, 'time', return_value=1000):
            result = reminders.enqueue_shift_reminders(service, at_day)
        self.assertEqual(result, {('夜', 'ou_a')})
        self.assertNotIn('shift:2026-10-10:夜:ou_a', service._notif)
        with patch('lan_bitable_template_portal.learning_personal._login_directory',
                   return_value=(directory, True)), \
             patch.object(reminders.time, 'time', return_value=1000):
            result = reminders.enqueue_shift_reminders(service, at_day.replace(hour=20, minute=5))
        self.assertEqual(service._get('notification', 'shift:2026-10-10:夜:ou_a')['status'], 'pending')

    def test_unresolved_person_sets_pending_warning_and_queues_nothing(self):
        people = [{'open_id': '', 'shift': '白', 'scope': 'A', 'name': '无人'}]
        service = self.FakeService(reader=lambda day: people, settings={})
        now = dt.datetime(2026, 10, 10, 14, 0, tzinfo=learning.TZ)
        with patch('lan_bitable_template_portal.learning_personal._login_directory',
                   return_value=({}, True)), \
             patch.object(reminders.time, 'time', return_value=1000):
            result = reminders.enqueue_shift_reminders(service, now)
        self.assertEqual(result, set())
        self.assertIn('学练排班待核对', service._last_error)
        self.assertEqual(service._notif, {})

    def test_reader_failure_sets_retry_and_does_not_queue(self):
        service = self.FakeService(reader=Mock(side_effect=ValueError('incomplete')), settings={})
        now = dt.datetime(2026, 10, 10, 14, 0, tzinfo=learning.TZ)
        with patch('lan_bitable_template_portal.learning_personal._login_directory',
                   return_value=({}, True)), \
             patch.object(reminders.time, 'time', return_value=1000):
            result = reminders.enqueue_shift_reminders(service, now)
        self.assertIsNone(result)
        retry = service._get('local', 'shift_roster_retry')
        self.assertEqual(retry['date'], DAY)
        self.assertIn('读取失败', service._last_error)

    def test_reader_failure_respects_active_retry_deadline(self):
        service = self.FakeService(reader=Mock(), settings={},
                                   local={'shift_roster_retry': {'date': DAY, 'retry_at': 2000, 'policy_version': reminders.ROSTER_POLICY_VERSION}})
        now = dt.datetime(2026, 10, 10, 14, 0, tzinfo=learning.TZ)
        with patch('lan_bitable_template_portal.learning_personal._login_directory',
                   return_value=({}, True)), \
             patch.object(reminders.time, 'time', return_value=1000):
            result = reminders.enqueue_shift_reminders(service, now)
        self.assertIsNone(result)
        service._shift_roster_reader.assert_not_called()

    def test_recovery_retries_after_retry_window_expires(self):
        people = [{'open_id': '', 'shift': '白', 'scope': 'A', 'name': '张三'}]
        service = self.FakeService(reader=lambda day: people, settings={},
                                   local={'shift_roster_retry': {'date': DAY, 'retry_at': 1000}})
        now = dt.datetime(2026, 10, 10, 14, 0, tzinfo=learning.TZ)
        with patch('lan_bitable_template_portal.learning_personal._login_directory',
                   return_value=({}, True)), \
             patch.object(reminders.time, 'time', return_value=2000):
            result = reminders.enqueue_shift_reminders(service, now)
        # Reader is re-invoked once the retry deadline has passed.
        self.assertEqual(service._local['shift_roster']['date'], DAY)
        self.assertEqual(result, set())

    def test_policy_upgrade_keeps_history_and_backoff_after_failed_current_refresh(self):
        today = dt.datetime.now(reminders.TZ).date()
        day, yesterday = today.isoformat(), (today-dt.timedelta(days=1)).isoformat()
        old = {'date':day, 'version':reminders.ROSTER_VERSION, 'people':[], 'observed_at':999}
        previous = {**old, 'date':yesterday}
        reader = Mock(side_effect=ValueError('offline'))
        service = self.FakeService(reader, local={'shift_roster':old, 'shift_roster:'+yesterday:previous})
        with patch.object(reminders.time,'time',return_value=1000):
            self.assertIs(reminders.roster_snapshot(service,yesterday),previous)
            reader.assert_not_called()
            self.assertIsNone(reminders.roster_snapshot(service,day))
            self.assertIsNone(reminders.roster_snapshot(service,day))
            self.assertEqual(reader.call_count,1,'failed upgrade retains the five-minute retry interval')
        reader.side_effect=None; reader.return_value=[{'name':'实习','position':'运维实习生','duty_kind':'长白','shift':'白','scope':'A'}]
        with patch.object(reminders.time,'time',return_value=1301):
            fresh=reminders.roster_snapshot(service,day)
            self.assertEqual(fresh['policy_version'],reminders.ROSTER_POLICY_VERSION)
            self.assertEqual(len(fresh['people']),1)
            self.assertEqual(reader.call_count,2)


if __name__ == '__main__':
    import unittest
    unittest.main()
