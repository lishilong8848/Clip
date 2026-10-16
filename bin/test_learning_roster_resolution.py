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


def roster_row(name=None, users=None, group='A班', shift='白', day=DAY_TS, scope='A楼', **extra):
    """One synthetic list-feed item. Text is authoritative; users are the linked array."""
    fields = {'班组': group, '班次': shift, '排班日期': day, '机楼': scope}
    if name is not None:
        fields['人员（文本）'] = name
    if users is not None:
        fields['人员'] = users
    fields.update(extra)
    return {'fields': fields}


def users(*pairs):
    """Build the '人员' users array from (name, open_id) pairs."""
    return {'users': [{'name': n, 'id': o} for n, o in pairs]}


def hr_row(name, scope, oid, *, selectable=True, inactive=False, position='值班员',
           scopes=None, login_ids=None):
    """One local HR directory row shaped for _hr_active/_hr_learning_scopes."""
    return {'name': name, 'scopes': scopes if scopes is not None else [scope],
            'selectable': selectable, 'inactive': inactive,
            'login_ids': login_ids if login_ids is not None else ([oid] if oid else []),
            'position': position}


def build_reader(items):
    """Return a service Mock whose _request_json feeds the given items across pages."""
    service = Mock()
    cur = items[:]
    bodies = []
    while True:
        page = cur[:2]
        cur = cur[2:]
        has_more = bool(cur)
        body = {'code': 0, 'data': {'items': page, 'has_more': has_more}}
        if has_more:
            body['data']['page_token'] = 'tok-%d' % len(bodies)
        bodies.append(body)
        if not has_more:
            break
    if not bodies:
        bodies = [{'code': 0, 'data': {'items': [], 'has_more': False}}]
    service._request_json.side_effect = bodies
    return service


class ReadRosterTests(TestCase):
    def assert_request_contract(self, service):
        params = service._request_json.call_args.kwargs['params']
        self.assertEqual(params['view_id'], 'vew6jLlGzq')
        self.assertEqual(params['page_size'], 500)
        self.assertEqual(params['user_id_type'], 'open_id')

    def test_uses_fixed_roster_view_and_request_contract(self):
        service = build_reader([roster_row('张三', users(('张三', 'ou_a')))])
        result = reminders.read_roster(service, DAY)
        self.assertEqual(result, [{'open_id': 'ou_a', 'shift': '白', 'scope': 'A', 'name': '张三'}])
        self.assert_request_contract(service)

    def test_text_names_authoritative_route_by_exact_linked_name(self):
        items = [
            roster_row('张三,李四', users(('张三', 'ou_a'), ('李四', 'ou_b'))),
            roster_row('王五', users(('王五', 'ou_c')), shift='夜', scope='B楼'),
        ]
        result = reminders.read_roster(build_reader(items), DAY)
        self.assertEqual(result, [
            {'open_id': 'ou_a', 'shift': '白', 'scope': 'A', 'name': '张三'},
            {'open_id': 'ou_b', 'shift': '白', 'scope': 'A', 'name': '李四'},
            {'open_id': 'ou_c', 'shift': '夜', 'scope': 'B', 'name': '王五'},
        ])

    def test_longday_110_and_wrong_shift_excluded(self):
        items = [
            roster_row('张三', users(('张三', 'ou_a')), group='长白'),
            roster_row('李四', users(('李四', 'ou_b')), group='110站'),
            roster_row('王五', users(('王五', 'ou_c')), shift='休'),
            roster_row('赵六', users(('赵六', 'ou_d')), scope='110站'),
            roster_row('汪七', users(('汪七', 'ou_e')), scope='110楼'),
            roster_row('真正值班', users(('真正值班', 'ou_ok'))),
        ]
        result = reminders.read_roster(build_reader(items), DAY)
        self.assertEqual([p['name'] for p in result], ['真正值班'])
        self.assertEqual(result[0]['scope'], 'A')

    def test_wrong_date_excluded_and_invalid_date_raises(self):
        service = build_reader([
            roster_row('张三', users(('张三', 'ou_a')), day=DAY_TS - 86400000),
            roster_row('李四', users(('李四', 'ou_b')), day='not-a-number'),
        ])
        with self.assertRaises(ValueError):
            reminders.read_roster(service, DAY)

    def test_rich_text_list_names_split_on_whitespace_and_punct(self):
        # 人员（文本） is a rich-text list: whitespace is the list delimiter, and each
        # full name is kept intact; only names with an exactly-linked user get an open_id.
        rich = [{'text': '张三 '}, {'text': '李四\n王五、赵六;甲乙，丙丁'}]
        result = reminders.read_roster(
            build_reader([roster_row(rich, users(('张三', 'ou_a'), ('李四', 'ou_b'), ('王五', 'ou_c')))]),
            DAY)
        self.assertEqual([p['name'] for p in result],
                         ['张三', '李四', '王五', '赵六', '甲乙', '丙丁'])
        self.assertEqual([p['open_id'] for p in result],
                         ['ou_a', 'ou_b', 'ou_c', '', '', ''])

    def test_rich_text_name_with_middle_dot_stays_whole_and_matches(self):
        service = build_reader([roster_row('李·白', users(('李·白', 'ou_lb')))])
        result = reminders.read_roster(service, DAY)
        self.assertEqual(result, [{'open_id': 'ou_lb', 'shift': '白', 'scope': 'A', 'name': '李·白'}])

    def test_partial_text_list_only_resolves_present_names(self):
        items = [roster_row('张三', users(('张三', 'ou_a'), ('李四', 'ou_b')))]
        result = reminders.read_roster(build_reader(items), DAY)
        self.assertEqual([p['name'] for p in result], ['张三'])
        self.assertEqual(result[0]['open_id'], 'ou_a')

    def test_empty_text_raises_even_when_linked_users_present(self):
        service = build_reader([roster_row(users=users(('张三', 'ou_a')))])
        with self.assertRaises(ValueError):
            reminders.read_roster(service, DAY)

    def test_mismatched_same_count_linked_users_do_not_match_text(self):
        items = [roster_row('张三,李四', users(('zhang', 'ou_x'), ('li', 'ou_y')))]
        result = reminders.read_roster(build_reader(items), DAY)
        # Authoritative text names exist but no linked user matches exactly -> open_id blank.
        self.assertEqual([p['open_id'] for p in result], ['', ''])
        self.assertEqual([p['name'] for p in result], ['张三', '李四'])

    def test_malformed_users_field_raises(self):
        # Force an invalid (non list) 人员 payload with valid authoritative text.
        bad = roster_row('张三', users=users(('张三', 'ou_a')))
        bad['fields']['人员'] = 'oops'
        service2 = Mock()
        service2._request_json.side_effect = [
            {'code': 0, 'data': {'items': [bad], 'has_more': False}}]
        with self.assertRaises(ValueError):
            reminders.read_roster(service2, DAY)

    def test_duplicate_identitical_rows_collapse(self):
        items = [roster_row('张三', users(('张三', 'ou_a'))),
                 roster_row('张三', users(('张三', 'ou_a')))]
        result = reminders.read_roster(build_reader(items), DAY)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['open_id'], 'ou_a')

    def test_duplicate_name_conflicting_linked_ids_blanked(self):
        items = [roster_row('张三', users(('张三', 'ou_a'))),
                 roster_row('张三', users(('张三', 'ou_b')))]
        result = reminders.read_roster(build_reader(items), DAY)
        self.assertEqual([p['open_id'] for p in result], [''])

    def test_pagination_accumulates_and_dedups_across_pages(self):
        service = build_reader([
            roster_row('张三', users(('张三', 'ou_a'))),
            roster_row('李四', users(('李四', 'ou_b')), shift='夜'),
            roster_row('张三', users(('张三', 'ou_a'))),
        ])
        result = reminders.read_roster(service, DAY)
        self.assertEqual(sorted(p['name'] for p in result), ['张三', '李四'])
        self.assertEqual(service._request_json.call_count, 2)

    def test_pagination_loop_detected_and_aborts(self):
        service = Mock()
        service._request_json.side_effect = [
            {'code': 0, 'data': {'items': [roster_row('张三', users(('张三', 'ou_a')))],
                                 'has_more': True, 'page_token': 'tok-x'}},
            {'code': 0, 'data': {'items': [], 'has_more': True, 'page_token': 'tok-x'}},
        ]
        with self.assertRaises(ValueError):
            reminders.read_roster(service, DAY)

    def test_nonzero_code_or_missing_bools_abort(self):
        service = Mock()
        service._request_json.return_value = {'code': 7,
                                              'data': {'items': [roster_row()], 'has_more': False}}
        with self.assertRaises(ValueError):
            reminders.read_roster(service, DAY)
        service2 = Mock()
        service2._request_json.return_value = {'code': 0, 'data': {'items': [], 'has_more': 'yes'}}
        with self.assertRaises(ValueError):
            reminders.read_roster(service2, DAY)


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
        rows = [
            roster_row('张三 李四', users(('张三', 'ou_l'))),      # partial link
            roster_row('王五 赵六', users()),                     # no link
            roster_row('甲乙 丙丁', users(('x1', 'ou_x'), ('x2', 'ou_y'))),  # wrong names
        ]
        people = reminders.read_roster(build_reader(rows), DAY)
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
                                   local={'shift_roster_retry': {'date': DAY, 'retry_at': 2000}})
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


if __name__ == '__main__':
    import unittest
    unittest.main()