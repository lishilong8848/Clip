"""No network: shared-directory recipient matching and freshness boundaries."""
import copy
import sys
import unittest
from pathlib import Path
from types import MethodType, SimpleNamespace
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lan_bitable_template_portal import plan_convergence_recipients as recipients
from lan_bitable_template_portal import notice_recipients
from lan_bitable_template_portal.portal_auth import PortalAuthManager


class _DirectoryFixture(unittest.TestCase):
    def setUp(self):
        self.snapshot = {'schema': 4, 'app_id': 'cli_fixture', 'loaded_at': 1800000000,
                         'people': {}}
        self.clock = patch.object(notice_recipients.time, 'time', return_value=1800000001).start()
        self.addCleanup(patch.stopall)
        self.disabled = set()
        self.auth = SimpleNamespace(
            current_feishu_app_id=Mock(return_value='cli_fixture'),
            _account_directory_provider=lambda: self.snapshot,
            _account_directory_loader=Mock(),
            _account_read_state=lambda: None,
            account_profile=Mock(return_value={'scope_rules': ['ALL'], 'modules': ['plan_convergence']}),
            _open_id_explicitly_disabled=lambda identity: identity in self.disabled)
        self.auth.directory_person = MethodType(PortalAuthManager.directory_person, self.auth)
        self.auth.current_feishu_open_id = MethodType(PortalAuthManager.current_feishu_open_id, self.auth)

    def person(self, key, **values):
        self.snapshot['people'][key] = {'id': key, 'name': key, 'scopes': ['B'],
            'specialties': ['暖通'], 'position': '工程师', 'inactive': False,
            'selectable': True, 'login_ids': ['ou_' + key], **values}


class EngineerRecipientsTests(_DirectoryFixture):
    def resolve(self, scopes=None, major='暖通'):
        scopes = scopes or ['B']
        return recipients.current_engineers(self.auth, {'major': major,
            'building': '、'.join(scope + '楼' for scope in scopes)}, scopes)

    def test_matches_role_building_and_specialty_without_admin_scope_expansion(self):
        self.person('correct')
        self.person('multi', scopes=['B', 'C'], specialties=['电气', '暖通'])
        self.person('otherbuilding', scopes=['C'])
        self.person('othermajor', specialties=['电气'])
        self.person('supervisor', position='主管')
        self.person('all', scopes=['ALL'])
        self.assertEqual(self.resolve(), (['ou_correct', 'ou_multi'], ''))
        self.auth._account_directory_loader.assert_not_called()

    def test_multiple_explicit_specialties_are_supported(self):
        self.person('hvac')
        self.person('electric', specialties=['电气'])
        self.person('weak', specialties=['弱电'])
        self.assertEqual(self.resolve(major='暖通、 电气')[0], ['ou_hvac', 'ou_electric'])

    def test_missing_or_unknown_specialty_never_expands_delivery(self):
        self.person('one')
        for major in ('', '楼宇自控', '暖通及其它专业', '暖通、未知'):
            with self.subTest(major=major):
                people, warning = self.resolve(major=major)
                self.assertEqual(people, [])
                self.assertTrue(warning)
        self.auth._account_directory_loader.assert_not_called()

    def test_disabled_departed_and_ambiguous_identities_are_not_sent(self):
        self.person('inactive', inactive=True)
        self.person('unselectable', selectable=False)
        self.person('disabled')
        self.disabled.add('ou_disabled')
        self.person('missing', login_ids=[])
        self.person('ambiguous', login_ids=['ou_one', 'ou_two'])
        self.person('duplicate1', login_ids=['ou_shared'])
        self.person('duplicate2', login_ids=['ou_shared'])
        self.person('bad', login_ids=['person:recTest'])
        people, warning = self.resolve()
        self.assertEqual(people, [])
        self.assertTrue(warning)

    def test_transfer_follows_replaced_shared_snapshot(self):
        self.person('moving')
        self.assertEqual(self.resolve()[0], ['ou_moving'])
        self.snapshot = copy.deepcopy(self.snapshot)
        self.snapshot['people']['moving']['scopes'] = ['C']
        self.assertEqual(self.resolve()[0], [])
        self.assertEqual(self.resolve(['C'])[0], ['ou_moving'])

    def test_excluded_and_unknown_buildings_do_not_query_directory(self):
        self.person('de', scopes=['D', 'E'])
        self.assertEqual(self.resolve(['D', 'E', 'ALL', 'unknown']), ([], ''))
        self.auth._account_directory_loader.assert_not_called()

    def test_mixed_buildings_keep_only_nonexcluded_engineers(self):
        self.person('b')
        self.person('d', scopes=['D'])
        self.assertEqual(self.resolve(['B', 'D'])[0], ['ou_b'])

    def test_stale_snapshot_refreshes_once_and_uses_new_assignment(self):
        self.person('old')
        self.snapshot['loaded_at'] -= 400
        def refresh():
            self.snapshot = {**self.snapshot, 'loaded_at': self.clock.return_value, 'people': {}}
            self.person('new')
        self.auth._account_directory_loader.side_effect = refresh
        self.assertEqual(self.resolve()[0], ['ou_new'])
        self.auth._account_directory_loader.assert_called_once_with()

    def test_invalid_or_failed_directory_is_never_treated_as_empty_success(self):
        cases = ({'loaded_at': 1}, {'loaded_at': 1800001000}, {'loaded_at': float('nan')},
                 {'app_id': 'cli_old'}, {'schema': 3}, {'people': None})
        for change in cases:
            with self.subTest(change=change):
                original = self.snapshot
                self.snapshot = {**original, **change}
                with self.assertRaises(RuntimeError):
                    self.resolve()
                self.snapshot = original
        self.auth._account_directory_refresh_failed = True
        with self.assertRaises(RuntimeError):
            self.resolve()

    def test_refresh_failure_propagates_without_using_stale_engineers(self):
        self.person('old')
        self.snapshot['loaded_at'] = 1
        self.auth._account_directory_loader.side_effect = TimeoutError()
        with self.assertRaises(TimeoutError):
            self.resolve()

    def test_directory_or_app_change_during_matching_defers_delivery(self):
        self.person('one')
        def switch_directory(*_):
            self.snapshot = copy.deepcopy(self.snapshot)
            return 'ou_one'
        self.auth.current_feishu_open_id = switch_directory
        with self.assertRaises(RuntimeError):
            self.resolve()
        self.auth.current_feishu_open_id = Mock(return_value='ou_one')
        self.auth.current_feishu_app_id.side_effect = ['cli_fixture', 'cli_other']
        with self.assertRaises(RuntimeError):
            self.resolve()

    def test_module_and_building_revocation_prevent_delivery(self):
        self.person('one')
        for profile in ({'modules': [], 'scope_rules': ['B']},
                        {'modules': ['plan_convergence'], 'scope_rules': ['C']},
                        {'modules': ['plan_convergence'], 'scope_rules': []}):
            with self.subTest(profile=profile):
                self.auth.account_profile.return_value = profile
                self.assertEqual(self.resolve()[0], [])
        self.auth.account_profile.return_value = {'modules': ['plan_convergence'], 'scope_rules': ['B']}
        self.assertEqual(self.resolve()[0], ['ou_one'])

    def test_multibuilding_notice_requires_existing_access_to_entire_notice(self):
        self.person('one')
        self.auth.account_profile.return_value = {'modules': ['plan_convergence'], 'scope_rules': ['B']}
        self.assertEqual(self.resolve(['B', 'C'])[0], [])
        self.auth.account_profile.return_value = {'modules': ['plan_convergence'], 'scope_rules': ['B', 'C']}
        self.assertEqual(self.resolve(['B', 'C'])[0], ['ou_one'])
        self.assertEqual(recipients.current_engineers(self.auth, {'major': '暖通', 'building': ''}, ['B'])[0], [])


class SupervisorRecipientsTests(_DirectoryFixture):
    def person(self, key, **values):
        super().person(key, **{'position': '运维主管', **values})

    def resolve(self, scopes=None, kind='维保通告'):
        return notice_recipients.current_supervisors(self.auth,
            {'scopes': scopes or ['B'], 'notice_type': kind})

    def test_only_current_building_supervisors_with_notice_access(self):
        self.auth.account_profile.return_value = {'scope_rules': ['B'], 'modules': ['maintenance']}
        self.person('right')
        self.person('other', scopes=['C'])
        self.person('engineer', position='暖通工程师')
        self.person('manager', position='经理')
        self.person('inactive', inactive=True)
        self.person('disabled')
        self.disabled.add('ou_disabled')
        self.assertEqual(self.resolve()[0], ['ou_right'])

    def test_all_buildings_and_notice_modules_including_de_and_power(self):
        self.person('supervisor', scopes=['110', 'A', 'B', 'C', 'D', 'E', 'H'])
        for kind, module in notice_recipients.WORK_TYPE_BY_NOTICE_TYPE.items():
            self.auth.account_profile.return_value = {'scope_rules': ['ALL'], 'modules': [module]}
            for scope in ('110', 'A', 'B', 'C', 'D', 'E', 'H'):
                with self.subTest(kind=kind, scope=scope):
                    self.assertEqual(self.resolve([scope], kind)[0], ['ou_supervisor'])

    def test_scope_or_module_revocation_does_not_send(self):
        self.person('one')
        for profile in ({'scope_rules': ['B'], 'modules': []},
                        {'scope_rules': ['C'], 'modules': ['maintenance']},
                        {'scope_rules': [], 'modules': ['maintenance']}):
            self.auth.account_profile.return_value = profile
            self.assertEqual(self.resolve()[0], [])

    def test_multibuilding_notice_follows_existing_tag_visibility(self):
        self.person('b')
        self.auth.account_profile.return_value = {'scope_rules': ['B'], 'modules': ['maintenance']}
        self.assertEqual(self.resolve(['B', 'C'])[0], ['ou_b'])
        self.auth.account_profile.return_value = {'scope_rules': ['C'], 'modules': ['maintenance']}
        self.assertEqual(self.resolve(['B', 'C'])[0], [])

    def test_transfer_and_current_app_identity_are_rechecked(self):
        self.person('one')
        self.auth.account_profile.return_value = {'scope_rules': ['ALL'], 'modules': ['maintenance']}
        self.assertEqual(self.resolve()[0], ['ou_one'])
        self.snapshot = copy.deepcopy(self.snapshot)
        self.snapshot['people']['one']['scopes'] = ['C']
        self.assertEqual(self.resolve()[0], [])
        self.assertEqual(self.resolve(['C'])[0], ['ou_one'])
        self.snapshot['app_id'] = 'cli_old'
        with self.assertRaises(RuntimeError):
            self.resolve(['C'])

    def test_ambiguous_identity_and_unknown_notice_never_broaden_recipients(self):
        self.person('missing', login_ids=[])
        self.person('duplicate1', login_ids=['ou_shared'])
        self.person('duplicate2', login_ids=['ou_shared'])
        self.assertEqual(self.resolve()[0], [])
        self.assertTrue(self.resolve()[1])
        self.assertEqual(self.resolve(kind='未知通告')[0], [])
        self.auth._account_directory_loader.assert_not_called()


if __name__ == '__main__':
    unittest.main()
