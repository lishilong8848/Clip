# -*- coding: utf-8 -*-
"""Isolated tests for read-only account job categories (岗位分类).

Everything runs against the same isolated temp SQLite/fake-Feishu harness used by
``test_personnel_password_login``. No real cloud requests or DB writes happen:
classification only adds read-only ``position``/``category`` fields to the
account listing and never mutates permissions or the personnel directory.
"""
from __future__ import annotations

import copy
from types import SimpleNamespace
from unittest.mock import patch

from .test_personnel_password_login import (
    _TempDirTestCase,
    PersonnelLoginHarness,
    _page,
    _user,
    IDENTITY_NUMBER,
    ppl,
)
from lan_bitable_template_portal.account_management import (
    AccountManagement,
    CATEGORY_OPTIONS,
    categorize_position,
)
from lan_bitable_template_portal.portal_auth import BUILDING_OPEN_ID_MAP
from lan_bitable_template_portal.portal_service import PortalError


class AccountCategoryTests(_TempDirTestCase):
    _tmp_prefix = "account_category_"

    def setUp(self):
        super().setUp()
        self.enterContext(patch(
            'lan_bitable_template_portal.portal_auth.config',
            SimpleNamespace(app_id='cli_fixture_categories', app_secret='fixture-only-not-a-real-secret'),
        ))
        self.h = PersonnelLoginHarness(self.root)
        self.addCleanup(self.h.close)
        self.h.auth._account_directory_provider = lambda: self.h.login.snapshot
        self._seed('recLeader', '值班长', '值班长甲', 'ou_fixtureCatLeader')
        self._seed('recOperator', '运维值班员', '值班员乙', 'ou_fixtureCatOperator')
        self._seed('recEngineer', '暖通工程师', '工程师丙', 'ou_fixtureCatEngineer')
        self._seed('recSupervisor', '值班主管', '主管丁', 'ou_fixtureCatSupervisor')
        self._seed('recManager', '项目经理', '经理戊', 'ou_fixtureCatManager')
        self._seed('recOther', '安保员', '安保己', 'ou_fixtureCatOther')
        self._seed('recBlank', '', '无岗位庚', 'ou_fixtureCatBlank')
        self.refresh()
        self.manager = AccountManagement(self.h.login)

    def _seed(self, rid, position, name, open_id):
        self.h.service._records[rid] = {
            '姓名': name,
            '员工工号': 'E' + rid[-3:],
            '机楼/专业': 'A楼',
            '员工姓名': _user(name, open_id),
            '账号性质': '外部账号',
            '体系岗位': position,
            '身份证号': IDENTITY_NUMBER,
        }

    def refresh(self, extra=()):
        self.h.service._pages = [_page([{'record_id': rid, 'fields': fields}
                                        for rid, fields in self.h.service._records.items()] + list(extra))]
        self.h.login.directory(force=True)

    def listing(self):
        return self.manager.accounts()

    def item(self, rid):
        return next(x for x in self.listing()['items'] if x['id'] == rid)

    def test_all_categories_are_classified_from_position(self):
        listing = self.listing()
        by_id = {x['id']: x for x in listing['items']}
        expected = {
            'recLeader': ('shift_leader', '值班长'),
            'recOperator': ('operator', '运维值班员'),
            'recEngineer': ('engineer', '暖通工程师'),
            'recSupervisor': ('supervisor', '值班主管'),
            'recManager': ('manager', '项目经理'),
            'recOther': ('other', '安保员'),
            'recBlank': ('other', ''),
        }
        for rid, (category, position) in expected.items():
            item = by_id[rid]
            self.assertEqual(item['category'], category, rid)
            self.assertEqual(item['position'], position, rid)
        labels = {o['value']: o['label'] for o in listing['category_options']}
        self.assertEqual(labels['shift_leader'], '值班长')
        self.assertEqual(labels['operator'], '值班员')
        self.assertEqual(labels['engineer'], '工程师')
        self.assertEqual(labels['supervisor'], '主管')
        self.assertEqual(labels['manager'], '经理')
        self.assertEqual(labels['other'], '其他/待分类')
        self.assertEqual(labels['duty'], '固定值班账号')

    def test_unknown_and_blank_positions_are_other(self):
        self.assertEqual(categorize_position(''), 'other')
        self.assertEqual(categorize_position('   '), 'other')
        self.assertEqual(categorize_position(None), 'other')
        self.assertEqual(categorize_position('安保/保洁'), 'other')
        self.assertEqual(categorize_position('临时工'), 'other')

    def test_substring_match_priority_is_conservative_and_ordered(self):
        # Priority follows the declared, ordered marker list: shift_leader and
        # operator are checked before engineer/supervisor so an ambiguous
        # "值班员/工程师" stays operator, and 经理 never overrides 工程师.
        self.assertEqual(categorize_position('值班长'), 'shift_leader')
        self.assertEqual(categorize_position('值班长兼运行'), 'shift_leader')
        self.assertEqual(categorize_position('值班员'), 'operator')
        self.assertEqual(categorize_position('运行值班员'), 'operator')
        self.assertEqual(categorize_position('值班员/工程师'), 'operator')
        self.assertEqual(categorize_position('暖通工程师'), 'engineer')
        self.assertEqual(categorize_position('值班主管'), 'supervisor')
        self.assertEqual(categorize_position('项目经理'), 'manager')

    def test_duty_accounts_always_duty_regardless_of_name(self):
        listing = self.listing()
        duty_items = [x for x in listing['items'] if x['kind'] == 'duty']
        self.assertTrue(duty_items)
        for item in duty_items:
            self.assertEqual(item['category'], 'duty', item['name'])
            self.assertEqual(item['position'], '')
        # Even if a duty permission-record name mentions an engineer/supervisor
        # title, the fixed duty account must stay duty (never inferred from name).
        duty_oid, duty_scope = next(
            (oid, code) for code, oid in BUILDING_OPEN_ID_MAP.items()
        )
        self.h.auth.upsert_permission_user(
            open_id=duty_oid, name='高级工程师值班账号', role='building',
            scopes=[duty_scope], enabled=True, updated_by='test',
        )
        duty_item = next(x for x in self.manager.accounts()['items'] if x['open_id'] == duty_oid)
        self.assertEqual(duty_item['category'], 'duty')
        self.assertNotEqual(duty_item['category'], 'engineer')

    def test_admin_engineer_stays_engineer_not_inferred_from_role(self):
        item = self.item('recEngineer')
        # Seed a legacy "admin" permission directly (the modernized API rejects
        # granting admin to a non-manager); the roster-derived role/category and
        # home scope must win over the legacy escalatory grant.
        payload = self.h.auth._load_permissions_locked()
        payload.setdefault('users', {})[item['open_id']] = {
            'name': '工程师管理员', 'role': 'admin', 'scopes': self.h.auth._admin_scopes(),
            'enabled': True, 'locked': False,
        }
        self.h.auth._save_permissions_locked(payload)
        refreshed = self.item('recEngineer')
        # A home-A engineer is an ordinary identity (L4), never an admin: the
        # legacy role/scopes are not honored.
        self.assertEqual(refreshed['role'], 'building')
        self.assertEqual(refreshed['position'], '暖通工程师')
        self.assertEqual(refreshed['category'], 'engineer')
        self.assertEqual(refreshed['scopes'], ['A'])
        # And the modernized API rejects granting an admin role to this
        # non-manager engineer outright (no cross-grant/escalation).
        with self.assertRaises(PortalError):
            self.h.auth.upsert_permission_user(
                open_id=item['open_id'], name='工程师管理员', role='admin',
                scopes=self.h.auth._admin_scopes(), enabled=True, updated_by='test',
            )

    def test_listing_does_not_mutate_permissions_or_directory(self):
        before_perms = copy.deepcopy(self.h.auth._load_permissions_locked())
        dir_key = ppl.DIRECTORY_KEY
        dir_before = copy.deepcopy(self.h.auth._state_store.get_document(ppl.DIRECTORY_NS, dir_key))
        for _ in range(2):
            self.manager.accounts()
        self.assertEqual(self.h.auth._load_permissions_locked(), before_perms)
        self.assertEqual(
            self.h.auth._state_store.get_document(ppl.DIRECTORY_NS, dir_key),
            dir_before,
        )
        self.assertEqual(self.h.service._patch_calls, 0)

    def test_category_options_returned_for_ui(self):
        options = self.listing()['category_options']
        self.assertIsInstance(options, list)
        values = {option['value'] for option in options}
        self.assertIn('duty', values)
        self.assertIn('other', values)
        for option in CATEGORY_OPTIONS:
            self.assertIn(option['value'], values)

    def test_departed_inactive_person_hidden_from_accounts(self):
        self._seed('recDeparted', '值班员', '离职人员', 'ou_fixtureDeparted')
        self.h.service._records['recDeparted']['离职/异动情况'] = '已离职'
        self.refresh()
        listing = self.listing()
        ids = {x['id'] for x in listing['items']}
        oids = {x.get('open_id') for x in listing['items']}
        self.assertNotIn('recDeparted', ids)
        self.assertNotIn('ou_fixtureDeparted', oids)

    def test_departed_oid_not_resurfacing_as_legacy_permission_row(self):
        # A permission/grant record still exists in the store for a departed
        # person's open_id. Seed it directly as a legacy value (the modernized
        # API rejects a grant for an identity that is not yet in the directory).
        payload = self.h.auth._load_permissions_locked()
        payload.setdefault('users', {})['ou_fixtureDeparted'] = {
            'name': '离职遗留', 'role': 'building', 'scopes': ['A'],
            'enabled': True, 'locked': False,
        }
        self.h.auth._save_permissions_locked(payload)
        self._seed('recDeparted', '值班员', '离职人员', 'ou_fixtureDeparted')
        self.h.service._records['recDeparted']['离职/异动情况'] = '已离职'
        self.refresh()
        listing = self.listing()
        self.assertTrue(self.h.auth._load_permissions_locked()['users'].get('ou_fixtureDeparted'))
        self.assertEqual(
            [x for x in listing['items'] if x.get('open_id') == 'ou_fixtureDeparted'],
            [],
        )

    def test_active_duplicate_shared_oid_retains_active_person_only(self):
        # Same open_id on a departed record and a currently-active record: the
        # active personnel must still be displayed and no departed legacy row
        # may be created for the shared id.
        self._seed('recDeparted', '值班员', '离职人员', 'ou_fixtureShared')
        self.h.service._records['recDeparted']['离职/异动情况'] = '已离职'
        self._seed('recActive', '工程师', '在职人员', 'ou_fixtureShared')
        self.refresh()
        listing = self.listing()
        by_id = {x['id']: x for x in listing['items']}
        self.assertNotIn('recDeparted', by_id)
        self.assertIn('recActive', by_id)
        self.assertEqual(by_id['recActive']['open_id'], 'ou_fixtureShared')
        self.assertFalse(
            any(x.get('open_id') == 'ou_fixtureShared' and x['kind'] == 'legacy'
                for x in listing['items'])
        )

    def test_disabled_active_person_remains_displayed(self):
        # A merely-disabled (but not departed) active person stays listed.
        self.h.auth.upsert_permission_user(
            open_id='ou_fixtureCatOperator', name='值班员乙', role='building',
            scopes=['A'], enabled=False, updated_by='test',
        )
        self.refresh()
        item = self.item('recOperator')
        self.assertIs(item['enabled'], False)
        self.assertIn('ou_fixtureCatOperator', {x.get('open_id') for x in self.listing()['items']})

    def test_fixed_duty_account_retained_even_if_person_row_inactive(self):
        duty_oid, duty_scope = next(
            (oid, code) for code, oid in BUILDING_OPEN_ID_MAP.items()
        )
        self.h.service._records['recDutyInactive'] = {
            '姓名': '值班账号', '员工工号': 'EDUTY', '机楼/专业': duty_scope,
            '员工姓名': _user('值班账号', duty_oid), '账号性质': '外部账号',
            '岗位': '', '身份证号': IDENTITY_NUMBER, '离职/异动情况': '已离职',
        }
        self.refresh()
        duty_items = [x for x in self.listing()['items'] if x['kind'] == 'duty']
        self.assertTrue(any(x['open_id'] == duty_oid for x in duty_items))

    def test_permissions_byte_equal_before_and_after_listing(self):
        # Seed a legacy departed grant directly; the read-only listing must not
        # mutate the persisted permissions.
        payload = self.h.auth._load_permissions_locked()
        payload.setdefault('users', {})['ou_fixtureDeparted'] = {
            'name': '离职遗留', 'role': 'building', 'scopes': ['A'],
            'enabled': True, 'locked': False,
        }
        self.h.auth._save_permissions_locked(payload)
        self._seed('recDeparted', '值班员', '离职人员', 'ou_fixtureDeparted')
        self.h.service._records['recDeparted']['离职/异动情况'] = '已离职'
        self.refresh()
        before = copy.deepcopy(self.h.auth._load_permissions_locked())
        for _ in range(2):
            self.listing()
        self.assertEqual(self.h.auth._load_permissions_locked(), before)