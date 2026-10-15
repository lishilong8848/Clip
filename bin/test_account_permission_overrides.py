"""Permission-override upgrade tests for account level/role/module/scope editing."""
import copy
from unittest.mock import patch

from .test_personnel_password_login import _TempDirTestCase, PersonnelLoginHarness, _page, _user
from lan_bitable_template_portal.account_management import AccountManagement
from lan_bitable_template_portal.account_policy import MODULE_IDS, default_modules, resolve_access_change
from lan_bitable_template_portal.account_policy import PolicyViolation
from lan_bitable_template_portal.personnel_password_login import PasswordLoginError
from lan_bitable_template_portal.portal_auth import BUILDING_OPEN_ID_MAP, LI_SHILONG_OPEN_ID
from lan_bitable_template_portal.portal_service import PortalError


class AccountPermissionOverrideTests(_TempDirTestCase):
    def setUp(self):
        super().setUp()
        self.h = PersonnelLoginHarness(self.root)
        self.addCleanup(self.h.close)
        self.h.auth._account_directory_provider = lambda: self.h.login.snapshot
        rows = []
        for rid, home, job in (("HM1", "H", "运维经理"), ("HM2", "H", "运维经理"),
                               ("HE1", "H", "电气专业工程师"), ("HE2", "H", "弱电专业工程师"),
                               ("HS", "H", "运维主管"), ("AS", "A", "运维主管"), ("BS", "B", "运维主管"),
                               ("AE", "A", "暖通工程师"), ("AO", "A", "值班员"), ("BE", "B", "电气工程师")):
            fields = {"姓名": "测试" + rid, "员工姓名": _user("测试" + rid, "ou_fixture" + rid),
                      "机楼/专业": [home + "楼"], "体系岗位": job, "岗位": "其他", "账号性质": "外部账号"}
            self.h.service._records["rec" + rid] = fields
            rows.append({"record_id": "rec" + rid, "fields": fields})
        self.h.service._pages = [_page(rows)]
        self.h.login.directory(force=True)
        self.manager = AccountManagement(self.h.login)

    def actor(self, rid):
        return {"open_id": "ou_fixture" + rid, "name": "测试" + rid}

    def item(self, rid):
        return next(item for item in self.manager.accounts()["items"] if item["id"] == "rec" + rid)

    def override_change(self, rid, **patch):
        """New-format change: only id/revision/enabled plus explicit access fields."""
        item = self.item(rid)
        data = {key: item[key] for key in ("id", "revision", "enabled")}
        return {**data, **patch}

    def test_l0_can_raise_l4_to_l1_gaining_admin_scopes_modules_and_manage(self):
        auth = self.h.auth
        admin = auth._admin_scopes()
        self.assertEqual(auth.account_profile("ou_fixtureAE")["level"], "L4")
        before = copy.deepcopy(self.h.auth._load_permissions_locked())
        self.manager.update({"changes": [
            self.override_change("AE", level="L1")
        ]}, {"open_id":LI_SHILONG_OPEN_ID})
        profile = auth.account_profile("ou_fixtureAE")
        self.assertEqual(profile["level"], "L1")
        self.assertEqual(set(profile["scope_rules"]), set(admin))
        self.assertEqual(set(profile["modules"]), set(MODULE_IDS))
        self.assertEqual(profile["role"], "admin")
        self.assertTrue(profile["can_manage_accounts"])
        self.assertTrue(auth.is_admin({"user": self.actor("AE"), "role": "building"}))
        # The upgrade is persisted and session scopes reflect the new level.
        self.assertEqual(set(auth.session_scopes({"user": self.actor("AE"), "allowed_scopes": []})), set(admin))
        # Historical pre-upgrade snapshot preserved.
        self.assertEqual(self.h.auth._state_store.get_document("account_management", "permissions_before_upgrade"), before)

    def test_l0_can_downgrade_l1_to_l4_restoring_limited_scope_and_modules(self):
        auth = self.h.auth
        self.manager.update({"changes": [self.override_change("AE", level="L1")]}, {"open_id":LI_SHILONG_OPEN_ID})
        self.manager.update({"changes": [self.override_change("AE", level="L4")]}, {"open_id":LI_SHILONG_OPEN_ID})
        profile = auth.account_profile("ou_fixtureAE")
        self.assertEqual(profile["level"], "L4")
        self.assertEqual(set(profile["scope_rules"]), {"A"})
        self.assertNotIn("accounts", profile["modules"])
        self.assertEqual(set(profile["modules"]), set(default_modules("L4")))
        self.assertEqual(profile["role"], "building")
        self.assertFalse(profile["can_manage_accounts"])
        self.assertFalse(auth.is_admin({"user": self.actor("AE"), "role": "admin"}))
        self.assertEqual(auth.session_scopes({"user": self.actor("AE"), "allowed_scopes": ["ALL"]}), ["A"])

    def test_l2_cannot_promote_to_l1_or_l3_and_no_partial_write(self):
        before = copy.deepcopy(self.h.auth._load_permissions_locked())
        # L2 may never hand out a higher (L1/L3) level, nor even override an L4
        # target's access (it only does base edit/enable).
        for target_level in ("L1", "L3"):
            with self.assertRaises(PasswordLoginError):
                self.manager.update(
                    {"changes": [self.override_change("AE", level=target_level)]},
                    self.actor("HE1"))
        self.assertEqual(self.h.auth._load_permissions_locked(), before)
        self.assertEqual(self.h.auth.account_profile("ou_fixtureAE")["level"], "L4")
        self.assertEqual(set(self.h.auth.account_profile("ou_fixtureAE")["scope_rules"]), {"A"})

    def test_l3_cannot_grant_outside_building(self):
        before = copy.deepcopy(self.h.auth._load_permissions_locked())
        with self.assertRaises(PasswordLoginError):
            self.manager.update({"changes": [self.override_change("BE", enabled=False)]}, self.actor("AS"))
        self.assertEqual(self.h.auth._load_permissions_locked(), before)
        self.assertTrue(self.h.auth._open_id_explicitly_disabled("ou_fixtureBE") is False)

    def test_unknown_scope_and_implicit_outside_scope_are_rejected(self):
        auth=self.h.auth
        with self.assertRaises(PolicyViolation):
            resolve_access_change(auth.account_profile(LI_SHILONG_OPEN_ID),auth.account_profile('ou_fixtureAO'),scopes=['A','NOT_A_BUILDING'])
        target={**auth.account_profile('ou_fixtureAO'),'scope_rules':['A','B']}
        with self.assertRaises(PolicyViolation):
            resolve_access_change(auth.account_profile('ou_fixtureAS'),target,modules=['learning'])

    def test_password_rechecks_actor_after_directory_fetch(self):
        original=self.h.login.record
        def fetched(rid):
            row=original(rid)
            payload=self.h.auth._load_permissions_locked()
            payload['users']['ou_fixtureHM1']['enabled']=False
            self.h.auth._save_permissions_locked(payload)
            return row
        with patch.object(self.h.login,'record',side_effect=fetched),patch.object(self.h.login,'save_password') as save:
            with self.assertRaises(PasswordLoginError):
                self.manager.password('recAO',{'new_password':'A-test-pass123!','confirm_password':'A-test-pass123!'},self.actor('HM1'))
            save.assert_not_called()

    def test_delete_rechecks_target_level_inside_write_lock(self):
        item=self.item('AO');original=self.manager.check
        def changed(actor,target,action):
            result=original(actor,target,action)
            payload=self.h.auth._load_permissions_locked()
            payload['users']['ou_fixtureAO'].update(access_version=1,level='L1',category='manager',modules=list(MODULE_IDS))
            self.h.auth._save_permissions_locked(payload)
            return result
        with patch.object(self.manager,'check',side_effect=changed),self.assertRaises(PasswordLoginError):
            self.manager.delete('recAO',{'revision':item['revision']},self.actor('HM1'))
        self.assertTrue(self.h.auth._permission_user('ou_fixtureAO')['enabled'])

    def test_audit_redacts_peer_and_other_building_changes(self):
        self.h.auth._state_store.record_business_operation_audit(domain='accounts',action='permissions',status='success',
            metadata={'changes':[{'open_id':'ou_fixtureAO','name':'A人员'},{'open_id':'ou_fixtureHM2','name':'同级经理'},{'open_id':'ou_fixtureBE','name':'B人员'}]})
        l1=self.manager.audit(self.actor('HM1'))
        self.assertEqual([row['open_id'] for row in l1[0]['metadata']['changes']],['ou_fixtureAO','ou_fixtureBE'])
        l3=self.manager.audit(self.actor('AS'))
        self.assertEqual([row['open_id'] for row in l3[0]['metadata']['changes']],['ou_fixtureAO'])

    def test_legacy_dirty_grants_never_become_new_authority(self):
        auth = self.h.auth
        payload = auth._load_permissions_locked()
        payload["users"]["ou_fixtureAO"] = {
            "name": "测试AO", "role": "admin", "scopes": auth._admin_scopes(),
            "enabled": True, "locked": False,
        }
        auth._save_permissions_locked(payload)
        # A legacy dirty record without access_version must NOT become admin.
        self.assertEqual(auth.account_profile("ou_fixtureAO")["level"], "L4")
        self.assertEqual(set(auth.account_profile("ou_fixtureAO")["scope_rules"]), {"A"})
        self.assertFalse(auth.is_admin({"user": self.actor("AO"), "role": "admin"}))
        # A new-format migration seeds explicit modules only from the HR profile.
        self.manager.update({"changes": [
            self.override_change("AO", level="L4", modules=["maintenance", "daily"])
        ]}, self.actor("HM1"))
        stored = auth._load_permissions_locked()["users"]["ou_fixtureAO"]
        self.assertEqual(stored["access_version"], 1)
        self.assertEqual(set(stored["modules"]), {"maintenance", "daily"})
        self.assertEqual(set(auth.account_profile("ou_fixtureAO")["scope_rules"]), {"A"})
        self.assertFalse(auth.is_admin({"user": self.actor("AO"), "role": "admin"}))

    def test_sync_preserves_manual_override_and_still_blocks_departure(self):
        auth = self.h.auth
        self.manager.update({"changes": [self.override_change("AE", level="L3", modules=["maintenance"])]}, self.actor("HM1"))
        person = self.h.login.snapshot["people"]["recAE"]
        auth.sync_directory_permissions([person], on_login=True)
        profile = auth.account_profile("ou_fixtureAE")
        self.assertEqual(profile["level"], "L3")
        self.assertEqual(profile["modules"], ["maintenance"])
        self.assertEqual(set(profile["scope_rules"]), {"A"})
        # Departure still fails closed regardless of the manual override.
        person.update(inactive=True, selectable=False, disabled_reason="已离职")
        auth._account_index_cache = (None, {})
        self.assertEqual(auth.scopes_for_open_id("ou_fixtureAE"), [])
        self.assertFalse(auth.account_profile("ou_fixtureAE")["can_manage_accounts"])

    def test_runtime_revision_protection_and_fixed_accounts(self):
        item = self.item("AE")
        self.manager.update({"changes": [self.override_change("AE", level="L4")]}, self.actor("HM1"))
        with self.assertRaisesRegex(PasswordLoginError, "已变化"):
            # Re-submit the stale revision.
            stale = {**self.override_change("AE", level="L4"), "revision": item["revision"]}
            self.manager.update({"changes": [stale]}, self.actor("HM1"))
        # H fixed duty maintains L1 and cannot be modified by a manager.
        h_duty = next(item for item in self.manager.accounts()["items"]
                      if item["kind"] == "duty" and item["own_scopes"] == ["H"])
        duty_change = {key: h_duty[key] for key in ("id", "revision", "enabled")}
        with self.assertRaises(PasswordLoginError):
            self.manager.update({"changes": [{**duty_change, "level": "L4"}]}, self.actor("HM1"))
        self.assertEqual(self.h.auth.account_profile(BUILDING_OPEN_ID_MAP["H"])["level"], "L1")
        # Li (L0) is fixed and cannot be downgraded.
        li = next(item for item in self.manager.accounts()["items"] if item["open_id"] == LI_SHILONG_OPEN_ID)
        li_change = {key: li[key] for key in ("id", "revision", "enabled")}
        with self.assertRaisesRegex(PasswordLoginError, "固定管理员"):
            self.manager.update({"changes": [{**li_change, "level": "L4"}]}, self.actor("HM1"))
        self.assertEqual(self.h.auth.account_profile(LI_SHILONG_OPEN_ID)["level"], "L0")

    def test_account_listing_exposes_new_fields(self):
        listing = self.manager.accounts(actor=self.actor("HM1"))
        self.assertIn("module_options", listing)
        self.assertIn("level_options", listing)
        self.assertIn("category_options", listing)
        self.assertTrue(listing["module_options"])
        levels = {opt["value"] for opt in listing["level_options"]}
        self.assertNotIn("L1", levels)
        self.assertIn("L4", levels)
        for item in listing["items"]:
            self.assertIn("modules", item)
            self.assertIn("can_edit_access", item)
        # L2 remains base-edit only; no assignable access levels.
        l2_listing = self.manager.accounts(actor=self.actor("HE1"))
        self.assertEqual({opt["value"] for opt in l2_listing["level_options"]}, set())
        self.assertEqual({opt["value"] for opt in l2_listing["category_options"]}, {"engineer"})
        # L2 can base-edit engineers but can_edit_access is False (no override UI).
        self.assertTrue(all(item["can_edit"] for item in l2_listing["items"] if not item['is_self']))
        self.assertTrue(all(item["can_edit_access"] is False for item in l2_listing["items"]))

    def test_l4_cannot_receive_accounts_module(self):
        with self.assertRaises(PasswordLoginError):
            self.manager.update({"changes": [
                self.override_change("AE", level="L4", modules=["maintenance", "accounts"])
            ]}, self.actor("HM1"))

    def test_same_level_and_higher_accounts_are_not_editable_through_any_entry(self):
        actor=self.actor('HM1');before=copy.deepcopy(self.h.auth._load_permissions_locked())
        for target in ('HM2','HM1'):
            with self.assertRaises(PasswordLoginError):
                self.manager.update({'changes':[self.override_change(target,enabled=False)]},actor)
        with self.assertRaises(PasswordLoginError):
            self.manager.update({'changes':[self.override_change('AE',level='L1')]},actor)
        with self.assertRaises(PortalError):
            self.h.auth.upsert_permission_user(open_id='ou_fixtureHM2',role='admin',scopes=self.h.auth._admin_scopes(),enabled=False,updated_by=actor['open_id'])
        listing=self.manager.accounts(actor=actor)
        self.assertNotIn('ou_fixtureHM2',{item['open_id'] for item in listing['items']})
        mine=next(item for item in listing['items'] if item['is_self'])
        self.assertFalse(mine['can_edit_access']);self.assertFalse(mine['can_edit'])
        self.assertEqual(self.h.auth._load_permissions_locked(),before)

    def test_self_access_request_requires_superior_and_preserves_proposed_scope(self):
        auth=self.h.auth;person='ou_fixtureAE';superior='ou_fixtureAS'
        access={'level':'L4','category':'engineer','scopes':['A'],'modules':['maintenance','learning']}
        self.assertIn(superior,auth.permission_approver_ids(person))
        self.assertNotIn('ou_fixtureBE',auth.permission_approver_ids(person))
        result=auth.create_permission_request(open_id=person,name='测试AE',scopes=['A'],reason='测试申请',approver_open_id=superior,requested_access=access)
        before=copy.deepcopy(auth._load_permissions_locked())
        with self.assertRaises(PortalError):auth.approve_permission_request(result['request_id'],updated_by=person)
        self.assertEqual(auth._load_permissions_locked(),before)
        auth.approve_permission_request(result['request_id'],updated_by=superior)
        self.assertEqual(auth.account_profile(person)['modules'],['maintenance','learning'])
        self.assertEqual(auth.account_profile(person)['scope_rules'],['A'])

    def test_pending_access_request_never_overwrites_newer_permission_edit(self):
        auth=self.h.auth
        result=auth.create_permission_request(open_id='ou_fixtureAE',name='测试AE',reason='隔离申请',approver_open_id='ou_fixtureHM1',
            requested_access={'level':'L3','category':'supervisor','scopes':['A'],'modules':['maintenance']})
        self.manager.update({'changes':[self.override_change('AE',modules=['learning'])]},self.actor('HM1'))
        before=copy.deepcopy(auth._load_permissions_locked())
        with self.assertRaisesRegex(PortalError,'权限已变化'):
            auth.approve_permission_request(result['request_id'],updated_by='ou_fixtureHM1')
        self.assertEqual(auth._load_permissions_locked(),before)

    def test_persistence_survives_cache_reload(self):
        self.manager.update({"changes": [self.override_change("AE", level="L2", modules=["engineer_mop"])]}, self.actor("HM1"))
        # Force the 5-second permission cache to reload from the persisted store.
        self.h.auth._permissions_cache = None
        self.h.auth._permissions_cache_until = 0.0
        self.assertEqual(self.h.auth.account_profile("ou_fixtureAE")["level"], "L2")
        self.assertEqual(self.h.auth.account_profile("ou_fixtureAE")["modules"], ["engineer_mop"])
        stored = self.h.auth._state_store.get_auth_permissions()["users"]["ou_fixtureAE"]
        self.assertEqual(stored["access_version"], 1)

    def test_sync_materializes_legacy_modules_in_store_without_access_version(self):
        """HR sync actually writes the old valid identity's modules to the store."""
        auth = self.h.auth
        stored = auth._load_permissions_locked()["users"]["ou_fixtureAE"]
        # The legacy L4 engineer was materialized with an explicit module set...
        self.assertEqual(stored.get("module_access_version"), 1)
        self.assertNotIn("access_version", stored)  # dirty role/scopes NOT promoted
        self.assertEqual(set(stored["modules"]), set(default_modules("L4")))
        # ...and the real store value is what the profile derives from.
        self.assertEqual(set(auth.account_profile("ou_fixtureAE")["modules"]), set(default_modules("L4")))
        self.assertEqual(auth.account_profile("ou_fixtureAE")["level"], "L4")

    def test_l2_cross_building_base_edit_but_no_override(self):
        """L2 (H engineer) can base-edit/enable a building-A engineer across floors."""
        auth = self.h.auth
        change = {**{k: self.item("AE")[k] for k in ("id", "revision", "role", "scopes", "enabled")},
                  "enabled": False}
        self.manager.update({"changes": [change]}, self.actor("HE1"))
        self.assertTrue(auth._open_id_explicitly_disabled("ou_fixtureAE"))
        # It must still be rejected from granting a higher L3 to that target.
        before = copy.deepcopy(auth._load_permissions_locked())
        with self.assertRaises(PasswordLoginError):
            self.manager.update({"changes": [self.override_change("AE", level="L3")]}, self.actor("HE1"))
        self.assertEqual(auth._load_permissions_locked(), before)

    def test_l0_can_grant_l4_multiple_buildings_a_and_b(self):
        """L0/L1 may hand a base account A+B across buildings."""
        auth = self.h.auth
        self.manager.update({"changes": [self.override_change("AE", level="L4", scopes=["A", "B"])]},
                            self.actor("HM1"))
        profile = auth.account_profile("ou_fixtureAE")
        self.assertEqual(profile["level"], "L4")
        self.assertEqual(set(profile["scope_rules"]), {"A", "B"})
        self.assertFalse(profile["can_manage_accounts"])

    def test_empty_modules_revocation_is_preserved(self):
        auth = self.h.auth
        self.manager.update({"changes": [self.override_change("AE", level="L4", modules=[])]},
                            self.actor("HM1"))
        stored = auth._load_permissions_locked()["users"]["ou_fixtureAE"]
        self.assertEqual(stored["access_version"], 1)
        self.assertEqual(stored["modules"], [])
        self.assertEqual(auth.account_profile("ou_fixtureAE")["modules"], [])

    def test_can_manage_accounts_requires_accounts_module(self):
        auth = self.h.auth
        # L0 raises AE to L2 but explicitly withholds the accounts module.
        self.manager.update({"changes": [
            self.override_change("AE", level="L2", modules=["maintenance", "change"])]},
            self.actor("HM1"))
        profile = auth.account_profile("ou_fixtureAE")
        self.assertEqual(profile["level"], "L2")
        self.assertEqual(profile["modules"], ["maintenance", "change"])
        self.assertFalse(profile["can_manage_accounts"])
        # Adding the accounts module back restores account management ability.
        self.manager.update({"changes": [
            self.override_change("AE", level="L2",
                                 modules=["maintenance", "change", "accounts"])]},
            self.actor("HM1"))
        self.assertTrue(auth.account_profile("ou_fixtureAE")["can_manage_accounts"])

    def test_unknown_or_disabled_identity_gets_no_default_modules(self):
        auth = self.h.auth
        # Identity absent from the directory must not inherit default modules.
        unknown = auth.account_profile("ou_unknown")
        self.assertEqual(unknown["modules"], [])
        self.assertFalse(unknown["can_manage_accounts"])
        # A disabled known identity also fails closed for modules.
        self.manager.update({"changes": [
            {**{k: self.item("AE")[k] for k in ("id", "revision", "role", "scopes", "enabled")},
             "enabled": False}]}, self.actor("HM1"))
        disabled = auth.account_profile("ou_fixtureAE")
        self.assertEqual(disabled["modules"], [])
        self.assertFalse(disabled["can_manage_accounts"])

    def test_old_format_payload_still_rejects_role_escalation(self):
        # Old role/scopes/enabled-only format keeps HR read-only.
        with self.assertRaisesRegex(PasswordLoginError, "不能单独更改或跨楼授权"):
            self.manager.update({"changes": [
                {**{k: self.item("AE")[k] for k in ("id", "revision", "role", "scopes", "enabled")},
                 "role": "admin", "scopes": ["ALL"]}
            ]}, self.actor("HM1"))

    def test_l2_cannot_change_peer_modules_even_with_forged_payload(self):
        before = copy.deepcopy(self.h.auth._load_permissions_locked())
        with self.assertRaises(PasswordLoginError):
            self.manager.update({"changes": [self.override_change("HE2", level="L2", modules=["accounts"])]}, self.actor("HE1"))
        self.assertEqual(self.h.auth._load_permissions_locked(), before)

    def test_disabled_manager_loses_modules_and_guest_still_has_cabinet(self):
        self.manager.update({"changes": [self.override_change("HM2", enabled=False)]}, {"open_id":LI_SHILONG_OPEN_ID})
        self.assertEqual(self.h.auth.account_profile("ou_fixtureHM2")["modules"], [])
        guest = {"role": "guest", "user": {"open_id": "guest_fixture"}}
        self.assertEqual(self.h.auth.public_user(guest)["modules"], ["cabinet"])

    def test_assignable_l1_honors_checked_modules_without_losing_business_level(self):
        self.manager.update({"changes": [self.override_change("AE", level="L1", modules=["maintenance"])]}, {"open_id":LI_SHILONG_OPEN_ID})
        profile = self.h.auth.account_profile("ou_fixtureAE")
        self.assertEqual(profile["level"], "L1")
        self.assertEqual(profile["modules"], ["maintenance"])
        self.assertFalse(profile["can_manage_accounts"])
        self.assertTrue(self.h.auth.is_admin({"user": self.actor("AE")}))
        self.h.auth.sync_directory_permissions([self.h.login.snapshot["people"]["recAE"]])
        self.assertEqual(self.h.auth.account_profile("ou_fixtureAE")["modules"], ["maintenance"])

    def test_restricted_manager_cannot_grant_unowned_defaults_by_omitting_modules(self):
        self.manager.update({"changes": [self.override_change("AE", level="L1", modules=["accounts", "maintenance"])]}, {"open_id":LI_SHILONG_OPEN_ID})
        before = copy.deepcopy(self.h.auth._load_permissions_locked())
        with self.assertRaises(PasswordLoginError):
            self.manager.update({"changes": [self.override_change("BE", level="L1")]}, self.actor("AE"))
        self.assertEqual(self.h.auth._load_permissions_locked(), before)
