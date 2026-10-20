"""Module-only batches against real account policy and isolated SQLite."""
import copy
from unittest.mock import patch

from .test_personnel_password_login import _TempDirTestCase, PersonnelLoginHarness, _page, _user
from lan_bitable_template_portal.account_management import AccountManagement
from lan_bitable_template_portal.personnel_password_login import PasswordLoginError
from lan_bitable_template_portal.portal_auth import LI_SHILONG_OPEN_ID


class AccountBulkModuleTests(_TempDirTestCase):
    def setUp(self):
        super().setUp()
        self.h = PersonnelLoginHarness(self.root)
        self.addCleanup(self.h.close)
        rows = []
        for key, home, job in (("A", "A", "值班员"), ("B", "B", "值班员"),
                               ("AS", "A", "运维主管"), ("HM", "H", "运维经理"),
                               ("HM2", "H", "运维经理"), ("HE", "H", "电气工程师")):
            fields = {"姓名": "隔离" + key, "员工姓名": _user("隔离" + key, "ou_bulk" + key),
                      "机楼/专业": home + "楼", "体系岗位": job, "账号性质": "外部账号"}
            self.h.service._records["rec" + key] = fields
            rows.append({"record_id": "rec" + key, "fields": fields})
        self.h.service._pages = [_page(rows)]
        self.h.login.directory(force=True)
        self.manager = AccountManagement(self.h.login)
        self.actor = {"open_id": LI_SHILONG_OPEN_ID, "name": "隔离管理员"}
        for key in ("A", "B", "AS", "HM", "HM2", "HE"):
            self.manager.update({"changes": [self.change(key, ["maintenance", "knowledge", "event", "accounts"]
                if key in {"AS", "HM", "HM2", "HE"} else ["maintenance", "knowledge", "event"])]}, self.actor)

    def item(self, key):
        return next(item for item in self.manager.accounts()["items"] if item["id"] == "rec" + key)

    def change(self, key, modules):
        item = self.item(key)
        return {"id": item["id"], "revision": item["revision"], "enabled": item["enabled"], "modules": modules}

    def persisted(self):
        return copy.deepcopy(self.h.auth._load_permissions_locked())

    def assert_rejected_atomically(self, changes, actor=None, message=None):
        before = self.persisted()
        with patch.object(self.h.auth, "_save_permissions_locked", wraps=self.h.auth._save_permissions_locked) as save:
            with self.assertRaises(PasswordLoginError) as raised:
                self.manager.update({"changes": changes}, actor or self.actor)
            save.assert_not_called()
        if message:
            self.assertIn(message, str(raised.exception))
        self.assertEqual(self.persisted(), before)

    def test_add_remove_preserves_other_modules_and_account_data_with_one_save(self):
        # Distinct existing grants must survive module-only edits.
        self.manager.update({"changes": [self.change("B", ["learning", "knowledge"])]}, self.actor)
        self.h.login.store.put_document("fixture_history", "business", {"keep": "history"})
        self.h.login.store.put_document("fixture_signature", "signature", {"keep": "signature"})
        roster = copy.deepcopy(self.h.login.snapshot)
        for mode in ("add", "remove"):
            before = self.persisted()
            changes = []
            for key in ("A", "B"):
                original = self.item(key)["modules"]
                modules = list(dict.fromkeys(original + ["water", "change"])) if mode == "add" else [m for m in original if m not in {"water", "change"}]
                changes.append(self.change(key, modules))
            with patch.object(self.h.auth, "_save_permissions_locked", wraps=self.h.auth._save_permissions_locked) as save:
                result = self.manager.update({"changes": changes}, self.actor)
                self.assertEqual(result, {"changed": 2})
                self.assertEqual(save.call_count, 1)
            after = self.persisted()
            for oid, config in before["users"].items():
                if oid in {"ou_bulkA", "ou_bulkB"}:
                    self.assertEqual({k: v for k, v in after["users"][oid].items() if k != "modules"},
                                     {k: v for k, v in config.items() if k != "modules"})
                else:
                    self.assertEqual(after["users"][oid], config)
            for change in changes:
                key = change["id"][3:]
                self.assertEqual(self.item(key)["modules"], change["modules"])
            self.assertEqual(self.h.login.snapshot, roster)
            self.assertEqual(self.h.login.store.get_document("fixture_history", "business"), {"keep": "history"})
            self.assertEqual(self.h.login.store.get_document("fixture_signature", "signature"), {"keep": "signature"})
        self.assertEqual(self.h.service._patch_calls, 0)
        audits = self.h.login.store.list_business_operation_audits(domain="accounts", limit=100)
        self.assertTrue(any(row["status"] == "success" and len(row["metadata"].get("changes", [])) == 2 for row in audits))

    def test_stale_second_revision_rolls_back_first_change(self):
        stale = self.change("B", ["water"])
        self.manager.update({"changes": [self.change("B", ["learning"])]}, self.actor)
        self.assert_rejected_atomically([self.change("A", ["change"]), stale], message="已变化")

    def test_disabled_listing_hides_saved_modules_so_client_must_not_merge_it(self):
        disabled = self.change("B", ["learning", "knowledge"])
        disabled["enabled"] = False
        self.manager.update({"changes": [disabled]}, self.actor)
        self.assertEqual(self.persisted()["users"]["ou_bulkB"]["modules"], ["learning", "knowledge"])
        self.assertEqual(self.item("B")["modules"], [])
        self.assertFalse(self.item("B")["enabled"])

    def test_second_out_of_scope_target_rolls_back_first_change(self):
        self.assert_rejected_atomically([self.change("A", ["change"]), self.change("B", ["change"])],
                                       {"open_id": "ou_bulkAS"})

    def test_preserved_module_outside_actor_authority_rejects_without_stripping(self):
        self.manager.update({"changes": [self.change("AS", ["maintenance", "accounts"])]}, self.actor)
        self.assert_rejected_atomically([self.change("A", ["maintenance", "knowledge", "event", "change"])],
                                       {"open_id": "ou_bulkAS"}, "自己没有的模块")
        self.assertEqual(self.item("A")["modules"], ["maintenance", "knowledge", "event"])

    def test_l4_cannot_receive_accounts_and_empty_revocation_is_explicit(self):
        self.assert_rejected_atomically([self.change("A", ["change"]), self.change("B", ["accounts"])],
                                       message="L4")
        self.assertEqual(self.manager.update({"changes": [self.change("A", [])]}, self.actor), {"changed": 1})
        self.assertEqual(self.item("A")["modules"], [])

    def test_self_peer_higher_and_l2_access_changes_fail_atomically(self):
        actor = {"open_id": "ou_bulkHM"}
        for target in ("HM", "HM2"):
            with self.subTest(target=target):
                self.assert_rejected_atomically([self.change("A", ["change"]), self.change(target, ["accounts"])], actor)
        higher = next(item for item in self.manager.accounts()["items"] if item["open_id"] == LI_SHILONG_OPEN_ID)
        change = {key: higher[key] for key in ("id", "revision", "enabled")}
        self.assert_rejected_atomically([self.change("A", ["change"]), {**change, "modules": ["accounts"]}], actor)
        self.assert_rejected_atomically([self.change("A", ["change"])], {"open_id": "ou_bulkHE"})

    def test_blocked_second_identity_and_duplicate_target_fail_atomically(self):
        changes = [self.change("A", ["change"]), self.change("B", ["change"])]
        self.h.login.snapshot["people"]["recB"]["disabled_reason"] = "隔离身份待核对"
        self.assert_rejected_atomically(changes)
        self.h.login.snapshot["people"]["recB"].pop("disabled_reason")
        one = self.change("A", ["change"])
        self.assert_rejected_atomically([one, one], message="重复")

    def test_accepts_300_distinct_accounts_rejects_301_before_saving(self):
        people = self.h.login.snapshot["people"]
        for index in range(300):
            person = copy.deepcopy(people["recA"])
            person.update(id="recLimit" + str(index), name="边界人员" + str(index), login_ids=["ou_bulkLimit" + str(index)])
            people[person["id"]] = person
        self.h.auth._account_index_cache = (None, {})
        listing = self.manager.accounts()["items"]
        changes = [{"id": item["id"], "revision": item["revision"], "enabled": item["enabled"], "modules": ["maintenance"]}
                   for item in listing if item["id"].startswith("recLimit")]
        self.assertEqual(len(changes), 300)
        self.assert_rejected_atomically(changes + [self.change("A", ["change"])], message="300")
        with patch.object(self.h.auth, "_save_permissions_locked", wraps=self.h.auth._save_permissions_locked) as save:
            self.assertEqual(self.manager.update({"changes": changes}, self.actor), {"changed": 300})
            self.assertEqual(save.call_count, 1)
