"""One-time migration evidence using synthetic records, with no service imports."""
import copy
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal.feishu_identity_manifest import (
    DUTY_PAIRS, TARGET_APP_ID, build_identity_manifest, load_identity_manifest,
    load_manifest, merge_identity_manifests, validate_manifest,
)


def row(rid="recOne", new="ou_New", old="ou_Old", name="Synthetic"):
    fields = {"姓名": name, "员工姓名": [{"name": name, "id": new}] if new else [],
              "飞书 open_id": old}
    return {"record_id": rid, "fields": fields}


def snapshot(rid="recOne", old="ou_Old"):
    return {"people": {rid: {"id": rid, "login_ids": [old], "name": "Synthetic"}}}


class FeishuIdentityManifestTests(unittest.TestCase):
    def build(self, rows, people=None, bindings=()):
        return build_identity_manifest(people or {}, bindings, rows, target_app_id=TARGET_APP_ID)

    def personal(self, manifest):
        return [item for item in manifest["mappings"] if "user_supplied_duty" not in item["evidence"]]

    def test_stable_record_and_matching_old_sources_produce_minimal_mapping(self):
        result = self.build([row()], snapshot())
        self.assertEqual(result["version"], 1)
        self.assertEqual(result["target_app_id"], TARGET_APP_ID)
        self.assertEqual(self.personal(result), [{"old_open_id": "ou_Old", "new_open_id": "ou_New",
            "record_ids": ["recOne"], "evidence": ["hr_text_open_id", "personnel_snapshot", "target_hr_person_field"]}])
        self.assertEqual(result["unresolved"], [])
        self.assertEqual(validate_manifest(result), result)

    def test_repeated_records_sharing_same_old_and_new_are_one_identity(self):
        rows = [row("recOne"), row("recTwo")]
        rows[1]["fields"]["离职/异动情况"] = True
        result = self.build(rows, {"people": {**snapshot()["people"], **snapshot("recTwo")["people"]}})
        self.assertEqual(len(self.personal(result)), 1)
        self.assertEqual(self.personal(result)[0]["record_ids"], ["recOne", "recTwo"])
        self.assertIn("shared_rows", self.personal(result)[0]["evidence"])
        self.assertEqual(result["unresolved"], [])

    def test_different_old_accounts_cannot_merge_into_same_new_account(self):
        result = self.build([row("recOne", old="ou_First"), row("recTwo", old="ou_Second")])
        self.assertEqual(self.personal(result), [])
        self.assertEqual({item["reason"] for item in result["unresolved"]}, {"target_multiple_old_ids"})
        self.assertEqual({oid for item in result["unresolved"] for oid in item["old_open_ids"]}, {"ou_First", "ou_Second"})

    def test_one_old_account_cannot_split_between_new_accounts(self):
        result = self.build([row("recOne", new="ou_First"), row("recTwo", new="ou_Second")])
        self.assertEqual(self.personal(result), [])
        self.assertEqual({item["reason"] for item in result["unresolved"]}, {"old_id_multiple_targets"})

    def test_same_name_never_bridges_different_records(self):
        result = self.build([row("recOther", old="", name="Synthetic")], snapshot())
        self.assertEqual(self.personal(result), [])
        self.assertEqual({item["reason"] for item in result["unresolved"]},
                         {"missing_old_evidence", "record_missing_from_target_hr"})
        separate = self.build([row("recOne"), row("recTwo", new="ou_NewTwo", old="ou_OldTwo")])
        self.assertEqual(len(self.personal(separate)), 2)

    def test_conflicting_snapshot_text_and_binding_are_unresolved(self):
        binding = {"payload": {"record_id": "recOne", "open_id": "ou_Third", "matched_by": "record_id"}}
        result = self.build([row(old="ou_Second")], snapshot(), [binding])
        self.assertEqual(self.personal(result), [])
        self.assertEqual(result["unresolved"][0]["reason"], "old_evidence_conflict")
        self.assertEqual(result["unresolved"][0]["old_open_ids"], ["ou_Old", "ou_Second", "ou_Third"])

    def test_sender_binding_needs_verified_record_evidence(self):
        for kind in ("record_id", "union_id", "admin", "name", ""):
            with self.subTest(kind=kind):
                binding = {"payload": {"record_id": "recOne", "open_id": "ou_Old", "matched_by": kind}}
                result = self.build([row(old="")], bindings=[binding])
                self.assertEqual(len(self.personal(result)), int(kind in {"record_id", "union_id", "admin"}))

    def test_missing_or_multi_person_fields_never_choose_first_user(self):
        for users in ([], [{"id": "ou_New"}, {"id": "ou_Other"}], [{"id": "not-an-open-id"}]):
            with self.subTest(users=users):
                item = row()
                item["fields"]["员工姓名"] = users
                result = self.build([item], snapshot())
                self.assertEqual(self.personal(result), [])
                self.assertEqual(result["unresolved"][0]["reason"], "missing_or_ambiguous_target_person")

    def test_duplicate_hr_record_ids_are_not_published(self):
        result = self.build([row(), row()], snapshot())
        self.assertEqual(self.personal(result), [])
        self.assertEqual(result["unresolved"][0]["reason"], "duplicate_hr_record_id")

    def test_explicit_duty_pairs_need_no_name_or_hr_record(self):
        result = self.build([])
        self.assertEqual(len(result["mappings"]), 7)
        self.assertEqual(DUTY_PAIRS["ou_55647926b7fbfe46507ef7e8afa76315"], "ou_3e7a45be43f809cd54e277243e9821ab")
        self.assertTrue(all(item["record_ids"] == [] and item["evidence"] == ["user_supplied_duty"]
                            for item in result["mappings"]))
        with self.assertRaises(ValueError):
            build_identity_manifest({}, [], [], target_app_id="cli_future")

    def test_hr_cannot_silently_override_explicit_duty_identity(self):
        old, new = next(iter(DUTY_PAIRS.items()))
        result = self.build([row(new=new, old="ou_Unrelated")])
        self.assertNotIn(old, {item["old_open_id"] for item in result["mappings"]})
        self.assertTrue(any(item["reason"] == "target_multiple_old_ids" for item in result["unresolved"]))

    def test_rich_text_old_ids_are_parsed_without_names(self):
        item = row()
        item["fields"]["飞书 open_id"] = [{"text": "ou_Old"}]
        self.assertEqual(self.personal(self.build([item]))[0]["old_open_id"], "ou_Old")

    def test_rewritten_snapshot_is_not_conflicting_old_identity(self):
        result = self.build([row()], snapshot(old="ou_New"))
        self.assertEqual(self.personal(result)[0]["old_open_id"], "ou_Old")
        self.assertEqual(result["unresolved"], [])

    def test_merge_trusted_saved_records_is_deterministic_and_preserves_inputs(self):
        first = self.build([row(old="")], snapshot())
        second = self.build([row("recTwo", new="ou_NewTwo", old="")], snapshot("recTwo", "ou_OldTwo"))
        originals = copy.deepcopy([first, second])
        result = merge_identity_manifests(first, second, first)
        self.assertEqual(len(self.personal(result)), 2)
        self.assertEqual([first, second], originals)
        self.assertEqual(result, merge_identity_manifests(second, first))

    def test_conflicting_trusted_profiles_are_not_last_writer_wins(self):
        first = self.build([row(old="ou_First")])
        second = self.build([row("recTwo", old="ou_Second")])
        result = merge_identity_manifests(first, second)
        self.assertEqual(self.personal(result), [])
        self.assertEqual({item["reason"] for item in result["unresolved"]}, {"target_multiple_old_ids"})

    def test_same_record_disagreement_across_profiles_is_not_two_independent_accounts(self):
        first = self.build([row()])
        second = self.build([row(old="ou_AnotherOld", new="ou_AnotherNew")])
        result = merge_identity_manifests(first, second)
        self.assertEqual(self.personal(result), [])
        self.assertEqual({item["reason"] for item in result["unresolved"]}, {"record_identity_conflict"})

    def test_identity_chains_cannot_rewrite_one_account_through_another(self):
        result = self.build([row("recOne", old="ou_First", new="ou_Second"),
                             row("recTwo", old="ou_Second", new="ou_Third")])
        self.assertEqual(self.personal(result), [])
        self.assertEqual({item["reason"] for item in result["unresolved"]}, {"identity_chain"})

    def test_validation_and_file_loading_require_provenance_and_one_to_one_mapping(self):
        valid = self.build([row()])
        for mutation in ("wrong_app", "wrong_version", "target_conflict", "missing_record", "untrusted_evidence", "secret_field"):
            with self.subTest(mutation=mutation):
                invalid = copy.deepcopy(valid)
                item = next(item for item in invalid["mappings"] if item["old_open_id"] == "ou_Old")
                if mutation == "wrong_app":
                    invalid["target_app_id"] = "cli_future"
                elif mutation == "wrong_version":
                    invalid["version"] = True
                elif mutation == "target_conflict":
                    invalid["mappings"].append({**item, "old_open_id": "ou_Other"})
                elif mutation == "missing_record":
                    item["record_ids"] = []
                elif mutation == "untrusted_evidence":
                    item["evidence"] = ["name"]
                else:
                    item["secret"] = "fixture-secret"
                with self.assertRaises(ValueError):
                    validate_manifest(invalid)
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "profile.json"
            path.write_text(json.dumps(valid), encoding="utf-8")
            self.assertEqual(load_identity_manifest(path), valid)
            self.assertEqual(load_manifest(path), valid)

    def test_profile_does_not_copy_sensitive_or_unrelated_fields(self):
        item = row(name="DO-NOT-COPY-NAME")
        item["fields"].update({"密码": "DO-NOT-COPY-PASSWORD", "手机号": "DO-NOT-COPY-PHONE",
                               "身份证号": "DO-NOT-COPY-NATIONAL-ID", "token": "DO-NOT-COPY-TOKEN"})
        result = self.build([item], snapshot())
        self.assertNotIn("DO-NOT-COPY", json.dumps(result))
        self.assertEqual(set(result), {"version", "target_app_id", "mappings", "unresolved"})

    def test_import_and_packaging_validation_do_not_import_config_or_service(self):
        code = """import sys
from lan_bitable_template_portal.feishu_identity_manifest import validate_manifest
assert validate_manifest({'version':1,'target_app_id':'cli_aa42dc770a381cb3','mappings':[],'unresolved':[]})['version']==1
assert 'upload_event_module.config' not in sys.modules
assert 'lan_bitable_template_portal.portal_service' not in sys.modules
"""
        result = subprocess.run([sys.executable, "-c", code], cwd=BIN_DIR, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
