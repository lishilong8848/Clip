"""Bounded personnel-password login identity integration for the learning service.

These tests are fully offline and isolated: they exercise the identity layer of
`learning_personal` with a local HR login snapshot presented through
`get_login_people`. No cloud is pulled, no password/national-ID field is ever
read, and the dirty autonomous-learning (practice) behaviour is left untouched.

The personnel login principal is `personnel_<HR record id>` and its persistent
learning alias is exactly `directory:<HR record id>`.
"""
import concurrent.futures
import json
import threading
import unittest
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from bin import test_learning as support
from bin.test_learning import CURRENT
from lan_bitable_template_portal import learning, learning_personal as personal

# The offline harness freezes threading.Thread.start to forbid worker threads.
# The concurrency tests temporarily restore the genuine implementation, captured
# here at import time (i.e. before the per-test setUp patch is installed).
_REAL_THREAD_START = threading.Thread.start


def hr(rid, name=None, no=None, scopes=("A",), selectable=True, inactive=False, login_ids=()):
    return {"id": rid,
            "name": name if name is not None else "人员:" + rid,
            "employee_no": no if no is not None else rid,
            "scopes": list(scopes),
            "inactive": inactive,
            "selectable": selectable,
            "needs_setup": False,
            "password_revision": "",
            "login_ids": list(login_ids)}


def snapshot(*rows):
    return {"people": {str(row["id"]): row for row in rows}, "loaded_at": 1234.0}


class PasswordIdentityTests(unittest.TestCase):
    setUp = support.LearningTests.setUp
    question = support.LearningTests.question
    seed = support.LearningTests.seed

    def make_service(self, login_rows, people_reader=None):
        self.service = learning.LearningService(
            self.root, self.cloud, self.sender,
            get_people=people_reader,
            get_login_people=(lambda: snapshot(*login_rows)),
        )
        self.service._restored = True
        return self.service

    def put_person(self, pid, name="人员", aliases=(), login_ids=(), scopes=("A",), active=True):
        with self.service.transaction() as conn:
            self.service._put("person", pid, {
                "id": pid, "person_id": pid, "name": name, "employee_no": pid,
                "scopes": list(scopes), "active": active,
                "aliases": list(aliases), "login_ids": list(login_ids),
            }, conn)

    def rese(self, open_id):
        return personal.resolve_self(self.service, open_id)

    def person_rows(self):
        """key -> (revision, dirty) for every durable person document."""
        with closing(self.service._connect()) as conn:
            rows = conn.execute("SELECT key,revision,dirty FROM documents WHERE kind='person' ORDER BY key").fetchall()
            return {row["key"]: (row["revision"], bool(row["dirty"])) for row in rows}

    def dirty_summary(self):
        """Total outbox size: durable documents still awaiting cloud sync (dirty=1)."""
        with closing(self.service._connect()) as conn:
            return conn.execute("SELECT COUNT(*) FROM documents WHERE dirty=1").fetchone()[0]

    def staff_reader(self, rows):
        def reader():
            return {"people": rows}
        return reader

    def test_unique_linking_preserves_history_id_and_adds_directory_alias(self):
        # A staff-linked learner with a unique real open id already owns history.
        self.make_service([hr("recH1", login_ids=["oid-p1"])])
        self.put_person("p1", name="张三", aliases=["staff:recS1"], login_ids=["oid-p1"])
        with self.service.transaction() as conn:
            self.service._put("record", "rec-evidence", {"person_id": "p1", "history": True}, conn)

        result = self.rese("personnel_recH1")
        self.assertEqual(result["person_id"], "p1")
        self.assertEqual(result["identity_issue"], "")
        person = self.service._get("person", "p1")
        self.assertIn("directory:recH1", person["aliases"])
        # History and identity are preserved; no duplicate learner is created.
        self.assertEqual(self.service._get("record", "rec-evidence")["person_id"], "p1")
        self.assertIsNone(self.service._get("person", "directory:recH1"))

    def test_no_name_merge_creates_separate_deterministic_directory_person(self):
        # Same name AND same employee number are NOT used to merge identities.
        self.make_service([hr("recH1", name="张三", no="100", login_ids=[])])
        self.put_person("p1", name="张三", aliases=["staff:recS1"], login_ids=["oid-p1"], scopes=("A",))
        with self.service.transaction() as conn:
            self.service._put("record", "rec-evidence", {"person_id": "p1", "history": True}, conn)

        result = self.rese("personnel_recH1")
        self.assertEqual(result["person_id"], "directory:recH1")
        person = self.service._get("person", "directory:recH1")
        self.assertEqual(person["name"], "张三")
        self.assertEqual(person["employee_no"], "100")
        self.assertEqual(person["aliases"], ["directory:recH1"])
        self.assertEqual(person["login_ids"], [])
        # Existing learner history untouched.
        self.assertEqual(self.service._get("record", "rec-evidence")["person_id"], "p1")

    def test_shared_openid_keeps_separate_profiles(self):
        # Two HR rows share one real open id: ambiguous, so each gets its own
        # directory person and neither receives the shared open id as login_id.
        self.make_service([
            hr("recH1", name="甲", login_ids=["oid-shared"]),
            hr("recH2", name="乙", login_ids=["oid-shared"]),
        ])
        first = self.rese("personnel_recH1")
        second = self.rese("personnel_recH2")
        self.assertNotEqual(first["person_id"], second["person_id"])
        self.assertEqual(first["person_id"], "directory:recH1")
        self.assertEqual(second["person_id"], "directory:recH2")
        self.assertEqual(self.service._get("person", "directory:recH1")["login_ids"], [])
        self.assertEqual(self.service._get("person", "directory:recH2")["login_ids"], [])

    def test_concurrent_mapping_creates_exactly_one_learner(self):
        self.make_service([hr("recH1", login_ids=["oid-p1"])])
        with patch("threading.Thread.start", _REAL_THREAD_START):
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
                results = list(pool.map(lambda _: self.rese("personnel_recH1"), range(32)))
        ids = {r["person_id"] for r in results}
        self.assertEqual(ids, {"directory:recH1"})
        owners = [p["id"] for p in self.service._all("person") if "directory:recH1" in (p.get("aliases") or [])]
        self.assertEqual(len(owners), 1)

    def test_retirement_revokes_login_and_refresh_deactivates_but_keeps_history(self):
        self.make_service([hr("recH1")])
        result = self.rese("personnel_recH1")
        self.assertEqual(result["person_id"], "directory:recH1")
        with self.service.transaction() as conn:
            self.service._put("record", "rec-evidence", {"person_id": "directory:recH1", "history": True}, conn)

        # Personnel row retires -> login no longer resolvable, history preserved.
        self.make_service([hr("recH1", selectable=False, inactive=True)])
        denied = self.rese("personnel_recH1")
        self.assertEqual(denied["person_id"], "")
        self.assertIn("尚未关联", denied["identity_issue"])
        self.assertEqual(self.service._get("record", "rec-evidence")["person_id"], "directory:recH1")

        # refresh with the retired metadata must deactivate but keep the row.
        personal.refresh_people(self.service)
        revived = self.service._get("person", "directory:recH1")
        self.assertIsNotNone(revived)
        self.assertFalse(revived["active"])
        self.assertEqual(revived["scopes"], ["A"])
        self.assertEqual(self.service._get("record", "rec-evidence")["person_id"], "directory:recH1")

    def test_source_change_alias_binds_existing_learner_to_new_hr_record(self):
        # Login source moved from the signature scheme (staff:recS1) to the HR
        # table (directory:recH1). A unique open id must bind them, never create
        # a second personality.
        self.make_service([hr("recH1", login_ids=["oid-p1"])])
        self.put_person("p1", name="李四", aliases=["staff:recS1"], login_ids=["oid-p1"])
        with self.service.transaction() as conn:
            self.service._put("record", "rec-evidence", {"person_id": "p1", "history": True}, conn)

        result = self.rese("personnel_recH1")
        self.assertEqual(result["person_id"], "p1")
        person = self.service._get("person", "p1")
        self.assertEqual(sorted(person["aliases"]), ["directory:recH1", "staff:recS1"])
        self.assertEqual(self.service._get("record", "rec-evidence")["person_id"], "p1")

    def test_no_external_fetch_resolves_only_from_local_snapshot(self):
        called = {"people_reader": 0}

        def people_reader():
            called["people_reader"] += 1
            raise AssertionError("resolve_self must not pull the cloud directory")

        self.make_service([hr("recH1", login_ids=["oid-p1"])], people_reader=people_reader)
        self.put_person("p1", aliases=["staff:recS1"], login_ids=["oid-p1"])

        result = self.rese("personnel_recH1")
        self.assertEqual(result["person_id"], "p1")
        self.assertEqual(called["people_reader"], 0)

        # refresh with only the local login snapshot works and never touches cloud
        # (get_people is None here, so login-only refresh is exercised).
        fresh = learning.LearningService(self.root / "login_only", self.cloud, self.sender,
                                         get_people=None,
                                         get_login_people=(lambda: snapshot(hr("recA", login_ids=["oid-a"]))))
        fresh._restored = True
        personal.refresh_people(fresh)
        people = fresh._all("person")
        self.assertEqual([p["id"] for p in people], ["directory:recA"])
        self.assertNotIn("password", json.dumps(people).lower())
        self.assertNotIn("national", json.dumps(people).lower())

    def test_unsupported_building_gets_no_arbitrary_mapping(self):
        # A building outside ABCDEH (e.g. 110) must not be invented or mapped.
        self.make_service([hr("rec110", scopes=["110"], selectable=True)])
        denied = self.rese("personnel_rec110")
        self.assertEqual(denied["person_id"], "")
        self.assertIn("尚未关联", denied["identity_issue"])
        self.assertIsNone(self.service._get("person", "directory:rec110"))

    def test_multiple_supported_floors_grant_nothing_no_first_floor(self):
        # Two supported ABCDEH floors must NOT pick an arbitrary first floor:
        # autonomous/daily learning requires one clear declared building.
        self.make_service([hr("recMul", scopes=["A", "B"], login_ids=["oid-m"])])
        denied = self.rese("personnel_recMul")
        self.assertEqual(denied["person_id"], "")
        self.assertIn("尚未关联", denied["identity_issue"])
        self.assertIsNone(self.service._get("person", "directory:recMul"))

    def test_zero_supported_floors_grant_nothing(self):
        # No declared scope at all also maps to no learner, never to an invention.
        self.make_service([hr("recNone", scopes=[], login_ids=["oid-n"])])
        denied = self.rese("personnel_recNone")
        self.assertEqual(denied["person_id"], "")
        self.assertIn("尚未关联", denied["identity_issue"])
        self.assertIsNone(self.service._get("person", "directory:recNone"))

    def test_retiring_explicitly_linked_staff_keeps_history_and_follows_hr(self):
        # A staff learner explicitly bound to the HR directory alias via a unique
        # real open id is shared by the same resolver.  When the HR row later
        # retires, the current profile must follow HR (deactivate) while history
        # and the learner row are preserved.
        self.make_service([hr("recH1", login_ids=["oid-p1"])])
        self.put_person("p1", name="张三", aliases=["staff:recS1"], login_ids=["oid-p1"])
        with self.service.transaction() as conn:
            self.service._put("record", "rec-evidence", {"person_id": "p1", "history": True}, conn)
        # First login binds directory:recH1 onto the existing staff learner.
        self.assertEqual(self.rese("personnel_recH1")["person_id"], "p1")
        self.assertIn("directory:recH1", self.service._get("person", "p1")["aliases"])

        # HR retires the row; the full refresh deactivates p1, keeps the row alive
        # (active=False) and never deletes historical records.
        self.make_service([hr("recH1", selectable=False, inactive=True)])
        personal.refresh_people(self.service)
        person = self.service._get("person", "p1")
        self.assertIsNotNone(person)
        self.assertFalse(person["active"])
        self.assertEqual(self.service._get("record", "rec-evidence")["person_id"], "p1")
        # The retired row no longer resolves as a valid login.
        denied = self.rese("personnel_recH1")
        self.assertEqual(denied["person_id"], "")
        self.assertIn("尚未关联", denied["identity_issue"])

    def test_refresh_before_first_login_links_unique_feishu_learner(self):
        # refresh-before-firstlogin: an existing unique Feishu learner already owns
        # history; a full HR refresh must BIND it through its unique open id (the
        # same algorithm lazy resolve uses), never materialise a duplicate
        # directory-only learner.
        self.make_service([hr("recH1", login_ids=["oid-p1"])])
        self.put_person("p1", name="张三", aliases=["staff:recS1"], login_ids=["oid-p1"])
        with self.service.transaction() as conn:
            self.service._put("record", "rec-evidence", {"person_id": "p1", "history": True}, conn)

        personal.refresh_people(self.service)
        person = self.service._get("person", "p1")
        self.assertIn("directory:recH1", person["aliases"])
        self.assertIsNone(self.service._get("person", "directory:recH1"))
        self.assertEqual(self.service._get("record", "rec-evidence")["person_id"], "p1")
        # The later login resolves to the same learner (no duplicate created).
        self.assertEqual(self.rese("personnel_recH1")["person_id"], "p1")
        owners = [p["id"] for p in self.service._all("person") if "directory:recH1" in (p.get("aliases") or [])]
        self.assertEqual(owners, ["p1"])

    def test_full_refresh_repeated_preserves_directory_alias(self):
        # alias-preservation-across-repeated-fullrefresh: a staff refresh must never
        # drop a previously bound directory alias, and repeated full (staff+HR)
        # refreshes must keep exactly one owner for that alias.
        self.make_service([hr("recH1", login_ids=["oid-p1"])])
        self.put_person("p1", name="张三", aliases=["staff:recS1"], login_ids=["oid-p1"])
        self.rese("personnel_recH1")  # attach the directory alias first
        self.assertIn("directory:recH1", self.service._get("person", "p1")["aliases"])

        self.service._people_reader = lambda: {"people": [
            {"person_key": "staff:recS1", "record_aliases": ["staff:recS1"],
             "name": "张三", "employee_no": "100", "building": "A楼"},
        ]}
        for _ in range(2):
            personal.refresh_people(self.service)
            person = self.service._get("person", "p1")
            self.assertIn("directory:recH1", person["aliases"])
            self.assertIn("staff:recS1", person["aliases"])
            self.assertIsNone(self.service._get("person", "directory:recH1"))
        owners = [p["id"] for p in self.service._all("person") if "directory:recH1" in (p.get("aliases") or [])]
        self.assertEqual(owners, ["p1"])

    def test_floor_A_to_B_replaces_scopes_no_union(self):
        # floorAtoB no union: HR scopes must REPLACE current scopes exactly — a move
        # from building A to building B must NOT retain A, while historical paper
        # scope stays untouched.
        self.make_service([hr("recH1", login_ids=["oid-p1"], scopes=("A",))])
        self.put_person("p1", name="张三", aliases=["staff:recS1"], login_ids=["oid-p1"], scopes=("A",))
        self.rese("personnel_recH1")
        self.assertEqual(self.service._get("person", "p1")["scopes"], ["A"])
        with self.service.transaction() as conn:
            self.service._put("paper", "old-paper", {"id": "old-paper", "person_id": "p1",
                                                     "scope": "A", "date": "2026-09-01"}, conn, False)

        # HR re-pulls and the person moved to building B.
        self.make_service([hr("recH1", login_ids=["oid-p1"], scopes=("B",))])
        personal.refresh_people(self.service)
        self.assertEqual(self.service._get("person", "p1")["scopes"], ["B"])
        # Historical paper scope remains A (papers are immutable snapshots).
        self.assertEqual(self.service._get("paper", "old-paper")["scope"], "A")

    def test_missing_hr_snapshot_preserves_then_explicit_retirement_deactivates(self):
        # metadata-absent-preserve-then-explicit-retirement: a MISSING HR snapshot
        # (reader -> {}) must not silently wipe existing directory people, but a
        # complete-empty/retired snapshot still deactivates cleanly.
        self.make_service([hr("recH1")])
        self.assertEqual(self.rese("personnel_recH1")["person_id"], "directory:recH1")
        self.assertTrue(self.service._get("person", "directory:recH1")["active"])

        self.service._login_people_reader = lambda: {}
        personal.refresh_people(self.service)
        self.assertTrue(self.service._get("person", "directory:recH1")["active"])

        # Explicit retirement (people key present, row inactive) deactivates.
        self.service._login_people_reader = lambda: snapshot(hr("recH1", selectable=False, inactive=True))
        personal.refresh_people(self.service)
        revived = self.service._get("person", "directory:recH1")
        self.assertIsNotNone(revived)
        self.assertFalse(revived["active"])
        self.assertEqual(revived["scopes"], ["A"])

    def test_hr_only_refresh_keeps_unrelated_staff_only_person_active(self):
        # Only the HR login snapshot was requested (signature reader is None); an
        # existing staff-only learner absent from HR must not be deactivated merely
        # because the staff source was not presented in this pass.
        self.make_service([hr("otherH", login_ids=["oid-other"])])
        self.put_person("p1", name="张三", aliases=["staff:recS1"], login_ids=["oid-p1"])
        personal.refresh_people(self.service)
        self.assertTrue(self.service._get("person", "p1")["active"])

    # --- Defect 1: unchanged HR refresh must not bump revisions/outbox. ---
    def test_repeated_unchanged_hr_refresh_leaves_revision_and_outbox_unchanged(self):
        self.make_service([hr("recH1", login_ids=["oid-p1"])])
        self.assertEqual(self.rese("personnel_recH1")["person_id"], "directory:recH1")
        # Settle once (binding may legitimately write on the first pass).
        personal.refresh_people(self.service)
        before = self.person_rows()
        for _ in range(3):
            personal.refresh_people(self.service)
        after = self.person_rows()
        self.assertEqual(after, before)
        self.assertEqual(self.dirty_summary(), self.dirty_summary())
        # No person revision advanced and none was re-enqueued for the cloud outbox.
        self.assertEqual(after, {k: (v[0], v[1]) for k, v in before.items()})

    # --- Defect 2: shared/unique OID index computed once per sync, not per row. ---
    def test_shared_openids_computed_once_per_sync_not_per_row(self):
        calls = []
        original = personal._shared_openids

        def counting(login_people):
            calls.append(1)
            return original(login_people)

        rows = [hr(f"rec{i}", login_ids=[f"oid-{i}"]) for i in range(288)]
        self.make_service(rows)
        with patch.object(personal, "_shared_openids", counting):
            personal.refresh_people(self.service)
        # One Counter pass for the whole 288-row refresh, then every per-row
        # resolver reuses the precomputed ambiguous set (no cubic recompute).
        self.assertEqual(len(calls), 1)
        self.assertEqual(len([p for p in self.service._all("person") if p.get("active")]), 288)

    # --- Defect 3: cold restore must never create/bind a NEW learner. ---
    def test_cold_restore_never_creates_new_learner_and_requests_worker(self):
        self.make_service([hr("recH1", login_ids=["oid-p1"])])
        self.service._restored = False
        self.service._restore_requested = False
        result = self.rese("personnel_recH1")
        self.assertEqual(result["person_id"], "")
        self.assertIn("正在恢复", result["identity_issue"])
        self.assertTrue(self.service._restore_requested)
        self.assertIsNone(self.service._get("person", "directory:recH1"))
        self.assertEqual(len(self.service._all("person")), 0)
        self.assertEqual(self.dirty_summary(), 0)  # no outbox write

    def test_cold_restore_still_reads_existing_local_exact_learner(self):
        self.make_service([hr("recH1", login_ids=["oid-p1"])])
        self.assertEqual(self.rese("personnel_recH1")["person_id"], "directory:recH1")
        before = self.person_rows()
        self.service._restored = False
        result = self.rese("personnel_recH1")  # fast exact read stays available
        self.assertEqual(result["person_id"], "directory:recH1")
        self.assertEqual(self.person_rows(), before)

    # --- Defect 4: staff-first refresh binds to existing directory learner. ---
    def test_staff_first_refresh_binds_alias_to_existing_directory_learner(self):
        self.make_service([hr("recH1", login_ids=["oid-p1"])])
        self.assertEqual(self.rese("personnel_recH1")["person_id"], "directory:recH1")
        self.service._people_reader = self.staff_reader([
            {"person_key": "staff:recS1", "record_aliases": ["staff:recS1"],
             "name": "张三", "employee_no": "100", "building": "A楼", "open_id": "oid-p1"},
        ])
        personal.refresh_people(self.service)
        owners = [p["id"] for p in self.service._all("person")
                  if "directory:recH1" in (p.get("aliases") or [])]
        self.assertEqual(owners, ["directory:recH1"])
        self.assertIn("staff:recS1", self.service._get("person", "directory:recH1")["aliases"])
        # exactly one learner, no duplicate person_... created
        self.assertEqual(len(self.service._all("person")), 1)

    def test_staff_first_refresh_with_ambiguous_openid_never_guesses(self):
        self.make_service([
            hr("recH1", login_ids=["oid-shared"]),
            hr("recH2", login_ids=["oid-shared"]),
        ])
        self.assertEqual(self.rese("personnel_recH1")["person_id"], "directory:recH1")
        self.assertEqual(self.rese("personnel_recH2")["person_id"], "directory:recH2")
        self.service._people_reader = self.staff_reader([
            {"person_key": "staff:recS1", "record_aliases": ["staff:recS1"],
             "name": "共享员工", "employee_no": "200", "building": "A楼", "open_id": "oid-shared"},
        ])
        personal.refresh_people(self.service)
        # A shared/ambiguous open id must never be guessed onto one of the two
        # directory learners; it stays a separate staff person instead.
        for pid in ("directory:recH1", "directory:recH2"):
            self.assertNotIn("staff:recS1", self.service._get("person", pid).get("aliases") or [])

    # --- Defect 5: exactly ONE declared scope total. ---
    def test_mixed_supported_and_unsupported_scopes_grant_nothing(self):
        self.make_service([hr("recMix", scopes=["A", "110"], login_ids=["oid-m"])])
        denied = self.rese("personnel_recMix")
        self.assertEqual(denied["person_id"], "")
        self.assertIn("尚未关联", denied["identity_issue"])
        self.assertIsNone(self.service._get("person", "directory:recMix"))
        self.assertEqual(personal._hr_learning_scopes(hr("recMix", scopes=["A", "110"])), [])

    # --- Defect 6: preserve source HR authority; no signature/HR floor flapping. ---
    def test_hr_authority_prevents_signature_floor_flapping(self):
        # HR says floor B / name 乙 while the legacy signature says floor A / 甲.
        self.make_service([hr("recH1", name="乙", no="h1", scopes=["B"], login_ids=["oid-p1"])])
        self.assertEqual(self.rese("personnel_recH1")["person_id"], "directory:recH1")
        self.service._people_reader = self.staff_reader([
            {"person_key": "staff:recS1", "record_aliases": ["staff:recS1"],
             "name": "甲", "employee_no": "s1", "building": "A楼", "open_id": "oid-p1"},
        ])
        # Converging refresh binds the staff alias AND keeps HR authority (floor B / 乙),
        # so exactly ONE write occurs (revision 2 from the lazy-create at revision 1).
        personal.refresh_people(self.service)
        after_bind = self.person_rows()
        self.assertEqual(after_bind["directory:recH1"][0], 2)  # no A->B double write
        person = self.service._get("person", "directory:recH1")
        self.assertIn("staff:recS1", person["aliases"])
        self.assertEqual(person["scopes"], ["B"])
        self.assertEqual(person["name"], "乙")

        # Repeated identical refreshes never change revisions/outbox again.
        for _ in range(3):
            personal.refresh_people(self.service)
        self.assertEqual(self.person_rows(), after_bind)

    def test_duplicate_staff_openid_does_not_merge_different_staff_rows(self):
        self.make_service([hr("recH1", login_ids=["oid-shared"])], people_reader=self.staff_reader([
            {"person_key": "staff:recS1", "record_aliases": ["staff:recS1"], "name": "甲", "building": "A楼", "open_id": "oid-shared"},
            {"person_key": "staff:recS2", "record_aliases": ["staff:recS2"], "name": "乙", "building": "A楼", "open_id": "oid-shared"},
        ]))
        personal.refresh_people(self.service)
        rows = self.service._all("person")
        first = next(p for p in rows if "staff:recS1" in p["aliases"])
        second = next(p for p in rows if "staff:recS2" in p["aliases"])
        self.assertNotEqual(first["id"], second["id"])
        self.assertNotIn("directory:recH1", first["aliases"] + second["aliases"])

    def test_retired_or_unassigned_hr_does_not_flap_on_signature_refresh(self):
        row = hr("recH1", login_ids=["oid-p1"])
        self.make_service([row], people_reader=self.staff_reader([
            {"person_key": "staff:recS1", "record_aliases": ["staff:recS1"], "name": "甲", "building": "A楼", "open_id": "oid-p1"},
        ]))
        personal.refresh_people(self.service)
        for changes in ({"inactive": True, "selectable": False}, {"inactive": False, "selectable": True, "scopes": []}):
            row.update(changes)
            personal.refresh_people(self.service)
            expected = self.person_rows()
            for _ in range(3):
                personal.refresh_people(self.service)
            self.assertEqual(self.person_rows(), expected)

    def test_hr_metadata_change_writes_once_then_repeats_are_noop(self):
        self.make_service([hr("recH1", name="乙", no="h1", scopes=["B"], login_ids=["oid-p1"])])
        self.assertEqual(self.rese("personnel_recH1")["person_id"], "directory:recH1")
        self.service._people_reader = self.staff_reader([
            {"person_key": "staff:recS1", "record_aliases": ["staff:recS1"],
             "name": "甲", "employee_no": "s1", "building": "A楼", "open_id": "oid-p1"},
        ])
        personal.refresh_people(self.service)  # bind staff alias under HR authority
        before = self.person_rows()

        # HR metadata changes name and floor; signature still says floor A / 甲.
        self.make_service([hr("recH1", name="丙", no="h1", scopes=["C"], login_ids=["oid-p1"])])
        self.service._people_reader = self.staff_reader([
            {"person_key": "staff:recS1", "record_aliases": ["staff:recS1"],
             "name": "甲", "employee_no": "s1", "building": "A楼", "open_id": "oid-p1"},
        ])
        personal.refresh_people(self.service)
        changed = self.person_rows()
        self.assertTrue(changed["directory:recH1"][0] > before["directory:recH1"][0])
        person = self.service._get("person", "directory:recH1")
        self.assertEqual(person["scopes"], ["C"])
        self.assertEqual(person["name"], "丙")

        # Repeated identical refreshes are now no-ops.
        for _ in range(3):
            personal.refresh_people(self.service)
        self.assertEqual(self.person_rows(), changed)


if __name__ == "__main__":
    unittest.main()
