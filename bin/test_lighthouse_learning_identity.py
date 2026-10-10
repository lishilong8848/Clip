"""Learning assistant identity adapter: trusted self/duty scoping for bank + materials.

These tests pin the adapter to the new self-service learning model without touching
learning.py, portal routes or the bridge: a real building shared account reads only
its own home building; a mapped self person reads only its own assigned papers and
revealed/submitted answers; an unmapped account fails closed. Client-supplied
person_id/home_scope never widen access for a non-real actor.
"""
import copy
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lan_bitable_template_portal.learning import LearningService
from lan_bitable_template_portal.lighthouse_ai import AssistantError
from lan_bitable_template_portal.lighthouse_sources import (
    _native_learning_context, question_bank, question_material)
from lan_bitable_template_portal.portal_service import BUILDING_OPEN_ID_MAP
import test_learning as learning_fixture
from test_lighthouse_stream import Store


def building_duty(scope):
    return {"id": BUILDING_OPEN_ID_MAP[scope], "is_admin": False,
            "scopes": list("ABCDEH"), "learning_scopes": [scope], "scope": scope}


def self_actor(login, building):
    return {"id": login, "is_admin": False, "scopes": [building],
            "learning_scopes": [building], "scope": building}


class LearningIdentityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.cloud = learning_fixture.FakeCloud(enabled=False)
        self.service = LearningService(self.root, self.cloud, learning_fixture.FakeSender())
        self.store = Store(self.root / "assistant.sqlite3")
        (self.root / "files").mkdir(parents=True, exist_ok=True)
        mat = {"id": "mat", "name": "mat.txt", "kind": "material",
               "file_token": "TOKEN-MAT", "local_file": "mat.txt", "size": 5}
        ans = {"id": "ans", "name": "ans.txt", "kind": "answer",
               "file_token": "TOKEN-ANS", "local_file": "ans.txt", "size": 5}
        (self.root / "files" / "mat.txt").write_text("material body", encoding="utf-8")
        (self.root / "files" / "ans.txt").write_text("answer body", encoding="utf-8")
        self.qh = learning_fixture.LearningTests.question(self, 1, attachments=[mat, ans])
        self.qa = learning_fixture.LearningTests.question(self, 100, attachments=[mat, ans])
        with self.service.transaction() as conn:
            for q in (self.qh, self.qa):
                self.service._put("question", q["id"], q, conn, False)
            for att in (mat, ans):
                self.service._put("attachment", att["id"],
                                  {**att, "question_id": self.qh["id"]}, conn, False)
            self.service._put("local", "refresh", {"at": "2026-10-03T08:00:00+08:00"}, conn, False)

    def paper(self, pid, scope, question, person_id, *, revealed=False):
        entries = {question["id"]: {"revealed": "yes"}} if revealed else {question["id"]: {}}
        with self.service.transaction() as conn:
            self.service._put("paper", pid, {
                "id": pid, "date": "2026-10-03", "scope": scope, "person_id": person_id,
                "shortage": {}, "created_at": "2026-10-03",
                "questions": copy.deepcopy([question])}, conn, False)
            self.service._put("record", pid, {"id": pid, "entries": entries, "version": 1},
                              conn, False)

    def add_person(self, pid, building, login):
        with self.service.transaction() as conn:
            self.service._put("person", pid, {
                "id": pid, "person_id": pid, "name": pid, "employee_no": pid,
                "scopes": [building], "active": True, "login_ids": [login]}, conn, False)

    def read_material(self, actor, material_id, scope="H"):
        return question_material(self.service, self.store, actor, {
            "scope": scope, "material_id": material_id, "offset": 0, "length": 4000})

    # ---- trusted identity resolution ----

    def test_duty_context_home_only_ignores_broad_scopes(self):
        duty = building_duty("H")
        allowed, native = _native_learning_context(self.service, duty)
        self.assertEqual(allowed, {"H"})
        self.assertTrue(native["shared_account"])
        self.assertEqual(native["scope"], "H")
        # Even a forged learning_scopes list cannot widen the native scope.
        duty["learning_scopes"] = ["A", "B", "C", "H"]
        allowed, native = _native_learning_context(self.service, duty)
        self.assertEqual(allowed, {"H"})

    def test_revoked_duty_account_fails_closed_even_with_local_person_login(self):
        # Duty identification must come first: even if a local person row lists the
        # building's shared open id as a login, a revoked H grant still raises 403
        # and never falls through to the ordinary person resolution.
        h_open = BUILDING_OPEN_ID_MAP["H"]
        with self.service.transaction() as conn:
            self.service._put("person", "sneaky", {
                "id": "sneaky", "person_id": "sneaky", "name": "sneaky", "employee_no": "sneaky",
                "scopes": ["H"], "active": True, "login_ids": [h_open]}, conn, False)
        revoked = {"id": h_open, "is_admin": False, "scopes": list("ABCDEH"),
                   "learning_scopes": [], "scope": "H"}
        with self.assertRaises(AssistantError) as caught:
            _native_learning_context(self.service, revoked)
        self.assertEqual(caught.exception.status, 403)
        # A 110 shared open id also never falls through to an ordinary person row.
        s110_open = BUILDING_OPEN_ID_MAP["110"]
        with self.service.transaction() as conn:
            self.service._put("person", "sneaky110", {
                "id": "sneaky110", "person_id": "sneaky110", "name": "sneaky110",
                "employee_no": "sneaky110", "scopes": ["110"], "active": True,
                "login_ids": [s110_open]}, conn, False)
        shared_110 = {"id": s110_open, "is_admin": False, "scopes": ["110"],
                      "learning_scopes": ["110"], "scope": "110"}
        with self.assertRaises(AssistantError) as caught:
            _native_learning_context(self.service, shared_110)
        self.assertEqual(caught.exception.status, 403)

    def test_mapped_self_context_is_person_only(self):
        self.add_person("sp", "H", "ou_sp")
        allowed, native = _native_learning_context(self.service, self_actor("ou_sp", "H"))
        # Own-history reads span every historical learning building, person-scoped.
        self.assertEqual(allowed, set("ABCDEH"))
        self.assertFalse(native["shared_account"])
        self.assertEqual(native["person_id"], "sp")
        self.assertEqual(native["scope"], "")

    def test_mapped_self_moved_building_keeps_own_history(self):
        # A person who moved H -> A keeps no broad building claim, but own-history
        # reads still span all historical learning buildings (person-scoped).
        with self.service.transaction() as conn:
            self.service._put("person", "moved", {
                "id": "moved", "person_id": "moved", "name": "moved", "employee_no": "moved",
                "scopes": ["A"], "active": True, "login_ids": ["ou_moved"]}, conn, False)
        allowed, native = _native_learning_context(self.service, self_actor("ou_moved", "A"))
        self.assertEqual(allowed, set("ABCDEH"))
        self.assertEqual(native["person_id"], "moved")
        self.assertEqual(native["scope"], "")

    def test_mapped_self_duplicate_building_values_are_deduped(self):
        with self.service.transaction() as conn:
            self.service._put("person", "dup", {
                "id": "dup", "person_id": "dup", "name": "dup", "employee_no": "dup",
                "scopes": ["H", "H", "A"], "active": True, "login_ids": ["ou_dup"]}, conn, False)
        allowed, _ = _native_learning_context(self.service, self_actor("ou_dup", "A"))
        self.assertEqual(allowed, set("ABCDEH"))

    def test_person_mapping_conflict_fails_closed(self):
        self.add_person("first", "H", "ou_conflict")
        self.add_person("second", "A", "ou_conflict")
        with self.assertRaises(AssistantError) as caught:
            _native_learning_context(self.service, self_actor("ou_conflict", "H"))
        self.assertEqual(caught.exception.status, 403)

    def test_inactive_mapped_person_fails_closed(self):
        with self.service.transaction() as conn:
            self.service._put("person", "ghost", {
                "id": "ghost", "person_id": "ghost", "name": "ghost", "employee_no": "ghost",
                "scopes": ["H"], "active": False, "login_ids": ["ou_ghost"]}, conn, False)
        with self.assertRaises(AssistantError) as caught:
            _native_learning_context(self.service, self_actor("ou_ghost", "H"))
        self.assertEqual(caught.exception.status, 403)

    def test_mapped_self_without_learning_building_reads_own_past_history(self):
        # Only ABCDEH are learning buildings; 110 alone is not a new claiming grant,
        # but it must not erase read access to the person's own historical papers.
        with self.service.transaction() as conn:
            self.service._put("person", "power", {
                "id": "power", "person_id": "power", "name": "power", "employee_no": "power",
                "scopes": ["110"], "active": True, "login_ids": ["ou_power"]}, conn, False)
        allowed, native = _native_learning_context(self.service, self_actor("ou_power", "110"))
        self.assertEqual(allowed, set("ABCDEH"))
        self.assertEqual(native["person_id"], "power")
        self.paper("own_old", "H", self.qh, "power", revealed=True)
        self.paper("peer_old", "H", self.qa, "peer", revealed=True)
        actor = {"id": "ou_power", "is_admin": False, "scopes": ["110"],
                 "learning_scopes": ["110"], "scope": "110"}
        bank = question_bank(self.service, actor, {"scope": "ALL"})
        self.assertEqual({q["id"] for q in bank["items"]}, {self.qh["id"]})
        self.assertNotIn(self.qa["id"], {q["id"] for q in bank["items"]})
        # The person's own revealed answer attachment still reads.
        revealed = self.read_material(actor, "ans", scope="ALL")
        self.assertIn("answer body", revealed["text"])

    def test_unmapped_context_fails_closed(self):
        with self.assertRaises(AssistantError) as caught:
            _native_learning_context(self.service, self_actor("no-such-oid", "H"))
        self.assertEqual(caught.exception.status, 403)

    def test_client_person_id_home_ignored_for_non_real_actor(self):
        forged = {"id": "ou_not_in_map", "is_admin": False, "scopes": ["H"],
                  "learning_scopes": ["H"], "scope": "H", "home_scope": "H",
                  "person_id": "sp"}
        with self.assertRaises(AssistantError) as caught:
            _native_learning_context(self.service, forged)
        self.assertEqual(caught.exception.status, 403)

    # ---- bank scope/self parity ----

    def test_mapped_self_bank_sees_own_revealed_only(self):
        self.add_person("sp", "H", "ou_sp")
        self.add_person("other", "H", "ou_other")
        self.paper("other_h", "H", self.qh, "other", revealed=True)
        self.paper("self_h", "H", self.qa, "sp", revealed=True)
        clear = question_bank(self.service, self_actor("ou_sp", "H"), {})
        self.assertEqual({q["id"] for q in clear["items"]}, {self.qa["id"]})
        # The revealed answer attachment is exposed by URL (body is fetched via the
        # material route), and the unrelated owner's question is never included.
        self.assertIn("/api/learning/attachments/ans", json.dumps(clear))
        self.assertNotIn(self.qh["id"], {q["id"] for q in clear["items"]})

    def test_unmapped_bank_denied(self):
        with self.assertRaises(AssistantError) as caught:
            question_bank(self.service, self_actor("no-such-oid", "H"), {})
        self.assertEqual(caught.exception.status, 403)

    def test_duty_cannot_cross_scope_in_bank(self):
        self.paper("h1", "H", self.qh, "", revealed=True)
        with self.assertRaises(AssistantError) as caught:
            question_bank(self.service, building_duty("H"), {"scope": "A"})
        self.assertEqual(caught.exception.status, 403)
        with self.assertRaises(AssistantError) as caught:
            question_bank(self.service, building_duty("A"), {"scope": "H"})
        self.assertEqual(caught.exception.status, 403)

    def test_mapped_self_moving_buildings_sees_own_old_historical_paper(self):
        # A person who moved from H to A now carries scopes=['A'] only. Their own old
        # H paper (revealed answer) stays visible in ALL, and the revealed attachment
        # stays readable, while an unrelated owner's H paper never leaks.
        with self.service.transaction() as conn:
            self.service._put("person", "moved", {
                "id": "moved", "person_id": "moved", "name": "moved", "employee_no": "moved",
                "scopes": ["A"], "active": True, "login_ids": ["ou_moved"]}, conn, False)
            self.service._put("person", "other", {
                "id": "other", "person_id": "other", "name": "other", "employee_no": "other",
                "scopes": ["H"], "active": True, "login_ids": ["ou_other"]}, conn, False)
        self.paper("old_h", "H", self.qh, "moved", revealed=True)
        self.paper("other_h", "H", self.qa, "other", revealed=True)
        actor = {"id": "ou_moved", "is_admin": False, "scopes": ["A", "H"],
                 "learning_scopes": ["A", "H"], "scope": "A"}
        # ALL reaches the moved person's own old H paper.
        bank = question_bank(self.service, actor, {"scope": "ALL"})
        self.assertIn(self.qh["id"], {q["id"] for q in bank["items"]})
        self.assertNotIn(self.qa["id"], {q["id"] for q in bank["items"]})
        # The revealed answer of the moved person's own old H paper is visible.
        bank_text = json.dumps(bank)
        self.assertIn(self.qh["id"], bank_text)
        self.assertIn("Choice 0", bank_text)
        # The revealed answer attachment of the moved person's own old H paper reads.
        revealed = self.read_material(actor, "ans", scope="ALL")
        self.assertIn("answer body", revealed["text"])
        # An unrelated owner's revealed answer in the same old H building never leaks.
        other_bank = question_bank(self.service, actor, {"scope": "H"})
        self.assertNotIn(self.qa["id"], {q["id"] for q in other_bank["items"]})

    def test_mapped_self_moving_explicit_own_old_building_works_too(self):
        with self.service.transaction() as conn:
            self.service._put("person", "moved", {
                "id": "moved", "person_id": "moved", "name": "moved", "employee_no": "moved",
                "scopes": ["A"], "active": True, "login_ids": ["ou_moved"]}, conn, False)
        self.paper("old_h", "H", self.qh, "moved", revealed=True)
        actor = {"id": "ou_moved", "is_admin": False, "scopes": ["A", "H"],
                 "learning_scopes": ["A", "H"], "scope": "A"}
        bank = question_bank(self.service, actor, {"scope": "H"})
        self.assertIn(self.qh["id"], {q["id"] for q in bank["items"]})

    # ---- material scope/self parity ----

    def test_mapped_self_material_cannot_read_other_owner_answer(self):
        self.add_person("sp", "H", "ou_sp")
        self.add_person("other", "H", "ou_other")
        self.paper("other_h", "H", self.qh, "other", revealed=True)
        with self.assertRaises(AssistantError) as caught:
            self.read_material(self_actor("ou_sp", "H"), "ans")
        self.assertEqual(caught.exception.status, 403)

    def test_unmapped_material_denied(self):
        with self.assertRaises(AssistantError) as caught:
            self.read_material(self_actor("no-such-oid", "H"), "mat")
        self.assertEqual(caught.exception.status, 403)

    def test_duty_cannot_cross_scope_in_material(self):
        self.paper("h1", "H", self.qh, "", revealed=True)
        with self.assertRaises(AssistantError) as caught:
            self.read_material(building_duty("A"), "mat", scope="H")
        self.assertEqual(caught.exception.status, 403)

    def test_no_cloud_calls_during_identity_resolution(self):
        self.add_person("sp", "H", "ou_sp")
        self.paper("self_h", "H", self.qa, "sp", revealed=True)
        question_bank(self.service, self_actor("ou_sp", "H"), {})
        self.read_material(self_actor("ou_sp", "H"), "mat")
        self.assertEqual(self.cloud.calls, [])


if __name__ == "__main__":
    unittest.main()