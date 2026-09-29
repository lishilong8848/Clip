"""Offline core regressions; run with python -m unittest discover -s bin -p test_learning.py -v."""
import copy
import datetime as dt
import hashlib
import json
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import Mock, patch

BIN = Path(__file__).resolve().parent
if str(BIN) not in sys.path:
    sys.path.insert(0, str(BIN))

from lan_bitable_template_portal import learning


ACTOR = {"id": "a", "scope": "A", "is_admin": False}
ADMIN = {"id": "admin", "is_admin": True, "scope": ""}
H_ACTOR = {"id": "h", "scope": "H", "is_admin": False}
DAY = dt.date(2026, 9, 28)
CURRENT = dt.datetime(2026, 9, 28, 9, tzinfo=learning.TZ)
SINGLE = "\u5355\u9009"
MULTIPLE = "\u591a\u9009"
INTERVIEW = "\u9762\u8bd5"
MASTERED = "\u5df2\u638c\u63e1"
PARTIAL = "\u90e8\u5206\u638c\u63e1"
REVIEW = "\u9700\u590d\u4e60"


class FakeCloud:
    def __init__(self, enabled=True):
        self.enabled = enabled
        self.questions = []
        self.entities = {}
        self.saved_questions = {}
        self.files = {}
        self.calls = []
        self.fail_once = None
        self.fail_after_commit = None
        self.fail_entity_once = None
        self.on_upsert = None

    def _call(self, method, *args, write=False):
        self.calls.append((method, copy.deepcopy(args)))
        if write and not self.enabled:
            raise RuntimeError("FakeCloud writes are disabled")
        if self.fail_once == method:
            self.fail_once = None
            raise OSError("fake cloud unavailable")

    def fetch_questions(self):
        self._call("fetch_questions")
        return copy.deepcopy(self.questions)

    def ensure_schema(self):
        self._call("ensure_schema", write=True)

    def load_entities(self):
        self._call("load_entities")
        return [{"kind": kind, "key": key, "payload": copy.deepcopy(value)}
                for (kind, key), value in self.entities.items()]

    def save_question(self, question, operation_id):
        self._call("save_question", question, operation_id, write=True)
        result = {**copy.deepcopy(question), "record_id": question.get("record_id") or "recFake"}
        self.saved_questions[question["id"]] = result
        return copy.deepcopy(result)

    def upsert_entity(self, kind, key, payload, operation_id):
        self._call("upsert_entity", kind, key, payload, operation_id, write=True)
        if self.fail_entity_once == (kind, key):
            self.fail_entity_once = None
            raise OSError("fake entity unavailable")
        self.entities[kind, key] = copy.deepcopy(payload)
        if self.on_upsert:
            callback, self.on_upsert = self.on_upsert, None
            callback()
        if self.fail_after_commit == "upsert_entity":
            self.fail_after_commit = None
            raise OSError("fake acknowledgement lost")
        return {"record_id": "recEntity"}

    def upload_attachment(self, path, name):
        self._call("upload_attachment", str(path), name, write=True)
        content = Path(path).read_bytes()
        token = "fakeToken" + hashlib.sha256(content).hexdigest()
        self.files[token] = content
        return {"file_token": token, "size": len(content)}

    def download_attachment(self, token, max_bytes):
        self._call("download_attachment", token, max_bytes)
        content = self.files[token]
        if len(content) > max_bytes:
            raise ValueError("fake attachment too large")
        return content


class FakeSender:
    def __init__(self):
        self.calls = []
        self.delivered = {}
        self.fail_once = False
        self.on_send = None

    def __call__(self, scope, message, identity):
        self.calls.append((scope, message, identity))
        if self.fail_once:
            self.fail_once = False
            return False, "fake send failed"
        self.delivered[identity] = (scope, message)
        if self.on_send:
            callback, self.on_send = self.on_send, None
            callback()
        return True, ""


class LearningTests(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory(prefix="learning_core_")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.cloud = FakeCloud()
        self.sender = FakeSender()
        self.service = learning.LearningService(self.root, self.cloud, self.sender)
        for target, kwargs in (
            ("lan_bitable_template_portal.learning.now", {"return_value": CURRENT}),
            ("threading.Thread.start", {"side_effect": AssertionError("tests must not start workers")}),
            ("socket.create_connection", {"side_effect": AssertionError("tests must remain offline")}),
            ("socket.socket.connect", {"side_effect": AssertionError("tests must remain offline")}),
        ):
            guard = patch(target, **kwargs)
            guard.start()
            self.addCleanup(guard.stop)

    def restart(self):
        self.service = learning.LearningService(self.root, self.cloud, self.sender)

    def enable(self, **changes):
        settings = {**learning.DEFAULT_SETTINGS, "enabled": True,
                    "portal_url": "http://learning.test:8000", **changes}
        with self.service.transaction() as conn:
            self.service._put("settings", "main", settings, conn, False)

    def dispatch(self, action, payload=None, actor=None, query=None):
        return self.service.dispatch(action, payload or {}, ACTOR if actor is None else actor, query or {})

    def question(self, number, bank="written", kind=None, **changes):
        kind = kind or ("single" if bank == "written" else "interview")
        qid = f"{bank}:{number}"
        options = [] if kind == "interview" else [
            {"id": f"o{i}", "text": f"Choice {i}", "label": chr(65 + i)} for i in range(3)]
        q = {"id": qid, "record_id": f"rec{bank}{number}", "bank": bank,
             "stem": f"{bank} question {number}", "type": kind,
             "type_label": INTERVIEW if kind == "interview" else MULTIPLE if kind == "multiple" else SINGLE,
             "year": "2026", "options": options,
             "correct_option_ids": [] if kind == "interview" else ["o0", "o2"] if kind == "multiple" else ["o0"],
             "answer_text": "REFERENCE-SECRET" if kind == "interview" else "Choice 0\nChoice 2" if kind == "multiple" else "Choice 0",
             "analysis": "ANALYSIS-SECRET", "hint": "HINT-SECRET", "topic": "Safety",
             "specialty": "Electrical", "difficulty": "normal", "attachments": [],
             "status": "published", "problems": [], "version": "v1"}
        q.update(changes)
        q.setdefault("family_id", learning.digest(learning.canonical(q["stem"])))
        return q

    def seed(self, questions, dirty=False):
        with self.service.transaction() as conn:
            for q in questions:
                self.service._put("question", q["id"], q, conn, dirty)
                for attachment in q.get("attachments", []):
                    self.service._put("attachment", attachment["id"],
                                      {**attachment, "question_id": q["id"]}, conn, False)

    def seed_pool(self, written=0, duty=0, professional=0):
        self.seed(self.question(i, bank) for bank, size in
                  (("written", written), ("duty", duty), ("professional", professional)) for i in range(size))

    def fixture(self, kind="single", bank="written", per_scope=1, attachments=False):
        questions = [self.question(i, bank, kind) for i in range(6 * per_scope)]
        if attachments:
            for q in questions:
                q["attachments"] = [{"id": f"{q['id']}-{category}", "name": category + ".txt",
                                     "kind": category, "size": 12, "file_token": "TOKEN-SECRET-" + category,
                                     "local_file": "PRIVATE-LOCAL-" + category + ".txt"}
                                    for category in ("question", "material", "answer")]
            for category in ("question", "material", "answer"):
                self.cloud.files["TOKEN-SECRET-" + category] = b"test content"
        self.seed(questions)
        self.service.publish(DAY)
        paper = self.service._get("paper", f"{DAY}_A")
        self.assertTrue(paper["questions"])
        return paper, paper["questions"][0]

    def answer_payload(self, paper, question, **changes):
        record = self.service._get("record", paper["id"]) or {}
        version = record.get("version", 0)
        payload = {"id": paper["id"], "question_id": question["id"], "version": version,
                   "operation_id": f"op-{question['id']}-{version}", "option_ids": question["correct_option_ids"]}
        payload.update(changes)
        return payload

    def entry(self, paper, question):
        return self.service._get("record", paper["id"])["entries"][question["id"]]

    def assert_status(self, status, function, *args, **kwargs):
        with self.assertRaises(learning.LearningError) as caught:
            function(*args, **kwargs)
        self.assertEqual(caught.exception.status, status, str(caught.exception))

    def assert_no_secrets(self, value, answers=False):
        forbidden = {"file_token", "local_file", "local_path", "_revision", "_dirty"}
        if answers:
            forbidden |= {"correct_option_ids", "answer_text", "analysis", "hint"}
        if isinstance(value, dict):
            self.assertFalse(forbidden.intersection(value), f"private fields: {forbidden.intersection(value)}")
            for item in value.values():
                self.assert_no_secrets(item, answers)
        elif isinstance(value, list):
            for item in value:
                self.assert_no_secrets(item, answers)
        elif isinstance(value, str):
            for secret in ("TOKEN-SECRET", "PRIVATE-LOCAL", str(self.root)):
                self.assertNotIn(secret, value)

    def test_seven_day_global_family_dedup_across_six_buildings_and_restarts(self):
        self.seed_pool(written=336, duty=42, professional=42)
        duplicate = self.question(999, stem="written question 0")
        self.seed([duplicate, self.question(1000, status="draft"),
                   self.question(1001, problems=["invalid answer"])])
        self.seed([self.question(1002)], dirty=True)
        start = dt.date(2026, 8, 28)
        by_day = []
        for offset in range(8):
            date = start + dt.timedelta(days=offset)
            if offset in (3, 4, 7):
                self.restart()
            publication = self.service.publish(date)
            self.assertEqual(len(publication["paper_ids"]), 6)
            families = []
            for scope in learning.SCOPES:
                paper = self.service._get("paper", f"{date}_{scope}")
                self.assertEqual(paper["shortage"], {"written": 0, "duty": 0, "professional": 0})
                self.assertEqual([sum(q["bank"] == bank for q in paper["questions"])
                                  for bank in learning.BANKS], [8, 1, 1])
                families.extend(q["family_id"] for q in paper["questions"])
                self.assertFalse({"written:1000", "written:1001", "written:1002"}.intersection(
                    q["id"] for q in paper["questions"]))
            self.assertEqual(len(families), len(set(families)))
            for recent in by_day[-6:]:
                self.assertTrue(set(families).isdisjoint(recent))
            by_day.append(set(families))
            before = self.service._all("paper")
            self.service.publish(date)
            self.assertEqual(before, self.service._all("paper"))
        self.assertEqual(by_day[0], by_day[7])
        self.assertEqual(len(self.service._all("paper")), 48)
        self.assertEqual(len(self.service._all("publication")), 8)
        self.assertEqual(self.cloud.calls, [])
        self.assertEqual(self.sender.calls, [])

    def test_all_available_questions_rotate_before_reuse(self):
        sizes = {"written": 400, "duty": 50, "professional": 50}
        daily = {"written": 48, "duty": 6, "professional": 6}
        self.seed_pool(**sizes)
        seen = {bank: set() for bank in sizes}
        for offset in range(9):
            day = dt.date(2026, 8, 28) + dt.timedelta(days=offset)
            self.service.publish(day)
            assigned = [q for scope in learning.SCOPES
                        for q in self.service._get("paper", f"{day}_{scope}")["questions"]]
            for bank, size in sizes.items():
                ids = {q["id"] for q in assigned if q["bank"] == bank}
                self.assertEqual(len(ids), daily[bank])
                self.assertEqual(len(ids - seen[bank]), daily[bank] if offset < 8 else size - 8 * daily[bank])
                seen[bank].update(ids)
        self.assertEqual({bank: len(ids) for bank, ids in seen.items()}, sizes)

    def test_admin_delete_paper_hides_it_without_erasing_answers_or_reusing_questions(self):
        paper, question = self.fixture()
        self.dispatch("paper.answer", self.answer_payload(paper, question))
        record = copy.deepcopy(self.service._get("record", paper["id"]))
        self.assert_status(403, self.dispatch, "paper.delete", {"id": paper["id"]})
        deleted = self.dispatch("paper.delete", {"id": paper["id"]}, ADMIN)
        self.assertEqual(deleted, {"id": paper["id"], "deleted": True})
        self.assertEqual(self.dispatch("paper.delete", {"id": paper["id"]}, ADMIN), deleted)
        self.assertEqual(self.dispatch("papers.list", actor=ACTOR, query={"today": "1"})["total"], 0)
        self.assertEqual(self.dispatch("history", actor=ACTOR)["total"], 0)
        self.assert_status(404, self.dispatch, "paper.get", {"id": paper["id"]})
        self.assert_status(404, self.dispatch, "paper.answer", self.answer_payload(paper, question))
        self.assertEqual(self.service._get("record", paper["id"]), record)
        self.assertEqual(self.service.profile(ACTOR, {})["summary"]["assigned"], 0)
        self.assertEqual(self.service._get("notification", "publish:" + paper["id"])["status"], "cancelled")
        self.service.publish(DAY)
        self.assertTrue(self.service._get("paper", paper["id"])["deleted_at"])
        self.service.sync_pending(limit=40, force=True)
        with TemporaryDirectory(prefix="learning_deleted_restore_") as folder:
            restored = learning.LearningService(Path(folder), self.cloud, self.sender)
            restored.restore()
            self.assertEqual(restored.list_papers(ACTOR, {})["total"], 0)
            self.assertEqual(restored._get("record", paper["id"])["entries"], record["entries"])
        self.restart()
        self.assertEqual(self.dispatch("history", actor=ACTOR)["total"], 0)
        self.service.publish(DAY + dt.timedelta(days=1))
        self.assertFalse(self.service._get("paper", f"{DAY + dt.timedelta(days=1)}_A")["questions"])

    def test_today_query_switches_to_new_publication_and_keeps_history_separate(self):
        self.seed_pool(written=96, duty=12, professional=12)
        self.service.publish(DAY)
        following = DAY + dt.timedelta(days=1)
        self.service.publish(following)
        with patch("lan_bitable_template_portal.learning.now", return_value=CURRENT + dt.timedelta(days=1)):
            with patch.object(self.service, "_all", side_effect=AssertionError("today must not scan every historical paper")):
                current = self.dispatch("papers.list", actor=ACTOR, query={"today": "1", "date": DAY.isoformat()})
        self.assertEqual(current["today"], following.isoformat())
        self.assertEqual([item["date"] for item in current["items"]], [following.isoformat()])
        self.assertEqual(self.dispatch("history", actor=ACTOR)["total"], 2)

    def test_shortage_is_balanced_and_daily_start_building_rotates(self):
        start = dt.date(2026, 8, 28)
        extra = {scope: 0 for scope in learning.SCOPES}
        for offset in range(6):
            self.seed(self.question(offset * 20 + i, bank) for bank, size in
                      (("written", 8), ("duty", 2), ("professional", 2)) for i in range(size))
            day = start + dt.timedelta(days=offset)
            self.service.publish(day)
            papers = [self.service._get("paper", f"{day}_{scope}") for scope in learning.SCOPES]
            for bank, wanted in (("written", 8), ("duty", 1), ("professional", 1)):
                counts = [sum(q["bank"] == bank for q in p["questions"]) for p in papers]
                self.assertLessEqual(max(counts) - min(counts), 1)
                self.assertEqual(sum(counts), 8 if bank == "written" else 2)
                self.assertEqual([p["shortage"][bank] for p in papers], [wanted - n for n in counts])
            for p in papers:
                extra[p["scope"]] += sum(q["bank"] == "written" for q in p["questions"]) - 1
        self.assertEqual(set(extra.values()), {2})

    def test_empty_pool_publishes_explicit_shortages_once(self):
        self.service.publish(DAY)
        self.restart()
        self.service.publish(DAY)
        papers = self.service.list_papers(ADMIN, {"page_size": 100})["items"]
        self.assertEqual(len(papers), 6)
        for paper in papers:
            self.assertEqual(paper["questions"], [])
            self.assertEqual(paper["stats"]["shortage"], 10)
            self.assertEqual(paper["status"], "pending")

    def test_manual_publication_is_durable_silent_and_does_not_repeat(self):
        self.enable(reminder_enabled=True)
        self.seed_pool(written=96, duty=12, professional=12)
        result = self.dispatch("publish", actor=ADMIN)
        self.assertFalse(result["notify"])
        self.restart()
        self.assertEqual(self.service.sync_status()["status"], "publishing")
        with patch.object(self.service, "refresh"):
            self.service.tick(CURRENT.replace(hour=7))
        papers = copy.deepcopy(self.service._all("paper"))
        self.assertEqual(len(papers), 6)
        self.assertTrue(all(len(p["questions"]) == 10 and not p["notify"] for p in papers))
        self.assertEqual(self.sender.calls, [])
        self.assertEqual(self.service._all("notification"), [])
        self.assertTrue(self.dispatch("publish", actor=ADMIN)["already_published"])
        self.restart()
        with patch.object(self.service, "refresh"):
            self.service.tick(CURRENT.replace(hour=18))
        self.assertEqual(self.sender.calls, [])
        self.assertEqual(self.service._all("notification"), [])
        self.assertEqual(self.service._all("paper"), papers)
        tomorrow = CURRENT + dt.timedelta(days=1)
        with patch.object(self.service, "refresh"):
            self.service.tick(tomorrow)
        self.assertEqual(len(self.sender.calls), 6)
        self.assertTrue(self.service._get("publication", tomorrow.date().isoformat())["notify"])

    def test_manual_publication_works_without_enabling_automatic_schedule(self):
        self.seed_pool(written=48, duty=6, professional=6)
        self.assertFalse(self.service.settings()["enabled"])
        self.assertFalse(self.dispatch("publish", actor=ADMIN)["notify"])
        self.restart()
        self.service.tick(CURRENT)
        self.assertEqual(len(self.service._all("paper")), 6)
        self.assertFalse(self.service._get("publication", DAY.isoformat())["notify"])
        self.assertFalse(self.service.settings()["enabled"])
        self.assertFalse(self.cloud.enabled)
        self.assertEqual(self.sender.calls, [])
        with self.service.transaction() as conn:
            self.service._put("record", "manual-answer", {"version": 1}, conn)
        self.service.tick(CURRENT + dt.timedelta(days=1))
        self.assertIn(("record", "manual-answer"), self.cloud.entities)
        self.assertEqual(len(self.service._all("paper")), 6)
        self.assertEqual(self.sender.calls, [])

    def test_manual_publication_requested_during_scheduled_refresh_stays_silent(self):
        self.enable()
        self.seed_pool(written=48, duty=6, professional=6)
        with patch.object(self.service, "refresh", side_effect=lambda: self.dispatch("publish", actor=ADMIN)):
            self.service.tick(CURRENT)
        self.assertFalse(self.service._get("publication", DAY.isoformat())["notify"])
        self.assertEqual(self.sender.calls, [])

    def test_failed_manual_publication_refresh_retries_silently_after_restart(self):
        self.enable()
        self.seed_pool(written=48, duty=6, professional=6)
        self.dispatch("publish", actor=ADMIN)
        with patch.object(self.service, "refresh", side_effect=OSError("temporary connection failure")):
            with self.assertRaises(OSError):
                self.service.tick(CURRENT)
        self.assertIsNone(self.service._get("publication", DAY.isoformat()))
        self.restart()
        with patch.object(self.service, "refresh"):
            self.service.tick(CURRENT)
        self.assertEqual(len(self.service._all("paper")), 6)
        self.assertEqual(self.sender.calls, [])

    def test_cloud_restore_keeps_manually_published_papers_silent(self):
        self.enable(reminder_enabled=True)
        self.seed_pool(written=48, duty=6, professional=6)
        self.dispatch("publish", actor=ADMIN)
        with patch.object(self.service, "refresh"):
            self.service.tick(CURRENT)
        restored = learning.LearningService(self.root / "silent_restore", self.cloud, self.sender)
        restored.restore()
        restored.send_notifications(CURRENT.replace(hour=18))
        self.assertEqual(restored._all("notification"), [])
        self.assertEqual(self.sender.calls, [])

    def test_question_core_fields_are_sufficient_without_optional_metadata(self):
        self.enable()
        saved = self.service.save_question({"bank": "written", "type": "single", "status": "published",
            "stem": "Minimal valid choice", "options": [{"id": "a", "text": "One"}, {"id": "b", "text": "Two"}],
            "correct_option_ids": ["a"]}, ADMIN)
        self.assertEqual(saved["answer_text"], "One")
        self.assertEqual(saved["problems"], [])
        self.assertEqual(saved["year"], "")
        self.assertEqual(saved["analysis"], "")

    def test_h_account_cannot_read_or_mutate_other_building(self):
        paper, question = self.fixture(attachments=True)
        issue = self.dispatch("issue.create", {"paper_id": paper["id"], "question_id": question["id"], "description": "Check"})
        record = self.service._get("record", paper["id"])
        for action in ("paper.get", "paper.answer", "paper.reveal", "paper.notes"):
            with self.subTest(action=action):
                self.assert_status(403, self.dispatch, action,
                                   self.answer_payload(paper, question), H_ACTOR)
        for action in ("papers.list", "history", "profile", "review", "issues.list"):
            with self.subTest(action=action):
                self.assert_status(403, self.dispatch, action, actor=H_ACTOR, query={"scope": "A"})
        self.assert_status(403, self.service.bootstrap, "A", H_ACTOR)
        self.assert_status(403, self.dispatch, "issue.create", {
            "paper_id": paper["id"], "question_id": question["id"], "description": "Cross scope"}, H_ACTOR)
        self.assert_status(403, self.dispatch, "issue.update", {
            "id": issue["id"], "version": issue["version"], "remark": "Cross scope"}, H_ACTOR)
        self.assert_status(403, self.service.attachment, question["attachments"][0]["id"], H_ACTOR)
        own = self.dispatch("papers.list", actor=H_ACTOR)["items"]
        self.assertEqual({p["scope"] for p in own}, {"H"})
        self.assertEqual(self.service._get("record", paper["id"]), record)

    def test_admin_can_answer_selected_building_paper_and_contribute_to_its_progress(self):
        paper, question = self.fixture()
        self.assertTrue(self.service.bootstrap("A", ADMIN)["can_answer"])
        self.assertNotIn("answer", self.dispatch("paper.get", {"id": paper["id"]}, ADMIN)["questions"][0])
        hinted = self.dispatch("paper.reveal", {"id": paper["id"], "question_id": question["id"], "kind": "hint"}, ADMIN)
        self.assertEqual(hinted["questions"][0]["answer"], {"hint": question["hint"]})
        self.dispatch("paper.notes", {"id": paper["id"], "question_id": question["id"], "note": "Administrator note"}, ADMIN)
        answered = self.dispatch("paper.answer", self.answer_payload(paper, question), ADMIN)
        self.assertTrue(answered["questions"][0]["attempt"]["correct"])
        self.assertEqual(self.service._get("record", paper["id"])["scope"], "A")
        self.assertEqual(self.dispatch("profile", actor=ADMIN, query={"scope": "A"})["summary"]["answered"], 1)

    def test_admin_empty_scope_includes_all_buildings_even_with_bound_account_scope(self):
        self.fixture()
        bound_admin = {**ADMIN, "scope": "A"}
        papers = self.dispatch("papers.list", actor=bound_admin, query={"scope": ""})["items"]
        self.assertEqual({paper["scope"] for paper in papers}, set(learning.SCOPES))
        self.assertEqual(self.service.bootstrap("", bound_admin)["scope"], "")
        summary = self.dispatch("profile", actor=bound_admin, query={"scope": ""})
        self.assertEqual({building["scope"] for building in summary["buildings"]}, set(learning.SCOPES))

    def test_non_admin_cannot_manage_questions_settings_or_exports(self):
        paper, question = self.fixture()
        for action in ("questions.list", "question.get", "question.save", "question.status",
                       "question.copy", "settings.get", "settings.save", "publish", "refresh", "import"):
            with self.subTest(action=action):
                self.assert_status(403, self.dispatch, action, {"id": question["id"]})
        self.assert_status(403, self.service.export, "questions", {}, ACTOR)

    def test_public_paper_hides_answers_tokens_and_local_paths(self):
        paper, question = self.fixture(attachments=True)
        public = self.dispatch("paper.get", {"id": paper["id"]})
        self.assert_no_secrets(public, answers=True)
        visible = public["questions"][0]
        self.assertNotIn("answer", visible)
        self.assertEqual({a["kind"] for a in visible["attachments"]}, {"question", "material"})
        self.assertEqual(len(visible["options"]), 3)
        after = self.dispatch("paper.answer", self.answer_payload(paper, question))
        self.assert_no_secrets(after)
        self.assertEqual(after["questions"][0]["answer"]["correct_option_ids"], ["o0"])
        self.assertEqual(len(after["questions"][0]["answer"]["attachments"]), 1)
        self.assert_no_secrets(self.dispatch("paper.get", {"id": paper["id"]}, ADMIN))

    def test_answer_attachment_requires_answer_or_full_reveal_not_hint(self):
        paper, question = self.fixture(attachments=True)
        answer_id = question["attachments"][2]["id"]
        self.assert_status(403, self.service.attachment, answer_id, ACTOR)
        self.dispatch("paper.reveal", {"id": paper["id"], "question_id": question["id"], "kind": "hint"})
        self.assert_status(403, self.service.attachment, answer_id, ACTOR)
        self.dispatch("paper.reveal", {"id": paper["id"], "question_id": question["id"], "kind": "answer"})
        path, name, mime = self.service.attachment(answer_id, ACTOR)
        self.assertTrue(path.is_relative_to(self.root / "files"))
        self.assertEqual(path.read_bytes(), b"test content")
        self.assertEqual((name, mime), ("answer.txt", "text/plain"))
        self.assert_status(403, self.service.attachment, answer_id, H_ACTOR)

    def test_mastered_marker_is_no_longer_editable_or_returned(self):
        paper, question = self.fixture()
        with self.service.transaction() as conn:
            self.service._put("record", paper["id"], {"id": paper["id"], "scope": "A", "date": paper["date"], "entries": {question["id"]: {"mastered": True}}, "version": 1}, conn, False)
        self.assertNotIn("mastered", self.dispatch("paper.get", {"id": paper["id"]})["questions"][0])
        self.assertNotIn("review_mastered", self.dispatch("profile")["summary"])
        self.assert_status(400, self.dispatch, "paper.notes", {"id": paper["id"], "question_id": question["id"], "mastered": False})

    def test_first_answer_is_idempotent_even_after_restart_and_version_change(self):
        paper, question = self.fixture()
        payload = self.answer_payload(paper, question)
        first = self.dispatch("paper.answer", payload)
        snapshot = self.service._get("record", paper["id"])
        self.assertEqual(self.dispatch("paper.answer", payload), first)
        self.assertEqual(self.service._get("record", paper["id"]), snapshot)
        self.restart()
        self.assertEqual(self.dispatch("paper.answer", payload), first)
        self.dispatch("paper.notes", {"id": paper["id"], "question_id": question["id"], "note": "Remember"})
        latest = self.service._get("record", paper["id"])
        replay = self.dispatch("paper.answer", payload)
        self.assertEqual(replay["version"], latest["version"])
        self.assertEqual(self.service._get("record", paper["id"]), latest)

    def test_reused_operation_id_different_payload_stale_version_and_overwrite_conflict(self):
        paper, question = self.fixture(per_scope=2)
        payload = self.answer_payload(paper, question)
        self.dispatch("paper.answer", payload)
        before = self.service._get("record", paper["id"])
        for rejected in (
            {**payload, "option_ids": ["o1"]},
            {**payload, "operation_id": "fresh", "version": 0},
            {**payload, "operation_id": "fresh", "version": 1},
            self.answer_payload(paper, paper["questions"][1], version=0),
        ):
            with self.subTest(payload=rejected):
                self.assert_status(409, self.dispatch, "paper.answer", rejected)
                self.assertEqual(self.service._get("record", paper["id"]), before)

    def test_practice_retry_is_idempotent_and_never_overwrites_first_answer(self):
        paper, question = self.fixture()
        self.dispatch("paper.answer", self.answer_payload(paper, question, option_ids=["o1"]))
        first = copy.deepcopy(self.entry(paper, question)["attempt"])
        payload = self.answer_payload(paper, question, practice=True)
        self.dispatch("paper.answer", payload)
        record = self.service._get("record", paper["id"])
        self.dispatch("paper.answer", payload)
        self.assertEqual(self.service._get("record", paper["id"]), record)
        entry = self.entry(paper, question)
        self.assertEqual(entry["attempt"], first)
        self.assertEqual(len(entry["practice"]), 1)
        self.assertTrue(entry["practice"][0]["correct"])
        self.assertEqual(self.service.profile(ACTOR, {})["summary"]["correct"], 0)

    def test_hint_before_answer_sets_assisted_without_exposing_answer(self):
        paper, question = self.fixture()
        stale = self.answer_payload(paper, question)
        visible = self.dispatch("paper.reveal", {
            "id": paper["id"], "question_id": question["id"], "kind": "hint"})["questions"][0]
        self.assertEqual(visible["answer"], {"hint": "HINT-SECRET"})
        self.assertTrue(visible["hinted"])
        self.assert_status(409, self.dispatch, "paper.answer", stale)
        self.dispatch("paper.answer", self.answer_payload(paper, question))
        self.assertTrue(self.entry(paper, question)["attempt"]["assisted"])
        summary = self.service.profile(ACTOR, {})["summary"]
        self.assertEqual(summary["hinted"], 1)
        self.assertEqual(summary["independent_answered"], 0)

    def test_hint_or_answer_reveal_after_answer_does_not_rewrite_assisted(self):
        paper, question = self.fixture()
        self.dispatch("paper.answer", self.answer_payload(paper, question))
        first = copy.deepcopy(self.entry(paper, question)["attempt"])
        for kind in ("hint", "analysis", "answer"):
            self.dispatch("paper.reveal", {"id": paper["id"], "question_id": question["id"], "kind": kind})
            self.assertEqual(self.entry(paper, question)["attempt"], first)
            self.assertFalse(self.entry(paper, question).get("assisted", False))
        self.assertEqual(self.service.profile(ACTOR, {})["summary"]["independent_correct"], 1)

    def test_multiple_choice_uses_exact_set_and_reports_missing_and_extra_options(self):
        paper, _ = self.fixture(kind="multiple", per_scope=4)
        cases = [(["o2", "o0"], True, [], []), (["o0"], False, ["o2"], []),
                 (["o0", "o1", "o2"], False, [], ["o1"]), (["o1"], False, ["o0", "o2"], ["o1"])]
        for question, (selected, correct, missed, wrong) in zip(paper["questions"], cases):
            with self.subTest(selected=selected):
                self.dispatch("paper.answer", self.answer_payload(paper, question, option_ids=selected))
                attempt = self.entry(paper, question)["attempt"]
                self.assertEqual((attempt["correct"], attempt["missed"], attempt["wrong"]),
                                 (correct, missed, wrong))
        self.assertEqual(self.service.profile(ACTOR, {})["summary"]["accuracy"], 25.0)

    def test_invalid_answers_are_rejected_without_persisting_partial_records(self):
        paper, question = self.fixture()
        for selected in (None, "o0", [], ["missing"], ["o0", "o1"], [1], [{}]):
            with self.subTest(selected=selected):
                self.assert_status(400, self.dispatch, "paper.answer",
                                   self.answer_payload(paper, question, option_ids=selected))
                self.assertIsNone(self.service._get("record", paper["id"]))
        self.assert_status(400, self.dispatch, "paper.answer",
                           self.answer_payload(paper, question, operation_id=""))

    def test_interview_requires_answer_and_self_rating_and_does_not_enter_choice_accuracy(self):
        paper, question = self.fixture(kind="interview", bank="duty")
        for body, rating in (("", REVIEW), ("   ", PARTIAL), ("Response", None), ("Response", "unknown"), ("Response", MASTERED)):
            with self.subTest(body=body, rating=rating):
                self.assert_status(400, self.dispatch, "paper.answer",
                                   self.answer_payload(paper, question, answer_text=body, self_rating=rating))
                self.assertIsNone(self.service._get("record", paper["id"]))
        for index, rating in enumerate((PARTIAL, REVIEW)):
            self.dispatch("paper.answer", self.answer_payload(
                paper, question, answer_text="  My explanation  ", self_rating=rating, practice=index > 0))
        entry = self.entry(paper, question)
        attempts = [entry["attempt"], *entry["practice"]]
        self.assertEqual([a["self_rating"] for a in attempts], [PARTIAL, REVIEW])
        self.assertTrue(all(a["answer_text"] == "My explanation" and "correct" not in a for a in attempts))
        summary = self.service.profile(ACTOR, {})["summary"]
        self.assertEqual((summary["interview_total"], summary["choice_answered"], summary["accuracy"]), (1, 0, None))
        self.assertEqual(self.service.review(ACTOR, {})["total"], 1)

    def test_duplicate_issues_merge_and_admin_reply_is_versioned_and_notified(self):
        paper, question = self.fixture()
        payload = {"paper_id": paper["id"], "question_id": question["id"], "description": "Please verify"}
        first = self.dispatch("issue.create", payload)
        second = self.dispatch("issue.create", {**payload, "description": "Additional evidence"})
        self.assertEqual(first["id"], second["id"])
        self.assertEqual((second["version"], len(second["comments"])), (2, 2))
        self.assertEqual(len(self.service._all("issue")), 1)
        self.assert_status(409, self.dispatch, "issue.update", {
            "id": first["id"], "version": 1, "remark": "Stale"}, ADMIN)
        self.assert_status(403, self.dispatch, "issue.update", {
            "id": first["id"], "version": 2, "status": "resolved", "remark": "Self resolve"})
        reply = self.dispatch("issue.update", {"id": first["id"], "version": 2,
                              "status": "resolved", "remark": "Corrected"}, ADMIN)
        self.assertEqual(reply["status"], "resolved")
        self.assertTrue(reply["comments"][-1]["is_admin"])
        self.assertEqual(reply["comments"][-1]["text"], "Corrected")
        notification = self.service._get("notification", f"issue:{first['id']}:3")
        self.assertEqual((notification["scope"], notification["status"]), ("A", "pending"))
        self.assertEqual(self.service.list_issues(H_ACTOR, {})["total"], 0)
        self.assertNotEqual(self.dispatch("issue.create", payload)["id"], first["id"])

    def test_issue_creation_does_not_leak_unrevealed_answer_or_attachment_secrets(self):
        paper, question = self.fixture(attachments=True)
        issue = self.dispatch("issue.create", {"paper_id": paper["id"], "question_id": question["id"],
                                               "description": "Question wording is unclear"})
        self.assert_no_secrets(issue, answers=True)
        self.assert_no_secrets(self.dispatch("issues.list"), answers=True)
        admin_view = self.dispatch("issues.list", actor=ADMIN)["items"][0]
        self.assertEqual(admin_view["question"]["correct_option_ids"], question["correct_option_ids"])
        self.assert_no_secrets(admin_view)
        self.assertIsNone(self.service._get("record", paper["id"]))

    def test_answer_correction_regrades_first_and_practice_and_preserves_audit(self):
        self.enable()
        paper, question = self.fixture()
        self.dispatch("paper.answer", self.answer_payload(paper, question, option_ids=["o1"]))
        self.dispatch("paper.answer", self.answer_payload(paper, question, practice=True))
        before = copy.deepcopy(self.entry(paper, question))
        saved = self.dispatch("question.save", {"id": question["id"], "version": question["version"],
                              "correct_option_ids": ["o1"], "reason": "Verified reference"}, ADMIN)
        entry = self.entry(paper, question)
        self.assertTrue(entry["attempt"]["correct"])
        self.assertFalse(entry["practice"][0]["correct"])
        self.assertFalse(entry["attempt"]["corrections"][0]["correct"])
        self.assertTrue(entry["practice"][0]["corrections"][0]["correct"])
        for field in ("operation_id", "submitted_at", "option_ids", "assisted"):
            self.assertEqual(entry["attempt"][field], before["attempt"][field])
        corrected = self.service._get("paper", paper["id"])["questions"][0]
        self.assertEqual(corrected["version"], saved["version"])
        self.assertEqual(corrected["correction_history"][0]["original"]["correct_option_ids"], ["o0"])
        self.assertEqual(len(self.service._all("audit")), 1)
        self.assertEqual(self.service.profile(ACTOR, {})["summary"]["correct"], 1)
        self.assertIsNotNone(self.service._get("notification", f"correction:{paper['id']}:{saved['version']}"))

    def test_structure_changes_invalidate_snapshots_and_exclude_them_from_profile(self):
        self.enable()
        paper, _ = self.fixture(per_scope=3)
        for question in paper["questions"]:
            self.dispatch("paper.answer", self.answer_payload(paper, question))
        changes = [{"stem": "Different wording"}, {"type": "multiple"},
                   {"options": [{"id": "o0", "text": "Replaced option"}, {"id": "o1", "text": "Choice 1"}]}]
        for question, change in zip(paper["questions"], changes):
            self.dispatch("question.save", {"id": question["id"], "version": question["version"],
                          "reason": "Structure correction", **change}, ADMIN)
            self.assert_status(400, self.dispatch, "paper.answer",
                               self.answer_payload(paper, question, practice=True))
        corrected = self.service._get("paper", paper["id"])
        for original, invalid in zip(paper["questions"], corrected["questions"]):
            self.assertTrue(invalid["invalid"])
            self.assertEqual((invalid["stem"], invalid["options"]), (original["stem"], original["options"]))
            self.assertTrue(self.entry(paper, original)["attempt"]["correct"])
        summary = self.service.profile(ACTOR, {})["summary"]
        self.assertEqual((summary["assigned"], summary["answered"], summary["choice_answered"]), (0, 0, 0))
        self.assertEqual(self.service.review(ACTOR, {})["total"], 0)

    def test_metadata_edit_does_not_prevent_later_answer_regrading(self):
        self.enable()
        paper, question = self.fixture()
        self.dispatch("paper.answer", self.answer_payload(paper, question, option_ids=["o1"]))
        revised = self.dispatch("question.save", {"id": question["id"], "version": question["version"],
                                "analysis": "Expanded explanation"}, ADMIN)
        self.dispatch("question.save", {"id": question["id"], "version": revised["version"],
                      "correct_option_ids": ["o1"], "reason": "Actual correction"}, ADMIN)
        self.assertTrue(self.entry(paper, question)["attempt"]["correct"])
        self.assertEqual(self.service._get("paper", paper["id"])["questions"][0]["correct_option_ids"], ["o1"])

    def test_metadata_edit_does_not_prevent_later_structure_invalidation(self):
        self.enable()
        paper, question = self.fixture()
        revised = self.dispatch("question.save", {"id": question["id"], "version": question["version"],
                                "topic": "Updated classification"}, ADMIN)
        self.dispatch("question.save", {"id": question["id"], "version": revised["version"],
                      "stem": "Meaningfully different question"}, ADMIN)
        self.assertTrue(self.service._get("paper", paper["id"])["questions"][0].get("invalid"))

    def test_stale_question_save_and_status_are_conflicts_without_mutation(self):
        self.enable()
        _, question = self.fixture()
        self.dispatch("question.save", {"id": question["id"], "version": question["version"],
                      "analysis": "New revision"}, ADMIN)
        before = self.service._get("question", question["id"])
        for action, change in (("question.save", {"stem": "Stale wording"}),
                               ("question.status", {"status": "deleted"})):
            with self.subTest(action=action):
                self.assert_status(409, self.dispatch, action,
                                   {"id": question["id"], "version": question["version"], **change}, ADMIN)
                self.assertEqual(self.service._get("question", question["id"]), before)

    def test_batch_question_status_checks_each_expected_version(self):
        self.enable()
        paper, _ = self.fixture(per_scope=2)
        first, second = paper["questions"]
        self.dispatch("question.save", {"id": first["id"], "version": first["version"],
                      "analysis": "New revision"}, ADMIN)
        before = self.service._get("question", first["id"])
        result = self.dispatch("question.status", {
            "ids": [first["id"], second["id"]], "status": "disabled",
            "versions": {first["id"]: first["version"], second["id"]: second["version"]}}, ADMIN)
        self.assertIn("error", result["items"][0])
        self.assertEqual(result["items"][1]["status"], "disabled")
        self.assertEqual(self.service._get("question", first["id"]), before)

    def test_interview_answer_correction_requires_review_without_overwriting_first_answer(self):
        self.enable()
        paper, question = self.fixture(kind="interview", bank="duty")
        self.dispatch("paper.answer", self.answer_payload(paper, question, answer_text="My response", self_rating=REVIEW))
        first = copy.deepcopy(self.entry(paper, question)["attempt"])
        self.dispatch("question.save", {"id": question["id"], "version": question["version"],
                      "answer_text": "Corrected reference", "reason": "Reference changed"}, ADMIN)
        self.assertTrue(self.entry(paper, question)["needs_review"])
        self.assertEqual(self.dispatch("paper.get", {"id": paper["id"]})["status"], "pending")
        self.dispatch("paper.answer", self.answer_payload(paper, question, answer_text="Reconsidered response", self_rating=PARTIAL))
        entry = self.entry(paper, question)
        self.assertEqual(entry["attempt"], first)
        self.assertFalse(entry["needs_review"])
        self.assertEqual(entry["practice"][0]["self_rating"], PARTIAL)

    def test_deletion_preserves_historical_paper_answer_and_attachments(self):
        self.enable()
        paper, question = self.fixture(attachments=True)
        self.dispatch("paper.answer", self.answer_payload(paper, question))
        before_paper = self.service._get("paper", paper["id"])
        before_record = self.service._get("record", paper["id"])
        self.dispatch("question.status", {"id": question["id"], "version": question["version"], "status": "deleted"}, ADMIN)
        self.restart()
        self.assertEqual(self.service._get("paper", paper["id"]), before_paper)
        self.assertEqual(self.service._get("record", paper["id"]), before_record)
        self.assertNotIn(question["id"], [q["id"] for q in self.service.questions(ADMIN, {})["items"]])
        self.service.publish(DAY + dt.timedelta(days=7))
        for next_paper in self.service._all("paper"):
            if next_paper["date"] != DAY.isoformat():
                self.assertNotIn(question["id"], [q["id"] for q in next_paper["questions"]])
        path, _, _ = self.service.attachment(question["attachments"][2]["id"], ACTOR)
        self.assertEqual(path.read_bytes(), b"test content")

    def test_deleting_attachment_keeps_bytes_accessible_to_issued_papers(self):
        self.enable()
        paper, question = self.fixture(attachments=True)
        attachment_id = question["attachments"][0]["id"]
        before = self.service._get("paper", paper["id"])
        self.service.delete_attachment(attachment_id, ADMIN, question["version"])
        self.assertNotIn(attachment_id, [a["id"] for a in self.service._get("question", question["id"])["attachments"]])
        self.assertEqual(self.service._get("paper", paper["id"]), before)
        self.restart()
        path, _, _ = self.service.attachment(attachment_id, ACTOR)
        self.assertEqual(path.read_bytes(), b"test content")
        self.assert_status(403, self.service.attachment, attachment_id, H_ACTOR)

    def test_uploaded_attachment_token_survives_entity_failure_without_uploading_twice(self):
        self.enable()
        question = self.question(1)
        self.seed([question])
        added = self.service.add_attachments([("reference.txt", b"small reference")],
                                            {"question_id": question["id"], "kind": "material",
                                             "version": question["version"]}, ADMIN)
        identity = added["items"][0]["id"]
        self.cloud.fail_entity_once = ("attachment", identity)
        self.assertEqual(self.service.sync_pending(), {"pending_errors": 1})
        uploaded = self.service._get("attachment", identity)
        self.assertTrue(uploaded["file_token"])
        self.assertTrue(uploaded["_dirty"])
        self.restart()
        self.assertEqual(self.service.sync_pending(), {"pending_errors": 0})
        self.assertEqual(sum(name == "upload_attachment" for name, _ in self.cloud.calls), 1)
        stored = self.service._get("attachment", identity)
        self.assertEqual(stored["file_token"], uploaded["file_token"])
        self.assertFalse(stored["_dirty"])
        for name, args in self.cloud.calls:
            if name == "upsert_entity" and args[0] == "attachment":
                self.assertNotIn("local_file", args[2])

    def test_question_sync_failure_is_durable_and_retries_same_operation_after_restart(self):
        q = self.question(1, record_id=None)
        other = self.question(2)
        self.seed([q, other], dirty=True)
        self.cloud.fail_once = "save_question"
        before = self.service._get("question", q["id"])
        result = self.service.sync_pending()
        self.assertEqual(result, {"pending_errors": 1})
        self.assertEqual(self.service._get("question", q["id"]), before)
        self.assertFalse(self.service._get("question", other["id"])["_dirty"])
        self.assertIn("fake cloud unavailable", self.service.sync_status()["error"])
        self.assertEqual(self.service.sync_status()["pending"], 1)
        self.restart()
        self.assertEqual(self.service.sync_pending(force=True), {"pending_errors": 0})
        saved = self.service._get("question", q["id"])
        self.assertFalse(saved["_dirty"])
        self.assertEqual(saved["record_id"], "recFake")
        operations = [args[1] for name, args in self.cloud.calls
                      if name == "save_question" and args[0]["id"] == q["id"]]
        self.assertEqual(len(operations), 2)
        self.assertEqual(operations[0], operations[1])
        self.assertEqual(self.service.sync_status()["pending"], 0)
        self.assertEqual(self.service.sync_status()["error"], "")

    def test_lost_sync_acknowledgement_retries_without_duplicate_entity(self):
        record = {"id": "record-1", "scope": "A", "entries": {}, "version": 1}
        with self.service.transaction() as conn:
            self.service._put("record", record["id"], record, conn)
        self.cloud.fail_after_commit = "upsert_entity"
        self.assertEqual(self.service.sync_pending(), {"pending_errors": 1})
        self.assertIn("fake acknowledgement lost", self.service.sync_status()["error"])
        self.assertTrue(self.service._get("record", record["id"])["_dirty"])
        self.restart()
        self.service.sync_pending(force=True)
        calls = [args for name, args in self.cloud.calls if name == "upsert_entity"]
        self.assertEqual(calls[0], calls[1])
        self.assertEqual(len(self.cloud.entities), 1)
        self.assertEqual(self.cloud.entities["record", record["id"]], record)
        self.assertFalse(self.service._get("record", record["id"])["_dirty"])

    def test_concurrent_local_update_is_not_marked_clean_by_older_sync(self):
        with self.service.transaction() as conn:
            self.service._put("record", "r", {"version": 1}, conn)

        def newer_update():
            with self.service.transaction() as conn:
                self.service._put("record", "r", {"version": 2}, conn)

        self.cloud.on_upsert = newer_update
        self.service.sync_pending()
        self.assertEqual(self.service._get("record", "r")["version"], 2)
        self.assertTrue(self.service._get("record", "r")["_dirty"])
        self.service.sync_pending()
        self.assertEqual(self.cloud.entities["record", "r"]["version"], 2)
        self.assertFalse(self.service._get("record", "r")["_dirty"])

    def test_publish_notification_waits_for_sync_and_is_not_resent_after_restart(self):
        self.enable()
        self.fixture()
        self.cloud.fail_once = "upsert_entity"
        self.assertEqual(self.service.sync_pending(), {"pending_errors": 1})
        self.service.send_notifications(CURRENT)
        self.assertEqual(self.sender.calls, [])
        self.restart()
        self.service.sync_pending(force=True)
        self.service.send_notifications(CURRENT)
        self.assertEqual(len(self.sender.calls), 6)
        self.assertEqual({call[0] for call in self.sender.calls}, set(learning.SCOPES))
        self.restart()
        self.service.send_notifications(CURRENT)
        self.assertEqual(len(self.sender.calls), 6)

    def test_learning_links_use_runtime_address_and_preserve_port(self):
        self.enable(portal_url="http://old-host.test:1111")
        resolver = Mock(return_value="https://portal.test:8787/")
        self.service._portal_url = resolver
        self.fixture()
        self.service.sync_pending()
        self.service.send_notifications(CURRENT)
        self.assertEqual(len(self.sender.calls), 6)
        resolver.assert_called_once()
        for scope, message, _ in self.sender.calls:
            self.assertIn(f"https://portal.test:8787/learning?scope={scope}", message)
            self.assertNotIn("old-host", message)
        resolver.return_value = "http://192.168.1.5:18766"
        self.assertEqual(self.service.bootstrap("A", ADMIN)["settings"]["portal_url"], resolver.return_value)
        saved = self.dispatch("settings.save", {"portal_url": "http://wrong.test:1234", "publish_time": "09:00"}, ADMIN)
        self.assertEqual(saved["portal_url"], resolver.return_value)
        self.assertNotIn("portal_url", self.service._get("settings", "main"))
        self.assertEqual(self.dispatch("settings.get", actor=ADMIN)["portal_url"], resolver.return_value)

    def test_unreachable_loopback_or_bad_runtime_links_wait_without_losing_notifications(self):
        self.enable()
        resolver = Mock()
        self.service._portal_url = resolver
        self.fixture()
        self.service.sync_pending()
        for address in ("", "http://127.0.0.1:18766", "http://127.0.2.1:18766", "http://[::1]:18766",
                        "http://0.0.0.0:18766", "http://localhost.:18766", "http://host.test:bad",
                        "https://user:secret@host.test", "https://host.test/unrelated", "javascript:alert(1)"):
            with self.subTest(address=address):
                resolver.return_value = address
                self.service.send_notifications(CURRENT)
                self.assertEqual(self.sender.calls, [])
                self.assertEqual(len([n for n in self.service._all("notification") if n["status"] == "pending"]), 6)
        resolver.return_value = "http://192.168.2.10:18766"
        self.service.send_notifications(CURRENT)
        self.assertEqual(len(self.sender.calls), 6)

    def test_notification_failure_persists_retry_deadline_and_identity(self):
        self.enable()
        self.fixture()
        self.service.sync_pending()
        self.sender.fail_once = True
        with patch.object(learning.time, "time", return_value=1000):
            self.service.send_notifications(CURRENT)
        pending = [n for n in self.service._all("notification") if n["status"] == "pending"]
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["retry_at"], 1300)
        self.restart()
        with patch.object(learning.time, "time", return_value=1299):
            self.service.send_notifications(CURRENT)
        self.assertEqual(len(self.sender.calls), 6)
        with patch.object(learning.time, "time", return_value=1300):
            self.service.send_notifications(CURRENT)
        self.assertEqual(len(self.sender.calls), 7)
        self.assertEqual(self.sender.calls[0][2], self.sender.calls[-1][2])
        self.assertEqual(len(self.sender.delivered), 6)

    def test_restore_keeps_local_dirty_data_and_preserves_publication_dedup(self):
        paper, question = self.fixture()
        self.service.sync_pending()
        self.dispatch("paper.answer", self.answer_payload(paper, question))
        record = self.service._get("record", paper["id"])
        self.cloud.entities["record", paper["id"]] = {"id": paper["id"], "entries": {}, "version": 0}
        self.restart()
        self.service.restore()
        self.assertEqual(self.service._get("record", paper["id"]), record)
        self.service = learning.LearningService(self.root / "restored", self.cloud, self.sender)
        self.service.restore()
        self.seed(self.question(i) for i in range(6))
        before = self.service._all("paper")
        self.service.publish(DAY)
        self.assertEqual(self.service._all("paper"), before)
        self.service.publish(DAY + dt.timedelta(days=1))
        next_day = self.service._get("paper", f"{DAY + dt.timedelta(days=1)}_A")
        self.assertEqual(next_day["questions"], [])

    def test_partial_restore_keeps_existing_papers_and_answers_and_fills_only_missing_buildings(self):
        self.seed_pool(written=48, duty=6, professional=6)
        self.service.publish(DAY)
        original = {scope: self.service._get("paper", f"{DAY}_{scope}") for scope in ("A", "H")}
        paper = original["A"]
        self.dispatch("paper.answer", self.answer_payload(paper, paper["questions"][0]))
        record = self.service._get("record", paper["id"])
        self.cloud.entities = {("paper", p["id"]): p for p in original.values()}
        self.cloud.entities["record", paper["id"]] = record
        self.service = learning.LearningService(self.root / "new_machine", self.cloud, self.sender)
        self.service.restore()
        self.assertIsNone(self.service._get("publication", DAY.isoformat()))
        self.seed_pool(written=48, duty=6, professional=6)
        restored = {scope: self.service._get("paper", p["id"]) for scope, p in original.items()}
        restored_record = self.service._get("record", paper["id"])
        publication = self.service.publish(DAY)
        self.assertEqual(set(publication["paper_ids"]), {f"{DAY}_{scope}" for scope in learning.SCOPES})
        for scope, p in restored.items():
            self.assertEqual(self.service._get("paper", p["id"]), p)
            self.assertEqual(p["questions"], original[scope]["questions"])
            self.assertIsNone(self.service._get("notification", "publish:" + p["id"]))
        self.assertEqual(self.service._get("record", paper["id"]), restored_record)
        self.assertEqual(restored_record["entries"], record["entries"])
        papers = self.service._all("paper")
        self.assertEqual(len(papers), 6)
        families = [q["family_id"] for p in papers for q in p["questions"]]
        self.assertEqual(len(families), 60)
        self.assertEqual(len(set(families)), 60)
        self.assertEqual(len(self.service._all("notification")), 4)
        self.assertEqual(self.sender.calls, [])

    def test_publication_sync_waits_for_every_paper_and_retries_after_restart(self):
        self.fixture()
        failed_id = f"{DAY}_H"
        self.cloud.fail_entity_once = ("paper", failed_id)
        self.assertEqual(self.service.sync_pending(), {"pending_errors": 1})
        self.assertEqual(sum(kind == "paper" for kind, _ in self.cloud.entities), 5)
        self.assertNotIn(("publication", DAY.isoformat()), self.cloud.entities)
        self.assertTrue(self.service._get("publication", DAY.isoformat())["_dirty"])
        self.assertTrue(self.service._get("paper", failed_id)["_dirty"])
        self.assertFalse(any(name == "upsert_entity" and args[0] == "publication"
                             for name, args in self.cloud.calls))
        self.restart()
        self.assertEqual(self.service.sync_pending(force=True), {"pending_errors": 0})
        self.assertEqual(sum(kind == "paper" for kind, _ in self.cloud.entities), 6)
        self.assertIn(("publication", DAY.isoformat()), self.cloud.entities)
        self.assertFalse(self.service._get("publication", DAY.isoformat())["_dirty"])
        uploads = [args for name, args in self.cloud.calls if name == "upsert_entity"]
        publication_index = next(i for i, args in enumerate(uploads) if args[0] == "publication")
        paper_indices = [i for i, args in enumerate(uploads) if args[0] == "paper"]
        self.assertGreater(publication_index, max(paper_indices))

    @staticmethod
    def raw_question():
        return {"bank": "written", "record_id": "recSource", "fields": {
            "\u9898\u76ee": "Source question", "\u9898\u578b": SINGLE,
            "\u9009\u9879": "A. Alpha\nB. Beta", "\u7b54\u6848": "A"}}

    def test_readonly_refresh_fetches_and_restores_without_schema_writes_or_messages(self):
        self.cloud.enabled = False
        self.cloud.questions = [self.raw_question()]
        self.service.request_refresh()
        self.service.tick(CURRENT)
        self.assertEqual([name for name, _ in self.cloud.calls], ["fetch_questions", "load_entities"])
        self.assertEqual(self.service.sync_status()["status"], "ready")
        self.assertEqual(self.service.sync_status()["pending"], 0)
        self.assertFalse(self.service._schema_ready)
        self.assertIsNone(self.service._thread)
        self.assertEqual(self.service._all("paper"), [])
        self.assertEqual(self.sender.calls, [])
        self.assertEqual(len(self.service._all("question")), 1)

    def test_enabling_after_readonly_refresh_enables_cloud_and_initializes_schema_once(self):
        self.cloud.enabled = False
        self.cloud.questions = [self.raw_question()]
        self.service.request_refresh()
        self.service.tick(CURRENT)
        self.dispatch("settings.save", {"enabled": True}, ADMIN)
        before_publish = CURRENT.replace(hour=7)
        self.service.tick(before_publish)
        self.assertTrue(self.cloud.enabled)
        self.assertTrue(self.service._schema_ready)
        self.service.tick(before_publish)
        names = [name for name, _ in self.cloud.calls]
        self.assertEqual(names.count("ensure_schema"), 1)
        self.assertEqual(names.count("load_entities"), 1)
        self.assertLess(names.index("ensure_schema"), names.index("upsert_entity"))
        self.assertEqual(self.sender.calls, [])

    def test_lazily_created_cloud_receives_enabled_setting_without_importing_real_config(self):
        self.enable()
        self.service._cloud = None
        cloud = FakeCloud(enabled=False)

        def create_cloud(**kwargs):
            cloud.enabled = kwargs.get("enabled", False)
            return cloud

        factory = Mock(side_effect=create_cloud)
        with patch.dict(sys.modules, {"lan_bitable_template_portal.learning_cloud":
                                    SimpleNamespace(LearningCloud=factory)}):
            self.service.tick(CURRENT.replace(hour=7))
        self.assertTrue(cloud.enabled)
        self.assertTrue(self.service._schema_ready)
        factory.assert_called_once()

    def test_refresh_preserves_dirty_question_and_tombstones_missing_clean_source(self):
        self.cloud.questions = [self.raw_question()]
        self.service.refresh()
        source = self.service._all("question")[0]
        source["stem"] = "Unsynced local edit"
        self.seed([source], dirty=True)
        self.cloud.questions = []
        self.service.refresh()
        self.assertEqual(self.service._get("question", source["id"])["stem"], "Unsynced local edit")
        self.assertEqual(self.service._get("question", source["id"])["status"], "published")
        self.seed([self.question(2)])
        self.service.refresh()
        self.assertEqual(self.service._get("question", "written:2")["status"], "deleted")

    def test_normalize_preserves_inline_labels_and_exact_multiline_metadata_options(self):
        raw = self.raw_question()
        raw["fields"]["\u9009\u9879"] = "A. Plan A. isolate first\nB. Continue operation"
        normalized = learning.normalize_question(raw)
        self.assertEqual([o["text"] for o in normalized["options"]],
                         ["Plan A. isolate first", "Continue operation"])
        self.assertEqual(normalized["correct_option_ids"], [normalized["options"][0]["id"]])
        self.assertEqual(normalized["problems"], [])
        options = [{"id": "stable-isolate", "label": "A", "text": "Plan A. isolate first\nB. verify isolation"},
                   {"id": "stable-continue", "label": "B", "text": "Continue operation\nRecord B. in the log"}]
        serialized = "\n".join(f"{o['label']}. {o['text']}" for o in options)
        raw["fields"]["\u5b66\u7ec3\u914d\u7f6e"] = json.dumps({
            "options": options, "correct_option_ids": ["stable-isolate"],
            "source_answer": "A", "answer_text": options[0]["text"], "version": "stable-version"})
        for source_text in (serialized, serialized.replace("\n", "\r\n")):
            with self.subTest(line_endings=repr(source_text)):
                raw["fields"]["\u9009\u9879"] = source_text
                normalized = learning.normalize_question(raw)
                self.assertEqual(normalized["options"], options)
                self.assertEqual(normalized["correct_option_ids"], ["stable-isolate"])
                self.assertEqual(normalized["answer_text"], options[0]["text"])
                self.assertEqual(normalized["version"], "stable-version")
                self.assertEqual(normalized["problems"], [])
        raw["fields"]["\u9009\u9879"] = "A. Replacement option\nB. Continue operation"
        changed = learning.normalize_question(raw)
        self.assertEqual(changed["options"][0]["text"], "Replacement option")
        self.assertNotEqual(changed["options"][0]["id"], "stable-isolate")

    def test_question_and_issue_attachments_enforce_expected_version_without_stale_bypass(self):
        self.enable()
        paper, question = self.fixture()
        issue = self.dispatch("issue.create", {"paper_id": paper["id"], "question_id": question["id"],
                                               "description": "Please check this reference"})
        for kind, entity, actor in (("question", question, ADMIN), ("issue", issue, ACTOR)):
            with self.subTest(owner=kind):
                query = {kind + "_id": entity["id"], "kind": "material", "version": str(entity["version"])}
                added = self.service.add_attachments([("first.txt", b"first reference")], query, actor)
                attachment_id, old_version = added["items"][0]["id"], added["version"]
                if kind == "question":
                    self.dispatch("question.save", {"id": entity["id"], "version": old_version,
                                  "analysis": "Newer editor saved this"}, ADMIN)
                else:
                    self.dispatch("issue.update", {"id": entity["id"], "version": old_version,
                                  "remark": "Newer evidence"}, ACTOR)
                before = self.service._get(kind, entity["id"])
                attachments_before = self.service._all("attachment")
                files_before = sorted((self.root / "files").iterdir())
                for expected in (old_version, None):
                    stale_query = {**query, "version": expected}
                    self.assert_status(409, self.service.add_attachments,
                                       [("stale.txt", b"stale editor bytes")], stale_query, actor)
                    self.assert_status(409, self.service.delete_attachment, attachment_id, actor, expected)
                    self.assertEqual(self.service._get(kind, entity["id"]), before)
                    self.assertEqual(self.service._all("attachment"), attachments_before)
                    self.assertEqual(sorted((self.root / "files").iterdir()), files_before)
                if kind == "question":
                    self.assert_status(409, self.dispatch, "question.save", {
                        "id": entity["id"], "version": old_version, "stem": "Stale editor overwrite"}, ADMIN)
                fresh = self.service.add_attachments([("second.txt", b"second reference")],
                    {**query, "version": str(before["version"])}, actor)
                self.assertNotEqual(fresh["version"], before["version"])
                self.assert_status(409, self.service.delete_attachment, attachment_id, actor, before["version"])
                deleted = self.service.delete_attachment(attachment_id, actor, fresh["version"])
                self.assertTrue(deleted["deleted"])
                self.assertNotIn(attachment_id, [a["id"] for a in self.service._get(kind, entity["id"])["attachments"]])

    def test_disabled_tick_syncs_only_disable_setting_and_fresh_restore_stays_disabled(self):
        self.dispatch("settings.save", {"enabled": True, "portal_url": "http://learning.test:8000"}, ADMIN)
        self.service.tick(CURRENT.replace(hour=7))
        self.assertTrue(self.cloud.entities["settings", "main"]["enabled"])
        with self.service.transaction() as conn:
            self.service._put("record", "unrelated", {"version": 1}, conn)
            self.service._put("notification", "unrelated-notification", {
                "id": "unrelated-notification", "kind": "issue", "date": DAY.isoformat(),
                "scope": "A", "status": "pending", "message": "Should remain pending"}, conn)
        self.dispatch("settings.save", {"enabled": False}, ADMIN)
        self.restart()
        self.assertTrue(self.service._get("local", "disable_sync")["required"])
        call_start = len(self.cloud.calls)
        self.service.tick(CURRENT)
        calls = self.cloud.calls[call_start:]
        self.assertEqual([(name, args[:2]) for name, args in calls],
                         [("ensure_schema", ()), ("upsert_entity", ("settings", "main"))])
        self.assertFalse(self.cloud.entities["settings", "main"]["enabled"])
        self.assertFalse(self.service._get("local", "disable_sync")["required"])
        self.assertFalse(self.service._get("settings", "main")["_dirty"])
        self.assertTrue(self.service._get("record", "unrelated")["_dirty"])
        self.assertTrue(self.service._get("notification", "unrelated-notification")["_dirty"])
        self.assertEqual(self.service._all("paper"), [])
        self.assertEqual(self.sender.calls, [])
        self.service = learning.LearningService(self.root / "disabled_restore", self.cloud, self.sender)
        self.service.restore()
        self.assertFalse(self.service.settings()["enabled"])
        call_start = len(self.cloud.calls)
        self.service.tick(CURRENT)
        self.assertEqual(self.cloud.calls[call_start:], [])
        self.assertEqual(self.sender.calls, [])

    def test_restore_rebuilds_imported_attachment_index_from_papers_after_source_removal(self):
        for i in range(6):
            token = f"sourceAttachment{i}"
            raw = self.raw_question()
            raw["record_id"] = f"recSource{i}"
            raw["fields"]["\u9898\u76ee"] = f"Source question {i}"
            raw["fields"]["\u5b66\u7ec3\u8d44\u6599"] = [
                {"name": "source.txt", "file_token": token, "size": len(b"historical source")}]
            self.cloud.questions.append(raw)
            self.cloud.files[token] = b"historical source"
        self.service.refresh()
        self.assertTrue(all(not a["_dirty"] for a in self.service._all("attachment")))
        self.service.publish(DAY)
        paper = self.service._get("paper", f"{DAY}_A")
        attachment_id = paper["questions"][0]["attachments"][0]["id"]
        self.assertEqual(self.service.sync_pending(), {"pending_errors": 0})
        self.assertFalse(any(kind == "attachment" for kind, _ in self.cloud.entities))
        self.cloud.questions = []
        self.service.refresh()
        self.assertEqual(self.service._get("question", paper["questions"][0]["id"])["status"], "deleted")
        self.service = learning.LearningService(self.root / "attachment_restore", self.cloud, self.sender)
        self.service.refresh()
        self.assertEqual(self.service._all("question"), [])
        self.assertIsNotNone(self.service._get("attachment", attachment_id))
        self.assert_status(403, self.service.attachment, attachment_id, H_ACTOR)
        path, name, mime = self.service.attachment(attachment_id, ACTOR)
        self.assertTrue(path.is_relative_to(self.root / "attachment_restore" / "files"))
        self.assertEqual((path.read_bytes(), name, mime), (b"historical source", "source.txt", "text/plain"))
        self.assert_no_secrets(self.dispatch("paper.get", {"id": paper["id"]}))

    def test_notifications_honor_disabled_reminders_and_stop_when_sender_disables_service(self):
        self.enable(reminder_enabled=True)
        self.fixture()
        self.service.sync_pending()
        self.service.send_notifications(CURRENT)
        self.assertEqual(len(self.sender.calls), 6)
        self.sender.fail_once = True
        evening = CURRENT.replace(hour=17)
        with patch.object(learning.time, "time", return_value=1000):
            self.service.send_notifications(evening)
        pending = [n for n in self.service._all("notification")
                   if n["kind"] == "reminder" and n["status"] == "pending"]
        self.assertEqual(len(pending), 1)
        self.dispatch("settings.save", {"reminder_enabled": False}, ADMIN)
        calls_before = len(self.sender.calls)
        with self.subTest(disabled="reminders"):
            with patch.object(learning.time, "time", return_value=1301):
                self.service.send_notifications(evening)
            self.assertEqual(len(self.sender.calls), calls_before)
        self.sender = FakeSender()
        self.service = learning.LearningService(self.root / "notification_stop", self.cloud, self.sender)
        self.enable()
        self.fixture()
        self.service.sync_pending()
        self.sender.on_send = lambda: self.dispatch("settings.save", {"enabled": False}, ADMIN)
        with self.subTest(disabled="during_first_send"):
            self.service.send_notifications(CURRENT)
            self.assertFalse(self.service.settings()["enabled"])
            self.assertEqual(len(self.sender.calls), 1)
            self.assertEqual(sum(n["status"] == "sent" for n in self.service._all("notification")), 1)
            self.service.send_notifications(CURRENT)
            self.assertEqual(len(self.sender.calls), 1)

    def test_restore_regrades_stale_cloud_record_and_correction_notice_waits_for_record_sync(self):
        self.enable()
        paper, question = self.fixture()
        self.dispatch("paper.answer", self.answer_payload(paper, question, option_ids=["o1"]))
        self.dispatch("paper.answer", self.answer_payload(paper, question, practice=True))
        self.service.sync_pending()
        self.service.send_notifications(CURRENT)
        self.service.sync_pending()
        old_record = copy.deepcopy(self.cloud.entities["record", paper["id"]])
        corrected = self.dispatch("question.save", {"id": question["id"], "version": question["version"],
                                  "correct_option_ids": ["o1"], "reason": "Verified corrected answer"}, ADMIN)
        notice_id = f"correction:{paper['id']}:{corrected['version']}"
        self.cloud.fail_entity_once = ("record", paper["id"])
        self.assertEqual(self.service.sync_pending(), {"pending_errors": 1})
        self.assertEqual(self.cloud.entities["paper", paper["id"]]["questions"][0]["correct_option_ids"], ["o1"])
        self.assertEqual(self.cloud.entities["record", paper["id"]], old_record)
        self.assertNotIn(("notification", notice_id), self.cloud.entities)
        source_service = self.service
        with self.subTest(stage="notice_waits_for_failed_record"):
            self.service.send_notifications(CURRENT)
            self.assertNotIn(notice_id, self.sender.delivered)
        self.sender = FakeSender()
        self.service = learning.LearningService(self.root / "correction_restore", self.cloud, self.sender)
        self.service.restore()
        repaired = self.service._get("record", paper["id"])
        entry = repaired["entries"][question["id"]]
        with self.subTest(stage="regrade_restored_record"):
            self.assertTrue(repaired["_dirty"])
            self.assertGreater(repaired["version"], old_record["version"])
            self.assertTrue(entry["attempt"]["correct"])
            self.assertFalse(entry["practice"][0]["correct"])
            self.assertEqual((entry["attempt"]["missed"], entry["attempt"]["wrong"]), ([], []))
            self.assertEqual((entry["practice"][0]["missed"], entry["practice"][0]["wrong"]), (["o1"], ["o0"]))
            for field in ("operation_id", "submitted_at", "option_ids", "assisted"):
                self.assertEqual(entry["attempt"][field], old_record["entries"][question["id"]]["attempt"][field])
            self.assertEqual(self.service.profile(ACTOR, {})["summary"]["correct"], 1)
        self.enable()
        with self.subTest(stage="notice_waits_for_repaired_record"):
            self.service.send_notifications(CURRENT)
            self.assertNotIn(notice_id, self.sender.delivered)
        self.assertEqual(self.service.sync_pending(), {"pending_errors": 0})
        self.assertEqual(source_service.sync_pending(force=True), {"pending_errors": 0})
        self.service.restore()
        self.service.send_notifications(CURRENT)
        with self.subTest(stage="notice_after_repaired_record_sync"):
            self.assertTrue(self.cloud.entities["record", paper["id"]]["entries"][question["id"]]["attempt"]["correct"])
            self.assertEqual(sum(identity == notice_id for _, _, identity in self.sender.calls), 1)

    def test_failed_paper_correction_blocks_record_sync_until_paper_retry_succeeds(self):
        self.enable()
        paper, question = self.fixture()
        self.dispatch("paper.answer", self.answer_payload(paper, question, option_ids=["o1"]))
        self.service.sync_pending()
        old_paper = copy.deepcopy(self.cloud.entities["paper", paper["id"]])
        old_record = copy.deepcopy(self.cloud.entities["record", paper["id"]])
        corrected = self.dispatch("question.save", {"id": question["id"], "version": question["version"],
                                  "correct_option_ids": ["o1"], "reason": "Reference correction"}, ADMIN)
        notice_id = f"correction:{paper['id']}:{corrected['version']}"
        notification = self.service._get("notification", notice_id)
        self.assertEqual(notification["question_id"], question["id"])
        self.assertTrue(notification["target_fingerprint"])
        self.cloud.fail_entity_once = ("paper", paper["id"])
        call_start = len(self.cloud.calls)
        self.assertEqual(self.service.sync_pending(), {"pending_errors": 1})
        self.assertEqual(self.cloud.saved_questions[question["id"]]["correct_option_ids"], ["o1"])
        self.assertFalse(self.service._get("question", question["id"])["_dirty"])
        self.assertEqual(self.cloud.entities["paper", paper["id"]], old_paper)
        self.assertEqual(self.cloud.entities["record", paper["id"]], old_record)
        self.assertTrue(self.service._get("paper", paper["id"])["_dirty"])
        self.assertTrue(self.service._get("record", paper["id"])["_dirty"])
        self.assertNotIn(("notification", notice_id), self.cloud.entities)
        self.assertTrue(self.service._get("notification", notice_id)["_dirty"])
        self.assertFalse(any(name == "upsert_entity" and args[:2] == ("record", paper["id"])
                             for name, args in self.cloud.calls[call_start:]))
        self.assertFalse(any(name == "upsert_entity" and args[:2] == ("notification", notice_id)
                             for name, args in self.cloud.calls[call_start:]))
        recovering = learning.LearningService(self.root / "paper_failure_restore", self.cloud, FakeSender())
        recovering.restore()
        recovered_paper = recovering._get("paper", paper["id"])
        recovered_record = recovering._get("record", paper["id"])
        self.assertEqual(recovered_paper["questions"][0]["correct_option_ids"], ["o0"])
        self.assertEqual(recovered_record["entries"], old_record["entries"])
        self.assertEqual(recovered_record["version"], old_record["version"])
        self.assertFalse(recovered_record["_dirty"])
        self.assertFalse(recovered_record["entries"][question["id"]]["attempt"]["correct"])
        self.restart()
        call_start = len(self.cloud.calls)
        self.assertEqual(self.service.sync_pending(force=True), {"pending_errors": 0})
        uploads = [args[:2] for name, args in self.cloud.calls[call_start:] if name == "upsert_entity"]
        self.assertLess(uploads.index(("paper", paper["id"])), uploads.index(("record", paper["id"])))
        self.assertEqual(self.cloud.entities["paper", paper["id"]]["questions"][0]["correct_option_ids"], ["o1"])
        self.assertTrue(self.cloud.entities["record", paper["id"]]["entries"][question["id"]]["attempt"]["correct"])
        self.assertFalse(self.service._get("paper", paper["id"])["_dirty"])
        self.assertFalse(self.service._get("record", paper["id"])["_dirty"])

    def test_restored_newer_correction_notice_is_superseded_for_older_cloud_paper(self):
        self.enable()
        paper, question = self.fixture()
        self.dispatch("paper.answer", self.answer_payload(paper, question, option_ids=["o1"]))
        self.service.sync_pending()
        self.service.send_notifications(CURRENT)
        self.service.sync_pending()
        old_cloud_paper = copy.deepcopy(self.cloud.entities["paper", paper["id"]])
        old_cloud_record = copy.deepcopy(self.cloud.entities["record", paper["id"]])
        corrected = self.dispatch("question.save", {"id": question["id"], "version": question["version"],
                                  "correct_option_ids": ["o1"], "reason": "Reference correction"}, ADMIN)
        notice_id = f"correction:{paper['id']}:{corrected['version']}"
        notice = self.service._get("notification", notice_id)
        self.assertEqual(notice["question_id"], question["id"])
        self.assertTrue(notice["target_fingerprint"])
        # Simulate a legacy partial write: newer notification beside an older paper.
        self.cloud.entities["notification", notice_id] = {k: v for k, v in notice.items() if not k.startswith("_")}
        self.sender = FakeSender()
        self.service = learning.LearningService(self.root / "stale_notice_restore", self.cloud, self.sender)
        self.service.restore()
        self.enable()
        restored_paper = self.service._get("paper", paper["id"])
        restored_record = self.service._get("record", paper["id"])
        self.assertEqual(restored_paper["questions"], old_cloud_paper["questions"])
        self.assertEqual(restored_record["entries"], old_cloud_record["entries"])
        self.assertFalse(restored_paper["_dirty"])
        self.assertFalse(restored_record["_dirty"])
        self.assertEqual(self.service._get("notification", notice_id)["status"], "pending")
        self.service.send_notifications(CURRENT)
        superseded = self.service._get("notification", notice_id)
        self.assertEqual(self.sender.calls, [])
        self.assertEqual(superseded["status"], "superseded")
        self.assertTrue(superseded["_dirty"])
        self.assertEqual(self.service.sync_pending(), {"pending_errors": 0})
        self.assertEqual(self.cloud.entities["notification", notice_id]["status"], "superseded")
        self.service.send_notifications(CURRENT)
        self.assertEqual(self.sender.calls, [])


    def test_year_validation_accepts_source_suffix_and_rejects_invalid_years(self):
        question = self.question(900)
        for year in ("", "1900", "2026", "2025年", "2100年"):
            with self.subTest(year=year):
                self.assertEqual(self.service._validated_question({**question, "year": year})["year"], year)
        for year in ("1899", "2101年", "20a6", "单选"):
            with self.subTest(year=year), self.assertRaises(learning.LearningError):
                self.service._validated_question({**question, "year": year})

    def test_completed_on_time_is_not_relabelled_late_by_review_or_notes(self):
        paper, question = self.fixture()
        self.dispatch("paper.answer", self.answer_payload(paper, question))
        original = self.service._get("record", paper["id"])
        with patch.object(learning, "now", return_value=CURRENT + dt.timedelta(days=1)):
            self.dispatch("paper.notes", {"id": paper["id"], "question_id": question["id"], "note": "next-day review"})
            self.dispatch("paper.answer", self.answer_payload(paper, question, practice=True))
        result = self.service._get("record", paper["id"])
        self.assertFalse(result["late"])
        self.assertEqual(result["completed_at"], original["completed_at"])

    def test_incomplete_draft_preserves_unmatched_reference_answer(self):
        self.enable()
        question = self.question(801, status="draft", correct_option_ids=[], answer_text="Original unresolved answer")
        self.seed([question])
        updated = self.dispatch("question.save", {"id": question["id"], "version": question["version"], "analysis": "Added context"}, ADMIN)
        self.assertEqual(updated["answer_text"], question["answer_text"])
        self.assertTrue(updated["problems"])
        self.service.sync_pending()
        self.assertEqual(self.cloud.saved_questions[question["id"]]["answer_text"], question["answer_text"])

    def test_new_question_lost_response_reuses_id_and_rejects_changed_retry(self):
        self.enable()
        payload = {"new_id": "q_retry_create", "bank": "written", "type": "single", "stem": "Retry safely", "status": "draft", "options": []}
        created = self.dispatch("question.save", payload, ADMIN)
        self.restart()
        repeated = self.dispatch("question.save", payload, ADMIN)
        self.assertEqual(created["id"], repeated["id"])
        self.assertEqual(len(self.service._all("question")), 1)
        self.assertEqual(len(self.service._all("audit")), 1)
        self.assert_status(409, self.service.save_question, {**payload, "stem": "Changed before retry"}, ADMIN)
        self.assertEqual(self.service._question(created["id"])["stem"], payload["stem"])

    def test_building_profile_has_rates_and_revalidation_is_not_completed(self):
        paper, question = self.fixture()
        self.dispatch("paper.answer", self.answer_payload(paper, question))
        row = next(b for b in self.service.profile(ADMIN, {})["buildings"] if b["scope"] == "A")
        self.assertEqual((row["accuracy"], row["independent_accuracy"]), (100.0, 100.0))
        self.enable()
        self.dispatch("question.save", {"id": question["id"], "version": question["version"], "stem": "Corrected wording"}, ADMIN)
        visible = self.dispatch("paper.get", {"id": paper["id"]})
        self.assertEqual((visible["stats"]["total"], visible["stats"]["answered"]), (0, 0))

    def test_interview_needing_recheck_has_consistent_completion_counts(self):
        self.enable()
        paper, question = self.fixture(kind="interview", bank="duty")
        self.dispatch("paper.answer", self.answer_payload(paper, question, answer_text="Response", self_rating=REVIEW))
        self.dispatch("question.save", {"id": question["id"], "version": question["version"], "answer_text": "Corrected answer"}, ADMIN)
        self.assertEqual(self.dispatch("paper.get", {"id": paper["id"]})["stats"]["answered"], 0)
        self.assertEqual(self.service.profile(ACTOR, {})["summary"]["answered"], 0)

    def test_exports_follow_question_filters_and_profile_period(self):
        self.seed([self.question(1), self.question(2, status="draft"), self.question(3, status="deleted")])
        for query, count in (({}, 2), ({"status": "published"}, 1), ({"status": "deleted"}, 1), ({"search": "question 2"}, 1)):
            with self.subTest(query=query):
                data, _, _ = self.service.export("questions", query, ADMIN)
                self.assertEqual(len(json.loads(data)["questions"]), count)
        self.service.publish(DAY - dt.timedelta(days=30))
        self.seed_pool(written=12)
        self.service.publish(DAY)
        data, _, _ = self.service.export("results", {"period": "day", "scope": "A"}, ADMIN)
        self.assertIn(DAY.isoformat(), data.decode("utf-8-sig"))
        self.assertNotIn((DAY - dt.timedelta(days=30)).isoformat(), data.decode("utf-8-sig"))
        filters = self.service._date_filters({"period": "month", "to": "2026-08-31"})
        self.assertEqual(filters["to"], "2026-08-31")
        self.assertNotIn("from", filters)

    def test_failed_question_batch_does_not_starve_other_sync_documents(self):
        self.seed([self.question(i) for i in range(45)], dirty=True)
        with self.service.transaction() as conn:
            self.service._put("record", "unrelated", {"id": "unrelated", "scope": "A", "entries": {}, "version": 0}, conn)
        with patch.object(self.cloud, "save_question", side_effect=ValueError("invalid source question")):
            self.assertEqual(self.service.sync_pending()["pending_errors"], 40)
            self.assertEqual(self.service.sync_pending()["pending_errors"], 5)
        self.assertIn(("record", "unrelated"), self.cloud.entities)
        self.assertEqual(self.service.sync_status()["pending"], 45)
        self.restart()
        self.service.sync_pending()
        self.assertEqual(self.service.sync_status()["pending"], 45)
        self.service.request_refresh()
        self.service.sync_pending(limit=100)
        self.assertEqual(self.service.sync_status()["pending"], 0)

    def test_questions_waiting_for_attachments_do_not_starve_other_records(self):
        self.seed([self.question(i, attachments=[{"id": "pending-file"}]) for i in range(45)], dirty=True)
        with self.service.transaction() as conn:
            self.service._put("record", "unrelated", {"id": "unrelated", "scope": "A", "entries": {}, "version": 0}, conn)
        for _ in range(2):
            self.assertEqual(self.service.sync_pending()["pending_errors"], 0)
        self.assertIn(("record", "unrelated"), self.cloud.entities)
        self.assertEqual(len(self.cloud.saved_questions), 0)
        self.assertEqual(self.service.sync_status()["pending"], 45)

    def test_import_ignores_embedded_identity_and_retries_without_overwriting_edits(self):
        self.enable()
        original = self.question(1)
        self.seed([original])
        baseline = self.service._question(original["id"])
        payload = {"questions": [{**original, "new_id": original["id"], "stem": "Imported copy"}]}
        self.service.dispatch("import", payload, ADMIN, {})
        questions = self.service._all("question")
        self.assertEqual(len(questions), 2)
        self.assertEqual(self.service._question(original["id"]), baseline)
        imported = next(q for q in questions if q["id"] != original["id"])
        edited = self.service.save_question({"id": imported["id"], "version": imported["version"], "stem": "Reviewed import"}, ADMIN)
        self.restart()
        self.service.dispatch("import", payload, ADMIN, {})
        self.assertEqual(len(self.service._all("question")), 2)
        self.assertEqual(self.service._question(imported["id"])["version"], edited["version"])
        self.assertEqual(self.service._question(imported["id"])["stem"], "Reviewed import")


if __name__ == "__main__":
    unittest.main()
