"""Question knowledge respects native visibility; no study workflow or cloud writes."""
import copy
import itertools
import json
import sys
import tempfile
import unittest
from unittest.mock import patch
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from pydantic_ai.models.function import DeltaToolCall, FunctionModel
from lan_bitable_template_portal.learning import LearningService
from lan_bitable_template_portal.lighthouse_ai import AssistantError, LighthouseAssistant
from lan_bitable_template_portal.lighthouse_agent import PortalAgent
from lan_bitable_template_portal.lighthouse_files import LighthouseFiles
from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog
from lan_bitable_template_portal.lighthouse_model import LighthouseModel, read_scope_operations
from lan_bitable_template_portal.lighthouse_sources import question_bank
from tools.lighthouse_test_backend import install_test_backend as install_lighthouse_routes
from lan_bitable_template_portal.portal_service import BUILDING_OPEN_ID_MAP
import test_learning as learning_fixture
from test_lighthouse_stream import Store

ADMIN = {"id": "admin", "is_admin": True, "scopes": ["A"], "learning_scopes": ["A"]}
DUTY_H = BUILDING_OPEN_ID_MAP["H"]
DUTY = {"id": DUTY_H, "is_admin": False, "scopes": list("ABCDEH"),
        "learning_scopes": ["H"], "scope": "H"}


class QuestionBankTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.cloud = learning_fixture.FakeCloud(enabled=False)
        self.service = LearningService(Path(self.temp.name), self.cloud, learning_fixture.FakeSender())
        self.questions = [learning_fixture.LearningTests.question(self, i, answer_text=f"answer-{i}") for i in range(4)]
        with self.service.transaction() as conn:
            for q in self.questions:
                self.service._put("question", q["id"], q, conn, False)
            self.service._put("local", "refresh", {"at": "2026-10-03T08:00:00+08:00"}, conn, False)
        self.paper("h1", "H", self.questions[:2])
        self.paper("a1", "A", self.questions[2:3])

    def paper(self, identity, scope, questions, *, deleted=False, entries=None):
        with self.service.transaction() as conn:
            self.service._put("paper", identity, {"id": identity, "date": "2026-10-03", "scope": scope,
                "shortage": {}, "created_at": "2026-10-03", "questions": copy.deepcopy(questions),
                "deleted_at": "2026-10-03" if deleted else ""}, conn, False)
            if entries:
                self.service._put("record", identity, {"id": identity, "entries": entries, "version": 1}, conn, False)

    def query(self, actor=None, **params):
        result = question_bank(self.service, actor or DUTY, params)
        self.assertEqual(self.cloud.calls, [], "question lookup must not access cloud or publish")
        return result

    def add_person(self, pid, building, login):
        with self.service.transaction() as conn:
            self.service._put("person", pid, {
                "id": pid, "person_id": pid, "name": pid, "employee_no": pid,
                "scopes": [building], "active": True, "login_ids": [login]}, conn, False)

    def self_actor(self, login, building, **changes):
        return {"id": login, "is_admin": False, "scopes": [building],
                "learning_scopes": [building], "scope": building, **changes}

    def test_supplemental_bank_filter_accepted(self):
        q = learning_fixture.LearningTests.question(self, 999, bank="supplemental")
        with self.service.transaction() as conn:
            self.service._put("question", q["id"], q, conn, False)
        self.paper("h_sup", "H", [q])
        self.assertEqual(self.query(bank="supplemental")["total"], 1)
        self.assertEqual(self.query(bank="written")["total"], 2)

    def test_mapped_self_ordinary_reads_only_own_assigned_papers(self):
        self.add_person("sp_h", "H", "ou_self_h")
        self.paper("self_h", "H", self.questions[3:], entries={
            self.questions[3]["id"]: {"revealed": "yes"}})
        with self.service.transaction() as conn:
            paper = self.service._get("paper", "self_h", conn)
            paper["person_id"] = "sp_h"
            self.service._put("paper", "self_h", paper, conn, False)
        # A different owner in the same building with a revealed answer must not leak.
        self.paper("other_h", "H", self.questions[0:1], entries={
            self.questions[0]["id"]: {"revealed": "yes"}})
        with self.service.transaction() as conn:
            paper = self.service._get("paper", "other_h", conn)
            paper["person_id"] = "other_h"
            self.service._put("paper", "other_h", paper, conn, False)
        actor = self.self_actor("ou_self_h", "H")
        result = self.query(actor)
        self.assertEqual({q["id"] for q in result["items"]}, {self.questions[3]["id"]})
        # Director answer shown only for own revealed paper.
        self.assertIn("answer-3", json.dumps(result))
        self.assertNotIn("answer-0", json.dumps(result))

    def test_unmapped_ordinary_forbidden_closed(self):
        actor = self.self_actor("ou_no_mapping", "H")
        with self.assertRaises(AssistantError) as caught:
            self.query(actor)
        self.assertEqual(caught.exception.status, 403)

    def test_duty_broad_scopes_still_cannot_cross_to_other_learning_building(self):
        # DUTY H has broad abcdeh scopes but may only read its own home building.
        duty_a = {"id": BUILDING_OPEN_ID_MAP["A"], "is_admin": False,
                  "scopes": list("ABCDEH"), "learning_scopes": ["A"], "scope": "A"}
        with self.assertRaises(AssistantError) as caught:
            self.query(duty_a, scope="H")
        self.assertEqual(caught.exception.status, 403)
        # And H duty cannot read A building either.
        with self.assertRaises(AssistantError) as caught:
            self.query(scope="A")
        self.assertEqual(caught.exception.status, 403)

    def test_admin_reads_full_bank_answers_but_no_storage_metadata(self):
        result = self.query(ADMIN)
        self.assertEqual(result["total"], 4)
        self.assertEqual({q["answer_text"] for q in result["items"]}, {f"answer-{i}" for i in range(4)})
        self.assertNotIn("record_id", result["items"][0])
        self.assertEqual(result["updated_at"], "2026-10-03T08:00:00+08:00")

    def test_duty_only_assigned_questions_no_implicit_answer_reveal(self):
        before = self.service._all("record")
        result = self.query()
        self.assertEqual({q["id"] for q in result["items"]}, {q["id"] for q in self.questions[:2]})
        for secret in ("answer-", "ANALYSIS-SECRET", "HINT-SECRET", "correct_option_ids"):
            self.assertNotIn(secret, json.dumps(result))
        self.assertEqual(self.service._all("record"), before)
        with self.assertRaises(AssistantError):
            self.query(scope="A")

    def test_revealed_answer_and_hint_follow_original_visibility(self):
        first, second = self.questions[:2]
        self.paper("h1", "H", [first, second], entries={first["id"]: {"revealed": "yes"}, second["id"]: {"hint_seen": True}})
        before = self.service._all("record")
        found = {q["id"]: q for q in self.query()["items"]}
        self.assertEqual(found[first["id"]]["answer"]["answer_text"], "answer-0")
        self.assertEqual(found[second["id"]]["answer"], {"hint": "HINT-SECRET"})
        self.assertEqual(self.query(search="answer-1")["total"], 0)
        self.assertEqual(self.service._all("record"), before)

    def test_deleted_papers_and_unassigned_bank_rows_are_invisible(self):
        self.paper("removed", "H", self.questions[3:], deleted=True)
        self.assertEqual({q["id"] for q in self.query()["items"]}, {q["id"] for q in self.questions[:2]})
        self.assertEqual(self.query(search="answer-3")["total"], 0)
        self.assertEqual(self.query(search="answer-2")["total"], 0)

    def test_repeated_paper_dedupes_version_without_hiding_permitted_answer(self):
        q = self.questions[0]
        self.paper("h2", "H", [q], entries={q["id"]: {"revealed": "yes"}})
        result = self.query()
        self.assertEqual(result["total"], 2)
        self.assertEqual(next(row for row in result["items"] if row["id"] == q["id"])["answer"]["answer_text"], "answer-0")

    def test_pagination_search_and_validation(self):
        self.assertEqual(self.query(page=2, page_size=1)["items"][0]["id"], self.questions[1]["id"])
        self.assertEqual(self.query(page=99)["items"], [])
        self.assertEqual(self.query(search="question")["total"], 2)
        for params in ({"page": "invalid"}, {"page_size": 41}, {"bank": "fake"}, {"search": "x" * 201}):
            with self.subTest(params=params), self.assertRaises(AssistantError):
                self.query(**params)

    def test_expanded_terms_recall_candidates_without_exact_sentence_match(self):
        q = {**self.questions[0], "stem": "UPS的主要作用是什么？", "answer_text": "保障连续供电"}
        with self.service.transaction() as conn:
            self.service._put("question", q["id"], q, conn, False)
        self.paper("h1", "H", [q])
        for actor in (ADMIN, DUTY):
            result = self.query(actor, search="ＵＰＳ 不间断电源 备用电源")
            self.assertEqual(result["total"], 1)
            self.assertEqual(result["items"][0]["stem"], q["stem"])
        self.assertEqual(self.query(search="保障连续供电")["total"], 0)
        self.assertEqual(self.query(ADMIN, search="保障连续供电")["total"], 1)

    def test_pressure_wording_variant_retrieves_only_visible_candidate(self):
        visible = {**self.questions[0], "stem": "CRAH过滤器堵塞对压差的影响是什么？"}
        hidden = {**self.questions[2], "stem": "压力差偏大的检查步骤？"}
        self.paper("h1", "H", [visible])
        self.paper("a1", "A", [hidden])
        result = self.query(search="空调 压力差 排查")
        self.assertEqual([q["id"] for q in result["items"]], [visible["id"]])
        self.assertNotIn("answer-", json.dumps(result))
        self.assertIn("候选", result["match_note"])

    def test_diagram_reference_is_reported_only_when_native_answer_visible(self):
        q = {**self.questions[0], "answer_text": "", "attachments": [
            {"id": "answer-diagram", "kind": "answer", "name": "answer.png", "size": 12, "file_token": "not-public"}]}
        self.paper("h1", "H", [q])
        self.assertNotIn("reference_note", self.query()["items"][0])
        self.paper("h1", "H", [q], entries={q["id"]: {"revealed": "yes"}})
        result = self.query()
        self.assertIn("未读取图示", result["items"][0]["reference_note"])
        self.assertNotIn("not-public", json.dumps(result))

    def test_uninitialized_cache_and_non_duty_are_not_reported_as_zero(self):
        with self.assertRaises(AssistantError) as err:
            question_bank(None, DUTY, {})
        self.assertEqual(err.exception.status, 503)
        # A revoked duty (learning_scopes emptied by authorize intersection) and an
        # unmapped ordinary account must both fail closed.
        with self.assertRaises(AssistantError) as err:
            self.query({**DUTY, "learning_scopes": []})
        self.assertEqual(err.exception.status, 403)
        unmapped = {"id": "ou_nobody", "is_admin": False, "scopes": ["H"],
                    "learning_scopes": ["H"]}
        with self.assertRaises(AssistantError) as err:
            self.query(unmapped)
        self.assertEqual(err.exception.status, 403)

    def test_query_budget_expiry_raises_503_not_zero(self):
        before = self.service._all("record")
        values = itertools.chain([0.0], itertools.repeat(100.0))
        with patch("lan_bitable_template_portal.lighthouse_sources.time.monotonic", lambda: next(values)):
            with self.assertRaises(AssistantError) as err:
                question_bank(self.service, DUTY, {})
        self.assertEqual(err.exception.status, 503)
        self.assertEqual(self.cloud.calls, [])
        self.assertEqual(self.service._all("record"), before)

    def test_many_papers_bank_filter_and_same_version_dedupe(self):
        before = self.service._all("record")
        # Same q0/version already in h1, so this duplicate paper adds no unique item.
        self.paper("h_bulk_dup", "H", [self.questions[0]])
        banks = ["written"] * 60 + ["duty"] * 30 + ["professional"] * 15
        for i, bank in enumerate(banks):
            self.paper(f"h_bulk_{i}", "H", [learning_fixture.LearningTests.question(self, 1000 + i, bank)])
        self.assertEqual(self.query()["total"], 107)
        self.assertEqual(self.query(bank="written")["total"], 62)
        self.assertEqual(self.query(bank="duty")["total"], 30)
        self.assertEqual(self.query(bank="professional")["total"], 15)
        self.assertEqual(self.service._all("record"), before)

    def test_out_of_range_page_empty_with_total_preserved(self):
        duty = self.query(page=99, page_size=20)
        self.assertEqual(duty["items"], [])
        self.assertEqual(duty["total"], 2)
        admin = self.query(ADMIN, page=99, page_size=20)
        self.assertEqual(admin["items"], [])
        self.assertEqual(admin["total"], 4)

    def test_invalid_scope_and_pagination_bounds(self):
        for actor in (ADMIN, DUTY):
            for params in ({"scope": "fake"}, {"page": 0}, {"page": -1},
                           {"page_size": 0}, {"page_size": -1}):
                with self.subTest(actor=actor["id"], params=params), self.assertRaises(AssistantError):
                    self.query(actor, **params)

    def test_question_bank_makes_no_cloud_calls_or_record_changes(self):
        before = self.service._all("record")
        self.assertNotEqual(self.query(ADMIN)["items"], [])
        self.assertNotEqual(self.query()["items"], [])
        self.assertEqual(self.cloud.calls, [])
        self.assertEqual(self.service._all("record"), before)

    def test_authenticated_route_and_catalog_preserve_native_exclusion(self):
        app = FastAPI()
        session = {"open_id": BUILDING_OPEN_ID_MAP["H"]}
        controller = SimpleNamespace(_current_session=lambda _: session)
        auth = SimpleNamespace(session_scopes=lambda _: list("ABCDEH"), is_admin=lambda _: False)
        runtime = SimpleNamespace(auth_manager=auth, learning_service=self.service)
        install_lighthouse_routes(app, controller, runtime)
        catalog = PortalAPICatalog(app)
        route = catalog.get("GET /api/assistant/question-bank")
        self.assertTrue(route["read_only"])
        self.assertEqual(set(route["schema"]["query"]), {"scope", "search", "bank", "page", "page_size"})
        admin_ops = read_scope_operations({"api_id": "GET /api/assistant/question-bank"}, route, {**ADMIN, "scopes": ["A", "B"]})
        self.assertNotIn("scope", admin_ops[0]["params"])
        for op in ("GET /api/learning/questions", "POST /api/learning/papers/{id}/reveal", "POST /api/assistant/question-bank"):
            with self.subTest(op=op), self.assertRaises(AssistantError):
                catalog.validate_operation({"api_id": op})
        client = TestClient(app)
        response = client.get("/api/assistant/question-bank")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["data"]["total"], 2)
        self.assertEqual(client.get("/api/assistant/question-bank?scope=A").status_code, 403)
        session["open_id"] = "ordinary-person"
        self.assertEqual(client.get("/api/assistant/question-bank").status_code, 403)
        session.clear()
        self.assertEqual(client.get("/api/assistant/question-bank").status_code, 401)

    async def test_model_uses_readonly_bank_without_answer_or_workflow_leak(self):
        app = FastAPI()
        identity = BUILDING_OPEN_ID_MAP["H"]
        controller = SimpleNamespace(_current_session=lambda _: {"open_id": identity})
        runtime = SimpleNamespace(learning_service=self.service,
            auth_manager=SimpleNamespace(session_scopes=lambda _: list("ABCDEH"), is_admin=lambda _: False))
        install_lighthouse_routes(app, controller, runtime)
        store = Store(Path(self.temp.name) / "assistant.sqlite3")
        assistant = LighthouseAssistant(store, lambda *_: ([], []), model=SimpleNamespace())
        portal = PortalAgent(assistant, PortalAPICatalog(app), LighthouseFiles(store))
        results = []

        async def stream(messages, info):
            returns = [p.content for m in messages for p in m.parts if getattr(p, "part_kind", "") == "tool-return"]
            if not returns:
                yield {0: DeltaToolCall(name="query", json_args=json.dumps({"operation": {
                    "api_id": "GET /api/assistant/question-bank", "params": {"search": "question"}}}))}
            else:
                results.extend(returns)
                yield "当前可见2道题；未开放的答案请在原学练页面查看。"

        @asynccontextmanager
        async def factory(*_):
            yield FunctionModel(stream_function=stream)

        async def authorize():
            return {**DUTY, "id": identity}

        async def emit(*_):
            pass

        request = Request({"type": "http", "scheme": "http", "server": ("testserver", 80),
                           "path": "/api/assistant/messages", "root_path": "", "query_string": b"", "headers": []})
        before = self.service._all("record")
        for index, question in enumerate(("查看题库的题目", "UPS是什么？")):
            with self.subTest(question=question):
                results.clear()
                answer = await LighthouseModel(portal, model_factory=factory).answer(await authorize(),
                    {"question": question, "operation_id": "question_bank_read_fixture_" + str(index),
                     "_profile": {"name": "Fixture", "model": "fixture"}},
                    [], request, emit, authorize, {})
                self.assertEqual(len(results), 1)
                self.assertTrue(results[0]["ok"], results)
                self.assertEqual(results[0]["data"]["total"], 2)
                self.assertNotIn("answer-", json.dumps(results))
                self.assertNotIn("plan", answer)
        self.assertEqual(self.service._all("record"), before)
        self.assertEqual(self.cloud.calls, [])


if __name__ == "__main__":
    unittest.main()
