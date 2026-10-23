"""Offline tests only. Discovery/packaging must never contact a model provider."""
import contextlib
import copy
import datetime as dt
import io
import json
import multiprocessing
from pathlib import Path
import sqlite3
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from tools import evaluate_lighthouse_models as evaluation


def _sleeping_worker(connection, *args):
    try:
        time.sleep(5)
    finally:
        connection.close()


def _answer_worker(connection, *args):
    connection.send(("answer", "synthetic response"))
    connection.close()


class LighthouseLiveEvaluationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dataset, cls.digest = evaluation.load_dataset()
        cls.cases = cls.dataset["cases"]

    def response(self, case=None):
        return evaluation.expected_response(case or self.cases[0])

    def score(self, answer, case=None):
        return evaluation.score_answer(case or self.cases[0], json.dumps(answer))

    def live_report(self):
        """Exercise live orchestration with a deterministic in-process transport."""
        model = SimpleNamespace(store=SimpleNamespace(saved={}), close=Mock())
        by_question = {case["question"]: case for case in self.cases}

        def complete(saved, profile, messages, **kwargs):
            case = by_question[json.loads(messages[-1]["content"])["question"]]
            return "answer", json.dumps(self.response(case))

        with patch.object(evaluation, "load_shared_models", return_value=(model, [{}])), \
                patch.object(evaluation, "profile_fingerprint", return_value="a" * 64), \
                patch.object(evaluation, "_bounded_call", side_effect=complete):
            report = evaluation.evaluate(live=True)
        model.close.assert_called_once()
        return report

    def test_offline_default_never_loads_settings_or_calls_models(self):
        with patch.object(evaluation, "load_shared_models", side_effect=AssertionError("config touched")), \
                patch.object(evaluation, "_bounded_call", side_effect=AssertionError("network touched")):
            first = evaluation.evaluate()
            self.assertEqual(first, evaluation.evaluate())
        self.assertEqual(first["mode"], "offline_oracle")
        self.assertTrue(all(row["passed"] for row in first["models"][0]["cases"]))
        self.assertEqual(evaluation.qualified_results(first, current_fingerprints={"offline_oracle"}, trusted=True), [])

    def test_live_requires_literal_opt_in(self):
        with patch.object(evaluation.multiprocessing, "get_context", side_effect=AssertionError("process started")):
            for flag in (False, None, "true", 1):
                with self.subTest(flag=flag), self.assertRaises(ValueError):
                    evaluation._bounded_call({}, {}, [], live=flag, max_tokens=128, timeout=1)
        with patch.object(evaluation, "load_shared_models", side_effect=AssertionError("config touched")):
            for flag in (None, "true", 1):
                with self.subTest(flag=flag), self.assertRaises(ValueError):
                    evaluation.evaluate(live=flag)

    def test_worker_uses_existing_complete_with_exact_token_and_timeout_bounds(self):
        from openclaw_service.assistant.lighthouse_ai import CustomModel
        model = Mock(spec=CustomModel)
        model.complete.return_value = "synthetic answer"
        pipe, messages, profile = Mock(), [{"role": "user", "content": "synthetic"}], {"id": "synthetic"}
        with patch("openclaw_service.assistant.lighthouse_ai.CustomModel", return_value=model), \
                patch("logging.disable"):
            evaluation._live_worker(pipe, {}, profile, messages, 129, 2.5)
        model.complete.assert_called_once_with(messages, profile=profile, structured=True, max_tokens=129, timeout=2.5)
        model.close.assert_called_once()
        pipe.send.assert_called_once_with(("answer", "synthetic answer"))
        pipe.close.assert_called_once()

    def test_worker_never_forwards_provider_exception_text(self):
        for error, status in ((TimeoutError("secret"), "timeout"), (RuntimeError("private endpoint"), "error")):
            model, pipe = Mock(), Mock()
            model.complete.side_effect = error
            with patch("openclaw_service.assistant.lighthouse_ai.CustomModel", return_value=model), \
                    patch("logging.disable"):
                evaluation._live_worker(pipe, {}, {}, [], 128, 1)
            pipe.send.assert_called_once_with((status, ""))
            model.close.assert_called_once()

    def test_cli_default_and_live_switch(self):
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "output" / "report.json"
            with patch.object(evaluation, "load_shared_models", side_effect=AssertionError("config touched")), \
                    contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(evaluation.main(["--output", str(output)]), 0)
            self.assertEqual(json.loads(output.read_text())["mode"], "offline_oracle")
            with patch.object(evaluation, "evaluate", return_value=evaluation.evaluate()) as runner, \
                    contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(evaluation.main(["--live", "--output", str(output)]), 0)
            self.assertIs(runner.call_args.kwargs["live"], True)

    def test_dataset_has_required_categories_and_oracles_are_perfect(self):
        for task_type in evaluation.TASK_TYPES:
            self.assertGreaterEqual(sum(c["task_type"] == task_type for c in self.cases), 3)
        for case in self.cases:
            with self.subTest(case=case["id"]):
                score = self.score(self.response(case), case)
                self.assertEqual(score["score"], 1)
                self.assertEqual(score["failures"], [])
                self.assertEqual(score["failed_checks"], [])
                prompt = json.dumps(evaluation.build_messages(case), ensure_ascii=False)
                self.assertLessEqual(len(prompt.encode()), evaluation.MAX_PROMPT_BYTES)
                self.assertNotIn('"expected"', prompt)

    def test_fixture_facts_are_supported_independently_of_the_scorer(self):
        cases = {case["id"]: case for case in self.cases}
        roster = cases["roster_count_names"]
        people = {row["person_id"]: row["name"] for row in roster["sources"][0]["rows"]
                  if row["scope"] == "D" and row["shift"] == "day"}
        self.assertEqual(roster["expected"]["facts"], {"count": len(people), "names": sorted(people.values())})
        learning = cases["learning_night_dedup"]
        rule, entries = learning["sources"]
        start, end = (dt.datetime.fromisoformat(rule[key]) for key in ("start_inclusive", "end_exclusive"))
        people = {row["person_id"]: row["name"] for row in entries["rows"]
                  if row["scope"] == rule["scope"] and start <= dt.datetime.fromisoformat(row["at"]) < end}
        self.assertEqual(learning["expected"]["facts"], {"count": len(people), "names": sorted(people.values()),
                                                       "business_date": rule["business_date"]})
        maintenance = cases["maintenance_notice_state"]
        ongoing = [row["id"] for row in maintenance["sources"][0]["rows"]
                   if row["scope"] == "D" and row["notice_state"] == "ongoing"]
        self.assertEqual(maintenance["expected"]["facts"]["notice_ids"], ongoing)
        self.assertEqual(maintenance["expected"]["facts"]["count"], len(ongoing))
        partial = cases["roster_partial_page"]
        self.assertFalse(partial["sources"][0]["complete"])
        self.assertIsNone(partial["expected"]["facts"]["count"])
        self.assertEqual(partial["expected"]["facts"]["observed_count"], len(partial["sources"][0]["rows"]))
        current_sop = cases["sop_current_version"]
        current = [source for source in current_sop["sources"] if source["current"]]
        self.assertEqual(len(current), 1)
        self.assertEqual(current_sop["expected"]["source_ids"], [current[0]["id"]])
        self.assertEqual(current_sop["expected"]["facts"]["reviewer_count"], current[0]["reviewer_count"])
        zero = cases["mixed_zero_current_and_document"]
        self.assertTrue(zero["sources"][0]["complete"])
        self.assertTrue(all(row["state"] == "closed" for row in zero["sources"][0]["rows"]))
        self.assertEqual(zero["expected"]["facts"]["count"], 0)
        self.assertEqual(zero["expected"]["facts"]["notice_ids"], [])
        resolved = cases["followup_resolved_target"]
        selected = next(row for row in resolved["sources"][0]["rows"] if row["id"] == "TASK-B")
        self.assertEqual(resolved["expected"]["facts"], {"target_id": selected["id"], "completed": selected["state"] == "complete"})
        self.assertEqual(resolved["expected"]["followups"], [])

    def test_prompt_explicitly_defines_empty_fields_types_and_completeness(self):
        messages = evaluation.build_messages(self.cases[0])
        schema = json.loads(messages[-1]["content"])["response_schema"]
        self.assertIn('""', schema["understanding"]["shift"])
        self.assertEqual(schema["evidence"]["count"]["value"], "integer or null")
        self.assertIn("partial means some", messages[0]["content"])
        self.assertIn("context.date", messages[0]["content"])

    def test_strict_scores_reject_wrong_facts_types_citations_and_extras(self):
        for value in ("3", 3.0, True, 4, None):
            answer = self.response()
            answer["final"]["facts"]["count"] = value
            with self.subTest(value=value):
                score = self.score(answer)
                self.assertFalse(score["passed"])
                self.assertEqual(score["metrics"]["final_correctness"], 0)
        answer = self.response()
        answer["final"]["facts"]["names"].append("TEST_A")
        self.assertFalse(self.score(answer)["passed"])
        answer = self.response()
        answer["evidence"]["count"]["citations"] = ["R2"]
        self.assertEqual(self.score(answer)["failures"], ["evidence"])
        answer = self.response()
        answer["final"]["citations"]["count"] = ["INVENTED"]
        self.assertEqual(self.score(answer)["failures"], ["final_correctness"])
        answer = self.response()
        answer["self_score"] = 1
        self.assertEqual(self.score(answer)["score"], 0)
        answer = self.response()
        answer["final"]["facts"]["names"].reverse()
        self.assertTrue(self.score(answer)["passed"])

    def test_failure_stages_are_independent(self):
        changes = (
            ("understanding", lambda a: a["understanding"].update(scope="E")),
            ("source", lambda a: a.update(source_ids=["R2"])),
            ("evidence", lambda a: a["evidence"]["count"].update(value=9)),
            ("final_correctness", lambda a: a["final"]["facts"].update(count=9)),
            ("completeness", lambda a: a["final"].update(completeness="partial")),
            ("followups", lambda a: a["final"].update(followups=["identify_target"])),
        )
        for category, mutate in changes:
            answer = self.response()
            mutate(answer)
            with self.subTest(category=category):
                self.assertEqual(self.score(answer)["failures"], [category])

    def test_malformed_responses_score_zero(self):
        for answer in ("", "[]", "NaN", '{"x":1,"x":2}', "```json\n{}\n```", "x" * 24001, None):
            with self.subTest(kind=type(answer).__name__):
                result = evaluation.score_answer(self.cases[0], answer)
                self.assertEqual(result["status"], "invalid_response")
                self.assertEqual(result["score"], 0)

    def test_diagnostics_distinguish_format_from_semantic_fields_without_raw_values(self):
        for text, reason in (("", "empty_response"), ("[]", "response_shape"),
                             ('{"x":1,"x":2}', "duplicate_json_key"), ("NaN", "nonfinite_json"),
                             ("```json\n{}\n```", "invalid_json"), ("x" * 24001, "answer_too_long")):
            self.assertEqual(evaluation.score_answer(self.cases[0], text)["format_error"], reason)
        answer = self.response()
        answer["understanding"]["shift"] = "empty"
        result = self.score(answer)
        self.assertEqual(result["status"], "scored")
        self.assertEqual(result["format_error"], "")
        self.assertEqual(result["failed_checks"], ["response.understanding.shift"])
        self.assertEqual(result["failures"], ["understanding"])
        answer["final"]["facts"]["unexpected_private_key"] = "private synthetic value"
        result = self.score(answer)
        self.assertIn("response.final.facts.keys", result["failed_checks"])
        self.assertNotIn("unexpected_private_key", json.dumps(result))
        self.assertNotIn("private synthetic value", json.dumps(result))

    def test_missing_page_and_unsupported_sop_cannot_claim_completeness(self):
        for identity in ("roster_partial_page", "sop_irrelevant_evidence", "sop_conflicting_evidence", "mixed_missing_document"):
            case = next(c for c in self.cases if c["id"] == identity)
            answer = self.response(case)
            answer["final"].update(completeness="complete", limitations=[])
            with self.subTest(case=identity):
                self.assertEqual(self.score(answer, case)["metrics"]["completeness"], 0)

    def test_token_and_time_limits_reject_before_config_or_network(self):
        with patch.object(evaluation, "load_shared_models", side_effect=AssertionError("config touched")):
            for params in ({"max_tokens": 127}, {"max_tokens": 4001}, {"max_tokens": True},
                           {"timeout": 0}, {"timeout": float("nan")}, {"timeout": 121},
                           {"budget_seconds": 1801}, {"budget_seconds": float("inf")}):
                with self.subTest(params=params), self.assertRaises(ValueError):
                    evaluation.evaluate(live=True, **params)
        case = copy.deepcopy(self.cases[0])
        case["question"] = "x" * evaluation.MAX_PROMPT_BYTES
        with self.assertRaises(ValueError):
            evaluation.build_messages(case)

    def test_process_deadline_cleans_up_and_success_is_received(self):
        before = {p.pid for p in multiprocessing.active_children()}
        started = time.monotonic()
        result = evaluation._bounded_call({}, {}, [], live=True, max_tokens=128, timeout=0.2, worker=_sleeping_worker)
        self.assertEqual(result, ("timeout", ""))
        self.assertLess(time.monotonic() - started, 4)
        self.assertEqual({p.pid for p in multiprocessing.active_children()}, before)
        self.assertEqual(evaluation._bounded_call({}, {}, [], live=True, max_tokens=128, timeout=5,
                                                  worker=_answer_worker), ("answer", "synthetic response"))

    def test_timeout_errors_and_total_budget_never_qualify(self):
        for status in ("timeout", "error", "budget_exhausted"):
            model = SimpleNamespace(store=SimpleNamespace(saved={}), close=Mock())
            with patch.object(evaluation, "load_shared_models", return_value=(model, [{}])), \
                    patch.object(evaluation, "profile_fingerprint", return_value="a" * 64), \
                    patch.object(evaluation, "_bounded_call", return_value=(status, "private-provider-text")):
                report = evaluation.evaluate(live=True)
            self.assertEqual(evaluation.qualified_results(report, current_fingerprints={"a" * 64}, trusted=True), [])
            self.assertNotIn("private-provider-text", json.dumps(report))
        model = SimpleNamespace(store=SimpleNamespace(saved={}), close=Mock())
        with patch.object(evaluation, "load_shared_models", return_value=(model, [{}])), \
                patch.object(evaluation, "profile_fingerprint", return_value="a" * 64), \
                patch.object(evaluation, "_bounded_call", side_effect=AssertionError("budget exceeded")), \
                patch.object(evaluation.time, "monotonic", side_effect=range(100)):
            report = evaluation.evaluate(live=True, budget_seconds=0.1)
        self.assertTrue(all(row["status"] == "budget_exhausted" for row in report["models"][0]["cases"]))

    def test_quality_hook_requires_trust_current_config_complete_suite_and_sample_count(self):
        report = self.live_report()
        fingerprints = {"a" * 64}
        self.assertEqual(evaluation.qualified_results(report, current_fingerprints=fingerprints), [])
        self.assertEqual(evaluation.qualified_results(report, current_fingerprints={"b" * 64}, trusted=True), [])
        qualified = evaluation.qualified_results(report, current_fingerprints=fingerprints, trusted=True)
        self.assertEqual({row["task_type"] for row in qualified}, set(evaluation.TASK_TYPES))
        for row in qualified:
            self.assertEqual(row["score"], 100)
            self.assertEqual(row["sample_count"], 5 if row["task_type"] == "statistics" else 3)
            self.assertTrue(all(isinstance(v, (int, float)) for k, v in row.items()
                                if k not in {"profile_fingerprint", "task_type"}))
        self.assertEqual(evaluation.qualified_results(report, current_fingerprints=fingerprints, trusted=True, min_samples=2), [])
        stricter = evaluation.qualified_results(report, current_fingerprints=fingerprints, trusted=True, min_samples=4)
        self.assertEqual({row["task_type"] for row in stricter}, {"statistics"})
        self.assertEqual(evaluation.qualified_results(report, current_fingerprints=fingerprints, trusted=True, min_samples=6), [])
        report["models"][0]["cases"].pop()
        self.assertEqual(evaluation.qualified_results(report, current_fingerprints=fingerprints, trusted=True), [])

    def test_quality_hook_rejects_tampered_metrics_and_preserves_low_scores(self):
        report = self.live_report()
        for mutation in (lambda r: r.update(score=0.9), lambda r: r.update(latency_ms=float("nan")),
                         lambda r: r.update(task_type="unknown"), lambda r: r.update(passed=1),
                         lambda r: r["metrics"].update(source=2)):
            bad = copy.deepcopy(report)
            mutation(bad["models"][0]["cases"][0])
            self.assertEqual(evaluation.qualified_results(bad, current_fingerprints={"a" * 64}, trusted=True), [])
        row = report["models"][0]["cases"][0]
        row.update(evaluation.score_answer(self.cases[0], "invalid"))
        rows = evaluation.qualified_results(report, current_fingerprints={"a" * 64}, trusted=True)
        self.assertEqual(next(r["score"] for r in rows if r["task_type"] == "statistics"), 80)

    def test_per_case_observations_preserve_distinct_scores_and_current_config_gate(self):
        report = self.live_report()
        row = report["models"][0]["cases"][0]
        row.update(evaluation.score_answer(self.cases[0], "invalid"))
        before = copy.deepcopy(report)
        observations = evaluation.qualified_observations(report, current_fingerprints={"a" * 64}, trusted=True)
        self.assertEqual(report, before)
        self.assertEqual(len(observations), 14)
        self.assertEqual(len({row["observation_id"] for row in observations}), 14)
        self.assertEqual(sorted(row["score"] for row in observations), [0] + [100] * 13)
        self.assertEqual({row["task_type"] for row in observations}, set(evaluation.TASK_TYPES))
        for row in observations:
            self.assertEqual(set(row), {"observation_id", "profile_fingerprint", "task_type", "case_id", "score", "latency_ms"})
            self.assertEqual(row["profile_fingerprint"], "a" * 64)
        self.assertEqual(evaluation.qualified_observations(report, current_fingerprints={"a" * 64}), [])
        self.assertEqual(evaluation.qualified_observations(report, current_fingerprints={"b" * 64}, trusted=True), [])
        self.assertEqual(evaluation.qualified_observations(report, current_fingerprints={"a" * 64}, trusted=True, min_samples=2), [])

    def test_observation_ids_deduplicate_reloads_reruns_and_resume_partial_imports(self):
        report = self.live_report()
        options = {"current_fingerprints": {"a" * 64}, "trusted": True}
        original = evaluation.qualified_observations(report, **options)
        identities = {row["observation_id"] for row in original}
        serialized_copy = json.loads(json.dumps(report))
        self.assertEqual(evaluation.qualified_observations(serialized_copy, **options, seen_observation_ids=identities), [])
        seen = {row["observation_id"] for row in original[:-1]}
        self.assertEqual(evaluation.qualified_observations(report, **options, seen_observation_ids=seen), original[-1:])
        changed_run = copy.deepcopy(report)
        for row in changed_run["models"][0]["cases"]:
            row["latency_ms"] += 10
        changed_run["models"][0]["cases"][0].update(evaluation.score_answer(self.cases[0], "invalid"))
        self.assertEqual(evaluation.qualified_observations(changed_run, **options, seen_observation_ids=identities), [])
        changed_config = copy.deepcopy(report)
        changed_config["models"][0]["profile_fingerprint"] = "b" * 64
        for row in changed_config["models"][0]["cases"]:
            row["profile_fingerprint"] = "b" * 64
        changed = evaluation.qualified_observations(changed_config, current_fingerprints={"b" * 64}, trusted=True)
        self.assertFalse(identities & {row["observation_id"] for row in changed})

    def test_observations_do_not_pool_models_or_import_partial_malformed_or_offline_runs(self):
        report = self.live_report()
        second = copy.deepcopy(report["models"][0])
        second["profile_fingerprint"] = "b" * 64
        for row in second["cases"]:
            row["profile_fingerprint"] = "b" * 64
        report["models"].append(second)
        options = {"current_fingerprints": {"a" * 64, "b" * 64}, "trusted": True, "min_samples": 4}
        observations = evaluation.qualified_observations(report, **options)
        self.assertEqual(len(observations), 10)
        self.assertEqual({row["task_type"] for row in observations}, {"statistics"})
        report["models"][1]["cases"] = None
        self.assertEqual(len(evaluation.qualified_observations(report, **options)), 5)
        for mutate in (lambda r: r.update(mode="offline_oracle"),
                       lambda r: r["models"][0]["cases"][0].update(score=2),
                       lambda r: r["models"][0]["cases"][0].update(status="timeout"),
                       lambda r: r["models"][0]["cases"].pop()):
            bad = self.live_report()
            mutate(bad)
            self.assertEqual(evaluation.qualified_observations(bad, **options), [])

    def test_quality_hook_rejects_malformed_or_duplicate_report_shapes(self):
        report = self.live_report()
        changes = (
            lambda r: r.update(models=None), lambda r: r.update(models=[None]),
            lambda r: r.update(limits=None), lambda r: r["limits"].update(max_tokens=5000),
            lambda r: r["models"][0].update(cases="invalid"),
            lambda r: r["models"][0].update(profile_fingerprint=[]),
            lambda r: r["models"][0]["cases"][0].update(case_id=[]),
            lambda r: r["models"][0]["cases"][0].update(metrics=None),
            lambda r: r["models"][0]["cases"][0].update(status=[]),
            lambda r: r["models"].append(copy.deepcopy(r["models"][0])),
            lambda r: r.update(scorer="deterministic_exact_v1"),
            lambda r: r.update(dataset_version="lighthouse-synthetic-v1"),
        )
        for mutate in changes:
            bad = copy.deepcopy(report)
            mutate(bad)
            self.assertEqual(evaluation.qualified_results(bad, current_fingerprints={"a" * 64}, trusted=True), [])

    def test_real_store_api_is_read_only_and_legacy_defaults_stay_in_memory(self):
        from openclaw_service.assistant.lighthouse_ai import NAMESPACE
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "state.sqlite3"
            payload = {"enabled": True, "key_cipher": "synthetic-cipher"}
            with contextlib.closing(sqlite3.connect(path)) as connection:
                connection.execute("CREATE TABLE json_documents (namespace TEXT, key TEXT, payload_json TEXT)")
                connection.execute("INSERT INTO json_documents VALUES (?, ?, ?)", (NAMESPACE, "model", json.dumps(payload)))
                connection.commit()
            before = path.read_bytes(), path.stat().st_mtime_ns, set(Path(folder).iterdir())
            model, profiles = evaluation.load_shared_models(path)
            self.assertEqual(len(profiles), 1)
            self.assertEqual(model.store.saved["image_input_default_version"], 1)
            self.assertIn("models", model.store.saved)
            self.assertEqual((path.read_bytes(), path.stat().st_mtime_ns, set(Path(folder).iterdir())), before)
            with self.assertRaises(ValueError):
                model.store.get_document(NAMESPACE, "model:private-person")
            model.close()

    def test_fingerprint_changes_when_current_settings_or_credential_change(self):
        profile = {"endpoint": "https://example.invalid/v1/chat/completions", "model": "synthetic-model",
                   "key_cipher": "synthetic-cipher", "context_window": 64000}
        original = evaluation.profile_fingerprint(profile)
        for change in ({"key_cipher": "rotated-cipher"}, {"model": "another-model"},
                       {"context_window": 32000}, {"tool_calls": False}):
            self.assertNotEqual(evaluation.profile_fingerprint({**profile, **change}), original)

    def test_no_report_raw_answers_or_exception_details_and_output_is_constrained(self):
        report = self.live_report()
        text = json.dumps(report)
        for marker in ("TEST_A", "question", "endpoint", "key_cipher", "messages", "evidence\": {"):
            self.assertNotIn(marker, text)
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "output" / "evaluation.json"
            evaluation.write_report(report, path)
            self.assertEqual(json.loads(path.read_text()), report)
            with self.assertRaises(ValueError):
                evaluation.write_report(report, Path(folder) / "config.json")
        output = io.StringIO()
        with patch.object(evaluation, "load_shared_models", side_effect=RuntimeError("secret endpoint person")), \
                contextlib.redirect_stdout(output):
            self.assertEqual(evaluation.main(["--inspect-config"]), 2)
        self.assertNotIn("secret", output.getvalue())
        self.assertNotIn("person", output.getvalue())


if __name__ == "__main__":
    unittest.main()
