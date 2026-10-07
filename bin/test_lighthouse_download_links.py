"""Pure-function tests for ``native_download_links``.

The tested module performs **no** network/server/filesystem calls; every case is
driven by native-shaped fixtures through the local pure function.  The suite
checks the exact allowed API families, strict safe-ID constraints, scope/id
conflict rejection, drill cloud-sync semantics, critical-guard source labels,
morning-meeting date handling, and removal of invented/speculative routes.
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lan_bitable_template_portal.lighthouse_downloads import native_download_links


def ok_envelope(raw):
    return {"ok": True, "_raw": raw}


LONG_ID = "x" * 160
TOO_LONG_ID = "x" * 161


def drill_execution(*, drill_id="drill-1", scope="A", execution_version=1,
                    generated_version=1, name="演练记录.xlsx", size=1024,
                    rule_current=True, status="sync_pending", drill_id_in=None,
                    generated=None):
    if generated is None:
        generated = {"name": name, "size": size}
    return {
        "drill_id": drill_id if drill_id_in is None else drill_id_in,
        "scope": scope,
        "execution_version": execution_version,
        "generated_version": generated_version,
        "generation_rule_current": rule_current,
        "status": status,
        "generated": generated,
    }


class DrillLinkTests(unittest.TestCase):
    def setUp(self):
        self.actor = {"id": "u", "scopes": ["A", "E"]}
        self.operation = {
            "api_id": "GET /api/drills/{drill_id}/execution",
            "path_params": {"drill_id": "drill-1"},
            "params": {"scope": "A"},
            "body": {},
        }

    def test_generate_after_success_yields_native_download(self):
        op = {"api_id": "POST /api/drills/{drill_id}/generate",
              "path_params": {"drill_id": "drill-1"}, "params": {"scope": "A"}, "body": {}}
        raw = {"execution": drill_execution()}
        result = native_download_links(self.actor, op, ok_envelope(raw))
        self.assertEqual(result, [{
            "name": "演练记录.xlsx",
            "url": "/api/drills/drill-1/download?scope=A",
        }])

    def test_cloud_sync_pending_does_not_block_current_file(self):
        # sync_pending means the local generated file exists; it must still link.
        raw = {"execution": drill_execution(status="sync_pending")}
        result = native_download_links(self.actor, self.operation, ok_envelope(raw))
        self.assertEqual([i["url"] for i in result], ["/api/drills/drill-1/download?scope=A"])

    def test_cloud_sync_error_does_not_block_current_file(self):
        raw = {"execution": drill_execution(status="error", rule_current=True)}
        result = native_download_links(self.actor, self.operation, ok_envelope(raw))
        self.assertEqual([i["url"] for i in result], ["/api/drills/drill-1/download?scope=A"])

    def test_queued_without_file_no_link(self):
        raw = {"execution": drill_execution(status="queued", generated={"name": ""})}
        self.assertEqual(native_download_links(self.actor, self.operation, ok_envelope(raw)), [])

    def test_generating_without_file_no_link(self):
        raw = {"execution": drill_execution(status="generating", generated={"name": ""})}
        self.assertEqual(native_download_links(self.actor, self.operation, ok_envelope(raw)), [])

    def test_newer_drill_version_not_link(self):
        raw = {"execution": drill_execution(execution_version=3, generated_version=2)}
        self.assertEqual(native_download_links(self.actor, self.operation, ok_envelope(raw)), [])

    def test_zero_version_not_link(self):
        raw = {"execution": drill_execution(execution_version=0, generated_version=0)}
        self.assertEqual(native_download_links(self.actor, self.operation, ok_envelope(raw)), [])

    def test_boolean_version_not_link(self):
        # bool is a subclass of int; explicit integers are required.
        raw = {"execution": drill_execution(execution_version=True, generated_version=1)}
        self.assertEqual(native_download_links(self.actor, self.operation, ok_envelope(raw)), [])
        raw = {"execution": drill_execution(execution_version=1, generated_version=True)}
        self.assertEqual(native_download_links(self.actor, self.operation, ok_envelope(raw)), [])

    def test_fractional_version_not_link(self):
        raw = {"execution": drill_execution(execution_version=1.5, generated_version=1)}
        self.assertEqual(native_download_links(self.actor, self.operation, ok_envelope(raw)), [])

    def test_missing_generated_metadata_not_link(self):
        raw = {"execution": drill_execution(name="")}
        self.assertEqual(native_download_links(self.actor, self.operation, ok_envelope(raw)), [])

    def test_rule_not_current_not_link(self):
        raw = {"execution": drill_execution(rule_current=False)}
        self.assertEqual(native_download_links(self.actor, self.operation, ok_envelope(raw)), [])

    def test_rule_current_missing_is_allowed(self):
        row = drill_execution()
        row["generation_rule_current"] = None
        raw = {"execution": row}
        self.assertEqual([i["url"] for i in native_download_links(self.actor, self.operation, ok_envelope(raw))],
                         ["/api/drills/drill-1/download?scope=A"])

    def test_scope_not_in_actor_not_link(self):
        raw = {"execution": drill_execution(scope="C")}
        self.assertEqual(native_download_links(self.actor, self.operation, ok_envelope(raw)), [])

    def test_request_scope_conflict_not_link(self):
        op = {"api_id": "GET /api/drills/{drill_id}/execution",
              "path_params": {"drill_id": "drill-1"}, "params": {"scope": "E"}, "body": {}}
        raw = {"execution": drill_execution(scope="A")}
        self.assertEqual(native_download_links(self.actor, op, ok_envelope(raw)), [])

    def test_params_body_scope_conflict_rejected(self):
        op = {"api_id": "GET /api/drills/{drill_id}/execution",
              "path_params": {"drill_id": "drill-1"},
              "params": {"scope": "A"}, "body": {"scope": "E"}}
        raw = {"execution": drill_execution(scope="A")}
        self.assertEqual(native_download_links(self.actor, op, ok_envelope(raw)), [])

    def test_unknown_scope_not_silently_fallback(self):
        op = {"api_id": "GET /api/drills/{drill_id}/execution",
              "path_params": {"drill_id": "drill-1"}, "params": {"scope": "Z"}, "body": {}}
        raw = {"execution": drill_execution(scope="A")}
        self.assertEqual(native_download_links(self.actor, op, ok_envelope(raw)), [])

    def test_drill_id_mismatch_not_link(self):
        raw = {"execution": drill_execution(drill_id_in="other-drill")}
        self.assertEqual(native_download_links(self.actor, self.operation, ok_envelope(raw)), [])

    def test_malformed_drill_id_not_link(self):
        for bad in ("", "a/../b", "sp ace", TOO_LONG_ID, "../x", "a.b"):
            op = {"api_id": "GET /api/drills/{drill_id}/execution",
                  "path_params": {"drill_id": bad}, "params": {"scope": "A"}, "body": {}}
            result = native_download_links(self.actor, op, ok_envelope({"execution": drill_execution()}))
            self.assertEqual(result, [], f"drill_id={bad!r} should be rejected")

    def test_no_arbitrary_or_upstream_urls(self):
        raw = {"execution": drill_execution(), "download_url": "https://evil.example/x"}
        result = native_download_links(self.actor, self.operation, ok_envelope(raw))
        self.assertEqual(result[0]["url"], "/api/drills/drill-1/download?scope=A")

    def test_not_ok_result_yields_nothing(self):
        result = native_download_links(self.actor, self.operation,
                                       {"ok": False, "error": "boom"})
        self.assertEqual(result, [])

    def test_retry_sync_accepted(self):
        op = {"api_id": "POST /api/drills/{drill_id}/retry-sync",
              "path_params": {"drill_id": "drill-1"}, "params": {"scope": "A"}, "body": {}}
        raw = {"execution": drill_execution()}
        result = native_download_links(self.actor, op, ok_envelope(raw))
        self.assertEqual(result[0]["url"], "/api/drills/drill-1/download?scope=A")


class CabinetLinkTests(unittest.TestCase):
    def setUp(self):
        self.actor = {"id": "u", "scopes": list("ABCDE")}

    def _job(self, *, status="succeeded", export_id="e" * 32, scope="A",
             filename="A楼.xlsm", deleted=False, file_available=True, job_id="job-1",
             kind="export", result_scope=None):
        result = {"export_id": export_id, "scope": result_scope or scope, "filename": filename}
        return {
            "job_id": job_id, "scope": scope, "status": status, "kind": kind,
            "phase": "generated", "result": result, "deleted": deleted,
            "file_available": file_available,
        }

    def test_single_job_success(self):
        op = {"api_id": "GET /api/cabinet-power/jobs/{job_id}",
              "path_params": {"job_id": "job-1", "scope": "A"}, "params": {"scope": "A"}}
        result = native_download_links(self.actor, op, ok_envelope(self._job()))
        self.assertEqual(result, [{
            "name": "A楼.xlsm",
            "url": f"/api/cabinet-power/exports/{'e' * 32}/download",
        }])

    def test_post_exports_nativejob_accepted(self):
        op = {"api_id": "POST /api/cabinet-power/exports",
              "body": {"scope": "A"}, "params": {}}
        result = native_download_links(self.actor, op, ok_envelope(self._job()))
        self.assertEqual(result, [{
            "name": "A楼.xlsm",
            "url": f"/api/cabinet-power/exports/{'e' * 32}/download",
        }])

    def test_job_id_mismatch_not_link(self):
        op = {"api_id": "GET /api/cabinet-power/jobs/{job_id}",
              "path_params": {"job_id": "job-1"}, "params": {"scope": "A"}}
        result = native_download_links(self.actor, op, ok_envelope(self._job(job_id="other")))
        self.assertEqual(result, [])

    def test_job_result_scope_mismatch_rejected(self):
        # Nested result.scope must agree with the job's own scope; a mismatch is
        # rejected rather than silently overwritten.
        op = {"api_id": "GET /api/cabinet-power/jobs/{job_id}",
              "path_params": {"job_id": "job-1"}, "params": {"scope": "A"}}
        result = native_download_links(self.actor, op, ok_envelope(self._job(result_scope="B")))
        self.assertEqual(result, [])

    def test_post_exports_nativejob_id_safety_checked_without_path(self):
        # Even when no job_id arrives in the request path, a native job_id that
        # is not a safe identifier must be rejected.
        op = {"api_id": "POST /api/cabinet-power/exports",
              "body": {"scope": "A"}, "params": {}}
        result = native_download_links(self.actor, op, ok_envelope(self._job(job_id="../x")))
        self.assertEqual(result, [])

    def test_job_not_export_kind_not_link(self):
        op = {"api_id": "GET /api/cabinet-power/jobs/{job_id}",
              "path_params": {"job_id": "job-1"}, "params": {"scope": "A"}}
        result = native_download_links(self.actor, op, ok_envelope(self._job(kind="refresh")))
        self.assertEqual(result, [])

    def test_job_not_done_not_link(self):
        result = native_download_links(
            self.actor,
            {"api_id": "GET /api/cabinet-power/jobs/{job_id}",
             "path_params": {"job_id": "job-1"}, "params": {"scope": "A"}},
            ok_envelope(self._job(status="failed")))
        self.assertEqual(result, [])

    def test_job_deleted_not_link(self):
        result = native_download_links(
            self.actor,
            {"api_id": "GET /api/cabinet-power/jobs/{job_id}",
             "path_params": {"job_id": "job-1"}, "params": {"scope": "A"}},
            ok_envelope(self._job(deleted=True)))
        self.assertEqual(result, [])

    def test_job_file_unavailable_not_link(self):
        result = native_download_links(
            self.actor,
            {"api_id": "GET /api/cabinet-power/jobs/{job_id}",
             "path_params": {"job_id": "job-1"}, "params": {"scope": "A"}},
            ok_envelope(self._job(file_available=False)))
        self.assertEqual(result, [])

    def test_malformed_export_id_not_link(self):
        for bad in ("", "a/b", "sp ace", TOO_LONG_ID, "a/b/c", "../x"):
            result = native_download_links(
                self.actor,
                {"api_id": "GET /api/cabinet-power/jobs/{job_id}",
                 "path_params": {"job_id": "job-1"}, "params": {"scope": "A"}},
                ok_envelope(self._job(export_id=bad)))
            self.assertEqual(result, [], f"export_id={bad!r} should be rejected")

    def test_malformed_job_id_not_link(self):
        op = {"api_id": "GET /api/cabinet-power/jobs/{job_id}",
              "path_params": {"job_id": "../x"}, "params": {"scope": "A"}}
        result = native_download_links(self.actor, op, ok_envelope(self._job()))
        self.assertEqual(result, [])

    def test_export_history(self):
        history = {
            "items": [
                {"export_id": "e1", "scope": "A", "filename": "A.xlsm",
                 "status": "succeeded", "deleted": False, "file_available": True},
                {"export_id": "s" * 16, "scope": "A", "filename": "C.xlsm",
                 "status": "done", "deleted": False, "file_available": True},
                {"export_id": "stale-id", "scope": "A", "filename": "A-old.xlsm",
                 "status": "done", "deleted": False, "file_available": False},
            ],
        }
        result = native_download_links(
            self.actor,
            {"api_id": "GET /api/cabinet-power/export-history", "params": {"scope": "A"}},
            ok_envelope(history))
        self.assertEqual([item["url"] for item in result],
                         [f"/api/cabinet-power/exports/{x}/download" for x in ("e1", "s" * 16)])

    def test_export_history_filters_request_scope(self):
        history = {"items": [
            {"export_id": "a" * 16, "scope": "A", "filename": "A.xlsm",
             "status": "succeeded", "deleted": False, "file_available": True},
            {"export_id": "b" * 16, "scope": "B", "filename": "B.xlsm",
             "status": "succeeded", "deleted": False, "file_available": True},
        ]}
        result = native_download_links(
            {"id": "u", "scopes": ["B"]},
            {"api_id": "GET /api/cabinet-power/export-history", "params": {"scope": "B"}},
            ok_envelope(history))
        self.assertEqual([item["url"] for item in result],
                         [f"/api/cabinet-power/exports/{'b' * 16}/download"])

    def test_export_history_requires_request_scope(self):
        history = {"items": [
            {"export_id": "a" * 16, "scope": "A", "filename": "A.xlsm",
             "status": "succeeded", "deleted": False, "file_available": True}]}
        result = native_download_links(
            self.actor,
            {"api_id": "GET /api/cabinet-power/export-history", "params": {}},
            ok_envelope(history))
        self.assertEqual(result, [])

    def test_post_export_history_removed(self):
        # POST /export-history is not a real route and must not produce links.
        history = {"items": [{"export_id": "a" * 16, "scope": "A", "filename": "A.xlsm",
                              "status": "succeeded", "deleted": False, "file_available": True}]}
        result = native_download_links(
            self.actor,
            {"api_id": "POST /api/cabinet-power/export-history", "params": {"scope": "A"}},
            ok_envelope(history))
        self.assertEqual(result, [])

    def test_post_jobs_removed(self):
        result = native_download_links(
            self.actor,
            {"api_id": "POST /api/cabinet-power/jobs/{job_id}",
             "path_params": {"job_id": "job-1"}, "params": {"scope": "A"}},
            ok_envelope(self._job()))
        self.assertEqual(result, [])

    def test_post_exports_export_batches_resume_removed(self):
        batch = {"items": {"A": {"scope": "A", "status": "succeeded",
                                  "result": {"export_id": "a12345", "scope": "A",
                                             "filename": "A楼.xlsm"}}}}
        result = native_download_links(
            self.actor,
            {"api_id": "POST /api/cabinet-power/exports/export-batches/resume",
             "params": {"scope": "A"}},
            ok_envelope(batch))
        self.assertEqual(result, [])

    def _batch(self, status="succeeded", batch_id="all_" + "0" * 32, scopes="ABCDE",
               result_scope_override=None, child_status=None, result_present=True):
        return {
            "batch_id": batch_id, "status": status, "phase": "completed",
            "items": {
                scope: {
                    "scope": scope,
                    "status": (child_status or {c: "succeeded" for c in "ABCDE"})[scope],
                    "result": ({
                        "export_id": scope + "12345", "scope": result_scope_override or scope,
                        "filename": f"{scope}楼.xlsm"}
                        if result_present else {}),
                }
                for scope in scopes
            },
        }

    def test_five_cabinet_batch_all_succeeded(self):
        batch = self._batch()
        result = native_download_links(
            self.actor,
            {"api_id": "GET /api/cabinet-power/export-batches/{batch_id}",
             "path_params": {"batch_id": "all_" + "0" * 32}},
            ok_envelope(batch))
        self.assertEqual(len(result), 5)
        self.assertEqual([item["url"] for item in result],
                         [f"/api/cabinet-power/exports/{sc}12345/download" for sc in "ABCDE"])

    def test_post_export_batches_new_batch(self):
        op = {"api_id": "POST /api/cabinet-power/export-batches",
              "body": {"batch_id": "all_" + "1" * 32}, "params": {}}
        batch = self._batch(batch_id="all_" + "1" * 32)
        result = native_download_links(self.actor, op, ok_envelope(batch))
        self.assertEqual(len(result), 5)
        self.assertEqual(result[0]["url"], "/api/cabinet-power/exports/A12345/download")

    def test_post_export_batches_batch_id_mismatch(self):
        op = {"api_id": "POST /api/cabinet-power/export-batches",
              "body": {"batch_id": "all_" + "2" * 32}, "params": {}}
        batch = self._batch(batch_id="all_" + "9" * 32)
        result = native_download_links(self.actor, op, ok_envelope(batch))
        self.assertEqual(result, [])

    def test_resume_batch_path_identity(self):
        op = {"api_id": "POST /api/cabinet-power/export-batches/{batch_id}/resume",
              "path_params": {"batch_id": "all_" + "3" * 32}}
        batch = self._batch(batch_id="all_" + "3" * 32)
        result = native_download_links(self.actor, op, ok_envelope(batch))
        self.assertEqual(len(result), 5)

    def test_resume_batch_path_identity_mismatch(self):
        op = {"api_id": "POST /api/cabinet-power/export-batches/{batch_id}/resume",
              "path_params": {"batch_id": "all_" + "3" * 32}}
        batch = self._batch(batch_id="all_" + "7" * 32)
        result = native_download_links(self.actor, op, ok_envelope(batch))
        self.assertEqual(result, [])

    def test_batch_path_body_batch_id_conflict_rejected(self):
        # A batch_id in both path and body must agree; a conflict is rejected
        # instead of silently selecting one of them.
        op = {"api_id": "POST /api/cabinet-power/export-batches/{batch_id}/resume",
              "path_params": {"batch_id": "all_" + "3" * 32},
              "body": {"batch_id": "all_" + "9" * 32}, "params": {}}
        batch = self._batch(batch_id="all_" + "3" * 32)
        result = native_download_links(self.actor, op, ok_envelope(batch))
        self.assertEqual(result, [])

    def test_partial_batch_keeps_succeeded_children(self):
        statuses = {c: ("succeeded" if c in "AB" else "failed") for c in "ABCDE"}
        items = {}
        for scope in "ABCDE":
            item = {"scope": scope, "status": statuses[scope]}
            if scope in "AB":
                item["result"] = {"export_id": scope + "abc", "scope": scope,
                                  "filename": f"{scope}楼.xlsm"}
            else:
                item["result"] = {}
            items[scope] = item
        batch = {"batch_id": "all_" + "1" * 32, "status": "failed", "items": items}
        result = native_download_links(
            self.actor,
            {"api_id": "POST /api/cabinet-power/export-batches/{batch_id}/resume",
             "path_params": {"batch_id": "all_" + "1" * 32}},
            ok_envelope(batch))
        self.assertEqual([item["url"] for item in result],
                         [f"/api/cabinet-power/exports/{sc}abc/download" for sc in "AB"])

    def test_batch_result_scope_mismatch_with_child_dict_rejected(self):
        # result.scope "B" nested under child dictionary "A" must not link even if
        # the actor is permitted for B.
        actor = {"id": "u", "scopes": ["A", "B"]}
        batch = {"items": {"A": {"scope": "A", "status": "succeeded",
                                  "result": {"export_id": "b01", "scope": "B",
                                             "filename": "B楼.xlsm"}}}}
        result = native_download_links(
            actor,
            {"api_id": "GET /api/cabinet-power/export-batches/{batch_id}",
             "path_params": {"batch_id": "b"}},
            ok_envelope(batch))
        self.assertEqual(result, [])

    def test_batch_child_dict_scope_mismatch_with_key_rejected(self):
        # Child dict scope "B" under key "A" must be rejected (scope identity).
        actor = {"id": "u", "scopes": ["A", "B"]}
        batch = {"items": {"A": {"scope": "B", "status": "succeeded",
                                  "result": {"export_id": "b01", "scope": "B",
                                             "filename": "B楼.xlsm"}}}}
        result = native_download_links(
            actor,
            {"api_id": "GET /api/cabinet-power/export-batches/{batch_id}",
             "path_params": {"batch_id": "b"}},
            ok_envelope(batch))
        self.assertEqual(result, [])

    def test_batch_result_missing_not_link(self):
        batch = {"items": {"A": {"scope": "A", "status": "succeeded", "result": {}}}}
        result = native_download_links(
            self.actor,
            {"api_id": "GET /api/cabinet-power/export-batches/{batch_id}",
             "path_params": {"batch_id": "x"}},
            ok_envelope(batch))
        self.assertEqual(result, [])

    def test_batch_malformed_batch_id_rejected(self):
        result = native_download_links(
            self.actor,
            {"api_id": "GET /api/cabinet-power/export-batches/{batch_id}",
             "path_params": {"batch_id": "../x"}},
            ok_envelope(self._batch()))
        self.assertEqual(result, [])


class CriticalGuardLinkTests(unittest.TestCase):
    def setUp(self):
        self.actor = {"id": "u", "scopes": ["A", "C"]}

    def _row(self, *, scope="A", response_id="resp-1", has_image=True,
             has_workbook=True, source_file=None, sheet_type="检查表", check_date="2026-10-03",
             task_id=None):
        row = {"scope": scope, "response_id": response_id,
               "has_image": has_image, "has_workbook": has_workbook,
               "sheet_type": sheet_type, "check_date": check_date,
               "status": "submitted"}
        if task_id is not None:
            row["task_id"] = task_id
        if source_file is not None:
            row["source_file"] = source_file
        return row

    def _source(self, *, file_id="file-1", scope="A", file_name="物资清单.xlsx",
                sheet_type="检查表", check_date="2026-10-03"):
        return {"file_id": file_id, "scope": scope, "file_name": file_name,
                "sheet_type": sheet_type, "check_date": check_date,
                "sha256": "0" * 64, "size": 100}

    def test_task_detail_wraps_responses_array(self):
        raw = {"task_id": "t1", "task_name": "重保",
               "responses": [self._row(scope="A"), self._row(scope="C", response_id="resp-9")]}
        result = native_download_links(
            self.actor,
            {"api_id": "GET /api/critical-guard/tasks/{task_id}",
             "path_params": {"task_id": "t1"}, "params": {}},
            ok_envelope(raw))
        self.assertEqual(
            [item["url"] for item in result],
            [
                "/api/critical-guard/images/resp-1",
                "/api/critical-guard/workbooks/resp-1",
                "/api/critical-guard/images/resp-9",
                "/api/critical-guard/workbooks/resp-9",
            ],
        )

    def test_task_id_mismatch_not_link(self):
        raw = {"task_id": "other", "responses": [self._row(scope="A")]}
        result = native_download_links(
            self.actor,
            {"api_id": "GET /api/critical-guard/tasks/{task_id}",
             "path_params": {"task_id": "t1"}, "params": {}},
            ok_envelope(raw))
        self.assertEqual(result, [])

    def test_task_response_row_task_id_mismatch_rejected(self):
        # A response row that carries its own task_id must agree with the
        # request's task_id; the mismatching row must never be linked.
        raw = {"task_id": "t1", "responses": [
            {"scope": "A", "response_id": "resp-1", "task_id": "other-task",
             "has_image": True, "has_workbook": True}]}
        result = native_download_links(
            self.actor,
            {"api_id": "GET /api/critical-guard/tasks/{task_id}",
             "path_params": {"task_id": "t1"}, "params": {}},
            ok_envelope(raw))
        self.assertEqual(result, [])

    def test_task_response_row_task_id_match_accepted(self):
        raw = {"task_id": "t1", "responses": [
            self._row(scope="A", response_id="resp-1", task_id="t1")]}
        result = native_download_links(
            self.actor,
            {"api_id": "GET /api/critical-guard/tasks/{task_id}",
             "path_params": {"task_id": "t1"}, "params": {}},
            ok_envelope(raw))
        self.assertEqual([item["url"] for item in result],
                         ["/api/critical-guard/images/resp-1",
                          "/api/critical-guard/workbooks/resp-1"])

    def test_task_malformed_id_rejected(self):
        raw = {"task_id": "t1", "responses": [self._row(scope="A")]}
        result = native_download_links(
            self.actor,
            {"api_id": "GET /api/critical-guard/tasks/{task_id}",
             "path_params": {"task_id": "../t"}, "params": {}},
            ok_envelope(raw))
        self.assertEqual(result, [])

    def test_put_response_saves_and_links(self):
        raw = self._row(scope="A")
        result = native_download_links(
            self.actor,
            {"api_id": "PUT /api/critical-guard/responses/{response_id}",
             "path_params": {"response_id": "resp-1"}, "body": {"scope": "A"}},
            ok_envelope(raw))
        self.assertEqual([item["url"] for item in result],
                         ["/api/critical-guard/images/resp-1",
                          "/api/critical-guard/workbooks/resp-1"])

    def test_put_response_id_mismatch_rejected(self):
        raw = self._row(scope="A", response_id="resp-other")
        result = native_download_links(
            self.actor,
            {"api_id": "PUT /api/critical-guard/responses/{response_id}",
             "path_params": {"response_id": "resp-1"}, "body": {"scope": "A"}},
            ok_envelope(raw))
        self.assertEqual(result, [])

    def test_put_response_malformed_id_rejected(self):
        raw = self._row(scope="A")
        result = native_download_links(
            self.actor,
            {"api_id": "PUT /api/critical-guard/responses/{response_id}",
             "path_params": {"response_id": "../x"}, "body": {"scope": "A"}},
            ok_envelope(raw))
        self.assertEqual(result, [])

    def test_get_responses_removed(self):
        # There is no GET /api/critical-guard/responses route.
        raw = {"response": self._row(scope="A")}
        result = native_download_links(
            self.actor,
            {"api_id": "GET /api/critical-guard/responses", "params": {"scope": "A"}},
            ok_envelope(raw))
        self.assertEqual(result, [])

    def test_scope_conflict_not_link(self):
        raw = {"responses": [self._row(scope="B")]}
        result = native_download_links(
            self.actor,
            {"api_id": "GET /api/critical-guard/tasks/{task_id}",
             "path_params": {"task_id": "t1"}},
            ok_envelope(raw))
        self.assertEqual(result, [])

    def test_images_labeled_generated_check_sheet_image(self):
        raw = {"responses": [self._row(scope="A")]}
        result = native_download_links(
            self.actor,
            {"api_id": "GET /api/critical-guard/tasks/{task_id}",
             "path_params": {"task_id": "t1"}, "params": {}},
            ok_envelope(raw))
        image = next(item for item in result if item["url"].startswith("/api/critical-guard/images/"))
        self.assertNotIn("现场图", image["name"])
        self.assertIn("重保检查表图片", image["name"])
        self.assertIn("检查表", image["name"])
        self.assertIn("2026-10-03", image["name"])

    def test_source_file_uses_file_name_key_and_matches_parent_scope(self):
        raw = {"responses": [self._row(scope="A", source_file=self._source(scope="A"))]}
        result = native_download_links(
            self.actor,
            {"api_id": "GET /api/critical-guard/tasks/{task_id}",
             "path_params": {"task_id": "t1"}, "params": {"scope": "A"}},
            ok_envelope(raw))
        source = next(item for item in result if item["url"].endswith("file-1"))
        self.assertEqual(source["name"], "物资清单.xlsx-A楼-检查表-2026-10-03")
        self.assertIn("/api/critical-guard/source-files/file-1", source["url"])

    def test_source_file_scope_mismatch_rejected(self):
        # Nested source scope "B" under parent "A" must be rejected.
        raw = {"responses": [self._row(scope="A", source_file=self._source(scope="B"))]}
        result = native_download_links(
            self.actor,
            {"api_id": "GET /api/critical-guard/tasks/{task_id}",
             "path_params": {"task_id": "t1"}, "params": {"scope": "A"}},
            ok_envelope(raw))
        self.assertTrue(not any(item["url"].endswith("file-1") for item in result))

    def test_source_file_sheet_type_mismatch_rejected(self):
        raw = {"responses": [self._row(
            scope="A", sheet_type="检查表",
            source_file=self._source(scope="A", sheet_type="物资清单"))]}
        result = native_download_links(
            self.actor,
            {"api_id": "GET /api/critical-guard/tasks/{task_id}",
             "path_params": {"task_id": "t1"}, "params": {"scope": "A"}},
            ok_envelope(raw))
        self.assertTrue(not any(item["url"].endswith("file-1") for item in result))

    def test_source_file_malformed_file_id_rejected(self):
        raw = {"responses": [self._row(scope="A", source_file=self._source(file_id="../x"))]}
        result = native_download_links(
            self.actor,
            {"api_id": "GET /api/critical-guard/tasks/{task_id}",
             "path_params": {"task_id": "t1"}, "params": {"scope": "A"}},
            ok_envelope(raw))
        self.assertTrue(not any(item["url"].endswith("file-1") for item in result))

    def test_source_file_label_scrubs_credentials(self):
        raw = {"responses": [self._row(
            scope="A",
            source_file=self._source(file_name="机密 token=sk-abc123456 清单.xlsx"))]}
        result = native_download_links(
            self.actor,
            {"api_id": "GET /api/critical-guard/tasks/{task_id}",
             "path_params": {"task_id": "t1"}, "params": {"scope": "A"}},
            ok_envelope(raw))
        source = next(item for item in result if item["url"].endswith("file-1"))
        self.assertNotIn("sk-", source["name"])

    def test_composed_image_label_sanitizes_sheet_type_and_check_date(self):
        # safe_text must be applied across the complete composed label even
        # when an API key leaks into sheet_type or check_date.
        raw = {"responses": [self._row(
            scope="A", response_id="resp-1",
            sheet_type="检查表 sk-abc123456789", check_date="2026-10-03")]}
        result = native_download_links(
            self.actor,
            {"api_id": "GET /api/critical-guard/tasks/{task_id}",
             "path_params": {"task_id": "t1"}, "params": {"scope": "A"}},
            ok_envelope(raw))
        image = next(item for item in result if item["url"].endswith("images/resp-1"))
        self.assertNotIn("sk-", image["name"])

    def test_post_source_files_returns_public_response(self):
        raw = self._row(scope="A", source_file=self._source(scope="A"))
        result = native_download_links(
            self.actor,
            {"api_id": "POST /api/critical-guard/source-files",
             "body": {"scope": "A", "response_id": "resp-1"}},
            ok_envelope(raw))
        self.assertEqual(
            [item["url"] for item in result],
            ["/api/critical-guard/images/resp-1",
             "/api/critical-guard/workbooks/resp-1",
             "/api/critical-guard/source-files/file-1"],
        )

    def test_post_source_files_body_scope_conflict_rejected(self):
        raw = self._row(scope="A", source_file=self._source(scope="A"))
        result = native_download_links(
            self.actor,
            {"api_id": "POST /api/critical-guard/source-files",
             "body": {"scope": "C", "response_id": "resp-1"}},
            ok_envelope(raw))
        self.assertEqual(result, [])

    def test_has_image_or_workbook_false_gives_no_native_url(self):
        row = self._row(has_image=False, has_workbook=False)
        raw = {"responses": [row]}
        result = native_download_links(
            self.actor,
            {"api_id": "GET /api/critical-guard/tasks/{task_id}",
             "path_params": {"task_id": "t1"}, "params": {"scope": "A"}},
            ok_envelope(raw))
        self.assertEqual(result, [])

    def test_malformed_response_id_not_link(self):
        row = self._row(response_id="a/../b")
        raw = {"responses": [row]}
        result = native_download_links(
            self.actor,
            {"api_id": "GET /api/critical-guard/tasks/{task_id}",
             "path_params": {"task_id": "t1"}, "params": {"scope": "A"}},
            ok_envelope(raw))
        self.assertEqual(result, [])


class MorningLinkTests(unittest.TestCase):
    def test_generate_requires_H_scope_and_canonical_date(self):
        actor = {"id": "u", "scopes": ["H"]}
        op = {"api_id": "POST /api/daily-tasks/morning-meeting/generate",
              "body": {"date": "2026-10-03"}}
        raw = {"date": "2026-10-03",
               "download_url": "/api/daily-tasks/morning-meeting/download?date=2026-10-03"}
        result = native_download_links(actor, op, ok_envelope(raw))
        self.assertEqual(result, [{
            "name": "晨会表-2026-10-03",
            "url": "/api/daily-tasks/morning-meeting/download?date=2026-10-03",
        }])

    def test_non_H_scope_rejected(self):
        actor = {"id": "u", "scopes": ["A"]}
        op = {"api_id": "POST /api/daily-tasks/morning-meeting/generate",
              "body": {"date": "2026-10-03"}}
        raw = {"date": "2026-10-03",
               "download_url": "/api/daily-tasks/morning-meeting/download?date=2026-10-03"}
        self.assertEqual(native_download_links(actor, op, ok_envelope(raw)), [])

    def test_upstream_url_never_accepted(self):
        actor = {"id": "u", "scopes": ["H"]}
        op = {"api_id": "POST /api/daily-tasks/morning-meeting/generate",
              "body": {"date": "2026-10-03"}}
        raw = {"date": "2026-10-03",
               "download_url": "https://evil.example/download?date=2026-10-03"}
        self.assertEqual(native_download_links(actor, op, ok_envelope(raw)), [])

    def test_invalid_date_not_linked(self):
        actor = {"id": "u", "scopes": ["H"]}
        for bad in ("2026-13-40", "2026/10/03", "today", ""):
            op = {"api_id": "POST /api/daily-tasks/morning-meeting/generate",
                  "body": {"date": bad}}
            raw = {"date": bad,
                   "download_url": f"/api/daily-tasks/morning-meeting/download?date={bad}"}
            self.assertEqual(native_download_links(actor, op, ok_envelope(raw)), [])

    def test_response_date_must_equal_request_date(self):
        actor = {"id": "u", "scopes": ["H"]}
        op = {"api_id": "POST /api/daily-tasks/morning-meeting/generate",
              "body": {"date": "2026-10-03"}}
        raw = {"date": "2026-10-04",
               "download_url": "/api/daily-tasks/morning-meeting/download?date=2026-10-03"}
        self.assertEqual(native_download_links(actor, op, ok_envelope(raw)), [])

    def test_invalid_supplied_date_never_falls_back_to_response(self):
        # A supplied-but-invalid request date must be rejected, not silently
        # repaired to the response's (valid) canonical date.
        actor = {"id": "u", "scopes": ["H"]}
        op = {"api_id": "GET /api/daily-tasks/morning-meeting/preview",
              "params": {"date": "2026-13-40"}, "body": {}}
        raw = {"date": "2026-10-03",
               "download_url": "/api/daily-tasks/morning-meeting/download?date=2026-10-03"}
        self.assertEqual(native_download_links(actor, op, ok_envelope(raw)), [])

    def test_conflicting_body_and_query_dates_rejected(self):
        actor = {"id": "u", "scopes": ["H"]}
        op = {"api_id": "GET /api/daily-tasks/morning-meeting/preview",
              "params": {"date": "2026-10-04"},
              "body": {"date": "2026-10-03"}}
        raw = {"date": "2026-10-03",
               "download_url": "/api/daily-tasks/morning-meeting/download?date=2026-10-03"}
        self.assertEqual(native_download_links(actor, op, ok_envelope(raw)), [])

    def test_response_canonical_date_required_without_request_date(self):
        # When no request date is supplied, the response must still carry the
        # canonical date; an invalid response date must never produce a link.
        actor = {"id": "u", "scopes": ["H"]}
        op = {"api_id": "GET /api/daily-tasks/morning-meeting/preview",
              "params": {}, "body": {}}
        for bad in ("2026/10/03", "today", "2026-13-40"):
            raw = {"date": bad, "download_url": f"/api/daily-tasks/morning-meeting/download?date={bad}"}
            self.assertEqual(native_download_links(actor, op, ok_envelope(raw)), [])

    def test_response_canonical_date_required_with_request_date(self):
        # A present request date still requires a valid canonical response date.
        actor = {"id": "u", "scopes": ["H"]}
        op = {"api_id": "POST /api/daily-tasks/morning-meeting/generate",
              "body": {"date": "2026-10-03"}}
        raw = {"date": "today",
               "download_url": "/api/daily-tasks/morning-meeting/download?date=2026-10-03"}
        self.assertEqual(native_download_links(actor, op, ok_envelope(raw)), [])

    def test_preview_defaults_today_from_response_date(self):
        actor = {"id": "u", "scopes": ["H"]}
        # Preview without an explicit request date uses the response's date.
        op = {"api_id": "GET /api/daily-tasks/morning-meeting/preview",
              "params": {}, "body": {}}
        raw = {"date": "2026-10-03",
               "download_url": "/api/daily-tasks/morning-meeting/download?date=2026-10-03"}
        result = native_download_links(actor, op, ok_envelope(raw))
        self.assertEqual(result, [{
            "name": "晨会表-2026-10-03",
            "url": "/api/daily-tasks/morning-meeting/download?date=2026-10-03",
        }])

    def test_preview_with_request_date_mismatch_rejected(self):
        actor = {"id": "u", "scopes": ["H"]}
        op = {"api_id": "GET /api/daily-tasks/morning-meeting/preview",
              "params": {"date": "2026-10-03"}, "body": {}}
        raw = {"date": "2026-10-04",
               "download_url": "/api/daily-tasks/morning-meeting/download?date=2026-10-03"}
        self.assertEqual(native_download_links(actor, op, ok_envelope(raw)), [])

    def test_exact_canonical_download_url_required(self):
        actor = {"id": "u", "scopes": ["H"]}
        op = {"api_id": "GET /api/daily-tasks/morning-meeting/preview",
              "params": {}, "body": {}}
        raw = {"date": "2026-10-03",
               "download_url": "/api/daily-tasks/morning-meeting/download?date=2026-10-03/extra"}
        self.assertEqual(native_download_links(actor, op, ok_envelope(raw)), [])


class NativeShapeAndSafetyTests(unittest.TestCase):
    def test_unknown_api_id_ignored(self):
        actor = {"id": "u", "scopes": list("ABCDE")}
        result = native_download_links(
            actor,
            {"api_id": "GET /api/settings/version"},
            ok_envelope({"items": [{"export_id": "x"}]}))
        self.assertEqual(result, [])

    def test_no_legacy_export_direct_listing(self):
        actor = {"id": "u", "scopes": list("ABCDE")}
        history = {"items": [{
            "export_id": "old-" + "0" * 12, "scope": "A", "filename": "old.xlsm",
            "status": "succeeded", "deleted": False, "file_available": False}]}
        result = native_download_links(
            actor, {"api_id": "GET /api/cabinet-power/export-history", "params": {"scope": "A"}},
            ok_envelope(history))
        self.assertEqual(result, [])

    def test_missing_native_route_never_expands_speculative_shape(self):
        # A speculative GET /api/critical-guard/source-files listing must not link.
        actor = {"id": "u", "scopes": ["A"]}
        raw = {"files": [{"file_id": "list-file-1", "scope": "A", "filename": "A清单.xlsx"}]}
        result = native_download_links(
            actor,
            {"api_id": "GET /api/critical-guard/source-files", "params": {}},
            ok_envelope(raw))
        self.assertEqual(result, [])

    def test_does_not_scan_arbitrary_recursive_response_urls(self):
        actor = {"id": "u", "scopes": ["A"]}
        raw = {"responses": [
            {"scope": "B", "response_id": "r1", "has_image": True,
             "has_workbook": True,
             "source_file": {"file_id": "f1", "scope": "B", "filename": "b.xlsx"}},
            {"scope": "A", "response_id": "r2", "has_image": True,
             "has_workbook": True},
        ]}
        result = native_download_links(
            actor, {"api_id": "GET /api/critical-guard/tasks/{task_id}",
                    "path_params": {"task_id": "t1"}}, ok_envelope(raw))
        self.assertEqual(
            [item["url"] for item in result],
            ["/api/critical-guard/images/r2", "/api/critical-guard/workbooks/r2"],
        )

    def test_unique_and_all_valid_rows_returned_no_truncation(self):
        actor = {"id": "u", "scopes": list("ABCDE")}
        batch = {"items": {
            scope: {"scope": scope, "status": "succeeded",
                     "result": {"export_id": "e" + scope, "scope": scope,
                                "filename": f"{scope}.xlsm"}}
            for scope in "ABCDE"}}
        links = native_download_links(
            actor,
            {"api_id": "GET /api/cabinet-power/export-batches/{batch_id}",
             "path_params": {"batch_id": "b"}},
            ok_envelope(batch))
        self.assertEqual(len(links), 5)
        urls = [item["url"] for item in links]
        self.assertEqual(len(set(urls)), len(urls))
        self.assertEqual(urls, [f"/api/cabinet-power/exports/e{c}/download" for c in "ABCDE"])


if __name__ == "__main__":
    unittest.main()