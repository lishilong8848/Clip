"""Shared read semantics and bounded pagination; synthetic data only."""
import asyncio
import copy
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_queries import (
    EventQuery, NoticeQuery, collect_events, collect_notice_sends, collect_notices,
    collect_query_pages, query_result_page, query_result_state,
)


def records(count, **extra):
    return [{"record_id": "r%d" % i, "scope": "A", **extra} for i in range(count)]


def result(data, **extra):
    return {"ok": True, "_raw": data, "data": copy.deepcopy(data), **extra}


class CompleteQueryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.calls = []
        self.rows = records(45)
        self.op = {"api_id": "GET /api/assistant/work-orders", "params": {
            "scope": "A", "state": "active", "work_type": "polling", "search": "fixture", "page_size": 20}}

    async def invoke(self, op):
        self.calls.append(copy.deepcopy(op))
        page = op["params"].get("page", 1)
        start = (page - 1) * 20
        return result({"items": self.rows[start:start + 20], "total": len(self.rows),
                       "page": page, "page_size": 20, "source": "fixture", "updated_at": "2026-10-20T08:00:00+08:00"})

    async def test_complete_lists_keep_sample_distinct_and_preserve_filters(self):
        original = copy.deepcopy(self.op)
        data = await collect_query_pages(self.op, self.invoke)
        self.assertTrue(data["complete"])
        self.assertEqual(len(data["_raw"]["items"]), 45)
        self.assertEqual(len(data["data"]["items"]), 40)
        self.assertTrue(data["sample_limited"])
        summary = data["_raw"]["query_completeness"]["lists"]["items"]
        self.assertEqual((summary["count"], summary["loaded_count"], summary["declared_total"]), (45, 45, 45))
        self.assertEqual(summary["unit"], "work_orders")
        self.assertEqual(query_result_state(data)[0], "found")
        self.assertEqual([op["params"]["page"] for op in self.calls], [1, 2, 3])
        for op in self.calls:
            self.assertEqual({k: v for k, v in op["params"].items() if k != "page"}, original["params"])
        self.assertEqual(self.op, original)
        self.assertEqual(query_result_page(data["_raw"], ["items"], offset=40)["loaded_count"], 45)
        evidence = data["_raw"]["query_completeness"]["source_freshness"][0]
        self.assertEqual(evidence["updated_at"], "2026-10-20T08:00:00+08:00")
        self.assertEqual(evidence["requested_scope"], "A")
        self.assertIn("queried_at", evidence)

    async def test_supplied_result_only_fetches_missing_pages_even_when_it_is_page_two(self):
        for supplied_page in (1, 2):
            with self.subTest(supplied_page=supplied_page):
                self.calls.clear()
                operation = copy.deepcopy(self.op)
                operation["params"]["page"] = supplied_page
                first = await self.invoke(operation)
                frozen = copy.deepcopy(first)
                self.calls.clear()
                data = await collect_query_pages(operation, self.invoke, first_result=first)
                self.assertEqual([op["params"]["page"] for op in self.calls], [p for p in (1, 2, 3) if p != supplied_page])
                self.assertEqual(len(data["_raw"]["items"]), 45)
                self.assertEqual(data["_raw"]["query_completeness"]["pages_read"], 3)
                self.assertEqual(first, frozen)

    async def test_empty_valid_list_does_not_refetch(self):
        self.rows = []
        data = await collect_query_pages(self.op, self.invoke)
        self.assertTrue(data["complete"])
        self.assertEqual(query_result_state(data)[0], "empty")
        self.assertEqual(len(self.calls), 1)
        for flag in ("source_snapshot_ready", "snapshot_exists"):
            async def missing(op):
                return result({"items": [], "total": 0, flag: False})
            data = await collect_query_pages(self.op, missing)
            self.assertFalse(data["complete"])
            self.assertNotEqual(query_result_state(data)[0], "empty")

    async def test_limits_leave_total_unknown_without_counting_sample_as_full(self):
        for kwargs, known, calls in (({"max_pages": 1}, 20, 1), ({"max_items": 25}, 25, 2)):
            with self.subTest(kwargs=kwargs):
                self.calls.clear()
                data = await collect_query_pages(self.op, self.invoke, **kwargs)
                self.assertFalse(data["complete"])
                summary = data["_raw"]["query_completeness"]["lists"]["items"]
                self.assertEqual(summary["loaded_count"], known)
                self.assertIsNone(summary["count"])
                self.assertEqual(summary["declared_total"], 45)
                self.assertEqual(query_result_state(data)[0], "partial")
                self.assertEqual(len(self.calls), calls)

    async def test_no_cross_request_or_cross_user_cache(self):
        await collect_query_pages(self.op, self.invoke)
        self.calls.clear()
        self.rows = records(1, title="second actor")
        data = await collect_query_pages(self.op, self.invoke)
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(data["_raw"]["items"][0]["title"], "second actor")

    async def test_scoped_callback_runs_each_page_and_denials_discard_earlier_data(self):
        for status in (401, 403):
            for raises in (False, True):
                async def denied(op):
                    if op["params"]["page"] > 1:
                        if raises:
                            raise AssistantError("fixture permission changed", status)
                        return {"ok": False, "status": status, "error": "fixture permission changed"}
                    return await self.invoke(op)
                data = await collect_query_pages(self.op, denied)
                self.assertFalse(data["ok"])
                self.assertEqual(data["status"], status)
                self.assertIsNone(data["data"])
                self.assertNotIn("_raw", data)

    async def test_bad_pages_do_not_claim_complete_or_loop(self):
        cases = (
            {"total": 46}, {"total": True}, {"total": -1}, {"total": None},
            {"page": 1}, {"page_size": 19}, {"has_more": False}, {"has_more": "false"},
            {"items": []}, {"items": records(20)}, {"updated_at": "changed"}, {"next_offset": 999},
            {"offset": 99},
            {"complete": False}, {"warnings": ["source retained"]}, {"source_snapshot_ready": False},
        )
        for changes in cases:
            with self.subTest(changes=changes):
                self.calls.clear()
                async def broken(op):
                    data = await self.invoke(op)
                    if op["params"]["page"] == 2:
                        data["_raw"].update(changes)
                    return data
                data = await collect_query_pages(self.op, broken)
                self.assertFalse(data["complete"])
                self.assertEqual(len(self.calls), 2)
                self.assertEqual(len(data["_raw"]["items"]), 20)

    async def test_final_raw_markers_describe_aggregate_not_the_first_native_page(self):
        async def invoke(op):
            data = await self.invoke(op)
            page = op["params"]["page"]
            data["_raw"].update(complete=True, has_more=page < 3,
                                next_offset=page * 20 if page < 3 else None, remaining=max(0, 45 - page * 20))
            return data
        for kwargs, complete in (({}, True), ({"max_pages": 1}, False)):
            with self.subTest(complete=complete):
                data = await collect_query_pages(self.op, invoke, **kwargs)
                raw = data["_raw"]
                self.assertEqual(raw["complete"], complete)
                self.assertEqual(raw["has_more"], not complete)
                self.assertEqual(raw["page"], 1)
                self.assertEqual(raw["remaining"], 0 if complete else 25)
                self.assertEqual(raw["next_offset"], None if complete else 20)
                declaration = raw["query_completeness"]["lists"]["items"]["declared_pagination"]
                self.assertTrue(declaration["has_more"])
                self.assertEqual(declaration["next_offset"], 20)
                self.assertTrue(data["ok"])
                self.assertTrue(data["available"])
                self.assertTrue(raw["available"])

    async def test_first_failure_is_unavailable_and_later_failure_is_partial(self):
        for first in (True, False):
            async def invoke(op):
                if first or op["params"]["page"] == 2:
                    return {"ok": False, "status": 503, "error": "fixture failure"}
                return await self.invoke(op)
            data = await collect_query_pages(self.op, invoke)
            self.assertEqual(data["ok"], not first)
            self.assertEqual(data["available"], not first)
            self.assertFalse(data["complete"])
            self.assertFalse(data["_raw"]["complete"])
            self.assertEqual(data["_raw"]["available"], not first)
            self.assertTrue(data["_raw"]["has_more"])
            self.assertIsNone(data["_raw"]["query_completeness"]["lists"]["items"]["count"])
            self.assertEqual(query_result_state(data)[0], "unavailable" if first else "partial")

    async def test_unavailable_or_incomplete_source_never_becomes_complete_zero(self):
        for changes in ({"available": False}, {"complete": False}):
            for on_envelope in (False, True):
                async def invoke(op):
                    data = result({"items": [], "total": 0})
                    data.update(changes) if on_envelope else data["_raw"].update(changes)
                    return data
                data = await collect_query_pages(self.op, invoke)
                self.assertFalse(data["ok"])
                self.assertFalse(data["available"])
                self.assertFalse(data["_raw"]["complete"])

    async def test_page_position_and_size_validation_happen_before_reads(self):
        for params in ({"page": False}, {"page": "0"}, {"page": "next"}, {"page_size": 0}, {"page_size": True}):
            with self.assertRaises(AssistantError):
                await collect_query_pages({**self.op, "params": {**self.op["params"], **params}}, self.invoke)
        self.assertEqual(self.calls, [])

    async def test_timeout_preserves_known_rows_and_does_not_retry(self):
        async def slow(op):
            if op["params"]["page"] == 2:
                await asyncio.sleep(1)
            return await self.invoke(op)
        data = await collect_query_pages(self.op, slow, timeout=.02)
        self.assertFalse(data["complete"])
        self.assertEqual(len(data["_raw"]["items"]), 20)
        self.assertEqual(len(self.calls), 1)

    async def test_offset_schema_uses_actual_rows_as_offset(self):
        async def invoke(op):
            self.calls.append(op)
            start = op["params"]["offset"]
            return result({"records": self.rows[start:start + 20], "total": 45, "offset": start, "limit": 20})
        data = await collect_query_pages({"api_id": "GET /api/repair-management/records", "params": {
            "scope": "A", "query": "fixture", "state": "active", "period": "all", "limit": 20}}, invoke)
        self.assertTrue(data["complete"])
        self.assertEqual([op["params"]["offset"] for op in self.calls], [0, 20, 40])

    async def test_workbench_sections_reuse_first_result_then_fetch_only_missing_section(self):
        first = result({"records": records(3), "ongoing": records(2, work_type="polling"),
                        "records_pagination": {"total": 3, "page": 1, "page_size": 3},
                        "ongoing_pagination": {"total": 3, "page": 1, "page_size": 2},
                        "source_snapshot_ready": True, "payload_version": "fixture-v1"})
        async def invoke(op):
            self.calls.append(op)
            self.assertEqual(op["params"]["sections"], "ongoing")
            return result({"ongoing": [{"record_id": "r2", "scope": "A", "work_type": "polling"}],
                           "ongoing_pagination": {"total": 3, "page": 2, "page_size": 2},
                           "source_snapshot_ready": True, "payload_version": "fixture-v1"})
        data = await collect_query_pages({"api_id": "GET /api/workbench", "params": {"scope": "A"}}, invoke, first_result=first)
        self.assertTrue(data["complete"])
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(len(data["_raw"]["ongoing"]), 3)
        self.assertEqual(len(data["_raw"]["records"]), 3)
        for name in ("records_pagination", "ongoing_pagination"):
            self.assertTrue(data["_raw"][name]["complete"])
            self.assertFalse(data["_raw"][name]["has_more"])
            self.assertIsNone(data["_raw"][name]["next_offset"])

    async def test_already_loaded_empty_section_does_not_consume_more_item_budget(self):
        first = result({"records": records(1), "ongoing": [], "source_snapshot_ready": True,
                        "records_pagination": {"total": 1, "page": 1, "page_size": 1},
                        "ongoing_pagination": {"total": 0, "page": 1, "page_size": 0}})
        async def forbidden(op):
            self.fail("No missing page should be retrieved")
        data = await collect_query_pages({"api_id": "GET /api/workbench"}, forbidden, first_result=first, max_items=1)
        self.assertTrue(data["complete"])

    async def test_unrequested_workbench_section_does_not_appear_as_an_empty_complete_list(self):
        async def invoke(op):
            return result({"records": [], "records_pagination": {"total": 99, "has_more": True},
                           "ongoing": records(1), "ongoing_pagination": {"total": 1, "page": 1, "page_size": 40},
                           "count": 99, "source_snapshot_ready": True})
        data = await collect_query_pages({"api_id": "GET /api/workbench", "params": {"sections": "ongoing"}}, invoke)
        self.assertTrue(data["complete"])
        self.assertNotIn("records", data["_raw"])
        self.assertNotIn("records_pagination", data["_raw"])
        self.assertEqual(data["_raw"]["count"], 1)
        self.assertEqual(data["_raw"]["query_completeness"]["declared_count"], 99)

    async def test_warning_keeps_source_time_without_inventing_complete_zero(self):
        async def invoke(op):
            return result({"items": [], "total": 0, "warnings": ["fixture retained"],
                           "updated_at": "2026-10-19T08:00:00+08:00"})
        data = await collect_query_pages(self.op, invoke)
        self.assertFalse(data["complete"])
        source = data["_raw"]["query_completeness"]["source_freshness"][0]
        self.assertEqual(source["updated_at"], "2026-10-19T08:00:00+08:00")
        self.assertEqual(source["requested_scope"], "A")

    def test_native_confirmed_empty_missing_and_history_units_are_distinct(self):
        self.assertEqual(query_result_state(result({"items": [], "complete": True, "total": 0}))[0], "empty")
        self.assertEqual(query_result_state(result({"items": []}))[0], "empty_page")
        self.assertEqual(query_result_state(result({"items": [], "snapshot_exists": False}))[0], "unavailable")
        self.assertEqual(query_result_state(result({"items": [], "available": False}))[0], "unavailable")
        self.assertEqual(query_result_state(result({"items": [], "warnings": ["retained"]}))[0], "partial")

    async def test_learning_totals_remain_papers_and_question_versions_stay_distinct(self):
        async def history(op):
            return result({"items": [{"id": "paper-one", "person_id": "same", "state": "pending"},
                                     {"id": "paper-two", "person_id": "same", "state": "completed"}],
                           "total": 2, "page": 1, "page_size": 20})
        data = await collect_query_pages({"api_id": "GET /api/learning/history"}, history)
        self.assertEqual(data["_raw"]["query_completeness"]["lists"]["items"]["unit"], "papers")
        self.assertIn("view=participation", query_result_state(data)[1])
        data["complete"] = False
        self.assertIn("view=participation", query_result_state(data)[1])
        async def questions(op):
            return result({"items": [{"id": "q1", "version": 1}, {"id": "q1", "version": 2}], "total": 2})
        data = await collect_query_pages({"api_id": "GET /api/assistant/question-bank"}, questions)
        self.assertTrue(data["complete"])
        self.assertEqual(data["_raw"]["query_completeness"]["lists"]["items"]["unit"], "question_versions")

    async def test_truncated_public_copy_cannot_validate_as_full_evidence(self):
        data = await collect_query_pages(self.op, self.invoke)
        public = {"ok": True, "data": data["data"]}
        self.assertEqual(query_result_state(public)[0], "partial")
        data["_raw"]["query_completeness"]["lists"] = {"items": "invalid"}
        self.assertEqual(query_result_state(data)[0], "partial")

    async def test_write_refresh_and_unknown_pagination_are_rejected_before_invoke(self):
        for op in ({"api_id": "POST /api/workbench-actions"}, {"api_id": "GET /api/admin/refresh"},
                   {"api_id": "GET /api/workbench", "params": {"force_refresh": "1"}},
                   {"api_id": self.op["api_id"], "body": {"admin": True}}):
            with self.assertRaises(AssistantError):
                await collect_query_pages(op, self.invoke)
        for kwargs in ({"max_pages": 21}, {"max_pages": True}, {"max_items": 2001}, {"timeout": 36}, {"timeout": float("nan")}):
            with self.assertRaises(AssistantError):
                await collect_query_pages(self.op, self.invoke, **kwargs)
        self.assertEqual(self.calls, [])


class NoticeSemanticsTests(unittest.IsolatedAsyncioTestCase):
    def test_typed_status_date_boundary(self):
        for values in ({"status": "sent"}, {"status": "planned", "start_date": "2026-10-20"},
                       {"status": "open", "end_date": "2026-10-20"},
                       {"status": "sent", "start_date": "2026-10-21", "end_date": "2026-10-20"},
                       {"status": "open", "work_types": ["event"]}, {"status": "open", "refresh": True}):
            with self.assertRaises(ValueError):
                NoticeQuery(**values)

    async def test_distinct_notice_count_is_not_send_attempts(self):
        calls = []
        async def invoke(op):
            calls.append(op)
            self.assertEqual(op["api_id"], "GET /api/history-summary")
            return result({"last_loaded_at": "fixture-source-time", "days": [{"date": "2026-10-20", "items": [{
                "record_id": "n1", "scope": "A", "work_type": "maintenance", "actions": [
                    {"action": "start", "job_id": "s1", "time": "2026-10-20 08:00"},
                    {"action": "update", "job_id": "s2", "time": "2026-10-20 09:00"}]}]}]})
        data = await collect_notices({"id": "fixture", "scopes": ["A"], "modules": ["maintenance"]},
            NoticeQuery(status="sent", start_date="2026-10-20", end_date="2026-10-20"), invoke)
        self.assertEqual((data["count"], data["send_count"]), (1, 2))
        self.assertEqual(data["date_basis"], "action.time")
        self.assertEqual(data["source_freshness"][0]["last_loaded_at"], "fixture-source-time")
        self.assertEqual(len(calls), 1)

    async def test_module_scope_grants_are_paired_and_denied_before_read(self):
        calls = []
        async def invoke(op):
            calls.append(op)
            return result({"days": []})
        actor = {"scopes": ["A", "B"], "modules": ["maintenance"], "_topic_capabilities": [
            {"modules": ["maintenance"], "scopes": ["A"]}, {"modules": ["change"], "scopes": ["B"]}]}
        query = NoticeQuery(status="sent", start_date="2026-10-20", end_date="2026-10-20")
        data = await collect_notices(actor, query, invoke)
        self.assertEqual([(op["params"]["scope"]) for op in calls], ["A", "B"])
        self.assertEqual(data["count"], 0)
        calls.clear()
        with self.assertRaises(AssistantError):
            await collect_notices({"scopes": ["A"], "modules": ["maintenance"]},
                                  NoticeQuery(status="open", work_types=["change"]), invoke)
        self.assertEqual(calls, [])

    async def test_current_open_reuses_native_collector_and_reads_only_granted_type(self):
        async def invoke(op):
            self.assertEqual(op["params"]["work_type"], "maintenance")
            self.assertEqual(op["params"]["sections"], "ongoing")
            return result({"source_snapshot_ready": True, "ongoing": records(1, work_type="maintenance", status="进行中"),
                           "ongoing_pagination": {"total": 1}})
        data = await collect_notices({"scopes": ["A"], "modules": ["maintenance"]}, NoticeQuery(status="open"), invoke)
        self.assertEqual(data["count"], 1)
        self.assertEqual(data["date_basis"], "current_status")
        self.assertIsNone(data["send_count"])

    async def test_planned_status_uses_existing_unsent_source_rules(self):
        calls = []
        async def invoke(op):
            calls.append(op)
            key = "records" if op["params"]["sections"] == "records" else "ongoing"
            rows = records(1, work_type="maintenance", source_progress="未开始") if key == "records" else []
            return result({key: rows, key + "_pagination": {"total": len(rows)}, "source_snapshot_ready": True})
        data = await collect_notices({"scopes": ["A"], "modules": ["maintenance"]}, NoticeQuery(status="planned"), invoke)
        self.assertTrue(data["complete"])
        self.assertEqual(data["count"], 1)
        self.assertEqual({op["params"]["sections"] for op in calls}, {"records", "ongoing"})

    async def test_history_source_failure_or_warning_is_unknown_not_zero(self):
        for changes in ({"complete": False}, {"source_snapshot_ready": False}, {"warnings": ["fixture retained"]}):
            async def invoke(op):
                return result({"days": [], **changes})
            data = await collect_notices({"scopes": ["A"], "modules": ["maintenance"]},
                NoticeQuery(status="sent", start_date="2026-10-20", end_date="2026-10-20"), invoke)
            self.assertFalse(data["complete"])
            self.assertIsNone(data["count"])
            self.assertIsNone(data["send_count"])

    async def test_notice_permissions_revoked_between_modules_discard_results(self):
        async def invoke(op):
            if op["params"]["scope"] == "B":
                raise AssistantError("fixture revoke", 403)
            return result({"days": []})
        with self.assertRaises(AssistantError):
            await collect_notices({"scopes": ["A", "B"], "modules": ["maintenance"]},
                NoticeQuery(status="sent", start_date="2026-10-20", end_date="2026-10-20"), invoke)


class NativeRetentionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.actor = {"scopes": ["A"], "modules": ["maintenance"]}
        self.calls = []
        self.count = 45

    async def invoke(self, op):
        self.calls.append(copy.deepcopy(op))
        if op["api_id"] == "GET /api/events/monthly":
            return result({"snapshot_exists": True, "last_refreshed_at": 123,
                           "records": records(self.count, status="处理中", occurrence_time="2026-10-20 08:00")})
        if op["api_id"] == "GET /api/history-summary":
            rows = records(self.count, work_type="maintenance")
            for row in rows:
                row.update(title=row["record_id"], actions=[{"action": "start", "job_id": row["record_id"], "time": "2026-10-20 08:00"}])
            return result({"days": [{"date": "2026-10-20", "items": rows}]})
        self.assertEqual(op["api_id"], "GET /api/workbench")
        key = op["params"]["sections"]
        rows = records(self.count, work_type="maintenance", source_progress="未开始", status="进行中")
        if key == "ongoing" and self.status == "planned":
            rows = []
        page = op["params"][key + "_page"]
        return result({key: rows[(page - 1) * 200:page * 200], key + "_pagination": {"total": len(rows)},
                       "source_snapshot_ready": True})

    async def test_event_tail_is_readable_without_more_fetches_or_count_changes(self):
        query = EventQuery(start_date="2026-10-20", end_date="2026-10-20")
        sample = await collect_events(self.actor, query, self.invoke)
        self.assertEqual((sample["count"], len(sample["items"]), sample["remaining"]), (45, 20, 25))
        self.assertTrue(sample["complete"])
        self.assertFalse(sample["list_complete"])
        full = await collect_events(self.actor, query, self.invoke, item_limit=2000)
        self.assertEqual((full["count"], len(full["items"]), full["remaining"]), (45, 45, 0))
        self.assertTrue(full["complete"])
        self.assertTrue(full["list_complete"])
        tail = query_result_page(full, ["items"], offset=40)
        self.assertEqual(len(tail["items"]), 5)
        self.assertEqual(tail["loaded_count"], 45)
        self.assertEqual(len(self.calls), 2)
        self.assertEqual(self.calls[0], self.calls[1])

    async def test_notice_send_tail_is_readable_and_convenience_retains_it(self):
        sample = await collect_notice_sends(self.actor, "2026-10-20", "2026-10-20", self.invoke)
        self.assertEqual((sample["count"], len(sample["items"]), sample["remaining"]), (45, 20, 25))
        self.assertFalse(sample["list_complete"])
        full = await collect_notice_sends(self.actor, "2026-10-20", "2026-10-20", self.invoke, item_limit=2000)
        self.assertEqual((full["count"], len(full["items"]), full["remaining"]), (45, 45, 0))
        self.assertTrue(full["list_complete"])
        self.assertEqual(query_result_page(full, ["items"], offset=40)["items"][0]["title"], "r40")
        self.assertEqual(self.calls[0], self.calls[1])
        self.assertEqual(len(self.calls), 2)
        full = await collect_notices(self.actor, NoticeQuery(status="sent", start_date="2026-10-20", end_date="2026-10-20"), self.invoke)
        self.assertEqual(len(full["groups"][0]["items"]), 45)
        self.assertTrue(full["list_complete"])
        self.assertEqual(full["remaining"], 0)
        self.assertEqual(len(self.calls), 3)

    async def test_retention_caps_do_not_reduce_source_counts(self):
        self.count = 2101
        for status in ("open", "planned", "sent"):
            with self.subTest(status=status):
                self.status = status
                self.calls.clear()
                query = NoticeQuery(status=status, **({"start_date": "2026-10-20", "end_date": "2026-10-20"} if status == "sent" else {}))
                data = await collect_notices(self.actor, query, self.invoke)
                self.assertTrue(data["complete"])
                self.assertEqual(data["count"], 2101)
                self.assertEqual(len(data["groups"][0]["items"]), 2000)
                self.assertEqual(data["remaining"], 101)
                self.assertFalse(data["list_complete"])
                self.assertLess(len(self.calls), 20)

    async def test_native_retention_limit_validation_precedes_reading(self):
        query = EventQuery(start_date="2026-10-20", end_date="2026-10-20")
        for limit in (0, 2001, True, 20.5, "20", None):
            with self.subTest(limit=limit):
                with self.assertRaises(AssistantError):
                    await collect_events(self.actor, query, self.invoke, item_limit=limit)
                with self.assertRaises(AssistantError):
                    await collect_notice_sends(self.actor, "2026-10-20", "2026-10-20", self.invoke, item_limit=limit)
        self.assertEqual(self.calls, [])


if __name__ == "__main__":
    unittest.main()
