import copy
import datetime as dt
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lan_bitable_template_portal import planned_notices as planned
from lan_bitable_template_portal.portal_service import MaintenancePortalService, PortalError
from lan_bitable_template_portal.state_store import LanPortalStateStore

_WORKBENCH_HTML_CACHE = None
_AUTOTIME_BLOCK_CACHE = None


def _auto_time_block():
    """Return the production auto-plan-time JS fragment rendered by workbench_lite.py."""
    global _WORKBENCH_HTML_CACHE, _AUTOTIME_BLOCK_CACHE
    if _AUTOTIME_BLOCK_CACHE is not None:
        return _AUTOTIME_BLOCK_CACHE
    from lan_bitable_template_portal.workbench_lite import render_workbench_lite
    if _WORKBENCH_HTML_CACHE is None:
        _WORKBENCH_HTML_CACHE = render_workbench_lite(
            payload={}, session={"user": {"name": "Fixture"}}, scope="E",
            work_type="maintenance", manual=True)
    script = "\n".join(re.findall(r"<script>(.*?)</script>", _WORKBENCH_HTML_CACHE, re.S))
    marker = ("if (workType === 'maintenance' && !linkedOngoing && action === 'start' "
              "&& draft._auto_plan_times === '1') {")
    idx = script.find(marker)
    if idx < 0:
        raise AssertionError("auto plan time JS fragment not found in rendered workbench script")
    depth = 0
    pos = idx
    while pos < len(script):
        if script[pos] == "{":
            depth += 1
        elif script[pos] == "}":
            depth -= 1
            if depth == 0:
                break
        pos += 1
    _AUTOTIME_BLOCK_CACHE = script[idx:pos + 1]
    return _AUTOTIME_BLOCK_CACHE


def _run_auto_time_node(payload):
    """Run the extracted production JS fragment in Node with a mocked Date.now."""
    node = shutil.which("node")
    if not node:
        raise unittest.SkipTest("node executable not available")
    block = _auto_time_block()
    harness = (
        "const payload = JSON.parse(process.argv[2]);\n"
        "let calls = 0;\n"
        "const seq = Array.isArray(payload.values) ? payload.values : null;\n"
        "const single = Array.isArray(payload.values) ? null : payload.value;\n"
        "Date.now = () => { const v = seq ? seq[Math.min(calls, seq.length - 1)] : single; calls += 1; return v; };\n"
        "const workType = 'maintenance';\n"
        "const linkedOngoing = false;\n"
        "const action = 'start';\n"
        "const draft = {_auto_plan_times: '1'};\n"
        "BLOCK\n"
        "console.log(JSON.stringify({calls, start: draft.start_time, end: draft.end_time}));\n"
    )
    src = harness.replace("BLOCK", block)
    fd, path = tempfile.mkstemp(suffix=".mjs")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(src)
        result = subprocess.run(
            [node, path, json.dumps(payload)], capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            raise AssertionError(
                f"node execution failed (rc={result.returncode}): {result.stderr}\nstdout={result.stdout}")
        return json.loads(result.stdout)
    finally:
        os.remove(path)


def _beijing_now_ms(clock):
    """Date.now mock value whose +8h shift reads as the given Beijing wall clock.

    `clock` is a tuple `(year, month, day, hour, minute, second, microsecond)`.
    """
    year, month, day, hour, minute, second, microsecond = clock
    base = int(dt.datetime(year, month, day, hour, minute, second, microsecond,
                           tzinfo=dt.timezone.utc).timestamp() * 1000)
    return base - 8 * 3600000


class PlannedNativeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.service = MaintenancePortalService()
        self.service._state_store = LanPortalStateStore(Path(self.tmp.name) / "state.sqlite3")
        self.service.ensure_snapshot_loaded = Mock()
        self.service._state_store.get_source_scope_snapshot = Mock(return_value={"exists": True, "meta": {}})
        self.service._source_refresh_status = {kind: {"status": "success"} for kind in ("maintenance", "change", "repair")}
        self.month = self.service._current_month_label()
        self.records = [{"record_id": "source-1", "work_type": "maintenance", "display_fields": {
            "楼栋": "A楼", "维护总项": "冷水机组月度维护", "维护周期": "每月", "维护实施状态": "未开始", "计划维护月份": self.month}}]
        self.service._workbench_records = Mock(side_effect=lambda **_: copy.deepcopy(self.records))
        self.service._work_status_by_records = Mock(return_value={})
        self.service._project_ongoing_items = Mock(side_effect=lambda scope, items: items)
        self.service._target_record_id_from_work_status = Mock(return_value="")

    def prefill(self, **kwargs):
        return planned.prefill(self.service, **{"scope": "A", "month": self.month, "work_type": "maintenance", "source_record_id": "source-1", **kwargs})

    def remember(self, content="历史内容", order=0):
        self.service._remember_draft_fields(building="A楼", maintenance_total="冷水机组月度维护", maintenance_cycle="每月",
            location="历史位置", content=content, reason="按周期维护", impact="无影响", success_order=order,
            extra_fields={"progress": "已完成100%", "execution_party": "厂维", "start_time": "2020-01-01", "target_record_id": "old"})

    def test_prefill_reads_history_but_not_state_or_old_time(self):
        self.remember()
        before = self.service._get_record_memory(self.records[0])
        result = self.prefill()
        self.assertEqual(result["draft"]["execution_party"], "厂维")
        self.assertEqual(result["draft"]["content"], "历史内容")
        self.assertEqual(result["draft"]["progress"], "")
        self.assertNotIn("target_record_id", result["draft"])
        self.assertNotIn("2020", result["draft"]["start_time"])
        self.assertEqual(result["field_sources"]["content"], "history")
        self.assertEqual(before, self.service._get_record_memory(self.records[0]))

    def test_no_memory_fields_remain_blank(self):
        result = self.prefill()
        self.assertEqual(result["draft"]["content"], "")
        self.assertEqual(result["draft"]["execution_party"], "")
        self.assertIn("content", result["missing_fields"])

    def test_current_plan_fact_wins(self):
        self.remember()
        self.records[0]["display_fields"]["内容"] = "本次计划内容"
        self.assertEqual(self.prefill()["draft"]["content"], "本次计划内容")

    def test_complete_pagination_beyond_200(self):
        self.records = [{**copy.deepcopy(self.records[0]), "record_id": f"source-{i:04}"} for i in range(605)]
        pages = [planned.candidate_page(self.service, scope="A", month=self.month, work_type="maintenance", page=i) for i in range(1, 5)]
        self.assertEqual([len(page["items"]) for page in pages], [200, 200, 200, 5])
        self.assertEqual(len({row["source_record_id"] for page in pages for row in page["items"]}), 605)
        self.assertFalse(pages[-1]["has_more"])

    def test_ongoing_and_finished_excluded(self):
        self.assertEqual(planned.candidate_page(self.service, scope="A", month=self.month, work_type="maintenance",
            ongoing_items=[{"source_record_id": "source-1", "work_type": "maintenance"}])["total"], 0)
        self.records[0]["display_fields"]["维护实施状态"] = "已结束"
        self.assertEqual(planned.candidate_page(self.service, scope="A", month=self.month, work_type="maintenance")["total"], 0)

    def test_invisible_sent_source_requires_verification(self):
        self.service._load_work_status_items_locked = Mock(return_value=[{
            "source_record_id": "source-1", "target_record_id": "real-target", "work_type": "maintenance"}])
        with self.assertRaisesRegex(PortalError, "核验"):
            self.prefill()

    def test_incomplete_not_empty(self):
        self.service._source_refresh_status["maintenance"]["status"] = "failed"
        with self.assertRaisesRegex(PortalError, "未完成或失败"):
            self.prefill()

    def test_old_month_event_rejected(self):
        with self.assertRaises(PortalError):
            self.prefill(month="旧月份")
        with self.assertRaises(PortalError):
            self.prefill(work_type="event")

    def test_nonmaintenance_requires_current_month_not_ongoing_old_plan(self):
        current = {"work_type": "repair", "display_fields": {"月份": self.month}}
        self.assertTrue(planned.matches_month(self.service, current, self.month))
        old = {"work_type": "repair", "display_fields": {"月份": "2020-01", "维修状态": "维修中"}}
        self.assertFalse(planned.matches_month(self.service, old, self.month))

    def test_identity_batch_query_does_not_truncate(self):
        for identity in ("source-1", "source-600"):
            self.service._state_store.upsert_notice_identity({"work_type": "maintenance", "source_record_id": identity,
                "target_record_id": "target-" + identity, "active_item_id": "active-" + identity})
        linked = self.service._state_store.notice_sources_with_targets("maintenance", [f"source-{i}" for i in range(700)])
        self.assertEqual(linked, {"source-1", "source-600"})

    def test_version_recheck_and_fixed_facts(self):
        result = self.prefill()
        body = {**result["draft"], "action": "start", "scope": "A", "source_month": self.month,
                "work_type": "maintenance", "source_record_id": "source-1", "planned_notice_version": result["version"]}
        planned.validate_submission(self.service, body)
        with self.assertRaisesRegex(PortalError, "不能在此更换"):
            planned.validate_submission(self.service, {**body, "title": "another"})
        self.records[0]["display_fields"]["内容"] = "changed"
        with self.assertRaisesRegex(PortalError, "已变化"):
            planned.validate_submission(self.service, body)

    def test_cancel_failure_do_not_write_and_success_order(self):
        self.remember("原成功内容", order=10)
        pending = []
        token = planned.deferred_memory.set(pending)
        try:
            self.remember("新内容")
        finally:
            planned.deferred_memory.reset(token)
        self.assertEqual(self.service._get_record_memory(self.records[0])["content"], "原成功内容")
        planned.save_success_memory(self.service, {"accepted_at": 20, "prepared": {"planned_memory": pending}})
        self.assertEqual(self.service._get_record_memory(self.records[0])["content"], "新内容")
        self.remember("迟到内容", order=11)
        self.assertEqual(self.service._get_record_memory(self.records[0])["content"], "新内容")
        self.assertEqual(self.service._get_record_memory(self.records[0])["progress"], "")

    def test_maintenance_rounding(self):
        from lan_bitable_template_portal.workbench_lite import _planned_maintenance_times
        start, end = _planned_maintenance_times(dt.datetime(2026, 10, 8, 15, 28))
        self.assertEqual((start, end), ("2026-10-08T15:30", "2026-10-08T18:30"))

    def test_planned_maintenance_times_boundaries(self):
        from lan_bitable_template_portal.workbench_lite import _planned_maintenance_times
        cases = [
            (dt.datetime(2026, 10, 8, 17, 59), ("2026-10-08T18:00", "2026-10-08T18:30")),
            (dt.datetime(2026, 10, 8, 18, 0), ("2026-10-08T18:00", "2026-10-08T18:30")),
            (dt.datetime(2026, 10, 8, 18, 0, 1), ("2026-10-08T18:30", "2026-10-09T09:00")),
            (dt.datetime(2026, 10, 8, 18, 1), ("2026-10-08T18:30", "2026-10-09T09:00")),
            (dt.datetime(2026, 10, 8, 23, 59), ("2026-10-09T00:00", "2026-10-09T09:00")),
            (dt.datetime(2026, 10, 9, 0, 1), ("2026-10-09T00:30", "2026-10-09T18:30")),
            (dt.datetime(2026, 10, 31, 23, 59), ("2026-11-01T00:00", "2026-11-01T09:00")),
            (dt.datetime(2026, 12, 31, 23, 59), ("2027-01-01T00:00", "2027-01-01T09:00")),
        ]
        for current, expected in cases:
            with self.subTest(current=current):
                self.assertEqual(_planned_maintenance_times(current), expected)

    def test_auto_plan_times_js_single_date_now_boundaries(self):
        from lan_bitable_template_portal.workbench_lite import _planned_maintenance_times
        # The production JS fragment must sample Date.now exactly once and, for the same
        # Beijing wall-clock snapshot, produce output identical to the Python reference.
        clocks = [
            (2026, 10, 8, 17, 59, 0, 0),
            (2026, 10, 8, 18, 0, 0, 0),
            (2026, 10, 8, 18, 0, 0, 1000),   # just past 18:00 -> start next half-hour, end next morning
            (2026, 10, 8, 18, 1, 0, 0),
            (2026, 10, 8, 23, 59, 0, 0),    # midnight
            (2026, 10, 9, 0, 1, 0, 0),
            (2026, 10, 31, 23, 59, 0, 0),   # cross-month
            (2026, 12, 31, 23, 59, 0, 0),   # cross-year
        ]
        for clock in clocks:
            with self.subTest(clock=clock):
                current = dt.datetime(*clock)
                expected = _planned_maintenance_times(current)
                out = _run_auto_time_node({"value": _beijing_now_ms(clock)})
                self.assertEqual(out["calls"], 1,
                    "auto plan time JS must call Date.now exactly once")
                self.assertEqual((out["start"], out["end"]), expected,
                    "JS auto plan time output must match the Python reference")

    def test_auto_plan_times_js_does_not_double_sample_at_18_boundary(self):
        # Regression for the boundary race: previously Date.now was sampled for `now`
        # (18:00:00.000) and again for `start` (18:00:00.001), giving start=18:30 but
        # end=18:30 (inconsistent). The fragment must sample once and derive both from it.
        snap0 = _beijing_now_ms((2026, 10, 8, 18, 0, 0, 0))
        snap1 = _beijing_now_ms((2026, 10, 8, 18, 0, 0, 1000))
        out = _run_auto_time_node({"values": [snap0, snap1]})
        self.assertEqual(out["calls"], 1,
            "the fragment must not double-sample Date.now even when successive reads differ")
        # Only the first (single) sample is consumed, so both fields come from 18:00:00.000.
        self.assertEqual((out["start"], out["end"]),
                         ("2026-10-08T18:00", "2026-10-08T18:30"))

    def submission(self):
        result = self.prefill()
        return {"command_format": "notice_command", "scope": "A", "source_month": self.month,
            "work_type": "maintenance", "action": "start", "source_record_id": "source-1",
            "planned_notice_version": result["version"], "patch": {**result["draft"], "execution_party": "厂维"}}

    def test_concurrent_clicks_share_native_job_and_failed_submission(self):
        self.service._jobs = {}
        body = self.submission()
        with ThreadPoolExecutor(max_workers=4) as workers:
            results = list(workers.map(lambda i: self.service.create_action_job({**body, "operation_id": f"click-{i}"}), range(4)))
        self.assertEqual(len({row[0] for row in results}), 1)
        self.assertEqual(sum(row[1] for row in results), 1)
        identity = results[0][0]
        self.service.mark_job(identity, phase="failed", error="network timeout", _skip_audit_finish=True)
        reused, started = self.service.create_action_job({**body, "operation_id": "new-click"})
        self.assertEqual(reused, identity)
        self.assertFalse(started)

    def test_prepare_defers_until_native_success(self):
        self.service._jobs = {}
        identity, _ = self.service.create_action_job(self.submission())
        def prepare(*args, **kwargs):
            self.remember("成功后保存")
            return {"work_type": "maintenance", "text": "fixture", "recipients": []}
        self.service.prepare_workbench_action = prepare
        self.service.prepare_action_job(identity)
        self.assertFalse(self.service._get_record_memory(self.records[0])["content"])
        self.service.mark_job(identity, phase="success")
        self.assertEqual(self.service._get_record_memory(self.records[0])["content"], "成功后保存")

    def test_memory_failure_does_not_fail_sent_job(self):
        self.service._jobs = {}
        identity, _ = self.service.create_action_job(self.submission())
        self.service.mark_job(identity, prepared={"planned_memory": [{"bad": "fixture"}]})
        with self.assertLogs(level="ERROR"):
            self.service.mark_job(identity, phase="success")
        saved = self.service.get_job(identity)
        self.assertEqual(saved["phase"], "success")
        self.assertIn("历史记忆未保存", saved["memory_warning"])


if __name__ == "__main__":
    unittest.main()
