"""Bounded PortalAgent integration tests for the web event transfer-repair form.

These tests drive the real ``PortalAgent.prepare / amend / confirm / field_options /
_execute`` against an in-process FastAPI app that fakes ONLY the web event
endpoints (no assistant/LLM mocking, no cloud access):

  * ``GET  /api/events/monthly``        the web monthly snapshot
  * ``POST /api/events/transfer-repair`` the web event->overhaul marker (accepts
                                          EventTransferRepairRequest)

Covered guarantees:

  1. opening scope/month/record dropdowns never writes a transfer;
  2. the record dropdown queries the chosen month with occurrence_time date_field;
  3. options exclude already-transferred / other-building / unknown-scope /
     deleted events;
  4. keyword search and the >200 result partial flag are honoured;
  5. a bound queried record is preselected and the two-confirm + execute flow
     writes the exact original record_id/month/scope exactly once;
  6. month/scope changes without reloading and forged record ids are rejected;
  7. an incomplete snapshot or a read failure is not silently treated as an
     empty success;
  8. returning to edit preserves the chosen record and never duplicates writes;
  9. the notice-channel guard rejects literal/patched/query-reference ``event``
     notices on both workbench-actions aliases before any business write while
     the web event-transfer path stays usable.
"""
import asyncio
import copy
import json
import sys
import tempfile
import unittest
from pathlib import Path
from urllib.parse import quote
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

from fastapi import FastAPI, Request
from clipflow_backend.api_models import EventTransferRepairRequest
from lan_bitable_template_portal.lighthouse_agent import PortalAgent, _EVENT_TRANSFER
from lan_bitable_template_portal.lighthouse_ai import AssistantError, LighthouseAssistant
from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog
from lan_bitable_template_portal.lighthouse_files import LighthouseFiles

ACTOR = {"id": "event-transfer-owner", "scopes": ["A"], "is_admin": False}

MONTH = "2026-10"


def _eligible_event(record_id="rec-ok", title="A楼漏水"):
    return {
        "record_id": record_id,
        "title": title,
        "building_codes": ["A"],
        "occurrence_time": "2026-10-02 10:00",
        "status": "处理中",
        "transfer_to_overhaul": "",
    }


def _snapshot(records, scope="A", month=MONTH):
    return {
        "scope": scope,
        "month": month,
        "date_field": "occurrence_time",
        "records": records,
        "snapshot_exists": True,
    }


class Store:
    def __init__(self, path):
        self.db_path, self.docs = path, {}

    def get_document(self, namespace, key):
        return copy.deepcopy(self.docs.get((namespace, key)))

    def put_document(self, namespace, key, value):
        self.docs[namespace, key] = copy.deepcopy(value)


async def gather_tasks(agent):
    if agent.tasks:
        await asyncio.gather(*tuple(agent.tasks))


class EventTransferWorkflowTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = Store(Path(self.tmp.name) / "state.sqlite3")
        model = Mock()
        model.settings.return_value = {
            "configured": True,
            "enabled": True,
            "active_model_id": "default",
            "models": [{"id": "default", "name": "测试模型", "model": "fixture-model", "configured": True}],
        }
        model.profile.return_value = {"id": "default", "name": "测试模型", "model": "fixture-model"}
        search = Mock(side_effect=AssertionError("agent must use fake native APIs, not local scraping"))
        self.assistant = LighthouseAssistant(self.store, search, model=model)
        self.files = LighthouseFiles(self.store)

        self.transfer_writes = []
        self.monthly_calls = []
        self.monthly_ok = True
        self.notice_writes = []
        self.monthly_payload = _snapshot([_eligible_event()])

        app = FastAPI()

        @app.get("/api/events/monthly")
        async def monthly(scope: str, month: str, date_field: str):
            self.monthly_calls.append({"scope": scope, "month": month, "date_field": date_field})
            if not self.monthly_ok:
                return {"ok": False, "status": 502, "error": "事件月报读取失败"}
            return {"ok": True, "data": copy.deepcopy(self.monthly_payload)}

        @app.post("/api/events/transfer-repair")
        async def transfer_repaired(body: EventTransferRepairRequest):
            self.transfer_writes.append(body.model_dump())
            return {"ok": True, "data": {"record_id": body.record_id}}

        @app.post("/api/workbench-actions")
        async def workbench_actions(payload: dict = {}):
            # The notice-channel guard should reject event notices before this
            # write path is ever reached; record it to catch a regression.
            self.notice_writes.append(payload)
            return {"ok": True, "data": {"accepted": True}}

        @app.post("/api/maintenance-actions")
        async def maintenance_actions(payload: dict = {}):
            self.notice_writes.append(payload)
            return {"ok": True, "data": {"accepted": True}}

        self.catalog = PortalAPICatalog(app)
        self.agent = PortalAgent(self.assistant, self.catalog, self.files)
        self.request = self._make_request()

    def _make_request(self, query=""):
        return Request({
            "type": "http",
            "scheme": "http",
            "server": ("testserver", 80),
            "client": ("127.0.0.1", 123),
            "path": "/api/events/monthly",
            "root_path": "",
            "query_string": quote(query, safe="=&%").encode(),
            "headers": [(b"origin", b"http://testserver"), (b"cookie", b"fixture=a")],
        })

    def _operation_id(self, suffix):
        return "event_transfer_" + suffix + "_" + "x" * 20

    def _transfer_decision(self, body=None):
        return {"operations": [{"api_id": _EVENT_TRANSFER, "body": body if body is not None else {}}]}

    # -- helpers ------------------------------------------------------------

    async def _amend(self, plan, values, actor=ACTOR):
        return self.agent.amend(actor, plan["id"], {"version": plan["version"], "values": values})

    async def _confirm(self, plan, stage, actor=ACTOR):
        return await self.agent.confirm(actor, plan["id"], {"version": plan["version"], "stage": stage}, self.request)

    async def _field_options(self, plan, field_name, query="", actor=ACTOR):
        return await self.agent.field_options(actor, plan["id"], field_name, self._make_request(query))

    def _raw_field(self, plan_id, field_name, actor=ACTOR):
        fields = self.agent.get_plan(actor, plan_id)["fields"]
        return next(f for f in fields if f["name"] == field_name)

    # -- 1) opening dropdowns never writes ----------------------------------

    async def test_opening_scope_month_and_record_dropdowns_never_write(self):
        plan = self.agent.prepare(ACTOR, self._transfer_decision({"scope": "A", "month": MONTH}),
                                  self._operation_id("open"), [])
        self.assertEqual(plan["status"], "needs_input")
        self.assertEqual(self.transfer_writes, [])
        names = {field["name"] for field in plan["fields"]}
        self.assertIn("step0.scope", names)
        self.assertIn("step0.month", names)
        self.assertIn("step0.record_id", names)

        # scope / month selects are static; field_options has no source for them.
        for field_name in ("step0.scope", "step0.month"):
            with self.assertRaises(AssistantError):
                await self._field_options(plan, field_name, "scope=A&month=" + MONTH)
            self.assertEqual(self.transfer_writes, [])
            self.assertEqual(self.monthly_calls, [])

        # record dropdown queries the monthly endpoint (a read).
        loaded = await self._field_options(plan, "step0.record_id", "scope=A&month=" + MONTH)
        self.assertEqual(self.monthly_calls, [{"scope": "A", "month": MONTH, "date_field": "occurrence_time"}])
        self.assertEqual(self.transfer_writes, [])
        field = next(f for f in loaded["fields"] if f["name"] == "step0.record_id")
        self.assertTrue(field.get("native_event_transfer"))
        self.assertEqual([option["value"] for option in field["options"]], ["rec-ok"])

    # -- 2) field_options uses chosen month / date_field --------------------

    async def test_field_options_queries_chosen_month_and_occurrence_date_field(self):
        self.monthly_payload["month"] = "2026-09"
        plan = self.agent.prepare(ACTOR, self._transfer_decision({"scope": "A", "month": "2026-09"}),
                                  self._operation_id("chosen"), [])
        await self._field_options(plan, "step0.record_id", "scope=A&month=2026-09")
        self.assertEqual(self.monthly_calls, [{"scope": "A", "month": "2026-09", "date_field": "occurrence_time"}])

    # -- 3) exclusions ------------------------------------------------------

    async def test_field_options_excludes_transferred_other_building_unknown_scope_deleted(self):
        self.monthly_payload = _snapshot([
            _eligible_event("rec-ok", "A楼漏水"),
            {**_eligible_event("rec-trans", "A楼已转"), "transfer_to_overhaul": "是"},
            {**_eligible_event("rec-trans2", "A楼已转1"), "transfer_to_overhaul": "已转检修"},
            {**_eligible_event("rec-b", "B楼漏水"), "building_codes": ["B"]},
            {**_eligible_event("rec-none", "无楼栋")},
            {**_eligible_event("rec-deleted", "A楼已删除"), "deleted_at": "2026-10-01 10:00"},
        ])
        # The unknown-scope record carries no scope/building fields at all.
        self.monthly_payload["records"][4].pop("building_codes")
        plan = self.agent.prepare(ACTOR, self._transfer_decision({"scope": "A", "month": MONTH}),
                                  self._operation_id("exclude"), [])
        loaded = await self._field_options(plan, "step0.record_id", "scope=A&month=" + MONTH)
        field = next(f for f in loaded["fields"] if f["name"] == "step0.record_id")
        self.assertEqual([option["value"] for option in field["options"]], ["rec-ok"])
        # The transferable record was retained (with its building codes), so the
        # binding metadata is not empty.
        raw_field = self._raw_field(plan["id"], "step0.record_id")
        self.assertIn("rec-ok", raw_field.get("_records", {}))
        self.assertEqual(raw_field["_options_scope"], "A")
        self.assertEqual(raw_field["_options_month"], MONTH)

    # -- 4) keyword search and >200 partial flag ----------------------------

    async def test_keyword_search_and_more_than_200_partial_flag(self):
        records = [_eligible_event("rec-leak-1", "A楼漏水1"), _eligible_event("rec-leak-2", "A楼漏水2")]
        records += [_eligible_event(f"rec-{i}", f"事件{i}") for i in range(205)]
        self.monthly_payload = _snapshot(records)
        plan = self.agent.prepare(ACTOR, self._transfer_decision({"scope": "A", "month": MONTH}),
                                  self._operation_id("partial"), [])

        filtered = await self._field_options(plan, "step0.record_id", "scope=A&month=" + MONTH + "&q=漏水")
        field = next(f for f in filtered["fields"] if f["name"] == "step0.record_id")
        self.assertEqual(field["options_total"], 2)
        self.assertFalse(field["options_has_more"])
        self.assertEqual([option["value"] for option in field["options"]], ["rec-leak-1", "rec-leak-2"])

        broad = await self._field_options(filtered, "step0.record_id", "scope=A&month=" + MONTH + "&q=事件")
        field = next(f for f in broad["fields"] if f["name"] == "step0.record_id")
        self.assertEqual(field["options_total"], 205)
        self.assertTrue(field["options_has_more"])
        self.assertEqual(len(field["options"]), 200)

    # -- 5) bound queried record preselect + exact single write -------------

    async def test_bound_queried_record_preselect_executes_exact_values_once(self):
        target = _eligible_event("rec-bound", "A楼绑定事件")
        queries = {"query_" + "a" * 32: _snapshot([copy.deepcopy(target)])}
        body = {"scope": "A", "month": MONTH, "record_id": "rec-bound"}
        plan = self.agent.prepare(ACTOR, self._transfer_decision(body), self._operation_id("bound"),
                                  [], queries=queries)
        self.assertEqual(plan["status"], "needs_input")
        record_field = next(f for f in plan["fields"] if f["name"] == "step0.record_id")
        self.assertEqual(record_field["value"], "rec-bound")
        self.assertEqual(record_field["_options_scope"], "A")
        self.assertEqual(record_field["_options_month"], MONTH)
        self.assertIn("rec-bound", record_field.get("_records", {}))

        amended = await self._amend(plan, {"step0.scope": "A", "step0.month": MONTH, "step0.record_id": "rec-bound"})
        self.assertEqual(amended["status"], "awaiting_confirmation")
        self.assertEqual(self.transfer_writes, [])

        reviewed = await self._confirm(amended, "review")
        self.assertEqual(reviewed["status"], "awaiting_second_confirmation")
        executed = await self._confirm(reviewed, "execute")
        self.assertEqual(executed["status"], "running")
        await gather_tasks(self.agent)
        finished = self.agent.get_plan(ACTOR, plan["id"])
        self.assertEqual(finished["status"], "completed", finished.get("error"))
        self.assertEqual(self.transfer_writes, [
            {"scope": "A", "month": MONTH, "record_id": "rec-bound"},
        ])

    # -- 6) changed scope/month without reload and forged ids rejected ------

    async def test_scope_month_change_without_reload_and_forged_ids_rejected(self):
        # A multi-scope owner so a switch to another *selectable* scope actually
        # exercises the "options were loaded for a different scope/month" guard
        # (with a one-scope owner any other scope is rejected even earlier).
        multi = {**ACTOR, "scopes": ["A", "B"]}
        plan = self.agent.prepare(multi, self._transfer_decision({"scope": "A", "month": MONTH}),
                                  self._operation_id("forged"), [])
        loaded = await self._field_options(plan, "step0.record_id", "scope=A&month=" + MONTH, actor=multi)
        field = next(f for f in loaded["fields"] if f["name"] == "step0.record_id")
        self.assertEqual([option["value"] for option in field["options"]], ["rec-ok"])
        raw_field = self._raw_field(plan["id"], "step0.record_id", actor=multi)
        self.assertIn("rec-ok", raw_field.get("_records", {}))
        self.assertEqual(raw_field["_options_scope"], "A")
        self.assertEqual(raw_field["_options_month"], MONTH)

        # Changing scope without reloading options must be rejected.
        with self.assertRaisesRegex(AssistantError, "楼栋或月份已变化"):
            await self._amend(loaded, {"step0.scope": "B", "step0.month": MONTH, "step0.record_id": "rec-ok"}, actor=multi)

        # Changing month without reloading options must be rejected.
        with self.assertRaisesRegex(AssistantError, "楼栋或月份已变化"):
            await self._amend(loaded, {"step0.scope": "A", "step0.month": "2026-09", "step0.record_id": "rec-ok"}, actor=multi)

        # A forged record id that was never loaded is rejected (not in options).
        with self.assertRaises(AssistantError):
            await self._amend(loaded, {"step0.scope": "A", "step0.month": MONTH, "step0.record_id": "rec-evil"}, actor=multi)

        self.assertEqual(self.transfer_writes, [])

    # -- 7) incomplete snapshot / read failure is not empty success ---------

    async def test_incomplete_snapshot_or_read_failure_is_not_empty_success(self):
        plan = self.agent.prepare(ACTOR, self._transfer_decision({"scope": "A", "month": MONTH}),
                                  self._operation_id("incomplete"), [])

        # records missing -> incomplete snapshot -> reject (not empty success).
        self.monthly_payload = _snapshot([])
        self.monthly_payload.pop("records")
        with self.assertRaises(AssistantError) as ctx:
            await self._field_options(plan, "step0.record_id", "scope=A&month=" + MONTH)
        self.assertEqual(ctx.exception.status, 502)

        # snapshot_exists False -> reject.
        self.monthly_payload = _snapshot([_eligible_event()])
        self.monthly_payload["snapshot_exists"] = False
        with self.assertRaises(AssistantError) as ctx:
            await self._field_options(plan, "step0.record_id", "scope=A&month=" + MONTH)
        self.assertEqual(ctx.exception.status, 502)

        # month mismatch -> reject.
        self.monthly_payload = _snapshot([_eligible_event()], month="2026-09")
        with self.assertRaises(AssistantError) as ctx:
            await self._field_options(plan, "step0.record_id", "scope=A&month=" + MONTH)
        self.assertEqual(ctx.exception.status, 502)

        # read failure (backend returns ok=False) -> reject.
        self.monthly_payload = _snapshot([_eligible_event()])
        self.monthly_ok = False
        try:
            with self.assertRaises(AssistantError) as ctx:
                await self._field_options(plan, "step0.record_id", "scope=A&month=" + MONTH)
            self.assertEqual(ctx.exception.status, 502)
        finally:
            self.monthly_ok = True

        # A complete snapshot returns success (not an error / not empty).
        self.monthly_payload = _snapshot([_eligible_event("rec-full", "A楼完整")])
        loaded = await self._field_options(plan, "step0.record_id", "scope=A&month=" + MONTH)
        field = next(f for f in loaded["fields"] if f["name"] == "step0.record_id")
        self.assertEqual([option["value"] for option in field["options"]], ["rec-full"])

    # -- 8) return-to-edit preserves choice and no duplication --------------

    async def test_return_to_edit_preserves_choice_and_no_duplication(self):
        plan = self.agent.prepare(ACTOR, self._transfer_decision({"scope": "A", "month": MONTH}),
                                  self._operation_id("edit"), [])
        loaded = await self._field_options(plan, "step0.record_id", "scope=A&month=" + MONTH)
        amended = await self._amend(loaded, {"step0.scope": "A", "step0.month": MONTH, "step0.record_id": "rec-ok"})
        self.assertEqual(amended["status"], "awaiting_confirmation")
        reviewed = await self._confirm(amended, "review")
        self.assertEqual(reviewed["status"], "awaiting_second_confirmation")

        # Return to edit from the second confirmation.
        reopened = self.agent.amend(ACTOR, reviewed["id"], {"action": "edit", "version": reviewed["version"]})
        self.assertEqual(reopened["status"], "needs_input")
        record_field = next(f for f in reopened["fields"] if f["name"] == "step0.record_id")
        self.assertEqual(record_field["value"], "rec-ok")
        raw_field = self._raw_field(reopened["id"], "step0.record_id")
        self.assertEqual(raw_field["_options_scope"], "A")
        self.assertEqual(raw_field["_options_month"], MONTH)

        re_amended = await self._amend(reopened, {"step0.scope": "A", "step0.month": MONTH, "step0.record_id": "rec-ok"})
        re_reviewed = await self._confirm(re_amended, "review")
        re_executed = await self._confirm(re_reviewed, "execute")
        await gather_tasks(self.agent)
        finished = self.agent.get_plan(ACTOR, plan["id"])
        self.assertEqual(finished["status"], "completed", finished.get("error"))
        self.assertEqual(self.transfer_writes, [
            {"scope": "A", "month": MONTH, "record_id": "rec-ok"},
        ])

    # -- 9) notice-channel guard rejects event notices before any write -----

    def _notice_decision(self, api_id, body):
        return {"operations": [{"api_id": api_id, "body": body}]}

    def test_event_guard_blocks_literal_patch_and_query_reference_on_both_aliases(self):
        for api_id in ("POST /api/workbench-actions", "POST /api/maintenance-actions"):
            with self.subTest(api=api_id):
                # Literal work_type: event.
                literal = self._notice_decision(api_id, {
                    "command_format": "notice_command", "scope": "A", "work_type": "event",
                    "action": "start", "patch": {"title": "事件A", "location": "A楼", "content": "事件内容"},
                })
                with self.assertRaises(AssistantError):
                    self.agent.prepare(ACTOR, literal, self._operation_id("event_literal"), [])
                self.assertEqual(self.transfer_writes, [])

                # Literal notice_type: 事件.
                notice_type = self._notice_decision(api_id, {
                    "command_format": "notice_command", "scope": "A", "work_type": "maintenance",
                    "action": "start", "notice_type": "事件", "patch": {"title": "事件A", "location": "A楼"},
                })
                with self.assertRaises(AssistantError):
                    self.agent.prepare(ACTOR, notice_type, self._operation_id("event_notice_type"), [])
                self.assertEqual(self.transfer_writes, [])

                # Patched work_type: event.
                patched = self._notice_decision(api_id, {
                    "command_format": "notice_command", "scope": "A", "work_type": "maintenance",
                    "action": "start", "patch": {"title": "x", "work_type": "event"},
                })
                with self.assertRaises(AssistantError):
                    self.agent.prepare(ACTOR, patched, self._operation_id("event_patch"), [])
                self.assertEqual(self.transfer_writes, [])

                # Patched notice_type inside the patch object.
                patched_type = self._notice_decision(api_id, {
                    "command_format": "notice_command", "scope": "A", "work_type": "maintenance",
                    "action": "start", "patch": {"title": "x", "notice_type": "事件通告"},
                })
                with self.assertRaises(AssistantError):
                    self.agent.prepare(ACTOR, patched_type, self._operation_id("event_patch_type"), [])
                self.assertEqual(self.transfer_writes, [])

                # Query-reference event (resolved inside the native notice form).
                qref = "query_" + "e" * 32
                queries = {qref: {"draft": {"title": "事件A", "location": "A楼", "content": "x", "work_type": "event"}}}
                referenced = self._notice_decision(api_id, {
                    "command_format": "notice_command", "scope": "A", "work_type": "maintenance",
                    "action": "start", "patch": {"$query": {"ref": qref, "path": "draft"}},
                })
                with self.assertRaises(AssistantError):
                    self.agent.prepare(ACTOR, referenced, self._operation_id("event_ref"), [], queries=queries)
                self.assertEqual(self.transfer_writes, [])

    async def test_repeat_execute_confirm_writes_once(self):
        # Re-clicking execute with the same reviewed proposal must not double-write.
        plan = self.agent.prepare(ACTOR, self._transfer_decision({"scope": "A", "month": MONTH}),
                                  self._operation_id("repeat_exec"), [])
        loaded = await self._field_options(plan, "step0.record_id", "scope=A&month=" + MONTH)
        amended = await self._amend(loaded, {"step0.scope": "A", "step0.month": MONTH, "step0.record_id": "rec-ok"})
        reviewed = await self._confirm(amended, "review")
        self.assertEqual(reviewed["status"], "awaiting_second_confirmation")

        executed_first = await self._confirm(reviewed, "execute")
        executed_second = await self._confirm(reviewed, "execute")
        self.assertEqual(executed_first["status"], "running")
        self.assertEqual(executed_second["status"], "running")
        await gather_tasks(self.agent)
        finished = self.agent.get_plan(ACTOR, plan["id"])
        self.assertEqual(finished["status"], "completed", finished.get("error"))
        self.assertEqual(self.transfer_writes, [
            {"scope": "A", "month": MONTH, "record_id": "rec-ok"},
        ])

    async def test_selected_event_retained_on_keyword_change_but_drops_if_transferred(self):
        plan = self.agent.prepare(ACTOR, self._transfer_decision({"scope": "A", "month": MONTH}),
                                  self._operation_id("retain"), [])
        loaded = await self._field_options(plan, "step0.record_id", "scope=A&month=" + MONTH)
        field = next(f for f in loaded["fields"] if f["name"] == "step0.record_id")
        self.assertEqual([option["value"] for option in field["options"]], ["rec-ok"])

        # A keyword that matches nothing must not drop the selected event.
        selected = json.dumps(["rec-ok"])
        filtered = await self._field_options(loaded, "step0.record_id",
                                             "scope=A&month=" + MONTH + "&q=不存在&selected=" + selected)
        field = next(f for f in filtered["fields"] if f["name"] == "step0.record_id")
        self.assertEqual([option["value"] for option in field["options"]], ["rec-ok"])

        # A fresh snapshot that now marks the event as transferred drops it.
        self.monthly_payload = _snapshot([{**_eligible_event("rec-ok", "A楼漏水"), "transfer_to_overhaul": "是"}])
        changed = await self._field_options(filtered, "step0.record_id",
                                            "scope=A&month=" + MONTH + "&q=不存在&selected=" + selected)
        field = next(f for f in changed["fields"] if f["name"] == "step0.record_id")
        self.assertEqual([option["value"] for option in field["options"]], [])

    def test_web_transfer_path_registered_and_qt_routes_excluded_from_catalog(self):
        # /api/events/transfer-repair is a WEB path and stays in the catalogue.
        transfer = self.catalog.get("POST /api/events/transfer-repair")
        self.assertFalse(transfer["read_only"])
        self.assertEqual(transfer["path"], "/api/events/transfer-repair")

        # The guard's "Qt 专用链路" claim is backed by the catalogue: GET/POST
        # /api/qt/... routes are deliberately excluded, so the agent can never
        # discover or invoke them through the generic catalogue.
        qt_app = FastAPI()

        @qt_app.get("/api/qt/foo")
        async def qt_get():
            return {"ok": True}

        @qt_app.post("/api/qt/bar")
        async def qt_post():
            return {"ok": True}

        qt_catalog = PortalAPICatalog(qt_app)
        with self.assertRaises(AssistantError):
            qt_catalog.get("GET /api/qt/foo")
        with self.assertRaises(AssistantError):
            qt_catalog.get("POST /api/qt/bar")


if __name__ == "__main__":
    unittest.main()