"""Isolated checks for the read-only assistant work-order projection.

Covers:
  * original native step completion semantics (operator/reviewer flags)
  * two-round (run_index) summaries and current round
  * delay remainder sourced from native ``PollingWorkOrderService.end_delay_status``
  * read-only role projection (names only, no tokens/hash/relay/photos/paths)
  * scope isolation (A-only cannot read B orders)
  * pending vs completed state filters
  * pagination
  * the authenticated ``GET /api/assistant/work-orders`` route
  * catalogue read-only boundary (work-orders discoverable, native
    polling-work-orders execution boundary excluded)
  * a detail fixture with >40 steps: full detail survives the endpoint and
    later steps stay addressable through the read_query (query_result_page)
    slice without any native refetch

Only the synthetic in-memory ``Store`` from ``test_lighthouse_stream`` is used as
the state store; no Feishu data, cloud calls or production server are involved.
"""
import json
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent))

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

from lan_bitable_template_portal.lighthouse_ai import AssistantError, safe_data
from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog
from lan_bitable_template_portal.lighthouse_queries import query_result_page
from tools.lighthouse_test_backend import install_test_backend as install_lighthouse_routes
from lan_bitable_template_portal.lighthouse_sources import work_order_records
from lan_bitable_template_portal.polling_work_orders import PollingWorkOrderService
from test_lighthouse_stream import Store


def step(run_index=1, step_index=1, *, operator_required=True, reviewer_required=True,
         operator_confirmed=False, reviewer_confirmed=False, photo_required=True,
         delay_minutes=0, due_at_ts=None, content="检查步骤", **extra):
    item = {
        "run_index": run_index,
        "run_label": "第%d轮" % run_index,
        "step_index": step_index,
        "content": content,
        "operator_required": operator_required,
        "reviewer_required": reviewer_required,
        "delay_reminder_minutes": delay_minutes,
        "delay_reminder": {"due_at_ts": due_at_ts or 0} if delay_minutes else {},
    }
    if operator_confirmed:
        item["operator_confirmation"] = {"at_ts": 1, "open_id": "op-open", "name": "张操作"}
    if reviewer_confirmed:
        item["reviewer_confirmation"] = {"at_ts": 1, "open_id": "rv-open", "name": "李审核"}
    if photo_required is not None:
        item["photo_required"] = photo_required
    item["photos"] = []
    item.update(extra)
    return item


def group(group_id, scope, *, state="active", work_type="polling", current_index=0,
          selected_run_index=None, created_at="2026-10-01T00:00:00", steps=None,
          operator=None, reviewer=None, **extra):
    sensitive_op = {"name": "操作员", "employee_no": "OP0001", "staff_no": "ST-O1",
                    "open_id": "op-token-hash-value", "token": "secret-op-token",
                    "relay": {"url": "https://relay.example/op"}}
    sensitive_rv = {"name": "审核员", "employee_no": "RV0001", "staff_no": "ST-R1",
                    "open_id": "rv-token-hash-value", "token": "secret-rv-token",
                    "relay": {"url": "https://relay.example/rv"}}
    spec = {
        "group_id": group_id,
        "title": "工单 " + group_id,
        "scope": scope,
        "work_type": work_type,
        "sop_name": "SOP-" + group_id,
        "state": state,
        "created_at": created_at,
        "updated_at": created_at,
        "current_index": current_index,
        "operator": operator or sensitive_op,
        "reviewer": reviewer or sensitive_rv,
        "steps": steps if steps is not None else [step()],
        **extra,
    }
    if selected_run_index is not None:
        spec["selected_run_index"] = selected_run_index
    return spec


def seed(store, groups):
    for item in groups:
        store.put_document("polling_work_order", item["group_id"], item)


class WorkOrderQueryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = Store(Path(self.tmp.name) / "state.sqlite3")
        self.actor_a = {"id": "fixture-a", "scopes": ["A"], "is_admin": False}

    def records(self, query, actor=None):
        return work_order_records(self.store, actor or self.actor_a, query)

    # -- native completion semantics -----------------------------------------

    def test_native_step_done_operator_reviewer_flags(self):
        svc = PollingWorkOrderService
        # Optional roles are considered done regardless of confirmation.
        self.assertTrue(svc._step_done({"operator_required": False, "reviewer_required": False}))
        self.assertTrue(svc._step_done({"operator_required": False, "reviewer_required": True,
                                        "reviewer_confirmation": {"at_ts": 1}}))
        self.assertFalse(svc._step_done({"operator_required": False, "reviewer_required": True}))
        # Required operator needs a confirmation.
        self.assertFalse(svc._step_done({"operator_required": True, "reviewer_required": False}))
        self.assertTrue(svc._step_done({"operator_required": True, "reviewer_required": False,
                                        "operator_confirmation": {"at_ts": 1}}))
        # Both required and confirmed.
        done = svc._step_done({"operator_required": True, "reviewer_required": True,
                               "operator_confirmation": {"at_ts": 1},
                               "reviewer_confirmation": {"at_ts": 1}})
        self.assertTrue(done)
        partial = svc._step_done({"operator_required": True, "reviewer_required": True,
                                  "operator_confirmation": {"at_ts": 1}})
        self.assertFalse(partial)

    # -- two rounds -----------------------------------------------------------

    def test_two_rounds_runs_and_current_run_index(self):
        # Native rounds are one-based; round 1 is done and round 2 is active.
        steps = [
            step(1, 1, operator_confirmed=True, reviewer_confirmed=True),
            step(1, 2, operator_confirmed=True, reviewer_confirmed=True),
            step(2, 1, operator_confirmed=True, reviewer_confirmed=True),
            step(2, 2, operator_required=True, reviewer_required=True, activated_at_ts=1),
        ]
        item = group("wo-two-rounds", "A", state="active", current_index=3,
                     steps=steps)
        seed(self.store, [item])
        result = self.records({"scope": "A", "group_id": "wo-two-rounds"})
        row = result["items"][0]
        self.assertEqual(sorted(run["run_index"] for run in row["runs"]), [1, 2])
        by_index = {run["run_index"]: run for run in row["runs"]}
        self.assertEqual(by_index[1]["step_count"], 2)
        self.assertEqual(by_index[1]["completed_steps"], 2)
        self.assertEqual(by_index[2]["step_count"], 2)
        self.assertEqual(by_index[2]["completed_steps"], 1)
        self.assertEqual(row["current_run_index"], 2)
        self.assertEqual(row["current_step_number"], 4)
        # steps carry run_index / run_label through projection
        self.assertEqual(row["steps"][2]["run_index"], 2)
        self.assertEqual(row["steps"][2]["run_label"], "第2轮")

    # -- delay remainder from native end_delay_status --------------------------

    def test_delay_status_comes_from_native_end_delay_status(self):
        due = time.time() + 3600.0
        steps = [step(1, 1, delay_minutes=5, due_at_ts=due,
                      operator_confirmed=False, reviewer_confirmed=True),
                 step(1, 2, delay_minutes=0, operator_confirmed=True, reviewer_confirmed=True)]
        item = group("wo-delay-a", "A", steps=steps, state="active", current_index=0)
        seed(self.store, [item])
        result = self.records({"scope": "A", "group_id": "wo-delay-a"})
        row = result["items"][0]
        delay = row["delay_status"]
        self.assertTrue(delay["enabled"])
        self.assertFalse(delay["missing_completion"])
        self.assertFalse(delay["ready"])
        self.assertAlmostEqual(delay["due_at_ts"], due, delta=2.0)
        self.assertGreater(delay["remaining_seconds"], 0)
        # Remaining seconds mirror the native computation around the same wall-clock.
        native = PollingWorkOrderService.end_delay_status(item, now=time.time())
        self.assertEqual(delay["due_at_ts"], native["due_at_ts"])
        self.assertLessEqual(abs(delay["remaining_seconds"] - native["remaining_seconds"]), 5)

    def test_delay_remaining_tracks_due_time_in_step_projection(self):
        due = time.time() + 600.0
        items = [step(1, 1, delay_minutes=3, due_at_ts=due)]
        item = group("wo-delay-b", "A", steps=items, state="active", current_index=0)
        seed(self.store, [item])
        result = self.records({"scope": "A", "group_id": "wo-delay-b"})
        step_row = result["items"][0]["steps"][0]
        self.assertTrue(step_row["delay_status"]["enabled"])
        self.assertFalse(step_row["delay_status"]["ready"])
        self.assertAlmostEqual(step_row["delay_status"]["due_at_ts"], due, delta=2.0)

    def test_activation_timer_and_device_runs_reuse_native_projection(self):
        started = time.time() - 10
        steps = [step(1, 1, time_limit_seconds=60),
                 step(2, 1, activated_at_ts=started, time_limit_seconds=60)]
        item = group("wo-native-timers", "A", current_index=1, selected_run_index=2,
                     steps=steps, runs=[{"from_unit": "1#", "to_unit": "2#", "private_token": "not-public"},
                                       {"from_unit": "2#", "to_unit": "3#"}],
                     uploaded_file_tokens=["not-public-attachment"])
        seed(self.store, [item])
        row = self.records({"scope": "A", "group_id": item["group_id"]})["items"][0]
        self.assertEqual(row["runs"][1]["from_unit"], "2#")
        self.assertEqual(row["runs"][1]["to_unit"], "3#")
        self.assertEqual(row["uploaded_file_count"], 1)
        self.assertFalse(row["upload_completed"])
        self.assertFalse(row["steps"][0]["activated"])
        self.assertFalse(row["steps"][0]["timer_started"])
        self.assertEqual(row["steps"][0]["remaining_seconds"], 60)
        active = row["steps"][1]
        native = PollingWorkOrderService._step_public({**steps[1], "global_index": 1}, 1)
        self.assertTrue(active["activated"])
        self.assertEqual(active["position"], native["position"])
        self.assertEqual(active["time_limit_seconds"], 60)
        self.assertAlmostEqual(active["remaining_seconds"], native["remaining_seconds"], delta=1)
        self.assertNotIn("not-public", json.dumps(row))

    def test_actual_confirmers_and_repeat_source_are_not_replaced_by_assignee(self):
        item = group("wo-confirmation-audit", "A", steps=[step(source_step_index=4, repeat_round=3,
            operator_confirmation={"assigned_name": "指定操作员", "actual_name": "实际确认人",
                                   "actual_open_id": "private-identity", "confirmed_at": "2026-10-03 09:00:00"})])
        seed(self.store, [item])
        row = self.records({"scope": "A", "group_id": item["group_id"]})["items"][0]["steps"][0]
        self.assertEqual(row["source_step_index"], 4)
        self.assertEqual(row["repeat_round"], 3)
        self.assertEqual(row["confirmations"]["operator"], {"assigned_name": "指定操作员", "actual_name": "实际确认人", "confirmed_at": "2026-10-03 09:00:00"})
        self.assertEqual(row["confirmations"]["reviewer"]["actual_name"], "")
        self.assertNotIn("private-identity", json.dumps(row))

    # -- role projection / sensitivity -----------------------------------------

    def test_role_names_only_no_tokens_hashes_relay_photos_or_paths(self):
        steps = [
            step(1, 1, operator_confirmed=False, reviewer_confirmed=True,
                 photos=[{"hash": "photo-hash-123", "path": r"D:\uploads\step1.png",
                          "url": "https://upload.example/step1.png"}],
                 extra_path="/private/step1.png"),
            step(1, 2, operator_confirmed=True, reviewer_confirmed=False,
                 photos=[{"hash": "photo-hash-456", "path": "step2.png"}],
                 extra_relay="https://relay.example/step2"),
        ]
        item = group("wo-redact-a", "A", steps=steps, state="active", current_index=0)
        seed(self.store, [item])
        result = self.records({"scope": "A", "group_id": "wo-redact-a"})
        row = result["items"][0]
        # role projection exposes only safe identity keys
        self.assertEqual(set(row["operator"]), {"name", "employee_no", "staff_no"})
        self.assertEqual(set(row["reviewer"]), {"name", "employee_no", "staff_no"})
        self.assertEqual(row["operator"]["name"], "操作员")
        # no secret tokens, hashes, relay links, raw photo data or file paths.
        # photo_required / photo_count are the only allowed photo-related metadata.
        serialized = json.dumps(safe_data(result, list_limit=1000), ensure_ascii=False)
        for forbidden in ("token", "open_id", "relay.example", "secret-op-token",
                          "secret-rv-token", "op-token-hash-value", "rv-token-hash-value",
                          "photo-hash-123", "photo-hash-456", r"D:\\uploads",
                          "/private/step1.png", ".png", "upload.example"):
            self.assertNotIn(forbidden, serialized)
        # steps expose only the projected fields
        for step_row in row["steps"]:
            allowed = {"number", "run_index", "run_label", "step_index", "content",
                       "operator_required", "reviewer_required", "delay_reminder_minutes",
                       "operator_confirmed", "reviewer_confirmed", "completed",
                       "photo_required", "photo_count", "delay_status", "activated", "position",
                       "time_limit_seconds", "timer_started", "confirm_available_at", "remaining_seconds",
                       "confirmations", "source_step_index", "repeat_round"}
            self.assertTrue(set(step_row) <= allowed, set(step_row) - allowed)
            self.assertNotIn("photos", step_row)
            self.assertNotIn("operator_confirmation", step_row)
            self.assertNotIn("reviewer_confirmation", step_row)
            self.assertEqual(step_row["photo_count"], 1)
            self.assertTrue(step_row["photo_required"])

    # -- scope isolation ---------------------------------------------------------

    def test_a_only_scope_cannot_access_b_order(self):
        a_item = group("wo-aaa-a", "A", state="active")
        b_item = group("wo-bbb-b", "B", state="completed")
        seed(self.store, [a_item, b_item])
        # direct read of a B order is rejected
        with self.assertRaises(AssistantError) as ctx:
            self.records({"scope": "A", "group_id": "wo-bbb-b"})
        self.assertEqual(ctx.exception.status, 403)
        # a listing returns only A orders
        result = self.records({"scope": "ALL", "state": "all"})
        ids = [item["group_id"] for item in result["items"]]
        self.assertIn("wo-aaa-a", ids)
        self.assertNotIn("wo-bbb-b", ids)
        self.assertEqual(result["total"], 1)

    # -- pending vs completed filters -------------------------------------------

    def test_pending_and_completed_state_filters(self):
        groups = [
            group("wo-active-a", "A", state="active"),
            group("wo-upload-a", "A", state="upload_pending"),
            group("wo-done-a", "A", state="completed"),
            group("wo-cancel-a", "A", state="cancelled"),
        ]
        seed(self.store, groups)
        pending = self.records({"scope": "A", "state": "pending"})
        self.assertEqual(sorted(item["group_id"] for item in pending["items"]),
                         ["wo-active-a", "wo-upload-a"])
        completed = self.records({"scope": "A", "state": "completed"})
        self.assertEqual([item["group_id"] for item in completed["items"]], ["wo-done-a"])
        all_states = self.records({"scope": "A", "state": "all"})
        self.assertEqual(all_states["total"], 4)
        active = self.records({"scope": "A", "state": "active"})
        self.assertEqual([item["group_id"] for item in active["items"]], ["wo-active-a"])

    # -- pagination ---------------------------------------------------------------

    def test_pagination(self):
        for n in range(1, 6):
            seed(self.store, [group("wo-p%d" % n, "A", state="active",
                                   created_at="2026-10-0%dT00:00:00" % n)])
        first = self.records({"scope": "A", "state": "all", "page": 1, "page_size": 2})
        self.assertEqual(first["total"], 5)
        self.assertEqual(len(first["items"]), 2)
        self.assertEqual(first["page"], 1)
        last = self.records({"scope": "A", "state": "all", "page": 3, "page_size": 2})
        self.assertEqual(len(last["items"]), 1)
        self.assertEqual(last["total"], 5)
        for bad in ({"page": 0, "page_size": 2}, {"page": 1, "page_size": 41},
                    {"page": 1, "page_size": 0}):
            with self.assertRaises(AssistantError):
                self.records({"scope": "A", "state": "all", **bad})

    # -- authenticated route ------------------------------------------------------

    def _route_runtime(self, session):
        controller = SimpleNamespace(
            _current_session=lambda request: session,
            _auth_required_response=lambda: JSONResponse({"ok": False}, status_code=401),
            _request_base_url=lambda request: str(request.base_url).rstrip("/"))
        runtime = SimpleNamespace(
            state_store=self.store,
            auth_manager=SimpleNamespace(
                is_admin=lambda s: bool(s and s.get("role") == "admin"),
                session_scopes=lambda s: list(s["allowed_scopes"])))
        return controller, runtime

    def test_authenticated_work_orders_route(self):
        seed(self.store, [
            group("wo-route-active-a", "A", state="active"),
            group("wo-route-done-a", "A", state="completed"),
            group("wo-route-b", "B", state="active"),
        ])
        session = {"open_id": "fixture-a", "user": {"open_id": "fixture-a"},
                   "allowed_scopes": ["A"], "role": "building"}
        controller, runtime = self._route_runtime(session)
        app = FastAPI()
        install_lighthouse_routes(app, controller, runtime)
        with TestClient(app, headers={"Origin": "http://testserver"}) as client:
            response = client.get("/api/assistant/work-orders?scope=A&state=pending")
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.headers["cache-control"], "no-store")
            data = response.json()["data"]
            self.assertEqual(sorted(item["group_id"] for item in data["items"]),
                             ["wo-route-active-a"])
            response_all = client.get("/api/assistant/work-orders?scope=ALL&state=all")
            self.assertEqual(response_all.status_code, 200, response_all.text)
            ids = [item["group_id"] for item in response_all.json()["data"]["items"]]
            self.assertIn("wo-route-active-a", ids)
            self.assertIn("wo-route-done-a", ids)
            self.assertNotIn("wo-route-b", ids)
        # unauthenticated
        controller, runtime = self._route_runtime(None)
        app2 = FastAPI()
        install_lighthouse_routes(app2, controller, runtime)
        with TestClient(app2, headers={"Origin": "http://testserver"}) as client:
            response = client.get("/api/assistant/work-orders?scope=A")
            self.assertEqual(response.status_code, 401, response.text)

    # -- catalogue read-only boundary ---------------------------------------------

    def test_catalog_read_only_boundary(self):
        session = {"open_id": "fixture-a", "user": {"open_id": "fixture-a"},
                   "allowed_scopes": ["A"], "role": "building"}
        controller, runtime = self._route_runtime(session)
        app = FastAPI()
        install_lighthouse_routes(app, controller, runtime)

        async def native_execution():
            return {"ok": True}

        app.add_api_route("/api/polling-work-orders/session", native_execution, methods=["GET"])
        app.add_api_route("/api/polling-work-orders/confirm", native_execution, methods=["POST"])
        catalog = PortalAPICatalog(app)
        desc = catalog.get("GET /api/assistant/work-orders")
        self.assertTrue(desc["read_only"])
        self.assertEqual(desc["method"], "GET")
        # execution-oriented native polling-work-orders stays out of the assistant
        # catalogue (no discoverable descriptor), so the agent cannot drive writes.
        found = catalog.discover(keyword="polling-work-orders")
        self.assertEqual(found["total"], 0)
        for item in found["items"]:
            self.assertNotIn("polling-work-orders", item["path"])
        for api_id in ("GET /api/polling-work-orders/session", "POST /api/polling-work-orders/confirm"):
            with self.assertRaises(AssistantError):
                catalog.validate_operation({"api_id": api_id})

    # -- detail >40 steps + read_query without refetch ----------------------------

    def test_detail_more_than_40_steps_survives_and_read_query_accesses_later(self):
        many = [step(run_index=1, step_index=i + 1, content="步骤%d" % (i + 1),
                     photo_required=bool(i % 2)) for i in range(45)]
        item = group("wo-detail-45", "A", state="active", current_index=0, steps=many)
        seed(self.store, [item])
        result = self.records({"scope": "A", "group_id": "wo-detail-45"})
        self.assertEqual(result["total"], 1)
        row = result["items"][0]
        self.assertEqual(len(row["steps"]), 45)
        self.assertEqual(row["step_count"], 45)
        # The endpoint serialises the full detail (list_limit=1000 in route).
        serialized = json.dumps(safe_data(result, list_limit=1000), ensure_ascii=False)
        self.assertIn('"步骤45"', serialized)
        self.assertIn('"number": 45', serialized)
        # read_query (query_result_page) reads the already-loaded snapshot by path,
        # including items beyond the default 40-row visible sample, without a refetch.
        page = query_result_page(result, ["items", "0", "steps"], offset=40, limit=10)
        self.assertEqual(page["loaded_count"], 45)
        self.assertEqual(len(page["items"]), 5)
        numbers = [s["number"] for s in page["items"]]
        self.assertEqual(numbers, [41, 42, 43, 44, 45])
        self.assertIsNone(page["next_offset"])


if __name__ == "__main__":
    unittest.main()
