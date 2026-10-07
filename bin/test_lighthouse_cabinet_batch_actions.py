"""Contract tests for cabinet-power batch confirm / rollback / restore-rows.

The upcoming contract requires every POST
  /api/cabinet-power/batches/{batch_id}/confirm
  /api/cabinet-power/batches/{batch_id}/rollback
  /api/cabinet-power/batches/{batch_id}/restore-rows
to be prepared from the original GET batch snapshot supplied in the `queries`
argument, shaped as::

    {query_x: {batch_id: str, version: int, scopes: [E], rows: [
        {row_id, scope, room, rack, action, actual,
         confirmable/rollbackable/restorable: bool}]}}

Contract points covered:
- No snapshot -> explicit "query-batch-first" error and no write.
- Version is always taken from the snapshot; a supplied different version is
  rejected with 409.
- body.all=true stays native all=true (a fixed snapshot version, no
  row_ids/version manual fields); all=true over mixed scopes is rejected for a
  single-scope actor.
- Anything else produces a required step0.row_ids multiselect built only from
  eligible native rows (scope in actor scopes + flag) with room/rack labels.
  Empty initial selection must be completed by the user.
- prepare rejects row_ids that are not in the eligible snapshot rows.
- amend validates the multiselect options (no foreign / forged row_ids).
- Model-provided generic version/row_ids fields do not replace the native
  controls.
- Foreign rows cannot become options or be confirmed.
- Confirm is high-risk and two-stage; the native fake receives the fixed
  snapshot version plus all=true or exact row_ids; repeat clicks do not
  duplicate the write; a stale native 409 leaves the plan failed with no retry.

Uses only fake in-process FastAPI routes with the original catalog schema and
a tiny in-memory store.  No cabinet templates / cloud / production access.
"""

import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from fastapi import FastAPI, Request

from lan_bitable_template_portal.lighthouse_agent import PortalAgent
from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog
from lan_bitable_template_portal.lighthouse_ai import AssistantError

import test_lighthouse_agent_workflows as workflows
from test_lighthouse_agent_workflows import gather_tasks

E_ACTOR = {"id": "cabinet-e", "scopes": ["E"], "is_admin": False}


def _row(row_id, scope="E", *, room="E101", rack="RACK-E1", action="上电",
         actual="已上电", confirmable=True, rollbackable=False, restorable=False):
    return {
        "row_id": row_id, "scope": scope, "room": room, "rack": rack,
        "action": action, "actual": actual,
        "confirmable": confirmable, "rollbackable": rollbackable,
        "restorable": restorable,
    }


def _confirm_snapshot(batch_id="batch-001", version=3, scopes=("E",)):
    return {
        "batch_id": batch_id, "version": version, "scopes": list(scopes),
        "rows": [
            _row("row-e-1", "E", room="E101", rack="RACK-E1", action="上电", actual="已上电"),
            _row("row-e-2", "E", room="E202", rack="RACK-E2", action="下电", actual="已下电",
                 confirmable=False),          # not eligible (already done)
            _row("row-a-1", "A", room="A101", rack="RACK-A1", action="上电", actual="已上电"),
        ],
    }


def _confirm_snapshot_e_only(batch_id="batch-001", version=3):
    """Whole-batch (all=true) fixture: every row belongs to the E scope.

    Native all=true checks per-row scopes too, so the happy path must not mix
    in a foreign-scope row.
    """
    return {
        "batch_id": batch_id, "version": version, "scopes": ["E"],
        "rows": [
            _row("row-e-1", "E", room="E101", rack="RACK-E1", action="上电", actual="已上电"),
            _row("row-e-2", "E", room="E202", rack="RACK-E2", action="下电", actual="已下电",
                 confirmable=False),          # not eligible (already done)
        ],
    }


def _rollback_snapshot(batch_id="batch-rb", version=5):
    return {
        "batch_id": batch_id, "version": version, "scopes": ["E"],
        "rows": [
            _row("rb-row-1", "E", room="E301", rack="RACK-E3", action="上电", actual="已上电",
                 confirmable=False, rollbackable=True),
            _row("rb-row-2", "A", room="A301", rack="RACK-A3", action="上电", actual="已上电",
                 confirmable=False, rollbackable=True),
        ],
    }


class CabinetBatchActionTests(unittest.IsolatedAsyncioTestCase):
    """Confirm/rollback/restore-rows contract tests.

    Composes ``workflows.WorkflowTests`` as a fixture instead of subclassing it,
    so the parent's 35 tests are not re-discovered / re-run here.  Each test
    builds a tiny app with only the fake native cabinet routes it needs.
    """

    def setUp(self):
        super().setUp()
        self.wf = workflows.WorkflowTests()
        self.wf.setUp()
        # Transfer the shared fixture's cleanup callbacks onto this test case so
        # its temp dir / sqlite store are cleaned up after this test completes.
        for fn, args, kwargs in self.wf._cleanups:
            self.addCleanup(fn, *args, **kwargs)
        self.wf._cleanups.clear()
        # Copy only the harness attributes the cabinet helper methods use.
        self.assistant = self.wf.assistant
        self.files = self.wf.files
        self.request = self.wf.request
        self.operation_id = self.wf.operation_id

    def _agent(self, batch_id, handler, *, method="confirm"):
        """Return (agent, writes) with one fake native cabinet route."""
        app, writes = FastAPI(), []
        path = f"/api/cabinet-power/batches/{{batch_id}}/{method}"
        expected_operation = f"POST /api/cabinet-power/batches/{{batch_id}}/{method}"

        async def route(batch_id: str, request: Request):
            body = json.loads(await request.body() or b"null")
            writes.append({"batch_id": batch_id, "body": body})
            return handler(batch_id, body)

        app.add_api_route(path, route, methods=["POST"])
        catalog = PortalAPICatalog(app)
        # Guard: the catalog must describe this native route under the exact
        # api_id the agent will target.
        self.assertEqual(catalog.get(expected_operation)["id"], expected_operation)
        self.assertEqual(catalog.get(expected_operation)["path"], path)
        agent = PortalAgent(self.assistant, catalog, self.files)
        return agent, writes

    # ------------------------------------------------------------------ #
    # Snapshot requirement / version handling
    # ------------------------------------------------------------------ #

    async def test_confirm_without_snapshot_raises_query_batch_first_no_write(self):
        agent, writes = self._agent(
            "batch-001",
            lambda batch_id, body: {"ok": True, "data": {}},
        )
        decision = {
            "operations": [{
                "api_id": "POST /api/cabinet-power/batches/{batch_id}/confirm",
                "path_params": {"batch_id": "batch-001"},
            }],
        }
        with self.assertRaisesRegex(Exception, "请先查询并选择该机柜批次的完整详情"):
            agent.prepare(E_ACTOR, decision, self.operation_id, [], queries={})
        self.assertEqual(writes, [])

    async def test_confirm_with_mismatched_snapshot_batch_id_is_query_batch_first(self):
        agent, writes = self._agent(
            "batch-001",
            lambda batch_id, body: {"ok": True, "data": {}},
        )
        snapshot = _confirm_snapshot(batch_id="other-batch")
        decision = {
            "operations": [{
                "api_id": "POST /api/cabinet-power/batches/{batch_id}/confirm",
                "path_params": {"batch_id": "batch-001"},
            }],
        }
        with self.assertRaisesRegex(Exception, "请先查询并选择该机柜批次的完整详情"):
            agent.prepare(E_ACTOR, decision, self.operation_id, [], queries={"query_batch": snapshot})
        self.assertEqual(writes, [])

    async def test_version_always_taken_from_snapshot(self):
        agent, writes = self._agent(
            "batch-001",
            lambda batch_id, body: {"ok": True, "data": {}},
        )
        snapshot = _confirm_snapshot(version=4)
        decision = {
            "operations": [{
                "api_id": "POST /api/cabinet-power/batches/{batch_id}/confirm",
                "path_params": {"batch_id": "batch-001"},
                "body": {"row_ids": ["row-e-1"]},
            }],
        }
        plan = agent.prepare(E_ACTOR, decision, self.operation_id, [], queries={"query_batch": snapshot})
        self.assertEqual(plan["operations"][0]["body"]["version"], 4)
        field = next(f for f in plan["fields"] if f["name"] == "step0.row_ids")
        # The snapshot version, not any model-suggested value, is authoritative.
        plan = agent.amend(E_ACTOR, plan["id"], {"version": plan["version"], "values": {"step0.row_ids": ["row-e-1"]}})
        await agent._execute(E_ACTOR, agent.get_plan(E_ACTOR, plan["id"]), self.request)
        self.assertEqual(writes, [{"batch_id": "batch-001", "body": {"version": 4, "row_ids": ["row-e-1"]}}])

    async def test_version_conflict_rejected_409(self):
        agent, writes = self._agent(
            "batch-001",
            lambda batch_id, body: {"ok": True, "data": {}},
        )
        snapshot = _confirm_snapshot(version=4)
        decision = {
            "operations": [{
                "api_id": "POST /api/cabinet-power/batches/{batch_id}/confirm",
                "path_params": {"batch_id": "batch-001"},
                "body": {"version": 3, "row_ids": ["row-e-1"]},
            }],
        }
        with self.assertRaises(AssistantError) as ctx:
            agent.prepare(E_ACTOR, decision, self.operation_id, [], queries={"query_batch": snapshot})
        self.assertEqual(ctx.exception.status, 409)
        self.assertEqual(writes, [])

    # ------------------------------------------------------------------ #
    # row_ids multiselect construction / eligibility
    # ------------------------------------------------------------------ #

    async def test_row_ids_multiselect_built_from_eligible_rows_with_room_rack_labels(self):
        agent, writes = self._agent(
            "batch-001",
            lambda batch_id, body: {"ok": True, "data": {}},
        )
        decision = {
            "operations": [{
                "api_id": "POST /api/cabinet-power/batches/{batch_id}/confirm",
                "path_params": {"batch_id": "batch-001"},
            }],
        }
        plan = agent.prepare(E_ACTOR, decision, self.operation_id, [], queries={"query_batch": _confirm_snapshot()})
        self.assertEqual(plan["status"], "needs_input")
        fields = plan["fields"]
        control = next(f for f in fields if f["name"] == "step0.row_ids")
        self.assertEqual(control["type"], "multiselect")
        self.assertTrue(control["required"])
        self.assertEqual(control["minItems"], 1)
        # Only the E, confirmable row may become an option; foreign A row and
        # non-eligible E row are excluded.
        self.assertEqual([opt["value"] for opt in control["options"]], ["row-e-1"])
        label = control["options"][0]["label"]
        self.assertIn("E楼 E101/RACK-E1", label)
        self.assertIn("上电", label)
        # Empty initial selection must be completed by the user.
        with self.assertRaisesRegex(Exception, "请填写："):
            agent.amend(E_ACTOR, plan["id"], {"version": plan["version"], "values": {"step0.row_ids": []}})
        self.assertEqual(writes, [])

    async def test_prepare_rejects_row_ids_outside_eligible_snapshot(self):
        agent, writes = self._agent(
            "batch-001",
            lambda batch_id, body: {"ok": True, "data": {}},
        )
        decision = {
            "operations": [{
                "api_id": "POST /api/cabinet-power/batches/{batch_id}/confirm",
                "path_params": {"batch_id": "batch-001"},
                "body": {"row_ids": ["row-e-1", "row-a-1"]},
            }],
        }
        with self.assertRaisesRegex(Exception, "所选机柜不在当前批次的可操作记录中"):
            agent.prepare(E_ACTOR, decision, self.operation_id, [], queries={"query_batch": _confirm_snapshot()})
        self.assertEqual(writes, [])

    async def test_foreign_rows_cannot_become_options_or_be_confirmed(self):
        agent, writes = self._agent(
            "batch-001",
            lambda batch_id, body: {"ok": True, "data": {}},
        )
        decision = {
            "operations": [{
                "api_id": "POST /api/cabinet-power/batches/{batch_id}/confirm",
                "path_params": {"batch_id": "batch-001"},
            }],
        }
        plan = agent.prepare(E_ACTOR, decision, self.operation_id, [], queries={"query_batch": _confirm_snapshot()})
        control = next(f for f in plan["fields"] if f["name"] == "step0.row_ids")
        self.assertEqual([opt["value"] for opt in control["options"]], ["row-e-1"])
        # A foreign scope row cannot be selected because it is never an option.
        with self.assertRaisesRegex(Exception, "请重新选择"):
            agent.amend(E_ACTOR, plan["id"], {"version": plan["version"], "values": {"step0.row_ids": ["row-a-1"]}})
        self.assertEqual(writes, [])

    async def test_amend_validates_multiselect_options_against_native_eligibility(self):
        agent, writes = self._agent(
            "batch-001",
            lambda batch_id, body: {"ok": True, "data": {}},
        )
        decision = {
            "operations": [{
                "api_id": "POST /api/cabinet-power/batches/{batch_id}/confirm",
                "path_params": {"batch_id": "batch-001"},
            }],
        }
        plan = agent.prepare(E_ACTOR, decision, self.operation_id, [], queries={"query_batch": _confirm_snapshot()})
        # Forged id that is not among native options is rejected.
        with self.assertRaisesRegex(Exception, "请重新选择"):
            agent.amend(E_ACTOR, plan["id"], {"version": plan["version"], "values": {"step0.row_ids": ["forged-row"]}})
        self.assertEqual(writes, [])

    async def test_model_generic_version_row_ids_fields_do_not_replace_controls(self):
        agent, writes = self._agent(
            "batch-001",
            lambda batch_id, body: {"ok": True, "data": {}},
        )
        snapshot = _confirm_snapshot(version=7)
        decision = {
            "operations": [{
                "api_id": "POST /api/cabinet-power/batches/{batch_id}/confirm",
                "path_params": {"batch_id": "batch-001"},
                "body": {"row_ids": ["row-e-1"]},
            }],
            "fields": [
                {"name": "step0.version", "path": "version", "type": "number", "label": "版本"},
                {"name": "step0.row_ids_overwrite", "path": "row_ids", "type": "multiselect",
                 "label": "自定义行", "options": []},
            ],
        }
        plan = agent.prepare(E_ACTOR, decision, self.operation_id, [], queries={"query_batch": snapshot})
        row_ids_fields = [f for f in plan["fields"] if f["path"] == "row_ids"]
        # The dedicated native control (with snapshot options) is the only row_ids
        # control; the model's generic version / row_ids fields are dropped.
        self.assertEqual(len(row_ids_fields), 1)
        control = row_ids_fields[0]
        self.assertEqual(control["name"], "step0.row_ids")
        self.assertEqual([opt["value"] for opt in control["options"]], ["row-e-1"])
        self.assertNotIn("step0.version", [f["name"] for f in plan["fields"]])
        self.assertNotIn("step0.row_ids_overwrite", [f["name"] for f in plan["fields"]])
        # Body version still comes from the snapshot, and row_ids is owned by the control.
        self.assertEqual(plan["operations"][0]["body"]["version"], 7)
        plan = agent.amend(E_ACTOR, plan["id"], {"version": plan["version"], "values": {"step0.row_ids": ["row-e-1"]}})
        await agent._execute(E_ACTOR, agent.get_plan(E_ACTOR, plan["id"]), self.request)
        self.assertEqual(writes, [{"batch_id": "batch-001", "body": {"version": 7, "row_ids": ["row-e-1"]}}])

    # ------------------------------------------------------------------ #
    # all=true semantics
    # ------------------------------------------------------------------ #

    async def test_all_true_uses_snapshot_version_and_no_row_ids_control(self):
        agent, writes = self._agent(
            "batch-001",
            lambda batch_id, body: {"ok": True, "data": {}},
        )
        snapshot = _confirm_snapshot_e_only(version=6)
        decision = {
            "operations": [{
                "api_id": "POST /api/cabinet-power/batches/{batch_id}/confirm",
                "path_params": {"batch_id": "batch-001"},
                "body": {"all": True},
            }],
        }
        plan = agent.prepare(E_ACTOR, decision, self.operation_id, [], queries={"query_batch": snapshot})
        self.assertEqual(plan["status"], "awaiting_confirmation")
        self.assertEqual(plan["risk"], "high")
        self.assertEqual(plan["fields"], [])
        self.assertEqual(plan["operations"][0]["body"], {"version": 6, "all": True})

    async def test_all_true_over_mixed_scopes_is_rejected(self):
        agent, writes = self._agent(
            "batch-001",
            lambda batch_id, body: {"ok": True, "data": {}},
        )
        snapshot = _confirm_snapshot(scopes=("E", "A"))
        decision = {
            "operations": [{
                "api_id": "POST /api/cabinet-power/batches/{batch_id}/confirm",
                "path_params": {"batch_id": "batch-001"},
                "body": {"all": True},
            }],
        }
        with self.assertRaises(AssistantError) as ctx:
            agent.prepare(E_ACTOR, decision, self.operation_id, [], queries={"query_batch": snapshot})
        self.assertEqual(ctx.exception.status, 403)
        self.assertEqual(writes, [])

    async def test_all_true_with_scopes_E_but_foreign_A_row_rejected_403(self):
        agent, writes = self._agent(
            "batch-001",
            lambda batch_id, body: {"ok": True, "data": {}},
        )
        # scopes declares the batch E-only, yet one row is foreign (scope A).
        # The assistant also verifies per-row scopes, so this inconsistency
        # must be rejected with a 403 before any write.
        snapshot = _confirm_snapshot()
        decision = {
            "operations": [{
                "api_id": "POST /api/cabinet-power/batches/{batch_id}/confirm",
                "path_params": {"batch_id": "batch-001"},
                "body": {"all": True},
            }],
        }
        with self.assertRaises(AssistantError) as ctx:
            agent.prepare(E_ACTOR, decision, self.operation_id, [], queries={"query_batch": snapshot})
        self.assertEqual(ctx.exception.status, 403)
        self.assertEqual(writes, [])

    async def test_all_true_with_row_ids_or_scope_is_rejected(self):
        agent, writes = self._agent(
            "batch-001",
            lambda batch_id, body: {"ok": True, "data": {}},
        )
        snapshot = _confirm_snapshot()
        for extra in ({"row_ids": ["row-e-1"]}, {"scope": "E"}):
            decision = {
                "operations": [{
                    "api_id": "POST /api/cabinet-power/batches/{batch_id}/confirm",
                    "path_params": {"batch_id": "batch-001"},
                    "body": {"all": True, **extra},
                }],
            }
            with self.assertRaisesRegex(Exception, "整批操作不能同时指定部分机柜或楼栋"):
                agent.prepare(E_ACTOR, decision, self.operation_id, [], queries={"query_batch": snapshot})
        self.assertEqual(writes, [])

    # ------------------------------------------------------------------ #
    # rollback / restore-rows
    # ------------------------------------------------------------------ #

    async def test_rollback_uses_rollbackable_rows_only(self):
        agent, writes = self._agent(
            "batch-rb",
            lambda batch_id, body: {"ok": True, "data": {}},
            method="rollback",
        )
        decision = {
            "operations": [{
                "api_id": "POST /api/cabinet-power/batches/{batch_id}/rollback",
                "path_params": {"batch_id": "batch-rb"},
            }],
        }
        plan = agent.prepare(E_ACTOR, decision, self.operation_id, [], queries={"query_batch": _rollback_snapshot()})
        self.assertEqual(plan["status"], "needs_input")
        control = next(f for f in plan["fields"] if f["name"] == "step0.row_ids")
        self.assertEqual([opt["value"] for opt in control["options"]], ["rb-row-1"])
        plan = agent.amend(E_ACTOR, plan["id"], {"version": plan["version"], "values": {"step0.row_ids": ["rb-row-1"]}})
        await agent._execute(E_ACTOR, agent.get_plan(E_ACTOR, plan["id"]), self.request)
        self.assertEqual(writes, [{"batch_id": "batch-rb", "body": {"version": 5, "row_ids": ["rb-row-1"]}}])

    async def test_restore_rows_builds_restorable_options_and_rejects_all(self):
        restore_rows = [
            _row("restore-row-1", "E", room="E401", rack="RACK-E4", action="恢复",
                 actual="待恢复", confirmable=False, restorable=True),
            _row("restore-row-2", "A", room="A401", rack="RACK-A4", action="恢复",
                 actual="待恢复", confirmable=False, restorable=True),
        ]
        snapshot = {"batch_id": "batch-rs", "version": 9, "scopes": ["E"], "rows": restore_rows}
        agent, writes = self._agent(
            "batch-rs",
            lambda batch_id, body: {"ok": True, "data": {}},
            method="restore-rows",
        )
        decision = {
            "operations": [{
                "api_id": "POST /api/cabinet-power/batches/{batch_id}/restore-rows",
                "path_params": {"batch_id": "batch-rs"},
            }],
        }
        plan = agent.prepare(E_ACTOR, decision, self.operation_id, [], queries={"query_batch": snapshot})
        control = next(f for f in plan["fields"] if f["name"] == "step0.row_ids")
        self.assertEqual([opt["value"] for opt in control["options"]], ["restore-row-1"])
        plan = agent.amend(E_ACTOR, plan["id"], {"version": plan["version"], "values": {"step0.row_ids": ["restore-row-1"]}})
        await agent._execute(E_ACTOR, agent.get_plan(E_ACTOR, plan["id"]), self.request)
        # Native restore-rows requires version + row_ids (no all=true scope).
        self.assertEqual(writes, [{"batch_id": "batch-rs", "body": {"version": 9, "row_ids": ["restore-row-1"]}}])

        # all=true is not a legal restore-rows range.
        all_decision = {
            "operations": [{
                "api_id": "POST /api/cabinet-power/batches/{batch_id}/restore-rows",
                "path_params": {"batch_id": "batch-rs"},
                "body": {"all": True},
            }],
        }
        with self.assertRaisesRegex(Exception, "整批操作不能同时指定部分机柜或楼栋"):
            agent.prepare(E_ACTOR, all_decision, self.operation_id, [], queries={"query_batch": snapshot})

    # ------------------------------------------------------------------ #
    # High-risk two-stage confirm flow
    # ------------------------------------------------------------------ #

    async def test_confirm_two_stages_native_receives_exact_row_ids_no_duplicate(self):
        agent, writes = self._agent(
            "batch-001",
            lambda batch_id, body: {"ok": True, "data": {}},
        )
        decision = {
            "operations": [{
                "api_id": "POST /api/cabinet-power/batches/{batch_id}/confirm",
                "path_params": {"batch_id": "batch-001"},
            }],
        }
        plan = agent.prepare(E_ACTOR, decision, self.operation_id, [], queries={"query_batch": _confirm_snapshot()})
        plan = agent.amend(E_ACTOR, plan["id"], {"version": plan["version"], "values": {"step0.row_ids": ["row-e-1"]}})
        self.assertEqual(plan["status"], "awaiting_confirmation")
        self.assertEqual(plan["risk"], "high")

        plan = await agent.confirm(E_ACTOR, plan["id"], {"version": plan["version"], "stage": "review"}, self.request)
        self.assertEqual(plan["status"], "awaiting_second_confirmation")
        self.assertEqual(writes, [])

        await agent.confirm(E_ACTOR, plan["id"], {"version": plan["version"], "stage": "execute"}, self.request)
        running = agent.get_plan(E_ACTOR, plan["id"])
        self.assertEqual(running["status"], "running")
        # Repeat click while running must not enqueue a second write.
        await agent.confirm(E_ACTOR, running["id"], {"version": running["version"], "stage": "execute"}, self.request)

        await gather_tasks(agent)
        finished = agent.get_plan(E_ACTOR, plan["id"])
        self.assertEqual(finished["status"], "completed", finished.get("error"))
        # Exactly one native call with snapshot version + exact row_ids.
        self.assertEqual(writes, [{"batch_id": "batch-001", "body": {"version": 3, "row_ids": ["row-e-1"]}}])

    async def test_confirm_all_true_two_stages_native_receives_fixed_version_and_all(self):
        agent, writes = self._agent(
            "batch-001",
            lambda batch_id, body: {"ok": True, "data": {}},
        )
        snapshot = _confirm_snapshot_e_only(version=8)
        decision = {
            "operations": [{
                "api_id": "POST /api/cabinet-power/batches/{batch_id}/confirm",
                "path_params": {"batch_id": "batch-001"},
                "body": {"all": True},
            }],
        }
        plan = agent.prepare(E_ACTOR, decision, self.operation_id, [], queries={"query_batch": snapshot})
        self.assertEqual(plan["status"], "awaiting_confirmation")

        plan = await agent.confirm(E_ACTOR, plan["id"], {"version": plan["version"], "stage": "review"}, self.request)
        self.assertEqual(plan["status"], "awaiting_second_confirmation")
        await agent.confirm(E_ACTOR, plan["id"], {"version": plan["version"], "stage": "execute"}, self.request)
        running = agent.get_plan(E_ACTOR, plan["id"])
        await agent.confirm(E_ACTOR, running["id"], {"version": running["version"], "stage": "execute"}, self.request)

        await gather_tasks(agent)
        finished = agent.get_plan(E_ACTOR, plan["id"])
        self.assertEqual(finished["status"], "completed", finished.get("error"))
        # Native receives fixed snapshot version + all=true, without row ids.
        self.assertEqual(writes, [{"batch_id": "batch-001", "body": {"version": 8, "all": True}}])

    async def test_stale_native_409_leaves_plan_failed_and_never_retries(self):
        writes = []

        def route(batch_id: str, body):
            writes.append({"batch_id": batch_id, "body": body})
            from fastapi import HTTPException
            raise HTTPException(status_code=409, detail="版本已过期")

        agent = self._agent_stale("batch-001", route)

        decision = {
            "operations": [{
                "api_id": "POST /api/cabinet-power/batches/{batch_id}/confirm",
                "path_params": {"batch_id": "batch-001"},
            }],
        }
        plan = agent.prepare(E_ACTOR, decision, self.operation_id, [], queries={"query_batch": _confirm_snapshot()})
        plan = agent.amend(E_ACTOR, plan["id"], {"version": plan["version"], "values": {"step0.row_ids": ["row-e-1"]}})
        plan = await agent.confirm(E_ACTOR, plan["id"], {"version": plan["version"], "stage": "review"}, self.request)
        await agent.confirm(E_ACTOR, plan["id"], {"version": plan["version"], "stage": "execute"}, self.request)

        await gather_tasks(agent)
        finished = agent.get_plan(E_ACTOR, plan["id"])
        self.assertEqual(finished["status"], "failed")
        self.assertIn("版本", finished.get("error", ""))
        # Stale native 409 -> exactly one attempt, never auto-retried.
        self.assertEqual(len(writes), 1)
        self.assertEqual(writes[0]["body"], {"version": 3, "row_ids": ["row-e-1"]})

    def _agent_stale(self, batch_id, handler):
        app, method = FastAPI(), "confirm"
        path = f"/api/cabinet-power/batches/{{batch_id}}/{method}"

        async def route(batch_id: str, request: Request):
            body = json.loads(await request.body() or b"null")
            return handler(batch_id, body)

        app.add_api_route(path, route, methods=["POST"])
        return PortalAgent(self.assistant, PortalAPICatalog(app), self.files)


if __name__ == "__main__":
    unittest.main(verbosity=2)
