"""Isolated PortalAgent native cabinet row-edit (PATCH) workflow tests.

Exercises the real ``PortalAgent`` flow for the new native interaction
``PATCH /api/cabinet-power/batches/{batch_id}`` (待办行编辑):

    prepare -> amend -> confirm(review) -> confirm(execute)

Contract points covered:
- Complete-detail snapshot requirement and version freezing from the snapshot.
- The UI only exposes rows the actor has permission to edit (scope + editable).
- Existing proof files / read-only audit entries are not exposed as editable
  controls and are preserved by the underlying update.
- Only actually-changed fields are submitted (delta rows).
- ``common`` pre-fill and ``row_ids`` checked-row selection.
- A no-change save is refused.
- A failed operation must carry a failure reason.
- An existing (invalid for current state) action may be kept unchanged but a
  newly-selected disabled option is rejected.
- Cross-building permission, completed rows, cancelled batches, partial_rows
  snapshots and native version conflicts produce no (successful) write.
- Repeating the execute confirmation does not duplicate the native write.
- ``acknowledge_warnings`` must be checked by the user and must not cross
  building scopes.

The native layer is the real ``CabinetBatchService.update`` running against an
isolated SQLite batch store with ``FakeFeishu``/``MemoryStore`` fakes (reusing
the ``bin.test_cabinet_power`` fixture by module composition).  No production
service, network or remote business writes are performed.

Run from the repository root (matches the other cabinet workflow modules)::

    python -m unittest bin.test_lighthouse_cabinet_edit_workflows
"""
from __future__ import annotations

import copy
import unittest
import uuid

from fastapi import FastAPI, Request, HTTPException

from . import test_cabinet_power as cb
from . import test_lighthouse_agent_workflows as workflows
from .lan_bitable_template_portal.cabinet_power_excel import CabinetError
from .lan_bitable_template_portal.lighthouse_agent import PortalAgent
from .lan_bitable_template_portal.lighthouse_api import PortalAPICatalog
from .lan_bitable_template_portal.lighthouse_ai import AssistantError

PATCH_API = "PATCH /api/cabinet-power/batches/{batch_id}"
_ROW_FIELD = "step0.cabinet_rows"
_ACK_FIELD = "step0.acknowledge_warnings"

# A row that satisfies the native manual-batch validation for scope E:
# room ``[1-4]\\d{2}``, rack ``[A-Z]\\d{2}``, valid OPS action and timestamps.
_READY_ROW = {
    "scope": "E", "room": "101", "rack": "E01", "action": "上正式电",
    "expected": "2026-09-16 16:02:59", "actual": "2026-09-16 16:03:16",
    "result": "成功",
}


def _row_map(batch):
    return {row["row_id"]: row for row in batch.get("rows", [])}


class CabinetEditWorkflowTests(unittest.IsolatedAsyncioTestCase):
    """Composition fixture: workflow agent + real CabinetBatchService(PATCH)."""

    @classmethod
    def setUpClass(cls):
        cb.CabinetPowerTests.setUpClass()

    async def asyncSetUp(self):
        self.wf = workflows.WorkflowTests()
        self.wf.setUp()
        for callback, args, kwargs in self.wf._cleanups:
            self.addCleanup(callback, *args, **kwargs)
        self.wf._cleanups.clear()

        self.cabfix = cb.CabinetPowerTests()
        self.cabfix.setUp()
        self.addCleanup(self.cabfix.tearDown)
        self.service = self.cabfix.service

        app = self.wf.catalog.app
        self.auth = {"owner": "", "allowed": [], "admin": False}
        self.patch_received = []
        self.fail_409 = False

        @app.patch("/api/cabinet-power/batches/{batch_id}")
        async def cabinet_batch_edit(batch_id: str, request: Request):
            payload = await request.json()
            self.patch_received.append(payload)
            if self.fail_409:
                raise HTTPException(status_code=409, detail="版本已过期")
            try:
                data = self.service.batches.update(
                    batch_id,
                    payload,
                    self.auth["owner"],
                    sorted(self.auth["allowed"]),
                    bool(self.auth["admin"]),
                )
                data = self.service.batches.visible(
                    data, self.auth["owner"], sorted(self.auth["allowed"]), bool(self.auth["admin"])
                )
                return {"ok": True, "data": data}
            except CabinetError as exc:
                raise HTTPException(status_code=exc.status_code, detail=str(exc)) from None

        self.catalog = PortalAPICatalog(app)
        self.agent = PortalAgent(self.wf.assistant, self.catalog, self.wf.files)
        self.request = self.wf.request
        self.actor = {"id": "cabinet-owner-e", "scopes": ["E"], "is_admin": False}

    async def asyncTearDown(self):
        await workflows.gather_tasks(self.agent)

    # ------------------------------------------------------------------ helpers

    def _set_auth(self, actor=None):
        actor = actor or self.actor
        self.auth.update(
            owner=actor["id"],
            allowed=list(actor.get("scopes") or []),
            admin=bool(actor.get("is_admin")),
        )
        return actor

    def _create_batch(self, rows, owner="cabinet-owner-e"):
        batch = self.service.batches.create_manual(rows, owner)
        return self.service.batches.get(batch["batch_id"])

    def _snapshot(self, batch_id, allowed=None, actor=None):
        actor = self._set_auth(actor)
        allowed = allowed or list(actor["scopes"])
        return self.service.batches.visible(
            self.service.batches.get(batch_id), actor["id"], sorted(allowed), bool(actor["is_admin"])
        )

    def _decision(self, batch_id, body=None):
        return {
            "title": "机柜待办行编辑",
            "explanation": "编辑本批待办机柜行",
            "operations": [
                {
                    "api_id": PATCH_API,
                    "path_params": {"batch_id": batch_id},
                    "body": body or {},
                }
            ],
        }

    def _prepare(self, batch_id, snapshot, body=None, actor=None, turn=None):
        actor = self._set_auth(actor)
        turn = turn or ("cabinet-turn-" + uuid.uuid4().hex)
        return self.agent.prepare(actor, self._decision(batch_id, body), turn, [], queries={"snapshot": snapshot})

    def _row_field(self, plan):
        return next(f for f in plan["fields"] if f["name"] == _ROW_FIELD)

    def _first_editable_row(self, plan):
        field = self._row_field(plan)
        return field["children"][0]["path"]

    def _amend(self, plan, values, actor=None):
        actor = self._set_auth(actor)
        return self.agent.amend(actor, plan["id"], {"version": plan["version"], "values": values})

    async def _double_confirm(self, amended, actor=None):
        actor = self._set_auth(actor)
        review = await self.agent.confirm(
            actor, amended["id"], {"version": amended["version"], "stage": "review"}, self.request
        )
        self.assertEqual(review["status"], "awaiting_second_confirmation")
        execute = await self.agent.confirm(
            actor, amended["id"], {"version": review["version"], "stage": "execute"}, self.request
        )
        self.assertEqual(execute["status"], "running")
        return execute

    async def _run_to_completion(self, amended, actor=None):
        actor = actor or self.actor
        await self._double_confirm(amended, actor)
        await workflows.gather_tasks(self.agent)
        return self.agent.get_plan(actor, amended["id"])

    def _amended_body(self, amended):
        return amended["operations"][0]["body"]

    # ------------------------------------------------------------------ tests

    async def test_prepare_freeze_full_snapshot_and_expose_only_editable_rows(self):
        batch = self._create_batch([_READY_ROW, {**_READY_ROW, "room": "102", "rack": "E02"}])
        snapshot = self._snapshot(batch["batch_id"])
        plan = self._prepare(batch["batch_id"], snapshot)

        self.assertEqual(plan["status"], "needs_input")
        self.assertEqual(plan["risk"], "high")
        op = plan["operations"][0]
        self.assertEqual(op["api_id"], PATCH_API)
        self.assertEqual(op["path_params"]["batch_id"], batch["batch_id"])
        # Version and rows come from the frozen full snapshot.
        self.assertEqual(op["body"]["version"], snapshot["version"])
        self.assertEqual(op["body"]["response_mode"], "delta")

        field = self._row_field(plan)
        self.assertTrue(field.get("native_cabinet_edit"))
        editable = [child["path"] for child in field["children"]]
        self.assertEqual(len(editable), 2)
        for row in snapshot["rows"]:
            self.assertTrue(row.get("editable"))
            self.assertIn(row["row_id"], editable)

    async def test_only_permitted_scoped_rows_are_editable_children(self):
        batch = self._create_batch([_READY_ROW, {**_READY_ROW, "scope": "A", "room": "101", "rack": "A01"}])
        snapshot = self._snapshot(batch["batch_id"])
        # E-only actor: the A-scope row is present in the snapshot but not editable.
        editable_rows = [row["row_id"] for row in snapshot["rows"] if row.get("editable")]
        self.assertEqual(len(editable_rows), 1)

        plan = self._prepare(batch["batch_id"], snapshot)
        field = self._row_field(plan)
        children = [child["path"] for child in field["children"]]
        self.assertEqual(children, editable_rows)
        # No A-scope row appears as an editable child.
        self.assertTrue(all(
            next(r for r in snapshot["rows"] if r["row_id"] == child)["scope"] == "E"
            for child in children
        ))

    async def test_proof_and_readonly_audit_not_editable_but_preserved_after_update(self):
        # Build a real native PDF batch whose only row simultaneously references a
        # valid evidence image (`evidence_images` <-> `batch.images`) and the
        # source PDF (`file_id` <-> `batch.files`, source=pdf).  The seeded proof
        # items are what `visible` derives `proof_files` from; they are not the
        # non-business `images` pseudo-field used by the old test.
        batch = self._create_batch([_READY_ROW])
        batch_id = batch["batch_id"]
        stored = self.service.batches.get(batch_id)
        row_id = _row_map(stored)[next(iter(_row_map(stored)))]["row_id"]
        image_id = "e" * 60 + "1" * 4  # sha256-shaped 64-char id used by add_images
        pdf_file_id = "f1_" + "a" * 20
        pdf_sha = "b" * 64

        def seed(current):
            current["source"] = "pdf"
            current["files"] = [{
                "file_id": pdf_file_id, "name": "机柜确认单.pdf", "sha256": pdf_sha,
                "size": 4321, "status": "done", "pages": 2, "processed_pages": 2,
                "error": "", "cloud_file_token": "pdfCloudTokenE123", "cloud_scope": "E",
            }]
            row = next(r for r in current["rows"] if r["row_id"] == row_id)
            row.update(
                file_id=pdf_file_id, file_name="机柜确认单.pdf", file_sha256=pdf_sha,
                page=1, source_row=1, status="ready",
                evidence_images=[image_id],
                edits=[{"field": "action", "before": "", "after": "上正式电",
                        "owner": "seeder", "at": "2026-09-01 08:00:00"}],
            )
            current.setdefault("images", []).append({
                "image_id": image_id, "name": "proof-screenshot.png", "extension": ".png",
                "size": 2048, "status": "done", "suggestions": [], "error": "",
                "cloud_file_token": "imageCloudTokenE123", "cloud_scope": "E",
            })

        self.service.batches._change(batch_id, seed)

        # The native view derives proof_files from the source PDF link; assert it
        # is exposed (read-only) before any edit so we know the seed is real.
        snapshot = self._snapshot(batch_id)
        proof = snapshot["rows"][0]
        self.assertEqual(proof["file_id"], pdf_file_id)
        self.assertEqual(proof["proof_files"], [{
            "file_id": pdf_file_id, "name": "机柜确认单.pdf",
            "available": True, "can_download": True,
        }])

        # proof_files / evidence / edits are visible but not among editable
        # controls in the frontend form.
        plan = self._prepare(batch_id, snapshot)
        field = self._row_field(plan)
        editable_keys = {child["path"] for child in field["children"][0]["children"]}
        self.assertTrue(field.get("native_cabinet_edit"))
        self.assertNotIn("proof_files", editable_keys)
        self.assertNotIn("edits", editable_keys)
        self.assertNotIn("evidence_images", editable_keys)

        # Edit one editable field; none of the private proof state may leak into
        # the submitted delta payload.
        row_id = self._first_editable_row(plan)
        baseline = copy.deepcopy(field["_baseline"][row_id])
        changed = dict(baseline)
        changed["supplier_rack"] = "SR-PRESERVED"
        amended = self._amend(plan, {_ROW_FIELD: {row_id: changed}})
        body = self._amended_body(amended)
        submitted = body["rows"][0]
        self.assertEqual(set(submitted.keys()), {"row_id", "supplier_rack"})
        for private_key in ("evidence_images", "proof_files", "edits",
                            "file_id", "file_name", "file_sha256", "page", "source_row"):
            self.assertNotIn(private_key, body)
            self.assertNotIn(private_key, submitted)

        after = await self._run_to_completion(amended)
        self.assertEqual(after["status"], "completed")

        saved_batch = self.service.batches.get(batch_id)
        saved = _row_map(saved_batch)[row_id]
        # Original evidence image is preserved and still linked in batch.images.
        self.assertEqual(saved.get("evidence_images"), [image_id])
        self.assertEqual([img["image_id"] for img in saved_batch.get("images", [])], [image_id])
        self.assertEqual(saved_batch["images"][0]["cloud_file_token"], "imageCloudTokenE123")
        # Source PDF link and its file metadata / cloud token are preserved.
        self.assertEqual(saved["file_id"], pdf_file_id)
        self.assertEqual(saved_batch["source"], "pdf")
        self.assertEqual([f["file_id"] for f in saved_batch.get("files", [])], [pdf_file_id])
        self.assertEqual(saved_batch["files"][0]["cloud_file_token"], "pdfCloudTokenE123")
        self.assertEqual(saved_batch["files"][0]["sha256"], pdf_sha)
        self.assertEqual(saved_batch["files"][0]["name"], "机柜确认单.pdf")
        # Read-only audit preserved and still grown by this update.
        prior_edits = [e for e in saved.get("edits", [])
                       if e.get("field") == "action" and e.get("owner") == "seeder"]
        self.assertTrue(prior_edits)
        self.assertGreaterEqual(len(saved.get("edits", [])), 2)

    async def test_submits_only_changed_fields_as_delta(self):
        batch = self._create_batch([_READY_ROW])
        snapshot = self._snapshot(batch["batch_id"])
        plan = self._prepare(batch["batch_id"], snapshot)
        field = self._row_field(plan)
        row_id = self._first_editable_row(plan)
        baseline = copy.deepcopy(field["_baseline"][row_id])
        changed = dict(baseline)
        changed["supplier_rack"] = "SR-DELTA"
        amended = self._amend(plan, {_ROW_FIELD: {row_id: changed}})

        body = self._amended_body(amended)
        self.assertEqual(body["response_mode"], "delta")
        self.assertEqual(len(body["rows"]), 1)
        submitted = body["rows"][0]
        # Only the changed field is transmitted.
        self.assertEqual(set(submitted.keys()), {"row_id", "supplier_rack"})
        self.assertNotIn("action", submitted)
        self.assertNotIn("scope", submitted)

        after = await self._run_to_completion(amended)
        self.assertEqual(after["status"], "completed")
        self.assertEqual(self.patch_received[-1]["rows"][0], submitted)

    async def test_common_prefills_and_selected_rows(self):
        # Three editable rows; only the first two are selected for the common
        # pre-fill.  The unselected third row must stay untouched.
        batch = self._create_batch([
            _READY_ROW,
            {**_READY_ROW, "room": "102", "rack": "E02"},
            {**_READY_ROW, "room": "103", "rack": "E03"},
        ])
        batch_id = batch["batch_id"]
        snapshot = self._snapshot(batch_id)
        all_ids = [row["row_id"] for row in snapshot["rows"]]
        self.assertEqual(len(all_ids), 3)
        selected = all_ids[:2]
        untouched = all_ids[2]
        plan = self._prepare(
            batch_id, snapshot,
            body={"common": {"rack_type": "服务器机柜"}, "row_ids": selected},
        )
        field = self._row_field(plan)
        # Only the selected rows are pre-filled with the common value; the third
        # row is never part of the editable initial form.
        self.assertEqual(set(field["_initial_form"].keys()), set(selected))
        for row_id in selected:
            self.assertEqual(field["_initial_form"][row_id]["rack_type"], "服务器机柜")

        # Confirming the pre-filled values submits exactly the common-changed
        # field and only for the two selected rows.
        amended = self._amend(plan, {_ROW_FIELD: copy.deepcopy(field["_initial_form"])})
        body = self._amended_body(amended)
        self.assertEqual(body["response_mode"], "delta")
        submitted_ids = [row["row_id"] for row in body["rows"]]
        self.assertEqual(set(submitted_ids), set(selected))
        self.assertNotIn(untouched, submitted_ids)
        self.assertTrue(all(row.get("rack_type") == "服务器机柜" for row in body["rows"]))
        self.assertTrue(all(set(row.keys()) == {"row_id", "rack_type"} for row in body["rows"]))

        # Run the update and confirm the unselected third row's rack_type is
        # unchanged on disk while the two selected rows received the new value.
        after = await self._run_to_completion(amended)
        self.assertEqual(after["status"], "completed")
        saved_rows = _row_map(self.service.batches.get(batch_id))
        self.assertEqual(saved_rows[untouched]["rack_type"], "")
        for row_id in selected:
            self.assertEqual(saved_rows[row_id]["rack_type"], "服务器机柜")

    async def test_no_change_save_is_refused(self):
        batch = self._create_batch([_READY_ROW])
        snapshot = self._snapshot(batch["batch_id"])
        plan = self._prepare(batch["batch_id"], snapshot)
        field = self._row_field(plan)
        before_writes = len(self.patch_received)
        with self.assertRaises(AssistantError) as ctx:
            self._amend(plan, {_ROW_FIELD: copy.deepcopy(field["_initial_form"])})
        self.assertIn("无需保存", str(ctx.exception))
        self.assertEqual(len(self.patch_received), before_writes)

    async def test_failure_requires_reason(self):
        batch = self._create_batch([_READY_ROW])
        snapshot = self._snapshot(batch["batch_id"])
        plan = self._prepare(batch["batch_id"], snapshot)
        field = self._row_field(plan)
        row_id = self._first_editable_row(plan)
        baseline = copy.deepcopy(field["_baseline"][row_id])

        failed = dict(baseline)
        failed["result"] = "失败"
        failed["failure_reason"] = ""
        with self.assertRaises(AssistantError) as ctx:
            self._amend(plan, {_ROW_FIELD: {row_id: failed}})
        self.assertIn("失败原因", str(ctx.exception))

        with_reason = dict(failed)
        with_reason["failure_reason"] = "电源模块损坏，已报修"
        amended = self._amend(plan, {_ROW_FIELD: {row_id: with_reason}})
        submitted = self._amended_body(amended)["rows"][0]
        self.assertEqual(submitted["result"], "失败")
        # The reason is normalized (full-width comma folded to ASCII) but kept.
        self.assertEqual(submitted["failure_reason"], "电源模块损坏,已报修")

    async def test_existing_invalid_action_kept_when_unchanged_and_not_newly_selectable(self):
        # Build a real manual batch whose stored row already has the OPS action
        # "上测试电".  The frontend view is forced to the image source with a
        # "formal" current power state so that action becomes a kept-disabled
        # option (like a legacy file row) instead of a fresh selectable choice.
        batch = self._create_batch([{**_READY_ROW, "action": "上测试电"}])
        batch_id = batch["batch_id"]
        snapshot = self._snapshot(batch_id)
        snapshot["source"] = "image"
        for row in snapshot["rows"]:
            row["editable"] = True
            row["current_power_state"] = "formal"

        plan = self._prepare(batch_id, snapshot)
        field = self._row_field(plan)
        row_id = self._first_editable_row(plan)
        action_control = next(
            child for child in field["children"][0]["children"] if child["path"] == "action"
        )
        disabled_values = [
            option["value"] for option in action_control["options"] if option.get("disabled")
        ]
        self.assertEqual(disabled_values, ["上测试电"])

        # Keep the existing (invalid-for-state) action unchanged; only a related
        # field changes.  The action must not be resent and must be preserved.
        baseline = copy.deepcopy(field["_baseline"][row_id])
        changed = dict(baseline)
        changed["supplier_rack"] = "SR-KEEP"
        amended = self._amend(plan, {_ROW_FIELD: {row_id: changed}})
        submitted = self._amended_body(amended)["rows"][0]
        self.assertNotIn("action", submitted)
        after = await self._run_to_completion(amended)
        self.assertEqual(after["status"], "completed")
        saved = _row_map(self.service.batches.get(batch_id))[row_id]
        self.assertEqual(saved["action"], "上测试电")
        self.assertEqual(saved["supplier_rack"], "SR-KEEP")

        # Newly selecting a value that is not an allowed (non-disabled) option is
        # rejected for a fresh plan.
        plan_b = self._prepare(batch_id, snapshot, turn="cabinet-turn-b-" + uuid.uuid4().hex)
        field_b = self._row_field(plan_b)
        row_id_b = self._first_editable_row(plan_b)
        baseline_b = copy.deepcopy(field_b["_baseline"][row_id_b])
        bad = dict(baseline_b)
        bad["action"] = "上正式电"  # not allowed for formal state, not the kept value
        bad["supplier_rack"] = "SR-BAD"
        with self.assertRaises(AssistantError) as ctx:
            self._amend(plan_b, {_ROW_FIELD: {row_id_b: bad}})
        self.assertIn("请重新选择", str(ctx.exception))

    async def test_cross_building_scope_not_editable(self):
        batch = self._create_batch([_READY_ROW, {**_READY_ROW, "scope": "A", "room": "101", "rack": "A01"}])
        snapshot = self._snapshot(batch["batch_id"])
        plan = self._prepare(batch["batch_id"], snapshot)
        field = self._row_field(plan)
        children = [child["path"] for child in field["children"]]
        self.assertEqual(len(children), 1)
        # The only child belongs to the E scope.
        child_row = next(r for r in snapshot["rows"] if r["row_id"] == children[0])
        self.assertEqual(child_row["scope"], "E")

    async def test_completed_row_is_not_editable(self):
        batch = self._create_batch([_READY_ROW, {**_READY_ROW, "room": "102", "rack": "E02"}])
        batch_id = batch["batch_id"]
        stored = self.service.batches.get(batch_id)
        completed_row = next(r for r in stored["rows"] if r["room"] == "102")
        self.service.batches._change(
            batch_id,
            lambda b: next(r for r in b["rows"] if r["row_id"] == completed_row["row_id"]).update(
                status="completed"
            ),
        )
        snapshot = self._snapshot(batch_id)
        editable = [row["row_id"] for row in snapshot["rows"] if row.get("editable")]
        self.assertEqual(len(editable), 1)
        plan = self._prepare(batch_id, snapshot)
        self.assertEqual([child["path"] for child in self._row_field(plan)["children"]], editable)

    async def test_cancelled_and_partial_rows_snapshots_do_not_write(self):
        batch = self._create_batch([_READY_ROW])
        batch_id = batch["batch_id"]
        before_writes = len(self.patch_received)

        cancelled = copy.deepcopy(self._snapshot(batch_id))
        cancelled["status"] = "cancelled"
        with self.assertRaises(AssistantError) as ctx:
            self._prepare(batch_id, cancelled)
        self.assertIn("作废", str(ctx.exception))

        partial = copy.deepcopy(self._snapshot(batch_id))
        partial["partial_rows"] = True
        with self.assertRaises(AssistantError) as ctx:
            self._prepare(batch_id, partial)
        self.assertIn("完整", str(ctx.exception))

        self.assertEqual(len(self.patch_received), before_writes)

    async def test_native_version_conflict_leaves_plan_failed_without_retry(self):
        batch = self._create_batch([_READY_ROW])
        snapshot = self._snapshot(batch["batch_id"])
        plan = self._prepare(batch["batch_id"], snapshot)
        field = self._row_field(plan)
        row_id = self._first_editable_row(plan)
        baseline = copy.deepcopy(field["_baseline"][row_id])
        changed = dict(baseline)
        changed["supplier_rack"] = "SR-CONFLICT"
        amended = self._amend(plan, {_ROW_FIELD: {row_id: changed}})
        before_writes = len(self.patch_received)

        self.fail_409 = True
        try:
            review = await self.agent.confirm(
                self.actor, amended["id"], {"version": amended["version"], "stage": "review"}, self.request
            )
            await self.agent.confirm(
                self.actor, amended["id"], {"version": review["version"], "stage": "execute"}, self.request
            )
            await workflows.gather_tasks(self.agent)
        finally:
            self.fail_409 = False
        final = self.agent.get_plan(self.actor, amended["id"])
        self.assertEqual(final["status"], "failed")
        self.assertIn("版本已过期", final.get("error", ""))
        # The single failed native attempt happened; no subsequent retry writes.
        self.assertEqual(len(self.patch_received), before_writes + 1)

    async def test_repeated_execute_confirm_does_not_duplicate_write(self):
        batch = self._create_batch([_READY_ROW])
        snapshot = self._snapshot(batch["batch_id"])
        plan = self._prepare(batch["batch_id"], snapshot)
        field = self._row_field(plan)
        row_id = self._first_editable_row(plan)
        baseline = copy.deepcopy(field["_baseline"][row_id])
        changed = dict(baseline)
        changed["supplier_rack"] = "SR-REPEAT"
        amended = self._amend(plan, {_ROW_FIELD: {row_id: changed}})
        before_writes = len(self.patch_received)

        await self._double_confirm(amended)
        running = self.agent.get_plan(self.actor, amended["id"])
        # A repeated execute confirmation while running must not re-enqueue.
        await self.agent.confirm(
            self.actor, running["id"], {"version": running["version"], "stage": "execute"}, self.request
        )
        await workflows.gather_tasks(self.agent)
        final = self.agent.get_plan(self.actor, amended["id"])
        self.assertEqual(final["status"], "completed")
        self.assertEqual(len(self.patch_received), before_writes + 1)

    async def test_acknowledge_warnings_must_be_checked_and_not_cross_building(self):
        batch = self._create_batch([_READY_ROW, {**_READY_ROW, "scope": "A", "room": "101", "rack": "A01"}])
        batch_id = batch["batch_id"]

        # Cross-building acknowledge is rejected at prepare time.
        cross_snapshot = self._snapshot(batch_id, allowed=["E", "A"])
        foreign_actor = {"id": "cabinet-owner-a", "scopes": ["A"], "is_admin": False}
        with self.assertRaises(AssistantError) as ctx:
            self._prepare(batch_id, cross_snapshot, body={"acknowledge_warnings": True}, actor=foreign_actor)
        self.assertEqual(ctx.exception.status, 403)

        # In-scope acknowledge requires the user to tick the checkbox.
        eonly = self._create_batch([_READY_ROW])
        e_snapshot = self._snapshot(eonly["batch_id"])
        plan = self._prepare(
            eonly["batch_id"],
            copy.deepcopy(e_snapshot),
            body={"acknowledge_warnings": True},
        )
        ack_field = next(f for f in plan["fields"] if f.get("native_cabinet_warning"))
        self.assertEqual(ack_field["name"], _ACK_FIELD)
        with self.assertRaises(AssistantError) as ctx:
            self._amend(plan, {_ACK_FIELD: False})
        self.assertIn("勾选", str(ctx.exception))

        # Ticking it wires acknowledge_warnings=True into the native payload.
        amended = self._amend(plan, {_ACK_FIELD: True})
        body = self._amended_body(amended)
        self.assertTrue(body.get("acknowledge_warnings"))
        self.assertEqual(body.get("rows"), [])
        before_writes = len(self.patch_received)
        after = await self._run_to_completion(amended)
        self.assertEqual(after["status"], "completed")
        self.assertEqual(self.patch_received[-1]["acknowledge_warnings"], True)
        self.assertGreater(len(self.patch_received), before_writes)


if __name__ == "__main__":
    unittest.main()