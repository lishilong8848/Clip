"""Isolated PortalAgent cabinet current-batch text-form workflow tests.

Exercises the real ``PortalAgent`` flow for the new ``native_cabinet_text_fill``
form:

    prepare -> preview_cabinet_text -> amend -> confirm(review) -> confirm(execute)

The native layer is the real ``CabinetBatchService.preview_text_fill`` /
``apply_text_fill`` running against an isolated SQLite batch store with
``FakeFeishu``/``MemoryStore`` fakes (reusing the ``bin.test_cabinet_power``
fixture by module composition).  No production service, network or remote
business writes are performed.

Run with unittest from the repository root (matches the repository's other
test modules, e.g. ``bin.test_cabinet_power``), e.g.::

    python -m unittest bin.test_lighthouse_cabinet_text_workflows -v
"""
from __future__ import annotations

import copy
import json
import unittest
import uuid
from types import SimpleNamespace

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from . import test_cabinet_power as cb
from . import test_lighthouse_agent_workflows as workflows
from .lan_bitable_template_portal.cabinet_power_excel import CabinetError
from .lan_bitable_template_portal.lighthouse_agent import PLAN_NAMESPACE, PortalAgent
from .lan_bitable_template_portal.lighthouse_api import PortalAPICatalog
from .lan_bitable_template_portal.lighthouse_ai import AssistantError
from .tools.lighthouse_test_backend import install_test_backend as install_lighthouse_routes

LARGE_LINE = "EA118-E2-2 A11 测试电转正式电 2026-09-16 16:02:59 2026-09-14 16:03:16 A11 Success"
_AMEND_FIELD = "step0.text_fill"


def _row_map(batch):
    return {row["row_id"]: row for row in batch.get("rows", [])}


class CabinetTextWorkflowTests(unittest.IsolatedAsyncioTestCase):
    """Composition fixture: workflow agent + native cabinet batch service."""

    @classmethod
    def setUpClass(cls):
        # Load the (expensive) cabinet template fixtures exactly once.
        cb.CabinetPowerTests.setUpClass()

    async def asyncSetUp(self):
        self.wf = workflows.WorkflowTests()
        self.wf.setUp()
        for callback, args, kwargs in self.wf._cleanups:
            self.addCleanup(callback, *args, **kwargs)
        self.wf._cleanups.clear()

        self.cabfix = cb.CabinetPowerTests()
        self.cabfix.setUp(scopes=("A", "E"))
        self.addCleanup(self.cabfix.tearDown)
        self.service = self.cabfix.service

        app = self.wf.catalog.app
        self.auth = {"owner": "", "allowed": [], "admin": False}
        self.preview_received = []
        self.apply_received = []
        self.apply_calls = 0
        self.fail_timeout = False
        self.create_received = []

        @app.post("/api/cabinet-power/batches/text-preview")
        async def create_preview(request: Request):
            try:
                data = self.service.batches.text_preview((await request.json())["sources"], self.actor["scopes"])
                return {"ok": True, "data": data}
            except CabinetError as exc:
                raise HTTPException(exc.status_code, str(exc)) from None

        @app.post("/api/cabinet-power/batches")
        async def create_text(request: Request):
            payload = await request.json()
            self.create_received.append(copy.deepcopy(payload))
            try:
                data = self.service.batches.create_text(payload, self.actor["id"], self.actor["scopes"])
                return {"ok": True, "data": data}
            except CabinetError as exc:
                raise HTTPException(exc.status_code, str(exc)) from None

        @app.post("/api/cabinet-power/batches/{batch_id}/text-preview")
        async def cabinet_text_preview(batch_id: str, request: Request):
            payload = await request.json()
            self.preview_received.append(payload)
            try:
                data = self.service.batches.preview_text_fill(
                    batch_id,
                    payload,
                    self.auth["owner"],
                    sorted(self.auth["allowed"]),
                    bool(self.auth["admin"]),
                )
                return {"ok": True, "data": data}
            except CabinetError as exc:
                raise HTTPException(status_code=exc.status_code, detail=str(exc)) from None

        @app.post("/api/cabinet-power/batches/{batch_id}/text-apply")
        async def cabinet_text_apply(batch_id: str, request: Request):
            payload = await request.json()
            self.apply_received.append(payload)
            if self.fail_timeout:
                return JSONResponse({"ok": False, "error": "接口响应超时"}, status_code=504)
            try:
                data = self.service.batches.apply_text_fill(
                    batch_id,
                    payload,
                    self.auth["owner"],
                    sorted(self.auth["allowed"]),
                    bool(self.auth["admin"]),
                )
                self.apply_calls += 1
                data = self.service.batches.visible(
                    data, self.auth["owner"], sorted(self.auth["allowed"]), bool(self.auth["admin"])
                )
                return {"ok": True, "data": data}
            except CabinetError as exc:
                raise HTTPException(status_code=exc.status_code, detail=str(exc)) from None

        self.catalog = PortalAPICatalog(app)
        self.agent = PortalAgent(self.wf.assistant, self.catalog, self.wf.files)
        self.actor = {"id": "cabinet-owner-e", "scopes": ["E"], "is_admin": False}
        self.request = self.wf.request

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

    def _decision(self, batch_id, sources):
        return {
            "title": "机柜文本回填",
            "explanation": "回填本批识别内容",
            "operations": [
                {
                    "api_id": "POST /api/cabinet-power/batches/{batch_id}/text-apply",
                    "path_params": {"batch_id": batch_id},
                    "body": {"sources": sources},
                }
            ],
        }

    def _prepare(self, batch_id, sources, actor=None, turn="cabinet-turn"):
        actor = self._set_auth(actor)
        plan = self.agent.prepare(actor, self._decision(batch_id, sources), turn, file_ids=[])
        self.assertEqual(plan["status"], "needs_input")
        self.assertEqual(plan["risk"], "high")
        self.assertEqual(len(plan["fields"]), 1)
        return plan, actor

    def _field_sources(self, plan):
        return plan["fields"][0]["value"]["sources"]

    async def _preview(self, plan):
        return await self.agent.preview_cabinet_text(
            self.actor,
            plan["id"],
            {
                "version": plan["version"],
                "field": _AMEND_FIELD,
                "sources": self._field_sources(plan),
            },
            self.request,
        )

    async def _amend_awaiting(self, plan, row_id, text_id, text_row, version):
        value = {
            "version": version,
            "sources": self._field_sources(plan),
            "rows": [{"text_id": text_id, "text_row": text_row, "row_id": row_id}],
            "omitted": [],
        }
        pub = self.agent.amend(
            self.actor, plan["id"], {"version": plan["version"], "values": {_AMEND_FIELD: value}}
        )
        self.assertEqual(pub["status"], "awaiting_confirmation")
        return pub

    async def _double_confirm(self, amended):
        rev = await self.agent.confirm(
            self.actor, amended["id"], {"version": amended["version"], "stage": "review"}, self.request
        )
        self.assertEqual(rev["status"], "awaiting_second_confirmation")
        exc = await self.agent.confirm(
            self.actor, amended["id"], {"version": rev["version"], "stage": "execute"}, self.request
        )
        self.assertEqual(exc["status"], "running")
        await workflows.gather_tasks(self.agent)
        return self.agent.get_plan(self.actor, amended["id"])

    # ------------------------------------------------------------------ tests

    async def test_text_creation_preview_edit_confirm_and_replay(self):
        self._set_auth()
        sources = [{"id": "create_sample", "text": LARGE_LINE}]
        plan = self.agent.prepare(self.actor, {"operations": [{"api_id": "POST /api/cabinet-power/batches", "body": {"source": "text", "sources": sources}}]}, "text-create", [])
        self.assertEqual(plan["status"], "needs_input")
        self.assertEqual(len(plan["fields"]), 1)
        self.assertTrue(plan["fields"][0]["native_cabinet_text_create"])
        field = plan["fields"][0]
        preview = await self.agent.preview_cabinet_text(self.actor, plan["id"], {"version": plan["version"], "field": field["name"], "sources": sources}, self.request)
        self.assertEqual(self.create_received, [])
        self.assertEqual((preview["rows"][0]["scope"], preview["rows"][0]["room"], preview["rows"][0]["rack"]), ("E", "202", "A11"))
        rows = [{key: value for key, value in row.items() if key not in {"row_id", "issues"}} for row in preview["rows"]]
        rows[0]["actual"] = "2026-09-14T16:30:00"
        amended = self.agent.amend(self.actor, plan["id"], {"version": plan["version"], "values": {field["name"]: {"sources": sources, "rows": rows}}})
        self.assertEqual(self.create_received, [])
        done = await self._double_confirm(amended)
        self.assertEqual(done["status"], "completed", done.get("error"))
        self.assertEqual(len(self.create_received), 1)
        payload = self.create_received[0]
        self.assertEqual(payload["rows"][0]["actual"], "2026-09-14T16:30:00")
        batch = self.service.batches.create_text(payload, self.actor["id"], self.actor["scopes"])
        self.assertEqual(batch["batch_id"], done["results"][0]["data"]["batch_id"])
        await self.agent.confirm(self.actor, done["id"], {"version": done["version"], "stage": "execute"}, self.request)
        self.assertEqual(len(self.create_received), 1)

    async def test_text_creation_rejects_foreign_rows_invalid_identity_and_reason(self):
        self._set_auth()
        sources = [{"id": "create_sample", "text": LARGE_LINE}]
        plan = self.agent.prepare(self.actor, {"operations": [{"api_id": "POST /api/cabinet-power/batches", "body": {"source": "text"}}]}, "text-create-invalid", [])
        field = plan["fields"][0]
        preview = await self.agent.preview_cabinet_text(self.actor, plan["id"], {"version": plan["version"], "field": field["name"], "sources": sources}, self.request)
        row = {key: value for key, value in preview["rows"][0].items() if key not in {"row_id", "issues"}}
        for update in ({"scope": "A"}, {"text_row": True}, {"text_row": 999}, {"cloud_file_token": "forged"}, {"result": "失败", "failure_reason": ""}):
            with self.subTest(update=update), self.assertRaises(AssistantError):
                self.agent.amend(self.actor, plan["id"], {"version": plan["version"], "values": {field["name"]: {"sources": sources, "rows": [{**row, **update}]}}})
        with self.assertRaises(AssistantError):
            await self.agent.preview_cabinet_text(self.actor, plan["id"], {"version": plan["version"] + 1, "field": field["name"], "sources": sources}, self.request)
        self.assertEqual(self.create_received, [])

    async def test_large_text_creation_return_to_edit_preserves_sources_and_next_step_input(self):
        app = self.catalog.app
        companion = []
        @app.post("/api/companion-record")
        async def save(payload: _CompanionPayload):
            companion.append(payload.model_dump())
            return {"ok": True, "data": {"saved": True}}
        self.agent = PortalAgent(self.wf.assistant, PortalAPICatalog(app), self.wf.files)
        sources = [{"id": "source-large", "text": "\n".join([LARGE_LINE] * 400)}]
        plan = self.agent.prepare(self.actor, {"operations": [{"api_id": "POST /api/cabinet-power/batches", "body": {"source": "text"}},
            {"api_id": "POST /api/companion-record"}]}, "text-and-note", [])
        field = next(field for field in plan["fields"] if field.get("native_cabinet_text_create"))
        preview = await self.agent.preview_cabinet_text(self.actor, plan["id"], {"version": plan["version"], "field": field["name"], "sources": sources}, self.request)
        row = {key: value for key, value in preview["rows"][0].items() if key not in {"row_id", "issues"}}
        values = {field["name"]: {"sources": sources, "rows": [row]}, "step1.title": "首次备注"}
        amended = self.agent.amend(self.actor, plan["id"], {"version": plan["version"], "values": values})
        reopened = self.agent.amend(self.actor, amended["id"], {"version": amended["version"], "action": "edit"})
        values = {field["name"]: field["value"] for field in reopened["fields"]}
        self.assertIn("$query", values[field["name"]]["sources"][0])
        values["step1.title"] = "最终备注"
        amended = self.agent.amend(self.actor, reopened["id"], {"version": reopened["version"], "values": values})
        done = await self._double_confirm(amended)
        self.assertEqual(done["status"], "completed", done.get("error"))
        self.assertEqual(self.create_received[0]["sources"][0]["text"], sources[0]["text"])
        self.assertEqual(companion[0]["title"], "最终备注")

    async def test_prepare_creates_single_native_cabinet_text_fill_field(self):
        batch = self._create_batch(
            [{"scope": "E", "room": "202", "rack": "A11", "action": "上正式电"}]
        )
        batch_id = batch["batch_id"]
        sources = [
            {"id": "prep_source_a", "text": "EA118-E2-2 A11 上正式电 2026-09-16 16:02:59 2026-09-14 16:03:16"}
        ]
        plan, _actor = self._prepare(batch_id, sources)

        self.assertEqual(
            plan["operations"][0]["api_id"], "POST /api/cabinet-power/batches/{batch_id}/text-apply"
        )
        self.assertEqual(plan["operations"][0]["path_params"]["batch_id"], batch_id)

        field = plan["fields"][0]
        self.assertEqual(field["name"], _AMEND_FIELD)
        self.assertTrue(field.get("native_cabinet_text_fill"))
        self.assertEqual(field.get("batch_id"), batch_id)
        self.assertEqual(field.get("type"), "object")
        self.assertTrue(field.get("required"))

        # The form prefills only sources; no raw version / rows / row ids are emitted.
        self.assertEqual(set(field["_initial_form"].keys()), {"sources"})
        for leaked in ("version", "rows", "raw_text", "text_id", "text_row"):
            self.assertNotIn(leaked, field)
            self.assertNotIn(leaked, field["_initial_form"])
            self.assertNotIn(leaked, field["value"])

        # The field value references the frozen query binding (gateway style).
        self.assertIn("$query", field["value"])
        ref = field["value"]["$query"]["ref"]
        self.assertTrue(ref.startswith("query_form_"))
        self.assertEqual(plan["_queries"][ref], {"sources": sources})
        # Leaf scalars are preserved verbatim (no model truncation).
        self.assertEqual(field["value"]["sources"][0]["text"], sources[0]["text"])

    async def test_preview_cabinet_text_invokes_native_and_returns_real_rows(self):
        batch = self._create_batch(
            [{"scope": "E", "room": "202", "rack": "A11", "action": "上正式电"}]
        )
        batch_id = batch["batch_id"]
        sources = [{"id": "preview_sample", "text": LARGE_LINE}]
        plan, _actor = self._prepare(batch_id, sources)

        result = await self._preview(plan)

        real_version = self.service.batches.get(batch_id)["version"]
        self.assertEqual(result["version"], real_version)
        self.assertIsInstance(result["rows"], list)
        self.assertTrue(result["rows"])
        # Native preview returns the A11 target for the matching candidate.
        self.assertEqual(result["rows"][0]["room"], "202")
        self.assertEqual(result["rows"][0]["rack"], "A11")
        self.assertTrue(result["rows"][0]["targets"])
        # Only display keys are exposed; raw OCR/long text must not leak.
        allowed = {"text_id", "text_row", "scope", "room", "rack", "action", "expected", "actual", "targets", "issue"}
        for row in result["rows"]:
            self.assertFalse(set(row.keys()) & {"raw_text", "raw", "source_system_name"})
            self.assertLessEqual(set(row.keys()), allowed)
        # Native preview did not write anything to the batch.
        self.assertEqual(self.service.batches.get(batch_id)["version"], real_version)

    async def test_amend_sets_native_body_and_two_confirmations_apply(self):
        batch = self._create_batch(
            [
                {"scope": "E", "room": "202", "rack": "A11", "action": "上正式电", "result": "失败", "failure_reason": "保留原因"},
                {"scope": "E", "room": "202", "rack": "A12", "action": "上正式电", "result": "失败", "failure_reason": "保留原因"},
            ]
        )
        batch_id = batch["batch_id"]
        before = copy.deepcopy(self.service.batches.get(batch_id))
        remote_before = copy.deepcopy(self.cabfix.remote.records)
        sources = [
            {
                "id": "fill_two_lines",
                "text": (
                    "EA118-E2-2 A11 测试电转正式电 2026-09-16 16:02:59 2026-09-14 16:03:16 A11 Success\n"
                    "EA118-E2-2 A12 上正式电 2026-09-16 16:02:59 2026-09-14 16:03:16"
                ),
            }
        ]
        plan, _actor = self._prepare(batch_id, sources)

        preview = await self._preview(plan)
        self.assertEqual(len(preview["rows"]), 2)
        a11 = next(r for r in preview["rows"] if r["rack"] == "A11")
        a11_row_id = a11["targets"][0]["row_id"]

        amended = await self._amend_awaiting(
            plan, a11_row_id, sources[0]["id"], a11["text_row"], preview["version"]
        )

        # Amend stores a native body limited to sources/version/rows (omitted dropped).
        body = amended["operations"][0]["body"]
        self.assertEqual(set(body.keys()), {"sources", "version", "rows"})
        self.assertEqual(body["version"], preview["version"])
        self.assertEqual(body["sources"], sources)
        self.assertEqual(
            body["rows"],
            [{"text_id": sources[0]["id"], "text_row": a11["text_row"], "row_id": a11_row_id}],
        )

        finished = await self._double_confirm(amended)
        self.assertEqual(finished["status"], "completed")
        self.assertEqual(self.apply_calls, 1)

        saved = self.service.batches.get(batch_id)
        rows = _row_map(saved)
        before_map = _row_map(before)
        changed = rows[a11_row_id]
        other_id = next(rid for rid in before_map if rid != a11_row_id)
        untouched = rows[other_id]

        # Only the selected row changed in action/expected/actual.
        self.assertEqual(
            (changed["action"], changed["expected"], changed["actual"]),
            ("测试电转正式电", "2026-09-16 16:02:59", "2026-09-14 16:03:16"),
        )
        self.assertEqual((untouched["expected"], untouched["actual"]), ("", ""))
        for key in ("scope", "room", "rack", "result", "failure_reason", "original", "operation_id"):
            self.assertEqual(changed[key], before_map[a11_row_id][key], key)

        # Audit trail preserved for the edited row; no spurious edits elsewhere.
        edits = changed["edits"]
        self.assertEqual([e["field"] for e in edits], ["action", "expected", "actual"])
        self.assertTrue(all(e["source"] == "text_fill" and e["raw_text"] for e in edits))
        self.assertEqual(untouched["edits"], [])

        # No remote business writes at all.
        self.assertEqual(self.cabfix.remote.records, remote_before)
        self.assertEqual(self.cabfix.remote.creates, 0)
        self.assertEqual(self.cabfix.remote.batch_create_calls, 0)
        self.assertEqual(self.cabfix.remote.batch_update_calls, 0)

    async def test_apply_http409_returns_needs_input_retains_sources_no_retry(self):
        batch = self._create_batch(
            [{"scope": "E", "room": "202", "rack": "A11", "action": "上正式电"}]
        )
        batch_id = batch["batch_id"]
        sources = [{"id": "stale_source", "text": LARGE_LINE}]
        plan, _actor = self._prepare(batch_id, sources)

        preview = await self._preview(plan)
        a11_row_id = preview["rows"][0]["targets"][0]["row_id"]
        amended = await self._amend_awaiting(
            plan,
            a11_row_id,
            sources[0]["id"],
            preview["rows"][0]["text_row"],
            preview["version"],
        )

        # Someone else updates the batch after the form was prepared -> stale version.
        self.service.batches._change(batch_id, lambda _batch: None)
        remote_before = copy.deepcopy(self.cabfix.remote.records)

        finished = await self._double_confirm(amended)

        self.assertEqual(finished["status"], "needs_input")
        # Form retained (as gateway $query references) with the full sources; no auto rewrite.
        self.assertEqual(len(finished["fields"]), 1)
        retained = finished["fields"][0]["value"]
        self.assertEqual(retained["version"], preview["version"])
        self.assertEqual(retained["sources"][0]["id"], sources[0]["id"])
        self.assertEqual(retained["sources"][0]["text"], sources[0]["text"])
        self.assertIn("$query", retained["sources"][0])
        self.assertEqual(retained["rows"][0]["text_id"], sources[0]["id"])
        self.assertEqual(retained["rows"][0]["text_row"], preview["rows"][0]["text_row"])
        self.assertEqual(retained["rows"][0]["row_id"], a11_row_id)
        self.assertIn("$query", retained["rows"][0])
        # No automatic second write: only one apply attempt reaches the route.
        self.assertEqual(len(self.apply_received), 1)
        self.assertEqual(self.apply_calls, 0)
        self.assertEqual(self.cabfix.remote.records, remote_before)

    async def test_timeout_failure_marks_plan_failed_no_retry(self):
        batch = self._create_batch(
            [{"scope": "E", "room": "202", "rack": "A11", "action": "上正式电"}]
        )
        batch_id = batch["batch_id"]
        sources = [{"id": "timeout_source", "text": LARGE_LINE}]
        plan, _actor = self._prepare(batch_id, sources)

        preview = await self._preview(plan)
        a11_row_id = preview["rows"][0]["targets"][0]["row_id"]
        amended = await self._amend_awaiting(
            plan,
            a11_row_id,
            sources[0]["id"],
            preview["rows"][0]["text_row"],
            preview["version"],
        )

        # Simulate a native timeout on the apply call (not a 409).
        self.fail_timeout = True
        remote_before = copy.deepcopy(self.cabfix.remote.records)

        finished = await self._double_confirm(amended)

        self.assertEqual(finished["status"], "failed")
        # A timeout is not re-interpreted as needs_input and is never auto-retried.
        self.assertEqual(len(self.apply_received), 1)
        self.assertEqual(self.apply_calls, 0)
        self.assertEqual(self.cabfix.remote.records, remote_before)

    async def test_out_of_scope_and_different_owner_denied(self):
        # Batch owned by another account, scoped to E; actor only has A.
        batch = self._create_batch(
            [{"scope": "E", "room": "202", "rack": "A11", "action": "上正式电"}],
            owner="another-owner",
        )
        batch_id = batch["batch_id"]
        sources = [{"id": "denied_source", "text": LARGE_LINE}]
        actor = {"id": "scope-a-only", "scopes": ["A"], "is_admin": False}
        self._set_auth(actor)

        plan = self.agent.prepare(actor, self._decision(batch_id, sources), "denied-turn", file_ids=[])
        self.assertEqual(plan["status"], "needs_input")

        # Preview is denied natively (different owner + out of scope).
        with self.assertRaises(AssistantError) as ctx:
            await self.agent.preview_cabinet_text(
                actor,
                plan["id"],
                {"version": plan["version"], "field": _AMEND_FIELD, "sources": self._field_sources(plan)},
                self.request,
            )
        self.assertEqual(ctx.exception.status, 403)

        # Confirming/executing is also denied and must not write anything.
        row_id = batch["rows"][0]["row_id"]
        value = {
            "version": batch["version"],
            "sources": self._field_sources(plan),
            "rows": [{"text_id": sources[0]["id"], "text_row": 1, "row_id": row_id}],
        }
        amended = self.agent.amend(
            actor, plan["id"], {"version": plan["version"], "values": {_AMEND_FIELD: value}}
        )
        rev = await self.agent.confirm(
            actor, plan["id"], {"version": amended["version"], "stage": "review"}, self.request
        )
        await self.agent.confirm(actor, plan["id"], {"version": rev["version"], "stage": "execute"}, self.request)
        await workflows.gather_tasks(self.agent)
        finished = self.agent.get_plan(actor, plan["id"])
        self.assertEqual(finished["status"], "failed")
        self.assertIn("无权", finished["error"])
        self.assertEqual(self.apply_calls, 0)
        # Batch untouched and no remote writes issued.
        self.assertEqual(self.service.batches.get(batch_id), batch)
        self.assertEqual(self.cabfix.remote.creates, 0)

    async def test_readonly_completed_rows_handled_as_conflict(self):
        batch = self._create_batch(
            [{"scope": "E", "room": "202", "rack": "A11", "action": "上正式电"}]
        )
        batch_id = batch["batch_id"]
        sources = [{"id": "completed_source", "text": LARGE_LINE}]
        plan, _actor = self._prepare(batch_id, sources)

        preview = await self._preview(plan)
        a11_row_id = preview["rows"][0]["targets"][0]["row_id"]
        amended = await self._amend_awaiting(
            plan,
            a11_row_id,
            sources[0]["id"],
            preview["rows"][0]["text_row"],
            preview["version"],
        )

        # The target row becomes read-only (completed) before the apply executes.
        self.service.batches._change(batch_id, lambda b: b["rows"][0].update(status="completed"))
        remote_before = copy.deepcopy(self.cabfix.remote.records)

        finished = await self._double_confirm(amended)

        self.assertEqual(finished["status"], "needs_input")
        self.assertEqual(len(finished["fields"]), 1)
        retained = finished["fields"][0]["value"]["sources"][0]
        self.assertEqual(retained["id"], sources[0]["id"])
        self.assertEqual(retained["text"], sources[0]["text"])
        self.assertIn("$query", retained)
        self.assertEqual(len(self.apply_received), 1)
        self.assertEqual(self.apply_calls, 0)
        self.assertEqual(self.cabfix.remote.records, remote_before)

    async def test_large_source_preserved_via_gateway_beyond_128k(self):
        # A single pasted block >128kB (still under the 200k per-text cap) flows
        # through the gateway references intact rather than being clipped by the
        # model's context window.
        big_text = "\n".join([LARGE_LINE] * 2000)
        self.assertGreater(len(big_text.encode("utf-8")), 128 * 1024)

        batch = self._create_batch(
            [{"scope": "E", "room": "202", "rack": "A11", "action": "上正式电"}]
        )
        batch_id = batch["batch_id"]
        sources = [{"id": "huge_fragment", "text": big_text}]
        plan, _actor = self._prepare(batch_id, sources)

        # The public field references a query binding; preview resolves the full text.
        preview = await self._preview(plan)
        # Native preview received the complete >128k source, not a truncated copy.
        self.assertEqual(len(self.preview_received), 1)
        self.assertEqual(self.preview_received[0]["sources"][0]["text"], big_text)
        self.assertEqual(len(preview["rows"]), 2000)

        a11 = next(r for r in preview["rows"] if r["text_row"] == 1 and r["targets"])
        a11_row_id = a11["targets"][0]["row_id"]
        amended = await self._amend_awaiting(
            plan, a11_row_id, sources[0]["id"], a11["text_row"], preview["version"]
        )

        # Amend resolved the full source into the native body (untruncated store copy).
        stored = self.wf.store.get_document(PLAN_NAMESPACE, plan["id"])
        self.assertEqual(stored["operations"][0]["body"]["sources"][0]["text"], big_text)

        finished = await self._double_confirm(amended)
        self.assertEqual(finished["status"], "completed")
        self.assertEqual(self.apply_calls, 1)
        # Apply route received the complete >128k source, proving no clipping.
        self.assertEqual(len(self.apply_received), 1)
        self.assertEqual(self.apply_received[0]["sources"][0]["text"], big_text)

        saved = _row_map(self.service.batches.get(batch_id))
        edited = saved[a11_row_id]
        self.assertEqual(edited["action"], "测试电转正式电")
        self.assertEqual(edited["expected"], "2026-09-16 16:02:59")
        self.assertEqual(edited["actual"], "2026-09-14 16:03:16")
        self.assertEqual(len(edited["edits"]), 3)
        self.assertTrue(all(e["source"] == "text_fill" and e["raw_text"] for e in edited["edits"]))
        self.assertEqual(self.cabfix.remote.creates, 0)


class CabinetTextRouteTests(unittest.IsolatedAsyncioTestCase):
    """Real installed Lighthouse route (POST /api/assistant/plans/{id}/cabinet-text-preview).

    The actual ``install_lighthouse_routes`` endpoint is driven through an in-process
    ASGI transport (no service/network).  Plans are prepared by the ordinary ``PortalAgent``
    against the same isolated store the route lazily builds its own agent from, and the
    fake native cabinet batch route is registered on the same app so the gateway-based
    preview resolves real native rows.  All data is mock/isolated; no production writes.
    """

    @classmethod
    def setUpClass(cls):
        # Load the (expensive) cabinet template fixtures exactly once.
        cb.CabinetPowerTests.setUpClass()

    async def asyncSetUp(self):
        self.wf = workflows.WorkflowTests()
        self.wf.setUp()
        for callback, args, kwargs in self.wf._cleanups:
            self.addCleanup(callback, *args, **kwargs)
        self.wf._cleanups.clear()

        self.cabfix = cb.CabinetPowerTests()
        self.cabfix.setUp(scopes=("A", "E"))
        self.addCleanup(self.cabfix.tearDown)
        self.service = self.cabfix.service

        app = self.wf.catalog.app
        self.app = app
        self.auth = {"owner": "", "allowed": [], "admin": False}
        self.preview_received = []

        @app.post("/api/cabinet-power/batches/{batch_id}/text-preview")
        async def cabinet_text_preview(batch_id: str, request: Request):
            payload = await request.json()
            self.preview_received.append(payload)
            try:
                data = self.service.batches.preview_text_fill(
                    batch_id,
                    payload,
                    self.auth["owner"],
                    sorted(self.auth["allowed"]),
                    bool(self.auth["admin"]),
                )
                return {"ok": True, "data": data}
            except CabinetError as exc:
                raise HTTPException(status_code=exc.status_code, detail=str(exc)) from None

        @app.post("/api/cabinet-power/batches/{batch_id}/text-apply")
        async def cabinet_text_apply(batch_id: str, request: Request):
            # Registered only so the cabinet decision is discovered/validated by the
            # catalog; this route test never executes the apply step.
            payload = await request.json()
            try:
                data = self.service.batches.apply_text_fill(
                    batch_id,
                    payload,
                    self.auth["owner"],
                    sorted(self.auth["allowed"]),
                    bool(self.auth["admin"]),
                )
                return {"ok": True, "data": data}
            except CabinetError as exc:
                raise HTTPException(status_code=exc.status_code, detail=str(exc)) from None

        self.catalog = PortalAPICatalog(app)
        # External PortalAgent shares the route's store: it prepares plans that the
        # route's own lazily-created agent reads for preview and body-limit decisions.
        self.agent = PortalAgent(self.wf.assistant, self.catalog, self.wf.files)
        self.actor = {"id": "cabinet-route-owner", "scopes": ["E"], "is_admin": False}
        self.session = {"user": {"open_id": self.actor["id"]}, "allowed_scopes": ["E"], "role": "building"}

        controller = SimpleNamespace(
            _current_session=lambda request: self.session,
            _request_base_url=lambda request: str(request.base_url).rstrip("/"),
        )
        runtime = SimpleNamespace(
            state_store=self.wf.store,
            auth_manager=SimpleNamespace(
                is_admin=lambda s: s.get("role") == "admin",
                session_scopes=lambda s: list(s["allowed_scopes"]),
            ),
        )
        install_lighthouse_routes(app, controller, runtime)

    async def asyncTearDown(self):
        if getattr(self, "agent", None):
            await workflows.gather_tasks(self.agent)

    # ------------------------------------------------------------------ helpers

    def _set_auth(self, owner=None, scopes=None, admin=False):
        owner = owner or self.actor["id"]
        scopes = scopes or self.actor["scopes"]
        self.auth.update(owner=owner, allowed=list(scopes), admin=bool(admin))

    def _create_batch(self, rows, owner="cabinet-route-owner"):
        batch = self.service.batches.create_manual(rows, owner)
        return self.service.batches.get(batch["batch_id"])

    def _decision(self, batch_id, sources):
        return {
            "title": "机柜文本回填",
            "explanation": "回填本批识别内容",
            "operations": [
                {
                    "api_id": "POST /api/cabinet-power/batches/{batch_id}/text-apply",
                    "path_params": {"batch_id": batch_id},
                    "body": {"sources": sources},
                }
            ],
        }

    def _prepare(self, batch_id, sources, turn="route-turn"):
        self._set_auth()
        plan = self.agent.prepare(self.actor, self._decision(batch_id, sources), turn, file_ids=[])
        self.assertEqual(plan["status"], "needs_input")
        return plan

    def _field_sources(self, plan):
        return plan["fields"][0]["value"]["sources"]

    async def _post_preview(self, plan, payload):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.app, client=("127.0.0.1", 123)),
            base_url="http://testserver",
            timeout=httpx.Timeout(connect=5, read=90, write=30, pool=5),
        ) as client:
            return await client.post(
                f"/api/assistant/plans/{plan['id']}/cabinet-text-preview",
                json=payload,
                headers={"Origin": "http://testserver"},
            )

    async def test_installed_route_preview_limits_and_guards(self):
        # ------------------------------------------------------------------ happy
        # >128k source is reference-preserved in the plan ($query), resolved by the
        # real preview endpoint, and returns complete native rows through the route.
        batch = self._create_batch(
            [{"scope": "E", "room": "202", "rack": "A11", "action": "上正式电"}]
        )
        batch_id = batch["batch_id"]
        big_text = "\n".join([LARGE_LINE] * 2000)
        self.assertGreater(len(big_text.encode("utf-8")), 128 * 1024)
        plan = self._prepare(batch_id, [{"id": "route_big", "text": big_text}])
        payload = {
            "version": plan["version"],
            "field": _AMEND_FIELD,
            "sources": self._field_sources(plan),
        }

        resp = await self._post_preview(plan, payload)
        self.assertEqual(resp.status_code, 200, resp.text)
        data = resp.json()["data"]
        self.assertEqual(len(self.preview_received), 1)
        # The full >128k source reached the native preview (not a truncated copy).
        self.assertEqual(self.preview_received[0]["sources"][0]["text"], big_text)
        # Complete preview rows are returned by the route response.
        self.assertEqual(len(data["rows"]), 2000)
        self.assertEqual(data["rows"][0]["room"], "202")
        self.assertEqual(data["rows"][0]["rack"], "A11")
        self.assertTrue(data["rows"][0]["targets"])
        # Native preview did not mutate the batch.
        self.assertEqual(self.service.batches.get(batch_id)["version"], batch["version"])

        literal = {**payload, "sources": [{"id": "route_big", "text": big_text}]}
        self.assertGreater(len(json.dumps(literal).encode("utf-8")), 128000)
        resp = await self._post_preview(plan, literal)
        self.assertEqual(resp.status_code, 200, resp.text)
        self.assertEqual(len(resp.json()["data"]["rows"]), 2000)
        self.assertEqual(self.preview_received[-1]["sources"][0]["text"], big_text)

        # ------------------------------------------------------------------ stale
        # A version bump after plan preparation must be rejected before native preview.
        self.preview_received.clear()
        stored = self.wf.store.get_document(PLAN_NAMESPACE, plan["id"])
        stored["version"] += 1
        self.wf.store.put_document(PLAN_NAMESPACE, plan["id"], stored)
        resp = await self._post_preview(plan, payload)
        self.assertEqual(resp.status_code, 409, resp.text)
        self.assertIn("已变化", resp.json()["error"])
        self.assertEqual(self.preview_received, [])

        # ------------------------------------------------------------------ wrong owner
        self.wf.store.put_document(PLAN_NAMESPACE, plan["id"], stored)
        self.preview_received.clear()
        original_open_id = self.session["user"]["open_id"]
        self.session["user"]["open_id"] = "different-owner"
        try:
            resp = await self._post_preview(plan, payload)
        finally:
            self.session["user"]["open_id"] = original_open_id
        self.assertEqual(resp.status_code, 404, resp.text)
        self.assertEqual(self.preview_received, [])

        # ------------------------------------------------------------------ unknown field
        # Restore the original version, then ask for a field the plan does not have.
        stored["version"] = plan["version"]
        self.wf.store.put_document(PLAN_NAMESPACE, plan["id"], stored)
        self.preview_received.clear()
        payload_fieldless = {
            "version": plan["version"],
            "field": "step9.nonexistent",
            "sources": self._field_sources(plan),
        }
        resp = await self._post_preview(plan, payload_fieldless)
        self.assertEqual(resp.status_code, 400, resp.text)
        self.assertIn("机柜文本回填", resp.json()["error"])
        self.assertEqual(self.preview_received, [])

        # ------------------------------------------------------------------ >4MiB
        # Native plan grants a 4MiB body limit; exceeding it is rejected 413 before
        # the native preview route is reached.
        self.preview_received.clear()
        oversized = dict(payload)
        oversized["pad"] = "x" * (4 * 1024 * 1024 + 1)
        resp = await self._post_preview(plan, oversized)
        self.assertEqual(resp.status_code, 413, resp.text)
        self.assertEqual(self.preview_received, [])

        # ------------------------------------------------------------------ non-native keeps 128k
        # A plan without native_cabinet_text_fill keeps the default 128000 limit, so a
        # >128k body is also rejected as 413 (proving the 4MiB grant is native-only).
        self.preview_received.clear()
        plain = self.agent.prepare(
            self.actor,
            {"operations": [{"api_id": "POST /api/step-one", "body": {}}]},
            "route-plain-turn",
            file_ids=[],
        )
        plain_payload = {"version": plain["version"], "field": "step0.native", "sources": []}
        plain_payload["pad"] = "y" * (128 * 1024 + 100)
        resp = await self._post_preview(plain, plain_payload)
        self.assertEqual(resp.status_code, 413, resp.text)
        self.assertEqual(self.preview_received, [])


    async def test_text_create_accepts_native_large_preview_and_500_row_form_over_http(self):
        self._set_auth()
        @self.app.post("/api/cabinet-power/batches/text-preview")
        async def preview(request: Request):
            payload = await request.json()
            self.preview_received.append(payload)
            return {"ok": True, "data": self.service.batches.text_preview(payload["sources"], ["E"])}
        @self.app.post("/api/cabinet-power/batches")
        async def create(request: Request):
            raise AssertionError("Preparing a large batch must not write")
        self.agent.catalog = PortalAPICatalog(self.app)
        plan = self.agent.prepare(self.actor, {"operations": [{"api_id": "POST /api/cabinet-power/batches", "body": {"source": "text"}}]}, "large-http-create", [])
        sources = [{"id": "large_http_source", "text": "\n".join([LARGE_LINE] * 1500)}]
        payload = {"version": plan["version"], "field": plan["fields"][0]["name"], "sources": sources}
        self.assertGreater(len(json.dumps(payload).encode()), 128000)
        response = await self._post_preview(plan, payload)
        self.assertEqual(response.status_code, 200, response.text[:1000])
        rows = [{key: value for key, value in row.items() if key not in {"row_id", "issues"}} for row in response.json()["data"]["rows"][:500]]
        data = {"version": plan["version"], "values": {plan["fields"][0]["name"]: {"sources": sources, "rows": rows}}}
        self.assertGreater(len(json.dumps(data).encode()), 128000)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app), base_url="http://testserver") as client:
            response = await client.patch(f"/api/assistant/plans/{plan['id']}", json=data, headers={"Origin": "http://testserver"})
        self.assertEqual(response.status_code, 200, response.text[:1000])
        saved = self.agent.get_plan(self.actor, plan["id"])
        self.assertEqual(saved["status"], "awaiting_confirmation")
        self.assertEqual(len(saved["operations"][0]["body"]["rows"]), 500)
        self.assertEqual(saved["operations"][0]["body"]["sources"], sources)
        self.assertEqual(self.cabfix.remote.creates, 0)


class _CompanionPayload(BaseModel):
    """Tiny body model used only by the mixed-operation test."""

    title: str
    count: int = 0


class CabinetMissingBatchPickerTests(unittest.IsolatedAsyncioTestCase):
    """Lightweight in-process tests for the new missing-target batch picker.

    No heavy cabinet templates / SQLite batch store are used.  A tiny FastAPI
    list fixture stands in for the native ``GET /api/cabinet-power/batches``
    todo-list so the batch picker can be driven fully in-process.  The shared
    workflow assistant/store/files/request harness from
    ``test_lighthouse_agent_workflows`` is composed and its cleanup callbacks
    are transferred into this test case.

    Covered behaviour:

    * ``prepare`` of a ``text-apply`` without a ``batch_id`` exposes the
      ``cabinet_batches`` scoped select (not the concrete ID text form).
    * ``field_options`` pages the native todo list, only touching the actor's
      A-E scopes, supports keyword search across pages, rejects malformed /
      repeated pages instead of claiming an empty result, and forwards
      ``from``/``to`` date filters.
    * Forged selections and out-of-scope scope queries are rejected.
    * Selecting a batch amends into a ``needs_input`` ``native_cabinet_text_fill``
      form with the original sources retained via frozen ``$query`` references,
      with no write/confirm executed.
    * A mixed-operation plan does not drop other operations' required fields
      during the transition.
    """

    async def asyncSetUp(self):
        self.wf = workflows.WorkflowTests()
        self.wf.setUp()
        for callback, args, kwargs in self.wf._cleanups:
            self.addCleanup(callback, *args, **kwargs)
        self.wf._cleanups.clear()

        # ---- fake native cabinet list fixture (no heavy templates) ----
        self.batch_pages = {}
        self.batch_has_more = {}
        self.batch_total = None
        self.batch_calls = []
        self.apply_guard_hit = False

        app = self.wf.catalog.app

        @app.get("/api/cabinet-power/batches")
        async def list_batches(request: Request):
            qp = dict(request.query_params)
            self.batch_calls.append(qp)
            scope = qp.get("scope")
            page = int(qp.get("page") or 1)
            items = self.batch_pages.get((scope, page), [])
            data = {"items": items}
            if self.batch_has_more.get((scope, page)):
                data["has_more"] = True
            if self.batch_total is not None:
                data["total"] = self.batch_total
            return {"ok": True, "data": data}

        # text-apply must never be confirmed/written in this picker-only class.
        @app.post("/api/cabinet-power/batches/{batch_id}/text-apply")
        async def text_apply(batch_id: str, request: Request):
            self.apply_guard_hit = True
            raise AssertionError("batch picker tests must not write/confirm")

        # companion route used by the mixed-operation test.
        @app.post("/api/companion-record")
        async def companion_record(payload: _CompanionPayload):
            return {"ok": True, "data": {"saved": True, "title_seen": payload.title}}

        self.catalog = PortalAPICatalog(app)
        self.agent = PortalAgent(self.wf.assistant, self.catalog, self.wf.files)
        self.actor = {"id": "picker-owner", "scopes": ["A", "E"], "is_admin": False}
        self.sources = [{"id": "picker-source", "text": LARGE_LINE}]

    async def asyncTearDown(self):
        await workflows.gather_tasks(self.agent)

    # ------------------------------------------------------------------ helpers

    def _req(self, query=b""):
        return Request({**self.wf.request.scope, "query_string": query})

    def _decision(self, sources=None, extra_ops=None):
        ops = [{
            "api_id": "POST /api/cabinet-power/batches/{batch_id}/text-apply",
            "body": {"sources": sources if sources is not None else self.sources},
        }]
        if extra_ops:
            ops.extend(extra_ops)
        return {"operations": ops, "title": "批次回填", "explanation": "待选批次"}

    def _prepare_picker(self, decision=None, actor=None):
        actor = actor or self.actor
        plan = self.agent.prepare(
            actor,
            decision or self._decision(),
            "picker-turn-" + uuid.uuid4().hex,
            file_ids=[],
        )
        self.assertEqual(plan["status"], "needs_input")
        return plan

    def _picker_field(self, plan):
        return next(f for f in plan["fields"] if f.get("options_source") == "cabinet_batches")

    async def _options_for(self, plan, query=b""):
        field = self._picker_field(plan)
        return await self.agent.field_options(self.actor, plan["id"], field["name"], self._req(query))

    # ------------------------------------------------------------------ tests

    async def test_prepare_no_batch_creates_cabinet_batches_select_not_id_text(self):
        plan = self._prepare_picker()
        self.assertEqual(len(plan["fields"]), 1)
        field = plan["fields"][0]
        self.assertEqual(field["name"], "step0.batch_id")
        self.assertEqual(field["path"], "batch_id")
        self.assertEqual(field["type"], "select")
        self.assertEqual(field["options_source"], "cabinet_batches")
        self.assertIs(field.get("native_cabinet_batch"), True)
        self.assertIs(field.get("required"), True)
        self.assertEqual(field.get("options"), [])
        # It is the missing-target picker, not the concrete ID text form.
        self.assertNotIn("native_cabinet_text_fill", field)
        self.assertNotIn("sources", field.get("_initial_form", {}))
        # The operation is still unresolved: no batch_id bound to the path yet.
        self.assertNotIn("batch_id", plan["operations"][0].get("path_params") or {})

    async def test_field_options_respects_actor_scopes_and_status_todo(self):
        self.batch_pages[("A", 1)] = [
            {"batch_id": "A-1001", "title": "A楼批次", "rooms": ["202"], "created_at": "2026-10-01 08:00:00"},
        ]
        self.batch_pages[("E", 1)] = [
            {"batch_id": "E-2001", "title": "E楼批次", "rooms": ["501"], "created_at": "2026-10-01 09:00:00"},
        ]
        # Actor has no B scope; this batch must never be queried or returned.
        self.batch_pages[("B", 1)] = [
            {"batch_id": "B-3001", "title": "B楼批次", "rooms": [], "created_at": ""},
        ]

        pub = await self._options_for(self._prepare_picker())
        options = next(f for f in pub["fields"] if f["name"] == "step0.batch_id")["options"]
        values = [option["value"] for option in options]
        self.assertEqual(set(values), {"A-1001", "E-2001"})

        self.assertTrue(self.batch_calls)
        for call in self.batch_calls:
            self.assertIn(call["scope"], {"A", "E"})
            self.assertEqual(call["status"], "todo")
            self.assertEqual(call["page_size"], "100")
        self.assertNotIn("B", [call["scope"] for call in self.batch_calls])

    async def test_field_options_keyword_finds_second_page(self):
        self.batch_pages[("A", 1)] = [
            {"batch_id": "A-1", "title": "晨会批次", "rooms": ["101"], "created_at": "2026-10-01 08:00:00"},
            {"batch_id": "A-2", "title": "日常批次", "rooms": ["102"], "created_at": "2026-10-01 08:05:00"},
        ]
        self.batch_pages[("A", 2)] = [
            {"batch_id": "A-3", "title": "目标批次", "rooms": ["103"], "created_at": "2026-10-01 09:00:00"},
        ]
        self.batch_has_more[("A", 1)] = True
        self.batch_pages[("E", 1)] = []

        pub = await self._options_for(self._prepare_picker(), query=b"q=A-3")
        options = next(f for f in pub["fields"] if f["name"] == "step0.batch_id")["options"]
        self.assertEqual([option["value"] for option in options], ["A-3"])
        pages = [(call["scope"], call["page"]) for call in self.batch_calls]
        self.assertIn(("A", "1"), pages)
        self.assertIn(("A", "2"), pages)

    async def test_field_options_rejects_malformed_and_repeated_pages(self):
        plan = self._prepare_picker()
        field = self._picker_field(plan)

        # malformed: rows include a batch without a batch_id
        self.batch_pages[("A", 1)] = [{"title": "缺ID", "rooms": [], "created_at": ""}]
        self.batch_pages[("E", 1)] = []
        with self.assertRaises(AssistantError) as ctx:
            await self.agent.field_options(self.actor, plan["id"], field["name"], self._req())
        self.assertEqual(ctx.exception.status, 502)
        self.assertIn("分页", str(ctx.exception))
        self.assertEqual(self.batch_calls, [{"scope": "A", "status": "todo", "page": "1", "page_size": "100"}])

        # empty batch_id string is also malformed
        plan2 = self._prepare_picker()
        field2 = self._picker_field(plan2)
        self.batch_pages.clear()
        self.batch_calls.clear()
        self.batch_pages[("A", 1)] = [{"batch_id": "", "title": "空ID", "rooms": [], "created_at": ""}]
        self.batch_pages[("E", 1)] = []
        with self.assertRaises(AssistantError) as ctx2:
            await self.agent.field_options(self.actor, plan2["id"], field2["name"], self._req())
        self.assertEqual(ctx2.exception.status, 502)
        self.assertIn("分页", str(ctx2.exception))

        # repeated page signature is rejected, never reported as an empty result
        plan3 = self._prepare_picker()
        field3 = self._picker_field(plan3)
        self.batch_pages.clear()
        self.batch_calls.clear()
        self.batch_pages[("A", 1)] = [{"batch_id": "A-1", "title": "x", "rooms": [], "created_at": ""}]
        self.batch_pages[("A", 2)] = [{"batch_id": "A-1", "title": "x", "rooms": [], "created_at": ""}]
        self.batch_has_more[("A", 1)] = True
        self.batch_pages[("E", 1)] = []
        with self.assertRaises(AssistantError) as ctx3:
            await self.agent.field_options(self.actor, plan3["id"], field3["name"], self._req())
        self.assertEqual(ctx3.exception.status, 502)
        self.assertIn("分页", str(ctx3.exception))

    async def test_field_options_forwards_from_to_dates(self):
        decision = self._decision()
        decision["operations"][0]["params"] = {"from": "2026-09-01", "to": "2026-09-30"}
        self.batch_pages[("A", 1)] = [{"batch_id": "A-1", "title": "x", "rooms": [], "created_at": ""}]
        self.batch_pages[("E", 1)] = []

        plan = self._prepare_picker(decision)
        await self._options_for(plan)
        self.assertTrue(self.batch_calls)
        for call in self.batch_calls:
            self.assertEqual(call.get("from"), "2026-09-01")
            self.assertEqual(call.get("to"), "2026-09-30")

    async def test_field_options_out_of_scope_and_forged_choice_rejected(self):
        self.batch_pages[("A", 1)] = [{"batch_id": "A-1", "title": "x", "rooms": [], "created_at": ""}]
        self.batch_pages[("E", 1)] = []
        plan = self._prepare_picker()
        field = self._picker_field(plan)

        # out-of-scope explicit scope query is rejected before any native call.
        with self.assertRaises(AssistantError) as ctx:
            await self.agent.field_options(self.actor, plan["id"], field["name"], self._req(b"scope=B"))
        self.assertEqual(ctx.exception.status, 403)
        self.assertIn("无权", str(ctx.exception))
        self.assertEqual(self.batch_calls, [])

        # forged batch choice: not among offered options -> rejected on amend.
        self.batch_pages.clear()
        self.batch_calls.clear()
        self.batch_pages[("A", 1)] = [{"batch_id": "A-1001", "title": "A楼批次", "rooms": [], "created_at": ""}]
        self.batch_pages[("E", 1)] = []
        plan2 = self._prepare_picker()
        pub = await self._options_for(plan2)
        options = next(f for f in pub["fields"] if f["name"] == "step0.batch_id")["options"]
        self.assertTrue(options)
        with self.assertRaises(AssistantError) as ctx2:
            self.agent.amend(
                self.actor,
                plan2["id"],
                {"version": pub["version"], "values": {"step0.batch_id": "FORGED-NOT-OPTION"}},
            )
        self.assertIn("重新选择", str(ctx2.exception))
        self.assertIs(self.apply_guard_hit, False)

    async def test_amend_selected_batch_returns_needs_input_native_form_no_write(self):
        self.batch_pages[("A", 1)] = [{"batch_id": "A-1001", "title": "A楼批次", "rooms": ["202"], "created_at": "2026-10-01 08:00:00"}]
        self.batch_pages[("E", 1)] = []
        plan = self._prepare_picker()
        pub = await self._options_for(plan)
        picker = next(f for f in pub["fields"] if f["name"] == "step0.batch_id")
        chosen = picker["options"][0]["value"]
        self.assertEqual(chosen, "A-1001")

        amended = self.agent.amend(
            self.actor,
            plan["id"],
            {"version": pub["version"], "values": {"step0.batch_id": chosen}},
        )
        self.assertEqual(amended["status"], "needs_input")
        self.assertEqual(amended["risk"], "high")
        self.assertEqual(len(amended["fields"]), 1)
        tf = amended["fields"][0]
        self.assertEqual(tf["name"], "step0.text_fill")
        self.assertIs(tf.get("native_cabinet_text_fill"), True)
        self.assertEqual(tf.get("batch_id"), chosen)
        self.assertIs(tf.get("required"), True)
        # The public field carries the frozen $query reference (not resolved text).
        self.assertIn("$query", tf["value"])
        ref = tf["value"]["$query"]["ref"]

        # Raw stored field retains the initial sources in _initial_form.
        stored = self.wf.store.get_document(PLAN_NAMESPACE, plan["id"])
        raw_field = stored["fields"][0]
        self.assertEqual(raw_field["_initial_form"], {"sources": self.sources})
        self.assertEqual(stored["_queries"][ref], {"sources": self.sources})

        # Operation bound to the chosen batch; sources preserved from decision.
        op = stored["operations"][0]
        self.assertEqual(op["path_params"]["batch_id"], chosen)
        self.assertEqual(op["body"]["sources"], self.sources)

        # No write/confirm occurred.
        self.assertIs(self.apply_guard_hit, False)

    async def test_mixed_operation_transition_keeps_other_required_fields(self):
        companion = {"api_id": "POST /api/companion-record", "body": {}}
        plan = self._prepare_picker(self._decision(extra_ops=[companion]))
        names = [f["name"] for f in plan["fields"]]
        self.assertIn("step0.batch_id", names)
        self.assertIn("step1.title", names)
        title_field = next(f for f in plan["fields"] if f["name"] == "step1.title")
        self.assertIs(title_field.get("required"), True)

        self.batch_pages[("A", 1)] = [{"batch_id": "A-1001", "title": "A楼批次", "rooms": [], "created_at": ""}]
        self.batch_pages[("E", 1)] = []
        pub = await self._options_for(plan)
        chosen = next(f for f in pub["fields"] if f["name"] == "step0.batch_id")["options"][0]["value"]

        # Selecting only the batch while another operation still requires a
        # field must be rejected, and must not transition/drop those fields.
        with self.assertRaises(AssistantError) as ctx:
            self.agent.amend(
                self.actor,
                plan["id"],
                {"version": pub["version"], "values": {"step0.batch_id": chosen}},
            )
        self.assertIn("请填写", str(ctx.exception))

        stored = self.wf.store.get_document(PLAN_NAMESPACE, plan["id"])
        names_after = [f["name"] for f in stored["fields"]]
        self.assertIn("step0.batch_id", names_after)
        self.assertIn("step1.title", names_after)
        self.assertEqual(stored["status"], "needs_input")
        self.assertIs(self.apply_guard_hit, False)


if __name__ == "__main__":
    unittest.main()
