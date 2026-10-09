"""Isolated integration tests for the plan-convergence no-match confirmation.

Covers two runtimes against a shared in-memory guard/sender contract:

* Assistant runtime (PortalAgent._execute): when the native POST comes back
  with ``confirmation_required`` / ``plan_convergence_unmatched``, the plan must
  drop into a confirmation-waiting state instead of retrying, the issued token
  must be kept out of the public plan, and an explicit later confirmation must
  re-invoke the exact same API/body plus the token (same operation_id reused).

* Panel runtime (NoticePanel moving items through confirm_unmatched): no remote
  job is started until the unmatched item is explicitly confirmed, the generic
  ``retry`` action cannot consent for it, the token stays hidden from the public
  run, the confirmation remains owner-bound (the shared guard re-validates the
  fingerprint under the running session), and the same native operation_id is
  reused on the confirmed dispatch.

Everything runs against in-memory stores and an in-process ASGI catalog; there
is no real cloud/provider/network access and no production-state mutation.
"""
from __future__ import annotations

import asyncio
import copy
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from lan_bitable_template_portal import notice_panel as panel
from lan_bitable_template_portal.plan_convergence_send import NoticeConvergenceGuard
from lan_bitable_template_portal.portal_service import (
    MaintenancePortalService,
    PortalConfirmationRequiredError,
    PortalError,
)
from lan_bitable_template_portal.lighthouse_agent import PortalAgent
from lan_bitable_template_portal.lighthouse_ai import LighthouseAssistant
from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog
from lan_bitable_template_portal.lighthouse_files import LighthouseFiles

# Reuse the canonical panel fixtures (in-memory Store/FakeAuth/FakeController/
# PanelTestBase) from the existing panel test module.  No production file is
# edited; these fixtures are imported as-is.
from test_notice_panel import PanelTestBase


# --------------------------------------------------------------------------- #
# Assistant runtime
# --------------------------------------------------------------------------- #
class _Store:
    """Small in-memory document store matching test_lighthouse_agent_workflows."""

    def __init__(self, path):
        self.db_path, self.docs = path, {}

    def get_document(self, namespace, key):
        return copy.deepcopy(self.docs.get((namespace, key)))

    def put_document(self, namespace, key, value):
        self.docs[namespace, key] = copy.deepcopy(value)


async def _gather_tasks(agent):
    if agent.tasks:
        await asyncio.gather(*tuple(agent.tasks))


_AGENT_ACTOR = {"id": "convergence-agent-a", "scopes": ["A"], "is_admin": False}
_CONFIRMATION_TOKEN = "nonce-token"


class AgentConvergenceConfirmationTests(unittest.IsolatedAsyncioTestCase):
    """PortalAgent execute() confirms then re-executes with the token."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = _Store(Path(self.tmp.name) / "state.sqlite3")

        model = Mock()
        model.settings.return_value = {
            "configured": True,
            "enabled": True,
            "active_model_id": "default",
            "models": [{"id": "default", "name": "测试模型", "model": "fixture-model", "configured": True}],
        }
        model.profile.return_value = {"id": "default", "name": "测试模型", "model": "fixture-model"}
        search = Mock(side_effect=AssertionError("agent must use backend APIs, not local/DOM scraping"))
        self.assistant = LighthouseAssistant(self.store, search, model=model)
        self.files = LighthouseFiles(self.store)

        # Native POST bookkeeping: every payload the assistant sends is recorded,
        # but only requests carrying the confirmation token count as remote starts.
        self.requests = []
        self.remote_starts = []

        app = FastAPI()

        @app.post("/api/workbench-actions")
        async def workbench_actions(request: Request):
            payload = await request.json()
            self.requests.append(payload)
            if not payload.get("plan_convergence_confirmation"):
                # No valid convergence match yet -> native confirmation required.
                # The `error_code`/`details` are placed at the top level of the
                # body because the catalog's `_consume_json` sets result["_raw"]
                # to the decoded body; the agent branch reads them from there.
                return {
                    "ok": False,
                    "error_code": "confirmation_required",
                    "details": {
                        "kind": "plan_convergence_unmatched",
                        "confirmation": _CONFIRMATION_TOKEN,
                    },
                    "error": "未匹配到计划收敛，请再次确认是否发送。",
                }
            # Explicit confirmation presented -> native write succeeds.  Note: the
            # success body intentionally has no job_id so _task_spec stays None and
            # the plan completes synchronously (no background job polling).
            self.remote_starts.append(payload)
            return {"ok": True, "data": {"record_id": "rec-converged"}}

        self.catalog = PortalAPICatalog(app)
        self.agent = PortalAgent(self.assistant, self.catalog, self.files)
        self.request = Request({
            "type": "http",
            "scheme": "http",
            "server": ("testserver", 80),
            "client": ("127.0.0.1", 4567),
            "path": "/api/assistant/agent",
            "root_path": "",
            "query_string": b"",
            "headers": [(b"cookie", b"fixture=convergence"), (b"origin", b"http://testserver")],
        })
        self.operation_id = "convergence_agent_00000001"

    def _decision(self):
        return {
            "operations": [{
                "api_id": "POST /api/workbench-actions",
                "body": {
                    "command_format": "notice_command",
                    "operation_id": "native-op-convergence-1",
                    "scope": "A",
                    "work_type": "change",
                    "action": "start",
                    "source_record_id": "src-converge",
                    "patch": {"title": "A楼变更", "content": "变更内容", "progress": "进行中"},
                },
            }],
        }

    async def test_convergence_no_match_waits_then_explicit_confirm_reuses_body_and_token(self):
        # prepare() requires the operator to fill the notice patch form first
        # (high-risk notice command -> needs_input), then it becomes confirmable.
        plan = self.agent.prepare(_AGENT_ACTOR, self._decision(), self.operation_id, [])
        self.assertEqual(plan["status"], "needs_input")
        self.assertEqual(plan["risk"], "high")

        patch_value = copy.deepcopy(plan["fields"][0]["value"])
        patch_value.pop("$query", None)
        # Notice patch form marks many children required (start_time/end_time as
        # valid datetime-local, specialty/execution_party are option selects).
        # Provide the resolved values for the required children.
        if not patch_value.get("start_time"):
            patch_value["start_time"] = "2026-10-09T10:00:00"
        if not patch_value.get("end_time"):
            patch_value["end_time"] = "2026-10-09T18:00:00"
        if not patch_value.get("specialty"):
            patch_value["specialty"] = "电气"
        if not patch_value.get("execution_party"):
            patch_value["execution_party"] = "厂维"
        for key in ("level", "location", "reason", "impact"):
            if patch_value.get(key) in (None, ""):
                patch_value[key] = "与本次计划一致"
        plan = self.agent.amend(
            _AGENT_ACTOR,
            plan["id"],
            {"version": plan["version"], "values": {"step0.patch": patch_value}},
        )
        # High-risk endpoint requires both review + execute approvals before bytes go out.
        self.assertEqual(plan["status"], "awaiting_confirmation")

        plan = await self.agent.confirm(
            _AGENT_ACTOR, plan["id"], {"version": plan["version"], "stage": "review"}, self.request
        )
        self.assertEqual(plan["status"], "awaiting_second_confirmation")
        self.assertEqual(self.remote_starts, [])

        # Execute now runs the native POST which returns confirmation_required.
        await self.agent.confirm(
            _AGENT_ACTOR, plan["id"], {"version": plan["version"], "stage": "execute"}, self.request
        )
        await _gather_tasks(self.agent)

        waiting = self.agent.get_plan(_AGENT_ACTOR, plan["id"])
        # It is a confidence/confirmation-waiting state, NOT a retry loop (no
        # repeated write, no failed error, not running).
        self.assertEqual(waiting["status"], "awaiting_second_confirmation")
        self.assertEqual(waiting["error"], "")
        self.assertEqual(len(self.requests), 1)
        self.assertEqual(self.remote_starts, [])

        first = self.requests[0]
        self.assertNotIn("plan_convergence_confirmation", first)

        # The operator token is stored on the plan's operation, but the public
        # plan projection must not leak it.
        internals = self.agent.get_plan(_AGENT_ACTOR, plan["id"])
        op_body = internals["operations"][0]["body"]
        self.assertEqual(op_body.get("plan_convergence_confirmation"), _CONFIRMATION_TOKEN)
        public = self.agent.public_plan(internals)
        self.assertNotIn(_CONFIRMATION_TOKEN, json.dumps(public, ensure_ascii=False))
        # public projection also does not contain the confirmation_required marker
        self.assertNotIn("confirmation_required", json.dumps(public, ensure_ascii=False))

        # Explicit second confirmation re-runs the SAME native call: same body +
        # the confirmation token only.
        await self.agent.confirm(
            _AGENT_ACTOR, waiting["id"], {"version": waiting["version"], "stage": "execute"}, self.request
        )
        await _gather_tasks(self.agent)

        finished = self.agent.get_plan(_AGENT_ACTOR, plan["id"])
        self.assertEqual(finished["status"], "completed")
        self.assertEqual(len(self.requests), 2)
        self.assertEqual(len(self.remote_starts), 1)

        second = self.remote_starts[0]
        body2 = dict(second)
        body2.pop("plan_convergence_confirmation")
        self.assertEqual(body2, first)
        self.assertEqual(second["plan_convergence_confirmation"], _CONFIRMATION_TOKEN)
        self.assertEqual(second["operation_id"], first["operation_id"])


# --------------------------------------------------------------------------- #
# Panel runtime
# --------------------------------------------------------------------------- #
class _NoMatchConvergence:
    """Stand-in for the portal convergence checker that never finds a match."""

    def __init__(self):
        self.calls = 0

    def _check_records(self, records, kind, seconds=8, refresh=False):
        self.calls += 1
        n = len(records) if isinstance(records, list) else 1
        return {"records": [{"hits": []} for _ in range(n)]}


class PanelConvergenceConfirmationTests(PanelTestBase):
    """NoticePanel confirm_unmatched flow against the real shared guard."""

    def setUp(self):
        super().setUp()

        # The panel fixture's service is a lightweight stand-in; wire the piece of
        # the real portal service the shared guard needs to resolve building codes.
        self.service._building_codes_from_value = lambda value: MaintenancePortalService._building_codes_from_value(value)
        self.no_match = _NoMatchConvergence()
        self.guard = NoticeConvergenceGuard(
            self.service, self.store, lambda: self.no_match
        )
        # get_job must return a completed job after a successful native write so a
        # confirmed dispatch moves the run back to 'done' cleanly.
        self.service.get_job = lambda job_id: {"phase": "success"}

        self.submit_tries = []
        self.remote_writes = []
        self._wire_guarded_submit()

    def _wire_guarded_submit(self):
        # Mirror the real submit_notice_action: stamp auth from the session, then
        # let the shared send-time guard decide.  A remote job only "starts"
        # (recorded below) if the guard approves.
        async def guarded_submit(controller, runtime, session, payload, *, card_submission=False):
            enriched = dict(payload)
            user = (session or {}).get("user") or {}
            enriched["_auth_open_id"] = str(user.get("open_id") or "")
            enriched["_auth_user_name"] = str(user.get("name") or "")
            self.submit_tries.append(copy.deepcopy(enriched))
            self.guard.check(enriched)  # raises PortalConfirmationRequiredError on no-match
            write = {key: value for key, value in enriched.items() if not key.startswith("_")}
            self.remote_writes.append(copy.deepcopy(write))
            return JSONResponse({"ok": True, "data": {"job_id": "job-conv-" + str(len(self.remote_writes)), "initial_phase": "accepted"}})

        self.native.side_effect = guarded_submit

    def _open_change(self):
        own = self._item("a", "A", work_type="change", action="start")
        own["draft"].update({"title": "A楼变更", "content": "内容", "progress": "进行中", "source_record_id": "src-a"})
        self.build_impl = lambda *args, **kwargs: [own]
        run = self.open(scope="A")
        self.select_fill_save(run["id"], "a", content="内容", progress="进行中")
        return run

    def test_no_remote_job_until_confirm_unmatched_retry_cannot_consent(self):
        run = self._open_change()
        previewed = self.preview(run["id"])
        self.assertEqual(previewed["state"], "confirm")

        confirmed = self.action(run["id"], "confirm")
        item_after_fail = self.item(confirmed, "a")
        self.assertEqual(item_after_fail["phase"], "failed")
        self.assertEqual(self.remote_writes, [])

        # The run is done-with-failure; the public run flags the requirement but
        # must not expose the raw token.
        public = self.manager.public_run(run["id"])
        self.assertTrue(public["items"][0]["convergence_confirmation_required"])
        doc = self.store.get_document(panel.NS, run["id"])
        token = doc["items"][0]["convergence_confirmation"]
        self.assertTrue(token)
        self.assertNotIn(token, json.dumps(public, ensure_ascii=False))

        # Generic retry must NOT consent for an unmatched item: _retry skips the
        # convergence item and, with nothing else left to re-run, refuses outright.
        with self.assertRaises(PortalError) as retry_ctx:
            self.action(run["id"], "retry")
        self.assertIn("没有可重试的失败操作", str(retry_ctx.exception))
        still = self.store.get_document(panel.NS, run["id"])
        self.assertEqual(still["items"][0]["phase"], "failed")
        self.assertTrue(still["items"][0]["convergence_confirmation"])
        self.assertEqual(self.remote_writes, [])

        # Still no remote job has started anywhere in the flow.
        self.assertEqual(self.remote_writes, [])

        # Explicit confirm_unmatched transfers the token and re-dispatches with the
        # same native operation_id; only now may a remote job start.
        op_id_before = self.store.get_document(panel.NS, run["id"])["items"][0]["operation_id"]
        matched = self.action(run["id"], "confirm_unmatched")
        self.assertEqual(self.item(matched, "a")["phase"], "success")
        self.assertEqual(len(self.remote_writes), 1)
        self.assertEqual(self.remote_writes[0]["operation_id"], op_id_before)
        self.assertEqual(self.remote_writes[0]["plan_convergence_confirmation"], token)

    def test_confirmation_remains_owner_bound(self):
        run = self._open_change()
        self.preview(run["id"])
        self.action(run["id"], "confirm")

        doc = self.store.get_document(panel.NS, run["id"])
        token = doc["items"][0]["convergence_confirmation"]
        # First guarded attempt recorded the exact payload the guard rejected.
        first_try = copy.deepcopy(self.submit_tries[0])

        # Same owner + token -> approved (no exception).
        same_owner = {**first_try, "plan_convergence_confirmation": token}
        self.assertIsNone(self.guard.check(copy.deepcopy(same_owner)))

        # A different operator presenting the same stolen token is rejected again
        # with a fresh confirmation_required -> confirmation is owner-bound.
        other_owner = {**first_try, "_auth_open_id": "ou-other", "plan_convergence_confirmation": token}
        with self.assertRaises(PortalConfirmationRequiredError) as ctx:
            self.guard.check(copy.deepcopy(other_owner))
        details = ctx.exception.details
        self.assertEqual(details.get("kind"), "plan_convergence_unmatched")
        self.assertNotEqual(details.get("confirmation"), token)

        # The original owner can still use their token afterwards.
        self.assertIsNone(self.guard.check(copy.deepcopy(same_owner)))


if __name__ == "__main__":
    unittest.main(verbosity=2)