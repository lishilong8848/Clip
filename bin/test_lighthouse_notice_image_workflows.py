"""Safe optional file_token selection for change-confirmation screenshot deletion.

This test drives the LighthousePortal flow for deleting change-confirmation
screenshot images through the real query -> prepare -> amend -> confirm path:

  query (native task with screenshot_items + local_screenshot_items)
    -> opaque file refs derived from the actual queried task
    -> prepare selectable image name (file_token as a select field)
    -> amend selecting image B's opaque ref
    -> confirm (two confirmations, high-risk DELETE)
    -> exact selected file_token reaches DELETE /api/change-confirmations/.../screenshot

There are no raw-token text inputs: file_token is exposed only as a `select`
field whose options are field-bound ``value_*`` references derived from the
queried ``business_refs`` (``{"field": "file_token", "value": "<token>"}``).
Forged refs/raw tokens are rejected.

Local cache removal is NOT linked to the cloud selection in production, so it
is exercised separately as an explicitly-fixed local-only action against the
existing ``/api/notice-images/{id}`` API; it never invokes the remote
screenshot delete and the linked workflow does not claim it.

The ``PortalAgent.prepare`` guard for ``file_token`` (declared in the route's
query params) requires an optional ``select`` field whose options are
field-bound references; a plain ``text`` raw-token field is rejected with
"请选择原记录中已读取的图片，不能输入附件标识。" and any unrelated field path
containing a credential keyword is rejected with "助手不能收集业务凭证字段。".

The completed plan keeps the human-reviewable filename label in its
``selected_labels`` review record while the public projection never exposes
the opaque file token.

This file intentionally does NOT import reference-workflow ``*Tests`` classes
so pytest/unittest do not re-collect them under this new module name.
"""
import asyncio
import copy
import json
import sys
import tempfile
import unittest
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fastapi import FastAPI, Query, Request
from pydantic_ai.models.function import DeltaToolCall, FunctionModel
from lan_bitable_template_portal.lighthouse_ai import AssistantError, LighthouseAssistant
from lan_bitable_template_portal.lighthouse_agent import PortalAgent, _result_refs
from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog
from lan_bitable_template_portal.lighthouse_files import LighthouseFiles
from lan_bitable_template_portal.lighthouse_model import LighthouseModel
from test_lighthouse_stream import Store

ACTOR = {"id": "notice-image-owner", "scopes": ["A"], "is_admin": False}


class NoticeImageWorkflowTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = Store(Path(self.tmp.name) / "state.sqlite3")
        model = Mock()
        model.settings.return_value = {
            "configured": True, "enabled": True, "active_model_id": "default",
            "models": [{"id": "default", "name": "fixture", "model": "fixture-model", "configured": True}],
        }
        self.assistant = LighthouseAssistant(self.store, lambda *_: ([], []), model=model)
        self.files = LighthouseFiles(self.store)
        self.remote_calls = []      # DELETE /api/change-confirmations/{id}/screenshot
        self.local_deletes = []     # DELETE /api/notice-images/{id}
        self.returned = []
        self.request = Request({"type": "http", "scheme": "http", "server": ("testserver", 80),
            "client": ("127.0.0.1", 123), "path": "/api/assistant/messages", "root_path": "",
            "query_string": b"", "headers": [(b"origin", b"http://testserver"),
                                             (b"cookie", b"fixture=owner")]})
        self.app = FastAPI()
        self._add_fake_routes()

    @staticmethod
    def _query_items():
        # Native change-confirmation task shape: one task object carrying both
        # the remote screenshot attachments and the locally mirrored cache.
        return {"items": [{
            "target_record_id": "rec-change",
            "record_id": "rec-change",
            "work_type": "change",
            "title": "A楼变更",
            "scope": "A",
            "screenshot_items": [
                {"name": "site-before.png", "file_token": "private-token-a", "scope": "A"},
                {"name": "ali-before.png", "file_token": "private-token-b", "scope": "A"},
            ],
            "local_screenshot_items": [
                {"local_image_id": "local-img-a", "file_name": "site-before.png", "scope": "A"},
                {"local_image_id": "local-img-b", "file_name": "ali-before.png", "scope": "A"},
            ],
        }]}

    def _add_fake_routes(self):
        @self.app.get("/api/change-confirmations")
        async def change_confirmations():
            return {"ok": True, "data": self._query_items()}

        # file_token / local_image_id are OPTIONAL query params so the assistant
        # may omit one of them; when only local_image_id is used the native
        # backend resolves the bound feishu token locally, never at the agent.
        @self.app.delete("/api/change-confirmations/{record_id}/screenshot")
        async def delete_screenshot(record_id: str, file_token: str = Query(""),
                                    local_image_id: str = Query("")):
            self.remote_calls.append({
                "record_id": record_id,
                "file_token": file_token,
                "local_image_id": local_image_id,
            })
            return {"ok": True, "data": {
                "target_record_id": record_id, "state": "missing_screenshot", "deleted": True}}

        @self.app.delete("/api/notice-images/{image_id}")
        async def delete_local_image(image_id: str):
            # Local-only cache removal. It must NOT call the remote screenshot
            # delete endpoint above.
            self.local_deletes.append(image_id)
            return {"ok": True, "data": {"deleted": True, "image_id": image_id}}

    async def answer(self, stream, question, *, history=None, turn=None):
        @asynccontextmanager
        async def factory(*_):
            yield FunctionModel(stream_function=stream)
        self.portal = PortalAgent(self.assistant, PortalAPICatalog(self.app), self.files)
        engine = LighthouseModel(self.portal, model_factory=factory)

        async def emit(*_):
            pass

        async def authorize():
            return copy.deepcopy(ACTOR)

        turn = turn or {"question": question, "operation_id": "notice_image_workflow_00001",
            "file_ids": [], "_profile": {"id": "default", "name": "fixture",
                                         "model": "fixture-model"}}
        self.turn = turn
        return await engine.answer(ACTOR, turn, history or [], self.request, emit, authorize, {})

    @staticmethod
    def tools(messages):
        return [part.content for message in messages for part in message.parts
                if getattr(part, "part_kind", "") == "tool-return"]

    async def execute_plan(self, result):
        plan = result["plan"]
        if plan["status"] == "needs_input":
            values = {}
            for field in plan["fields"]:
                if field["path"] == "file_token":
                    # Select the second screenshot (ali-before.png) by its
                    # public filename label; values stay opaque refs.
                    options = field.get("options") or []
                    target = next(
                        (option["value"] for option in options
                         if option.get("label") == "ali-before.png"),
                        options[-1]["value"] if options else None,
                    )
                    values[field["name"]] = target
                else:
                    values[field["name"]] = copy.deepcopy(field.get("value"))
            plan = self.portal.amend(ACTOR, plan["id"], {"version": plan["version"], "values": values})
        plan = await self.portal.confirm(ACTOR, plan["id"], {"version": plan["version"], "stage": "review"}, self.request)
        if plan["status"] == "awaiting_second_confirmation":
            plan = await self.portal.confirm(ACTOR, plan["id"], {"version": plan["version"], "stage": "execute"}, self.request)
        if self.portal.tasks:
            await asyncio.gather(*tuple(self.portal.tasks))
        return self.portal.get_plan(ACTOR, plan["id"])

    # -- projection / security guarantees (independent of the Codex fix) -------

    async def test_query_projects_opaque_file_refs_and_hides_tokens(self):
        async def stream(messages, info):
            results = self.tools(messages)
            if not results:
                yield {0: DeltaToolCall(name="query", json_args=json.dumps(
                    {"operation": {"api_id": "GET /api/change-confirmations"}}))}
            else:
                self.returned.append(copy.deepcopy(results[0]))
                yield "已列出变更确认截图，可选择删除。"
        result = await self.answer(stream, "读取待删除的变更确认截图")
        public = json.dumps(self.returned + result["sources"], ensure_ascii=False)
        self.assertNotIn("private-token-a", public)
        self.assertNotIn("private-token-b", public)
        task = result["sources"][0]["data"]["items"][0]
        self.assertEqual(task["target_record_id"], "rec-change")
        screenshots = task["screenshot_items"]
        image_a = next(image for image in screenshots if image["name"] == "site-before.png")
        self.assertIn("business_refs", image_a)
        refs = result["_references"]
        binding = next(ref for ref, bind in refs.items()
                       if isinstance(bind, dict) and bind["field"] == "file_token"
                       and bind["value"] == "private-token-a")
        self.assertIn(binding, [br["ref"] for br in image_a["business_refs"]])
        # The public task preserves the selectable image name (and opaque
        # non-token material that survives the safe projection) while the raw
        # file_token stays only a field-bound reference.
        self.assertEqual(image_a["name"], "site-before.png")
        self.assertNotIn("file_token", image_a)
        self.assertNotIn("private-token-a", json.dumps(task, ensure_ascii=False))

    def test_result_refs_reject_forged_or_misplaced_token_references(self):
        refs = {"value_ok": {"field": "file_token", "value": "private-token-a"}}
        # Unknown ref -> rejected.
        with self.assertRaises(AssistantError):
            _result_refs({"$reference": "forged_ref"}, [], refs, target_field="file_token")
        # Bound field used on a different target field -> rejected (no raw-token
        # leakage into unrelated params/body values).
        with self.assertRaises(AssistantError):
            _result_refs({"$reference": "value_ok"}, [], refs, target_field="local_image_id")
        # Exact field-bound target resolves.
        self.assertEqual(
            _result_refs({"$reference": "value_ok"}, [], refs, target_field="file_token"),
            "private-token-a",
        )

    async def test_raw_token_text_field_is_rejected_in_prepare(self):
        self.portal = PortalAgent(self.assistant, PortalAPICatalog(self.app), self.files)
        decision = {
            "operations": [{"api_id": "DELETE /api/change-confirmations/{record_id}/screenshot",
                            "path_params": {"record_id": "rec-change"}, "params": {}}],
            "fields": [{"path": "file_token", "section": "params", "type": "text",
                        "label": "待删除截图", "value": "private-token-a"}],
        }
        with self.assertRaises(AssistantError) as ctx:
            self.portal.prepare(ACTOR, decision, "raw_token_field_00001", [])
        self.assertIn("不能输入附件标识", str(ctx.exception))
        self.assertEqual(self.remote_calls, [])

    # -- local-only delete uses the existing local API, never the remote -------

    async def test_local_only_delete_does_not_invoke_remote(self):
        self.portal = PortalAgent(self.assistant, PortalAPICatalog(self.app), self.files)
        decision = {
            "operations": [{"api_id": "DELETE /api/notice-images/{image_id}",
                            "path_params": {"image_id": "local-img-a"}}],
        }
        plan = self.portal.prepare(ACTOR, decision, "local_only_delete_00001", [])
        self.assertEqual(self.local_deletes, [])
        self.assertEqual(self.remote_calls, [])
        completed = await self.execute_plan({"plan": self.portal.public_plan(plan)})
        self.assertEqual(completed["status"], "completed", completed.get("error"))
        self.assertEqual(self.local_deletes, ["local-img-a"])
        # A local-only image removal must never hit the remote screenshot API.
        self.assertEqual(self.remote_calls, [])

    # -- full workflow: query native task, select one screenshot only --------

    async def test_selected_file_token_reaches_remote_delete_with_only_selected_image(self):
        async def stream(messages, info):
            results = self.tools(messages)
            if not results:
                yield {0: DeltaToolCall(name="query", json_args=json.dumps(
                    {"operation": {"api_id": "GET /api/change-confirmations"}}))}
            elif len(results) == 1:
                self.returned.append(copy.deepcopy(results[0]))
                task = results[0]["data"]["items"][0]
                self.assertEqual(task["target_record_id"], "rec-change")

                def ref_for(filename):
                    image = next(row for row in task["screenshot_items"]
                                 if row["name"] == filename)
                    return next(br["ref"] for br in image["business_refs"]
                                if br["field"] == "file_token")

                a_ref, b_ref = ref_for("site-before.png"), ref_for("ali-before.png")
                decision = {
                    "title": "删除变更确认截图",
                    "operations": [{
                        "api_id": "DELETE /api/change-confirmations/{record_id}/screenshot",
                        "path_params": {"record_id": task["target_record_id"]},
                    }],
                    "fields": [{
                        "path": "file_token", "section": "params", "type": "select",
                        "label": "待删除截图", "required": False,
                        "options": [
                            {"value": a_ref, "label": "site-before.png"},
                            {"value": b_ref, "label": "ali-before.png"},
                        ],
                    }],
                }
                yield {0: DeltaToolCall(name="prepare_business",
                                        json_args=json.dumps(decision))}
            else:
                yield "已准备删除变更确认截图，请核对。"
        result = await self.answer(stream, "删除A楼变更确认的阿里截图，选ali-before这张")
        plan = result["plan"]
        self.assertEqual(plan["status"], "needs_input", plan.get("error"))
        token_field = next(field for field in plan["fields"] if field["path"] == "file_token")
        # file_token must render as an optional SELECT, never a raw-token input.
        self.assertEqual(token_field["type"], "select")
        # Options are opaque references, never raw tokens.
        public = json.dumps(self.returned + result["sources"], ensure_ascii=False)
        self.assertNotIn("private-token-a", public)
        self.assertNotIn("private-token-b", public)
        self.assertNotIn("private-token-a", json.dumps(self.portal.public_plan(plan), ensure_ascii=False))
        completed = await self.execute_plan(result)
        self.assertEqual(completed["status"], "completed", completed.get("error"))
        # The selected image B is the only remote screenshot deleted; image A
        # is untouched.
        self.assertEqual(len(self.remote_calls), 1)
        self.assertEqual(self.remote_calls[0]["record_id"], "rec-change")
        self.assertEqual(self.remote_calls[0]["file_token"], "private-token-b")
        self.assertNotIn("private-token-a", [call["file_token"] for call in self.remote_calls])
        plan_labels = completed.get("selected_labels", {}).get("0", {})
        self.assertEqual(plan_labels.get("file_token"), "ali-before.png")
        public_completed = self.portal.public_plan(completed)
        self.assertEqual(public_completed["operations"][0]["selected_labels"]["selected_document"], "ali-before.png")
        self.assertNotIn("private-token-a", json.dumps(public_completed, ensure_ascii=False))
        self.assertNotIn("private-token-b", json.dumps(public_completed, ensure_ascii=False))
        # Local cache removal is a separate explicitly-fixed action; this
        # linked workflow must not claim it.
        self.assertEqual(self.local_deletes, [])


if __name__ == "__main__":
    unittest.main()
