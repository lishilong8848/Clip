"""Assistant proof forms call the existing local batch service, never the cloud."""
import copy
import unittest
from unittest.mock import patch

from fastapi import HTTPException, Request

from . import test_lighthouse_cabinet_edit_workflows as edits
from .lan_bitable_template_portal.cabinet_power_excel import CabinetError
from .lan_bitable_template_portal.lighthouse_agent import PortalAgent, _CABINET_PROOF_APPLY, _CABINET_PROOF_CORRECT
from .lan_bitable_template_portal.lighthouse_api import PortalAPICatalog
from .lan_bitable_template_portal.lighthouse_ai import AssistantError


class CabinetProofWorkflowTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        edits.cb.CabinetPowerTests.setUpClass()

    async def asyncSetUp(self):
        self.base = edits.CabinetEditWorkflowTests()
        await self.base.asyncSetUp()
        for callback, args, kwargs in self.base._cleanups:
            self.addCleanup(callback, *args, **kwargs)
        self.base._cleanups.clear()
        self.service, self.actor, self.writes = self.base.service.batches, self.base.actor, []
        self.directory_calls, self.directory_override = [], None
        app = self.base.catalog.app

        @app.get("/api/cabinet-power/racks")
        async def directory(scope: str):
            self.directory_calls.append(scope)
            if scope not in self.actor["scopes"]:
                raise HTTPException(403, "无楼栋权限")
            return {"ok": True, "data": self.directory_override or self.base.service.racks(scope)}

        @app.post("/api/cabinet-power/batches/{batch_id}/images/{image_id}/correct")
        async def correct(batch_id: str, image_id: str, request: Request):
            payload = await request.json()
            self.writes.append(copy.deepcopy(payload))
            try:
                data = self.service.correct_image(batch_id, image_id, payload, self.actor["id"], self.actor["scopes"])
                return {"ok": True, "data": self.service.visible(data, self.actor["id"], self.actor["scopes"])}
            except CabinetError as exc:
                raise HTTPException(exc.status_code, str(exc)) from None

        @app.post("/api/cabinet-power/batches/{batch_id}/images/{image_id}/apply")
        async def apply(batch_id: str, image_id: str, request: Request):
            payload = await request.json()
            self.writes.append(copy.deepcopy(payload))
            try:
                data = self.service.apply_image(batch_id, image_id, payload, self.actor["id"], self.actor["scopes"])
                return {"ok": True, "data": self.service.visible(data, self.actor["id"], self.actor["scopes"])}
            except CabinetError as exc:
                raise HTTPException(exc.status_code, str(exc)) from None

        @app.delete("/api/cabinet-power/batches/{batch_id}/images/{image_id}")
        async def remove(batch_id: str, image_id: str, version: int):
            self.writes.append({"action": "delete", "version": version})
            try:
                data = self.service.delete_image(batch_id, image_id, version, self.actor["id"], self.actor["scopes"])
                return {"ok": True, "data": data}
            except CabinetError as exc:
                raise HTTPException(exc.status_code, str(exc)) from None

        @app.post("/api/cabinet-power/batches/{batch_id}/images/{image_id}/restore")
        @app.post("/api/cabinet-power/batches/{batch_id}/images/{image_id}/retry")
        async def restore_or_retry(batch_id: str, image_id: str, request: Request):
            payload = await request.json()
            action = request.url.path.rsplit("/", 1)[1]
            self.writes.append({"action": action, **payload})
            try:
                with patch.object(self.service, "_queue_image", side_effect=lambda bid, image: self.service._change(
                        bid, lambda data: next(item for item in data["images"] if item["image_id"] == image["image_id"]).update(status="done", phase="finished"))):
                    if action == "restore":
                        data = self.service.restore_image(batch_id, image_id, payload["version"], self.actor["id"], self.actor["scopes"])
                    else:
                        data = self.service.retry_image(batch_id, image_id, payload, self.actor["id"], self.actor["scopes"])
                return {"ok": True, "data": data}
            except CabinetError as exc:
                raise HTTPException(exc.status_code, str(exc)) from None

        @app.get("/api/cabinet-power/batches/{batch_id}/status")
        async def batch_status(batch_id: str):
            return {"ok": True, "data": self.service.status(batch_id, self.actor["id"], self.actor["scopes"])}

        self.agent = PortalAgent(self.base.wf.assistant, PortalAPICatalog(app), self.base.wf.files)
        self.base.agent = self.agent
        batch = self.base._create_batch([edits._READY_ROW, {**edits._READY_ROW, "rack": "E02"}])
        self.batch_id = batch["batch_id"]
        self.row_ids = [row["row_id"] for row in batch["rows"]]
        self.image_id = "a" * 64

        def seed(data):
            data["images"] = [{"image_id": self.image_id, "name": "确认图.png", "status": "done", "suggestions": [
                {"scope": "E", "room": "101", "rack": "E01", "row_id": self.row_ids[0], "status": "needs_review",
                 "expected": "2026-09-17 12:00:00", "actual": "2026-09-17 10:00:00", "action": "上测试电", "result": "失败"}]}]
        self.service._change(self.batch_id, seed)

    async def asyncTearDown(self):
        await edits.workflows.gather_tasks(self.agent)

    def prepare(self, *, body=None, snapshot=None, image_id=None):
        self.snapshot = snapshot or self.base._snapshot(self.batch_id)
        self.plan = self.agent.prepare(self.actor, {"operations": [{"api_id": _CABINET_PROOF_APPLY,
            "path_params": {"batch_id": self.batch_id, **({"image_id": image_id} if image_id else {})}, "body": body or {}}]},
            "proof-turn", [], queries={"batch": self.snapshot})
        self.field = next(field for field in self.plan["fields"] if field.get("native_cabinet_proof"))
        return copy.deepcopy(self.field["_initial_form"])

    def amend(self, value):
        return self.agent.amend(self.actor, self.plan["id"], {"version": self.plan["version"], "values": {self.field["name"]: value}})

    def prepare_correct(self):
        current = self.service.get(self.batch_id)
        existing = {(row["room"], row["rack"]) for row in current["rows"]}
        self.new_rack = next(row for row in self.base.service.racks("E")["items"] if (row["room"], row["rack"]) not in existing)
        def seed(batch):
            batch["source"] = "image"
            batch["images"][0]["suggestions"] = [{"scope": "E", "room": self.new_rack["room"], "rack": self.new_rack["rack"],
                "action": "上正式电", "expected": "2026-09-17 12:00:00", "actual": "2026-09-17 10:00:00", "result": "成功", "status": "unmatched"}]
        self.service._change(self.batch_id, seed)
        snapshot = self.base._snapshot(self.batch_id)
        self.plan = self.agent.prepare(self.actor, {"operations": [{"api_id": _CABINET_PROOF_CORRECT,
            "path_params": {"batch_id": self.batch_id}, "body": {"candidate_index": 0}}]}, "correct-proof", [], queries={"batch": snapshot})
        self.field = self.plan["fields"][0]
        return snapshot

    async def load_directory(self, scope="E"):
        request = Request({**self.base.wf.request.scope, "query_string": ("scope=" + scope).encode()})
        public = await self.agent.field_options(self.actor, self.plan["id"], self.field["name"], request)
        self.plan = self.agent.get_plan(self.actor, self.plan["id"])
        self.field = self.plan["fields"][0]
        return public

    async def test_correct_uses_native_directory_preserves_existing_rows_and_attaches_proof(self):
        before = self.prepare_correct()
        self.assertTrue(self.field["native_cabinet_correct"])
        self.assertEqual(self.field["rows"], [])
        self.assertEqual(self.directory_calls, [])
        public = await self.load_directory()
        field = public["fields"][0]
        self.assertEqual(field["directory_scope"], "E")
        self.assertTrue(field["images"])
        self.assertEqual(field["value"]["image_id"], self.image_id)
        existing = {(row["scope"], row["room"], row["rack"]) for row in before["rows"]}
        self.assertTrue(all((row["scope"], row["room"], row["rack"]) not in existing for row in field["rows"]))
        choice = next(row for row in field["rows"] if row["room"] == self.new_rack["room"] and row["rack"] == self.new_rack["rack"])
        value = {"image_id": self.image_id, "candidate_index": 0, "row_id": choice["row_id"], "scope": "E",
                 "fields": {**self.field["_initial_form"]["fields"], "type_detail": "本批补录"}}
        amended = self.amend(value)
        self.assertEqual(self.writes, [])
        done = await self.base._run_to_completion(amended)
        self.assertEqual(done["status"], "completed", done.get("error"))
        batch = self.service.get(self.batch_id)
        self.assertEqual(len(batch["rows"]), len(before["rows"]) + 1)
        row = batch["rows"][-1]
        self.assertEqual(row["evidence_images"], [self.image_id])
        self.assertEqual(row["actual"], "2026-09-17 10:00:00")
        self.assertEqual(row["rack_type"], self.new_rack["rack_type"])
        self.assertEqual(row["type_detail"], "本批补录")
        self.assertEqual(self.base.cabfix.remote.creates, 0)
        await self.agent.confirm(self.actor, done["id"], {"version": done["version"], "stage": "execute"}, self.base.request)
        self.assertEqual(len(self.writes), 1)

    async def test_correct_guards_scope_identity_business_fields_and_version(self):
        self.prepare_correct()
        with self.assertRaises(AssistantError):
            await self.load_directory("A")
        self.assertEqual(self.directory_calls, [])
        await self.load_directory()
        choice = self.field["rows"][0]
        value = {"image_id": self.image_id, "candidate_index": 0, "row_id": choice["row_id"], "scope": "E", "fields": {"action": "上正式电"}}
        for changes in ({"scope": "A"}, {"row_id": "E/unknown/fake"}, {"image_id": "foreign"}, {"candidate_index": 20},
                        {"attach": False}, {"fields": {"rack": "forged"}}, {"fields": {"result": "失败", "failure_reason": ""}}):
            with self.subTest(changes=changes), self.assertRaises(AssistantError):
                self.amend({**copy.deepcopy(value), **changes})
        amended = self.amend(value)
        self.service._change(self.batch_id, lambda batch: batch.update(title="changed elsewhere"))
        done = await self.base._run_to_completion(amended)
        self.assertEqual(done["status"], "failed")
        self.assertEqual(len(self.service.get(self.batch_id)["rows"]), 2)
        self.assertEqual(len(self.writes), 1)

    async def test_correct_rejects_non_image_batch_and_mismatched_directory(self):
        with self.assertRaises(AssistantError):
            self.agent.prepare(self.actor, {"operations": [{"api_id": _CABINET_PROOF_CORRECT, "path_params": {"batch_id": self.batch_id}}]},
                               "wrong-source", [], queries={"batch": self.base._snapshot(self.batch_id)})
        self.prepare_correct()
        self.directory_override = {"version": "fixture", "items": [{"scope": "A", "room": "201", "rack": "A01"}]}
        with self.assertRaises(AssistantError):
            await self.load_directory()
        self.assertEqual(self.agent.get_plan(self.actor, self.plan["id"])["fields"][0]["rows"], [])
        self.assertEqual(self.writes, [])

    async def test_native_link_only_preserves_business_fields_and_second_confirmation(self):
        value = self.prepare()
        self.assertEqual(value["image_id"], self.image_id)
        self.assertEqual(value["row_id"], "")
        self.assertEqual(self.writes, [])
        self.assertEqual(len(self.plan["fields"]), 1)
        self.assertFalse(any(field.get("path") in {"row_id", "version", "image_id"} for field in self.plan["fields"]))
        value = copy.deepcopy(self.agent.public_plan(self.plan)["fields"][0]["value"])
        public = self.agent.public_plan(self.plan)["fields"][0]
        self.assertEqual(public["images"][0]["image_id"], self.image_id)
        self.assertEqual(public["value"]["image_id"], self.image_id)
        self.assertEqual(public["images"][0]["url"], f"/api/cabinet-power/batches/{self.batch_id}/images/{self.image_id}")
        self.assertEqual(public["images"][0]["candidates"][0]["index"], 0)
        self.assertNotIn("_editor", public)
        value["row_id"] = self.row_ids[0]
        value["fields"] = self.field["rows"][0]["fields"]
        amended = self.amend(value)
        self.assertEqual(amended["operations"][0]["selected_labels"]["selected_document"], "确认图.png")
        self.assertEqual(amended["operations"][0]["body"]["fields"], {})
        result = await self.base._run_to_completion(amended)
        self.assertEqual(result["status"], "completed", result.get("error"))
        saved = self.service.get(self.batch_id)["rows"][0]
        self.assertEqual(saved["evidence_images"], [self.image_id])
        self.assertEqual(saved["actual"], edits._READY_ROW["actual"])

    async def test_image_lifecycle_uses_snapshot_version_and_native_select(self):
        self.service._change(self.batch_id, lambda batch: batch["rows"][0].update(evidence_images=[self.image_id]))
        for action in ("delete", "restore", "retry"):
            with self.subTest(action=action):
                method = "DELETE" if action == "delete" else "POST"
                api = method + " /api/cabinet-power/batches/{batch_id}/images/{image_id}" + ("" if action == "delete" else "/" + action)
                snapshot = self.base._snapshot(self.batch_id)
                plan = self.agent.prepare(self.actor, {"operations": [{"api_id": api, "path_params": {"batch_id": self.batch_id}}]}, "image-" + action, [], queries={"batch": snapshot})
                self.assertEqual(len(plan["fields"]), 1)
                field = plan["fields"][0]
                self.assertEqual(field["type"], "select")
                self.assertEqual(field["options"][0]["value"], self.image_id)
                self.assertNotEqual(field["label"], "image_id")
                self.assertEqual(plan["operations"][0]["params" if action == "delete" else "body"]["version"], snapshot["version"])
                amended = self.agent.amend(self.actor, plan["id"], {"version": plan["version"], "values": {field["name"]: self.image_id}})
                self.assertIn("确认图.png", amended["operations"][0]["selected_labels"]["selected_document"])
                done = await self.base._run_to_completion(amended)
                self.assertEqual(done["status"], "completed", done.get("error"))
                batch = self.service.get(self.batch_id)
                self.assertEqual(bool(batch["images"][0].get("deleted_at")), action == "delete")
                self.assertEqual(self.image_id in batch["rows"][0]["evidence_images"], action != "delete")
        self.assertEqual([item["action"] for item in self.writes], ["delete", "restore", "retry"])

    async def test_image_delete_rejects_wrong_owner_locked_and_stale_version(self):
        api = "DELETE /api/cabinet-power/batches/{batch_id}/images/{image_id}"
        snapshot = self.base._snapshot(self.batch_id)
        def prepare(data, **extra):
            return self.agent.prepare(self.actor, {"operations": [{"api_id": api, "path_params": {"batch_id": self.batch_id}, **extra}]}, "delete-check", [], queries={"batch": data})
        with self.assertRaises(AssistantError):
            prepare({**snapshot, "owner_id": "other"})
        with self.assertRaises(AssistantError):
            prepare(snapshot, params={"version": snapshot["version"] - 1})
        locked = copy.deepcopy(snapshot)
        locked["rows"][0].update(status="completed", evidence_images=[self.image_id])
        with self.assertRaises(AssistantError):
            prepare(locked)
        plan = prepare(snapshot)
        amended = self.agent.amend(self.actor, plan["id"], {"version": plan["version"], "values": {plan["fields"][0]["name"]: self.image_id}})
        self.service._change(self.batch_id, lambda batch: batch.update(title="new revision"))
        done = await self.base._run_to_completion(amended)
        self.assertEqual(done["status"], "failed")
        self.assertEqual(len(self.writes), 1)
        self.assertFalse(self.service.get(self.batch_id)["images"][0].get("deleted_at"))

    async def test_matching_candidate_suggests_times_but_keeps_existing_action_and_result(self):
        value = self.prepare(body={"candidate_index": 0}, image_id=self.image_id)
        self.assertEqual(value["row_id"], self.row_ids[0])
        self.assertEqual(value["fields"]["actual"], "2026-09-17 10:00:00")
        self.assertEqual(value["fields"]["action"], "上正式电")
        self.assertEqual(value["fields"]["result"], "成功")
        before = self.service.get(self.batch_id)
        amended = self.amend(value)
        result = await self.base._run_to_completion(amended)
        self.assertEqual(result["status"], "completed", result.get("error"))
        saved = self.service.get(self.batch_id)
        self.assertEqual(saved["rows"][0]["actual"], value["fields"]["actual"])
        self.assertEqual(saved["rows"][1], before["rows"][1])
        self.assertEqual(set(self.writes[0]["fields"]), {"expected", "actual"})

    async def test_return_to_edit_retains_selected_proof_and_can_change_rack_before_submit(self):
        second_image = "b" * 64
        self.service._change(self.batch_id, lambda batch: batch["images"].append({"image_id": second_image, "name": "第二张.png", "status": "done", "suggestions": []}))
        value = self.prepare(body={"row_id": self.row_ids[0]}, image_id=self.image_id)
        value["image_id"] = second_image
        amended = self.amend(value)
        self.assertTrue(amended["can_edit"])
        reopened = self.agent.amend(self.actor, amended["id"], {"version": amended["version"], "action": "edit"})
        form = reopened["fields"][0]
        self.assertEqual(form["value"]["image_id"], second_image)
        self.assertEqual(form["value"]["row_id"], self.row_ids[0])
        changed = {**form["value"], "row_id": self.row_ids[1]}
        amended = self.agent.amend(self.actor, reopened["id"], {"version": reopened["version"], "values": {form["name"]: changed}})
        self.assertEqual(self.writes, [])
        result = await self.base._run_to_completion(amended)
        self.assertEqual(result["status"], "completed", result.get("error"))
        rows = self.service.get(self.batch_id)["rows"]
        self.assertNotIn(second_image, rows[0].get("evidence_images", []))
        self.assertIn(second_image, rows[1].get("evidence_images", []))

    async def test_mismatched_rack_has_no_time_suggestions_and_detach_does_not_overwrite(self):
        value = self.prepare(body={"row_id": self.row_ids[1], "candidate_index": 0}, image_id=self.image_id)
        self.assertEqual(value["fields"]["actual"], edits._READY_ROW["actual"])
        value.update(attach=False)
        value["fields"]["actual"] = "2026-09-17T10:00:00"
        amended = self.amend(value)
        self.assertEqual(amended["operations"][0]["body"]["fields"], {})
        result = await self.base._run_to_completion(amended)
        self.assertEqual(result["status"], "completed", result.get("error"))
        saved = self.service.get(self.batch_id)["rows"][1]
        self.assertEqual(saved["actual"], edits._READY_ROW["actual"])
        self.assertNotIn(self.image_id, saved.get("evidence_images", []))

    async def test_field_and_identity_tampering_is_rejected_before_write(self):
        value = self.prepare(body={"row_id": self.row_ids[0]})
        for patch in ({"image_id": "foreign"}, {"row_id": "foreign"}, {"candidate_index": 999}, {"candidate_index": True},
                      {"attach": "false"}, {"version": 99}, {"fields": {"cloud_file_token": "forged"}},
                      {"fields": {"action": "not-a-real-action"}}, {"fields": {"actual": "not-a-date"}},
                      {"fields": {"result": "失败", "failure_reason": ""}}):
            with self.subTest(patch=patch), self.assertRaises(AssistantError):
                self.amend({**copy.deepcopy(value), **patch})
        self.assertEqual(self.writes, [])

    async def test_foreign_image_locked_rows_and_stale_version_cannot_be_applied(self):
        snapshot = self.base._snapshot(self.batch_id)
        snapshot["images"][0]["suggestions"][0]["scope"] = "A"
        with self.assertRaises(AssistantError):
            self.prepare(snapshot=snapshot)
        snapshot = self.base._snapshot(self.batch_id)
        for row in snapshot["rows"]:
            row["editable"] = False
        with self.assertRaises(AssistantError):
            self.prepare(snapshot=snapshot)
        value = self.prepare(body={"row_id": self.row_ids[0]})
        amended = self.amend(value)
        self.service._change(self.batch_id, lambda batch: batch.update(title="changed elsewhere"))
        result = await self.base._run_to_completion(amended)
        self.assertEqual(result["status"], "failed")
        self.assertEqual(len(self.writes), 1)
        self.assertNotIn(self.image_id, self.service.get(self.batch_id)["rows"][0].get("evidence_images", []))


if __name__ == "__main__":
    unittest.main()
