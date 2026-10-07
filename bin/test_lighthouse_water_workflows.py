"""Assistant water forms and staged photos exercise isolated native-shaped APIs."""
import asyncio
import copy
import io
import json
import sys
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import test_lighthouse_agent_workflows as workflows
from fastapi import Request
from fastapi.responses import JSONResponse
from PIL import Image
from clipflow_backend.api_models import WaterConsumptionRecordRequest
from lan_bitable_template_portal.lighthouse_agent import PortalAgent, _result_refs
from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog
from lan_bitable_template_portal.lighthouse_ai import AssistantError

ACTOR = {"id": "water-fixture", "scopes": ["E"], "is_admin": True}
CREATE = "POST /api/capacity/water/records"
UPDATE = "PATCH /api/capacity/water/records/{record_id}"


class WaterWorkflowTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.fixture = workflows.WorkflowTests()
        self.fixture.setUp()
        for callback, args, kwargs in self.fixture._cleanups:
            self.addCleanup(callback, *args, **kwargs)
        self.fixture._cleanups.clear()
        self.uploads, self.saves = [], []
        self.fail_photo = 0
        self.write_failure = False
        self.large_change = False
        self.upload_wait = None
        self.write_wait = None
        self.bootstrap = {"scope": "E", "building": "E楼", "permissions": {"can_create": True},
            "options": {"meters": ["市政水", "中水"], "frequencies": ["日", "月"], "shifts": ["白", "夜"]}}
        self.detail = {"record_id": "recWaterE", "scope_code": "E", "version": "v-water-original", "edit_policy": {"can_edit": True, "remaining_edits": 2},
            "meter": "市政水", "frequency": "日", "shift": "白", "statistic_date": "2026-10-02", "meter_value": 100,
            "corrected_usage": -3.5, "photos": [{"image_id": "old-one", "file_name": "原照片一.png", "file_token": "secret-one"},
                                                 {"image_id": "old-two", "file_name": "原照片二.png", "file_token": "secret-two"}]}
        app = self.fixture.catalog.app

        @app.post("/api/capacity/water/uploads")
        async def upload(request: Request):
            content = await request.body()
            self.uploads.append({"scope": request.query_params.get("scope"), "bytes": content})
            with Image.open(io.BytesIO(content)) as img:
                img.verify()
            if self.upload_wait:
                await self.upload_wait.wait()
            if self.fail_photo == len(self.uploads):
                return JSONResponse({"ok": False, "error": "隔离照片上传失败"}, status_code=503)
            return {"ok": True, "data": {"upload_id": "staged-" + str(len(self.uploads)), "expires_at": time.time() + 86400}}

        async def save(request: Request, payload: WaterConsumptionRecordRequest):
            self.saves.append(payload.model_dump())
            if self.write_wait:
                await self.write_wait.wait()
            if self.write_failure:
                return JSONResponse({"ok": False, "error": "写入响应未确认"}, status_code=504)
            if self.large_change and not payload.large_change_confirmed:
                return JSONResponse({"ok": False, "error": "水表数值变化较大", "error_code": "confirmation_required",
                                     "details": {"kind": "water_large_change", "note_required": True}}, status_code=409)
            return {"ok": True, "data": {"record_id": "recWaterE", "version": "v-water-new"}}

        app.add_api_route("/api/capacity/water/records", save, methods=["POST"])
        app.add_api_route("/api/capacity/water/records/{record_id}", save, methods=["PATCH"])
        self.agent = PortalAgent(self.fixture.assistant, PortalAPICatalog(app), self.fixture.files)

    def prepare(self, editing=False, body=None, actor=ACTOR, queries=None):
        op = {"api_id": UPDATE if editing else CREATE, "body": {"scope": "E", **(body or {})}}
        if editing:
            op["path_params"] = {"record_id": self.detail["record_id"]}
        return self.agent.prepare(actor, {"operations": [op]}, "water-workflow", [],
            queries=queries or {"query_bootstrap": copy.deepcopy(self.bootstrap), "query_detail": copy.deepcopy(self.detail)})

    def photo(self, name="new.png", actor=ACTOR):
        image = io.BytesIO()
        Image.new("RGB", (4, 4), "orange").save(image, format="PNG")
        return self.fixture.files.upload(actor, name, image.getvalue(), extract=False)["id"]

    def amend(self, plan, updates=None, photos=None, actor=ACTOR):
        plan = self.agent.get_plan(actor, plan["id"])
        field = next(f for f in plan["fields"] if f.get("native_water_record"))
        filled = _result_refs(field.get("_edit_value", field["value"]), [], plan.get("_references"), plan.get("_queries"))
        filled.update(updates or {})
        values = {field["name"]: filled}
        if photos is not None:
            values[next(f["name"] for f in plan["fields"] if f.get("native_water_photos"))] = photos
        return self.agent.amend(actor, plan["id"], {"version": plan["version"], "values": values})

    async def execute(self, plan, actor=ACTOR):
        result = await self.agent.confirm(actor, plan["id"], {"version": plan["version"], "stage": "review"}, self.fixture.request)
        if result["status"] == "awaiting_second_confirmation":
            await self.agent.confirm(actor, plan["id"], {"version": result["version"], "stage": "execute"}, self.fixture.request)
        if self.agent.tasks:
            await asyncio.gather(*tuple(self.agent.tasks))
        return self.agent.get_plan(actor, plan["id"])

    def create_review(self, count=1):
        return self.amend(self.prepare(), {"meter": "市政水", "meter_value": 120.5}, [self.photo(f"photo-{i}.png") for i in range(count)])

    async def test_create_typed_form_photos_then_single_save(self):
        plan = self.prepare()
        stable_id = plan["operations"][0]["body"]["operation_id"]
        field = next(f for f in plan["fields"] if f.get("native_water_record"))
        self.assertEqual(next(c for c in field["children"] if c["path"] == "meter")["type"], "select")
        self.assertEqual(next(c for c in field["children"] if c["path"] == "statistic_date")["type"], "date")
        review = self.amend(plan, {"meter": "中水", "meter_value": 0, "corrected_usage": None}, [self.photo(), self.photo("second.png")])
        self.assertEqual(self.uploads, [])
        self.assertEqual(self.saves, [])
        done = await self.execute(review)
        self.assertEqual(done["status"], "completed", done["error"])
        self.assertEqual(len(self.saves), 1)
        self.assertEqual([u["scope"] for u in self.uploads], ["E", "E"])
        self.assertEqual(self.saves[0]["operation_id"], stable_id)
        self.assertEqual(self.saves[0]["upload_ids"], ["staged-1", "staged-2"])
        self.assertEqual(self.saves[0]["meter_value"], 0)
        self.assertIsNone(self.saves[0]["corrected_usage"])
        await self.execute(review)
        self.assertEqual(len(self.saves), 1)

    async def test_edit_keeps_unmodified_values_version_and_selected_original_photos(self):
        actor = {**ACTOR, "is_admin": False}
        plan = self.prepare(editing=True, actor=actor)
        public = self.agent.public_plan(plan, actor)
        field = next(f for f in public["fields"] if f.get("native_water_record"))
        self.assertEqual(field["value"]["retained_image_ids"], ["old-one", "old-two"])
        choices = next(c for c in field["children"] if c["path"] == "retained_image_ids")
        self.assertEqual(choices["options"][0]["label"], "原照片一.png")
        self.assertTrue(choices["photo_choices"])
        self.assertNotIn("secret-one", json.dumps(public))
        done = await self.execute(self.amend(plan, {"meter_value": 110, "retained_image_ids": ["old-two"]}, actor=actor), actor=actor)
        self.assertEqual(done["status"], "completed", done["error"])
        self.assertEqual(self.uploads, [])
        self.assertEqual(self.saves[0]["expected_version"], "v-water-original")
        self.assertEqual(self.saves[0]["corrected_usage"], -3.5)
        self.assertEqual(self.saves[0]["retained_image_ids"], ["old-two"])

    async def test_photo_failure_preserves_form_and_successful_receipt_after_restart(self):
        self.fail_photo = 2
        review = self.create_review(2)
        pending = await self.execute(review)
        self.assertEqual(pending["status"], "needs_input", pending["error"])
        self.assertEqual(self.saves, [])
        self.assertEqual(len(pending["_water_uploads"]["0"]), 1)
        self.agent = PortalAgent(self.fixture.assistant, self.agent.catalog, self.fixture.files)
        done = await self.execute(self.amend(pending))
        self.assertEqual(done["status"], "completed", done["error"])
        self.assertEqual(len(self.uploads), 3)
        self.assertEqual(self.saves[0]["upload_ids"], ["staged-1", "staged-3"])
        self.assertEqual(self.saves[0]["meter_value"], 120.5)

    async def test_large_change_resumes_same_id_and_does_not_reupload(self):
        self.large_change = True
        review = self.create_review()
        pending = await self.execute(review)
        self.assertEqual(pending["status"], "needs_input", pending["error"])
        field = pending["fields"][0]
        self.assertTrue(field["native_water_confirmation"])
        resumed = self.agent.amend(ACTOR, pending["id"], {"version": pending["version"], "values": {field["name"]: "现场已核实"}})
        done = await self.execute(resumed)
        self.assertEqual(done["status"], "completed", done["error"])
        self.assertEqual(len(self.uploads), 1)
        self.assertEqual(len(self.saves), 2)
        self.assertEqual(self.saves[0]["operation_id"], self.saves[1]["operation_id"])
        self.assertEqual(self.saves[0]["upload_ids"], self.saves[1]["upload_ids"])

    async def test_write_timeout_never_reopens_edit_or_resubmits(self):
        self.write_failure = True
        done = await self.execute(self.create_review())
        self.assertEqual(done["status"], "failed")
        self.assertEqual(done["fields"], [])
        with self.assertRaises(AssistantError):
            await self.execute(done)
        self.assertEqual(len(self.saves), 1)

    async def test_unexpected_photo_failure_preserves_input_before_record_write(self):
        review = self.create_review()
        with patch.object(self.agent, "_stage_water_photos", side_effect=RuntimeError("isolated storage error")):
            pending = await self.execute(review)
        self.assertEqual(pending["status"], "needs_input")
        self.assertEqual(self.saves, [])
        done = await self.execute(self.amend(pending))
        self.assertEqual(done["status"], "completed", done["error"])

    def test_missing_photos_invalid_files_and_foreign_owner_rejected(self):
        with self.assertRaisesRegex(AssistantError, "照片"):
            self.amend(self.prepare(), {"meter": "市政水", "meter_value": 1})
        with self.assertRaisesRegex(AssistantError, "照片"):
            self.amend(self.prepare(editing=True), {"retained_image_ids": []})
        wrong = self.fixture.files.upload(ACTOR, "document.txt", b"test", extract=False)["id"]
        for file_id in (wrong, self.photo(actor={**ACTOR, "id": "other"})):
            with self.assertRaises(AssistantError):
                self.amend(self.prepare(), {"meter": "市政水", "meter_value": 1}, [file_id])
        with self.assertRaises(AssistantError):
            self.prepare(body={"upload_ids": ["made-up-upload"]})
        self.assertEqual(self.saves, [])

    async def test_restart_during_photos_restores_but_during_save_does_not(self):
        for phase in ("photos", "saving"):
            with self.subTest(phase=phase):
                review = self.create_review()
                stored = self.agent.get_plan(ACTOR, review["id"])
                stored.update(status="running", fields=[], _water_stage={"index": 0, "phase": phase})
                self.agent._save_plan(ACTOR, stored)
                fresh = PortalAgent(self.fixture.assistant, self.agent.catalog, self.fixture.files)
                result = await fresh.refresh(ACTOR, stored, self.fixture.request)
                self.assertEqual(result["status"], "needs_input" if phase == "photos" else "failed")
        self.assertEqual(self.saves, [])

    async def test_cancellation_during_real_photo_call_restores_input(self):
        self.upload_wait = asyncio.Event()
        plan = self.create_review()
        started = await self.agent.confirm(ACTOR, plan["id"], {"version": plan["version"], "stage": "review"}, self.fixture.request)
        if started["status"] == "awaiting_second_confirmation":
            await self.agent.confirm(ACTOR, plan["id"], {"version": started["version"], "stage": "execute"}, self.fixture.request)
        for _ in range(200):
            if self.uploads:
                break
            await asyncio.sleep(.01)
        self.assertEqual(len(self.uploads), 1)
        tasks = tuple(self.agent.tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks)
        stored = self.agent.get_plan(ACTOR, plan["id"])
        self.assertEqual(stored["status"], "needs_input")
        self.assertEqual(self.saves, [])

    async def test_twenty_photos_single_native_write_and_review_count(self):
        review = self.create_review(20)
        self.assertEqual(len(review["operations"][0]["selected_files"]), 20)
        done = await self.execute(review)
        self.assertEqual(done["status"], "completed", done["error"])
        self.assertEqual(len(self.uploads), 20)
        self.assertEqual(len(self.saves), 1)
        self.assertEqual(len(set(self.saves[0]["upload_ids"])), 20)

    async def test_expired_photos_refresh_only_before_first_write(self):
        self.fail_photo = 2
        # Opaque IDs can contain phone-like digits; retry must retain them exactly.
        identities = ['a289617ab13940249922e985f84575f8', 'b289617ab13940249922e985f84575f8']
        with patch('openclaw_service.assistant.lighthouse_files.uuid.uuid4',
                   side_effect=[SimpleNamespace(hex=identity) for identity in identities]):
            photos = [self.photo('first.png'), self.photo('second.png')]
        review = self.amend(self.prepare(), {'meter': '市政水', 'meter_value': 120.5}, photos)
        pending = await self.execute(review)
        photo_field = next(field for field in pending['fields'] if field.get('native_water_photos'))
        self.assertEqual(photo_field['_edit_value'], identities)
        receipt = next(iter(pending["_water_uploads"]["0"].values()))
        receipt["expires_at"] = time.time() - 1
        self.agent._save_plan(ACTOR, pending)
        done = await self.execute(self.amend(pending))
        self.assertEqual(done["status"], "completed", done["error"])
        self.assertEqual(len(self.uploads), 4)
        self.assertEqual(len(self.saves), 1)
        self.assertEqual(self.saves[0]["upload_ids"], ["staged-3", "staged-4"])

    def test_cannot_bypass_permissions_or_preconfirm_abnormality(self):
        with self.assertRaises(AssistantError):
            self.prepare(actor={**ACTOR, "is_admin": False})
        self.detail["edit_policy"]["can_edit"] = False
        with self.assertRaises(AssistantError):
            self.prepare(editing=True)
        self.detail["edit_policy"]["can_edit"] = True
        with self.assertRaises(AssistantError):
            self.prepare(editing=True, body={"expected_version": "outdated"})
        plan = self.prepare(body={"large_change_confirmed": True, "abnormal_note": "model invented"})
        self.assertFalse(plan["operations"][0]["body"]["large_change_confirmed"])
        self.assertEqual(plan["operations"][0]["body"]["abnormal_note"], "")

    def test_newer_incomplete_metadata_cannot_fall_back_to_old_snapshot(self):
        for newer in ({**self.bootstrap, "options": {}}, {**self.detail, "scope_code": "B"}, {**self.detail, "photos": None}):
            with self.subTest(newer=newer), self.assertRaises(AssistantError):
                self.prepare(editing=True, queries={"old_bootstrap": self.bootstrap, "old_detail": self.detail, "newer": newer})


if __name__ == "__main__":
    unittest.main()
