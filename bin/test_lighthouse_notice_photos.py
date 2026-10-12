"""Native notice photo continuation with isolated HTTP endpoints and real files."""
import copy
import io
import time
import unittest

from fastapi import Request
from fastapi.responses import JSONResponse
from PIL import Image

from .test_lighthouse_notice_workflows import NoticeWorkflowTests, ACTOR, ORIGINAL, VALID_STARTS
from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_notice_photos import photo_fields


class NoticePhotoTests(unittest.IsolatedAsyncioTestCase):
    setUp = NoticeWorkflowTests.setUp
    _operation_id = NoticeWorkflowTests._operation_id
    _update_end_decision = NoticeWorkflowTests._update_end_decision
    _execute = NoticeWorkflowTests._execute

    def native_endpoints(self, *, already_has_photo=False, reject=False):
        app = self.catalog.app
        app.router.routes[:] = [route for route in app.router.routes if getattr(route, "path", "") not in {"/api/workbench-actions", "/api/notice-attachments"}]

        @app.post("/api/workbench-actions")
        async def submit(request: Request):
            data = await request.json()
            if reject:
                return JSONResponse({"ok": False, "error": "无权访问"}, status_code=403)
            if data["action"] == "end" and not already_has_photo and not data.get("patch", {}).get("extra_images"):
                return JSONResponse({"ok": False, "error": "结束通告前必须至少上传过一张现场照片。", "error_code": "notice_photo_required", "details": {"kind": "site"}}, status_code=422)
            self.notice_writes.append(data)
            return {"ok": True, "data": {"record_id": "rec-orig"}}

        @app.post("/api/notice-attachments")
        async def upload(request: Request):
            self.upload_writes.append(await request.body())
            return {"ok": True, "data": {"upload_id": "up-site", "expires_at": time.time() + 86400}}

    def prepare_end(self, work="maintenance"):
        self.ongoing = [{**copy.deepcopy(ORIGINAL), **VALID_STARTS[work], "work_type": work, "site_photos": []}]
        plan = self.agent.prepare(ACTOR, self._update_end_decision("end", "工作已完成", work_type=work),
                                  self._operation_id(work), [], queries={"query_" + "a" * 32: {"ongoing": self.ongoing}})
        return self.agent.amend(ACTOR, plan["id"], {"version": plan["version"], "values": {}})

    def image(self, actor=ACTOR):
        data = io.BytesIO()
        Image.new("RGB", (3, 3), "white").save(data, format="PNG")
        return self.files.upload(actor, "现场.png", data.getvalue(), purpose="notice_photo")

    async def test_missing_photo_returns_form_then_same_notice_finishes(self):
        self.native_endpoints()
        plan = self.prepare_end()
        self.assertEqual(self.notice_writes, [])
        missing = await self._execute(plan)
        self.assertEqual(missing["status"], "needs_input", missing)
        self.assertEqual(missing["results"], [])
        photo = next(field for field in missing["fields"] if field.get("native_notice_photos") == "site")
        self.assertTrue(photo["required"])
        uploaded = self.image()
        partial = self.agent.amend(ACTOR, missing["id"], {"version": missing["version"], "action": "attach_files", "values": {photo["name"]: [uploaded["id"]]}})
        self.assertEqual(partial["status"], "needs_input")
        self.assertEqual(self.upload_writes, [])
        review = self.agent.amend(ACTOR, partial["id"], {"version": partial["version"], "values": {}})
        self.assertEqual(review["status"], "awaiting_confirmation")
        self.assertEqual(review["operations"][0]["selected_files"][0]["name"], "现场.png")
        final = await self._execute(review)
        self.assertEqual(final["status"], "completed", final.get("error"))
        self.assertEqual(len(self.notice_writes), 1)
        body = self.notice_writes[0]
        self.assertEqual(body["target_record_id"], "rec-orig")
        self.assertEqual(body["action"], "end")
        self.assertEqual(body["patch"]["extra_images"][0]["upload_id"], "up-site")
        self.assertEqual(body["patch"]["progress"], "工作已完成")
        self.assertEqual(len(self.upload_writes), 1)

    async def test_existing_cumulative_photo_needs_no_new_image(self):
        self.native_endpoints(already_has_photo=True)
        final = await self._execute(self.prepare_end())
        self.assertEqual(final["status"], "completed")
        self.assertEqual(self.upload_writes, [])

    async def test_other_failures_are_not_replayed_as_missing_photo(self):
        self.native_endpoints(reject=True)
        final = await self._execute(self.prepare_end())
        self.assertEqual(final["status"], "failed")
        self.assertEqual(self.notice_writes, [])

    async def test_other_owner_and_non_image_are_rejected_without_write(self):
        self.native_endpoints()
        missing = await self._execute(self.prepare_end())
        field = next(field for field in missing["fields"] if field.get("native_notice_photos"))
        other = self.image({**ACTOR, "id": "another-person"})
        text = self.files.upload(ACTOR, "材料.txt", b"not an image", extract=False)
        for identity in (other["id"], text["id"]):
            with self.subTest(identity=identity), self.assertRaises(AssistantError):
                self.agent.amend(ACTOR, missing["id"], {"version": missing["version"], "action": "attach_files", "values": {field["name"]: [identity]}})
        self.assertEqual(self.notice_writes, [])
        self.assertEqual(self.upload_writes, [])

    def test_all_six_types_expose_site_upload_and_change_also_ali(self):
        for work in VALID_STARTS:
            fields = photo_fields(0, work)
            self.assertEqual([field["native_notice_photos"] for field in fields], ["site", "ali"] if work == "change" else ["site"])
            self.assertFalse(fields[0]["required"])
            self.assertEqual(fields[0]["max_bytes"], 8 * 1024 * 1024)
