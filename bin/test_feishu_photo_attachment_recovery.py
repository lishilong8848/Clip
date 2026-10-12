import copy
import time
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from .test_feishu_notice_cards import MemoryStore
from lan_bitable_template_portal import feishu_assistant_files as files
from openclaw_service.assistant.lighthouse_ai import AssistantError


class PhotoRecoveryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.store = MemoryStore()
        self.bot = SimpleNamespace(runtime=SimpleNamespace(state_store=self.store), _call=AsyncMock())
        self.actor = {"id": "fixture-person", "channel": "feishu:fixture"}
        self.message = {"message_id": "om_fixture", "chat_id": "fixture", "question": "现场照片"}
        self.field = {"name": "step0.notice_site_photos", "native_notice_photos": "site", "value": ["old"], "maxItems": 10}
        self.plan = {"id": "plan-a", "version": 2, "status": "needs_input", "fields": [self.field]}
        self.conversation = {"conversation_id": "conv", "turns": [{"plan": self.plan}]}

    async def test_append_preserves_previous_images_and_never_submits(self):
        result_plan = copy.deepcopy(self.plan)
        result_plan["fields"][0]["value"] = ["old", "new"]
        self.bot._call.return_value = result_plan
        with patch("lan_bitable_template_portal.feishu_notice_cards.prepare_card", new=AsyncMock(return_value={"card": {"body": {}}, "ticket": {}})):
            await files._attach_photos_to_plan(self.bot, self.message, None, self.actor, self.store,
                "owner", self.actor["id"], self.actor["channel"], [], "现场照片", self.conversation,
                self.plan, self.field, imported_ids=["new"])
        args = self.bot._call.call_args
        self.assertEqual(args.args[2], "plans/plan-a")
        self.assertEqual(args.args[3]["values"][self.field["name"]], ["old", "new"])
        self.assertEqual(args.args[3]["action"], "attach_files")
        self.assertEqual(self.bot._call.await_count, 1)

    async def test_disappeared_first_candidate_does_not_retarget_second(self):
        self.store.put_document(files.NAMESPACE, "photo:owner", {
            "actor": self.actor["id"], "channel": self.actor["channel"], "expires": time.time() + 60,
            "conversation_id": "conv", "entries": [
                {"plan_id": "removed", "field_name": self.field["name"], "plan_version": 2},
                {"plan_id": "plan-a", "field_name": self.field["name"], "plan_version": 2}], "imported_file_ids": ["new"]})
        result = await files._attach_stashed_photos(self.bot, self.message, None, self.actor, self.store,
            "owner", self.actor["id"], self.actor["channel"], self.conversation, 1)
        self.assertIn("不存在", result["text"])
        self.bot._call.assert_not_called()

    async def test_partial_import_reuses_durable_receipt(self):
        resources = [{"kind": "image", "key": "img_a"}, {"kind": "image", "key": "img_b"}]
        upload = AsyncMock(side_effect=["file-a", AssistantError("temporary"), "file-b"])
        with patch.object(files, "_download", new=AsyncMock(return_value=b"fixture")), patch.object(files, "_upload_name", return_value="photo.png"), patch.object(files, "_upload", new=upload):
            with self.assertRaises(AssistantError):
                await files._import_photo_file_ids(self.bot, self.message, None, self.actor, copy.deepcopy(resources), self.conversation)
            result = await files._import_photo_file_ids(self.bot, self.message, None, self.actor, copy.deepcopy(resources), self.conversation)
        self.assertEqual(result, ["file-a", "file-b"])
        self.assertEqual(upload.await_count, 3)

    async def test_picture_question_does_not_attach_to_waiting_notice(self):
        self.field.update(required=True, value=[])
        message = {**self.message, "question": "查看图片内容", "content": {"image_key": "img_fixture"}}
        with patch.object(files, "_take_pending", new=AsyncMock(return_value={"text": "read"})) as read:
            self.assertEqual(await files.handle_files(self.bot, message, None, self.actor, self.conversation), {"text": "read"})
        read.assert_awaited_once()
        self.bot._call.assert_not_called()
