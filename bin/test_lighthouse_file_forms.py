"""Isolated attachment selection, reopen, clearing, transport and ownership checks."""
import asyncio
import copy
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fastapi import FastAPI, File, UploadFile
from lan_bitable_template_portal.lighthouse_agent import PortalAgent
from lan_bitable_template_portal.lighthouse_ai import AssistantError, LighthouseAssistant
from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog
from lan_bitable_template_portal.lighthouse_files import LighthouseFiles
from test_lighthouse_stream import Store
from test_lighthouse_plan_edit import _request


class FileFormTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = Store(Path(self.temp.name) / "state.sqlite3")
        model = Mock()
        model.settings.return_value = {}
        self.assistant = LighthouseAssistant(self.store, lambda *_: ([], []), model=model)
        self.files = LighthouseFiles(self.store)
        self.actor = {"id": "file-owner", "scopes": ["A"], "allowed_scopes": ["A", "B"], "is_admin": True}
        self.native = []
        self.app = FastAPI()

        @self.app.post("/api/upload-fixture/optional")
        async def optional(files: list[UploadFile] = File(default=[])):
            self.native.append([(file.filename, await file.read()) for file in files])
            return {"ok": True, "data": {"count": len(files)}}

        @self.app.post("/api/upload-fixture/single")
        async def single(file: UploadFile = File(...)):
            self.native.append([(file.filename, await file.read())])
            return {"ok": True}

        self.catalog = PortalAPICatalog(self.app)
        self.agent = PortalAgent(self.assistant, self.catalog, self.files)
        self.first = self.files.upload(self.actor, "原文件.txt", b"first", extract=False)
        self.second = self.files.upload(self.actor, "替换文件.txt", b"second", extract=False)

    def prepare(self, *, single=False, edit=True):
        path, name = ("single", "file") if single else ("optional", "files")
        plan = self.agent.prepare(self.actor, {"operations": [{"api_id": "POST /api/upload-fixture/" + path,
            "files": {name: [self.first["id"]]}}]}, "file-form", [self.first["id"]])
        if edit:
            self.agent.amend(self.actor, plan["id"], {"version": plan["version"], "action": "edit"})
            return self.agent.get_plan(self.actor, plan["id"])
        return plan

    async def test_already_filled_upload_keeps_original_confirmation_sequence(self):
        plan = self.prepare(single=True, edit=False)
        public = self.agent.public_plan(plan, self.actor)
        self.assertEqual(public["status"], "awaiting_confirmation")
        self.assertTrue(public["can_edit"])
        self.assertEqual(public["operations"][0]["selected_files"][0]["name"], "原文件.txt")
        await self.execute(plan)
        self.assertEqual(self.native, [[("原文件.txt", b"first")]])

    async def execute(self, plan):
        plan = await self.agent.confirm(self.actor, plan["id"], {"version": plan["version"], "stage": "review"}, _request())
        if plan["status"] == "awaiting_second_confirmation":
            await self.agent.confirm(self.actor, plan["id"], {"version": plan["version"], "stage": "execute"}, _request())
        await asyncio.gather(*tuple(self.agent.tasks))
        final = self.agent.get_plan(self.actor, plan["id"])
        self.assertEqual(final["status"], "completed", final.get("error"))

    async def test_prefilled_file_is_visible_then_replaced_after_reopen_and_restart(self):
        plan = self.prepare(single=True)
        public = self.agent.public_plan(plan, self.actor)
        field = public["fields"][0]
        self.assertEqual(plan["status"], "needs_input")
        self.assertEqual(field["value"], [self.first["id"]])
        self.assertEqual(field["maxItems"], 1)
        self.assertTrue(field["required"])
        self.assertEqual(field["selected_files"][0]["name"], "原文件.txt")
        self.assertNotIn("path", field["selected_files"][0])
        self.assertNotIn("text", field["selected_files"][0])
        submitted = self.agent.amend(self.actor, plan["id"], {"version": plan["version"], "values": {field["name"]: [self.second["id"]]}})
        self.assertEqual(submitted["operations"][0]["selected_files"][0]["name"], "替换文件.txt")
        self.agent = PortalAgent(self.assistant, self.catalog, self.files)
        edited = self.agent.amend(self.actor, plan["id"], {"version": submitted["version"], "action": "edit"})
        self.assertEqual(edited["fields"][0]["value"], [self.second["id"]])
        self.assertEqual(edited["fields"][0]["selected_files"][0]["name"], "替换文件.txt")
        self.assertEqual(self.native, [])
        final = self.agent.amend(self.actor, plan["id"], {"version": edited["version"], "values": {field["name"]: edited["fields"][0]["value"]}})
        await self.execute(final)
        self.assertEqual(self.native, [[("替换文件.txt", b"second")]])

    async def test_optional_files_can_be_cleared_without_deleting_source(self):
        plan = self.prepare()
        field = plan["fields"][0]
        self.assertFalse(field["required"])
        filled = self.agent.amend(self.actor, plan["id"], {"version": plan["version"], "values": {field["name"]: [self.first["id"], self.second["id"]]}})
        edited = self.agent.amend(self.actor, plan["id"], {"version": filled["version"], "action": "edit"})
        cleared = self.agent.amend(self.actor, plan["id"], {"version": edited["version"], "values": {field["name"]: []}})
        self.assertEqual(cleared["operations"][0]["selected_files"], [])
        await self.execute(cleared)
        self.assertEqual(self.native, [[]])
        self.assertEqual(self.files.get(self.actor, self.first["id"])["name"], "原文件.txt")

    async def test_required_single_file_rejects_empty_multiple_and_foreign_ids(self):
        plan = self.prepare(single=True)
        field = plan["fields"][0]
        foreign = self.files.upload({**self.actor, "id": "someone-else"}, "foreign.txt", b"private", extract=False)
        for selection in ([], [self.first["id"], self.second["id"]], [foreign["id"]]):
            with self.subTest(selection=selection), self.assertRaises(AssistantError):
                self.agent.amend(self.actor, plan["id"], {"version": plan["version"], "values": {field["name"]: selection}})
        with self.assertRaises(AssistantError):
            self.catalog.validate_operation({"api_id": "POST /api/upload-fixture/single", "files": {"file": [self.first["id"], self.second["id"]]}})
        self.assertEqual(self.native, [])

    async def test_metadata_respects_current_file_access_and_missing_files(self):
        plan = self.prepare(single=True)
        protected = self.files.upload(self.actor, "B楼资料.txt", b"scoped", extract=False, source_scopes=["B"])
        field = plan["fields"][0]
        field["value"] = [protected["id"]]
        public = self.agent.public_plan(plan, self.actor)
        self.assertEqual(public["fields"][0]["selected_files"][0]["name"], "B楼资料.txt")
        restricted = self.agent.public_plan(plan, {**self.actor, "allowed_scopes": ["A"]})
        selected = restricted["fields"][0]["selected_files"][0]
        self.assertTrue(selected["unavailable"])
        self.assertNotIn("url", selected)
        self.assertNotIn("B楼资料", str(selected))
        missing = copy.deepcopy(plan)
        missing["fields"][0]["value"] = ["a" * 32]
        self.assertTrue(self.agent.public_plan(missing, self.actor)["fields"][0]["selected_files"][0]["unavailable"])


if __name__ == "__main__":
    unittest.main()
