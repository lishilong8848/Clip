"""Offline adapter checks: every HTTP call uses MockTransport, including retries."""
import copy
import gzip
import hashlib
import json
import sys
import tempfile
import types
import unittest
import uuid
from email.parser import BytesParser
from email.policy import default
from pathlib import Path
from unittest.mock import Mock, patch

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lan_bitable_template_portal import learning_cloud as cloud
from upload_event_module.services.http_client import FeishuHttpClient


class FakeFeishu:
    def __init__(self):
        self.tables = [{"table_id": t, "name": b} for b, t in cloud.BANK_TABLES.items()]
        self.fields, self.records, self.tokens = {}, {}, {}
        self.media = {}
        for bank, table in cloud.BANK_TABLES.items():
            spec = {"题目": 1, "答案": 1}
            spec.update({"题型": 3, "选项": 1, "年份": 1, "备注": 1, "附件": 1} if bank == "written" else {"年度": 3, "答案图片": 17})
            self.fields[table] = [{"field_name": n, "type": t} for n, t in spec.items()]
            self.records[table] = []
        self.calls = []
        self.before = self.after = None

    @staticmethod
    def response(data):
        return httpx.Response(200, json={"code": 0, "data": data})

    def page(self, items, request):
        start = int(request.url.params.get("page_token", "0"))
        return self.response({"items": items[start:start + 2], "has_more": start + 2 < len(items),
                              "page_token": str(start + 2)})

    def __call__(self, request):
        path = request.url.path.split(f"/apps/{cloud.APP_TOKEN}/")[-1]
        self.calls.append((request.method, path, dict(request.url.params), request))
        if self.before:
            response = self.before(request)
            if response is not None:
                return response
        response = self.dispatch(request, path)
        if self.after:
            replacement = self.after(request, response)
            if replacement is not None:
                return replacement
        return response

    def dispatch(self, request, path):
        method = request.method
        if "/medias/upload_all" in path:
            message = BytesParser(policy=default).parsebytes(
                f"Content-Type: {request.headers['content-type']}\r\n\r\n".encode() + request.content,
            )
            file = next(part for part in message.iter_parts()
                        if part.get_param("name", header="content-disposition") == "file")
            token = f"fileUploaded{len(self.media) + 1}" if self.media else "fileUploaded"
            self.media[token] = file.get_payload(decode=True)
            return self.response({"file_token": token})
        if "/medias/" in path:
            token = path.split("/medias/")[-1].split("/")[0]
            if token not in self.media:
                return httpx.Response(404)
            return httpx.Response(200, content=self.media[token], headers={"content-type": "application/octet-stream"})
        body = json.loads(request.content) if request.content else {}
        if path == "tables":
            if method == "GET":
                return self.page(self.tables, request)
            spec = body["table"]
            table = "tblEntities"
            self.tables.append({"table_id": table, "name": spec["name"]})
            self.fields[table] = copy.deepcopy(spec["fields"])
            self.records[table] = []
            return self.response({"table_id": table})
        parts = path.split("/")
        table, resource = parts[1:3]
        if resource == "fields":
            if method == "GET":
                return self.page(self.fields[table], request)
            if body["field_name"] not in {f["field_name"] for f in self.fields[table]}:
                self.fields[table].append(body)
            return self.response({"field": body})
        records = self.records[table]
        if path.endswith("/search"):
            def matches(record):
                for condition in body["filter"]["conditions"]:
                    val = record["fields"].get(condition["field_name"])
                    if isinstance(val, list):
                        val = "".join(x["text"] for x in val)
                    if val != condition["value"][0]:
                        return False
                return True
            return self.page([r for r in records if matches(r)], request)
        if method == "GET":
            return self.page(records, request)
        if method == "PUT":
            record = next(r for r in records if r["record_id"] == parts[-1])
            record["fields"].update(body["fields"])
        else:
            token = request.url.params["client_token"]
            identity = (table, token)
            if identity not in self.tokens:
                record = {"record_id": f"recNew{len(records) + 1}", "fields": copy.deepcopy(body["fields"])}
                self.tokens[identity] = record
                records.append(record)
            record = self.tokens[identity]
        return self.response({"record": record})

    def mutations(self):
        return [c for c in self.calls if c[0] != "GET" and not c[1].endswith("/search")]


class LearningCloudTests(unittest.TestCase):
    def test_migrated_source_table_configuration(self):
        self.assertEqual(cloud.APP_TOKEN, "GKyVb2Az6auMwvs7VzVc3GO0nob")
        self.assertEqual(cloud.BANK_TABLES, {
            "written": "tbldEn2ODX9CbZ1n",
            "duty": "tblpM9nCRs4UJ8io",
            "professional": "tblnyQverizwaQcT",
        })

    def setUp(self):
        self.fake = FakeFeishu()
        self.guard = Mock(return_value={"real_write_allowed": True})
        self.manager = Mock()
        self.manager.get_tenant_token.return_value = "offline-test-token"
        self.stubs = patch.dict(sys.modules, {
            "lan_bitable_template_portal.portal_service": types.SimpleNamespace(external_real_write_guard=self.guard),
            "upload_event_module.services.feishu_token_manager": types.SimpleNamespace(token_manager=self.manager),
        })
        self.stubs.start()
        self.addCleanup(self.stubs.stop)
        self.sleep = patch("upload_event_module.services.http_client.time.sleep")
        self.sleep.start()
        self.addCleanup(self.sleep.stop)
        self.remote = self.new_remote()

    def new_remote(self, enabled=True):
        client = FeishuHttpClient(transport=httpx.MockTransport(self.fake), retries=1)
        remote = cloud.LearningCloud(enabled=enabled, http_client=client)
        self.addCleanup(remote.close)
        return remote

    def question(self, bank="written", **changes):
        q = {
            "id": "b9441894-239a-41d9-b828-0833d37d2f4c", "bank": bank,
            "stem": "测试题目", "type": "multiple", "type_label": "多选题", "year": "2026",
            "options": [{"id": "stable-b", "text": "第二项"}, {"id": "stable-a", "text": "第一项"}],
            "correct_option_ids": ["stable-a"], "answer_text": "说明", "version": 2,
            "versions": [{"version": 1}], "family_id": "family-1", "status": "active",
            "audit": [{"action": "edit"}], "topic": "安全", "specialty": "电气", "difficulty": 2,
            "analysis": "解析", "hint": "提示", "metadata": {"custom_extension": "keep"},
            "attachments": [{"id": "attachment-id", "sha256": "abc", "name": "答案.png",
                             "kind": "answer", "file_token": "fileAnswer", "size": 123}],
        }
        q.update(changes)
        return q

    def test_disabled_initialization_and_writes_do_not_touch_http_or_credentials(self):
        remote = self.new_remote(enabled=False)
        for action in (remote.ensure_schema,
                       lambda: remote.save_question(self.question(), "op"),
                       lambda: remote.upsert_entity("doc", "key", {}, "op"),
                       lambda: remote.upload_attachment("missing", "file")):
            with self.assertRaisesRegex(cloud.LearningCloudError, "管理员启用"):
                action()
        self.assertEqual(self.fake.calls, [])
        self.manager.get_tenant_token.assert_not_called()

    def test_real_write_guard_blocks_every_write_even_after_schema_ready(self):
        self.remote.ensure_schema()
        self.fake.calls.clear()
        self.guard.return_value = {"real_write_allowed": False, "reason": "offline blocked"}
        for action in (self.remote.ensure_schema,
                       lambda: self.remote.save_question(self.question(), "op"),
                       lambda: self.remote.upsert_entity("doc", "key", {}, "op"),
                       lambda: self.remote.upload_attachment("missing", "file")):
            with self.assertRaisesRegex(cloud.LearningCloudError, "offline blocked"):
                action()
        self.assertFalse(self.fake.calls)

    def test_fetch_all_banks_and_pages_returns_raw_fields_without_schema_writes(self):
        for bank, table in cloud.BANK_TABLES.items():
            self.fake.records[table] = [{"record_id": f"rec{i}", "fields": {
                "题目": [{"type": "text", "text": bank}], cloud.META_FIELD: "{not decoded}", "unknown": i,
            }} for i in range(5)]
        result = self.new_remote(enabled=False).fetch_questions()
        self.assertEqual(len(result), 15)
        self.assertEqual([r["bank"] for r in result], [b for b in cloud.BANK_TABLES for _ in range(5)])
        self.assertEqual(result[-1]["fields"][cloud.META_FIELD], "{not decoded}")
        self.assertEqual(len(self.fake.calls), 9)
        self.assertFalse(self.fake.mutations())

    def test_schema_is_idempotent_including_a_new_adapter_instance(self):
        before = copy.deepcopy(self.fake.fields)
        self.remote.ensure_schema()
        mutations = self.fake.mutations()
        self.assertEqual(len(mutations), 7)
        self.assertEqual([c[1] for c in mutations].count("tables"), 1)
        for table in cloud.BANK_TABLES.values():
            self.assertEqual(self.fake.fields[table][:-2], before[table])
            self.assertEqual({f["field_name"]: f["type"] for f in self.fake.fields[table][-2:]}, cloud.EXTRA_FIELDS)
        self.assertEqual({f["field_name"]: f["type"] for f in self.fake.fields["tblEntities"]}, cloud.ENTITY_FIELDS)
        self.remote.ensure_schema()
        self.new_remote().ensure_schema()
        self.assertEqual(len(self.fake.mutations()), 7)

    def test_wrong_source_and_extension_field_types_fail_before_any_mutation(self):
        for bank, field in (("written", "附件"), ("written", "题型"), ("duty", "答案图片"), ("professional", "答案图片")):
            with self.subTest(bank=bank, field=field):
                table = cloud.BANK_TABLES[bank]
                item = next(f for f in self.fake.fields[table] if f["field_name"] == field)
                original = item["type"]
                item["type"] = 99
                with self.assertRaisesRegex(cloud.LearningCloudError, "类型错误"):
                    self.new_remote().ensure_schema()
                item["type"] = original
        self.fake.fields[cloud.BANK_TABLES["duty"]].append({"field_name": cloud.META_FIELD, "type": 17})
        with self.assertRaisesRegex(cloud.LearningCloudError, "类型错误"):
            self.remote.ensure_schema()
        self.assertFalse(self.fake.mutations())

    def test_existing_entity_table_adds_only_missing_fields_and_checks_types(self):
        self.fake.tables.append({"table_id": "tblExisting", "name": cloud.ENTITY_TABLE_NAME})
        self.fake.fields["tblExisting"] = [{"field_name": "业务键", "type": 1}]
        self.fake.records["tblExisting"] = []
        self.remote.ensure_schema()
        self.assertEqual(len(self.fake.mutations()), 9)
        self.assertFalse(any(c[1] == "tables" for c in self.fake.mutations()))
        self.fake.fields["tblExisting"][0]["type"] = 3
        with self.assertRaisesRegex(cloud.LearningCloudError, "类型错误"):
            self.new_remote().ensure_schema()

    def test_question_preserves_id_metadata_unknown_columns_and_old_attachments(self):
        for bank, table in cloud.BANK_TABLES.items():
            original = {"未知字段": "preserve", "备注": "keep notes", "附件": "legacy TEXT",
                        "答案图片": [{"file_token": "fileLegacy"}]}
            self.fake.records[table].append({"record_id": "recOriginal", "fields": copy.deepcopy(original)})
            q = self.question(bank, record_id="recOriginal")
            saved = self.remote.save_question(q, f"save-{bank}")
            self.assertEqual(saved, q)
            fields = self.fake.records[table][0]["fields"]
            for name, value in original.items():
                self.assertEqual(fields[name], value)
            if bank == "written":
                self.assertEqual(fields["题型"], "多选题")
                self.assertEqual(fields["选项"], "A. 第二项\nB. 第一项")
            else:
                self.assertNotIn("题型", fields)
                self.assertNotIn("选项", fields)
            self.assertEqual(fields["答案"], "B")
            self.assertEqual(fields[cloud.MATERIAL_FIELD], [{"file_token": "fileAnswer"}])
            metadata = json.loads(fields[cloud.META_FIELD])
            for key in ("id", "version", "versions", "family_id", "options", "correct_option_ids", "status",
                        "audit", "topic", "specialty", "difficulty", "analysis", "hint", "type", "type_label", "attachments"):
                self.assertEqual(metadata[key], q[key])
            self.assertEqual(metadata["custom_extension"], "keep")
            self.assertNotIn("stem", metadata)

    def test_question_missing_old_id_uses_source_id_but_new_id_is_retained(self):
        table = cloud.BANK_TABLES["written"]
        self.fake.records[table].append({"record_id": "recOld", "fields": {}})
        q = self.question(record_id="recOld")
        del q["id"]
        self.assertEqual(self.remote.save_question(q, "op-old")["id"], "written:recOld")
        created = self.remote.save_question(self.question(), "op-new")
        self.assertEqual(created["id"], self.question()["id"])
        self.assertEqual(created["record_id"], "recNew2")

    def test_removed_attachment_tokens_roundtrip_without_changing_legacy_images(self):
        removed = ["fileLegacy1", "fileLegacy2"]
        for bank, nested in (("duty", False), ("professional", True)):
            with self.subTest(bank=bank, nested_metadata=nested):
                legacy = [{"file_token": token, "name": f"{token}.png"} for token in removed]
                self.fake.records[cloud.BANK_TABLES[bank]].append({
                    "record_id": "recOriginal", "fields": {"答案图片": copy.deepcopy(legacy)},
                })
                q = self.question(bank, record_id="recOriginal", type="interview", type_label="面试",
                                  options=[], correct_option_ids=[], attachments=[])
                target = q["metadata"] if nested else q
                target["removed_attachment_tokens"] = removed.copy()
                original = copy.deepcopy(q)
                saved = self.remote.save_question(q, f"remove-{bank}")
                fields = self.fake.records[cloud.BANK_TABLES[bank]][0]["fields"]
                metadata = json.loads(fields[cloud.META_FIELD])
                self.assertEqual(metadata["removed_attachment_tokens"], removed)
                self.assertEqual(metadata["attachments"], [])
                self.assertEqual(fields["答案图片"], legacy)
                self.assertEqual(fields[cloud.MATERIAL_FIELD], [])
                self.assertEqual(saved, original)
                self.assertEqual(q, original)

    def test_incomplete_draft_with_empty_correct_selection_saves_full_metadata(self):
        cases = (("single", self.question()["options"]), ("multiple", []))
        for kind, options in cases:
            with self.subTest(type=kind, options=options):
                q = self.question(id=f"draft-{kind}", status="draft", type=kind,
                                  type_label=cloud.TYPE_LABELS[kind], stem="", options=options,
                                  correct_option_ids=[], answer_text="待管理员核对的原答案",
                                  removed_attachment_tokens=["fileRemoved"], updated_at="2026-09-29")
                result = self.remote.save_question(q, f"save-draft-{kind}")
                record = next(r for r in self.fake.records[cloud.BANK_TABLES["written"]]
                              if r["record_id"] == result["record_id"])
                fields = record["fields"]
                metadata = json.loads(fields[cloud.META_FIELD])
                self.assertEqual(fields["答案"], q["answer_text"])
                self.assertEqual(metadata["source_answer"], q["answer_text"])
                for key in ("id", "status", "version", "versions", "family_id", "options",
                            "correct_option_ids", "answer_text", "audit", "analysis", "hint",
                            "attachments", "removed_attachment_tokens", "updated_at"):
                    self.assertEqual(metadata[key], q[key])
                self.assertEqual(metadata["custom_extension"], "keep")
                self.assertEqual(result["id"], q["id"])

    def test_year_uses_actual_field_type_and_interview_answer_is_text(self):
        for bank, kind, value in (("written", 2, 2026.0), ("duty", 3, "2026"), ("professional", 4, ["2026"])):
            table = cloud.BANK_TABLES[bank]
            name = "年份" if bank == "written" else "年度"
            next(f for f in self.fake.fields[table] if f["field_name"] == name)["type"] = kind
            self.new_remote().save_question(self.question(bank, type="interview", type_label="面试题", options=[], correct_option_ids=[]), bank)
            fields = self.fake.records[table][0]["fields"]
            self.assertEqual(fields[name], value)
            self.assertEqual(fields["答案"], "说明")

    def test_invalid_options_and_binary_metadata_fail_before_schema_mutation(self):
        invalid = [self.question(correct_option_ids=["missing"]), self.question(options=[{"id": "x", "text": "a"}, {"id": "x", "text": "b"}]),
                   self.question(metadata={"binary": b"not metadata"}), self.question(id=None),
                   self.question(record_id="rec/path")]
        for q in invalid:
            with self.assertRaises(ValueError):
                self.remote.save_question(q, "op")
        self.assertFalse(self.fake.mutations())

    def test_second_page_failure_never_returns_partial_questions(self):
        table = cloud.BANK_TABLES["written"]
        self.fake.records[table] = [{"record_id": f"rec{i}", "fields": {}} for i in range(3)]
        def fail(request):
            if request.url.params.get("page_token"):
                raise httpx.ReadTimeout("private-token must not leak", request=request)
        self.fake.before = fail
        with self.assertRaises(cloud.LearningCloudError) as error:
            self.remote.fetch_questions()
        self.assertNotIn("private-token", str(error.exception))
        self.assertEqual(len(self.fake.calls), 3)

    def test_missing_repeated_cursor_and_malformed_pages_fail_closed(self):
        for page in ({"items": [], "has_more": True}, {"items": [], "has_more": True, "page_token": "again"},
                     {"items": []}, {"items": [None], "has_more": False}):
            with self.subTest(page=page):
                self.fake.before = lambda request: self.fake.response(page)
                with self.assertRaises(cloud.LearningCloudError):
                    self.remote.fetch_questions()

    def test_empty_page_without_items_is_accepted_only_for_a_genuinely_empty_first_page(self):
        table = cloud.BANK_TABLES["written"]
        path = f"tables/{table}/records"
        self.fake.before = lambda request: self.fake.response({"has_more": False, "total": 0})
        self.assertEqual(self.remote._list_all(path), [])
        for page in ({"has_more": False, "total": 1}, {"has_more": True, "total": 0}, {"has_more": False}):
            self.fake.before = lambda request, page=page: self.fake.response(page)
            with self.subTest(page=page), self.assertRaises(cloud.LearningCloudError):
                self.remote._list_all(path)
        self.fake.records[table] = [{"record_id": f"rec{i}", "fields": {}} for i in range(3)]
        self.fake.before = lambda request: self.fake.response({"has_more": False, "total": 0}) if request.url.params.get("page_token") else None
        with self.assertRaises(cloud.LearningCloudError):
            self.remote._list_all(path)

    def test_upsert_roundtrips_full_document_and_updates_the_unique_key(self):
        payload = {"questions": [self.question(), self.question(id="immutable-q2")], "nested": {"answer": "原始快照"}}
        first = self.remote.upsert_entity("exam", 'key-"-中文', payload, "op1")
        payload["nested"]["extra"] = [1, 2, 3]
        second = self.remote.upsert_entity("exam", 'key-"-中文', payload, "op2")
        other = self.remote.upsert_entity("session", 'key-"-中文', {"other": True}, "op3")
        self.assertEqual(first["record_id"], second["record_id"])
        self.assertNotEqual(first["record_id"], other["record_id"])
        self.assertEqual(self.remote.load_entities(), [
            {"kind": "exam", "key": 'key-"-中文', "payload": payload},
            {"kind": "session", "key": 'key-"-中文', "payload": {"other": True}},
        ])

    def test_source_extensions_and_entity_attachment_references_survive(self):
        q = self.question(updated_at="2026-09-29", custom_extension={"keep": True}, _dirty=True)
        self.remote.save_question(q, "op-question")
        fields = self.fake.records[cloud.BANK_TABLES["written"]][0]["fields"]
        meta = json.loads(fields[cloud.META_FIELD])
        self.assertEqual(meta["custom_extension"], q["custom_extension"])
        self.assertEqual(meta["updated_at"], q["updated_at"])
        self.assertEqual(meta["source_answer"], fields["答案"])
        self.assertNotIn("_dirty", meta)
        payload = {"attachments": [{"id": "pending", "local_file": "pending.pdf"},
                                   {"id": "uploaded", "file_token": "fileUploaded", "name": "file.pdf"}]}
        record = self.remote.upsert_entity("issue", "issue1", payload, "op-issue")
        self.assertEqual(record["fields"]["附件"], [{"file_token": "fileUploaded"}])
        self.assertEqual(json.loads(record["fields"]["内容"]), payload)
        attachment = {"id": "uploaded", "file_token": "fileUploaded", "name": "file.pdf", "sha256": "abc"}
        record = self.remote.upsert_entity("attachment", "uploaded", attachment, "op-attachment")
        self.assertEqual(record["fields"]["附件"], [{"file_token": "fileUploaded"}])
        self.assertEqual(json.loads(record["fields"]["内容"]), attachment)

    def test_upsert_checks_all_pages_for_duplicates_before_writing(self):
        self.remote.ensure_schema()
        self.fake.records["tblEntities"] = [{"record_id": f"rec{i}", "fields": {"类别": "doc", "业务键": "key", "内容": "{}"}} for i in range(3)]
        self.fake.calls.clear()
        with self.assertRaisesRegex(cloud.LearningCloudError, "多条记录"):
            self.remote.upsert_entity("doc", "key", {}, "op")
        self.assertEqual(len(self.fake.calls), 2)
        self.assertFalse(self.fake.mutations())

    def test_entity_pagination_failure_cannot_turn_into_a_create(self):
        self.remote.ensure_schema()
        self.fake.calls.clear()
        def broken(request):
            if request.url.path.endswith("/search"):
                return self.fake.response({"items": [], "has_more": True})
        self.fake.before = broken
        with self.assertRaises(cloud.LearningCloudError):
            self.remote.upsert_entity("doc", "key", {}, "op")
        self.assertFalse(self.fake.mutations())

    def test_oversize_payload_is_explicit_and_never_truncated(self):
        payload = {"questions": [{"stem": "x" * cloud.MAX_DOCUMENT_BYTES}]}
        with self.assertRaisesRegex(ValueError, "未截断或写入"):
            self.remote.upsert_entity("doc", "key", payload, "op")
        with self.assertRaisesRegex(ValueError, "未截断或写入"):
            self.remote.save_question(self.question(analysis="中" * cloud.MAX_TEXT_LENGTH), "op")
        self.assertEqual(self.fake.calls, [])

    def test_lost_create_response_reuses_uuid4_token_even_after_restart(self):
        self.remote.ensure_schema()
        path = f"tables/{cloud.BANK_TABLES['written']}/records"
        def lose(request, response):
            if request.method == "POST" and request.url.path.endswith("/records"):
                raise httpx.ReadTimeout("lost response", request=request)
        self.fake.after = lose
        with self.assertRaises(cloud.LearningCloudWriteUncertain):
            self.remote.save_question(self.question(), "same-operation")
        creates = [c for c in self.fake.calls if c[1] == path and c[0] == "POST"]
        self.assertEqual(len(creates), 2)
        self.fake.after = None
        saved = self.new_remote().save_question(self.question(), "same-operation")
        creates = [c for c in self.fake.calls if c[1] == path and c[0] == "POST"]
        tokens = {c[2]["client_token"] for c in creates}
        self.assertEqual(len(tokens), 1)
        self.assertEqual(uuid.UUID(tokens.pop()).version, 4)
        self.assertEqual(saved["record_id"], "recNew1")
        self.assertEqual(len(self.fake.records[cloud.BANK_TABLES["written"]]), 1)

    def test_question_edited_after_lost_create_reuses_identity_and_saves_new_version(self):
        self.remote.ensure_schema()
        def lose(request, response):
            if request.method == "POST" and request.url.path.endswith("/records"):
                raise httpx.ReadTimeout("lost", request=request)
        self.fake.after = lose
        with self.assertRaises(cloud.LearningCloudWriteUncertain):
            self.remote.save_question(self.question(), "revision-1")
        self.fake.after = None
        latest = self.question(stem="再次修改的题目", version=3)
        result = self.new_remote().save_question(latest, "revision-2")
        table = cloud.BANK_TABLES["written"]
        self.assertEqual(len(self.fake.records[table]), 1)
        self.assertEqual(self.fake.records[table][0]["fields"]["题目"], latest["stem"])
        self.assertEqual(json.loads(self.fake.records[table][0]["fields"][cloud.META_FIELD])["version"], 3)
        self.assertEqual(result["id"], latest["id"])
        tokens = {c[2]["client_token"] for c in self.fake.calls if c[0] == "POST" and c[1] == f"tables/{table}/records"}
        self.assertEqual(len(tokens), 1)

    def test_entity_create_token_is_stable_across_revisions_and_stale_search(self):
        self.remote.upsert_entity("doc", "key", {"v": 1}, "revision-1")
        def stale_search(request):
            if request.url.path.endswith("/records/search"):
                return self.fake.response({"items": [], "has_more": False})
        self.fake.before = stale_search
        result = self.new_remote().upsert_entity("doc", "key", {"v": 2}, "revision-2")
        self.assertEqual(len(self.fake.records["tblEntities"]), 1)
        self.assertEqual(json.loads(result["fields"]["内容"]), {"v": 2})
        tokens = {c[2]["client_token"] for c in self.fake.calls if c[0] == "POST" and c[1] == "tables/tblEntities/records"}
        self.assertEqual(len(tokens), 1)

    def test_unknown_entity_result_is_found_on_retry_without_another_create(self):
        self.remote.ensure_schema()
        def lose(request, response):
            if request.url.path.endswith("/tblEntities/records") and request.method == "POST":
                raise httpx.ReadTimeout("lost", request=request)
        self.fake.after = lose
        with self.assertRaises(cloud.LearningCloudWriteUncertain):
            self.remote.upsert_entity("doc", "key", {"v": 1}, "op")
        self.fake.after = None
        self.fake.calls.clear()
        self.remote.upsert_entity("doc", "key", {"v": 1}, "op")
        self.assertEqual(len(self.fake.records["tblEntities"]), 1)
        self.assertEqual([c[0] for c in self.fake.mutations()], ["PUT"])

    def test_missing_write_record_is_unknown_not_success(self):
        self.remote.ensure_schema()
        self.fake.after = lambda request, response: self.fake.response({}) if request.method == "POST" and request.url.path.endswith("/records") else None
        with self.assertRaises(cloud.LearningCloudWriteUncertain):
            self.remote.save_question(self.question(), "op")

    def test_lost_table_response_is_reconciled_by_name(self):
        def lose(request, response):
            if request.method == "POST" and request.url.path.endswith("/tables"):
                raise httpx.ReadTimeout("lost", request=request)
        self.fake.after = lose
        with self.assertRaises(cloud.LearningCloudWriteUncertain):
            self.remote.ensure_schema()
        self.fake.after = None
        self.remote.ensure_schema()
        self.assertEqual(len([c for c in self.fake.mutations() if c[1] == "tables"]), 1)

    def test_unknown_table_create_is_not_blindly_retried(self):
        def lose(request):
            if request.method == "POST" and request.url.path.endswith("/tables"):
                raise httpx.ReadTimeout("unknown", request=request)
        self.fake.before = lose
        with self.assertRaises(cloud.LearningCloudWriteUncertain):
            self.remote.ensure_schema()
        self.fake.before = None
        with self.assertRaises(cloud.LearningCloudWriteUncertain):
            self.remote.ensure_schema()
        self.assertEqual(len([c for c in self.fake.mutations() if c[1] == "tables"]), 1)

    def test_schema_duplicate_table_and_pagination_failure_never_mutate(self):
        self.fake.tables.extend([{"table_id": f"tblDuplicate{i}", "name": cloud.ENTITY_TABLE_NAME} for i in range(2)])
        with self.assertRaisesRegex(cloud.LearningCloudError, "同名表"):
            self.remote.ensure_schema()
        def broken(request):
            if request.url.params.get("page_token"):
                return self.fake.response({"items": [], "has_more": True})
        self.fake.before = broken
        with self.assertRaises(cloud.LearningCloudError):
            self.remote.ensure_schema()
        self.assertFalse(self.fake.mutations())

    def test_field_create_lost_response_is_idempotent(self):
        def lose(request, response):
            if request.method == "POST" and request.url.path.endswith("/fields"):
                raise httpx.ReadTimeout("lost", request=request)
        self.fake.after = lose
        with self.assertRaises(cloud.LearningCloudWriteUncertain):
            self.remote.ensure_schema()
        self.fake.after = None
        self.new_remote().ensure_schema()
        for fields in self.fake.fields.values():
            names = [f["field_name"] for f in fields]
            self.assertEqual(len(names), len(set(names)))

    def test_entity_load_second_page_failure_never_returns_partial_documents(self):
        self.remote.ensure_schema()
        self.fake.records["tblEntities"] = [{"record_id": f"rec{i}", "fields": {
            "类别": "doc", "业务键": f"key{i}", "内容": "{}",
        }} for i in range(3)]
        def broken(request):
            if request.url.path.endswith("/tblEntities/records") and request.url.params.get("page_token"):
                return httpx.Response(200, json={"code": 1254002, "msg": "permission denied"})
        self.fake.before = broken
        with self.assertRaises(cloud.LearningCloudError):
            self.remote.load_entities()

    def test_unknown_upload_has_no_automatic_retry_and_invalid_response_is_unknown(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "data.txt"
            path.write_bytes(b"data")
            def lose(request, response):
                raise httpx.ReadTimeout("lost upload", request=request)
            self.fake.after = lose
            with self.assertRaises(cloud.LearningCloudWriteUncertain):
                self.remote.upload_attachment(path, "data.txt")
            self.assertEqual(len(self.fake.calls), 1)
            self.fake.after = lambda request, response: self.fake.response({})
            with self.assertRaises(cloud.LearningCloudWriteUncertain):
                self.remote.upload_attachment(path, "data.txt")

    def test_default_client_has_finite_timeouts_and_is_disabled(self):
        remote = cloud.LearningCloud()
        self.addCleanup(remote.close)
        self.assertFalse(remote.enabled)
        timeout = remote._http.timeout
        self.assertTrue(all(0 < t <= 60 for t in (timeout.connect, timeout.read, timeout.write, timeout.pool)))
        self.assertLessEqual(remote._http.retries, 1)

    def test_load_without_entity_table_is_read_only_and_rich_text_decodes(self):
        remote = self.new_remote(enabled=False)
        self.assertEqual(remote.load_entities(), [])
        self.assertFalse(self.fake.mutations())
        self.remote.ensure_schema()
        self.fake.records["tblEntities"] = [{"record_id": "rec1", "fields": {
            "类别": [{"text": "doc"}], "业务键": [{"text": "key"}], "内容": [{"text": '{"v":'}, {"text": "1}"}],
        }}]
        self.assertEqual(remote.load_entities(), [{"kind": "doc", "key": "key", "payload": {"v": 1}}])
        self.fake.records["tblEntities"].append({"record_id": "rec2", "fields": {"类别": "doc", "业务键": "bad", "内容": "[broken"}})
        with self.assertRaises(cloud.LearningCloudError):
            remote.load_entities()

    def test_token_refresh_is_bounded_and_remote_messages_are_not_exposed(self):
        count = 0
        def expired(request):
            nonlocal count
            count += 1
            if count <= 2:
                return httpx.Response(200, json={"code": 99991663, "msg": "private-token"})
        self.fake.before = expired
        with self.assertRaises(cloud.LearningCloudError) as error:
            self.remote.fetch_questions()
        self.assertNotIn("private-token", str(error.exception))
        self.assertEqual(count, 2)
        self.manager.get_tenant_token.assert_called_with(force_refresh=True)

    def test_upload_and_download_are_official_only_and_enforce_download_bound(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "data.txt"
            path.write_bytes(b"attachment")
            self.assertEqual(self.remote.upload_attachment(path, "资料.txt"),
                             {"file_token": "fileUploaded", "name": "资料.txt", "size": 10})
        self.assertEqual(self.remote.download_attachment("fileUploaded", 10), b"attachment")
        with self.assertRaises(cloud.LearningCloudError):
            self.remote.download_attachment("fileUploaded", 9)
        count = len(self.fake.calls)
        for token in ("https://evil.example/file", "../other", "file?token=x", "file%2fother"):
            with self.assertRaises(ValueError):
                self.remote.download_attachment(token, 10)
        with self.assertRaises(ValueError):
            self.remote.download_attachment("file", 0)
        self.assertEqual(len(self.fake.calls), count)
        self.assertTrue(all(c[3].url.host == "open.feishu.cn" for c in self.fake.calls))

    def test_download_does_not_follow_redirects(self):
        self.fake.before = lambda request: httpx.Response(302, headers={"location": "https://evil.example/file"})
        with self.assertRaises(cloud.LearningCloudError):
            self.remote.download_attachment("fileToken", 100)
        self.assertTrue(all(c[3].url.host == "open.feishu.cn" for c in self.fake.calls))

    def test_close_releases_shared_http_client(self):
        self.remote.fetch_questions()
        client = self.remote._http._client
        self.remote.close()
        self.remote.close()
        self.assertTrue(client.is_closed)

    def large_payload(self):
        return {"notes": "面试复习笔记\n" * 20_001, "questions": [self.question()],
                "history": [{"answer": "保持完整", "attempt": 1}]}

    def seed_document(self, raw, **overrides):
        self.remote.ensure_schema()
        pointer = {"schema_marker": cloud.DOCUMENT_SCHEMA, "version": 1, "file_token": "fileDocument",
                   "sha256": hashlib.sha256(raw).hexdigest(), "raw_bytes": len(raw), **overrides}
        self.fake.media["fileDocument"] = gzip.compress(raw, mtime=0)
        self.fake.records["tblEntities"] = [{"record_id": "recDocument", "fields": {
            "类别": "record", "业务键": "paper1", "内容": json.dumps(pointer),
            "附件": [{"file_token": "fileDocument"}],
        }}]
        self.fake.calls.clear()
        return pointer

    def test_large_document_roundtrip_is_complete_read_only_and_cleans_temporary_file(self):
        payload = self.large_payload()
        with patch.object(self.remote, "upload_attachment", wraps=self.remote.upload_attachment) as upload:
            result = self.remote.upsert_entity("record", "paper1", payload, "op1")
        path = upload.call_args.args[0]
        self.assertFalse(path.exists())
        self.assertFalse(path.parent.exists())
        pointer = json.loads(result["fields"]["内容"])
        self.assertEqual(pointer["schema_marker"], cloud.DOCUMENT_SCHEMA)
        self.assertEqual(pointer["version"], 1)
        self.assertLess(len(result["fields"]["内容"]), cloud.MAX_TEXT_LENGTH)
        raw = gzip.decompress(self.fake.media[pointer["file_token"]])
        self.assertEqual(pointer["sha256"], hashlib.sha256(raw).hexdigest())
        self.assertEqual(pointer["raw_bytes"], len(raw))
        self.assertEqual(json.loads(raw), payload)
        self.assertEqual(result["fields"]["附件"], [{"file_token": pointer["file_token"]}])
        self.fake.calls.clear()
        self.assertEqual(self.new_remote(enabled=False).load_entities(), [{"kind": "record", "key": "paper1", "payload": payload}])
        self.assertFalse(self.fake.mutations())
        downloads = [c for c in self.fake.calls if c[1].endswith("/download")]
        self.assertEqual(len(downloads), 1)
        self.assertEqual(downloads[0][3].url.host, "open.feishu.cn")

    def test_small_document_stays_inline_without_upload_or_download(self):
        payload = {"notes": "小文档", "history": [1, 2, 3]}
        result = self.remote.upsert_entity("record", "paper1", payload, "op1")
        self.assertEqual(json.loads(result["fields"]["内容"]), payload)
        self.assertEqual(self.remote.load_entities()[0]["payload"], payload)
        self.assertFalse(self.fake.media)
        self.assertFalse(any("/medias/" in c[1] for c in self.fake.calls))

    def test_large_document_preserves_and_deduplicates_business_attachments(self):
        original = {"attachments": [{"file_token": "fileLegacy", "name": "legacy.pdf"}]}
        self.remote.upsert_entity("record", "paper1", original, "op1")
        payload = self.large_payload()
        payload["attachments"] = [{"file_token": "fileBusiness", "name": "notes.pdf"},
                                  {"file_token": "fileBusiness", "name": "notes-copy.pdf"}]
        first = self.remote.upsert_entity("record", "paper1", payload, "op2")
        pointer = json.loads(first["fields"]["内容"])
        self.assertEqual(first["fields"]["附件"], [{"file_token": "fileLegacy"},
                         {"file_token": "fileBusiness"}, {"file_token": pointer["file_token"]}])
        again = self.new_remote().upsert_entity("record", "paper1", payload, "op3")
        self.assertEqual(again["fields"]["附件"], first["fields"]["附件"])
        self.assertEqual(len(self.fake.media), 1)
        self.assertEqual(self.remote.load_entities()[0]["payload"], payload)

    def test_large_to_small_document_keeps_business_files_and_removes_document_reference(self):
        payload = self.large_payload()
        payload["attachments"] = [{"file_token": "fileBusiness"}]
        self.remote.upsert_entity("record", "paper1", payload, "op1")
        result = self.remote.upsert_entity("record", "paper1", {"notes": "small"}, "op2")
        self.assertEqual(json.loads(result["fields"]["内容"]), {"notes": "small"})
        self.assertEqual(result["fields"]["附件"], [{"file_token": "fileBusiness"}])
        self.assertEqual(len(self.fake.media), 1)

    def test_document_hash_and_byte_count_fail_closed(self):
        for overrides, message in (({"sha256": "0" * 64}, "SHA256"), ({"raw_bytes": 99}, "字节数")):
            with self.subTest(overrides=overrides):
                self.seed_document(b'{"valid":true}', **overrides)
                with self.assertRaisesRegex(cloud.LearningCloudError, message):
                    self.remote.load_entities()

    def test_document_gzip_bomb_stops_at_declared_byte_bound(self):
        self.assertEqual(cloud.MAX_DOCUMENT_BYTES, 20 * 1024 * 1024)
        self.seed_document(b"x" * (cloud.MAX_DOCUMENT_BYTES + 1), raw_bytes=cloud.MAX_DOCUMENT_BYTES)
        with self.assertRaisesRegex(cloud.LearningCloudError, "字节数"):
            self.remote.load_entities()
        self.seed_document(b"x" * 200_000, raw_bytes=10)
        with self.assertRaisesRegex(cloud.LearningCloudError, "字节数"):
            self.remote.load_entities()

    def test_document_pointer_rejects_urls_bad_versions_and_oversize_before_downloading(self):
        for overrides in ({"file_token": "https://evil.example/file"}, {"file_token": "../file"},
                          {"version": 2}, {"version": True}, {"raw_bytes": cloud.MAX_DOCUMENT_BYTES + 1},
                          {"raw_bytes": -1}, {"raw_bytes": True}, {"sha256": "bad"}):
            with self.subTest(overrides=overrides):
                self.seed_document(b"{}", **overrides)
                with self.assertRaises(cloud.LearningCloudError):
                    self.remote.load_entities()
                self.assertFalse(any("/medias/" in c[1] for c in self.fake.calls))

    def test_document_rejects_corrupt_gzip_invalid_json_and_invalid_utf8(self):
        for data in (b"not-json", b'"\xff"'):
            self.seed_document(data)
            with self.assertRaisesRegex(cloud.LearningCloudError, "UTF-8 JSON"):
                self.remote.load_entities()
        for compressed in (b"not-gzip", gzip.compress(b"{}")[:-4]):
            self.seed_document(b"{}")
            self.fake.media["fileDocument"] = compressed
            with self.assertRaisesRegex(cloud.LearningCloudError, "损坏或不完整"):
                self.remote.load_entities()

    def test_document_download_failure_never_returns_partial_entities(self):
        self.seed_document(b"{}")
        self.fake.records["tblEntities"].insert(0, {"record_id": "recInline", "fields": {
            "类别": "record", "业务键": "first", "内容": "{}",
        }})
        def fail(request):
            if request.url.path.endswith("/download"):
                raise httpx.ReadTimeout("offline", request=request)
        self.fake.before = fail
        with self.assertRaises(cloud.LearningCloudError):
            self.remote.load_entities()

    def test_large_document_reuses_cloud_pointer_after_lost_write_and_restart(self):
        payload = self.large_payload()
        def lose(request, response):
            if request.method == "POST" and request.url.path.endswith("/records"):
                raise httpx.ReadTimeout("lost response", request=request)
        self.fake.after = lose
        with self.assertRaises(cloud.LearningCloudWriteUncertain):
            self.remote.upsert_entity("record", "paper1", payload, "revision-1")
        pointer = json.loads(self.fake.records["tblEntities"][0]["fields"]["内容"])
        self.fake.after = None
        self.fake.calls.clear()
        result = self.new_remote().upsert_entity("record", "paper1", payload, "revision-2")
        self.assertEqual(json.loads(result["fields"]["内容"]), pointer)
        self.assertEqual(len(self.fake.records["tblEntities"]), 1)
        self.assertEqual(len(self.fake.media), 1)
        self.assertFalse(any("upload_all" in c[1] for c in self.fake.calls))
        self.assertEqual(self.remote.load_entities()[0]["payload"], payload)

    def test_large_document_reuses_upload_if_record_never_committed(self):
        payload = self.large_payload()
        def fail(request):
            if request.method == "POST" and request.url.path.endswith("/records"):
                raise httpx.ReadTimeout("before commit", request=request)
        self.fake.before = fail
        with self.assertRaises(cloud.LearningCloudWriteUncertain):
            self.remote.upsert_entity("record", "paper1", payload, "revision-1")
        self.assertEqual(self.fake.records["tblEntities"], [])
        self.assertEqual(len(self.fake.media), 1)
        self.fake.before = None
        self.remote.upsert_entity("record", "paper1", payload, "revision-2")
        self.assertEqual(len(self.fake.media), 1)
        self.assertFalse(self.remote._pending_documents)

    def test_large_document_upload_failure_cleans_temp_and_does_not_write_record(self):
        def fail(request):
            if request.url.path.endswith("/upload_all"):
                raise httpx.ReadTimeout("upload failed", request=request)
        self.fake.before = fail
        with patch.object(self.remote, "upload_attachment", wraps=self.remote.upload_attachment) as upload:
            with self.assertRaises(cloud.LearningCloudWriteUncertain):
                self.remote.upsert_entity("record", "paper1", self.large_payload(), "op1")
        self.assertFalse(upload.call_args.args[0].parent.exists())
        self.assertEqual(self.fake.records["tblEntities"], [])
        self.assertEqual(len([c for c in self.fake.calls if c[1].endswith("/upload_all")]), 1)

    def test_entity_byte_limit_counts_utf8_and_marker_is_reserved(self):
        with patch.object(cloud, "MAX_DOCUMENT_BYTES", 100):
            with self.assertRaisesRegex(ValueError, "20MiB"):
                self.remote.upsert_entity("record", "paper1", {"notes": "中" * 40}, "op1")
        with self.assertRaisesRegex(ValueError, "schema_marker"):
            self.remote.upsert_entity("record", "paper1", {"schema_marker": cloud.DOCUMENT_SCHEMA}, "op2")
        self.assertEqual(self.fake.calls, [])


if __name__ == "__main__":
    unittest.main()
