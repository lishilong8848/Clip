"""Isolated shift storage tests; every request is served by MockTransport."""
import copy
import gzip
import hashlib
import json
import unittest
from unittest.mock import patch

import httpx

from bin import test_learning_cloud as support
from lan_bitable_template_portal import learning_cloud as cloud
from lan_bitable_template_portal.learning_completion import DISPLAY_FIELDS


class ShiftFeishu(support.FakeFeishu):
    def dispatch(self, request, path):
        parts = path.split("/")
        if request.method == "GET" and len(parts) == 4 and parts[2] == "records":
            record = next((r for r in self.records[parts[1]] if r["record_id"] == parts[3]), None)
            return self.response({"record": record})
        return super().dispatch(request, path)


class ShiftCloudTests(unittest.TestCase):
    new_remote = support.LearningCloudTests.new_remote

    def setUp(self):
        support.LearningCloudTests.setUp(self)
        self.fake = ShiftFeishu()
        self.table = "tblOriginal"
        self.fake.tables.append({"table_id": self.table, "name": cloud.ENTITY_TABLE_NAME})
        self.fake.fields[self.table] = [{"field_name": n, "type": t} for n, t in {
            **cloud.ENTITY_FIELDS, **DISPLAY_FIELDS, "人员": 11,
        }.items()]
        for table in cloud.BANK_TABLES.values():
            self.fake.fields[table].extend({"field_name": n, "type": t} for n, t in cloud.EXTRA_FIELDS.items())
        self.fake.records[self.table] = []
        self.remote = self.new_remote()

    def bundle(self, **changes):
        value = {"schema_version": 1, "date": "2026-10-21", "shift": "白", "person_id": "stable-person",
                 "entities": [{"kind": "record", "key": "paper-1", "payload": {
                     "id": "paper-1", "entries": {"q1": {"attempt": {"correct": True}}},
                 }}], "summary": {"status": "待核对", "answered": 1}}
        value.update(changes)
        if isinstance(value["summary"], dict):
            value["summary"] = {**{name: value[name] for name in ("date", "shift", "person_id")}, **value["summary"]}
        return value

    def display(self, **changes):
        values = {name: "" if kind == 1 else None for name, kind in DISPLAY_FIELDS.items()}
        values.update({"日期": 1792512000000, "人员标识": "stable-person", "班次": "白",
                       "完成状态": "待核对", "应答题数": 1, "已答题数": 1, "人员": []})
        values.update(changes)
        return values

    def row(self, kind, key, payload, *, fields=None, compressed=False):
        content = json.dumps(payload, ensure_ascii=False)
        refs = []
        if compressed:
            raw = content.encode("utf-8")
            token = "fileDocument" + str(len(self.fake.media))
            self.fake.media[token] = gzip.compress(raw)
            content = json.dumps({"schema_marker": cloud.DOCUMENT_SCHEMA, "version": 1,
                                  "raw_bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest(),
                                  "file_token": token})
            refs = [{"file_token": token}]
        rows = self.fake.records[self.table]
        row = {"record_id": f"recOriginal{len(rows) + 1}", "fields": {
            "类别": kind, "业务键": key, "内容": content, "附件": refs, **(fields or {}),
        }}
        rows.append(row)
        return row

    def upsert(self, value=None, **kwargs):
        return self.remote.upsert_entity("shift_person", "班次人员:2026-10-21:白:stable-person",
                                         self.bundle() if value is None else value, "operation", **kwargs)

    def test_capability_and_all_existing_entity_kind_labels(self):
        self.assertTrue(cloud.LearningCloud.shift_storage_enabled)
        kinds = {"person", "reserve", "paper", "record", "issue", "settings", "publication", "notification",
                 "engagement", "audit", "attachment", "question", "sharing_batch", "sharing_attempt",
                 "sharing_delivery", "daily_progress", "shift_person"}
        self.assertTrue(kinds <= cloud.ENTITY_KIND_LABELS.keys())
        self.assertEqual(len(cloud.ENTITY_KIND_LABELS), len(set(cloud.ENTITY_KIND_LABELS.values())))
        for kind in kinds:
            self.assertRegex(cloud.ENTITY_KIND_LABELS[kind], r"[\u4e00-\u9fff]")
            self.assertEqual(cloud.ENTITY_KIND_ALIASES[cloud.ENTITY_KIND_LABELS[kind]], kind)

    def test_legacy_english_and_new_chinese_queries_keep_id_and_business_key(self):
        for kind in ("record", "settings", "sharing_delivery"):
            key = 'original-key-"-' + kind
            row = self.row(kind, key, {"old": True})
            original_id = row["record_id"]
            for _ in range(2):
                saved = self.remote.upsert_entity(kind, key, {"new": True}, "op", display_fields={"姓名": "测试"})
                self.assertEqual(saved["record_id"], original_id)
                self.assertEqual(saved["fields"]["业务键"], key)
                self.assertEqual(saved["fields"]["类别"], cloud.ENTITY_KIND_LABELS[kind])
        self.assertEqual(len(self.fake.records[self.table]), 3)
        queries = [json.loads(c[3].content)["filter"]["conditions"][0]["value"][0]
                   for c in self.fake.calls if c[1].endswith("/search")]
        self.assertIn("record", queries)
        self.assertIn(cloud.ENTITY_KIND_LABELS["record"], queries)
        self.assertTrue(all(c[0] == "PUT" for c in self.fake.mutations()))

    def test_english_and_chinese_duplicate_rows_are_rejected_before_write(self):
        self.row("record", "same", {})
        self.row(cloud.ENTITY_KIND_LABELS["record"], "same", {})
        with self.assertRaisesRegex(cloud.LearningCloudError, "多条"):
            self.remote.upsert_entity("record", "same", {}, "op", display_fields={})
        with self.assertRaisesRegex(cloud.LearningCloudError, "重复"):
            self.remote.load_entities()
        self.assertFalse(self.fake.mutations())

    def test_search_failure_or_wrong_alias_never_creates(self):
        for wrong_alias in (False, True):
            with self.subTest(wrong_alias=wrong_alias):
                def broken(request):
                    if request.url.path.endswith("/search"):
                        alias = json.loads(request.content)["filter"]["conditions"][0]["value"][0]
                        if alias == cloud.ENTITY_KIND_LABELS["record"]:
                            if wrong_alias:
                                return self.fake.response({"items": [{"record_id": "recMismatch", "fields": {
                                    "类别": "settings", "业务键": "same"}}], "has_more": False})
                            raise httpx.ReadTimeout("offline", request=request)
                self.fake.before = broken
                with self.assertRaises(cloud.LearningCloudError):
                    self.remote.upsert_entity("record", "same", {}, "op", display_fields={})
                self.assertFalse(self.fake.mutations())

    def test_bundle_roundtrip_overrides_legacy_in_both_orders_and_returns_owner(self):
        value = self.bundle()
        value["entities"].append({"kind": "daily_progress", "key": "daily:2026-10-21:stable-person",
                                  "payload": {"id": "daily:2026-10-21:stable-person", "tasks": [{"source": "original"}]}})
        original = copy.deepcopy(value)
        for reverse in (False, True):
            with self.subTest(reverse=reverse):
                self.fake.records[self.table] = []
                self.row("record", "paper-1", {"old": True})
                self.row("settings", "main", {"enabled": True})
                self.row(cloud.ENTITY_KIND_LABELS["shift_person"], "班次人员:2026-10-21:白:stable-person", value, compressed=True)
                if reverse:
                    self.fake.records[self.table].reverse()
                restored = self.remote.load_entities()
                by_kind = {r["kind"]: r for r in restored}
                self.assertEqual(len(restored), 3)
                self.assertNotIn("storage_owner", by_kind["settings"])
                for entity in value["entities"]:
                    self.assertEqual(by_kind[entity["kind"]], {**entity, "storage_owner": ["2026-10-21", "白", "stable-person"]})
                self.assertEqual(value, original)
                self.assertFalse(self.fake.mutations())

    def test_list_records_decompresses_without_flattening_or_writing(self):
        original = self.row(cloud.ENTITY_KIND_LABELS["record"], "original-key", {"data": "原样"}, fields={'姓名': [{'text': '原姓名'}]}, compressed=True)
        bundle = self.row("shift_person", "班次人员:2026-10-21:白:stable-person", self.bundle(), compressed=True)
        rows = self.new_remote(enabled=False).list_entity_records()
        self.assertEqual(rows[0], {"record_id": original["record_id"], "kind": "record", "key": "original-key",
                                  "payload": {"data": "原样"}, "display_name": "原姓名",
                                  "payload_sha256": hashlib.sha256('{"data":"原样"}'.encode("utf-8")).hexdigest()})
        self.assertEqual(rows[1]["record_id"], bundle["record_id"])
        self.assertEqual(rows[1]["payload"], self.bundle())
        self.assertEqual(rows[1]["kind"], "shift_person")
        self.assertFalse(self.fake.mutations())

    def test_enrich_rechecks_original_name_before_person_field_write(self):
        original = self.row('paper', 'key', {}, fields={'姓名': '原姓名', '人员': [{'id': 'ou_existing'}]})
        before = copy.deepcopy(original)
        with self.assertRaisesRegex(cloud.LearningCloudConflict, '姓名已变化'):
            self.remote.enrich_entity_record(original['record_id'], 'paper', 'key',
                {'人员': [{'id': 'ou_wrong'}]}, expected_display_name='过期姓名')
        self.assertFalse(self.fake.mutations())
        self.assertEqual(original, before)
        self.remote.enrich_entity_record(original['record_id'], 'paper', 'key',
            {'姓名': '原姓名'}, expected_display_name='原姓名')
        self.assertEqual(original['fields']['人员'], [{'id': 'ou_existing'}], 'Omitting unresolved personnel must not clear it')
        self.assertEqual(original['fields']['内容'], before['fields']['内容'])


    def test_invalid_bundle_shapes_fail_read_and_write_without_partial_data(self):
        variants = [None, [], {}, self.bundle(schema_version=True), self.bundle(schema_version=2),
                    self.bundle(date="2026-02-30"), self.bundle(date="20261021"), self.bundle(date=None),
                    self.bundle(shift=""), self.bundle(shift="白班"), self.bundle(shift=[]),
                    self.bundle(person_id=" "), self.bundle(person_id=None), self.bundle(summary=[]),
                    self.bundle(summary={"invalid": float("nan")}), self.bundle(summary={"person_id": "other"}),
                    self.bundle(summary={"date": "2026-10-20"}), self.bundle(summary={"shift": "夜"}),
                    self.bundle(entities={}), self.bundle(entities=[None]), self.bundle(entities=[{}]),
                    self.bundle(entities=[{"kind": "record", "key": "k", "payload": []}]),
                    self.bundle(entities=[{"kind": "record", "key": " ", "payload": {}}]),
                    self.bundle(entities=[{"kind": "shift_person", "key": "k", "payload": {}}]),
                    self.bundle(entities=[{"kind": "settings", "key": "k", "payload": {}}]),
                    self.bundle(entities=[{"kind": "person", "key": "k", "payload": {}}]),
                    self.bundle(entities=[{"kind": "attachment", "key": "k", "payload": {}}]),
                    self.bundle(entities=[{"kind": "record", "key": "k", "payload": {"person_id": "other"}}]),
                    self.bundle(entities=[{"kind": "record", "key": "k", "payload": {"person_id": None}}]),
                    self.bundle(entities=[{"kind": "record", "key": "k", "payload": {}},
                                          {"kind": cloud.ENTITY_KIND_LABELS["record"], "key": "k", "payload": {}}])]
        for value in variants:
            with self.subTest(value=value):
                self.fake.records[self.table] = []
                self.row("settings", "main", {})
                self.row("shift_person", "bundle", value)
                with self.assertRaises(cloud.LearningCloudError):
                    self.remote.load_entities()
                with self.assertRaises(cloud.LearningCloudError):
                    self.remote.upsert_entity("shift_person", "bundle", value, "op", display_fields=self.display())
                self.assertFalse(self.fake.mutations())

    def test_cross_bundle_inner_duplicates_fail_even_if_payloads_match(self):
        for changed in (False, True):
            with self.subTest(changed=changed):
                self.fake.records[self.table] = []
                self.row("shift_person", "first", self.bundle())
                other = self.bundle(shift="夜")
                if changed:
                    other["entities"][0]["payload"]["changed"] = True
                self.row(cloud.ENTITY_KIND_LABELS["shift_person"], "second", other)
                with self.assertRaisesRegex(cloud.LearningCloudError, "冲突"):
                    self.remote.load_entities()
        self.assertFalse(self.fake.mutations())

    def test_summary_only_row_is_valid_and_restores_no_inner_entities(self):
        value = self.bundle(entities=[])
        self.upsert(value, display_fields=self.display())
        self.assertEqual(self.remote.load_entities(), [])
        self.assertEqual(self.remote.list_entity_records()[0]["payload"], value)
        self.row("shift_person", "other-shift", self.bundle(shift="夜"))
        restored = self.remote.load_entities()
        self.assertEqual(len(restored), 1)
        self.assertEqual(restored[0]["storage_owner"], ["2026-10-21", "夜", "stable-person"])

    def test_duplicate_group_with_different_keys_is_rejected(self):
        self.row("shift_person", "first", self.bundle())
        self.row("shift_person", "second", self.bundle(entities=[{"kind": "record", "key": "another", "payload": {}}]))
        with self.assertRaisesRegex(cloud.LearningCloudError, "多个聚合"):
            self.remote.load_entities()

    def test_late_corruption_and_bad_compressed_document_return_no_partial_list(self):
        self.row("settings", "main", {})
        row = self.row("shift_person", "bundle", self.bundle(), compressed=True)
        for content in ('{"incomplete"', json.dumps({**json.loads(row["fields"]["内容"]), "sha256": "0" * 64})):
            row["fields"]["内容"] = content
            with self.assertRaises(cloud.LearningCloudError):
                self.remote.load_entities()
            with self.assertRaises(cloud.LearningCloudError):
                self.remote.list_entity_records()
        self.assertFalse(self.fake.mutations())

    def test_missing_person_column_keeps_old_backup_and_daily_progress_working(self):
        self.fake.fields[self.table] = [f for f in self.fake.fields[self.table] if f["field_name"] != "人员"]
        status = self.remote.ensure_completion_schema()
        self.assertEqual(status, {"table_id": self.table, "ready": False, "missing": ["人员"]})
        self.assertTrue(self.remote.completion_schema_ready)
        self.assertFalse(self.fake.mutations())
        self.remote.upsert_entity("settings", "main", {"enabled": True}, "op")
        with patch("lan_bitable_template_portal.learning_completion.display_fields", return_value={"姓名": "测试"}):
            self.remote.upsert_entity("daily_progress", "daily", {"id": "daily"}, "op")
        self.fake.calls.clear()
        with self.assertRaisesRegex(cloud.LearningCloudError, "人员.*pending"):
            self.upsert(display_fields=self.display())
        self.assertFalse(self.fake.mutations())
        self.assertEqual(len(self.remote.load_entities()), 2)

    def test_person_type_is_read_checked_and_never_implicitly_created(self):
        for field in self.fake.fields[self.table]:
            if field["field_name"] == "人员":
                field["type"] = 1
        with self.assertRaisesRegex(cloud.LearningCloudError, "类型错误"):
            self.remote.ensure_completion_schema()
        with self.assertRaisesRegex(cloud.LearningCloudError, "人员.*pending"):
            self.upsert(display_fields=self.display())
        self.assertFalse(self.fake.mutations())

    def test_absent_original_table_does_not_create_or_migrate(self):
        self.fake.tables = [t for t in self.fake.tables if t["table_id"] != self.table]
        with self.assertRaisesRegex(cloud.LearningCloudError, "原表.*pending"):
            self.upsert(display_fields=self.display())
        with self.assertRaises(cloud.LearningCloudError):
            self.remote.enrich_entity_record("recMissing", "record", "k", {})
        self.assertEqual(self.remote.list_entity_records(), [])
        self.assertFalse(self.fake.mutations())

    def test_person_extra_receipt_fields_and_blank_numbers_are_normalized(self):
        def decorate(request, response):
            if request.method in ("PUT", "POST") and "/records" in request.url.path and not request.url.path.endswith("/search"):
                data = response.json()["data"]
                fields = data["record"]["fields"]
                fields["人员"] = [{"id": "ou_verified", "name": "测试人员", "email": "offline@example.invalid"}]
                for name, value in list(fields.items()):
                    if value == "":
                        fields[name] = None
                return self.fake.response(data)
        self.fake.after = decorate
        display = self.display(人员=[{"id": "ou_verified", "name": "仅显示"}], 首次客观正确率=" ")
        original = copy.deepcopy(display)
        self.upsert(display_fields=display)
        self.upsert(display_fields=display)
        self.assertEqual(display, original)
        writes = [call for call in self.fake.mutations() if "/records" in call[1]]
        self.assertEqual([c[0] for c in writes], ["POST", "PUT"])
        for call in writes:
            self.assertEqual(call[2]["user_id_type"], "open_id")
            fields = json.loads(call[3].content)["fields"]
            self.assertEqual(fields["人员"], [{"id": "ou_verified"}])
            self.assertIsNone(fields["首次客观正确率"])
        self.assertEqual(len(self.fake.records[self.table]), 1)

    def test_unverified_person_can_be_empty_without_fabrication(self):
        saved = self.upsert(display_fields=self.display())
        self.assertEqual(saved["fields"]["人员"], [])
        self.assertEqual(saved["fields"]["完成状态"], "待核对")
        self.assertEqual(self.remote.list_entity_records()[0]["payload"], self.bundle())

    def test_short_bundle_uses_chinese_pointer_and_preserves_complete_payload(self):
        value = self.bundle()
        original = copy.deepcopy(value)
        saved = self.upsert(value, display_fields=self.display(姓名="测试人员"))
        visible = json.loads(saved["fields"]["内容"])
        self.assertEqual(set(visible), {"记录说明", "完整明细"})
        self.assertEqual(visible["记录说明"], "2026-10-21 白 测试人员")
        self.assertEqual(set(visible["完整明细"]), {"附件标识", "原始字节数", "内容校验", "版本"})
        pointer = cloud._document_pointer(visible)
        raw = gzip.decompress(self.fake.media[pointer["file_token"]])
        self.assertEqual(json.loads(raw), original)
        self.assertEqual(len(raw), pointer["raw_bytes"])
        self.assertEqual(hashlib.sha256(raw).hexdigest(), pointer["sha256"])
        self.assertEqual(value, original)
        upload = next(call for call in self.fake.calls if "upload_all" in call[1])
        self.assertIn(f"学练明细-{pointer['sha256']}.json.gz".encode("utf-8"), upload[3].content)
        restored = self.remote.list_entity_records()[0]
        self.assertEqual(restored["payload"], original)
        self.remote.enrich_entity_record(saved["record_id"], "shift_person", saved["fields"]["业务键"],
                                         self.display(姓名="新名称"), expected_payload_sha256=restored["payload_sha256"])
        self.assertEqual(json.loads(self.fake.records[self.table][0]["fields"]["内容"]), visible)
        self.assertEqual(len(self.fake.media), 1)

    def test_chinese_pointer_damage_uses_existing_integrity_and_size_checks(self):
        self.upsert(display_fields=self.display())
        row = self.fake.records[self.table][0]
        original = json.loads(row["fields"]["内容"])
        for name, value in (("版本", True), ("版本", 2), ("原始字节数", cloud.MAX_DOCUMENT_BYTES + 1),
                            ("附件标识", "https://invalid.example/file"), ("内容校验", "0" * 64),
                            ("原始字节数", original["完整明细"]["原始字节数"] + 1)):
            with self.subTest(name=name, value=value):
                changed = copy.deepcopy(original)
                changed["完整明细"][name] = value
                row["fields"]["内容"] = json.dumps(changed)
                with self.assertRaises(cloud.LearningCloudError):
                    self.remote.load_entities()

    def test_bundle_inner_attachments_remain_linked_and_payload_unchanged(self):
        value = self.bundle(entities=[
            {"kind": "paper", "key": "paper1", "payload": {"attachments": [{"file_token": "fileOriginal", "name": "资料.pdf"}]}},
            {"kind": "record", "key": "paper1", "payload": {"attachments": [
                {"file_token": "fileOriginal", "name": "资料.pdf"}, {"local_file": "pending.pdf"}]}},
        ])
        original = copy.deepcopy(value)
        saved = self.upsert(value, display_fields=self.display())
        pointer = cloud._document_pointer(json.loads(saved["fields"]["内容"]))
        self.assertEqual(saved["fields"]["附件"], [{"file_token": "fileOriginal"}, {"file_token": pointer["file_token"]}])
        self.assertEqual(self.remote.list_entity_records()[0]["payload"], original)

    def test_chinese_input_alias_uses_the_same_identity(self):
        row = self.remote.upsert_entity("record", "stable-key", {"v": 1}, "op", display_fields={})
        result = self.remote.upsert_entity(cloud.ENTITY_KIND_LABELS["record"], "stable-key", {"v": 2}, "op2", display_fields={})
        self.assertEqual(result["record_id"], row["record_id"])
        self.assertEqual(len(self.fake.records[self.table]), 1)

    def test_display_field_whitelist_and_value_types_fail_before_network(self):
        for fields in ({"内容": "bad"}, {"附件": []}, {"业务键": "other"}, {"类别": "record"}, {"unknown": 1},
                       {"人员": "ou_verified"}, {"人员": [{"id": "recPerson"}]},
                       {"人员": [{"id": "ou_one"}, {"id": "ou_two"}]},
                       {"人员": [None]}, {"姓名": 1}, {"已答题数": True}, {"已答题数": float("inf")},
                       {"日期": "2026-10-21"}):
            with self.subTest(fields=fields), self.assertRaises(ValueError):
                self.upsert(display_fields=fields)
        self.assertEqual(self.fake.calls, [])

    def test_enrich_verifies_snapshot_and_only_updates_allowed_columns(self):
        value = {"z": {"second": 2, "first": 1}, "id": "original"}
        row = self.row("record", "untouched-key", value, compressed=True, fields={"custom": "keep"})
        before = copy.deepcopy(row)
        snapshot = self.remote.list_entity_records()[0]
        expected_hash = hashlib.sha256(b'{"id":"original","z":{"first":1,"second":2}}').hexdigest()
        self.assertEqual(snapshot["payload_sha256"], expected_hash)
        result = self.remote.enrich_entity_record(row["record_id"], "record", "untouched-key",
                                                  self.display(), expected_payload_sha256=snapshot["payload_sha256"])
        self.assertEqual(result["record_id"], before["record_id"])
        for name in ("内容", "附件", "业务键", "custom"):
            self.assertEqual(row["fields"][name], before["fields"][name])
        writes = self.fake.mutations()
        self.assertEqual(len(writes), 1)
        self.assertEqual(writes[0][:2], ("PUT", f"tables/{self.table}/records/{row['record_id']}"))
        self.assertEqual(set(json.loads(writes[0][3].content)["fields"]), set(self.display()) | {"类别"})
        self.assertTrue(any(c[0] == "GET" and c[1].endswith("/" + row["record_id"]) for c in self.fake.calls))

    def test_enrich_identity_or_changed_payload_never_writes(self):
        row = self.row("record", "original", {"revision": 2})
        snapshot = self.remote.list_entity_records()[0]
        cases = [("settings", "original", snapshot["payload_sha256"]), ("record", "different", snapshot["payload_sha256"]),
                 ("record", "original", hashlib.sha256(b'{"revision":1}').hexdigest())]
        for kind, key, digest in cases:
            with self.subTest(kind=kind, key=key, digest=digest), self.assertRaises(cloud.LearningCloudError) as caught:
                self.remote.enrich_entity_record(row["record_id"], kind, key, self.display(), expected_payload_sha256=digest)
            self.assertTrue(caught.exception.conflict)
        self.assertFalse(self.fake.mutations())
        self.assertEqual(row["fields"]["类别"], "record")

    def test_enrich_invalid_payload_or_wrong_get_record_id_never_writes(self):
        row = self.row("record", "original", {"revision": 1})
        digest = self.remote.list_entity_records()[0]["payload_sha256"]
        row["fields"]["内容"] = "broken"
        with self.assertRaises(cloud.LearningCloudError):
            self.remote.enrich_entity_record(row["record_id"], "record", "original", self.display(), expected_payload_sha256=digest)
        def wrong_id(request):
            if request.method == "GET" and request.url.path.endswith("/" + row["record_id"]):
                return self.fake.response({"record": {**row, "record_id": "recOther"}})
        self.fake.before = wrong_id
        with self.assertRaises(cloud.LearningCloudError):
            self.remote.enrich_entity_record(row["record_id"], "record", "original", self.display())
        self.assertFalse(self.fake.mutations())

    def test_enrich_lost_response_retries_get_and_skips_already_applied_fields(self):
        row = self.row("record", "original", {"revision": 1})
        digest = self.remote.list_entity_records()[0]["payload_sha256"]
        def lose(request, response):
            if request.method == "PUT":
                raise httpx.ReadTimeout("offline lost response", request=request)
        self.fake.after = lose
        with self.assertRaises(cloud.LearningCloudWriteUncertain):
            self.remote.enrich_entity_record(row["record_id"], "record", "original", self.display(), expected_payload_sha256=digest)
        self.fake.after = None
        self.fake.calls.clear()
        self.new_remote().enrich_entity_record(row["record_id"], "record", "original", self.display(), expected_payload_sha256=digest)
        self.assertFalse(self.fake.mutations())

    def test_bundle_lost_create_response_reuses_record_and_compressed_document(self):
        value = self.bundle(summary={"notes": "中" * (cloud.MAX_TEXT_LENGTH + 1)})
        def lose(request, response):
            if request.method == "POST" and request.url.path.endswith("/records"):
                raise httpx.ReadTimeout("offline lost response", request=request)
        self.fake.after = lose
        with self.assertRaises(cloud.LearningCloudWriteUncertain):
            self.upsert(value, display_fields=self.display())
        self.fake.after = None
        first = self.fake.records[self.table][0]
        original_id = first["record_id"]
        self.remote = self.new_remote()
        saved = self.upsert(value, display_fields=self.display())
        self.assertEqual(saved["record_id"], original_id)
        self.assertEqual(len(self.fake.records[self.table]), 1)
        self.assertEqual(len(self.fake.media), 1)
        self.assertEqual(self.remote.list_entity_records()[0]["payload"], value)
        self.assertEqual(self.remote.load_entities()[0]["storage_owner"], ["2026-10-21", "白", "stable-person"])

    def test_short_bundle_acknowledged_upload_survives_uncommitted_record_retry(self):
        def lose(request):
            if request.method == "POST" and request.url.path.endswith("/records"):
                raise httpx.ReadTimeout("offline unknown write", request=request)
        self.fake.before = lose
        with self.assertRaises(cloud.LearningCloudWriteUncertain):
            self.upsert(display_fields=self.display())
        self.assertEqual(self.fake.records[self.table], [])
        self.assertEqual(len(self.fake.media), 1)
        self.fake.before = None
        self.upsert(display_fields=self.display())
        self.assertEqual(len(self.fake.media), 1)
        self.assertEqual(len(self.fake.records[self.table]), 1)
        self.assertEqual(self.remote._pending_documents, {})

    def test_uncertain_put_receipt_with_wrong_person_is_not_acknowledged(self):
        self.upsert(display_fields=self.display())
        def wrong_person(request, response):
            if request.method == "PUT":
                data = response.json()["data"]
                data["record"]["fields"]["人员"] = [{"id": "ou_wrong"}]
                return self.fake.response(data)
        self.fake.after = wrong_person
        with self.assertRaises(cloud.LearningCloudWriteUncertain):
            self.upsert(display_fields=self.display(人员=[{"id": "ou_verified"}]))
        self.assertEqual(len(self.fake.records[self.table]), 1)

    def test_operation_credentials_switch_after_search_prevents_upload_and_write(self):
        self.manager._credentials.return_value = ("fixture-app-a", "fixture-secret-a")
        def switch(request, response):
            if request.url.path.endswith("/search"):
                self.manager._credentials.return_value = ("fixture-app-b", "fixture-secret-b")
        self.fake.after = switch
        with self.assertRaisesRegex(cloud.LearningCloudError, "Qt.*鉴权.*变化") as caught:
            self.upsert(display_fields=self.display(人员=[{"id": "ou_verified"}]))
        self.assertTrue(caught.exception.conflict)
        self.assertFalse(self.fake.mutations())
        self.assertIsNone(self.remote._entity_context.credentials)

    def test_upsert_expected_app_blocks_directory_identity_from_previous_application(self):
        self.manager._credentials.return_value = ("fixture-app-a", "fixture-secret-a")
        expected_app = self.remote.current_app_id()
        self.assertEqual(expected_app, "fixture-app-a")
        self.manager._credentials.return_value = ("fixture-app-b", "fixture-secret-b")
        with self.assertRaises(cloud.LearningCloudConflict) as caught:
            self.upsert(display_fields=self.display(人员=[{"id": "ou_old_app"}]), expected_app_id=expected_app)
        self.assertTrue(caught.exception.conflict)
        self.assertEqual(self.fake.calls, [])
        self.manager.get_tenant_token.assert_not_called()
        self.assertEqual(self.remote.current_app_id(), "fixture-app-b")
        self.upsert(display_fields=self.display(人员=[{"id": "ou_current_app"}]),
                    expected_app_id=self.remote.current_app_id())
        self.assertEqual(len(self.fake.records[self.table]), 1)

    def test_enrich_expected_app_blocks_directory_identity_from_previous_application(self):
        self.manager._credentials.return_value = ("fixture-app-a", "fixture-secret-a")
        row = self.row("record", "original", {"v": 1})
        original = copy.deepcopy(row)
        snapshot = self.remote.list_entity_records()[0]
        expected_app = self.remote.current_app_id()
        self.fake.calls.clear()
        self.manager._credentials.return_value = ("fixture-app-b", "fixture-secret-b")
        with self.assertRaises(cloud.LearningCloudConflict) as caught:
            self.remote.enrich_entity_record(row["record_id"], "record", "original",
                self.display(人员=[{"id": "ou_old_app"}]), expected_payload_sha256=snapshot["payload_sha256"],
                expected_app_id=expected_app)
        self.assertTrue(caught.exception.conflict)
        self.assertEqual(self.fake.calls, [])
        self.assertEqual(row, original)
        self.remote.enrich_entity_record(row["record_id"], "record", "original",
            self.display(人员=[{"id": "ou_current_app"}]), expected_payload_sha256=snapshot["payload_sha256"],
            expected_app_id=self.remote.current_app_id())
        self.assertEqual(row["fields"]["人员"], [{"id": "ou_current_app"}])

    def test_operation_credentials_switch_in_upload_headers_prevents_upload(self):
        self.manager._credentials.return_value = ("fixture-app-a", "fixture-secret-a")
        original = self.remote._headers
        def headers(force_refresh=False):
            result = original(force_refresh)
            searches = [c for c in self.fake.calls if c[1].endswith("/search")]
            if len(searches) == 2:
                self.manager._credentials.return_value = ("fixture-app-a", "fixture-secret-b")
            return result
        with patch.object(self.remote, "_headers", side_effect=headers), self.assertRaisesRegex(cloud.LearningCloudError, "鉴权.*变化"):
            self.upsert(display_fields=self.display())
        self.assertFalse(self.fake.mutations())

    def test_operation_credentials_switch_after_upload_stops_record_write(self):
        self.manager._credentials.return_value = ("fixture-app-a", "fixture-secret-a")
        def switch(request, response):
            if "upload_all" in request.url.path:
                self.manager._credentials.return_value = ("fixture-app-b", "fixture-secret-b")
        self.fake.after = switch
        with self.assertRaisesRegex(cloud.LearningCloudError, "鉴权.*变化"):
            self.upsert(display_fields=self.display())
        self.assertEqual(len(self.fake.media), 1)
        self.assertEqual(self.fake.records[self.table], [])
        self.assertEqual(self.remote._pending_documents, {})

    def test_operation_credentials_switch_after_record_write_is_not_acknowledged(self):
        self.manager._credentials.return_value = ("fixture-app-a", "fixture-secret-a")
        def switch(request, response):
            if request.method == "POST" and request.url.path.endswith("/records"):
                self.manager._credentials.return_value = ("fixture-app-b", "fixture-secret-b")
        self.fake.after = switch
        with self.assertRaisesRegex(cloud.LearningCloudError, "鉴权.*变化"):
            self.upsert(display_fields=self.display())
        self.assertEqual(len(self.fake.records[self.table]), 1)
        self.assertEqual(self.remote._pending_documents, {})

    def test_enrich_credentials_switch_on_get_download_or_put_keeps_pending(self):
        row = self.row("record", "original", {"v": 1}, compressed=True)
        digest = self.remote.list_entity_records()[0]["payload_sha256"]
        for phase in ("GET", "download", "PUT"):
            with self.subTest(phase=phase):
                self.manager._credentials.return_value = ("fixture-app-a", "fixture-secret-a")
                self.fake.calls.clear()
                def switch(request, response):
                    if (phase == "download" and request.url.path.endswith("/download")
                            or phase in ("GET", "PUT") and request.method == phase and request.url.path.endswith("/" + row["record_id"])):
                        self.manager._credentials.return_value = ("fixture-app-b", "fixture-secret-b")
                self.fake.after = switch
                with self.assertRaisesRegex(cloud.LearningCloudError, "鉴权.*变化") as caught:
                    self.remote.enrich_entity_record(row["record_id"], "record", "original", self.display(), expected_payload_sha256=digest)
                self.assertTrue(caught.exception.conflict)
                self.assertEqual(len(self.fake.mutations()), 1 if phase == "PUT" else 0)

    def test_pending_upload_cache_is_not_reused_by_another_application(self):
        self.manager._credentials.return_value = ("fixture-app-a", "fixture-secret-a")
        def lose(request):
            if request.method == "POST" and request.url.path.endswith("/records"):
                raise httpx.ReadTimeout("offline unknown write", request=request)
        self.fake.before = lose
        with self.assertRaises(cloud.LearningCloudWriteUncertain):
            self.upsert(display_fields=self.display())
        self.assertEqual(len(self.fake.media), 1)
        self.manager._credentials.return_value = ("fixture-app-b", "fixture-secret-b")
        self.fake.before = None
        saved = self.upsert(display_fields=self.display())
        self.assertEqual(len(self.fake.media), 2)
        pointer = cloud._document_pointer(json.loads(saved["fields"]["内容"]))
        self.assertEqual(pointer["file_token"], "fileUploaded2")


if __name__ == "__main__":
    unittest.main()
