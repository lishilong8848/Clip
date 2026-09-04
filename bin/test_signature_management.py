import base64
import copy
import io
import json
import re
import sys
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse
from unittest.mock import Mock, patch

from PIL import Image, ImageDraw

BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal.signature_management import (
    ALIASES_FIELD, ORIGIN_FIELD, REQUEST_NS, SignatureManagement,
    SignatureManagementError, dispatch, resolve_directory,
)
from lan_bitable_template_portal.state_store import LanPortalStateStore


class FakeService:
    def __init__(self, store):
        self._state_store = store
        self._signature_people_cache_lock = threading.RLock()
        self._external_signature_people_cache_lock = threading.RLock()
        self._signature_people_cache = self._external_signature_people_cache = None
        self.staff, self.external = {}, {}
        self.uploads = 0
        self.creates = 0
        self.deletes = []
        self.patch_after_error = False
        self.staff_error = self.external_error = False
        self.readback_error = False
        self._signature_crypto = SimpleNamespace(
            metadata_from_field=lambda v: json.loads(v) if isinstance(v, str) and v else (v or {}),
            metadata_to_text=lambda v: json.dumps(v, sort_keys=True),
            is_portable_metadata=lambda v: bool(v),
        )
        self._write_http_client = None
        self.manager = SignatureManagement(self)
        self.d = self.manager.d

    def add(self, rid, name="测试人员", *, source="staff", signature=True, origin="", aliases=None, employee_no="", account_nature="VNET"):
        fields = {"姓名" if source == "staff" else "员工姓名": name, "楼栋": "E楼", "员工工号": employee_no,
                  ORIGIN_FIELD: origin, ALIASES_FIELD: "\n".join(aliases or [])}
        if source == "staff":
            fields["账号性质"] = account_nature
        if signature:
            fields.update({"手写签名": [{"file_token": "old-" + rid}], "密钥": json.dumps({"version": 2, "sig": rid})})
        (self.staff if source == "staff" else self.external)[rid] = fields

    @staticmethod
    def _mop_field_text(fields, names):
        return next((str(fields.get(n) or "") for n in names if fields.get(n)), "")

    @staticmethod
    def _building_codes_from_value(value):
        return re.findall(r"[ABCDEH]|110", str(value))

    def _load(self, source, force):
        cache_name = "_signature_people_cache" if source == "staff" else "_external_signature_people_cache"
        cached = getattr(self, cache_name)
        if not force and cached:
            return copy.deepcopy(cached["people"])
        if self.staff_error if source == "staff" else self.external_error:
            raise RuntimeError("simulated unavailable")
        records = self.staff if source == "staff" else self.external
        people = []
        for rid, fields in records.items():
            attachments = fields.get("手写签名") or []
            account_nature = fields.get("账号性质") or ""
            people.append({"record_id": rid, "name": fields.get("姓名") or fields.get("员工姓名"), "building": fields["楼栋"],
                "employee_no": fields.get("员工工号") or "", "has_signature": bool(attachments and fields.get("密钥")),
                "signature_count": len(attachments), "signature_version": attachments[0]["file_token"] if attachments else "",
                "signature_requires_resign": bool(attachments and not fields.get("密钥")), "portable_signature": bool(fields.get("密钥")),
                "origin_staff_record_id": fields.get(ORIGIN_FIELD, ""), "historical_record_ids": fields.get(ALIASES_FIELD, "").split(),
                "raw_fields": copy.deepcopy(fields), "open_id": "ou-person" if source == "staff" else "",
                "account_nature": account_nature,
                "can_receive_message": bool(source == "staff" and account_nature == "VNET")})
        setattr(self, cache_name, {"loaded_ts": time.time(), "people": people})
        return copy.deepcopy(people)

    def _load_signature_people(self, force=False):
        return self._load("staff", force)

    def _load_external_signature_people(self, force=False):
        return self._load("external", force)

    def _request_json(self, path, *, table_id, **kwargs):
        records = self.staff if table_id == self.d.SIGNATURE_TABLE_ID else self.external
        rid = path.rsplit("/", 1)[-1]
        if rid not in records:
            raise RuntimeError("RecordIdNotFound")
        fields = copy.deepcopy(records[rid])
        if self.readback_error and self.uploads:
            fields["手写签名"] = [{"file_token": "incorrect"}]
        return {"data": {"record": {"record_id": rid, "fields": fields}}}

    def _load_table_fields(self, **kwargs):
        return [], {key: SimpleNamespace(field_type=1, has_formula=False) for key in (ORIGIN_FIELD, ALIASES_FIELD)}

    def _field_option_write_values(self, metas, name, values):
        return []

    def _create_record_fields(self, *, fields, **kwargs):
        self.creates += 1
        rid = "recCreated" + str(self.creates)
        self.external[rid] = copy.deepcopy(fields)
        return {"data": {"record": {"record_id": rid}}}

    def _patch_record_fields(self, *, table_id, record_id, fields, **kwargs):
        records = self.staff if table_id == self.d.SIGNATURE_TABLE_ID else self.external
        records[record_id].update(copy.deepcopy(fields))
        if self.patch_after_error:
            raise TimeoutError("response lost")

    def _delete_record_fields(self, *, record_id, **kwargs):
        self.deletes.append(record_id)
        self.external.pop(record_id, None)

    def _save_encrypted_signature_record(self, *, before_write=None, **kwargs):
        self.uploads += 1
        token, metadata = "new-" + str(self.uploads), {"version": 2, "sig": str(self.uploads)}
        if before_write:
            before_write(token, metadata)
        self._patch_record_fields(table_id=kwargs["table_id"], record_id=kwargs["record_id"],
                                  fields={"手写签名": [{"file_token": token}], "密钥": json.dumps(metadata)})
        return token, metadata

    @staticmethod
    def _extract_signature_attachments(fields):
        return fields.get("手写签名") or []

    @staticmethod
    def _signature_public_base_url(**kwargs):
        return "http://192.168.1.2:18766"

    @staticmethod
    def _decode_signature_png(value):
        return base64.b64decode(value.split(",", 1)[-1])

    @staticmethod
    def _transparent_signature_png(value):
        return value

    def _download_mop_attachment(self, attachment):
        return b"encrypted-signature", "application/octet-stream"

    def external_signature_image_bytes(self, *, record_id):
        return signature_bytes(), "image/png"


def signature_bytes(blank=False):
    image = Image.new("RGBA", (120, 60), (255, 255, 255, 0))
    if not blank:
        ImageDraw.Draw(image).line([(5, 45), (20, 5), (75, 50), (110, 20)], fill=(0, 0, 0, 255), width=4)
    out = io.BytesIO()
    image.save(out, "PNG")
    return out.getvalue()


class SignatureManagementTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = LanPortalStateStore(Path(self.tmp.name) / "state.sqlite3")
        self.s = FakeService(self.store)
        self.m = self.s.manager
        self.s.add("recStaff", signature=False)
        self.messages = []
        self.sender = Mock(side_effect=lambda text, ids: (self.messages.append((text, ids)) or True, "", []))
        self.sequence = 0

    def request(self, *, source="staff", rid="recStaff", recipient="", sender=None, operation_id=None):
        self.sequence += 1
        return self.m.send({"source": source, "record_id": rid, "recipient": recipient,
                           "operation_id": operation_id or f"req{self.sequence}"},
                          actor="ou-actor", base_url="http://localhost:18766", send_text=sender or self.sender)

    def payload(self, message=-1, blank=False):
        url = self.messages[message][0].splitlines()[-1]
        parsed = urlparse(url)
        return {"request_id": parse_qs(parsed.query)["request_id"][0], "token": parse_qs(parsed.fragment)["token"][0],
                "signature_png": "data:image/png;base64," + base64.b64encode(signature_bytes(blank)).decode(), "confirmed_self": True}

    def test_name_alone_does_not_merge(self):
        self.s.add("recTemp", source="external")
        self.assertEqual(len(self.m.directory()["people"]), 2)

    def test_explicit_origin_prefers_formal_then_falls_back(self):
        self.s.add("recTemp", source="external", origin="recStaff")
        data = self.m.directory()
        self.assertEqual([p["source"] for p in data["people"]], ["external"])
        self.s.add("recStaff", signature=True)
        self.m.invalidate()
        self.assertEqual([p["source"] for p in self.m.directory()["people"]], ["staff"])

    def test_aliases_resolve_old_id_without_name_matching(self):
        self.s.add("recTemp", source="external", aliases=["recOld"])
        self.assertEqual(self.m.directory()["resolved"]["external:recOld"]["record_id"], "recTemp")

    def test_multiple_explicit_candidates_are_not_arbitrarily_chosen(self):
        self.s.add("recT1", source="external", origin="recStaff")
        self.s.add("recT2", source="external", origin="recStaff")
        data = self.m.directory()
        self.assertFalse(data["resolved"]["staff:recStaff"]["has_signature"])
        self.assertEqual(self.request(recipient="E")["destination"], "正式人员表")
        self.assertEqual(self.s.creates, 0)

    def test_refresh_reads_both_and_preserves_failed_source(self):
        self.s.add("recTemp", source="external")
        self.m.refresh()
        self.s.external_error = True
        data = self.m.refresh()
        self.assertFalse(data["sources"]["external"]["ok"])
        self.assertTrue(any(p["record_id"] == "recTemp" for p in data["people"]))

    def test_sequential_manual_refresh_is_not_throttled(self):
        self.m.refresh()
        self.s.add("recNew", source="external")
        self.assertTrue(any(p["record_id"] == "recNew" for p in self.m.refresh()["people"]))

    def test_unsigned_external_and_invalid_signature_are_visible(self):
        self.s.add("recUnsigned", source="external", signature=False)
        self.s.add("recInvalid", source="external")
        self.s.external["recInvalid"]["密钥"] = ""
        rows = {p["record_id"]: p for p in self.m.people({})["people"]}
        self.assertEqual(rows["recUnsigned"]["signature_status"], "unsigned")
        self.assertEqual(rows["recInvalid"]["signature_status"], "resign")
        self.assertNotIn("raw_fields", rows["recInvalid"])

    def test_same_request_does_not_send_twice(self):
        first = self.request(operation_id="reqSame")
        self.request(operation_id="reqSame")
        self.assertEqual(self.sender.call_count, 1)
        self.assertAlmostEqual(first["expires_at"] - first["created_at"], 86400, delta=2)
        self.assertNotIn("token", urlparse(self.messages[0][0].splitlines()[-1]).query)

    def test_completed_link_can_no_longer_open_but_submit_retry_is_idempotent(self):
        self.request()
        payload = self.payload()
        self.assertEqual(self.m.submit(payload)["status"], "completed")
        with self.assertRaises(SignatureManagementError) as error:
            self.m.session(payload)
        self.assertEqual(error.exception.status, 410)
        self.assertEqual(self.m.submit(payload)["status"], "completed")

    def test_send_reuses_the_directory_already_loaded_by_the_page(self):
        self.m.people({})
        staff_forces, external_forces = [], []
        staff_loader, external_loader = self.s._load_signature_people, self.s._load_external_signature_people
        with (patch.object(self.s, "_load_signature_people", side_effect=lambda force=False: (staff_forces.append(force), staff_loader(force))[1]),
              patch.object(self.s, "_load_external_signature_people", side_effect=lambda force=False: (external_forces.append(force), external_loader(force))[1])):
            self.request()
        self.assertFalse(any(staff_forces + external_forces))

    def test_save_and_duplicate_submission_upload_once(self):
        self.request()
        payload = self.payload()
        self.assertEqual(self.m.submit(payload)["status"], "completed")
        self.assertEqual(self.m.submit(payload)["status"], "completed")
        self.assertEqual(self.s.uploads, 1)

    def test_forwarded_link_ignores_logged_in_actor_but_requires_token(self):
        self.request()
        result = dispatch(self.m, "POST", "submit", self.payload(), actor="ou-duty")
        self.assertEqual(result["status"], "completed")
        bad = dict(self.payload(), token="wrong")
        with self.assertRaises(SignatureManagementError):
            dispatch(self.m, "POST", "submit", bad, actor="ou-duty")

    def test_reissue_invalidates_previous_link(self):
        self.request()
        old = self.payload()
        self.request()
        with self.assertRaises(SignatureManagementError):
            self.m.submit(old)
        self.assertEqual(self.m.submit(self.payload())["status"], "completed")

    def test_fallback_link_revokes_previous_direct_link(self):
        self.request()
        direct = self.payload()
        self.request(recipient="E")
        with self.assertRaises(SignatureManagementError):
            self.m.submit(direct)

    def test_expired_link_rejected(self):
        self.request()
        payload = self.payload()
        with patch("lan_bitable_template_portal.signature_management.time.time", return_value=time.time() + 90000):
            with self.assertRaises(SignatureManagementError):
                self.m.submit(payload)

    def test_blank_signature_and_missing_self_confirmation_rejected(self):
        self.request()
        for payload in (self.payload(blank=True), dict(self.payload(), confirmed_self=False)):
            with self.assertRaises(SignatureManagementError):
                self.m.submit(payload)
        self.assertEqual(self.s.uploads, 0)

    def test_changed_remote_signature_does_not_get_overwritten(self):
        self.request()
        self.s.staff["recStaff"]["密钥"] = '{"changed":true}'
        with self.assertRaises(SignatureManagementError):
            self.m.submit(self.payload())
        self.assertEqual(self.s.uploads, 0)

    def test_changed_person_identity_rejects_old_link(self):
        self.request()
        self.s.staff["recStaff"]["姓名"] = "另一个人"
        with self.assertRaises(SignatureManagementError):
            self.m.submit(self.payload())

    def test_remote_write_response_loss_is_reconciled(self):
        self.request()
        self.s.patch_after_error = True
        self.assertEqual(self.m.submit(self.payload())["status"], "completed")
        self.assertEqual(self.s.uploads, 1)

    def test_exact_readback_failure_is_retryable_without_uploading_again(self):
        self.request()
        payload = self.payload()
        self.s.readback_error = True
        with self.assertRaises(SignatureManagementError):
            self.m.submit(payload)
        self.assertNotEqual(self.store.get_document(REQUEST_NS, payload["request_id"])["status"], "completed")
        self.s.readback_error = False
        self.assertEqual(self.m.submit(payload)["status"], "completed")
        self.assertEqual(self.s.uploads, 1)

    def test_local_completion_failure_recovers_after_restart(self):
        self.request()
        payload = self.payload()
        put = self.store.put_document
        def fail_once(namespace, key, data):
            if namespace == REQUEST_NS and data.get("status") == "completed":
                raise OSError("disk full")
            return put(namespace, key, data)
        with patch.object(self.store, "put_document", side_effect=fail_once):
            with self.assertRaises(OSError):
                self.m.submit(payload)
        new_manager = SignatureManagement(self.s)
        self.assertEqual(new_manager.submit(payload)["status"], "completed")
        self.assertEqual(self.s.uploads, 1)

    def test_relay_recipient_keeps_formal_person_as_write_target(self):
        self.request(recipient="E")
        self.assertEqual(self.s.creates, 0)
        self.m.submit(self.payload())
        self.request(recipient="H")
        self.assertEqual(self.s.creates, 0)
        self.assertIn("手写签名", self.s.staff["recStaff"])
        self.assertEqual(self.messages[-1][1], [self.s.d.BUILDING_OPEN_ID_MAP["H"]])

    def test_only_explicit_new_temporary_person_writes_temporary_table(self):
        person = self.m._new_temporary(
            {"name": "临时新人员", "building": "E楼"},
            "create-temp",
            "ou-actor",
        )
        self.request(source="external", rid=person["record_id"], recipient="E")
        self.m.submit(self.payload())
        self.assertEqual(self.s.creates, 1)
        self.assertIn("手写签名", self.s.external[person["record_id"]])
        self.assertNotIn("手写签名", self.s.staff["recStaff"])

    def test_non_vnet_person_requires_relay_and_vnet_person_can_relay(self):
        self.s.staff["recStaff"]["账号性质"] = "外部账号"
        self.m.invalidate()
        self.assertEqual(self.request()["status"], "recipient_required")
        self.s.add("recRelay", name="代收人", account_nature="VNET")
        self.m.invalidate()
        result = self.request(recipient="staff:recRelay")
        self.assertEqual(result["destination"], "正式人员表")
        self.assertEqual(self.messages[-1][1], ["ou-person"])

    def test_linked_temporary_signature_migrates_to_formal_once(self):
        self.s.add("recTemp", source="external", origin="recStaff")
        result = self.m.migrate_linked_signatures("ou-admin")
        self.assertEqual(result["migrated"], 1)
        self.assertIn("手写签名", self.s.staff["recStaff"])
        self.assertEqual([person["source"] for person in self.m.directory()["people"]], ["staff"])
        self.assertEqual([person["source"] for person in self.m.people({})["people"]], ["staff"])
        self.assertEqual(self.m.migrate_linked_signatures("ou-admin")["already_signed"], 1)

    def test_failed_direct_message_does_not_create_temporary_person(self):
        response = self.request(sender=Mock(return_value=(False, "timeout", [{"failure_kind": "network"}])))
        self.assertEqual(response["status"], "send_failed")
        self.assertEqual(self.s.creates, 0)

    def test_multiple_or_arbitrary_recipients_rejected(self):
        for recipient in ("AB", "duty:X", "ou-attacker", "staff:missing"):
            with self.assertRaises(SignatureManagementError):
                self.request(recipient=recipient)
        self.assertEqual(self.request(recipient="110")["status"], "sent")

    def test_association_requires_current_identity_version(self):
        self.s.add("recTemp", source="external")
        row = next(p for p in self.m.people({})["people"] if p["record_id"] == "recTemp")
        with self.assertRaises(SignatureManagementError):
            self.m.associate({"record_id": "recTemp", "staff_record_id": "recStaff",
                              "expected_version": "stale", "confirmed_same_person": True})
        self.assertEqual(self.s.external["recTemp"].get(ORIGIN_FIELD), "")
        self.m.associate({"record_id": "recTemp", "staff_record_id": "recStaff",
                          "expected_version": row["identity_version"], "confirmed_same_person": True})
        self.assertEqual(self.s.external["recTemp"][ORIGIN_FIELD], "recStaff")

    def test_association_reuses_loaded_directory_and_schema(self):
        self.s.add("recTemp", source="external")
        row = next(p for p in self.m.people({})["people"] if p["record_id"] == "recTemp")
        staff_forces, external_forces = [], []
        staff_loader, external_loader = self.s._load_signature_people, self.s._load_external_signature_people
        with (patch.object(self.s, "_load_signature_people", side_effect=lambda force=False: (staff_forces.append(force), staff_loader(force))[1]),
              patch.object(self.s, "_load_external_signature_people", side_effect=lambda force=False: (external_forces.append(force), external_loader(force))[1]),
              patch.object(self.s, "_load_table_fields", wraps=self.s._load_table_fields) as fields):
            payload = {"record_id": "recTemp", "staff_record_id": "recStaff",
                       "expected_version": row["identity_version"], "confirmed_same_person": True}
            self.m.associate(payload)
            self.assertFalse(any(staff_forces + external_forces))
            self.assertEqual(fields.call_count, 1)

    def test_people_search_matches_separate_terms(self):
        rows = self.m.people({"q": "测试 E楼"})["people"]
        self.assertEqual([row["record_id"] for row in rows], ["recStaff"])

    def test_concurrent_save_is_one_upload(self):
        self.request()
        payload = self.payload()
        with ThreadPoolExecutor(max_workers=3) as executor:
            results = list(executor.map(lambda _: self.m.submit(payload), range(3)))
        self.assertTrue(all(r["status"] == "completed" for r in results))
        self.assertEqual(self.s.uploads, 1)

    def test_creation_uncertainty_is_not_repeated(self):
        original = self.s._create_record_fields
        def lost_response(**kwargs):
            original(**kwargs)
            raise TimeoutError()
        with patch.object(self.s, "_create_record_fields", side_effect=lost_response):
            with self.assertRaises(TimeoutError):
                self.m._new_temporary({"name": "临时新人员", "building": "E楼"}, "create1", "ou-actor")
        with self.assertRaises(SignatureManagementError):
            self.m._new_temporary({"name": "临时新人员", "building": "E楼"}, "create1", "ou-actor")
        self.assertEqual(self.s.creates, 1)

    def test_management_auth_and_merge_admin_required(self):
        for operation, method in (("people", "GET"), ("refresh", "POST"), ("requests", "POST")):
            with self.assertRaises(SignatureManagementError) as error:
                dispatch(self.m, method, operation, {})
            self.assertEqual(error.exception.status, 401)
        for operation in ("merge", "migrate"):
            with self.assertRaises(SignatureManagementError) as error:
                dispatch(self.m, "POST", operation, {}, actor="ou-person", is_admin=False)
            self.assertEqual(error.exception.status, 403)
        with self.assertRaises(SignatureManagementError) as error:
            dispatch(self.m, "GET", "preview", {}, actor="ou-person")
        self.assertEqual(error.exception.status, 404)

    def test_merge_requires_review_backs_up_and_is_idempotent(self):
        self.s.add("recOne", source="external", signature=True)
        self.s.add("recTwo", source="external", signature=True)
        group = self.m.duplicates()["groups"][0]
        payload = {"record_ids": ["recOne", "recTwo"], "keep_record_id": "recOne", "signature_record_id": "recTwo",
                   "operation_id": "merge1", "expected_version": group["version"],
                   "confirmed_same_person": True, "all_clients_upgraded": True}
        self.assertEqual(self.m.merge(payload, "ou-admin")["deleted_ids"], ["recTwo"])
        self.assertEqual(self.m.merge(payload, "ou-admin")["status"], "completed")
        self.assertEqual(self.s.deletes, ["recTwo"])
        journal = self.store.get_document("signature_management_merge", "merge1")
        self.assertIn("recTwo", journal["backup"])
        self.assertEqual(self.m.directory()["resolved"]["external:recTwo"]["record_id"], "recOne")

    def test_merge_does_not_delete_on_readback_failure(self):
        self.s.add("recOne", source="external")
        self.s.add("recTwo", source="external")
        group = self.m.duplicates()["groups"][0]
        self.s.readback_error = True
        with self.assertRaises(SignatureManagementError):
            self.m.merge({"record_ids": ["recOne", "recTwo"], "keep_record_id": "recOne", "signature_record_id": "recTwo",
                          "operation_id": "merge1", "expected_version": group["version"],
                          "confirmed_same_person": True, "all_clients_upgraded": True}, "ou-admin")
        self.assertEqual(self.s.deletes, [])


if __name__ == "__main__":
    unittest.main()
