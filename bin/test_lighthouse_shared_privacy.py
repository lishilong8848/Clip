"""Synthetic privacy projections; no service, credentials, runtime data or network."""
import base64
import copy
import hashlib
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from openclaw_service.assistant.lighthouse_ai import (
    AssistantError, private_identifier, safe_data, safe_text, shared_data,
)
from openclaw_service.assistant import lighthouse_documents as documents
from openclaw_service.assistant.lighthouse_sources import question_material_text

PHONE = "13900000000"
IDENTITY = "990000200001010016"
PIXELS = base64.b64encode(b"synthetic-image-bytes").decode("ascii")
DATA_IMAGE = "data:image/png;base64," + PIXELS
CREDENTIAL = "SYNTHETIC_CREDENTIAL_VALUE"


class MemoryStore:
    def __init__(self):
        self.docs = {}

    def get_document(self, namespace, key):
        return copy.deepcopy(self.docs.get((namespace, key)))

    def put_document(self, namespace, key, value):
        self.docs[namespace, key] = copy.deepcopy(value)


class SharedPrivacyTests(unittest.TestCase):
    def test_phone_and_identity_in_values_and_field_names(self):
        for number in (PHONE, IDENTITY, "990000000101016", "99000020000101001X"):
            for separator in ("", " ", "-", "\t", "\u200b", "\u200c", "\u2060", "\ufeff", "\u2011", "\u2013"):
                value = separator.join(number)
                with self.subTest(number=number, separator=repr(separator)):
                    text = safe_text("note " + value)
                    self.assertNotIn(value, text)
                    payload = {"note " + value: "FIELD_MUST_DISAPPEAR", "note": value, "nested": [value]}
                    for project in (safe_data, shared_data):
                        result = json.dumps(project(payload), ensure_ascii=False)
                        self.assertNotIn(value, result)
                        self.assertNotIn("FIELD_MUST_DISAPPEAR", result)
                    if number != PHONE:
                        self.assertTrue(private_identifier(value))
        self.assertEqual(safe_text("\uff11\uff13\uff19\uff10\uff10\uff10\uff10\uff10\uff10\uff10\uff10"), "")

    def test_numeric_phone_and_identity_keep_existing_redaction_marker(self):
        for value in (int(PHONE), float(PHONE), int(IDENTITY), float(IDENTITY)):
            with self.subTest(value=value):
                self.assertEqual(safe_data(value), "[\u654f\u611f\u4fe1\u606f\u5df2\u9690\u85cf]")
                self.assertEqual(shared_data(value), "[\u654f\u611f\u4fe1\u606f\u5df2\u9690\u85cf]")
                self.assertEqual(safe_data({"value": value})["value"], "[\u654f\u611f\u4fe1\u606f\u5df2\u9690\u85cf]")
        self.assertEqual(safe_data([0, 235, 10.5, False, None]), [0, 235, 10.5, False, None])

    def test_credential_field_aliases_and_normalized_names_are_removed(self):
        for label in ("AppSecret", "refresh_token", "\u8bbf\u95ee\u4ee4\u724c", "\u5e94\u7528\u51ed\u8bc1",
                      "\u51ed\u636e", "\u5bc6\u78bc", "\u5bc6\u9470", "\u6191\u8b49", "App\u200bSecret",
                      "\uff21\uff50\uff50\uff33\uff45\uff43\uff52\uff45\uff54"):
            with self.subTest(label=label):
                self.assertEqual(safe_data({label: CREDENTIAL, "count": 2}), {"count": 2})

    def test_free_text_credentials_include_quoted_labels_and_next_line_values(self):
        for label in ("AppSecret", "refresh_token", "\u8bbf\u95ee\u4ee4\u724c", "\u51ed\u636e", "\u51ed\u8bc1", "\u5bc6\u7801"):
            for assignment in (f"{label}={CREDENTIAL}", f'"{label}": "{CREDENTIAL}"',
                               f"{label}\uff1a\n\n  {CREDENTIAL}", f"{label}:\r\n{CREDENTIAL}",
                               f"**{label}**:\n```\n{CREDENTIAL}\n```"):
                with self.subTest(label=label, assignment=assignment):
                    value = safe_text("before\n" + assignment + "\nafter")
                    self.assertNotIn(CREDENTIAL, value)
                    self.assertIn("before", value)
                    self.assertIn("after", value)
                    self.assertEqual(safe_text(value), value)

    def test_signature_inline_images_and_original_urls_are_removed(self):
        references = (
            f"![\u7b7e\u540d]({DATA_IMAGE})",
            f'<img src="{DATA_IMAGE}" alt="signature">',
            f"\u7b7e\u540d\uff1a\n\n{DATA_IMAGE}",
            f"\u7b7e\u540d\uff1a\ndata:image/png;base64,\n{PIXELS}",
            "/api/signatures/image?record_id=fixture",
            "https://example.invalid/api/signatures/temporary/image?record_id=fixture",
            "https://example.invalid/api%2Fsignatures%2Fimage?record_id=fixture",
            "https://example.invalid/signatures/fixture.png",
            "[\u7b7e\u540d\u539f\u56fe](https://example.invalid/opaque)",
        )
        for reference in references:
            with self.subTest(reference=reference):
                value = safe_text("before\n" + reference + "\nafter")
                self.assertNotIn(PIXELS, value)
                self.assertNotIn("example.invalid", value)
                self.assertNotIn("/api/signatures/", value)
                self.assertIn("after", value)
        self.assertEqual(safe_data({"name": "signature", "content": DATA_IMAGE}), {"name": "signature"})
        self.assertEqual(safe_data({"name": "signature", "details": {"raw": DATA_IMAGE}}), {"name": "signature"})
        self.assertTrue(safe_text(f"![signature]({DATA_IMAGE})\n" + "x" * 6500, limit=None).endswith("x" * 6500))

    def test_identifiers_split_across_lines_do_not_survive(self):
        for value in ("139\n0000\n0000", "990000\n20000101\n0016"):
            with self.subTest(value=value):
                result = safe_text(value)
                self.assertNotIn(PHONE, "".join(result.split()))
                self.assertNotIn(IDENTITY, "".join(result.split()))

    def test_business_images_charts_downloads_and_signature_time_survive(self):
        for reference in (
            f"![UPS\u56fe\u8868]({DATA_IMAGE})",
            '<img alt="UPS chart" src="https://example.invalid/business/ups.png">',
            "[\u751f\u6210\u6587\u4ef6](/api/assistant/files/fixture)",
            "\u7b7e\u540d\u5b8c\u6210 [\u6f14\u7ec3\u8bb0\u5f55](/api/drills/fixture/download?scope=A)",
            "\u7b7e\u540d\u5b8c\u6210 [\u751f\u6210\u6587\u4ef6](https://example.invalid/signature-report.xlsx)",
        ):
            with self.subTest(reference=reference):
                self.assertEqual(safe_text(reference), reference)
                self.assertEqual(safe_data({"note": reference}), {"note": reference})
        value = {"signature_time": "2026-10-01T10:30", "ip_address": "10.0.0.1",
                 "mac_address": "00:1A:2B:3C:4D:5E", "path": "/api/assistant/files/fixture",
                 "ref": "query_form_a289617ab13940249922e985f84575f8"}
        self.assertEqual(safe_data(value), value)


class SharedAttachmentPrivacyTests(unittest.TestCase):
    def source(self, text, *, offset=0, length=9):
        content = text.encode("utf-8")
        return {"material_id": "fixture", "name": "business.txt", "kind": "material", "mime": "text/plain",
                "suffix": ".txt", "offset": offset, "length": length,
                "content_base64": base64.b64encode(content).decode("ascii"),
                "digest": hashlib.sha256(b".txt\0" + content).hexdigest()}

    def test_full_text_is_sanitized_before_caching_and_all_offsets(self):
        text = "before\n" + IDENTITY + "\n" + PHONE + "\nAppSecret=" + CREDENTIAL + "\nafter"
        source, store = self.source(text), MemoryStore()
        with patch("openclaw_service.assistant.lighthouse_files.extract_text", return_value=text) as extract:
            pages = [question_material_text(store, {**source, "offset": offset})["text"] for offset in range(0, len(text), 9)]
        self.assertEqual("".join(pages), safe_text(text, limit=None))
        self.assertEqual(next(iter(store.docs.values()))["text"], safe_text(text, limit=None))
        extract.assert_called_once()
        self.assertNotIn(IDENTITY, "".join(pages))
        self.assertNotIn(PHONE, "".join(pages))
        self.assertNotIn(CREDENTIAL, "".join(pages))

    def test_old_cache_is_sanitized_before_any_pagination(self):
        text = IDENTITY + "\n" + PHONE + "\nrefresh_token=" + CREDENTIAL + "\n" + "x" * 6500
        source, store = self.source(text), MemoryStore()
        store.put_document("lighthouse_question_text", source["digest"], {"text": text})
        with patch("openclaw_service.assistant.lighthouse_files.extract_text", side_effect=AssertionError("cached")):
            first = question_material_text(store, {**source, "length": 6000})
            second = question_material_text(store, {**source, "offset": first["next_offset"], "length": 6000})
        self.assertEqual(first["text"] + second["text"], safe_text(text, limit=None))
        self.assertIsNone(second["next_offset"])

    def test_document_body_is_sanitized_before_pagination_without_auth_imports(self):
        body = "\u5408\u6210\u4e1a\u52a1\u6b63\u6587\u3002" * 1000
        raw = "A\u697c\n" + IDENTITY + "\nAppSecret=" + CREDENTIAL + "\n![\u7b7e\u540d](" + DATA_IMAGE + ")\n" + body
        reader = Mock(side_effect=lambda url, params: {"code": 0, "data": {"content": raw} if url.endswith("/raw_content") else {"document": {"title": "A\u697c\u4e1a\u52a1\u7eaa\u5f55"}}})
        actor = {"id": "fixture", "scopes": ["A"]}
        with patch.object(documents, "document_reader", return_value=reader):
            first = documents.read_document(None, actor, "https://vnet.feishu.cn/docx/Fixture")
            self.assertEqual(len(first["text"]), 6000)
            self.assertTrue(first["truncated"])
            self.assertIsNotNone(first["next_offset"])
            second = documents.read_document(None, actor, "https://vnet.feishu.cn/docx/Fixture", first["next_offset"])
        self.assertIsNone(second["next_offset"])
        self.assertEqual(first["text"] + second["text"], safe_text(raw, limit=None))
        for forbidden in (IDENTITY, CREDENTIAL, PIXELS):
            self.assertNotIn(forbidden, first["text"] + second["text"])

    def test_document_credential_title_blocks_body_fetch(self):
        for title in ("A\u697c\u8bbf\u95ee\u4ee4\u724c", "A\u697cAppSecret", "A\u697c\u51ed\u636e"):
            reader = Mock(return_value={"code": 0, "data": {"document": {"title": title}}})
            with self.subTest(title=title), patch.object(documents, "document_reader", return_value=reader):
                with self.assertRaises(AssistantError):
                    documents.read_document(None, {"scopes": ["A"]}, "https://vnet.feishu.cn/docx/Fixture")
                reader.assert_called_once()


class SharedDataContractTests(unittest.TestCase):
    def test_native_raw_forms_business_ids_links_and_full_lists_survive(self):
        original = {"_raw": {"_native": True, "openid": "ou_a13900000000fixture", "open_id": "ou_fixture1234",
                    "url": "https://example.invalid/base/fixture?table=tblFixture&view=vewFixture",
                    "attachments": [{"file_token": "fixturefile", "url": "/api/assistant/files/fixture", "name": "chart.png"}],
                    "image": DATA_IMAGE, "signature_time": "10:30", "values": list(range(65)),
                    "ip_address": "10.0.0.1", "device_address": "A-floor-1", "\u8bbe\u5907\u5730\u5740": "A\u697c\u673a\u623f",
                    "ref": "query_form_a289617ab13940249922e985f84575f8"}}
        snapshot = copy.deepcopy(original)
        self.assertEqual(shared_data(original), original)
        self.assertEqual(original, snapshot)

    def test_raw_private_fields_and_formatted_values_are_scrubbed(self):
        value = {"_raw": {"AppSecret": CREDENTIAL, "refresh_token": CREDENTIAL, "app_secret_value": CREDENTIAL,
                 "\u51ed\u636e": CREDENTIAL, "phone": PHONE, "email": "fixture@example.invalid",
                 "home_address": "123 Main Street", "\u8eab\u4efd\u8bc1": IDENTITY,
                 "count": 3, "note": "139-0000-0000", "extra": int(IDENTITY)}}
        result = shared_data(value)
        self.assertEqual(set(result["_raw"]), {"count", "note", "extra"})
        for forbidden in (CREDENTIAL, PHONE, IDENTITY, "fixture@example.invalid", "123 Main Street"):
            self.assertNotIn(forbidden, json.dumps(result))

    def test_url_credentials_are_hidden_in_query_fragment_and_basic_auth(self):
        urls = (
            f"https://user:{CREDENTIAL}@example.invalid/file?AppSecret={CREDENTIAL}&scope=A#refresh_token={CREDENTIAL}&view=chart",
            f"https://example.invalid/chart.png?signature={CREDENTIAL}#/page?access_token={CREDENTIAL}&scope=A",
            f"/api/assistant/files/fixture?%61pp_secret={CREDENTIAL}&scope=A#\u8bbf\u95ee\u4ee4\u724c={CREDENTIAL}",
            f"https://example.invalid/file?note={PHONE}#note={IDENTITY}",
        )
        for url in urls:
            with self.subTest(url=url):
                result = shared_data({"_raw": {"url": url}})["_raw"]["url"]
                self.assertTrue(result.startswith(("https://example.invalid/", "/api/assistant/files/")))
                for forbidden in (CREDENTIAL, PHONE, IDENTITY, "user:"):
                    self.assertNotIn(forbidden, result)
                self.assertEqual(shared_data({"url": result})["url"], result)

    def test_signature_originals_are_removed_without_hiding_business_charts(self):
        value = {"_raw": {"signature_image": DATA_IMAGE, "signature": {"content": DATA_IMAGE},
                 "url": "/api/signatures/image?record_id=fixture", "attachments": [{"kind": "signature", "data": DATA_IMAGE}],
                 "name": "signature-report.xlsx", "image": DATA_IMAGE,
                 "download_url": "/api/drills/fixture/download?scope=A"}}
        result = shared_data(value)["_raw"]
        self.assertNotIn("signature_image", result)
        self.assertEqual(result["signature"]["content"], "")
        self.assertEqual(result["url"], "")
        self.assertEqual(result["attachments"][0]["data"], "")
        self.assertEqual(result["image"], DATA_IMAGE)
        self.assertEqual(result["download_url"], value["_raw"]["download_url"])
        self.assertEqual(shared_data({"kind": "signature", "url": "https://example.invalid/opaque"}),
                         {"kind": "signature", "url": ""})

    def test_bare_signature_image_magic_and_binary_cannot_escape(self):
        headers = (b"\x89PNG\r\n\x1a\n", b"\xff\xd8\xff", b"GIF89a", b"BM", b"II*\0",
                   b"RIFF\x10\0\0\0WEBP", b'<svg xmlns="http://www.w3.org/2000/svg">')
        for header in headers:
            content = header + b"synthetic-image-content"
            encoded = base64.b64encode(content).decode("ascii")
            for name in ("signature", "\u7b7e\u540d", "signature_data", "raw_signature"):
                for value in (encoded, content, bytearray(content)):
                    with self.subTest(header=header, name=name, kind=type(value).__name__):
                        self.assertEqual(shared_data({"_raw": {name: value, "count": 2}}), {"_raw": {"count": 2}})
            self.assertEqual(shared_data({"kind": "signature", "data": encoded})["data"], "")
            self.assertEqual(shared_data({"kind": "diagram", "data": encoded})["data"], encoded)
            self.assertNotIn(encoded, safe_text("\u7b7e\u540d: " + encoded))
        self.assertEqual(shared_data({"signature": "iVBORw0KGgo..."}), {})
        states = {"signature_time": "10:30", "\u7b7e\u540d\u59d3\u540d": "\u6d4b\u8bd5\u4eba\u5458",
                  "\u662f\u5426\u7b7e\u540d": True, "has_signature_image": False, "signature_data": {"status": "pending"}}
        self.assertEqual(shared_data(states), states)

    def test_json_text_keeps_its_type_and_non_private_business_fields(self):
        ordinary = '{ "_raw": {"openid": "ou_fixture1234", "url": "/api/assistant/files/fixture"} }'
        self.assertEqual(shared_data(ordinary), ordinary)
        text = json.dumps({"_raw": {"AppSecret": CREDENTIAL, "openid": "ou_fixture1234"}})
        result = shared_data(text)
        self.assertIsInstance(result, str)
        self.assertEqual(json.loads(result), {"_raw": {"openid": "ou_fixture1234"}})


if __name__ == "__main__":
    unittest.main()
