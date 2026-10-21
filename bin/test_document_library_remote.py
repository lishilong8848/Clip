"""Isolated checks for the document-library transport adapter.

Every collaborator is faked: the token manager, the HTTP client factory, the
external write guard and AssistantError. No configuration is read and no
network call can leave the process. All temp files live in a temporary dir.
"""
from __future__ import annotations

import os
import sys
import tempfile
import unittest
import zlib
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lan_bitable_template_portal import document_library_remote as remote_module

SOURCE = {"base_token": "bascnFixtureBase", "table_id": "tblFixtureTable"}
RECORD_ID = "recFixture0001"
OTHER_RECORD_ID = "recFixture0002"
FILE_TOKEN = "boxcnFixtureFile0001"
PART_FILE_TOKEN = "boxcnFixturePart0001"
UPLOAD_ID = "upload-fixture-0001"
FIXTURE_CREDENTIALS = ("cli_fixture", "fixture-secret")
ROTATED_CREDENTIALS = ("cli_other", "other-secret")


class AssistantError(ValueError):
    """Stand-in with the same public surface as the real safe error."""

    def __init__(self, message, status=400, *, category=""):
        super().__init__(message)
        self.status = status
        self.category = category


class FakeTokenManager:
    """Synthetic credential holder exposing the shared manager's surface."""

    def __init__(self, app_id="cli_fixture", app_secret="fixture-secret"):
        self.credentials = (app_id, app_secret)
        self.issued = 0
        self.forced = []
        self.fail_token = False
        self.on_token = None

    def _credentials(self):
        return self.credentials

    def get_tenant_token(self, *, force_refresh=False):
        if self.fail_token:
            raise RuntimeError("fixture token backend unavailable")
        self.issued += 1
        self.forced.append(bool(force_refresh))
        token = "tenant-token-{0}".format(self.issued)
        if callable(self.on_token):
            self.on_token(self.issued)
        return token


class FakeHttpClient:
    """Records every call; behavior is scripted per test.

    There is deliberately no raw ``request`` hook: the adapter must not use
    an unbounded raw upload branch any more, so such a call fails loudly.
    """

    instances = []

    def __init__(self, *args, **kwargs):
        self.calls = []
        self.json_responses = []
        self.json_error = None
        self.file_response = {"code": 0, "data": {"file_token": FILE_TOKEN}}
        self.file_error = None
        self.bytes_payload = b"fixture-bytes"
        self.bytes_error = None
        self.bytes_response = None
        self.before_call = None
        self.file_callback = None
        self.closed = 0
        FakeHttpClient.instances.append(self)

    def close(self):
        self.closed += 1

    def _hook(self):
        if callable(self.before_call):
            self.before_call()

    @staticmethod
    def _next(script):
        if not script:
            raise AssertionError("fake received an unexpected HTTP call")
        item = script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    def request_json(self, method, url, **kwargs):
        self.calls.append({"kind": "json", "method": method, "url": url, **kwargs})
        self._hook()
        if self.json_error is not None:
            raise self.json_error
        return self._next(self.json_responses)

    def request_file_json(self, method, url, **kwargs):
        call = {"kind": "file", "method": method, "url": url, **kwargs}
        self.calls.append(call)
        self._hook()
        if self.file_error is not None:
            raise self.file_error
        if callable(self.file_callback):
            return self.file_callback(call)
        return self.file_response

    def request(self, *args, **kwargs):
        raise AssertionError("a raw request was made; uploads must stay bounded")

    def request_bytes(self, method, url, **kwargs):
        self.calls.append({"kind": "bytes", "method": method, "url": url, **kwargs})
        self._hook()
        if self.bytes_error is not None:
            raise self.bytes_error
        if self.bytes_response is not None:
            return self.bytes_response
        return self.bytes_payload, "application/octet-stream"


class RemoteTestCase(unittest.TestCase):
    def setUp(self):
        FakeHttpClient.instances = []
        self.manager = FakeTokenManager()
        self.guard = {"real_write_allowed": True, "reason": ""}
        self.guard_calls = 0
        self.delay = patch.object(remote_module, "_READ_RETRY_DELAY_SECONDS", 0.0)
        self.delay.start()
        self.addCleanup(self.delay.stop)
        self.patches = [
            patch.object(remote_module, "_error", side_effect=lambda message, status=400,
                         category="": AssistantError(message, status, category=category)),
            patch.object(remote_module.DocumentLibraryRemote, "_token_manager",
                         staticmethod(lambda: self.manager)),
            patch.object(remote_module.DocumentLibraryRemote, "_http_client",
                         lambda adapter: self._fake_client(adapter)),
            patch("lan_bitable_template_portal.portal_service.external_real_write_guard",
                  self._guard),
        ]
        for item in self.patches:
            item.start()
            self.addCleanup(item.stop)
        self.remote = remote_module.DocumentLibraryRemote(dict(SOURCE))
        self.http = None

    def _fake_client(self, adapter):
        if adapter._http is None:
            adapter._http = FakeHttpClient()
            self.http = adapter._http
        return adapter._http

    def _guard(self):
        self.guard_calls += 1
        return dict(self.guard)

    def use_client(self):
        self.http = FakeHttpClient()
        self.remote._http = self.http
        return self.http

    @property
    def calls(self):
        return self.http.calls if self.http is not None else []

    def assert_no_retry(self):
        for call in self.calls:
            self.assertEqual(call.get("retries"), 0, call)


class SourceValidationTests(RemoteTestCase):
    def test_accepts_a_source_dict(self):
        self.assertEqual(self.remote.base_token, SOURCE["base_token"])
        self.assertEqual(self.remote.table_id, SOURCE["table_id"])
        self.assertEqual(
            self.remote._records_url,
            "https://open.feishu.cn/open-apis/bitable/v1/apps/{0}/tables/{1}".format(
                SOURCE["base_token"], SOURCE["table_id"]),
        )

    def test_rejects_bad_sources_and_bad_ids(self):
        for source in (None, "bascnFixtureBase", [], {},
                       {"base_token": "bad token!", "table_id": SOURCE["table_id"]},
                       {"base_token": SOURCE["base_token"], "table_id": "tbl/../x"},
                       {"base_token": SOURCE["base_token"], "table_id": ""}):
            with self.subTest(source=source):
                with self.assertRaises(AssistantError) as raised:
                    remote_module.DocumentLibraryRemote(source)
                self.assertEqual(raised.exception.category, "invalid_source")

    def test_invalid_record_and_file_tokens_are_rejected_before_http(self):
        for record_id in ("", "rec", "tblNotARecord", "rec/../x", None, 5):
            with self.subTest(record_id=record_id):
                with self.assertRaises(AssistantError) as raised:
                    self.remote.get(record_id)
                self.assertEqual(raised.exception.category, "invalid_record_id")
        with self.assertRaises(AssistantError) as raised:
            self.remote.update("bad id", {"name": "x"})
        self.assertEqual(raised.exception.category, "invalid_record_id")
        for token in ("", "short", "../etc/passwd", "boxcn/with/slash", None):
            with self.subTest(token=token):
                with self.assertRaises(AssistantError) as raised:
                    self.remote.download(token)
                self.assertEqual(raised.exception.category, "invalid_file_token")
        self.assertEqual(self.calls, [])
        self.assertIsNone(self.remote._http)


class ReadTests(RemoteTestCase):
    def test_fields_walks_every_page_and_keeps_types_unchanged(self):
        first = {"field_id": "fldOne", "field_name": "name", "type": 1}
        second = {"field_id": "fldTwo", "field_name": "files", "type": 17,
                  "property": {"multiple": True}}
        self.use_client().json_responses = [
            {"code": 0, "data": {"items": [first], "has_more": True, "page_token": "p2"}},
            {"code": 0, "data": {"items": [second], "has_more": False}},
        ]
        self.assertEqual(self.remote.fields(), [first, second])
        self.assertEqual(self.calls[0]["url"], self.remote._records_url + "/fields")
        self.assertEqual(self.calls[0]["params"], {"page_size": 100})
        self.assertEqual(self.calls[1]["params"], {"page_size": 100, "page_token": "p2"})
        self.assertEqual(self.calls[0]["retries"], 0)

    def test_fields_pagination_rejects_missing_or_repeated_cursor(self):
        self.use_client().json_responses = [
            {"code": 0, "data": {"items": [], "has_more": True}},
        ]
        with self.assertRaises(AssistantError) as raised:
            self.remote.fields()
        self.assertEqual(raised.exception.category, "pagination")
        self.http.json_responses = [
            {"code": 0, "data": {"items": [], "has_more": True, "page_token": "loop"}},
            {"code": 0, "data": {"items": [], "has_more": True, "page_token": "loop"}},
        ]
        with self.assertRaises(AssistantError) as raised:
            self.remote.fields()
        self.assertEqual(raised.exception.category, "pagination")
        self.assertEqual(len(self.calls), 3)

    def test_records_returns_contract_shape_and_validates_input(self):
        page = {"items": [{"record_id": RECORD_ID, "fields": {"name": "doc"}}],
                "has_more": True, "page_token": "next"}
        self.use_client().json_responses = [{"code": 0, "data": page}]
        self.assertEqual(self.remote.records(),
                         {"items": page["items"], "has_more": True, "page_token": "next"})
        self.assertEqual(self.calls[0]["params"], {"page_size": 100})
        self.http.json_responses = [
            {"code": 0, "data": {"items": [], "has_more": False, "page_token": "ignored"}},
        ]
        self.assertEqual(self.remote.records(cursor="cursor-1", page_size=50),
                         {"items": [], "has_more": False, "page_token": ""})
        self.assertEqual(self.calls[1]["params"],
                         {"page_size": 50, "page_token": "cursor-1"})
        for cursor, size in (("x" * 513, 100), (None, 100), ("", 0), ("", 501),
                             ("", True), ("", "100")):
            with self.subTest(cursor=cursor, size=size):
                with self.assertRaises(AssistantError):
                    self.remote.records(cursor=cursor, page_size=size)

    def test_get_returns_record_and_none_only_for_not_found(self):
        record = {"record_id": RECORD_ID, "fields": {"name": "doc"}}
        self.use_client().json_responses = [{"code": 0, "data": {"record": record}}]
        self.assertEqual(self.remote.get(RECORD_ID), record)
        self.assertEqual(self.calls[0]["url"],
                         self.remote._records_url + "/records/" + RECORD_ID)
        self.http.json_responses = [{"code": 1254043, "msg": "RecordIdNotFound"}]
        self.assertIsNone(self.remote.get(RECORD_ID))
        # A malformed code on a read is never mistaken for not-found/success.
        for payload in ({"code": 1254002, "msg": "Record not found"},
                        {"code": 1254302}, {"code": 403, "msg": "forbidden"},
                        {"code": 1254060}, {"code": "1254043"},
                        {"code": "garbage"}):
            with self.subTest(payload=payload):
                self.http.json_responses = [payload]
                with self.assertRaises(AssistantError) as raised:
                    self.remote.get(RECORD_ID)
                expected = ("bad_response" if not isinstance(payload["code"], int)
                            else "provider_error")
                self.assertEqual(raised.exception.category, expected)

    def test_get_rejects_a_mismatched_or_missing_record(self):
        for record in (None, {"record_id": OTHER_RECORD_ID, "fields": {}},
                       {"record_id": "not-a-record", "fields": {}}):
            with self.subTest(record=record):
                self.use_client().json_responses = [{"code": 0, "data": {"record": record}}]
                with self.assertRaises(AssistantError) as raised:
                    self.remote.get(RECORD_ID)
                self.assertEqual(raised.exception.category, "bad_response")

    def test_read_transport_failure_retries_once_then_succeeds(self):
        self.use_client().json_responses = [
            RuntimeError("fixture dropped"),
            {"code": 0, "data": {"items": [], "has_more": False}}]
        self.assertEqual(self.remote.records(), {"items": [], "has_more": False,
                                                 "page_token": ""})
        self.assertEqual(len(self.calls), 2)
        self.assert_no_retry()

    def test_read_transport_failure_stays_bounded_and_safe(self):
        self.use_client().json_error = RuntimeError("fixture secret detail")
        with self.assertRaises(AssistantError) as raised:
            self.remote.records()
        self.assertEqual(raised.exception.status, 503)
        self.assertEqual(raised.exception.category, "transport")
        self.assertNotIn("fixture secret detail", str(raised.exception))
        self.assertEqual(str(raised.exception), "资料库记录读取未完成。")
        self.assertNotIn("tenant-token", str(raised.exception))
        self.assertEqual(len(self.calls), 2)

    def test_malformed_provider_code_fails_closed(self):
        """``code: "garbage"`` must not be coerced to 0 (previous bug)."""
        self.use_client()
        for payload in ({"code": "garbage", "data": {"items": [], "has_more": False}},
                        {"code": None}, {"code": ""}, {"code": 0.0}, {"code": True},
                        {"code": False}, {"code": -1}, {"code": [0]}, {}, []):
            with self.subTest(payload=payload):
                self.http.json_responses = [payload]
                with self.assertRaises(AssistantError) as raised:
                    self.remote.records()
                self.assertEqual(raised.exception.category, "bad_response")
                self.assertEqual(raised.exception.status, 502)
                self.assertEqual(len(self.calls), 1)
                self.calls.clear()

    def test_malformed_code_in_a_read_body_is_not_retried(self):
        """A rejected body whose code is not an int must not look like 0."""
        self.use_client().json_responses = [{"code": "garbage"},
                                            {"code": 0, "data": {"record": {
                                                "record_id": RECORD_ID, "fields": {}}}}]
        with self.assertRaises(AssistantError) as raised:
            self.remote.get(RECORD_ID)
        self.assertEqual(raised.exception.category, "bad_response")
        self.assertEqual(len(self.calls), 1)

    def test_parse_accepts_zero_and_rejects_malformed_codes(self):
        self.assertEqual(self.remote._parse({"code": 0}), {"code": 0})
        for payload in ({"code": "garbage"}, {"code": "0"}, {"code": 0.0},
                        {"code": True}, {"code": -7}, {"msg": "no code"}, None):
            with self.subTest(payload=payload):
                with self.assertRaises(AssistantError) as raised:
                    self.remote._parse(payload)
                self.assertEqual(raised.exception.category, "bad_response")

    def test_single_token_rejection_retry_forces_refresh_for_reads(self):
        self.use_client().json_responses = [
            {"code": 99991663, "msg": "token invalid"},
            {"code": 0, "data": {"items": [], "has_more": False}}]
        self.assertEqual(self.remote.records()["items"], [])
        self.assertEqual(len(self.calls), 2)
        self.assertEqual(self.manager.forced, [False, True])
        self.assertEqual(self.calls[0]["headers"]["Authorization"], "Bearer tenant-token-1")
        self.assertEqual(self.calls[1]["headers"]["Authorization"], "Bearer tenant-token-2")
        self.assert_no_retry()

    def test_repeated_token_rejection_fails_safely(self):
        self.use_client().json_responses = [{"code": 99991663}, {"code": 99991668}]
        with self.assertRaises(AssistantError) as raised:
            self.remote.records()
        self.assertEqual(raised.exception.category, "token_rejected")
        self.assertEqual(raised.exception.status, 401)
        self.assertNotIn("99991", str(raised.exception))
        self.assertEqual(str(raised.exception), "资料库飞书授权被拒绝，请联系管理员。")
        self.assertEqual(len(self.calls), 2)
        self.assertEqual(self.manager.forced, [False, True])


class WriteTests(RemoteTestCase):
    def test_create_uses_a_stable_uuid_client_token(self):
        record = {"record_id": RECORD_ID, "fields": {"name": "doc"}}
        self.use_client().json_responses = [
            {"code": 0, "data": {"record": record}},
            {"code": 0, "data": {"record": record}},
        ]
        fields = {"name": "doc", "files": [{"file_token": FILE_TOKEN}]}
        self.assertEqual(self.remote.create(fields, "op-1"), record)
        self.assertEqual(self.remote.create(dict(fields), "op-1"), record)
        first, second = self.calls
        self.assertEqual(first["method"], "POST")
        self.assertEqual(first["url"], self.remote._records_url + "/records")
        self.assertEqual(first["retries"], 0)
        self.assertEqual(first["json_payload"], {"fields": fields})
        self.assertEqual(first["params"]["client_token"], second["params"]["client_token"])
        self.assertEqual(len(first["params"]["client_token"]), 36)
        self.assertEqual(first["params"]["client_token"][14], "4")
        # The guard runs once in the payload pre-check and once in the
        # request, so two creates pass it exactly four times.
        self.assertEqual(self.guard_calls, 4)

        self.http.json_responses = [{"code": 0, "data": {"record": record}}]
        self.remote.create(fields, "op-2")
        self.assertNotEqual(self.calls[2]["params"]["client_token"],
                            first["params"]["client_token"])

    def test_create_rejects_empty_operation_id_and_empty_fields(self):
        with self.assertRaises(AssistantError) as raised:
            self.remote.create({"name": "doc"}, "")
        self.assertEqual(raised.exception.category, "invalid_operation_id")
        with self.assertRaises(AssistantError) as raised:
            self.remote.create({}, "op-1")
        self.assertEqual(raised.exception.category, "invalid_fields")
        with self.assertRaises(AssistantError):
            self.remote.create({"": "x"}, "op-1")
        self.assertIsNone(self.remote._http)

    def test_update_puts_fields_without_retry(self):
        record = {"record_id": RECORD_ID, "fields": {"name": "doc2"}}
        self.use_client().json_responses = [{"code": 0, "data": {"record": record}}]
        self.assertEqual(self.remote.update(RECORD_ID, {"name": "doc2"}), record)
        call = self.calls[0]
        self.assertEqual(call["method"], "PUT")
        self.assertEqual(call["url"], self.remote._records_url + "/records/" + RECORD_ID)
        self.assertEqual(call["retries"], 0)
        self.assertEqual(call["json_payload"], {"fields": {"name": "doc2"}})

    def test_write_transport_failure_is_never_retried(self):
        self.use_client().json_error = RuntimeError("fixture write uncertain")
        with self.assertRaises(AssistantError) as raised:
            self.remote.update(RECORD_ID, {"name": "doc2"})
        self.assertEqual(raised.exception.category, "transport")
        self.assertNotIn("uncertain", str(raised.exception))
        self.assertEqual(len(self.calls), 1)

    def test_write_token_rejection_is_not_retried_and_not_refreshed(self):
        self.use_client().json_responses = [{"code": 99991663}]
        with self.assertRaises(AssistantError) as raised:
            self.remote.update(RECORD_ID, {"name": "doc2"})
        self.assertEqual(raised.exception.category, "token_rejected")
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(self.manager.forced, [False])

    def test_write_guard_blocks_before_any_http(self):
        self.guard = {"real_write_allowed": False, "reason": "fixture guard reason"}
        with self.assertRaises(AssistantError) as raised:
            self.remote.update(RECORD_ID, {"name": "doc2"})
        self.assertEqual(raised.exception.status, 403)
        self.assertEqual(raised.exception.category, "write_blocked")
        with self.assertRaises(AssistantError):
            self.remote.create({"name": "doc"}, "op-1")
        self.assertIsNone(self.remote._http)
        self.assertEqual(self.manager.issued, 0)

    def test_provider_error_is_reported_without_provider_text(self):
        self.use_client().json_responses = [{"code": 1254060, "msg": "TextFieldConvFail secret-ish"}]
        with self.assertRaises(AssistantError) as raised:
            self.remote.update(RECORD_ID, {"name": "doc2"})
        self.assertEqual(raised.exception.category, "provider_error")
        self.assertEqual(raised.exception.status, 502)
        self.assertEqual(str(raised.exception), "资料库来源请求失败。")
        self.assertNotIn("TextFieldConvFail", str(raised.exception))

    def test_write_with_malformed_code_is_not_treated_as_success(self):
        self.use_client().json_responses = [
            {"code": "garbage", "data": {"record": {"record_id": RECORD_ID}}}]
        with self.assertRaises(AssistantError) as raised:
            self.remote.update(RECORD_ID, {"name": "doc2"})
        self.assertEqual(raised.exception.category, "bad_response")
        self.assertEqual(len(self.calls), 1)


class CredentialTests(RemoteTestCase):
    def test_credential_change_during_token_retrieval_fails_closed(self):
        self.manager.on_token = lambda issued: setattr(
            self.manager, "credentials", ROTATED_CREDENTIALS)
        with self.assertRaises(AssistantError) as raised:
            self.remote.records()
        self.assertEqual(raised.exception.category, "credential_changed")
        self.assertEqual(raised.exception.status, 401)
        self.assertIsNone(self.remote._http)

    def test_credential_rotation_after_token_before_http_sends_nothing(self):
        original = remote_module.DocumentLibraryRemote._token

        def token_then_rotate(adapter, manager, identity, *, force_refresh=False):
            value = original(adapter, manager, identity, force_refresh=force_refresh)
            manager.credentials = ROTATED_CREDENTIALS
            return value

        with patch.object(remote_module.DocumentLibraryRemote, "_token", token_then_rotate):
            with self.assertRaises(AssistantError) as raised:
                self.remote.records()
        self.assertEqual(raised.exception.category, "credential_changed")
        self.assertEqual(self.calls, [])
        self.assertEqual(self.manager.issued, 1)

    def test_credential_rotation_before_a_write_sends_nothing(self):
        def rotate(_issued):
            self.manager.credentials = ROTATED_CREDENTIALS

        for call in (lambda: self.remote.update(RECORD_ID, {"name": "x"}),
                     lambda: self.remote.create({"name": "x"}, "op-1")):
            with self.subTest(call=call):
                self.manager.credentials = FIXTURE_CREDENTIALS
                self.manager.on_token = rotate
                with self.assertRaises(AssistantError) as raised:
                    call()
                self.assertEqual(raised.exception.category, "credential_changed")
        self.assertEqual(self.calls, [])
        self.assertEqual(self.manager.issued, 2)

    def test_credential_change_during_http_fails_closed_and_does_not_retry(self):
        self.use_client()
        self.http.before_call = lambda: setattr(
            self.manager, "credentials", ROTATED_CREDENTIALS)
        self.http.json_responses = [{"code": 0, "data": {"items": [], "has_more": False}}]
        with self.assertRaises(AssistantError) as raised:
            self.remote.records()
        self.assertEqual(raised.exception.category, "credential_changed")
        self.assertEqual(len(self.calls), 1)

    def test_credential_change_after_write_fails_closed(self):
        self.use_client()
        self.http.before_call = lambda: setattr(
            self.manager, "credentials", ROTATED_CREDENTIALS)
        self.http.json_responses = [{"code": 0, "data": {"record": {"record_id": RECORD_ID}}}]
        with self.assertRaises(AssistantError) as raised:
            self.remote.update(RECORD_ID, {"name": "doc2"})
        self.assertEqual(raised.exception.category, "credential_changed")
        self.assertEqual(len(self.calls), 1)

    def test_token_retry_keeps_the_original_identity_reference(self):
        """A refresh returning new credentials must not re-baseline the check.

        The identity captured when the call started stays the reference, so
        a credential swap observed by the forced refresh aborts the call
        instead of being adopted as the new expected pair. ``_check`` is
        wrapped, not replaced, so the real fail-closed comparison still runs.
        """
        self.use_client().json_responses = [
            {"code": 99991663},
            {"code": 0, "data": {"items": [], "has_more": False}}]

        def rotate_on_refresh(issued):
            if issued == 2:
                self.manager.credentials = ROTATED_CREDENTIALS

        self.manager.on_token = rotate_on_refresh
        sent = []
        original = remote_module.DocumentLibraryRemote._check

        def recording_check(adapter, manager, identity):
            sent.append(identity)
            return original(adapter, manager, identity)

        with patch.object(remote_module.DocumentLibraryRemote, "_check", recording_check):
            with self.assertRaises(AssistantError) as raised:
                self.remote.records()
        self.assertEqual(raised.exception.category, "credential_changed")
        self.assertEqual(raised.exception.status, 401)
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(self.manager.forced, [False, True])
        # Checked against the original pair, never the refreshed one.
        self.assertEqual(set(sent), {FIXTURE_CREDENTIALS})
        self.assertGreaterEqual(len(sent), 2)

    def test_retry_still_targets_the_original_pair_after_a_stable_refresh(self):
        """Identity stays fixed across the retry; only the token changes."""
        self.use_client().json_responses = [
            {"code": 99991668}, {"code": 0, "data": {"items": [], "has_more": False}}]
        seen = []
        original = remote_module.DocumentLibraryRemote._check

        def recording_check(adapter, manager, identity):
            seen.append(identity)
            return original(adapter, manager, identity)

        with patch.object(remote_module.DocumentLibraryRemote, "_check", recording_check):
            self.assertEqual(self.remote.records()["items"], [])
        self.assertEqual(set(seen), {FIXTURE_CREDENTIALS})
        self.assertEqual(self.manager.forced, [False, True])
        self.assertEqual(len(self.calls), 2)

    def test_identity_swapped_after_refresh_is_never_adopted(self):
        """Rotation between the forced refresh and the retry must fail closed.

        This is the exact shape of the old defect: the retry re-read the
        credentials after refreshing and continued with the new pair. The
        captured pair must stay the reference, so the second attempt is not
        sent and the rotated identity is never trusted.
        """
        self.use_client().json_responses = [
            {"code": 99991663}, {"code": 0, "data": {"items": [], "has_more": False}}]
        forced_calls = [0]
        original = remote_module.DocumentLibraryRemote._token

        def token_then_rotate(adapter, manager, identity, *, force_refresh=False):
            value = original(adapter, manager, identity, force_refresh=force_refresh)
            if force_refresh:
                forced_calls[0] += 1
                manager.credentials = ROTATED_CREDENTIALS
            return value

        with patch.object(remote_module.DocumentLibraryRemote, "_token", token_then_rotate):
            with self.assertRaises(AssistantError) as raised:
                self.remote.records()
        self.assertEqual(raised.exception.category, "credential_changed")
        self.assertEqual(raised.exception.status, 401)
        self.assertEqual(forced_calls[0], 1)
        self.assertEqual(self.manager.forced, [False, True])
        # Only the original attempt was sent; no request carried a new pair.
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(self.calls[0]["headers"]["Authorization"], "Bearer tenant-token-1")

    def test_missing_credentials_or_token_fail_safely(self):
        self.manager.credentials = ("", "")
        with self.assertRaises(AssistantError) as raised:
            self.remote.fields()
        self.assertEqual(raised.exception.category, "credential_unavailable")
        self.assertIsNone(self.remote._http)

        self.manager.credentials = FIXTURE_CREDENTIALS
        self.manager.fail_token = True
        with self.assertRaises(AssistantError) as raised:
            self.remote.fields()
        self.assertEqual(raised.exception.category, "token_unavailable")
        self.assertNotIn("fixture token backend", str(raised.exception))


class AttachmentTests(RemoteTestCase):
    def setUp(self):
        super().setUp()
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.local = Path(self.tmp.name) / "fixture-upload.bin"
        self.local.write_bytes(b"fixture payload")

    def test_upload_returns_file_token_name_and_size(self):
        self.use_client()
        result = self.remote.upload(str(self.local), "doc.bin")
        self.assertEqual(result, {"file_token": FILE_TOKEN, "name": "doc.bin", "size": 15})
        self.assertEqual(len(self.calls), 1)
        call = self.calls[0]
        self.assertEqual(call["kind"], "file")
        self.assertEqual(call["url"], remote_module.MEDIA_UPLOAD_ALL_URL)
        self.assertEqual(call["retries"], 0)
        self.assertEqual(call["data"]["parent_type"], "bitable_file")
        self.assertEqual(call["data"]["parent_node"], SOURCE["base_token"])
        self.assertEqual(call["data"]["size"], "15")
        self.assertEqual(call["file_path"], str(self.local))
        self.assertEqual(self.guard_calls, 1)

    def test_upload_requires_the_write_guard_and_a_valid_file(self):
        self.guard = {"real_write_allowed": False, "reason": "blocked"}
        with self.assertRaises(AssistantError) as raised:
            self.remote.upload(str(self.local), "doc.bin")
        self.assertEqual(raised.exception.category, "write_blocked")
        self.guard = {"real_write_allowed": True}
        empty = Path(self.tmp.name) / "empty.bin"
        empty.write_bytes(b"")
        for path, name in ((str(empty), "empty.bin"),
                           (str(Path(self.tmp.name) / "missing.bin"), "missing.bin"),
                           (str(self.local), "bad\nname.bin")):
            with self.subTest(path=path, name=name):
                with self.assertRaises(AssistantError) as raised:
                    self.remote.upload(path, name)
                self.assertEqual(raised.exception.category, "upload_source")
        self.assertIsNone(self.remote._http)

    def test_upload_is_never_retried(self):
        self.use_client().file_error = RuntimeError("fixture upload uncertain")
        with self.assertRaises(AssistantError) as raised:
            self.remote.upload(str(self.local), "doc.bin")
        self.assertEqual(raised.exception.category, "transport")
        self.assertNotIn("uncertain", str(raised.exception))
        self.assertEqual(len(self.calls), 1)

    def test_upload_without_a_file_token_is_an_error(self):
        self.use_client().file_response = {"code": 0, "data": {}}
        with self.assertRaises(AssistantError) as raised:
            self.remote.upload(str(self.local), "doc.bin")
        self.assertEqual(raised.exception.category, "bad_response")

    def test_upload_token_rejection_is_not_refreshed_or_retried(self):
        self.use_client().file_response = {"code": 99991663}
        with self.assertRaises(AssistantError) as raised:
            self.remote.upload(str(self.local), "doc.bin")
        self.assertEqual(raised.exception.category, "token_rejected")
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(self.manager.forced, [False])

    def test_download_uses_the_fixed_endpoint_and_returns_bytes(self):
        self.use_client()
        self.assertEqual(self.remote.download(FILE_TOKEN), b"fixture-bytes")
        call = self.calls[0]
        self.assertEqual(
            call["url"],
            "https://open.feishu.cn/open-apis/drive/v1/medias/{0}/download".format(FILE_TOKEN))
        self.assertEqual(call["retries"], 0)
        self.assertEqual(call["max_bytes"], remote_module.MAX_DOWNLOAD_BYTES + 1)
        self.assertEqual(self.guard_calls, 0)

    def test_download_is_bounded_to_one_hundred_mib(self):
        self.use_client().bytes_payload = b"x" * (remote_module.MAX_DOWNLOAD_BYTES + 1)
        with self.assertRaises(AssistantError) as raised:
            self.remote.download(FILE_TOKEN)
        self.assertEqual(raised.exception.category, "too_large")
        self.http.bytes_error = RuntimeError("fixture download lost")
        with self.assertRaises(AssistantError) as raised:
            self.remote.download(FILE_TOKEN)
        self.assertEqual(raised.exception.category, "transport")


class LargeUploadTests(RemoteTestCase):
    """Files above the 20 MiB ``upload_all`` limit use the parts protocol."""

    BLOCK = 1024 * 1024

    def setUp(self):
        super().setUp()
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.size = remote_module.UPLOAD_ALL_MAX_BYTES + 2048
        self.payload = (b"0123456789abcdef" * (self.size // 16)) + b"Z" * (self.size % 16)
        self.assertEqual(len(self.payload), self.size)
        self.local = Path(self.tmp.name) / "fixture-large.bin"
        self.local.write_bytes(self.payload)
        self.parts = []

    def prepare(self, *, upload_id=UPLOAD_ID, block_size=BLOCK, block_num=None):
        blocks = block_num if block_num is not None else -(-self.size // block_size)
        return {"code": 0, "data": {"upload_id": upload_id,
                                    "block_size": block_size, "block_num": blocks}}

    def script(self, prepare=None, finish=None):
        """Queue: prepare, one answer per part, finish (parts echo by seq)."""
        self.prepared = prepare if prepare is not None else self.prepare()
        blocks = _blocks(self.prepared) or 1
        self.http = self.use_client()
        self.http.json_responses = [self.prepared]
        self.finish_response = finish or {"code": 0, "data": {"file_token": PART_FILE_TOKEN}}

        def on_file(call):
            # The part file is deleted with the scratch dir, so snapshot it now.
            call["body"] = Path(call["file_path"]).read_bytes()
            self.parts.append(call)
            if len(self.parts) == blocks:
                self.http.json_responses = [self.finish_response]
            return {"code": 0, "data": {}}

        self.http.file_callback = on_file

    def map_calls(self):
        kinds = []
        for call in self.calls:
            if call["url"] == remote_module.MEDIA_UPLOAD_PREPARE_URL:
                kinds.append("prepare")
            elif call["url"] == remote_module.MEDIA_UPLOAD_PART_URL:
                kinds.append(("part", call["data"]["seq"]))
            elif call["url"] == remote_module.MEDIA_UPLOAD_FINISH_URL:
                kinds.append("finish")
            else:
                kinds.append(call["url"])
        return kinds

    def test_large_upload_uses_prepare_parts_and_finish(self):
        self.script()
        result = self.remote.upload(str(self.local), "large.bin")
        self.assertEqual(result, {"file_token": PART_FILE_TOKEN, "name": "large.bin",
                                  "size": self.size})
        blocks = -(-self.size // self.BLOCK)
        self.assertEqual(blocks, 21)
        self.assertEqual(self.map_calls(),
                         ["prepare"] + [("part", str(seq)) for seq in range(blocks)]
                         + ["finish"])
        self.assertNotIn(remote_module.MEDIA_UPLOAD_ALL_URL,
                         [call["url"] for call in self.calls])
        self.assert_no_retry()
        prepare = self.calls[0]
        self.assertEqual(prepare["kind"], "json")
        self.assertEqual(prepare["json_payload"],
                         {"file_name": "large.bin", "parent_type": "bitable_file",
                          "parent_node": SOURCE["base_token"], "size": self.size})
        seen = b""
        for seq, call in enumerate(self.parts):
            expected = remote_module._part_length(self.size, self.BLOCK, seq)
            body = call["body"]
            self.assertEqual(len(body), expected)
            self.assertLessEqual(len(body), self.BLOCK)
            self.assertEqual(call["data"]["upload_id"], UPLOAD_ID)
            self.assertEqual(call["data"]["seq"], str(seq))
            self.assertEqual(call["data"]["size"], str(expected))
            self.assertEqual(call["data"]["checksum"], str(zlib.adler32(body) & 0xFFFFFFFF))
            seen += body
        self.assertEqual(seen, self.payload)
        self.assertEqual(self.calls[-1]["kind"], "json")
        self.assertEqual(self.calls[-1]["json_payload"],
                         {"upload_id": UPLOAD_ID, "block_num": blocks})
        self.assertEqual(self.guard_calls, 1)

    def test_part_files_are_removed_afterwards(self):
        self.script()
        self.remote.upload(str(self.local), "large.bin")
        part_paths = {call["file_path"] for call in self.parts}
        self.assertEqual(len(part_paths), 1)
        self.assertFalse(os.path.exists(part_paths.pop()))

    def test_invalid_prepare_layout_fails_closed_without_uploading_a_part(self):
        cases = {
            "missing upload id": {"upload_id": "", "block_size": self.BLOCK,
                                  "block_num": -(-self.size // self.BLOCK)},
            "bad upload id": {"upload_id": "../escape", "block_size": self.BLOCK,
                              "block_num": -(-self.size // self.BLOCK)},
            "zero block size": {"upload_id": UPLOAD_ID, "block_size": 0,
                                "block_num": 1},
            "oversized block": {"upload_id": UPLOAD_ID, "block_size": 32 * 1024 * 1024,
                                "block_num": 1},
            "block size as str": {"upload_id": UPLOAD_ID, "block_size": str(self.BLOCK),
                                  "block_num": -(-self.size // self.BLOCK)},
            "block count too small": {"upload_id": UPLOAD_ID, "block_size": self.BLOCK,
                                      "block_num": 1},
            "block count too large": {"upload_id": UPLOAD_ID, "block_size": self.BLOCK,
                                      "block_num": 99},
            "block count bool": {"upload_id": UPLOAD_ID, "block_size": self.BLOCK,
                                 "block_num": True},
            "empty data": {},
        }
        for label, data in cases.items():
            with self.subTest(label):
                self.parts = []
                self.http = self.use_client()
                self.http.json_responses = [{"code": 0, "data": data}]
                with self.assertRaises(AssistantError) as raised:
                    self.remote.upload(str(self.local), "large.bin")
                self.assertEqual(raised.exception.category, "bad_response")
                self.assertEqual(self.parts, [])
                self.assertEqual(self.map_calls(), ["prepare"])
                self.remote._http = None

    def test_prepare_provider_error_stops_before_any_part(self):
        self.http = self.use_client()
        self.http.json_responses = [{"code": 1061045, "msg": "prepare rejected"}]
        with self.assertRaises(AssistantError) as raised:
            self.remote.upload(str(self.local), "large.bin")
        self.assertEqual(raised.exception.category, "provider_error")
        self.assertNotIn("prepare rejected", str(raised.exception))
        self.assertEqual(self.map_calls(), ["prepare"])

    def test_prepare_malformed_code_is_not_success(self):
        self.http = self.use_client()
        self.http.json_responses = [{"code": "garbage", "data": {"upload_id": UPLOAD_ID}}]
        with self.assertRaises(AssistantError) as raised:
            self.remote.upload(str(self.local), "large.bin")
        self.assertEqual(raised.exception.category, "bad_response")
        self.assertEqual(self.map_calls(), ["prepare"])

    def test_part_failure_stops_and_never_restarts_the_upload(self):
        self.script()

        def fail_on_third(call):
            self.parts.append(call)
            if len(self.parts) == 3:
                raise RuntimeError("fixture part dropped")
            return {"code": 0, "data": {}}

        self.http.file_callback = fail_on_third
        with self.assertRaises(AssistantError) as raised:
            self.remote.upload(str(self.local), "large.bin")
        self.assertEqual(raised.exception.category, "transport")
        self.assertEqual(raised.exception.status, 503)
        self.assertNotIn("fixture part dropped", str(raised.exception))
        self.assertEqual(self.map_calls(),
                         ["prepare", ("part", "0"), ("part", "1"), ("part", "2")])
        self.assertEqual(self.manager.issued, 1)
        self.assert_no_retry()

    def test_part_provider_error_stops_without_finish(self):
        self.script()

        def reject_second(call):
            self.parts.append(call)
            if len(self.parts) == 2:
                return {"code": 1061046, "msg": "part rejected"}
            return {"code": 0, "data": {}}

        self.http.file_callback = reject_second
        with self.assertRaises(AssistantError) as raised:
            self.remote.upload(str(self.local), "large.bin")
        self.assertEqual(raised.exception.category, "provider_error")
        self.assertEqual(self.map_calls(),
                         ["prepare", ("part", "0"), ("part", "1")])
        self.assertEqual(self.manager.issued, 1)

    def test_finish_failure_never_restarts_any_part(self):
        self.script(finish={"code": 1061047, "msg": "finish rejected"})
        with self.assertRaises(AssistantError) as raised:
            self.remote.upload(str(self.local), "large.bin")
        self.assertEqual(raised.exception.category, "provider_error")
        self.assertEqual(self.map_calls()[-1], "finish")
        self.assertEqual(len(self.parts), 21)
        self.assertEqual(sum(1 for kind in self.map_calls() if kind == "prepare"), 1)

    def test_finish_without_file_token_fails_closed(self):
        self.script(finish={"code": 0, "data": {}})
        with self.assertRaises(AssistantError) as raised:
            self.remote.upload(str(self.local), "large.bin")
        self.assertEqual(raised.exception.category, "bad_response")

    def test_token_rejection_on_any_part_is_not_retried(self):
        self.script()

        def reject_first(call):
            self.parts.append(call)
            return {"code": 99991663}

        self.http.file_callback = reject_first
        with self.assertRaises(AssistantError) as raised:
            self.remote.upload(str(self.local), "large.bin")
        self.assertEqual(raised.exception.category, "token_rejected")
        self.assertEqual(raised.exception.status, 401)
        self.assertEqual(self.map_calls(), ["prepare", ("part", "0")])
        self.assertEqual(self.manager.forced, [False])
        self.assertEqual(self.manager.issued, 1)

    def test_credential_change_mid_parts_fails_closed_and_stops(self):
        self.script()

        def rotate_on_second(call):
            self.parts.append(call)
            if len(self.parts) == 2:
                self.manager.credentials = ROTATED_CREDENTIALS
            return {"code": 0, "data": {}}

        self.http.file_callback = rotate_on_second
        with self.assertRaises(AssistantError) as raised:
            self.remote.upload(str(self.local), "large.bin")
        self.assertEqual(raised.exception.category, "credential_changed")
        self.assertEqual(raised.exception.status, 401)
        self.assertEqual(self.map_calls(), ["prepare", ("part", "0"), ("part", "1")])
        self.assertEqual(self.manager.issued, 1)
        self.assert_no_retry()

    def test_credential_change_before_finish_blocks_the_finish(self):
        self.script()

        def rotate_on_last_part(call):
            self.parts.append(call)
            if len(self.parts) == 21:
                self.manager.credentials = ROTATED_CREDENTIALS
            return {"code": 0, "data": {}}

        self.http.file_callback = rotate_on_last_part
        with self.assertRaises(AssistantError) as raised:
            self.remote.upload(str(self.local), "large.bin")
        self.assertEqual(raised.exception.category, "credential_changed")
        self.assertNotIn("finish", self.map_calls())
        self.assertEqual(len(self.parts), 21)

    def test_credential_rotation_before_a_large_upload_sends_nothing(self):
        self.use_client()
        self.manager.on_token = lambda _issued: setattr(
            self.manager, "credentials", ROTATED_CREDENTIALS)
        with self.assertRaises(AssistantError) as raised:
            self.remote.upload(str(self.local), "large.bin")
        self.assertEqual(raised.exception.category, "credential_changed")
        self.assertEqual(self.calls, [])

    def test_write_guard_blocks_a_large_upload_before_http(self):
        self.guard = {"real_write_allowed": False, "reason": "blocked"}
        with self.assertRaises(AssistantError) as raised:
            self.remote.upload(str(self.local), "large.bin")
        self.assertEqual(raised.exception.category, "write_blocked")
        self.assertIsNone(self.remote._http)

    def test_upload_exactly_at_the_limit_still_uses_upload_all(self):
        self.local.write_bytes(b"x")
        with patch.object(remote_module, "UPLOAD_ALL_MAX_BYTES", 1):
            self.use_client()
            self.remote.upload(str(self.local), "edge.bin")
        self.assertEqual([call["url"] for call in self.calls],
                         [remote_module.MEDIA_UPLOAD_ALL_URL])

    def test_one_byte_over_the_limit_uses_parts(self):
        self.local.write_bytes(b"x")
        with patch.object(remote_module, "UPLOAD_ALL_MAX_BYTES", 0):
            self.http = self.use_client()
            prepare = {"code": 0,
                       "data": {"upload_id": UPLOAD_ID, "block_size": 1, "block_num": 1}}
            self.http.json_responses = [prepare]
            self.http.file_callback = lambda call: (
                self.http.json_responses.append({"code": 0,
                                                 "data": {"file_token": PART_FILE_TOKEN}})
                or {"code": 0, "data": {}})
            self.remote.upload(str(self.local), "edge.bin")
        self.assertNotIn(remote_module.MEDIA_UPLOAD_ALL_URL,
                         [call["url"] for call in self.calls])

    def test_large_upload_larger_than_the_hard_ceiling_is_refused(self):
        with patch.object(remote_module, "MAX_UPLOAD_BYTES", self.size - 1):
            with self.assertRaises(AssistantError) as raised:
                self.remote.upload(str(self.local), "large.bin")
        self.assertEqual(raised.exception.category, "upload_source")
        self.assertEqual(raised.exception.status, 413)
        self.assertIsNone(self.remote._http)


def _blocks(prepare):
    try:
        return prepare["data"]["block_num"]
    except (KeyError, TypeError):
        return None


class LifecycleTests(RemoteTestCase):
    def test_close_is_idempotent_and_blocks_later_calls(self):
        self.use_client()
        self.remote.close()
        self.remote.close()
        self.assertEqual(self.http.closed, 1)
        with self.assertRaises(AssistantError) as raised:
            self.remote.records()
        self.assertEqual(raised.exception.category, "closed")

    def test_http_client_is_created_lazily_and_reused(self):
        self.assertIsNone(self.remote._http)
        self.use_client().json_responses = [
            {"code": 0, "data": {"items": [], "has_more": False}},
            {"code": 0, "data": {"items": [], "has_more": False}},
        ]
        self.remote.records()
        self.remote.records()
        self.assertIs(self.remote._http, self.http)
        self.assertEqual(len(self.calls), 2)


class ModuleImportTests(unittest.TestCase):
    def test_adapter_import_pulls_no_transport_token_manager_or_config(self):
        import importlib
        import subprocess

        probe = (
            "import sys\n"
            "sys.path.insert(0, {bin_dir!r})\n"
            "from lan_bitable_template_portal import document_library_remote\n"
            "names = ('httpx', 'upload_event_module.config',\n"
            "         'upload_event_module.services.feishu_token_manager',\n"
            "         'lan_bitable_template_portal.portal_service')\n"
            "print('|'.join(name for name in names if name in sys.modules))\n"
        ).format(bin_dir=str(Path(__file__).resolve().parent))
        result = subprocess.run(
            [sys.executable, "-B", "-c", probe], capture_output=True, text=True,
            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "")
        # The adapter also stays side-effect free when reloaded in-process.
        importlib.reload(remote_module)

    def test_no_unbounded_raw_upload_branch_remains(self):
        with open(remote_module.__file__, "r", encoding="utf-8") as source:
            text = source.read()
        self.assertNotIn("_UPLOAD_STREAM_BYTES", text)
        self.assertNotIn(".request(", text)
        self.assertNotIn("files=", text)


if __name__ == "__main__":
    unittest.main()
