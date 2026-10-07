# -*- coding: utf-8 -*-
"""Focused isolated tests for PortalAPICatalog.read_operation URL normalization.

Covers valid query-string + path-param resolution, method case-insensitivity,
and rejection of external URLs, fragments, raw/encoded backslashes, control
characters, single- and double-encoded traversal, duplicate query keys,
conflicting params/path_params and write routes.  Also verifies malformed URL
parsing is surfaced as AssistantError (never a raw ValueError), input
immutability, and that auth-spoof query args survive normalisation only to be
rejected by the downstream validator.
"""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from fastapi import FastAPI, Request

from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog
from lan_bitable_template_portal.lighthouse_ai import AssistantError


def _read_url_catalog():
    app = FastAPI()

    @app.get("/api/repair-management/records")
    async def _records(request: Request):
        return {"ok": True}

    @app.get("/api/records/{record_id}")
    async def _record_detail(request: Request, record_id: str):
        return {"ok": True}

    @app.get("/api/drills/{drill_id}/execution")
    async def _drill_execution(request: Request, drill_id: str):
        return {"ok": True}

    @app.post("/api/records")
    async def _create_record(request: Request):
        return {"ok": True}

    return PortalAPICatalog(app)


class ReadUrlNormalizationTests(unittest.TestCase):
    def setUp(self):
        self.catalog = _read_url_catalog()

    # -- valid resolution ---------------------------------------------------

    def test_query_url_normalizes_to_exact_route_plus_params(self):
        out = self.catalog.read_operation({"api_id": "GET /api/repair-management/records?scope=A&page=2"})
        self.assertEqual(out["api_id"], "GET /api/repair-management/records")
        self.assertEqual(out["params"], {"scope": "A", "page": "2"})

    def test_lowercase_method_normalizes(self):
        out = self.catalog.read_operation({"api_id": "get /api/repair-management/records?scope=A"})
        self.assertEqual(out["api_id"], "GET /api/repair-management/records")
        self.assertEqual(out["params"], {"scope": "A"})

    def test_concrete_path_placeholder_resolves(self):
        out = self.catalog.read_operation({"api_id": "GET /api/records/abc123"})
        self.assertEqual(out["api_id"], "GET /api/records/{record_id}")
        self.assertEqual(out["path_params"], {"record_id": "abc123"})

    def test_concrete_path_placeholder_with_query_resolves(self):
        out = self.catalog.read_operation({"api_id": "GET /api/drills/d1/execution?scope=A&page=1"})
        self.assertEqual(out["api_id"], "GET /api/drills/{drill_id}/execution")
        self.assertEqual(out["path_params"], {"drill_id": "d1"})
        self.assertEqual(out["params"], {"scope": "A", "page": "1"})

    def test_query_merges_with_existing_nonconflicting_params(self):
        out = self.catalog.read_operation({
            "api_id": "GET /api/repair-management/records?scope=A",
            "params": {"page": "2"},
        })
        self.assertEqual(out["params"], {"scope": "A", "page": "2"})

    def test_exact_registered_id_is_returned_unchanged(self):
        op = {"api_id": "GET /api/repair-management/records", "params": {"page": "3"}}
        self.assertIs(self.catalog.read_operation(op), op)

    def test_existing_matching_path_param_is_not_conflict(self):
        out = self.catalog.read_operation({
            "api_id": "GET /api/records/abc123",
            "path_params": {"record_id": "abc123"},
        })
        self.assertEqual(out["path_params"], {"record_id": "abc123"})

    # -- conflict / ambiguity rejection ------------------------------------

    def test_repeated_query_key_is_rejected(self):
        with self.assertRaises(AssistantError):
            self.catalog.read_operation({"api_id": "GET /api/repair-management/records?scope=A&scope=B"})

    def test_conflicting_query_param_is_rejected(self):
        with self.assertRaises(AssistantError):
            self.catalog.read_operation({
                "api_id": "GET /api/repair-management/records?scope=A",
                "params": {"scope": "B"},
            })

    def test_conflicting_path_param_is_rejected(self):
        with self.assertRaises(AssistantError):
            self.catalog.read_operation({
                "api_id": "GET /api/records/abc123",
                "path_params": {"record_id": "other"},
            })

    def test_query_key_repeating_path_param_is_rejected(self):
        with self.assertRaises(AssistantError):
            self.catalog.read_operation({"api_id": "GET /api/records/abc123?record_id=xyz"})

    # -- safety / route bounds ---------------------------------------------

    def test_external_absolute_url_is_rejected(self):
        with self.assertRaises(AssistantError):
            self.catalog.read_operation({"api_id": "GET http://evil.example/api/records/abc123"})

    def test_protocol_relative_host_is_rejected(self):
        with self.assertRaises(AssistantError):
            self.catalog.read_operation({"api_id": "GET //evil.example/api/records/abc123"})

    def test_fragment_is_rejected(self):
        with self.assertRaises(AssistantError):
            self.catalog.read_operation({"api_id": "GET /api/records/abc123#frag"})

    def test_backslash_is_rejected(self):
        with self.assertRaises(AssistantError):
            self.catalog.read_operation({"api_id": "GET /api/records\\abc123"})

    def test_encoded_path_traversal_is_rejected(self):
        with self.assertRaises(AssistantError):
            self.catalog.read_operation({"api_id": "GET /api/records/%2e%2e"})

    def test_write_route_is_not_normalized(self):
        with self.assertRaises(AssistantError):
            self.catalog.read_operation({"api_id": "POST /api/records"})

    def test_unknown_route_is_an_error(self):
        with self.assertRaises(AssistantError):
            self.catalog.read_operation({"api_id": "GET /api/does-not-exist"})

    def test_non_local_path_is_rejected(self):
        with self.assertRaises(AssistantError):
            self.catalog.read_operation({"api_id": "GET /other/path"})

    # -- malformed URL parsing is surfaced as AssistantError ----------------

    def test_malformed_url_never_leaks_raw_value_error(self):
        with self.assertRaises(AssistantError):
            self.catalog.read_operation({"api_id": "GET https://[bad"})

    # -- local safe decoded path enforcement ---------------------------------

    def test_percent_encoded_backslash_path_is_rejected(self):
        with self.assertRaises(AssistantError):
            self.catalog.read_operation({"api_id": "GET /api/records/%5cfoo"})

    def test_double_encoded_traversal_is_rejected(self):
        with self.assertRaises(AssistantError):
            self.catalog.read_operation({"api_id": "GET /api/records/%252e%252e"})

    def test_encoded_control_character_is_rejected(self):
        with self.assertRaises(AssistantError):
            self.catalog.read_operation({"api_id": "GET /api/records/%0a"})

    def test_valid_encoded_path_param_still_resolves(self):
        out = self.catalog.read_operation({"api_id": "GET /api/records/abc%20def"})
        self.assertEqual(out["api_id"], "GET /api/records/{record_id}")
        self.assertEqual(out["path_params"], {"record_id": "abc%20def"})

    # -- immutability and downstream validation ------------------------------

    def test_read_operation_does_not_mutate_input(self):
        op = {"api_id": "GET /api/records/abc123?scope=A", "params": {"page": "2"}}
        before = {
            "api_id": op["api_id"],
            "params": dict(op["params"]),
        }
        self.catalog.read_operation(op)
        self.assertEqual(op["api_id"], before["api_id"])
        self.assertEqual(op["params"], before["params"])
        self.assertNotIn("path_params", op)

    def test_downstream_auth_spoof_query_is_rejected(self):
        normalized = self.catalog.read_operation({"api_id": "GET /api/repair-management/records?is_admin=true"})
        self.assertEqual(normalized["params"]["is_admin"], "true")
        with self.assertRaises(AssistantError):
            self.catalog.validate_operation(normalized)


if __name__ == "__main__":
    unittest.main()