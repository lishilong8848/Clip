"""Offline integration checks for existing grants on new workspace extensions."""
import copy
import hashlib
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

BIN = Path(__file__).resolve().parent
if str(BIN) not in sys.path:
    sys.path.insert(0, str(BIN))

import httpx
from fastapi import FastAPI, Request
from lan_bitable_template_portal.account_policy import default_modules
from lan_bitable_template_portal.document_library import DocumentLibraryService
from lan_bitable_template_portal.document_library_routes import install_document_library_routes
from lan_bitable_template_portal.module_access import (
    MODULE_IDS, PREFIXES, allowed_modules, module_denial, required_modules,
)
from lan_bitable_template_portal.portal_auth import PortalAuthManager
from lan_bitable_template_portal.release_journal import ReleaseJournalStore
from lan_bitable_template_portal.release_journal_routes import install_release_journal_routes
from openclaw_service.assistant.lighthouse_ai import AssistantError
from public_access.config import Settings
from public_access.ingress import ANONYMOUS, API_ROOTS, MARKER, SIGNED, PublicGuard
from upload_event_module.services.release_journal import write_json


DOCUMENT_PATHS = (
    "/api/document-library", "/api/document-library/tables",
    "/document-library", "/document-library/",
)
ENTRY_ID = hashlib.sha256(b"workspace-extension-fixture").hexdigest()
JOURNAL_PATHS = ("/api/release-journal", "/api/release-journal/" + ENTRY_ID)


class WorkspaceExtensionPolicyTests(unittest.TestCase):
    def test_new_prefixes_require_only_the_existing_module(self):
        for prefix, module in (("/api/document-library", "knowledge"),
                               ("/document-library", "knowledge"),
                               ("/api/release-journal", "accounts")):
            for suffix in ("", "/", "/fixture/nested"):
                with self.subTest(path=prefix + suffix):
                    self.assertEqual(required_modules(prefix + suffix), {module})

    def test_prefix_matching_keeps_path_boundaries(self):
        for path in ("/api/document-library-other", "/document-library-other",
                     "/api/release-journal-other"):
            with self.subTest(path=path):
                self.assertEqual(required_modules(path), frozenset())

    def test_public_api_roots_include_both_without_anonymous_or_signed_bypass(self):
        derived = {prefix.split("/")[2] for prefix, _ in PREFIXES
                   if prefix.startswith("/api/") and not prefix.endswith("-")}
        self.assertTrue({"document-library", "release-journal"} <= derived)
        self.assertTrue(derived <= API_ROOTS)
        for path in (*DOCUMENT_PATHS[:2], *JOURNAL_PATHS):
            for method in ("GET", "POST", "PUT", "PATCH", "DELETE"):
                with self.subTest(method=method, path=path):
                    self.assertNotIn((method, path), ANONYMOUS | SIGNED)

    def test_extensions_do_not_add_modules_or_accounts_to_base_defaults(self):
        self.assertTrue({"knowledge", "accounts"} <= MODULE_IDS)
        self.assertTrue({"document_library", "release_journal",
                         "document-library", "release-journal"}.isdisjoint(MODULE_IDS))
        self.assertEqual(set(default_modules("L4")), MODULE_IDS - {"accounts"})


class WorkspaceExtensionAccessTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        # The event loop already exists; ASGI requests need no network sockets.
        for target in ("socket.socket.connect", "socket.socket.connect_ex", "socket.getaddrinfo"):
            self.enterContext(patch(target, side_effect=AssertionError("Network is forbidden")))
        root = self.enterContext(tempfile.TemporaryDirectory(prefix="workspace-access-"))
        self.identity = "ou_WorkspaceFixture"
        self.profile = {"role": "building", "level": "L4", "modules": ["assistant"],
                        "scope_rules": ["A"]}
        self.session = {"user": {"open_id": self.identity}, "role": "admin", "is_admin": True,
                        "modules": ["knowledge", "accounts"]}
        self.reader = Mock(side_effect=lambda request: self.session)
        self.auth = SimpleNamespace(account_profile=Mock(side_effect=lambda identity: copy.deepcopy(self.profile)))
        self.auth.is_admin = Mock(side_effect=lambda session: PortalAuthManager.is_admin(self.auth, session))
        controller = SimpleNamespace(_current_session=self.reader)
        runtime = SimpleNamespace(auth_manager=self.auth)
        self.remote = Mock(side_effect=AssertionError("No document provider may be created"))
        self.library = DocumentLibraryService(root, remote_factory=self.remote)
        self.library.tables = Mock(wraps=self.library.tables)
        self.get_library = Mock(return_value=self.library)
        self.journal = ReleaseJournalStore(root)
        self.entry = {"id": ENTRY_ID, "version": "ClipFlow_workspace_fixture", "patch": 1,
                      "published_at": "2026-10-20T12:00:00+00:00", "commits": [],
                      "items": [{"problem": "Fixture issue", "solution": "Fixture fix", "module": "accounts"}]}
        write_json(self.journal.path, {"schema": 1, "entries": [self.entry]})
        self.journal.entries = Mock(wraps=self.journal.entries)
        self.probes = []
        app = FastAPI()

        @app.middleware("http")
        async def module_guard(request, call_next):
            denied = await module_denial(request, controller, runtime)
            return denied if denied is not None else await call_next(request)

        async def authorize(request):
            session = controller._current_session(request)
            if not session:
                raise AssistantError("Login required.", 401)
            return {"id": session["user"]["open_id"], "role": session.get("role"),
                    "is_guest": session.get("is_guest", False),
                    "modules": sorted(allowed_modules(self.auth, session)), "scopes": ["A"],
                    "is_admin": self.auth.is_admin(session)}

        install_document_library_routes(app, authorize, self.get_library)
        install_release_journal_routes(app, controller, runtime, store=self.journal)

        # Policy probes exercise the prefix root and HTML shell without frontend assets.
        @app.get("/api/document-library")
        @app.get("/document-library")
        @app.get("/document-library/")
        async def shell(request: Request):
            self.probes.append(request.url.path)
            return {"ok": True}

        settings = Settings(enabled=True, public_url="https://workspace.example.com",
                            server_addr="192.0.2.2", token="fixture-only-token")
        marker = "workspace-fixture-marker"
        public = PublicGuard(app, marker, settings, self.reader)
        self.clients = {
            "lan": httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test"),
            "public": httpx.AsyncClient(
                transport=httpx.ASGITransport(app=public, client=("127.0.0.1", 1)),
                base_url=settings.public_url, headers={MARKER.decode(): marker}),
        }
        for client in self.clients.values():
            self.addAsyncCleanup(client.aclose)
        self.addCleanup(self.remote.assert_not_called)

    def assert_no_extension_reads(self):
        self.get_library.assert_not_called()
        self.library.tables.assert_not_called()
        self.journal.entries.assert_not_called()
        self.assertEqual(self.probes, [])

    async def test_ordinary_assignment_is_not_expanded_by_extensions_or_session_flags(self):
        original = copy.deepcopy(self.profile)
        for name, client in self.clients.items():
            for path in (*DOCUMENT_PATHS, *JOURNAL_PATHS):
                with self.subTest(ingress=name, path=path):
                    response = await client.get(path)
                    self.assertEqual(response.status_code, 403, response.text)
                    self.assertEqual(response.json()["error_code"], "module_access_denied")
        self.assertEqual(allowed_modules(self.auth, self.session), {"assistant"})
        self.assertEqual(self.profile, original)
        self.assert_no_extension_reads()

    async def test_knowledge_grant_opens_documents_but_not_release_journal(self):
        self.profile["modules"] = ["knowledge"]
        for name, client in self.clients.items():
            for path in DOCUMENT_PATHS:
                with self.subTest(ingress=name, path=path):
                    response = await client.get(path)
                    self.assertEqual(response.status_code, 200, response.text)
                    if path.endswith("/tables"):
                        self.assertEqual(response.json()["data"], {"items": [], "can_configure": False})
            for path in JOURNAL_PATHS:
                self.assertEqual((await client.get(path)).status_code, 403)
        self.assertEqual(self.library.tables.call_count, 2)
        self.journal.entries.assert_not_called()
        self.assertEqual(allowed_modules(self.auth, self.session), {"knowledge"})

    async def test_accounts_grant_is_not_knowledge_or_administrator_authority(self):
        self.profile["modules"] = ["accounts"]
        for name, client in self.clients.items():
            for path in DOCUMENT_PATHS:
                self.assertEqual((await client.get(path)).status_code, 403)
            for path in JOURNAL_PATHS:
                with self.subTest(ingress=name, path=path):
                    response = await client.get(path)
                    self.assertEqual(response.status_code, 403, response.text)
                    self.assertNotIn("error_code", response.json())
                    self.assertIn("no-store", response.headers["cache-control"])
        self.assertEqual(self.auth.is_admin.call_count, 4)
        self.assert_no_extension_reads()

    async def test_admin_profile_still_requires_the_assigned_module(self):
        self.profile["role"] = "admin"
        for client in self.clients.values():
            for path in (*DOCUMENT_PATHS, *JOURNAL_PATHS):
                response = await client.get(path)
                self.assertEqual(response.status_code, 403)
                self.assertEqual(response.json()["error_code"], "module_access_denied")
        self.auth.is_admin.assert_not_called()
        self.assert_no_extension_reads()

    async def test_admin_with_accounts_reads_real_journal_without_knowledge_access(self):
        self.profile.update(role="admin", modules=["accounts"])
        for name, client in self.clients.items():
            for path in JOURNAL_PATHS:
                with self.subTest(ingress=name, path=path):
                    response = await client.get(path)
                    self.assertEqual(response.status_code, 200, response.text)
                    data = response.json()["data"]
                    self.assertEqual(data if path != JOURNAL_PATHS[0] else data["items"][0], self.entry)
                    self.assertEqual(response.headers["cache-control"], "private, no-store")
                    self.assertEqual(response.headers["vary"], "Cookie")
            for path in DOCUMENT_PATHS:
                self.assertEqual((await client.get(path)).status_code, 403)
        self.assertEqual(self.journal.entries.call_count, 4)
        self.get_library.assert_not_called()
        self.assertEqual(self.probes, [])

    async def test_public_anonymous_apis_require_login_before_any_data_read(self):
        self.session = None
        for path in (*DOCUMENT_PATHS[:2], *JOURNAL_PATHS):
            with self.subTest(path=path):
                response = await self.clients["public"].get(path)
                self.assertEqual(response.status_code, 401, response.text)
                self.assertTrue(response.json()["auth_required"])
        self.assert_no_extension_reads()

    async def test_lan_anonymous_real_routes_require_login(self):
        self.session = None
        for path in ("/api/document-library/tables", *JOURNAL_PATHS):
            with self.subTest(path=path):
                response = await self.clients["lan"].get(path)
                self.assertEqual(response.status_code, 401, response.text)
        self.assert_no_extension_reads()

    async def test_guest_cannot_borrow_staff_modules_or_admin_flags(self):
        self.profile.update(role="admin", modules=["knowledge", "accounts"])
        self.session["role"] = "guest"
        for name, client in self.clients.items():
            for path in (*DOCUMENT_PATHS, *JOURNAL_PATHS):
                with self.subTest(ingress=name, path=path):
                    response = await client.get(path)
                    self.assertEqual(response.status_code, 403, response.text)
        self.assertEqual(allowed_modules(self.auth, self.session), {"cabinet"})
        self.auth.account_profile.assert_not_called()
        self.assert_no_extension_reads()

    async def test_release_route_rejects_guest_flag_and_temporary_admin_sessions(self):
        self.profile.update(role="admin", modules=["accounts"])
        for flags in ({"role": "admin", "is_guest": True}, {"role": "temporary"}):
            self.session = {"user": {"open_id": self.identity}, "is_admin": True, **flags}
            for name, client in self.clients.items():
                for path in JOURNAL_PATHS:
                    with self.subTest(flags=flags, ingress=name, path=path):
                        response = await client.get(path)
                        self.assertEqual(response.status_code, 403, response.text)
                        self.assertNotIn("error_code", response.json())
        self.assert_no_extension_reads()

    async def test_existing_public_session_immediately_loses_revoked_grants(self):
        client = self.clients["public"]
        self.profile["modules"] = ["knowledge"]
        self.assertEqual((await client.get("/api/document-library/tables")).status_code, 200)
        self.library.tables.reset_mock()
        self.get_library.reset_mock()
        self.profile["modules"] = ["assistant"]
        for path in DOCUMENT_PATHS:
            self.assertEqual((await client.get(path)).status_code, 403)
        self.get_library.assert_not_called()
        self.library.tables.assert_not_called()

        self.profile.update(role="admin", modules=["accounts"])
        self.assertEqual((await client.get(JOURNAL_PATHS[0])).status_code, 200)
        self.journal.entries.reset_mock()
        self.profile["role"] = "building"
        for path in JOURNAL_PATHS:
            response = await client.get(path)
            self.assertEqual(response.status_code, 403)
            self.assertNotIn("error_code", response.json())
        self.profile.update(role="admin", modules=[])
        for path in JOURNAL_PATHS:
            response = await client.get(path)
            self.assertEqual(response.status_code, 403)
            self.assertEqual(response.json()["error_code"], "module_access_denied")
        self.journal.entries.assert_not_called()


if __name__ == "__main__":
    unittest.main()
