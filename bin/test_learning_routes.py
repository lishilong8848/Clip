"""Offline learning routes, real-core wiring, and packaging regressions."""
import asyncio
import importlib
import json
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlsplit

from fastapi import FastAPI
from fastapi.responses import JSONResponse, Response
from fastapi.testclient import TestClient
from starlette.requests import Request

BIN = Path(__file__).resolve().parent
if str(BIN) not in sys.path:
    sys.path.insert(0, str(BIN))

from lan_bitable_template_portal import learning_routes as routes


class LearningError(Exception):
    def __init__(self, message, status=400):
        super().__init__(message)
        self.message, self.status = message, status


class LearningRouteTests(unittest.TestCase):
    def setUp(self):
        self.loop_thread = None
        self.events = []
        self.session = {"user": {"open_id": "building-H", "name": "H operator"}, "role": "building"}
        self.folder = tempfile.TemporaryDirectory(prefix="learning_routes_")
        self.addCleanup(self.folder.cleanup)
        attachment = Path(self.folder.name) / "sample.txt"
        attachment.write_bytes(b"file content")

        def record(name, *args):
            self.assertNotEqual(threading.get_ident(), self.loop_thread, name + " blocked the event loop")
            self.events.append((name, args))

        self.service = SimpleNamespace(
            start=lambda: record("start"),
            stop=lambda: record("stop"),
            bootstrap=lambda scope, actor: (record("bootstrap", scope, actor) or {"scope": scope, "actor": actor}),
            dispatch=lambda action, payload, actor, query: (
                record("dispatch", action, payload, actor, query)
                or {"action": action, "payload": payload, "actor": actor, "query": query}
            ),
            attachment=lambda oid, actor: (record("attachment", oid, actor) or (attachment, "sample.txt", "text/plain")),
            add_attachments=lambda files, query, actor: (
                record("attachments", files, query, actor) or {"names": [name for name, _ in files], "query": query}
            ),
            export=lambda kind, query, actor: (record("export", kind, query, actor) or (b"export content", "export.csv", "text/csv")),
        )
        self.factory = Mock(side_effect=lambda **kwargs: (record("init", kwargs) or self.service))
        modules = {
            "lan_bitable_template_portal.learning": SimpleNamespace(LearningError=LearningError, LearningService=self.factory),
            "lan_bitable_template_portal.portal_service": SimpleNamespace(
                BUILDING_OPEN_ID_MAP={scope: "building-" + scope for scope in (*"ABCDEH", "110")}
            ),
            "lan_bitable_template_portal.server": SimpleNamespace(portal_index_file=lambda: Path("portal.html")),
            "clipflow_backend.runtime_helpers": SimpleNamespace(send_text_to_open_ids_guarded=Mock()),
        }
        module_patch = patch.dict(sys.modules, modules)
        module_patch.start()
        self.addCleanup(module_patch.stop)
        self.auth = SimpleNamespace(
            is_admin=lambda session: (record("is_admin") or session.get("role") == "admin"),
            scope_allowed=Mock(side_effect=AssertionError("Learning must not call scope_allowed")),
        )
        self.controller = SimpleNamespace(
            _current_session=lambda request: (record("session") or self.session),
            _auth_required_response=lambda: JSONResponse({"ok": False, "auth_required": True}, status_code=401),
            _json_ok=lambda request, session, data: (record("json") or JSONResponse({"ok": True, "data": data})),
            _request_base_url=lambda request: str(request.base_url),
            _static_file_response=lambda request, path, html: (record("page", path, html) or Response("portal", media_type="text/html")),
        )
        self.runtime = SimpleNamespace(auth_manager=self.auth)
        self.app = FastAPI()

        async def remember_loop():
            self.loop_thread = threading.get_ident()

        self.app.add_event_handler("startup", remember_loop)

        @self.app.middleware("http")
        async def remember_request_loop(request, call_next):
            self.loop_thread = threading.get_ident()
            return await call_next(request)

        routes.install_learning_routes(self.app, self.controller, self.runtime)
        self.client = TestClient(self.app)
        self.addCleanup(self.client.close)

    def test_every_api_rejects_anonymous_and_non_building_accounts(self):
        for session, status in (
            (None, 401),
            ({"user": {"open_id": ""}, "role": "admin"}, 401),
            ({"user": {"open_id": "outsider"}, "allowed_scopes": list("ABCDEH")}, 403),
            ({"user": {"open_id": "building-110"}}, 403),
            ({"user": {"open_id": "building-H"}, "role": "guest"}, 403),
        ):
            self.session = session
            for path, methods in routes.ROUTES.items():
                for method in methods:
                    with self.subTest(session=session, path=path, method=method):
                        response = self.client.request(method, "/api/learning/" + path.replace("{id}", "resource"))
                        self.assertEqual(response.status_code, status, response.text)
        self.factory.assert_not_called()
        self.auth.scope_allowed.assert_not_called()

    def test_exact_building_scope_and_admin_identity(self):
        for scope in "ABCDEH":
            self.session = {"user": {"open_id": "building-" + scope, "name": "operator"}}
            response = self.client.get("/api/learning/bootstrap")
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json()["data"]["scope"], scope)
            self.assertEqual(response.json()["data"]["actor"]["scope"], scope)
        for scope, status in (("A", 403), ("110", 400), ("ALL", 400), ("CAMPUS", 400), ("a", 400)):
            response = self.client.get("/api/learning/papers", params={"scope": scope})
            self.assertEqual(response.status_code, status, response.text)
        self.assertEqual(self.client.get("/api/learning/papers?scope=A&scope=H").status_code, 403)
        self.assertEqual(self.client.post("/api/learning/issues", json={"scope": "A"}).status_code, 403)
        self.session = {"open_id": "administrator", "role": "admin", "name": "Admin"}
        for scope in "ABCDEH":
            response = self.client.get("/api/learning/bootstrap", params={"scope": scope})
            actor = response.json()["data"]["actor"]
            self.assertEqual(actor, {"id": "administrator", "name": "Admin", "is_admin": True, "scope": "", "can_answer": True})
        self.assertEqual(self.client.get("/api/learning/bootstrap?scope=ALL").status_code, 400)
        self.assertEqual(self.client.post("/api/learning/papers/p/answer", json={}).status_code, 200)
        self.auth.scope_allowed.assert_not_called()

    def test_empty_scope_defaults_to_own_building_or_admin_all_buildings(self):
        for session, expected in (
            ({"user": {"open_id": "building-H"}}, "H"),
            ({"open_id": "administrator", "role": "admin"}, ""),
            ({"user": {"open_id": "building-H"}, "role": "admin"}, ""),
        ):
            self.session = session
            for suffix in ("", "?scope="):
                response = self.client.get("/api/learning/bootstrap" + suffix)
                self.assertEqual(response.status_code, 200, response.text)
                self.assertEqual(response.json()["data"]["scope"], expected)
            for path in ("papers", "history", "review", "profile", "issues", "questions", "settings"):
                response = self.client.get("/api/learning/" + path + "?scope=")
                self.assertEqual(response.status_code, 200, response.text)
                self.assertEqual(response.json()["data"]["query"]["scope"], expected)
            response = self.client.post("/api/learning/issues?scope=", json={"scope": ""})
            self.assertEqual(response.json()["data"]["payload"]["scope"], expected)
            response = self.client.post("/api/learning/attachments", files={"files": ("a.txt", b"x")}, data={"scope": ""})
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json()["data"]["query"]["scope"], expected)
            self.assertEqual(self.client.get("/learning?scope=").text, "portal")
        self.session = {"user": {"open_id": "building-H"}}
        self.assertEqual(self.client.get("/api/learning/papers?scope=A&scope=").status_code, 403)
        self.assertEqual(self.client.post("/api/learning/issues", json={"scope": None}).status_code, 400)

    def test_dispatch_contract_and_path_ids_override_client_ids(self):
        cases = [
            ("GET", "papers", "papers.list"), ("GET", "papers/p", "paper.get"),
            ("DELETE", "papers/p", "paper.delete"),
            ("POST", "papers/p/answer", "paper.answer"), ("POST", "papers/p/reveal", "paper.reveal"),
            ("POST", "papers/p/notes", "paper.notes"), ("GET", "history", "history"),
            ("GET", "review", "review"), ("GET", "profile", "profile"),
            ("GET", "issues", "issues.list"), ("POST", "issues", "issue.create"),
            ("PATCH", "issues/p", "issue.update"), ("GET", "questions", "questions.list"),
            ("GET", "questions/p", "question.get"), ("POST", "questions", "question.save"),
            ("PUT", "questions/p", "question.save"), ("POST", "questions/p/status", "question.status"),
            ("POST", "questions/p/copy", "question.copy"), ("DELETE", "attachments/p", "attachment.delete"),
            ("GET", "settings", "settings.get"), ("PUT", "settings", "settings.save"),
            ("POST", "refresh", "refresh"), ("POST", "publish", "publish"), ("POST", "import", "import"),
        ]
        for method, path, action in cases:
            with self.subTest(path=path, method=method):
                kwargs = {"json": {"id": "spoofed", "value": 7}} if method != "GET" else {}
                response = self.client.request(method, "/api/learning/" + path + "?id=spoofed&scope=H", **kwargs)
                self.assertEqual(response.status_code, 200, response.text)
                data = response.json()["data"]
                self.assertEqual(data["action"], action)
                if "/p" in path:
                    self.assertEqual(data["query"]["id"], "p")
                    self.assertEqual(data["payload"]["id"], "p")
        self.assertEqual(self.client.post("/api/learning/papers", json={}).status_code, 405)
        self.assertEqual(self.client.get("/api/learning/unknown").status_code, 404)

    def test_csrf_same_origin_and_legacy_cookie_clients(self):
        for headers, expected in (
            ({}, 200), ({"Origin": "http://testserver"}, 200),
            ({"Origin": "http://testserver:80"}, 200),
            ({"Referer": "http://testserver/learning"}, 200),
            ({"Origin": "http://other.test"}, 403), ({"Origin": "null"}, 403),
            ({"Origin": "http://testserver:bad"}, 403),
            ({"Referer": "http://other.test/learning"}, 403),
            ({"Sec-Fetch-Site": "cross-site"}, 403),
            ({"Origin": "http://other.test", "Sec-Fetch-Site": "same-site"}, 403),
        ):
            with self.subTest(headers=headers):
                response = self.client.post("/api/learning/issues", json={}, headers=headers)
                self.assertEqual(response.status_code, expected, response.text)

    def test_json_limit_uses_actual_bytes_and_rejects_non_objects(self):
        for content in (b"[1]", b"null", b"{", b"\xff"):
            self.assertEqual(self.client.post("/api/learning/issues", content=content).status_code, 400)
        with patch.object(routes, "MAX_JSON_BYTES", 32):
            accepted = self.client.post("/api/learning/issues", content=b'{}', headers={"Content-Length": "9999"})
            self.assertEqual(accepted.status_code, 200)
            rejected = self.client.post("/api/learning/issues", content=b'{' + b' ' * 40 + b'}', headers={"Content-Length": "1"})
            self.assertEqual(rejected.status_code, 413)

    def test_streaming_upload_limits_and_partial_upload_cleanup(self):
        with patch.object(routes, "MAX_FILE_BYTES", 8), patch.object(routes, "MAX_TOTAL_BYTES", 12):
            good = self.client.post("/api/learning/attachments?issue_id=i&kind=evidence", files=[("files", ("a.txt", b"12345678")), ("files", ("b.txt", b"1234"))])
            self.assertEqual(good.status_code, 200, good.text)
            self.assertEqual(good.json()["data"]["names"], ["a.txt", "b.txt"])
            self.assertEqual(good.json()["data"]["query"]["issue_id"], "i")
            for files in ([('files', ('a', b'123456789'))], [('files', ('a', b'12345678')), ('files', ('b', b'12345'))], [('files', (str(n), b'x')) for n in range(11)]):
                response = self.client.post("/api/learning/attachments", files=files, headers={"Content-Length": "1"})
                self.assertEqual(response.status_code, 413, response.text)
        closed = []
        original_close = routes.File.close

        def close_file(upload):
            closed.append(upload)
            return original_close(upload)

        with patch.object(routes.File, "close", close_file), patch.object(routes, "MAX_FILE_BYTES", 4):
            response = self.client.post("/api/learning/attachments", files={"files": ("x", b"12345")})
            self.assertEqual(response.status_code, 413, response.text)
        self.assertTrue(closed)
        self.assertTrue(all(upload.file_object.closed for upload in closed))
        self.assertEqual(self.client.post("/api/learning/attachments", json={}).status_code, 400)
        cross_scope = self.client.post("/api/learning/attachments", files={"files": ("x", b"x")}, data={"scope": "A"})
        self.assertEqual(cross_scope.status_code, 403)
        repeated_kinds = self.client.post("/api/learning/attachments", files=[
            part for n in range(10) for part in (("files", (str(n), b"x")), ("kind", (None, "answer")))
        ])
        self.assertEqual(repeated_kinds.status_code, 200, repeated_kinds.text)
        self.assertEqual(repeated_kinds.json()["data"]["query"]["kind"], "answer")

    def test_chunked_requests_stop_before_reading_the_remainder(self):
        async def run(path, content_type, chunks, limits):
            self.loop_thread = threading.get_ident()
            chunks = iter(chunks)
            reads = []

            async def receive():
                chunk = next(chunks)
                reads.append(chunk)
                return {"type": "http.request", "body": chunk, "more_body": True}

            route = next(r for r in self.app.routes if getattr(r, "path", "") == path)
            request = Request({
                "type": "http", "http_version": "1.1", "method": "POST", "scheme": "http",
                "path": route.path, "raw_path": route.path.encode(), "query_string": b"",
                "headers": [(b"content-type", content_type), (b"content-length", b"1")],
                "server": ("testserver", 80), "client": ("testclient", 1), "route": route, "path_params": {},
            }, receive)
            with patch.multiple(routes, **limits):
                response = await route.endpoint(request)
            self.assertEqual(response.status_code, 413)
            self.assertEqual(len(reads), 2)

        asyncio.run(run("/api/learning/issues", b"application/json", [b'{"x":"', b'x' * 40, b'"}'], {"MAX_JSON_BYTES": 32}))
        first = b'--chunk\r\nContent-Disposition: form-data; name="files"; filename="a"\r\n\r\n1234'
        second = b'\r\n--chunk\r\nContent-Disposition: form-data; name="files"; filename="b"\r\n\r\n1234'
        end = b'\r\n--chunk--\r\n'
        cases = [
            ([first, b'56789', end], {"MAX_FILE_BYTES": 8}),
            ([first, second, end], {"MAX_TOTAL_BYTES": 7}),
            ([first, second, end], {"MAX_FILES": 1}),
            ([first, b'x' * 20, end], {"MAX_TOTAL_BYTES": len(first), "MAX_JSON_BYTES": 1}),
        ]
        for chunks, limits in cases:
            with self.subTest(limits=limits):
                asyncio.run(run("/api/learning/attachments", b"multipart/form-data; boundary=chunk", chunks, limits))
        self.factory.assert_not_called()

    def test_notification_adapter_uses_guard_and_stable_id(self):
        self.client.get("/api/learning/bootstrap")
        send = self.factory.call_args.kwargs["send_message"]
        guard = sys.modules["clipflow_backend.runtime_helpers"].send_text_to_open_ids_guarded
        guard.return_value = (True, "queued", [])
        self.assertEqual(send("H", "message", "publish:today_H"), (True, "queued", []))
        first = guard.call_args
        send("H", "message", "publish:today_H")
        self.assertEqual(first, guard.call_args)
        self.assertEqual(first.args, ("message", ["building-H"]))
        self.assertTrue(first.kwargs["message_uuid"])
        with self.assertRaises(LearningError):
            send("110", "message", "identity")

    def test_runtime_address_resolver_reuses_program_configuration_not_request_host(self):
        configured = Mock(return_value="http://192.168.224.130:18766")
        self.runtime.service = SimpleNamespace(_critical_guard_public_base_url=configured)
        self.client.get("/api/learning/bootstrap", headers={"Host": "127.0.0.1:18766"})
        resolve = self.factory.call_args.kwargs["get_portal_url"]
        self.assertEqual(resolve(), "http://192.168.224.130:18766")
        configured.return_value = "https://portal.example.test:8787"
        self.assertEqual(resolve(), "https://portal.example.test:8787")

    def test_only_supported_images_and_pdf_are_inline_with_sandbox(self):
        for mime, expected in (
            ("image/png", "inline"), ("image/jpeg", "inline"),
            ("image/webp", "inline"), ("application/pdf", "inline"),
            ("application/msword", "attachment"),
            ("application/vnd.openxmlformats-officedocument.wordprocessingml.document", "attachment"),
            ("text/plain", "attachment"), ("text/html", "attachment"),
            ("image/svg+xml", "attachment"), ("image/gif", "attachment"),
        ):
            with self.subTest(mime=mime):
                self.service.attachment = Mock(return_value=(Path(self.folder.name) / "sample.txt", "sample.txt", mime))
                response = self.client.get("/api/learning/attachments/a")
                self.assertEqual(response.status_code, 200)
                self.assertTrue(response.headers["content-disposition"].startswith(expected + ";"))
                self.assertEqual(response.headers["x-content-type-options"], "nosniff")
                self.assertEqual(response.headers["content-security-policy"], "sandbox; default-src 'none'; frame-ancestors 'self'")
                self.assertIn("no-store", response.headers["cache-control"])

    def test_downloads_safe_errors_page_login_and_lifecycle(self):
        self.factory.assert_not_called()
        with TestClient(self.app) as client:
            self.factory.assert_called_once()
            self.assertEqual(self.events[-1][0], "start")
            self.assertIs(self.controller._learning, self.runtime.learning_service)
            for path, content in (("attachments/a", b"file content"), ("export?kind=history", b"export content")):
                response = client.get("/api/learning/" + path)
                self.assertEqual(response.status_code, 200, response.text)
                self.assertEqual(response.content, content)
                self.assertIn("attachment", response.headers["content-disposition"])
                self.assertEqual(response.headers["x-content-type-options"], "nosniff")
            self.service.dispatch = Mock(side_effect=LearningError("expected", 409))
            self.assertEqual(client.get("/api/learning/papers").status_code, 409)
            self.service.dispatch = Mock(side_effect=RuntimeError("secret stack or path"))
            with self.assertLogs(routes.__name__, level="ERROR"):
                response = client.get("/api/learning/papers")
            self.assertEqual(response.status_code, 500)
            self.assertNotIn("secret", response.text)
            self.assertEqual(response.json()["error"], "画像学练服务暂时不可用，请稍后重试。")
            self.session = None
            response = client.get("/learning?scope=H", follow_redirects=False)
            self.assertEqual(response.status_code, 302)
            target = parse_qs(urlsplit(response.headers["location"]).query)["next"][0]
            self.assertEqual(target, "/learning?scope=H")
            self.session = {"user": {"open_id": "building-H"}}
            self.assertEqual(client.get("/learning/").text, "portal")
            self.assertEqual(client.get("/learning?scope=A").status_code, 403)
        self.assertEqual(self.events[-1][0], "stop")


class LearningSourceContractTests(unittest.TestCase):
    def test_real_core_dispatch_and_binary_return_contracts_offline(self):
        core = importlib.import_module("lan_bitable_template_portal.learning")
        self.assertEqual(Path(core.__file__).resolve(), BIN / "lan_bitable_template_portal/learning.py")
        admin = {"id": "administrator", "name": "Admin", "is_admin": True, "scope": ""}
        actor = {"id": "building-H", "name": "H", "is_admin": False, "scope": "H"}
        actions = set()
        with tempfile.TemporaryDirectory(prefix="learning_contract_") as folder, patch("socket.socket.connect", side_effect=AssertionError("Network forbidden")):
            cloud = Mock()
            service = core.LearningService(root=folder, cloud=cloud, send_message=Mock())
            self.addCleanup(service.stop)

            def dispatch(action, payload=None, who=None, query=None):
                result = service.dispatch(action, payload or {}, who or admin, query or {})
                self.assertIsInstance(result, dict, action)
                json.dumps(result, allow_nan=False)
                actions.add(action)
                return result

            self.assertIs(service.settings()["enabled"], False)
            self.assertEqual(service.bootstrap("", admin)["scope"], "")
            self.assertEqual(service.bootstrap("", actor)["scope"], "H")
            dispatch("settings.get")
            dispatch("settings.save", {"enabled": True})
            payload = {"bank": "written", "stem": "Contract question", "type": "single", "status": "published",
                       "options": [{"id": "o1", "text": "One"}, {"id": "o2", "text": "Two"}], "correct_option_ids": ["o1"]}
            question = dispatch("question.save", payload)
            dispatch("questions.list", query={"scope": ""})
            dispatch("question.get", {"id": question["id"]})
            question = dispatch("question.status", {"id": question["id"], "version": question["version"], "status": "disabled"})
            dispatch("question.copy", {"id": question["id"]})
            dispatch("import", {"questions": [payload], "preview": True})
            dispatch("import", {"questions": [payload]})
            with service.transaction() as connection:
                for scope in ("A", "H"):
                    paper = {"id": "paper-" + scope, "scope": scope, "date": "2026-09-29", "created_at": "2026-09-29T08:00:00+08:00",
                             "shortage": {"written": 7, "duty": 1, "professional": 1}, "questions": [question]}
                    service._put("paper", paper["id"], paper, connection)
            self.assertEqual(dispatch("papers.list", query={"scope": ""})["total"], 2)
            self.assertEqual(dispatch("papers.list", who=actor, query={"scope": ""})["total"], 1)
            dispatch("history", query={"scope": ""})
            dispatch("paper.get", {"id": "paper-H"}, actor)
            paper = dispatch("paper.reveal", {"id": "paper-H", "question_id": question["id"], "kind": "hint"}, actor)
            paper = dispatch("paper.notes", {"id": "paper-H", "question_id": question["id"], "note": "Note", "favorite": True}, actor)
            dispatch("paper.answer", {"id": "paper-H", "question_id": question["id"], "version": paper["version"], "operation_id": "contract-answer", "option_ids": ["o2"]}, actor)
            dispatch("review", who=actor, query={"scope": ""})
            dispatch("profile", query={"scope": ""})
            issue = dispatch("issue.create", {"paper_id": "paper-H", "question_id": question["id"], "description": "Check this question"}, actor)
            issue = dispatch("issue.update", {"id": issue["id"], "version": issue["version"], "remark": "More detail"}, actor)
            dispatch("issues.list", who=actor, query={"scope": ""})
            added = service.add_attachments([("evidence.txt", b"evidence")], {"issue_id": issue["id"], "kind": "material", "version": issue["version"]}, actor)
            self.assertIsInstance(added, dict)
            attached = service.attachment(added["items"][0]["id"], actor)
            self.assertIsInstance(attached[0], Path)
            self.assertEqual(attached[1:], ("evidence.txt", "text/plain"))
            dispatch("attachment.delete", {"id": added["items"][0]["id"]}, actor, {"version": str(added["version"])})
            for kind in ("questions", "records"):
                result = service.export(kind, {"scope": ""}, admin)
                self.assertIsInstance(result[0], bytes)
                self.assertTrue(all(isinstance(value, str) for value in result[1:]))
            dispatch("refresh")
            dispatch("publish")
            dispatch("paper.delete", {"id": "paper-H"})
            self.assertEqual(actions, {action for methods in routes.ROUTES.values() for action in methods.values()} - {"bootstrap", "attachments", "attachment", "export"})
            self.assertEqual(cloud.mock_calls, [])

    def test_real_disabled_service_start_waits_without_cloud_io(self):
        core = importlib.import_module("lan_bitable_template_portal.learning")
        with tempfile.TemporaryDirectory(prefix="learning_idle_") as folder:
            cloud = Mock()
            service = core.LearningService(root=folder, cloud=cloud)
            waiting = threading.Event()
            original_wait = service._wake.wait

            def wait(timeout):
                waiting.set()
                return original_wait(timeout)

            try:
                with patch.object(service._wake, "wait", side_effect=wait):
                    service.start()
                    self.assertTrue(waiting.wait(2))
                    self.assertFalse(service.settings()["enabled"])
                    service.stop()
                service.tick()
                self.assertEqual(cloud.mock_calls, [])
            finally:
                service.stop()


class LearningWireTests(unittest.TestCase):
    def test_real_service_http_answer_issue_and_admin_correction(self):
        from bin.test_learning import CURRENT, DAY, FakeCloud, FakeSender

        core = importlib.import_module("lan_bitable_template_portal.learning")
        cloud, sender = FakeCloud(enabled=False), FakeSender()
        sessions = {
            "floor": {"user": {"open_id": "building-A", "name": "A operator"}},
            "admin": {"user": {"open_id": "administrator", "name": "Admin"}, "role": "admin"},
        }
        controller = SimpleNamespace(
            _current_session=lambda request: sessions.get(request.cookies.get("session")),
            _auth_required_response=lambda: JSONResponse({"ok": False}, status_code=401),
            _json_ok=lambda request, session, data: JSONResponse({"ok": True, "data": data}),
            _request_base_url=lambda request: str(request.base_url),
        )
        runtime = SimpleNamespace(auth_manager=SimpleNamespace(is_admin=lambda session: session.get("role") == "admin"))
        constants = SimpleNamespace(BUILDING_OPEN_ID_MAP={scope: "building-" + scope for scope in "ABCDEH"})
        with tempfile.TemporaryDirectory(prefix="learning_wire_") as folder, \
                patch.object(core, "now", return_value=CURRENT), \
                patch("socket.create_connection", side_effect=AssertionError("Network forbidden")), \
                patch("socket.getaddrinfo", side_effect=AssertionError("DNS forbidden")):
            service = core.LearningService(root=folder, cloud=cloud, send_message=sender)
            with service.transaction() as connection:
                for number in range(6):
                    question = service._validated_question({
                        "bank": "written", "stem": f"Wire question {number}", "type": "single", "status": "published",
                        "options": [{"id": "a", "text": "One"}, {"id": "b", "text": "Two"}],
                        "correct_option_ids": ["a"], "hint": "Read the question carefully",
                    })
                    service._put("question", question["id"], question, connection, False)
            service.publish(DAY)
            with patch.object(core, "LearningService", return_value=service) as factory, \
                    patch.dict(sys.modules, {"lan_bitable_template_portal.portal_service": constants}):
                app = FastAPI()
                routes.install_learning_routes(app, controller, runtime)
                # Without a lifespan context TestClient never starts the service worker.
                client = TestClient(app)
                client.cookies.set("session", "floor")

                def request(method, path, **kwargs):
                    response = client.request(method, "/api/learning/" + path, **kwargs)
                    self.assertEqual(response.status_code, 200, response.text)
                    self.assertIs(response.json()["ok"], True)
                    return response.json()["data"]

                try:
                    listing = request("GET", "papers", params={"scope": "", "date": DAY.isoformat()})
                    self.assertEqual(listing["total"], 1)
                    paper_path = "papers/" + listing["items"][0]["id"]
                    paper = request("GET", paper_path)
                    question = paper["questions"][0]
                    self.assertNotIn("answer", question)
                    question_id = question["id"]
                    paper = request("POST", paper_path + "/reveal", json={"question_id": question_id, "kind": "hint"})
                    self.assertEqual(paper["questions"][0]["answer"], {"hint": "Read the question carefully"})
                    answered = request("POST", paper_path + "/answer", json={
                        "question_id": question_id, "version": paper["version"],
                        "operation_id": "wire-first-answer", "option_ids": ["b"],
                    })
                    attempt = answered["questions"][0]["attempt"]
                    self.assertIs(attempt["correct"], False)
                    self.assertIs(attempt["assisted"], True)
                    issue = request("POST", "issues", json={
                        "paper_id": paper["id"], "question_id": question_id,
                        "category": "答案", "description": "Please verify the reference answer",
                    })
                    self.assertEqual((issue["scope"], issue["paper_id"], issue["question_id"]), ("A", paper["id"], question_id))
                    self.assertNotIn("correct_option_ids", issue["question"])
                    added = request("POST", "attachments", params={"issue_id": issue["id"], "kind": "material"},
                                    data={"version": str(issue["version"])}, files={"files": ("evidence.txt", b"wire evidence")})
                    attachment_path = "attachments/" + added["items"][0]["id"]
                    stale_upload = client.post("/api/learning/attachments", params={"issue_id": issue["id"], "kind": "material"},
                                               data={"version": str(issue["version"])}, files={"files": ("other.txt", b"stale evidence")})
                    self.assertEqual(stale_upload.status_code, 409, stale_upload.text)
                    stale_delete = client.delete("/api/learning/" + attachment_path, params={"version": issue["version"]})
                    self.assertEqual(stale_delete.status_code, 409, stale_delete.text)
                    current_issue = request("GET", "issues")["items"][0]
                    self.assertEqual(current_issue["version"], added["version"])
                    self.assertEqual(current_issue["attachments"], added["attachments"])
                    deleted = request("DELETE", attachment_path, params={"version": current_issue["version"]})
                    self.assertIs(deleted["deleted"], True)
                    current_issue = request("GET", "issues")["items"][0]
                    self.assertEqual(current_issue["version"], deleted["version"])
                    self.assertEqual(current_issue["attachments"], [])

                    client.cookies.set("session", "admin")
                    request("PUT", "settings", json={"enabled": True})
                    question_path = "questions/" + question_id
                    original = request("GET", question_path)
                    added = request("POST", "attachments", params={"question_id": question_id, "kind": "material", "version": original["version"]},
                                    files={"files": ("reference.txt", b"wire reference")})
                    editor = request("GET", question_path)
                    self.assertEqual(editor["version"], added["version"])
                    deleted = request("DELETE", "attachments/" + added["items"][0]["id"], params={"version": editor["version"]})
                    self.assertIs(deleted["deleted"], True)
                    original = request("GET", question_path)
                    self.assertEqual(original["version"], deleted["version"])
                    self.assertEqual(original["attachments"], [])
                    update = {"version": original["version"], "correct_option_ids": ["b"], "reason": "Verified correction"}
                    corrected = request("PUT", question_path, json=update)
                    self.assertNotEqual(corrected["version"], original["version"])
                    stale = client.put("/api/learning/" + question_path, json=update)
                    self.assertEqual(stale.status_code, 409, stale.text)
                    self.assertIs(stale.json()["ok"], False)
                    self.assertIn("题目已被修改", stale.json()["error"])

                    client.cookies.set("session", "floor")
                    final = request("GET", paper_path)
                    result = final["questions"][0]
                    self.assertEqual(result["answer"]["correct_option_ids"], ["b"])
                    self.assertIs(result["attempt"]["correct"], True)
                    self.assertIs(result["attempt"]["corrections"][0]["correct"], False)
                    self.assertIn("Verified correction", result["correction"])
                    self.assertEqual(result["version"], corrected["version"])
                    self.assertGreater(final["version"], answered["version"])
                    for field in ("operation_id", "submitted_at", "option_ids", "assisted"):
                        self.assertEqual(result["attempt"][field], attempt[field])
                    factory.assert_called_once()
                    self.assertIs(runtime.learning_service, service)
                    self.assertIsNone(service._thread)
                    self.assertEqual(cloud.calls, [])
                    self.assertEqual(sender.calls, [])
                finally:
                    client.close()
                    service.stop()


class LearningPackagingTests(unittest.TestCase):
    def test_packaging_iterator_and_full_copy_include_new_modules(self):
        import package_portable as package

        paths = [Path("bin/lan_bitable_template_portal") / name for name in ("learning.py", "learning_cloud.py", "learning_routes.py")]
        paths.append(Path("bin/clipflow_backend/main.py"))
        with tempfile.TemporaryDirectory(prefix="learning_package_") as directory:
            root, output = Path(directory) / "source", Path(directory) / "output"
            for path in paths:
                target = root / path
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text("# package check\n", encoding="utf-8")
            with patch.object(package, "PROJECT_ROOT", root):
                actual = {path.relative_to(root) for path in package._iter_project_files(root, exclude_venv=True)}
                self.assertTrue(set(paths) <= actual)
                package.copy_project(output)
                self.assertTrue(all((output / path).is_file() for path in paths))
        self.assertEqual(package.RUNTIME_MODULE_TO_PACKAGE["multipart"], "python-multipart")

    def test_route_module_import_does_not_import_core_or_runtime(self):
        import importlib.util

        spec = importlib.util.spec_from_file_location("isolated_learning_routes", BIN / "lan_bitable_template_portal/learning_routes.py")
        module = importlib.util.module_from_spec(spec)
        with patch("socket.create_connection", side_effect=AssertionError("Network at import")):
            spec.loader.exec_module(module)
        self.assertFalse(hasattr(module, "LearningService"))

    def test_multipart_legacy_fallback_only_for_missing_top_level_package(self):
        original_import = __import__
        for missing in ("python_multipart", "python_multipart.exceptions", "missing_dependency"):
            with self.subTest(missing=missing):
                legacy_imports = []

                def import_module(name, *args, **kwargs):
                    if name.startswith("python_multipart"):
                        raise ModuleNotFoundError(name=missing)
                    if name in {"multipart", "multipart.exceptions", "multipart.multipart"}:
                        legacy_imports.append(name)
                        return routes
                    return original_import(name, *args, **kwargs)

                spec = importlib.util.spec_from_file_location("isolated_learning_routes", BIN / "lan_bitable_template_portal/learning_routes.py")
                module = importlib.util.module_from_spec(spec)
                with patch("builtins.__import__", side_effect=import_module):
                    if missing == "python_multipart":
                        spec.loader.exec_module(module)
                        self.assertIs(module.FormParser, routes.FormParser)
                    else:
                        with self.assertRaises(ModuleNotFoundError) as caught:
                            spec.loader.exec_module(module)
                        self.assertEqual(caught.exception.name, missing)
                self.assertEqual(legacy_imports, ["multipart", "multipart.exceptions", "multipart.multipart"] if missing == "python_multipart" else [])


if __name__ == "__main__":
    unittest.main()
