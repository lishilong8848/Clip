"""Real controller/SQLite draft boundaries; no service workers or business sends."""
import sys
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from fastapi.testclient import TestClient
from clipflow_backend.main import FastAPIPortalController
from lan_bitable_template_portal.server import PortalRuntime, WORKBENCH_NOTICE_DRAFT_NAMESPACE
from lan_bitable_template_portal.state_store import LanPortalStateStore


class WorkbenchDraftScopeTests(unittest.TestCase):
    def setUp(self):
        folder = tempfile.TemporaryDirectory(prefix="draft-scope-")
        self.addCleanup(folder.cleanup)
        self.store = LanPortalStateStore(Path(folder.name) / "drafts.sqlite3")
        self.addCleanup(self.store.shutdown_write_worker, timeout=2.0)
        self.store.put_document("test_fixture", "auth-ready", {})
        self.owner = "fixture-draft-owner"
        self.grants = {self.owner: ["D", "E"], "fixture-other-owner": ["D", "E"]}
        self.modules = ["maintenance", "change"]
        self.session = {"user": {"open_id": self.owner, "name": "隔离草稿人员"},
                        "role": "building", "allowed_scopes": ["D", "E"]}
        auth = PortalRuntime.auth_manager
        for guard in (
            patch.object(PortalRuntime, "state_store", self.store),
            patch.object(auth, "_state_store", self.store),
            patch("clipflow_backend.main.LanPortalStateStore", return_value=self.store),
            patch.object(auth, "scopes_for_open_id", side_effect=lambda identity: self.grants.get(identity, [])),
            patch.object(auth, "account_profile", side_effect=lambda identity: {
                "role": "building", "level": "L4", "modules": self.modules,
                "scope_rules": self.grants.get(identity, []),
            }),
            patch.object(auth, "directory_person", return_value={}),
            patch("socket.create_connection", side_effect=AssertionError("No external connections")),
            patch("socket.getaddrinfo", side_effect=AssertionError("No external DNS")),
        ):
            self.enterContext(guard)
        self.controller = FastAPIPortalController(host="127.0.0.1", port=18766)
        self.enterContext(patch.object(self.controller, "_current_session", side_effect=lambda _request: self.session))
        self.client = TestClient(self.controller._build_app())  # No lifespan/startup workers.
        self.addCleanup(self.client.close)

    def seed(self, scope="E", *, owner=None, target="fixture-target", revision=7):
        return PortalRuntime.save_workbench_notice_draft(
            open_id=owner or self.owner, scope=scope, work_type="maintenance",
            target_record_id=target, action="update", client_revision=revision,
            fields={"progress": "fixture-private-old-content"},
        )

    def stored(self, *, owner=None, target="fixture-target"):
        key = PortalRuntime._workbench_draft_key(owner or self.owner, "maintenance", target)
        return self.store.get_document(WORKBENCH_NOTICE_DRAFT_NAMESPACE, key)

    def get(self, scope="D", **params):
        return self.client.get("/api/workbench/draft", params={
            "scope": scope, "work_type": "maintenance", "target_record_id": "fixture-target",
            "action": "update", **params,
        })

    def put(self, revision, *, scope="D", content="fixture-new-content", fields=None, action="update"):
        return self.client.put("/api/workbench/draft", json={
            "scope": scope, "work_type": "maintenance", "target_record_id": "fixture-target",
            "action": action, "client_revision": revision,
            "fields": {"progress": content} if fields is None else fields,
        })

    def hidden(self, response, draft):
        self.assertEqual(response.status_code, 200, response.text)
        data = response.json()["data"]
        self.assertIs(data["exists"], False)
        self.assertNotIn("draft", data)
        self.assertEqual(data["draft_base"], {"version": draft["version"], "client_revision": draft["client_revision"]})
        self.assertTrue(data["warning"])
        self.assertNotIn("E楼", data["warning"])
        self.assertNotIn("UNKNOWN", data["warning"])
        self.assertNotIn("fixture-private-old-content", response.text)
        return data

    def conflict(self, response):
        self.assertEqual(response.status_code, 409, response.text)
        self.assertEqual(set(response.json()), {"ok", "error"})
        self.assertFalse(response.json()["ok"])
        self.assertNotIn("fixture-private-old-content", response.text)
        self.assertNotIn("E楼", response.text)

    def test_authorized_view_context_can_cross_all_and_building(self):
        self.grants[self.owner] = ["ALL"]
        for index, (stored_scope, requested) in enumerate([("ALL", "D"), ("D", "ALL"), ("E", "D")]):
            with self.subTest(stored=stored_scope, requested=requested):
                target = "target-" + str(index)
                draft = self.seed(stored_scope, target=target)
                response = self.get(requested, target_record_id=target)
                self.assertEqual(response.status_code, 200, response.text)
                self.assertTrue(response.json()["data"]["exists"])
                self.assertEqual(response.json()["data"]["draft"], draft)
                self.assertEqual(self.stored(target=target), draft)

    def test_both_concrete_scopes_are_authorized_without_all(self):
        draft = self.seed("E")
        response = self.get("D")
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["data"]["draft"], draft)

    def test_known_scope_aliases_use_normalized_authorization(self):
        for index, (stored_scope, requested, grants) in enumerate([
            (" d楼 ", "e楼", ["D", "E"]), ("D栋", "d", ["D"]),
            ("全部", "D", ["ALL"]), ("园区", "D", ["D", "CAMPUS"]),
            ("PARK", "D", ["D", "CAMPUS"]), ("110站", "D", ["D", "110"]),
        ]):
            with self.subTest(stored=stored_scope, requested=requested):
                self.grants[self.owner] = grants
                target = "alias-" + str(index)
                draft = self.seed(stored_scope, target=target)
                response = self.get(requested, target_record_id=target)
                self.assertEqual(response.status_code, 200, response.text)
                self.assertEqual(response.json()["data"]["draft"], draft)

    def test_scope_reduction_hides_content_preserves_base_and_keeps_current_work_readable(self):
        draft = self.seed("E")
        current = self.seed("D", target="current-D")
        self.grants[self.owner] = ["D"]  # Session still advertises D/E; current grants win.
        self.hidden(self.get("D"), draft)
        self.assertEqual(self.stored(), draft)
        response = self.get("D", target_record_id="current-D")
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["data"]["draft"], current)
        self.grants[self.owner] = ["D", "E"]
        self.assertEqual(self.get("D").json()["data"]["draft"], draft)

    def test_invalid_missing_or_ambiguous_scope_never_falls_back_to_authorized_all(self):
        self.grants[self.owner] = ["ALL"]
        for index, scope in enumerate([None, "", "   ", "???", "UNKNOWN", "DE", "D/E", ["D"], {"scope": "D"}]):
            with self.subTest(scope=scope):
                target = "invalid-" + str(index)
                draft = self.seed("D", target=target)
                draft["scope"] = scope
                key = PortalRuntime._workbench_draft_key(self.owner, "maintenance", target)
                self.store.put_document(WORKBENCH_NOTICE_DRAFT_NAMESPACE, key, draft)
                self.hidden(self.get("D", target_record_id=target), draft)
                self.assertEqual(self.stored(target=target), draft)
        draft = self.seed("D")
        del draft["scope"]
        self.store.put_document(WORKBENCH_NOTICE_DRAFT_NAMESPACE,
                                PortalRuntime._workbench_draft_key(self.owner, "maintenance", "fixture-target"), draft)
        self.hidden(self.get("D"), draft)
        self.assertEqual(self.stored(), draft)

    def test_owner_work_type_and_target_remain_isolated(self):
        draft = self.seed("E")
        self.session["user"]["open_id"] = "fixture-other-owner"
        data = self.get("D").json()["data"]
        self.assertFalse(data["exists"])
        self.assertNotIn("draft_base", data)
        self.assertNotIn("fixture-private-old-content", str(data))
        self.assertEqual(self.stored(), draft)
        self.session["user"]["open_id"] = self.owner
        for query in ({"work_type": "change"}, {"target_record_id": "another-target"}):
            data = self.get("D", **query).json()["data"]
            self.assertFalse(data["exists"])
            self.assertNotIn("draft_base", data)
        self.assertEqual(self.get("D").json()["data"]["draft"], draft)

    def test_unauthorized_requests_are_rejected_before_draft_lookup(self):
        draft = self.seed("E")
        self.grants[self.owner] = ["D"]
        with patch.object(PortalRuntime, "get_workbench_notice_draft", wraps=PortalRuntime.get_workbench_notice_draft) as read:
            for scope in ("E", "ALL"):
                response = self.get(scope)
                self.assertEqual(response.status_code, 400, response.text)
                self.assertNotIn("draft_base", response.text)
                self.assertNotIn("fixture-private-old-content", response.text)
            read.assert_not_called()
        self.assertEqual(self.put(8, scope="E").status_code, 400)
        self.assertEqual(self.stored(), draft)

    def test_missing_session_and_missing_module_still_deny_access(self):
        self.seed("D")
        self.modules = []
        self.assertEqual(self.get().status_code, 403)
        self.modules = ["maintenance"]
        self.session = None
        self.assertEqual(self.get().status_code, 401)

    def test_start_get_does_not_restore_or_delete_old_draft(self):
        draft = self.seed("E")
        self.grants[self.owner] = ["D"]
        data = self.get("D", action="start").json()["data"]
        self.assertFalse(data["exists"])
        self.assertNotIn("draft", data)
        self.assertNotIn("draft_base", data)
        self.assertEqual(self.stored(), draft)

    def test_hidden_get_then_stale_put_cannot_leak_or_modify_old_content(self):
        draft = self.seed("E")
        self.grants[self.owner] = ["D"]
        self.hidden(self.get("D"), draft)
        for revision in (1, draft["client_revision"]):
            with self.subTest(revision=revision):
                self.conflict(self.put(revision))
                self.assertEqual(self.stored(), draft)

    def test_stale_put_cannot_echo_unreadable_content_without_a_prior_get(self):
        draft = self.seed("E")
        self.grants[self.owner] = ["D"]
        self.conflict(self.put(1))
        self.assertEqual(self.stored(), draft)

    def test_base_plus_one_can_save_current_scope_and_outdated_windows_conflict(self):
        draft = self.seed("E")
        self.grants[self.owner] = ["D"]
        base = self.hidden(self.get("D"), draft)["draft_base"]
        response = self.put(base["client_revision"] + 1)
        self.assertEqual(response.status_code, 200, response.text)
        saved = response.json()["data"]["draft"]
        self.assertEqual(saved["scope"], "D")
        self.assertEqual(saved["version"], base["version"] + 1)
        self.assertEqual(saved["client_revision"], base["client_revision"] + 1)
        self.assertEqual(saved["fields"], {"progress": "fixture-new-content"})
        self.conflict(self.put(saved["client_revision"], content="outdated-window"))
        self.assertEqual(self.stored(), saved)
        self.assertEqual(self.get("D").json()["data"]["draft"], saved)

    def test_identical_put_retry_returns_the_original_version_and_timestamps(self):
        first = self.put(1)
        self.assertEqual(first.status_code, 200, first.text)
        saved = first.json()["data"]["draft"]
        repeated = self.put(1)
        self.assertEqual(repeated.status_code, 200, repeated.text)
        self.assertEqual(repeated.json()["data"]["draft"], saved)
        self.assertEqual(self.stored(), saved)

    def test_equivalent_normalized_fields_are_idempotent(self):
        robot = "a" * 32
        first = self.put(1, fields={"progress": 12, "building_codes": [" d ", "E", "d"],
                                    "dingtalk_robot_ids": [robot, "invalid", robot], "ignored": "drop"})
        self.assertEqual(first.status_code, 200, first.text)
        saved = first.json()["data"]["draft"]
        self.assertEqual(saved["fields"], {"progress": "12", "building_codes": ["D", "E"], "dingtalk_robot_ids": [robot]})
        repeated = self.put(1, fields={"progress": "12", "building_codes": "D,E",
                                       "dingtalk_robot_ids": [robot], "ignored": "another ignored value"})
        self.assertEqual(repeated.status_code, 200, repeated.text)
        self.assertEqual(repeated.json()["data"]["draft"], saved)
        self.assertEqual(self.stored(), saved)

    def test_identical_cross_view_retry_requires_both_scopes_to_remain_authorized(self):
        draft = self.seed("E")
        repeated = self.put(draft["client_revision"], content="fixture-private-old-content")
        self.assertEqual(repeated.status_code, 200, repeated.text)
        self.assertEqual(repeated.json()["data"]["draft"], draft)
        self.grants[self.owner] = ["D"]
        self.conflict(self.put(draft["client_revision"], content="fixture-private-old-content"))
        self.assertEqual(self.stored(), draft)

    def test_action_changes_or_older_revisions_are_conflicts_even_with_same_fields(self):
        first = self.put(3)
        self.assertEqual(first.status_code, 200, first.text)
        saved = first.json()["data"]["draft"]
        self.conflict(self.put(3, action="end"))
        self.conflict(self.put(2))
        self.assertEqual(self.stored(), saved)

    def test_concurrent_identical_replays_share_one_saved_version(self):
        draft = self.seed("E")
        self.grants[self.owner] = ["D"]
        barrier = threading.Barrier(2)

        def write(_index):
            barrier.wait(timeout=5)
            return self.put(draft["client_revision"] + 1)

        with ThreadPoolExecutor(max_workers=2) as pool:
            responses = list(pool.map(write, (1, 2)))
        self.assertEqual([response.status_code for response in responses], [200, 200])
        saved = responses[0].json()["data"]["draft"]
        self.assertEqual(responses[1].json()["data"]["draft"], saved)
        self.assertEqual(saved["version"], draft["version"] + 1)
        self.assertEqual(self.stored(), saved)

    def test_original_round_trip_and_owner_version_with_isolated_auth_fixture(self):
        from test_lan_template_work_status import LanTemplateWorkStatusTests
        from lan_bitable_template_portal.portal_auth import PortalAuthManager

        # A separate auth object keeps the original A-building cookie/session fixture away
        # from this class's D/E profile mocks. Session lookup and scope_allowed remain real.
        auth_root = self.store.db_path.parent / "original-auth"
        with patch("lan_bitable_template_portal.portal_auth.get_data_file_path",
                   side_effect=lambda name: str(auth_root / name)):
            auth = PortalAuthManager()
        auth._state_store.put_document("test_fixture", "auth-ready", {})
        identity = "ou_draft_user"
        auth._state_store.put_auth_permissions({
            "users": {identity: {"enabled": True, "role": "building", "scopes": ["A"],
                                  "modules": ["maintenance"]}},
            "default_scopes": [],
        })
        profile = {"role": "building", "level": "L4", "enabled": True,
                   "modules": ["maintenance"], "scope_rules": ["A"]}
        try:
            with (
                patch.object(PortalRuntime, "auth_manager", auth),
                patch.object(auth, "account_profile", side_effect=lambda actor:
                             dict(profile) if actor == identity else {"modules": [], "scope_rules": []}),
                patch.object(auth, "directory_person", return_value={}),
            ):
                session = {"user": {"open_id": identity}, "allowed_scopes": ["A"]}
                self.assertTrue(auth.scope_allowed(session, "A"))
                self.assertFalse(auth.scope_allowed(session, "D"))
                self.assertFalse(auth.scope_allowed({"user": {"open_id": "fixture-ungranted"}}, "A"))
                for name in (
                    "test_fastapi_workbench_notice_draft_round_trip",
                    "test_workbench_notice_drafts_are_owner_scoped_and_version_safe",
                ):
                    with self.subTest(original=name):
                        result = unittest.TestResult()
                        LanTemplateWorkStatusTests(name).run(result)
                        self.assertEqual(result.testsRun, 1)
                        self.assertFalse(result.skipped)
                        self.assertTrue(result.wasSuccessful(), "\n".join(
                            trace for _case, trace in result.errors + result.failures))
        finally:
            auth._cleanup_executor.shutdown(wait=True, cancel_futures=True)
            auth._state_store.shutdown_write_worker(timeout=2.0)

    def test_two_windows_using_the_same_base_have_only_one_winner(self):
        draft = self.seed("E")
        self.grants[self.owner] = ["D"]
        barrier = threading.Barrier(2)

        def write(index):
            barrier.wait(timeout=5)
            return self.put(draft["client_revision"] + 1, content="window-" + str(index))

        with ThreadPoolExecutor(max_workers=2) as pool:
            responses = list(pool.map(write, (1, 2)))
        self.assertEqual(sorted(response.status_code for response in responses), [200, 409])
        saved = next(response.json()["data"]["draft"] for response in responses if response.status_code == 200)
        self.conflict(next(response for response in responses if response.status_code == 409))
        self.assertEqual(self.stored(), saved)
        self.assertEqual(saved["version"], draft["version"] + 1)

    def test_put_checks_current_scope_again_before_returning_saved_fields(self):
        save = PortalRuntime.save_workbench_notice_draft

        def revoke_after_save(**kwargs):
            result = save(**kwargs)
            self.grants[self.owner] = ["E"]
            return result

        with patch.object(PortalRuntime, "save_workbench_notice_draft", side_effect=revoke_after_save):
            response = self.put(1, content="private-result-after-revocation")
        self.conflict(response)
        self.assertNotIn("private-result-after-revocation", response.text)
        self.assertEqual(self.stored()["fields"]["progress"], "private-result-after-revocation")


if __name__ == "__main__":
    unittest.main()
