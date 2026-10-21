"""Synthetic, offline checks for current-only Feishu runtime identities."""
import asyncio
import copy
import hashlib
import sqlite3
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from urllib.parse import parse_qs, urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_personnel_password_login import _TempDirTestCase, PersonnelLoginHarness, _page, _user
from lan_bitable_template_portal import portal_auth as auth_module
from lan_bitable_template_portal import personnel_password_login as ppl
from lan_bitable_template_portal.feishu_identity import (
    TARGET_APP_ID, TARGET_DUTY_OPEN_IDS, canonical_duty_identity, duty_recipient_open_id,
)
from lan_bitable_template_portal.portal_service import (
    BUILDING_OPEN_ID_MAP, LI_SHILONG_OPEN_ID, MA_JINYU_OPEN_ID, PortalError,
)
from lan_bitable_template_portal.personnel_password_login import PersonnelPasswordLogin


class FeishuDirectoryIdentityTests(_TempDirTestCase):
    def setUp(self):
        super().setUp()
        refresher = patch.object(auth_module.config, "refresh_credentials", return_value=(TARGET_APP_ID, "synthetic-secret"), create=True)
        refresher.start()
        self.addCleanup(refresher.stop)
        self.h = PersonnelLoginHarness(self.root, migrated_grants=False)
        self.addCleanup(self.h.close)
        self.auth, self.login, self.service = self.h.auth, self.h.login, self.h.service
        self.auth._account_directory_loader = self.login.directory

    def row(self, rid="recPerson", oid="ou_New", **fields):
        return {"record_id": rid, "fields": {
            "姓名": "Synthetic Person", "员工姓名": _user("Synthetic Person", oid),
            "账号性质": "外部账号", "机楼/专业": "A楼", "体系岗位": "电气工程师",
            "密码": "pbkdf2_sha256$600000$synthetic$synthetic", **fields}}

    def load(self, rows=None, app_id=TARGET_APP_ID):
        auth_module.config.app_id = app_id
        self.service._pages = [_page([self.row()] if rows is None else rows)]
        return self.login.directory(force=True)

    def grant(self, oid, **values):
        with self.auth._lock:
            payload = self.auth._load_permissions_locked()
            payload["users"][oid] = {"name": "Synthetic", "enabled": True, "role": "building",
                "access_version": 1, "level": "L4", "modules": ["assistant"], "scopes": ["A"], **values}
            self.auth._save_permissions_locked(payload)

    def oauth(self, oid, union=""):
        state, redirect = "synthetic-state", "https://synthetic.test/callback"
        self.auth._state_store.put_auth_oauth_state(self.auth._secret_hash(state),
            redirect_uri=redirect, next_path="/", expires_at=time.time() + 600)
        with patch.object(self.auth, "_exchange_login_code", return_value={
                "open_id": oid, "union_id": union, "name": "Untrusted Display Name"}):
            return self.auth.complete_login(code="synthetic-code", state=state, redirect_uri=redirect)[0]

    def channel(self):
        from lan_bitable_template_portal.feishu_assistant import FeishuAssistant
        channel = object.__new__(FeishuAssistant)
        channel.config = {"app_id": TARGET_APP_ID}
        channel.runtime = SimpleNamespace(state_store=self.auth._state_store, auth_manager=self.auth)
        return channel

    def test_login_url_uses_latest_qt_app_and_preserves_state_and_redirect(self):
        redirect = "https://synthetic.test/callback?return=%2Fworkbench&literal=a+b&empty="
        qt_app = {"app_id": TARGET_APP_ID}

        def refresh():
            auth_module.config.app_id = qt_app["app_id"]
            return qt_app["app_id"], "synthetic-secret"

        with patch.object(auth_module, "token_manager") as tokens:
            tokens._credentials.side_effect = refresh
            for app_id in (TARGET_APP_ID, "cli_syntheticFuture"):
                qt_app["app_id"] = app_id
                url = self.auth.start_login(redirect_uri=redirect, next_path="/workbench-lite?scope=H")
                query = parse_qs(urlsplit(url).query)
                self.assertEqual(query["client_id"], [app_id])
                self.assertEqual(query["redirect_uri"], [redirect])
                persisted = self.auth._state_store.consume_auth_oauth_state(
                    self.auth._secret_hash(query["state"][0]), now=time.time())
                self.assertEqual(persisted["redirect_uri"], redirect)
                self.assertEqual(persisted["next"], "/workbench-lite?scope=H")

    def test_current_constants_and_duty_helpers_never_translate(self):
        self.assertEqual(BUILDING_OPEN_ID_MAP, TARGET_DUTY_OPEN_IDS)
        self.assertEqual(LI_SHILONG_OPEN_ID, "ou_083f6c32c2cd3da8ccfd62d800e742fd")
        self.assertEqual(MA_JINYU_OPEN_ID, "ou_cee45ea48df99def799710e782f5434c")
        for oid in TARGET_DUTY_OPEN_IDS.values():
            self.assertEqual(canonical_duty_identity(oid, TARGET_APP_ID), oid)
            self.assertEqual(duty_recipient_open_id(oid, TARGET_APP_ID), oid)
            self.assertIsNone(duty_recipient_open_id(oid, "cli_future"))
        self.assertEqual(canonical_duty_identity("ou_Old", TARGET_APP_ID), "ou_Old")
        self.assertIsNone(duty_recipient_open_id("ou_Old", TARGET_APP_ID))

    def test_unique_current_open_id_is_principal_regardless_of_account_nature(self):
        for nature in ("外部账号", "VNET"):
            with self.subTest(nature=nature):
                person = self.load([self.row(**{"账号性质": nature})])["people"]["recPerson"]
                self.assertEqual(self.login.login_identity(person), "ou_New")
                self.assertEqual(self.auth.directory_person("ou_New")["id"], "recPerson")
                self.assertEqual(self.auth.directory_person("person:recPerson"), {})
                self.assertEqual(self.auth._permission_user("person:recPerson"), {})
                if nature == "VNET":
                    with self.assertRaises(PortalError):
                        self.auth.create_personnel_session(person)
                else:
                    self.grant("ou_New", denied_scopes=["B"])
                    sid = self.auth.create_personnel_session(person)
                    self.assertEqual(self.auth.get_session(sid)["user"]["open_id"], "ou_New")
                    self.assertEqual(self.auth._permission_user("ou_New")["denied_scopes"], ["B"])

    def test_old_text_field_never_becomes_current_login_but_password_remains_available(self):
        row = self.row(**{"员工姓名": [], "飞书 open_id": "ou_Old"})
        person = self.load([row])["people"]["recPerson"]
        self.assertEqual(person["login_ids"], [])
        self.assertTrue(person["selectable"])
        self.assertEqual(self.login.login_identity(person), "person:recPerson")
        self.service._records[row["record_id"]] = row["fields"]
        with patch.object(ppl, "verify_password", return_value=True):
            _, sid = self.login.login({"person_id": "recPerson", "password": "synthetic"}, "fixture")
        self.assertEqual(self.auth.get_session(sid)["user"]["open_id"], "person:recPerson")
        self.assertIsNone(self.auth.current_feishu_open_id("ou_Old", TARGET_APP_ID))
        self.assertIsNone(self.auth.current_feishu_open_id("person:recPerson", TARGET_APP_ID))

    def test_vnet_without_current_user_field_is_not_selectable(self):
        person = self.load([self.row(**{"员工姓名": [], "飞书 open_id": "ou_Old", "账号性质": "VNET"})])["people"]["recPerson"]
        self.assertFalse(person["selectable"])
        self.assertEqual(person["login_ids"], [])

    def test_new_oauth_and_union_identity_are_exact_current_id(self):
        self.load()
        sid = self.oauth("ou_New", "on_Synthetic")
        self.assertEqual(self.auth.get_session(sid)["user"]["open_id"], "ou_New")
        self.assertEqual(self.auth._state_store.get_document("feishu_assistant_identity", "on_Synthetic")["open_id"], "ou_New")

    def test_old_permission_cannot_resurrect_sender_identity(self):
        self.load()
        self.grant("ou_Old", role="admin", level="L1")
        self.assertIsNone(self.channel()._sender_user({"sender_id": "ou_Old"}))

    def test_union_history_is_not_aliased_at_runtime(self):
        self.load()
        self.auth._state_store.put_document("feishu_assistant_identity", "on_Synthetic", {
            "open_id": "ou_Old", "union_id": "on_Synthetic", "name": "Synthetic Person"})
        with self.assertRaises(PortalError):
            self.oauth("ou_New", "on_Synthetic")
        self.assertEqual(self.auth._state_store.get_document("feishu_assistant_identity", "on_Synthetic")["open_id"], "ou_Old")

    def test_premigrated_persistent_cookie_hash_survives_restart(self):
        from lan_bitable_template_portal.feishu_identity_migration import migrate_identity_databases

        self.load([self.row(oid="ou_Old")], "cli_fixture")
        old = copy.deepcopy(self.login.snapshot["people"]["recPerson"])
        self.grant("ou_Old", denied_scopes=["B"], modules=["assistant", "learning"])
        password_sid = self.auth.create_personnel_session(old)
        oauth_sid = self.oauth("ou_Old")
        store = self.auth._state_store
        before = {sid: store.get_auth_session(self.auth._secret_hash(sid))
                  for sid in (password_sid, oauth_sid)}
        for session in before.values():
            self.assertEqual(session["user"]["open_id"], "ou_Old")
        history = {"id": "synthetic-conversation", "turns": [{"text": "original", "at": 1}]}
        store.put_document("lighthouse_ai", "conversation:ou_Old", history)
        store.put_document(ppl.DIRECTORY_NS, ppl.DIRECTORY_KEY, {**self.login.snapshot, "schema": 3})
        migrate_identity_databases(self.root, manifest={
            "version": 1, "target_app_id": TARGET_APP_ID, "unresolved": [],
            "mappings": [{"old_open_id": "ou_Old", "new_open_id": "ou_New",
                          "record_ids": ["recPerson"],
                          "evidence": ["personnel_snapshot", "target_hr_person_field"]}],
        })
        auth_module.config.app_id = TARGET_APP_ID
        with patch("lan_bitable_template_portal.portal_auth.get_data_file_path", side_effect=lambda name: str(self.root / name)):
            restarted = auth_module.PortalAuthManager()
        self.addCleanup(restarted.shutdown)
        self.addCleanup(restarted._state_store.shutdown_write_worker)
        current = PersonnelPasswordLogin(lambda: self.service, restarted._state_store, restarted)
        person = current.snapshot["people"]["recPerson"]
        self.assertEqual(current.snapshot["schema"], 4)
        self.assertEqual(current.snapshot["app_id"], TARGET_APP_ID)
        self.assertEqual(person["login_ids"], ["ou_New"])
        self.assertEqual(person["id"], old["id"])
        self.assertEqual(person["password_revision"], old["password_revision"])
        for sid in (password_sid, oauth_sid):
            session = restarted.get_session(sid)
            self.assertEqual(session["user"]["open_id"], "ou_New")
            self.assertEqual(session["created_at_ts"], before[sid]["created_at_ts"])
            self.assertEqual(session["allowed_scopes"], ["A"])
            self.assertIsNotNone(store.get_auth_session(self.auth._secret_hash(sid)))
        self.assertEqual(restarted._permission_user("ou_New")["denied_scopes"], ["B"])
        self.assertEqual(restarted._permission_user("ou_New")["modules"], ["assistant", "learning"])
        self.assertEqual(restarted._permission_user("ou_Old"), {})
        self.assertEqual(restarted._permission_user("person:recPerson"), {})
        self.assertEqual(store.get_document("lighthouse_ai", "conversation:ou_New"), history)
        self.assertIsNone(store.get_document("lighthouse_ai", "conversation:ou_Old"))

    def test_duty_oauth_does_not_create_grants_and_keeps_exact_id(self):
        self.load([])
        for scope, oid in TARGET_DUTY_OPEN_IDS.items():
            self.assertEqual(self.auth.get_session(self.oauth(oid))["allowed_scopes"], [])
            self.grant(oid, scopes=[scope])
            session = self.auth.get_session(self.oauth(oid))
            self.assertEqual(session["user"]["open_id"], oid)
            self.assertEqual(session["allowed_scopes"], [scope])

    def test_shared_hr_rows_preserve_duty_grants_and_fixed_superuser_full_access(self):
        for scope, oid in [*TARGET_DUTY_OPEN_IDS.items(), ("A", LI_SHILONG_OPEN_ID)]:
            with self.subTest(identity=oid):
                snapshot = self.load([self.row("recShared" + str(i), oid) for i in range(9)])
                self.assertEqual(len(snapshot["people"]), 9)
                self.assertTrue(all(not person["selectable"] for person in snapshot["people"].values()))
                self.grant(oid, scopes=[scope], modules=["assistant"],
                           role="admin" if scope == "H" or oid == LI_SHILONG_OPEN_ID else "building",
                           level="L0" if oid == LI_SHILONG_OPEN_ID else "L1" if scope == "H" else "L4")
                saved = copy.deepcopy(self.auth._state_store.get_auth_permissions())
                oauth_session = self.auth.get_session(self.oauth(oid))
                channel = self.channel()
                channel.sessions = {}
                sender = channel._sender_user({"sender_id": oid})
                bot_session = self.auth.get_session(channel._session(sender))
                self.assertEqual(sender["open_id"], oid)
                for session in (oauth_session, bot_session):
                    self.assertEqual(session["user"]["open_id"], oid)
                    self.assertEqual(session["allowed_scopes"], self.auth._admin_scopes() if oid == LI_SHILONG_OPEN_ID else [scope])
                self.assertEqual(self.auth.account_profile(oid)["modules"],
                                 list(auth_module.default_modules('L0')) if oid == LI_SHILONG_OPEN_ID else ["assistant"])
                self.assertEqual(self.auth._state_store.get_auth_permissions(), saved)
                self.assertTrue(all(not person["selectable"] for person in snapshot["people"].values()))

    def test_fixed_sender_requires_current_app_saved_scope_module_and_enabled_account(self):
        from openclaw_service.assistant.lighthouse_ai import AssistantError
        oid = TARGET_DUTY_OPEN_IDS["A"]
        rows = [self.row("recShared" + str(i), oid) for i in range(9)]
        self.load(rows)
        channel = self.channel()
        with self.assertRaises(AssistantError):
            channel._sender_user({"sender_id": oid})
        self.assertEqual(self.auth._permission_user(oid), {})
        for changes in ({"scopes": []}, {"modules": []}, {"enabled": False}):
            with self.subTest(changes=changes):
                self.grant(oid, **changes)
                with self.assertRaises(AssistantError):
                    channel._sender_user({"sender_id": oid})
        self.grant(oid)
        channel.config["app_id"] = "cli_syntheticFuture"
        with self.assertRaises(AssistantError) as mismatch:
            channel._sender_user({"sender_id": oid})
        self.assertEqual(mismatch.exception.status, 503)
        self.load(rows, app_id="cli_syntheticFuture")
        with self.assertRaises(AssistantError):
            channel._sender_user({"sender_id": oid})

    def test_ma_and_regular_employees_still_reject_duplicate_and_inactive_hr(self):
        from openclaw_service.assistant.lighthouse_ai import AssistantError
        for oid in (MA_JINYU_OPEN_ID, "ou_SyntheticEmployee"):
            for reason in ("duplicate", "inactive"):
                with self.subTest(identity=oid, reason=reason):
                    rows = ([self.row("recShared" + str(i), oid) for i in range(9)] if reason == "duplicate"
                            else [self.row(oid=oid, **{"离职/异动情况": "离职"})])
                    self.load(rows)
                    self.grant(oid, role="admin", level="L1")
                    with self.assertRaises(AssistantError):
                        self.channel()._sender_user({"sender_id": oid})
                    with self.assertRaises(PortalError):
                        self.oauth(oid)

    def test_migrated_current_id_password_cookie_is_retained_but_old_id_is_rejected(self):
        self.load()
        sid = self.auth.create_personnel_session(self.login.snapshot["people"]["recPerson"])
        cookie_hash = self.auth._secret_hash(sid)
        session = self.auth._state_store.get_auth_session(cookie_hash)
        self.assertEqual(session["user"]["open_id"], "ou_New")
        self.auth._sessions.clear()
        self.assertEqual(self.auth.get_session(sid)["user"]["open_id"], "ou_New")
        for identity in ("ou_Old", "person:recPerson"):
            with self.subTest(identity=identity):
                session["user"]["open_id"] = identity
                self.auth._state_store.put_auth_session(cookie_hash, session)
                self.auth._sessions.clear()
                self.assertIsNone(self.auth.get_session(sid))

    def test_verified_oauth_without_hr_can_reach_permission_application(self):
        self.load([])
        session = self.auth.get_session(self.oauth("ou_NewApplicant"))
        self.assertEqual(session["user"]["open_id"], "ou_NewApplicant")
        self.assertEqual(session["allowed_scopes"], [])
        self.assertEqual(self.auth.account_profile("ou_NewApplicant")["modules"], [])

    def test_verified_oauth_keeps_migrated_grants_during_directory_outage(self):
        self.load()
        self.grant("ou_New")
        self.login.snapshot = {**self.login.snapshot, "loaded_at": time.time() - 301}
        with patch.object(self.auth, "_account_directory_loader", side_effect=RuntimeError("synthetic outage")):
            session = self.auth.get_session(self.oauth("ou_New"))
        self.assertEqual(session["user"]["open_id"], "ou_New")
        self.assertEqual(session["allowed_scopes"], ["A"])
        self.assertIsNone(self.auth.current_feishu_open_id("ou_New", TARGET_APP_ID))

    def test_loading_does_not_add_reenable_or_promote_administrators(self):
        self.load([])
        self.assertEqual(self.auth.get_permissions_payload()["users"], [])
        for oid in (MA_JINYU_OPEN_ID, TARGET_DUTY_OPEN_IDS["H"]):
            self.grant(oid, enabled=False, modules=[], scopes=[])
            self.auth._permissions_cache = None
            self.assertFalse(self.auth._permission_user(oid)["enabled"])
            self.assertEqual(self.auth.account_profile(oid)["modules"], [])
            self.grant(oid, modules=[], scopes=[])
            profile = self.auth.account_profile(oid)
            self.assertEqual(profile["level"], "L4")
            self.assertEqual(profile["scope_rules"], [])
            self.assertFalse(profile["can_manage_accounts"])
        self.grant(LI_SHILONG_OPEN_ID, enabled=False, modules=[], scopes=[])
        saved = copy.deepcopy(self.auth._state_store.get_auth_permissions())
        self.auth._permissions_cache = None
        profile = self.auth.account_profile(LI_SHILONG_OPEN_ID)
        self.assertEqual(profile['level'], 'L0')
        self.assertEqual(profile['scope_rules'], self.auth._admin_scopes())
        self.assertEqual(set(profile['modules']), set(auth_module.default_modules('L0')))
        self.assertTrue(profile['can_manage_accounts'])
        self.assertEqual(self.auth._state_store.get_auth_permissions(), saved)

    def test_sender_bindings_never_override_current_hr_id(self):
        from openclaw_service.assistant.lighthouse_ai import AssistantError
        for evidence in ("record_id", "admin", "union_id", "name"):
            self.auth._state_store.put_document("feishu_assistant_sender", "cli_old:" + evidence, {
                "record_id": "recPerson", "open_id": "ou_Old", "matched_by": evidence})
        person = self.load()["people"]["recPerson"]
        self.assertTrue(person["selectable"])
        self.assertEqual(person["login_ids"], ["ou_New"])
        channel = self.channel()
        self.assertEqual(channel._sender_user({"sender_id": "ou_New"})["open_id"], "ou_New")
        with self.assertRaises(AssistantError):
            channel._sender_user({"sender_id": "ou_Old"}, record_id="recPerson")

    def test_name_or_union_match_cannot_authenticate_another_sender(self):
        from openclaw_service.assistant.lighthouse_ai import AssistantError
        self.load()
        channel = self.channel()
        with self.assertRaises(AssistantError):
            channel._sender_user({"sender_id": "ou_Other"}, name="Synthetic Person")
        self.auth._state_store.put_document("feishu_assistant_identity", "on_Synthetic", {
            "open_id": "ou_New", "union_id": "on_Synthetic"})
        channel.messenger = SimpleNamespace(sender_record=AsyncMock(return_value=None))
        with self.assertRaises(AssistantError):
            asyncio.run(channel._resolve_user({"sender_id": "ou_Other", "union_id": "on_Synthetic"}))

    def test_current_sender_and_same_union_keep_verified_snapshot_during_hr_outage(self):
        self.load()
        self.login.snapshot = {**self.login.snapshot, "loaded_at": time.time() - 301}
        self.auth._state_store.put_document("feishu_assistant_identity", "on_Synthetic", {
            "open_id": "ou_New", "union_id": "on_Synthetic"})
        channel = self.channel()
        with patch.object(self.auth, "_account_directory_loader", side_effect=AssertionError("No HR network read")):
            self.assertEqual(channel._sender_user({"sender_id": "ou_New"})["open_id"], "ou_New")
            self.assertEqual(channel._user("on_Synthetic")["open_id"], "ou_New")
        self.assertIsNone(self.auth.current_feishu_open_id("ou_New", TARGET_APP_ID))

    def test_recipient_requires_fresh_current_app_exact_id(self):
        self.load()
        before = self.service._request_payload_calls
        self.assertEqual(self.auth.current_feishu_open_id("ou_New", TARGET_APP_ID), "ou_New")
        self.assertIsNone(self.auth.current_feishu_open_id("ou_Old", TARGET_APP_ID))
        self.assertEqual(self.service._request_payload_calls, before)
        auth_module.config.app_id = "cli_future"
        self.assertEqual(self.auth.directory_person("ou_New"), {})
        auth_module.config.app_id = TARGET_APP_ID
        self.login.snapshot = {**self.login.snapshot, "loaded_at": time.time() - 301}
        self.assertIsNone(self.auth.current_feishu_open_id("ou_New", TARGET_APP_ID))

    def test_inactive_duplicate_or_disabled_current_ids_fail_closed(self):
        for kind in ("inactive", "duplicate", "disabled"):
            with self.subTest(kind=kind):
                rows = [self.row()]
                if kind == "inactive":
                    rows[0]["fields"]["离职/异动情况"] = "离职"
                if kind == "duplicate":
                    rows.append(self.row("recOther"))
                self.load(rows)
                if kind == "disabled":
                    self.grant("ou_New", enabled=False)
                self.assertIsNone(self.auth.current_feishu_open_id("ou_New", TARGET_APP_ID))
                with self.assertRaises(PortalError):
                    self.oauth("ou_New")

    def test_current_source_replacement_wins_and_multiple_users_are_ambiguous(self):
        self.load()
        person = self.login.person(self.row(oid="ou_Replacement"))
        self.assertTrue(person["selectable"])
        self.assertEqual(person["login_ids"], ["ou_Replacement"])
        row = self.row()
        row["fields"]["员工姓名"] += _user("Another Synthetic", "ou_Other")
        self.assertFalse(self.login.person(row)["selectable"])

    def test_legacy_snapshot_is_refreshed_not_used_as_current_identity(self):
        self.load()
        self.login.snapshot = {**self.login.snapshot, "schema": 3}
        self.login.snapshot["people"]["recPerson"].update(login_ids=["ou_Old"], feishu_open_ids={TARGET_APP_ID: "ou_New"})
        self.assertEqual(self.auth.directory_person("ou_Old"), {})
        self.service._pages = [_page([self.row()])]
        person = self.login.directory()["people"]["recPerson"]
        self.assertEqual(person["login_ids"], ["ou_New"])
        self.assertNotIn("feishu_open_ids", person)

    def test_single_record_check_cannot_bypass_duplicate_current_id(self):
        self.load([self.row(), self.row("recOther")])
        with self.assertRaises(ppl.PasswordLoginError):
            self.login.login_identity(self.login.person(self.row()))

    def test_switch_during_fetch_never_publishes_mixed_app_snapshot(self):
        self.load(app_id="cli_fixture")
        original = copy.deepcopy(self.login.snapshot)
        self.service._pages = [_page([self.row()])]
        with patch.object(self.auth, "current_feishu_app_id", side_effect=[TARGET_APP_ID, "cli_future"]):
            with self.assertRaises(ppl.PasswordLoginError):
                self.login.directory()
        self.assertEqual(self.login.snapshot, original)

    def test_absent_quarantine_table_does_not_block_current_or_new_accounts(self):
        self.load()
        self.assertFalse(self.auth._identity_quarantined("ou_New"))
        self.assertIsNotNone(self.auth.get_session(self.oauth("ou_LaterApplicant")))

    def test_quarantined_identity_cannot_use_cookie_or_bot_but_records_are_preserved(self):
        from lan_bitable_template_portal.feishu_identity_migration import QUARANTINE_TABLE
        from openclaw_service.assistant.lighthouse_ai import AssistantError
        self.load()
        self.grant("ou_Old", role="admin", level="L1")
        sid = self.oauth("ou_Old")
        cookie_hash = self.auth._secret_hash(sid)
        original_session = self.auth._state_store.get_auth_session(cookie_hash)
        original_permissions = self.auth._state_store.get_auth_permissions()
        self.auth._state_store.put_document("synthetic_history", "ou_Old", {"turns": ["synthetic"]})
        with sqlite3.connect(self.auth._state_store.db_path) as db:
            db.execute(f"CREATE TABLE {QUARANTINE_TABLE} (table_name TEXT,key_hash TEXT,identity_hash TEXT,"
                       "reason TEXT,plan_hash TEXT,original_state_json TEXT)")
            db.execute(f"INSERT INTO {QUARANTINE_TABLE} VALUES(?,?,?,?,?,?)", (
                "auth_permissions", "synthetic-key", hashlib.sha256(b"ou_Old").hexdigest(),
                "unverified_identity", "synthetic-plan", "{}"))
        self.auth._sessions.clear()
        self.assertTrue(self.auth._open_id_explicitly_disabled("ou_Old"))
        self.assertEqual(self.auth.account_profile("ou_Old")["modules"], [])
        self.assertIsNone(self.auth.get_session(sid))
        self.assertEqual(self.auth._state_store.get_auth_session(cookie_hash), original_session)
        self.assertEqual(self.auth._state_store.get_auth_permissions(), original_permissions)
        self.assertEqual(self.auth._state_store.get_document("synthetic_history", "ou_Old"), {"turns": ["synthetic"]})
        with self.assertRaises(AssistantError):
            self.channel()._sender_user({"sender_id": "ou_Old"})
        with self.assertRaises(PortalError):
            self.oauth("ou_Old")
        self.assertFalse(self.auth._identity_quarantined("ou_New"))
        self.assertIsNotNone(self.auth.get_session(self.oauth("ou_New")))
        self.assertIsNotNone(self.auth.get_session(self.oauth("ou_LaterApplicant")))

    def test_quarantine_read_error_denies_without_revoking_stored_cookie(self):
        self.load()
        sid = self.oauth("ou_New")
        cookie_hash = self.auth._secret_hash(sid)
        original = self.auth._state_store.get_auth_session(cookie_hash)
        with patch("lan_bitable_template_portal.feishu_identity_migration.is_identity_quarantined",
                   side_effect=sqlite3.OperationalError("synthetic read failure")):
            with self.assertRaises(auth_module.PortalAuthUnavailableError) as blocked:
                self.auth._open_id_explicitly_disabled("ou_New")
            self.assertEqual(blocked.exception.status, 503)
            with self.assertRaises(auth_module.PortalAuthUnavailableError):
                self.auth.get_session(sid)
        self.assertEqual(self.auth._state_store.get_auth_session(cookie_hash), original)
        self.assertEqual(self.auth.get_session(sid)["user"]["open_id"], "ou_New")

    def test_second_quarantine_read_failure_keeps_cookie_and_recovers_without_login(self):
        self.load()
        sid = self.oauth("ou_New")
        cookie_hash = self.auth._secret_hash(sid)
        original = self.auth._state_store.get_auth_session(cookie_hash)
        with patch("lan_bitable_template_portal.feishu_identity_migration.is_identity_quarantined",
                   side_effect=[False, sqlite3.OperationalError("synthetic second read failure")]) as reads:
            with self.assertRaises(auth_module.PortalAuthUnavailableError) as blocked:
                self.auth.get_session(sid)
            self.assertEqual(blocked.exception.status, 503)
            self.assertEqual(blocked.exception.status_code, 503)
            self.assertEqual(reads.call_count, 2)
        self.assertEqual(self.auth._state_store.get_auth_session(cookie_hash), original)
        self.assertEqual(self.auth.get_session(sid)["user"]["open_id"], "ou_New")

    def test_malformed_quarantine_table_is_not_treated_as_absent(self):
        from lan_bitable_template_portal.feishu_identity_migration import QUARANTINE_TABLE
        self.load()
        with sqlite3.connect(self.auth._state_store.db_path) as db:
            db.execute(f"CREATE TABLE {QUARANTINE_TABLE} (key_hash TEXT)")
        with self.assertRaises(auth_module.PortalAuthUnavailableError):
            self.auth._identity_quarantined("ou_New")


if __name__ == "__main__":
    import unittest
    unittest.main()
