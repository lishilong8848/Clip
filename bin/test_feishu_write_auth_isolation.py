"""Offline request guards and delivery-directory ownership; no live configuration."""
import ast
import copy
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import httpx
import upload_event_module.services as services
from upload_event_module.services.http_client import FeishuHTTPError, FeishuHttpClient
from lan_bitable_template_portal.message_delivery import (
    DeliveryError, FeishuDeliveryTransport, MessageDelivery, DIRECTORY_NS,
)


class PortalError(RuntimeError):
    pass


class SyntheticPath:
    name = "fixture.bin"

    def __init__(self, value):
        pass

    def resolve(self):
        return self

    def is_file(self):
        return True

    def stat(self):
        return SimpleNamespace(st_size=3)

    def __str__(self):
        return self.name


def portal_helpers(namespace):
    # Load the real bounded methods without importing Qt, its settings, or migrations.
    path = Path(__file__).parent / "lan_bitable_template_portal" / "portal_service.py"
    tree = ast.parse(path.read_text(encoding="utf-8-sig"))
    namespace["source_tree"] = tree
    guard = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "_feishu_write_auth")
    exec(compile(ast.Module(body=[guard], type_ignores=[]), str(path), "exec"), namespace)
    owner = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "MaintenancePortalService")
    methods = [n for n in owner.body if isinstance(n, ast.FunctionDef)
               and n.name in {"_create_record_fields", "_upload_drive_media_file", "_request_payload"}]
    exec(compile(ast.Module(body=methods, type_ignores=[]), str(path), "exec"), namespace)
    return type("PortalHelpers", (), {n.name: namespace[n.name] for n in methods})


def portal_write_closures(namespace):
    targets = {
        "ensure_deletion_audit_schema": ("do_create",),
        "ensure_maintenance_work_order_schema": ("create_attachment_field",),
        "_patch_record_fields_exact": ("do_update",),
        "_update_bitable_field_definition": ("do_update",),
        "_delete_repair_followup_field_definition": ("do_delete",),
        "initialize_repair_followup_schema": ("do_create",),
        "_ensure_repair_followup_select_options": ("do_update",),
        "_create_record_fields": ("do_create",),
        "_delete_record_fields": ("do_delete",),
        "_ensure_drill_archive_schema": ("create_field", "update_year"),
    }
    owner = next(n for n in namespace["source_tree"].body if isinstance(n, ast.ClassDef) and n.name == "MaintenancePortalService")
    exercises = {}
    for method in owner.body:
        if not isinstance(method, ast.FunctionDef) or method.name not in targets:
            continue
        guard = next(n for n in ast.walk(method) if isinstance(n, ast.Assign)
                     and isinstance(n.value, ast.Call) and isinstance(n.value.func, ast.Name)
                     and n.value.func.id == "_feishu_write_auth")
        for name in targets[method.name]:
            closure = next(n for n in ast.walk(method) if isinstance(n, ast.FunctionDef) and n.name == name)
            block = next(n.body for n in ast.walk(method) if isinstance(getattr(n, "body", None), list) and closure in n.body)
            call = next(n for n in block if isinstance(n, ast.Assign) and isinstance(n.value, ast.Call)
                        and isinstance(n.value.func, ast.Name) and n.value.func.id == name)
            retry = block[block.index(call) + 1]
            assert isinstance(retry, ast.If)
            wrapper = ast.parse("def exercise(self):\n    pass").body[0]
            wrapper.body = copy.deepcopy([guard, closure, call, retry,
                                          ast.Return(value=ast.Name(id=call.targets[0].id, ctx=ast.Load()))])
            exec(compile(ast.fix_missing_locations(ast.Module(body=[wrapper], type_ignores=[])), "<bounded-write-closure>", "exec"), namespace)
            exercises[method.name + "." + name] = namespace["exercise"]
    return exercises


def sdk_upload_call(namespace):
    owner = next(n for n in namespace["source_tree"].body if isinstance(n, ast.ClassDef) and n.name == "MaintenancePortalService")
    method = next(n for n in owner.body if isinstance(n, ast.FunctionDef) and n.name == "_upload_large_bitable_file")
    guard = next(n for n in method.body if isinstance(n, ast.Assign) and isinstance(n.value, ast.Call)
                 and isinstance(n.value.func, ast.Name) and n.value.func.id == "_feishu_write_auth")
    token = next(n for n in method.body if isinstance(n, ast.Assign) and isinstance(n.targets[0], ast.Name) and n.targets[0].id == "token")
    missing = method.body[method.body.index(token) + 1]
    call = next(n for n in method.body if isinstance(n, ast.FunctionDef) and n.name == "call")
    wrapper = ast.parse("def sdk_call(self):\n    pass").body[0]
    wrapper.body = copy.deepcopy([guard, token, missing, call, ast.Return(value=ast.Name(id="call", ctx=ast.Load()))])
    exec(compile(ast.fix_missing_locations(ast.Module(body=[wrapper], type_ignores=[])), "<bounded-sdk-upload>", "exec"), namespace)
    return namespace["sdk_call"]


class WriteAuthIsolationTests(unittest.TestCase):
    kinds = ("create", "upload", "message", "message_file")

    @classmethod
    def setUpClass(cls):
        cls.namespace = {"Any": object, "Path": SyntheticPath, "PortalError": PortalError,
                         "FeishuHTTPError": FeishuHTTPError, "FeishuHttpClient": FeishuHttpClient,
                         "TOKEN_ERROR_CODES": {99991663}, "REPAIR_MANAGEMENT_REPAIR_TABLE_ID": "other",
                         "REPAIR_FOLLOWUP_TABLE_ID": "other", "REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME": "synthetic",
                         "url": "https://fixture.invalid", "fields_url": "https://fixture.invalid",
                         "field_name": "synthetic", "attachment_name": "synthetic", "context": "synthetic",
                         "context_label": "synthetic", "request_body": {"synthetic": True},
                         "client_token": "stable-fixture-uuid",
                         "body": {"synthetic": True}, "fields": {"synthetic": True}}
        cls.portal_type = portal_helpers(cls.namespace)
        cls.closures = portal_write_closures(cls.namespace)
        cls.sdk_call = staticmethod(sdk_upload_call(cls.namespace))

    def setUp(self):
        self.credentials = ["cli_fixture_a", "synthetic-secret-a"]
        self.token = "fixture-token"
        self.manager = SimpleNamespace(_credentials=Mock(side_effect=lambda: tuple(self.credentials)))
        self.auth = SimpleNamespace(token_manager=self.manager)
        self.refresh = Mock(side_effect=self.refresh_token)
        self.robot = SimpleNamespace(_get_tenant_access_token=Mock(side_effect=self.robot_token),
                                     _is_token_error_result=lambda value: value.get("code") == 99991663)
        modules = {"upload_event_module.services.feishu_token_manager": self.auth,
                   "upload_event_module.services.robot_webhook": self.robot}
        self.modules = patch.dict(sys.modules, modules)
        self.modules.start()
        self.addCleanup(self.modules.stop)
        robot_patch = patch.object(services, "robot_webhook", self.robot, create=True)
        robot_patch.start()
        self.addCleanup(robot_patch.stop)
        self.namespace.update(config=SimpleNamespace(user_token=""), ensure_feishu_token=lambda: self.token,
                              feishu_auth_error=lambda: "Synthetic application authentication unavailable",
                              refresh_feishu_token=self.refresh)

    def refresh_token(self):
        self.token = "fixture-refreshed-token"
        return self.token

    def robot_token(self, *, force_refresh=False):
        return (self.refresh() if force_refresh else self.token), ""

    def headers(self):
        if not self.token:
            raise PortalError("No synthetic authentication")
        return {"Authorization": "Bearer " + self.token}

    @staticmethod
    def success():
        return {"code": 0, "data": {"record": {"record_id": "fixture-record"},
                                    "file_token": "fixture-file", "message_id": "fixture-message"}}

    def case(self, kind, replies):
        self.credentials[:] = ["cli_fixture_a", "synthetic-secret-a"]
        self.token = "fixture-token"
        self.refresh.reset_mock(side_effect=True)
        self.refresh.side_effect = self.refresh_token
        self.robot._get_tenant_access_token.reset_mock(side_effect=True)
        self.robot._get_tenant_access_token.side_effect = self.robot_token
        post = Mock(side_effect=replies)
        service = self.portal_type()
        service._auth_headers = Mock(side_effect=self.headers)
        service._request_payload = post
        service._http_client = SimpleNamespace(request_file_json=post)
        service._write_http_client = SimpleNamespace(retries=0)
        service._repair_physical_record_fields = lambda table, fields: fields
        service._repair_create_client_token = lambda table, fields: "stable-fixture-uuid"
        service._repair_before_write = Mock(return_value={"phase": "writing"})
        service._repair_after_write = Mock()
        service._repair_rejected_write = Mock()
        service._repair_physical_field_name = lambda table, name: name
        service._created_record_id = lambda value: value.get("data", {}).get("record", {}).get("record_id")
        if kind == "create":
            invoke = lambda: service._create_record_fields(app_token="fixture-base", table_id="fixture-table", fields={"synthetic": True})
        elif kind == "upload":
            invoke = lambda: service._upload_drive_media_file(file_path="fixture.bin", file_name="fixture.bin",
                parent_type="bitable_file", parent_node="fixture-base", size=3, context="Synthetic upload")
        else:
            self.robot._HTTP_CLIENT = SimpleNamespace(request_json=post, request_file_json=post)
            transport = FeishuDeliveryTransport()
            invoke = (lambda: transport._call("messages", payload={"uuid": "stable-fixture-uuid"})) if kind == "message" else (
                lambda: transport._call("files", file={"path": "fixture.bin", "name": "fixture.bin"}))
        return invoke, post, service

    def test_rejection_after_app_or_secret_change_cannot_refresh_or_replay(self):
        for kind in self.kinds:
            for index in (0, 1):
                with self.subTest(kind=kind, credential=index):
                    def rejected(*args, **kwargs):
                        self.credentials[index] += "-changed"
                        return {"code": 99991663}
                    invoke, post, service = self.case(kind, rejected)
                    with self.assertRaises((PortalError, DeliveryError)):
                        invoke()
                    self.assertEqual(post.call_count, 1)
                    self.refresh.assert_not_called()
                    service._repair_rejected_write.assert_not_called()

    def test_change_during_refresh_cannot_replay(self):
        for kind in self.kinds:
            for index in (0, 1):
                with self.subTest(kind=kind, credential=index):
                    invoke, post, _ = self.case(kind, [{"code": 99991663}, self.success()])
                    def refresh():
                        self.credentials[index] += "-changed"
                        return self.refresh_token()
                    self.refresh.side_effect = refresh
                    with self.assertRaises((PortalError, DeliveryError)):
                        invoke()
                    self.assertEqual(post.call_count, 1)
                    self.refresh.assert_called_once()

    def test_change_during_initial_authentication_sends_nothing(self):
        for kind in self.kinds:
            with self.subTest(kind=kind):
                invoke, post, service = self.case(kind, [self.success()])
                def authenticate(*args, **kwargs):
                    self.credentials[1] += "-changed"
                    return self.headers() if kind == "create" else (self.token, "") if kind.startswith("message") else self.token
                if kind == "create":
                    service._auth_headers.side_effect = authenticate
                elif kind == "upload":
                    self.namespace["ensure_feishu_token"] = authenticate
                else:
                    self.robot._get_tenant_access_token.side_effect = authenticate
                try:
                    with self.assertRaises((PortalError, DeliveryError)):
                        invoke()
                    post.assert_not_called()
                finally:
                    self.namespace["ensure_feishu_token"] = lambda: self.token

    def test_same_credentials_recover_once_without_changing_payload_or_uuid(self):
        for kind in self.kinds:
            with self.subTest(kind=kind):
                invoke, post, _ = self.case(kind, [{"code": 99991663}, self.success()])
                invoke()
                self.assertEqual(post.call_count, 2)
                self.refresh.assert_called_once()
                first, second = post.call_args_list
                self.assertNotEqual(first.kwargs["headers"], second.kwargs["headers"])
                for key in ("json_payload", "params", "data", "file_path"):
                    self.assertEqual(first.kwargs.get(key), second.kwargs.get(key))
                if kind != "create":
                    self.assertEqual(second.kwargs["retries"], 0)

    def test_repeated_rejection_is_bounded(self):
        for kind in self.kinds:
            with self.subTest(kind=kind):
                invoke, post, _ = self.case(kind, [{"code": 99991663}] * 3)
                with self.assertRaises((PortalError, DeliveryError)):
                    invoke()
                self.assertEqual(post.call_count, 2)
                self.refresh.assert_called_once()

    def test_lost_write_response_is_never_refreshed_or_replayed(self):
        for kind in self.kinds:
            with self.subTest(kind=kind):
                invoke, post, service = self.case(kind, TimeoutError("Synthetic lost response"))
                with self.assertRaises((PortalError, TimeoutError)):
                    invoke()
                self.assertEqual(post.call_count, 1)
                self.refresh.assert_not_called()
                service._repair_rejected_write.assert_not_called()
                service._repair_after_write.assert_not_called()

    def test_original_success_survives_subsequent_app_or_secret_change(self):
        for kind in self.kinds:
            for index in (0, 1):
                with self.subTest(kind=kind, credential=index):
                    def accepted(*args, **kwargs):
                        self.credentials[index] += "-changed"
                        return self.success()
                    invoke, post, service = self.case(kind, accepted)
                    result = invoke()
                    self.assertTrue(result)
                    post.assert_called_once()
                    self.refresh.assert_not_called()
                    service._repair_rejected_write.assert_not_called()
                    if kind == "create":
                        service._repair_after_write.assert_called_once_with({"phase": "writing"}, "fixture-record")

    def test_missing_token_or_failed_credential_validation_sends_nothing(self):
        for kind in self.kinds:
            with self.subTest(kind=kind):
                invoke, post, _ = self.case(kind, [self.success()])
                self.token = ""
                with self.assertRaises((PortalError, DeliveryError)):
                    invoke()
                post.assert_not_called()
                self.manager._credentials.side_effect = RuntimeError("Synthetic Qt unavailable")
                with self.assertRaises(RuntimeError):
                    invoke()
                post.assert_not_called()
                self.manager._credentials.side_effect = lambda: tuple(self.credentials)

    def test_failed_upload_refresh_does_not_reuse_cached_rejected_token(self):
        invoke, post, _ = self.case("upload", [{"code": 99991663}, self.success()])
        self.namespace["config"].user_token = "rejected-fixture-token"
        self.refresh.side_effect = None
        self.refresh.return_value = None
        with self.assertRaises(PortalError):
            invoke()
        post.assert_called_once()

    def test_all_reported_portal_write_closures_are_bound_and_do_not_replay_uncertain_writes(self):
        self.assertEqual(len(self.closures), 11)
        for name, exercise in self.closures.items():
            for scenario in ("same", "before_refresh", "during_refresh", "before_send", "lost", "accepted"):
                for index in (0, 1):
                    with self.subTest(closure=name, scenario=scenario, credential=index):
                        def changed_reply(*args, **kwargs):
                            self.credentials[index] += "-changed"
                            return self.success() if scenario == "accepted" else {"code": 99991663}
                        replies = ([{"code": 99991663}, self.success()] if scenario in {"same", "during_refresh"}
                                   else TimeoutError("Synthetic lost response") if scenario == "lost" else changed_reply)
                        _, post, service = self.case("create", replies)
                        if scenario == "during_refresh":
                            def refresh():
                                self.credentials[index] += "-changed"
                                return self.refresh_token()
                            self.refresh.side_effect = refresh
                        if scenario == "before_send":
                            def headers():
                                self.credentials[index] += "-changed"
                                return self.headers()
                            service._auth_headers.side_effect = headers
                        if scenario in {"same", "accepted"}:
                            self.assertEqual(exercise(service)["code"], 0)
                        else:
                            with self.assertRaises((PortalError, TimeoutError)):
                                exercise(service)
                        self.assertEqual(post.call_count, 0 if scenario == "before_send" else 2 if scenario == "same" else 1)
                        self.assertEqual(self.refresh.call_count, int(scenario in {"same", "during_refresh"}))
                        for call in post.call_args_list:
                            self.assertEqual(call.kwargs["retries"], 0)

    def test_underlying_http_client_does_not_retry_a_lost_write(self):
        _, _, service = self.case("create", [])
        calls = []
        def lost(request):
            calls.append(request.method)
            raise httpx.ReadTimeout("Synthetic lost response")
        client = FeishuHttpClient(transport=httpx.MockTransport(lost), retries=4)
        self.addCleanup(client.close)
        service._http_client = service._write_http_client = client
        service._request_payload = self.portal_type._request_payload.__get__(service)
        with self.assertRaises(PortalError):
            service._create_record_fields(app_token="fixture", table_id="fixture", fields={"synthetic": True})
        self.assertEqual(calls, ["POST"])
        self.refresh.assert_not_called()

    def test_sdk_upload_only_recovers_explicit_rejection_with_the_same_credentials(self):
        rejected = SimpleNamespace(success=lambda: False, code=99991663, msg="Synthetic rejection")
        accepted = SimpleNamespace(success=lambda: True)
        for scenario in ("same", "before_refresh", "during_refresh", "failed_refresh"):
            for index in (0, 1):
                with self.subTest(scenario=scenario, credential=index):
                    _, _, service = self.case("create", [])
                    call = self.sdk_call(service)
                    def first(token):
                        if scenario == "before_refresh":
                            self.credentials[index] += "-changed"
                        return rejected
                    fn = Mock(side_effect=[rejected, accepted])
                    if scenario == "before_refresh":
                        fn.side_effect = first
                    if scenario == "during_refresh":
                        def refresh():
                            self.credentials[index] += "-changed"
                            return self.refresh_token()
                        self.refresh.side_effect = refresh
                    if scenario == "failed_refresh":
                        self.namespace["config"].user_token = "rejected-fixture-token"
                        self.refresh.side_effect = None
                        self.refresh.return_value = None
                    if scenario == "same":
                        self.assertIs(call("Synthetic", fn), accepted)
                        self.assertEqual([c.args[0] for c in fn.call_args_list], ["fixture-token", "fixture-refreshed-token"])
                    else:
                        with self.assertRaises(PortalError):
                            call("Synthetic", fn)
                        fn.assert_called_once()
                    self.assertEqual(self.refresh.call_count, int(scenario != "before_refresh"))

    def test_sdk_lost_invalid_or_rejected_write_is_not_blindly_replayed(self):
        for reply in (TimeoutError("Synthetic lost response"), None,
                      SimpleNamespace(success=lambda: False, code=1255002, msg="Synthetic unknown result")):
            with self.subTest(reply=type(reply).__name__):
                _, _, service = self.case("create", [])
                fn = Mock(side_effect=reply) if isinstance(reply, Exception) else Mock(return_value=reply)
                with self.assertRaises(PortalError):
                    self.sdk_call(service)("Synthetic", fn)
                fn.assert_called_once()
                self.refresh.assert_not_called()

    def test_sdk_success_is_kept_but_next_phase_cannot_switch_app_or_secret(self):
        for index in (0, 1):
            with self.subTest(credential=index):
                _, _, service = self.case("create", [])
                call = self.sdk_call(service)
                accepted = SimpleNamespace(success=lambda: True)
                def write(token):
                    self.credentials[index] += "-changed"
                    return accepted
                fn = Mock(side_effect=write)
                self.assertIs(call("Synthetic", fn), accepted)
                with self.assertRaises(PortalError):
                    call("Next phase", fn)
                fn.assert_called_once()
                self.refresh.assert_not_called()

    def test_sdk_missing_token_never_calls_the_sdk(self):
        _, _, service = self.case("create", [])
        self.token = ""
        fn = Mock()
        with self.assertRaises(PortalError):
            self.sdk_call(service)("Synthetic", fn)
        fn.assert_not_called()


class DirectoryAuthIsolationTests(unittest.TestCase):
    def setUp(self):
        self.credentials = ["cli_fixture_a", "synthetic-secret-a"]
        manager = SimpleNamespace(_credentials=lambda: tuple(self.credentials))
        modules = patch.dict(sys.modules, {
            "upload_event_module.services.feishu_token_manager": SimpleNamespace(token_manager=manager),
            "lan_bitable_template_portal.personnel_password_login": SimpleNamespace(DIRECTORY_NS="synthetic-logins", DIRECTORY_KEY="directory"),
        })
        modules.start()
        self.addCleanup(modules.stop)
        self.documents = {}
        self.store = SimpleNamespace(db_path=Path("synthetic/state.sqlite3"),
            get_document=lambda ns, key: copy.deepcopy(self.documents.get((ns, key))),
            put_document=lambda ns, key, value: self.documents.__setitem__((ns, key), copy.deepcopy(value)))
        self.service = SimpleNamespace(_state_store=self.store, _signature_people_cache=None,
                                       _load_signature_people=Mock(side_effect=self.load))
        self.delivery = MessageDelivery(self.service, transport=SimpleNamespace(), clock=lambda: 1000)

    def load(self, *, force=False):
        rows = [{"record_id": "stable-person", "open_id": "open-" + self.credentials[0]}]
        self.service._signature_people_cache = {"loaded_ts": 1000, "people": rows}
        return rows

    def test_same_app_directory_is_cached_and_survives_restart(self):
        first = self.delivery.directory()
        self.assertEqual(first["app_id"], self.credentials[0])
        self.assertEqual(self.delivery.directory(), first)
        self.assertEqual(MessageDelivery(self.service, transport=SimpleNamespace(), clock=lambda: 1000).directory(), first)
        self.service._load_signature_people.assert_called_once_with(force=False)

    def test_app_change_forces_reload_of_both_persistent_and_service_cache(self):
        self.delivery.directory()
        self.credentials[0] = "cli_fixture_b"
        result = self.delivery.directory()
        self.assertEqual(result["app_id"], "cli_fixture_b")
        self.assertEqual(result["people"][0]["open_id"], "open-cli_fixture_b")
        self.assertEqual(result["people"][0]["record_id"], "stable-person")
        self.service._load_signature_people.assert_called_with(force=True)
        self.assertEqual(self.service._signature_people_cache["app_id"], "cli_fixture_b")
        self.assertEqual(self.documents[(DIRECTORY_NS, "staff")]["app_id"], "cli_fixture_b")

    def test_legacy_persistent_or_shared_cache_without_app_is_not_adopted(self):
        for source in ("persistent", "shared"):
            with self.subTest(source=source):
                self.documents.clear()
                self.delivery._directory = None
                self.service._signature_people_cache = None
                legacy = {"people": [{"record_id": "stable-person", "open_id": "old-fixture"}], "loaded_at": 1000, "loaded_ts": 2000}
                if source == "persistent":
                    self.documents[(DIRECTORY_NS, "staff")] = legacy
                else:
                    self.service._signature_people_cache = legacy
                result = self.delivery.directory()
                self.assertEqual(result["people"][0]["open_id"], "open-cli_fixture_a")
                self.service._load_signature_people.assert_called_with(force=True)

    def test_changed_credentials_during_directory_load_are_not_persisted(self):
        for index in (0, 1):
            with self.subTest(credential=index):
                def load(*, force=False):
                    rows = self.load(force=force)
                    self.credentials[index] += "-changed"
                    return rows
                self.service._load_signature_people.side_effect = load
                with self.assertRaises(DeliveryError):
                    self.delivery.directory()
                self.assertFalse(self.documents)
                self.assertIsNone(self.delivery._directory)

    def test_self_selection_accepts_canonical_or_current_alias_via_trusted_record_id(self):
        rows = [{"record_id": "recFixturePerson", "open_id": "ou_fixtureCurrent", "name": "Synthetic",
                 "employee_no": "1001", "can_receive_message": True, "account_nature": "VNET"}]
        self.service._load_signature_people.side_effect = lambda **kwargs: copy.deepcopy(rows)
        self.documents[("synthetic-logins", "directory")] = {
            "people": {"recFixturePerson": {"id": "recFixturePerson", "selectable": True, "inactive": False,
                                              "disabled_reason": "", "login_ids": ["ou_fixtureCanonical"]}}}
        for owner in ("ou_fixtureCanonical", "ou_fixtureCurrent"):
            with self.subTest(owner=owner):
                self.assertEqual(self.delivery.recipients(owner)["self"]["record_id"], "recFixturePerson")
                selected = self.delivery._selected(owner, ["__self__"])
                self.assertEqual(selected[0]["open_id"], "ou_fixtureCurrent")
                self.assertEqual(selected[0]["record_id"], "recFixturePerson")

    def test_self_selection_rejects_inactive_duplicate_or_external_associations(self):
        row = {"record_id": "recFixturePerson", "open_id": "ou_fixtureCurrent", "name": "Synthetic",
               "employee_no": "1001", "can_receive_message": True, "account_nature": "VNET"}
        for scenario in ("inactive", "unselectable", "disabled", "duplicate-login", "duplicate-alias", "external"):
            for owner in ("ou_fixtureCanonical", "ou_fixtureCurrent"):
                with self.subTest(scenario=scenario, owner=owner):
                    self.delivery._directory = None
                    self.service._signature_people_cache = None
                    self.documents.pop((DIRECTORY_NS, "staff"), None)
                    rows = [copy.deepcopy(row)]
                    people = {"recFixturePerson": {"id": "recFixturePerson", "selectable": True, "inactive": False,
                                                   "disabled_reason": "", "login_ids": ["ou_fixtureCanonical"]}}
                    if scenario == "inactive":
                        people["recFixturePerson"]["inactive"] = True
                    elif scenario == "unselectable":
                        people["recFixturePerson"]["selectable"] = False
                    elif scenario == "disabled":
                        people["recFixturePerson"]["disabled_reason"] = "Synthetic disabled association"
                    elif scenario == "duplicate-login":
                        people["recFixtureOther"] = {"id": "recFixtureOther", "selectable": True, "inactive": False,
                                                     "disabled_reason": "", "login_ids": ["ou_fixtureCanonical"]}
                    elif scenario == "duplicate-alias":
                        rows.append({**row, "record_id": "recFixtureOther"})
                    else:
                        rows[0]["account_nature"] = "External"
                    self.documents[("synthetic-logins", "directory")] = {"people": people}
                    self.service._load_signature_people.side_effect = lambda **kwargs: copy.deepcopy(rows)
                    self.assertIsNone(self.delivery.recipients(owner)["self"])
                    with self.assertRaises(DeliveryError):
                        self.delivery._selected(owner, ["__self__"])


if __name__ == "__main__":
    unittest.main()
