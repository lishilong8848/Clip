# -*- coding: utf-8 -*-
from __future__ import annotations

import copy
import hashlib
import json
import sys
import tempfile
import unittest
import urllib.parse
from pathlib import Path
from unittest.mock import MagicMock


BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal.polling_work_order_relay import (  # noqa: E402
    POLLING_RELAY_COMMAND_NAMESPACE,
    POLLING_RELAY_GROUP_NAMESPACE,
    PollingRelayConfig,
    PollingRelayConfigurationError,
    PollingRelayProtocolError,
    PollingWorkOrderRelayConnector,
    RelayResponse,
    probe_polling_relay_health,
    public_link_with_configured_port,
)
from lan_bitable_template_portal.state_store import LanPortalStateStore  # noqa: E402


class FakeClock:
    def __init__(self, value: float = 1_800_000_000.0) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value


class FakeRelayTransport:
    def __init__(self, config: PollingRelayConfig, clock: FakeClock) -> None:
        self.config = config
        self.clock = clock
        self.calls: list[dict] = []
        self.commands: list[dict] = []
        self.uploads: dict[str, tuple[bytes, str]] = {}
        self.acks: list[dict] = []
        self.cancel_failures = 0
        self.ack_failures = 0
        self.projection_missing_failures = 0

    @staticmethod
    def assert_equal(actual, expected) -> None:
        if actual != expected:
            raise AssertionError(f"{actual!r} != {expected!r}")

    def _response(
        self,
        status: int,
        body: bytes,
        *,
        content_type: str = "application/json",
    ) -> RelayResponse:
        return RelayResponse(
            status,
            {"content-type": content_type},
            body,
        )

    def request(
        self,
        method: str,
        url: str,
        *,
        headers,
        body: bytes,
        timeout: float,
        max_bytes: int,
    ) -> RelayResponse:
        parsed = urllib.parse.urlsplit(url)
        headers = dict(headers)
        self.assert_equal(headers.get("X-Relay-Authority-Id"), None)
        self.assert_equal(headers.get("X-Relay-Signature"), None)
        if parsed.path != "/api/v1/internal/authority/lease":
            self.assert_equal(
                headers.get("X-Relay-Fencing-Token"),
                "fencing_1234567890abcdefghijklmnop",
            )
        payload = json.loads(body.decode()) if body else {}
        self.calls.append(
            {
                "method": method.upper(),
                "path": parsed.path,
                "query": urllib.parse.parse_qs(parsed.query),
                "payload": payload,
                "headers": headers,
                "timeout": timeout,
                "max_bytes": max_bytes,
            }
        )
        status = 200
        response: dict | bytes
        content_type = "application/json"
        if parsed.path == "/api/v1/internal/authority/lease":
            response = {
                "instance_id": payload["instance_id"],
                "authority_epoch": 1,
                "fencing_token": "fencing_1234567890abcdefghijklmnop",
                "expires_at": self.clock() + 90,
            }
        elif method.upper() == "PUT" and parsed.path.endswith("/projection"):
            if self.projection_missing_failures:
                self.projection_missing_failures -= 1
                status, response = 404, {"error": "group missing"}
            else:
                response = {
                    "state": payload["state"],
                    "authority_version": payload["authority_version"],
                    "projection_revision": payload["projection_revision"],
                }
        elif method.upper() == "PUT" and "/api/v1/internal/groups/" in parsed.path:
            public_id = parsed.path.rsplit("/", 1)[-1]
            response = {
                "public_group_id": public_id,
                "protocol_version": 1,
                "state": payload["state"],
                "projection_revision": payload["projection_revision"],
                "entry_url": "https://public.example/polling-work-order",
            }
        elif parsed.path == "/api/v1/internal/commands/lease":
            response = {"commands": copy.deepcopy(self.commands)}
        elif parsed.path.startswith("/api/v1/internal/uploads/"):
            upload_id = parsed.path.split("/")[-2]
            response, content_type = self.uploads[upload_id]
        elif parsed.path.endswith("/ack"):
            if self.ack_failures:
                self.ack_failures -= 1
                status, response = 503, {"error": "ack unavailable"}
            else:
                self.acks.append(copy.deepcopy(payload))
                response = {"status": "succeeded"}
        elif parsed.path.endswith("/cancel"):
            if self.cancel_failures:
                self.cancel_failures -= 1
                status, response = 503, {"error": "cancel unavailable"}
            else:
                response = {"state": "cancelled"}
        else:
            status, response = 404, {"error": f"unknown path {parsed.path}"}
        if isinstance(response, bytes):
            response_body = response
        else:
            envelope = (
                {"ok": True, "data": response}
                if 200 <= status < 300
                else {"ok": False, "error": response.get("error", "error"), "error_code": "test_error"}
            )
            response_body = json.dumps(envelope, ensure_ascii=False).encode()
        return self._response(
            status,
            response_body,
            content_type=content_type,
        )


class FakeWorkOrders:
    _lock = __import__("threading").RLock()

    def __init__(self, store: LanPortalStateStore) -> None:
        self.state_store = store
        self.calls: list[tuple] = []
        self.target_record_id = "rec_target_001"
        self.state_store.put_document(
            "polling_work_order",
            self.target_record_id,
            {
                "group_id": self.target_record_id,
                "target_record_id": self.target_record_id,
                "title": "二次泵轮巡",
                "sop_name": "二次泵SOP",
                "scope": "E",
                "state": "active",
                "version": 1,
                "runs": [
                    {"run_index": 1, "from_unit": "1#", "to_unit": "2#"}
                ],
                "operator": {"name": "操作员", "open_id": "ou_secret"},
                "reviewer": {"name": "审核员", "open_id": "ou_secret2"},
                "current_index": 0,
                "selected_run_index": 0,
                "selected_by_role": "",
                "steps": [
                    {
                        "step_key": "1:1",
                        "global_index": 0,
                        "run_index": 1,
                        "run_count": 1,
                        "run_label": "1#→2#",
                        "step_index": 1,
                        "step_count": 1,
                        "content": "执行步骤",
                        "operator_required": True,
                        "reviewer_required": True,
                        "photo_required": False,
                        "time_limit_seconds": 0,
                        "photos": [],
                    }
                ],
                "created_at": "2026-08-28 09:00:00",
                "updated_at": "2026-08-28 09:00:00",
            },
        )

    def get_group(self, target_record_id: str) -> dict:
        value = self.state_store.get_document("polling_work_order", target_record_id)
        if not isinstance(value, dict):
            raise RuntimeError("not found")
        return copy.deepcopy(value)

    def open_groups(self) -> list[dict]:
        group = self.get_group(self.target_record_id)
        return [group] if group.get("state") in {"active", "upload_pending", "completed"} else []

    @staticmethod
    def role_token(target_record_id: str, role: str) -> str:
        return f"local:{target_record_id}:{role}"

    def session(self, token: str) -> dict:
        role = token.rsplit(":", 1)[-1]
        group = self.get_group(self.target_record_id)
        step = group["steps"][0]
        return {
            "group_id": self.target_record_id,
            "title": group["title"],
            "sop_name": group["sop_name"],
            "role": role,
            "role_label": "操作人" if role == "operator" else "现场审核人",
            "assigned_person": copy.deepcopy(group[role]),
            "state": group["state"],
            "version": group["version"],
            "current_index": 0,
            "total_steps": 1,
            "current_run_index": int(group.get("selected_run_index") or 0),
            "can_release_selection": False,
            "can_rollback_previous": False,
            "work_orders": [
                {
                    "run_index": 1,
                    "from_unit": "1#",
                    "to_unit": "2#",
                    "label": "1#→2#",
                    "step_count": 1,
                    "completed_steps": 0,
                    "state": "available",
                    "selectable": True,
                }
            ],
            "steps": [
                {
                    **step,
                    "position": "current",
                    "operator_confirmed": False,
                    "reviewer_confirmed": False,
                    "timer_started": True,
                    "confirm_available_at": 0,
                    "remaining_seconds": 0,
                    "photos": copy.deepcopy(step.get("photos") or []),
                }
            ],
            "last_error": "",
        }

    def _bump(self, name: str, *values) -> None:
        group = self.get_group(self.target_record_id)
        group["version"] += 1
        self.state_store.put_document(
            "polling_work_order", self.target_record_id, group
        )
        self.calls.append((name, *values))

    def activate(self, token: str, *, run_index: int, expected_version: int):
        self._bump("activate", token, run_index, expected_version)

    def release_selection(self, token: str, *, run_index: int, expected_version: int):
        self._bump("release", token, run_index, expected_version)

    def confirm(self, token: str, **kwargs):
        self._bump("confirm", token, kwargs)

    def rollback_previous(self, token: str, **kwargs):
        self._bump("rollback", token, kwargs)

    def add_step_photo(self, token: str, **kwargs):
        group = self.get_group(self.target_record_id)
        group["steps"][0]["photos"].append(
            {
                "photo_id": "local-photo-1",
                "name": kwargs["file_name"],
                "mime_type": kwargs["mime_type"],
                "size": len(kwargs["content"]),
                "sha256": hashlib.sha256(kwargs["content"]).hexdigest(),
            }
        )
        group["version"] += 1
        self.state_store.put_document(
            "polling_work_order", self.target_record_id, group
        )
        self.calls.append(("photo", token, copy.deepcopy(kwargs)))


def _config() -> PollingRelayConfig:
    return PollingRelayConfig(
        enabled=True,
        base_url="https://relay.example",
        connector_id="connector_test_001",
    )


class PollingWorkOrderRelayTests(unittest.TestCase):
    def test_health_probe_requires_ready_matching_protocol(self) -> None:
        class HealthTransport:
            def __init__(self, payload: dict) -> None:
                self.payload = payload

            def request(self, *_args, **_kwargs) -> RelayResponse:
                return RelayResponse(
                    200,
                    {"content-type": "application/json"},
                    json.dumps(self.payload).encode(),
                )

        ready = probe_polling_relay_health(
            "https://relay.example",
            transport=HealthTransport(
                {
                    "service": "public_polling_work_order",
                    "protocol_version": 1,
                    "ready": True,
                }
            ),
        )
        self.assertTrue(ready["ready"])
        legacy_name = probe_polling_relay_health(
            "https://relay.example",
            transport=HealthTransport(
                {
                    "service": "public_polling_relay",
                    "protocol_version": 1,
                    "ready": True,
                }
            ),
        )
        self.assertFalse(legacy_name["ready"])
        incompatible = probe_polling_relay_health(
            "https://relay.example",
            transport=HealthTransport(
                {
                    "service": "public_polling_work_order",
                    "protocol_version": 2,
                    "ready": True,
                }
            ),
        )
        self.assertFalse(incompatible["ready"])
        self.assertIn("版本不兼容", incompatible["error"])

    def test_health_probe_rejects_missing_create_route(self) -> None:
        class MissingCreateTransport:
            def request(self, method, *_args, **_kwargs) -> RelayResponse:
                if method == "OPTIONS":
                    return RelayResponse(404, {}, b"{}")
                return RelayResponse(
                    200,
                    {"content-type": "application/json"},
                    json.dumps(
                        {
                            "service": "public_polling_work_order",
                            "protocol_version": 1,
                            "ready": True,
                        }
                    ).encode(),
                )

        result = probe_polling_relay_health(
            "https://relay.example", transport=MissingCreateTransport()
        )
        self.assertFalse(result["ready"])
        self.assertIn("内部中继接口", result["error"])

    def test_health_probe_rejects_internal_source_ip_filter(self) -> None:
        class SourceFilteredTransport:
            def request(self, method, url, **_kwargs) -> RelayResponse:
                if method == "GET":
                    return RelayResponse(
                        200,
                        {"content-type": "application/json"},
                        json.dumps({
                            "service": "public_polling_work_order",
                            "protocol_version": 1,
                            "ready": True,
                        }).encode(),
                    )
                if method == "OPTIONS":
                    return RelayResponse(405, {"allow": "POST"}, b'{}')
                return RelayResponse(
                    403,
                    {"content-type": "application/json"},
                    json.dumps({
                        "ok": False,
                        "error": "当前来源 IP 无权调用内部工单接口。",
                        "error_code": "internal_source_forbidden",
                    }, ensure_ascii=False).encode(),
                )

        result = probe_polling_relay_health(
            "https://relay.example",
            transport=SourceFilteredTransport(),
        )
        self.assertFalse(result["ready"])
        self.assertIn("来源 IP", result["error"])

    def test_health_probe_accepts_latest_internal_validation_response(self) -> None:
        class LatestTransport:
            def request(self, method, url, **_kwargs) -> RelayResponse:
                if method == "GET":
                    payload = {
                        "service": "public_polling_work_order",
                        "protocol_version": 1,
                        "ready": True,
                    }
                    return RelayResponse(200, {"content-type": "application/json"}, json.dumps(payload).encode())
                if method == "OPTIONS":
                    return RelayResponse(405, {"allow": "POST"}, b'{}')
                return RelayResponse(
                    422,
                    {"content-type": "application/json"},
                    b'{"ok":false,"error":"instance_id: Field required","error_code":"validation_error"}',
                )

        result = probe_polling_relay_health(
            "https://relay.example",
            transport=LatestTransport(),
        )
        self.assertTrue(result["ready"])

    def test_reconcile_does_not_migrate_legacy_local_group(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            store = LanPortalStateStore(Path(temporary) / "state.sqlite3")
            manager = FakeWorkOrders(store)
            clock = FakeClock()
            transport = FakeRelayTransport(_config(), clock)
            connector = PollingWorkOrderRelayConnector(
                store, manager, config=_config(), transport=transport, clock=clock
            )
            result = connector.reconcile_groups()
            self.assertEqual(result, {"registered": 0, "pending": 0, "cancelled": 0})
            self.assertFalse(
                any(
                    call["method"] == "PUT"
                    and call["path"].startswith("/api/v1/internal/groups/")
                    for call in transport.calls
                )
            )

    def test_config_requires_only_the_https_relay_url(self) -> None:
        with self.assertRaises(PollingRelayConfigurationError):
            PollingRelayConfig.from_env(
                {
                    "CLIPFLOW_POLLING_RELAY_ENABLED": "1",
                    "CLIPFLOW_POLLING_RELAY_URL": "http://relay.example",
                }
            )
        with self.assertRaises(PollingRelayConfigurationError):
            PollingRelayConfig.from_env(
                {
                    "CLIPFLOW_POLLING_RELAY_ENABLED": "1",
                    "CLIPFLOW_POLLING_RELAY_URL": "https://relay.example/prefix",
                }
            )
        https_config = PollingRelayConfig.from_env(
            {
                "CLIPFLOW_POLLING_RELAY_ENABLED": "1",
                "CLIPFLOW_POLLING_RELAY_URL": "https://relay.example",
            }
        )
        self.assertTrue(https_config.enabled)
        with self.assertRaises(PollingRelayConfigurationError):
            PollingRelayConfig.from_env(
                {
                    "CLIPFLOW_POLLING_RELAY_ENABLED": "1",
                    "CLIPFLOW_POLLING_RELAY_URL": "https://relay.example",
                    "CLIPFLOW_POLLING_RELAY_CONNECTOR_ID": "bad\nheader",
                }
            )
        config = PollingRelayConfig.from_env(
            {
                "CLIPFLOW_POLLING_RELAY_ENABLED": "1",
                "CLIPFLOW_POLLING_RELAY_URL": "http://127.0.0.1:9000",
                "CLIPFLOW_POLLING_RELAY_ALLOW_INSECURE_HTTP": "1",
                "CLIPFLOW_POLLING_RELAY_LONG_POLL_SECONDS": "40",
                "CLIPFLOW_POLLING_RELAY_TIMEOUT_SECONDS": "10",
            }
        )
        self.assertTrue(config.enabled)
        self.assertEqual(config.long_poll_seconds, 15)
        self.assertEqual(config.request_timeout_seconds, 20)

    def test_each_connector_process_uses_a_unique_lease_instance(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            store = LanPortalStateStore(Path(temporary) / "state.sqlite3")
            first = PollingWorkOrderRelayConnector(
                store, FakeWorkOrders(store), config=_config()
            )
            second = PollingWorkOrderRelayConnector(
                store, FakeWorkOrders(store), config=_config()
            )
            self.assertNotEqual(first.connector_id, second.connector_id)
            self.assertTrue(first.connector_id.startswith("connector_test_001_"))

    def test_authority_registration_projection_and_group_metadata_are_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            store = LanPortalStateStore(Path(temporary) / "state.sqlite3")
            manager = FakeWorkOrders(store)
            clock = FakeClock()
            transport = FakeRelayTransport(_config(), clock)
            connector = PollingWorkOrderRelayConnector(
                store, manager, config=_config(), transport=transport, clock=clock
            )
            first = connector.register_group(manager.target_record_id)
            second = connector.register_group(manager.target_record_id)
            self.assertEqual(first["registration_state"], "registered")
            self.assertEqual(first["operator_link"], second["operator_link"])
            self.assertNotIn(manager.target_record_id, first["public_group_id"])
            registrations = [
                call
                for call in transport.calls
                if call["method"] == "PUT"
                and call["path"].startswith("/api/v1/internal/groups/")
                and not call["path"].endswith("/projection")
            ]
            self.assertEqual(len(registrations), 1)
            payload_text = json.dumps(registrations[0]["payload"], ensure_ascii=False)
            self.assertIn('"protocol_version": 1', payload_text)
            self.assertIn('"operator"', payload_text)
            self.assertIn('"reviewer"', payload_text)
            self.assertIn('"photo_required": false', payload_text)
            self.assertNotIn("ou_secret", payload_text)
            group = manager.get_group(manager.target_record_id)
            self.assertEqual(group["relay"]["mode"], "public_relay")
            self.assertEqual(group["relay"]["operator_link"], first["operator_link"])
            self.assertIn("#link_id=", group["relay"]["operator_link"])
            self.assertIn("&secret=", group["relay"]["operator_link"])
            self.assertTrue(group["relay"]["link_ids"]["reviewer"].startswith("lnk_"))

            clock.value += 301
            connector.register_group(manager.target_record_id)
            manager._bump("external_update")
            connector.reconcile_groups()
            registrations = [
                call
                for call in transport.calls
                if call["method"] == "PUT"
                and call["path"].startswith("/api/v1/internal/groups/")
                and not call["path"].endswith("/projection")
            ]
            projections = [
                call for call in transport.calls if call["path"].endswith("/projection")
            ]
            self.assertEqual(len(registrations), 2)
            self.assertEqual(len(projections), 1)
            relay = store.get_document(
                POLLING_RELAY_GROUP_NAMESPACE, manager.target_record_id
            )
            self.assertEqual(relay["registered_version"], 2)

            connector.ensure_lease(force=True)
            connector.ensure_lease(force=True)
            lease_calls = [
                call
                for call in transport.calls
                if call["path"] == "/api/v1/internal/authority/lease"
            ]
            self.assertGreaterEqual(len(lease_calls), 3)
            self.assertNotEqual(
                lease_calls[-1]["headers"]["Idempotency-Key"],
                lease_calls[-2]["headers"]["Idempotency-Key"],
            )

    def test_public_links_keep_configured_port_for_new_and_existing_groups(self) -> None:
        from lan_bitable_template_portal.polling_work_orders import PollingWorkOrderService
        base = 'https://public.example:8787'
        tail = '/polling-work-order?view=work#link_id=fixture&secret=fixture'
        for original in ('https://public.example', 'https://public.example:443', base):
            self.assertEqual(public_link_with_configured_port(original+tail, base), base+tail)
        self.assertEqual(public_link_with_configured_port('https://other.example'+tail, base), 'https://other.example'+tail)
        self.assertEqual(public_link_with_configured_port('https://[::1]'+tail, 'https://[::1]:8787'), 'https://[::1]:8787'+tail)
        with tempfile.TemporaryDirectory() as temporary:
            store = LanPortalStateStore(Path(temporary) / 'state.sqlite3')
            manager = FakeWorkOrders(store); clock = FakeClock()
            config = PollingRelayConfig(enabled=True, base_url=base)
            connector = PollingWorkOrderRelayConnector(store, manager, config=config, transport=FakeRelayTransport(config, clock), clock=clock)
            registered = connector.register_group(manager.target_record_id)
            for key in ('operator_link','reviewer_link'):
                self.assertEqual(urllib.parse.urlsplit(registered[key]).netloc,'public.example:8787')
            group = manager.get_group(manager.target_record_id)
            for key in ('operator_link','reviewer_link'):
                group['relay'][key] = group['relay'][key].replace(':8787','')
            before = copy.deepcopy(group)
            formatted = PollingWorkOrderService.group_with_links(None, group, '')
            self.assertEqual(group, before)
            for key in ('operator_link','reviewer_link'):
                self.assertEqual(formatted[key], registered[key])
                self.assertEqual(formatted['relay'][key], registered[key])
            group['relay']['registration_state']='pending'
            self.assertEqual(PollingWorkOrderService.group_with_links(None,group,'')['operator_link'],'')

    def test_missing_remote_group_is_registered_again_before_projection_retry(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            store = LanPortalStateStore(Path(temporary) / "state.sqlite3")
            manager = FakeWorkOrders(store)
            clock = FakeClock()
            transport = FakeRelayTransport(_config(), clock)
            connector = PollingWorkOrderRelayConnector(
                store, manager, config=_config(), transport=transport, clock=clock
            )
            connector.register_group(manager.target_record_id)
            manager._bump("external_update")
            transport.projection_missing_failures = 1

            first = connector.reconcile_groups()
            self.assertEqual(first["registered"], 1)
            registrations = [
                call
                for call in transport.calls
                if call["method"] == "PUT"
                and call["path"].startswith("/api/v1/internal/groups/")
                and not call["path"].endswith("/projection")
            ]
            self.assertEqual(len(registrations), 2)
            document = store.get_document(
                POLLING_RELAY_GROUP_NAMESPACE, manager.target_record_id
            )
            self.assertEqual(document["registration_state"], "registered")

            connector.reconcile_groups()
            document = store.get_document(
                POLLING_RELAY_GROUP_NAMESPACE, manager.target_record_id
            )
            self.assertEqual(document["registered_version"], 2)

    def test_photo_command_downloads_validates_executes_once_and_retries_ack(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            store = LanPortalStateStore(Path(temporary) / "state.sqlite3")
            manager = FakeWorkOrders(store)
            clock = FakeClock()
            transport = FakeRelayTransport(_config(), clock)
            connector = PollingWorkOrderRelayConnector(
                store, manager, config=_config(), transport=transport, clock=clock
            )
            registration = connector.register_group(manager.target_record_id)
            photo = b"valid-photo-content"
            transport.uploads["upload_123456"] = (photo, "image/png")
            command = {
                "command_id": "command_photo_123456",
                "public_group_id": registration["public_group_id"],
                "kind": "attach_photo",
                "role": "operator",
                "expected_version": 1,
                "payload": {
                    "step_key": "1:1",
                },
                "upload": {
                    "upload_id": "upload_123456",
                    "filename": "photo.png",
                    "content_type": "image/png",
                    "size": len(photo),
                    "sha256": hashlib.sha256(photo).hexdigest(),
                },
            }
            transport.commands = [command]
            transport.ack_failures = 1
            result = connector.poll_commands_once()
            self.assertEqual(result["completed"], 1)
            self.assertEqual(len([item for item in manager.calls if item[0] == "photo"]), 1)
            ledger = store.get_document(
                POLLING_RELAY_COMMAND_NAMESPACE, command["command_id"]
            )
            self.assertEqual(ledger["status"], "completed")
            self.assertEqual(ledger["ack_status"], "pending")
            ledger["ack_next_retry_at"] = 0
            store.put_document(
                POLLING_RELAY_COMMAND_NAMESPACE, command["command_id"], ledger
            )
            self.assertEqual(connector.retry_pending_acks(), 1)
            connector.process_command(command)
            self.assertEqual(len([item for item in manager.calls if item[0] == "photo"]), 1)
            self.assertEqual(len(transport.acks), 2)
            self.assertEqual(transport.acks[-1]["outcome"], "succeeded")
            self.assertEqual(transport.acks[-1]["authority_version"], 2)
            self.assertIn("operator", transport.acks[-1]["projection"])
            tampered = copy.deepcopy(command)
            tampered["payload"]["step_key"] = "1:2"
            with self.assertRaises(PollingRelayProtocolError):
                connector.process_command(tampered)

    def test_stale_pending_projection_is_rebuilt_after_photo_is_saved(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            store = LanPortalStateStore(Path(temporary) / "state.sqlite3")
            manager = FakeWorkOrders(store)
            clock = FakeClock()
            connector = PollingWorkOrderRelayConnector(
                store,
                manager,
                config=_config(),
                transport=FakeRelayTransport(_config(), clock),
                clock=clock,
            )
            connector.register_group(manager.target_record_id)
            relay = store.get_document(
                POLLING_RELAY_GROUP_NAMESPACE, manager.target_record_id
            )
            relay["pending_projection"] = {
                "state": "active",
                "authority_version": 1,
                "projection_revision": 2,
                "projection": connector.dual_projection(manager.target_record_id),
            }
            store.put_document(
                POLLING_RELAY_GROUP_NAMESPACE, manager.target_record_id, relay
            )

            manager.add_step_photo(
                manager.role_token(manager.target_record_id, "operator"),
                step_key="1:1",
                expected_version=1,
                file_name="photo.png",
                mime_type="image/png",
                content=b"new-photo",
            )
            staged = connector._stage_projection(manager.target_record_id)

            self.assertEqual(staged["authority_version"], 2)
            self.assertEqual(staged["projection_revision"], 3)
            self.assertEqual(
                staged["projection"]["operator"]["steps"][0]["photos"][0][
                    "sha256"
                ],
                hashlib.sha256(b"new-photo").hexdigest(),
            )
            self.assertFalse(
                staged["projection"]["operator"]["steps"][0]["photo_required"]
            )

    def test_executing_photo_command_recovers_without_repeating_local_action(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            store = LanPortalStateStore(Path(temporary) / "state.sqlite3")
            manager = FakeWorkOrders(store)
            clock = FakeClock()
            transport = FakeRelayTransport(_config(), clock)
            connector = PollingWorkOrderRelayConnector(
                store, manager, config=_config(), transport=transport, clock=clock
            )
            registration = connector.register_group(manager.target_record_id)
            photo = b"already-stored-photo"
            command = {
                "command_id": "command_recover_123456",
                "public_group_id": registration["public_group_id"],
                "type": "photo",
                "role": "operator",
                "payload": {
                    "step_key": "1:1",
                    "expected_version": 1,
                    "upload": {
                        "upload_id": "upload_recover_123",
                        "file_name": "photo.png",
                        "mime_type": "image/png",
                        "size": len(photo),
                        "sha256": hashlib.sha256(photo).hexdigest(),
                    },
                },
            }
            manager.add_step_photo(
                manager.role_token(manager.target_record_id, "operator"),
                step_key="1:1",
                expected_version=1,
                file_name="photo.png",
                mime_type="image/png",
                content=photo,
            )
            prior_call_count = len(manager.calls)
            digest = connector._command_fingerprint(command)
            store.put_document(
                POLLING_RELAY_COMMAND_NAMESPACE,
                command["command_id"],
                {
                    "command_id": command["command_id"],
                    "payload_sha256": digest,
                    "target_record_id": manager.target_record_id,
                    "public_group_id": registration["public_group_id"],
                    "status": "executing",
                    "ack_status": "pending",
                },
            )
            result = connector.process_command(command)
            self.assertEqual(result["status"], "completed")
            self.assertTrue(result["result"]["recovered_after_restart"])
            self.assertEqual(len(manager.calls), prior_call_count)

    def test_cancel_registration_retries_and_clears_public_links(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            store = LanPortalStateStore(Path(temporary) / "state.sqlite3")
            manager = FakeWorkOrders(store)
            clock = FakeClock()
            transport = FakeRelayTransport(_config(), clock)
            connector = PollingWorkOrderRelayConnector(
                store, manager, config=_config(), transport=transport, clock=clock
            )
            connector.register_group(manager.target_record_id)
            group = manager.get_group(manager.target_record_id)
            group["state"] = "cancelled"
            group["cancel_reason"] = "target_deleted"
            store.put_document("polling_work_order", manager.target_record_id, group)
            transport.cancel_failures = 1
            first = connector.reconcile_groups()
            self.assertEqual(first["pending"], 1)
            document = store.get_document(
                POLLING_RELAY_GROUP_NAMESPACE, manager.target_record_id
            )
            self.assertEqual(document["registration_state"], "cancel_pending")
            document["next_retry_at"] = 0
            store.put_document(
                POLLING_RELAY_GROUP_NAMESPACE, manager.target_record_id, document
            )
            second = connector.reconcile_groups()
            self.assertEqual(second["cancelled"], 1)
            document = store.get_document(
                POLLING_RELAY_GROUP_NAMESPACE, manager.target_record_id
            )
            self.assertEqual(document["registration_state"], "cancelled")
            self.assertEqual(document["operator_link"], "")
            self.assertEqual(manager.get_group(manager.target_record_id)["relay"]["operator_link"], "")

if __name__ == "__main__":
    unittest.main()
