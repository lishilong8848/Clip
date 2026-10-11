"""Focused tests for Feishu assistant admin settings routes and config helpers.

No real credentials, network calls, worker launches, messages or runtime data are
used. Credential cipher functions are replaced with a reversible in-memory ASCII
cipher so tests only exercise the JSON/config contract.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

import httpx
from fastapi import FastAPI

from openclaw_service.protocol import atomic_json
from openclaw_service.assistant.lighthouse_ai import AssistantError
from lan_bitable_template_portal.feishu_assistant_settings import (
    install_feishu_assistant_settings,
    public_config,
    save_config,
)
from lan_bitable_template_portal.feishu_assistant import FeishuAssistant, FeishuMessenger

BASE = "http://127.0.0.1:18766"
ROUTE = "/api/assistant/feishu-settings"


def _protect(value):
    # Reversible fixture cipher: enc:<base64>. Never a real Windows key blob.
    return "enc:" + base64.b64encode(value.encode()).decode()


def _unprotect(value):
    return base64.b64decode(value.split("enc:", 1)[1]).decode()


def make_active(app_id="cli_running", secret="running-secret", *, isolated=False):
    return {
        "app_id": app_id,
        "enabled": True,
        "secret_cipher": _protect(secret),
        "bridge_cipher": _protect("running-bridge:" + app_id),
        **({"isolated_inbox": True} if isolated else {}),
    }


class SettingsRouteTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.config_path = self.root / "feishu_assistant.json"
        self.active = make_active()
        atomic_json(self.config_path, self.active)
        self.controller = SimpleNamespace(
            bound_port=18766,
            preferred_port=18766,
            _current_session=Mock(return_value=None),
            _request_base_url=Mock(return_value=BASE),
        )
        self.runtime = SimpleNamespace(
            auth_manager=SimpleNamespace(is_admin=Mock(return_value=False)),
            start=Mock(),
            restart=Mock(),
        )
        self.channel = SimpleNamespace(config_path=self.config_path, config=dict(self.active))
        # Credential helpers must be reversible fixture ciphers for the whole test.
        self._protect_patch = patch(
            "lan_bitable_template_portal.feishu_assistant_settings.protect_key", new=_protect)
        self._unprotect_patch = patch(
            "lan_bitable_template_portal.feishu_assistant_settings.unprotect_key", new=_unprotect)
        self._protect_patch.start()
        self._unprotect_patch.start()
        self.addCleanup(self._unprotect_patch.stop)
        self.addCleanup(self._protect_patch.stop)

        self.app = FastAPI()
        install_feishu_assistant_settings(self.app, self.controller, self.runtime, self.channel)
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app),
                                        base_url="http://testserver")

    async def asyncTearDown(self):
        await self.client.aclose()
        self.temp.cleanup()

    def set_admin(self, session):
        self.controller._current_session.return_value = session
        self.runtime.auth_manager.is_admin.return_value = True

    def seed_config(self, value):
        atomic_json(self.config_path, value)

    def file_content(self):
        return json.loads(self.config_path.read_text(encoding="utf-8"))

    def snapshot(self):
        return self.config_path.read_bytes()

    def revision(self):
        return hashlib.sha256(self.config_path.read_bytes()).hexdigest()

    async def test_anonymous_get_returns_401(self):
        self.controller._current_session.return_value = None
        response = await self.client.get(ROUTE)
        self.assertEqual(response.status_code, 401)
        self.assertFalse(response.json()["ok"])

    async def test_ordinary_and_guest_get_returns_403(self):
        # Ordinary non-admin user is blocked.
        self.controller._current_session.return_value = {"user": "bob"}
        self.runtime.auth_manager.is_admin.return_value = False
        response = await self.client.get(ROUTE)
        self.assertEqual(response.status_code, 403)

        # Guest is blocked even when auth_manager would consider it an admin.
        self.controller._current_session.return_value = {"user": "g", "is_guest": True}
        self.runtime.auth_manager.is_admin.return_value = True
        response = await self.client.get(ROUTE)
        self.assertEqual(response.status_code, 403)

    async def test_non_admin_post_cannot_change_credentials(self):
        before = self.snapshot()
        for session, status in ((None, 401), ({"role": "user"}, 403), ({"is_guest": True}, 403)):
            self.controller._current_session.return_value = session
            response = await self.client.post(ROUTE, headers={"origin": BASE}, json={
                "app_id": "cli_intruder", "app_secret": "fixture-secret", "enabled": True,
                "revision": self.revision()})
            self.assertEqual(response.status_code, status)
            self.assertEqual(self.snapshot(), before)

    async def test_admin_get_never_exposes_ciphers_or_plain_secret(self):
        self.set_admin({"user": "admin", "role": "admin"})
        response = await self.client.get(ROUTE)
        self.assertEqual(response.status_code, 200, response.text)
        data = response.json()["data"]
        self.assertEqual(data["app_id"], "cli_running")
        self.assertTrue(data["enabled"])
        self.assertTrue(data["has_secret"])
        self.assertFalse(data["restart_required"])
        self.assertNotIn("secret_cipher", data)
        self.assertNotIn("bridge_cipher", data)
        self.assertNotIn("running-secret", response.text)

    async def test_post_rejects_missing_and_cross_site_origin(self):
        self.set_admin({"user": "admin"})
        payload = {"app_id": "cli_x", "enabled": True, "app_secret": "s"}

        response = await self.client.post(ROUTE, json=payload)
        self.assertEqual(response.status_code, 403, response.text)

        response = await self.client.post(
            ROUTE, headers={"origin": "https://evil.example", "sec-fetch-site": "cross-site"},
            json=payload)
        self.assertEqual(response.status_code, 403, response.text)

        response = await self.client.post(
            ROUTE, headers={"origin": "http://evil:1234"}, json=payload)
        self.assertEqual(response.status_code, 403, response.text)

    async def test_invalid_json_rejects_without_writes(self):
        self.set_admin({"user": "admin"})
        before = self.snapshot()
        response = await self.client.post(ROUTE, headers={"origin": BASE}, content=b"{not json")
        self.assertEqual(response.status_code, 400, response.text)
        self.assertEqual(self.snapshot(), before)

    async def test_oversize_body_rejects_without_writes(self):
        self.set_admin({"user": "admin"})
        before = self.snapshot()
        payload = json.dumps({"app_id": "cli_x", "enabled": True, "app_secret": "a" * 5000})
        self.assertGreater(len(payload), 4096)
        response = await self.client.post(ROUTE, headers={"origin": BASE}, content=payload)
        self.assertEqual(response.status_code, 413, response.text)
        self.assertEqual(self.snapshot(), before)

    async def test_wrong_field_types_and_extra_key_reject_without_writes(self):
        self.set_admin({"user": "admin"})
        before = self.snapshot()
        for payload in (
            {"app_id": 123, "enabled": True, "app_secret": "s", "revision": self.revision()},
            {"app_id": "cli_x", "enabled": "yes", "app_secret": "s", "revision": self.revision()},
            {"app_id": "cli_x", "enabled": True, "app_secret": 42, "revision": self.revision()},
            {"app_id": "not-valid", "enabled": True, "app_secret": "s", "revision": self.revision()},
            {"app_id": "cli_x", "enabled": True, "app_secret": "s", "revision": self.revision(), "extra": 1},
        ):
            with self.subTest(payload=payload):
                response = await self.client.post(
                    ROUTE, headers={"origin": BASE}, json=payload)
                self.assertEqual(response.status_code, 400, response.text)
                self.assertEqual(self.snapshot(), before)

    async def test_save_encrypts_secret_generates_bridge_and_keeps_running_config(self):
        self.set_admin({"user": "admin"})
        # Fresh config with secret but no bridge yet: bridge must be generated.
        self.seed_config({"app_id": "cli_new", "secret_cipher": _protect("new-secret")})
        before = dict(self.channel.config)
        response = await self.client.post(ROUTE, headers={"origin": BASE}, json={
            "app_id": "cli_new", "app_secret": "new-secret", "enabled": True,
            "revision": self.revision()})
        self.assertEqual(response.status_code, 200, response.text)
        data = response.json()["data"]
        self.assertEqual(data["app_id"], "cli_new")
        self.assertTrue(data["enabled"])
        self.assertTrue(data["has_secret"])
        self.assertNotIn("new-secret", response.text)

        saved = self.file_content()
        self.assertNotEqual(saved["secret_cipher"], "new-secret")
        self.assertEqual(_unprotect(saved["secret_cipher"]), "new-secret")
        self.assertIn("bridge_cipher", saved)
        self.assertTrue(saved["bridge_cipher"])

        # The running channel config and runtime must stay untouched: settings take
        # effect only on the next program restart.
        self.assertEqual(self.channel.config, before)
        self.runtime.start.assert_not_called()
        self.runtime.restart.assert_not_called()

        # Because the file now differs from the running active config, the public
        # response reports that a restart is required.
        self.assertTrue(data["restart_required"])

    async def test_blank_secret_keeps_old_secret_for_same_app(self):
        self.set_admin({"user": "admin"})
        self.seed_config(make_active())
        original = self.file_content()["secret_cipher"]
        response = await self.client.post(ROUTE, headers={"origin": BASE}, json={
            "app_id": "cli_running", "enabled": True, "app_secret": "",
            "revision": self.revision()})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(self.file_content()["secret_cipher"], original)
        self.assertEqual(self.file_content()["app_id"], "cli_running")

    async def test_new_app_id_requires_secret(self):
        self.set_admin({"user": "admin"})
        self.seed_config(make_active())
        before = self.snapshot()
        response = await self.client.post(ROUTE, headers={"origin": BASE}, json={
            "app_id": "cli_other", "enabled": True, "app_secret": "",
            "revision": self.revision()})
        self.assertEqual(response.status_code, 400, response.text)
        self.assertIn("必须填写", response.json()["error"])
        self.assertEqual(self.snapshot(), before)

    async def test_revision_conflict_returns_409_without_writes(self):
        self.set_admin({"user": "admin"})
        before = self.snapshot()
        response = await self.client.post(ROUTE, headers={"origin": BASE}, json={
            "app_id": "cli_running", "enabled": True, "app_secret": "",
            "revision": "stale-revision"})
        self.assertEqual(response.status_code, 409, response.text)
        self.assertEqual(self.snapshot(), before)

    async def test_isolated_inbox_only_flags_on_application_change(self):
        self.set_admin({"user": "admin"})
        # Legacy config carries no isolated_inbox.
        self.seed_config({"app_id": "cli_a", "enabled": True, "secret_cipher": _protect("a"),
                          "bridge_cipher": _protect("b")})
        self.assertNotIn("isolated_inbox", self.file_content())

        # Same-app edit (no secret change) must preserve legacy absence.
        response = await self.client.post(ROUTE, headers={"origin": BASE}, json={
            "app_id": "cli_a", "enabled": False, "app_secret": "", "revision": self.revision()})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertNotIn("isolated_inbox", self.file_content())

        # Changing the app id forces isolated_inbox so old-app retries never
        # receive new-app credentials.
        response = await self.client.post(ROUTE, headers={"origin": BASE}, json={
            "app_id": "cli_b", "enabled": True, "app_secret": "b-secret",
            "revision": self.revision()})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertTrue(self.file_content()["isolated_inbox"])

    async def test_public_config_restart_required_tracks_stale_active(self):
        self.set_admin({"user": "admin"})
        initial = await self.client.get(ROUTE)
        self.assertEqual(initial.status_code, 200, initial.text)
        self.assertFalse(initial.json()["data"]["restart_required"])

        # Save a new secret for the same app -> file differs from running active.
        response = await self.client.post(ROUTE, headers={"origin": BASE}, json={
            "app_id": "cli_running", "enabled": True, "app_secret": "rotated-secret",
            "revision": self.revision()})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertTrue(response.json()["data"]["restart_required"])

        # Running active config is untouched, so GET keeps reporting restart.
        self.assertEqual(self.channel.config["app_id"], "cli_running")
        after = await self.client.get(ROUTE)
        self.assertTrue(after.json()["data"]["restart_required"])


class ConfigHelperTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "feishu_assistant.json"
        self.active = make_active()
        atomic_json(self.path, {})
        self.protect = patch("lan_bitable_template_portal.feishu_assistant_settings.protect_key",
                             new=_protect)
        self.unprotect = patch("lan_bitable_template_portal.feishu_assistant_settings.unprotect_key",
                               new=_unprotect)
        self.protect.start()
        self.unprotect.start()
        self.addCleanup(self.unprotect.stop)
        self.addCleanup(self.protect.stop)

    def revision(self):
        return hashlib.sha256(self.path.read_bytes()).hexdigest()

    def test_public_config_hides_all_cipher_fields(self):
        atomic_json(self.path, make_active(secret="hidden-secret"))
        data = public_config(self.path, make_active())
        self.assertEqual(data["app_id"], "cli_running")
        self.assertNotIn("secret_cipher", data)
        self.assertNotIn("bridge_cipher", data)
        self.assertNotIn("hidden-secret", json.dumps(data))

    def test_missing_file_is_unconfigured_and_can_be_created(self):
        self.path.unlink()
        original = public_config(self.path, {})
        self.assertFalse(original['has_secret'])
        self.assertEqual(original['app_id'], '')
        saved = save_config(self.path, {"app_id": "cli_first", "enabled": True,
            "app_secret": "fixture-secret", "revision": original['revision']}, {})
        self.assertTrue(saved['has_secret'])
        self.assertTrue(saved['restart_required'])

    def test_unreadable_bridge_is_rebuilt_without_changing_app_secret(self):
        original = make_active()
        original['bridge_cipher'] = 'fixture-from-other-computer'
        atomic_json(self.path, original)
        save_config(self.path, {"app_id": original['app_id'], "enabled": True,
            "app_secret": "", "revision": self.revision()}, self.active)
        saved = json.loads(self.path.read_text(encoding='utf-8'))
        self.assertTrue(_unprotect(saved['bridge_cipher']))
        self.assertEqual(saved['secret_cipher'], original['secret_cipher'])

    def test_credentials_are_not_exposed_as_agent_tools(self):
        from openclaw_service.assistant.lighthouse_api import _route_excluded
        for method in ('GET', 'POST'):
            self.assertTrue(_route_excluded(ROUTE, method))

    def test_save_generates_bridge_when_missing(self):
        atomic_json(self.path, {"app_id": "cli_a", "enabled": True,
                                "secret_cipher": _protect("s")})
        result = save_config(self.path, {"app_id": "cli_a", "enabled": True,
                                         "app_secret": "", "revision": self.revision()}, self.active)
        saved = json.loads(self.path.read_text(encoding="utf-8"))
        self.assertIn("bridge_cipher", saved)
        self.assertEqual(result["app_id"], "cli_a")
        self.assertEqual(_unprotect(saved["secret_cipher"]), "s")

    def test_blank_secret_same_app_preserves_legacy_no_isolated_flag(self):
        atomic_json(self.path, {"app_id": "cli_a", "enabled": True,
                                "secret_cipher": _protect("s"), "bridge_cipher": _protect("b")})
        save_config(self.path, {"app_id": "cli_a", "enabled": False, "app_secret": "",
                                "revision": self.revision()}, self.active)
        saved = json.loads(self.path.read_text(encoding="utf-8"))
        self.assertNotIn("isolated_inbox", saved)
        self.assertEqual(_unprotect(saved["secret_cipher"]), "s")

    def test_save_rejects_blank_secret_when_changing_app(self):
        atomic_json(self.path, make_active("cli_old", "old"))
        with self.assertRaises(AssistantError) as ctx:
            save_config(self.path, {"app_id": "cli_new", "enabled": True, "app_secret": "",
                                    "revision": self.revision()}, self.active)
        self.assertIn("必须填写", str(ctx.exception))


class WorkerSnapshotTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        runtime = SimpleNamespace(state_store=SimpleNamespace(
            db_path=str(self.root / "portal.sqlite3")))
        self.controller = SimpleNamespace(bound_port=18766, preferred_port=18766)
        self.assistant = FeishuAssistant(self.controller, runtime, AsyncMock())
        self.assistant.root = self.root

    async def test_start_worker_snapshots_running_config_not_newly_saved_config(self):
        running = make_active("cli_running", "running-secret", isolated=True)
        self.assistant.config = dict(running)
        # A newer saved configuration is written to the runtime settings file but
        # must NOT affect the snapshot produced for the running worker.
        newer = make_active("cli_new", "new-secret", isolated=True)
        atomic_json(self.assistant.config_path, newer)

        fake_process = SimpleNamespace(pid=4242, poll=lambda: None,
                                       terminate=Mock(), wait=Mock())
        with patch("lan_bitable_template_portal.feishu_assistant.subprocess.Popen",
                   return_value=fake_process) as popen, \
             patch("upload_event_module.services.process_lifetime.register_child_process",
                   return_value=True) as register:
            await self.assistant._start_worker()

        snapshot_path = self.root / "feishu_assistant_active.json"
        snap = json.loads(snapshot_path.read_text(encoding="utf-8"))
        self.assertEqual(snap["app_id"], "cli_running")
        self.assertNotEqual(snap["app_id"], "cli_new")
        self.assertEqual(snap, running)

        # Worker is launched against the snapshot file, not the settings file.
        command = popen.call_args.args[0]
        self.assertIn("--config", command)
        self.assertEqual(command[command.index("--config") + 1], str(snapshot_path))
        self.assertNotIn(str(self.assistant.config_path), command)
        register.assert_called_once_with(4242)

    async def test_register_child_process_failure_terminates_and_raises(self):
        self.assistant.config = make_active("cli_running", "running-secret", isolated=True)
        fake_process = SimpleNamespace(pid=9, poll=lambda: None,
                                       terminate=Mock(), wait=Mock())
        with patch("lan_bitable_template_portal.feishu_assistant.subprocess.Popen",
                   return_value=fake_process) as popen, \
             patch("upload_event_module.services.process_lifetime.register_child_process",
                   return_value=False) as register:
            with self.assertRaisesRegex(RuntimeError, "bind Feishu worker lifetime"):
                await self.assistant._start_worker()
        fake_process.terminate.assert_called_once()
        fake_process.wait.assert_called_once_with(5)
        self.assertIsNotNone(popen)


class FeishuAssistantStartTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        runtime = SimpleNamespace(state_store=SimpleNamespace(
            db_path=str(self.root / "portal.sqlite3")))
        self.controller = SimpleNamespace(bound_port=18766, preferred_port=18766)
        self.assistant = FeishuAssistant(self.controller, runtime, AsyncMock())
        self.assistant.root = self.root

    def write_config(self, value):
        atomic_json(self.assistant.config_path, value)

    async def _run_start(self, config):
        self.write_config(config)
        guard = self.enterContext(patch(
            "lan_bitable_template_portal.portal_service.external_real_write_guard",
            return_value={"real_write_allowed": True}))
        start_worker = self.enterContext(patch.object(self.assistant, "_start_worker",
                                                      new=AsyncMock()))
        run = self.enterContext(patch.object(self.assistant, "_run", new=AsyncMock()))
        unprotect = self.enterContext(patch(
            "lan_bitable_template_portal.feishu_assistant.unprotect_key", side_effect=_unprotect))
        messenger = self.enterContext(patch(
            "lan_bitable_template_portal.feishu_assistant.FeishuMessenger"))
        await self.assistant.start()
        return guard, start_worker, run, unprotect, messenger

    async def test_legacy_start_uses_shared_inbox_and_running_config(self):
        config = make_active("cli_running", "running-secret")  # no isolated_inbox
        _, start_worker, run, _, messenger = await self._run_start(config)
        self.assertEqual(str(self.assistant.inbox.path),
                         str(self.root / "feishu_assistant.sqlite3"))
        self.assertFalse(self.assistant.inbox.path.name.endswith("sharded"))
        start_worker.assert_awaited_once()
        run.assert_called_once()
        messenger.assert_called_once_with(config)

    async def test_isolated_start_uses_app_hash_inbox(self):
        app_id = "cli_iso"
        config = make_active(app_id, "iso-secret", isolated=True)
        _, start_worker, run, _, messenger = await self._run_start(config)
        suffix = "_" + hashlib.sha256(app_id.encode()).hexdigest()[:24]
        self.assertEqual(self.assistant.inbox.path.name,
                         "feishu_assistant" + suffix + ".sqlite3")
        self.assertEqual(str(self.assistant.inbox.path.parent), str(self.root))
        start_worker.assert_awaited_once()
        run.assert_called_once()
        messenger.assert_called_once_with(config)


if __name__ == "__main__":
    unittest.main()
