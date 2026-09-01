from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlencode
from unittest.mock import patch

from fastapi.testclient import TestClient

from .app import RelayError, RelaySettings, _verify_image_dimensions, create_app
from .frontend import _COMMON_SCRIPT


PNG_1X1 = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="
)


class RelayFlowTest(unittest.TestCase):
    def test_lan_http_photo_hash_has_working_non_webcrypto_fallback(self) -> None:
        match = re.search(
            r"(const SHA256_K=.*?\nasync function sha256Fallback\(.*?\n\})\nasync function sha256Hex",
            _COMMON_SCRIPT,
            re.DOTALL,
        )
        self.assertIsNotNone(
            match,
            "局域网 HTTP 页面必须包含不依赖 crypto.subtle 的 SHA-256。",
        )
        node = shutil.which("node")
        if not node:
            self.skipTest("Node.js unavailable; fallback presence was verified statically.")
        values = ["", "abc", "中文", "a" * 1000]
        expected = [hashlib.sha256(value.encode()).hexdigest() for value in values]
        script = (
            "const sleep=ms=>new Promise(resolve=>setTimeout(resolve,ms));\n"
            + str(match.group(1))
            + "\nconst values="
            + json.dumps(values, ensure_ascii=False)
            + ";Promise.all(values.map(value=>sha256Fallback(new TextEncoder().encode(value).buffer)))"
            ".then(result=>console.log(JSON.stringify(result)));"
        )
        completed = subprocess.run(
            [node, "-e", script],
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        self.assertEqual(json.loads(completed.stdout), expected)

    def test_production_settings_require_canonical_public_url(self) -> None:
        environment: dict[str, str] = {}
        with patch.dict(os.environ, environment, clear=True):
            with self.assertRaisesRegex(RuntimeError, "公网 HTTPS 根地址"):
                RelaySettings.from_env()
            os.environ["PUBLIC_POLLING_RELAY_PUBLIC_URL"] = (
                "https://workorder.example.com"
            )
            settings = RelaySettings.from_env()
            self.assertEqual(
                settings.public_base_url, "https://workorder.example.com"
            )

    def test_public_photo_dimension_and_step_count_limits(self) -> None:
        from PIL import Image

        oversized = Path(self.temporary.name) / "too-wide.png"
        Image.new("RGB", (12001, 1), "white").save(oversized, "PNG")
        with self.assertRaisesRegex(RelayError, "像素或尺寸超过限制"):
            _verify_image_dimensions(oversized)

        self._lease()
        self._register()
        current_step = {
            "step_key": "1:1",
            "run_index": 1,
            "step_index": 1,
            "step_count": 1,
            "content": "当前步骤",
            "position": "current",
            "operator_required": True,
            "reviewer_required": True,
            "photos": [],
        }
        projected = self._internal(
            "PUT",
            "/api/v1/internal/groups/public-group-0001/projection",
            payload={
                "state": "active",
                "authority_version": 1,
                "projection_revision": 2,
                "projection": {
                    "operator": {"state": "active", "steps": [current_step]},
                    "reviewer": {"state": "active", "steps": [current_step]},
                },
            },
        )
        self.assertEqual(projected.status_code, 200, projected.text)
        csrf = self._exchange_operator()["csrf_token"]
        first_upload_id = ""
        for index in range(5):
            request_payload = {
                "step_key": "1:1",
                "expected_version": 1,
                "file_name": f"step-{index}.png",
                "content_type": "image/png",
                "size": len(PNG_1X1),
                "sha256": hashlib.sha256(PNG_1X1 + bytes([index])).hexdigest(),
            }
            response = self.operator_client.post(
                "/api/v1/work-orders/uploads",
                json=request_payload,
                headers={"X-CSRF-Token": csrf},
            )
            self.assertEqual(response.status_code, 201, response.text)
            if index == 0:
                first_upload_id = response.json()["data"]["upload_id"]
                repeated = self.operator_client.post(
                    "/api/v1/work-orders/uploads",
                    json=request_payload,
                    headers={"X-CSRF-Token": csrf},
                )
                self.assertEqual(
                    repeated.json()["data"]["upload_id"], first_upload_id
                )
        blocked = self.operator_client.post(
            "/api/v1/work-orders/uploads",
            json={
                "step_key": "1:1",
                "expected_version": 1,
                "file_name": "step-6.png",
                "content_type": "image/png",
                "size": len(PNG_1X1),
                "sha256": hashlib.sha256(b"sixth").hexdigest(),
            },
            headers={"X-CSRF-Token": csrf},
        )
        self.assertEqual(blocked.status_code, 409, blocked.text)
        self.assertEqual(blocked.json()["error_code"], "step_photo_limit")

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        root = Path(self.temporary.name)
        settings = RelaySettings(
            db_path=root / "relay.sqlite3",
            upload_root=root / "uploads",
            secure_cookie=False,
        )
        self.app = create_app(settings)
        self.internal_client = TestClient(self.app)
        self.operator_client = TestClient(self.app)
        self.fencing_token = ""

    def tearDown(self) -> None:
        self.operator_client.close()
        self.internal_client.close()
        self.temporary.cleanup()

    def _internal_headers(
        self,
        *,
        fence: bool = True,
    ) -> dict[str, str]:
        headers = {
            "Content-Type": "application/json",
        }
        if fence:
            headers["X-Relay-Fencing-Token"] = self.fencing_token
        return headers

    def _internal(
        self,
        method: str,
        path: str,
        *,
        payload: dict | None = None,
        query: dict[str, object] | None = None,
        fence: bool = True,
    ):
        body = json.dumps(payload or {}, ensure_ascii=False, separators=(",", ":")).encode()
        query_text = urlencode(query or {})
        url = path + (f"?{query_text}" if query_text else "")
        return self.internal_client.request(
            method,
            url,
            content=body if method.upper() not in {"GET", "HEAD"} else b"",
            headers=self._internal_headers(fence=fence),
        )

    def _lease(self) -> dict:
        response = self._internal(
            "POST",
            "/api/v1/internal/authority/lease",
            payload={"instance_id": "test-instance-01"},
            fence=False,
        )
        self.assertEqual(response.status_code, 200, response.text)
        data = response.json()["data"]
        self.fencing_token = data["fencing_token"]
        return data

    def _register(self, operator_secret: str = "operator-role-secret-0123456789") -> dict:
        shared_projection = {
            "title": "E楼二次泵轮巡",
            "sop_name": "二次泵SOP",
            "state": "active",
            "target_record_id": "must-not-leak",
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
            "steps": [],
        }
        projection = {
            "operator": {**shared_projection, "can_rollback_previous": False},
            "reviewer": {**shared_projection, "can_rollback_previous": True},
        }
        payload = {
            "protocol_version": 1,
            "registration_version": 1,
            "state": "active",
            "authority_version": 1,
            "projection_revision": 1,
            "projection": projection,
            "links": {
                "operator": {
                    "link_id": "operator-link-0001",
                    "secret_sha256": hashlib.sha256(operator_secret.encode()).hexdigest(),
                    "generation": 1,
                    "assigned_name": "操作员",
                },
                "reviewer": {
                    "link_id": "reviewer-link-0001",
                    "secret_sha256": hashlib.sha256(b"reviewer-role-secret-0123456789").hexdigest(),
                    "generation": 1,
                    "assigned_name": "审核员",
                },
            },
        }
        response = self._internal(
            "PUT", "/api/v1/internal/groups/public-group-0001", payload=payload
        )
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()["data"]

    def _exchange_operator(self, secret: str = "operator-role-secret-0123456789") -> dict:
        response = self.operator_client.post(
            "/api/v1/link-sessions/exchange",
            json={"link_id": "operator-link-0001", "secret": secret},
        )
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()["data"]

    def test_full_command_photo_projection_and_cancel_flow(self) -> None:
        lease = self._lease()
        self.assertEqual(lease["authority_epoch"], 1)
        registered = self._register()
        self.assertTrue(registered["entry_url"].endswith("/polling-work-order"))

        exchanged = self._exchange_operator()
        csrf = exchanged["csrf_token"]
        snapshot = exchanged["snapshot"]
        self.assertEqual(snapshot["role"], "operator")
        self.assertTrue(snapshot["authority_online"])
        self.assertFalse(snapshot["can_rollback_previous"])
        self.assertNotIn("target_record_id", snapshot)

        command_payload = {"type": "activate", "expected_version": 1, "run_index": 1}
        headers = {"X-CSRF-Token": csrf, "Idempotency-Key": "activate-request-0001"}
        first = self.operator_client.post(
            "/api/v1/work-orders/commands", json=command_payload, headers=headers
        )
        duplicate = self.operator_client.post(
            "/api/v1/work-orders/commands", json=command_payload, headers=headers
        )
        self.assertEqual(first.status_code, 202, first.text)
        self.assertEqual(first.json()["data"]["command_id"], duplicate.json()["data"]["command_id"])
        conflict = self.operator_client.post(
            "/api/v1/work-orders/commands",
            json={"type": "release", "expected_version": 1, "run_index": 1},
            headers=headers,
        )
        self.assertEqual(conflict.status_code, 409)
        self.assertEqual(conflict.json()["error_code"], "idempotency_conflict")

        leased = self._internal(
            "GET",
            "/api/v1/internal/commands/lease",
            query={"limit": 20, "wait_seconds": 0},
        )
        self.assertEqual(leased.status_code, 200, leased.text)
        command = leased.json()["data"]["commands"][0]
        self.assertEqual(command["type"], "activate")
        self.assertEqual(command["expected_version"], 1)

        projection2 = {
            "title": "E楼二次泵轮巡",
            "sop_name": "二次泵SOP",
            "state": "active",
            "current_run_index": 1,
            "work_orders": [],
            "steps": [
                {
                    "step_key": "1:1",
                    "run_index": 1,
                    "step_index": 1,
                    "step_count": 1,
                    "content": "切换1#至2#",
                    "position": "current",
                    "operator_required": True,
                    "reviewer_required": True,
                    "photos": [],
                }
            ],
        }
        ack_body = {
            "outcome": "succeeded",
            "authority_version": 2,
            "projection_revision": 2,
            "result": {},
            "projection": projection2,
        }
        ack = self._internal(
            "POST", f"/api/v1/internal/commands/{command['command_id']}/ack", payload=ack_body
        )
        self.assertEqual(ack.status_code, 200, ack.text)
        repeated_ack = self._internal(
            "POST", f"/api/v1/internal/commands/{command['command_id']}/ack", payload=ack_body
        )
        self.assertEqual(repeated_ack.status_code, 200, repeated_ack.text)
        replayed_request = self.operator_client.post(
            "/api/v1/work-orders/commands", json=command_payload, headers=headers
        )
        self.assertEqual(replayed_request.status_code, 200, replayed_request.text)
        self.assertEqual(
            replayed_request.json()["data"]["command_id"], command["command_id"]
        )

        digest = hashlib.sha256(PNG_1X1).hexdigest()
        upload = self.operator_client.post(
            "/api/v1/work-orders/uploads",
            json={
                "step_key": "1:1",
                "expected_version": 2,
                "file_name": "step.png",
                "content_type": "image/png",
                "size": len(PNG_1X1),
                "sha256": digest,
            },
            headers={"X-CSRF-Token": csrf},
        )
        self.assertEqual(upload.status_code, 201, upload.text)
        upload_id = upload.json()["data"]["upload_id"]
        content = self.operator_client.put(
            f"/api/v1/work-orders/uploads/{upload_id}/content",
            content=PNG_1X1,
            headers={
                "Content-Type": "image/png",
                "X-Content-SHA256": digest,
                "X-CSRF-Token": csrf,
            },
        )
        self.assertEqual(content.status_code, 200, content.text)
        completed = self.operator_client.post(
            f"/api/v1/work-orders/uploads/{upload_id}/complete",
            headers={"X-CSRF-Token": csrf},
        )
        self.assertEqual(completed.status_code, 202, completed.text)
        attach_id = completed.json()["data"]["command_id"]
        attach_lease = self._internal(
            "GET",
            "/api/v1/internal/commands/lease",
            query={"limit": 20, "wait_seconds": 0},
        ).json()["data"]["commands"][0]
        self.assertEqual(attach_lease["command_id"], attach_id)
        self.assertEqual(attach_lease["payload"]["upload"]["sha256"], digest)

        download = self._internal(
            "GET", f"/api/v1/internal/uploads/{upload_id}/content"
        )
        self.assertEqual(download.status_code, 200, download.text)
        self.assertEqual(download.content, PNG_1X1)

        projection3 = dict(projection2)
        projection3["steps"] = [dict(projection2["steps"][0])]
        projection3["steps"][0]["photos"] = [
            {"photo_id": "local-photo-id", "name": "step.png", "sha256": digest, "size": len(PNG_1X1)}
        ]
        attach_ack = self._internal(
            "POST",
            f"/api/v1/internal/commands/{attach_id}/ack",
            payload={
                "outcome": "succeeded",
                "authority_version": 3,
                "projection_revision": 3,
                "result": {},
                "projection": projection3,
            },
        )
        self.assertEqual(attach_ack.status_code, 200, attach_ack.text)

        public = self.operator_client.get("/api/v1/work-orders/session").json()["data"]
        public_photo = public["steps"][0]["photos"][0]
        self.assertEqual(public_photo["photo_id"], upload_id)
        preview = self.operator_client.get(public_photo["preview_url"])
        self.assertEqual(preview.status_code, 200)
        self.assertEqual(preview.content, PNG_1X1)

        projection4 = dict(projection3)
        projection4["steps"] = [dict(projection3["steps"][0])]
        projection4["steps"][0]["photos"] = [
            {"name": "step.png", "sha256": digest, "size": len(PNG_1X1)}
        ]
        pushed = self._internal(
            "PUT",
            "/api/v1/internal/groups/public-group-0001/projection",
            payload={
                "state": "active",
                "authority_version": 4,
                "projection_revision": 4,
                "projection": projection4,
            },
        )
        self.assertEqual(pushed.status_code, 200, pushed.text)
        persisted_photo = self.operator_client.get(
            "/api/v1/work-orders/session"
        ).json()["data"]["steps"][0]["photos"][0]
        self.assertEqual(persisted_photo["photo_id"], upload_id)

        projection5 = dict(projection4)
        projection5["steps"] = [dict(projection4["steps"][0])]
        projection5["steps"][0]["photos"] = []
        cleared = self._internal(
            "PUT",
            "/api/v1/internal/groups/public-group-0001/projection",
            payload={
                "state": "active",
                "authority_version": 5,
                "projection_revision": 5,
                "projection": projection5,
            },
        )
        self.assertEqual(cleared.status_code, 200, cleared.text)
        self.assertEqual(
            self.operator_client.get(public_photo["preview_url"]).status_code,
            404,
        )

        cancelled = self._internal(
            "POST",
            "/api/v1/internal/groups/public-group-0001/cancel",
            payload={"reason": "target_deleted"},
        )
        self.assertEqual(cancelled.status_code, 200, cancelled.text)
        self.assertEqual(self.operator_client.get("/api/v1/work-orders/session").status_code, 401)

    def test_expired_photo_command_can_retry_without_duplicate_or_success_replay(self) -> None:
        self._lease()
        self._register()
        current_step = {
            "step_key": "1:1",
            "run_index": 1,
            "step_index": 1,
            "step_count": 1,
            "content": "当前步骤",
            "position": "current",
            "operator_required": True,
            "reviewer_required": True,
            "photos": [],
        }
        projection = {
            "operator": {"state": "active", "steps": [current_step]},
            "reviewer": {"state": "active", "steps": [current_step]},
        }
        updated = self._internal(
            "PUT",
            "/api/v1/internal/groups/public-group-0001/projection",
            payload={
                "state": "active",
                "authority_version": 1,
                "projection_revision": 2,
                "projection": projection,
            },
        )
        self.assertEqual(updated.status_code, 200, updated.text)
        csrf = self._exchange_operator()["csrf_token"]
        digest = hashlib.sha256(PNG_1X1).hexdigest()
        initialized = self.operator_client.post(
            "/api/v1/work-orders/uploads",
            json={
                "step_key": "1:1",
                "expected_version": 1,
                "file_name": "retry.png",
                "content_type": "image/png",
                "size": len(PNG_1X1),
                "sha256": digest,
            },
            headers={"X-CSRF-Token": csrf},
        )
        upload_id = initialized.json()["data"]["upload_id"]
        content = self.operator_client.put(
            f"/api/v1/work-orders/uploads/{upload_id}/content",
            content=PNG_1X1,
            headers={
                "Content-Type": "image/png",
                "X-Content-SHA256": digest,
                "X-CSRF-Token": csrf,
            },
        )
        self.assertEqual(content.status_code, 200, content.text)
        completed = self.operator_client.post(
            f"/api/v1/work-orders/uploads/{upload_id}/complete",
            headers={"X-CSRF-Token": csrf},
        )
        self.assertEqual(completed.status_code, 202, completed.text)
        command_id = completed.json()["data"]["command_id"]
        store = self.app.state.store
        with store._transaction() as connection:
            connection.execute(
                "UPDATE commands SET status='expired', expires_at=?, updated_at=? WHERE command_id=?",
                (0, 0, command_id),
            )
        session = store.authenticate_session(
            str(self.operator_client.cookies.get("wo_session") or ""), touch=False
        )

        def retry() -> tuple[str, str]:
            command, _created = store.enqueue_command(
                session=session,
                idempotency_key=f"attach_{upload_id}",
                kind="attach_photo",
                payload={"upload_id": upload_id, "step_key": "1:1"},
                expected_version=1,
                ttl_seconds=60,
            )
            return command["command_id"], command["status"]

        with ThreadPoolExecutor(max_workers=4) as executor:
            retried = list(executor.map(lambda _index: retry(), range(4)))
        self.assertEqual({item[0] for item in retried}, {command_id})
        self.assertEqual({item[1] for item in retried}, {"pending"})
        with store._connection() as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM commands WHERE idempotency_key=?",
                    (f"attach_{upload_id}",),
                ).fetchone()[0],
                1,
            )
        with store._transaction() as connection:
            connection.execute(
                "UPDATE commands SET status='succeeded', updated_at=? WHERE command_id=?",
                (1, command_id),
            )
            connection.execute(
                "UPDATE uploads SET status='authority_attached', updated_at=? WHERE upload_id=?",
                (1, upload_id),
            )
        succeeded_id, succeeded_status = retry()
        self.assertEqual((succeeded_id, succeeded_status), (command_id, "succeeded"))

    def test_authority_lease_fencing_without_static_credentials(self) -> None:
        first = self._internal(
            "POST",
            "/api/v1/internal/authority/lease",
            payload={"instance_id": "test-instance-01"},
            fence=False,
        )
        self.assertEqual(first.status_code, 200, first.text)
        first_data = first.json()["data"]
        self.assertNotIn("authority_id", first_data)
        self.fencing_token = first_data["fencing_token"]
        renewed = self._lease()
        self.assertEqual(renewed["authority_epoch"], first_data["authority_epoch"])
        self.assertEqual(renewed["fencing_token"], first_data["fencing_token"])

        busy = self._internal(
            "POST",
            "/api/v1/internal/authority/lease",
            payload={"instance_id": "other-instance-02"},
            fence=False,
        )
        self.assertEqual(busy.status_code, 409, busy.text)
        self.assertEqual(busy.json()["error_code"], "authority_lease_busy")

    def test_role_link_isolation_and_projection_monotonicity(self) -> None:
        self._lease()
        self._register()
        wrong_secret = self.operator_client.post(
            "/api/v1/link-sessions/exchange",
            json={"link_id": "operator-link-0001", "secret": "reviewer-role-secret-0123456789"},
        )
        self.assertEqual(wrong_secret.status_code, 403)
        with TestClient(self.app) as reviewer_client:
            reviewer = reviewer_client.post(
                "/api/v1/link-sessions/exchange",
                json={
                    "link_id": "reviewer-link-0001",
                    "secret": "reviewer-role-secret-0123456789",
                },
            )
            self.assertEqual(reviewer.status_code, 200, reviewer.text)
            reviewer_snapshot = reviewer.json()["data"]["snapshot"]
            self.assertEqual(reviewer_snapshot["role"], "reviewer")
            self.assertTrue(reviewer_snapshot["can_rollback_previous"])

        projection = {
            "state": "active",
            "authority_version": 2,
            "projection_revision": 2,
            "projection": {"state": "active", "title": "新版", "steps": [], "work_orders": []},
        }
        updated = self._internal(
            "PUT", "/api/v1/internal/groups/public-group-0001/projection", payload=projection
        )
        self.assertEqual(updated.status_code, 200, updated.text)
        stale = dict(projection)
        stale["projection_revision"] = 1
        stale_result = self._internal(
            "PUT", "/api/v1/internal/groups/public-group-0001/projection", payload=stale
        )
        self.assertEqual(stale_result.status_code, 409, stale_result.text)
        self.assertEqual(stale_result.json()["error_code"], "stale_projection")


if __name__ == "__main__":
    unittest.main()
