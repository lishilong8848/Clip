from __future__ import annotations

import ast
import json
import sys
import tempfile
import threading
import time
import unittest
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

import httpx

BIN = Path(__file__).resolve().parent
ROOT = BIN.parent
if str(BIN) not in sys.path:
    sys.path.insert(0, str(BIN))
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from upload_event_module.services.http_client import FeishuHttpClient
from upload_event_module.services.remote_patch_updater import RemotePatchUpdater
from upload_event_module.ui.active_notice_index import ActiveNoticeIndex
from lan_bitable_template_portal.state_store import LanPortalStateStore
import package_portable


class _Item:
    valid = True

    def __init__(self, record_id: str):
        self.payload = {"record_id": record_id, "active_item_id": record_id, "text": record_id}

    def data(self, _role):
        return dict(self.payload)


class PerformanceGuardTests(unittest.TestCase):
    def test_active_index_reuses_snapshot_until_invalidated(self):
        calls = 0
        item = _Item("one")

        def items():
            nonlocal calls
            calls += 1
            return [(object(), item)]

        index = ActiveNoticeIndex(lambda candidate: candidate.valid)
        self.assertEqual(index.data_snapshot(items)[0]["record_id"], "one")
        self.assertEqual(index.data_snapshot(items)[0]["record_id"], "one")
        self.assertEqual(calls, 1)
        index.invalidate()
        index.data_snapshot(items)
        self.assertEqual(calls, 2)

    def test_empty_runtime_queue_does_not_begin_immediate(self):
        with tempfile.TemporaryDirectory() as temp:
            statements = []

            class Store(LanPortalStateStore):
                def _connect(self):
                    conn = super()._connect()
                    conn.set_trace_callback(statements.append)
                    return conn

            store = Store(Path(temp) / "state.sqlite3")
            store.get_settings()
            statements.clear()
            self.assertEqual(store.lease_runtime_queue_items("message"), [])
            self.assertFalse(any("BEGIN IMMEDIATE" in sql.upper() for sql in statements))

    def test_outbox_payload_is_idempotent(self):
        with tempfile.TemporaryDirectory() as temp:
            store = LanPortalStateStore(Path(temp) / "state.sqlite3")
            first = store.enqueue_outbox_event("qt_action", {"idempotency_key": "delete:r1", "kind": "active_delete", "payload": {"record_id": "r1"}})
            second = store.enqueue_outbox_event("qt_action", {"idempotency_key": "delete:r1", "payload": {"record_id": "r1"}, "kind": "active_delete"})
            third = store.enqueue_outbox_event("qt_action", {"idempotency_key": "delete:r2", "kind": "active_delete", "payload": {"record_id": "r2"}})
            self.assertEqual(first, second)
            self.assertNotEqual(first, third)

    def test_http_client_allows_parallel_requests_and_honors_retry_after(self):
        lock = threading.Lock()
        active = maximum = calls = 0

        def handler(request):
            nonlocal active, maximum, calls
            with lock:
                calls += 1
                call = calls
                active += 1
                maximum = max(maximum, active)
            threading.Event().wait(0.04)
            with lock:
                active -= 1
            if call == 3:
                return httpx.Response(429, headers={"Retry-After": "0.01"}, json={"code": 429}, request=request)
            return httpx.Response(200, json={"code": 0}, request=request)

        client = FeishuHttpClient(transport=httpx.MockTransport(handler), retries=1)
        try:
            with ThreadPoolExecutor(max_workers=2) as pool:
                list(pool.map(lambda _: client.request_json("GET", "https://open.feishu.cn/test"), range(2)))
            with patch("upload_event_module.services.http_client.time.sleep") as sleep:
                client.request_json("GET", "https://open.feishu.cn/retry")
                sleep.assert_called_once_with(0.01)
        finally:
            client.close()
        self.assertEqual(maximum, 2)

    def test_patch_zip_rejects_parent_paths_and_meta_has_file_hashes(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            archive = root / "bad.zip"
            with zipfile.ZipFile(archive, "w") as stream:
                stream.writestr("../outside.txt", "bad")
            updater = RemotePatchUpdater(root / "app", root / "data", "")
            with self.assertRaisesRegex(RuntimeError, "unsafe path"):
                updater._extract_patch_dir(archive)
            self.assertFalse((root / "outside.txt").exists())

            patch_dir = root / "patch"
            source = patch_dir / "bin" / "sample.py"
            source.parent.mkdir(parents=True)
            source.write_text("VALUE = 1\n", encoding="utf-8")
            package_portable.write_patch_meta(patch_dir, target_build_id="test")
            meta = json.loads((patch_dir / "bin" / "patch_meta.json").read_text(encoding="utf-8"))
            self.assertIn("bin/sample.py", meta["file_sha256"])

    def test_cabinet_editor_filters_all_blank_operation_groups(self):
        source = (BIN / "lan_bitable_template_portal" / "frontend" / "src" / "components" / "CabinetPowerPage.vue").read_text(encoding="utf-8")
        self.assertIn("groupHasBusinessData(group) || group?._editing", source)
        self.assertNotIn("form.groups = form.groups.filter(groupHasBusinessData)", source)

    def test_extra_screenshot_callback_does_not_submit_notice(self):
        source = (BIN / "upload_event_module" / "ui" / "dialogs.py").read_text(encoding="utf-8")
        methods = {
            node.name: ast.unparse(node)
            for node in ast.walk(ast.parse(source))
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        }
        self.assertIn("self.upload_confirmed.emit", methods["skip_screenshot"])
        self.assertNotIn("self.upload_confirmed.emit", methods["_on_extra_screenshot_encoded"])
        self.assertNotIn("self._capture_generation += 1", methods["_on_extra_screenshot_captured"])


if __name__ == "__main__":
    unittest.main()
