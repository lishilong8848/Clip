from __future__ import annotations

import importlib.util
import re
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).parent / "public_polling_relay" / "frontend.py"
SPEC = importlib.util.spec_from_file_location("public_polling_relay_frontend", MODULE_PATH)
assert SPEC and SPEC.loader
frontend = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(frontend)


class PublicPollingRelayFrontendTests(unittest.TestCase):
    def test_pages_are_separate_and_use_fragment_exchange(self) -> None:
        overview = frontend.render_polling_work_order_page()
        steps = frontend.render_polling_work_order_steps_page()

        self.assertIn('id="overview"', overview)
        self.assertNotIn('id="steps"', overview)
        self.assertIn('id="steps"', steps)
        self.assertNotIn('id="overview"', steps)
        for page in (overview, steps):
            self.assertIn("/api/v1/link-sessions/exchange", page)
            self.assertIn("history.replaceState", page)
            self.assertIn("params.get('link')", page)
            self.assertIn("params.get('link_id')", page)
            self.assertNotIn("?token=", page)
            self.assertIn("sessionStorage.setItem('polling-relay-csrf'", page)
            self.assertNotIn("localStorage", page)

    def test_relay_api_contract_and_offline_guards_are_present(self) -> None:
        combined = frontend.render_polling_work_order_page() + frontend.render_polling_work_order_steps_page()

        for path in (
            "/api/v1/work-orders/session",
            "/api/v1/work-orders/commands",
            "/api/v1/work-orders/uploads",
            "/api/v1/work-orders/photos",
        ):
            self.assertIn(path, combined)
        for command in ("activate", "release", "confirm", "rollback", "retry_attachment"):
            self.assertIn(f"'{command}'", combined)
        self.assertIn("data.command_id", combined)
        self.assertIn("'X-CSRF-Token':relay.csrf", combined)
        self.assertIn("'Idempotency-Key':randomId()", combined)
        self.assertIn("authorityOnline()", combined)
        self.assertIn("function writeAvailable()", combined)
        self.assertIn("if(!writeAvailable())throw", combined)
        self.assertIn("所有操作已禁用", combined)
        self.assertIn("wait_seconds", combined)
        self.assertIn("after_revision", combined)

    def test_steps_support_countdown_and_multiple_mobile_photos(self) -> None:
        page = frontend.render_polling_work_order_steps_page()

        self.assertIn("setInterval(refreshCountdown,1000)", page)
        self.assertIn("input.multiple=true", page)
        self.assertIn("input.setAttribute('capture','environment')", page)
        self.assertIn("8*1024*1024", page)
        self.assertIn("X-Content-SHA256", page)
        self.assertIn("回退上一步", page)
        self.assertIn("noopener noreferrer", page)
        self.assertNotIn("photoRequired", page)
        self.assertNotIn("无需拍照", page)

    def test_embedded_javascript_parses(self) -> None:
        node = shutil.which("node")
        if not node:
            self.skipTest("node is unavailable")
        for page in (
            frontend.render_polling_work_order_page(),
            frontend.render_polling_work_order_steps_page(),
        ):
            scripts = re.findall(r"<script>(.*?)</script>", page, flags=re.S)
            self.assertEqual(len(scripts), 1)
            with tempfile.NamedTemporaryFile("w", suffix=".js", encoding="utf-8", delete=False) as handle:
                handle.write(scripts[0])
                path = Path(handle.name)
            try:
                result = subprocess.run([node, "--check", str(path)], capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, result.stderr)
            finally:
                path.unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
