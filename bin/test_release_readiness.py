# -*- coding: utf-8 -*-
from __future__ import annotations

import unittest
from pathlib import Path

from bin.tools import release_readiness_check as readiness


class ReleaseReadinessTests(unittest.TestCase):
    def test_frontend_dist_rejects_native_prompt(self) -> None:
        dist_index = (
            readiness.BIN_DIR
            / "lan_bitable_template_portal"
            / "frontend"
            / "dist"
            / "index.html"
        )
        self.assertTrue(dist_index.is_file(), "Vue dist/index.html must exist")
        asset_names = readiness.re.findall(
            r"/assets/([^\"'>]+)", dist_index.read_text(encoding="utf-8", errors="ignore")
        )
        self.assertTrue(asset_names, "Vue dist/index.html must reference assets")
        for name in asset_names:
            path = dist_index.parent / "assets" / name
            if Path(name).suffix.lower() != ".js":
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
            self.assertNotIn("window.prompt", text)
            self.assertNotIn("chooseCandidateByPrompt", text)

    def test_legacy_server_not_instantiable(self) -> None:
        server_text = (
            readiness.BIN_DIR / "lan_bitable_template_portal" / "server.py"
        ).read_text(encoding="utf-8", errors="ignore")
        self.assertNotIn("PortalHandler", server_text)
        self.assertNotIn("from http.server import", server_text)
        self.assertNotIn("ThreadingHTTPServer(", server_text)


if __name__ == "__main__":
    unittest.main()
