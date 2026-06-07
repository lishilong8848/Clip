# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import textwrap
import threading
import time
from pathlib import Path

BIN_DIR = Path(__file__).resolve().parents[1]
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from clipflow_backend.main import FastAPIPortalController, _wait_until_listening  # noqa: E402
from lan_bitable_template_portal.portal_auth import AUTH_COOKIE_NAME, PortalAuthManager  # noqa: E402
from lan_bitable_template_portal.portal_service import SCOPE_OPTIONS  # noqa: E402
from lan_bitable_template_portal.server import PortalRuntime, find_available_port  # noqa: E402
from lan_bitable_template_portal.state_store import LanPortalStateStore  # noqa: E402


class _SmokePortalService:
    _last_loaded_at = "smoke"
    _last_loaded_ts = 0.0
    _load_warnings: list[str] = []

    def _normalize_scope(self, scope: str) -> str:
        text = str(scope or "").strip().upper()
        if text in {"ALL", "CAMPUS", "110"}:
            return text
        for code in ("A", "B", "C", "D", "E", "H"):
            if code in text:
                return code
        return "ALL"

    def _scope_matches_item(self, scope: str, item: dict) -> bool:
        normalized = self._normalize_scope(scope)
        if normalized == "ALL":
            return True
        codes = [
            self._normalize_scope(code)
            for code in (item.get("building_codes") or [])
            if self._normalize_scope(code) not in {"", "ALL"}
        ]
        if not codes:
            return True
        if normalized == "CAMPUS":
            return len(set(codes)) >= 2
        return len(set(codes)) == 1 and codes[0] == normalized

    def get_scope_overview(
        self,
        *,
        ongoing_items: list[dict] | None = None,
        scopes: list[str] | None = None,
        include_prepared: bool = False,
    ) -> dict:
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        scope_values = [str(scope or "").strip() for scope in (scopes or []) if str(scope or "").strip()]
        if not scope_values:
            scope_values = ["A", "B"]
        items: dict[str, dict] = {}
        for scope in scope_values:
            items[scope] = {
                "scope": scope,
                "maintenance_pending": 1,
                "change_pending": 1 if scope == "A" else 0,
                "repair_pending": 0,
                "maintenance_ongoing": 0,
                "change_ongoing": 0,
                "repair_ongoing": 0,
                "closed_today": 0,
                "last_loaded_at": now,
                "warnings": [],
            }
        return {
            "items": items,
            "scopes": items,
            "last_loaded_at": now,
            "warnings": [],
            "source_snapshot_ready": True,
        }

    def get_handover_links(self) -> dict:
        return {
            "links": {
                "A": "https://example.invalid/a",
                "B": "https://example.invalid/b",
            }
        }

    def query_records(
        self,
        *,
        month: str = "",
        specialty: str = "",
        scope: str = "ALL",
        ongoing_items: list[dict] | None = None,
    ) -> dict:
        today = time.strftime("%Y-%m-%d")
        records = [
            {
                "record_id": "src-maint-a-001",
                "source_record_id": "src-maint-a-001",
                "work_type": "maintenance",
                "notice_type": "维保通告",
                "building_codes": ["A"],
                "source_progress": "未开始",
                "maintenance_cycle": "每月",
                "display_fields": {
                    "楼栋": "A楼",
                    "维护总项": "冷机月度巡检",
                    "维护周期": "每月",
                    "维护实施状态": "未开始",
                    "专业类别": "暖通",
                },
            },
            {
                "record_id": "src-change-a-001",
                "source_record_id": "src-change-a-001",
                "work_type": "change",
                "notice_type": "设备变更",
                "building_codes": ["A"],
                "source_progress": "未开始",
                "display_fields": {
                    "变更简述": "A楼网络设备变更测试",
                    "变更楼栋": "A楼",
                    "变更进度": "未开始",
                    "专业": "网络",
                    "变更等级（阿里）": "I3",
                },
            },
        ]
        ongoing = [
            {
                "active_item_id": "active-change-a-001",
                "record_id": "target-change-a-001",
                "target_record_id": "target-change-a-001",
                "source_record_id": "src-change-active-a-001",
                "work_type": "change",
                "notice_type": "设备变更",
                "title": "A楼UPS旁路切换变更测试",
                "building": "A楼",
                "building_codes": ["A"],
                "specialty": "供配电",
                "level": "I3",
                "start_time": f"{today}T09:30",
                "end_time": f"{today}T18:30",
                "location": "A楼配电间",
                "content": "测试测试测试",
                "reason": "测试测试测试",
                "impact": "测试测试测试",
                "progress": "测试测试测试",
                "status": "已开始",
                "uploaded": True,
            }
        ]
        return {
            "scope": scope,
            "records": records,
            "ongoing": ongoing,
            "zhihang_change_records": [
                {
                    "record_id": "zhihang-a-001",
                    "title": "A楼智航侧变更测试",
                    "building": "A楼",
                    "progress": "进行中",
                }
            ],
            "daily_summary": {
                "date": today,
                "items": [],
                "stats": {"started": 1, "updated": 0, "ended": 0, "ongoing": 1},
            },
            "defaults": {
                "impact": "对业务无影响",
                "progress": "准备工作已完成",
            },
            "warnings": [],
            "last_loaded_at": "smoke",
            "source_snapshot_ready": True,
        }

    def list_available_notice_undos(
        self,
        *,
        scope: str = "ALL",
        action_type: str = "",
        since_seconds: float = 0,
    ) -> list[dict]:
        return []


def _build_playwright_script(url: str, session_id: str) -> str:
    payload = {
        "url": url,
        "session_id": session_id,
        "cookie_name": AUTH_COOKIE_NAME,
    }
    return textwrap.dedent(
        f"""
        const {{ chromium }} = require('playwright');
        const cfg = {json.dumps(payload, ensure_ascii=False)};

        (async () => {{
          const browser = await chromium.launch({{ headless: true }});
          const context = await browser.newContext();
          await context.addCookies([{{
            name: cfg.cookie_name,
            value: cfg.session_id,
            domain: '127.0.0.1',
            path: '/',
          }}]);
          const page = await context.newPage();
          const errors = [];
          const failedResponses = [];
          page.on('console', msg => {{
            if (msg.type() === 'error') errors.push(msg.text());
          }});
          page.on('pageerror', err => errors.push(String(err)));
          page.on('response', response => {{
            if (response.status() >= 400) {{
              failedResponses.push(`${{response.status()}} ${{response.url()}}`);
            }}
          }});
          const response = await page.goto(cfg.url, {{
            waitUntil: 'domcontentloaded',
            timeout: 20000,
          }});
          if (!response || !response.ok()) {{
            throw new Error(`page load failed: ${{response && response.status()}}`);
          }}
          await page.waitForSelector('text=南通基地-运维灯塔工作台', {{ timeout: 10000 }});
          await page.waitForSelector('text=功能选择', {{ timeout: 10000 }});
          const bodyText = await page.locator('body').innerText({{ timeout: 10000 }});
          const required = ['功能选择', '交接班审核页', '维保 / 变更 / 检修'];
          for (const marker of required) {{
            if (!bodyText.includes(marker)) throw new Error(`missing marker: ${{marker}}`);
          }}
          const forbidden = ['Vue migration workspace', '当前生产页面仍由 legacy index.html 提供'];
          for (const marker of forbidden) {{
            if (bodyText.includes(marker)) throw new Error(`legacy marker visible: ${{marker}}`);
          }}
          await page.getByRole('button', {{ name: '选择楼栋' }}).click();
          await page.waitForSelector('text=选择楼栋进入工作台', {{ timeout: 10000 }});
          const scopeCard = page.locator('article.scope-card').filter({{ hasText: 'A楼' }}).first();
          await scopeCard.getByRole('button', {{ name: '进入工作台' }}).click();
          await page.waitForSelector('text=待发起事项', {{ timeout: 10000 }});
          await page.waitForSelector('text=冷机月度巡检', {{ timeout: 10000 }});
          await page.waitForSelector('text=已开始未结束', {{ timeout: 10000 }});
          await page.waitForSelector('text=A楼UPS旁路切换变更测试', {{ timeout: 10000 }});
          await page.locator('article.notice-row').filter({{ hasText: '冷机月度巡检' }}).first().click();
          await page.waitForSelector('text=待发起通告', {{ timeout: 10000 }});
          await page.waitForSelector('text=发送开始', {{ timeout: 10000 }});
          await page.locator('article.ongoing-card').filter({{ hasText: 'A楼UPS旁路切换变更测试' }}).first().click();
          await page.waitForSelector('text=更新', {{ timeout: 10000 }});
          await page.waitForSelector('text=结束', {{ timeout: 10000 }});
          if (errors.length || failedResponses.length) {{
            throw new Error(`browser runtime errors: ${{errors.join(' | ')}} failedResponses=${{failedResponses.join(' | ')}}`);
          }}
          await browser.close();
          console.log(JSON.stringify({{
            ok: true,
            title: await page.title().catch(() => ''),
            markers: [...required, 'A楼工作台', '待发起事项', '已开始未结束'],
          }}));
        }})().catch(async err => {{
          console.error(String(err && err.stack || err));
          process.exit(1);
        }});
        """
    ).strip()


def run_smoke(*, port: int = 18976, keep_server_seconds: float = 0.0) -> dict:
    node = shutil.which("node")
    npx = shutil.which("npx")
    frontend_dir = BIN_DIR / "lan_bitable_template_portal" / "frontend"
    frontend_node_modules = frontend_dir / "node_modules"
    local_playwright = frontend_node_modules / "playwright"
    if not node and not npx:
        raise RuntimeError("node/npx 不可用，无法运行 Playwright 浏览器 smoke。")

    previous_mock = os.environ.get("CLIPFLOW_BACKEND_MOCK_EXTERNAL")
    os.environ["CLIPFLOW_BACKEND_MOCK_EXTERNAL"] = "1"

    original_service = PortalRuntime.service
    original_auth = PortalRuntime.auth_manager
    original_store = PortalRuntime.state_store
    original_callbacks = (
        PortalRuntime.notice_callback,
        PortalRuntime.ongoing_callback,
        PortalRuntime.ongoing_delete_callback,
        PortalRuntime.maintenance_action_callback,
    )
    temp_dir = tempfile.TemporaryDirectory()
    server = None
    server_thread: threading.Thread | None = None
    try:
        import uvicorn

        state_store = LanPortalStateStore(Path(temp_dir.name) / "state.sqlite3")
        auth_manager = PortalAuthManager()
        auth_manager._state_store = state_store
        auth_manager.upsert_permission_user(
            open_id="ou_frontend_smoke",
            name="frontend-smoke-admin",
            role="admin",
            scopes=["ALL"],
            enabled=True,
            updated_by="frontend-runtime-smoke",
        )
        session_id = "frontend-runtime-smoke-session"
        with auth_manager._lock:
            auth_manager._sessions[session_id] = {
                "session_id": session_id,
                "user": {
                    "name": "frontend-smoke-admin",
                    "open_id": "ou_frontend_smoke",
                },
                "role": "admin",
                "allowed_scopes": ["ALL"],
                "scope_options": SCOPE_OPTIONS,
                "expires_at": time.time() + 3600,
            }

        PortalRuntime.service = _SmokePortalService()
        PortalRuntime.state_store = state_store
        PortalRuntime.auth_manager = auth_manager
        PortalRuntime.notice_callback = None
        PortalRuntime.ongoing_callback = None
        PortalRuntime.ongoing_delete_callback = None
        PortalRuntime.maintenance_action_callback = None

        controller = FastAPIPortalController(host="127.0.0.1", port=port)
        app = controller._build_app()
        bound_port = find_available_port("127.0.0.1", int(port or 18976))
        config = uvicorn.Config(
            app,
            host="127.0.0.1",
            port=bound_port,
            log_level="warning",
            access_log=False,
            log_config=None,
        )
        server = uvicorn.Server(config)

        def _serve() -> None:
            server.run()

        server_thread = threading.Thread(
            target=_serve,
            name="ClipFlowFrontendRuntimeSmoke",
            daemon=True,
        )
        server_thread.start()
        if not _wait_until_listening("127.0.0.1", bound_port, timeout_s=5.0):
            raise RuntimeError("smoke FastAPI 服务启动超时")

        url = f"http://127.0.0.1:{bound_port}/"
        with tempfile.TemporaryDirectory() as script_dir:
            script_path = Path(script_dir) / "frontend_smoke.js"
            script_path.write_text(_build_playwright_script(url, session_id), encoding="utf-8")
            env = dict(os.environ)
            env["npm_config_cache"] = str(Path(script_dir) / "npm-cache")
            env["npm_config_prefix"] = str(Path(script_dir) / "npm-prefix")
            if local_playwright.is_dir() and node:
                command = [node, str(script_path)]
                env["NODE_PATH"] = str(frontend_node_modules)
            elif npx:
                command = [
                    npx,
                    "--yes",
                    "--package",
                    "playwright",
                    "node",
                    str(script_path),
                ]
            else:
                raise RuntimeError(
                    "未找到本地 playwright 依赖，且 npx 不可用；请先在 frontend 执行 npm install。"
                )
            completed = subprocess.run(
                command,
                cwd=str(BIN_DIR.parent),
                text=True,
                encoding="utf-8",
                errors="replace",
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env,
                timeout=60,
            )
        if keep_server_seconds > 0:
            time.sleep(float(keep_server_seconds))
        if completed.returncode != 0:
            raise RuntimeError(
                (completed.stderr or completed.stdout or "Playwright smoke failed").strip()
            )
        output = (completed.stdout or "").strip().splitlines()[-1]
        data = json.loads(output)
        data["url"] = url
        return data
    finally:
        if server is not None:
            server.should_exit = True
        if server_thread and server_thread.is_alive():
            server_thread.join(timeout=3)
        PortalRuntime.service = original_service
        PortalRuntime.auth_manager = original_auth
        PortalRuntime.state_store = original_store
        (
            PortalRuntime.notice_callback,
            PortalRuntime.ongoing_callback,
            PortalRuntime.ongoing_delete_callback,
            PortalRuntime.maintenance_action_callback,
        ) = original_callbacks
        temp_dir.cleanup()
        if previous_mock is None:
            os.environ.pop("CLIPFLOW_BACKEND_MOCK_EXTERNAL", None)
        else:
            os.environ["CLIPFLOW_BACKEND_MOCK_EXTERNAL"] = previous_mock


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a real-browser smoke test for the Vue portal dist.")
    parser.add_argument("--port", type=int, default=18976)
    parser.add_argument("--keep-server-seconds", type=float, default=0.0)
    args = parser.parse_args()
    try:
        result = run_smoke(port=args.port, keep_server_seconds=args.keep_server_seconds)
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False, indent=2))
        return 1
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
