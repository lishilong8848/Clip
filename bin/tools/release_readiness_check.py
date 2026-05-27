# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path


BIN_DIR = Path(__file__).resolve().parents[1]
PROJECT_ROOT = BIN_DIR.parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from lan_bitable_template_portal.state_store import LanPortalStateStore  # noqa: E402
from lan_bitable_template_portal import server as portal_server  # noqa: E402


RUNTIME_SUFFIXES = (
    ".db",
    ".sqlite",
    ".sqlite3",
    ".db-wal",
    ".db-shm",
    ".sqlite-wal",
    ".sqlite-shm",
    ".sqlite3-wal",
    ".sqlite3-shm",
)

ALLOWED_REQUESTS_FILES = {
    "bin/upload_event_module/services/remote_patch_updater.py",  # package/patch download
    "bin/tools/release_readiness_check.py",  # scans for the literal pattern
}

DISALLOWED_RELEASE_ENV_FLAGS = (
    "CLIPFLOW_DISABLE_UNIFIED_FEISHU_HTTP",
)


def _git_tracked_files() -> list[str]:
    try:
        proc = subprocess.run(
            ["git", "ls-files", "-z"],
            cwd=PROJECT_ROOT,
            check=True,
            capture_output=True,
        )
    except Exception:
        return []
    raw = proc.stdout.decode("utf-8", errors="ignore")
    return [item for item in raw.split("\0") if item]


def _is_runtime_path(path: str) -> bool:
    normalized = path.replace("\\", "/").lower()
    parts = normalized.split("/")
    if "node_modules" in parts:
        return True
    if "bin" in parts:
        for index, part in enumerate(parts[:-1]):
            if part == "bin" and parts[index + 1] == "data":
                return True
    if normalized.endswith(RUNTIME_SUFFIXES):
        return True
    return False


def check_git_runtime_data() -> tuple[bool, list[str]]:
    tracked = _git_tracked_files()
    offenders = [path for path in tracked if _is_runtime_path(path)]
    return not offenders, offenders


def check_state_schema() -> tuple[bool, dict]:
    with tempfile.TemporaryDirectory() as tmp:
        store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
        report = store.runtime_health_report()
    return bool(report.get("ok")), report


def check_requests_usage() -> tuple[bool, list[str]]:
    offenders: list[str] = []
    for path_text in _git_tracked_files():
        normalized = path_text.replace("\\", "/")
        if not normalized.endswith(".py"):
            continue
        if normalized in ALLOWED_REQUESTS_FILES:
            continue
        path = PROJECT_ROOT / path_text
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        if "import requests" in text or "requests." in text:
            offenders.append(normalized)
    return not offenders, offenders


def check_frontend_dist() -> tuple[bool, str, bool]:
    dist_index = BIN_DIR / "lan_bitable_template_portal" / "frontend" / "dist" / "index.html"
    if not dist_index.is_file():
        return False, f"Vue 构建产物不存在: {dist_index}", False
    ready = portal_server.portal_frontend_dist_ready()
    if not ready:
        return False, "Vue dist 不可用；生产默认页面必须使用 Vue dist。", False
    html = dist_index.read_text(encoding="utf-8", errors="ignore")
    referenced_assets = set(re.findall(r"/assets/([^\"'>]+)", html))
    if not referenced_assets:
        return False, "Vue dist/index.html 未引用构建资源。", False
    dist_assets = dist_index.parent / "assets"
    missing = [name for name in sorted(referenced_assets) if not (dist_assets / name).is_file()]
    if missing:
        return False, "Vue dist 缺少入口引用资源: " + ", ".join(missing[:10]), False
    extra_assets = [
        path.name
        for path in sorted(dist_assets.glob("*"))
        if path.is_file() and path.name not in referenced_assets and path.suffix.lower() in {".js", ".css"}
    ]
    if extra_assets:
        return False, "Vue dist 存在入口未引用的旧资源: " + ", ".join(extra_assets[:10]), False
    placeholder_hits: list[str] = []
    for name in sorted(referenced_assets):
        path = dist_assets / name
        if path.suffix.lower() not in {".js", ".css"}:
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        if "Vue migration workspace" in text or "当前生产页面仍由 legacy" in text:
            placeholder_hits.append(name)
    if placeholder_hits:
        return False, "Vue dist 仍包含迁移占位文案: " + ", ".join(placeholder_hits[:10]), False
    return True, f"Vue dist 已就绪: {dist_index}", True


def check_release_env_flags() -> tuple[bool, list[str]]:
    offenders = [
        name
        for name in DISALLOWED_RELEASE_ENV_FLAGS
        if str(os.environ.get(name) or "").strip() == "1"
    ]
    return not offenders, offenders


def check_fastapi_models() -> tuple[bool, list[str]]:
    main_path = BIN_DIR / "clipflow_backend" / "main.py"
    text = main_path.read_text(encoding="utf-8", errors="ignore")
    offenders: list[str] = []
    for match in re.finditer(r"await\s+self\._read_json_request\(request\)", text):
        prefix = text[max(0, match.start() - 160): match.start()]
        if "def _read_model_request" in prefix:
            continue
        line = text.count("\n", 0, match.start()) + 1
        offenders.append(f"bin/clipflow_backend/main.py:{line}")
    return not offenders, offenders


def check_qt_model_view_default() -> tuple[bool, str]:
    ui_path = BIN_DIR / "upload_event_module" / "ui" / "main_window_ui.py"
    records_path = BIN_DIR / "upload_event_module" / "ui" / "main_window_records.py"
    ui_text = ui_path.read_text(encoding="utf-8", errors="ignore")
    records_text = records_path.read_text(encoding="utf-8", errors="ignore")
    required = (
        "def _active_model_view_visible" in ui_text
        and "ActiveNoticeListRoute" in ui_text
        and 'self.list_active_event = ActiveNoticeListRoute("event")' in ui_text
        and 'self.list_active_other = ActiveNoticeListRoute("other")' in ui_text
        and "return True" in ui_text
        and "def _active_notice_model_enabled" in records_text
        and "return True" in records_text
    )
    if not required:
        return False, "Qt 活动列表默认 model/delegate 路径检查失败。"
    return True, "Qt 活动列表固定走 model/delegate。"


def main() -> int:
    failures: list[str] = []
    warnings: list[str] = []

    ok, offenders = check_git_runtime_data()
    if not ok:
        failures.append("git 已跟踪运行数据或依赖目录: " + ", ".join(offenders[:20]))

    ok, report = check_state_schema()
    if not ok:
        failures.append(f"SQLite schema 自检失败: {report}")

    ok, requests_offenders = check_requests_usage()
    if not ok:
        failures.append(
            "业务路径存在未隔离 requests 调用: " + ", ".join(requests_offenders[:20])
        )

    ok, frontend_message, frontend_ready = check_frontend_dist()
    if not ok:
        failures.append(frontend_message)
    elif not frontend_ready:
        warnings.append(frontend_message)

    ok, env_offenders = check_release_env_flags()
    if not ok:
        failures.append("当前环境启用了生产禁用的回退/禁用开关: " + ", ".join(env_offenders))

    ok, model_offenders = check_fastapi_models()
    if not ok:
        failures.append("FastAPI 业务接口仍存在未模型化 JSON 请求读取: " + ", ".join(model_offenders))

    ok, qt_model_message = check_qt_model_view_default()
    if not ok:
        failures.append(qt_model_message)
    else:
        warnings.append(qt_model_message)

    if failures:
        print("[ReleaseCheck] FAIL")
        for item in failures:
            print(f"- {item}")
        return 1
    if warnings:
        print("[ReleaseCheck] WARN")
        for item in warnings:
            print(f"- {item}")
    print("[ReleaseCheck] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
