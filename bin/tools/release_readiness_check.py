# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import re
import json
import gc
import importlib.util
import subprocess
import sys
import tempfile
import sqlite3
import time
from pathlib import Path


BIN_DIR = Path(__file__).resolve().parents[1]
PROJECT_ROOT = BIN_DIR.parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

REQUIRED_IMPORTS_FOR_CHECK = {
    "httpx": "httpx",
    "fastapi": "fastapi",
    "starlette": "starlette",
    "multipart": "python-multipart",
    "pydantic": "pydantic",
    "uvicorn": "uvicorn",
    "apscheduler": "APScheduler",
    "lark_oapi": "lark-oapi",
}


def _candidate_site_packages() -> list[Path]:
    candidates: list[Path] = []
    venv_root = BIN_DIR / ".venv"
    if os.name == "nt":
        candidates.append(venv_root / "Lib" / "site-packages")
    else:
        candidates.extend((venv_root / "lib").glob("python*/site-packages"))
    return [path for path in candidates if path.is_dir()]


def _add_project_runtime_site_packages() -> None:
    for path in _candidate_site_packages():
        text = str(path)
        if text not in sys.path:
            sys.path.insert(0, text)


_add_project_runtime_site_packages()

_missing_for_check = [
    package
    for module, package in REQUIRED_IMPORTS_FOR_CHECK.items()
    if importlib.util.find_spec(module) is None
]
if _missing_for_check:
    print("[ReleaseCheck] FAIL")
    print("- 发布就绪检查缺少运行依赖: " + ", ".join(_missing_for_check))
    print("  请先运行 package_portable.py 或主程序依赖自安装后重试。")
    sys.exit(1)

from lan_bitable_template_portal.state_store import LanPortalStateStore  # noqa: E402
from lan_bitable_template_portal import server as portal_server  # noqa: E402
from lan_bitable_template_portal.portal_service import (  # noqa: E402
    MaintenancePortalService,
    NOTICE_TEXT_TEMPLATES,
    NOTICE_TYPE_BY_WORK_TYPE,
    WORK_TYPE_BY_NOTICE_TYPE,
)


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
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
        report = store.runtime_health_report()
    return bool(report.get("ok")), report


def check_notice_identity_cleanup() -> tuple[bool, str]:
    message = "旧 local/空目标 identity 已在启动时软删除。"
    ok = True
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        db_path = Path(tmp) / "state.sqlite3"
        store = LanPortalStateStore(db_path)
        store.runtime_health_report()
        now = time.time()
        with sqlite3.connect(str(db_path)) as conn:
            conn.execute(
                """
                INSERT INTO notice_identity_map(
                    identity_id, work_type, notice_type, active_item_id,
                    source_record_id, target_record_id, title,
                    building_codes_json, payload_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "maintenance:active:release-local-only",
                    "maintenance",
                    "维保通告",
                    "release-local-only",
                    "",
                    "local_release_placeholder",
                    "旧本地占位",
                    "[]",
                    json.dumps(
                        {
                            "active_item_id": "release-local-only",
                            "record_id": "local_release_placeholder",
                            "target_record_id": "local_release_placeholder",
                            "work_type": "maintenance",
                        },
                        ensure_ascii=False,
                    ),
                    now,
                    now,
                ),
            )
            conn.execute(
                "DELETE FROM meta WHERE key = 'notice_identity_unbound_cleanup_v1_done'"
            )
            conn.commit()
        reloaded = LanPortalStateStore(db_path)
        identity = reloaded.resolve_notice_identity(
            work_type="maintenance",
            active_item_id="release-local-only",
        )
        if identity:
            ok = False
            message = "旧 local/空目标 identity 仍可被 resolve_notice_identity 命中。"
        del reloaded
        del store
        gc.collect()
    return ok, message


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
    frontend_dir = BIN_DIR / "lan_bitable_template_portal" / "frontend"
    package_path = frontend_dir / "package.json"
    smoke_script = BIN_DIR / "tools" / "frontend_runtime_smoke.py"
    dist_index = BIN_DIR / "lan_bitable_template_portal" / "frontend" / "dist" / "index.html"
    if not smoke_script.is_file():
        return False, "缺少 Vue 生产页浏览器 smoke 工具。", False
    try:
        package_data = json.loads(package_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return False, f"frontend/package.json 读取失败: {exc}", False
    scripts = package_data.get("scripts") if isinstance(package_data, dict) else {}
    dev_dependencies = package_data.get("devDependencies") if isinstance(package_data, dict) else {}
    if not isinstance(scripts, dict) or "frontend_runtime_smoke.py" not in str(
        scripts.get("smoke") or ""
    ):
        return False, "frontend/package.json 未接入 smoke 脚本。", False
    if not isinstance(dev_dependencies, dict) or "playwright" not in dev_dependencies:
        return False, "frontend/package.json 未声明 Playwright smoke 依赖。", False
    if not dist_index.is_file():
        return False, f"Vue 构建产物不存在: {dist_index}", False
    ready = portal_server.portal_frontend_dist_ready()
    if not ready:
        return False, "Vue dist 不可用；生产默认页面必须使用 Vue dist。", False
    html = dist_index.read_text(encoding="utf-8", errors="ignore")
    if "南通基地-运维灯塔工作台" not in html:
        return False, "Vue dist/index.html 缺少生产页面标题。", False
    referenced_assets = set(re.findall(r"/assets/([^\"'>]+)", html))
    if not referenced_assets:
        return False, "Vue dist/index.html 未引用构建资源。", False
    dist_assets = dist_index.parent / "assets"
    missing = [name for name in sorted(referenced_assets) if not (dist_assets / name).is_file()]
    if missing:
        return False, "Vue dist 缺少入口引用资源: " + ", ".join(missing[:10]), False
    reachable_assets = set(referenced_assets)
    pending_assets = list(sorted(referenced_assets))
    asset_ref_pattern = re.compile(r"(?:^|[\"'`(,])/?assets/([^\"'`),\s]+)")
    while pending_assets:
        name = pending_assets.pop()
        path = dist_assets / name
        if not path.is_file() or path.suffix.lower() not in {".js", ".css"}:
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        for match in asset_ref_pattern.findall(text):
            asset_name = match.strip()
            if not asset_name or asset_name in reachable_assets:
                continue
            reachable_assets.add(asset_name)
            pending_assets.append(asset_name)
    missing_reachable = [
        name for name in sorted(reachable_assets)
        if not (dist_assets / name).is_file()
    ]
    if missing_reachable:
        return False, "Vue dist 缺少动态引用资源: " + ", ".join(missing_reachable[:10]), False
    extra_assets = [
        path.name
        for path in sorted(dist_assets.glob("*"))
        if path.is_file() and path.name not in reachable_assets and path.suffix.lower() in {".js", ".css"}
    ]
    if extra_assets:
        return False, "Vue dist 存在入口未引用的旧资源: " + ", ".join(extra_assets[:10]), False
    placeholder_hits: list[str] = []
    forbidden_hits: list[str] = []
    syntax_failures: list[str] = []
    dist_text_by_suffix: dict[str, str] = {}
    for name in sorted(reachable_assets):
        path = dist_assets / name
        if path.suffix.lower() not in {".js", ".css"}:
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        dist_text_by_suffix[path.suffix.lower()] = dist_text_by_suffix.get(path.suffix.lower(), "") + "\n" + text
        if "Vue migration workspace" in text or "当前生产页面仍由 legacy" in text:
            placeholder_hits.append(name)
        if path.suffix.lower() == ".js":
            for pattern in ("window.prompt", "chooseCandidateByPrompt"):
                if pattern in text:
                    forbidden_hits.append(f"{name}:{pattern}")
            try:
                subprocess.run(
                    ["node", "--check", str(path)],
                    cwd=PROJECT_ROOT,
                    check=True,
                    capture_output=True,
                    text=True,
                    timeout=30,
                )
            except FileNotFoundError:
                syntax_failures.append(f"{name}: node 不可用")
            except subprocess.CalledProcessError as exc:
                detail = (exc.stderr or exc.stdout or "").strip().splitlines()
                syntax_failures.append(f"{name}: {detail[0] if detail else exc}")
            except Exception as exc:
                syntax_failures.append(f"{name}: {exc}")
    if placeholder_hits:
        return False, "Vue dist 仍包含迁移占位文案: " + ", ".join(placeholder_hits[:10]), False
    if forbidden_hits:
        return False, "Vue dist 包含禁用的旧前端交互: " + ", ".join(forbidden_hits[:10]), False
    if syntax_failures:
        return False, "Vue dist JS 语法检查失败: " + ", ".join(syntax_failures[:5]), False
    logo_path = dist_assets / "vnet-logo.png"
    if not logo_path.is_file() or logo_path.stat().st_size <= 100:
        return False, "Vue dist 缺少世纪互联 logo 资源: assets/vnet-logo.png", False
    css_text = dist_text_by_suffix.get(".css", "")
    css_markers = ("#0757d7", "#1678ff", "linear-gradient", "status-banner")
    missing_css_markers = [marker for marker in css_markers if marker not in css_text]
    if missing_css_markers:
        return False, "Vue dist CSS 缺少 VNET 蓝白生产样式: " + ", ".join(missing_css_markers), False
    js_text = dist_text_by_suffix.get(".js", "")
    js_markers = ("南通基地-运维灯塔工作台", "实时动态", "维护管理", "变更管理", "检修管理")
    missing_js_markers = [marker for marker in js_markers if marker not in js_text]
    if missing_js_markers:
        return False, "Vue dist JS 缺少生产页面关键文案: " + ", ".join(missing_js_markers), False
    return True, f"Vue dist 已就绪: {dist_index}", True


def check_frontend_api_client() -> tuple[bool, list[str]]:
    src_dir = BIN_DIR / "lan_bitable_template_portal" / "frontend" / "src"
    api_client = src_dir / "api" / "client.ts"
    offenders: list[str] = []
    if not api_client.is_file():
        return False, ["缺少 frontend/src/api/client.ts"]
    for path in sorted(src_dir.rglob("*")):
        if not path.is_file() or path.suffix.lower() not in {".ts", ".vue"}:
            continue
        rel = path.relative_to(PROJECT_ROOT).as_posix()
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        if path != api_client and "fetch(" in text:
            offenders.append(f"{rel}: 直接 fetch")
        if rel.endswith(".vue") and re.search(r"\basync\s+function\s+api\s*\(", text):
            offenders.append(f"{rel}: 局部 api() 封装")
    return not offenders, offenders


def check_frontend_realtime_coordination() -> tuple[bool, list[str]]:
    src_dir = BIN_DIR / "lan_bitable_template_portal" / "frontend" / "src"
    app_vue = src_dir / "App.vue"
    smoke_script = BIN_DIR / "tools" / "frontend_runtime_smoke.py"
    errors: list[str] = []
    try:
        app_text = app_vue.read_text(encoding="utf-8", errors="ignore")
    except Exception as exc:
        return False, [f"读取 App.vue 失败: {exc}"]
    try:
        smoke_text = smoke_script.read_text(encoding="utf-8", errors="ignore")
    except Exception as exc:
        return False, [f"读取 frontend_runtime_smoke.py 失败: {exc}"]

    app_markers = [
        "enterScope(scope: string, workType = \"maintenance\")",
        "new URL(\"/workbench-lite\", window.location.origin)",
        "portalRequest(`/api/auth/status?",
        "redirectToLogin",
        "signatureLinkMode",
    ]
    for marker in app_markers:
        if marker not in app_text:
            errors.append(f"App.vue 轻量门户壳缺少 {marker}")
    for forbidden in (
        "createCrossTabStreamCoordinator",
        "clipflow-jobs-stream-v1",
        "clipflow-active-items-stream-v1",
        "actionSubmitWorker",
        "workbenchLoadWorker",
        "submitWorkbenchActionInBackground",
    ):
        if forbidden in app_text:
            errors.append(f"App.vue 仍包含旧 Vue 工作台实时/worker 热路径: {forbidden}")

    smoke_markers = [
        "workbench-lite",
        "lite workbench action click blocked main thread",
        "lite workbench became sluggish after submit",
        "lite ongoing click caused full page reload",
        "lite paste parse caused full page reload",
        "source link options missing expected record",
    ]
    for marker in smoke_markers:
        if marker not in smoke_text:
            errors.append(f"frontend_runtime_smoke.py 未覆盖 {marker}")
    return not errors, errors


def check_frontend_freeze_guards() -> tuple[bool, list[str]]:
    src_dir = BIN_DIR / "lan_bitable_template_portal" / "frontend" / "src"
    dist_assets = BIN_DIR / "lan_bitable_template_portal" / "frontend" / "dist" / "assets"
    app_vue = src_dir / "App.vue"
    workbench_lite = BIN_DIR / "lan_bitable_template_portal" / "workbench_lite.py"
    backend_main = BIN_DIR / "clipflow_backend" / "main.py"
    portal_server = BIN_DIR / "lan_bitable_template_portal" / "server.py"
    errors: list[str] = []
    try:
        app_text = app_vue.read_text(encoding="utf-8", errors="ignore")
        lite_text = workbench_lite.read_text(encoding="utf-8", errors="ignore")
        backend_text = backend_main.read_text(encoding="utf-8", errors="ignore")
        portal_server_text = portal_server.read_text(encoding="utf-8", errors="ignore")
    except Exception as exc:
        return False, [f"读取轻量工作台检查文件失败: {exc}"]

    for forbidden in (
        "actionSubmitWorker",
        "workbenchLoadWorker",
        "WorkbenchDraftsPanel",
        "WorkbenchOngoingPanel",
        "loadWorkbenchInBackground",
        "submitWorkbenchActionInBackground",
        "draftStorage",
        "streamCoordinator",
    ):
        if forbidden in app_text:
            errors.append(f"App.vue 仍包含旧 Vue 工作台代码: {forbidden}")

    lite_markers = [
        "render_workbench_lite",
        "parse_pasted_notice_to_draft",
        "source-link-field",
        "_source_link_options",
        "command_format: 'notice_command'",
        "function nextBrowserTurn()",
        "applyLiteHtml",
        "navigateLite",
        "manual-menu",
        "selectors = isRow ? ['.status', '#detail-panel']",
        "SPECIALTY_OPTIONS",
        "MAINTENANCE_CYCLE_OPTIONS",
        "canonicalLiteUrlFromDocument",
        "refresh-picker",
        "lite-refresh-page",
        "lite-refresh-repair",
        "lite-refresh-change",
        "type-count",
        "panel-count",
        "captureWorkspaceScroll",
        "restoreWorkspaceScroll",
        "preserveWorkspaceScroll",
        "confirmDiscardLiteChanges",
        "has-dirty-lite-form",
        "applyOptimisticSubmission",
        "schedulePostSubmitRefresh",
        "applyJobPatch(job.frontend_patch",
        "pollSubmittedJob(jobId, label || '任务已完成，正在更新列表...', payload)",
        "pendingSubmitAction",
        "const targetRecordId = previewValue(form, 'target_record_id');",
        "_safe_draft_json_attr",
        "'upload_id', 'file_token', 'token'",
    ]
    for marker in lite_markers:
        if marker not in lite_text:
            errors.append(f"workbench_lite.py 缺少轻量工作台防卡顿/关联能力: {marker}")
    for forbidden in ("setTimeout(() => location.reload", "liteFetchThenReload", "beforeunload"):
        if forbidden in lite_text:
            errors.append(f"workbench_lite.py 普通交互仍依赖整页刷新: {forbidden}")
    for forbidden in (
        "row.setAttribute('data-draft', JSON.stringify(draft))",
        'row.setAttribute("data-draft", JSON.stringify(draft))',
    ):
        if forbidden in lite_text:
            errors.append(
                "workbench_lite.py 会把完整 draft 写入 DOM，附件/图片场景可能导致浏览器卡死"
            )
    for marker in ("setSafeDraftAttr", "safeDraftSnapshot", "compactOptimisticDraft"):
        if marker not in lite_text:
            errors.append(f"workbench_lite.py 缺少轻量 draft DOM 防卡死保护: {marker}")
    for marker in (
        'extra_images = payload.get("site_photos")',
        'data["site_photo_count"] = max(previous_count, uploaded_site_photo_count)',
    ):
        if marker not in portal_server_text:
            errors.append(f"server.py 缺少 Qt 现场照片结束保护: {marker}")

    backend_markers = [
        "@app.get(\"/workbench-lite\")",
        "@app.post(\"/workbench-lite/parse\")",
        "render_workbench_lite(",
        "parse_pasted_notice_to_draft",
        "if key != \"frontend\"",
        "headers={\"Location\": f\"/workbench-lite?",
    ]
    for marker in backend_markers:
        if marker not in backend_text:
            errors.append(f"clipflow_backend/main.py 缺少轻量工作台路由/重定向能力: {marker}")

    portal_service = BIN_DIR / "lan_bitable_template_portal" / "portal_service.py"
    try:
        portal_service_text = portal_service.read_text(encoding="utf-8", errors="ignore")
    except Exception as exc:
        errors.append(f"读取 portal_service.py 失败: {exc}")
        portal_service_text = ""
    for marker in (
        "or (source_record_id and target_record_id == source_record_id)",
        'target_record_id == str(source_record_id or "").strip()',
    ):
        if marker not in portal_service_text:
            errors.append(f"portal_service.py 缺少源表/目标表 ID 边界保护: {marker}")

    try:
        dist_js = "\n".join(
            path.read_text(encoding="utf-8", errors="ignore")
            for path in sorted(dist_assets.glob("*.js"))
            if path.is_file()
        )
    except Exception as exc:
        errors.append(f"读取 Vue dist JS 失败: {exc}")
        dist_js = ""
    if dist_js:
        for forbidden in ("actionSubmitWorker", "workbenchLoadWorker", "/api/workbench-actions", "/api/workbench?"):
            if forbidden in dist_js:
                errors.append(f"Vue dist 仍包含旧通告工作台热路径: {forbidden}")
        if "/workbench-lite" not in dist_js:
            errors.append("Vue dist 缺少轻量工作台入口 /workbench-lite")

    return not errors, errors


def check_frontend_vnet_skin() -> tuple[bool, list[str]]:
    src_dir = BIN_DIR / "lan_bitable_template_portal" / "frontend" / "src"
    app_vue = src_dir / "App.vue"
    app_topbar = src_dir / "components" / "AppTopbar.vue"
    app_status_notices = src_dir / "components" / "AppStatusNotices.vue"
    auth_panels = src_dir / "components" / "AuthPanels.vue"
    admin_tools = src_dir / "components" / "AdminTools.vue"
    admin_permission_users = src_dir / "components" / "AdminPermissionUsers.vue"
    scope_home = src_dir / "components" / "ScopeHome.vue"
    scope_home_utils = src_dir / "scopeHomeUtils.ts"
    history_memory = src_dir / "components" / "HistoryMemoryPage.vue"
    workbench_lite = BIN_DIR / "lan_bitable_template_portal" / "workbench_lite.py"
    smoke_script = BIN_DIR / "tools" / "frontend_runtime_smoke.py"
    errors: list[str] = []

    files: dict[str, str] = {}
    for label, path in (
        ("App.vue", app_vue),
        ("AppTopbar.vue", app_topbar),
        ("AppStatusNotices.vue", app_status_notices),
        ("AdminTools.vue", admin_tools),
        ("AdminPermissionUsers.vue", admin_permission_users),
        ("AuthPanels.vue", auth_panels),
        ("ScopeHome.vue", scope_home),
        ("scopeHomeUtils.ts", scope_home_utils),
        ("HistoryMemoryPage.vue", history_memory),
        ("workbench_lite.py", workbench_lite),
        ("frontend_runtime_smoke.py", smoke_script),
    ):
        try:
            files[label] = path.read_text(encoding="utf-8", errors="ignore")
        except Exception as exc:
            errors.append(f"读取 {label} 失败: {exc}")
    if errors:
        return False, errors

    app_text = files["App.vue"]
    app_markers = [
        "VNET blue-white production skin",
        "const headerSubtitle = computed",
        "const pageStatusText = computed",
        "服务暂未就绪，页面会在连接恢复后自动刷新。",
        "<AppStatusNotices",
        ":page-status-text=\"pageStatusText\"",
    ]
    for marker in app_markers:
        if marker not in app_text:
            errors.append(f"App.vue 蓝白生产皮肤缺少 {marker}")
    if "}} · {{ syncText }}" in app_text:
        errors.append("App.vue 顶栏仍直接拼接 syncText，容易暴露 HTTP 技术错误")

    topbar_text = files["AppTopbar.vue"]
    for marker in (
        "header.app-topbar",
        "linear-gradient(115deg, #064fc5",
        "header.app-topbar::after",
        "filter: brightness(0) invert(1)",
        "class=\"app-topbar\"",
    ):
        if marker not in topbar_text:
            errors.append(f"AppTopbar.vue VNET 顶栏皮肤缺少 {marker}")

    status_text = files["AppStatusNotices.vue"]
    for marker in (
        "<div v-if=\"pageStatusText\" class=\"page-status\">",
        ".status-banner.warning",
        ".status-banner.failed",
        "overflow-wrap: anywhere",
    ):
        if marker not in status_text:
            errors.append(f"AppStatusNotices.vue 状态提示组件缺少 {marker}")

    admin_text = files["AdminTools.vue"]
    for marker in (
        "VNET admin skin",
    ):
        if marker not in admin_text:
            errors.append(f"AdminTools.vue 管理员页缺少蓝白权限卡片样式: {marker}")
    admin_permission_users_text = files["AdminPermissionUsers.vue"]
    for marker in (
        ".permission-row:hover",
        ".scope-checks label:has(input:checked)",
        "grid-template-columns: minmax(140px, 0.75fr) minmax(200px, 1fr)",
    ):
        if marker not in admin_permission_users_text:
            errors.append(f"AdminPermissionUsers.vue 管理员权限用户列表缺少蓝白权限卡片样式: {marker}")

    auth_text = files["AuthPanels.vue"]
    for marker in (
        ".scope-pill",
        ".scope-pill.selected",
        "linear-gradient(135deg, #0757d7, #1678ff)",
        "VNET auth access skin",
        ".center-state:not(:has(.spinner))::before",
        ".verify-box",
    ):
        if marker not in auth_text:
            errors.append(f"AuthPanels.vue 权限申请楼栋选择未统一蓝白胶囊样式: {marker}")

    scope_text = files["ScopeHome.vue"]
    for marker in ("HomeBroadcastTicker", "module-card", "module-icon"):
        if marker not in scope_text:
            errors.append(f"ScopeHome.vue 功能选择页缺少生产入口样式/文案: {marker}")
    scope_home_utils_text = files["scopeHomeUtils.ts"]
    for marker in ("交接班审核页", "维护单管理", "进入事件管理", "进入变更管理"):
        if marker not in scope_home_utils_text:
            errors.append(f"scopeHomeUtils.ts 功能选择页缺少生产入口配置: {marker}")

    lite_text = files["workbench_lite.py"]
    for marker in (
        "background:linear-gradient(120deg,#0c57d8,#07348d)",
        ".notice-row,.ongoing-row",
        "text-overflow:ellipsis",
        ".source-link-field",
        "source-link-title",
        "target-link-panel",
        "detail-status-board",
    ):
        if marker not in lite_text:
            errors.append(f"workbench_lite.py 轻量工作台缺少蓝白/关联交互样式: {marker}")

    history_text = files["HistoryMemoryPage.vue"]
    for marker in (
        "VNET history memory skin",
        "max-height: calc(100vh - 330px)",
        ".form-grid.compact label.wide",
        "确认一键填充并保存",
    ):
        if marker not in history_text:
            errors.append(f"HistoryMemoryPage.vue 历史记忆页缺少紧凑蓝白样式/批量确认: {marker}")

    smoke_text = files["frontend_runtime_smoke.py"]
    for marker in (
        "assertHeaderSubtitle",
        "assertVnetSkin",
        "VNET skin issues",
        "功能选择 · 请先登录",
        "功能选择 · 申请访问权限",
        "permission request scope pills too few",
        "handover link href mismatch",
        "选择楼栋打开交接班审核页",
        "VNET蓝白皮肤",
        "解析到待发起通告",
        "source link options missing expected record",
        "admin permission rows missing",
        "admin permission VNET skin missing",
        "A楼轻量工作台",
        "管理工具 · 历史通告记忆导入",
    ):
        if marker not in smoke_text:
            errors.append(f"frontend_runtime_smoke.py 未覆盖生产页顶栏断言: {marker}")

    return not errors, errors


def check_legacy_static_portal_removed() -> tuple[bool, list[str]]:
    legacy_paths = [
        BIN_DIR / "lan_bitable_template_portal" / "static" / "index.html",
        BIN_DIR / "lan_bitable_template_portal" / "static" / "assets" / "vnet-logo.png",
    ]
    offenders = [
        path.relative_to(PROJECT_ROOT).as_posix()
        for path in legacy_paths
        if path.exists()
    ]
    return not offenders, offenders


def check_package_portable_distribution_rules() -> tuple[bool, list[str]]:
    package_path = PROJECT_ROOT / "package_portable.py"
    errors: list[str] = []
    if not package_path.is_file():
        return False, ["缺少 package_portable.py"]
    try:
        spec = importlib.util.spec_from_file_location("clipflow_package_portable_check", package_path)
        if spec is None or spec.loader is None:
            return False, ["无法加载 package_portable.py"]
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    except Exception as exc:
        return False, [f"导入 package_portable.py 失败: {exc}"]

    excluded_paths = [
        PROJECT_ROOT / "bin" / "data" / "lan_portal_state.sqlite3",
        PROJECT_ROOT / "bin" / "data" / "lan_portal_state.sqlite3-wal",
        PROJECT_ROOT / "bin" / "data" / "lan_portal_state.sqlite3-shm",
        PROJECT_ROOT / "bin" / "data" / "secure" / "signature_master.key",
        PROJECT_ROOT / "bin" / "data" / "signature_cache" / "rec_xxx.png",
        PROJECT_ROOT / "bin" / "lan_bitable_template_portal" / "frontend" / "node_modules" / "vue" / "index.js",
    ]
    for path in excluded_paths:
        try:
            if not module._is_excluded(path, exclude_venv=True):
                errors.append(f"打包规则未排除运行/依赖路径: {path.relative_to(PROJECT_ROOT).as_posix()}")
        except Exception as exc:
            errors.append(f"检查排除路径异常 {path}: {exc}")

    included_paths = [
        PROJECT_ROOT / "bin" / "lan_bitable_template_portal" / "frontend" / "dist" / "index.html",
        PROJECT_ROOT / "bin" / "lan_bitable_template_portal" / "frontend" / "dist" / "assets" / "index-prod.js",
        PROJECT_ROOT / "bin" / "lan_bitable_template_portal" / "frontend" / "dist" / "assets" / "index-prod.css",
    ]
    for path in included_paths:
        try:
            if module._is_excluded(path, exclude_venv=True):
                errors.append(f"打包规则错误排除了 Vue dist: {path.relative_to(PROJECT_ROOT).as_posix()}")
        except Exception as exc:
            errors.append(f"检查包含路径异常 {path}: {exc}")

    ignore = module._ignore_names(
        os.fspath(PROJECT_ROOT / "bin" / "lan_bitable_template_portal" / "frontend"),
        ["node_modules", "dist", "data"],
    )
    if "node_modules" not in ignore:
        errors.append("copytree ignore 未排除 frontend/node_modules")
    if "dist" in ignore:
        errors.append("copytree ignore 错误排除了 frontend/dist")
    if "data" in ignore:
        errors.append("copytree ignore 错误排除了 frontend/data；只应排除根 data 和 bin/data")

    runtime_suffixes = getattr(module, "RUNTIME_DATA_SUFFIXES", set())
    for suffix in (".sqlite", ".sqlite3", ".sqlite3-wal", ".sqlite3-shm"):
        if suffix not in runtime_suffixes:
            errors.append(f"package_portable.py 运行数据后缀缺少 {suffix}")
    if "node_modules" not in getattr(module, "EXCLUDE_DIR_NAMES", set()):
        errors.append("package_portable.py EXCLUDE_DIR_NAMES 缺少 node_modules")
    return not errors, errors


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
    workflow_path = BIN_DIR / "upload_event_module" / "ui" / "main_window_workflow.py"
    ui_text = ui_path.read_text(encoding="utf-8", errors="ignore")
    records_text = records_path.read_text(encoding="utf-8", errors="ignore")
    workflow_text = workflow_path.read_text(encoding="utf-8", errors="ignore")
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
    if "self.deleted_history_list = QListWidget()" in ui_text:
        return False, "Qt 历史删除列表仍在使用 QListWidget。"
    if "DeletedNoticeModel" not in ui_text or "DeletedNoticeDelegate" not in ui_text:
        return False, "Qt 历史删除列表未接入 model/delegate。"
    if "ClipboardItemWidget" in records_text or "setItemWidget(" in records_text:
        return False, "Qt 活动列表主代码仍保留 QWidget 卡片挂载热路径。"
    if "QListWidget" in workflow_text or "QListWidgetItem" in workflow_text:
        return False, "Qt 目标记录候选弹窗仍在使用 QListWidget。"
    return True, "Qt 活动列表固定走 model/delegate。"


def check_qt_ui_no_direct_remote_business() -> tuple[bool, list[str]]:
    ui_dir = BIN_DIR / "upload_event_module" / "ui"
    forbidden_patterns = (
        "create_bitable_record",
        "update_bitable_record",
        "delete_bitable_record",
        "upload_media_to_feishu",
        "send_robot_title_and_content",
        "send_personal_message",
        "send_group_robot_message",
    )
    offenders: list[str] = []
    for path in sorted(ui_dir.rglob("*.py")):
        rel = path.relative_to(PROJECT_ROOT).as_posix()
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        for pattern in forbidden_patterns:
            if pattern in text:
                offenders.append(f"{rel}: {pattern}")
    return not offenders, offenders


def check_qt_active_cache_no_runtime_legacy_document_writes() -> tuple[bool, list[str]]:
    path = BIN_DIR / "upload_event_module" / "ui" / "active_cache_store.py"
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except Exception as exc:
        return False, [f"读取失败: {exc}"]
    forbidden = (
        "_write_legacy_document",
        "_legacy_document_signature",
        "_payload_from_qt_items_unlocked",
        "_last_legacy_document_signature",
        "_sync_legacy_document_from_qt_items_unlocked",
    )
    offenders = [pattern for pattern in forbidden if pattern in text]
    marker = "    def _save_payload_unlocked(self, payload: dict) -> bool:"
    start = text.find(marker)
    if start < 0:
        offenders.append(f"缺少 {marker.strip()}")
    else:
        next_def = text.find("\n    def ", start + len(marker))
        body = text[start:] if next_def < 0 else text[start:next_def]
        if "replace_qt_active_items_from_payload(" not in body:
            offenders.append(f"{marker.strip()} 未写入 SQLite qt_active_items 规范表")
        if "put_document(" in body or "_put_table_document" in body:
            offenders.append(f"{marker.strip()} 仍写回 json_documents")
    return not offenders, offenders


def check_qt_active_cache_full_replace_disabled_by_default() -> tuple[bool, list[str]]:
    path = BIN_DIR / "upload_event_module" / "ui" / "main_window_cache.py"
    offenders: list[str] = []
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except Exception as exc:
        return False, [f"读取 main_window_cache.py 失败: {exc}"]
    if "CLIPFLOW_ACTIVE_CACHE_FULL_REPLACE_ENABLED" not in text:
        offenders.append("缺少 active cache 全量 replace 显式开关")
    if "self._active_cache_full_replace_enabled = str(" not in text:
        offenders.append("active cache 全量 replace 未默认由 env flag 控制")
    init_marker = "    def _init_active_cache_timer(self):"
    init_start = text.find(init_marker)
    if init_start < 0:
        offenders.append("缺少 _init_active_cache_timer")
    else:
        next_def = text.find("\n    def ", init_start + len(init_marker))
        body = text[init_start:] if next_def < 0 else text[init_start:next_def]
        if "self.active_cache_timer.start(30000)" in body and "if bool(getattr(self, \"_active_cache_full_replace_enabled\", False))" not in body:
            offenders.append("active cache 周期全量扫描未受开关保护")
    schedule_marker = "    def schedule_active_cache_save(self, delay_ms: int = 800):"
    schedule_start = text.find(schedule_marker)
    if schedule_start < 0:
        offenders.append("缺少 schedule_active_cache_save")
    else:
        next_def = text.find("\n    def ", schedule_start + len(schedule_marker))
        body = text[schedule_start:] if next_def < 0 else text[schedule_start:next_def]
        flag_pos = body.find("_active_cache_full_replace_enabled")
        timer_pos = body.find("timer.start")
        singleshot_pos = body.find("QTimer.singleShot")
        if flag_pos < 0:
            offenders.append("普通延迟保存入口缺少 active cache 全量 replace 开关")
        if timer_pos >= 0 and flag_pos > timer_pos:
            offenders.append("普通延迟保存入口在检查开关前启动 timer")
        if singleshot_pos >= 0 and flag_pos > singleshot_pos:
            offenders.append("普通延迟保存入口在检查开关前启动 singleShot")
    return not offenders, offenders


def check_qt_runtime_uses_deferred_active_cache_save() -> tuple[bool, list[str]]:
    ui_dir = BIN_DIR / "upload_event_module" / "ui"
    allowed_files = {
        "main_window.py",  # startup cache-id repair runs before the delayed timer exists
        "main_window_cache.py",  # owns save_active_cache implementation and timer flushes
    }
    offenders: list[str] = []
    for path in sorted(ui_dir.rglob("main_window*.py")):
        if path.name in allowed_files:
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        for match in re.finditer(r"self\.save_active_cache\(\)", text):
            line = text.count("\n", 0, match.start()) + 1
            offenders.append(f"{path.relative_to(PROJECT_ROOT).as_posix()}:{line}")
    return not offenders, offenders


def check_qt_remote_validation_default_noop_before_queue() -> tuple[bool, list[str]]:
    path = BIN_DIR / "upload_event_module" / "ui" / "main_window_records.py"
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except Exception as exc:
        return False, [f"读取 main_window_records.py 失败: {exc}"]
    offenders: list[str] = []
    checks = {
        "record_binding": (
            "    def _schedule_record_binding_validation(self, data_dict: dict | None):",
            "self._record_binding_validation_queue.put_nowait",
        ),
        "today_in_progress": (
            "    def _schedule_today_in_progress_sync(self, data_dict: dict | None):",
            "self._today_in_progress_sync_queue.put_nowait",
        ),
    }
    for label, (marker, queue_marker) in checks.items():
        start = text.find(marker)
        if start < 0:
            offenders.append(f"缺少 {marker.strip()}")
            continue
        next_def = text.find("\n    def ", start + len(marker))
        body = text[start:] if next_def < 0 else text[start:next_def]
        env_pos = body.find("CLIPFLOW_QT_REMOTE_VALIDATION")
        queue_pos = body.find(queue_marker)
        if env_pos < 0:
            offenders.append(f"{label} 调度入口缺少 CLIPFLOW_QT_REMOTE_VALIDATION 开关")
        if queue_pos >= 0 and env_pos > queue_pos:
            offenders.append(f"{label} 远端同步开关晚于入队，默认关闭时仍会产生队列开销")
    startup_checks = {
        "record_binding_startup": "    def _validate_record_bindings_on_startup(self):",
        "today_in_progress_startup": "    def _schedule_today_in_progress_sync_on_startup(self):",
    }
    for label, marker in startup_checks.items():
        start = text.find(marker)
        if start < 0:
            offenders.append(f"缺少 {marker.strip()}")
            continue
        next_def = text.find("\n    def ", start + len(marker))
        body = text[start:] if next_def < 0 else text[start:next_def]
        env_pos = body.find("CLIPFLOW_QT_REMOTE_VALIDATION")
        snapshot_pos = body.find("_active_data_snapshot_for_startup_sync")
        timer_pos = body.find("QTimer.singleShot")
        if env_pos < 0:
            offenders.append(f"{label} 启动入口缺少 CLIPFLOW_QT_REMOTE_VALIDATION 开关")
        if snapshot_pos >= 0 and env_pos > snapshot_pos:
            offenders.append(f"{label} 远端同步开关晚于启动快照扫描")
        if timer_pos >= 0 and env_pos > timer_pos:
            offenders.append(f"{label} 远端同步开关晚于启动 QTimer 调度")
    return not offenders, offenders


def check_clipboard_file_polling_is_backend_aware() -> tuple[bool, list[str]]:
    path = BIN_DIR / "upload_event_module" / "ui" / "main_window_clipboard.py"
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except Exception as exc:
        return False, [f"读取 main_window_clipboard.py 失败: {exc}"]
    offenders: list[str] = []
    if "def _clipboard_backend_direct_event_available" not in text:
        offenders.append("缺少 _clipboard_backend_direct_event_available")
    if "def _clipboard_file_poll_interval_ms" not in text:
        offenders.append("缺少 _clipboard_file_poll_interval_ms")
    if "self._clipboard_file_poll_interval_ms()" not in text:
        offenders.append("剪贴板文件 timer 未使用动态轮询间隔")
    if "def _should_replay_local_clipboard_entries_on_startup" not in text:
        offenders.append("缺少 _should_replay_local_clipboard_entries_on_startup")
    marker = "    def _clipboard_file_poll_interval_ms(self) -> int:"
    start = text.find(marker)
    if start >= 0:
        next_def = text.find("\n    def ", start + len(marker))
        body = text[start:] if next_def < 0 else text[start:next_def]
        if "_clipboard_backend_direct_event_available()" not in body:
            offenders.append("剪贴板文件轮询间隔未根据后端直连能力降频")
        if "5000" not in body:
            offenders.append("剪贴板后端直连可用时未默认降到 5 秒兜底轮询")
        if "CLIPFLOW_CLIPBOARD_POLL_MS" not in body:
            offenders.append("剪贴板轮询间隔缺少环境变量覆盖")
    replay_marker = "    def _init_clipboard_ipc(self):"
    replay_start = text.find(replay_marker)
    if replay_start >= 0:
        next_def = text.find("\n    def ", replay_start + len(replay_marker))
        body = text[replay_start:] if next_def < 0 else text[replay_start:next_def]
        replay_call = "QTimer.singleShot(0, self._replay_pending_clipboard_entries)"
        replay_pos = body.find(replay_call)
        guard_pos = body.rfind(
            "if self._should_replay_local_clipboard_entries_on_startup():",
            0,
            replay_pos if replay_pos >= 0 else 0,
        )
        if replay_pos >= 0 and guard_pos < 0:
            offenders.append("启动时本地剪贴板 pending replay 未受后端直连判断保护")
    else:
        offenders.append("缺少 _init_clipboard_ipc")
    return not offenders, offenders


def check_runtime_queues_use_sqlite_runtime_queue() -> tuple[bool, list[str]]:
    forbidden = (
        "message_queue.append(",
        "message_queue.pop(",
        "cls.message_queue = remaining",
        "for queued_job_id in cls.message_queue",
        "enumerate(cls.message_queue)",
        "message_queue: list",
        "PortalRuntime.message_queue =",
        "cls.message_queue =",
        "action_queue.append(",
        "action_queue.pop(",
        "cls.action_queue = remaining",
        "for queued_job_id in cls.action_queue",
        "enumerate(cls.action_queue)",
        "action_queue: list",
        "PortalRuntime.action_queue =",
        "cls.action_queue =",
        "upload_wait_jobs",
    )
    paths = (
        BIN_DIR / "lan_bitable_template_portal" / "server.py",
        BIN_DIR / "clipflow_backend" / "main.py",
    )
    offenders: list[str] = []
    for path in paths:
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except Exception as exc:
            offenders.append(f"{path.relative_to(PROJECT_ROOT).as_posix()}: 读取失败: {exc}")
            continue
        rel = path.relative_to(PROJECT_ROOT).as_posix()
        offenders.extend(f"{rel}: {pattern}" for pattern in forbidden if pattern in text)
    return not offenders, offenders


def check_qt_upload_path_has_no_local_queue_dispatch() -> tuple[bool, list[str]]:
    path = BIN_DIR / "upload_event_module" / "ui" / "main_window_workflow.py"
    main_path = BIN_DIR / "upload_event_module" / "ui" / "main_window.py"
    ui_path = BIN_DIR / "upload_event_module" / "ui" / "main_window_ui.py"
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
        lines = text.splitlines()
    except Exception as exc:
        return False, [f"读取失败: {exc}"]
    try:
        main_text = main_path.read_text(encoding="utf-8", errors="ignore")
        ui_text = ui_path.read_text(encoding="utf-8", errors="ignore")
    except Exception as exc:
        return False, [f"读取 Qt 上传线程池相关文件失败: {exc}"]
    forbidden_calls = ("_enqueue_upload(", "_mark_upload_queued(")
    offenders: list[str] = []
    for index, line in enumerate(lines, start=1):
        stripped = line.strip()
        if stripped.startswith("def "):
            continue
        for pattern in forbidden_calls:
            if pattern in stripped:
                offenders.append(f"bin/upload_event_module/ui/main_window_workflow.py:{index}: {pattern}")
    forbidden_state = ("_upload_queues", "_upload_workers", "_upload_worker_semaphore", "_upload_lock")
    ui_files = [
        BIN_DIR / "upload_event_module" / "ui" / "main_window.py",
        BIN_DIR / "upload_event_module" / "ui" / "main_window_workflow.py",
        BIN_DIR / "upload_event_module" / "ui" / "main_window_records.py",
        BIN_DIR / "upload_event_module" / "ui" / "main_window_runtime.py",
    ]
    for ui_file in ui_files:
        try:
            ui_file_text = ui_file.read_text(encoding="utf-8", errors="ignore")
        except Exception as exc:
            offenders.append(f"{ui_file.relative_to(PROJECT_ROOT).as_posix()}: 读取失败: {exc}")
            continue
        rel = ui_file.relative_to(PROJECT_ROOT).as_posix()
        for pattern in forbidden_state:
            if pattern in ui_file_text:
                offenders.append(f"{rel}: {pattern}")
    marker = "    def _has_pending_upload(self, record_id):"
    start = text.find(marker)
    if start < 0:
        offenders.append("bin/upload_event_module/ui/main_window_workflow.py: 缺少 _has_pending_upload")
    else:
        next_def = text.find("\n    def ", start + len(marker))
        body = text[start:] if next_def < 0 else text[start:next_def]
        if "_upload_queues" in body or "_upload_workers" in body:
            offenders.append(
                "bin/upload_event_module/ui/main_window_workflow.py: _has_pending_upload 仍依赖 Qt 本地上传队列"
            )
    dispatch_marker = "    def _dispatch_backend_notice_upload(self, record_id: str, task) -> None:"
    dispatch_start = text.find(dispatch_marker)
    if dispatch_start < 0:
        offenders.append("bin/upload_event_module/ui/main_window_workflow.py: 缺少 _dispatch_backend_notice_upload")
    else:
        next_def = text.find("\n    def ", dispatch_start + len(dispatch_marker))
        body = text[dispatch_start:] if next_def < 0 else text[dispatch_start:next_def]
        if "threading.Thread(" in body:
            offenders.append("_dispatch_backend_notice_upload 仍直接创建线程")
        if "_qt_backend_command_executor" not in body or ".submit(" not in body:
            offenders.append("_dispatch_backend_notice_upload 未使用受控 Qt 后端命令线程池")
    if "ThreadPoolExecutor" not in main_text or "_qt_backend_command_executor" not in main_text:
        offenders.append("main_window.py 缺少 Qt 后端命令线程池")
    if "_shutdown_qt_backend_command_executor" not in ui_text:
        offenders.append("main_window_ui.py 退出流程未关闭 Qt 后端命令线程池")
    return not offenders, offenders


def check_qt_upload_transient_state_not_persisted() -> tuple[bool, list[str]]:
    workflow_path = BIN_DIR / "upload_event_module" / "ui" / "main_window_workflow.py"
    records_path = BIN_DIR / "upload_event_module" / "ui" / "main_window_records.py"
    offenders: list[str] = []
    try:
        workflow_text = workflow_path.read_text(encoding="utf-8", errors="ignore")
        records_text = records_path.read_text(encoding="utf-8", errors="ignore")
    except Exception as exc:
        return False, [f"读取 Qt 上传热路径文件失败: {exc}"]
    if "def _update_active_item_data(self, record_id, data_dict, *, persist_cache=True)" not in records_text:
        offenders.append("_update_active_item_data 缺少 persist_cache 开关")
    marker = "    def handle_action(self, data_dict, action_type):"
    start = workflow_text.find(marker)
    if start < 0:
        offenders.append("main_window_workflow.py 缺少 handle_action")
    else:
        next_def = workflow_text.find("\n    def ", start + len(marker))
        body = workflow_text[start:] if next_def < 0 else workflow_text[start:next_def]
        show_pos = body.find("self._show_screenshot_dialog")
        pre_dialog = body if show_pos < 0 else body[:show_pos]
        for forbidden in ("_upsert_active_cache_record", "request_active_cache_save"):
            if forbidden in pre_dialog:
                offenders.append(f"handle_action 上传中临时状态仍持久化: {forbidden}")
    marker = "    def do_feishu_upload("
    start = workflow_text.find(marker)
    if start < 0:
        offenders.append("main_window_workflow.py 缺少 do_feishu_upload")
    else:
        next_def = workflow_text.find("\n    def ", start + len(marker))
        body = workflow_text[start:] if next_def < 0 else workflow_text[start:next_def]
        if "persist_cache=False" not in body:
            offenders.append("do_feishu_upload 更新上传中 UI 状态时未使用 persist_cache=False")
        snapshot_pos = body.find("data_snapshot =")
        pre_submit = body if snapshot_pos < 0 else body[:snapshot_pos]
        if "request_active_cache_save()" in pre_submit:
            offenders.append("do_feishu_upload 提交后端前仍直接保存 active cache")
    return not offenders, offenders


def check_qt_normal_history_removed() -> tuple[bool, list[str]]:
    forbidden = (
        "save_to_history_file",
        "load_all_history",
        "_save_history_payload",
        "_load_history_payload",
        "_delete_history_payload",
        "_history_override_records",
        "HISTORY_FILE",
        "HISTORY_STATE_NAMESPACE",
        "qt_notice_history",
    )
    ui_files = [
        BIN_DIR / "upload_event_module" / "ui" / "main_window.py",
        BIN_DIR / "upload_event_module" / "ui" / "main_window_cache.py",
        BIN_DIR / "upload_event_module" / "ui" / "main_window_records.py",
        BIN_DIR / "upload_event_module" / "ui" / "main_window_runtime.py",
        BIN_DIR / "upload_event_module" / "ui" / "main_window_ui.py",
        BIN_DIR / "upload_event_module" / "ui" / "main_window_workflow.py",
    ]
    offenders: list[str] = []
    for path in ui_files:
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except Exception as exc:
            offenders.append(f"{path.relative_to(PROJECT_ROOT).as_posix()}: 读取失败: {exc}")
            continue
        rel = path.relative_to(PROJECT_ROOT).as_posix()
        for pattern in forbidden:
            if pattern in text:
                offenders.append(f"{rel}: {pattern}")
    return not offenders, offenders


def check_qt_active_delta_flush_on_stop() -> tuple[bool, list[str]]:
    path = BIN_DIR / "clipflow_backend" / "process_controller.py"
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except Exception as exc:
        return False, [f"读取失败: {exc}"]
    offenders: list[str] = []
    stop_marker = "    def stop(self) -> None:"
    start = text.find(stop_marker)
    if start < 0:
        return False, ["process_controller.py 缺少 stop()"]
    next_def = text.find("\n    def ", start + len(stop_marker))
    body = text[start:] if next_def < 0 else text[start:next_def]
    if "_flush_active_delta_once(" not in body:
        offenders.append("stop() 未在退出前 flush Qt active item 增量")
    if '"_active_delta_thread"' not in body:
        offenders.append("stop() 未等待 active delta 线程结束")
    if "def _flush_active_delta_once" not in text:
        offenders.append("缺少 _flush_active_delta_once")
    return not offenders, offenders


def check_qt_legacy_business_callbacks_disabled() -> tuple[bool, list[str]]:
    path = BIN_DIR / "clipflow_backend" / "process_controller.py"
    offenders: list[str] = []
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except Exception as exc:
        return False, [f"读取 process_controller.py 失败: {exc}"]
    if "CLIPFLOW_ENABLE_QT_LEGACY_PORTAL_CALLBACKS" not in text:
        offenders.append("缺少旧 Qt 业务回调显式开关")
    if "self._legacy_portal_callbacks_enabled = _env_flag(" not in text:
        offenders.append("旧 Qt 业务回调未默认由 env flag 控制")
    for setter in (
        "def set_maintenance_action_callback(self, callback) -> None:",
        "def set_ongoing_delete_callback(self, callback) -> None:",
    ):
        start = text.find(setter)
        if start < 0:
            offenders.append(f"缺少 {setter}")
            continue
        next_def = text.find("\n    def ", start + len(setter))
        body = text[start:] if next_def < 0 else text[start:next_def]
        if "_legacy_portal_callbacks_enabled" not in body:
            offenders.append(f"{setter} 未受旧回调开关保护")
    if '"CLIPFLOW_QT_SNAPSHOT_INTERVAL_SECONDS",\n            300' not in text:
        offenders.append("Qt ongoing 全量快照默认间隔必须至少 300 秒")
    if "minimum=30" not in text or "maximum=3600" not in text:
        offenders.append("Qt ongoing 全量快照环境变量边界不合理")
    return not offenders, offenders


def check_backend_background_threads_are_bounded() -> tuple[bool, list[str]]:
    path = BIN_DIR / "clipflow_backend" / "main.py"
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except Exception as exc:
        return False, [f"读取 clipflow_backend/main.py 失败: {exc}"]
    offenders: list[str] = []
    if "ThreadPoolExecutor" not in text:
        offenders.append("FastAPI 后端缺少受控 ThreadPoolExecutor")
    if "def _submit_background" not in text:
        offenders.append("FastAPI 后端缺少 _submit_background")
    if "def _submit_notice_undo_job" not in text:
        offenders.append("FastAPI 后端缺少 _submit_notice_undo_job")
    allowed_thread_names = {
        "ClipFlowBackendShutdown",
        "ClipFlowFastAPIBackend",
    }
    for match in re.finditer(r"threading\.Thread\((?P<body>.*?)\)\.start\(\)", text, re.S):
        body = match.group("body")
        if not any(name in body for name in allowed_thread_names):
            line = text.count("\n", 0, match.start()) + 1
            offenders.append(f"clipflow_backend/main.py:{line}: 请求路径直接创建线程")
    if "_background_executor.shutdown(" not in text:
        offenders.append("FastAPI 后端 stop() 未关闭后台 executor")
    return not offenders, offenders


def check_qt_active_items_sse_uses_lightweight_meta_gate() -> tuple[bool, list[str]]:
    main_path = BIN_DIR / "clipflow_backend" / "main.py"
    store_path = BIN_DIR / "lan_bitable_template_portal" / "state_store.py"
    offenders: list[str] = []
    try:
        main_text = main_path.read_text(encoding="utf-8", errors="ignore")
        store_text = store_path.read_text(encoding="utf-8", errors="ignore")
    except Exception as exc:
        return False, [f"读取 active-items SSE 检查文件失败: {exc}"]
    for marker in (
        "def get_ongoing_snapshot_meta",
        "def active_source_snapshot_meta",
        "def qt_active_items_meta",
    ):
        if marker not in store_text:
            offenders.append(f"state_store.py 缺少 {marker}")
    marker = "async def _qt_active_items_stream"
    start = main_text.find(marker)
    if start < 0:
        offenders.append("clipflow_backend/main.py 缺少 _qt_active_items_stream")
        return False, offenders
    next_def = main_text.find("\n    async def ", start + len(marker))
    if next_def < 0:
        next_def = main_text.find("\n    def ", start + len(marker))
    body = main_text[start:] if next_def < 0 else main_text[start:next_def]
    required = (
        "get_ongoing_snapshot_meta()",
        "active_source_snapshot_meta()",
        "qt_active_items_meta()",
        "last_version_signature",
        "version_signature == last_version_signature",
    )
    for item in required:
        if item not in body:
            offenders.append(f"_qt_active_items_stream 缺少轻量版本门禁: {item}")
    meta_pos = body.find("get_ongoing_snapshot_meta()")
    full_pos = body.find("get_ongoing_snapshot()")
    if meta_pos < 0 or full_pos < 0 or meta_pos > full_pos:
        offenders.append("_qt_active_items_stream 必须先读轻量 meta，再按需读完整 ongoing snapshot")
    if "source_snapshot_stats()" in body:
        offenders.append("_qt_active_items_stream 空闲路径仍调用 source_snapshot_stats() 重查询")
    return not offenders, offenders


def check_system_alert_worker_can_shutdown() -> tuple[bool, list[str]]:
    service_path = BIN_DIR / "upload_event_module" / "services" / "system_alert_webhook.py"
    ui_path = BIN_DIR / "upload_event_module" / "ui" / "main_window_ui.py"
    offenders: list[str] = []
    try:
        service_text = service_path.read_text(encoding="utf-8", errors="ignore")
    except Exception as exc:
        return False, [f"{service_path.relative_to(PROJECT_ROOT).as_posix()}: 读取失败: {exc}"]
    try:
        ui_text = ui_path.read_text(encoding="utf-8", errors="ignore")
    except Exception as exc:
        return False, [f"{ui_path.relative_to(PROJECT_ROOT).as_posix()}: 读取失败: {exc}"]
    if "def shutdown_system_alert_worker" not in service_text:
        offenders.append("system_alert_webhook.py 缺少 shutdown_system_alert_worker")
    if "_STOP_SENTINEL" not in service_text:
        offenders.append("system_alert_webhook.py 缺少停止 sentinel")
    if "name=\"ClipFlowSystemAlertWorker\"" not in service_text:
        offenders.append("system_alert_webhook.py 告警线程未命名")
    if "shutdown_system_alert_worker" not in ui_text:
        offenders.append("main_window_ui.py 退出流程未停止系统告警 worker")
    append_marker = "def _append_local_record(record: dict[str, Any]) -> None:"
    append_start = service_text.find(append_marker)
    if append_start < 0:
        offenders.append("system_alert_webhook.py 缺少 _append_local_record")
    else:
        next_def = service_text.find("\ndef ", append_start + len(append_marker))
        body = service_text[append_start:] if next_def < 0 else service_text[append_start:next_def]
        if "_get_state_store()" not in body:
            offenders.append("system_alert_webhook.py 未复用 StateStore")
        if "append_event_async" not in body:
            offenders.append("system_alert_webhook.py 告警记录未优先异步写入")
    return not offenders, offenders


def check_event_relay_stop_clears_thread_state() -> tuple[bool, list[str]]:
    path = BIN_DIR / "upload_event_module" / "services" / "event_relay_server.py"
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except Exception as exc:
        return False, [f"{path.relative_to(PROJECT_ROOT).as_posix()}: 读取失败: {exc}"]
    marker = "    def stop(self):"
    start = text.find(marker)
    if start < 0:
        return False, ["event_relay_server.py 缺少 stop()"]
    next_def = text.find("\n    def ", start + len(marker))
    body = text[start:] if next_def < 0 else text[start:next_def]
    offenders: list[str] = []
    if "server.force_exit = True" not in body:
        offenders.append("EventRelay stop() 未设置 force_exit")
    if "thread.join(timeout=1)" not in body:
        offenders.append("EventRelay stop() force_exit 后未二次等待")
    if "self._thread = None" not in body or "self._server = None" not in body:
        offenders.append("EventRelay stop() 未清理 _thread/_server")
    if "shutdown_write_worker" not in body:
        offenders.append("EventRelay stop() 未停止 StateStore 写入 worker")
    append_marker = "    def _append_event_log(self, event: dict) -> None:"
    append_start = text.find(append_marker)
    if append_start < 0:
        offenders.append("EventRelay 缺少 _append_event_log")
    else:
        next_def = text.find("\n    def ", append_start + len(append_marker))
        append_body = text[append_start:] if next_def < 0 else text[append_start:next_def]
        if "self._get_state_store()" not in append_body:
            offenders.append("EventRelay 未复用 StateStore")
        if "append_event_async" not in append_body:
            offenders.append("EventRelay 事件记录未优先异步写入")
    return not offenders, offenders


def check_notice_type_mappings() -> tuple[bool, list[str]]:
    expected = {
        "maintenance": "维保通告",
        "change": "变更通告",
        "repair": "设备检修",
        "power": "上电通告",
        "polling": "设备轮巡",
        "adjust": "设备调整",
    }
    expected_fields = {
        "maintenance": (
            ("名称", "title"),
            ("时间", "time_range"),
            ("位置", "location"),
            ("内容", "content"),
            ("原因", "reason"),
            ("影响", "impact"),
            ("进度", "progress"),
        ),
        "change": (
            ("名称", "title"),
            ("等级", "level"),
            ("时间", "time_range"),
            ("位置", "location"),
            ("内容", "content"),
            ("原因", "reason"),
            ("影响", "impact"),
            ("进度", "progress"),
        ),
        "repair": (
            ("标题", "title"),
            ("地点", "location"),
            ("紧急程度", "level"),
            ("专业", "specialty"),
            ("发现故障时间", "fault_time"),
            ("期望完成时间", "expected_time"),
            ("维修设备", "repair_device"),
            ("维修故障", "repair_fault"),
            ("故障类型", "fault_type"),
            ("维修方式", "repair_mode"),
            ("影响范围", "impact"),
            ("故障发现方式", "discovery"),
            ("故障现象", "symptom"),
            ("故障原因", "reason"),
            ("解决方案", "solution"),
            ("备件更换情况", "spare_parts"),
            ("完成情况", "progress"),
        ),
        "power": (
            ("名称", "title"),
            ("时间", "time_range"),
            ("柜号", "cabinet"),
            ("数量", "quantity"),
            ("进度", "progress"),
        ),
        "polling": (
            ("标题", "title"),
            ("时间", "time_range"),
            ("设备", "device"),
            ("内容", "content"),
            ("影响", "impact"),
            ("进度", "progress"),
        ),
        "adjust": (
            ("名称", "title"),
            ("时间", "time_range"),
            ("位置", "location"),
            ("内容", "content"),
            ("原因", "reason"),
            ("影响", "impact"),
            ("进度", "progress"),
        ),
    }
    expected_headings = dict(expected)
    expected_headings["power"] = "上电通告"
    errors: list[str] = []
    for work_type, notice_type in expected.items():
        actual_notice_type = NOTICE_TYPE_BY_WORK_TYPE.get(work_type)
        if actual_notice_type != notice_type:
            errors.append(
                f"{work_type} -> {actual_notice_type!r}，期望 {notice_type!r}"
            )
        actual_work_type = WORK_TYPE_BY_NOTICE_TYPE.get(notice_type)
        if actual_work_type != work_type:
            errors.append(
                f"{notice_type} -> {actual_work_type!r}，期望 {work_type!r}"
            )
        service_notice_type = MaintenancePortalService._notice_type_for_work_type(work_type)
        if service_notice_type != notice_type:
            errors.append(
                f"_notice_type_for_work_type({work_type!r})={service_notice_type!r}"
            )
        service_work_type = MaintenancePortalService._work_type_for_notice_type(notice_type)
        if service_work_type != work_type:
            errors.append(
                f"_work_type_for_notice_type({notice_type!r})={service_work_type!r}"
            )
        if work_type not in NOTICE_TEXT_TEMPLATES:
            errors.append(f"NOTICE_TEXT_TEMPLATES 缺少 {work_type}")
            continue
        template = NOTICE_TEXT_TEMPLATES[work_type]
        actual_heading = str(template.get("heading") or "")
        if actual_heading != expected_headings[work_type]:
            errors.append(
                f"{work_type} 模板标题 {actual_heading!r}，期望 {expected_headings[work_type]!r}"
            )
        actual_fields = tuple(template.get("fields") or ())
        if actual_fields != expected_fields[work_type]:
            errors.append(f"{work_type} 模板字段顺序不符合六类通告合同")
    lite_path = BIN_DIR / "lan_bitable_template_portal" / "workbench_lite.py"
    try:
        lite_text = lite_path.read_text(encoding="utf-8", errors="ignore")
    except Exception as exc:
        errors.append(f"读取 workbench_lite.py 失败: {exc}")
        return not errors, errors
    for marker in (
        "def _pasted_work_type",
        "if \"变更通告\" in source:",
        "return \"change\"",
        "parse_pasted_notice_to_draft",
        "parsed_action",
    ):
        if marker not in lite_text:
            errors.append(f"轻量工作台解析粘贴链路缺少 {marker}")
    for label in ("故障类型", "维修方式", "故障发现方式", "故障现象", "备件更换情况"):
        if label not in lite_text:
            errors.append(f"轻量工作台表单缺少检修中文字段 {label}")
    for forbidden in ("【设备变更】", "设备变更"):
        if forbidden in lite_text:
            errors.append(f"轻量工作台仍包含旧变更文案 {forbidden}")
    return not errors, errors


def check_target_candidate_match_reason() -> tuple[bool, list[str]]:
    service_path = BIN_DIR / "lan_bitable_template_portal" / "portal_service.py"
    qt_path = BIN_DIR / "upload_event_module" / "ui" / "main_window_workflow.py"
    errors: list[str] = []
    try:
        service_text = service_path.read_text(encoding="utf-8", errors="ignore")
    except Exception as exc:
        return False, [f"读取 portal_service.py 失败: {exc}"]
    try:
        qt_text = qt_path.read_text(encoding="utf-8", errors="ignore")
    except Exception as exc:
        return False, [f"读取 main_window_workflow.py 失败: {exc}"]
    if "def _target_candidate_match_reason" not in service_text:
        errors.append("portal_service.py 缺少 _target_candidate_match_reason")
    if service_text.count('"match_reason": match_reason') < 2:
        errors.append("目标候选返回结构未同时覆盖变更和通用通告 match_reason")
    for marker in ('"returned_count"', '"total_matched"', '"limited"'):
        if marker not in service_text:
            errors.append(f"目标候选返回结构缺少 {marker}")
    if "candidate.get(\"match_reason\")" not in qt_text:
        errors.append("Qt 目标记录选择弹窗未展示 match_reason")
    return not errors, errors


def check_notice_site_photo_payload_not_reused() -> tuple[bool, list[str]]:
    service_path = BIN_DIR / "lan_bitable_template_portal" / "portal_service.py"
    qt_path = BIN_DIR / "upload_event_module" / "ui" / "main_window_workflow.py"
    errors: list[str] = []
    try:
        service_text = service_path.read_text(encoding="utf-8", errors="ignore")
    except Exception as exc:
        return False, [f"读取 portal_service.py 失败: {exc}"]
    try:
        qt_text = qt_path.read_text(encoding="utf-8", errors="ignore")
    except Exception as exc:
        return False, [f"读取 main_window_workflow.py 失败: {exc}"]

    required_service_markers = (
        "explicit_site_photos = self._has_site_photo_payload(payload) or self._has_site_photo_payload(patch)",
        'for stale_image_key in ("extra_images", "site_photos", "process_site_images")',
        "expanded.pop(stale_image_key, None)",
    )
    for marker in required_service_markers:
        if marker not in service_text:
            errors.append(f"后端 notice command 缺少旧现场照片清理保护: {marker}")

    forbidden_qt_markers = (
        'if not payload.get("extra_images") and existing.get("extra_images")',
        'merged["extra_images"] = existing.get("extra_images")',
    )
    for marker in forbidden_qt_markers:
        if marker in qt_text:
            errors.append(f"Qt 更新队列仍会继承旧现场照片: {marker}")
    return not errors, errors


def check_fastapi_backend_entry() -> tuple[bool, str]:
    main_path = BIN_DIR / "refactored_main.py"
    backend_main_path = BIN_DIR / "clipflow_backend" / "main.py"
    runtime_path = BIN_DIR / "upload_event_module" / "ui" / "main_window_runtime.py"
    server_path = BIN_DIR / "lan_bitable_template_portal" / "server.py"
    main_text = main_path.read_text(encoding="utf-8", errors="ignore")
    backend_main_text = backend_main_path.read_text(encoding="utf-8", errors="ignore")
    runtime_text = runtime_path.read_text(encoding="utf-8", errors="ignore")
    server_text = server_path.read_text(encoding="utf-8", errors="ignore")
    if "BackendProcessPortalController as PortalServerController" not in main_text:
        return False, "主入口没有固定使用 FastAPI 后端子进程控制器。"
    legacy_runtime_name = "Portal" + "Handler"
    if "PortalRuntime" not in backend_main_text or legacy_runtime_name in backend_main_text:
        return False, "FastAPI 后端主模块仍未切到 PortalRuntime 运行态命名。"
    if "class PortalRuntime" not in server_text:
        return False, "门户运行态未暴露 PortalRuntime。"
    if legacy_runtime_name in server_text:
        return False, "门户服务模块仍暴露旧运行态兼容别名。"
    if "LanTemplatePortalServer(" in main_text or "LanTemplatePortalServer(" in runtime_text:
        return False, "生产入口仍可能直接启动旧 ThreadingHTTPServer 门户。"
    if "from http.server import" in server_text or "ThreadingHTTPServer(" in server_text:
        return False, "旧 ThreadingHTTPServer 门户实现仍未物理移除。"
    try:
        from clipflow_backend.main import FastAPIPortalController

        app = FastAPIPortalController(host="127.0.0.1", port=18766)._build_app()
        route_paths = {getattr(route, "path", "") for route in app.routes}
        if "/api/engineer/mop/upload-local" not in route_paths:
            return False, "FastAPI 后端缺少 MOP 本地上传路由。"
    except Exception as exc:
        return False, f"FastAPI 后端路由构建失败: {exc}"
    return True, "生产入口固定拉起 FastAPI 后端子进程。"


def check_legacy_portal_start_disabled() -> tuple[bool, str]:
    try:
        controller = portal_server.PortalServerController(
            host="127.0.0.1",
            port=18766,
            start_source_refresh_worker=False,
        )
        controller.start()
    except RuntimeError as exc:
        message = str(exc)
        if "旧 ThreadingHTTPServer" in message and "FastAPI" in message:
            return True, "旧 ThreadingHTTPServer 入口已动态禁用。"
        return False, f"旧门户 start() 异常文案不符合预期: {message}"
    except Exception as exc:
        return False, f"旧门户 start() 动态检查异常: {exc}"
    return False, "旧门户 start() 仍可启动，生产入口存在双服务风险。"


def main() -> int:
    failures: list[str] = []
    warnings: list[str] = []

    ok, offenders = check_git_runtime_data()
    if not ok:
        failures.append("git 已跟踪运行数据或依赖目录: " + ", ".join(offenders[:20]))

    ok, report = check_state_schema()
    if not ok:
        failures.append(f"SQLite schema 自检失败: {report}")

    ok, notice_identity_message = check_notice_identity_cleanup()
    if not ok:
        failures.append("SQLite notice_identity_map 清理不完整: " + notice_identity_message)

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

    ok, frontend_api_offenders = check_frontend_api_client()
    if not ok:
        failures.append(
            "Vue 前端请求必须统一走 frontend/src/api/client.ts: "
            + ", ".join(frontend_api_offenders[:20])
        )

    ok, frontend_realtime_offenders = check_frontend_realtime_coordination()
    if not ok:
        failures.append(
            "Vue 前端实时连接必须支持多标签降噪和隐藏页释放连接: "
            + ", ".join(frontend_realtime_offenders[:20])
        )

    ok, frontend_freeze_offenders = check_frontend_freeze_guards()
    if not ok:
        failures.append(
            "Vue 前端通告提交和列表渲染必须具备防卡顿保护: "
            + ", ".join(frontend_freeze_offenders[:20])
        )

    ok, frontend_skin_offenders = check_frontend_vnet_skin()
    if not ok:
        failures.append(
            "Vue 前端必须保持 VNET 蓝白生产皮肤、稳定顶栏和紧凑业务页面: "
            + ", ".join(frontend_skin_offenders[:20])
        )

    ok, legacy_static_offenders = check_legacy_static_portal_removed()
    if not ok:
        failures.append(
            "旧静态门户入口必须移除，生产只允许 Vue dist: "
            + ", ".join(legacy_static_offenders[:20])
        )

    ok, package_rule_offenders = check_package_portable_distribution_rules()
    if not ok:
        failures.append(
            "package_portable.py 必须排除用户运行数据和 node_modules，同时保留 Vue dist: "
            + ", ".join(package_rule_offenders[:20])
        )

    ok, env_offenders = check_release_env_flags()
    if not ok:
        failures.append("当前环境启用了生产禁用的回退/禁用开关: " + ", ".join(env_offenders))

    ok, model_offenders = check_fastapi_models()
    if not ok:
        failures.append("FastAPI 业务接口仍存在未模型化 JSON 请求读取: " + ", ".join(model_offenders))

    ok, qt_model_message = check_qt_model_view_default()
    if not ok:
        failures.append(qt_model_message)

    ok, qt_remote_offenders = check_qt_ui_no_direct_remote_business()
    if not ok:
        failures.append(
            "Qt UI 层仍直接引用飞书/多维业务函数，必须改为后端 command: "
            + ", ".join(qt_remote_offenders[:20])
        )

    ok, active_cache_offenders = check_qt_active_cache_no_runtime_legacy_document_writes()
    if not ok:
        failures.append(
            "Qt active cache 运行时仍保留兼容文档写回路径: "
            + ", ".join(active_cache_offenders[:20])
        )

    ok, active_cache_full_replace_offenders = check_qt_active_cache_full_replace_disabled_by_default()
    if not ok:
        failures.append(
            "Qt active cache 运行时全量 replace 必须默认关闭，只允许显式兜底/诊断路径: "
            + ", ".join(active_cache_full_replace_offenders[:20])
        )

    ok, direct_cache_save_offenders = check_qt_runtime_uses_deferred_active_cache_save()
    if not ok:
        failures.append(
            "Qt 运行期 UI 路径仍直接 save_active_cache，必须改为 request_active_cache_save: "
            + ", ".join(direct_cache_save_offenders[:20])
        )

    ok, remote_validation_offenders = check_qt_remote_validation_default_noop_before_queue()
    if not ok:
        failures.append(
            "Qt 远端校验默认关闭时必须在调度入口直接返回，不能先入队/启动 worker: "
            + ", ".join(remote_validation_offenders[:20])
        )

    ok, clipboard_poll_offenders = check_clipboard_file_polling_is_backend_aware()
    if not ok:
        failures.append(
            "Qt 剪贴板文件轮询必须以后端直连为主、文件兜底降频: "
            + ", ".join(clipboard_poll_offenders[:20])
        )

    ok, message_queue_offenders = check_runtime_queues_use_sqlite_runtime_queue()
    if not ok:
        failures.append(
            "消息/Qt 展示/上传等待队列仍在运行时使用内存结构调度，必须走 SQLite runtime_queue: "
            + ", ".join(message_queue_offenders[:20])
        )

    ok, qt_upload_queue_offenders = check_qt_upload_path_has_no_local_queue_dispatch()
    if not ok:
        failures.append(
            "Qt 上传按钮路径仍在调用本地上传队列，必须直接提交后端 command: "
            + ", ".join(qt_upload_queue_offenders[:20])
        )

    ok, upload_transient_offenders = check_qt_upload_transient_state_not_persisted()
    if not ok:
        failures.append(
            "Qt 上传中临时状态不能写入 active cache，避免重启后残留上传中和额外 IO: "
            + ", ".join(upload_transient_offenders[:20])
        )

    ok, qt_history_offenders = check_qt_normal_history_removed()
    if not ok:
        failures.append(
            "Qt 普通历史通告存储路径必须移除，只保留后端历史删除/撤销: "
            + ", ".join(qt_history_offenders[:20])
        )

    ok, active_delta_offenders = check_qt_active_delta_flush_on_stop()
    if not ok:
        failures.append(
            "Qt active item 增量上报退出保护不完整: "
            + ", ".join(active_delta_offenders[:20])
        )

    ok, legacy_qt_callback_offenders = check_qt_legacy_business_callbacks_disabled()
    if not ok:
        failures.append(
            "独立后端模式必须默认禁用旧 Qt 业务回调，并把全量快照降为低频兜底: "
            + ", ".join(legacy_qt_callback_offenders[:20])
        )

    ok, backend_thread_offenders = check_backend_background_threads_are_bounded()
    if not ok:
        failures.append(
            "FastAPI 请求触发的后台任务必须走受控 executor，避免线程风暴: "
            + ", ".join(backend_thread_offenders[:20])
        )

    ok, active_sse_offenders = check_qt_active_items_sse_uses_lightweight_meta_gate()
    if not ok:
        failures.append(
            "Qt active-items SSE 必须先用轻量版本门禁，避免多连接周期性扫完整快照: "
            + ", ".join(active_sse_offenders[:20])
        )

    ok, system_alert_offenders = check_system_alert_worker_can_shutdown()
    if not ok:
        failures.append(
            "系统告警 worker 必须支持退出释放: "
            + ", ".join(system_alert_offenders[:20])
        )

    ok, event_relay_offenders = check_event_relay_stop_clears_thread_state()
    if not ok:
        failures.append(
            "EventRelay 停止流程必须清理线程状态: "
            + ", ".join(event_relay_offenders[:20])
        )

    ok, notice_type_errors = check_notice_type_mappings()
    if not ok:
        failures.append(
            "六类通告 work_type/notice_type 映射不完整: "
            + "; ".join(notice_type_errors[:20])
        )

    ok, target_candidate_errors = check_target_candidate_match_reason()
    if not ok:
        failures.append(
            "目标多维候选记录必须返回并展示匹配原因: "
            + "; ".join(target_candidate_errors[:20])
        )

    ok, site_photo_reuse_errors = check_notice_site_photo_payload_not_reused()
    if not ok:
        failures.append(
            "通告更新/结束不得复用旧现场照片上传凭证: "
            + "; ".join(site_photo_reuse_errors[:20])
        )

    ok, fastapi_entry_message = check_fastapi_backend_entry()
    if not ok:
        failures.append(fastapi_entry_message)

    ok, legacy_start_message = check_legacy_portal_start_disabled()
    if not ok:
        failures.append(legacy_start_message)

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
