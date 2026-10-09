import argparse

import hashlib

import json

import os

import re

import shutil

import subprocess

import sys

import tempfile

import time

import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
import threading

from pathlib import Path
from functools import lru_cache
from bin.frontend_assets import FRONTEND_INDEX, is_frontend_asset, referenced_assets

from urllib.parse import urlencode, urlparse



APP_NAME = "ClipFlow"

PROJECT_ROOT = Path(__file__).resolve().parent

BUILD_DIR = PROJECT_ROOT / "build_output"

BIN_DIR = PROJECT_ROOT / "bin"

WHEELS_DIR = PROJECT_ROOT / "bin" / "wheels"

PATCH_SEQUENCE_STATE_FILE = BUILD_DIR / ".patch_sequence_state.json"

PREFERRED_BUILD_PYTHON = Path(r"D:\Python313\python.exe")

DEFAULT_MAJOR_VERSION = 2  # 默认大版本

BASE_VERSION_ID = f"{APP_NAME}_V{DEFAULT_MAJOR_VERSION}"

DEFAULT_GITEE_REPO = "https://gitee.com/myligitt/test.git"#

DEFAULT_GITEE_BRANCH = "master"

DEFAULT_GITEE_SUBDIR = "updates/patches"

DEFAULT_GITEE_MANIFEST_PATH = "updates/latest_patch.json"

LEGACY_PATCH_ZIP_NAME = "ClipFlow_patch_only.zip"

REMOTE_PATCH_HISTORY = 3

PREFLIGHT_WORKERS = min(4, os.cpu_count() or 1)

AUTO_UPLOAD_GITEE = True  # 将zip补丁上传gitee

# 版本命名模式：

# - "timestamp": ClipFlow_portable_YYYYmmdd_HHMMSS (旧格式)

# - "base_ts":   ClipFlow_M{BASE}_S{YYYYmmdd_HHMMSS} (大版本+小版本)

# - "base_simple": ClipFlow_V1_YYYYmmdd_HHMMSS (大版本+时间戳)

VERSION_NAMING = "base_simple"

DEFAULT_FORCE_UI_UPDATE = True  # 是否强制用户重启程序以更新UI界面

EXCLUDE_FILES = {

    "vc_redist.x64.exe",

    "package_portable.py",

    "分发前检查清单.md",

    "静默启动.bat",

    "active_cache.json",

    "app_log.txt",

    "config.json",

    "history.json",

    "feishu_error_code.md",

    "debug_feishu_fields.py",

    "debug_feishu_fields_json.py",

    "fields_output.json",

    "test_create_record.py",

}



EXCLUDE_TOP_LEVEL = {

    ".git",

    ".idea",

    ".vscode",

    "output",

    "__pycache__",

    "build_output",

    "data",

} | EXCLUDE_FILES



EXCLUDE_DIR_NAMES = {
    ".deepcode",
    ".agents",
    ".codex",
    ".claude",

    ".git",

    ".idea",

    ".vscode",

    ".vite",

    ".pytest_cache",

    "node_modules",

    "public_polling_relay",

    "__pycache__",

}


RUNTIME_DATA_DIR_PARTS = (
    ("bin", "data"),
    ("bin", "runtime"),
    ("data",),
)

RUNTIME_DATA_SUFFIXES = {
    ".db",
    ".sqlite",
    ".sqlite3",
    ".db-wal",
    ".db-shm",
    ".sqlite-wal",
    ".sqlite-shm",
    ".sqlite3-wal",
    ".sqlite3-shm",
}



FORCE_PATCH_INCLUDE_FILES = {

    # The local baseline can already be updated while clients are still older.
    # Always ship both entries; their complete asset closure is copied below.
    FRONTEND_INDEX,
    FRONTEND_INDEX.with_name("assistant.html"),

    Path("bin") / "upload_event_module" / "web" / "index.html",

    Path("bin") / "upload_event_module" / "services" / "process_lifetime.py",
    Path("启动程序.bat"),
    Path("bin/refactored_main.py"),
    Path("bin/openclaw_service/__main__.py"),
    Path("bin/openclaw_service/launcher.py"),
    Path("bin/openclaw_service/client.py"),
    Path("bin/openclaw_service/protocol.py"),
    Path("bin/openclaw_service/update.py"),
    Path("bin/lan_bitable_template_portal/lighthouse_routes.py"),
    Path("bin/lan_bitable_template_portal/lighthouse_bridge.py"),
    Path("bin/openclaw_service/assistant/openclaw/runtime.json"),
    Path("bin/openclaw_service/assistant/openclaw/native-paths.mjs"),
    Path("bin/openclaw_service/assistant/openclaw/gateway-start.mjs"),
    Path("bin/openclaw_service/assistant/openclaw/distribution.json"),
    Path("bin/openclaw_service/assistant/openclaw/plugin/index.mjs"),
    Path("bin/openclaw_service/assistant/openclaw/plugin/package.json"),
    Path("bin/openclaw_service/assistant/openclaw/plugin/openclaw.plugin.json"),
    Path("bin/openclaw_service/assistant/openclaw/skills/registry.json"),
    Path("bin/openclaw_service/assistant/openclaw/skills/alert-tagging/references/rules.md"),
    Path("bin/openclaw_service/assistant/openclaw/skills/workbuddy-registry.json"),

}

IMPORTED_SKILLS_DIR = Path("bin/openclaw_service/assistant/openclaw/skills/workbuddy")

RUNTIME_TOOL_FILES = {

    Path("bin") / "tools" / "mock_lan_portal_pressure.py",

}



RUNTIME_MODULE_TO_PACKAGE = {

    "PyQt6": "PyQt6",

    "watchdog": "watchdog",

    "requests": "requests",

    "urllib3": "urllib3",

    "httpx": "httpx",
    "websocket": "websocket-client",
    "websockets": "websockets==16.0",

    "cryptography": "cryptography",

    "PIL": "Pillow",

    "openpyxl": "openpyxl",

    "pypdf": "pypdf",
    "yaml": "PyYAML",

    "anyio": "anyio",

    "apscheduler": "APScheduler",

    "fastapi": "fastapi",

    "uvicorn": "uvicorn",

    "pydantic": "pydantic",
    "pydantic_ai": "pydantic-ai-slim[openai]==2.52.0",
    "openai": "openai==3.22.1",

    "starlette": "starlette",

    "multipart": "python-multipart",

    "lark_oapi": "lark-oapi",

    "winocr": "winocr",

}



WINDOWS_RUNTIME_MODULE_TO_PACKAGE = {

    "win32api": "pywin32",

    "win32com.client": "pywin32",

    "win32gui": "pywin32",

    "win32job": "pywin32",

    "win32clipboard": "pywin32",

    "pythoncom": "pywin32",

    "pywintypes": "pywin32",

}



RUNTIME_PACKAGE_INSTALL_ORDER = [

    "PyQt6",

    "watchdog",

    "requests",

    "urllib3",

    "httpx",
    "websocket",
    "websockets",

    "cryptography",

    "PIL",

    "openpyxl",

    "pypdf",
    "yaml",

    "anyio",

    "apscheduler",

    "pydantic",
    "pydantic_ai",
    "openai",

    "starlette",

    "multipart",

    "fastapi",

    "uvicorn",

    "lark_oapi",

]



if sys.platform == "win32":

    RUNTIME_MODULE_TO_PACKAGE.update(WINDOWS_RUNTIME_MODULE_TO_PACKAGE)

    for module_name in WINDOWS_RUNTIME_MODULE_TO_PACKAGE:

        if module_name not in RUNTIME_PACKAGE_INSTALL_ORDER:

            RUNTIME_PACKAGE_INSTALL_ORDER.append(module_name)



SMOKE_IMPORT_MODULES = [
    "openclaw_service.__main__",
    "openclaw_service.server",
    "openclaw_service.client",
    "openclaw_service.protocol",
    "openclaw_service.store",
    "openclaw_service.bridge",
    "openclaw_service.launcher",
    "openclaw_service.update",
    "openclaw_service.gateway_log",
    "openclaw_service.assistant.routes",
    "openclaw_service.assistant.lighthouse_commands",
    "openclaw_service.assistant.lighthouse_shared_skills",
    "lan_bitable_template_portal.lighthouse_bridge",
    "upload_event_module.services.event_relay_server",
    "upload_event_module.services.process_lifetime",
    "upload_event_module.ui.event_relay_bridge",
    "upload_event_module.ui.main_window",
    "upload_event_module.ui.main_window_clipboard",
    "upload_event_module.ui.main_window_records",
    "upload_event_module.ui.main_window_workflow",
    "upload_event_module.ui.main_window_ui",
    "upload_event_module.ui.main_window_runtime",
    "lan_bitable_template_portal.portal_service",
    "lan_bitable_template_portal.server",
    "lan_bitable_template_portal.plan_convergence",
    "lan_bitable_template_portal.lighthouse_routes",
    "lan_bitable_template_portal.lighthouse_appearance",
    "lan_bitable_template_portal.lighthouse_widget",
    "lan_bitable_template_portal.lighthouse_model",
    "lan_bitable_template_portal.lighthouse_notice_sop",
    "lan_bitable_template_portal.lighthouse_notice_identity",
    "lan_bitable_template_portal.lighthouse_water",
    "lan_bitable_template_portal.lighthouse_downloads",
    "lan_bitable_template_portal.lighthouse_queries",
    "lan_bitable_template_portal.lighthouse_stream",
    "lan_bitable_template_portal.lighthouse_gateway",
    "lan_bitable_template_portal.lighthouse_runtime",
    "lan_bitable_template_portal.lighthouse_startup_log",
    "lan_bitable_template_portal.lighthouse_openclaw",
    "lan_bitable_template_portal.lighthouse_public",
    "lan_bitable_template_portal.lighthouse_distribution",
    "lan_bitable_template_portal.lighthouse_skills",
    "clipflow_backend.main",
    "clipflow_backend.process_controller",
    "lan_bitable_template_portal.cabinet_power_batches",
    "lan_bitable_template_portal.cabinet_power_evidence",
    "lan_bitable_template_portal.cabinet_power_text",
    "pypdf",
    "lark_oapi",
]

PACKAGING_PREFLIGHT_MODULES = [
    "yaml",
    "pydantic_ai",
    "openai",
    "httpx",
    "websocket",
    "websockets",
    "anyio",
    "apscheduler",
    "pydantic",
    "starlette",
    "multipart",
    "fastapi",
    "uvicorn",
    "lark_oapi",
    "pypdf",
]




def log(msg: str) -> None:

    print(f"[Package] {msg}", flush=True)





def _extract_base_tag(base_build_id: str) -> str:

    if not base_build_id:

        return ""

    for prefix in (f"{APP_NAME}_portable_", f"{APP_NAME}_"):

        if base_build_id.startswith(prefix):

            return base_build_id[len(prefix) :]

    return base_build_id





def _extract_major_version(version_text: str) -> int:

    text = version_text or ""

    match = re.search(r"_V(\d+)", text, re.IGNORECASE)

    if match:

        try:

            return int(match.group(1))

        except Exception:

            return DEFAULT_MAJOR_VERSION

    return DEFAULT_MAJOR_VERSION





def _safe_int(value, default: int = 0) -> int:

    try:

        return int(value)

    except Exception:

        return default





def _build_display_version(

    major_version: int, patch_version: int, build_date: str

) -> str:

    major_value = _safe_int(major_version, DEFAULT_MAJOR_VERSION)

    patch_value = _safe_int(patch_version, 0)

    date_value = (build_date or "").strip()

    if len(date_value) != 8 or not date_value.isdigit():

        date_value = time.strftime("%Y%m%d")

    return f"V{major_value}.{patch_value}.{date_value}"





def format_dist_name(timestamp: str, base_build_id: str) -> str:

    if VERSION_NAMING == "timestamp":

        return f"{APP_NAME}_portable_{timestamp}"

    if VERSION_NAMING == "base_ts":

        base_tag = _extract_base_tag(base_build_id) or timestamp

        return f"{APP_NAME}_M{base_tag}_S{timestamp}"

    if VERSION_NAMING == "base_simple":

        base_tag = base_build_id or f"{APP_NAME}_V1"

        return f"{base_tag}_{timestamp}"

    return f"{APP_NAME}_portable_{timestamp}"





def _read_build_meta(meta_path: Path) -> dict:

    if not meta_path.exists():

        return {}

    try:

        return json.loads(meta_path.read_text(encoding="utf-8"))

    except Exception:

        return {}


def _load_patch_sequence_state() -> dict:

    if not PATCH_SEQUENCE_STATE_FILE.exists():

        return {}

    try:

        data = json.loads(PATCH_SEQUENCE_STATE_FILE.read_text(encoding="utf-8"))

        return data if isinstance(data, dict) else {}

    except Exception:

        return {}


def _save_patch_sequence_state(state: dict) -> None:

    PATCH_SEQUENCE_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)

    PATCH_SEQUENCE_STATE_FILE.write_text(
        json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def _patch_sequence_key(base_build_id: str, major_version: int) -> str:

    key = (base_build_id or "").strip()

    if key:

        return key

    return f"{APP_NAME}_V{_safe_int(major_version, DEFAULT_MAJOR_VERSION)}"


def _resolve_base_patch_version(
    sequence_key: str, *, baseline_patch_version: int
) -> int:

    baseline_value = _safe_int(baseline_patch_version, 0)

    state = _load_patch_sequence_state()

    entry = state.get(sequence_key)

    if not isinstance(entry, dict):

        return baseline_value

    state_patch_value = _safe_int(entry.get("patch_version"), 0)

    return max(baseline_value, state_patch_value)





def _compute_ui_version(root_dir: Path) -> str:

    hasher = hashlib.sha256()

    ui_root = root_dir / "bin" / "upload_event_module" / "ui"

    web_root = root_dir / "bin" / "upload_event_module" / "web"

    tracked_files: list[Path] = []

    for base in (ui_root, web_root):

        if not base.exists():

            continue

        for path in base.rglob("*"):

            if path.is_file():

                tracked_files.append(path)

    tracked_files.sort()

    for path in tracked_files:

        try:

            rel = path.relative_to(root_dir)

        except Exception:

            rel = path

        hasher.update(str(rel).replace("\\", "/").encode("utf-8"))

        with path.open("rb") as f:

            for chunk in iter(lambda: f.read(1024 * 1024), b""):

                hasher.update(chunk)

    digest = hasher.hexdigest()

    return digest[:12]





def write_build_meta(
    target_dir: Path,
    build_id: str,
    *,

    base_build_id: str = "",

    venv_hash: str = "",

    major_version: int = DEFAULT_MAJOR_VERSION,

    patch_version: int = 0,

    display_version: str = "",

    ui_version: str = "",

) -> None:

    major_value = _safe_int(major_version, DEFAULT_MAJOR_VERSION)

    patch_value = _safe_int(patch_version, 0)

    display_value = (display_version or "").strip() or _build_display_version(

        major_value,

        patch_value,

        time.strftime("%Y%m%d"),

    )

    meta = {

        "app_name": APP_NAME,

        "build_id": build_id,

        "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),

        "major_version": major_value,

        "patch_version": patch_value,

        "display_version": display_value,

        "ui_version": (ui_version or "").strip(),

    }

    if base_build_id:

        meta["base_build_id"] = base_build_id

    if venv_hash:

        meta["venv_hash"] = venv_hash

    meta_dir = target_dir / "bin"

    meta_dir.mkdir(parents=True, exist_ok=True)

    (meta_dir / "build_meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def _advance_local_patch_sequence(
    sequence_key: str,
    *,
    target_patch_version: int,
    target_display_version: str = "",
    major_version: int = DEFAULT_MAJOR_VERSION,
    base_build_id: str = "",
) -> bool:
    target_patch_value = _safe_int(target_patch_version, 0)
    if target_patch_value <= 0:
        return False
    state = _load_patch_sequence_state()
    entry = state.get(sequence_key)
    if not isinstance(entry, dict):
        entry = {}
    current_patch_value = _safe_int(entry.get("patch_version"), 0)
    if current_patch_value >= target_patch_value:
        return False
    major_value = _safe_int(major_version, _extract_major_version(base_build_id))
    display_value = (target_display_version or "").strip() or _build_display_version(
        major_value,
        target_patch_value,
        time.strftime("%Y%m%d"),
    )
    entry["base_build_id"] = (base_build_id or "").strip()
    entry["major_version"] = major_value
    entry["patch_version"] = target_patch_value
    entry["display_version"] = display_value
    entry["updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
    state[sequence_key] = entry
    _save_patch_sequence_state(state)
    return True


def write_patch_meta(
    patch_dir: Path,
    *,
    base_build_id: str = "",

    min_version: str = "",

    target_build_id: str = "",

    venv_hash: str = "",

    base_venv_hash: str = "",

    include_venv: bool = False,

    major_version: int = DEFAULT_MAJOR_VERSION,

    base_patch_version: int = 0,

    target_patch_version: int = 1,

    target_display_version: str = "",

    base_ui_version: str = "",

    target_ui_version: str = "",

    ui_changed: bool = False,

    force_ui_update: bool = False,

    required_packages: list[str] | None = None,

    module_to_package: dict[str, str] | None = None,

    python_version: str = "",

) -> None:

    major_value = _safe_int(major_version, DEFAULT_MAJOR_VERSION)

    base_patch_value = _safe_int(base_patch_version, 0)

    target_patch_value = _safe_int(target_patch_version, base_patch_value + 1)

    display_value = (target_display_version or "").strip() or _build_display_version(

        major_value,

        target_patch_value,

        time.strftime("%Y%m%d"),

    )

    min_version_value = (min_version or "").strip()

    meta = {

        "app_name": APP_NAME,

        "min_version": min_version_value,

        "target_version": target_build_id,

        "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),

        "restart_required": bool(ui_changed),

        "include_venv": include_venv,

        "major_version": major_value,

        "base_patch_version": base_patch_value,

        "target_patch_version": target_patch_value,

        "target_display_version": display_value,

        "base_ui_version": (base_ui_version or "").strip(),

        "target_ui_version": (target_ui_version or "").strip(),

        "ui_changed": bool(ui_changed),

        "force_ui_update": bool(force_ui_update),

        "required_packages": list(required_packages or []),

        "module_to_package": dict(module_to_package or {}),

        "python_version": (python_version or "").strip(),

    }

    meta["file_sha256"] = {
        path.relative_to(patch_dir).as_posix(): _hash_file(path)
        for path in sorted(patch_dir.rglob("*"))
        if path.is_file() and path.name != "patch_meta.json"
    }

    if venv_hash:

        meta["venv_hash"] = venv_hash

    if base_venv_hash:

        meta["base_venv_hash"] = base_venv_hash

    meta_dir = patch_dir / "bin"

    meta_dir.mkdir(parents=True, exist_ok=True)

    (meta_dir / "patch_meta.json").write_text(

        json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"

    )





def _find_build_python() -> Path | None:

    candidates: list[Path] = []

    env_python = (os.environ.get("PACKAGE_BUILD_PYTHON") or "").strip()

    if env_python:

        candidates.append(Path(env_python))

    candidates.append(PREFERRED_BUILD_PYTHON)

    current_python = (sys.executable or "").strip()

    if current_python:

        candidates.append(Path(current_python))

    candidates.extend(

        [

            BIN_DIR / ".venv" / "Scripts" / "python.exe",

            BIN_DIR / ".venv" / "bin" / "python",

            PROJECT_ROOT / ".venv" / "Scripts" / "python.exe",

            PROJECT_ROOT / ".venv" / "bin" / "python",

        ]

    )

    seen: set[str] = set()

    for path in candidates:

        normalized = str(path).strip()

        if not normalized or normalized in seen:

            continue

        seen.add(normalized)

        if path.exists():

            return path

    return None





def _find_dist_venv_python(dist_dir: Path) -> Path | None:

    candidates = [

        dist_dir / "bin" / ".venv" / "Scripts" / "python.exe",

        dist_dir / "bin" / ".venv" / "bin" / "python",

        dist_dir / ".venv" / "Scripts" / "python.exe",

        dist_dir / ".venv" / "bin" / "python",

    ]

    for path in candidates:

        if path.exists():

            return path

    return None





def _run_cmd(

    args: list[str], *, log_output_on_error: bool = True, cwd: Path | None = None

) -> bool:

    try:

        result = subprocess.run(

            args,

            capture_output=True,

            text=True,

            encoding="utf-8",

            errors="ignore",

            check=False,

            cwd=str(cwd) if cwd else None,

        )

    except Exception as exc:

        log(f"命令执行失败: {exc}")

        return False



    if result.returncode != 0:

        if log_output_on_error and result.stdout.strip():

            log(f"标准输出:\n{result.stdout.strip()}")

        if log_output_on_error and result.stderr.strip():

            log(f"标准错误:\n{result.stderr.strip()}")

        return False

    return True





def _run_cmd_capture(args: list[str], *, cwd: Path | None = None) -> tuple[bool, str]:

    try:

        result = subprocess.run(

            args,

            capture_output=True,

            text=True,

            encoding="utf-8",

            errors="ignore",

            check=False,

            cwd=str(cwd) if cwd else None,

        )

    except Exception as exc:

        log(f"命令执行失败: {exc}")

        return False, ""



    if result.returncode != 0:

        if result.stdout.strip():

            log(f"标准输出:\n{result.stdout.strip()}")

        if result.stderr.strip():

            log(f"标准错误:\n{result.stderr.strip()}")

        return False, ""

    return True, result.stdout





def _get_venv_hash(venv_python: Path) -> str:

    ok, output = _run_cmd_capture([str(venv_python), "-m", "pip", "freeze"])

    if not ok:

        return ""

    lines = [line.strip() for line in output.splitlines() if line.strip()]

    lines.sort()

    digest = hashlib.sha256("\n".join(lines).encode("utf-8")).hexdigest()

    return digest





def _missing_runtime_modules(venv_python: Path) -> list[str]:
    return _missing_selected_modules(venv_python, list(RUNTIME_MODULE_TO_PACKAGE))


def _missing_selected_modules(venv_python: Path, modules: list[str]) -> list[str]:
    # Match startup's lightweight dependency check. Runtime imports and behavior
    # are checked by the separate smoke check and preflight tests.
    script_lines = [
        "import importlib.util",
        "import importlib.metadata",
        "pinned = {'pydantic_ai': ('pydantic-ai-slim', '2.52.0'), 'openai': ('openai', '3.22.1'), 'websockets': ('websockets', '16.0')}",
        "mods = " + repr(modules),
        "missing = []",
        "for name in mods:",
        "    try:",
        "        if importlib.util.find_spec(name) is None: missing.append(name)",
        "        elif name in pinned and importlib.metadata.version(pinned[name][0]) != pinned[name][1]: missing.append(name)",
        "    except Exception:",
        "        missing.append(name)",
        "print('\\n'.join(missing))",
    ]
    ok, output = _run_cmd_capture([str(venv_python), "-c", "\n".join(script_lines)])
    if not ok:
        return modules
    return [line.strip() for line in output.splitlines() if line.strip()]


def _ensure_packaging_preflight_dependencies(python_exe: Path | None = None) -> None:
    python_exe = python_exe or _find_dist_venv_python(PROJECT_ROOT) or Path(sys.executable)
    missing = _missing_selected_modules(python_exe, PACKAGING_PREFLIGHT_MODULES)
    if not missing:
        return

    packages = list(
        dict.fromkeys(
            RUNTIME_MODULE_TO_PACKAGE[m]
            for m in PACKAGING_PREFLIGHT_MODULES
            if m in missing and m in RUNTIME_MODULE_TO_PACKAGE
        )
    )
    if not packages:
        return

    log("发布就绪检查缺少依赖，先补齐: " + ", ".join(packages))
    if WHEELS_DIR.exists():
        _pip_install_packages(python_exe, packages, use_local_wheels=True)

    missing_after_wheels = _missing_selected_modules(python_exe, PACKAGING_PREFLIGHT_MODULES)
    if not missing_after_wheels:
        log("发布就绪检查依赖已从本地 wheels 补齐。")
        return

    packages_after_wheels = list(
        dict.fromkeys(
            RUNTIME_MODULE_TO_PACKAGE[m]
            for m in PACKAGING_PREFLIGHT_MODULES
            if m in missing_after_wheels and m in RUNTIME_MODULE_TO_PACKAGE
        )
    )
    if packages_after_wheels:
        _pip_install_packages(python_exe, packages_after_wheels, use_local_wheels=False)

    final_missing = _missing_selected_modules(python_exe, PACKAGING_PREFLIGHT_MODULES)
    if final_missing:
        missing_text = ", ".join(RUNTIME_MODULE_TO_PACKAGE.get(m, m) for m in final_missing)
        raise RuntimeError(f"发布就绪检查依赖缺失: {missing_text}")





def _pip_install_packages(

    venv_python: Path, packages: list[str], *, use_local_wheels: bool

) -> bool:

    if not packages:

        return True

    args = [str(venv_python), "-m", "pip", "install"]

    if use_local_wheels:

        args.extend(["--no-index", "--find-links", str(WHEELS_DIR)])

    args.extend(packages)

    return _run_cmd(args)





def _verify_runtime_imports(venv_python: Path, project_root: Path) -> bool:

    script_lines = [

        "import importlib",

        "import pathlib",

        "import sys",

        f"root = pathlib.Path(r'''{str(project_root)}''')",

        "sys.path.insert(0, str(root / 'bin'))",

        "mods = " + repr(SMOKE_IMPORT_MODULES),

        "errors = []",

        "for m in mods:",

        "    try:",

        "        importlib.import_module(m)",

        "    except Exception as exc:",

        "        errors.append(f'{m}: {exc!r}')",

        "if errors:",

        "    print('\\n'.join(errors))",

        "    raise SystemExit(2)",

    ]

    return _run_cmd([str(venv_python), "-c", "\n".join(script_lines)])





def ensure_runtime_dependencies(venv_python: Path) -> None:

    missing = _missing_runtime_modules(venv_python)

    if not missing:

        log("运行时依赖已安装。")

        return



    missing_pkgs = list(

        dict.fromkeys(

            [

                RUNTIME_MODULE_TO_PACKAGE[m]

                for m in RUNTIME_PACKAGE_INSTALL_ORDER

                if m in missing

            ]

        )

    )

    log(f"缺少运行时依赖: {', '.join(missing_pkgs)}")



    if WHEELS_DIR.exists():

        log("正在从本地 wheels 安装运行时依赖...")

        _pip_install_packages(venv_python, missing_pkgs, use_local_wheels=True)



    missing_after_wheels = _missing_runtime_modules(venv_python)

    if not missing_after_wheels:

        log("已从本地 wheels 安装运行时依赖。")

        return



    missing_after_wheels_pkgs = list(

        dict.fromkeys(

            [

                RUNTIME_MODULE_TO_PACKAGE[m]

                for m in RUNTIME_PACKAGE_INSTALL_ORDER

                if m in missing_after_wheels

            ]

        )

    )

    log(

        "本地 wheels 安装后仍缺少依赖，回退到 PyPI 安装: "

        + ", ".join(missing_after_wheels_pkgs)

    )

    _pip_install_packages(

        venv_python, missing_after_wheels_pkgs, use_local_wheels=False

    )



    final_missing = _missing_runtime_modules(venv_python)

    if final_missing:

        missing_text = ", ".join(RUNTIME_MODULE_TO_PACKAGE[m] for m in final_missing)

        raise RuntimeError(f"运行时依赖安装不完整: {missing_text}")



    log("运行时依赖安装完成。")


@lru_cache(maxsize=4)
def _registered_skill_resources(registry_path: str, modified: int, size: int) -> frozenset[str]:
    if size > 1024 * 1024:
        raise RuntimeError('WorkBuddy skill registry exceeds packaging limit')
    records = json.loads(Path(registry_path).read_text(encoding='utf-8'))
    if not isinstance(records, list) or len(records) > 40:
        raise RuntimeError('WorkBuddy skill registry is invalid')
    result = set()
    for record in records:
        name = record.get('name', '')
        if not re.fullmatch(r'[a-z][a-z0-9-]{0,60}', name) or record.get('kind') != 'workflow_guide':
            raise RuntimeError('WorkBuddy skill registry entry is invalid')
        prefix = 'workbuddy/' + name + '/'
        for value in [record.get('path', ''), *record.get('references', [])]:
            if not isinstance(value, str) or not value.startswith(prefix) or '\\' in value or '..' in Path(value).parts:
                raise RuntimeError('WorkBuddy skill resource path is invalid')
            result.add(value)
    return frozenset(result)


def _is_development_only_path(path: Path, root: Path, *, relative_path: Path | None = None) -> bool:
    try:
        rel = relative_path if relative_path is not None else path.resolve().relative_to(root.resolve())
    except Exception:
        return False
    if rel in FORCE_PATCH_INCLUDE_FILES:
        return False
    parts = tuple(part.lower() for part in rel.parts)
    if not parts:
        return False
    if parts[0] in {".codex-audit", ".pytest_cache", "docs", ".deepcode", ".agents", ".codex", ".claude"} or parts[0].startswith(".patch-"):
        return True
    if parts[:2] == ("bin", "tests"):
        return True
    if len(parts) == 2 and parts[0] == "bin":
        is_test_or_mock = parts[1].startswith("test_") or parts[1] == "mock_lan_notice_pressure.py"
        if is_test_or_mock and path.suffix.lower() == ".py":
            return True
    if parts[:2] == ("bin", "tools") and len(parts) > 2:
        return rel not in RUNTIME_TOOL_FILES
    frontend = ("bin", "lan_bitable_template_portal", "frontend")
    if parts[:3] == frontend and len(parts) > 3 and parts[3] not in {"data", "dist"}:
        return True
    name = path.name.lower()
    if parts[:5] == ('bin', 'openclaw_service', 'assistant', 'openclaw', 'skills') and name == 'skill.md':
        return False
    if parts[:5] == ('bin', 'openclaw_service', 'assistant', 'openclaw', 'skills'):
        registry = root / 'bin/openclaw_service/assistant/openclaw/skills/workbuddy-registry.json'
        if registry.is_file():
            stamp = registry.stat()
            registered = _registered_skill_resources(str(registry), stamp.st_mtime_ns, stamp.st_size)
            if Path(*rel.parts[5:]).as_posix() in registered:
                return False
            if len(parts) > 5 and parts[5] == 'workbuddy' and path.is_file():
                return True
    return (
        ".legacy_conflict_" in name
        or name == ".gitignore"
        or path.suffix.lower() in {".log", ".map", ".md"}
    )





def _ignore_names(dirpath: str, names: list[str]) -> set[str]:

    ignore = set()

    base = Path(dirpath)

    for name in names:

        if (base / name).resolve() == (PROJECT_ROOT / IMPORTED_SKILLS_DIR).resolve():
            ignore.add(name)
            continue

        if _is_development_only_path(base / name, PROJECT_ROOT):

            ignore.add(name)

            continue

        if name in EXCLUDE_DIR_NAMES or name in EXCLUDE_FILES:

            ignore.add(name)

            continue



        if name in ("build", "dist") and base.name == "bin":

            ignore.add(name)

            continue

        if name == "data" and base.name in {"bin", PROJECT_ROOT.name}:

            ignore.add(name)

            continue



        if name.endswith((".pyc", ".pyo")):

            ignore.add(name)

            continue



    return ignore





def _has_relative_prefix(parts: tuple[str, ...], prefix: tuple[str, ...]) -> bool:

    if not prefix or len(parts) < len(prefix):

        return False

    lowered = tuple(str(part).lower() for part in parts)

    lowered_prefix = tuple(str(part).lower() for part in prefix)

    return lowered[: len(lowered_prefix)] == lowered_prefix


def _is_runtime_data_path(path: Path, root: Path | None = None, *, relative_path: Path | None = None) -> bool:

    try:

        parts = (relative_path if relative_path is not None else path.resolve().relative_to((root or PROJECT_ROOT).resolve())).parts

    except Exception:

        parts = path.parts

    if any(_has_relative_prefix(parts, prefix) for prefix in RUNTIME_DATA_DIR_PARTS):

        return True

    lowered_parts = tuple(str(part).lower() for part in parts)

    for index, part in enumerate(lowered_parts[:-1]):

        if part == "bin" and lowered_parts[index + 1] == "data":

            return True

    return path.name.lower().endswith(tuple(RUNTIME_DATA_SUFFIXES))


def _scan_runtime_data_files(root: Path) -> list[Path]:

    root = Path(root)

    if not root.exists():

        return []

    found: list[Path] = []

    for path in root.rglob("*"):

        if path.is_dir():

            continue

        if _is_runtime_data_path(path, root, relative_path=path.relative_to(root)):

            found.append(path)

    return found


def _assert_no_runtime_data_in_output(root: Path, label: str) -> None:

    found = _scan_runtime_data_files(root)

    if found:

        preview = ", ".join(str(path.relative_to(root)) for path in found[:10])

        more = "" if len(found) <= 10 else f" 等 {len(found)} 个文件"

        raise RuntimeError(

            f"{label}包含运行数据文件，已中止打包，避免覆盖用户现场数据: {preview}{more}"

        )

    log(f"{label}运行数据排除检查通过。")


def _assert_no_development_files_in_output(root: Path, label: str) -> None:
    found = [
        path
        for path in Path(root).rglob("*")
        if path.is_file() and _is_development_only_path(path, Path(root), relative_path=path.relative_to(root))
    ]
    if found:
        preview = ", ".join(str(path.relative_to(root)) for path in found[:10])
        raise RuntimeError(f"{label}包含开发文件，已中止打包: {preview}")
    log(f"{label}开发文件排除检查通过。")


def _assert_project_iterator_excludes_runtime_data() -> None:

    leaked = [
        path
        for path in _iter_project_files(PROJECT_ROOT, exclude_venv=True)
        if _is_runtime_data_path(path, PROJECT_ROOT, relative_path=path.relative_to(PROJECT_ROOT))
    ]

    if leaked:

        preview = ", ".join(str(path.relative_to(PROJECT_ROOT)) for path in leaked[:10])

        raise RuntimeError(f"源码复制列表包含运行数据，已中止: {preview}")

    log("源码复制列表运行数据排除检查通过。")


def _run_preflight_check(args: list[str], **kwargs) -> None:
    label = (f"{args[3]} 等 {len(args) - 3} 个测试模块" if args[1:3] == ["-m", "unittest"]
             else "Python 语法检查" if args[1] == "-c" else Path(args[1]).name)
    started = time.monotonic()
    log(f"检查开始: {label}")
    try:
        if args[1:3] == ["-m", "unittest"]:
            _run_preflight_test_shards(args, **kwargs)
        else:
            subprocess.run(args, **kwargs)
    finally:
        log(f"检查耗时: {label}，{time.monotonic() - started:.1f} 秒")


def _run_preflight_test_shards(args: list[str], **kwargs) -> None:
    modules = args[3:]
    shard_size = min(8, max(1, (len(modules) + PREFLIGHT_WORKERS - 1) // PREFLIGHT_WORKERS))
    shards = [modules[index:index + shard_size] for index in range(0, len(modules), shard_size)]
    output_lock = threading.Lock()
    failures = []

    def run_shard(index, names):
        label = f"测试组 {index + 1}/{len(shards)}: {names[0]} 等 {len(names)} 项"
        started = time.monotonic()
        with output_lock:
            log(f"{label} 开始")
        with tempfile.TemporaryDirectory(prefix="clipflow_preflight_") as directory:
            env = dict(kwargs.get("env", os.environ))
            env.update(CLIPFLOW_DATA_DIR=directory, PYTHONIOENCODING="utf-8")
            options = {**kwargs, "env": env}
            with tempfile.TemporaryFile(mode="w+", encoding="utf-8", errors="replace") as output:
                failed = False
                try:
                    subprocess.run(args[:3] + names, **options, stdout=output,
                                   stderr=subprocess.STDOUT, timeout=900)
                except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
                    failed = True
                    raise
                finally:
                    output.seek(0)
                    detail = output.read()
                    with output_lock:
                        print(detail, end="", flush=True)
                        if failed:
                            failures.append((label, detail))
                        log(f"{label} 耗时 {time.monotonic() - started:.1f} 秒")

    log(f"测试分组并行执行，最多 {PREFLIGHT_WORKERS} 个进程，各组使用独立临时数据。")
    try:
        with ThreadPoolExecutor(max_workers=PREFLIGHT_WORKERS) as pool:
            futures = [pool.submit(run_shard, index, names) for index, names in enumerate(shards)]
            try:
                for future in as_completed(futures):
                    future.result()
            except BaseException:
                for future in futures:
                    future.cancel()
                raise
    finally:
        # Repeat failures after running siblings finish, not before later OK logs.
        for label, detail in failures:
            log(f"未通过: {label}，已停止打包。失败详情：")
            start = re.search(r"(?m)^={10,}\r?\n(?:FAIL|ERROR):", detail)
            print(detail[start.start():] if start else detail[-6000:], end="\n", flush=True)


def _run_packaging_preflight_tests() -> None:

    log("开始执行打包前自动测试。")
    test_python = str(_find_dist_venv_python(PROJECT_ROOT) or sys.executable)
    log(f"测试解释器: {test_python}")

    _assert_project_iterator_excludes_runtime_data()
    _cleanup_vue_dist_assets()

    py_targets = [
        PROJECT_ROOT / "bin" / "refactored_main.py",
        PROJECT_ROOT / "bin" / "clipflow_backend" / "main.py",
        PROJECT_ROOT / "bin" / "clipflow_backend" / "preflight.py",
        PROJECT_ROOT / "bin" / "clipflow_backend" / "process_controller.py",
        PROJECT_ROOT / "bin" / "clipflow_backend" / "runtime_helpers.py",
        PROJECT_ROOT / "bin" / "upload_event_module" / "services" / "process_lifetime.py",
        PROJECT_ROOT / "bin" / "lan_bitable_template_portal" / "portal_service.py",
        PROJECT_ROOT / "bin" / "lan_bitable_template_portal" / "signature_print.py",
        PROJECT_ROOT / "bin" / "lan_bitable_template_portal" / "repair_operations.py",
        PROJECT_ROOT / "bin" / "lan_bitable_template_portal" / "learning.py",
        PROJECT_ROOT / "bin" / "lan_bitable_template_portal" / "learning_cloud.py",
        PROJECT_ROOT / "bin" / "lan_bitable_template_portal" / "learning_routes.py",
        PROJECT_ROOT / "bin" / "lan_bitable_template_portal" / "lighthouse_ai.py",
        PROJECT_ROOT / "bin" / "lan_bitable_template_portal" / "lighthouse_sources.py",
        PROJECT_ROOT / "bin" / "lan_bitable_template_portal" / "lighthouse_routes.py",
        PROJECT_ROOT / "bin" / "lan_bitable_template_portal" / "lighthouse_appearance.py",
        PROJECT_ROOT / "bin" / "lan_bitable_template_portal" / "lighthouse_widget.py",
        PROJECT_ROOT / "bin" / "lan_bitable_template_portal" / "lighthouse_pending.py",
        PROJECT_ROOT / "bin" / "lan_bitable_template_portal" / "lighthouse_scope.py",
        PROJECT_ROOT / "bin" / "lan_bitable_template_portal" / "lighthouse_model.py",
        PROJECT_ROOT / "bin" / "lan_bitable_template_portal" / "lighthouse_notice_sop.py",
        PROJECT_ROOT / "bin" / "lan_bitable_template_portal" / "lighthouse_notice_identity.py",
        PROJECT_ROOT / "bin" / "lan_bitable_template_portal" / "lighthouse_water.py",
        PROJECT_ROOT / "bin" / "lan_bitable_template_portal" / "lighthouse_downloads.py",
        PROJECT_ROOT / "bin" / "lan_bitable_template_portal" / "lighthouse_queries.py",
        PROJECT_ROOT / "bin" / "lan_bitable_template_portal" / "lighthouse_stream.py",
        PROJECT_ROOT / "bin/lan_bitable_template_portal/lighthouse_gateway.py",
        PROJECT_ROOT / "bin/lan_bitable_template_portal/lighthouse_runtime.py",
        PROJECT_ROOT / "bin/lan_bitable_template_portal/lighthouse_startup_log.py",
        PROJECT_ROOT / "bin/lan_bitable_template_portal/lighthouse_openclaw.py",
        PROJECT_ROOT / "bin/lan_bitable_template_portal/lighthouse_public.py",
        PROJECT_ROOT / "bin/lan_bitable_template_portal/lighthouse_distribution.py",
        PROJECT_ROOT / "bin/lan_bitable_template_portal/lighthouse_skills.py",
        PROJECT_ROOT / "bin" / "lan_bitable_template_portal" / "plan_convergence_routes.py",
        PROJECT_ROOT / "bin" / "lan_bitable_template_portal" / "plan_convergence_compare.py",
        PROJECT_ROOT / "bin" / "lan_bitable_template_portal" / "plan_convergence_auth.py",
        PROJECT_ROOT / "bin" / "lan_bitable_template_portal" / "plan_convergence_browser_login.py",
        PROJECT_ROOT / "bin" / "lan_bitable_template_portal" / "plan_convergence_maintenance.py",
        PROJECT_ROOT / "bin" / "lan_bitable_template_portal" / "plan_convergence_rules.py",
        PROJECT_ROOT / "bin" / "lan_bitable_template_portal" / "plan_convergence.py",
        PROJECT_ROOT / "bin" / "lan_bitable_template_portal" / "plan_convergence_points.py",
        PROJECT_ROOT / "bin" / "test_submission_reliability.py",
        PROJECT_ROOT / "bin" / "lan_bitable_template_portal" / "critical_guard.py",
        PROJECT_ROOT / "bin" / "lan_bitable_template_portal" / "polling_work_orders.py",
        PROJECT_ROOT / "bin" / "lan_bitable_template_portal" / "polling_work_order_relay.py",
        PROJECT_ROOT / "bin" / "lan_bitable_template_portal" / "server.py",
        PROJECT_ROOT / "bin" / "lan_bitable_template_portal" / "state_store.py",
        PROJECT_ROOT / "bin" / "lan_bitable_template_portal" / "cabinet_power_evidence.py",
        PROJECT_ROOT / "bin" / "lan_bitable_template_portal" / "cabinet_power_text.py",
        PROJECT_ROOT / "bin" / "test_critical_guard.py",
        PROJECT_ROOT / "bin" / "test_notice_identity_boundaries.py",
        PROJECT_ROOT / "bin" / "tools" / "notice_flow_smoke.py",
        PROJECT_ROOT / "bin" / "tools" / "release_readiness_check.py",
        PROJECT_ROOT / "bin" / "upload_event_module" / "ui" / "main_window_runtime.py",
        PROJECT_ROOT / "package_portable.py",
    ]

    # The Qt window is split across multiple mixin modules. Compile every UI
    # module so a syntax error in an indirectly imported file cannot ship in a
    # patch that passed preflight.
    py_targets.extend(
        sorted(
            (PROJECT_ROOT / "bin" / "upload_event_module" / "ui").rglob("*.py")
        )
    )
    py_targets.extend(sorted((PROJECT_ROOT / "bin/openclaw_service").rglob("*.py")))
    py_targets.append(PROJECT_ROOT / "bin/lan_bitable_template_portal/lighthouse_bridge.py")
    py_targets = list(dict.fromkeys(path for path in py_targets if path.exists()))

    if py_targets:
        with tempfile.TemporaryDirectory(prefix="clipflow_pycompile_") as pycache_dir:
            compile_script = "import pathlib,py_compile,sys\nfor index,source in enumerate(sys.argv[2:]): py_compile.compile(source,cfile=str(pathlib.Path(sys.argv[1])/f'{index}.pyc'),doraise=True)"
            _run_preflight_check(
                [test_python, "-c", compile_script, pycache_dir, *[os.fspath(path) for path in py_targets]],
                cwd=PROJECT_ROOT,
                check=True,
            )

        log(f"Python 语法检查通过: {len(py_targets)} 个文件。")

    log("Vue 生产页 JS 检查由发布就绪检查负责。")

    readiness_script = PROJECT_ROOT / "bin" / "tools" / "release_readiness_check.py"
    if readiness_script.exists():
        dependency_started = time.monotonic()
        log("检查开始: 测试依赖安装状态")
        _ensure_packaging_preflight_dependencies(Path(test_python))
        log(f"测试依赖检查通过，耗时 {time.monotonic() - dependency_started:.1f} 秒")
        _run_preflight_check(
            [test_python, os.fspath(readiness_script)],
            cwd=PROJECT_ROOT,
            check=True,
        )
        log("发布就绪检查通过。")
    else:
        raise RuntimeError("缺少发布就绪检查脚本，已中止打包。")

    notice_flow_smoke = PROJECT_ROOT / "bin" / "tools" / "notice_flow_smoke.py"
    if notice_flow_smoke.exists():
        _run_preflight_check(
            [test_python, os.fspath(notice_flow_smoke)],
            cwd=PROJECT_ROOT,
            check=True,
        )
        log("通告链路静态烟测通过。")
    else:
        raise RuntimeError("缺少通告链路静态烟测脚本，已中止打包。")

    _run_preflight_check(
        [test_python, "-m", "unittest", "bin.test_notice_identity_boundaries"],
        cwd=PROJECT_ROOT,
        check=True,
    )
    log("通告 ID 边界测试通过。")

    _run_preflight_check(
        [test_python, "-m", "unittest",
         "bin.test_learning", "bin.test_learning_personal", "bin.test_learning_routes", "bin.test_learning_cloud", "bin.test_learning_self_service", "bin.test_learning_scope_boundaries",
         "bin.test_lighthouse_assistant", "bin.test_lighthouse_account_models", "bin.test_lighthouse_widget", "bin.test_lighthouse_appearance", "bin.test_lighthouse_appearance_routes", "bin.test_lighthouse_pending", "bin.test_lighthouse_scope",
         "bin.test_lighthouse_stream", "bin.test_lighthouse_fast_paths", "bin.test_lighthouse_notice_command_regression", "bin.test_notice_navigation_cache", "bin.test_lighthouse_workbench_shell", "bin.test_lighthouse_queries", "bin.test_lighthouse_api",
         "bin.test_lighthouse_question_intent", "bin.test_lighthouse_query_reliability", "bin.test_lighthouse_repair_routing",
         "bin.test_lighthouse_notice_counts", "bin.test_lighthouse_native_task_progress", "bin.test_lighthouse_basics", "bin.test_lighthouse_read_urls",
         "bin.test_lighthouse_gateway", "bin.test_lighthouse_openclaw", "bin.test_lighthouse_runtime", "bin.test_lighthouse_startup", "bin.test_lighthouse_startup_log", "bin.test_lighthouse_public", "bin.test_lighthouse_distribution", "bin.test_lighthouse_skills", "bin.test_lighthouse_module_skills", "bin.test_lighthouse_imported_skills", "bin.test_lighthouse_shared_skills", "bin.test_lighthouse_commands", "bin.test_lighthouse_skill_routes",
         "bin.test_lighthouse_agent", "bin.test_lighthouse_agent_boundaries", "bin.test_lighthouse_agent_workflows",
         "bin.test_lighthouse_guard_fields", "bin.test_lighthouse_guard_template_fields",
         "bin.test_lighthouse_mop_fields", "bin.test_lighthouse_mop_workflows", "bin.test_lighthouse_plan_workflows",
         "bin.test_lighthouse_plan_rule_editor", "bin.test_lighthouse_daily_water_workflows", "bin.test_lighthouse_upload_fields",
         "bin.test_lighthouse_notice_image_workflows", "bin.test_lighthouse_cabinet_text_workflows", "bin.test_lighthouse_cabinet_batch_actions",
         "bin.test_lighthouse_cabinet_edit_fields", "bin.test_lighthouse_cabinet_edit_workflows", "bin.test_lighthouse_cabinet_proof_workflows",
         "bin.test_lighthouse_interactive_intent", "bin.test_event_month_selection", "bin.test_lighthouse_reference_ids",
         "bin.test_lighthouse_business_matrix", "bin.test_lighthouse_reference_workflows",
         "bin.test_lighthouse_drill_configuration_fields", "bin.test_lighthouse_drill_configuration_workflows",
         "bin.test_lighthouse_creation_fields", "bin.test_lighthouse_creation_workflows", "bin.test_lighthouse_drill_upload",
         "bin.test_lighthouse_water_fields", "bin.test_lighthouse_water_workflows",
         "bin.test_lighthouse_download_links", "bin.test_lighthouse_download_workflows", "bin.test_lighthouse_query_pages", "bin.test_lighthouse_question_bank",
         "bin.test_lighthouse_question_materials", "bin.test_lighthouse_work_order_queries",
         "bin.test_lighthouse_business_addresses", "bin.test_lighthouse_interactive_coverage",
         "bin.test_lighthouse_dynamic_endpoints",
         "bin.test_lighthouse_signature_usage_workflows", "bin.test_message_delivery", "bin.test_notice_alert_tags",
         "bin.test_lighthouse_plan_edit", "bin.test_lighthouse_secure_navigation", "bin.test_lighthouse_file_forms", "bin.test_lighthouse_repair_people", "bin.test_lighthouse_repair_relations", "bin.test_lighthouse_repair_catalog", "bin.test_lighthouse_repair_prefill", "bin.test_lighthouse_notice_fields", "bin.test_lighthouse_notice_workflows", "bin.test_lighthouse_notice_sop", "bin.test_lighthouse_notice_binding", "bin.test_lighthouse_notice_identity", "bin.test_lighthouse_notice_identity_workflows", "bin.test_lighthouse_event_transfer",
         "bin.test_planned_notices", "bin.test_lighthouse_planned", "bin.test_lighthouse_planned_match",
                "bin.test_notice_panel", "bin.test_notice_panel_data", "bin.test_link_directory",
                "bin.test_feishu_assistant", "bin.test_lighthouse_unlimited",
                "bin.test_assistant_tables", "bin.test_lighthouse_people_stats",
         "bin.test_lighthouse_frontend_contracts", "bin.test_lighthouse_frontend_coverage"],
        cwd=PROJECT_ROOT,
        check=True,
    )
    log("画像学练与灯塔助手专项测试通过。")
    _run_preflight_check(
        [test_python, "-m", "unittest", "bin.test_openclaw_service", "bin.test_openclaw_service_client",
         "bin.test_openclaw_service_launcher", "bin.test_openclaw_service_store", "bin.test_openclaw_service_update",
         "bin.test_openclaw_backend_proxy", "bin.test_openclaw_packaging_imports",
         "bin.test_openclaw_gateway_log",
         "bin.test_openclaw_connection_pool", "bin.test_openclaw_plan_nonblocking",
         "bin.test_openclaw_plan_io", "bin.test_openclaw_early_exit",
         "bin.test_openclaw_portal_startup", "bin.test_openclaw_python_entry"], cwd=PROJECT_ROOT, check=True,
    )
    log("助手后台、统一启动、迁移、权限代理与更新专项测试通过。")

    _run_preflight_check(
        [test_python, "-m", "unittest", "bin.test_plan_convergence", "bin.test_plan_convergence_stability", "bin.test_plan_convergence_points_adapter", "bin.test_plan_convergence_notice_results", "bin.test_plan_convergence_send", "bin.test_plan_convergence_expiry", "bin.test_convergence_confirmation_flows"],
        cwd=PROJECT_ROOT,
        check=True,
    )
    log("计划收敛审查专项测试通过。")

    _run_preflight_check(
        [test_python, "-m", "unittest", "bin.test_submission_reliability",
         "bin.test_event_remote_atomicity", "bin.test_notice_upload_reliability", "bin.test_notice_undo",
         "bin.test_repair_snapshot_cache", "bin.test_repair_project_identity", "bin.test_event_repair_id_rule", "bin.test_process_lifetime"],
        cwd=PROJECT_ROOT,
        check=True,
    )
    log("通告与维修可靠性、恢复及进程退出测试通过。")

    _run_preflight_check(
        [test_python, "-m", "unittest", "bin.test_critical_guard"],
        cwd=PROJECT_ROOT,
        check=True,
    )
    log("重保管理专项测试通过。")

    _run_preflight_check(
        [test_python, "-m", "unittest", "bin.test_transport_safety", "bin.test_release_readiness", "bin.test_dependency_bootstrap", "bin.test_feishu_credentials", "bin.test_cabinet_guest"],
        cwd=PROJECT_ROOT,
        check=True,
    )
    log("补丁传输与打包完整性测试通过。")

    log("打包前自动测试完成。")


def _cleanup_vue_dist_assets() -> None:
    dist_dir = PROJECT_ROOT / "bin" / "lan_bitable_template_portal" / "frontend" / "dist"
    index_path = dist_dir / "index.html"
    assets_dir = dist_dir / "assets"
    if not index_path.exists() or not assets_dir.exists():
        return
    reachable = referenced_assets(PROJECT_ROOT)
    removed = 0
    for path in assets_dir.glob("*"):
        if not path.is_file():
            continue
        if path.relative_to(PROJECT_ROOT) in reachable:
            continue
        if path.suffix.lower() not in {".js", ".css"}:
            continue
        path.unlink()
        removed += 1
    if removed:
        log(f"已清理 Vue dist 未引用旧资源: {removed} 个。")


def _is_under_bin_build_or_dist(path: Path) -> bool:

    parts = path.parts

    for i, part in enumerate(parts[:-1]):

        if part != "bin":

            continue

        next_part = parts[i + 1]

        if next_part in {"build", "dist"}:

            return True

    return False





def _is_excluded(
    path: Path, *, root: Path = PROJECT_ROOT, exclude_venv: bool = False, relative_path: Path | None = None
) -> bool:

    try:
        relative_path = relative_path if relative_path is not None else path.resolve().relative_to(root.resolve())
        parts = set(relative_path.parts)
    except Exception:
        parts = set(path.parts)

    if _is_development_only_path(path, root, relative_path=relative_path):

        return True

    if _is_runtime_data_path(path, root, relative_path=relative_path):

        return True

    if parts & EXCLUDE_DIR_NAMES:

        return True

    if exclude_venv and ".venv" in parts:

        return True

    if "build_output" in parts:

        return True

    if path.name in EXCLUDE_FILES:

        return True

    if path.name.endswith((".pyc", ".pyo")):

        return True

    if _is_under_bin_build_or_dist(path):

        return True

    return False
def _iter_project_files(root: Path, *, exclude_venv: bool = False) -> list[Path]:
    files: list[Path] = []

    root = root.resolve()

    for dirpath, dirnames, filenames in os.walk(root):
        base = Path(dirpath)
        # Resolve each directory once, including junctions; ordinary files do
        # not need repeated Windows final-path lookups through every ancestor.
        rel_base = base.resolve().relative_to(root)
        kept_dirnames: list[str] = []
        for dirname in dirnames:
            child = base / dirname
            if rel_base == Path(".") and dirname in EXCLUDE_TOP_LEVEL:
                continue
            if dirname in EXCLUDE_DIR_NAMES:
                continue
            if exclude_venv and dirname == ".venv":
                continue
            if dirname == "build_output":
                continue
            relative = child.resolve().relative_to(root) if child.is_symlink() or child.is_junction() else rel_base / dirname
            if _is_development_only_path(child, root, relative_path=relative):
                continue
            if _is_runtime_data_path(child, root, relative_path=relative):
                continue
            if _is_under_bin_build_or_dist(child):
                continue
            kept_dirnames.append(dirname)
        dirnames[:] = kept_dirnames

        for filename in filenames:
            path = base / filename
            if filename in EXCLUDE_TOP_LEVEL and base == root:
                continue
            if path.suffix.lower() == ".zip" and base == root:
                continue
            relative = path.resolve().relative_to(root) if path.is_symlink() else rel_base / filename
            if _is_excluded(path, root=root, exclude_venv=exclude_venv, relative_path=relative):
                continue
            files.append(path)

    return files





def _include_frontend_generation(root: Path, patch_dir: Path) -> int:
    """Make an entry-changing patch independent of older frontend generations."""
    if not (patch_dir / FRONTEND_INDEX).is_file():
        return 0
    copied = 0
    for rel in referenced_assets(root):
        dest = patch_dir / rel
        if dest.is_file():
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(root / rel, dest)
        copied += 1
    referenced_assets(patch_dir)
    return copied





def _has_code_changes(baseline_dir: Path | None, *, exclude_venv: bool = True) -> bool:

    if not baseline_dir or not baseline_dir.exists():

        return True

    baseline_files: dict[Path, Path] = {}

    for path in _iter_project_files(baseline_dir, exclude_venv=exclude_venv):

        baseline_files[path.relative_to(baseline_dir)] = path



    current_files = _iter_project_files(PROJECT_ROOT, exclude_venv=exclude_venv)

    current_set = {p.relative_to(PROJECT_ROOT) for p in current_files}



    for src in current_files:

        rel = src.relative_to(PROJECT_ROOT)

        if rel.suffix.lower() != ".py":

            continue

        old = baseline_files.get(rel)

        if not old or not old.exists():

            return True

        try:

            if _hash_file(src) != _hash_file(old):

                return True

        except Exception:

            return True



    for rel in baseline_files.keys():

        if rel.suffix.lower() != ".py":

            continue

        if rel not in current_set:

            return True



    return False





def _has_ui_changes(baseline_dir: Path | None) -> bool:

    if not baseline_dir or not baseline_dir.exists():

        return True



    tracked_roots = (

        Path("bin") / "upload_event_module" / "ui",

        Path("bin") / "upload_event_module" / "web",

    )



    def _collect(root: Path) -> dict[Path, Path]:

        out: dict[Path, Path] = {}

        for rel_root in tracked_roots:

            base = root / rel_root

            if not base.exists():

                continue

            for path in base.rglob("*"):

                if path.is_file():

                    out[path.relative_to(root)] = path

        return out



    current_files = _collect(PROJECT_ROOT)

    baseline_files = _collect(baseline_dir)

    if not current_files and not baseline_files:

        return False



    all_paths = set(current_files.keys()) | set(baseline_files.keys())

    for rel in all_paths:

        current_path = current_files.get(rel)

        baseline_path = baseline_files.get(rel)

        if not current_path or not baseline_path:

            return True

        try:

            if _hash_file(current_path) != _hash_file(baseline_path):

                return True

        except Exception:

            return True

    return False





def _hash_file(path: Path) -> str:

    hasher = hashlib.sha256()

    with path.open("rb") as f:

        for chunk in iter(lambda: f.read(1024 * 1024), b""):

            hasher.update(chunk)

    return hasher.hexdigest()





def _cleanup_old_patch_zips() -> int:

    if not BUILD_DIR.exists():

        return 0

    removed = 0

    for zip_path in BUILD_DIR.glob("*_patch_only.zip"):

        if not zip_path.is_file():

            continue

        try:

            zip_path.unlink()

            removed += 1

        except Exception as exc:

            log(f"跳过删除旧补丁压缩包 {zip_path.name}: {exc}")

    return removed





def _zip_patch_dir(patch_dir: Path) -> Path:

    zip_path = BUILD_DIR / f"{patch_dir.name}.zip"

    # Older clients extract below this layout. Reserve 64 characters for the install root.
    legacy_prefix = "x" * 64 + f"/bin/data/remote_patch/.extract_{zip_path.stem}/"
    for src in patch_dir.rglob("*"):
        if not src.is_file():
            continue
        member = f"{patch_dir.name}/{src.relative_to(patch_dir).as_posix()}"
        if len((legacy_prefix + member).encode("utf-16-le")) // 2 >= 260:
            raise RuntimeError(f"补丁路径过长，旧版 Windows 更新器无法解压；请缩短产物文件名: {member}")

    removed = _cleanup_old_patch_zips()

    if removed:

        log(f"已删除旧补丁压缩包: {removed}")

    with zipfile.ZipFile(
        zip_path,
        "w",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=1,
    ) as zf:

        for src in patch_dir.rglob("*"):

            if src.is_dir():

                continue

            arcname = f"{patch_dir.name}/{src.relative_to(patch_dir).as_posix()}"

            zf.write(src, arcname=arcname)

    with zipfile.ZipFile(zip_path, "r") as zf:

        broken = zf.testzip()

    if broken:

        zip_path.unlink(missing_ok=True)

        raise RuntimeError(f"补丁压缩包校验失败: {broken}")

    return zip_path





def _gitee_raw_base(repo_url: str, branch: str) -> str:

    cleaned = (repo_url or "").strip()

    if cleaned.endswith(".git"):

        cleaned = cleaned[:-4]

    parsed = urlparse(cleaned)

    if parsed.scheme and parsed.netloc:

        return f"{parsed.scheme}://{parsed.netloc}{parsed.path}/raw/{branch}"

    if cleaned.startswith("gitee.com/"):

        return f"https://{cleaned}/raw/{branch}"

    return f"{cleaned}/raw/{branch}"





def _write_latest_patch_manifest(

    patch_dir: Path,

    patch_zip: Path,

    *,

    gitee_repo: str,

    gitee_branch: str,

    gitee_subdir: str,

) -> tuple[Path, dict]:

    patch_meta_path = patch_dir / "bin" / "patch_meta.json"

    patch_meta = _read_build_meta(patch_meta_path)

    if not isinstance(patch_meta, dict):

        patch_meta = {}

    zip_sha256 = _hash_file(patch_zip)

    zip_size = patch_zip.stat().st_size

    raw_base = _gitee_raw_base(gitee_repo, gitee_branch).rstrip("/")

    subdir = (gitee_subdir or "").strip().strip("/")

    zip_url = (

        f"{raw_base}/{subdir}/{patch_zip.name}"

        if subdir

        else f"{raw_base}/{patch_zip.name}"

    )



    manifest = {

        "version": patch_meta.get("target_version", patch_dir.name),

        "major_version": _safe_int(

            patch_meta.get("major_version"), DEFAULT_MAJOR_VERSION

        ),

        "target_version": patch_meta.get("target_version", patch_dir.name),

        "target_display_version": patch_meta.get("target_display_version", ""),

        "target_ui_version": patch_meta.get("target_ui_version", ""),

        "target_patch_version": _safe_int(patch_meta.get("target_patch_version"), 0),

        "base_patch_version": _safe_int(patch_meta.get("base_patch_version"), 0),

        "base_ui_version": patch_meta.get("base_ui_version", ""),

        "ui_changed": bool(patch_meta.get("ui_changed")),

        "restart_required": bool(patch_meta.get("restart_required")),

        "force_ui_update": bool(patch_meta.get("force_ui_update")),

        "min_version": patch_meta.get("min_version", ""),

        "required_packages": patch_meta.get("required_packages", []),

        "module_to_package": patch_meta.get("module_to_package", {}),

        "python_version": patch_meta.get("python_version", ""),

        "zip_name": patch_zip.name,

        "zip_sha256": zip_sha256,

        "zip_size": zip_size,

        "zip_url": zip_url,

        "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),

    }

    manifest_path = BUILD_DIR / "latest_patch.json"

    manifest_path.write_text(

        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"

    )

    return manifest_path, manifest





def _remove_gitee_upload_dir(path: Path) -> None:
    if not path.exists():
        return
    if path.resolve().parent != BUILD_DIR.resolve() or not path.name.startswith(".gitee_upload_"):
        raise RuntimeError(f"拒绝清理非打包临时目录: {path}")

    def clear_readonly(func, name, error):
        if not isinstance(error, PermissionError):
            raise error
        os.chmod(name, 0o666)
        func(name)

    shutil.rmtree(path, onexc=clear_readonly)


def _upload_patch_to_gitee(

    patch_zip: Path,

    latest_manifest_path: Path,

    *,

    repo_url: str,

    branch: str,

    subdir: str,

    manifest_repo_path: str,

) -> bool:

    if not repo_url.strip():

        log("跳过上传：Gitee 仓库地址为空。")

        return False

    patch_subdir_name = (subdir or "").strip().strip("/")
    manifest_name = manifest_repo_path.strip().strip("/")
    patch_repo_name = f"{patch_subdir_name}/{patch_zip.name}" if patch_subdir_name else patch_zip.name
    git_http = ["git"]
    if os.name == "nt":
        git_http += ["-c", "http.sslBackend=openssl", "-c", "http.version=HTTP/1.1"]

    for attempt in range(3):
        temp_repo = BUILD_DIR / f".gitee_upload_{time.time_ns()}"
        clone_ok = _run_cmd(
            git_http + ["clone", "--depth", "1", "--single-branch", "--filter=blob:none",
                        "--no-checkout", "-b", branch, repo_url, str(temp_repo)]
        )
        if clone_ok:
            clone_ok = _run_cmd(
                ["git", "sparse-checkout", "set", "--no-cone", f"/{manifest_name}", f"/{patch_repo_name}"],
                cwd=temp_repo,
            ) and _run_cmd(git_http + ["read-tree", "-mu", "HEAD"], cwd=temp_repo)
        if clone_ok:
            break
        _remove_gitee_upload_dir(temp_repo)
        if attempt < 2:
            log(f"Gitee 稀疏克隆中断，稍后重试（{attempt + 1}/3）。")
            time.sleep(2 * (attempt + 1))
    else:
        log("Gitee 上传失败：稀疏克隆连续 3 次失败。")
        return False



    try:

        patch_subdir = temp_repo / patch_subdir_name

        patch_subdir.mkdir(parents=True, exist_ok=True)

        shutil.copy2(patch_zip, patch_subdir / patch_zip.name)

        ok, tracked_paths = _run_cmd_capture(
            ["git", "ls-tree", "-r", "--name-only", "HEAD", "--", patch_subdir_name or "."],
            cwd=temp_repo,
        )
        if not ok:
            log("Gitee 上传失败：无法读取旧补丁列表。")
            return False
        versioned_zips = sorted(
            {patch_repo_name, *(path for path in tracked_paths.splitlines()
                                if path.endswith("_patch_only.zip")
                                and Path(path).name != LEGACY_PATCH_ZIP_NAME)},
            reverse=True,
        )
        removed_old = 0
        for old_zip in versioned_zips[REMOTE_PATCH_HISTORY:]:
            if not _run_cmd(["git", "rm", "--cached", "--sparse", "--", old_zip], cwd=temp_repo):
                log(f"Gitee 上传失败：无法移除旧补丁 {old_zip}。")
                return False
            removed_old += 1
        if removed_old:
            log(f"Gitee 仓库中已删除过期补丁压缩包: {removed_old}")



        manifest_target = temp_repo / manifest_name

        manifest_target.parent.mkdir(parents=True, exist_ok=True)

        shutil.copy2(latest_manifest_path, manifest_target)



        if not _run_cmd(
            ["git", "add", "--sparse", "-f", "--", patch_repo_name, manifest_name],
            cwd=temp_repo,
        ):
            log("Gitee 上传失败：无法暂存补丁文件。")
            return False

        commit_msg = f"chore: publish patch {patch_zip.stem}"

        committed = _run_cmd(

            ["git", "commit", "-m", commit_msg],

            cwd=temp_repo,

            log_output_on_error=False,

        )

        if not committed:

            ok, status_output = _run_cmd_capture(

                ["git", "status", "--porcelain"], cwd=temp_repo

            )

            if ok and not status_output.strip():
                ok, head_manifest = _run_cmd_capture(
                    ["git", "show", f"HEAD:{manifest_name}"], cwd=temp_repo
                )
                try:
                    head_payload = json.loads(head_manifest) if ok else {}
                except (TypeError, ValueError):
                    head_payload = {}
                patch_exists = _run_cmd(
                    ["git", "cat-file", "-e", f"HEAD:{patch_repo_name}"],
                    cwd=temp_repo,
                    log_output_on_error=False,
                )
                expected_payload = json.loads(
                    latest_manifest_path.read_text(encoding="utf-8")
                )
                if not patch_exists or any(
                    head_payload.get(key) != expected_payload.get(key)
                    for key in ("target_patch_version", "zip_name", "zip_sha256")
                ):
                    log("Gitee 上传失败：暂存区为空，但当前提交不包含本次补丁。")
                    return False
                pushed = _run_cmd(["git", "push", "origin", branch], cwd=temp_repo)
                if pushed:
                    log("Gitee 上传：本次补丁已提交，已补做远端推送。")
                    return True
                log("Gitee 上传失败：补做远端推送失败。")
                return False

            log("Gitee 上传失败：提交变更失败。")

            return False

        pushed = _run_cmd(["git", "push", "origin", branch], cwd=temp_repo)

        if pushed:

            log(f"Gitee 上传成功: {repo_url} ({branch})")

            return True

        log("Gitee 上传失败：推送失败。")

        return False

    finally:

        try:
            _remove_gitee_upload_dir(temp_repo)
        except OSError as exc:
            log(f"Gitee 上传临时目录清理失败: {exc}")


def _load_existing_patch() -> tuple[Path, Path, dict]:
    manifest_path = BUILD_DIR / "latest_patch.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict) or _safe_int(manifest.get("target_patch_version"), 0) <= 0:
        raise RuntimeError("本地补丁清单格式或补丁号无效。")
    name = manifest.get("zip_name")
    if (not isinstance(name, str) or not name or name != Path(name).name
            or "\\" in name or name != f"{manifest.get('target_version', '')}_patch_only.zip"):
        raise RuntimeError("本地补丁清单的 ZIP 文件名无效。")
    patch_zip = BUILD_DIR / name
    if (not patch_zip.is_file()
            or patch_zip.stat().st_size != _safe_int(manifest.get("zip_size"), -1)
            or _hash_file(patch_zip) != str(manifest.get("zip_sha256", "")).lower()):
        raise RuntimeError("本地补丁 ZIP 与清单不一致，停止上传。")
    return patch_zip, manifest_path, manifest


def _publish_patch(
    patch_zip: Path, manifest_path: Path, manifest: dict, *,
    repo_url: str, branch: str, subdir: str, manifest_repo_path: str,
) -> None:
    if not _upload_patch_to_gitee(
        patch_zip, manifest_path, repo_url=repo_url, branch=branch,
        subdir=subdir, manifest_repo_path=manifest_repo_path,
    ):
        raise RuntimeError("Gitee 补丁上传失败，用户尚无法更新；本地补丁已保留。")
    _verify_published_patch(
        manifest, repo_url=repo_url, branch=branch, manifest_path=manifest_repo_path,
    )


def _verify_published_patch(manifest: dict, *, repo_url: str, branch: str, manifest_path: str) -> None:
    from urllib.error import HTTPError, URLError
    from urllib.request import Request, urlopen

    manifest_url = (
        f"{_gitee_raw_base(repo_url, branch).rstrip('/')}/"
        f"{manifest_path.strip().strip('/')}"
    )
    expected_hash = str(manifest["zip_sha256"]).lower()
    expected_size = int(manifest["zip_size"])
    last_error = ""
    for attempt in range(10):
        stage = "远端清单"
        try:
            manifest_request = Request(
                manifest_url + ("&" if "?" in manifest_url else "?")
                + urlencode({"_clipflow": str(time.time_ns())}),
                headers={"Cache-Control": "no-cache"},
            )
            with urlopen(manifest_request, timeout=30) as response:
                published = json.load(response)
            if (
                published.get("target_patch_version") != manifest["target_patch_version"]
                or published.get("zip_sha256", "").lower() != expected_hash
                or published.get("zip_name") != manifest["zip_name"]
            ):
                raise RuntimeError("远端清单尚未更新到本次补丁")
            hasher = hashlib.sha256()
            size = 0
            stage = "补丁文件"
            with urlopen(Request(manifest["zip_url"]), timeout=60) as archive:
                for chunk in iter(lambda: archive.read(1024 * 1024), b""):
                    size += len(chunk)
                    if size > expected_size:
                        raise RuntimeError("远端补丁大小超出本地清单")
                    hasher.update(chunk)
            if size != expected_size or hasher.hexdigest().lower() != expected_hash:
                raise RuntimeError("远端补丁 ZIP 的大小或 SHA256 与本地清单不一致")
            log("Gitee 补丁下载核验通过。")
            return
        except HTTPError as exc:
            with exc:
                detail = exc.read(2048).decode("utf-8", errors="replace").lower()
            if exc.code in {401, 403}:
                reason = ("Gitee 拒绝匿名下载大文件，需要登录；请移除补丁中的安装器等非运行文件后重新打包"
                          if "large file require login" in detail
                          else "Gitee 拒绝匿名下载，请检查仓库公开权限或下载限制")
                raise RuntimeError(
                    f"Gitee 已推送但下载核验失败，暂不要通知用户更新：{stage} HTTP {exc.code}，{reason}。"
                    "本地补丁已保留，未将推送成功视为更新成功。"
                ) from exc
            last_error = f"{stage} HTTP {exc.code}"
            log(f"下载核验暂未通过（{attempt + 1}/10）：{last_error}")
            if attempt < 9:
                time.sleep(5)
        except (URLError, OSError, ValueError, RuntimeError) as exc:
            last_error = f"{stage}：{exc}"
            log(f"下载核验暂未通过（{attempt + 1}/10）：{last_error}")
            if attempt < 9:
                time.sleep(5)
    raise RuntimeError(f"Gitee 已推送但下载核验失败，暂不要通知用户更新：{last_error}")





def copy_project(dist_dir: Path) -> None:

    if dist_dir.exists():

        shutil.rmtree(dist_dir)

    dist_dir.mkdir(parents=True, exist_ok=True)



    for item in PROJECT_ROOT.iterdir():

        if item.name in EXCLUDE_TOP_LEVEL or _is_development_only_path(
            item, PROJECT_ROOT
        ):

            continue

        if item.is_file() and item.suffix.lower() == ".zip":

            continue



        target = dist_dir / item.name

        if item.is_dir():

            shutil.copytree(item, target, ignore=_ignore_names, dirs_exist_ok=True)

        else:

            shutil.copy2(item, target)

    _include_assistant_skills(PROJECT_ROOT, dist_dir)
    _assert_no_runtime_data_in_output(dist_dir, "完整构建产物")
    _assert_no_development_files_in_output(dist_dir, "完整构建产物")





def find_latest_full_dist(exclude: Path | None = None) -> Path | None:

    if not BUILD_DIR.exists():

        return None

    candidates = [

        p

        for p in BUILD_DIR.iterdir()

        if p.is_dir() and p.name.startswith(f"{APP_NAME}_portable_")

    ]

    if exclude:

        candidates = [p for p in candidates if p.resolve() != exclude.resolve()]

    if not candidates:

        return None

    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)

    return candidates[0]





def find_base_dist(base_build_id: str) -> Path | None:

    if not base_build_id:

        return None

    direct = BUILD_DIR / base_build_id

    if direct.exists():

        return direct

    if not BUILD_DIR.exists():

        return None

    for candidate in BUILD_DIR.iterdir():

        if not candidate.is_dir():

            continue

        meta = _read_build_meta(candidate / "bin" / "build_meta.json")

        if isinstance(meta, dict) and meta.get("build_id") == base_build_id:

            return candidate

    return None





def _detect_base_build_id(default_build_id: str) -> str:

    """Auto-detect an existing base build id to avoid manual edits."""

    if not BUILD_DIR.exists():

        return default_build_id

    base_pattern = re.compile(rf"^{re.escape(APP_NAME)}_V\d+$", re.IGNORECASE)

    candidates: list[Path] = []

    for candidate in BUILD_DIR.iterdir():

        if candidate.is_dir() and base_pattern.match(candidate.name):

            candidates.append(candidate)

    if not candidates:

        return default_build_id

    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)

    return candidates[0].name





def _include_assistant_skills(root: Path, destination: Path) -> int:
    """Keep approved resources byte-identical without long extracted paths."""
    skills = root / IMPORTED_SKILLS_DIR.parent
    registry = skills / "workbuddy-registry.json"
    if not registry.is_file():
        return 0
    stamp = registry.stat()
    resources = _registered_skill_resources(str(registry.resolve()), stamp.st_mtime_ns, stamp.st_size)
    if not resources:
        return 0
    target = destination / IMPORTED_SKILLS_DIR.parent / "workbuddy.zip"
    target.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(target, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for relative in sorted(resources):
            source = skills / relative
            current = source
            while current != skills:
                if current.is_symlink() or current.is_junction():
                    raise RuntimeError("Approved skill resource contains an external link")
                current = current.parent
            if not source.resolve().is_relative_to(skills.resolve()) or not source.is_file():
                raise RuntimeError("Approved skill resource is missing")
            if source.stat().st_size > 512 * 1024:
                raise RuntimeError("Approved skill resource exceeds the reader limit")
            archive.write(source, relative)
    if target.stat().st_size > 20 * 1024 * 1024:
        raise RuntimeError("Approved skill bundle exceeds the reader limit")
    return len(resources)


def build_patch(

    dist_dir: Path,

    baseline_dir: Path | None,

    dist_name: str,

    *,

    exclude_venv: bool = True,

    baseline_meta_path: Path | None = None,

    venv_hash: str = "",

    base_venv_hash: str = "",

    include_venv: bool = False,

    major_version: int = DEFAULT_MAJOR_VERSION,

    base_patch_version: int = 0,

    target_patch_version: int = 1,

    target_display_version: str = "",

    base_ui_version: str = "",

    target_ui_version: str = "",

    ui_changed: bool = False,

    force_ui_update: bool = False,

    patch_min_version: str = "",

    required_packages: list[str] | None = None,

    module_to_package: dict[str, str] | None = None,

    python_version: str = "",

) -> tuple[int, int, int]:

    patch_dir = BUILD_DIR / (dist_name + "_patch_only")

    if patch_dir.exists():

        shutil.rmtree(patch_dir)

    patch_dir.mkdir(parents=True, exist_ok=True)



    baseline_files: dict[Path, Path] = {}

    if baseline_dir and baseline_dir.exists():

        for path in _iter_project_files(baseline_dir, exclude_venv=exclude_venv):

            baseline_files[path.relative_to(baseline_dir)] = path



    changed = 0

    added = 0

    forced_py = 0

    current_files = _iter_project_files(PROJECT_ROOT, exclude_venv=exclude_venv)

    # Ship the complete runtime, even when this machine's baseline was updated.
    # The baseline is used only to identify retired files, never to omit files.
    for src in current_files:
        rel = src.relative_to(PROJECT_ROOT)
        if rel.is_relative_to(IMPORTED_SKILLS_DIR):
            continue
        if rel.suffix.lower() == ".py":
            forced_py += 1
        if rel in baseline_files:
            changed += 1
        else:
            added += 1
        dest = patch_dir / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)



    bundled_frontend_assets = _include_frontend_generation(PROJECT_ROOT, patch_dir)
    if bundled_frontend_assets:
        log(f"已补齐当前前端资源: {bundled_frontend_assets} 个。")

    _include_assistant_skills(PROJECT_ROOT, patch_dir)

    deleted = 0

    deleted_paths: list[str] = []

    if baseline_files:

        current_set = {

            p.relative_to(PROJECT_ROOT)

            for p in current_files

        }

        for rel in baseline_files.keys():

            # Installed clients retain the previous entry's hashed resources.
            if rel not in current_set and not is_frontend_asset(rel):

                deleted += 1

                deleted_paths.append(str(rel))


    # Cumulative patches must also retire entries added after the base release.
    for retired in ("启动程序openclaw.bat", "启动程序openclaw.py"):
        if retired not in deleted_paths:
            deleted_paths.append(retired)
            deleted += 1


    manifest = patch_dir / "patch_manifest.txt"

    with manifest.open("w", encoding="utf-8") as f:

        f.write("Patch Summary\n")

        f.write(f"Baseline: {baseline_dir or 'None'}\n")

        f.write(f"Added: {added}\n")

        f.write(f"Changed: {changed}\n")

        f.write(f"Deleted: {deleted}\n\n")

        f.write(f"ForcedPy: {forced_py}\n")

        f.write(f"UiChanged: {bool(ui_changed)}\n")

        f.write(f"ForceUiUpdate: {bool(force_ui_update)}\n")

        f.write(f"BaseUiVersion: {base_ui_version}\n")

        f.write(f"TargetUiVersion: {target_ui_version}\n\n")

        if deleted_paths:

            f.write("Deleted files (remove manually if needed):\n")

            for rel in deleted_paths:

                f.write(f"{rel}\n")



    base_build_id = ""

    if baseline_meta_path and baseline_meta_path.exists():

        base_meta = _read_build_meta(baseline_meta_path)

        if isinstance(base_meta, dict):

            base_build_id = base_meta.get("build_id", "") or ""

            if not base_venv_hash:

                base_venv_hash = base_meta.get("venv_hash", "") or ""

    elif baseline_dir:

        base_meta = _read_build_meta(baseline_dir / "bin" / "build_meta.json")

        if isinstance(base_meta, dict):

            base_build_id = base_meta.get("build_id", "") or ""

            if not base_venv_hash:

                base_venv_hash = base_meta.get("venv_hash", "") or ""

    write_patch_meta(

        patch_dir,

        base_build_id=base_build_id,

        min_version=patch_min_version,

        target_build_id=dist_name,

        venv_hash=venv_hash,

        base_venv_hash=base_venv_hash,

        include_venv=include_venv,

        major_version=major_version,

        base_patch_version=base_patch_version,

        target_patch_version=target_patch_version,

        target_display_version=target_display_version,

        base_ui_version=base_ui_version,

        target_ui_version=target_ui_version,

        ui_changed=ui_changed,

        force_ui_update=force_ui_update,

        required_packages=required_packages,

        module_to_package=module_to_package,

        python_version=python_version,

    )



    return added, changed, deleted





def _ensure_lighthouse_distribution(python_exe):
    spec = BIN_DIR / 'openclaw_service/assistant/openclaw/distribution.json'
    if spec.is_file():
        subprocess.run([str(python_exe), '-c',
            "import json,sys;sys.path.insert(0,'bin');from lan_bitable_template_portal.lighthouse_distribution import SPEC,validate_spec;validate_spec(json.loads(SPEC.read_text(encoding='utf-8')))"],
            cwd=PROJECT_ROOT, check=True)
        log('灯塔助手固定依赖镜像清单校验通过（运行目录不进入补丁）。')
        return
    runtime = BUILD_DIR / 'lighthouse_openclaw'
    subprocess.run([str(python_exe), str(BIN_DIR / 'tools/prepare_lighthouse_openclaw.py'), '--destination', str(runtime)],
        cwd=PROJECT_ROOT, check=True)
    subprocess.run([str(python_exe), str(BIN_DIR / 'tools/publish_lighthouse_runtime.py'), '--runtime', str(runtime),
        '--output', str(BUILD_DIR / 'lighthouse_dependencies'), '--publish'], cwd=PROJECT_ROOT, check=True)


@contextmanager
def _packaging_lock():
    BUILD_DIR.mkdir(parents=True, exist_ok=True)
    with (BUILD_DIR / ".package.lock").open("a+b") as handle:
        if not handle.tell():
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        if os.name == "nt":
            import msvcrt
            acquire = lambda: msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            release = lambda: msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            acquire = lambda: fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            release = lambda: fcntl.flock(handle, fcntl.LOCK_UN)
        try:
            acquire()
        except OSError as exc:
            raise RuntimeError("已有打包或发布任务正在运行，请勿同时启动多个打包程序；单次任务已启用并行测试。") from exc
        try:
            yield
        finally:
            handle.seek(0)
            release()


def main() -> None:

    parser = argparse.ArgumentParser(

        description="Package ClipFlow into a portable folder."

    )

    parser.add_argument(

        "--name",

        default="",

        help="Output folder name. Defaults to ClipFlow_portable_YYYYmmdd_HHMMSS",

    )

    parser.add_argument(

        "--baseline",

        default="",

        help=(

            "Baseline folder to compare for patch_only. "

            "Defaults to latest build_output/ClipFlow_portable_*"

        ),

    )

    parser.add_argument(

        "--baseline-meta",

        default="",

        help=(

            "Path to a baseline build_meta.json (usually from the current online version). "

            "Used to infer patch/dependency/UI comparison baseline."

        ),

    )

    parser.add_argument(

        "--skip-watchdog",

        action="store_true",

        help="Legacy option: skip runtime dependency install.",

    )

    parser.add_argument(

        "--skip-runtime-deps",

        action="store_true",

        help="Skip installing runtime dependencies into portable venv.",

    )

    parser.add_argument(

        "--patch-include-venv",

        action="store_true",

        help="Force include .venv in patch_only (auto by default).",

    )

    parser.add_argument(

        "--strict-min-version",

        action="store_true",

        help=(

            "Write patch_meta.min_version for strict baseline matching. "

            "Default is non-strict so users can apply the patch directly."

        ),

    )

    parser.add_argument(

        "--force-ui-update",

        action="store_true",

        help=(

            "Force this patch to be treated as a UI update. "

            "Client will perform restart-required update even if ui_version is unchanged."

        ),

    )

    parser.add_argument(

        "--gitee-repo",

        default=DEFAULT_GITEE_REPO,

        help=f"Gitee repository url. Default: {DEFAULT_GITEE_REPO}",

    )

    parser.add_argument(

        "--gitee-branch",

        default=DEFAULT_GITEE_BRANCH,

        help=f"Gitee branch. Default: {DEFAULT_GITEE_BRANCH}",

    )

    parser.add_argument(

        "--gitee-subdir",

        default=DEFAULT_GITEE_SUBDIR,

        help=f"Patch zip subdir in repo. Default: {DEFAULT_GITEE_SUBDIR}",

    )

    parser.add_argument(

        "--gitee-manifest-path",

        default=DEFAULT_GITEE_MANIFEST_PATH,

        help=f"Latest patch manifest path in repo. Default: {DEFAULT_GITEE_MANIFEST_PATH}",

    )
    parser.add_argument(

        "--preflight-test",

        action="store_true",

        help="Run packaging preflight checks and exit without building.",

    )
    parser.add_argument(

        "--skip-preflight",

        action="store_true",

        help="Skip mandatory packaging preflight checks. Only use for local debugging.",

    )
    parser.add_argument(

        "--skip-gitee-upload",

        action="store_true",

        help="Build patch locally but skip cloning/pushing the Gitee update repository.",

    )
    parser.add_argument(
        "--retry-upload", action="store_true",
        help="Upload and verify the existing build_output/latest_patch.json and ZIP without rebuilding.",
    )

    args = parser.parse_args()

    if args.retry_upload:
        if args.skip_gitee_upload:
            parser.error("--retry-upload cannot be combined with --skip-gitee-upload")
        patch_zip, manifest_path, manifest = _load_existing_patch()
        log(f"重试上传现有补丁: {patch_zip.name}")
        _publish_patch(
            patch_zip, manifest_path, manifest,
            repo_url=args.gitee_repo, branch=args.gitee_branch,
            subdir=args.gitee_subdir, manifest_repo_path=args.gitee_manifest_path,
        )
        major_version = _safe_int(manifest.get("major_version"), DEFAULT_MAJOR_VERSION)
        _advance_local_patch_sequence(
            _patch_sequence_key(BASE_VERSION_ID, major_version),
            target_patch_version=_safe_int(manifest["target_patch_version"], 0),
            target_display_version=str(manifest.get("target_display_version", "")),
            major_version=major_version, base_build_id=BASE_VERSION_ID,
        )
        log("现有补丁上传并核验完成。")
        return

    if args.preflight_test:

        _run_packaging_preflight_tests()

        return

    if not args.skip_preflight:

        _run_packaging_preflight_tests()



    timestamp = time.strftime("%Y%m%d_%H%M%S")

    current_ui_version = _compute_ui_version(PROJECT_ROOT)

    base_build_id = _detect_base_build_id(BASE_VERSION_ID)

    BUILD_DIR.mkdir(parents=True, exist_ok=True)

    build_python = _find_build_python()

    skip_runtime_deps = bool(args.skip_watchdog or args.skip_runtime_deps)

    if build_python:

        log(f"使用打包解释器: {build_python}")

    if build_python and not skip_runtime_deps:

        log(f"正在校验并补齐打包解释器依赖: {build_python}")

        ensure_runtime_dependencies(build_python)

        if not _verify_runtime_imports(build_python, PROJECT_ROOT):

            raise RuntimeError("源码目录导入冒烟检查失败。")

        log("源码目录导入冒烟检查通过。")

    elif not build_python:

        log("未找到可用的打包解释器，跳过运行时依赖安装。")

    _ensure_lighthouse_distribution(build_python or sys.executable)

    current_venv_hash = ""

    if build_python:

        current_venv_hash = _get_venv_hash(build_python)

        if current_venv_hash:

            log("已识别当前运行时依赖哈希。")

        else:

            log("识别当前运行时依赖哈希失败。")



    base_dist_dir = find_base_dist(base_build_id)

    if base_dist_dir is None:

        dist_name = base_build_id

        dist_dir = BUILD_DIR / dist_name

        major_version = _extract_major_version(dist_name)

        log(f"未找到基线版本，正在创建基础构建: {dist_name}")

        if dist_dir.exists():

            shutil.rmtree(dist_dir)

        copy_project(dist_dir)

        if not skip_runtime_deps:

            if build_python:

                log(

                    f"正在使用打包解释器执行基础构建导入冒烟检查: {build_python}"

                )

                if not _verify_runtime_imports(build_python, dist_dir):

                    raise RuntimeError("基础构建导入冒烟检查失败。")

                log("基础构建导入冒烟检查通过。")

            else:

                raise RuntimeError("基础构建导入冒烟检查缺少可用的打包解释器。")

        write_build_meta(

            dist_dir,

            dist_name,

            venv_hash=current_venv_hash,

            major_version=major_version,

            patch_version=0,

            display_version=_build_display_version(major_version, 0, timestamp[:8]),

            ui_version=current_ui_version,

        )

        base_dist_dir = dist_dir

        log("基础构建已创建。")

        log("继续：基于新创建的基础构建生成补丁包。")

    dist_name = args.name or format_dist_name(timestamp, base_build_id)

    dist_dir = BUILD_DIR / dist_name



    log("当前为仅补丁模式：跳过完整构建输出。")



    if base_dist_dir is None:

        baseline_dir = None

    else:

        baseline_dir = base_dist_dir

    baseline_missing_runtime: list[str] = []

    if baseline_dir:

        log(f"正在基于基线版本生成累积补丁: {baseline_dir.name}")

    else:

        log("未找到基线版本，本次补丁将包含全部文件。")

    if args.baseline_meta:

        baseline_meta_path = Path(args.baseline_meta)

    elif baseline_dir:

        baseline_meta_path = baseline_dir / "bin" / "build_meta.json"

    else:

        baseline_meta_path = None

    baseline_build_meta: dict = {}

    base_venv_hash = ""

    if baseline_meta_path and baseline_meta_path.exists():

        baseline_build_meta = _read_build_meta(baseline_meta_path)

        if isinstance(baseline_build_meta, dict):

            base_venv_hash = baseline_build_meta.get("venv_hash", "") or ""

    elif baseline_dir:

        baseline_build_meta = _read_build_meta(baseline_dir / "bin" / "build_meta.json")

        if isinstance(baseline_build_meta, dict):

            base_venv_hash = baseline_build_meta.get("venv_hash", "") or ""



    major_version = _extract_major_version(base_build_id)

    base_patch_version = 0

    base_ui_version = ""

    if isinstance(baseline_build_meta, dict):

        major_version = _safe_int(

            baseline_build_meta.get("major_version"),

            _extract_major_version(base_build_id),

        )

        base_patch_version = _safe_int(

            baseline_build_meta.get("patch_version"),

            0,

        )

        base_ui_version = (baseline_build_meta.get("ui_version") or "").strip()

    patch_sequence_key = _patch_sequence_key(base_build_id, major_version)

    baseline_patch_version = base_patch_version

    base_patch_version = _resolve_base_patch_version(
        patch_sequence_key,
        baseline_patch_version=baseline_patch_version,
    )

    if base_patch_version > baseline_patch_version:

        log(
            "使用本地补丁序列状态: "
            f"{baseline_patch_version} -> {base_patch_version}"
        )

    target_patch_version = base_patch_version + 1

    target_display_version = _build_display_version(

        major_version,

        target_patch_version,

        timestamp[:8],

    )

    if base_venv_hash:

        log("已从基线元数据识别运行时依赖哈希。")

    force_ui_update = bool(DEFAULT_FORCE_UI_UPDATE or args.force_ui_update)
    # Forced restarts do not need a second scan before build_patch compares files.
    code_changed = False if force_ui_update else _has_code_changes(baseline_dir, exclude_venv=True)

    if not force_ui_update and not code_changed:

        log("相对基线未检测到 .py 代码变化。")

    deps_changed = False

    if current_venv_hash and base_venv_hash:

        deps_changed = current_venv_hash != base_venv_hash

    elif current_venv_hash and baseline_dir and not base_venv_hash:

        log("基线运行时依赖哈希不可用，按依赖已变化处理。")

        deps_changed = True

    if baseline_missing_runtime:

        deps_changed = True

    include_venv = args.patch_include_venv

    if deps_changed and not include_venv:

        log(

            "补丁模式：依赖已变化，但不包含 .venv 运行时目录（由用户侧自动安装依赖）。"

        )

    if include_venv:

        log("补丁将包含 .venv 运行时目录（--patch-include-venv）。")

    else:

        log("补丁将排除 .venv 运行时目录。")

    # Python code is loaded by the running Qt/backend processes, so every code
    # patch must restart even when the visible UI assets did not change.
    ui_changed = bool(force_ui_update or code_changed)

    if force_ui_update:

        log("已启用强制界面更新，本次补丁需要重启程序。")

    else:

        log("未启用强制界面更新；根据代码变化决定是否重启程序。")

    if ui_changed:

        log("检测到界面版本变化，本次补丁需要重启程序。")

    patch_min_version = ""

    if args.strict_min_version and isinstance(baseline_build_meta, dict):

        patch_min_version = (baseline_build_meta.get("build_id") or "").strip()

        if patch_min_version:

            log(f"已启用严格最小版本限制: {patch_min_version}")

    if not patch_min_version:

        log(

            "当前为非严格补丁模式：min_version 为空（适合手动分发补丁）。"

        )

    log("本次包含全部运行文件，不依赖用户已安装的补丁版本；保留用户数据与配置。")
    added, changed, deleted = build_patch(

        dist_dir,

        baseline_dir,

        dist_name,

        exclude_venv=not include_venv,

        baseline_meta_path=baseline_meta_path,

        venv_hash=current_venv_hash,

        base_venv_hash=base_venv_hash,

        include_venv=include_venv,

        major_version=major_version,

        base_patch_version=base_patch_version,

        target_patch_version=target_patch_version,

        target_display_version=target_display_version,

        base_ui_version=base_ui_version,

        target_ui_version=current_ui_version,

        ui_changed=ui_changed,

        force_ui_update=force_ui_update,

        patch_min_version=patch_min_version,

        required_packages=[

            pkg

            for pkg in dict.fromkeys(

                [

                    RUNTIME_MODULE_TO_PACKAGE[m]

                    for m in RUNTIME_PACKAGE_INSTALL_ORDER

                    if m in RUNTIME_MODULE_TO_PACKAGE

                ]

            )

        ],

        module_to_package=RUNTIME_MODULE_TO_PACKAGE,

        python_version=f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",

    )

    log(f"补丁构建完成。新增={added}，变更={changed}，删除={deleted}")



    patch_dir = BUILD_DIR / (dist_name + "_patch_only")

    _assert_no_runtime_data_in_output(patch_dir, "补丁产物")
    _assert_no_development_files_in_output(patch_dir, "补丁产物")

    patch_zip = _zip_patch_dir(patch_dir)

    log(f"补丁压缩包已生成: {patch_zip.name}")

    latest_manifest_path, latest_manifest = _write_latest_patch_manifest(

        patch_dir,

        patch_zip,

        gitee_repo=args.gitee_repo,

        gitee_branch=args.gitee_branch,

        gitee_subdir=args.gitee_subdir,

    )

    log(f"最新补丁清单已生成: {latest_manifest_path.name}")

    log(

        "补丁元数据摘要: "

        f"目标版本={latest_manifest.get('target_version')} "

        f"补丁号={latest_manifest.get('target_patch_version')} "

        f"界面变化={latest_manifest.get('ui_changed')}"

    )



    if AUTO_UPLOAD_GITEE and not args.skip_gitee_upload:
        _publish_patch(
            patch_zip, latest_manifest_path, latest_manifest,
            repo_url=args.gitee_repo, branch=args.gitee_branch,
            subdir=args.gitee_subdir, manifest_repo_path=args.gitee_manifest_path,
        )

    else:
        reason = "--skip-gitee-upload" if args.skip_gitee_upload else "AUTO_UPLOAD_GITEE=False"
        log(f"已跳过 Gitee 上传（{reason}）。")

    if _advance_local_patch_sequence(
        patch_sequence_key,
        target_patch_version=target_patch_version,
        target_display_version=target_display_version,
        major_version=major_version,
        base_build_id=base_build_id,
    ):
        log(
            "本地补丁序列已推进到 "
            f"{target_patch_version}。"
        )

    log("打包完成。")




if __name__ == "__main__":
    with _packaging_lock():
        main()

