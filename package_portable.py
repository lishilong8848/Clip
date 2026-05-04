import argparse

import hashlib

import json

import os

import re

import shutil

import subprocess

import sys

import time

import zipfile

from pathlib import Path

from urllib.parse import urlparse





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

DEFAULT_PATCH_ZIP_NAME = "ClipFlow_patch_only.zip"

AUTO_UPLOAD_GITEE = True  # 将zip补丁上传gitee

# 版本命名模式：

# - "timestamp": ClipFlow_portable_YYYYmmdd_HHMMSS (旧格式)

# - "base_ts":   ClipFlow_M{BASE}_S{YYYYmmdd_HHMMSS} (大版本+小版本)

# - "base_simple": ClipFlow_V1_YYYYmmdd_HHMMSS (大版本+时间戳)

VERSION_NAMING = "base_simple"

DEFAULT_FORCE_UI_UPDATE = True  # 是否强制用户重启程序以更新UI界面

EXCLUDE_FILES = {

    "package_portable.py",

    "分发前检查清单.md",

    "静默启动.bat",

    "active_cache.json",

    "app_log.txt",

    "config.json",

    "history.json",

    "feishu_error_code.md",

}



EXCLUDE_TOP_LEVEL = {

    ".git",

    ".idea",

    ".vscode",

    "__pycache__",

    "build_output",

} | EXCLUDE_FILES



EXCLUDE_DIR_NAMES = {

    ".git",

    ".idea",

    ".vscode",

    "__pycache__",

}



FORCE_PATCH_INCLUDE_FILES = {

    Path("bin") / "upload_event_module" / "web" / "index.html",

}



RUNTIME_MODULE_TO_PACKAGE = {

    "PyQt6": "PyQt6",

    "watchdog": "watchdog",

    "requests": "requests",

    "urllib3": "urllib3",

    "PIL": "Pillow",

    "anyio": "anyio",

    "fastapi": "fastapi",

    "uvicorn": "uvicorn",

    "pydantic": "pydantic",

    "starlette": "starlette",

    "lark_oapi": "lark-oapi",

    "winocr": "winocr",

}



WINDOWS_RUNTIME_MODULE_TO_PACKAGE = {

    "win32api": "pywin32",

    "win32gui": "pywin32",

    "win32clipboard": "pywin32",

    "pywintypes": "pywin32",

}



RUNTIME_PACKAGE_INSTALL_ORDER = [

    "PyQt6",

    "watchdog",

    "requests",

    "urllib3",

    "PIL",

    "anyio",

    "pydantic",

    "starlette",

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
    "upload_event_module.services.event_relay_server",
    "upload_event_module.ui.event_relay_bridge",
    "upload_event_module.ui.main_window",
    "upload_event_module.ui.main_window_clipboard",
    "upload_event_module.ui.main_window_records",
    "upload_event_module.ui.main_window_workflow",
    "upload_event_module.ui.main_window_ui",
    "upload_event_module.ui.main_window_runtime",
    "lan_bitable_template_portal.portal_service",
    "lan_bitable_template_portal.server",
    "lark_oapi",
]




def log(msg: str) -> None:

    print(f"[Package] {msg}")





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

    missing: list[str] = []

    for module_name in RUNTIME_MODULE_TO_PACKAGE.keys():

        if not _run_cmd(

            [str(venv_python), "-c", f"import {module_name}"],

            log_output_on_error=False,

        ):

            missing.append(module_name)

    return missing





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





def _ignore_names(dirpath: str, names: list[str]) -> set[str]:

    ignore = set()

    base = Path(dirpath)

    for name in names:

        if name in EXCLUDE_DIR_NAMES or name in EXCLUDE_FILES:

            ignore.add(name)

            continue



        if name in ("build", "dist") and base.name == "bin":

            ignore.add(name)

            continue



        if name.endswith((".pyc", ".pyo")):

            ignore.add(name)

            continue



    return ignore





def _is_under_bin_build_or_dist(path: Path) -> bool:

    parts = path.parts

    for i, part in enumerate(parts[:-1]):

        if part != "bin":

            continue

        next_part = parts[i + 1]

        if next_part in {"build", "dist"}:

            return True

    return False





def _is_excluded(path: Path, *, exclude_venv: bool = False) -> bool:

    parts = set(path.parts)

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





def _is_baseline_excluded(path: Path, *, exclude_venv: bool = False) -> bool:

    parts = set(path.parts)

    if parts & EXCLUDE_DIR_NAMES:

        return True

    if exclude_venv and ".venv" in parts:

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

    for path in root.rglob("*"):

        if path.is_dir():

            continue

        if _is_excluded(path, exclude_venv=exclude_venv):

            continue

        if path.name in EXCLUDE_TOP_LEVEL and path.parent == root:

            continue

        if path.suffix.lower() == ".zip" and path.parent == root:

            continue

        files.append(path)

    return files





def _should_force_include_in_patch(relative_path: Path) -> bool:

    norm = Path(str(relative_path).replace("\\", "/"))

    return norm in FORCE_PATCH_INCLUDE_FILES





def _has_code_changes(baseline_dir: Path | None, *, exclude_venv: bool = True) -> bool:

    if not baseline_dir or not baseline_dir.exists():

        return True

    baseline_files: dict[Path, Path] = {}

    for path in baseline_dir.rglob("*"):

        if path.is_dir():

            continue

        rel = path.relative_to(baseline_dir)

        if exclude_venv and ".venv" in rel.parts:

            continue

        baseline_files[rel] = path



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

    patterns = ("*_patch_only.zip", DEFAULT_PATCH_ZIP_NAME)

    visited: set[Path] = set()

    for pattern in patterns:

        for zip_path in BUILD_DIR.glob(pattern):

            if zip_path in visited:

                continue

            visited.add(zip_path)

            if not zip_path.is_file():

                continue

            try:

                zip_path.unlink()

                removed += 1

            except Exception as exc:

                log(f"跳过删除旧补丁压缩包 {zip_path.name}: {exc}")

    return removed





def _zip_patch_dir(patch_dir: Path) -> Path:

    zip_path = BUILD_DIR / DEFAULT_PATCH_ZIP_NAME

    removed = _cleanup_old_patch_zips()

    if removed:

        log(f"已删除旧补丁压缩包: {removed}")

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:

        for src in patch_dir.rglob("*"):

            if src.is_dir():

                continue

            arcname = f"{patch_dir.name}/{src.relative_to(patch_dir).as_posix()}"

            zf.write(src, arcname=arcname)

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

    temp_repo = BUILD_DIR / f".gitee_upload_{int(time.time())}"

    if temp_repo.exists():

        shutil.rmtree(temp_repo, ignore_errors=True)

    clone_ok = _run_cmd(

        ["git", "clone", "--depth", "1", "-b", branch, repo_url, str(temp_repo)]

    )

    if not clone_ok:

        log("Gitee 上传失败：clone 仓库失败。")

        return False



    try:

        patch_subdir = temp_repo / (subdir or "").strip().strip("/")

        patch_subdir.mkdir(parents=True, exist_ok=True)

        removed_old = 0

        for old_zip in patch_subdir.glob("*_patch_only.zip"):

            if old_zip.name == patch_zip.name:

                continue

            try:

                old_zip.unlink()

                removed_old += 1

            except Exception as exc:

                log(f"跳过删除远端旧补丁压缩包 {old_zip.name}: {exc}")

        if removed_old:

            log(f"Gitee 仓库中已删除旧补丁压缩包: {removed_old}")

        shutil.copy2(patch_zip, patch_subdir / patch_zip.name)



        manifest_target = temp_repo / manifest_repo_path.strip().strip("/")

        manifest_target.parent.mkdir(parents=True, exist_ok=True)

        shutil.copy2(latest_manifest_path, manifest_target)



        _run_cmd(["git", "add", "."], cwd=temp_repo)

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

                log("Gitee 上传：没有可提交的变更。")

                return True

            log("Gitee 上传失败：提交变更失败。")

            return False

        pushed = _run_cmd(["git", "push", "origin", branch], cwd=temp_repo)

        if pushed:

            log(f"Gitee 上传成功: {repo_url} ({branch})")

            return True

        log("Gitee 上传失败：推送失败。")

        return False

    finally:

        shutil.rmtree(temp_repo, ignore_errors=True)





def copy_project(dist_dir: Path) -> None:

    if dist_dir.exists():

        shutil.rmtree(dist_dir)

    dist_dir.mkdir(parents=True, exist_ok=True)



    for item in PROJECT_ROOT.iterdir():

        if item.name in EXCLUDE_TOP_LEVEL:

            continue

        if item.is_file() and item.suffix.lower() == ".zip":

            continue



        target = dist_dir / item.name

        if item.is_dir():

            shutil.copytree(item, target, ignore=_ignore_names, dirs_exist_ok=True)

        else:

            shutil.copy2(item, target)





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

        for path in baseline_dir.rglob("*"):

            if path.is_dir():

                continue

            rel = path.relative_to(baseline_dir)

            if _is_baseline_excluded(path, exclude_venv=exclude_venv):

                continue

            baseline_files[rel] = path



    changed = 0

    added = 0

    forced_py = 0

    for src in _iter_project_files(PROJECT_ROOT, exclude_venv=exclude_venv):

        rel = src.relative_to(PROJECT_ROOT)

        if rel.suffix.lower() == ".py":

            forced_py += 1

            if baseline_files and rel in baseline_files:

                changed += 1

            else:

                added += 1

            dest = patch_dir / rel

            dest.parent.mkdir(parents=True, exist_ok=True)

            shutil.copy2(src, dest)

            continue

        if _should_force_include_in_patch(rel):

            if baseline_files and rel in baseline_files:

                changed += 1

            else:

                added += 1

            dest = patch_dir / rel

            dest.parent.mkdir(parents=True, exist_ok=True)

            shutil.copy2(src, dest)

            continue

        if baseline_files:

            old = baseline_files.get(rel)

            if old and old.exists():

                try:

                    if _hash_file(src) == _hash_file(old):

                        continue

                except Exception:

                    pass

                changed += 1

            else:

                added += 1

        else:

            added += 1



        dest = patch_dir / rel

        dest.parent.mkdir(parents=True, exist_ok=True)

        shutil.copy2(src, dest)



    deleted = 0

    deleted_paths: list[str] = []

    if baseline_files:

        current_set = {

            p.relative_to(PROJECT_ROOT)

            for p in _iter_project_files(PROJECT_ROOT, exclude_venv=exclude_venv)

        }

        for rel in baseline_files.keys():

            if rel not in current_set:

                deleted += 1

                deleted_paths.append(str(rel))



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

    args = parser.parse_args()



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

    code_changed = _has_code_changes(baseline_dir, exclude_venv=True)

    if not code_changed:

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

    force_ui_update = bool(DEFAULT_FORCE_UI_UPDATE or args.force_ui_update)

    # UI restart policy is explicitly controlled by force_ui_update.

    # No automatic UI change detection is performed.

    ui_changed = force_ui_update

    if force_ui_update:

        log("已启用强制界面更新，本次补丁需要重启程序。")

    else:

        log("未启用强制界面更新，本次补丁将使用热更新（无需重启）。")

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



    if AUTO_UPLOAD_GITEE:
        _upload_patch_to_gitee(
            patch_zip,
            latest_manifest_path,

            repo_url=args.gitee_repo,

            branch=args.gitee_branch,

            subdir=args.gitee_subdir,

            manifest_repo_path=args.gitee_manifest_path,

        )

    else:
        log("已跳过 Gitee 上传（AUTO_UPLOAD_GITEE=False）。")

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

    main()

