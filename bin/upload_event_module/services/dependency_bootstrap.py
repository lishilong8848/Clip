import importlib.util
import subprocess
import sys
import urllib.request
from pathlib import Path
from typing import Any, Callable

from ..logger import log_error, log_info, log_warning


DEFAULT_MODULE_TO_PACKAGE = {
    "PyQt6": "PyQt6",
    "watchdog": "watchdog",
    "requests": "requests",
    "urllib3": "urllib3",
    "PIL": "Pillow",
    "anyio": "anyio",
    "pydantic": "pydantic",
    "starlette": "starlette",
    "fastapi": "fastapi",
    "uvicorn": "uvicorn",
    "lark_oapi": "lark-oapi",
    "winocr": "winocr",
}

DEFAULT_WINDOWS_MODULE_TO_PACKAGE = {
    "win32api": "pywin32",
    "win32gui": "pywin32",
    "win32clipboard": "pywin32",
    "pywintypes": "pywin32",
}

DEFAULT_MIRRORS = [
    "https://pypi.tuna.tsinghua.edu.cn/simple",
    "https://mirrors.aliyun.com/pypi/simple",
    "https://pypi.mirrors.ustc.edu.cn/simple",
]

GET_PIP_URLS = [
    "https://bootstrap.pypa.io/get-pip.py",
    "https://mirrors.aliyun.com/pypi/get-pip.py",
]


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _run_cmd(args: list[str], timeout_seconds: int = 30) -> tuple[bool, str]:
    try:
        proc = subprocess.run(
            args,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="ignore",
            timeout=max(5, timeout_seconds),
            check=False,
        )
    except Exception as exc:
        return False, str(exc)
    if proc.returncode == 0:
        return True, proc.stdout.strip()
    err = (proc.stderr or proc.stdout or "").strip()
    return False, err


def _has_module(module_name: str) -> bool:
    try:
        return importlib.util.find_spec(module_name) is not None
    except Exception:
        return False


def _bootstrap_pip(
    python_exe: Path,
    *,
    allow_get_pip: bool,
    timeout_seconds: int,
) -> tuple[bool, str]:
    ok, _ = _run_cmd([str(python_exe), "-m", "pip", "--version"], timeout_seconds)
    if ok:
        return True, "pip available"

    ok, err = _run_cmd(
        [str(python_exe), "-m", "ensurepip", "--upgrade"],
        timeout_seconds,
    )
    if ok:
        ok2, _ = _run_cmd([str(python_exe), "-m", "pip", "--version"], timeout_seconds)
        if ok2:
            return True, "pip bootstrapped by ensurepip"
    else:
        log_warning(f"依赖安装: ensurepip 失败: {err}")

    if not allow_get_pip:
        return False, "pip unavailable and get-pip disabled"

    bin_dir = Path(__file__).resolve().parents[2]
    get_pip = bin_dir / "tools" / "get-pip.py"
    if not get_pip.exists():
        get_pip.parent.mkdir(parents=True, exist_ok=True)
        downloaded = False
        for url in GET_PIP_URLS:
            try:
                with urllib.request.urlopen(
                    url, timeout=max(10, timeout_seconds)
                ) as resp:
                    content = resp.read()
                if not content:
                    continue
                get_pip.write_bytes(content)
                downloaded = True
                break
            except Exception:
                continue
        if not downloaded:
            return False, f"pip unavailable and failed to download get-pip: {get_pip}"

    ok, err = _run_cmd(
        [
            str(python_exe),
            str(get_pip),
            "--disable-pip-version-check",
            "--no-input",
        ],
        timeout_seconds,
    )
    if not ok:
        return False, f"get-pip failed: {err}"

    ok, _ = _run_cmd([str(python_exe), "-m", "pip", "--version"], timeout_seconds)
    if not ok:
        return False, "pip still unavailable after get-pip"
    return True, "pip bootstrapped by get-pip"


def _normalize_mirror_list(value: Any) -> list[str]:
    if isinstance(value, list):
        result = [str(item).strip() for item in value if str(item).strip()]
        if result:
            return result
    return list(DEFAULT_MIRRORS)


def _normalize_module_to_package(value: Any) -> dict[str, str]:
    cleaned: dict[str, str] = dict(DEFAULT_MODULE_TO_PACKAGE)
    if sys.platform == "win32":
        cleaned.update(DEFAULT_WINDOWS_MODULE_TO_PACKAGE)
    if not isinstance(value, dict):
        return cleaned
    for mod, pkg in value.items():
        mod_name = str(mod).strip()
        pkg_name = str(pkg).strip()
        if mod_name and pkg_name:
            cleaned[mod_name] = pkg_name
    return cleaned


def _resolve_missing_packages(
    required_packages: Any,
    module_to_package: dict[str, str],
) -> tuple[list[str], list[str]]:
    missing_modules = [
        module for module in module_to_package if not _has_module(module)
    ]
    missing_pkgs_from_modules = [module_to_package[m] for m in missing_modules]

    required_list: list[str] = []
    if isinstance(required_packages, list):
        required_list = [str(p).strip() for p in required_packages if str(p).strip()]
    # No explicit requirement list means "install only missing modules".
    target = list(dict.fromkeys(missing_pkgs_from_modules + required_list))
    return target, missing_modules


def _verify_modules(module_to_package: dict[str, str]) -> list[str]:
    return [module for module in module_to_package if not _has_module(module)]


def ensure_runtime_dependencies(
    manifest: dict,
    python_exe: Path,
    *,
    mirrors: Any = None,
    timeout_seconds: int = 20,
    retries_per_mirror: int = 1,
    allow_get_pip: bool = True,
    status_callback: Callable[[str], None] | None = None,
) -> tuple[bool, str]:
    module_to_package = _normalize_module_to_package(
        (manifest or {}).get("module_to_package")
    )
    required_packages = (manifest or {}).get("required_packages")
    packages_to_install, missing_modules = _resolve_missing_packages(
        required_packages,
        module_to_package,
    )
    if status_callback:
        status_callback("远程更新: 依赖检查中")

    if not missing_modules:
        return True, "依赖已就绪"

    py = Path(python_exe or sys.executable)
    timeout_value = _safe_int(timeout_seconds, 20)
    retry_value = max(1, _safe_int(retries_per_mirror, 1))
    mirror_list = _normalize_mirror_list(mirrors)

    ok, detail = _bootstrap_pip(
        py,
        allow_get_pip=bool(allow_get_pip),
        timeout_seconds=timeout_value,
    )
    if not ok:
        return False, detail

    if status_callback:
        status_callback("远程更新: 正在安装依赖")
    log_info(f"依赖安装: 缺失模块 {', '.join(missing_modules)}")
    for mirror in mirror_list:
        for attempt in range(1, retry_value + 1):
            cmd = [
                str(py),
                "-m",
                "pip",
                "install",
                "--disable-pip-version-check",
                "--no-input",
                "--prefer-binary",
                "--retries",
                "1",
                "--timeout",
                str(timeout_value),
                "-i",
                mirror,
            ] + packages_to_install
            log_info(f"依赖安装: mirror={mirror} attempt={attempt}")
            ok, err = _run_cmd(cmd, timeout_value + 60)
            if not ok:
                log_warning(f"依赖安装失败({mirror}): {err}")
                continue
            still_missing = _verify_modules(module_to_package)
            if not still_missing:
                return True, f"依赖安装成功({mirror})"
            log_warning("依赖安装后仍缺模块: " + ", ".join(still_missing))

    final_missing = _verify_modules(module_to_package)
    if final_missing:
        return False, "仍缺模块: " + ", ".join(final_missing)
    log_error("依赖安装失败: 未知错误")
    return False, "未知错误"
