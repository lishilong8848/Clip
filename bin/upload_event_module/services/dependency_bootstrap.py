import importlib.util
import importlib.metadata
import logging
import os
import re
import subprocess
import sys
import threading
import time
import urllib.request
from collections import deque
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlsplit, urlunsplit

from ..logger import setup_logging

setup_logging()
_LOG = logging.getLogger("DependencyInstall")
_LOG.setLevel(logging.INFO)


def _safe_log(value: Any) -> str:
    def redact_url(match):
        try:
            parsed = urlsplit(match.group())
            return urlunsplit((parsed.scheme, parsed.netloc.rsplit('@', 1)[-1], parsed.path, '', ''))
        except ValueError:
            return '[URL]'
    text = re.sub(r'https?://[^\s<>"\']+', redact_url, str(value))
    return text[:2000]


def log_info(message):
    _LOG.info('%s', _safe_log(message))


def log_warning(message):
    _LOG.warning('%s', _safe_log(message))


def log_error(message):
    _LOG.error('%s', _safe_log(message))


DEFAULT_MODULE_TO_PACKAGE = {
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
    "fastembed": "fastembed==0.9.0",
    "faiss": "faiss-cpu==1.15.1",
    "yaml": "PyYAML",
    "anyio": "anyio",
    "pydantic": "pydantic",
    "pydantic_ai": "pydantic-ai-slim[openai]==2.52.0",
    "openai": "openai==3.22.1",
    "starlette": "starlette",
    "multipart": "python-multipart==0.0.22",
    "apscheduler": "APScheduler",
    "fastapi": "fastapi",
    "uvicorn": "uvicorn",
    "lark_oapi": "lark-oapi",
    "winocr": "winocr",
}

DEFAULT_WINDOWS_MODULE_TO_PACKAGE = {
    "win32api": "pywin32",
    "win32com.client": "pywin32",
    "win32gui": "pywin32",
    "win32job": "pywin32",
    "win32clipboard": "pywin32",
    "pythoncom": "pywin32",
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


def _run_cmd(args: list[str], timeout_seconds: int = 30, *, status_callback=None) -> tuple[bool, str]:
    startupinfo = None
    creationflags = 0
    if sys.platform == "win32":
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        startupinfo.wShowWindow = 0  # SW_HIDE
        creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0) | getattr(subprocess, "BELOW_NORMAL_PRIORITY_CLASS", 0)
    started = time.monotonic()
    tail = deque(maxlen=40)
    label = '安装依赖' if 'install' in args else '准备 pip'
    log_info(f"{label}: 开始，超时限制 {max(5, timeout_seconds)} 秒")
    proc = None
    reader = None
    timed_out = False
    try:
        proc = subprocess.Popen(
            args,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            env={**os.environ, 'PYTHONUNBUFFERED': '1', 'PIP_PROGRESS_BAR': 'off', 'NO_COLOR': '1'},
            startupinfo=startupinfo,
            creationflags=creationflags,
        )
        def read_output():
            try:
                with proc.stdout:
                    for line in proc.stdout:
                        line = _safe_log(line.strip())
                        if line:
                            tail.append(line)
                            log_info('pip: ' + line)
            except (OSError, ValueError):
                pass
        reader = threading.Thread(target=read_output, name='DependencyOutput', daemon=True)
        reader.start()
        deadline = started + max(5, timeout_seconds)
        while proc.poll() is None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                timed_out = True
                proc.kill()
                proc.wait(timeout=5)
                break
            try:
                proc.wait(timeout=min(10, remaining))
            except subprocess.TimeoutExpired:
                elapsed = int(time.monotonic() - started)
                message = f'{label}中，已等待 {elapsed} 秒'
                log_info(message)
                if status_callback:
                    try:
                        status_callback('远程更新: ' + message)
                    except Exception as exc:
                        log_warning(f'依赖安装进度通知失败: {type(exc).__name__}')
    except Exception as exc:
        message = _safe_log(f'{label}异常: {type(exc).__name__}: {exc}')
        log_error(message)
        return False, message
    finally:
        if proc is not None and proc.poll() is None:
            try:
                proc.kill()
                proc.wait(timeout=5)
            except (OSError, subprocess.TimeoutExpired) as exc:
                log_warning(f'依赖安装进程退出未完成: {type(exc).__name__}')
        if reader is not None:
            reader.join(timeout=2)
    elapsed = round(time.monotonic() - started, 1)
    detail = '\n'.join(tail)
    if timed_out:
        message = f'{label}超时（{elapsed} 秒），将核验已安装的依赖'
        log_warning(message)
        return False, message + ('\n' + detail if detail else '')
    log_info(f'{label}: 退出码 {proc.returncode}，耗时 {elapsed} 秒')
    if proc.returncode == 0:
        return True, detail
    return False, f'{label}退出码 {proc.returncode}' + ('\n' + detail if detail else '')


def _has_module(module_name: str) -> bool:
    try:
        if importlib.util.find_spec(module_name) is None:
            return False
        pinned = {"pydantic_ai": ("pydantic-ai-slim", "2.52.0"), "openai": ("openai", "3.22.1"), "websockets": ("websockets", "16.0"), "multipart": ("python-multipart", "0.0.22")}
        if module_name in pinned:
            distribution, version = pinned[module_name]
            return importlib.metadata.version(distribution) == version
        return True
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
    importlib.invalidate_caches()
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
    log_info(f'依赖检查开始，解释器: {python_exe or sys.executable}')
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
        log_info('依赖检查通过，无需安装')
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
    last_error = ''
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
            ok, err = _run_cmd(cmd, timeout_value + 60, status_callback=status_callback)
            if not ok:
                last_error = err
                log_warning(f"依赖安装失败({mirror}): {err}")
            still_missing = _verify_modules(module_to_package)
            if not still_missing:
                message = '依赖安装后核验通过' if ok else '安装命令未正常结束，但依赖已全部就绪，核验通过'
                log_info(message)
                return True, message
            log_warning("依赖安装后仍缺模块: " + ", ".join(still_missing))

    final_missing = _verify_modules(module_to_package)
    if final_missing:
        detail = "仍缺模块: " + ", ".join(final_missing) + ('；最后安装结果: ' + last_error[-1500:] if last_error else '')
        log_error(detail)
        return False, detail
    log_info('依赖最终核验通过，可以继续更新')
    return True, '依赖最终核验通过'
