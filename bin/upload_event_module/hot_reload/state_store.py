import json
import os
import shutil
import sys
import time
from pathlib import Path
from typing import Any

from ..logger import log_error, log_info


STATE_SCHEMA_VERSION = 1


def get_user_data_dir(app_name: str = "ClipFlow") -> Path:
    if getattr(sys, "frozen", False):
        bin_dir = Path(sys.executable).resolve().parent
    else:
        # .../bin/upload_event_module/hot_reload/state_store.py -> .../bin
        bin_dir = Path(__file__).resolve().parents[2]
    data_root = bin_dir / "data"
    if app_name and app_name != "ClipFlow":
        return data_root / app_name
    return data_root


def migrate_legacy_state_file(path: Path) -> None:
    if path.exists():
        return
    legacy_candidates: list[Path] = []
    if os.name == "nt":
        local_app_data = os.getenv("LOCALAPPDATA")
        if local_app_data:
            legacy_candidates.append(Path(local_app_data) / "ClipFlow" / path.name)
    # old layout: files under bin directory
    legacy_candidates.append(path.parent.parent / path.name)
    for candidate in legacy_candidates:
        if not candidate.exists():
            continue
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(candidate, path)
            return
        except Exception:
            continue


def write_state_atomic(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(path.name + ".tmp")
    try:
        with tmp_path.open("w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
    except Exception as exc:
        log_error(f"StateStore: 写入失败 {path}: {exc}")
        raise


def read_state_safe(path: Path) -> dict[str, Any] | None:
    migrate_legacy_state_file(path)
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as exc:
        log_error(f"StateStore: 读取失败 {path}: {exc}")
        return None


def cleanup_state(path: Path) -> None:
    try:
        if path.exists():
            path.unlink()
            log_info(f"StateStore: 已清理 {path}")
    except Exception as exc:
        log_error(f"StateStore: 清理失败 {path}: {exc}")


def build_state_payload(data: dict[str, Any], app_version: str | None = None) -> dict[str, Any]:
    payload = {
        "schema_version": STATE_SCHEMA_VERSION,
        "created_at": time.time(),
        "pid": os.getpid(),
        "data": data,
    }
    if app_version:
        payload["app_version"] = app_version
    return payload
