import os
import re
import shutil
import sys
import hashlib
import time as _time
from datetime import datetime, time


WHITESPACE_TRANSLATOR = str.maketrans("", "", " \t\n\r\x0b\x0c")


def get_base_dir() -> str:
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def get_user_data_dir(app_name: str = "ClipFlow") -> str:
    # Data directory is fixed to the bin-level isolated folder.
    data_root = os.path.join(BASE_DIR, "data")
    if app_name and app_name != "ClipFlow":
        return os.path.join(data_root, app_name)
    return data_root


def get_data_dir() -> str:
    return get_user_data_dir("ClipFlow")


def ensure_data_dir() -> str:
    data_dir = DATA_DIR
    try:
        os.makedirs(data_dir, exist_ok=True)
    except Exception:
        pass
    return data_dir


def get_data_file_path(filename: str) -> str:
    return os.path.join(ensure_data_dir(), filename)


LEGACY_RUNTIME_FILES = [
    "config.json",
    "history.json",
    "active_cache.json",
    "app_log.txt",
    "crash_trace.log",
    "hot_reload_state.json",
    "restart_overlay_state.json",
    "restart_guard.json",
    "update_overlay_state.json",
    "clipboard_events.jsonl",
    "event_relay_events.jsonl",
]


def _legacy_candidates(filename: str) -> list[str]:
    candidates = [os.path.join(BASE_DIR, filename)]
    if os.name == "nt":
        local_app_data = os.getenv("LOCALAPPDATA")
        if local_app_data:
            candidates.append(os.path.join(local_app_data, "ClipFlow", filename))
    seen: set[str] = set()
    ordered: list[str] = []
    for path in candidates:
        norm = os.path.normcase(os.path.abspath(path))
        if norm in seen:
            continue
        seen.add(norm)
        ordered.append(path)
    return ordered


def _file_sha256(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _build_conflict_path(source_path: str) -> str:
    ts = _time.strftime("%Y%m%d_%H%M%S", _time.localtime())
    candidate = f"{source_path}.legacy_conflict_{ts}"
    idx = 1
    while os.path.exists(candidate):
        candidate = f"{source_path}.legacy_conflict_{ts}_{idx}"
        idx += 1
    return candidate


def move_legacy_data_file(filename: str) -> dict:
    target_path = get_data_file_path(filename)
    result = {
        "filename": filename,
        "source": None,
        "target": target_path,
        "status": "skipped",
        "method": None,
        "error": None,
    }
    try:
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
    except Exception as exc:
        result["status"] = "failed"
        result["error"] = f"mkdir_failed: {exc}"
        return result

    target_exists = os.path.exists(target_path)
    for source_path in _legacy_candidates(filename):
        try:
            if not os.path.exists(source_path):
                continue
            if os.path.normcase(os.path.abspath(source_path)) == os.path.normcase(
                os.path.abspath(target_path)
            ):
                continue
            result["source"] = source_path
            if target_exists:
                conflict_path = _build_conflict_path(source_path)
                try:
                    os.replace(source_path, conflict_path)
                    result["status"] = "conflict_renamed"
                    result["method"] = "rename"
                except Exception as exc:
                    result["status"] = "failed"
                    result["error"] = f"conflict_rename_failed: {exc}"
                return result

            try:
                os.replace(source_path, target_path)
                result["status"] = "moved"
                result["method"] = "replace"
                return result
            except Exception:
                pass

            try:
                shutil.copy2(source_path, target_path)
                src_size = os.path.getsize(source_path)
                dst_size = os.path.getsize(target_path)
                if src_size != dst_size:
                    raise RuntimeError("size_mismatch")
                src_hash = _file_sha256(source_path)
                dst_hash = _file_sha256(target_path)
                if src_hash != dst_hash:
                    raise RuntimeError("hash_mismatch")
                os.remove(source_path)
                result["status"] = "moved"
                result["method"] = "copy_verify_delete"
                return result
            except Exception as exc:
                result["status"] = "failed"
                result["error"] = f"copy_verify_failed: {exc}"
                return result
        except Exception as exc:
            result["status"] = "failed"
            result["error"] = str(exc)
            return result
    return result


def migrate_runtime_data_files() -> list[dict]:
    outcomes: list[dict] = []
    for filename in LEGACY_RUNTIME_FILES:
        outcomes.append(move_legacy_data_file(filename))
    return outcomes


def migrate_legacy_data_file(filename: str) -> str:
    # Backward compatible single-file migrator used by existing call sites.
    move_legacy_data_file(filename)
    return get_data_file_path(filename)


BASE_DIR = get_base_dir()
DATA_DIR = get_data_dir()


def get_resource_path(relative_path: str) -> str:
    if hasattr(sys, "_MEIPASS"):
        return os.path.join(sys._MEIPASS, relative_path)
    return os.path.join(BASE_DIR, relative_path)


HISTORY_FILE = migrate_legacy_data_file("history.json")
ACTIVE_CACHE_FILE = migrate_legacy_data_file("active_cache.json")
ICON_FILE = get_resource_path("a.ico")


def _normalize(value: int, limit: int) -> int:
    if value >= limit:
        return value % limit
    return value


def extract_datetime(text: str):
    h = m = s = None

    h_match = re.search(r"(\d{1,2})\s*时", text)
    m_match = re.search(r"(\d{1,2})\s*分", text)
    s_match = re.search(r"(\d{1,2})\s*秒", text)

    if h_match:
        h = int(h_match.group(1))
    if m_match:
        m = int(m_match.group(1))
    if s_match:
        s = int(s_match.group(1))

    if h is None or m is None:
        colon_match = re.search(
            r"(\d{1,2})\s*[:：]\s*(\d{1,2})(?:\s*[:：]\s*(\d{1,2}))?", text
        )
        if colon_match:
            if h is None:
                h = int(colon_match.group(1))
            if m is None:
                m = int(colon_match.group(2))
            if s is None and colon_match.group(3):
                s = int(colon_match.group(3))

    h = h if h is not None else 0
    m = m if m is not None else 0
    s = s if s is not None else 0

    h = _normalize(h, 24)
    m = _normalize(m, 60)
    s = _normalize(s, 60)

    try:
        current_date = datetime.now().date()
        date_match = re.search(r"(\d{4})[-年/.](\d{1,2})[-月/.](\d{1,2})", text)
        if date_match:
            year_value = int(date_match.group(1))
            month_value = int(date_match.group(2))
            day_value = int(date_match.group(3))
            current_date = current_date.replace(
                year=year_value, month=month_value, day=day_value
            )
        return datetime.combine(current_date, time(h, m, s))
    except ValueError:
        return None
