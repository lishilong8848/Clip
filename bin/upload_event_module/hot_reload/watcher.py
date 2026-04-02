import fnmatch
from pathlib import Path
from typing import Iterable

try:
    from watchdog.events import FileSystemEventHandler
    from watchdog.observers import Observer

    WATCHDOG_AVAILABLE = True
except ImportError:
    WATCHDOG_AVAILABLE = False
    FileSystemEventHandler = object  # type: ignore
    Observer = None  # type: ignore

from ..config import (
    HOT_RELOAD_IGNORE_DIR_NAMES,
    HOT_RELOAD_IGNORE_GLOBS,
    HOT_RELOAD_IGNORE_SUFFIXES,
    PACKAGE_EXCLUDE_DIR_NAMES,
    PACKAGE_EXCLUDE_FILES,
    PACKAGE_EXCLUDE_TOP_LEVEL,
)
from ..logger import log_info
from .change_batcher import ChangeBatcher


class PathFilter:
    def __init__(self, root: Path) -> None:
        self.root = root.resolve()

    def _is_patch_only_target(self, rel: Path) -> bool:
        parts = rel.parts
        if not parts:
            return False
        if parts[0].lower() != "bin":
            return False
        return any("_patch_only" in part for part in parts)

    def should_ignore(self, path: Path) -> bool:
        # Only react to Python source changes
        if path.suffix.lower() != ".py":
            return True
        try:
            resolved = path.resolve()
        except Exception:
            resolved = path
        try:
            rel = resolved.relative_to(self.root)
        except Exception:
            rel = resolved

        # 热更新仅允许 patch_only 目录中的代码变更触发
        if not self._is_patch_only_target(rel):
            return True

        parts = set(rel.parts)
        if parts & set(HOT_RELOAD_IGNORE_DIR_NAMES):
            return True
        if parts & set(PACKAGE_EXCLUDE_DIR_NAMES):
            return True
        if "build_output" in parts:
            return True
        if rel.name in PACKAGE_EXCLUDE_FILES:
            return True
        if rel.name in PACKAGE_EXCLUDE_TOP_LEVEL and rel.parent == Path("."):
            return True
        if rel.suffix.lower() in HOT_RELOAD_IGNORE_SUFFIXES:
            return True
        if rel.is_dir():
            return True
        # ignore build/dist under bin
        if rel.name in ("build", "dist") and rel.parent.name == "bin":
            return True
        for pattern in HOT_RELOAD_IGNORE_GLOBS:
            if fnmatch.fnmatch(rel.name, pattern):
                return True
        return False


class _HotReloadEventHandler(FileSystemEventHandler):
    def __init__(self, batcher: ChangeBatcher, path_filter: PathFilter) -> None:
        self.batcher = batcher
        self.path_filter = path_filter

    def on_any_event(self, event):
        if event.is_directory:
            return
        src_path = Path(event.src_path)
        if self.path_filter.should_ignore(src_path):
            self.batcher.record_ignored()
        else:
            self.batcher.push(src_path)

        dest_path = getattr(event, "dest_path", None)
        if dest_path:
            dest = Path(dest_path)
            if self.path_filter.should_ignore(dest):
                self.batcher.record_ignored()
            else:
                self.batcher.push(dest)


class HotReloadWatcher:
    def __init__(self, root: Path, batcher: ChangeBatcher) -> None:
        self.root = root
        self.batcher = batcher
        self.path_filter = PathFilter(root)
        self._handler = _HotReloadEventHandler(batcher, self.path_filter)
        self._observer = None

    def start(self) -> None:
        if not WATCHDOG_AVAILABLE:
            log_info("HotReload: watchdog 未安装，Watcher 已禁用")
            return
        if self._observer and self._observer.is_alive():
            return
        self._observer = Observer()
        self._observer.schedule(self._handler, str(self.root), recursive=True)
        self._observer.daemon = True
        self._observer.start()
        log_info(f"HotReload: watcher started at {self.root}")

    def stop(self) -> None:
        if not WATCHDOG_AVAILABLE:
            return
        observer = self._observer
        self._observer = None
        if not observer:
            return
        if observer.is_alive():
            observer.stop()
            observer.join(timeout=2)
            log_info("HotReload: watcher stopped")
