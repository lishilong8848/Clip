"""Administrator-only release history; never imports a packaged draft on read."""
from pathlib import Path
import threading

from upload_event_module.services.release_journal import (
    BUNDLE_PATH, MAX_BYTES, ReleaseJournalError, merge_entries, published_entries,
    read_json, write_json,
)


_LOCK = threading.RLock()


class ReleaseJournalStore:
    def __init__(self, data_dir):
        self.path = Path(data_dir) / "release_journal" / "history.json"

    def entries(self):
        value = read_json(self.path, {"schema": 1, "entries": []})
        if not isinstance(value, dict) or value.get("schema") != 1:
            raise ReleaseJournalError("更新历史格式无效，未覆盖原记录。")
        return merge_entries([], value.get("entries"))

    def import_published(self, app_root, manifest):
        """Call only after the existing updater has verified and installed this manifest."""
        path = Path(app_root) / BUNDLE_PATH
        with _LOCK:
            if not manifest.get("release_journal"):
                return False
            if path.stat().st_size > MAX_BYTES:
                raise ReleaseJournalError("更新历史快照过大。")
            content = path.read_bytes()
            entries = published_entries(content, manifest)
            # The updater excludes patch_meta.json and commits build_meta.json last.
            installed = read_json(Path(app_root) / "bin/build_meta.json", {})
            if (installed.get("build_id") != manifest.get("target_version")
                    or type(installed.get("patch_version")) is not int
                    or installed["patch_version"] != manifest.get("target_patch_version")):
                raise ReleaseJournalError("已安装文件与发布记录不一致，未导入。")
            previous = self.entries()
            merged = merge_entries(previous, entries)
            if merged != previous:
                write_json(self.path, {"schema": 1, "entries": merged})
        return True

    def list(self, *, q="", module="", page=1, page_size=20):
        if not isinstance(q, str) or len(q) > 120 or not isinstance(module, str) or len(module) > 80:
            raise ReleaseJournalError("更新记录查询条件无效。")
        if type(page) is not int or not 1 <= page <= 100000 or type(page_size) is not int or not 1 <= page_size <= 50:
            raise ReleaseJournalError("更新记录分页参数无效。")
        entries = self.entries()
        modules = sorted({item["module"] for entry in entries for item in entry["items"]})
        query = q.strip().casefold()
        filtered = [entry for entry in entries if (not module or any(item["module"] == module for item in entry["items"]))
                    and (not query or query in (entry["version"] + " " + str(entry["patch"]) + " " +
                         " ".join(value for item in entry["items"] for value in item.values()) + " " +
                         " ".join(commit["subject"] for commit in entry["commits"])).casefold())]
        return {"items": filtered[(page - 1) * page_size:page * page_size], "total": len(filtered),
                "page": page, "page_size": page_size, "modules": modules}

    def detail(self, identity):
        return next((entry for entry in self.entries() if entry["id"] == identity), None)


def import_published_release(app_root, data_dir, manifest):
    return ReleaseJournalStore(data_dir).import_published(app_root, manifest)
