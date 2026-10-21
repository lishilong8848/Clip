"""Local release drafts and immutable, publication-confirmed history bundles."""
import copy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import subprocess
import uuid
import zipfile


BUNDLE_PATH = Path("bin/release_history/history.json")
MAX_BYTES = 8 * 1024 * 1024
KIND = "clipflow_release_history"
SHA = re.compile(r"[a-f0-9]{40}(?:[a-f0-9]{24})?\Z")


class ReleaseJournalError(ValueError):
    pass


def clean_text(value, limit=1000):
    text = str(value or "")
    text = re.sub(r"-----BEGIN [^-]*PRIVATE KEY-----[\s\S]*?-----END [^-]*PRIVATE KEY-----", "[REDACTED]", text)
    text = re.sub(r"https?://\S+|[A-Za-z]:[\\/][^\s]+", "[PRIVATE_LINK]", text)
    text = re.sub(r"(?i)\bBearer\s+\S+", "Bearer [REDACTED]", text)
    text = re.sub(r"(?i)((?:app[_ -]?secret|client[_ -]?secret|password|token|authorization|api[_ -]?key|secret|密钥|密码|令牌)[\"']?\s*[:=：]\s*)(?:\"[^\"]*\"|'[^']*'|[^\s,;，；]+)", r"\1[REDACTED]", text)
    text = re.sub(r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}|\b1[3-9]\d{9}\b|\b(?:ou|on|oc|cli)_[A-Za-z0-9_-]+", "[REDACTED]", text)
    text = re.sub(r"(?<![A-Za-z0-9])(?:\d{17}[\dXx]|\d{15})(?![A-Za-z0-9])", "[REDACTED]", text)
    text = re.sub(r"\b[A-Za-z0-9_-]{32,}\b", "[REDACTED]", text)
    return re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", text).strip()[:limit]


def encode(value):
    content = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    if len(content) > MAX_BYTES:
        raise ReleaseJournalError("更新记录超过大小限制，未写入。")
    return content


def read_json(path, default=None):
    path = Path(path)
    try:
        if path.stat().st_size > MAX_BYTES:
            raise ReleaseJournalError("更新记录文件过大。")
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return copy.deepcopy(default)
    except (OSError, ValueError) as exc:
        raise ReleaseJournalError("更新记录文件暂不可读，未覆盖原记录。") from exc


def write_json(path, value):
    path = Path(path)
    content = encode(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    try:
        temporary.write_bytes(content)
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def notes_items(values, *, required=True):
    if not isinstance(values, list) or len(values) > 200:
        raise ReleaseJournalError("更新事项格式或数量无效。")
    items = [{key: clean_text(item.get(key), 80 if key == "module" else 1000)
              for key in ("problem", "solution", "module")} for item in values if isinstance(item, dict)]
    if len(items) != len(values) or required and (not items or any(not all(item.values()) for item in items)):
        raise ReleaseJournalError("请核对并填写每项问题、解决方案和所属模块。")
    return items


def public_entry(value, *, pending=False):
    if not isinstance(value, dict) or not re.fullmatch(r"[a-f0-9]{64}", str(value.get("id", ""))):
        raise ReleaseJournalError("更新记录标识无效。")
    if not re.fullmatch(r"[A-Za-z0-9_.-]{1,120}", str(value.get("version", ""))) or type(value.get("patch")) is not int or value["patch"] < 1:
        raise ReleaseJournalError("更新记录版本无效。")
    stamp = value.get("published_at")
    if stamp or not pending:
        try:
            parsed = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                raise ValueError
        except (AttributeError, TypeError, ValueError):
            raise ReleaseJournalError("更新记录发布时间无效。") from None
    commits = value.get("commits")
    if not isinstance(commits, list) or len(commits) > 2000 or any(not isinstance(c, dict) or not SHA.fullmatch(str(c.get("id", ""))) for c in commits):
        raise ReleaseJournalError("更新记录提交摘要无效。")
    return {"id": value["id"], "version": value["version"], "patch": value["patch"],
            "published_at": stamp or "", "commits": [{"id": c["id"], "subject": clean_text(c.get("subject"), 300)} for c in commits],
            "items": notes_items(value.get("items"))}


def merge_entries(previous, incoming):
    if not isinstance(previous, list) or not isinstance(incoming, list):
        raise ReleaseJournalError("更新历史格式无效。")
    entries = {}
    for raw in previous + incoming:
        entry = public_entry(raw)
        if entry["id"] in entries and entries[entry["id"]] != entry:
            raise ReleaseJournalError("同一更新记录存在冲突，未覆盖原历史。")
        entries[entry["id"]] = entry
    return sorted(entries.values(), key=lambda entry: (entry["published_at"], entry["patch"], entry["id"]), reverse=True)


def validate_bundle(content, manifest):
    if len(content) > MAX_BYTES:
        raise ReleaseJournalError("更新记录快照过大。")
    try:
        bundle = json.loads(content)
        if bundle["schema"] != 1 or bundle["kind"] != KIND:
            raise ValueError
        entries = bundle["entries"]
        current = bundle["current_id"]
        if not isinstance(entries, list) or len(entries) > 10000:
            raise ValueError
        entries = [public_entry(entry, pending=entry.get("id") == current) for entry in entries]
        candidates = [entry for entry in entries if entry["id"] == current]
        if len({entry["id"] for entry in entries}) != len(entries) or len(candidates) != 1:
            raise ValueError
        entry = candidates[0]
        if entry["version"] != manifest["target_version"] or entry["patch"] != manifest["target_patch_version"]:
            raise ValueError
        if bundle.get("source_commit") and not SHA.fullmatch(bundle["source_commit"]):
            raise ValueError
        if hashlib.sha256(content).hexdigest() != manifest["release_journal_bundle"]["sha256"]:
            raise ValueError
    except (KeyError, TypeError, ValueError, AttributeError):
        raise ReleaseJournalError("更新记录与明确发布的补丁不一致。") from None
    return bundle


def published_entries(content, manifest):
    bundle = validate_bundle(content, manifest)
    proof = manifest.get("release_journal") or {}
    if (proof.get("schema") != 1 or proof.get("entry_id") != bundle["current_id"]
            or proof.get("bundle_sha256") != hashlib.sha256(content).hexdigest()
            or not re.fullmatch(r"[a-f0-9]{64}", str(manifest.get("zip_sha256", "")))):
        raise ReleaseJournalError("该补丁没有已确认的发布记录，不导入草稿。")
    entries = []
    for entry in bundle["entries"]:
        if entry["id"] == bundle["current_id"]:
            entry = dict(entry, published_at=proof.get("published_at"))
        entries.append(public_entry(entry))
    return merge_entries([], entries)


def read_archive_bundle(archive, manifest):
    spec = manifest.get("release_journal_bundle")
    if not spec:
        return None
    if not isinstance(spec, dict) or spec.get("kind") != KIND:
        raise ReleaseJournalError("未核对的更新草稿不能重试发布，请重新打包核对。")
    expected = manifest["target_version"] + "_patch_only/" + BUNDLE_PATH.as_posix()
    try:
        with zipfile.ZipFile(archive) as zipped:
            matches = [info for info in zipped.infolist() if info.filename == expected]
            if len(matches) != 1 or matches[0].file_size > MAX_BYTES:
                raise ValueError
            content = zipped.read(matches[0])
        validate_bundle(content, manifest)
        return content
    except (OSError, ValueError, KeyError, zipfile.BadZipFile):
        raise ReleaseJournalError("补丁中的更新记录快照无效，未发布。") from None


class BuildReleaseJournal:
    def __init__(self, project_root, build_dir):
        self.root, self.directory = Path(project_root), Path(build_dir)
        self.state_path = self.directory / "release_journal_state.json"
        self.draft_path = self.directory / "release_notes_draft.json"
        self.receipt_path = self.directory / "release_publication_pending.json"

    def state(self):
        value = read_json(self.state_path, {"schema": 1, "baseline": "", "entries": []})
        if not isinstance(value, dict) or value.get("schema") != 1 or value.get("baseline") and not SHA.fullmatch(str(value["baseline"])):
            raise ReleaseJournalError("发布基线状态无效，未重置原记录。")
        return {**value, "entries": merge_entries([], value.get("entries"))}

    def _git(self, *args):
        try:
            result = subprocess.run(["git", "-c", "core.quotepath=false", "-C", str(self.root), *args],
                                    capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=15)
        except (OSError, subprocess.TimeoutExpired):
            return None
        return result.stdout if result.returncode == 0 else None

    def snapshot(self):
        baseline = self.state()["baseline"]
        head = (self._git("rev-parse", "HEAD") or "").strip()
        if not SHA.fullmatch(head):
            head = ""
        usable = bool(head and baseline and self._git("merge-base", "--is-ancestor", baseline, head) is not None)
        commit_range = baseline + ".." + head if usable else head
        raw = self._git("log", "--format=%H%x00%s", "-z", *([commit_range] if usable else ["-1", head])) if head else ""
        parts = (raw or "").strip("\x00").split("\x00") if raw else []
        commits = [{"id": parts[i].strip(), "subject": clean_text(parts[i + 1], 300)}
                   for i in range(0, len(parts) - 1, 2) if SHA.fullmatch(parts[i].strip())]
        if len(commits) > 2000:
            raise ReleaseJournalError("待发布提交超过 2000 项，请先核对发布基线。")
        status = self._git("status", "--porcelain=v1", "-z", "--untracked-files=all") or ""
        files, fields, index = [], status.split("\x00"), 0
        while index < len(fields):
            row = fields[index]
            index += 1
            if len(row) < 4:
                continue
            code, name = row[:2], row[3:]
            if code[0] in "RC" or code[1] in "RC":
                index += 1
            if name.split("/", 1)[0] in {"build_output", ".git"}:
                continue
            path = self.root / name
            try:
                stamp = [path.stat().st_size, path.stat().st_mtime_ns]
            except OSError:
                stamp = []
            files.append({"status": code, "path": clean_text(name, 240), "stamp": stamp})
        return {"head": head, "baseline": baseline if usable else "", "commits": commits, "files": files}

    def prepare_draft(self):
        snapshot = self.snapshot()
        old = read_json(self.draft_path, {})
        if isinstance(old, dict) and old.get("snapshot") == snapshot:
            return old
        draft = {"schema": 1, "id": uuid.uuid4().hex, "snapshot": snapshot, "confirmed": False,
                 "items": notes_items(old.get("items", []), required=False) if isinstance(old, dict) else []}
        if not draft["items"]:
            draft["items"] = [{"problem": "", "solution": "", "module": ""}]
        return self.save_draft(draft)

    def save_draft(self, draft, *, confirmed=False):
        value = {"schema": 1, "id": draft["id"], "snapshot": draft["snapshot"],
                 "confirmed": bool(confirmed), "items": notes_items(draft["items"], required=confirmed)}
        write_json(self.draft_path, value)
        return value

    def bundle(self, draft, version, patch, *, publish):
        if publish and not draft.get("confirmed"):
            raise ReleaseJournalError("更新记录尚未核对，未生成发布包。")
        entries = self.state()["entries"]
        current_id = ""
        if publish:
            current_id = hashlib.sha256(encode([draft["id"], version, patch])).hexdigest()
            entries = [entry for entry in entries if entry["id"] != current_id]
            entries.append(public_entry({"id": current_id, "version": version, "patch": patch,
                                         "published_at": "", "commits": draft["snapshot"]["commits"],
                                         "items": draft["items"]}, pending=True))
        return {"schema": 1, "kind": KIND if publish else KIND + "_draft", "current_id": current_id,
                "source_commit": draft["snapshot"]["head"], "entries": entries}

    def prepare_receipt(self, content, manifest):
        bundle = validate_bundle(content, manifest)
        digest = hashlib.sha256(content).hexdigest()
        old = read_json(self.receipt_path, {})
        if old.get("zip_sha256") == manifest.get("zip_sha256") and old.get("proof", {}).get("bundle_sha256") == digest:
            published_entries(content, dict(manifest, release_journal=old["proof"]))
            return old["proof"]
        existing = next((e for e in self.state()["entries"] if e["id"] == bundle["current_id"]), None)
        proof = manifest.get("release_journal") or {
            "schema": 1, "entry_id": bundle["current_id"], "bundle_sha256": digest,
            "published_at": existing["published_at"] if existing else datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        published_entries(content, dict(manifest, release_journal=proof))
        write_json(self.receipt_path, {"zip_sha256": manifest["zip_sha256"], "proof": proof})
        return proof

    def mark_published(self, content, manifest):
        entries = published_entries(content, manifest)
        bundle = validate_bundle(content, manifest)
        state = self.state()
        known = {entry["id"] for entry in state["entries"]}
        included = {entry["id"] for entry in entries}
        baseline = state["baseline"]
        if bundle["current_id"] not in known and known <= included:
            baseline = bundle.get("source_commit") or baseline
        write_json(self.state_path, {"schema": 1, "baseline": baseline,
                                     "entries": merge_entries(state["entries"], entries)})
