"""Conservative release-check selection; receipts are local, never release logs."""
from __future__ import annotations

import hashlib
import hmac
import json
import os
from fnmatch import fnmatchcase
from pathlib import Path
import secrets
import shutil
import subprocess
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor


VERSION = 2
FULL_BASELINE_MAX_AGE = 7 * 24 * 60 * 60
EXACT_RECHECK_TESTS = frozenset((
    "bin.test_packaging_preflight_selection", "bin.test_transport_safety",
    "bin.test_patch_mirror", "bin.test_update_check_recovery", "bin.test_release_readiness",
))
FRONTEND = "bin/lan_bitable_template_portal/frontend/"
# Only reviewed leaf modules qualify. Shared code, storage, auth, runners and
# new paths deliberately have no mapping: they invalidate every cached check.
SOURCE_AREAS = {
    "bin/lan_bitable_template_portal/learning_reminders.py": "learning",
    "bin/lan_bitable_template_portal/cabinet_power_text.py": "cabinet",
    "bin/openclaw_service/assistant/lighthouse_diagrams.py": "assistant",
    FRONTEND + "src/components/LearningSharing.vue": "frontend",
    FRONTEND + "src/components/CabinetBatchTextFill.vue": "frontend",
}
AREAS = frozenset({"learning", "cabinet", "assistant", "frontend"})
CORE_TESTS = frozenset("bin.test_" + name for name in (
    "lighthouse_agent_boundaries", "lighthouse_sandbox", "lighthouse_shared_privacy",
    "lighthouse_readonly_integrity", "lighthouse_secure_navigation",
    "lighthouse_connection_isolation", "lighthouse_notice_identity",
    "lighthouse_notice_identity_workflows", "lighthouse_notice_command_regression",
    "lighthouse_scope", "lighthouse_account_models", "lighthouse_model_policy",
    "learning_scope_boundaries", "learning_password_identity", "cabinet_guest",
    "openclaw_service_update", "openclaw_service_store", "openclaw_packaging_imports",
))
# All unclassified checks are mandatory, including notices, permissions, data
# preservation, updater, release readiness and the selector's own tests.
TEST_AREAS = (
    (("bin.test_lighthouse_", "bin.test_assistant_", "bin.test_openclaw_"), AREAS),
    (("bin.test_learning", "bin.test_pre_shift_"), frozenset({"learning", "frontend"})),
    (("bin.test_cabinet_",), frozenset({"cabinet", "frontend"})),
    (("bin.test_document_library", "bin.test_knowledge_", "bin.test_sharing_"),
     frozenset({"learning", "assistant", "frontend"})),
)
CORE_FRONTEND = frozenset((
    "check-workspace-navigation.mjs", "check-login-version-switch.mjs", "check-legacy-module-entry.mjs",
    "check-notice-photo-paste.mjs", "check-legacy-notice-cycle.mjs",
    "check-notice-cycle-warnings.mjs", "check-account-fixed-duty.mjs",
    "check-notice-convergence-confirmation.mjs", "check-assistant-shared-query.mjs",
    "check-document-library-all-scopes.mjs",
))
OPTIONAL_FRONTEND = frozenset((
    "check-repair-overview.mjs", "check-document-library-resilience.mjs",
    "check-learning-sharing-types.mjs", "check-learning-sharing-resilience.mjs",
    "check-learning-question-import.mjs", "check-pre-shift-edits.mjs", "check-repair-plan-binding.mjs",
))

# Output/data directories are never source inputs. Do not use gitignore here:
# local edits, untracked files, fixtures, private config and built assets matter.
SKIP_DIRS = frozenset((
    ".git", ".idea", ".vscode", ".agents", ".deepcode", ".codex", ".claude",
    ".codex-audit", ".playwright-cli", ".pytest_cache", ".mypy_cache", ".ruff_cache",
    "__pycache__", ".venv", "venv", "node_modules", ".vite", ".cache", ".npm-cache",
    "build_output", "output", "artifacts", ".runtime", ".workbuddy", "playwright-report",
    "test-results",
))
DATA_DIRS = ("data", "bin/data", "bin/runtime", "bin/resources/knowledge_model")
TRANSIENT_SUFFIXES = (".pyc", ".pyo", ".log", ".tmp", ".tsbuildinfo", ".db", ".sqlite",
                      ".sqlite3", ".db-wal", ".db-shm", ".sqlite-wal", ".sqlite-shm",
                      ".sqlite3-wal", ".sqlite3-shm")


def _digest(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=True,
                                     separators=(",", ":")).encode()).hexdigest()


def _file(path: Path, before=None) -> tuple[str, tuple[int, int, int]]:
    before = before or path.stat()
    with path.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    after = path.stat()
    stamp = lambda item: (item.st_size, item.st_mtime_ns, item.st_ctime_ns)
    if stamp(before) != stamp(after):
        raise RuntimeError(f"预检指纹读取期间文件发生变化: {path.name}")
    return digest, stamp(after)


def _tree(root: Path, *, project=False, exclude=()) -> dict:
    files = []
    if not root.is_dir():
        raise OSError(f"指纹目录不存在: {root}")
    pending = [root]
    while pending:
        directory = pending.pop()
        with os.scandir(directory) as entries:
            for entry in entries:
                path = Path(entry.path)
                rel = path.relative_to(root).as_posix()
                if any(fnmatchcase(rel, pattern) for pattern in exclude):
                    continue
                if entry.is_dir(follow_symlinks=False):
                    skip = entry.name in {"__pycache__", ".cache", ".vite"}
                    if project:
                        skip |= entry.name in SKIP_DIRS or entry.name.startswith(".tmp") or rel in DATA_DIRS
                    if skip:
                        continue
                    if entry.is_symlink() or path.is_junction():
                        raise OSError(f"不能信任带链接的指纹目录: {rel}")
                    pending.append(path)
                else:
                    fixture = bool({"tests", "fixtures", "testdata"} & set(path.relative_to(root).parts))
                    if project and not fixture and entry.name.endswith(TRANSIENT_SUFFIXES):
                        continue
                    if entry.is_symlink():
                        raise OSError(f"不能信任带链接的指纹文件: {rel}")
                    files.append((rel, path, entry.stat(follow_symlinks=False)))
    def read(item):
        rel, path, stamp = item
        return rel, _file(path, stamp)
    with ThreadPoolExecutor(max_workers=4) as pool:
        return dict(pool.map(read, files))


def snapshot(root: Path, python: str) -> dict:
    """Hash actual interpreter/import trees, Node dependencies and browser builds.

    No version-only dependency cache: same-version local package edits count too.
    An unavailable/unreadable environment disables reuse, never any safety check.
    """
    env = {**os.environ, "PYTHONNOUSERSITE": "1"}
    probe = subprocess.run([python, "-c",
        "import json,sys,sysconfig,platform,time,locale;print(json.dumps(dict("
        "version=sys.version,platform=platform.platform(),executable=sys.executable,"
        "paths=sys.path,stdlib=sysconfig.get_path('stdlib'),timezone=time.tzname,"
        "utc_offset=time.timezone,locale=locale.getlocale(),encoding=locale.getpreferredencoding(False))))"],
        cwd=root, env=env, check=True, capture_output=True, text=True, timeout=60)
    info = json.loads(probe.stdout)
    root = root.resolve()
    paths = {Path(value).resolve() for value in info["paths"] if value}
    paths.add(Path(info["stdlib"]).resolve())
    paths.discard(root)
    paths.discard(root / "bin")
    dependencies = {}
    for path in sorted(paths):
        if path.is_dir():
            # A stdlib tree can already contain site-packages.
            if not any(path.is_relative_to(parent) for parent in paths if parent != path and parent.is_dir()):
                dependencies[str(path)] = _tree(path)
        elif path.is_file():
            dependencies[str(path)] = _file(path)
        else:
            dependencies[str(path)] = None
    dependencies[info["executable"]] = _file(Path(info["executable"]))
    node = shutil.which("node")
    if not node:
        raise OSError("缺少 Node，不能复用预检")
    dependencies[node] = _file(Path(node))
    node_modules = root / FRONTEND / "node_modules"
    # vue-tsc regenerates these from the compiler/options, both fingerprinted.
    dependencies[str(node_modules)] = _tree(node_modules, exclude=(".vue-global-types",))
    # Include Playwright's bundled browsers and the system Edge used by checks.
    local = Path(os.environ.get("LOCALAPPDATA", str(root / "missing-local-appdata")))
    browsers = {local / "ms-playwright"}
    if os.environ.get("PLAYWRIGHT_BROWSERS_PATH") not in (None, "0"):
        browsers.add(Path(os.environ["PLAYWRIGHT_BROWSERS_PATH"]))
    browser_caches = set(browsers)
    for variable in ("PROGRAMFILES", "PROGRAMFILES(X86)", "LOCALAPPDATA"):
        if os.environ.get(variable):
            browsers.add(Path(os.environ[variable]) / "Microsoft/Edge/Application")
    for path in sorted(browsers):
        # Playwright's live-browser registry and Chromium debug log are outputs.
        exclude = ("b", "**/debug.log") if path in browser_caches else ()
        dependencies[str(path)] = _tree(path, exclude=exclude) if path.is_dir() else None
    # Only hashes are persisted, never environment variables or private config.
    # The host's per-command trace ID does not configure Python, Node or checks.
    variables = {key: value for key, value in env.items() if key != "CODEX_TOOL_CALL_ID"}
    environment = _digest({"probe": info, "variables": variables, "dependencies": dependencies})
    files = _tree(root, project=True)
    return {"root": str(root), "environment": environment,
            "files": {name: value[0] for name, value in files.items()},
            "stamps": {name: value[1] for name, value in files.items()}}


def source_area(path: str) -> str | None:
    if path.startswith((FRONTEND + "dist/", FRONTEND + "public/", FRONTEND + "src/assets/")):
        return "frontend"
    return SOURCE_AREAS.get(path)


def check_areas(check: str) -> frozenset[str] | None:
    if check.startswith("frontend:"):
        name = check.split(":", 1)[1]
        return AREAS if name in OPTIONAL_FRONTEND and name not in CORE_FRONTEND else None
    if check in CORE_TESTS or any(word in check for word in (
            "permission", "auth", "identity", "privacy", "isolation", "notice", "migration",
            "store", "storage", "update", "access", "preservation", "packaging")):
        return None
    for prefixes, areas in TEST_AREAS:
        if check.startswith(prefixes):
            return areas
    return None


def fingerprints(state: dict, checks: tuple[str, ...]) -> dict[str, str]:
    common = {name: value for name, value in state["files"].items() if source_area(name) is None}
    common_key = _digest([VERSION, state["root"], state["environment"], checks, common])
    area_keys = {area: _digest({name: value for name, value in state["files"].items()
                               if source_area(name) == area}) for area in AREAS}
    return {check: _digest([common_key, {area: area_keys[area] for area in sorted(check_areas(check) or AREAS)}])
            for check in checks}


class Selection:
    def __init__(self, state: dict | None, checks, cache: Path, *, full=False):
        self.state, self.cache = state, cache
        self.checks = tuple(dict.fromkeys(checks))
        self.completed: set[str] = set()
        self.keys = fingerprints(state, self.checks) if state else {}
        previous = self._read()
        self.previous = previous
        self.full = True
        unchanged = False
        self.reason = "显式 FULL" if full else "缺少可信完整基线或指纹不可用"
        if not full and state and previous:
            old_files = previous["files"]
            changed = {path for path in set(old_files) | set(state["files"])
                       if old_files.get(path) != state["files"].get(path)}
            age = time.time() - previous["full_passed_at"]
            if age < 0 or age >= FULL_BASELINE_MAX_AGE:
                self.reason = "完整测试基线已过期或系统时间发生变化"
            elif previous["environment"] != state["environment"]:
                self.reason = "执行环境或已安装依赖发生变化"
            elif set(old_files) != set(state["files"]):
                self.reason = "文件新增或删除"
            elif any(source_area(path) is None for path in changed):
                self.reason = "共享、测试、配置、依赖或未知文件发生变化"
            else:
                self.full = False
                unchanged = not changed
                self.reason = "已知业务模块改动" if changed else "源文件及环境完全一致"
        self.selected = frozenset(check for check in self.checks
            if self.full or check.startswith("safety:") or check in EXACT_RECHECK_TESTS
            or (not unchanged and check_areas(check) is None)
            or previous["coverage"].get(check) != self.keys[check])

    def _read(self) -> dict:
        try:
            key = self.cache.with_suffix(".key").read_bytes()
            envelope = json.loads(self.cache.read_text(encoding="utf-8"))
            payload = envelope["payload"]
            if len(key) != 32 or not hmac.compare_digest(envelope["signature"],
                    hmac.new(key, _digest(payload).encode(), "sha256").hexdigest()):
                return {}
            if (payload["version"] != VERSION or not self.state
                    or not isinstance(payload["full_passed_at"], (int, float))
                    or isinstance(payload["full_passed_at"], bool)
                    or not 0 < payload["full_passed_at"] < 10_000_000_000
                    or payload["root"] != self.state["root"]
                    or payload["checks"] != list(self.checks)
                    or set(payload["coverage"]) != set(self.checks)
                    or payload["coverage"] != fingerprints(payload, self.checks)):
                return {}
            return payload
        except (OSError, ValueError, KeyError, TypeError, AttributeError):
            return {}

    def passed(self, *checks: str) -> None:
        if not set(checks) <= self.selected:
            raise RuntimeError("预检回执包含未选择的检查")
        self.completed.update(checks)

    def finish(self, current: dict | None) -> None:
        if self.completed != self.selected:
            raise RuntimeError("预检存在未完成的检查，不能记录通过或继续打包")
        if self.state is None:
            return
        if current != self.state:
            raise RuntimeError("预检执行期间源文件或环境发生变化，请重新执行；未更新通过基线")
        self.cache.parent.mkdir(parents=True, exist_ok=True)
        key_path = self.cache.with_suffix(".key")
        try:
            key = key_path.read_bytes()
        except FileNotFoundError:
            key = b""
        if len(key) != 32:
            key = secrets.token_bytes(32)
            key_path.write_bytes(key)
        payload = {"version": VERSION, "root": current["root"],
                   "environment": current["environment"], "files": current["files"],
                   "checks": list(self.checks), "coverage": self.keys,
                   "full_passed_at": time.time() if self.full else self.previous["full_passed_at"]}
        envelope = {"payload": payload, "signature": hmac.new(key, _digest(payload).encode(), "sha256").hexdigest()}
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=self.cache.parent,
                                             prefix=".preflight-", delete=False) as stream:
                temporary = Path(stream.name)
                json.dump(envelope, stream, sort_keys=True)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.cache)
        finally:
            if temporary:
                temporary.unlink(missing_ok=True)
