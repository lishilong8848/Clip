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
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor


VERSION = 3
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
    "check-test-server-ports.mjs",
    "check-workspace-palettes.mjs",
    "check-workspace-navigation.mjs", "check-login-version-switch.mjs", "check-legacy-module-entry.mjs",
    "check-notice-photo-paste.mjs", "check-legacy-notice-cycle.mjs",
    "check-notice-cycle-warnings.mjs", "check-account-fixed-duty.mjs",
    "check-notice-convergence-confirmation.mjs", "check-assistant-shared-query.mjs",
    "check-document-library-all-scopes.mjs",
))
OPTIONAL_FRONTEND = frozenset((
    "check-repair-overview.mjs", "check-document-library-resilience.mjs",
    "check-learning-sharing-types.mjs", "check-learning-sharing-resilience.mjs",
    "check-learning-question-import.mjs", "check-learning-daily-entry.mjs", "check-pre-shift-edits.mjs", "check-repair-plan-binding.mjs",
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
PROBE_LABELS = {
    "version": "Python版本", "platform": "操作系统", "executable": "Python解释器路径",
    "paths": "Python导入路径", "stdlib": "Python标准库路径", "timezone": "系统时区名称",
    "utc_offset": "系统UTC偏移", "locale": "区域设置", "encoding": "默认文本编码",
}


class PreflightChanged(RuntimeError):
    """Checks passed, but their inputs changed; never a passing receipt."""
    def __init__(self, message, *, retryable=True):
        super().__init__(message)
        self.retryable = retryable


def run_with_stable_inputs(check, *, log=print):
    """Retry one fresh selection on drift, never on a failing check."""
    try:
        return check()
    except PreflightChanged as exc:
        if not exc.retryable:
            raise
        log(str(exc))
        log("预检期间文件有更新，自动重新核对一次；仅复用已有可信基线，变化部分重新检查。请暂勿修改源码或重建前端。")
    return check()


def loaded_project_sources(root: str) -> set[str]:
    """Imported release helpers keep old code until the packager is restarted."""
    base = Path(root).resolve()
    sources = {'package_portable.py', 'packaging_preflight.py'}
    for module in tuple(sys.modules.values()):
        filename = getattr(module, '__file__', None)
        if not isinstance(filename, (str, Path)):
            continue
        try:
            path = Path(filename).resolve()
            if path.suffix == '.pyc':
                from importlib.util import source_from_cache
                path = Path(source_from_cache(str(path)))
            sources.add(path.relative_to(base).as_posix())
        except (ValueError, OSError):
            continue
    return sources


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
        exclude = ("b", "daemon", "**/debug.log") if path in browser_caches else ()
        dependencies[str(path)] = _tree(path, exclude=exclude) if path.is_dir() else None
    # Only hashes are persisted, never environment variables or private config.
    # The host's per-command trace ID does not configure Python, Node or checks.
    variables = {key: value for key, value in env.items() if key != "CODEX_TOOL_CALL_ID"}
    environment = _digest({"probe": info, "variables": variables, "dependencies": dependencies})
    files = _tree(root, project=True)
    return {"root": str(root), "environment": environment,
            "files": {name: value[0] for name, value in files.items()},
            "stamps": {name: value[1] for name, value in files.items()},
            "environment_details": {"probe": _digest(info),
                "probe_fields": {name: _digest(value) for name, value in info.items()},
                "variables": {name: _digest(value) for name, value in variables.items()},
                "dependencies": dependencies}}


def same_inputs(before: dict, after: dict | None) -> bool:
    if after is None:
        return False
    # A byte-identical frontend rebuild may touch every emitted asset. Content,
    # additions/deletions and all source/dependency stamps still remain checked.
    def significant(state):
        return {**state, 'stamps': {name: stamp for name, stamp in state.get('stamps', {}).items()
                                  if not name.startswith(FRONTEND + 'dist/')}}
    return significant(before) == significant(after)


def changes(before: dict, after: dict | None) -> dict[str, list[str]]:
    """Identify drift without writing environment values, credentials or file contents."""
    if after is None:
        return {"指纹读取失败": ["检查结束后未取得可信指纹"]}
    result = {}
    def add(kind, path):
        result.setdefault(kind, []).append(str(path))
    if before.get("root") != after.get("root"):
        add("项目目录变化", after.get("root", ""))
    old, new = before.get("files", {}), after.get("files", {})
    for name in sorted(set(old) | set(new)):
        if name not in old:
            add("项目文件新增", name)
        elif name not in new:
            add("项目文件删除", name)
        elif old[name] != new[name]:
            add("项目文件内容变化", name)
        elif before.get("stamps", {}).get(name) != after.get("stamps", {}).get(name):
            add("项目文件时间或状态变化", name)
    if before.get("environment") != after.get("environment"):
        a, b = before.get("environment_details"), after.get("environment_details")
        if not isinstance(a, dict) or not isinstance(b, dict):
            add("执行环境变化", "原指纹没有环境明细，无法还原具体依赖")
        else:
            if a.get("probe") != b.get("probe"):
                ap, bp = a.get("probe_fields"), b.get("probe_fields")
                fields = sorted(name for name in set(ap) | set(bp) if ap.get(name) != bp.get(name)) \
                    if isinstance(ap, dict) and isinstance(bp, dict) else []
                for name in fields:
                    add("Python运行环境变化", f"{PROBE_LABELS.get(name, '运行参数')}（{name}）")
                if not fields:
                    add("Python运行环境变化", "解释器、导入路径、系统或时区（原指纹未记录分项）")
            av, bv = a.get("variables", {}), b.get("variables", {})
            for name in sorted(set(av) | set(bv)):
                if av.get(name) != bv.get(name):
                    add("环境变量变化（仅名称）", name)
            ad, bd = a.get("dependencies", {}), b.get("dependencies", {})
            for root in sorted(set(ad) | set(bd)):
                left, right = ad.get(root), bd.get(root)
                if isinstance(left, dict) and isinstance(right, dict):
                    pairs = ((str(Path(root) / name), left.get(name), right.get(name))
                             for name in sorted(set(left) | set(right)))
                else:
                    pairs = ((root, left, right),)
                for name, previous, current in pairs:
                    if previous == current:
                        continue
                    metadata_only = (isinstance(previous, (tuple, list)) and isinstance(current, (tuple, list))
                                     and previous[0] == current[0])
                    add("依赖文件时间或状态变化" if metadata_only else "依赖文件新增、删除或内容变化", name)
            if not result:
                add("执行环境变化", "环境摘要不一致")
    return result


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
        if not same_inputs(self.state, current):
            details = changes(self.state, current)
            report = self.cache.with_name('.preflight-changes.json')
            try:
                report.parent.mkdir(parents=True, exist_ok=True)
                report.write_text(json.dumps({'created_at': time.time(), 'changes': {
                    kind: {'count': len(paths), 'paths': paths[:200]} for kind, paths in details.items()}},
                    ensure_ascii=False, indent=2), encoding='utf-8')
            except OSError:
                report = None
            summary = [f"{kind}（{len(paths)}项）：" + '、'.join(
                name.replace('\r', r'\r').replace('\n', r'\n') for name in paths[:8])
                for kind, paths in details.items()]
            if report:
                summary.append(f"变化明细已保存: {report}")
            # The running packager/imported dependencies cannot hot-reload safely.
            retryable = bool(current and current.get('environment') == self.state.get('environment')
                and all(current['files'].get(name) == self.state['files'].get(name)
                        for name in loaded_project_sources(self.state['root'])))
            if not retryable:
                summary.append('打包程序或执行环境已改变，请重新运行打包程序，不能在旧进程中继续。')
            raise PreflightChanged("预检执行期间源文件或环境发生变化；未更新通过基线\n" + '\n'.join(summary), retryable=retryable)
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
