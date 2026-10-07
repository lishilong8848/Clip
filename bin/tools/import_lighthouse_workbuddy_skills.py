"""Guidance-only importer for WorkBuddy skills into the LightHouse openclaw bundle.

This is a bounded import-only tool.  It inventories SKILL.md files (including
nested ones) under a source skills root, audits every candidate resource and
copies ONLY safe, non-executable text guidance/resources into:

    <output>/workbuddy/                    (managed workbuddy subtree)
    <output>/workbuddy-registry.json

Design principles
-----------------
* Guidance-only.  The bundle never grants permission to run scripts or write
  anywhere; every imported record is ``kind: workflow_guide`` and executable
  sources are never imported.  They are instead recorded as
  ``missing_executable_dependencies`` in the metadata.  This tool does not
  claim to produce a functional CLI.
* Deterministic.  Sorting everywhere is by POSIX-relative path.  Re-running the
  importer produces byte-identical output.
* Safe by default.  The ``zhinav-point-data`` and ``告警描述核查`` source roots
  (and all of their descendant skills) are always excluded before any
  SKILL.md is opened, with reason ``user excluded``.  There is intentionally no
  flag to re-enable them.
* Audited.  Every plan is verified before copying: pruned walking that never
  crosses symlinks/junctions/reparse points, resolved-path containment, size
  guards performed before any read, hardcoded-credential scans (filename +
  reason only, never secret values), and fail-closed frontmatter parsing.
* Safe source preservation.  The source tree is only ever read; application
  writes to a staging subtree and atomically swaps it into place.

Usage
-----
    python import_lighthouse_workbuddy_skills.py --source <skills> --output <out> --dry-run
    python import_lighthouse_workbuddy_skills.py --source <skills> --output <out> --apply

Defaults: --source C:/Users/l1773/.workbuddy/skills
          --output bin/openclaw_service/assistant/openclaw/skills
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import sys
import unicodedata
import uuid
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

import yaml

# ---------------------------------------------------------------------------
# Reasonable limits
# ---------------------------------------------------------------------------
MAX_FILE_BYTES = 512 * 1024        # per copied resource file
MAX_SKILL_BYTES = 4 * 1024 * 1024  # cumulative text bytes for one skill
MAX_TOTAL_BYTES = 40 * 1024 * 1024 # cumulative text bytes for the whole job
MAX_SKILLS = 128                   # sanity bound on the number of skills
MAX_ID_LEN = 61                    # matches [a-z][a-z0-9-]{0,60}

# ---------------------------------------------------------------------------
# Root-level exclusions applied by default, before SKILL.md is ever opened.
# ---------------------------------------------------------------------------
USER_EXCLUDED_ROOTS = {"zhinav-point-data", "告警描述核查"}
CURATED_FRONTMATTER = {
    'agent-browser__skillhub': {
        'name': 'agent-browser', 'display_name': 'Agent Browser', 'version': '0.1.0', 'license': 'MIT',
        'description': '使用 agent-browser CLI 的网页自动化工作指南；具体操作需已提供且已授权的浏览器工具。',
    },
    'wps-office-suite__skillhub': {
        'name': 'wps-office-suite', 'display_name': 'WPS Office 全家桶', 'version': '5.2.5',
        'description': 'Word、Excel、PPT 办公文档工作指南；外部 Office 引擎和脚本需独立的受控执行适配。',
    },
}

# ---------------------------------------------------------------------------
# Inclusion / exclusion buckets
# ---------------------------------------------------------------------------
SAFE_TEXT_EXTS = {
    ".md", ".markdown", ".txt", ".yaml", ".yml", ".xml", ".xsd", ".rels",
    ".svg",
}
SAFE_JSON_EXTS = {".json"}
SAFE_NO_EXT_NAMES = {"LICENSE", "NOTICE", "README", "CHANGELOG", "AUTHORS"}

EXEC_BINARY_EXTS = {
    ".py", ".pyc", ".pyo", ".js", ".mjs", ".cjs", ".ts", ".tsx", ".jsx",
    ".cs", ".csproj", ".slnx", ".sh", ".bash", ".ps1", ".bat", ".cmd",
    ".node", ".exe", ".dll", ".so", ".dylib", ".jar", ".class", ".cpp",
    ".c", ".h", ".go", ".rs", ".php", ".rb", ".java", ".kt", ".swift",
    ".html", ".htm", ".css", ".scss", ".vue", ".css.map", ".wasm",
    ".ttf", ".otf", ".woff", ".woff2", ".eot", ".png", ".jpg", ".jpeg",
    ".gif", ".webp", ".ico", ".icns", ".pdf", ".xlsx", ".xls", ".docx",
    ".doc", ".pptx", ".zip", ".tar", ".gz", ".bz2", ".7z", ".sqlite",
    ".db", ".db3", ".sql", ".lock", ".template",
}

# Resources that, when present, represent an executable the distilled guide
# would depend on running.  Recorded as ``missing_executable_dependencies``;
# pure binary/media assets (e.g. png/pdf) are excluded but are not themselves
# executable dependencies.
EXECUTABLE_DEP_EXTS = {
    ".py", ".js", ".mjs", ".cjs", ".ts", ".tsx", ".jsx",
    ".cs", ".csproj", ".sh", ".bash", ".ps1", ".bat", ".cmd",
    ".exe", ".jar", ".class", ".go", ".rs", ".php", ".rb",
    ".java", ".kt", ".swift", ".wasm", ".c", ".cpp", ".h",
}

SKIP_DIRS = {
    ".git", ".hg", ".svn", "__pycache__", ".pytest_cache", ".mypy_cache",
    "node_modules", ".venv", "venv", "env", ".claude-plugin", ".idea",
    ".vscode", "dist", "build", ".tox", ".eggs", ".ruff_cache",
}

SKIP_FILES_EXACT = {
    ".gitignore", ".gitattributes", ".gitmodules", ".DS_Store", ".coverage",
    "_skillhub_meta.json", "_meta.json", "_bm_skillid_migration.json",
    "package-lock.json", "yarn.lock", "pnpm-lock.yaml", "npm-shrinkwrap.json",
    "workbuddy.json",
}

CONFIG_NAME_SUBSTRINGS = ("_config.", "-config.", "config.json", ".config.")

# ---------------------------------------------------------------------------
# Credential audit (filename + reason only; secrets are never surfaced)
# ---------------------------------------------------------------------------
# Only clearly-synthetic placeholders are exempt.  A placeholder must contain
# a strong placeholder marker; merely containing the substring "example"
# inside an otherwise long key is NOT enough to be treated as a placeholder.
_PLACEHOLDER_TOKEN_RE = re.compile(
    r"(?i)(your[_-]?key|your[_-]?token|your[_-]?secret|your[_-]?id|replace_?\s+"
    r"|sample[_ -]?key|sample[_ -]?token|demo[_ -]?key|demo[_ -]?token"
    r"|sk-live-[a-z0-9]{0,8}changeme|xxx+|<[^>]+>|\$\{[^}]+\}"
    r"|your[_-]?api[_-]?key|example[_-]?api[_-]?key"
    r"|example[_-]?secret|sample[_-]?secret)"
)


def _is_placeholder(value: str) -> bool:
    """True only for clearly synthetic placeholder placeholders.

    A bare long token (e.g. ``sk-live-9f8...``) is never a placeholder even if
    it happens to be followed by the word ``example`` in surrounding prose,
    because we only ever inspect the matched credential token itself.
    """
    return bool(_PLACEHOLDER_TOKEN_RE.search(value))


_CRED_PATTERNS = [
    re.compile(r"AKIA[0-9A-Z]{16}"),
    re.compile(r"ASIA[0-9A-Z]{16}"),
    re.compile(r"ghp_[A-Za-z0-9]{30,}"),
    re.compile(r"xox[baprs]-[A-Za-z0-9]{10,}"),
    re.compile(r"-----BEGIN (?:RSA |OPENSSH |EC |DSA )?PRIVATE KEY-----"),
    re.compile(r"cli_[A-Za-z0-9]{16,}"),
    re.compile(r"sk-[A-Za-z0-9_\-]{24,}"),
    re.compile(
        r"(?i)\bAuthorization[ \t]*:[ \t]*Bearer[ \t]+[A-Za-z0-9._~+/=_-]{12,}"
    ),
    re.compile(
        r"(?i)(?:api[_-]?key|app[_-]?secret|access[_-]?token|auth[_-]?token|"
        r"client[_-]?secret|client[_-]?id|password|passwd|secret)"
        r"[ \t]*[:=][ \t]*['\"][A-Za-z0-9_\-.+]{12,}"
    ),
    re.compile(
        r"(?i)(?:app[_-]?token|table[_-]?id|wiki[_-]?token)"
        r"[ \t]*[:=][ \t]*['\"][A-Za-z0-9]{16,}"
    ),
]


def _scan_for_credentials(text: str) -> Optional[str]:
    """Return a short reason string if the text looks credential-bearing."""
    for pattern in _CRED_PATTERNS:
        for match in pattern.finditer(text):
            context = match.group(0)
            if _is_placeholder(context):
                continue
            if "PRIVATE KEY" in context:
                return "private key material"
            if context.startswith("AKIA") or context.startswith("ASIA"):
                return "AWS access key id"
            if context.startswith("ghp_"):
                return "GitHub token"
            if context.startswith("xox"):
                return "Slack token"
            if context.startswith("cli_"):
                return "app id"
            if context.lower().startswith("sk-"):
                return "OpenAI-style API key"
            if context.lower().startswith("authorization") and "bearer" in context.lower():
                return "bearer token"
            if "secret" in context.lower() or "_SECRET" in context.upper():
                return "hardcoded secret"
            if "token" in context.lower():
                return "hardcoded token/id"
            return "likely credential assignment"
    return None


# ---------------------------------------------------------------------------
# Path helpers (reparse-safe, resolved containment)
# ---------------------------------------------------------------------------
try:
    import ctypes
    from ctypes import wintypes
    _FILE_ATTRIBUTE_REPARSE_POINT = 0x400
    _INVALID_FILE_ATTRIBUTES = 0xFFFFFFFF

    def _win_reparse_attr(path: str) -> bool:
        """True when the Windows file attribute REPARSE_POINT is set."""
        try:
            attrs = ctypes.windll.kernel32.GetFileAttributesW(wintypes.LPCWSTR(path))
            if attrs == _INVALID_FILE_ATTRIBUTES:
                return True  # treat unavailable/invalid as unsafe
            return bool(attrs & _FILE_ATTRIBUTE_REPARSE_POINT)
        except Exception:
            return True  # fail closed if the API cannot be queried
except Exception:  # pragma: no cover - non-Windows
    def _win_reparse_attr(path: str) -> bool:
        return False


def _is_reparse(path: Path) -> bool:
    """True for symlinks and any Windows reparse point (junction included).

    ``is_symlink``/``is_junction`` alone are insufficient because a directory
    mounted as an arbitrary reparse point (e.g. in a container/sandbox) has the
    FILE_ATTRIBUTE_REPARSE_POINT flag set without being either.  A path whose
    attributes cannot be read is treated as unsafe (fail closed).
    """
    try:
        if path.is_symlink() or path.is_junction():
            return True
        if os.name == "nt" and _win_reparse_attr(str(path)):
            return True
    except OSError:
        return True
    return False


def _is_within(path: Path, root: Path) -> bool:
    return path == root or path.is_relative_to(root)


def _raise_oserror(exc: OSError) -> None:
    raise exc


def _walk_pruned(root: Path) -> Iterable[Tuple[Path, List[str], List[str]]]:
    """Yield (dir_path, dirnames, filenames) without entering reparse points.

    ``os.walk`` is used with ``followlinks=False``; any directory that is a
    symlink/junction/reparse point or a known local/cache/lock directory
    (SKIP_DIRS) is dropped from ``dirnames`` before returning from the
    enclosing directory.  Resolved paths are recomputed fresh per directory
    (no staled cache is carried across recursion).
    """
    for dirpath, dirnames, filenames in os.walk(
        root, topdown=True, followlinks=False, onerror=_raise_oserror
    ):
        dir_path = Path(dirpath)
        keep = [
            d for d in dirnames
            if not _is_reparse(dir_path / d) and d not in SKIP_DIRS
        ]
        dirnames[:] = sorted(keep)
        yield dir_path, dirnames, filenames


def _checked_read(path: Path, max_bytes: int) -> Tuple[Optional[bytes], Optional[str]]:
    """Size-check before reading; never follow reparse targets.

    The read is still bounded: only ``max_bytes + 1`` bytes are requested so a
    misreported size can never cause us to allocate more than the boundary.
    """
    if _is_reparse(path):
        return None, "symbolic link / reparse point"
    try:
        size = path.stat().st_size
    except OSError as exc:
        return None, f"unreadable ({exc.strerror or 'io'})"
    if size > max_bytes:
        return None, f"file exceeds {max_bytes} bytes"
    try:
        with path.open("rb") as fh:
            data = fh.read(max_bytes + 1)
    except OSError as exc:
        return None, f"unreadable ({exc.strerror or 'io'})"
    if len(data) > max_bytes:
        return None, f"file exceeds {max_bytes} bytes"
    return data, None


# ---------------------------------------------------------------------------
# Slug / stable-ID helpers
# ---------------------------------------------------------------------------
def _safe_ascii(value: str) -> str:
    """Best-effort ASCII slug (Chinese-only input yields an empty string)."""
    normalized = unicodedata.normalize("NFKD", value)
    ascii_chars = []
    for ch in normalized:
        if "a" <= ch <= "z" or "A" <= ch <= "Z" or "0" <= ch <= "9":
            ascii_chars.append(ch.lower())
        elif ch in ("-", "_", " ", "."):
            ascii_chars.append("-")
    return re.sub(r"-+", "-", "".join(ascii_chars)).strip("-")


def _stable_id(rel: str) -> str:
    """Deterministic ID derived only from the relative skill root.

    The ID is stable with respect to unrelated folders being inserted and is
    collision-resistant across underscore/hyphen slug collisions by hashing the
    full relative path.  The whole ID matches ``[a-z][a-z0-9-]{0,60}``.
    """
    folder = Path(rel).name
    base = (_safe_ascii(folder) or "skill")[:42]
    digest = hashlib.sha256(rel.encode("utf-8")).hexdigest()[:8]
    return f"workbuddy-{base}-{digest}"


def _stable_ids(rel_dirs: List[str]) -> Dict[str, str]:
    mapping = {rel: _stable_id(rel) for rel in rel_dirs}
    if len(set(mapping.values())) != len(mapping):
        raise ValueError("stable ID collision detected; refusing to proceed")
    for ident in mapping.values():
        if not re.fullmatch(r"[a-z][a-z0-9-]{0,60}", ident) or len(ident) > MAX_ID_LEN:
            raise ValueError(f"invalid generated ID length/format: {len(ident)}")
    return mapping


# ---------------------------------------------------------------------------
# User-excluded roots (applied before SKILL.md is opened)
# ---------------------------------------------------------------------------
def _is_user_excluded(rel: Path) -> bool:
    return any(part in USER_EXCLUDED_ROOTS for part in rel.parts)


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------
def discover_skills(source_root: Path) -> List[Path]:
    """Return all SKILL.md-containing directories, deterministically sorted.

    Only pruned, reparse-safe walking is used; any resolved directory that
    escapes the source root is ignored.  Symlinked/reparse SKILL.md files are
    not treated as skill roots.
    """
    source_root = source_root.resolve()
    discovered: Set[Path] = set()
    for dir_path, _dirnames, filenames in _walk_pruned(source_root):
        if "SKILL.md" not in filenames:
            continue
        skill_md = dir_path / "SKILL.md"
        if _is_reparse(skill_md):
            continue
        resolved_dir = dir_path.resolve()
        if not _is_within(resolved_dir, source_root):
            continue
        discovered.add(resolved_dir)
    ordered = sorted(
        discovered, key=lambda p: _posix(p.relative_to(source_root))
    )
    if len(ordered) > MAX_SKILLS:
        raise ValueError(f"refusing to import > {MAX_SKILLS} skills ({len(ordered)} found)")
    return ordered


def _posix(path: Path) -> str:
    return path.as_posix()


# ---------------------------------------------------------------------------
# Per-file include/exclude decisions
# ---------------------------------------------------------------------------
def _is_excluded_by_name(rel: Path) -> Optional[str]:
    parts = rel.parts
    for part in parts:
        if part in SKIP_DIRS:
            return "local/cache/lock directory"
    name = rel.name.lower()
    if rel.name in SKIP_FILES_EXACT:
        return "local metadata/config file"
    if rel.name.startswith(".") and rel.suffix in ("", ".json", ".md"):
        return "dotfile metadata"
    for part in CONFIG_NAME_SUBSTRINGS:
        if part in name:
            return "local config file"
    lowered = rel.as_posix().lower()
    if ".env" in lowered or lowered.startswith("env."):
        return "environment config file"
    if name.endswith((".key", ".pem", ".crt", ".p12", ".pfx", ".cer", ".jks")):
        return "private key / certificate material"
    if name.endswith((".sqlite", ".sqlite3", ".db", ".db3")):
        return "database file"
    if name.endswith((".log", ".pyc", ".pyo")):
        return "generated/cache file"
    return None


def _is_safe_extension(rel: Path) -> bool:
    return rel.suffix.lower() in SAFE_TEXT_EXTS or rel.suffix.lower() in SAFE_JSON_EXTS


def _is_unsafe_extension(rel: Path) -> bool:
    return rel.suffix.lower() in EXEC_BINARY_EXTS


# ---------------------------------------------------------------------------
# Frontmatter (fail-closed; no tolerant parser)
# ---------------------------------------------------------------------------
class MalformedFrontmatterError(ValueError):
    """Raised when SKILL.md frontmatter cannot be parsed safely."""


def parse_frontmatter(skill_md: str) -> Dict[str, object]:
    """Parse the leading YAML front matter with safe_load.

    Fail-closed by design: missing frontmatter, an empty frontmatter, a
    non-mapping frontmatter, malformed YAML, and a missing ``name`` or
    ``description`` all raise :class:`MalformedFrontmatterError` (reason only,
    never the malformed contents).  There is deliberately no tolerant fallback
    parser.  A UTF-8 BOM is stripped first, and the ``---`` delimiters must be
    standalone lines (``---evil`` is not a delimiter).
    """
    if skill_md.startswith("\ufeff"):
        skill_md = skill_md[1:]
    lines = skill_md.splitlines()
    if not lines or lines[0].rstrip("\r\n ") != "---":
        raise MalformedFrontmatterError("missing YAML frontmatter")
    body_lines: List[str] = []
    closed = False
    for ln in lines[1:]:
        if ln.rstrip("\r\n ") == "---":
            closed = True
            break
        body_lines.append(ln)
    if not closed:
        raise MalformedFrontmatterError("missing closing '---' in frontmatter")
    raw = "\n".join(body_lines)
    if not raw.strip():
        raise MalformedFrontmatterError("empty YAML frontmatter")
    try:
        data = yaml.safe_load(raw)
    except yaml.YAMLError as exc:
        raise MalformedFrontmatterError("malformed YAML frontmatter") from exc
    if data is None:
        raise MalformedFrontmatterError("empty YAML frontmatter")
    if not isinstance(data, dict):
        raise MalformedFrontmatterError("frontmatter is not a mapping")
    result = {str(k): v for k, v in data.items()}
    if not _as_text(result.get("name")):
        raise MalformedFrontmatterError("frontmatter missing name")
    if not _as_text(result.get("description")):
        raise MalformedFrontmatterError("frontmatter missing description")
    return result


def _as_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        return " ".join(str(v) for v in value)
    if isinstance(value, dict):
        return ""
    return str(value).strip()


# ---------------------------------------------------------------------------
# Hashing
# ---------------------------------------------------------------------------
def _sha256_bundle(copied: List[Tuple[str, bytes]]) -> str:
    digest = hashlib.sha256()
    for rel, content in copied:
        digest.update(rel.encode("utf-8"))
        digest.update(b"\x00")
        digest.update(content)
        digest.update(b"\x00")
    return digest.hexdigest()


# ---------------------------------------------------------------------------
# Skill planning
# ---------------------------------------------------------------------------
def _empty_plan(skill_id: str, skill_root_abs: Path, source_root: Path,
                reason: str) -> dict:
    rel = _posix(skill_root_abs.relative_to(source_root))
    return {
        "id": skill_id,
        "name": "",
        "display_name": "",
        "description": "",
        "source": str(skill_root_abs),
        "rel_root": rel,
        "excluded": True,
        "exclude_reason": reason,
        "path": "",
        "references": [],
        "excluded_files": [{"path": rel, "reason": reason}],
        "copied": [],
        "content_hash": "",
        "size": 0,
        "kind": "workflow_guide",
        "missing_executable_dependencies": [],
    }


def _plan_walk(skill_root: Path, other_skill_roots: Set[Path],
               excluded_files: List[Dict[str, str]]) -> Iterable[Tuple[Path, List[str]]]:
    """Top-down walk that prunes reparse/SKIP/nested/user-excluded directories.

    Skipped directory entries are recorded in ``excluded_files`` (reason only)
    and are never descended into, so descendant resources cannot leak into a
    parent skill's copied set (the user-excluded leak described in review).
    """
    for dirpath, dirnames, filenames in os.walk(
        skill_root, topdown=True, followlinks=False, onerror=_raise_oserror
    ):
        dir_path = Path(dirpath)
        keep = []
        for d in sorted(dirnames):
            child = dir_path / d
            rel_child = child.relative_to(skill_root)
            if _is_reparse(child):
                excluded_files.append({"path": _posix(rel_child),
                                       "reason": "symbolic link / reparse point"})
                continue
            if d in SKIP_DIRS:
                excluded_files.append({"path": _posix(rel_child),
                                       "reason": "local/cache/lock directory"})
                continue
            resolved_child = child.resolve()
            if resolved_child in other_skill_roots:
                excluded_files.append({"path": _posix(rel_child),
                                       "reason": "nested skill root (separate skill)"})
                continue
            if _is_user_excluded(rel_child):
                excluded_files.append({"path": _posix(rel_child),
                                       "reason": "user excluded"})
                continue
            keep.append(d)
        dirnames[:] = sorted(keep)
        yield dir_path, filenames


def plan_skill(source_root: Path, skill_root_abs: Path, skill_id: str,
               other_skill_roots: Set[Path], *, reviewed=None) -> dict:
    """Build a full read-only plan for one skill (never writes anything)."""
    source_root = source_root.resolve()
    skill_rel_dir = skill_root_abs.relative_to(source_root)
    skill_md_path = skill_root_abs / "SKILL.md"

    if _is_user_excluded(skill_rel_dir):
        # Applied before SKILL.md is ever opened, per review requirement.
        return _empty_plan(skill_id, skill_root_abs, source_root, "user excluded")

    if not skill_md_path.is_file():
        raise FileNotFoundError(f"missing SKILL.md in {skill_root_abs}")
    if _is_reparse(skill_md_path):
        raise ValueError(
            f"symbolic link / reparse SKILL.md not allowed: {_posix(skill_rel_dir / 'SKILL.md')}"
        )

    skill_md_bytes, size_reason = _checked_read(skill_md_path, MAX_FILE_BYTES)
    if skill_md_bytes is None:
        return _empty_plan(skill_id, skill_root_abs, source_root,
                           f"SKILL.md {size_reason}")
    original_hash = hashlib.sha256(skill_md_bytes.decode('utf-8-sig').replace('\r\n', '\n').encode()).hexdigest()
    if reviewed is not None and reviewed.get('source_hash') != original_hash:
        return _empty_plan(skill_id, skill_root_abs, source_root, 'source differs from reviewed skill')

    secret_reason = _scan_for_credentials(skill_md_bytes.decode("utf-8", errors="replace"))
    if secret_reason:
        reason = f"SKILL.md contains {secret_reason}"
        plan = _empty_plan(skill_id, skill_root_abs, source_root, reason)
        plan["excluded_files"] = [{
            "path": _posix(skill_rel_dir / "SKILL.md"),
            "reason": f"hardcoded {secret_reason}",
        }]
        return plan

    metadata_repaired = False
    try:
        front = parse_frontmatter(skill_md_bytes.decode("utf-8", errors="replace"))
    except MalformedFrontmatterError as exc:
        if reviewed is not None and _posix(skill_rel_dir) in CURATED_FRONTMATTER:
            # Only the two explicitly reviewed metadata defects are repaired.
            lines = skill_md_bytes.decode('utf-8-sig').splitlines()
            end = next((i for i, line in enumerate(lines[1:], 1) if line.strip() == '---'), None)
            if end is None:
                return _empty_plan(skill_id, skill_root_abs, source_root, 'missing reviewed metadata boundary')
            front = CURATED_FRONTMATTER[_posix(skill_rel_dir)]
            metadata_repaired = True
            skill_md_bytes = ('---\n' + yaml.safe_dump(front, allow_unicode=True, sort_keys=False) +
                              '---\n' + '\n'.join(lines[end + 1:]) + '\n').encode('utf-8')
        else:
            # Reason-only message; never echoes malformed or secret contents.
            return _empty_plan(skill_id, skill_root_abs, source_root,
                               f"invalid SKILL.md frontmatter ({exc})")

    name = _as_text(front.get("name")) or skill_rel_dir.name
    display_name = (_as_text(front.get("display_name_zh"))
                    or _as_text(front.get("display_name"))
                    or name)
    description = (_as_text(front.get("description_zh"))
                   or _as_text(front.get("description"))
                   or _as_text(front.get("summary"))
                   or name)

    copied: List[Tuple[str, bytes]] = [("SKILL.md", skill_md_bytes)]
    excluded_files: List[Dict[str, str]] = []
    references: List[str] = []
    missing_exec: List[str] = []

    def _audit_file(path: Path, rel: Path) -> None:
        posix_rel = _posix(rel)
        if _is_reparse(path):
            excluded_files.append({"path": posix_rel, "reason": "symbolic link / reparse point"})
            return
        try:
            resolved = path.resolve()
        except OSError:
            excluded_files.append({"path": posix_rel, "reason": "unreadable (resolve)"})
            return
        if not _is_within(resolved, source_root):
            excluded_files.append({"path": posix_rel, "reason": "escapes source root"})
            return
        reason = _is_excluded_by_name(rel)
        if reason:
            excluded_files.append({"path": posix_rel, "reason": reason})
            return
        if _is_unsafe_extension(rel):
            excluded_files.append({"path": posix_rel, "reason": "executable/binary resource"})
            if rel.suffix.lower() in EXECUTABLE_DEP_EXTS:
                missing_exec.append(posix_rel)
            return
        if not _is_safe_extension(rel) and rel.name not in SAFE_NO_EXT_NAMES:
            excluded_files.append({
                "path": posix_rel,
                "reason": f"unsupported resource type ({rel.suffix or 'none'})",
            })
            return
        data, read_reason = _checked_read(path, MAX_FILE_BYTES)
        if data is None:
            excluded_files.append({"path": posix_rel, "reason": read_reason})
            return
        text = data.decode("utf-8", errors="replace")
        sec = _scan_for_credentials(text)
        if sec:
            excluded_files.append({"path": posix_rel, "reason": f"hardcoded {sec}"})
            return
        if not text.strip():
            excluded_files.append({"path": posix_rel, "reason": "empty resource"})
            return
        copied.append((posix_rel, data))
        references.append(posix_rel)

    for dir_path, filenames in _plan_walk(skill_root_abs, other_skill_roots,
                                          excluded_files):
        for fname in sorted(filenames):
            fpath = dir_path / fname
            frel = fpath.relative_to(skill_root_abs)
            if frel == Path("SKILL.md"):
                continue  # handled above
            _audit_file(fpath, frel)

    copied.sort(key=lambda item: item[0])
    references.sort()
    missing_exec = sorted(set(missing_exec))
    excluded_files.sort(key=lambda item: item["path"])
    total_bytes = sum(len(content) for _, content in copied)
    if total_bytes > MAX_SKILL_BYTES:
        raise ValueError(f"skill {skill_id} exceeds {MAX_SKILL_BYTES} bytes of text resources")

    return {
        "id": skill_id,
        "name": name,
        "display_name": display_name,
        "description": description,
        "source": str(skill_root_abs),
        "rel_root": _posix(skill_rel_dir),
        "excluded": False,
        "exclude_reason": None,
        "path": f"workbuddy/{skill_id}/SKILL.md",
        "references": [f"workbuddy/{skill_id}/{ref}" for ref in references],
        "excluded_files": excluded_files,
        "copied": copied,
        "content_hash": _sha256_bundle(copied),
        "size": total_bytes,
        "kind": "workflow_guide",
        "missing_executable_dependencies": missing_exec,
        'original_source_hash': original_hash,
        'execution_status': 'guide_only',
        'source_test_status': (reviewed or {}).get('status', 'not_tested'),
        'warnings': ['仅安装说明和参考资料；不新增脚本执行、浏览器控制或云端写入权限。'] +
                    ([str(reviewed.get('detail', ''))[:400]] if reviewed else []),
        'metadata_repaired': metadata_repaired,
    }


def build_plan(source_root: Path, *, test_report=None) -> List[dict]:
    """Discover skills and plan every import deterministically (read-only)."""
    source_root = source_root.resolve()
    if not source_root.is_dir():
        raise ValueError(f"source directory not found: {source_root}")
    skill_dirs = discover_skills(source_root)
    skill_dirs_set = set(skill_dirs)
    rel_dirs = sorted({_posix(d.relative_to(source_root)) for d in skill_dirs})
    ids = _stable_ids(rel_dirs)
    reviewed = {}
    if test_report is not None:
        report = json.loads(Path(test_report).read_text(encoding='utf-8'))
        if report.get('source_unchanged') is not True or not isinstance(report.get('results'), list):
            raise ValueError('skill test report is incomplete')
        reviewed = {r['skill']: r for r in report['results']}
        if len(reviewed) != len(report['results']):
            raise ValueError('duplicate skill test result')
    plans = [
        plan_skill(source_root, d, ids[_posix(d.relative_to(source_root))], skill_dirs_set,
                   reviewed=reviewed.get(_posix(d.relative_to(source_root)), {}) if test_report else None)
        for d in skill_dirs
    ]
    plans.sort(key=lambda p: p["rel_root"])
    total = sum(p["size"] for p in plans if not p["excluded"])
    if total > MAX_TOTAL_BYTES:
        raise ValueError(f"planned import exceeds {MAX_TOTAL_BYTES} bytes total")
    if len(plans) > MAX_SKILLS:
        raise ValueError("too many skills in plan")
    return plans


# ---------------------------------------------------------------------------
# Apply / output (validated, staging + atomic swap; never touches registry.json)
# ---------------------------------------------------------------------------
def _registry_records(plans: List[dict]) -> List[dict]:
    records = []
    for p in plans:
        if p["excluded"]:
            continue
        records.append({
            "name": p["id"],
            "kind": p["kind"],
            "display_name": p["display_name"],
            "description": p["description"],
            "source": p["rel_root"],
            "path": p["path"],
            "references": p["references"],
            "content_hash": p["content_hash"],
            **{key: p[key] for key in ('original_source_hash', 'execution_status', 'source_test_status', 'warnings', 'metadata_repaired')},
            "missing_executable_dependencies": p["missing_executable_dependencies"],
            "excluded_files": [{"path": f["path"], "reason": f["reason"]}
                               for f in p["excluded_files"]],
        })
    return records


def _reject_reparse_ancestors(path: Path) -> None:
    cur = path
    while True:
        if _is_reparse(cur):
            raise ValueError(f"reparse/symlink/junction destination ancestor: {cur}")
        if cur.parent == cur:
            break
        cur = cur.parent


def _remove_path(path: Path) -> None:
    if _is_reparse(path):
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass
    elif path.is_dir():
        shutil.rmtree(path)
    else:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass


def _staging_tag() -> str:
    return f"{os.getpid()}-{uuid.uuid4().hex[:8]}"


def apply_plan(plans: List[dict], output_root: Path, source_root: Path) -> List[dict]:
    """Write imported skills + registry metadata into a managed workbuddy subtree.

    All safety validations happen before any write.  The fresh managed subtree
    is built in a staging directory and atomically swapped over the previous
    one, so previously imported/now-excluded skills are removed.  Only the
    ``workbuddy/`` subtree and ``workbuddy-registry.json`` are touched; the
    openclaw ``registry.json`` and any other guides are never modified.
    """
    source_root = source_root.resolve()
    # Validate the UNRESOLVED absolute destination ancestors first.  If the
    # caller supplies a symlink as the output root, calling resolve() first
    # would hide the original symlink from ancestor reparse validation.
    output_root_abs = output_root.absolute()
    if output_root_abs.name != "skills":
        raise ValueError("output root must be the openclaw/skills directory")
    _reject_reparse_ancestors(output_root_abs)
    output_root = output_root_abs.resolve()
    if not output_root.is_dir():
        raise ValueError("output root does not exist")
    if (source_root == output_root
            or source_root.is_relative_to(output_root)
            or output_root.is_relative_to(source_root)):
        raise ValueError("source and output roots must be disjoint (no containment)")

    skills_out = output_root / "workbuddy"
    reg_path = output_root / "workbuddy-registry.json"
    for dest in (skills_out, reg_path):
        if dest.exists() and _is_reparse(dest):
            raise ValueError(f"preexisting destination is a symlink/reparse point: {dest}")
    if skills_out.exists() and not skills_out.is_dir():
        raise ValueError(f"managed workbuddy destination is not a directory: {skills_out}")

    # Refuse to proceed if a previous run left recovery state behind.  We never
    # glob-delete arbitrary staging/backup names and never auto-resume, because
    # deleting a leftover backup could erase the only recoverable subtree from a
    # crash between the two renames.
    leftover = (sorted(output_root.glob(".workbuddy-staging-*"))
                + sorted(output_root.glob("workbuddy-backup-*")))
    if leftover:
        names = ", ".join(p.name for p in leftover)
        raise ValueError(
            "leftover recovery state from a previous run exists; refusing to "
            f"overwrite (manual review required): {names}")

    tag = _staging_tag()
    staging = output_root / f".workbuddy-staging-{tag}"
    backup = output_root / f"workbuddy-backup-{tag}"

    # Clean up ONLY the current unique staging/backup paths, and only after
    # verifying bounded containment strictly inside the output root.
    out_resolved = output_root.resolve()
    for artifact in (staging, backup):
        if not _is_within(artifact.resolve(), out_resolved):
            raise ValueError("staging/backup path escapes output root")
        if artifact.exists():
            _remove_path(artifact)

    staging.mkdir()
    try:
        skills_staging = staging / "workbuddy"
        skills_staging.mkdir(parents=True, exist_ok=True)
        for p in plans:
            if p["excluded"]:
                continue
            dest = skills_staging / p["id"]
            dest_resolved = dest.resolve()
            for rel, content in p["copied"]:
                target = (dest / rel).resolve()
                if not _is_within(target, dest_resolved):
                    raise ValueError(f"unsafe destination path: {rel}")
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(content)

        records = _registry_records(plans)
        (staging / "workbuddy-registry.json").write_text(
            json.dumps(records, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

        moved_old = False
        if skills_out.exists() or skills_out.is_symlink():
            skills_out.rename(backup)
            moved_old = True
        try:
            skills_staging.rename(skills_out)
            os.replace(staging / "workbuddy-registry.json", reg_path)
        except Exception:
            # Roll back: restore the previous managed subtree.
            if skills_out.exists():
                _remove_path(skills_out)
            if moved_old and backup.exists():
                backup.rename(skills_out)
            raise

        if staging.exists():
            staging.rmdir()
        if moved_old and backup.exists():
            _remove_path(backup)
        return records
    finally:
        if staging.exists():
            _remove_path(staging)


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------
def dry_run_text(plans: List[dict], source_root: Path) -> str:
    lines = []
    included = [p for p in plans if not p["excluded"]]
    excluded = [p for p in plans if p["excluded"]]
    lines.append("WorkBuddy import dry-run (guidance-only; nothing written)")
    lines.append(f"source: {source_root}")
    lines.append(f"skills found: {len(plans)}  included: {len(included)}  "
                 f"excluded: {len(excluded)}")
    lines.append("")
    for p in included:
        lines.append(f"[import] {p['id']:40s} {p['display_name'][:40]:40s} "
                     f"refs={len(p['references']):3d} bytes={p['size']:6d}")
    for p in excluded:
        lines.append(f"[SKIP ] {p['id']:40s} reason={p['exclude_reason']}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
DEFAULT_SOURCE = Path("C:/Users/l1773/.workbuddy/skills")
DEFAULT_OUTPUT = (Path(__file__).resolve().parents[1]
                  / "openclaw_service/assistant/openclaw/skills")


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Guidance-only WorkBuddy importer "
                    "(dry-run audit; explicit --apply required to write).")
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument('--test-report', type=Path, help='Install reviewed copies; repair only two known metadata defects')
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true",
                        help="audit and report only (default when --apply absent)")
    mode.add_argument("--apply", action="store_true",
                        help="write workbuddy/ and workbuddy-registry.json")
    args = parser.parse_args(argv)

    try:
        plans = build_plan(args.source, test_report=args.test_report)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    if not args.apply:
        print(dry_run_text(plans, args.source.resolve()))
        total = sum(p["size"] for p in plans if not p["excluded"])
        print(f"\nplanned write: {len([p for p in plans if not p['excluded']])} "
              f"guidance skills, {total} text bytes -> "
              f"{args.output.resolve() / 'workbuddy'}")
        print("(nothing written; import is guidance-only)")
        return 0

    try:
        records = apply_plan(plans, args.output.resolve(), args.source.resolve())
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(f"applied {len(records)} guidance skills -> "
          f"{args.output.resolve() / 'workbuddy'}")
    print(f"registry -> {args.output.resolve() / 'workbuddy-registry.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
