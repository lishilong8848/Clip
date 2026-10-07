"""Guidance-only shared SKILL.md storage, shared by every logged-in account.

Any authenticated account may install lightweight ``SKILL.md`` guides (single
markdown or a one-skill ZIP with markdown references) into the shared
``lighthouse_installed_skills`` namespace.  Skills are global and readable by all
authorized accounts; removal is reserved for the installing creator or an
administrator.  Storage is split so catalog/tool searches never parse full
skill bodies:

* one small global metadata index document ``installed`` holds per-skill public
  metadata (name, description, references, warnings, dedupe hash, creator hash,
  creator name, installed_at), and
* one per-skill content document ``installed:<shared-name>`` holds the readable
  markdown text for that skill only.

install/remove persist the updated index together with the affected content
document through an atomic ``store.put_documents`` batch, so a reader never
sees an index pointing at missing or stale content.  Skills are stored as text
only: scripts/assets are never extracted, loaded or executed, and every entry
is surfaced with an explicit warning that they are unavailable.

Threat model / invariants:

* Shared access: a single global ``installed`` index is used by every account.
  A missing identity is rejected.  Reads/catalog are allowed for any authorized
  actor, while remove is allowed only for the creator or an administrator.
* Creator preservation: exact-content dedupe across accounts reuses the
  existing skill and never transfers ownership; ``removable`` is computed for
  the current actor each time.
* Builtin prevention: every external name is prefixed with ``shared-`` so it can
  never collide with (or override) a packaged built-in skill.
* Owner privacy: full source owner ids and guide bodies are never placed in the
  public metadata surface (only a creator hash + safe creator name are kept).
* No filesystem / shell / network: nothing is extracted to disk and nothing is
  executed.  ZIP members are validated and decoded strictly in memory.
* Bounded: 100 skills shared globally, 10 MiB input zip, 2 MiB total
  uncompressed, 64 archive entries, 32 readable markdown files, 512 KiB per
  markdown chunk, 16000 char read pagination.
"""

from __future__ import annotations

import hashlib
import io
import posixpath
import re
import threading
import time
import zipfile

import yaml

from .lighthouse_ai import AssistantError, safe_text
from .lighthouse_skills import _is_excluded

NAMESPACE = "lighthouse_installed_skills"

GLOBAL_LIMIT = 100
MAX_ARCHIVE_BYTES = 10 * 1024 * 1024
MAX_UNCOMPRESSED = 2 * 1024 * 1024
MAX_ENTRIES = 64
MAX_MD_FILES = 32
MAX_RESOURCE = 512 * 1024
CHUNK = 16000
MAX_NAME = 64
MAX_DESCRIPTION = 1024
MAX_DISPLAY_NAME = 200
MAX_WARNINGS = 64
MAX_WARNING_LEN = 500
LONG_LINE = 2048
OMITTED_MARKER = "过长单行内容未发送"

_NAME_RE = re.compile(r"[a-z0-9][a-z0-9-]{0,63}")
_EXTERNAL_NAME_RE = re.compile(r"shared-[a-z0-9][a-z0-9-]{0,95}")
_GUIDE_FILE = "SKILL.md"
_MARKDOWN_SUFFIXES = (".md", ".markdown")

# Global metadata/content split keys in the ``lighthouse_installed_skills``
# namespace.  One global index plus one content document per shared skill.
INDEX_KEY = "installed"

_PERSONAL_NOTE = "共享技能说明不是业务数据，不构成执行或权限授权。"


class SharedSkills:
    """Guidance-only shared skill storage over a document store.

    Skills are globally readable by all authorized accounts.  Only the creating
    account (or an administrator) may remove a shared skill.  Actor identity is
    required by every public method.
    """

    def __init__(self, store):
        self.store = store
        # Serialize mutations with the store's own lock so every SharedSkills
        # facade on the same store shares one lock (AssistantStore ships an
        # RLock).  Minimal stores that don't expose one get a shared lock lazily.
        lock = getattr(store, "_lock", None)
        if lock is None:
            try:
                lock = store.__dict__.setdefault("_lock", threading.RLock())
            except (AttributeError, TypeError):
                raise AssistantError('技能存储不支持共享锁，未写入。', 503) from None
        self._lock = lock

    # ------------------------------------------------------------------ helpers

    @staticmethod
    def _empty_index():
        return {"version": 1, "installed": {}}

    @staticmethod
    def _empty_content():
        return {"version": 1, "removed": True, "contents": {}}

    @staticmethod
    def _identity(actor):
        if not isinstance(actor, dict):
            raise AssistantError("缺少登录身份，无法访问共享技能。", 403)
        for key in ("id", "user_id", "open_id"):
            value = actor.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        raise AssistantError("缺少登录身份，无法访问共享技能。", 403)

    @staticmethod
    def _identity_hash(identity):
        return hashlib.sha256(
            ("lighthouse_shared_skills:" + identity).encode("utf-8")
        ).hexdigest()

    @staticmethod
    def _actor_name(actor):
        for key in ("name", "user_name", "display_name", "nickname"):
            value = actor.get(key)
            if isinstance(value, str) and value.strip():
                return safe_text(value.strip(), limit=100)
        return ""

    def _content_key(self, name):
        return INDEX_KEY + ":" + name

    def _load_index_key(self):
        doc = self.store.get_document(NAMESPACE, INDEX_KEY)
        if doc is None:
            return self._empty_index()
        if not isinstance(doc, dict) or not isinstance(doc.get("installed"), dict):
            raise AssistantError("共享技能数据损坏，未加载。", 503)
        return doc

    def _load_index(self, actor):
        # Identity is required even for global indexes so we never serve shared
        # skills to an unauthenticated caller.
        self._identity(actor)
        return self._load_index_key()

    def _can_remove(self, actor, skill):
        identity = self._identity(actor)
        creator_hash = skill.get("creator_hash")
        if isinstance(creator_hash, str) and creator_hash == self._identity_hash(identity):
            return True
        return bool(actor.get("is_admin"))

    @staticmethod
    def _public_meta(skill, duplicate=False, removable=False):
        entry = {
            "name": skill["name"],
            "description": safe_text(skill.get("description", ""), limit=300),
            "execution_status": "guide_only",
            "reference_count": len(skill.get("references") or []),
            "source": "shared",
            "removable": bool(removable),
        }
        display = skill.get("display_name")
        if isinstance(display, str) and display:
            entry["display_name"] = safe_text(display, limit=MAX_DISPLAY_NAME)
        warnings = skill.get("warnings")
        if warnings:
            entry["warnings"] = [safe_text(w, limit=500) for w in warnings]
        if duplicate:
            entry["duplicate"] = True
        return entry

    # ------------------------------------------------------------------ public

    def catalog(self, actor):
        """Return public metadata for all shared skills (no full text).

        Only the small global ``installed`` index document is fetched;
        per-skill content documents are never parsed during a catalog/search.
        """
        with self._lock:
            index = self._load_index(actor)
            names = sorted(index["installed"].keys())
            return [
                self._public_meta(
                    index["installed"][name],
                    removable=self._can_remove(actor, index["installed"][name]),
                )
                for name in names
            ]

    def install(self, actor, filename: str, content: bytes) -> dict:
        if not isinstance(filename, str) or not filename.strip():
            raise AssistantError("技能文件名无效。", 400)
        if not isinstance(content, (bytes, bytearray)) or not content:
            raise AssistantError("技能内容无效。", 400)
        raw = bytes(content)
        if len(raw) > MAX_ARCHIVE_BYTES:
            raise AssistantError("技能文件超过大小上限。", 413)

        parsed = self._parse(raw, filename)
        external = self._external_name(parsed)
        self._reject_forbidden(parsed, external)
        original_sha = hashlib.sha256(raw).hexdigest()

        with self._lock:
            identity = self._identity(actor)
            index = self._load_index_key()
            skills = index["installed"]

            # Exact-content dedupe across accounts: the existing skill is reused
            # and ownership is never transferred.
            for skill in skills.values():
                if skill.get("original_sha256") == original_sha:
                    return {
                        **self._public_meta(
                            skill,
                            duplicate=True,
                            removable=self._can_remove(actor, skill),
                        ),
                        "duplicate": True,
                    }
            if external in skills:
                raise AssistantError("存在同名但内容不同的技能，请修改源名称后重试。", 409)
            if len(skills) >= GLOBAL_LIMIT:
                raise AssistantError("共享技能数量已达上限（%d 个）。" % GLOBAL_LIMIT, 409)

            skill = {
                "name": external,
                "display_name": parsed["display_name"],
                "description": parsed["description"],
                "source_identity": parsed["source_identity"],
                "source_shortid": parsed["source_shortid"],
                "original_sha256": original_sha,
                "references": list(parsed["references"]),
                "warnings": list(parsed["warnings"]),
                "creator_hash": self._identity_hash(identity),
                "creator_name": self._actor_name(actor),
                "installed_at": time.time(),
            }
            skills[external] = skill
            content_key = self._content_key(external)
            content_doc = {"version": 1, "contents": parsed["contents"]}
            # Publish the updated index and the new content document atomically.
            self.store.put_documents(
                NAMESPACE,
                {INDEX_KEY: index, content_key: content_doc},
            )
            # Return the metadata saved under the lock; never read the index back
            # (a concurrent remove could otherwise make this look-up disappear).
            return {
                **self._public_meta(skill, removable=True),
                "duplicate": False,
            }

    def read(self, actor, name, reference="", offset=0):
        if not isinstance(actor, dict):
            raise AssistantError("缺少登录身份，无法访问共享技能。", 403)
        if not isinstance(name, str) or not name:
            raise AssistantError("技能名称无效。", 400)
        if not isinstance(offset, int) or isinstance(offset, bool) or offset < 0:
            raise AssistantError("分页偏移无效。", 400)
        with self._lock:
            self._identity(actor)
            index = self._load_index_key()
            skills = index["installed"]
            if name not in skills:
                raise AssistantError("技能不存在或未安装。", 404)
            skill = skills[name]
            if reference == "":
                rel = _GUIDE_FILE
            else:
                if not isinstance(reference, str) or reference not in skill.get("references", []):
                    raise AssistantError("引用不存在或未注册。", 404)
                rel = reference
            # Load only the selected skill's content document under the lock.
            content_doc = self.store.get_document(
                NAMESPACE, self._content_key(name)
            )
            if not isinstance(content_doc, dict) \
                    or not isinstance(content_doc.get("contents"), dict):
                raise AssistantError("技能内容无效，未加载。", 503)
            text = content_doc["contents"].get(rel)
            if not isinstance(text, str):
                raise AssistantError("技能内容无效，未加载。", 503)
            content, next_offset, omitted = self._paginate(text, offset)

            result = {
                "name": name,
                "reference": rel,
                "offset": offset,
                "content": content,
                "next_offset": next_offset,
                "references": list(skill.get("references") or []),
                "guide_only": True,
                "source": "shared",
                "removable": self._can_remove(actor, skill),
                "note": _PERSONAL_NOTE,
                "omitted_long_lines": omitted,
            }
            display = skill.get("display_name")
            if isinstance(display, str) and display:
                result["display_name"] = safe_text(display, limit=MAX_DISPLAY_NAME)
            warnings = skill.get("warnings")
            if warnings:
                result["warnings"] = [safe_text(w, limit=500) for w in warnings]
            return result

    def remove(self, actor, name):
        if not isinstance(name, str) or not name or not _EXTERNAL_NAME_RE.fullmatch(name):
            raise AssistantError("技能名称无效。", 400)
        with self._lock:
            identity = self._identity(actor)
            index = self._load_index_key()
            skills = index["installed"]
            if name not in skills:
                raise AssistantError("技能不存在或未安装。", 404)
            skill = skills[name]
            if not self._can_remove(actor, skill):
                raise AssistantError("无权移除该共享技能。", 403)
            del skills[name]
            # Atomically persist the pruned index and replace the content
            # document with an empty tombstone so its stored bytes are cleared
            # and the skill becomes inaccessible.
            content_key = self._content_key(name)
            self.store.put_documents(
                NAMESPACE,
                {INDEX_KEY: index, content_key: self._empty_content()},
            )
            return {"removed": True, "name": name}

    # ------------------------------------------------------------------ parsing

    @staticmethod
    def _external_name(parsed):
        return "%s-%s-%s" % ("shared", parsed["slug"], parsed["source_shortid"])

    @staticmethod
    def _short_identity(source_identity):
        return hashlib.sha256(source_identity.encode("utf-8")).hexdigest()[:8]

    @staticmethod
    def _reject_forbidden(parsed, external):
        """Reject skills matching a forbidden source or name (descendants included).

        Mirrors the built-in lighthouse_skills._is_excluded rule for
        ``zhinav-point-data`` and ``告警描述核查`` so a shared installation can
        never re-introduce an excluded skill under any descendant path or label.
        """
        source = parsed.get("source_identity", "")
        for candidate in (external, parsed.get("name"), parsed.get("slug"), parsed.get("display_name")):
            if candidate and _is_excluded(source, candidate):
                raise AssistantError("该技能来源/名称被禁止安装。", 400)

    def _parse(self, raw, filename):
        lower = filename.lower()
        if lower.endswith(".zip"):
            return self._parse_zip(raw, filename)
        if lower.endswith(_MARKDOWN_SUFFIXES):
            return self._parse_single_markdown(raw, filename)
        raise AssistantError("仅支持 .md 或 .zip 技能文件。", 400)

    # ------------------------------------------------------------------ single md

    def _parse_single_markdown(self, raw, filename):
        if len(raw) > MAX_RESOURCE:
            raise AssistantError("技能内容超过大小上限。", 413)
        text = self._decode_utf8(raw, "SKILL.md")
        frontmatter, _body = self._split_frontmatter(text)
        name, display_name, description = self._frontmatter_fields(frontmatter)
        source_identity = posixpath.basename(filename)[:120] or "markdown"
        slug = self._slug(name)
        return {
            "name": name,
            "display_name": display_name,
            "description": description,
            "slug": slug,
            "source_identity": source_identity,
            "source_shortid": self._short_identity(source_identity),
            "references": [],
            "warnings": [],
            "contents": {_GUIDE_FILE: text},
        }

    # ------------------------------------------------------------------ zip

    def _parse_zip(self, raw, filename):
        if len(raw) > MAX_ARCHIVE_BYTES:
            raise AssistantError("技能包超过大小上限。", 413)
        try:
            archive = zipfile.ZipFile(io.BytesIO(raw))
        except (zipfile.BadZipFile, OSError, ValueError) as exc:
            raise AssistantError("技能包无效，未加载。", 400) from exc

        with archive:
            entries = archive.infolist()
            if len(entries) > MAX_ENTRIES:
                raise AssistantError("技能包条目过多。", 413)
            if len({item.filename for item in entries}) != len(entries):
                raise AssistantError("技能包存在重复路径。", 400)

            normalized = []  # (info, kind, rel_name)
            seen_lower = set()
            total_size = 0
            for info in entries:
                ok, reason = self._valid_zip_path(info.orig_filename)
                if not ok:
                    raise AssistantError("技能包路径无效（%s）。" % reason, 400)
                if info.compress_type not in {zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED}:
                    raise AssistantError('技能包仅支持普通ZIP存储或Deflate压缩。', 400)
                lower_key = posixpath.normpath(info.filename).lower()
                if lower_key in seen_lower:
                    raise AssistantError("技能包路径存在大小写冲突。", 400)
                seen_lower.add(lower_key)
                total_size += int(info.file_size or 0)
                if total_size > MAX_UNCOMPRESSED:
                    raise AssistantError("技能包解压后超过大小上限。", 413)
                if info.flag_bits & 1:
                    raise AssistantError("技能包内存在加密条目。", 400)
                kind, error = self._entry_kind(info)
                if error:
                    raise AssistantError(error, 400)
                if kind == "file":
                    if info.file_size > MAX_RESOURCE:
                        raise AssistantError("技能包内单文件超过大小上限。", 413)
                normalized.append((info, kind, info.filename))

            base = self._resolve_base(normalized)
            source_identity = base if base else (posixpath.basename(filename)[:120] or "zip")

            guide_text = None
            references = []
            warnings = []
            md_count = 0
            contents = {}
            for info, kind, name in normalized:
                if kind == "dir":
                    continue
                if kind != "file":
                    raise AssistantError("技能包内存在非常规文件。", 400)
                if not self._within_base(name, base):
                    raise AssistantError("技能包内文件未位于统一根目录。", 400)
                rel = self._rel_within_base(name, base)
                is_markdown = rel.lower().endswith(_MARKDOWN_SUFFIXES)
                if is_markdown:
                    md_count += 1
                    if md_count > MAX_MD_FILES:
                        raise AssistantError("技能包内 Markdown 文件过多。", 413)
                if rel == _GUIDE_FILE:
                    if guide_text is not None:
                        raise AssistantError("技能包内存在多个 SKILL.md。", 400)
                    guide_text = self._read_member(archive, info)
                    continue
                if is_markdown:
                    if rel not in references:
                        references.append(rel)
                    contents[rel] = self._read_member(archive, info)
                else:
                    # Scripts/assets/support files are validated above (path,
                    # size, encryption) but never extracted or executed.
                    if len(warnings) < MAX_WARNINGS:
                        warnings.append(
                            "资源文件 %s 未加载，脚本/资源不可执行也不可用。" % rel
                        )

            if guide_text is None:
                raise AssistantError("技能包内缺少 SKILL.md。", 400)
            references.sort()
            contents[_GUIDE_FILE] = guide_text
            frontmatter, _body = self._split_frontmatter(guide_text)
            name, display_name, description = self._frontmatter_fields(frontmatter)
            return {
                "name": name,
                "display_name": display_name,
                "description": description,
                "slug": self._slug(name),
                "source_identity": source_identity,
                "source_shortid": self._short_identity(source_identity),
                "references": references,
                "warnings": warnings,
                "contents": contents,
            }

    @staticmethod
    def _read_member(archive, info):
        try:
            with archive.open(info) as stream:
                data = stream.read(MAX_RESOURCE + 1)
        except (OSError, RuntimeError, zipfile.BadZipFile, KeyError) as exc:
            raise AssistantError("技能包内文件读取失败。", 400) from exc
        if len(data) > MAX_RESOURCE:
            raise AssistantError("技能包内单文件超过大小上限。", 413)
        return SharedSkills._decode_utf8(data, info.filename)

    @staticmethod
    def _decode_utf8(data, label):
        try:
            text = bytes(data).decode("utf-8")
        except UnicodeDecodeError as exc:
            raise AssistantError("技能文件 %s 不是有效 UTF-8。" % label, 400) from exc
        # Normalise Windows line endings and strip a leading UTF-8 BOM so
        # frontmatter/body handling is consistent across MD and ZIP uploads.
        if text.startswith("\ufeff"):
            text = text[1:]
        text = text.replace("\r\n", "\n").replace("\r", "\n")
        return text

    @staticmethod
    def _valid_zip_path(name):
        if not name or "\x00" in name:
            return False, "空路径或包含 NUL"
        if len(name) > 240 or any(ord(char) < 32 or ord(char) == 127 for char in name):
            return False, '路径过长或含控制字符'
        if "\\" in name:
            return False, "包含反斜杠"
        if name.startswith("/") or re.match(r"^[A-Za-z]:", name):
            return False, "绝对路径"
        if name.startswith("//") or name.startswith("\\\\"):
            return False, "UNC 路径"
        clean = name.rstrip("/")
        if clean != posixpath.normpath(clean):
            return False, "未归一化路径"
        if clean.endswith("/"):
            return False, "路径无效"
        if any(part in ("", ".", "..") for part in clean.split("/")):
            return False, "路径穿越"
        return True, ""

    @staticmethod
    def _entry_kind(info):
        name = info.filename
        mode = (info.external_attr >> 16) & 0o170000
        # Check symlink / unsupported modes BEFORE the trailing-slash directory
        # convention, so a symbolic entry named like a directory cannot bypass
        # the symlink/variant-mode rejection.
        if mode == 0o120000:
            return None, "技能包内存在符号链接。"
        if mode not in (0, 0o040000, 0o100000):
            return None, "技能包内存在非常规文件。"
        if name.endswith("/") or mode == 0o040000:
            return "dir", None
        return "file", None

    @staticmethod
    def _resolve_base(normalized):
        """Return the skill-root prefix ('' = archive root) for a one-skill ZIP.

        SKILL.md defines the base: it must exist exactly once.  It may live at
        the archive root (with markdown references wherever they are inside the
        archive) or under a single top-level directory — in that case every file
        must live under that directory so a mixed/ambiguous archive is rejected.
        """
        guides = [
            name for _info, kind, name in normalized
            if kind == "file" and posixpath.basename(name) == _GUIDE_FILE
        ]
        if not guides:
            raise AssistantError("技能包内缺少 SKILL.md。", 400)
        if len(guides) > 1:
            raise AssistantError("技能包内存在多个 SKILL.md。", 400)
        base = posixpath.dirname(guides[0])
        if base in ("", "."):
            base = ""
        if base and "/" in base:
            raise AssistantError("技能包根目录应保持单层。", 400)
        if base:
            for _info, kind, name in normalized:
                if kind == "file" and not name.startswith(base + "/"):
                    raise AssistantError("技能包根目录不唯一。", 400)
        return base

    @staticmethod
    def _within_base(name, base):
        if not base:
            return True
        return name.startswith(base + "/")

    @staticmethod
    def _rel_within_base(name, base):
        if base:
            return name[len(base) + 1:]
        return name

    # ------------------------------------------------------------------ frontmatter

    @staticmethod
    def _split_frontmatter(text):
        # Defensive: callers already strip the BOM during decoding, but tolerate
        # a leading BOM here so a stray one can never break parsing.
        if text.startswith("\ufeff"):
            text = text[1:]
        if not text.startswith("---\n"):
            raise AssistantError("SKILL.md 缺少 YAML frontmatter。", 400)
        lines = text.split("\n")
        end = None
        for idx in range(1, len(lines)):
            if lines[idx].strip() == "---":
                end = idx
                break
        if end is None:
            raise AssistantError("SKILL.md YAML frontmatter 未闭合。", 400)
        fm = "\n".join(lines[1:end])
        body = "\n".join(lines[end + 1:])
        try:
            data = yaml.safe_load(fm)
        except yaml.YAMLError as exc:
            raise AssistantError("SKILL.md YAML 无效。", 400) from exc
        if data is None:
            data = {}
        if not isinstance(data, dict):
            raise AssistantError("SKILL.md YAML 必须是映射。", 400)
        return data, body

    @staticmethod
    def _frontmatter_fields(data):
        name = data.get("name")
        description = data.get("description")
        if not isinstance(name, str) or not _NAME_RE.fullmatch(name):
            raise AssistantError("SKILL.md 名称无效。", 400)
        if not isinstance(description, str) or not description.strip():
            raise AssistantError("SKILL.md 描述无效。", 400)
        description = description.strip()
        if len(description) > MAX_DESCRIPTION:
            raise AssistantError("SKILL.md 描述过长。", 400)
        display_name = name
        raw_display = data.get("display_name")
        if raw_display is not None:
            if not isinstance(raw_display, str) or not raw_display.strip():
                raise AssistantError("SKILL.md display_name 无效。", 400)
            display_name = raw_display.strip()
            if len(display_name) > MAX_DISPLAY_NAME:
                raise AssistantError("SKILL.md display_name 过长。", 400)
        return name, display_name, description

    @staticmethod
    def _slug(name):
        return re.sub(r"[^a-z0-9-]+", "-", name.lower()).strip("-") or "skill"

    # ------------------------------------------------------------------ paging

    @staticmethod
    def _paginate(text, offset):
        lines = text.split("\n")
        omitted = 0
        for idx, line in enumerate(lines):
            if len(line) > LONG_LINE:
                lines[idx] = OMITTED_MARKER
                omitted += 1
        sanitized = safe_text("\n".join(lines), limit=MAX_RESOURCE)
        if offset >= len(sanitized):
            return "", 0, omitted
        part = sanitized[offset:offset + CHUNK]
        end = offset + len(part)
        next_offset = 0 if end >= len(sanitized) else end
        return part, next_offset, omitted