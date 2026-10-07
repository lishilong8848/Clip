"""Read reviewed, packaged SKILL.md guides without granting filesystem access.

The distributed openclaw/skills bundle contains two registries:

* ``registry.json`` — the reviewed built-in guides (always required).
* ``workbuddy-registry.json`` — an OPTIONAL list written by the audited
  WorkBuddy importer.  When present it adds guidance-only ``workflow_guide``
  skills; when absent the built-in catalog alone is served.

Everything stays a workflow guide.  No executable, browser or cloud-write
permission is granted by these entries.  Two source/name roots
(``zhinav-point-data`` and ``告警描述核查``) are always excluded, including
any descendants, before a record is ever served.
"""
import json
import os
import posixpath
import re
from pathlib import Path
import zipfile

from .lighthouse_ai import AssistantError, safe_text

ROOT = Path(__file__).parent / 'openclaw/skills'

_NAME_RE = re.compile(r'[a-z][a-z0-9-]{0,60}')
_MAX_TOTAL = 64
_CHUNK = 16000
_MAX_RESOURCE = 512 * 1024
_MAX_REGISTRY = 1024 * 1024
_MAX_REFERENCES = 128
_MAX_WARNINGS = 20
_MAX_DESCRIPTION = 8000
_MAX_DISPLAY_NAME = 200
_MAX_WARNING_LEN = 1000
_MAX_SOURCE_TEST_STATUS = 80
_LONG_LINE = 2048
_OMITTED_MARKER = '过长单行内容未发送'
_FILE_ATTRIBUTE_REPARSE_POINT = 0x400

_EXCLUDED_PARTS = ('zhinav-point-data', '告警描述核查')
_BUILTIN = ('lighthouse-business', 'public-research', 'lighthouse-extension')
MODULE_GUIDES = {
    '飞书消息': 'lighthouse-people-signatures',
    '通告与事件': 'lighthouse-notices', '工作台': 'lighthouse-notices',
    '变更确认': 'lighthouse-notices', '事件': 'lighthouse-events',
    '维修单与跟进': 'lighthouse-repairs', '机柜上下电': 'lighthouse-cabinet-power',
    '演练': 'lighthouse-drills', '维护单': 'lighthouse-mops',
    '轮巡工单': 'lighthouse-sops', 'SOP工单查询': 'lighthouse-sops',
    '重保管理': 'lighthouse-critical-guard', '容量与水耗': 'lighthouse-water',
    '日常工作': 'lighthouse-daily-work', '计划收敛审查': 'lighthouse-plan-convergence',
    '画像学练': 'lighthouse-learning-query', '题库资料': 'lighthouse-learning-query',
    '人员与签名管理': 'lighthouse-people-signatures', '通告历史': 'lighthouse-history',
    '未完成工作': 'lighthouse-overview', '楼栋概览': 'lighthouse-overview',
    '记录': 'lighthouse-overview', '后端任务': 'lighthouse-overview',
    '门户引导': 'lighthouse-overview',
    '管理设置': 'lighthouse-business', '业务接口': 'lighthouse-business',
}
_NOTE = '技能说明不是业务数据，不构成执行或权限授权。'


# ---------------------------------------------------------------------------
# Reparse / symlink / junction guards
# ---------------------------------------------------------------------------
def _is_reparse(path: Path) -> bool:
    """True for symlinks, junctions and arbitrary Windows reparse points.

    Uses the OS-native ``st_file_attributes`` attribute when present and relies
    on ``is_symlink``/``is_junction`` otherwise.  A missing path (``FileNotFound``)
    is simply "not there", never an unsafe reparse, so a missing optional file
    is not mistaken for a corrupt entry.
    """
    try:
        if path.is_symlink() or path.is_junction():
            return True
        if os.name == 'nt':
            attrs = getattr(path.lstat(), 'st_file_attributes', 0)
            if attrs & _FILE_ATTRIBUTE_REPARSE_POINT:
                return True
    except FileNotFoundError:
        return False
    except OSError:
        return True
    return False


def _is_within(path: Path, root: Path) -> bool:
    return path == root or path.is_relative_to(root)


# ---------------------------------------------------------------------------
# Bounded reads
# ---------------------------------------------------------------------------
def _checked_read(path: Path, max_bytes: int):
    """Size-check before reading; never follow reparse targets."""
    if _is_reparse(path):
        return None, 'symbolic link / reparse point'
    try:
        size = path.stat().st_size
    except OSError:
        return None, 'unreadable'
    if size > max_bytes:
        return None, f'file exceeds {max_bytes} bytes'
    try:
        with path.open('rb') as fh:
            data = fh.read(max_bytes + 1)
    except OSError:
        return None, 'unreadable'
    if len(data) > max_bytes:
        return None, f'file exceeds {max_bytes} bytes'
    return data, None


def _safe_resource(logical: Path) -> bool:
    """A logical bundle path must be contained and free of reparse ancestors."""
    root = ROOT
    # Refuse a symlink/junction anywhere in the UNRESOLVED chain from ROOT down
    # to the requested leaf, so resolve() cannot be used to hide a reparse.
    chain = []
    cur = logical
    while cur != root and cur != cur.parent:
        chain.append(cur)
        cur = cur.parent
    chain.append(root)
    for p in reversed(chain):
        if _is_reparse(p):
            return False
    try:
        resolved = logical.resolve()
    except OSError:
        return False
    if not _is_within(resolved, root.resolve()):
        return False
    if not resolved.is_file():
        return False
    return True


def _read_bundle(relative):
    bundle = ROOT / 'workbuddy.zip'
    try:
        if not _safe_resource(bundle) or bundle.stat().st_size > 20 * 1024 * 1024:
            raise ValueError('invalid resource bundle')
        with zipfile.ZipFile(bundle) as archive:
            entries = archive.infolist()
            if len(entries) > 1024 or len({item.filename for item in entries}) != len(entries):
                raise ValueError('invalid resource index')
            item = archive.getinfo(relative)
            if (item.file_size > _MAX_RESOURCE or item.flag_bits & 1
                    or item.compress_type not in {zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED}
                    or item.external_attr >> 16 & 0o170000 == 0o120000):
                raise ValueError('invalid resource')
            with archive.open(item) as stream:
                data = stream.read(_MAX_RESOURCE + 1)
            if len(data) > _MAX_RESOURCE:
                raise ValueError('oversized resource')
            return data
    except (OSError, KeyError, ValueError, RuntimeError, zipfile.BadZipFile):
        raise AssistantError('技能资源包无效，未加载。', 503) from None


# ---------------------------------------------------------------------------
# Mandatory exclusions (by source or name, descendants included)
# ---------------------------------------------------------------------------
def _is_excluded(source, name) -> bool:
    src = str(source or '').replace('\\', '/')
    if any(part in _EXCLUDED_PARTS for part in src.split('/')):
        return True
    nm = str(name or '')
    dashed = f'-{nm}-'
    if any(f'-{token}-' in dashed for token in _EXCLUDED_PARTS):
        return True
    return False


# ---------------------------------------------------------------------------
# Registry parsing (fail closed)
# ---------------------------------------------------------------------------
def _parse_builtin(data):
    if not isinstance(data, list):
        raise AssistantError('技能目录无效，未加载。', 503)
    items = {}
    for rec in data:
        if not isinstance(rec, dict):
            raise AssistantError('技能目录无效，未加载。', 503)
        name = rec.get('name')
        description = rec.get('description')
        if not isinstance(name, str) or not _NAME_RE.fullmatch(name):
            raise AssistantError('技能目录无效，未加载。', 503)
        if not isinstance(description, str):
            raise AssistantError('技能目录无效，未加载。', 503)
        if len(description) > _MAX_DESCRIPTION:
            raise AssistantError('技能目录无效，未加载。', 503)
        if name in items:
            raise AssistantError('技能目录存在重复名称，未加载。', 503)
        references = rec.get('references', [])
        if not isinstance(references, list) or len(references) > _MAX_REFERENCES or not all(_canonical_under(path, name) for path in references):
            raise AssistantError('技能引用无效，未加载。', 503)
        items[name] = {'name': name, 'description': description,
                       'path': f'{name}/SKILL.md', 'references': list(references)}
    return items


def _canonical_under(path, base):
    """Canonical POSIX ``path`` strictly under ``base`` (both use '/' separators)."""
    if not isinstance(path, str):
        return False
    if '\\' in path or '..' in path or path.startswith('/'):
        return False
    if re.match(r'^[A-Za-z]:', path):
        return False
    if not path.startswith(base + '/'):
        return False
    if path != posixpath.normpath(path):
        return False
    parts = path.split('/')
    return bool(parts) and all(p not in ('', '.') for p in parts)


def _parse_workbuddy(data):
    if not isinstance(data, list):
        raise AssistantError('workbuddy-registry.json 结构无效，未加载。', 503)
    items = {}
    for rec in data:
        if not isinstance(rec, dict):
            raise AssistantError('workbuddy-registry.json 记录无效，未加载。', 503)
        kind = rec.get('kind')
        name = rec.get('name')
        if kind != 'workflow_guide':
            raise AssistantError('workbuddy-registry.json 仅允许 workflow_guide，未加载。', 503)
        if not isinstance(name, str) or not _NAME_RE.fullmatch(name):
            raise AssistantError('workbuddy-registry.json 名称无效，未加载。', 503)
        source = rec.get('source')
        if _is_excluded(source, name):
            continue  # mandatory source/name exclusion (incl. descendants)
        description = rec.get('description') or ''
        if not isinstance(description, str):
            raise AssistantError('workbuddy-registry.json 描述无效，未加载。', 503)
        if len(description) > _MAX_DESCRIPTION:
            raise AssistantError('workbuddy-registry.json 元数据无效，未加载。', 503)
        path = rec.get('path')
        if not _canonical_under(path, 'workbuddy') or path != f'workbuddy/{name}/SKILL.md':
            raise AssistantError('workbuddy-registry.json 路径无效，未加载。', 503)
        references = rec.get('references')
        if not isinstance(references, list) or len(references) > _MAX_REFERENCES:
            raise AssistantError('workbuddy-registry.json 引用无效，未加载。', 503)
        base = f'workbuddy/{name}'
        if not all(_canonical_under(r, base) for r in references):
            raise AssistantError('workbuddy-registry.json 引用无效，未加载。', 503)
        if name in items:
            raise AssistantError('workbuddy-registry.json 存在重复名称，未加载。', 503)
        item = {
            'name': name,
            'description': description,
            'path': path,
            'references': list(references),
        }
        display = rec.get('display_name')
        if display is not None:
            if not isinstance(display, str) or len(display) > _MAX_DISPLAY_NAME:
                raise AssistantError('workbuddy-registry.json 元数据无效，未加载。', 503)
            item['display_name'] = display
        status = rec.get('source_test_status')
        if status is not None:
            if not isinstance(status, str) or len(status) > _MAX_SOURCE_TEST_STATUS:
                raise AssistantError('workbuddy-registry.json 元数据无效，未加载。', 503)
            item['source_test_status'] = status
        warnings = rec.get('warnings')
        if warnings is not None:
            if (not isinstance(warnings, list) or len(warnings) > _MAX_WARNINGS
                    or not all(isinstance(w, str) and len(w) <= _MAX_WARNING_LEN
                               for w in warnings)):
                raise AssistantError('workbuddy-registry.json 元数据无效，未加载。', 503)
            item['warnings'] = list(warnings)
        items[name] = item
    return items


def _load_items():
    if _is_reparse(ROOT):
        raise AssistantError('技能目录无效，未加载。', 503)
    builtin_path = ROOT / 'registry.json'
    if _is_reparse(builtin_path):
        raise AssistantError('技能目录无效，未加载。', 503)
    builtin_bytes, _ = _checked_read(builtin_path, _MAX_REGISTRY)
    if builtin_bytes is None:
        raise AssistantError('技能目录无效，未加载。', 503)
    try:
        builtin_data = json.loads(builtin_bytes.decode('utf-8'))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AssistantError('技能目录无效，未加载。', 503) from exc
    items = _parse_builtin(builtin_data)

    wb_path = ROOT / 'workbuddy-registry.json'
    if os.path.lexists(wb_path):
        if _is_reparse(wb_path):
            raise AssistantError('workbuddy-registry.json 损坏，未加载。', 503)
        wb_bytes, _ = _checked_read(wb_path, _MAX_REGISTRY)
        if wb_bytes is None:
            raise AssistantError('workbuddy-registry.json 损坏，未加载。', 503)
        try:
            wb_data = json.loads(wb_bytes.decode('utf-8'))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise AssistantError('workbuddy-registry.json 损坏，未加载。', 503) from exc
        wb_items = _parse_workbuddy(wb_data)
        for name, item in wb_items.items():
            if name in items:
                raise AssistantError('技能目录存在重复名称，未加载。', 503)
            items[name] = item

    if len(items) > _MAX_TOTAL:
        raise AssistantError('技能目录数量超过上限，未加载。', 503)
    return items


# ---------------------------------------------------------------------------
# Shared metadata sanitization
# ---------------------------------------------------------------------------
def _sanitized_warnings(warnings, limit=500):
    if isinstance(warnings, list):
        return [safe_text(w, limit=limit) for w in warnings]
    return safe_text(warnings, limit=limit)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def annotate_discovery(found):
    """Attach guide names to real descriptors, without changing authority/schema."""
    available = catalog()
    names = {item['name'] for item in available}
    recommended = ['lighthouse-business'] if 'lighthouse-business' in names else []
    for kind in ('groups', 'items'):
        for item in found.get(kind, []):
            name = MODULE_GUIDES.get(item.get('group') or item.get('name'), 'lighthouse-business')
            if name in names:
                item['skill'] = name
                if kind == 'items' and name not in recommended:
                    recommended.append(name)
    found['skills'] = available
    found['recommended_skills'] = recommended
    return found


def catalog():
    items = _load_items()
    result = []
    for item in items.values():
        entry = {
            'name': item['name'],
            'description': safe_text(item['description'], limit=300),
            'execution_status': 'guide_only',
            'reference_count': len(item['references']),
        }
        display = item.get('display_name')
        if display is not None:
            entry['display_name'] = safe_text(display, limit=500)
        warnings = item.get('warnings')
        if warnings is not None:
            entry['warnings'] = _sanitized_warnings(warnings)
        status = item.get('source_test_status')
        if status is not None:
            entry['source_test_status'] = safe_text(status, limit=500)
        result.append(entry)
    return result


def read(name, reference='', offset=0):
    items = _load_items()
    if name not in items:
        raise AssistantError('技能不存在或未审核。', 404)
    item = items[name]
    if not isinstance(offset, int) or isinstance(offset, bool) or offset < 0:
        raise AssistantError('分页偏移无效。', 400)
    if reference == '':
        rel_path = item['path']
    else:
        if not isinstance(reference, str) or reference not in item['references']:
            raise AssistantError('引用不存在或未注册。', 404)
        rel_path = reference

    if rel_path.startswith('workbuddy/') and os.path.lexists(ROOT / 'workbuddy.zip'):
        data = _read_bundle(rel_path)
    else:
        logical = ROOT / rel_path
        if not _safe_resource(logical):
            raise AssistantError('技能内容无效，未加载。', 503)
        data, reason = _checked_read(logical, _MAX_RESOURCE)
        if data is None:
            raise AssistantError('技能内容无效，未加载。', 503)
    # Drop pathological single lines (e.g. a 40KB one-line guide) BEFORE safe_text
    # so the per-line CONTACT regex never sees a giant line.  Sensitive lines are
    # never split — the whole oversized line is replaced with an explicit marker.
    raw = data.decode('utf-8', errors='replace')
    lines = raw.split('\n')
    omitted_long_lines = 0
    for idx, line in enumerate(lines):
        if len(line) > _LONG_LINE:
            lines[idx] = _OMITTED_MARKER
            omitted_long_lines += 1
    # Scrub the FULL content first, then paginate.  Offsets refer to the
    # sanitized text so a secret/contact split across a raw chunk boundary is
    # removed whole instead of leaking across pages.
    text = safe_text('\n'.join(lines), limit=_MAX_RESOURCE)
    if offset >= len(text):
        content = ''
        next_offset = 0
    else:
        part = text[offset:offset + _CHUNK]
        content = part
        end = offset + len(part)
        next_offset = 0 if end >= len(text) else end

    result = {
        'name': name,
        'reference': rel_path,
        'content': content,
        'next_offset': next_offset,
        'omitted_long_lines': omitted_long_lines,
        'kind': 'workflow_guide',
        'note': _NOTE,
        'execution_status': 'guide_only',
    }
    display = item.get('display_name')
    if display is not None:
        result['display_name'] = safe_text(display, limit=500)
    warnings = item.get('warnings')
    if warnings is not None:
        result['warnings'] = _sanitized_warnings(warnings)
    refs = item.get('references') or []
    if refs:
        result['references'] = list(refs[:_MAX_REFERENCES])
    status = item.get('source_test_status')
    if status is not None:
        result['source_test_status'] = safe_text(status, limit=500)
    return result
