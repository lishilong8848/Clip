"""Feishu assistant text editing for native notice drafts and SOP/person selection.

This module gives the Deterministic Feishu plan-edit commands a safe, flattened
view of two object fields that historically fell back to the web page:

* ``step0.patch`` (``native_notice``) — a native non-event notice draft.  The
  field's ``children`` are authoritative: only safe leaf controls are editable
  by text, hidden/read-only/protected children are never accepted, and the
  PATCH payload we build is a plain parent object the existing Agent
  ``amend()`` merges via ``_editable_patch`` so unedited/hidden values survive.

* ``step0.notice_sop`` (``native_notice_sop``) — use/no-work-order, scope, SOP,
  operator, reviewer and equipment/runs selection.  Every selection is resolved
  against options the existing GET options endpoint already returned; we never
  invent IDs or grant exemptions; the existing Agent ``selection_payload`` still
  enforces operator/reviewer and normal/cooling/polling rules at PATCH time.

Nothing here sends Feishu messages, writes real work orders or bypasses the
existing screenshot/work-order guards.  Work-order execution remains outside
the assistant.
"""
from __future__ import annotations

import datetime as _dt
import math
import re

from openclaw_service.assistant.lighthouse_ai import AssistantError, safe_text

# ---------------------------------------------------------------------------
# Marker / safety helpers
# ---------------------------------------------------------------------------


def is_native_notice(field):
    return bool(field and field.get("native_notice"))


def is_native_notice_sop(field):
    return bool(field and field.get("native_notice_sop"))


def child_protected(child):
    return bool(child and (child.get("read_only") or child.get("readonly") or child.get("disabled")
                           or child.get("hidden") or child.get("只读")))


def field_protected(field):
    """Parent/whole-field protection mirrors the assistant's ``_field_protected``.

    A native notice / native SOP field whose own ``read_only`` / ``readonly`` /
    ``disabled`` / ``hidden`` / ``只读`` marker is set must never be edited via
    leaf lines, even when an individual leaf looks writable.
    """
    return bool(field and (field.get("read_only") or field.get("readonly") or field.get("disabled")
                           or field.get("hidden") or field.get("只读")))


SUPPORTED_CHILD_TYPES = {
    "text", "textarea", "number", "integer", "float", "date", "datetime-local",
    "time", "month", "checkbox", "boolean", "select", "radio", "multiselect",
}


def child_supported(child):
    return bool(child and child.get("type") in SUPPORTED_CHILD_TYPES)


def notice_parent_value(field):
    """Plain ``{path: value}`` parent dict for a native notice/SOP field.

    ``public_plan`` strips underscore internals but keeps the filled ``value``
    (which for object fields is a ``$query`` binding plus leaf values).  We drop
    ``$``-prefixed protocol keys and return the editable leaf values only.
    """
    value = field.get("value")
    if not isinstance(value, dict):
        return {}
    return {key: item for key, item in value.items() if not str(key).startswith("$")}


# ---------------------------------------------------------------------------
# Native notice (step0.patch) leaf model
# ---------------------------------------------------------------------------


def _qualified_names(base, key, label):
    names = []
    if base and key:
        names.append(base + "." + key)
    if base and label and str(label).strip() and str(label) != key:
        names.append(base + "." + str(label).strip())
    return names


def _child_matches(field, raw):
    """Match ``raw`` to exactly one native_notice child.

    Supports the qualified ``field.name.path`` / ``field.name.label`` form and a
    bare path/label if it is unique within the plan's native notice leaves.
    Ambiguity is rejected rather than silently picking the first match.
    """
    names = []
    for child in field.get("children") or []:
        key = str(child.get("path") or "")
        label = str(child.get("label") or "")
        names.append((child, _qualified_names(field.get("name", ""), key, label)))

    direct = [(child, set(candidates)) for child, candidates in names]
    exact = [child for child, candidates in direct if raw in candidates]
    if len(exact) == 1:
        return exact[0]
    if len(exact) > 1:
        raise AssistantError("字段“" + safe_text(raw, limit=80) + "”对应多个通告填写项，请使用完整路径填写。")
    for child, candidates in direct:
        key = str(child.get("path") or "")
        label = str(child.get("label") or "")
        if raw == key or (label and raw == label):
            if any(other is not child and (raw == str(other.get("path") or "") or (label and raw == str(other.get("label") or "")))
                   for other, _ in direct):
                raise AssistantError("填写项“" + safe_text(raw, limit=80) + "”在多个字段中出现，请加字段前缀，例如“"
                                     + next(iter(candidates)) + "”。")
            return child
    return None


# ---------------------------------------------------------------------------
# Native notice SOP (step0.notice_sop) leaf model
# ---------------------------------------------------------------------------

_SOP_LEAVES = (
    ("exempt", ("不使用工单",)),
    ("scope", ("楼栋",)),
    ("sop_id", ("SOP",)),
    ("operator_record_id", ("操作人",)),
    ("reviewer_record_id", ("审核人",)),
    ("runs", ("运行", "设备", "轮巡")),
)


def _sop_leaf_matches(field, raw):
    base = str(field.get("name") or "")
    direct = []
    for key, labels in _SOP_LEAVES:
        candidate_names = [base + "." + key] + [base + "." + label for label in labels]
        direct.append((key, candidate_names, labels))
    exact = [key for key, candidates, labels in direct if raw in candidates]
    if len(exact) == 1:
        return exact[0]
    if len(exact) > 1:
        raise AssistantError("字段“" + safe_text(raw, limit=80) + "”对应多个 SOP 填写项，请使用完整路径填写。")
    for key, candidates, labels in direct:
        if raw == key or raw in labels:
            if len([1 for other_key, _, other_labels in direct if other_key != key and (raw == other_key or raw in other_labels)]):
                raise AssistantError("填写项“" + safe_text(raw, limit=80) + "”存在歧义，请用“" + base + "."
                                     + key + "”完整填写。")
            return key
    return None


def resolve_notice_leaf(fields, raw):
    """Resolve ``raw`` to ``(parent_field, child)`` for either native form.

    Returns ``None`` when the name does not belong to a native notice/SOP leaf.
    A name that matches leaves in more than one field is ambiguous and must be
    qualified with the field prefix instead of silently picking the first one.
    """
    raw = (raw or "").strip()
    if not raw:
        return None
    matches = []
    for field in fields or []:
        if is_native_notice(field):
            child = _child_matches(field, raw)
            if child is not None:
                matches.append((field, child))
        elif is_native_notice_sop(field):
            key = _sop_leaf_matches(field, raw)
            if key is not None:
                matches.append((field, {"key": key}))
    if not matches:
        return None
    if len(matches) == 1:
        return matches[0]
    candidates = "、".join(str(field.get("name") or "字段") for field, _ in matches)
    raise AssistantError("填写项“" + safe_text(raw, limit=80) + "”在多个字段（" + safe_text(candidates, limit=200)
                         + "）中都存在，请用“字段名.填写项”完整限定，避免串改。")


# ---------------------------------------------------------------------------
# Typed leaf-value coercion (mirrors the existing plan text-field rules)
# ---------------------------------------------------------------------------


def _options_value_label(options):
    return [(str(option.get("value") if isinstance(option, dict) else option),
             str(option.get("label") if isinstance(option, dict) else "")) for option in options or []]


def typed_child_value(child, raw):
    """Coerce ``raw`` to the typed value of a native notice child/SOP leaf.

    ``child`` may be a native_notice child dict or a ``{"key": ...}`` SOP leaf
    descriptor; options come from the child for native notice and are supplied
    separately for SOP leaves by the caller.
    """
    ctype = child.get("type")
    if ctype is None and isinstance(child.get("key"), str):
        ctype = {"exempt": "boolean", "scope": "text", "sop_id": "select",
                 "operator_record_id": "select", "reviewer_record_id": "select",
                 "runs": "runs"}.get(child["key"])
    options = child.get("options") or []
    label = str(child.get("label") or child.get("path") or child.get("key") or "字段")
    if ctype in ("text", "textarea"):
        return raw
    if ctype == "number":
        try:
            value = float(raw)
        except (ValueError, TypeError):
            raise AssistantError("请填写数字：" + label + "。") from None
        if not math.isfinite(value):
            raise AssistantError("数字无效：" + label + "。")
        return value
    if ctype in ("integer", "float"):
        try:
            value = int(raw) if ctype == "integer" else float(raw)
        except (ValueError, TypeError):
            raise AssistantError(("请填写整数：" if ctype == "integer" else "请填写数字：") + label + "。") from None
        if ctype == "float" and not math.isfinite(value):
            raise AssistantError("数字无效：" + label + "。")
        return value
    if ctype == "date":
        try:
            _dt.date.fromisoformat(raw)
        except (ValueError, TypeError):
            raise AssistantError("请填写日期，格式 YYYY-MM-DD：" + label + "。") from None
        return raw
    if ctype == "datetime-local":
        try:
            _dt.datetime.fromisoformat(raw.replace(" ", "T"))
        except (ValueError, TypeError):
            raise AssistantError("请填写日期时间，格式如 2026-10-09T10:30：" + label + "。") from None
        return raw.replace(" ", "T")
    if ctype == "time":
        try:
            _dt.time.fromisoformat(raw)
        except (ValueError, TypeError):
            raise AssistantError("请填写时间，格式 HH:MM:SS：" + label + "。") from None
        return raw
    if ctype == "month":
        try:
            _dt.date.fromisoformat(raw + "-01")
        except (ValueError, TypeError):
            raise AssistantError("请填写月份，格式 YYYY-MM：" + label + "。") from None
        return raw
    if ctype in ("checkbox", "boolean"):
        low = raw.strip().lower()
        if low in {"true", "1", "是", "勾选", "checked", "对", "使用"}:
            return True
        if low in {"false", "0", "否", "取消", "unchecked", "不对", "不使用"}:
            return False
        raise AssistantError("勾选项请填写 是/否：" + label + "。")
    if ctype in ("select", "radio"):
        return _resolve_choice(options, raw, label, multiselect=False)
    if ctype == "multiselect":
        parts = [part.strip() for part in re.split(r"[,，、]+", raw) if part.strip()]
        return [_resolve_choice(options, part, label, multiselect=True) for part in parts]
    raise AssistantError("该字段类型不支持飞书文本填写：" + label + "。请在灯塔原页面填写。")


def _resolve_choice(options, raw, label, *, multiselect):
    pairs = _options_value_label(options)
    exact = [value for value, _ in pairs if value == raw]
    if len(exact) == 1:
        return exact[0]
    if len(exact) > 1:
        raise AssistantError("候选选项值重复，无法确定“" + safe_text(raw, limit=80) + "”对应哪一项。")
    label_matches = [value for value, l in pairs if l == raw]
    if len(label_matches) == 1:
        return label_matches[0]
    if len(label_matches) > 1:
        raise AssistantError("候选中存在多个名为“" + safe_text(raw, limit=80) + "”的选项，请填写唯一的选项值。")
    raise AssistantError("“" + safe_text(raw, limit=80) + "”不在候选选项中" + ("（多选请用“、”分隔）" if multiselect else "") + "：" + label + "。")


# ---------------------------------------------------------------------------
# Native notice display
# ---------------------------------------------------------------------------


def _child_label(child):
    return str(child.get("label") or child.get("path") or "字段")


def _leaf_display(child, value):
    value = value if value not in (None, "") else None
    if value is None:
        return "（空）"
    ctype = child.get("type")
    options = child.get("options") or []
    if ctype in ("select", "radio"):
        labels = {str(option.get("value")): str(option.get("label") or "") for option in options}
        return labels.get(str(value)) or safe_text(str(value), limit=120)
    if ctype == "multiselect":
        labels = {str(option.get("value")): str(option.get("label") or "") for option in options}
        return "、".join(labels.get(str(item), safe_text(str(item), limit=80)) for item in (value if isinstance(value, list) else [value])) or "（空）"
    if ctype in ("checkbox", "boolean"):
        return "是" if value else "否"
    return safe_text(str(value), limit=120)


def native_notice_display(field, prefix="  "):
    """Render flattened safe leaf fields of a native_notice draft."""
    parent = notice_parent_value(field)
    lines, editable = [], False
    if field_protected(field):
        lines.append(f"{prefix}{str(field.get('name') or '')}（该字段为只读/隐藏，不可通过飞书文本修改，"
                     f"请在灯塔原业务页面核对。）")
        return lines, False
    for child in field.get("children") or []:
        if child.get("hidden"):
            continue
        qualified = str(field.get("name") or "") + "." + (str(child.get("label") or child.get("path") or ""))
        label = _child_label(child)
        value = _leaf_display(child, parent.get(child.get("path")))
        if child_protected(child):
            lines.append(f"{prefix}{qualified}（{label}）：{value}（只读，不可修改）")
            continue
        if not child_supported(child):
            lines.append(f"{prefix}{qualified}（{label}）：{value}（复杂内容，请在灯塔原页面填写）")
            continue
        editable = True
        line = f"{prefix}{qualified}（{label}）：{value}"
        if child.get("type") in {"select", "multiselect", "radio"} and (child.get("options") or []):
            line += "  → 查看选项 " + qualified
        elif child.get("type") == "datetime-local":
            line += "（格式 2026-10-09T10:30）"
        elif child.get("type") == "date":
            line += "（格式 YYYY-MM-DD）"
        elif child.get("type") in {"checkbox", "boolean"}:
            line += "（是/否）"
        elif child.get("type") == "multiselect":
            line += "（多选用“、”分隔）"
        lines.append(line)
    if not lines:
        lines.append(f"{prefix}（无可用填写字段）")
    return lines, editable


# ---------------------------------------------------------------------------
# Native notice SOP display
# ---------------------------------------------------------------------------


def _people_by_id(field):
    return {str(person.get("record_id")): person for person in field.get("people") or []}


def _sops_by_id(field):
    return {str(sop.get("sop_id")): sop for sop in field.get("sops") or []}


def scope_code_label(code):
    """Map a bare scope code to its canonical Chinese display label.

    The real scope catalogue uses ``110站`` for the station and ``<code>楼`` for
    building codes (e.g. ``E`` → ``E楼``).
    """
    code = str(code)
    return "110站" if code == "110" else code + "楼"


def _scope_candidates(field):
    """Return ``(values, labels_by_label)`` for a SOP field's candidate scopes.

    ``labels_by_label`` maps every candidate's Chinese label back to its code so
    the assistant can accept ``E楼`` / ``110站`` while staying strictly within
    the candidate (permission) set.  Dict-style fixtures keep their own labels.
    """
    values, labels = [], {}
    for item in field.get("scopes") or []:
        if isinstance(item, dict):
            value = str(item.get("value") or "")
            if not value:
                continue
            values.append(value)
            label = str(item.get("label") or "")
            if label:
                labels[label] = value
        elif item is not None:
            value = str(item)
            values.append(value)
            labels[scope_code_label(value)] = value
    return values, labels


def scope_label(scopes, value):
    """Return the display label for a scope code.

    Real ``native_notice_sop`` fields expose ``scopes`` as a plain list of
    strings (e.g. ``["E"]``); their Chinese labels are derived from the code
    (``E`` → ``E楼``, ``110`` → ``110站``).  Dict-scope fixtures are also handled
    for safety.
    """
    value = str(value)
    for item in scopes or []:
        if isinstance(item, dict):
            if str(item.get("value")) == value:
                return str(item.get("label") or value)
        elif str(item) == value:
            return scope_code_label(value)
    return value


def scope_display_names(scopes):
    """Return human-readable names for the available scopes."""
    names = []
    for item in scopes or []:
        if isinstance(item, dict):
            label = str(item.get("label") or item.get("value") or "")
        elif item is not None:
            label = scope_code_label(str(item))
        else:
            label = ""
        if label:
            names.append(label)
    return names


def notice_sop_display(field, prefix="  ", *, status=None):
    """Render use/no-work-order, scope, SOP, people and runs for a SOP field."""
    parent = notice_parent_value(field)
    lines = []
    if field_protected(field):
        display_label = scope_label(field.get("scopes") or [], str(parent.get("scope") or ""))
        lines.append(f"{prefix}{str(field.get('name') or '')}（SOP/人员选择）：（该字段为只读/隐藏，"
                     f"当前选择：{str(parent.get('sop_id') or '（空）')}，楼栋：{display_label}）新建/修改请在灯塔原业务页面操作。")
        return lines
    exempt = bool(parent.get("exempt"))
    lines.append(f"{prefix}{str(field.get('name') or '')}.不使用工单：{'是' if exempt else '否'}（是=本次不使用工单）")

    scope = str(parent.get("scope") or "")
    if scope:
        display_label = scope_label(field.get("scopes") or [], scope)
        lines.append(f"{prefix}{str(field.get('name') or '')}.楼栋：{scope}（{display_label}）")
    else:
        lines.append(f"{prefix}{str(field.get('name') or '')}.楼栋：（空）")

    sop_id = str(parent.get("sop_id") or "")
    sop = _sops_by_id(field).get(sop_id)
    lines.append(f"{prefix}{str(field.get('name') or '')}.SOP：{sop.get('name') if sop else (sop_id or '（空）')}")
    if sop and sop.get("mode"):
        lines.append(f"{prefix}  SOP 模式：{_mode_label(sop.get('mode'))}")

    ops_by_id = _people_by_id(field)
    operator = ops_by_id.get(str(parent.get("operator_record_id") or ""))
    reviewer = ops_by_id.get(str(parent.get("reviewer_record_id") or ""))
    lines.append(f"{prefix}{str(field.get('name') or '')}.操作人：{operator.get('name') if operator else (parent.get('operator_record_id') or '（空）')}")
    lines.append(f"{prefix}{str(field.get('name') or '')}.审核人：{reviewer.get('name') if reviewer else (parent.get('reviewer_record_id') or '（空）')}")

    runs = parent.get("runs") or []
    if runs:
        lines.append(f"{prefix}{str(field.get('name') or '')}.运行：{_runs_display(runs)}")
    else:
        lines.append(f"{prefix}{str(field.get('name') or '')}.运行：（空）")

    directory = str(field.get("directory_scope") or "")
    if status == "needs_input":
        if not field.get("sops") or not field.get("people"):
            lines.append(f"{prefix}（尚未读取 SOP 目录，回复“查看选项 {str(field.get('name') or '')} [楼栋]”加载）")
        else:
            lines.append(f"{prefix}（目录楼栋：{directory}；回复“查看选项 {str(field.get('name') or '')} [楼栋]”可重新加载）")
    return lines


def _mode_label(mode):
    return {"normal": "普通作业", "cooling": "制冷调整", "polling": "轮巡"}.get(mode, str(mode))


def _runs_display(runs):
    parts = []
    for run in runs or []:
        if not isinstance(run, dict):
            parts.append(safe_text(str(run), limit=60))
            continue
        if run.get("other_unit"):
            parts.append(f"{run.get('from_unit')}→{run.get('to_unit')}／{run.get('other_unit')}")
        elif run.get("from_unit") or run.get("to_unit"):
            parts.append(f"{run.get('from_unit')}→{run.get('to_unit')}")
        elif run.get("label"):
            parts.append(str(run["label"]))
        else:
            parts.append("（运行）")
    return "、".join(parts)


# ---------------------------------------------------------------------------
# PATCH payload building
# ---------------------------------------------------------------------------


def build_native_notice_value(field, edits):
    """Merge only edited native_notice children into the existing parent value.

    ``edits`` maps child ``path`` to its typed value.  All unedited non-hidden
    children already in the parent value are preserved; hidden/read-only children
    are never inserted here and were rejected earlier.
    """
    parent = notice_parent_value(field)
    merged = {**parent, **edits}
    # A fresh start draft has no value yet: explicitly keep only what was
    # supplied (the server supplies its own defaults for untouched children).
    if not parent:
        merged = dict(edits)
    return merged


def _resolve_sop_choice(field, key, raw):
    """Resolve a SOP leaf raw value to an option key (sop_id / person record_id)."""
    if key in ("sop_id", "operator_record_id", "reviewer_record_id"):
        if key == "sop_id":
            options = field.get("sops") or []
        else:
            options = field.get("people") or []
        candidates = []
        for option in options:
            value = str(option.get("sop_id") if key == "sop_id" else option.get("record_id") or "")
            label_values = [str(option.get("name") or ""), str(option.get("label") or ""), value]
            if not value:
                continue
            if raw == value or raw in label_values:
                candidates.append(value)
        unique = list(dict.fromkeys(candidates))
        if len(unique) == 1:
            return unique[0]
        if len(unique) > 1:
            raise AssistantError("“" + safe_text(raw, limit=80) + "”对应多个选项，请填写唯一的选项值。")
        kind = "SOP" if key == "sop_id" else "人员"
        raise AssistantError("请从已读取的" + kind + "目录中选择：请先发送“查看选项 "
                             + str(field.get("name") or "") + " [楼栋]”。")
    if key == "scope":
        values, labels = _scope_candidates(field)
        if raw in values:
            return raw
        if raw in labels:
            return labels[raw]
        raise AssistantError("楼栋须为本操作权限内的候选：请从“查看选项 "
                             + str(field.get("name") or "") + "”中的楼栋选择。")
    if key == "exempt":
        low = raw.strip().lower()
        if low in {"true", "1", "是", "勾选", "checked", "对"}:
            return True
        if low in {"false", "0", "否", "取消", "unchecked", "不对"}:
            return False
        raise AssistantError("不使用工单请填写 是/否。")
    raise AssistantError("不支持的 SOP 填写项。")


def _parse_runs_for_mode(sop, raw):
    """Parse run lines according to the selected SOP's mode."""
    mode = (sop or {}).get("mode", "")
    raw = (raw or "").strip()
    tokens = [token for token in re.split(r"[；;，,、]+", raw) if token.strip()]
    runs = []
    if mode == "normal":
        raise AssistantError("普通/维保作业无需填写设备指向，SOP 会按作业名称生成。")
    for token in tokens:
        token = token.strip()
        # from→to／other  (cooling) -- arrow separators may be -> or the unicode arrow.
        arrow = r"(?:->|→)"
        cooling = re.fullmatch(r"([^\s／/]+)" + arrow + r"([^\s／/]+)[／/]([^\s]+)", token)
        if cooling:
            runs.append({"from_unit": cooling.group(1), "to_unit": cooling.group(2), "other_unit": cooling.group(3)})
            continue
        cooling2 = re.fullmatch(r"([^\s／/]+)[／/]([^\s]+)" + arrow + r"([^\s]+)", token)
        if cooling2:
            runs.append({"other_unit": cooling2.group(1), "from_unit": cooling2.group(2), "to_unit": cooling2.group(3)})
            continue
        simple = re.fullmatch(r"([^\s]+)" + arrow + r"([^\s]+)", token)
        if simple:
            runs.append({"from_unit": simple.group(1), "to_unit": simple.group(2)})
            continue
        raise AssistantError("设备指向格式无效：“" + safe_text(token, limit=80)
                             + "”。轮巡/调整请按“起点#→终点#”（制冷再以“／”加调整单元）。")
    return runs


def build_notice_sop_value(field, edits):
    """Build the SOP selection object for PATCH from leaf edits.

    ``edits`` maps SOP leaf key to raw string values.  Every numeric/option ID
    is resolved from the directory already loaded into ``field``; no client IDs
    are invented.  Exemption must be explicitly selected (``不使用工单：是``).

    When the SOP itself is changed without ``runs`` being filled, the previous
    SOP's runs are never silently reused: ``selection_payload`` semantics mean a
    ``normal`` SOP regenerates its own run label (so the old runs are cleared),
    while ``cooling``/``polling`` require explicit runs (an explicit prompt).
    """
    parent = notice_parent_value(field)
    selection = {"exempt": bool(parent.get("exempt")), "scope": str(parent.get("scope") or ""),
                 "sop_id": str(parent.get("sop_id") or ""),
                 "operator_record_id": str(parent.get("operator_record_id") or ""),
                 "reviewer_record_id": str(parent.get("reviewer_record_id") or ""),
                 "runs": list(parent.get("runs") or [])}
    old_sop = _selected_sop(field, selection["sop_id"])
    old_mode = (old_sop or {}).get("mode", "")
    current_sop_id = selection["sop_id"]
    # If this same message also selects a new SOP, runs must be validated
    # against the newly chosen SOP's mode, not the previously stored one.
    if "sop_id" in edits:
        current_sop_id = _resolve_sop_choice(field, "sop_id", edits["sop_id"])
    for key, raw in edits.items():
        if key == "exempt":
            selection["exempt"] = _resolve_sop_choice(field, "exempt", raw)
        elif key == "scope":
            selection["scope"] = _resolve_sop_choice(field, "scope", raw)
        elif key in ("sop_id", "operator_record_id", "reviewer_record_id"):
            selection[key] = _resolve_sop_choice(field, key, raw)
        elif key == "runs":
            selection["runs"] = _parse_runs_for_mode(_selected_sop(field, current_sop_id), raw)
    # Defect #3: switching SOP type must never silently carry over the previous
    # SOP's runs.  Mirror selection_payload's clear/requires-fill behaviour.
    if "sop_id" in edits and "runs" not in edits:
        new_sop = _selected_sop(field, current_sop_id)
        new_mode = (new_sop or {}).get("mode", "")
        if new_mode != old_mode:
            if new_mode == "normal":
                # selection_payload regenerates the normal-mode run label itself.
                selection["runs"] = []
            elif new_mode in ("cooling", "polling"):
                raise AssistantError("已更换SOP，该SOP需要填写设备指向，请补充“"
                                     + str(field.get("name") or "step0.notice_sop")
                                     + ".运行：[起点#→终点#]”（制冷以“／”加调整单元）。")
    return selection


def _selected_sop(field, sop_id):
    return _sops_by_id(field).get(str(sop_id or ""))


# ---------------------------------------------------------------------------
# Text fragments reused by feishu_assistant
# ---------------------------------------------------------------------------


def leaf_options_line(field, child, search):
    from .feishu_assistant import _filter_options, _options_text
    options = child.get("options") or []
    filtered = _filter_options(options, search)
    pseudo = {**child, "name": str(field.get("name") or "") + "." + (str(child.get("label") or child.get("path") or ""))}
    return _options_text(pseudo, filtered, search)


def full_native_fields_text(plan):
    """Render a needs_input plan including flattened native notice/SOP fields.

    Returns a tuple ``(text, any_editable)`` that mirrors ``_fields_text``.
    """
    lines = ["**" + safe_text(str(plan.get("title") or "业务操作"), limit=120)
             + "**（待补充字段，版本 " + str(plan.get("version")) + "）"]
    fields = plan.get("fields", [])
    editable = False
    if not fields:
        lines.append("（没有需要补充的字段）")
    for field in fields[:40]:
        name = str(field.get("name"))
        label = safe_text(str(field.get("label") or ""), limit=60)
        value = _generic_value_display(field)
        if field.get("native_notice"):
            lines.append(f"{name}（{label}）：")
            sub, sub_editable = native_notice_display(field)
            lines.extend(sub)
            editable = editable or sub_editable
            continue
        if field.get("native_notice_sop"):
            lines.append(f"{name}（{label}）：")
            sub = notice_sop_display(field, status=plan.get("status"))
            lines.extend(sub)
            if not field_protected(field):
                editable = True
                lines.append("  → 查看选项 " + name)
            continue
        if child_protected(field):
            lines.append(f"{name}（{label}）：{value}（只读，不可修改）")
            continue
        if not _generic_supported(field):
            lines.append(f"{name}（{label}）：{value}（复杂表单，请在灯塔原业务页面填写）")
            continue
        editable = True
        line = f"{name}（{label}）：{value}"
        if field.get("options_source") and field.get("type") in {"select", "multiselect", "radio"}:
            line += "  → 查看选项 " + name
        lines.append(line)
    if len(fields) > 40:
        lines.append("（共" + str(len(fields)) + "个字段，仅展示前40个）")
    lines.append("")
    if editable:
        lines.append("请按“字段名：值”逐行回复，不填写的行保持不变。多选用“、”分隔。")
    else:
        lines.append("以上字段均不可通过飞书文本修改；请在灯塔原业务页面填写提交。")
    return "\n".join(lines), editable


def _generic_value_display(field):
    from .feishu_assistant import _field_value_display
    return _field_value_display(field)


def _generic_supported(field):
    from .feishu_assistant import _feishu_supported_type
    return _feishu_supported_type(field)