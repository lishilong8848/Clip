"""In-card editing of native notice drafts and SOP/person selection.

This module is the card (schema 2.0 ``form`` container) front-end for the
existing, authoritative plan APIs.  It does NOT create a new business execution
chain and nothing here writes real work orders:

* ``prepare_card`` (async) renders an editable card from a public plan and
  persists an immutable, short-lived ``ticket`` in ``bot.runtime.state_store``.
  The caller may supply an (optionally async) ``load_options`` hook that warms
  the plan's SOP directory before rendering.
* ``handle_card`` (async) validates the server-issued ``ticket`` (owner union /
  open id, plan id + version, conversation and original chat) against the real
  operator, then either saves card form edits via the existing PATCH ``amend``
  API (never executes), returns a plan to ``needs_input`` via ``action: edit``,
  cancels via ``cancel``, applies a recommendation (adopt) locally without
  submitting, loads the SOP directory (``field_options``), or confirms via the
  original ``confirm`` API (high-risk keeps the built-in second confirmation).
* ``parse_card_event`` turns the raw ``card.action.trigger`` payload into a
  durable, deduplicated inbox message.  Its synthetic id includes the operator
  union and the original ``event_id``, and the original card ``open_message_id``
  is preserved as ``reply_to_message_id``.
"""

from __future__ import annotations

import asyncio
import copy
import datetime as _dt
import hashlib
import inspect
import json
import logging
import re
import secrets
import sqlite3
import time

from openclaw_service.assistant.lighthouse_ai import AssistantError, safe_text
from . import feishu_notice_forms as notice_forms

log = logging.getLogger(__name__)

_TICKET_TTL_SECONDS = 30 * 60
_NAMESPACE = "feishu_notice_cards"
_ALLOWED_KINDS = frozenset({"notice_form", "notice_card"})
_ALLOWED_ACTIONS = frozenset({"save", "confirm", "edit", "cancel", "adopt", "load-directory", "add-row", "remove-row"})

_STATUS_PLAIN = {
    "draft": "草稿",
    "editing": "编辑中",
    "needs_input": "待补充内容",
    "awaiting_confirmation": "待确认",
    "awaiting_second_confirmation": "待再次确认",
    "running": "处理中",
    "submitted": "已提交，处理中",
    "accepted": "已受理",
    "completed": "已完成",
    "failed": "处理失败",
    "cancelled": "已取消",
    "superseded": "已被新操作替代",
}


def _status_plain(plan):
    """User-facing label for a plan state. Never leaks kernel status terms (#5/#7)."""
    raw = str(plan.get("status") or "")
    return _STATUS_PLAIN.get(raw, "处理中")

_SOP_LEAF_LABELS = {
    "exempt": "不使用工单",
    "scope": "楼栋",
    "sop_id": "SOP",
    "operator_record_id": "操作人",
    "reviewer_record_id": "审核人",
    "runs": "运行",
}
# Directory-dependent SOP leaves: editing them requires a loaded directory.
_SOP_DIRECTORY_KEYS = {"scope", "sop_id", "operator_record_id", "reviewer_record_id", "runs"}


def _leaf_id(kind, field, leaf) -> str:
    key = str(field.get("name") or "") + "|" + str(leaf or "")
    return kind + "_" + hashlib.sha1(key.encode("utf-8")).hexdigest()[:12]


def _coerce_card_string(raw):
    """Normalize picker values before typed coercion.

    Feishu datetime pickers append a timezone suffix.  A value *with* an offset
    is converted to Beijing time (+08:00); a pure local value is treated as
    Beijing time as-is.  The offset is applied via ``datetime`` so it can never
    silently shift/tamper the stored instant (e.g. ``-05:00`` becomes +05h).
    Non-datetime strings pass through unchanged.
    """
    if not isinstance(raw, str):
        return raw
    text = raw.strip()
    m = re.match(r"^(\d{4}-\d{2}-\d{2}[ T]\d{1,2}:\d{2}(?::\d{2})?)\s*([+-]\d{2}:?\d{2})$", text)
    if m:
        base, offset = m.group(1), m.group(2).replace(":", "")
        sign = 1 if offset[0] == "+" else -1
        off_min = sign * (int(offset[1:3]) * 60 + int(offset[3:5]))
        try:
            naive = _dt.datetime.fromisoformat(base.replace(" ", "T"))
            aware = naive.replace(tzinfo=_dt.timezone(_dt.timedelta(minutes=off_min)))
            beijing = aware.astimezone(_dt.timezone(_dt.timedelta(hours=8)))
            return beijing.strftime("%Y-%m-%dT%H:%M")
        except ValueError:
            return text
    return text


def _multiselect_join(raw):
    if isinstance(raw, (list, tuple)):
        return "、".join(str(item) for item in raw if item not in (None, ""))
    return raw


def _initial_datetime(value):
    return "" if value in (None, "") else str(value).replace("T", " ")


# ---------------------------------------------------------------------------
# Ticket persistence (defect #4 / #8): durable, immutable, short-lived
# ---------------------------------------------------------------------------

def _require_store(bot):
    store = None
    if bot is not None:
        runtime = getattr(bot, "runtime", None)
        if runtime is not None:
            store = getattr(runtime, "state_store", None)
    if store is None:
        raise AssistantError("当前运行环境缺少持久化门票存储，卡片操作不可用。")
    return store


_MAX_TICKETS_PER_ACTOR = 100


def _actor_prefix(message, actor):
    union = str((message or {}).get("union_id") or "") or str((actor or {}).get("id") or "")
    return hashlib.sha256(union.encode("utf-8")).hexdigest()[:12]


def _prune_tickets(store, prefix):
    """Remove expired tickets and cap live ones per actor (no full-db scan)."""
    try:
        now = time.time()
        docs = store.list_documents(_NAMESPACE, key_prefix=prefix) or []
    except sqlite3.Error:
        return
    alive = []
    for doc in docs:
        payload = doc.get("payload") or {}
        key = doc.get("key") or ""
        if now - (payload.get("created_at") or 0) > _TICKET_TTL_SECONDS:
            try:
                store.delete_document(_NAMESPACE, key)
            except sqlite3.Error:
                pass
        else:
            alive.append((payload.get("created_at") or 0, key))
    alive.sort(reverse=True)
    for _, key in alive[_MAX_TICKETS_PER_ACTOR:]:
        try:
            store.delete_document(_NAMESPACE, key)
        except sqlite3.Error:
            pass


def _issue_ticket(bot, message, actor, conversation, plan, *, event_id="", mark_consumed=False):
    store = _require_store(bot)
    ticket = _actor_prefix(message, actor) + "_" + secrets.token_urlsafe(16)
    original = str((message or {}).get("reply_to_message_id")
                   or (message or {}).get("message_id") or "")
    payload = {
        "owner_open_id": str((actor or {}).get("id") or ""),
        "owner_union": str((message or {}).get("union_id") or ""),
        "plan_id": str((plan or {}).get("id") or ""),
        "plan_version": (plan or {}).get("version"),
        "fields": (plan or {}).get("fields") or [],
        "conversation_id": str((conversation or {}).get("conversation_id") or ""),
        "chat_id": str((message or {}).get("chat_id") or ""),
        "chat_type": str((message or {}).get("chat_type") or "group"),
        "original_message_id": original,
        "event_id": str(event_id or ""),
        "created_at": time.time(),
        "consumed": bool(mark_consumed),
        "confirm_stage_executed": None,
    }
    store.put_document(_NAMESPACE, ticket, payload)
    _prune_tickets(store, _actor_prefix(message, actor))
    return ticket


def _consume_ticket(bot, ticket):
    try:
        _require_store(bot).delete_document(_NAMESPACE, str(ticket))
    except sqlite3.Error:
        log.warning("card ticket cleanup deferred")


def _plan_from_conversation(conversation, plan_id):
    for turn in (conversation or {}).get("turns", []) or []:
        p = turn.get("plan")
        if p and str(p.get("id") or "") == str(plan_id or ""):
            return p
    return None


def _validate_ticket(bot, ticket, message, actor, conversation):
    """Return ``(ticket, plan, None)`` on success, else ``(None, None, error)``."""
    store = _require_store(bot)
    tick = store.get_document(_NAMESPACE, str(ticket))
    if not isinstance(tick, dict):
        return None, None, "卡片已失效，请重新打开后处理。此操作未执行。"
    if tick.get("consumed"):
        return None, None, "该操作已处理，请勿重复点击。"
    if time.time() - (tick.get("created_at") or 0) > _TICKET_TTL_SECONDS:
        return None, None, "卡片已过期，请重新打开后处理。此操作未执行。"
    operator_open = str((actor or {}).get("id") or "")
    operator_union = str((message or {}).get("union_id") or "")
    if not operator_open or operator_open != str(tick.get("owner_open_id") or ""):
        return None, None, "只能由发起该操作的本人在卡片中操作，管理员不能代他人执行。"
    if operator_union and tick.get("owner_union") and operator_union != str(tick.get("owner_union") or ""):
        return None, None, "操作人身份不匹配，无法继续。"
    if str((message or {}).get("chat_id") or "") != str(tick.get("chat_id") or ""):
        return None, None, "原会话已变化，无法在此卡片中操作。"
    if str((conversation or {}).get("conversation_id") or "") != str(tick.get("conversation_id") or ""):
        return None, None, "原会话已清空，请重新发起并核对。"
    plan = _plan_from_conversation(conversation, tick.get("plan_id"))
    if plan is None:
        return None, None, "计划已不存在或会话已清空，请重新发起。"
    if int(plan.get("version") or -1) != int(tick.get("plan_version") or -2):
        return None, None, "计划已更新（版本变化），请重新打开最新卡片处理。"
    return tick, {**plan, "fields": tick.get("fields", plan.get("fields") or [])}, None


# ---------------------------------------------------------------------------
# Typed coercion (defect #3: no silent fallback)
# ---------------------------------------------------------------------------

def _typed_simple(field, raw):
    """Strict typed coercion for top-level simple fields (raises on nonsense)."""
    ftype = field.get("type")
    if ftype == "multiselect" and isinstance(raw, (list, tuple)):
        raw = "、".join(str(item) for item in raw if item not in (None, ""))
    raw = _coerce_card_string("" if raw is None else str(raw))
    from .feishu_assistant import _typed_value
    return _typed_value(field, raw)


def _field_protected_self(field):
    return bool(field.get("readonly") or field.get("read_only") or field.get("disabled")
                or field.get("hidden") or field.get("只读"))


def _simple_editable(field):
    if field.get("type") not in {"text", "textarea", "number", "integer", "float", "date",
                                 "datetime-local", "time", "month", "checkbox", "boolean",
                                 "select", "radio", "multiselect"}:
        return False
    # Option-backed types are editable only once their options are loaded (inline
    # options are supported even when ``options_source`` is set); a missing
    # option list requires the options API load button instead.
    if field.get("type") in {"select", "radio", "multiselect"}:
        return bool(field.get("options"))
    return True


def _set_nested(container, parts, value):
    node = container
    for part in parts[:-1]:
        nxt = node[part] if isinstance(node, list) else node.get(part)
        if not isinstance(nxt, (dict, list)):
            nxt = {}
            node[part] = nxt
        node = nxt
    node[parts[-1]] = value
    return container


def _generic_leaves(control, value, path=(), label=""):
    if _field_protected_self(control):
        return
    label = label or str(control.get("label") or control.get("path") or "字段")
    if control.get("person_picker"):
        yield path, {**control, "type": "select", "label": label,
                     "value": value.get("record_id", "") if isinstance(value, dict) else ""}
    elif control.get("type") == "object":
        parent = value if isinstance(value, dict) else {}
        for child in control.get("children") or []:
            key = child.get("path")
            if not isinstance(key, str) or not key:
                continue
            # Native schemas use literal keys at each level, not dotted paths.
            yield from _generic_leaves(child, parent.get(key), (*path, key),
                                       label + " / " + str(child.get("label") or key))
    elif control.get("type") == "array" and control.get("item"):
        for index, item in enumerate(value if isinstance(value, list) else []):
            yield from _generic_leaves(control["item"], item, (*path, index), label + f" / {index + 1}")
    elif _simple_editable(control):
        yield path, {**control, "label": label, "value": value}


def _generic_id(field, path):
    return _leaf_id("gobj", field, json.dumps(path, ensure_ascii=False))


def _generic_arrays(control, value, path=()):
    if _field_protected_self(control):
        return
    if control.get('type') == 'array' and control.get('item'):
        rows = value if isinstance(value, list) else []
        yield path, control, rows
        for index, row in enumerate(rows):
            yield from _generic_arrays(control['item'], row, (*path, index))
    elif control.get('type') == 'object':
        parent = value if isinstance(value, dict) else {}
        for child in control.get('children') or []:
            if child.get('path'):
                yield from _generic_arrays(child, parent.get(child['path']), (*path, child['path']))


def _card_sop_value(field, edits, *, partial):
    selection = copy.deepcopy(notice_forms.notice_parent_value(field))
    selection.pop('_card_runs', None)
    # Unselected dropdowns are valid while preparing the draft, not arbitrary IDs.
    for key, raw in edits.items():
        if key == "runs":
            continue
        selection[key] = "" if raw == "" and key != "exempt" else notice_forms._resolve_sop_choice(field, key, raw)
    sop = notice_forms._selected_sop(field, selection.get("sop_id"))
    if selection.get("sop_id") != notice_forms.notice_parent_value(field).get("sop_id"):
        selection["runs"] = []
    if "runs" in edits:
        if partial:
            selection["_card_runs"] = edits["runs"]
        elif edits["runs"]:
            selection["runs"] = notice_forms._parse_runs_for_mode(sop, edits["runs"])
        else:
            selection["runs"] = []
    if not partial and not selection.get("exempt") and (sop or {}).get("mode") in {"polling", "cooling"} and not selection.get("runs"):
        raise AssistantError("所选工单需要设备指向，请先刷新表单，再补充起点和终点。")
    return selection


def _build_values(fields, form_value, *, partial=False):
    """Convert card form_value into a PATCH ``values`` payload (amend contract).

    * Empty string is an explicit clear (never dropped).
    * Unknown form keys, protected/hidden parents and protected leaves are rejected.
    * No truncation or silent fallback ever happens.
    """
    if not isinstance(form_value, dict):
        raise AssistantError("卡片表单数据无效。")
    known = set()
    values = {}
    for field in fields or []:
        name = str(field.get("name") or "")
        if notice_forms.is_native_notice(field):
            if notice_forms.field_protected(field):
                # Read-only parents are deliberately left out of ``known`` so a
                # fabricated key for them is rejected as unknown below without
                # blocking the other, writable fields.
                continue
            edits = {}
            for child in field.get("children") or []:
                if child.get("hidden") or notice_forms.child_protected(child) or not notice_forms.child_supported(child):
                    # Protected / hidden / complex leaves are never editable here,
                    # so a fabricated value for them is rejected as unknown below.
                    continue
                cid = _leaf_id("nnot", field, child.get("path"))
                known.add(cid)
                if cid not in form_value:
                    continue
                raw = form_value[cid]
                if raw is None:
                    raise AssistantError("通告字段“" + safe_text(child.get("label") or child.get("path"), limit=60)
                                         + "”的内容无效，请重新填写。")
                if child.get("type") == "multiselect":
                    raw = _multiselect_join(raw)
                if str(raw) == "":
                    edits[child["path"]] = ""
                    continue
                try:
                    edits[child["path"]] = notice_forms.typed_child_value(child, _coerce_card_string(str(raw)))
                except AssistantError:
                    raise
            if edits:
                values[name] = notice_forms.build_native_notice_value(field, edits)
        elif notice_forms.is_native_notice_sop(field):
            if notice_forms.field_protected(field):
                continue
            directory_loaded = bool(field.get("sops")) or bool(field.get("people")) or bool(field.get("scopes"))
            edits = {}
            for key in ("exempt", "scope", "sop_id", "operator_record_id", "reviewer_record_id", "runs"):
                cid = _leaf_id("sop", field, key)
                known.add(cid)
                if cid not in form_value:
                    continue
                raw = form_value[cid]
                if raw is None:
                    raise AssistantError(_SOP_LEAF_LABELS[key] + "选择无效，请重新选择。")
                edits[key] = _coerce_card_string(str(raw))
            if edits:
                if any(edits.get(k) not in (None, "") for k in _SOP_DIRECTORY_KEYS) and not directory_loaded:
                    raise AssistantError("SOP 目录尚未加载，请先点击卡片中的“读取工单及人员”后再选择。")
                try:
                    values[name] = _card_sop_value(field, edits, partial=partial)
                except AssistantError:
                    raise
        elif field.get("type") in {"object", "array"} or field.get("person_picker"):
            if _field_protected_self(field):
                continue
            updated = copy.deepcopy(field.get("value") or ([] if field.get("type") == "array" else {}))
            changed = False
            for path, child in _generic_leaves(field, field.get("value")):
                cid = _generic_id(field, path)
                known.add(cid)
                if cid not in form_value:
                    continue
                raw = form_value[cid]
                if raw is None:
                    raise AssistantError("字段“" + safe_text(child.get("label") or path, limit=60)
                                         + "”的内容无效。")
                typed = "" if raw == "" else _typed_simple(child, raw)
                if child.get("person_picker"):
                    typed = next((copy.deepcopy(o["person"]) for o in child.get("options", []) if o.get("value") == typed), {})
                updated = _set_nested(updated, path, typed) if path else typed
                changed = True
            if changed:
                values[name] = updated
        else:
            if _field_protected_self(field):
                # Read-only top-level fields are skipped; a forged key is unknown below.
                continue
            if not _simple_editable(field):
                continue  # complex / option-source without loaded options needs the options API
            cid = _leaf_id("simple", field, name)
            known.add(cid)
            if cid not in form_value:
                continue
            raw = form_value[cid]
            if raw is None:
                raise AssistantError("字段“" + safe_text(field.get("label") or name, limit=60) + "”的内容无效。")
            if str(raw) == "":
                values[name] = ""
                continue
            if field.get("type") == "multiselect":
                raw = _multiselect_join(raw)
            values[name] = _typed_simple(field, raw)
    unknown = [key for key in form_value.keys() if key not in known]
    if unknown:
        raise AssistantError("表单包含未知字段，已拒绝保存。请重新打开最新卡片操作。")
    return values


# ---------------------------------------------------------------------------
# Card component builders (schema 2.0)
# ---------------------------------------------------------------------------

def _plain(text, limit=120):
    return {"tag": "plain_text", "content": safe_text(str(text or ""), limit=limit)}


def _option(value, label):
    return {"text": _plain(label, limit=80), "value": str(value)}


def _select_static(cid, options, initial_value):
    valid = {str(o.get("value")) for o in options} if options else set()
    initial = str(initial_value) if initial_value not in (None, "") and str(initial_value) in valid else ""
    return {"tag": "select_static", "name": cid, "placeholder": {"tag": "plain_text", "content": "请选择"},
            **({"initial_option": initial} if initial else {}),
            "options": [o for o in (options or []) if o.get("value") is not None]}


def _multi_select_static(cid, options, values):
    valid = {str(o.get("value")) for o in options} if options else set()
    selected = [str(v) for v in (values if isinstance(values, (list, tuple)) else [values])
                if v not in (None, "") and str(v) in valid]
    return {"tag": "multi_select_static", "name": cid, "placeholder": {"tag": "plain_text", "content": "请选择"},
            "selected_values": selected, "options": [o for o in (options or []) if o.get("value") is not None]}


def _yes_no_select(cid, value):
    return {"tag": "select_static", "name": cid, "placeholder": {"tag": "plain_text", "content": "请选择"},
            "initial_option": "是" if value else "否",
            "options": [_option("是", "是"), _option("否", "否")]}


def _label_md(label):
    return {"tag": "markdown", "content": "**" + safe_text(str(label or "字段"), limit=120) + "**"}


def _simple_component(field, cid):
    """Render a top-level simple field as a schema 2.0 form component."""
    ftype = field.get("type")
    value = field.get("value")
    options = [o for o in field.get("options") or [] if not o.get("disabled")]
    if ftype in {"select", "radio"}:
        if not options:
            return None, []
        comp = _select_static(cid, [_option(o.get("value"), o.get("label") or o.get("value")) for o in options], value)
        return (comp, [])
    if ftype == "multiselect":
        if not options:
            return None, []
        comp = _multi_select_static(cid, [_option(o.get("value"), o.get("label") or o.get("value")) for o in options],
                                    value if isinstance(value, list) else ([value] if value not in (None, "") else []))
        return (comp, [])
    if ftype in {"boolean", "checkbox"}:
        return (_yes_no_select(cid, bool(value)), [])
    if ftype == "date":
        return ({"tag": "date_picker", "name": cid, "placeholder": {"tag": "plain_text", "content": "请选择日期"},
                 **({"initial_date": str(value)} if value else {})}, [])
    if ftype == "datetime-local":
        return ({"tag": "picker_datetime", "name": cid, "placeholder": {"tag": "plain_text", "content": "请选择日期时间"},
                 **({"initial_datetime": _initial_datetime(value)} if value else {})}, [])
    if ftype == "time":
        return ({"tag": "picker_time", "name": cid, "placeholder": {"tag": "plain_text", "content": "请选择时间"},
                 **({"initial_time": str(value)} if value else {})}, [])
    return ({"tag": "input", "name": cid, "placeholder": {"tag": "plain_text", "content": "请填写"},
             "input_type": "multiline_text" if ftype == "textarea" else "text",
             **({"rows": 2} if ftype == "textarea" else {}),
             "default_value": "" if value in (None, "") else str(value)}, [])


def _notice_leaf_component(field, child, cid):
    """Render one writable native_notice leaf as a schema 2.0 form component."""
    value = (notice_forms.notice_parent_value(field) or {}).get(child.get("path"))
    return _simple_component({**child, "value": value}, cid)


def _scope_options(field):
    options = []
    for item in field.get("scopes") or []:
        if isinstance(item, dict):
            value = str(item.get("value") or "")
            label = str(item.get("label") or value)
        elif item is not None:
            value = str(item)
            label = notice_forms.scope_code_label(value)
        else:
            continue
        if value:
            options.append(_option(value, label))
    return options


def _sop_component(field, key, cid, mode):
    """Render one SOP leaf. Directory-dependent selects return None if not loaded."""
    parent = notice_forms.notice_parent_value(field)
    current = parent.get(key)
    if key == "exempt":
        return _yes_no_select(cid, bool(current))
    if key == "scope":
        options = _scope_options(field)
        if not options:
            return None
        return _select_static(cid, options, current)
    if key in ("sop_id", "operator_record_id", "reviewer_record_id"):
        items = field.get("sops") if key == "sop_id" else field.get("people")
        val_key = "sop_id" if key == "sop_id" else "record_id"
        options = [_option(it.get(val_key), it.get("name") or it.get("label") or it.get(val_key))
                   for it in (items or []) if it.get(val_key)]
        if not options:
            return None
        return _select_static(cid, options, current)
    # runs
    if mode not in {"polling", "cooling"}:
        return None
    display = parent.get("_card_runs", notice_forms._runs_display(parent.get("runs") or []))
    return {"tag": "input", "name": cid, "placeholder": {"tag": "plain_text", "content": "起点#→终点#（轮巡可多组，制冷可“／”加调整单元）"},
            "default_value": str(display)}


def _sop_directory_loaded(field):
    return bool(field.get("sops")) or bool(field.get("people")) or bool(field.get("scopes"))


def _action_button(name, text, kind, ticket, action, style="default", extra=None):
    value = {"kind": kind, "ticket": ticket, "action": action}
    if extra:
        value.update(extra)
    return {"tag": "button", "name": name, "type": style,
            "text": _plain(text), "behaviors": [{"type": "callback", "value": value}]}


def _submit_button(ticket):
    return {"tag": "button", "name": "save", "form_action_type": "submit", "type": "primary",
            "text": _plain("保存修改"),
            "behaviors": [{"type": "callback", "value": {"kind": "notice_form", "ticket": ticket, "action": "save"}}]}


# ---------------------------------------------------------------------------
# Editable card builder
# ---------------------------------------------------------------------------

def _intro_markdown(plan, editable):
    title = safe_text(str(plan.get("title") or "业务操作"), limit=120)
    version = plan.get("version")
    line = "**" + title + "**（版本 " + str(version) + "，状态：" + _status_plain(plan) + "）"
    if editable:
        line += "\n请在卡片中修改可编辑字段，然后点击【保存修改】。保存仅更新操作计划，不会执行任何业务。"
    else:
        line += "\n当前没有可编辑的字段可在卡片中修改，请在灯塔原页面继续处理。"
    return {"tag": "markdown", "content": line}


def _form_submit_button(name, text, ticket, action, style="default", extra=None):
    value = {"kind": "notice_form", "ticket": ticket, "action": action}
    if extra:
        value.update(extra)
    return {"tag": "button", "name": name, "form_action_type": "submit", "type": style,
            "text": _plain(text), "behaviors": [{"type": "callback", "value": value}]}


def _recommendation_blocks(plan, ticket):
    """Adopt buttons for recommendations (defect #10).

    These are form submit buttons so the user's just-filled values are carried
    into the callback; adopting only merges into blank writable leaves locally
    (never submits business) and re-renders the complete form.
    """
    recommendations = ((plan.get("planned_notice") or {}).get("recommendations")) or []
    elements = []
    for idx, rec in enumerate(recommendations):
        source_building = rec.get("source_building") or ""
        source_title = rec.get("source_title") or ""
        suggested = rec.get("suggested_values") or {}
        summary = "推荐来自：" + safe_text(str(source_building), limit=40) + " · "
        summary += safe_text(str(source_title), limit=80)
        if isinstance(suggested, dict) and suggested:
            summary += "\n可预填 " + "、".join(safe_text(str(k), limit=30) for k in list(suggested)[:6])
        elements.append({"tag": "markdown", "content": "## 推荐（" + str(idx + 1) + "）\n" + summary})
        elements.append(_form_submit_button(
            _leaf_id("adopt", {"name": "rec"}, idx), "采用此推荐",
            ticket, "adopt", style="default", extra={"rec_index": idx}))
    return elements


def _cancel_button(ticket, kind="notice_card"):
    return _action_button("cancel", "取消操作", kind, ticket, "cancel", style="default")


def _load_buttons(plan, ticket):
    """Submit-style load buttons for SOP directories and option-source selects."""
    buttons = []
    for field in plan.get("fields") or []:
        if notice_forms.is_native_notice_sop(field) and not notice_forms.field_protected(field):
            buttons.append(_form_submit_button(
                _leaf_id("sopbtn", field, "load"), "刷新工单表单" if _sop_directory_loaded(field) else "读取工单及人员",
                ticket, "load-directory", style="default",
                extra={"field": str(field.get("name") or "")}))
        elif (field.get("type") in {"select", "radio", "multiselect"}
                and not (field.get("options") or []) and not _field_protected_self(field)):
            buttons.append(_form_submit_button(
                _leaf_id("optbtn", field, "load"), "读取选项",
                ticket, "load-directory", style="default",
                extra={"field": str(field.get("name") or "")}))
    return buttons


def _array_buttons(plan, ticket):
    buttons = []
    for field in plan.get('fields') or []:
        if notice_forms.is_native_notice(field) or notice_forms.is_native_notice_sop(field):
            continue
        for path, control, rows in _generic_arrays(field, field.get('value')):
            key = _generic_id(field, path)
            for action, allowed, text in (('add-row', len(rows) < control.get('maxItems', 2000), '增加一条'),
                                          ('remove-row', len(rows) > control.get('minItems', 0), '移除最后一条')):
                if allowed:
                    buttons.append(_form_submit_button(action + key, str(control.get('label') or field.get('label') or '明细') + '：' + text,
                                                       ticket, action, extra={'array': key}))
    return buttons


def _photos_markdown(field):
    """Renderer for native_notice_photos fields: show selected photo names and
    explicit sending instructions. Never fabricates a fake file-upload control
    and never leaks the internal field name or an empty maxBytes hint (audit #7)."""
    kind = str(field.get("native_notice_photos"))
    label = safe_text(str(field.get("label") or ""), limit=120) or ("现场照片" if kind == "site" else "资料截图")
    names = []
    for item in field.get("selected_files") or []:
        if isinstance(item, dict):
            names.append(safe_text(str(item.get("name") or "照片"), limit=120))
        elif item:
            names.append(safe_text(str(item), limit=120))
    max_items = int(field.get("maxItems") or 1)
    lines = ["## " + label]
    if names:
        lines.append("已选照片：**" + ("、".join(names[:max_items])) + "**"
                     + ((" 等共 " + str(len(names)) + " 张") if len(names) > max_items else ""))
    else:
        lines.append("（尚未选择照片）")
    lines.append("> 飞书卡片不能直接上传文件控件，请把照片图片直接发送到本会话补充"
                 "（单张 ≤8MiB，最多 " + str(max_items) + " 张）。"
                 "若同时有多个待补充照片的通告/类别，请先回复“补照片计划 N”指定要补哪一项。")
    return {"tag": "markdown", "content": "\n".join(lines)}


def _photo_guidance_text(plan):
    """Short text hint for a needs_input plan that has pending notice photos.
    A photo field is "missing" only when it is required and empty (at least one
    required); optional site/alias fields are never reported as missing (audit #1)."""
    missing, shown, optional = [], [], []
    for field in (plan.get("fields") or []):
        if not field.get("native_notice_photos"):
            continue
        kind = str(field.get("native_notice_photos"))
        label = safe_text(str(field.get("label") or ""), limit=60) or ("现场照片" if kind == "site" else "资料截图")
        count = len(field.get("value") or field.get("selected_files") or [])
        required = bool(field.get("required"))
        if count == 0 and required:
            missing.append(label)
        elif count == 0:
            optional.append(label + "（可选）")
        else:
            shown.append(f"{label}（已选 {count} 张）")
    parts = []
    if shown:
        parts.append("已选：" + "、".join(shown))
    if missing:
        parts.append("缺少：" + "、".join(missing))
    if optional:
        parts.append("可选未填：" + "、".join(optional))
    if not parts:
        return ""
    return "照片状态：" + "；".join(parts) + "。请直接发送照片图片到本会话补充。"


def _build_editable_card(plan, ticket):
    title = safe_text(str(plan.get("title") or "业务操作"), limit=120)
    kind = "notice_card"
    elements = []
    form_children = [_intro_markdown(plan, editable=False)]
    needs_form = False
    editable_any = False
    for field in plan.get("fields") or []:
        name = str(field.get("name") or "")
        if field.get("native_notice_photos"):
            form_children.append(_photos_markdown(field))
            # Photo info is sent as direct chat images, not a Feishu input control,
            # so it never appears in form_value.  A photo-only needs_input card must
            # still render its Save button so the operator can proceed after sending
            # the photos; _handle_save preserves the selected files.
            needs_form = True
            editable_any = True
            continue
        if notice_forms.is_native_notice(field):
            if notice_forms.field_protected(field):
                form_children.append({"tag": "markdown", "content": "## " + safe_text(field.get("label") or name, limit=120)
                                      + "（只读/隐藏，不可修改）"})
                continue
            form_children.append({"tag": "markdown", "content": "## " + safe_text(field.get("label") or name, limit=120)})
            rendered = False
            for child in field.get("children") or []:
                if child.get("hidden") or notice_forms.child_protected(child) or not notice_forms.child_supported(child):
                    continue
                cid = _leaf_id("nnot", field, child.get("path"))
                comp, notes = _notice_leaf_component(field, child, cid)
                if comp:
                    form_children.append(_label_md(child.get("label") or child.get("path")))
                    for note in notes:
                        form_children.append({"tag": "markdown", "content": note})
                    form_children.append(comp)
                    rendered = True
                    editable_any = True
                    needs_form = True
            if not rendered:
                form_children.append({"tag": "markdown", "content": "（该字段没有可编辑项）"})
        elif notice_forms.is_native_notice_sop(field):
            if notice_forms.field_protected(field):
                form_children.append({"tag": "markdown", "content": "## " + safe_text(field.get("label") or name, limit=120)
                                      + "（只读/隐藏，不可修改）"})
                continue
            form_children.append({"tag": "markdown", "content": "## " + safe_text(field.get("label") or name, limit=120)})
            parent = notice_forms.notice_parent_value(field)
            sop_id = str(parent.get("sop_id") or "")
            selected_sop = next((s for s in (field.get("sops") or []) if str(s.get("sop_id")) == sop_id), None)
            mode = (selected_sop or {}).get("mode") or ""
            rendered = False
            for key in ("exempt", "scope", "sop_id", "operator_record_id", "reviewer_record_id", "runs"):
                cid = _leaf_id("sop", field, key)
                comp = _sop_component(field, key, cid, mode)
                if comp:
                    form_children.append(_label_md(_SOP_LEAF_LABELS[key]))
                    form_children.append(comp)
                    rendered = True
                    needs_form = True
            if not _sop_directory_loaded(field):
                form_children.append({"tag": "markdown",
                                      "content": "⚠️ SOP 目录尚未加载，请先点击下方【读取工单及人员】后再选择楼栋/SOP/人员。"})
            if not rendered and _sop_directory_loaded(field):
                form_children.append({"tag": "markdown", "content": "（该 SOP 字段当前无可编辑项）"})
            editable_any = editable_any or rendered
        elif field.get("type") in {"object", "array"} or field.get("person_picker"):
            if _field_protected_self(field):
                form_children.append({"tag": "markdown", "content": "## " + safe_text(field.get("label") or name, limit=120)
                                      + "（只读/隐藏，不可修改）"})
                continue
            form_children.append({"tag": "markdown", "content": "## " + safe_text(field.get("label") or name, limit=120)})
            rendered = False
            for path, child in _generic_leaves(field, field.get("value")):
                cid = _generic_id(field, path)
                comp, notes = _simple_component(child, cid)
                if comp:
                    form_children.append(_label_md(child.get("label") or path))
                    for note in notes:
                        form_children.append({"tag": "markdown", "content": note})
                    form_children.append(comp)
                    rendered = True
                    editable_any = True
                    needs_form = True
            if not rendered:
                form_children.append({"tag": "markdown", "content": "（该字段没有可编辑项）"})
        else:
            if _field_protected_self(field) or not _simple_editable(field):
                continue
            cid = _leaf_id("simple", field, name)
            comp, notes = _simple_component(field, cid)
            if comp:
                form_children.append(_label_md(field.get("label") or name))
                for note in notes:
                    form_children.append({"tag": "markdown", "content": note})
                form_children.append(comp)
                editable_any = True
                needs_form = True

    load_buttons = _load_buttons(plan, ticket) + _array_buttons(plan, ticket)
    recommend = _recommendation_blocks(plan, ticket)
    needs_form = needs_form or bool(load_buttons) or bool(recommend)

    # The intro line must reflect actual editability now that we know.
    form_children[0] = _intro_markdown(plan, editable=bool(needs_form))
    if needs_form:
        form_children.extend(load_buttons)
        form_children.extend(recommend)
        if editable_any:
            form_children.append(_submit_button(ticket))
        elements.append({"tag": "form", "name": "notice_form", "elements": form_children})
    else:
        # No editable field and no load/recommend actions: a form container would
        # be illegal JSON2, so fall back to plain markdown.
        elements.extend(form_children)
        elements.extend(recommend)

    elements.append(_cancel_button(ticket, kind))
    card = {"schema": "2.0",
            "header": {"title": {"tag": "plain_text", "content": title}},
            "body": {"elements": elements}}
    return card


# ---------------------------------------------------------------------------
# Public card API (async)
# ---------------------------------------------------------------------------

async def prepare_card(bot, message, actor, conversation, plan, *, load_options=None):
    """Build the card for the plan's current status and persist a fresh ticket.

    ``load_options`` may be sync or async ``Callable[[plan], plan]`` used to warm
    the SOP directory before rendering; the returned plan is used for the card.

    The card is selected by ``plan.status``: ``needs_input`` renders an editable
    form; ``awaiting_*`` renders a confirm preview; any other terminal status
    renders a plain status card (no execute button).
    """
    if load_options is not None:
        loaded = load_options(plan)
        if inspect.isawaitable(loaded):
            loaded = await loaded
        plan = loaded if loaded is not None else plan
    ticket = await asyncio.to_thread(_issue_ticket, bot, message, actor, conversation, plan,
                           event_id=str((message or {}).get("event_id") or ""))
    status = plan.get("status")
    if status == "needs_input":
        card = _build_editable_card(plan, ticket)
    elif status in {"awaiting_confirmation", "awaiting_second_confirmation"}:
        card = build_preview_card(plan, ticket, just_saved=False)
    else:
        card = _build_status_card(plan, ticket)
    if len(json.dumps(card, ensure_ascii=False).encode('utf-8')) > 28_000:
        await asyncio.to_thread(_consume_ticket, bot, ticket)
        raise AssistantError('表单超出飞书单张卡片大小，请通过字段文字回复或在灯塔内继续填写。')
    return {"card": card, "ticket": ticket}


# ---------------------------------------------------------------------------
# Preview card builder (no internal IDs; shows every actual field)
# ---------------------------------------------------------------------------

_EXECUTE_LIMIT = 22_000  # Reserve space under Feishu's 30 KB card request limit.


def _simple_display(field):
    value = field.get("value")
    if value in (None, ""):
        return "（空）"
    ftype = field.get("type")
    options = field.get("options") or []
    if ftype in {"select", "radio"}:
        labels = {str(o.get("value")): str(o.get("label") or o.get("value") or "") for o in options}
        return labels.get(str(value), str(value))
    if ftype == "multiselect":
        labels = {str(o.get("value")): str(o.get("label") or o.get("value") or "") for o in options}
        items = value if isinstance(value, list) else [value]
        return "、".join(labels.get(str(x), str(x)) for x in items) or "（空）"
    if ftype in {"boolean", "checkbox"}:
        return "是" if value else "否"
    return str(value)


def _child_preview_value(field, child):
    parent = notice_forms.notice_parent_value(field) or {}
    value = parent.get(child.get("path"))
    if value in (None, ""):
        return "（空）"
    ftype = child.get("type")
    options = child.get("options") or []
    if ftype in {"select", "radio"}:
        labels = {str(o.get("value")): str(o.get("label") or o.get("value") or "") for o in options}
        return labels.get(str(value), str(value))
    if ftype == "multiselect":
        labels = {str(o.get("value")): str(o.get("label") or o.get("value") or "") for o in options}
        items = value if isinstance(value, list) else [value]
        return "、".join(labels.get(str(x), str(x)) for x in items) or "（空）"
    if ftype in {"boolean", "checkbox"}:
        return "是" if value else "否"
    return str(value)


def _name_for(field, items, key, value):
    if value in (None, ""):
        return "（空）"
    for it in (items or []):
        if isinstance(it, dict):
            if str(it.get(key) or "") == str(value):
                name = str(it.get("name") or it.get("label") or "")
                if name:
                    return name
        elif str(it) == str(value):
            # scopes/people may be bare value strings; the value itself is then the code.
            return notice_forms.scope_code_label(str(value)) if field.get("native_notice_sop") else str(value)
    return str(value)


def _runs_preview(runs):
    if not runs:
        return ""
    parts = []
    for run in (runs if isinstance(runs, list) else [runs]):
        if not isinstance(run, dict):
            parts.append(str(run))
            continue
        base = "→".join(str(run.get(k) or "") for k in ("from_unit", "to_unit") if run.get(k))
        if run.get("other_unit"):
            base = (base + "／" + str(run["other_unit"])) if base else str(run["other_unit"])
        parts.append(base or "（运行）")
    return "、".join(parts)


def _preview_field_lines(field):
    name = str(field.get("name") or "")
    label = safe_text(str(field.get("label") or name), limit=60)
    if field.get("native_notice_photos"):
        names = [safe_text(str(item.get("name") or "照片"), limit=60)
                 if isinstance(item, dict) else safe_text(str(item), limit=60)
                 for item in (field.get("selected_files") or field.get("value") or [])]
        return [f"{label}：" + (("、".join(names)) if names else "（未选）")
                + "（发送照片补充，非上传控件）"]
    if field.get("hidden"):
        return []
    if not notice_forms.is_native_notice(field) and (field.get("type") in {"object", "array"} or field.get("person_picker")):
        return [f"{child['label']}：{_simple_display(child)}" for _, child in _generic_leaves(field, field.get("value"))]
    if notice_forms.is_native_notice(field):
        lines = [f"{name}（{label}）："]
        for child in field.get("children") or []:
            if child.get("hidden") or notice_forms.child_protected(child):
                continue
            cpath = str(child.get("path") or "")
            clabel = safe_text(str(child.get("label") or cpath), limit=60)
            lines.append(f"  {cpath}（{clabel}）：{_child_preview_value(field, child)}")
        return lines
    if notice_forms.is_native_notice_sop(field):
        parent = notice_forms.notice_parent_value(field) or {}
        lines = [f"{name}（{label}）："]
        lines.append(f"  不使用工单：{'是' if parent.get('exempt') else '否'}")
        scope = parent.get("scope") or ""
        lines.append(f"  楼栋：{_name_for(field, field.get('scopes') or [], 'value', scope) if scope else '（空）'}")
        lines.append(f"  SOP：{_name_for(field, field.get('sops') or [], 'sop_id', parent.get('sop_id'))}")
        lines.append(f"  操作人：{_name_for(field, field.get('people') or [], 'record_id', parent.get('operator_record_id'))}")
        lines.append(f"  审核人：{_name_for(field, field.get('people') or [], 'record_id', parent.get('reviewer_record_id'))}")
        runs = _runs_preview(parent.get("runs"))
        if runs:
            lines.append(f"  运行：{runs}")
        return lines
    return [f"{name}（{label}）：{_simple_display(field)}"]


def _preview_fields_markdown(plan):
    lines = []
    fields = plan.get("fields") or []
    if not fields:
        for operation in plan.get("operations") or []:
            lines.append(str(operation.get("name") or "业务操作"))
            labels = operation.get("selected_labels") or {}
            visible = {**(operation.get("body") or {}), **labels}
            for key, value in visible.items():
                if str(key).startswith("_") or key.endswith(("_id", "_ids", "_token")):
                    continue
                lines.append(f"{key}：{json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else value}")
            lines.extend(str(f.get("name") or "附件") for f in operation.get("selected_files") or [])
    for field in fields:
        lines.extend(_preview_field_lines(field))
    return "\n".join(lines)


def _preview_markdown(plan, action):
    status = plan.get("status")
    title = safe_text(str(plan.get("title") or "业务操作"), limit=120)
    lines = ["**" + title + "**（版本 " + str(plan.get("version")) + "，状态：" + _status_plain(plan) + "）"]
    planned = plan.get("planned_notice") or {}
    text = planned.get("text")
    if text:
        lines.append(str(text))
    lines.append("")
    lines.append("实际填写内容：")
    lines.append(_preview_fields_markdown(plan))
    recommendations = planned.get("recommendations") or []
    if recommendations:
        lines.append("")
        lines.append("推荐：")
        for idx, rec in enumerate(recommendations):
            source_building = rec.get("source_building") or ""
            source_title = rec.get("source_title") or ""
            lines.append(f"  {idx + 1}. {safe_text(str(source_building), limit=40)} · {safe_text(str(source_title), limit=80)}")
    if status == "awaiting_second_confirmation":
        lines.append("")
        lines.append("此操作风险较高，尚未执行；请核对后确认。")
    return "\n".join(lines)


def _build_status_card(plan, ticket):
    """Plain status card (terminal statuses: no execute button)."""
    status = plan.get("status")
    title = safe_text(str(plan.get("title") or "业务操作"), limit=120)
    lines = ["**" + title + "**（版本 " + str(plan.get("version")) + "，状态：" + _status_plain(plan) + "）"]
    if status == "completed":
        lines.append("该操作已完成，无需卡片操作。")
    elif status in {"running", "submitted"}:
        lines.append("该操作正在原业务流程中处理，这里没有可以执行的按钮。")
    elif status in {"failed", "cancelled", "superseded"}:
        lines.append("此操作在当前状态不能通过卡片确认/编辑。" + ("如需重试可返回原消息或网页操作。" if status == "failed" else ""))
    else:
        lines.append("当前状态无需卡片操作，请在灯塔原页面处理。")
    elements = [{"tag": "markdown", "content": "\n".join(lines)}]
    if status in {"failed"}:
        elements.append(_cancel_button(ticket))
    card = {"schema": "2.0",
            "header": {"title": {"tag": "plain_text", "content": title}},
            "body": {"elements": elements}}
    return card


def build_preview_card(plan, ticket, *, just_saved=True):
    """Render the non-editable preview with status-appropriate action buttons.

    Field values are shown in full (no truncation and no internal SOP/people
    IDs when a display name is available).  If the preview body is too long to
    fit a single Feishu message, the execute button is withheld and a clear
    limited-notice is shown instead of letting the user confirm blind.
    """
    status = plan.get("status")
    kind = "notice_card"
    preview_text = _preview_markdown(plan, "save" if just_saved else "confirm")
    over_limit = len(preview_text.encode('utf-8')) > _EXECUTE_LIMIT
    elements = [{"tag": "markdown", "content": preview_text[:6000] if over_limit else preview_text}]
    if over_limit:
        elements.append({"tag": "markdown",
                         "content": "⚠️ 实际字段较多，飞书内预览已超限。已隐藏执行按钮，请在灯塔原页面核对全部字段后再确认，卡片不会缺失字段执行。"})
    elif status == "awaiting_second_confirmation":
        elements.append(_action_button("confirm", "再次确认执行", kind, ticket, "confirm", style="danger"))
    elif status == "awaiting_confirmation":
        elements.append(_action_button("confirm", "确认执行", kind, ticket, "confirm", style="primary"))
    elif status in {"needs_input", "failed"}:
        elements.append({"tag": "markdown",
                         "content": "（当前状态不允许确认执行。可返回编辑或取消该待处理操作。）"})
    if status in {"needs_input", "awaiting_confirmation", "awaiting_second_confirmation", "failed"}:
        elements.append(_action_button("edit", "编辑", kind, ticket, "edit", style="default"))
    if status in {"needs_input", "awaiting_confirmation", "awaiting_second_confirmation"}:
        elements.append(_cancel_button(ticket, kind))
    card = {"schema": "2.0",
            "header": {"title": {"tag": "plain_text", "content": safe_text(str(plan.get("title") or "业务操作"), limit=120)}},
            "body": {"elements": elements}}
    return card


# ---------------------------------------------------------------------------
# Recommendation / directory helpers (local, never auto-submits)
# ---------------------------------------------------------------------------

def _apply_recommendation(plan, suggested_values):
    """Merge only into currently blank, editable native_notice leaves (no submit)."""
    modified = copy.deepcopy(plan)
    if not isinstance(suggested_values, dict):
        return modified
    for field in modified.get("fields") or []:
        if not notice_forms.is_native_notice(field) or notice_forms.field_protected(field):
            continue
        parent = notice_forms.notice_parent_value(field)
        value = dict(parent)
        changed = False
        for child in field.get("children") or []:
            if child.get("hidden") or notice_forms.child_protected(child) or not notice_forms.child_supported(child):
                continue
            path = child.get("path")
            if path in suggested_values and (parent.get(path) in (None, "")):
                value[path] = suggested_values[path]
                changed = True
        if changed:
            field["value"] = value
    return modified


def _merge_directory(base_plan, updated):
    """Merge directory/options from a ``field_options`` result into a plan copy."""
    modified = copy.deepcopy(base_plan)
    name = str(updated.get("name") or "")
    for field in modified.get("fields") or []:
        if str(field.get("name") or "") != name:
            continue
        for key in ("sops", "people", "scopes", "directory_scope", "options",
                    "options_has_more", "options_total", "work_type"):
            if key in updated:
                field[key] = updated.get(key)
        break
    # The full plan returned by the options API carries the incremented version;
    # use it so the freshly minted ticket matches the next step's plan.
    if base_plan.get("version") is not None:
        modified["version"] = base_plan.get("version")
    return modified


# ---------------------------------------------------------------------------
# Card action handlers (async, real bot._call)
# ---------------------------------------------------------------------------

def _reply_text(text, **extra):
    return {"text": text, **extra}


def _merge_form_values(plan, form_value):
    """Merge a submitted form's writable values into a local plan draft (no PATCH).

    Used by adopt / load-directory so a user's just-filled (but not yet saved)
    values are preserved when the card is re-rendered.
    """
    if not isinstance(form_value, dict) or not form_value:
        return plan
    values = _build_values(plan.get("fields") or [], form_value, partial=True)
    if not values:
        return plan
    modified = copy.deepcopy(plan)
    by_name = {str(f.get("name") or ""): f for f in modified.get("fields") or []}
    for name, val in values.items():
        field = by_name.get(name)
        if field is not None:
            field["value"] = val
    return modified


async def _handle_save(bot, message, request, actor, conversation, plan, ticket):
    card = message.get("card") or {}
    try:
        values = _build_values(plan.get("fields") or [], card.get("form_value") or {})
    except AssistantError as exc:
        return _reply_text(str(exc), error=str(exc))
    # Photo fields are NOT Feishu input controls and therefore never appear in
    # form_value.  Preserve their currently selected files on every save so a
    # photo-only card can be saved (and the selection is never silently dropped).
    for field in plan.get("fields") or []:
        if not field.get("native_notice_photos"):
            continue
        selected = field.get("value") or field.get("selected_files") or []
        ids = []
        for item in selected:
            if isinstance(item, dict):
                fid = item.get("file_id") or item.get("id")
                if fid:
                    ids.append(fid)
            elif item not in (None, ""):
                ids.append(item)
        if ids:
            values[str(field.get("name") or "")] = ids
    if not values:
        return _reply_text("没有需要保存的字段变更，请修改后再保存。", error="empty_values")
    try:
        result = await bot._call(request, actor, "plans/" + plan["id"],
                                 {"version": plan["version"], "values": values},
                                 method="PATCH")
    except AssistantError as exc:
        return _reply_text(safe_text(str(exc)), error=safe_text(str(exc)))
    except Exception as exc:  # pragma: no cover - defensive
        log.warning("card save failed: %s", type(exc).__name__)
        return _reply_text("保存未完成，请稍后重试。未执行业务。", error="save_failed")
    # Missing required fields: keep the editable card instead of a confirm preview.
    if (result or {}).get("status") == "needs_input" or (result or {}).get("missing_fields"):
        await asyncio.to_thread(_consume_ticket, bot, ticket)
        editable = await prepare_card(bot, message, actor, conversation, result or plan)
        return _reply_text("已保存修改，但仍缺少部分必填项，请继续在卡片中补充核对。",
                           card=editable["card"], status="needs_more", plan=result,
                           ticket=editable["ticket"])
    # The old (now obsolete) ticket is retired and a fresh preview ticket minted.
    await asyncio.to_thread(_consume_ticket, bot, ticket)
    new_ticket = await asyncio.to_thread(_issue_ticket, bot, message, actor, conversation, result)
    preview = build_preview_card(result, new_ticket, just_saved=True)
    return _reply_text("已保存修改（未执行）。请在卡片预览中核对后确认。",
                       card=preview, status="saved", plan=result, ticket=new_ticket)


async def _handle_confirm(bot, message, request, actor, conversation, plan, ticket):
    status = plan.get("status")
    if status == "awaiting_second_confirmation":
        stage = "execute"
    elif status == "awaiting_confirmation":
        stage = "review"
    else:
        return _reply_text("当前状态不允许确认，请重新打开最新计划。", error="bad_status")
    try:
        result = await bot._call(request, actor, "plans/" + plan["id"] + "/confirm",
                                 {"version": plan["version"], "stage": stage},
                                 plan_id=plan["id"], method="POST")
    except AssistantError as exc:
        return _reply_text(safe_text(str(exc)), error=safe_text(str(exc)))
    except Exception as exc:  # pragma: no cover - defensive
        log.warning("card confirm failed: %s", type(exc).__name__)
        return _reply_text("确认未完成，请稍后重试。", error="confirm_failed")
    if not (result or {}).get("results") and (result or {}).get("status") == "awaiting_second_confirmation":
        await asyncio.to_thread(_consume_ticket, bot, ticket)
        new_ticket = await asyncio.to_thread(_issue_ticket, bot, message, actor, conversation, result)
        preview = build_preview_card(result, new_ticket, just_saved=False)
        return _reply_text("此操作风险较高，已进入再次确认。请核对后在卡片中点击【再次确认执行】。",
                           card=preview, status="promoted", plan=result, ticket=new_ticket)
    final_status = (result or {}).get("status")
    await asyncio.to_thread(_consume_ticket, bot, ticket)
    # The kernel returns the plan to needs_input when notice photos are missing;
    # render a fresh form with explicit photo guidance (request #1 / #3) instead
    # of always answering "已确认并交由原业务流程执行".
    if final_status == "needs_input" or (result or {}).get("missing_fields"):
        editable = await prepare_card(bot, message, actor, conversation, result or plan)
        extra = _photo_guidance_text(result or plan)
        text = ("确认未完成：内核已返回待补充填写（可能缺少现场照片），你的填写已保留。"
                + (("\n" + extra) if extra else "")
                + "\n请在卡片中补充后重新点【保存】并确认，系统不会重复提交业务。")
        return _reply_text(text, card=editable["card"], status="needs_more",
                           plan=result, ticket=editable["ticket"])
    if final_status in {"running", "submitted"}:
        await bot.schedule_plan_status(message, result, conversation, (actor or {}).get("id"))
        text = ("已确认并提交原业务流程，正在原流程中处理。结果将稍后自动推送，"
                "也可发送“操作状态”查询，不会重复提交。")
        if (result or {}).get("error"):
            text = "已确认并交由原业务流程执行。原流程提示：" + safe_text(str(result.get("error")), limit=300)
        return _reply_text(text, card=None, status="confirmed", plan=result)
    if final_status == "completed":
        return _reply_text("操作已确认并完成。", card=None, status="completed", plan=result)
    if final_status == "failed":
        return _reply_text("已确认，但原流程执行失败：" + safe_text(str((result or {}).get("error") or "请查看原任务"), limit=300),
                           card=None, status="failed", plan=result)
    return _reply_text((result or {}).get("error") or "已确认并交由原业务流程执行。",
                       card=None, status=final_status or "confirmed", plan=result)


async def _handle_edit(bot, message, request, actor, conversation, plan, ticket):
    if plan.get("status") not in {"awaiting_confirmation", "awaiting_second_confirmation"}:
        return _reply_text("当前不可返回编辑，请重新打开。", error="bad_status")
    try:
        result = await bot._call(request, actor, "plans/" + plan["id"],
                                 {"version": plan["version"], "action": "edit"},
                                 method="PATCH")
    except AssistantError as exc:
        return _reply_text(safe_text(str(exc)), error=safe_text(str(exc)))
    except Exception as exc:  # pragma: no cover - defensive
        log.warning("card edit failed: %s", type(exc).__name__)
        return _reply_text("返回编辑未完成，请稍后重试。", error="edit_failed")
    await asyncio.to_thread(_consume_ticket, bot, ticket)
    editable = await prepare_card(bot, message, actor, conversation, result or plan)
    return _reply_text("已返回可编辑状态，请在卡片中修改后保存（未执行）。",
                       card=editable["card"], status="editable", plan=result,
                       ticket=editable["ticket"])


async def _handle_cancel(bot, message, request, actor, conversation, plan, ticket):
    status = plan.get("status")
    if status not in {"needs_input", "awaiting_confirmation", "awaiting_second_confirmation"}:
        return _reply_text("当前状态不可取消，请重新打开。", error="bad_status")
    try:
        await bot._call(request, actor, "plans/" + plan["id"] + "/cancel", {},
                        method="POST")
    except AssistantError as exc:
        return _reply_text(safe_text(str(exc)), error=safe_text(str(exc)))
    except Exception as exc:  # pragma: no cover - defensive
        log.warning("card cancel failed: %s", type(exc).__name__)
        return _reply_text("取消未完成，请稍后重试。", error="cancel_failed")
    await asyncio.to_thread(_consume_ticket, bot, ticket)
    return _reply_text("已取消该待补充/待确认操作，未提交新的业务。",
                       card=None, status="cancelled", plan=plan)


async def _handle_adopt(bot, message, request, actor, conversation, plan, ticket):
    card = message.get("card") or {}
    value = card.get("value") or {}
    rec_index = value.get("rec_index")
    recommendations = ((plan.get("planned_notice") or {}).get("recommendations")) or []
    try:
        idx = int(rec_index)
    except (TypeError, ValueError):
        return _reply_text("推荐参数无效。", error="bad_rec_index")
    if not (0 <= idx < len(recommendations)):
        return _reply_text("推荐不存在或已更新，请重新打开。", error="bad_rec_index")
    suggested = recommendations[idx].get("suggested_values") or {}
    base = _merge_form_values(plan, card.get("form_value") or {})
    modified = _apply_recommendation(base, suggested)
    await asyncio.to_thread(_consume_ticket, bot, ticket)
    editable = await prepare_card(bot, message, actor, conversation, modified)
    return _reply_text("已按推荐预填当前空白可编辑字段，并保留你尚未保存的填写（未提交）。",
                       card=editable["card"], status="adopted", plan=modified,
                       ticket=editable["ticket"])


async def _handle_load_directory(bot, message, request, actor, conversation, plan, ticket):
    card = message.get("card") or {}
    value = card.get("value") or {}
    field_name = str(value.get("field") or "")
    if not field_name:
        return _reply_text("读取参数缺失。", error="bad_field")
    draft = _merge_form_values(plan, card.get("form_value") or {})
    if field_name not in {f.get("name") for f in plan.get("fields") or [] if not _field_protected_self(f)}:
        return _reply_text("此字段不可读取。", error="bad_field")
    try:
        result = await bot._call(request, actor, "plans/" + plan["id"] + "/options",
                                 params={"field": field_name})
    except AssistantError as exc:
        return _reply_text(safe_text(str(exc)), error=safe_text(str(exc)))
    except Exception as exc:  # pragma: no cover - defensive
        log.warning("card options failed: %s", type(exc).__name__)
        return _reply_text("读取目录/选项失败，请稍后重试。", error="options_failed")
    updated = next((item for item in (result or {}).get("fields") or []
                    if str(item.get("name")) == field_name), None)
    if updated is None:
        return _reply_text("未获取到目录/选项，请重试。", error="no_directory")
    # The options API returns the complete new plan (version incremented); use it
    # as the base so the freshly minted ticket matches the next step's plan, then
    # re-apply the user's just-filled (but not yet saved) values.
    base = result if (result or {}).get("version") is not None else plan
    draft_values = {f["name"]: f.get("value") for f in draft.get("fields") or []}
    base = copy.deepcopy(base)
    for f in base.get("fields") or []:
        if f.get("name") in draft_values:
            f["value"] = draft_values[f["name"]]
    modified = _merge_directory(base, updated)
    await asyncio.to_thread(_consume_ticket, bot, ticket)
    editable = await prepare_card(bot, message, actor, conversation, modified)
    return _reply_text("已读取目录/选项，可在卡片中选择（保留了尚未保存的填写）。",
                       card=editable["card"], status="loaded", plan=modified,
                       ticket=editable["ticket"])


async def _handle_array(bot, message, actor, conversation, plan, ticket, action):
    card = message.get('card') or {}
    modified = _merge_form_values(plan, card.get('form_value') or {})
    modified = copy.deepcopy(modified)
    key = (card.get('value') or {}).get('array')
    for field in modified.get('fields') or []:
        if notice_forms.is_native_notice(field) or notice_forms.is_native_notice_sop(field):
            continue
        for path, control, rows in _generic_arrays(field, field.get('value')):
            if _generic_id(field, path) != key:
                continue
            if action == 'add-row' and len(rows) < control.get('maxItems', 2000):
                item = control['item']
                rows = [*rows, {} if item.get('type') == 'object' or item.get('person_picker') else [] if item.get('type') == 'array' else '']
            elif action == 'remove-row' and len(rows) > control.get('minItems', 0):
                rows = rows[:-1]
            else:
                return _reply_text('明细数量已达到业务允许的范围。', error='array_limit')
            field['value'] = _set_nested(field.get('value') or {}, path, rows) if path else rows
            prepared = await prepare_card(bot, message, actor, conversation, modified)
            await asyncio.to_thread(_consume_ticket, bot, ticket)
            return _reply_text('已更新表单明细，尚未提交业务。', card=prepared['card'], ticket=prepared['ticket'], plan=modified)
    return _reply_text('此明细不可修改，请使用最新表单。', error='bad_array')


async def handle_card(bot, message, request, actor, conversation):
    """Process a card callback (save / confirm / edit / cancel / adopt / load-directory)."""
    card = message.get("card") or {}
    value = card.get("value") or {}
    kind = str(value.get("kind") or card.get("kind") or "")
    action = str(value.get("action") or card.get("action") or "")
    ticket = str(value.get("ticket") or card.get("ticket") or "")
    if kind not in _ALLOWED_KINDS or not ticket or not action or action not in _ALLOWED_ACTIONS:
        return _reply_text("卡片回调数据不完整或不支持，未执行任何业务操作。", error="bad_payload")
    tick, plan, error = await asyncio.to_thread(_validate_ticket, bot, ticket, message, actor, conversation)
    if error:
        return _reply_text(error, error=error)
    if action in {"save", "adopt", "load-directory", "add-row", "remove-row"}:
        if plan.get("status") != "needs_input":
            return _reply_text("请先返回编辑后再修改。", error="bad_status")
        submitted = card.get("form_value") or {}
        buttons = {"save", *(b["name"] for b in _load_buttons(plan, ticket)),
                   *(b["name"] for b in _array_buttons(plan, ticket)),
                   *(b["name"] for b in _recommendation_blocks(plan, ticket) if b.get("tag") == "button")}
        card = {**card, "form_value": {key: val for key, val in submitted.items() if key not in buttons}}
        message = {**message, "card": card}
        try:
            _build_values(plan.get("fields") or [], card["form_value"], partial=True)
        except AssistantError as exc:
            return _reply_text(str(exc), error=str(exc))
    # Dependency / action mapping.
    if action == "save":
        # Only the form submit button uses kind=notice_form.
        if kind != "notice_form":
            return _reply_text("保存操作参数无效。", error="bad_payload")
        return await _handle_save(bot, message, request, actor, conversation, plan, ticket)
    if action == "confirm":
        return await _handle_confirm(bot, message, request, actor, conversation, plan, ticket)
    if action == "edit":
        return await _handle_edit(bot, message, request, actor, conversation, plan, ticket)
    if action == "cancel":
        return await _handle_cancel(bot, message, request, actor, conversation, plan, ticket)
    if action == "adopt":
        return await _handle_adopt(bot, message, request, actor, conversation, plan, ticket)
    if action == "load-directory":
        return await _handle_load_directory(bot, message, request, actor, conversation, plan, ticket)
    if action in {'add-row', 'remove-row'}:
        return await _handle_array(bot, message, actor, conversation, plan, ticket, action)
    return _reply_text("不支持的卡片操作。", error="unknown_action")


# ---------------------------------------------------------------------------
# Event -> inbox message mapping (defect #5)
# ---------------------------------------------------------------------------

def parse_card_event(payload, app_id):
    """Turn a ``card.action.trigger`` payload into a durable inbox message.

    The synthetic ``message_id`` includes the operator union and the original
    ``event_id`` so repeated identical clicks dedup while genuine re-edits flow.
    The real card ``open_message_id`` is preserved as ``reply_to_message_id``.
    """
    if not isinstance(payload, dict):
        return None
    header, event = payload.get("header") or {}, payload.get("event") or {}
    if not isinstance(header, dict) or not isinstance(event, dict):
        return None
    if header.get("app_id") != app_id or header.get("event_type") != "card.action.trigger":
        return None
    event_id = str(header.get("event_id") or "")
    operator = event.get("operator") or {}
    context = event.get("context") or {}
    action = event.get("action") or {}
    if not all(isinstance(part, dict) for part in (operator, context, action)):
        return None
    open_id = str(operator.get("open_id") or "")
    union_id = str(operator.get("union_id") or "")
    chat_id = str(context.get("open_chat_id") or "")
    open_message_id = str(context.get("open_message_id") or "")
    if not (open_id and union_id and event_id) or len(event_id) > 128:
        return None
    if not all(re.fullmatch(pat, val) for val, pat in (
            (open_id, r"ou_[A-Za-z0-9]+"), (union_id, r"on_[A-Za-z0-9]+"), (chat_id, r"oc_[A-Za-z0-9]+"),
            (open_message_id, r"om_[A-Za-z0-9]+"))):
        return None
    value = action.get("value") or {}
    if not isinstance(value, dict):
        return None
    kind = str(value.get("kind") or "")
    ticket = str(value.get("ticket") or "")
    card_action = str(value.get("action") or "")
    if kind not in _ALLOWED_KINDS or not re.fullmatch(r"[a-f0-9]{12}_[A-Za-z0-9_-]{22}", ticket) or card_action not in _ALLOWED_ACTIONS:
        return None
    form_value = action.get("form_value") or {}
    if form_value is None:
        form_value = {}
    if not isinstance(form_value, dict) or len(form_value) > 500:
        return None
    try:
        encoded = json.dumps(form_value, ensure_ascii=False, sort_keys=True).encode("utf-8")
        if len(encoded) > 100_000 or any(not isinstance(k, str) or len(k) > 120 for k in form_value):
            return None
        if any(not isinstance(v, (str, bool, int, float, list, type(None))) or isinstance(v, list) and any(not isinstance(item, str) for item in v) for v in form_value.values()):
            return None
        payload_digest = hashlib.sha256(
            encoded).hexdigest()[:16]
    except (TypeError, ValueError):  # pragma: no cover - defensive
        return None
    synthetic = "crd_" + hashlib.sha256((
        event_id + ":" + union_id + ":" + open_message_id + ":"
        + str(action.get("name") or "") + ":" + ticket + ":" + card_action + ":" + payload_digest
    ).encode("utf-8")).hexdigest()[:32]
    return {
        "message_id": synthetic,
        "reply_to_message_id": open_message_id,
        "chat_id": chat_id,
        "chat_type": "group",
        "sender_id": open_id,
        "union_id": union_id,
        "problem": "",
        "question": "[卡片操作] " + safe_text(str(action.get("tag") or "按钮"), limit=40),
        "kind": "card_action",
        "card": {
            "kind": kind,
            "action": card_action,
            "value": value,
            "form_value": form_value,
            "name": str(action.get("name") or ""),
            "open_message_id": open_message_id,
            "host": str(event.get("host") or "im_message"),
            "timezone": str(action.get("timezone") or ""),
            "event_id": event_id,
        },
    }
