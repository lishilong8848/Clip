# -*- coding: utf-8 -*-
"""Assistant notice identity checks and native binding form adaptation."""
from __future__ import annotations
import copy
import datetime as dt
import re
from clipflow_backend.api_models import NoticeIdentityBindRequest
from .lighthouse_ai import AssistantError
from lan_bitable_template_portal.workbench_lite import (
    NOTICE_TYPE_BY_WORK_TYPE,
    WORK_TYPE_LABELS,
    ITEM_WORK_TYPE_LABELS,
    WORK_TYPE_BY_NOTICE_TYPE,
    _draft_from_record,
    _item_work_type,
)
from lan_bitable_template_portal.identity_utils import (
    canonical_source_record_id,
    canonical_target_record_id,
    is_local_record_id,
)
from .lighthouse_sources import SCOPES, codes, record_codes, record_title
from lan_bitable_template_portal.portal_service import MaintenancePortalService

BIND_API = "POST /api/notice-identity/bind"
_BINDING_CONTEXTS = {"planned", "ongoing"}
_SRC = "notice_identity_sources"
_TGT = "notice_identity_targets"
_NATIVE = set(NoticeIdentityBindRequest.model_fields)
_ALL = set(SCOPES)
_CAMPUS = set("ABCDE")
DELETE_IDENTITY_FIELDS = ("scope", "work_type", "notice_type", "active_item_id", "target_record_id", "source_record_id", "record_id", "title")


def current_notice_body(actor, body, queries):
    """Resolve an existing notice only from the current ongoing list."""
    work_type = _id(body.get("work_type") or "maintenance")
    if work_type not in WORK_TYPE_LABELS or body.get("notice_type") == "事件通告":
        raise AssistantError("助手不办理事件通告，请使用原入口。")
    allowed = _scope(actor, body.get("scope"))
    requested = {key: _id(body.get(key)) for key in ("active_item_id", "target_record_id", "record_id", "source_record_id") if _id(body.get(key))}
    if not any(requested.get(key) for key in ("active_item_id", "target_record_id", "record_id")):
        raise AssistantError("办理前请先查询当前未结束通告并选择真实记录，不能仅凭名称处理。")
    latest, seen = [], set()
    for rows in reversed(_query_row_groups(queries, False)):
        group_keys, group_rows = set(), set()
        for row in rows:
            identity = _ident(row)
            key = (identity["active_item_id"] or identity["target_record_id"] or identity["record_id"], _item_work_type(row))
            signature = (*identity.values(), key[1], _id(record_title(row)), _status(row), _dead(row), _finished(row), _inactive(row), tuple(sorted(record_codes(row))))
            if key[0] and key not in seen and signature not in group_rows:
                latest.append(row)
                group_rows.add(signature)
            group_keys.add(key)
        seen.update(group_keys)
    candidates = []
    for row in latest:
        identity = _ident(row)
        if any(value in ({identity["record_id"], identity["target_record_id"]} if key == "record_id" else {identity.get(key)}) for key, value in requested.items() if key != "source_record_id"):
            candidates.append(row)
    if not candidates:
        raise AssistantError("该通告不在本轮查询的未结束记录中，请重新查询；不能沿用历史或同名通告的记录ID。")
    if len(candidates) != 1:
        raise AssistantError("通告目标存在多条匹配或记录ID冲突，请重新选择唯一通告。")
    row = candidates[0]
    identity = _ident(row)
    if any(value not in ({identity["record_id"], identity["target_record_id"]} if key == "record_id" else {identity.get(key)}) for key, value in requested.items()):
        raise AssistantError("通告的条目ID、源记录和目标记录不一致，请重新查询，未更换目标。")
    if _item_work_type(row) != work_type or _dead(row) or _finished(row) or (identity["target_record_id"] and _inactive(row)):
        raise AssistantError("该通告已删除、已结束或类型已变化，助手不再办理，请重新查询。")
    record_s = set(record_codes(row))
    if not record_s or not record_s <= allowed:
        raise AssistantError("通告的楼栋无法核实或超出当前权限。", 403)
    title = _id(record_title(row))
    if any(_id(body.get(key)) and _id(body[key]) != title for key in ("title", "name")):
        raise AssistantError("目标名称与当前通告不一致，请重新查询，未执行操作。")
    if body.get("notice_type") and body["notice_type"] != (row.get("notice_type") or NOTICE_TYPE_BY_WORK_TYPE.get(work_type)):
        raise AssistantError("目标通告类型不一致。")
    return {
        "scope": _eff(actor["scopes"], record_s), "work_type": work_type,
        "notice_type": row.get("notice_type") or NOTICE_TYPE_BY_WORK_TYPE[work_type],
        **identity, "record_id": identity["target_record_id"] or identity["record_id"],
        "title": title, "building": _id(row.get("building")),
        "operation_id": _id(body.get("operation_id")),
        "expected_record_version": _id(body.get("expected_record_version")),
    }


def deletion_body(actor, body, queries):
    return current_notice_body(actor, body, queries)


def current_undo_target(actor, undo_id, scope, queries):
    rows = []
    for group in _query_row_groups(queries, False):
        for row in group:
            if row.get("undo_id") == undo_id and row not in rows:
                rows.append(row)
    if not undo_id or len(rows) != 1 or rows[0].get("undo_action_type") in {"end", "delete"}:
        raise AssistantError("该撤销操作不属于当前未结束通告，已结束或已删除的通告不在助手中办理。")
    row = rows[0]
    return current_notice_body(actor, {**_ident(row), "scope": scope, "work_type": _item_work_type(row)}, {"current": {"ongoing": rows}})


def check_deletion_anchor(body, anchor):
    if not isinstance(anchor, dict) or any(_id(body.get(key)) != _id(anchor.get(key)) for key in DELETE_IDENTITY_FIELDS):
        raise AssistantError("操作清单没有有效的当前通告核对结果，或目标已变化；请重新查询并核对，未执行旧清单。")


def _id(v):
    return str(v or "").strip()


def _fin(s):
    return MaintenancePortalService._target_status_is_finished(s)


def _dead(r):
    status = _id(r.get("target_record_status") or r.get("status")).lower()
    return bool(r.get("deleted") or r.get("is_deleted") or r.get("deleted_at")
                or status in {"deleted", "cancelled", "archived"}
                or any(word in status for word in ("删除", "作废", "已取消")))


def _status(r, *, source=False):
    value = _id((r.get("source_status") or r.get("status")) if source else (r.get("target_record_status") or r.get("status") or r.get("source_status")))
    if source and not value:
        value = _id(r.get("progress"))
    return value


def _finished(r, *, source=False):
    return r.get("target_finished") is True or _fin(_status(r, source=source))


def _inactive(r):
    return r.get("target_active") is False


def _ident(r):
    return {
        "active_item_id": _id(r.get("active_item_id")),
        "source_record_id": canonical_source_record_id(r),
        "target_record_id": canonical_target_record_id(r),
        "record_id": _id(r.get("record_id")),
    }


def _month(v, now=None):
    text = _id(v)
    if not text:
        now = now or dt.datetime.now(dt.timezone(dt.timedelta(hours=8)))
        return "{}月".format(now.month)
    if re.fullmatch(r"(?:0?[1-9]|1[0-2])月", text):
        return "{}月".format(int(text.rstrip("月")))
    if re.fullmatch(r"\d{4}-\d{2}", text):
        month = int(text.split("-")[1])
        if 1 <= month <= 12:
            return "{}月".format(month)
    raise AssistantError("来源月份格式无效。")


def _bool(v):
    if isinstance(v, bool):
        return v
    if v in (None, ""):
        return False
    raise AssistantError("source_binding_only 必须是布尔值。")


def _scope(actor, requested):
    actor_s = {str(s).upper() for s in (actor.get("scopes") or [])}
    if not actor_s:
        raise AssistantError("无楼栋权限，不能绑定通告。", 403)
    text = _id(requested)
    if text.upper() in {"ALL", "全部楼栋", "全楼"}:
        return set(actor_s)
    req = set(codes(text))
    if not req:
        raise AssistantError("请明确楼栋范围。")
    if not req <= actor_s:
        raise AssistantError("通告绑定超出当前楼栋权限。", 403)
    return req


def _eff(actor_s, record_s):
    actor_s = {str(s).upper() for s in actor_s}
    record_s = {str(s).upper() for s in (record_s or [])}
    if not record_s:
        raise AssistantError("通告缺少楼栋范围，不能绑定。")
    if len(record_s) == 1:
        return next(iter(record_s))
    if record_s <= _CAMPUS and _CAMPUS <= actor_s:
        return "CAMPUS"
    if record_s <= _ALL and actor_s == _ALL:
        return "ALL"
    raise AssistantError("通告涉及多楼栋且原接口无对应别名，请按单栋操作。")


def _query_row_groups(queries, planned):
    """Collect row groups per query, newest snapshot last (insertion order)."""
    if not isinstance(queries, dict):
        return []
    names = ("records", "items", "pending") if planned else ("ongoing",)
    groups = []
    for data in queries.values():
        rows = []
        if not isinstance(data, dict):
            groups.append(rows)
            continue

        def extend(container):
            if isinstance(container, list):
                rows.extend(row for row in container if isinstance(row, dict))

        for name in names:
            extend(data.get(name))
        for building in data.get("buildings") or []:
            if isinstance(building, dict) and isinstance(building.get("data"), dict):
                for name in names:
                    extend(building["data"].get(name))
        groups.append(rows)
    return groups


def _resolve(body, work_type, queries, allowed, actor_s, planned, source_only):
    tag = "计划" if planned else "进行中"
    groups = _query_row_groups(queries, planned)
    active_id = ""
    target_id = ""
    if planned:
        needle = _id(body.get("source_record_id")) or _id(body.get("record_id"))
        if not needle:
            raise AssistantError("计划通告绑定缺少已读取的源记录。")

        def key(row):
            return (canonical_source_record_id(row), _id(row.get("record_id")))

        def matches(row):
            item = _ident(row)
            return needle in {item["source_record_id"], item["record_id"]}
    else:
        active_id = _id(body.get("active_item_id"))
        raw_target = _id(body.get("target_record_id"))
        if source_only and raw_target and is_local_record_id(raw_target):
            raise AssistantError("源表绑定需要原通告已关联真实目标记录。")
        target_id = _id(canonical_target_record_id(body))
        if not target_id:
            record_id = _id(body.get("record_id"))
            if record_id and not is_local_record_id(record_id):
                target_id = record_id
        if not active_id and not target_id:
            raise AssistantError("进行中通告绑定缺少已读取的进行中条目。")

        def key(row):
            item = _ident(row)
            return (item["active_item_id"], item["source_record_id"])

        def matches(row):
            item = _ident(row)
            if active_id and item["active_item_id"] == active_id:
                return True
            if target_id and item["target_record_id"] == target_id:
                return True
            return False

    seen_sources = set()
    unique = []
    for group in reversed(groups):
        seen_full = set()
        kept = []
        for row in group:
            full = tuple(_ident(row).values())
            if full in seen_full:
                continue
            seen_full.add(full)
            candidate_key = key(row)
            if any(candidate_key) and candidate_key in seen_sources:
                continue
            kept.append(row)
        for row in kept:
            candidate_key = key(row)
            if any(candidate_key):
                seen_sources.add(candidate_key)
            unique.append(row)

    matched = [row for row in unique if matches(row)]
    if active_id:
        active_rows = [row for row in matched if _ident(row)["active_item_id"] == active_id]
        if not active_rows:
            raise AssistantError("原进行中通告已变化，请重新读取。")
        if len(active_rows) == 1:
            # The active item is authoritative; a conflicting target in the
            # request body must not create a second ambiguous match.
            matched = active_rows
        elif len(active_rows) > 1:
            raise AssistantError(tag + "通告存在多条进行中匹配，无法确定唯一记录。")
    if not matched:
        raise AssistantError(tag + "通告未找到对应条目，请先读取。")
    if len(matched) > 1:
        raise AssistantError(tag + "通告存在多条匹配，无法确定唯一记录。")
    row = matched[0]
    if _source_work_mismatch(row, work_type):
        raise AssistantError(tag + "通告工作类型与原通告不一致。")
    if _dead(row):
        raise AssistantError(tag + "通告已删除，请重新读取。")
    if _finished(row, source=planned) or not planned and _inactive(row):
        raise AssistantError(tag + "通告已结束，不能绑定。")
    if source_only:
        real_target = _ident(row)["target_record_id"]
        if not real_target or is_local_record_id(real_target):
            raise AssistantError("源表绑定需要原通告已关联真实目标记录。")
        if target_id and not is_local_record_id(target_id) and target_id != real_target:
            raise AssistantError("源表绑定目标与原通告不一致，请重新读取。")
    record_s = set(record_codes(row))
    if not record_s:
        raise AssistantError("通告缺少楼栋范围，不能绑定。")
    if not record_s <= allowed or not record_s <= set(actor_s):
        raise AssistantError("通告不在当前楼栋权限内。", 403)
    return row, record_s


def _source_work_mismatch(row, work_type):
    """Only an explicit work_type/notice_type mismatch rejects a source row."""
    notice_type = _id(row.get("notice_type"))
    mapped = WORK_TYPE_BY_NOTICE_TYPE.get(notice_type)
    if mapped:
        return mapped != work_type
    raw = _id(row.get("work_type") or row.get("lan_work_type"))
    if raw in ITEM_WORK_TYPE_LABELS:
        return raw != work_type
    return False


def _cand_id(row, *, source):
    value = canonical_source_record_id(row) if source else canonical_target_record_id(row)
    return value or _id(row.get("record_id"))


def _cand_label(row, *, work_type, source=False):
    building = _id(row.get("building") or row.get("building_name") or row.get("scope") or row.get("楼栋"))
    status = _status(row, source=source)
    start = _id(row.get("start_time") or row.get("started_at"))
    end = _id(row.get("end_time") or row.get("ended_at"))
    span = "{} ~ {}".format(start or "…", end or "…") if (start or end) else ""
    finished = _finished(row, source=source)
    parts = [part for part in (building, "已结束" if finished else status, span) if part]
    label = _id(record_title(row)) or WORK_TYPE_LABELS.get(work_type) or work_type
    if label in set(_ident(row).values()):
        label = "未命名" + WORK_TYPE_LABELS.get(work_type, "") + "通告"
    return " · ".join([label, *parts]) if parts else label


def _canonical_body(anchor):
    """One canonical native snapshot shared by both field and apply builders."""
    original = anchor.get("record") or {}
    identity = anchor.get("identity") or {}
    work_type = _id(anchor.get("work_type") or "maintenance")
    source_only = bool(anchor.get("source_binding_only"))
    draft = _draft_from_record(original, work_type=work_type)

    def pick(key):
        value = _id(original.get(key))
        return value or _id(draft.get(key))

    source_record_id = _id(identity.get("source_record_id"))
    if not source_record_id and anchor.get("binding_context") == "planned":
        source_record_id = _id(identity.get("record_id"))

    return {
        "scope": _id(anchor.get("scope") or "ALL"),
        "binding_context": _id(anchor.get("binding_context") or ""),
        "work_type": work_type,
        "notice_type": _id(original.get("notice_type") or draft.get("notice_type") or NOTICE_TYPE_BY_WORK_TYPE.get(work_type)),
        "source_binding_only": source_only,
        "active_item_id": _id(identity.get("active_item_id")),
        "source_record_id": source_record_id,
        "target_record_id": _id(identity.get("target_record_id")),
        "record_id": _id(identity.get("record_id")),
        "title": pick("title"),
        "reason": pick("reason"),
        "start_time": pick("start_time"),
        "end_time": pick("end_time"),
    }


def _fresh(anchor, selected, sel_row, src_month):
    body = _canonical_body(anchor)
    source_only = body["source_binding_only"]
    if source_only:
        body["source_record_id"] = selected
        month = _id(src_month) or _id(anchor.get("month"))
        if month:
            body["source_month"] = _month(month)
    else:
        body["target_record_id"] = selected
        body["record_id"] = selected
        if not body["active_item_id"]:
            active = _id(sel_row.get("active_item_id"))
            if active and not is_local_record_id(active):
                body["active_item_id"] = active
        status = _status(sel_row, source=False)
        if status:
            body["status"] = status
    return body


def identity_choices(field, rows, actor):
    if not isinstance(rows, list):
        raise AssistantError("通告候选列表格式无效。")
    if field.get("options_source") not in {_SRC, _TGT}:
        raise AssistantError("选项不属于通告身份绑定。")
    anchor = field.get("_anchor") or {}
    work_type = _id(field.get("work_type") or anchor.get("work_type") or "maintenance")
    source = field.get("options_source") == _SRC
    actor_s = {str(s).upper() for s in (actor.get("scopes") or [])}
    selection = _id(field.get("scope") or anchor.get("scope") or "")
    all_sel = selection.upper() in {"ALL", "全部楼栋", "全楼"}
    anchor_scopes = {str(s).upper() for s in (anchor.get("scopes") or [])}
    if anchor_scopes:
        sel_codes = anchor_scopes
    else:
        sel_codes = set(actor_s) if all_sel else set(codes(selection))
    admin = bool(actor.get("is_admin")) and actor_s == _ALL
    options = []
    records = {}
    for row in rows:
        if not isinstance(row, dict):
            raise AssistantError("通告候选列表包含无效记录。")
        if _dead(row):
            continue
        cid = _cand_id(row, source=source)
        if not cid:
            raise AssistantError("通告候选缺少记录ID。")
        if is_local_record_id(cid) or cid in records:
            continue
        if source:
            if _source_work_mismatch(row, work_type):
                continue
        elif _item_work_type(row) != work_type:
            continue
        row_s = set(record_codes(row))
        if row_s:
            if not row_s <= sel_codes or not row_s <= actor_s:
                continue
        elif not (admin and all_sel and sel_codes == _ALL):
            continue
        if _finished(row, source=source) or not source and _inactive(row):
            continue
        options.append({"value": cid, "label": _cand_label(row, work_type=work_type, source=source)})
        records[cid] = row
    return options, records


def apply_identity_choice(field, operation, actor, selected):
    if not _id(selected):
        raise AssistantError("请选择已读取的候选记录。")
    loaded = field.get("_records") or {}
    if selected not in loaded:
        raise AssistantError("请从已读取的候选记录中选择，不能直接填写记录 ID。")
    anchor = field.get("_anchor") or {}
    if not anchor:
        raise AssistantError("通告绑定锚点丢失，请重新读取。")
    sel_row = loaded[selected]
    options, _ = identity_choices(field, [sel_row], actor)
    label = next((option["label"] for option in options if option["value"] == selected), "")
    if not label:
        raise AssistantError("所选候选记录已失效，请重新读取。")
    body = operation.get("body")
    if not isinstance(body, dict):
        raise AssistantError("通告绑定请求体无效。")
    src_month = _id(body.get("source_month"))
    body.clear()
    body.update(_fresh(anchor, selected, sel_row, src_month))
    original = anchor.get("record") or {}
    return {field["path"]: label, "original_notice": _id(anchor.get("title") or record_title(original))}


def binding_fields(actor, operation, queries, index):
    body = operation.get("body")
    if not isinstance(body, dict):
        raise AssistantError("通告绑定请求体无效。")
    unknown = set(body) - _NATIVE
    if unknown:
        raise AssistantError("通告绑定请求包含不支持的字段：{}".format(", ".join(sorted(unknown))))
    requested = copy.deepcopy(body)
    allowed = _scope(actor, requested.get("scope"))
    actor_s = {str(s).upper() for s in (actor.get("scopes") or [])}
    work_type = _id(requested.get("work_type") or "maintenance")
    if work_type not in set(WORK_TYPE_LABELS):
        raise AssistantError("不支持的通告工作类型：" + work_type)
    context = _id(requested.get("binding_context") or "").lower()
    if context not in _BINDING_CONTEXTS:
        raise AssistantError("请明确通告绑定上下文（planned/ongoing）。")
    source_only = _bool(requested.get("source_binding_only"))
    if source_only and context != "ongoing":
        raise AssistantError("源表绑定仅支持进行中上下文。")
    month = _month(requested.get("source_month")) if source_only else None
    original, record_s = _resolve(requested, work_type, queries, allowed, actor_s, context == "planned", source_only)
    identity = _ident(original)
    effective = _eff(actor_s, record_s)
    anchor = {
        "work_type": work_type,
        "binding_context": context,
        "source_binding_only": source_only,
        "scope": effective,
        "scopes": sorted(record_s),
        "month": month,
        "record": copy.deepcopy(original),
        "identity": copy.deepcopy(identity),
    }
    fresh = _canonical_body(anchor)
    anchor["title"] = fresh["title"]
    if source_only:
        fresh["source_record_id"] = ""
        fresh["source_month"] = month
    body.clear()
    body.update(fresh)
    path = "source_record_id" if source_only else "target_record_id"
    selector = {
        "name": "step{}.{}".format(index, path),
        "path": path,
        "label": "选择来源通告" if source_only else "选择目标通告",
        "type": "select",
        "options_source": _SRC if source_only else _TGT,
        "native_notice_identity": True,
        "source_binding_only": source_only,
        "work_type": work_type,
        "binding_context": context,
        "scope": effective,
        "_anchor": anchor,
        "_records": {},
        "options": [],
        "operation_index": index,
        "section": "body",
        "value": "",
        "required": True,
        "question_text": "当前通告：" + (anchor["title"] or "未命名通告"),
    }
    fields = [selector]
    if source_only:
        fields.insert(0, {
            "name": "step{}.source_month".format(index),
            "path": "source_month",
            "label": "来源月份",
            "type": "select",
            "native_notice_identity": True,
            "source_binding_only": True,
            "work_type": work_type,
            "binding_context": context,
            "scope": effective,
            "options": [{"value": "{}月".format(month), "label": "{}月".format(month)} for month in range(1, 13)],
            "operation_index": index,
            "section": "body",
            "value": month,
            "_records": {},
            "required": True,
        })
    return fields
