"""Read-only planned notice selection; sending stays in the native workbench."""
from __future__ import annotations

import copy
from contextvars import ContextVar
import datetime as dt
import hashlib
import json

deferred_memory: ContextVar[list | None] = ContextVar("planned_notice_memory", default=None)
TYPES = ("maintenance", "change", "repair", "polling", "adjust", "power")
STABLE = {
    "maintenance": {"location", "content", "reason", "impact", "specialty", "execution_party", "maintenance_cycle"},
    "change": {"location", "content", "reason", "impact", "specialty", "execution_party", "level"},
    "repair": {"location", "content", "reason", "impact", "specialty", "repair_mode", "solution"},
    "polling": {"content", "impact", "specialty"},
    "adjust": {"location", "content", "reason", "impact", "specialty"},
    "power": {"specialty"},
}


def _error(message):
    from .portal_service import PortalError
    raise PortalError(message)


def matches_month(service, record, month):
    if service._record_work_type(record) == "maintenance":
        return service._source_record_matches_month_window(record, month)
    fields = record.get("display_fields") or {}
    explicit = fields.get("计划维护月份") or fields.get("计划月份") or fields.get("月份")
    if explicit:
        text = str(explicit).strip()
        now = dt.datetime.now(dt.timezone(dt.timedelta(hours=8)))
        return text in {month, str(now.month), f"{now.month:02}", f"{now.year}-{now.month:02}", f"{now.year}年{now.month}月"}
    return service._source_record_matches_month_window(record, month)


def _ready(service, scope, types):
    service.ensure_snapshot_loaded()
    snapshot = service._state_store.get_source_scope_snapshot(scope)
    if not snapshot.get("exists"):
        _error("计划列表尚未加载完整，请先在通告页面完成加载后重试。")
    statuses = (snapshot.get("meta") or {}).get("source_refresh_status") or service._source_refresh_status
    for kind in {kind if kind in {"change", "repair"} else "maintenance" for kind in types}:
        state = statuses.get(kind) or {}
        if state.get("status") in {"failed", "retained", "loading", "partial"}:
            _error("计划列表读取尚未完成或失败，请刷新该类型计划后重试；本次未匹配通告。")
    return snapshot


def candidate_page(service, *, scope, month, work_type="", ongoing_items=(), page=1, page_size=200):
    from .identity_utils import canonical_source_record_id, canonical_target_record_id
    types = (work_type,) if work_type else TYPES
    if any(kind not in TYPES for kind in types):
        _error("此入口仅支持非事件计划通告。")
    if month != service._current_month_label():
        _error("计划名称匹配只使用当前月份，请重新匹配。")
    _ready(service, scope, types)
    with service._summary_lock:
        status_items = service._load_work_status_items_locked("ALL")
    sent_sources = {(service._item_work_type(item), canonical_source_record_id(item)) for item in status_items
                    if canonical_target_record_id(item)}
    rows, seen = [], set()
    for kind in types:
        candidates = service.list_bindable_source_items(scope=scope, month=month,
            work_type=kind, ongoing_items=list(ongoing_items), limit=None, include_linked=True, strict_month=True)
        linked_ids = service._state_store.notice_sources_with_targets(kind, [item["source_record_id"] for item in candidates])
        for item in candidates:
            key = (kind, item["source_record_id"])
            if key in seen:
                continue
            seen.add(key)
            target = key[1] in linked_ids or key in sent_sources or item.get("requires_verification")
            rows.append({**item, "requires_verification": bool(target),
                "verification_note": "已存在首条发送关联，请核验原通告，不再新增。" if target else ""})
    page, page_size = max(1, int(page)), min(500, max(1, int(page_size)))
    start = (page - 1) * page_size
    version = hashlib.sha256(json.dumps(rows, ensure_ascii=False, sort_keys=True).encode()).hexdigest()
    return {"items": rows[start:start + page_size], "total": len(rows), "page": page, "version": version,
            "page_size": page_size, "has_more": start + page_size < len(rows), "complete": True,
            "scope": scope, "month": month}


def prefill(service, *, scope, month, work_type, source_record_id, ongoing_items=()):
    from .workbench_lite import _draft_from_record, _datetime_local, REQUIRED_UPLOAD_FIELDS_BY_WORK_TYPE
    from .portal_service import NOTICE_TYPE_BY_WORK_TYPE, WORK_TYPE_BY_NOTICE_TYPE
    page = candidate_page(service, scope=scope, month=month, work_type=work_type,
                          ongoing_items=ongoing_items, page_size=500)
    items = page["items"]
    for number in range(2, (page["total"] + 499) // 500 + 1):
        items += candidate_page(service, scope=scope, month=month, work_type=work_type,
                                ongoing_items=ongoing_items, page=number, page_size=500)["items"]
    selected = next((row for row in items if row["source_record_id"] == source_record_id), None)
    if selected is None:
        _error("计划已不在当前可发送列表中，请重新选择；本次未新增。")
    if selected["requires_verification"]:
        _error(selected["verification_note"])
    record = next((row for row in service._workbench_records(scope=scope, month=month)
                   if row.get("record_id") == source_record_id and service._record_work_type(row) == work_type), None)
    if record is None:
        _error("未读取到完整计划，请重新加载。")
    return prefill_record(service, record, selected, scope=scope, month=month, work_type=work_type)


def prefill_record(service, record, selected, *, scope, month, work_type, source=None):
    """Build the same read-only draft for name matching and scheduled cards."""
    from .workbench_lite import _draft_from_record, _datetime_local, REQUIRED_UPLOAD_FIELDS_BY_WORK_TYPE
    from .portal_service import NOTICE_TYPE_BY_WORK_TYPE, WORK_TYPE_BY_NOTICE_TYPE
    source = copy.deepcopy(source) if source is not None else service._serialize_record(record)
    memory = source.pop("memory", {})
    source.pop("submitted_draft", None)
    draft = _draft_from_record(source, work_type=work_type)
    if work_type == "power" and draft.get("notice_type") not in {"上电通告", "下电通告"}:
        title = selected["title"]
        draft["notice_type"] = ("下电通告" if "下电" in title else "上电通告" if "上电" in title else "") if "上下电" not in title else ""
    elif WORK_TYPE_BY_NOTICE_TYPE.get(draft.get("notice_type")) != work_type:
        draft["notice_type"] = NOTICE_TYPE_BY_WORK_TYPE[work_type]
    if work_type == "repair":
        # Use the same source-field mapping without a second cloud lookup.
        fields = record.get("display_fields") or {}
        draft.update(start_time=_datetime_local(service._format_source_datetime(service._repair_first_field(
            fields, "期望完成时间", "维修结束时间", "维修结束时间（2026）"))),
            end_time=_datetime_local(service._format_source_datetime(service._repair_first_field(
                fields, "故障发生时间", "发现故障时间", "维修开始时间"))))
        draft.update(repair_device=service._repair_device_text(fields),
            repair_fault=service._repair_first_field(fields, "维修故障", "故障维修原因", "故障发生现象描述", "事件描述"),
            location=service._repair_location_text(fields), level=service._repair_first_field(fields, "紧急程度") or service._repair_level(fields),
            discovery=service._repair_first_field(fields, "故障发现方式", "对应来源", "事件发现来源"))
    draft["title"] = selected["title"]
    draft["progress"] = ""
    origins = {key: "plan" for key, value in draft.items() if value not in (None, "", [])}
    for key in STABLE[work_type]:
        if not draft.get(key) and memory.get(key):
            draft[key], origins[key] = memory[key], "history"
    if work_type == "maintenance":
        origins.update(start_time="generated", end_time="generated")
    required = set(REQUIRED_UPLOAD_FIELDS_BY_WORK_TYPE[work_type])
    if work_type == "adjust":
        required.discard("progress")
    if work_type == "power":
        required.add("notice_type")
    if work_type in {"maintenance", "change"}:
        required.add("execution_party")
    missing = sorted(key for key in required if not draft.get(key))
    version_data = {"scope": scope, "month": month, "type": work_type, "source": source,
                    "memory": {key: memory.get(key) for key in STABLE[work_type]}}
    version = hashlib.sha256(json.dumps(version_data, ensure_ascii=False, sort_keys=True, default=str).encode()).hexdigest()
    return {"source_record": source, "selected": selected, "draft": draft, "field_sources": origins,
            "missing_fields": missing, "version": version, "scope": scope, "month": month}


def validate_submission(service, payload):
    version = payload.get("planned_notice_version")
    if not version:
        return
    if payload.get("action") != "start" or payload.get("manual") or payload.get("target_record_id"):
        _error("计划预览只能通过原计划开始流程提交。")
    current = prefill(service, scope=payload["scope"], month=payload["source_month"],
        work_type=payload["work_type"], source_record_id=payload["source_record_id"],
        ongoing_items=service._state_store.list_qt_active_items(include_deleted=False))
    if current["version"] != version:
        _error("计划或历史内容已变化，请重新预览并确认；本次未发送。")
    fixed = {"title", "building_codes", "repair_management_record_id", "device", "repair_device", "cabinet", "quantity"}
    if payload["work_type"] == "power":
        fixed.add("notice_type")
    for key in fixed:
        if current["draft"].get(key) and payload.get(key) != current["draft"][key]:
            _error("计划名称、楼栋、操作对象和来源关系不能在此更换，请重新选择计划。")


def validate_source_scope(service, payload):
    if not payload.get("planned_notice_version"):
        return
    source_id, work = payload.get("source_record_id"), payload.get("work_type")
    if not source_id or work not in TYPES or payload.get("scope") in {"ALL", "CAMPUS", ""}:
        _error("请先选择本次计划和具体楼栋。")
    records = service._workbench_records(scope=payload["scope"], month=payload.get("source_month"))
    record = next((row for row in records if row.get("record_id") == source_id and service._record_work_type(row) == work), None)
    if record is None:
        _error("计划不在本次楼栋和月份内，请重新匹配。")
    source = service._serialize_record(record)
    if set(source.get("building_codes") or []) != {payload["scope"]}:
        _error("计划涉及的楼栋与本次选择不同，请核对原计划。")


def save_success_memory(service, job):
    prepared = job.get("prepared") or {}
    pending = prepared.get("planned_memory", [])
    work = prepared.get("work_type")
    if not pending and work in {"polling", "adjust", "power"}:
        pending = [{"building": prepared.get("building", ""), "maintenance_total": prepared.get("title", ""),
            "work_type": work, **{key: prepared.get(key, "") if key in STABLE[work] else ""
                                 for key in ("location", "content", "reason", "impact")},
            "extra_fields": {key: prepared.get(key, "") for key in STABLE[work]}}]
    for fields in pending:
        fields = copy.deepcopy(fields)
        allowed = STABLE.get(fields.get("work_type", "maintenance"), set())
        fields["extra_fields"] = {key: value for key, value in (fields.get("extra_fields") or {}).items() if key in allowed}
        fields["success_order"] = float(job.get("accepted_at") or 0)
        service._remember_draft_fields(**fields)
