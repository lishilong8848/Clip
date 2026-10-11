"""Native notice drafts and identity checks for the assistant notice panel."""
from __future__ import annotations

import copy
import datetime as dt
import hashlib
import json

from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_notice_sop import (
    SELECTION_KEYS, selection_field, selection_payload, update_directory,
)
from openclaw_service.assistant.lighthouse_sources import codes

from .identity_utils import canonical_source_record_id, canonical_target_record_id, is_local_record_id
from .planned_notices import TYPES, _ready, matches_month, prefill_record
from .portal_service import PortalError
from .workbench_lite import (
    REQUIRED_UPLOAD_FIELDS_BY_WORK_TYPE, _draft_from_record,
    EXECUTION_PARTY_OPTIONS, MAINTENANCE_CYCLE_OPTIONS, SPECIALTY_OPTIONS,
)
from .polling_work_orders import POLLING_H_DUTY_RECORD_ID

ZONE = dt.timezone(dt.timedelta(hours=8))
LABELS = {
    "title": "通告名称", "start_time": "开始时间", "end_time": "结束时间",
    "location": "位置", "content": "内容", "reason": "原因", "impact": "影响",
    "progress": "本次进度", "specialty": "专业", "execution_party": "执行方",
    "maintenance_cycle": "维保周期", "level": "等级", "repair_device": "故障设备",
    "repair_fault": "故障描述", "fault_type": "故障类型", "repair_mode": "维修方式",
    "discovery": "发现方式", "symptom": "故障现象", "solution": "解决方案",
    "spare_parts": "备件更换", "device": "设备", "cabinet": "柜号", "quantity": "数量",
    "notice_type": "上下电类型", "notice_sop": "本次工单",
    "work_order_choice": "本次工单", "notice_action": "本次操作",
}

SOP_WORK_TYPES = ("maintenance", "polling", "adjust")
NOTICE_SOP_KEYS = {"exempt", "scope", "sop_id", "sop_version",
                   "operator_record_id", "reviewer_record_id", "runs"}
NOTICE_SOP_RUN_KEYS = {"from_unit", "to_unit", "other_unit", "label", "run_index"}
NOTICE_SOP_STRING_MAX = 200
NOTICE_SOP_MAX_RUNS = 2

# Canned 本次进度 text auto-filled when an end action is chosen with a blank value.
# Only native work types that already carry a progress control are affected.
END_PROGRESS_DEFAULT = "工作已完成，设备运行正常，请知晓！"


def _progress_supports_end_default(item):
    """Whether this native notice draft carries an editable progress control.

    Only maintenance/change/repair/polling/power qualify; device-adjust and
    event/Qt notices never expose 本次进度 and are therefore never defaulted.
    """
    draft = item.get("draft") or {}
    work = str(draft.get("work_type") or "").strip()
    if work in ("", "adjust", "event"):
        return False
    return any(f.get("key") == "progress" for f in item.get("fields") or [])


def apply_end_default(draft, item, previous_action=None):
    """Fill/revert the end-on 本次进度 default on a mutable draft.

    Selecting 结束 fills the canned text when 本次进度 is blank; only a real
    ``结束`` → ``更新`` transition (pass ``previous_action=\"结束\"`` from the merge
    path) clears the untouched canned default.  An arbitrary update submission
    that merely happens to carry the default text must never erase an intentional
    user entry, so ``previous_action`` is left empty there.  A nonblank custom
    progress is always preserved.  The action must already be ``update``/``end``
    (start rows and later send paths are untouched).
    """
    if not _progress_supports_end_default(item):
        return
    if str(item.get("action") or "") not in ("update", "end"):
        return
    notice_action = str(draft.get("notice_action") or "")
    if notice_action == "结束":
        if not str(draft.get("progress") or "").strip():
            draft["progress"] = END_PROGRESS_DEFAULT
    elif notice_action == "更新":
        if str(previous_action or "") == "结束" and str(draft.get("progress") or "").strip() == END_PROGRESS_DEFAULT:
            draft["progress"] = ""


def _default_notice_sop(exempt=False):
    return {"exempt": bool(exempt), "scope": "", "sop_id": "", "sop_version": 0,
            "operator_record_id": "", "reviewer_record_id": "", "runs": []}


def _is_default_notice_sop(value):
    return (isinstance(value, dict) and value.get("exempt") is False
            and not value.get("scope") and not value.get("sop_id")
            and value.get("sop_version") in (0, None)
            and not value.get("operator_record_id")
            and not value.get("reviewer_record_id") and not value.get("runs"))


def _normalize_notice_sop_shape(value):
    """Strict/bounded shape validation of the UI-only notice_sop draft dict.

    The typed schema is fixed and never coerced: ``exempt`` must be a real bool,
    the version a non-negative int, every identifier/scope a string, and runs a
    list of string-keyed dicts (at most two).  Blank strings/empty lists are
    accepted so partial drafts can be saved.
    """
    if not isinstance(value, dict):
        raise PortalError("本次工单填写格式无效。")
    unknown = set(value) - NOTICE_SOP_KEYS
    if unknown:
        raise PortalError("本次工单含未知字段。")
    if "exempt" not in value:
        raise PortalError("本次工单缺少豁免标记。")
    if type(value["exempt"]) is not bool:
        raise PortalError("本次工单豁免标记格式无效。")
    result = {"exempt": value["exempt"], "scope": "", "sop_id": "", "sop_version": 0,
              "operator_record_id": "", "reviewer_record_id": "", "runs": []}
    for key in ("scope", "sop_id", "operator_record_id", "reviewer_record_id"):
        raw = value.get(key)
        if raw is None:
            result[key] = ""
        elif isinstance(raw, str):
            result[key] = raw.strip()
        else:
            raise PortalError("本次工单字段格式无效。")
        if len(result[key]) > NOTICE_SOP_STRING_MAX:
            raise PortalError("本次工单内容过长。")
    raw_version = value.get("sop_version")
    if raw_version is None:
        result["sop_version"] = 0
    elif type(raw_version) is int and raw_version >= 0:
        result["sop_version"] = raw_version
    else:
        raise PortalError("本次工单SOP版本格式无效。")
    raw_runs = value.get("runs")
    if raw_runs is None:
        result["runs"] = []
    elif isinstance(raw_runs, list):
        if len(raw_runs) > NOTICE_SOP_MAX_RUNS:
            raise PortalError("本次工单设备指向数量过多。")
        clean_runs = []
        for run in raw_runs:
            if not isinstance(run, dict):
                raise PortalError("本次工单设备指向格式无效。")
            run_unknown = set(run) - NOTICE_SOP_RUN_KEYS
            if run_unknown:
                raise PortalError("本次工单设备指向含未知字段。")
            clean_run = {}
            for key in ("from_unit", "to_unit", "other_unit", "label"):
                if key not in run:
                    continue
                run_value = run[key]
                if not isinstance(run_value, str):
                    raise PortalError("本次工单设备指向字段格式无效。")
                clean_run[key] = run_value.strip()
                if len(clean_run[key]) > NOTICE_SOP_STRING_MAX:
                    raise PortalError("本次工单设备指向内容过长。")
            if "run_index" in run:
                run_index = run["run_index"]
                if type(run_index) is not int or run_index < 0:
                    raise PortalError("本次工单设备指向序号无效。")
                clean_run["run_index"] = run_index
            clean_runs.append(clean_run)
        result["runs"] = clean_runs
    else:
        raise PortalError("本次工单设备指向配置无效。")
    return result


def _notice_sop_from_draft(draft, work_type, action):
    """Return a normalized notice_sop, migrating legacy work_order_choice in memory."""
    current = None
    if isinstance(draft.get("notice_sop"), dict):
        current = _normalize_notice_sop_shape(draft["notice_sop"])
        if not _is_default_notice_sop(current) or "work_order_choice" not in draft:
            return current
    choice = draft.get("work_order_choice")
    if work_type in SOP_WORK_TYPES and action == "start":
        if choice == "本次无需工单":
            return _default_notice_sop(exempt=True)
        if choice == "使用网页已配置工单":
            return _default_notice_sop(exempt=False)
    return current if current is not None else _default_notice_sop(exempt=False)


def _normalize_item_notice_sop(item):
    if not any(f.get("key") == "notice_sop" for f in item.get("fields") or []):
        return
    draft = item.setdefault("draft", {})
    draft["notice_sop"] = _notice_sop_from_draft(
        draft, str(draft.get("work_type") or ""), item.get("action") or draft.get("action"))


def load_sop_field(runtime, service, scope, work_type, actor_scopes):
    """Build a populated selection_field from live (non-refreshing) directories.

    No cloud full-sync is forced and no workers/preload are spawned here by itself.
    """
    if not isinstance(actor_scopes, (list, tuple, set)) or not actor_scopes:
        raise PortalError("无权选择该楼栋的工单。")
    scope = str(scope or "").strip().upper()
    work_type = str(work_type or "").strip()
    actor = {"scopes": [str(s).upper() for s in actor_scopes]}
    probe = {"action": "start", "scope": scope, "work_type": work_type}
    field = selection_field(actor, probe, {}, 0)
    if not field or scope not in field["scopes"]:
        raise PortalError("无权选择该楼栋的工单。")
    if runtime is None or not hasattr(runtime, "polling_work_orders"):
        raise PortalError("工单目录暂不可用。")
    sops = runtime.polling_work_orders().list_sops(scope, work_type)
    people = service.signature_management.directory(refresh=False)["people"]
    update_directory(field, scope, sops, people)
    return field


SOP_OPTIONS_PEOPLE_CAP = 200


def _person_label(person):
    parts = [str(person.get(key) or "").strip()
             for key in ("name", "employee_no", "building", "position", "shift")]
    label = " · ".join(part for part in parts if part)
    return label or str(person.get("record_id") or "")


def build_sop_options_field(runtime, service, scope, work_type, actor_scopes,
                            selected_ids=(), q=""):
    """Build a public-only sop selection field for the lazy directory endpoint.

    Only local staff (and the H 楼值班账号) are returned.  The response never
    contains ``open_id``, ``_sops``/``_people`` internals, snapshot paths, or any
    record that is not part of the persisted scope.  People are filtered by the
    ``q`` search against name/employee-no/building while always keeping the
    currently selected operator/reviewer, capped at ``SOP_OPTIONS_PEOPLE_CAP``.
    """
    query = str(q or "").strip().lower()
    field = load_sop_field(runtime, service, scope, work_type, actor_scopes)
    known = field.get("_people") or {}
    # Persisted choices must always be shown to the frontend, even while the user
    # is searching (they may not match ``q``).  Valid selected records are stable,
    # placed first, then the H 楼值班账号, then ordinary query matches.
    selected_valid = [str(rid).strip() for rid in (selected_ids or ())
                      if str(rid).strip() in known]
    selected_set = set(selected_valid)
    matched = []
    for record_id, person in known.items():
        record_id = str(record_id or "").strip()
        if not record_id or not isinstance(person, dict):
            continue
        if query:
            haystack = " ".join(str(person.get(key) or "").lower()
                                for key in ("name", "employee_no", "building"))
            if query not in haystack:
                continue
        matched.append(record_id)
    ordered = list(dict.fromkeys([*selected_valid, POLLING_H_DUTY_RECORD_ID, *matched]))
    public_people = []
    for record_id in ordered:
        if len(public_people) >= SOP_OPTIONS_PEOPLE_CAP and record_id not in selected_set:
            continue
        person = known[record_id]
        public_people.append({
            "record_id": record_id,
            "name": str(person.get("name") or "").strip(),
            "label": _person_label(person),
        })
    public_sops = [dict(sop) for sop in (field.get("sops") or []) if isinstance(sop, dict)]
    return {
        "directory_scope": scope,
        "work_type": work_type,
        "sops": public_sops,
        "people": public_people,
    }


def _clear_native_sop_fields(body):
    for key in SELECTION_KEYS:
        body.pop(key, None)


def _sop_preview_suffix(labels):
    if not isinstance(labels, dict):
        return ""
    parts = []
    sop = str(labels.get("sop") or "").strip()
    operator = str(labels.get("operator") or "").strip()
    reviewer = str(labels.get("reviewer") or "").strip()
    if sop:
        parts.append("工单：" + sop)
    if operator:
        parts.append("操作人：" + operator)
    if reviewer:
        parts.append("审核人：" + reviewer)
    return "\n——本次工单——\n" + "\n".join(parts) if parts else ""


def _prepare_notice_sop(service, item, body, runtime):
    """Validate the notice_sop selection locally and merge native polling_* fields."""
    work_type = str(body.get("work_type") or "").strip()
    if body.get("action") != "start" or work_type not in SOP_WORK_TYPES:
        return None
    draft = item.get("draft") or {}
    notice_sop = _notice_sop_from_draft(draft, work_type, body.get("action"))
    _clear_native_sop_fields(body)
    if notice_sop["exempt"]:
        body["polling_work_order_exempt"] = True
        body.pop("notice_sop", None)
        return {"sop": "本次不使用工单"}
    scope = str(notice_sop.get("scope") or body.get("scope") or "").upper()
    body["polling_work_order_exempt"] = False
    if not notice_sop.get("sop_id") or not notice_sop.get("operator_record_id") or not notice_sop.get("reviewer_record_id"):
        raise PortalError("请选择有效的 SOP、操作人和现场审核人。")
    if scope != str(body.get("scope") or "").upper():
        raise PortalError("所选工单楼栋与通告楼栋不一致。")
    if runtime is None or not hasattr(runtime, "polling_work_orders"):
        raise PortalError("工单目录暂不可用，请重新打开待办。")
    field = load_sop_field(runtime, service, scope, work_type, [scope])
    current = field.get("_sops") or {}
    sop = current.get(notice_sop["sop_id"])
    if not sop or sop.get("blocked_reason"):
        raise PortalError((sop or {}).get("blocked_reason") or "本次选择的工单已不可用，请重新选择。")
    if int(sop.get("version") or 0) != int(notice_sop.get("sop_version") or 0):
        raise PortalError("所选的工单 SOP 已更新，请重新选择后核对。")
    value_for_payload = {k: v for k, v in notice_sop.items() if k != "sop_version"}
    actor = {"scopes": [scope]}
    try:
        selection, labels = selection_payload(
            field, value_for_payload, actor, codes(body.get("building_codes")))
    except AssistantError as exc:
        raise PortalError(str(exc)) from None
    _clear_native_sop_fields(body)
    body.update(selection)
    body.pop("notice_sop", None)
    return labels


def now():
    return dt.datetime.now(ZONE)


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, default=str).encode()).hexdigest()


def plan_window(record, current):
    fields = record.get("raw_fields") or record.get("display_fields") or {}
    values = [fields.get(key) for key in ("计划开始维护时间", "计划结束维护时间")]
    def parse(value):
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return dt.datetime.fromtimestamp(value / 1000, ZONE).date()
        return dt.date.fromisoformat(str(value or "")[:10].replace("/", "-"))
    try:
        start, end = map(parse, values)
    except (ValueError, OSError, OverflowError):
        return "计划日期待核对", "计划日期缺失或异常，请先在网页核对。", False
    if end < start:
        return "计划日期待核对", "计划结束日期早于开始日期。", False
    return (f"{start:%Y-%m-%d} 至 {end:%Y-%m-%d}",
            "未到计划时间" if current.date() < start else "", end < current.date())


def fields_for(work, draft, action):
    required = set(REQUIRED_UPLOAD_FIELDS_BY_WORK_TYPE[work])
    if work == "adjust":
        required.discard("progress")
    if work == "power":
        required.add("notice_type")
    if action in {"update", "end"}:
        required.add("notice_action")
    if work in SOP_WORK_TYPES and action == "start":
        required.add("notice_sop")
    options = {"specialty": list(SPECIALTY_OPTIONS), "execution_party": [x for x in EXECUTION_PARTY_OPTIONS if x],
               "maintenance_cycle": list(MAINTENANCE_CYCLE_OPTIONS), "notice_type": ["上电通告", "下电通告"],
               "notice_action": ["更新", "结束"]}
    if work == "repair":
        options["level"] = ["低", "中", "高"]
    result = []
    for key, label in LABELS.items():
        if key not in required and not (work == "repair" and key == "spare_parts"):
            continue
        if work == "repair" and key in {"start_time", "end_time"}:
            label = "期望完成时间" if key == "start_time" else "故障发生时间"
        if key in {"start_time", "end_time"}:
            label += "（YYYY-MM-DD HH:mm）"
        if key == "notice_sop":
            result.append({"key": "notice_sop", "label": "本次工单", "required": True,
                           "readonly": False, "kind": "notice_sop", "options": []})
            continue
        choices = options.get(key, [])
        if draft.get(key) and choices and str(draft[key]) not in choices:
            choices = [str(draft[key]), *choices]
        result.append({"key": key, "label": label, "required": key in required,
            "readonly": key == "title" or (action == "start" and key in {"device", "repair_device", "cabinet", "quantity"} and bool(draft.get(key))) or len(str(draft.get(key) or "")) > 1000,
            "kind": "select" if choices else "multiline" if key in {"content", "reason", "impact", "progress", "symptom", "solution"} else "text",
            "options": choices})
        if key == "progress":
            result[-1]["default_on_end"] = END_PROGRESS_DEFAULT
    return result


def build_items(service, scope, work, ongoing, current=None):
    if work not in (*TYPES, "all"):
        raise PortalError("事件通告仍须在 Qt 中发送。")
    current = current or now()
    month = f"{current.month}月"
    kinds = TYPES if work == "all" else (work,)
    snapshot = _ready(service, scope, kinds)
    month_records = (service._workbench_records(scope=scope, month=month, source_snapshot=snapshot)
                     if any(kind != "maintenance" for kind in kinds) else [])
    memory_cache = {}
    result = [item for kind in kinds for item in _build_kind_items(
        service, scope, kind, ongoing, current, snapshot, month_records, memory_cache)]
    for item in result:
        _normalize_item_notice_sop(item)
    return result


def _build_kind_items(service, scope, work, ongoing, current, snapshot, month_records, memory_cache):
    month = f"{current.month}月"
    records = (snapshot.get("records") or []) if work == "maintenance" else month_records
    rows = [r for r in records if service._record_work_type(r) == work]
    summary = service._work_status_by_records(rows, scope=scope)
    linked = service._state_store.notice_sources_with_targets(work, [str(r.get("record_id") or "") for r in rows])
    result, included_targets = [], set()
    for record in rows:
        source_id = str(record.get("record_id") or "")
        serialized = service._serialize_record(record, summary, memory_cache)
        if set(serialized.get("building_codes") or []) != {scope}:
            continue
        status = str(serialized.get("source_progress") or "").strip()
        if not service._source_progress_allows_start(status):
            continue
        window, blocked, expired = plan_window(record, current) if work == "maintenance" else ("当前月份计划", "", False)
        if expired:
            continue
        if work in {"polling", "adjust", "power"} and not matches_month(service, record, month):
            continue
        active = [r for r in ongoing if service._item_work_type(r) == work and canonical_source_record_id(r) == source_id]
        if len(active) > 1:
            blocked = "发现多个进行中关联，请先在网页核验。"
        active = active[0] if len(active) == 1 else None
        target_id = canonical_target_record_id(active or {})
        if active and (not target_id or is_local_record_id(target_id) or target_id == source_id):
            blocked = "首条发送结果尚未确认，请在网页继续原任务。"
        if not active and (source_id in linked or serialized.get("target_record_id")
                           or canonical_target_record_id(serialized.get("work_summary") or {})):
            blocked = "存在首条发送关联但未在未结束列表中，请先核验原记录。"
        if active:
            item = ongoing_item(service, scope, work, active)
            included_targets.add(target_id)
        else:
            selected = {"title": serialized.get("title") or service._maintenance_title(record), "source_record_id": source_id, "work_type": work}
            # The native version uses the source without local work-summary overrides.
            source = (service._serialize_record(record, building_memory_cache=memory_cache)
                      if work in {"polling", "adjust", "power"} else serialized)
            data = prefill_record(service, record, selected, scope=scope, month=month, work_type=work, source=source)
            body = {**data["draft"], "scope": scope, "work_type": work, "action": "start", "manual": False,
                    "record_id": source_id, "source_record_id": source_id, "source_month": month}
            if work in {"polling", "adjust", "power"} and not blocked:
                body["planned_notice_version"] = data["version"]
            item = {"title": selected["title"], "action": "start", "draft": body,
                    "version": fingerprint(record.get("raw_fields") or record.get("display_fields") or record)}
        item.update(key=fingerprint([work, source_id, target_id])[:12], window=window, status=status,
                    blocked=blocked, selected=False, fields=fields_for(work, item["draft"], item["action"]))
        result.append(item)
    # Independent notices have no plan row but still belong in the pending list.
    for active in ongoing:
        if service._item_work_type(active) != work or canonical_target_record_id(active) in included_targets:
            continue
        target = canonical_target_record_id(active)
        if not target or is_local_record_id(target):
            continue
        item = ongoing_item(service, scope, work, active)
        if set(item["draft"].get("building_codes") or []) != {scope}:
            continue
        item.update(key=fingerprint([work, canonical_source_record_id(active), target])[:12], window="已开始未结束（含较早开始的通告）", status="进行中", blocked="",
                    selected=False, fields=fields_for(work, item["draft"], "update"))
        result.append(item)
    return sorted(result, key=lambda item: (bool(item["blocked"]), item["action"] != "update", item["title"], item["key"]))


def ongoing_item(service, scope, work, active):
    record = copy.deepcopy(active)
    record.pop("memory", None)
    record.pop("submitted_draft", None)
    draft = _draft_from_record(record, work_type=work)
    draft.update(scope=scope, work_type=work, action="update", active_item_id=active.get("active_item_id", ""),
                 source_record_id=canonical_source_record_id(active), target_record_id=canonical_target_record_id(active), notice_action="更新")
    # The user must supply this update's progress rather than re-send yesterday's progress.
    if work != "adjust":
        draft["progress"] = ""
    return {"title": draft["title"], "action": "update", "draft": draft,
            "version": fingerprint({k: active.get(k) for k in ("text", "target_record_id", "source_record_id", "status", "progress", "start_time", "end_time")})}


def submission(service, item, runtime=None):
    body = copy.deepcopy(item["draft"])
    # Older clients / Feishu may send an end action without 本次进度; default it
    # before missing-field checks, preview and the final payload so drafts,
    # previews and confirm keep the same value.
    apply_end_default(body, item)
    has_notice_sop_field = any(f.get("key") == "notice_sop" for f in item.get("fields") or [])
    missing = [f["label"] for f in item["fields"]
               if f["required"] and f["key"] != "notice_sop"
               and not str(body.get(f["key"]) or "").strip()]
    if missing:
        raise PortalError("请填写：" + "、".join(missing))
    for field in item["fields"]:
        if field["key"] == "notice_sop":
            continue
        if field["options"] and body.get(field["key"]) not in field["options"]:
            raise PortalError("请选择有效的" + field["label"])
    notice_action = body.pop("notice_action", "")
    if item["action"] in {"update", "end"}:
        body["action"] = "end" if notice_action == "结束" else "update"
    labels = None
    if has_notice_sop_field:
        labels = _prepare_notice_sop(service, item, body, runtime)
    else:
        choice = body.pop("work_order_choice", "")
        if choice == "本次无需工单":
            body["polling_work_order_exempt"] = True
        elif choice == "使用网页已配置工单":
            raise PortalError("本次需使用工单，请在网页完成SOP、执行人和审核人配置后发送；此处不会跳过工单要求。")
    for key in ("start_time", "end_time"):
        value = service._format_input_datetime(body.get(key))
        try:
            parsed = dt.datetime.fromisoformat(value)
            if len(value) < 16:
                raise ValueError("time required")
        except (TypeError, ValueError):
            raise PortalError("请填写有效的" + LABELS[key])
        body[key] = (parsed.astimezone(ZONE) if parsed.tzinfo else parsed).strftime("%Y-%m-%d %H:%M")
    if body["work_type"] != "repair":
        service._validate_minimum_notice_duration(body["start_time"], body["end_time"])
    if body["action"] == "end":
        service._require_end_site_photo_cumulative(body, "end", notice_type=body.get("notice_type", ""), work_type=body["work_type"])
    preview = service._synchronize_prepared_notice_text({**body, "status": {"start": "开始", "update": "更新", "end": "结束"}[body["action"]]})["text"]
    if labels:
        preview = preview + _sop_preview_suffix(labels)
    return body, preview
