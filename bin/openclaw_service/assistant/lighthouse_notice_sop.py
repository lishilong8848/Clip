"""Notice-start selection only; work-order execution stays in its native UI."""
from copy import deepcopy

from .lighthouse_ai import AssistantError
from .lighthouse_sources import codes
from lan_bitable_template_portal.polling_work_orders import (
    ADJUST_COOLING_MODES, POLLING_H_DUTY_RECORD_ID, POLLING_SOP_SCOPES,
    POLLING_UNITS, POLLING_WORK_ORDER_MAX_EXPANDED_STEPS, WORK_ORDER_TYPES,
    PollingWorkOrderService, _adjust_cooling_sop_tokens, _polling_unit_group,
)
from lan_bitable_template_portal.portal_service import PortalError

SELECTION_KEYS = (
    "polling_work_order_exempt", "polling_sop_id", "polling_sop_version",
    "polling_run_count", "polling_runs", "polling_operator_record_id", "polling_reviewer_record_id",
)


def selection_field(actor, body, selection, index):
    if body.get("action") != "start" or body.get("work_type", "maintenance") not in WORK_ORDER_TYPES:
        return None
    allowed = set(actor["scopes"]) & (codes(body.get("scope")) or set(actor["scopes"])) & POLLING_SOP_SCOPES
    scope = next(iter(allowed)) if len(allowed) == 1 else ""
    initial = {"exempt": selection.get("polling_work_order_exempt") is True, "scope": scope,
               "sop_id": selection.get("polling_sop_id") or "",
               "operator_record_id": selection.get("polling_operator_record_id") or "",
               "reviewer_record_id": selection.get("polling_reviewer_record_id") or "",
               "runs": deepcopy(selection.get("polling_runs") or [])}
    return {"name": f"step{index}.notice_sop", "path": "", "section": "body", "operation_index": index,
            "type": "object", "required": True, "label": "SOP 与人员", "native_notice_sop": True,
            "options_source": "notice_sops", "work_type": body.get("work_type", "maintenance"),
            "scopes": sorted(allowed), "directory_scope": "", "sops": [], "people": [],
            "_initial_form": initial, "_sops": {}, "_people": {}}


def update_directory(field, scope, sops, people):
    selected = {}
    for row in sops:
        if not isinstance(row, dict) or not row.get("sop_id") or row.get("scope") != scope or row.get("work_type", "polling") != field["work_type"]:
            raise AssistantError("SOP 目录内容或楼栋不一致，请重新读取。", 502)
        tokens = _adjust_cooling_sop_tokens(row.get("steps") or [])
        mode = "polling" if field["work_type"] == "polling" else "cooling" if field["work_type"] == "adjust" and tokens == {"{{from}}"} else "normal"
        blocked = ""
        if not row.get("ready") or not row.get("steps") or not row.get("attachments"):
            blocked = "缺少步骤或附件"
        elif mode != "polling" and tokens and mode != "cooling":
            blocked = "包含当前工单不支持的设备占位符"
        elif row.get("cloud_sync_required") and row.get("cloud_sync_status") != "synced":
            blocked = row.get("cloud_sync_error") or "尚未同步多维"
        selected[row["sop_id"]] = {**deepcopy(row), "mode": mode, "blocked_reason": blocked}
    known_people = deepcopy(field.get("_people") or {}) if field.get("directory_scope") == scope else {}
    for person in people:
        if isinstance(person, dict) and person.get("record_id") and person.get("name") and person.get("source", "staff") == "staff":
            known_people[person["record_id"]] = {key: person[key] for key in (
                "record_id", "name", "employee_no", "building", "position", "shift", "open_id", "can_receive_message") if key in person}
    known_people[POLLING_H_DUTY_RECORD_ID] = PollingWorkOrderService._person_by_id([], POLLING_H_DUTY_RECORD_ID, allow_h_duty=True)
    public_people = [{"record_id": key, "name": person["name"],
                      "label": " · ".join(str(person.get(name) or "") for name in ("name", "employee_no", "building", "position", "shift") if person.get(name))}
                     for key, person in known_people.items()]
    field.update(directory_scope=scope, _sops=selected, _people=known_people,
                 sops=[{key: row.get(key) for key in ("sop_id", "name", "version", "mode", "blocked_reason", "steps")}
                       | {"documents": [{"name": file.get("name", "附件")} for file in row.get("attachments", [])]}
                       for row in selected.values()], people=public_people)


def selection_payload(field, value, actor, notice_buildings):
    if not isinstance(value, dict) or set(value) - set(field["_initial_form"]) or type(value.get("exempt")) is not bool or any(not isinstance(value.get(key), str) for key in ("scope", "sop_id", "operator_record_id", "reviewer_record_id")):
        raise AssistantError("请选择有效的 SOP 和人员配置。")
    empty = dict(zip(SELECTION_KEYS, (False, "", 0, 0, [], "", "")))
    if value["exempt"]:
        return {**empty, "polling_work_order_exempt": True}, {"sop": "本次不使用工单"}
    scope = value.get("scope")
    if scope not in set(field["scopes"]) & set(actor["scopes"]) or set(notice_buildings) != {scope}:
        raise AssistantError("使用 SOP 的通告须选择一个楼栋，且与所选 SOP 楼栋一致。", 403)
    if field.get("directory_scope") != scope:
        raise AssistantError("请先读取当前楼栋 SOP 和人员目录。")
    sop = field["_sops"].get(value.get("sop_id"))
    if not sop or sop.get("blocked_reason"):
        raise AssistantError((sop or {}).get("blocked_reason") or "请选择已读取的 SOP。")
    if value.get("operator_record_id") == POLLING_H_DUTY_RECORD_ID:
        raise AssistantError("H楼值班账号只能作为现场审核人。")
    try:
        people = list(field["_people"].values())
        operator = PollingWorkOrderService._person_by_id(people, value.get("operator_record_id"))
        reviewer = PollingWorkOrderService._person_by_id(people, value.get("reviewer_record_id"), allow_h_duty=True)
        steps = PollingWorkOrderService._expanded_steps(sop["steps"])
    except PortalError as exc:
        raise AssistantError(str(exc)) from None
    if operator["record_id"] == reviewer["record_id"] or operator.get("open_id") and operator.get("open_id") == reviewer.get("open_id"):
        raise AssistantError("操作人和现场审核人不能是同一人。")
    runs = value.get("runs")
    if not isinstance(runs, list) or any(not isinstance(run, dict) or set(run) - {"from_unit", "to_unit", "other_unit", "label", "run_index"} for run in runs):
        raise AssistantError("设备指向配置无效。")
    if sop["mode"] == "normal":
        runs = [{"label": "维保作业" if field["work_type"] == "maintenance" else "设备调整作业"}]
    elif sop["mode"] == "cooling":
        if len(runs) != 1 or runs[0].get("other_unit") not in POLLING_UNITS:
            raise AssistantError("请选择本次调整的制冷单元。")
        if any(runs[0].get(key) not in ADJUST_COOLING_MODES for key in ("from_unit", "to_unit")) or runs[0]["from_unit"] == runs[0]["to_unit"]:
            raise AssistantError("请选择不同的当前运行模式和切换后模式。")
        runs = [{key: runs[0][key] for key in ("from_unit", "to_unit", "other_unit")}]
    else:
        if not 1 <= len(runs) <= 2:
            raise AssistantError("轮巡次数必须为 1–2 次。")
        used = set()
        for run in runs:
            start, end = run.get("from_unit"), run.get("to_unit")
            if start not in POLLING_UNITS or end not in _polling_unit_group(start) or start == end or start in used or end in used:
                raise AssistantError("轮巡起终点须在同组，且不能重复使用前次设备。")
            used.update((start, end))
        runs = [{key: run[key] for key in ("from_unit", "to_unit")} for run in runs]
    if len(steps) * len(runs) > POLLING_WORK_ORDER_MAX_EXPANDED_STEPS:
        raise AssistantError("本次工单展开后的总步骤不能超过100条。")
    return {"polling_work_order_exempt": False, "polling_sop_id": sop["sop_id"], "polling_sop_version": sop["version"],
            "polling_run_count": len(runs), "polling_runs": runs, "polling_operator_record_id": operator["record_id"],
            "polling_reviewer_record_id": reviewer["record_id"]}, {"sop": sop["name"], "operator": operator["name"], "reviewer": reviewer["name"]}
