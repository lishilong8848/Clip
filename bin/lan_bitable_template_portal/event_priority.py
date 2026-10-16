"""Event notices take priority; waiting is a persisted state, not an upload."""
import copy
import re
import threading
import time

from upload_event_module.core.parser import extract_event_info
from .identity_utils import canonical_target_record_id, is_local_record_id

NAMESPACE = "notice_event_priority"
WAITING = "event_waiting"
STATE_LOCK = threading.RLock()


def is_event(payload):
    return (payload.get("notice_type") == "事件通告"
            or (payload.get("work_type") or payload.get("lan_work_type")) == "event")


def event_key(payload):
    return canonical_target_record_id(payload) or str(payload.get("active_item_id") or "")


def summary(payload):
    info = extract_event_info(str(payload.get("text") or "")) or {}
    sections = dict(re.findall(r"【([^】]+)】\s*([^【]*)", str(payload.get("text") or "")))
    buildings = payload.get("building_codes") or payload.get("buildings") or info.get("buildings") or []
    if isinstance(buildings, str):
        buildings = [buildings]
    scopes = [str(value).replace("楼", "").replace("站", "").strip().upper() for value in buildings]
    return {
        "building_codes": scopes,
        "building": "、".join(f"{value}站" if value == "110" else f"{value}楼" for value in scopes) or "相关楼栋",
        "title": str(payload.get("title") or info.get("title") or "事件通告"),
        "occurrence_time": str(payload.get("time_str") or payload.get("occurrence_time") or info.get("time_str") or sections.get("事件发生时间") or "待核对").strip(),
        "source": str(payload.get("event_source") or payload.get("source") or info.get("source") or "待核对"),
        "level": str(payload.get("level") or info.get("level") or "待核对"),
        "progress": str(payload.get("progress") or sections.get("当前进度") or sections.get("进展") or sections.get("进度") or "待核对").strip(),
    }


def record_result(store, payload, action, target=""):
    with STATE_LOCK:
        _record_result(store, payload, action, target)


def _record_result(store, payload, action, target):
    if not is_event(payload):
        return
    payload = {**payload, **({"target_record_id": target} if target else {})}
    key = event_key(payload)
    if not key or is_local_record_id(key):
        return
    previous = store.get_document(NAMESPACE, key) or {}
    store.put_document(NAMESPACE, key, {
        "state": "ended" if action in {"end", "delete", "remove"} else "active",
        "active_item_id": str(payload.get("active_item_id") or previous.get("active_item_id") or ""),
        "target_record_id": key,
        "first_uploaded_at": previous.get("first_uploaded_at") or time.time(),
        "summary": summary(payload),
        "updated_at": time.time(),
    })


def blockers(store):
    with STATE_LOCK:
        return _blockers(store)


def _blockers(store):
    # A real ID alone is not proof of an upload (the user can bind one manually).
    rows = [row for row in store.list_qt_active_items(include_deleted=False)
            if is_event(row.get("payload") or row)]
    documents = {row["key"]: row["payload"] for row in store.list_documents(NAMESPACE)}
    live = {}
    for row in rows:
        payload = row.get("payload") or row
        key = event_key(payload)
        if not key or is_local_record_id(key):
            continue
        live[key] = payload
        if key not in documents:
            confirmed = bool(payload.get("last_remote_write_at"))
            if not confirmed:
                confirmed = any(
                    (op.get("result") or {}).get("remote_verified")
                    and (op.get("result") or {}).get("local_projection_completed")
                    and op.get("operation_type") in {"start", "update", "upload"}
                    for op in store.list_notice_remote_operations_for_target(key)
                )
            if confirmed:
                record_result(store, payload, "update", key)
                documents[key] = store.get_document(NAMESPACE, key)
    return [summary(live[key]) if key in live else copy.deepcopy(doc["summary"])
            for key, doc in documents.items() if doc.get("state") == "active"]


def wait_message(events, hidden=False):
    lines = [f"当前 {event['building']}事件：{event['title']}\n发生时间：{event['occurrence_time']}；"
             f"事件来源：{event['source']}；级别：{event['level']}；当前进度：{event['progress']}。"
             for event in events]
    if hidden:
        lines.append("另有无权查看详情的事件正在处理中。")
    lines.append("当前通告已排队，将在所有已上传事件结束或删除后自动发送。")
    buildings = "、".join(dict.fromkeys(event["building"] for event in events)) or "相关楼栋"
    lines.append(f"提醒：{buildings}发生事件，钉钉群中不要发送其它非事件通告，保证事件通告可读性。")
    return "\n\n".join(lines)


def public_job(job, auth, session):
    result = copy.deepcopy(job)
    wait = result.get("event_wait")
    if not isinstance(wait, dict):
        return result
    from .module_access import allowed_modules
    events = wait.get("events") or []
    admin = auth.is_admin(session)
    can_read = admin or "event" in allowed_modules(auth, session)
    visible = [event for event in events if admin or (can_read and event.get("building_codes")
        and all(auth.scope_allowed(session, scope) for scope in event["building_codes"]))]
    wait["events"] = visible
    wait["message"] = wait_message(visible, len(visible) != len(events))
    return result


def plan_wait_message(plan):
    if plan.get("status") not in {"running", "submitted"}:
        return ""
    for result in reversed(plan.get("results") or []):
        job = result.get("job_result") or result
        job = job.get("_raw", job.get("data")) or {}
        if (job.get("phase") or job.get("initial_phase")) == WAITING:
            return str((job.get("event_wait") or {}).get("message") or "已排队，等待事件结束后自动发送。")
    return ""
