"""Task-scoped invitations reuse the shared personnel and one-use signing flow."""
from .signature_management import REQUEST_NS, SignatureManagementError, _hash, _id, _text

NAMESPACE = "mop_signature_invitation"


def _directory(manager):
    data = manager.directory()
    if any(not info["ok"] for info in data["sources"].values()):
        raise SignatureManagementError("人员表读取失败，请稍后重试。", 503)
    return data


def _request(manager, request_id):
    request = manager.store.get_document(REQUEST_NS, request_id) or {}
    return manager._public_request(request) if request else {"request_id": request_id, "status": "pending"}


def _item(row, operation_id, person, request):
    signed = bool(person and person.get("has_signature"))
    status = request["status"]
    reason = {"expired": "签名链接已过期，请重新发送链接。", "revoked": "签名链接已失效，请重新发送链接。",
              "send_failed": request.get("error") or "签名链接发送失败，请重试。",
              "send_unknown": "消息发送结果未确认，请先核对原请求，不要重复发送。",
              "sending": "消息发送中，请先核对原请求。"}.get(status, "等待本人签名并保存。")
    request_id = str(row.get("request_id") or "")
    return {"source": "external", "mop_invitation": True, "record_id": row["record_id"], "role": row["role"],
            "name": row["name"], "building": row["building"], "scope": row["scope"], "specialty": row["specialty"],
            "employee_no": row.get("employee_no", ""), "operation_id": operation_id,
            "send_operation_id": request_id.removeprefix("mopsend_"), "request_id": request_id,
            "recipient": row.get("recipient", ""), "expires_at": request.get("expires_at"),
            "has_signature": signed, "status": "signed" if signed else "pending",
            "request_status": status, "signature_reason": "" if signed else reason}


def dispatch(manager, operation, payload, *, actor, auth, base_url, send_text):
    profile = auth.account_profile(actor)
    if "engineer_mop" not in profile.get("modules", []) or auth.directory_block_reason(actor):
        raise SignatureManagementError("当前账号没有维护单办理权限。", 403)
    scopes = [s for s in profile.get("scope_rules", []) if s in manager.d.BUILDING_OPEN_ID_MAP]
    scope = _text(payload.get("scope") or "ALL")
    if scope not in {"ALL", "CAMPUS", *scopes}:
        raise SignatureManagementError("不能为未授权楼栋添加维护单签名人员。", 403)
    notice_key = _text(payload.get("notice_key"), 500)
    if not notice_key:
        raise SignatureManagementError("请先选择维保通告。")
    prefix = _hash([actor, notice_key]) + ":"
    if operation == "mop-list":
        rows = [{**item["payload"], "operation_id": item["key"][len(prefix):]}
                for item in manager.store.list_documents(NAMESPACE, key_prefix=prefix)]
        rows = [row for row in rows if row.get("scope") in scopes
                and (scope in {"ALL", "CAMPUS"} or row["scope"] == scope) and row.get("record_id")]
        if not rows:
            return {"items": []}
        directory = _directory(manager)
        people = {p["record_id"]: p for p in directory["external"]}
        items = []
        for row in rows:
            person = people.get(row["record_id"])
            request = _request(manager, row.get("request_id", ""))
            items.append(_item(row, row["operation_id"], person, request))
        return {"items": items}

    notices = manager.s._engineer_month_maintenance_notices(scope=scope, ongoing_items=None)
    notice = next((item for item in notices if item.get("notice_key") == notice_key), None)
    notice_scopes = manager.s._building_codes_from_value(
        (notice or {}).get("building_codes") or (notice or {}).get("building"))
    available = [s for s in scopes if s in notice_scopes]
    if not notice or not available:
        raise SignatureManagementError("维保通告不存在或不在授权楼栋内，请重新读取。", 403)
    directory = _directory(manager)
    recipients = [p for p in directory["staff"] if p.get("can_receive_message")
                  and set(manager.s._building_codes_from_value(p.get("building"))) & set(scopes)
                  and not auth.directory_block_reason(p.get("open_id") or "")]
    if operation == "mop-people":
        return {"scope_options": [{"value": s, "label": "110站" if s == "110" else s + "楼"} for s in available],
                "recipients": [{"value": "duty:" + s, "label": "110站值班账号" if s == "110" else s + "楼值班账号"}
                               for s in scopes] + [{"value": "staff:" + p["record_id"],
                    "label": " · ".join(str(p.get(k) or "") for k in ("name", "employee_no", "building")).strip(" ·")}
                    for p in recipients]}

    if scope not in available:
        raise SignatureManagementError("请选择该维保通告所属的具体楼栋。")
    recipient = _text(payload.get("recipient"))
    if recipient not in {"duty:" + s for s in scopes} | {"staff:" + p["record_id"] for p in recipients}:
        raise SignatureManagementError("请选择授权范围内可接收签名链接的人员或值班账号。", 403)
    role = _text(payload.get("role"))
    if role not in {"implementer", "auditor"}:
        raise SignatureManagementError("维护单签名角色无效。")
    details = {"name": _text(payload.get("name"), 80), "building": "110站" if scope == "110" else scope + "楼",
               "employee_no": _text(payload.get("employee_no"), 80), "specialty": _text(payload.get("specialty"), 80)}
    if not details["name"]:
        raise SignatureManagementError("请填写签名人员姓名。")
    operation_id, send_id = _id(payload.get("operation_id")), _id(payload.get("send_operation_id"))
    _id("mopcreate_" + operation_id)
    _id("mopsend_" + send_id)
    expected_request = _text(payload.get("expected_request_id"), 128)
    key = prefix + operation_id
    fingerprint = _hash([actor, notice_key, scope, role, details])
    with manager.lock("mop-invite:" + key):
        row = manager.store.get_document(NAMESPACE, key)
        if row and row.get("fingerprint") != fingerprint:
            raise SignatureManagementError("本次添加信息已变化，请完成原请求后重新添加。", 409)
        if expected_request and not row:
            raise SignatureManagementError("原签名请求不存在或不属于当前账号，请重新读取。", 403)
        person = (manager.person("external", row["record_id"]) if row and row.get("record_id") else
                  manager._new_temporary(details, "mopcreate_" + operation_id, actor))
        requested_id = "mopsend_" + send_id
        if row and row.get("request_id") == requested_id and manager.store.get_document(REQUEST_NS, requested_id):
            current = _request(manager, requested_id)
            return {"person": _item(row, operation_id, person, current), "request": current}
        if row and row.get("request_id") and row["request_id"] != requested_id:
            current = _request(manager, row["request_id"])
            # A stale tab must not replace the newest link, even if its old attempt was revoked.
            if (person.get("has_signature") or expected_request != row["request_id"]
                    or current["status"] not in {"send_failed", "expired", "revoked"}
                    or manager.store.get_document(REQUEST_NS, requested_id)):
                return {"person": _item(row, operation_id, person, current), "request": current}
        row = {**details, "scope": scope, "role": role, "record_id": person["record_id"],
               "fingerprint": fingerprint, "request_id": requested_id, "recipient": recipient}
        manager.store.put_document(NAMESPACE, key, row)
        request = manager.send({"source": "external", "record_id": person["record_id"],
            "recipient": recipient, "operation_id": row["request_id"]},
            actor=actor, base_url=base_url, send_text=send_text)
        return {"person": _item(row, operation_id, person, request), "request": request}
