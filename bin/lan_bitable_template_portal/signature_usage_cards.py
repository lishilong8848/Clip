"""Feishu and web decisions use the same signature-use confirmation row."""
import asyncio
import hashlib
import json
import logging
import re
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote

from .feishu_card_lifetime import expired, EXPIRED_CARD_MESSAGE, MAX_CARD_AGE


DELIVERY_NS = "mop_signature_usage_delivery"
RECIPIENT_NS = "mop_signature_usage_delivery_recipient"
_delivery_lock = threading.RLock()
_running_stores = set()


def _delivery_public(job):
    if job.get("management"):
        return {
            "delivery_id": job["id"], "status": job["status"],
            "result": job.get("result"), "error": job.get("error", ""),
        }
    results = list(job["results"].values())
    sent = sum(item["ok"] for item in results)
    uncertain = sum(item["failure_kind"] == "uncertain" for item in results)
    return {
        "delivery_id": job["id"], "status": job["status"], "sent_count": sent,
        "failed_count": len(results) - sent - uncertain, "uncertain_count": uncertain,
        "results": results, "skipped": job.get("skipped", []), "error": job.get("error", ""),
    }


def get_delivery(service, identity, owner):
    job = service._state_store.get_document(DELIVERY_NS, identity)
    if not owner or not job or job.get("owner", job.get("request", {}).get("operator_open_id")) != owner:
        from .portal_service import PortalNotFoundError
        raise PortalNotFoundError("签名确认发送任务不存在或无权查看。")
    return _delivery_public(job)


def enqueue_management_delivery(service, operation, payload, *, actor, base_url):
    from upload_event_module.config import config
    from .signature_management import SignatureManagementError, _id

    if not actor:
        raise SignatureManagementError("请先登录。", 401)
    fields = ("scope", "notice_key", "role", "recipient", "name", "employee_no", "specialty",
              "operation_id", "send_operation_id", "expected_request_id") if operation == "mop-invite" else (
        "source", "record_id", "recipient", "operation_id",
    )
    payload = {key: payload[key] for key in fields if key in payload}
    operation_id = _id(payload.get("send_operation_id") if operation == "mop-invite" else payload.get("operation_id"))
    identity = hashlib.sha256(json.dumps([actor, operation, operation_id]).encode()).hexdigest()
    store = service._state_store
    with _delivery_lock:
        job = store.get_document(DELIVERY_NS, identity)
        if not job:
            job = {"id": identity, "status": "queued", "management": operation, "owner": actor,
                   "app_id": str(config.app_id or ""), "created_at": time.time(),
                   "request": payload, "base_url": base_url}
            store.put_document(DELIVERY_NS, identity, job)
        elif payload != job["request"]:
            raise SignatureManagementError("原签名请求内容已变化，请先核对原请求。", 409)
        elif job["status"] == "failed":
            job.update(status="queued", error="")
            store.put_document(DELIVERY_NS, identity, job)
        response = _delivery_public(job)
    resume_deliveries(service)
    return response


def enqueue_delivery(service, **request):
    from upload_event_module.config import config
    from .portal_service import PortalError

    roles = sorted({
        (str(item.get("record_id") or "").strip(), str(item.get("role") or "").strip())
        for item in request["signatures"]
        if str(item.get("source") or "staff").strip() in {"", "staff"}
    })
    roles = [(record, role) for record, role in roles
             if record and role in {"implementer", "auditor", "inspector"}]
    if not roles or not request["notice_key"] or not request["operator_open_id"]:
        raise PortalError("请先选择事项和需要确认签名的人员。")
    request["signatures"] = [{"record_id": record, "role": role, "source": "staff"} for record, role in roles]
    key = [request[name] for name in (
        "operator_open_id", "scope", "notice_key", "context_type", "mop_attachment_name", "signatures",
    )]
    identity = hashlib.sha256(json.dumps(key, sort_keys=True).encode()).hexdigest()
    store = service._state_store
    with _delivery_lock:
        job = store.get_document(DELIVERY_NS, identity)
        if not job or time.time() >= job["created_at"] + MAX_CARD_AGE:
            job = {"id": identity, "status": "queued", "app_id": str(config.app_id or ""),
                   "created_at": time.time(), "request": request, "results": {}}
            store.put_document(DELIVERY_NS, identity, job)
        elif job["status"] == "failed":
            job.update(status="queued", error="")
            store.put_document(DELIVERY_NS, identity, job)
        response = _delivery_public(job)
    resume_deliveries(service)
    return response


def _deliver_item(item, app_id):
    from upload_event_module.config import config

    if str(config.app_id or "") != app_id:
        return {"record_id": item["record_id"], "name": item.get("name", ""), "ok": False,
                "message": "飞书应用已变更，请核对原发送记录。", "failure_kind": "blocked"}
    try:
        ok, message, results = send(item)
        result = next(iter(results or []), {})
    except Exception:
        ok, message, result = False, "发送结果未确认，请核对原卡片。", {}
    ok = bool(ok and result.get("ok", ok))
    return {
        "record_id": item["record_id"], "name": item.get("name", ""), "ok": ok,
        "message": str(result.get("message") or message or ""),
        "failure_kind": "" if ok else str(result.get("failure_kind") or "uncertain"),
    }


def _prepare_delivery(service, job):
    request = job["request"]
    store = service._state_store
    group = [job["app_id"], *[request[name] for name in (
        "operator_open_id", "scope", "notice_key", "context_type", "mop_attachment_name",
    )]]
    roles = {}
    for item in request["signatures"]:
        roles.setdefault(item["record_id"], []).append(item["role"])
    keys = {record: hashlib.sha256(json.dumps([group, record, sorted(names)]).encode()).hexdigest()
            for record, names in roles.items()}
    messages, fresh = [], set(keys)
    for record, key in keys.items():
        receipt = store.get_document(RECIPIENT_NS, key) or {}
        if not receipt or time.time() >= receipt["expires_at"]:
            continue
        prior = store.get_document(DELIVERY_NS, receipt["delivery_id"]) or {}
        result = prior.get("results", {}).get(record)
        if result and result["ok"]:
            job["results"][record] = result
        else:
            item = next((item for item in prior.get("messages", []) if item["record_id"] == record and item.get("card")), None)
            if item is None:
                continue
            messages.append(item)
        fresh.remove(record)
    prepared = service.build_signature_usage_confirmation_messages(
        **{**request, "signatures": [item for item in request["signatures"] if item["record_id"] in fresh]}
    ) if fresh else {}
    new_messages = [
        {**{name: item[name] for name in ("record_id", "open_id", "confirmation_id", "card")},
         "name": (item.get("person") or {}).get("name", "")}
        for item in prepared.get("messages", [])
    ]
    job.update(messages=[*messages, *new_messages], skipped=prepared.get("skipped", []))
    documents = {(DELIVERY_NS, job["id"]): job}
    for item in job["messages"]:
        key = keys[item["record_id"]]
        old = store.get_document(RECIPIENT_NS, key) or {}
        expires_at = old["expires_at"] if item["record_id"] not in fresh else job["created_at"] + MAX_CARD_AGE
        documents[(RECIPIENT_NS, key)] = {"delivery_id": job["id"], "expires_at": expires_at}
    store.put_documents_atomic(documents)


def _run_delivery(service, job):
    from upload_event_module.config import config
    from .portal_service import external_real_write_guard, PortalError

    store = service._state_store
    identity = job["id"]
    try:
        if str(config.app_id or "") != job["app_id"]:
            raise PortalError("飞书应用已变更，请核对原发送记录。")
        if not external_real_write_guard()["real_write_allowed"]:
            raise PortalError("当前环境未启用真实飞书发送。")
        if job.get("management"):
            _run_management_delivery(service, job)
            store.put_document(DELIVERY_NS, identity, job)
            return
        if "messages" not in job:
            _prepare_delivery(service, job)
        pending = [item for item in job["messages"] if not job["results"].get(item["record_id"], {}).get("ok")]
        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = {pool.submit(_deliver_item, item, job["app_id"]): item for item in pending}
            for future in as_completed(futures):
                result = future.result()
                job["results"][result["record_id"]] = result
                if result["ok"]:
                    futures[future].pop("card", None)
                store.put_document(DELIVERY_NS, identity, job)
        failures = any(not item["ok"] for item in job["results"].values())
        job.update(status="failed" if failures else "completed",
                   error="部分签名确认卡片未送达或结果待核对。" if failures else "")
        if not failures:
            # The rendered action token is only needed while retrying delivery.
            job.pop("messages", None)
    except Exception as exc:
        logging.warning("Signature confirmation delivery %s failed: %s", identity[:12], type(exc).__name__)
        job.update(status="failed", error=str(exc) if isinstance(exc, PortalError)
                   else "签名卡片数据暂未读取或发送未完成，请继续原任务。")
    store.put_document(DELIVERY_NS, identity, job)


def _run_management_delivery(service, job):
    from .server import PortalRuntime, _send_text_to_open_ids_guarded
    from .signature_management import SignatureManagementError, dispatch

    try:
        result = dispatch(service.signature_management, "POST", job["management"], job["request"],
                          actor=job["owner"], base_url=job["base_url"], send_text=_send_text_to_open_ids_guarded,
                          auth=PortalRuntime.auth_manager)
    except SignatureManagementError as exc:
        job.update(status="failed", error=str(exc))
        return
    request = result.get("request", result)
    if request.get("status") == "sending":
        message = "原签名链接的投递结果未确认，请先核对接收情况。"
        request.update(status="send_unknown", error=message)
        if result.get("person"):
            result["person"].update(request_status="send_unknown", signature_reason=message)
    job.update(status="completed", result=result, error="")


def resume_deliveries(service):
    store = service._state_store
    key = str(store.db_path)
    with _delivery_lock:
        if key in _running_stores:
            return
        _running_stores.add(key)

    def drain():
        try:
            while True:
                with _delivery_lock:
                    jobs = [row["payload"] for row in store.list_documents(DELIVERY_NS)
                            if row["payload"]["status"] in {"queued", "running"}]
                    if not jobs:
                        _running_stores.discard(key)
                        return
                    job = min(jobs, key=lambda value: value["created_at"])
                    lifetime = 86400 if job.get("management") else MAX_CARD_AGE
                    if time.time() >= job["created_at"] + lifetime:
                        job.update(status="expired", error="原请求已过期，请重新发起。")
                        job.pop("messages", None)
                        store.put_document(DELIVERY_NS, job["id"], job)
                        continue
                    job["status"] = "running"
                    store.put_document(DELIVERY_NS, job["id"], job)
                _run_delivery(service, job)
        except Exception:
            logging.exception("Signature confirmation background queue stopped")
            with _delivery_lock:
                _running_stores.discard(key)

    try:
        threading.Thread(target=drain, name="SignatureUsageDelivery", daemon=True).start()
    except RuntimeError:
        with _delivery_lock:
            _running_stores.discard(key)
        logging.exception("Could not start signature delivery worker; saved jobs remain queued")


def render(row, token, link):
    payload = row.get('payload') or {}
    status = row.get('status') or 'pending'
    if status == 'pending' and expired(row.get('created_at'), deadline=row.get('expires_at')):
        status = 'expired'
    plain = lambda text: {'tag': 'plain_text', 'content': str(text)}
    title = '重保检查签名使用确认' if payload.get('context_type') == 'critical_guard' else '维护单签名使用确认'
    lines = [f"签名人员：{row.get('signer_name') or ''}", f"签名角色：{payload.get('role_text') or ''}",
             f"事项：{payload.get('notice_title') or ''}", f"附件：{payload.get('mop_attachment_name') or '当前附件'}",
             f"发起人：{row.get('requested_by_name') or ''}"]
    message = {'confirmed': '已确认使用签名', 'rejected': '已拒绝使用签名',
               'expired': EXPIRED_CARD_MESSAGE}.get(status, '请确认是否允许本次事项使用你的已保存签名。卡片及确认链接最长7天有效，处理后不能重复决定。')
    elements = [{'tag': 'div', 'text': plain('\n'.join(lines))}, {'tag': 'div', 'text': plain(message)}]
    if status == 'pending':
        for action, label, style in [('confirmed', '确认使用签名', 'primary'), ('rejected', '拒绝', 'danger')]:
            elements.append({'tag': 'button', 'text': plain(label), 'type': style,
                'behaviors': [{'type': 'callback', 'value': {'kind': 'signature_usage', 'token': token,
                              'confirmation_id': row['confirmation_id'], 'decision': action}}]})
    if link:
        elements.append({'tag': 'button', 'text': plain('打开灯塔处理'), 'type': 'default',
                         'behaviors': [{'type': 'open_url', 'default_url': link}]})
    return {'schema': '2.0', 'header': {'title': plain(title), 'template': 'blue' if status == 'pending' else 'grey'},
            'body': {'elements': elements}}


def send(item):
    from upload_event_module.services.robot_webhook import send_interactive_to_open_ids
    if not item.get('card') or not item.get('confirmation_id'):
        return False, '签名授权卡片尚未生成，未发送。', []
    return send_interactive_to_open_ids(item['card'], [item['open_id']],
                                       message_uuid=str(uuid.uuid5(uuid.NAMESPACE_URL, 'signature-usage:' + item['confirmation_id'])))


def apply(runtime, data, app_id):
    header, event = data.get('header') or {}, data.get('event') or {}
    value = (event.get('action') or {}).get('value') or {}
    sender = (event.get('operator') or {}).get('open_id') or ''
    if (header.get('app_id') != app_id or header.get('event_type') != 'card.action.trigger'
            or value.get('kind') != 'signature_usage' or not re.fullmatch(r'ou_[A-Za-z0-9]+', sender)
            or not re.fullmatch(r'[A-Za-z0-9_-]{43}', str(value.get('token') or ''))
            or value.get('decision') not in {'confirmed', 'rejected'}):
        raise ValueError('签名确认卡片回调无效，未更改授权。')
    store = runtime.state_store
    row = store.get_mop_signature_usage_confirmation(token=value['token'], allow_expired=True)
    if (row['confirmation_id'] != value.get('confirmation_id') or row['signer_open_id'] != sender
            or row['payload'].get('app_id') != app_id):
        raise ValueError('只有原卡片指定的签名人员本人可以处理，转发卡片不能代为授权。')
    from .feishu_delivery import current_recipient_open_id
    current_recipient_open_id(runtime, sender, app_id)
    if runtime.auth_manager._open_id_explicitly_disabled(sender):
        raise ValueError('当前账号已停用，未更改授权。')
    is_expired = expired(row['created_at'], deadline=row['expires_at'])
    if row['status'] == 'pending' and not is_expired:
        row = store.decide_mop_signature_usage(token=value['token'], decision=value['decision'],
                                               expected_signer=sender, confirmation_id=row['confirmation_id'])
    link = str(row['payload'].get('public_base_url') or '')
    link = link.rstrip('/') + '/api/signatures/usage-confirm?token=' + quote(value['token'], safe='') if link else ''
    toast = {'confirmed': '已确认使用签名', 'rejected': '已拒绝使用签名'}.get(row['status'], EXPIRED_CARD_MESSAGE)
    return {'ok': True, 'toast': toast, 'card': render(row, value['token'], link)}


async def callback(runtime, data, app_id):
    try:
        return await asyncio.wait_for(asyncio.to_thread(apply, runtime, data, app_id), timeout=2)
    except asyncio.TimeoutError:
        return {'ok': False, 'toast': '结果尚待确认，请重试原卡片或打开灯塔核对。'}
    except Exception as exc:
        from .feishu_delivery import FeishuDeliveryError
        return {'ok': False, 'toast': str(exc) if isinstance(exc, (ValueError, FeishuDeliveryError))
                else '暂未完成确认，请重试原卡片或打开灯塔核对。'}
