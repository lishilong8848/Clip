"""Interpret the event delivery extension of the local Qt operation endpoint."""


def qt_event_delivery_reply(operation, operation_id, record_id):
    result = (operation or {}).get("result") or {}
    if not result.get("qt_event_delivery"):
        return None
    status = operation.get("status")
    receipt = result.get("qt_result")
    if status in {"completed", "failed"} and isinstance(receipt, dict) and "ok" in receipt:
        return dict(receipt)
    if status == "failed":
        return {"ok": False, "qt_event_delivery": True, "record_id": record_id,
                "message": operation.get("error") or "群消息发送失败，可重试。"}
    return {"queued": True, "event_delivery_pending": True, "job_id": operation_id,
            "event_wait_attempt": 0, "record_id": record_id, "message": "正在发送事件群消息"}
