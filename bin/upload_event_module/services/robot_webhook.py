import json
import re
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from ..config import config
from ..logger import log_error, log_info
from .feishu_token_manager import FeishuTokenError, TOKEN_ERROR_CODES, token_manager
from .http_client import FeishuHTTPError, FeishuHttpClient


_CHAT_CACHE: Dict[str, Any] = {"timestamp": 0.0, "items": []}
_CHAT_CACHE_TTL_SEC = 300
_HTTP_CLIENT = FeishuHttpClient()
_SENDER_LOCK = threading.Lock()
_AGENT_SENDER = None


@dataclass
class DeliveryAttempt:
    # Only not_sent permits switching applications; uncertain never does.
    status: str
    message: str = ""
    receipt: dict[str, Any] = field(default_factory=dict)
    code: int | None = None


def register_agent_sender(sender):
    global _AGENT_SENDER
    with _SENDER_LOCK:
        _AGENT_SENDER = sender


def unregister_agent_sender(sender):
    global _AGENT_SENDER
    with _SENDER_LOCK:
        if _AGENT_SENDER is sender:
            _AGENT_SENDER = None


def card_requires_agent(value: Any) -> bool:
    """URL buttons and static tables need no callback; forms and actions do."""
    if isinstance(value, list):
        return any(card_requires_agent(item) for item in value)
    if not isinstance(value, dict):
        return False
    tag = str(value.get("tag") or "")
    if (value.get("type") in ("callback", "request") or value.get("form_action_type")
            or tag in {"form", "input", "overflow", "checker"}
            or tag.startswith(("select_", "multi_select", "picker_", "date_picker"))):
        return True
    if tag == "button":
        links = value.get("url") or value.get("multi_url") or any(
            isinstance(item, dict) and item.get("type") == "open_url"
            for item in (value.get("behaviors") or []))
        if "value" in value or not links:
            return True
    return any(card_requires_agent(item) for item in value.values())


def _request_json(
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
    json_payload: Any = None,
    retries: int = 2,
) -> dict[str, Any]:
    if method.upper() == "POST" and url == "https://open.feishu.cn/open-apis/im/v1/messages":
        # Keep HTTP uncertainty: a timeout body may still contain a Feishu error code.
        try:
            response = _HTTP_CLIENT.request(method, url, headers=headers, params=params, json=json_payload)
            status = response.status_code
            if status == 408 or status >= 500 or 300 <= status < 400:
                raise FeishuHTTPError(f"飞书消息响应 HTTP {status}，发送结果未确认", category="remote")
            result = response.json()
            if (not isinstance(result, dict) or type(result.get("code")) is not int
                    or status >= 400 and result["code"] == 0):
                raise FeishuHTTPError("飞书消息响应不完整，发送结果未确认")
            return result
        except FeishuHTTPError:
            raise
        except Exception:
            raise FeishuHTTPError("飞书消息发送结果未确认，未自动重试", category="network") from None
    return _HTTP_CLIENT.request_json(
        method,
        url,
        headers=headers,
        params=params,
        json_payload=json_payload,
        retries=retries,
    )


def _get_tenant_access_token(*, force_refresh: bool = False) -> Tuple[str, str]:
    try:
        return token_manager.get_tenant_token(force_refresh=force_refresh), ""
    except FeishuTokenError as exc:
        return "", str(exc)
    except Exception as exc:
        return "", str(exc)


def _is_token_error_result(result: dict[str, Any]) -> bool:
    try:
        code = int(result.get("code") or 0)
    except (TypeError, ValueError, OverflowError):
        code = -1
    msg = str(result.get("msg") or "").lower()
    return code in TOKEN_ERROR_CODES or "token" in msg or "access_token" in msg


def _get_bot_chats(
    tenant_access_token: str, page_size: int = 100
) -> Tuple[List[Dict[str, Any]], str]:
    url = "https://open.feishu.cn/open-apis/im/v1/chats"
    headers = {
        "Authorization": f"Bearer {tenant_access_token}",
        "Content-Type": "application/json; charset=utf-8",
    }

    chats: List[Dict[str, Any]] = []
    page_token = ""
    seen_page_tokens: set[str] = set()
    has_more = True

    while has_more:
        params: Dict[str, Any] = {"page_size": page_size}
        if page_token:
            params["page_token"] = page_token
        try:
            result = _request_json("GET", url, headers=headers, params=params, retries=2)
            if _is_token_error_result(result):
                tenant_access_token, token_err = _get_tenant_access_token(
                    force_refresh=True
                )
                if token_err:
                    return [], token_err
                headers["Authorization"] = f"Bearer {tenant_access_token}"
                result = _request_json(
                    "GET",
                    url,
                    headers=headers,
                    params=params,
                    retries=2,
                )
            if result.get("code", 0) != 0:
                return [], result.get("msg", "unknown error")
            data = result.get("data", {})
            chats.extend(data.get("items", []))
            has_more = bool(data.get("has_more", False))
            if not has_more:
                continue
            next_page_token = str(data.get("page_token") or "").strip()
            if (
                not next_page_token
                or next_page_token == page_token
                or next_page_token in seen_page_tokens
            ):
                return [], "飞书群列表分页返回缺失或重复的 page_token"
            seen_page_tokens.add(next_page_token)
            page_token = next_page_token
        except FeishuHTTPError as exc:
            return [], str(exc)

    return chats, ""


def _get_chat_cache() -> Tuple[List[Dict[str, Any]], str]:
    now = time.time()
    if _CHAT_CACHE["items"] and now - _CHAT_CACHE["timestamp"] < _CHAT_CACHE_TTL_SEC:
        return _CHAT_CACHE["items"], ""

    token, err = _get_tenant_access_token()
    if err:
        return [], err

    chats, err = _get_bot_chats(token)
    if err:
        return [], err

    _CHAT_CACHE["items"] = chats
    _CHAT_CACHE["timestamp"] = now
    return chats, ""


def _find_chat_id_by_name(target_name: str) -> Tuple[str, str]:
    if not target_name:
        return "", "未配置群名称"

    chats, err = _get_chat_cache()
    if err:
        return "", err

    matches = [c for c in chats if c.get("name") == target_name]
    if not matches:
        return "", f"未找到群: {target_name}"
    if len(matches) > 1:
        log_info(f"群名称重复，默认使用第一个匹配: {target_name}")
    return matches[0].get("chat_id", ""), ""


def _resolve_group_name(notice_type: str, level: str) -> str:
    if notice_type == "变更通告":
        if level == "I3":
            return config.group_name_change_i3
        return ""
    if notice_type == "维保通告":
        return getattr(config, "group_name_maintenance", "")
    if notice_type == "事件通告":
        if level == "I2":
            return config.group_name_event_i2
        if level == "I3":
            return config.group_name_event_i3
        return ""
    return ""


def _send_message_to_chat(
    tenant_access_token: str,
    chat_id: str,
    text: str,
    *,
    message_uuid: str = "",
    receipt: dict[str, Any] | None = None,
    outcome: dict[str, Any] | None = None,
) -> Tuple[bool, str]:
    return _send_message_to_receive_id(
        tenant_access_token,
        chat_id,
        text,
        receive_id_type="chat_id",
        message_uuid=message_uuid,
        receipt=receipt,
        outcome=outcome,
    )


def _send_message_to_open_id(
    tenant_access_token: str, open_id: str, text: str, *, message_uuid: str = "",
    receipt: dict[str, Any] | None = None,
    outcome: dict[str, Any] | None = None,
) -> Tuple[bool, str]:
    return _send_message_to_receive_id(
        tenant_access_token, open_id, text, receive_id_type="open_id", message_uuid=message_uuid,
        receipt=receipt, outcome=outcome
    )


def _send_message_to_receive_id(
    tenant_access_token: str,
    receive_id: str,
    text: str,
    *,
    receive_id_type: str,
    message_uuid: str = "",
    receipt: dict[str, Any] | None = None,
    outcome: dict[str, Any] | None = None,
) -> Tuple[bool, str]:
    if receipt is not None:
        receipt.clear()
    if outcome is not None:
        outcome.clear()
        outcome["status"] = "uncertain"
    url = "https://open.feishu.cn/open-apis/im/v1/messages"
    headers = {
        "Authorization": f"Bearer {tenant_access_token}",
        "Content-Type": "application/json; charset=utf-8",
    }
    payload = {
        "receive_id": receive_id,
        "msg_type": "text",
        "content": json.dumps({"text": text}, ensure_ascii=False),
        # Feishu deduplicates retries carrying the same UUID.  Most callers use
        # a fresh value; durable notice operations pass a stable one.
        "uuid": str(message_uuid or uuid.uuid4()),
    }
    params = {"receive_id_type": receive_id_type}

    try:
        result = _request_json(
            "POST",
            url,
            headers=headers,
            params=params,
            json_payload=payload,
            # The durable caller owns retries; a hidden retry can erase an uncertain write.
            retries=0,
        )
        if type(result.get("code")) is int and result["code"] != 0 and _is_token_error_result(result):
            tenant_access_token, token_err = _get_tenant_access_token(
                force_refresh=True
            )
            if token_err:
                if outcome is not None:
                    outcome.update(status="not_sent", code=result["code"])
                return False, token_err
            headers["Authorization"] = f"Bearer {tenant_access_token}"
            result = _request_json(
                "POST",
                url,
                headers=headers,
                params=params,
                json_payload=payload,
                retries=0,
            )
        if type(result.get("code")) is not int:
            return False, "飞书响应缺少有效错误码，发送结果未确认"
        if result["code"] != 0:
            if outcome is not None:
                outcome.update(status="not_sent", code=result["code"])
            return False, f"{result['code']} - {result.get('msg') or 'unknown error'}"
        if receipt is not None:
            data = result.get("data")
            if not isinstance(data, dict) or not all(
                isinstance(data.get(key), str) and re.fullmatch(pattern, data[key])
                for key, pattern in (("message_id", r"om_[A-Za-z0-9]+"), ("chat_id", r"oc_[A-Za-z0-9]+"))
            ):
                return False, "飞书响应缺少有效消息ID或会话ID，发送结果未确认"
            receipt.update(message_id=data["message_id"], chat_id=data["chat_id"],
                           sender_app_id=str(config.app_id), message_uuid=payload["uuid"], accepted_at=time.time())
        if outcome is not None:
            outcome["status"] = "sent"
        return True, "ok"
    except FeishuHTTPError as exc:
        return False, str(exc)


def _send_interactive_to_receive_id(
    tenant_access_token: str,
    receive_id: str,
    card: dict[str, Any],
    *,
    receive_id_type: str,
    message_uuid: str = "",
    receipt: dict[str, Any] | None = None,
    outcome: dict[str, Any] | None = None,
) -> Tuple[bool, str]:
    if receipt is not None:
        receipt.clear()
    if outcome is not None:
        outcome.clear()
        outcome["status"] = "uncertain"
    url = "https://open.feishu.cn/open-apis/im/v1/messages"
    headers = {
        "Authorization": f"Bearer {tenant_access_token}",
        "Content-Type": "application/json; charset=utf-8",
    }
    payload = {
        "receive_id": receive_id,
        "msg_type": "interactive",
        "content": json.dumps(card, ensure_ascii=False),
        "uuid": str(message_uuid or uuid.uuid4()),
    }
    params = {"receive_id_type": receive_id_type}
    try:
        result = _request_json(
            "POST",
            url,
            headers=headers,
            params=params,
            json_payload=payload,
            retries=0,
        )
        if type(result.get("code")) is int and result["code"] != 0 and _is_token_error_result(result):
            tenant_access_token, token_err = _get_tenant_access_token(
                force_refresh=True
            )
            if token_err:
                if outcome is not None:
                    outcome.update(status="not_sent", code=result["code"])
                return False, token_err
            headers["Authorization"] = f"Bearer {tenant_access_token}"
            result = _request_json(
                "POST",
                url,
                headers=headers,
                params=params,
                json_payload=payload,
                retries=0,
            )
        if type(result.get("code")) is not int:
            return False, "飞书响应缺少有效错误码，发送结果未确认"
        if result["code"] != 0:
            if outcome is not None:
                outcome.update(status="not_sent", code=result["code"])
            return False, f"{result['code']} - {result.get('msg') or 'unknown error'}"
        data = result.get("data")
        if not isinstance(data, dict) or not all(
            isinstance(data.get(field), str) and re.fullmatch(pattern, data[field])
            for field, pattern in (("message_id", r"om_[A-Za-z0-9]+"), ("chat_id", r"oc_[A-Za-z0-9]+"))
        ):
            return False, "飞书响应缺少有效消息ID或会话ID，发送结果未确认"
        if receipt is not None:
            receipt.update(message_id=data["message_id"], chat_id=data["chat_id"],
                           sender_app_id=str(config.app_id), message_uuid=payload["uuid"],
                           accepted_at=time.time())
        if outcome is not None:
            outcome["status"] = "sent"
        return True, "ok"
    except FeishuHTTPError as exc:
        return False, str(exc)


def _deliver_many(kind, content, recipients, receive_type, message_uuid):
    with _SENDER_LOCK:
        sender = _AGENT_SENDER
    # Standalone legacy callers retain their existing upstream write guard.
    blocked_reason = ""
    if sender:
        from lan_bitable_template_portal.portal_service import external_real_write_guard
        guard = external_real_write_guard()
        if not guard.get("real_write_allowed"):
            blocked_reason = guard.get("reason") or "当前运行模式不允许真实飞书消息发送。"
    requires_agent = kind == "interactive" and card_requires_agent(content)
    token, token_error, results = None, "", []
    for recipient in recipients:
        # Preserve singleton UUIDs, and keep batch retry identities stable by recipient.
        identity = (str(uuid.uuid5(uuid.NAMESPACE_URL, message_uuid + ':' + recipient))
                    if message_uuid and len(recipients) > 1 else str(message_uuid or uuid.uuid4()))
        try:
            attempt = (DeliveryAttempt("blocked", blocked_reason) if blocked_reason else
                       sender(recipient, kind, content, receive_type, identity) if sender else
                       DeliveryAttempt("not_sent", "当前进程未注册飞书智能体运行时（例如独立Qt进程），尚未发送。"))
            if not isinstance(attempt, DeliveryAttempt):
                attempt = DeliveryAttempt("uncertain", "公共发送器未返回有效回执，发送结果未确认。")
        except Exception:
            attempt = DeliveryAttempt("uncertain", "公共发送器异常，发送结果未确认，未跨应用补发。")
        ok, message, receipt = attempt.status == "sent", attempt.message, dict(attempt.receipt)
        failure_kind = "" if ok else attempt.status
        code = attempt.code
        if attempt.status == "not_sent" and requires_agent:
            message += " 回调交互卡片必须由飞书智能体发送，已保留失败，未使用备用应用。"
            failure_kind = "agent_required"
        elif attempt.status == "not_sent":
            # Lazy legacy authentication: an unavailable old application cannot block the agent.
            failure_kind, code, receipt, posted = "not_sent", None, {}, False
            if token is None:
                token, token_error = _get_tenant_access_token()
            if token_error:
                ok, message = False, "备用应用认证未就绪，尚未发送。"
            elif sender and not external_real_write_guard().get("real_write_allowed"):
                ok, message = False, "真实消息写入已禁用，尚未发送。"
                failure_kind = "blocked"
            else:
                outcome = {}
                try:
                    claim = getattr(sender, "legacy_started", None)
                    if claim and not claim(recipient, receive_type, identity, str(config.app_id)):
                        ok, message = False, "原任务已绑定其他发送应用，未跨应用补发。"
                    else:
                        posted = True
                        if kind == "interactive":
                            kwargs = ({"receipt": receipt, "outcome": outcome}
                                      if sender or receive_type != "chat_id" else {})
                            ok, message = _send_interactive_to_receive_id(token, recipient, content,
                                receive_id_type=receive_type,
                                message_uuid=identity if kwargs else message_uuid, **kwargs)
                        else:
                            send = _send_message_to_chat if receive_type == "chat_id" else _send_message_to_open_id
                            ok, message = send(token, recipient, content["text"], message_uuid=identity,
                                               receipt=receipt, outcome=outcome)
                        failure_kind = "" if ok else outcome.get("status", "uncertain")
                        code = outcome.get("code") if failure_kind == "not_sent" else None
                        # Preserve the original SDK-style unavailable result used by legacy callers.
                        if not ok and not outcome and "no availability" in message.lower():
                            failure_kind = "not_sent"
                        receipt.update(delivery_channel="legacy", primary_error=attempt.message)
                        finish = getattr(sender, "legacy_finished", None)
                        if ok and finish:
                            finish(recipient, receive_type, identity, dict(receipt))
                        log_info("飞书智能体未发送，已尝试备用应用：" + attempt.message)
                except Exception:
                    ok, message = False, "备用应用发送或回执保存未完成，结果未确认；请继续原任务。"
                    failure_kind, code = "uncertain", None
            if not ok and failure_kind == "not_sent":
                try:
                    mark = getattr(sender, "legacy_not_sent", None)
                    if mark and not mark(recipient, receive_type, identity, posted=posted, code=code):
                        failure_kind, code = "uncertain", None
                        message = "原发送结果仍未确认，未将本次失败视为可跨应用重发。"
                except Exception:
                    failure_kind, code = "uncertain", None
                    message = "未发送状态保存失败，结果待核验；未跨应用补发。"
        if not ok and failure_kind == "not_sent" and "no availability" in message.lower():
            failure_kind = "bot_unavailable"
            message += "。飞书返回机器人对该用户不可用，请核对应用可用范围。"
        results.append({receive_type: recipient, "ok": ok, "message": message,
                        "failure_kind": failure_kind, "message_uuid": identity,
                        **({"code": code} if code is not None else {}), **receipt})
    failed = [item for item in results if not item["ok"]]
    if failed:
        return False, "；".join(item[receive_type] + ": " + item["message"] for item in failed), results
    return True, "ok", results


def send_text_to_open_ids(
    text: str, open_ids: List[str], *, message_uuid: str = ""
) -> Tuple[bool, str, List[Dict[str, Any]]]:
    """发送文本给一个或多个 open_id。任一收件人失败则整体失败。"""
    text = str(text or "").strip()
    recipients = list(dict.fromkeys(str(item or "").strip() for item in open_ids if str(item or "").strip()))
    if not text:
        return False, "消息内容为空", []
    if not recipients:
        return False, "收件人为空", []
    return _deliver_many("text", {"text": text}, recipients, "open_id", message_uuid)


def send_interactive_to_open_ids(
    card: dict[str, Any],
    open_ids: List[str],
    *,
    message_uuid: str = "",
) -> Tuple[bool, str, List[Dict[str, Any]]]:
    """向一个或多个 open_id 发送飞书交互卡片。"""
    if not isinstance(card, dict) or not card:
        return False, "卡片内容为空", []
    recipients = list(dict.fromkeys([str(item or "").strip() for item in open_ids]))
    recipients = [item for item in recipients if item]
    if not recipients:
        return False, "收件人为空", []
    return _deliver_many("interactive", card, recipients, "open_id", message_uuid)


def send_interactive_to_chat_id(
    card: dict[str, Any], chat_id: str, *, message_uuid: str = ""
) -> Tuple[bool, str]:
    """按 chat_id 发送飞书交互卡片。"""
    chat_id = str(chat_id or "").strip()
    if not isinstance(card, dict) or not card:
        return False, "卡片内容为空"
    if not chat_id:
        return False, "群 chat_id 为空"
    ok, message, _ = _deliver_many("interactive", card, [chat_id], "chat_id", message_uuid)
    if ok:
        log_info(f"群交互卡片发送成功: chat_id={chat_id}")
    else:
        log_error(f"群交互卡片发送失败: chat_id={chat_id} - {message}")
    return ok, message


def send_text_to_chat_id(text: str, chat_id: str, *, message_uuid: str = "") -> Tuple[bool, str]:
    """按 chat_id 发送文本群消息。"""
    text = str(text or "").strip()
    chat_id = str(chat_id or "").strip()
    if not text:
        return False, "消息内容为空"
    if not chat_id:
        return False, "群 chat_id 为空"

    ok, message, _ = _deliver_many("text", {"text": text}, [chat_id], "chat_id", message_uuid)
    if ok:
        log_info(f"群消息发送成功: chat_id={chat_id}")
    else:
        log_error(f"群消息发送失败: chat_id={chat_id} - {message}")
    return ok, message


def send_robot_title_and_content(
    title: str,
    content: str,
    notice_type: str,
    level: str,
    *,
    message_uuid: str = "",
) -> Tuple[bool, str]:
    """发送单条消息（标题 + 内容），并根据群名称路由发送。"""
    if not title and not content:
        return False, "标题与内容为空"

    group_name = _resolve_group_name(notice_type, level)
    if not group_name:
        return False, "skip"

    chat_id, err = _find_chat_id_by_name(group_name)
    if err:
        return False, err

    text = f"{title}\n{content}" if title and content else (title or content)
    ok, msg = send_text_to_chat_id(text, chat_id, message_uuid=message_uuid)
    if ok:
        log_info(f"群消息发送成功: {group_name}")
    else:
        log_error(f"群消息发送失败: {group_name} - {msg}")
    return ok, msg


def send_event_prompt_message(building: str, content: str) -> Tuple[bool, str]:
    """发送事件提示群消息（倒计时剩余1分钟提醒）。"""
    group_name = config.group_name_event_prompt
    if not group_name:
        return False, "skip"

    chat_id, err = _find_chat_id_by_name(group_name)
    if err:
        return False, err

    prefix = f"此事件距更新通告超时还剩【1】分钟，请及时更新或补位！事件信息如下："
    text = f"{prefix}\n{content}" if content else prefix
    ok, msg = send_text_to_chat_id(text, chat_id)
    if ok:
        log_info(f"事件提示群消息发送成功: {group_name}")
    else:
        log_error(f"事件提示群消息发送失败: {group_name} - {msg}")
    return ok, msg
