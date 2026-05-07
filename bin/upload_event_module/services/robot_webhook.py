import json
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

import requests

from ..config import config
from ..logger import log_error, log_info


_CHAT_CACHE: Dict[str, Any] = {"timestamp": 0.0, "items": []}
_CHAT_CACHE_TTL_SEC = 300


def _get_tenant_access_token() -> Tuple[str, str]:
    app_id = config.app_id
    app_secret = config.app_secret
    if not app_id or not app_secret:
        return "", "未配置 App ID 或 App Secret"

    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    payload = {"app_id": app_id, "app_secret": app_secret}
    headers = {"Content-Type": "application/json; charset=utf-8"}

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=8.0)
        response.raise_for_status()
        result = response.json()
        if result.get("code", 0) != 0:
            return "", result.get("msg", "unknown error")
        return result.get("tenant_access_token", ""), ""
    except Exception as exc:
        return "", str(exc)


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
    has_more = True

    while has_more:
        params: Dict[str, Any] = {"page_size": page_size}
        if page_token:
            params["page_token"] = page_token
        try:
            response = requests.get(url, headers=headers, params=params, timeout=8.0)
            response.raise_for_status()
            result = response.json()
            if result.get("code", 0) != 0:
                return [], result.get("msg", "unknown error")
            data = result.get("data", {})
            chats.extend(data.get("items", []))
            has_more = data.get("has_more", False)
            page_token = data.get("page_token", "")
        except Exception as exc:
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
    if notice_type in ("设备变更", "变更通告"):
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
    tenant_access_token: str, chat_id: str, text: str
) -> Tuple[bool, str]:
    return _send_message_to_receive_id(
        tenant_access_token, chat_id, text, receive_id_type="chat_id"
    )


def _send_message_to_open_id(
    tenant_access_token: str, open_id: str, text: str
) -> Tuple[bool, str]:
    return _send_message_to_receive_id(
        tenant_access_token, open_id, text, receive_id_type="open_id"
    )


def _send_message_to_receive_id(
    tenant_access_token: str, receive_id: str, text: str, *, receive_id_type: str
) -> Tuple[bool, str]:
    url = "https://open.feishu.cn/open-apis/im/v1/messages"
    headers = {
        "Authorization": f"Bearer {tenant_access_token}",
        "Content-Type": "application/json; charset=utf-8",
    }
    payload = {
        "receive_id": receive_id,
        "msg_type": "text",
        "content": json.dumps({"text": text}, ensure_ascii=False),
        "uuid": str(uuid.uuid4()),
    }
    params = {"receive_id_type": receive_id_type}

    try:
        response = requests.post(
            url, headers=headers, params=params, json=payload, timeout=8.0
        )
        response.raise_for_status()
        result = response.json()
        if result.get("code", 0) != 0:
            return False, result.get("msg", "unknown error")
        return True, "ok"
    except Exception as exc:
        return False, str(exc)


def send_text_to_open_ids(
    text: str, open_ids: List[str]
) -> Tuple[bool, str, List[Dict[str, Any]]]:
    """发送文本给一个或多个 open_id。任一收件人失败则整体失败。"""
    text = str(text or "").strip()
    recipients = list(dict.fromkeys([str(open_id or "").strip() for open_id in open_ids]))
    recipients = [open_id for open_id in recipients if open_id]
    if not text:
        return False, "消息内容为空", []
    if not recipients:
        return False, "收件人为空", []

    token, err = _get_tenant_access_token()
    if err:
        return False, err, []

    results: List[Dict[str, Any]] = []
    all_ok = True
    for open_id in recipients:
        ok, msg = _send_message_to_open_id(token, open_id, text)
        results.append({"open_id": open_id, "ok": ok, "message": msg})
        if not ok:
            all_ok = False

    if all_ok:
        log_info(f"个人消息发送成功: recipients={len(recipients)}")
        return True, "ok", results
    failed = [item for item in results if not item.get("ok")]
    detail = "；".join(
        f"{item.get('open_id')}: {item.get('message')}" for item in failed
    )
    log_error(f"个人消息发送失败: {detail}")
    return False, detail or "发送失败", results


def send_robot_title_and_content(
    title: str,
    content: str,
    notice_type: str,
    level: str,
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

    token, err = _get_tenant_access_token()
    if err:
        return False, err

    text = f"{title}\n{content}" if title and content else (title or content)
    ok, msg = _send_message_to_chat(token, chat_id, text)
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

    token, err = _get_tenant_access_token()
    if err:
        return False, err

    prefix = f"此事件距更新通告超时还剩【1】分钟，请及时更新或补位！事件信息如下："
    text = f"{prefix}\n{content}" if content else prefix
    ok, msg = _send_message_to_chat(token, chat_id, text)
    if ok:
        log_info(f"事件提示群消息发送成功: {group_name}")
    else:
        log_error(f"事件提示群消息发送失败: {group_name} - {msg}")
    return ok, msg
