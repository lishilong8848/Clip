"""Permission cards use the same authoritative review methods as the web page."""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import re
import time

from fastapi import Request
from openclaw_service.assistant.lighthouse_ai import AssistantError


def parse_event(payload, app_id):
    if not isinstance(payload, dict):
        return None
    header, event = payload.get("header") or {}, payload.get("event") or {}
    if not isinstance(header, dict) or not isinstance(event, dict):
        return None
    action = event.get("action") or {}
    if not isinstance(action, dict):
        return None
    value = action.get("value") or {}
    if not isinstance(value, dict) or value.get("kind") != "permission_review":
        return None
    operator, context = event.get("operator") or {}, event.get("context") or {}
    if not isinstance(operator, dict) or not isinstance(context, dict):
        return None
    if header.get("app_id") != app_id or header.get("event_type") != "card.action.trigger":
        return None
    fields = action.get("form_value") or {}
    if not isinstance(fields, dict) or set(fields) - {"reason"} or not isinstance(fields.get("reason", ""), str):
        return None
    uid, oid, chat, mid = operator.get("union_id", ""), operator.get("open_id", ""), context.get("open_chat_id", ""), context.get("open_message_id", "")
    if not all(isinstance(v, str) and re.fullmatch(pattern, v) for v, pattern in (
        (uid, r"on_[A-Za-z0-9]+"), (oid, r"ou_[A-Za-z0-9]+"), (chat, r"oc_[A-Za-z0-9]+"), (mid, r"om_[A-Za-z0-9]+"),
        (value.get("request_id"), r"[A-Za-z0-9_-]{16,80}"), (value.get("token"), r"[A-Za-z0-9_-]{40,64}"))):
        return None
    event_id = header.get("event_id")
    if not isinstance(event_id, str) or not event_id or len(event_id) > 128 or value.get("action") not in ("approve", "reject") or len(fields.get("reason", "")) > 500:
        return None
    return {"kind": "permission_review", "message_id": "prv_" + hashlib.sha256((event_id + uid + mid).encode()).hexdigest()[:32],
            "reply_to_message_id": mid, "chat_id": chat, "chat_type": "p2p", "sender_id": oid, "union_id": uid,
            "question": "[权限申请审批]", "review": {**value, "reason": fields.get("reason", "").strip()}}


def card(data):
    from .feishu_card_lifetime import expired, expired_card
    if data.get('status') == 'pending' and expired(data.get('created_at_ts') or data.get('created_at'), deadline=data.get('expires_at_ts')):
        return expired_card()
    plain = lambda text: {"tag": "plain_text", "content": str(text)}
    elements = [{"tag": "div", "text": plain("申请人：" + str(data.get("name") or "用户"))},
                {"tag": "div", "text": plain("申请范围：" + "、".join(data.get("requested_scopes") or []))},
                {"tag": "div", "text": plain("申请原因：" + str(data.get("reason") or "未填写"))},
                {"tag": "div", "text": plain("提交时间：" + str(data.get("created_at") or ""))}]
    status = data.get("status")
    access = data.get('requested_access')
    if isinstance(access, dict):
        from .account_policy import MODULE_OPTIONS, CATEGORY_OPTIONS
        module_labels = {item['value']:item['label'] for item in MODULE_OPTIONS}
        category_labels = {item['value']:item['label'] for item in CATEGORY_OPTIONS}
        elements[2:2] = [
            {'tag':'div','text':plain('目标级别：'+str(access.get('level') or '')+' · 岗位：'+category_labels.get(access.get('category'),str(access.get('category') or '')))},
            {'tag':'div','text':plain('申请模块：'+'、'.join(module_labels.get(v,v) for v in access.get('modules',[])))}]
        elements.append({'tag':'div','text':plain('通过后以上述完整权限生效，未选择的模块不授权。')})
    if status == "pending":
        buttons = []
        for action, title, style in (("approve", "通过申请", "primary"), ("reject", "拒绝申请", "danger")):
            buttons.append({"tag": "button", "name": action, "text": plain(title), "type": style, "form_action_type": "submit",
                            "value": {"kind": "permission_review", "request_id": data["request_id"], "token": data["review_card_token"], "action": action},
                            "confirm": {"title": plain(title), "text": plain("请核对申请人及申请范围，确认后立即生效。" if action == "approve" else "确认拒绝本次申请？")}})
        elements.append({"tag": "form", "name": "permission_review", "elements": [
            {"tag": "input", "name": "reason", "label": plain("审批意见"), "placeholder": plain("拒绝时请填写原因"), "max_length": 500},
            {"tag": "column_set", "flex_mode": "flow", "columns": [{"tag": "column", "width": "weighted", "weight": 1, "elements": [button]} for button in buttons]}]})
    else:
        label = {"approved": "已通过", "rejected": "已拒绝", "superseded": "已被新申请替代"}.get(status, "已失效")
        elements.append({"tag": "div", "text": plain(label + ("：" + data["reject_reason"] if data.get("reject_reason") else ""))})
    return {"schema": "2.0", "header": {"title": plain("灯塔 · 权限申请"), "template": "blue"}, "body": {"elements": elements}}


class PermissionCards:
    def __init__(self, bot):
        self.bot, self.tasks = bot, set()
        self.pending = set()
        self.slots = asyncio.Semaphore(2)

    def request(self, request_id):
        with self.bot.runtime.auth_manager._lock:
            return self.bot.runtime.state_store.get_permission_request(request_id) or {}

    def update(self, request_id, **fields):
        auth = self.bot.runtime.auth_manager
        with auth._lock:
            data = self.request(request_id)
            if data:
                data.update(fields)
                auth._state_store.put_permission_request(data)

    def union_id(self, open_id):
        service = self.bot.runtime.service
        from upload_event_module.config import config
        from .feishu_delivery import current_recipient_open_id
        app_id = str(getattr(self.bot, 'config', {}).get('app_id') or config.app_id or '')
        current_id = current_recipient_open_id(self.bot.runtime, open_id, app_id)
        result = service._request_payload("GET", "https://open.feishu.cn/open-apis/contact/v3/users/" + current_id,
            context="权限审批人身份核对", headers=service._auth_headers(), params={"user_id_type": "open_id"})
        user = (result.get("data") or {}).get("user") or {}
        if result.get("code") or user.get("open_id") != current_id or not re.fullmatch(r"on_[A-Za-z0-9]+", str(user.get("union_id") or "")):
            raise AssistantError("无法核对审批人的飞书身份，请该管理员先登录灯塔。", 400)
        return user["union_id"]

    def enqueue(self, request_id):
        if request_id in self.pending:
            return
        self.pending.add(request_id)
        async def send():
            try:
                async with self.slots:
                    await self.notify(request_id)
            finally:
                self.pending.discard(request_id)
        task = asyncio.create_task(send())
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    async def recover(self):
        pending = await asyncio.to_thread(self.bot.runtime.auth_manager.list_permission_requests_for_admin, status="pending", limit=500)
        for row in pending:
            if row.get("approver_open_id") and row.get("notification_status") in {"queued", "failed"}:
                self.enqueue(row["request_id"])

    def enqueue_update(self, request_id):
        async def update():
            await self.update_card(await asyncio.to_thread(self.request, request_id))
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        task = loop.create_task(update())
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    async def notify(self, request_id):
        try:
            data = await asyncio.to_thread(self.request, request_id)
            from .feishu_card_lifetime import expired
            if expired(data.get('created_at_ts') or data.get('created_at'), deadline=data.get('expires_at_ts')):
                await asyncio.to_thread(self.update, request_id, notification_status='expired', notification_error='卡片超过7天，停止重试；请在灯塔处理。')
                return
            if data.get("status") != "pending" or not data.get("approver_open_id") or data.get("notification_status") == "sent":
                return
            if data.get("expires_at_ts") and time.time() >= float(data["expires_at_ts"]):
                raise AssistantError("申请已过期，请重新提交。", 400)
            if not await asyncio.to_thread(self.bot.runtime.auth_manager.can_review_permission_request, data['open_id'], data['approver_open_id']):
                raise AssistantError("所选审批人的管理员权限已变化，请选择其他审批人重新申请。", 400)
            from .portal_service import external_real_write_guard
            if not external_real_write_guard()["real_write_allowed"]:
                raise AssistantError("当前模式不发送真实审批卡片。", 403)
            messenger = self.bot.messenger
            if not messenger:
                raise AssistantError("飞书智能体未连接，申请仍可在网页审批。", 503)
            current = await asyncio.to_thread(self.request, request_id)
            if current.get("status") != "pending":
                return
            from .feishu_delivery import send_agent_message
            receipt = await send_agent_message(self.bot.runtime, data['approver_open_id'], card(data),
                message_uuid=hashlib.sha256(('permission:' + request_id).encode()).hexdigest()[:32])
            mid = receipt['message_id']
            await asyncio.to_thread(self.update, request_id, notification_status="sent", notification_error="",
                review_message_id=mid, review_sender_app_id=receipt['sender_app_id'])
            current = await asyncio.to_thread(self.request, request_id)
            if current.get("status") != "pending":
                await self.update_card(current)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logging.warning("Permission card notification failed: %s", type(exc).__name__)
            await asyncio.to_thread(self.update, request_id, notification_status="failed", notification_error=str(exc) if isinstance(exc, AssistantError) else "飞书通知失败，申请仍可在网页审批。")

    async def handle(self, message):
        user = await self.bot._resolve_user(message)
        value = message["review"]
        auth = self.bot.runtime.auth_manager
        def decide():
            with auth._lock:
                data = self.request(value["request_id"])
                if not data or not hmac.compare_digest(str(data.get("review_card_token") or ""), value["token"]) or data.get("approver_open_id") != user["open_id"] or not auth.can_review_permission_request(data.get('open_id'), user['open_id']):
                    raise AssistantError("只有所选且仍具备管理员权限的审批人能处理该卡片；其他管理员可在网页审批。", 403)
                if data.get("review_message_id") and data["review_message_id"] != message["reply_to_message_id"]:
                    raise AssistantError("审批卡片标识不一致，未更改权限。", 403)
                applied = data.get("status") == "pending"
                if applied:
                    from .feishu_card_lifetime import expired, EXPIRED_CARD_MESSAGE
                    if expired(data.get('created_at_ts') or data.get('created_at'), deadline=data.get('expires_at_ts')):
                        raise AssistantError(EXPIRED_CARD_MESSAGE, 410)
                    if value["action"] == "approve":
                        auth.approve_permission_request(data["request_id"], updated_by=user["open_id"])
                    else:
                        if not value["reason"]:
                            raise AssistantError("请填写拒绝原因后再提交。", 400)
                        auth.reject_permission_request(data["request_id"], reason=value["reason"], updated_by=user["open_id"])
                return self.request(data["request_id"]), applied
        from .portal_service import PortalError
        from .account_policy import PolicyViolation
        try:
            data, applied = await asyncio.to_thread(decide)
        except (PortalError, PolicyViolation) as exc:
            raise AssistantError(str(exc), 409) from exc
        self.bot.controller._clear_read_cache(("auth_status",))
        await self.update_card(data)
        if applied and data.get("status") in {"approved", "rejected"}:
            # A failed result notification must not roll back a successful permission decision.
            try:
                await asyncio.to_thread(self.bot.controller._notify_permission_review_result, auth._public_permission_request(data),
                    approved=data["status"] == "approved", actor_name=user.get("name") or "管理员", reason=data.get("reject_reason") or "")
            except Exception as exc:
                logging.warning("Permission result notification failed: %s", type(exc).__name__)
        label = {"approved": "已通过，权限已同步生效", "rejected": "已拒绝", "superseded": "已由新申请替代"}.get(data.get("status"), "已失效")
        return {"text": "权限申请" + label + "。", "card": card(data)}

    async def update_card(self, data):
        from .feishu_card_lifetime import expired
        if expired(data.get('created_at_ts') or data.get('created_at'), deadline=data.get('expires_at_ts')):
            return
        if not data.get("review_message_id") or not self.bot.messenger:
            return
        if data.get('review_sender_app_id') and data['review_sender_app_id'] != getattr(self.bot, 'config', {}).get('app_id'):
            return
        try:
            from .portal_service import external_real_write_guard
            if not external_real_write_guard()["real_write_allowed"]:
                return
            client = await self.bot.messenger.client()
            response = await client.patch("https://open.feishu.cn/open-apis/im/v1/messages/" + data["review_message_id"],
                headers=await self.bot.messenger.headers(), json={"content": json.dumps(card(data), ensure_ascii=False)})
            if response.json().get("code") != 0:
                raise ValueError("card update rejected")
        except Exception as exc:
            logging.warning("Permission result card update failed: %s", type(exc).__name__)

    async def close(self):
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)


def install(app, bot):
    service = PermissionCards(bot)
    bot.permission_cards = service
    bot.controller._permission_cards = service
    @app.get("/api/auth/permission-approvers")
    async def approvers(request: Request):
        session = bot.controller._current_session(request)
        if session is None:
            return bot.controller._auth_required_response()
        def options():
            auth = bot.runtime.auth_manager
            from .account_policy import MODULE_OPTIONS, category_options_for, level_options_for
            oid = (session.get('user') or {}).get('open_id')
            ids = set(auth.permission_approver_ids(oid))
            items = []
            for row in auth.get_permissions_payload()['users']:
                if row['open_id'] not in ids or not row['enabled']: continue
                profile = auth.account_profile(row['open_id'])
                items.append({'open_id':row['open_id'], 'name':row['name'] or '上级审批人', 'level':profile['level'],
                    'scopes':profile['scope_rules'], 'modules':profile['modules'],
                    'level_options':level_options_for(profile['level']), 'category_options':category_options_for(profile['level'])})
            items.sort(key=lambda item:(-int(item['level'][1:]), item['name']))
            return {'items':items, 'current':auth.account_profile(oid), 'module_options':list(MODULE_OPTIONS),
                    'scope_options':auth.get_permissions_payload()['scope_options']}
        return bot.controller._json_ok(request, session, await asyncio.to_thread(options))
    @app.post("/api/auth/permission-requests/{request_id}/notify")
    async def retry_notify(request_id: str, request: Request):
        session = bot.controller._current_session(request)
        if session is None:
            return bot.controller._auth_required_response()
        data = await asyncio.to_thread(service.request, request_id)
        if data.get("open_id") != (session.get("user") or {}).get("open_id") or data.get("status") != "pending" or not data.get("approver_open_id"):
            from fastapi.responses import JSONResponse
            return JSONResponse({"ok": False, "error": "只能重试本人待审批申请的通知。"}, status_code=403)
        if data.get("notification_status") != "sent":
            await asyncio.to_thread(service.update, request_id, notification_status="queued", notification_error="")
            service.enqueue(request_id)
        return bot.controller._json_ok(request, session, {"queued": True})
    app.add_event_handler("shutdown", service.close)
    return service
