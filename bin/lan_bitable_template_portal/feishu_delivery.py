"""Bridge synchronous public notifications to the portal's existing agent client."""
from __future__ import annotations

import asyncio
from concurrent.futures import TimeoutError as FutureTimeout
import hashlib
import json
import re
import threading
import time
import uuid
from weakref import WeakValueDictionary

from upload_event_module.services.robot_webhook import (
    DeliveryAttempt, register_agent_sender, unregister_agent_sender,
)


class FeishuDeliveryError(RuntimeError):
    def __init__(self, message, *, uncertain=False, code=None):
        super().__init__(message)
        self.uncertain = uncertain
        self.code = code


async def send_agent_message(runtime, receive_id, content, *, receive_id_type='open_id',
                             msg_type='interactive', message_uuid='') -> dict:
    """Agent-only async API. Personal IDs are portal IDs, never foreign bot IDs."""
    sender = getattr(runtime, 'feishu_public_sender', None)
    if sender is None or sender.closing or sender.loop is not asyncio.get_running_loop():
        raise FeishuDeliveryError('飞书智能体公共发送器尚未就绪，未使用备用应用。')
    if receive_id_type not in {'open_id', 'chat_id'} or msg_type not in {'text', 'interactive'}:
        raise FeishuDeliveryError('公共发送仅支持人员或原群的文本、卡片消息。')
    if not isinstance(content, dict) or not content:
        raise FeishuDeliveryError('消息内容为空或格式无效。')
    attempt = await sender.send(receive_id, msg_type, content, receive_id_type,
                                str(message_uuid or uuid.uuid4()))
    if attempt.status != 'sent':
        raise FeishuDeliveryError(attempt.message, uncertain=attempt.status == 'uncertain', code=attempt.code)
    if attempt.receipt.get('delivery_channel') != 'agent':
        raise FeishuDeliveryError('原任务已由备用应用处理，智能体专用接口未跨应用重发。')
    return attempt.receipt


class PublicFeishuSender:
    def __init__(self, runtime, *, timeout=60):
        self.runtime, self.timeout = runtime, timeout
        self.loop, self.closing, self.tasks = None, False, set()
        self.route_lock, self.locks = threading.RLock(), WeakValueDictionary()

    @staticmethod
    def route_key(receive_id, receive_type, message_uuid):
        return hashlib.sha256(json.dumps([receive_type, receive_id, message_uuid]).encode()).hexdigest()

    def route(self, key):
        return self.runtime.state_store.get_document('feishu_public_delivery', key) or {}

    def save_route(self, key, value):
        self.runtime.state_store.put_document('feishu_public_delivery', key, value)

    def claim(self, key, channel, app_id, fingerprint='', target=None):
        with self.route_lock:
            previous = self.route(key)
            if previous.get('status') in {'posting', 'sent'} and (
                    previous.get('channel') != channel or previous.get('app_id') != app_id):
                return False
            if fingerprint and previous.get('fingerprint', fingerprint) != fingerprint:
                return False
            if previous.get('target') and target and previous['target'] != target:
                return False
            self.save_route(key, {'channel': channel, 'app_id': app_id, 'status': 'posting',
                'fingerprint': fingerprint or previous.get('fingerprint', ''), 'target': target,
                'prior_unconfirmed': previous.get('status') == 'posting',
                'attempted_at': previous.get('attempted_at') if previous.get('status') == 'posting' else time.time()})
            return True

    def legacy_started(self, receive_id, receive_type, message_uuid, app_id):
        return self.claim(self.route_key(receive_id, receive_type, message_uuid), 'legacy', app_id,
                          target=[receive_type, receive_id])

    def legacy_finished(self, receive_id, receive_type, message_uuid, receipt):
        key = self.route_key(receive_id, receive_type, message_uuid)
        with self.route_lock:
            value = self.route(key)
            value.update(status='sent', receipt=receipt)
            self.save_route(key, value)

    def legacy_not_sent(self, receive_id, receive_type, message_uuid, *, posted=False, code=None):
        key = self.route_key(receive_id, receive_type, message_uuid)
        with self.route_lock:
            value = self.route(key)
            # A new rejection cannot disprove an older unacknowledged write.
            if value.get('status') == 'sent' or value.get('status') == 'posting' and (
                    value.get('channel') != 'legacy' or not posted or value.get('prior_unconfirmed')):
                return False
            self.save_route(key, {'status': 'not_sent', 'fingerprint': value.get('fingerprint', ''),
                                 **({'code': code} if code is not None else {})})
            return True

    def start(self):
        self.loop, self.closing = asyncio.get_running_loop(), False
        self.runtime.feishu_public_sender = self
        register_agent_sender(self)

    async def close(self):
        self.closing = True
        unregister_agent_sender(self)
        tasks = list(self.tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if getattr(self.runtime, 'feishu_public_sender', None) is self:
            self.runtime.feishu_public_sender = None
        self.loop = None

    def __call__(self, receive_id, kind, content, receive_type, message_uuid):
        loop = self.loop
        if self.closing or loop is None or loop.is_closed() or not loop.is_running():
            return DeliveryAttempt('not_sent', '飞书智能体公共发送器未就绪，尚未发送。')
        try:
            if asyncio.get_running_loop() is loop:
                return DeliveryAttempt('blocked', '同步消息入口不可阻塞主事件循环，请在后台线程调用；尚未发送。')
        except RuntimeError:
            pass
        coroutine = self.send(receive_id, kind, content, receive_type, message_uuid)
        try:
            future = asyncio.run_coroutine_threadsafe(coroutine, loop)
        except RuntimeError:
            coroutine.close()
            return DeliveryAttempt('not_sent', '飞书智能体公共发送器已关闭，尚未发送。')
        try:
            return future.result(timeout=self.timeout)
        except FutureTimeout:
            future.cancel()
            return DeliveryAttempt('uncertain', '飞书智能体发送等待超时，结果未确认；未改用备用应用补发。')
        except Exception:
            return DeliveryAttempt('uncertain', '飞书智能体发送中断，结果未确认；未改用备用应用补发。')

    async def recipient(self, bot, open_id):
        auth = self.runtime.auth_manager
        person = await asyncio.to_thread(auth.directory_person, open_id) or {}
        if person and (person.get('inactive') or not person.get('selectable')):
            return DeliveryAttempt('blocked', '接收人已停用或身份待核对，未发送。')
        record_id = str(person.get('id') or '')
        if re.fullmatch(r'rec[A-Za-z0-9]+', record_id):
            try:
                mapped = await bot.messenger.recipient_open_id(record_id)
            except Exception as exc:
                if getattr(exc, 'category', '') == 'feishu_recipient_ambiguous':
                    return DeliveryAttempt('blocked', '接收人的智能体身份不唯一，未猜测发送。')
                if getattr(exc, 'category', '') != 'feishu_sender_directory_unavailable':
                    raise
                mapped = None
            if mapped:
                return 'open_id', mapped
        # These existing helpers verify persisted app bindings or a cross-app UnionID.
        cards = getattr(bot, 'handover_cards', None)
        if cards:
            return await asyncio.to_thread(cards.recipient_id,
                                           {'id': record_id or open_id, 'open_id': open_id})
        return 'union_id', await asyncio.to_thread(bot.permission_cards.union_id, open_id)

    async def send(self, receive_id, kind, content, receive_type, message_uuid):
        key = self.route_key(receive_id, receive_type, message_uuid)
        fingerprint = hashlib.sha256(json.dumps([kind, content], sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        lock = self.locks.setdefault(key, asyncio.Lock())
        task = asyncio.current_task()
        self.tasks.add(task)
        try:
            async with lock:
                previous = await asyncio.to_thread(self.route, key)
                if previous.get('fingerprint') and previous['fingerprint'] != fingerprint:
                    return DeliveryAttempt('blocked', '原发送UUID对应的消息内容已变化，未误报成功或重新发送。')
                if previous.get('status') == 'sent':
                    return DeliveryAttempt('sent', 'ok', previous['receipt'])
                if previous.get('status') == 'posting' and time.time() - previous.get('attempted_at', 0) > 3000:
                    return DeliveryAttempt('uncertain', '原消息结果未确认且已超出去重安全时限，请先核验原消息；未重复发送。')
                if previous.get('channel') == 'legacy' and previous.get('status') == 'posting':
                    return DeliveryAttempt('not_sent', '原任务已绑定备用应用，只能继续原应用及原UUID。')
                attempt = await self._send(receive_id, kind, content, receive_type, message_uuid, key, fingerprint)
                if attempt.status == 'sent':
                    await asyncio.to_thread(self.save_route, key, {'channel': 'agent', 'status': 'sent',
                        'app_id': attempt.receipt['sender_app_id'], 'receipt': attempt.receipt, 'fingerprint': fingerprint})
                elif attempt.status == 'not_sent':
                    if previous.get('status') == 'posting':
                        return DeliveryAttempt('uncertain', '原智能体发送结果仍未确认，未跨应用补发。')
                    # The first attempt was explicitly rejected; only now may a non-callback use legacy.
                    current = await asyncio.to_thread(self.route, key)
                    if current.get('channel') != 'legacy':
                        await asyncio.to_thread(self.save_route, key, {'status': 'not_sent', 'fingerprint': fingerprint,
                            **({'code': attempt.code} if attempt.code is not None else {})})
                return attempt
        except asyncio.CancelledError:
            raise
        except Exception:
            return DeliveryAttempt('uncertain', '发送状态保存或读取未完成，未跨应用补发。')
        finally:
            self.tasks.discard(task)

    async def _send(self, receive_id, kind, content, receive_type, message_uuid, key, fingerprint):
        posted = False
        try:
            from .portal_service import external_real_write_guard
            if not external_real_write_guard().get('real_write_allowed'):
                return DeliveryAttempt('blocked', '当前运行模式不允许真实飞书消息发送。')
            bot = getattr(self.runtime, 'feishu_assistant_channel', None)
            if (self.closing or not bot or getattr(bot, 'closing', False)
                    or not getattr(bot, 'messenger', None) or not getattr(bot, 'config', {}).get('app_id')):
                return DeliveryAttempt('not_sent', '飞书智能体尚未就绪，尚未发送。')
            if receive_type == 'chat_id':
                mapped_type, mapped_id = 'chat_id', receive_id
            else:
                try:
                    mapped = await self.recipient(bot, receive_id)
                except Exception:
                    return DeliveryAttempt('not_sent', '无法安全映射接收人的飞书智能体身份，尚未发送。')
                if isinstance(mapped, DeliveryAttempt):
                    return mapped
                mapped_type, mapped_id = mapped
            pattern = {'chat_id': r'oc_[A-Za-z0-9]+', 'open_id': r'ou_[A-Za-z0-9]+',
                       'union_id': r'on_[A-Za-z0-9]+'}.get(mapped_type)
            if not pattern or not isinstance(mapped_id, str) or not re.fullmatch(pattern, mapped_id):
                return DeliveryAttempt('not_sent', '接收人的飞书智能体身份映射无效，尚未发送。')
            client, headers = await bot.messenger.client(), await bot.messenger.headers()
            if self.closing or not external_real_write_guard().get('real_write_allowed'):
                return DeliveryAttempt('blocked', '发送器已关闭或真实写入已禁用，尚未发送。')
            if not await asyncio.to_thread(self.claim, key, 'agent', bot.config['app_id'], fingerprint,
                                            [mapped_type, mapped_id]):
                return DeliveryAttempt('blocked', '原任务已绑定其他发送应用，未跨应用重发。')
            posted = True
            response = await client.post('https://open.feishu.cn/open-apis/im/v1/messages',
                params={'receive_id_type': mapped_type}, headers=headers,
                json={'receive_id': mapped_id, 'msg_type': kind,
                      'content': json.dumps(content, ensure_ascii=False), 'uuid': message_uuid})
            status = response.status_code
            value = response.json()
            if (status == 408 or status >= 500 or not isinstance(value, dict) or type(value.get('code')) is not int
                    or status >= 400 and value['code'] == 0):
                return DeliveryAttempt('uncertain', '飞书智能体响应不完整，发送结果未确认；未补发。')
            if value['code'] != 0:
                from upload_event_module.services.feishu_token_manager import TOKEN_ERROR_CODES
                if value['code'] in TOKEN_ERROR_CODES:
                    bot.messenger.expires = 0
                return DeliveryAttempt('not_sent', f"飞书智能体明确拒绝消息（code={value['code']}），未发送。",
                                       code=value['code'])
            data = value.get('data')
            if not isinstance(data, dict) or not all(
                    isinstance(data.get(key), str) and re.fullmatch(pattern, data[key])
                    for key, pattern in (('message_id', r'om_[A-Za-z0-9]+'), ('chat_id', r'oc_[A-Za-z0-9]+'))):
                return DeliveryAttempt('uncertain', '飞书智能体缺少有效消息回执，发送结果未确认；未补发。')
            if receive_type == 'chat_id' and data['chat_id'] != receive_id:
                return DeliveryAttempt('uncertain', '飞书回执与原群不一致，发送结果待核对；未改发其他群。')
            return DeliveryAttempt('sent', 'ok', {
                'message_id': data['message_id'], 'chat_id': data['chat_id'],
                'sender_app_id': bot.config['app_id'], 'message_uuid': message_uuid,
                'accepted_at': time.time(), 'delivery_channel': 'agent',
            })
        except asyncio.CancelledError:
            raise
        except Exception:
            return DeliveryAttempt('uncertain' if posted else 'not_sent',
                '飞书智能体发送结果未确认，未补发。' if posted else '飞书智能体连接或凭据未就绪，尚未发送。')
