"""Bridge synchronous public notifications to the portal's existing agent client."""
from __future__ import annotations

import asyncio
from concurrent.futures import TimeoutError as FutureTimeout
import hashlib
import json
import re
import sqlite3
import threading
import time
import uuid
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace
from weakref import WeakValueDictionary

from upload_event_module.services.robot_webhook import (
    DeliveryAttempt, active_bot_matches, current_app_credentials,
    register_agent_sender, unregister_agent_sender,
)
from upload_event_module.config import config

_ROUTE_LOCK = threading.RLock()


class FeishuDeliveryError(RuntimeError):
    def __init__(self, message, *, uncertain=False, code=None):
        super().__init__(message)
        self.uncertain = uncertain
        self.code = code


def current_recipient_open_id(runtime, open_id, app_id, *, record_id=None):
    """Validate current-app evidence without substituting another recipient ID."""
    from .feishu_identity import TARGET_APP_ID
    from .portal_auth import REQUIRED_ADMIN_USERS
    from .portal_service import BUILDING_OPEN_ID_MAP
    from .personnel_password_login import DIRECTORY_NS, DIRECTORY_KEY

    if not isinstance(open_id, str) or not re.fullmatch(r'ou_[A-Za-z0-9]+', open_id) or not app_id:
        raise FeishuDeliveryError('接收人的当前应用OpenID无效，尚未发送。')
    auth = getattr(runtime, 'auth_manager', None)
    disabled = getattr(auth, '_open_id_explicitly_disabled', None)
    if callable(disabled) and disabled(open_id) is True:
        raise FeishuDeliveryError('接收人已停用，未发送。')
    quarantined = getattr(auth, '_identity_quarantined', None)
    if callable(quarantined) and quarantined(open_id) is True:
        raise FeishuDeliveryError('接收人身份尚未通过迁移核验，未发送。')
    if app_id == TARGET_APP_ID and open_id in {*BUILDING_OPEN_ID_MAP.values(), *REQUIRED_ADMIN_USERS}:
        store = runtime.state_store
        # Standalone delivery has no auth manager; retain its local safety checks.
        if not callable(disabled):
            read_permissions = getattr(store, 'get_auth_permissions', None)
            permissions = read_permissions() if callable(read_permissions) else {}
            if ((permissions or {}).get('users') or {}).get(open_id, {}).get('enabled') is False:
                raise FeishuDeliveryError('接收人已停用，未发送。')
        if not callable(quarantined):
            path = getattr(store, 'db_path', None)
            if isinstance(path, Path) and path.is_file():
                from .feishu_identity_migration import is_identity_quarantined
                with closing(sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True)) as db:
                    if is_identity_quarantined(db, open_id):
                        raise FeishuDeliveryError('接收人身份尚未通过迁移核验，未发送。')
        if current_app_credentials()[0] != app_id:
            raise FeishuDeliveryError('接收人与当前Qt应用不一致，未发送。')
        return open_id
    lookup = getattr(auth, 'directory_person', None)
    person = lookup(open_id) if callable(lookup) else {}
    if isinstance(person, dict) and person and (person.get('inactive') or person.get('selectable') is False):
        raise FeishuDeliveryError('接收人已停用或身份待核对，未发送。')
    store = runtime.state_store
    read = getattr(store, 'get_document', None)
    snapshot = read(DIRECTORY_NS, DIRECTORY_KEY) if callable(read) else {}
    snapshot = snapshot if isinstance(snapshot, dict) else {}
    people = snapshot.get('people') or {}
    matches, retired = [], False
    for rid, row in people.items() if isinstance(people, dict) else ():
        if not isinstance(row, dict):
            raise FeishuDeliveryError('当前人员目录格式无效，未发送。')
        current = (row.get('feishu_open_ids') or {}).get(app_id)
        ids = row.get('login_ids') or []
        if current:
            retired |= open_id in ids and current != open_id
            present = current == open_id
        else:
            present = snapshot.get('app_id') == app_id and open_id in ids
        if present:
            if not current and len(set(ids)) != 1:
                raise FeishuDeliveryError('接收人的当前应用身份不唯一，未发送。')
            matches.append((rid, row))
    if retired or len(matches) > 1:
        raise FeishuDeliveryError('接收人的当前应用身份不唯一或已失效，未发送。')
    if matches:
        rid, row = matches[0]
        if (row.get('inactive') or row.get('selectable') is not True
                or record_id is not None and str(row.get('id') or rid) != record_id):
            raise FeishuDeliveryError('接收人已停用或人员记录不一致，未发送。')
        return open_id
    prefix = str(app_id) + ':'
    for row in store.list_documents('feishu_assistant_sender', key_prefix=prefix):
        value = row.get('payload') or {}
        if (row.get('key') == prefix + open_id and value.get('open_id') == open_id
                and value.get('matched_by') in {'record_id', 'union_id', 'admin'}
                and not value.get('inactive') and value.get('selectable') is not False
                and (record_id is None or value.get('record_id') == record_id)):
            return open_id
    path = getattr(store, 'db_path', None)
    if isinstance(path, Path) and path.is_file():
        with closing(sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True)) as db:
            columns = {r[1] for r in db.execute('PRAGMA table_info(auth_sessions)')}
            if 'user_json' in columns:
                extra = [name for name in ('payload_json', 'expires_at', 'revoked_at') if name in columns]
                db.row_factory = sqlite3.Row
                for row in db.execute('SELECT user_json' + ''.join(',' + name for name in extra) +
                                      " FROM auth_sessions WHERE json_extract(user_json,'$.open_id')=?", (open_id,)):
                    user = json.loads(row['user_json'])
                    payload = json.loads(row['payload_json']) if 'payload_json' in extra else {}
                    apps = {value for source in (user, payload) for name in ('app_id', 'feishu_app_id')
                            if (value := source.get(name))}
                    if (apps == {app_id} and not user.get('inactive') and user.get('selectable') is not False
                            and ('expires_at' not in extra or row['expires_at'] > time.time())
                            and ('revoked_at' not in extra or row['revoked_at'] is None) and record_id is None):
                        return open_id
    raise FeishuDeliveryError('无法核对接收人的当前应用身份，请同步当前人员目录；未替换接收人。')


def standalone_sender():
    from .state_store import LanPortalStateStore
    return PublicFeishuSender(SimpleNamespace(state_store=LanPortalStateStore()))


async def send_agent_message(runtime, receive_id, content, *, receive_id_type='open_id',
                             msg_type='interactive', message_uuid='') -> dict:
    """Agent-only async API. Personal IDs are portal IDs, never foreign bot IDs."""
    sender = getattr(runtime, 'feishu_public_sender', None)
    if sender is None or sender.closing or sender.loop is not asyncio.get_running_loop():
        raise FeishuDeliveryError('飞书智能体公共发送器尚未就绪，尚未发送。')
    if receive_id_type not in {'open_id', 'chat_id'} or msg_type not in {'text', 'interactive'}:
        raise FeishuDeliveryError('公共发送仅支持人员或原群的文本、卡片消息。')
    if not isinstance(content, dict) or not content:
        raise FeishuDeliveryError('消息内容为空或格式无效。')
    attempt = await sender.send(receive_id, msg_type, content, receive_id_type,
                                str(message_uuid or uuid.uuid4()))
    if attempt.status != 'sent':
        raise FeishuDeliveryError(attempt.message, uncertain=attempt.status == 'uncertain', code=attempt.code)
    if attempt.receipt.get('delivery_channel') != 'agent':
        raise FeishuDeliveryError('原任务已有其他发送通道的历史回执，智能体专用接口未重新发送。')
    return attempt.receipt


class PublicFeishuSender:
    def __init__(self, runtime, *, timeout=60):
        self.runtime, self.timeout = runtime, timeout
        self.loop, self.closing, self.tasks = None, False, set()
        self.route_lock, self.locks = _ROUTE_LOCK, WeakValueDictionary()

    @staticmethod
    def route_key(receive_id, receive_type, message_uuid):
        return hashlib.sha256(json.dumps([receive_type, receive_id, message_uuid]).encode()).hexdigest()

    def route(self, key):
        return self.runtime.state_store.get_document('feishu_public_delivery', key) or {}

    def delivery_route(self, receive_id, receive_type, message_uuid):
        key = self.route_key(receive_id, receive_type, message_uuid)
        current = self.route(key)
        if current:
            if any(current.get(field, expected) != expected for field, expected in (
                    ('receive_id', receive_id), ('receive_type', receive_type), ('message_uuid', message_uuid))):
                raise FeishuDeliveryError('原任务投递记录与输入不一致，未重新发送。', uncertain=True)
            return current
        found = {}
        for entry in self.runtime.state_store.list_documents('feishu_public_delivery'):
            value = entry.get('payload') or {}
            migration = value.get('identity_migration') or {}
            previous = migration.get('previous_receive_id')
            if (migration.get('version') == 1 and migration.get('route_digest_verified') is True
                    and migration.get('new_receive_id') == receive_id
                    and migration.get('receive_type') == receive_type and migration.get('message_uuid') == message_uuid
                    and isinstance(previous, str) and self.route_key(previous, receive_type, message_uuid) == entry.get('key')):
                found[entry['key']] = value
        # Migration evidence recovers an original task hash, never a sending identity.
        from .feishu_identity_manifest import load_identity_manifest
        manifest = load_identity_manifest()
        for pair in manifest['mappings']:
            if pair['new_open_id'] != receive_id:
                continue
            old_key = self.route_key(pair['old_open_id'], receive_type, message_uuid)
            value = self.route(old_key)
            if value:
                found[old_key] = value
        if len(found) > 1:
            raise FeishuDeliveryError('原任务存在多条迁移投递记录，请核验原消息；未重复发送。', uncertain=True)
        if found:
            value = next(iter(found.values()))
            if value.get('status') in {'posting', 'pending', 'sending', 'uncertain'}:
                raise FeishuDeliveryError('原任务迁移前的发送结果未确认，请核验原消息；未重复发送。', uncertain=True)
            return value
        return {}

    def save_route(self, key, value):
        self.runtime.state_store.put_document('feishu_public_delivery', key, value)

    def claim(self, key, channel, app_id, fingerprint='', target=None, *, receive_id=None, receive_type=None, message_uuid=None):
        with self.route_lock:
            previous = self.route(key)
            if previous.get('status') in {'posting', 'pending', 'uncertain', 'sent'} and (
                    previous.get('channel') != channel or previous.get('app_id') != app_id):
                return False
            if fingerprint and previous.get('fingerprint', fingerprint) != fingerprint:
                return False
            if previous.get('target') and target and previous['target'] != target:
                return False
            self.save_route(key, {'channel': channel, 'app_id': app_id, 'status': 'posting',
                'fingerprint': fingerprint or previous.get('fingerprint', ''), 'target': target,
                'receive_id': receive_id, 'receive_type': receive_type, 'message_uuid': message_uuid,
                'prior_unconfirmed': previous.get('status') in {'posting', 'pending', 'uncertain'},
                'attempted_at': previous.get('attempted_at') if previous.get('status') in {'posting', 'pending', 'uncertain'} else time.time()})
            return True

    @staticmethod
    def prior_attempt(previous, fingerprint):
        if previous.get('fingerprint') and previous['fingerprint'] != fingerprint:
            return DeliveryAttempt('blocked', '原发送UUID对应的消息内容已变化，未误报成功或重新发送。')
        if previous.get('status') == 'sent':
            return DeliveryAttempt('sent', 'ok', previous['receipt'])
        if previous.get('status') in {'posting', 'pending', 'sending', 'uncertain'}:
            if (previous.get('identity_migration') or {}).get('blocked'):
                return DeliveryAttempt('uncertain', '原任务迁移前的发送结果未确认，请核验原消息；未重复发送。')
            if previous.get('app_id') != config.app_id or previous.get('channel') == 'legacy':
                return DeliveryAttempt('uncertain', '原应用发送结果未确认，请先核验原消息；未换应用重发。')
            if time.time() - previous.get('attempted_at', 0) > 3000:
                return DeliveryAttempt('uncertain', '原消息结果未确认且已超出去重安全时限，请先核验原消息；未重复发送。')

    def direct_recipient(self, receive_id, receive_type):
        if receive_type == 'chat_id' or re.fullmatch(r'on_[A-Za-z0-9]+', receive_id):
            return ('chat_id' if receive_type == 'chat_id' else 'union_id'), receive_id
        return 'open_id', current_recipient_open_id(self.runtime, receive_id, str(config.app_id or ''))

    def send_direct(self, receive_id, kind, content, receive_type, message_uuid):
        from upload_event_module.services import robot_webhook as robot
        key = self.route_key(receive_id, receive_type, message_uuid)
        fingerprint = hashlib.sha256(json.dumps([kind, content], sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        previous, receipt = {}, {}
        with self.route_lock:
            try:
                previous = self.delivery_route(receive_id, receive_type, message_uuid)
                if attempt := self.prior_attempt(previous, fingerprint):
                    return attempt
                if kind == 'interactive' and robot.card_requires_agent(content):
                    return DeliveryAttempt('blocked', '回调交互卡片必须由飞书智能体长连接处理，未直接发送。')
                credentials = current_app_credentials()
                app_id = credentials[0]
                if not app_id:
                    return DeliveryAttempt('not_sent', '当前Qt飞书应用认证未就绪，尚未发送。')
                token, error = robot._get_tenant_access_token()
                if error:
                    if previous.get('status') in {'posting', 'pending', 'uncertain'}:
                        return DeliveryAttempt('uncertain', '原消息结果尚未确认，当前应用认证未就绪；未重新发送。')
                    return DeliveryAttempt('not_sent', '当前Qt飞书应用认证未就绪，尚未发送。')
                mapped_type, mapped_id = self.direct_recipient(receive_id, receive_type)
                if current_app_credentials() != credentials:
                    return DeliveryAttempt('uncertain', 'Qt应用配置已变化，未重新发送。')
                if not self.claim(key, 'direct', app_id, fingerprint, [mapped_type, mapped_id],
                                  receive_id=receive_id, receive_type=receive_type, message_uuid=message_uuid):
                    return DeliveryAttempt('uncertain', '原消息发送状态尚未确认，未重新发送。')
                if current_app_credentials() != credentials:
                    return DeliveryAttempt('uncertain', 'Qt应用配置已变化，未重新发送。')
                outcome = {}
                if kind == 'interactive':
                    ok, message = robot._send_interactive_to_receive_id(token, mapped_id, content,
                        receive_id_type=mapped_type, message_uuid=message_uuid, receipt=receipt, outcome=outcome)
                else:
                    ok, message = robot._send_message_to_receive_id(token, mapped_id, content['text'],
                        receive_id_type=mapped_type, message_uuid=message_uuid, receipt=receipt, outcome=outcome)
                if current_app_credentials() != credentials:
                    return DeliveryAttempt('uncertain', '发送期间Qt应用配置已变化，请核验原消息；未重新发送。')
                if ok:
                    if receive_type == 'chat_id' and receipt.get('chat_id') != receive_id:
                        return DeliveryAttempt('uncertain', '飞书回执与原群不一致，发送结果待核对；未改发其他群。')
                    receipt.update(sender_app_id=app_id, delivery_channel='direct')
                    self.save_route(key, {'channel': 'direct', 'app_id': app_id, 'status': 'sent',
                        'receipt': receipt, 'fingerprint': fingerprint, 'receive_id': receive_id,
                        'receive_type': receive_type, 'message_uuid': message_uuid})
                    return DeliveryAttempt('sent', 'ok', receipt)
                if outcome.get('status') == 'not_sent' and previous.get('status') not in {'posting', 'pending', 'uncertain'}:
                    self.save_route(key, {'status': 'not_sent', 'fingerprint': fingerprint,
                        'receive_id': receive_id, 'receive_type': receive_type, 'message_uuid': message_uuid,
                        **({'code': outcome['code']} if 'code' in outcome else {})})
                    return DeliveryAttempt('not_sent', message, code=outcome.get('code'))
                return DeliveryAttempt('uncertain', message)
            except FeishuDeliveryError as exc:
                return DeliveryAttempt('uncertain' if exc.uncertain or previous.get('status') in {'posting', 'pending', 'uncertain'} else 'not_sent', str(exc))
            except Exception:
                return DeliveryAttempt('uncertain', '当前应用发送或回执读取未完成；未重新发送。', receipt)

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
            return DeliveryAttempt('uncertain', '飞书智能体发送等待超时，结果未确认；未重复发送。')
        except Exception:
            return DeliveryAttempt('uncertain', '飞书智能体发送中断，结果未确认；未重复发送。')

    async def recipient(self, bot, open_id):
        try:
            return 'open_id', await asyncio.to_thread(current_recipient_open_id, self.runtime, open_id, bot.config['app_id'])
        except FeishuDeliveryError as exc:
            return DeliveryAttempt('blocked', str(exc))

    async def send(self, receive_id, kind, content, receive_type, message_uuid):
        key = self.route_key(receive_id, receive_type, message_uuid)
        fingerprint = hashlib.sha256(json.dumps([kind, content], sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        lock = self.locks.setdefault(key, asyncio.Lock())
        task = asyncio.current_task()
        self.tasks.add(task)
        try:
            async with lock:
                previous = await asyncio.to_thread(self.delivery_route, receive_id, receive_type, message_uuid)
                if attempt := self.prior_attempt(previous, fingerprint):
                    return attempt
                attempt = await self._send(receive_id, kind, content, receive_type, message_uuid, key, fingerprint)
                if attempt.status == 'sent':
                    await asyncio.to_thread(self.save_route, key, {'channel': 'agent', 'status': 'sent',
                        'app_id': attempt.receipt['sender_app_id'], 'receipt': attempt.receipt, 'fingerprint': fingerprint,
                        'receive_id': receive_id, 'receive_type': receive_type, 'message_uuid': message_uuid})
                elif attempt.status == 'not_sent':
                    if previous.get('status') in {'posting', 'pending', 'uncertain'}:
                        return DeliveryAttempt('uncertain', '原智能体发送结果仍未确认，未跨应用补发。')
                    await asyncio.to_thread(self.save_route, key, {'status': 'not_sent', 'fingerprint': fingerprint,
                        'receive_id': receive_id, 'receive_type': receive_type, 'message_uuid': message_uuid,
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
            credentials = await asyncio.to_thread(current_app_credentials)
            app_id = credentials[0]
            messenger = bot.messenger
            if not active_bot_matches(bot, credentials, messenger):
                return DeliveryAttempt('not_sent', '飞书智能体与当前Qt应用尚未同步，尚未发送。')
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
            client, headers = await messenger.client(), await messenger.headers()
            if (self.closing or await asyncio.to_thread(current_app_credentials) != credentials
                    or not active_bot_matches(bot, credentials, messenger) or not external_real_write_guard().get('real_write_allowed')):
                return DeliveryAttempt('blocked', '发送器已关闭或真实写入已禁用，尚未发送。')
            if not await asyncio.to_thread(self.claim, key, 'agent', app_id, fingerprint,
                                            [mapped_type, mapped_id], receive_id=receive_id,
                                            receive_type=receive_type, message_uuid=message_uuid):
                return DeliveryAttempt('blocked', '原任务已绑定其他发送应用，未跨应用重发。')
            if await asyncio.to_thread(current_app_credentials) != credentials or not active_bot_matches(bot, credentials, messenger):
                return DeliveryAttempt('uncertain', 'Qt应用配置已变化，未重新发送。')
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
                    messenger.expires = 0
                return DeliveryAttempt('not_sent', f"飞书智能体明确拒绝消息（code={value['code']}），未发送。",
                                       code=value['code'])
            data = value.get('data')
            if not isinstance(data, dict) or not all(
                    isinstance(data.get(key), str) and re.fullmatch(pattern, data[key])
                    for key, pattern in (('message_id', r'om_[A-Za-z0-9]+'), ('chat_id', r'oc_[A-Za-z0-9]+'))):
                return DeliveryAttempt('uncertain', '飞书智能体缺少有效消息回执，发送结果未确认；未补发。')
            if receive_type == 'chat_id' and data['chat_id'] != receive_id:
                return DeliveryAttempt('uncertain', '飞书回执与原群不一致，发送结果待核对；未改发其他群。')
            if await asyncio.to_thread(current_app_credentials) != credentials or not active_bot_matches(bot, credentials, messenger):
                return DeliveryAttempt('uncertain', '发送期间Qt应用配置已变化，请核验原消息；未重新发送。')
            return DeliveryAttempt('sent', 'ok', {
                'message_id': data['message_id'], 'chat_id': data['chat_id'],
                'sender_app_id': app_id, 'message_uuid': message_uuid,
                'accepted_at': time.time(), 'delivery_channel': 'agent',
            })
        except asyncio.CancelledError:
            raise
        except Exception:
            return DeliveryAttempt('uncertain' if posted else 'not_sent',
                '飞书智能体发送结果未确认，未补发。' if posted else '飞书智能体连接或凭据未就绪，尚未发送。')
