"""Feishu review cards share the web decision and authorization paths."""

import asyncio
import base64
import copy
import hashlib
import io
import json
import logging
import re
import time
import uuid

from .risk_inspection import RiskError


def photo_bytes(value):
    from PIL import Image

    try:
        mime, _, data = value.partition(',')
        if mime not in ('data:image/jpeg;base64', 'data:image/png;base64') or len(data) > 1_398_104:
            raise ValueError()
        content = base64.b64decode(data, validate=True)
        with Image.open(io.BytesIO(content)) as picture:
            expected = 'PNG' if 'png' in mime else 'JPEG'
            if picture.format != expected or picture.width * picture.height > 16_000_000:
                raise ValueError()
            picture.verify()
        return content, mime[5:].split(';')[0]
    except (ValueError, AttributeError, OSError, Image.DecompressionBombError):
        raise RiskError('现场照片须为 1 MB 以内的有效 JPG 或 PNG。') from None


def validated_photos(values):
    if not isinstance(values, list) or len(values) > 3:
        raise RiskError('每台工具最多上传 3 张现场照片。')
    for value in values:
        photo_bytes(value)
    return values


def plain(text):
    return {'tag': 'plain_text', 'content': text}


def review_elements(value, url):
    actions = []
    for decision, title, style in (
        ('approve', '签名并确认' if value['review'] == 'self' else '同意报废', 'primary'),
        ('reject', '驳回', 'danger'),
    ):
        actions.append({'tag': 'button', 'text': plain(title), 'type': style,
                        'behaviors': [{'type': 'callback', 'value': value | {'kind': 'tool_review', 'decision': decision}}]})
    if url:
        actions.append({'tag': 'button', 'text': plain('打开灯塔查看'), 'type': 'default',
                        'behaviors': [{'type': 'open_url', 'default_url': url}]})
    return actions


def for_recipient(card, open_id):
    wrapped = copy.deepcopy(card)
    for element in wrapped['body']['elements']:
        for behavior in element.get('behaviors', []):
            if behavior.get('type') == 'callback':
                behavior['value']['recipient'] = open_id
    return wrapped


def delivery_groups(rows, open_id):
    """Keep retries together even when part of an already-sent card was handled."""
    groups = {}
    for row in rows:
        delivery = json.loads(row['card'] or '{}').get(open_id, {})
        identifier = delivery.get('id', '')
        _, members = groups.setdefault(identifier, (delivery, []))
        members.append(row)
    return groups.values()


def remember_delivery(raw, open_id, identifier, card):
    deliveries = json.loads(raw or '{}')
    deliveries[open_id] = {'id': identifier, 'card': card}
    return json.dumps(deliveries, ensure_ascii=False)


def send_scrap_review_one(service, deliver, link):
    from .tool_management import SCRAP_APPROVERS

    now = time.time()
    with service.connect(True) as db:
        candidates = """FROM tool_scrap_votes v JOIN tool_work_orders w ON w.id=v.order_id
            LEFT JOIN tool_workspace_items i ON i.order_id=w.id
            WHERE w.outcome='scrap' AND w.ledger_deleted_at='' AND v.sent=0 AND v.decision='' AND v.next_try<=?
              AND NOT EXISTS (SELECT 1 FROM tool_ledger_deletions d WHERE d.receipt_key=w.id)
              AND v.request_revision=(SELECT MAX(request_revision) FROM tool_scrap_votes WHERE order_id=v.order_id)"""
        seed = db.execute(f"""SELECT v.employee_no,COALESCE(i.session_id,w.id) batch {candidates}
            ORDER BY v.next_try,v.order_id,v.employee_no LIMIT 1""", (now,)).fetchone()
        if not seed:
            return False
        votes = db.execute(f"""SELECT v.*, w.asset,w.scope,w.owner_name,w.reason,w.actual,w.resolution,w.kind,
            w.scrap_qty,w.scrap_reason,w.scrap_photos,w.records,i.draft {candidates}
            AND COALESCE(i.session_id,w.id)=? AND v.employee_no=? ORDER BY v.order_id""",
            (now, seed['batch'], seed['employee_no'])).fetchall()
        keys = [(vote['order_id'], vote['request_revision'], vote['employee_no']) for vote in votes]
        db.executemany('UPDATE tool_scrap_votes SET next_try=? WHERE order_id=? AND request_revision=? AND employee_no=?',
                       [(now + 60, *key) for key in keys])
    name = SCRAP_APPROVERS[seed['employee_no']]
    matches = [
        person for person in (service.workspace_directory() if service.workspace_directory else [])
        if str(person.get('employee_no') or '') == seed['employee_no'] and person.get('name') == name
        and person.get('selectable') and not person.get('departed')]
    if len(matches) != 1:
        return True
    actor = service.person_for(matches[0]['record_id'])
    open_id = actor.get('open_id', '')
    if service.scrap_approver(actor) != seed['employee_no'] or not re.fullmatch(r'ou_[A-Za-z0-9]+', open_id):
        return True
    for delivery, group in delivery_groups(votes, open_id):
        send_scrap_review_group(service, deliver, link, open_id, group, delivery)
    return True


def send_scrap_review_group(service, deliver, link, open_id, votes, delivery):
    keys = [(vote['order_id'], vote['request_revision'], vote['employee_no']) for vote in votes]
    entries = [{'order': key[0], 'revision': key[1]} for key in keys]
    message_uuid = delivery.get('id') or str(uuid.uuid5(uuid.NAMESPACE_URL, f'clipflow:tool-scrap:{entries}:{open_id}'))
    card = delivery.get('card') or next((saved for vote in votes
        if (saved := json.loads(vote['card'] or '{}').get(message_uuid))), None)
    if card is None:
        url = link(votes[0]['scope'], keys[0][0] if len(keys) == 1 else '')
        elements = [{'tag': 'div', 'text': plain(f'{len(votes)} 项待审批，梅冰冰、马进宇任一位同意即可报废。')}]
        if len(votes) <= 12:
            for vote in votes:
                asset = json.loads(vote['asset'])
                draft = json.loads(vote['draft'] or '{}')
                original = next(iter(json.loads(vote['records'])), {})
                description = '\n'.join((
                    f"楼栋：{vote['scope']}楼　工具：{asset['name']}",
                    f"型号：{asset.get('model') or '—'}　报废数量：{vote['scrap_qty']}",
                    f"检查人：{draft.get('inspector_name') or original.get('inspector') or vote['owner_name'] or '—'}",
                    f"审核人：{draft.get('reviewer') or original.get('reviewer') or '—'}",
                    f"检查时间：{draft.get('checked_at_text') or original.get('checked_at') or (vote['actual'] if vote['kind']=='self' else '—')}",
                    f"送检时间：{vote['actual'] if vote['kind']=='external' else '—'}",
                    '灯塔处理说明：' + '；'.join(dict.fromkeys(filter(None, (vote['reason'], vote['scrap_reason'], vote['resolution'])))),
                ))
                elements.append({'tag': 'div', 'text': plain(description)})
                photos = json.loads(vote['scrap_photos'])
                if not photos:
                    elements.append({'tag': 'div', 'text': plain('照片：未上传')})
                for value in photos:
                    content, mime = photo_bytes(value)
                    photo = service.cloud.review_photo(content, mime)
                    elements.append({'tag': 'img', 'img_key': photo, 'alt': plain('现场照片'), 'preview': True})
        card = batch_card('工具报废待审批', 'orange', elements, {'review': 'scrap', 'items': entries}, url, open_id)
    if not delivery:
        with service.connect(True) as db:
            db.executemany('UPDATE tool_scrap_votes SET card=? WHERE order_id=? AND request_revision=? AND employee_no=?',
                           [(remember_delivery(vote['card'], open_id, message_uuid, card), *key)
                            for vote, key in zip(votes, keys)])
    with service.connect() as db:
        if not any(db.execute("""SELECT 1 FROM tool_scrap_votes v JOIN tool_work_orders w ON w.id=v.order_id
            WHERE v.order_id=? AND v.request_revision=? AND v.employee_no=? AND v.decision=''
              AND v.sent=0 AND w.outcome='scrap' AND w.ledger_deleted_at=''
              AND NOT EXISTS (SELECT 1 FROM tool_ledger_deletions d WHERE d.receipt_key=w.id)
              AND v.request_revision=(SELECT MAX(request_revision) FROM tool_scrap_votes WHERE order_id=v.order_id)
              """, key).fetchone() for key in keys):
            return
    if deliver(open_id, card, message_uuid) is True:
        with service.connect(True) as db:
            db.executemany('UPDATE tool_scrap_votes SET sent=1 WHERE order_id=? AND request_revision=? AND employee_no=?', keys)


def batch_card(title, color, elements, value, url, open_id):
    def make(parts):
        return for_recipient({'schema': '2.0', 'header': {'template': color, 'title': plain(title)},
                              'body': {'elements': parts}}, open_id)
    entries = value['items']
    full = make(elements + review_elements(value, url))
    if len(entries) <= 12 and len(json.dumps(full, ensure_ascii=False).encode()) <= 28_000:
        return full
    fallback = {'tag': 'div', 'text': plain(f'共 {len(entries)} 项。请打开灯塔查看全部资料并审核。')}
    button = {'tag': 'button', 'text': plain('打开灯塔查看'), 'type': 'primary',
              'behaviors': [{'type': 'open_url', 'default_url': url}]}
    if not url:
        raise RiskError('批次较大，尚未取得灯塔审核链接。', 503)
    return make([fallback, button])


def review_action(data, app_id):
    header, event = data.get('header') or {}, data.get('event') or {}
    value, sender = (event.get('action') or {}).get('value') or {}, (event.get('operator') or {}).get('open_id') or ''
    if (header.get('event_type') != 'card.action.trigger' or header.get('app_id') != app_id
            or value.get('kind') != 'tool_review' or not re.fullmatch(r'ou_[A-Za-z0-9]+', sender)
            or value.get('recipient') != sender or value.get('decision') not in ('approve', 'reject')):
        raise RiskError('仅卡片指定审核人可以处理本次审核。', 403)
    entries = value.get('items')
    if (not isinstance(entries, list) or not 1 <= len(entries) <= 12 or any(
            not isinstance(entry, dict) or type(entry.get('revision')) is not int
            or not re.fullmatch(r'[a-f0-9]{32}', str(entry.get('order') or '')) for entry in entries)
            or len({entry['order'] for entry in entries}) != len(entries)):
        raise RiskError('审批卡片的任务范围已变化，请重新打开记录。', 409)
    review = value.get('review')
    if review not in ('self', 'scrap'):
        raise RiskError('审核类型无效。')
    if review == 'self' and (not re.fullmatch(r'[a-f0-9]{32}', str(value.get('session') or '')) or any(
            not re.fullmatch(r'rec[A-Za-z0-9]+', str(entry.get('asset') or '')) for entry in entries)):
        raise RiskError('自检审核卡片已失效。', 409)
    return value, sender


def queued_review(data, app_id):
    value, sender = review_action(data, app_id)
    context = data['event'].get('context') or {}
    original = context.get('open_message_id') or ''
    chat = context.get('open_chat_id') or ''
    if not re.fullmatch(r'om_[A-Za-z0-9]+', original) or not re.fullmatch(r'oc_[A-Za-z0-9]+', chat):
        raise RiskError('未识别到原审批卡片，请从飞书原卡片操作。')
    items = sorted(([entry['order'], entry['revision']] for entry in value['items']))
    # Both buttons on the same review round share the first accepted request.
    key = json.dumps([app_id, original, sender, value['review'], value.get('session'), items])
    identity = 'tool_review_' + hashlib.sha256(key.encode()).hexdigest()
    return {'kind': 'tool_review', 'message_id': identity, 'app_id': app_id, 'sender_id': sender,
            'union_id': '', 'chat_id': chat, 'chat_type': 'p2p', 'reply_to_message_id': original, 'callback': data}


def review_reply(message, *, ok=True):
    return {'ok': ok, 'toast': message, 'card': {'schema': '2.0',
            'header': {'template': 'grey' if ok else 'orange', 'title': plain('工具管理审核')},
            'body': {'elements': [{'tag': 'div', 'text': plain(message)}]}}}


def apply(runtime, data, app_id, *, card_receipt=''):
    value, sender = review_action(data, app_id)
    if runtime.auth_manager.current_feishu_app_id() != app_id:
        raise RiskError('飞书应用已变更，请在灯塔查看原记录。', 403)
    entries = value['items']
    service = runtime.tool_management()
    actor = runtime.risk_inspection_actor(sender)
    decision = value['decision']
    note = '' if decision == 'approve' else '审批人在飞书卡片驳回'
    if value.get('review') == 'scrap':
        count = service.scrap_review_batch(actor, entries, decision, note, card_receipt=card_receipt)
        message = ''
        if count:
            message = f'{count} 项已批准报废。' if decision == 'approve' else f'已驳回 {count} 项报废申请。'
    else:
        result = service.workspace_review(actor, value['session'], {'action': decision, 'note': note,
            'use_saved_signature': decision == 'approve',
            'items': [{'asset_id': entry['asset'], 'revision': entry['revision'], 'order_id': entry['order']} for entry in entries]},
            from_card=True, card_receipt=card_receipt)
        count = result['card_review_count']
        message = ''
        if count:
            message = f'{count} 项自检审核通过。' if decision == 'approve' else f'已驳回 {count} 项自检记录。'
    if count < len(entries):
        message = f'{message} {len(entries) - count} 项已处理或已变更，已跳过。'.strip()
    if count:
        message += ' 结果通知的投递异常可在灯塔办理记录中查看。'
    return review_reply(message)


async def process_queued(bot, task):
    from .portal_service import external_real_write_guard

    inbox, identity = bot.inbox, task['message_id']
    reply = task.get('reply') or {}
    try:
        if not reply.get('card'):
            try:
                reply = await asyncio.to_thread(apply, bot.runtime, task['callback'], task['app_id'], card_receipt=identity)
            except RiskError as exc:
                if exc.status == 429 or exc.status >= 500:
                    raise
                reply = review_reply('审核未完成。' + str(exc) + ' 请在灯塔办理记录中核对。', ok=False)
            # Retry the original card update independently of the committed approval.
            await asyncio.to_thread(inbox.set, identity, 'processing', reply)
        await bot._require_current_config()
        if bot.config.get('app_id') != task['app_id']:
            await asyncio.to_thread(inbox.set, identity, 'stopped')
            return
        if not external_real_write_guard()['real_write_allowed']:
            await asyncio.to_thread(inbox.defer_delivery, identity, reason='飞书卡片更新暂不可用')
            return
        client = await bot.messenger.client()
        response = await client.patch('https://open.feishu.cn/open-apis/im/v1/messages/' + task['reply_to_message_id'],
            headers=await bot.messenger.headers(), json={'content': json.dumps(reply['card'], ensure_ascii=False)})
        if response.status_code in (403, 404, 410):
            await asyncio.to_thread(inbox.defer_delivery, identity, permanent=True, reason='原飞书审批卡片已无法更新')
            return
        response.raise_for_status()
        if response.json().get('code') != 0:
            raise RiskError('飞书未确认原卡片更新。', 503)
        await asyncio.to_thread(inbox.set, identity, 'done')
    except Exception as exc:
        logging.warning('Feishu tool review retry deferred: %s', type(exc).__name__)
        await asyncio.to_thread(inbox.defer_delivery, identity,
                                reason='原卡片更新待重试' if reply.get('card') else '工具审批待重试')
