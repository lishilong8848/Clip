"""Feishu review cards share the web decision and authorization paths."""

import asyncio
import base64
import copy
import io
import json
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
        ('approve', '签名并通过' if value['review'] == 'self' else '同意报废', 'primary'),
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
            WHERE w.outcome='scrap' AND v.sent=0 AND v.decision='' AND v.next_try<=?
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
        elements = [{'tag': 'div', 'text': plain(f'{len(votes)} 项待审批，需梅冰冰、马进宇均同意后生效。')}]
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
              AND v.sent=0 AND w.outcome='scrap'
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


def apply(runtime, data, app_id):
    header, event = data.get('header') or {}, data.get('event') or {}
    value, sender = (event.get('action') or {}).get('value') or {}, (event.get('operator') or {}).get('open_id') or ''
    if (header.get('event_type') != 'card.action.trigger' or header.get('app_id') != app_id
            or runtime.auth_manager.current_feishu_app_id() != app_id
            or value.get('kind') != 'tool_review' or not re.fullmatch(r'ou_[A-Za-z0-9]+', sender)
            or value.get('recipient') != sender or value.get('decision') not in ('approve', 'reject')):
        raise RiskError('仅卡片指定审核人可以处理本次审核。', 403)
    entries = value.get('items')
    if (not isinstance(entries, list) or not 1 <= len(entries) <= 12 or any(
            not isinstance(entry, dict) or type(entry.get('revision')) is not int
            or not re.fullmatch(r'[a-f0-9]{32}', str(entry.get('order') or '')) for entry in entries)
            or len({entry['order'] for entry in entries}) != len(entries)):
        raise RiskError('审批卡片的任务范围已变化，请重新打开记录。', 409)
    service = runtime.tool_management()
    actor = runtime.risk_inspection_actor(sender)
    decision = value['decision']
    note = '' if decision == 'approve' else '审批人在飞书卡片驳回'
    if value.get('review') == 'scrap':
        results = service.scrap_review_batch(actor, entries, decision, note)
        finished = sum(result['outcome'] == 'scrapped' for result in results)
        waiting = sum(result['outcome'] == 'scrap' for result in results)
        message = f'已记录审批。{finished} 项已批准报废，{waiting} 项等待另一位经理审批。' if decision == 'approve' else f'已驳回 {len(results)} 项报废申请。'
        if len(results) < len(entries):
            message += f' {len(entries) - len(results)} 项已处理或已变更，已跳过。'
    elif value.get('review') == 'self':
        if not re.fullmatch(r'[a-f0-9]{32}', str(value.get('session') or '')) or any(
                not re.fullmatch(r'rec[A-Za-z0-9]+', str(entry.get('asset') or '')) for entry in entries):
            raise RiskError('自检审核卡片已失效。', 409)
        result = service.workspace_review(actor, value['session'], {'action': decision, 'note': note,
            'use_saved_signature': decision == 'approve',
            'items': [{'asset_id': entry['asset'], 'revision': entry['revision'], 'order_id': entry['order']} for entry in entries]},
            from_card=True)
        count = result['card_review_count']
        message = f'{count} 项自检审核通过。' if decision == 'approve' else f'已驳回 {count} 项自检记录。'
        if count < len(entries):
            message += f' {len(entries) - count} 项已处理或已变更，已跳过。'
    else:
        raise RiskError('审核类型无效。')
    return {'ok': True, 'toast': message, 'card': {'schema': '2.0', 'header': {'template': 'grey', 'title': plain('工具管理审核')},
                                                 'body': {'elements': [{'tag': 'div', 'text': plain(message)}]}}}


async def callback(runtime, data, app_id):
    try:
        return await asyncio.wait_for(asyncio.to_thread(apply, runtime, data, app_id), timeout=2)
    except RiskError as exc:
        return {'ok': False, 'toast': str(exc)}
    except Exception:
        return {'ok': False, 'toast': '审核结果暂未确认，请重试原卡片或到灯塔核对。'}
