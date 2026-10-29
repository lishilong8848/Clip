"""Feishu and web decisions use the same signature-use confirmation row."""
import asyncio
import re
import uuid
from urllib.parse import quote

from .feishu_card_lifetime import expired, EXPIRED_CARD_MESSAGE


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
