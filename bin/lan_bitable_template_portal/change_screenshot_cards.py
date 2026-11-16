"""Interactive Ali screenshot confirmation backed by the original record operation."""
import asyncio
import datetime as dt
import hashlib
import re
import secrets
import time
import uuid
from zoneinfo import ZoneInfo

from upload_event_module.services.feishu_token_manager import token_manager
from upload_event_module.services.http_client import FeishuHttpClient

NS='change_screenshot_cards'
_HTTP = FeishuHttpClient()
_BEIJING = dt.timezone(dt.timedelta(hours=8))
_IMAGE_KEY = re.compile(r'img_[A-Za-z0-9_-]{1,500}')


def format_start_time(value):
    from .eop_scenario_editor import plain

    text = str(value) if type(value) in (int, float) else plain(value).strip()
    try:
        if re.fullmatch(r'(?:\d{10}|\d{13})(?:\.\d+)?', text):
            timestamp = float(text)
            if timestamp >= 10_000_000_000:
                timestamp /= 1000
            start = dt.datetime.fromtimestamp(timestamp, _BEIJING)
        else:
            start = dt.datetime.fromisoformat(text)
            if start.tzinfo is None:
                start = start.replace(tzinfo=_BEIJING)
        return start.astimezone(_BEIJING).strftime('%Y-%m-%d %H:%M')
    except (ValueError, OverflowError, OSError):
        return ''

def check_lifetime(lifecycle, now=None):
    now=time.time() if now is None else now
    if lifecycle.get('finished'):
        try:
            end=dt.datetime.fromisoformat(str(lifecycle.get('ended_at') or '').replace('Z','+00:00'))
            if end.tzinfo is None:end=end.replace(tzinfo=ZoneInfo('Asia/Shanghai'))
        except (ValueError,TypeError):
            raise ValueError('实际结束时间尚未核实，请在灯塔核对后再确认。') from None
        if now>=end.timestamp()+7200:raise ValueError('通告结束已超过两小时，此确认卡片已过期。')
    elif not lifecycle.get('active'):
        raise ValueError('通告状态尚未核实，请打开灯塔核对。')

def render(task, token, link, done=False, *, image_keys=()):
    from .eop_scenario_editor import plain
    text=lambda s:{'tag':'plain_text','content':s}
    start = format_start_time(task.get('started_at'))
    details = '\n'.join((
        '变更名称：' + (plain(task.get('title')) or '未记录'),
        '楼栋：' + (plain(task.get('building')) or '未记录'),
        '开始时间：' + (start + '（北京时间）' if start else '未记录'),
    ))
    elements = [{'tag': 'div', 'text': text(details)}]
    elements.extend(
        {'tag': 'img', 'img_key': key, 'alt': text(f'阿里确认截图 {index}'),
         'preview': True, 'scale_type': 'fit_horizontal'}
        for index, key in enumerate(image_keys, 1)
    )
    elements.append({'tag': 'div', 'text': text(
        '已确认阿里截图。' if done else '请核对截图后确认。通告实际结束两小时后，卡片确认失效。'
    )})
    if not done:elements.append({'tag':'button','text':text('确认阿里截图'),'type':'primary','behaviors':[{'type':'callback','value':{'kind':'change_screenshot_confirm','token':token}}]})
    if link:elements.append({'tag':'button','text':text('打开灯塔处理'),'type':'default','behaviors':[{'type':'open_url','default_url':link}]})
    return {'schema':'2.0','header':{'title':text('变更阿里确认截图'+('已确认' if done else '待确认')),'template':'green' if done else 'blue'},'body':{'elements':elements}}

def _upload_card_image(runtime, record_id, file_token, credentials):
    from upload_event_module.services.robot_webhook import current_app_credentials

    try:
        content, mime_type, name = runtime.get_change_confirmation_screenshot_bytes(
            record_id, file_token=file_token, allowed_scopes=None, privileged=True,
        )
    except Exception:
        raise ValueError('阿里确认截图读取失败，等待重试。') from None
    if not content or len(content) > 10 * 1024 * 1024 or not mime_type.startswith('image/'):
        raise ValueError('阿里确认截图为空、格式无效或超过飞书图片大小限制。')
    try:
        access_token = token_manager.get_tenant_token()
        if not access_token or current_app_credentials(refresh=False) != credentials:
            raise ValueError('飞书应用已变化。')
        response = _HTTP.request(
            'POST', 'https://open.feishu.cn/open-apis/im/v1/images',
            headers={'Authorization': 'Bearer ' + access_token},
            data={'image_type': 'message'}, files={'image': (name, content, mime_type)},
            timeout=10,
        )
        body = response.json()
        key = (body.get('data') or {}).get('image_key')
        if (not 200 <= response.status_code < 300 or type(body.get('code')) is not int or body['code'] != 0 or
                not isinstance(key, str) or not _IMAGE_KEY.fullmatch(key)):
            raise ValueError('飞书未返回有效的图片标识。')
    except Exception:
        raise ValueError('阿里确认截图上传飞书消息失败，等待重试。') from None
    return key


def _identity(app_id, task):
    return hashlib.sha256((app_id + ':' + task['target_record_id'] + ':'
                           + str(task.get('screenshot_fingerprint') or '')).encode()).hexdigest()


def prepare(runtime,task,recipient,link, *, credentials=None):
    from .eop_scenario_editor import plain
    from upload_event_module.services.robot_webhook import current_app_credentials

    credentials = credentials or current_app_credentials()
    app_id=credentials[0]
    if not app_id or not recipient:raise ValueError('飞书应用或接收人员尚未配置。')
    fingerprint=str(task.get('screenshot_fingerprint') or '')
    tokens = list(dict.fromkeys(task.get('screenshot_tokens') or []))
    if not fingerprint or not tokens:
        raise ValueError('阿里确认截图尚未核实，等待重试。')
    identity = _identity(app_id, task)
    target_key = 'target:' + identity
    existing=runtime.state_store.get_document(NS,target_key)
    if not existing:
        token=secrets.token_urlsafe(32)
        existing={'token':token,'record_id':task['target_record_id'],'fingerprint':fingerprint,'app_id':app_id,'recipient':recipient,'link':link}
    if not existing.get('image_keys'):
        image_map = existing.setdefault('image_map', {})
        for file_token in tokens:
            if file_token not in image_map:
                image_map[file_token] = _upload_card_image(runtime, task['target_record_id'], file_token, credentials)
                runtime.state_store.put_document(NS, target_key, existing)
        existing.update(link=link, image_keys=[image_map[file_token] for file_token in tokens],
                        display={'title': plain(task.get('title')), 'building': plain(task.get('building')),
                                 'started_at': format_start_time(task.get('started_at'))})
        callback_key = hashlib.sha256(existing['token'].encode()).hexdigest()
        runtime.state_store.put_documents(NS, {target_key: existing, callback_key: existing})
    # The shared delivery ledger binds the UUID to the exact card content.
    return render(existing['display'], existing['token'], existing['link'], image_keys=existing['image_keys']), str(
        uuid.uuid5(uuid.NAMESPACE_URL, 'change-screenshot-images:' + identity)
    )

def send(runtime,task,recipient,link):
    from .server import external_real_write_guard
    from upload_event_module.services.robot_webhook import current_app_credentials, send_interactive_to_open_ids
    guard=external_real_write_guard()
    if guard.get('mock_external'):return True,'mock external send skipped',[]
    if not guard.get('real_write_allowed'):return False,str(guard.get('reason') or '真实外部写入未确认'),[]
    credentials = current_app_credentials()
    identity = _identity(credentials[0], task)
    existing = runtime.state_store.get_document(NS, 'target:' + identity) or {}
    if existing and not existing.get('image_keys'):
        from .feishu_delivery import PublicFeishuSender

        old_uuid = str(uuid.uuid5(uuid.NAMESPACE_URL, 'change-screenshot:' + identity))
        route_key = PublicFeishuSender.route_key(existing['recipient'], 'open_id', old_uuid)
        prior = runtime.state_store.get_document('feishu_public_delivery', route_key) or {}
        if prior.get('status') == 'sent' or (
            task.get('h_notification_fingerprint') == task.get('screenshot_fingerprint')
            and task.get('h_notification_sent_at')
        ):
            return True, '原确认卡已送达', []
        if prior and prior.get('status') != 'not_sent':
            return False, '原确认卡片投递结果待核对，等待核验。', []
    try:
        card, message_uuid = prepare(runtime, task, recipient, link, credentials=credentials)
    except ValueError as exc:
        return False, str(exc), []
    if current_app_credentials(refresh=False) != credentials:
        return False, '飞书应用已变化，等待使用当前应用重试。', []
    return send_interactive_to_open_ids(card,[recipient],message_uuid=message_uuid)

def apply(runtime,data,app_id):
    from .portal_service import BUILDING_OPEN_ID_MAP
    from .feishu_delivery import current_recipient_open_id
    header,event=data.get('header') or {},data.get('event') or {}
    value=(event.get('action') or {}).get('value') or {}
    sender=(event.get('operator') or {}).get('open_id') or ''
    token=value.get('token')
    if header.get('app_id')!=app_id or header.get('event_type')!='card.action.trigger' or value.get('kind')!='change_screenshot_confirm' or not isinstance(token,str) or len(token)!=43:
        raise ValueError('确认卡片回调无效。')
    row=runtime.state_store.get_document(NS,hashlib.sha256(token.encode()).hexdigest()) or {}
    if row.get('app_id')!=app_id:raise ValueError('卡片应用身份无效，请重新打开灯塔。')
    current_recipient_open_id(runtime,sender,app_id)
    auth=runtime.auth_manager
    if auth._open_id_explicitly_disabled(sender):raise ValueError('当前账号已停用。')
    session={'user':{'open_id':sender},'role':auth.role_for_open_id(sender)}
    if not (sender==str(BUILDING_OPEN_ID_MAP.get('H') or '') or auth.is_admin(session)):
        raise ValueError('只有管理员或H楼账号可以确认。')
    task=runtime.confirm_change_confirmation(row['record_id'],actor_open_id=sender,actor_name='飞书卡片确认',
        expected_fingerprint=row['fingerprint'],allow_recently_ended=True)
    runtime.clear_payload_cache()
    display = dict(row.get('display') or {})
    display.update({key: task[key] for key in ('title', 'building', 'started_at') if task.get(key)})
    return {'ok': True, 'toast': '阿里截图已确认',
            'card': render(display, token, row['link'], True, image_keys=row.get('image_keys') or ())}

async def callback(runtime,data,app_id):
    # The loopback worker has a 2s request budget. Leave time for transport
    # and serialization so a slow cloud write can return a pending receipt.
    try:return await asyncio.wait_for(asyncio.to_thread(apply,runtime,data,app_id),timeout=1.6)
    except asyncio.TimeoutError:return {'ok':False,'toast':'确认结果尚在核对，请稍后重试原卡片或打开灯塔查看。'}
    except Exception as exc:
        from .portal_service import PortalError
        return {'ok':False,'toast':str(exc) if isinstance(exc,(ValueError,PortalError)) else '确认尚未完成，请重试原卡片或打开灯塔核对。'}
