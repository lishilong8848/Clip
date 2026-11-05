"""Interactive Ali screenshot confirmation backed by the original record operation."""
import asyncio
import datetime as dt
import hashlib
import secrets
import time
import uuid
from zoneinfo import ZoneInfo

NS='change_screenshot_cards'

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

def render(task, token, link, done=False):
    from .eop_scenario_editor import plain
    text=lambda s:{'tag':'plain_text','content':s}
    elements=[{'tag':'div','text':text('名称：'+plain(task.get('title'))+'\n楼栋：'+plain(task.get('building')))},
      {'tag':'div','text':text('已确认阿里截图。' if done else '截图已上传，无需重复上传。请核对后确认；通告实际结束两小时后，卡片确认失效。')}]
    if not done:elements.append({'tag':'button','text':text('确认阿里截图'),'type':'primary','behaviors':[{'type':'callback','value':{'kind':'change_screenshot_confirm','token':token}}]})
    if link:elements.append({'tag':'button','text':text('打开灯塔处理'),'type':'default','behaviors':[{'type':'open_url','default_url':link}]})
    return {'schema':'2.0','header':{'title':text('变更阿里确认截图'+('已确认' if done else '待确认')),'template':'green' if done else 'blue'},'body':{'elements':elements}}

def prepare(runtime,task,recipient,link):
    from upload_event_module.services.robot_webhook import current_app_credentials
    app_id=current_app_credentials()[0]
    if not app_id or not recipient:raise ValueError('飞书应用或接收人员尚未配置。')
    fingerprint=str(task.get('screenshot_fingerprint') or '')
    identity=hashlib.sha256((app_id+':'+task['target_record_id']+':'+fingerprint).encode()).hexdigest()
    existing=runtime.state_store.get_document(NS,'target:'+identity)
    if not existing:
        token=secrets.token_urlsafe(32)
        existing={'token':token,'record_id':task['target_record_id'],'fingerprint':fingerprint,'app_id':app_id,'recipient':recipient,'link':link}
        runtime.state_store.put_documents(NS,{'target:'+identity:existing,hashlib.sha256(token.encode()).hexdigest():existing})
    return render(task,existing['token'],link),str(uuid.uuid5(uuid.NAMESPACE_URL,'change-screenshot:'+identity))

def send(runtime,task,recipient,link):
    from .server import external_real_write_guard
    from upload_event_module.services.robot_webhook import send_interactive_to_open_ids
    guard=external_real_write_guard()
    if guard.get('mock_external'):return True,'mock external send skipped',[]
    if not guard.get('real_write_allowed'):return False,str(guard.get('reason') or '真实外部写入未确认'),[]
    card,identity=prepare(runtime,task,recipient,link)
    return send_interactive_to_open_ids(card,[recipient],message_uuid=identity)

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
    return {'ok':True,'toast':'阿里截图已确认','card':render(task,token,row['link'],True)}

async def callback(runtime,data,app_id):
    try:return await asyncio.wait_for(asyncio.to_thread(apply,runtime,data,app_id),timeout=2)
    except asyncio.TimeoutError:return {'ok':False,'toast':'确认结果尚在核对，请稍后重试原卡片或打开灯塔查看。'}
    except Exception as exc:
        from .portal_service import PortalError
        return {'ok':False,'toast':str(exc) if isinstance(exc,(ValueError,PortalError)) else '确认尚未完成，请重试原卡片或打开灯塔核对。'}
