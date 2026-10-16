"""Read existing business records; never infer personal ownership from a building."""
import calendar
import datetime as dt
import hashlib
from urllib.parse import urlencode

from .work_handover import HandoverError, authorized, timestamp

NOTICE_TYPES = {'maintenance','change','repair','polling','adjust','power'}
SOURCES = {'maintenance':'维保通告','change':'变更通告','repair':'检修通告','polling':'设备轮巡',
           'adjust':'设备调整','power':'上下电通告','repair_management':'维修项目','event':'事件',
           'engineer_mop':'维护单','water':'水耗记录','drills':'演练','critical_guard':'风险与重保','cabinet':'机柜上下电'}


def title_of(row):
    fields = row.get('display_fields') or row.get('fields') or {}
    return str(row.get('title') or row.get('name') or row.get('notice_title') or row.get('meter_name')
               or next((fields.get(k) for k in ('维护总项','变更名称','维修项目','检修名称','事件名称') if isinstance(fields.get(k),str)), '') or '未命名业务事项')[:240]


def explicit_participation(row, actor):
    ids = {actor['id'], actor.get('open_id','')}
    def matches(value):
        if isinstance(value,dict):
            return any(isinstance(value.get(k),str) and value[k] in ids for k in ('id','open_id','record_id','person_id'))
        if isinstance(value,list):
            return any(matches(v) for v in value)
        return isinstance(value,str) and bool(value) and value in ids
    fields = row.get('raw_fields') or row.get('fields') or {}
    return any(matches(row.get(k)) for k in ('owner_id','operator','operator_id','created_by','updated_by','person_id','assignee','operator_open_id')) or any(matches(fields.get(k)) for k in ('负责人','操作人','维护人员','检修人','提交人'))


class HandoverSources:
    def __init__(self, runtime, controller, store):
        self.runtime, self.controller, self.store = runtime, controller, store

    def load(self, actor, module, scope, period):
        if module not in SOURCES or not authorized(actor,scope,module):
            raise HandoverError('没有该业务或楼栋的访问权限。',403)
        try:
            date = dt.date.fromisoformat(period+'-01')
        except (TypeError,ValueError):
            raise HandoverError('请选择有效月份。') from None
        svc, rows, warnings = self.runtime.service, [], []

        def append(raw, key, route, params, status='', kind=''):
            if not key:
                warnings.append('有记录缺少稳定标识，未纳入候选。')
                return
            status = str(status or raw.get('status') or raw.get('workflow') or raw.get('state') or '待核对')
            completed = status in {'已完成','已结束','维修完成','completed','synced','done'}
            pending = status in {'未开始','未记录','未执行','未上传','待处理','pending','draft'}
            rows.append({'id':hashlib.sha256(f'{module}:{scope}:{key}'.encode()).hexdigest(),
                'module':module,'scope':scope,'period':period,'title':title_of(raw),'business_status':status,
                'kind':kind or ('history' if completed else 'todo' if pending else 'ongoing'),
                'source_url':route+'?'+urlencode(params),'source_checked_at':timestamp(),
                'participated':explicit_participation(raw,actor)})

        if module in NOTICE_TYPES:
            svc.ensure_snapshot_loaded()
            if not svc._source_snapshot_exists(scope):
                warnings.append('计划快照尚未完整加载，当前只展示已有通告记录。')
            ongoing = self.controller._get_ongoing(scope)
            history = {}
            for day in range(1,calendar.monthrange(date.year,date.month)[1]+1):
                for row in svc.get_daily_summary(scope=scope,date=f'{period}-{day:02d}').get('items',[]):
                    key = row.get('active_item_id') or row.get('target_record_id') or row.get('key')
                    if key:
                        history[str(key)] = row
            for row in ongoing:
                key = row.get('active_item_id') or row.get('target_record_id') or row.get('key')
                if key:
                    history[str(key)] = row
            linked = set()
            for key,row in history.items():
                if svc._item_work_type(row)!=module:
                    continue
                if row.get('source_record_id'):
                    linked.add(row['source_record_id'])
                params={'scope':scope,'work_type':module}
                if row.get('active_item_id'):
                    params['active_item_id']=row['active_item_id']
                elif row.get('source_record_id'):
                    params['record_id']=row['source_record_id']
                else:
                    params['search']=title_of(row)
                append(row,'notice:'+key,'/workbench-lite',params)
            if date.year==dt.datetime.now(dt.timezone(dt.timedelta(hours=8))).year:
                for row in svc._workbench_records(month=f'{date.month}月',scope=scope):
                    rid=row.get('record_id')
                    if rid in linked or svc._record_work_type(row)!=module:
                        continue
                    progress = svc._source_record_progress(row) or '状态待核对'
                    kind = 'history' if svc._target_status_is_finished(progress) else 'ongoing' if svc._source_progress_is_ongoing(progress) else 'todo'
                    append(row,'plan:'+str(rid) if rid else '', '/workbench-lite',{'scope':scope,'work_type':module,'month':f'{date.month}月','record_id':rid},progress,kind)
            else:
                warnings.append('历史月份仅展示已有通告历史，不使用本年度计划冒充历史计划。')
        elif module=='repair_management':
            offset=0
            while True:
                data=svc.get_repair_management_records(scope=scope,limit=200,offset=offset,period='all')
                page=data.get('records',[])
                warnings.extend(data.get('warnings') or [])
                for row in page:
                    append(row,row.get('record_id'),'/repair-management',{'scope':scope,'record_id':row.get('record_id')},'状态待核对' if row.get('state_locked') else '')
                offset+=len(page)
                total=data.get('total', (data.get('pagination') or {}).get('total'))
                if not page or total is not None and offset>=int(total) or len(page)<200:
                    break
                if offset>=10000:
                    raise HandoverError('维修项目数量过多，请缩小原业务范围后添加具体事项。')
            warnings.append('维修项目按现有全量项目读取，不受月份筛选限制。')
        elif module=='event':
            data=svc.get_event_monthly_snapshot(scope=scope,month=period)
            if not data.get('snapshot_exists'):
                raise HandoverError('事件月度快照尚未加载，请先在事件管理中读取该月份。',503)
            for row in data.get('records',[]):
                append(row,row.get('record_id') or row.get('event_id'),'/',{'mode':'events','detail':'1','scope':scope,'month':period,'search':title_of(row)})
        elif module=='engineer_mop':
            for row in svc._engineer_month_maintenance_notices(scope=scope,ongoing_items=self.controller._get_ongoing(scope)):
                uploaded=bool(row.get('mop_uploaded') or row.get('mop_attachment_count'))
                append(row,row.get('notice_key'),'/engineer/mop',{'scope':scope,'notice_key':row.get('notice_key')},'已完成' if uploaded else '待处理')
            warnings.append('维护单候选采用维护单管理当前月份的数据。')
        elif module=='water':
            page_no=1
            while True:
                data=svc.list_water_consumption_records(scope=scope,month=period,page=page_no,page_size=100)
                page=data.get('records',[])
                for row in page:
                    append(row,row.get('record_id'),'/water-management',{'scope':scope,'water_view':'records','record_id':row.get('record_id'),'month':period},'已完成' if row.get('meter_value') not in (None,'') else '未记录')
                if not page or len(page)<100 or page_no*100>=int(data.get('total',1000000)):
                    break
                page_no+=1
                if page_no>100:
                    raise HandoverError('水耗记录过多，请在原业务中缩小范围。')
        elif module=='drills':
            from .drill_management import drill_assigned_scopes
            for row in self.controller._drills.list_definitions(period):
                if row.get('status')!='published' or scope not in drill_assigned_scopes(row):
                    continue
                try:
                    execution=self.controller._drills.get_execution(row['drill_id'],scope,create=False) or {}
                except Exception as exc:
                    if getattr(exc,'status_code',0)!=404:
                        raise
                    execution={}
                append(row,row['drill_id'],'/drill-management',{'scope':scope,'drill_id':row['drill_id'],'month':period},execution.get('status') or '未开始')
        elif module=='critical_guard':
            for row in svc.list_critical_guard_tasks(scope=scope).get('tasks',[]):
                append(row,row.get('task_id'),'/critical-guard',{'scope':scope,'task_id':row.get('task_id')},'已完成' if row.get('complete') is True else '待处理')
            warnings.append('风险与重保按现有活动任务读取。')
        elif module=='cabinet':
            page_no=1
            allowed=[s for s in 'ABCDE' if authorized(actor,s,module)]
            while True:
                data=self.controller._cabinet_power.batches.list(actor['open_id'],allowed,actor.get('role')=='admin',scope=scope,page=page_no,page_size=100)
                page=data.get('items',[])
                for row in page:
                    if row.get('status') in {'cancelled','canceled','deleted'}:
                        continue
                    if not set(row.get('scopes') or [scope]).issubset(allowed):
                        continue
                    append(row,row.get('batch_id'),'/cabinet-power/batches',{'scope':scope,'batch_id':row.get('batch_id')},'待处理' if row.get('is_todo') else '已完成')
                if not page or len(page)<100 or page_no*100>=int(data.get('total',1000000)):
                    break
                page_no+=1
            warnings.append('机柜按已有全部批次读取。')
        rows=list({row['id']:row for row in rows}.values())
        self.store.cache_sources(rows)
        return {'items':rows,'warnings':list(dict.fromkeys(warnings)),'loaded_at':timestamp()}
