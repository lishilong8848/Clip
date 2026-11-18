import { eventStatusTone } from '../../eventManagementUtils';
import { beijingDate } from '../modules';
import { todoOptions, todoSources, sortTodos, todoTimestamp, type TodoContext, type TodoRead, type TodoRow } from '../businessTodos';
import type { Dict } from '../client';
import type { Role, Section } from './recipes';
import { mopNoticeNeedsAction, noticeIsEnded } from '../../mopSelectionUtils';

export type SourceId = TodoRow['kind'] | 'events' | 'event_notices' | 'drills' | 'engineer_mop' | 'cabinet' | 'tool_management' | 'accounts' | 'ali_confirmation' | 'inspection';
export type WorkItem = Omit<TodoRow, 'kind'> & { kind: SourceId; referencedScopes?:string[]; readOnly?:boolean };
export type SourceResult = { rows: WorkItem[]; more?: boolean; delayed?: boolean; note?: string };
export type Source = {
  id: SourceId;
  label: string;
  section: Exclude<Section, 'urgent'>;
  url: string;
  readOnly?: boolean;
  load: (signal: AbortSignal, pages: number) => Promise<SourceResult>;
};
export const scopedUrl = (path: string, args: Record<string,string>) => path+'?'+new URLSearchParams(args);
const labels = {
  events: '处理中事件', notices:'通告待办', repair_management:'待跟进维修',
  critical_guard:'待填写重保', risk_inspection:'专项排查待办',
  water:'水耗待补', learning:'个人学练', signatures:'个人签名',
  event_notices:'事件通告', drills:'待完成演练', engineer_mop:'待处理维护单', cabinet:'待确认机柜批次',
  tool_management:'待办工具记录', accounts:'待审权限申请', ali_confirmation:'阿里截图确认', inspection:'日常巡检',
};
const sections: Record<SourceId,Source['section']> = {
  events:'operations', notices:'business', repair_management:'business',
  critical_guard:'operations', risk_inspection:'business', water:'operations', learning:'people', signatures:'people',
  event_notices:'business', drills:'operations', engineer_mop:'operations', cabinet:'operations', tool_management:'operations', accounts:'people', ali_confirmation:'business', inspection:'operations',
};
const managerNoticeTypes = ['change','repair','power'];
type HomeContext = TodoContext & {role:Role; pending?:SourceId[]; accessLevel?:string};
const array = (value: unknown): Dict[] => {
  if (!Array.isArray(value)) throw new Error('待办数据未完整返回。');
  return value;
};

export function workbenchSources(ctx: HomeContext, get: TodoRead): Source[] {
  if (!ctx.scope) return [];
  const scoped = ['ALL','CAMPUS'].includes(ctx.scope) || /^[A-EH]$/.test(ctx.scope);
  const month = Number(beijingDate().slice(5,7))+'月';
  const requestScopes=ctx.selectedScopes?.length?ctx.selectedScopes:[ctx.scope];
  const link = (path: string, query: Record<string,string> = {}) => scopedUrl(path, {scope:requestScopes[0],...query});
  const enabled = (id:SourceId,grant:string=id) => (!ctx.pending||ctx.pending.includes(id))
    && (ctx.user.modules||[]).includes(grant);
  const urls = {
    notices: link('/workbench-lite',{work_type:'all',month}), repair_management:link('/repair-management',{workspace_tab:'followups'}),
    critical_guard:link('/critical-guard'), risk_inspection:link('/risk-inspections'),
    water:link('/water-management',{water_view:'records'}), learning:'/learning?view=practice', signatures:'/signature-management?self=1',
  };
  const sources: Source[] = todoOptions(ctx.user).flatMap(({value:id}) => {
    if (ctx.pending&&!ctx.pending.includes(id) || id==='learning'&&ctx.user.category==='duty') return [];
    if (!scoped && ['critical_guard','water'].includes(id)) return [];
    if (id==='signatures' && ctx.user.impersonation) return [];
    const readOnly=ctx.role==='manager'&&id==='notices'&&!ctx.accessLevel;
    const available = todoSources(id,{...ctx,home:true,noticeTypes:readOnly?managerNoticeTypes:undefined},get);
    if (!available.length) return [];
    return [{
      id,label:readOnly?'通告信息':labels[id],section:sections[id],url:urls[id],readOnly,
      async load(signal: AbortSignal, pages: number): Promise<SourceResult> {
        const settled = await Promise.allSettled(available.map(async source => {
          const rows: TodoRow[] = [];
          let result;
          let partial=false;
          // A partial read can offer real actions, but must not become a total.
          for (let page=1; page<=pages; page++) {
            signal.throwIfAborted();
            try { result = await source.load(page,signal); }
            catch(error) {
              if (!result||signal.aborted||[401,403].includes((error as {status?:number})?.status||0)) throw error;
              partial=true;
              break;
            }
            partial ||= result.partial===true;
            rows.push(...result.rows);
            if (!result.more) break;
          }
          return {rows,more:result?.more,note:result?.note,partial};
        }));
        const lists = settled.flatMap(result=>result.status==='fulfilled'?[result.value]:[]);
        if (!lists.length) throw (settled[0] as PromiseRejectedResult).reason;
        const rows = sortTodos(lists.flatMap(list=>list.rows));
        const matches = (row: TodoRow) => id!=='signatures' || ['ALL','CAMPUS'].includes(ctx.scope)
          || row.scope.replace(/楼|栋|站/g,'').trim()===ctx.scope;
        return {
          rows:rows.filter(matches),more:lists.some(list=>list.more),
          delayed:lists.length!==available.length||lists.some(list=>list.partial),
          note:lists.length===1 ? lists[0]?.note : undefined,
        };
      },
    }];
  });
  if (enabled('events','event')) {
    const url=link('/',{mode:'events',detail:'1'});
    sources.unshift({
      id:'events',label:labels.events,section:sections.events,url,
      async load(signal) {
        return byScope(requestScopes, async scope=>{
          const data = await get(link('/api/events/monthly',{scope,month:beijingDate().slice(0,7)}),signal);
          if (data.snapshot_exists!==true) return {rows:[],delayed:true};
          if (data.scope!==scope || !Array.isArray(data.records)) throw new Error('事件范围或明细未确认');
          const rows: WorkItem[] = data.records.flatMap((record: Dict) => {
          if (eventStatusTone(record.status)!=='processing') return [];
          const id=record.source_record_id||record.record_id||record.id;
          const title=typeof record.title==='string' ? record.title.trim() : '';
          if (!id || !title) throw new Error('事件明细未确认');
          return [{
            key:'event:'+id,kind:'events',module:'event',title,
            scope:typeof record.building==='string'&&record.building?record.building:scope,status:'处理中',
            time:String(record.occurrence_time||''),dueAt:todoTimestamp(record.deadline),
            url:link('/',{scope,mode:'events',detail:'1',event_id:String(id)}),
          }];
        });
          return {rows,delayed:!!data.config_missing||!!Object.keys(data.last_failed||{}).length};
        });
      },
    });
  }

  function add(id:SourceId, url:string, scopes:string[], read:(scope:string,signal:AbortSignal,pages:number)=>Promise<SourceResult>) {
    if (scopes.length) sources.push({id,label:labels[id],section:sections[id],url,
      load:(signal,pages)=>byScope(scopes,scope=>read(scope,signal,pages))});
  }
  const buildings = ctx.scopes.map(option=>option.value).filter(scope=>/^[A-EH]$/.test(scope)
    && (['ALL','CAMPUS'].includes(ctx.scope)||scope===ctx.scope));
  const operated = buildings.filter(scope=>scope!=='H');
  const row = (kind:SourceId,id:string,scope:string,title:string,status:string,url:string,details:Partial<WorkItem>={}): WorkItem =>
    ({key:kind+':'+scope+':'+id,kind,module:kind,title,scope,status,time:'',url,...details});
  if (ctx.role==='duty'&&enabled('event_notices','event')) {
    add('event_notices',link('/workbench-lite',{work_type:'event'}),requestScopes,async(scope,signal)=>{
      const data=await get(link('/api/workbench-events',{scope,work_type:'event'}),signal);
      return {rows:array(data.items).flatMap(item=>{
        if (!item.event_id||item.state!=='active') return [];
        const title=String(item.metadata?.summary||item.text||'事件通告').split('\n').slice(0,2).join(' ').slice(0,160);
        return [row('event_notices',item.event_id,scope,title,'进行中',link('/workbench-lite',{scope,work_type:'event',event_id:item.event_id}),{ongoing:true,detail:'事件通告'})];
      }),delayed:Boolean(data.warning)};
    });
  }
  if (ctx.user.category==='duty' && ctx.user.home_scope==='H' && (ctx.scope==='H'||['ALL','CAMPUS'].includes(ctx.scope)) && enabled('ali_confirmation','change')) {
    const url=link('/workbench-lite',{work_type:'change'});
    add('ali_confirmation',url,['H'],async(scope,signal)=>{
      const data=await get('/api/change-confirmations',signal);
      if(data.can_confirm!==true) return {rows:[],delayed:true};
      return {rows:array(data.items).flatMap(item=>item.state==='awaiting_confirmation' && item.target_record_id
        ?[row('ali_confirmation',item.target_record_id,scope,item.title||'阿里截图待确认','待确认',
          scopedUrl('/workbench-lite',{scope,work_type:'change',change_confirmation:item.target_record_id}),
          {time:String(item.started_at||item.start_time||'')})]:[]),delayed:!!data.warning};
    });
  }
  if(ctx.accessLevel==='duty'&&['D','E'].includes(ctx.scope)&&enabled('inspection','daily')){
    const url=link('/daily-inspection');
    add('inspection',url,[ctx.scope],async(scope,signal)=>{
      const data=await get(scopedUrl('/api/daily-inspection/status',{view_scope:scope}),signal);
      if(data.view_scope!==scope||data.duty?.scope!==scope||!Number.isSafeInteger(data.next_round)||!Number.isSafeInteger(data.round_limit))
        return {rows:[],delayed:true};
      const sameShift=data.active?.duty?.date===data.duty.date&&data.active?.duty?.shift===data.duty.shift;
      const active=sameShift&&data.active?.pending===false;
      const round=active?data.active.round:data.next_round;
      if(!Number.isSafeInteger(round)||round<1) return {rows:[],delayed:true};
      if(round>data.round_limit) return {rows:[]};
      return {rows:[row('inspection',data.duty.date+':'+data.duty.shift+':'+round,scope,
        scope+'楼'+data.duty.shift+'班第 '+round+' 轮巡检',active?'进行中':'待开始',url,
        {detail:'巡检',scheduledAt:!active?data.opens_at:undefined})]};
    });
  }
  if (enabled('drills')) add('drills',link('/drill-management'),operated,async(scope,signal)=>{
    const month=beijingDate().slice(0,7);
    const data=await get(link('/api/drills',{scope,month}),signal);
    return {rows:array(data.items??data.drills).flatMap(item=>{
      if (item.status!=='published'||!item.drill_id||['synced','completed'].includes(item.execution?.status)) return [];
      return [row('drills',item.drill_id,scope,item.name||'演练任务',item.execution?.status==='error'?'生成异常':item.execution?.status==='sync_pending'?'待同步':'待完成',
        link('/drill-management',{scope,month,drill_id:item.drill_id}),
        {detail:'演练',scheduledAt:item.scheduled_at||item.start_time||item.plan_date,dueAt:todoTimestamp(item.deadline)})];
    })};
  });
  if (enabled('engineer_mop')) add('engineer_mop',link('/engineer/mop'),buildings,async(scope,signal)=>{
    const data=await get(link('/api/engineer/mop/bootstrap',{scope,month}),signal);
    return {rows:array(data.notices).flatMap(item=>{
      if (!item.notice_key||noticeIsEnded(item)||!mopNoticeNeedsAction(item)) return [];
      return [row('engineer_mop',item.notice_key,scope,item.title||item.name||'通告维护单',item.mop_binding?'待上传':'待绑定',
        link('/engineer/mop',{scope,month,notice_key:item.notice_key}))];
    })};
  });
  if (enabled('cabinet')) add('cabinet',link('/cabinet-power/batches',{status:'todo'}),operated,async(scope,signal,pages)=>{
    const rows:WorkItem[]=[];
    let more=false;
    for (let page=1;page<=pages;page++) {
      const data=await get(link('/api/cabinet-power/batches',{scope,status:'todo',page:String(page),page_size:'30'}),signal);
      for (const item of array(data.items)) {
        if (!item.batch_id||!item.is_todo||item.source_notice_deleted) continue;
        rows.push(row('cabinet',item.batch_id,scope,item.name||item.title||item.notice_title||'机柜上下电批次','待确认',
          link('/cabinet-power/batches',{scope,batch_id:item.batch_id,status:'todo'})));
      }
      more=page*30<Number(data.total);
      if (!more) break;
    }
    return {rows,more};
  });
  if ((ctx.role==='administrator'||['L0','L1'].includes(ctx.accessLevel||''))&&enabled('accounts')&&ctx.user.can_manage_accounts===true) {
    const url='/admin/settings?tab=permissions';
    sources.push({id:'accounts',label:labels.accounts,section:sections.accounts,url,async load(signal) {
      const data=await get('/api/auth/permission-requests/admin?status=pending&limit=200',signal);
      return {rows:array(data.items).flatMap(item=>item.request_id&&item.status==='pending'
        && (!ctx.accessLevel||['ALL','CAMPUS'].includes(ctx.scope)||item.requested_scopes?.includes(ctx.scope))
        ? [row('accounts',item.request_id,ctx.scope,String(item.name||item.applicant_name||'楼栋权限申请'),'待审核',
          scopedUrl('/admin/settings',{tab:'permissions',request_id:item.request_id}),{
          time:item.created_at,referencedScopes:Array.isArray(item.requested_scopes)?item.requested_scopes.filter((scope:unknown)=>typeof scope==='string'):undefined,
        })] : []),
        more:!!data.has_more};
    }});
  }
  if ((ctx.role==='administrator'||['L0','L1'].includes(ctx.accessLevel||''))&&ctx.pending?.includes('tool_management')&&ctx.user.tool_management_enabled===true) {
    const url=link('/risk-inspections',{view:'tool-self-check',tool_view:'followups'});
    sources.push({id:'tool_management',label:labels.tool_management,section:sections.tool_management,url,async load(signal,pages) {
      const rows:WorkItem[]=[];
      let cursor:Record<string,string>|null=null;
      for (let page=1;page<=pages;page++) {
        const data=await get(scopedUrl('/api/tool-self-check/workspace/history',cursor||{}),signal);
        for (const session of array(data.sessions)) {
          if (!session.id) continue;
          for (const item of array(session.history_items)) {
            if (!item.id||!buildings.includes(item.scope)||item.order_status==='cancelled') continue;
            const state=item.order_status==='review_pending'?'待审核':item.order_status==='pending'?'待填写'
              : item.archive_status==='retrying'?'归档待重试':item.archive_status==='verifying'?'归档待核验':'';
            if (!state) continue;
            rows.push(row('tool_management',session.id+':'+item.id,item.scope,item.asset_name||'工具办理记录',state,
              link('/risk-inspections',{scope:item.scope,view:'tool-self-check',tool_session:session.id,tool_asset:item.id})));
          }
        }
        cursor=data.history_cursor?.before&&data.history_cursor?.id?{before:data.history_cursor.before,id:data.history_cursor.id}:null;
        if (!cursor) break;
      }
      return {rows,more:!!cursor};
    }});
  }
  return sources;
}

async function byScope(scopes:string[], read:(scope:string)=>Promise<SourceResult>): Promise<SourceResult> {
  const settled=await Promise.allSettled(scopes.map(read));
  const results=settled.flatMap(result=>result.status==='fulfilled'?[result.value]:[]);
  if (!results.length) throw (settled[0] as PromiseRejectedResult).reason;
  return {
    rows:[...new Map(results.flatMap(result=>result.rows).map(row=>[row.key,row])).values()],
    more:results.some(result=>result.more), delayed:results.length!==scopes.length||results.some(result=>result.delayed),
  };
}
