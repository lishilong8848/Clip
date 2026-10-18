import type { Dict } from './client';
import { NOTICE_IDS, beijingDate } from './modules';
import { waterNumber, waterPhotoCount, waterRecordState } from './water';

export const todoCatalog = [
  { value:'notices', label:'通告' }, { value:'repair_management', label:'维修项目' },
  { value:'critical_guard', label:'重保填写' }, { value:'risk_inspection', label:'风险排查' },
  { value:'water', label:'水耗填写' }, { value:'learning', label:'我的学练' },
  { value:'signatures', label:'我的签名' },
] as const;
export type TodoKind = typeof todoCatalog[number]['value'];
export type TodoRow = { key:string; kind:TodoKind; module:string; title:string; scope:string; status:string; time:string; url:string; detail?:string; dueAt?:number; sourceKey?:string; ongoing?:boolean };
export type TodoPage = { rows:TodoRow[]; more:boolean; note?:string };
export type TodoSource = { id:string; page:number; more:boolean; load:(page:number, signal:AbortSignal)=>Promise<TodoPage> };
export type TodoContext = { user:Dict; scope:string; scopes:{value:string;label:string}[]; selectedScopes?:string[] };
export type TodoRead = (path:string, signal:AbortSignal)=>Promise<Dict>;

export function todoOptions(user:Dict) { const granted:string[]=user.modules||[]; return ['guest','temporary'].includes(user.role)?[]:todoCatalog.filter(item=>item.value==='notices'?NOTICE_IDS.some(id=>granted.includes(id)):granted.includes(item.value)); }
export function todoSelection(value:unknown,user:Dict):TodoKind[] {
  const available=todoOptions(user).map(item=>item.value);
  return Array.isArray(value)?[...new Set(value.filter((id):id is TodoKind=>available.includes(id)))]:available;
}
const text=(v:unknown):string=>Array.isArray(v)?v.map(text).join('、'):v&&typeof v==='object'?String((v as Dict).text||(v as Dict).name||''):String(v??'');
function rows(value:unknown):Dict[] { if(!Array.isArray(value))throw new Error('待办记录未完整返回，请重新读取。');return value; }
function number(value:unknown):number { const n=waterNumber(value);if(n===null||n<0)throw new Error('待办分页信息未完整返回，请重新读取。');return n; }
const link=(path:string,query:Dict)=>path+'?'+new URLSearchParams(Object.entries(query).filter(([,v])=>v!==''&&v!==undefined&&v!==null).map(([k,v])=>[k,String(v)])).toString();
const ended=(status:string)=>!/未结束|未完成|未闭环/.test(status)&&/已结束|正常结束|延期结束|闭环|已完成|维修完成|作废|删除|^(结束|完成|end|completed|deleted|cancelled)$/.test(status);
function title(row:Dict) { const f=row.display_fields||row.fields||{};return text(row.title||row.name||row.notice_title||f['维护总项']||f['名称（标题）']||f['维修名称']||f['名称'])||'未命名事项'; }
export function todoTimestamp(value:unknown):number|undefined {
  if(typeof value==='number')return Number.isFinite(value)&&value>0?(value<1e12?value*1000:value):undefined;
  const t=text(value).trim().replace(/\//g,'-').replace(' ','T');if(!t)return;
  const parsed=Date.parse(/(?:Z|[+-]\d{2}:?\d{2})$/.test(t)?t:t+(t.includes('T')?'':'T00:00:00')+'+08:00');return Number.isFinite(parsed)?parsed:undefined;
}
export function sortTodos(input:TodoRow[], now=Date.now()):TodoRow[] {
  const linked=new Set(input.filter(r=>r.ongoing&&r.sourceKey).map(r=>r.sourceKey));
  const unique=new Map(input.filter(r=>r.ongoing||!r.sourceKey||!linked.has(r.sourceKey)).map(row=>[row.key,row]));
  const priority=(r:TodoRow)=>r.dueAt&&r.dueAt<now?0:/退回|异常|失败/.test(r.status)?1:2;
  return [...unique.values()].sort((a,b)=>priority(a)-priority(b)||(a.dueAt??todoTimestamp(a.time)??Infinity)-(b.dueAt??todoTimestamp(b.time)??Infinity)||a.key.localeCompare(b.key));
}

export function todoSources(kind:TodoKind,ctx:TodoContext,get:TodoRead):TodoSource[] {
  if(!todoOptions(ctx.user).some(item=>item.value===kind))return [];
  if(ctx.selectedScopes&&!['learning','signatures'].includes(kind))return [...new Set(ctx.selectedScopes)].filter(scope=>ctx.scopes.some(option=>option.value===scope)).flatMap(scope=>
    todoSources(kind,{...ctx,scope,selectedScopes:undefined},get).map(source=>({...source,id:scope+':'+source.id})));
  const source=(id:string,load:TodoSource['load']):TodoSource=>({id,page:0,more:true,load});
  const read=(path:string,q:Dict,signal:AbortSignal)=>get(link(path,q),signal);
  const buildings=(supported:string)=>ctx.scopes.filter(s=>supported.includes(s.value)&&s.value.length===1&&(['ALL','CAMPUS',''].includes(ctx.scope)||ctx.scope===s.value)).map(s=>s.value);
  if(kind==='notices')return [source('notices',async(page,signal)=>{
    const month=Number(beijingDate().slice(5,7))+'月';
    const data=await read('/api/workbench',{scope:ctx.scope,work_type:'all',month,sections:'ongoing',ongoing_page:page,ongoing_page_size:30},signal);
    if(data.source_snapshot_ready===false)throw new Error('通告快照尚未准备完成。');
    const result:TodoRow[]=[];
    for(const row of rows(data.ongoing)){
      const type=String(row.work_type||'');if(!NOTICE_IDS.includes(type)||!(ctx.user.modules||[]).includes(type))continue;
      const status=text(row.status_label||row.workflow||row.source_progress||row.source_status||row.work_summary?.status||row.status||row.display_fields?.['维护实施状态']||row.display_fields?.['变更进度']);
      if(ended(status)||row.deleted||row.deleted_at)continue;
      const id=row.active_item_id||row.record_id||row.id;if(!id)throw new Error('通告缺少记录标识，暂不能办理。');
      const sourceId=row.source_record_id||row.record_id;
      result.push({key:`notice:${type}:active:${id}`,kind,module:'notices',title:title(row),scope:text(row.building||row.scope||row.building_codes)||ctx.scope,
        status:status||'进行中',time:text(row.start_time||row.time||row.updated_at),detail:({maintenance:'维保',change:'变更',repair:'检修',polling:'轮巡',adjust:'设备调整',power:'上下电'} as Dict)[type],
        ongoing:true,sourceKey:sourceId?type+':'+sourceId:undefined,dueAt:todoTimestamp(row.deadline),
        url:link('/workbench-lite',{scope:ctx.scope,work_type:type,month,[row.active_item_id?'active_item_id':'record_id']:id})});
    }
    return {rows:result,more:page<number(data.ongoing_pagination?.total_pages),note:'已发送且进行中的通告'};
  })];
  if(kind==='repair_management')return [source(kind,async(page,signal)=>{
    const data=await read('/api/repair-management/status',{scope:ctx.scope,state:'all',period:'all',limit:50,offset:(page-1)*50},signal);
    const result=rows(data.records).filter(row=>row.is_completed!==true&&!ended(text(row.workflow||row.status||row.state))).map(row=>{
      if(!row.record_id)throw new Error('维修项目缺少记录标识。');
      return {key:'repair:'+row.record_id,kind,module:kind,title:title(row),scope:text(row.building||row.building_codes||row.scope)||ctx.scope,status:text(row.workflow||row.status)||(row.state==='without_followup'?'未开始':'维修中'),time:text(row.fault_time||row.latest_followup_time||row.updated_at),detail:waterNumber(row.progress_percent)!==null?`进度 ${row.progress_percent}%`:undefined,url:link('/repair-management',{scope:ctx.scope,record_id:row.record_id,workspace_tab:'followups'})};
    });
    return {rows:result,more:(page-1)*50+rows(data.records).length<number(data.total)};
  })];
  if(kind==='critical_guard')return buildings('ABCDE').map(scope=>source(kind+scope,async(_page,signal)=>{
    const data=await read('/api/critical-guard/tasks',{scope},signal);
    return {rows:rows(data.tasks).flatMap(row=>{
      if(typeof row.complete!=='boolean')throw new Error(scope+'楼重保完成状态未返回。');
      if(row.complete||ended(text(row.status)))return [];
      if(!row.task_id)throw new Error('重保任务标识未返回。');
      return [{key:`guard:${scope}:${row.task_id}`,kind,module:kind,title:title(row),scope,status:'待填写',time:text(row.created_at||row.start_date),dueAt:todoTimestamp(row.deadline),url:link('/critical-guard',{scope,task_id:row.task_id})}];
    }),more:false};
  }));
  if(kind==='risk_inspection')return [source(kind,async(page,signal)=>{
    const data=await read('/api/risk-inspections',{scope:ctx.scope,page,view:'all'},signal);
    return {rows:rows(data.items).flatMap(row=>{
      const total=number(row.total),approved=number(row.approved),submitted=number(row.submitted);
      if(ended(text(row.status))||!row.can_claim&&(!total||approved>=total))return [];
      if(!row.id)throw new Error('风险排查任务标识未返回。');
      return [{key:'risk:'+row.id,kind,module:kind,title:title(row),scope:ctx.scope,status:row.status!=='active'?'发布处理中':row.can_claim?'待领取':submitted>=total?'待审核':'待填写',time:'',dueAt:todoTimestamp(row.deadline),detail:`已通过 ${approved} / ${total} 项`,url:link('/risk-inspections',{scope:ctx.scope,id:row.id})}];
    }),more:page*20<number(data.total)};
  })];
  if(kind==='water')return buildings('ABCDEH').map(scope=>source(kind+scope,async(page,signal)=>{
    const data=await read('/api/capacity/water/records',{scope,page,page_size:50},signal);
    if(data.snapshot?.exists===false)throw new Error(scope+'楼水耗快照尚未准备完成。');
    const list=rows(data.records);
    return {rows:list.flatMap(row=>{
      if(waterNumber(row.meter_value)!==null&&(waterPhotoCount(row)??0)>0)return [];
      if(!row.record_id)throw new Error('水耗记录标识未返回。');
      return [{key:`water:${scope}:${row.record_id}`,kind,module:kind,title:text(row.meter)||'水表未填写',scope,status:waterRecordState(row).label,time:text(row.statistic_date||row.statistic_date_key),detail:[row.frequency,row.shift&&row.shift+'班'].filter(Boolean).join(' · '),url:link('/water-management',{scope,water_view:'records',record_id:row.record_id})}];
    }),more:(page-1)*50+list.length<number(data.total)};
  }));
  if(kind==='learning'){
    let self:Dict|undefined;
    return [source(kind,async(page,signal)=>{
      self??=await get('/api/learning/bootstrap',signal);
      if(!self.self_person?.id)return {rows:[],more:false,note:'当前账号尚未关联个人学练身份'};
      const data=await read('/api/learning/papers',{person_id:self.self_person.id,scope:self.self_scope,page,page_size:20},signal);
      return {rows:rows(data.items).flatMap(row=>{
        if(row.person_id!==self!.self_person.id||row.legacy||row.status==='completed'||row.deleted_at)return [];
        if(row.status!=='pending'||!row.id)throw new Error('个人题单状态未完整返回。');
        return [{key:'learning:'+row.id,kind,module:kind,title:row.mode==='practice'?'自主练习':'每日学练',scope:text(row.scope),status:row.started_at?'答题中':'未开始',time:text(row.date),detail:`已答 ${number(row.stats?.answered)} / ${number(row.stats?.total)} 题`,url:link('/learning',{view:'practice',paper_id:row.id})}];
      }),more:page*number(data.page_size)<number(data.total),note:'仅本人已分配或已领取的题单'};
    })];
  }
  return [source('signatures',async(_page,signal)=>{
    const data=await get('/api/signatures/management/account',signal),p=data.person;
    if(!p)return {rows:[],more:false,note:text(data.reason)||'当前账号暂无个人签名身份'};
    if(p.is_self!==true)throw new Error('未取得本人签名信息。');
    if(!['signed','unsigned','resign'].includes(p.signature_status))throw new Error('本人签名状态暂未确认。');
    return {rows:p.signature_status==='signed'?[]:[{key:'signature:self',kind,module:'signatures',title:p.signature_status==='resign'?'重新填写个人签名':'填写个人签名',scope:text(p.building),status:p.signature_status==='resign'?'需重新签名':'未签名',time:text(p.request?.created_at),url:'/signature-management?self=1'}],more:false};
  })];
}
