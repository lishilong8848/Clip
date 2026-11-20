import type { Dict } from './client';
import { NOTICE_IDS, beijingDate } from './modules';
import { waterDefaultFilters, waterNumber, waterPhotoCount, waterQuery, waterRecordState } from './water';
import { noticeListSpecialty, noticeListStatus, noticePlanWindow } from './noticeListMeta';

export const todoCatalog = [
  { value:'notices', label:'通告' }, { value:'repair_management', label:'维修项目' },
  { value:'critical_guard', label:'重保填写' }, { value:'risk_inspection', label:'专项排查' },
  { value:'water', label:'水耗填写' }, { value:'learning', label:'我的学练' },
  { value:'signatures', label:'我的签名' },
] as const;
export type TodoKind = typeof todoCatalog[number]['value'];
export type TodoRow = { key:string; kind:TodoKind; module:string; title:string; scope:string; status:string; time:string; url:string; detail?:string; dueAt?:number; sourceKey?:string; ongoing?:boolean; noticeWindow?:{start:string;end:string}; actorIds?:string[]; ownerLevel?:string; scheduledAt?:string|number; specialty?:string; referencedScopes?:string[] };
export type TodoPage = { rows:TodoRow[]; more:boolean; note?:string; partial?:boolean };
export type TodoSource = { id:string; page:number; more:boolean; load:(page:number, signal:AbortSignal)=>Promise<TodoPage> };
export type TodoContext = { user:Dict; scope:string; scopes:{value:string;label:string}[]; selectedScopes?:string[]; home?:boolean; noticeTypes?:string[] };
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
const noticeTypeLabels:Record<string,string>={maintenance:'维保',change:'变更',repair:'检修',polling:'轮巡',adjust:'设备调整',power:'上下电'};
const ownership=(row:Dict)=>({
  actorIds:[row.created_by_open_id,row.creator_open_id,row.assignee_open_id,row.owner_open_id,
    ...(Array.isArray(row.assignee_open_ids)?row.assignee_open_ids:[])].filter((id):id is string=>typeof id==='string'&&!!id),
  ownerLevel:typeof row.owner_level==='string'?row.owner_level:undefined,
});
function title(row:Dict) { const f=row.display_fields||row.fields||{};return text(row.title||row.name||row.notice_title||f['维护总项']||f['名称（标题）']||f['维修名称']||f['名称'])||'未命名事项'; }
export function todoTimestamp(value:unknown):number|undefined {
  if(typeof value==='number')return Number.isFinite(value)&&value>0?(value<1e12?value*1000:value):undefined;
  const t=text(value).trim().replace(/\//g,'-').replace(' ','T');if(!t)return;
  if(/^\d{10}(?:\d{3})?$/.test(t))return todoTimestamp(Number(t));
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
  const allowedNotice=(type:string)=>NOTICE_IDS.includes(type)&&(ctx.user.modules||[]).includes(type)&&(!ctx.noticeTypes||ctx.noticeTypes.includes(type));
  if(kind==='notices')return (ctx.noticeTypes?.filter(allowedNotice)??['all']).map(workType=>source('notices:'+workType,async(page,signal)=>{
    const month=Number(beijingDate().slice(5,7))+'月';
    const data=await read('/api/workbench',{scope:ctx.scope,work_type:workType,month,sections:'ongoing',ongoing_page:page,ongoing_page_size:ctx.home?0:30},signal);
    const result:TodoRow[]=[];
    let partial=false;
    for(const row of rows(data.ongoing)){
      const type=String(row.work_type||'');if(!allowedNotice(type)||(workType!=='all'&&type!==workType))continue;
      const targetStatus=text(row.status||row.status_label||row.work_summary?.status||row.workflow).trim();
      if(ended(targetStatus)||row.action==='end'||row.deleted||row.deleted_at)continue;
      const status=noticeListStatus(row,'ongoing').label;
      const id=row.active_item_id||row.record_id||row.id;if(!id){partial=true;continue;}
      const sourceId=row.source_record_id||row.record_id;
      const window=noticePlanWindow(row);
      const referencedScopes:Array<string>=Array.isArray(row.building_codes)?row.building_codes.filter(code=>/^(?:[A-EH]|110)$/.test(code)):[];
      result.push({...ownership(row),key:`notice:${type}:active:${id}`,kind,module:'notices',title:title(row),scope:text(row.building||row.scope||row.building_codes)||ctx.scope,
        status,time:text(row.start_time||row.time||row.updated_at),detail:noticeTypeLabels[type],noticeWindow:{...window,end:window.end||text(row.end_time)},referencedScopes,
        ongoing:true,sourceKey:sourceId?type+':'+sourceId:undefined,dueAt:todoTimestamp(row.deadline),specialty:noticeListSpecialty(row),
        url:link('/workbench-lite',{scope:ctx.scope,work_type:type,month,[row.active_item_id?'active_item_id':'record_id']:id})});
    }
    return {rows:result,more:!ctx.home&&page<number(data.ongoing_pagination?.total_pages),partial,note:'进行中的通告'};
  }));
  if(kind==='repair_management')return [source(kind,async(page,signal)=>{
    const limit=ctx.home?200:50, offset=(page-1)*limit;
    const data=await read('/api/repair-management/status',{scope:ctx.scope,state:'all',period:'all',limit,offset},signal);
    const list=rows(data.records);
    const result=list.filter(row=>row.is_completed!==true&&row.state!=='completed'
      &&(row.state==='in_progress'||row.state==='without_followup'||!ended(text(row.workflow||row.status||row.state)))).map(row=>{
      if(!row.record_id)throw new Error('维修项目缺少记录标识。');
      const time=ctx.home
        ? [row.repair_start_time,row.fault_time,row.project_created_time,row.latest_followup_time,row.updated_at].map(text).find(value=>todoTimestamp(value)!==undefined)||''
        : text(row.fault_time||row.latest_followup_time||row.updated_at);
      return {...ownership(row),key:'repair:'+row.record_id,kind,module:kind,title:text(row.repair_title)||title(row),scope:text(row.building||row.building_codes||row.scope)||ctx.scope,status:text(row.status_label||row.workflow||row.status)||(row.state==='without_followup'?'未开始':'维修中'),time,specialty:text(row.specialty),detail:waterNumber(row.progress_percent)!==null?`进度 ${row.progress_percent}%`:undefined,url:link('/repair-management',{scope:ctx.scope,record_id:row.record_id,workspace_tab:'followups'})};
    });
    const more=offset+list.length<number(data.total);
    if(ctx.home&&more&&list.length!==limit)throw new Error('维修项目分页未完整返回。');
    return {rows:result,more};
  })];
  if(kind==='critical_guard')return buildings('ABCDE').map(scope=>source(kind+scope,async(_page,signal)=>{
    const data=await read('/api/critical-guard/tasks',{scope},signal);
    return {rows:rows(data.tasks).flatMap(row=>{
      if(typeof row.complete!=='boolean')throw new Error(scope+'楼重保完成状态未返回。');
      if(row.complete||ended(text(row.status)))return [];
      if(!row.task_id)throw new Error('重保任务标识未返回。');
      return [{key:`guard:${scope}:${row.task_id}`,kind,module:kind,title:title(row),scope,status:'待填写',time:text(row.created_at||row.start_date),scheduledAt:text(row.start_date),dueAt:todoTimestamp(row.deadline),url:link('/critical-guard',{scope,task_id:row.task_id})}];
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
    const params=ctx.home?waterQuery(scope,waterDefaultFilters(),page):new URLSearchParams({scope,page:String(page),page_size:'50'});
    if(ctx.home){params.set('incomplete','1');params.set('latest_only','1');}
    const data=await get('/api/capacity/water/records?'+params,signal);
    const list=rows(data.records);
    return {rows:list.flatMap(row=>{
      if(waterNumber(row.meter_value)!==null&&(waterPhotoCount(row)??0)>0)return [];
      if(!row.record_id)throw new Error('水耗记录标识未返回。');
      return [{key:`water:${scope}:${row.record_id}`,kind,module:kind,title:text(row.meter)||'水表未填写',scope,status:waterRecordState(row).label,time:text(row.statistic_date||row.statistic_date_key),detail:[row.frequency,row.shift&&row.shift+'班'].filter(Boolean).join(' · '),url:link('/water-management',{scope,water_view:'records',record_id:row.record_id})}];
    }),more:(page-1)*50+list.length<number(data.total),partial:data.snapshot?.exists===false};
  }));
  if(kind==='learning'){
    let self:Dict|undefined;
    return [source(kind,async(page,signal)=>{
      if(!self||ctx.home&&page===1)self=await get('/api/learning/bootstrap',signal);
      const person=self.self_person, exemption=self.daily_exemption?.status==='exempt' ? self.daily_exemption : null;
      if(!self.self_person?.id)return {rows:[],more:false,note:'当前账号尚未关联个人学练身份'};
      const scope=self.self_scope;
      if(ctx.home && (!ctx.scopes.some(option=>option.value===scope)||!['ALL','CAMPUS',scope].includes(ctx.scope)))return {rows:[],more:false};
      const data=await read('/api/learning/papers',{person_id:self.self_person.id,scope:self.self_scope,page,page_size:20},signal);
      return {rows:rows(data.items).flatMap(row=>{
        if(row.person_id!==person.id||row.legacy||row.status==='completed'||row.deleted_at)return [];
        if(ctx.home&&(row.daily_exempt===true||exemption)&&row.mode!=='practice')return [];
        if(row.status!=='pending'||!row.id)throw new Error('个人题单状态未完整返回。');
        return [{key:'learning:'+row.id,kind,module:kind,title:row.mode==='practice'?'自主练习':'每日学练',scope:text(row.scope),status:row.started_at?'答题中':'未开始',time:text(row.date),detail:`已答 ${number(row.stats?.answered)} / ${number(row.stats?.total)} 题`,url:link('/learning',{view:'practice',paper_id:row.id})}];
      }),more:page*number(data.page_size)<number(data.total),note:ctx.home&&exemption
        ? '入职首月免做当班题单'+(/^\d{4}-\d{2}-\d{2}$/.test(exemption.resume_date)?' · '+exemption.resume_date+' 起按排班安排':'')
        : '仅本人已分配或已领取的题单'};
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
