import type { Dict } from '../client';
import { modules, navigationModules, NOTICE_IDS } from '../modules';
import { personaFor } from '../persona';
import { randomHexId } from '../../browserStorage';
import { chartViews,type ChartView } from '../vipboard';

export type WidgetKind = 'tasks' | 'shortcuts' | 'notices' | 'events' | 'learning' | 'water' | 'pre_shift';
export type Widget = {
  id:string; kind:WidgetKind; title:string; module:string; scope:string;
  display:ChartView|'number'; period:'week'|'month'; metric:'ongoing'|'pending';
  presentation:'default'|'timeline'|'progress'|'metrics'|'chart';
  links:string[]; x:number; y:number; w:number; h:number;
};
export type Layout = {version:1; widgets:Widget[]};
export const catalog = [
  {kind:'pre_shift',title:'班前检查',description:'本班检查清单、异常备注及提交状态',icon:'ClipboardList'},
  {kind:'tasks',title:'业务待办',description:'未开始、进行中与需关注事项',icon:'ClipboardList'},
  {kind:'shortcuts',title:'常用操作',description:'直接进入常用业务办理',icon:'LayoutGrid'},
  {kind:'notices',title:'通告分布',description:'按类型查看当前进行中或待发计划',icon:'ChartNoAxesCombined'},
  {kind:'events',title:'事件趋势',description:'本周或本月的每日事件与明细',icon:'ChartColumn'},
  {kind:'learning',title:'我的学练',description:'本人今日进度及近期答题情况',icon:'BookOpen'},
  {kind:'water',title:'水耗概览',description:'授权楼栋本月水耗及记录',icon:'Droplets'},
] as const;
export const motionTemplates = [
  {id:'timeline',kind:'tasks',title:'待办时间线',description:'当前业务事项、时间与状态，直接进入办理',icon:'ListTree',presentation:'timeline'},
  {id:'progress',kind:'learning',title:'学练进度环',description:'本人今日已答与剩余题量，继续今日学练',icon:'CircleCheck',presentation:'progress'},
  {id:'metrics',kind:'water',title:'水耗指标卡',description:'本月用量、记录及照片数量，点击查看明细',icon:'PanelsTopLeft',presentation:'metrics'},
] as const;
export const chartTemplates = [
  {id:'notice-donut',kind:'notices',title:'通告占比环',description:'按通告类型展示占比与合计',display:'donut',presentation:'default',preview:'donut'},
  {id:'notice-bars',kind:'notices',title:'通告横向对比',description:'长名称保持清晰，直接查看分类明细',display:'horizontal',presentation:'default',preview:'horizontal'},
  {id:'notice-pie',kind:'notices',title:'通告分类饼图',description:'查看各类型进行中或待发计划构成',display:'pie',presentation:'default',preview:'pie'},
  {id:'event-area',kind:'events',title:'事件面积趋势',description:'本周或本月逐日变化，点击查看当天事件',display:'area',presentation:'default',preview:'area'},
  {id:'event-line',kind:'events',title:'事件折线趋势',description:'沿时间轴查看事件数量变化',display:'line',presentation:'default',preview:'line'},
  {id:'water-bars',kind:'water',title:'楼栋水耗对比',description:'以吨为单位比较授权楼栋本月水耗',display:'horizontal',presentation:'chart',preview:'horizontal'},
  {id:'learning-donut',kind:'learning',title:'今日答题构成',description:'本人已答与未答，仅统计已领取题单',display:'donut',presentation:'chart',preview:'donut'},
] as const;
export function displayOptions(kind:WidgetKind){
  const allowed=kind==='events'?['bar','line','area']:['bar','horizontal','donut','pie'];
  return [...chartViews.filter(view=>allowed.includes(view.value)),{value:'number',label:'指标数值'}];
}
export function presentationOptions(kind:WidgetKind){
  return [{label:'标准',value:'default'},...motionTemplates.filter(t=>t.kind===kind).map(t=>({label:t.title,value:t.presentation})),...(['water','learning'].includes(kind)?[{label:kind==='water'?'楼栋水耗对比':'今日答题构成',value:'chart'}]:[])];
}
const taskIds=[...NOTICE_IDS,'event','repair_management','daily','plan_convergence','cabinet','engineer_mop','drills','critical_guard','learning'];
export function taskOptions(user:Dict){return modules.filter(m=>taskIds.includes(m.id)&&(user.modules||[]).includes(m.id)).map(m=>({value:m.id,label:m.label}));}
export function allowedKind(kind:WidgetKind,user:Dict){
  const granted:string[]=user.modules||[];
  if(kind==='pre_shift')return user.role!=='guest'&&granted.length>0&&(mandatoryCheck(user)||['L0','L1'].includes(user.level)||['supervisor','manager'].includes(user.category));
  return kind==='shortcuts'?navigationModules(granted).length>0:kind==='tasks'?taskOptions(user).length>0:kind==='notices'?NOTICE_IDS.some(id=>granted.includes(id)):granted.includes(kind==='events'?'event':kind);
}
export function widget(kind:WidgetKind,user:Dict,overrides:Partial<Widget>={}):Widget{
  return {id:randomHexId(),kind,title:catalog.find(c=>c.kind===kind)!.title,module:taskOptions(user)[0]?.value||'',scope:'current',display:'bar',period:'week',metric:'ongoing',presentation:'default',links:navigationModules(user.modules||[]).slice(0,4).map(m=>m.id),x:0,y:0,w:6,h:5,...overrides};
}
export function mandatoryCheck(user:Dict){return user.role!=='guest'&&['operator','shift_leader','duty'].includes(user.category);}
function withChecks(widgets:Widget[],user:Dict,initial=false){
  if(allowedKind('pre_shift',user)&&(mandatoryCheck(user)||initial)&&!widgets.some(item=>item.kind==='pre_shift')){
    widgets=widgets.map(item=>({...item,y:item.y+5}));
    widgets.unshift(widget('pre_shift',user,{id:'pre_shift_required',x:0,y:0,w:12,h:5}));
  }
  return {version:1 as const,widgets};
}
export function defaults(user:Dict):Layout{
  const role=personaFor(user),granted:string[]=user.modules||[];
  const primary=role==='personal'?'learning':role==='engineer'||role==='supervisor'?'event':NOTICE_IDS.find(id=>granted.includes(id))||taskOptions(user)[0]?.value;
  const quick={global:['notices','event','repair_management','daily'],duty:['notices','water','drills','critical_guard'],engineer:['event','repair_management','plan_convergence'],supervisor:['event','notices','cabinet','plan_convergence'],personal:['learning','signatures','knowledge']}[role];
  const available=navigationModules(granted).map(m=>m.id);
  const links=quick.filter(id=>available.includes(id));
  const widgets:Widget[]=[];
  if(allowedKind('shortcuts',user))widgets.push(widget('shortcuts',user,{x:8,w:4,h:3,title:'常用办理',links:links.length?links:available.slice(0,4)}));
  if(allowedKind('tasks',user))widgets.push(widget('tasks',user,{x:0,w:8,h:6,module:granted.includes(primary||'')?primary!:taskOptions(user)[0]!.value,title:role==='personal'&&granted.includes('learning')?'我的待办':'待处理事项'}));
  const secondary=(role==='personal'?[]:role==='duty'?['critical_guard','drills']:['plan_convergence','repair_management','daily']).find(id=>granted.includes(id)&&id!==primary);
  if(secondary)widgets.push(widget('tasks',user,{x:8,y:3,w:4,h:3,module:secondary,title:secondary==='plan_convergence'?'收敛核对 · 需关注':modules.find(m=>m.id===secondary)!.label}));
  return withChecks(widgets,user,true);
}
const integer=(v:unknown,fallback:number,min:number,max:number)=>Number.isInteger(v)?Math.min(max,Math.max(min,Number(v))):fallback;
export function sanitize(value:unknown,user:Dict,scopes:string[]):Layout{
  if(!value||typeof value!=='object'||(value as Dict).version!==1||!Array.isArray((value as Dict).widgets))return defaults(user);
  const seen=new Set<string>(),granted=taskOptions(user).map(item=>item.value),links=navigationModules(user.modules||[]).map(m=>m.id);
  const widgets:Widget[]=[];
  for(const item of (value as Layout).widgets){
    if(!item||!catalog.some(c=>c.kind===item.kind)||!allowedKind(item.kind,user)||typeof item.id!=='string'||!/^[a-zA-Z0-9_-]{1,80}$/.test(item.id)||seen.has(item.id))continue;
    if(item.kind==='tasks'&&!granted.includes(item.module))continue;
    if(item.kind==='pre_shift'&&widgets.some(w=>w.kind==='pre_shift'))continue;
    seen.add(item.id);const w=integer(item.w,6,3,12);
    const presentation=presentationOptions(item.kind).some(p=>p.value===item.presentation)?item.presentation:'default';
    widgets.push({id:item.id,kind:item.kind,title:typeof item.title==='string'?item.title.trim().slice(0,40):catalog.find(c=>c.kind===item.kind)!.title,module:granted.includes(item.module)?item.module:'',scope:scopes.includes(item.scope)?item.scope:'current',display:displayOptions(item.kind).some(option=>option.value===item.display)?item.display:'bar',period:item.period==='month'?'month':'week',metric:item.metric==='pending'?'pending':'ongoing',presentation,links:Array.isArray(item.links)?[...new Set(item.links.filter(id=>links.includes(id)))]:[],x:integer(item.x,0,0,12-w),y:integer(item.y,0,0,1000),w,h:integer(item.h,5,3,12)});
  }
  return withChecks(widgets,user);
}
export function layoutKey(user:Dict){return 'clipflow:workspace-layout:v1:'+String(user.union_id||user.open_id||'');}
export function readLayout(user:Dict,scopes:string[]):{layout:Layout;raw:string|null;error:string}{
  let raw:string|null=null;
  try{raw=localStorage.getItem(layoutKey(user));return {layout:raw?sanitize(JSON.parse(raw),user,scopes):defaults(user),raw,error:''};}
  catch{return {layout:defaults(user),raw,error:'未能读取已保存布局，当前使用角色默认布局。'};}
}
export function saveLayout(user:Dict,scopes:string[],value:Layout,expected:string|null){
  if(localStorage.getItem(layoutKey(user))!==expected)throw new Error('另一个页面已修改布局。请先重新读取，避免覆盖那边的修改。');
  const raw=JSON.stringify(sanitize(value,user,scopes));
  localStorage.setItem(layoutKey(user),raw);return raw;
}
