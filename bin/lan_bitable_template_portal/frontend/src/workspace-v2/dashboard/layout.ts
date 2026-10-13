import type { Dict } from '../client';
import { modules, navigationModules, NOTICE_IDS } from '../modules';
import { personaFor } from '../persona';
import { randomHexId } from '../../browserStorage';

export type WidgetKind = 'tasks' | 'shortcuts' | 'notices' | 'events' | 'learning' | 'water';
export type Widget = {
  id:string; kind:WidgetKind; title:string; module:string; scope:string;
  display:'bar'|'line'|'donut'|'number'; period:'week'|'month'; metric:'ongoing'|'pending';
  links:string[]; x:number; y:number; w:number; h:number;
};
export type Layout = {version:1; widgets:Widget[]};
export const catalog = [
  {kind:'tasks',title:'业务待办',description:'未开始、进行中与需关注事项',icon:'ClipboardList'},
  {kind:'shortcuts',title:'常用操作',description:'直接进入常用业务办理',icon:'LayoutGrid'},
  {kind:'notices',title:'通告分布',description:'按类型查看当前进行中或待发计划',icon:'ChartNoAxesCombined'},
  {kind:'events',title:'事件趋势',description:'本周或本月的每日事件与明细',icon:'ChartColumn'},
  {kind:'learning',title:'我的学练',description:'本人今日进度及近期答题情况',icon:'BookOpen'},
  {kind:'water',title:'水耗概览',description:'授权楼栋本月水耗及记录',icon:'Droplets'},
] as const;
const taskIds=[...NOTICE_IDS,'event','repair_management','daily','plan_convergence','cabinet','engineer_mop','drills','critical_guard','learning'];
export function taskOptions(user:Dict){return modules.filter(m=>taskIds.includes(m.id)&&(user.modules||[]).includes(m.id)).map(m=>({value:m.id,label:m.label}));}
export function allowedKind(kind:WidgetKind,user:Dict){
  const granted:string[]=user.modules||[];
  return kind==='shortcuts'?navigationModules(granted).length>0:kind==='tasks'?taskOptions(user).length>0:kind==='notices'?NOTICE_IDS.some(id=>granted.includes(id)):granted.includes(kind==='events'?'event':kind);
}
export function widget(kind:WidgetKind,user:Dict,overrides:Partial<Widget>={}):Widget{
  return {id:randomHexId(),kind,title:catalog.find(c=>c.kind===kind)!.title,module:taskOptions(user)[0]?.value||'',scope:'current',display:'bar',period:'week',metric:'ongoing',links:navigationModules(user.modules||[]).slice(0,4).map(m=>m.id),x:0,y:0,w:6,h:5,...overrides};
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
  return {version:1,widgets};
}
const integer=(v:unknown,fallback:number,min:number,max:number)=>Number.isInteger(v)?Math.min(max,Math.max(min,Number(v))):fallback;
export function sanitize(value:unknown,user:Dict,scopes:string[]):Layout{
  if(!value||typeof value!=='object'||(value as Dict).version!==1||!Array.isArray((value as Dict).widgets))return defaults(user);
  const seen=new Set<string>(),granted=taskOptions(user).map(item=>item.value),links=navigationModules(user.modules||[]).map(m=>m.id);
  const widgets:Widget[]=[];
  for(const item of (value as Layout).widgets){
    if(!item||!catalog.some(c=>c.kind===item.kind)||!allowedKind(item.kind,user)||typeof item.id!=='string'||!/^[a-zA-Z0-9_-]{1,80}$/.test(item.id)||seen.has(item.id))continue;
    if(item.kind==='tasks'&&!granted.includes(item.module))continue;
    seen.add(item.id);const w=integer(item.w,6,3,12);
    widgets.push({id:item.id,kind:item.kind,title:typeof item.title==='string'?item.title.trim().slice(0,40):catalog.find(c=>c.kind===item.kind)!.title,module:granted.includes(item.module)?item.module:'',scope:scopes.includes(item.scope)?item.scope:'current',display:['bar','line','donut','number'].includes(item.display)?item.display:'bar',period:item.period==='month'?'month':'week',metric:item.metric==='pending'?'pending':'ongoing',links:Array.isArray(item.links)?[...new Set(item.links.filter(id=>links.includes(id)))]:[],x:integer(item.x,0,0,12-w),y:integer(item.y,0,0,1000),w,h:integer(item.h,5,3,12)});
  }
  return {version:1,widgets};
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
