import { computed, inject, type ComputedRef, type InjectionKey } from 'vue';
import type { Dict } from './client';
import { allowedKind, sanitize, widget, type Widget, type WidgetKind } from './dashboard/layout';

export type PageBlock = {id:string;title:string;w?:number;h?:number;minW?:number;minH?:number;when?:boolean;required?:boolean};
export type PageWidget = Widget & {builtin?:boolean;minW?:number;minH?:number};
export type PageLayout = {version:1;items:PageWidget[]};
export const PAGE_WIDGET_LIMIT=16;
export const pageContext:InjectionKey<{user:ComputedRef<Dict>;scopes:ComputedRef<string[]>;scope:ComputedRef<string>;module:ComputedRef<string>;changeScope?:(scope:string)=>void}> = Symbol('page-layout');
export type Capacity={width:number;height:number;rows:number;columns:number;compact:boolean};
export const capacityContext:InjectionKey<ComputedRef<Capacity>>=Symbol('widget-capacity');
export function useCapacity(){return inject(capacityContext,computed(()=>capacity(800,480)));}
export function capacity(width:number,height:number):Capacity{
  const w=Math.max(0,width),h=Math.max(0,height);
  return {width:w,height:h,rows:Math.max(3,Math.min(60,Math.floor((h-100)/48))),columns:w<500?4:w<760?6:w<1040?9:20,compact:w<560};
}
export function pageLayoutKey(user:Dict,page:string){return 'clipflow:page-layout:v1:'+String(user.union_id||user.open_id||'')+':'+page;}
export function extraKinds(module:string,user:Dict):WidgetKind[]{
  const kinds:WidgetKind[]=module==='notices'?['notice_actions','notices','tasks']:module==='event'?['events','tasks']:module==='repair_management'?['repair_actions','tasks']:module==='water'?['water']:module==='learning'?['learning']:['cabinet','engineer_mop','drills','critical_guard','daily','plan_convergence'].includes(module)?['tasks']:['shortcuts'];
  return kinds.filter(kind=>allowedKind(kind,user));
}
export function defaultPageLayout(blocks:PageBlock[],user:Dict):PageLayout{
  let x=0,y=0,rowHeight=0;
  const items:PageWidget[]=blocks.map(block=>{
    const w=block.w||12,h=block.h||7;if(x+w>12){x=0;y+=rowHeight;rowHeight=0;}
    const item={...widget('shortcuts',user),id:'block_'+block.id,title:block.title,builtin:true,kind:'shortcuts' as const,w,h,x,y,minW:block.minW||4,minH:block.minH||4};
    x+=w;rowHeight=Math.max(rowHeight,h);return item;
  });
  return {version:1,items};
}
export function sanitizePageLayout(value:unknown,blocks:PageBlock[],user:Dict,scopes:string[],module:string):PageLayout{
  const fallback=defaultPageLayout(blocks,user);
  if(!value||typeof value!=='object'||(value as Dict).version!==1||!Array.isArray((value as Dict).items))return fallback;
  const defaults=new Map(fallback.items.map(item=>[item.id,item])),seen=new Set<string>(),items:PageWidget[]=[];
  for(const raw of (value as PageLayout).items.slice(0,PAGE_WIDGET_LIMIT)){
    if(!raw||typeof raw.id!=='string'||seen.has(raw.id))continue;
    const base=defaults.get(raw.id);
    if(base){
      const integer=(v:unknown,d:number,min:number,max:number)=>Number.isInteger(v)?Math.min(max,Math.max(min,Number(v))):d;
      const w=integer(raw.w,base.w,base.minW!,12);
      items.push({...base,w,h:integer(raw.h,base.h,base.minH!,30),x:integer(raw.x,0,0,12-w),y:integer(raw.y,0,0,1000)});
    }else if(!raw.builtin&&extraKinds(module,user).includes(raw.kind)){
      const clean=sanitize({version:1,widgets:[raw]},user,scopes).widgets.find(item=>item.id===raw.id);
      if(clean)items.push({...clean,minW:4,minH:4});
    }else continue;
    seen.add(raw.id);
  }
  // New business sections and required forms stay available after an upgrade.
  for(const block of blocks){const item=defaults.get('block_'+block.id)!;if(!seen.has(item.id)&&block.required!==false)items.push({...item,y:Math.max(0,...items.map(row=>row.y+row.h))});}
  return {version:1,items:items.slice(0,Math.max(PAGE_WIDGET_LIMIT,blocks.length))};
}
