// 独立新版工作台的角色展示规则。实际权限仍由原业务接口执行。
import type { Dict } from '../client';
import type { SourceId, WorkItem } from './sources';

export type DeskRole = 'L0'|'L1'|'L2'|'L3'|'L4'|'duty';
export function deskRole(user:Dict):DeskRole|undefined {
  if (['guest','temporary'].includes(user.role)) return;
  if (user.category==='duty') return user.home_scope==='H'&&(user.role==='admin'||user.is_admin===true)?'L1':'duty';
  if (['L0','L1','L2','L3','L4'].includes(user.level)) return user.level as DeskRole;
}

const common:SourceId[]=['notices','events','engineer_mop','repair_management','critical_guard','risk_inspection','drills','accounts','cabinet','water','tool_management'];
export const deskSources:Record<DeskRole,SourceId[]> = {
  L0:[...common,'ali_confirmation'], L1:[...common,'ali_confirmation'],
  L2:['notices','repair_management','events','engineer_mop'],
  L3:['events','critical_guard','risk_inspection','notices','engineer_mop','repair_management'],
  L4:['notices','repair_management'],
  duty:['ali_confirmation','inspection','notices','event_notices','events','critical_guard','drills','engineer_mop','water'],
};
const owned = (user:Dict,item:WorkItem) =>
  [user.open_id,user.union_id].some(id=>typeof id==='string'&&id&&item.actorIds?.includes(id));
export function inDeskScope(item:WorkItem,scope:string) {
  return (item.referencedScopes?.length?item.referencedScopes:item.scope.split(/[,，、/\s]+/))
    .some(value=>value.replace(/[楼栋站]/g,'')===scope);
}
export function deskTaskVisible(role:DeskRole|undefined,user:Dict,item:WorkItem,ownScope:string) {
  if (role==='L4') return !!ownScope&&inDeskScope(item,ownScope)&&owned(user,item);
  if (role==='L3') return !!ownScope&&inDeskScope(item,ownScope)
    && (['events','critical_guard','risk_inspection'].includes(item.kind)||item.ownerLevel==='L4');
  if (role==='L2'&&!inDeskScope(item,ownScope)) return item.ownerLevel==='L4';
  return true;
}

export function deskActionOrder(role:DeskRole|undefined,item:WorkItem) {
  if(role==='L2') {
    const sequence=['轮巡','维保','变更','检修','维修单','设备调整'];
    const index=sequence.indexOf(item.kind==='repair_management'?'维修单':item.detail||'');
    return index<0?sequence.length:index;
  }
  if(role==='duty') {
    if(item.kind==='ali_confirmation') return 0;
    if(item.kind==='inspection'||item.kind==='notices'&&['上下电','检修','轮巡'].includes(item.detail||'')) return 1;
    if(item.kind==='event_notices'||item.kind==='events') return 2;
    return 3;
  }
  return 0;
}
