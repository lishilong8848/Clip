import type { Dict } from '../client';
import { navigationModules, NOTICE_IDS, type ModuleEntry } from '../modules';
import { canViewInspectionHistory } from '../../inspectionAccess';
import type { SourceId } from './sources';

export type ScopeOption = { value: string; label: string };
export type Section = 'urgent' | 'operations' | 'business' | 'people';
export type Role = 'manager' | 'supervisor' | 'engineer' | 'duty' | 'specialist' | 'administrator' | 'personal';
type Recipe = {
  label: string;
  focusTitle: string;
  sections: Section[];
  actions: string[];
  pending?: SourceId[];
  rail?: boolean;
};

export const recipes: Record<Role, Recipe> = {
  manager: {label:'园区经理',focusTitle:'园区关注',sections:['urgent','business','operations','people'],
    actions:['event','repair_management','learning','critical_guard','risk_inspection'],pending:['events','repair_management','critical_guard','notices','risk_inspection']},
  supervisor: {label:'楼栋主管',focusTitle:'本楼风险',sections:['urgent','people','business','operations'],
    actions:['event','repair_management','cabinet','learning','critical_guard','risk_inspection'],pending:['events','repair_management','cabinet','critical_guard','risk_inspection'],rail:true},
  engineer: {label:'楼栋工程师',focusTitle:'维修与闭环',sections:['urgent','operations','business','people'],
    actions:['repair_management','risk_inspection'],pending:['repair_management','risk_inspection'],rail:true},
  duty: {label:'值班人员',focusTitle:'本班关注',sections:['operations','urgent','business','people'],
    actions:['notices','drills','engineer_mop','water','learning','pre_shift'],pending:['notices','event_notices','drills','engineer_mop','water','learning'],rail:true},
  specialist: {label:'园区主管工程师',focusTitle:'专业关注',sections:['operations','urgent','people','business'],
    actions:['station','repair_management','plan_convergence','risk_inspection','event'],pending:['repair_management','risk_inspection','events'],rail:true},
  administrator: {label:'综合管理员',focusTitle:'业务关注',sections:['business','urgent','people','operations'],
    actions:['tool_management','accounts','signatures'],pending:['tool_management','accounts','signatures']},
  personal: {label:'个人工作台',focusTitle:'需要关注',sections:['urgent','operations','business','people'],
    actions:['learning','daily','work_handover']},
};

export function workbenchRole(user: Dict, scopes: ScopeOption[], onDuty: boolean): Role {
  if (['guest','temporary'].includes(user.role)) return 'personal';
  if (user.category==='duty'||onDuty&&['operator','shift_leader'].includes(user.category)) return 'duty';
  const campus = scopes.some(s => s.value === 'ALL' || s.value === 'CAMPUS')
    || scopes.filter(s => /^[A-EH]$/.test(s.value)).length > 1;
  switch (user.category) {
    case 'operator':
    case 'shift_leader': return 'personal';
    case 'manager': return 'manager';
    case 'supervisor': return 'supervisor';
    case 'engineer': return campus ? 'specialist' : 'engineer';
    case 'administrator': return 'administrator';
    default: return campus && user.role === 'admin' && ['L0', 'L1'].includes(user.level) ? 'administrator' : 'personal';
  }
}

export function workbenchScopes(options: ScopeOption[]) {
  const scopes = options.filter(s => /^(ALL|CAMPUS|[A-EH]|110)$/.test(s.value));
  const buildings = scopes.filter(s => !['ALL','CAMPUS'].includes(s.value));
  return buildings.length === 1 ? buildings : scopes;
}

export function workbenchModules(user: Dict, scopes: ScopeOption[]): ModuleEntry[] {
  const grants = Array.isArray(user.modules) ? user.modules : [];
  const entries = navigationModules(grants, user.tool_management_enabled === true, canViewInspectionHistory(user))
    .filter(entry => (entry.id !== 'signatures' || user.role !== 'guest' && !user.impersonation)
      && (entry.id !== 'daily_inspection' || scopes.some(s => ['ALL','D','E'].includes(s.value))));
  if (grants.includes('event') && !grants.some((id: string) => NOTICE_IDS.includes(id))) {
    entries.push({ id:'notices', label:'通告办理', group:'通告办理', path:'/workbench-lite?work_type=event', icon:'notice' });
  }
  return entries;
}
