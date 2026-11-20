import { BookOpen, ClipboardList, House, ListChecks, Radio, Server, Settings, ShieldCheck, Wrench } from 'lucide-vue-next';
import type { Component } from 'vue';
import { personaFor, type Persona } from './persona';
import { workbenchRole, type ScopeOption } from './workbench/recipes';
import type { Dict } from './client';

type Entry = { key: string; label: string; icon?: Component };
type Branch = { key: string; label: string; entries: string[] };
type Section = { label: string; icon: Component; branches: Branch[] };
type SidebarRole = Persona | 'manager' | 'administrator';
export type SidebarGroup = Entry & {
  direct?: Entry;
  defaultBranch?: string;
  branches?: { key: string; label: string; entries: Entry[] }[];
};

const sections = {
  operations: { label: '运行值守', icon: Radio, branches: [
    { key: 'shift', label: '当班任务', entries: ['daily', 'daily_inspection'] },
    { key: 'readings', label: '运行记录', entries: ['cold_source', 'water'] },
    { key: 'handover', label: '交接办理', entries: ['work_handover'] },
  ] },
  notices: { label: '通告中心', icon: ClipboardList, branches: [
    { key: 'notice', label: '', entries: ['plan_convergence', 'notices'] },
  ] },
  repairs: { label: '维修跟进', icon: Wrench, branches: [
    { key: 'repairs', label: '', entries: ['repair_management'] },
  ] },
  maintenance: { label: '作业规程', icon: ListChecks, branches: [
    { key: 'orders', label: '', entries: ['engineer_mop', 'sops', 'eop_scenarios'] },
  ] },
  safety: { label: '应急保障', icon: ShieldCheck, branches: [
    { key: 'events', label: '事件与演练', entries: ['event', 'drills'] },
    { key: 'risk', label: '风险与保障', entries: ['risk_inspection', 'critical_guard'] },
  ] },
  resources: { label: '资产管理', icon: Server, branches: [
    { key: 'resources', label: '', entries: ['cabinet', 'tool_management'] },
  ] },
  learning: { label: '知识中心', icon: BookOpen, branches: [
    { key: 'study', label: '学习成长', entries: ['learning', 'life_guide'] },
    { key: 'library', label: '资料查询', entries: ['knowledge', 'documents', 'links'] },
  ] },
  administration: { label: '平台运维', icon: Settings, branches: [
    { key: 'permissions', label: '人员与权限', entries: ['settings:permissions', 'signatures'] },
    { key: 'configuration', label: '业务配置', entries: ['settings:handover', 'settings:mop'] },
    { key: 'channels', label: '消息渠道', entries: ['settings:feishu', 'settings:dingtalk'] },
    { key: 'system', label: '系统运维', entries: ['settings:status', 'zhihang_data', 'settings:development', 'settings:pressure'] },
  ] },
} satisfies Record<string, Section>;

type SectionKey = keyof typeof sections;
// Append the remaining sections in their standard order after the role's priorities.
const priorities: Record<SidebarRole, SectionKey[]> = {
  global: [],
  duty: ['operations', 'notices', 'safety'],
  engineer: ['repairs', 'maintenance', 'safety', 'notices'],
  supervisor: ['safety', 'operations', 'repairs', 'resources'],
  manager: ['safety', 'repairs', 'notices'],
  administrator: ['resources', 'administration'],
  personal: ['operations', 'learning'],
};

export function sidebarRole(user: Dict, scopes: ScopeOption[]): SidebarRole {
  const persona = personaFor(user);
  if (persona === 'global' && !['manager', 'administrator'].includes(user.category)) return 'global';
  const role = workbenchRole(user, scopes, true);
  if (role === 'specialist') return 'engineer';
  return role === 'personal' ? persona : role;
}

export function sidebarGroups(entries: Entry[], role: SidebarRole): SidebarGroup[] {
  const available = new Map(entries.map(entry => [entry.key, entry]));
  const keys = new Set([...priorities[role], ...(Object.keys(sections) as SectionKey[])]);
  const groups: SidebarGroup[] = [{ key: 'home', label: '个人工作台', icon: House }];
  for (const key of keys) {
    const { label, icon, branches } = sections[key];
    const visible = branches.map(branch => ({
      ...branch,
      entries: branch.entries.flatMap(id => available.has(id) ? [available.get(id)!] : []),
    })).filter(branch => branch.entries.length);
    if (!visible.length) continue;
    if (visible.length === 1 && visible[0].entries.length === 1) {
      const entry = visible[0].entries[0];
      groups.push({ key, label, icon, direct: key === 'repairs' ? { ...entry, label } : entry });
    } else {
      const defaultBranch = role === 'duty' && key === 'operations' ? 'shift'
        : role === 'administrator' && key === 'administration' ? 'permissions' : undefined;
      groups.push({ key, label, icon, branches: visible,
        defaultBranch: visible.some(branch => branch.key === defaultBranch) ? defaultBranch : undefined });
    }
  }
  return groups;
}
