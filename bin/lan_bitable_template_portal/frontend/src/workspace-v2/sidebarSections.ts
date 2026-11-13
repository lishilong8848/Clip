import { BookOpen, ClipboardList, House, Radio, Server, Settings, ShieldCheck } from 'lucide-vue-next';
import type { Component } from 'vue';
import type { Persona } from './persona';

type Branch = { key: string; label: string; entries: string[] };
type Section = { icon: Component; label: string; branches: Branch[] };
type Entry = { key: string; label: string; icon?: Component };
export type SidebarGroup = Entry & { branches?: Array<{ key: string; label: string; entries: Entry[] }> };

const sections = {
  operations: { label: '运行值班', icon: Radio, branches: [
    { key: 'shift', label: '当班作业', entries: ['daily', 'daily_inspection', 'cold_source'] },
    { key: 'handover', label: '交接班', entries: ['handover', 'work_handover'] },
  ] },
  notices: { label: '通告与工单', icon: ClipboardList, branches: [
    { key: 'notice', label: '通告', entries: ['notices', 'plan_convergence'] },
    { key: 'orders', label: '工单与签名', entries: ['repair_management', 'engineer_mop', 'signatures'] },
    { key: 'procedures', label: '流程配置', entries: ['sops'] },
  ] },
  safety: { label: '风险与应急', icon: ShieldCheck, branches: [
    { key: 'safety', label: '', entries: ['event', 'risk_inspection', 'critical_guard', 'drills', 'eop_scenarios'] },
  ] },
  resources: { label: '设备与资源', icon: Server, branches: [
    { key: 'resources', label: '', entries: ['cabinet', 'water', 'tool_management'] },
  ] },
  learning: { label: '学习与资料', icon: BookOpen, branches: [
    { key: 'study', label: '学习', entries: ['learning', 'life_guide'] },
    { key: 'library', label: '资料', entries: ['knowledge', 'documents', 'links'] },
  ] },
  administration: { label: '系统管理', icon: Settings, branches: [
    { key: 'permissions', label: '人员权限', entries: ['settings:permissions'] },
    { key: 'configuration', label: '业务配置', entries: ['settings:handover', 'settings:mop', 'settings:feishu', 'settings:dingtalk'] },
    { key: 'system', label: '系统运维', entries: ['settings:status', 'settings:development', 'settings:pressure'] },
  ] },
} satisfies Record<string, Section>;

type SectionKey = keyof typeof sections;
const orders: Record<Persona, SectionKey[]> = {
  duty: ['operations', 'notices', 'safety', 'resources', 'learning', 'administration'],
  engineer: ['notices', 'resources', 'operations', 'safety', 'learning', 'administration'],
  supervisor: ['safety', 'operations', 'resources', 'notices', 'learning', 'administration'],
  global: ['operations', 'notices', 'safety', 'resources', 'learning', 'administration'],
  personal: ['operations', 'learning', 'notices', 'safety', 'resources', 'administration'],
};

export function sidebarGroups(entries: Entry[], persona: Persona): SidebarGroup[] {
  const available = new Map(entries.map(entry => [entry.key, entry]));
  return [{ key: 'home', label: '工作台', icon: House }, ...orders[persona].flatMap(key => {
    const { label, icon, branches } = sections[key];
    const visible = branches.flatMap(branch => {
      const items = branch.entries.flatMap(id => {
        const entry = available.get(id);
        return entry ? [entry] : [];
      });
      if (persona === 'supervisor' && key === 'safety') items.sort((a, b) => Number(b.key === 'risk_inspection') - Number(a.key === 'risk_inspection'));
      if (persona === 'supervisor' && key === 'resources') items.sort((a, b) => Number(b.key === 'tool_management') - Number(a.key === 'tool_management'));
      return items.length ? [{ key: branch.key, label: branch.label, entries: items }] : [];
    });
    return visible.length ? [{ key, label, icon, branches: visible }] : [];
  })];
}
