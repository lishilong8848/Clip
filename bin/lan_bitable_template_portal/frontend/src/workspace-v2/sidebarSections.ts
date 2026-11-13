import { Activity, BookOpen, ClipboardList, House, ListChecks, Radio, Server, Settings, ShieldAlert, ShieldCheck, Wrench } from 'lucide-vue-next';
import type { Component } from 'vue';
import type { Persona } from './persona';

type Branch = { key: string; label: string; entries: string[] };
type Section = { icon: Component; label: string; branches: Branch[] };
type Entry = { key: string; label: string; icon?: Component };
export type SidebarGroup = Entry & { branches?: Array<{ key: string; label: string; entries: Entry[] }> };

const sections = {
  operations: { label: '值班与交接', icon: Radio, branches: [
    { key: 'shift', label: '', entries: ['daily'] },
    { key: 'handover', label: '交接事务', entries: ['handover', 'work_handover'] },
  ] },
  inspection: { label: '巡检与监测', icon: Activity, branches: [
    { key: 'inspection', label: '', entries: ['daily_inspection', 'cold_source'] },
  ] },
  notices: { label: '通告与计划', icon: ClipboardList, branches: [
    { key: 'notice', label: '', entries: ['notices', 'plan_convergence'] },
  ] },
  repairs: { label: '维修管理', icon: Wrench, branches: [
    { key: 'repairs', label: '', entries: ['repair_management'] },
  ] },
  maintenance: { label: '维护与工单', icon: ListChecks, branches: [
    { key: 'orders', label: '', entries: ['engineer_mop', 'sops', 'signatures'] },
  ] },
  safety: { label: '风险管控', icon: ShieldCheck, branches: [
    { key: 'safety', label: '', entries: ['risk_inspection', 'critical_guard'] },
  ] },
  emergency: { label: '应急与演练', icon: ShieldAlert, branches: [
    { key: 'event', label: '', entries: ['event'] },
    { key: 'preparedness', label: '演练与预案', entries: ['drills', 'eop_scenarios'] },
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
    { key: 'system', label: '系统运维', entries: ['settings:status', 'zhihang_data', 'settings:development', 'settings:pressure'] },
  ] },
} satisfies Record<string, Section>;

type SectionKey = keyof typeof sections;
const orders: Record<Persona, SectionKey[]> = {
  duty: ['operations', 'inspection', 'notices', 'emergency', 'safety', 'repairs', 'maintenance', 'resources', 'learning', 'administration'],
  engineer: ['notices', 'repairs', 'maintenance', 'resources', 'inspection', 'operations', 'safety', 'emergency', 'learning', 'administration'],
  supervisor: ['safety', 'operations', 'inspection', 'resources', 'repairs', 'notices', 'emergency', 'maintenance', 'learning', 'administration'],
  global: ['operations', 'inspection', 'notices', 'repairs', 'maintenance', 'safety', 'emergency', 'resources', 'learning', 'administration'],
  personal: ['operations', 'learning', 'inspection', 'notices', 'repairs', 'maintenance', 'safety', 'emergency', 'resources', 'administration'],
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
      if (persona === 'supervisor' && key === 'resources') items.sort((a, b) => Number(b.key === 'tool_management') - Number(a.key === 'tool_management'));
      return items.length ? [{ key: branch.key, label: branch.label, entries: items }] : [];
    });
    return visible.length ? [{ key, label, icon, branches: visible }] : [];
  })];
}
