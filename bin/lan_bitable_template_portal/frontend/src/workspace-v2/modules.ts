import { Activity, BookOpen, Bot, CircleAlert, ClipboardList, Droplets, FolderOpen, Library, Link, ScanLine, Server, Settings, ShieldCheck, Signature, Wrench } from 'lucide-vue-next';

export type ModuleEntry = { id: string; label: string; group: string; path: string; icon: string; permission?: string };
export const moduleIcons = { event: CircleAlert, notice: ClipboardList, wrench: Wrench, shield: ShieldCheck, task: ClipboardList, server: Server, water: Droplets, scan: ScanLine, activity: Activity, book: BookOpen, library: Library, documents: FolderOpen, link: Link, signature: Signature, assistant: Bot, settings: Settings };
export const NOTICE_IDS = ['maintenance', 'change', 'repair', 'polling', 'adjust', 'power'];
export const SOP_IDS = ['maintenance', 'polling', 'adjust'];
export const zhihangDataModule: ModuleEntry = { id: 'zhihang_data', label: '智航数据拉取', group: '系统管理', path: '/zhihang-data', icon: 'server', permission: 'accounts' };
export function canManageZhihangData(user: {role?: string; modules?: string[]; impersonation?: unknown}): boolean {
  return user.role === 'admin' && Array.isArray(user.modules) && user.modules.includes('accounts') && !user.impersonation;
}
export const modules: ModuleEntry[] = [
  { id: 'event', label: '事件管理', group: '运行管理', path: '/?mode=events&detail=1', icon: 'event' },
  { id: 'cold_source', label: '冷源运行状态', group: '运行管理', path: '/cold-source-monitor', icon: 'activity', permission: 'daily' },
  ...['维保通告', '变更通告', '检修通告', '设备轮巡', '设备调整', '上下电通告'].map((label, i) => ({ id: NOTICE_IDS[i], label, group: '通告办理', path: '/workbench-lite?work_type=' + NOTICE_IDS[i], icon: 'notice' })),
  { id: 'engineer_mop', label: '维护单管理', group: '运行管理', path: '/engineer/mop', icon: 'wrench' },
  { id: 'repair_management', label: '维修项目', group: '运行管理', path: '/repair-management', icon: 'wrench' },
  { id: 'critical_guard', label: '重保管理', group: '运行管理', path: '/critical-guard', icon: 'shield' },
  { id: 'risk_inspection', label: '专项排查', group: '专项功能', path: '/risk-inspections', icon: 'shield' },
  { id: 'tool_management', label: '工具管理', group: '专项功能', path: '/risk-inspections?view=tool-self-check', icon: 'wrench', permission: 'risk_inspection' },
  { id: 'eop_scenarios', label: 'EOP 场景维护', group: '专项功能', path: '/eop-scenarios', icon: 'book', permission: 'drills' },
  { id: 'daily', label: '每日任务', group: '运行管理', path: '/daily-tasks', icon: 'task' },
  { id: 'daily_inspection', label: '日常巡检', group: '运行管理', path: '/daily-inspection', icon: 'activity', permission: 'daily' },
  { id: 'cabinet', label: '机柜管理', group: '设备与资源', path: '/cabinet-power', icon: 'server' },
  { id: 'water', label: '水耗管理', group: '设备与资源', path: '/water-management', icon: 'water' },
  { id: 'plan_convergence', label: '计划收敛审查', group: '专项功能', path: '/plan-convergence', icon: 'scan' },
  { id: 'drills', label: '演练管理', group: '学习与资料', path: '/drill-management', icon: 'activity' },
  { id: 'learning', label: '画像学练', group: '学习与资料', path: '/learning', icon: 'book' },
  { id: 'knowledge', label: '共享知识库', group: '学习与资料', path: '/knowledge-base', icon: 'library' },
  { id: 'documents', label: '南通文件', group: '学习与资料', path: '/document-library', icon: 'documents', permission: 'knowledge' },
  { id: 'links', label: '多维表导航', group: '学习与资料', path: '/link-directory', icon: 'link' },
  { id: 'life_guide', label: '参考人生指南', group: '学习与资料', path: '/life-guide', icon: 'book' },
  { id: 'signatures', label: '签名管理', group: '账号与协作', path: '/signature-management', icon: 'signature' },
  { id: 'handover', label: '交接班审核', group: '账号与协作', path: '/?view=handover', icon: 'task' },
  { id: 'work_handover', label: '人员工作交接', group: '专项功能', path: '/work-handover', icon: 'task' },
  { id: 'assistant', label: '灯塔助手', group: '账号与协作', path: '/?view=assistant', icon: 'assistant' },
  { id: 'accounts', label: '账号与权限', group: '账号与协作', path: '/admin/settings', icon: 'settings' },
];
export function modulePermission(id: string): string { return modules.find(item => item.id === id)?.permission || id; }
export function navigationModules(permitted: string[], toolManagement = false, inspectionHistory = false): ModuleEntry[] {
  const notices=NOTICE_IDS.filter(id=>permitted.includes(id));
  return modules.flatMap(item=>{
    if(item.id==='tool_management'&&!toolManagement)return [];
    if(item.id==='tool_management'&&toolManagement)return [item];
    if(item.id!=='signatures'&&!permitted.includes(item.permission||item.id)
        && !(item.id==='daily_inspection' && inspectionHistory) || item.id==='assistant')return [];
    if(!NOTICE_IDS.includes(item.id))return [item];
    if(item.id!==notices[0])return [];
    return [{id:'notices',label:'通告办理',group:'运行管理',path:'/workbench-lite?work_type='+(notices.length===1?notices[0]:'all'),icon:'notice'},
      ...(SOP_IDS.some(id=>permitted.includes(id))?[{id:'sops',label:'SOP 步骤编辑',group:'运行管理',path:'/?view=sops',icon:'task'}]:[])];
  });
}
export function moduleForRoute(url: URL): string {
  if (url.pathname.replace(/\/$/, '') === zhihangDataModule.path) return zhihangDataModule.id;
  if (url.pathname === '/risk-inspections' && url.searchParams.get('view') === 'tool-self-check') return 'tool_management';
  if (url.pathname === '/workbench-lite') return url.searchParams.get('work_type') || 'maintenance';
  if (url.pathname === '/') {
    if (url.searchParams.get('mode') === 'events') return 'event';
    if (['handover', 'assistant', 'sops'].includes(url.searchParams.get('view') || '')) return url.searchParams.get('view')!;
    return '';
  }
  if (url.pathname === '/repair-status') return 'repair_management';
  if (url.pathname === '/signature') return 'signatures';
  if (url.pathname.startsWith('/polling-work-order')) return '';
  if (url.pathname === '/admin/history-memory') return 'accounts';
  return modules.find(item => new URL(item.path, url.origin).pathname === url.pathname)?.id
    || modules.find(item => new URL(item.path, url.origin).pathname !== '/' && url.pathname.startsWith(new URL(item.path, url.origin).pathname + '/'))?.id || '';
}
export function moduleUrl(module: ModuleEntry, scope: string): string {
  const url = new URL(module.path, window.location.origin);
  if (!['accounts', 'assistant', 'knowledge', 'documents', 'links', 'life_guide', 'signatures', 'handover', 'work_handover', 'learning', 'eop_scenarios', zhihangDataModule.id].includes(module.id) && scope) url.searchParams.set('scope', scope);
  return url.pathname + url.search;
}
export function beijingDate(): string {
  return new Intl.DateTimeFormat('sv-SE', { timeZone: 'Asia/Shanghai', year: 'numeric', month: '2-digit', day: '2-digit' }).format(new Date());
}
export function count(value: unknown): number | null {
  if (value === null || value === undefined || value === '' || !Number.isFinite(Number(value))) return null;
  return Math.max(0, Number(value));
}
