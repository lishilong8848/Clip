import { knownPage } from './usageEvents';

export function noticePage(kind: string | null, action?: string | null) {
  if (action && ['start','update','end','verify'].includes(action)) return 'notices/' + action;
  if (!kind || kind === 'all') return 'notices/main';
  return ['maintenance','change','repair','polling','adjust','power','event'].includes(kind) ? 'notices/' + kind : '';
}

// Only constant selectors and existence flags affect the result. Never return path or query text.
export function usageRoute(path: string, p: URLSearchParams, legacy = false): string {
  switch (path) {
    case '/':
      if (p.get('mode') === 'events') return 'event';
      if (p.has('mode')) return '';
      if (legacy) {
        if (p.get('entry') === 'pre_shift') return 'pre_shift/main';
        if (p.get('view') === 'notice_history') return 'notices/history';
        return !p.has('view') ? 'scope_home' : '';
      }
      switch (p.get('view') || 'home') {
        case 'home': return 'portal/home';
        case 'tasks': return 'portal/tasks';
        case 'functions': return 'portal/functions';
        case 'my': return 'portal/my';
        case 'permission': return 'auth/permission';
        case 'assistant': return 'assistant';
        case 'sops': return 'sops/main';
        case 'handover': return 'handover/main';
        default: return '';
      }
    case '/workbench-lite': return legacy ? 'lite_frame' : 'notices';
    case '/polling-work-order': case '/polling-work-order/steps': return 'work_order';
    case '/admin/settings': {
      const tab = p.get('tab') || 'permissions';
      return knownPage('accounts',tab) ? 'accounts/'+tab : '';
    }
    case '/admin/history-memory': return 'notice_history/main';
    case '/signature': return 'signature_link';
    case '/signature-management': return 'signatures/main';
    case '/engineer/mop': return 'engineer_mop';
    case '/repair-management': return 'repair_management';
    case '/repair-status': return 'repair_management/status';
    case '/critical-guard': return 'critical_guard';
    case '/risk-inspections': return p.get('view') === 'tool-self-check' ? 'tool_management' : 'risk_inspection';
    case '/cold-source-monitor': return legacy ? '' : 'cold_source/main';
    case '/zhihang-data': return legacy ? '' : 'zhihang_data/main';
    case '/eop-scenarios': return 'eop_scenarios/main';
    case '/daily-tasks': case '/daily-tasks/morning-meeting/print': return 'daily';
    case '/daily-inspection': return 'daily_inspection/main';
    case '/cabinet-power': return 'cabinet';
    case '/cabinet-power/batches': return p.has('batch_id') ? 'cabinet/batch_detail' : p.get('mode') === 'new' ? 'cabinet/batch_new' : 'cabinet/batches';
    case '/water-management': return 'water';
    case '/plan-convergence': return 'plan_convergence';
    case '/drill-management': case '/drill-management/print': return 'drills';
    case '/learning': return 'learning';
    case '/knowledge-base': return 'knowledge';
    case '/document-library': return 'documents';
    case '/link-directory': return 'links/main';
    case '/life-guide': return 'life_guide/main';
    case '/work-handover': return 'work_handover';
    default: return '';
  }
}

export function legacyHome(mode: string) {
  if (!mode) return 'portal/home';
  if (['tools','special','capacity','risk'].includes(mode)) return 'portal/' + mode;
  if (['maintenance','change','repair','polling','adjust','power'].includes(mode)) return 'notices/' + mode + '_scope';
  const module = ({maintenance_mop:'engineer_mop',cabinet_power:'cabinet',drill:'drills'} as Record<string,string>)[mode] || mode;
  return knownPage(module,'scope') ? module + '/scope' : '';
}
