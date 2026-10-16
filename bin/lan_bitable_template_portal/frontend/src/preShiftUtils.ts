import type { Dict } from './api/client';

const scopes = ['110', 'A', 'B', 'C', 'D', 'E', 'H'];

export function preShiftScopes(user: Dict): string[] {
  if (user.role === 'guest' || !Array.isArray(user.modules) || !user.modules.length) return [];
  if (['L0', 'L1'].includes(user.level) || ['supervisor', 'manager'].includes(user.category)) {
    const allowed = Array.isArray(user.scope_rules) ? user.scope_rules : [];
    return scopes.filter(scope => allowed.includes('ALL') || allowed.includes(scope));
  }
  return ['operator', 'shift_leader', 'duty'].includes(user.category) && scopes.includes(user.home_scope)
    ? [user.home_scope] : [];
}

export function preShiftSummary(state: Dict): { pending: number; completed: number } {
  const now = Date.parse(state.server_time) / 1000;
  if (!Number.isFinite(now) || !Array.isArray(state.tasks) || !Array.isArray(state.definition?.items)) {
    throw new Error('检查状态未完整返回');
  }
  let pending = 0, completed = 0;
  for (const task of state.tasks) {
    if (!Array.isArray(task.items) || !Number.isFinite(task.due) || !Number.isFinite(task.deadline)
        || ![task.scope, task.day, task.shift].every(value => typeof value === 'string' && value)) {
      throw new Error('检查状态未完整返回');
    }
    if (!task.items.length) continue;
    if (task.submitted) { if (task.deadline > now) completed++; }
    else if (task.due <= now && (state.definition.items.length || task.deadline <= now)) pending++;
  }
  return { pending, completed };
}
