export function canViewInspectionHistory(user: Record<string, any> | null | undefined): boolean {
  if (!user?.modules?.includes('daily_inspection') || !user.enabled || user.role === 'guest' || !['D','E'].includes(user.home_scope)) return false;
  const scopes = Array.isArray(user.scope_rules) ? user.scope_rules : [];
  return scopes.includes('ALL') || scopes.includes(user.home_scope);
}
