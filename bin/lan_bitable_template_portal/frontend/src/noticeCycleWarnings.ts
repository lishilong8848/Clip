export type MaintenanceCycleRule = { value: string; aliases: string[] };
export type NoticeCycleWarnings = Partial<Record<'content' | 'reason', string>>;

export function maintenanceCycleRules(raw: unknown): MaintenanceCycleRule[] {
  if (typeof raw === 'string') {
    try { raw = JSON.parse(raw); } catch { return []; }
  }
  if (!Array.isArray(raw)) return [];
  return raw.flatMap(rule => {
    if (!rule || typeof rule.value !== 'string' || !rule.value.trim() || !Array.isArray(rule.aliases)) return [];
    const value = rule.value.trim();
    return [{ value, aliases: [...new Set([...rule.aliases.filter((alias: unknown): alias is string => typeof alias === 'string' && !!alias.trim()).map((alias: string) => alias.trim()), value])] }];
  });
}

export function isMaintenanceNotice(values: Record<string, unknown>): boolean {
  return values.work_type === 'maintenance' && (values.notice_type == null || values.notice_type === '' || values.notice_type === '维保通告');
}

// Presentation only: never add these warnings to validation or submission data.
export function noticeCycleWarnings(values: Record<string, unknown>, rawRules: unknown): NoticeCycleWarnings {
  if (!isMaintenanceNotice(values) || typeof values.maintenance_cycle !== 'string') return {};
  const rules = maintenanceCycleRules(rawRules), owners = new Map<string, Set<string>>();
  for (const rule of rules) for (const alias of rule.aliases) {
    if (!owners.has(alias)) owners.set(alias, new Set());
    owners.get(alias)!.add(rule.value);
  }
  const selected = owners.get(values.maintenance_cycle.trim());
  if (selected?.size !== 1) return {};
  const current = [...selected][0];
  // One longest-first scan consumes 半年度/每两年 before their shorter 年度/年 aliases.
  const pattern = [...owners.keys()].sort((a, b) => b.length - a.length).map(alias => alias.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('|');
  if (!pattern) return {};
  const warnings: NoticeCycleWarnings = {};
  for (const [key, label] of [['content', '内容'], ['reason', '原因']] as const) {
    if (typeof values[key] !== 'string') continue;
    const conflicts = new Set<string>();
    for (const match of values[key].matchAll(new RegExp(pattern, 'g'))) {
      const cycles = owners.get(match[0])!;
      if (cycles.size === 1 && !cycles.has(current)) conflicts.add(match[0]);
    }
    if (conflicts.size) warnings[key] = `已选维保周期“${current}”，【${label}】含其它周期字样“${[...conflicts].join('、')}”，请核对（仅提醒，不阻止发送）。`;
  }
  return warnings;
}
