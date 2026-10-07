import type { LooseDict, ScopeOption } from './types';

const PREFIX = 'clipflow:home-overview:';
const MAX_AGE = 5 * 60 * 1000;
export function overviewKey(user: LooseDict, scopes: ScopeOption[]): string {
  return PREFIX + JSON.stringify([user.open_id || '', user.role || '', scopes.map(item => item.value).sort()]);
}
export function readOverview(key: string): Record<string, LooseDict> | null {
  try {
    const stored = JSON.parse(sessionStorage.getItem(key) || 'null');
    const age = Date.now() - Number(stored?.at);
    if (age < 0 || age > MAX_AGE || !Number.isFinite(age) || !stored?.scopes || typeof stored.scopes !== 'object' || Array.isArray(stored.scopes)) return null;
    return stored.scopes;
  } catch { return null; }
}
export function saveOverview(key: string, scopes: Record<string, LooseDict>): void {
  try { sessionStorage.setItem(key, JSON.stringify({ at: Date.now(), scopes })); } catch { /* Cache is optional. */ }
}
export function clearOverview(): void {
  try {
    for (const key of Object.keys(sessionStorage)) if (key.startsWith(PREFIX)) sessionStorage.removeItem(key);
  } catch { /* Private browsing may disable storage. */ }
}
