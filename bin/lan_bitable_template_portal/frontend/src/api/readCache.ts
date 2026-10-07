type Entry = { at: number; json: string };
const entries = new Map<string, Entry>();
const LIST_PATHS = new Set([
  '/api/scope-overview', '/api/handover-links', '/api/events/overview', '/api/events/monthly',
  '/api/repair-management/records', '/api/repair-management/bootstrap',
  '/api/capacity/water/bootstrap', '/api/capacity/water/records',
  '/api/drills', '/api/drills/bootstrap', '/api/critical-guard/bootstrap', '/api/critical-guard/tasks',
  '/api/daily-tasks', '/api/daily-tasks/bootstrap', '/api/learning/bootstrap', '/api/learning/papers',
  '/api/cabinet-power/buildings', '/api/cabinet-power/overview', '/api/cabinet-power/racks', '/api/cabinet-power/batches',
]);
let identity = '', generation = 0, bytes = 0;
export const READ_CACHE_UPDATED = 'clipflow-read-cache-updated';
export function invalidateReadCache(): void { entries.clear(); bytes = 0; generation++; }
export function setReadCacheIdentity(next: string): void {
  if (next !== identity) { identity = next; invalidateReadCache(); }
}
export function readCacheGeneration(): number { return generation; }
export function readCacheKey(path: string, origin: string): string {
  if (!identity) return '';
  const url = new URL(path, origin);
  if (url.origin !== origin || !LIST_PATHS.has(url.pathname) || ['refresh', 'force', 'prefetch'].some(key => url.searchParams.has(key))) return '';
  url.searchParams.sort();
  return identity + ':' + url.pathname + url.search;
}
export function cachedRead(key: string): { data: Record<string, any>; at: number } | null {
  const entry = entries.get(key);
  if (!entry) return null;
  if (Date.now() - entry.at > 60_000) { entries.delete(key); bytes -= entry.json.length; return null; }
  entries.delete(key); entries.set(key, entry);
  return { data: JSON.parse(entry.json), at: entry.at };
}
export function saveRead(key: string, value: Record<string, any>, stamp: number): void {
  if (!key || stamp !== generation || !value || typeof value !== 'object' || Array.isArray(value) || value.source_snapshot_ready === false) return;
  const json = JSON.stringify(value);
  if (json.length > 1_000_000) return;
  const previous = entries.get(key);
  bytes -= previous?.json.length || 0;
  entries.delete(key); entries.set(key, { at: Date.now(), json }); bytes += json.length;
  while (entries.size > 48 || bytes > 8_000_000) {
    const oldest = entries.keys().next().value!;
    bytes -= entries.get(oldest)!.json.length; entries.delete(oldest);
  }
}
