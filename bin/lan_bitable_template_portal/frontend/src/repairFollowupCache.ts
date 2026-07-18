import type { LooseDict } from "./types";

type FollowupCacheEntry = {
  expiresAt: number;
  payload: LooseDict;
};

const FOLLOWUP_CACHE_TTL_MS = 60_000;
const FOLLOWUP_CACHE_MAX_ENTRIES = 80;
const responseCache = new Map<string, FollowupCacheEntry>();

export function getRepairFollowupCache(key: string): LooseDict | null {
  const cached = responseCache.get(key);
  if (!cached) return null;
  if (cached.expiresAt <= Date.now()) {
    responseCache.delete(key);
    return null;
  }
  return cached.payload;
}

export function setRepairFollowupCache(key: string, payload: LooseDict): void {
  responseCache.set(key, {
    expiresAt: Date.now() + FOLLOWUP_CACHE_TTL_MS,
    payload,
  });
  while (responseCache.size > FOLLOWUP_CACHE_MAX_ENTRIES) {
    responseCache.delete(responseCache.keys().next().value || "");
  }
}

export function clearRepairFollowupCache(summaryRecordId = ""): void {
  const normalizedId = String(summaryRecordId || "").trim();
  if (!normalizedId) {
    responseCache.clear();
    return;
  }
  const encodedId = `summary_record_id=${encodeURIComponent(normalizedId)}`;
  for (const key of responseCache.keys()) {
    if (key.includes(encodedId)) responseCache.delete(key);
  }
}
