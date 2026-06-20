import { isKnownWorkType, manualPrefillWorkTypes } from "./noticeTemplates";

type Dict = Record<string, any>;
type StoredEnvelope<T> = {
  version: 1;
  updated_at: number;
  value: T;
};

const draftStorageTtlMs = 14 * 24 * 60 * 60 * 1000;
const manualRecentTtlMs = 90 * 24 * 60 * 60 * 1000;
const manualTemplateTtlMs = 180 * 24 * 60 * 60 * 1000;
const maxStoredDrafts = 80;
const maxManualRecentTypes = 3;

function normalizeOpenId(value: string): string {
  return String(value || "anonymous").trim() || "anonymous";
}

function normalizeScope(value: string): string {
  return String(value || "ALL").trim() || "ALL";
}

function draftStorageKey(openId: string, scope: string): string {
  return `clipflow-vue-workbench:${normalizeOpenId(openId)}:${normalizeScope(scope)}`;
}

function legacyDraftStorageKey(scope: string): string {
  return `clipflow-vue-workbench:${normalizeScope(scope)}`;
}

function manualRecentStorageKey(openId: string, scope: string): string {
  return `clipflow-manual-recent:${normalizeOpenId(openId)}:${normalizeScope(scope)}`;
}

function manualTemplateMemoryKey(openId: string, scope: string, type: string): string {
  return `clipflow-manual-template:${normalizeOpenId(openId)}:${normalizeScope(scope)}:${type}`;
}

function envelope<T>(value: T): StoredEnvelope<T> {
  return {
    version: 1,
    updated_at: Date.now(),
    value,
  };
}

function readJsonValue<T>(raw: string | null, maxAgeMs: number): { expired: boolean; value: T | null } {
  if (!raw) return { expired: false, value: null };
  const payload = JSON.parse(raw);
  if (
    payload
    && typeof payload === "object"
    && payload.version === 1
    && typeof payload.updated_at === "number"
    && Object.prototype.hasOwnProperty.call(payload, "value")
  ) {
    if (Date.now() - Number(payload.updated_at || 0) > maxAgeMs) {
      return { expired: true, value: null };
    }
    return { expired: false, value: payload.value as T };
  }
  return { expired: false, value: payload as T };
}

function removeStorageKey(key: string): void {
  try {
    localStorage.removeItem(key);
  } catch {
    // 清理失败不影响页面使用。
  }
}

function setJsonValue<T>(key: string, value: T): void {
  localStorage.setItem(key, JSON.stringify(envelope(value)));
}

function normalizeRecentTypes(values: unknown): string[] {
  return Array.isArray(values)
    ? values.filter((value) => isKnownWorkType(value)).slice(0, maxManualRecentTypes)
    : [];
}

function normalizeDraftPayload(payload: unknown): { selected: string[]; drafts: Record<string, Dict> } {
  if (!payload || typeof payload !== "object") throw new Error("invalid draft payload");
  const source = payload as Dict;
  const selected = Array.isArray(source.selected)
    ? Array.from(new Set(source.selected.map((key: unknown) => String(key)).filter(Boolean))).slice(0, maxStoredDrafts)
    : [];
  const draftPayload = source.drafts && typeof source.drafts === "object" ? source.drafts : {};
  const loadedDrafts: Record<string, Dict> = {};
  const orderedKeys = [
    ...selected,
    ...Object.keys(draftPayload).filter((key) => !selected.includes(key)),
  ].slice(0, maxStoredDrafts);
  for (const key of orderedKeys) {
    const value = (draftPayload as Record<string, unknown>)[key];
    if (value && typeof value === "object") loadedDrafts[key] = value as Dict;
  }
  return {
    selected: selected.filter((key) => loadedDrafts[key]),
    drafts: loadedDrafts,
  };
}

function limitedDraftPayload(selected: string[], drafts: Record<string, Dict>): { selected: string[]; drafts: Record<string, Dict> } {
  const selectedKeys = Array.from(new Set(selected.map((key) => String(key)).filter(Boolean))).slice(0, maxStoredDrafts);
  const orderedKeys = [
    ...selectedKeys,
    ...Object.keys(drafts).filter((key) => !selectedKeys.includes(key)),
  ].slice(0, maxStoredDrafts);
  const limitedDrafts: Record<string, Dict> = {};
  for (const key of orderedKeys) {
    const value = drafts[key];
    if (value && typeof value === "object") limitedDrafts[key] = value;
  }
  return {
    selected: selectedKeys.filter((key) => limitedDrafts[key]),
    drafts: limitedDrafts,
  };
}

export function loadManualRecentTypesFromStorage(openId: string, scope: string): string[] {
  const key = manualRecentStorageKey(openId, scope);
  try {
    const result = readJsonValue<unknown>(localStorage.getItem(key), manualRecentTtlMs);
    if (result.expired) removeStorageKey(key);
    return normalizeRecentTypes(result.value || []);
  } catch {
    removeStorageKey(key);
    return [];
  }
}

export function saveManualRecentTypeToStorage(openId: string, scope: string, type: string, currentTypes: string[]): string[] {
  if (!isKnownWorkType(type)) return currentTypes;
  const next = [type, ...currentTypes.filter((value) => value !== type)].slice(0, maxManualRecentTypes);
  try {
    setJsonValue(manualRecentStorageKey(openId, scope), next);
  } catch {
    // 最近使用只是体验优化，失败不阻塞纯手填。
  }
  return next;
}

export function manualTemplateSnapshot(draft: Dict): Dict {
  const allowed = [
    "title",
    "building",
    "building_codes",
    "specialty",
    "level",
    "maintenance_cycle",
    "start_time",
    "end_time",
    "location",
    "content",
    "reason",
    "impact",
    "progress",
    "repair_device",
    "repair_fault",
    "fault_type",
    "repair_mode",
    "discovery",
    "symptom",
    "solution",
    "spare_parts",
    "device",
    "cabinet",
    "quantity",
  ];
  const snapshot: Dict = {};
  for (const field of allowed) {
    const value = draft[field];
    if (Array.isArray(value)) snapshot[field] = [...value];
    else if (value !== undefined) snapshot[field] = value;
  }
  return snapshot;
}

export function loadManualTemplateMemoryFromStorage(openId: string, scope: string, type: string): Dict | null {
  if (!isKnownWorkType(type) || !manualPrefillWorkTypes.has(type)) return null;
  const key = manualTemplateMemoryKey(openId, scope, type);
  try {
    const result = readJsonValue<unknown>(localStorage.getItem(key), manualTemplateTtlMs);
    if (result.expired) {
      removeStorageKey(key);
      return null;
    }
    const payload = result.value;
    if (!payload || typeof payload !== "object") return null;
    return manualTemplateSnapshot(payload as Dict);
  } catch {
    removeStorageKey(key);
    return null;
  }
}

export function saveManualTemplateMemoryToStorage(openId: string, scope: string, type: string, draft: Dict): boolean {
  if (!isKnownWorkType(type) || !manualPrefillWorkTypes.has(type)) return true;
  try {
    setJsonValue(
      manualTemplateMemoryKey(openId, scope, type),
      manualTemplateSnapshot(draft),
    );
    return true;
  } catch {
    return false;
  }
}

export function loadDraftStorage(openId: string, scope: string): { selected: string[]; drafts: Record<string, Dict> } {
  const key = draftStorageKey(openId, scope);
  const legacyKey = legacyDraftStorageKey(scope);
  try {
    const primary = readJsonValue<unknown>(localStorage.getItem(key), draftStorageTtlMs);
    if (primary.expired) removeStorageKey(key);
    if (primary.value) return normalizeDraftPayload(primary.value);
    const legacy = readJsonValue<unknown>(localStorage.getItem(legacyKey), draftStorageTtlMs);
    if (legacy.expired) {
      removeStorageKey(legacyKey);
      return { selected: [], drafts: {} };
    }
    return legacy.value ? normalizeDraftPayload(legacy.value) : { selected: [], drafts: {} };
  } catch {
    removeStorageKey(key);
    return { selected: [], drafts: {} };
  }
}

export function saveDraftStorage(openId: string, scope: string, selected: string[], drafts: Record<string, Dict>): boolean {
  try {
    setJsonValue(draftStorageKey(openId, scope), limitedDraftPayload(selected, drafts));
    return true;
  } catch {
    return false;
  }
}
