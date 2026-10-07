type Draft = { conversation: string; at: number; text: string; commands: any[]; files: any[]; plans: Record<string, { version: number; values: Record<string, unknown>; files: any[] }> };
const prefix = 'lighthouse_draft:';
const lifetime = 7 * 86400000, maxBytes = 1024 * 1024;

export function readAssistantDraft(storage: Storage, account: string, conversation: string, now = Date.now()): Draft | null {
  if (!account || !conversation) return null;
  try {
    const raw = storage.getItem(prefix + account);
    if (!raw || raw.length > maxBytes) return null;
    const value = JSON.parse(raw);
    if (value.conversation !== conversation || !Number.isFinite(value.at) || now - value.at > lifetime || value.at > now + 60000) return null;
    if (typeof value.text !== 'string' || !Array.isArray(value.commands) || !Array.isArray(value.files) || !value.plans || typeof value.plans !== 'object' || Array.isArray(value.plans)) return null;
    return value;
  } catch { return null; }
}

export function writeAssistantDraft(storage: Storage, account: string, value: Draft): boolean {
  if (!account || !value.conversation) return true;
  try {
    if (!value.text && !value.files.length && !value.commands.length && !Object.keys(value.plans).length) {
      storage.removeItem(prefix + account); return true;
    }
    // Store editable business values and uploaded file references, never credentials or image bytes.
    const raw = JSON.stringify(value, (key, item) => /^(api[_-]?key|password|secret|access_token|refresh_token|authorization|preview|base64)$/i.test(key) ? undefined : item);
    if (raw.length > maxBytes) return false;
    storage.setItem(prefix + account, raw);
    return true;
  } catch { return false; }
}

export function planDraftValues(saved: Draft | null, plan: any): Record<string, unknown> | null {
  const item = saved?.plans?.[plan.id];
  if (plan.status !== 'needs_input' || !item || item.version !== plan.version || !item.values || typeof item.values !== 'object' || Array.isArray(item.values)) return null;
  return Object.fromEntries((plan.fields || []).filter((field: any) => Object.prototype.hasOwnProperty.call(item.values, field.name)).map((field: any) => [field.name, item.values[field.name]]));
}
