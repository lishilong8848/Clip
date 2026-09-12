export function resilientStorage(area: "localStorage" | "sessionStorage", onFailure: () => void) {
  const pending = new Map<string, string | null>();
  return {
    getItem(key: string): string | null {
      if (pending.has(key)) return pending.get(key) ?? null;
      try { return window[area].getItem(key); }
      catch { onFailure(); return null; }
    },
    setItem(key: string, value: string): void {
      pending.set(key, value);
      try { window[area].setItem(key, value); pending.delete(key); }
      catch { onFailure(); }
    },
    removeItem(key: string): void {
      pending.set(key, null);
      try { window[area].removeItem(key); pending.delete(key); }
      catch { onFailure(); }
    },
  };
}
