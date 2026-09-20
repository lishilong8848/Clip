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

export function randomHexId(): string {
  const cryptoApi = typeof globalThis !== "undefined" ? globalThis.crypto : undefined;
  if (typeof cryptoApi?.randomUUID === "function") return cryptoApi.randomUUID().replace(/-/g, "");
  if (typeof cryptoApi?.getRandomValues === "function") {
    const bytes = cryptoApi.getRandomValues(new Uint8Array(16));
    return Array.from(bytes, value => value.toString(16).padStart(2, "0")).join("");
  }
  return `${Date.now().toString(16)}${Math.random().toString(16).slice(2)}${Math.random().toString(16).slice(2)}`.padEnd(32, "0").slice(0, 32);
}
