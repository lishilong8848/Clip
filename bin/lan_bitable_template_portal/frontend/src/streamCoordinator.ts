type Dict = Record<string, any>;

type BroadcastHandler<T extends Dict> = (payload: T) => void;
type RoleHandler = (leader: boolean) => void;

type LeaderRecord = {
  tab_id: string;
  expires_at: number;
};

export type CrossTabStreamCoordinator<T extends Dict> = {
  readonly supported: boolean;
  isLeader(): boolean;
  start(onBroadcast: BroadcastHandler<T>, onRoleChange: RoleHandler): void;
  stop(): void;
  broadcast(payload: T): void;
};

function makeTabId(): string {
  try {
    const cryptoRef = globalThis.crypto;
    if (cryptoRef?.randomUUID) return cryptoRef.randomUUID();
  } catch {
    // Fall through to the timestamp/random fallback.
  }
  return `tab-${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
}

function readLeader(key: string): LeaderRecord | null {
  try {
    const raw = localStorage.getItem(key);
    if (!raw) return null;
    const payload = JSON.parse(raw);
    const tabId = String(payload?.tab_id || "");
    const expiresAt = Number(payload?.expires_at || 0);
    if (!tabId || !Number.isFinite(expiresAt)) return null;
    return { tab_id: tabId, expires_at: expiresAt };
  } catch {
    return null;
  }
}

function writeLeader(key: string, tabId: string, ttlMs: number): boolean {
  try {
    localStorage.setItem(key, JSON.stringify({
      tab_id: tabId,
      expires_at: Date.now() + ttlMs,
    }));
    return readLeader(key)?.tab_id === tabId;
  } catch {
    return false;
  }
}

function removeLeaderIfOwned(key: string, tabId: string): void {
  try {
    if (readLeader(key)?.tab_id === tabId) localStorage.removeItem(key);
  } catch {
    // Best-effort cleanup only.
  }
}

export function createCrossTabStreamCoordinator<T extends Dict>(
  options: {
    channelName: string;
    leaderKey: string;
    heartbeatMs?: number;
    leaderTtlMs?: number;
  },
): CrossTabStreamCoordinator<T> {
  const tabId = makeTabId();
  const heartbeatMs = Math.max(1000, Number(options.heartbeatMs || 5000));
  const leaderTtlMs = Math.max(heartbeatMs * 2, Number(options.leaderTtlMs || 15000));
  const supported = typeof window !== "undefined"
    && typeof BroadcastChannel !== "undefined"
    && typeof localStorage !== "undefined";

  let channel: BroadcastChannel | null = null;
  let heartbeatTimer: number | null = null;
  let leader: boolean | null = null;
  let started = false;
  let onBroadcastHandler: BroadcastHandler<T> = () => undefined;
  let onRoleChangeHandler: RoleHandler = () => undefined;

  function setLeader(next: boolean): void {
    if (leader === next) return;
    leader = next;
    onRoleChangeHandler(next);
  }

  function evaluateLeadership(): void {
    if (!supported || !started) {
      setLeader(true);
      return;
    }
    const now = Date.now();
    const current = readLeader(options.leaderKey);
    if (!current || current.tab_id === tabId || current.expires_at <= now) {
      setLeader(writeLeader(options.leaderKey, tabId, leaderTtlMs));
      return;
    }
    setLeader(false);
  }

  function handleStorage(event: StorageEvent): void {
    if (event.key === options.leaderKey) evaluateLeadership();
  }

  function handlePageHide(): void {
    removeLeaderIfOwned(options.leaderKey, tabId);
  }

  return {
    supported,
    isLeader: () => leader === true,
    start(onBroadcast: BroadcastHandler<T>, onRoleChange: RoleHandler): void {
      if (started) return;
      started = true;
      onBroadcastHandler = onBroadcast;
      onRoleChangeHandler = onRoleChange;
      if (!supported) {
        setLeader(true);
        return;
      }
      channel = new BroadcastChannel(options.channelName);
      channel.onmessage = (event: MessageEvent) => {
        const payload = event.data || {};
        if (payload.origin === tabId) return;
        if (payload.message && typeof payload.message === "object") {
          onBroadcastHandler(payload.message as T);
        }
      };
      window.addEventListener("storage", handleStorage);
      window.addEventListener("pagehide", handlePageHide);
      heartbeatTimer = window.setInterval(evaluateLeadership, heartbeatMs);
      evaluateLeadership();
    },
    stop(): void {
      started = false;
      if (heartbeatTimer !== null) {
        window.clearInterval(heartbeatTimer);
        heartbeatTimer = null;
      }
      window.removeEventListener("storage", handleStorage);
      window.removeEventListener("pagehide", handlePageHide);
      if (channel) {
        channel.close();
        channel = null;
      }
      removeLeaderIfOwned(options.leaderKey, tabId);
      setLeader(false);
    },
    broadcast(payload: T): void {
      if (!supported || !channel) return;
      try {
        channel.postMessage({ origin: tabId, message: payload });
      } catch {
        // Broadcast is an optimization; the original polling fallback still works.
      }
    },
  };
}
