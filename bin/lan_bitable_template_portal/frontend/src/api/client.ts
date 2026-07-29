export type Dict = Record<string, any>;

export type ApiClientHooks = {
  onOnline?: () => void;
  onOffline?: (message: string, error: unknown) => void;
  onAuthExpired?: (message: string, response: Response, payload: Dict) => void;
  onServerError?: (message: string, response: Response, payload: Dict) => void;
};

export const AUTH_EXPIRED_EVENT = "clipflow-auth-expired";
const AUTH_REDIRECT_FLAG = "__clipflowAuthRedirecting";

export class ApiError extends Error {
  readonly status: number;
  readonly payload: Dict;
  readonly authRequired: boolean;
  readonly offline: boolean;

  constructor(
    message: string,
    options: {
      status?: number;
      payload?: Dict;
      authRequired?: boolean;
      offline?: boolean;
    } = {},
  ) {
    super(message);
    this.name = "ApiError";
    this.status = options.status ?? 0;
    this.payload = options.payload || {};
    this.authRequired = Boolean(options.authRequired);
    this.offline = Boolean(options.offline);
  }
}

function buildHeaders(options: RequestInit): Headers {
  const headers = new Headers(options.headers || {});
  const body = options.body;
  if (!headers.has("Content-Type") && !(typeof FormData !== "undefined" && body instanceof FormData)) {
    headers.set("Content-Type", "application/json");
  }
  return headers;
}

function requestSignalWithTimeout(
  externalSignal: AbortSignal | null | undefined,
  timeoutMs: number,
): {
  signal: AbortSignal;
  timedOut: () => boolean;
  cleanup: () => void;
} {
  const controller = new AbortController();
  let timeoutTriggered = false;
  const abortFromExternal = () => controller.abort();
  if (externalSignal?.aborted) {
    abortFromExternal();
  } else {
    externalSignal?.addEventListener("abort", abortFromExternal, { once: true });
  }
  const timer = window.setTimeout(() => {
    timeoutTriggered = true;
    controller.abort();
  }, Math.max(1000, timeoutMs));
  return {
    signal: controller.signal,
    timedOut: () => timeoutTriggered,
    cleanup: () => {
      window.clearTimeout(timer);
      externalSignal?.removeEventListener("abort", abortFromExternal);
    },
  };
}

function currentLoginUrl(): string {
  if (typeof window === "undefined") return "/api/auth/login";
  const next = `${window.location.pathname}${window.location.search}`;
  return `/api/auth/login?next=${encodeURIComponent(next || "/")}`;
}

function shouldSuppressAuthRedirect(): boolean {
  if (typeof window === "undefined") return true;
  const path = window.location.pathname.replace(/\/$/, "") || "/";
  const params = new URLSearchParams(window.location.search);
  const isSignatureRoute = path === "/signature" || params.get("mode") === "signature";
  return Boolean(isSignatureRoute && (params.get("record_id") || params.get("temporary_id")));
}

function scheduleAuthRedirect(loginUrl: string): void {
  if (typeof window === "undefined" || shouldSuppressAuthRedirect()) return;
  const state = window as unknown as Record<string, unknown>;
  if (state[AUTH_REDIRECT_FLAG]) return;
  state[AUTH_REDIRECT_FLAG] = true;
  window.setTimeout(() => {
    window.location.assign(loginUrl || currentLoginUrl());
  }, 0);
}

function authExpiredDetail(message: string, payload: Dict): Dict {
  return {
    message,
    login_url: String(payload.login_url || payload.loginUrl || currentLoginUrl()),
  };
}

export async function requestJson(
  path: string,
  options: RequestInit = {},
  hooks: ApiClientHooks = {},
): Promise<Dict> {
  let response: Response;
  const requestSignal = requestSignalWithTimeout(options.signal, 45_000);
  try {
    response = await fetch(path, {
      ...options,
      credentials: options.credentials || "same-origin",
      headers: buildHeaders(options),
      signal: requestSignal.signal,
    });
    hooks.onOnline?.();
  } catch (error: unknown) {
    if (error instanceof Error && error.name === "AbortError") {
      throw new ApiError(requestSignal.timedOut() ? "请求超时，请稍后重试。" : "请求已取消。");
    }
    const message = error instanceof Error && error.message ? error.message : "服务连接中断";
    hooks.onOffline?.("服务连接中断，已保留当前页面数据。", error);
    throw new ApiError(message, { offline: true });
  } finally {
    requestSignal.cleanup();
  }

  const payload = await response.json().catch(() => ({} as Dict));
  if (response.status === 401 || payload.auth_required) {
    const message = String(payload.error || "登录已过期，请重新扫码登录。");
    const detail = authExpiredDetail(message, payload);
    hooks.onAuthExpired?.(message, response, payload);
    if (typeof window !== "undefined") {
      window.dispatchEvent(new CustomEvent(AUTH_EXPIRED_EVENT, { detail }));
      scheduleAuthRedirect(String(detail.login_url || ""));
    }
    throw new ApiError(message, {
      status: response.status,
      payload,
      authRequired: true,
    });
  }

  if (response.status >= 500) {
    hooks.onServerError?.(String(payload.error || "服务异常，稍后会自动重试。"), response, payload);
  }

  if (!response.ok || payload.ok === false) {
    throw new ApiError(String(payload.error || `HTTP ${response.status}`), {
      status: response.status,
      payload,
    });
  }

  return Object.prototype.hasOwnProperty.call(payload, "data") ? payload.data : payload;
}

export type RemoteSourceRefreshKind = "maintenance" | "repair" | "change" | "event";

type RemoteSourceRefreshOptions = {
  scope?: string;
  month?: string;
  signal?: AbortSignal | null;
  maxWaitMs?: number;
  onProgress?: (status: Dict) => void;
};

function waitWithSignal(delayMs: number, signal?: AbortSignal | null): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(new ApiError("请求已取消。"));
      return;
    }
    const timer = window.setTimeout(() => {
      signal?.removeEventListener("abort", abort);
      resolve();
    }, delayMs);
    const abort = () => {
      window.clearTimeout(timer);
      signal?.removeEventListener("abort", abort);
      reject(new ApiError("请求已取消。"));
    };
    signal?.addEventListener("abort", abort, { once: true });
  });
}

export async function refreshRemoteSourceAndWait(
  kind: RemoteSourceRefreshKind,
  startPath: string,
  options: RemoteSourceRefreshOptions = {},
): Promise<Dict> {
  const startPayload = await requestJson(
    startPath,
    { method: kind === "event" ? "POST" : "GET", signal: options.signal || undefined },
  );
  options.onProgress?.(startPayload);
  if (startPayload.mock_external === true) return startPayload;

  const inflightKey = `${kind}_refresh_inflight`;
  if (
    startPayload[inflightKey] === false
    && (
      startPayload[`${kind}_refresh_reused`] === true
      || startPayload[`${kind}_refreshed_at`]
    )
  ) {
    return startPayload;
  }
  if (
    kind === "event"
    && options.month
    && startPayload.event_refresh_month
    && String(startPayload.event_refresh_month) !== String(options.month)
  ) {
    throw new ApiError("其他月份的事件数据正在刷新，请稍后再试。");
  }

  const deadline = Date.now() + Math.max(15_000, options.maxWaitMs ?? 180_000);
  let delayMs = 650;
  while (Date.now() < deadline) {
    await waitWithSignal(delayMs, options.signal);
    const params = new URLSearchParams({
      kind,
      scope: options.scope || "ALL",
    });
    if (options.month) params.set("month", options.month);
    const status = await requestJson(
      `/api/source-refresh-status?${params.toString()}`,
      {
        signal: options.signal || undefined,
        cache: "no-store",
      },
    );
    options.onProgress?.(status);
    if (status.status === "success") {
      return status.result && typeof status.result === "object"
        ? { ...status.result, ...status }
        : status;
    }
    if (status.status === "failed") {
      throw new ApiError(String(status.error || "远端数据刷新失败。"));
    }
    delayMs = Math.min(2_500, Math.round(delayMs * 1.25));
  }
  throw new ApiError("后台刷新等待超时，当前仍显示上次成功数据；可稍后再试。");
}

export async function requestBinaryJson(
  path: string,
  body: BodyInit,
  options: RequestInit = {},
  hooks: ApiClientHooks = {},
): Promise<Dict> {
  let response: Response;
  const requestSignal = requestSignalWithTimeout(options.signal, 120_000);
  try {
    response = await fetch(path, {
      ...options,
      method: options.method || "POST",
      credentials: options.credentials || "same-origin",
      headers: options.headers,
      body,
      signal: requestSignal.signal,
    });
    hooks.onOnline?.();
  } catch (error: unknown) {
    if (error instanceof Error && error.name === "AbortError") {
      throw new ApiError(requestSignal.timedOut() ? "请求超时，请稍后重试。" : "请求已取消。");
    }
    const message = error instanceof Error && error.message ? error.message : "服务连接中断";
    hooks.onOffline?.("服务连接中断，已保留当前页面数据。", error);
    throw new ApiError(message, { offline: true });
  } finally {
    requestSignal.cleanup();
  }

  const payload = await response.json().catch(() => ({} as Dict));
  if (response.status === 401 || payload.auth_required) {
    const message = String(payload.error || "登录已过期，请重新扫码登录。");
    const detail = authExpiredDetail(message, payload);
    hooks.onAuthExpired?.(message, response, payload);
    if (typeof window !== "undefined") {
      window.dispatchEvent(new CustomEvent(AUTH_EXPIRED_EVENT, { detail }));
      scheduleAuthRedirect(String(detail.login_url || ""));
    }
    throw new ApiError(message, {
      status: response.status,
      payload,
      authRequired: true,
    });
  }

  if (response.status >= 500) {
    hooks.onServerError?.(String(payload.error || "服务异常，稍后会自动重试。"), response, payload);
  }

  if (!response.ok || payload.ok === false) {
    throw new ApiError(String(payload.error || `HTTP ${response.status}`), {
      status: response.status,
      payload,
    });
  }

  return Object.prototype.hasOwnProperty.call(payload, "data") ? payload.data : payload;
}
