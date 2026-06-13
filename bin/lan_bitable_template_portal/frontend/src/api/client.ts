export type Dict = Record<string, any>;

export type ApiClientHooks = {
  onOnline?: () => void;
  onOffline?: (message: string, error: unknown) => void;
  onAuthExpired?: (message: string, response: Response, payload: Dict) => void;
  onServerError?: (message: string, response: Response, payload: Dict) => void;
};

export const AUTH_EXPIRED_EVENT = "clipflow-auth-expired";

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

export async function requestJson(
  path: string,
  options: RequestInit = {},
  hooks: ApiClientHooks = {},
): Promise<Dict> {
  let response: Response;
  try {
    response = await fetch(path, {
      ...options,
      credentials: options.credentials || "same-origin",
      headers: buildHeaders(options),
    });
    hooks.onOnline?.();
  } catch (error: unknown) {
    const message = error instanceof Error && error.message ? error.message : "后端连接中断";
    hooks.onOffline?.("后端连接中断，已保留当前页面数据。", error);
    throw new ApiError(message, { offline: true });
  }

  const payload = await response.json().catch(() => ({} as Dict));
  if (response.status === 401 || payload.auth_required) {
    const message = String(payload.error || "登录已过期，请重新扫码登录。");
    hooks.onAuthExpired?.(message, response, payload);
    if (typeof window !== "undefined") {
      window.dispatchEvent(new CustomEvent(AUTH_EXPIRED_EVENT, { detail: { message } }));
    }
    throw new ApiError(message, {
      status: response.status,
      payload,
      authRequired: true,
    });
  }

  if (response.status >= 500) {
    hooks.onServerError?.(String(payload.error || "后端服务异常，稍后会自动重试。"), response, payload);
  }

  if (!response.ok || payload.ok === false) {
    throw new ApiError(String(payload.error || `HTTP ${response.status}`), {
      status: response.status,
      payload,
    });
  }

  return Object.prototype.hasOwnProperty.call(payload, "data") ? payload.data : payload;
}

export async function requestBinaryJson(
  path: string,
  body: BodyInit,
  options: RequestInit = {},
  hooks: ApiClientHooks = {},
): Promise<Dict> {
  let response: Response;
  try {
    response = await fetch(path, {
      ...options,
      method: options.method || "POST",
      credentials: options.credentials || "same-origin",
      headers: options.headers,
      body,
    });
    hooks.onOnline?.();
  } catch (error: unknown) {
    const message = error instanceof Error && error.message ? error.message : "后端连接中断";
    hooks.onOffline?.("后端连接中断，已保留当前页面数据。", error);
    throw new ApiError(message, { offline: true });
  }

  const payload = await response.json().catch(() => ({} as Dict));
  if (response.status === 401 || payload.auth_required) {
    const message = String(payload.error || "登录已过期，请重新扫码登录。");
    hooks.onAuthExpired?.(message, response, payload);
    if (typeof window !== "undefined") {
      window.dispatchEvent(new CustomEvent(AUTH_EXPIRED_EVENT, { detail: { message } }));
    }
    throw new ApiError(message, {
      status: response.status,
      payload,
      authRequired: true,
    });
  }

  if (response.status >= 500) {
    hooks.onServerError?.(String(payload.error || "后端服务异常，稍后会自动重试。"), response, payload);
  }

  if (!response.ok || payload.ok === false) {
    throw new ApiError(String(payload.error || `HTTP ${response.status}`), {
      status: response.status,
      payload,
    });
  }

  return Object.prototype.hasOwnProperty.call(payload, "data") ? payload.data : payload;
}
