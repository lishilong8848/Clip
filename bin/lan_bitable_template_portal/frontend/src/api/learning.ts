import { requestJson, type Dict } from "./client";

export type LearningApiOptions = {
  signal?: AbortSignal | null;
  onSyncPending?: () => void;
};

/**
 * 学练请求：统一走 client.requestJson，固定 /api/learning 前缀，
 * 支持 AbortSignal、GET no-store、FormData / JSON 序列化，
 * 并在非 GET 响应带 sync_pending 时回调，便于组件刷新同步状态。
 */
export async function requestLearning(
  options: LearningApiOptions,
  path: string,
  method = "GET",
  body?: unknown,
): Promise<Dict> {
  const result = await requestJson(`/api/learning${path}`, {
    method,
    signal: options.signal || undefined,
    ...(method === "GET" ? { cache: "no-store" as RequestCache } : {}),
    ...(body !== undefined ? { body: body instanceof FormData ? body : JSON.stringify(body) } : {}),
  });
  if (method !== "GET" && result.sync_pending) options.onSyncPending?.();
  return result;
}
