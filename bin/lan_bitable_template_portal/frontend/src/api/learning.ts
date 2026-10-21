import { requestJson, type Dict } from "./client";
import { completionDateError } from "../learningDates";

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

export type CompletionQuery = {
  from?: string; to?: string; scope?: string; person_id?: string;
  status?: string; shift?: string; q?: string; page?: number; page_size?: number;
};

export async function getLearningCompletion(query: CompletionQuery, signal?: AbortSignal): Promise<Dict> {
  const error = completionDateError(query.from, query.to, true);
  if (error) throw new Error(error);
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(query)) {
    if (value !== undefined && value !== '') params.set(key, String(value));
  }
  return requestLearning({ signal }, `/completion?${params}`);
}
