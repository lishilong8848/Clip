import { requestJson, type Dict } from "./api/client";

const MOP_BOOTSTRAP_CACHE_TTL_MS = 10_000;
const mopBootstrapCache = new Map<string, { expiresAt: number; data: Dict }>();
const mopBootstrapInflight = new Map<string, Promise<Dict>>();
const mopBootstrapGeneration = new Map<string, number>();

function clonePayload(payload: Dict): Dict {
  if (typeof structuredClone === "function") {
    return structuredClone(payload);
  }
  return JSON.parse(JSON.stringify(payload)) as Dict;
}

function bootstrapCacheKey(scope: unknown): string {
  return String(scope || "").trim().toUpperCase();
}

function nextBootstrapGeneration(cacheKey: string): number {
  const generation = (mopBootstrapGeneration.get(cacheKey) || 0) + 1;
  mopBootstrapGeneration.set(cacheKey, generation);
  return generation;
}

export function invalidateEngineerMopBootstrap(scope = ""): void {
  const cacheKey = bootstrapCacheKey(scope);
  if (cacheKey) {
    nextBootstrapGeneration(cacheKey);
    mopBootstrapCache.delete(cacheKey);
    mopBootstrapInflight.delete(cacheKey);
    return;
  }
  for (const key of new Set([
    ...mopBootstrapCache.keys(),
    ...mopBootstrapInflight.keys(),
    ...mopBootstrapGeneration.keys(),
  ])) {
    nextBootstrapGeneration(key);
  }
  mopBootstrapCache.clear();
  mopBootstrapInflight.clear();
}

export function fetchEngineerMopBootstrap(
  scope: string,
  options: { force?: boolean; signal?: AbortSignal } = {},
): Promise<Dict> {
  const cacheKey = bootstrapCacheKey(scope);
  const now = Date.now();
  const cached = mopBootstrapCache.get(cacheKey);
  if (!options.force && cached && cached.expiresAt > now) {
    return Promise.resolve(clonePayload(cached.data));
  }
  if (!options.force && !options.signal) {
    const pending = mopBootstrapInflight.get(cacheKey);
    if (pending) return pending.then(clonePayload);
  }
  if (options.force) {
    mopBootstrapCache.delete(cacheKey);
    mopBootstrapInflight.delete(cacheKey);
  }
  const requestGeneration = nextBootstrapGeneration(cacheKey);

  const request = requestJson(
    `/api/engineer/mop/bootstrap?scope=${encodeURIComponent(scope)}`,
    { signal: options.signal },
  ).then((data) => {
    if (mopBootstrapGeneration.get(cacheKey) === requestGeneration) {
      mopBootstrapCache.set(cacheKey, {
        expiresAt: Date.now() + MOP_BOOTSTRAP_CACHE_TTL_MS,
        data: clonePayload(data),
      });
    }
    return data;
  });

  if (options.signal) return request.then(clonePayload);

  mopBootstrapInflight.set(cacheKey, request);
  return request
    .then(clonePayload)
    .finally(() => {
      if (mopBootstrapInflight.get(cacheKey) === request) {
        mopBootstrapInflight.delete(cacheKey);
      }
    });
}

export function bindEngineerMop(payload: Dict): Promise<Dict> {
  return requestJson("/api/engineer/mop/bind", {
    method: "POST",
    body: JSON.stringify(payload),
  }).then((data) => {
    invalidateEngineerMopBootstrap(String(payload.scope || ""));
    return data;
  });
}

export function uploadLocalEngineerMop(formData: FormData): Promise<Dict> {
  return requestJson("/api/engineer/mop/upload-local", {
    method: "POST",
    body: formData,
  }).then((data) => {
    invalidateEngineerMopBootstrap(String(formData.get("scope") || ""));
    return data;
  });
}

export function previewEngineerMop(params: {
  scope: string;
  mopRecordId: string;
  fileToken: string;
  fileName: string;
  uploadId?: string;
}): Promise<Dict> {
  const query = new URLSearchParams({
    scope: params.scope,
    mop_record_id: params.mopRecordId,
    file_token: params.fileToken,
    file_name: params.fileName,
  });
  if (params.uploadId) query.set("upload_id", params.uploadId);
  return requestJson(`/api/engineer/mop/preview?${query.toString()}`);
}

export function fillEngineerMop(payload: Dict): Promise<Dict> {
  return requestJson("/api/engineer/mop/fill", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function uploadSignedEngineerMop(payload: Dict): Promise<Dict> {
  return requestJson("/api/engineer/mop/upload-signed", {
    method: "POST",
    body: JSON.stringify(payload),
  }).then((data) => {
    invalidateEngineerMopBootstrap(String(payload.scope || ""));
    return data;
  });
}

export function resetEngineerMop(payload: {
  scope: string;
  filledFilePath: string;
  mopRecordId: string;
  fileToken: string;
  fileName: string;
}): Promise<Dict> {
  return requestJson("/api/engineer/mop/reset", {
    method: "POST",
    body: JSON.stringify({
      scope: payload.scope,
      filled_file_path: payload.filledFilePath,
      mop_record_id: payload.mopRecordId,
      file_token: payload.fileToken,
      file_name: payload.fileName,
    }),
  }).then((data) => {
    invalidateEngineerMopBootstrap(payload.scope);
    return data;
  });
}
