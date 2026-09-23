import { computed, onBeforeUnmount, ref, watch } from "vue";
import { ApiError, requestJson, type Dict } from "../api/client";
import { resilientStorage } from "../browserStorage";

type Pending = { id: string; body: string; path: string; method: string; retried?: boolean };

export function useRepairSubmission(key: () => string, recovered: (result: Dict) => void, legacyKey?: () => string) {
  const pending = ref<Pending | null>(null);
  const checking = ref(false);
  const detail = ref("");
  const status = ref("");
  const storage = resilientStorage("sessionStorage", () => { detail.value = "浏览器未允许保存提交标识，请勿关闭本页。"; });
  let disposed = false;
  let submitting = false;
  const storageKey = () => `repair-submission:${key()}`;
  const overwrite = computed(() => pending.value?.method === "PUT" && pending.value.path.startsWith("/api/repair-management/records/"));
  const message = computed(() => {
    if (status.value === "failed") {
      if (detail.value.includes("原保存内容不完整")) return "上次保存已中断，请按当前填写重新保存。";
      return detail.value || "保存失败，请按当前填写重新保存。";
    }
    if (status.value === "remote_written") return "多维已保存，正在更新页面。";
    if (status.value === "uncertain") return "保存响应超时，请点击下方“继续保存”。";
    return overwrite.value ? "维修单正在保存。" : "跟进记录正在保存。";
  });

  function clear() {
    storage.removeItem(storageKey());
    pending.value = null;
  }

  async function check(notify = true, recover = false): Promise<Dict | null> {
    const item = pending.value;
    const currentKey = key();
    if (!item || checking.value || disposed || submitting) return null;
    checking.value = true;
    try {
      const result = await requestJson(`/api/repair-management/operations/${encodeURIComponent(item.id)}`, {
        method: recover ? "POST" : "GET",
        timeoutMs: recover ? 90_000 : 15_000,
      });
      if (disposed || currentKey !== key() || pending.value?.id !== item.id) return null;
      status.value = String(result.status || "");
      detail.value = String(result.error || "");
      if (result.result && ["completed", "sync_pending", "superseded"].includes(status.value)) {
        clear();
        if (notify) recovered(result.result);
        return result.result;
      }
      if (result.retryable) {
        status.value = "failed";
        detail.value ||= "本次未写入，可保留填写后重新提交。";
      }
    } catch (error) {
      if (disposed || currentKey !== key() || pending.value?.id !== item.id) return null;
      detail.value = error instanceof Error ? error.message : "服务暂时无法连接，填写已保留。";
      if (overwrite.value && error instanceof ApiError && error.status === 404) {
        if (item.retried) status.value = "failed";
        else {
          item.retried = true;
          storage.setItem(storageKey(), JSON.stringify(item));
          try {
            const result = await requestJson(item.path, { method: item.method, body: item.body, timeoutMs: 90_000 });
            if (disposed || currentKey !== key() || pending.value?.id !== item.id) return null;
            clear();
            if (notify) recovered(result);
            return result;
          } catch (retryError) {
            if (currentKey === key()) detail.value = retryError instanceof Error ? retryError.message : "保存暂未完成。";
          }
        }
      }
    } finally {
      if (currentKey === key()) {
        checking.value = false;
      }
    }
    return null;
  }

  async function submit(path: string, options: { method: string; body: string }): Promise<Dict> {
    if (pending.value && status.value !== "failed") {
      const recovered = await check(false, true);
      if (recovered) return recovered;
      if (status.value !== "failed") throw new Error(message.value);
    }
    if (pending.value) clear();
    const body = JSON.parse(options.body);
    const originalKey = key();
    const originalStorageKey = storageKey();
    pending.value = { id: String(body.operation_id), body: options.body, path, method: options.method };
    storage.setItem(storageKey(), JSON.stringify(pending.value));
    submitting = true;
    status.value = "processing";
    detail.value = "";
    try {
      const result = await requestJson(path, { ...options, timeoutMs: 90_000 });
      if (disposed || originalKey !== key()) {
        storage.removeItem(originalStorageKey);
        throw new Error("原页面提交已保存，返回原页面可查看记录。");
      }
      clear();
      return result;
    } catch (error) {
      if (originalKey === key()) submitting = false;
      if (disposed || originalKey !== key()) throw error;
      // A definite pre-handler validation error cannot have performed a write.
      if (error instanceof ApiError && [401, 403, 422].includes(error.status)) clear();
      else {
        const result = await check(false);
        if (result) return result;
      }
      throw error;
    } finally {
      if (originalKey === key()) submitting = false;
    }
  }

  watch(key, () => {
    pending.value = null;
    status.value = "";
    detail.value = "";
    checking.value = false;
    submitting = false;
    if (legacyKey && !storage.getItem(storageKey())) {
      const oldKey = `repair-submission:${legacyKey()}`;
      try {
        const old = JSON.parse(storage.getItem(oldKey) || "null") as Pending | null;
        const target = old?.method === "PUT" ? old.path.split("/").pop() : "new";
        if (old && key().endsWith(`:${target}`)) {
          storage.setItem(storageKey(), JSON.stringify(old));
          storage.removeItem(oldKey);
        }
      } catch { /* Invalid old drafts are not submitted again. */ }
    }
    try { pending.value = JSON.parse(storage.getItem(storageKey()) || "null"); } catch { /* Invalid browser draft is not a submitted record. */ }
    if (pending.value) void check();
  }, { immediate: true });
  onBeforeUnmount(() => { disposed = true; });
  return { pending, checking, status, message, detail, overwrite, submit, check };
}
