import { computed, onBeforeUnmount, ref, watch } from "vue";
import { ApiError, requestJson, type Dict } from "../api/client";
import { resilientStorage } from "../browserStorage";

type Pending = { id: string; body: string; path: string; method: string };

export function useRepairSubmission(key: () => string, recovered: (result: Dict) => void) {
  const pending = ref<Pending | null>(null);
  const checking = ref(false);
  const detail = ref("");
  const status = ref("");
  const storage = resilientStorage("sessionStorage", () => { detail.value = "浏览器未允许保存提交标识，请勿关闭本页。"; });
  let timer: ReturnType<typeof setTimeout> | undefined;
  let disposed = false;
  let submitting = false;
  const storageKey = () => `repair-submission:${key()}`;
  const message = computed(() => status.value === "remote_written"
    ? "多维已保存，正在恢复本地显示和关联同步。"
    : status.value === "uncertain" ? "写入结果待核实，请勿重新新增同一条记录。"
    : "原提交正在核验，可离开本页，返回后继续核验。"
  );

  function clear() {
    storage.removeItem(storageKey());
    pending.value = null;
    clearTimeout(timer);
  }

  async function check(notify = true): Promise<Dict | null> {
    const item = pending.value;
    const currentKey = key();
    if (!item || checking.value || disposed || submitting) return null;
    checking.value = true;
    try {
      const result = await requestJson(`/api/repair-management/operations/${encodeURIComponent(item.id)}`, { method: "POST" });
      if (disposed || currentKey !== key() || pending.value?.id !== item.id) return null;
      status.value = String(result.status || "");
      detail.value = String(result.error || "");
      if (result.result && ["completed", "sync_pending"].includes(status.value)) {
        clear();
        if (notify) recovered(result.result);
        return result.result;
      }
      if (result.retryable) {
        status.value = "failed";
        detail.value ||= "本次未写入，可保留填写后重新提交。";
      }
    } catch (error) {
      if (currentKey === key()) detail.value = error instanceof Error ? error.message : "暂时无法核验，原提交已保留。";
    } finally {
      checking.value = false;
      clearTimeout(timer);
      if (!disposed && pending.value && status.value !== "failed") timer = setTimeout(() => { void check(); }, 10000);
    }
    return null;
  }

  async function submit(path: string, options: { method: string; body: string }): Promise<Dict> {
    if (pending.value) throw new Error("请先核验上次提交。");
    const body = JSON.parse(options.body);
    const originalKey = key();
    const originalStorageKey = storageKey();
    pending.value = { id: String(body.operation_id), body: options.body, path, method: options.method };
    storage.setItem(storageKey(), JSON.stringify(pending.value));
    submitting = true;
    status.value = "processing";
    try {
      const result = await requestJson(path, options);
      if (disposed || originalKey !== key()) {
        storage.removeItem(originalStorageKey);
        throw new Error("原页面提交已保存，返回原页面可查看记录。");
      }
      clear();
      return result;
    } catch (error) {
      submitting = false;
      if (disposed || originalKey !== key()) throw error;
      // A definite pre-handler validation error cannot have performed a write.
      if (error instanceof ApiError && [401, 403, 422].includes(error.status)) clear();
      else {
        const result = await check(false);
        if (result) return result;
      }
      throw error;
    } finally {
      submitting = false;
    }
  }

  function dismissFailed() {
    if (status.value === "failed") clear();
  }

  async function copyInput() {
    try {
      if (pending.value) await navigator.clipboard.writeText(JSON.stringify(JSON.parse(pending.value.body), null, 2));
    } catch { detail.value = "浏览器未允许复制，原填写仍保存在本页提交记录中。"; }
  }

  watch(key, () => {
    clearTimeout(timer);
    pending.value = null;
    status.value = "";
    detail.value = "";
    try { pending.value = JSON.parse(storage.getItem(storageKey()) || "null"); } catch { /* Invalid browser draft is not a submitted record. */ }
    if (pending.value) timer = setTimeout(() => { void check(); }, 0);
  }, { immediate: true });
  onBeforeUnmount(() => { disposed = true; clearTimeout(timer); });
  return { pending, checking, status, message, detail, submit, check, dismissFailed, copyInput };
}
