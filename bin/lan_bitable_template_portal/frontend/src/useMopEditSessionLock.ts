import { ref } from "vue";
import type { Dict } from "./api/client";

type RefLike<T> = {
  readonly value: T;
};

type MopEditSessionLockOptions = {
  scope: RefLike<string>;
  selectedNotice: RefLike<Dict | null>;
  selectedMop: RefLike<Dict | null>;
  selectedAttachment: RefLike<Dict | null>;
  selectedNoticeSourceRecordId: RefLike<string>;
  selectedNoticeKey: RefLike<string>;
  selectedMopRecordId: RefLike<string>;
  selectedAttachmentToken: RefLike<string>;
  warnings: RefLike<string[]> & { value: string[] };
};

const editSessionTtlMs = 30 * 60 * 1000;

export function useMopEditSessionLock(options: MopEditSessionLockOptions) {
  const instanceId = `mop-${Date.now()}-${Math.random().toString(36).slice(2)}`;
  const activeStorageKey = ref("");

  function storageKey(): string {
    const noticeId = options.selectedNoticeSourceRecordId.value
      || options.selectedNotice.value?.notice_key
      || options.selectedNoticeKey.value
      || "notice";
    const mopId = options.selectedMop.value?.record_id || options.selectedMopRecordId.value || "mop";
    const attachmentId = options.selectedAttachmentToken.value || "attachment";
    return `clipflow:mop-edit:${options.scope.value}:${noticeId}:${mopId}:${attachmentId}`;
  }

  function release(): void {
    const key = activeStorageKey.value;
    if (!key) return;
    try {
      const existing = JSON.parse(window.localStorage.getItem(key) || "{}");
      if (existing?.instance_id === instanceId) {
        window.localStorage.removeItem(key);
      }
    } catch {
      window.localStorage.removeItem(key);
    }
    activeStorageKey.value = "";
  }

  function claim(): void {
    if (!options.selectedNotice.value || !options.selectedMop.value || !options.selectedAttachment.value) return;
    const key = storageKey();
    const now = Date.now();
    try {
      const existing = JSON.parse(window.localStorage.getItem(key) || "{}");
      const existingAge = now - Number(existing?.updated_at || 0);
      if (
        existing?.instance_id
        && existing.instance_id !== instanceId
        && existingAge > 0
        && existingAge < editSessionTtlMs
      ) {
        options.warnings.value = [...new Set([
          ...options.warnings.value,
          "同一个 MOP 填写页可能已在其他标签页打开，请避免两边同时修改后互相覆盖。",
        ])];
      }
      window.localStorage.setItem(key, JSON.stringify({
        instance_id: instanceId,
        updated_at: now,
        title: options.selectedNotice.value.title || "",
        mop_title: options.selectedMop.value.title || "",
      }));
      activeStorageKey.value = key;
    } catch {
      // localStorage may be disabled by browser policy; editing can continue.
    }
  }

  return {
    activeStorageKey,
    claim,
    release,
  };
}
