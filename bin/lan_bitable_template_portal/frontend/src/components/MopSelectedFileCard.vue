<template>
  <section class="mop-selected-file-card">
    <div class="selected-file-main">
      <span>已选 MOP 表格</span>
      <strong>{{ title }}</strong>
      <small>{{ attachmentLabel }}</small>
      <small class="attachment-count">{{ attachmentCountText }}</small>
      <em v-if="bindingStatus" class="bind-status success">{{ bindingStatus }}</em>
      <em v-else-if="bindingError" class="bind-status failed">{{ bindingError }}</em>
    </div>

    <label v-if="attachments.length" class="attachment-select">
      <span>表格附件</span>
      <select :value="selectedAttachmentToken" @change="emitAttachmentChange">
        <option
          v-for="attachment in attachments"
          :key="attachmentKey(attachment)"
          :value="attachmentKey(attachment)"
        >
          {{ attachment.name || "MOP表格" }}
        </option>
      </select>
    </label>
    <div v-else class="attachment-empty">
      该 MOP 记录暂未识别到 xlsx/csv 附件
    </div>

    <div class="selected-file-actions">
      <small class="open-hint">{{ canPreview ? "点击后自动绑定，并进入表格填写页" : "请先选择可预览附件" }}</small>
      <button
        class="btn blue"
        type="button"
        :disabled="!canPreview || busy"
        :title="disabledReason"
        @click="$emit('open')"
      >
        {{ buttonText }}
      </button>
      <DisabledReason v-if="disabledReason && !busy" :text="disabledReason" />
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed } from "vue";
import type { Dict } from "../api/client";
import DisabledReason from "./DisabledReason.vue";

const props = defineProps<{
  selectedMop: Dict;
  attachments: Dict[];
  selectedAttachment: Dict | null;
  selectedAttachmentToken: string;
  bindingStatus: string;
  bindingError: string;
  canPreview: boolean;
  busy: boolean;
  disabledReason: string;
  buttonText: string;
}>();

const emit = defineEmits<{
  open: [];
  "update:selectedAttachmentToken": [value: string];
}>();

const title = computed(() => String(props.selectedMop.title || "未命名 MOP"));

const attachmentLabel = computed(() => {
  if (props.selectedAttachment?.name) return String(props.selectedAttachment.name);
  if (props.attachments.length) return "请选择表格附件";
  return "暂无可预览附件";
});

const attachmentCountText = computed(() => {
  const count = props.attachments.length;
  if (!count) return "未识别到表格附件";
  return props.selectedAttachment?.name ? `附件 ${count} 个 · 已选` : `附件 ${count} 个`;
});

function attachmentKey(attachment: Dict): string {
  return String(attachment.file_token || attachment.url || attachment.name || "");
}

function emitAttachmentChange(event: Event): void {
  emit("update:selectedAttachmentToken", (event.target as HTMLSelectElement).value);
}
</script>

<style scoped>
.mop-selected-file-card {
  display: grid;
  grid-template-columns: minmax(0, 1.2fr) minmax(210px, 0.72fr);
  gap: 10px 12px;
  align-items: stretch;
  padding: 12px;
  border: 1px solid rgba(191, 219, 254, 0.88);
  border-radius: 18px;
  background:
    linear-gradient(135deg, rgba(255, 255, 255, 0.96), rgba(239, 246, 255, 0.82)),
    #fff;
  box-shadow: 0 14px 34px rgba(37, 99, 235, 0.1);
}

.selected-file-main,
.attachment-select,
.selected-file-actions {
  min-width: 0;
}

.selected-file-main {
  display: grid;
  gap: 3px;
  align-content: center;
  padding-right: 4px;
}

.selected-file-main span,
.attachment-select span {
  color: #64748b;
  font-size: 12px;
  font-weight: 900;
}

.selected-file-main strong {
  display: -webkit-box;
  overflow: hidden;
  color: #0f172a;
  font-size: 14px;
  font-weight: 950;
  text-overflow: ellipsis;
  white-space: normal;
  -webkit-box-orient: vertical;
  -webkit-line-clamp: 2;
}

.selected-file-main small {
  display: -webkit-box;
  overflow: hidden;
  color: #475569;
  font-size: 12px;
  text-overflow: ellipsis;
  white-space: normal;
  -webkit-box-orient: vertical;
  -webkit-line-clamp: 2;
}

.attachment-count {
  width: fit-content;
  border: 1px solid #cfe0ff;
  border-radius: 999px;
  background: #eff6ff;
  padding: 3px 7px;
  color: #0757d7 !important;
  font-weight: 900;
}

.bind-status {
  width: fit-content;
  padding: 4px 9px;
  border-radius: 999px;
  font-size: 12px;
  font-style: normal;
  font-weight: 900;
}

.bind-status.success {
  background: rgba(220, 252, 231, 0.9);
  color: #047857;
}

.bind-status.failed {
  background: rgba(254, 226, 226, 0.9);
  color: #b91c1c;
}

.attachment-select {
  display: grid;
  gap: 6px;
  align-content: center;
}

.attachment-select select {
  width: 100%;
  min-height: 38px;
  border: 1px solid rgba(203, 213, 225, 0.95);
  border-radius: 14px;
  background: rgba(255, 255, 255, 0.9);
  color: #0f172a;
  font-weight: 800;
}

.attachment-empty {
  padding: 10px 12px;
  border: 1px solid rgba(245, 158, 11, 0.3);
  border-radius: 14px;
  background: rgba(255, 251, 235, 0.92);
  color: #92400e;
  font-size: 12px;
  font-weight: 800;
}

.selected-file-actions {
  grid-column: 1 / -1;
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  min-width: 0;
  border-top: 1px solid rgba(216, 229, 247, 0.9);
  padding-top: 10px;
}

.open-hint {
  min-width: 0;
  color: #48627f;
  font-size: 12px;
  font-weight: 900;
  text-align: left;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.btn {
  border: none;
  border-radius: 15px;
  padding: 9px 16px;
  min-height: 40px;
  color: #fff;
  background: linear-gradient(135deg, #1e63ff, #1554df);
  box-shadow: 0 12px 24px rgba(37, 99, 235, 0.18);
  font-weight: 900;
  cursor: pointer;
  white-space: nowrap;
}

.btn:disabled {
  opacity: 0.56;
  cursor: not-allowed;
  box-shadow: none;
}

@media (max-width: 980px) {
  .mop-selected-file-card {
    grid-template-columns: 1fr;
  }

  .selected-file-actions {
    display: grid;
    grid-template-columns: 1fr;
    justify-items: stretch;
  }

  .open-hint {
    text-align: center;
    white-space: normal;
  }
}
</style>
