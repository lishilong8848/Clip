<template>
  <div class="sign-actions">
    <span class="sign-status" :class="{ failed: messageType === 'failed', success: messageType === 'success' }" aria-live="polite">
      {{ message || roleHint }}
    </span>
    <div class="file-action-group">
      <div>
        <button type="button"
          class="btn ghost local-fill"
          :disabled="fillSaving || uploadSaving || Boolean(fillDisabledReason)"
          :title="fillDisabledReason"
          @click="emit('fill')"
        >
          {{ fillSaving ? "写入中" : "签名写入MOP" }}
        </button>
        <button type="button"
          v-if="filledMopAvailable"
          class="btn ghost reset-clean"
          :disabled="resetSaving || fillSaving || uploadSaving"
          title="删除当前已签名文件，并重新下载干净 MOP"
          @click="emit('reset')"
        >
          {{ resetSaving ? "下载中" : "重新下载干净MOP" }}
        </button>
      </div>
      <small v-if="fillDisabledReason" class="disabled-hint">{{ fillDisabledReason }}</small>
      <small v-else-if="filledMopAvailable" class="warning-hint">已生成</small>
    </div>
  </div>
</template>

<script setup lang="ts">
defineProps<{
  message: string;
  messageType: string;
  roleHint: string;
  fillSaving: boolean;
  uploadSaving: boolean;
  resetSaving: boolean;
  fillDisabledReason: string;
  filledMopAvailable: boolean;
}>();

const emit = defineEmits<{
  fill: [];
  reset: [];
}>();
</script>

<style scoped>
.sign-actions {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  align-items: center;
  gap: 8px;
}

.sign-status {
  display: -webkit-box;
  overflow: hidden;
  border: 1px solid rgba(216, 229, 247, 0.9);
  border-radius: 10px;
  background: rgba(248, 251, 255, 0.9);
  padding: 5px 8px;
  color: #475569;
  font-size: 11px;
  font-weight: 800;
  line-height: 1.35;
  text-overflow: ellipsis;
  white-space: normal;
  -webkit-box-orient: vertical;
  -webkit-line-clamp: 2;
}

.sign-status.success {
  border-color: #bbf7d0;
  background: #ecfdf5;
  color: #047857;
}

.sign-status.failed {
  border-color: #fed7aa;
  background: #fff7ed;
  color: #b45309;
}

.file-action-group {
  display: grid;
  grid-template-columns: minmax(0, 1fr);
  align-items: center;
  gap: 4px;
}

.file-action-group > div {
  display: flex;
  flex-wrap: wrap;
  gap: 4px;
  align-items: center;
  justify-content: flex-end;
  min-width: 0;
}

.file-action-group small {
  justify-self: end;
  max-width: 320px;
  color: #64748b;
  font-size: 10px;
  font-weight: 850;
  line-height: 1.35;
}

.btn {
  min-height: 28px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  border: 1px solid #d8e5f7;
  border-radius: 999px;
  padding: 0 9px;
  background: rgba(255, 255, 255, 0.94);
  color: #1d4ed8;
  font: inherit;
  font-size: 11px;
  font-weight: 900;
  line-height: 1;
  cursor: pointer;
  box-shadow: 0 8px 18px rgba(37, 99, 235, 0.06);
}

.btn:disabled {
  cursor: not-allowed;
  opacity: 0.58;
}

.local-fill {
  border-color: #1e63ff;
  color: #ffffff;
  background: linear-gradient(135deg, #1e63ff, #005bd8);
  box-shadow: 0 12px 22px rgba(30, 99, 255, 0.16);
  min-width: 130px;
  min-height: 34px;
}

.reset-clean {
  border-color: #fde68a;
  color: #92400e;
  background: #fffbeb;
}

.warning-hint {
  color: #b45309 !important;
}

.disabled-hint {
  display: inline-flex;
  align-items: center;
  width: fit-content;
  max-width: 100%;
  border: 1px solid #fed7aa;
  border-radius: 999px;
  padding: 5px 10px;
  color: #9a3412 !important;
  background: #fff7ed;
  font-weight: 900;
}

@media (max-width: 720px) {
  .sign-actions,
  .file-action-group {
    grid-template-columns: 1fr;
  }

  .file-action-group > div {
    align-items: stretch;
    justify-content: stretch;
  }

  .btn,
  .file-action-group > div {
    width: 100%;
  }

  .btn {
    min-height: 44px;
  }
}
</style>
