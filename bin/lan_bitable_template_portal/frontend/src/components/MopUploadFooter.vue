<template>
  <div class="floating-upload-box" :class="{ expanded: detailsOpen }">
    <div class="floating-upload-compact">
      <div class="floating-upload-title">
        <strong>维护单上传</strong>
        <small :class="{ ready: allReady }">{{ footerStateLabel }}</small>
      </div>
      <div class="floating-upload-summary">
        <span :class="{ ready: timeReady }">
          <b>{{ timeReady ? "完成" : "待补" }}</b>
          <em>时间</em>
          <strong>{{ timeReadyCount }}/{{ timeItems.length || 0 }}</strong>
        </span>
        <span :class="{ ready: personReady }">
          <b>{{ personReady ? "完成" : "待补" }}</b>
          <em>签名</em>
          <strong>{{ personReadyCount }}/{{ personItems.length || 0 }}</strong>
        </span>
      </div>
      <button
        v-if="items.length"
        class="detail-toggle"
        type="button"
        :aria-expanded="detailsOpen"
        @click="detailsOpen = !detailsOpen"
      >
        {{ detailsOpen ? "收起" : missingCount ? `缺项 ${missingCount}` : "详情" }}
      </button>
    </div>

    <p v-if="uploadReadinessHint" class="floating-upload-hint" :class="{ ready: allReady }">
      {{ uploadReadinessHint }}
    </p>

    <div v-if="detailsOpen || missingCount" class="floating-upload-checks" :class="{ compact: !detailsOpen && Boolean(missingCount) }">
      <div v-if="timeItems.length" class="floating-upload-group time-group">
        <span class="floating-upload-group-label">时间</span>
        <div class="floating-upload-signature-counts">
          <span
            v-for="item in visibleTimeItems"
            :key="item.key"
            :class="{ ready: item.ready }"
            :title="`${item.label}：${item.text}`"
          >
            <b>{{ item.ready ? "完成" : "待补" }}</b>
            <em>{{ item.label }}</em>
            <strong>{{ item.text }}</strong>
          </span>
        </div>
      </div>
      <div v-if="personItems.length" class="floating-upload-group person-group">
        <span class="floating-upload-group-label">人员</span>
        <div class="floating-upload-signature-counts">
          <span
            v-for="item in visiblePersonItems"
            :key="item.key"
            :class="{ ready: item.ready }"
            :title="`${item.label}：${item.text}`"
          >
            <b>{{ item.ready ? "完成" : "待补" }}</b>
            <em>{{ item.label }}</em>
            <strong>{{ item.text }}</strong>
          </span>
        </div>
      </div>
    </div>
    <div class="floating-upload-action-row">
      <span v-if="uploadedAtText" class="floating-uploaded-at">
        {{ uploadedAtText }}
      </span>
      <button
        class="btn blue floating-upload-signed-mop"
        type="button"
        :disabled="disabled"
        :title="disabledReason"
        @click="emit('upload')"
      >
        {{ uploadButtonText }}
      </button>
    </div>
    <DisabledReason
      v-if="disabledReason && !saving"
      :text="disabledReason"
      tone="warning"
    />
  </div>
</template>

<script setup lang="ts">
import { computed, ref, watch } from "vue";
import DisabledReason from "./DisabledReason.vue";

const props = defineProps<{
  items: Array<{ key: string; label: string; text: string; ready: boolean }>;
  uploadedAtText?: string;
  saving?: boolean;
  disabled?: boolean;
  disabledReason?: string;
}>();

const readyCount = computed(() => props.items.filter((item) => item.ready).length);
const allReady = computed(() => props.items.length > 0 && readyCount.value >= props.items.length);
const timeItems = computed(() => props.items.filter((item) => /时间/.test(String(item.label || ""))));
const personItems = computed(() => {
  const timeKeys = new Set(timeItems.value.map((item) => item.key));
  return props.items.filter((item) => !timeKeys.has(item.key));
});
const detailsOpen = ref(false);
const missingCount = computed(() => Math.max(0, props.items.length - readyCount.value));
const timeReadyCount = computed(() => timeItems.value.filter((item) => item.ready).length);
const personReadyCount = computed(() => personItems.value.filter((item) => item.ready).length);
const missingItems = computed(() => props.items.filter((item) => !item.ready));
const visibleTimeItems = computed(() => (
  detailsOpen.value ? timeItems.value : timeItems.value.filter((item) => !item.ready)
));
const visiblePersonItems = computed(() => (
  detailsOpen.value ? personItems.value : personItems.value.filter((item) => !item.ready)
));
const timeReady = computed(() => timeItems.value.length > 0 && timeReadyCount.value >= timeItems.value.length);
const personReady = computed(() => personItems.value.length > 0 && personReadyCount.value >= personItems.value.length);
const footerStateLabel = computed(() => {
  if (allReady.value) return "已满足";
  if (missingCount.value > 0) return `缺 ${missingCount.value} 项`;
  return "待检查";
});
const uploadButtonText = computed(() => {
  if (props.saving) return "上传中";
  if (!props.disabled) return "上传已签名MOP";
  if (missingCount.value > 0) return "补齐后上传";
  return "暂不能上传";
});
const uploadReadinessHint = computed(() => {
  if (props.saving) return "正在上传维护单，请不要重复点击。";
  if (allReady.value) return props.uploadedAtText ? "已满足，可覆盖上传最新维护单。" : "已满足，可以上传维护单。";
  if (missingCount.value > 0) return `还差：${missingItems.value.map((item) => `${item.label}${item.text && item.text !== "未填" ? ` ${item.text}` : ""}`).join("、")}`;
  return "";
});

watch(allReady, (value) => {
  if (value) {
    detailsOpen.value = false;
  }
});

watch(missingCount, (count) => {
  if (count === 0) {
    detailsOpen.value = false;
  }
}, { immediate: true });

const emit = defineEmits<{
  upload: [];
}>();
</script>

<style scoped>
.floating-upload-box {
  position: static;
  z-index: auto;
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(168px, auto);
  align-items: center;
  gap: 6px 8px;
  width: 100%;
  max-height: none;
  overflow: visible;
  margin: 16px 0 0;
  padding: 8px 10px;
  border: 1px solid rgba(191, 219, 254, 0.88);
  border-radius: 16px;
  background:
    linear-gradient(135deg, rgba(255, 255, 255, 0.97), rgba(248, 251, 255, 0.92)),
    #fff;
  box-shadow:
    0 8px 22px rgba(15, 73, 153, 0.08),
    0 0 0 1px rgba(255, 255, 255, 0.72) inset;
}

.floating-upload-box.expanded {
  width: 100%;
}

.floating-upload-compact {
  display: grid;
  grid-template-columns: auto minmax(0, 1fr) auto;
  align-items: center;
  gap: 7px;
  min-width: 0;
}

.floating-upload-checks {
  grid-column: 1 / -1;
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  align-items: start;
  gap: 7px;
  min-width: 0;
  padding-top: 6px;
  border-top: 1px solid rgba(216, 229, 247, 0.86);
}

.floating-upload-checks.compact {
  gap: 6px;
  padding-top: 5px;
}

.floating-upload-hint {
  grid-column: 1 / -1;
  margin: -2px 0 0;
  border: 1px solid #fed7aa;
  border-radius: 14px;
  padding: 7px 10px;
  background: #fff7ed;
  color: #9a3412;
  font-size: 12px;
  font-weight: 900;
  line-height: 1.35;
  text-align: center;
}

.floating-upload-hint.ready {
  border-color: #bbf7d0;
  background: #ecfdf5;
  color: #047857;
}

.floating-upload-title {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  min-height: 24px;
  min-width: 0;
  color: #0f2f6a;
  font-size: 11px;
  font-weight: 950;
  white-space: nowrap;
}

.floating-upload-title small {
  border: 1px solid #fed7aa;
  border-radius: 999px;
  padding: 3px 7px;
  background: #fff7ed;
  color: #9a3412;
  font-size: 11px;
  font-weight: 950;
}

.floating-upload-title small.ready {
  border-color: #bbf7d0;
  background: #ecfdf5;
  color: #047857;
}

.floating-upload-summary {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 6px;
  min-width: 0;
}

.floating-upload-summary span {
  display: inline-grid;
  grid-template-columns: auto auto minmax(36px, auto);
  align-items: center;
  gap: 5px;
  min-height: 22px;
  max-width: min(100%, 188px);
  border: 1px solid #fed7aa;
  border-radius: 999px;
  padding: 2px 6px;
  background: #fff7ed;
  color: #9a3412;
  font-size: 11px;
  font-weight: 900;
  white-space: normal;
}

.floating-upload-summary span.ready {
  border-color: #bbf7d0;
  background: #ecfdf5;
  color: #047857;
}

.floating-upload-summary b {
  border-radius: 999px;
  padding: 2px 5px;
  background: rgba(255, 255, 255, 0.72);
  font-size: 10px;
  font-weight: 950;
  line-height: 1;
}

.floating-upload-summary em,
.floating-upload-summary strong {
  min-width: 0;
  font-style: normal;
  line-height: 1.15;
}

.floating-upload-summary em {
  font-weight: 950;
}

.floating-upload-summary strong {
  overflow: hidden;
  font-size: 11px;
  font-weight: 950;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.detail-toggle {
  min-height: 26px;
  border: 1px solid #cfe0ff;
  border-radius: 999px;
  padding: 0 9px;
  background: #eff6ff;
  color: #0757d7;
  font-size: 12px;
  font-weight: 950;
  cursor: pointer;
  white-space: nowrap;
}

.detail-toggle:hover {
  border-color: #8dbbfb;
  background: #dbeafe;
}

.floating-upload-group {
  display: grid;
  grid-template-columns: 42px minmax(0, 1fr);
  align-items: start;
  gap: 6px;
  min-width: 0;
}

.floating-upload-group-label {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-height: 27px;
  border: 1px solid #d8e5f7;
  border-radius: 999px;
  padding: 0 8px;
  background: #f7fbff;
  color: #48627f;
  font-size: 11px;
  font-weight: 950;
  white-space: nowrap;
}

.time-group .floating-upload-group-label {
  border-color: #cfe0ff;
  background: #eff6ff;
  color: #0757d7;
}

.person-group {
  grid-column: auto;
}

.person-group .floating-upload-group-label {
  border-color: #bbf7d0;
  background: #ecfdf5;
  color: #047857;
}

.floating-upload-signature-counts {
  display: flex;
  flex-wrap: wrap;
  gap: 5px;
  align-items: center;
  min-width: 0;
  max-height: 62px;
  overflow: auto;
  padding: 1px 2px 3px 0;
  scrollbar-width: thin;
}

.floating-upload-checks.compact .floating-upload-signature-counts {
  max-height: 74px;
}

.floating-upload-signature-counts span {
  display: grid;
  grid-template-columns: auto minmax(52px, auto) minmax(0, 1fr);
  align-items: center;
  gap: 5px;
  box-sizing: border-box;
  flex: 1 1 190px;
  min-width: 180px;
  max-width: 360px;
  min-height: 28px;
  border: 1px solid #fed7aa;
  border-radius: 14px;
  padding: 4px 7px;
  color: #9a3412;
  background: #fff7ed;
  font-size: 11px;
  font-weight: 850;
  line-height: 1.3;
  text-align: left;
  white-space: normal;
  overflow: hidden;
  text-overflow: ellipsis;
  overflow-wrap: anywhere;
  word-break: break-word;
}

.floating-upload-signature-counts span b {
  border-radius: 999px;
  padding: 3px 6px;
  background: rgba(255, 255, 255, 0.72);
  font-size: 10px;
  font-weight: 950;
  line-height: 1;
  white-space: nowrap;
}

.floating-upload-signature-counts span em,
.floating-upload-signature-counts span strong {
  min-width: 0;
  overflow: hidden;
  font-style: normal;
  line-height: 1.2;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.floating-upload-signature-counts span em {
  font-weight: 900;
}

.floating-upload-signature-counts span strong {
  font-weight: 850;
  white-space: normal;
  display: -webkit-box;
  -webkit-box-orient: vertical;
  -webkit-line-clamp: 2;
  overflow-wrap: anywhere;
}

.floating-upload-signature-counts span.ready {
  border-color: #bbf7d0;
  color: #047857;
  background: #ecfdf5;
}

.floating-upload-action-row {
  grid-column: 2;
  grid-row: 1;
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  justify-content: flex-end;
  gap: 6px;
  min-width: 0;
  align-self: stretch;
}

.floating-uploaded-at {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  flex: 1 1 180px;
  min-height: 26px;
  min-width: 0;
  max-width: 230px;
  border: 1px solid #bbf7d0;
  border-radius: 999px;
  padding: 4px 10px;
  color: #047857;
  background: rgba(236, 253, 245, 0.96);
  font-size: 12px;
  font-weight: 900;
  line-height: 1.25;
  overflow: hidden;
  text-overflow: ellipsis;
  text-align: center;
  white-space: nowrap;
  box-shadow: 0 12px 28px rgba(4, 120, 87, 0.14);
}

.floating-upload-signed-mop {
  min-width: 152px;
  min-height: 36px;
  border-radius: 13px;
  padding: 0 16px;
  font-size: 13px;
  font-weight: 950;
  white-space: nowrap;
  box-shadow: 0 18px 36px rgba(30, 99, 255, 0.24);
}

.floating-upload-signed-mop:disabled {
  border-color: #cbd5e1;
  background: linear-gradient(135deg, #f8fafc, #eef2f7);
  color: #64748b;
  box-shadow: 0 12px 28px rgba(100, 116, 139, 0.16);
}

.floating-upload-box :deep(.control-disabled-reason) {
  grid-column: 1 / -1;
  margin: 0;
  width: 100%;
  align-items: flex-start;
  justify-content: flex-start;
  text-align: left;
  border-radius: 12px;
  padding: 6px 8px;
}

@media (max-width: 1180px) {
  .floating-upload-box {
    width: min(680px, 100%);
    grid-template-columns: 1fr;
    justify-items: stretch;
    padding: 9px;
  }

  .floating-upload-signature-counts {
    max-height: 70px;
    overflow: auto;
  }

  .floating-upload-checks {
    grid-template-columns: 1fr;
  }

  .floating-upload-compact {
    grid-template-columns: 1fr;
    align-items: stretch;
  }

  .detail-toggle {
    justify-self: start;
  }

  .floating-upload-action-row {
    grid-column: auto;
    grid-row: auto;
    justify-content: space-between;
    width: 100%;
  }

  .floating-uploaded-at {
    max-width: none;
    width: 100%;
  }

  .floating-upload-signed-mop {
    flex: 1 1 220px;
  }
}

@media (max-width: 680px) {
  .floating-upload-box {
    width: 100%;
  }

  .floating-upload-signature-counts span {
    min-width: min(144px, 100%);
    max-width: none;
  }

  .floating-upload-action-row {
    display: grid;
    grid-template-columns: 1fr;
  }

  .floating-upload-signed-mop {
    width: 100%;
    min-width: 0;
  }
}
</style>
