<template>
  <section class="panel drafts-panel">
    <div class="panel-head">
      <div>
        <h2><span class="step-badge">2</span>待发起通告</h2>
        <small>核对字段后发送，成功后进入右侧</small>
      </div>
      <div class="panel-head-status">
        <b>{{ rows.length }} 条</b>
        <em>{{ draftStepText }}</em>
      </div>
    </div>
    <div v-if="rows.length === 0" class="empty-block">
      <strong>待发起通告为空</strong>
      <p>
        {{ specialtyFilter ? "当前专业下没有待发起通告，可切换专业或选择全部。" : "从左侧选择事项，或使用纯手填、解析粘贴通告。" }}
      </p>
    </div>
    <template v-else>
      <div v-if="draftAttentionItems.length" class="draft-attention-strip" aria-label="待发起通告发送前检查">
        <span v-for="item in draftAttentionItems" :key="item.key" :class="item.tone">
          <b>{{ item.count }}</b>
          <strong>{{ item.label }}</strong>
          <em>{{ item.hint }}</em>
        </span>
      </div>
      <div ref="draftStackRef" class="draft-stack">
        <DraftNoticeCard
          v-for="row in rows"
          :key="row.key"
          :row-key="row.key"
          :record="row.record"
          :draft="row.draft"
          :title="row.title"
          :active="row.key === activeDraftKey"
          :busy="isLineBusy(row.key)"
          :meta="draftCardMeta(row.record, row.draft, row.key === activeDraftKey)"
          :summary="draftSummary(row.record, row.draft)"
          :warning-text="row.record.manual && !row.draft.validation_touched ? draftTypeConflictText(row.record, row.draft) : ''"
          :missing-text="draftMissingText(row.record, row.draft)"
          :work-type="draftWorkType(row.record, row.draft)"
          :requestable-scopes="requestableScopes"
          :maintenance-cycle-options="maintenanceCycleOptions"
          :zhihang-records="zhihangRecords"
          :upload-preview-rows="draftUploadPreviewRows(row.record, row.draft)"
          :notice-preview-text="noticePreviewText(row.record, row.draft)"
          :preview-visible="previewDraftKey === row.key"
          :type-override-visible="canToggleWorkTypeOverride(row.record)"
          :type-override-busy="typeOverrideBusyKey === row.key"
          :type-override-label="workTypeOverrideButtonLabel(row.record)"
          :sync-maintenance-visible="isConvertedMaintenanceChange(row.record, row.draft)"
          :send-label="sendDraftButtonLabel(row.record, row.draft)"
          :field-class="(field) => draftFieldClass(row.record, row.draft, field)"
          :job-text="jobText"
          :job-class="jobClass"
          :copy-text="jobCopyText(row.key, noticePreviewText(row.record, row.draft))"
          @activate="emit('activate', row.key)"
          @pin="emit('pin', row.key)"
          @remove="emit('remove', row.key)"
          @set-draft="(field, value) => emit('set-draft', row.draft, field, value)"
          @manual-type-change="emit('manual-type-change', row.draft)"
          @building-change="emit('building-change', row.draft)"
          @bind-zhihang="emit('bind-zhihang', row.draft)"
          @toggle-preview="emit('toggle-preview', row.key)"
          @copy-notice="emit('copy-notice', row.key, noticePreviewText(row.record, row.draft))"
          @send="emit('send', row.key)"
          @toggle-work-type-override="emit('toggle-work-type-override', row.record)"
        />
      </div>
    </template>
  </section>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";
import DraftNoticeCard from "./DraftNoticeCard.vue";
import type { ScopeOption } from "../types";

type Dict = Record<string, any>;
type DraftRow = { key: string; record: Dict; draft: Dict; title: string };
type PreviewRow = { label: string; value: string };
type ClassMap = Record<string, boolean>;

const props = defineProps<{
  rows: DraftRow[];
  activeDraftKey: string;
  specialtyFilter: string;
  requestableScopes: ScopeOption[];
  maintenanceCycleOptions: string[];
  zhihangRecords: Dict[];
  previewDraftKey: string;
  typeOverrideBusyKey: string;
  isLineBusy: (key: string) => boolean;
  draftCardMeta: (record: Dict, draft: Dict, active: boolean) => string;
  draftSummary: (record: Dict, draft: Dict) => string;
  draftTypeConflictText: (record: Dict, draft: Dict) => string;
  draftMissingText: (record: Dict, draft: Dict) => string;
  draftWorkType: (record: Dict, draft: Dict) => string;
  draftUploadPreviewRows: (record: Dict, draft: Dict) => PreviewRow[];
  noticePreviewText: (record: Dict, draft: Dict) => string;
  canToggleWorkTypeOverride: (record: Dict) => boolean;
  workTypeOverrideButtonLabel: (record: Dict) => string;
  isConvertedMaintenanceChange: (record: Dict, draft: Dict) => boolean;
  sendDraftButtonLabel: (record: Dict, draft: Dict) => string;
  draftFieldClass: (record: Dict, draft: Dict, field: string) => ClassMap;
  jobText: (key: string) => string;
  jobClass: (key: string) => string;
  jobCopyText: (key: string, text: string) => string;
}>();

const emit = defineEmits<{
  activate: [key: string];
  pin: [key: string];
  remove: [key: string];
  "set-draft": [draft: Dict, field: string, value: any];
  "manual-type-change": [draft: Dict];
  "building-change": [draft: Dict];
  "bind-zhihang": [draft: Dict];
  "toggle-preview": [key: string];
  "copy-notice": [key: string, text: string];
  send: [key: string];
  "toggle-work-type-override": [record: Dict];
}>();

const draftStackRef = ref<HTMLElement | null>(null);
const busyCount = computed(() => props.rows.filter((row) => props.isLineBusy(row.key)).length);
const missingCount = computed(() => props.rows.filter((row) => Boolean(props.draftMissingText(row.record, row.draft))).length);
const warningCount = computed(() => props.rows.filter((row) => (
  Boolean(row.record.manual && !row.draft.validation_touched && props.draftTypeConflictText(row.record, row.draft))
)).length);
const draftStepText = computed(() => {
  if (!props.rows.length) return "等待选择事项";
  if (busyCount.value) return "后台处理中";
  if (missingCount.value) return "先补齐字段";
  if (warningCount.value) return "先确认类型";
  return "下一步：发送";
});
const draftAttentionItems = computed(() => [
  {
    key: "busy",
    label: "处理中",
    hint: "等待后台完成",
    count: busyCount.value,
    tone: "blue",
  },
  {
    key: "missing",
    label: "缺字段",
    hint: "补齐后再发送",
    count: missingCount.value,
    tone: "amber",
  },
  {
    key: "warning",
    label: "类型待确认",
    hint: "检查通告类型",
    count: warningCount.value,
    tone: "rose",
  },
].filter((item) => item.count > 0));

function scrollToTop(): void {
  draftStackRef.value?.scrollTo({ top: 0, behavior: "smooth" });
}

defineExpose({ scrollToTop });
</script>

<style scoped>
.drafts-panel {
  position: relative;
  overflow: hidden;
  display: grid;
  align-content: start;
  gap: 14px;
  min-height: 0;
  border: 1px solid #d8e5f7;
  border-radius: 22px;
  padding: 14px;
  background: rgba(255, 255, 255, 0.82);
  box-shadow: 0 12px 30px rgba(0, 47, 135, 0.08);
  backdrop-filter: blur(10px);
}

.panel-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  min-width: 0;
  margin: -14px -14px 0;
  border-radius: 22px 22px 0 0;
  border-bottom: 1px solid #e7f0fb;
  padding: 14px 16px 10px;
  background:
    linear-gradient(180deg, rgba(255, 255, 255, 0.99), rgba(248, 251, 255, 0.98)),
    linear-gradient(90deg, rgba(48, 128, 255, 0.06), transparent 42%);
  box-shadow: 0 8px 20px rgba(22, 78, 151, 0.04);
}

.panel-head h2 {
  margin: 0;
  color: #09204a;
  font-size: 16px;
  font-weight: 900;
  letter-spacing: 0;
}

.panel-head small {
  display: block;
  margin-top: 4px;
  color: #64748b;
  font-size: 12px;
  font-weight: 800;
  line-height: 1.35;
}

.step-badge {
  display: inline-grid;
  place-items: center;
  width: 22px;
  min-width: 22px;
  height: 22px;
  margin-right: 8px;
  border-radius: 8px;
  color: #ffffff;
  font-size: 12px;
  font-weight: 950;
  vertical-align: 1px;
  background: linear-gradient(180deg, #1e63ff, #00b7d7);
  box-shadow: 0 6px 14px rgba(22, 120, 255, 0.18);
}

.panel-head-status {
  flex: 0 0 auto;
  display: grid;
  justify-items: end;
  gap: 4px;
  min-width: 0;
}

.panel-head-status b,
.panel-head-status em {
  border: 1px solid #cfe0ff;
  border-radius: 999px;
  padding: 3px 8px;
  background: rgba(239, 246, 255, 0.86);
  color: #005bff;
  font-size: 12px;
  font-weight: 900;
  font-style: normal;
  line-height: 1.2;
  white-space: nowrap;
}

.panel-head-status em {
  border-color: #d8e5f7;
  background: #ffffff;
  color: #64748b;
  font-size: 11px;
  font-weight: 850;
}

.draft-stack {
  overflow: auto;
  display: grid;
  align-content: start;
  gap: 10px;
  padding-right: 2px;
  scroll-behavior: smooth;
  scrollbar-color: #9cc7ff #eef6ff;
  scrollbar-width: thin;
}

.draft-stack::-webkit-scrollbar {
  width: 8px;
}

.draft-stack::-webkit-scrollbar-track {
  border-radius: 999px;
  background: #eef6ff;
}

.draft-stack::-webkit-scrollbar-thumb {
  border-radius: 999px;
  background: linear-gradient(180deg, #9cc7ff, #1678ff);
}

.draft-attention-strip {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 7px;
}

.draft-attention-strip span {
  display: grid;
  grid-template-columns: auto minmax(0, 1fr);
  grid-template-areas:
    "count label"
    "count hint";
  align-items: center;
  column-gap: 8px;
  min-width: 0;
  min-height: 44px;
  border: 1px solid #d8e5f7;
  border-radius: 16px;
  padding: 7px 9px;
  background: #f8fbff;
  color: #48627f;
}

.draft-attention-strip b {
  grid-area: count;
  display: grid;
  place-items: center;
  min-width: 28px;
  height: 28px;
  border-radius: 10px;
  background: #eaf3ff;
  color: #0757d7;
  font-size: 13px;
  font-weight: 950;
}

.draft-attention-strip strong,
.draft-attention-strip em {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.draft-attention-strip strong {
  grid-area: label;
  color: #0f2f6a;
  font-size: 12px;
  font-weight: 950;
}

.draft-attention-strip em {
  grid-area: hint;
  color: #64748b;
  font-size: 11px;
  font-style: normal;
  font-weight: 850;
}

.draft-attention-strip .blue {
  border-color: #bfdbfe;
  background: #eff6ff;
}

.draft-attention-strip .amber {
  border-color: #fed7aa;
  background: #fff7ed;
}

.draft-attention-strip .amber b {
  background: #ffedd5;
  color: #c2410c;
}

.draft-attention-strip .rose {
  border-color: #fecaca;
  background: #fff1f2;
}

.draft-attention-strip .rose b {
  background: #ffe4e6;
  color: #be123c;
}

.empty-block {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 6px;
  min-height: 140px;
  border-radius: 22px;
  padding: 18px;
  background: rgba(248, 251, 255, 0.94);
  color: #64748b;
  text-align: center;
  line-height: 1.7;
}

.empty-block strong {
  color: #0f2f6a;
  font-size: 14px;
  font-weight: 950;
}

.empty-block p {
  max-width: 300px;
  margin: 0;
  color: #64748b;
  font-size: 12px;
  font-weight: 850;
  line-height: 1.6;
}

@media (max-width: 760px) {
  .draft-attention-strip {
    grid-template-columns: 1fr;
  }
}
</style>
