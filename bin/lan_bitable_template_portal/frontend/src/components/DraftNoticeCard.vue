<template>
  <article
    class="draft-card"
    :class="{ active, collapsed: !active, busy }"
    @click="emit('activate')"
  >
    <div class="card-title">
      <strong>{{ title }}</strong>
      <span>{{ meta }}</span>
    </div>

    <div v-if="!active" class="draft-compact">
      <p>{{ summary || "已加入待发起通告，点击展开编辑。" }}</p>
      <div class="card-actions compact-actions">
        <span class="job-line" :class="jobClass(rowKey)">{{ jobText(rowKey) }}</span>
        <button v-if="copyText" class="btn ghost" type="button" @click.stop="emit('copy-notice')">复制通告</button>
        <button class="btn ghost" :disabled="busy" :title="busy ? '当前通告正在处理，请等待任务完成。' : '展开这条通告并编辑字段'" @click.stop="emit('pin')">展开编辑</button>
        <button class="btn ghost danger-lite" :disabled="busy" :title="busy ? '当前通告正在处理，请等待任务完成。' : '从待发起通告中移除'" @click.stop="emit('remove')">移除</button>
      </div>
      <DisabledReason
        v-if="busy"
        text="当前通告正在处理，请等待任务完成。"
        tone="warning"
      />
    </div>

    <template v-else>
      <p v-if="record.manual && draft.prefilled_from_last" class="draft-hint">
        已带入上次{{ workTypeLabel(workType) }}手填内容，请核对时间和字段后再发送。
      </p>
      <p v-if="warningText" class="draft-warning-line">{{ warningText }}</p>
      <p v-if="missingText" class="draft-required-line">{{ missingText }}</p>
      <div class="draft-next-action-hint" :class="draftNextActionHint.tone" aria-live="polite">
        <strong>{{ draftNextActionHint.title }}</strong>
        <span>{{ draftNextActionHint.text }}</span>
      </div>
      <NoticeCompletionPanel :items="completionItems" />

      <div v-if="record.manual" class="form-grid manual-meta-grid">
        <label v-if="record.manual">
          通告类型
          <select :value="draft.work_type" @change="changeManualType(($event.target as HTMLSelectElement).value)">
            <option v-for="type in workTypes" :key="type.value" :value="type.value">{{ type.label }}</option>
          </select>
        </label>
        <label v-if="record.manual && workType === 'power'">
          上/下电类型
          <select :value="draft.notice_type || '上电通告'" @change="setDraft('notice_type', ($event.target as HTMLSelectElement).value)">
            <option value="上电通告">上电通告</option>
            <option value="下电通告">下电通告</option>
          </select>
        </label>
        <label v-if="record.manual" :class="fieldClass('building')">
          楼栋/范围
          <select :value="draft.building" @change="changeBuilding(($event.target as HTMLSelectElement).value)">
            <option value="">请选择</option>
            <option v-for="item in requestableScopes" :key="item.value" :value="item.label">
              {{ item.label }}
            </option>
          </select>
        </label>
      </div>

      <NoticeMessageFields
        :work-type="workType"
        :draft="draft"
        :list-id-prefix="rowKey"
        :disabled="busy"
        :field-class="fieldClass"
        @set-field="setDraft"
      />

      <NoticeUploadFields
        :work-type="workType"
        :draft="draft"
        :line-key="rowKey"
        :maintenance-cycle-options="maintenanceCycleOptions"
        :zhihang-records="zhihangRecords"
        :sync-maintenance-visible="syncMaintenanceVisible"
        :show-non-plan="Boolean(record.manual)"
        :maintenance-cycle-label="record.manual ? '维护周期' : '维保周期'"
        :field-class="fieldClass"
        @set-field="setDraft"
        @bind-zhihang="emit('bind-zhihang', $event)"
      />

      <details class="upload-fields upload-preview">
        <summary>多维上传预览</summary>
        <dl class="upload-preview-grid">
          <template v-for="item in uploadPreviewRows" :key="item.label">
            <dt>{{ item.label }}</dt>
            <dd>{{ item.value }}</dd>
          </template>
        </dl>
      </details>

      <pre v-if="previewVisible" class="notice-preview">{{ noticePreviewText }}</pre>

      <div class="card-actions draft-action-clusters">
        <span class="job-line" :class="jobClass(rowKey)">{{ jobText(rowKey) }}</span>
        <DisabledReason
          v-if="busy"
          text="当前通告正在处理，请等待任务完成。"
          tone="warning"
        />
        <div class="action-group primary-action">
          <strong>发送</strong>
          <DisabledReason
            v-if="sendDisabledReason"
            :text="sendDisabledReason"
            tone="warning"
          />
          <button
            class="btn blue"
            :disabled="Boolean(sendDisabledReason)"
            :title="sendDisabledReason"
            @click.stop="emit('send')"
          >
            {{ sendLabel }}
          </button>
        </div>
        <div class="action-group support-actions">
          <strong>辅助操作</strong>
          <button v-if="copyText" class="btn ghost" type="button" @click.stop="emit('copy-notice')">复制通告</button>
          <button
            v-if="typeOverrideVisible"
            class="btn ghost"
            :disabled="busy || typeOverrideBusy"
            @click.stop="emit('toggle-work-type-override')"
          >
            {{ typeOverrideBusy ? "切换中" : typeOverrideLabel }}
          </button>
          <button class="btn ghost" :disabled="busy" @click.stop="emit('toggle-preview')">
            {{ previewVisible ? "收起预览" : "预览飞书文本" }}
          </button>
          <button class="btn ghost danger-lite" :disabled="busy" @click.stop="emit('remove')">移除</button>
        </div>
      </div>
    </template>
  </article>
</template>

<script setup lang="ts">
import { computed } from "vue";
import {
  isNoticeUploadField,
  noticeTemplate,
  workTypeLabel,
  workTypes,
} from "../noticeTemplates";
import DisabledReason from "./DisabledReason.vue";
import NoticeCompletionPanel from "./NoticeCompletionPanel.vue";
import NoticeMessageFields from "./NoticeMessageFields.vue";
import NoticeUploadFields from "./NoticeUploadFields.vue";

type Dict = Record<string, any>;
type ScopeOption = { value: string; label: string };
type PreviewRow = { label: string; value: string };
type ClassMap = Record<string, boolean>;

const props = defineProps<{
  rowKey: string;
  record: Dict;
  draft: Dict;
  title: string;
  active: boolean;
  busy: boolean;
  meta: string;
  summary: string;
  warningText: string;
  missingText: string;
  workType: string;
  requestableScopes: ScopeOption[];
  maintenanceCycleOptions: string[];
  zhihangRecords: Dict[];
  uploadPreviewRows: PreviewRow[];
  noticePreviewText: string;
  previewVisible: boolean;
  typeOverrideVisible: boolean;
  typeOverrideBusy: boolean;
  typeOverrideLabel: string;
  syncMaintenanceVisible: boolean;
  sendLabel: string;
  fieldClass: (field: string) => ClassMap;
  jobText: (key: string) => string;
  jobClass: (key: string) => string;
  copyText: string;
}>();

const emit = defineEmits<{
  activate: [];
  pin: [];
  remove: [];
  "set-draft": [key: string, value: any];
  "manual-type-change": [];
  "building-change": [];
  "bind-zhihang": [recordId: string];
  "toggle-preview": [];
  "copy-notice": [];
  send: [];
  "toggle-work-type-override": [];
}>();

function setDraft(key: string, value: any): void {
  emit("set-draft", key, value);
}

function changeManualType(value: string): void {
  setDraft("work_type", value);
  emit("manual-type-change");
}

function changeBuilding(value: string): void {
  setDraft("building", value);
  emit("building-change");
}

const messageFieldStatus = computed(() => {
  const fields = noticeTemplate(props.workType).messageFields;
  const total = fields.length;
  const done = fields.filter((field) => hasValue(props.draft[field] ?? props.record[field])).length;
  return { total, done };
});
const uploadFieldStatus = computed(() => {
  const fields = noticeTemplate(props.workType).uploadFields.filter((field) => field !== "non_plan" && field !== "zhihang");
  const total = fields.length;
  const done = fields.filter((field) => hasValue(props.draft[field] ?? props.record[field])).length;
  return { total, done };
});
const zhihangStatus = computed(() => {
  if (!isNoticeUploadField(props.workType, "zhihang")) return { done: true, text: "不涉及" };
  const involved = Boolean(props.draft.zhihang_involved || props.record.zhihang_involved);
  if (!involved) return { done: true, text: "未涉及" };
  const selected = hasValue(props.draft.zhihang_record_id || props.record.zhihang_record_id);
  return { done: selected, text: selected ? "已绑定" : "待选择" };
});
const completionItems = computed(() => {
  const uploadDone = uploadFieldStatus.value.total === 0 || uploadFieldStatus.value.done >= uploadFieldStatus.value.total;
  return [
    {
      key: "message",
      label: "通告字段",
      text: `${messageFieldStatus.value.done}/${messageFieldStatus.value.total}`,
      done: messageFieldStatus.value.done >= messageFieldStatus.value.total,
    },
    {
      key: "upload",
      label: "上传字段",
      text: uploadFieldStatus.value.total ? `${uploadFieldStatus.value.done}/${uploadFieldStatus.value.total}` : "无额外字段",
      done: uploadDone,
    },
    {
      key: "photo",
      label: "现场照片",
      text: "开始不必填",
      done: true,
    },
    {
      key: "zhihang",
      label: "智航绑定",
      text: zhihangStatus.value.text,
      done: zhihangStatus.value.done,
    },
  ];
});
const draftNextActionHint = computed(() => {
  if (props.busy) {
    return {
      tone: "warning",
      title: "后台处理中",
      text: "这条通告正在发送或上传，完成前不要重复操作。",
    };
  }
  if (props.missingText) {
    return {
      tone: "danger",
      title: "先补必填",
      text: props.missingText,
    };
  }
  if (props.warningText) {
    return {
      tone: "warning",
      title: "先核对类型",
      text: "通告类型可能不匹配，确认无误后再发送。",
    };
  }
  return {
    tone: "ready",
    title: "可以发送",
    text: "字段已具备发送条件，发送成功后会进入右侧进行中。",
  };
});
const sendDisabledReason = computed(() => {
  if (props.busy) return "当前通告正在处理，请等待任务完成。";
  if (props.missingText) return props.missingText;
  return "";
});

function hasValue(value: unknown): boolean {
  return String(value ?? "").trim() !== "";
}
</script>

<style scoped>
.draft-card {
  padding: 10px;
  border: 1px solid #dbe3ee;
  border-radius: 8px;
  background: #ffffff;
  transition: border-color 0.14s ease, background-color 0.14s ease, box-shadow 0.14s ease;
}

.draft-card.collapsed {
  padding: 9px 10px;
  cursor: pointer;
  background: #fbfdff;
}

.draft-card.collapsed:hover {
  border-color: #bfdbfe;
  background: #f8fbff;
}

.draft-card.active {
  border-color: #2563eb;
  box-shadow: 0 0 0 2px rgba(37, 99, 235, 0.12);
}

.draft-next-action-hint {
  display: grid;
  grid-template-columns: auto minmax(0, 1fr);
  align-items: center;
  gap: 8px;
  border: 1px solid #cfe0ff;
  border-radius: 16px;
  padding: 9px 11px;
  background: #eff6ff;
  color: #1d4ed8;
  font-size: 12px;
  line-height: 1.4;
}

.draft-next-action-hint strong {
  border-radius: 999px;
  padding: 4px 8px;
  background: rgba(255, 255, 255, 0.82);
  color: inherit;
  font-weight: 950;
  white-space: nowrap;
}

.draft-next-action-hint span {
  min-width: 0;
  color: #31506f;
  font-weight: 850;
}

.draft-next-action-hint.ready {
  border-color: #bbf7d0;
  background: #ecfdf5;
  color: #047857;
}

.draft-next-action-hint.warning {
  border-color: #fed7aa;
  background: #fff7ed;
  color: #c2410c;
}

.draft-next-action-hint.danger {
  border-color: #fecaca;
  background: #fef2f2;
  color: #b91c1c;
}

.draft-card.busy .form-grid,
.draft-card.busy .repair-fields,
.draft-card.busy .upload-fields {
  opacity: 0.72;
  pointer-events: none;
}

.card-title,
.card-actions {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
}

.card-title span {
  flex: 0 0 auto;
  padding: 3px 8px;
  border-radius: 999px;
  background: #eef2ff;
  color: #3730a3;
  font-size: 12px;
}

.draft-compact {
  display: grid;
  gap: 6px;
  margin-top: 6px;
}

.draft-compact p {
  overflow: hidden;
  margin: 0;
  color: #64748b;
  font-size: 13px;
  line-height: 1.45;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.compact-actions {
  margin-top: 0;
}

.draft-hint,
.draft-required-line,
.draft-warning-line {
  margin: 0;
  padding: 8px 10px;
  border-radius: 7px;
  font-size: 13px;
  line-height: 1.5;
}

.draft-hint {
  border: 1px solid #bfdbfe;
  background: #eff6ff;
  color: #1d4ed8;
}

.draft-required-line {
  border: 1px solid #fecaca;
  background: #fef2f2;
  color: #b91c1c;
}

.draft-warning-line {
  border: 1px solid #fde68a;
  background: #fffbeb;
  color: #92400e;
}

.form-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 8px;
  margin-top: 8px;
}

label {
  display: grid;
  gap: 5px;
  color: #475569;
  font-size: 13px;
}

.checkbox-field {
  display: flex;
  align-items: center;
  gap: 8px;
}

.checkbox-field input,
.zhihang-line input[type="checkbox"] {
  width: auto;
  flex: 0 0 auto;
}

input,
select,
textarea {
  width: 100%;
  border: 1px solid #cbd5e1;
  border-radius: 6px;
  padding: 7px 9px;
  background: #ffffff;
  color: #0f172a;
  font: inherit;
}

textarea {
  min-height: 58px;
  resize: vertical;
}

.span-2 {
  grid-column: 1 / -1;
}

.repair-fields,
.upload-fields,
.zhihang-line,
.card-actions {
  margin-top: 10px;
}

.repair-fields h3 {
  margin: 0 0 8px;
  color: #334155;
  font-size: 13px;
}

.repair-fields h4 {
  margin: 8px 0 6px;
  color: #64748b;
  font-size: 12px;
  font-weight: 700;
}

.upload-fields {
  border: 1px solid #e2e8f0;
  border-radius: 8px;
  padding: 8px 10px 10px;
  background: #f8fafc;
}

.upload-fields h3,
.upload-fields summary {
  margin: 0 0 8px;
  cursor: pointer;
  color: #334155;
  font-size: 13px;
  font-weight: 600;
}

.upload-fields h3 {
  cursor: default;
}

.zhihang-line {
  display: grid;
  gap: 8px;
}

.upload-preview-grid {
  display: grid;
  grid-template-columns: 112px minmax(0, 1fr);
  gap: 6px 10px;
  margin: 8px 0 0;
}

.upload-preview-grid dt {
  color: #64748b;
  font-size: 12px;
}

.upload-preview-grid dd {
  margin: 0;
  color: #0f172a;
  font-size: 13px;
  line-height: 1.45;
  word-break: break-word;
}

.notice-preview {
  max-height: 260px;
  margin: 0;
  padding: 10px;
  overflow: auto;
  border: 1px solid #bfdbfe;
  border-radius: 8px;
  background: #f8fbff;
  color: #0f172a;
  font-family: ui-monospace, SFMono-Regular, Consolas, "Liberation Mono", monospace;
  font-size: 12px;
  line-height: 1.65;
  white-space: pre-wrap;
}

.field-missing,
.field-missing span {
  color: #b91c1c;
}

.field-missing input,
.field-missing textarea,
.field-missing select {
  border-color: #ef4444;
  background: #fff7f7;
}

.job-line {
  flex: 1 1 auto;
  color: #64748b;
  font-size: 13px;
}

.job-line.busy {
  color: #1d4ed8;
}

.job-line.success {
  color: #15803d;
}

.job-line.failed {
  color: #b91c1c;
}

.btn {
  border: 1px solid #cbd5e1;
  border-radius: 6px;
  padding: 8px 12px;
  background: #ffffff;
  color: #0f172a;
  font-size: 14px;
  line-height: 1;
  cursor: pointer;
}

.btn:disabled {
  cursor: not-allowed;
  opacity: 0.58;
}

.btn.blue {
  border-color: #2563eb;
  background: #2563eb;
  color: #ffffff;
}

@media (max-width: 720px) {
  .form-grid {
    grid-template-columns: 1fr;
  }
}

/* VNET notice card skin */
.draft-card {
  position: relative;
  overflow: hidden;
  border-color: #d8e7f8;
  border-radius: 14px;
  background: rgba(255, 255, 255, 0.96);
  box-shadow: 0 12px 30px rgba(22, 78, 151, 0.08);
}

.draft-card::before {
  content: "";
  position: absolute;
  inset: 0 0 auto;
  height: 3px;
  background: linear-gradient(90deg, #0757d7, #1678ff 58%, #21c6e7);
  opacity: 0.18;
}

.draft-card > * {
  position: relative;
  z-index: 1;
}

.draft-card.collapsed {
  background: #ffffff;
}

.draft-card.collapsed:hover {
  border-color: #9cc7ff;
  background: #f5faff;
  box-shadow: 0 14px 32px rgba(22, 78, 151, 0.12);
}

.draft-card.active {
  border-color: #1678ff;
  box-shadow: inset 4px 0 0 #1678ff, 0 18px 42px rgba(22, 120, 255, 0.14);
}

.draft-card.active::before {
  opacity: 1;
}

.card-title strong {
  color: #09204a;
  font-weight: 900;
}

.card-title span {
  background: #eaf3ff;
  color: #0757d7;
  font-weight: 800;
}

.draft-hint,
.upload-fields,
.notice-preview {
  border-color: #d8e7f8;
  border-radius: 12px;
  background: #f7fbff;
}

input,
select,
textarea {
  border-color: #c8dcf3;
  border-radius: 9px;
  background: #fbfdff;
}

input:focus,
select:focus,
textarea:focus {
  border-color: #1678ff;
  outline: none;
  box-shadow: 0 0 0 3px rgba(22, 120, 255, 0.12);
}

.btn {
  min-height: 36px;
  border-color: #c5d9f2;
  border-radius: 9px;
  color: #09204a;
  font-weight: 750;
  transition: border-color 0.14s ease, background 0.14s ease, box-shadow 0.14s ease, transform 0.14s ease;
}

.btn:hover:not(:disabled) {
  border-color: #8dbbfb;
  box-shadow: 0 8px 20px rgba(27, 101, 213, 0.13);
  transform: translateY(-1px);
}

.btn.blue {
  border-color: transparent;
  background: linear-gradient(135deg, #0757d7, #1678ff);
  color: #ffffff;
  box-shadow: 0 12px 24px rgba(20, 103, 226, 0.22);
}

/* Softer rounded VNET notice card polish */
.draft-card {
  border-radius: 20px;
}

.draft-hint,
.upload-fields,
.notice-preview {
  border-radius: 16px;
}

input,
select,
textarea,
.btn {
  border-radius: 12px;
}

.card-title strong {
  font-weight: 820;
  letter-spacing: 0;
}

.card-title span {
  border: 1px solid #d8e7f8;
  background: rgba(234, 243, 255, 0.78);
  color: #0b5ed8;
  font-weight: 720;
}

.btn {
  font-weight: 720;
}

/* Panorama construction-management polish */
.draft-card {
  border-color: rgba(207, 224, 255, 0.94);
  background: rgba(255, 255, 255, 0.98);
  box-shadow: 0 10px 26px rgba(20, 70, 138, 0.08);
}

.draft-card.collapsed {
  background: rgba(255, 255, 255, 0.96);
}

.draft-card.collapsed:hover {
  background: #f8fbff;
  box-shadow: 0 14px 34px rgba(20, 70, 138, 0.11);
}

.draft-card.active {
  box-shadow: inset 4px 0 0 #3080ff, 0 16px 38px rgba(21, 93, 252, 0.13);
}

.card-title span {
  background: rgba(239, 246, 255, 0.92);
  color: #155dfc;
}

input,
select,
textarea,
.btn {
  border-radius: 14px;
}

.btn.blue {
  background: linear-gradient(135deg, #155dfc, #3080ff);
  box-shadow: 0 10px 22px rgba(21, 93, 252, 0.2);
}

/* Panorama construction-management card skin */
.draft-card {
  border-color: #d8e5f7;
  background: rgba(255, 255, 255, 0.9);
  box-shadow: 0 10px 22px rgba(0, 47, 135, 0.07);
}

.draft-card.collapsed {
  background: rgba(255, 255, 255, 0.86);
}

.draft-card.collapsed:hover {
  border-color: #bdd2f4;
  background: #ffffff;
}

.draft-card.active {
  border-color: #005bff;
  box-shadow: inset 4px 0 0 #005bff, 0 14px 28px rgba(0, 91, 255, 0.12);
}

.draft-card::before {
  background: linear-gradient(90deg, #1e63ff, #005bff);
}

.card-title span,
.draft-hint,
.upload-fields,
.notice-preview {
  border-color: #cfe0ff;
  background: rgba(239, 246, 255, 0.78);
  color: #005bff;
}

.draft-warning-line {
  border-color: #fde68a;
  background: #fffbeb;
  color: #92400e;
}

.draft-required-line,
.field-missing input,
.field-missing textarea,
.field-missing select {
  border-color: #fecaca;
  background: #fef2f2;
}

input,
select,
textarea {
  border-color: #d8e5f7;
  background: rgba(255, 255, 255, 0.9);
}

/* Final VNET production card pass */
.draft-card {
  padding: 12px;
  border-color: rgba(207, 224, 255, 0.96);
  border-radius: 20px;
  background:
    linear-gradient(135deg, rgba(255, 255, 255, 0.98), rgba(248, 251, 255, 0.92)),
    #fff;
  box-shadow: 0 12px 28px rgba(0, 47, 135, 0.08);
}

.draft-card.collapsed {
  padding: 11px 12px;
}

.draft-card.collapsed:hover {
  border-color: #bdd2f4;
  background: #ffffff;
  box-shadow: 0 16px 34px rgba(0, 47, 135, 0.11);
}

.draft-card.active {
  border-color: #1e63ff;
  box-shadow:
    inset 4px 0 0 #1e63ff,
    0 16px 36px rgba(30, 99, 255, 0.13);
}

.card-title {
  min-width: 0;
}

.card-title strong {
  min-width: 0;
  overflow: hidden;
  color: #0f172a;
  font-size: 15px;
  font-weight: 900;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.card-title span {
  border-color: #cfe0ff;
  border-radius: 999px;
  background: #eff6ff;
  color: #1d4ed8;
  font-size: 12px;
  font-weight: 850;
}

.draft-hint,
.draft-required-line,
.draft-warning-line,
.upload-fields,
.notice-preview,
.action-group {
  border-radius: 16px;
}

input,
select,
textarea {
  border-color: #d8e5f7;
  border-radius: 14px;
  background: rgba(255, 255, 255, 0.94);
}

.btn {
  min-height: 38px;
  border-radius: 14px;
  font-weight: 850;
}

.btn.blue {
  background: linear-gradient(135deg, #1e63ff, #1554df);
  box-shadow: 0 12px 26px rgba(30, 99, 255, 0.22);
}

input:focus,
select:focus,
textarea:focus {
  border-color: #005bff;
  box-shadow: 0 0 0 3px rgba(0, 91, 255, 0.14);
}

.btn.blue {
  background: linear-gradient(135deg, #1e63ff, #1554df);
}

.btn.blue:hover:not(:disabled) {
  background: #1554df;
}

/* Final VNET production card pass */
.draft-card {
  padding: 12px;
  border-color: rgba(207, 224, 255, 0.96);
  border-radius: 20px;
  background:
    linear-gradient(135deg, rgba(255, 255, 255, 0.98), rgba(248, 251, 255, 0.92)),
    #fff;
  box-shadow: 0 12px 28px rgba(0, 47, 135, 0.08);
}

.draft-card.collapsed {
  padding: 11px 12px;
}

.draft-card.collapsed:hover {
  border-color: #bdd2f4;
  background: #ffffff;
  box-shadow: 0 16px 34px rgba(0, 47, 135, 0.11);
}

.draft-card.active {
  border-color: #1e63ff;
  box-shadow:
    inset 4px 0 0 #1e63ff,
    0 16px 36px rgba(30, 99, 255, 0.13);
}

.card-title {
  min-width: 0;
}

.card-title strong {
  min-width: 0;
  overflow: hidden;
  color: #0f172a;
  font-size: 15px;
  font-weight: 900;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.card-title span {
  border-color: #cfe0ff;
  border-radius: 999px;
  background: #eff6ff;
  color: #1d4ed8;
  font-size: 12px;
  font-weight: 850;
}

.draft-hint,
.draft-required-line,
.draft-warning-line,
.upload-fields,
.notice-preview {
  border-radius: 16px;
}

input,
select,
textarea {
  border-color: #d8e5f7;
  border-radius: 14px;
  background: rgba(255, 255, 255, 0.94);
}

.btn {
  min-height: 38px;
  border-radius: 14px;
  font-weight: 850;
}

.btn.blue {
  background: linear-gradient(135deg, #1e63ff, #1554df);
  box-shadow: 0 12px 26px rgba(30, 99, 255, 0.22);
}

.draft-action-clusters {
  align-items: stretch;
  flex-wrap: wrap;
  padding-top: 4px;
  border-top: 1px solid rgba(216, 229, 247, 0.82);
}

.draft-action-clusters .job-line {
  display: inline-flex;
  align-items: center;
  min-height: 38px;
}

.job-line {
  max-width: 100%;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-height: 32px;
  border: 1px solid #d8e5f7;
  border-radius: 999px;
  padding: 5px 10px;
  background: #f8fbff;
  color: #48627f;
  font-size: 12px;
  font-weight: 850;
  line-height: 1.25;
  overflow: hidden;
  text-overflow: ellipsis;
}

.job-line.busy {
  border-color: #bfdbfe;
  background: #eff6ff;
  color: #1d4ed8;
}

.job-line.success {
  border-color: #bbf7d0;
  background: #ecfdf5;
  color: #047857;
}

.job-line.failed {
  border-color: #fecaca;
  background: #fef2f2;
  color: #b91c1c;
}

.action-group {
  display: inline-flex;
  align-items: center;
  gap: 7px;
  min-height: 42px;
  border: 1px solid rgba(191, 219, 254, 0.74);
  border-radius: 16px;
  padding: 4px 5px 4px 10px;
  background: rgba(248, 251, 255, 0.88);
}

.action-group strong {
  color: #48627f;
  font-size: 12px;
  font-weight: 950;
  white-space: nowrap;
}

.primary-action {
  border-color: rgba(30, 99, 255, 0.2);
  background: linear-gradient(135deg, rgba(239, 246, 255, 0.96), rgba(255, 255, 255, 0.9));
}

.primary-action :deep(.disabled-reason) {
  flex: 1 1 100%;
  margin: 0;
  justify-content: flex-start;
}

.support-actions {
  flex-wrap: wrap;
}

.danger-lite {
  border-color: #fecaca;
  color: #b91c1c;
  background: #fff7f7;
}

@media (max-width: 760px) {
  .draft-action-clusters,
  .action-group {
    width: 100%;
  }

  .action-group {
    align-items: stretch;
    flex-direction: column;
  }

  .support-actions .btn,
  .primary-action .btn {
    width: 100%;
  }
}
</style>


