<template>
  <article
    class="ongoing-card"
    :class="{ active: expanded, collapsed: !expanded, busy }"
    @click="emit('expand')"
  >
    <div class="card-title" @click.stop="emit('toggle')">
      <strong>{{ title }}</strong>
      <span>{{ workTypeLabel(item.work_type) }}{{ expanded ? " · 正在编辑" : " · 点击展开编辑" }}</span>
    </div>
    <p>{{ meta }}</p>
    <div v-if="!expanded" class="ongoing-compact">
      <p>{{ compactSummary || "已开始未结束，点击展开后可更新、结束或删除。" }}</p>
      <div class="card-actions compact-actions">
        <span class="job-line" :class="jobClass(lineKey)">{{ jobText(lineKey) }}</span>
        <button v-if="copyText" class="btn ghost" type="button" @click.stop="emit('copy-notice')">复制通告</button>
        <button class="btn ghost compact-expand" type="button" title="展开后可更新、结束、删除或回退" @click.stop="emit('toggle')">展开处理</button>
      </div>
    </div>
    <template v-else>
      <div class="ongoing-expanded" @click.stop>
        <div class="next-action-hint" :class="nextActionHint.tone" aria-live="polite">
          <strong>{{ nextActionHint.title }}</strong>
          <span>{{ nextActionHint.text }}</span>
        </div>
        <NoticeCompletionPanel :items="completionItems" />
        <NoticeMessageFields
          :work-type="workType"
          :draft="draft"
          :list-id-prefix="lineKey"
          :disabled="busy"
          @set-field="setEdit"
        />
        <NoticeUploadFields
          :work-type="workType"
          :draft="draft"
          :line-key="lineKey"
          :maintenance-cycle-options="maintenanceCycleOptions"
          :zhihang-records="zhihangRecords"
          :sync-maintenance-visible="syncMaintenanceVisible"
          @set-field="setEdit"
          @bind-zhihang="emit('bind-zhihang', $event)"
        />
        <section
          class="site-photo-fields"
          :class="{ required: sitePhotoRequired, missing: sitePhotoRequired && photoCount === 0 }"
          tabindex="0"
          @paste.stop="emitPhotoPaste"
        >
          <div class="site-photo-head">
            <h3>现场照片</h3>
            <span :class="{ required: sitePhotoRequired && photoCount === 0 }">已添加 {{ photoCount }} 张</span>
          </div>
          <div class="site-photo-actions">
            <label class="btn ghost site-photo-picker">
              添加现场照片
              <input type="file" accept="image/*" multiple @change="emit('photo-input', $event)" />
            </label>
            <small>
              {{
                sitePhotoRequired
                  ? "结束前必填，可粘贴截图或选择图片。"
                  : "可粘贴截图或选择图片。"
              }}
            </small>
          </div>
          <div class="site-photo-paste-hint">复制图片后点击此区域，按 Ctrl+V 粘贴现场照片</div>
          <DisabledReason
            v-if="endDisabledReason"
            :text="endDisabledReason"
            tone="warning"
          />
          <div v-if="photoCount" class="site-photo-list">
            <button
              v-for="(photo, index) in draft.extra_images || []"
              :key="`${lineKey}:photo:${index}`"
              class="site-photo-chip"
              :class="{ 'has-preview': Boolean(photoPreviewUrl(photo)) }"
              type="button"
              @click="emit('remove-photo', index)"
            >
              <img v-if="photoPreviewUrl(photo)" :src="photoPreviewUrl(photo)" :alt="photo.file_name || `现场照片 ${index + 1}`" loading="lazy" />
              <span>{{ photo.file_name || `现场照片 ${index + 1}` }}</span>
              <em>移除</em>
            </button>
          </div>
        </section>
        <div class="card-actions action-clusters">
          <span class="job-line" :class="jobClass(lineKey)">{{ jobText(lineKey) }}</span>
          <DisabledReason
            v-if="busy"
            text="当前通告正在处理，请等待任务完成。"
            tone="warning"
          />
          <div class="action-group primary-actions">
            <strong>发送</strong>
            <DisabledReason
              v-if="updateDisabledReason"
              :text="updateDisabledReason"
              tone="warning"
            />
            <button
              class="btn blue"
              :disabled="Boolean(updateDisabledReason)"
              :title="updateDisabledReason"
              @click="emit('send', 'update')"
            >
              发送{{ workTypeLabel(workType) }}更新
            </button>
            <button
              class="btn green"
              :disabled="Boolean(endDisabledReason)"
              :title="endDisabledReason"
              @click="emit('send', 'end')"
            >
              发送{{ workTypeLabel(workType) }}结束
            </button>
          </div>
          <details class="more-actions">
            <summary>更多操作</summary>
            <div class="more-action-grid">
              <button v-if="copyText" class="btn ghost" type="button" @click.stop="emit('copy-notice')">复制通告</button>
              <button v-if="needsBinding" class="btn ghost" :disabled="busy" @click="emit('bind-target')">选择对应通告</button>
              <button v-if="item.undo_available" class="btn ghost" :disabled="undoBusy" @click="emit('apply-undo')">回退上一步</button>
              <button class="btn danger" :disabled="busy" @click="emit('delete')">删除通告</button>
            </div>
          </details>
        </div>
        <div v-if="item.undo_available" class="undo-line">
          {{ item.undo_label || "可回退上一步" }}
          <span :class="jobClass(undoLineKey)">{{ jobText(undoLineKey) }}</span>
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
} from "../noticeTemplates";
import DisabledReason from "./DisabledReason.vue";
import NoticeCompletionPanel from "./NoticeCompletionPanel.vue";
import NoticeMessageFields from "./NoticeMessageFields.vue";
import NoticeUploadFields from "./NoticeUploadFields.vue";

type Dict = Record<string, any>;

const props = defineProps<{
  item: Dict;
  draft: Dict;
  title: string;
  meta: string;
  compactSummary: string;
  lineKey: string;
  undoLineKey: string;
  expanded: boolean;
  busy: boolean;
  undoBusy: boolean;
  needsBinding: boolean;
  photoCount: number;
  sitePhotoRequired: boolean;
  maintenanceCycleOptions: string[];
  zhihangRecords: Dict[];
  syncMaintenanceVisible: boolean;
  jobText: (key: string) => string;
  jobClass: (key: string) => string;
  copyText: string;
}>();

const emit = defineEmits<{
  expand: [];
  toggle: [];
  "set-edit": [key: string, value: any];
  "bind-zhihang": [recordId: string];
  "photo-input": [event: Event];
  "photo-paste": [event: ClipboardEvent];
  "remove-photo": [index: number];
  send: [action: string];
  "copy-notice": [];
  delete: [];
  "bind-target": [];
  "apply-undo": [];
}>();

const workType = computed(() => props.item.work_type || "maintenance");
const messageFieldStatus = computed(() => {
  const fields = noticeTemplate(workType.value).messageFields;
  const total = fields.length;
  const done = fields.filter((field) => hasValue(props.draft[field] ?? props.item[field])).length;
  return { total, done };
});
const uploadFieldStatus = computed(() => {
  const fields = noticeTemplate(workType.value).uploadFields.filter((field) => field !== "non_plan" && field !== "zhihang");
  const total = fields.length;
  const done = fields.filter((field) => hasValue(props.draft[field] ?? props.item[field])).length;
  return { total, done };
});
const zhihangStatus = computed(() => {
  if (!isNoticeUploadField(workType.value, "zhihang")) return { required: false, done: true, text: "不涉及" };
  const involved = Boolean(props.draft.zhihang_involved || props.item.zhihang_involved);
  if (!involved) return { required: true, done: true, text: "未涉及" };
  const selected = hasValue(props.draft.zhihang_record_id || props.item.zhihang_record_id);
  return { required: true, done: selected, text: selected ? "已绑定" : "待选择" };
});
const completionItems = computed(() => {
  const photoDone = !props.sitePhotoRequired || props.photoCount > 0;
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
      text: props.sitePhotoRequired ? `${props.photoCount} 张 / 必填` : `${props.photoCount} 张 / 可选`,
      done: photoDone,
    },
    {
      key: "zhihang",
      label: "智航绑定",
      text: zhihangStatus.value.text,
      done: zhihangStatus.value.done,
    },
  ];
});
const nextActionHint = computed(() => {
  if (props.busy) {
    return {
      tone: "warning",
      title: "后台处理中",
      text: "这条通告正在发送或上传，完成前不要重复操作。",
    };
  }
  if (props.needsBinding) {
    return {
      tone: "warning",
      title: "先选择对应通告",
      text: "当前记录缺少可更新的多维记录，先在更多操作中选择对应通告。",
    };
  }
  if (props.sitePhotoRequired && props.photoCount === 0) {
    return {
      tone: "danger",
      title: "结束前补现场照片",
      text: "维保、变更、检修结束前需要至少一张现场照片；更新不受影响。",
    };
  }
  return {
    tone: "ready",
    title: "可以发送",
    text: "核对字段后，可发送更新；确认闭环时再发送结束。",
  };
});
const updateDisabledReason = computed(() => {
  if (props.busy) return "当前通告正在处理，请等待任务完成。";
  if (props.needsBinding) return "当前通告缺少对应多维记录，请先在更多操作中选择对应通告。";
  return "";
});
const endDisabledReason = computed(() => {
  if (updateDisabledReason.value) return updateDisabledReason.value;
  if (props.sitePhotoRequired && props.photoCount === 0) return "结束通告前需要至少添加一张现场照片。";
  return "";
});

function setEdit(key: string, value: any): void {
  emit("set-edit", key, value);
}

function emitPhotoPaste(event: ClipboardEvent): void {
  emit("photo-paste", event);
}

function hasValue(value: unknown): boolean {
  return String(value ?? "").trim() !== "";
}

function photoPreviewUrl(photo: Dict): string {
  const direct = String(photo?.preview_url || photo?.url || photo?.download_url || "").trim();
  if (direct) return direct;
  const dataUrl = String(photo?.data_url || "").trim();
  if (dataUrl.startsWith("data:image/")) return dataUrl;
  const bytes = String(photo?.bytes_b64 || "").trim();
  const mime = String(photo?.mime_type || "image/png").trim() || "image/png";
  return bytes ? `data:${mime};base64,${bytes}` : "";
}
</script>

<style scoped>
.ongoing-card {
  display: grid;
  gap: 8px;
  padding: 10px;
  border: 1px solid #dbe3ee;
  border-radius: 8px;
  background: #ffffff;
  transition: border-color 0.14s ease, background-color 0.14s ease, box-shadow 0.14s ease;
}

.ongoing-card.collapsed {
  padding: 9px 10px;
  cursor: pointer;
  background: #fbfdff;
}

.ongoing-card.collapsed:hover {
  border-color: #bfdbfe;
  background: #f8fbff;
}

.ongoing-card.active {
  border-color: #2563eb;
  box-shadow: 0 0 0 2px rgba(37, 99, 235, 0.12);
}

.ongoing-card.busy .form-grid,
.ongoing-card.busy .repair-fields,
.ongoing-card.busy .upload-fields,
.ongoing-card.busy .site-photo-fields {
  opacity: 0.72;
  pointer-events: none;
}

.ongoing-card p {
  color: #334155;
  font-size: 13px;
  line-height: 1.45;
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

.ongoing-compact {
  display: grid;
  gap: 6px;
}

.ongoing-compact p {
  display: -webkit-box;
  margin: 0;
  overflow: hidden;
  -webkit-line-clamp: 2;
  -webkit-box-orient: vertical;
  color: #475569;
}

.ongoing-expanded {
  display: grid;
  gap: 8px;
}

.next-action-hint {
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

.next-action-hint strong {
  border-radius: 999px;
  padding: 4px 8px;
  background: rgba(255, 255, 255, 0.82);
  color: inherit;
  font-weight: 950;
  white-space: nowrap;
}

.next-action-hint span {
  min-width: 0;
  color: #31506f;
  font-weight: 850;
}

.next-action-hint.ready {
  border-color: #bbf7d0;
  background: #ecfdf5;
  color: #047857;
}

.next-action-hint.warning {
  border-color: #fed7aa;
  background: #fff7ed;
  color: #c2410c;
}

.next-action-hint.danger {
  border-color: #fecaca;
  background: #fef2f2;
  color: #b91c1c;
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
  min-height: 54px;
  resize: vertical;
}

.span-2 {
  grid-column: 1 / -1;
}

.repair-fields,
.upload-fields {
  padding: 8px 10px;
  border: 1px solid #e2e8f0;
  border-radius: 8px;
  background: #fbfdff;
}

.repair-fields h3 {
  margin: 0 0 8px;
  font-size: 13px;
}

.upload-fields h3,
.upload-fields summary {
  margin: 0 0 8px;
  cursor: pointer;
  color: #2563eb;
  font-weight: 700;
  font-size: 13px;
}

.upload-fields h3 {
  cursor: default;
}

.zhihang-line {
  display: grid;
  gap: 8px;
}

.zhihang-line label {
  display: flex;
  align-items: center;
  gap: 8px;
}

.zhihang-line input[type="checkbox"] {
  width: auto;
}

.site-photo-fields {
  display: grid;
  gap: 8px;
  padding: 9px 10px;
  border: 1px solid #dbeafe;
  border-radius: 8px;
  background: #f8fbff;
  outline: none;
}

.site-photo-fields:focus-within,
.site-photo-fields:focus {
  border-color: #2563eb;
  box-shadow: 0 0 0 3px rgba(37, 99, 235, 0.12);
}

.site-photo-head,
.site-photo-actions,
.site-photo-list {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}

.site-photo-head {
  justify-content: space-between;
}

.site-photo-head h3 {
  margin: 0;
  font-size: 13px;
}

.site-photo-head span {
  color: #047857;
  font-size: 12px;
}

.site-photo-head span.required {
  color: #b91c1c;
}

.site-photo-actions small {
  color: #64748b;
  font-size: 12px;
}

.site-photo-paste-hint {
  border: 1px dashed #bfdbfe;
  border-radius: 8px;
  padding: 8px 10px;
  background: #ffffff;
  color: #2563eb;
  font-size: 12px;
  line-height: 1.5;
}

.site-photo-picker {
  position: relative;
  overflow: hidden;
  cursor: pointer;
}

.site-photo-picker input {
  position: absolute;
  inset: 0;
  opacity: 0;
  cursor: pointer;
}

.site-photo-chip {
  display: inline-grid;
  grid-template-columns: auto minmax(0, 1fr) auto;
  align-items: center;
  gap: 7px;
  max-width: 100%;
  border: 1px solid #bfdbfe;
  border-radius: 12px;
  padding: 6px 8px;
  background: #ffffff;
  color: #1d4ed8;
  font-size: 12px;
  text-align: left;
  cursor: pointer;
}

.site-photo-chip img {
  width: 42px;
  height: 32px;
  border-radius: 8px;
  object-fit: cover;
  background: #eff6ff;
}

.site-photo-chip span {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.site-photo-chip em {
  color: #dc2626;
  font-style: normal;
  font-weight: 800;
}

.site-photo-chip:not(.has-preview) {
  grid-template-columns: minmax(0, 1fr) auto;
}

.site-photo-chip:hover {
  border-color: #93c5fd;
  background: #eff6ff;
}

.undo-line {
  display: flex;
  justify-content: space-between;
  gap: 8px;
  color: #64748b;
  font-size: 12px;
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

.btn.green {
  border-color: #16a34a;
  background: #16a34a;
  color: #ffffff;
}

.btn.danger {
  border-color: #dc2626;
  background: #dc2626;
  color: #ffffff;
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

@media (max-width: 720px) {
  .form-grid {
    grid-template-columns: 1fr;
  }
}

/* VNET ongoing card skin */
.ongoing-card {
  position: relative;
  overflow: hidden;
  border-color: #d8e7f8;
  border-radius: 14px;
  background: rgba(255, 255, 255, 0.96);
  box-shadow: 0 12px 30px rgba(22, 78, 151, 0.08);
}

.ongoing-card::before {
  content: "";
  position: absolute;
  inset: 0 0 auto;
  height: 3px;
  background: linear-gradient(90deg, #0757d7, #1678ff 58%, #21c6e7);
  opacity: 0.18;
}

.ongoing-card > * {
  position: relative;
  z-index: 1;
}

.ongoing-card.collapsed {
  background: #ffffff;
}

.ongoing-card.collapsed:hover {
  border-color: #9cc7ff;
  background: #f5faff;
  box-shadow: 0 14px 32px rgba(22, 78, 151, 0.12);
}

.ongoing-card.active {
  border-color: #1678ff;
  box-shadow: inset 4px 0 0 #1678ff, 0 18px 42px rgba(22, 120, 255, 0.14);
}

.ongoing-card.active::before {
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

.repair-fields,
.upload-fields,
.site-photo-fields {
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

.btn.green {
  border-color: transparent;
  background: linear-gradient(135deg, #16a36d, #2fd083);
  color: #ffffff;
}

.btn.danger {
  border-color: transparent;
  background: linear-gradient(135deg, #dc2626, #f05656);
  color: #ffffff;
}

/* Softer rounded VNET ongoing card polish */
.ongoing-card {
  border-radius: 18px;
}

.repair-fields,
.upload-fields,
.site-photo-fields {
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
.ongoing-card {
  border-color: rgba(207, 224, 255, 0.94);
  background: rgba(255, 255, 255, 0.98);
  box-shadow: 0 10px 26px rgba(20, 70, 138, 0.08);
}

.ongoing-card.collapsed {
  background: rgba(255, 255, 255, 0.96);
}

.ongoing-card.collapsed:hover {
  background: #f8fbff;
  box-shadow: 0 14px 34px rgba(20, 70, 138, 0.11);
}

.ongoing-card.active {
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
  border-radius: 13px;
}

.btn.blue {
  background: linear-gradient(135deg, #155dfc, #3080ff);
  box-shadow: 0 10px 22px rgba(21, 93, 252, 0.2);
}

/* Panorama construction-management card skin */
.ongoing-card {
  border-color: #d8e5f7;
  background: rgba(255, 255, 255, 0.9);
  box-shadow: 0 10px 22px rgba(0, 47, 135, 0.07);
}

.ongoing-card.collapsed {
  background: rgba(255, 255, 255, 0.86);
}

.ongoing-card.collapsed:hover {
  border-color: #bdd2f4;
  background: #ffffff;
}

.ongoing-card.active {
  border-color: #005bff;
  box-shadow: inset 4px 0 0 #005bff, 0 14px 28px rgba(0, 91, 255, 0.12);
}

.ongoing-card::before {
  background: linear-gradient(90deg, #1e63ff, #005bff);
}

.card-title span,
.repair-fields,
.upload-fields,
.site-photo-fields {
  border-color: #cfe0ff;
  background: rgba(239, 246, 255, 0.78);
}

input,
select,
textarea {
  border-color: #d8e5f7;
  background: rgba(255, 255, 255, 0.9);
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

.btn.green {
  background: #059669;
}

.btn.danger {
  background: #e11d48;
}

.site-photo-fields.required {
  border-color: #bfdbfe;
}

.site-photo-fields.missing {
  border-color: #fecaca;
  background: #fff7f7;
}

.site-photo-head span.required {
  border-radius: 999px;
  padding: 3px 8px;
  color: #b91c1c;
  background: #fee2e2;
  font-weight: 800;
}

.action-clusters {
  display: grid;
  grid-template-columns: minmax(150px, 1fr) auto auto auto;
  align-items: start;
  gap: 8px;
}

.action-group {
  display: grid;
  gap: 6px;
  min-width: 0;
  padding: 8px;
  border: 1px solid #d8e5f7;
  border-radius: 14px;
  background: rgba(248, 251, 255, 0.82);
}

.action-group > strong {
  color: #64748b;
  font-size: 12px;
  font-weight: 820;
}

.primary-actions {
  grid-template-columns: auto auto;
}

.primary-actions > strong {
  grid-column: 1 / -1;
}

.fix-actions {
  border-color: #bfdbfe;
  background: #eff6ff;
}

.danger-actions {
  border-color: #fde68a;
  background: #fffbeb;
}

@media (max-width: 720px) {
  .action-clusters {
    grid-template-columns: 1fr;
  }

  .primary-actions {
    grid-template-columns: 1fr;
  }
}

/* Final VNET production card pass */
.ongoing-card {
  padding: 12px;
  border-color: rgba(207, 224, 255, 0.96);
  border-radius: 20px;
  background:
    linear-gradient(135deg, rgba(255, 255, 255, 0.98), rgba(248, 251, 255, 0.92)),
    #fff;
  box-shadow: 0 12px 28px rgba(0, 47, 135, 0.08);
}

.ongoing-card.collapsed {
  padding: 11px 12px;
}

.ongoing-card.collapsed:hover {
  border-color: #bdd2f4;
  background: #ffffff;
  box-shadow: 0 16px 34px rgba(0, 47, 135, 0.11);
}

.ongoing-card.active {
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

.repair-fields,
.upload-fields,
.site-photo-fields,
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

.btn.green {
  background: #059669;
  box-shadow: 0 12px 24px rgba(5, 150, 105, 0.16);
}

.btn.danger {
  background: #e11d48;
  box-shadow: 0 12px 24px rgba(225, 29, 72, 0.14);
}

/* Final action-area density pass: keep the expanded card stable in the right rail. */
.action-clusters {
  grid-template-columns: 1fr;
  align-items: stretch;
  gap: 9px;
  padding-top: 4px;
  border-top: 1px solid rgba(216, 229, 247, 0.82);
}

.action-clusters > .job-line {
  min-height: 30px;
  display: flex;
  align-items: center;
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

.action-clusters > .btn.ghost {
  justify-self: start;
}

.primary-actions {
  grid-template-columns: repeat(2, minmax(0, 1fr));
}

.primary-actions > strong,
.primary-actions :deep(.disabled-reason) {
  grid-column: 1 / -1;
}

.primary-actions :deep(.disabled-reason) {
  margin: 0;
}

.primary-actions .btn,
.fix-actions .btn,
.danger-actions .btn {
  min-width: 0;
}

.more-actions {
  border: 1px solid #d8e5f7;
  border-radius: 16px;
  background: rgba(248, 251, 255, 0.82);
  overflow: hidden;
}

.more-actions summary {
  min-height: 40px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  padding: 0 12px;
  color: #48627f;
  font-size: 13px;
  font-weight: 900;
  cursor: pointer;
  user-select: none;
}

.more-actions summary::after {
  content: "展开";
  border-radius: 999px;
  padding: 4px 8px;
  background: #eff6ff;
  color: #0757d7;
  font-size: 12px;
  font-weight: 900;
}

.more-actions[open] summary {
  border-bottom: 1px solid #e5eefb;
  background: #ffffff;
}

.more-actions[open] summary::after {
  content: "收起";
}

.more-action-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 8px;
  padding: 8px;
}

.more-action-grid .btn {
  min-width: 0;
}

.fix-actions {
  border-color: #bfdbfe;
  background: #eff6ff;
}

.danger-actions {
  grid-template-columns: repeat(2, minmax(0, 1fr));
  border-color: #fecaca;
  background: #fff7f7;
}

.fix-actions > strong,
.danger-actions > strong {
  grid-column: 1 / -1;
}

.undo-line {
  align-items: center;
  gap: 8px;
  padding: 8px 10px;
  border: 1px solid #fde68a;
  border-radius: 14px;
  background: #fffbeb;
  color: #92400e;
  font-weight: 850;
}

@media (max-width: 720px) {
  .primary-actions,
  .danger-actions,
  .more-action-grid {
    grid-template-columns: 1fr;
  }
}
</style>


