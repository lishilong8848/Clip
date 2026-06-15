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
        <button class="btn ghost" :disabled="busy" @click.stop="emit('pin')">编辑</button>
        <button class="btn ghost" :disabled="busy" @click.stop="emit('remove')">移除</button>
      </div>
    </div>

    <template v-else>
      <p v-if="record.manual && draft.prefilled_from_last" class="draft-hint">
        已带入上次{{ workTypeLabel(workType) }}手填内容，请核对时间和字段后再发送。
      </p>
      <p v-if="warningText" class="draft-warning-line">{{ warningText }}</p>
      <p v-if="missingText" class="draft-required-line">{{ missingText }}</p>

      <div class="form-grid">
        <label v-if="record.manual">
          通告类型
          <select :value="draft.work_type" @change="changeManualType(($event.target as HTMLSelectElement).value)">
            <option v-for="type in workTypes" :key="type.value" :value="type.value">{{ type.label }}</option>
          </select>
        </label>
        <label :class="fieldClass('title')">
          {{ noticeFieldLabel(workType, "title") }}
          <input :value="draft.title" placeholder="通告标题" @input="setDraft('title', ($event.target as HTMLInputElement).value)" />
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
        <label v-if="isNoticeMessageField(workType, 'specialty')" :class="fieldClass('specialty')">
          {{ noticeFieldLabel(workType, "specialty") }}
          <SpecialtyInput :list-id="`${rowKey}-message-specialty-options`" :model-value="draft.specialty || ''" @update:model-value="setDraft('specialty', $event)" />
        </label>
        <label v-if="isNoticeMessageField(workType, 'level')" :class="fieldClass('level')">
          {{ noticeFieldLabel(workType, "level") }}
          <input :value="draft.level" placeholder="等级" @input="setDraft('level', ($event.target as HTMLInputElement).value)" />
        </label>
        <label :class="fieldClass('start_time')">
          {{ noticeFieldLabel(workType, "start_time") }}
          <input :value="draft.start_time" type="datetime-local" @input="setDraft('start_time', ($event.target as HTMLInputElement).value)" />
        </label>
        <label :class="fieldClass('end_time')">
          {{ noticeFieldLabel(workType, "end_time") }}
          <input :value="draft.end_time" type="datetime-local" @input="setDraft('end_time', ($event.target as HTMLInputElement).value)" />
        </label>
        <label v-if="isNoticeMessageField(workType, 'location')" class="span-2" :class="fieldClass('location')">
          {{ noticeFieldLabel(workType, "location") }}
          <input :value="draft.location" placeholder="地点" @input="setDraft('location', ($event.target as HTMLInputElement).value)" />
        </label>
        <label v-if="isNoticeMessageField(workType, 'content')" class="span-2" :class="fieldClass('content')">
          {{ noticeFieldLabel(workType, "content") }}
          <textarea :value="draft.content" placeholder="内容" @input="setDraft('content', ($event.target as HTMLTextAreaElement).value)"></textarea>
        </label>
        <label v-if="isNoticeMessageField(workType, 'reason')" :class="fieldClass('reason')">
          {{ noticeFieldLabel(workType, "reason") }}
          <textarea :value="draft.reason" placeholder="原因" @input="setDraft('reason', ($event.target as HTMLTextAreaElement).value)"></textarea>
        </label>
        <label v-if="isNoticeMessageField(workType, 'impact')" :class="fieldClass('impact')">
          {{ noticeFieldLabel(workType, "impact") }}
          <textarea :value="draft.impact" placeholder="影响" @input="setDraft('impact', ($event.target as HTMLTextAreaElement).value)"></textarea>
        </label>
        <label v-if="isNoticeMessageField(workType, 'progress')" class="span-2" :class="fieldClass('progress')">
          {{ noticeFieldLabel(workType, "progress") }}
          <textarea :value="draft.progress" placeholder="进度" @input="setDraft('progress', ($event.target as HTMLTextAreaElement).value)"></textarea>
        </label>
      </div>

      <section v-if="workType === 'power'" class="repair-fields">
        <h3>上电字段</h3>
        <div class="form-grid">
          <label :class="fieldClass('cabinet')"><span>柜号</span><input :value="draft.cabinet" @input="setDraft('cabinet', ($event.target as HTMLInputElement).value)" /></label>
          <label :class="fieldClass('quantity')"><span>数量</span><input :value="draft.quantity" @input="setDraft('quantity', ($event.target as HTMLInputElement).value)" /></label>
        </div>
      </section>

      <section v-if="workType === 'polling'" class="repair-fields">
        <h3>轮巡字段</h3>
        <div class="form-grid">
          <label class="span-2" :class="fieldClass('device')"><span>设备</span><input :value="draft.device" @input="setDraft('device', ($event.target as HTMLInputElement).value)" /></label>
        </div>
      </section>

      <section v-if="workType === 'repair'" class="repair-fields">
        <h3>检修字段</h3>
        <h4>设备与故障</h4>
        <div class="form-grid">
          <label :class="fieldClass('repair_device')"><span>维修设备</span><input :value="draft.repair_device" @input="setDraft('repair_device', ($event.target as HTMLInputElement).value)" /></label>
          <label :class="fieldClass('repair_fault')"><span>维修故障</span><input :value="draft.repair_fault" @input="setDraft('repair_fault', ($event.target as HTMLInputElement).value)" /></label>
          <label><span>故障类型</span><input :value="draft.fault_type" @input="setDraft('fault_type', ($event.target as HTMLInputElement).value)" /></label>
          <label><span>维修方式</span><input :value="draft.repair_mode" @input="setDraft('repair_mode', ($event.target as HTMLInputElement).value)" /></label>
          <label><span>故障发现方式</span><input :value="draft.discovery" @input="setDraft('discovery', ($event.target as HTMLInputElement).value)" /></label>
          <label><span>故障现象</span><input :value="draft.symptom" @input="setDraft('symptom', ($event.target as HTMLInputElement).value)" /></label>
        </div>
        <h4>处理与结果</h4>
        <div class="form-grid">
          <label class="span-2" :class="fieldClass('solution')"><span>解决方案</span><textarea :value="draft.solution" @input="setDraft('solution', ($event.target as HTMLTextAreaElement).value)"></textarea></label>
          <label class="span-2"><span>备件更换情况</span><textarea :value="draft.spare_parts" @input="setDraft('spare_parts', ($event.target as HTMLTextAreaElement).value)"></textarea></label>
        </div>
      </section>

      <section v-if="hasNoticeUploadFields(workType)" class="upload-fields required-upload-fields">
        <h3>多维上传字段（必填）</h3>
        <div class="form-grid">
          <label v-if="isNoticeUploadField(workType, 'specialty')" :class="fieldClass('specialty')">
            专业
            <SpecialtyInput
              :list-id="`${rowKey}-upload-specialty-options`"
              :model-value="draft.specialty || ''"
              placeholder="用于目标多维字段"
              @update:model-value="setDraft('specialty', $event)"
            />
          </label>
          <label v-if="isNoticeUploadField(workType, 'maintenance_cycle')" :class="fieldClass('maintenance_cycle')">
            {{ record.manual ? "维护周期" : "维保周期" }}
            <select :value="draft.maintenance_cycle" @change="setDraft('maintenance_cycle', ($event.target as HTMLSelectElement).value)">
              <option value="">请选择</option>
              <option v-for="item in maintenanceCycleOptions" :key="item" :value="item">{{ item }}</option>
            </select>
          </label>
          <label v-if="record.manual && isNoticeUploadField(workType, 'non_plan')" class="checkbox-field span-2">
            <input :checked="Boolean(draft.non_plan)" type="checkbox" @change="setDraft('non_plan', ($event.target as HTMLInputElement).checked)" />
            <span>非计划，发送时标题末尾自动追加“（非计划性）”</span>
          </label>
          <div v-if="isNoticeUploadField(workType, 'zhihang')" class="zhihang-line span-2">
            <label>
              <input :checked="Boolean(draft.zhihang_involved)" type="checkbox" @change="setDraft('zhihang_involved', ($event.target as HTMLInputElement).checked)" />
              涉及智航
            </label>
            <select v-if="draft.zhihang_involved" :class="fieldClass('zhihang_record_id')" :value="draft.zhihang_record_id" @change="changeZhihang(($event.target as HTMLSelectElement).value)">
              <option value="">选择智航变更</option>
              <option v-for="item in zhihangRecords" :key="item.record_id" :value="item.record_id">
                {{ item.title || item.record_id }}
              </option>
            </select>
          </div>
        </div>
      </section>

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

      <div class="card-actions">
        <span class="job-line" :class="jobClass(rowKey)">{{ jobText(rowKey) }}</span>
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
        <button class="btn blue" :disabled="busy" @click.stop="emit('send')">
          {{ sendLabel }}
        </button>
        <button class="btn ghost" :disabled="busy" @click.stop="emit('remove')">移除</button>
      </div>
    </template>
  </article>
</template>

<script setup lang="ts">
import {
  hasNoticeUploadFields,
  isNoticeMessageField,
  isNoticeUploadField,
  noticeFieldLabel,
  workTypeLabel,
  workTypes,
} from "../noticeTemplates";
import SpecialtyInput from "./SpecialtyInput.vue";

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
  sendLabel: string;
  fieldClass: (field: string) => ClassMap;
  jobText: (key: string) => string;
  jobClass: (key: string) => string;
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

function changeZhihang(value: string): void {
  setDraft("zhihang_record_id", value);
  emit("bind-zhihang", value);
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
  border-radius: 18px;
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
  border-radius: 13px;
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
</style>


