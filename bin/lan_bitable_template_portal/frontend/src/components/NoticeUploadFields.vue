<template>
  <section v-if="hasNoticeUploadFields(workType)" class="upload-fields required-upload-fields">
    <div class="upload-fields__head">
      <h3>归档字段（必填）</h3>
      <span>用于记录归档，不进入通告正文</span>
    </div>
    <div class="form-grid">
      <label v-if="isNoticeUploadField(workType, 'specialty')" :class="fieldClassFor('specialty')">
        专业
        <SpecialtyInput
          :list-id="`${lineKey}-upload-specialty-options`"
          :model-value="draft.specialty || ''"
          :placeholder="specialtyPlaceholder"
          @update:model-value="setField('specialty', $event)"
        />
      </label>
      <label v-if="isNoticeUploadField(workType, 'maintenance_cycle')" :class="fieldClassFor('maintenance_cycle')">
        {{ maintenanceCycleLabel }}
        <select :value="draft.maintenance_cycle" @change="setField('maintenance_cycle', ($event.target as HTMLSelectElement).value)">
          <option value="">请选择</option>
          <option v-for="item in maintenanceCycleOptions" :key="item" :value="item">{{ item }}</option>
        </select>
      </label>
      <label v-if="showNonPlan && isNoticeUploadField(workType, 'non_plan')" class="checkbox-field span-2">
        <input :checked="Boolean(draft.non_plan)" type="checkbox" @change="setField('non_plan', ($event.target as HTMLInputElement).checked)" />
        <span>非计划，发送时标题末尾自动追加“（非计划性）”</span>
      </label>
      <label v-if="syncMaintenanceVisible" class="checkbox-field span-2">
        <input
          :checked="draft.sync_maintenance_target !== false"
          type="checkbox"
          @change="setField('sync_maintenance_target', ($event.target as HTMLInputElement).checked)"
        />
        <span>同时写入维保记录（不发送维保文本）</span>
      </label>
      <div v-if="isNoticeUploadField(workType, 'zhihang')" class="zhihang-line span-2">
        <label>
          <input :checked="Boolean(draft.zhihang_involved)" type="checkbox" @change="setField('zhihang_involved', ($event.target as HTMLInputElement).checked)" />
          涉及智航
        </label>
        <select
          v-if="draft.zhihang_involved"
          :class="fieldClassFor('zhihang_record_id')"
          :value="draft.zhihang_record_id"
          @change="changeZhihang(($event.target as HTMLSelectElement).value)"
        >
          <option value="">选择智航变更</option>
          <option v-for="item in zhihangRecords" :key="item.record_id" :value="item.record_id">
            {{ item.title || "未命名智航变更" }}
          </option>
        </select>
      </div>
    </div>
  </section>
</template>

<script setup lang="ts">
import {
  hasNoticeUploadFields,
  isNoticeUploadField,
} from "../noticeTemplates";
import SpecialtyInput from "./SpecialtyInput.vue";

type Dict = Record<string, any>;
type ClassMap = Record<string, boolean>;

const props = withDefaults(defineProps<{
  workType: string;
  draft: Dict;
  lineKey: string;
  maintenanceCycleOptions: string[];
  zhihangRecords: Dict[];
  syncMaintenanceVisible?: boolean;
  showNonPlan?: boolean;
  maintenanceCycleLabel?: string;
  specialtyPlaceholder?: string;
  fieldClass?: (field: string) => ClassMap;
}>(), {
  syncMaintenanceVisible: false,
  showNonPlan: false,
  maintenanceCycleLabel: "维保周期",
  specialtyPlaceholder: "选择或输入专业",
  fieldClass: undefined,
});

const emit = defineEmits<{
  "set-field": [key: string, value: any];
  "bind-zhihang": [recordId: string];
}>();

function fieldClassFor(field: string): ClassMap {
  return props.fieldClass ? props.fieldClass(field) : {};
}

function setField(key: string, value: any): void {
  emit("set-field", key, value);
}

function changeZhihang(value: string): void {
  setField("zhihang_record_id", value);
  emit("bind-zhihang", value);
}
</script>

<style scoped>
.upload-fields {
  display: grid;
  gap: 10px;
  margin-top: 10px;
  border: 1px solid #d8e5f7;
  border-radius: 18px;
  padding: 12px;
  background: linear-gradient(135deg, rgba(248, 251, 255, 0.96), rgba(255, 255, 255, 0.94));
}

.upload-fields__head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
}

.upload-fields h3 {
  margin: 0;
  color: #0f2f6a;
  font-size: 14px;
  font-weight: 950;
}

.upload-fields__head span {
  min-width: 0;
  border-radius: 999px;
  padding: 5px 9px;
  color: #3156c9;
  background: #eef2ff;
  font-size: 12px;
  font-weight: 900;
  line-height: 1.3;
  text-align: right;
}

.form-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 10px;
}

.span-2 {
  grid-column: 1 / -1;
}

label {
  display: grid;
  gap: 5px;
  color: #475569;
  font-size: 12px;
  font-weight: 900;
}

label.field-missing {
  color: #be123c;
}

label.field-missing input,
label.field-missing textarea,
label.field-missing select,
.field-missing {
  border-color: #fb7185;
  background-color: #fff1f2;
}

select {
  min-height: 38px;
  border: 1px solid #d8e5f7;
  border-radius: 14px;
  padding: 0 10px;
  background: #ffffff;
  color: #0f172a;
  font: inherit;
  font-size: 13px;
  font-weight: 800;
}

.checkbox-field,
.zhihang-line {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 10px;
  border: 1px dashed #cfe0ff;
  border-radius: 15px;
  padding: 10px 12px;
  background: rgba(239, 246, 255, 0.62);
  color: #244a75;
}

.checkbox-field input,
.zhihang-line input[type="checkbox"] {
  width: 16px;
  height: 16px;
  accent-color: #1e63ff;
}

.checkbox-field span,
.zhihang-line label {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  color: #244a75;
  font-size: 13px;
  font-weight: 900;
}

.zhihang-line select {
  flex: 1 1 260px;
}

@media (max-width: 760px) {
  .upload-fields__head {
    align-items: flex-start;
    flex-direction: column;
  }

  .form-grid {
    grid-template-columns: 1fr;
  }
}
</style>
