<template>
  <section class="notice-message-fields" :class="{ disabled }">
    <div class="notice-message-fields__head">
      <h3>通告内容字段</h3>
      <span>会进入飞书通告文本</span>
    </div>
    <div class="form-grid">
      <label :class="fieldClassFor('title')">
        {{ noticeFieldLabel(workType, "title") }}
        <input :value="draft.title" placeholder="通告标题" @input="setField('title', ($event.target as HTMLInputElement).value)" />
      </label>
      <label v-if="isNoticeMessageField(workType, 'specialty')" :class="fieldClassFor('specialty')">
        {{ noticeFieldLabel(workType, "specialty") }}
        <SpecialtyInput :list-id="`${listIdPrefix}-message-specialty-options`" :model-value="draft.specialty || ''" @update:model-value="setField('specialty', $event)" />
      </label>
      <label v-if="isNoticeMessageField(workType, 'level')" :class="fieldClassFor('level')">
        {{ noticeFieldLabel(workType, "level") }}
        <input :value="draft.level" placeholder="等级" @input="setField('level', ($event.target as HTMLInputElement).value)" />
      </label>
      <label :class="fieldClassFor('start_time')">
        {{ noticeFieldLabel(workType, "start_time") }}
        <input :value="draft.start_time" type="datetime-local" @input="setField('start_time', ($event.target as HTMLInputElement).value)" />
      </label>
      <label :class="fieldClassFor('end_time')">
        {{ noticeFieldLabel(workType, "end_time") }}
        <input :value="draft.end_time" type="datetime-local" @input="setField('end_time', ($event.target as HTMLInputElement).value)" />
      </label>
      <label v-if="isNoticeMessageField(workType, 'location')" class="span-2" :class="fieldClassFor('location')">
        {{ noticeFieldLabel(workType, "location") }}
        <input :value="draft.location" placeholder="地点" @input="setField('location', ($event.target as HTMLInputElement).value)" />
      </label>
      <label v-if="isNoticeMessageField(workType, 'content')" class="span-2" :class="fieldClassFor('content')">
        {{ noticeFieldLabel(workType, "content") }}
        <textarea :value="draft.content" placeholder="内容" @input="setField('content', ($event.target as HTMLTextAreaElement).value)"></textarea>
      </label>
      <label v-if="isNoticeMessageField(workType, 'reason')" :class="fieldClassFor('reason')">
        {{ noticeFieldLabel(workType, "reason") }}
        <textarea :value="draft.reason" placeholder="原因" @input="setField('reason', ($event.target as HTMLTextAreaElement).value)"></textarea>
      </label>
      <label v-if="isNoticeMessageField(workType, 'impact')" :class="fieldClassFor('impact')">
        {{ noticeFieldLabel(workType, "impact") }}
        <textarea :value="draft.impact" placeholder="影响" @input="setField('impact', ($event.target as HTMLTextAreaElement).value)"></textarea>
      </label>
      <label v-if="isNoticeMessageField(workType, 'progress')" class="span-2" :class="fieldClassFor('progress')">
        {{ noticeFieldLabel(workType, "progress") }}
        <textarea :value="draft.progress" placeholder="进度" @input="setField('progress', ($event.target as HTMLTextAreaElement).value)"></textarea>
      </label>
    </div>

    <section v-if="workType === 'power'" class="type-fields">
      <h4>上/下电字段</h4>
      <div class="form-grid">
        <label :class="fieldClassFor('cabinet')"><span>柜号</span><input :value="draft.cabinet" @input="setField('cabinet', ($event.target as HTMLInputElement).value)" /></label>
        <label :class="fieldClassFor('quantity')"><span>数量</span><input :value="draft.quantity" @input="setField('quantity', ($event.target as HTMLInputElement).value)" /></label>
      </div>
    </section>

    <section v-if="workType === 'polling'" class="type-fields">
      <h4>轮巡字段</h4>
      <div class="form-grid">
        <label class="span-2" :class="fieldClassFor('device')"><span>设备</span><input :value="draft.device" @input="setField('device', ($event.target as HTMLInputElement).value)" /></label>
      </div>
    </section>

    <section v-if="workType === 'repair'" class="type-fields">
      <h4>检修字段</h4>
      <div class="form-grid">
        <label :class="fieldClassFor('repair_device')"><span>维修设备</span><input :value="draft.repair_device" @input="setField('repair_device', ($event.target as HTMLInputElement).value)" /></label>
        <label :class="fieldClassFor('repair_fault')"><span>维修故障</span><input :value="draft.repair_fault" @input="setField('repair_fault', ($event.target as HTMLInputElement).value)" /></label>
        <label :class="fieldClassFor('fault_type')"><span>故障类型</span><input :value="draft.fault_type" @input="setField('fault_type', ($event.target as HTMLInputElement).value)" /></label>
        <label :class="fieldClassFor('repair_mode')"><span>维修方式</span><input :value="draft.repair_mode" @input="setField('repair_mode', ($event.target as HTMLInputElement).value)" /></label>
        <label :class="fieldClassFor('discovery')"><span>故障发现方式</span><input :value="draft.discovery" @input="setField('discovery', ($event.target as HTMLInputElement).value)" /></label>
        <label :class="fieldClassFor('symptom')"><span>故障现象</span><input :value="draft.symptom" @input="setField('symptom', ($event.target as HTMLInputElement).value)" /></label>
        <label class="span-2" :class="fieldClassFor('solution')"><span>解决方案</span><textarea :value="draft.solution" @input="setField('solution', ($event.target as HTMLTextAreaElement).value)"></textarea></label>
        <label class="span-2" :class="fieldClassFor('spare_parts')"><span>备件更换情况</span><textarea :value="draft.spare_parts" @input="setField('spare_parts', ($event.target as HTMLTextAreaElement).value)"></textarea></label>
      </div>
    </section>
  </section>
</template>

<script setup lang="ts">
import { isNoticeMessageField, noticeFieldLabel } from "../noticeTemplates";
import SpecialtyInput from "./SpecialtyInput.vue";

type Dict = Record<string, any>;
type ClassMap = Record<string, boolean>;

const props = withDefaults(defineProps<{
  workType: string;
  draft: Dict;
  listIdPrefix: string;
  disabled?: boolean;
  fieldClass?: (field: string) => ClassMap;
}>(), {
  disabled: false,
  fieldClass: undefined,
});

const emit = defineEmits<{
  "set-field": [key: string, value: any];
}>();

function fieldClassFor(field: string): ClassMap {
  return props.fieldClass ? props.fieldClass(field) : {};
}

function setField(key: string, value: any): void {
  emit("set-field", key, value);
}
</script>

<style scoped>
.notice-message-fields {
  display: grid;
  gap: 10px;
  margin-top: 10px;
}

.notice-message-fields.disabled {
  opacity: 0.72;
  pointer-events: none;
}

.notice-message-fields__head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
}

.notice-message-fields__head h3,
.type-fields h4 {
  margin: 0;
  color: #0f2f6a;
  font-size: 14px;
  font-weight: 950;
}

.notice-message-fields__head span {
  border-radius: 999px;
  padding: 5px 9px;
  color: #075bd8;
  background: #eff6ff;
  font-size: 12px;
  font-weight: 900;
  line-height: 1.3;
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
label.field-missing select {
  border-color: #fb7185;
  background-color: #fff1f2;
}

input,
textarea {
  width: 100%;
  border: 1px solid #d8e5f7;
  border-radius: 14px;
  background: #ffffff;
  color: #0f172a;
  font: inherit;
  font-size: 13px;
  font-weight: 800;
}

input {
  min-height: 38px;
  padding: 0 10px;
}

textarea {
  min-height: 62px;
  padding: 8px 10px;
  resize: vertical;
}

.type-fields {
  display: grid;
  gap: 9px;
  border: 1px solid #d8e5f7;
  border-radius: 18px;
  padding: 12px;
  background: linear-gradient(135deg, rgba(248, 251, 255, 0.96), rgba(255, 255, 255, 0.94));
}

@media (max-width: 760px) {
  .notice-message-fields__head {
    align-items: flex-start;
    flex-direction: column;
  }

  .form-grid {
    grid-template-columns: 1fr;
  }
}
</style>
