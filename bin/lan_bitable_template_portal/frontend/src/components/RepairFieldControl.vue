<template>
  <div
    class="repair-field-control"
    :class="{ wide, compact, invalid: Boolean(error) }"
    :data-field-name="String(field.field_name || '')"
  >
    <label class="repair-field-label" :for="inputId">
      <span>{{ label || field.field_name }}</span>
      <i v-if="required" aria-hidden="true">*</i>
    </label>

    <VnetSelect
      v-if="usesSelect"
      :input-id="inputId"
      :label="label || String(field.field_name || '')"
      :model-value="modelValue"
      :options="options"
      :placeholder="placeholder || `请选择${label || field.field_name || ''}`"
      :required="required"
      :error="error"
      :disabled="disabled"
      :allow-custom="allowCustomSelect"
      @update:model-value="updateValue"
    />
    <div
      v-else-if="percentage"
      class="repair-percentage-control"
      :class="[percentageTone, { 'is-readonly': disabled }]"
      :style="percentageStyle"
    >
      <div class="percentage-summary">
        <span class="percentage-stage">
          <i aria-hidden="true"></i>
          {{ percentageStage }}
        </span>
        <div class="percentage-number-wrap">
          <output
            v-if="disabled"
            :id="inputId"
            data-progress-value
            class="percentage-number percentage-output"
            :aria-label="`${label || field.field_name || '维修进度'}${percentageValue}%`"
          >
            {{ percentageValue }}
          </output>
          <input
            v-else
            :id="inputId"
            data-repair-control
            data-progress-number
            class="percentage-number"
            type="number"
            min="0"
            max="100"
            step="1"
            :value="percentageValue"
            :required="required"
            :disabled="disabled"
            :aria-invalid="Boolean(error)"
            :aria-describedby="error ? errorId : undefined"
            @input="handlePercentageInput"
          />
          <span aria-hidden="true">%</span>
        </div>
      </div>
      <div class="percentage-slider">
        <div
          v-if="disabled"
          class="percentage-readonly-track"
          role="progressbar"
          :aria-label="label || String(field.field_name || '维修进度')"
          aria-valuemin="0"
          aria-valuemax="100"
          :aria-valuenow="percentageValue"
          :aria-valuetext="`${percentageValue}%，${percentageStage}`"
        >
          <i aria-hidden="true"></i>
        </div>
        <input
          v-else
          class="percentage-range"
          type="range"
          min="0"
          max="100"
          step="1"
          :value="percentageValue"
          :disabled="disabled"
          :aria-label="`${label || field.field_name || '维修进度'}百分比滑块`"
          :aria-valuetext="`${percentageValue}%，${percentageStage}`"
          @input="handlePercentageInput"
        />
      </div>
    </div>
    <div v-else-if="isDate" class="repair-date-control">
      <input
        :id="inputId"
        ref="dateInputRef"
        data-repair-control
        :value="modelValue"
        type="datetime-local"
        :required="required"
        :disabled="disabled"
        :aria-invalid="Boolean(error)"
        :aria-describedby="error ? errorId : undefined"
        @input="handleInput"
      />
      <button
        v-if="modelValue && !required && !disabled"
        type="button"
        class="date-control-button date-clear-button"
        :aria-label="`清空${label || field.field_name || '日期时间'}`"
        title="清空"
        @click="updateValue('')"
      >
        <X :size="14" aria-hidden="true" />
      </button>
      <button
        type="button"
        class="date-control-button date-picker-button"
        :disabled="disabled"
        :aria-label="`选择${label || field.field_name || '日期时间'}`"
        title="选择日期时间"
        @click="openDatePicker"
      >
        <CalendarDays :size="16" aria-hidden="true" />
      </button>
    </div>
    <input
      v-else-if="isNumber"
      :id="inputId"
      data-repair-control
      :value="modelValue"
      type="number"
      :min="numberMin"
      :max="numberMax"
      :step="numberStep"
      :required="required"
      :disabled="disabled"
      :aria-invalid="Boolean(error)"
      :aria-describedby="error ? errorId : undefined"
      @input="handleInput"
    />
    <textarea
      v-else-if="isTextarea"
      :id="inputId"
      data-repair-control
      :value="modelValue"
      :rows="compact ? 1 : 2"
      :placeholder="placeholder || '填写字段内容'"
      :required="required"
      :disabled="disabled"
      :aria-invalid="Boolean(error)"
      :aria-describedby="error ? errorId : undefined"
      @input="handleInput"
    />
    <input
      v-else
      :id="inputId"
      data-repair-control
      :value="modelValue"
      type="text"
      :placeholder="placeholder || '填写字段内容'"
      :required="required"
      :disabled="disabled"
      :aria-invalid="Boolean(error)"
      :aria-describedby="error ? errorId : undefined"
      @input="handleInput"
    />

    <small v-if="error" :id="errorId" class="repair-field-error">{{ error }}</small>
  </div>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";
import type { CSSProperties } from "vue";
import { CalendarDays, X } from "lucide-vue-next";
import { repairFieldUsesTextarea } from "../repairManagementUtils";
import type { LooseDict } from "../types";
import VnetSelect from "./VnetSelect.vue";

const props = withDefaults(defineProps<{
  field: LooseDict;
  modelValue?: string;
  inputId: string;
  label?: string;
  required?: boolean;
  error?: string;
  wide?: boolean;
  disabled?: boolean;
  placeholder?: string;
  numberMin?: number;
  numberMax?: number;
  numberStep?: number | string;
  percentage?: boolean;
  selectOptions?: string[] | null;
  allowCustomSelect?: boolean;
  compact?: boolean;
}>(), {
  modelValue: "",
  label: "",
  required: false,
  error: "",
  wide: false,
  disabled: false,
  placeholder: "",
  numberMin: undefined,
  numberMax: undefined,
  numberStep: "any",
  percentage: false,
  selectOptions: null,
  allowCustomSelect: false,
  compact: false,
});

const emit = defineEmits<{
  "update:modelValue": [value: string];
  edited: [];
}>();

const dateInputRef = ref<HTMLInputElement | null>(null);

const options = computed(() => {
  const source = props.selectOptions !== null ? props.selectOptions : props.field.options;
  return Array.isArray(source)
    ? source.map((item: unknown) => String(item || "").trim()).filter(Boolean)
    : [];
});
const usesSelect = computed(() => props.selectOptions !== null || options.value.length > 0);
const uiType = computed(() => String(props.field.ui_type || "").toLowerCase());
const fieldType = computed(() => Number(props.field.field_type || 0));
const isDate = computed(() => fieldType.value === 5 || uiType.value.includes("datetime"));
const isNumber = computed(() => fieldType.value === 2 || uiType.value === "number" || uiType.value === "progress");
const isTextarea = computed(() => repairFieldUsesTextarea(props.field.field_name));
const errorId = computed(() => `${props.inputId}-error`);
const percentageValue = computed(() => {
  const raw = Number(props.modelValue);
  if (!Number.isFinite(raw)) return 0;
  const percentageNumber = raw <= 1 ? raw * 100 : raw;
  return Math.min(100, Math.max(0, Math.round(percentageNumber)));
});
const percentageStage = computed(() => {
  if (percentageValue.value <= 0) return "未开始";
  if (percentageValue.value < 25) return "已开始";
  if (percentageValue.value < 75) return "维修中";
  if (percentageValue.value < 100) return "接近完成";
  return "已完成";
});
const percentageTone = computed(() => {
  if (percentageValue.value <= 0) return "is-idle";
  if (percentageValue.value < 75) return "is-active";
  if (percentageValue.value < 100) return "is-near";
  return "is-complete";
});
const percentageStyle = computed<CSSProperties>(() => ({
  "--repair-progress": `${percentageValue.value}%`,
}));

function updateValue(value: string): void {
  emit("update:modelValue", value);
  emit("edited");
}

function handleInput(event: Event): void {
  updateValue((event.target as HTMLInputElement | HTMLTextAreaElement).value);
}

function handlePercentageInput(event: Event): void {
  const raw = Number((event.target as HTMLInputElement).value);
  const percentageNumber = Number.isFinite(raw) ? Math.min(100, Math.max(0, raw)) : 0;
  updateValue(String(percentageNumber / 100));
}

function openDatePicker(): void {
  const input = dateInputRef.value as (HTMLInputElement & { showPicker?: () => void }) | null;
  if (!input || input.disabled) return;
  input.focus();
  try {
    input.showPicker?.();
  } catch {
    // Some browsers reject showPicker outside a trusted user gesture; focus still exposes the control.
  }
}
</script>

<style scoped>
.repair-field-control {
  min-width: 0;
  display: grid;
  align-content: start;
  gap: 5px;
}

.repair-field-control.wide {
  grid-column: 1 / -1;
}

.repair-field-control.compact {
  gap: 3px;
}

.repair-field-control.compact.wide {
  grid-column: span 2;
}

.repair-field-control.compact .repair-field-label {
  font-size: 11px;
  line-height: 1.2;
}

.repair-field-control.compact input,
.repair-field-control.compact textarea {
  min-height: 32px;
  padding: 5px 9px;
  font-size: 13px;
}

.repair-field-control.compact textarea {
  height: 32px;
  line-height: 1.3;
}

.repair-field-control.compact .repair-percentage-control {
  min-height: 34px;
  padding: 3px 7px;
}

.repair-field-control.compact .percentage-range {
  height: 18px;
  min-height: 18px;
  padding: 0;
}

.repair-field-control.compact .percentage-number-wrap .percentage-number {
  min-height: 27px;
  height: 27px;
  padding: 3px 27px 3px 8px;
}

.repair-field-control.compact .date-control-button {
  width: 26px;
  height: 26px;
}

.repair-field-control.compact .date-picker-button { right: 3px; }
.repair-field-control.compact .date-clear-button { right: 29px; }

.repair-field-control.compact .repair-date-control input {
  padding-right: 62px;
}

.repair-field-control.compact :deep(.vnet-select-trigger),
.repair-field-control.compact :deep(.vnet-combobox-control) {
  min-height: 32px;
  font-size: 13px;
}

.repair-field-label {
  min-width: 0;
  display: flex;
  align-items: center;
  gap: 4px;
  color: #425c79;
  font-size: 12px;
  font-weight: 650;
  line-height: 1.35;
}

.repair-field-label span {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.repair-field-label i {
  color: #d82d43;
  font-size: 14px;
  font-style: normal;
}

input,
textarea {
  width: 100%;
  min-height: 36px;
  border: 1px solid #cbd8e8;
  border-radius: 8px;
  padding: 7px 10px;
  background: #fff;
  color: #142b49;
  font: inherit;
  font-size: 14px;
  font-weight: 500;
  transition: border-color 160ms ease, box-shadow 160ms ease, background 160ms ease;
}

.repair-date-control {
  position: relative;
}

.repair-percentage-control {
  --repair-progress: 0%;
  --repair-progress-color: #1769e8;
  --repair-progress-soft: #eaf2ff;
  min-height: 38px;
  display: grid;
  grid-template-columns: minmax(76px, auto) minmax(80px, 1fr) 76px;
  align-items: center;
  gap: 8px;
  border: 1px solid #c9dbf2;
  border-radius: 10px;
  padding: 4px 8px;
  background: #f8fbff;
  transition: border-color 160ms ease, box-shadow 160ms ease, background 160ms ease;
}

.repair-percentage-control:hover {
  border-color: #9ebfe9;
}

.repair-percentage-control:focus-within {
  border-color: var(--repair-progress-color);
  box-shadow: 0 0 0 3px color-mix(in srgb, var(--repair-progress-color) 15%, transparent);
}

.repair-percentage-control.is-readonly {
  cursor: default;
  background: color-mix(in srgb, var(--repair-progress-soft) 34%, #fff);
}

.repair-percentage-control.is-idle {
  --repair-progress-color: #70839a;
  --repair-progress-soft: #eef2f6;
}

.repair-percentage-control.is-near {
  --repair-progress-color: #e28a13;
  --repair-progress-soft: #fff4df;
}

.repair-percentage-control.is-complete {
  --repair-progress-color: #12966f;
  --repair-progress-soft: #e5f7f0;
}

.percentage-summary {
  display: contents;
}

.percentage-stage {
  grid-column: 1;
  grid-row: 1;
  min-width: 0;
  display: inline-flex;
  align-items: center;
  gap: 6px;
  border-radius: 999px;
  padding: 3px 8px;
  background: var(--repair-progress-soft);
  color: var(--repair-progress-color);
  font-size: 11px;
  font-weight: 800;
  line-height: 1.2;
}

.percentage-stage i {
  width: 6px;
  height: 6px;
  flex: 0 0 6px;
  border-radius: 50%;
  background: currentColor;
  box-shadow: 0 0 0 3px color-mix(in srgb, currentColor 14%, transparent);
}

.percentage-slider {
  grid-column: 2;
  grid-row: 1;
  min-width: 0;
  display: block;
}

.percentage-readonly-track {
  position: relative;
  width: 100%;
  height: 6px;
  overflow: hidden;
  border-radius: 999px;
  background: #dce6f2;
}

.percentage-readonly-track > i {
  position: absolute;
  inset: 0 auto 0 0;
  width: var(--repair-progress);
  border-radius: inherit;
  background: var(--repair-progress-color);
  transition: width 180ms ease;
}

.percentage-range {
  appearance: none;
  -webkit-appearance: none;
  width: 100%;
  height: 18px;
  min-height: 18px;
  border: 0;
  border-radius: 999px;
  padding: 0;
  background: linear-gradient(
    to right,
    var(--repair-progress-color) 0,
    var(--repair-progress-color) var(--repair-progress),
    #dce6f2 var(--repair-progress),
    #dce6f2 100%
  );
  background-size: 100% 5px;
  background-position: center;
  background-repeat: no-repeat;
  box-shadow: none;
  cursor: pointer;
}

.percentage-range:focus,
.percentage-range:focus-visible {
  outline: 0;
  box-shadow: none;
}

.percentage-range::-webkit-slider-thumb {
  width: 16px;
  height: 16px;
  appearance: none;
  -webkit-appearance: none;
  border: 3px solid #fff;
  border-radius: 50%;
  background: var(--repair-progress-color);
  box-shadow: 0 1px 5px rgba(23, 70, 126, 0.28);
}

.percentage-range::-moz-range-thumb {
  width: 11px;
  height: 11px;
  border: 3px solid #fff;
  border-radius: 50%;
  background: var(--repair-progress-color);
  box-shadow: 0 1px 5px rgba(23, 70, 126, 0.28);
}

.percentage-number-wrap {
  grid-column: 3;
  grid-row: 1;
  position: relative;
  width: 76px;
}

.percentage-number-wrap .percentage-number,
.percentage-number-wrap .percentage-output {
  width: 100%;
  min-height: 27px;
  height: 27px;
  display: flex;
  align-items: center;
  justify-content: flex-end;
  border-color: color-mix(in srgb, var(--repair-progress-color) 28%, #d3dfed);
  border-style: solid;
  border-width: 1px;
  border-radius: 8px;
  padding-left: 8px;
  padding-top: 3px;
  padding-bottom: 3px;
  padding-right: 27px;
  background: #fff;
  color: var(--repair-progress-color);
  font-size: 13px;
  font-weight: 800;
  font-variant-numeric: tabular-nums;
  text-align: right;
}

.percentage-number-wrap .percentage-output {
  cursor: default;
}

.percentage-number-wrap input[type="number"] {
  appearance: textfield;
  -moz-appearance: textfield;
}

.percentage-number-wrap input[type="number"]::-webkit-inner-spin-button,
.percentage-number-wrap input[type="number"]::-webkit-outer-spin-button {
  margin: 0;
  appearance: none;
  -webkit-appearance: none;
}

.percentage-number-wrap > span {
  position: absolute;
  top: 50%;
  right: 9px;
  color: var(--repair-progress-color);
  font-size: 12px;
  font-weight: 800;
  transform: translateY(-50%);
  pointer-events: none;
}

.repair-date-control input {
  padding-right: 70px;
}

.repair-date-control input::-webkit-calendar-picker-indicator {
  opacity: 0;
  pointer-events: none;
}

.date-control-button {
  position: absolute;
  top: 50%;
  width: 30px;
  height: 30px;
  display: grid;
  place-items: center;
  border: 0;
  border-radius: 7px;
  padding: 0;
  background: transparent;
  color: #47709e;
  transform: translateY(-50%);
  cursor: pointer;
}

.date-control-button:hover:not(:disabled),
.date-control-button:focus-visible {
  outline: 0;
  background: #edf4ff;
  color: #145fc9;
}

.date-control-button:disabled {
  cursor: not-allowed;
  opacity: 0.45;
}

.date-picker-button { right: 3px; }
.date-clear-button { right: 33px; }

textarea {
  min-height: 72px;
  resize: vertical;
  line-height: 1.5;
}

input:hover:not(:disabled),
textarea:hover:not(:disabled) {
  border-color: #9eb7d6;
}

input:focus,
textarea:focus {
  border-color: #1e63ff;
  outline: 0;
  box-shadow: 0 0 0 3px rgba(30, 99, 255, 0.14);
}

input[aria-invalid="true"],
textarea[aria-invalid="true"] {
  border-color: #e1495b;
}

input:disabled,
textarea:disabled {
  cursor: not-allowed;
  background: #f3f6fa;
  color: #8796a9;
}

.repair-field-error {
  color: #c92f43;
  font-size: 11px;
  font-weight: 600;
  line-height: 1.35;
}

@media (max-width: 760px) {
  .repair-field-control.compact.wide {
    grid-column: 1 / -1;
  }
}
</style>
