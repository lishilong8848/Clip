<template>
  <div
    v-if="mode !== 'none'"
    class="floating-cell-popover"
    :class="popoverClass"
    :style="style"
    @mousedown.stop
    @click.stop
  >
    <template v-if="mode === 'checkbox'">
      <span class="popover-label">{{ label }}</span>
      <div class="popover-actions compact-actions">
        <button type="button"
          v-for="option in checkboxOptions"
          :key="option.value || option.label"
          :class="{ active: checkboxValue === option.value }"
          @click.stop="emit('select-checkbox', option.value)"
        >
          {{ option.label }}
        </button>
      </div>
    </template>

    <template v-else-if="mode === 'field-time'">
      <span class="popover-label">{{ label }}</span>
      <input
        :value="dateTime"
        type="datetime-local"
        step="3600"
        @input="emit('update:dateTime', ($event.target as HTMLInputElement).value)"
      />
      <button type="button" class="primary-action" @click.stop="emit('fill-date')">填入</button>
    </template>

    <template v-else-if="mode === 'field-completion'">
      <span class="popover-label">{{ label }}</span>
      <div class="popover-actions compact-actions">
        <button type="button" class="primary-action" @click.stop="emit('fill-completion', '已完成[√] 未完成[ ]')">已完成</button>
        <button type="button" @click.stop="emit('fill-completion', '已完成[ ] 未完成[√]')">未完成</button>
      </div>
    </template>

    <template v-else-if="mode === 'raw'">
      <div class="raw-popover-head">
        <span class="popover-label">{{ label }}</span>
        <small>普通单元格</small>
      </div>
      <textarea
        :value="rawValue"
        @input="emit('update:rawValue', ($event.target as HTMLTextAreaElement).value)"
      ></textarea>
      <div class="popover-actions raw-actions">
        <button type="button" @click.stop="emit('copy')">复制</button>
        <button type="button" class="primary-action" @click.stop="emit('paste')">粘贴</button>
        <button type="button" @click.stop="emit('restore')">还原</button>
        <button type="button" class="quiet-action" @click.stop="emit('cancel')">取消</button>
      </div>
    </template>

    <template v-else-if="mode === 'selection'">
      <span class="popover-label selection-label">已选 {{ selectedCount }} 个单元格</span>
      <div class="popover-actions selection-actions">
        <button type="button" @click.stop="emit('copy')">复制</button>
        <button type="button" class="primary-action" @click.stop="emit('paste')">粘贴</button>
        <button type="button" @click.stop="emit('restore')">还原</button>
        <button type="button" class="quiet-action" @click.stop="emit('cancel')">取消</button>
      </div>
    </template>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";

export type MopCellPopoverMode = "none" | "checkbox" | "field-time" | "field-completion" | "raw" | "selection";

const props = defineProps<{
  mode: MopCellPopoverMode;
  style: Record<string, string>;
  label?: string;
  checkboxOptions?: Array<{ label: string; value: string }>;
  checkboxValue?: string;
  dateTime?: string;
  rawValue?: string;
  selectedCount?: number;
}>();

const emit = defineEmits<{
  "select-checkbox": [value: string];
  "update:dateTime": [value: string];
  "fill-date": [];
  "fill-completion": [value: string];
  "update:rawValue": [value: string];
  copy: [];
  paste: [];
  restore: [];
  cancel: [];
}>();

const popoverClass = computed(() => ({
  "floating-cell-fill": props.mode === "checkbox",
  "floating-field-popover": props.mode === "field-time" || props.mode === "field-completion",
  "floating-time-popover": props.mode === "field-time",
  "floating-completion-popover": props.mode === "field-completion",
  "floating-raw-popover": props.mode === "raw",
  "floating-selection-popover": props.mode === "selection",
}));
</script>

<style scoped>
.floating-cell-popover {
  position: fixed;
  z-index: var(--cf-z-cell-popover, 760);
  box-sizing: border-box;
  border: 1px solid rgba(191, 219, 254, 0.94);
  border-radius: 14px;
  background: rgba(255, 255, 255, 0.98);
  box-shadow: 0 12px 28px rgba(7, 37, 86, 0.18);
  backdrop-filter: blur(10px);
  color: #0f172a;
}

.floating-cell-fill,
.floating-field-popover,
.floating-selection-popover {
  display: inline-flex;
  align-items: center;
  gap: 3px;
  width: max-content;
  min-width: 0;
  max-width: min(300px, calc(100vw - 32px));
  border-radius: 999px;
  padding: 2px;
}

.floating-cell-fill {
  max-width: min(198px, calc(100vw - 32px));
}

.floating-field-popover {
  max-width: min(360px, calc(100vw - 32px));
}

.floating-time-popover {
  max-width: min(360px, calc(100vw - 32px));
}

.floating-completion-popover {
  max-width: min(260px, calc(100vw - 32px));
}

.floating-selection-popover {
  max-width: min(300px, calc(100vw - 32px));
}

.popover-label {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  flex: 0 0 auto;
  max-width: none;
  min-height: 22px;
  border-radius: 999px;
  background: #eff6ff;
  padding: 0 7px;
  color: #3156c9;
  font-size: 11px;
  font-weight: 950;
  line-height: 1;
  white-space: nowrap;
}

.floating-cell-fill .popover-label,
.floating-completion-popover .popover-label {
  max-width: 96px;
}

.floating-cell-fill .popover-label {
  max-width: 58px;
  overflow: hidden;
  padding-inline: 6px;
  text-overflow: ellipsis;
}

.floating-time-popover .popover-label {
  min-width: 76px;
}

.floating-completion-popover .popover-label {
  min-width: 76px;
}

.floating-raw-popover .popover-label {
  max-width: 100%;
  justify-content: flex-start;
  overflow: visible;
  text-overflow: clip;
}

.selection-label {
  max-width: 116px;
  color: #0757d7;
  background: #dbeafe;
}

.floating-cell-popover input {
  width: auto;
  min-width: 112px;
  max-width: 128px;
  height: 24px;
  box-sizing: border-box;
  border: 1px solid #d8e5f7;
  border-radius: 999px;
  padding: 0 8px;
  color: #0f172a;
  background: #ffffff;
  font: inherit;
  font-size: 11px;
  font-weight: 800;
  outline: none;
}

.popover-actions {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  min-width: 0;
}

.floating-cell-popover button {
  border: 1px solid transparent;
  border-radius: 999px;
  background: #eef2ff;
  min-height: 22px;
  padding: 2px 6px;
  color: #3156c9;
  font-size: 11px;
  font-weight: 900;
  white-space: nowrap;
  cursor: pointer;
}

.floating-cell-fill button {
  min-width: 38px;
  padding-inline: 7px;
}

.floating-completion-popover button {
  min-width: 44px;
}

.floating-cell-popover button.primary-action,
.floating-cell-popover button.active,
.floating-cell-popover button:hover {
  border-color: transparent;
  background: #1e63ff;
  color: #ffffff;
}

.floating-cell-popover button.quiet-action {
  background: #f8fbff;
  color: #64748b;
}

.floating-raw-popover {
  display: grid;
  gap: 6px;
  border-radius: 16px;
  padding: 7px;
  width: min(300px, calc(100vw - 24px));
}

.raw-popover-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  min-width: 0;
}

.raw-popover-head small {
  flex: 0 0 auto;
  color: #64748b;
  font-size: 11px;
  font-weight: 850;
}

.floating-raw-popover textarea {
  min-height: 56px;
  max-height: min(22vh, 132px);
  resize: vertical;
  box-sizing: border-box;
  border: 1px solid #d8e5f7;
  border-radius: 12px;
  padding: 6px 8px;
  color: #0f172a;
  background: #ffffff;
  font: inherit;
  font-size: 12px;
  font-weight: 800;
  line-height: 1.45;
  outline: none;
}

.raw-actions,
.selection-actions {
  display: flex;
  flex-wrap: wrap;
  gap: 4px;
}

@media (max-width: 560px) {
  .floating-cell-fill,
  .floating-field-popover,
  .floating-selection-popover {
    border-radius: 18px;
    flex-wrap: wrap;
    justify-content: flex-start;
  }

  .floating-cell-popover span {
    flex: 1 1 100%;
    max-width: none;
  }
}
</style>
