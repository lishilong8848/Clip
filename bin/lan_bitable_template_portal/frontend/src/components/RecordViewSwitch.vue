<script setup lang="ts">
import { LayoutGrid, List } from 'lucide-vue-next'

type RecordView = 'cards' | 'table'

const props = defineProps<{
  modelValue: RecordView
}>()

const emit = defineEmits<{
  (e: 'update:modelValue', value: RecordView): void
}>()

const options = [
  { value: 'cards', label: '卡片显示', icon: LayoutGrid },
  { value: 'table', label: '表单显示', icon: List },
] as const

function select(value: RecordView) {
  if (value !== props.modelValue) emit('update:modelValue', value)
}
</script>

<template>
  <div class="record-view-switch" role="group" aria-label="显示方式">
    <button
      v-for="option in options"
      :key="option.value"
      type="button"
      class="rvs-btn"
      :class="{ 'is-active': modelValue === option.value }"
      :title="option.label"
      :aria-label="option.label"
      :aria-pressed="modelValue === option.value"
      @click="select(option.value)"
    >
      <component :is="option.icon" :size="16" aria-hidden="true" focusable="false" />
    </button>
  </div>
</template>

<style scoped>
.record-view-switch {
  display: inline-flex;
  align-items: center;
  gap: 2px;
  padding: 2px;
  border: 1px solid var(--v2-border);
  border-radius: 8px;
  background: var(--soft-face);
  box-sizing: border-box;
}

.rvs-btn {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 32px;
  height: 32px;
  margin: 0;
  padding: 0;
  border: 1px solid transparent;
  border-radius: 6px;
  background: transparent;
  color: var(--v2-muted);
  cursor: pointer;
  line-height: 0;
  transition: background-color 120ms ease, color 120ms ease, border-color 120ms ease;
  -webkit-tap-highlight-color: transparent;
}

.rvs-btn:hover {
  color: var(--v2-ink);
}

.rvs-btn.is-active {
  background: var(--v2-palette-selection);
  color: var(--v2-palette-accent-text);
  border-color: var(--v2-border);
}

.rvs-btn:focus-visible {
  outline: 2px solid var(--v2-palette-accent-text);
  outline-offset: 1px;
}

@media (prefers-reduced-motion: reduce) {
  .rvs-btn {
    transition: none;
  }
}

@media (pointer: coarse) {
  .rvs-btn {
    width: 44px;
    height: 44px;
  }
}
</style>
