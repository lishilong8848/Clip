<template>
  <div ref="rootRef" class="manual-create">
    <button class="btn ghost" type="button" :aria-expanded="open" @click.stop="emit('update:open', !open)">
      纯手填
    </button>
    <div v-if="open" class="manual-type-popover" @click.stop>
      <strong>选择纯手填通告类型</strong>
      <p>先选类型再填写，避免在维保页误发成维保通告。</p>
      <div v-if="recentTypes.length" class="manual-recent">
        <span>最近使用</span>
        <button
          v-for="type in recentTypes"
          :key="type.value"
          type="button"
          @click="selectType(type.value)"
        >
          {{ type.label }}
        </button>
      </div>
      <div class="manual-type-grid">
        <button
          v-for="type in workTypes"
          :key="type.value"
          type="button"
          @click="selectType(type.value)"
        >
          <span>{{ type.label }}</span>
          <small v-if="prefillTypes.includes(type.value)">带入上次内容</small>
          <small v-else>空白模板</small>
        </button>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { onBeforeUnmount, ref, watch } from "vue";
import type { WorkTypeOption, WorkTypeValue } from "../types";

const props = defineProps<{
  open: boolean;
  workTypes: WorkTypeOption[];
  recentTypes: WorkTypeOption[];
  prefillTypes: string[];
}>();

const emit = defineEmits<{
  "update:open": [value: boolean];
  select: [value: WorkTypeValue];
}>();

const rootRef = ref<HTMLElement | null>(null);

function selectType(value: WorkTypeValue): void {
  emit("select", value);
  emit("update:open", false);
}

function handlePointerDown(event: MouseEvent): void {
  const root = rootRef.value;
  if (!root || root.contains(event.target as Node)) return;
  emit("update:open", false);
}

function handleKeydown(event: KeyboardEvent): void {
  if (event.key === "Escape") emit("update:open", false);
}

function setListeners(enabled: boolean): void {
  if (enabled) {
    document.addEventListener("mousedown", handlePointerDown, true);
    document.addEventListener("keydown", handleKeydown, true);
  } else {
    document.removeEventListener("mousedown", handlePointerDown, true);
    document.removeEventListener("keydown", handleKeydown, true);
  }
}

watch(
  () => props.open,
  (open) => setListeners(open),
  { immediate: true },
);

onBeforeUnmount(() => setListeners(false));
</script>

<style scoped>
.manual-create {
  position: relative;
}

.manual-type-popover {
  position: absolute;
  top: calc(100% + 10px);
  right: 0;
  z-index: 170;
  width: min(360px, calc(100vw - 36px));
  display: grid;
  gap: 12px;
  padding: 14px;
  border: 1px solid #dbe7f5;
  border-radius: 18px;
  background: rgba(255, 255, 255, 0.98);
  box-shadow: 0 20px 44px rgba(7, 37, 86, 0.2);
}

.manual-type-popover strong {
  color: #071a39;
}

.manual-type-popover p {
  margin: 0;
  color: #627895;
  font-size: 12px;
  line-height: 1.6;
}

.manual-recent,
.manual-type-grid {
  display: grid;
  gap: 8px;
}

.manual-recent {
  grid-template-columns: auto repeat(2, minmax(0, 1fr));
  align-items: center;
}

.manual-recent span {
  color: #6c7f98;
  font-size: 12px;
  font-weight: 800;
}

.manual-type-grid {
  grid-template-columns: repeat(2, minmax(0, 1fr));
}

.manual-recent button,
.manual-type-grid button {
  min-height: 44px;
  display: grid;
  gap: 3px;
  align-content: center;
  padding: 9px 10px;
  border: 1px solid #dbe7f5;
  border-radius: 12px;
  color: #0f356c;
  background: #f8fbff;
  font-weight: 900;
  text-align: left;
  cursor: pointer;
}

.manual-recent button:hover,
.manual-type-grid button:hover {
  border-color: #1678ff;
  color: #075bd8;
  background: #eef6ff;
}

.manual-type-grid small {
  color: #74869f;
  font-size: 11px;
  font-weight: 700;
}
</style>
