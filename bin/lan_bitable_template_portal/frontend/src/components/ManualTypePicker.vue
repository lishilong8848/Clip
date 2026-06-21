<template>
  <div ref="rootRef" class="manual-create">
    <button
      class="btn ghost"
      type="button"
      :aria-expanded="open"
      title="选择通告类型后创建纯手填草稿"
      @click.stop="emit('update:open', !open)"
    >
      纯手填通告
    </button>
    <div v-if="open" class="manual-type-popover" @click.stop>
      <div class="manual-popover-head">
        <div>
          <strong>选择纯手填通告类型</strong>
          <em>创建草稿，不会直接发送</em>
        </div>
        <p>先选类型再填写，避免把调整、轮巡、维保等通告发错模板。</p>
      </div>
      <div v-if="recentTypes.length" class="manual-recent">
        <span>最近手填</span>
        <div class="manual-recent-actions">
          <button
            v-for="type in recentTypes"
            :key="type.value"
            type="button"
            @click="selectType(type.value)"
          >
            {{ type.label }}
          </button>
        </div>
      </div>
      <div class="manual-type-grid">
        <button
          v-for="type in workTypes"
          :key="type.value"
          type="button"
          @click="selectType(type.value)"
        >
          <span>{{ type.label }}</span>
          <small v-if="prefillTypes.includes(type.value)">创建草稿 · 带入上次内容</small>
          <small v-else>创建草稿 · 空白模板</small>
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
let listenersAttached = false;

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
    if (listenersAttached) return;
    document.addEventListener("mousedown", handlePointerDown, true);
    document.addEventListener("keydown", handleKeydown, true);
    listenersAttached = true;
  } else {
    if (!listenersAttached) return;
    document.removeEventListener("mousedown", handlePointerDown, true);
    document.removeEventListener("keydown", handleKeydown, true);
    listenersAttached = false;
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
  z-index: var(--cf-z-dropdown, 720);
  isolation: isolate;
}

.manual-create > .btn {
  min-height: 42px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  padding: 0 16px;
  border: 1px solid #d8e5f7;
  border-radius: 16px;
  background: rgba(255, 255, 255, 0.94);
  color: #0e4fb2;
  font: inherit;
  font-size: 14px;
  font-weight: 900;
  line-height: 1;
  cursor: pointer;
  box-shadow: 0 10px 22px rgba(15, 86, 228, 0.08);
  transition:
    transform 0.16s ease,
    background 0.16s ease,
    border-color 0.16s ease,
    box-shadow 0.16s ease;
}

.manual-create > .btn:hover {
  border-color: #9cc7ff;
  color: #075bd8;
  background: #f5faff;
  transform: translateY(-1px);
}

.manual-create > .btn:focus-visible {
  outline: none;
  box-shadow:
    0 0 0 3px rgba(21, 93, 252, 0.14),
    0 10px 22px rgba(15, 86, 228, 0.08);
}

.manual-type-popover {
  position: absolute;
  top: calc(100% + 10px);
  right: 0;
  z-index: var(--cf-z-dropdown, 720);
  width: min(390px, calc(100vw - 36px));
  max-height: min(520px, calc(100vh - 132px));
  display: grid;
  gap: 12px;
  padding: 14px;
  border: 1px solid #dbe7f5;
  border-radius: 20px;
  background: rgba(255, 255, 255, 0.98);
  box-shadow: 0 20px 44px rgba(7, 37, 86, 0.2);
  overflow: auto;
  overscroll-behavior: contain;
}

.manual-popover-head {
  border: 1px solid rgba(191, 219, 254, 0.82);
  border-radius: 16px;
  background:
    linear-gradient(135deg, rgba(239, 246, 255, 0.96), rgba(255, 255, 255, 0.86)),
    #f8fbff;
  padding: 10px 12px;
}

.manual-popover-head div {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  min-width: 0;
}

.manual-type-popover strong {
  display: block;
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  color: #071a39;
  font-size: 14px;
  font-weight: 950;
}

.manual-type-popover em {
  flex: 0 0 auto;
  border: 1px solid #bbf7d0;
  border-radius: 999px;
  background: #ecfdf5;
  padding: 4px 8px;
  color: #047857;
  font-size: 11px;
  font-style: normal;
  font-weight: 950;
  line-height: 1;
}

.manual-type-popover p {
  margin: 4px 0 0;
  color: #627895;
  font-size: 12px;
  font-weight: 800;
  line-height: 1.5;
}

.manual-recent,
.manual-type-grid {
  display: grid;
  gap: 8px;
}

.manual-recent {
  grid-template-columns: 72px minmax(0, 1fr);
  align-items: start;
}

.manual-recent span {
  min-width: 0;
  min-height: 34px;
  display: inline-flex;
  align-items: center;
  color: #6c7f98;
  font-size: 12px;
  font-weight: 800;
}

.manual-recent-actions {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  min-width: 0;
}

.manual-type-grid {
  grid-template-columns: repeat(2, minmax(0, 1fr));
}

.manual-recent-actions button,
.manual-type-grid button {
  min-height: 44px;
  display: grid;
  gap: 3px;
  align-content: center;
  padding: 9px 10px;
  border: 1px solid #dbe7f5;
  border-radius: 14px;
  color: #0f356c;
  background: #f8fbff;
  font-weight: 900;
  text-align: left;
  cursor: pointer;
  transition:
    transform 0.16s ease,
    border-color 0.16s ease,
    background 0.16s ease,
    box-shadow 0.16s ease;
}

.manual-recent-actions button:hover,
.manual-type-grid button:hover {
  border-color: #1678ff;
  color: #075bd8;
  background: #eef6ff;
  box-shadow: 0 10px 20px rgba(15, 86, 228, 0.1);
  transform: translateY(-1px);
}

.manual-type-grid button span,
.manual-recent-actions button {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.manual-recent-actions button {
  min-height: 34px;
  max-width: 128px;
  padding: 7px 10px;
}

.manual-type-grid small {
  color: #74869f;
  font-size: 11px;
  font-weight: 700;
}

@media (max-width: 520px) {
  .manual-popover-head div {
    align-items: flex-start;
    flex-direction: column;
  }
}
</style>
