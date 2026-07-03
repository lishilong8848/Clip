<template>
  <div ref="rootRef" class="refresh-menu">
    <button
      class="btn ghost"
      type="button"
      aria-haspopup="menu"
      :aria-expanded="open"
      title="刷新数据"
      @click.stop="emit('update:open', !open)"
    >
      刷新数据
    </button>
    <div v-if="open" class="refresh-menu-panel" role="menu" @click.stop>
      <div class="refresh-menu-head">
        <strong>刷新数据</strong>
      </div>
      <button
        class="refresh-option"
        type="button"
        role="menuitem"
        :disabled="eventRefreshing || cooldownEvent"
        :title="eventTitle"
        @click="emitRefresh"
      >
        <span>
          <strong>刷新事件</strong>
        </span>
        <b>{{ eventRefreshing ? "读取中" : cooldownEvent ? "稍后再试" : "读取" }}</b>
      </button>
    </div>
  </div>
</template>

<script setup lang="ts">
import { onBeforeUnmount, ref, watch } from "vue";

const props = defineProps<{
  open: boolean;
  eventRefreshing?: boolean;
  cooldownEvent?: boolean;
  eventTitle?: string;
}>();

const emit = defineEmits<{
  "update:open": [value: boolean];
  "refresh-event": [];
}>();

const rootRef = ref<HTMLElement | null>(null);
let listenersAttached = false;

function emitRefresh(): void {
  emit("refresh-event");
  emit("update:open", false);
}

function optionHint(defaultText: string, busy?: boolean, cooldown?: boolean, title?: string): string {
  if (busy || cooldown) return String(title || (busy ? "正在刷新，请稍后。" : "刚刷新过，稍后再试。"));
  return defaultText;
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
.refresh-menu {
  position: relative;
  isolation: isolate;
}

.refresh-menu > .btn {
  min-height: 42px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  padding: 0 16px;
  border: 1px solid rgba(255, 255, 255, 0.34);
  border-radius: 16px;
  background: rgba(255, 255, 255, 0.14);
  color: #ffffff;
  font: inherit;
  font-size: 14px;
  font-weight: 900;
  line-height: 1;
  cursor: pointer;
  box-shadow:
    inset 0 1px 0 rgba(255, 255, 255, 0.2),
    0 10px 22px rgba(4, 46, 145, 0.08);
  backdrop-filter: blur(10px);
  transition:
    transform 0.16s ease,
    background 0.16s ease,
    border-color 0.16s ease;
}

.refresh-menu > .btn:hover {
  border-color: rgba(255, 255, 255, 0.52);
  background: rgba(255, 255, 255, 0.22);
  transform: translateY(-1px);
}

.refresh-menu > .btn:focus-visible {
  outline: none;
  box-shadow:
    0 0 0 3px rgba(255, 255, 255, 0.24),
    0 0 0 5px rgba(48, 128, 255, 0.28);
}

.refresh-menu-panel {
  position: absolute;
  top: calc(100% + 10px);
  right: 0;
  z-index: var(--cf-z-dropdown, 720);
  width: min(316px, calc(100vw - 36px));
  max-height: min(390px, calc(100vh - 132px));
  display: grid;
  gap: 8px;
  padding: 10px;
  border: 1px solid #d8e5f7;
  border-radius: 20px;
  background: rgba(255, 255, 255, 0.98);
  box-shadow: 0 20px 44px rgba(7, 37, 86, 0.24);
  overflow: auto;
  overscroll-behavior: contain;
  isolation: isolate;
}

.refresh-menu-panel::before {
  content: "";
  position: absolute;
  top: -7px;
  right: 26px;
  width: 14px;
  height: 14px;
  border-top: 1px solid rgba(255, 255, 255, 0.42);
  border-left: 1px solid rgba(255, 255, 255, 0.42);
  background: #fff;
  transform: rotate(45deg);
}

.refresh-menu-head {
  position: relative;
  z-index: 1;
  display: grid;
  gap: 3px;
  border: 1px solid rgba(191, 219, 254, 0.82);
  border-radius: 15px;
  background:
    linear-gradient(135deg, rgba(239, 246, 255, 0.96), rgba(255, 255, 255, 0.86)),
    #f8fbff;
  padding: 9px 11px;
}

.refresh-menu-head strong {
  color: #071a39;
  font-size: 14px;
  font-weight: 950;
}

.refresh-menu-head small {
  color: #1d4ed8;
  font-size: 12px;
  font-weight: 850;
  line-height: 1.35;
}

.refresh-option {
  position: relative;
  z-index: 1;
  width: 100%;
  min-height: 48px;
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  align-items: center;
  gap: 10px;
  padding: 8px 10px;
  border: 1px solid #dbe7f5;
  border-radius: 16px;
  color: #0e4fb2;
  background: linear-gradient(135deg, #ffffff, #f7fbff);
  text-align: left;
  font: inherit;
  cursor: pointer;
  box-shadow: 0 8px 18px rgba(15, 86, 228, 0.07);
  transition:
    transform 0.16s ease,
    border-color 0.16s ease,
    box-shadow 0.16s ease,
    background 0.16s ease;
}

.refresh-option:hover:not(:disabled) {
  border-color: #a7c7ff;
  background: #ffffff;
  box-shadow: 0 12px 24px rgba(15, 86, 228, 0.12);
  transform: translateY(-1px);
}

.refresh-option span {
  min-width: 0;
  display: grid;
  gap: 4px;
}

.refresh-option strong {
  color: #071a39;
  font-size: 13px;
  font-weight: 950;
}

.refresh-option small {
  color: #5d7391;
  font-size: 12px;
  font-weight: 800;
  line-height: 1.3;
}

.refresh-option b {
  min-width: 56px;
  border-radius: 999px;
  padding: 6px 9px;
  background: #eff6ff;
  color: #075bd8;
  font-size: 12px;
  font-weight: 950;
  text-align: center;
}

.refresh-option:disabled {
  cursor: not-allowed;
  color: #8193aa;
  background: #f4f7fb;
  box-shadow: none;
}

.refresh-option:disabled strong,
.refresh-option:disabled small {
  color: #8193aa;
}

.refresh-option:disabled b {
  background: #e8eef6;
  color: #687b92;
}
</style>
