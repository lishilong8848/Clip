<template>
  <div ref="rootRef" class="refresh-menu">
    <button
      class="btn ghost"
      type="button"
      :aria-expanded="open"
      @click.stop="emit('update:open', !open)"
    >
      刷新数据
    </button>
    <div v-if="open" class="refresh-menu-panel" @click.stop>
      <p>低频使用。刷新失败时不会清空页面，会继续显示上次成功数据。</p>
      <button
        class="btn ghost"
        type="button"
        :disabled="loading || cooldownWorkbench"
        :title="workbenchTitle"
        @click="emitRefresh('refresh-workbench')"
      >
        {{ loading ? "刷新中" : cooldownWorkbench ? "刚刷新过，稍后再试" : "刷新本页" }}
      </button>
      <button
        class="btn ghost"
        type="button"
        :disabled="repairRefreshing || cooldownRepair"
        :title="repairTitle"
        @click="emitRefresh('refresh-repair')"
      >
        {{ repairRefreshing ? "检修刷新中" : cooldownRepair ? "刚刷新过，稍后再试" : "刷新检修" }}
      </button>
      <button
        class="btn ghost"
        type="button"
        :disabled="changeRefreshing || cooldownChange"
        :title="changeTitle"
        @click="emitRefresh('refresh-change')"
      >
        {{ changeRefreshing ? "变更刷新中" : cooldownChange ? "刚刷新过，稍后再试" : "刷新变更" }}
      </button>
    </div>
  </div>
</template>

<script setup lang="ts">
import { onBeforeUnmount, ref, watch } from "vue";

const props = defineProps<{
  open: boolean;
  loading?: boolean;
  repairRefreshing?: boolean;
  changeRefreshing?: boolean;
  cooldownWorkbench?: boolean;
  cooldownRepair?: boolean;
  cooldownChange?: boolean;
  workbenchTitle?: string;
  repairTitle?: string;
  changeTitle?: string;
}>();

const emit = defineEmits<{
  "update:open": [value: boolean];
  "refresh-workbench": [];
  "refresh-repair": [];
  "refresh-change": [];
}>();

const rootRef = ref<HTMLElement | null>(null);

function emitRefresh(eventName: "refresh-workbench" | "refresh-repair" | "refresh-change"): void {
  if (eventName === "refresh-workbench") emit("refresh-workbench");
  else if (eventName === "refresh-repair") emit("refresh-repair");
  else emit("refresh-change");
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
.refresh-menu {
  position: relative;
}

.refresh-menu-panel {
  position: absolute;
  top: calc(100% + 10px);
  right: 0;
  z-index: 180;
  width: min(330px, calc(100vw - 36px));
  display: grid;
  gap: 10px;
  padding: 14px;
  border: 1px solid rgba(255, 255, 255, 0.42);
  border-radius: 18px;
  background: rgba(255, 255, 255, 0.98);
  box-shadow: 0 20px 44px rgba(7, 37, 86, 0.24);
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

.refresh-menu-panel p {
  position: relative;
  z-index: 1;
  margin: 0;
  color: #48627f;
  font-size: 12px;
  line-height: 1.6;
}

.refresh-menu-panel .btn {
  position: relative;
  z-index: 1;
  justify-content: flex-start;
  width: 100%;
  border-color: #dbe7f5;
  color: #0e4fb2;
}

.refresh-menu-panel .btn:disabled {
  cursor: not-allowed;
  color: #8193aa;
  background: #f4f7fb;
}
</style>
