<template>
  <div ref="parentRef" class="virtual-list">
    <div class="virtual-list__spacer" :style="{ height: `${totalSize}px` }">
      <article
        v-for="virtualRow in virtualRows"
        :key="rowKey(virtualRow)"
        class="notice-row"
        :style="{ transform: `translateY(${virtualRow.start}px)` }"
      >
        <div class="notice-row__main">
          <span class="notice-row__type">{{ rows[virtualRow.index]?.type || "通告" }}</span>
          <strong>{{ rows[virtualRow.index]?.title || "未命名通告" }}</strong>
          <small>{{ rows[virtualRow.index]?.meta || "" }}</small>
        </div>
        <span class="notice-row__status">{{ rows[virtualRow.index]?.status || "待处理" }}</span>
      </article>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";
import { useVirtualizer } from "@tanstack/vue-virtual";

export interface NoticeRow {
  id: string;
  title: string;
  type?: string;
  meta?: string;
  status?: string;
}

const props = defineProps<{
  rows: NoticeRow[];
}>();

const parentRef = ref<HTMLElement | null>(null);
const rowVirtualizerOptions = computed(() => ({
  count: props.rows.length,
  getScrollElement: () => parentRef.value,
  estimateSize: () => 74,
  overscan: 8,
}));
const rowVirtualizer = useVirtualizer(rowVirtualizerOptions);
const virtualRows = computed(() => rowVirtualizer.value.getVirtualItems());
const totalSize = computed(() => rowVirtualizer.value.getTotalSize());

function rowKey(virtualRow: { index: number; key: unknown }): string {
  return props.rows[virtualRow.index]?.id || String(virtualRow.key);
}
</script>

<style scoped>
.virtual-list {
  height: min(420px, 58vh);
  overflow: auto;
  border: 1px solid #d1d5db;
  border-radius: 8px;
  background: #ffffff;
}

.virtual-list__spacer {
  position: relative;
  width: 100%;
}

.notice-row {
  position: absolute;
  left: 0;
  right: 0;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  min-height: 66px;
  padding: 10px 14px;
  border-bottom: 1px solid #e5e7eb;
  background: #ffffff;
}

.notice-row__main {
  min-width: 0;
  display: grid;
  gap: 4px;
}

.notice-row__main strong,
.notice-row__main small {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.notice-row__type,
.notice-row__status {
  width: fit-content;
  border: 1px solid #cbd5e1;
  border-radius: 999px;
  padding: 2px 8px;
  color: #334155;
  font-size: 12px;
  background: #f8fafc;
}

.notice-row__status {
  flex: 0 0 auto;
}
</style>
