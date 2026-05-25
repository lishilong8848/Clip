<template>
  <div ref="parentRef" class="virtual-list">
    <div v-if="rows.length === 0" class="virtual-list__empty">{{ emptyText }}</div>
    <div class="virtual-list__spacer" :style="{ height: `${totalSize}px` }">
      <article
        v-for="virtualRow in virtualRows"
        :key="rowKey(virtualRow)"
        class="notice-row"
        :class="{ selected: rows[virtualRow.index]?.id === selectedId, compact }"
        :style="{ transform: `translateY(${virtualRow.start}px)` }"
        @click="$emit('select', rows[virtualRow.index])"
      >
        <div class="notice-row__main">
          <span class="notice-row__type">{{ rows[virtualRow.index]?.type || "通告" }}</span>
          <strong>{{ rows[virtualRow.index]?.title || "未命名通告" }}</strong>
          <small>{{ rows[virtualRow.index]?.meta || "" }}</small>
        </div>
        <span v-if="showStatus" class="notice-row__status">
          {{ rows[virtualRow.index]?.status || "待处理" }}
        </span>
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
  raw?: unknown;
}

const props = defineProps<{
  rows: NoticeRow[];
  selectedId?: string;
  emptyText?: string;
  compact?: boolean;
  showStatus?: boolean;
}>();

defineEmits<{
  select: [row: NoticeRow | undefined];
}>();

const parentRef = ref<HTMLElement | null>(null);
const rowVirtualizerOptions = computed(() => ({
  count: props.rows.length,
  getScrollElement: () => parentRef.value,
  estimateSize: () => (props.compact ? 62 : 76),
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
  position: relative;
  height: 100%;
  min-height: 180px;
  overflow: auto;
  border-radius: 6px;
  background: #f8fafc;
}

.virtual-list__empty {
  position: absolute;
  inset: 0;
  display: grid;
  place-items: center;
  padding: 18px;
  color: #64748b;
  text-align: center;
  line-height: 1.6;
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
  min-height: 68px;
  padding: 10px 14px;
  border-bottom: 1px solid #e5e7eb;
  background: #ffffff;
  cursor: pointer;
  transition: background-color 0.14s ease, box-shadow 0.14s ease;
}

.notice-row.compact {
  min-height: 56px;
  padding-block: 8px;
}

.notice-row:hover {
  background: #f8fbff;
}

.notice-row.selected {
  background: #eff6ff;
  box-shadow: inset 3px 0 0 #2563eb;
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
