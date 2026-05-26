<template>
  <div ref="parentRef" class="virtual-list">
    <div v-if="rows.length === 0" class="virtual-list__empty">{{ emptyText }}</div>
    <div class="virtual-list__spacer" :style="{ height: `${totalSize}px` }">
      <article
        v-for="virtualRow in virtualRows"
        :key="rowKey(virtualRow)"
        class="notice-row"
        :class="{ selected: rows[virtualRow.index]?.id === selectedId, queued: rows[virtualRow.index]?.selected, compact }"
        :style="{ transform: `translateY(${virtualRow.start}px)` }"
        @click="$emit('select', rows[virtualRow.index])"
      >
        <div class="notice-row__main">
          <span class="notice-row__type">{{ rows[virtualRow.index]?.type || "通告" }}</span>
          <strong>{{ rows[virtualRow.index]?.title || "未命名通告" }}</strong>
          <small>{{ rows[virtualRow.index]?.meta || "" }}</small>
        </div>
        <span
          v-if="showStatus"
          class="notice-row__status"
          :class="rows[virtualRow.index]?.statusTone ? `notice-row__status--${rows[virtualRow.index]?.statusTone}` : ''"
        >
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
  statusTone?: string;
  selected?: boolean;
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
  estimateSize: () => (props.compact ? 64 : 98),
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
  left: 6px;
  right: 6px;
  height: 90px;
  box-sizing: border-box;
  overflow: hidden;
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 10px;
  min-height: 66px;
  padding: 8px 10px 8px 12px;
  border: 1px solid #dbe3ee;
  border-radius: 8px;
  background: #ffffff;
  cursor: pointer;
  box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04);
  transition: border-color 0.14s ease, background-color 0.14s ease, box-shadow 0.14s ease;
}

.notice-row.compact {
  height: 56px;
  min-height: 56px;
  padding-block: 8px;
}

.notice-row:hover {
  background: #f8fbff;
  border-color: #bfdbfe;
}

.notice-row.queued {
  border-color: #bfdbfe;
  background: #f8fbff;
}

.notice-row.selected {
  background: #eff6ff;
  border-color: #2563eb;
  box-shadow: inset 3px 0 0 #2563eb, 0 2px 8px rgba(37, 99, 235, 0.12);
}

.notice-row__main {
  min-width: 0;
  display: grid;
  gap: 2px;
}

.notice-row__main strong,
.notice-row__main small {
  overflow: hidden;
  text-overflow: ellipsis;
}

.notice-row__main strong {
  display: -webkit-box;
  -webkit-line-clamp: 2;
  -webkit-box-orient: vertical;
  line-height: 1.32;
  font-size: 15px;
}

.notice-row__main small {
  white-space: nowrap;
}

.notice-row__type,
.notice-row__status {
  width: fit-content;
  border: 1px solid #cbd5e1;
  border-radius: 999px;
  padding: 1px 7px;
  color: #334155;
  font-size: 11px;
  background: #f8fafc;
}

.notice-row__status {
  flex: 0 0 auto;
  margin-top: 1px;
  max-width: 96px;
  text-align: center;
  white-space: normal;
}

.notice-row__status--pending {
  border-color: #bbf7d0;
  background: #f0fdf4;
  color: #15803d;
}

.notice-row__status--update {
  border-color: #fed7aa;
  background: #fff7ed;
  color: #c2410c;
}

.notice-row__status--ongoing {
  border-color: #bfdbfe;
  background: #eff6ff;
  color: #1d4ed8;
}

.notice-row__status--queued {
  border-color: #c7d2fe;
  background: #eef2ff;
  color: #4338ca;
}

.notice-row__status--failed {
  border-color: #fecaca;
  background: #fef2f2;
  color: #b91c1c;
}
</style>
