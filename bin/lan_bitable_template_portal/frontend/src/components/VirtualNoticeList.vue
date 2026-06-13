<template>
  <div ref="parentRef" class="virtual-list">
    <div v-if="rows.length === 0" class="virtual-list__empty">{{ emptyText }}</div>
    <div class="virtual-list__spacer" :style="{ height: `${totalSize}px` }">
      <article
        v-for="virtualRow in virtualRows"
        :key="rowKey(virtualRow)"
        class="notice-row"
        :class="{ selected: rows[virtualRow.index]?.id === selectedId, queued: rows[virtualRow.index]?.selected, disabled: rows[virtualRow.index]?.disabled, compact }"
        :style="{ transform: `translateY(${virtualRow.start}px)` }"
        :title="rows[virtualRow.index]?.disabledReason || ''"
        @click="handleSelect(rows[virtualRow.index])"
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
  disabled?: boolean;
  disabledReason?: string;
  raw?: unknown;
}

const props = defineProps<{
  rows: NoticeRow[];
  selectedId?: string;
  emptyText?: string;
  compact?: boolean;
  showStatus?: boolean;
}>();

const emit = defineEmits<{
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

function handleSelect(row: NoticeRow | undefined): void {
  if (!row || row.disabled) return;
  emit("select", row);
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

.notice-row.disabled {
  cursor: not-allowed;
  background: #f8fafc;
  border-color: #e2e8f0;
  opacity: 0.78;
}

.notice-row.disabled:hover {
  background: #f8fafc;
  border-color: #e2e8f0;
  box-shadow: none;
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

/* VNET list skin */
.virtual-list {
  border: 1px solid #d8e7f8;
  border-radius: 14px;
  background: #f7fbff;
  scrollbar-color: #9cc7ff #eef6ff;
  scrollbar-width: thin;
}

.virtual-list::-webkit-scrollbar {
  width: 8px;
}

.virtual-list::-webkit-scrollbar-track {
  background: #eef6ff;
  border-radius: 999px;
}

.virtual-list::-webkit-scrollbar-thumb {
  background: linear-gradient(180deg, #9cc7ff, #1678ff);
  border-radius: 999px;
}

.notice-row {
  left: 8px;
  right: 8px;
  isolation: isolate;
  border-color: #d8e7f8;
  border-radius: 12px;
  background: #ffffff;
  box-shadow: 0 8px 18px rgba(22, 78, 151, 0.06);
}

.notice-row::before {
  content: "";
  position: absolute;
  inset: 0 auto 0 0;
  z-index: 0;
  width: 4px;
  border-radius: 12px 0 0 12px;
  background: linear-gradient(180deg, #cfe2f8, #eaf3ff);
}

.notice-row:hover {
  border-color: #9cc7ff;
  background: #f5faff;
  box-shadow: 0 12px 26px rgba(22, 78, 151, 0.1);
}

.notice-row.queued {
  border-color: #9cc7ff;
  background: #f1f7ff;
}

.notice-row.selected {
  border-color: #1678ff;
  background: #edf6ff;
  box-shadow: 0 12px 26px rgba(22, 120, 255, 0.12);
}

.notice-row.selected::before {
  background: linear-gradient(180deg, #0757d7, #21c6e7);
}

.notice-row.disabled {
  background: linear-gradient(180deg, #f8fbff, #f2f6fb);
  color: #7b8ba0;
  opacity: 0.7;
}

.notice-row.disabled .notice-row__type,
.notice-row.disabled .notice-row__status {
  border-color: #d7e2ee;
  background: #f2f6fb;
  color: #7b8ba0;
}

.notice-row__type,
.notice-row__status {
  position: relative;
  z-index: 1;
  border-color: #cfe2f8;
  background: #eaf3ff;
  color: #0757d7;
  font-weight: 800;
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.7);
}

.notice-row__main strong {
  position: relative;
  z-index: 1;
  color: #09204a;
  font-weight: 850;
}

.notice-row__main small {
  position: relative;
  z-index: 1;
  color: #64748b;
}

/* Softer rounded VNET list polish */
.virtual-list {
  border-radius: 18px;
}

.notice-row {
  border-radius: 16px;
}

.notice-row::before {
  border-radius: 16px 0 0 16px;
}

.notice-row__main strong {
  font-weight: 800;
  letter-spacing: 0;
}

.notice-row__main small {
  color: #5f7189;
}

.notice-row__type,
.notice-row__status {
  background: rgba(234, 243, 255, 0.78);
  color: #0b5ed8;
  font-weight: 720;
}

/* Panorama construction-management polish */
.virtual-list {
  border-color: rgba(207, 224, 255, 0.94);
  border-radius: 20px;
  background: rgba(248, 251, 255, 0.94);
}

.notice-row {
  border-color: rgba(216, 231, 248, 0.95);
  border-radius: 18px;
  background: rgba(255, 255, 255, 0.97);
  box-shadow: 0 8px 20px rgba(20, 70, 138, 0.06);
}

.notice-row:hover {
  background: #f8fbff;
  box-shadow: 0 12px 28px rgba(20, 70, 138, 0.1);
}

.notice-row.selected {
  border-color: #3080ff;
  background: #eff6ff;
  box-shadow: inset 4px 0 0 #3080ff, 0 12px 28px rgba(21, 93, 252, 0.12);
}

.notice-row::before {
  border-radius: 18px 0 0 18px;
}

/* Panorama construction-management list skin */
.virtual-list {
  border-color: #d8e5f7;
  background: rgba(255, 255, 255, 0.72);
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.82);
}

.notice-row {
  border-color: #d8e5f7;
  background: rgba(255, 255, 255, 0.9);
  box-shadow: 0 8px 18px rgba(0, 47, 135, 0.06);
}

.notice-row:hover {
  border-color: #bdd2f4;
  background: #ffffff;
}

.notice-row.selected {
  border-color: #005bff;
  background: #eff6ff;
  box-shadow: inset 4px 0 0 #005bff, 0 12px 24px rgba(0, 91, 255, 0.12);
}

.notice-row.selected::before {
  background: linear-gradient(180deg, #1e63ff, #005bff);
}

.notice-row__type,
.notice-row__status {
  border-color: #cfe0ff;
  background: rgba(239, 246, 255, 0.86);
  color: #005bff;
}

.notice-row__status--pending,
.notice-row__status--ongoing {
  border-color: #cfe0ff;
  background: #eff6ff;
  color: #005bff;
}

.notice-row__status--update {
  border-color: #fde68a;
  background: #fffbeb;
  color: #92400e;
}

.notice-row__status--failed {
  border-color: #fecaca;
  background: #fef2f2;
  color: #b91c1c;
}
</style>
