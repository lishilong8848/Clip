<template>
  <div ref="parentRef" class="event-virtual-list">
    <div class="event-virtual-spacer" :style="{ height: `${totalSize}px` }">
      <button type="button"
        v-for="virtualRow in virtualRows"
        :key="rowKey(virtualRow)"
        class="event-row"
        :class="[statusTone(rowAt(virtualRow.index)?.status), levelTone(rowAt(virtualRow.index)?.level)]"
        :style="{ transform: `translateY(${virtualRow.start}px)` }"
        @click="emit('select', rowAt(virtualRow.index))"
      >
        <div class="event-row__main">
          <div>
            <div class="event-row__titleline">
              <span class="event-level-chip" :class="levelTone(rowAt(virtualRow.index)?.level)">
                {{ rowAt(virtualRow.index)?.level || "未填写等级" }}
              </span>
              <strong>{{ rowAt(virtualRow.index)?.title || rowAt(virtualRow.index)?.alarm_desc || "未命名事件" }}</strong>
            </div>
            <p>{{ eventSummary(rowAt(virtualRow.index)) }}</p>
          </div>
          <span class="status-pill" :class="statusTone(rowAt(virtualRow.index)?.status)">
            {{ rowAt(virtualRow.index)?.status || "未知" }}
          </span>
        </div>
        <div class="event-row__meta">
          <span>来源：{{ rowAt(virtualRow.index)?.source || "未填写" }}</span>
          <span>楼栋：{{ rowAt(virtualRow.index)?.building || scopeLabel }}</span>
          <span>专业：{{ rowAt(virtualRow.index)?.specialty || "未填写" }}</span>
          <span>发生：{{ rowAt(virtualRow.index)?.occurrence_time || "未填写" }}</span>
          <em>查看详情</em>
        </div>
      </button>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";
import { useVirtualizer } from "@tanstack/vue-virtual";
import type { LooseDict } from "../types";

const props = defineProps<{
  records: LooseDict[];
  scopeLabel: string;
}>();

const emit = defineEmits<{
  select: [record: LooseDict | undefined];
}>();

const parentRef = ref<HTMLElement | null>(null);
const rowVirtualizerOptions = computed(() => ({
  count: props.records.length,
  getScrollElement: () => parentRef.value,
  estimateSize: () => 144,
  overscan: 6,
}));
const rowVirtualizer = useVirtualizer(rowVirtualizerOptions);
const virtualRows = computed(() => rowVirtualizer.value.getVirtualItems());
const totalSize = computed(() => rowVirtualizer.value.getTotalSize());

function rowAt(index: number): LooseDict | undefined {
  return props.records[index];
}

function rowKey(virtualRow: { index: number; key: unknown }): string {
  return eventKey(rowAt(virtualRow.index)) || String(virtualRow.key);
}

function eventKey(item: LooseDict | undefined): string {
  if (!item) return "";
  return String(item.source_record_id || item.record_id || `${item.title}-${item.occurrence_time}`);
}

function eventSummary(item: LooseDict | undefined): string {
  return String(item?.alarm_desc || item?.progress || item?.latest_progress || "无告警描述").trim();
}

function statusTone(status: unknown): string {
  const text = String(status || "");
  if (text.includes("结束")) return "ended";
  if (text.includes("恢复")) return "recovered";
  return "processing";
}

function levelTone(level: unknown): string {
  const text = String(level || "").toUpperCase();
  if (/(I1|一级|紧急|严重|高)/.test(text)) return "critical";
  if (/(I2|I3|二级|三级|中)/.test(text)) return "warning";
  return "normal";
}
</script>

<style scoped>
.event-virtual-list {
  position: relative;
  min-height: 300px;
  height: min(64vh, 680px);
  overflow: auto;
  border-radius: 18px;
  background: rgba(248, 251, 255, 0.68);
}

.event-virtual-spacer {
  position: relative;
  width: 100%;
}

.event-row {
  position: absolute;
  left: 0;
  right: 0;
  overflow: hidden;
  width: 100%;
  height: 132px;
  text-align: left;
  display: grid;
  gap: 10px;
  padding: 15px 16px 15px 20px;
  border: 1px solid #e0e9f7;
  border-radius: 18px;
  background: linear-gradient(135deg, #ffffff 0%, #f8fbff 100%);
  cursor: pointer;
  transition: border-color 0.18s ease, box-shadow 0.18s ease;
}

.event-row::before {
  content: "";
  position: absolute;
  inset: 0 auto 0 0;
  width: 4px;
  background: #1e63ff;
}

.event-row.warning::before {
  background: #f59e0b;
}

.event-row.critical::before {
  background: #e11d48;
}

.event-row.normal.ended::before {
  background: #059669;
}

.event-row:hover {
  border-color: #9fc0ff;
  box-shadow: 0 12px 26px rgba(29, 91, 215, 0.12);
}

.event-row__main {
  position: relative;
  z-index: 1;
  display: flex;
  justify-content: space-between;
  gap: 12px;
  min-width: 0;
}

.event-row__main > div {
  min-width: 0;
}

.event-row__titleline {
  display: flex;
  align-items: center;
  gap: 8px;
  min-width: 0;
}

.event-row strong {
  display: block;
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  color: #071a39;
  font-size: 15px;
  font-weight: 950;
}

.event-row p {
  margin: 5px 0 0;
  color: #516a88;
  line-height: 1.5;
  display: -webkit-box;
  overflow: hidden;
  -webkit-box-orient: vertical;
  -webkit-line-clamp: 2;
}

.event-row__meta {
  position: relative;
  z-index: 1;
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
  color: #5e728f;
  font-size: 12px;
  align-items: center;
}

.event-row__meta span {
  padding: 4px 9px;
  border-radius: 999px;
  background: #f2f7ff;
  color: #45627f;
  font-weight: 850;
}

.event-row__meta em {
  margin-left: auto;
  color: #075bd8;
  font-style: normal;
  font-weight: 950;
  white-space: nowrap;
}

.event-level-chip {
  flex: 0 0 auto;
  padding: 4px 8px;
  border-radius: 999px;
  font-size: 11px;
  font-weight: 950;
  line-height: 1;
}

.event-level-chip.normal {
  background: #edf5ff;
  color: #075bd8;
}

.event-level-chip.warning {
  background: #fff7ed;
  color: #c2410c;
}

.event-level-chip.critical {
  background: #fff1f2;
  color: #be123c;
}

.status-pill {
  flex: 0 0 auto;
  align-self: flex-start;
  padding: 5px 10px;
  border-radius: 999px;
  font-size: 12px;
  font-weight: 950;
}

.status-pill.processing {
  background: #fff7ed;
  color: #c2410c;
}

.status-pill.recovered {
  background: #eff6ff;
  color: #1d4ed8;
}

.status-pill.ended {
  background: #ecfdf5;
  color: #047857;
}

@media (max-width: 760px) {
  .event-row {
    height: 132px;
    padding: 14px;
  }

  .event-row__main {
    display: grid;
    gap: 8px;
  }

  .event-row__titleline {
    align-items: flex-start;
  }

  .event-row__meta em {
    width: 100%;
    margin-left: 0;
  }
}
</style>
