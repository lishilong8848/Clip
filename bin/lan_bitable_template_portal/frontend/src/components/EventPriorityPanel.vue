<template>
  <aside v-if="detail" class="priority-panel">
    <div class="surface-head tight">
      <div>
        <h3>{{ detailScopeLabel }}重点与挂起事件</h3>
      </div>
    </div>
    <div class="priority-tabs">
      <span>重点 {{ highPriorityCount }}</span>
      <span>挂起 {{ pendingCount }}</span>
      <span>全部事件 {{ eventsCount }}</span>
    </div>
    <div v-if="priorityEvents.length" class="priority-list">
      <button
        v-for="item in priorityEvents"
        :key="eventKey(item)"
        type="button"
        class="priority-row"
        @click="$emit('select', item)"
      >
        <span class="priority-level" :class="levelTone(item.level)">{{ item.level || "P2" }}</span>
        <div>
          <small>{{ item.building || scopeLabel }} · {{ item.occurrence_time || "未填写时间" }}</small>
          <strong>{{ item.title || item.alarm_desc || "未命名事件" }}</strong>
        </div>
        <em :class="statusTone(item.status)">{{ item.status || "未知" }}</em>
      </button>
    </div>
    <div v-else class="event-empty compact">
      <strong>暂无重点或挂起事件</strong>
    </div>
  </aside>

  <aside v-else class="priority-panel overview-panel">
    <div class="surface-head tight">
      <div>
        <h3>态势查看方式</h3>
      </div>
    </div>
    <div class="overview-guide-grid">
      <span>
        <small>可查看楼栋</small>
        <strong>{{ allowedCount }}</strong>
      </span>
      <span>
        <small>本月事件</small>
        <strong>{{ Number(overviewStats.total || 0) }}</strong>
      </span>
      <span>
        <small>挂起</small>
        <strong>{{ Number(overviewStats.pending || 0) }}</strong>
      </span>
      <span>
        <small>重点</small>
        <strong>{{ Number(overviewStats.high_level || 0) }}</strong>
      </span>
    </div>
  </aside>
</template>

<script setup lang="ts">
import {
  eventKey,
  eventLevelTone as levelTone,
  eventStatusTone as statusTone,
} from "../eventManagementUtils";
import type { LooseDict } from "../types";

defineProps<{
  detail: boolean;
  detailScopeLabel: string;
  scopeLabel: string;
  priorityEvents: LooseDict[];
  highPriorityCount: number;
  pendingCount: number;
  eventsCount: number;
  overviewStats: LooseDict;
  allowedCount: number;
}>();

defineEmits<{
  select: [item: LooseDict];
}>();
</script>

<style scoped>
.priority-panel {
  min-width: 0;
  border: 1px solid rgba(216, 229, 247, 0.92);
  border-radius: 24px;
  background: rgba(255, 255, 255, 0.74);
  padding: 18px;
}

.surface-head {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 18px;
}

.surface-head.tight {
  align-items: center;
}

.surface-head h3 {
  margin: 0 0 4px;
  color: #071a39;
  font-size: 20px;
  font-weight: 950;
}

.priority-tabs {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 8px;
  margin-top: 16px;
}

.priority-tabs span {
  min-width: 0;
  overflow: hidden;
  padding: 10px 12px;
  border-radius: 999px;
  background: #edf5ff;
  color: #1d4ed8;
  font-size: 12px;
  font-weight: 950;
  text-align: center;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.priority-tabs span:first-child { background: #fff1f2; color: #be123c; }
.priority-tabs span:nth-child(2) { background: #fff7ed; color: #c2410c; }

.overview-panel {
  align-content: start;
}

.overview-guide-grid {
  margin-top: 16px;
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 10px;
}

.overview-guide-grid span {
  min-width: 0;
  display: grid;
  gap: 5px;
  padding: 14px;
  border: 1px solid #d8e5f7;
  border-radius: 18px;
  background: rgba(248, 251, 255, 0.92);
}

.overview-guide-grid small {
  color: #6b7f9d;
  font-size: 12px;
  font-weight: 850;
}

.overview-guide-grid strong {
  color: #0e4fb2;
  font-size: 26px;
  font-weight: 950;
  line-height: 1;
}

.priority-list {
  margin-top: 12px;
  display: grid;
  gap: 8px;
}

.priority-row {
  min-width: 0;
  display: grid;
  grid-template-columns: 46px minmax(0, 1fr) auto;
  align-items: center;
  gap: 12px;
  padding: 11px 0;
  border: 0;
  border-bottom: 1px solid #edf2f8;
  background: transparent;
  text-align: left;
  cursor: pointer;
}

.priority-level {
  min-width: 42px;
  min-height: 28px;
  display: grid;
  place-items: center;
  border-radius: 999px;
  background: #eff6ff;
  color: #1d4ed8;
  font-size: 12px;
  font-weight: 950;
}

.priority-level.warning { background: #fff7ed; color: #c2410c; }
.priority-level.critical { background: #fff1f2; color: #be123c; }

.priority-row div {
  min-width: 0;
  display: grid;
  gap: 3px;
}

.priority-row small,
.priority-row em {
  color: #6b7f9d;
  font-size: 12px;
  font-style: normal;
  font-weight: 850;
}

.priority-row strong {
  min-width: 0;
  overflow: hidden;
  color: #071a39;
  font-size: 14px;
  font-weight: 950;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.priority-row em.processing { color: #c2410c; }
.priority-row em.recovered { color: #1d4ed8; }
.priority-row em.ended { color: #047857; }

.event-empty {
  color: #5e728f;
  display: grid;
  place-items: center;
  min-height: 220px;
  border: 1px dashed #cbd9ec;
  border-radius: 18px;
  background: rgba(248, 251, 255, 0.88);
  text-align: center;
}

.event-empty.compact {
  min-height: 120px;
}

.event-empty strong {
  color: #10294f;
  font-size: 15px;
}
</style>
