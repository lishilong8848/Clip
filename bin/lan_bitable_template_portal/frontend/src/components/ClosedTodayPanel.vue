<template>
  <section v-if="items.length" class="closed-today">
    <div class="panel-head compact">
      <h3>今日结束通告</h3>
      <span>{{ items.length }}</span>
    </div>
    <article v-for="item in items" :key="closedLineKey(item)" class="closed-card">
      <div>
        <strong>{{ item.title || "未命名通告" }}</strong>
        <p>{{ workTypeLabel(item.work_type) }} · {{ item.building || "-" }} · {{ item.ended_at || item.updated_at || "-" }}</p>
      </div>
      <button
        v-if="item.undo_available"
        class="btn ghost"
        :disabled="isLineBusy(undoLineKey(item))"
        :title="isLineBusy(undoLineKey(item)) ? '正在回退当前通告，请等待后台完成' : '回退当前通告的上一步操作'"
        @click="emit('apply-undo', item)"
      >
        {{ isLineBusy(undoLineKey(item)) ? "回退中" : "回退" }}
      </button>
      <span v-if="jobText(undoLineKey(item))" class="job-line" :class="jobClass(undoLineKey(item))">
        {{ jobText(undoLineKey(item)) }}
      </span>
    </article>
  </section>
</template>

<script setup lang="ts">
import { workTypeLabel } from "../noticeTemplates";

type Dict = Record<string, any>;

defineProps<{
  items: Dict[];
  jobText: (key: string) => string;
  jobClass: (key: string) => string;
  isLineBusy: (key: string) => boolean;
  closedLineKey: (item: Dict) => string;
  undoLineKey: (item: Dict) => string;
}>();

const emit = defineEmits<{
  "apply-undo": [item: Dict];
}>();
</script>

<style scoped>
.closed-today {
  display: grid;
  gap: 9px;
  border: 1px solid #d8e5f7;
  border-radius: 20px;
  padding: 10px;
  background: rgba(255, 255, 255, 0.72);
  box-shadow: 0 10px 22px rgba(0, 47, 135, 0.07);
}

.panel-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
}

.panel-head.compact {
  padding: 0 0 4px;
}

.panel-head h3 {
  margin: 0;
  color: #09204a;
  font-size: 14px;
  font-weight: 820;
  letter-spacing: 0;
}

.panel-head span {
  border: 1px solid #cfe0ff;
  border-radius: 999px;
  padding: 3px 8px;
  background: #eff6ff;
  color: #005bff;
  font-size: 12px;
  font-weight: 720;
}

.closed-card {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  align-items: center;
  gap: 8px;
  border: 1px solid #d8e5f7;
  border-radius: 18px;
  padding: 10px;
  background: rgba(255, 255, 255, 0.9);
  box-shadow: 0 8px 20px rgba(20, 70, 138, 0.06);
  transition: border-color 0.14s ease, background-color 0.14s ease, box-shadow 0.14s ease;
}

.closed-card:hover {
  border-color: #bdd2f4;
  background: #ffffff;
  box-shadow: 0 12px 28px rgba(20, 70, 138, 0.1);
}

.closed-card strong {
  min-width: 0;
  display: block;
  overflow: hidden;
  color: #09204a;
  font-size: 13px;
  font-weight: 820;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.closed-card p {
  margin: 3px 0 0;
  color: #5f7189;
  font-size: 12px;
  line-height: 1.5;
}

.btn {
  min-height: 34px;
  border: 1px solid #c5d9f2;
  border-radius: 14px;
  padding: 0 12px;
  background: linear-gradient(135deg, #ffffff, #edf6ff);
  color: #0757d7;
  font-weight: 750;
  white-space: nowrap;
  cursor: pointer;
}

.btn:hover:not(:disabled) {
  border-color: #8dbbfb;
  box-shadow: 0 8px 20px rgba(27, 101, 213, 0.13);
}

.btn:disabled {
  cursor: not-allowed;
  opacity: 0.58;
}

.job-line {
  grid-column: 1 / -1;
  color: #64748b;
  font-size: 13px;
  line-height: 1.45;
  overflow-wrap: anywhere;
}

.job-line.busy {
  color: #1d4ed8;
}

.job-line.success {
  color: #15803d;
}

.job-line.failed {
  color: #b91c1c;
}
</style>
