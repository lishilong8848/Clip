<template>
  <section v-if="items.length" class="recent-undo-panel">
    <div class="panel-head compact">
      <h3>近三天可回退</h3>
      <span>{{ filteredItems.length }}/{{ items.length }}</span>
    </div>
    <div class="undo-filter">
      <button
        v-for="option in filterOptions"
        :key="option.value"
        type="button"
        :class="{ active: modelValue === option.value }"
        @click="emit('update:modelValue', option.value)"
      >
        {{ option.label }} {{ filterCounts[option.value] || 0 }}
      </button>
    </div>
    <article v-for="item in filteredItems" :key="undoLineKey(item)" class="undo-card">
      <div>
        <strong>{{ item.title || "未命名通告" }}</strong>
        <p>
          <span class="undo-action-chip">{{ undoActionLabel(item) }}</span>
          {{ workTypeLabel(item.work_type) }} · {{ item.building || "-" }} · {{ formatUndoTime(item.undo_created_at) }}
        </p>
      </div>
      <button class="btn ghost" :disabled="isLineBusy(undoLineKey(item))" @click="emit('apply', item)">
        {{ undoButtonLabel(item) }}
      </button>
      <span class="job-line" :class="jobClass(undoLineKey(item))">{{ jobText(undoLineKey(item)) }}</span>
    </article>
  </section>
</template>

<script setup lang="ts">
import { computed } from "vue";
import { workTypeLabel } from "../noticeTemplates";

type Dict = Record<string, any>;

const props = defineProps<{
  items: Dict[];
  modelValue: string;
  jobText: (key: string) => string;
  jobClass: (key: string) => string;
  isLineBusy: (key: string) => boolean;
}>();

const emit = defineEmits<{
  "update:modelValue": [value: string];
  apply: [item: Dict];
}>();

const filterOptions = [
  { value: "all", label: "全部" },
  { value: "delete", label: "删除" },
  { value: "end", label: "结束" },
  { value: "update", label: "更新" },
];

const filterCounts = computed(() => {
  const counts: Record<string, number> = { all: props.items.length, delete: 0, end: 0, update: 0 };
  for (const item of props.items) {
    const action = undoActionType(item);
    if (action in counts) counts[action] += 1;
  }
  return counts;
});

const filteredItems = computed(() => {
  if (props.modelValue === "all") return props.items;
  return props.items.filter((item) => undoActionType(item) === props.modelValue);
});

function undoLineKey(item: Dict): string {
  return `undo:${item.undo_id || item.active_item_id || item.target_record_id || item.record_id || item.key || item.title || ""}`;
}

function formatUndoTime(value: any): string {
  const numeric = Number(value || 0);
  const date = numeric > 0 ? new Date(numeric * 1000) : new Date(String(value || ""));
  if (Number.isNaN(date.getTime())) return "-";
  return date.toLocaleString("zh-CN", { hour12: false });
}

function undoActionType(item: Dict): string {
  return String(item?.undo_action_type || item?.action_type || "").trim().toLowerCase();
}

function undoActionLabel(item: Dict): string {
  const action = undoActionType(item);
  if (action === "delete") return "删除";
  if (action === "end") return "结束";
  if (action === "update") return "更新";
  return "上一步";
}

function undoButtonLabel(item: Dict): string {
  return `回退${undoActionLabel(item)}`;
}
</script>

<style scoped>
.recent-undo-panel {
  display: grid;
  gap: 8px;
  border: 1px solid #dbeafe;
  border-radius: 10px;
  padding: 10px;
  background: #f8fbff;
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

.panel-head.compact h3 {
  margin: 0;
  font-size: 14px;
}

.panel-head span {
  flex: 0 0 auto;
  padding: 3px 8px;
  border-radius: 999px;
  background: #eef2ff;
  color: #3730a3;
  font-size: 12px;
}

.undo-filter {
  display: flex;
  gap: 6px;
  flex-wrap: wrap;
}

.undo-filter button,
.btn {
  border: 1px solid #cbd5e1;
  border-radius: 6px;
  padding: 8px 12px;
  background: #ffffff;
  color: #0f172a;
  font-size: 14px;
  line-height: 1;
  cursor: pointer;
}

.undo-filter button {
  min-height: 30px;
  padding: 6px 10px;
  border-color: #dbeafe;
  color: #334155;
  font-size: 12px;
}

.undo-filter button.active {
  border-color: #2563eb;
  background: #eff6ff;
  color: #1d4ed8;
  font-weight: 700;
}

.btn:disabled {
  cursor: not-allowed;
  opacity: 0.58;
}

.undo-card {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  align-items: center;
  gap: 8px;
  padding: 9px 10px;
  border: 1px solid #dbe3ee;
  border-radius: 8px;
  background: #ffffff;
}

.undo-card p {
  margin: 3px 0 0;
  color: #64748b;
  font-size: 12px;
}

.undo-action-chip {
  display: inline-flex;
  align-items: center;
  margin-right: 6px;
  padding: 2px 6px;
  border: 1px solid #bfdbfe;
  border-radius: 999px;
}

.job-line {
  grid-column: 1 / -1;
  color: #64748b;
  font-size: 13px;
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

/* VNET undo panel skin */
.recent-undo-panel {
  border-color: #d8e7f8;
  border-radius: 14px;
  background: #f7fbff;
  box-shadow: 0 12px 30px rgba(22, 78, 151, 0.08);
}

.recent-undo-panel::before {
  content: "";
  height: 3px;
  border-radius: 999px;
  background: linear-gradient(90deg, #0757d7, #1678ff 55%, #21c6e7);
}

.panel-head.compact h3 {
  color: #09204a;
  font-weight: 900;
}

.panel-head span,
.undo-action-chip {
  border-color: #cfe2f8;
  background: #eaf3ff;
  color: #0757d7;
  font-weight: 800;
}

.undo-filter button,
.btn {
  border-color: #c5d9f2;
  border-radius: 9px;
  color: #09204a;
  font-weight: 750;
}

.undo-filter button.active {
  border-color: transparent;
  background: linear-gradient(135deg, #0757d7, #1678ff);
  color: #ffffff;
}

.undo-card {
  border-color: #d8e7f8;
  border-radius: 12px;
  background: #ffffff;
  box-shadow: 0 8px 18px rgba(22, 78, 151, 0.06);
  transition: border-color 0.14s ease, background-color 0.14s ease, box-shadow 0.14s ease;
}

.undo-card:hover {
  border-color: #9cc7ff;
  background: #f5faff;
  box-shadow: 0 12px 26px rgba(22, 78, 151, 0.1);
}

.undo-card strong {
  color: #09204a;
}

.undo-card .btn {
  border-color: transparent;
  background: linear-gradient(135deg, #ffffff, #edf6ff);
  color: #0757d7;
}

.undo-card .btn:hover:not(:disabled) {
  border-color: #8dbbfb;
  box-shadow: 0 8px 20px rgba(27, 101, 213, 0.13);
}

/* Softer rounded VNET undo polish */
.recent-undo-panel {
  border-radius: 18px;
}

.undo-card {
  border-radius: 16px;
}

.undo-filter button,
.btn {
  border-radius: 12px;
}

.panel-head.compact h3,
.undo-card strong {
  font-weight: 820;
  letter-spacing: 0;
}

.undo-card p {
  color: #5f7189;
}

.panel-head span,
.undo-action-chip {
  background: rgba(234, 243, 255, 0.78);
  color: #0b5ed8;
  font-weight: 720;
}

/* Panorama construction-management polish */
.recent-undo-panel {
  border-color: rgba(207, 224, 255, 0.94);
  border-radius: 20px;
  background: rgba(248, 251, 255, 0.94);
  box-shadow: 0 10px 26px rgba(20, 70, 138, 0.08);
}

.undo-card {
  border-color: rgba(216, 231, 248, 0.95);
  border-radius: 18px;
  background: rgba(255, 255, 255, 0.97);
  box-shadow: 0 8px 20px rgba(20, 70, 138, 0.06);
}

.undo-card:hover {
  box-shadow: 0 12px 28px rgba(20, 70, 138, 0.1);
}

.undo-filter button,
.btn {
  border-radius: 14px;
}

/* Panorama construction-management undo skin */
.recent-undo-panel {
  border-color: #d8e5f7;
  background: rgba(255, 255, 255, 0.72);
  box-shadow: 0 10px 22px rgba(0, 47, 135, 0.07);
}

.recent-undo-panel::before {
  background: linear-gradient(90deg, #1e63ff, #005bff);
}

.undo-card {
  border-color: #d8e5f7;
  background: rgba(255, 255, 255, 0.9);
}

.undo-card:hover {
  border-color: #bdd2f4;
  background: #ffffff;
}

.undo-filter button.active {
  background: linear-gradient(135deg, #1e63ff, #1554df);
}

.panel-head span,
.undo-action-chip {
  border-color: #cfe0ff;
  background: #eff6ff;
  color: #005bff;
}
</style>


