<template>
  <div class="target-choice-panel">
    <div>
      <strong>请选择要{{ selection.actionLabel }}的{{ workTypeName }}通告</strong>
      <p>原文状态为“{{ selection.actionLabel }}”。请选择它对应的已上传通告；如没有找到，也可以先选择原始事项继续处理。</p>
      <p v-if="selection.totalMatched" class="target-count-line">
        当前显示 {{ selection.returnedCount }} 条，共找到 {{ selection.totalMatched }} 条相近通告{{ selection.limited ? "，请按标题、时间和内容选择" : "" }}。
      </p>
    </div>
    <div class="target-choice-layout">
      <div class="target-choice-column">
        <label class="candidate-search">
          <span>搜索已上传通告</span>
          <input v-model="targetSearchModel" type="search" placeholder="标题、楼栋、时间、内容" />
        </label>
        <p class="candidate-count">当前显示 {{ targetCandidates.length }} / {{ rawTargetCount }} 条</p>
        <p v-if="selectedTargetHiddenBySearch" class="target-filter-note">
          已保留已选通告；当前搜索条件暂时隐藏了它，可清空搜索查看。
        </p>
        <div class="target-choice-list">
          <p v-if="!rawTargetCount" class="target-empty-line">没有找到同名已上传通告，可先选择原始事项继续处理。</p>
          <p v-else-if="!targetCandidates.length" class="target-empty-line">没有匹配的已上传通告，请调整搜索条件。</p>
          <button
            v-for="item in targetCandidates"
            :key="targetCandidateId(item)"
            class="target-choice"
            :class="{ active: selectedTargetId === targetCandidateId(item) }"
            @mouseenter="$emit('preview-target', item)"
            @focus="$emit('preview-target', item)"
            @click="$emit('select-target', item)"
          >
            <strong>{{ item.title || "未命名通告" }}</strong>
            <span>{{ item.building || "-" }} · {{ item.status || "未标记状态" }} · {{ item.start_time || "-" }} 至 {{ item.end_time || "-" }}</span>
            <small>{{ item.match_reason || (item.date_matched ? "时间匹配" : "按名称匹配") }}</small>
          </button>
        </div>
      </div>
      <aside v-if="activeTargetCandidate" class="target-detail-popover">
        <div class="target-detail-head">
          <strong>{{ activeTargetCandidate.title || `${workTypeName}通告` }}</strong>
          <span>{{ activeTargetCandidate.building || "-" }} · {{ activeTargetCandidate.status || "未标记状态" }}</span>
          <small>{{ activeTargetCandidate.match_reason || "" }}</small>
        </div>
        <dl class="target-detail-grid">
          <template v-for="row in detailRowsFor(activeTargetCandidate)" :key="row.label">
            <dt>{{ row.label }}</dt>
            <dd>{{ row.value }}</dd>
          </template>
        </dl>
        <button class="btn blue target-confirm" :disabled="confirming || !selectedTargetVisible" @click="$emit('confirm')">
          {{ confirming ? "确认中" : "确认选择这条通告" }}
        </button>
        <DisabledReason
          v-if="targetConfirmDisabledReason"
          :text="targetConfirmDisabledReason"
          tone="warning"
        />
      </aside>
    </div>
    <div v-if="sourceCandidates.length" class="source-choice-panel">
      <div>
        <strong>对应原始事项</strong>
        <p>选择原始事项后，后续状态和闭环会更准确；如果已经选中已上传通告，也可以直接继续。</p>
      </div>
      <label class="candidate-search">
        <span>搜索原始事项</span>
        <input v-model="sourceSearchModel" type="search" placeholder="标题、楼栋、状态、时间、内容" />
      </label>
      <p class="candidate-count">当前显示 {{ filteredSourceCandidates.length }} / {{ sourceCandidates.length }} 条</p>
      <p v-if="selectedSourceHiddenBySearch" class="target-filter-note">
        已保留已选原始事项；当前搜索条件暂时隐藏了它。
      </p>
      <div class="source-choice-list">
        <p v-if="!filteredSourceCandidates.length" class="target-empty-line">没有匹配的原始事项，请调整搜索条件。</p>
        <button
          v-for="item in filteredSourceCandidates"
          :key="sourceCandidateId(item)"
          class="source-choice"
          :class="{ active: selectedSourceId === sourceCandidateId(item) }"
          @click="$emit('update:selectedSourceId', sourceCandidateId(item))"
        >
          <strong>{{ item.title || "未命名原始事项" }}</strong>
          <span>{{ item.building || "-" }} · {{ item.status || "未标记状态" }} · {{ item.start_time || "-" }} 至 {{ item.end_time || "-" }}</span>
        </button>
      </div>
      <button
        v-if="!rawTargetCount"
        class="btn blue target-confirm"
        :disabled="confirming || !selectedSourceVisible"
        @click="$emit('confirm')"
      >
        {{ confirming ? "确认中" : "确认选择原始事项" }}
      </button>
      <DisabledReason
        v-if="!rawTargetCount && sourceConfirmDisabledReason"
        :text="sourceConfirmDisabledReason"
        tone="warning"
      />
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import DisabledReason from "./DisabledReason.vue";
import type { Dict } from "../api/client";

const props = defineProps<{
  selection: Dict;
  targetCandidates: Dict[];
  sourceCandidates: Dict[];
  filteredSourceCandidates: Dict[];
  activeTargetCandidate: Dict | null;
  selectedTargetId: string;
  selectedSourceId: string;
  targetSearchText: string;
  sourceSearchText: string;
  confirming: boolean;
  selectedTargetVisible: boolean;
  selectedSourceVisible: boolean;
  workTypeLabel: (value: string) => string;
  targetCandidateId: (item: Dict) => string;
  sourceCandidateId: (item: Dict) => string;
  detailRowsFor: (item: Dict | null) => Array<{ label: string; value: string }>;
}>();

const emit = defineEmits<{
  "update:targetSearchText": [value: string];
  "update:sourceSearchText": [value: string];
  "update:selectedSourceId": [value: string];
  "preview-target": [item: Dict];
  "select-target": [item: Dict];
  confirm: [];
}>();

const targetSearchModel = computed({
  get: () => props.targetSearchText,
  set: (value: string) => emit("update:targetSearchText", value),
});

const sourceSearchModel = computed({
  get: () => props.sourceSearchText,
  set: (value: string) => emit("update:sourceSearchText", value),
});

const rawTargetCount = computed(() => (
  Array.isArray(props.selection?.candidates) ? props.selection.candidates.length : 0
));

const workTypeName = computed(() => props.workTypeLabel(String(props.selection?.type || "")));
const selectedTargetHiddenBySearch = computed(() => {
  const selected = String(props.selectedTargetId || "").trim();
  if (!selected) return false;
  return !props.targetCandidates.some((item) => props.targetCandidateId(item) === selected);
});
const selectedSourceHiddenBySearch = computed(() => {
  const selected = String(props.selectedSourceId || "").trim();
  if (!selected) return false;
  return !props.filteredSourceCandidates.some((item) => props.sourceCandidateId(item) === selected);
});
const targetConfirmDisabledReason = computed(() => {
  if (props.confirming || props.selectedTargetVisible) return "";
  if (selectedTargetHiddenBySearch.value) return "已选通告被搜索条件隐藏，请清空搜索后确认。";
  if (rawTargetCount.value) return "请先选择一条已上传通告。";
  return "";
});
const sourceConfirmDisabledReason = computed(() => {
  if (props.confirming || props.selectedSourceVisible) return "";
  if (selectedSourceHiddenBySearch.value) return "已选原始事项被搜索条件隐藏，请清空搜索后确认。";
  return "请先选择一条原始事项。";
});
</script>

<style scoped>
.target-choice-panel {
  display: grid;
  gap: 10px;
  margin-top: 10px;
  padding: 12px;
  border: 1px solid #bfdbfe;
  border-radius: 18px;
  background:
    linear-gradient(135deg, rgba(248, 251, 255, 0.98), rgba(255, 255, 255, 0.92)),
    #ffffff;
  box-shadow: 0 14px 34px rgba(0, 47, 135, 0.1);
}

.target-choice-panel p {
  margin: 4px 0 0;
  color: #475569;
  font-size: 13px;
}

.target-count-line {
  color: #2563eb;
  font-size: 12px;
}

.target-choice-layout {
  display: grid;
  grid-template-columns: minmax(250px, 0.95fr) minmax(320px, 1.05fr);
  gap: 12px;
  align-items: start;
}

.target-choice-column,
.source-choice-panel {
  display: grid;
  gap: 8px;
  min-width: 0;
}

.candidate-search {
  display: grid;
  gap: 5px;
  color: #475569;
  font-size: 12px;
}

.candidate-search input {
  width: 100%;
  min-width: 0;
  border: 1px solid #dbe3ee;
  border-radius: 14px;
  padding: 9px 11px;
  background: #ffffff;
  color: #0f172a;
  font-size: 13px;
  outline: none;
}

.candidate-search input:focus {
  border-color: #2563eb;
  box-shadow: 0 0 0 3px rgba(37, 99, 235, 0.1);
}

.candidate-count {
  margin: 0;
  color: #64748b;
  font-size: 12px;
}

.target-filter-note {
  margin: 0;
  border: 1px solid #fde68a;
  border-radius: 14px;
  padding: 7px 9px;
  background: #fffbeb;
  color: #92400e;
  font-size: 12px;
  font-weight: 850;
}

.target-choice-list {
  display: grid;
  gap: 7px;
  max-height: 360px;
  overflow: auto;
  padding-right: 2px;
}

.target-choice,
.source-choice {
  display: grid;
  gap: 4px;
  width: 100%;
  padding: 9px 10px;
  border: 1px solid #dbe3ee;
  border-radius: 14px;
  background:
    linear-gradient(135deg, rgba(255, 255, 255, 0.98), rgba(248, 251, 255, 0.86)),
    #ffffff;
  color: #0f172a;
  text-align: left;
  cursor: pointer;
  transition: border-color 0.14s ease, box-shadow 0.14s ease, background-color 0.14s ease;
}

.target-choice:hover,
.source-choice:hover {
  border-color: #2563eb;
  box-shadow: 0 10px 22px rgba(15, 86, 228, 0.09);
}

.target-choice.active {
  border-color: #2563eb;
  background: #eff6ff;
  box-shadow: inset 4px 0 0 #2563eb, 0 12px 24px rgba(37, 99, 235, 0.12);
}

.target-choice span,
.source-choice span {
  color: #64748b;
  font-size: 12px;
}

.target-choice small {
  color: #2563eb;
  font-size: 12px;
}

.target-detail-popover {
  position: sticky;
  top: 10px;
  padding: 12px;
  border: 1px solid #d8e5f7;
  border-radius: 20px;
  background:
    linear-gradient(135deg, #ffffff 0%, #f8fbff 100%),
    #ffffff;
  box-shadow: 0 18px 42px rgba(0, 47, 135, 0.14);
}

.target-detail-head {
  display: grid;
  gap: 4px;
  margin-bottom: 10px;
  border-bottom: 1px solid #e5edf8;
  padding-bottom: 10px;
}

.target-detail-head strong {
  color: #071a39;
  font-size: 15px;
}

.target-detail-head span {
  color: #64748b;
  font-size: 12px;
}

.target-detail-head small {
  color: #2563eb;
  font-size: 12px;
}

.target-detail-grid {
  display: grid;
  grid-template-columns: 96px 1fr;
  gap: 6px 10px;
  max-height: 300px;
  margin: 0;
  overflow: auto;
}

.target-detail-grid dt {
  color: #64748b;
  font-size: 12px;
}

.target-detail-grid dd {
  margin: 0;
  color: #0f172a;
  font-size: 13px;
  line-height: 1.45;
  word-break: break-word;
}

.target-confirm {
  width: 100%;
  margin-top: 12px;
  min-height: 42px;
  font-weight: 950;
}

.source-choice-panel {
  padding-top: 10px;
  border-top: 1px solid #bfdbfe;
}

.source-choice-panel p {
  margin: 4px 0 0;
  color: #475569;
  font-size: 13px;
}

.source-choice-list {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
  gap: 8px;
}

.source-choice.active {
  border-color: #0f766e;
  background: #f0fdfa;
  box-shadow: 0 0 0 2px rgba(15, 118, 110, 0.1);
}

.target-empty-line {
  margin: 0;
  color: #64748b;
  font-size: 12px;
}

.btn {
  min-height: 38px;
  border: 1px solid #d8e5f7;
  border-radius: 16px;
  padding: 8px 14px;
  color: #0f172a;
  font-weight: 750;
  text-decoration: none;
  background: #ffffff;
  cursor: pointer;
}

.btn:disabled {
  cursor: not-allowed;
  opacity: 0.58;
}

.btn.blue {
  border-color: transparent;
  color: #ffffff;
  background: linear-gradient(135deg, #1e63ff, #1554df);
  box-shadow: 0 10px 22px rgba(30, 99, 255, 0.22);
}

@media (max-width: 980px) {
  .target-choice-layout {
    grid-template-columns: 1fr;
  }

  .target-detail-popover {
    position: static;
  }
}
</style>
