<template>
  <div v-if="selection" class="modal-backdrop" @click.self="emit('cancel')">
    <section class="candidate-modal" role="dialog" aria-modal="true" aria-label="选择对应通告">
      <div class="candidate-modal-head">
        <div>
          <strong>选择{{ selection.label || "通告" }}对应通告</strong>
          <p>{{ selection.title || "找到多条相近通告，请选择要继续处理的一条。" }}</p>
        </div>
        <button class="btn ghost" type="button" @click="emit('cancel')">关闭</button>
      </div>
      <div class="target-choice-layout modal-choice-layout">
        <div class="target-choice-column">
          <label class="candidate-search">
            <span>搜索候选</span>
            <input v-model="searchText" type="search" placeholder="标题、楼栋、时间、内容" />
          </label>
          <p class="candidate-count">
            当前显示 {{ filteredCandidates.length }} / {{ candidates.length }} 条
          </p>
          <div class="target-choice-list modal-choice-list">
            <p v-if="!filteredCandidates.length" class="candidate-empty">
              没有匹配的通告，请调整搜索条件。
            </p>
          <button
            v-for="item in filteredCandidates"
            :key="candidateId(item)"
            class="target-choice"
            :class="{ active: selectedId === candidateId(item) }"
            type="button"
            :aria-pressed="selectedId === candidateId(item)"
            @mouseenter="emit('preview', item)"
            @focus="emit('preview', item)"
            @click="chooseCandidate(item)"
          >
            <span v-if="selectedId === candidateId(item)" class="selected-marker">已选</span>
            <strong>{{ item.title || "未命名通告" }}</strong>
            <span>{{ item.building || "-" }} · {{ item.status || "未标记状态" }} · {{ item.start_time || "-" }} 至 {{ item.end_time || "-" }}</span>
            <small>{{ item.match_reason || (item.date_matched ? "时间匹配" : "按名称匹配") }}</small>
          </button>
          </div>
        </div>
        <aside v-if="visibleActiveCandidate" class="target-detail-popover modal-detail-popover">
          <div class="target-detail-head">
            <strong>{{ visibleActiveCandidate.title || "对应通告" }}</strong>
            <span>{{ visibleActiveCandidate.building || "-" }} · {{ visibleActiveCandidate.status || "未标记状态" }}</span>
            <small>{{ visibleActiveCandidate.match_reason || "" }}</small>
          </div>
          <dl class="target-detail-grid">
            <template v-for="row in detailRows" :key="row.label">
              <dt>{{ row.label }}</dt>
              <dd>{{ row.value }}</dd>
            </template>
          </dl>
        </aside>
      </div>
      <div class="candidate-modal-actions">
        <DisabledReason
          v-if="confirmDisabledReason"
          :text="confirmDisabledReason"
          tone="warning"
        />
        <span v-else class="job-line">
          已选择：{{ selectedSummary }}。确认后只关联当前通告，并继续后续上传/更新。
        </span>
        <div class="candidate-action-buttons">
          <button class="btn ghost" type="button" @click="emit('cancel')">取消</button>
          <button class="btn blue" type="button" :disabled="!canConfirm" :title="confirmDisabledReason" @click="emit('confirm')">确认关联并继续</button>
        </div>
      </div>
    </section>
  </div>
</template>

<script setup lang="ts">
import { computed, ref, watch } from "vue";
import { filterCandidatesBySearch } from "../candidateSearch";
import DisabledReason from "./DisabledReason.vue";

type Dict = Record<string, any>;

const props = defineProps<{
  selection: Dict | null;
  candidates: Dict[];
  selectedId: string;
  activeCandidate: Dict | null;
  candidateId: (item: Dict) => string;
  detailRowsFor: (item: Dict | null) => Array<{ label: string; value: string }>;
}>();

const emit = defineEmits<{
  cancel: [];
  confirm: [];
  preview: [item: Dict];
  select: [item: Dict];
}>();

const searchText = ref("");

watch(
  () => props.selection,
  () => {
    searchText.value = "";
  },
);

const filteredCandidates = computed(() => {
  return filterCandidatesBySearch(props.candidates, searchText.value);
});

const visibleActiveCandidate = computed(() => {
  if (selectedCandidate.value) return selectedCandidate.value;
  const activeId = props.activeCandidate ? props.candidateId(props.activeCandidate) : "";
  if (activeId) {
    const visible = filteredCandidates.value.find((item) => props.candidateId(item) === activeId);
    if (visible) return visible;
  }
  return filteredCandidates.value[0] || null;
});

const detailRows = computed(() => props.detailRowsFor(visibleActiveCandidate.value));
const selectedCandidate = computed(() => {
  const selected = String(props.selectedId || "").trim();
  if (!selected) return null;
  return props.candidates.find((item) => props.candidateId(item) === selected) || null;
});
const selectedSummary = computed(() => {
  const item = selectedCandidate.value;
  if (!item) return "未选择";
  const title = String(item.title || "未命名通告").trim();
  const building = String(item.building || "-").trim();
  const time = [item.start_time, item.end_time].filter(Boolean).join("~");
  return time ? `${title}（${building} · ${time}）` : `${title}（${building}）`;
});

const canConfirm = computed(() => Boolean(selectedCandidate.value));
const confirmDisabledReason = computed(() => {
  if (canConfirm.value) return "";
  if (!props.candidates.length) return "暂未找到可选择的对应通告。";
  if (!filteredCandidates.value.length) return "当前搜索条件下没有候选通告，请调整搜索。";
  return "请先在左侧选择一条对应通告。";
});

function chooseCandidate(item: Dict): void {
  emit("preview", item);
  emit("select", item);
}
</script>

<style scoped>
.modal-backdrop {
  position: fixed;
  inset: 0;
  z-index: var(--cf-z-modal-backdrop, 800);
  display: grid;
  place-items: center;
  padding: 24px;
  background: rgba(15, 23, 42, 0.28);
}

.candidate-modal {
  position: relative;
  z-index: var(--cf-z-modal, 840);
  display: grid;
  width: min(980px, 100%);
  max-height: min(760px, calc(100vh - 48px));
  overflow: hidden;
  border: 1px solid #dbe3ee;
  border-radius: 10px;
  background: #ffffff;
  box-shadow: 0 24px 60px rgba(15, 23, 42, 0.18);
}

.candidate-modal-head,
.candidate-modal-actions {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 14px 16px;
  border-bottom: 1px solid #e2e8f0;
}

.candidate-modal-actions {
  border-top: 1px solid #e2e8f0;
  border-bottom: 0;
}

.candidate-action-buttons {
  flex: 0 0 auto;
  display: inline-flex;
  align-items: center;
  justify-content: flex-end;
  gap: 10px;
}

.candidate-modal-head p {
  margin: 4px 0 0;
  color: #64748b;
  font-size: 13px;
}

.target-choice-layout {
  display: grid;
  grid-template-columns: minmax(220px, 0.9fr) minmax(280px, 1.1fr);
  gap: 10px;
  align-items: start;
}

.target-choice-column {
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
  border-radius: 7px;
  padding: 8px 10px;
  background: #ffffff;
  color: #0f172a;
  font-size: 13px;
  outline: none;
}

.candidate-search input:focus {
  border-color: #2563eb;
  box-shadow: 0 0 0 2px rgba(37, 99, 235, 0.1);
}

.candidate-count {
  margin: 0;
  color: #64748b;
  font-size: 12px;
}

.candidate-empty {
  margin: 0;
  padding: 12px;
  border: 1px dashed #cbd5e1;
  border-radius: 8px;
  color: #64748b;
  font-size: 13px;
}

.modal-choice-layout {
  min-height: 360px;
  max-height: 560px;
  overflow: hidden;
  padding: 12px;
}

.target-choice-list {
  display: grid;
  gap: 7px;
  max-height: 360px;
  overflow: auto;
}

.modal-choice-list,
.modal-detail-popover {
  overflow: auto;
}

.target-choice {
  position: relative;
  display: grid;
  gap: 4px;
  width: 100%;
  padding: 10px 52px 10px 12px;
  border: 1px solid #dbe3ee;
  border-radius: 8px;
  background: #ffffff;
  color: #0f172a;
  text-align: left;
  cursor: pointer;
}

.target-choice:hover {
  border-color: #2563eb;
}

.target-choice.active {
  border-color: #2563eb;
  box-shadow: 0 0 0 2px rgba(37, 99, 235, 0.12);
}

.target-choice span {
  color: #64748b;
  font-size: 12px;
}

.target-choice small {
  color: #2563eb;
  font-size: 12px;
}

.selected-marker {
  position: absolute;
  top: 10px;
  right: 10px;
  display: inline-flex;
  align-items: center;
  min-height: 22px;
  border: 1px solid #bfdbfe;
  border-radius: 999px;
  padding: 0 8px;
  background: #eff6ff;
  color: #155dfc;
  font-size: 12px;
  font-weight: 900;
}

.target-detail-popover {
  position: sticky;
  top: 10px;
  padding: 10px;
  border: 1px solid #bfdbfe;
  border-radius: 8px;
  background: #ffffff;
  box-shadow: 0 12px 30px rgba(15, 23, 42, 0.12);
}

.target-detail-head {
  display: grid;
  gap: 3px;
  margin-bottom: 8px;
}

.target-detail-head span {
  color: #64748b;
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

.btn:disabled {
  cursor: not-allowed;
  opacity: 0.58;
}

.btn.blue {
  border-color: #2563eb;
  background: #2563eb;
  color: #ffffff;
}

.job-line {
  flex: 1 1 auto;
  min-width: 0;
  color: #64748b;
  font-size: 13px;
  line-height: 1.55;
  overflow-wrap: anywhere;
}

@media (max-width: 720px) {
  .modal-backdrop {
    padding: 10px;
  }

  .candidate-modal-head,
  .candidate-modal-actions {
    align-items: stretch;
    flex-direction: column;
  }

  .candidate-action-buttons {
    justify-content: stretch;
  }

  .candidate-action-buttons .btn {
    flex: 1 1 0;
  }

  .modal-choice-layout {
    grid-template-columns: 1fr;
    max-height: calc(100vh - 180px);
  }
}

/* VNET target selection skin */
.modal-backdrop {
  background:
    linear-gradient(rgba(5, 20, 55, 0.42), rgba(5, 20, 55, 0.42)),
    radial-gradient(circle at 52% 0%, rgba(21, 116, 239, 0.28), transparent 42%);
  backdrop-filter: blur(6px);
}

.candidate-modal {
  border-color: #d8e7f8;
  border-radius: 18px;
  background: linear-gradient(180deg, #ffffff, #f8fbff);
  box-shadow: 0 30px 90px rgba(4, 43, 116, 0.28);
}

.candidate-modal-head,
.candidate-modal-actions {
  border-color: #e7f0fb;
}

.candidate-modal-head strong {
  color: #071634;
  font-size: 22px;
  font-weight: 900;
}

.candidate-search input {
  border-color: #c8dcf3;
  border-radius: 9px;
  background: #fbfdff;
}

.candidate-search input:focus {
  border-color: #1678ff;
  box-shadow: 0 0 0 3px rgba(22, 120, 255, 0.12);
}

.target-choice,
.target-detail-popover,
.candidate-empty {
  border-color: #d8e7f8;
  border-radius: 12px;
  background: #ffffff;
  box-shadow: 0 8px 18px rgba(22, 78, 151, 0.06);
}

.target-choice:hover {
  border-color: #9cc7ff;
  background: #f5faff;
}

.target-choice.active {
  border-color: #1678ff;
  background: #edf6ff;
  box-shadow: inset 4px 0 0 #1678ff, 0 12px 26px rgba(22, 120, 255, 0.12);
}

.target-choice.active .selected-marker {
  border-color: transparent;
  background: linear-gradient(135deg, #1e63ff, #1554df);
  color: #ffffff;
  box-shadow: 0 8px 16px rgba(30, 99, 255, 0.16);
}

.target-choice strong,
.target-detail-head strong {
  color: #09204a;
  font-weight: 900;
}

.btn {
  min-height: 36px;
  border-color: #c5d9f2;
  border-radius: 9px;
  color: #09204a;
  font-weight: 750;
}

.btn.blue {
  border-color: transparent;
  background: linear-gradient(135deg, #0757d7, #1678ff);
  color: #ffffff;
  box-shadow: 0 12px 24px rgba(20, 103, 226, 0.22);
}

/* Final VNET modal skin */
.candidate-modal {
  border-color: #d8e5f7;
  border-radius: 28px;
  background: rgba(255, 255, 255, 0.94);
  box-shadow: 0 24px 64px rgba(0, 47, 135, 0.18);
}

.target-choice,
.target-detail-popover,
.candidate-empty {
  border-color: #d8e5f7;
  border-radius: 18px;
  background: rgba(255, 255, 255, 0.9);
}

.candidate-search input,
.btn {
  border-radius: 14px;
}

.candidate-modal-head strong,
.target-choice strong,
.target-detail-head strong {
  font-weight: 820;
  letter-spacing: 0;
}

.candidate-modal-head p,
.target-choice span,
.target-choice small,
.target-detail-head span,
.target-detail-grid dt,
.job-line {
  color: #5f7189;
}

.btn {
  font-weight: 720;
}

.target-choice:hover {
  border-color: #bdd2f4;
  background: #ffffff;
}

.target-choice.active {
  border-color: #005bff;
  background: #eff6ff;
  box-shadow: inset 4px 0 0 #005bff, 0 12px 24px rgba(0, 91, 255, 0.12);
}

.candidate-search input {
  border-color: #d8e5f7;
  background: rgba(255, 255, 255, 0.9);
}

.candidate-search input:focus {
  border-color: #005bff;
  box-shadow: 0 0 0 3px rgba(0, 91, 255, 0.14);
}

.btn.blue {
  background: linear-gradient(135deg, #1e63ff, #1554df);
}

.btn.blue:hover:not(:disabled) {
  background: #1554df;
}
</style>


