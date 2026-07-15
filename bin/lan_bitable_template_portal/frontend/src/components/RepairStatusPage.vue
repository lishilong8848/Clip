<template>
  <section class="repair-status-page">
    <header class="status-hero">
      <div class="status-title-row">
        <VnetBackButton @click="backToRepairManagement" />
        <div>
          <span class="scope-badge">{{ scopeLabel }}</span>
          <h2>检修状态</h2>
        </div>
      </div>
      <button type="button" class="refresh-button" :disabled="loading" @click="refreshStatus">
        <RefreshCw :size="16" :class="{ spinning: loading }" aria-hidden="true" />
        <span>{{ loading && !records.length ? "读取中" : "刷新" }}</span>
      </button>
    </header>

    <MessageBanner v-if="message" :tone="messageTone" :text="message" />

    <div class="status-summary" aria-label="检修状态统计">
      <button type="button" :class="{ active: activeState === 'all' }" @click="setStatusFilter('all')">
        <span class="summary-icon blue"><ClipboardList :size="19" aria-hidden="true" /></span>
        <span><small>当前状态项目</small><strong>{{ stats.total }}</strong></span>
      </button>
      <button type="button" :class="{ active: activeState === 'without_followup' }" @click="setStatusFilter('without_followup')">
        <span class="summary-icon amber"><CircleAlert :size="19" aria-hidden="true" /></span>
        <span><small>待首次跟进</small><strong>{{ stats.without_followup }}</strong></span>
      </button>
      <button type="button" :class="{ active: activeState === 'in_progress' }" @click="setStatusFilter('in_progress')">
        <span class="summary-icon teal"><Activity :size="19" aria-hidden="true" /></span>
        <span><small>维修进行中</small><strong>{{ stats.in_progress }}</strong></span>
      </button>
      <button type="button" :class="{ active: activeState === 'completed' }" @click="setStatusFilter('completed')">
        <span class="summary-icon green"><CircleCheckBig :size="19" aria-hidden="true" /></span>
        <span><small>历史已完成</small><strong>{{ stats.completed_total }}</strong></span>
      </button>
    </div>

    <section class="status-workspace">
      <header class="status-toolbar">
        <strong class="status-view-title">{{ statusViewTitle }}</strong>
        <div v-if="activeState === 'completed'" class="history-period-tabs" aria-label="完成时间范围">
          <button type="button" :class="{ active: activePeriod === 'all' }" @click="setHistoryPeriod('all')">
            全部 <b>{{ stats.completed_total }}</b>
          </button>
          <button type="button" :class="{ active: activePeriod === 'month' }" @click="setHistoryPeriod('month')">
            本月 <b>{{ stats.completed_month }}</b>
          </button>
          <button type="button" :class="{ active: activePeriod === 'week' }" @click="setHistoryPeriod('week')">
            本周 <b>{{ stats.completed_week }}</b>
          </button>
          <button type="button" :class="{ active: activePeriod === 'today' }" @click="setHistoryPeriod('today')">
            今天 <b>{{ stats.completed_today }}</b>
          </button>
        </div>
        <label class="status-search">
          <Search :size="16" aria-hidden="true" />
          <input v-model.trim="searchText" type="search" placeholder="搜索维修名称、专业、位置或进展" />
          <button v-if="searchText" type="button" aria-label="清空搜索" @click="searchText = ''">
            <X :size="15" aria-hidden="true" />
          </button>
        </label>
        <span class="result-count">{{ total }} 项</span>
      </header>

      <div class="status-table-wrap" :aria-busy="loading">
        <div class="status-table-head" aria-hidden="true">
          <span>维修项目</span>
          <span>状态</span>
          <span>跟进记录</span>
          <span>当前进度</span>
          <span>最近跟进</span>
          <span>操作</span>
        </div>
        <div v-if="loading && !records.length" class="empty-state">正在读取检修状态...</div>
        <div v-else-if="!records.length" class="empty-state">{{ statusEmptyText }}</div>
        <article v-for="record in records" v-else :key="record.record_id" class="status-row">
          <div class="project-cell">
            <strong :title="String(record.title || '')">{{ record.title || "未命名维修项目" }}</strong>
            <span>
              <b v-if="record.specialty">{{ record.specialty }}</b>
              <i v-if="record.event_sent_time">{{ formatTime(record.event_sent_time) }}</i>
              <i v-if="record.location">{{ record.location }}</i>
            </span>
          </div>
          <div>
            <span class="state-pill" :class="record.state">
              {{ record.status_label }}
            </span>
          </div>
          <div class="followup-count-cell">
            <strong>{{ record.completed_followup_count }}/{{ record.followup_count }}</strong>
            <span>{{ record.followup_count ? "已完成 / 全部" : "尚未填写" }}</span>
          </div>
          <div class="progress-cell">
            <div>
              <span>{{ record.progress_percent }}%</span>
              <div class="progress-track" aria-hidden="true">
                <i :style="{ width: `${record.progress_percent}%` }"></i>
              </div>
            </div>
          </div>
          <div class="latest-cell">
            <strong>{{ record.latest_followup_time ? formatTime(record.latest_followup_time) : "未跟进" }}</strong>
            <span :title="String(record.latest_followup || '')">{{ record.latest_followup || "-" }}</span>
          </div>
          <div class="action-cell">
            <button type="button" @click="openRecord(record)">
              <span>跟进检修管理</span>
              <ArrowRight :size="15" aria-hidden="true" />
            </button>
          </div>
        </article>
      </div>

      <nav v-if="pageCount > 1" class="status-pager" aria-label="检修状态分页">
        <button type="button" :disabled="loading || page <= 1" @click="changePage(-1)">上一页</button>
        <span>{{ page }} / {{ pageCount }}</span>
        <button type="button" :disabled="loading || page >= pageCount" @click="changePage(1)">下一页</button>
      </nav>
    </section>
  </section>
</template>

<script setup lang="ts">
import { computed, onActivated, onBeforeUnmount, onMounted, ref, watch } from "vue";
import {
  Activity,
  ArrowRight,
  CircleAlert,
  CircleCheckBig,
  ClipboardList,
  RefreshCw,
  Search,
  X,
} from "lucide-vue-next";
import { requestJson } from "../api/client";
import { navigate } from "../navigation";
import { REPAIR_STATUS_INVALIDATED_EVENT } from "../repairStatusState";
import type { LooseDict } from "../types";
import MessageBanner from "./MessageBanner.vue";
import VnetBackButton from "./VnetBackButton.vue";

type StatusFilter = "all" | "without_followup" | "in_progress" | "completed";
type HistoryPeriod = "all" | "month" | "week" | "today";

const props = defineProps<{
  scope: string;
}>();

const PAGE_SIZE = 24;
const STATUS_CACHE_TTL_MS = 60 * 1000;
const records = ref<LooseDict[]>([]);
const total = ref(0);
const page = ref(1);
const loading = ref(false);
const searchText = ref("");
const message = ref("");
const messageTone = ref<"success" | "warning" | "failed">("success");
const stats = ref({
  total: 0,
  without_followup: 0,
  in_progress: 0,
  completed_total: 0,
  completed_month: 0,
  completed_week: 0,
  completed_today: 0,
  average_progress: 0,
});
const activeState = ref<StatusFilter>("all");
const activePeriod = ref<HistoryPeriod>("all");
let searchTimer: ReturnType<typeof setTimeout> | undefined;
let requestVersion = 0;
let statusStale = false;
let statusAbortController: AbortController | null = null;
const responseCache = new Map<string, { payload: LooseDict; cachedAt: number }>();

const pageCount = computed(() => Math.max(1, Math.ceil(total.value / PAGE_SIZE)));
const statusViewTitle = computed(() => ({
  all: "当前状态项目",
  without_followup: "待首次跟进",
  in_progress: "维修进行中",
  completed: "历史已完成",
}[activeState.value]));
const statusEmptyText = computed(() => ({
  all: "当前没有待处理检修项目",
  without_followup: "当前没有待首次跟进的项目",
  in_progress: "当前没有维修进行中的项目",
  completed: "当前时间范围内没有已完成项目",
}[activeState.value]));
const scopeLabel = computed(() => {
  const scope = String(props.scope || "ALL").toUpperCase();
  if (scope === "110") return "110站";
  if (scope === "CAMPUS") return "园区";
  if (scope === "ALL") return "全部楼栋";
  return `${scope}楼`;
});

function formatTime(value: unknown): string {
  return String(value || "")
    .replace("T", " ")
    .replace(/(\d{2}:\d{2}):\d{2}(?:\.\d+)?$/, "$1");
}

function backToRepairManagement(): void {
  const url = new URL("/repair-management", window.location.origin);
  url.searchParams.set("scope", props.scope || "ALL");
  navigate(url);
}

function openRecord(record: LooseDict): void {
  const recordId = String(record.record_id || "").trim();
  if (!recordId) return;
  const url = new URL("/repair-management", window.location.origin);
  url.searchParams.set("scope", props.scope || "ALL");
  url.searchParams.set("record_id", recordId);
  navigate(url);
}

function setStatusFilter(state: StatusFilter): void {
  if (activeState.value === state) return;
  activeState.value = state;
  activePeriod.value = "all";
  page.value = 1;
  records.value = [];
  total.value = 0;
  void loadStatus(false);
}

function setHistoryPeriod(period: HistoryPeriod): void {
  if (activePeriod.value === period) return;
  activePeriod.value = period;
  page.value = 1;
  records.value = [];
  total.value = 0;
  void loadStatus(false);
}

function changePage(delta: number): void {
  const nextPage = Math.min(pageCount.value, Math.max(1, page.value + delta));
  if (nextPage === page.value) return;
  page.value = nextPage;
  void loadStatus(false);
}

async function refreshStatus(): Promise<void> {
  await loadStatus(true);
}

async function loadStatus(forceRefresh = false): Promise<void> {
  const params = new URLSearchParams({
    scope: props.scope || "ALL",
    q: searchText.value,
    state: activeState.value,
    period: activeState.value === "completed" ? activePeriod.value : "all",
    limit: String(PAGE_SIZE),
    offset: String((page.value - 1) * PAGE_SIZE),
  });
  const cacheKey = params.toString();
  if (!forceRefresh && !statusStale) {
    const cached = responseCache.get(cacheKey);
    if (cached && Date.now() - cached.cachedAt <= STATUS_CACHE_TTL_MS) {
      requestVersion += 1;
      statusAbortController?.abort();
      statusAbortController = null;
      loading.value = false;
      message.value = "";
      applyStatusPayload(cached.payload);
      return;
    }
  }
  const version = ++requestVersion;
  statusAbortController?.abort();
  const abortController = new AbortController();
  statusAbortController = abortController;
  loading.value = true;
  message.value = "";
  try {
    if (forceRefresh) params.set("refresh", "1");
    const payload = await requestJson(
      `/api/repair-management/status?${params.toString()}`,
      { signal: abortController.signal },
    );
    if (version !== requestVersion) return;
    statusStale = false;
    responseCache.set(cacheKey, { payload, cachedAt: Date.now() });
    applyStatusPayload(payload);
    const warnings = Array.isArray(payload.warnings)
      ? payload.warnings.map((item: unknown) => String(item || "").trim()).filter(Boolean)
      : [];
    const maxPage = Math.max(1, Math.ceil(total.value / PAGE_SIZE));
    if (page.value > maxPage) {
      page.value = maxPage;
      loading.value = false;
      await loadStatus(forceRefresh);
      return;
    }
    if (warnings.length) {
      message.value = warnings.join("；");
      messageTone.value = "warning";
    } else if (forceRefresh) {
      message.value = "检修状态已刷新。";
      messageTone.value = "success";
    }
  } catch (error: unknown) {
    if (abortController.signal.aborted) return;
    if (version !== requestVersion) return;
    message.value = error instanceof Error ? error.message : "检修状态读取失败。";
    messageTone.value = "failed";
  } finally {
    if (statusAbortController === abortController) statusAbortController = null;
    if (version === requestVersion) loading.value = false;
  }
}

function applyStatusPayload(payload: LooseDict): void {
  records.value = Array.isArray(payload.records) ? payload.records : [];
  total.value = Number(payload.total || records.value.length || 0);
  const payloadStats = payload.stats && typeof payload.stats === "object"
    ? payload.stats as LooseDict
    : {};
  stats.value = {
    total: Number(payloadStats.total || 0),
    without_followup: Number(payloadStats.without_followup || 0),
    in_progress: Number(payloadStats.in_progress || 0),
    completed_total: Number(payloadStats.completed_total || 0),
    completed_month: Number(payloadStats.completed_month || 0),
    completed_week: Number(payloadStats.completed_week || 0),
    completed_today: Number(payloadStats.completed_today || 0),
    average_progress: Number(payloadStats.average_progress || 0),
  };
}

function markStatusStale(): void {
  statusStale = true;
  responseCache.clear();
}

watch(searchText, () => {
  if (searchTimer) clearTimeout(searchTimer);
  searchTimer = setTimeout(() => {
    page.value = 1;
    void loadStatus(false);
  }, 280);
});

watch(
  () => props.scope,
  () => {
    activeState.value = "all";
    activePeriod.value = "all";
    page.value = 1;
    void loadStatus(false);
  },
  { immediate: true },
);

onMounted(() => {
  window.addEventListener(REPAIR_STATUS_INVALIDATED_EVENT, markStatusStale);
});

onActivated(() => {
  if (!statusStale || loading.value) return;
  void loadStatus(false);
});

onBeforeUnmount(() => {
  if (searchTimer) clearTimeout(searchTimer);
  statusAbortController?.abort();
  window.removeEventListener(REPAIR_STATUS_INVALIDATED_EVENT, markStatusStale);
});
</script>

<style scoped>
.repair-status-page {
  width: min(1680px, 100%);
  margin: 0 auto;
  padding: 16px 28px 34px;
  display: grid;
  gap: 12px;
  color: #132d4f;
}

.status-hero,
.status-workspace,
.status-summary > * {
  border: 1px solid #d8e5f5;
  background: #fff;
  box-shadow: 0 10px 26px rgba(20, 75, 150, 0.07);
}

.status-hero {
  min-height: 76px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  border-radius: 12px;
  padding: 12px 16px;
}

.status-title-row {
  min-width: 0;
  display: flex;
  align-items: center;
  gap: 12px;
}

.status-title-row > div {
  min-width: 0;
  display: flex;
  align-items: center;
  gap: 10px;
}

.status-title-row h2 {
  overflow: hidden;
  margin: 0;
  color: #10294a;
  font-size: 20px;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.scope-badge {
  min-height: 26px;
  display: inline-flex;
  align-items: center;
  border-radius: 8px;
  padding: 0 9px;
  background: #eaf3ff;
  color: #1458bd;
  font-size: 12px;
  font-weight: 800;
  white-space: nowrap;
}

.refresh-button,
.action-cell button,
.status-pager button {
  min-height: 36px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 7px;
  border: 1px solid #cbd9eb;
  border-radius: 9px;
  padding: 0 13px;
  background: #fff;
  color: #24527e;
  font: inherit;
  font-size: 13px;
  font-weight: 750;
  cursor: pointer;
}

.refresh-button:hover,
.refresh-button:focus-visible,
.status-pager button:hover:not(:disabled),
.status-pager button:focus-visible {
  border-color: #1e63ff;
  outline: 0;
  color: #1257bc;
  box-shadow: 0 0 0 3px rgba(30, 99, 255, 0.12);
}

.refresh-button:disabled,
.status-pager button:disabled {
  cursor: not-allowed;
  opacity: 0.52;
}

.status-summary {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 10px;
}

.status-summary > button {
  min-width: 0;
  min-height: 88px;
  display: flex;
  align-items: center;
  gap: 12px;
  border-radius: 12px;
  padding: 13px 15px;
  color: #183553;
  font: inherit;
  text-align: left;
}

.status-summary > button {
  cursor: pointer;
  transition: border-color 160ms ease, box-shadow 160ms ease, transform 160ms ease;
}

.status-summary > button:hover,
.status-summary > button.active,
.status-summary > button:focus-visible {
  border-color: #70a5f8;
  outline: 0;
  box-shadow: 0 10px 28px rgba(30, 99, 255, 0.13);
  transform: translateY(-1px);
}

.summary-icon {
  width: 42px;
  height: 42px;
  flex: 0 0 auto;
  display: grid;
  place-items: center;
  border-radius: 11px;
}

.summary-icon.blue { background: #e8f1ff; color: #1b63d8; }
.summary-icon.amber { background: #fff3df; color: #c8750a; }
.summary-icon.teal { background: #e3f7f3; color: #0b8d7a; }
.summary-icon.green { background: #e8f8ee; color: #198754; }

.status-summary small,
.status-summary strong {
  display: block;
}

.status-summary small {
  color: #6e8299;
  font-size: 12px;
  font-weight: 650;
}

.status-summary strong {
  margin-top: 3px;
  color: #10294a;
  font-size: 24px;
  line-height: 1;
}

.progress-track {
  overflow: hidden;
  border-radius: 999px;
  background: #e8eef6;
}

.progress-track i {
  height: 100%;
  display: block;
  border-radius: inherit;
  background: linear-gradient(90deg, #1f66ed, #14a58d);
}

.status-workspace {
  overflow: hidden;
  border-radius: 12px;
}

.status-toolbar {
  min-height: 62px;
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 12px;
  padding: 12px 14px;
  border-bottom: 1px solid #e2eaf4;
}

.status-view-title {
  flex: 0 0 auto;
  color: #17314f;
  font-size: 14px;
}

.status-search {
  min-width: 0;
  max-width: 620px;
  min-height: 38px;
  flex: 1;
  display: flex;
  align-items: center;
  gap: 8px;
  border: 1px solid #cbd9e8;
  border-radius: 9px;
  padding: 0 10px;
  color: #59728e;
  background: #fbfdff;
}

.status-search:focus-within {
  border-color: #1e63ff;
  box-shadow: 0 0 0 3px rgba(30, 99, 255, 0.12);
}

.status-search input {
  min-width: 0;
  flex: 1;
  border: 0;
  outline: 0;
  background: transparent;
  color: #17314f;
  font: inherit;
  font-size: 13px;
}

.status-search button {
  width: 28px;
  height: 28px;
  display: grid;
  place-items: center;
  border: 0;
  border-radius: 7px;
  background: transparent;
  color: #71849a;
  cursor: pointer;
}

.result-count {
  margin-left: auto;
  color: #647d98;
  font-size: 13px;
  font-weight: 750;
}

.status-table-wrap {
  min-width: 0;
}

.status-table-head,
.status-row {
  display: grid;
  grid-template-columns: minmax(300px, 2.2fr) 118px 112px minmax(150px, 0.9fr) minmax(190px, 1.2fr) 112px;
  align-items: center;
  gap: 12px;
}

.status-table-head {
  min-height: 38px;
  padding: 0 14px;
  background: #f5f8fc;
  color: #61778f;
  font-size: 12px;
  font-weight: 750;
}

.status-row {
  min-height: 76px;
  padding: 10px 14px;
  border-top: 1px solid #e8eef5;
  background: #fff;
}

.status-row:hover {
  background: #fbfdff;
}

.status-row > div {
  min-width: 0;
}

.project-cell,
.followup-count-cell,
.latest-cell {
  display: grid;
  gap: 5px;
}

.project-cell > strong,
.latest-cell > span {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.project-cell > strong {
  color: #122c4e;
  font-size: 14px;
}

.project-cell > span {
  min-width: 0;
  display: flex;
  align-items: center;
  gap: 7px;
  overflow: hidden;
  color: #71849a;
  font-size: 11px;
  white-space: nowrap;
}

.project-cell b {
  flex: 0 0 auto;
  border-radius: 6px;
  padding: 2px 6px;
  background: #edf4ff;
  color: #1658b5;
  font-style: normal;
}

.project-cell i {
  overflow: hidden;
  text-overflow: ellipsis;
  font-style: normal;
}

.state-pill {
  min-height: 27px;
  display: inline-flex;
  align-items: center;
  border-radius: 8px;
  padding: 0 9px;
  font-size: 12px;
  font-weight: 800;
  white-space: nowrap;
}

.state-pill.without_followup {
  background: #fff1df;
  color: #ad5c08;
}

.state-pill.in_progress {
  background: #e7f7f3;
  color: #087c6c;
}

.state-pill.completed {
  background: #e8f8ee;
  color: #18794e;
}

.followup-count-cell strong,
.latest-cell strong {
  color: #254665;
  font-size: 13px;
}

.followup-count-cell span,
.latest-cell span {
  color: #75899f;
  font-size: 11px;
}

.progress-cell > div {
  display: grid;
  grid-template-columns: 42px minmax(80px, 1fr);
  align-items: center;
  gap: 8px;
}

.progress-cell span {
  color: #214969;
  font-size: 12px;
  font-weight: 800;
  text-align: right;
}

.progress-track {
  height: 7px;
}

.action-cell button {
  border-color: #1e63ff;
  background: #1e63ff;
  color: #fff;
}

.action-cell button:hover,
.action-cell button:focus-visible {
  border-color: #1458d2;
  outline: 0;
  background: #1458d2;
  box-shadow: 0 0 0 3px rgba(30, 99, 255, 0.14);
}

.empty-state {
  min-height: 180px;
  display: grid;
  place-items: center;
  color: #71849a;
  font-size: 13px;
}

.status-pager {
  min-height: 52px;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 10px;
  border-top: 1px solid #e2eaf4;
}

.status-pager span {
  color: #637a94;
  font-size: 12px;
  font-weight: 750;
}

.spinning {
  animation: spin 0.9s linear infinite;
}

@keyframes spin {
  to { transform: rotate(360deg); }
}

.history-period-tabs {
  flex: 0 0 auto;
  display: inline-flex;
  gap: 4px;
  border: 1px solid #d7e3f2;
  border-radius: 9px;
  padding: 3px;
  background: #f1f6fc;
}

.history-period-tabs button {
  min-height: 32px;
  display: inline-flex;
  align-items: center;
  gap: 6px;
  border: 0;
  border-radius: 7px;
  padding: 0 10px;
  background: transparent;
  color: #48637f;
  font: inherit;
  font-size: 12px;
  font-weight: 700;
  cursor: pointer;
}

.history-period-tabs button.active,
.history-period-tabs button:hover,
.history-period-tabs button:focus-visible {
  outline: 0;
  background: #fff;
  color: #1458bd;
  box-shadow: 0 2px 8px rgba(19, 73, 145, 0.12);
}

.history-period-tabs b {
  color: inherit;
  font-size: 11px;
}

@media (max-width: 1180px) {
  .status-table-head {
    display: none;
  }

  .status-row {
    grid-template-columns: minmax(260px, 1.8fr) 110px minmax(140px, 0.8fr) 108px;
  }

  .followup-count-cell,
  .latest-cell {
    display: none;
  }

}

@media (max-width: 760px) {
  .repair-status-page { padding: 12px; }
  .status-summary { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .status-row { grid-template-columns: 1fr auto; }
  .status-row > div:not(.project-cell):not(.action-cell) { display: none; }
  .history-period-tabs { width: 100%; }
  .history-period-tabs button { flex: 1; }
  .status-search { max-width: none; flex-basis: 100%; }
}

@media (prefers-reduced-motion: reduce) {
  .status-summary > button,
  .spinning { transition: none; animation: none; }
}
</style>
