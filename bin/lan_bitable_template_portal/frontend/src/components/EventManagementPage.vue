<template>
  <section class="event-page">
    <div class="page-back-row">
      <VnetBackButton v-if="showingDetails" @click="returnToOverview" />
      <VnetBackButton v-else to="/" />
    </div>
    <div class="event-command-card">
      <div class="event-command-head">
        <div class="event-title-block">
          <span class="section-kicker">事件管理</span>
          <h2>事件态势与楼栋入口</h2>
        </div>
        <div class="event-command-actions">
          <span class="update-pill">{{ lastRefreshCompactText }}</span>
          <label class="month-picker">
            <span>月份</span>
            <input v-model="selectedMonth" type="month" :disabled="loading || refreshing" />
          </label>
          <button type="button" class="btn quiet" :disabled="loading" title="只重新读取当前页面数据" @click="loadEvents()">
            {{ loading ? "刷新中" : "刷新本页" }}
          </button>
          <button type="button" class="btn secondary source-refresh" :disabled="refreshing" title="刷新事件" @click="refreshEvents()">
            {{ refreshing ? "刷新中" : "刷新事件" }}
          </button>
        </div>
      </div>

      <MessageBanner v-if="errorText" tone="failed" :text="errorText" />
      <MessageBanner
        v-else-if="configMissing"
        tone="warning"
        :text="configError || '未配置事件数据，请先在 Qt 设置中填写事件数据表。'"
      />
      <MessageBanner
        v-else-if="lastFailedError"
        tone="warning"
        :text="`最近刷新失败，当前仍显示上一次成功数据：${lastFailedError}`"
      />
      <EventStatsCards :cards="statCards" />

      <div class="event-work-surface" :class="{ detail: showingDetails }">
        <EventBuildingOverview
          v-if="!showingDetails"
          :cards="buildingCards"
          :active-code="activeBuildingCode"
          :loading="loading"
          @select="openBuilding"
        />
        <EventPriorityPanel
          :detail="showingDetails"
          :detail-scope-label="detailScopeLabel"
          :scope-label="scopeLabel"
          :priority-events="priorityEvents"
          :high-priority-count="highPriorityEvents.length"
          :pending-count="pendingEventsCount"
          :events-count="events.length"
          :overview-stats="overviewStats"
          :allowed-count="allowedBuildingCodes.size"
          @select="selectedEvent = $event || null"
        />
      </div>
    </div>

    <div v-if="showingDetails" class="event-list-panel">
      <div class="event-list-head">
        <div>
          <span class="section-kicker">事件明细</span>
          <strong>{{ detailScopeLabel }}月度事件列表</strong>
          <span>{{ filteredEvents.length }} / {{ events.length }} 条</span>
        </div>
        <div class="event-list-tools">
          <input v-model="searchText" type="search" placeholder="搜索标题、告警描述、专业" aria-label="搜索事件" />
          <select v-model="statusFilter" aria-label="状态筛选">
            <option value="">全部状态</option>
            <option v-for="item in statusOptions" :key="item" :value="item">{{ item }}</option>
          </select>
          <select v-model="levelFilter" aria-label="等级筛选">
            <option value="">全部等级</option>
            <option v-for="item in levelOptions" :key="item" :value="item">{{ item }}</option>
          </select>
          <select v-model="sourceFilter" aria-label="来源筛选">
            <option value="">全部来源</option>
            <option v-for="item in sourceOptions" :key="item" :value="item">{{ item }}</option>
          </select>
          <select v-model="specialtyFilter" aria-label="专业筛选">
            <option value="">全部专业</option>
            <option v-for="item in specialtyOptions" :key="item" :value="item">{{ item }}</option>
          </select>
          <button type="button" v-if="activeFilterCount" class="btn quiet" @click="clearFilters">
            清空筛选
          </button>
        </div>
      </div>

      <div class="event-scope-summary" aria-label="当前事件查看范围">
        <span v-for="item in visibleFilterSummaryItems" :key="item.label" :class="{ active: item.active }">
          <small>{{ item.label }}</small>
          <strong>{{ item.value }}</strong>
        </span>
      </div>

      <div v-if="loading" class="event-empty">
        <strong>正在读取事件数据</strong>
      </div>
      <div v-else-if="!events.length" class="event-empty">
        <strong>本月暂无事件</strong>
      </div>
      <div v-else-if="!filteredEvents.length" class="event-empty">
        <strong>没有符合筛选条件的事件</strong>
      </div>
      <EventVirtualList
        v-else
        :records="filteredEvents"
        :scope-label="scopeLabel"
        @select="selectedEvent = $event || null"
      />
    </div>

    <div v-if="selectedEvent" class="event-drawer-backdrop" @click.self="selectedEvent = null">
      <aside class="event-drawer">
        <header>
          <div>
            <span class="section-kicker">事件详情</span>
            <h3>{{ selectedEvent.title || selectedEvent.alarm_desc || "未命名事件" }}</h3>
          </div>
          <button type="button" class="drawer-close" @click="selectedEvent = null">关闭</button>
        </header>
        <div class="event-detail-summary">
          <span class="event-level-chip" :class="levelTone(selectedEvent.level)">{{ selectedEvent.level || "未填写等级" }}</span>
          <span class="status-pill" :class="statusTone(selectedEvent.status)">{{ selectedEvent.status || "未知" }}</span>
          <small>{{ selectedEvent.occurrence_time || "未填写发生时间" }}</small>
        </div>
        <section class="event-repair-actions">
          <div>
            <small>是否转检修</small>
            <strong :class="{ enabled: eventTransferEnabled(selectedEvent) }">
              {{ eventRepairFlowLabel(selectedEvent) }}
            </strong>
            <em>{{ eventRepairFlowHint(selectedEvent) }}</em>
          </div>
          <button type="button"
            class="btn secondary"
            :disabled="eventTransferBusy || eventTransferEnabled(selectedEvent)"
            @click="markSelectedEventTransferred"
          >
            {{ eventTransferBusy ? "处理中" : eventTransferEnabled(selectedEvent) ? "已转检修" : "标记转检修" }}
          </button>
          <button type="button" class="btn primary" @click="openRepairManagementForSelectedEvent">
            填写/选择维修单
          </button>
        </section>
        <div class="event-detail-tabs" aria-label="事件详情分区">
          <button type="button"
            :class="{ active: selectedEventDetailTab === 'summary' }"
            @click="selectedEventDetailTab = 'summary'"
          >
            概要
          </button>
          <button type="button"
            :class="{ active: selectedEventDetailTab === 'timeline' }"
            @click="selectedEventDetailTab = 'timeline'"
          >
            时间线
          </button>
          <button type="button"
            v-if="isAdmin"
            :class="{ active: selectedEventDetailTab === 'fields' }"
            @click="selectedEventDetailTab = 'fields'"
          >
            管理员字段
          </button>
        </div>
        <div v-if="selectedEventDetailTab === 'summary'" class="detail-grid">
          <article v-for="field in detailFields" :key="field.key">
            <small>{{ field.label }}</small>
            <strong>{{ field.value || "未填写" }}</strong>
          </article>
        </div>
        <section v-if="selectedEventDetailTab === 'timeline'" class="timeline">
          <h4>事件时间线</h4>
          <ol>
            <li v-for="node in timelineItems" :key="node.label" :class="{ muted: !node.value }">
              <span></span>
              <div>
                <strong>{{ node.label }}</strong>
                <small>{{ node.value || "未填写" }}</small>
              </div>
            </li>
          </ol>
        </section>
        <section v-if="isAdmin && selectedEventDetailTab === 'fields'" class="full-fields">
          <h4>管理员字段</h4>
          <dl>
            <template v-for="item in visibleDisplayFields" :key="item.key">
              <dt>{{ item.key }}</dt>
              <dd>{{ item.value || "未填写" }}</dd>
            </template>
          </dl>
        </section>
        <a
          v-if="selectedEvent.source_record_url"
          class="source-link"
          :href="selectedEvent.source_record_url"
          target="_blank"
          rel="noopener noreferrer"
        >
          打开事件多维表
        </a>
      </aside>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from "vue";
import { requestJson } from "../api/client";
import {
  EVENT_BUILDING_SCOPE_CODES as BUILDING_SCOPE_CODES,
  EVENT_BUILDING_ORDER as BUILDING_ORDER,
  type EventBuildingCard as BuildingCard,
  buildEventBuildingCardFromStats as buildBuildingCardFromStats,
  eventBuildingCodesForItem as buildingCodesForItem,
  eventBuildingSortIndex as buildingSortIndex,
  eventLevelTone as levelTone,
  eventMatchesBuilding,
  eventPriorityScore as priorityScore,
  eventRecordId,
  eventRepairFlowHint,
  eventRepairFlowLabel,
  eventScopeText as scopeText,
  eventStatusTone as statusTone,
  eventTransferEnabled,
  formatEventEpoch as formatEpoch,
  isHighLevelEvent as isHighLevel,
  isTechnicalEventDisplayField as isTechnicalDisplayField,
  normalizeEventScope as normalizeScope,
  uniqueEventOptions as uniqueOptions,
} from "../eventManagementUtils";
import { navigate } from "../navigation";
import type { LooseDict, ScopeOption } from "../types";
import EventBuildingOverview from "./EventBuildingOverview.vue";
import EventPriorityPanel from "./EventPriorityPanel.vue";
import EventStatsCards from "./EventStatsCards.vue";
import EventVirtualList from "./EventVirtualList.vue";
import MessageBanner from "./MessageBanner.vue";
import VnetBackButton from "./VnetBackButton.vue";

const props = defineProps<{
  scope: string;
  scopeOptions: ScopeOption[];
  refreshNonce?: number;
  isAdmin?: boolean;
}>();

const emit = defineEmits<{
  refreshing: [value: boolean];
  status: [value: string];
  "switch-scope": [scope: string, detail?: boolean];
}>();

const selectedMonth = ref(new Date().toISOString().slice(0, 7));
const loading = ref(false);
const refreshing = ref(false);
const errorText = ref("");
const events = ref<LooseDict[]>([]);
const stats = ref<LooseDict>({});
const overviewStats = ref<LooseDict>({});
const overviewBuildingStats = ref<LooseDict[]>([]);
const lastRefreshedAt = ref(0);
const lastFailed = ref<LooseDict>({});
const configMissing = ref(false);
const configError = ref("");
const searchText = ref("");
const buildingFilter = ref("");
const statusFilter = ref("");
const levelFilter = ref("");
const sourceFilter = ref("");
const specialtyFilter = ref("");
const initialDetailRequested = new URLSearchParams(window.location.search).get("detail") === "1";
const initialDetailScope = initialDetailRequested ? normalizeScope(props.scope) : "";
const detailScope = ref(initialDetailScope);
const detailRequested = ref(initialDetailRequested);
const selectedEvent = ref<LooseDict | null>(null);
const selectedEventDetailTab = ref<"summary" | "timeline" | "fields">("summary");
const eventTransferBusy = ref(false);

const scopeLabel = computed(() => {
  const normalized = normalizeScope(props.scope);
  return props.scopeOptions.find((item) => normalizeScope(item.value) === normalized)?.label || scopeText(normalized);
});
const showingDetails = computed(() => Boolean(detailScope.value));
const detailScopeLabel = computed(() => detailScope.value ? scopeText(detailScope.value) : "");
const activeBuildingCode = computed(() => detailScope.value || "");

const lastFailedError = computed(() => String(lastFailed.value?.error || "").trim());
const lastRefreshText = computed(() => {
  if (!lastRefreshedAt.value) return "尚无成功更新";
  return `最近成功更新：${formatEpoch(lastRefreshedAt.value)}`;
});
const lastRefreshCompactText = computed(() => {
  if (!lastRefreshedAt.value) return "尚无成功更新";
  const date = new Date(Number(lastRefreshedAt.value) * 1000);
  if (Number.isNaN(date.getTime())) return "数据已更新";
  return `数据更新 ${date.toLocaleTimeString("zh-CN", { hour: "2-digit", minute: "2-digit", hour12: false })}`;
});

const overviewPendingCount = computed(() => Number(overviewStats.value.pending || stats.value.pending || 0));
const pendingEventsCount = computed(() => events.value.filter((item) => statusTone(item.status) === "recovered").length);
const highPriorityEvents = computed(() => {
  return events.value.filter((item) => isHighLevel(item));
});
const statCards = computed(() => [
  {
    key: "total",
    label: "本月新增事件",
    value: Number(overviewStats.value.total || stats.value.total || events.value.length || 0),
    unit: "件",
    badge: "月度总览",
    tone: "blue",
    icon: "+",
  },
  {
    key: "pending",
    label: "挂起中事件",
    value: overviewPendingCount.value,
    unit: "件",
    badge: overviewPendingCount.value ? "需跟进" : "暂无挂起",
    tone: "amber",
    icon: "Ⅱ",
  },
  {
    key: "high",
    label: "重点事件",
    value: Number(overviewStats.value.high_level || stats.value.high_level || highPriorityEvents.value.length || 0),
    unit: "件",
    badge: Number(overviewStats.value.high_level || stats.value.high_level || 0) ? "需优先处置" : "运行平稳",
    tone: "rose",
    icon: "!",
  },
  {
    key: "ended",
    label: "本月闭环",
    value: Number(overviewStats.value.ended || stats.value.ended || 0),
    unit: "件",
    badge: `闭环率 ${closureRate.value}`,
    tone: "emerald",
    icon: "✓",
  },
]);

const closureRate = computed(() => {
  const total = Number(overviewStats.value.total || stats.value.total || events.value.length || 0);
  if (!total) return "0%";
  const ended = Number(overviewStats.value.ended || stats.value.ended || 0);
  return `${Math.round((ended / total) * 1000) / 10}%`;
});

const allowedBuildingCodes = computed(() => {
  const values = props.scopeOptions.map((item) => normalizeScope(item.value)).filter(Boolean);
  if (values.includes("ALL")) return new Set(BUILDING_ORDER.filter((code) => BUILDING_SCOPE_CODES.includes(code)));
  const codes = new Set<string>();
  for (const value of values) {
    if (value === "CAMPUS") {
      ["A", "B", "C"].forEach((code) => codes.add(code));
    } else if (BUILDING_SCOPE_CODES.includes(value)) {
      codes.add(value);
    }
  }
  return codes;
});

const buildingCards = computed<BuildingCard[]>(() => {
  if (overviewBuildingStats.value.length) {
    return overviewBuildingStats.value
      .map((item) => buildBuildingCardFromStats(item, allowedBuildingCodes.value))
      .sort((left, right) => buildingSortIndex(left.code) - buildingSortIndex(right.code));
  }
  const codes = new Set<string>();
  for (const option of props.scopeOptions || []) {
    const code = normalizeScope(option.value);
    if (code && !["ALL", "CAMPUS"].includes(code)) codes.add(code);
  }
  for (const item of events.value) {
    for (const code of buildingCodesForItem(item)) {
      if (code && !["ALL", "CAMPUS"].includes(code)) codes.add(code);
    }
  }
  const current = normalizeScope(props.scope);
  if (current && !["ALL", "CAMPUS"].includes(current)) codes.add(current);
  const sortedCodes = Array.from(codes).sort((left, right) => buildingSortIndex(left) - buildingSortIndex(right));
  return sortedCodes.map((code) => {
    const rows = events.value.filter((item) => eventMatchesBuilding(item, code, props.scope));
    const processing = rows.filter((item) => statusTone(item.status) === "processing").length;
    const pending = rows.filter((item) => statusTone(item.status) === "recovered").length;
    const ended = rows.filter((item) => statusTone(item.status) === "ended").length;
    const high = rows.filter((item) => isHighLevel(item)).length;
    const tone = high ? "critical" : pending ? "warning" : processing ? "active" : "stable";
    const statusLabel = high ? `重点 ${high}` : pending ? `挂起 ${pending}` : processing ? "待处理" : "运行平稳";
    return {
      code,
      label: scopeText(code),
      total: rows.length,
      processing,
      pending,
      ended,
      high,
      tone,
      statusLabel,
      allowed: allowedBuildingCodes.value.has(code),
    };
  });
});

const filteredEvents = computed(() => {
  if (!showingDetails.value) return [];
  const query = searchText.value.trim().toLowerCase();
  return events.value.filter((item) => {
    if (buildingFilter.value && !eventMatchesBuilding(item, buildingFilter.value, props.scope)) return false;
    if (statusFilter.value && String(item.status || "") !== statusFilter.value) return false;
    if (levelFilter.value && String(item.level || "") !== levelFilter.value) return false;
    if (sourceFilter.value && String(item.source || "") !== sourceFilter.value) return false;
    if (specialtyFilter.value && String(item.specialty || "") !== specialtyFilter.value) return false;
    if (!query) return true;
    const haystack = [
      item.title,
      item.alarm_desc,
      item.level,
      item.source,
      item.building,
      item.specialty,
    ].map((value) => String(value || "").toLowerCase()).join("\n");
    return haystack.includes(query);
  });
});

const priorityEvents = computed(() => {
  return filteredEvents.value
    .slice()
    .sort((left, right) => {
      const leftScore = priorityScore(left);
      const rightScore = priorityScore(right);
      if (leftScore !== rightScore) return rightScore - leftScore;
      return String(right.occurrence_time || right.progress_update || "")
        .localeCompare(String(left.occurrence_time || left.progress_update || ""));
    })
    .slice(0, 6);
});
const statusOptions = computed(() => uniqueOptions(events.value.map((item) => item.status)));
const levelOptions = computed(() => uniqueOptions(events.value.map((item) => item.level)));
const sourceOptions = computed(() => uniqueOptions(events.value.map((item) => item.source)));
const specialtyOptions = computed(() => uniqueOptions(events.value.map((item) => item.specialty)));
const activeFilterCount = computed(() => {
  return [searchText.value, buildingFilter.value, statusFilter.value, levelFilter.value, sourceFilter.value, specialtyFilter.value]
    .filter((value) => String(value || "").trim()).length;
});
const filterSummaryItems = computed(() => [
  { label: "楼栋", value: buildingFilter.value ? scopeText(buildingFilter.value) : detailScopeLabel.value || scopeLabel.value, active: true },
  { label: "月份", value: selectedMonth.value || "当前月", active: true },
  { label: "状态", value: statusFilter.value || "全部状态", active: Boolean(statusFilter.value) },
  { label: "等级", value: levelFilter.value || "全部等级", active: Boolean(levelFilter.value) },
  { label: "来源", value: sourceFilter.value || "全部来源", active: Boolean(sourceFilter.value) },
  { label: "专业", value: specialtyFilter.value || "全部专业", active: Boolean(specialtyFilter.value) },
]);
const visibleFilterSummaryItems = computed(() => (
  filterSummaryItems.value.filter((item) => item.active || item.label === "楼栋" || item.label === "月份")
));

const detailFields = computed(() => {
  const item = selectedEvent.value || {};
  return [
    { key: "level", label: "事件等级", value: item.level },
    { key: "building", label: "机楼", value: item.building },
    { key: "specialty", label: "专业", value: item.specialty },
    { key: "source", label: "事件发现来源", value: item.source },
    { key: "status", label: "状态", value: item.status },
    { key: "transfer", label: "检修链路", value: eventRepairFlowLabel(item) },
  ];
});

const timelineItems = computed(() => {
  const item = selectedEvent.value || {};
  return [
    { label: "事件发生时间", value: item.occurrence_time },
    { label: "进展响应时间", value: item.response_time },
    { label: "进展更新时间", value: item.progress_update },
    { label: "事件恢复时间", value: item.recover_time },
    { label: "事件结束时间", value: item.end_time },
  ];
});

const visibleDisplayFields = computed(() => {
  const fields = selectedEvent.value?.display_fields;
  if (!fields || typeof fields !== "object") return [];
  return Object.entries(fields)
    .map(([key, value]) => ({ key, value: String(value || "") }))
    .filter((item) => item.key && item.value && !isTechnicalDisplayField(item.key))
    .slice(0, 80);
});

function setEventTransferState(recordId: string): void {
  if (!recordId) return;
  if (selectedEvent.value && eventRecordId(selectedEvent.value) === recordId) {
    selectedEvent.value = { ...selectedEvent.value, transfer_to_overhaul: "True" };
  }
  events.value = events.value.map((item) => (
    eventRecordId(item) === recordId ? { ...item, transfer_to_overhaul: "True" } : item
  ));
}

async function markSelectedEventTransferred(): Promise<void> {
  const recordId = eventRecordId(selectedEvent.value);
  if (!recordId) {
    errorText.value = "当前事件缺少记录 ID，无法转检修。";
    return;
  }
  eventTransferBusy.value = true;
  try {
    await requestJson("/api/events/transfer-repair", {
      method: "POST",
      body: JSON.stringify({
        scope: detailScope.value || props.scope || "ALL",
        month: selectedMonth.value,
        record_id: recordId,
      }),
    });
    setEventTransferState(recordId);
    emit("status", "事件已标记为转检修。");
  } catch (error: unknown) {
    errorText.value = error instanceof Error ? error.message : "转检修失败。";
    emit("status", errorText.value);
  } finally {
    eventTransferBusy.value = false;
  }
}

function openRepairManagementForSelectedEvent(): void {
  const item = selectedEvent.value || {};
  const url = new URL("/repair-management", window.location.origin);
  url.searchParams.set("scope", detailScope.value || props.scope || "ALL");
  url.searchParams.set("mode", "create");
  const recordId = eventRecordId(item);
  if (recordId) url.searchParams.set("from_event_record_id", recordId);
  const title = String(item.title || item.alarm_desc || "").trim();
  if (title) url.searchParams.set("event_title", title);
  navigate(url);
}

function openBuilding(code: string): void {
  const nextScope = normalizeScope(code);
  if (!allowedBuildingCodes.value.has(nextScope)) return;
  detailRequested.value = true;
  detailScope.value = nextScope;
  setEventDetailUrlFlag(true);
  clearFilters();
  selectedEvent.value = null;
  if (nextScope && nextScope !== normalizeScope(props.scope)) {
    emit("switch-scope", nextScope, true);
    return;
  }
  void loadEvents();
}

function returnToOverview(): void {
  detailRequested.value = false;
  detailScope.value = "";
  setEventDetailUrlFlag(false);
  selectedEvent.value = null;
  clearFilters();
  events.value = [];
  stats.value = {};
  void loadEvents();
}

function setEventDetailUrlFlag(enabled: boolean): void {
  const url = new URL(window.location.href);
  if (enabled) {
    url.searchParams.set("detail", "1");
  } else {
    url.searchParams.delete("detail");
  }
  window.history.replaceState({}, "", url.toString());
}

function clearFilters(): void {
  searchText.value = "";
  buildingFilter.value = "";
  statusFilter.value = "";
  levelFilter.value = "";
  sourceFilter.value = "";
  specialtyFilter.value = "";
}

async function loadEvents(): Promise<void> {
  if (!props.scope) return;
  loading.value = true;
  errorText.value = "";
  try {
    await loadEventOverview().catch(() => {
      overviewStats.value = {};
      overviewBuildingStats.value = [];
    });
    if (detailScope.value) {
      await loadMonthlyEvents(detailScope.value);
      emit("status", events.value.length ? `${detailScopeLabel.value}事件数据已就绪` : `${detailScopeLabel.value}本月暂无事件`);
    } else {
      events.value = [];
      stats.value = {};
      selectedEvent.value = null;
      emit("status", "事件态势已就绪");
    }
  } catch (error: unknown) {
    errorText.value = error instanceof Error ? error.message : "事件数据读取失败。";
    emit("status", errorText.value);
  } finally {
    loading.value = false;
  }
}

async function loadMonthlyEvents(scope: string): Promise<void> {
  const params = new URLSearchParams({ scope, month: selectedMonth.value });
  const payload = await requestJson(`/api/events/monthly?${params.toString()}`);
  events.value = Array.isArray(payload.records) ? payload.records : [];
  stats.value = payload.stats && typeof payload.stats === "object" ? payload.stats : {};
  lastRefreshedAt.value = Number(payload.last_refreshed_at || lastRefreshedAt.value || 0);
  lastFailed.value = payload.last_failed && typeof payload.last_failed === "object" ? payload.last_failed : {};
  configMissing.value = Boolean(payload.config_missing);
  configError.value = String(payload.config_error || "");
}

async function loadEventOverview(): Promise<void> {
  const params = new URLSearchParams({ month: selectedMonth.value });
  const payload = await requestJson(`/api/events/overview?${params.toString()}`);
  overviewStats.value = payload.stats && typeof payload.stats === "object" ? payload.stats : {};
  overviewBuildingStats.value = Array.isArray(payload.building_stats) ? payload.building_stats : [];
  lastRefreshedAt.value = Number(payload.last_refreshed_at || lastRefreshedAt.value || 0);
  lastFailed.value = payload.last_failed && typeof payload.last_failed === "object" ? payload.last_failed : {};
  configMissing.value = Boolean(payload.config_missing);
  configError.value = String(payload.config_error || "");
}

async function refreshEvents(): Promise<void> {
  if (!props.scope) return;
  refreshing.value = true;
  emit("refreshing", true);
  emit("status", "正在刷新事件。");
  errorText.value = "";
  try {
    const refreshScope = detailScope.value || props.scope;
    const params = new URLSearchParams({ scope: refreshScope, month: selectedMonth.value });
    const payload = await requestJson(`/api/events/refresh?${params.toString()}`, { method: "POST" });
    await loadEventOverview().catch(() => null);
    if (detailScope.value) {
      events.value = Array.isArray(payload.records) ? payload.records : events.value;
      stats.value = payload.stats && typeof payload.stats === "object" ? payload.stats : stats.value;
    } else {
      events.value = [];
      stats.value = {};
    }
    lastRefreshedAt.value = Number(payload.last_refreshed_at || payload.updated_at || lastRefreshedAt.value || 0);
    lastFailed.value = payload.last_failed && typeof payload.last_failed === "object" ? payload.last_failed : {};
    configMissing.value = Boolean(payload.config_missing);
    configError.value = String(payload.config_error || "");
    emit("status", "事件已刷新，页面已更新。");
  } catch (error: unknown) {
    errorText.value = error instanceof Error ? error.message : "事件刷新失败。";
    emit("status", `事件刷新失败：${errorText.value}`);
    await loadEvents();
  } finally {
    refreshing.value = false;
    emit("refreshing", false);
  }
}

watch(
  () => [props.scope, selectedMonth.value],
  () => {
    selectedEvent.value = null;
    buildingFilter.value = "";
    const normalizedScope = normalizeScope(props.scope);
    if (detailRequested.value && BUILDING_SCOPE_CODES.includes(normalizedScope)) {
      detailScope.value = normalizedScope;
    } else if (!detailRequested.value) {
      detailScope.value = "";
    }
    void loadEvents();
  },
);

watch(selectedEvent, () => {
  selectedEventDetailTab.value = "summary";
});

watch(
  () => props.isAdmin,
  (value) => {
    if (!value && selectedEventDetailTab.value === "fields") {
      selectedEventDetailTab.value = "summary";
    }
  },
);

watch(
  () => props.refreshNonce,
  (value, oldValue) => {
    if (value && value !== oldValue) void refreshEvents();
  },
);

onMounted(() => {
  void loadEvents();
});
</script>

<style scoped>
.event-page {
  width: min(1880px, 100%);
  margin: 0 auto;
  padding: 26px 42px 48px;
  display: grid;
  gap: 18px;
}

.page-back-row {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  justify-content: flex-start;
  gap: 8px;
}

.page-back-btn {
  min-height: 36px;
  padding: 0 13px;
  border-radius: 999px;
}

.page-back-btn span {
  font-size: 19px;
  line-height: 1;
}

.event-command-card,
.event-list-panel {
  border: 1px solid #d8e5f7;
  border-radius: 28px;
  background:
    linear-gradient(180deg, rgba(255, 255, 255, 0.96), rgba(248, 251, 255, 0.94)),
    radial-gradient(circle at 86% 10%, rgba(30, 99, 255, 0.08), transparent 30%);
  box-shadow: 0 24px 64px rgba(0, 47, 135, 0.12);
}

.event-command-card {
  padding: 22px 28px 28px;
  display: grid;
  gap: 18px;
}

.event-command-head,
.event-list-head {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 18px;
}

.event-title-block h2 {
  margin: 14px 0 4px;
  color: #071a39;
  font-size: 27px;
  font-weight: 950;
  line-height: 1.15;
}

.event-title-block p,
.event-empty,
.event-list-head span,
.timeline small,
.full-fields dd {
  color: #5e728f;
}

.section-kicker {
  display: inline-flex;
  align-items: center;
  min-height: 28px;
  padding: 0 14px;
  border: 1px solid #d8e7f8;
  border-radius: 999px;
  background: rgba(239, 246, 255, 0.9);
  color: #0e5bd8;
  font-size: 12px;
  font-weight: 950;
}

.event-command-actions {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: 10px;
  flex-wrap: wrap;
}

.update-pill,
.event-inline-hint {
  border: 1px solid #d8e5f7;
  border-radius: 999px;
  background: rgba(255, 255, 255, 0.76);
  color: #6b7f9d;
  font-size: 12px;
  font-weight: 850;
  padding: 9px 13px;
}

.event-inline-hint {
  justify-self: start;
  border-color: #cfe0ff;
  background: rgba(239, 246, 255, 0.92);
  color: #3156c9;
}

.month-picker {
  min-width: 160px;
  display: grid;
  gap: 5px;
  color: #516a88;
  font-size: 12px;
  font-weight: 900;
}

.month-picker input,
.event-list-tools input,
.event-list-tools select {
  min-height: 42px;
  border: 1px solid #d8e5f7;
  border-radius: 16px;
  background: #fff;
  color: #0b1f3f;
  font: inherit;
  font-weight: 800;
  padding: 0 14px;
}

.btn.quiet {
  min-height: 40px;
  border-color: #cfe0ff;
  background: #f8fbff;
  color: #3156c9;
}

.btn.source-refresh {
  min-height: 40px;
  border-color: #d8e5f7;
  background: rgba(255, 255, 255, 0.86);
  color: #0e4fb2;
}

.btn {
  min-height: 42px;
  border: 1px solid #d8e5f7;
  border-radius: 16px;
  padding: 0 16px;
  font: inherit;
  font-size: 14px;
  font-weight: 950;
  cursor: pointer;
}

.btn.secondary {
  background: #f7fbff;
  color: #0e4fb2;
}

.btn.primary {
  border-color: #1e63ff;
  background: linear-gradient(135deg, #2a77ff, #0050d9);
  color: #fff;
}

.btn:disabled {
  cursor: not-allowed;
  opacity: 0.65;
}

.event-work-surface {
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(360px, 0.36fr);
  gap: 22px;
}

.event-work-surface.detail {
  grid-template-columns: 1fr;
}

.event-list-panel {
  padding: 18px;
  display: grid;
  gap: 12px;
}

.event-list-head {
  align-items: center;
}

.event-list-head > div:first-child {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
}

.event-list-head strong {
  color: #071a39;
  font-size: 18px;
  font-weight: 950;
}

.event-list-head small {
  color: #6b7f9d;
  font-size: 12px;
}

.event-list-tools {
  display: flex;
  justify-content: flex-end;
  gap: 8px;
  flex-wrap: wrap;
}

.event-list-tools input {
  width: 240px;
}

.event-list-tools select {
  width: auto;
  min-width: 122px;
}

.event-scope-summary {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}

.event-scope-summary span {
  min-height: 38px;
  display: inline-flex;
  align-items: center;
  gap: 7px;
  padding: 6px 11px;
  border: 1px solid rgba(216, 229, 247, 0.92);
  border-radius: 999px;
  background: #f8fbff;
  color: #64748b;
  font-size: 12px;
  font-weight: 850;
}

.event-scope-summary span.active {
  border-color: #bdd7ff;
  background: #edf5ff;
  color: #0e5bd8;
}

.event-scope-summary small {
  color: inherit;
  font-size: 11px;
  font-weight: 850;
  opacity: 0.78;
}

.event-scope-summary strong {
  max-width: 170px;
  overflow: hidden;
  color: inherit;
  font-size: 12px;
  font-weight: 950;
  text-overflow: ellipsis;
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

.event-level-chip.normal,
.event-level-chip {
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

.event-empty {
  min-height: 140px;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 7px;
  border: 1px dashed #bdd2f4;
  border-radius: 18px;
  background: rgba(247, 251, 255, 0.8);
  padding: 18px;
  text-align: center;
}

.event-empty.compact {
  min-height: 112px;
  margin-top: 14px;
}

.event-empty strong {
  color: #0c2d63;
  font-size: 15px;
  font-weight: 950;
}

.event-empty p {
  max-width: 520px;
  margin: 0;
  color: #5e728f;
  font-size: 13px;
  font-weight: 800;
  line-height: 1.55;
}

.event-drawer-backdrop {
  position: fixed;
  inset: 0;
  z-index: var(--cf-z-modal-backdrop, 800);
  display: flex;
  justify-content: flex-end;
  background: rgba(6, 21, 48, 0.28);
}

.event-drawer {
  width: min(680px, 100vw);
  height: 100%;
  overflow: auto;
  display: grid;
  align-content: start;
  gap: 16px;
  padding: 24px;
  background: #f7fbff;
  box-shadow: -20px 0 48px rgba(7, 37, 86, 0.22);
}

.event-drawer header {
  display: flex;
  align-items: start;
  justify-content: space-between;
  gap: 16px;
}

.event-drawer h3 {
  margin: 6px 0 0;
  color: #071a39;
  font-size: 22px;
  font-weight: 950;
  line-height: 1.25;
}

.event-detail-summary {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
  border: 1px solid #d8e5f7;
  border-radius: 18px;
  background: #fff;
  padding: 12px 14px;
}

.event-detail-summary small {
  color: #516a88;
  font-size: 12px;
  font-weight: 900;
}

.event-repair-actions {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
  border: 1px solid #d8e5f7;
  border-radius: 18px;
  background: #f8fbff;
  padding: 12px 14px;
}

.event-repair-actions > div {
  min-width: 150px;
  display: grid;
  gap: 2px;
  margin-right: auto;
}

.event-repair-actions small {
  color: #64748b;
  font-size: 12px;
  font-weight: 900;
}

.event-repair-actions em {
  color: #4f6684;
  font-size: 12px;
  font-style: normal;
  font-weight: 780;
  line-height: 1.45;
}

.event-repair-actions strong {
  color: #c2410c;
  font-weight: 950;
}

.event-repair-actions strong.enabled {
  color: #047857;
}

.event-detail-tabs {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  border: 1px solid #d8e5f7;
  border-radius: 18px;
  background: rgba(255, 255, 255, 0.84);
  padding: 6px;
}

.event-detail-tabs button {
  min-height: 34px;
  border: 0;
  border-radius: 14px;
  padding: 7px 13px;
  background: transparent;
  color: #48627f;
  font: inherit;
  font-size: 13px;
  font-weight: 900;
  cursor: pointer;
}

.event-detail-tabs button.active {
  background: linear-gradient(135deg, #1e63ff, #1554df);
  color: #ffffff;
  box-shadow: 0 10px 20px rgba(30, 99, 255, 0.18);
}

.drawer-close,
.source-link {
  border: 1px solid #d8e5f7;
  border-radius: 14px;
  background: #fff;
  color: #0e4fb2;
  padding: 10px 14px;
  font: inherit;
  font-weight: 950;
  text-decoration: none;
  cursor: pointer;
}

.detail-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 10px;
}

.detail-grid article,
.timeline,
.full-fields {
  border: 1px solid #d8e5f7;
  border-radius: 18px;
  background: #fff;
  padding: 14px;
}

.detail-grid small {
  display: block;
  color: #6b7f9d;
  font-size: 12px;
  font-weight: 900;
}

.detail-grid strong {
  display: block;
  margin-top: 5px;
  color: #071a39;
  font-weight: 950;
  word-break: break-word;
}

.timeline h4,
.full-fields h4 {
  margin: 0 0 10px;
  color: #071a39;
  font-size: 15px;
  font-weight: 950;
}

.timeline ol {
  list-style: none;
  margin: 0;
  padding: 0;
  display: grid;
  gap: 10px;
}

.timeline li {
  display: grid;
  grid-template-columns: 16px 1fr;
  gap: 8px;
}

.timeline li > span {
  width: 10px;
  height: 10px;
  margin-top: 4px;
  border-radius: 50%;
  background: #1e63ff;
}

.timeline li.muted > span {
  background: #cbd5e1;
}

.timeline strong {
  color: #071a39;
  font-weight: 950;
}

.full-fields dl {
  margin: 0;
  display: grid;
  grid-template-columns: 150px minmax(0, 1fr);
  gap: 8px 12px;
}

.full-fields dt {
  color: #516a88;
  font-weight: 900;
}

.full-fields dd {
  margin: 0;
  word-break: break-word;
}

@media (max-width: 1440px) {
  .event-page {
    padding-inline: 28px;
  }

  .event-work-surface {
    grid-template-columns: minmax(0, 1fr) minmax(330px, 0.36fr);
  }

}

@media (max-width: 1120px) {
  .event-command-head,
  .event-list-head {
    flex-direction: column;
    align-items: stretch;
  }

  .event-work-surface {
    grid-template-columns: 1fr;
  }

}

@media (max-width: 760px) {
  .event-page {
    padding: 18px 14px 34px;
  }

  .event-command-card {
    padding: 18px;
    border-radius: 22px;
  }

  .event-command-actions,
  .event-list-tools {
    justify-content: stretch;
  }

  .event-command-actions > *,
  .event-list-tools > * {
    width: 100%;
  }

  .detail-grid {
    grid-template-columns: 1fr;
  }
}
</style>
