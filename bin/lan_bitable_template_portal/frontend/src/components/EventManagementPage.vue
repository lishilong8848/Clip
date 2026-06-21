<template>
  <section class="event-page">
    <div class="event-toolbar">
      <div>
        <span class="section-kicker">事件管理</span>
        <h2>{{ scopeLabel }} · 月度事件视图</h2>
        <p>{{ statusLine }}</p>
      </div>
      <div class="event-toolbar__actions">
        <label>
          <span>月份</span>
          <input v-model="selectedMonth" type="month" :disabled="loading || refreshing" />
        </label>
        <button class="btn quiet" type="button" :disabled="loading" title="只重新读取当前页面数据" @click="loadEvents()">
          {{ loading ? "刷新中" : "刷新本页" }}
        </button>
        <button class="btn secondary source-refresh" type="button" :disabled="refreshing" title="读取最新事件数据，失败不会清空当前页面" @click="refreshEvents()">
          {{ refreshing ? "刷新中" : "刷新事件" }}
        </button>
        <small v-if="eventActionHint" class="event-action-hint">{{ eventActionHint }}</small>
      </div>
    </div>

    <div class="event-scope-summary" aria-label="当前事件查看范围">
      <span v-for="item in visibleFilterSummaryItems" :key="item.label" :class="{ active: item.active }">
        <small>{{ item.label }}</small>
        <strong>{{ item.value }}</strong>
      </span>
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

    <div class="event-stats">
      <article>
        <span class="stat-dot blue"></span>
        <small>本月事件总数</small>
        <strong>{{ stats.total || 0 }}</strong>
      </article>
      <article>
        <span class="stat-dot amber"></span>
        <small>处理中</small>
        <strong>{{ stats.processing || 0 }}</strong>
      </article>
      <article>
        <span class="stat-dot emerald"></span>
        <small>已结束</small>
        <strong>{{ stats.ended || 0 }}</strong>
      </article>
      <article>
        <span class="stat-dot rose"></span>
        <small>高等级事件</small>
        <strong>{{ stats.high_level || 0 }}</strong>
      </article>
    </div>

    <div class="event-filters" :class="{ open: showAdvancedFilters }">
      <div class="event-filter-main">
        <input v-model="searchText" class="search" placeholder="搜索标题、告警描述、专业" />
        <button class="btn secondary filter-toggle" type="button" @click="showAdvancedFilters = !showAdvancedFilters">
          筛选条件
          <span v-if="activeFilterCount">{{ activeFilterCount }}</span>
        </button>
        <button v-if="activeFilterCount" class="btn quiet" type="button" @click="clearFilters">
          清空
        </button>
      </div>
      <div v-show="showAdvancedFilters" class="event-filter-advanced">
        <label>
          <span>状态</span>
          <select v-model="statusFilter">
            <option value="">全部状态</option>
            <option v-for="item in statusOptions" :key="item" :value="item">{{ item }}</option>
          </select>
        </label>
        <label>
          <span>等级</span>
          <select v-model="levelFilter">
            <option value="">全部等级</option>
            <option v-for="item in levelOptions" :key="item" :value="item">{{ item }}</option>
          </select>
        </label>
        <label>
          <span>来源</span>
          <select v-model="sourceFilter">
            <option value="">全部来源</option>
            <option v-for="item in sourceOptions" :key="item" :value="item">{{ item }}</option>
          </select>
        </label>
        <label>
          <span>专业</span>
          <select v-model="specialtyFilter">
            <option value="">全部专业</option>
            <option v-for="item in specialtyOptions" :key="item" :value="item">{{ item }}</option>
          </select>
        </label>
      </div>
    </div>

    <div class="event-list-panel">
      <div class="event-list-head">
        <div>
          <strong>事件列表</strong>
          <span>{{ filteredEvents.length }} / {{ events.length }} 条</span>
        </div>
        <small>{{ lastRefreshText }}</small>
      </div>

      <div v-if="loading" class="event-empty">
        <strong>正在读取事件数据</strong>
        <p>正在读取当前页面数据，不会影响筛选和查看。</p>
      </div>
      <div v-else-if="!events.length" class="event-empty">
        <strong>本月暂无事件</strong>
        <p>可切换月份，或点击刷新事件读取最新数据。</p>
      </div>
      <div v-else-if="!filteredEvents.length" class="event-empty">
        <strong>没有符合筛选条件的事件</strong>
        <p>可清空筛选条件，或调整关键词、状态、等级和专业。</p>
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
          <button class="drawer-close" type="button" @click="selectedEvent = null">关闭</button>
        </header>
        <div class="event-detail-summary">
          <span class="event-level-chip" :class="levelTone(selectedEvent.level)">{{ selectedEvent.level || "未填写等级" }}</span>
          <span class="status-pill" :class="statusTone(selectedEvent.status)">{{ selectedEvent.status || "未知" }}</span>
          <small>{{ selectedEvent.occurrence_time || "未填写发生时间" }}</small>
        </div>
        <div class="event-detail-tabs" aria-label="事件详情分区">
          <button
            type="button"
            :class="{ active: selectedEventDetailTab === 'summary' }"
            @click="selectedEventDetailTab = 'summary'"
          >
            概要
          </button>
          <button
            type="button"
            :class="{ active: selectedEventDetailTab === 'timeline' }"
            @click="selectedEventDetailTab = 'timeline'"
          >
            时间线
          </button>
          <button
            v-if="isAdmin"
            type="button"
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
import type { LooseDict, ScopeOption } from "../types";
import EventVirtualList from "./EventVirtualList.vue";
import MessageBanner from "./MessageBanner.vue";

const props = defineProps<{
  scope: string;
  scopeOptions: ScopeOption[];
  refreshNonce?: number;
  isAdmin?: boolean;
}>();

const emit = defineEmits<{
  refreshing: [value: boolean];
  status: [value: string];
}>();

const selectedMonth = ref(new Date().toISOString().slice(0, 7));
const loading = ref(false);
const refreshing = ref(false);
const errorText = ref("");
const events = ref<LooseDict[]>([]);
const stats = ref<LooseDict>({});
const lastRefreshedAt = ref(0);
const lastFailed = ref<LooseDict>({});
const configMissing = ref(false);
const configError = ref("");
const searchText = ref("");
const statusFilter = ref("");
const levelFilter = ref("");
const sourceFilter = ref("");
const specialtyFilter = ref("");
const selectedEvent = ref<LooseDict | null>(null);
const showAdvancedFilters = ref(false);
const selectedEventDetailTab = ref<"summary" | "timeline" | "fields">("summary");

const scopeLabel = computed(() => {
  const normalized = normalizeScope(props.scope);
  return props.scopeOptions.find((item) => normalizeScope(item.value) === normalized)?.label || scopeText(normalized);
});

const statusLine = computed(() => {
  if (refreshing.value) return "正在读取最新事件数据，失败不会清空当前页面。";
  if (loading.value) return "正在读取当前页面数据。";
  if (!events.value.length) return "当前月份没有可展示事件。";
  return `显示 ${selectedMonth.value} 的事件数据。`;
});
const eventActionHint = computed(() => {
  if (refreshing.value) return "正在刷新事件；失败时仍保留上次成功数据。";
  if (loading.value) return "正在刷新本页；请稍候。";
  if (configMissing.value) return "事件表未配置，刷新前需先在 Qt 设置中补齐。";
  if (lastFailedError.value) return "最近刷新失败，当前显示上次成功数据。";
  return "";
});

const lastFailedError = computed(() => String(lastFailed.value?.error || "").trim());
const lastRefreshText = computed(() => {
  if (!lastRefreshedAt.value) return "尚无成功更新";
  return `最近成功更新：${formatEpoch(lastRefreshedAt.value)}`;
});

const filteredEvents = computed(() => {
  const query = searchText.value.trim().toLowerCase();
  return events.value.filter((item) => {
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

const statusOptions = computed(() => uniqueOptions(events.value.map((item) => item.status)));
const levelOptions = computed(() => uniqueOptions(events.value.map((item) => item.level)));
const sourceOptions = computed(() => uniqueOptions(events.value.map((item) => item.source)));
const specialtyOptions = computed(() => uniqueOptions(events.value.map((item) => item.specialty)));
const activeFilterCount = computed(() => {
  return [statusFilter.value, levelFilter.value, sourceFilter.value, specialtyFilter.value]
    .filter((value) => String(value || "").trim()).length;
});
const filterSummaryItems = computed(() => [
  { label: "楼栋", value: scopeLabel.value, active: true },
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
    { key: "transfer", label: "是否转检修", value: item.transfer_to_overhaul },
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

function isTechnicalDisplayField(key: string): boolean {
  const text = String(key || "").trim();
  if (!text) return true;
  return /(record_id|active_item_id|source_record_id|target_record_id|raw_record_id|feishu_record_id|app_token|table_id|file_token|open_id|openid|session_id|snapshot_id|payload_json)$/i.test(text);
}

function normalizeScope(value: string): string {
  const text = String(value || "").trim().toUpperCase();
  if (text === "CAMPUS" || text === "ALL" || text === "110") return text;
  const match = text.match(/[ABCDEH]/);
  return match ? match[0] : text;
}

function scopeText(value: string): string {
  if (value === "110") return "110站";
  if (value === "CAMPUS") return "园区";
  if (value === "ALL") return "全部";
  return value ? `${value}楼` : "未选择楼栋";
}

function uniqueOptions(values: unknown[]): string[] {
  return Array.from(new Set(values.map((value) => String(value || "").trim()).filter(Boolean))).sort();
}

function formatEpoch(value: unknown): string {
  const seconds = Number(value || 0);
  if (!seconds) return "";
  const date = new Date(seconds * 1000);
  if (Number.isNaN(date.getTime())) return "";
  return date.toLocaleString("zh-CN", { hour12: false });
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

function clearFilters(): void {
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
    const params = new URLSearchParams({ scope: props.scope, month: selectedMonth.value });
    const payload = await requestJson(`/api/events/monthly?${params.toString()}`);
    events.value = Array.isArray(payload.records) ? payload.records : [];
    stats.value = payload.stats && typeof payload.stats === "object" ? payload.stats : {};
    lastRefreshedAt.value = Number(payload.last_refreshed_at || 0);
    lastFailed.value = payload.last_failed && typeof payload.last_failed === "object" ? payload.last_failed : {};
    configMissing.value = Boolean(payload.config_missing);
    configError.value = String(payload.config_error || "");
    emit("status", events.value.length ? "事件数据已就绪" : "本月暂无事件");
  } catch (error: unknown) {
    errorText.value = error instanceof Error ? error.message : "事件数据读取失败。";
    emit("status", errorText.value);
  } finally {
    loading.value = false;
  }
}

async function refreshEvents(): Promise<void> {
  if (!props.scope) return;
  refreshing.value = true;
  emit("refreshing", true);
  emit("status", "正在读取最新事件数据，失败不会清空当前页面。");
  errorText.value = "";
  try {
    const params = new URLSearchParams({ scope: props.scope, month: selectedMonth.value });
    const payload = await requestJson(`/api/events/refresh?${params.toString()}`, { method: "POST" });
    events.value = Array.isArray(payload.records) ? payload.records : events.value;
    stats.value = payload.stats && typeof payload.stats === "object" ? payload.stats : stats.value;
    lastRefreshedAt.value = Number(payload.last_refreshed_at || payload.updated_at || 0);
    lastFailed.value = payload.last_failed && typeof payload.last_failed === "object" ? payload.last_failed : {};
    configMissing.value = Boolean(payload.config_missing);
    configError.value = String(payload.config_error || "");
    emit("status", "事件已刷新，页面已更新。");
  } catch (error: unknown) {
    errorText.value = error instanceof Error ? error.message : "事件刷新失败。";
    emit("status", `事件刷新失败，仍显示上次成功数据：${errorText.value}`);
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
  padding: 28px 34px 40px;
  display: grid;
  gap: 18px;
}

.event-toolbar,
.event-list-panel,
.event-filters,
.event-stats article {
  border: 1px solid #d8e5f7;
  border-radius: 22px;
  background: rgba(255, 255, 255, 0.92);
  box-shadow: 0 18px 42px rgba(15, 73, 153, 0.12);
}

.event-toolbar {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 18px;
  padding: 18px 22px;
}

.event-toolbar h2 {
  margin: 6px 0 6px;
  color: #071a39;
  font-size: 24px;
  font-weight: 950;
}

.event-toolbar p,
.event-list-head span,
.event-empty,
.timeline small,
.full-fields dd {
  color: #5e728f;
}

.section-kicker {
  color: #0e5bd8;
  font-size: 12px;
  font-weight: 950;
}

.event-filter-main,
.event-filter-advanced {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
}

.event-scope-summary {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  padding: 10px 12px;
  border: 1px solid rgba(216, 229, 247, 0.9);
  border-radius: 18px;
  background: rgba(255, 255, 255, 0.78);
  box-shadow: 0 10px 24px rgba(15, 73, 153, 0.07);
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

.event-toolbar__actions {
  display: grid;
  grid-template-columns: auto auto auto;
  justify-content: end;
  align-items: end;
  gap: 8px;
  min-width: min(100%, 520px);
}

.event-toolbar__actions label {
  display: grid;
  gap: 5px;
  min-width: 154px;
  color: #516a88;
  font-size: 12px;
  font-weight: 900;
}

.event-toolbar input,
.event-filters input,
.event-filters select {
  min-height: 42px;
  border: 1px solid #d8e5f7;
  border-radius: 16px;
  background: #fff;
  color: #0b1f3f;
  font: inherit;
  font-weight: 800;
  padding: 0 14px;
}

.event-filters {
  display: grid;
  gap: 10px;
  padding: 12px 14px;
}

.event-filter-main .search {
  flex: 1 1 320px;
}

.event-filter-advanced {
  padding-top: 10px;
  border-top: 1px solid rgba(216, 229, 247, 0.86);
}

.event-filter-advanced label {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  min-height: 42px;
  padding: 4px 6px 4px 12px;
  border: 1px solid rgba(216, 229, 247, 0.9);
  border-radius: 16px;
  background: rgba(248, 251, 255, 0.78);
}

.event-filter-advanced label span {
  color: #475569;
  font-size: 12px;
  font-weight: 900;
  white-space: nowrap;
}

.filter-toggle {
  gap: 8px;
}

.filter-toggle span {
  min-width: 20px;
  height: 20px;
  display: inline-grid;
  place-items: center;
  border-radius: 999px;
  background: #1e63ff;
  color: #fff;
  font-size: 11px;
}

.btn.quiet {
  min-height: 38px;
  border-color: #cfe0ff;
  background: #f8fbff;
  color: #3156c9;
}

.btn.source-refresh {
  min-height: 38px;
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

.btn.primary {
  border-color: #1e63ff;
  background: #1e63ff;
  color: #fff;
}

.btn.secondary {
  background: #f7fbff;
  color: #0e4fb2;
}

.btn:disabled {
  cursor: not-allowed;
  opacity: 0.65;
}

.event-toolbar__actions .btn,
.event-toolbar__actions input {
  min-height: 38px;
  border-radius: 999px;
  font-size: 13px;
}

.event-toolbar__actions .btn {
  padding: 0 13px;
}

.event-action-hint {
  grid-column: 1 / -1;
  justify-self: end;
  max-width: min(100%, 520px);
  overflow: hidden;
  border: 1px solid #cfe0ff;
  border-radius: 999px;
  background: rgba(239, 246, 255, 0.92);
  padding: 6px 10px;
  color: #3156c9;
  font-size: 12px;
  font-weight: 900;
  line-height: 1.35;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.event-stats {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 12px;
}

.event-stats article {
  display: grid;
  grid-template-columns: 44px minmax(0, 1fr);
  align-items: center;
  column-gap: 12px;
  padding: 14px 18px;
}

.event-stats small {
  color: #5b708c;
  font-weight: 850;
}

.event-stats strong {
  color: #075bd8;
  font-size: 26px;
  font-weight: 950;
}

.stat-dot {
  grid-row: span 2;
  width: 44px;
  height: 44px;
  border-radius: 16px;
  box-shadow: 0 14px 24px rgba(21, 92, 214, 0.18);
}

.stat-dot.blue { background: linear-gradient(135deg, #2a77ff, #0050d9); }
.stat-dot.amber { background: linear-gradient(135deg, #f59e0b, #f97316); }
.stat-dot.emerald { background: linear-gradient(135deg, #22c55e, #059669); }
.stat-dot.rose { background: linear-gradient(135deg, #fb7185, #e11d48); }

.event-list-panel {
  padding: 18px;
  display: grid;
  gap: 12px;
}

.event-list-head {
  display: flex;
  justify-content: space-between;
  gap: 12px;
  align-items: center;
}

.event-list-head strong {
  color: #071a39;
  font-size: 17px;
  font-weight: 950;
}

.event-list-head small {
  color: #6b7f9d;
  font-size: 12px;
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

@media (max-width: 1024px) {
  .event-toolbar,
  .event-list-head {
    flex-direction: column;
    align-items: stretch;
  }

  .event-toolbar__actions {
    grid-template-columns: 1fr;
    justify-content: stretch;
  }

  .event-stats {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}
</style>
