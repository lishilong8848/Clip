<template>
  <section class="event-page">
    <div class="event-command-card">
      <div class="event-command-head">
        <div class="event-title-block">
          <span class="section-kicker">事件管理</span>
          <h2>事件态势与楼栋入口</h2>
          <p>先查看全局事件态势，再按楼栋进入详情处理</p>
        </div>
        <div class="event-command-actions">
          <span class="update-pill">{{ lastRefreshCompactText }}</span>
          <label class="month-picker">
            <span>月份</span>
            <input v-model="selectedMonth" type="month" :disabled="loading || refreshing" />
          </label>
          <button class="btn quiet" type="button" :disabled="loading" title="只重新读取当前页面数据" @click="loadEvents()">
            {{ loading ? "刷新中" : "刷新本页" }}
          </button>
          <button class="btn secondary source-refresh" type="button" :disabled="refreshing" title="读取最新事件数据，失败不会清空当前页面" @click="refreshEvents()">
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
      <div v-if="eventActionHint" class="event-inline-hint">{{ eventActionHint }}</div>

      <div class="event-stats">
        <article v-for="card in statCards" :key="card.key" :class="card.tone">
          <span class="stat-icon">{{ card.icon }}</span>
          <div class="stat-main">
            <small>{{ card.label }}</small>
            <strong>{{ card.value }}</strong>
            <em>{{ card.unit }}</em>
          </div>
          <span class="stat-badge">{{ card.badge }}</span>
          <span class="stat-bars" aria-hidden="true">
            <i v-for="index in 6" :key="index"></i>
          </span>
        </article>
      </div>

      <div class="event-work-surface" :class="{ detail: showingDetails }">
        <section v-if="!showingDetails" class="building-panel">
          <div class="surface-head">
            <div>
              <h3>楼栋事件概览</h3>
              <p>点击有权限楼栋进入事件明细；无权限楼栋仅展示态势数据</p>
            </div>
          </div>

          <div v-if="loading" class="event-empty compact">
            <strong>正在读取事件数据</strong>
            <p>页面会保留上次筛选状态。</p>
          </div>
          <div v-else class="building-grid">
            <button
              v-for="card in buildingCards"
              :key="card.code"
              class="building-card"
              :class="[card.tone, { active: activeBuildingCode === card.code, disabled: !card.allowed }]"
              type="button"
              :disabled="!card.allowed"
              :title="card.allowed ? '进入该楼栋事件明细' : '当前账号无该楼栋权限，仅展示态势数据'"
              @click="openBuilding(card.code)"
            >
              <span class="building-card__bar"></span>
              <div class="building-card__head">
                <span class="building-icon">▦</span>
                <strong>{{ card.label }}</strong>
                <em>{{ card.statusLabel }}</em>
              </div>
              <div class="building-card__numbers">
                <span><b>{{ card.total }}</b><small>本月</small></span>
                <span><b>{{ card.processing }}</b><small>处理中</small></span>
                <span><b>{{ card.pending }}</b><small>挂起</small></span>
                <span><b>{{ card.ended }}</b><small>已闭环</small></span>
              </div>
              <div class="building-card__action">{{ card.allowed ? "进入管理 ›" : "仅可查看态势" }}</div>
            </button>
          </div>
        </section>

        <aside v-if="showingDetails" class="priority-panel">
          <div class="surface-head tight">
            <div>
              <h3>{{ detailScopeLabel }}重点与挂起事件</h3>
              <p>优先处理影响范围较大的事项</p>
            </div>
            <button class="btn quiet" type="button" @click="returnToOverview">
              返回态势
            </button>
          </div>
          <div class="priority-tabs">
            <span>重点 {{ highPriorityEvents.length }}</span>
            <span>挂起 {{ pendingEventsCount }}</span>
            <span>全部事件 {{ events.length }}</span>
          </div>
          <div v-if="priorityEvents.length" class="priority-list">
            <button
              v-for="item in priorityEvents"
              :key="eventKey(item)"
              type="button"
              class="priority-row"
              @click="selectedEvent = item"
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
            <p>当前筛选范围内没有需要优先处理的事件。</p>
          </div>
          <div class="priority-note">
            {{ priorityNote }}
          </div>
        </aside>
        <aside v-else class="priority-panel overview-panel">
          <div class="surface-head tight">
            <div>
              <h3>态势查看方式</h3>
              <p>当前页面只展示全局态势，楼栋明细需点击楼栋卡片进入</p>
            </div>
          </div>
          <div class="overview-guide-grid">
            <span>
              <small>可查看楼栋</small>
              <strong>{{ allowedBuildingCodes.size }}</strong>
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
          <div class="priority-note">
            点击楼栋卡片后才会加载该楼栋“事件明细”和“月度事件列表”，避免态势页混入明细内容。
          </div>
        </aside>
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
          <button v-if="activeFilterCount" class="btn quiet" type="button" @click="clearFilters">
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
  "switch-scope": [scope: string];
}>();

type BuildingCard = {
  code: string;
  label: string;
  total: number;
  processing: number;
  pending: number;
  ended: number;
  high: number;
  tone: string;
  statusLabel: string;
  allowed: boolean;
};

const BUILDING_SCOPE_CODES = ["110", "A", "B", "C", "D", "E", "H"];
const BUILDING_ORDER = ["110", "A", "B", "C", "D", "E", "H", "CAMPUS", "ALL"];

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
const detailScope = ref("");
const detailRequested = ref(false);
const selectedEvent = ref<LooseDict | null>(null);
const selectedEventDetailTab = ref<"summary" | "timeline" | "fields">("summary");

const scopeLabel = computed(() => {
  const normalized = normalizeScope(props.scope);
  return props.scopeOptions.find((item) => normalizeScope(item.value) === normalized)?.label || scopeText(normalized);
});
const showingDetails = computed(() => Boolean(detailScope.value));
const detailScopeLabel = computed(() => detailScope.value ? scopeText(detailScope.value) : "");
const activeBuildingCode = computed(() => detailScope.value || "");

const statusLine = computed(() => {
  if (refreshing.value) return "正在读取最新事件数据，失败不会清空当前页面。";
  if (loading.value) return "正在读取当前页面数据。";
  if (!showingDetails.value) return `显示 ${selectedMonth.value} 全局事件态势。`;
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
      .map((item) => buildBuildingCardFromStats(item))
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
  const sortedCodes = Array.from(codes).sort((left, right) => {
    const leftIndex = BUILDING_ORDER.indexOf(left);
    const rightIndex = BUILDING_ORDER.indexOf(right);
    return (leftIndex < 0 ? 99 : leftIndex) - (rightIndex < 0 ? 99 : rightIndex);
  });
  return sortedCodes.map((code) => {
    const rows = events.value.filter((item) => eventMatchesBuilding(item, code));
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
    if (buildingFilter.value && !eventMatchesBuilding(item, buildingFilter.value)) return false;
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
const priorityNote = computed(() => {
  if (!showingDetails.value) return "点击楼栋卡片后查看该楼栋的重点与挂起事件。";
  if (!events.value.length) return "暂无事件数据，可点击刷新事件读取最新快照。";
  if (pendingEventsCount.value) return `挂起提醒：当前有 ${pendingEventsCount.value} 条事件需要跟进。`;
  if (highPriorityEvents.value.length) return `重点提醒：当前有 ${highPriorityEvents.value.length} 条高等级事件。`;
  return "当前范围未发现挂起或高等级事件。";
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
  return /(record_id|active_item_id|source_record_id|target_record_id|app_token|table_id|file_token|open_id|openid|session_id|snapshot_id|payload_json)$/i.test(text);
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
  if (text.includes("恢复") || text.includes("挂起")) return "recovered";
  return "processing";
}

function buildingSortIndex(code: string): number {
  const index = BUILDING_ORDER.indexOf(code);
  return index < 0 ? 99 : index;
}

function buildBuildingCardFromStats(item: LooseDict): BuildingCard {
  const code = normalizeScope(String(item.code || item.scope || ""));
  const total = Number(item.total || 0);
  const processing = Number(item.processing || 0);
  const pending = Number(item.pending || 0);
  const ended = Number(item.ended || 0);
  const high = Number(item.high_level || item.high || 0);
  const tone = high ? "critical" : pending ? "warning" : processing ? "active" : "stable";
  const statusLabel = high ? `重点 ${high}` : pending ? `挂起 ${pending}` : processing ? "待处理" : "运行平稳";
  return {
    code,
    label: String(item.label || scopeText(code)),
    total,
    processing,
    pending,
    ended,
    high,
    tone,
    statusLabel,
    allowed: allowedBuildingCodes.value.has(code),
  };
}

function levelTone(level: unknown): string {
  const text = String(level || "").toUpperCase();
  if (/(I1|一级|紧急|严重|高)/.test(text)) return "critical";
  if (/(I2|I3|二级|三级|中)/.test(text)) return "warning";
  return "normal";
}

function isHighLevel(item: LooseDict): boolean {
  return Boolean(item.high_level) || levelTone(item.level) === "critical";
}

function priorityScore(item: LooseDict): number {
  let score = 0;
  if (isHighLevel(item)) score += 100;
  if (statusTone(item.status) === "recovered") score += 60;
  if (statusTone(item.status) === "processing") score += 20;
  return score;
}

function buildingCodesForItem(item: LooseDict): string[] {
  const raw = item.building_codes;
  const codes = Array.isArray(raw)
    ? raw.map((value) => normalizeScope(String(value || ""))).filter(Boolean)
    : [];
  if (codes.length) return Array.from(new Set(codes));
  const fallback = normalizeScope(String(item.building || ""));
  return fallback ? [fallback] : [];
}

function eventMatchesBuilding(item: LooseDict, code: string): boolean {
  const normalized = normalizeScope(code);
  if (!normalized) return true;
  const codes = buildingCodesForItem(item);
  return codes.includes(normalized) || (!codes.length && normalized === normalizeScope(props.scope));
}

function eventKey(item: LooseDict | undefined): string {
  if (!item) return "";
  return String(item.source_record_id || item.record_id || `${item.title}-${item.occurrence_time}`);
}

function openBuilding(code: string): void {
  const nextScope = normalizeScope(code);
  if (!allowedBuildingCodes.value.has(nextScope)) return;
  detailRequested.value = true;
  detailScope.value = nextScope;
  clearFilters();
  selectedEvent.value = null;
  if (nextScope && nextScope !== normalizeScope(props.scope)) {
    emit("switch-scope", nextScope);
    return;
  }
  void loadEvents();
}

function returnToOverview(): void {
  detailRequested.value = false;
  detailScope.value = "";
  selectedEvent.value = null;
  clearFilters();
  events.value = [];
  stats.value = {};
  void loadEvents();
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
  emit("status", "正在读取最新事件数据，失败不会清空当前页面。");
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
.surface-head,
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
.surface-head p,
.event-empty,
.priority-note,
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
.compact-filters input,
.compact-filters select,
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

.btn:disabled {
  cursor: not-allowed;
  opacity: 0.65;
}

.event-stats {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 28px;
}

.event-stats article {
  position: relative;
  min-height: 112px;
  overflow: hidden;
  display: grid;
  grid-template-columns: 58px minmax(0, 1fr) auto;
  align-items: center;
  gap: 14px;
  padding: 20px 22px;
  border: 1px solid #d8e5f7;
  border-radius: 22px;
  background: rgba(255, 255, 255, 0.95);
  box-shadow: 0 18px 42px rgba(15, 73, 153, 0.11);
}

.stat-icon {
  grid-row: span 2;
  width: 58px;
  height: 58px;
  display: grid;
  place-items: center;
  border-radius: 18px;
  background: linear-gradient(135deg, #2a77ff, #0050d9);
  color: #fff;
  font-size: 27px;
  font-weight: 950;
  box-shadow: 0 16px 24px rgba(21, 92, 214, 0.2);
}

.event-stats article.amber .stat-icon { background: linear-gradient(135deg, #f59e0b, #fb923c); }
.event-stats article.rose .stat-icon { background: linear-gradient(135deg, #fb7185, #e11d48); }
.event-stats article.emerald .stat-icon { background: linear-gradient(135deg, #22c55e, #059669); }

.stat-main {
  min-width: 0;
  display: flex;
  align-items: baseline;
  gap: 8px;
  flex-wrap: wrap;
}

.stat-main small {
  width: 100%;
  color: #43536a;
  font-size: 14px;
  font-weight: 950;
}

.stat-main strong {
  color: #06152f;
  font-size: 32px;
  font-weight: 950;
  line-height: 1;
}

.stat-main em {
  color: #6b7f9d;
  font-style: normal;
  font-weight: 850;
}

.stat-badge {
  align-self: start;
  max-width: 120px;
  overflow: hidden;
  padding: 8px 12px;
  border-radius: 999px;
  background: #eff6ff;
  color: #1d4ed8;
  font-size: 12px;
  font-weight: 950;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.event-stats article.amber .stat-badge { background: #fff7ed; color: #c2410c; }
.event-stats article.rose .stat-badge { background: #fff1f2; color: #be123c; }
.event-stats article.emerald .stat-badge { background: #ecfdf5; color: #047857; }

.stat-bars {
  position: absolute;
  right: 22px;
  bottom: 15px;
  display: flex;
  align-items: end;
  gap: 6px;
}

.stat-bars i {
  width: 8px;
  border-radius: 999px;
  background: #1e63ff;
}

.stat-bars i:nth-child(1) { height: 7px; opacity: 0.55; }
.stat-bars i:nth-child(2) { height: 13px; opacity: 0.65; }
.stat-bars i:nth-child(3) { height: 18px; opacity: 0.75; }
.stat-bars i:nth-child(4) { height: 23px; opacity: 0.84; }
.stat-bars i:nth-child(5) { height: 29px; opacity: 0.92; }
.stat-bars i:nth-child(6) { height: 35px; }

.event-stats article.amber .stat-bars i { background: #f59e0b; }
.event-stats article.rose .stat-bars i { background: #e11d48; }
.event-stats article.emerald .stat-bars i { background: #059669; }

.event-work-surface {
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(360px, 0.36fr);
  gap: 22px;
}

.event-work-surface.detail {
  grid-template-columns: 1fr;
}

.building-panel,
.priority-panel {
  min-width: 0;
  border: 1px solid rgba(216, 229, 247, 0.92);
  border-radius: 24px;
  background: rgba(255, 255, 255, 0.74);
  padding: 18px;
}

.surface-head h3 {
  margin: 0 0 4px;
  color: #071a39;
  font-size: 20px;
  font-weight: 950;
}

.surface-head p {
  margin: 0;
  font-size: 13px;
  font-weight: 760;
}

.surface-head.tight {
  align-items: center;
}

.surface-head.tight .btn {
  flex: 0 0 auto;
}

.compact-filters {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: 10px;
  flex-wrap: wrap;
}

.compact-filters input {
  width: 210px;
}

.building-grid {
  margin-top: 18px;
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 16px;
}

.building-card {
  position: relative;
  min-width: 0;
  overflow: hidden;
  display: grid;
  gap: 14px;
  padding: 18px 18px 14px;
  border: 1px solid #d8e5f7;
  border-radius: 20px;
  background: rgba(255, 255, 255, 0.96);
  text-align: left;
  cursor: pointer;
  box-shadow: 0 12px 26px rgba(15, 73, 153, 0.08);
  transition: transform 0.16s ease, border-color 0.16s ease, box-shadow 0.16s ease;
}

.building-card:hover,
.building-card.active {
  transform: translateY(-2px);
  border-color: #9cc7ff;
  box-shadow: 0 18px 36px rgba(15, 86, 228, 0.12);
}

.building-card.disabled {
  cursor: not-allowed;
  opacity: 0.72;
  transform: none;
}

.building-card.disabled:hover {
  border-color: #d8e5f7;
  box-shadow: 0 12px 26px rgba(15, 73, 153, 0.08);
}

.building-card.disabled .building-card__action {
  background: #f1f5f9;
  color: #64748b;
}

.building-card__bar {
  position: absolute;
  inset: 0 0 auto;
  height: 5px;
  background: #1e63ff;
}

.building-card.warning .building-card__bar { background: #f59e0b; }
.building-card.critical .building-card__bar { background: #e11d48; }
.building-card.stable .building-card__bar { background: #10b981; }

.building-card__head {
  min-width: 0;
  display: grid;
  grid-template-columns: 38px minmax(0, 1fr) auto;
  align-items: center;
  gap: 12px;
}

.building-icon {
  width: 38px;
  height: 38px;
  display: grid;
  place-items: center;
  border-radius: 14px;
  background: #edf5ff;
  color: #1d4ed8;
  font-size: 18px;
  font-weight: 950;
}

.building-card__head strong {
  min-width: 0;
  overflow: hidden;
  color: #071a39;
  font-size: 20px;
  font-weight: 950;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.building-card__head em {
  padding: 7px 12px;
  border-radius: 999px;
  background: #eff6ff;
  color: #1d4ed8;
  font-size: 12px;
  font-style: normal;
  font-weight: 950;
  white-space: nowrap;
}

.building-card.warning .building-card__head em { background: #fff7ed; color: #c2410c; }
.building-card.critical .building-card__head em { background: #fff1f2; color: #be123c; }
.building-card.stable .building-card__head em { background: #ecfdf5; color: #047857; }

.building-card__numbers {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 8px;
}

.building-card__numbers span {
  min-width: 0;
  display: grid;
  gap: 2px;
  padding-right: 8px;
  border-right: 1px solid #e5edf8;
}

.building-card__numbers span:last-child {
  border-right: 0;
}

.building-card__numbers b {
  color: #1d4ed8;
  font-size: 17px;
  font-weight: 950;
}

.building-card__numbers small {
  color: #6b7f9d;
  font-size: 11px;
  font-weight: 850;
}

.building-card__action {
  justify-self: end;
  min-height: 30px;
  display: inline-flex;
  align-items: center;
  padding: 0 13px;
  border-radius: 999px;
  background: #edf5ff;
  color: #0e5bd8;
  font-size: 12px;
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

.priority-note {
  margin-top: 14px;
  padding: 9px 12px;
  border-radius: 14px;
  background: #f1f6ff;
  font-size: 12px;
  font-weight: 850;
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

  .event-stats {
    gap: 14px;
  }

  .event-work-surface {
    grid-template-columns: minmax(0, 1fr) minmax(330px, 0.36fr);
  }

  .building-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}

@media (max-width: 1120px) {
  .event-command-head,
  .surface-head,
  .event-list-head {
    flex-direction: column;
    align-items: stretch;
  }

  .event-work-surface {
    grid-template-columns: 1fr;
  }

  .event-stats {
    grid-template-columns: repeat(2, minmax(0, 1fr));
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
  .compact-filters,
  .event-list-tools {
    justify-content: stretch;
  }

  .event-command-actions > *,
  .compact-filters > *,
  .event-list-tools > * {
    width: 100%;
  }

  .event-stats,
  .building-grid,
  .detail-grid {
    grid-template-columns: 1fr;
  }
}
</style>
