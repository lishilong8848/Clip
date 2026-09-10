<template>
  <section class="home-shell" :class="{ 'dashboard-mode': !activeMode }">
    <HomeDashboard
      v-if="!activeMode"
      :modules="moduleCards"
      :enabled-module-count="enabledModuleCount"
      :can-request-more-scopes="Boolean(canRequestMoreScopes)"
      :broadcast-items="homeBroadcastItems"
      :broadcast-summary="homeBroadcastSummary"
      :module-metrics="homeModuleMetrics"
      @select-action="selectModuleAction"
      @activate-broadcast="activateBroadcastItem"
      @request-permission="$emit('request-permission')"
    />

    <section v-else class="feature-section" :class="{ 'scope-selection': activeMode !== 'tools' }">
      <div class="page-back-row">
        <VnetBackButton @click="returnFromFeature" />
      </div>
      <header class="feature-section__head" :class="{ 'scope-section-head': activeMode !== 'tools' }">
        <div class="feature-title-block">
          <span class="section-kicker">{{ activeConfig.kicker }}</span>
          <h2>{{ activeConfig.title }}</h2>
        </div>
        <div
          v-if="activeMode !== 'tools'"
          class="scope-summary-strip"
          :class="{ 'repair-metrics': activeMode === 'repair_management' }"
          aria-label="当前模块楼栋统计"
          :aria-busy="activeMode === 'repair_management' && repairOverviewLoading"
        >
          <template v-if="activeMode === 'repair_management'">
            <article>
              <span class="summary-icon pending" aria-hidden="true"></span>
              <small>总待检修条</small>
              <strong>{{ repairMetricValue(repairAggregate.pending) }}</strong>
            </article>
            <article>
              <span class="summary-icon ongoing" aria-hidden="true"></span>
              <small>进行中条</small>
              <strong>{{ repairMetricValue(repairAggregate.inProgress) }}</strong>
            </article>
            <article>
              <span class="summary-icon coverage" aria-hidden="true"></span>
              <small>本年度总检修</small>
              <strong>{{ repairMetricValue(repairAggregate.yearTotal) }}</strong>
            </article>
            <article>
              <span class="summary-icon monthly" aria-hidden="true"></span>
              <small>本月检修</small>
              <strong>{{ repairMetricValue(repairAggregate.monthTotal) }}</strong>
            </article>
          </template>
          <template v-else-if="activeMode === 'water'">
            <article>
              <span class="summary-icon pending" aria-hidden="true"></span>
              <small>本月记录</small>
              <strong>{{ waterMetricValue(waterAggregate.recordCount) }}</strong>
            </article>
            <article>
              <span class="summary-icon ongoing" aria-hidden="true"></span>
              <small>本月耗水量</small>
              <strong>{{ waterMetricValue(waterAggregate.totalUsage, " t") }}</strong>
            </article>
            <article>
              <span class="summary-icon coverage" aria-hidden="true"></span>
              <small>可访问楼栋</small>
              <strong>{{ waterMetricValue(displayScopeOptions.length) }}</strong>
            </article>
          </template>
          <template v-else-if="activeMode === 'daily'">
            <article>
              <span class="summary-icon pending" aria-hidden="true"></span>
              <small>日期</small>
              <strong>今天</strong>
            </article>
            <article>
              <span class="summary-icon ongoing" aria-hidden="true"></span>
              <small>任务分类</small>
              <strong>5</strong>
            </article>
            <article>
              <span class="summary-icon coverage" aria-hidden="true"></span>
              <small>可访问楼栋</small>
              <strong>{{ displayScopeOptions.length }}</strong>
            </article>
          </template>
          <template v-else>
            <article>
              <span class="summary-icon pending" aria-hidden="true"></span>
              <small>总待发起</small>
              <strong>{{ activeAggregate.pending }}</strong>
            </article>
            <article>
              <span class="summary-icon ongoing" aria-hidden="true"></span>
              <small>进行中</small>
              <strong>{{ activeAggregate.ongoing }}</strong>
            </article>
            <article>
              <span class="summary-icon coverage" aria-hidden="true"></span>
              <small>覆盖对象</small>
              <strong>{{ displayScopeOptions.length }}</strong>
            </article>
          </template>
        </div>
      </header>

      <div v-if="activeMode === 'tools'" class="tool-grid">
        <button type="button"
          v-for="tool in toolEntries"
          :key="tool.key"
          class="tool-card"
          :class="tool.tone"
          :aria-label="`选择${tool.title}`"
          @click="selectEntry(tool.key)"
        >
          <span class="tool-icon" :class="tool.icon" aria-hidden="true"></span>
          <span>
            <strong>{{ tool.title }}</strong>
          </span>
          <b>{{ tool.badge }}</b>
        </button>
      </div>

      <div
        v-if="activeMode === 'water' && waterBuildingsError && !displayScopeOptions.length"
        class="scope-loading-state error"
        role="alert"
      >
        <span>{{ waterBuildingsError }}</span>
        <button type="button" @click="loadWaterBuildings">重新读取</button>
      </div>

      <div
        v-else-if="activeMode === 'water' && waterBuildingsLoading && !displayScopeOptions.length"
        class="scope-loading-state"
        role="status"
      >
        正在读取水耗楼栋数据
      </div>

      <div v-else-if="activeMode !== 'tools'" class="scope-grid scope-overview-grid">
        <article
          v-for="scope in displayScopeOptions"
          :key="scope.value"
          class="scope-card"
          :class="[scopeCardClass(scope.value), { interactive: scopeCardIsEnabled(scope.value) }]"
          :role="scopeCardIsEnabled(scope.value) ? 'button' : undefined"
          :tabindex="scopeCardIsEnabled(scope.value) ? 0 : -1"
          :aria-label="scopeCardIsEnabled(scope.value) ? `${activeConfig.actionLabel}：${scopeDisplayLabel(scope)}` : undefined"
          :aria-disabled="scopeCardIsEnabled(scope.value) ? undefined : 'true'"
          @click="activateScopeCard($event, scope.value)"
          @keydown.enter="activateScopeCard($event, scope.value)"
          @keydown.space="activateScopeCard($event, scope.value)"
        >
          <div class="scope-card__main">
            <span class="scope-building-icon" :class="scopeIconClass(scope.value)" aria-hidden="true"></span>
            <strong>{{ scopeDisplayLabel(scope) }}</strong>
          </div>
          <div class="scope-badges">
            <template v-if="activeMode === 'water'">
              <span>本月记录 {{ waterScopeItem(scope.value).month_record_count || 0 }}</span>
              <span>最近 {{ formatWaterDate(waterScopeItem(scope.value).latest_date_ms) }}</span>
            </template>
            <template v-else-if="activeMode === 'daily'">
              <span>今日任务清单</span>
              <span>通告 · 事件 · 检修</span>
            </template>
            <template v-else>
              <span>{{ scopePrimaryMetricLabel(scope.value) }} {{ scopeCounts(scope.value).pending }}</span>
              <span>{{ scopeSecondaryMetricLabel(scope.value) }} {{ scopeCounts(scope.value).ongoing }}</span>
            </template>
          </div>
          <div class="scope-actions">
            <button type="button"
              v-if="activeMode === 'event'"
              class="primary"
              @click="$emit('event', scope.value)"
            >
              进入事件管理
            </button>
            <button type="button"
              v-else-if="activeMode === 'repair_management'"
              class="primary"
              @click="$emit('repair-management', scope.value)"
            >
              进入检修单管理
            </button>
            <button type="button"
              v-else-if="activeMode === 'maintenance_mop'"
              class="primary"
              @click="$emit('engineer', scope.value)"
            >
              进入维护单管理
            </button>
            <button type="button"
              v-else-if="activeMode === 'water'"
              class="primary"
              @click="$emit('water', scope.value)"
            >
              进入水耗管理
            </button>
            <button type="button"
              v-else-if="activeMode === 'daily'"
              class="primary"
              @click="$emit('daily', scope.value)"
            >
              查看每日任务
            </button>
            <a
              v-else-if="activeMode === 'handover' && handoverLinks[scope.value]"
              class="primary"
              :href="handoverLinks[scope.value]"
              target="_blank"
              rel="noopener noreferrer"
            >
              打开审核页
            </a>
            <button type="button"
              v-else-if="activeMode === 'handover'"
              class="secondary"
              disabled
              title="未配置"
            >
              未配置
            </button>
            <button type="button"
              v-else
              class="primary"
              :disabled="Boolean(openingWorkbenchKey)"
              :aria-busy="isOpeningWorkbench(scope.value) ? 'true' : 'false'"
              @pointerenter="prefetchNoticeWorkbench(scope.value)"
              @focus="prefetchNoticeWorkbench(scope.value)"
              @click="enterNoticeWorkbench(scope.value)"
            >
              {{ isOpeningWorkbench(scope.value) ? "正在进入" : activeConfig.actionLabel }}
            </button>
          </div>
          <span class="scope-building-art" aria-hidden="true"></span>
        </article>
      </div>
    </section>
  </section>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from "vue";
import { requestJson } from "../api/client";
import {
  SCOPE_HOME_ENTRY_CONFIGS as entryConfigs,
  SCOPE_HOME_MODULE_CARDS as moduleCards,
  SCOPE_HOME_TOOL_ENTRIES as toolEntries,
  normalizeScopeValue,
  scopeCardClass,
  scopeDisplayLabel,
  scopeIconClass,
  scopeSortIndex,
  typedScopeCounts as resolveTypedScopeCounts,
  type ScopeHomeEntryKey as EntryKey,
  type ScopeHomeBroadcastItem,
  type ScopeHomeModuleAction as ModuleAction,
  type ScopeHomeModuleMetric,
} from "../scopeHomeUtils";
import HomeDashboard from "./HomeDashboard.vue";
import VnetBackButton from "./VnetBackButton.vue";

type Dict = Record<string, any>;
const props = defineProps<{
  scopeOptions: Array<{ value: string; label: string }>;
  overview: Record<string, Dict>;
  handoverLinks: Record<string, string>;
  canRequestMoreScopes?: boolean;
}>();

const emit = defineEmits<{
  enter: [scope: string, workType?: string];
  prefetch: [scope: string, workType?: string];
  event: [scope: string];
  engineer: [scope: string];
  "repair-management": [scope: string];
  water: [scope: string];
  "critical-guard": [];
  drill: [];
  "cabinet-power": [];
  daily: [scope: string];
  "request-permission": [];
  "dashboard-visible": [visible: boolean];
}>();

const activeMode = ref<EntryKey>("");
const openingWorkbenchKey = ref("");
const repairOverviewLoading = ref(false);
const repairOverviewLoaded = ref(false);
const repairOverview = ref<Dict>({});
const waterBuildingsLoading = ref(false);
const waterBuildings = ref<Array<{ value: string; label: string } & Dict>>([]);
const waterSnapshot = ref<Dict>({});
const waterBuildingsError = ref("");
let waterBuildingsPollTimer: number | null = null;

const enabledModuleCount = computed(() => moduleCards.filter((item) => !item.disabled).length);
const activeConfig = computed(() => entryConfigs[activeMode.value || "tools"]);
const isToolScopeMode = computed(() => ["daily", "power", "polling", "adjust", "handover"].includes(activeMode.value));
const activeMetricWorkType = computed(() => {
  if (activeMode.value === "maintenance_mop") return "maintenance";
  if (activeMode.value === "repair_management") return "repair";
  if (activeMode.value === "event") return "event";
  if (activeMode.value === "daily") return "daily";
  if (activeMode.value === "handover") return "handover";
  return activeConfig.value.workType || "maintenance";
});
const displayScopeOptions = computed(() => {
  const source = activeMode.value === "water"
    ? waterBuildings.value
    : props.scopeOptions;
  return [...source].sort((left, right) => {
    const leftIndex = scopeSortIndex(left.value);
    const rightIndex = scopeSortIndex(right.value);
    if (leftIndex !== rightIndex) return leftIndex - rightIndex;
    return String(left.label || left.value).localeCompare(String(right.label || right.value), "zh-CN");
  });
});
const activeAggregate = computed(() => {
  const allCounts = scopeCounts("ALL");
  if (allCounts.pending || allCounts.ongoing) return allCounts;
  return displayScopeOptions.value.reduce(
    (total, scope) => {
      const code = normalizeScopeValue(scope.value, "");
      if (!code || code === "ALL") return total;
      const current = scopeCounts(code);
      total.pending += current.pending;
      total.ongoing += current.ongoing;
      return total;
    },
    { pending: 0, ongoing: 0 },
  );
});
const repairAggregate = computed(() => {
  const aggregate = repairOverview.value.aggregate && typeof repairOverview.value.aggregate === "object"
    ? repairOverview.value.aggregate as Dict
    : {};
  return {
    pending: Number(aggregate.pending || 0),
    inProgress: Number(aggregate.in_progress || 0),
    yearTotal: Number(aggregate.year_total || 0),
    monthTotal: Number(aggregate.month_total || 0),
  };
});
const waterAggregate = computed(() => {
  return waterBuildings.value.reduce(
    (total, item) => {
      total.recordCount += Number(item.month_record_count || 0);
      total.totalUsage += Number(item.month_total_usage || 0);
      return total;
    },
    { recordCount: 0, totalUsage: 0 },
  );
});

const broadcastWorkTypes = [
  { key: "maintenance", label: "维保" },
  { key: "change", label: "变更" },
  { key: "repair", label: "检修" },
  { key: "power", label: "上/下电" },
  { key: "polling", label: "轮巡" },
  { key: "adjust", label: "调整" },
] as const;
const BROADCAST_ITEM_LIMIT = 24;
const broadcastWorkTypeLabelByKey: Record<string, string> = Object.fromEntries(
  broadcastWorkTypes.map((item) => [item.key, item.label]),
);

const broadcastScopes = computed(() => {
  const scopes = new Map<string, { value: string; label: string }>();
  for (const scope of displayScopeOptions.value) {
    const code = normalizeScopeValue(scope.value, "");
    if (!code || code === "ALL") continue;
    scopes.set(code, { value: code, label: scopeDisplayLabel(scope) });
  }
  if (!scopes.size) {
    for (const rawCode of Object.keys(props.overview || {})) {
      const code = normalizeScopeValue(rawCode, "");
      if (!code || code === "ALL") continue;
      scopes.set(code, { value: code, label: scopeDisplayLabel({ value: code, label: "" }) });
    }
  }
  if (!scopes.size && displayScopeOptions.value.length) {
    const scope = displayScopeOptions.value[0];
    const code = normalizeScopeValue(scope.value, "ALL");
    scopes.set(code, { value: code, label: scopeDisplayLabel(scope) });
  }
  return [...scopes.values()].sort((left, right) => {
    const leftIndex = scopeSortIndex(left.value);
    const rightIndex = scopeSortIndex(right.value);
    if (leftIndex !== rightIndex) return leftIndex - rightIndex;
    return left.label.localeCompare(right.label, "zh-CN");
  });
});

const homeBroadcastStats = computed(() => {
  let ongoing = 0;
  let pending = 0;
  let events = 0;
  let processingEvents = 0;
  const ongoingItems: ScopeHomeBroadcastItem[] = [];
  const fallbackItems: ScopeHomeBroadcastItem[] = [];

  for (const scope of broadcastScopes.value) {
    let scopeOngoing = 0;
    let scopePending = 0;
    const scopeOngoingItems: ScopeHomeBroadcastItem[] = [];
    const scopePendingParts: string[] = [];
    const overviewItem = props.overview[scope.value] || {};
    const titleItems = Array.isArray(overviewItem.ongoing_titles) ? overviewItem.ongoing_titles : [];

    for (const workType of broadcastWorkTypes) {
      const counts = typedScopeCounts(scope.value, workType.key);
      if (counts.ongoing > 0) {
        scopeOngoing += counts.ongoing;
        scopeOngoingItems.push({
          key: `ongoing-${scope.value}-${workType.key}`,
          label: "进行中",
          text: `${scope.label} · ${workType.label} ${counts.ongoing} 条`,
          tone: "ongoing",
          scope: scope.value,
          workType: workType.key,
          action: "workbench",
        });
      }
      if (counts.pending > 0) {
        scopePending += counts.pending;
        scopePendingParts.push(`${workType.label}${counts.pending}`);
      }
    }
    if (titleItems.length) {
      const titledOngoingItems: ScopeHomeBroadcastItem[] = [];
      for (const [titleIndex, item] of titleItems.entries()) {
        const workType = String(item?.work_type || "");
        const title = String(item?.title || "").trim();
        if (!title) continue;
        const canOpenWorkbench = Boolean(broadcastWorkTypeLabelByKey[workType]);
        titledOngoingItems.push({
          key: `ongoing-title-${scope.value}-${String(item?.key || title)}-${titleIndex}`,
          label: "进行中",
          text: `${scope.label} · ${broadcastWorkTypeLabelByKey[workType] || "通告"} · ${title}`,
          tone: "ongoing",
          scope: scope.value,
          workType: canOpenWorkbench ? workType : undefined,
          action: canOpenWorkbench ? "workbench" : undefined,
        });
      }
      if (titledOngoingItems.length) {
        scopeOngoingItems.splice(0, scopeOngoingItems.length, ...titledOngoingItems);
        scopeOngoing = Math.max(scopeOngoing, titledOngoingItems.length);
      }
    }

    ongoing += scopeOngoing;
    pending += scopePending;
    ongoingItems.push(...scopeOngoingItems);

    const eventCounts = eventScopeCounts(scope.value);
    const eventTotal = eventCounts.total;
    const eventProcessing = eventCounts.processing;
    events += eventTotal;
    processingEvents += eventProcessing;

    if (scopePending > 0) {
      fallbackItems.push({
        key: `pending-${scope.value}`,
        label: "待发起",
        text: `${scope.label} · ${scopePendingParts.join("、")}，共 ${scopePending} 条`,
        tone: "pending",
      });
    }
    if (eventTotal > 0) {
      fallbackItems.push({
        key: `event-${scope.value}`,
        label: "事件",
        text: `${scope.label} · 本月 ${eventTotal} 条，处理中 ${eventProcessing} 条`,
        tone: "event",
        scope: scope.value,
        action: "event",
      });
    }
  }

  const allEventStats = props.overview.ALL || {};
  const hasAllEventStats = Object.prototype.hasOwnProperty.call(allEventStats, "event_total");
  return {
    ongoing,
    pending,
    events: hasAllEventStats ? Number(allEventStats.event_total || 0) : events,
    processingEvents: hasAllEventStats
      ? Number(allEventStats.event_processing || 0)
      : processingEvents,
    items: ongoing > 0 ? ongoingItems : fallbackItems,
  };
});

const homeModuleMetrics = computed<Record<string, ScopeHomeModuleMetric>>(() => {
  const maintenance = aggregateWorkTypeCounts("maintenance");
  const change = aggregateWorkTypeCounts("change");
  const repair = aggregateWorkTypeCounts("repair");
  const stats = homeBroadcastStats.value;
  return {
    event: {
      primaryLabel: "本月事件",
      primaryValue: stats.events,
      secondaryLabel: "处理中",
      secondaryValue: stats.processingEvents,
    },
    maintenance: {
      primaryLabel: "待发起",
      primaryValue: maintenance.pending,
      secondaryLabel: "进行中",
      secondaryValue: maintenance.ongoing,
    },
    change: {
      primaryLabel: "待发起",
      primaryValue: change.pending,
      secondaryLabel: "进行中",
      secondaryValue: change.ongoing,
    },
    repair_management: {
      primaryLabel: "待发起",
      primaryValue: repair.pending,
      secondaryLabel: "进行中",
      secondaryValue: repair.ongoing,
    },
  };
});

const homeBroadcastItems = computed<ScopeHomeBroadcastItem[]>(() => {
  const items = homeBroadcastStats.value.items;
  if (items.length > BROADCAST_ITEM_LIMIT) {
    return [
      ...items.slice(0, BROADCAST_ITEM_LIMIT),
      {
        key: "broadcast-more",
        label: "更多",
        text: `还有 ${items.length - BROADCAST_ITEM_LIMIT} 条动态，进入对应模块查看`,
        tone: "quiet",
      },
    ];
  }
  if (items.length) return items;
  return [{
    key: "quiet",
    label: "就绪",
    text: "当前账号暂无进行中、待发起或事件提醒",
    tone: "quiet",
  }];
});

const homeBroadcastSummary = computed(() => {
  const stats = homeBroadcastStats.value;
  const scopeCount = broadcastScopes.value.length || displayScopeOptions.value.length;
  return `楼栋 ${scopeCount} · 进行中 ${stats.ongoing} · 待发起 ${stats.pending} · 事件 ${stats.events}`;
});

function aggregateWorkTypeCounts(workType: string): { pending: number; ongoing: number } {
  return broadcastScopes.value.reduce(
    (total, scope) => {
      const counts = typedScopeCounts(scope.value, workType);
      total.pending += counts.pending;
      total.ongoing += counts.ongoing;
      return total;
    },
    { pending: 0, ongoing: 0 },
  );
}

function activateBroadcastItem(item: ScopeHomeBroadcastItem): void {
  const scope = normalizeScopeValue(String(item.scope || ""), "");
  if (!scope) return;
  if (item.action === "event") {
    emit("event", scope);
    return;
  }
  const workType = String(item.workType || "").trim();
  if (item.action === "workbench" && workType) {
    emit("prefetch", scope, workType);
    emit("enter", scope, workType);
  }
}

function selectEntry(key: EntryKey): void {
  if (!key) return;
  if (key === "event") {
    const scope = defaultEventScope();
    if (scope) emit("event", scope);
    return;
  }
  if (key === "critical_guard") {
    emit("critical-guard");
    return;
  }
  if (key === "drill") {
    emit("drill");
    return;
  }
  if (key === "cabinet_power") {
    emit("cabinet-power");
    return;
  }
  activeMode.value = key;
  emit("dashboard-visible", false);
  if (key === "repair_management") void loadRepairOverview();
  if (key === "water") void loadWaterBuildings();
}

async function loadRepairOverview(): Promise<void> {
  if (repairOverviewLoading.value || repairOverviewLoaded.value) return;
  repairOverviewLoading.value = true;
  try {
    repairOverview.value = await requestJson("/api/repair-management/overview");
    repairOverviewLoaded.value = true;
  } catch {
    repairOverview.value = {};
  } finally {
    repairOverviewLoading.value = false;
  }
}

function repairMetricValue(value: number): number | string {
  return repairOverviewLoading.value && !repairOverviewLoaded.value ? "…" : value;
}

function waterMetricValue(value: number, suffix = ""): string {
  if (waterBuildingsLoading.value && !waterBuildings.value.length) return "…";
  const formatted = suffix
    ? new Intl.NumberFormat("zh-CN", { maximumFractionDigits: 2 }).format(value)
    : new Intl.NumberFormat("zh-CN").format(value);
  return `${formatted}${suffix}`;
}

function clearWaterBuildingsPoll(): void {
  if (waterBuildingsPollTimer !== null) {
    window.clearTimeout(waterBuildingsPollTimer);
    waterBuildingsPollTimer = null;
  }
}

async function loadWaterBuildings(): Promise<void> {
  if (waterBuildingsLoading.value) return;
  waterBuildingsLoading.value = true;
  waterBuildingsError.value = "";
  try {
    const data = await requestJson("/api/capacity/water/buildings", { cache: "no-store" });
    waterBuildings.value = Array.isArray(data.scopes) ? data.scopes : [];
    waterSnapshot.value = data.snapshot && typeof data.snapshot === "object" ? data.snapshot : {};
    clearWaterBuildingsPoll();
    if (
      activeMode.value === "water"
      && waterSnapshot.value.refreshing
    ) {
      waterBuildingsPollTimer = window.setTimeout(() => {
        waterBuildingsPollTimer = null;
        void loadWaterBuildings();
      }, 1500);
    }
  } catch (error: any) {
    clearWaterBuildingsPoll();
    waterBuildingsError.value = error?.message || "水耗楼栋数据读取失败。";
  } finally {
    waterBuildingsLoading.value = false;
  }
}

function waterScopeItem(scope: string): Dict {
  const code = normalizeScopeValue(scope, "");
  return waterBuildings.value.find((item) => normalizeScopeValue(item.value, "") === code) || {};
}

function formatWaterDate(value: unknown): string {
  const numeric = Number(value || 0);
  if (!numeric) return "暂无";
  const date = new Date(numeric);
  if (Number.isNaN(date.getTime())) return "暂无";
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}-${String(date.getDate()).padStart(2, "0")}`;
}

function selectModuleAction(action: ModuleAction, disabled?: boolean): void {
  if (disabled || action.disabled || !action.key) return;
  selectEntry(action.key);
}

function returnFromFeature(): void {
  if (activeMode.value === "water") clearWaterBuildingsPoll();
  activeMode.value = isToolScopeMode.value ? "tools" : "";
  emit("dashboard-visible", !activeMode.value);
}

function enterNoticeWorkbench(scope: string): void {
  const workType = activeConfig.value.workType || "maintenance";
  const key = workbenchEntryKey(scope, workType);
  if (openingWorkbenchKey.value) return;
  openingWorkbenchKey.value = key;
  emit("prefetch", scope, workType);
  emit("enter", scope, workType);
  window.setTimeout(() => {
    if (openingWorkbenchKey.value === key) openingWorkbenchKey.value = "";
  }, 15000);
}

function prefetchNoticeWorkbench(scope: string): void {
  if (openingWorkbenchKey.value) return;
  emit("prefetch", scope, activeConfig.value.workType || "maintenance");
}

function workbenchEntryKey(scope: string, workType: string): string {
  return `${normalizeScopeValue(scope)}:${workType || "maintenance"}`;
}

function isOpeningWorkbench(scope: string): boolean {
  return openingWorkbenchKey.value === workbenchEntryKey(
    scope,
    activeConfig.value.workType || "maintenance",
  );
}

function scopeCardIsEnabled(scope: string): boolean {
  if (activeMode.value === "handover") {
    return Boolean(props.handoverLinks[normalizeScopeValue(scope, "")]);
  }
  return !openingWorkbenchKey.value;
}

function activateScopeCard(event: MouseEvent | KeyboardEvent, scope: string): void {
  const card = event.currentTarget as HTMLElement | null;
  const target = event.target as Element | null;
  if (!card || !scopeCardIsEnabled(scope)) return;
  if (target !== card && target?.closest("button, a, input, select, textarea, label")) return;
  if (event instanceof KeyboardEvent && event.key === " ") event.preventDefault();
  card.querySelector<HTMLElement>(".scope-actions .primary:not(:disabled)")?.click();
}

function defaultEventScope(): string {
  const values = props.scopeOptions.map((item) => normalizeScopeValue(item.value, "")).filter(Boolean);
  return values.find((value) => value === "ALL")
    || values.find((value) => value === "CAMPUS")
    || values[0]
    || "";
}

function typedScopeCounts(scope: string, workType: string): { pending: number; ongoing: number } {
  return resolveTypedScopeCounts(props.overview, scope, workType);
}

function eventScopeCounts(scope: string): { total: number; processing: number } {
  const code = normalizeScopeValue(scope, "ALL");
  const item = props.overview[code] || {};
  return {
    total: Number(item.event_total || item.event_pending || item.event_count || 0),
    processing: Number(item.event_processing || item.event_ongoing || item.event_open || 0),
  };
}

function scopeCounts(scope: string): { pending: number; ongoing: number } {
  const code = normalizeScopeValue(scope, "ALL");
  const item = props.overview[code] || {};
  if (activeMode.value === "repair_management") {
    const repairScopes = repairOverview.value.scopes && typeof repairOverview.value.scopes === "object"
      ? repairOverview.value.scopes as Record<string, Dict>
      : {};
    const repairItem = repairScopes[code] || {};
    return {
      pending: Number(repairItem.pending || 0),
      ongoing: Number(repairItem.in_progress || 0),
    };
  }
  if (activeMetricWorkType.value === "event") {
    return {
      pending: Number(item.event_total || item.total || 0),
      ongoing: Number(item.event_processing || item.processing || 0),
    };
  }
  if (activeMetricWorkType.value === "handover") {
    return {
      pending: props.handoverLinks[code] ? 1 : 0,
      ongoing: 0,
    };
  }
  return typedScopeCounts(code, activeMetricWorkType.value);
}

function scopePrimaryMetricLabel(scope: string): string {
  if (activeMode.value === "repair_management") return "待检修";
  if (activeMetricWorkType.value === "event") return "本月";
  if (activeMetricWorkType.value === "handover") return props.handoverLinks[normalizeScopeValue(scope, "")] ? "已配置" : "未配置";
  return "待发起";
}

function scopeSecondaryMetricLabel(_scope: string): string {
  if (activeMode.value === "repair_management") return "进行中";
  if (activeMetricWorkType.value === "event") return "处理中";
  if (activeMetricWorkType.value === "handover") return "待配置";
  return "进行中";
}

onMounted(() => emit("dashboard-visible", !activeMode.value));
onBeforeUnmount(clearWaterBuildingsPoll);

</script>

<style scoped>
.home-shell {
  padding: 16px 22px 26px;
  display: grid;
  gap: 10px;
}

.home-shell.dashboard-mode {
  padding: 0;
}

.scope-grid,
.tool-grid {
  display: grid;
  gap: 12px;
}

.feature-section,
.scope-card,
.tool-card {
  border: 1px solid #d8e5f7;
  background: rgba(255, 255, 255, 0.92);
  box-shadow: 0 18px 42px rgba(15, 73, 153, 0.12);
}

.scope-card::after {
  content: "";
  position: absolute;
  right: -36px;
  bottom: -44px;
  width: 132px;
  height: 92px;
  pointer-events: none;
  opacity: 0.42;
  background:
    repeating-linear-gradient(0deg, rgba(22, 120, 255, 0.12) 0 1px, transparent 1px 15px),
    repeating-linear-gradient(90deg, rgba(22, 120, 255, 0.08) 0 1px, transparent 1px 15px);
  transform: rotate(-14deg);
}

.scope-card strong,
.tool-card strong {
  color: #071a39;
  font-weight: 900;
}

.feature-section__head p,
.scope-card span,
.tool-card small {
  margin: 0;
  color: #5e728f;
  line-height: 1.7;
}

.tool-icon {
  display: inline-grid;
  place-items: center;
  border-radius: 14px;
  color: #fff;
  box-shadow: 0 14px 24px rgba(21, 92, 214, 0.22);
}

.tool-icon::before {
  content: "";
  width: 24px;
  height: 24px;
  border: 3px solid currentColor;
  border-radius: 7px;
}

.tool-icon.link::before {
  width: 28px;
  height: 14px;
  border-radius: 999px;
  transform: rotate(-35deg);
}

.tool-icon.daily::before {
  width: 27px;
  height: 24px;
  border-width: 3px;
  border-radius: 5px;
  background:
    linear-gradient(currentColor, currentColor) 50% 7px / 100% 3px no-repeat,
    radial-gradient(circle, currentColor 0 2px, transparent 2.5px)
      4px 13px / 9px 8px repeat-x;
}

.tool-icon {
  width: 46px;
  height: 46px;
  background: linear-gradient(135deg, #2a77ff, #004fc4);
}
.tool-icon.adjust::before {
  border-radius: 50%;
}
.tool-icon.polling::before,
.tool-icon.power::before {
  width: 28px;
  height: 8px;
  border-radius: 999px;
  box-shadow: 0 -10px 0 currentColor, 0 10px 0 currentColor;
  border: 0;
  background: currentColor;
}


.feature-section {
  padding: 20px;
  border-radius: 20px;
  display: grid;
  gap: 14px;
}

.scope-loading-state {
  display: grid;
  min-height: 260px;
  place-items: center;
  border: 1px dashed #bdd4f2;
  border-radius: 16px;
  background: #f7fbff;
  color: #52708f;
  font-size: 14px;
  font-weight: 850;
}

.scope-loading-state.error {
  align-content: center;
  gap: 12px;
  border-color: #f3c5c5;
  background: #fff8f8;
  color: #a63737;
}

.scope-loading-state.error button {
  justify-self: center;
  min-height: 34px;
  padding: 0 16px;
  border: 1px solid #d8e5f7;
  border-radius: 9px;
  background: #fff;
  color: #1554b8;
  cursor: pointer;
  font: inherit;
  font-weight: 850;
}

.feature-section.scope-selection {
  position: relative;
  overflow: hidden;
  padding: 20px;
  border-radius: 24px;
  background:
    linear-gradient(180deg, rgba(255, 255, 255, 0.98), rgba(249, 252, 255, 0.94)),
    radial-gradient(circle at 84% 8%, rgba(66, 153, 255, 0.14), transparent 32%);
  box-shadow:
    0 26px 68px rgba(18, 73, 140, 0.13),
    inset 0 1px 0 rgba(255, 255, 255, 0.86);
}

.feature-section__head {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 18px;
}

.feature-section__head.scope-section-head {
  display: grid;
  grid-template-columns: minmax(240px, 0.88fr) minmax(390px, 1.32fr);
  align-items: center;
  gap: 18px;
}

.page-back-row {
  display: flex;
  align-items: center;
  justify-content: flex-start;
  gap: 8px;
}

.page-back-btn {
  min-height: 36px;
  padding: 0 13px;
  border-radius: 999px;
  box-shadow: 0 8px 20px rgba(22, 78, 151, 0.08);
}

.page-back-btn span {
  margin-top: 0 !important;
  font-size: 19px;
  line-height: 1;
}

.feature-title-block {
  min-width: 0;
}

.feature-section__head h2 {
  margin: 5px 0 2px;
  color: #071a39;
  font-size: 20px;
  font-weight: 950;
}

.scope-section-head h2 {
  font-size: 23px;
  line-height: 1.12;
}

.scope-summary-strip {
  min-height: 62px;
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  align-items: center;
  overflow: hidden;
  border: 1px solid #e1ebf8;
  border-radius: 20px;
  background: rgba(255, 255, 255, 0.82);
  box-shadow:
    0 16px 34px rgba(28, 84, 161, 0.09),
    inset 0 1px 0 rgba(255, 255, 255, 0.78);
}

.scope-summary-strip.repair-metrics {
  grid-template-columns: repeat(4, minmax(0, 1fr));
}

.scope-summary-strip article {
  min-width: 0;
  height: 100%;
  display: grid;
  grid-template-columns: 34px minmax(0, 1fr);
  grid-template-rows: auto auto;
  align-content: center;
  column-gap: 9px;
  row-gap: 2px;
  padding: 10px 14px;
  border-left: 1px solid #e6eef8;
}

.scope-summary-strip article:first-child {
  border-left: 0;
}

.scope-summary-strip small {
  color: #73839c;
  font-size: 12px;
  font-weight: 900;
}

.scope-summary-strip strong {
  color: #075bd8;
  font-size: 21px;
  line-height: 1;
  font-weight: 950;
}

.summary-icon {
  grid-row: 1 / span 2;
  width: 34px;
  height: 34px;
  display: inline-grid;
  place-items: center;
  border-radius: 15px;
  background: linear-gradient(135deg, #2a77ff, #0055d8);
  box-shadow: 0 12px 24px rgba(31, 101, 255, 0.18);
}

.summary-icon.monthly {
  background: linear-gradient(135deg, #14b8a6, #059669);
}

.summary-icon::before {
  content: "";
  width: 19px;
  height: 23px;
  border: 3px solid #ffffff;
  border-radius: 5px;
}

.summary-icon.ongoing {
  background: linear-gradient(135deg, #2bd4be, #0a9c86);
}

.summary-icon.ongoing::before {
  width: 21px;
  height: 21px;
  border-radius: 50%;
  background: linear-gradient(#ffffff, #ffffff) 50% 28% / 3px 9px no-repeat;
}

.summary-icon.coverage {
  background: linear-gradient(135deg, #a46cff, #6b4be8);
}

.summary-icon.coverage::before {
  width: 22px;
  height: 17px;
  border-radius: 999px 999px 7px 7px;
}

.feature-section__actions {
  display: flex;
  flex-wrap: wrap;
  justify-content: flex-end;
  gap: 10px;
}

.feature-section__actions .back-button {
  min-width: 154px;
  border-radius: 14px;
}

.feature-section__actions .back-button span {
  margin-top: 0;
  font-size: 20px;
  line-height: 1;
}

.section-kicker {
  display: inline-flex;
  width: fit-content;
  padding: 6px 12px;
  border-radius: 999px;
  background: #eaf3ff;
  color: #125bd2;
  font-size: 12px;
  font-weight: 900;
}

.scope-grid {
  grid-template-columns: repeat(3, minmax(0, 1fr));
}

.scope-overview-grid {
  gap: 14px 16px;
}

.scope-card {
  position: relative;
  overflow: hidden;
  min-height: 144px;
  display: flex;
  flex-direction: column;
  justify-content: space-between;
  gap: 10px;
  padding: 18px 20px;
  border-radius: 18px;
  background:
    linear-gradient(135deg, rgba(255, 255, 255, 0.98), rgba(247, 251, 255, 0.94)),
    radial-gradient(circle at 92% 14%, rgba(28, 108, 255, 0.12), transparent 31%);
  isolation: isolate;
}

.scope-card.interactive {
  cursor: pointer;
}

.scope-card.interactive:focus-visible {
  outline: 3px solid rgba(22, 120, 255, 0.28);
  outline-offset: 3px;
}

.scope-card::before {
  content: "";
  position: absolute;
  inset: 0 auto auto 0;
  width: 100%;
  height: 4px;
  background: linear-gradient(90deg, #2c7cff, #0bc2d6);
}

.scope-card strong {
  display: block;
  font-size: 21px;
  line-height: 1.12;
}

.scope-card span {
  display: block;
  margin-top: 8px;
  font-size: 13px;
}

.scope-card__main {
  position: relative;
  z-index: 1;
  display: flex;
  align-items: center;
  gap: 10px;
}

.scope-building-icon {
  width: 32px;
  height: 32px;
  flex: 0 0 auto;
  margin-top: 0 !important;
  border-radius: 11px;
  background: #eaf4ff;
  color: #1e72df;
  box-shadow: inset 0 0 0 1px rgba(80, 139, 222, 0.12);
}

.scope-building-icon::before {
  content: "";
  display: block;
  width: 18px;
  height: 22px;
  margin: 6px auto;
  border: 2px solid currentColor;
  border-radius: 3px;
  background:
    linear-gradient(currentColor, currentColor) 5px 4px / 3px 3px no-repeat,
    linear-gradient(currentColor, currentColor) 11px 4px / 3px 3px no-repeat,
    linear-gradient(currentColor, currentColor) 5px 10px / 3px 3px no-repeat,
    linear-gradient(currentColor, currentColor) 11px 10px / 3px 3px no-repeat;
}

.scope-building-icon.all::before {
  width: 22px;
  height: 18px;
  margin-top: 8px;
  border: 0;
  border-radius: 3px;
  background:
    linear-gradient(currentColor, currentColor) 50% 0 / 22px 4px no-repeat,
    linear-gradient(currentColor, currentColor) 50% 7px / 22px 4px no-repeat,
    linear-gradient(currentColor, currentColor) 50% 14px / 22px 4px no-repeat;
}

.scope-building-icon.campus::before {
  border-radius: 50%;
}

.scope-badges {
  position: relative;
  z-index: 1;
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
}

.scope-badges span {
  display: inline-flex;
  align-items: center;
  min-height: 24px;
  margin-top: 0;
  padding: 4px 8px;
  border-radius: 999px;
  background: #eef5ff;
  color: #1763d7;
  font-size: 12px;
  font-weight: 900;
  line-height: 1;
}

.scope-badges span + span {
  background: #e9fbf7;
  color: #087c67;
}

.scope-hint {
  position: relative;
  z-index: 1;
  min-height: 18px;
  margin: -2px 0 0;
  color: #6d7f98;
  font-size: 12px;
  font-weight: 800;
}

.scope-actions {
  position: relative;
  z-index: 1;
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  padding-top: 4px;
}

.scope-actions .primary,
.scope-actions .secondary {
  min-height: 34px;
  border-radius: 999px;
  padding-inline: 14px;
}

.scope-building-art {
  position: absolute;
  right: 12px;
  bottom: 8px;
  z-index: 0;
  width: 104px;
  height: 66px;
  margin-top: 0 !important;
  opacity: 0.22;
  pointer-events: none;
  background:
    linear-gradient(180deg, rgba(38, 122, 230, 0.1), rgba(38, 122, 230, 0.26)),
    linear-gradient(90deg, transparent 0 17%, rgba(22, 101, 216, 0.32) 17% 19%, transparent 19% 38%, rgba(22, 101, 216, 0.32) 38% 40%, transparent 40% 59%, rgba(22, 101, 216, 0.32) 59% 61%, transparent 61%),
    repeating-linear-gradient(0deg, transparent 0 12px, rgba(22, 101, 216, 0.28) 12px 14px);
  clip-path: polygon(16% 28%, 43% 10%, 68% 28%, 68% 100%, 16% 100%);
}

.scope-card.scope-all {
  border-color: #82b5ef;
  background:
    linear-gradient(135deg, rgba(247, 252, 255, 0.98), rgba(235, 246, 255, 0.95)),
    radial-gradient(circle at 88% 16%, rgba(32, 113, 225, 0.18), transparent 34%);
}

.scope-card.scope-all .scope-building-icon {
  color: #1763d7;
  background: #e4f0ff;
}

.tool-grid {
  grid-template-columns: repeat(3, minmax(0, 1fr));
}

.tool-card {
  width: 100%;
  min-height: 112px;
  display: grid;
  grid-template-columns: 56px minmax(0, 1fr) auto;
  align-items: center;
  gap: 16px;
  padding: 20px;
  border-radius: 20px;
  text-align: left;
  cursor: pointer;
}

.tool-card:hover,
.scope-card:hover {
  border-color: #b7d0f5;
  box-shadow: 0 22px 54px rgba(15, 73, 153, 0.16);
}

.tool-card small {
  display: block;
  margin-top: 6px;
  font-size: 13px;
}

.tool-card b {
  padding: 7px 12px;
  border-radius: 999px;
  background: #f2f7ff;
  color: #1763d7;
  font-size: 12px;
}

.tool-icon {
  width: 56px;
  height: 56px;
}

.tool-card.cyan .tool-icon {
  background: linear-gradient(135deg, #27d1df, #0a8fb8);
}

.tool-card.emerald .tool-icon {
  background: linear-gradient(135deg, #29cd8d, #07945f);
}

.tool-card.slate .tool-icon {
  background: linear-gradient(135deg, #5d9df4, #2a65bd);
}

button,
a.primary,
a.secondary {
  border: none;
  font: inherit;
  text-decoration: none;
}

.primary,
.secondary {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  min-height: 44px;
  padding: 7px 12px;
  border-radius: 14px;
  font-size: 13px;
  font-weight: 900;
  cursor: pointer;
  transition: transform 0.16s ease, box-shadow 0.16s ease, border-color 0.16s ease, background 0.16s ease;
}

.primary {
  background: linear-gradient(135deg, #1f6dff, #0055d8);
  color: #fff;
  box-shadow: 0 14px 24px rgba(30, 99, 255, 0.25);
}

.secondary {
  border: 1px solid #d4e3f7;
  background: rgba(255, 255, 255, 0.92);
  color: #1b5bbd;
}

.primary:hover,
.secondary:hover {
  box-shadow: 0 12px 26px rgba(21, 92, 214, 0.16);
}

.primary:disabled,
.secondary:disabled {
  cursor: not-allowed;
  opacity: 0.58;
  transform: none;
  box-shadow: none;
}

.primary:focus-visible,
.secondary:focus-visible,
.tool-card:focus-visible {
  outline: 3px solid rgba(22, 120, 255, 0.28);
  outline-offset: 3px;
}

@media (prefers-reduced-motion: reduce) {
  *,
  *::before,
  *::after {
    transition: none !important;
    animation: none !important;
    scroll-behavior: auto !important;
  }
}

@media (max-width: 1280px) {
  .scope-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .feature-section__head.scope-section-head {
    grid-template-columns: minmax(0, 1fr);
  }

  .scope-summary-strip {
    width: 100%;
  }

  .feature-section__actions {
    justify-content: flex-start;
  }
}

@media (max-width: 760px) {
  .home-shell {
    padding: 22px 18px 30px;
  }

  .scope-grid,
  .tool-grid {
    grid-template-columns: 1fr;
  }

  .tool-card {
    grid-template-columns: 54px minmax(0, 1fr);
  }

  .tool-card b {
    grid-column: 2;
    justify-self: start;
  }

  .feature-section__head {
    align-items: flex-start;
    flex-direction: column;
  }

  .feature-section.scope-selection {
    padding: 22px;
    border-radius: 22px;
  }

  .scope-summary-strip {
    grid-template-columns: 1fr;
  }

  .scope-summary-strip article {
    border-left: 0;
    border-top: 1px solid #e6eef8;
  }

  .scope-summary-strip article:first-child {
    border-top: 0;
  }

}
</style>
