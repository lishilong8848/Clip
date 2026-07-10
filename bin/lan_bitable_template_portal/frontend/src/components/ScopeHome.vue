<template>
  <section class="home-shell">
    <HomeBroadcastTicker
      v-if="!activeMode"
      :items="homeBroadcastItems"
      :summary="homeBroadcastSummary"
    />

    <div v-if="canRequestMoreScopes" class="permission-more-card">
      <div>
        <span class="section-kicker">权限申请</span>
        <strong>需要访问其他楼栋？</strong>
      </div>
      <button type="button" class="secondary" @click="$emit('request-permission')">申请其他楼权限</button>
    </div>

    <template v-if="!activeMode">
      <div class="module-heading">
        <div>
          <h2>业务工作台</h2>
        </div>
        <span>已开放 {{ enabledModuleCount }} / 共 {{ moduleCards.length }} 个模块</span>
      </div>

      <div class="module-grid">
        <article
          v-for="module in moduleCards"
          :key="module.key"
          class="module-card"
          :class="[module.tone, module.size || 'compact', { disabled: module.disabled }]"
          :aria-disabled="module.disabled ? 'true' : 'false'"
          :title="module.disabled ? '暂未开放' : ''"
        >
          <div class="module-card__head">
            <span class="module-icon" :class="module.icon" aria-hidden="true"></span>
            <span class="module-badge">{{ module.badge }}</span>
          </div>
          <div class="module-card__body">
            <strong>{{ module.title }}</strong>
            <span v-if="module.disabled" class="module-disabled-note">暂未开放</span>
            <div class="module-tags">
              <span v-for="tag in module.tags" :key="tag">{{ tag }}</span>
            </div>
          </div>
          <div class="module-actions">
            <button type="button"
              v-for="action in module.actions"
              :key="action.key"
              :class="action.primary ? 'primary' : 'secondary'"
              :disabled="module.disabled || action.disabled"
              :title="module.disabled || action.disabled ? '暂未开放' : ''"
              @click.stop="selectModuleAction(action, module.disabled)"
            >
              {{ action.label }}
              <span v-if="!module.disabled && !action.disabled" aria-hidden="true">›</span>
            </button>
          </div>
        </article>
      </div>
    </template>

    <section v-else class="feature-section" :class="{ 'scope-selection': activeMode !== 'tools' }">
      <div class="page-back-row">
        <VnetBackButton @click="returnFromFeature" />
      </div>
      <header class="feature-section__head" :class="{ 'scope-section-head': activeMode !== 'tools' }">
        <div class="feature-title-block">
          <span class="section-kicker">{{ activeConfig.kicker }}</span>
          <h2>{{ activeConfig.title }}</h2>
        </div>
        <div v-if="activeMode !== 'tools'" class="scope-summary-strip" aria-label="当前模块楼栋统计">
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

      <div v-else class="scope-grid scope-overview-grid">
        <article
          v-for="scope in displayScopeOptions"
          :key="scope.value"
          class="scope-card"
          :class="scopeCardClass(scope.value)"
        >
          <div class="scope-card__main">
            <span class="scope-building-icon" :class="scopeIconClass(scope.value)" aria-hidden="true"></span>
            <strong>{{ scopeDisplayLabel(scope) }}</strong>
          </div>
          <div class="scope-badges">
            <span>{{ scopePrimaryMetricLabel(scope.value) }} {{ scopeCounts(scope.value).pending }}</span>
            <span>{{ scopeSecondaryMetricLabel(scope.value) }} {{ scopeCounts(scope.value).ongoing }}</span>
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
              @click="enterNoticeWorkbench(scope.value)"
            >
              {{ activeConfig.actionLabel }}
            </button>
          </div>
          <span class="scope-building-art" aria-hidden="true"></span>
        </article>
      </div>
    </section>
  </section>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";
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
  type ScopeHomeModuleAction as ModuleAction,
} from "../scopeHomeUtils";
import HomeBroadcastTicker from "./HomeBroadcastTicker.vue";
import VnetBackButton from "./VnetBackButton.vue";

type Dict = Record<string, any>;
type HomeBroadcastItem = {
  key: string;
  label: string;
  text: string;
  tone: "ongoing" | "pending" | "event" | "quiet";
};

const props = defineProps<{
  scopeOptions: Array<{ value: string; label: string }>;
  overview: Record<string, Dict>;
  handoverLinks: Record<string, string>;
  canRequestMoreScopes?: boolean;
}>();

const emit = defineEmits<{
  enter: [scope: string, workType?: string];
  event: [scope: string];
  engineer: [scope: string];
  "repair-management": [scope: string];
  "request-permission": [];
}>();

const activeMode = ref<EntryKey>("");

const enabledModuleCount = computed(() => moduleCards.filter((item) => !item.disabled).length);
const activeConfig = computed(() => entryConfigs[activeMode.value || "tools"]);
const isToolScopeMode = computed(() => ["power", "polling", "adjust", "handover"].includes(activeMode.value));
const activeMetricWorkType = computed(() => {
  if (activeMode.value === "maintenance_mop") return "maintenance";
  if (activeMode.value === "repair_management") return "repair";
  if (activeMode.value === "event") return "event";
  if (activeMode.value === "handover") return "handover";
  return activeConfig.value.workType || "maintenance";
});
const displayScopeOptions = computed(() => {
  return [...props.scopeOptions].sort((left, right) => {
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
  const ongoingItems: HomeBroadcastItem[] = [];
  const fallbackItems: HomeBroadcastItem[] = [];

  for (const scope of broadcastScopes.value) {
    let scopeOngoing = 0;
    let scopePending = 0;
    const scopeOngoingItems: HomeBroadcastItem[] = [];
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
        });
      }
      if (counts.pending > 0) {
        scopePending += counts.pending;
        scopePendingParts.push(`${workType.label}${counts.pending}`);
      }
    }
    if (titleItems.length) {
      scopeOngoingItems.splice(0, scopeOngoingItems.length);
      for (const item of titleItems) {
        const workType = String(item?.work_type || "");
        const title = String(item?.title || "").trim();
        if (!title) continue;
        scopeOngoingItems.push({
          key: `ongoing-title-${scope.value}-${String(item?.key || title)}`,
          label: "进行中",
          text: `${scope.label} · ${broadcastWorkTypeLabelByKey[workType] || "通告"} · ${title}`,
          tone: "ongoing",
        });
      }
    }

    ongoing += scopeOngoing;
    pending += scopePending;
    ongoingItems.push(...scopeOngoingItems);

    const eventCounts = eventScopeCounts(scope.value);
    const eventTotal = eventCounts.total;
    const eventProcessing = eventCounts.processing;
    events += eventTotal;

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
      });
    }
  }

  return {
    ongoing,
    pending,
    events,
    items: ongoing > 0 ? ongoingItems : fallbackItems,
  };
});

const homeBroadcastItems = computed<HomeBroadcastItem[]>(() => {
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
  if (stats.ongoing > 0) return `有权限楼栋 ${scopeCount} 个 · 进行中 ${stats.ongoing} 条`;
  if (stats.pending > 0) return `有权限楼栋 ${scopeCount} 个 · 待发起 ${stats.pending} 条`;
  if (stats.events > 0) return `有权限楼栋 ${scopeCount} 个 · 事件 ${stats.events} 条`;
  return `有权限楼栋 ${scopeCount} 个 · 数据就绪`;
});

function selectEntry(key: EntryKey): void {
  if (!key) return;
  if (key === "event") {
    const scope = defaultEventScope();
    if (scope) emit("event", scope);
    return;
  }
  activeMode.value = key;
}

function selectModuleAction(action: ModuleAction, disabled?: boolean): void {
  if (disabled || action.disabled || !action.key) return;
  selectEntry(action.key);
}

function returnFromFeature(): void {
  activeMode.value = isToolScopeMode.value ? "tools" : "";
}

function enterNoticeWorkbench(scope: string): void {
  const workType = activeConfig.value.workType || "maintenance";
  emit("enter", scope, workType);
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
  if (activeMetricWorkType.value === "event") return "本月";
  if (activeMetricWorkType.value === "handover") return props.handoverLinks[normalizeScopeValue(scope, "")] ? "已配置" : "未配置";
  return "待发起";
}

function scopeSecondaryMetricLabel(_scope: string): string {
  if (activeMetricWorkType.value === "event") return "处理中";
  if (activeMetricWorkType.value === "handover") return "待配置";
  return "进行中";
}

</script>

<style scoped>
.home-shell {
  padding: 16px 22px 26px;
  display: grid;
  gap: 10px;
}

.module-grid,
.scope-grid,
.tool-grid {
  display: grid;
  gap: 12px;
}

.permission-more-card,
.module-card,
.feature-section,
.scope-card,
.tool-card {
  border: 1px solid #d8e5f7;
  background: rgba(255, 255, 255, 0.92);
  box-shadow: 0 18px 42px rgba(15, 73, 153, 0.12);
}

.module-card::after,
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

.module-card strong,
.scope-card strong,
.tool-card strong {
  color: #071a39;
  font-weight: 900;
}

.module-card p,
.feature-section__head p,
.scope-card span,
.tool-card small,
.permission-more-card p,
.module-heading p {
  margin: 0;
  color: #5e728f;
  line-height: 1.7;
}

.module-icon,
.tool-icon {
  display: inline-grid;
  place-items: center;
  border-radius: 14px;
  color: #fff;
  box-shadow: 0 14px 24px rgba(21, 92, 214, 0.22);
}

.module-icon::before,
.tool-icon::before {
  content: "";
  width: 24px;
  height: 24px;
  border: 3px solid currentColor;
  border-radius: 7px;
}

.module-icon.wrench::before {
  width: 25px;
  height: 16px;
  border-radius: 5px;
}

.tool-icon.link::before {
  width: 28px;
  height: 14px;
  border-radius: 999px;
  transform: rotate(-35deg);
}

.permission-more-card {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 18px;
  padding: 13px 16px;
  border-radius: 16px;
}

.permission-more-card strong {
  display: block;
  margin-top: 3px;
  color: #071a39;
  font-size: 16px;
  font-weight: 900;
}

.module-heading {
  display: flex;
  align-items: end;
  justify-content: space-between;
  gap: 16px;
  margin-top: 2px;
}

.module-heading h2 {
  margin: 0;
  color: #071a39;
  font-size: 18px;
  font-weight: 950;
}

.module-heading span {
  align-self: center;
  padding: 6px 12px;
  border: 1px solid #d8e5f7;
  border-radius: 999px;
  background: rgba(255, 255, 255, 0.7);
  color: #1b5bbd;
  font-size: 13px;
  font-weight: 900;
}

.module-grid {
  grid-template-columns: repeat(12, minmax(0, 1fr));
  align-items: stretch;
}

.module-card {
  position: relative;
  overflow: hidden;
  grid-column: span 3;
  min-height: 128px;
  display: flex;
  flex-direction: column;
  gap: 7px;
  padding: 13px 15px;
  border-radius: 18px;
  cursor: default;
  transition: transform 0.16s ease, box-shadow 0.16s ease, border-color 0.16s ease;
}

.module-card.main {
  grid-column: span 4;
  min-height: 156px;
  padding: 15px 18px;
  gap: 8px;
}

.module-card.disabled {
  cursor: default;
  border-color: rgba(216, 229, 247, 0.7);
  background:
    linear-gradient(135deg, rgba(255, 255, 255, 0.72), rgba(241, 245, 249, 0.82)),
    #f8fafc;
  box-shadow: 0 8px 22px rgba(71, 85, 105, 0.06);
  opacity: 0.88;
}

.module-card.disabled .module-icon {
  filter: grayscale(0.45);
  opacity: 0.72;
}

.module-card.disabled .module-badge,
.module-card.disabled .module-tags span {
  color: #64748b;
  background: rgba(241, 245, 249, 0.88);
}

.module-card.disabled .module-card__body strong,
.module-card.disabled .module-card__body p {
  color: #64748b;
}

.module-card.disabled::before {
  background: #cbd5e1;
}

.module-card.disabled .module-actions {
  display: none;
}

.module-card.disabled .module-tags {
  opacity: 0.72;
}

.module-disabled-note {
  width: fit-content;
  padding: 5px 9px;
  border: 1px solid rgba(203, 213, 225, 0.86);
  border-radius: 999px;
  background: rgba(255, 255, 255, 0.78);
  color: #64748b;
  font-size: 12px;
  font-weight: 900;
}

.module-card:focus-visible {
  outline: 3px solid rgba(22, 120, 255, 0.28);
  outline-offset: 3px;
}

.module-card::before {
  content: "";
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  height: 4px;
  background: #1e63ff;
}

.module-card.violet::before {
  background: #7657e6;
}

.module-card.orange::before {
  background: #ff8a3d;
}

.module-card.cyan::before {
  background: #11b7ca;
}

.module-card.emerald::before {
  background: #22b981;
}

.module-card.rose::before {
  background: #ef5260;
}

.module-card.slate::before {
  background: #4b86d9;
}

.module-card__head {
  display: flex;
  justify-content: space-between;
  gap: 16px;
  align-items: center;
}

.module-icon,
.tool-icon {
  width: 46px;
  height: 46px;
  background: linear-gradient(135deg, #2a77ff, #004fc4);
}

.module-card.main .module-icon {
  width: 54px;
  height: 54px;
}

.module-card.orange .module-icon {
  background: linear-gradient(135deg, #ff984d, #f36a32);
}

.module-card.violet .module-icon {
  background: linear-gradient(135deg, #8b68ff, #6044d6);
}

.module-card.cyan .module-icon {
  background: linear-gradient(135deg, #27d1df, #0a8fb8);
}

.module-card.emerald .module-icon {
  background: linear-gradient(135deg, #29cd8d, #07945f);
}

.module-card.rose .module-icon {
  background: linear-gradient(135deg, #ff6871, #dd3447);
}

.module-card.slate .module-icon {
  background: linear-gradient(135deg, #4b9bff, #2260b9);
}

.module-icon.switch::before {
  border-radius: 50%;
  background:
    linear-gradient(currentColor, currentColor) 50% 50% / 26px 3px no-repeat;
}

.module-icon.repair::before,
.module-icon.drill::before,
.module-icon.capacity::before,
.module-icon.risk::before,
.tool-icon.adjust::before {
  border-radius: 50%;
}

.module-icon.event::before {
  width: 30px;
  height: 14px;
  border: 0;
  border-radius: 999px;
  background:
    linear-gradient(currentColor, currentColor) 0 50% / 100% 3px no-repeat,
    linear-gradient(115deg, transparent 0 40%, currentColor 41% 52%, transparent 53%);
}

.module-icon.more::before,
.tool-icon.polling::before,
.tool-icon.power::before {
  width: 28px;
  height: 8px;
  border-radius: 999px;
  box-shadow: 0 -10px 0 currentColor, 0 10px 0 currentColor;
  border: 0;
  background: currentColor;
}

.module-badge {
  padding: 6px 11px;
  border: 1px solid #dce8f8;
  border-radius: 999px;
  background: #f6faff;
  color: #1763d7;
  font-size: 12px;
  font-weight: 900;
}

.module-card__body {
  position: relative;
  z-index: 1;
  display: grid;
  gap: 6px;
  flex: 1;
}

.module-card__body strong {
  font-size: 18px;
  line-height: 1.15;
}

.module-card.main .module-card__body strong {
  font-size: 21px;
}

.module-card__body p {
  min-height: 24px;
  font-size: 13px;
  line-height: 1.35;
}

.module-card.compact .module-card__body p {
  min-height: 28px;
  font-size: 12px;
}

.module-tags {
  display: flex;
  flex-wrap: wrap;
  gap: 5px;
  margin-top: 1px;
}

.module-tags span {
  padding: 3px 7px;
  border: 1px solid #e0e9f6;
  border-radius: 999px;
  background: #f7fbff;
  color: #4e6381;
  font-size: 11px;
  font-weight: 800;
}

.module-actions {
  position: relative;
  z-index: 1;
  display: flex;
  flex-wrap: wrap;
  gap: 5px;
  padding-top: 6px;
  border-top: 1px solid #e8eef7;
}

.module-actions button {
  cursor: pointer;
}

.module-actions button:disabled {
  cursor: not-allowed;
}

.module-actions .primary {
  min-width: 124px;
}

.module-actions .secondary {
  min-height: 38px;
  padding: 0 12px;
  border-radius: 999px;
  box-shadow: none;
}

.feature-section {
  padding: 20px;
  border-radius: 20px;
  display: grid;
  gap: 14px;
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

.module-card:not(.disabled):focus-within,
.module-card:not(.disabled):hover {
  border-color: #b7d0f5;
  box-shadow: 0 22px 54px rgba(15, 73, 153, 0.14);
}

.module-card.disabled:hover {
  border-color: rgba(216, 229, 247, 0.7);
  box-shadow: 0 8px 22px rgba(71, 85, 105, 0.06);
  transform: none;
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
  .module-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .module-card,
  .module-card.main {
    grid-column: auto;
  }

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

  .module-grid,
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

  .feature-section__head,
  .permission-more-card,
  .module-heading {
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

  .module-card {
    min-height: auto;
    padding: 24px;
    grid-column: auto;
  }
}
</style>
