<template>
  <section class="home-shell">
    <div v-if="!activeMode" class="home-metrics">
      <article>
        <span class="metric-icon grid" aria-hidden="true"></span>
        <div>
          <strong>业务模块</strong>
          <p>统一业务导航</p>
        </div>
        <b>4 个模块</b>
      </article>
      <article>
        <span class="metric-icon work" aria-hidden="true"></span>
        <div>
          <strong>工作台</strong>
          <p>通告集中处理</p>
        </div>
        <b>统一待办</b>
      </article>
      <article>
        <span class="metric-icon link" aria-hidden="true"></span>
        <div>
          <strong>审核中心</strong>
          <p>交接班审核页</p>
        </div>
        <b>{{ configuredHandoverCount ? "已配置" : "待配置" }}</b>
      </article>
      <article>
        <span class="metric-icon user" aria-hidden="true"></span>
        <div>
          <strong>访问权限</strong>
          <p>按角色展示</p>
        </div>
        <b>{{ scopeOptions.length }} 个入口</b>
      </article>
    </div>

    <div v-if="canRequestMoreScopes" class="permission-more-card">
      <div>
        <span class="section-kicker">权限申请</span>
        <strong>需要访问其他楼栋？</strong>
        <p>提交申请后，管理员会收到验证码，确认后即可追加楼栋权限。</p>
      </div>
      <button class="secondary" @click="$emit('request-permission')">申请其他楼权限</button>
    </div>

    <template v-if="!activeMode">
      <div class="module-heading">
        <div>
          <h2>业务工作台</h2>
          <p>按维护、变更、检修和辅助工具进入对应工作模块</p>
        </div>
        <span>共 4 个业务模块</span>
      </div>

      <div class="module-grid">
        <article
          v-for="module in moduleCards"
          :key="module.key"
          class="module-card"
          :class="module.tone"
          role="button"
          tabindex="0"
          @click="selectPrimaryModuleAction(module)"
          @keydown.enter.prevent="selectPrimaryModuleAction(module)"
          @keydown.space.prevent="selectPrimaryModuleAction(module)"
        >
          <div class="module-card__head">
            <span class="module-icon" :class="module.icon" aria-hidden="true"></span>
            <span class="module-badge">{{ module.badge }}</span>
          </div>
          <div class="module-card__body">
            <strong>{{ module.title }}</strong>
            <p>{{ module.description }}</p>
            <div class="module-tags">
              <span v-for="tag in module.tags" :key="tag">{{ tag }}</span>
            </div>
          </div>
          <div class="module-actions">
            <button
              v-for="action in module.actions"
              :key="action.key"
              :class="action.primary ? 'primary' : 'secondary'"
              @click.stop="selectEntry(action.key)"
            >
              {{ action.label }}
              <span aria-hidden="true">›</span>
            </button>
          </div>
        </article>
      </div>
    </template>

    <section v-else class="feature-section">
      <header class="feature-section__head">
        <div>
          <span class="section-kicker">{{ activeConfig.kicker }}</span>
          <h2>{{ activeConfig.title }}</h2>
          <p>{{ activeConfig.description }}</p>
        </div>
        <div class="feature-section__actions">
          <button v-if="isToolScopeMode" class="secondary" @click="activeMode = 'tools'">返回其他工具</button>
          <button class="secondary" @click="activeMode = ''">返回功能选择</button>
        </div>
      </header>

      <div v-if="activeMode === 'tools'" class="tool-grid">
        <button
          v-for="tool in toolEntries"
          :key="tool.key"
          class="tool-card"
          :class="tool.tone"
          type="button"
          @click="selectEntry(tool.key)"
        >
          <span class="tool-icon" :class="tool.icon" aria-hidden="true"></span>
          <span>
            <strong>{{ tool.title }}</strong>
            <small>{{ tool.description }}</small>
          </span>
          <b>{{ tool.badge }}</b>
        </button>
      </div>

      <div v-else class="scope-grid">
        <article
          v-for="scope in scopeOptions"
          :key="scope.value"
          class="scope-card"
        >
          <div>
            <strong>{{ scope.label }}</strong>
            <span>{{ scopeMetricText(scope.value) }}</span>
          </div>
          <div class="scope-actions">
            <button
              v-if="activeMode === 'maintenance_mop'"
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
            <button
              v-else-if="activeMode === 'handover'"
              class="secondary"
              disabled
              title="该楼栋暂未配置交接班审核页链接"
            >
              未配置
            </button>
            <button
              v-else
              class="primary"
              @click="enterWorkbench(scope.value)"
            >
              {{ activeConfig.actionLabel }}
            </button>
          </div>
        </article>
      </div>
    </section>
  </section>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";

type Dict = Record<string, any>;
type EntryKey = "" | "maintenance" | "maintenance_mop" | "change" | "repair" | "tools" | "power" | "polling" | "adjust" | "handover";

const props = defineProps<{
  scopeOptions: Array<{ value: string; label: string }>;
  overview: Record<string, Dict>;
  handoverLinks: Record<string, string>;
  canRequestMoreScopes?: boolean;
}>();

const emit = defineEmits<{
  enter: [scope: string, workType?: string];
  engineer: [scope: string];
  "request-permission": [];
}>();

const activeMode = ref<EntryKey>("");

const moduleCards: Array<{
  key: string;
  tone: string;
  icon: string;
  badge: string;
  title: string;
  description: string;
  tags: string[];
  actions: Array<{ key: EntryKey; label: string; primary?: boolean }>;
}> = [
  {
    key: "maintenance",
    tone: "blue",
    icon: "wrench",
    badge: "核心模块",
    title: "维护管理",
    description: "统一管理维保计划、MOP 执行、签名与维护单归档",
    tags: ["维保计划", "MOP 执行", "工单归档"],
    actions: [
      { key: "maintenance", label: "进入维护管理", primary: true },
      { key: "maintenance_mop", label: "进入维护单管理" },
    ],
  },
  {
    key: "change",
    tone: "violet",
    icon: "switch",
    badge: "流程审批",
    title: "变更管理",
    description: "进入变更通告，处理风险评估、实施更新与回退确认",
    tags: ["变更申请", "风险评估", "回退确认"],
    actions: [{ key: "change", label: "进入变更管理", primary: true }],
  },
  {
    key: "repair",
    tone: "cyan",
    icon: "repair",
    badge: "故障处理",
    title: "检修管理",
    description: "进入检修通告，处理故障发现、解决方案和完成情况",
    tags: ["故障检修", "过程跟踪", "闭环确认"],
    actions: [{ key: "repair", label: "进入检修管理", primary: true }],
  },
  {
    key: "tools",
    tone: "slate",
    icon: "more",
    badge: "辅助入口",
    title: "其他工具",
    description: "汇总上电、轮巡、调整、交接班审核页等辅助入口",
    tags: ["上电", "轮巡", "调整", "交接班"],
    actions: [
      { key: "power", label: "上电", primary: true },
      { key: "polling", label: "轮巡" },
      { key: "adjust", label: "调整" },
      { key: "handover", label: "交接班" },
    ],
  },
];

const entryConfigs: Record<Exclude<EntryKey, "">, {
  kicker: string;
  title: string;
  description: string;
  actionLabel: string;
  workType?: string;
}> = {
  maintenance: {
    kicker: "维护管理",
    title: "选择楼栋进入维护管理",
    description: "进入后自动选中维保通告。",
    actionLabel: "进入维护管理",
    workType: "maintenance",
  },
  maintenance_mop: {
    kicker: "维护单管理",
    title: "选择楼栋进入 MOP 填写",
    description: "进入后选择维保通告、绑定 MOP 表格、写入签名并上传维护单。",
    actionLabel: "进入维护单管理",
  },
  change: {
    kicker: "变更管理",
    title: "选择楼栋进入变更管理",
    description: "进入后自动选中变更通告。",
    actionLabel: "进入变更管理",
    workType: "change",
  },
  repair: {
    kicker: "检修管理",
    title: "选择楼栋进入检修管理",
    description: "进入后自动选中检修通告。",
    actionLabel: "进入检修管理",
    workType: "repair",
  },
  tools: {
    kicker: "其他工具",
    title: "选择辅助工具",
    description: "选择上电、轮巡、调整或交接班审核页。",
    actionLabel: "选择工具",
  },
  power: {
    kicker: "其他工具",
    title: "选择楼栋进入上电通告",
    description: "进入后自动选中上电通告。",
    actionLabel: "进入上电通告",
    workType: "power",
  },
  polling: {
    kicker: "其他工具",
    title: "选择楼栋进入设备轮巡",
    description: "进入后自动选中设备轮巡。",
    actionLabel: "进入设备轮巡",
    workType: "polling",
  },
  adjust: {
    kicker: "其他工具",
    title: "选择楼栋进入设备调整",
    description: "进入后自动选中设备调整。",
    actionLabel: "进入设备调整",
    workType: "adjust",
  },
  handover: {
    kicker: "外部链接",
    title: "选择楼栋打开交接班审核页",
    description: "按楼栋打开已配置的交接班审核页面。",
    actionLabel: "打开审核页",
  },
};

const toolEntries: Array<{
  key: EntryKey;
  title: string;
  description: string;
  badge: string;
  icon: string;
  tone: string;
}> = [
  { key: "power", title: "上电通告", description: "机柜上电、数量和进度确认", badge: "通告", icon: "power", tone: "blue" },
  { key: "polling", title: "设备轮巡", description: "设备轮巡切换和影响确认", badge: "通告", icon: "polling", tone: "cyan" },
  { key: "adjust", title: "设备调整", description: "设备运行模式调整与现场进度", badge: "通告", icon: "adjust", tone: "emerald" },
  { key: "handover", title: "交接班审核页", description: "按楼栋跳转审核链接", badge: "链接", icon: "link", tone: "slate" },
];

const configuredHandoverCount = computed(() => {
  return Object.values(props.handoverLinks || {}).filter((value) => String(value || "").trim()).length;
});

const activeConfig = computed(() => entryConfigs[activeMode.value || "tools"]);
const isToolScopeMode = computed(() => ["power", "polling", "adjust", "handover"].includes(activeMode.value));

function selectEntry(key: EntryKey): void {
  activeMode.value = key;
}

function selectPrimaryModuleAction(module: { actions: Array<{ key: EntryKey; primary?: boolean }> }): void {
  const action = module.actions.find((item) => item.primary) || module.actions[0];
  if (action) selectEntry(action.key);
}

function normalizeScopeValue(value: string, fallback = "ALL"): string {
  const text = String(value || "").trim().toUpperCase();
  if (!text) return fallback;
  if (["ALL", "CAMPUS", "110"].includes(text)) return text;
  const match = text.match(/[ABCDEH]/);
  return match ? match[0] : fallback;
}

function enterWorkbench(scope: string): void {
  const workType = activeConfig.value.workType || "maintenance";
  emit("enter", scope, workType);
}

function countText(scope: string, workType: string): string {
  const item = props.overview[normalizeScopeValue(scope, "ALL")] || {};
  const pending = Number(item[`${workType}_pending`] || 0);
  const ongoing = Number(item[`${workType}_ongoing`] || 0);
  return `待发起 ${pending} / 进行中 ${ongoing}`;
}

function scopeMetricText(scope: string): string {
  if (activeMode.value === "maintenance") return countText(scope, "maintenance");
  if (activeMode.value === "change") return countText(scope, "change");
  if (activeMode.value === "repair") return countText(scope, "repair");
  if (activeMode.value === "maintenance_mop") return "MOP 填写 / 签名 / 上传";
  if (activeMode.value === "handover") return props.handoverLinks[scope] ? "审核页已配置" : "暂未配置审核页链接";
  if (["power", "polling", "adjust"].includes(activeMode.value)) return "进入后显示该类型通告";
  return "";
}
</script>

<style scoped>
.home-shell {
  padding: 34px 38px 42px;
  display: grid;
  gap: 24px;
}

.home-metrics,
.module-grid,
.scope-grid,
.tool-grid {
  display: grid;
  gap: 22px;
}

.home-metrics {
  grid-template-columns: repeat(4, minmax(0, 1fr));
}

.home-metrics article,
.permission-more-card,
.module-card,
.feature-section,
.scope-card,
.tool-card {
  border: 1px solid #d8e5f7;
  background: rgba(255, 255, 255, 0.92);
  box-shadow: 0 18px 42px rgba(15, 73, 153, 0.12);
}

.home-metrics article {
  position: relative;
  overflow: hidden;
  min-height: 112px;
  display: grid;
  grid-template-columns: 58px minmax(0, 1fr) auto;
  align-items: center;
  gap: 16px;
  padding: 20px 22px;
  border-radius: 16px;
}

.home-metrics article::after,
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

.home-metrics strong,
.module-card strong,
.scope-card strong,
.tool-card strong {
  color: #071a39;
  font-weight: 900;
}

.home-metrics p,
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

.home-metrics p {
  margin-top: 4px;
  font-size: 13px;
}

.home-metrics b {
  position: relative;
  z-index: 1;
  color: #075bd8;
  font-size: 16px;
  white-space: nowrap;
}

.metric-icon,
.module-icon,
.tool-icon {
  display: inline-grid;
  place-items: center;
  border-radius: 14px;
  color: #fff;
  box-shadow: 0 14px 24px rgba(21, 92, 214, 0.22);
}

.metric-icon {
  width: 58px;
  height: 58px;
  background: linear-gradient(135deg, #2a77ff, #004fc4);
}

.metric-icon.work {
  background: linear-gradient(135deg, #20c4d2, #0b8fd0);
}

.metric-icon.link {
  background: linear-gradient(135deg, #4e9cff, #1d5bd7);
}

.metric-icon.user {
  background: linear-gradient(135deg, #2ecb87, #0a9a5b);
}

.metric-icon::before,
.module-icon::before,
.tool-icon::before {
  content: "";
  width: 24px;
  height: 24px;
  border: 3px solid currentColor;
  border-radius: 7px;
}

.metric-icon.grid::before {
  box-shadow: inset 10px 0 0 transparent;
  background:
    linear-gradient(currentColor, currentColor) 50% 0 / 3px 100% no-repeat,
    linear-gradient(currentColor, currentColor) 0 50% / 100% 3px no-repeat;
}

.metric-icon.work::before,
.module-icon.wrench::before {
  width: 25px;
  height: 16px;
  border-radius: 5px;
}

.metric-icon.link::before,
.tool-icon.link::before {
  width: 28px;
  height: 14px;
  border-radius: 999px;
  transform: rotate(-35deg);
}

.metric-icon.user::before {
  width: 22px;
  height: 22px;
  border-radius: 50% 50% 45% 45%;
}

.permission-more-card {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 18px;
  padding: 20px 22px;
  border-radius: 18px;
}

.permission-more-card strong {
  display: block;
  margin-top: 6px;
  color: #071a39;
  font-size: 19px;
  font-weight: 900;
}

.module-heading {
  display: flex;
  align-items: end;
  justify-content: space-between;
  gap: 18px;
}

.module-heading h2 {
  margin: 0;
  color: #071a39;
  font-size: 22px;
  font-weight: 950;
}

.module-heading span {
  align-self: center;
  padding: 9px 18px;
  border: 1px solid #d8e5f7;
  border-radius: 999px;
  background: rgba(255, 255, 255, 0.7);
  color: #1b5bbd;
  font-size: 13px;
  font-weight: 900;
}

.module-grid {
  grid-template-columns: repeat(2, minmax(0, 1fr));
}

.module-card {
  position: relative;
  overflow: hidden;
  min-height: 260px;
  display: flex;
  flex-direction: column;
  gap: 22px;
  padding: 30px;
  border-radius: 18px;
  cursor: pointer;
  transition: transform 0.16s ease, box-shadow 0.16s ease, border-color 0.16s ease;
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
  height: 5px;
  background: #1e63ff;
}

.module-card.violet::before {
  background: #7657e6;
}

.module-card.cyan::before {
  background: #11b7ca;
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
  width: 72px;
  height: 72px;
  background: linear-gradient(135deg, #2a77ff, #004fc4);
}

.module-card.violet .module-icon {
  background: linear-gradient(135deg, #8b68ff, #6044d6);
}

.module-card.cyan .module-icon {
  background: linear-gradient(135deg, #27d1df, #0a8fb8);
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
.tool-icon.adjust::before {
  border-radius: 50%;
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
  padding: 8px 16px;
  border: 1px solid #dce8f8;
  border-radius: 999px;
  background: #f6faff;
  color: #1763d7;
  font-size: 13px;
  font-weight: 900;
}

.module-card__body {
  position: relative;
  z-index: 1;
  display: grid;
  gap: 14px;
  flex: 1;
}

.module-card__body strong {
  font-size: 28px;
  line-height: 1.15;
}

.module-tags {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  margin-top: 8px;
}

.module-tags span {
  padding: 8px 14px;
  border: 1px solid #e0e9f6;
  border-radius: 999px;
  background: #f7fbff;
  color: #4e6381;
  font-size: 13px;
  font-weight: 800;
}

.module-actions {
  position: relative;
  z-index: 1;
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  padding-top: 16px;
  border-top: 1px solid #e8eef7;
}

.feature-section {
  padding: 28px;
  border-radius: 18px;
  display: grid;
  gap: 22px;
}

.feature-section__head {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 18px;
}

.feature-section__head h2 {
  margin: 8px 0 4px;
  color: #071a39;
  font-size: 24px;
  font-weight: 950;
}

.feature-section__actions {
  display: flex;
  flex-wrap: wrap;
  justify-content: flex-end;
  gap: 10px;
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

.scope-card {
  position: relative;
  overflow: hidden;
  min-height: 148px;
  display: flex;
  flex-direction: column;
  justify-content: space-between;
  gap: 18px;
  padding: 22px;
  border-radius: 16px;
}

.scope-card strong {
  display: block;
  font-size: 21px;
}

.scope-card span {
  display: block;
  margin-top: 8px;
  font-size: 13px;
}

.scope-actions {
  position: relative;
  z-index: 1;
}

.tool-grid {
  grid-template-columns: repeat(2, minmax(0, 1fr));
}

.tool-card {
  width: 100%;
  min-height: 112px;
  display: grid;
  grid-template-columns: 56px minmax(0, 1fr) auto;
  align-items: center;
  gap: 16px;
  padding: 20px;
  border-radius: 16px;
  text-align: left;
  cursor: pointer;
}

.tool-card:hover,
.module-card:hover,
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
  min-height: 42px;
  padding: 10px 18px;
  border-radius: 12px;
  font-size: 14px;
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
  transform: translateY(-1px);
}

.primary:disabled,
.secondary:disabled {
  cursor: not-allowed;
  opacity: 0.58;
  transform: none;
  box-shadow: none;
}

@media (max-width: 1280px) {
  .home-metrics,
  .module-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .scope-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}

@media (max-width: 760px) {
  .home-shell {
    padding: 22px 18px 30px;
  }

  .home-metrics,
  .module-grid,
  .scope-grid,
  .tool-grid {
    grid-template-columns: 1fr;
  }

  .home-metrics article,
  .tool-card {
    grid-template-columns: 54px minmax(0, 1fr);
  }

  .home-metrics b,
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

  .module-card {
    min-height: auto;
    padding: 24px;
  }
}
</style>
