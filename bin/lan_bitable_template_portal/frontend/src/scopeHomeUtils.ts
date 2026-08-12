import type { Dict } from "./api/client";

export type ScopeHomeEntryKey = "" | "event" | "maintenance" | "maintenance_mop" | "change" | "repair_management" | "repair" | "water" | "critical_guard" | "tools" | "daily" | "power" | "polling" | "adjust" | "handover";
export type ScopeHomeModuleAction = { key: ScopeHomeEntryKey; label: string; primary?: boolean; disabled?: boolean };
export type ScopeHomeModuleCard = {
  key: string;
  tone: string;
  icon: string;
  badge: string;
  title: string;
  tags: string[];
  disabled?: boolean;
  primaryAction: ScopeHomeModuleAction;
  secondaryActions?: ScopeHomeModuleAction[];
};
export type ScopeHomeBroadcastItem = {
  key: string;
  label: string;
  text: string;
  tone: "ongoing" | "pending" | "event" | "quiet";
  scope?: string;
  workType?: string;
  action?: "workbench" | "event";
};
export type ScopeHomeEntryConfig = {
  kicker: string;
  title: string;
  description: string;
  actionLabel: string;
  workType?: string;
};
export type ScopeHomeToolEntry = {
  key: ScopeHomeEntryKey;
  title: string;
  description: string;
  badge: string;
  icon: string;
  tone: string;
};

export const SCOPE_HOME_DISPLAY_ORDER = ["110", "A", "B", "C", "D", "E", "H", "CAMPUS", "ALL"];

export const SCOPE_HOME_MODULE_CARDS: ScopeHomeModuleCard[] = [
  {
    key: "event",
    tone: "orange",
    icon: "event",
    badge: "全流程",
    title: "事件管理",
    tags: ["事件处置", "复盘归档"],
    primaryAction: { key: "event", label: "进入事件管理", primary: true },
  },
  {
    key: "maintenance",
    tone: "blue",
    icon: "wrench",
    badge: "核心模块",
    title: "维护管理",
    tags: ["维保计划", "MOP 执行"],
    primaryAction: { key: "maintenance", label: "进入维护管理", primary: true },
    secondaryActions: [
      { key: "maintenance_mop", label: "维护单管理" },
    ],
  },
  {
    key: "change",
    tone: "violet",
    icon: "switch",
    badge: "流程审批",
    title: "变更管理",
    tags: ["风险评估", "回退确认"],
    primaryAction: { key: "change", label: "进入变更管理", primary: true },
  },
  {
    key: "repair_management",
    tone: "blue",
    icon: "repair",
    badge: "检修单",
    title: "检修管理",
    tags: ["检修单", "检修通告"],
    primaryAction: { key: "repair_management", label: "进入检修单管理", primary: true },
    secondaryActions: [
      { key: "repair", label: "检修通告管理" },
    ],
  },
  {
    key: "risk",
    tone: "rose",
    icon: "risk",
    badge: "闭环管理",
    title: "风险管理",
    tags: ["重保任务", "结果汇总"],
    primaryAction: { key: "critical_guard", label: "进入重保管理", primary: true },
  },
  {
    key: "capacity",
    tone: "emerald",
    icon: "capacity",
    badge: "数据洞察",
    title: "容量管理",
    tags: ["水耗台账", "趋势统计"],
    primaryAction: { key: "water", label: "进入水耗管理", primary: true },
  },
  {
    key: "tools",
    tone: "slate",
    icon: "more",
    badge: "辅助入口",
    title: "其他工具",
    tags: ["每日任务", "辅助通告"],
    primaryAction: { key: "tools", label: "进入其他工具", primary: true },
    secondaryActions: [
      { key: "daily", label: "每日任务" },
    ],
  },
  {
    key: "drill",
    tone: "cyan",
    icon: "drill",
    badge: "计划管理",
    title: "演练管理",
    tags: ["演练计划", "评估改进"],
    disabled: true,
    primaryAction: { key: "", label: "建设中", disabled: true },
  },
];

export const SCOPE_HOME_ENTRY_CONFIGS: Record<Exclude<ScopeHomeEntryKey, "">, ScopeHomeEntryConfig> = {
  event: {
    kicker: "事件管理",
    title: "选择楼栋进入事件管理",
    description: "查看本月事件、筛选状态等级并打开完整详情。",
    actionLabel: "进入事件管理",
  },
  maintenance: {
    kicker: "维护管理",
    title: "选择楼栋进入维护管理",
    description: "",
    actionLabel: "进入维护管理",
    workType: "maintenance",
  },
  maintenance_mop: {
    kicker: "维护单管理",
    title: "选择楼栋进入 MOP 填写",
    description: "",
    actionLabel: "进入维护单管理",
  },
  change: {
    kicker: "变更管理",
    title: "选择楼栋进入变更管理",
    description: "",
    actionLabel: "进入变更管理",
    workType: "change",
  },
  repair: {
    kicker: "检修管理",
    title: "选择楼栋进入检修通告管理",
    description: "",
    actionLabel: "进入检修通告管理",
    workType: "repair",
  },
  repair_management: {
    kicker: "检修单管理",
    title: "选择楼栋进入检修单管理",
    description: "",
    actionLabel: "进入检修单管理",
  },
  water: {
    kicker: "容量管理",
    title: "选择楼栋进入水耗管理",
    description: "",
    actionLabel: "进入水耗管理",
  },
  critical_guard: {
    kicker: "风险管理",
    title: "重保管理",
    description: "",
    actionLabel: "进入重保管理",
  },
  tools: {
    kicker: "其他工具",
    title: "选择辅助工具",
    description: "",
    actionLabel: "选择工具",
  },
  daily: {
    kicker: "每日任务清单",
    title: "选择楼栋查看每日任务",
    description: "",
    actionLabel: "查看每日任务",
  },
  power: {
    kicker: "其他工具",
    title: "选择楼栋进入上/下电通告",
    description: "",
    actionLabel: "进入上/下电通告",
    workType: "power",
  },
  polling: {
    kicker: "其他工具",
    title: "选择楼栋进入设备轮巡",
    description: "",
    actionLabel: "进入设备轮巡",
    workType: "polling",
  },
  adjust: {
    kicker: "其他工具",
    title: "选择楼栋进入设备调整",
    description: "",
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

export const SCOPE_HOME_TOOL_ENTRIES: ScopeHomeToolEntry[] = [
  { key: "daily", title: "每日任务清单", description: "汇总当天通告、事件、检修和维护记录", badge: "今日", icon: "daily", tone: "blue" },
  { key: "power", title: "上/下电通告", description: "机柜上电、下电、数量和进度确认", badge: "通告", icon: "power", tone: "blue" },
  { key: "polling", title: "设备轮巡", description: "设备轮巡切换和影响确认", badge: "通告", icon: "polling", tone: "cyan" },
  { key: "adjust", title: "设备调整", description: "设备运行模式调整与现场进度", badge: "通告", icon: "adjust", tone: "emerald" },
  { key: "handover", title: "交接班审核页", description: "按楼栋跳转审核链接", badge: "链接", icon: "link", tone: "slate" },
];

export function normalizeScopeValue(value: string, fallback = "ALL"): string {
  const text = String(value || "").trim().toUpperCase();
  if (!text) return fallback;
  if (["ALL", "CAMPUS", "110"].includes(text)) return text;
  const match = text.match(/[ABCDEH]/);
  return match ? match[0] : fallback;
}

export function scopeSortIndex(value: string): number {
  const index = SCOPE_HOME_DISPLAY_ORDER.indexOf(normalizeScopeValue(value, ""));
  return index >= 0 ? index : 99;
}

export function scopeDisplayLabel(scope: { value: string; label: string }): string {
  const code = normalizeScopeValue(scope.value, "");
  if (code === "ALL") return "全部";
  if (code === "CAMPUS") return "园区";
  if (code === "110") return "110站";
  return scope.label || (code ? `${code}楼` : "未命名");
}

export function scopeCardClass(value: string): string {
  const code = normalizeScopeValue(value, "");
  if (code === "ALL") return "scope-all";
  if (code === "CAMPUS") return "scope-campus";
  return "";
}

export function scopeIconClass(value: string): string {
  const code = normalizeScopeValue(value, "");
  if (code === "ALL") return "all";
  if (code === "CAMPUS") return "campus";
  return "building";
}

export function typedScopeCounts(overview: Record<string, Dict>, scope: string, workType: string): { pending: number; ongoing: number } {
  const item = overview[normalizeScopeValue(scope, "ALL")] || {};
  return {
    pending: Number(item[`${workType}_pending`] || 0),
    ongoing: Number(item[`${workType}_ongoing`] || 0),
  };
}
