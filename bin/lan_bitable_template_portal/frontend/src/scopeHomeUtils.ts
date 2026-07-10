import type { Dict } from "./api/client";

export type ScopeHomeEntryKey = "" | "event" | "maintenance" | "maintenance_mop" | "change" | "repair_management" | "repair" | "tools" | "power" | "polling" | "adjust" | "handover";
export type ScopeHomeModuleAction = { key: ScopeHomeEntryKey; label: string; primary?: boolean; disabled?: boolean };
export type ScopeHomeModuleCard = {
  key: string;
  tone: string;
  icon: string;
  badge: string;
  title: string;
  description: string;
  tags: string[];
  size?: "main" | "compact";
  disabled?: boolean;
  actions: ScopeHomeModuleAction[];
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
    description: "覆盖事件发现、分级响应、处置升级与复盘归档",
    tags: ["事件上报", "处置跟踪", "复盘归档"],
    size: "main",
    actions: [{ key: "event", label: "进入事件管理", primary: true }],
  },
  {
    key: "maintenance",
    tone: "blue",
    icon: "wrench",
    badge: "核心模块",
    title: "维护管理",
    description: "统一管理维保计划、MOP 执行、签名与维护单归档",
    tags: ["维保计划", "MOP 执行", "工单归档"],
    size: "main",
    actions: [
      { key: "maintenance", label: "进入维护管理", primary: true },
      { key: "maintenance_mop", label: "维护单管理" },
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
    size: "main",
    actions: [{ key: "change", label: "进入变更管理", primary: true }],
  },
  {
    key: "repair_management",
    tone: "blue",
    icon: "repair",
    badge: "检修单",
    title: "检修管理",
    description: "统一处理检修单记录与检修通告",
    tags: ["检修单", "检修通告", "转检修"],
    actions: [
      { key: "repair_management", label: "进入检修单管理", primary: true },
      { key: "repair", label: "检修通告管理" },
    ],
  },
  {
    key: "drill",
    tone: "cyan",
    icon: "drill",
    badge: "计划管理",
    title: "演练管理",
    description: "沉淀演练计划、场景脚本和评估改进",
    tags: ["演练计划", "场景脚本", "评估改进"],
    disabled: true,
    actions: [{ key: "", label: "建设中", disabled: true }],
  },
  {
    key: "capacity",
    tone: "emerald",
    icon: "capacity",
    badge: "数据洞察",
    title: "容量管理",
    description: "管理电力、制冷、空间及端口容量",
    tags: ["容量台账", "趋势预测", "阈值预警"],
    disabled: true,
    actions: [{ key: "", label: "建设中", disabled: true }],
  },
  {
    key: "risk",
    tone: "rose",
    icon: "risk",
    badge: "闭环管理",
    title: "风险管理",
    description: "风险识别、分级管控、整改跟踪与闭环验收",
    tags: ["风险识别", "整改跟踪", "闭环验收"],
    disabled: true,
    actions: [{ key: "", label: "建设中", disabled: true }],
  },
  {
    key: "tools",
    tone: "slate",
    icon: "more",
    badge: "辅助入口",
    title: "其他工具",
    description: "汇总上/下电、轮巡、调整和交接班入口",
    tags: ["上/下电", "轮巡", "调整", "交接班"],
    actions: [
      { key: "power", label: "上/下电", primary: true },
      { key: "polling", label: "轮巡" },
      { key: "adjust", label: "调整" },
      { key: "handover", label: "交接班" },
    ],
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
  tools: {
    kicker: "其他工具",
    title: "选择辅助工具",
    description: "选择上/下电、轮巡、调整或交接班审核页。",
    actionLabel: "选择工具",
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
