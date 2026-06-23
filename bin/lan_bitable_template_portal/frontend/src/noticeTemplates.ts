export type NoticeTemplate = {
  heading: string;
  titleLabel: string;
  messageFields: string[];
  uploadFields: string[];
};

import type { WorkTypeFilterValue, WorkTypeOption, WorkTypeValue } from "./types";

export const workTypes: WorkTypeOption[] = [
  { value: "maintenance", label: "维保" },
  { value: "change", label: "变更" },
  { value: "repair", label: "检修" },
  { value: "power", label: "上电" },
  { value: "polling", label: "轮巡" },
  { value: "adjust", label: "调整" },
];

export function normalizeWorkTypeFilter(value: string, fallback: WorkTypeFilterValue = ""): WorkTypeFilterValue {
  if (!String(value || "").trim()) return "";
  return isKnownWorkType(value) ? value : fallback;
}

export const manualPrefillWorkTypes = new Set<WorkTypeValue>(["repair", "power", "polling", "adjust"]);

export const noticeTemplates: Record<WorkTypeValue, NoticeTemplate> = {
  maintenance: {
    heading: "维保通告",
    titleLabel: "名称",
    messageFields: ["title", "start_time", "end_time", "location", "content", "reason", "impact", "progress"],
    uploadFields: ["specialty", "maintenance_cycle", "non_plan"],
  },
  change: {
    heading: "变更通告",
    titleLabel: "名称",
    messageFields: ["title", "level", "start_time", "end_time", "location", "content", "reason", "impact", "progress"],
    uploadFields: ["specialty", "zhihang"],
  },
  repair: {
    heading: "设备检修",
    titleLabel: "标题",
    messageFields: [
      "title",
      "location",
      "level",
      "specialty",
      "end_time",
      "start_time",
      "repair_device",
      "repair_fault",
      "fault_type",
      "repair_mode",
      "impact",
      "discovery",
      "symptom",
      "reason",
      "solution",
      "spare_parts",
      "progress",
    ],
    uploadFields: [],
  },
  power: {
    heading: "上电通告",
    titleLabel: "名称",
    messageFields: ["title", "start_time", "end_time", "cabinet", "quantity", "progress"],
    uploadFields: ["specialty"],
  },
  polling: {
    heading: "设备轮巡",
    titleLabel: "标题",
    messageFields: ["title", "start_time", "end_time", "device", "content", "impact", "progress"],
    uploadFields: ["specialty"],
  },
  adjust: {
    heading: "设备调整",
    titleLabel: "名称",
    messageFields: ["title", "start_time", "end_time", "location", "content", "reason", "impact", "progress"],
    uploadFields: ["specialty"],
  },
};

export const noticeTypeKeywordRules: Record<string, RegExp[]> = {
  maintenance: [/维保|维护/],
  change: [/变更/],
  repair: [/检修|维修/],
  power: [/上电|下电|上下电/],
  polling: [/轮巡/],
  adjust: [/调整/],
};

const commonLabels: Record<string, string> = {
  building: "楼栋/范围",
  maintenance_cycle: "维保周期",
  cabinet: "柜号",
  quantity: "数量",
  device: "设备",
  repair_device: "维修设备",
  repair_fault: "维修故障",
  fault_type: "故障类型",
  repair_mode: "维修方式",
  solution: "解决方案",
  discovery: "故障发现方式",
  symptom: "故障现象",
  zhihang_record_id: "智航变更记录",
};

const noticeFieldLabels: Record<string, Record<string, string>> = {
  maintenance: {
    title: "名称",
    specialty: "专业",
    start_time: "计划开始时间",
    end_time: "计划结束时间",
    location: "位置",
    content: "内容",
    reason: "原因",
    impact: "影响",
    progress: "进度",
  },
  change: {
    title: "名称",
    specialty: "专业",
    level: "变更等级",
    start_time: "计划开始时间",
    end_time: "计划结束时间",
    location: "位置",
    content: "内容",
    reason: "原因",
    impact: "影响",
    progress: "进度",
  },
  repair: {
    title: "标题",
    specialty: "专业",
    level: "紧急程度",
    start_time: "期望完成时间",
    end_time: "发现故障时间",
    location: "地点",
    content: "标题补充内容",
    reason: "故障原因",
    impact: "影响范围",
    progress: "完成情况",
    spare_parts: "备件更换情况",
  },
  power: {
    title: "名称",
    specialty: "专业",
    start_time: "计划开始时间",
    end_time: "计划结束时间",
    location: "位置",
    content: "内容",
    reason: "原因",
    impact: "影响",
    progress: "进度",
  },
  polling: {
    title: "标题",
    specialty: "专业",
    start_time: "计划开始时间",
    end_time: "计划结束时间",
    location: "位置",
    content: "内容",
    reason: "原因",
    impact: "影响",
    progress: "进度",
  },
  adjust: {
    title: "名称",
    specialty: "专业",
    start_time: "计划开始时间",
    end_time: "计划结束时间",
    location: "位置",
    content: "内容",
    reason: "原因",
    impact: "影响",
    progress: "进度",
  },
};

export function isKnownWorkType(value: string): value is WorkTypeValue {
  return workTypes.some((item) => item.value === value);
}

export function normalizeWorkType(value: string, fallback = "maintenance"): WorkTypeValue {
  return isKnownWorkType(value) ? value : isKnownWorkType(fallback) ? fallback : "maintenance";
}

export function workTypeLabel(value: string): string {
  return workTypes.find((item) => item.value === value)?.label || "维保";
}

export function noticeTemplate(type: string): NoticeTemplate {
  return noticeTemplates[normalizeWorkType(type || "maintenance")];
}

export function isNoticeMessageField(type: string, field: string): boolean {
  return noticeTemplate(type).messageFields.includes(field);
}

export function isNoticeUploadField(type: string, field: string): boolean {
  return noticeTemplate(type).uploadFields.includes(field);
}

export function hasNoticeUploadFields(type: string): boolean {
  return noticeTemplate(type).uploadFields.length > 0;
}

export function noticeFieldLabel(type: string, field: string): string {
  const workType = type || "maintenance";
  return noticeFieldLabels[workType]?.[field] || commonLabels[field] || noticeFieldLabels.maintenance[field] || field;
}

function parseNoticeDateTime(value: unknown): Date | null {
  const text = String(value || "").trim();
  if (!text) return null;
  const normalized = text.replace("T", " ").replace(/\//g, "-");
  const iso = normalized.match(/^(\d{4})-(\d{1,2})-(\d{1,2})\s+(\d{1,2}):(\d{1,2})/);
  if (iso) {
    const date = new Date(
      Number(iso[1]),
      Number(iso[2]) - 1,
      Number(iso[3]),
      Number(iso[4]),
      Number(iso[5]),
      0,
      0,
    );
    return Number.isNaN(date.getTime()) ? null : date;
  }
  const cn = normalized.match(
    /(\d{4})[年-](\d{1,2})[月-](\d{1,2})日?\s*(\d{1,2})(?:[：:点时.](\d{1,2}))?/
  );
  if (cn) {
    const date = new Date(
      Number(cn[1]),
      Number(cn[2]) - 1,
      Number(cn[3]),
      Number(cn[4]),
      Number(cn[5] || "0"),
      0,
      0,
    );
    return Number.isNaN(date.getTime()) ? null : date;
  }
  return null;
}

export function noticeDurationError(workType: string, startValue: unknown, endValue: unknown): string {
  const start = parseNoticeDateTime(startValue);
  const end = parseNoticeDateTime(endValue);
  if (!start || !end) return "";
  const durationMs = end.getTime() - start.getTime();
  if (durationMs >= 60 * 60 * 1000) return "";
  if (workType === "repair") return "发现故障时间和期望完成时间之间不能少于1小时。";
  return "开始时间和结束时间之间不能少于1小时。";
}
