import type { LooseDict } from "./types";

export type EventBuildingCard = {
  code: string;
  label: string;
  total: number;
  processing: number;
  underRepair: number;
  i2OrHigher: number;
  high: number;
  tone: string;
  statusLabel: string;
  allowed: boolean;
};

export const EVENT_BUILDING_SCOPE_CODES = ["110", "A", "B", "C", "D", "E", "H"];
export const EVENT_BUILDING_ORDER = ["110", "A", "B", "C", "D", "E", "H", "CAMPUS", "ALL"];
export const EVENT_I2_OR_HIGHER_LEVELS = new Set(["I2", "I1", "I3→I2（升级）", "I3→I1（升级）"]);

export function isTechnicalEventDisplayField(key: string): boolean {
  const text = String(key || "").trim();
  if (!text) return true;
  return /(record_id|active_item_id|source_record_id|target_record_id|app_token|table_id|file_token|open_id|openid|session_id|snapshot_id|payload_json)$/i.test(text);
}

export function normalizeEventScope(value: string): string {
  const text = String(value || "").trim().toUpperCase();
  if (text === "CAMPUS" || text === "ALL" || text === "110") return text;
  const match = text.match(/[ABCDEH]/);
  return match ? match[0] : text;
}

export function eventScopeText(value: string): string {
  if (value === "110") return "110站";
  if (value === "CAMPUS") return "园区";
  if (value === "ALL") return "全部";
  return value ? `${value}楼` : "未选择楼栋";
}

export function uniqueEventOptions(values: unknown[]): string[] {
  return Array.from(new Set(values.map((value) => String(value || "").trim()).filter(Boolean))).sort();
}

export function formatEventEpoch(value: unknown): string {
  const seconds = Number(value || 0);
  if (!seconds) return "";
  const date = new Date(seconds * 1000);
  if (Number.isNaN(date.getTime())) return "";
  return date.toLocaleString("zh-CN", { hour12: false });
}

export function eventStatusTone(status: unknown): string {
  const text = String(status || "");
  if (text.includes("结束")) return "ended";
  if (text.includes("恢复") || text.includes("挂起")) return "recovered";
  return "processing";
}

export function eventBuildingSortIndex(code: string): number {
  const index = EVENT_BUILDING_ORDER.indexOf(code);
  return index < 0 ? 99 : index;
}

export function buildEventBuildingCardFromStats(
  item: LooseDict,
  allowedBuildingCodes: Set<string>,
): EventBuildingCard {
  const code = normalizeEventScope(String(item.code || item.scope || ""));
  const total = Number(item.total || 0);
  const processing = Number(item.processing || 0);
  const underRepair = Number(item.under_repair || 0);
  const i2OrHigher = Number(item.i2_or_higher || 0);
  const i3 = Number(item.i3 || 0);
  const high = Number(item.high_level || item.high || 0);
  const tone = i3 ? "critical" : i2OrHigher ? "warning" : underRepair ? "active" : "stable";
  const statusLabel = i3
    ? `I3 ${i3}`
    : i2OrHigher
      ? `I2级以上 ${i2OrHigher}`
      : underRepair
        ? `检修中 ${underRepair}`
        : "运行平稳";
  return {
    code,
    label: String(item.label || eventScopeText(code)),
    total,
    processing,
    underRepair,
    i2OrHigher,
    high,
    tone,
    statusLabel,
    allowed: allowedBuildingCodes.has(code),
  };
}

export function eventLevelTone(level: unknown): string {
  const text = String(level || "").toUpperCase();
  if (/(I1|一级|紧急|严重|高)/.test(text)) return "critical";
  if (/(I2|I3|二级|三级|中)/.test(text)) return "warning";
  return "normal";
}

export function isHighLevelEvent(item: LooseDict): boolean {
  return Boolean(item.high_level) || eventLevelTone(item.level) === "critical";
}

export function eventPriorityScore(item: LooseDict): number {
  let score = 0;
  if (isHighLevelEvent(item)) score += 100;
  if (eventStatusTone(item.status) === "recovered") score += 60;
  if (eventStatusTone(item.status) === "processing") score += 20;
  return score;
}

export function eventBuildingCodesForItem(item: LooseDict): string[] {
  const raw = item.building_codes;
  const codes = Array.isArray(raw)
    ? raw.map((value) => normalizeEventScope(String(value || ""))).filter(Boolean)
    : [];
  if (codes.length) return Array.from(new Set(codes));
  const fallback = normalizeEventScope(String(item.building || ""));
  return fallback ? [fallback] : [];
}

export function eventMatchesBuilding(item: LooseDict, code: string, fallbackScope = ""): boolean {
  const normalized = normalizeEventScope(code);
  if (!normalized) return true;
  const codes = eventBuildingCodesForItem(item);
  return codes.includes(normalized) || (!codes.length && normalized === normalizeEventScope(fallbackScope));
}

export function eventKey(item: LooseDict | undefined): string {
  if (!item) return "";
  return String(item.source_record_id || item.record_id || `${item.title}-${item.occurrence_time}`);
}

export function eventRecordId(item: LooseDict | null | undefined): string {
  if (!item) return "";
  return String(item.source_record_id || item.record_id || "").trim();
}

export function eventTransferEnabled(item: LooseDict | null | undefined): boolean {
  const text = String(item?.transfer_to_overhaul ?? "").trim().toLowerCase();
  return ["true", "1", "是", "已转", "已转检修", "yes", "y"].includes(text);
}

export function eventUnderRepair(item: LooseDict | null | undefined): boolean {
  if (!item) return false;
  if ("under_repair" in item) return Boolean(item.under_repair);
  return eventTransferEnabled(item) && !String(item.repair_completion_time || "").trim();
}

export function eventI2OrHigher(item: LooseDict | null | undefined): boolean {
  if (!item) return false;
  if ("i2_or_higher" in item) return Boolean(item.i2_or_higher);
  return EVENT_I2_OR_HIGHER_LEVELS.has(String(item.level || "").replace(/\s+/g, "").toUpperCase());
}

export function eventI3Level(item: LooseDict | null | undefined): boolean {
  if (!item) return false;
  if ("i3_level" in item) return Boolean(item.i3_level);
  return String(item.level || "").replace(/\s+/g, "").toUpperCase() === "I3";
}

export function eventRepairFlowLabel(item: LooseDict | null | undefined): string {
  return eventTransferEnabled(item) ? "已转检修" : "未转检修";
}

export function eventRepairFlowHint(item: LooseDict | null | undefined): string {
  if (eventTransferEnabled(item)) {
    return "下一步：填写维修单，再从检修通告页选择记录发起通告。";
  }
  return "如需转检修，先标记转检修，再填写维修单。";
}
