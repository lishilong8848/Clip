import type { LooseDict, ScopeOption } from "./types";

export const REPAIR_REQUIRED_FIELD_GROUPS = [
  ["检修通告名称", "维修名称", "标题", "名称"],
  ["地点"],
  ["紧急程度"],
  ["专业"],
  ["发现故障时间"],
  ["期望完成时间"],
  ["维修设备"],
  ["维修故障"],
  ["故障类型"],
  ["维修方式"],
  ["影响范围"],
  ["故障发现方式"],
  ["故障现象"],
  ["故障原因"],
  ["解决方案"],
  ["完成情况"],
];

export function isRequiredRepairField(fieldName: unknown): boolean {
  const name = String(fieldName || "");
  return REPAIR_REQUIRED_FIELD_GROUPS.some((group) => group.includes(name));
}

export function sortedRepairFields(source: LooseDict[]): LooseDict[] {
  const ordered = source.slice();
  ordered.sort((left, right) => {
    const leftRequired = isRequiredRepairField(left.field_name);
    const rightRequired = isRequiredRepairField(right.field_name);
    if (leftRequired && !rightRequired) return -1;
    if (!leftRequired && rightRequired) return 1;
    if (left.is_primary && !right.is_primary) return -1;
    if (!left.is_primary && right.is_primary) return 1;
    return String(left.field_name || "").localeCompare(String(right.field_name || ""), "zh-Hans-CN");
  });
  return ordered;
}

export function repairFieldBadge(field: LooseDict, editingRecordId = ""): string {
  if (isRequiredRepairField(field.field_name) && !editingRecordId) return "必填";
  if (field.options?.length) return "下拉选择";
  return String(field.ui_type || "可编辑");
}

export function repairFieldValueToText(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

export function parseRepairDraftValue(value: string): unknown {
  const text = String(value ?? "").trim();
  if (!text) return "";
  if ((text.startsWith("{") && text.endsWith("}")) || (text.startsWith("[") && text.endsWith("]"))) {
    try {
      return JSON.parse(text);
    } catch {
      return text;
    }
  }
  return text;
}

export function repairEventRecordId(item: LooseDict | null | undefined): string {
  if (!item) return "";
  return String(item.record_id || item.source_record_id || "").trim();
}

export function repairEventMeta(item: LooseDict | null | undefined): string {
  if (!item) return "";
  return [
    item.building,
    item.specialty,
    item.level,
    item.source,
    item.status,
    item.occurrence_time,
  ].map((value) => String(value || "").trim()).filter(Boolean).join(" · ");
}

export function repairCurrentScopeLabel(scope: string, scopeOptions: ScopeOption[]): string {
  const normalized = String(scope || "ALL").trim().toUpperCase();
  const matched = scopeOptions.find((item) => String(item.value || "").toUpperCase() === normalized);
  if (matched?.label) return matched.label;
  if (normalized === "ALL") return "全部";
  if (normalized === "110") return "110站";
  return normalized ? `${normalized}楼` : "全部";
}

export function repairRecordBuildingLabel(record: LooseDict): string {
  const fields = record.display_fields || {};
  const explicit = fields["楼栋"] || fields["所属数据中心/楼栋-使用"] || fields["所属数据中心/楼栋（关联CMDB唯一ID关联,DE不选）"];
  const text = String(explicit || "").trim();
  if (text) return text;
  const codes = Array.isArray(record.building_codes) ? record.building_codes : [];
  if (codes.length) {
    return codes.map((code) => (String(code).toUpperCase() === "110" ? "110站" : `${String(code).toUpperCase()}楼`)).join("、");
  }
  return "楼栋未填";
}

export function repairRecordSpecialtyLabel(record: LooseDict): string {
  const fields = record.display_fields || {};
  return String(fields["专业"] || fields["所属专业"] || fields["专业（推送消息用）"] || "专业未填").trim();
}

export function repairRecordTimeLabel(record: LooseDict): string {
  const fields = record.display_fields || {};
  const time = fields["发现故障时间"] || fields["期望完成时间"] || fields["维修开始时间"] || record.last_modified_time;
  return String(time || "时间未填").trim();
}
