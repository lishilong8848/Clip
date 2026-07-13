import type { LooseDict, ScopeOption } from "./types";

export const REPAIR_REQUIRED_FIELD_GROUPS = [
  ["维修名称"],
  ["故障发生时间"],
  ["故障维修原因"],
  ["所属专业", "专业（推送消息用）"],
  ["所属数据中心/楼栋-使用", "所属数据中心/楼栋（关联CMDB唯一ID关联,DE不选）"],
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
  if (field.auto_filled) return "自动填写";
  if (field.options?.length) return "下拉选择";
  return String(field.ui_type || "可编辑");
}

export function repairDraftInputValue(field: LooseDict, value: unknown): string {
  const uiType = String(field.ui_type || "").toLowerCase();
  const fieldType = Number(field.field_type || 0);
  if (fieldType === 5 || uiType.includes("datetime")) {
    if (typeof value === "number" && Number.isFinite(value)) {
      const date = new Date(value);
      if (!Number.isNaN(date.getTime())) {
        const pad = (item: number) => String(item).padStart(2, "0");
        return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}`;
      }
    }
    const text = repairFieldValueToText(value).trim().replace(" ", "T");
    return /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}/.test(text) ? text.slice(0, 16) : "";
  }
  if (fieldType === 15 || uiType === "url") {
    const link = value && typeof value === "object" && !Array.isArray(value)
      ? (value as Record<string, unknown>).link
      : undefined;
    return repairFieldValueToText(link ?? value);
  }
  return repairFieldValueToText(value);
}

export function repairFieldUsesTextarea(fieldName: unknown): boolean {
  return /(描述|原因|现象|措施|方案|跟进|进展|人员|附件)/.test(String(fieldName || ""));
}

function isValueRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function uniqueReadableValues(values: string[]): string[] {
  return Array.from(new Set(values.map((item) => item.trim()).filter(Boolean)));
}

function decodedStructuredText(value: string): unknown | undefined {
  const text = value.trim();
  if (!text || !["[", "{"].includes(text[0])) return undefined;
  try {
    const decoded = JSON.parse(text);
    return decoded && typeof decoded === "object" ? decoded : undefined;
  } catch {
    return undefined;
  }
}

function readableFieldValue(value: unknown, seen: WeakSet<object>, depth: number): string {
  if (value === null || value === undefined) return "";
  if (depth > 8) return "";
  if (typeof value === "string") {
    const decoded = decodedStructuredText(value);
    return decoded === undefined ? value : readableFieldValue(decoded, seen, depth + 1);
  }
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  if (Array.isArray(value)) {
    if (
      value.length > 0
      && value.every((item) => isValueRecord(item) && typeof item.text === "string")
    ) {
      return value.map((item) => String((item as Record<string, unknown>).text || "")).join("");
    }
    return uniqueReadableValues(
      value.map((item) => readableFieldValue(item, seen, depth + 1)),
    ).join("、");
  }
  if (!isValueRecord(value)) return String(value);
  if (seen.has(value)) return "";
  seen.add(value);
  try {
    for (const key of ["text", "name", "title", "value", "file_name", "fileName"]) {
      if (value[key] === null || value[key] === undefined || value[key] === "") continue;
      const text = readableFieldValue(value[key], seen, depth + 1);
      if (text) return text;
    }
    for (const key of ["text_arr", "users", "groups", "records", "record_ids", "link_record_ids"]) {
      if (!Array.isArray(value[key])) continue;
      const text = readableFieldValue(value[key], seen, depth + 1);
      if (text) return text;
    }
    if (typeof value.link === "string" && value.link.trim()) return value.link.trim();
    return uniqueReadableValues(
      Object.entries(value)
        .filter(([key]) => !["type", "id", "open_id", "user_id", "record_id"].includes(key))
        .map(([, item]) => readableFieldValue(item, seen, depth + 1)),
    ).join("、");
  } finally {
    seen.delete(value);
  }
}

export function repairFieldValueToText(value: unknown): string {
  return readableFieldValue(value, new WeakSet<object>(), 0);
}

export function repairFieldPreservesRawValue(field: LooseDict): boolean {
  const fieldType = Number(field.field_type || 0);
  const uiType = String(field.ui_type || "").toLowerCase();
  return [4, 11, 15, 17, 18, 21].includes(fieldType)
    || /(multiselect|user|url|attachment|link)/.test(uiType);
}

export function parseRepairDraftValue(value: string, field?: LooseDict): unknown {
  const text = String(value ?? "").trim();
  if (!text) return "";
  if (field && !repairFieldPreservesRawValue(field)) return text;
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
  const time = fields["故障发生时间"] || fields["维修开始时间"] || fields["维修结束时间"] || record.last_modified_time;
  return String(time || "时间未填").trim();
}
