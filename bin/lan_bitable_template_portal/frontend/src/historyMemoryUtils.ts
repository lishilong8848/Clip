import type { Dict } from "./api/client";

export const HISTORY_MEMORY_WORK_TYPES = [
  { value: "maintenance", label: "维保" },
  { value: "change", label: "变更" },
  { value: "repair", label: "检修" },
];

export const HISTORY_MEMORY_EDITABLE_FIELD_DEFS = [
  { key: "location", label: "位置/地点" },
  { key: "content", label: "内容", multi: true },
  { key: "reason", label: "原因/故障原因", multi: true },
  { key: "impact", label: "影响/影响范围", multi: true },
  { key: "progress", label: "进度/完成情况", multi: true },
  { key: "specialty", label: "专业" },
  { key: "level", label: "等级/紧急程度" },
  { key: "repair_device", label: "维修设备" },
  { key: "repair_fault", label: "维修故障" },
  { key: "fault_type", label: "故障类型" },
  { key: "repair_mode", label: "维修方式" },
  { key: "discovery", label: "故障发现方式" },
  { key: "symptom", label: "故障现象", multi: true },
  { key: "solution", label: "解决方案", multi: true },
  { key: "zhihang_title", label: "智航关联标题" },
  { key: "zhihang_record_id", label: "智航关联记录" },
  { key: "zhihang_progress", label: "智航进展" },
] as Array<{ key: string; label: string; multi?: boolean }>;

export const HISTORY_MEMORY_PRIMARY_FIELD_KEYS = new Set([
  "location",
  "content",
  "reason",
  "impact",
  "progress",
  "specialty",
  "level",
]);

export function historyMemoryWorkTypeLabel(value: string): string {
  return HISTORY_MEMORY_WORK_TYPES.find((item) => item.value === value)?.label || "维保";
}

export function historyMemoryHasMeaningfulFields(fields: Dict | undefined): boolean {
  const payload = fields || {};
  return Object.entries(payload).some(([key, value]) => {
    if (["updated_at", "history_imported_at", "imported_by", "imported_from"].includes(key)) return false;
    return String(value ?? "").trim() !== "";
  });
}

export function historyMemorySourceHasMemory(source: Dict | undefined): boolean {
  return historyMemoryHasMeaningfulFields(source?.memory);
}

export function historyMemorySourceInitialFields(source: Dict): Dict {
  if (historyMemorySourceHasMemory(source)) return { ...(source.memory || {}) };
  return { ...(source.current_fields || {}) };
}

export function historyMemorySourceInitialOrigin(source: Dict): string {
  return historyMemorySourceHasMemory(source) ? "memory" : "current";
}

export function historyMemoryOriginLabel(origin: string): string {
  if (origin === "candidate") return "当前显示：历史候选";
  if (origin === "memory") return "当前显示：已有记忆";
  if (origin === "current") return "当前显示：当前事项";
  return "当前显示：未初始化";
}

export function historyMemoryFieldVisible(key: string, workType: string): boolean {
  const type = workType || "maintenance";
  if (key.startsWith("repair_") || ["fault_type", "repair_mode", "discovery", "symptom", "solution"].includes(key)) {
    return type === "repair";
  }
  if (key.startsWith("zhihang_")) return type === "change";
  if (key === "level") return type !== "maintenance";
  return true;
}

export function buildHistoryMemorySavePayload(input: {
  sources: Dict[];
  selectedMap: Record<string, boolean>;
  candidateMap: Record<string, string>;
  fieldEdits: Record<string, Dict>;
  fieldOriginMap: Record<string, string>;
}): Dict[] {
  return input.sources
    .filter((source) => Boolean(input.selectedMap[source.id]))
    .map((source) => ({
      selected: true,
      source_item: source,
      candidate_id: input.candidateMap[source.id],
      fields: input.fieldEdits[source.id] || {},
      field_origin: input.fieldOriginMap[source.id] || historyMemorySourceInitialOrigin(source),
    }));
}
