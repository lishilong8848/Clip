export type AdminTabKey = "status" | "permissions" | "handover" | "mop" | "pressure";

export type ScopeLabelOption = {
  value: string;
  label: string;
};

export const adminBuildingScopes: ScopeLabelOption[] = [
  { value: "110", label: "110站" },
  { value: "A", label: "A楼" },
  { value: "B", label: "B楼" },
  { value: "C", label: "C楼" },
  { value: "D", label: "D楼" },
  { value: "E", label: "E楼" },
  { value: "H", label: "H楼" },
];

export function prettyJson(value: unknown): string {
  return JSON.stringify(value || {}, null, 2);
}

export function formatBytes(value: unknown): string {
  const bytes = Number(value || 0);
  if (!Number.isFinite(bytes) || bytes <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB"];
  let size = bytes;
  let unit = 0;
  while (size >= 1024 && unit < units.length - 1) {
    size /= 1024;
    unit += 1;
  }
  return `${size.toFixed(unit === 0 ? 0 : 1)} ${units[unit]}`;
}

export function formatUnixTime(value: unknown): string {
  const ts = Number(value || 0);
  if (!ts) return "暂无";
  const date = new Date(ts * 1000);
  if (Number.isNaN(date.getTime())) return "暂无";
  return date.toLocaleString("zh-CN", { hour12: false });
}

export function shortId(value: unknown): string {
  const text = String(value || "").trim();
  if (!text) return "-";
  return text.length > 12 ? `${text.slice(0, 8)}...${text.slice(-4)}` : text;
}

export function scopeOptionLabel(options: ScopeLabelOption[], value: unknown): string {
  const text = String(value || "").trim();
  if (!text) return "";
  return options.find((item) => String(item.value) === text)?.label || text;
}

export function cleanupRemovedTotal(value: Record<string, any>): number {
  const keys = [
    "deleted",
    "runtime_queue_removed",
    "outbox_removed",
    "append_events_removed",
    "undo_removed",
    "attachment_removed",
    "clipboard_removed",
    "dialog_removed",
    "mop_temp_signature_removed",
  ];
  return keys.reduce((total, key) => total + Number(value?.[key] || 0), 0);
}
