import type { LooseDict } from "./types";

export type MopSignatureRole = "implementer" | "auditor";
export type MopCellPosition = { row: number; col: number };
export type MopCellBounds = { minRow: number; maxRow: number; minCol: number; maxCol: number };
export type MopCellOverlayMode = "none" | "checkbox" | "field-time" | "field-completion" | "raw" | "selection";

function pad2(value: number): string {
  return String(value).padStart(2, "0");
}

export function defaultMopDateTimeLocal(): string {
  const now = new Date();
  now.setMinutes(0, 0, 0);
  return `${now.getFullYear()}-${pad2(now.getMonth() + 1)}-${pad2(now.getDate())}T${pad2(now.getHours())}:00`;
}

export function formatMopDateTime(value: string): string {
  const text = String(value || "").trim();
  if (!text) return "";
  const date = new Date(text);
  if (Number.isNaN(date.getTime())) return text.replace("T", " ");
  return `${date.getFullYear()}年${date.getMonth() + 1}月${date.getDate()}日${pad2(date.getHours())}时${pad2(date.getMinutes())}分`;
}

export function formatMopUploadTime(value: string): string {
  const text = String(value || "").trim();
  if (!text) return "";
  const normalized = text.replace("T", " ");
  const match = normalized.match(/^(\d{4})[-/年](\d{1,2})[-/月](\d{1,2})日?\s+(\d{1,2})[:时](\d{1,2})/);
  if (match) {
    return `${match[1]}年${Number(match[2])}月${Number(match[3])}日${Number(match[4])}时${pad2(Number(match[5]))}分`;
  }
  const date = new Date(text);
  if (Number.isNaN(date.getTime())) return normalized;
  return `${date.getFullYear()}年${date.getMonth() + 1}月${date.getDate()}日${date.getHours()}时${pad2(date.getMinutes())}分`;
}

export function normalizeMopNumberText(value: unknown): string {
  return String(value || "").replace(/[０-９]/g, (char) => String(char.charCodeAt(0) - 0xff10));
}

export function stripMopFieldLabel(value: unknown): string {
  return String(value || "")
    .replace(/^[\s　]*(维护开始时间|维护完成时间|审核确认时间)[\s　:：]*/u, "")
    .trim();
}

export function normalizeMopRequiredTimeText(value: unknown): string {
  const text = stripMopFieldLabel(value)
    .replace(/[：]/g, ":")
    .replace(/\s+/g, " ")
    .trim();
  if (!text || !/\d/.test(normalizeMopNumberText(text))) return "";
  return text.replace("T", " ");
}

export function parseMopComparableDate(value: unknown): Date | null {
  const raw = normalizeMopNumberText(stripMopFieldLabel(value))
    .replace(/[：]/g, ":")
    .replace("T", " ")
    .replace(/\s+/g, " ")
    .trim();
  if (!raw || !/\d/.test(raw)) return null;
  const chinese = raw.match(/(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日\s*(?:(\d{1,2})\s*(?:时|点)(?:\s*(\d{1,2})\s*分?)?)?/);
  if (chinese) {
    const parsed = new Date(
      Number(chinese[1]),
      Number(chinese[2]) - 1,
      Number(chinese[3]),
      Number(chinese[4] || 0),
      Number(chinese[5] || 0),
      0,
      0,
    );
    return Number.isNaN(parsed.getTime()) ? null : parsed;
  }
  const text = raw
    .replace(/年|\/|\./g, "-")
    .replace(/月/g, "-")
    .replace(/日/g, " ")
    .replace(/时|点/g, ":")
    .replace(/分|秒/g, "")
    .replace(/:(?=\s*$)/, ":00")
    .trim();
  if (!text) return null;
  const match = text.match(/(\d{4})-(\d{1,2})-(\d{1,2})(?:\s+(\d{1,2})(?::(\d{1,2}))?)?/);
  if (!match) {
    const fallback = new Date(text);
    return Number.isNaN(fallback.getTime()) ? null : fallback;
  }
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const hour = Number(match[4] || 0);
  const minute = Number(match[5] || 0);
  const parsed = new Date(year, month - 1, day, hour, minute, 0, 0);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

export function parsePeopleCount(value: unknown): number {
  const text = normalizeMopNumberText(value);
  const digitMatch = text.match(/(\d{1,3})\s*人/);
  if (digitMatch) return Math.max(0, Number(digitMatch[1] || 0));
  const zhDigits: Record<string, number> = {
    零: 0,
    一: 1,
    二: 2,
    两: 2,
    三: 3,
    四: 4,
    五: 5,
    六: 6,
    七: 7,
    八: 8,
    九: 9,
    十: 10,
  };
  const zhMatch = text.match(/([一二两三四五六七八九十]{1,3})\s*人/);
  if (!zhMatch) return 0;
  const raw = zhMatch[1] || "";
  if (raw === "十") return 10;
  if (raw.startsWith("十")) return 10 + (zhDigits[raw.slice(1)] || 0);
  if (raw.includes("十")) {
    const [ten, one] = raw.split("十");
    return (zhDigits[ten] || 0) * 10 + (zhDigits[one] || 0);
  }
  return zhDigits[raw] || 0;
}

export function makeMopCheckboxKey(sheetName: string, cell: LooseDict): string {
  return `${sheetName || ""}:${cell.cell_ref || `${cell.row}:${cell.col}`}`;
}

export function makeMopMaintenanceKey(sheetName: string, field: LooseDict): string {
  return `${sheetName || ""}:${field.label || ""}:${field.value_cell_ref || field.label_cell_ref || `${field.row}:${field.value_col}`}`;
}

export function makeMopCellKey(sheetName: string, rowIndex: number, colIndex: number): string {
  return `${sheetName || "sheet"}:${rowIndex}:${colIndex}`;
}

export function makeRawMopCellKey(sheetName: string, rowIndex: number, colIndex: number): string {
  return `${sheetName || ""}:${rowIndex}:${colIndex}`;
}

export function roleForMopMaintenanceLabel(label: unknown): MopSignatureRole | "" {
  const text = String(label || "");
  if (text.includes("维护实施人")) return "implementer";
  if (text.includes("维护审核人")) return "auditor";
  return "";
}

export function selectedMopCellBounds(positions: MopCellPosition[]): MopCellBounds | null {
  if (!positions.length) return null;
  return {
    minRow: Math.min(...positions.map((item) => item.row)),
    maxRow: Math.max(...positions.map((item) => item.row)),
    minCol: Math.min(...positions.map((item) => item.col)),
    maxCol: Math.max(...positions.map((item) => item.col)),
  };
}

export function normalizeMopClipboardText(text: unknown): string {
  return String(text || "").replace(/\r\n/g, "\n").replace(/\r/g, "\n");
}

export function parseMopClipboardMatrix(text: unknown): string[][] {
  const normalized = normalizeMopClipboardText(text);
  if (normalized.includes("\n") || normalized.includes("\t")) {
    return normalized.replace(/\n$/, "").split("\n").map((line) => line.split("\t"));
  }
  return [[normalized]];
}

export function isMopMatrixClipboardText(text: unknown): boolean {
  return /[\n\r\t]/.test(String(text || ""));
}

export function mopCellOverlayStyle(
  rect: Pick<DOMRect, "top" | "bottom" | "left" | "width">,
  mode: MopCellOverlayMode,
  viewportWidth?: number,
  viewportHeight?: number,
): Record<string, string> {
  const width = viewportWidth || (typeof window !== "undefined" ? window.innerWidth : 1024);
  const height = viewportHeight || (typeof window !== "undefined" ? window.innerHeight : 768);
  const overlayWidthByMode: Record<string, number> = {
    checkbox: Math.min(198, Math.max(136, rect.width + 56)),
    "field-time": Math.min(328, Math.max(258, rect.width + 132)),
    "field-completion": Math.min(218, Math.max(166, rect.width + 64)),
    selection: Math.min(318, Math.max(218, rect.width + 82)),
    raw: Math.min(320, Math.max(246, width - 32)),
    none: 240,
  };
  const overlayHeightByMode: Record<string, number> = {
    checkbox: 44,
    "field-time": 44,
    "field-completion": 44,
    selection: 44,
    raw: 184,
    none: 44,
  };
  const estimatedWidth = Math.min(width - 24, overlayWidthByMode[mode] || 240);
  const estimatedHeight = overlayHeightByMode[mode] || 44;
  const topSafe = 14;
  let top = rect.top - estimatedHeight - 10;
  if (top < topSafe) top = rect.bottom + 10;
  if (top + estimatedHeight > height - 14) {
    top = Math.max(topSafe, height - estimatedHeight - 14);
  }
  const centeredLeft = rect.left + rect.width / 2 - estimatedWidth / 2;
  const rawLeft = rect.left;
  const preferredLeft = mode === "raw" ? rawLeft : centeredLeft;
  const left = Math.min(Math.max(12, preferredLeft), Math.max(12, width - estimatedWidth - 12));
  const style: Record<string, string> = {
    top: `${Math.round(top)}px`,
    left: `${Math.round(left)}px`,
    maxWidth: "calc(100vw - 24px)",
  };
  if (mode === "raw" || mode === "field-time" || mode === "selection") style.width = `${Math.round(estimatedWidth)}px`;
  return style;
}
