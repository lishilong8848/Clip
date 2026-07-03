import type { Dict } from "./api/client";
import { parseMopCellEditKey } from "./mopSheetUtils";

export function mopPayloadList(value: unknown): Dict[] {
  return Array.isArray(value) ? value.filter((item): item is Dict => Boolean(item && typeof item === "object")) : [];
}

export function mopMemoryFieldKeys(field: Dict): string[] {
  const label = String(field.label || "").trim();
  const valueCellRef = String(field.value_cell_ref || "").trim();
  const labelCellRef = String(field.label_cell_ref || "").trim();
  const row = String(Number(field.row));
  const valueCol = String(Number(field.value_col ?? field.label_col ?? -1));
  const keys = [
    valueCellRef ? `value:${valueCellRef}` : "",
    labelCellRef ? `label-cell:${labelCellRef}` : "",
    label && valueCellRef ? `label-value:${label}:${valueCellRef}` : "",
    label ? `label-pos:${label}:${row}:${valueCol}` : "",
    label ? `label:${label}` : "",
  ];
  return keys.filter(Boolean);
}

export function buildMopCheckboxPayload(
  cells: Dict[],
  getSelection: (cell: Dict) => string,
): Dict[] {
  return cells
    .map((cell) => ({ ...cell, selection: getSelection(cell), selected_label: getSelection(cell) }))
    .filter((cell) => cell.selection);
}

export function buildMopFieldPayload(
  fields: Dict[],
  getValue: (field: Dict) => string,
): Dict[] {
  return fields.map((field) => ({
    ...field,
    fill_value: getValue(field),
  }));
}

export function buildMopCellEditPayload(edits: Record<string, string>): Dict[] {
  return Object.entries(edits).flatMap(([key, value]) => {
    const parsed = parseMopCellEditKey(key);
    if (!parsed) return [];
    return {
      sheet: parsed.sheet,
      row: parsed.row,
      col: parsed.col,
      value,
    };
  });
}
