import type { Dict } from './api/client';

export type MopFillTarget = {
  row: number; col: number; label: string;
  kind: 'check' | 'field' | 'signature'; done: boolean;
  required?: boolean;
};

// A result counts only when exactly one option is selected, including values
// already present in the workbook. Merely editing a cell is not completion.
export function mopSelectedChoice(value: unknown, labels: string[]): string {
  const text = String(value ?? '').trim();
  if (labels.includes(text)) return text;
  const selected = labels.filter(label => {
    const escaped = label.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    return new RegExp(`[☑☒■√✔✓]\\s*${escaped}(?![\\p{L}\\p{N}])`, 'u').test(text)
      || new RegExp(`(?:^|[^\\p{L}\\p{N}])${escaped}\\s*[\\[（(【]\\s*[√✔✓☑☒■]\\s*[\\]）)】]`, 'u').test(text);
  });
  return selected.length === 1 ? selected[0] : '';
}

export function uniqueMopFillTargets(targets: MopFillTarget[]): MopFillTarget[] {
  const byCell = new Map<string, MopFillTarget>();
  for (const target of targets) {
    if (!Number.isInteger(target.row) || !Number.isInteger(target.col) || target.row < 0 || target.col < 0) continue;
    byCell.set(`${target.row}:${target.col}`, target);
  }
  return [...byCell.values()].sort((a, b) => a.row - b.row || a.col - b.col);
}

// Hide spacer rows in the preview only. Coordinates and workbook data remain
// unchanged; keep merge anchors needed by any visible continuation row.
export function mopPreviewRowIndexes(sheet: Dict, keepRow: (row: number) => boolean): number[] {
  const rows: unknown[][] = Array.isArray(sheet.rows) ? sheet.rows : [];
  const included = new Set<number>();
  rows.forEach((_row, row) => { if (keepRow(row)) included.add(row); });
  const merges: Dict[] = Array.isArray(sheet.merges) ? sheet.merges : [];
  let changed = true;
  while (changed) {
    changed = false;
    for (const merge of merges) {
      const start = Number(merge.row), end = Math.min(rows.length, start + Number(merge.rowspan || 1));
      if (!Number.isInteger(start) || start < 0 || included.has(start)) continue;
      if ([...included].some(row => row > start && row < end)) {
        included.add(start); changed = true;
      }
    }
  }
  return [...included].sort((a, b) => a - b);
}
