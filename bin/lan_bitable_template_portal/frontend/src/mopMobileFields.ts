import type { Dict } from './api/client';

export type MopMobileField = { row: number; col: number; label: string; prefix: string };
export type MopMobileCheck = { row: number; col: number; title: string; detail: string; fields: MopMobileField[]; options: Array<{ label: string; value: string }> };

export function mopMobileFields(sheet: Dict, editable: (row: number, col: number) => boolean, merge: (row: number, col: number) => Dict) {
  const common: MopMobileField[] = [], checks: MopMobileCheck[] = [];
  if (sheet.is_cover) return { common, checks };
  const rows: unknown[][] = Array.isArray(sheet.rows) ? sheet.rows : [];
  const checkboxRows = new Map<number, Dict[]>();
  for (const cell of sheet.checkbox_cells || []) {
    if (Number(cell.row) < 9) continue;
    const entries = checkboxRows.get(Number(cell.row)) || [];
    entries.push(cell);
    checkboxRows.set(Number(cell.row), entries);
  }
  let headers: Array<{ col: number; label: string }> = [];
  const text = (value: unknown) => String(value ?? '').trim();
  rows.forEach((row, rowIndex) => {
    const labels = row.map(text);
    const notes = labels.flatMap((label, col) => /^(备注|异常备注|复核备注|数据记录|测量数据)$/.test(label.replace(/\s/g, '')) ? [{ col, label }] : []);
    // Read only explicit headers; never infer a writable target from an instruction paragraph.
    if (notes.length && labels.some(label => /^(检查结果|维护结果|检查项|维护项|设备\/编号|检查内容)$/.test(label.replace(/\s/g, '')))) headers = notes;
    labels.forEach((label, col) => {
      if (merge(rowIndex, col)?.hidden) return;
      const match = label.match(/^(维护涉及位置|维护位置|设备位置|安装位置|位置|备注)\s*[:：]\s*(.*)$/s);
      if (!match) return;
      let target = col;
      if (!match[2]) {
        const next = col + Number(merge(rowIndex, col)?.colspan || 1);
        if (next < row.length && !merge(rowIndex, next)?.hidden && editable(rowIndex, next) && !/[:：]$/.test(labels[next])) target = next;
      }
      if (!editable(rowIndex, target)) return;
      common.push({ row: rowIndex, col: target, label: match[1], prefix: target === col ? label.slice(0, label.length - match[2].length) : '' });
    });
    for (const cell of checkboxRows.get(rowIndex) || []) {
      const col = Number(cell.col);
      if (merge(rowIndex, col)?.hidden) continue;
      const titles = labels.slice(0, col).filter(label => label && !/^\d+$/.test(label));
      const fields = headers.filter(header => editable(rowIndex, header.col) && !merge(rowIndex, header.col)?.hidden)
        .map(header => ({ row: rowIndex, col: header.col, label: header.label, prefix: '' }));
      checks.push({ row: rowIndex, col, title: titles.slice(0, 2).join(' · ') || `第 ${rowIndex + 1} 行`, detail: titles.slice(2).join(' · '), fields,
        options: (cell.options?.length ? cell.options : [{ label: '正常' }, { label: '异常' }]).map((option: Dict) => ({ label: text(option.label || option.key), value: text(option.label || option.key) })) });
    }
  });
  return { common, checks };
}
