import type { Dict } from './api/client';

export type MopMobileField = { row: number; col: number; label: string; prefix: string };
export type MopMobileCheck = { row: number; col: number; title: string; detail: string; fields: MopMobileField[]; options: Array<{ label: string; value: string }> };

// Keep classification aligned with MaintenancePortalService._mop_row_has_check_description.
const checkHeaders = new Set(['序号', '编号', '设备/编号', '设备编号', '设备名称', '检查项', '检查项目', '维护项', '维护项目', '检查内容', '维护内容', '检查结果', '维护结果', '维护步骤', '操作步骤', '检查步骤', '检查要求', '维护要求', '备注', '异常备注', '复核备注', '数据记录', '测量数据']);

function stripChoiceGroups(value: string): string {
  const escape = (text: string) => text.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const alternatives = (labels: string[]) => [...new Set(labels)].sort((left, right) => right.length - left.length).map(escape).join('|');
  const prefix = [...value.matchAll(/([□☐■☑√✔✓])([^□☐■☑√✔✓\[\]；;，,、/]+)/g)]
    .map(match => match[2].trim());
  if (new Set(prefix.map(label => label.replace(/\s/g, ''))).size >= 2) {
    return value.replace(new RegExp(`[□☐■☑√✔✓]\\s*(?:${alternatives(prefix)})`, 'g'), '');
  }
  const bracket = [...value.matchAll(/([^\[\]□☐■☑√✔✓；;，,、/]{1,24})\[(.*?)\]/g)]
    .map(match => match[1].split(/[:：]/).pop()!.trim()).filter(Boolean);
  if (new Set(bracket.map(label => label.replace(/\s/g, ''))).size >= 2) {
    return value.replace(new RegExp(`(?:${alternatives(bracket)})\\s*\\[[^\\]]*\\]`, 'g'), '');
  }
  return value;
}

function trimChoiceEdges(text: string): string {
  return text.replace(/^[\s_—–\-/\\:：;；,，、.。()（）\[\]【】□☐■☑√✔✓]+|[\s_—–\-/\\:：;；,，、.。()（）\[\]【】□☐■☑√✔✓]+$/g, '');
}

export function mopChoicePlaceholder(value: unknown): boolean {
  const text = String(value ?? '').replace(/\s/g, '');
  const stripped = stripChoiceGroups(text);
  return stripped !== text && !trimChoiceEdges(stripped);
}

function checkDescription(value: unknown): string {
  const raw = String(value ?? '').trim();
  const text = trimChoiceEdges(stripChoiceGroups(raw.replace(/\s/g, '')));
  const number = text.replace(/[.．、()（）#\-—_]+/g, '');
  const numeric = /^[\p{Number}零〇一二三四五六七八九十百千万亿两壹贰叁肆伍陆柒捌玖拾佰仟]+$/u.test(number);
  const datePlaceholder = ['年', '月', '日', '时'].every(token => text.includes(token))
    && (text.includes('__') || text.includes('年月日时') || !/\d{4}年\d{1,2}月\d{1,2}日\d{1,2}时/.test(text));
  return text && !checkHeaders.has(text) && !numeric && !datePlaceholder ? trimChoiceEdges(stripChoiceGroups(raw)) : '';
}

function cellAnchor(sheet: Dict, row: number, col: number): { row: number; col: number } {
  for (const merge of Array.isArray(sheet.merges) ? sheet.merges : []) {
    const startRow = Number(merge.row), startCol = Number(merge.col);
    if (row >= startRow && row < startRow + Math.max(1, Number(merge.rowspan) || 1)
      && col >= startCol && col < startCol + Math.max(1, Number(merge.colspan) || 1)) {
      return { row: startRow, col: startCol };
    }
  }
  return { row, col };
}

export function mopCheckLabels(sheet: Dict, row: number, col: number): string[] {
  if (sheet.is_cover || !Number.isInteger(row) || !Number.isInteger(col) || row < 0 || col < 0) return [];
  const rows: unknown[][] = Array.isArray(sheet.rows) ? sheet.rows : [];
  if (!Array.isArray(rows[row])) return [];
  const mergedColumns = (Array.isArray(sheet.merges) ? sheet.merges : [])
    .filter(merge => row >= Number(merge.row) && row < Number(merge.row) + Math.max(1, Number(merge.rowspan) || 1))
    .map(merge => Number(merge.col) + 1).filter(value => Number.isInteger(value) && value > 0);
  const width = Math.max(rows[row].length, ...mergedColumns);
  const labels = new Set<string>(), anchors = new Set<string>();
  for (let currentCol = 0; currentCol < width; currentCol += 1) {
    const anchor = cellAnchor(sheet, row, currentCol);
    const key = `${anchor.row}:${anchor.col}`;
    if (anchors.has(key)) continue;
    anchors.add(key);
    const label = checkDescription(rows[anchor.row]?.[anchor.col]);
    if (label) labels.add(label);
  }
  return [...labels];
}

export function mopCheckboxCells(sheet: Dict): Dict[] {
  const cells: Dict[] = Array.isArray(sheet.checkbox_cells) ? sheet.checkbox_cells : [];
  return cells.filter(cell => {
    const row = Number(cell.row), col = Number(cell.col);
    const anchor = cellAnchor(sheet, row, col);
    return anchor.row === row && anchor.col === col && mopCheckLabels(sheet, row, col).length > 0;
  });
}

export function mopMobileFields(sheet: Dict, editable: (row: number, col: number) => boolean, merge: (row: number, col: number) => Dict) {
  const common: MopMobileField[] = [], checks: MopMobileCheck[] = [];
  if (sheet.is_cover) return { common, checks };
  const rows: unknown[][] = Array.isArray(sheet.rows) ? sheet.rows : [];
  const checkboxRows = new Map<number, Dict[]>();
  for (const cell of mopCheckboxCells(sheet)) {
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
      const titles = mopCheckLabels(sheet, rowIndex, col);
      if (!titles.length) continue;
      const fields = headers.filter(header => editable(rowIndex, header.col) && !merge(rowIndex, header.col)?.hidden)
        .map(header => ({ row: rowIndex, col: header.col, label: header.label, prefix: '' }));
      checks.push({ row: rowIndex, col, title: titles.slice(0, 2).join(' · '), detail: titles.slice(2).join(' · '), fields,
        options: (cell.options?.length ? cell.options : [{ label: '正常' }, { label: '异常' }]).map((option: Dict) => ({ label: text(option.label || option.key), value: text(option.label || option.key) })) });
    }
  });
  return { common, checks };
}
