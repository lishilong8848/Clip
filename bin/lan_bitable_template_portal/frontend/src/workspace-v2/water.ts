import type { Dict } from './client';
import type { ChartItem } from './vipboard';

export type WaterDraft = { meter: string; frequency: string; shift: string; statisticDate: string; meterValue: number | null; correctedUsage: number | null; expectedVersion: string };
export type WaterPhoto = { localId: string; uploadId: string; name: string; previewUrl: string; file: File; uploading: boolean; error: string };
export type WaterFilters = { range: string; month: string; startDate: string; endDate: string; meter: string; frequency: string; shift: string; query: string };
export const waterScopes = new Set([...'ABCDEH']);
export function waterNumber(value: unknown): number | null {
  if (value === null || value === undefined || typeof value === 'string' && !value.trim() || typeof value === 'boolean') return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}
export function waterNumberText(value: unknown, digits = 3): string {
  const number = waterNumber(value);
  return number === null ? '—' : number.toLocaleString('zh-CN', { maximumFractionDigits: digits });
}
export function waterDay(offset = 0): string {
  const date = new Date(Date.now() + 8 * 3600000 + offset * 86400000);
  return date.toISOString().slice(0, 10);
}
export function validWaterDate(value: string): boolean {
  return /^\d{4}-\d{2}-\d{2}$/.test(value) && Number.isFinite(Date.parse(value)) && new Date(value + 'T00:00:00Z').toISOString().slice(0, 10) === value;
}
export function waterQuery(scope: string, filters: WaterFilters, page = 1, today = waterDay()): URLSearchParams {
  const params = new URLSearchParams({ scope, page: String(page), page_size: '50' });
  if (filters.range === '近3天') { params.set('start_date', new Date(Date.parse(today+'T00:00:00Z')-2*86400000).toISOString().slice(0,10)); params.set('end_date', today); }
  if (filters.range === '近30天') { params.set('start_date', new Date(Date.parse(today+'T00:00:00Z')-29*86400000).toISOString().slice(0,10)); params.set('end_date', today); }
  if (filters.range === '本月') {
    if (!/^\d{4}-(0[1-9]|1[0-2])$/.test(filters.month)) throw new Error('请选择月份。');
    params.set('month', filters.month);
  }
  if (filters.range === '自定义日期') {
    if (!validWaterDate(filters.startDate) || !validWaterDate(filters.endDate)) throw new Error('请选择完整的开始日期和结束日期。');
    if (filters.startDate > filters.endDate) throw new Error('开始日期不能晚于结束日期。');
    params.set('start_date', filters.startDate); params.set('end_date', filters.endDate);
  }
  for (const key of ['meter', 'frequency', 'shift'] as const) if (filters[key]) params.set(key, filters[key]);
  if (filters.query.trim()) params.set('q', filters.query.trim());
  return params;
}
export function waterPhotoCount(row: Dict): number | null {
  if (Array.isArray(row.photos)) return row.photos.length;
  return waterNumber(row.photo_count);
}
export function waterRecordState(row: Dict): { label: string; type: 'info' | 'error' | 'warning' | 'success' | 'default'; title?: string } {
  if (waterNumber(row.meter_value) === null) return { label: '未记录', type: 'info' };
  if (row.large_change?.requires_confirmation === true) return { label: '变化较大', type: 'error', title: row.large_change.baseline_zero ? '上期读数为零，本次读数变化需核查' : `较上期读数变化 ${waterNumberText(row.large_change.ratio_percent, 1)}%，超过现有阈值` };
  if (waterPhotoCount(row) === 0) return { label: '待补照片', type: 'warning' };
  if (waterPhotoCount(row) === null) return { label: '照片待核验', type: 'default' };
  return { label: '已记录', type: 'success' };
}
export function waterTrend(rows: Dict[]): { items: ChartItem[]; missing: number } {
  const days = new Map<string, { value: number; complete: boolean }>();
  let missing = 0;
  for (const row of rows.filter(row => row.frequency === '日')) {
    const day = String(row.statistic_date_key || row.statistic_date || '').slice(0, 10);
    if (!validWaterDate(day)) { missing++; continue; }
    const value = waterNumber(row.corrected_usage) ?? waterNumber(row.computed_usage);
    const entry = days.get(day) || { value: 0, complete: true };
    if (value === null || value < 0) { entry.complete = false; missing++; }
    else entry.value += value;
    days.set(day, entry);
  }
  return { items: [...days].filter(([, value]) => value.complete).sort(([a], [b]) => a.localeCompare(b)).map(([label, data]) => ({ label, value: Number(data.value.toFixed(3)) })), missing };
}
export function waterImageMime(file: File): string {
  if (file.type.toLowerCase().startsWith('image/')) return file.type.toLowerCase();
  return ({ jpg: 'image/jpeg', jpeg: 'image/jpeg', png: 'image/png', webp: 'image/webp', gif: 'image/gif', bmp: 'image/bmp', tif: 'image/tiff', tiff: 'image/tiff' } as Record<string, string>)[file.name.split('.').pop()?.toLowerCase() || ''] || '';
}
