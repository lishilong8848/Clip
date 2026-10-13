import { eventI2OrHigher, eventI3Level, eventRecordId, eventUnderRepair } from '../eventManagementUtils';
import type { LooseDict } from '../types';

export type EventPeriod = 'week' | 'month' | 'history';
const DAY = 86400000, OFFSET = 8 * 3600000;
export function beijingDate(time = Date.now()): string {
  return new Date(time + OFFSET).toISOString().slice(0, 10);
}
export function occurrenceTimestamp(value: unknown): number {
  const text = String(value ?? '').replace(/[年月]/g, '-').replace(/日\s*/, ' ').replace(/：/g, ':').trim();
  const match = /^(\d{4})[-/](\d{1,2})[-/](\d{1,2})(?:[ T](\d{1,2}):(\d{1,2})(?::(\d{1,2})(?:\.\d+)?)?)?(Z|[+-]\d{2}:?\d{2})?$/.exec(text);
  if (!match) return NaN;
  const [, y, m, d, h = '0', min = '0', sec = '0', zone = '+08:00'] = match;
  const date = new Date(Date.UTC(+y, +m - 1, +d));
  if (date.getUTCFullYear() !== +y || date.getUTCMonth() !== +m - 1 || date.getUTCDate() !== +d || +h > 23 || +min > 59 || +sec > 59) return NaN;
  return Date.parse(`${y}-${m.padStart(2, '0')}-${d.padStart(2, '0')}T${h.padStart(2, '0')}:${min.padStart(2, '0')}:${sec.padStart(2, '0')}${zone}`);
}
export function eventPeriodRange(period: EventPeriod, month = '', now = Date.now()) {
  const today = beijingDate(now), currentMonth = today.slice(0, 7);
  const selected = period === 'history' && /^\d{4}-(0[1-9]|1[0-2])$/.test(month) ? month : currentMonth;
  const [year, m] = selected.split('-').map(Number);
  let start = Date.UTC(year!, m! - 1, 1) - OFFSET;
  let end = Date.UTC(year!, m!, 1) - OFFSET;
  if (period === 'week') {
    const day = occurrenceTimestamp(today);
    const weekday = new Date(day + OFFSET).getUTCDay();
    start = day - ((weekday + 6) % 7) * DAY;
    end = start + 7 * DAY;
  }
  const first = beijingDate(start), last = beijingDate(end - 1);
  const months = [...new Set([first.slice(0, 7), last.slice(0, 7)])];
  return { start, end, months, label: period === 'week' ? '本周' : period === 'month' ? '本月' : `${year}年${m}月`,
    caption: `${first.replace(/-/g, '.')} - ${last.replace(/-/g, '.')}` };
}
export type EventRange = ReturnType<typeof eventPeriodRange>;
export function eventTrend(records: LooseDict[], range: EventRange) {
  const step = DAY;
  const bins: Array<{label:string;value:number;start:number;end:number}> = [];
  for(let start=range.start; start<range.end; start+=step) {
    const end=Math.min(start+step,range.end), first=beijingDate(start).slice(5).replace('-', '/');
    bins.push({label:first,value:0,start,end});
  }
  for(const item of records){const time=occurrenceTimestamp(item.occurrence_time);const bin=bins.find(item=>time>=item.start&&time<item.end);if(bin)bin.value++;}
  return bins;
}
export function eventPeriodStats(records: LooseDict[]) {
  return { total: records.length, under_repair: records.filter(eventUnderRepair).length,
    i2_or_higher: records.filter(eventI2OrHigher).length, i3: records.filter(eventI3Level).length };
}
export function mergeEventMonths(payloads: LooseDict[], range: EventRange): LooseDict {
  const unique = new Map<string, LooseDict>();
  let invalid = 0;
  for (const payload of payloads) {
    if (payload.config_missing) throw new Error(String(payload.config_error || '事件数据源尚未配置。'));
    if (payload.snapshot_exists === false) throw new Error('所选时间范围的事件快照尚未准备好，请点击“刷新事件”后重试。');
    if (!Array.isArray(payload.records)) throw new Error('事件列表未完整返回，请重试。');
    for (const record of payload.records) {
      const time = occurrenceTimestamp(record.occurrence_time);
      if (!Number.isFinite(time)) { invalid++; continue; }
      if (time < range.start || time >= range.end) continue;
      // Source record IDs, never titles, identify the same event across monthly snapshots.
      const key = eventRecordId(record) || `unidentified:${unique.size}`;
      if (!unique.has(key)) unique.set(key, record);
    }
  }
  const records = [...unique.values()].sort((a, b) => occurrenceTimestamp(b.occurrence_time) - occurrenceTimestamp(a.occurrence_time));
  const stamps = payloads.map(p => Number(p.last_refreshed_at || 0));
  return { records, stats: eventPeriodStats(records),
    unknown_time_count: Math.max(invalid, ...payloads.map(p => Number(p.unknown_time_count || 0))),
    last_refreshed_at: stamps.length ? Math.min(...stamps) : 0,
    last_failed: payloads.find(p => p.last_failed?.error)?.last_failed || {} };
}
