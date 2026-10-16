import { eventI2OrHigher, eventI3Level, eventRecordId, eventUnderRepair } from '../eventManagementUtils';
import type { LooseDict } from '../types';

export type EventPeriod = 'week' | 'month' | 'history' | 'custom';
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
export function eventPeriodRange(period: EventPeriod, month = '', now = Date.now(), dates?: [string, string]) {
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
  let error = '';
  if (period === 'custom') {
    const [from, to] = dates || [currentMonth + '-01', today];
    start = /^\d{4}-\d{2}-\d{2}$/.test(from) ? occurrenceTimestamp(from) : NaN;
    end = /^\d{4}-\d{2}-\d{2}$/.test(to) ? occurrenceTimestamp(to) + DAY : NaN;
    if (!Number.isFinite(start) || !Number.isFinite(end)) error = '请选择有效的开始日期和结束日期。';
    else if (end <= start) error = '开始日期不能晚于结束日期。';
    else if (end - start > 366 * DAY) error = '单次最多查询366天，请缩小日期区间。';
  }
  if (error) return { start: 0, end: 0, months: [] as string[], label: '自定义', caption: '自定义日期待确认', error };
  const first = beijingDate(start), last = beijingDate(end - 1);
  const months: string[] = [];
  const [firstYear, firstMonth] = first.slice(0, 7).split('-').map(Number);
  for (let cursor = Date.UTC(firstYear!, firstMonth! - 1, 1) - OFFSET; cursor < end;) {
    months.push(beijingDate(cursor).slice(0, 7));
    const date = new Date(cursor + OFFSET);
    cursor = Date.UTC(date.getUTCFullYear(), date.getUTCMonth() + 1, 1) - OFFSET;
  }
  return { start, end, months, label: period === 'week' ? '本周' : period === 'month' ? '本月' : period === 'custom' ? '自定义' : `${year}年${m}月`,
    caption: `${first.replace(/-/g, '.')} - ${last.replace(/-/g, '.')}`, error };
}
export type EventRange = ReturnType<typeof eventPeriodRange>;
export function eventCalendarRange(period: 'week'|'month'|'year', now = Date.now()) {
  const today=beijingDate(now);
  const first=period==='year'?today.slice(0,4)+'-01-01':beijingDate(eventPeriodRange(period,'',now).start);
  return eventPeriodRange('custom','',now,[first,today]);
}
export function eventTrend(records: LooseDict[], range: EventRange, daily = false) {
  const step = (daily ? 1 : Math.max(1, Math.ceil((range.end - range.start) / DAY / 62))) * DAY;
  const bins: Array<{label:string;value:number;start:number;end:number}> = [];
  for(let start=range.start; start<range.end; start+=step) {
    const end=Math.min(start+step,range.end), first=beijingDate(start).slice(5).replace('-', '/');
    const label = step === DAY ? first : first + '至' + beijingDate(end - 1).slice(5).replace('-', '/');
    bins.push({label,value:0,start,end});
  }
  for(const item of records){const time=occurrenceTimestamp(item.occurrence_time);if(time>=range.start&&time<range.end){const bin=bins[Math.floor((time-range.start)/step)];if(bin)bin.value++;}}
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
