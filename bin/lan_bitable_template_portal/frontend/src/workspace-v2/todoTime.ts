import { beijingDate } from './modules';

export function todoTimeLabel(value: string, full = false): string {
  const text = String(value || '').trim();
  if (!text) return '';
  // Date-only business values have no implicit time or timezone.
  const date = /^\d{4}-\d{2}-\d{2}$/.test(text);
  const timestamp = /^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:?\d{2})?$/i.test(text);
  if (!date && !timestamp) return '时间待核对';
  const calendar = new Date(text.slice(0, 10) + 'T12:00:00Z');
  if (!Number.isFinite(calendar.getTime()) || calendar.toISOString().slice(0, 10) !== text.slice(0, 10)) return '时间待核对';
  const zoned = date ? text + 'T12:00:00+08:00' : /(?:Z|[+-]\d{2}:?\d{2})$/i.test(text) ? text.replace(' ', 'T') : text.replace(' ', 'T') + '+08:00';
  const time = new Date(zoned);
  if (!Number.isFinite(time.getTime())) return '时间待核对';
  const parts = Object.fromEntries(new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Shanghai', year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', hourCycle: 'h23',
  }).formatToParts(time).map(part => [part.type, part.value]));
  const day = `${parts.year}-${parts.month}-${parts.day}`;
  if (date) return day === text ? day : '时间待核对';
  const label = `${full || parts.year !== beijingDate().slice(0, 4) ? parts.year + '-' : ''}${parts.month}-${parts.day} ${parts.hour}:${parts.minute}`;
  return label + (full ? '（北京时间）' : '');
}
