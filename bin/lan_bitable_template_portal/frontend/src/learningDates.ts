function isIsoDay(value: string): boolean {
  if (!/^(?!0000)\d{4}-\d{2}-\d{2}$/.test(value)) return false;
  const date = new Date(`${value}T00:00:00Z`);
  return Number.isFinite(date.getTime()) && date.toISOString().slice(0, 10) === value;
}

export function completionDateError(from = '', to = '', allowEmpty = false): string {
  const dates = allowEmpty ? [from, to].filter(value => value !== '') : [from, to];
  if (dates.some(value => !isIsoDay(value))) return '请选择有效统计日期。';
  if (from === '' || to === '') return '';
  const days = (Date.parse(to) - Date.parse(from)) / 86400000;
  return days < 0 || days > 30 ? '一次最多查询31天，开始日期不能晚于结束日期。' : '';
}
