import type { Dict } from './client';

export function noticeSourceMode(row: Dict): { label: string; tone: 'info' | 'warning'; bound: boolean } {
  const source = String(row.source_record_id || '').trim();
  const target = String(row.target_record_id || '').trim();
  const bound = !!source && !/^(?:local_|manual:)/.test(source) && source !== target;
  return { label: bound ? '源表事项' : '纯手填', tone: bound ? 'info' : 'warning', bound };
}

export function noticeListStatus(row: Dict, lane: string): { label: string; tone: 'default' | 'info' | 'warning' | 'error' } {
  if (lane !== 'ongoing') {
    return { label: String(row.source_progress || row.status_label || row.status || '未开始'), tone: 'default' };
  }
  // The active notice status is independent of its source plan's progress.
  const state = ['status', 'phase', 'error', 'last_error', 'failure_reason'].map(key => String(row[key] || '')).join(' ');
  if (/失败|错误|异常/.test(state) || row.phase === 'failed') return { label: '发送失败', tone: 'error' };
  if (/待处理|需处理|重试/.test(state)) return { label: '需处理', tone: 'warning' };
  return { label: '进行中', tone: 'info' };
}

export function noticeListCycle(row: Dict): string {
  const fields = row.display_fields || {};
  const cycle = [row.maintenance_cycle, fields['维保周期'], fields['维护周期']]
    .map(value => String(value ?? '').trim()).find(Boolean);
  return cycle ? '周期 ' + cycle : row.work_type === 'maintenance' ? '周期未填' : '';
}

export function noticeListSpecialty(row: Dict): string {
  const fields = row.display_fields || {};
  return [row.specialty, fields['专业类别'], fields['专业'], fields['所属专业']]
    .map(value => String(value ?? '').trim()).find(Boolean) || '';
}

export function noticePlanWindow(row: Dict): { start: string; end: string } {
  const fields = row.display_fields || {};
  const first = (...values: unknown[]) => values.map(value => String(value ?? '').trim()).find(Boolean) || '';
  const change = row.work_type === 'change';
  // Use plan fields only: a send/update time or remembered draft is not a plan window.
  const start = first(change && fields['变更开始日期（阿里）'] || '', change && fields['计划开始日期（阿里）'] || '',
    fields['计划开始维护时间'], fields['计划开始'], fields['计划开始时间'], fields['计划延迟开始日期'],
    row.plan_start_time, row.planned_start_time, row.plan_start);
  const end = first(change && fields['变更结束日期（阿里）'] || '', change && fields['计划结束日期（阿里）'] || '',
    fields['计划结束维护时间'], fields['计划结束'], fields['计划结束时间'], fields['计划延迟结束日期'],
    row.plan_end_time, row.planned_end_time, row.plan_end);
  const display = (value: string) => value.replace(/(\d)T(?=\d)/, '$1 ').replace(/^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}):00$/, '$1');
  return { start: display(start), end: display(end) };
}
