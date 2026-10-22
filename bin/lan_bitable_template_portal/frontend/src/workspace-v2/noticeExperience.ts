import type { Dict } from './client';
import type { NoticeForm } from './noticeForm';
import { noticeCycleWarnings } from '../noticeCycleWarnings';

const buildingCodes = ['110', ...'ABCDEH'];
export function noticeBuildings(row: Dict): string[] {
  const explicit = Array.isArray(row.building_codes) ? row.building_codes : [];
  const values = explicit.length ? explicit : String(row.building || row.scope || '').toUpperCase().split(/[、,，/\s]+/);
  return [...new Set(values.flatMap((v: unknown) => {
    const text = String(v).replace(/^南通/, '').replace(/楼|栋|站/g, '');
    if (buildingCodes.includes(text)) return [text];
    if (/^[ABCDEH]+$/.test(text)) return [...text];
    if (/园区|CAMPUS/.test(text)) return [...'ABCDE'];
    return [];
  }))];
}
export function noticeInScope(row: Dict, scope: string): boolean {
  if (scope === 'ALL') return true;
  const codes = noticeBuildings(row);
  return codes.some(code => (scope === 'CAMPUS' ? [...'ABCDE'] : [scope]).includes(code));
}

// Only stable, sanitized recommendations from the existing prefill API are eligible.
const recommendationKeys: Record<string, string[]> = {
  maintenance: ['location', 'content', 'reason', 'impact', 'specialty', 'execution_party'],
  change: ['location', 'content', 'reason', 'impact', 'specialty', 'execution_party', 'level'],
  repair: ['location', 'content', 'reason', 'impact', 'specialty', 'repair_mode', 'solution'],
  polling: ['content', 'impact', 'specialty'], adjust: ['location', 'content', 'reason', 'impact', 'specialty'], power: ['specialty'],
};
export function recommendationFields(form: NoticeForm, candidate: Dict, touched: Set<string> = new Set()) {
  const keys = recommendationKeys[form.values.work_type] || [];
  return form.fields.filter(field => !field.readonly && keys.includes(field.key) && !touched.has(field.key)
    && !String(form.values[field.key] ?? '').trim()
    && typeof candidate.suggested_values?.[field.key] === 'string' && candidate.suggested_values[field.key].trim()
    && (!field.options.length || field.type !== 'select-one' || field.options.some(o => o.value === candidate.suggested_values[field.key])))
    .sort((a, b) => Number(b.required) - Number(a.required));
}
export type NoticeIssue = { message: string; target: string; label: string };
export function noticeIssueTarget(form: NoticeForm, action: string, sop: Dict, photos: Dict[]): Omit<NoticeIssue, 'message'> {
  const cycleField=Object.keys(noticeCycleWarnings(form.values,form.cycleRules))[0];
  if(cycleField)return {target:'field:'+cycleField,label:'修改周期冲突'};
  const missing = form.fields.find(f => f.required && !String(form.values[f.key] ?? '').trim());
  if (missing) return { target: 'field:' + missing.key, label: '填写' + missing.label };
  if (!form.values.building_codes?.length) return { target: 'buildings', label: '选择楼栋' };
  if (form.values.manual_binding_required && action === 'start' && (form.values.manual_binding_choice !== 'unbound' && !form.values.source_record_id || !['bind', 'unbound'].includes(form.values.manual_binding_choice))) return { target: 'binding', label: '确认计划关联' };
  const time = (v: unknown) => Date.parse(String(v || '').replace(' ', 'T') + '+08:00');
  const v = form.values, start = time(v.work_type === 'repair' ? v.end_time : v.start_time), end = time(v.work_type === 'repair' ? v.start_time : v.end_time);
  if (Number.isFinite(start) && Number.isFinite(end) && end - start < 3600000) return { target: 'field:start_time', label: '修改时间' };
  if (v.work_type === 'repair' && action === 'start') {
    const prefix = 'EA118_C01机房' + ([...'ABCDEH'].includes(v.scope) ? v.scope + '楼' : '');
    if (!v.title?.startsWith(prefix) || !v.title.endsWith('检修') || !v.title.slice(prefix.length, -2).trim()) return { target: 'field:title', label: '修改标题' };
  }
  if (action === 'end' && form.photoRequired && Number(v.site_photo_count || 0) + photos.length === 0) return { target: 'photos', label: '补充现场照片' };
  if (form.sop && action === 'start' && !sop.exempt) return { target: 'sop', label: '选择工单及人员' };
  return { target: 'action', label: '核对本次操作' };
}
