import type { Dict } from '../api/client';

export const riskTypeOptions = [
  { value: 1, label: '文本' }, { value: 2, label: '数字' }, { value: 3, label: '单选' },
  { value: 4, label: '多选' }, { value: 5, label: '日期' }, { value: 7, label: '勾选' },
  { value: 13, label: '电话' }, { value: 15, label: '链接' }, { value: 17, label: '附件 / 照片' },
];
export const riskRoles: Record<string, string> = {
  input: '排查人填写', number: '任务编号', status: '排查状态', scope: '楼栋', person: '排查人',
  reviewer: '复核人', review: '审核意见', reviewed_at: '复核日期', sync: '落表状态',
};
export const riskTypeName = (type: number) => riskTypeOptions.find(option => option.value === type)?.label || '不支持的类型';
export const riskRoleName = (role: string) => role === 'input' ? riskRoles.input : '系统填写 · ' + (riskRoles[role] || role);

export function reviewerIssue(person: Dict | undefined, recipients: Dict[]): string {
  if (!person?.can_review) return '请选择有审核权限的人员';
  if (recipients.some(row => row.id === person.id)) return '审核人不能同时作为接收人员';
  const missing = [...new Set(recipients.map(row => row.scope).filter(scope => !person.scopes?.includes(scope)))];
  return missing.length ? '审核人无' + missing.map(scope => scope === '110' ? '110站' : scope + '楼').join('、') + '权限' : '';
}

const empty = (value: unknown) => value == null || typeof value === 'string' && !value.trim() || Array.isArray(value) && !value.length;
export const missingRiskFields = (fields: Dict[], values: Dict) => fields.filter(field => field.role === 'input' && field.required && empty(values[field.key]));
export function riskTaskPriority(task: Dict, now = Date.now()): number {
  if (task.status !== 'active' || !(task.total > task.approved)) return 3;
  return task.deadline < now ? 0 : task.submitted < task.total ? 1 : 2;
}
export function riskFeedbackPriority(item: Dict, reviewer: boolean, revoked: boolean): number {
  if (revoked || item.status === 'revoked') return 4;
  if (reviewer && item.status === 'submitted') return -1;
  return ({ returned: 0, pending: 1, submitted: 2, approved: 3 } as Record<string, number>)[item.status] ?? 3;
}
export function prefillIssues(fields: Dict[], items: Dict[]): string[] {
  const inputs = fields.filter(field => field.role === 'input');
  const inputKeys = new Set(inputs.map(field => field.key));
  const issues: string[] = [];
  items.forEach((item, index) => {
    if (Object.keys(item.values || {}).some(key => !inputKeys.has(key))) issues.push(`第${index + 1}项含有已移除或非填写字段`);
    for (const field of inputs) {
      const value = item.values?.[field.key];
      if (empty(value)) continue; // Required answers are collected from the assignee, not the publisher.
      const options = (field.property?.options || []).map((option: Dict) => option.name);
      const valid = [1, 13].includes(field.type) ? typeof value === 'string' && value.length <= 10000
        : field.type === 15 ? typeof value === 'string' && value.length <= 2000 && /^https?:\/\/[^\s]+$/.test(value)
        : [2, 5].includes(field.type) ? typeof value === 'number' && Number.isFinite(value) && (field.type !== 5 || value >= 0 && value <= 32503680000000)
        : field.type === 7 ? typeof value === 'boolean'
        : field.type === 3 ? typeof value === 'string' && options.includes(value)
        : field.type === 4 ? Array.isArray(value) && value.length <= 100 && value.every(option => options.includes(option))
        : false;
      if (!valid) issues.push(`第${index + 1}项「${field.field_name}」预填值无效`);
    }
  });
  return issues;
}

export function publicationIssues(publish: Dict, fields: Dict[], people: Dict[], now = Date.now()): string[] {
  const issues: string[] = [];
  if (!publish.name?.trim()) issues.push('排查表名');
  if (!Number.isFinite(publish.deadline) || publish.deadline <= now || publish.deadline > 32503680000000) issues.push('晚于当前时间的反馈截止时间');
  const selected: Dict[] = publish.people || [];
  if (!selected.length || selected.length > 500) issues.push('1至500名接收人员');
  if (new Set(selected.map(person => person.id)).size !== selected.length || selected.some(row => !people.find(person => person.id === row.id)?.scopes?.includes(row.scope))) issues.push('接收人员的有效楼栋');
  const reviewer = reviewerIssue(people.find(person => person.id === publish.reviewer_id), selected);
  if (reviewer) issues.push(reviewer);
  if (!publish.items?.length || publish.items.length > 100) issues.push('1至100项排查事项');
  else publish.items.forEach((item: Dict, index: number) => { if (!item.title?.trim()) issues.push(`第${index + 1}项的事项名称`); });
  if (fields.some(field => field.unsupported)) issues.push('先调整模板中不支持的字段类型');
  return [...issues, ...prefillIssues(fields, publish.items || [])];
}
