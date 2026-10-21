import type { Dict } from '../api/client';

export type RiskTaskView = 'published' | 'mine' | 'review';
export const riskTaskView = (value: string | null): RiskTaskView | null => value === 'published' || value === 'mine' || value === 'review' ? value : null;

export const riskTypeOptions = [
  { value: 1, label: '文本' }, { value: 2, label: '数字' }, { value: 3, label: '单选' },
  { value: 4, label: '多选' }, { value: 5, label: '日期' }, { value: 7, label: '勾选' },
  { value: 13, label: '电话' }, { value: 15, label: '链接' }, { value: 17, label: '附件 / 照片' },
];
export const riskRoles: Record<string, string> = {
  input: '排查人填写', number: '任务编号', status: '排查进度', scope: '楼栋', person: '排查人',
  reviewer: '复核人', review: '复核结果', reviewed_at: '复核日期', sync: '最终推送', inspected_at: '排查日期',
};
export const riskFieldRole = (field: Dict): string => field.role === 'input' && field.field_name === '排查日期' ? 'inspected_at' : field.role;
export const riskSystemField = (field: Dict): boolean => riskFieldRole(field) !== 'input' && Object.prototype.hasOwnProperty.call(riskRoles, riskFieldRole(field));
export const riskPresentationFields = (fields: Dict[]): Dict[] => fields.map(field => riskFieldRole(field) === field.role ? field : { ...field, role: riskFieldRole(field) });
const concreteScopes = new Set(['A', 'B', 'C', 'D', 'E', 'H', '110']);
export function riskRecipientScopes(person: Dict): string[] {
  return [...new Set<string>((person.scopes || []).filter((scope: string) => concreteScopes.has(scope)))];
}
export function riskRecipientScope(person: Dict): string | null {
  const scopes = riskRecipientScopes(person);
  return scopes.includes(person.home_scope) ? person.home_scope : scopes.length === 1 ? scopes[0]! : null;
}
export const riskAssignedScopes = (person: Dict): string[] => Array.isArray(person.scopes) ? person.scopes : person.scope ? [person.scope] : [];
export const riskFinalScopes = (person: Dict): string[] => Array.isArray(person.final_scopes) ? person.final_scopes : person.scopes || [];
export const riskReviewerIds = (task: Dict): string[] => Array.isArray(task.reviewer_ids) ? task.reviewer_ids : task.reviewers?.length ? task.reviewers.map((person: Dict) => person.id) : task.reviewer_id ? [task.reviewer_id] : [];
export const riskReviewerNames = (task: Dict): string => task.reviewers?.length ? task.reviewers.map((person: Dict) => person.name).join('、') : task.reviewer || '';
export function riskSystemText(field: Dict, values: Dict = {}): string {
  const value = values[field.key], role = riskFieldRole(field);
  if (value == null || value === '') return ({ inspected_at: '提交后自动生成', reviewed_at: '复核后自动生成', review: '待复核', sync: '待推送', reviewer: '待分配' } as Record<string, string>)[role] || '—';
  if (['inspected_at', 'reviewed_at'].includes(role) || field.type === 5) {
    const date = new Date(value);
    return Number.isFinite(date.getTime()) ? new Intl.DateTimeFormat('zh-CN', { timeZone: 'Asia/Shanghai', dateStyle: 'short', timeStyle: 'short' }).format(date) : '日期无效';
  }
  return Array.isArray(value) ? value.join('、') : String(value);
}
export const riskTypeName = (type: number) => riskTypeOptions.find(option => option.value === type)?.label || '不支持的类型';
export const riskRoleName = (role: string) => role === 'input' ? riskRoles.input : '系统填写 · ' + (riskRoles[role] || role);

export function reviewerIssue(ids: string[], people: Dict[]): string {
  return !Array.isArray(ids) || !ids.length || new Set(ids).size !== ids.length || ids.some(id => typeof id !== 'string' || !people.some(person => person.id === id && person.active !== false)) ? '请选择至少1位有效的在岗审核人' : '';
}

const empty = (value: unknown) => value == null || typeof value === 'string' && !value.trim() || Array.isArray(value) && !value.length;
export const missingRiskFields = (fields: Dict[], values: Dict) => fields.filter(field => riskFieldRole(field) === 'input' && field.required && empty(values[field.key]));
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
  const inputs = fields.filter(field => riskFieldRole(field) === 'input');
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
  if (new Set(selected.map(person => person.id)).size !== selected.length || selected.some(row => { const person = people.find(person => person.id === row.id), scopes = riskAssignedScopes(row); return !person || person.active === false || !scopes.length || new Set(scopes).size !== scopes.length || scopes.some(scope => !riskRecipientScopes(person).includes(scope)); })) issues.push('接收人员的有效楼栋');
  const reviewer = reviewerIssue(riskReviewerIds(publish), people);
  if (reviewer) issues.push(reviewer);
  const managers: string[] = publish.manager_ids || [];
  const chosenManagers = people.filter(person => managers.includes(person.id) && person.can_receive_final === true);
  const scopes = selected.flatMap(riskAssignedScopes);
  if (!managers.length || managers.length > 50 || new Set(managers).size !== managers.length || managers.some(id => !chosenManagers.some(person => person.id === id && scopes.some(scope => riskFinalScopes(person).includes(scope))))) issues.push('1至50位有效的接收经理');
  if (managers.length && scopes.some(scope => !chosenManagers.some(person => riskFinalScopes(person).includes(scope)))) issues.push('接收经理需覆盖全部接收楼栋');
  if (!publish.items?.length || publish.items.length > 100) issues.push('1至100项排查事项');
  else publish.items.forEach((item: Dict, index: number) => { if (!item.title?.trim()) issues.push(`第${index + 1}项的事项名称`); });
  if (fields.some(field => field.unsupported)) issues.push('先调整模板中不支持的字段类型');
  return [...issues, ...prefillIssues(fields, publish.items || [])];
}
