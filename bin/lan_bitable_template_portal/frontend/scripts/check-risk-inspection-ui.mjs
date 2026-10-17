import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import ts from 'typescript';

const source = await readFile(new URL('../src/components/riskInspectionUi.ts', import.meta.url), 'utf8');
const code = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2022 } }).outputText;
const { publicationIssues, prefillIssues, reviewerIssue } = await import('data:text/javascript;base64,' + Buffer.from(code).toString('base64'));
const fields = [
  { key: 'text', field_name: '现场情况', type: 1, role: 'input', required: true },
  { key: 'number', field_name: '数量', type: 2, role: 'input' },
  { key: 'checked', field_name: '有异常', type: 7, role: 'input' },
  { key: 'level', field_name: '风险等级', type: 3, role: 'input', property: { options: [{ name: '正常' }, { name: '异常' }] } },
  { key: 'link', field_name: '链接', type: 15, role: 'input' },
  { key: 'multi', field_name: '设备', type: 4, role: 'input', property: { options: [{ name: '甲' }, { name: '乙' }] } },
  { key: 'system', field_name: '审核人', type: 1, role: 'reviewer' },
];
const people = [{ id: 'recipient', scopes: ['A'] }, { id: 'reviewer', scopes: ['A', 'B'], can_review: true }];
const publish = { name: '隔离检查', deadline: 2000, reviewer_id: 'reviewer', people: [{ id: 'recipient', scope: 'A' }], items: [{ title: '演示事项', values: {} }] };
assert.deepEqual(publicationIssues(publish, fields, people, 1000), [], 'Required answers are not required publisher prefills');
assert.deepEqual(prefillIssues(fields, [{ values: { number: 0, checked: false, multi: ['乙', '甲'] } }]), [], 'False, zero and unordered multiple selections remain valid');
assert.deepEqual(publish.items[0].values, {}, 'Validation must not change the draft');
assert.match(reviewerIssue({ id: 'recipient', scopes: ['A'], can_review: true }, publish.people), /不能同时/);
assert.match(reviewerIssue({ id: 'reviewer', scopes: ['B'], can_review: true }, publish.people), /无A楼权限/);
assert.match(reviewerIssue(undefined, []), /审核权限/);
assert(publicationIssues({ ...publish, deadline: 1000 }, fields, people, 1000).some(issue => issue.includes('截止时间')));
assert(publicationIssues({ ...publish, people: [{ id: 'recipient', scope: 'B' }] }, fields, people, 1000).some(issue => issue.includes('有效楼栋')));
assert(publicationIssues({ ...publish, people: [...publish.people, ...publish.people] }, fields, people, 1000).some(issue => issue.includes('有效楼栋')));
assert(publicationIssues({ ...publish, items: [] }, fields, people, 1000).some(issue => issue.includes('1至100')));
assert(publicationIssues({ ...publish, items: Array.from({ length: 101 }, () => publish.items[0]) }, fields, people, 1000).some(issue => issue.includes('1至100')));
assert(prefillIssues(fields, [{ values: { system: '不能伪造审核人' } }]).some(issue => issue.includes('非填写字段')));
for (const values of [{ number: Infinity }, { checked: 'false' }, { level: '未知' }, { multi: ['丙'] }, { link: 'javascript:alert(1)' }]) {
  assert(prefillIssues(fields, [{ values }]).length, JSON.stringify(values));
}
assert(publicationIssues(publish, [...fields, { key: 'formula', unsupported: true }], people, 1000).some(issue => issue.includes('不支持')));
console.log('PASS risk publication validation: reviewer scopes/self-review, missing data, bounds, zero/false, options, URLs and system fields. No API calls.');
