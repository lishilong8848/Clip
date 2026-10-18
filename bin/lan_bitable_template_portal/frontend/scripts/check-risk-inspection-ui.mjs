import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import ts from 'typescript';

const source = await readFile(new URL('../src/components/riskInspectionUi.ts', import.meta.url), 'utf8');
const code = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2022 } }).outputText;
const { publicationIssues, prefillIssues, reviewerIssue, missingRiskFields, riskTaskPriority, riskFeedbackPriority } = await import('data:text/javascript;base64,' + Buffer.from(code).toString('base64'));
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
const required = [...fields.map(field => ({...field, required:true})), {key:'photo',field_name:'照片',role:'input',type:17,required:true}, {key:'date',field_name:'日期',role:'input',type:5,required:true}];
const answers = {text:'现场正常',number:0,checked:false,level:'正常',link:'https://example.test',multi:['甲'],photo:['a'.repeat(32)],date:0};
assert.deepEqual(missingRiskFields(required, answers), [], 'Zero, false, date zero and existing attachments are complete; system fields are not required');
assert.deepEqual(missingRiskFields(required, {...answers,text:'  ',photo:[],multi:[]}).map(field => field.key), ['text','multi','photo']);
assert.deepEqual(missingRiskFields(fields, {} ).map(field => field.key), ['text'], 'Optional fields stay optional');
assert.deepEqual(answers.photo, ['a'.repeat(32)], 'Validation does not alter uploaded file references');
const tasks = [
  {id:'done',status:'active',total:2,submitted:2,approved:2,deadline:500},
  {id:'review',status:'active',total:2,submitted:2,approved:0,deadline:2000},
  {id:'waiting',status:'active',total:2,submitted:0,approved:0,deadline:2000},
  {id:'overdue',status:'active',total:2,submitted:0,approved:0,deadline:500},
];
assert.deepEqual([...tasks].sort((a,b)=>riskTaskPriority(a,1000)-riskTaskPriority(b,1000)).map(task=>task.id), ['overdue','waiting','review','done']);
assert.equal(tasks[0].id,'done','Display sorting leaves source records untouched');
const states=['approved','submitted','pending','returned','revoked'];
assert.deepEqual([...states].sort((a,b)=>riskFeedbackPriority({status:a},false,false)-riskFeedbackPriority({status:b},false,false)), ['returned','pending','submitted','approved','revoked']);
assert.deepEqual([...states].sort((a,b)=>riskFeedbackPriority({status:a},true,false)-riskFeedbackPriority({status:b},true,false)), ['submitted','returned','pending','approved','revoked']);
assert.equal(riskFeedbackPriority({status:'pending'},false,true),4,'Revoked assignees are never promoted as pending work');
console.log('PASS risk validation and priority: reviewer scopes, missing required answers, zero/false, attachments, draft preservation and pending/review order. No API calls.');
