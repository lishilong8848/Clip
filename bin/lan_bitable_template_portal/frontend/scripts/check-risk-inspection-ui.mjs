import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import ts from 'typescript';

const source = await readFile(new URL('../src/components/riskInspectionUi.ts', import.meta.url), 'utf8');
const code = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2022 } }).outputText;
const { publicationIssues, prefillIssues, reviewerIssue, missingRiskFields, riskTaskPriority, riskFeedbackPriority, riskRecipientScope, riskRecipientScopes, riskPresentationFields, riskSystemField, riskSystemText, riskReviewerIds, riskReviewerNames, riskAssignedScopes, riskFinalScopes } = await import('data:text/javascript;base64,' + Buffer.from(code).toString('base64'));
const fields = [
  { key: 'text', field_name: '现场情况', type: 1, role: 'input', required: true },
  { key: 'number', field_name: '数量', type: 2, role: 'input' },
  { key: 'checked', field_name: '有异常', type: 7, role: 'input' },
  { key: 'level', field_name: '风险等级', type: 3, role: 'input', property: { options: [{ name: '正常' }, { name: '异常' }] } },
  { key: 'link', field_name: '链接', type: 15, role: 'input' },
  { key: 'multi', field_name: '设备', type: 4, role: 'input', property: { options: [{ name: '甲' }, { name: '乙' }] } },
  { key: 'system', field_name: '审核人', type: 1, role: 'reviewer' },
];
const people = [{ id: 'recipient', scopes: ['A'], home_scope: 'A' }, { id: 'reviewer', scopes: ['A', 'B'], can_review: true }, { id: 'manager', category: 'manager', can_receive_final: true, scopes: ['A'] }];
const publish = { name: '隔离检查', deadline: 2000, reviewer_id: 'reviewer', manager_ids: ['manager'], people: [{ id: 'recipient', scope: 'A' }], items: [{ title: '演示事项', values: {} }] };
assert.deepEqual(publicationIssues(publish, fields, people, 1000), [], 'Required answers are not required publisher prefills');
assert.deepEqual(prefillIssues(fields, [{ values: { number: 0, checked: false, multi: ['乙', '甲'] } }]), [], 'False, zero and unordered multiple selections remain valid');
assert.deepEqual(publish.items[0].values, {}, 'Validation must not change the draft');
assert.equal(reviewerIssue(['recipient'], people),'','Recipients and reviewers can be the same active person without prior permissions');
assert.equal(reviewerIssue(['reviewer','recipient'], people),'');
for(const ids of [[],['unknown'],['reviewer','unknown'],['reviewer','reviewer'],[null]])assert.match(reviewerIssue(ids,people),/至少1位有效/);
assert.match(reviewerIssue(['recipient'],[{...people[0],active:false}]),/至少1位有效/);
assert.deepEqual(riskReviewerIds({reviewer_id:'old'}),['old']);
assert.deepEqual(riskReviewerIds({reviewer_ids:['a','b'],reviewer_id:'old'}),['a','b']);
assert.equal(riskReviewerNames({reviewers:[{id:'a',name:'甲'},{id:'b',name:'乙'}],reviewer:'旧'}),'甲、乙');
assert.deepEqual(riskAssignedScopes({scope:'A'}),['A']);
assert.deepEqual(riskAssignedScopes({scopes:['A','B'],scope:'A'}),['A','B']);
assert.deepEqual(riskFinalScopes({scopes:['A','B'],final_scopes:[]}),[],'An explicit empty manager scope must not fall back to publishing scopes');
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
assert.equal(riskRecipientScope({home_scope:'B',scopes:['A','B']}),'B');
assert.equal(riskRecipientScope({home_scope:'B',scopes:['A']}),'A','Home only supplies a default inside publisher-authorized scopes');
assert.equal(riskRecipientScope({scopes:['A','B']}),null,'Multiple scopes must be explicitly chosen');
assert.equal(riskRecipientScope({scopes:['*','A']}),'A','Only concrete scopes are selectable');
assert.deepEqual(riskRecipientScopes({scopes:['*','all']}),[]);
assert.deepEqual(riskRecipientScopes({home_scope:'A',scopes:['A','B']}),['A','B'],'Home never locks the selectable buildings');
const multiPeople=[{...people[0],scopes:['A','B']},people[1],{...people[2],scopes:['A','B'],final_scopes:['A']},{id:'managerB',scopes:['A','B'],final_scopes:['B'],can_receive_final:true}];
const multiPublish={...publish,reviewer_ids:['recipient','reviewer'],people:[{id:'recipient',scopes:['A','B']}]};
assert(publicationIssues(multiPublish,fields,multiPeople,1000).includes('接收经理需覆盖全部接收楼栋'),'Publisher scopes cannot make manager A cover B');
assert.deepEqual(publicationIssues({...multiPublish,manager_ids:['manager','managerB']},fields,multiPeople,1000),[],'All active reviewers can cover a multi-building assignee including themselves');
for(const scopes of [[],['A','A'],['A','C']])assert(publicationIssues({...multiPublish,people:[{id:'recipient',scopes}]},fields,multiPeople,1000).includes('接收人员的有效楼栋'));
for(const manager_ids of [[],['recipient'],['manager','manager'],['unknown']])assert(publicationIssues({...publish,manager_ids},fields,people,1000).some(issue=>issue.includes('接收经理')));
assert(publicationIssues(publish,fields,[...people.slice(0,2),{...people[2],scopes:['B']}],1000).some(issue=>issue.includes('接收经理')));
const crossBuilding={...publish,people:[...publish.people,{id:'other',scope:'B'}]};
const withOther=[...people,{id:'other',home_scope:'B',scopes:['B']},{id:'managerB',scopes:['B'],can_receive_final:true}];
assert(publicationIssues(crossBuilding,fields,withOther,1000).includes('接收经理需覆盖全部接收楼栋'));
assert.deepEqual(publicationIssues({...crossBuilding,manager_ids:['manager','managerB']},fields,withOther,1000),[],'Selected managers jointly cover all recipient buildings');
assert.equal(riskRecipientScope({home_scope:'H',scopes:['A','H']}),'H');
const legacyDate=Object.freeze({key:'legacy_date',field_name:'排查日期',role:'input',type:5,required:true});
assert.equal(riskPresentationFields([legacyDate])[0].role,'inspected_at');
assert.equal(legacyDate.role,'input','Presentation does not modify the historical schema');
assert(riskSystemField(legacyDate));
assert.deepEqual(missingRiskFields([legacyDate],Object.freeze({})),[]);
assert.equal(riskSystemText(legacyDate,{}),'提交后自动生成');
assert.equal(riskSystemText({key:'actual_review',role:'reviewed_at'},{}),'复核后自动生成');
assert.equal(riskSystemText({key:'progress',role:'status'},{progress:'50%'}),'50%');
assert.equal(riskSystemText({key:'push',role:'sync'},{push:'等待经理推送'}),'等待经理推送');
assert.match(riskSystemText(legacyDate,Object.freeze({legacy_date:1792152000000})),/2026.*10.*16.*20:00/,'System dates are milliseconds in Shanghai time');
console.log('PASS risk validation: active multi-reviewers and self-review, publisher-authorized multi-buildings, final manager scope coverage, legacy responses, zero/false, attachments and draft preservation. No API calls.');
