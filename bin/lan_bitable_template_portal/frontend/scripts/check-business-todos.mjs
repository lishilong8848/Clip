import assert from 'node:assert/strict';
import { build } from 'esbuild';
const bundle=await build({entryPoints:['src/workspace-v2/businessTodos.ts'],bundle:true,write:false,format:'esm',platform:'node'});
const {todoOptions,todoSelection,todoSources,sortTodos}=await import('data:text/javascript;base64,'+Buffer.from(bundle.outputFiles[0].text).toString('base64'));
const user={open_id:'alice',role:'building',modules:['maintenance','repair_management','critical_guard','risk_inspection','water','learning','signatures']};
const ctx={user,scope:'ALL',scopes:['ALL','A','B'].map(value=>({value,label:value}))};
const signal=new AbortController().signal;
assert.deepEqual(todoSelection(undefined,user),['notices','repair_management','critical_guard','risk_inspection','water','learning','signatures']);
assert.deepEqual(todoSelection(['water','notices','water','accounts'],user),['water','notices']);
assert.deepEqual(todoSelection([],user),[]);
assert.deepEqual(todoOptions({...user,role:'guest'}),[]);
assert.deepEqual(todoSources('water',{...ctx,user:{...user,modules:[]}},()=>assert.fail('unauthorized read')),[]);
const calls=[];
const get=async path=>{
  const url=new URL(path,'http://isolated.test');calls.push(url);
  if(url.pathname==='/api/workbench')return {source_snapshot_ready:true,records_pagination:{total_pages:3},ongoing_pagination:{total_pages:1},records:[
    {record_id:'plan',work_type:'maintenance',title:'未开始计划',source_status:'未开始'},
    {active_item_id:'hidden-linked',source_record_id:'linked',work_type:'maintenance',title:'已关联计划',source_status:'进行中',linked_ongoing:{active_item_id:'active'}},
  ],ongoing:[
    {active_item_id:'active-1',source_record_id:'linked-1',work_type:'maintenance',title:'跨月进行中通告',status:'未结束',deadline:'2026-10-20'},
    {record_id:'completed',work_type:'maintenance',title:'已完成通告',source_status:'已结束'},
    {record_id:'hidden',work_type:'change',title:'无权限变更',status:'未开始'},
    {record_id:'id-only',work_type:'maintenance',title:'仅record_id通告',status:'进行中'},
  ]};
  if(url.pathname==='/api/repair-management/status')return {records:[{record_id:'repair',title:'检修项目',state:'in_progress',progress_percent:40}],total:51};
  if(url.pathname==='/api/critical-guard/tasks')return {tasks:[{task_id:'guard',name:'检查清单',complete:false},{task_id:'done',complete:true}]};
  if(url.pathname==='/api/risk-inspections')return {items:[{id:'r1',name:'未完成',total:2,submitted:1,approved:1,status:'active'},{id:'r2',name:'待审核',total:1,submitted:1,approved:0,status:'active'},{id:'r3',name:'已完',total:1,submitted:1,approved:1,status:'active'},{id:'r4',name:'已撤销',total:0,submitted:0,approved:0,status:'active'}],total:21};
  if(url.pathname==='/api/capacity/water/records')return {records:[{record_id:'empty',meter:'水表1',meter_value:null,photo_count:0},{record_id:'zero',meter_value:0,photo_count:1},{record_id:'photo',meter_value:2,photo_count:0},{record_id:'unknown',meter_value:2,photo_count:null}],total:51,snapshot:{exists:true}};
  if(url.pathname==='/api/learning/bootstrap')return {self_person:{id:'person-alice'},self_scope:'B'};
  if(url.pathname==='/api/learning/papers')return {items:[{id:'paper1',person_id:'person-alice',scope:'A',status:'pending',stats:{total:15,answered:3},date:'2026-10-16'},{id:'other',person_id:'person-bob',status:'pending'},{id:'legacy',legacy:true,status:'pending'},{id:'completed',person_id:'person-alice',status:'completed'}],page_size:20,total:21};
  if(url.pathname==='/api/signatures/management/account')return {person:{is_self:true,building:'B',signature_status:'resign'}};
  assert.fail(path);
};
const notices=todoSources('notices',ctx,get)[0];
const page1=await notices.load(1,signal);assert.equal(page1.more,false,'ongoing pagination is independent of records');assert.deepEqual(page1.rows.map(r=>r.title),['跨月进行中通告','仅record_id通告']);
assert(!page1.rows.some(r=>r.title==='未开始计划'||r.title==='已完成通告'||r.title==='无权限变更'));
const wb=calls.at(-1);assert.equal(wb.searchParams.get('sections'),'ongoing');assert.equal(wb.searchParams.get('ongoing_page'),'1');assert(!wb.searchParams.has('records_page'));
const activeRow=page1.rows.find(r=>r.title==='跨月进行中通告');assert(activeRow.url.includes('active_item_id=active-1'));
const idRow=page1.rows.find(r=>r.title==='仅record_id通告');assert(idRow.url.includes('record_id=id-only'));
const page2=await notices.load(2,signal);assert.equal(page2.more,false);assert.equal(calls.at(-1).searchParams.get('ongoing_page'),'2');assert.equal(calls.at(-1).searchParams.get('sections'),'ongoing');assert.equal(sortTodos([...page1.rows,...page2.rows]).length,2);
const repairs=todoSources('repair_management',ctx,get)[0];assert.equal((await repairs.load(1,signal)).more,true);assert.equal((await repairs.load(2,signal)).more,false);assert.equal(calls.at(-1).searchParams.get('offset'),'50');
const guards=todoSources('critical_guard',{...ctx,scope:'B'},get);assert.equal(guards.length,1);assert.equal((await guards[0].load(1,signal)).rows.length,1);assert.equal(calls.at(-1).searchParams.get('scope'),'B');
const risks=await todoSources('risk_inspection',ctx,get)[0].load(1,signal);assert.deepEqual(risks.rows.map(r=>r.status),['待填写','待审核']);assert(risks.more);
const waters=todoSources('water',ctx,get);assert.equal(waters.length,2);const w=await waters[0].load(1,signal);assert.deepEqual(w.rows.map(r=>r.status),['未记录','待补照片','照片待核验']);assert(w.more);
const learn=await todoSources('learning',ctx,get)[0].load(1,signal);assert.equal(learn.rows.length,1);assert.equal(learn.rows[0].scope,'A','moving buildings retains assigned paper');assert.equal(calls.at(-1).searchParams.get('person_id'),'person-alice');assert.equal(calls.at(-1).searchParams.get('scope'),'B');assert(learn.more);
const sig=await todoSources('signatures',ctx,get)[0].load(1,signal);assert.equal(sig.rows[0].status,'需重新签名');assert.equal(calls.at(-1).search,'','signature query never accepts another identity');
await assert.rejects(()=>todoSources('notices',ctx,async()=>({ongoing:[]}))[0].load(1,signal),/分页/);
await assert.rejects(()=>todoSources('signatures',ctx,async()=>({person:{is_self:false,signature_status:'unsigned'}}))[0].load(1,signal),/本人/);
const finished=await todoSources('repair_management',ctx,async()=>({records:[{record_id:'done',workflow:'维修完成'},{record_id:'flagged',is_completed:true},{record_id:'pending',workflow:'未闭环'}],total:3}))[0].load(1,signal);
assert.deepEqual(finished.rows.map(row=>row.key),['repair:pending']);
const template={kind:'notices',module:'notices',title:'record',scope:'A',status:'进行中',time:'',url:'/'};
assert.deepEqual(sortTodos([{...template,key:'plan',sourceKey:'maintenance:a'},{...template,key:'active',sourceKey:'maintenance:a',ongoing:true},{...template,key:'other',sourceKey:'change:a'},{...template,key:'overdue',dueAt:1}]).map(r=>r.key),['overdue','active','other']);
console.log('PASS: seven authorized categories, ordered/empty selections, ongoing-only notices with independent pagination and real-ID drill, risk review states, water zero/unknown photos, self-only learning/signatures and incomplete-response errors.');
