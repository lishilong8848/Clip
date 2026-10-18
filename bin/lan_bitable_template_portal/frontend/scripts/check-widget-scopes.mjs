import assert from 'node:assert/strict';
import {build} from 'esbuild';
async function load(file){const result=await build({entryPoints:[file],bundle:true,write:false,format:'esm',platform:'node'});return import('data:text/javascript;base64,'+Buffer.from(result.outputFiles[0].text).toString('base64'));}
const {widgetScopeOptions,selectedWidgetScopes,sanitizeWidgetScope}=await load('src/workspace-v2/dashboard/widgetScopes.ts');
const options=['ALL','110','A','B','C','H'].map(value=>({value,label:value}));
assert.deepEqual(widgetScopeOptions({kind:'module',module:'cabinet'},options).map(o=>o.value),['A','B','C']);
for(const kind of ['pre_shift','learning'])assert.deepEqual(widgetScopeOptions({kind,module:''},options),[]);
assert.deepEqual(selectedWidgetScopes('A,B,E','ALL',options),['A','B']);
assert.deepEqual(selectedWidgetScopes('current','ALL',options.filter(o=>o.value!=='ALL')),['110','A','B','C','H']);
assert.equal(sanitizeWidgetScope('A,B,B,E',['A','B']),'A,B');
assert.equal(sanitizeWidgetScope('E',['A','B']),'current');
const {sanitize,widget}=await load('src/workspace-v2/dashboard/layout.ts');
const user={open_id:'isolated',role:'user',level:'L4',category:'engineer',modules:['maintenance']};
const value=sanitize({version:1,widgets:[widget('module',user,{module:'notices',scope:'A,B,E'})]},user,['A','B']);
assert.equal(value.widgets[0].scope,'A,B','multi-building selection uses the existing account-layout scope string');
const {todoSources}=await load('src/workspace-v2/businessTodos.ts');
const selected=todoSources('notices',{user,scopes:options,scope:'ALL',selectedScopes:['A','B','E','B']},async()=>({}));
assert.deepEqual(selected.map(source=>source.id),['A:notices','B:notices']);

const {readNoticeBuildings}=await load('src/workspace-v2/noticeBuildingPages.ts');
const records=scope=>Array.from({length:243},(_,i)=>({record_id:i===0?'shared':scope+'-'+i,work_type:'maintenance',title:scope+'计划'+i}));
const calls=[];
const read=async q=>{
  calls.push(new URLSearchParams(q));const scope=q.get('scope');assert(['A','B'].includes(scope));const data={source_snapshot_ready:true};
  for(const lane of q.get('sections').split(',')){
    const all=lane==='records'?records(scope):[{active_item_id:scope+'-active',work_type:'maintenance'}];
    const page=Number(q.get(lane+'_page')),size=Number(q.get(lane+'_page_size'));
    data[lane]=all.slice((page-1)*size,page*size);data[lane+'_pagination']={total:all.length};
  }
  return data;
};
const previous=[];
for(let page=1;page<=25;page++){
  const result=await readNoticeBuildings(new URLSearchParams({work_type:'all',records_page:String(page),ongoing_page:'1'}),['A','B'],read);
  assert.equal(result.records.length,page===25?5:20);
  assert(!result.records.some(row=>previous.includes(row.record_id)),'multi-building pagination does not repeat a record');
  previous.push(...result.records.map(row=>row.record_id));
  assert.deepEqual(result.ongoing.map(row=>row._request_scope),['A','B']);
  if(page===1)assert(result.records.some(row=>row._request_scope==='A')&&result.records.some(row=>row._request_scope==='B'));
  if(page===25){assert.equal(result.records_pagination.total,485);assert.equal(result.records_pagination.partial,false);}
}
assert.equal(new Set(previous).size,485);
assert(calls.every(q=>Number(q.get('records_page_size'))<=200));
await assert.rejects(()=>readNoticeBuildings(new URLSearchParams(),['A'],async()=>({source_snapshot_ready:false})),/尚未完整/);
await assert.rejects(()=>readNoticeBuildings(new URLSearchParams(),['A'],async()=>({records:[],records_pagination:{total:10}})),/读取不完整/);
console.log('PASS: authorized scopes, account-layout roundtrip, personal widgets excluded, scoped todo sources, combined notice pagination/deduplication and incomplete reads.');
