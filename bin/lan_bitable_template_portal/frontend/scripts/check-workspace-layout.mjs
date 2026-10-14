import assert from 'node:assert/strict';
import { build } from 'esbuild';
import { createRequire } from 'node:module';
const result=await build({entryPoints:['src/workspace-v2/dashboard/layout.ts'],bundle:true,platform:'node',format:'cjs',write:false});
const module={exports:{}};
new Function('module','exports','require',result.outputFiles[0].text)(module,module.exports,createRequire(import.meta.url));
const {defaults,sanitize,saveLayout,readLayout,layoutKey,widget,motionTemplates,allowedKind}=module.exports;
const user={open_id:'test-alice',level:'L1',modules:['maintenance','change','event','plan_convergence','learning','water']};
const other={...user,open_id:'test-bob'};
const storage=new Map();
globalThis.localStorage={getItem:key=>storage.get(key)??null,setItem:(key,value)=>storage.set(key,value)};
const base=defaults(user);
assert.equal(Math.max(...base.widgets.map(w=>w.y+w.h)),11,'checklist occupies five rows above the existing six-row work surface');
const randomUUID=globalThis.crypto.randomUUID;
globalThis.crypto.randomUUID=undefined;
assert.match(widget('notices',user).id,/^[a-f0-9]{32}$/,'LAN HTTP must not require secure-context randomUUID');
globalThis.crypto.randomUUID=randomUUID;
assert(base.widgets.some(w=>w.kind==='tasks')&&base.widgets.some(w=>w.kind==='shortcuts'));
assert(!base.widgets.some(w=>w.kind==='notices'||w.kind==='events'),'default is work, not a mandatory chart board');
const operationUser={...user,modules:['change','repair_management']};
for(const kind of ['notice_actions','repair_actions']){
  const item=widget(kind,operationUser);
  assert.equal(item.module,kind==='notice_actions'?'change':'repair_management');
  assert(allowedKind(kind,operationUser));assert(!allowedKind(kind,{...operationUser,role:'guest'}));
  assert.equal(sanitize({version:1,widgets:[item]},operationUser,['A']).widgets[0].kind,kind);
  assert.equal(sanitize({version:1,widgets:[item]},{...operationUser,modules:[]},['A']).widgets.length,0);
  assert.equal(sanitize({version:1,widgets:[{...item,module:'accounts'}]},operationUser,['A']).widgets.length,0);
}
for(const template of motionTemplates){
  const configured=widget(template.kind,user,{presentation:template.presentation});
  assert.equal(sanitize({version:1,widgets:[configured]},user,['A']).widgets[0].presentation,template.presentation);
  assert.equal(allowedKind(template.kind,{...user,modules:[]}),false,'templates cannot grant business permissions');
}
const legacyWidget=widget('tasks',user);delete legacyWidget.presentation;
assert.equal(sanitize({version:1,widgets:[legacyWidget]},user,['A']).widgets[0].presentation,'default','existing layouts keep their appearance');
assert.equal(sanitize({version:1,widgets:[widget('water',user,{presentation:'timeline'})]},user,['A']).widgets[0].presentation,'default','invalid per-module presentations are removed');
for(const role of ['L0','L1','L2','L3','L4'])assert(defaults({...user,level:role,category:role==='L4'?'operator':'engineer'}).widgets.length);
assert.equal(defaults({...user,level:'L4',category:'operator',modules:['maintenance']}).widgets.find(w=>w.kind==='tasks').title,'待处理事项','building work is not labelled as personal ownership');
const malicious={version:1,widgets:[{...widget('notices',user),scope:'E',w:999,h:-1,x:900,title:'<script>x</script>'},{...widget('tasks',user),module:'accounts'},null]};
const clean=sanitize(malicious,user,['A']);assert.equal(clean.widgets.length,1);assert.equal(clean.widgets[0].scope,'current');assert.equal(clean.widgets[0].w,12);assert.equal(clean.widgets[0].x,0);assert.equal(clean.widgets[0].h,3);
for(const category of ['operator','shift_leader','duty']){
  const person={...user,level:category==='duty'?'L1':'L4',category,home_scope:'A'};
  assert.equal(sanitize({version:1,widgets:[]},person,['A']).widgets.filter(w=>w.kind==='pre_shift').length,1,'required checklist survives older or empty layouts');
  const check=widget('pre_shift',person);
  assert.equal(sanitize({version:1,widgets:[check,{...check,id:'another'}]},person,['A']).widgets.length,1,'only one checklist widget per board');
}
assert.equal(allowedKind('pre_shift',{...user,role:'guest'}),false);
assert.equal(allowedKind('pre_shift',{...user,level:'L4',category:'engineer'}),false);
assert.equal(sanitize(base,{...user,modules:[]},['A']).widgets.length,0,'revoked modules cannot render from saved layout');
let raw=saveLayout(user,['A'],base,null);
assert.deepEqual(readLayout(user,['A']).layout,base);
const personalSize={...base,widgets:base.widgets.map(w=>({...w,h:9}))};raw=saveLayout(user,['A'],personalSize,raw);assert.equal(readLayout(user,['A']).layout.widgets[0].h,9,'new defaults do not resize an existing personal layout');
assert.equal(localStorage.getItem(layoutKey(other)),null,'accounts have independent preferences');
assert.throws(()=>saveLayout(user,['A'],base,null),/另一个页面/,'stale editors cannot overwrite another tab');
saveLayout(user,['A'],{version:1,widgets:[]},raw);assert.equal(readLayout(user,['A']).layout.widgets.length,0,'an intentionally empty board survives reload');
storage.set(layoutKey(user),'bad JSON');assert(readLayout(user,['A']).error);assert(readLayout(user,['A']).layout.widgets.length);
const recovered=readLayout(user,['A']);saveLayout(user,['A'],recovered.layout,recovered.raw);assert.equal(readLayout(user,['A']).error,'','a corrupted layout can be restored and saved');
localStorage.setItem=()=>{throw new Error('quota exceeded')};assert.throws(()=>saveLayout(other,['A'],base,null),/quota/);
console.log('PASS: role defaults, optional charts, permission+scope validation, account isolation, conflict protection, empty layouts, corrupted storage and quota errors.');
