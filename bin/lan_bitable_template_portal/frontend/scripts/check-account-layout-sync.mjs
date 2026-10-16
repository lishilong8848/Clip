import assert from 'node:assert/strict';
import path from 'node:path';
import {createServer} from 'vite';
import {chromium} from 'playwright';
import {layoutApiFixture} from './layout-api-fixture.mjs';

const fixture=`import {createApp,ref,h} from 'vue';
import {useLayoutSync} from '/src/workspace-v2/useLayoutSync.ts';
const user=ref({open_id:'alice',role:'building'}),locked=ref(false),value=ref({title:'default'});
createApp({setup(){const sync=useLayoutSync({user:()=>user.value,page:()=>'workspace',legacyKey:()=> 'legacy:'+user.value.open_id,locked:()=>locked.value,
  normalize:raw=>raw&&typeof raw==='object'?raw:{title:'default'},apply:next=>value.value=next});
  window.test={sync,user,locked,value};
  return ()=>h('main',[h('output',{id:'value'},JSON.stringify(value.value)),h('output',{id:'error'},sync.error.value)]);
}}).mount('#app');`;
const server=await createServer({configFile:false,root:process.cwd(),server:{host:'127.0.0.1',port:0},logLevel:'error',plugins:[{
  name:'account-layout-test-host',configureServer(server){server.middlewares.use((req,res,next)=>{
    if(req.url==='/layout-test'){res.setHeader('Content-Type','text/html');res.end('<div id="app"></div><script type="module" src="/layout-fixture.js"></script>');}else next();
  });},resolveId(id){if(id==='/layout-fixture.js')return '\0layout-fixture';},load(id){if(id==='\0layout-fixture')return fixture;}
}]});
await server.listen();
const origin='http://127.0.0.1:'+server.httpServer.address().port,browser=await chromium.launch({headless:true}),api=layoutApiFixture();
const contexts=[],errors=[];
async function client({owner='alice',width=1440,legacy=null,fail=false,blockedStorage=false,delay=0}={}){
  const context=await browser.newContext({viewport:{width,height:900}});contexts.push(context);
  if(legacy)await context.addInitScript(value=>localStorage.setItem('legacy:alice',JSON.stringify(value)),legacy);
  if(blockedStorage)await context.addInitScript(()=>{Storage.prototype.getItem=Storage.prototype.setItem=()=>{throw new Error('storage blocked');};});
  const state={owner,fail,delay};
  await context.route('**/*',async route=>{
    const url=new URL(route.request().url());if(url.origin!==origin)return route.abort();
    if(!url.pathname.startsWith('/api/'))return route.continue();
    const ownerAtStart=state.owner;if(state.delay)await new Promise(r=>setTimeout(r,state.delay));
    if(state.fail)return route.fulfill({status:503,json:{ok:false,error:'layout offline'}});
    if(await api.handle(route,ownerAtStart))return;
    throw new Error('unexpected API '+url.pathname);
  });
  const page=await context.newPage();page.on('pageerror',e=>errors.push(e.message));
  await page.goto(origin+'/layout-test');await page.waitForFunction(()=>!!window.test);
  if(owner!=='alice')await page.evaluate(owner=>{window.test.user.value={open_id:owner,role:'building'};},owner);
  await page.waitForFunction(()=>!window.test.sync.loading.value);
  return {page,state};
}
async function save(page,title){return page.evaluate(async title=>{
  const t=window.test;t.locked.value=true;t.value.value={title};
  try{await t.sync.save(t.value.value);t.locked.value=false;return '';}catch(e){return e.message;}
},title);}
async function title(page){return page.evaluate(()=>window.test.value.value.title);}
try{
  const a=await client({legacy:{title:'legacy PC'}});assert.equal(await title(a.page),'legacy PC');assert.equal(api.writes,1,'legacy migrates only after server confirms missing');
  const b=await client();assert.equal(await title(b.page),'legacy PC','fresh browser has no local storage but gets same account');
  assert.equal(await save(a.page,'desktop update'),'');assert.match(await save(b.page,'stale edit'),/另一台设备/);assert.equal(await title(b.page),'stale edit','conflict keeps draft');
  await b.page.evaluate(async()=>{await window.test.sync.reload();window.test.locked.value=false;});assert.equal(await title(b.page),'desktop update');
  const mobile=await client({width:390});assert.equal(await title(mobile.page),'default','phone never imports desktop browser cache');
  assert.equal(await save(mobile.page,'phone layout'),'');const phone2=await client({width:430});assert.equal(await title(phone2.page),'phone layout');
  assert.equal(await title(a.page),'desktop update');const other=await client({owner:'bob'});assert.equal(await title(other.page),'default','other person cannot reuse Alice');
  const existing=await client({legacy:{title:'stale legacy'}});assert.equal(await title(existing.page),'desktop update','remote wins over obsolete browser layout');
  const before=api.writes,offline=await client({legacy:{title:'offline cache'},fail:true});assert.equal(await title(offline.page),'offline cache');assert.equal(api.writes,before,'read failure never causes migration write');
  assert.equal(await offline.page.evaluate(()=>window.test.sync.ready.value),false);assert.match(await save(offline.page,'must not write'),/读取/);assert.equal(api.writes,before);
  const blocked=await client({blockedStorage:true});assert.equal(await title(blocked.page),'desktop update');assert.equal(await save(blocked.page,'storage disabled still syncs'),'');
  await blocked.page.evaluate(()=>window.test.locked.value=true);await blocked.page.setViewportSize({width:390,height:844});
  assert.equal(await blocked.page.evaluate(()=>window.test.sync.device.value),'desktop','resize cannot replace active editing session');
  await blocked.page.evaluate(()=>window.test.locked.value=false);await blocked.page.waitForFunction(()=>window.test.sync.device.value==='mobile'&&!window.test.sync.loading.value);assert.equal(await title(blocked.page),'phone layout');
  const late=await client();late.state.delay=100;late.state.owner='alice';
  await late.page.evaluate(()=>void window.test.sync.reload());late.state.owner='bob';await late.page.evaluate(()=>{window.test.user.value={open_id:'bob',role:'building'};});
  await late.page.waitForFunction(()=>!window.test.sync.loading.value);assert.equal(await title(late.page),'default','late old-account reads cannot overwrite new identity');
  await late.page.evaluate(()=>{window.test.user.value={open_id:'guest',role:'guest'};});await late.page.waitForTimeout(150);assert.equal(await late.page.evaluate(()=>window.test.sync.ready.value),false);
  assert.deepEqual(errors,[]);console.log('PASS: account synchronization across fresh PC/phone browsers, person isolation, migration, CAS conflict, offline safety, blocked storage, responsive edit locking and stale reads.');
}finally{await Promise.all(contexts.map(c=>c.close()));await browser.close();await server.close();}
