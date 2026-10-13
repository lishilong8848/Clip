import assert from 'node:assert/strict';
import {build} from 'esbuild';
import {fileURLToPath} from 'node:url';

const timers=[];
globalThis.window=Object.assign(new EventTarget(),{location:new URL('http://fixture.local/'),setTimeout:(fn,ms)=>{timers.push(ms);return setTimeout(fn,ms);},clearTimeout});
globalThis.location=window.location;globalThis.document={};
const pending=[],started=[];
globalThis.fetch=(url,options)=>new Promise((resolve,reject)=>{
  started.push(url);const row={url,options,resolve:data=>resolve(new Response(JSON.stringify({ok:true,data}),{status:200,headers:{'content-type':'application/json'}}))};pending.push(row);
  options.signal?.addEventListener('abort',()=>reject(new DOMException('Aborted','AbortError')),{once:true});
});
const compiled=await build({entryPoints:[fileURLToPath(new URL('../src/workspace-v2/client.ts',import.meta.url))],bundle:true,format:'esm',write:false,plugins:[{name:'no-dom-motion',setup(api){api.onResolve({filter:/\/actionMotion$/},()=>({path:'motion',namespace:'fixture'}));api.onLoad({filter:/.*/,namespace:'fixture'},()=>({contents:'export const trackButtonRequest = promise => promise;',loader:'js'}));}}]});
const client=await import('data:text/javascript;base64,'+Buffer.from(compiled.outputFiles[0].text).toString('base64'));
const next=()=>new Promise(setImmediate);
client.setReadCacheIdentity('account-a');
const slow=[0,1,2].map(i=>client.requestJson('/api/slow/'+i));
const queuedAbort=new AbortController(),queued=client.requestJson('/api/slow/queued',{signal:queuedAbort.signal}).catch(e=>e.name);
queuedAbort.abort();assert.equal(await queued,'AbortError');assert.equal(started.length,3);
const auth=client.requestJson('/api/auth/status');assert.equal(started.at(-1),'/api/auth/status','authentication is not blocked by the business queue');
pending.at(-1).resolve({logged_in:true});await auth;
for(const row of pending.slice(0,3))row.resolve({ok:true});await Promise.all(slow);await next();
assert(!started.includes('/api/slow/queued'));assert(timers.includes(45000),'reads retain the shared timeout instead of a shorter V2 limit');

const a=new AbortController(),b=new AbortController();
const path='/api/workbench?scope=A&work_type=maintenance';
const count=started.length;
const first=client.requestJson(path,{signal:a.signal}).catch(e=>e.name),second=client.requestJson('/api/workbench?work_type=maintenance&scope=A',{signal:b.signal});
assert.equal(started.length,count+1,'identical visible widgets share one request');a.abort();assert.equal(await first,'AbortError');assert.equal(pending.at(-1).options.signal.aborted,false,'one cancelled reader must not cancel another');
pending.at(-1).resolve({records:[{id:'fixture-a'}]});assert.equal((await second).records[0].id,'fixture-a');
const hit=await client.requestJson(path);hit.records[0].id='local-edit';assert.equal((await client.requestJson(path)).records[0].id,'fixture-a','cache data is not shared by reference');assert.equal(started.length,count+1,'returning to the workspace reuses the short-lived snapshot');
window.dispatchEvent(new Event('clipflow-business-changed'));
const updated=client.requestJson(path);assert.equal(started.length,count+2);pending.at(-1).resolve({records:[{id:'after-write'}]});await updated;
client.setReadCacheIdentity('account-b');const isolated=client.requestJson(path);assert.equal(started.length,count+3,'account change cannot reuse the previous account snapshot');pending.at(-1).resolve({records:[]});await isolated;
const c=new AbortController(),d=new AbortController();
const cancelled=[client.requestJson(path+'&search=cancel',{signal:c.signal}).catch(e=>e.name),client.requestJson(path+'&search=cancel',{signal:d.signal}).catch(e=>e.name)];c.abort();d.abort();assert.deepEqual(await Promise.all(cancelled),['AbortError','AbortError']);assert(pending.at(-1).options.signal.aborted);await next();
for(let i=0;i<2;i++){const incomplete=client.requestJson(path+'&search=incomplete');await next();pending.at(-1).resolve({source_snapshot_ready:false,records:[]});await incomplete;}
assert.equal(started.filter(url=>url.endsWith('search=incomplete')).length,2,'incomplete data is never reused as a complete snapshot');
const independent=client.requestJson(path,{cache:'no-store'});await next();pending.at(-1).resolve({records:[]});await independent;
const fresh=client.requestJson(path,{fresh:true});await next();pending.at(-1).resolve({records:[{id:'fresh'}]});await fresh;
const before=started.length,afterFresh=client.requestJson(path);await next();assert.equal(started.length,before+1,'explicit refresh invalidates the earlier quick snapshot');pending.at(-1).resolve({records:[{id:'fresh'}]});await afterFresh;
console.log('PASS: auth priority, bounded reads, prompt queue cancellation, single-flight isolation, cloned short-lived cache, write/account invalidation, incomplete snapshot rejection and explicit fresh read.');
