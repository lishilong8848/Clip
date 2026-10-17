import assert from 'node:assert/strict';
import {build} from 'esbuild';
import {fileURLToPath} from 'node:url';
import {runInNewContext} from 'node:vm';

const ready=new Set(),reads=[],refreshes=[];
let active=0,maxActive=0,fail=false,configMissing=false,remainMissing=false;
const module={exports:{}};
const code=await build({entryPoints:[fileURLToPath(new URL('../src/workspace-v2/eventSnapshots.ts',import.meta.url))],bundle:true,write:false,format:'cjs',plugins:[{
  name:'isolated-event-client',setup(build){
    build.onResolve({filter:/^\.\/client$/},()=>({path:'client',namespace:'fixture'}));
    build.onLoad({filter:/.*/,namespace:'fixture'},()=>({contents:'export const requestJson=(...args)=>read(...args);export const refreshRemoteSourceAndWait=(...args)=>refresh(...args);'}));
  },
}]});
runInNewContext(code.outputFiles[0].text,{module,exports:module.exports,URLSearchParams,DOMException,
  read:async path=>{const params=new URL(path,'http://isolated').searchParams;reads.push(Object.fromEntries(params));assert.equal(params.get('date_field'),'occurrence_time');return {snapshot_exists:ready.has(params.get('month')),config_missing:configMissing,scope:params.get('scope')};},
  refresh:async(kind,path,options)=>{
    assert.equal(kind,'event');assert.equal(new URL(path,'http://isolated').pathname,'/api/events/refresh');refreshes.push(options.month);maxActive=Math.max(maxActive,++active);
    try{await new Promise(resolve=>setTimeout(resolve,15));if(fail)throw new Error('isolated refresh failure');if(!remainMissing)ready.add(options.month);}finally{active--;}
  },
});
const {eventSnapshot}=module.exports;
const abort=new AbortController();
const first=eventSnapshot('A','2026-01',abort.signal),second=eventSnapshot('B','2026-01');
abort.abort();
const results=await Promise.allSettled([first,second]);
assert.equal(results[0].status,'rejected');assert.equal(results[0].reason.name,'AbortError');
assert.equal(results[1].value.scope,'B');assert.deepEqual(refreshes,['2026-01'],'one cancelled widget cannot cancel or duplicate a shared month refresh');
await Promise.all([eventSnapshot('A','2026-02'),eventSnapshot('A','2026-03')]);
assert.equal(maxActive,1,'the existing backend receives only one month refresh at a time');
fail=true;const failed=await Promise.allSettled([eventSnapshot('A','2026-04'),eventSnapshot('B','2026-04')]);
assert(failed.every(value=>value.status==='rejected'));assert.equal(refreshes.filter(month=>month==='2026-04').length,1);
fail=false;await eventSnapshot('B','2026-04');assert.equal(refreshes.filter(month=>month==='2026-04').length,2,'a failed month can be retried, without poisoning the queue');
const before=refreshes.length;await eventSnapshot('A','2026-04');assert.equal(refreshes.length,before,'prepared snapshots do not trigger extra source sync');
configMissing=true;await eventSnapshot('A','2026-05');assert.equal(refreshes.length,before,'unconfigured sources are left to the normal configuration error');
configMissing=false;remainMissing=true;await assert.rejects(eventSnapshot('A','2026-06'),/仍未准备完成/);
assert(reads.some(row=>row.scope==='B'&&row.month==='2026-01'),'readback retains each widget permission scope');
console.log('PASS: shared month recovery, independent widget cancellation, sequential refreshes, failure/retry cleanup, ready/configuration states and scoped readback. All requests isolated.');
