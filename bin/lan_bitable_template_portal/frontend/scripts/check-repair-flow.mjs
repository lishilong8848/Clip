import assert from 'node:assert/strict';
import {build} from 'esbuild';
const bundled=await build({entryPoints:['src/workspace-v2/repairFlow.ts'],bundle:true,write:false,format:'esm',platform:'node'});
const {repairFlowSteps}=await import('data:text/javascript;base64,'+Buffer.from(bundled.outputFiles[0].text).toString('base64'));
const event={record_id:'event-1',transfer_to_overhaul:true};
const project={record_id:'project-1',source_event_id:'event-1',source_repair_ids:['notice-1'],followup_count:2,followup_state_verified:true,is_completed:false,progress_percent:100};
let steps=repairFlowSteps('event',null,{...event,transfer_to_overhaul:false});
assert.deepEqual(steps.map(s=>s.status),['process','wait','wait','wait','wait']);
steps=repairFlowSteps('event',null,event);
assert.deepEqual(steps.map(s=>s.status),['finish','process','wait','wait','wait']);
steps=repairFlowSteps('project',project,event);
assert.deepEqual(steps.map(s=>s.status),['finish','finish','finish','finish','process']);
assert.match(steps[2].description,/发送状态需核对/);
assert.match(steps[4].description,/尚未完成/,'100% alone is not completion');
assert.equal(repairFlowSteps('project',{...project,is_completed:true},event)[4].status,'finish');
for(const extra of [{followup_state_verified:false},{state_locked:true}]){
  steps=repairFlowSteps('project',{...project,is_completed:true,...extra},event);
  assert.notEqual(steps[4].status,'finish');assert.match(steps[3].description,/待核验/);
}
for(const followup_count of [undefined,null,'',NaN,-1,'invalid',1.5]){
  steps=repairFlowSteps('project',{...project,followup_count},event);
  assert.equal(steps[3].status,'process');assert.match(steps[3].description,/待核验/);
}
steps=repairFlowSteps('project',{...project,followup_count:0},event);
assert.equal(steps[3].status,'process');assert.match(steps[3].description,/尚无/);
assert.equal(repairFlowSteps('project',project,{...event,record_id:'someone-else'})[0].status,'wait','different event cannot mark this event transferred');
assert.match(repairFlowSteps('project',project,null)[0].description,/转检修待核对/);
steps=repairFlowSteps('project',{record_id:'project-1',is_completed:true,followup_count:0},null);
assert.equal(steps[4].status,'finish');assert.equal(steps[0].status,'wait');assert.notEqual(steps[2].status,'finish','completion cannot invent missing source links');
for(const status of ['pending','failed']){
  steps=repairFlowSteps('project',project,event,status);
  assert.equal(steps[1].status,status==='failed'?'error':'process');assert.match(steps[1].description,/已保存/);
}
steps=repairFlowSteps('project',null,event);
assert.equal(steps[0].status,'wait','unsaved association is not shown as saved');assert.equal(steps[1].status,'process');
console.log('PASS: five evidence-based steps, missing/locked states, source identity, saved versus unsaved, 100% versus completed, sync failure and no invented notice send.');
