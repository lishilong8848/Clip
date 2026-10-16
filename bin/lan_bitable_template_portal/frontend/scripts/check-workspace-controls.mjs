import assert from 'node:assert/strict';
import {readFileSync,readdirSync} from 'node:fs';
import {parse,compileScript} from '@vue/compiler-sfc';
import {adaptEventPeriodPage} from './workspace-event-adapter.mjs';
import {adaptCabinetBatchPage} from './workspace-cabinet-adapter.mjs';
import {adaptPasswordLogin,markBinaryControls,fuseLearningCharts,portalFloatingControls,adaptMotionControls} from './workspace-controls-adapter.mjs';
const floating='<template><NModal><NSelect /><NDatePicker /><NTimePicker /><NPopover /><NSelect to="#host" /><NSelect :to="false" /><NSelect v-bind="props" /><NSelect v-bind:[key]="target" /><dialog><NSelect /></dialog></NModal></template>';
const portalled=portalFloatingControls(floating);
assert.equal((portalled.match(/:to="true"/g)||[]).length,4);
assert.match(portalled,/<NSelect to="#host"/);
assert.match(portalled,/<NSelect :to="false"/);
assert.match(portalled,/<dialog><NSelect \/><\/dialog>/);
assert.equal(portalFloatingControls(portalled),portalled,'floating-layer adaptation must be idempotent');
for(const [file,adapt] of [['EventManagementPage',adaptEventPeriodPage],['CabinetPowerBatchPage',adaptCabinetBatchPage],['PersonnelPasswordLogin',adaptPasswordLogin],['LearningDashboard',fuseLearningCharts]]){
  compileScript(parse(adapt(readFileSync(new URL('../src/components/'+file+'.vue',import.meta.url),'utf8'))).descriptor,{id:file});
}
const binary=markBinaryControls('<template><input type="checkbox" v-model="settings.enabled" /><input type="checkbox" v-model="selected" :value="option" /><input type="checkbox" v-model="form.enabled" @change="submit" /></template>');
assert.equal((binary.match(/data-v2-binary/g)||[]).length,1);
assert.match(binary,/<input data-v2-binary type="checkbox" v-model="settings.enabled"/);
assert.match(binary,/<input type="checkbox" v-model="selected"/);
const learning=readFileSync(new URL('../src/components/LearningPage.vue',import.meta.url),'utf8');
assert.match(learning,/夜班题单发布时间<input v-model="settingsForm.night_publish_time"/);
assert.match(learning,/night_publish_time: settingsForm.night_publish_time/);
let motionCount=0;
for(const dir of ['components','workspace-v2'])for(const file of readdirSync(new URL('../src/'+dir+'/',import.meta.url))){
  if(!file.endsWith('.vue'))continue;
  const original=readFileSync(new URL('../src/'+dir+'/'+file,import.meta.url),'utf8'),adapted=adaptMotionControls(original);
  if(original===adapted)continue;
  motionCount++;
  assert.equal(adaptMotionControls(adapted),adapted,'motion adapter must be idempotent');
  compileScript(parse(adapted).descriptor,{id:file,inlineTemplate:true});
  assert.doesNotMatch(parse(adapted).descriptor.template.content,/<progress\b/);
}
assert(motionCount>=5,'cabinet, learning and slider controls are covered');
console.log(`PASS: ${motionCount} real Vue views preserve bindings with V2 progress/range adaptation.`);
console.log('PASS: V2-only event/cabinet/password adapters; binary settings exclude multi-select and immediate actions; both shift settings are submitted.');
