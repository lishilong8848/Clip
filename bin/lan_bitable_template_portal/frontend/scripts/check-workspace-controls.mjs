import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {parse,compileScript} from '@vue/compiler-sfc';
import {adaptEventPeriodPage} from './workspace-event-adapter.mjs';
import {adaptCabinetBatchPage} from './workspace-cabinet-adapter.mjs';
import {adaptPasswordLogin,markBinaryControls,fuseLearningCharts} from './workspace-controls-adapter.mjs';
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
console.log('PASS: V2-only event/cabinet/password adapters; binary settings exclude multi-select and immediate actions; both shift settings are submitted.');
