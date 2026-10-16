import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {parse,compileScript} from '@vue/compiler-sfc';
import {adaptBrowserReferences} from './workspace-build-adapter.mjs';
import {adaptEmbeddedNavigation} from './workspace-embedded-adapter.mjs';

const sample=`import {history as importedHistory} from './history';
const history=ref({items:[]});const location={name:'local'};
function read(window){return history.value.items+location.name+window.location.name+importedHistory.value;}
function nested(history){return history.items;}
window.history.back();window.location.assign('/');window.open('/signature');`;
for(const filename of ['fixture.ts','/components/Fixture.vue','/components/Fixture.vue?vue&type=script&setup=true&lang.ts','D:/project/页面.vue']){
  const result=adaptBrowserReferences(sample,filename,'/browser.ts');
  for(const local of ['history.value.items','location.name','window.location.name','importedHistory.value','history.items'])assert(result.includes(local),`${filename}: keep local ${local}`);
  for(const global of ['__v2History.back()','__v2Location.assign','__v2Open('])assert(result.includes(global),`${filename}: redirect global ${global}`);
  assert(!result.includes('__v2History.value')&&!result.includes('__v2Location.name'));
}
assert.equal(adaptBrowserReferences('const history=ref({});history.value={items:[]};','Fixture.vue','/browser.ts'),null);
const source=await readFile(new URL('../src/components/CabinetPowerPage.vue',import.meta.url),'utf8');
const embedded=adaptEmbeddedNavigation(source,'/components/CabinetPowerPage.vue');
assert(embedded.includes('history.value = data'),'embedded cabinet history stays reactive');
const compiled=compileScript(parse(embedded).descriptor,{id:'cabinet-fixture',inlineTemplate:true}).content;
const result=adaptBrowserReferences(compiled,'/components/CabinetPowerPage.vue','/browser.ts')||compiled;
assert(result.includes('history.value = data'));
assert(!result.includes('__v2History.value'));
console.log('PASS: local/imported/shadowed browser names in TS, Vue, Windows paths, embedded cabinet and compiled template.');
