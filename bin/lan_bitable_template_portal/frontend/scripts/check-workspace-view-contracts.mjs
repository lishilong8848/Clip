import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {compileScript,parse} from '@vue/compiler-sfc';
import {resolveConfig} from 'vite';

const root=fileURLToPath(new URL('..',import.meta.url));
const config=await resolveConfig({root,configFile:path.join(root,'vite.workspace-v2.config.ts'),logLevel:'error'},'build');
const adapters=config.plugins.filter(plugin=>plugin.enforce==='pre' && plugin.name.startsWith('workspace-'));
const views=fs.readdirSync(path.join(root,'src/workspace-v2/views')).filter(name=>name.endsWith('.html'));
const compilerFs={fileExists:fs.existsSync,readFile:file=>fs.readFileSync(file,'utf8')};
let checked=0;
for(const view of views){
  const id=path.join(root,'src/components',view.replace(/\.html$/,'.vue')).replaceAll('\\','/');
  // Some views have their own typed V2 controller and are already checked by vue-tsc.
  if(!fs.existsSync(id))continue;
  let code=fs.readFileSync(id,'utf8');
  for(const plugin of adapters){
    const transform=plugin.transform?.handler || plugin.transform;
    if(!transform)continue;
    const out=await transform.call({addWatchFile(){}},code,id);
    code=typeof out==='string'?out:out?.code||code;
  }
  const {descriptor,errors}=parse(code,{filename:id});
  assert.deepEqual(errors,[],view);
  const compiled=compileScript(descriptor,{id,inlineTemplate:true,fs:compilerFs});
  const unbound=[...new Set([...compiled.content.matchAll(/\b_ctx\.([\w$]+)/g)].map(m=>m[1]).filter(k=>!k.startsWith('$')))];
  assert.deepEqual(unbound,[],view+' references an undeclared controller field');
  const unresolved=[...new Set([...compiled.content.matchAll(/_resolveComponent\("([^"]+)"\)/g)].map(m=>m[1]).filter(k=>!['LoadingIndicator','UiTransition'].includes(k)))];
  assert.deepEqual(unresolved,[],view+' contains a component that is not registered');
  checked++;
}
assert(checked>0);
console.log('PASS: '+checked+' independently rendered views have matching controller bindings.');
