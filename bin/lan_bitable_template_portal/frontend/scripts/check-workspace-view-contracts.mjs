import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {compileScript,parse} from '@vue/compiler-sfc';
import {parse as parseTemplate} from '@vue/compiler-dom';
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

// Vueuc searches for a div between its focus sentinels. A naked section can
// render correctly yet make the real dialog unreachable with the keyboard.
const viewRoot=path.join(root,'src/workspace-v2');
let dialogs=0;
function checkFocusRoot(node,file){
  if(node.tag && /^n-?modal$/i.test(node.tag) && !node.props.some(p=>p.name==='preset' || p.arg?.content==='preset')){
    for(const child of node.children.filter(c=>c.type===1)){
      if(child.tag==='template')continue;
      assert(!/^(section|article|aside|form|main|header)$/i.test(child.tag),file+' needs a focusable dialog container');
      dialogs++;
    }
  }
  for(const child of node.children||[])checkFocusRoot(child,file);
}
function visit(dir){
  for(const entry of fs.readdirSync(dir,{withFileTypes:true})){
    const file=path.join(dir,entry.name);
    if(entry.isDirectory()){visit(file);continue;}
    if(!/\.(vue|html)$/.test(file)||entry.name==='entry.html')continue;
    const source=fs.readFileSync(file,'utf8');
    const template=entry.name.endsWith('.vue')?parse(source,{filename:file}).descriptor.template?.content:source;
    if(template)checkFocusRoot(parseTemplate(template),path.relative(viewRoot,file));
  }
}
visit(viewRoot);
assert(dialogs>0);
console.log('PASS: '+dialogs+' custom dialog roots checked for focus-trap compatibility.');
