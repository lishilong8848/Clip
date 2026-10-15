import assert from 'node:assert/strict';
import {readFile,readdir} from 'node:fs/promises';
import {createRequire} from 'node:module';
import {build} from 'esbuild';
import {parse,compileTemplate} from '@vue/compiler-sfc';
import ts from 'typescript';
import {adaptPageLayouts} from './workspace-page-layout-adapter.mjs';
import {adaptEmbeddedNavigation} from './workspace-embedded-adapter.mjs';

const result=await build({stdin:{contents:"export * from './src/workspace-v2/pageLayout';export * from './src/workspace-v2/dashboard/layout';",resolveDir:process.cwd(),loader:'ts'},bundle:true,platform:'node',format:'cjs',write:false});
const module={exports:{}};
new Function('module','exports','require',result.outputFiles[0].text)(module,module.exports,createRequire(import.meta.url));
const {widget,sanitize,capacity,pageLayoutKey,sanitizePageLayout,defaultPageLayout}=module.exports;
const user={open_id:'layout-test-a',role:'building',level:'L4',category:'engineer',modules:['maintenance','change','event','repair_management','water']};
const full=widget('module',user,{module:'notices',h:24,w:12});
assert.deepEqual(sanitize({version:1,widgets:[full]},user,['A']).widgets,[full]);
assert.equal(sanitize({version:1,widgets:[full,{...full,id:'second'}]},user,['A']).widgets.length,1,'one controller per module');
assert.equal(sanitize({version:1,widgets:[{...full,module:'accounts'}]},user,['A']).widgets.length,0,'saved layouts never grant permissions');
assert.equal(sanitize({version:1,widgets:[full]},{...user,role:'guest'},['A']).widgets.length,0,'guest cannot embed writable modules');
assert.equal(sanitize({version:1,widgets:[{...full,scope:'E',w:1,h:1}]},user,['A']).widgets[0].scope,'current');
assert.equal(sanitize({version:1,widgets:[{...full,w:1,h:1}]},user,['A']).widgets[0].w,4);
assert.equal(sanitize({version:1,widgets:[{...full,w:1,h:1}]},user,['A']).widgets[0].h,6);
assert.equal(sanitize({version:1,widgets:Array.from({length:30},(_,i)=>widget('tasks',user,{id:'t'+i}))},user,['A']).widgets.length,16);
for(const [small,big] of [[capacity(350,280),capacity(1000,800)],[capacity(600,400),capacity(1200,1200)]]){assert(big.rows>small.rows);assert(big.columns>small.columns);}
assert.equal(capacity(100,10).rows,3);assert.equal(capacity(2000,10000).rows,60);
const blocks=[{id:'list',title:'列表',w:4,h:6},{id:'editor',title:'填写',w:8,h:8}];
assert.notEqual(pageLayoutKey(user,'event'),pageLayoutKey(user,'water'));
assert.notEqual(pageLayoutKey(user,'event'),pageLayoutKey({...user,open_id:'other'},'event'));
const base=defaultPageLayout(blocks,user);
const restored=sanitizePageLayout({version:1,items:[{...base.items[0],w:999,h:-1,x:999},null]},blocks,user,['A'],'event');
assert.equal(restored.items.length,2,'missing business forms are restored');assert.equal(restored.items[0].w,12);assert.equal(restored.items[0].x,0);assert.equal(restored.items[0].h,4);
assert.equal(sanitizePageLayout({version:1,items:[widget('water',user)]},blocks,user,['A'],'event').items.length,2,'only compatible optional widgets are restored');

function handlers(source){const values=[];function visit(node){for(const prop of node.props||[])if(prop.type===7&&['on','model'].includes(prop.name))values.push([prop.name,prop.arg?.content,prop.exp?.content].join('|'));for(const child of node.children||[])visit(child);}visit(parse(source).descriptor.template.ast);return values.sort();}
let adapted=0;
for(const dir of ['src/components','src/workspace-v2'])for(const file of (await readdir(dir)).filter(name=>name.endsWith('.vue'))){
  const filename=process.cwd().replaceAll('\\','/')+'/'+dir+'/'+file,source=await readFile(filename,'utf8'),layout=adaptPageLayouts(source,filename),embedded=adaptEmbeddedNavigation(layout,filename);
  if(source!==layout){assert.deepEqual(handlers(layout),handlers(source),file+' keeps original handlers and value bindings');adapted++;}
  const {descriptor,errors}=parse(embedded);assert.deepEqual(errors,[],file);
  assert.deepEqual(compileTemplate({id:file,filename,source:descriptor.template.content}).errors,[],file+' template compiles');
  const script=descriptor.scriptSetup?.content;if(script)assert.deepEqual(ts.createSourceFile(filename+'.ts',script,ts.ScriptTarget.Latest,true,ts.ScriptKind.TS).parseDiagnostics,[],file+' script parses');
  assert.equal(adaptPageLayouts(layout,filename),layout,file+' layout adaptation is idempotent');
  assert.equal(adaptEmbeddedNavigation(embedded,filename),embedded,file+' route adaptation is idempotent');
}
assert(adapted>=18);
const scoped=adaptEmbeddedNavigation('<template><div /></template><script setup lang="ts">import {navigate,navigateBack} from "./navigation";const query=window.location.search;function show(){window.history.replaceState({},"","/water-management");navigate("/water-management");navigateBack();}</script>','/src/workspace-v2/WaterWorkspace.vue');
assert.match(scoped,/useEmbeddedBrowser\(\)/);assert.match(scoped,/useEmbeddedBack/);assert(!scoped.includes('window.location.search'));
console.log(`PASS: complete module permissions, uniqueness, sizes, account/page isolation, compatible widgets, restored forms; ${adapted} layout templates retain original bindings.`);
