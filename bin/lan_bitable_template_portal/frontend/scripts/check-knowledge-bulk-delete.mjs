import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {mkdir,readFile,writeFile} from 'node:fs/promises';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {createServer,preview} from 'vite';
import vue from '@vitejs/plugin-vue';
import {chromium} from 'playwright';
import {adaptKnowledgeFolders, adaptKnowledgeReader} from './workspace-knowledge-adapter.mjs';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const built=process.argv.includes('--dist');
const output=path.resolve(root,'../../../output/playwright/knowledge-bulk-delete'+(built?'-dist':''));
const evidence=[];
await mkdir(output,{recursive:true});
const hash=value=>createHash('sha256').update(JSON.stringify(value)).digest('hex');
const browser=await chromium.launch({headless:true});
try {
 for(const variant of ['legacy','v2']) {
  let docs,writes,mode;
  const unexpectedWrites=[];
  function reset(){
   docs=Array.from({length:26},(_,i)=>({id:'d'+i,name:'guide-'+i+'.txt',version:1,active_version:1,status:'ready',size:100,chunks:1,can_edit:i!==0,owner_name:'Fixture',updated_at:1,group_id:null,deleted:false}));
   docs.push({...docs[1],id:'auto',name:'guide-auto.txt',source_kind:'document_library'});
   writes=[];mode='ok';
  }
  reset();
  const fixture={name:'knowledge-fixture',enforce:'pre',resolveId(id){if(id==='/kb-fixture-entry.js')return '\0kb-fixture-entry';},
   load(id){if(id==='\0kb-fixture-entry')return `import {createApp} from 'vue';import Page from '/src/components/KnowledgeBasePage.vue';import UiTransition from '/src/components/UiTransition.vue';import '/src/global.css';createApp(Page).component('UiTransition',UiTransition).mount('#app');`;},
   transform(code,id){
    return variant==='v2'&&id.replaceAll('\\','/').endsWith('/KnowledgeBasePage.vue')?adaptKnowledgeReader(adaptKnowledgeFolders(code,id),id):undefined;
   },configureServer(server){server.middlewares.use(async(req,res,next)=>{
    const url=new URL(req.url,'http://fixture');
    if(built&&url.pathname==='/assets/connection-guard.js'){res.setHeader('Content-Type','application/javascript');res.end('');return;}
    if(built&&url.pathname==='/assets/vnet-logo.png'){res.setHeader('Content-Type','image/png');res.end(await readFile(path.join(root,'public/assets/vnet-logo.png')));return;}
    if(!built&&url.pathname==='/kb-fixture'){
     res.setHeader('Content-Type','text/html');res.end(await server.transformIndexHtml(req.url,'<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1"><style>*{box-sizing:border-box}</style></head><body><div id="app"></div><script type="module" src="/kb-fixture-entry.js"></script></body></html>'));return;
    }
    if(!url.pathname.startsWith('/api/'))return next();
    const ok=data=>{res.setHeader('Content-Type','application/json');res.end(JSON.stringify({ok:true,data}));};
    const fail=(status,error)=>{res.statusCode=status;res.setHeader('Content-Type','application/json');res.end(JSON.stringify({ok:false,error}));};
    if(req.method!=='GET'&&!(url.pathname==='/api/assistant/knowledge/bulk-delete'&&req.method==='POST')){unexpectedWrites.push(req.method+' '+url.pathname);return fail(400,'Unexpected fixture write');}
    if(url.pathname==='/api/auth/status')return ok({logged_in:true,user:{open_id:'knowledge-fixture',name:'隔离测试',role:'building',level:'L3',category:'supervisor',home_scope:'A',scope_rules:['A'],modules:['knowledge']},scope_options:[{value:'A',label:'A楼'}],default_scope:'A'});
    if(url.pathname==='/api/health'){res.setHeader('Content-Type','application/json');res.end(JSON.stringify({ok:true,service:'clipflow_backend',instance_id:'knowledge-dist-fixture'}));return;}
    if(url.pathname.startsWith('/api/workspace-layouts/'))return ok({revision:0,value:null});
    if(url.pathname==='/api/assistant/knowledge/groups')return ok({groups:[]});
    if(url.pathname==='/api/assistant/knowledge'){
     const q=url.searchParams.get('q')||'',group=url.searchParams.get('group')||'';
     const result=docs.filter(d=>!d.source_kind&&d.deleted===(url.searchParams.get('deleted')==='1')&&d.name.includes(q));
     const selected=result.filter(d=>d.can_edit).sort((a,b)=>a.id.localeCompare(b.id));
     const page=Math.min(Number(url.searchParams.get('page')||1),Math.max(1,Math.ceil(result.length/20)));
     return ok({items:result.slice((page-1)*20,page*20),page,total:result.length,is_admin:false,
      ...(url.searchParams.has('select_all')?{selection:{q,group,count:selected.length,fingerprint:hash(selected.map(d=>d.id)),revision_fingerprint:hash(selected.map(d=>[d.id,d.version]))}}:{})});
    }
    if(url.pathname==='/api/assistant/knowledge/bulk-delete'&&req.method==='POST'){
     let body='';for await(const chunk of req)body+=chunk;body=JSON.parse(body);
     writes.push(body);
     if(mode==='partial')return ok({deleted:1,errors:[{error:'fixture partial error'}]});
     const selected=body.selection?docs.filter(d=>!d.source_kind&&!d.deleted&&d.can_edit&&d.name.includes(body.selection.q)).sort((a,b)=>a.id.localeCompare(b.id)):body.documents.map(r=>docs.find(d=>d.id===r.id));
     if(body.selection&&(hash(selected.map(d=>d.id))!==body.selection.fingerprint||hash(selected.map(d=>[d.id,d.version]))!==body.selection.revision_fingerprint))return fail(409,'文件版本已变化，未删除任何文件，请重新选择。');
     if(!body.selection&&selected.some((d,i)=>!d||d.source_kind||!d.can_edit||d.version!==body.documents[i].version))return fail(409,'文件版本已变化，未删除任何文件，请重新选择。');
     const affected=selected.filter(d=>!body.selection?.excluded_ids.includes(d.id));
     affected.forEach(d=>{d.deleted=true;d.version++;});return ok({deleted:affected.length});
    }
    if(req.method!=='GET')return fail(400,'Unexpected fixture write');
    return ok({});
   });}};
  fixture.configurePreviewServer=fixture.configureServer;
  const server=built?await preview({root,configFile:false,logLevel:'error',build:{outDir:'dist'},preview:{host:'127.0.0.1',port:0,proxy:{}},plugins:[fixture]}):await createServer({root,configFile:false,logLevel:'error',server:{host:'127.0.0.1',port:0},plugins:[fixture,vue()]});
  if(!built)await server.listen();
  const url='http://127.0.0.1:'+server.httpServer.address().port+(built?(variant==='legacy'?'/knowledge-base?frontend=legacy':'/assets/workspace-v2/index.html?__page=/knowledge-base'):'/kb-fixture');
  try{
   for(const width of [1440,402,440]){
    reset();const context=await browser.newContext({viewport:{width,height:900},reducedMotion:'reduce',serviceWorkers:'block'});
    if(built)await context.addInitScript(()=>{if(location.pathname!=='/assets/workspace-v2/index.html')localStorage.setItem('clipflow:frontend-version:knowledge-fixture','legacy');});
    const page=await context.newPage(),errors=[];page.setDefaultTimeout(20000);page.on('pageerror',e=>errors.push(e.message));
    const scripts=new Set();page.on('request',request=>{if(request.resourceType()==='script')scripts.add(new URL(request.url()).pathname);});
    await context.route('**/*',route=>new URL(route.request().url()).origin===new URL(url).origin?route.continue():route.abort());
    try{
     await page.goto(url);await page.locator('.kb-row').first().waitFor();
     assert.equal(await page.getByText('guide-auto.txt',{exact:true}).count(),0);
     assert.equal(await page.locator('.kb-select-all').count(),1);
     assert((await page.locator('.kb-select-all').boundingBox()).y<(await page.locator('.kb-list').boundingBox()).y);
     await page.screenshot({path:path.join(output,`${variant}-${width}-list.png`)});
     await page.getByRole('checkbox',{name:'全选本页',exact:true}).click();
     await page.getByRole('button',{name:'批量删除',exact:true}).click();
     const confirm=page.getByRole('dialog',{name:'批量移入回收站',exact:true});
     await confirm.getByText(/选中的 19 个文件/).waitFor();
     await confirm.getByRole('button',{name:'取消',exact:true}).click();assert.equal(writes.length,0);
     await page.getByRole('button',{name:'批量删除',exact:true}).click();
     await confirm.getByRole('button',{name:'确认移入回收站',exact:true}).click();
     await page.getByText('已将 19 个文件移入回收站。',{exact:true}).waitFor();
     assert.equal(writes.length,1);assert.equal(docs.filter(d=>d.deleted).length,19);assert(!docs[0].deleted&&!docs.at(-1).deleted);
     reset();await page.reload();await page.locator('.kb-row').first().waitFor();
     await page.getByRole('searchbox',{name:'搜索知识库'}).fill('guide');
     await page.waitForTimeout(500);
     await page.getByRole('button',{name:'全选全部',exact:true}).click();await page.getByText('已选 25 个文件（全部搜索结果）',{exact:true}).waitFor();
     await page.getByRole('checkbox',{name:'取消选择guide-1.txt',exact:true}).click();
     await page.getByRole('button',{name:'末页',exact:true}).click();await page.getByRole('checkbox',{name:'取消选择guide-25.txt',exact:true}).waitFor();
     await page.getByRole('button',{name:'批量删除',exact:true}).click();await confirm.getByText(/选中的 24 个文件/).waitFor();
     const box=await confirm.boundingBox();assert(box.x>=0&&box.x+box.width<=width,'confirmation fits the viewport');
     await page.screenshot({path:path.join(output,`${variant}-${width}-confirm.png`),fullPage:true});
     await confirm.getByRole('button',{name:'确认移入回收站',exact:true}).click();await page.getByText('已将 24 个文件移入回收站。',{exact:true}).waitFor();
     assert(!docs[0].deleted&&!docs[1].deleted&&!docs.at(-1).deleted);
     reset();await page.reload();await page.locator('.kb-row').first().waitFor();
     await page.getByRole('button',{name:'全选全部',exact:true}).click();await page.getByText('已选 25 个文件（全部搜索结果）',{exact:true}).waitFor();
     docs.push({...docs[1],id:'new',name:'guide-new.txt'});
     await page.getByRole('button',{name:'刷新列表',exact:true}).click();await page.getByText(/已取消全部选择/).waitFor();
     assert.equal(await page.locator('.kb-batch-bar').count(),0);assert.equal(writes.length,0);
     await page.getByRole('checkbox',{name:'选择guide-1.txt',exact:true}).click();
     docs[1].version++;
     await page.getByRole('button',{name:'批量删除',exact:true}).click();await confirm.getByRole('button',{name:'确认移入回收站',exact:true}).click();
     await page.getByRole('alert').filter({hasText:'版本已变化'}).waitFor();assert.equal(docs.filter(d=>d.deleted).length,0);
     await page.getByRole('checkbox',{name:'全选本页',exact:true}).click();mode='partial';
     await page.getByRole('button',{name:'批量删除',exact:true}).click();await confirm.getByRole('button',{name:'确认移入回收站',exact:true}).click();
     await page.getByRole('alert').filter({hasText:'结果不完整'}).waitFor();assert.equal(await page.getByText(/^已将 .*移入回收站/).count(),0);
     assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false);
     await page.screenshot({path:path.join(output,`${variant}-${width}-top.png`),fullPage:true});
     assert.deepEqual(errors,[]);assert.deepEqual(unexpectedWrites,[]);
     if(built){assert(scripts.size>1,'built application scripts loaded');assert([...scripts].every(script=>script.startsWith('/assets/')),'only built assets, no source fixture');}
     evidence.push({variant,width,built,scripts:[...scripts],passed:true});
     console.log('PASS '+(built?'dist ':'source ')+variant+' '+width+' selection, cancellation, soft delete, stale snapshots, partial errors');
    }catch(error){console.error(errors,await page.locator('body').innerText());await page.screenshot({path:path.join(output,`${variant}-${width}-failure.png`),fullPage:true});throw error;}
    finally{await context.close();}
   }
   if(process.env.KNOWLEDGE_RESPONSIVELY==='1'&&variant==='v2'){
    reset();try{const result=await promisify(execFile)('node',['C:/Users/l1773/.codex/scripts/responsively-preview.mjs',url,path.join(output,'responsively-'+variant)],{windowsHide:true,timeout:120000});console.log(result.stdout);}
    catch(error){console.warn('Responsively capture unavailable: '+error.stderr);}
   }
  }finally{if(built)await new Promise((resolve,reject)=>server.httpServer.close(error=>error?reject(error):resolve()));else await server.close();}
 }
 await writeFile(path.join(output,'results.json'),JSON.stringify(evidence,null,2));
}finally{await browser.close();}
