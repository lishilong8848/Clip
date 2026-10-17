import assert from 'node:assert/strict';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {mkdir,writeFile} from 'node:fs/promises';
import {spawnSync} from 'node:child_process';
import {build,preview} from 'vite';
import vue from '@vitejs/plugin-vue';
import {chromium} from 'playwright';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const outDir=path.resolve(root,'../../../output/knowledge-group-picker-check');
const shots=path.resolve(root,'../../../output/playwright/knowledge-group-picker');
await mkdir(shots,{recursive:true});
await build({root,configFile:false,publicDir:false,logLevel:'error',plugins:[{
  name:'knowledge-group-picker-check',enforce:'post',resolveId(id){if(id==='virtual:knowledge-group-check')return '\0knowledge-group-check';},
  load(id){if(id!=='\0knowledge-group-check')return;return `
    import {createApp,h} from 'vue';
    import {NConfigProvider,zhCN} from 'naive-ui';
    import Page from '/src/components/KnowledgeBasePage.vue';
    import '/src/global.css';
    import '/src/workspace-v2/workspace.css';
    import '/src/workspace-v2/knowledge.css';
    const query=new URLSearchParams(location.search);
    if(!query.has('legacy')&&query.get('frontend')!=='legacy')document.documentElement.classList.add('workspace-v2');
    createApp({render:()=>h(NConfigProvider,{locale:zhCN},()=>h(Page))}).mount('#app');`;
  },
  generateBundle(_options,bundle){
    const entry=Object.values(bundle).find(item=>item.type==='chunk'&&item.isEntry);
    const css=Object.values(bundle).filter(item=>item.fileName.endsWith('.css'));
    assert(css.length,'The isolated page must load the real production styles');
    this.emitFile({type:'asset',fileName:'index.html',source:`<!doctype html><html lang="zh-CN"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">${css.map(item=>`<link rel="stylesheet" href="/${item.fileName}">`).join('')}</head><body style="margin:0"><div id="page-back-slot"></div><main id="app" class="v2-main" style="margin-left:0;padding:12px"></main><script type="module" src="/${entry.fileName}"></script></body></html>`});
  },
},vue()],build:{outDir,emptyOutDir:true,cssCodeSplit:false,rollupOptions:{input:'virtual:knowledge-group-check'}}});
const config=path.join(outDir,'tsconfig.json');
await writeFile(config,JSON.stringify({extends:path.join(root,'tsconfig.json'),compilerOptions:{types:[]},include:[path.join(root,'src/components/KnowledgeGroupPicker.vue'),path.join(root,'node_modules/vite/client.d.ts')]}));
const checked=spawnSync(process.execPath,[path.join(root,'node_modules/vue-tsc/bin/vue-tsc.js'),'--noEmit','-p',config],{cwd:root,encoding:'utf8'});
assert.equal(checked.status,0,checked.stdout+checked.stderr);

const server=await preview({root,configFile:false,logLevel:'error',build:{outDir},preview:{host:'127.0.0.1',port:0}});
const origin='http://127.0.0.1:'+server.httpServer.address().port;
const browser=await chromium.launch({headless:true});
const rootName='根目录公司运维管理制度与设备操作规程资料归档目录';
const branchName='南通基地电气专业现场作业指导及应急预案资料';
const leafName='10KV变压器故障应急处理流程及现场处置参考文件';
const targetPath=[rootName,branchName,leafName].join(' / ');
try{
  for(const legacy of [false,true])for(const width of [1440,390,360]){
    const context=await browser.newContext({viewport:{width,height:width===360?640:width<700?844:900},reducedMotion:'reduce'});
    const errors=[],writes=[];
    const groups=[{id:'root',name:rootName},{id:'branch',name:branchName,parent_id:'root'},
      {id:'target',name:leafName,parent_id:'branch'},{id:'other',name:'其它楼栋资料'},
      {id:'other-leaf',name:leafName,parent_id:'other'}].map(g=>({...g,owner_name:'隔离验收',created_at:1,count:0,can_manage:true}));
    const doc={id:'d1',name:'验收文件.txt',can_edit:true,version:1,active_version:1,status:'ready',size:100,chunks:1,owner_name:'隔离验收',updated_at:1};
    await context.route('**/*',route=>{
      const request=route.request(),url=new URL(request.url());
      if(url.origin!==origin)return route.abort();
      if(!url.pathname.startsWith('/api/'))return route.continue();
      const ok=data=>route.fulfill({json:{ok:true,data}});
      if(url.pathname==='/api/assistant/knowledge/groups')return ok({groups,total:groups.length});
      if(url.pathname==='/api/assistant/knowledge')return ok({items:[doc],total:1,page:1,page_size:20,is_admin:false,settings:{},engine:{},stats:{}});
      if(request.method()==='PUT'&&url.pathname==='/api/assistant/knowledge/groups/target/documents'){
        assert.deepEqual(request.postDataJSON(),{document_ids:['d1']});writes.push(request.postDataJSON());return ok({updated:1});
      }
      if(request.method()!=='GET')throw new Error('Unexpected isolated write: '+url.pathname);
      return ok({});
    });
    const page=await context.newPage();page.on('pageerror',error=>errors.push(error.message));page.setDefaultTimeout(12000);
    try{
      await page.goto(origin+(legacy?'/?legacy':''));
      await page.getByRole('checkbox',{name:'选择验收文件.txt',exact:true}).click();
      const trigger=page.getByRole('combobox',{name:'目标分组',exact:true});
      assert.equal(await trigger.evaluate(el=>getComputedStyle(el).display),'grid','The actual picker styles are loaded');
      await trigger.click();
      const modal=page.locator('.kb-group-picker-dialog');await modal.waitFor();
      await page.waitForFunction(()=>{const el=document.querySelector('.kb-group-picker-dialog');return el&&getComputedStyle(el).opacity==='1'&&getComputedStyle(el).transform==='none';});
      const chosen=modal.getByTitle(targetPath,{exact:true});await chosen.waitFor();
      assert.equal(await chosen.textContent(),leafName);
      const fits=await chosen.evaluate(el=>{const text=el.closest('.n-tree-node-content__text');return text&&text.scrollWidth<=text.clientWidth+1&&text.scrollHeight<=text.clientHeight+1&&getComputedStyle(el).whiteSpace==='normal';});
      assert(fits,'Long folder names wrap instead of being cut off');
      if(width<700)await page.waitForFunction(path=>[...document.querySelectorAll('.kb-group-picker-dialog .kb-group-picker-label')].some(el=>el.getAttribute('title')===path&&el.getBoundingClientRect().height>=30),targetPath);
      await modal.getByRole('textbox',{name:'搜索分组',exact:true}).fill(branchName+' / '+leafName);
      await chosen.click();
      await modal.getByLabel('待选分组完整路径',{exact:true}).getByText(targetPath,{exact:true}).waitFor();
      assert.equal(writes.length,0,'Choosing a directory never moves documents');
      await page.screenshot({path:path.join(shots,`${legacy?'legacy':'v2'}-${width}-picker.png`),fullPage:false});
      const buttonBox=await modal.getByRole('button',{name:'选择此分组',exact:true}).boundingBox();
      assert(buttonBox&&buttonBox.y>=0&&buttonBox.y+buttonBox.height<=(width===360?640:width<700?844:900),'The confirmation button remains inside small screens');
      await modal.getByRole('button',{name:'取消',exact:true}).click();
      await modal.waitFor({state:'hidden'});
      assert.equal(await page.getByLabel('已选分组完整路径',{exact:true}).count(),0,'Cancel keeps the original target unchanged');
      await trigger.click();await modal.waitFor();await modal.getByTitle(targetPath,{exact:true}).click();
      await modal.getByRole('button',{name:'选择此分组',exact:true}).click();await modal.waitFor({state:'hidden'});
      await page.getByLabel('已选分组完整路径',{exact:true}).getByText(targetPath,{exact:true}).waitFor();
      assert.equal(await trigger.textContent(),leafName);
      assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false);
      assert.equal(writes.length,0,'Confirming the target still requires the original Apply action');
      await page.screenshot({path:path.join(shots,`${legacy?'legacy':'v2'}-${width}-selected.png`),fullPage:false});
      await page.getByRole('button',{name:'应用分组',exact:true}).click();
      await page.getByText(`已将 1 个文件分入「${targetPath}」。`,{exact:true}).waitFor();
      assert.equal(writes.length,1,'Original endpoint and stable group ID are preserved');
      assert.deepEqual(errors,[]);
      console.log(`PASS: ${legacy?'legacy':'v2'} ${width}: full names/paths, duplicate leaf names, path search, cancel and explicit single assignment.`);
    }catch(error){await page.screenshot({path:path.join(shots,`failure-${legacy?'legacy':'v2'}-${width}.png`),fullPage:true});throw error;}
    finally{await context.close();}
  }
}finally{await browser.close();await server.close();}
