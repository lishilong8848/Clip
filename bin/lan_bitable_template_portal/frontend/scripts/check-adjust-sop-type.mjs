import assert from 'node:assert/strict';
import {mkdtemp, mkdir, rm} from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import vm from 'node:vm';
import {createServer} from 'vite';
import vue from '@vitejs/plugin-vue';
import {chromium} from 'playwright';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const project=path.resolve(root,'../../..'),scratch=await mkdtemp(path.join(os.tmpdir(),'adjust-sop-browser-'));
const output=path.join(project,'output/playwright/adjust-sop-type');
let server,browser;
try {
  const rendered=spawnSync(path.join(project,'bin/.venv/Scripts/python.exe'),['-B','-c',
    "import sys;sys.path.insert(0,'bin');from lan_bitable_template_portal.workbench_lite import render_workbench_lite;print(render_workbench_lite(payload={'records':[],'ongoing':[],'stats':{}},session={'role':'admin'},scope='A',work_type='adjust'))"],
    {cwd:project,encoding:'utf8',env:{...process.env,CLIPFLOW_DATA_DIR:scratch,PYTHONIOENCODING:'utf-8',PYTHONDONTWRITEBYTECODE:'1'}});
  assert.equal(rendered.status,0,rendered.stderr);
  const names=['pollingSopPlaceholderTokens','pollingSopIsAdjustCooling','pollingAdjustSopType','pollingSopHasUnsupportedDevicePlaceholders'];
  const helpers=names.map(name=>{
    const matches=rendered.stdout.split('\n').filter(line=>line.trimStart().startsWith('function '+name+'('));
    assert.equal(matches.length,1,name);return matches[0];
  }).join('\n');
  const saveLines=rendered.stdout.split('\n').filter(line=>line.includes("save.textContent='保存 SOP'"));
  assert.equal(saveLines.length,1);
  for(const content of ['Inspect the selected unit','Inspect {{from}}','Inspect {{to}}','Inspect {{other}}']){
    const writes=[],errors=[],sop={sop_id:'legacy-fixture',work_type:'adjust',adjust_sop_type:'cooling',name:'Legacy',version:1,
      steps:[{content}],attachments:[{name:'fixture.txt'}]};
    const context={assert,sop,litePollingPendingSopFiles:[],litePollingEditingSop:sop,
      pollingSopWorkType:()=> 'adjust',pollingSopScope:()=> 'A',editor:{querySelector:()=>null},
      document:{createElement:()=>({isConnected:true})},setButtonBusy:()=>{},setPollingSopFeedback:()=>{},
      loadPollingSops:async()=>{},renderPollingSopList:()=>{},renderPollingSopEditor:()=>{},setLiteStatus:()=>{},
      showLiteError:message=>errors.push(message),pollingApi:async(_url,options)=>{
        const body=JSON.parse(options.body);writes.push(body);return {...sop,...body,version:2};
      }};
    await vm.runInNewContext('(async()=>{'+helpers+'\n'+saveLines[0]+'\nawait save.onclick();})()',context);
    const invalid=/\{\{(?:to|other)\}\}/.test(content);
    assert.equal(writes.length,invalid?0:1);
    assert.equal(errors.length,invalid?1:0);
    if(!invalid){assert.equal(writes[0].adjust_sop_type,'cooling');assert.equal(writes[0].steps[0].content,content);}
  }
  vm.runInNewContext(helpers+`
    assert.equal(pollingSopIsAdjustCooling({steps:[{content:'plain'}]}),false);
    assert.equal(pollingSopIsAdjustCooling({steps:[{content:'{{from}}'}]}),true);
    assert.equal(pollingSopIsAdjustCooling({adjust_sop_type:'cooling',steps:[{content:'plain'}]}),true);
    assert.equal(pollingSopHasUnsupportedDevicePlaceholders({adjust_sop_type:'cooling',steps:[{content:'{{to}}'}]}),true);
  `,{assert,pollingSopWorkType:()=> 'adjust'});

  const fixture=`import {createApp,h} from 'vue';
import {NConfigProvider,NDialogProvider,NMessageProvider} from 'naive-ui';
import Sop from '/src/workspace-v2/NoticeSopManager.vue';
import {theme} from '/src/workspace-v2/theme.ts';
import '/src/global.css';import '/src/workspace-v2/workspace.css';
import '/src/workspace-v2/dashboard/dashboard.css';import '/src/workspace-v2/feedback.css';
document.documentElement.classList.add('workspace-v2');
createApp({render:()=>h(NConfigProvider,{themeOverrides:theme},()=>h(NMessageProvider,null,()=>h(NDialogProvider,null,()=>h('main',{class:'v2-main'},[h(Sop,{initialScope:'A',initialType:'adjust',scopes:[{value:'A',label:'A'}],types:[{value:'adjust',label:'Adjust'}],canDelete:true})]))))}).mount('#app');`;
  server=await createServer({root,configFile:false,cacheDir:path.join(scratch,'vite'),logLevel:'error',plugins:[{
    name:'adjust-sop-fixture',resolveId:id=>id==='/__adjust-entry.js'?id:null,load:id=>id==='/__adjust-entry.js'?fixture:null,
    configureServer(server){server.middlewares.use((req,res,next)=>{
      if(req.url!=='/__adjust-check')return next();res.setHeader('Content-Type','text/html');
      res.end('<!doctype html><meta name="viewport" content="width=device-width,initial-scale=1"><div id="app"></div><script type="module" src="/__adjust-entry.js"></script>');
    });}},vue()],server:{host:'127.0.0.1',port:0}});
  await server.listen();
  const origin='http://127.0.0.1:'+server.httpServer.address().port;
  browser=await chromium.launch({headless:true});
  const context=await browser.newContext({viewport:{width:1440,height:1000},reducedMotion:'reduce'}),writes=[],errors=[];
  let rows=[['ordinary','Ordinary fixture','Inspect the unit'],['legacy','Legacy cooling','Inspect {{from}}']].map(([sop_id,name,content])=>({
    sop_id,name,scope:'A',work_type:'adjust',version:1,attachments:[{attachment_id:'fixture-file',name:'fixture.txt'}],
    steps:[{step_id:sop_id+'-step',content,operator_required:true,reviewer_required:true,photo_required:false,time_limit_seconds:0,delay_reminder_minutes:0,repeat_rules:[]}],
  }));
  await context.route('**/*',async route=>{
    const request=route.request(),url=new URL(request.url());
    if(url.origin!==origin)return route.abort();
    if(!url.pathname.startsWith('/api/'))return route.continue();
    if(url.pathname==='/api/polling-sops'&&request.method()==='GET')return route.fulfill({json:{ok:true,data:{items:rows}}});
    if(url.pathname.startsWith('/api/polling-sops/')&&request.method()==='PUT'){
      const body=request.postDataJSON();writes.push(body);
      const previous=rows.find(row=>row.sop_id===body.sop_id);assert(previous);assert.equal(body.expected_version,previous.version);
      const saved={...previous,...body,version:previous.version+1};rows=rows.map(row=>row.sop_id===saved.sop_id?saved:row);
      return route.fulfill({json:{ok:true,data:saved}});
    }
    throw new Error('Unexpected fixture API: '+request.method()+' '+url.pathname);
  });
  const page=await context.newPage();page.on('pageerror',error=>errors.push(error.message));page.setDefaultTimeout(15000);
  const choose=()=>page.locator('[aria-label="调整SOP类型"]').first();
  const open=async name=>{await page.goto(origin+'/__adjust-check');await page.getByRole('button',{name,exact:false}).click();};
  const save=()=>page.getByRole('button',{name:'保存SOP',exact:true}).click();
  await open('Ordinary fixture');assert.match(await choose().innerText(),/普通设备调整/);
  await page.getByRole('button',{name:'修改',exact:true}).click();await choose().click();
  await page.locator('.n-base-select-option').filter({hasText:'制冷单元模式切换 SOP'}).click();await save();
  await page.getByRole('button',{name:'修改',exact:true}).waitFor();assert.equal(writes.length,1);
  assert.equal(writes[0].adjust_sop_type,'cooling');assert.equal(writes[0].steps[0].content,'Inspect the unit');
  await open('Ordinary fixture');assert.match(await choose().innerText(),/制冷单元模式切换/);
  await page.getByRole('button',{name:'修改',exact:true}).click();
  for(const token of ['{{to}}','{{other}}']){
    await page.getByRole('textbox',{name:'步骤1内容',exact:true}).fill('Inspect '+token);await save();
    await page.getByRole('alert').filter({hasText:'只允许使用'}).waitFor();assert.equal(writes.length,1);
  }
  await page.getByRole('textbox',{name:'步骤1内容',exact:true}).fill('Inspect {{from}}');await save();
  await page.getByRole('button',{name:'修改',exact:true}).waitFor();assert.equal(writes.length,2);
  await open('Legacy cooling');assert.match(await choose().innerText(),/制冷单元模式切换/);
  await page.getByRole('button',{name:'修改',exact:true}).click();
  await page.getByRole('textbox',{name:'步骤1内容',exact:true}).fill('Keep original operation text');await save();
  await page.getByRole('button',{name:'修改',exact:true}).waitFor();assert.equal(writes[2].adjust_sop_type,'cooling');
  await open('Legacy cooling');assert.match(await choose().innerText(),/制冷单元模式切换/);
  await mkdir(output,{recursive:true});await page.screenshot({path:path.join(output,'cooling-reopened.png'),fullPage:true});
  assert.deepEqual(errors,[]);console.log('Legacy save and Vue source-browser subtype regressions passed; no external requests or dist build.');
} finally {
  await browser?.close();await server?.close();await rm(scratch,{recursive:true,force:true});
}
