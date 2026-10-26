import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {mkdir, mkdtemp, rm, writeFile} from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {preview} from 'vite';
import {chromium} from 'playwright';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const repo=path.resolve(root,'../../..'),output=path.join(repo,'output/playwright/repair-plan-binding');
await mkdir(output,{recursive:true});
const temporary=await mkdtemp(path.join(os.tmpdir(),'clipflow-binding-ui-'));
let generated;
try{
 generated=spawnSync(path.join(repo,'bin/.venv/Scripts/python.exe'),['-c',`
import json,sys
sys.path.insert(0,'bin')
from lan_bitable_template_portal.workbench_lite import render_workbench_lite,extract_workbench_lite_fragments
from test_lighthouse_notice_command_regression import VALID_STARTS
draft={**VALID_STARTS['repair'],'title':'EA118_C01机房A楼现场设备异常检修','building_codes':['A'],'scope':'A','start_time':'2026-10-26 18:00','end_time':'2026-10-26 08:00'}
ongoing={**draft,'active_item_id':'local-fixture','record_id':'manual:lite:A:repair:fixture','target_record_id':'','work_type':'repair','notice_type':'设备检修','status':''}
args=dict(session={'role':'building','allowed_modules':['repair']},scope='A',work_type='repair',month='10月')
manual=render_workbench_lite(**args,payload={'records':[],'ongoing':[]},manual=True,parsed_draft=draft)
pending=render_workbench_lite(**args,payload={'records':[],'ongoing':[ongoing]},active_item_id='local-fixture')
print(json.dumps({'manual':manual,'pending':pending,'details':{'manual':extract_workbench_lite_fragments(manual)['detail'],'pending':extract_workbench_lite_fragments(pending)['detail']},'ongoing':ongoing},ensure_ascii=False))
`],{cwd:repo,encoding:'utf8',maxBuffer:12*1024*1024,env:{...process.env,CLIPFLOW_DATA_DIR:temporary,PYTHONIOENCODING:'utf-8'}});
}finally{
 assert.equal(path.dirname(path.resolve(temporary)),path.resolve(os.tmpdir()));
 assert(path.basename(temporary).startsWith('clipflow-binding-ui-'));
 await rm(temporary,{recursive:true,force:true});
}
assert.equal(generated.status,0,generated.stderr);
const fixtures=JSON.parse(generated.stdout);
await writeFile(path.join(output,'fixtures.json'),generated.stdout);
const server=await preview({root,logLevel:'error',preview:{host:'127.0.0.1',port:19130}});
const origin='http://127.0.0.1:'+server.httpServer.address().port;
const browser=await chromium.launch({headless:true});
const png=Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Wl6oDQAAAAASUVORK5CYII=','base64');
const candidate={source_record_id:'rec_plan_0504',title:'维修单原始标题',building:'A楼',progress:'未开始'};
try{
 for(const mode of ['legacy-manual','legacy-parsed','native-manual','native-pending']){
  const context=await browser.newContext({viewport:{width:1440,height:900},reducedMotion:'reduce'});
  const errors=[],writes=[];let reads=0,prefills=0,release,started;
  const prefillStarted=new Promise(resolve=>started=resolve);
  const waitForPrefill=new Promise(resolve=>release=resolve);
  const legacy=mode.startsWith('legacy'),detail=fixtures.details[mode.endsWith('pending')?'pending':'manual'];
  await context.route('**/*',async route=>{
   const req=route.request(),url=new URL(req.url());
   if(url.origin!==origin)return route.abort();
   const ok=data=>route.fulfill({json:{ok:true,data}});
   if(url.pathname==='/__legacy')return route.fulfill({contentType:'text/html',body:mode==='legacy-manual'?fixtures.manual.replace(/ data-parsed-keys="[^"]*"/g,''):fixtures.manual});
   if(!url.pathname.startsWith('/api/'))return route.continue();
   if(req.method()!=='GET')writes.push({path:url.pathname,body:req.postData()});
   if(url.pathname==='/api/auth/status')return ok({logged_in:true,user:{open_id:'ou_binding_fixture',name:'测试值班员',role:'building',level:'L4',modules:['repair']},scope_options:[{value:'A',label:'A楼'}]});
   if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'isolated-binding'}});
   if(url.pathname==='/api/workbench')return ok({records:[],ongoing:[fixtures.ongoing],source_snapshot_ready:true,records_pagination:{total:0},ongoing_pagination:{total:1}});
   if(url.pathname==='/api/workbench/lite-detail'){reads++;return ok({fragments:{detail}});}
   if(url.pathname==='/api/workbench/draft')return ok({draft:null});
   if(url.pathname==='/api/workbench/source-options')return ok({items:[candidate]});
   if(url.pathname==='/api/workbench/repair-event-prefill'){
    prefills++;assert.equal(url.searchParams.get('repair_management_record_id'),candidate.source_record_id);
    started();await waitForPrefill;
    return ok({source_record_id:candidate.source_record_id,repair_management_record_id:candidate.source_record_id,target_record_id:'',draft:{title:candidate.title,reason:'计划原始原因',location:'',solution:'计划补充方案'}});
   }
   if(url.pathname==='/api/notice-attachments')return ok({upload_id:'local-photo',file_name:'现场照片.png',preview_url:'/api/notice-images/'+'a'.repeat(32)});
   if(url.pathname.startsWith('/api/notice-images/'))return route.fulfill({contentType:'image/png',body:png});
   if(req.method()!=='GET')throw Error('Unexpected business write '+url.pathname);
   return ok({items:[],records:[],settings:{},available:false});
  });
  const page=await context.newPage();page.setDefaultTimeout(15000);page.on('pageerror',e=>errors.push(e.message));
  await page.goto(origin+(legacy?'/__legacy':'/assets/workspace-v2/index.html?scope=A&__page=/workbench-lite&work_type=repair&active_item_id=local-fixture'));
  const field=key=>legacy?page.locator('#lite-notice-form [name="'+key+'"]'):page.locator('[data-notice-field="'+key+'"] input,[data-notice-field="'+key+'"] textarea').first();
  await field('title').waitFor();
  const values={title:'EA118_C01机房A楼用户手填故障检修',location:'用户填写的机房位置',reason:'用户填写的故障原因',progress:'用户填写的进度'};
  for(const [key,value] of Object.entries(values))await field(key).fill(value);
  await field('solution').fill('');
  if(legacy){
   await page.locator('[data-manual-binding-mode="bind"]').click();
   await page.locator('[data-manual-source-index="0"]').click();
   await page.locator('#lite-manual-source-confirm').click();
   await prefillStarted;
   values.reason='读取维修单期间继续填写的原因';await field('reason').fill(values.reason);
   release();
   await page.waitForFunction(()=>document.querySelector('#lite-notice-form [name=source_record_id]')?.value==='rec_plan_0504');
   assert.equal(await field('solution').inputValue(),'计划补充方案');
   assert.equal(prefills,1);
  }else{
   await page.getByLabel('添加现场照片',{exact:true}).setInputFiles({name:'现场照片.png',mimeType:'image/png',buffer:png});
   await page.getByText('本次新增 · 已就绪',{exact:true}).waitFor();
   const binding=page.locator('[data-notice-section=binding]');
   if(await binding.getByRole('button',{name:'选择办理方式',exact:true}).isVisible())await binding.getByRole('button',{name:'选择办理方式',exact:true}).click();
   await binding.getByText('关联计划',{exact:true}).click();
   await binding.locator('.n-select').click();
   await page.locator('.n-base-select-option').getByText(/维修单原始标题/).click();
   const before=reads;await binding.getByRole('button',{name:'确认关联',exact:true}).click();
   await binding.getByText('已关联计划',{exact:true}).waitFor();
   assert.equal(reads,before,'binding an unuploaded notice must not reopen the plan');
   assert.equal(await page.getByText('本次新增 · 已就绪',{exact:true}).count(),1,'selected photos remain attached');
   const stored=await page.evaluate(()=>Object.keys(sessionStorage).filter(k=>k.startsWith('v2-notice:')).map(k=>JSON.parse(sessionStorage[k])).find(v=>v.snapshot?.values.source_record_id==='rec_plan_0504'));
   assert.equal(stored.snapshot.values.repair_management_record_id,candidate.source_record_id);
   assert.equal(stored.snapshot.photos[0].upload_id,'local-photo');
  }
  for(const [key,value] of Object.entries(values))assert.equal(await field(key).inputValue(),value,mode+' keeps '+key);
  assert.equal(writes.filter(w=>w.path!=='/api/notice-attachments').length,0,'choosing a plan does not send notices or save remote records');
  assert.deepEqual(errors,[]);
  await page.screenshot({path:path.join(output,mode+'.png'),fullPage:true});
  for(const width of [402,440]){
   await page.setViewportSize({width,height:874});
   assert.equal(await field('title').inputValue(),values.title);
   if(!legacy)assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false);
   await page.screenshot({path:path.join(output,mode+'-'+width+'.png'),fullPage:true});
  }
  console.log('PASS: '+mode+' keeps hand-written/parsed content, identity and local attachments; no business sends.');
  await context.close();
 }
}finally{await browser.close();await server.close();}
