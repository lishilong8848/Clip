import assert from 'node:assert/strict';
import path from 'node:path';
import {spawnSync} from 'node:child_process';
import {mkdir} from 'node:fs/promises';
import {chromium} from 'playwright';
import {preview} from 'vite';
import {fileURLToPath} from 'node:url';
const root=fileURLToPath(new URL('..',import.meta.url));
const project=path.resolve(root,'../../..'),output=path.join(project,'output/playwright/multiple-work-order');
await mkdir(output,{recursive:true});
const legacy=spawnSync(path.join(project,'bin/.venv/Scripts/python.exe'),['-c',"import sys;sys.path.insert(0,'bin');from lan_bitable_template_portal.workbench_lite import render_polling_work_order_steps_page;print(render_polling_work_order_steps_page())"],{cwd:project,encoding:'utf8',env:{...process.env,PYTHONIOENCODING:'utf-8'}});
assert.equal(legacy.status,0,legacy.stderr);
const server=await preview({root,...(process.env.CLIPFLOW_WORKSPACE_BUILD_DIR?{configFile:path.join(root,'vite.workspace-v2.config.ts')}:{}),logLevel:'error',preview:{host:'127.0.0.1',port:19140,strictPort:true}});
const origin=`http://127.0.0.1:${server.httpServer.address().port}`;
const browser=await chromium.launch({headless:true}),errors=[],writes=[];
const initial=()=>({group_id:'group_fixture',title:'隔离测试工单',role:'operator',role_label:'操作人',assigned_person:{name:'测试操作人'},work_type:'maintenance',state:'active',version:1,current_run_index:0,can_release_selection:false,can_rollback_previous:false,work_orders:[{run_index:1,label:'设备维护',completed_steps:0,step_count:1,selectable:true,state:'available'}],steps:[]});
let state=initial();
const step=()=>({step_key:'step_fixture',step_index:1,position:'current',run_index:1,content:'核对设备状态',photo_required:true,operator_required:true,reviewer_required:true,operator_confirmed:false,reviewer_confirmed:false,photos:[]});
try{
 const context=await browser.newContext();
 await context.route('**/*',async route=>{
  const req=route.request(),url=new URL(req.url());if(url.origin!==origin)return route.abort();
  if(url.pathname==='/legacy-work-order')return route.fulfill({contentType:'text/html',body:legacy.stdout});
  if(url.pathname.startsWith('/polling-work-order'))return route.fulfill({contentType:'text/html',body:legacy.stdout});
  if(!url.pathname.startsWith('/api/'))return route.continue();
  const ok=data=>route.fulfill({json:{ok:true,data}});
  if(url.pathname==='/api/auth/status')return ok({logged_in:false,user:{},scope_options:[]});
  if(url.pathname.startsWith('/api/polling-work-orders/photos/'))return route.fulfill({contentType:'image/png',body:Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aZQAAAABJRU5ErkJggg==','base64')});
  if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend'}});
  if(url.pathname==='/api/polling-work-orders/session')return ok(state);
  if(req.method()==='POST'){
   writes.push(url.pathname);
   if(url.pathname.endsWith('/photo')){assert.equal(url.searchParams.get('expected_version'),String(state.version));state.version++;state.steps[0].photos.push({photo_id:'fixture_photo'+state.version,name:'操作照片',preview_url:'/api/polling-work-orders/photos/fixture'+state.version});return ok(state);}
   const data=req.postDataJSON();assert.equal(data.token,'fixture_token');assert.equal(data.expected_version,state.version);state.version++;
   if(url.pathname.endsWith('/activate')){state.current_run_index=1;state.can_release_selection=true;state.steps=[step()];return ok(state);}
   if(url.pathname.endsWith('/confirm')){assert.equal(data.step_key,'step_fixture');assert.equal(state.steps[0].photos.length,1);state.steps[0].operator_confirmed=true;state.can_release_selection=false;return ok({session:state});}
   if(url.pathname.endsWith('/release')){state.current_run_index=0;state.steps=[];return ok(state);}
   throw new Error('Unexpected write '+url.pathname);
  }
  return ok({});
 });
 const page=await context.newPage();page.on('pageerror',e=>errors.push(e.message));
 await page.goto(origin+'/assets/workspace-v2/index.html?__page=/polling-work-order&token=fixture_token');
 await page.getByRole('button',{name:'进入步骤',exact:true}).click();
 await page.getByText('核对设备状态',{exact:true}).waitFor();assert.equal(await page.locator('iframe').count(),0);
 assert(await page.getByRole('button',{name:'请先上传操作照片',exact:true}).isDisabled());
 await page.locator('.v2-order-upload input').setInputFiles({name:'fixture.png',mimeType:'image/png',buffer:Buffer.from('fixture image')});
 await page.getByRole('button',{name:'确认当前步骤（操作人）',exact:true}).click();
 await page.getByRole('button',{name:'已确认',exact:true}).waitFor();assert(await page.getByRole('button',{name:'已确认',exact:true}).isDisabled());
 assert.equal(await page.getByRole('button',{name:'回退上一步',exact:true}).count(),0,'operator cannot roll back reviewer steps');
 for(const width of [1440,390]){await page.setViewportSize({width,height:900});assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false);}
 await page.getByRole('button',{name:'返回工单',exact:true}).click();await page.getByRole('button',{name:'进入步骤',exact:true}).waitFor();assert.equal(writes.filter(p=>p.endsWith('/release')).length,0,'return after confirmation cannot release selection');
 state.role='reviewer';state.role_label='现场审核人';state.current_run_index=1;
 for(const surface of ['new','legacy']){
  await page.goto(surface==='new'?origin+'/assets/workspace-v2/index.html?__page=/polling-work-order/steps&token=fixture_token':origin+'/polling-work-order/steps?token=fixture_token&run_index=1');
  const input=page.locator(surface==='new'?'.v2-order-upload input':'.photo-upload input');
  await input.waitFor({state:'attached'});assert(await input.isEnabled(),'reviewer can add photos after operator confirmation');assert(await input.locator('..').isVisible());
  const chooser=page.waitForEvent('filechooser'),response=page.waitForResponse(r=>r.url().includes('/api/polling-work-orders/photo?'));await input.locator('..').click();await (await chooser).setFiles({name:'review.png',mimeType:'image/png',buffer:Buffer.from('review fixture')});await response;
  await page.waitForFunction(()=>Array.from(document.querySelectorAll('.v2-order-photos img,.photos img')).every(img=>img.complete&&img.naturalWidth>0));
  for(const width of [1440,402,440]){await page.setViewportSize({width,height:width===402?874:width===440?956:900});assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false);await page.screenshot({path:path.join(output,`${surface}-${width}.png`)});}
 }
 assert.equal(writes.filter(p=>p.endsWith('/photo')).length,3);
 assert.deepEqual(errors,[]);console.log('PASS: new signed work-order page, original token/version API, photo gate, operator confirmation and responsive layout.');
}finally{await browser.close();await server.close();}
