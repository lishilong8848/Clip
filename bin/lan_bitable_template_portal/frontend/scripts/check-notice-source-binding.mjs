import assert from 'node:assert/strict';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {mkdir} from 'node:fs/promises';
import {preview} from 'vite';
import {chromium} from 'playwright';

if(!process.env.CLIPFLOW_WORKSPACE_BUILD_DIR)throw new Error('Use an isolated workspace build.');
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const server=await preview({root,configFile:path.join(root,'vite.workspace-v2.config.ts'),logLevel:'error',preview:{host:'127.0.0.1',port:0}});
const origin='http://127.0.0.1:'+server.httpServer.address().port;
const screenshots=path.resolve(root,'../../../output/side-notice-source-cycle/screenshots');
await mkdir(screenshots,{recursive:true});
const browser=await chromium.launch({headless:true});
const errors=[],writes=[];
let bound=false;
const title='B楼手填过滤器检查';
const details=()=>`<form id="lite-notice-form" data-action="${bound?'start':'update'}" data-detail-mode="ongoing">
  ${Object.entries({scope:'B',work_type:'maintenance',notice_type:'维保通告',active_item_id:'active-manual',target_record_id:'rec-manual',source_record_id:bound?'rec-month':'',source_month:'10月',site_photo_count:'1'}).map(([key,value])=>`<input type="hidden" name="${key}" value="${value}">`).join('')}
  <input type="checkbox" name="building_codes" checked value="B">
  <label><span>名称</span><input name="title" value="${title}" required></label>
  <label><span>进度</span><textarea name="progress">现场进行中</textarea></label>
  ${bound?'<button name="submit_action" value="start"></button>':''}<button name="submit_action" value="update"></button><button name="submit_action" value="end"></button>
  </form>`;
const ok=(route,data)=>route.fulfill({json:{ok:true,data}});
try{
 const context=await browser.newContext({viewport:{width:1440,height:1000},reducedMotion:'reduce'});
 await context.route('**/*',async route=>{
  const request=route.request(),url=new URL(request.url());
  if(url.origin!==origin)return route.abort();
  if(!url.pathname.startsWith('/api/'))return route.continue();
  if(request.method()!=='GET')writes.push(url.pathname);
  if(url.pathname==='/api/auth/status')return ok(route,{logged_in:true,user:{open_id:'source-fixture',name:'隔离测试',role:'admin',modules:['maintenance']},scope_options:[{value:'B',label:'B楼'}]});
  if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'source-fixture'}});
  if(url.pathname==='/api/workbench')return ok(route,{source_snapshot_ready:true,records:[],ongoing:[
   {active_item_id:'active-manual',target_record_id:'rec-manual',source_record_id:bound?'rec-month':'',work_type:'maintenance',title,status:'开始',building:'B楼'},
   {active_item_id:'active-source',target_record_id:'rec-source-target',source_record_id:'rec-source',work_type:'maintenance',title:'B楼已绑定计划',status:'开始',building:'B楼'}
  ],records_pagination:{total:0},ongoing_pagination:{total:2}});
  if(url.pathname==='/api/workbench/lite-detail'){
   const unresolved=url.searchParams.get('record_id')==='fixture-missing';
   const recovered=url.searchParams.get('active_item_id')==='active-recovered';
   const detail=unresolved?details().replace('data-detail-mode="ongoing"','data-detail-mode="manual"')
     .replace('name="active_item_id" value="active-manual"','name="active_item_id" value=""')
     .replace('name="target_record_id" value="rec-manual"','name="target_record_id" value=""')
     .replace('name="source_record_id" value="rec-month"','name="source_record_id" value=""')
     .replace('<button name="submit_action" value="start"></button>','')
     .replace('data-action="start"','data-action="update"')
     :recovered?details().replace('active-manual','active-recovered').replace('rec-manual','rec-recovered'):details();
   return ok(route,{fragments:{detail}});
  }
  if(url.pathname==='/api/workbench/draft')return ok(route,{draft:null});
  if(url.pathname==='/api/workbench/source-options')return ok(route,{items:[
   {source_record_id:'rec-month',title:'同名过滤器检查',maintenance_cycle:'月度',building:'B楼',progress:'未开始'},
   {source_record_id:'rec-quarter',title:'同名过滤器检查',maintenance_cycle:'季度',building:'B楼',progress:'未开始'}
  ]});
  if(url.pathname==='/api/notice-identity/bind'){
   assert.equal(request.method(),'POST');const body=request.postDataJSON();
   if(body.target_record_id==='rec-recovered'){
    assert.equal(body.binding_context,'ongoing');
    assert.equal(body.source_record_id,'');
    return ok(route,{active_item_id:'active-recovered',target_record_id:'rec-recovered'});
   }
   assert.equal(body.binding_context,'ongoing');assert.equal(body.source_binding_only,true);
   assert.equal(body.source_record_id,'rec-month');assert.equal(body.target_record_id,'rec-manual');
   assert.equal(body.active_item_id,'active-manual');assert.equal(body.source_month,'10月');
   bound=true;return ok(route,{active_item_id:'active-manual',source_record_id:'rec-month',target_record_id:'rec-manual'});
  }
  if(url.pathname==='/api/notice-target-candidates'){
   assert.equal(request.postDataJSON().lookup_context,'scoped_active_target_table');
   return ok(route,{candidates:[{active_item_id:'active-recovered',target_record_id:'rec-recovered',title:'本次已发维保',building:'B楼',status:'开始',start_time:'2026-11-18 09:30'}]});
  }
  if(request.method()!=='GET')throw new Error('Unexpected business write: '+url.pathname);
  return ok(route,{items:[],records:[],settings:{},available:false});
 });
 const page=await context.newPage();page.on('pageerror',error=>errors.push(error.message));
 await page.goto(origin+'/assets/workspace-v2/index.html?__page=/workbench-lite&scope=B&work_type=maintenance&month=10月');
 const lane=page.locator('[data-list=ongoing]'),manual=lane.locator('.v2-notice-entry').filter({hasText:title});
 await manual.getByText('纯手填',{exact:true}).waitFor();
 await lane.locator('.v2-notice-entry').filter({hasText:'B楼已绑定计划'}).getByText('源表事项',{exact:true}).waitFor();
 await page.screenshot({path:path.join(screenshots,'notice-source-desktop.png'),fullPage:true});
 for(const width of [360,390,430]){
  await page.setViewportSize({width,height:844});
  const ongoingTab=page.getByRole('tab',{name:/进行中/}).first();if(await ongoingTab.isVisible())await ongoingTab.click();
  await manual.getByRole('button',{name:'绑定源表',exact:true}).waitFor();
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false,'mobile page fits '+width);
  if(width===390)await page.screenshot({path:path.join(screenshots,'notice-source-mobile.png'),fullPage:true});
 }
 await page.setViewportSize({width:1440,height:1000});
 assert.equal(await lane.locator('.v2-notice-entry').filter({hasText:'B楼已绑定计划'}).getByRole('button',{name:'绑定源表',exact:true}).count(),0);
 await manual.getByRole('button',{name:'绑定源表',exact:true}).click();
 const binding=page.locator('[data-notice-section=binding]');await binding.getByText('独立通告 · 未关联计划',{exact:true}).waitFor();
 await binding.getByText('关联计划',{exact:true}).click();
 await binding.locator('.n-select').click();
 await page.locator('.n-base-select-option').getByText('同名过滤器检查 · 月度 · B楼 · 未开始',{exact:true}).click();
 await page.screenshot({path:path.join(screenshots,'notice-source-binding.png'),fullPage:true});
 await binding.getByRole('button',{name:'确认关联',exact:true}).click();
 assert.equal(writes.length,0,'selecting a plan is not a business submission');
 await page.getByRole('button',{name:'返回核对',exact:true}).click();
 assert.equal(writes.length,0,'cancelling confirmation does not bind');
 await binding.getByRole('button',{name:'确认关联',exact:true}).click();
 await page.getByRole('button',{name:'确认绑定',exact:true}).click();
 await binding.getByText('已关联计划',{exact:true}).first().waitFor();
  await page.getByRole('button',{name:'预览并确认更新',exact:true}).waitFor();
  assert.equal(await page.getByRole('button',{name:'预览并确认开始',exact:true}).count(),0,'binding preserves the user-selected update even if a stale source proposes start');
 await manual.getByText('源表事项',{exact:true}).waitFor();
 assert.deepEqual(writes,['/api/notice-identity/bind'],'binding reuses the existing API and never resends the notice');
 assert.equal(await binding.locator('.n-select').count(),0,'successful association collapses the picker');
 await binding.getByRole('button',{name:'更换关联',exact:true}).click();
 assert.equal(await binding.getByRole('button',{name:'确认关联',exact:true}).isDisabled(),true);
 await page.locator('.v2-notice-editor-footer').getByRole('button',{name:'关闭',exact:true}).click();
 await page.setViewportSize({width:390,height:844});
 const ongoingTab=page.getByRole('tab',{name:/进行中/}).first();if(await ongoingTab.isVisible())await ongoingTab.click();
 await manual.getByText('源表事项',{exact:true}).waitFor();
 assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false,'mobile has no page overflow');
 await page.goto(origin+'/assets/workspace-v2/index.html?__page=/workbench-lite&scope=B&work_type=maintenance&record_id=fixture-missing');
 const editor=page.locator('.v2-notice-editor');
 await editor.locator('textarea').fill('本次更新进度保留');
 await editor.getByRole('button',{name:'查找原通告',exact:true}).click();
 assert.equal(await editor.getByRole('button',{name:'预览并确认更新',exact:true}).count(),0);
 await editor.locator('.v2-relation-picker .n-select').click();
 await page.getByText('本次已发维保 · B楼 · 开始 · 2026-11-18 09:30',{exact:true}).click();
 await editor.getByRole('button',{name:'核对并保存关联',exact:true}).click();
 await page.getByRole('button',{name:'确认关联并重新读取',exact:true}).click();
 await editor.getByRole('button',{name:'预览并确认更新',exact:true}).waitFor();
 assert.equal(await editor.locator('textarea').inputValue(),'本次更新进度保留');
 assert.deepEqual(writes,['/api/notice-identity/bind','/api/notice-target-candidates','/api/notice-identity/bind']);
 assert.deepEqual(errors,[]);
 console.log('PASS: PC/mobile source binding, missing-target recovery, retained progress and no notice resend. All business writes mocked.');
}finally{await browser.close();await server.close();}
