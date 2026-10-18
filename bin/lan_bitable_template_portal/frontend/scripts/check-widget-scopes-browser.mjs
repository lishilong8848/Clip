import assert from 'node:assert/strict';
import {mkdir} from 'node:fs/promises';
import path from 'node:path';
import {preview} from 'vite';
import {chromium} from 'playwright';
import {layoutApiFixture} from './layout-api-fixture.mjs';
const output=path.resolve('../../../output/playwright/widget-scopes');await mkdir(output,{recursive:true});
const server=await preview({...(process.env.CLIPFLOW_WORKSPACE_BUILD_DIR?{configFile:'vite.workspace-v2.config.ts'}:{}),logLevel:'error',preview:{host:'127.0.0.1',port:0,proxy:{}}});
const origin='http://127.0.0.1:'+server.httpServer.address().port;
const browser=await chromium.launch({headless:true});
const context=await browser.newContext({viewport:{width:1440,height:1000},reducedMotion:'reduce',hasTouch:true});
const layouts=layoutApiFixture(),reads=[],errors=[];
const user={open_id:'widget-scope-test',name:'隔离验收',role:'admin',level:'L1',category:'manager',home_scope:'A',modules:['maintenance','change','repair','polling','adjust','power','water','event']};
const options=['ALL','A','B','C'].map(value=>({value,label:value==='ALL'?'全部楼栋':value+'楼'}));
const base={scope:'A',module:'maintenance',display:'bar',period:'week',metric:'ongoing',presentation:'default',links:[],x:0,w:12,h:8};
const widgets=[{...base,id:'notices',kind:'module',module:'notices',title:'通告办理',y:0,h:10},
  {...base,id:'tasks',kind:'tasks',title:'业务待办',taskKinds:['notices'],scope:'B',y:10,h:5},
  {...base,id:'checks',kind:'pre_shift',title:'班前检查',y:15,h:5},
  {...base,id:'water',kind:'water',title:'水耗概览',y:20,h:5}];
for(const device of ['desktop','mobile'])layouts.records.set(user.open_id+':/api/workspace-layouts/'+device+'/workspace',{revision:1,value:{version:1,widgets:structuredClone(widgets)}});
const now=Date.now()/1000;
await context.route('**/*',async route=>{
  const req=route.request(),url=new URL(req.url());if(url.origin!==origin)return route.abort();
  if(url.pathname==='/assets/vnet-logo.png')return route.fulfill({path:path.resolve('public/assets/vnet-logo.png'),contentType:'image/png'});
  if(!url.pathname.startsWith('/api/'))return route.continue();
  if(await layouts.handle(route,user.open_id))return;
  assert.equal(req.method(),'GET','only isolated layout preferences may be changed');reads.push(url);
  const ok=data=>route.fulfill({json:{ok:true,data}}),scope=url.searchParams.get('scope')||'A';
  if(url.pathname==='/api/auth/status')return ok({logged_in:true,user,scope_options:options});
  if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'scopes-isolated'}});
  if(url.pathname==='/api/workbench'){
    const buildings=scope==='ALL'?['A','B','C']:[scope],data={source_snapshot_ready:true};
    for(const lane of ['records','ongoing']){
      const all=buildings.flatMap(building=>Array.from({length:3},(_,i)=>({record_id:building+'-'+lane+i,...(lane==='ongoing'?{active_item_id:building+'-active'+i}:{}),work_type:'maintenance',title:building+'楼'+(lane==='records'?'计划':'进行中')+i,building:building+'楼',scope:building,source_progress:lane==='records'?'未开始':'进行中',status:lane==='records'?'未开始':'进行中'})));
      const page=Number(url.searchParams.get(lane+'_page')||1),size=Number(url.searchParams.get(lane+'_page_size')||20);
      data[lane]=all.slice((page-1)*size,page*size);data[lane+'_pagination']={total:all.length,total_pages:Math.ceil(all.length/size)};
    }
    return ok(data);
  }
  if(url.pathname==='/api/workbench/lite-detail')return ok({fragments:{detail:`<form id="lite-notice-form"><input name="scope" type="hidden" value="${scope}"><input name="work_type" type="hidden" value="maintenance"><input name="notice_type" type="hidden" value="维保通告"><input name="source_record_id" type="hidden" value="source"><label><span>名称</span><input name="title" required value="${scope}楼办理测试"></label><button name="submit_action" value="start">开始</button></form>`}});
  if(url.pathname==='/api/workbench/draft')return ok({draft:null});
  if(url.pathname==='/api/pre-shift-checks')return ok({can_manage:true,scopes:['A','B','C'],scope,server_time:new Date(now*1000).toISOString(),roster_ready:true,roster_error:'',definition:{version:1,items:[{id:'x',text:'交接检查'}]},tasks:[],overview:{rows:['A','B','C'].map(scope=>({id:scope+'-day',scope,shift:'白',day:'2026-10-18',status:'待完成',total:1,completed:0}))}});
  if(url.pathname==='/api/capacity/water/bootstrap')return ok({summary:{record_count:1,total_usage:scope==='A'?10:20,photo_records:1}});
  if(url.pathname==='/api/scope-overview')return ok({source_snapshot_ready:true,scopes:Object.fromEntries(['A','B','C','ALL'].map(scope=>[scope,{maintenance_pending:3,maintenance_ongoing:3}]))});
  return ok({items:[],records:[],total:0,groups:[],scopes:[],settings:{},permissions:{},stats:{},people:[],fields:[],summary:{}});
});
const page=await context.newPage();page.setDefaultTimeout(15000);page.on('pageerror',error=>errors.push(error.message));
const notice=page.locator('.v2-dashboard .grid-stack-item[data-widget-id="notices"]>.v2-widget');
const tasks=page.locator('.v2-dashboard .grid-stack-item[data-widget-id="tasks"]>.v2-widget');
const checks=page.locator('.v2-dashboard .grid-stack-item[data-widget-id="checks"]>.v2-widget');
const savedScope=()=>layouts.records.get(user.open_id+':/api/workspace-layouts/desktop/workspace').value.widgets.find(item=>item.id==='notices').scope;
async function choose(label){const picker=notice.locator('.v2-widget-head .v2-widget-building-picker');await picker.click();await page.locator('.v2-widget-building-menu .n-base-select-option').getByText(label,{exact:true}).click();await page.keyboard.press('Escape');await page.locator('.v2-widget-building-menu').waitFor({state:'hidden'});}
try{
  await page.goto(origin+'/assets/workspace-v2/index.html');await notice.getByText('A楼计划0',{exact:true}).waitFor();
  assert.equal(await notice.locator('.v2-widget-head').getByRole('button',{name:'独立页面',exact:true}).count(),1);
  assert.equal(await notice.locator('.v2-module-toolbar').count(),0,'no duplicated scope/standalone toolbar');
  assert.equal(await notice.getByLabel('通告楼栋',{exact:true}).count(),0,'widget scope picker owns both notice lanes');
  assert.equal(await notice.locator('.v2-notice-types').getByRole('button',{name:'全部',exact:true}).count(),1);
  assert.equal(await notice.locator('[data-list="planned"]>header .v2-notice-plan-search').count(),1);
  assert.equal(await notice.locator('.v2-notice-command .v2-notice-filters').count(),0,'filters do not crowd the primary command row');
  assert.equal(await notice.locator('.v2-notices>.v2-notice-filters').count(),1,'shared filters stay local, directly below the primary commands');
  for(const width of [4,8,12]){
    await notice.evaluate((el,w)=>{const item=el.closest('.grid-stack-item');item.closest('.grid-stack').gridstack.update(item,{w});},width);
    await page.waitForTimeout(350);
    assert(await notice.locator('.v2-notice-command,.v2-notices>.v2-notice-filters').evaluateAll(nodes=>nodes.every(el=>el.scrollWidth<=el.clientWidth+1)),'notice toolbar responds to widget width, not only viewport width');
  }
  await tasks.scrollIntoViewIfNeeded();await tasks.getByText('B楼进行中0',{exact:true}).waitFor();
  await checks.scrollIntoViewIfNeeded();await checks.locator('.v2-check-overview').waitFor();
  assert.equal(await checks.locator('.v2-widget-head').getByRole('button',{name:'刷新班前检查',exact:true}).count(),1,'checklist refresh belongs in the compact widget header');
  assert.equal(await checks.locator('.v2-widget-body').getByRole('button',{name:'刷新班前检查',exact:true}).count(),0,'no duplicated body refresh');
  assert.equal(await checks.getByLabel('检查楼栋',{exact:true}).count(),0);
  assert.equal(await checks.getByText('本楼检查',{exact:true}).count()+await checks.getByText('完成总览',{exact:true}).count(),0);
  await checks.locator('.v2-check-overview button').first().click();await checks.getByLabel('检查楼栋',{exact:true}).waitFor();await checks.getByRole('button',{name:'返回楼栋列表',exact:true}).click();
  await notice.scrollIntoViewIfNeeded();await notice.locator('.v2-notice-types').getByRole('button',{name:'维保通告',exact:true}).click();
  const writesBefore=layouts.writes;
  await notice.locator('.v2-widget-building-picker').click();
  for(const label of ['B楼','C楼','C楼']){
    await page.locator('.v2-widget-building-menu .n-base-select-option').getByText(label,{exact:true}).click();
    await page.waitForTimeout(240);
    assert(await page.locator('.v2-widget-building-menu').isVisible(),'selecting a building keeps the menu open');
    assert.equal(layouts.writes,writesBefore,'selection is saved once when the menu closes, not on every click');
  }
  await page.mouse.move(900,80);await page.locator('.v2-widget-building-menu').waitFor({state:'hidden'});
  await notice.getByText('B楼计划0',{exact:true}).waitFor();await notice.getByText('B楼进行中0',{exact:true}).waitFor();assert.equal(savedScope(),'A,B');
  assert.equal(layouts.writes,writesBefore+1,'leaving the complete dropdown commits a single layout update');
  assert.equal(await notice.getByText('C楼计划0',{exact:true}).count(),0);
  assert.equal(await notice.locator('.v2-notice-types').getByRole('button',{name:'维保通告',exact:true}).getAttribute('aria-pressed'),'true','changing buildings retains the active notice type');
  assert.equal(await tasks.getByText('A楼进行中0',{exact:true}).count(),0,'another widget retains its own building');
  await notice.locator('.v2-widget-building-picker .n-tag__close').first().click();await notice.getByText('A楼计划0',{exact:true}).waitFor({state:'hidden'});assert.equal(savedScope(),'B','removing a visible tag still saves while the menu is closed');
  await choose('全部授权楼栋');await notice.getByText('C楼计划0',{exact:true}).waitFor();assert.equal(savedScope(),'ALL');
  await choose('B楼');await notice.getByText('C楼计划0',{exact:true}).waitFor({state:'hidden'});assert.equal(savedScope(),'B');
  await page.reload();await notice.getByText('B楼计划0',{exact:true}).waitFor();assert.equal(await notice.getByText('A楼计划0',{exact:true}).count(),0,'saved widget selection survives login/page reload');
  await notice.locator('[data-list="planned"] .v2-notice-entry').first().getByRole('button',{name:'开始',exact:true}).click();await page.locator('.v2-notice-editor').waitFor();
  assert.equal(reads.filter(url=>url.pathname==='/api/workbench/lite-detail').at(-1).searchParams.get('scope'),'B');
  const title=page.locator('.v2-notice-editor').getByRole('textbox').first();
  const originalTitle=await title.inputValue();
  await title.fill('B楼保留本次填写');
  const authReads=reads.filter(url=>url.pathname==='/api/auth/status').length;
  await page.evaluate(()=>window.dispatchEvent(new Event('auth-account-changed')));
  await page.waitForTimeout(400);
  assert(reads.filter(url=>url.pathname==='/api/auth/status').length>authReads);
  assert(await page.locator('.v2-notice-editor').isVisible(),'same account status refresh must not close an embedded editor');
  assert.equal(await title.inputValue(),'B楼保留本次填写','status refresh cannot reset an in-progress notice draft');
  await title.fill(originalTitle);
  await page.goto(origin+'/assets/workspace-v2/index.html');
  for(const width of [1440,360,375,390]){
    await page.setViewportSize({width,height:width<700?667:1000});await notice.scrollIntoViewIfNeeded();await notice.getByText(/楼计划0/).first().waitFor();
    assert(!await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1));
    const geometry=await notice.locator('.v2-widget-head').evaluate(el=>{const name=el.querySelector('h2').getBoundingClientRect(),picker=el.querySelector('.v2-widget-building-picker').getBoundingClientRect();return {fits:el.scrollWidth<=el.clientWidth+1,sameRow:Math.abs(name.top+name.height/2-picker.top-picker.height/2)<8,ordered:picker.left>=name.right};});
    assert(geometry.fits&&geometry.sameRow&&geometry.ordered,JSON.stringify({width,geometry}));
    const plan=notice.locator('[data-list="planned"]>header');assert(await plan.evaluate(el=>el.scrollWidth<=el.clientWidth+1));
    await page.evaluate(()=>{window.scrollTo(0,0);document.querySelector('.v2-main').scrollTop=0;});await notice.locator('.v2-widget-head').scrollIntoViewIfNeeded();await page.screenshot({path:path.join(output,'notices-'+width+'.png')});
  }
  await notice.locator('.v2-widget-building-picker').tap();await page.locator('.v2-widget-building-menu .n-base-select-option').getByText('A楼',{exact:true}).tap();
  await page.waitForTimeout(240);assert(await page.locator('.v2-widget-building-menu').isVisible(),'touch selections do not close the menu');
  await page.touchscreen.tap(10,90);await page.locator('.v2-widget-building-menu').waitFor({state:'hidden'});await notice.getByText('A楼计划0',{exact:true}).waitFor();
  await page.setViewportSize({width:1440,height:1000});
  await page.goto(origin+'/assets/workspace-v2/index.html?__page=/workbench-lite&scope=ALL&buildings=A,B');
  await page.getByText('A楼计划0',{exact:true}).waitFor();await page.getByText('B楼计划0',{exact:true}).waitFor();assert.equal(await page.getByText('C楼计划0',{exact:true}).count(),0);
  const standalonePicker=page.locator('.v2-notice-filters .v2-widget-building-picker');await standalonePicker.click();
  await page.waitForTimeout(450);assert(await page.locator('.v2-widget-building-menu').isVisible(),'hovering collapsed selection tags cannot dismiss the real menu');
  await page.locator('.n-base-select-option').getByText('C楼',{exact:true}).click();await page.keyboard.press('Escape');await page.getByText('C楼计划0',{exact:true}).waitFor();
  assert.deepEqual(errors,[]);console.log('PASS: widget scopes single/multi/all, scoped operations, independent widgets, account persistence, compact notice tools, checklist building drill, PC/mobile and standalone multi-building filters.');
}catch(error){await page.screenshot({path:path.join(output,'failure.png'),fullPage:true});console.error(errors);throw error;}
finally{await browser.close();await server.close();}
