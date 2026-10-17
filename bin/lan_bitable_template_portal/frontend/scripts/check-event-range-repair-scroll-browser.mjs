import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { preview } from 'vite';
import { chromium } from 'playwright';
import { layoutApiFixture } from './layout-api-fixture.mjs';

assert(process.env.CLIPFLOW_WORKSPACE_BUILD_DIR,'An isolated V2 build is required.');
const output=path.resolve('../../../output/playwright/event-range-repair-scroll');
await mkdir(output,{recursive:true});
const server=await preview({configFile:'vite.workspace-v2.config.ts',logLevel:'error',preview:{host:'127.0.0.1',port:0,proxy:{}}});
const origin=`http://127.0.0.1:${server.httpServer.address().port}`, entry=origin+'/assets/workspace-v2/index.html';
const browser=await chromium.launch({headless:true});
const errors=[],requests=[],writes=[];
const fixture={empty:true};
let inFlight=0,maxInFlight=0;
try {
  const ctx=await browser.newContext({viewport:{width:1440,height:900},reducedMotion:'reduce',timezoneId:'America/Los_Angeles'});
  const layouts=layoutApiFixture();
  await ctx.route('**/*',async route=>{
    const req=route.request(),url=new URL(req.url());
    if(url.origin!==origin)return route.abort();
    if(url.pathname==='/assets/vnet-logo.png')return route.fulfill({path:path.resolve('public/assets/vnet-logo.png'),contentType:'image/png'});
    if(!url.pathname.startsWith('/api/'))return route.continue();
    if(await layouts.handle(route,'isolated-range'))return;
    if(req.method()!=='GET'){writes.push(url.pathname);return route.fulfill({status:400,json:{ok:false,error:'Writes disabled'}});}
    const ok=data=>route.fulfill({json:{ok:true,data}});
    if(url.pathname==='/api/auth/status')return ok({logged_in:true,user:{open_id:'isolated-range',name:'测试值班员',role:'building',level:'L4',category:'operator',modules:['event','repair_management']},scope_options:[{value:'A',label:'A楼'},{value:'B',label:'B楼'}]});
    if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'range-scroll-fixture'}});
    if(url.pathname==='/api/events/monthly'){
      requests.push(Object.fromEntries(url.searchParams));inFlight++;maxInFlight=Math.max(maxInFlight,inFlight);
      await new Promise(resolve=>setTimeout(resolve,50));
      const month=url.searchParams.get('month');
      const records=[['first','2026-01-15 00:00:00'],['middle','2026-02-10 09:00:00'],['last','2026-03-15 23:59:59'],['excluded','2026-03-16 00:00:00']]
        .filter(([,time])=>time.startsWith(month)).map(([record_id,occurrence_time])=>({record_id,title:'区间事件-'+record_id,occurrence_time,building:'A楼',building_codes:['A'],level:'I3',status:'处理中'}));
      inFlight--;return ok({records,snapshot_exists:true,last_refreshed_at:1792029600});
    }
    if(url.pathname==='/api/repair-management/records')return ok({records:fixture.empty?[]:Array.from({length:30},(_,i)=>({record_id:'repair-'+i,title:'A楼配电设备检修-'+i,workflow:'维修中',building:'A楼',building_codes:['A'],progress_percent:30,followup_count:0})),fields:[],total:fixture.empty?0:30,offset:0});
    if(url.pathname==='/api/repair-management/status')return ok({stats:{total:fixture.empty?0:30,completed_total:0}});
    if(url.pathname==='/api/repair-management/stream')return route.fulfill({status:204,body:''});
    return ok({records:[],items:[],fields:[],stats:{},total:0,changes:[],cursor:0});
  });
  const page=await ctx.newPage();page.on('pageerror',error=>errors.push(error.message));
  const custom='?__page=/&mode=events&scope=A&period=custom&event_start=2026-01-15&event_end=2026-03-15';
  await page.goto(entry+custom);
  await page.getByRole('tab',{name:'自定义',selected:true}).waitFor();
  assert.equal(await page.getByRole('button',{name:/^刷新本页$|^刷新事件$/}).count(),0);
  const scopeBox=await page.locator('.v2-event-controls .v2-local-scope').boundingBox();
  const periodBox=await page.locator('.v2-event-controls .n-tabs').boundingBox();
  await page.screenshot({path:path.join(output,'event-controls.png')});
  assert(scopeBox&&periodBox&&Math.abs(scopeBox.y-periodBox.y)<4,'building and period controls share a toolbar: '+JSON.stringify({scopeBox,periodBox}));
  await page.locator('[data-metric=total] .n-statistic-value__content').filter({hasText:'3'}).waitFor();
  assert.deepEqual([...new Set(requests.map(r=>r.month))].sort(),['2026-01','2026-02','2026-03']);
  assert(requests.every(r=>r.scope==='A'));
  await page.getByRole('button',{name:'展开事件明细',exact:true}).click();
  await page.locator('.event-list-panel .event-row').filter({hasText:'区间事件-middle'}).waitFor();
  assert.equal(await page.locator('.event-list-panel .event-row').count(),3);
  assert.equal(await page.locator('.event-list-panel').getByText('区间事件-excluded',{exact:true}).count(),0);
  for(const width of [1440,360,390]){
    await page.setViewportSize({width,height:900});
    await page.getByRole('tab',{name:'自定义'}).scrollIntoViewIfNeeded();
    await page.locator('.v2-event-custom-dates .n-date-picker').first().click();
    await page.locator('.n-date-panel').waitFor();
    const panel=await page.locator('.n-date-panel').boundingBox();assert(panel.x>=0&&panel.x+panel.width<=width+1,'calendar fits viewport');
    await page.keyboard.press('Escape');
    assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false);
    await page.screenshot({path:path.join(output,`event-range-${width}.png`)});
  }
  await page.reload();await page.getByRole('tab',{name:'自定义',selected:true}).waitFor();
  assert.equal(await page.locator('.v2-event-custom-dates input').first().inputValue(),'2026-01-15');
  await page.locator('.v2-event-custom-dates .n-date-picker').first().click();
  await page.locator('.n-date-panel-date:not(.n-date-panel-date--excluded)').filter({hasText:/^16$/}).click();
  await page.locator('[data-metric=total] .n-statistic-value__content').filter({hasText:'2'}).waitFor();
  assert.equal(new URL(page.url()).searchParams.get('event_start'),'2026-01-16');
  await page.reload();await page.getByRole('tab',{name:'自定义',selected:true}).waitFor();
  assert.equal(await page.locator('.v2-event-custom-dates input').first().inputValue(),'2026-01-16');
  await page.goto(entry+'?__page=/&mode=events&scope=A&period=custom&event_start=2026-01-01&event_end=2026-12-31');
  await page.locator('[data-metric=total] .n-statistic-value__content').filter({hasText:'4'}).waitFor();
  assert(maxInFlight<=3,'cross-month requests are bounded');
  const before=requests.length;
  await page.goto(entry+'?__page=/&mode=events&scope=A&period=custom&event_start=2026-03-15&event_end=2026-01-15');
  await page.getByText('开始日期不能晚于结束日期。',{exact:true}).waitFor();
  assert.equal(requests.length,before);
  for(const empty of [true,false])for(const width of [1440,390]){
    fixture.empty=empty;await page.setViewportSize({width,height:700});
    await page.goto(entry+'?__page=/repair-management&scope=A');
    await page.locator(empty?'.record-list .n-empty':'.record-row').first().waitFor();
    assert.equal(await page.locator('.record-row').count(),empty?0:30);
    assert.deepEqual(await page.locator('.record-list').evaluate(el=>({overflow:getComputedStyle(el).overflowY,overscroll:getComputedStyle(el).overscrollBehaviorY})),{overflow:'visible',overscroll:'auto'});
    // Add read-only space below an empty fixture to verify wheel propagation.
    if(empty)await page.locator('.v2-main').evaluate(el=>{const space=document.createElement('div');space.style.height='1200px';el.append(space);});
    const wheelTarget=page.locator(empty?'.record-list .n-empty':'.record-row').first();
    await wheelTarget.scrollIntoViewIfNeeded();
    const box=await wheelTarget.boundingBox();
    await page.mouse.move(box.x+box.width/2,Math.min(box.y+60,550));
    // Custom layouts scroll the panel body; empty panels hand the wheel to the page.
    const scroll=()=>page.evaluate(()=>[document.documentElement,...document.querySelectorAll('.v2-main,.v2-responsive-body')].reduce((sum,el)=>sum+el.scrollTop,0));
    const top=await scroll();await page.mouse.wheel(0,260);
    try {
      await page.waitForFunction(top=>[document.documentElement,...document.querySelectorAll('.v2-main,.v2-responsive-body')].reduce((sum,el)=>sum+el.scrollTop,0)>top+40,top);
    } catch(error) {
      await page.screenshot({path:path.join(output,`scroll-failed-${empty?'empty':'records'}-${width}.png`)});
      throw new Error(`Wheel failed: ${empty?'empty':'records'}, ${width}px, before=${top}, after=${await scroll()}`,{cause:error});
    }
    assert.equal(await page.locator('.record-list').evaluate(el=>el.scrollTop),0,'no nested list scroll');
    assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false);
    await page.screenshot({path:path.join(output,`repair-${empty?'empty':'records'}-${width}.png`)});
  }
  assert.deepEqual(writes,[]);assert.deepEqual(errors,[]);
  console.log('PASS: custom range, month completeness, bounded reads, reload, calendars and empty/populated page scrolling on PC/mobile.');
} finally {await browser.close();await server.close();}
