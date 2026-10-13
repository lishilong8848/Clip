import assert from 'node:assert/strict';
import { readFile, mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { build } from 'esbuild';
import { parse, compileScript } from '@vue/compiler-sfc';
import { preview } from 'vite';
import { chromium } from 'playwright';
import { adaptEventPeriodPage } from './workspace-event-adapter.mjs';
import { adaptCabinetBatchPage } from './workspace-cabinet-adapter.mjs';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const output = path.resolve(root, '../../../output/playwright/workspace-v2');
await mkdir(output, {recursive:true});
const bundled = await build({entryPoints:[path.join(root,'src/workspace-v2/eventPeriod.ts')],bundle:true,write:false,format:'esm',platform:'node'});
const {eventPeriodRange,occurrenceTimestamp,mergeEventMonths,eventTrend} = await import('data:text/javascript;base64,'+Buffer.from(bundled.outputFiles[0].text+'\n//# sourceURL=event-period-test.mjs').toString('base64'));
const now=Date.parse('2026-10-01T02:00:00Z');
const week=eventPeriodRange('week','',now), month=eventPeriodRange('month','',now);
assert.deepEqual(week.months,['2026-09','2026-10']);
assert.equal(week.caption,'2026.09.28 - 2026.10.04');
assert.equal(eventPeriodRange('week','',Date.parse('2027-01-01T00:00:00Z')).caption,'2026.12.28 - 2027.01.03');
assert.equal(eventPeriodRange('month','',Date.parse('2026-09-30T17:00:00Z')).label,'本月');
assert.equal(eventPeriodRange('month','',Date.parse('2026-09-30T17:00:00Z')).months[0],'2026-10');
assert.equal(eventPeriodRange('history','2024-02',now).caption,'2024.02.01 - 2024.02.29');
assert.equal(eventPeriodRange('history','2026-99',now).months[0],'2026-10');
assert(Number.isNaN(occurrenceTimestamp('2026-02-30 01:00')));
assert.equal(occurrenceTimestamp('2026/10/1 09:00'),Date.parse('2026-10-01T01:00:00Z'));
const records=[
  {record_id:'sep-before',title:'窗口外旧事件',occurrence_time:'2026-09-27 23:59:59',building:'A楼',building_codes:['A'],status:'已结束',level:'I3'},
  {record_id:'sep',title:'跨月变压器告警',occurrence_time:'2026-09-28 00:00:00',building:'A楼',building_codes:['A'],status:'处理中',level:'I2',transfer_to_overhaul:'False'},
  {record_id:'oct',title:'本周空调告警',occurrence_time:'2026-10-01 09:00:00',building:'A楼',building_codes:['A'],status:'处理中',level:'I3',under_repair:true},
  {record_id:'sun',title:'本周B楼告警',occurrence_time:'2026-10-04 23:59:59',building:'B楼',building_codes:['B'],status:'已结束',level:'I3'},
  {record_id:'next',title:'下周设备告警',occurrence_time:'2026-10-05 00:00:00',building:'A楼',building_codes:['A'],status:'处理中',level:'I2'},
];
const merged=mergeEventMonths([{records:[...records,{...records[1]}],unknown_time_count:1}],week);
assert.deepEqual(merged.records.map(r=>r.record_id),['sun','oct','sep']);
assert.deepEqual(merged.stats,{total:3,under_repair:1,i2_or_higher:1,i3:2});
assert.equal(mergeEventMonths([{records}],month).records.length,3);
assert.equal(eventTrend(records,week).length,7);assert.equal(eventTrend(records,week).reduce((sum,b)=>sum+b.value,0),3);
assert.equal(eventTrend(records,month).length,31);assert.equal(eventTrend(records,month).reduce((sum,b)=>sum+b.value,0),3);
assert.throws(()=>mergeEventMonths([{records:[],snapshot_exists:false}],week),/快照/);
assert.throws(()=>mergeEventMonths([{}],week),/完整/);
for(const [file,adapt] of [['EventManagementPage.vue',adaptEventPeriodPage],['CabinetPowerBatchPage.vue',adaptCabinetBatchPage]]){
  const source=await readFile(path.join(root,'src/components',file),'utf8');
  const result=adapt(source);compileScript(parse(result).descriptor,{id:'isolated-'+file});
  assert.notEqual(result,source);
  if(file.startsWith('Event'))assert.match(result,/for \(const month of eventRange.value.months\)/);
  else {assert.match(result,/createMode = ref\('text'\)/);assert.match(source,/createMode = ref\('pdf'\)/);}
}
const server=await preview({root,logLevel:'error',preview:{host:'127.0.0.1',port:0}});
const origin=`http://127.0.0.1:${server.httpServer.address().port}`;
const entry=origin+'/assets/workspace-v2/index.html';
const browser=await chromium.launch({headless:true});
const requests=[],writes=[],errors=[];
let delayedMonth='',failedMonth='',partial=false,created=false;
let page;
try{
  const context=await browser.newContext({viewport:{width:1440,height:1000},timezoneId:'America/Los_Angeles'});
  await context.route('**/*',async route=>{
    const req=route.request(),url=new URL(req.url());
    if(url.origin!==origin)return route.abort();
    if(!url.pathname.startsWith('/api/'))return route.continue();
    requests.push({path:url.pathname,month:url.searchParams.get('month'),scope:url.searchParams.get('scope')});
    const ok=data=>route.fulfill({json:{ok:true,data}});
    if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'v2-period-isolated'}});
    if(url.pathname==='/api/auth/status')return ok({logged_in:true,user:{open_id:'isolated',name:'隔离验收',role:'admin',level:'L1',category:'manager',modules:['event','cabinet']},scope_options:[{value:'ALL',label:'全部楼栋'},{value:'A',label:'A楼'},{value:'B',label:'B楼'}]});
    if(url.pathname==='/api/events/monthly'){
      const m=url.searchParams.get('month'),s=url.searchParams.get('scope');
      const fail=m===failedMonth;
      if(m===delayedMonth)await new Promise(r=>setTimeout(r,800));
      if(fail)return route.fulfill({status:503,json:{ok:false,error:'隔离测试读取失败'}});
      return ok({records:records.filter(r=>r.occurrence_time.startsWith(m)&&(s==='ALL'||r.building_codes.includes(s))),snapshot_exists:!(partial&&m==='2026-09'),last_refreshed_at:1790816400,unknown_time_count:1});
    }
    if(url.pathname==='/api/events/refresh')return ok({mock_external:true});
    if(url.pathname==='/api/events/transfer-repair'){writes.push(req.postDataJSON());return ok({});}
    if(url.pathname==='/api/cabinet-power/batches/text-preview'){
      const body=req.postDataJSON();writes.push(body);
      return ok({rows:body.sources.map(s=>({row_id:'row-'+s.id,text_id:s.id,text_row:1,scope:'A',room:'201',rack:'A01',rack_type:'服务器机柜',action:'上正式电',expected:'2026-10-01 10:00:00',actual:'2026-10-01 10:00:00',result:'成功',issues:[]}))});
    }
    if(url.pathname==='/api/cabinet-power/batches'&&req.method()==='POST'){writes.push(req.postDataJSON());created=true;return ok({batch_id:'isolated-batch'});}
    if(url.pathname.includes('/api/cabinet-power/batches/isolated-batch'))return ok({batch:{batch_id:'isolated-batch',rows:[],scopes:['A'],status:'pending',stats:{total:1},images:[]},batch_id:'isolated-batch',rows:[],scopes:['A'],stats:{total:1},images:[]});
    return ok({records:[],items:[],scopes:{},stats:{},rows:[],batches:[],total:0,buildings:[],settings:{},data:{}});
  });
  page=await context.newPage();
  page.on('pageerror',e=>errors.push(e.message));
  await page.clock.install({time:new Date(now)});
  await page.goto(entry+'?__page=/&mode=events&scope=ALL');
  await page.locator('[data-metric=total] .n-statistic-value__content').filter({hasText:'3'}).waitFor();
  assert.equal(await page.locator('.event-list-panel').count(),0,'details stay closed until requested');
  await page.getByRole('button',{name:'展开事件明细',exact:true}).click();
  await page.getByRole('tab',{name:'本周事件'}).click();
  await page.getByText('2026.09.28 - 2026.10.04',{exact:true}).first().waitFor();
  await page.locator('.event-list-panel .event-row').filter({hasText:'跨月变压器告警'}).waitFor();
  assert.equal(await page.locator('.event-list-panel .event-row').count(),3);
  assert.equal(await page.locator('.v2-event-charts [role=img]').count(),1,'one chart switches dimensions without duplicate charts');
  await page.locator('.v2-event-charts .v2-chart-canvas svg').first().waitFor();
  await page.locator('.v2-event-charts summary').focus();await page.locator('.v2-event-charts summary').press('Enter');
  await page.getByRole('button',{name:/09\/28\s*1 件/}).focus();
  await page.getByRole('button',{name:/09\/28\s*1 件/}).click();
  assert.equal(await page.locator('.event-list-panel .event-row').count(),1,'date chart drills into matching records');
  await page.getByRole('button',{name:'清空筛选',exact:true}).click();
  await page.locator('.v2-event-charts').scrollIntoViewIfNeeded();
  await page.getByText('按等级',{exact:true}).click();
  if(!await page.locator('.v2-event-charts details').evaluate(el=>el.open))await page.locator('.v2-event-charts summary').click();
  await page.getByRole('button',{name:/I3\s*2 件/}).focus();
  await page.getByRole('button',{name:/I3\s*2 件/}).click();
  assert.equal(await page.locator('.event-list-panel .event-row').count(),2,'grade chart drills into matching records');
  await page.getByRole('button',{name:'清空筛选',exact:true}).click();
  await page.locator('.v2-event-charts').scrollIntoViewIfNeeded();
  await page.getByText('按天',{exact:true}).click();
  assert.equal(await page.locator('.event-list-panel').getByText('下周设备告警',{exact:true}).count(),0);
  assert.equal(requests.some(r=>r.path==='/api/events/overview'),false,'no redundant unscoped monthly overview');
  await page.getByRole('button',{name:'查看本周I2级及以上记录',exact:true}).click();
  await page.locator('.event-metric-dialog .event-row').filter({hasText:'跨月变压器告警'}).waitFor();
  assert.equal(await page.locator('.event-metric-dialog .event-row').count(),1);
  await page.locator('.event-metric-dialog').getByRole('button',{name:'关闭',exact:true}).click();
  await page.locator('.event-list-panel .event-row').filter({hasText:'跨月变压器告警'}).click();
  await page.locator('.event-drawer').getByRole('button',{name:'标记转检修',exact:true}).click();
  await page.locator('.event-repair-actions strong').filter({hasText:'已转检修'}).waitFor();
  assert.equal(writes.at(-1).month,'2026-09','cross-month action retains occurrence month');
  assert.equal(writes.at(-1).record_id,'sep');
  await page.locator('.event-drawer').getByRole('button',{name:'关闭',exact:true}).click();
  await page.getByRole('button',{name:'刷新事件',exact:true}).click();
  await page.locator('.event-command-actions .source-refresh:not(:disabled)').waitFor();
  assert.deepEqual(requests.filter(r=>r.path==='/api/events/refresh').map(r=>r.month),['2026-09','2026-10']);
  await page.getByRole('searchbox',{name:'搜索事件',exact:true}).fill('空调');
  assert.equal(await page.locator('.event-list-panel .event-row').count(),1);
  await page.getByRole('tab',{name:'本月事件'}).click();
  await page.locator('.event-list-panel .event-row').filter({hasText:'下周设备告警'}).waitFor();
  assert.equal(await page.getByRole('searchbox',{name:'搜索事件',exact:true}).inputValue(),'');
  // Week/month responses may arrive out of order; the selected period must win.
  delayedMonth='2026-09';
  await page.goto(entry+'?__page=/&mode=events&scope=ALL&period=week');
  await page.getByRole('tab',{name:'本周事件',selected:true}).waitFor();
  await page.getByRole('tab',{name:'本月事件'}).click();
  await page.waitForTimeout(1100);
  await page.getByRole('button',{name:'展开事件明细',exact:true}).click();
  assert.equal(await page.locator('.event-list-panel').getByText('跨月变压器告警',{exact:true}).count(),0);
  await page.locator('.v2-event-detail-toggle button:not([data-v2-action])').waitFor();
  await page.getByRole('button',{name:'收起事件明细',exact:true}).click();
  delayedMonth='';
  for(const width of [1280,1366,1440,1920,360,390,430]){
    await page.setViewportSize({width,height:width<700?900:1000});
    await page.getByRole('tab',{name:'本周事件'}).click();
    await page.getByText('2026.09.28 - 2026.10.04',{exact:true}).first().waitFor();
    await page.locator('[data-metric=total] .n-statistic-value__content').filter({hasText:'3'}).waitFor();
    assert.equal(await page.locator('.event-list-panel').count(),0,'summary does not repeat records');
    await page.screenshot({path:path.join(output,`event-period-${width}.png`),fullPage:true});
    assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false,`event overflow ${width}`);
  }
  await page.reload();await page.getByRole('tab',{name:'本周事件',selected:true}).waitFor();
  await page.getByRole('tab',{name:'本周事件'}).focus();await page.keyboard.press('ArrowRight');
  await page.getByRole('tab',{name:'本月事件',selected:true}).waitFor();
  await page.getByRole('tab',{name:'历史月份'}).click();
  await page.locator('.v2-event-period .n-date-picker').click();
  await page.locator('.n-date-panel').waitFor();await page.keyboard.press('Escape');
  await page.goto(entry+'?__page=/&mode=events&scope=A&period=week');
  await page.getByRole('button',{name:'展开事件明细',exact:true}).click();
  await page.locator('.event-list-panel .event-row').filter({hasText:'跨月变压器告警'}).waitFor();
  assert.equal(await page.locator('.event-list-panel .event-row').count(),2,'building filter stays authorized');
  failedMonth='2026-09';
  await page.reload();await page.getByText('隔离测试读取失败',{exact:true}).waitFor();
  assert.equal(await page.locator('[data-metric=total] strong').innerText(),'—');
  assert.equal(await page.locator('.event-list-panel .event-row').count(),0,'partial data is not shown as complete');
  failedMonth='';partial=true;
  await page.reload();await page.getByText('所选时间范围的事件快照尚未准备好，请点击“刷新事件”后重试。',{exact:true}).waitFor();partial=false;

  // Cabinet entry defaults to paste. Parsing/editing/create keep their original API contract.
  await page.goto(entry+'?__page=/cabinet-power/batches&scope=A&mode=new');
  await page.getByRole('textbox',{name:'粘贴机柜确认文本',exact:true}).waitFor();
  assert.equal(await page.getByRole('heading',{name:'上传确认单',exact:true}).count(),0);
  await page.setViewportSize({width:1440,height:1000});await page.screenshot({path:path.join(output,'cabinet-paste-empty.png'),fullPage:true});
  const paste=()=>page.getByRole('textbox',{name:'粘贴机柜确认文本',exact:true}).evaluate(el=>{const clipboardData=new DataTransfer();clipboardData.setData('text/plain','A楼\t201\tA01\t上正式电');el.dispatchEvent(new ClipboardEvent('paste',{clipboardData,bubbles:true,cancelable:true}));});
  await paste();await page.getByRole('textbox',{name:'文本记录包间',exact:true}).waitFor();
  await page.getByRole('heading',{name:'识别结果分布',exact:true}).waitFor();
  assert.equal(writes.filter(w=>w.sources?.[0]?.text==='A楼\t201\tA01\t上正式电').length,1,'one paste triggers one recognition');
  await page.getByRole('textbox',{name:'文本记录包间',exact:true}).fill('202');
  await page.getByRole('textbox',{name:'搜索粘贴记录',exact:true}).focus();
  await page.getByRole('button',{name:'其他录入方式',exact:true}).click();
  await page.locator('.n-dropdown-menu').getByText('PDF识别',{exact:true}).click();
  await page.getByRole('heading',{name:'上传确认单',exact:true}).waitFor();
  await page.getByRole('button',{name:'粘贴文本识别',exact:true}).click();
  assert.equal(await page.getByRole('textbox',{name:'文本记录包间',exact:true}).inputValue(),'202','switching modes preserves preview');
  await page.reload();await page.getByRole('textbox',{name:'文本记录包间',exact:true}).waitFor();
  assert.equal(await page.getByRole('textbox',{name:'文本记录包间',exact:true}).inputValue(),'202','draft survives reload');
  for(const width of [1440,390]){
    await page.setViewportSize({width,height:width<700?900:1000});
    await page.screenshot({path:path.join(output,`cabinet-paste-${width}.png`),fullPage:true});
    assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false,`cabinet overflow ${width}`);
  }
  await page.getByRole('button',{name:/确认并创建待办/}).click();
  await page.getByRole('heading',{name:'上下电待办详情',exact:true}).waitFor();
  assert(created);assert.equal(writes.at(-1).source,'text');assert.equal(writes.at(-1).rows[0].room,'202');
  await context.close();assert.deepEqual(errors,[]);
  console.log('PASS: Beijing week/month/year boundaries, period counts+list+drawer, aggregate/building scope, stale response guard, missing snapshot/error states, 6 viewports, original cross-month actions; cabinet paste default, single recognition, edits, mode switch, draft restore and original create API. Isolated data only.');
}catch(error){
  if(page&&!page.isClosed()){
    await page.screenshot({path:path.join(output,'events-cabinet-failure.png'),fullPage:true}).catch(()=>{});
    console.error('Overflow:',await page.evaluate(()=>[...document.querySelectorAll('body *')].map(el=>({tag:el.tagName,cls:el.className,x:el.getBoundingClientRect().x,right:el.getBoundingClientRect().right})).filter(el=>el.right>innerWidth+1&&el.x>=0).slice(0,20)));
  }
  console.error('Browser errors:',errors);
  throw error;
}finally{await browser.close();await new Promise(resolve=>server.httpServer.close(resolve));}
