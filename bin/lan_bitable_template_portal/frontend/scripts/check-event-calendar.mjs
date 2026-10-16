import assert from 'node:assert/strict';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {mkdir} from 'node:fs/promises';
import {createRequire} from 'node:module';
import {build} from 'esbuild';
import {createServer} from 'vite';
import vue from '@vitejs/plugin-vue';
import {chromium} from 'playwright';
import {portalFloatingControls} from './workspace-controls-adapter.mjs';

const root=fileURLToPath(new URL('..',import.meta.url));
const output=path.resolve(root,'../../../output/playwright/event-calendar');
async function load(file){const result=await build({entryPoints:[path.join(root,'src/workspace-v2',file)],bundle:true,write:false,platform:'node',format:'cjs'});const module={exports:{}};new Function('module','exports','require',result.outputFiles[0].text)(module,module.exports,createRequire(import.meta.url));return module.exports;}
const {eventCalendarRange,eventTrend,mergeEventMonths,occurrenceTimestamp}=await load('eventPeriod.ts');
const {chartOption}=await load('vipboard.ts');
const {widget,sanitize,displayOptions,chartTemplates}=await load('dashboard/layout.ts');
const now=Date.parse('2026-10-15T04:00:00Z'),range=eventCalendarRange('year',now);
assert.equal(range.months.length,10);assert.equal(range.end,occurrenceTimestamp('2026-10-16'));
const days=eventTrend([],range,true);assert.equal(days.length,288);assert(days.every(item=>item.end-item.start===86400000));
assert.equal(eventTrend([],eventCalendarRange('year',Date.parse('2024-02-29T04:00:00Z')),true).length,60,'leap day is retained');
assert.equal(eventCalendarRange('week',Date.parse('2026-01-01T04:00:00Z')).months.join(','),'2025-12,2026-01','week can cross a year boundary');
assert.throws(()=>mergeEventMonths([{snapshot_exists:false}],range),/尚未准备/);
const option=chartOption(days,'calendar','条',false,false);assert.equal(option.series[0].data.length,288);assert.equal(option.calendar.range[1],'2026-10-15');assert.equal(option.visualMap.max,1);assert.equal(option.tooltip.renderMode,'richText');assert.equal(option.animation,false);
assert.throws(()=>chartOption(eventTrend([],range),'calendar'),/逐日/,'aggregated bins cannot masquerade as days');
const user={open_id:'calendar-fixture',modules:['event']},item=widget('events',user,{display:'calendar'});
assert.equal(item.period,'year');assert.equal(sanitize({version:1,widgets:[item]},user,['A']).widgets[0].display,'calendar');
assert.equal(sanitize({version:1,widgets:[item]},{...user,modules:[]},['A']).widgets.length,0);assert(!displayOptions('notices').some(o=>o.value==='calendar'));assert(chartTemplates.some(t=>t.id==='event-calendar'));

const fixture=`<template><NConfigProvider><div style="width:100%;max-width:900px;padding:12px;box-sizing:border-box"><NButton @click="editor=item">组件设置</NButton><MetricWidget v-if="mounted" :item="item" :user="user" :scope="scope" @open="opened=$event"/><WidgetEditor :value="editor" :user="user" :scopes="[{value:'A',label:'A楼'},{value:'B',label:'B楼'}]" @close="editor=null" @apply="item=$event;editor=null" /></div></NConfigProvider></template>
<script setup>import {ref} from 'vue';import {NConfigProvider,NButton} from 'naive-ui';import {getInstanceByDom} from 'echarts/core';import MetricWidget from '/src/workspace-v2/dashboard/MetricWidget.vue';import WidgetEditor from '/src/workspace-v2/dashboard/WidgetEditor.vue';import '/src/workspace-v2/workspace.css';import '/src/workspace-v2/dashboard/dashboard.css';import '/src/workspace-v2/feedback.css';document.body.classList.add('workspace-v2');
const user={open_id:'calendar-fixture',modules:['event']},scope=ref('A'),mounted=ref(true),opened=ref(''),editor=ref(null),item=ref(${JSON.stringify(item)});
const engine=()=>{const node=document.querySelector('.v2-chart-canvas');return node?getInstanceByDom(node):undefined};
window.fixture={item,mounted,scope,option:()=>engine()?.getOption(),point:date=>engine().convertToPixel({calendarIndex:0},date),opened:()=>opened.value,engine};
</script>`;
const server=await createServer({root,configFile:false,cacheDir:path.join(root,'node_modules/.vite-calendar-check'),logLevel:'error',optimizeDeps:{include:['vue','naive-ui','lucide-vue-next','echarts/core','echarts/charts','echarts/components','echarts/renderers','echarts/features']},plugins:[{name:'calendar-fixture',enforce:'pre',resolveId(id){if(['/calendar.vue','/calendar.js'].includes(id))return id;},load(id){if(id==='/calendar.vue')return fixture;if(id==='/calendar.js')return 'import {createApp} from "vue";import App from "/calendar.vue";createApp(App).mount("#app")';},transform(code,id){if(id.endsWith('.vue'))return portalFloatingControls(code);}},vue()],server:{host:'127.0.0.1',port:0}});
await server.listen();const origin='http://127.0.0.1:'+server.httpServer.address().port;
const browser=await chromium.launch({headless:true});let page,missing=false,zero=false;const errors=[],requests=[];
try{
  await mkdir(output,{recursive:true});page=await browser.newPage({viewport:{width:1440,height:900}});await page.clock.setFixedTime(new Date(now));
  page.on('pageerror',e=>errors.push(e.message));
  await page.route('**/*',route=>{
    const u=new URL(route.request().url());if(u.origin!==origin)return route.abort();
    if(u.pathname==='/fixture')return route.fulfill({contentType:'text/html',body:'<!doctype html><meta name="viewport" content="width=device-width,initial-scale=1"><div id="app"></div><script type="module" src="/calendar.js"></script>'});
    if(!u.pathname.startsWith('/api/'))return route.continue();assert.equal(route.request().method(),'GET');
    const month=u.searchParams.get('month'),scope=u.searchParams.get('scope');requests.push({month,scope});assert.equal(u.pathname,'/api/events/monthly');assert(['A','B'].includes(scope));assert(month<='2026-10','never requests future months');
    const records=zero?[]:[{record_id:month+'-'+scope,title:scope+'楼实际事件',scope,occurrence_time:month+'-15 09:00'},...(month==='2026-10'?[{record_id:'second-'+scope,title:scope+'楼第二条事件',scope,occurrence_time:'2026-10-15 10:00'},{record_id:'earlier-'+scope,title:scope+'楼早前事件',scope,occurrence_time:'2026-10-13 09:00'},{record_id:'future-'+scope,title:'未来记录不计入',scope,occurrence_time:'2026-10-16 09:00'}]:[])];
    return route.fulfill({json:{ok:true,data:{snapshot_exists:!missing,records}}});
  });
  await page.goto(origin+'/fixture');await page.waitForFunction(()=>window.fixture.option()?.series[0]?.data?.length===288);await page.waitForTimeout(600);
  assert.equal(requests.length,10);assert.equal(await page.evaluate(()=>window.fixture.option().series[0].data.reduce((sum,d)=>sum+d.value[1],0)),12);
  assert.equal(await page.evaluate(()=>window.fixture.option().visualMap[0].max),2,'color scale reflects actual counts');
  assert(await page.evaluate(()=>{const data=window.fixture.engine().getModel().getSeriesByIndex(0).getData();return Array.from({length:data.count()},(_,i)=>data.getItemGraphicEl(i).shape.r).every(r=>r===3);}), 'every rendered day has rounded corners');
  const engineId=await page.locator('.v2-chart-canvas').getAttribute('_echarts_instance_');
  const point=await page.evaluate(()=>window.fixture.point('2026-10-15'));const box=await page.locator('.v2-chart-canvas').boundingBox();await page.mouse.click(box.x+point[0],box.y+point[1]);
  await page.getByText('10/15 · 事件明细',{exact:true}).waitFor();assert.equal(await page.locator('.v2-chart-records .n-list-item').count(),2);await page.keyboard.press('Escape');
  await page.locator('.n-radio-button').filter({hasText:'本月'}).click();await page.waitForFunction(()=>window.fixture.option()?.series?.[0]?.data.length===15);assert.equal(await page.locator('.v2-chart-canvas').getAttribute('_echarts_instance_'),engineId);await page.waitForTimeout(600);assert(await page.locator('.v2-chart-canvas svg text').filter({hasText:/^15$/}).count()>0,'month cells show their dates');await page.screenshot({path:path.join(output,'calendar-month.png')});
  await page.locator('.n-radio-button').filter({hasText:'本周'}).click();await page.waitForFunction(()=>window.fixture.option()?.series?.[0]?.data.length===4);
  await page.getByRole('button',{name:'组件设置',exact:true}).click();await page.locator('.n-form-item').filter({hasText:'展示方式'}).locator('.n-select').click();await page.locator('.n-base-select-option').getByText('柱状图',{exact:true}).click();await page.getByRole('button',{name:'应用设置',exact:true}).click();await page.waitForFunction(()=>window.fixture.option()?.series?.[0]?.type==='bar');assert.equal(await page.evaluate(()=>window.fixture.option().calendar?.length||0),0,'switching styles clears the old calendar');
  await page.evaluate(()=>window.fixture.item.value={...window.fixture.item.value,display:'calendar',period:'year'});await page.waitForFunction(()=>window.fixture.option()?.series?.[0]?.data.length===288);
  for(const width of [1440,390]){await page.setViewportSize({width,height:900});await page.waitForTimeout(350);assert(!await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1));await page.screenshot({path:path.join(output,'calendar-'+width+'.png')});}
  await page.getByText('数据明细',{exact:false}).click();await page.getByRole('button',{name:'10/15 2 条',exact:true}).click();await page.getByText('10/15 · 事件明细',{exact:true}).waitFor();await page.keyboard.press('Escape');
  zero=true;await page.getByRole('button',{name:'刷新图表',exact:true}).click();await page.waitForFunction(()=>window.fixture.option()?.series?.[0]?.data.every(d=>d.value[1]===0));assert.equal(await page.getByText('暂无数据',{exact:true}).count(),0,'known zero days remain valid');
  missing=true;await page.getByRole('button',{name:'刷新图表',exact:true}).click();await page.getByText('数据暂不可用',{exact:true}).waitFor();assert(await page.getByRole('button',{name:/查看明细/}).isDisabled());
  missing=false;zero=false;await page.evaluate(()=>window.fixture.scope.value='B');await page.waitForFunction(()=>window.fixture.option()?.series?.[0]?.data.some(d=>d.value[1]===2));
  await page.emulateMedia({reducedMotion:'reduce'});await page.waitForFunction(()=>window.fixture.option()?.animation===false);
  await page.evaluate(()=>{window.previousEngine=window.fixture.engine();window.fixture.mounted.value=false;});await page.locator('.v2-chart-canvas').waitFor({state:'detached'});assert(await page.evaluate(()=>window.previousEngine.isDisposed()));
  assert.deepEqual(errors,[]);console.log('PASS: calendar date ranges/leap year/daily counts, template permissions/persistence, real-source totals, date drilldown, chart reuse, PC/mobile, zeros/missing snapshots, building isolation and cleanup. No business writes.');
}catch(error){if(page){await page.screenshot({path:path.join(output,'failure.png'),fullPage:true});console.error(errors,await page.locator('body').innerText());}throw error;}finally{await browser.close();await server.close();}
