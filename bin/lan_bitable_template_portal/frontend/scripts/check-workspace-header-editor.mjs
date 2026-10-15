import assert from 'node:assert/strict';
import path from 'node:path';
import {mkdir} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
import {createServer} from 'vite';
import vue from '@vitejs/plugin-vue';
import {chromium} from 'playwright';
import {portalFloatingControls} from './workspace-controls-adapter.mjs';

const root=fileURLToPath(new URL('..',import.meta.url));
const output=path.resolve(root,'../../../output/playwright/header-editor');
const fixture=`<template><NConfigProvider>
  <div class="v2-shell"><header class="v2-header"><a class="v2-brand"><span>灯塔<small>南通运维工作空间</small></span></a></header></div>
  <div class="lighthouse-launcher" style="position:fixed;right:24px;bottom:24px;z-index:10200;pointer-events:none"><button style="border:0;padding:0;background:none;pointer-events:none" @click="clicks++"><CompactBot :appearance="appearance" interactive :position="position" @moved="position=$event" /></button></div>
  <WidgetEditor :value="value" :user="{modules:['maintenance']}" :scopes="[]" @apply="applied=$event;value=null" @close="value=null" />
  <NModal v-model:show="other" preset="card" title="浮层回归" style="width:360px;overflow:auto" @close="other=false">
    <NFormItem label="日期"><NDatePicker type="datetime" /></NFormItem>
    <NFormItem label="时间"><NTimePicker /></NFormItem>
    <NPopover trigger="click"><template #trigger><NButton>展开选项</NButton></template><span>完整选项</span></NPopover>
  </NModal>
</NConfigProvider></template><script setup>
import {ref} from 'vue';import {NConfigProvider,NModal,NFormItem,NDatePicker,NTimePicker,NPopover,NButton} from 'naive-ui';
import WidgetEditor from '/src/workspace-v2/dashboard/WidgetEditor.vue';import CompactBot from '/src/workspace-v2/CompactBot.vue';
import '/src/global.css';import '/src/workspace-v2/workspace.css';import '/src/workspace-v2/dashboard/dashboard.css';import '/src/workspace-v2/feedback.css';import '/src/workspace-v2/header.css';
document.body.classList.add('workspace-v2');
const value=ref(null),other=ref(false),applied=ref(null),clicks=ref(0),position=ref({x:700,y:250});
const appearance=ref({size:160,color:'encre',shape:'weather-lighthouse-mini',snap_back:false});
window.fixture={open(){value.value={id:'test',kind:'module',title:'通告办理',w:6,h:14,x:0,module:'maintenance',scope:'current'}},openOther(){other.value=true},closeOther(){other.value=false},applied:()=>applied.value,position:()=>position.value,clicks:()=>clicks.value};
</script>`;
const server=await createServer({root,configFile:false,cacheDir:path.join(root,'node_modules/.vite-header-check'),logLevel:'error',
  plugins:[{name:'header-editor-fixture',enforce:'pre',resolveId(id){if(['/fixture.vue','/fixture.js'].includes(id))return id;},load(id){if(id==='/fixture.vue')return fixture;if(id==='/fixture.js')return 'import {createApp} from "vue";import App from "/fixture.vue";createApp(App).mount("#app");';},transform(code,id){if(id.endsWith('.vue'))return portalFloatingControls(code);}},vue()],server:{host:'127.0.0.1',port:0}});
await server.listen();const origin='http://127.0.0.1:'+server.httpServer.address().port;
const browser=await chromium.launch({headless:true}),errors=[];
try{
  await mkdir(output,{recursive:true});
  const page=await browser.newPage({viewport:{width:1440,height:800}});
  page.on('pageerror',error=>errors.push(error.message));
  await page.route('**/*',route=>{
    const url=new URL(route.request().url());if(url.origin!==origin)return route.abort();
    if(url.pathname.startsWith('/api/'))return route.fulfill({json:{ok:true,data:{available:false,city:'南通'}}});
    if(url.pathname==='/fixture')return route.fulfill({contentType:'text/html',body:'<!doctype html><meta name="viewport" content="width=device-width,initial-scale=1"><div id="app"></div><script type="module" src="/fixture.js"></script>'});
    return route.continue();
  });
  await page.goto(origin+'/fixture');await page.locator('.v2-header').waitFor();
  assert.match(await page.locator('.v2-header').evaluate(el=>getComputedStyle(el,'::after').maskImage),/linear-gradient/);
  for(const viewport of [{width:1440,height:800},{width:1366,height:600},{width:390,height:700}]){
    await page.setViewportSize(viewport);
    assert.equal(Math.round((await page.locator('.v2-header').boundingBox()).height),viewport.width>760?60:56);
    await page.evaluate(()=>window.fixture.open());
    for(const [label,option] of [['组件宽度','整行'],['组件高度','最大工作区']]){
      await page.locator('.n-form-item').filter({hasText:label}).locator('.n-select').click();
      const item=page.locator('.n-base-select-option').filter({hasText:new RegExp('^'+option+'$')});
      await item.scrollIntoViewIfNeeded();await page.waitForTimeout(250);
      assert(await item.evaluate(el=>{const r=el.getBoundingClientRect();return r.top>=0&&r.bottom<=innerHeight&&el.contains(document.elementFromPoint(r.x+r.width/2,r.y+r.height/2));}),'last option must be visible and clickable, not clipped by the modal');
      await page.screenshot({path:path.join(output,viewport.width+'-'+label+'.png')});
      await item.click();
    }
    await page.getByRole('button',{name:'应用设置',exact:true}).click();
    assert.equal(await page.evaluate(()=>window.fixture.applied().w),12);assert.equal(await page.evaluate(()=>window.fixture.applied().h),30);
    await page.locator('.v2-widget-editor').waitFor({state:'hidden'});
  }
  await page.setViewportSize({width:1440,height:800});await page.bringToFront();await page.waitForTimeout(600);
  await page.evaluate(()=>window.fixture.openOther());
  for(const [input,menu] of [['.n-date-picker','.n-date-panel'],['.n-time-picker','.n-time-picker-panel'],['button:has-text("展开选项")','.n-popover']]){
    await page.locator('.n-modal').locator(input).click();const panel=page.locator(menu).last();await panel.waitFor();await page.waitForTimeout(250);
    assert(await panel.evaluate(el=>!el.closest('.n-modal')&&el.contains(document.elementFromPoint(el.getBoundingClientRect().x+10,el.getBoundingClientRect().y+10))),'floating control escapes the scrolling modal');
    await page.locator('.n-card-header__main').filter({hasText:'浮层回归'}).click();
  }
  await page.evaluate(()=>window.fixture.closeOther());await page.locator('.n-modal').waitFor({state:'hidden'});
  const hit=page.locator('[data-bot-hit]:visible').first();await hit.waitFor();const box=await hit.boundingBox();
  await page.mouse.move(box.x+box.width/2,box.y+box.height/2);await page.mouse.down();await page.mouse.move(box.x+box.width/2,0,{steps:15});await page.mouse.up();
  await page.waitForFunction(()=>window.fixture.position().y===12-160*.2);await page.waitForTimeout(500);
  const lamp=page.locator('rect[data-bot-hit]:visible').first(),moved=await lamp.boundingBox();
  assert(moved.y>=0&&moved.y+moved.height/2<60,'lamp remains visible and can be placed over the header');
  await lamp.click();assert.equal(await page.evaluate(()=>window.fixture.clicks()),1,'launcher remains clickable over the header');
  await page.screenshot({path:path.join(output,'assistant-in-header.png')});
  assert.deepEqual(errors,[]);console.log('PASS: compact header with bottom fade, uncut select/date/time/popover menus, assistant drag into header');
}finally{await browser.close();await server.close();}
