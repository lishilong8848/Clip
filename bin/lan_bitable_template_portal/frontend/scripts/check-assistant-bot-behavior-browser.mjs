import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';
import { createServer } from 'vite';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const output = path.resolve(root, '../../../output/playwright/assistant-bot');
const backend = process.env.ASSISTANT_PREVIEW_URL || 'http://127.0.0.1:19003';
assert.equal((await (await fetch(backend + '/api/health')).json()).instance_id, 'isolated-lighthouse-stream');
await mkdir(output, { recursive: true });
const html = `<html><head><meta charset="UTF-8"><style>
body{margin:0;padding:24px;font:14px sans-serif}#controls{display:flex;gap:8px}button{cursor:pointer}
.fixture-overlay{position:fixed;inset:0;z-index:5000;background:#17263d30}
.fixture-drawer{position:absolute;right:0;top:0;height:100%;width:680px;background:white;padding:24px}
.fixture-modal{position:absolute;left:560px;bottom:24px;width:300px;height:200px;background:white;padding:24px}
.mop-sign-panel{position:fixed;right:0;top:0;width:680px;height:100%;background:white;z-index:5001}
dialog{width:590px;padding:24px;border:1px solid #abb}
</style></head><body><div id="app"></div><script type="module">
import {createApp,ref,h,KeepAlive} from 'vue';
import UiTransition from '/src/components/UiTransition.vue';
import LoadingIndicator from '/src/components/LoadingIndicator.vue';
import Assistant from '/src/components/LighthouseAssistant.vue';
import '/src/global.css';
const drawer=ref(false),modal=ref(false),host=ref(true),mop=ref(false);
const first={name:'First',render(){return h(UiTransition,{name:'ui-drawer'},()=>drawer.value?h('div',{class:'fixture-overlay'},h('aside',{class:'fixture-drawer',role:'dialog','aria-label':'测试抽屉'},'抽屉')):null)}};
const second={name:'Second',render(){return h('p',{},'另一页面')}};
window.openDrawer=()=>drawer.value=true;window.closeDrawer=()=>drawer.value=false;
window.openModal=()=>modal.value=true;window.closeModal=()=>modal.value=false;
window.toggleHost=()=>host.value=!host.value;
const app=createApp({render(){return h('div',{class:'app-shell'},[
 h('div',{id:'controls'},[
 h('button',{id:'drawer-open',onClick:window.openDrawer},'抽屉'),h('input',{id:'page-input'}),
 h('button',{id:'mop-open',onClick:()=>mop.value=true},'维护单签名'),
 h('button',{id:'native-open',onClick:()=>document.querySelector('dialog').showModal()},'原生弹窗')]),
 h(KeepAlive,{},()=>h(host.value?first:second,{key:host.value?'first':'second'})),
 h(UiTransition,{},()=>modal.value?h('div',{class:'fixture-overlay'},h('section',{class:'fixture-modal',role:'dialog'},'弹窗')):null),
 h(UiTransition,{name:'ui-drawer'},()=>mop.value?h('div',{class:'signature-manager-backdrop fixture-overlay'}):null),
 mop.value?h('section',{class:'mop-sign-panel manager-open',role:'dialog'},h('button',{id:'mop-close',onClick:()=>mop.value=false},'关闭维护签名')):null,
 h('dialog',{id:'native-dialog'},[h('input',{id:'native-input'}),h('button',{id:'native-close',onClick:()=>document.querySelector('dialog').close()},'关闭')]),
 h(Assistant,{userName:'隔离测试D',userId:'fixture-D'})
 ])}}).component('UiTransition',UiTransition).component('LoadingIndicator',LoadingIndicator);
app.mount('#app');window.ready=true;window.unmount=()=>app.unmount();
</script></body></html>`;
const server = await createServer({ root, logLevel: 'error', server: { host: '127.0.0.1', port: 0, hmr: false, proxy: { '/api': {
  target:backend,changeOrigin:true,configure(proxy){proxy.on('proxyReq',request=>request.setHeader('origin',backend));},
} } },
  plugins: [{ name: 'assistant-bot-check', configureServer(dev) {
    dev.middlewares.use('/__bot-check', async (_req, res) => {
      res.setHeader('Content-Type', 'text/html'); res.end(await dev.transformIndexHtml('/__bot-check', html));
    });
  } }],
});
let browser, page;
const errors = [];
const same = (a, b) => Math.hypot(a.x - b.x, a.y - b.y) < 3;
const overlaps = (a, b) => a.x < b.x + b.width && a.x + a.width > b.x && a.y < b.y + b.height && a.y + a.height > b.y;
const iconBox = () => page.locator('.assistant-launcher').boundingBox();
async function motion(action) {
  return page.evaluate(action => new Promise(resolve => {
    const samples=[],start=performance.now(); window[action]();
    function sample(now) {
      const r=document.querySelector('.assistant-launcher').getBoundingClientRect(); samples.push({x:r.x,y:r.y});
      if(now-start<750)requestAnimationFrame(sample);else resolve(samples);
    }
    requestAnimationFrame(sample);
  }), action);
}
async function assertVisibleAbove() {
  assert(await page.locator('.assistant-launcher').isVisible());
  assert(await page.locator('.assistant-launcher').evaluate(el => {
    const r=el.getBoundingClientRect();return el.contains(document.elementFromPoint(r.x+r.width/2,r.y+r.height/2));
  }), 'launcher stays visible and clickable above the overlay');
}
async function assertIdle() {
  await page.waitForFunction(() => document.querySelector('.assistant-launcher .lighthouse-bot')?.dataset.botMood === 'idle');
}
try {
  await server.listen();
  const { springStep } = await server.ssrLoadModule('/src/vendor/bloub/engine/math.ts');
  const settleAt = hz => { let x=600,v=0;for(let n=0;n<hz/2;n++)[x,v]=springStep(x,v,0,1/hz);return x; };
  assert(Math.abs(settleAt(30)-settleAt(120))<0.001,'motion has the same timing at different frame rates');
  assert(settleAt(60)<1,'long reposition settles without snapping');
  const base={x:1360,y:920},drawerRect={left:760,top:0,right:1440,bottom:1000};
  browser=await chromium.launch({headless:true});
  const context=await browser.newContext({viewport:{width:1440,height:1000},reducedMotion:'no-preference'});
  await context.addCookies([{name:'fixture_user',value:'D',url:backend}]);
  await context.request.put(backend+'/api/assistant/appearance',{data:{color:'encre',shape:'cercle',size:56,snap_back:true},headers:{origin:backend}});
  page=await context.newPage();page.on('pageerror',error=>errors.push(error.message));
  await page.goto(`http://127.0.0.1:${server.httpServer.address().port}/__bot-check`);
  await page.locator('.assistant-launcher svg').waitFor();await page.waitForTimeout(350);
  const original=await iconBox();assert(same(original,base));
  await page.evaluate(() => {
    window.fixtureHasFocus = document.hasFocus.bind(document);
    document.hasFocus = () => false;
    window.dispatchEvent(new Event('blur'));
  });
  await page.waitForFunction(() => document.querySelector('.assistant-launcher .lighthouse-bot')?.dataset.animationActive === 'false');
  const pausedScene = await page.locator('.assistant-launcher svg').innerHTML();
  await page.waitForTimeout(500);
  assert.equal(await page.locator('.assistant-launcher svg').innerHTML(), pausedScene, 'unfocused window must stop SVG repainting');
  await page.evaluate(() => { document.hasFocus = window.fixtureHasFocus; window.dispatchEvent(new Event('focus')); });
  await page.waitForFunction(() => document.querySelector('.assistant-launcher .lighthouse-bot')?.dataset.animationActive === 'true');
  await page.waitForTimeout(1000);
  assert(await page.locator('.assistant-launcher svg mask g').evaluate(group=>{
    const eyes=[...group.querySelectorAll('path')];return eyes.length===2&&eyes.reduce((n,eye)=>n+new DOMMatrix(eye.getAttribute('transform')).e,0)<0;
  }),'resting eyes face left into the page');
  await page.getByRole('button',{name:'打开灯塔助手',exact:true}).click();
  await page.getByRole('button',{name:'图标设置',exact:true}).click();
  const slider=page.getByRole('slider',{name:'尺寸',exact:true});await slider.focus();await slider.press('End');
  await page.waitForFunction(()=>document.querySelector('.assistant-launcher').getBoundingClientRect().width===200);
  assert.equal((await page.locator('.bot-settings .preview .lighthouse-bot').boundingBox()).width,200,'settings preview uses the selected real size');
  const previewBox=await page.locator('.bot-settings .preview').boundingBox();
  const settingsIcon=await page.locator('.bot-settings .preview .lighthouse-bot').boundingBox();
  assert(settingsIcon.y>=previewBox.y&&settingsIcon.y+settingsIcon.height<=previewBox.y+previewBox.height,'200px preview never overlaps color controls');
  await slider.press('Home');await page.waitForFunction(()=>document.querySelector('.assistant-launcher').getBoundingClientRect().width===40);
  await page.getByRole('button',{name:'取消',exact:true}).click();await page.getByRole('dialog',{name:'图标设置',exact:true}).waitFor({state:'detached'});
  assert.equal((await iconBox()).width,56,'cancel restores saved size');
  await page.getByRole('button',{name:'收起助手',exact:true}).click();await page.locator('.assistant-panel').waitFor({state:'detached'});
  const svgId=await page.locator('.assistant-launcher svg mask').getAttribute('id');
  const greeting=await page.evaluate(()=>new Promise(resolve=>{
    const values=[],start=performance.now();history.pushState({},'',location.pathname+'?view=next');window.dispatchEvent(new PopStateEvent('popstate'));
    function sample(now){values.push(document.querySelector('[data-bot-scene]').getAttribute('transform'));if(now-start<650)requestAnimationFrame(sample);else resolve([...new Set(values)]);}
    requestAnimationFrame(sample);
  }));
  assert(greeting.length>8,'page changes get a continuous greeting motion');
  assert.equal(await page.locator('.assistant-launcher svg mask').getAttribute('id'),svgId,'SPA navigation keeps the same renderer');
  await page.reload();await page.locator('.assistant-launcher svg').waitFor();
  assert.equal(await page.locator('.assistant-launcher .lighthouse-bot').getAttribute('data-motion-resumed'),'true','hard page changes restore per-account animation state');
  const paths=await page.locator('.assistant-launcher svg path').first().getAttribute('d');
  await page.waitForTimeout(2700);
  assert.notEqual(await page.locator('.assistant-launcher svg path').first().getAttribute('d'),paths,'native idle cycle animates automatically');
  await page.locator('#page-input').focus();
  assert.equal(await page.locator('.assistant-launcher .lighthouse-bot').getAttribute('data-bot-mood'),'engaged');
  await page.locator('#page-input').evaluate(el=>el.blur());await assertIdle();
  await motion('openDrawer');const outside=await iconBox();
  assert(same(outside,original),'opening drawer never moves the floating bot');
  assert(overlaps(outside,await page.locator('.fixture-drawer').boundingBox()),'bot floats above the drawer, not outside it');
  await assertVisibleAbove();
  const stored=await page.evaluate(()=>({...localStorage}));
  await motion('openModal');await assertVisibleAbove();
  assert(same(await iconBox(),original),'modal does not move the bot');
  await motion('closeModal');assert(same(await iconBox(),outside));
  await page.evaluate(()=>{window.closeDrawer();setTimeout(window.openDrawer,40)});await page.waitForTimeout(500);
  assert.equal(await page.locator('.fixture-drawer').count(),1,'cancelled leave retains one drawer');
  await page.evaluate(()=>window.toggleHost());await page.waitForTimeout(450);
  assert.equal(await page.locator('.fixture-drawer:visible').count(),0,'cached page deactivation removes its overlay');
  assert(same(await iconBox(),original));
  await page.evaluate(()=>window.toggleHost());await page.waitForTimeout(450);
  assert.equal(await page.locator('.fixture-drawer:visible').count(),1,'cached page activation restores its overlay');
  assert(same(await iconBox(),outside));
  const finish=await motion('closeDrawer');
  assert(finish.every(p=>same(p,original)),'closing drawer leaves bot position unchanged');
  assert(same(await iconBox(),original));
  assert.deepEqual(await page.evaluate(()=>({...localStorage})),stored,'temporary avoidance never writes user position');
  await page.locator('#mop-open').click();await page.waitForTimeout(450);
  assert(same(await iconBox(),original),'signature drawer does not move the bot');
  await page.locator('#mop-close').click();await page.waitForTimeout(450);assert(same(await iconBox(),original));
  await page.locator('#native-open').click();await page.waitForTimeout(450);await assertVisibleAbove();
  await page.locator('#native-input').fill('native dialog remains usable');
  await page.getByRole('button',{name:'打开灯塔助手',exact:true}).click();
  await page.getByRole('textbox',{name:'询问灯塔助手'}).waitFor();await assertVisibleAbove();
  await page.getByRole('button',{name:'图标设置',exact:true}).click();
  await page.getByRole('dialog',{name:'图标设置',exact:true}).waitFor();
  await page.getByRole('button',{name:'取消',exact:true}).click();
  await page.getByRole('dialog',{name:'图标设置',exact:true}).waitFor({state:'detached'});
  await page.evaluate(()=>{
    const inner=document.createElement('dialog');inner.id='internal-dialog';inner.textContent='助手内原图';
    document.querySelector('.assistant-panel').appendChild(inner);inner.showModal();
  });
  await page.waitForTimeout(350);await assertVisibleAbove();
  assert(await page.locator('#internal-dialog .lighthouse-launcher').count(),'assistant-owned native dialog avoids circular teleport');
  await page.evaluate(()=>{const inner=document.getElementById('internal-dialog');inner.close();inner.remove();});
  await page.waitForTimeout(350);await assertVisibleAbove();
  await page.getByRole('button',{name:'收起助手',exact:true}).click();
  await page.locator('.assistant-panel').waitFor({state:'detached'});
  await page.locator('#native-close').click();await page.waitForTimeout(450);assert(same(await iconBox(),original));
  await page.getByRole('button',{name:'打开灯塔助手',exact:true}).click();
  await page.getByRole('textbox',{name:'询问灯塔助手'}).fill('快速测试自动表情');
  await page.getByRole('button',{name:'发送问题',exact:true}).click();
  await page.waitForFunction(()=>document.querySelector('.assistant-launcher .lighthouse-bot')?.dataset.botMood==='thinking');
  await page.waitForFunction(()=>document.querySelector('.assistant-launcher .lighthouse-bot')?.dataset.botMood==='success',null,{timeout:20000});
  await page.screenshot({path:path.join(output,'automatic-desktop.png')});
  await page.route('**/api/assistant/messages',async route=>{
    if(route.request().method()==='POST'){await new Promise(resolve=>setTimeout(resolve,200));return route.fulfill({status:503,json:{ok:false,error:'隔离模拟错误'}});}
    return route.continue();
  });
  await page.getByRole('textbox',{name:'询问灯塔助手'}).fill('隔离模拟失败');
  await page.getByRole('button',{name:'发送问题',exact:true}).click();
  await page.waitForFunction(()=>document.querySelector('.assistant-launcher .lighthouse-bot')?.dataset.botMood==='error');
  await page.unroute('**/api/assistant/messages');
  await page.getByRole('button',{name:'图标设置',exact:true}).click();
  await page.getByLabel('拖拽后返回右下角',{exact:true}).uncheck();
  await page.getByRole('button',{name:'保存',exact:true}).click();
  await page.getByRole('dialog',{name:'图标设置',exact:true}).waitFor({state:'detached'});
  await page.getByRole('button',{name:'收起助手',exact:true}).click();await page.locator('.assistant-panel').waitFor({state:'detached'});await assertIdle();
  const beforeDrag=await iconBox();
  await page.mouse.move(beforeDrag.x+28,beforeDrag.y+28);await page.mouse.down();
  await page.mouse.move(beforeDrag.x-132,beforeDrag.y-152,{steps:12});await page.waitForTimeout(100);
  assert(await page.locator('.assistant-launcher [data-bot-scene]').evaluate(el=>{
    const m=new DOMMatrix(getComputedStyle(el).transform);return m.a*m.d-m.b*m.c>1.07&&m.f<-1;
  }),'drag has an animated lift and soft body deformation');
  await page.mouse.up();await page.waitForTimeout(500);
  const retained=await iconBox(),saved=await page.evaluate(()=>({...localStorage}));
  assert(!same(retained,original));assert.equal(await page.locator('.assistant-panel').count(),0,'native drag does not open chat');
  await motion('openDrawer');assert(same(await iconBox(),retained));await assertVisibleAbove();
  await motion('closeDrawer');assert(same(await iconBox(),retained),'overlay close restores the user-retained position');
  assert.deepEqual(await page.evaluate(()=>({...localStorage})),saved,'retained position is not overwritten by avoidance');
  await page.emulateMedia({reducedMotion:'reduce'});
  await page.waitForFunction(()=>document.querySelector('.assistant-launcher .lighthouse-bot')?.dataset.animationActive==='true');
  await motion('openDrawer');await assertVisibleAbove();
  assert(same(await iconBox(),retained));
  await motion('closeDrawer');
  await page.evaluate(()=>document.getElementById('native-dialog').showModal());await page.waitForTimeout(250);
  await page.evaluate(()=>document.getElementById('native-dialog').remove());await page.waitForTimeout(750);
  await assertVisibleAbove();assert(same(await iconBox(),retained),'removed modal host cannot strand the launcher');
  await page.evaluate(()=>window.unmount());
  assert.equal(await page.locator('.lighthouse-launcher, .lighthouse-layer').count(),0,'all floating nodes released on unmount');
  assert.deepEqual(errors,[],'no runtime errors');
  console.log('[pass] automatic moods, stable bot above drawers/modals, cached overlays, native top layer, reduced motion and cleanup');
} catch(error) {
  if(page)await page.screenshot({path:path.join(output,'behavior-failure.png')});
  console.error(errors);throw error;
} finally { await browser?.close();await server.close(); }
