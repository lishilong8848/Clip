import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { createServer } from 'vite';
import vue from '@vitejs/plugin-vue';
import { chromium } from 'playwright';

const root = fileURLToPath(new URL('..', import.meta.url));
const output = path.resolve(root, '../../../output/playwright/weather-lighthouse');
await mkdir(output, { recursive: true });
const html = `<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<style>body{margin:0;background:#edf1f6;font:14px sans-serif}#under{position:fixed;inset:0;width:100%;height:100%;border:0;background:transparent}.lighthouse-launcher{position:fixed;right:24px;bottom:24px;border:0;padding:0;background:none;pointer-events:none}.chat{position:fixed;right:200px;bottom:24px;width:400px;padding:24px;background:white}#preview{position:absolute;left:24px;top:24px;padding:50px 24px 24px}</style>
<div id="app"></div><script type="module">
import {createApp,h,ref} from 'vue';import Bot from '/src/components/LighthouseBot.vue';
import Settings from '/src/components/LighthouseBotSettings.vue';import UiTransition from '/src/components/UiTransition.vue';
import {DEFAULT_BOT} from '/src/botAppearance.ts';import {weatherScene} from '/src/botWeather.ts';
const appearance=ref({...DEFAULT_BOT,shape:'weather-lighthouse',size:144}),shown=ref(true),open=ref(false),settings=ref(false),retained=ref(null);
window.fixture={appearance,shown,open,settings,weatherScene,set:patch=>Object.assign(appearance.value,patch)};
window.underClicks=0;
createApp({render(){return h('main',[
 h('button',{id:'under',onClick:()=>window.underClicks++},'底层页面'),
 h('div',{id:'preview'},h(Bot,{appearance:appearance.value,visible:shown.value})),
 h('button',{class:'lighthouse-launcher',onClick:()=>open.value=!open.value},h(Bot,{appearance:appearance.value,interactive:true,visible:shown.value,position:retained.value,onMoved:p=>retained.value=p})),
 open.value?h('section',{class:'chat'},[h('h2','灯塔助手'),h('button',{id:'settings',onClick:()=>settings.value=true},'图标设置'),h('button',{id:'close',onClick:()=>open.value=false},'关闭')]):null,
 h(Settings,{open:settings.value,appearance:appearance.value,onClose:()=>settings.value=false,onSaved:a=>appearance.value=a})
 ])}}).component('UiTransition',UiTransition).mount('#app');
</script>`;
const server = await createServer({root,configFile:false,plugins:[vue(),{name:'weather-fixture',configureServer(dev){dev.middlewares.use('/__weather',async(req,res)=>{res.setHeader('Content-Type','text/html; charset=utf-8');res.end(await dev.transformIndexHtml('/__weather',html));});}}],logLevel:'error',server:{host:'127.0.0.1',port:0,hmr:false}});
await server.listen(); const origin='http://127.0.0.1:'+server.httpServer.address().port;
const browser=await chromium.launch({headless:true});
const errors=[],calls=[];
try {
 const page=await browser.newPage({viewport:{width:1440,height:900}});
 page.on('pageerror',err=>errors.push(err.message));
 await page.route(origin+'/api/**',async route=>{
  const request=route.request(),url=new URL(request.url());calls.push(url.pathname+'?'+url.searchParams.toString());
  let data;
  if(url.pathname==='/api/assistant/appearance') { assert.equal(request.method(),'PUT');data=request.postDataJSON(); }
  else {
   assert.equal(url.pathname,'/api/assistant/icon-weather');assert.equal(request.method(),'GET');
   const city=url.searchParams.get('city');const actual=Date.now()/1000;
   const start=Math.floor((actual+8*3600)/86400)*86400-8*3600;
   const now=start+(city==='上海'?22:12)*3600;
   data={available:city!=='无效城市',city,date:new Date((now+8*3600)*1000).toISOString().slice(0,10),description:'晴转多云',minimum:13,maximum:23,fetched_at:now,server_time:now,sunrise:start+6*3600,sunset:start+18*3600};
  }
  await route.fulfill({json:{ok:true,data}});
 });
 await page.goto(origin+'/__weather');
 await page.waitForFunction(()=>document.querySelector('.lighthouse-launcher .weather-lighthouse')?.dataset.weather==='clear');
 assert.equal(calls.length,1,'launcher and preview share one request');
 const orbitStart=await page.locator('.lighthouse-launcher .wl-sun').getAttribute('style');
 await page.waitForTimeout(1300);
 assert.notEqual(await page.locator('.lighthouse-launcher .wl-sun').getAttribute('style'),orbitStart,'celestial position advances without another weather request');
 assert.equal(calls.length,1,'clock animation never bypasses weather cache');
 const dayColor=await page.locator('.lighthouse-launcher .wl-tower-mid').evaluate(el=>getComputedStyle(el).stopColor);
 const dayWater=await page.locator('.lighthouse-launcher .wl-swell').evaluate(el=>getComputedStyle(el).stroke);
 const waveDurations=await page.locator('.lighthouse-launcher .wl-waves path').evaluateAll(nodes=>nodes.map(node=>getComputedStyle(node).animationDuration));
 assert.equal(new Set(waveDurations).size,3,'water layers have independently varied rhythms');
 const waterBefore=await page.locator('.lighthouse-launcher .wl-swell').evaluate(el=>getComputedStyle(el).transform);
 await page.waitForTimeout(350);
 assert.notEqual(await page.locator('.lighthouse-launcher .wl-swell').evaluate(el=>getComputedStyle(el).transform),waterBefore,'water gently ripples');
 const allIds=await page.locator('svg [id]').evaluateAll(nodes=>nodes.map(node=>node.id));
 assert.equal(new Set(allIds).size,allIds.length,'SVG gradient ids are unique across instances');
 const before=await page.locator('.lighthouse-launcher .wl-eyes').innerHTML();
 await page.mouse.move(100,100);await page.waitForTimeout(600);
 assert.notEqual(await page.locator('.lighthouse-launcher .wl-eyes').innerHTML(),before,'eyes follow cursor');
 for (const [x,y] of [[0,0],[1440,0],[0,900],[1440,900],[720,450]]) {
  await page.mouse.move(x,y);await page.waitForTimeout(380);
  const contained=await page.locator('.lighthouse-launcher .wl-eyes path').evaluateAll(nodes=>nodes.every(node=>{
    const b=node.getBBox(),matrix=node.getCTM(),parent=node.closest('.wl-assembly').getCTM().inverse();
    return [[b.x,b.y],[b.x+b.width,b.y],[b.x,b.y+b.height],[b.x+b.width,b.y+b.height]].every(([x,y])=>{
      const point=new DOMPoint(x,y).matrixTransform(matrix).matrixTransform(parent);
      return point.x>=352&&point.x<=408&&point.y>=127&&point.y<=163;
    });
  }));
  assert(contained,'eyes stay inside lamp at every cursor extreme');
 }
 const box=await page.locator('.lighthouse-launcher').boundingBox();
 await page.mouse.click(box.x+4,box.y+4);assert.equal(await page.evaluate(()=>window.underClicks),1);
 await page.mouse.click(box.x+box.width/2,box.y+box.height/2);await page.locator('.chat').waitFor();
 assert(await page.locator('.lighthouse-launcher .wl-tower').isVisible(),'icon remains when chat opens');
 await page.locator('#settings').click();
 const dialog=page.getByRole('dialog',{name:'图标设置'});await dialog.waitFor();
 await dialog.getByLabel('天气城市',{exact:true}).fill('上海');
 await dialog.getByRole('button',{name:'保存',exact:true}).click();
 await dialog.waitFor({state:'detached'});
 await page.waitForFunction(()=>document.querySelector('.lighthouse-launcher .weather-lighthouse')?.dataset.phase==='night');
 await page.waitForTimeout(900);
 const nightColor=await page.locator('.lighthouse-launcher .wl-tower-mid').evaluate(el=>getComputedStyle(el).stopColor);
 assert.notEqual(nightColor,dayColor,'night tower is dimmed without costly SVG filters');
 assert.notEqual(await page.locator('.lighthouse-launcher .wl-swell').evaluate(el=>getComputedStyle(el).stroke),dayWater,'night water is dimmed too');
 assert(Number(await page.locator('.lighthouse-launcher .wl-lamp').evaluate(el=>getComputedStyle(el).opacity))>.9,'lamp remains lit inside the dark tower');
 assert.match(await page.locator('.lighthouse-launcher .lighthouse-bot').getAttribute('title'),/上海.*今日预报/);
 await page.screenshot({path:path.join(output,'night-chat.png')});
 await page.evaluate(()=>window.fixture.set({size:200}));await page.waitForTimeout(900);
 await page.locator('#preview').screenshot({path:path.join(output,'lighthouse-night.png')});
 await page.evaluate(()=>window.fixture.set({size:144}));await page.waitForTimeout(400);
 await page.locator('#close').click();
 await page.evaluate(()=>window.fixture.set({snap_back:false}));
 const from=await page.locator('.lighthouse-launcher').boundingBox();
 await page.mouse.move(from.x+72,from.y+90);await page.mouse.down();await page.mouse.move(from.x-100,from.y-80,{steps:12});await page.mouse.up();
 await page.waitForTimeout(700);assert((await page.locator('.lighthouse-launcher').boundingBox()).x<from.x-80,'drag retains position');
 await page.evaluate(()=>window.fixture.set({weather_city:'无效城市'}));
 await page.waitForFunction(()=>document.querySelector('.lighthouse-launcher .weather-lighthouse')?.dataset.weather==='unknown');
 await page.waitForTimeout(550);assert.match(await page.locator('.lighthouse-launcher .lighthouse-bot').getAttribute('title'),/天气暂不可用/);
 const paused=await page.evaluate(async()=>{
  window.fixture.shown.value=false;await new Promise(r=>setTimeout(r,50));
  const node=document.querySelector('.lighthouse-launcher .weather-lighthouse');let changes=0;
  const observer=new MutationObserver(()=>changes++);observer.observe(node,{attributes:true,subtree:true});
  await new Promise(r=>setTimeout(r,300));observer.disconnect();return {changes,play:getComputedStyle(node.querySelector('.wl-beams')).animationPlayState};
 });
 assert.equal(paused.changes,0,'hidden bot stops eye rendering');assert.equal(paused.play,'paused','hidden weather stops animation');
 await page.evaluate(()=>window.fixture.shown.value=true);
 await page.emulateMedia({reducedMotion:'reduce'});
 await page.waitForTimeout(80);
 assert.equal(await page.locator('.lighthouse-launcher .wl-ripple').evaluate(el=>getComputedStyle(el).animationName),'none');
 const checks=await page.evaluate(()=>{
  const today=Date.UTC(2026,9,14)/1000,start=today-8*3600;
  const data={available:true,city:'南通',date:'2026-10-14',description:'晴转小雨',fetched_at:start+12*3600,sunrise:start+6*3600,sunset:start+18*3600};
  return [window.fixture.weatherScene(data,start+12*3600),window.fixture.weatherScene({...data,fetched_at:start+21*3600},start+21*3600),window.fixture.weatherScene(data,start+86400+12*3600)];
 });
 assert.deepEqual(checks.map(value=>value.weather),['clear','rain','unknown']);assert.equal(checks[1].phase,'night');
 const trajectory=await page.evaluate(()=>{
   const start=Date.UTC(2026,9,14)/1000-8*3600;
   return [7,12,17].map(hour=>window.fixture.weatherScene({available:false,city:'南通',sunrise:start+6*3600,sunset:start+18*3600},start+hour*3600));
 });
 assert(trajectory[0].x<trajectory[1].x&&trajectory[1].x<trajectory[2].x,'clockwise arc moves from left to top to right');
 assert(trajectory[1].y<trajectory[0].y&&trajectory[1].y<trajectory[2].y,'noon celestial body is above tower, not in the lamp');
 const dawn=await page.evaluate(()=>{
   const start=Date.UTC(2026,9,14)/1000-8*3600,weather={available:false,city:'南通',sunrise:start+6*3600,sunset:start+18*3600};
   return [329,331].map(minutes=>window.fixture.weatherScene(weather,start+minutes*60));
 });
 assert(Math.abs(dawn[0].moonX-dawn[1].moonX)<3,'moon and its stars keep their own orbit while fading at dawn, without teleporting to the sun');
 await page.evaluate(()=>window.fixture.set({weather_city:'南通',size:200}));await page.waitForTimeout(650);
 await page.waitForFunction(()=>document.querySelector('#preview .weather-lighthouse')?.dataset.weather==='clear');
 await page.waitForTimeout(1000);
 assert.equal(await page.locator('#preview .wl-stars').evaluate(el=>getComputedStyle(el).opacity),'0','stars fade away in daytime');
 const art=await page.locator('#preview .weather-lighthouse').evaluate(root=>({
   assembly:root.querySelector('.wl-assembly').getAttribute('transform'),waves:root.querySelector('.wl-waves').getAttribute('transform'),
   title:root.closest('.lighthouse-bot').title,weather:root.dataset.weather,phase:root.dataset.phase,
 }));
 assert.equal(art.waves,null,'only the tower is enlarged, not the waves');
 assert.match(art.assembly,/scale\(1.18 1.12\)/);
 await page.locator('#preview').screenshot({path:path.join(output,'lighthouse-day.png')});
 await page.setViewportSize({width:390,height:844});await page.evaluate(()=>window.fixture.set({size:56}));await page.waitForTimeout(200);
 assert.equal(Math.round((await page.locator('.lighthouse-launcher').boundingBox()).width),56);
 assert.deepEqual(errors,[]);
 console.log(JSON.stringify({ok:true,calls:calls.length,checks:'day/night, Chinese forecast, no foreign queries, shared cache, independent SVGs, settings save, eyes, drag, transparent clicks, hidden/reduced motion, mobile sizing',screenshots:output}));
} finally {await browser.close();await server.close();}
