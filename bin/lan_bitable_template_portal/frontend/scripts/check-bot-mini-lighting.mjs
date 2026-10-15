import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { createServer } from 'vite';
import { chromium } from 'playwright';

const root = fileURLToPath(new URL('..', import.meta.url));
const output = path.resolve(root, '../../../output/playwright/weather-lighthouse-mini');
await mkdir(output, { recursive: true });
const html = `<!doctype html><meta charset="utf-8"><style>
body{margin:0;padding:24px;background:#edf1f6;font:14px system-ui;color:#253853}h1{font-size:18px;margin:0 0 16px}
main{display:grid;grid-template-columns:repeat(3,200px);gap:12px}article{text-align:center;padding:12px 0;border-bottom:1px solid #cfd9e5}
.scene{width:168px;height:168px;margin:auto}p{margin:10px 0 0}small{color:#667991}
#moon-gallery{display:grid;grid-template-columns:repeat(4,156px);gap:4px;margin-top:24px}#moon-gallery section{text-align:center;padding:10px 0}#moon-gallery p{margin:6px 0;font-size:13px}
</style><h1>雾屿灯塔 mini</h1><main id="scenes"></main><script type="module">
import {BloubBot} from '/src/vendor/bloub/entry.ts';
import {weatherScene} from '/src/botWeather.ts';
import {miniLighting,moonPath} from '/src/vendor/bloub/weatherLighthouseMini.ts';
import {lunarMoon} from '/src/lunarMoon.ts';
const start=Date.UTC(2026,9,15)/1000-8*3600;
function scene(hour){const now=start+hour*3600;return weatherScene({available:true,city:'南通',description:'多云',date:'2026-10-15',fetched_at:now,sunrise:start+6*3600,sunset:start+18*3600},now);}
window.fixture={bots:[],scene,miniLighting,moonPath,lunarMoon,BloubBot,weatherScene};
for(const [hour,title,time] of [[3,'凌晨','03:00'],[6.2,'日出','06:12'],[12,'白天','12:00'],[17.75,'日落','17:45'],[18.6,'暮色','18:36'],[22,'夜晚','22:00']]){
 const item=document.createElement('article');item.dataset.hour=hour;
 const host=document.createElement('div');host.className='scene';item.append(host);
 const label=document.createElement('p');label.textContent=title+' · '+time;item.append(label);document.querySelector('main').append(item);
 const bot=new BloubBot(host,{shape:'weather-lighthouse-mini',size:168,autoplay:false,draggable:false,reactOnClick:false});
 bot.setWeather(scene(hour));bot.setActive(true);window.fixture.bots.push(bot);
}
const gallery=document.createElement('div');gallery.id='moon-gallery';document.body.append(gallery);
for(const [date,label] of [['2026-10-10T00:00:00','新月'],['2026-10-13T00:00:00','渐盈月牙'],['2026-10-17T12:00:00','上弦月'],['2026-10-21T12:00:00','渐盈凸月'],['2026-10-25T00:00:00','满月'],['2026-10-28T12:00:00','渐亏凸月'],['2026-11-01T12:00:00','下弦月'],['2026-11-05T12:00:00','渐亏月牙']]){
 const lunar=lunarMoon(Date.parse(date+'+08:00')/1000),item=document.createElement('section');
 item.innerHTML='<svg width="68" height="68" viewBox="-21 -21 42 42"><path fill="#f8e6ac"/></svg>';
 item.querySelector('path').setAttribute('d',moonPath(lunar.phase));
 const text=document.createElement('p');text.textContent=label;item.append(text);
 const day=document.createElement('small');day.textContent='农历 '+lunar.day+' 日';item.append(day);gallery.append(item);
}
window.ready=true;
</script>`;
const server = await createServer({ root, configFile: false, logLevel: 'error', server: { host: '127.0.0.1', port: 0, hmr: false },
  plugins: [{ name: 'mini-lighting-fixture', configureServer(dev) {
    dev.middlewares.use('/__mini', async (req, res) => { res.setHeader('Content-Type', 'text/html; charset=utf-8'); res.end(await dev.transformIndexHtml('/__mini', html)); });
  } }],
});
await server.listen();
const browser = await chromium.launch({ headless: true });
try {
  const page = await browser.newPage({ viewport: { width: 684, height: 570 }, timezoneId: 'America/Los_Angeles' });
  const errors = [], requests = [];
  page.on('pageerror', error => errors.push(error.message));
  await page.route('**/*', route => {
    if (new URL(route.request().url()).pathname.startsWith('/api/')) {
      requests.push(route.request().url()); return route.abort();
    }
    return route.continue();
  });
  await page.goto('http://127.0.0.1:' + server.httpServer.address().port + '/__mini');
  await page.waitForFunction(() => window.ready);
  await page.waitForTimeout(1400);
  const states = await page.locator('.weather-lighthouse-mini').evaluateAll(roots => roots.map(root => ({
    tower: getComputedStyle(root.querySelector('.wl-tower-mid')).stopColor,
    water: getComputedStyle(root.querySelector('.wl-swell')).stroke,
    cloudTop: getComputedStyle(root.querySelector('.wl-cloud-start')).stopColor,
    cloudBottom: getComputedStyle(root.querySelector('.wl-cloud-end')).stopColor,
    sun: getComputedStyle(root.querySelector('.wl-sun-end')).stopColor,
    sunVisible: getComputedStyle(root.querySelector('.wl-sun')).visibility,
    sunOpacity: getComputedStyle(root.querySelector('.wl-sun')).opacity,
  })));
  assert.equal(new Set(states.map(state => state.tower)).size, 6, 'each lighting period has its own tower illumination');
  assert.notEqual(states[0].water, states[2].water, 'day and night water colors differ');
  assert(states.every(state => state.cloudTop !== state.cloudBottom), 'clouds carry a real vertical color gradient');
  assert.notEqual(states[1].sun, states[2].sun, 'sunrise and noon sun color differ');
  assert.notEqual(states[2].sun, states[3].sun, 'sunset warms the sun');
  assert(states.every(state => state.sunOpacity === '1'), 'solar body remains opaque');
  assert.equal(await page.locator('.weather-lighthouse-mini .wl-moon circle:not(.wl-glint)').count(),0,'moon draws only its illuminated silhouette, without a dark backing disc');
  const lightDirections=await page.locator('.weather-lighthouse-mini').evaluateAll(roots=>roots.map(root=>{
    const gradients=['.wl-tower-start','.wl-enamel-start','.wl-band-start'].map(selector=>root.querySelector(selector).parentElement);
    return gradients.map(gradient=>['x1','y1','x2','y2'].map(name=>Number(gradient.getAttribute(name))));
  }));
  assert(lightDirections[1][0][0]<.2,'sunrise illuminates the left side');
  assert(Math.abs(lightDirections[2][0][0]-.5)<.01&&lightDirections[2][0][1]<.01,'noon illuminates from above');
  assert(lightDirections[3][0][0]>.8,'sunset illuminates the right side');
  assert(lightDirections.every(items=>items.every(item=>JSON.stringify(item)===JSON.stringify(items[0]))),'tower, metal and orange ring share the sky light direction');
  // Calendar cases checked against HKO's published 2026 Gregorian/lunar conversion table.
  const calendar=await page.evaluate(()=>['2026-10-09T23:59:59','2026-10-10T00:00:00','2026-10-15T12:00:00','2026-10-24T12:00:00','2026-10-25T00:00:00','2026-11-08T23:59:59','2026-11-09T00:00:00'].map(value=>window.fixture.lunarMoon(Date.parse(value+'+08:00')/1000)));
  assert.deepEqual(calendar.map(value=>value.day),[29,1,6,15,16,30,1],'lunar dates and month boundaries use Beijing time even on a foreign-timezone browser');
  assert.equal(calendar[4].phase,.5,'calendar midpoint produces a full moon');
  assert(calendar[0].phase>.999&&calendar[5].phase>.999,'both 29- and 30-day months complete the phase cycle');
  const silhouette=await page.evaluate(()=>[0,.1,.25,.5,.75,.9].map(phase=>{
    const path=document.createElementNS('http://www.w3.org/2000/svg','path');path.setAttribute('d',window.fixture.moonPath(phase));
    document.querySelector('#moon-gallery svg').append(path);
    const result={left:path.isPointInFill(new DOMPoint(-10,0)),right:path.isPointInFill(new DOMPoint(10,0))};path.remove();return result;
  }));
  assert.deepEqual(silhouette,[{left:false,right:false},{left:false,right:false},{left:false,right:true},{left:true,right:true},{left:true,right:false},{left:false,right:false}],'waxing and waning light is on opposite sides');
  const cacheCalls=await page.evaluate(()=>{
    const original=Intl.DateTimeFormat.prototype.formatToParts;let count=0;
    Intl.DateTimeFormat.prototype.formatToParts=function(...args){count++;return original.apply(this,args);};
    try{
      const stamp=Date.parse('2026-10-10T00:00:00+08:00')/1000;
      for(let i=0;i<1000;i++)window.fixture.lunarMoon(stamp+i);
      window.fixture.lunarMoon(stamp+86400);return count;
    }finally{Intl.DateTimeFormat.prototype.formatToParts=original;}
  });
  assert.equal(cacheCalls,4,'calendar conversion is cached per Beijing day, not repeated on each animation tick');
  const updatedMoon=await page.evaluate(()=>{
    const bot=window.fixture.bots[0],root=document.querySelector('.weather-lighthouse-mini');
    bot.setWeather({...window.fixture.scene(22),lunarDay:16,lunarPhase:.5});
    const full=root.querySelector('.wl-moon-light').getAttribute('d');
    bot.setWeather({...window.fixture.scene(22),lunarDay:23,lunarPhase:.75});
    const quarter=root.querySelector('.wl-moon-light').getAttribute('d');
    const day=root.querySelector('.wl-moon').dataset.lunarDay;
    bot.setWeather(window.fixture.scene(3));return {full,quarter,day,expected:window.fixture.moonPath(.75)};
  });
  assert.notEqual(updatedMoon.full,updatedMoon.quarter,'open page updates lunar phase without reloading');
  assert.equal(updatedMoon.quarter,updatedMoon.expected);assert.equal(updatedMoon.day,'23');
  const wave = page.locator('.wl-swell').first();
  const before = await wave.evaluate(el => getComputedStyle(el).d);
  await page.waitForTimeout(350);
  assert.notEqual(await wave.evaluate(el => getComputedStyle(el).d), before, 'wave geometry moves while idle');
  const geometry = await wave.evaluate(el => {
    const animation = el.getAnimations()[0], { duration, delay } = animation.effect.getTiming();
    animation.pause();
    const samples = [0, .25, .5, .75, 1].map(t => {
      animation.currentTime = Number(delay) + Number(duration) * (1 + t);
      return { d: getComputedStyle(el).d, points: [.25, .5, .75].map(p => el.getPointAtLength(el.getTotalLength() * p).y) };
    });
    animation.play(); return samples;
  });
  assert.equal(geometry[0].d, geometry[4].d, 'wave loop joins without a jump');
  assert(Math.max(...geometry.map(sample => sample.points[0])) - Math.min(...geometry.map(sample => sample.points[0])) > 6, 'wave heights visibly rise and fall');
  assert.notEqual((geometry[1].points[0] - geometry[0].points[0]).toFixed(2), (geometry[1].points[1] - geometry[0].points[1]).toFixed(2), 'crests propagate instead of translating a rigid line');
  const fadedEdges = await page.locator('.weather-lighthouse-mini').first().evaluate(async root => {
    const result = [];
    for (const wave of root.querySelectorAll('.wl-waves path')) {
      const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
      svg.setAttribute('width', '380'); svg.setAttribute('height', '90'); svg.setAttribute('viewBox', '190 330 380 90');
      svg.append(root.querySelector('defs').cloneNode(true));
      const copy = wave.cloneNode(true), style = getComputedStyle(wave), box = wave.getBBox();
      copy.style.d = style.d; copy.style.animation = 'none'; copy.setAttribute('fill', 'none'); copy.setAttribute('stroke', '#000');
      svg.append(copy);
      const url = URL.createObjectURL(new Blob([new XMLSerializer().serializeToString(svg)], {type:'image/svg+xml'}));
      const image = new Image(); image.src = url; await image.decode();
      const canvas = document.createElement('canvas'); canvas.width = 380; canvas.height = 90;
      const ctx = canvas.getContext('2d'); ctx.drawImage(image, 0, 0); URL.revokeObjectURL(url);
      const alpha = x => Math.max(...Array.from({length:90}, (_, y) => ctx.getImageData(Math.round(x - 190), y, 1, 1).data[3]));
      result.push({left:alpha(box.x + 1),right:alpha(box.x + box.width - 1),center:alpha(380)});
    }
    return result;
  });
  assert(fadedEdges.every(edge => edge.center > 20 && edge.left < edge.center * .15 && edge.right < edge.center * .15), 'every wave fades at both its own ends, without disappearing in the middle: ' + JSON.stringify(fadedEdges));
  const continuous = await page.evaluate(() => [6, 18, 24].map(hour => {
    const before = window.fixture.miniLighting(window.fixture.scene(hour - 1 / 3600));
    const after = window.fixture.miniLighting(window.fixture.scene(hour + 1 / 3600));
    return Object.keys(before).filter(key => key !== 'sun-size').every(key => {
      const a = before[key], b = after[key];
      return [1, 3, 5].every(i => Math.abs(parseInt(a.slice(i, i + 2), 16) - parseInt(b.slice(i, i + 2), 16)) <= 2);
    });
  }));
  assert(continuous.every(Boolean), 'colors stay continuous through sunrise, sunset and midnight');
  await page.screenshot({ path: path.join(output, 'mini-lighting-periods.png'), fullPage: true });
  await page.locator('#moon-gallery').screenshot({ path: path.join(output, 'moon-phases.png') });
  await page.evaluate(async () => {
    window.fixture.bots.forEach(bot => bot.setActive(false));
    await Promise.all([...document.querySelectorAll('.wl-waves path')].flatMap(el=>el.getAnimations().map(animation=>animation.ready)));
  });
  const paused = await wave.evaluate(el => getComputedStyle(el).d);
  await page.waitForTimeout(200);
  assert.equal(await wave.evaluate(el => getComputedStyle(el).d), paused, 'hidden water stops');
  await page.evaluate(async () => {
    window.fixture.bots.forEach(bot => { bot.setReducedMotion(true); bot.setActive(true); });
    await Promise.all([...document.querySelectorAll('.wl-waves path')].flatMap(el=>el.getAnimations().map(animation=>animation.ready)));
  });
  const reduced = await wave.evaluate(el => getComputedStyle(el).d);
  await page.waitForTimeout(200);
  assert.equal(await wave.evaluate(el => getComputedStyle(el).d), reduced, 'reduced motion keeps a still water surface');
  await page.evaluate(() => window.fixture.bots.forEach(bot => bot.destroy()));
  assert.equal(await page.locator('.weather-lighthouse-mini').count(), 0);
  const types = await page.evaluate(() => {
    window.fixture.bots = [];
    document.querySelector('h1').textContent = '雾屿灯塔 mini · 天气样式';
    document.querySelector('#moon-gallery').remove();
    const main = document.querySelector('main'); main.replaceChildren(); main.style.gridTemplateColumns = 'repeat(4, 230px)';
    const base = Date.UTC(2026,9,15) / 1000 - 8 * 3600;
    const types = [];
    for (const [description, hour, title] of [['晴',12,'晴 · 白天'],['多云',10,'多云'],['阴',10,'阴'],['雾',10,'雾'],['大风',10,'风'],['小雨',10,'小雨'],['暴雨',10,'大雨'],['雷阵雨',10,'雷电'],['雪',10,'雪'],['台风',10,'台风'],['高温晴热',10,'高温 · 蒸腾纹影'],['晴',22,'晴 · 夜晚']]) {
      const now = base + hour * 3600;
      const scene = window.fixture.weatherScene({available:true,city:'南通',description,date:'2026-10-15',fetched_at:now,sunrise:base+6*3600,sunset:base+18*3600}, now);
      types.push(scene.weather);
      const item = document.createElement('article'), host = document.createElement('div'), label = document.createElement('p');
      host.className = 'scene'; label.textContent = title; item.append(host, label); main.append(item);
      const bot = new window.fixture.BloubBot(host,{shape:'weather-lighthouse-mini',size:168,autoplay:false,draggable:false,reactOnClick:false});
      bot.setWeather(scene); bot.setActive(true); window.fixture.bots.push(bot);
    }
    const now = base + 10*3600;
    types.push(window.fixture.weatherScene({available:true,city:'南通',description:'台风',date:'2026-10-14',fetched_at:now-86400},now).weather);
    return types;
  });
  assert.deepEqual(types, ['clear','cloudy','overcast','fog','wind','rain','heavy-rain','storm','snow','typhoon','heat','clear','unknown'], 'weather follows fresh descriptions; strong wind is not a typhoon and stale data is not displayed');
  const heatForecasts = await page.evaluate(() => {
    const base=Date.UTC(2026,9,15)/1000-8*3600;
    return [[12,'晴',35],[12,'多云',34],[22,'晴',38],[12,'雷阵雨',38]].map(([hour,description,maximum])=>{
      const now=base+hour*3600;
      return window.fixture.weatherScene({available:true,city:'南通',description,maximum,date:'2026-10-15',fetched_at:now},now).weather;
    });
  });
  assert.deepEqual(heatForecasts,['heat','cloudy','clear','storm'],'daytime hot forecast produces heat; daytime maximum is not treated as nighttime temperature, nor does it hide thunderstorms');
  await page.setViewportSize({width:1004,height:820}); await page.waitForTimeout(1400);
  const heat = page.locator('[data-weather=heat] .wl-heat-wisp');
  const heatBefore = await heat.evaluateAll(nodes => nodes.map(node => getComputedStyle(node).transform));
  await page.waitForTimeout(250);
  assert.notDeepEqual(await heat.evaluateAll(nodes => nodes.map(node => getComputedStyle(node).transform)), heatBefore, 'heat wisps rise independently');
  const randomHeat = await heat.evaluateAll(nodes => nodes.map(node => [node.style.getPropertyValue('--wl-heat-x'),node.style.animationDuration,node.style.animationDelay]));
  assert.equal(new Set(randomHeat.map(value=>value[0])).size,4);
  assert.equal(new Set(randomHeat.map(value=>value[1])).size,4);
  const resetHeat = await heat.first().evaluate(node => {
    const before = node.style.getPropertyValue('--wl-heat-x');
    node.dispatchEvent(new AnimationEvent('animationiteration',{bubbles:true}));
    return before !== node.style.getPropertyValue('--wl-heat-x');
  });
  assert(resetHeat, 'heat locations change only at invisible cycle boundaries');
  const visibility = await page.locator('.weather-lighthouse-mini').evaluateAll(roots => roots.map(root => {
    const style = selector => getComputedStyle(root.querySelector(selector));
    return {weather:root.dataset.weather, sun:style('.wl-sun').visibility, clouds:style('.wl-clouds').opacity,
      heat:style('.wl-heat').opacity, cyclone:style('.wl-typhoon').opacity, rain:style('.wl-rain').opacity,
      heatRun:style('.wl-heat-wisp').animationPlayState, cycloneRun:style('.wl-cyclone').animationPlayState};
  }));
  assert(visibility.every(v => (v.heatRun === 'running') === (v.weather === 'heat') && (v.cycloneRun === 'running') === (v.weather === 'typhoon')), 'hidden weather effects do not keep animating');
  assert.equal(visibility[2].sun,'hidden'); assert.equal(visibility[2].clouds,'1');
  assert.equal(visibility[9].cyclone,'1'); assert.equal(visibility[9].rain,'1'); assert.equal(visibility[10].heat,'1');
  // Capture the lightning during its lit phase, not its deliberately quiet interval.
  await page.locator('[data-weather=storm] .wl-storm path').evaluate(node => {
    const animation = node.getAnimations()[0], timing = animation.effect.getTiming();
    animation.pause(); animation.currentTime = Number(timing.delay) + Number(timing.duration) * .84;
  });
  await page.screenshot({path:path.join(output,'weather-styles.png'),fullPage:true});
  await page.evaluate(() => window.fixture.bots.forEach(bot => bot.setActive(false)));
  assert.equal(await heat.first().evaluate(node=>getComputedStyle(node).animationPlayState),'paused');
  await page.evaluate(() => window.fixture.bots.forEach(bot => {bot.setReducedMotion(true);bot.setActive(true);}));
  assert.equal(await heat.first().evaluate(node=>getComputedStyle(node).animationPlayState),'paused');
  await page.evaluate(() => window.fixture.bots.forEach(bot => bot.destroy()));
  assert.deepEqual(errors, []);
  assert.deepEqual(requests, [], 'rendering adds no business or weather requests');
  console.log('PASS: mini lighting, lunar phases, solar colors, wave edge pixels, weather gallery, randomized rising heat wisps, pause/reduced motion and cleanup.');
} finally { await browser.close(); await server.close(); }
