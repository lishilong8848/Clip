import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createServer } from 'vite';
import { chromium } from 'playwright';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const output = path.resolve(root, '../../..', 'output/playwright/home-broadcast');
await mkdir(output, { recursive: true });
const html = `<html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<style>*{box-sizing:border-box}body{margin:0;padding:20px;font-family:"Microsoft YaHei",sans-serif;max-width:1200px}</style></head>
<body><h1>实时动态</h1><div id="app"></div><script type="module">
import { createApp, h, ref } from 'vue';
import Ticker from '/src/components/HomeBroadcastTicker.vue';
import '/src/global.css';
const items=ref([]);
window.activations=[];
window.setItems=list=>{items.value=list};
const app=createApp({render:()=>h(Ticker,{items:items.value,summary:'测试汇总',onActivate:item=>window.activations.push(item.key)})});
app.mount('#app');
window.unmountApp=()=>app.unmount();
</script></body></html>`;
const server = await createServer({ root, logLevel: 'error',
  server: { host: '127.0.0.1', port: 0, hmr: false },
  plugins: [{ name: 'broadcast-check', configureServer(dev) {
    dev.middlewares.use('/__broadcast-check', async (_req, res) => {
      res.setHeader('Content-Type', 'text/html');
      res.end(await dev.transformIndexHtml('/__broadcast-check', html));
    });
  } }],
});
const items = [
  { key: 'a', label: '维保', text: '待处理现场设备维护工作', tone: 'ongoing', action: 'workbench' },
  { key: 'b', label: '检修', text: '需要复核运行方式并确认隔离点', tone: 'pending' },
  { key: 'c', label: '事件', text: '存在事件告警需要关注', tone: 'event', action: 'event' },
];
const longItem = { ...items[0], key: 'long', text: '检查供配电和空调设备运行情况，核对现场隔离点与风险控制措施。'.repeat(30) };
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const activeLabel = page => page.locator('.broadcast-item.is-active b').first().innerText();
const position = track => track.evaluate(el => new DOMMatrixReadOnly(getComputedStyle(el).transform).m41);
const visibleContent = page => page.evaluate(() => {
  const viewport = document.querySelector('.broadcast-viewport').getBoundingClientRect();
  return [...document.querySelectorAll('.broadcast-item span')].some(el => {
    const r = el.getBoundingClientRect();
    return Math.min(r.right, viewport.right) - Math.max(r.left, viewport.left) > 24;
  });
});
let browser;
try {
  await server.listen();
  const base = `http://127.0.0.1:${server.httpServer.address().port}`;
  browser = await chromium.launch({ headless: true });
  for (const width of [1440, 390]) {
    const context = await browser.newContext({ viewport: { width, height: 900 }, reducedMotion: 'no-preference' });
    const page = await context.newPage(), errors = [];
    page.on('pageerror', error => errors.push(error.message));
    await page.goto(base + '/__broadcast-check');
    await page.evaluate(list => window.setItems(list), items);
    await page.waitForFunction(() => document.querySelectorAll('.broadcast-item').length === 6);
    const track = page.locator('.broadcast-track');
    assert.equal(await page.locator('.broadcast-control').count(), 0, 'no play/pause control');
    assert.equal(await page.getByRole('button', { name: /暂停|播放/ }).count(), 0);
    assert.match(await track.evaluate(el => getComputedStyle(el).animationName), /^broadcast-scroll/);
    assert.equal(await track.evaluate(el => getComputedStyle(el).animationIterationCount), 'infinite');
    assert.equal(await track.evaluate(el => getComputedStyle(el).animationTimingFunction), 'linear');
    assert.equal(await visibleContent(page), true, 'content visible immediately');
    const before = await position(track);
    await sleep(450);
    assert(await position(track) < before - 1, 'actual right-to-left movement');
    await page.locator('.home-broadcast-card').hover();
    assert.equal(await track.evaluate(el => getComputedStyle(el).animationPlayState), 'running', 'hover must not freeze the ticker');
    const hovering = await position(track);
    await sleep(450);
    assert(await position(track) < hovering - 1, 'still moves while hovered');
    const primary = page.locator('.broadcast-item.interactive:not([aria-hidden="true"])');
    assert.equal(await primary.count(), 2);
    const itemBox = await primary.first().boundingBox();
    const viewportBox = await page.locator('.broadcast-viewport').boundingBox();
    await page.mouse.click(Math.max(viewportBox.x + 8, Math.min(itemBox.x + itemBox.width / 2, viewportBox.x + viewportBox.width - 8)), itemBox.y + itemBox.height / 2);
    assert.deepEqual(await page.evaluate(() => window.activations), ['a']);
    await page.locator('.broadcast-item.interactive[aria-hidden="true"]').first().evaluate(el => el.click());
    assert.deepEqual(await page.evaluate(() => window.activations), ['a'], 'duplicates cannot activate');
    await page.getByRole('heading').click();
    await page.mouse.move(0, 0);
    assert.equal(await track.evaluate(el => getComputedStyle(el).animationPlayState), 'running');
    for (const progress of [0, 0.999, 1]) {
      await track.evaluate((el, p) => {
        const animation = el.getAnimations()[0];
        animation.pause();
        animation.currentTime = Number(animation.effect.getTiming().duration) * p;
      }, progress);
      assert.equal(await visibleContent(page), true, 'loop boundary has no empty gap');
    }
    await track.evaluate(el => el.getAnimations()[0].play());
    await page.screenshot({ path: path.join(output, `normal-${width}.png`), fullPage: true });
    await page.evaluate(list => window.setItems(list), [longItem]);
    await page.waitForFunction(() => document.querySelectorAll('.broadcast-item').length === 2);
    await page.waitForFunction(() => parseFloat(getComputedStyle(document.querySelector('.broadcast-track')).animationDuration) > 90);
    const speed = await track.evaluate(el => (el.getBoundingClientRect().width / 2 + 11) / parseFloat(getComputedStyle(el).animationDuration));
    assert(speed > 0 && speed <= 18.5, `long content stays slow: ${speed}px/s`);
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
    await page.emulateMedia({ reducedMotion: 'reduce' });
    await page.evaluate(list => window.setItems(list), items);
    await page.waitForFunction(() => document.querySelector('.broadcast-track')?.classList.contains('is-reduced'));
    assert.equal(await track.evaluate(el => getComputedStyle(el).animationName), 'none');
    assert.equal(await page.locator('.broadcast-control').count(), 0, 'reduced-motion has no control either');
    if (width === 1440) {
      const initial = await activeLabel(page);
      await sleep(5400);
      assert.notEqual(await activeLabel(page), initial, 'reduced-motion still displays items automatically');
      const focused = page.locator('.broadcast-item.interactive[data-instance-key="c-primary"]');
      await focused.focus();
      assert.equal(await activeLabel(page), '事件');
      await sleep(5400);
      assert.equal(await activeLabel(page), '事件', 'keyboard focus stays readable');
      await page.getByRole('heading').click();
      await page.mouse.move(0, 0);
      await sleep(5400);
      assert.notEqual(await activeLabel(page), '事件', 'display resumes after blur');
    }
    assert.equal(await visibleContent(page), true);
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
    await page.screenshot({ path: path.join(output, `reduced-${width}.png`), fullPage: true });
    await page.evaluate(list => window.setItems(list), []);
    await page.waitForFunction(() => document.querySelector('.broadcast-track')?.classList.contains('is-static'));
    assert.equal(await track.evaluate(el => getComputedStyle(el).animationName), 'none');
    await page.evaluate(() => window.unmountApp());
    await page.emulateMedia({ reducedMotion: 'no-preference' });
    await sleep(100);
    assert.equal(await page.locator('.broadcast-track').count(), 0);
    assert.deepEqual(errors, []);
    await context.close();
    console.log(`Broadcast ${width}px: slow right-to-left movement, hover continues, no controls, seam, actions, reduced-motion and cleanup OK`);
  }
} finally {
  await browser?.close();
  await server.close();
}
