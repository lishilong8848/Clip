import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { chromium } from 'playwright';

const base = 'http://127.0.0.1:19003';
const output = path.resolve('../../../output/playwright/assistant-bot');
await mkdir(output, { recursive: true });
const browser = await chromium.launch({ headless: true });
const defaults = { color: 'encre', shape: 'cercle', expression: 'neutre', state: 'idle', size: 56, animated: true, follow: false, snap_back: true };
const failures = [];
async function box(page) { return page.locator('.assistant-launcher').boundingBox(); }
function same(actual, expected, label) {
  assert(actual && expected, label);
  assert(Math.abs(actual.x - expected.x) < 3 && Math.abs(actual.y - expected.y) < 3, `${label}: ${JSON.stringify(actual)} vs ${JSON.stringify(expected)}`);
}
async function drag(page, dx, dy) {
  const from = await box(page);
  await page.mouse.move(from.x + from.width / 2, from.y + from.height / 2);
  await page.mouse.down();
  await page.mouse.move(from.x + from.width / 2 + dx, from.y + from.height / 2 + dy, { steps: 15 });
  const during = await box(page);
  assert(Math.abs(during.x - (from.x + dx)) < 3 && Math.abs(during.y - (from.y + dy)) < 3, 'native drag must follow the pointer');
  await page.mouse.up();
  return from;
}
async function open(page) {
  if (!(await page.locator('.assistant-panel').count())) await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
  await page.locator('.assistant-panel').waitFor();
  await page.getByRole('textbox', { name: '询问灯塔助手' }).waitFor();
}
async function settings(page) {
  await open(page);
  await page.getByRole('button', { name: '图标设置', exact: true }).click();
  await page.getByRole('dialog', { name: '图标设置', exact: true }).waitFor();
}
async function mutations(page, duration = 700) {
  return page.locator('.assistant-launcher svg').evaluate((svg, duration) => new Promise(resolve => {
    let frames = 0, inserted = 0;
    const watcher = new MutationObserver(records => { frames++; inserted += records.filter(r => r.type === 'childList').reduce((n, r) => n + r.addedNodes.length, 0); });
    watcher.observe(svg, { subtree: true, attributes: true, childList: true });
    setTimeout(() => { watcher.disconnect(); resolve({ frames, inserted }); }, duration);
  }), duration);
}
try {
  const context = await browser.newContext({ viewport: { width: 1440, height: 1000 }, reducedMotion: 'no-preference' });
  await context.addCookies([{ name: 'fixture_user', value: 'D', url: base }]);
  const health = await context.request.get(base + '/api/health');
  assert.equal((await health.json()).instance_id, 'isolated-lighthouse-stream', 'never test writes against production');
  await context.request.put(base + '/api/assistant/appearance', { data: defaults, headers: { origin: base } });
  const page = await context.newPage();
  page.on('pageerror', err => failures.push(err.message));
  await page.goto(base);
  await page.locator('.assistant-launcher svg').waitFor();
  const original = await box(page);
  assert.equal(Math.round(original.x), 1440 - 24 - 56);
  assert.equal(Math.round(original.y), 1000 - 24 - 56);
  assert.equal(await page.locator('.assistant-launcher svg rect[fill]').getAttribute('fill'), '#0a0a0c');
  await page.waitForTimeout(1200);
  const pixels = await page.locator('.assistant-launcher svg').evaluate(async svg => {
    const src = URL.createObjectURL(new Blob([new XMLSerializer().serializeToString(svg)], { type: 'image/svg+xml' }));
    const img = new Image(); img.src = src; await img.decode();
    const canvas = document.createElement('canvas'); canvas.width = canvas.height = 56;
    const ctx = canvas.getContext('2d'); ctx.drawImage(img, 0, 0, 56, 56); URL.revokeObjectURL(src);
    const bytes = ctx.getImageData(0, 0, 56, 56).data;
    let dark = 0, light = 0;
    for (let i = 0; i < bytes.length; i += 4) if (bytes[i + 3] > 240) { if (bytes[i] < 40) dark++; if (bytes[i] > 240) light++; }
    return { dark, light };
  });
  assert(pixels.dark > 300 && pixels.light > 8, `black circular body and visible eye holes must render: ${JSON.stringify(pixels)}`);
  await page.screenshot({ path: path.join(output, 'desktop.png') });

  await drag(page, -260, -180);
  assert.equal(await page.locator('.assistant-panel').count(), 0, 'drag must not open chat');
  await page.waitForTimeout(1800);
  same(await box(page), original, 'default spring-back returns to lower-right');
  await open(page);
  same(await box(page), original, 'icon stays while panel opens');
  await page.getByRole('button', { name: '收起助手', exact: true }).click();
  await page.locator('.assistant-panel').waitFor({ state: 'detached' });
  same(await box(page), original, 'icon stays while panel closes');

  await settings(page);
  assert.equal(await page.getByRole('button', { name: '模型设置', exact: true }).count(), 1, 'formal account may configure models under the latest permission rule');
  for (const swatch of await page.locator('.colors .swatch').all()) {
    await swatch.click();
    const contrast = await page.locator('.lighthouse').evaluate(root => {
      const probe = document.createElement('span'); root.appendChild(probe);
      const canvas = document.createElement('canvas'); canvas.width = canvas.height = 1;
      const ctx = canvas.getContext('2d');
      function luminance(token) {
        probe.style.color = `var(${token})`; ctx.fillStyle = getComputedStyle(probe).color; ctx.fillRect(0, 0, 1, 1);
        const rgb = [...ctx.getImageData(0, 0, 1, 1).data].slice(0, 3).map(value => { const c = value / 255; return c <= .04045 ? c / 12.92 : ((c + .055) / 1.055) ** 2.4; });
        return rgb[0] * .2126 + rgb[1] * .7152 + rgb[2] * .0722;
      }
      const foreground = luminance('--lh-faint-muted');
      const values = ['--lh-surface', '--lh-surface-subtle', '--lh-surface-hover'].map(token => (foreground + .05) / (luminance(token) + .05));
      probe.remove(); return Math.min(...values);
    });
    assert(contrast >= 4.5, `all bot themes must keep readable text contrast, got ${contrast}`);
  }
  await page.getByRole('button', { name: '蓝色', exact: true }).click();
  assert.equal(await page.locator('.lighthouse').evaluate(el => getComputedStyle(el).getPropertyValue('--bot-color').trim()), '#3b93f0', 'chat theme follows the bot color preview');
  await page.getByLabel('形状', { exact: true }).selectOption('squircle');
  assert.equal(await page.getByLabel('表情', { exact: true }).count(), 0, 'expressions are automatic, not manually configured');
  assert.equal(await page.getByLabel('开启动画', { exact: true }).count(), 0, 'idle animations are automatic');
  await page.getByLabel('拖拽后返回右下角', { exact: true }).uncheck();
  await page.getByRole('button', { name: '保存', exact: true }).click();
  await page.getByRole('dialog', { name: '图标设置', exact: true }).waitFor({ state: 'detached' });
  assert.equal(await page.locator('.assistant-launcher svg rect[fill]').getAttribute('fill'), '#3b93f0');
  await drag(page, -350, -220);
  const retained = await box(page);
  await page.waitForTimeout(1800);
  same(await box(page), retained, 'snap-back off retains native drag position');
  const panelBox = await page.locator('.assistant-panel').boundingBox();
  const headerBox = await page.locator('.assistant-header').boundingBox();
  await page.mouse.move(headerBox.x + 95, headerBox.y + headerBox.height / 2);
  await page.mouse.down(); await page.mouse.move(headerBox.x - 90, headerBox.y - 80, { steps: 12 }); await page.mouse.up();
  assert((await page.locator('.assistant-panel').boundingBox()).x < panelBox.x - 50, 'panel keeps its independent original drag');
  same(await box(page), retained, 'panel drag must not move icon');
  await page.getByRole('button', { name: '收起灯塔助手', exact: true }).click();
  await page.locator('.assistant-panel').waitFor({ state: 'detached' });
  for (let i = 0; i < 3; i++) {
    await open(page);
    same(await box(page), retained, 'repeated open keeps icon');
    await page.getByRole('button', { name: '收起灯塔助手', exact: true }).click();
    await page.locator('.assistant-panel').waitFor({ state: 'detached' });
    same(await box(page), retained, 'repeated close keeps icon');
  }
  await page.reload(); await page.locator('.assistant-launcher svg').waitFor(); await page.waitForTimeout(350);
  same(await box(page), retained, 'retained position survives reload');
  assert.equal(await page.locator('.assistant-launcher svg rect[fill]').getAttribute('fill'), '#3b93f0');
  await context.addCookies([{ name: 'fixture_user', value: 'B', url: base }]);
  await page.reload(); await page.locator('.assistant-launcher svg').waitFor(); await page.waitForTimeout(350);
  assert.equal(await page.locator('.assistant-launcher svg rect[fill]').getAttribute('fill'), '#0a0a0c', 'another account has default appearance');
  same(await box(page), original, 'another account has its own anchor');
  await context.addCookies([{ name: 'fixture_user', value: 'D', url: base }]);
  await page.reload(); await page.locator('.assistant-launcher svg').waitFor(); await page.waitForTimeout(350);
  same(await box(page), retained, 'original account remains isolated');

  await settings(page);
  await page.getByRole('button', { name: '红色', exact: true }).click();
  await page.route('**/api/assistant/appearance', async route => {
    if (route.request().method() === 'PUT') { await new Promise(resolve => setTimeout(resolve, 500)); return route.fulfill({ status: 503, json: { ok: false, error: '隔离保存失败' } }); }
    return route.continue();
  });
  await page.getByRole('button', { name: '保存', exact: true }).click();
  assert(await page.getByRole('button', { name: '保存中…', exact: true }).isDisabled());
  await page.getByRole('alert').filter({ hasText: '隔离保存失败' }).waitFor();
  await page.getByRole('button', { name: '取消', exact: true }).click();
  await page.getByRole('dialog', { name: '图标设置', exact: true }).waitFor({ state: 'detached' });
  assert.equal(await page.locator('.assistant-launcher svg rect[fill]').getAttribute('fill'), '#3b93f0', 'cancelled/failed save restores the saved appearance');
  assert.equal(await page.locator('.lighthouse').evaluate(el => getComputedStyle(el).getPropertyValue('--bot-color').trim()), '#3b93f0', 'cancel also restores the saved chat theme');
  await page.unroute('**/api/assistant/appearance');
  await page.getByRole('textbox', { name: '询问灯塔助手' }).focus();
  await page.waitForFunction(() => document.querySelector('.assistant-launcher .lighthouse-bot')?.dataset.botMood === 'engaged');
  await page.waitForTimeout(700);
  const animated = await mutations(page, 1000);
  assert(animated.frames > 5 && animated.frames <= 33, `animation must be live but capped: ${JSON.stringify(animated)}`);
  assert.equal(animated.inserted, 0, 'stable eye paths are reused after expression morphing');
  await page.evaluate(() => { Object.defineProperty(document, 'hidden', { configurable: true, value: true }); document.dispatchEvent(new Event('visibilitychange')); });
  assert.equal((await mutations(page)).frames, 0, 'hidden page must stop rendering');
  await page.evaluate(() => { delete document.hidden; document.dispatchEvent(new Event('visibilitychange')); });
  await page.setViewportSize({ width: 390, height: 850 });
  await page.waitForTimeout(100);
  const mobile = await box(page);
  assert(mobile.x >= 12 && mobile.x + mobile.width <= 378 && mobile.y >= 12 && mobile.y + mobile.height <= 838, 'retained position stays inside narrow viewport');
  await settings(page);
  const modal = page.getByRole('dialog', { name: '图标设置', exact: true });
  assert(await modal.evaluate(el => el.scrollWidth <= el.clientWidth + 1), 'appearance dialog must not overflow');
  await page.waitForTimeout(230);
  assert(await modal.evaluate(el => { const r = el.getBoundingClientRect(); return el.contains(document.elementFromPoint(r.x + r.width / 2, r.y + r.height / 2)); }), 'settings modal must be above the chat panel');
  await page.screenshot({ path: path.join(output, 'mobile-settings.png') });
  await page.getByRole('button', { name: '恢复默认', exact: true }).click();
  await page.getByRole('button', { name: '保存', exact: true }).click();
  await modal.waitFor({ state: 'detached' });
  await page.waitForFunction(() => {
    const bounds = document.querySelector('.assistant-launcher')?.getBoundingClientRect();
    return bounds && Math.round(bounds.x) === innerWidth - 24 - 56 && Math.round(bounds.y) === innerHeight - 24 - 56;
  });
  const narrowAnchor = await box(page);
  assert.equal(Math.round(narrowAnchor.x), 390 - 24 - 56);
  assert.equal(Math.round(narrowAnchor.y), 850 - 24 - 56);
  const sendBox = await page.locator('.composer .send').boundingBox();
  assert(sendBox.y + sendBox.height < narrowAnchor.y, 'default icon must not obscure the mobile send control');
  await page.keyboard.press('Escape');
  await page.screenshot({ path: path.join(output, 'mobile.png') });
  await context.close();

  const legacy = await browser.newContext({ viewport: { width: 1440, height: 1000 } });
  await legacy.request.put(base + '/api/assistant/appearance', { data: defaults, headers: { origin: base } });
  const legacyPage = await legacy.newPage();
  legacyPage.on('pageerror', err => failures.push(err.message));
  await legacyPage.goto(base + '/legacy-widget-fixture');
  await legacyPage.locator('.assistant-launcher svg').waitFor();
  assert.equal(await legacyPage.locator('#clipflow-lighthouse-widget').count(), 1, 'legacy page mounts only one isolated assistant');
  assert.deepEqual(await legacyPage.locator('#legacy-business').evaluate(el => {
    const style = getComputedStyle(el); return { background: style.backgroundColor, padding: style.padding, color: style.color };
  }), { background: 'rgb(255, 243, 223)', padding: '17px', color: 'rgb(49, 68, 95)' }, 'widget styles must not replace legacy business styles');
  assert.equal(await legacyPage.evaluate(() => getComputedStyle(document.body).margin), '8px', 'main application global CSS is not loaded into legacy HTML');
  const legacyAnchor = await box(legacyPage);
  await legacyPage.locator('#drawer-open').click(); await legacyPage.waitForTimeout(500);
  assert((await box(legacyPage)).x + 56 < 760, 'legacy drawer moves icon outside');
  await legacyPage.evaluate(() => {
    const old = document.getElementById('legacy-drawer'), next = old.cloneNode(true);
    next.querySelector('section').style.width = '920px'; old.replaceWith(next);
  });
  await legacyPage.waitForTimeout(500);
  assert((await box(legacyPage)).x + 56 < 520, 'replacement AJAX drawer is re-observed');
  await legacyPage.getByRole('button', { name: 'Close drawer', exact: true }).click(); await legacyPage.waitForTimeout(500);
  same(await box(legacyPage), legacyAnchor, 'legacy drawer close restores icon');
  await open(legacyPage);
  await legacyPage.screenshot({ path: path.join(output, 'legacy-desktop.png') });
  await legacy.close();

  const reduced = await browser.newContext({ reducedMotion: 'reduce', viewport: { width: 1440, height: 1000 } });
  const reducedPage = await reduced.newPage();
  reducedPage.on('pageerror', err => failures.push(err.message));
  await reducedPage.goto(base); await reducedPage.locator('.assistant-launcher svg').waitFor(); await reducedPage.waitForTimeout(200);
  assert.equal(await reducedPage.locator('.lighthouse-bot').getAttribute('data-animation-active'), 'true', 'reduced motion must not disable eye tracking and feedback');
  await reducedPage.waitForTimeout(1200);
  const quiet = await mutations(reducedPage, 1000);
  assert(quiet.frames > 10 && quiet.frames <= 33, 'quiet feedback remains capped at 30fps');
  const eyes = () => reducedPage.locator('.assistant-launcher svg mask g').evaluate(g => {
    const matrices = [...g.children].map(p => new DOMMatrix(p.getAttribute('transform')));
    return { x: matrices.reduce((n,m)=>n+m.e,0)/matrices.length, y: matrices.reduce((n,m)=>n+m.f,0)/matrices.length };
  });
  await reducedPage.mouse.move(100, 150); await reducedPage.waitForTimeout(600); const left = await eyes();
  await reducedPage.mouse.move(600, 150); await reducedPage.waitForTimeout(600); const right = await eyes();
  assert(right.x - left.x > 5, `tracking must not saturate across the left half of the page: ${JSON.stringify({left,right})}`);
  await reducedPage.mouse.move(600, 700); await reducedPage.waitForTimeout(600); const down = await eyes();
  assert(down.y - right.y > 5, 'eyes track the vertical cursor position');
  await reducedPage.waitForTimeout(3500);const held = await eyes();
  assert(Math.abs(held.x-down.x)<3, 'stationary cursor remains the gaze target');
  await reduced.close();
  const large = await browser.newContext({viewport:{width:1440,height:1000}});
  await large.addCookies([{name:'fixture_user',value:'E',url:base}]);
  const largePage=await large.newPage();largePage.on('pageerror',err=>failures.push(err.message));
  await largePage.goto(base);await largePage.locator('.assistant-launcher svg').waitFor();
  await settings(largePage);
  await largePage.getByRole('slider',{name:'尺寸',exact:true}).press('End');
  await largePage.getByRole('button',{name:'保存',exact:true}).click();
  await largePage.getByRole('dialog',{name:'图标设置',exact:true}).waitFor({state:'detached'});
  await largePage.reload();await largePage.locator('.assistant-launcher svg').waitFor();
  await largePage.waitForTimeout(800);
  const largeBox=await box(largePage);assert.equal(largeBox.width,200);assert.equal(largeBox.height,200);
  assert(largeBox.x>=0&&largeBox.y>=0&&largeBox.x+200<=1440&&largeBox.y+200<=1000,'200px retained appearance stays inside viewport');
  const largePanel = await largePage.locator('.assistant-panel').boundingBox();
  assert(largePanel.x + largePanel.width <= largeBox.x || largePanel.x >= largeBox.x + largeBox.width || largePanel.y + largePanel.height <= largeBox.y, 'large icon must not cover the conversation controls');
  await largePage.screenshot({path:path.join(output,'desktop-200px.png')});
  await large.close();
  assert.deepEqual(failures, [], 'no frontend runtime errors');
  console.log('[pass] bot pixels, native drag/spring-back, persistent icon, account settings, automatic animation, reload, failed save, capped/hidden/reduced rendering and existing viewport bounds');
} finally { await browser.close(); }
