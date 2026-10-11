import assert from 'node:assert/strict';
import http from 'node:http';
import { readFile, mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const dist = path.join(root, 'dist');
const output = path.resolve(root, '../../../output/playwright/bot-hit-area');
await mkdir(output, { recursive: true });
const server = http.createServer(async (req, res) => {
  try {
    const file = path.resolve(dist, '.' + (req.url === '/' ? '/assistant.html' : new URL(req.url, 'http://fixture').pathname));
    assert(file.startsWith(dist + path.sep));
    let bytes = await readFile(file);
    if (file.endsWith('assistant.html')) bytes = Buffer.from(bytes.toString().replace('id="clipflow-lighthouse-widget"', 'id="clipflow-lighthouse-widget" data-user-id="hit-fixture" data-user-name="Fixture"'));
    res.setHeader('Content-Type', file.endsWith('.js') ? 'application/javascript' : file.endsWith('.css') ? 'text/css' : 'text/html; charset=utf-8');
    res.end(bytes);
  } catch { res.writeHead(404); res.end(); }
});
await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
const base = 'http://127.0.0.1:' + server.address().port;
const browser = await chromium.launch({ headless: true });
let shape = 'cercle';
try {
  const page = await browser.newPage({ viewport: { width: 1440, height: 1000 }, reducedMotion: 'reduce' });
  await page.addInitScript(() => { localStorage.clear(); sessionStorage.clear(); });
  const errors = []; page.on('pageerror', error => errors.push(error.message));
  await page.route(base + '/api/**', async route => {
    const url = new URL(route.request().url());
    assert.equal(route.request().method(), 'GET', 'no real business writes');
    let data = {};
    if (url.pathname.endsWith('/appearance')) data = { color: 'encre', size: 200, shape, snap_back: false };
    if (url.pathname.endsWith('/conversation')) data = { enabled: true, configured: true, turns: [], busy: false };
    if (url.pathname.endsWith('/knowledge')) data = { total: 1, items: [{ id: 'fixture-doc', name: '测试公司手册.txt', status: 'ready' }], settings: { mode: 'local_faiss', ready: true, vectors: 12 } };
    await route.fulfill({ json: { ok: true, data } });
  });
  for (shape of ['cercle', 'galet', 'squircle', 'capsule', 'triangle', 'hexagone', 'nuage', 'goutte', 'swirl-pile', 'heart', 'star', 'flower', 'diamond', 'shield']) {
    await page.goto(base);
    await page.locator('.assistant-launcher [data-bot-hit]').waitFor();
    await page.waitForTimeout(650);
    await page.evaluate(() => {
      window.underClicks = 0;
      const under = document.createElement('button'); under.id = 'under-bot';
      under.textContent = '底层页面按钮';
      Object.assign(under.style, { position: 'fixed', inset: '0', width: '100vw', height: '100vh', zIndex: '1' });
      under.addEventListener('click', () => window.underClicks++);
      document.body.prepend(under);
    });
    const sampled = await page.evaluate(() => {
      const body = document.querySelector('.assistant-launcher [data-bot-hit]');
      const rect = document.querySelector('.assistant-launcher svg').getBoundingClientRect();
      const inverse = body.getScreenCTM().inverse();
      const outside = [], inside = [], wrong = [];
      for (let x = rect.left + 3; x < rect.right; x += 7) for (let y = rect.top + 3; y < rect.bottom; y += 7) {
        const expected = body.isPointInFill(new DOMPoint(x, y).matrixTransform(inverse));
        const actual = !!document.elementFromPoint(x, y)?.closest('.assistant-launcher');
        if (actual !== expected) wrong.push({ x, y, expected, actual });
        (expected ? inside : outside).push({ x, y });
      }
      return { wrong, inside, outside, rect: { x: rect.x, y: rect.y, width: rect.width, height: rect.height } };
    });
    assert.equal(sampled.wrong.length, 0, `${shape} must match actual animated SVG fill: ${JSON.stringify(sampled.wrong.slice(0, 3))}`);
    assert(sampled.inside.length > 20 && sampled.outside.length > 20, 'exercise both shape and transparent margins');
    await page.mouse.click(sampled.outside[0].x, sampled.outside[0].y);
    assert.equal(await page.evaluate(() => window.underClicks), 1, 'outside click reaches page below');
    assert.equal(await page.locator('.assistant-panel').count(), 0, 'outside must not open chat');
    const center = { x: sampled.rect.x + 100, y: sampled.rect.y + 100 };
    await page.mouse.click(center.x, center.y);
    await page.locator('.assistant-panel').waitFor();
    await page.getByRole('button', { name: '收起助手', exact: true }).click();
    await page.locator('.assistant-panel').waitFor({ state: 'detached' });
    await page.mouse.move(center.x, center.y); await page.mouse.down();
    await page.mouse.move(center.x - 140, center.y - 90, { steps: 12 }); await page.mouse.up();
    await page.waitForTimeout(250);
    const moved = await page.locator('.assistant-launcher').boundingBox();
    assert(moved.x < sampled.rect.x - 80, `${shape}: visible body still drags (${moved.x} vs ${sampled.rect.x})`);
    assert.equal(await page.locator('.assistant-panel').count(), 0, 'drag must not open chat');
    await page.screenshot({ path: path.join(output, shape + '.png') });
  }
  await page.getByRole('button', { name: '打开灯塔助手', exact: true }).focus();
  await page.keyboard.press('Enter');
  await page.locator('.assistant-panel').waitFor();
  const collapse = page.getByRole('button', { name: '收起助手', exact: true });
  assert.equal((await collapse.boundingBox()).width, 42, 'larger close hit target');
  assert.equal((await collapse.innerText()).trim(), '', 'close remains an X without text');
  const sidebar = page.getByRole('button', { name: '侧边栏（通告待办 / 知识库）', exact: true });
  if (await sidebar.getAttribute('aria-expanded') !== 'true') await sidebar.click();
  await page.getByRole('tab', { name: '知识库', exact: true }).click();
  await page.getByText('测试公司手册.txt', { exact: true }).waitFor();
  assert.match(await page.getByRole('region', { name: '知识库概况', exact: true }).innerText(), /1 份文档[\s\S]*12 个索引片段/);
  await page.screenshot({ path: path.join(output, 'knowledge-overview.png') });
  await page.getByRole('button', { name: '图标设置', exact: true }).click();
  const dialog = page.getByRole('dialog', { name: '图标设置' });
  await dialog.waitFor();
  assert.equal(await dialog.locator('select option').count(), 14);
  await dialog.getByRole('button', { name: '蓝色', exact: true }).click();
  await page.waitForTimeout(100);
  const blue = await dialog.evaluate(el => getComputedStyle(el).backgroundColor);
  assert.equal(blue, await page.locator('.assistant-panel').evaluate(el => getComputedStyle(el).backgroundColor));
  await dialog.getByRole('button', { name: '红色', exact: true }).click();
  await page.waitForTimeout(100);
  assert.notEqual(blue, await dialog.evaluate(el => getComputedStyle(el).backgroundColor));
  await dialog.getByLabel('形状', { exact: true }).selectOption('swirl-pile');
  await page.waitForTimeout(550);
  await page.screenshot({ path: path.join(output, 'settings-themed.png') });
  await dialog.screenshot({ path: path.join(output, 'settings-dialog.png') });
  await dialog.getByRole('button', { name: '取消', exact: true }).click();
  assert.deepEqual(errors, []);
  console.log(JSON.stringify({ ok: true, shapes: 14, screenshots: output, checks: 'SVG shape hit-testing, transparent pass-through, open/close, drag, keyboard' }));
} finally { await browser.close(); await new Promise(resolve => server.close(resolve)); }
