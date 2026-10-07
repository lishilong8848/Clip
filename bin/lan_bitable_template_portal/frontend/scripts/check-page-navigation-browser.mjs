import assert from 'node:assert/strict';
import { mkdir, readFile, readdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const output = path.resolve(root, '../../..', 'output/playwright/navigation');
const eager = process.argv.includes('--eager');
await mkdir(output, { recursive: true });
const assets = path.join(root, 'dist/assets');
const moduleAssets = {};
for (const filename of await readdir(assets)) {
  if (!filename.endsWith('.js')) continue;
  const content = await readFile(path.join(assets, filename), 'utf8');
  for (const name of ['AdminTools', 'LighthouseStructuredField']) {
    if (new RegExp(`__name:["']${name}["']`).test(content)) moduleAssets[name] = filename;
  }
}
assert(moduleAssets.AdminTools && moduleAssets.LighthouseStructuredField);
const base = 'http://127.0.0.1:19003';
const browser = await chromium.launch({ headless: true });
try {
  const context = await browser.newContext({ viewport: { width: 1440, height: 900 }, reducedMotion: 'reduce' });
  assert.equal((await (await context.request.get(base + '/api/health')).json()).instance_id, 'isolated-lighthouse-stream');
  const page = await context.newPage(), errors = [], calls = [], scripts = new Set();
  page.on('pageerror', error => errors.push(error.message));
  page.on('console', message => { if (message.type() === 'warning' && /Transition|non-element root/.test(message.text())) errors.push(message.text()); });
  page.on('request', request => { if (request.resourceType() === 'script') scripts.add(new URL(request.url()).pathname); });
  let releaseRecords;
  const recordsGate = new Promise(resolve => { releaseRecords = resolve; });
  await page.route('**/api/**', async route => {
    const request = route.request(), url = new URL(request.url());
    assert.equal(request.method(), 'GET', 'navigation tests never write business data');
    if (url.pathname === '/api/health') return route.fallback();
    calls.push(url.pathname);
    let data = {};
    if (url.pathname === '/api/auth/status') data = { logged_in: true, user: { open_id: 'fixture-nav', name: '隔离测试', role: 'admin' }, scope_options: [{ value: 'D', label: 'D楼' }] };
    else if (url.pathname === '/api/repair-management/records') {
      await recordsGate;
      data = { records: [], fields: [], total: 0 };
    } else if (url.pathname === '/api/scope-overview') data = { scopes: {} };
    else if (url.pathname === '/api/handover-links') data = { links: {} };
    else if (url.pathname === '/api/assistant/conversation') data = { configured: true, enabled: true, turns: [], busy: false };
    return route.fulfill({ json: { ok: true, data } });
  });
  const cdp = await context.newCDPSession(page);
  await cdp.send('Emulation.setCPUThrottlingRate', { rate: 4 });
  await page.addInitScript(() => {
    window.navigationLongTasks = [];
    new PerformanceObserver(list => window.navigationLongTasks.push(...list.getEntries().map(entry => entry.duration))).observe({ type: 'longtask', buffered: true });
  });
  if (eager) await page.addInitScript(files => {
    // Reproduce the unnecessary eager imports without changing application code.
    window.eagerModules = Promise.all(files.map(file => import('/assets/' + file)));
  }, Object.values(moduleAssets));
  const start = Date.now();
  await page.goto(base + '/repair-management?scope=D');
  await page.locator('.record-list').waitFor();
  const firstPageMs = Date.now() - start;
  await page.getByRole('button', { name: '打开灯塔助手', exact: true }).waitFor();
  if (!eager) {
    assert.equal(scripts.has('/assets/' + moduleAssets.AdminTools), false, 'closed admin tools are not loaded');
    assert.equal(scripts.has('/assets/' + moduleAssets.LighthouseStructuredField), false, 'closed assistant does not load business forms');
    assert.equal(calls.includes('/api/scope-overview'), false, 'non-home pages do not fetch homepage statistics');
    assert.equal(calls.includes('/api/handover-links'), false);
    const spinner = page.locator('.record-list .loading-spinner');
    await spinner.waitFor();
    assert.equal(await spinner.evaluate(el => getComputedStyle(el).animationIterationCount), 'infinite');
    const before = await spinner.evaluate(el => getComputedStyle(el).transform);
    await page.waitForTimeout(220);
    assert.notEqual(await spinner.evaluate(el => getComputedStyle(el).transform), before, 'spinner moves under reduced-motion');
  }
  if (eager) await page.evaluate(() => window.eagerModules);
  await page.screenshot({ path: path.join(output, eager ? 'eager-loading.png' : 'loading.png'), fullPage: true });
  const loadedBytes = (await Promise.all([...scripts].filter(url => url.startsWith('/assets/')).map(async url => (await readFile(path.join(root, 'dist', url))).length))).reduce((sum, value) => sum + value, 0);
  releaseRecords();
  await page.waitForFunction(() => document.querySelector('.record-list')?.getAttribute('aria-busy') === 'false');
  const backStart = Date.now();
  await page.getByRole('button', { name: '返回', exact: true }).click();
  await page.locator('.home-shell').waitFor();
  const backMs = Date.now() - backStart;
  await page.waitForFunction(() => Boolean(document.querySelector('.home-shell')));
  if (!eager) {
    await page.waitForTimeout(150);
    assert(calls.includes('/api/scope-overview'), 'homepage still loads its own statistics');
    assert(calls.includes('/api/handover-links'));
  }
  const metrics = { firstPageMs, backMs, loadedBytes, scriptCount: scripts.size,
    adminLoaded: scripts.has('/assets/' + moduleAssets.AdminTools),
    formLoaded: scripts.has('/assets/' + moduleAssets.LighthouseStructuredField),
    longTasks: await page.evaluate(() => window.navigationLongTasks), errors };
  await writeFile(path.join(output, eager ? 'eager.json' : 'optimized.json'), JSON.stringify(metrics, null, 2));
  assert.deepEqual(errors, []);
  console.log(JSON.stringify({ mode: eager ? 'eager-import-comparison' : 'optimized', ...metrics }));
  await context.close();
} finally { await browser.close(); }
