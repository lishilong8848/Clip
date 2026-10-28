import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';
import { preview } from 'vite';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const output = path.resolve(root, '../../../output/playwright/repair-overview');
await mkdir(output, { recursive: true });
const server = await preview({ root, logLevel: 'error', preview: { host: '127.0.0.1', port: 0 } });
const origin = `http://127.0.0.1:${server.httpServer.address().port}`;
const browser = await chromium.launch({ headless: true });
const codes = ['A', 'B', 'C', 'D', 'E', 'H'];
const scopes = codes.map(value => ({ value, label: value + '楼' }));
const overviewPath = '/api/repair-management/overview';
let fail = false, incomplete = false, count = 4, reads = 0, delay = false, release;
const errors = [], writes = [];
const user = { open_id: 'repair-overview-fixture', name: '隔离验收', role: 'admin', level: 'L1',
  category: 'engineer', home_scope: 'E', scope_rules: codes, modules: ['repair_management', 'repair'] };
try {
  const context = await browser.newContext({ viewport: { width: 1440, height: 900 }, reducedMotion: 'reduce' });
  await context.route('**/*', async route => {
    const req = route.request(), url = new URL(req.url()), key = url.pathname;
    if (url.origin !== origin) return route.abort();
    if (!key.startsWith('/api/')) return route.continue();
    if (req.method() !== 'GET') { writes.push(key); return route.fulfill({ status: 503, json: { ok: false } }); }
    const ok = data => route.fulfill({ json: { ok: true, data } });
    if (key === '/api/health') return route.fulfill({ json: { ok: true, service: 'clipflow_backend', instance_id: 'repair-count-isolated' } });
    if (key === '/api/auth/status') return ok({ logged_in: true, user, scope_options: scopes });
    if (key === '/api/scope-overview') return ok({ source_snapshot_ready: true,
      scopes: { ALL: { repair_pending: 347, repair_ongoing: 0 }, E: { repair_pending: 99, repair_ongoing: 0 } } });
    if (key === overviewPath) {
      reads++;
      if (fail) return route.fulfill({ status: 503, json: { ok: false, error: '隔离读取失败' } });
      if (incomplete) return ok({});
      const result = { aggregate: { pending: count, in_progress: 3, year_total: 400, month_total: 10 },
        scopes: Object.fromEntries(codes.map(code => [code, { pending: code === 'D' ? 1 : code === 'E' ? count - 1 : 0,
          in_progress: code === 'D' ? 1 : code === 'E' ? 2 : 0 }])) };
      if (delay) { delay = false; await new Promise(resolve => { release = resolve; }); }
      return ok(result);
    }
    return ok({ items: [], records: [], stats: {}, summary: {} });
  });
  const page = await context.newPage();
  page.on('pageerror', error => errors.push(error.message));
  const card = page.locator('.module-repair_management');
  const values = card.locator('.module-metrics b');
  await page.goto(origin + '/?frontend=legacy');
  await card.waitFor();
  await values.first().filter({ hasText: /^4$/ }).waitFor();
  assert.deepEqual(await values.allTextContents(), ['4', '3'], 'homepage uses projects, not 347 pending notices');
  assert.match(await card.innerText(), /待检修/);
  assert.equal(reads, 1, 'one shared overview read on entry');
  await card.getByRole('button', { name: '进入检修单管理', exact: true }).click();
  const eScope = page.locator('.scope-card').filter({ has: page.locator('.scope-card__main strong', { hasText: /^E楼$/ }) });
  await eScope.waitFor();
  assert.match(await eScope.innerText(), /待检修 3/);
  assert.match(await eScope.innerText(), /进行中 2/);
  for (const code of ['A', 'B', 'C', 'H']) {
    const scope = page.locator('.scope-card').filter({ has: page.locator('.scope-card__main strong', { hasText: new RegExp('^' + code + '楼$') }) });
    assert.match(await scope.innerText(), /待检修 0/);
    assert.match(await scope.innerText(), /进行中 0/);
  }
  for (const [name, width, height] of [['iphone17pro', 402, 874], ['iphone17promax', 440, 956], ['macbookpro', 1440, 900]]) {
    await page.setViewportSize({ width, height });
    assert(!await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1), name + ' has no horizontal overflow');
    assert(await page.locator('.repair-metrics small').evaluateAll(nodes => nodes.every(el => el.getBoundingClientRect().width >= 68)), name + ' keeps summary labels readable');
    await page.screenshot({ path: path.join(output, name + '-scopes.png'), fullPage: true });
  }
  count = 2;
  await page.evaluate(() => dispatchEvent(new Event('clipflow-business-changed')));
  await page.locator('.scope-summary-strip article strong').first().filter({ hasText: /^2$/ }).waitFor();
  fail = true;
  await page.evaluate(() => dispatchEvent(new Event('clipflow-business-changed')));
  await page.getByRole('alert').filter({ hasText: /当前显示上次成功数据/ }).waitFor();
  assert.equal(await page.locator('.scope-summary-strip article strong').first().innerText(), '2');
  fail = false;
  await page.getByRole('button', { name: '重新读取', exact: true }).click();
  await page.getByRole('alert').waitFor({ state: 'detached' });

  fail = true;
  await page.goto(origin + '/?entry=repair_management&frontend=legacy');
  await page.getByRole('alert').filter({ hasText: /检修统计暂未读取/ }).waitFor();
  assert.equal(await page.locator('.scope-summary-strip article strong').first().innerText(), '待读取', 'failed initial read is not zero');
  fail = false; incomplete = true;
  await page.getByRole('button', { name: '重新读取', exact: true }).click();
  await page.waitForFunction(() => document.querySelector('.scope-summary-strip').getAttribute('aria-busy') === 'false');
  assert.equal(await page.locator('.scope-summary-strip article strong').first().innerText(), '待读取', 'partial responses do not become zero');
  incomplete = false;
  await page.getByRole('button', { name: '重新读取', exact: true }).click();
  await page.getByRole('alert').waitFor({ state: 'detached' });

  delay = true;
  await page.evaluate(() => dispatchEvent(new Event('clipflow-business-changed')));
  await page.waitForFunction(() => document.querySelector('.scope-summary-strip').getAttribute('aria-busy') === 'true');
  user.open_id = 'another-repair-user'; count = 1;
  await page.evaluate(() => dispatchEvent(new Event('auth-account-changed')));
  await page.locator('.scope-summary-strip article strong').first().filter({ hasText: /^1$/ }).waitFor();
  release();
  await page.waitForTimeout(100);
  assert.equal(await page.locator('.scope-summary-strip article strong').first().innerText(), '1', 'older account response cannot replace current counts');
  assert.deepEqual(writes, [], 'no business writes');
  assert.deepEqual(errors, []);
  console.log('PASS: repair homepage/scopes use native project counts; refresh, failure/retry, incomplete response, account isolation; phone and PC. All APIs isolated.');
} finally {
  release?.();
  await browser.close();
  await server.close();
}
