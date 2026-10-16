import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { preview } from 'vite';
import { chromium } from 'playwright';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const output = path.resolve(root, '../../../output/playwright/home-counts');
await mkdir(output, { recursive: true });
const server = await preview({ root, logLevel: 'error', preview: { host: '127.0.0.1', port: 0 } });
const base = `http://127.0.0.1:${server.httpServer.address().port}`;
const browser = await chromium.launch({ headless: true });
const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
const errors = [], writes = [];
let admin = true, empty = false;
const counts = (n, events = 19, zero = empty) => ({ event_total: events, event_processing: zero ? 0 : 2,
  maintenance_pending: zero ? 0 : n * 2, maintenance_ongoing: zero ? 0 : n,
  change_pending: zero ? 0 : n, change_ongoing: zero ? 0 : n,
  repair_pending: zero ? 0 : n * 3, repair_ongoing: zero ? 0 : n,
  ongoing_title_count: zero ? 0 : n * 3 });
const scopeOptions = () => admin ? ['ALL', 'A', 'B', 'CAMPUS'].map(value => ({ value, label: value })) : [{ value: 'D', label: 'D楼' }];
page.on('pageerror', e => errors.push(e.message));
await page.route('**/api/**', route => {
  const url = new URL(route.request().url());
  if (route.request().method() !== 'GET') writes.push(url.pathname);
  const reply = data => route.fulfill({ json: { ok: true, data } });
  if (url.pathname === '/api/health') return route.fulfill({ json: { ok: true, service: 'clipflow_backend', instance_id: 'isolated-home-counts' } });
  if (url.pathname === '/api/auth/status') return reply({ logged_in: true,
    user: { name: '隔离测试', open_id: admin ? 'isolated-admin' : 'isolated-D', role: admin ? 'admin' : 'building', modules: ['event', 'maintenance', 'change', 'repair_management'] }, scope_options: scopeOptions() });
  if (url.pathname === '/api/scope-overview') return reply({ source_snapshot_ready: true, scope_options: scopeOptions(),
    scopes: admin ? { ALL: counts(1, empty ? 0 : 19), A: counts(2, 400, false), B: counts(2, 400, false), CAMPUS: counts(2, 400, false) } : { D: counts(4, 5) } });
  if (url.pathname === '/api/assistant/conversation') return reply({ enabled: false, turns: [] });
  return reply({});
});
const values = type => page.locator(`.module-${type} .module-metrics b`);
async function expectValues(type, expected) {
  await page.waitForFunction(({ type, expected }) => JSON.stringify([...document.querySelectorAll(`.module-${type} .module-metrics b`)].map(el => el.textContent.trim())) === JSON.stringify(expected), { type, expected });
  assert.deepEqual(await values(type).allTextContents(), expected);
}
try {
  await page.goto(base);
  await expectValues('event', ['19', '2']);
  await expectValues('maintenance', ['2', '1']);
  await expectValues('change', ['1', '1']);
  await expectValues('repair_management', ['3', '1']);
  await page.screenshot({ path: path.join(output, 'monthly-deduplicated.png'), fullPage: true });
  empty = true;
  await page.reload();
  await expectValues('event', ['0', '0']);
  await expectValues('maintenance', ['0', '0']);
  await expectValues('change', ['0', '0']);
  await expectValues('repair_management', ['0', '0']);
  await page.locator('.module-maintenance .module-card__main').click();
  await page.waitForFunction(() => document.querySelector('.scope-summary-strip')?.textContent.includes('0'));
  const totals = await page.locator('.scope-summary-strip strong').allTextContents();
  assert.deepEqual(totals.slice(0, 2), ['0', '0'], 'authoritative zero must not fall back to stale building totals');
  admin = false; empty = false;
  await page.goto(base);
  await expectValues('event', ['5', '2']);
  await expectValues('maintenance', ['8', '4']);
  await expectValues('change', ['4', '4']);
  await expectValues('repair_management', ['12', '4']);
  assert.deepEqual(errors, []);
  assert.deepEqual(writes, [], 'home statistics never mutate business data');
  console.log('[HomeCountsBrowser] OK: monthly counts, ALL deduplication, authoritative zero, D-only account');
} finally {
  await browser.close();
  await new Promise(resolve => server.httpServer.close(resolve));
}
