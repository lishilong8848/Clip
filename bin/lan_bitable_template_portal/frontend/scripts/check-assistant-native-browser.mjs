import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

// Only the loopback synthetic fixture is writable by this acceptance probe.
const base = 'http://127.0.0.1:19004';
const output = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../../output/playwright/assistant-native');
const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({ viewport: { width: 1440, height: 1000 } });
const errors = [];
let posted = 0;
async function state() {
  return (await context.request.get(base + '/fixture/native-state')).json();
}
async function open(page) {
  await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
  await page.getByRole('textbox', { name: '询问灯塔助手' }).waitFor();
}
async function send(page, text) {
  await page.locator('#assistant-question').fill(text);
  await page.getByRole('button', { name: '发送问题', exact: true }).click();
  assert.equal(await page.locator('#assistant-question').inputValue(), '');
}
async function complete(page, text) {
  await page.locator('.turn').last().locator('.assistant-rich-reply').filter({ hasText: text }).waitFor({ timeout: 120000 });
  await page.getByRole('button', { name: '停止生成', exact: true }).waitFor({ state: 'detached', timeout: 30000 });
  assert.equal(await page.locator('.turn').last().locator('table').count(), 1);
  assert.ok(await page.locator('.turn').last().locator('.sources').count());
  assert.equal(await page.locator('.turn').last().locator('.process').getAttribute('open'), null);
}
try {
  const health = await (await context.request.get(base + '/api/health')).json();
  assert.equal(health.instance_id, 'isolated-lighthouse-openclaw');
  const previous = (await (await context.request.get(base + '/api/assistant/conversation')).json()).data;
  if (previous.active_run_id) await context.request.post(base + `/api/assistant/runs/${previous.active_run_id}/cancel`, { headers: { origin: base }, data: {} });
  assert.equal((await context.request.delete(base + '/api/assistant/conversation', { headers: { origin: base } })).ok(), true);
  const baseline = await state();
  const page = await context.newPage();
  page.on('pageerror', error => errors.push(error.message));
  page.on('request', request => { if (request.method() === 'POST' && request.url().endsWith('/api/assistant/messages')) posted++; });
  await page.goto(base);
  await open(page);
  const accepted = page.waitForResponse(response => response.url().endsWith('/api/assistant/messages') && response.status() === 202);
  await send(page, '查询D楼维修项目列表');
  const run = (await (await accepted).json()).data.run_id;
  await page.reload();
  await page.locator('.assistant-panel').waitFor({ timeout: 10000 });
  await complete(page, 'D楼维修项目共');
  assert.equal(posted, 1, 'browser reload must resume the original run, never resend agent input');
  const first = await state();
  assert.deepEqual(first.business_calls.slice(baseline.business_calls.length), ['D']);
  assert.ok(first.model_calls.length >= baseline.model_calls.length + 2 && first.model_calls.every(call => call.tools === 17));

  const other = await browser.newContext();
  await other.addCookies([{ name: 'fixture_user', value: 'E', url: base }]);
  assert.equal((await other.request.get(base + `/api/assistant/runs/${run}/stream`)).status(), 404);
  await other.close();

  await send(page, '慢速查询D楼维修项目列表');
  await page.waitForFunction(async count => (await (await fetch('/fixture/native-state')).json()).model_calls.length >= count, first.model_calls.length + 1, { timeout: 30000 });
  await page.getByRole('button', { name: '停止生成', exact: true }).click();
  await page.getByRole('button', { name: '停止生成', exact: true }).waitFor({ state: 'detached', timeout: 15000 });
  await send(page, '查询D楼维修项目列表');
  await complete(page, 'D楼维修项目共');
  const finished = await state();
  assert.deepEqual(finished.business_calls.slice(baseline.business_calls.length), ['D', 'D'], 'stopped native run must not continue querying or repeat earlier business input');
  assert.equal(posted, 3);
  const box = await page.locator('.assistant-panel').boundingBox();
  assert.equal(Math.round(box.width), 620);
  assert.equal(Math.round(box.height), 700);
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth), false);
  await mkdir(output, { recursive: true });
  await page.screenshot({ path: path.join(output, 'pc-native.png') });
  assert.deepEqual(errors, []);
  console.log('[AssistantNativeBrowser] PASS: actual route -> OpenClaw Gateway -> typed plugin -> authorized native query -> UI source/table; reload, stop, retry, isolation.');
} finally {
  await context.close();
  await browser.close();
}
