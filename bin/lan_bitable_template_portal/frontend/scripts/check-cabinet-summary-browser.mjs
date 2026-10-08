import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp, mkdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import net from 'node:net';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../..');
const output = path.join(root, 'output/playwright/cabinet-summary');
await mkdir(output, { recursive: true });
const temp = await mkdtemp(path.join(tmpdir(), 'cabinet-summary-'));
const socket = net.createServer();
await new Promise(resolve => socket.listen(0, '127.0.0.1', resolve));
const port = socket.address().port;
await new Promise(resolve => socket.close(resolve));
const server = spawn(path.join(root, 'bin/.venv/Scripts/python.exe'), [path.join(root, 'bin/tools/cabinet_ui_fixture.py'), String(port)], {
  cwd: root, windowsHide: true, env: { ...process.env, CLIPFLOW_DATA_DIR: temp }, stdio: ['ignore', 'pipe', 'pipe'],
});
const exit = once(server, 'exit');
let log = '', browser;
server.stdout.on('data', data => log += data);
server.stderr.on('data', data => log += data);
const base = `http://127.0.0.1:${port}`;
try {
  for (let n = 0; n < 180; n++) {
    try { if ((await fetch(base + '/api/health')).ok) break; } catch {}
    if (server.exitCode != null || n === 179) throw Error(log);
    await new Promise(resolve => setTimeout(resolve, 500));
  }
  browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  await page.route('**/*', route => {
    const url = new URL(route.request().url());
    if (url.origin !== base) return route.abort();
    if (url.pathname.startsWith('/api/drills')) return route.fulfill({ contentType: 'application/json', body: JSON.stringify({ ok: true, data: { drills: [], people: [] } }) });
    return route.continue();
  });
  const expected = { A: [1072,240,832], B: [1076,244,832], C: [998,156,842], D: [988,156,832], E: [1272,180,1092] };
  for (const scope of 'ABCDE') {
    await page.goto(`${base}/cabinet-power?scope=${scope}`);
    await page.locator('.room-totals').waitFor();
    const values = await page.locator('.room-totals td').allTextContents();
    assert.deepEqual([values[0], values[6], values[9]].map(Number), expected[scope]);
    if (scope === 'C') assert.deepEqual([values[1], values[4], values[7], values[8], values[10], values[11]].map(Number), [970,28,152,4,818,24]);
    if (scope === 'A') for (const room of ['203','303','403']) assert(await page.locator('tbody tr').filter({ hasText: room + ' 包间' }).count());
    if (scope === 'B') for (const room of ['216','247']) assert(await page.getByRole('button', { name: new RegExp(`B-${room}运营商机房`) }).count());
    await page.screenshot({ path: path.join(output, scope + '-summary.png'), fullPage: true });
    assert(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1));
  }
  await page.clock.install({ time: new Date('2026-10-08T10:00:00+08:00') });
  for (const query of ['mode=admin', ...[...'ABCDE'].map(scope => 'scope=' + scope)]) {
    await page.goto(`${base}/drill-management?${query}`);
    const month = page.locator('input[type=month]').first();
    await month.waitFor();
    assert.equal(await month.inputValue(), '2026-10');
    await month.fill('2026-12');
    await month.dispatchEvent('change');
    assert.equal(await month.inputValue(), '2026-12');
  }
  await page.screenshot({ path: path.join(output, 'drill-december.png'), fullPage: true });
  assert.deepEqual(errors, []);
  console.log('[CabinetSummaryBrowser] OK: five building totals, C baseline, added rooms and drill month defaults');
} finally {
  await browser?.close();
  if (server.exitCode == null) server.kill();
  await exit;
}
