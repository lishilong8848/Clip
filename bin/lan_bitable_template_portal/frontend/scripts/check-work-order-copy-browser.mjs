import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdir, mkdtemp, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../..');
const data = await mkdtemp(path.join(os.tmpdir(), 'clipflow-copy-fixture-'));
const output = path.join(repo, 'output/playwright/work-order-copy');
await mkdir(output, { recursive: true });
let browser;
try {
  const generated = spawnSync(path.join(repo, 'bin/.venv/Scripts/python.exe'), ['-c', `
import sys
from unittest.mock import patch
sys.path.insert(0,'bin')
from lan_bitable_template_portal.workbench_lite import render_workbench_lite, _polling_work_order_status
from lan_bitable_template_portal.server import PortalRuntime
html=render_workbench_lite(payload={'records':[],'ongoing':[]}, session={'role':'admin'}, scope='A',work_type='maintenance')
with patch.object(PortalRuntime, 'polling_work_orders', side_effect=RuntimeError('isolated fixture')):
    panel=_polling_work_order_status({'polling_work_order_group_id':'fixture',
        'polling_work_order_operator_link':'http://orders.test/steps?token=fixture-operator&run=1',
        'polling_work_order_reviewer_link':'http://orders.test/steps?token=fixture-reviewer&run=1'}, 'maintenance')
print(html.replace('</body>', panel+'</body>'))
`], { cwd: repo, encoding: 'utf8', maxBuffer: 8 * 1024 * 1024,
    env: { ...process.env, CLIPFLOW_DATA_DIR: data, PYTHONIOENCODING: 'utf-8' } });
  assert.equal(generated.status, 0, generated.stderr);
  browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  await page.route('**/*', route => {
    const url = new URL(route.request().url());
    if (url.pathname === '/workbench-lite') return route.fulfill({ contentType: 'text/html; charset=utf-8', body: generated.stdout });
    if (url.pathname.startsWith('/api/')) return route.fulfill({ json: { ok: true, data: { items: [], records: [], ongoing: [] } } });
    return route.fulfill({ body: '', contentType: url.pathname.endsWith('.css') ? 'text/css' : 'application/javascript' });
  });
  await page.goto('http://clipflow-http.test/workbench-lite');
  assert.equal(await page.evaluate(() => isSecureContext), false);
  assert.equal(await page.evaluate(() => !!navigator.clipboard), false);
  await page.evaluate(() => {
    window.copied = [];
    document.addEventListener('copy', event => {
      window.copied.push(document.activeElement.value);
      event.preventDefault(); // Validate selection without replacing the user's OS clipboard.
    });
  });
  for (const role of ['操作人', '审核人']) await page.getByRole('button', { name: `复制${role}链接`, exact: true }).click();
  assert.deepEqual(await page.evaluate(() => window.copied), [
    'http://orders.test/steps?token=fixture-operator&run=1',
    'http://orders.test/steps?token=fixture-reviewer&run=1',
  ]);
  assert.equal(await page.locator('[data-polling-manual-copy]').count(), 0);
  await page.evaluate(() => {
    Object.defineProperty(navigator, 'clipboard', { configurable: true, value: { writeText: () => Promise.reject(new Error('denied')) } });
  });
  await page.getByRole('button', { name: '复制操作人链接', exact: true }).click();
  assert.equal((await page.evaluate(() => window.copied)).length, 3);
  await page.evaluate(() => { document.execCommand = () => false; });
  await page.getByRole('button', { name: '复制审核人链接', exact: true }).click();
  const manual = page.getByRole('textbox', { name: '工单链接，按 Ctrl+C 复制' });
  assert.equal(await manual.inputValue(), 'http://orders.test/steps?token=fixture-reviewer&run=1');
  assert.equal(await manual.evaluate(node => node.selectionEnd - node.selectionStart), (await manual.inputValue()).length);
  await page.locator('.polling-work-order-status').screenshot({ path: path.join(output, 'manual-fallback.png') });
  assert.deepEqual(errors, []);
  console.log('PASS: HTTP operator/reviewer copy, denied Clipboard API fallback, selected manual fallback; no real API writes.');
} finally {
  await browser?.close();
  assert.equal(path.dirname(path.resolve(data)), path.resolve(os.tmpdir()));
  assert(path.basename(data).startsWith('clipflow-copy-fixture-'));
  await rm(data, { recursive: true, force: true });
}
