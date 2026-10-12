import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdir, mkdtemp, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../..');
const output = path.join(root, 'output/playwright/work-order-compatibility');
const data = await mkdtemp(path.join(os.tmpdir(), 'clipflow-order-dom-'));
await mkdir(output, { recursive: true });
let browser;
try {
  const generated = spawnSync(path.join(root, 'bin/.venv/Scripts/python.exe'), ['-c', `
import sys,json
sys.path.insert(0,'bin')
from lan_bitable_template_portal.workbench_lite import render_polling_work_order_page,render_polling_work_order_steps_page
print(json.dumps([render_polling_work_order_page(),render_polling_work_order_steps_page()]))
`], { cwd: root, encoding: 'utf8', env: { ...process.env, CLIPFLOW_DATA_DIR: data, PYTHONIOENCODING: 'utf-8' } });
  assert.equal(generated.status, 0, generated.stderr);
  const html = JSON.parse(generated.stdout);
  browser = await chromium.launch({ headless: true });
  const errors = [];
  for (const [index, pathname] of ['/polling-work-order', '/polling-work-order/steps'].entries()) {
    const page = await browser.newPage({ viewport: { width: 1280, height: 900 } });
    page.on('pageerror', error => errors.push(error.message));
    await page.addInitScript(() => {
      delete Element.prototype.replaceChildren;
      delete Element.prototype.append;
    });
    let revision = 1;
    const state = () => ({ version: revision, role: 'operator', role_label: '操作人', assigned_person: { name: '隔离测试人员' },
      title: '隔离工单兼容性验证', sop_name: '合成SOP', work_type: 'maintenance', state: 'active', current_run_index: 1,
      work_orders: [{ run_index: 1, label: '执行第1遍', state: 'active', selectable: true, step_count: 2, completed_steps: revision - 1 }],
      steps: [{ step_index: revision, step_key: 'fixture-' + revision, run_index: 1, run_label: '第1遍', position: 'current',
        content: '仅页面模拟：不会执行设备操作或上传业务', operator_required: true, operator_confirmed: false,
        reviewer_required: false, reviewer_confirmed: false, photo_required: false, photos: [] }] });
    await page.route('**/*', route => {
      const url = new URL(route.request().url());
      if (url.pathname === pathname) return route.fulfill({ contentType: 'text/html', body: html[index] });
      if (url.pathname === '/api/polling-work-orders/session') return route.fulfill({ json: { ok: true, data: state() } });
      if (url.pathname === '/api/polling-work-orders/confirm') {
        revision++;
        return route.fulfill({ json: { ok: true, data: { session: state() } } });
      }
      return route.fulfill({ contentType: 'text/plain', body: '' });
    });
    await page.goto(`http://work-order.fixture${pathname}?token=fixture&run_index=1`);
    await page.getByRole('heading', { name: '隔离工单兼容性验证' }).waitFor();
    if (index === 0) {
      assert.equal(await page.locator('#overview .work-order').count(), 1);
      revision++;
      await page.getByText(/已完成 1\/2 步/).waitFor();
    } else {
      assert.equal(await page.locator('#steps .step').count(), 1);
      await page.getByRole('button', { name: /确认当前步骤/ }).click();
      await page.getByText('第 2 步', { exact: true }).waitFor();
      assert.equal(await page.locator('#steps .step').count(), 1, 'rerender must replace, not duplicate steps');
    }
    await page.screenshot({ path: path.join(output, index ? 'steps.png' : 'overview.png'), fullPage: true });
    await page.close();
  }
  assert.deepEqual(errors, []);
  console.log(JSON.stringify({ ok: true, screenshots: output }));
} finally {
  await browser?.close();
  await rm(data, { recursive: true, force: true });
}
