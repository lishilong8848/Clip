import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import { chromium } from 'playwright';

const base = 'http://127.0.0.1:19003';
const browser = await chromium.launch({ headless: true });
try {
  const context = await browser.newContext({ viewport: { width: 1440, height: 1000 } });
  const health = await context.request.get(base + '/api/health');
  assert.equal((await health.json()).instance_id, 'isolated-lighthouse-stream');
  const failed = { id: 'failed-notice', title: '维保通告发送未完成', status: 'failed', version: 1, can_retry: true,
    fields: [], results: [{ ok: false, error: '远端结果未确认' }], error: '远端结果未确认' };
  const draft = { id: 'new-notice', title: '核对最新通告', status: 'needs_input', version: 1, results: [], fields: [
    { name: 'binding', path: 'binding', label: '计划通告关联', type: 'select', required: true, value: 'unbound',
      options: [{ value: 'bind', label: '绑定计划通告' }, { value: 'unbound', label: '独立通告' }] },
    { name: 'record', path: 'record', label: '计划事项', type: 'select', value: '', options_source: 'fixture',
      options: Array.from({ length: 20 }, (_, index) => ({ value: 'record-' + index, label: '计划事项 ' + index })) },
  ] };
  const turns = [
    { operation_id: 'failed-turn', question: '发送原通告', answer: '操作未完成', phase: 'completed', plan: failed },
    { operation_id: 'new-turn', question: '核对最新通告', answer: '', phase: 'completed', plan: draft },
  ];
  let retries = 0;
  const page = await context.newPage();
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  await page.route('**/api/assistant/**', async route => {
    const path = new URL(route.request().url()).pathname;
    let data = {};
    if (path.endsWith('/conversation')) data = { turns, phase: 'idle', scopes: ['D'], active_model: '隔离模型', can_manage_settings: false };
    else if (path.endsWith('/retry')) {
      retries++;
      assert.equal(path, '/api/assistant/plans/failed-notice/retry');
      assert.equal(route.request().postDataJSON().version, 1);
      await new Promise(resolve => setTimeout(resolve, 250));
      Object.assign(failed, { status: 'completed', can_retry: false, error: '' });
      data = failed;
    }
    await route.fulfill({ json: { ok: true, data } });
  });
  await page.goto(base);
  await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
  await page.getByRole('radio', { name: '独立通告', exact: true }).waitFor();
  assert.equal(await page.getByRole('radio', { name: '独立通告', exact: true }).isChecked(), true);
  await page.getByRole('radio', { name: '绑定计划通告', exact: true }).check();
  assert.equal(await page.getByRole('radio', { name: '绑定计划通告', exact: true }).isChecked(), true);
  const select = page.getByRole('combobox').filter({ hasText: '请选择' });
  await select.click();
  const search = page.getByPlaceholder('搜索计划事项');
  await search.fill('事项 17');
  assert.equal(await page.getByRole('option', { name: '计划事项 17', exact: true }).count(), 1);
  await page.getByRole('option', { name: '计划事项 17', exact: true }).click();
  assert.match(await page.getByRole('combobox').textContent(), /计划事项 17/);
  await page.getByRole('combobox').press('Enter');
  await page.getByRole('listbox').waitFor();
  await page.getByPlaceholder('搜索计划事项').press('Escape');
  await page.getByRole('listbox').waitFor({ state: 'hidden' });
  // Exercise the same control inside a native business modal/top layer.
  await page.evaluate(() => {
    const dialog = document.createElement('dialog');
    dialog.id = 'notice-controls-modal';
    dialog.textContent = 'Isolated business dialog';
    document.body.append(dialog);
    dialog.showModal();
  });
  await page.getByRole('combobox').click();
  await page.getByPlaceholder('搜索计划事项').fill('事项 19');
  await page.getByRole('option', { name: '计划事项 19', exact: true }).click();
  assert.match(await page.getByRole('combobox').textContent(), /计划事项 19/);
  await page.evaluate(() => {
    const dialog = document.getElementById('notice-controls-modal');
    dialog.close();
    dialog.remove();
  });
  await mkdir('../../../output/playwright/assistant-notice', { recursive: true });
  await page.screenshot({ path: '../../../output/playwright/assistant-notice/controls.png' });
  const retry = page.getByRole('button', { name: '重试原任务', exact: true });
  await retry.click();
  assert.equal(await retry.isDisabled(), true);
  await retry.waitFor({ state: 'hidden' });
  assert.equal(retries, 1);
  assert.equal(errors.length, 0, errors.join('\n'));
  console.log('Notice retry, compact radios, searchable dropdown and keyboard checks passed.');
} finally {
  await browser.close();
}
