import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { chromium } from 'playwright';

const base = 'http://127.0.0.1:19003';
const output = path.resolve('../../../output/playwright/assistant-fields');
await mkdir(output, { recursive: true });
const browser = await chromium.launch({ headless: true });
const fields = [
  { name: 'date', path: 'date', label: '演练日期', type: 'date', required: true },
  { name: 'time', path: 'time', label: '首步开始时间', type: 'time', required: true },
  { name: 'month', path: 'month', label: '统计月份', type: 'month', required: true },
  { name: 'actual', path: 'actual', label: '实际完成时间', type: 'datetime-local', step: 1, required: true },
  { name: 'progress', path: 'progress', label: '维修进度', type: 'number', min: 0, max: 100, step: 1, required: true },
  { name: 'project', path: 'project', label: '维修项目', type: 'select', options_source: 'repair_projects', required: true, options: [] },
  { name: 'devices', path: 'devices', label: 'CMDB设备', type: 'multiselect', maxItems: 1, options: [{ value: 'device-1', label: '5#柴油发电机 · 5.140008.1' }, { value: 'device-2', label: '6#柴油发电机 · 5.140008.2' }] },
  { name: 'rows', path: 'rows', label: '明细记录', type: 'array', minItems: 1, maxItems: 2,
    value: [{ content: '原步骤', enabled: false, actual: '2026-10-01T10:30:00', mode: 1, extra: { note: '原值' }, retained: '必须保留' }],
    item: { type: 'object', children: [
      { path: 'content', label: '步骤内容', type: 'text', required: true },
      { path: 'enabled', label: '启用提醒', type: 'checkbox', required: true },
      { path: 'actual', label: '步骤时间', type: 'datetime-local', step: 1, required: true },
      { path: 'mode', label: '选项模式', type: 'select', required: true, options: [{ value: 1, label: '模式一' }, { value: 2, label: '模式二' }] },
      { path: 'extra', label: '附加资料', type: 'textarea', value_format: 'json' },
    ] } },
];
try {
  for (const width of [1440, 390]) {
    const context = await browser.newContext({ viewport: { width, height: 1000 } });
    const health = await context.request.get(base + '/api/health');
    assert.equal((await health.json()).instance_id, 'isolated-lighthouse-stream');
    const page = await context.newPage(), errors = [], saved = [];
    page.on('pageerror', error => errors.push(error.message));
    let plan = { id: 'form-test', status: 'needs_input', title: '隔离维修填写', version: 1, fields, operations: [], results: [] };
    const conversation = () => ({ ok: true, data: { conversation_id: 'fixture', configured: true, enabled: true, busy: false,
      turns: [{ operation_id: 'calendar-fixture', question: '登记维修及设备', answer: '请补充信息。', status: 'completed', plan }] } });
    await page.route('**/api/assistant/conversation', async route => { await new Promise(resolve => setTimeout(resolve, 250)); return route.fulfill({ json: conversation() }); });
    let searches = 0;
    await page.route('**/api/assistant/plans/form-test/options?*', route => {
      const params = new URL(route.request().url()).searchParams;
      assert.equal(params.get('field'), 'project');
      assert.deepEqual(JSON.parse(params.get('selected')), searches ? ['rec-project'] : []);
      searches++;
      plan = { ...plan, version: plan.version + 1, fields: fields.map(field => field.name === 'project' ? { ...field, options: [
        { value: 'rec-project', label: 'A楼柴油发电机检修' }, ...(searches > 1 ? [{ value: 'rec-new', label: '另一检修项目' }] : []),
      ] } : field) };
      return route.fulfill({ json: { ok: true, data: plan } });
    });
    await page.route('**/api/assistant/plans/form-test', route => {
      assert.equal(route.request().method(), 'PATCH');
      saved.push(route.request().postDataJSON());
      plan = { ...plan, status: 'awaiting_confirmation', fields: [], version: plan.version + 1 };
      return route.fulfill({ json: { ok: true, data: plan } });
    });
    await page.goto(base);
    await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
    const panel = page.locator('.assistant-panel');
    const small = await panel.boundingBox();
    assert.equal(Math.round(small.width), Math.min(520, width - 48));
    const form = page.locator('.plan-form');
    await form.waitFor();
    const sizes = await panel.evaluate(el => new Promise(resolve => {
      const frames = [], started = performance.now();
      function sample() {
        const rect = el.getBoundingClientRect();
        frames.push({ width: rect.width, height: rect.height, right: rect.right, bottom: rect.bottom, left: rect.left, top: rect.top });
        if (performance.now() - started < 400) requestAnimationFrame(sample); else resolve(frames);
      }
      sample();
    }));
    assert.equal(Math.round(sizes.at(-1).width), Math.min(760, width - 48));
    assert.equal(Math.round(sizes.at(-1).height), 820);
    if (width > 800) assert(new Set(sizes.map(rect => Math.round(rect.width))).size > 2, 'expansion must have intermediate frames');
    for (const rect of sizes) assert(rect.left >= 10 && rect.top >= 10 && rect.right <= width - 10 && rect.bottom <= 990, 'resizing must stay in viewport');
    for (const [label, type, value] of [
      ['演练日期', 'date', '2026-10-01'], ['首步开始时间', 'time', '10:30'], ['统计月份', 'month', '2026-10'],
      ['实际完成时间', 'datetime-local', '2026-10-01T11:45:30'], ['维修进度', 'number', '58'],
    ]) {
      const input = form.getByLabel(label, { exact: true });
      assert.equal(await input.getAttribute('type'), type);
      await input.fill(value);
    }
    assert.equal(await form.getByLabel('维修进度', { exact: true }).getAttribute('max'), '100');
    await form.getByRole('button', { name: '查找', exact: true }).click();
    await form.getByLabel('维修项目', { exact: true }).selectOption('rec-project');
    await form.getByRole('searchbox', { name: '查找可选记录', exact: true }).fill('另一检修');
    await form.getByRole('button', { name: '查找', exact: true }).click();
    await form.getByRole('option', { name: '另一检修项目', exact: true }).waitFor({ state: 'attached' });
    assert.equal(await form.getByLabel('维修项目', { exact: true }).inputValue(), 'rec-project');
    const devices = form.getByRole('group', { name: 'CMDB设备', exact: true });
    await devices.getByRole('checkbox', { name: '5#柴油发电机 · 5.140008.1', exact: true }).check();
    assert.equal(await devices.getByRole('checkbox', { name: '6#柴油发电机 · 5.140008.2', exact: true }).isDisabled(), true);
    await devices.getByRole('button', { name: '清空CMDB设备', exact: true }).click();
    await devices.getByRole('checkbox', { name: '6#柴油发电机 · 5.140008.2', exact: true }).check();
    const records = form.getByRole('group', { name: '明细记录', exact: true });
    await records.getByLabel('步骤内容', { exact: true }).fill('已更正');
    await records.getByRole('button', { name: '添加明细记录', exact: true }).click();
    assert.equal(await records.getByRole('button', { name: '添加明细记录', exact: true }).isDisabled(), true);
    assert.equal(await records.getByLabel('启用提醒', { exact: true }).nth(1).isChecked(), false);
    await records.getByRole('button', { name: '删除第 2 项', exact: true }).click();
    assert.equal(await records.getByRole('button', { name: '删除第 1 项', exact: true }).isDisabled(), true);
    await records.getByLabel('选项模式', { exact: true }).selectOption({ label: '模式二' });
    const extra = records.getByLabel('附加资料', { exact: true });
    assert.match(await extra.inputValue(), /原值/);
    await extra.fill('{');
    assert.equal(await form.evaluate(el => el.checkValidity()), false);
    assert.equal(saved.length, 0);
    await extra.fill('{"note":"已核对"}');
    assert.equal(await form.evaluate(el => el.checkValidity()), true);
    await form.scrollIntoViewIfNeeded();
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
    await page.screenshot({ path: path.join(output, `fields-${width}.png`), fullPage: true });
    await form.getByRole('button', { name: '补充并继续', exact: true }).click();
    await page.getByRole('button', { name: '确认操作清单', exact: true }).waitFor();
    await page.waitForFunction(() => {
      const rect = document.querySelector('.assistant-panel').getBoundingClientRect();
      return Math.abs(rect.height - 680) < .1 && Math.abs(rect.width - Math.min(520, innerWidth - 48)) < .1;
    });
    const restored = await panel.boundingBox();
    assert.equal(Math.round(restored.width), Math.min(520, width - 48));
    assert.equal(saved.length, 1);
    assert.deepEqual(saved[0].values, { date: '2026-10-01', time: '10:30', month: '2026-10', actual: '2026-10-01T11:45:30', progress: 58, project: 'rec-project', devices: ['device-2'],
      rows: [{ content: '已更正', enabled: false, actual: '2026-10-01T10:30:00', mode: 2, extra: { note: '已核对' }, retained: '必须保留' }] });
    assert.equal(saved[0].version, 3);
    const largeRows = Array.from({ length: 61 }, (_, i) => ({ ...fields.at(-1).value[0], content: '记录' + i, extra: { note: '原值' + i }, retained: '保留' + i }));
    plan = { ...plan, status: 'needs_input', version: 10, fields: [{ ...fields.at(-1), minItems: 1, maxItems: 62, value: largeRows }] };
    await page.reload();
    await page.locator('.plan-form').waitFor();
    const large = page.getByRole('group', { name: '明细记录', exact: true });
    assert.equal(await large.getByLabel('步骤内容', { exact: true }).count(), 10);
    await large.getByLabel('步骤内容', { exact: true }).first().fill('首条已更正');
    await large.getByRole('button', { name: '下一页', exact: true }).click();
    assert.equal(await large.getByLabel('步骤内容', { exact: true }).first().inputValue(), '记录10');
    await large.getByLabel('步骤内容', { exact: true }).first().fill('第二页已更正');
    const largeExtra = large.getByLabel('附加资料', { exact: true }).first();
    await largeExtra.fill('{');
    await large.getByRole('button', { name: '下一页', exact: true }).click();
    assert.match(await large.getByRole('navigation').innerText(), /第 2 \/ 7 页/);
    assert.equal(await largeExtra.inputValue(), '{', 'invalid draft must survive blocked paging');
    await largeExtra.fill('{"note":"跨页已核对"}');
    for (let i = 0; i < 5; i++) await large.getByRole('button', { name: '下一页', exact: true }).click();
    assert.equal(await large.getByLabel('步骤内容', { exact: true }).count(), 1);
    assert.equal(await large.getByLabel('步骤内容', { exact: true }).inputValue(), '记录60');
    await large.getByLabel('步骤内容', { exact: true }).fill('末条已更正');
    await page.locator('.plan-form').getByRole('button', { name: '补充并继续', exact: true }).click();
    await page.getByRole('button', { name: '确认操作清单', exact: true }).waitFor();
    assert.equal(saved.length, 2);
    const expectedRows = largeRows.map(row => ({ ...row }));
    expectedRows[0].content = '首条已更正';
    expectedRows[10] = { ...expectedRows[10], content: '第二页已更正', extra: { note: '跨页已核对' } };
    expectedRows[60].content = '末条已更正';
    assert.deepEqual(saved[1], { version: 10, values: { rows: expectedRows } });
    assert.deepEqual(errors, []);
    await context.close();
    console.log(`Assistant fields ${width}px: controls, structured rows, JSON validation, retained values, adaptive resize and version OK`);
  }
} finally {
  await browser.close();
}
