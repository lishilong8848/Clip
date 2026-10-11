import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../..');
const python = path.join(root, 'bin/.venv', process.platform === 'win32' ? 'Scripts/python.exe' : 'bin/python');
const code = `import sys,json
sys.path.insert(0,'bin')
from test_lighthouse_frontend_contracts import build_native_catalog
from lan_bitable_template_portal.lighthouse_api import _frontend_field
schema=build_native_catalog().get('POST /api/polling-sops')['schema']['body']
print(json.dumps(_frontend_field(schema['properties']['steps'],schema,name='steps'),ensure_ascii=False))`;
const control = JSON.parse(execFileSync(python, ['-c', code], { cwd: root, env: { ...process.env, PYTHONIOENCODING: 'utf-8', PYTHONWARNINGS: 'ignore' }, encoding: 'utf8' }));
const output = path.join(root, 'output/playwright/assistant-sop');
await mkdir(output, { recursive: true });
const base = 'http://127.0.0.1:19003';
const browser = await chromium.launch({ headless: true });
try {
  for (const width of [1440, 390]) {
    const context = await browser.newContext({ viewport: { width, height: 1000 } });
    assert.equal((await (await context.request.get(base + '/api/health')).json()).instance_id, 'isolated-lighthouse-stream');
    const page = await context.newPage(), errors = [], saved = [];
    page.on('pageerror', error => errors.push(error.message));
    const initial = Array.from({ length: 14 }, (_, index) => ({ step_id: 's' + (index + 1), content: '步骤' + (index + 1), operator_required: true, reviewer_required: true, photo_required: true, time_limit_seconds: 0, delay_reminder_minutes: 0, repeat_rules: [] }));
    const makePlan = value => ({ id: 'sop-test', status: 'needs_input', title: 'SOP配置', version: 1, fields: [{ ...control, name: 'steps', path: 'steps', label: 'SOP步骤', required: true, value }], operations: [], results: [] });
    let plan = makePlan(initial);
    await page.route('**/api/assistant/conversation', route => route.fulfill({ json: { ok: true, data: { conversation_id: 'sop-fixture', configured: true, enabled: true, busy: false, turns: [{ operation_id: 'sop-fixture', question: '配置两个步骤循环', answer: '请核对步骤。', status: 'completed', plan }] } } }));
    await page.route('**/api/assistant/plans/sop-test', route => {
      assert.equal(route.request().method(), 'PATCH');
      saved.push(route.request().postDataJSON());
      plan = { ...plan, status: 'awaiting_confirmation', fields: [], version: 2 };
      return route.fulfill({ json: { ok: true, data: plan } });
    });
    await page.goto(base);
    await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
    const top = page.getByRole('group', { name: 'SOP步骤', exact: true });
    await top.waitFor();
    const rows = () => top.locator(':scope > ol > li');
    const firstLoop = rows().nth(5).getByRole('group', { name: '本步后循环', exact: true });
    await firstLoop.getByRole('button', { name: '添加本步后循环', exact: true }).click();
    await firstLoop.getByLabel('起始步骤', { exact: true }).selectOption('s1');
    await firstLoop.getByLabel('结束步骤', { exact: true }).selectOption('s6');
    await firstLoop.getByLabel('总执行遍数', { exact: true }).fill('4');
    assert.equal(await firstLoop.getByLabel('结束步骤', { exact: true }).locator('option[value="s7"]').count(), 0);
    await top.getByRole('button', { name: '下一页', exact: true }).click();
    const secondLoop = rows().nth(3).getByRole('group', { name: '本步后循环', exact: true });
    await secondLoop.getByRole('button', { name: '添加本步后循环', exact: true }).click();
    await secondLoop.getByLabel('起始步骤', { exact: true }).selectOption('s11');
    await secondLoop.getByLabel('结束步骤', { exact: true }).selectOption('s14');
    await secondLoop.getByLabel('总执行遍数', { exact: true }).fill('4');
    assert.equal(await secondLoop.getByLabel('结束步骤', { exact: true }).locator('option[value="s1"]').count(), 0);
    const reminder = rows().nth(3).getByRole('checkbox', { name: '开启完成后延时提醒', exact: true });
    const minutes = rows().nth(3).getByLabel('延时分钟数', { exact: true });
    assert.equal(await minutes.isDisabled(), true);
    await reminder.check();
    await minutes.fill('12');
    await reminder.uncheck();
    await reminder.check();
    assert.equal(await minutes.inputValue(), '12');
    await top.getByRole('button', { name: '添加SOP步骤', exact: true }).click();
    const added = rows().last();
    await added.getByLabel('内容', { exact: true }).fill('新步骤');
    for (const label of ['操作人确认', '审核人确认', '需要照片']) assert.equal(await added.getByLabel(label, { exact: true }).isChecked(), true);
    assert.equal(await page.getByLabel('step id', { exact: true }).count(), 0);
    await added.scrollIntoViewIfNeeded();
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
    await page.screenshot({ path: path.join(output, `sop-${width}.png`), fullPage: true });
    await page.getByRole('button', { name: '补充并继续', exact: true }).click();
    await page.getByRole('button', { name: '确认操作清单', exact: true }).waitFor();
    const submitted = saved[0].values.steps;
    assert.equal(submitted.length, 15);
    assert.deepEqual([submitted[5].repeat_rules[0].from_step_id, submitted[5].repeat_rules[0].to_step_id, submitted[5].repeat_rules[0].count], ['s1', 's6', 4]);
    assert.deepEqual([submitted[13].repeat_rules[0].from_step_id, submitted[13].repeat_rules[0].to_step_id, submitted[13].repeat_rules[0].count], ['s11', 's14', 4]);
    assert.equal(submitted[13].delay_reminder_minutes, 12);
    assert(submitted[14].step_id && !initial.some(step => step.step_id === submitted[14].step_id));
    plan = makePlan(submitted);
    await page.reload();
    await top.waitFor();
    await rows().first().getByRole('button', { name: '删除第 1 项', exact: true }).click();
    await page.getByRole('button', { name: '补充并继续', exact: true }).click();
    await page.getByRole('button', { name: '确认操作清单', exact: true }).waitFor();
    const afterDelete = saved[1].values.steps;
    assert.equal(afterDelete.length, 14);
    assert.equal(afterDelete.find(step => step.step_id === 's6').repeat_rules.length, 0);
    assert.equal(afterDelete.find(step => step.step_id === 's14').repeat_rules[0].from_step_id, 's11');
    assert.deepEqual(errors, []);
    await context.close();
    console.log(`SOP ${width}px: native metadata, two independent loops, delay toggle, stable new IDs, delete cleanup and no overflow OK`);
  }
} finally {
  await browser.close();
}
