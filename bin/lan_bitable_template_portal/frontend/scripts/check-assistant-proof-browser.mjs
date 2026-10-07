import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../..');
const python = path.join(root, 'bin/.venv', process.platform === 'win32' ? 'Scripts/python.exe' : 'bin/python');
const fixture = JSON.parse(execFileSync(python, ['-c', `import asyncio,json
from bin.test_lighthouse_cabinet_proof_workflows import CabinetProofWorkflowTests
async def main():
 test=CabinetProofWorkflowTests()
 test.setUpClass()
 try:
  await test.asyncSetUp()
  test.prepare()
  fixture={'plan':test.agent.public_plan(test.plan),'image_id':test.image_id,'row_id':test.row_ids[0]}
  test.prepare_correct()
  fixture['correct_plan']=test.agent.public_plan(test.plan)
  fixture['correct_loaded']=await test.load_directory()
  fixture['correct_rack']=next(row for row in test.field['rows'] if row['room']==test.new_rack['room'] and row['rack']==test.new_rack['rack'])
  print(json.dumps(fixture,ensure_ascii=False))
 finally:
  await test.asyncTearDown()
  test.doCleanups()
asyncio.run(main())
`], { cwd: root, encoding: 'utf8', maxBuffer: 8 * 1024 * 1024, env: { ...process.env, PYTHONIOENCODING: 'utf-8' } }));
const base = 'http://127.0.0.1:19003';
const output = path.join(root, 'output/playwright/assistant-proof-widget');
await mkdir(output, { recursive: true });
const png = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=', 'base64');
const browser = await chromium.launch({ headless: true });
try {
  for (const width of [1440, 390]) {
    const context = await browser.newContext({ viewport: { width, height: 1000 } });
    assert.equal((await (await context.request.get(base + '/api/health')).json()).instance_id, 'isolated-lighthouse-stream');
    await context.addCookies([{ name: 'fixture_user', value: 'E', url: base }]);
    const page = await context.newPage(), writes = [], errors = [];
    page.on('pageerror', error => errors.push(error.message));
    let plan = structuredClone(fixture.plan);
    await page.route('**/api/assistant/conversation', route => route.fulfill({ json: { ok: true, data: {
      conversation_id: 'proof-widget', configured: true, enabled: true, busy: false,
      turns: [{ operation_id: 'proof-widget', question: '把这张截图关联到机柜', status: 'completed', answer: '请核对下方填写项。', plan }],
    } } }));
    await page.route('**/api/cabinet-power/batches/**/images/**', route => {
      assert.equal(route.request().method(), 'GET');
      return route.fulfill({ contentType: 'image/png', body: png });
    });
    await page.route('**/api/assistant/plans/' + plan.id, route => {
      assert.equal(route.request().method(), 'PATCH');
      const body = route.request().postDataJSON();
      writes.push(body);
      const value = body.values['step0.proof'];
      plan = { ...plan, status: 'awaiting_confirmation', version: 2, fields: [], operations: [{
        api_id: 'POST /api/cabinet-power/batches/{batch_id}/images/{image_id}/apply', name: '关联截图',
        selected_labels: { selected_document: '确认图.png', row_id: 'E楼 101/E01' }, body: { attach: value.attach, fields: value.fields },
      }] };
      return route.fulfill({ json: { ok: true, data: plan } });
    });
    await page.goto(base);
    await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
    const form = page.locator('.plan-form'), submit = form.getByRole('button', { name: '补充并继续', exact: true });
    await form.locator('.lhp-proof').waitFor();
    await page.waitForFunction(() => {
      const form = document.querySelector('.plan-form').getBoundingClientRect();
      const thread = document.querySelector('.thread').getBoundingClientRect();
      return form.top >= thread.top - 1 && form.top < thread.top + 30;
    });
    assert.equal(await submit.isDisabled(), true, 'No target cabinet selected yet');
    await form.getByRole('combobox', { name: '识别候选', exact: true }).click();
    await page.getByRole('option').filter({ hasText: '第1项' }).click();
    assert.equal(await form.getByLabel('实际完成时间', { exact: true }).inputValue(), '2026-09-17T10:00');
    assert.equal(await form.getByLabel('操作类型', { exact: true }).inputValue(), '上正式电');
    await form.getByRole('button', { name: '查看原图', exact: true }).click();
    await page.getByRole('dialog', { name: '证明原图预览', exact: true }).waitFor();
    await page.keyboard.press('Escape');
    assert.equal(await page.locator('dialog[open]').count(), 0);
    await form.getByLabel('结果', { exact: true }).selectOption('失败');
    await submit.click();
    assert.equal(writes.length, 0, 'Failure reason must be supplied before preparing confirmation');
    await form.getByLabel('失败原因', { exact: true }).fill('隔离测试原因');
    await form.getByLabel('实际完成时间', { exact: true }).fill('2026-09-17T10:00:23');
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
    if (width > 800) assert((await page.locator('.assistant-panel').boundingBox()).width >= 740);
    await page.screenshot({ path: path.join(output, `proof-${width}.png`), fullPage: true });
    await submit.click();
    await page.getByRole('button', { name: '确认操作清单', exact: true }).waitFor();
    assert.equal(writes.length, 1);
    const value = writes[0].values['step0.proof'];
    assert.equal(value.image_id, fixture.image_id);
    assert.equal(value.row_id, fixture.row_id);
    assert.equal(value.candidate_index, 0);
    assert.equal(value.fields.actual, '2026-09-17 10:00:23');
    assert.equal(value.fields.failure_reason, '隔离测试原因');
    const preview = await page.locator('.plan-steps').innerText();
    assert(preview.includes('E楼 101/E01') && preview.includes('确认图.png'));
    assert(!preview.includes('row_id') && !preview.includes('image_id'));
    assert.deepEqual(errors, []);
    await context.close();
  }
  for (const width of [1440, 390]) {
    const context = await browser.newContext({ viewport: { width, height: 1000 } });
    await context.addCookies([{ name: 'fixture_user', value: 'E', url: base }]);
    const page = await context.newPage(), writes = [], errors = [], directories = [];
    page.on('pageerror', error => errors.push(error.message));
    let plan = structuredClone(fixture.correct_plan);
    await page.route('**/api/assistant/conversation', route => route.fulfill({ json: { ok: true, data: {
      conversation_id: 'correct-widget', configured: true, enabled: true, busy: false,
      turns: [{ operation_id: 'correct-widget', question: '补全这个图片批次漏识别的机柜', status: 'completed', answer: '请核对下方填写项。', plan }],
    } } }));
    await page.route('**/api/cabinet-power/**', route => {
      assert.equal(route.request().method(), 'GET', 'Before confirmation the native cabinet API must not write');
      return route.fulfill({ contentType: 'image/png', body: png });
    });
    await page.route(url => url.pathname.endsWith('/options'), async route => {
      const url = new URL(route.request().url());
      directories.push(url.searchParams.get('scope'));
      assert.equal(url.searchParams.get('scope'), 'E');
      await new Promise(resolve => setTimeout(resolve, 250));
      plan = structuredClone(fixture.correct_loaded);
      return route.fulfill({ json: { ok: true, data: plan } });
    });
    await page.route('**/api/assistant/plans/' + plan.id, route => {
      assert.equal(route.request().method(), 'PATCH');
      const body = route.request().postDataJSON(); writes.push(body);
      assert.equal(body.version, fixture.correct_loaded.version);
      const value = body.values['step0.proof'];
      plan = { ...plan, status: 'awaiting_confirmation', version: plan.version + 1, fields: [], operations: [{
        api_id: 'POST /api/cabinet-power/batches/{batch_id}/images/{image_id}/correct', name: '补全截图机柜',
        selected_labels: { selected_document: '确认图.png', row_id: fixture.correct_rack.label }, body: { fields: value.fields },
      }] };
      return route.fulfill({ json: { ok: true, data: plan } });
    });
    await page.goto(base);
    await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
    const form = page.locator('.plan-form'), submit = form.getByRole('button', { name: '补充并继续', exact: true });
    await form.getByRole('button', { name: '读取机柜目录', exact: true }).waitFor();
    await page.waitForFunction(() => !document.querySelector('[aria-label="读取机柜目录"]')?.disabled);
    assert.deepEqual(directories, ['E'], 'Opening the correction form loads the authorized directory once');
    assert.equal((await form.getByRole('combobox', { name: '目标机柜', exact: true }).innerText()).trim(), fixture.correct_rack.label);
    assert.equal(await form.getByLabel('实际完成时间', { exact: true }).inputValue(), '2026-09-17T10:00');
    assert.equal(await form.getByRole('checkbox', { name: '关联证明', exact: true }).count(), 0);
    await form.getByLabel('实际完成时间', { exact: true }).fill('2026-09-17T10:15:32');
    await form.getByRole('button', { name: '读取机柜目录', exact: true }).click();
    await page.waitForFunction(() => !document.querySelector('[aria-label="读取机柜目录"]')?.disabled);
    assert.equal(await form.getByLabel('实际完成时间', { exact: true }).inputValue(), '2026-09-17T10:15:32', 'Directory refresh preserves manual edits');
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
    await page.screenshot({ path: path.join(output, `correct-${width}.png`), fullPage: true });
    await submit.click();
    await page.getByRole('button', { name: '确认操作清单', exact: true }).waitFor();
    assert.equal(writes.length, 1);
    const value = writes[0].values['step0.proof'];
    assert.equal(value.row_id, fixture.correct_rack.row_id);
    assert.equal(value.scope, 'E');
    assert.equal(value.fields.actual, '2026-09-17 10:15:32');
    assert.equal(value.image_id, fixture.image_id);
    assert.equal('attach' in value, false);
    const preview = await page.locator('.plan-steps').innerText();
    assert(preview.includes(fixture.correct_rack.label) && preview.includes('尚不写入台账'));
    assert.deepEqual(errors, []);
    await context.close();
  }
  console.log('Native public proof plan -> assistant widget -> confirmation desktop/mobile OK');
} finally { await browser.close(); }
