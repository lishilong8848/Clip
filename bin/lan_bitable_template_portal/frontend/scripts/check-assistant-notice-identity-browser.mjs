import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../..');
const fixture = spawnSync(path.join(root, 'bin/.venv/Scripts/python.exe'), ['-c', `
import sys,json,asyncio
sys.path.insert(0,'bin')
from test_lighthouse_notice_identity_workflows import NoticeIdentityWorkflowTests,ACTOR
async def main():
    fixture=NoticeIdentityWorkflowTests();fixture.setUp()
    try:
        plans={}
        for mode in ('planned','target','source'):
            plan=fixture.prepare(mode)
            plans[mode]={'initial':fixture.agent.public_plan(plan,ACTOR),'loaded':await fixture.load(plan)}
        print(json.dumps(plans,ensure_ascii=False))
    finally: fixture.doCleanups()
asyncio.run(main())
`], { cwd: root, encoding: 'utf8', env: { ...process.env, PYTHONIOENCODING: 'utf-8', PYTHONWARNINGS: 'ignore' } });
assert.equal(fixture.status, 0, fixture.stderr);
const plans = JSON.parse(fixture.stdout), base = 'http://127.0.0.1:19003';
const output = path.join(root, 'output/playwright/assistant-notice-identity');
await mkdir(output, { recursive: true });
const browser = await chromium.launch({ headless: true });
try {
  for (const width of [1440, 390]) {
    const context = await browser.newContext({ viewport: { width, height: 1000 } });
    assert.equal((await (await context.request.get(base + '/api/health')).json()).instance_id, 'isolated-lighthouse-stream');
    const page = await context.newPage(), errors = [], saves = [], lookups = [];
    let mode, plan, writes = 0, failLookup = false;
    page.on('pageerror', error => errors.push(error.message));
    await page.route('**/api/notice-identity/bind', route => { writes++; return route.abort(); });
    await page.route('**/api/workbench-actions', route => { writes++; return route.abort(); });
    await page.route('**/api/assistant/conversation', route => route.fulfill({ json: { ok: true, data: {
      conversation_id: 'identity-fixture', configured: true, enabled: true, busy: false,
      turns: [{ operation_id: 'identity-fixture', question: '绑定通告关系', answer: '请选择关联记录。', status: 'completed', plan }],
    } } }));
    await page.route('**/api/assistant/plans/**', async route => {
      const url = new URL(route.request().url());
      if (url.pathname.endsWith('/options')) {
        lookups.push(Object.fromEntries(url.searchParams));
        if (failLookup) return route.fulfill({ status: 503, json: { ok: false, error: '隔离候选读取失败' } });
        const version = plan.version + 1;
        plan = structuredClone(plans[mode].loaded);
        plan.version = version;
        return route.fulfill({ json: { ok: true, data: plan } });
      }
      assert.equal(route.request().method(), 'PATCH');
      const body = route.request().postDataJSON();
      saves.push(body);
      const selectedField = plan.fields.find(field => field.native_notice_identity && field.options_source);
      const selected = body.values[selectedField.name];
      const label = selectedField.options.find(option => option.value === selected)?.label;
      plan = { ...plan, fields: [], version: plan.version + 1, status: 'awaiting_confirmation', can_edit: true,
        operations: plan.operations.map(op => ({ ...op, selected_labels: { original_notice: 'A楼配电维护', [selectedField.path]: label } })) };
      return route.fulfill({ json: { ok: true, data: plan } });
    });
    for (const current of ['planned', 'target', 'source']) {
      mode = current;
      plan = structuredClone(plans[mode].initial);
      await page.goto(base);
      await page.locator('.assistant-launcher, .assistant-panel').waitFor();
      const open = page.getByRole('button', { name: '打开灯塔助手', exact: true });
      if (await open.isVisible()) await open.click();
      const form = page.locator('.plan-form');
      await form.waitFor();
      const selector = plans[mode].initial.fields.find(field => field.native_notice_identity && field.options_source);
      const choice = form.getByLabel(selector.label, { exact: true });
      assert.equal(await choice.getAttribute('type'), null);
      assert.equal(await choice.inputValue(), '');
      assert.equal(await form.getByRole('button', { name: '查找', exact: true }).count(), 1, 'month must not have a remote search');
      if (mode === 'planned') {
        failLookup = true;
        await form.getByRole('button', { name: '查找', exact: true }).click();
        await page.getByText('隔离候选读取失败', { exact: true }).waitFor();
        failLookup = false;
      }
      await form.getByRole('button', { name: '查找', exact: true }).click();
      await choice.selectOption(mode === 'source' ? 'rec-source-new' : 'rec-target-new');
      assert.equal(await page.getByText('隔离候选读取失败', { exact: true }).count(), 0, 'successful retry clears the obsolete lookup error');
      assert.equal(await choice.getAttribute('required'), '', 'record selection is visibly required');
      if (mode === 'source') {
        const monthField = plan.fields.find(field => field.path === 'source_month');
        const month = form.getByLabel(monthField.label, { exact: true });
        assert.equal(await month.inputValue(), '9月');
        await month.selectOption('10月');
        assert.equal(await choice.inputValue(), '', 'changing source month clears stale selection');
        assert.equal(await choice.locator('option').count(), 1);
        await form.getByRole('button', { name: '查找', exact: true }).click();
        assert.equal(lookups.at(-1).month, '10月');
        assert.equal(lookups.at(-1).selected, '[]');
        await choice.selectOption('rec-source-new');
      } else {
        assert.equal(await choice.locator('option[value="rec-ended"]').count(), mode === 'planned' ? 1 : 0);
      }
      assert.equal(await choice.locator('option[value="rec-other-building"]').count(), 0);
      assert.equal(await form.evaluate(el => el.checkValidity()), true);
      assert.equal(await form.evaluate(el => el.scrollWidth <= el.clientWidth + 1), true);
      assert.doesNotMatch(await form.innerText(), /rec-target|rec-source|rec-plan|active-a|_anchor|\$query/);
      assert.match(await form.innerText(), /当前通告[:：]/);
      await page.screenshot({ path: path.join(output, `${mode}-form-${width}.png`), fullPage: true });
      await form.getByRole('button', { name: '补充并继续', exact: true }).click();
      await page.getByRole('button', { name: '确认操作清单', exact: true }).waitFor();
      await page.getByText('仅保存关联关系，不发送通告', { exact: true }).waitFor();
      assert.equal(saves.at(-1).values[selector.name], mode === 'source' ? 'rec-source-new' : 'rec-target-new');
      assert.doesNotMatch(await page.locator('.operation-plan').innerText(), /rec-target|rec-source|rec-plan|active-a|source_binding_only/);
      assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
      await page.screenshot({ path: path.join(output, `${mode}-${width}.png`), fullPage: true });
    }
    assert.equal(writes, 0);
    assert.deepEqual(errors, []);
    await context.close();
    console.log(`Notice identity ${width}px: three modes/native selects/month reset/retry/review/no raw IDs/no writes/no overflow OK`);
  }
} finally { await browser.close(); }
