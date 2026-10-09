import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';
import { preview } from 'vite';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../..');
const result = spawnSync(path.join(root, 'bin/.venv/Scripts/python.exe'), ['-c', `
import asyncio,json
from bin.test_lighthouse_notice_sop import NoticeSopTests
from bin.test_lighthouse_notice_workflows import ACTOR
async def main():
    test=NoticeSopTests();test.setUp();plans={}
    try:
        for work in ('maintenance','polling','adjust'):
            plan=test.prepare(work)
            plans[work]={'initial':test.agent.public_plan(plan,ACTOR),'loaded':await test.load(plan)}
        print(json.dumps(plans,ensure_ascii=False))
    finally: test.doCleanups()
asyncio.run(main())
`], { cwd: root, encoding: 'utf8', env: { ...process.env, PYTHONIOENCODING: 'utf-8', PYTHONWARNINGS: 'ignore' } });
assert.equal(result.status, 0, result.stderr);
const plans = JSON.parse(result.stdout);
const frontend = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const server = await preview({ root: frontend, logLevel: 'error', preview: { host: '127.0.0.1', port: 0, strictPort: false } });
const base = `http://127.0.0.1:${server.httpServer.address().port}/assistant.html`;
const output = path.join(root, 'output/playwright/assistant-notice-sop');
await mkdir(output, { recursive: true });
const browser = await chromium.launch({ headless: true });
try {
  for (const width of [1440, 1024]) {
    const context = await browser.newContext({ viewport: { width, height: 1000 } });
    const page = await context.newPage(), errors = [], saves = [];
    page.on('pageerror', err => errors.push(err.message));
    let display, loaded, requests = 0, failFirst = false;
    await page.route('**/assistant.html*', async route => { const response = await route.fetch(); await route.fulfill({ response, body: (await response.text()).replace('id="clipflow-lighthouse-widget"', 'id="clipflow-lighthouse-widget" data-user-id="sop-fixture" data-user-name="隔离测试"') }); });
    await page.route('**/api/**', route => route.fulfill({ json: { ok: true, data: {} } }));
    await page.route('**/api/assistant/conversation', route => route.fulfill({ json: { ok: true, data: {
      conversation_id: 'sop-form', configured: true, enabled: true, busy: false,
      turns: [{ operation_id: 'sop-form', question: '填写通告及SOP', answer: '请核对。', status: 'completed', plan: display }],
    } } }));
    await page.route('**/api/assistant/plans/*/options?*', async route => {
      assert.equal(new URL(route.request().url()).searchParams.get('scope'), 'A');
      requests++;
      await new Promise(resolve => setTimeout(resolve, 250));
      if (failFirst) { failFirst = false; return route.fulfill({ status: 503, json: { ok: false, error: '工单目录暂不可用' } }); }
      loaded.version = display.version + 1;
      display = structuredClone(loaded);
      return route.fulfill({ json: { ok: true, data: display } });
    });
    await page.route('**/api/assistant/plans/*', route => {
      assert.equal(route.request().method(), 'PATCH');
      saves.push(route.request().postDataJSON());
      display = { ...display, fields: [], version: display.version + 1, status: 'awaiting_confirmation' };
      return route.fulfill({ json: { ok: true, data: display } });
    });
    for (const [work, data] of Object.entries(plans)) {
      display = structuredClone(data.initial); loaded = structuredClone(data.loaded); requests = 0; failFirst = work === 'maintenance';
      await page.goto(base);
      await page.locator('.assistant-launcher').waitFor();
      const launcher = page.getByRole('button', { name: '打开灯塔助手', exact: true });
      if (await launcher.isVisible()) await launcher.click();
      if (work === 'maintenance') {
        await page.getByRole('button', { name: '重试读取工单', exact: true }).click();
        await page.getByText('正在读取工单和人员…', { exact: true }).waitFor();
      }
      const sop = page.locator('select[aria-label="工单SOP"]');
      await sop.waitFor();
      const dates = page.locator('.plan-form input[type="datetime-local"]');
      assert.equal(await dates.count(), 2, 'notice start/end use datetime controls');
      for (const input of await dates.all()) assert.ok(await input.inputValue(), 'prefilled time remains visible');
      assert.equal(await page.locator('.plan-form .date-picker-button').count(), 2, 'calendar actions remain visible in conversation');
      const control = loaded.fields.find(field => field.native_notice_sop);
      await sop.selectOption(control.sops[0].sop_id);
      assert.equal(await page.locator('.lhs-steps').count(), 0);
      await page.locator('.lhs-detail-toggle').click();
      assert.equal(await page.locator('.lhs-attach-list').innerText(), 'fixture.txt');
      const operator = page.locator('[id$="-operator"][role="combobox"]');
      await operator.click();
      await page.getByRole('option', { name: '测试操作人', exact: true }).click();
      await page.locator('[id$="-reviewer"][role="combobox"]').click();
      await page.getByRole('option', { name: '测试审核人', exact: true }).click();
      if (work === 'polling') {
        await page.getByLabel('轮巡次数', { exact: true }).selectOption('2');
        await page.getByLabel('第1次起点', { exact: true }).selectOption('1#');
        await page.getByLabel('第1次终点', { exact: true }).selectOption('3#');
        const second = page.getByLabel('第2次起点', { exact: true });
        assert.deepEqual(await second.locator('option').evaluateAll(rows => rows.map(row => row.value).filter(Boolean)), ['4#', '5#', '6#']);
        await second.selectOption('4#');
        await page.getByLabel('第2次终点', { exact: true }).selectOption('6#');
      } else if (work === 'adjust') {
        await page.getByLabel('制冷单元', { exact: true }).selectOption('6#');
        await page.getByLabel('当前运行模式', { exact: true }).selectOption('2#');
        await page.getByLabel('切换后运行模式', { exact: true }).selectOption('2#');
        assert.equal(await page.getByLabel('切换后运行模式', { exact: true }).evaluate(input => input.checkValidity()), false);
        await page.getByLabel('切换后运行模式', { exact: true }).selectOption('4#');
        assert.equal(await page.getByRole('button', { name: '增加机柜' }).count(), 0);
      }
      const exempt = page.locator('.lhs-exempt input');
      await exempt.check(); await exempt.uncheck();
      assert.equal(await sop.inputValue(), control.sops[0].sop_id);
      await page.getByRole('button', { name: '重新加载目录' }).click();
      await sop.waitFor({ state: 'visible' });
      await page.waitForFunction(() => !document.querySelector('.lhs-exempt input')?.disabled);
      assert.match(await operator.innerText(), /测试操作人/);
      assert.equal(requests, work === 'maintenance' ? 3 : 2, 'mount, optional retry and one explicit reload only');
      const text = await page.locator('.plan-form').innerText();
      assert.ok(!text.includes('person-one') && !text.includes('query_form_') && !text.includes('private-one'));
      for (const element of [page.locator('html'), page.locator('.plan-form'), page.locator('.lhs')]) {
        const size = await element.evaluate(el => ({ scroll: el.scrollWidth, client: el.clientWidth }));
        assert.ok(size.scroll <= size.client + 1, `${work}/${width}: overflow`);
      }
      await page.locator('.lhs').scrollIntoViewIfNeeded();
      await page.screenshot({ path: path.join(output, `${work}-${width}.png`) });
      await page.getByRole('button', { name: '补充并继续', exact: true }).click();
      await page.waitForFunction(() => !document.querySelector('.lhs'));
      const filled = saves.at(-1).values['step0.notice_sop'];
      assert.deepEqual(Object.keys(filled).filter(key => key !== '$query').sort(), ['exempt', 'scope', 'sop_id', 'operator_record_id', 'reviewer_record_id', 'runs'].sort());
      assert.equal(filled.exempt, false); assert.equal(filled.operator_record_id, 'person-one'); assert.equal(filled.reviewer_record_id, 'person-two');
      if (work === 'adjust') assert.deepEqual(filled.runs.map(({ from_unit, to_unit, other_unit }) => ({ from_unit, to_unit, other_unit })), [{ from_unit: '2#', to_unit: '4#', other_unit: '6#' }]);
      if (work === 'polling') assert.deepEqual(filled.runs.map(({ from_unit, to_unit }) => ({ from_unit, to_unit })), [{ from_unit: '1#', to_unit: '3#' }, { from_unit: '4#', to_unit: '6#' }]);
    }
    assert.deepEqual(errors, []);
    await context.close();
    console.log(`Notice SOP browser: three modes, payloads, people, reload, ${width}px OK`);
  }
} finally { await browser.close(); await new Promise(resolve => server.httpServer.close(resolve)); }
