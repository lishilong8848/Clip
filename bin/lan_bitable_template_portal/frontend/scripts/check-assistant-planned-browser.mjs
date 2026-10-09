import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdir } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';
import { createServer } from 'vite';

// Browser plugin not available. Use the project's Playwright with isolated APIs.
const frontend = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const repo = path.resolve(frontend, '../../..');
const output = path.join(os.tmpdir(), 'clipflow-planned-notice-check');
const productionBase = process.env.PLANNED_BROWSER_BASE || '';
await mkdir(output, { recursive: true });
const fixtures = spawnSync(path.join(repo, 'bin/.venv/Scripts/python.exe'), ['-c', `
import asyncio,json,sys
sys.path.insert(0,'bin')
from test_lighthouse_planned import PlannedAssistantTests
async def main():
    c=PlannedAssistantTests();c.setUp()
    try:
        c.rows=[c.row('source-'+str(i)) for i in range(1,7)]
        candidates=await c.begin()
        preview=await c.amend(candidates,{'planned.source':'source-6'})
        review=await c.amend(preview,c.fill(preview))
        reset=await c.amend(review,action='planned-reset',reset_confirmed=True)
        print(json.dumps(dict(candidates=candidates,preview=preview,review=review,reset=reset),ensure_ascii=False))
    finally: c.doCleanups()
asyncio.run(main())
`], { cwd: repo, encoding: 'utf8', env: { ...process.env, PYTHONIOENCODING: 'utf-8', PYTHONWARNINGS: 'ignore', CLIPFLOW_DATA_DIR: output } });
assert.equal(fixtures.status, 0, fixtures.stderr);
const stages = JSON.parse(fixtures.stdout);
const html = `<html><head><meta charset="utf-8"><title>计划通告隔离验证</title></head><body><div id="app"></div><script type="module">
import {createApp,h} from 'vue';import Assistant from '/src/components/LighthouseAssistant.vue';
import UiTransition from '/src/components/UiTransition.vue';import LoadingIndicator from '/src/components/LoadingIndicator.vue';import '/src/global.css';
createApp({render:()=>h(Assistant,{userName:'隔离测试账号',userId:'planned-browser'})}).component('UiTransition',UiTransition).component('LoadingIndicator',LoadingIndicator).mount('#app');
</script></body></html>`;
const server = await createServer({ root: frontend, logLevel: 'error', server: { host: '127.0.0.1', port: 0, hmr: false }, plugins: [{ name: 'planned-fixture', configureServer(dev) {
  dev.middlewares.use('/__planned', async (_request, response) => { response.setHeader('Content-Type', 'text/html'); response.end(await dev.transformIndexHtml('/__planned', html)); });
} }] });
let browser, plan = structuredClone(stages.candidates), confirmed = 0;
const requests = [], errors = [];
try {
  if (!productionBase) await server.listen();
  browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
  if (productionBase) assert.equal((await (await page.request.get(productionBase + '/api/health')).json()).instance_id, 'isolated-lighthouse-stream');
  page.on('pageerror', error => errors.push(error.message));
  await page.route('**/api/**', async route => {
    const url = new URL(route.request().url()), method = route.request().method();
    if (!url.pathname.startsWith('/api/')) return route.continue();
    requests.push([method, url.pathname]);
    if (url.pathname === '/api/assistant/conversation') return route.fulfill({ json: { ok: true, data: {
      conversation_id: 'planned-browser', configured: true, enabled: true, busy: false, model_options: [],
      turns: [{ operation_id: 'planned-browser-turn', question: '发送冷水机组月度维护', answer: '请选择本次计划。', status: 'completed', plan }],
    } } });
    if (url.pathname.includes('/plans/')) {
      const body = method === 'GET' ? {} : route.request().postDataJSON();
      if (url.pathname.endsWith('/confirm')) { confirmed++; plan = { ...plan, status: 'completed', results: [{ ok: true, data: {} }] }; }
      else if (body.action === 'planned-reset') { assert.equal(body.reset_confirmed, true); plan = structuredClone(stages.reset); }
      else if (body.values?.['planned.source']) { assert.equal(body.values['planned.source'], 'source-6'); plan = structuredClone(stages.preview); }
      else if (body.values?.['planned.query']) plan = structuredClone(stages.candidates);
      else if (body.values?.['step0.patch']) {
        assert.equal(body.values['step0.patch'].progress, '本次现场已核对');
        assert.equal(body.values['step0.notice_sop'].exempt, true);
        plan = structuredClone(stages.review);
      }
      return route.fulfill({ json: { ok: true, data: plan } });
    }
    if (url.pathname === '/api/workbench-actions') throw new Error('Browser must not submit business directly');
    return productionBase ? route.continue() : route.fulfill({ json: { ok: true, data: {} } });
  });
  await page.goto(productionBase || `http://127.0.0.1:${server.httpServer.address().port}/__planned`);
  await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
  await page.locator('.planned-choices').waitFor();
  assert.equal(await page.locator('.planned-choices input').count(), 5);
  await page.screenshot({ path: path.join(output, 'candidates.png') });
  await page.getByRole('button', { name: /查看更多/ }).click();
  assert.equal(await page.locator('.planned-choices input').count(), 6);
  await page.locator('.planned-choices input').last().check();
  await page.getByRole('button', { name: '补充并继续', exact: true }).click();
  await page.locator('.planned-notice-selected').waitFor();
  assert.equal(await page.getByText('计划通告关联', { exact: true }).count(), 0);
  assert.equal(await page.getByLabel('名称', { exact: true }).isDisabled(), true);
  await page.getByRole('button', { name: '重新选择计划', exact: true }).click();
  await page.getByRole('button', { name: '保留填写', exact: true }).click();
  await page.getByLabel('进度', { exact: true }).fill('本次现场已核对');
  await page.locator('.lhs-exempt input').check();
  await page.getByRole('button', { name: '补充并继续', exact: true }).click();
  await page.locator('.planned-notice-text').waitFor();
  assert.match(await page.locator('.planned-notice-text').innerText(), /本次现场已核对/);
  assert.equal(confirmed, 0);
  await page.screenshot({ path: path.join(output, 'review.png') });
  const composer = page.locator('.composer textarea');
  await composer.fill('确认发送');
  await composer.press('Enter');
  await page.waitForFunction(() => document.querySelector('[data-plan-status="completed"]'));
  assert.equal(confirmed, 1);
  assert.equal(await composer.inputValue(), '');
  assert.equal(requests.some(([, url]) => url === '/api/workbench-actions'), false);
  assert.deepEqual(errors, []);
  console.log(JSON.stringify({ ok: true, viewport: '1440x1000', screenshots: output, assertions: '5+more, select, fixed facts, no binding prompt, fill, full preview, natural confirmation, no direct business writes' }));
} finally { if (browser) await browser.close(); await server.close(); }
