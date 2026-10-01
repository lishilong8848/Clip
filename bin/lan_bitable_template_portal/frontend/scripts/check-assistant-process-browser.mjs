import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

// Bounded acceptance for the assistant's folded "处理过程" (process) control and the
// prominent Stop button. Runs ONLY against the isolated preview on 19003
// (lighthouse_stream_preview.py) and validates health.instance_id before any write.
//
// Phase 1/2 are self-contained UI mocks: process is injected via a routed conversation
// payload, Stop's cancel request is counted and delayed, and the SSE stream endpoint is
// routed to a no-op body (no cloud, no real business data). Phase 3 is a real backend
// end-to-end check in a separate unmocked context against the preview: it sends one query,
// receives process over the real SSE stream, closes/reopens the panel, cancels once, and
// asserts the stopped response survives a refresh. No real cloud is contacted.
const base = process.env.LIGHTHOUSE_STREAM_URL || 'http://127.0.0.1:19003';
const output = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../../output/playwright/assistant-process');
await mkdir(output, { recursive: true });

const VW = 1440, VH = 1000;
const PAGE_ERRORS = [];

const completedTurn = {
  operation_id: 'op-1', run_id: 'run-1', question: '处理一下B楼设备', answer: '已完成处理。',
  status: 'completed', error: '', at: 1, model_name: '测试模型',
  output_files: [{ id: 'a'.repeat(32), name: '隔离演练审核记录及评估记录_人员姓名占位与签字时间核对_20261001.xlsx', url: '/api/assistant/files/' + 'a'.repeat(32), is_image: false }],
  process: [
    { label: '已接收请求', at: 100 },
    { label: '正在查询资料', at: 101 },
    { label: '回答完成', at: 102 },
  ],
};
const runningTurn = {
  operation_id: 'op-2', run_id: 'run-2', question: '继续处理', answer: '正在生成…',
  status: 'pending', error: '', at: 2, model_name: '测试模型',
  process: [{ label: '已接收请求', at: 200 }, { label: '正在整理上下文', at: 201 }],
};

function convBody({ busy, canceled }) {
  // Once stopped the mocked turn is kept (status changed in-place) so the conversation
  // reflects the real backend: a stopped turn stays in history at the same position.
  const turns = busy || canceled ? [runningTurn] : [completedTurn];
  const active = busy && !canceled ? runningTurn.run_id : '';
  return {
    ok: true,
    data: {
      conversation_id: 'c-proc', configured: true, enabled: true,
      can_manage_settings: false, model_name: '测试模型', model_options: [],
      turns, active_run_id: active, busy: busy && !canceled,
      phase: busy && !canceled ? '正在整理上下文' : '',
    },
  };
}

const browser = await chromium.launch({ headless: true });

async function assertIsolated(context) {
  const res = await context.request.get(base + '/api/health');
  assert.equal(res.ok(), true, 'isolated server health endpoint must respond 200 before any write');
  const body = await res.json();
  assert.equal(body.instance_id, 'isolated-lighthouse-stream', 'refuse to write against a non-isolated server');
  assert.equal(body.ok, true, 'isolated health must report ok');
}

async function installRoutes(page, state) {
  await page.route('**/api/assistant/conversation', async route => {
    if (route.request().method() === 'GET') {
      await route.fulfill({ json: convBody(state) });
    } else {
      await route.continue();
    }
  });
  await page.route('**/api/assistant/messages', async route => {
    if (route.request().method() === 'POST') {
      let operationId = 'op-2';
      try { operationId = JSON.parse(route.request().postData() || '{}').operation_id || operationId; } catch { /* keep default */ }
      const turn = { ...runningTurn, operation_id: operationId };
      await route.fulfill({ json: { ok: true, data: { conversation_id: 'c-proc', run_id: runningTurn.run_id, turn } } });
    } else {
      await route.continue();
    }
  });
  await page.route('**/api/assistant/runs/*/cancel', async route => {
    if (route.request().method() === 'POST') {
      state.cancelCalls += 1;
      await new Promise(resolve => setTimeout(resolve, 400)); // keep '正在停止' observable
      // Mutate the in-flight mock so the follow-up conversation read reflects a real backend
      // stop: the same turn is kept in history, marked 'stopped', and the run is no longer active.
      runningTurn.status = 'stopped';
      runningTurn.error = '回答已停止，可继续或重新提问。';
      state.canceled = true;
      await route.fulfill({ json: { ok: true } });
    } else {
      await route.continue();
    }
  });
  await page.route('**/api/assistant/runs/*/stream', async route => {
    if (route.request().method() === 'GET') {
      await route.fulfill({ status: 200, contentType: 'text/event-stream', body: '' });
    } else {
      await route.continue();
    }
  });
}

async function ensureOpen(page) {
  await page.locator('.lighthouse').first().waitFor();
  if ((await page.locator('.assistant-panel').count()) === 0) {
    const btn = page.getByRole('button', { name: '打开灯塔助手', exact: true });
    await btn.waitFor();
    await btn.click();
  }
  await page.locator('.assistant-panel').waitFor();
  await page.getByRole('textbox', { name: '询问灯塔助手' }).waitFor();
}

async function assertNoHorizontalOverflow(page, label) {
  const docOk = await page.evaluate(() =>
    document.documentElement.scrollWidth <= innerWidth + 1 && document.body.scrollWidth <= innerWidth + 1);
  assert(docOk, `${label}: document must not overflow horizontally`);
  const panelOk = await page.locator('.assistant-panel').evaluate(el => el.scrollWidth <= el.clientWidth + 1);
  assert(panelOk, `${label}: assistant panel must not overflow horizontally`);
}

try {
  const context = await browser.newContext({ viewport: { width: VW, height: VH } });
  await assertIsolated(context);
  const page = await context.newPage();
  page.on('pageerror', e => PAGE_ERRORS.push(`[${page.url()}] ${e.message}`));

  const state = { busy: false, canceled: false, cancelCalls: 0 };
  await installRoutes(page, state);

  // ---------- Phase 1: default-closed, expand, persist process ----------
  await page.goto(base);
  await ensureOpen(page);
  await page.waitForFunction(() => document.querySelectorAll('.turn').length >= 1);
  const download = page.locator('.turn .answer .output-files a');
  assert.equal(await download.count(), 1, 'generated file must appear with the assistant reply');
  assert.equal(await download.getAttribute('href'), completedTurn.output_files[0].url, 'generated file uses authenticated native download');
  assert.equal(await page.locator('.user-bubble .output-files').count(), 0, 'generated files are not presented as user uploads');

  const processDetails = page.locator('.turn').first().locator('details.process');
  await processDetails.waitFor();
  assert.equal(await processDetails.evaluate(el => el.open), false, 'process details must be default-closed');
  const summaryText = await processDetails.locator('summary').innerText();
  assert(summaryText.includes('处理过程'), 'summary must show 处理过程');
  assert(!(await page.locator('.turn .answer .reasoning').count()), 'no raw reasoning part must render');

  // The located summary must render an interactive chevron: points right while folded, then rotates open.
  const chevron = processDetails.locator('summary .process-chevron');
  await chevron.waitFor();
  assert.equal(await chevron.evaluate(el => getComputedStyle(el).transform === 'none'), true,
    'chevron must not be rotated while details are folded');

  await processDetails.locator('summary').click();
  const rows = processDetails.locator('.process-rows li');
  assert.equal(await rows.count(), 3, 'expanded process must show 3 rows');
  const labels = await rows.allInnerTexts();
  assert(labels[0].includes('已接收请求') && labels[1].includes('正在查询资料') && labels[2].includes('回答完成'),
    `process rows should show safe execution labels (${JSON.stringify(labels)})`);
  // Poll until the chevron matrix shows a 90deg rotation (point right -> down) once open.
  await page.waitForFunction(() => {
    const el = document.querySelector('.turn details.process summary .process-chevron');
    if (!el) return false;
    const m = new DOMMatrixReadOnly(getComputedStyle(el).transform);
    return Math.abs(Math.round(m.a)) === 0 && Math.abs(Math.round(m.b)) === 1;
  }, undefined, { timeout: 3000 });
  console.log('[pass] process: default-closed above answer and expands to safe labels, no raw thinking, chevron rotates');

  // persist: hard refresh while open must retain the folded process history
  await page.reload({ waitUntil: 'load' });
  await ensureOpen(page);
  await page.waitForFunction(() => document.querySelectorAll('.turn').length >= 1);
  const persistedDetails = page.locator('.turn').first().locator('details.process');
  await persistedDetails.waitFor();
  assert.equal(await persistedDetails.evaluate(el => el.open), false, 'process details must stay collapsed after refresh');
  await persistedDetails.locator('summary').click();
  assert.equal(await persistedDetails.locator('.process-rows li').count(), 3, 'process history must persist through refresh');
  console.log('[pass] process: persists through refresh/resume with no duplicate items');

  // Phase 1 narrow screen containment (folded details)
  await page.setViewportSize({ width: 390, height: 844 });
  await page.waitForFunction(() => {
    const r = document.querySelector('.assistant-panel')?.getBoundingClientRect();
    return r && r.right <= innerWidth && r.bottom <= innerHeight;
  });
  await assertNoHorizontalOverflow(page, 'process-mobile');
  await page.screenshot({ path: path.join(output, 'process-mobile.png') });
  await page.setViewportSize({ width: VW, height: VH });
  console.log('[pass] process: usable and no overflow at 390px');

  // ---------- Phase 2: prominent Stop, one cancel request, input usable ----------
  state.busy = true;
  state.canceled = false;
  await page.reload({ waitUntil: 'load' });
  await ensureOpen(page);
  const stopBtn = page.getByRole('button', { name: '停止生成', exact: true });
  await stopBtn.waitFor({ timeout: 10000 });
  assert(await stopBtn.isVisible(), 'stop button visible while busy');
  assert.equal(await stopBtn.innerText(), '停止回答', 'stop button shows 停止回答 text');
  const stopStyle = await stopBtn.evaluate(el => {
    const cs = getComputedStyle(el);
    return { borderColor: cs.borderTopColor, borderWidth: cs.borderTopWidth, bg: cs.backgroundColor };
  });
  assert(stopStyle.borderWidth !== '0px' && stopStyle.borderColor !== 'rgba(0, 0, 0, 0)',
    `stop must have a prominent outline (${JSON.stringify(stopStyle)})`);
  assert.equal(await stopBtn.locator('svg').count(), 1, 'stop button shows a Square icon');
  console.log('[pass] stop: prominent red-outline 停止回答 with Square icon and preserved aria-label 停止生成');

  // stop remains usable on 390px (no overflow while stop visible)
  await page.setViewportSize({ width: 390, height: 844 });
  await page.waitForFunction(() => {
    const r = document.querySelector('.assistant-panel')?.getBoundingClientRect();
    return r && r.right <= innerWidth && r.bottom <= innerHeight;
  });
  await assertNoHorizontalOverflow(page, 'stop-mobile');
  const stopBox = await stopBtn.boundingBox();
  assert(stopBox && stopBox.x >= 0 && stopBox.x + stopBox.width <= 390, 'stop button must fit 390px viewport');
  await page.screenshot({ path: path.join(output, 'stop-mobile.png') });

  await stopBtn.click();
  await waitForCondition(() => state.cancelCalls >= 1, 4000);
  assert.equal(state.cancelCalls, 1, 'one cancel request must be issued');
  await expectStopStopping(page);

  // after cancel resolves, stop hides (readState returns not busy) and input stays usable
  await page.waitForFunction(() => {
    const btn = document.querySelector('.composer-footer .stop');
    return !btn;
  }, undefined, { timeout: 8000 });
  const input = page.getByRole('textbox', { name: '询问灯塔助手' });
  assert.equal(await input.isEditable(), true, 'input must remain editable after stop');
  await input.fill('下一个问题：只看D楼');
  assert.equal(await input.inputValue(), '下一个问题：只看D楼', 'input must accept supplementary text');
  console.log("[pass] stop: cancel requested exactly once, 正在停止 shown disabled, input still usable");

  // The stopped turn must survive a hard refresh: same turn at same position, no Stop button.
  await page.reload({ waitUntil: 'load' });
  await ensureOpen(page);
  await page.waitForFunction(() => document.querySelectorAll('.turn').length >= 1);
  const stoppedTurn = page.locator('.turn').first();
  await stoppedTurn.waitFor();
  assert.equal(((await stoppedTurn.innerText()) || '').includes('继续处理'), true, 'stopped turn question must persist after refresh');
  assert.equal(await stoppedTurn.locator('.composer-footer .stop').count(), 0, 'stop must not reappear after refresh of a stopped run');
  await stoppedTurn.locator('details.process summary').click();
  assert.equal(await stoppedTurn.locator('details.process .process-rows li').count(), 2, 'stopped turn keeps its own process rows after refresh');
  console.log('[pass] stop: stopped turn persists across refresh with its process and no stop button');

  assert.deepEqual(PAGE_ERRORS, [], PAGE_ERRORS.join('\n'));

  // ---------- Phase 3: real backend end-to-end (unmocked preview) ----------
  // A separate context with NO route mocks talks directly to the isolated preview backend
  // (lighthouse_stream_preview.py). Its synthetic model streams process via SSE and persists
  // cancelled runs. This is a genuine backend assertion: one query, process received over SSE,
  // close/reopen to reconnect, one cancel, and the stopped response survives a refresh.
  {
    const realContext = await browser.newContext({ viewport: { width: VW, height: VH } });
    await assertIsolated(realContext);
    await realContext.addCookies([{ name: 'fixture_user', value: 'A', url: base }]);
    const realPage = await realContext.newPage();
    realPage.on('pageerror', e => PAGE_ERRORS.push(`[e2e] ${e.message}`));
    try {
      await realPage.goto(base);
      await ensureOpen(realPage);

      const realInput = realPage.getByRole('textbox', { name: '询问灯塔助手' });
      await realInput.fill('处理一下维保事项 长回答 快速');
      await realPage.getByRole('button', { name: '发送问题', exact: true }).click();

      // Receive process via SSE: the real run emits data-progress rows.
      await realPage.waitForFunction(() => {
        const box = document.querySelector('.turn details.process');
        return !!box && box.querySelectorAll('.process-rows li').length >= 1;
      }, undefined, { timeout: 20000 });
      const realProcessRows = await realPage.locator('.turn').first().locator('details.process .process-rows li').count();
      assert(realProcessRows >= 1, `real backend must stream process rows via SSE (got ${realProcessRows})`);
      console.log('[pass] e2e: real backend streamed process via SSE for one query');

      // Close/reopen the panel: the stream disconnects, then reconnects to the same run.
      await realPage.getByRole('button', { name: '收起助手', exact: true }).click();
      const launcher = realPage.getByRole('button', { name: '打开灯塔助手', exact: true });
      await launcher.waitFor();
      await launcher.click();
      await ensureOpen(realPage);

      // The run is still active, so the Stop button reappears and can cancel once.
      const realStop = realPage.getByRole('button', { name: '停止生成', exact: true });
      await realStop.waitFor({ timeout: 20000 });
      await realStop.click();
      await realPage.waitForFunction(() => !document.querySelector('.composer-footer .stop'), undefined, { timeout: 20000 });
      await realPage.waitForFunction(() => {
        const retry = [...document.querySelectorAll('.turn .message-actions .retry')].find(el => (el.textContent || '').includes('继续回答'));
        return !!retry;
      }, undefined, { timeout: 20000 });
      console.log('[pass] e2e: cancel issued once and turn became stopped');

      // The stopped response must survive a hard refresh.
      await realPage.reload({ waitUntil: 'load' });
      await ensureOpen(realPage);
      await realPage.waitForFunction(() => document.querySelectorAll('.turn').length >= 1);
      const e2eTurn = realPage.locator('.turn').first();
      await e2eTurn.waitFor();
      assert(((await e2eTurn.innerText()) || '').includes('维保事项'), 'stopped E2E turn question must persist after refresh');
      assert.equal(await e2eTurn.locator('.composer-footer .stop').count(), 0, 'no stop button on persisted stopped E2E turn');
      const e2eProcess = e2eTurn.locator('details.process');
      await e2eProcess.waitFor();
      assert((await e2eProcess.locator('.process-rows li').count()) >= 1, 'persisted stopped turn must keep process from SSE');
      assert(((await e2eTurn.locator('.message-actions .retry').innerText().catch(() => '')) || '').includes('继续回答'),
        'persisted stopped turn must expose 继续回答 after refresh');
      console.log('[pass] e2e: stopped response survived refresh with its process');
    } finally {
      await realContext.close();
    }
  }

  console.log('Assistant process browser acceptance PASSED: default-closed/expand/persist process, safe labels only, chevron, prominent stop with single cancel, input usable, 390px no overflow, real-backend SSE + cancel + refresh persistence, no pageerror.');
} catch (error) {
  console.error(error);
  const outputDir = output;
  for (const p of browser.contexts().flatMap(ctx => ctx.pages())) {
    await p.screenshot({ path: path.join(outputDir, 'assistant-process-failure.png') }).catch(() => {});
    const text = await p.locator('.lighthouse').innerText().catch(() => 'assistant unavailable');
    console.error(text);
    break;
  }
  throw error;
} finally {
  await browser.close();
}

async function expectStopStopping(page) {
  await page.waitForFunction(() => {
    const el = document.querySelector('.composer-footer .stop span');
    return el && el.textContent === '正在停止';
  }, undefined, { timeout: 4000 });
  const stopBtn = page.locator('.composer-footer .stop');
  await stopBtn.waitFor({ timeout: 4000 });
  assert(await stopBtn.isDisabled(), 'stop button must be disabled while stopping');
}

function waitForCondition(condition, timeoutMs) {
  return new Promise((resolve, reject) => {
    const start = Date.now();
    const tick = () => {
      if (condition()) { resolve(); return; }
      if (Date.now() - start > timeoutMs) { reject(new Error('condition not met within timeout')); return; }
      setTimeout(tick, 40);
    };
    tick();
  });
}
