import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

// 隔离预览服务器由 Codex 启动/维护，本脚本只写 instance_id === 'isolated-ai-preview' 的实例。
// 覆盖当前 LighthouseAssistant.vue 面向 agent 的关键路径：
//   - POST /api/assistant/agent（携带 file_ids），不再有 /api/assistant/chat
//   - 计划多步确认：awaiting_confirmation → awaiting_second_confirmation → completed，
//     并在 /api/fixture/writes 上做写计数（绝不写真实业务 API）
//   - 需补字段（needs_input）与取消：不产生新写入
//   - 附件：.composer input[type=file] 上传 TXT/PNG，preview = /api/assistant/files/{32hex}
//   - 账号隔离 / reload 保留 plans/turns / 成功失败结果可见
//   - desktop(1440) 与 mobile(390) 无横向溢出、计划表单/按钮可见
const base = process.env.ASSISTANT_PREVIEW_URL || 'http://127.0.0.1:19002';
const output = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../../output/playwright/assistant-agent');
await mkdir(output, { recursive: true });

const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({ viewport: { width: 1440, height: 1000 } });

// 任何写入前先校验隔离实例。
{
  const health = await context.request.get(base + '/api/health');
  assert.equal(health.ok(), true, 'isolated server health endpoint must respond 200 before any write');
  const healthBody = await health.json();
  assert.equal(healthBody.instance_id, 'isolated-ai-preview', 'refuse to write against a non-isolated server');
  assert.equal(healthBody.ok, true, 'isolated health must report ok');
}

// 初始会话 DELETE 必须携带 Origin 头并通过同源校验（仅隔离实例允许）。
const reset = await context.request.delete(base + '/api/assistant/conversation', { headers: { origin: base } });
assert.equal(reset.ok(), true, 'initial conversation DELETE must succeed with a matching Origin header');

const pageErrors = [];
const page = await context.newPage();
page.on('pageerror', e => pageErrors.push(e.message));

// ---------- helpers ----------
async function fixtureWrites() {
  const resp = await context.request.get(base + '/api/fixture/writes');
  assert.equal(resp.ok(), true, 'GET /api/fixture/writes must be readable');
  const body = await resp.json();
  assert.equal(body.ok, true, 'fixture writes must report ok');
  assert(typeof body.data?.count === 'number', 'fixture writes count must be a number');
  return body.data.count;
}
function lastTurn(loc = page) {
  return loc.locator('.turn').last();
}
function lastPlan(loc = page) {
  return lastTurn(loc).locator('.operation-plan').first();
}
async function waitPlanStatus(status, loc = page) {
  await loc.waitForFunction((s) => {
    const plans = Array.from(document.querySelectorAll('.operation-plan[data-plan-status]'));
    const el = plans[plans.length - 1];
    return Boolean(el && el.getAttribute('data-plan-status') === s);
  }, status, { timeout: 20000 });
}
async function openAssistant(loc = page) {
  await loc.locator('.assistant-panel, .assistant-launcher').first().waitFor();
  if (await loc.locator('.assistant-panel').count() === 0) {
    const launcher = loc.getByRole('button', { name: '打开灯塔助手', exact: true });
    await launcher.waitFor();
    await launcher.click();
  }
  await loc.getByRole('textbox', { name: '询问灯塔助手' }).waitFor();
}
async function closeAssistant(loc = page) {
  const btn = loc.getByRole('button', { name: '收起助手', exact: true });
  if (await btn.count()) await btn.click();
}
async function ask(question, loc = page) {
  await loc.getByRole('textbox', { name: '询问灯塔助手' }).fill(question);
  await loc.getByRole('button', { name: '发送问题', exact: true }).click();
}
async function confirmPlanButton(loc = page) {
  return lastPlan(loc).locator('.plan-actions button.primary').first();
}
async function waitDraftReady(loc = page) {
  await loc.locator('.draft-files .draft-file', { hasText: '已就绪' }).first().waitFor({ timeout: 20000 });
}

const PNG_1X1 = 'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=';

try {
  // ---------- 打开并确认空会话 ----------
  await page.goto(base);
  await openAssistant();
  await page.getByText('今天有什么需要处理的？', { exact: true }).waitFor();
  assert.equal(await page.locator('.turn').count(), 0, 'fresh conversation must have no turns');

  // ---------- 1) 发送测试通告：多步确认 + 写计数 ----------
  const countBeforeFirstPlan = await fixtureWrites();
  assert.equal(countBeforeFirstPlan, 0, 'fixture must reset writes to 0 before the plan test');

  await ask('发送测试通告');
  await waitPlanStatus('awaiting_confirmation');
  assert.equal(await fixtureWrites(), 0, 'review confirmation must not write yet');

  // 一级确认 → awaiting_second_confirmation，仍不写入
  const reviewBtn = await confirmPlanButton();
  assert.match((await reviewBtn.innerText()).trim(), /确认操作清单/);
  await reviewBtn.click();
  await waitPlanStatus('awaiting_second_confirmation');
  assert.equal(await fixtureWrites(), 0, 'first confirmation (review) must not write');

  // 二级确认 → 执行，恰好写入 1 次，且重复点击被 planBusy 保护
  const executeBtn = await confirmPlanButton();
  assert.match((await executeBtn.innerText()).trim(), /再次确认并执行/);
  let gateResolve;
  const executeGate = new Promise(res => { gateResolve = res; });
  let firstExecuteSeenResolve;
  const firstExecuteSeen = new Promise(res => { firstExecuteSeenResolve = res; });
  let executeHandlerDoneResolve;
  const executeHandlerDone = new Promise(res => { executeHandlerDoneResolve = res; });
  let executePosts = 0;
  await page.route('**/api/assistant/plans/*/confirm', async route => {
    const method = route.request().method();
    const isExecute = method === 'POST' && (route.request().postData() || '').includes('"stage":"execute"');
    try {
      if (isExecute) {
        executePosts += 1;
        if (executePosts === 1) firstExecuteSeenResolve();
        await executeGate;
      }
      await route.continue();
    } finally {
      // 标记第一个被 gate 拦截的 execute 处理者真正完成（continue 已返回），
      // 供测试端在移除路由前等待，避免 unroute 时 handler 仍在途导致的
      // “Route is already handled!” 竞态。不改变写计数/双击确认断言。
      if (isExecute && executePosts === 1) executeHandlerDoneResolve();
    }
  });
  await executeBtn.click();
  // 等待第一次 execute POST 已被拦截（请求在途，响应因 gate 尚未返回）
  await firstExecuteSeen;
  // planBusy=true 时按钮禁用 → 防止第二次点击重复提交
  assert.equal(await executeBtn.isDisabled(), true, 'while executing, plan confirm button must be disabled (repeat-click protection)');
  // 即便强点也不应产生第二次 execute（按钮已禁用；此处仅确认 confirm POST 计数仍为 1）
  await executeBtn.click({ force: true }).catch(() => {});
  await page.waitForTimeout(120);
  assert.equal(executePosts, 1, 'repeat click must not create a second execute confirm POST');

  gateResolve();
  // 等待被 gate 拦截的拦截处理者完成 route.continue() 后再移除路由，
  // 消除“Route is already handled!”竞态。
  await executeHandlerDone;
  await page.unroute('**/api/assistant/plans/*/confirm');
  await waitPlanStatus('completed');
  assert.equal(await fixtureWrites(), 1, 'execute confirmation must write exactly one fixture item');

  // 成功结果清晰显示：已完成标签 + 无失败信息
  const completedPlan = lastPlan();
  assert.match(await completedPlan.innerText(), /已完成/);
  assert.equal(await completedPlan.locator('.failure').count(), 0, 'success plan must not contain failure text');
  assert((await completedPlan.locator('.plan-results').count()) >= 1 || (await completedPlan.innerText()).includes('OK'),
    'completed plan must surface a clear result region');

  // reload 保留 plans/turns
  await page.reload();
  await openAssistant();
  await waitPlanStatus('completed');
  assert((await page.locator('.turn').count()) >= 1, 'reload must retain the plan turn');
  assert.match(await lastPlan().innerText(), /已完成/, 'reloaded plan must still be completed');
  await page.screenshot({ path: path.join(output, 'agent-send-test-completed.png') });

  // ---------- 2) 准备一条通告：缺字段补填 + 取消不写入 ----------
  const writesBeforePrepare = await fixtureWrites();
  await ask('准备一条通告');
  await waitPlanStatus('needs_input');

  const titleField = page.locator('.plan-form label').filter({ hasText: '标题' }).first();
  await titleField.waitFor();
  assert(await titleField.innerText().then(t => t.includes('*')), 'title field must be marked required');
  assert(await titleField.locator('input').count() > 0, 'title field must contain an input');

  // 未填直接提交：原生 required 阻止请求，状态保持 needs_input、无写入
  await page.locator('.plan-form button.primary', { hasText: '补充并继续' }).first().click().catch(() => {});
  await page.waitForTimeout(150);
  await waitPlanStatus('needs_input');
  assert.equal(await fixtureWrites(), writesBeforePrepare, 'empty required submit must not write');

  // 填标题后补充并继续 → awaiting_confirmation
  await titleField.locator('input').fill('测试通告标题');
  let amendRelease, amendSeenResolve, amendHandledResolve;
  const amendGate = new Promise(resolve => { amendRelease = resolve; });
  const amendSeen = new Promise(resolve => { amendSeenResolve = resolve; });
  const amendHandled = new Promise(resolve => { amendHandledResolve = resolve; });
  await page.route('**/api/assistant/plans/*', async route => {
    if (route.request().method() !== 'PATCH') { await route.continue(); return; }
    amendSeenResolve();
    try { await amendGate; await route.continue(); }
    finally { amendHandledResolve(); }
  });
  const amendPostPromise = page.waitForResponse(
    r => r.request().method() === 'PATCH' && r.url().includes('/api/assistant/plans/'),
  );
  await page.locator('.plan-form button.primary', { hasText: '补充并继续' }).first().click();
  await amendSeen;
  assert.equal(await titleField.locator('input').isDisabled(), true, 'submitted fields must not be edited in flight');
  // Closing/reopening replaces every turn object while the original PATCH is
  // still pending. Its response must update the current turn, not a stale copy.
  await closeAssistant();
  await openAssistant();
  await waitPlanStatus('needs_input');
  amendRelease();
  await amendPostPromise;
  await amendHandled;
  await page.unroute('**/api/assistant/plans/*');
  await waitPlanStatus('awaiting_confirmation');
  assert.equal(await fixtureWrites(), writesBeforePrepare, 'supply title must still not write');

  // 取消操作 → cancelled，且不再产生写入
  await page.locator('.plan-actions button', { hasText: '取消操作', exact: false }).first().click();
  await waitPlanStatus('cancelled');
  assert.equal(await fixtureWrites(), writesBeforePrepare, 'cancelling the plan must not write');
  assert.match(await lastPlan().innerText(), /已取消/, 'cancelled plan must display clearly');
  await page.screenshot({ path: path.join(output, 'agent-prepare-cancelled.png') });

  // ---------- 3) 附件：TXT 传递 + PNG preview URL + 移除草稿 + 粘贴多图 ----------
  // 3a) TXT：上传到"已就绪"，发送读取文件，turn 附件展示文件名
  const composerFile = page.locator('.composer input[type=file]');
  assert.equal(await composerFile.count(), 1, 'composer must expose a hidden file input');
  await composerFile.setInputFiles({
    name: 'plan附件.txt',
    mimeType: 'text/plain',
    buffer: Buffer.from('测试通告附件内容', 'utf8'),
  });
  await waitDraftReady();
  assert((await page.locator('.draft-files').innerText()).includes('plan附件.txt'), 'TXT draft must appear ready');
  const txtAgent = page.waitForResponse(r => r.request().method() === 'POST' && r.url().includes('/api/assistant/agent'));
  await ask('读取文件');
  await txtAgent;
  await page.waitForFunction(() => {
    const last = Array.from(document.querySelectorAll('.turn')).pop();
    return last && Array.from(last.querySelectorAll('.message-files')).length > 0;
  }, undefined, { timeout: 15000 });
  const lastTxtTurn = lastTurn();
  assert.match(await lastTxtTurn.locator('.message-files').innerText(), /plan附件\.txt/,
    'uploaded TXT file name must appear in turn attachment');

  // 3b) PNG：上传后发送，turn 附件 img 的 src 必须是 /api/assistant/files/{32hex}
  const pngName = 'plan图.png';
  await composerFile.setInputFiles({
    name: pngName,
    mimeType: 'image/png',
    buffer: Buffer.from(PNG_1X1, 'base64'),
  });
  await waitDraftReady();
  // 移除草稿附件可用
  const removeDraft = page.getByRole('button', { name: /移除附件/ }).first();
  await removeDraft.click();
  await page.waitForFunction((name) => !Array.from(document.querySelectorAll('.draft-file')).some(el => el.textContent?.includes(name)), pngName);
  assert.equal(await page.locator('.draft-file', { hasText: pngName }).count(), 0, 'removing a draft attachment must work');

  // 重新挂上 PNG 并发送，验证 preview 是 /api/assistant/files/{uuid}
  await composerFile.setInputFiles({
    name: pngName,
    mimeType: 'image/png',
    buffer: Buffer.from(PNG_1X1, 'base64'),
  });
  await waitDraftReady();
  const pngAgent = page.waitForResponse(r => r.request().method() === 'POST' && r.url().includes('/api/assistant/agent'));
  await ask('读取图片');
  await pngAgent;
  await page.waitForFunction(() => {
    const last = Array.from(document.querySelectorAll('.turn')).pop();
    return last && last.querySelector('.message-files img[src^="/api/assistant/files/"]') !== null;
  }, undefined, { timeout: 15000 });
  const imgSrc = await lastTurn().locator('.message-files img').first().getAttribute('src');
  assert(/^\/api\/assistant\/files\/[a-f0-9]{32}$/.test(imgSrc || ''),
    `image preview must be /api/assistant/files/{uuid} but got ${imgSrc}`);
  assert.equal(await lastTurn().locator('.message-files img').first().getAttribute('alt'), pngName,
    'image attachment alt must be the file name');

  // 3c) 粘贴多图：通过 DataTransfer + ClipboardEvent，不覆盖原生全局 Clipboard API
  const pasted = await page.evaluate((b64) => {
    const binary = atob(b64);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i += 1) bytes[i] = binary.charCodeAt(i);
    const file = () => new File([bytes], `粘贴图${Math.random().toString(36).slice(2)}.png`, { type: 'image/png' });
    const dt = new DataTransfer();
    dt.items.add(file());
    dt.items.add(file());
    const ta = document.querySelector('#assistant-question');
    ta.dispatchEvent(new ClipboardEvent('paste', { clipboardData: dt, bubbles: true, cancelable: true }));
    return dt.files.length;
  }, PNG_1X1);
  assert.equal(pasted, 2, 'paste clipboard DataTransfer must carry two files');
  await page.waitForFunction(() => document.querySelectorAll('.draft-file').length >= 2, undefined, { timeout: 10000 });
  assert((await page.locator('.draft-file').count()) >= 2, 'pasting multiple images must add multiple draft files');
  // 清理粘贴附件，保持会话干净（循环直到为空，避免 DOM 重渲染导致引用失效）
  for (let guard = 0; guard < 10; guard += 1) {
    const remove = page.locator('.draft-file button[aria-label^="移除附件"]').first();
    if ((await remove.count()) === 0) break;
    await remove.click();
    await page.waitForTimeout(120);
  }
  await page.waitForFunction(() => document.querySelectorAll('.draft-file').length === 0, undefined, { timeout: 10000 });

  // ---------- 4) 失败结果清晰显示 -------------
  // 注入一个失败的执行确认响应，验证 .assistant-error 全局错误清晰可见，且计划保持待执行
  await ask('发送测试通告');
  await waitPlanStatus('awaiting_confirmation');
  await page.locator('.plan-actions button.primary', { hasText: '确认操作清单' }).first().click();
  await waitPlanStatus('awaiting_second_confirmation');
  let failHttp = true;
  await page.route('**/api/assistant/plans/*/confirm', async route => {
    if (route.request().method() === 'POST' && (route.request().postData() || '').includes('"stage":"execute"') && failHttp) {
      failHttp = false;
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ ok: false, error: '模拟的计划执行失败' }),
      });
      return;
    }
    await route.continue();
  });
  await page.locator('.plan-actions button.primary', { hasText: '再次确认并执行' }).first().click();
  await page.waitForFunction(() => {
    const err = document.querySelector('.assistant-error');
    return err && /模拟的计划执行失败/.test(err.textContent || '');
  }, undefined, { timeout: 15000 });
  assert.match(await page.locator('.assistant-error').innerText(), /模拟的计划执行失败/, 'plan failure must be displayed clearly in the global error');
  // 计划保持 awaiting_second_confirmation（错误失败不会推进状态）
  await page.waitForTimeout(150);
  await waitPlanStatus('awaiting_second_confirmation');
  assert.equal(await page.locator('.operation-plan[data-plan-status="failed"]').count(), 0, 'a failed confirm must not mark the plan as failed/executed');
  await page.unroute('**/api/assistant/plans/*/confirm');

  // ---------- 5) 桌面/移动端布局 ----------
  // desktop 已是 1440x1000
  assert(await page.locator('.assistant-panel').evaluate(e => e.scrollWidth <= e.clientWidth + 1),
    'desktop assistant panel must not overflow horizontally');
  assert(await page.locator('.send').isVisible(), 'desktop send button must be visible');
  await page.screenshot({ path: path.join(output, 'agent-desktop.png') });

  // mobile(390)：无横向溢出，且 needs_input 计划表单可见可操作
  await page.setViewportSize({ width: 390, height: 844 });
  await page.waitForFunction(() => {
    const panel = document.querySelector('.assistant-panel');
    if (!panel) return false;
    const r = panel.getBoundingClientRect();
    return r.left >= 0 && r.top >= 0 && r.right <= innerWidth && r.bottom <= innerHeight;
  }, undefined, { timeout: 5000 });
  const mobilePanel = await page.locator('.assistant-panel').boundingBox();
  assert(mobilePanel && mobilePanel.x >= 0 && mobilePanel.x + mobilePanel.width <= 390,
    `mobile panel must fit viewport (${JSON.stringify(mobilePanel)})`);
  assert(await page.locator('.assistant-panel').evaluate(e => e.scrollWidth <= e.clientWidth + 1),
    'mobile assistant panel must not overflow horizontally');
  assert(await page.locator('.send').isVisible(), 'mobile send button must be visible');

  await ask('准备一条通告');
  await waitPlanStatus('needs_input');
  const mobileForm = page.locator('.plan-form');
  await mobileForm.scrollIntoViewIfNeeded();
  assert(await mobileForm.isVisible(), 'mobile plan form must be visible');
  const mobileTitle = mobileForm.locator('label').filter({ hasText: '标题' }).first();
  assert(await mobileTitle.isVisible(), 'mobile required title field must be visible');
  assert(await mobileTitle.locator('input').evaluate(e => e.offsetWidth > 0 && e.offsetHeight > 0),
    'mobile title input must be on-screen and usable');
  assert(await mobileForm.locator('button.primary', { hasText: '补充并继续' }).isVisible(),
    'mobile plan amend button must be visible');
  assert(await page.locator('.send').isVisible(), 'mobile send button must remain visible alongside the plan');
  assert(await page.locator('.assistant-panel').evaluate(e => e.scrollWidth <= e.clientWidth + 1),
    'mobile assistant panel must not overflow horizontally with an open plan form');
  await page.screenshot({ path: path.join(output, 'agent-mobile.png') });

  // ---------- 6) 账号隔离：user2 看不到 admin 的 plans/turns ----------
  {
    const second = await browser.newContext({ viewport: { width: 390, height: 844 } });
    await second.addCookies([{ name: 'fixture_user', value: 'user2', url: base }]);
    const otherPage = await second.newPage();
    await otherPage.goto(base);
    await openAssistant(otherPage);
    await otherPage.getByText('今天有什么需要处理的？', { exact: true }).waitFor();
    assert.equal(await otherPage.locator('.turn').count(), 0, 'user2 must not see admin plan history');
    assert.equal(await otherPage.locator('.operation-plan').count(), 0, 'user2 must not see any operation plan');
    await otherPage.screenshot({ path: path.join(output, 'agent-isolation-user2.png') });
    await second.close();
  }

  assert.equal(pageErrors.length, 0, pageErrors.join('\n'));
  console.log('Assistant agent fixture checks passed: POST /api/assistant/agent lifecycle, multi-stage plan confirm + fixture write count (0→0→1), repeat-click protection, needs_input title fill + cancel (no write), file TXT/PNG attach + /api/assistant/files/{uuid} preview + draft removal + multi-image paste, reload retaining plans/turns, success/failure result visibility, desktop/mobile no-h-overflow + plan form/button visibility, account isolation (user2 sees no plans).');
} catch (error) {
  await page.screenshot({ path: path.join(output, 'agent-failure.png') }).catch(() => {});
  const text = await page.locator('.lighthouse').innerText().catch(() => 'assistant unavailable');
  console.error(text);
  throw error;
} finally {
  await browser.close();
}
