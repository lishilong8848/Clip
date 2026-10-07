import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

// 有限验收脚本：只针对 19003 上的隔离 preview（fixture_user cookie / synthetic model）。
// 规则：
//  - 只允许创建/修改本脚本，绝不触碰组件、后端、其他脚本、真实业务或凭证。
//  - 服务不接触真实多维；任何写入前先验证 /api/health.instance_id === 'isolated-lighthouse-stream'。
//  - 页面使用已构建 dist；默认账号是 D 楼（不设 cookie），cookie=ALL 为全权限，=E 为 E 楼。
//  - 每账号一会话；DELETE conversation 重置前必须先完成当前任务或显式 cancel。
//  - 不停止 19003、不启动其它浏览器窗口、不访问 18766。
const base = process.env.LIGHTHOUSE_STREAM_URL || 'http://127.0.0.1:19003';
const output = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../../output/playwright/assistant-stream');
await mkdir(output, { recursive: true });

const VW = 1440, VH = 1000;
const CAP_X = 160, CAP_Y = 140;
const PAGE_ERRORS = [];
const RESPONSES = [];

const browser = await chromium.launch({ headless: true });

// ---------- helpers ----------
async function assertIsolated(context) {
  const res = await context.request.get(base + '/api/health');
  assert.equal(res.ok(), true, 'isolated server health endpoint must respond 200 before any write');
  const body = await res.json();
  assert.equal(body.instance_id, 'isolated-lighthouse-stream', 'refuse to write against a non-isolated server');
  assert.equal(body.ok, true, 'isolated health must report ok');
}

async function resetConversation(context) {
  const conv = await context.request.get(base + '/api/assistant/conversation');
  if (conv.ok()) {
    const runId = (await conv.json()).data?.active_run_id;
    if (runId) {
      const cancelled = await context.request.post(base + `/api/assistant/runs/${encodeURIComponent(runId)}/cancel`,
        { data: {}, headers: { origin: base } });
      assert.equal(cancelled.ok(), true, 'stale active run must be cancellable before reset');
    }
  }
  const reset = await context.request.delete(base + '/api/assistant/conversation', { headers: { origin: base } });
  assert.equal(reset.ok(), true, 'conversation DELETE must succeed with matching Origin header before test');
}

function watchPages(context) {
  const pages = [];
  context.on('page', p => {
    pages.push(p);
    p.on('pageerror', e => PAGE_ERRORS.push(`[${p.url()}] ${e.message}`));
    p.on('response', async response => {
      if (!/\/api\/assistant\/(conversation|messages)$/.test(response.url())) return;
      const body = await response.json().catch(() => ({}));
      const data = body.data || {};
      RESPONSES.push({ path: new URL(response.url()).pathname, status: response.status(), busy: data.busy,
        run: data.active_run_id || data.run_id, error: body.error,
        turns: (data.turns || (data.turn ? [data.turn] : [])).map(t => ({ question: t.question, status: t.status, at: t.at, operation: t.operation_id, run: t.run_id })) });
    });
  });
  return pages;
}

function trackMessagesPosts(page) {
  let count = 0;
  page.on('request', r => { if (r.method() === 'POST' && r.url().includes('/api/assistant/messages')) count += 1; });
  return () => count;
}

async function openAssistant(page) {
  await page.locator('.lighthouse').first().waitFor({ state: 'attached' });
  if ((await page.locator('.assistant-panel').count()) === 0) {
    const btn = page.getByRole('button', { name: '打开灯塔助手', exact: true });
    await btn.waitFor();
    await btn.click();
  }
  await page.locator('.assistant-panel').waitFor();
  await page.getByRole('textbox', { name: '询问灯塔助手' }).waitFor();
}

async function closeAssistant(page) {
  const btn = page.getByRole('button', { name: '收起助手', exact: true });
  if (await btn.count()) {
    await btn.click();
    await page.locator('.assistant-panel').waitFor({ state: 'detached', timeout: 6000 }).catch(() => {});
  }
}

async function ask(page, text) {
  await page.locator('#assistant-question').fill(text);
  await page.getByRole('button', { name: '发送问题', exact: true }).click();
}

function lastTurn(page) {
  return page.locator('.turn').last();
}

async function lastAnswerLen(page) {
  const count = await page.locator('.turn').count();
  if (!count) return 0;
  const text = await page.locator('.turn').nth(count - 1).locator('.answer').innerText().catch(() => '');
  return text.length;
}

async function waitAnswerComplete(page, { timeout = 60000 } = {}) {
  await page.waitForFunction(() => {
    const turns = Array.from(document.querySelectorAll('.turn'));
    if (!turns.length) return false;
    const last = turns[turns.length - 1];
    const actions = Array.from(last.querySelectorAll('.message-actions button'));
    return last.querySelector('.used-model') !== null
      && actions.some(b => b.getAttribute('aria-label') === '重新生成回答');
  }, undefined, { timeout });
}

async function assertNoHorizontalOverflow(page, label) {
  const noDocOverflow = await page.evaluate(() =>
    document.documentElement.scrollWidth <= innerWidth + 1 && document.body.scrollWidth <= innerWidth + 1);
  assert(noDocOverflow, `${label}: document must not overflow horizontally`);
  const panelOverflow = await page.locator('.assistant-panel').evaluate(el => el.scrollWidth <= el.clientWidth + 1);
  assert(panelOverflow, `${label}: assistant panel must not overflow horizontally`);
}

async function assertBoxesDoNotOverlap(page, leftSel, rightSel, label) {
  const rb = await page.locator(rightSel).boundingBox();
  const lb = await page.locator(leftSel).boundingBox();
  assert(rb && lb, label + ': both controls must have measurable boxes');
  const overlap = !(lb.x + lb.width <= rb.x || rb.x + rb.width <= lb.x
    || lb.y + lb.height <= rb.y || rb.y + rb.height <= lb.y);
  assert(!overlap, `${label}: controls must not overlap (${JSON.stringify({ left: lb, right: rb })})`);
}

async function dragPanelTo(page, tx, ty) {
  const root = page.locator('.lighthouse');
  await root.waitFor();
  const c0 = await root.boundingBox();
  assert(c0, 'panel box required before drag');
  const header = page.locator('.assistant-header').first();
  await header.waitFor();
  const hb = await header.boundingBox();
  assert(hb, 'header box required before drag');
  const sx = hb.x + hb.width / 2, sy = hb.y + hb.height / 2;
  const dx = tx - c0.x, dy = ty - c0.y;
  await page.mouse.move(sx, sy);
  await page.mouse.down();
  await page.mouse.move(sx + dx, sy + dy, { steps: 10 });
  await page.mouse.up();
}

async function expectPanelAt(page, x, y, label) {
  await page.waitForFunction(({ ex, ey }) => {
    const el = document.querySelector('.lighthouse');
    if (!el) return false;
    const r = el.getBoundingClientRect();
    return Math.abs(r.x - ex) <= 2 && Math.abs(r.y - ey) <= 2;
  }, { ex: x, ey: y }, { timeout: 8000 });
}

async function assertLauncherBottomRight(page, label) {
  const lb = await page.locator('.assistant-launcher').boundingBox();
  assert(lb, label + ': launcher must be measurable');
  assert(
    Math.abs((lb.x + lb.width) - (VW - 24)) <= 8 && Math.abs((lb.y + lb.height) - (VH - 24)) <= 8,
    `${label}: launcher must stay at bottom-right, not jump to top (${JSON.stringify(lb)})`,
  );
}

try {
  // ---------- D 楼账号（默认未设 cookie）----------
  const dCtx = await browser.newContext({ viewport: { width: VW, height: VH } });
  await assertIsolated(dCtx);
  await resetConversation(dCtx);
  await watchPages(dCtx);
  const dPage = await dCtx.newPage();
  await dPage.goto(base);

  // ==== 验收 1：打开助手无可执行操作按钮 ====
  await openAssistant(dPage);
  assert.equal(await dPage.locator('.turn').count(), 0, 'fresh D conversation starts empty');
  assert.equal(await dPage.locator('.operation-plan').count(), 0, 'no operation plan before asking');
  assert.equal(await dPage.locator('.message-actions').count(), 0, 'no answer actions before asking');
  assert.equal(await dPage.locator('.retry').count(), 0, 'no retry button before asking');
  assert.equal(await dPage.locator('.interactions').count(), 0, 'no interaction buttons before asking');
  assert.equal(await dPage.getByRole('button', { name: '停止生成', exact: true }).count(), 0, 'no stop button before asking');
  const emptySend = dPage.getByRole('button', { name: '发送问题', exact: true });
  assert.equal(await emptySend.isDisabled(), true, 'send button disabled while input empty');
  console.log('[pass] 1) 打开助手（空会话）无可执行操作按钮');

  // ==== 验收 2：发送后输入立即清空、仍可编辑、流式未完成前可见 ====
  await ask(dPage, '快速长回答 汇总D楼今天的情况');
  await dPage.waitForFunction(() => document.querySelector('#assistant-question')?.value === '', undefined, { timeout: 8000 });
  assert.equal(await dPage.locator('#assistant-question').inputValue(), '', 'input must clear immediately after send');
  const inputEl = dPage.locator('#assistant-question');
  assert.equal(await inputEl.isEditable(), true, 'input must remain editable while streaming');
  // 流式未完成前可见：last turn 同时出现处理状态与部分回答文本
  await dPage.waitForFunction(() => {
    const turns = Array.from(document.querySelectorAll('.turn'));
    const last = turns[turns.length - 1];
    return last && last.querySelector('.pending, .process summary .spin') && /查询范围：D楼/.test(last.querySelector('.answer')?.textContent || '');
  }, undefined, { timeout: 15000 });
  await waitAnswerComplete(dPage);
  const dFirstAnswer = await lastTurn(dPage).locator('.answer').innerText();
  assert(dFirstAnswer.includes('查询范围：D楼'), 'D answer must be scoped to D楼');
  console.log('[pass] 2) 发送后输入清空且仍可编辑，流式内容在完成前可见');

  // ==== 验收 4：关闭/重开；展开状态刷新后关闭，launcher 坐标不跳到上方 ====
  await dragPanelTo(dPage, CAP_X, CAP_Y);
  await expectPanelAt(dPage, CAP_X, CAP_Y, 'panel after drag-to-cap');
  // 4a) 关闭 → 重开 → 关闭：面板位置保持，launcher 回到右下角
  await closeAssistant(dPage);
  const launcherBoxAfterClose = await dPage.locator('.assistant-launcher').boundingBox();
  assert(launcherBoxAfterClose, 'launcher box after first close');
  await assertLauncherBottomRight(dPage, 'close#1 launcher');
  await openAssistant(dPage);
  await expectPanelAt(dPage, CAP_X, CAP_Y, 'reopen keeps panel at cap');
  await closeAssistant(dPage);
  const launcherBoxAfterReopenClose = await dPage.locator('.assistant-launcher').boundingBox();
  assert(launcherBoxAfterReopenClose, 'launcher box after reopen-close');
  assert(Math.abs(launcherBoxAfterReopenClose.x - launcherBoxAfterClose.x) <= 2
    && Math.abs(launcherBoxAfterReopenClose.y - launcherBoxAfterClose.y) <= 2,
    `launcher must not jump after close/reopen (${JSON.stringify(launcherBoxAfterClose)} -> ${JSON.stringify(launcherBoxAfterReopenClose)})`);
  // 4b) 展开状态硬刷新后关闭：launcher 不跳到上方
  await openAssistant(dPage);
  await expectPanelAt(dPage, CAP_X, CAP_Y, 'panel before hard refresh');
  await dPage.reload({ waitUntil: 'load' });
  await dPage.locator('.assistant-panel').waitFor();
  await dPage.getByRole('textbox', { name: '询问灯塔助手' }).waitFor();
  await expectPanelAt(dPage, CAP_X, CAP_Y, 'panel reopens at cap after hard refresh');
  await closeAssistant(dPage);
  await assertLauncherBottomRight(dPage, 'launcher after refresh-while-open then close');
  const launcherAtEdge = dPage.getByRole('button', { name: '打开灯塔助手', exact: true });
  await launcherAtEdge.press('Alt+ArrowDown');
  await launcherAtEdge.press('Alt+ArrowDown');
  await launcherAtEdge.press('Alt+ArrowRight');
  await launcherAtEdge.press('Alt+ArrowRight');
  const edge = await launcherAtEdge.boundingBox();
  await openAssistant(dPage);
  const initialBox = await dPage.locator('.assistant-panel').boundingBox();
  assert(initialBox && Math.abs(initialBox.width - 620) < 2 && Math.abs(initialBox.height - 700) < 2,
    `PC default panel should be 620x700, got ${JSON.stringify(initialBox)}`);
  await dPage.getByRole('button', { name: '展开会话', exact: true }).click();
  await dPage.waitForFunction(() => document.querySelector('.assistant-panel')?.getBoundingClientRect().width >= 759);
  await dPage.getByRole('button', { name: '还原会话', exact: true }).click();
  await dPage.waitForFunction(() => Math.abs(document.querySelector('.assistant-panel')?.getBoundingClientRect().width - 620) < 2);
  await dPage.screenshot({ path: path.join(output, 'pc-default.png') });
  await dPage.reload({ waitUntil: 'load' });
  await dPage.locator('.assistant-panel').waitFor();
  await closeAssistant(dPage);
  const restoredEdge = await launcherAtEdge.boundingBox();
  assert(edge && restoredEdge && Math.abs(edge.x - restoredEdge.x) <= 2 && Math.abs(edge.y - restoredEdge.y) <= 2,
    'saved launcher at viewport edge must not be clamped using hidden panel/estimated dimensions');
  console.log('[pass] 4) 关闭/重开及展开状态硬刷新后关闭，launcher 坐标不跳到上方');

  // ==== 验收 8（桌面）：无横向溢出、控件不重叠，输出截图 ====
  await openAssistant(dPage);
  await assertNoHorizontalOverflow(dPage, 'desktop');
  await assertBoxesDoNotOverlap(dPage, '#assistant-question', '.composer-footer .send', 'desktop send vs textarea');
  await dPage.screenshot({ path: path.join(output, 'assistant-stream-desktop.png') });

  await dPage.setViewportSize({ width: 390, height: 844 });
  await dPage.waitForFunction(() => {
    const r = document.querySelector('.assistant-panel')?.getBoundingClientRect();
    return r && r.left >= 0 && r.top >= 0 && r.right <= innerWidth && r.bottom <= innerHeight;
  }, undefined, { timeout: 8000 });
  await assertNoHorizontalOverflow(dPage, 'mobile(390x844)');
  await assertBoxesDoNotOverlap(dPage, '#assistant-question', '.composer-footer .send', 'mobile send vs textarea');
  const mobileSend = await dPage.getByRole('button', { name: '发送问题', exact: true }).boundingBox();
  assert(mobileSend && mobileSend.x >= 0 && mobileSend.x + mobileSend.width <= 390,
    'mobile send button must fit viewport');
  await dPage.screenshot({ path: path.join(output, 'assistant-stream-mobile.png') });
  await dPage.setViewportSize({ width: VW, height: VH });
  console.log('[pass] 8) 桌面1440x1000与390x844无横向溢出、控件不重叠，已输出两张截图');

  // ==== 验收 7：D 楼账号不能问 E 楼，且不得显示 E 楼数据 ====
  await ask(dPage, '只看E楼的维修单');
  await dPage.waitForFunction(() => {
    const err = document.querySelector('.assistant-error');
    if (err && /无权/.test(err.textContent || '')) return true;
    const turns = Array.from(document.querySelectorAll('.turn'));
    const last = turns[turns.length - 1];
    return last && /无权/.test(last.textContent || '') && last.querySelector('.failure') !== null;
  }, undefined, { timeout: 15000 });
  const panelTextAfterPerm = await dPage.locator('.assistant-panel').innerText();
  assert(!panelTextAfterPerm.includes('查询范围：E楼'), 'D account must not render a E楼 scoped answer');
  assert(!/查询范围：.*E楼/.test(panelTextAfterPerm), 'D answer must not claim E楼 visibility');
  const failedCount = await lastTurn(dPage).locator('.failure').count();
  assert(failedCount >= 1, 'unauthorized D->E request must show failure');
  console.log('[pass] 7) D 楼账号询问 E 楼被拒，且不显示 E 楼数据');

  // ==== 验收 6：停止后有继续回答 ====
  await ask(dPage, '快速长回答 继续测试停止功能');
  await dPage.waitForFunction(() => {
    const turns = Array.from(document.querySelectorAll('.turn'));
    const last = turns[turns.length - 1];
    return last && last.querySelector('.answer') && /查询范围：D楼/.test(last.querySelector('.answer')?.textContent || '');
  }, undefined, { timeout: 15000 });
  const stopBtn = dPage.getByRole('button', { name: '停止生成', exact: true });
  await stopBtn.waitFor();
  await stopBtn.click();
  const retry = dPage.getByRole('button', { name: '继续回答', exact: true });
  await retry.waitFor({ timeout: 15000 });
  assert(await retry.isVisible(), 'stopped turn must expose 继续回答 button');
  await retry.click();
  await waitAnswerComplete(dPage);
  const continued = await lastTurn(dPage).locator('.answer').innerText();
  assert(continued.includes('查询范围：D楼'), 'continued answer must complete to D楼');
  assert(continued.length > 60, 'continued answer must contain substantial streamed content');
  console.log('[pass] 6) 停止后出现继续回答并可完成');

  await dCtx.close();

  // ---------- ALL 楼全权限账号 ----------
  const aCtx = await browser.newContext({ viewport: { width: VW, height: VH } });
  await aCtx.addCookies([{ name: 'fixture_user', value: 'ALL', url: base }]);
  await assertIsolated(aCtx);
  await resetConversation(aCtx);
  await watchPages(aCtx);
  const aPage = await aCtx.newPage();
  let allPosts = 0;
  aPage.on('request', r => { if (r.method() === 'POST' && r.url().includes('/api/assistant/messages')) allPosts += 1; });
  await aPage.goto(base);
  await openAssistant(aPage);

  // ==== 验收 3：全权限询问后中途追加只看D楼，最终答案仅D楼，旧回复不能覆盖 ====
  await ask(aPage, '快速长回答 汇总全楼情况');
  // 等待全权限范围正在流式呈现（证明一开始覆盖 E 等楼栋）
  await aPage.waitForFunction(() => {
    const turns = Array.from(document.querySelectorAll('.turn'));
    const last = turns[turns.length - 1];
    return last && last.querySelector('.pending, .process summary .spin') && /查询范围：.*E楼/.test(last.querySelector('.answer')?.textContent || '');
  }, undefined, { timeout: 20000 });
  const turnsBeforeSupplement = await aPage.locator('.turn').count();
  // 中途追加只看 D 楼
  await ask(aPage, '只看D楼');
  await aPage.waitForFunction(n => document.querySelectorAll('.turn').length === n + 1, turnsBeforeSupplement, { timeout: 10000 });
  await waitAnswerComplete(aPage);
  const allTurns = aPage.locator('.turn');
  const supplementTurn = allTurns.nth((await allTurns.count()) - 1);
  const finalAnswer = await supplementTurn.locator('.answer').innerText();
  assert(finalAnswer.includes('查询范围：D楼'), 'final answer must be D楼-only');
  assert(!/查询范围：.*(?:110楼|A楼|B楼|C楼|E楼|H楼)/.test(finalAnswer),
    `supplemented final answer must not contain other scopes (${finalAnswer})`);
  // 旧回复不得覆盖：旧 turn 保留自身全范围回答，未被追加的 D 楼答案覆盖
  const totalTurns = await allTurns.count();
  assert(totalTurns >= 2, 'supplement must produce at least two turns');
  const previousTurn = allTurns.nth(totalTurns - 2);
  const previousAnswer = await previousTurn.locator('.answer').innerText();
  const otherScopeRe = /(?:110楼|A楼|B楼|C楼|E楼|H楼)/;
  assert(otherScopeRe.test(previousAnswer), `previous full-scope answer must be retained, not overwritten (${previousAnswer})`);
  assert(previousAnswer !== finalAnswer, 'previous answer must differ from the D-only supplement answer');
  console.log('[pass] 3) 全权限中途只看D楼，最终仅D楼，旧回复不覆盖');

  // ==== 验收 5：回答中关闭/重开或硬刷新，能继续查看同一 run 且无重复 POST ====
  await ask(aPage, '快速长回答 继续测试恢复');
  await aPage.waitForFunction(() => {
    const turns = Array.from(document.querySelectorAll('.turn'));
    const last = turns[turns.length - 1];
    return last && last.querySelector('.answer') && /查询范围：/.test(last.querySelector('.answer')?.textContent || '');
  }, undefined, { timeout: 20000 });
  const baselinePosts = allPosts;
  // 5a) 关闭/重开
  const lenBeforeClose = await lastAnswerLen(aPage);
  await closeAssistant(aPage);
  await openAssistant(aPage);
  await aPage.waitForFunction(prev => {
    const turns = Array.from(document.querySelectorAll('.turn'));
    const last = turns[turns.length - 1];
    const text = last?.querySelector('.answer')?.textContent || '';
    return text.length > prev;
  }, lenBeforeClose, { timeout: 15000 });
  assert.equal(allPosts, baselinePosts, 'reopen while streaming must not POST again (/api/assistant/messages)');
  // 5b) 硬刷新（展开态）
  const lenBeforeReload = await lastAnswerLen(aPage);
  await aPage.reload({ waitUntil: 'load' });
  await aPage.locator('.assistant-panel').waitFor();
  await aPage.getByRole('textbox', { name: '询问灯塔助手' }).waitFor();
  await aPage.waitForFunction(prev => {
    const turns = Array.from(document.querySelectorAll('.turn'));
    const last = turns[turns.length - 1];
    const text = last?.querySelector('.answer')?.textContent || '';
    return text.length > prev;
  }, lenBeforeReload, { timeout: 15000 });
  assert.equal(allPosts, baselinePosts, 'hard refresh while streaming must not POST again (/api/assistant/messages)');
  await waitAnswerComplete(aPage);
  const recovered = await lastAnswerLen(aPage);
  assert(recovered > lenBeforeReload, 'recovered run must continue producing content to completion');
  console.log('[pass] 5) 回答中关闭/重开及硬刷新均继续同一 run，无重复 POST');

  await aCtx.close();

  assert.deepEqual(PAGE_ERRORS, [], PAGE_ERRORS.join('\n'));
  console.log('Assistant stream browser acceptance PASSED: input cleared+editable, streaming visible, D-only supplement with no overwrite, launcher position stable, same-run resume without duplicate POST, stop->continue, D-scope isolation, desktop/mobile containment + screenshots, no pageerror.');
} catch (error) {
  console.error(error);
  console.error(JSON.stringify(RESPONSES.slice(-16), null, 2));
  const outputDir = output;
  for (const p of browser.contexts().flatMap(ctx => ctx.pages())) {
    await p.screenshot({ path: path.join(outputDir, 'assistant-stream-failure.png') }).catch(() => {});
    const text = await p.locator('.lighthouse').innerText().catch(() => 'assistant unavailable');
    console.error(text);
    break;
  }
  throw error;
} finally {
  await browser.close();
}
