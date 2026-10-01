import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

// 有限验收脚本：只针对 19003 上的隔离 preview（fixture_user cookie / synthetic model）。
// 覆盖本次有界修复目标：拖启动器 → 打开面板 → 收起 → 再拖的“闪跳到上一个/下一个位置”根因。
// 根因保护点（组件侧）：
//  - openAssistant 在 delayed conversation GET 返回后的尾部 ensureInBounds('panel') 必须带
//    open/epoch 守卫；close 的 nextTick(applyShapePosition('launcher')) 同样带守卫，避免陈旧回写。
//  - 手势绑定 beginDrag 时的形状（dragShape），move/end 只在 dragShape === activeShape() 时写坐标；
//    切换形状前先 stopGesture，避免把旧形状坐标写进当前元素。
//
// 规则（沿用 check-assistant-stream-browser.mjs）：
//  - 服务不接触真实多维；任何写入前先验证 /api/health.instance_id === 'isolated-lighthouse-stream'。
//  - 页面使用已构建 dist（本修复合入后需重新构建 dist 再运行本脚本）。
//  - 每账号一会话；DELETE conversation 重置前必须先 cancel 未完成的 active run。
//  - 不停止 19003、不启动其它常驻服务、不访问 18766、不发云请求、不使用真实凭证。
const base = process.env.LIGHTHOUSE_STREAM_URL || 'http://127.0.0.1:19003';
const output = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../../output/playwright/assistant-drag');
await mkdir(output, { recursive: true });

const VW = 1440, VH = 1000;
const PAGE_ERRORS = [];

// 每轮拖动使用不同目标，确保每一轮都是真实的“再来一次”而非仅最终坐标。
const PANEL_TARGETS = [
  { x: 200, y: 160 },
  { x: 320, y: 240 },
  { x: 150, y: 300 },
];
const LAUNCHER_TARGETS = [
  { x: 420, y: 320 },
  { x: 520, y: 200 },
  { x: 340, y: 430 },
];

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

async function openBox(page) {
  await page.locator('.lighthouse').first().waitFor();
  const b = await page.locator('.lighthouse').boundingBox();
  assert(b, '.lighthouse bounding box required');
  return b;
}

async function openAssistant(page) {
  await page.locator('.lighthouse').first().waitFor();
  if ((await page.locator('.assistant-panel').count()) === 0) {
    await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
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

async function expectAt(page, target, label) {
  await page.waitForFunction(({ x, y }) => {
    const el = document.querySelector('.lighthouse');
    if (!el) return false;
    const r = el.getBoundingClientRect();
    return Math.abs(r.x - x) <= 5 && Math.abs(r.y - y) <= 5;
  }, { x: target.x, y: target.y }, { timeout: 8000 });
}

// 点到线段（[a,b]）的直线距离：拖动过程中面板/启动器应严格沿 origin->target 直线滑动，
// 任何“闪跳到另一形状坐标”都会偏离该线段而被捕获（不只断言最终坐标）。
function distToSegment(p, a, b) {
  const vx = b.x - a.x, vy = b.y - a.y;
  const len2 = vx * vx + vy * vy;
  if (!len2) return Math.hypot(p.x - a.x, p.y - a.y);
  let t = ((p.x - a.x) * vx + (p.y - a.y) * vy) / len2;
  t = Math.max(0, Math.min(1, t));
  const px = a.x + t * vx, py = a.y + t * vy;
  return Math.hypot(p.x - px, p.y - py);
}

// 连续 real mouse 拖动采样：真实 mouse down/move/up（走真实 pointer capture / click 路径），
// 逐帧移动、逐帧读取 DOM 位置，并在移动过程中断言线性轨迹。
async function dragWithSamples(page, handleSel, targetX, targetY, label) {
  const hb = await page.locator(handleSel).first().boundingBox();
  assert(hb, `${label}: drag handle box required`);
  const start = await openBox(page);
  const gx = hb.x + hb.width / 2, gy = hb.y + hb.height / 2;
  const dx = targetX - start.x, dy = targetY - start.y;
  const steps = 14;
  const samples = [];
  await page.mouse.move(gx, gy);
  await page.mouse.down();
  for (let i = 1; i <= steps; i++) {
    await page.mouse.move(gx + dx * i / steps, gy + dy * i / steps);
    await page.evaluate(() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))));
    const b = await openBox(page);
    samples.push({ x: b.x, y: b.y });
  }
  await page.mouse.up();
  await page.waitForTimeout(40);
  const end = await openBox(page);
  // 除首采样（drag 尚未越过 6px 阈值）外，均须贴近直线路径。
  for (let i = 1; i < samples.length; i++) {
    const d = distToSegment(samples[i], start, { x: targetX, y: targetY });
    assert(d < 16, `${label}: sample[${i}] (${samples[i].x},${samples[i].y}) deviated ${d.toFixed(1)}px from straight drag path ${JSON.stringify(start)} -> (${targetX},${targetY})`);
  }
  assert(Math.abs(end.x - targetX) <= 4 && Math.abs(end.y - targetY) <= 4,
    `${label}: drag must settle at target (${JSON.stringify(end)} vs (${targetX},${targetY}))`);
  return samples;
}

// ---------- scenarios ----------
// A. 真实 delayed conversation GET + 中途收起：launcher 不得闪跳到面板坐标。
async function scenarioDelayedGetEarlyClose(page) {
  const launcherBtn = page.getByRole('button', { name: '打开灯塔助手', exact: true });
  await launcherBtn.waitFor();
  const L0 = await openBox(page);
  // 真实延迟针对 conversation GET；其它路径（health 等）不受影响。
  await launcherBtn.click();
  await page.locator('.assistant-panel').waitFor({ timeout: 5000 });
  await page.getByRole('button', { name: '收起助手', exact: true }).click();
  await page.locator('.assistant-panel').waitFor({ state: 'detached', timeout: 5000 }).catch(() => {});
  // 跨过 delayed GET 返回窗口持续采样 launcher，绝不能出现闪跳到面板位置的瞬间。
  const samples = [];
  const deadline = Date.now() + 1400;
  while (Date.now() < deadline) {
    const b = await openBox(page);
    samples.push({ x: b.x, y: b.y });
    await page.waitForTimeout(60);
  }
  assert(samples.length >= 10, 'must sample launcher across the delayed GET window');
  for (const s of samples) {
    assert(Math.abs(s.x - L0.x) <= 4 && Math.abs(s.y - L0.y) <= 4,
      `launcher flashed away from default during delayed GET after early close (${JSON.stringify(L0)} vs ${JSON.stringify(s)})`);
  }
  assert.equal(await page.locator('.assistant-panel').count(), 0, 'panel must stay closed');
  console.log('[pass] A) delayed conversation GET + early close: launcher keeps its own position (no flash)');
}

// B. 重复 open-close-drag，连续 pointer move 采样 + 每形态独立坐标。
async function scenarioRepeatedToggleDrag(page, launcherDefault) {
  for (let i = 1; i <= 3; i++) {
    const tag = `cycle#${i}`;
    const pt = PANEL_TARGETS[i - 1];
    const lt = LAUNCHER_TARGETS[i - 1];
    await openAssistant(page);
    await dragWithSamples(page, '.assistant-header', pt.x, pt.y, `${tag} panel drag`);
    await expectAt(page, pt, `${tag} panel after drag`);
    await closeAssistant(page);
    // 收起后 launcher 应保持“上一轮启动器坐标”（第一轮为默认右下角），而非误用面板坐标。
    const launcherExpected = i === 1 ? launcherDefault : LAUNCHER_TARGETS[i - 2];
    await expectAt(page, launcherExpected, `${tag} launcher after close`);
    // 重开：面板必须保留自己的坐标。
    await openAssistant(page);
    await expectAt(page, pt, `${tag} reopen keeps panel position`);
    await closeAssistant(page);
    // 拖启动器到本轮目标。
    await dragWithSamples(page, '.assistant-launcher', lt.x, lt.y, `${tag} launcher drag`);
    await expectAt(page, lt, `${tag} launcher after drag`);
    // 再开面板：launcher 拖动不得污染面板坐标。
    await openAssistant(page);
    await expectAt(page, pt, `${tag} panel retained after launcher drag`);
    await closeAssistant(page);
    await expectAt(page, lt, `${tag} launcher retained after close`);
    console.log(`[pass] B) ${tag} open-close-drag: panel@(${pt.x},${pt.y}) launcher@(${lt.x},${lt.y}) isolated & straight`);
  }
}

// C. pointercancel/blur 清理手势后，坐标不被回写/不闪跳，且后续 fresh drag 仍有效。
async function scenarioMidDragPointerCancelBlur(page) {
  await openAssistant(page);
  await closeAssistant(page);
  const btn = page.getByRole('button', { name: '打开灯塔助手', exact: true });
  await btn.waitFor();
  const b = await btn.boundingBox();
  assert(b, 'launcher box required for blur-drag');
  const gx = b.x + b.width / 2, gy = b.y + b.height / 2;
  // 真实 mouse down + 移动一段距离（触发真实 pointer capture），随后 blur 触发 pointercancel 收尾。
  await page.mouse.move(gx, gy);
  await page.mouse.down();
  await page.mouse.move(gx - 90, gy - 70, { steps: 6 });
  await page.evaluate(() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))));
  const mid = await openBox(page);
  // window blur -> stopGesture -> onDragEnd(pointercancel)；手势绑定 launcher，未切换形状，允许正常收尾。
  await page.evaluate(() => window.dispatchEvent(new FocusEvent('blur')));
  await page.waitForTimeout(40);
  const after = await openBox(page);
  assert(Math.abs(after.x - mid.x) <= 4 && Math.abs(after.y - mid.y) <= 4,
    `pointercancel/blur must not jump the launcher (${JSON.stringify(mid)} -> ${JSON.stringify(after)})`);
  // blur 收尾后释放物理鼠标键，确保下一轮真实 down 从干净状态开始。
  await page.mouse.up();
  await page.waitForTimeout(20);

  // fresh drag 仍有效：读取 blur 之后最新 handle 坐标，真实按下并拖动已知增量，
  // 以“blur 后坐标 + 增量”为期望，验证这轮拖动手势确实重新开始并生效（而非沿用旧起点）。
  const freshBtn = page.getByRole('button', { name: '打开灯塔助手', exact: true });
  const fb = await freshBtn.boundingBox();
  assert(fb, 'fresh launcher box required');
  const fgx = fb.x + fb.width / 2, fgy = fb.y + fb.height / 2;
  const afterBlur = await openBox(page);
  const deltaX = -170, deltaY = -120;
  await page.mouse.move(fgx, fgy);
  await page.mouse.down();
  await page.mouse.move(fgx + deltaX, fgy + deltaY, { steps: 8 });
  await page.mouse.up();
  await page.waitForTimeout(40);
  const moved = await openBox(page);
  assert(Math.abs(moved.x - (afterBlur.x + deltaX)) <= 5 && Math.abs(moved.y - (afterBlur.y + deltaY)) <= 5,
    `fresh drag after blur must move by known delta from after-blur coordinate (${JSON.stringify(afterBlur)} + (${deltaX},${deltaY}) -> ${JSON.stringify(moved)})`);
  console.log('[pass] C) pointercancel/blur mid-drag: no jump, fresh drag (real mouse, known delta) works');
}

try {
  const context = await browser.newContext({ viewport: { width: VW, height: VH } });
  await assertIsolated(context);
  await context.addCookies([{ name: 'fixture_user', value: 'A', url: base }]);
  await resetConversation(context);
  context.on('page', p => p.on('pageerror', e => PAGE_ERRORS.push(`[${p.url()}] ${e.message}`)));

  const page = await context.newPage();
  // 真实 delayed conversation GET：仅对 GET 请求延迟，仍让后端真实处理（continue 前 sleep）。
  let delayConversationUntil = 0;
  await page.route('**/api/assistant/conversation', async (route) => {
    const req = route.request();
    if (req.method() === 'GET' && delayConversationUntil > Date.now()) {
      await new Promise(r => setTimeout(r, delayConversationUntil - Date.now()));
    }
    try { await route.continue(); } catch { /* 请求可能已被 abort（提前收起） */ }
  });

  await page.goto(base);
  await page.getByRole('button', { name: '打开灯塔助手', exact: true }).waitFor();
  await page.waitForTimeout(80);
  const launcherDefault = await openBox(page);

  delayConversationUntil = Date.now() + 900;
  await scenarioDelayedGetEarlyClose(page);

  delayConversationUntil = 0;
  await scenarioRepeatedToggleDrag(page, launcherDefault);

  await scenarioMidDragPointerCancelBlur(page);

  await page.screenshot({ path: path.join(output, 'assistant-drag-final.png') });
  assert.deepEqual(PAGE_ERRORS, [], PAGE_ERRORS.join('\n'));
  console.log('Assistant drag regression PASSED: delayed GET + early close (no launcher flash), repeated open-close-drag with continuous pointer-move sample linearity, per-shape position isolation, pointercancel/blur cleanup, no pageerror.');
} catch (error) {
  console.error(error);
  for (const p of browser.contexts().flatMap(ctx => ctx.pages())) {
    await p.screenshot({ path: path.join(output, 'assistant-drag-failure.png') }).catch(() => {});
    const text = await p.locator('.lighthouse').innerText().catch(() => 'assistant unavailable');
    console.error(text);
    break;
  }
  throw error;
} finally {
  await browser.close();
}
