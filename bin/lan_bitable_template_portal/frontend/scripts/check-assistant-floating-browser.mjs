import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

// 隔离预览服务器契约：只测试 instance_id === 'isolated-ai-preview' 的实例。
// fixture_user cookie：admin 为默认身份（不设 cookie），user2 为另一身份（非白名单）。
const base = process.env.ASSISTANT_PREVIEW_URL || 'http://127.0.0.1:19002';
const output = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../../output/playwright/assistant-floating');
await mkdir(output, { recursive: true });

const browser = await chromium.launch({ headless: true });
let page = null;
let agentPosts = 0;
const errors = [];

const VW = 1440;
const VH = 1000;
const CAP_X = 160;
const CAP_Y = 140;
const RIGHT_MARGIN = 12, BOTTOM_MARGIN = 96;
const ROUTES = [
  '/learning',
  '/cabinet-power?scope=E',
  '/repair-management?scope=A',
  '/plan-convergence',
  '/workbench-lite?scope=E&work_type=maintenance',
  '/drill-management',
];

async function assertDefaultLauncherBottomRight(pageLike) {
  const lb = await pageLike.locator('.lighthouse').boundingBox();
  assert(lb, 'launcher bounding box required');
  assert(
    Math.abs((lb.x + lb.width) - (VW - RIGHT_MARGIN)) <= 8 &&
    Math.abs((lb.y + lb.height) - (VH - BOTTOM_MARGIN)) <= 8,
    `default launcher must sit at bottom-right (${JSON.stringify(lb)})`,
  );
  assert(lb.width <= 40 && lb.height < 100,
    `default launcher must use its own small size (${JSON.stringify(lb)})`);
}

async function dragContainerTo(pageLike, targetX, targetY, label = 'assistant container') {
  const container = pageLike.locator('.lighthouse');
  await container.waitFor();
  const c0 = await container.boundingBox();
  assert(c0, `${label} box required before drag-to-cap`);
  if (Math.abs(c0.x - targetX) <= 2 && Math.abs(c0.y - targetY) <= 2) return;
  const header = pageLike.locator('.assistant-header').first();
  const hb = await header.boundingBox();
  assert(hb, 'assistant header box required before drag-to-cap');
  const sx = hb.x + hb.width / 2;
  const sy = hb.y + hb.height / 2;
  const dx = targetX - c0.x;
  const dy = targetY - c0.y;
  // 直接拖动：移动超过 6px 立即生效，不等待长按。
  await pageLike.mouse.move(sx, sy);
  await pageLike.mouse.down();
  await pageLike.mouse.move(sx + dx, sy + dy, { steps: 8 });
  await pageLike.mouse.up();
}

try {
  // 浏览器资源（context/page/监听器与健康检查）全部置于 cleanup 作用域内，
  // 任何一步失败都会走到 finally 关闭 Chromium，绝不泄漏。
  const context = await browser.newContext({ viewport: { width: 1440, height: 1000 } });
  // 任何操作前必须确认这是隔离预览实例。
  {
    const health = await context.request.get(base + '/api/health');
    assert.equal(health.ok(), true, 'isolated server health endpoint must respond 200 before any operation');
    const healthBody = await health.json();
    assert.equal(healthBody.instance_id, 'isolated-ai-preview', 'refuse to run against a non-isolated server');
    assert.equal(healthBody.ok, true, 'isolated health must report ok');
  }
  page = await context.newPage();
  page.on('request', r => {
    if (r.method() === 'POST' && r.url().includes('/api/assistant/agent')) agentPosts++;
  });
  page.on('pageerror', e => errors.push(e.message));

  // 1) 默认收起启动器固定在右下角；点击打开恰好一次。
  await page.goto(base);
  await page.getByRole('button', { name: '打开灯塔助手', exact: true }).waitFor();
  await page.waitForTimeout(60);
  await assertDefaultLauncherBottomRight(page);
  assert.equal(await page.locator('.assistant-panel').count(), 0, 'must start collapsed');
  await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
  await page.getByRole('textbox', { name: '询问灯塔助手' }).waitFor();
  assert.equal(await page.locator('.assistant-panel').count(), 1, 'short click must open exactly once');
  assert.equal(await page.getByRole('button', { name: '收起助手', exact: true }).count(), 1,
    'open panel header must show collapse button');

  // 2) 桌面 1440x1000 下用 header 直接鼠标拖动面板到安全位置 (160,140)。
  await dragContainerTo(page, CAP_X, CAP_Y, 'panel header');
  const capBox = await page.locator('.lighthouse').boundingBox();
  assert(capBox, 'container box required after drag to cap');
  assert(
    Math.abs(capBox.x - CAP_X) <= 2 && Math.abs(capBox.y - CAP_Y) <= 2,
    `panel must sit at cap (${CAP_X},${CAP_Y}) got (${capBox.x},${capBox.y})`,
  );

  // 3) 真实 page.goto 遍历路由：面板保持打开、位置恢复且不得 >2px 跳动。
  for (const route of ROUTES) {
    await page.goto(base + route);
    await page.waitForFunction(() => {
      const el = document.querySelector('.assistant-panel');
      return el !== null;
    }, undefined, { timeout: 6000 });
    await page.waitForFunction(({ x, y }) => {
      const el = document.querySelector('.lighthouse');
      if (!el) return false;
      const r = el.getBoundingClientRect();
      return Math.abs(r.x - x) <= 2 && Math.abs(r.y - y) <= 2;
    }, { x: CAP_X, y: CAP_Y }, { timeout: 6000 });
  }

  // 3.5) 全页 reload 保持打开态与位置（full page load 的最强形式）。
  await page.reload();
  await page.waitForFunction(() => {
    const el = document.querySelector('.assistant-panel');
    return el !== null;
  }, undefined, { timeout: 6000 });
  await page.waitForFunction(({ x, y }) => {
    const el = document.querySelector('.lighthouse');
    if (!el) return false;
    const r = el.getBoundingClientRect();
    return Math.abs(r.x - x) <= 2 && Math.abs(r.y - y) <= 2;
  }, { x: CAP_X, y: CAP_Y }, { timeout: 6000 });

  // 4) SPA 导航：history.pushState 后派发 popstate，同账号下面板保持打开且位置不跳动。
  await page.evaluate(() => {
    history.pushState({}, '', '/cabinet-power?scope=E');
    window.dispatchEvent(new PopStateEvent('popstate'));
  });
  await page.waitForFunction(() => document.querySelector('.assistant-panel') !== null, undefined, { timeout: 6000 });
  await page.waitForFunction(() => new URLSearchParams(window.location.search).get('scope') === 'E', undefined, { timeout: 6000 });
  const boxAfterSpa = await page.locator('.lighthouse').boundingBox();
  assert(
    Math.abs(boxAfterSpa.x - CAP_X) <= 2 && Math.abs(boxAfterSpa.y - CAP_Y) <= 2,
    `SPA popstate must keep open panel at cap (${JSON.stringify(boxAfterSpa)})`,
  );

  // 5) 全程不发送模型消息，导航不得重发业务请求（POST /api/assistant/agent 必须保持 0）。
  assert.equal(agentPosts, 0, 'navigation must never resubmit business requests via POST /api/assistant/agent');

  // 6) 高 z-index：assistant 必须位于普通页面遮罩之上；合成 fixed 遮罩 z-index 5000 时
  //    elementFromPoint 必须命中 assistant header 子元素（而非遮罩本身），随后移除。
  {
    const header = page.locator('.assistant-header').first();
    await header.waitFor();
    const hb = await header.boundingBox();
    assert(hb, 'header box required for z-index check');
    const hit = await page.evaluate(({ x, y, w, h }) => {
      const mask = document.createElement('div');
      mask.id = 'floating-z-mask';
      mask.style.cssText = `position:fixed;left:${x}px;top:${y}px;width:${w}px;height:${h}px;z-index:5000;background:rgba(0,0,0,0.3);pointer-events:auto;`;
      document.documentElement.appendChild(mask);
      const el = document.elementFromPoint(x + w / 2, y + h / 2);
      let insideHeader = false;
      for (let node = el; node && node !== document.documentElement; node = node.parentElement) {
        if (node.classList && node.classList.contains('assistant-header')) { insideHeader = true; break; }
      }
      mask.remove();
      return insideHeader;
    }, { x: hb.x, y: hb.y, w: hb.width, h: hb.height });
    assert.equal(hit, true, 'assistant header must sit ABOVE a z-index 5000 synthetic mask (elementFromPoint must hit an assistant header child)');
    await page.evaluate(() => document.getElementById('floating-z-mask')?.remove());
  }

  // 7) 收起态启动器直接用鼠标拖动（移动超过 6px 立即生效，无 350ms 等待）。
  {
    await page.getByRole('button', { name: '收起助手', exact: true }).click();
    await page.getByRole('button', { name: '打开灯塔助手', exact: true }).waitFor();
    assert.equal(await page.locator('.assistant-panel').count(), 0, 'launcher gesture must start from a closed panel');
    const lb = await page.locator('.lighthouse').boundingBox();
    assert(lb, 'launcher box required for direct drag');
    const lx = lb.x + lb.width / 2;
    const ly = lb.y + lb.height / 2;
    await page.mouse.move(lx, ly);
    await page.mouse.down();
    await page.mouse.move(lx - 80, ly - 60, { steps: 6 }); // 无 waitForTimeout，立即拖动
    await page.mouse.up();
    const lbAfter = await page.locator('.lighthouse').boundingBox();
    assert(lbAfter, 'launcher must remain measurable after direct drag');
    assert(
      Math.abs(lbAfter.x - lb.x) > 2 || Math.abs(lbAfter.y - lb.y) > 2,
      `launcher must move with immediate >6px drag (${JSON.stringify(lb)} -> ${JSON.stringify(lbAfter)})`,
    );
    assert.equal(await page.locator('.assistant-panel').count(), 0, 'launcher drag must not open the panel');
  }

  // 8) 收起状态跨 page.goto 与 reload 保持（关闭持久化）。
  {
    await page.goto(base + '/drill-management');
    await page.getByRole('button', { name: '打开灯塔助手', exact: true }).waitFor();
    assert.equal(await page.locator('.assistant-panel').count(), 0, 'closed panel must persist across page.goto');
    await page.reload();
    await page.getByRole('button', { name: '打开灯塔助手', exact: true }).waitFor();
    assert.equal(await page.locator('.assistant-panel').count(), 0, 'closed panel must persist across full page reload');
  }

  // 9) 不同身份（fixture_user=user2）不得复用旧位置/打开态/会话。
  {
    const second = await browser.newContext({ viewport: { width: 1440, height: 1000 } });
    await second.addCookies([{ name: 'fixture_user', value: 'user2', url: base }]);
    const otherPage = await second.newPage();
    await otherPage.goto(base + '/drill-management');
    await otherPage.getByRole('button', { name: '打开灯塔助手', exact: true }).waitFor();
    assert.equal(await otherPage.locator('.assistant-panel').count(), 0, 'different identity must start collapsed');
    const lbOther = await otherPage.locator('.lighthouse').boundingBox();
    assert(lbOther, 'user2 launcher box required');
    assert(
      Math.abs((lbOther.x + lbOther.width) - (VW - RIGHT_MARGIN)) <= 8 &&
      Math.abs((lbOther.y + lbOther.height) - (VH - BOTTOM_MARGIN)) <= 8,
      `different identity must get default bottom-right launcher, not reused cap position (${JSON.stringify(lbOther)})`,
    );
    await otherPage.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
    await otherPage.getByRole('textbox', { name: '询问灯塔助手' }).waitFor();
    assert.equal(await otherPage.locator('.turn').count(), 0, 'different identity must not reuse chat history');
    const otherOpenBox = await otherPage.locator('.lighthouse').boundingBox();
    assert(
      Math.abs(otherOpenBox.x - CAP_X) > 2 || Math.abs(otherOpenBox.y - CAP_Y) > 2,
      `different identity must not reuse prior open position (${JSON.stringify(otherOpenBox)})`,
    );
    await second.close();
  }

  // 10) 触屏长按拖动依然工作；拖动后的合成 click 必须被抑制。
  {
    await page.goto(base + '/learning');
    await page.getByRole('button', { name: '打开灯塔助手', exact: true }).waitFor();
    const tb = page.getByRole('button', { name: '打开灯塔助手', exact: true });
    const box0 = await tb.boundingBox();
    assert(box0, 'launcher box required for touch longpress drag');
    const tx = box0.x + box0.width / 2;
    const ty = box0.y + box0.height / 2;
    await page.evaluate(({ x, y }) => {
      document.elementFromPoint(x, y)?.dispatchEvent(new PointerEvent('pointerdown', {
        bubbles: true, cancelable: true, pointerId: 11, pointerType: 'touch', isPrimary: true,
        clientX: x, clientY: y, button: 0, buttons: 1,
      }));
    }, { x: tx, y: ty });
    await page.waitForTimeout(430);
    await page.evaluate(({ x, y }) => {
      document.dispatchEvent(new PointerEvent('pointermove', {
        bubbles: true, cancelable: true, pointerId: 11, pointerType: 'touch', isPrimary: true,
        clientX: x, clientY: y, buttons: 1,
      }));
    }, { x: tx - 80, y: ty - 60 });
    await page.evaluate(({ x, y }) => {
      document.dispatchEvent(new PointerEvent('pointerup', {
        bubbles: true, cancelable: true, pointerId: 11, pointerType: 'touch', isPrimary: true,
        clientX: x, clientY: y, button: 0, buttons: 0,
      }));
      document.elementFromPoint(x, y)?.dispatchEvent(new MouseEvent('click', {
        bubbles: true, cancelable: true, clientX: x, clientY: y,
      }));
    }, { x: tx - 80, y: ty - 60 });
    await page.waitForTimeout(60);
    assert.equal(await page.locator('.assistant-panel').count(), 0, 'touch longpress drag must not open / synthetic click suppressed');
    const boxTAfter = await tb.boundingBox();
    assert(boxTAfter, 'launcher must remain measurable after touch drag');
    assert(
      Math.abs(boxTAfter.x - box0.x) > 2 || Math.abs(boxTAfter.y - box0.y) > 2,
      'touch longpress drag must move the launcher',
    );
  }

  // 11) pointercancel 清理手势：cancel 后短点击仍恰好打开一次。
  {
    const tb2 = page.getByRole('button', { name: '打开灯塔助手', exact: true });
    const b2 = await tb2.boundingBox();
    assert(b2, 'launcher box required for pointercancel test');
    const cx = b2.x + b2.width / 2;
    const cy = b2.y + b2.height / 2;
    await page.evaluate(({ x, y }) => {
      document.elementFromPoint(x, y)?.dispatchEvent(new PointerEvent('pointerdown', {
        bubbles: true, cancelable: true, pointerId: 12, pointerType: 'touch', isPrimary: true,
        clientX: x, clientY: y, button: 0, buttons: 1,
      }));
    }, { x: cx, y: cy });
    await page.waitForTimeout(430);
    await page.evaluate(({ x, y }) => {
      document.dispatchEvent(new PointerEvent('pointercancel', {
        bubbles: true, cancelable: true, pointerId: 12, pointerType: 'touch', isPrimary: true,
        clientX: x, clientY: y, buttons: 0,
      }));
    }, { x: cx, y: cy });
    await page.waitForTimeout(40);
    assert.equal(await page.locator('.assistant-panel').count(), 0, 'pointercancel must clean the pending touch gesture');
    await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
    await page.getByRole('textbox', { name: '询问灯塔助手' }).waitFor();
    assert.equal(await page.locator('.assistant-panel').count(), 1, 'short click after pointercancel must open exactly once');
  }

  // 12) mouse blur 清理拖动手势后，fresh drag 依然有效。
  {
    await page.getByRole('button', { name: '收起助手', exact: true }).click();
    await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
    await page.getByRole('textbox', { name: '询问灯塔助手' }).waitFor();
    const header = page.locator('.assistant-header').first();
    const hb0 = await header.boundingBox();
    assert(hb0, 'header box required for blur cleanup');
    await page.mouse.move(hb0.x + 40, hb0.y + hb0.height / 2);
    await page.mouse.down();
    await page.evaluate(() => {
      document.querySelector('.assistant-header')?.dispatchEvent(new FocusEvent('blur'));
      window.dispatchEvent(new FocusEvent('blur'));
    });
    await page.mouse.up();
    const beforeBlurDrag = await page.locator('.lighthouse').boundingBox();
    assert(beforeBlurDrag, 'container box required before fresh drag');
    const hb1 = await page.locator('.assistant-header').first().boundingBox();
    assert(hb1, 'header box required for fresh drag');
    await page.waitForTimeout(30);
    await page.mouse.move(hb1.x + 40, hb1.y + hb1.height / 2);
    await page.mouse.down();
    await page.mouse.move(hb1.x + 40 - 120, hb1.y + hb1.height / 2 - 50, { steps: 8 });
    await page.mouse.up();
    const afterBlurDrag = await page.locator('.lighthouse').boundingBox();
    assert(afterBlurDrag, 'container box required after fresh drag');
    assert(
      Math.abs(afterBlurDrag.x - beforeBlurDrag.x) > 2 || Math.abs(afterBlurDrag.y - beforeBlurDrag.y) > 2,
      'fresh drag after mouse blur must still move the panel',
    );
  }

  // 13) 小视口：面板必须被夹紧在视口内，且 body/document 不得产生水平溢出。
  {
    await page.setViewportSize({ width: 390, height: 844 });
    await page.waitForFunction(() => {
      const r = document.querySelector('.assistant-panel')?.getBoundingClientRect();
      return r && r.left >= 0 && r.top >= 0 && r.right <= 390 && r.bottom <= 844;
    }, undefined, { timeout: 6000 });
    const smallBox = await page.locator('.assistant-panel').boundingBox();
    assert(smallBox, 'small-viewport panel box required');
    assert(
      smallBox.x >= 0 && smallBox.y >= 0 &&
      smallBox.x + smallBox.width <= 390 && smallBox.y + smallBox.height <= 844,
      `panel must be clamped on small viewport (${JSON.stringify(smallBox)})`,
    );
    const noOverflow = await page.evaluate(() =>
      document.documentElement.scrollWidth <= innerWidth + 1 && document.body.scrollWidth <= innerWidth + 1);
    assert(noOverflow, 'small viewport must not create horizontal overflow');
    const headerS = page.locator('.assistant-header').first();
    const hs = await headerS.boundingBox();
    assert(hs, 'header box required for clamp drag');
    await page.mouse.move(hs.x + 40, hs.y + hs.height / 2);
    await page.mouse.down();
    await page.mouse.move(1000, 1000, { steps: 8 }); // 拖出视口
    await page.mouse.up();
    const clamped = await page.locator('.assistant-panel').boundingBox();
    assert(clamped, 'clamped panel box required');
    assert(
      clamped.x >= 0 && clamped.y >= 0 &&
      clamped.x + clamped.width <= 390 && clamped.y + clamped.height <= 844,
      `panel must clamp when dragged out on small viewport (${JSON.stringify(clamped)})`,
    );
    await page.setViewportSize({ width: VW, height: VH });
  }

  assert.equal(errors.length, 0, errors.join('\n'));
  assert.equal(agentPosts, 0, 'must never POST /api/assistant/agent (no model messages or navigation resubmits)');
  console.log('Assistant floating/UI regression passed: default bottom-right launcher, click-once, immediate mouse drag (launcher+header, no 350ms wait), per-tab open-state+position persistence across hard routes/SPA popstate/full reload, close-persists, user2 identity isolation, agent POST=0, z-index5000 mask layering, touch longpress + synthetic click suppression, pointercancel/blur gesture cleanup, fresh drag, small-viewport clamp + no horizontal overflow.');
} catch (error) {
  if (page) {
    console.error(await page.locator('.lighthouse').innerText().catch(() => 'assistant unavailable'));
    await page.screenshot({ path: path.join(output, 'assistant-floating-failure.png') });
  }
  throw error;
} finally {
  await browser.close();
}
