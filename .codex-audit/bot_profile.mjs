import assert from 'node:assert/strict';
import { chromium } from '../bin/lan_bitable_template_portal/frontend/node_modules/playwright/index.mjs';

const base = 'http://127.0.0.1:19043';
const browser = await chromium.launch({ headless: true });
try {
  const context = await browser.newContext({ viewport: { width: 1440, height: 1000 } });
  await context.addCookies([{ name: 'fixture_user', value: 'D', url: base }]);
  assert.equal((await (await context.request.get(base + '/api/health')).json()).instance_id, 'isolated-lighthouse-stream');
  const page = await context.newPage();
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  const cdp = await context.newCDPSession(page);
  await cdp.send('Performance.enable');
  const metrics = async () => Object.fromEntries((await cdp.send('Performance.getMetrics')).metrics.map(item => [item.name, item.value]));
  async function sample(name) {
    const before = await metrics();
    await page.waitForTimeout(5000);
    const after = await metrics();
    console.log(JSON.stringify({ name, task_core_pct: +(20 * (after.TaskDuration - before.TaskDuration)).toFixed(2),
      script_core_pct: +(20 * (after.ScriptDuration - before.ScriptDuration)).toFixed(2),
      layout_ms: +(1000 * (after.LayoutDuration - before.LayoutDuration)).toFixed(1),
      heap_mib: +(after.JSHeapUsedSize / 1024**2).toFixed(1), nodes: after.Nodes }));
  }
  await page.goto(base);
  await page.locator('.lighthouse-bot[data-animation-active="true"]').waitFor();
  await page.waitForTimeout(1500);
  await sample('closed_idle');
  await page.route('**/api/assistant/conversation', async route => {
    const response = await route.fetch();
    const body = await response.json();
    body.data.turns = Array.from({ length: 60 }, (_, index) => ({ operation_id: `profile-${index}`, status: 'completed',
      question: 'Synthetic question ' + index, answer: '**Synthetic answer**\n\n' + 'Preserved conversation text.\n'.repeat(30),
      scopes: ['D'], at: Date.now() / 1000, sources: [] }));
    await route.fulfill({ response, json: body });
  });
  await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
  await page.locator('.turn').last().waitFor();
  await page.waitForTimeout(2000);
  await sample('open_60_turns');
  await page.screenshot({ path: '.codex-audit/assistant-performance.png' });
  const other = await context.newPage();
  await other.goto('about:blank');
  await other.bringToFront();
  await page.waitForTimeout(500);
  await sample('background');
  assert.deepEqual(errors, []);
} finally { await browser.close(); }
