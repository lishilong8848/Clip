import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';
import { preview } from 'vite';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const output = path.resolve(root, '../../../output/playwright/plan-convergence-notices');
await mkdir(output, { recursive: true });
const server = await preview({ root, logLevel: 'error', preview: { host: '127.0.0.1', port: 0, strictPort: false } });
const browser = await chromium.launch({ headless: true });
const page = await browser.newPage({ viewport: { width: 1440, height: 1000 }, timezoneId: 'UTC' });
page.setDefaultTimeout(15000);
const base = `http://127.0.0.1:${server.httpServer.address().port}`;
const title = 'EA118_C01机房B楼B-124-BAS-101/103/104/105/401、B-150-BAS-101/102控制柜指示灯微亮或不亮检修';
const checkedAt = Date.parse('2026-10-09T13:14:19+08:00') / 1000;
const row = (id, name, extra = {}) => ({ record_id: id, name, status: '开始', device: 'B-124-BAS-101/B-124-BAS-103/B-150-BAS-102', location: 'B-124/150控制柜', building: 'B楼', fault: '控制柜指示灯微亮或不亮', start_time: '2026-10-09 10:37', ...extra });
const hit = { blockId: 'block-b', blockName: 'B-124控制柜检修屏蔽', reason: '房间号 B-124' };
const records = {
  maintenance: [row('same-id', title, { auto_check_status: 'ready', auto_checked_at: checkedAt, hits: [] }),
    row('matched', 'B楼空调检修', { auto_check_status: 'ready', auto_checked_at: checkedAt, hits: [hit] }),
    row('unchecked', '尚未核对记录'), row('pending', '待处理记录', { auto_check_status: 'pending' }),
    row('failed', '核对失败记录', { auto_check_status: 'failed', auto_checked_at: checkedAt }),
    row('stale', '内容已变化记录', { check_status: 'stale', check_source: 'manual', checked_at: checkedAt })],
  change: [row('same-id', 'B楼控制柜更换变更', { fault: '更换控制柜指示灯', device: 'B-124-BAS-101' })],
};
const requests = [], errors = [];
let failNext = false, checks = 0;
page.on('pageerror', error => errors.push(error.message));
await page.route('**/api/**', async route => {
  const url = new URL(route.request().url());
  requests.push(url.pathname);
  const reply = data => route.fulfill({ json: { ok: true, data } });
  if (url.pathname === '/api/health') return route.fulfill({ json: { ok: true, service: 'clipflow_backend', instance_id: 'isolated-convergence-review' } });
  if (url.pathname === '/api/auth/status') return reply({ logged_in: true, user: { name: '隔离测试', open_id: 'isolated-review', role: 'admin' }, scope_options: [{ value: 'ALL', label: '全部' }] });
  if (url.pathname === '/api/plan-convergence/bootstrap') return reply({ is_admin: false, catalog_ready: true, points_ready: true, blocks: { items: [], loaded_at: 0 } });
  if (url.pathname === '/api/plan-convergence/rulesets') return reply([]);
  const match = url.pathname.match(/^\/api\/plan-convergence\/(maintenance|change)\/(records|check|points)$/);
  if (match) {
    const [, kind, action] = match;
    if (action === 'records') return reply(structuredClone(records[kind]));
    if (action === 'points') {
      return reply({
        items: [
          { device: 'B-124-BAS-101', space: 'B-124', point: 'BAS-101控制柜指示灯', config_id: 'cfg-bas-101', device_type: '楼宇自控' },
          { device: 'B-124-BAS-103', space: 'B-124', point: 'BAS-103控制柜指示灯', config_id: 'cfg-bas-103', device_type: '楼宇自控' },
        ],
        block_name: hit.blockName,
        notice_name: 'B楼空调检修',
        checked_at: checkedAt,
        note: '该点位于匹配屏蔽中',
      });
    }
    checks++;
    await new Promise(resolve => setTimeout(resolve, 350));
    if (failNext) { failNext = false; return route.fulfill({ status: 502, json: { ok: false, error: '无法连接智航，请连接 VPN 后重试' } }); }
    const id = route.request().postDataJSON().record_id;
    const selected = records[kind].filter(row => !id || row.record_id === id);
    for (const item of selected) Object.assign(item, { hits: kind === 'maintenance' ? [hit] : [], check_status: 'ready', checked_at: checkedAt + checks * 60, check_source: 'manual' });
    return reply({ records: structuredClone(selected), stats: { [kind]: selected.length, blocks: 1, matched_records: selected.filter(row => row.hits.length).length, orphan_blocks: kind === 'change' ? 1 : 0 } });
  }
  if (url.pathname === '/api/assistant/conversation') return reply({ enabled: false, turns: [] });
  return reply({});
});
const tableRow = name => page.locator('.pc-maintenance tbody tr').filter({ has: page.getByText(name, { exact: true }) });
try {
  await page.goto(base + '/plan-convergence?tab=maintenance');
  await tableRow(title).waitFor();
  assert.match(await tableRow(title).locator('.pc-check-result').innerText(), /未找到匹配屏蔽[\s\S]*自动核对/);
  assert.match(await tableRow('B楼空调检修').locator('.pc-check-result').innerText(), /找到 1 条候选屏蔽/);
  assert.match(await tableRow('尚未核对记录').locator('.pc-check-result').innerText(), /^尚未核对$/);
  assert.match(await tableRow('待处理记录').locator('.pc-check-result').innerText(), /正在自动核对/);
  assert.equal(await tableRow('待处理记录').locator('.spin').count(), 1);
  for (const name of ['尚未核对记录', '待处理记录', '核对失败记录', '内容已变化记录']) assert.doesNotMatch(await tableRow(name).locator('.pc-check-result').innerText(), /未找到匹配屏蔽/);
  await page.screenshot({ path: path.join(output, 'maintenance-states.png'), fullPage: true });
  await tableRow(title).getByRole('button', { name: '核对', exact: true }).click();
  await tableRow(title).getByRole('button', { name: '核对中…', exact: true }).waitFor();
  await page.getByRole('button', { name: '变更核对', exact: true }).click();
  await tableRow('B楼控制柜更换变更').waitFor();
  assert.match(await tableRow('B楼控制柜更换变更').locator('.pc-check-result').innerText(), /尚未核对/);
  await page.waitForTimeout(500);
  assert.equal(await tableRow(title).count(), 0, 'late repair response must not replace change records');
  await page.getByRole('button', { name: '核对进行中变更', exact: true }).click();
  await page.getByText('未找到匹配屏蔽', { exact: true }).waitFor();
  assert.match(await tableRow('B楼控制柜更换变更').locator('.pc-check-result').innerText(), /手动核对/);
  await page.screenshot({ path: path.join(output, 'change-result.png'), fullPage: true });
  await page.getByRole('button', { name: '检修核对', exact: true }).click();
  assert.match(await tableRow(title).locator('.pc-check-result').innerText(), /找到 1 条候选屏蔽[\s\S]*手动核对/);
  const countBeforeReload = checks;
  await page.reload();
  await tableRow(title).waitFor();
  assert.match(await tableRow(title).locator('.pc-check-result').innerText(), /手动核对/);
  assert.equal(checks, countBeforeReload, 'reopening reads saved results without performing another check');
  failNext = true;
  await tableRow(title).getByRole('button', { name: '核对', exact: true }).click();
  await page.getByText('无法连接智航，请连接 VPN 后重试', { exact: true }).waitFor();
  assert.match(await tableRow(title).locator('.pc-check-result').innerText(), /找到 1 条候选屏蔽/, 'failed retry preserves prior successful result');
  // Clicking a hit opens the local points modal (GET maintenance/points) and
  // stays on the same tab - it must not navigate away to the review page.
  const tabBeforePoints = await page.locator('.pc-tabs button.active').innerText();
  await tableRow(title).locator('.pc-hit').first().click();
  const pointsDialog = page.locator('.pc-dialog');
  await pointsDialog.waitFor();
  assert.match(await pointsDialog.locator('#pc-dialog-title').innerText(), /对应点位 · B-124控制柜检修屏蔽/);
  await pointsDialog.getByText('B-124-BAS-101', { exact: true }).waitFor();
  await pointsDialog.getByText('BAS-101控制柜指示灯', { exact: true }).waitFor();
  assert.equal(await page.locator('.pc-tabs button.active').innerText(), tabBeforePoints, 'points modal must not switch tabs');
  await pointsDialog.screenshot({ path: path.join(output, 'maintenance-points-modal.png') });
  await page.getByRole('button', { name: '关闭弹窗', exact: true }).click();
  await pointsDialog.waitFor({ state: 'detached' });
  await page.getByRole('button', { name: '变更核对', exact: true }).click();
  await tableRow('B楼控制柜更换变更').waitFor();
  assert.equal(await page.getByText('无法连接智航，请连接 VPN 后重试', { exact: true }).count(), 0, 'separate tabs do not share error state');
  await page.reload();
  await tableRow('B楼控制柜更换变更').waitFor();
  assert.match(await tableRow('B楼控制柜更换变更').locator('.pc-check-result').innerText(), /未找到匹配屏蔽[\s\S]*手动核对/);
  assert.equal(checks, countBeforeReload + 1);
  for (const width of [1920, 1366, 1024]) {
    await page.setViewportSize({ width, height: 1000 });
    assert.ok(await page.locator('.pc-page').evaluate(el => el.scrollWidth <= el.clientWidth + 1), `PC page overflow at ${width}`);
  }
  assert.deepEqual(errors, []);
  assert.ok(!requests.some(url => url === '/api/workbench-actions' || url.startsWith('/api/notice-cards')));
  console.log(JSON.stringify({ ok: true, production: true, checks, screenshots: output, assertions: 'unmatched with timestamp, pending/failed/unchecked/stale, change isolation, late results, reload persistence, failed retry preservation, 3 PC widths' }));
} catch (error) { await page.screenshot({ path: path.join(output, 'failure.png'), fullPage: true }); throw error; }
finally { await browser.close(); await new Promise(resolve => server.httpServer.close(resolve)); }
