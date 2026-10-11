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

const blockNameSingle = 'B-124控制柜检修屏蔽';
const blockNameMulti = 'B-150控制柜更换屏蔽';
const blockNameFail = 'VPN失败屏蔽';
const blockNameEmpty = '空明细屏蔽';
const blockNameLateSnap = '快照迟到屏蔽';
const blockNameLateDetail = '详情迟到屏蔽';
const blockNameMissing = '缓存外屏蔽';
const bootstrapBlocks = [
  ...Array.from({ length: 20 }, (_, i) => ({ blockId: 2001 + i, blockName: '占位屏蔽' + (i + 1), status: '1' })),
  ...[1001, 1002, 1003, 1004, 1005, 1006].map(id => ({ blockId: id, blockName: ({ 1001: blockNameSingle, 1002: blockNameMulti, 1003: blockNameFail, 1004: blockNameEmpty, 1005: blockNameLateSnap, 1006: blockNameLateDetail })[id], status: '1' })),
];
const targetBlockPage = Math.floor(bootstrapBlocks.findIndex(b => String(b.blockId) === '1001') / 20) + 1;
const initialBlockCount = bootstrapBlocks.length;
const detailRow = (blockDetailId, classifyModel, spaceModel, relateConfig, instances, instanceIds) => ({ blockDetailId, classifyModel, spaceModel, relateConfig, instances, instanceIds });
const blockData = {
  1001: { blockId: 1001, blockName: blockNameSingle, status: '1', alarmBlockDetailResultList: [detailRow('d-1001-1', '楼宇自控', 'B-124机房', 'BAS-101控制柜指示灯告警', 'BAS-101/BAS-103', 'ins-bas-101,ins-bas-103')] },
  1002: { blockId: 1002, blockName: blockNameMulti, status: '1', alarmBlockDetailResultList: Array.from({ length: 30 }, (_, i) => detailRow('d-1002-' + (i + 1), '楼宇自控', 'B-150机房', 'BAS-150柜' + (i + 1) + '告警', 'BAS-150-0' + (i + 1), 'ins-150-' + (i + 1))) },
  1003: { blockId: 1003, blockName: blockNameFail, status: '1', alarmBlockDetailResultList: [] },
  1004: { blockId: 1004, blockName: blockNameEmpty, status: '1', alarmBlockDetailResultList: [] },
  1005: { blockId: 1005, blockName: blockNameLateSnap, status: '1', alarmBlockDetailResultList: [detailRow('d-1005-1', '楼宇自控', 'L-300机房', 'L-300告警', 'L-300-DEV', 'ins-l300')] },
  1006: { blockId: 1006, blockName: blockNameLateDetail, status: '1', alarmBlockDetailResultList: [detailRow('d-1006-1', '楼宇自控', 'D-400机房', 'D-400告警', 'D-400-DEV', 'ins-d400')] },
  9999: { blockId: 9999, blockName: blockNameMissing, status: '1', alarmBlockDetailResultList: [detailRow('d-9999-1', '楼宇自控', 'Z-999机房', 'Z-999告警', 'Z-999-DEV', 'ins-z999')] },
};
const hitSingle = { blockId: 1001, blockName: blockNameSingle, reason: '房间号 B-124' };
const hitMulti = { blockId: 1002, blockName: blockNameMulti, reason: '房间号 B-150' };
const hitFail = { blockId: 1003, blockName: blockNameFail, reason: '房间号 F-100' };
const hitEmpty = { blockId: 1004, blockName: blockNameEmpty, reason: '房间号 E-200' };
const hitLateSnap = { blockId: 1005, blockName: blockNameLateSnap, reason: '房间号 L-300' };
const hitLateDetail = { blockId: 1006, blockName: blockNameLateDetail, reason: '房间号 D-400' };
const hitMissing = { blockId: 9999, blockName: blockNameMissing, reason: '房间号 Z-999' };
const records = {
  maintenance: [
    row('same-id', title, { auto_check_status: 'ready', auto_checked_at: checkedAt, hits: [] }),
    row('matched', 'B楼空调检修', { auto_check_status: 'ready', auto_checked_at: checkedAt, hits: [hitSingle] }),
    row('multi', 'C楼空调检修多明细', { auto_check_status: 'ready', auto_checked_at: checkedAt, hits: [hitMulti] }),
    row('unchecked', '尚未核对记录'),
    row('pending', '待处理记录', { auto_check_status: 'pending' }),
    row('failed', '核对失败记录', { auto_check_status: 'failed', auto_checked_at: checkedAt }),
    row('stale', '内容已变化记录', { check_status: 'stale', check_source: 'manual', checked_at: checkedAt }),
    row('jump-fail', 'C楼VPN失败', { auto_check_status: 'ready', auto_checked_at: checkedAt, hits: [hitFail] }),
    row('jump-empty', 'D楼空明细', { auto_check_status: 'ready', auto_checked_at: checkedAt, hits: [hitEmpty] }),
    row('jump-late-snap', 'E楼迟到快照', { auto_check_status: 'ready', auto_checked_at: checkedAt, hits: [hitLateSnap] }),
    row('jump-late-detail', 'F楼迟到详情', { auto_check_status: 'ready', auto_checked_at: checkedAt, hits: [hitLateDetail] }),
    row('jump-missing', 'G楼缓存外屏蔽', { auto_check_status: 'ready', auto_checked_at: checkedAt, hits: [hitMissing] }),
  ],
  change: [
    row('same-id', 'B楼控制柜更换变更', { fault: '更换控制柜指示灯', device: 'B-124-BAS-101' }),
    row('change-multi', 'C楼控制柜更换多明细', { device: 'BAS-150-027', auto_check_status: 'ready', auto_checked_at: checkedAt, hits: [hitMulti] }),
  ],
};
const requests = [], errors = [], snapshots = [];
let failNext = false, checks = 0, delaySnapshots = false, delayDetailMs = 0;
page.on('pageerror', error => errors.push(error.message));
await page.route('**/api/**', async route => {
  const url = new URL(route.request().url());
  requests.push(url.pathname);
  const reply = data => route.fulfill({ json: { ok: true, data } });
  if (url.pathname === '/api/health') return route.fulfill({ json: { ok: true, service: 'clipflow_backend', instance_id: 'isolated-convergence-review' } });
  if (url.pathname === '/api/auth/status') return reply({ logged_in: true, user: { name: '隔离测试', open_id: 'isolated-review', role: 'admin' }, scope_options: [{ value: 'ALL', label: '全部' }] });
  if (url.pathname === '/api/plan-convergence/bootstrap') return reply({ is_admin: false, catalog_ready: true, points_ready: true, blocks: { items: structuredClone(bootstrapBlocks), loaded_at: checkedAt } });
  if (url.pathname === '/api/plan-convergence/rulesets') return reply([]);
  let match = url.pathname.match(/^\/api\/plan-convergence\/(maintenance|change)\/records$/);
  if (match) return reply(structuredClone(records[match[1]]));
  match = url.pathname.match(/^\/api\/plan-convergence\/(maintenance|change)\/check$/);
  if (match) {
    const kind = match[1];
    checks++;
    await new Promise(resolve => setTimeout(resolve, 350));
    if (failNext) { failNext = false; return route.fulfill({ status: 502, json: { ok: false, error: '无法连接智航，请连接 VPN 后重试' } }); }
    const id = route.request().postDataJSON().record_id;
    const selected = records[kind].filter(row => !id || row.record_id === id);
    for (const item of selected) Object.assign(item, { hits: kind === 'maintenance' ? [hitSingle] : [], check_status: 'ready', checked_at: checkedAt + checks * 60, check_source: 'manual' });
    return reply({ records: structuredClone(selected), stats: { [kind]: selected.length, blocks: 1, matched_records: selected.filter(row => row.hits.length).length, orphan_blocks: kind === 'change' ? 1 : 0 } });
  }
  match = url.pathname.match(/^\/api\/plan-convergence\/blocks\/(\d+)$/);
  if (match) {
    const id = Number(match[1]);
    if (delayDetailMs && id === 1006) await new Promise(resolve => setTimeout(resolve, delayDetailMs));
    if (id === 1003) return route.fulfill({ status: 502, json: { ok: false, error: '无法连接智航，请连接 VPN 后重试' } });
    return reply(structuredClone(blockData[id]));
  }
  if (url.pathname === '/api/plan-convergence/snapshots') {
    const body = route.request().postDataJSON();
    snapshots.push({ pathname: url.pathname, body });
    if (delaySnapshots) await new Promise(resolve => setTimeout(resolve, 900));
    const instances = String(body.instanceIds || '').split(',').map(s => s.trim()).filter(Boolean);
    return reply(instances.map((n, i) => ({ insName: n, insStandardId: 'std-' + n, objName: '楼宇设备' })));
  }
  if (url.pathname === '/api/assistant/conversation') return reply({ enabled: false, turns: [] });
  return reply({});
});
const tableRow = name => page.locator('.pc-maintenance tbody tr').filter({ has: page.getByText(name, { exact: true }) });
const switchTo = label => page.getByRole('button', { name: label, exact: true }).click();

try {
  // ---- retained: maintenance check states ----
  await page.goto(base + '/plan-convergence?tab=maintenance');
  await tableRow(title).waitFor();
  assert.match(await tableRow(title).locator('.pc-check-result').innerText(), /未找到匹配屏蔽[\s\S]*自动核对/);
  assert.match(await tableRow('B楼空调检修').locator('.pc-check-result').innerText(), /找到 1 条候选屏蔽/);
  assert.match(await tableRow('尚未核对记录').locator('.pc-check-result').innerText(), /^尚未核对$/);
  assert.match(await tableRow('待处理记录').locator('.pc-check-result').innerText(), /正在自动核对/);
  assert.equal(await tableRow('待处理记录').locator('.spin').count(), 1);
  for (const name of ['尚未核对记录', '待处理记录', '核对失败记录', '内容已变化记录']) assert.doesNotMatch(await tableRow(name).locator('.pc-check-result').innerText(), /未找到匹配屏蔽/);
  await page.screenshot({ path: path.join(output, 'maintenance-states.png'), fullPage: true });

  // Set the review side list status filter + search so the jump must clear them to reveal the 屏蔽中 block.
  await switchTo('核对台');
  await page.locator('#pc-block-status').click();
  await page.getByRole('option', { name: '已关闭', exact: true }).click();
  await page.locator('.pc-list-filters input[type=search]').fill('占位屏蔽99');
  await switchTo('检修核对');

  // ---- maintenance single-detail jump: locate + highlight only (no dialog/snapshot), then user clicks camera ----
  await tableRow('B楼空调检修').locator('.pc-hit').click();
  const dlg = page.locator('.pc-dialog');
  await page.waitForTimeout(400);
  assert.equal(await page.locator('.pc-tabs button.active').innerText(), '核对台');
  assert.equal(await page.locator('.pc-list-filters input[type=search]').inputValue(), '', 'hit clears old search');
  assert.match(await page.locator('#pc-block-status').innerText(), /全部状态/, 'hit clears old status filter');
  assert.equal(await page.locator('.pc-list .pc-section-head small').innerText(), String(initialBlockCount), 'full block list retained, count not reduced');
  assert.equal(await page.locator('.pc-list .pc-pager span').first().innerText(), `${targetBlockPage} / ${Math.ceil(initialBlockCount / 20)}`, 'auto flips to target block page');
  const selectedRecord = page.locator('.pc-record.selected');
  await selectedRecord.waitFor();
  assert.match(await selectedRecord.innerText(), new RegExp(blockNameSingle));
  assert.match(await selectedRecord.innerText(), /1001/, 'selected id is 1001 with blue highlight');
  assert.match(await page.locator('.pc-main > .pc-section-head h3').innerText(), new RegExp(blockNameSingle), 'detail loaded without separate popup');
  assert.ok(requests.includes('/api/plan-convergence/blocks/1001'), 'block detail fetched via numeric id');
  assert.equal(await dlg.count(), 0, 'hit must not open any modal/dialog');
  assert.equal(await page.locator('.pc-focused-detail').count(), 1, 'hit highlights the sole detail row');
  assert.match(await page.locator('.pc-focused-detail td').first().evaluate(el => getComputedStyle(el).animationName), /^pcFocusBreath(?:-|$)/, 'detail uses a breathing animation');
  assert.ok(!snapshots.some(s => String(s.body.blockId) === '1001'), 'no snapshot auto-posted on hit');
  await page.screenshot({ path: path.join(output, 'single-locate.png'), fullPage: true });
  // User actively opens the snapshot via the review platform camera button.
  await page.locator('.pc-main tbody tr button[aria-label="查看实例快照"]').first().click();
  await dlg.waitFor();
  assert.match(await dlg.locator('#pc-dialog-title').innerText(), new RegExp('实例快照 · ' + blockNameSingle));
  const singlePost = snapshots.find(s => String(s.body.blockId) === '1001');
  assert.ok(singlePost, 'single-detail posts /snapshots only after user camera click');
  assert.deepEqual(singlePost.body, { blockId: '1001', blockDetailId: 'd-1001-1', instanceIds: 'ins-bas-101,ins-bas-103' });
  await dlg.getByText('ins-bas-101', { exact: true }).waitFor();
  await dlg.getByText('ins-bas-103', { exact: true }).waitFor();
  await dlg.screenshot({ path: path.join(output, 'single-snapshot.png') });
  await page.getByRole('button', { name: '关闭弹窗', exact: true }).click();
  await dlg.waitFor({ state: 'detached' });
  assert.equal(await page.locator('.pc-focused-detail').count(), 1, 'closing preserves remaining highlight duration');
  await page.screenshot({ path: path.join(output, 'single-jump.png') });
  await page.locator('.pc-focused-detail').waitFor({ state: 'detached', timeout: 4000 });
  assert.equal(await page.locator('.pc-focused-action').count(), 0, 'highlight expires instead of sticking on action button');
  await switchTo('检修核对');

  // ---- change multi-detail jump: locate + highlight only, no auto selection/popup; user flips detail page 2 then picks row 27 camera ----
  await switchTo('变更核对');
  await tableRow('C楼控制柜更换多明细').waitFor();
  await tableRow('C楼控制柜更换多明细').locator('.pc-hit').click();
  await page.waitForTimeout(400);
  assert.equal(await page.locator('.pc-tabs button.active').innerText(), '核对台');
  assert.equal(await dlg.count(), 0, 'multi-detail hit must not open selection modal');
  assert.equal(await page.locator('.pc-focused-detail').filter({ hasText: 'BAS-150-027' }).count(), 1, 'multi-detail hit locates exact equipment and highlights second-page row');
  assert.ok(!snapshots.some(s => String(s.body.blockId) === '1002'), 'multi-detail hit must not auto query any snapshot group');
  assert.match(await page.locator('.pc-main > .pc-section-head h3').innerText(), new RegExp(blockNameMulti), 'multi block detail loaded without separate popup');
  // User actively flips to detail page 2, then picks row 27 snapshot.
  assert.equal(await page.locator('.pc-main button[aria-label="下一页明细"]').isDisabled(), true, 'target detail page selected automatically');
  const row27 = page.locator('.pc-main tbody tr').filter({ has: page.getByText('BAS-150-027', { exact: true }) });
  await row27.waitFor();
  await row27.getByRole('button', { name: '查看实例快照', exact: true }).click();
  await dlg.waitFor();
  assert.match(await dlg.locator('#pc-dialog-title').innerText(), new RegExp('实例快照 · ' + blockNameMulti));
  const row27Post = snapshots.find(s => String(s.body.blockId) === '1002');
  assert.ok(row27Post, 'specific row query posted to /snapshots only after user camera click');
  assert.deepEqual(row27Post.body, { blockId: '1002', blockDetailId: 'd-1002-27', instanceIds: 'ins-150-27' });
  assert.equal(await page.locator('.pc-focused-detail').filter({ hasText: 'BAS-150-027' }).count(), 1, 'cross-pagination highlight locates focused detail on page 2');
  await page.getByRole('button', { name: '关闭弹窗', exact: true }).click();
  await dlg.waitFor({ state: 'detached' });
  assert.equal(await page.locator('.pc-focused-detail').filter({ hasText: 'BAS-150-027' }).count(), 1, 'closing keeps cross-page focus highlight');
  await page.screenshot({ path: path.join(output, 'multi-jump.png') });
  await switchTo('检修核对');

  // ---- cache-missing block appended into the list when not cached ----
  await tableRow('G楼缓存外屏蔽').waitFor();
  await tableRow('G楼缓存外屏蔽').locator('.pc-hit').click();
  await page.waitForTimeout(400);
  assert.equal(await page.locator('.pc-tabs button.active').innerText(), '核对台');
  assert.equal(await page.locator('.pc-list .pc-section-head small').innerText(), String(initialBlockCount + 1), 'missing block appended to full block list');
  const missingRecord = page.locator('.pc-record.selected');
  await missingRecord.waitFor();
  assert.match(await missingRecord.innerText(), new RegExp(blockNameMissing));
  assert.match(await missingRecord.innerText(), /9999/);
  assert.equal(await dlg.count(), 0, 'cache-missing hit must not open modal');
  assert.ok(!snapshots.some(s => String(s.body.blockId) === '9999'), 'cache-missing hit must not auto-post snapshot');
  await switchTo('检修核对');

  // ---- no-detail / empty state: no snapshot modal, detail pane shows empty hint ----
  await tableRow('D楼空明细').waitFor();
  await tableRow('D楼空明细').locator('.pc-hit').click();
  await page.getByText('此记录没有屏蔽明细', { exact: true }).waitFor();
  assert.equal(await page.locator('.pc-dialog').count(), 0, 'empty detail must not open a snapshot dialog');
  assert.ok(!snapshots.some(s => String(s.body.blockId) === '1004'), 'empty detail must not auto-post snapshot');
  await page.screenshot({ path: path.join(output, 'empty-detail.png') });
  await switchTo('检修核对');

  // ---- VPN detail read failure: global error alert, no snapshot ----
  await tableRow('C楼VPN失败').waitFor();
  await tableRow('C楼VPN失败').locator('.pc-hit').click();
  await page.getByText('无法连接智航，请连接 VPN 后重试', { exact: true }).waitFor();
  assert.equal(await page.locator('.pc-dialog').count(), 0, 'failed detail must not open a snapshot dialog');
  assert.ok(!snapshots.some(s => String(s.body.blockId) === '1003'), 'no snapshot posted when detail fetch fails');
  await page.getByRole('button', { name: '关闭提示', exact: true }).click();
  await switchTo('检修核对');

  // ---- delayed detail returning after switching away: no popup ----
  await tableRow('F楼迟到详情').waitFor();
  delayDetailMs = 900;
  await tableRow('F楼迟到详情').locator('.pc-hit').click();
  await switchTo('变更核对');
  await page.waitForTimeout(1200);
  delayDetailMs = 0;
  assert.equal(await page.locator('.pc-dialog').count(), 0, 'late detail must not reopen popup after switching away');
  assert.ok(!snapshots.some(s => String(s.body.blockId) === '1006'), 'no snapshot posted after switching away before detail returns');
  await switchTo('检修核对');

  // ---- user opens snapshot then closes before its late response: not reopened ----
  await tableRow('E楼迟到快照').waitFor();
  await tableRow('E楼迟到快照').locator('.pc-hit').click();
  await page.waitForTimeout(400);
  assert.equal(await page.locator('.pc-dialog').count(), 0, 'late-snapshot hit must not auto-open modal');
  assert.ok(!snapshots.some(s => String(s.body.blockId) === '1005'), 'late-snapshot hit must not auto-post snapshot');
  delaySnapshots = true;
  await page.locator('.pc-main tbody tr button[aria-label="查看实例快照"]').first().click();
  await dlg.waitFor();
  assert.match(await dlg.locator('#pc-dialog-title').innerText(), new RegExp('实例快照 · ' + blockNameLateSnap));
  await page.getByRole('button', { name: '关闭弹窗', exact: true }).click();
  await dlg.waitFor({ state: 'detached' });
  await page.waitForTimeout(1100);
  delaySnapshots = false;
  assert.equal(await page.locator('.pc-dialog').count(), 0, 'late snapshot response must not reopen closed dialog');
  assert.ok(snapshots.some(s => String(s.body.blockId) === '1005'), 'late snapshot request was abandoned after close');

  // ---- retained: response persistence, type isolation, failed-retry retention ----
  await switchTo('检修核对');
  await tableRow(title).waitFor();
  await tableRow(title).getByRole('button', { name: '核对', exact: true }).click();
  await tableRow(title).getByRole('button', { name: '核对中…', exact: true }).waitFor();
  await switchTo('变更核对');
  await tableRow('B楼控制柜更换变更').waitFor();
  assert.match(await tableRow('B楼控制柜更换变更').locator('.pc-check-result').innerText(), /尚未核对/);
  await page.waitForTimeout(500);
  assert.equal(await tableRow(title).count(), 0, 'late repair response must not replace change records');
  await page.getByRole('button', { name: '核对进行中变更', exact: true }).click();
  await tableRow('B楼控制柜更换变更').getByText('未找到匹配屏蔽', { exact: true }).waitFor();
  assert.match(await tableRow('B楼控制柜更换变更').locator('.pc-check-result').innerText(), /手动核对/);
  await switchTo('检修核对');
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

  // ---- retained: change-tab error isolation, refresh persistence, checks count ----
  await switchTo('变更核对');
  await tableRow('B楼控制柜更换变更').waitFor();
  assert.equal(await page.getByText('无法连接智航，请连接 VPN 后重试', { exact: true }).count(), 0, 'separate tabs do not share error state');
  await page.reload();
  await tableRow('B楼控制柜更换变更').waitFor();
  assert.match(await tableRow('B楼控制柜更换变更').locator('.pc-check-result').innerText(), /未找到匹配屏蔽[\s\S]*手动核对/);
  assert.equal(checks, countBeforeReload + 1, 'change-tab reload reads saved result without another check');

  for (const width of [1920, 1366, 1024]) {
    await page.setViewportSize({ width, height: 1000 });
    assert.ok(await page.locator('.pc-page').evaluate(el => el.scrollWidth <= el.clientWidth + 1), `PC page overflow at ${width}`);
  }

  assert.deepEqual(errors, []);
  assert.ok(!requests.some(url => url === '/api/workbench-actions' || url.startsWith('/api/notice-cards')));
  assert.ok(!requests.some(url => url.includes('/maintenance/points') || url.includes('/change/points')), 'points endpoint is no longer the main hit action');
  console.log(JSON.stringify({ ok: true, production: true, checks, snapshots: snapshots.length, screenshots: output, assertions: 'numeric blocks, single/multi locate + highlight only (no auto modal/snapshot, full list retained, cleared filters, target page + selected id), user camera opens exact snapshot payload, cache-missing block appended, empty/vpn/late-detail/late-snapshot guards, response persistence/isolation/refresh/checks count, PC widths' }));
} catch (error) { await page.screenshot({ path: path.join(output, 'failure.png'), fullPage: true }); throw error; }
finally { await browser.close(); await new Promise(resolve => server.httpServer.close(resolve)); }
