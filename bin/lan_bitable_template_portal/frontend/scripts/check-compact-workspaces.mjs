import assert from 'node:assert/strict';
import {layoutApiFixture} from './layout-api-fixture.mjs';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { preview } from 'vite';
import { chromium } from 'playwright';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const OUTPUT = path.resolve(root, '../../../output/playwright/compact-workspaces');

const scopes = ['A', 'B', 'C', 'D', 'E', 'H'];

// ---------------------------------------------------------------- contrast
function parseColor(value) {
  if (!value) return null;
  let v = String(value).trim();
  if (v.startsWith('var(')) return null;
  if (v.startsWith('#') && v.length === 4) {
    const h = v.slice(1);
    return [0, 1, 2].map((i) => parseInt(h[i] + h[i], 16));
  }
  if (v.startsWith('#')) {
    const h = v.slice(1);
    return [0, 2, 4].map((i) => parseInt(h.slice(i, i + 2), 16));
  }
  const m = v.match(/rgba?\(\s*([\d.]+)[,\s]+([\d.]+)[,\s]+([\d.]+)(?:[,\s/]+([\d.]+))?\s*\)/);
  if (m) return [Number(m[1]), Number(m[2]), Number(m[3])];
  const m2 = v.match(/[\d.]+/g);
  if (m2 && m2.length >= 3) return [Number(m2[0]), Number(m2[1]), Number(m2[2])];
  return null;
}
function luminance(rgb) {
  const [r, g, b] = rgb.map((v) => {
    const s = v / 255;
    return s <= 0.03928 ? s / 12.92 : ((s + 0.055) / 1.055) ** 2.4;
  });
  return 0.2126 * r + 0.7152 * g + 0.0722 * b;
}
function contrast(a, b) {
  const la = luminance(a), lb = luminance(b);
  return (Math.max(la, lb) + 0.05) / (Math.min(la, lb) + 0.05);
}
function assertContrast(bg, ink, label) {
  const bgc = parseColor(bg), inkc = parseColor(ink);
  assert(bgc, `${label}: could not parse background ${bg}`);
  assert(inkc, `${label}: could not parse ink ${ink}`);
  const ratio = contrast(bgc, inkc);
  assert(ratio >= 4.5, `${label}: contrast ${ratio.toFixed(2)} < 4.5 (bg=${bg} ink=${ink})`);
}
function cssVar(handle, name) {
  return handle.evaluate((el, n) => getComputedStyle(el).getPropertyValue(n).trim(), name);
}
// wait until the cell's cabinet palette has actually been applied inline
// (the app paints _v2-cell-* vars into the inline style; the inherited
// night text color must not be sampled before the palette lands)
async function waitPalette(handle) {
  await handle.waitFor();
  await handle.evaluate(async (el) => {
    const t0 = Date.now();
    while (Date.now() - t0 < 5000 && !el.style.getPropertyValue('--v2-cell-ink')) {
      await new Promise((r) => setTimeout(r, 30));
    }
    if (!el.style.getPropertyValue('--v2-cell-ink')) throw new Error('cabinet palette --v2-cell-ink never applied inline (palette race)');
  });
}

// ---------------------------------------------------------------- mock state
const user1 = { open_id: 'Athen-1', name: 'Athen', role: 'admin', level: 'L1', category: 'manager', modules: ['maintenance', 'change', 'repair', 'polling', 'adjust', 'power', 'links', 'knowledge', 'cabinet', 'critical_guard', 'repair_management', 'plan_convergence', 'event', 'signatures', 'sops'] };
const user2 = { open_id: 'Athen-2', name: 'Athen2', role: 'admin', level: 'L1', category: 'manager', modules: user1.modules.slice() };
let authUser = user1;
const layouts=layoutApiFixture();
let planFails = false;

const sop = (id, scope, work_type, name) => ({ sop_id: id, scope, work_type, name, version: 1, steps: [{ step_id: 'step-' + id, content: '原操作内容', repeat_rules: [] }], attachments: [] });
const templates = [sop('sop-a', 'A', 'maintenance', 'A楼维保模板'), sop('sop-b', 'B', 'polling', 'B楼轮巡模板')];

const noticeRecords = [
  { record_id: 'n1', title: 'A楼维保通告', work_type: 'maintenance', status: '进行中', scope: 'A' },
  { record_id: 'n2', title: 'A楼变更通告', work_type: 'change', status: '未开始', scope: 'A' },
];

const eventRecords = [
  { record_id: 'e1', title: 'A楼开关事件', scope: 'A', started_at: '2026-10-01 09:00:00', occurrence_time: '2026-10-01 09:00:00', state: '进行中', kind: 'power' },
];

const repairRecords = [
  { record_id: 'r1', title: '测试维修项目', building: 'A', scope: 'A', building_name: 'A楼', workflow: '进行中', specialty: '暖通', fault_floor: '3F' },
];

const planBlockRows = [
  { blockId: '1', blockName: 'A楼冷机检修屏蔽', status: '1' },
  { blockId: '2', blockName: 'B楼设备维护屏蔽', status: '0' },
];

const cabinetRack = { room: '201', rack: 'A01', state: 'formal', rack_type: '服务器机柜' };
const cabinetLayoutCells = [
  { ref: 'title', x: 0, y: 0, width: 300, height: 32, text: '201机房', style: {} },
  { ref: 'humid', x: 20, y: 60, width: 90, height: 40, text: '加湿', style: { fill: '#ffc000' } },
  { ref: 'ac', x: 120, y: 60, width: 90, height: 40, text: '空调', style: { fill: '#00b050' } },
  { ref: 'A1', x: 120, y: 130, width: 70, height: 40, text: 'A01', rack: 'A01', state: 'formal', style: { fill: '#ff0000' } },
  { ref: 'A2', x: 210, y: 130, width: 70, height: 40, text: 'A02', rack: 'A02', state: 'test', style: { fill: '#ffc000' } },
  { ref: 'A3', x: 300, y: 130, width: 70, height: 40, text: 'A03', rack: 'A03', state: 'off', style: { fill: '#00b050' } },
];

// ---------------------------------------------------------------- server
const server = await preview({
  root,
  ...(process.env.CLIPFLOW_WORKSPACE_BUILD_DIR ? { configFile: path.join(root, 'vite.workspace-v2.config.ts') } : {}),
  logLevel: 'error',
  preview: { host: '127.0.0.1', port: 0, proxy: {} },
});
const origin = 'http://127.0.0.1:' + server.httpServer.address().port;
const entry = origin + '/assets/workspace-v2/index.html';

const browser = await chromium.launch({ headless: true });
const failures = [];
const executed = [];
let page;

async function shot(name) {
  await mkdir(OUTPUT, { recursive: true });
  await page.screenshot({ path: path.join(OUTPUT, name + '.png'), fullPage: true });
}
function record(name, message, stack) {
  failures.push({ name, message, stack });
  console.error('FAIL [' + name + '] ' + message);
}

async function scenario(name, fn) {
  executed.push(name);
  try {
    await fn();
    console.log('PASS [' + name + ']');
  } catch (error) {
    record(name, error.message, error.stack);
    try { await shot('fail-' + name.replace(/[^A-Za-z0-9]+/g, '-').replace(/^-|-$/g, '')); } catch {}
  }
}

try {
  const context = await browser.newContext({ viewport: { width: 1440, height: 900 }, reducedMotion: 'reduce' });
  await context.route('**/*', async (route) => {
    const req = route.request(), url = new URL(req.url());
    if (url.origin !== origin) return route.abort();
    if (!url.pathname.startsWith('/api/')) return route.continue();
    const ok = (data) => route.fulfill({ json: { ok: true, data } });
    if (planFails && url.pathname === '/api/plan-convergence/bootstrap') {
      return route.fulfill({ status: 500, json: { ok: false, error: 'bootstrap network failure' } });
    }
    const p = url.pathname;
    if(await layouts.handle(route,authUser.open_id))return;
    if (p === '/api/auth/status') {
      return ok({ logged_in: true, user: authUser, scope_options: scopes.map((value) => ({ value, label: value + '楼' })) });
    }
    if (p === '/api/health') return route.fulfill({ json: { ok: true, service: 'clipflow_backend', instance_id: 'compact-workspaces' } });
    if (p === '/api/workbench') {
      return ok({ records: noticeRecords, ongoing: [], records_pagination: { total: noticeRecords.length }, ongoing_pagination: { total: 0 } });
    }
    if (p === '/api/workbench/lite-detail') return ok({ fragments: {}, source_detected: false });
    if (p === '/api/workbench/draft') return ok({ draft: null });
    if (p === '/api/workbench/polling-delay-status') return ok({ state: 'none', due_at_ts: 0, enabled: false, missing_completion: false });
    if (p === '/api/polling-sops' && req.method() === 'GET') {
      const scope = url.searchParams.get('scope'), type = url.searchParams.get('work_type');
      assert(scopes.includes(scope), 'SOP requested unauthorized scope ' + scope);
      return ok({ items: templates.filter((item) => item.scope === scope && item.work_type === type) });
    }
    if (p === '/api/events/monthly') return ok({ records: eventRecords, snapshot_exists: true, stats: { total: eventRecords.length }, unknown_time_count: 0, last_refreshed_at: 0 });
    if (p === '/api/events/overview') return ok({ snapshot_exists: true, stats: { total: eventRecords.length }, building_stats: [], unknown_time_count: 0, last_refreshed_at: 0 });
    if (p === '/api/events/refresh') return ok({ records: eventRecords, stats: { total: eventRecords.length } });
    if (p === '/api/repair-management/records') {
      return ok({ records: repairRecords, total: repairRecords.length, offset: 0, fields: [] });
    }
    if (p === '/api/repair-management/status') return ok({ stats: { active: 1, completed_total: 0 } });
    if (p === '/api/repair-management/health') return ok({ clean: true });
    if (p === '/api/plan-convergence/bootstrap') {
      return ok({ is_admin: false, catalog_ready: true, points_ready: true, zh_ready: false, blocks: { items: planBlockRows, loaded_at: Math.floor(Date.now() / 1000) }, counts: { devices: planBlockRows.length, rules: 0 } });
    }
    if (p === '/api/plan-convergence/blocks') return ok({ items: planBlockRows, loaded_at: Math.floor(Date.now() / 1000) });
    if (p === '/api/plan-convergence/rulesets') return ok([]);
    if (p === '/api/plan-convergence/maintenance/records') return ok([]);
    if (p === '/api/plan-convergence/change/records') return ok([]);
    if (p === '/api/cabinet-power/bootstrap') return ok({ status: 'succeeded', ready: 1, total: 1, buildings: [{ scope: 'A', status: 'succeeded' }] });
    if (p === '/api/cabinet-power/overview') return ok({ version: 1, record_count: 1, issues: [], counts: { total: 1076, powered: 1022, formal: 1012, test: 10, off: 54, unknown: 0 }, rooms: [{ id: '201', name: '201机房', sheet: '201', total: 1, counts: { formal: 1, test: 0, off: 0, unknown: 0 }, types: { '服务器机柜': 1, '网络机柜': 0 } }], sheet_formats: [] });
    if (p === '/api/cabinet-power/racks') return ok({ version: 1, items: [cabinetRack] });
    if (p === '/api/cabinet-power/rooms/201/layout') return ok({ layout: { width: 720, height: 360, cells: cabinetLayoutCells } });
    if (p === '/api/cabinet-power/operations') return ok({ items: [], total: 0, page: 1, rack_state: cabinetRack });
    if (p === '/api/cabinet-power/batches/text-preview') return ok([]);
    if (p === '/api/cabinet-power/rooms/201/layout-save' || p === '/api/cabinet-power/save') return ok({});
    if (req.method() !== 'GET') return route.fulfill({ status: 400, json: { ok: false, error: 'unexpected write' } });
    return ok({ items: [], records: [], scopes: [], settings: {}, total: 0, groups: [], permissions: {}, engine: {}, stats: {} });
  });

  page = await context.newPage();
  page.on('pageerror', (error) => record('pageerror', error.message));

  const waitSpinner = async () => {
    await page.waitForTimeout(120);
  };

  // ---------------------------------------------------------- 1. Notice command row
  await scenario('notice-category-actions-layout-same-command-row', async () => {
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.goto(entry + '?__page=/workbench-lite&work_type=maintenance&scope=A');
    await page.locator('.v2-notice-command').waitFor();
    await page.waitForFunction(() => document.querySelectorAll('.v2-notice-item, .v2-notice-list > *').length > 0);
    const command = page.locator('.v2-notice-command');
    await command.locator('.v2-layout-trigger').waitFor();
    const categoryCount = await command.locator('.v2-notice-types button').count();
    assert(categoryCount >= 1, 'category buttons absent from notice command row');
    const actionTexts = await command.locator('button').allTextContents();
    const hasLayout = await command.locator('.v2-layout-trigger').count();
    assert.equal(hasLayout, 1, 'custom layout trigger must sit in the notice command row');
    const joined = actionTexts.join(' ');
    assert(/更多办理|刷新|手填维保通告|粘贴原文/.test(joined), 'notice action buttons must be in the command row: ' + joined);
    // same compact command row on desktop: categories + actions + layout trigger
    // are all placed inside one non-wrapping command row
    const band = await command.evaluate((c) => {
      const tops = [...c.querySelectorAll('.v2-notice-types button, button, .v2-layout-trigger')].map((el) => el.getBoundingClientRect().top);
      const trigger = c.querySelector('.v2-layout-trigger').getBoundingClientRect();
      return { height: c.getBoundingClientRect().height, min: Math.min(...tops), max: Math.max(...tops), triggerTop: trigger.top };
    });
    assert(band.height <= 60, 'notice command row must stay a single compact row, height=' + band.height);
    assert((band.max - band.min) < 20, 'notice categories/actions/layout must share one desktop row');
    assert(Math.abs(band.triggerTop - band.min) < 20, 'custom layout trigger must sit on the same notice row');
    assert(!await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1), 'notice page overflows at 1440');
    await shot('notice-command-row-1440');
  });

  // ---------------------------------------------------------- 2. SOP filters
  await scenario('sop-two-selects-same-row-search-inside-list', async () => {
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.goto(entry + '?view=sops');
    await page.locator('.v2-sop-item').filter({ hasText: 'A楼维保模板' }).waitFor();
    const filters = page.locator('.v2-sop-filters');
    await filters.waitFor();
    assert.equal(await filters.locator('.n-base-selection').count(), 2, 'SOP filters must contain exactly two selects');
    assert.equal(await filters.getByLabel('SOP楼栋', { exact: true }).count(), 1, 'SOP building select missing');
    assert.equal(await filters.getByLabel('SOP类型', { exact: true }).count(), 1, 'SOP type select missing');
    // two selects share the same row on desktop
    const ySet = new Set();
    for (const box of await filters.locator('.n-base-selection').evaluateAll((els) => els.map((el) => Math.round(el.getBoundingClientRect().top)))) ySet.add(box);
    assert(ySet.size === 1, 'SOP two filters are not on the same row at 1440');
    // search box inside the list
    const searchInsideList = await page.locator('.v2-sop-layout aside').getByRole('textbox', { name: '查找SOP', exact: true }).count();
    assert.equal(searchInsideList, 1, 'SOP search box must live inside the list aside');
    assert.equal(await filters.getByRole('textbox', { name: '查找SOP', exact: true }).count(), 0, 'SOP search box must not be in the top filters row');
    await shot('sop-filters-1440');
    // mobile width
    await page.setViewportSize({ width: 390, height: 844 });
    await page.waitForTimeout(200);
    assert(!await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1), 'SOP overflows at 390');
    await shot('sop-filters-390');
  });

  // ---------------------------------------------------------- 3. Event root has no return button
  await scenario('event-root-has-no-meaningless-return-button', async () => {
    for (const width of [1440, 390]) {
      await page.setViewportSize({ width, height: width < 700 ? 844 : 900 });
      await page.goto(entry + '?__page=/&mode=events&scope=A');
      await page.locator('.event-page').waitFor();
      await waitSpinner();
      const buildings=page.getByRole('region',{name:'楼栋事件条数',exact:true});
      await buildings.getByRole('button',{name:'查看A楼本月事件',exact:true}).waitFor();
      await buildings.getByRole('button',{name:'查看A楼本月事件',exact:true}).locator('strong').waitFor();
      assert.equal((await buildings.getByRole('button',{name:'查看A楼本月事件',exact:true}).locator('strong').innerText()).trim(),'1');
      assert.equal(await buildings.getByRole('button').count(),scopes.length,'only authorized buildings appear, including zero-count buildings');
      assert.equal(await buildings.getByRole('button',{name:'查看B楼本月事件',exact:true}).locator('strong').innerText(),'0');
      await buildings.getByRole('button',{name:'查看A楼本月事件',exact:true}).click();
      await page.locator('.event-list-panel .event-row').filter({hasText:'A楼开关事件'}).waitFor();
      assert.equal(await page.locator('.event-list-panel .event-row').count(),1,'building count drills into current-period event records');
      assert.equal(await page.locator('.vnet-back-button[aria-label="返回"], .vnet-back-button, [aria-label="返回"]').count(), 0, 'event root must not show a return button at width ' + width);
      assert(!await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1), 'event page overflows at ' + width);
      await shot('event-root-no-return-' + width);
    }
  });

  // ---------------------------------------------------------- 4. Repair compact command row
  await scenario('repair-compact-action-filter-command-row', async () => {
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.goto(entry + '?__page=/repair-management&scope=A');
    await page.locator('.v2-repair-command').waitFor();
    await page.waitForFunction(() => document.body.innerText.includes('测试维修项目'));
    const command = page.locator('.v2-repair-command');
    await command.locator('.v2-layout-trigger').waitFor();
    const text = (await command.innerText());
    assert(/测试维修项目|搜索/.test(text) || await command.getByRole('textbox').count() >= 1, 'repair filter/search must be in command row');
    assert(/新建维修单|刷新/.test(text), 'repair action buttons must be in command row: ' + text);
    assert.equal(await page.locator('[data-widget-id="block_records"] .v2-widget-head [aria-label="维修项目状态"]').count(),1,'state controls are part of the repair list header');
    await shot('repair-command-1440');
  });

  // ---------------------------------------------------------- 5. Plan tabs route URLs + back
  await scenario('plan-four-tabs-separate-routes-and-browser-back', async () => {
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.goto(entry + '?__page=/plan-convergence');
    await page.locator('.pc-tabs').waitFor();
    await page.waitForFunction(() => document.body.innerText.includes('A楼冷机检修屏蔽'));
    const tabs = page.locator('.pc-tabs button');
    const tabLabels = {
      '核对台': 'review',
      '规则配置': 'rules',
      '检修核对': 'maintenance',
      '变更核对': 'change',
    };
    for (const [label, id] of Object.entries(tabLabels)) {
      assert.equal(await tabs.getByText(label, { exact: true }).count(), 1, 'tab missing: ' + label);
    }
    // switch to rules
    await tabs.getByText('规则配置', { exact: true }).click();
    await page.waitForFunction(() => new URL(location.href).searchParams.get('tab') === 'rules');
    // switch to maintenance
    await tabs.getByText('检修核对', { exact: true }).click();
    await page.waitForFunction(() => new URL(location.href).searchParams.get('tab') === 'maintenance');
    // browser back must return to rules
    await page.goBack();
    await page.waitForFunction(() => new URL(location.href).searchParams.get('tab') === 'rules');
    assert.equal(await page.locator('.pc-ruleset-list, .pc-rules').count(), 1, 'back must restore the rules tab content');
    await shot('plan-tabs-routes-1440');
  });

  // ---------------------------------------------------------- 6. Block record heading singleton
  await scenario('plan-block-record-heading-singleton-status-refresh', async () => {
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.goto(entry + '?__page=/plan-convergence');
    await page.locator('.pc-tabs').waitFor();
    await page.waitForFunction(() => document.body.innerText.includes('屏蔽记录'));
    assert.equal(await page.locator('.v2-plan-record-head').count(), 1, '屏蔽记录 heading must be a singleton');
    const head = page.locator('.v2-plan-record-head');
    assert((await head.innerText()).includes('屏蔽记录'), 'block record heading missing title');
    assert(await head.locator('.pc-badge').count() === 1, 'block record heading must show active count badge');
    assert(await head.getByRole('button').count() >= 1, 'block record heading must expose a refresh action');
    await shot('plan-block-record-heading-1440');
  });

  // ---------------------------------------------------------- 7. Failed bootstrap single close
  await scenario('plan-failed-bootstrap-single-close-message', async () => {
    await page.setViewportSize({ width: 1440, height: 900 });
    planFails = true;
    try {
      await page.goto(entry + '?__page=/plan-convergence');
      await page.locator('.pc-alert.error').waitFor();
      const toast=page.locator('.pc-alert.error');
      await toast.locator('[data-v2-feedback-close]').waitFor();
      assert.equal(await toast.locator('button:visible').count(),1,'only one close is actually visible');
      assert.equal(await toast.locator('[data-v2-feedback-icon]:visible').count(),1,'one visible warning icon');
      assert.equal(await toast.locator(':scope > svg:visible').count(),0,'source warning icon is not duplicated');
      await shot('plan-failed-bootstrap-close-1440');
      await toast.locator('[data-v2-feedback-close]').click();await toast.waitFor({state:'hidden'});
    } finally {
      planFails = false;
    }
  });

  // ---------------------------------------------------------- 8+9. Scope memory / H scope not hitting cabinet
  await scenario('scope-h-workbench-and-module-scope-memory', async () => {
    await page.setViewportSize({ width: 1440, height: 900 });
    authUser = user1;
    await context.clearCookies();
    await page.goto(entry+'?scope=H');
    await page.locator('.v2-sidebar').getByText('机柜管理',{exact:true}).click();
    await page.locator('.cabinet-page').waitFor();
    await page.locator('.v2-cabinet-rate-value').getByText('95.0%',{exact:true}).waitFor();
    assert((await page.locator('.v2-cabinet-status').boundingBox()).height<70,'desktop status strip is compact');
    assert((await page.locator('[data-widget-id="block_summary"]').boundingBox()).height<=144,'summary no longer reserves 256px');
    assert(!await page.getByText('H楼尚未接入此业务',{exact:true}).count(),'home H does not leak into cabinet');
    await page.goto(entry + '?__page=/workbench-lite&work_type=maintenance&scope=H');
    // workbench saved H to its module memory and rendered
    await page.locator('.v2-notice-command').waitFor();
    await page.waitForTimeout(200);

    // navigate to cabinet via sidebar; cabinet must resolve its own scope (A), never H
    const cabResp = page.waitForResponse((r) => r.url().includes('/api/cabinet-power/overview'), { timeout: 10000 }).catch(() => null);
    await page.locator('.v2-sidebar').getByText('机柜管理', { exact: true }).first().click();
    await page.waitForFunction(() => new URL(location.href).pathname.includes('cabinet-power') || location.href.includes('cabinet-power'));
    await page.locator('.cabinet-page').waitFor({ timeout: 8000 });
    const resp = await cabResp;
    assert(resp, 'cabinet overview request must fire when navigating from workbench');
    const scopeQ = new URL(resp.url()).searchParams.get('scope');
    assert.equal(scopeQ, 'A', 'cabinet must resolve scope A, got ' + scopeQ);
    assert.notEqual(scopeQ, 'H', 'cabinet must never request scope H');
    // workbench must remember its own H scope when navigating back
    const wbPromise = page.waitForResponse((r) => r.url().includes('/api/workbench') && r.url().includes('scope='), { timeout: 10000 }).catch(() => null);
    await page.locator('.v2-sidebar').getByText('通告办理', { exact: true }).first().click();
    await page.waitForFunction(() => document.body.innerText.includes('A楼维保通告') && (new URL(location.href).pathname.includes('workbench-lite') || location.href.includes('workbench-lite')));
    await page.waitForTimeout(300);
    const wb = await wbPromise;
    assert(wb, 'workbench must re-request on return');
    const workbenchScope = new URL(wb.url()).searchParams.get('scope');
    assert.equal(workbenchScope, 'H', 'workbench/notices module must restore its own remembered scope H, got ' + workbenchScope);
    await shot('scope-memory-1440');
  });

  // ---------------------------------------------------------- 10. Per-person PageCanvas layout save/reload isolation
  await scenario('pagecanvas-per-person-layout-save-reload-isolation', async () => {
    await page.setViewportSize({ width: 1440, height: 900 });
    authUser = user1;
    await page.goto(entry + '?view=sops');
    await page.locator('.v2-sop-item').filter({ hasText: 'A楼维保模板' }).waitFor();
    // ensure the SOP page layout trigger is present (page wrapped by PageCanvas)
    await page.locator('[data-v2-layout-tools="sops"] .v2-layout-trigger, .v2-sop-filters .v2-layout-trigger').first().waitFor({ timeout: 8000 });
    await page.locator('[aria-label="自定义SOP模板布局"]').click();
    await page.locator('.v2-page-layout-tools').waitFor();
    // exercise the custom layout controls: reorder the editor block above the templates block
    await page.locator('[aria-label="上移SOP步骤与附件"]').click();
    await page.getByRole('button',{name:'设置SOP列表',exact:true}).click();
    const sizing=page.locator('.v2-page-layout-library:visible');
    await sizing.locator('.n-select').nth(0).click();await page.locator('.n-base-select-option').getByText('整行',{exact:true}).click();
    await sizing.locator('.n-select').nth(1).click();await page.locator('.n-base-select-option').getByText('384 像素',{exact:true}).click();
    await sizing.getByRole('button',{name:'完成设置',exact:true}).click();
    await page.getByRole('button',{name:'上移SOP步骤与附件',exact:true}).click();
    await page.getByRole('button', { name: '保存布局', exact: true }).click();
    await page.getByText('本页布局已同步到账号', { exact: true }).waitFor();
    const saved = layouts.records.get('Athen-1:/api/workspace-layouts/desktop/sops');
    assert(saved?.value.layout, 'saved layout must be written to the account store');
    const templates=saved.value.layout.items.find(item=>item.id==='block_templates');assert.equal(templates.w,12);assert.equal(templates.h,6);

    // reload restores that saved layout for user1
    await page.reload();
    await page.locator('.v2-sop-item').filter({ hasText: 'A楼维保模板' }).waitFor();
    await page.waitForFunction(()=>{const node=document.querySelector('[data-widget-id="block_templates"]')?.gridstackNode;return node?.w===12&&node?.h===6;});
    await page.waitForTimeout(300);
    const orderUser1 = await page.locator('.v2-module-block[data-business-block]').evaluateAll((els) => els.map((el) => el.getAttribute('data-business-block')));
    assert(orderUser1.length >= 2, 'SOP page canvas should have multiple layout blocks');
    // when user moved editor up, editor appears before templates
    assert(orderUser1.indexOf('block_editor') < orderUser1.indexOf('block_templates'), 'user1 saved editor block position was not restored: ' + orderUser1.join(','));

    // second user must never see the first user's saved layout
    authUser = user2;
    await page.reload();
    await page.locator('.v2-sop-item').filter({ hasText: 'A楼维保模板' }).waitFor();
    await page.waitForTimeout(300);
    assert(!layouts.records.has('Athen-2:/api/workspace-layouts/desktop/sops'), 'user2 must have no saved sops layout (isolation)');
    const orderUser2 = await page.locator('.v2-module-block[data-business-block]').evaluateAll((els) => els.map((el) => el.getAttribute('data-business-block')));
    assert(orderUser2.indexOf('block_editor') > orderUser2.indexOf('block_templates'), 'user2 must see default templates-first layout, got ' + orderUser2.join(','));
    // switch back to user1 to leave clean state
    authUser = user1;
    await shot('pagecanvas-per-person-layout');
  });

  // ---------------------------------------------------------- 11. Cabinet dark floorplan labels + contrast
  await scenario('cabinet-dark-floorplan-labels-and-contrast', async () => {
    await page.setViewportSize({ width: 1440, height: 900 });
    authUser = user1;
    await page.goto(entry + '?__page=/cabinet-power&scope=A');
    await page.locator('.cabinet-page').waitFor();
    // enable night mode through the nav theme picker
    if (!(await page.getByRole('switch', { name: '夜间模式' }).isVisible().catch(() => false))) await page.getByRole('button', { name: '导航配色', exact: true }).click();
    const nightToggle = page.getByRole('switch', { name: '夜间模式' });
    await nightToggle.waitFor({ state: 'visible' });
    if ((await nightToggle.getAttribute('aria-checked')) !== 'true') await nightToggle.click();
    await page.waitForFunction(() => document.documentElement.dataset.v2Theme === 'dark');
    await page.getByRole('button', { name: '导航配色', exact: true }).click();
    await nightToggle.waitFor({ state: 'hidden' }).catch(() => {});
    await page.waitForTimeout(200);
    await page.getByRole('button', { name: '原始平面图', exact: true }).click();
    await page.getByRole('textbox', { name: '搜索平面图', exact: true }).waitFor();
    const humid = page.locator('.map-cell').filter({ hasText: '加湿', exact: true });
    const ac = page.locator('.map-cell').filter({ hasText: '空调', exact: true });
    await humid.first().waitFor();
    await ac.first().waitFor();
    assert.equal(await page.locator('.map-cell').filter({ hasText: '加湿', exact: true }).count(), 1, 'humidifier label (加湿) must appear on dark floorplan');
    assert.equal(await page.locator('.map-cell').filter({ hasText: '空调', exact: true }).count(), 1, 'AC label (空调) must appear on dark floorplan');

    // rack state cells keep semantic colors while label contrast >= 4.5
    for (const [text, state, semantic] of [['A01', 'formal', '#ff0000'], ['A02', 'test', '#ffc000'], ['A03', 'off', '#00b050']]) {
      const cell = page.locator('.map-cell').filter({ hasText: text, exact: true }).first();
      await waitPalette(cell);
      const bg = await cssVar(cell, '--v2-cell-background');
      const ink = await cssVar(cell, '--v2-cell-ink');
      assert(bg && bg !== '', 'rack cell ' + text + ' must carry a semantic --v2-cell-background');
      assert(ink && ink !== '', 'rack cell ' + text + ' must carry --v2-cell-ink');
      // cell remains semantically colored
      assert.equal(bg.toLowerCase(), semantic, 'rack ' + text + ' (' + state + ') must keep semantic fill ' + semantic + ', got ' + bg);
      assertContrast(bg, ink, 'dark rack ' + text + ' (' + state + ')');
    }
    // also ensure the humidifier/AC label contrast is acceptable
    for (const cell of [humid.first(), ac.first()]) {
      await waitPalette(cell);
      const bg = await cssVar(cell, '--v2-cell-background');
      const ink = await cssVar(cell, '--v2-cell-ink');
      if (bg && ink) assertContrast(bg, ink, 'dark metric cell');
    }
    // dark desktop screenshot
    await shot('cabinet-dark-floorplan-1440');
  });

  // separate dark mobile screenshot
  await scenario('cabinet-dark-floorplan-mobile-screenshot', async () => {
    await page.setViewportSize({ width: 390, height: 844 });
    authUser = user1;
    await page.goto(entry + '?__page=/cabinet-power&scope=A');
    await page.locator('.cabinet-page').waitFor();
    await page.locator('.v2-cabinet-rate-value').getByText('95.0%',{exact:true}).waitFor();
    assert((await page.locator('.v2-cabinet-status').boundingBox()).height<120,'mobile status stays compact');
    if (!(await page.getByRole('switch', { name: '夜间模式' }).isVisible().catch(() => false))) await page.getByRole('button', { name: '导航配色', exact: true }).click();
    const mToggle = page.getByRole('switch', { name: '夜间模式' });
    await mToggle.waitFor({ state: 'visible' });
    if ((await mToggle.getAttribute('aria-checked')) !== 'true') await mToggle.click();
    await page.waitForFunction(() => document.documentElement.dataset.v2Theme === 'dark');
    await page.getByRole('button', { name: '导航配色', exact: true }).click();
    await mToggle.waitFor({ state: 'hidden' }).catch(() => {});
    await page.waitForTimeout(200);
    await page.getByRole('button', { name: '原始平面图', exact: true }).click();
    await page.getByRole('textbox', { name: '搜索平面图', exact: true }).waitFor();
    await page.locator('.map-cell').filter({ hasText: '加湿', exact: true }).first().waitFor();
    await shot('cabinet-dark-floorplan-390');
  });

  await browser.close();
  await server.close();

  const passed = executed.length - failures.length;
  console.log('\n==== COMPACT WORKSPACES EXECUTION SUMMARY ====');
  console.log('Total executed: ' + executed.length + ', passed: ' + passed + ', failed: ' + failures.length);
  if (failures.length) {
    console.log('\nEXECUTED FAILURES:');
    for (const f of failures) {
      console.log('  - ' + f.name + ': ' + f.message);
    }
    console.log('\nScreenshots saved under ' + OUTPUT);
    process.exitCode = 1;
  } else {
    console.log('ALL PASSED. screenshots in ' + OUTPUT);
  }
} catch (error) {
  record('setup', error.message, error.stack);
  try { if (page) await shot('fail-setup'); } catch {}
  console.error(error);
  process.exitCode = 1;
} finally {
  try { await browser.close(); } catch {}
  try { await server.close(); } catch {}
}
