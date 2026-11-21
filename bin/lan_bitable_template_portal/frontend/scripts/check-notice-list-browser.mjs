import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { chromium } from 'playwright';
import { preview } from 'vite';

assert(process.env.CLIPFLOW_WORKSPACE_BUILD_DIR, 'Use an isolated workspace build directory.');
const output = path.resolve('../../../output/playwright/notice-list-metadata');
await mkdir(output, { recursive: true });
const server = await preview({ configFile: 'vite.workspace-v2.config.ts', logLevel: 'error', preview: { host: '127.0.0.1', port: 0, proxy: {} } });
const origin = `http://127.0.0.1:${server.httpServer.address().port}`;
const browser = await chromium.launch({ headless: true });
// Maintenance plan fixtures: a real plan window on every row, cycle present on the first four, absent on the fifth,
// and specialty taken from display_fields (the current notice metadata contract, noticeListMeta.ts).
const rows = Array.from({ length: 5 }, (_, i) => ({ record_id: 'plan-' + i, work_type: 'maintenance', building: 'E楼',
  title: ['EA118机房E楼BA系统资料维护', 'EA118机房E楼蓄电池内阻刷新维护', 'EA118机房E楼冷水机组月度维护', 'EA118机房E楼消防系统年度维保与多设备联合检查', 'EA118机房E楼空调过滤网更换维护'][i],
  source_progress: '未开始', display_fields: { '维护周期': i === 4 ? '' : ['半年', '季度', '每月', '年度'][i],
    '计划开始维护时间': '2026-10-01T09:00', '计划结束维护时间': '2026-10-31T18:30', '专业': '电气' } }));
// The fifth ongoing row carries no plan dates at all (only display_fields 专业), so it must not render a plan window.
const ongoing = rows.map((row, i) => ({ ...row, active_item_id: 'active-' + i, target_record_id: 'target-' + i,
  status: i === 3 ? '发送失败' : '开始', maintenance_cycle: i === 4 ? '' : ['半年', '季度', '每月', '年度'][i],
  start_time: '2026-10-15T09:30', updated_at: '2026-10-15T10:45', site_photo_count: 1 }));
ongoing[4].display_fields = { '专业': '暖通' };
const errors = [], writes = [], queries = [];
let user = { open_id: 'fixture-list', name: '测试值班员', role: 'building', level: 'L4', category: 'operator', modules: ['maintenance'] };
let scopeOptions = [{ value: 'E', label: 'E楼' }], defaultScope = 'E';

// The lanes no longer use an entry list; planned records sit in https://…/NoticeDeskRecord.vue (.desk-record:not(.is-ongoing)),
// ongoing records in .desk-record.is-ongoing. Lane visibility is a container query (notice-desk ≤760px shows one lane,
// wider shows both side-by-side) so we drive the .desk-switch segmented control only when it is actually rendered.
const switchLane = async (page, id) => page.locator(`.desk-switch button:has-text("${id === 'ongoing' ? '进行中' : '计划通告'}")`).click();

try {
  const context = await browser.newContext({ viewport: { width: 1440, height: 1000 } });
  await context.route('**/*', async route => {
    const req = route.request(), url = new URL(req.url());
    if (url.origin !== origin) return route.abort();
    if (url.pathname === '/assets/vnet-logo.png') return route.fulfill({ path: path.resolve('public/assets/vnet-logo.png'), contentType: 'image/png' });
    if (!url.pathname.startsWith('/api/')) return route.continue();
    if (req.method() !== 'GET') { writes.push(url.pathname); return route.fulfill({ status: 400, json: { ok: false, error: 'No business writes in this test' } }); }
    const ok = data => route.fulfill({ json: { ok: true, data } });
    if (url.pathname === '/api/auth/status') return ok({ logged_in: true, user, scope_options: scopeOptions, default_scope: defaultScope });
    if (url.pathname === '/api/health') return route.fulfill({ json: { ok: true, service: 'clipflow_backend', instance_id: 'notice-list-test' } });
    if (url.pathname === '/api/workbench') { queries.push(url.searchParams.get('scope')); return ok({ records: rows, ongoing, records_pagination: { total: 5 }, ongoing_pagination: { total: 5 } }); }
    return ok({ items: [], records: [], options: [], settings: {}, available: false });
  });
  const page = await context.newPage();
  page.on('pageerror', error => errors.push(error.message));
  const entry = origin + '/assets/workspace-v2/index.html';
  await page.goto(entry + '?__page=/workbench-lite&work_type=maintenance');
  await page.locator('.desk-record').first().waitFor();
  assert.equal(queries.at(-1), 'E', 'single-building account defaults to its authorized building');
  // The shell brand is now an inline SVG mark (TopNavigation.vue .topnav-brand), not a raster <img>.
  const brand = page.getByRole('navigation', { name: '功能导航' }).locator('.topnav-brand .topnav-mark svg');
  const brandBox = await brand.boundingBox();
  assert(brandBox && brandBox.width > 0 && brandBox.height > 0, 'the navigation brand mark renders with real size');

  for (const width of [1440, 1366, 1920, 390, 360]) {
    await page.setViewportSize({ width, height: width < 700 ? 844 : 1000 });
    // Both lanes render side-by-side on desktop; below the notice-desk container breakpoint the segmented
    // switch picks one lane, and the pressed (aria-pressed=true) lane is the visible one.
    const lanesStacked = width < 700;
    if (lanesStacked) await switchLane(page, 'planned');
    const planned = page.locator('[data-list="planned"]');
    assert.equal(await planned.isVisible(), true);
    const plannedRecords = planned.locator('.desk-record:not(.is-ongoing)');
    assert.equal(await plannedRecords.count(), 5);
    // Metadata: formatted plan window (T separator normalised), cycle label from 维护周期, source progress tag.
    assert.match(await plannedRecords.first().locator('.record-window').innerText(), /计划时段[\s\S]*2026-10-01 09:00[\s\S]*→[\s\S]*2026-10-31 18:30/);
    assert.equal(await plannedRecords.first().locator('.record-meta span').nth(2).innerText(), '周期 半年');
    assert.equal(await plannedRecords.first().locator('.record-status').innerText(), '未开始');
    // The fifth plan has no 维护周期, so it falls back to 周期未填 instead of an empty cell.
    assert.equal(await plannedRecords.nth(4).locator('.record-meta span').nth(2).innerText(), '周期未填');
    // Specialty comes from display_fields['专业']; it is always rendered in the meta line on ongoing/planned records.
    assert.equal(await plannedRecords.first().locator('.record-meta span').nth(1).innerText(), '电气');
    await page.screenshot({ path: path.join(output, `planned-${width}.png`), fullPage: true });

    if (lanesStacked) await switchLane(page, 'ongoing');
    const active = page.locator('[data-list="ongoing"]');
    assert.equal(await active.isVisible(), true);
    const activeRecords = active.locator('.desk-record.is-ongoing');
    assert.equal(await activeRecords.count(), 5);
    // Ongoing status is derived from the live notice state, not the source plan progress.
    assert.equal(await activeRecords.first().locator('.record-status').innerText(), '进行中');
    assert.equal(await activeRecords.nth(3).locator('.record-status').innerText(), '发送失败');
    assert.equal(await activeRecords.nth(3).locator('.record-status').getAttribute('data-tone'), 'error');
    // The last ongoing record has no cycle at all → 周期未填; the others show 周期 <value>.
    assert.equal(await activeRecords.last().locator('.record-meta span').nth(2).innerText(), '周期未填');
    // Only rows with real plan dates render a plan window; the fifth ongoing row has none.
    assert.equal(await activeRecords.locator('.record-window').count(), 4, 'only records with real plan dates have a plan window');
    // Send/update time and remembered draft timestamps must never leak into the list.
    assert.doesNotMatch(await active.innerText(), /2026-10-15|T09:30|T10:45/);
    // Site photo count is rendered for maintenance rows.
    assert.match(await activeRecords.first().locator('footer').innerText(), /现场照片 1 张/);

    const boxes = await activeRecords.evaluateAll(elements => elements.filter(el => el.getClientRects().length).map(el => {
      const rect = el.getBoundingClientRect();
      return {
        top: rect.top, bottom: rect.bottom, left: rect.left, right: rect.right,
        borderTop: getComputedStyle(el).borderTopWidth,
        isOngoing: el.classList.contains('is-ongoing'),
        width: el.clientWidth, content: el.scrollWidth,
        heading: el.querySelector('.record-heading').getBoundingClientRect(),
        title: el.querySelector('h3').getBoundingClientRect(),
        actions: el.querySelector('.record-actions').getBoundingClientRect(),
        meta: el.querySelector('.record-meta').getBoundingClientRect(),
        window: (el.querySelector('.record-window') || { getBoundingClientRect: () => null }).getBoundingClientRect(),
      };
    }));
    assert(boxes.length > 0, 'the current lane must contain visible records');
    for (const [i, box] of boxes.entries()) {
      assert.equal(box.isOngoing, true, 'ongoing lane only renders ongoing cards');
      assert.equal(box.borderTop, '1px', 'ongoing card keeps its visible frame');
      assert(box.content <= box.width + 1, 'row content must fit its own box: ' + width + ' ' + JSON.stringify(box));
      // The action cluster (结束/更新) is right-aligned in the heading; it must not run into the location/status copy.
      assert(box.actions.left >= box.heading.left, 'actions stay inside the heading');
      assert(box.actions.right <= box.heading.right + 1, 'actions stay inside the heading box');
      // Title, meta and (when present) the plan window must sit below the heading row, never behind the actions.
      assert(box.title.top >= box.heading.bottom - 1, 'title starts below the heading row');
      assert(box.meta.top >= box.title.bottom - 1, 'meta follows the title');
      if (box.window && box.window.width) assert(box.window.top >= box.meta.bottom - 1, 'plan window follows meta');
      // Consecutive notice cards need visible separation.
      for (const other of boxes.slice(0, i)) {
        assert(Math.max(box.left - other.right, other.left - box.right, box.top - other.bottom, other.top - box.bottom) >= 7, 'each notice needs visible separation');
      }
    }
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1), false, 'no horizontal page scroll at ' + width + 'px');
    await page.screenshot({ path: path.join(output, `ongoing-${width}.png`), fullPage: true });
  }

  // Container-driven density: the notice list reacts to its own lane width, not the viewport width.
  await page.setViewportSize({ width: 1920, height: 1000 });
  const planned = page.locator('[data-list="planned"]');
  const readsBeforeResize = queries.length;
  for (const width of [430, 650]) {
    await planned.evaluate((element, width) => element.style.width = width + 'px', width);
    assert.equal(await planned.locator('.desk-record:not(.is-ongoing)').first().isVisible(), true, 'a resized lane keeps its records: ' + width);
  }
  await planned.evaluate(element => element.style.removeProperty('width'));
  assert.equal(queries.length, readsBeforeResize, 'layout changes never fetch more data');

  // All-building access defaults to ALL, regardless of a stale ?scope from the link.
  user = { ...user, role: 'admin', level: 'L1' };
  scopeOptions = [{ value: 'ALL', label: '全部' }, ...['110', ...'ABCDEH'].map(value => ({ value, label: value === '110' ? '110站' : value + '楼' }))];
  await page.goto(entry + '?view=my&scope=E');
  await page.getByRole('navigation', { name: '功能导航' }).getByRole('button', { name: '通告中心', exact: true }).click();
  await page.locator('.desk-record').first().waitFor();
  assert.equal(queries.at(-1), 'ALL', 'all-building access defaults to ALL rather than the previous module scope');
  assert.match(await page.getByLabel('通告楼栋', { exact: true }).innerText(), /全部授权楼栋/);
  await page.screenshot({ path: path.join(output, 'all-buildings-desktop.png'), fullPage: true });

  // The mobile directory entry honours the same authorized default.
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto(entry + '?view=functions');
  await page.locator('.directory-link[data-nav-entry="notices"]').click();
  await page.locator('.desk-record').first().waitFor();
  assert.equal(queries.at(-1), 'ALL', 'the mobile function entry uses the same authorized default');

  // An explicit building/deep link is preserved.
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.getByLabel('通告楼栋', { exact: true }).click();
  await page.locator('.n-base-select-option').getByText('E楼', { exact: true }).click();
  await page.keyboard.press('Escape');
  await page.waitForFunction(() => new URL(location.href).searchParams.get('scope') === 'E');
  await page.getByLabel('通告楼栋', { exact: true }).getByText('E楼', { exact: true }).waitFor();
  await page.reload(); await page.locator('.desk-record').first().waitFor();
  assert.equal(queries.at(-1), 'E', 'an explicitly selected building/deep link is preserved');

  // Partially authorized account: never falls back to the first option, never ignores the server default.
  user = { ...user, open_id: 'fixture-partial', role: 'building', level: 'L4' };
  scopeOptions = [{ value: 'A', label: 'A楼' }, { value: 'E', label: 'E楼' }]; defaultScope = 'E';
  await page.goto(entry + '?__page=/workbench-lite&work_type=maintenance');
  await page.locator('.desk-record').first().waitFor();
  assert.equal(queries.at(-1), 'E', 'partially authorized account uses the server-authorized default, not the first option');
  assert.doesNotMatch(await page.getByLabel('通告楼栋', { exact: true }).innerText(), /全部/);

  // An unauthorized deep link must not read business records.
  const beforeDenied = queries.length;
  await page.goto(entry + '?__page=/workbench-lite&scope=B&work_type=maintenance');
  await page.getByText('没有该功能权限', { exact: true }).waitFor();
  assert.equal(queries.length, beforeDenied, 'unauthorized links do not read business records');

  // A stale server default outside the authorized scopes falls back to the first authorized building.
  defaultScope = 'B';
  await page.goto(entry + '?__page=/workbench-lite&work_type=maintenance');
  await page.locator('.desk-record').first().waitFor();
  assert.equal(queries.at(-1), 'A', 'invalid/stale server default never expands permissions');

  assert.deepEqual(errors, []);
  assert.deepEqual(writes, []);
  console.log('PASS: real V2 planned/ongoing notice metadata, authorized defaults, ALL label, explicit scopes, denied links, container-responsive notice lists at 360/390/1366/1440/1920px; no extra reads or business writes.');
} finally { await browser.close(); await server.close(); }
