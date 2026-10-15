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
const rows = Array.from({ length: 5 }, (_, i) => ({ record_id: 'plan-' + i, work_type: 'maintenance', building: 'E楼',
  title: ['EA118机房E楼BA系统资料维护', 'EA118机房E楼蓄电池内阻刷新维护', 'EA118机房E楼冷水机组月度维护', 'EA118机房E楼消防系统年度维保与多设备联合检查', 'EA118机房E楼空调过滤网更换维护'][i],
  source_progress: '未开始', display_fields: { '维护周期': i === 4 ? '' : ['半年', '季度', '每月', '年度'][i],
    '计划开始维护时间': '2026-10-01T09:00', '计划结束维护时间': '2026-10-31T18:30', '专业': '电气' } }));
const ongoing = rows.map((row, i) => ({ ...row, active_item_id: 'active-' + i, target_record_id: 'target-' + i,
  status: i === 3 ? '发送失败' : '开始', maintenance_cycle: i === 4 ? '' : ['半年', '季度', '每月', '年度'][i],
  start_time: '2026-10-15T09:30', updated_at: '2026-10-15T10:45', site_photo_count: 1 }));
ongoing[4].display_fields = { '专业': '暖通' };
const errors = [], writes = [], queries = [];
let user = { open_id: 'fixture-list', name: '测试值班员', role: 'building', level: 'L4', category: 'operator', modules: ['maintenance'] };
let scopeOptions = [{ value: 'E', label: 'E楼' }], defaultScope = 'E';
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
  await page.locator('.v2-notice-entry').first().waitFor();
  assert.equal(queries.at(-1),'E','single-building account defaults to its authorized building');
  await page.locator('.v2-brand img').evaluate(img => img.decode());
  assert(await page.locator('.v2-brand img').evaluate(img => img.naturalWidth > 0));
  for (const width of [1440, 1366, 1920, 390, 360]) {
    await page.setViewportSize({ width, height: width < 700 ? 844 : 1000 });
    if (width < 700) await page.getByRole('tab', { name: /计划通告/ }).click();
    const planned = page.locator('[data-list="planned"]');
    assert.match(await planned.locator('.v2-notice-plan-window').first().innerText(), /2026-10-01 09:00[\s\S]*至[\s\S]*2026-10-31 18:30/);
    assert.equal(await planned.locator('.v2-notice-cycle').first().innerText(), '周期 半年');
    assert.equal(await planned.locator('.v2-notice-entry-state .n-tag').first().innerText(), '未开始');
    const plannedWide = (await planned.boundingBox()).width >= 520;
    assert.equal(await planned.locator('.v2-notice-specialty').first().isVisible(), plannedWide, 'specialty follows the lane width, not the screen width');
    await page.screenshot({ path: path.join(output, `planned-${width}.png`), fullPage: true });
    if (width < 700) await page.getByRole('tab', { name: /进行中/ }).click();
    const active = page.locator('[data-list="ongoing"]');
    assert.equal(await active.locator('.v2-notice-entry-state .n-tag').first().innerText(), '进行中');
    assert.equal(await active.locator('.v2-notice-entry-state .n-tag').nth(3).innerText(), '发送失败');
    assert.equal(await active.locator('.v2-notice-cycle').last().innerText(), '周期未填');
    const activeWide = (await active.boundingBox()).width >= 520;
    assert.equal(await active.locator('.v2-notice-plan-window').count(), 4, 'only records with real plan dates have a plan window');
    assert.equal(await active.locator('.v2-notice-plan-window').first().isVisible(), activeWide);
    assert.equal(await active.locator('.v2-notice-specialty').first().isVisible(), activeWide);
    assert.doesNotMatch(await active.innerText(), /2026-10-15|T09:30|T10:45/);
    const boxes = await active.locator('.v2-notice-entry').evaluateAll(elements => elements.filter(el=>el.getClientRects().length).map(el => ({
      top: el.getBoundingClientRect().top, bottom: el.getBoundingClientRect().bottom,
      border: getComputedStyle(el).borderTopWidth, width: el.clientWidth, content: el.scrollWidth,
      copyRight: el.querySelector('.v2-notice-entry-copy').getBoundingClientRect().right,
      stateLeft: el.querySelector('.v2-notice-entry-state').getBoundingClientRect().left,
    })));
    assert(boxes.length>0,'the current group must contain visible records');
    for (const [i, box] of boxes.entries()) {
      assert.equal(box.border, '1px');
      assert(box.content <= box.width + 1, 'row content must fit');
      assert(box.copyRight < box.stateLeft, 'actions must not overlap text: '+width+' '+JSON.stringify(box));
      if (i) assert(box.top - boxes[i - 1].bottom >= 7, 'each notice needs visible separation');
    }
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1), false);
    await page.screenshot({ path: path.join(output, `ongoing-${width}.png`), fullPage: true });
  }
  await page.setViewportSize({width:1920,height:1000});
  const planned = page.locator('[data-list="planned"]');
  const readsBeforeResize = queries.length;
  for (const width of [430,650]) {
    await planned.evaluate((element,width)=>element.style.width=width+'px',width);
    assert.equal(await planned.locator('.v2-notice-specialty').first().isVisible(),width>=520,'a resized component changes density on the same viewport');
  }
  await planned.evaluate(element=>element.style.removeProperty('width'));
  assert.equal(queries.length,readsBeforeResize,'layout changes never fetch more data');

  user = {...user,role:'admin',level:'L1'};
  scopeOptions = [{value:'ALL',label:'全部'},...['110',...'ABCDEH'].map(value=>({value,label:value==='110'?'110站':value+'楼'}))];
  await page.goto(entry+'?view=my&scope=E');
  await page.locator('.v2-sidebar').getByText('通告办理',{exact:true}).click();
  await page.locator('.v2-notice-entry').first().waitFor();
  assert.equal(queries.at(-1),'ALL','all-building access defaults to ALL rather than the previous module scope');
  assert.match(await page.locator('.v2-notice-heading').innerText(),/全部楼栋/);
  assert.match(await page.getByLabel('通告楼栋',{exact:true}).innerText(),/全部楼栋/);
  await page.screenshot({path:path.join(output,'all-buildings-desktop.png'),fullPage:true});
  await page.setViewportSize({width:390,height:844});
  await page.goto(entry+'?view=functions&scope=E');
  await page.locator('.v2-feature-entry').filter({hasText:'通告办理'}).click();
  await page.locator('.v2-notice-entry').first().waitFor();
  assert.equal(queries.at(-1),'ALL','the mobile function entry uses the same authorized default');
  await page.setViewportSize({width:1440,height:1000});
  await page.getByLabel('通告楼栋',{exact:true}).click();
  await page.locator('.n-base-select-option').getByText('E楼',{exact:true}).click();
  await page.waitForFunction(()=>new URL(location.href).searchParams.get('scope')==='E');
  await page.waitForFunction(()=>document.querySelector('.v2-notice-heading p')?.textContent.startsWith('E楼'));
  await page.reload();await page.locator('.v2-notice-entry').first().waitFor();
  assert.equal(queries.at(-1),'E','an explicitly selected building/deep link is preserved');

  user = {...user,open_id:'fixture-partial',role:'building',level:'L4'};
  scopeOptions = [{value:'A',label:'A楼'},{value:'E',label:'E楼'}]; defaultScope='E';
  await page.goto(entry+'?__page=/workbench-lite&work_type=maintenance');
  await page.locator('.v2-notice-entry').first().waitFor();
  assert.equal(queries.at(-1),'E','partially authorized account uses the server-authorized default, not the first option');
  assert.doesNotMatch(await page.getByLabel('通告楼栋',{exact:true}).innerText(),/全部/);
  const beforeDenied = queries.length;
  await page.goto(entry+'?__page=/workbench-lite&scope=B&work_type=maintenance');
  await page.getByText('没有该功能权限',{exact:true}).waitFor();
  assert.equal(queries.length,beforeDenied,'unauthorized links do not read business records');
  defaultScope='B';
  await page.goto(entry+'?__page=/workbench-lite&work_type=maintenance');
  await page.locator('.v2-notice-entry').first().waitFor();
  assert.equal(queries.at(-1),'A','invalid/stale server default never expands permissions');
  assert.deepEqual(errors, []);
  assert.deepEqual(writes, []);
  console.log('PASS: authorized defaults, ALL label, explicit scopes, denied links, container-responsive notice metadata at 360/390/1366/1440/1920px; no extra reads or business writes.');
} finally { await browser.close(); await server.close(); }
