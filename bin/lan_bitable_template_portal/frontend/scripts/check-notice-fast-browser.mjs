import assert from 'node:assert/strict';
import http from 'node:http';
import { spawnSync } from 'node:child_process';
import { readFile, mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../..');
const dist = path.join(repo, 'bin/lan_bitable_template_portal/frontend/dist');
const output = path.join(repo, 'output/playwright/notice-fast');
await mkdir(output, { recursive: true });
const result = spawnSync(path.join(repo, 'bin/.venv/Scripts/python.exe'), ['-c', `
import sys,json
sys.path.insert(0,'bin')
from lan_bitable_template_portal.workbench_lite import render_workbench_lite,extract_workbench_lite_fragments
from test_lighthouse_notice_command_regression import NoticeCommandRegressionBase,VALID_STARTS
test=NoticeCommandRegressionBase();test.setUp()
try:
    pages={};plans={}
    for kind,draft in VALID_STARTS.items():
        label={'maintenance':'维保','change':'变更','repair':'设备检修','power':'上电','polling':'设备轮巡','adjust':'设备调整'}[kind]
        decision=test._start_decision(kind,body_extra={'manual_binding_choice':'unbound'})
        decision['operations'][0]['body']['patch']['title']='A楼'+label+'测试'
        decision['operations'][0]['body']['patch']['progress']='已完成60%'
        plan=test.agent.prepare({'id':'fixture','scopes':['A'],'is_admin':True},decision,'browser-fast-'+kind,[])
        plan['status']='awaiting_confirmation'
        plans[kind]=test.agent.public_plan(plan)
        row={**draft,'title':'A楼'+label+'测试','active_item_id':'active-'+kind,'target_record_id':'rec-'+kind,'record_id':'rec-'+kind,
             'work_type':kind,'scope':'A','building_codes':['A'],'status':'开始'}
        payload={'records':[],'ongoing':[row]}
        args=dict(payload=payload,session={'is_admin':True,'user':{'name':'Fixture'}},scope='A',work_type=kind,month='10月',
                  scope_options=[{'value':'A','label':'A楼'}])
        html=render_workbench_lite(**args)
        detail=extract_workbench_lite_fragments(render_workbench_lite(**args,active_item_id=row['active_item_id']))['detail']
        linked_record={**draft,'record_id':'plan-'+kind,'work_type':kind,'linked_ongoing':row,
                       'display_fields':{'名称':row['title'],'楼栋':'A楼','专业':'电气','计划进度':'进行中'}}
        linked_html=render_workbench_lite(**{**args,'payload':{'records':[linked_record],'ongoing':[row]}})
        pages[kind]={'html':html,'detail':detail,'linked_html':linked_html}
    print(json.dumps({'plans':plans,'pages':pages},ensure_ascii=False))
finally: test.tmp.cleanup()
`], { cwd: repo, encoding: 'utf8', maxBuffer: 16 * 1024 * 1024, env: { ...process.env, PYTHONIOENCODING: 'utf-8' } });
assert.equal(result.status, 0, result.stderr);
const fixture = JSON.parse(result.stdout);
let frameLoads = 0, fragments = 0, frameGate = null;
const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url, 'http://localhost');
    if (url.pathname === '/native') {
      res.setHeader('Content-Type', 'text/html; charset=utf-8');
      const entry = fixture.pages[url.searchParams.get('kind')];
      return res.end(url.searchParams.has('source') ? entry.linked_html : entry.html);
    }
    if (url.pathname === '/workbench-lite' && url.searchParams.has('_assistant_frame')) {
      frameLoads++;
      if (frameGate) await frameGate;
      res.setHeader('Content-Type', 'text/html; charset=utf-8');
      return res.end(fixture.pages.maintenance.html);
    }
    const appPage = ['/cabinet-power', '/workbench-lite', '/home-test', '/plan-convergence'].includes(url.pathname);
    const filename = path.resolve(dist, '.' + (appPage ? '/index.html' : url.pathname === '/' ? '/assistant.html' : url.pathname));
    assert(filename.startsWith(dist + path.sep));
    let bytes = await readFile(filename);
    if (filename.endsWith('assistant.html')) bytes = Buffer.from(bytes.toString().replace('id="clipflow-lighthouse-widget"', 'id="clipflow-lighthouse-widget" data-user-id="notice-fast-fixture" data-user-name="Fixture"'));
    res.setHeader('Content-Type', filename.endsWith('.js') ? 'application/javascript' : filename.endsWith('.css') ? 'text/css' : 'text/html; charset=utf-8');
    res.end(bytes);
  } catch { res.writeHead(404); res.end(); }
});
await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
const base = 'http://127.0.0.1:' + server.address().port;
const browser = await chromium.launch({ headless: true });
const errors = [];
try {
  const context = await browser.newContext({ viewport: { width: 1440, height: 1000 } });
  let details = 0;
  let detailGate = null;
  let reviewKind = 'maintenance';
  const planReads = [];
  await context.route(base + '/api/**', async route => {
    const url = new URL(route.request().url());
    if (url.pathname === '/api/cabinet-power/bootstrap' && route.request().method() === 'POST') {
      return route.fulfill({ json: { ok: true, data: { status: 'succeeded', ready: 5, total: 5,
        buildings: [...'ABCDE'].map(scope => ({ scope, status: 'succeeded' })) } } });
    }
    if (url.pathname === '/api/workbench/draft' && route.request().method() === 'PUT') return route.fulfill({ json: { ok: true, data: { draft: { version: 1 } } } });
    assert.equal(route.request().method(), 'GET', 'Browser test must never submit business');
    let data = {};
    if (url.pathname.startsWith('/api/plan-convergence/')) {
      planReads.push(url.pathname + url.search);
      if (url.pathname === '/api/plan-convergence/bootstrap') data = { is_admin: true, catalog_ready: true, points_ready: false,
        counts: { devices: 2, rules: 1 }, zh_ready: true, blocks: { items: [{ blockId: '123', blockName: '离线缓存记录', status: 1 }], loaded_at: 123 } };
      else if (url.pathname === '/api/plan-convergence/rulesets') data = [];
      else if (url.pathname === '/api/plan-convergence/catalog') data = { items: [] };
      else if (url.pathname === '/api/plan-convergence/blocks' && url.searchParams.get('refresh') === '1') {
        return route.fulfill({ status: 502, json: { ok: false, code: 'zh_connection_unavailable',
          error: '无法连接智航，请在运行灯塔的电脑上连接 VPN 后重试。其他功能和本地规则配置不受影响' } });
      } else assert.fail('Unexpected remote access: ' + url.pathname);
      return route.fulfill({ json: { ok: true, data } });
    }
    if (url.pathname === '/api/health') return route.fulfill({ json: { ok: true, service: 'clipflow_backend', instance_id: 'notice-fast-fixture' } });
    if (url.pathname === '/api/workbench/lite-detail') {
      details++;
      const gate = detailGate;
      if (gate) await gate;
      else await new Promise(resolve => setTimeout(resolve, 180));
      data = { canonical_url: '/workbench-lite?' + url.searchParams, fragments: { detail: fixture.pages[url.searchParams.get('work_type')].detail } };
    } else if (url.pathname === '/api/workbench/lite-fragment') {
      fragments++;
      return route.fulfill({ status: 503, json: { ok: false, error: 'Fixture must not reload unchanged lists' } });
    } else if (url.pathname === '/api/auth/status') data = { logged_in: true, user: { open_id: 'fixture-nav', name: 'Fixture', role: 'admin' },
      scope_options: [...'ABCDE'].map(value => ({ value, label: value + '楼' })) };
    else if (url.pathname === '/api/cabinet-power/buildings') data = { buildings: [...'ABCDE'].map(scope => ({ scope, bootstrap_status: 'succeeded',
      counts: { total: 100, powered: 80, formal: 60, test: 20, off: 20, unknown: 0 }, record_count: 100, inventory_only: 0 })) };
    else if (url.pathname === '/api/cabinet-power/exports/batches') data = { items: [] };
    else if (url.pathname === '/api/scope-overview') data = { scopes: {} };
    else if (url.pathname === '/api/handover-links') data = { links: {} };
    else if (url.pathname === '/api/assistant/appearance') data = { color: 'encre', size: 56, shape: 'cercle', snap_back: true };
    else if (url.pathname === '/api/assistant/conversation') data = { configured: true, enabled: true, conversation_id: 'notice-fast', busy: false,
      turns: [{ operation_id: 'notice-fast', question: '发送通告', answer: '请核对通告后确认。', status: 'completed', plan: fixture.plans[reviewKind] }] };
    return route.fulfill({ json: { ok: true, data } });
  });
  const page = await context.newPage();
  page.on('pageerror', error => errors.push(error.message));
  for (const kind of Object.keys(fixture.pages)) {
    let releaseDetail;
    detailGate = new Promise(resolve => { releaseDetail = resolve; });
    await page.goto(base + '/native?kind=' + kind);
    const row = page.locator('.ongoing-row').first();
    await row.click();
    try { await page.locator('#lite-notice-detail-overlay.open').waitFor(); }
    catch (error) {
      await page.screenshot({ path: path.join(output, 'opening-failure-' + kind + '.png') });
      console.error(kind, await page.locator('#lite-notice-detail-overlay').evaluate(el => ({ outer: el.outerHTML.slice(0, 1200), css: { display: getComputedStyle(el).display, opacity: getComputedStyle(el).opacity, visibility: getComputedStyle(el).visibility } })), errors);
      throw error;
    }
    assert.equal(await page.locator('[name=target_record_id]').inputValue(), 'rec-' + kind);
    assert.equal(await page.locator('[name=active_item_id]').inputValue(), 'active-' + kind);
    await page.evaluate(() => { window.fixtureDrawer = document.getElementById('lite-notice-detail-overlay'); });
    const before = await page.locator('#detail-panel').evaluate(el => el.getBoundingClientRect().width);
    assert.equal(await page.locator('#lite-notice-detail-overlay').evaluate(el => getComputedStyle(el).backdropFilter), 'none');
    assert.equal(await page.locator('button[name=submit_action]').count(), 2);
    assert.equal(await page.locator('.manual-source-binding-head strong').innerText(), '源表事项关联（更新前可选）');
    assert.equal(await page.locator('.manual-source-binding-actions [data-manual-binding-mode=unbound]').isVisible(), false);
    if (kind === 'maintenance') {
      await page.locator('[name=content]').fill('User edit must survive background verification');
      detailGate = null; releaseDetail();
      await page.waitForTimeout(450);
      assert.equal(await page.locator('[name=content]').inputValue(), 'User edit must survive background verification');
      await page.screenshot({ path: path.join(output, 'ongoing-detail.png') });
      await page.evaluate(() => {
        const form = document.getElementById('lite-notice-form');
        setFormValue(form, 'source_record_id', 'fixture-existing-binding');
        setFormValue(form, 'manual_binding_choice', 'bind');
        syncManualSourceBindingControls(form);
      });
      assert.equal(await page.locator('.manual-source-binding-head strong').innerText(), '源表');
      assert.equal(await page.locator('.manual-source-recommendations').isVisible(), false);
      assert.equal(await page.getByRole('button', { name: '重新绑定', exact: true }).isVisible(), true);
    } else { detailGate = null; releaseDetail(); await page.waitForTimeout(450); }
    assert(Math.abs(await page.locator('#detail-panel').evaluate(el => el.getBoundingClientRect().width) - before) < 1);
    assert(await page.evaluate(() => window.fixtureDrawer === document.getElementById('lite-notice-detail-overlay')), 'detail must not recreate the drawer shell');
  }
  assert.equal(details, 6);
  await page.goto(base + '/native?kind=maintenance&source=1');
  const linkedSource = page.locator('.notice-row[data-linked-ongoing="1"]').first();
  assert.equal(await linkedSource.getAttribute('aria-disabled'), 'true', 'started plans keep their original disabled entry');
  await page.locator('.ongoing-row').first().click();
  await page.locator('#lite-notice-detail-overlay.open').waitFor();
  const linkedWidth = await page.locator('#detail-panel').evaluate(el => el.getBoundingClientRect().width);
  assert.equal(await page.locator('[name=target_record_id]').inputValue(), 'rec-maintenance');
  assert.equal(await page.locator('[name=active_item_id]').inputValue(), 'active-maintenance');
  assert.equal(await page.locator('button[name=submit_action][value=update]').count(), 1);
  await page.waitForTimeout(1300);
  assert(Math.abs(await page.locator('#detail-panel').evaluate(el => el.getBoundingClientRect().width) - linkedWidth) < 1, 'loading a binding panel must not resize the drawer');
  assert.equal(await page.locator('[name=target_record_id]').inputValue(), 'rec-maintenance');
  assert.equal(details, 7, 'a linked plan is operated through its original ongoing notice, not another start');
  await page.locator('#lite-notice-drawer-close').click();
  await page.evaluate(() => { window.fixtureTimeout = window.setTimeout; window.setTimeout = (fn, ms, ...args) => window.fixtureTimeout(fn, ms === 15000 ? 60 : ms, ...args); });
  detailGate = new Promise(() => {});
  await page.evaluate(async () => {
    await navigateLite('/workbench-lite?scope=A&work_type=maintenance&active_item_id=active-maintenance', { detailOnly: true })
      .then(() => { throw new Error('Expected timeout'); }, error => { if (!error.message.includes('读取超时')) throw error; });
  });
  assert.equal(await page.locator('#detail-panel.loading').count(), 0);
  assert.equal(await page.locator('.ongoing-row').count(), 1, 'timeout retains the current list');
  detailGate = null;
  await page.evaluate(() => { window.setTimeout = window.fixtureTimeout; });
  let releaseLate;
  detailGate = new Promise(resolve => { releaseLate = resolve; });
  await page.locator('.ongoing-row').first().click();
  await page.locator('#lite-notice-detail-overlay.open').waitFor();
  await page.locator('#lite-notice-drawer-close').click();
  detailGate = null; releaseLate();
  await page.waitForTimeout(400);
  assert.equal(await page.locator('#lite-notice-detail-overlay.open').count(), 0, 'a late response must not reopen a closed notice');
  for (const kind of Object.keys(fixture.plans)) {
    reviewKind = kind;
    await page.goto(base);
    await page.locator('.assistant-launcher').waitFor();
    const opener = page.getByRole('button', { name: '打开灯塔助手', exact: true });
    if (await opener.count()) await opener.click();
    await page.locator('.operation-plan').waitFor();
    const text = await page.locator('.operation-plan').innerText();
    for (const internal of ['building', 'location', 'notice_type', 'building_codes', 'polling_run_count', 'polling_runs']) assert(!text.includes(internal), 'raw field visible: ' + internal);
    assert(text.includes('作为独立通告'));
    if (kind === 'adjust') assert(!text.includes('进度') && !text.includes('已完成60%'));
    else assert(text.includes(kind === 'repair' ? '完成情况' : '进度'));
    if (kind === 'power') assert(text.includes('柜号') && !text.includes('原因'));
    if (kind === 'polling') assert(text.includes('设备') && !text.includes('维保周期'));
    await page.screenshot({ path: path.join(output, 'assistant-' + kind + '-review.png') });
  }
  reviewKind = 'maintenance';
  fixture.plans.maintenance.status = 'running';
  fixture.plans.maintenance.results = [{ ok: true, api_id: 'POST /api/workbench-actions', data: { job_id: 'same-job' },
    job_result: { ok: true, data: { job_id: 'same-job', phase: 'uploading', status: 'running' } } }];
  await page.goto(base);
  await page.locator('.assistant-launcher').waitFor();
  const progressOpener = page.getByRole('button', { name: '打开灯塔助手', exact: true });
  if (await progressOpener.count()) await progressOpener.click();
  await page.locator('.operation-plan').waitFor();
  const runningText = await page.locator('.plan-title > span').innerText();
  assert.equal(runningText, '正在上传多维');
  assert.equal(await page.locator('.plan-results > div > span').innerText(), '正在上传多维');

  const shell = await context.newPage();
  shell.on('pageerror', error => errors.push(error.message));
  await shell.goto(base + '/workbench-lite?scope=A&work_type=maintenance');
  const native = shell.frameLocator('iframe[title="通告管理"]');
  await native.locator('.ongoing-row').first().waitFor();
  // Home entry omits month and pagination; retained child URLs include them.
  await shell.locator('iframe').evaluate(el => { el.contentWindow.setLiteLocation('/workbench-lite?work_type=maintenance&scope=A&month=' + (new Date().getMonth()+1) + '%E6%9C%88&pending_page=1&ongoing_page=1', true); });
  await shell.locator('iframe').evaluate(el => { el.contentWindow.fixtureDocument = 'preserve-me'; });
  // Use the application's existing history navigation rather than reloading the document.
  await shell.evaluate(() => { history.pushState({}, '', '/home-test'); dispatchEvent(new PopStateEvent('popstate')); });
  await shell.waitForFunction(() => document.querySelector('iframe')?.closest('section')?.style.display === 'none');
  await shell.evaluate(() => { history.pushState({}, '', '/workbench-lite?scope=A&work_type=maintenance'); dispatchEvent(new PopStateEvent('popstate')); });
  await native.locator('.ongoing-row').first().waitFor();
  assert.equal(frameLoads, 1, 'returning to notices must reuse the native document');
  assert.equal(await shell.locator('iframe').evaluate(el => el.contentWindow.fixtureDocument), 'preserve-me');
  await shell.waitForTimeout(400);
  assert.equal(fragments, 0, 'returning with equivalent defaults must not fetch the workbench again');
  await shell.locator('iframe').evaluate(el => {
    el.contentWindow.applyQtActiveIdentitySnapshot({ display_signature: 'same-version' });
    el.contentWindow.applyQtActiveIdentitySnapshot({ display_signature: 'changed-version' });
  });
  await shell.waitForTimeout(400);
  assert.equal(fragments, 1, 'a real source version change still refreshes the retained list');
  assert.equal(await native.locator('.ongoing-row').count(), 1, 'failed refresh retains visible records');
  await shell.locator('iframe').evaluate(el => { el.contentWindow.applyQtActiveIdentitySnapshot({ display_signature: 'changed-version' }); });
  await shell.waitForTimeout(200);
  assert.equal(fragments, 1, 'the same source version never repeatedly refreshes');
  await shell.evaluate(() => { history.pushState({}, '', '/cabinet-power'); dispatchEvent(new PopStateEvent('popstate')); });
  await shell.locator('.buildings .building').nth(4).waitFor();
  for (const width of [1366, 1440, 1920]) {
    await shell.setViewportSize({ width, height: 1000 });
    const boxes = await shell.locator('.buildings .building').evaluateAll(elements => elements.map(el => {
      const r = el.getBoundingClientRect(); return { top: r.top, left: r.left, right: r.right };
    }));
    assert.equal(boxes.length, 5);
    assert(boxes.every(box => Math.abs(box.top - boxes[0].top) < 2), 'five PC buildings must occupy one row');
    const pageWidth = await shell.locator('.cabinet-page').evaluate(el => el.getBoundingClientRect().width);
    assert(pageWidth > Math.min(width - 100, 1750), 'cabinet pages must not shrink to the intrinsic card width');
  }
  await shell.screenshot({ path: path.join(output, 'cabinet-wide.png'), fullPage: true });
  await shell.close();
  let releaseFrame;
  frameGate = new Promise(resolve => { releaseFrame = resolve; });
  const late = await context.newPage();
  late.on('pageerror', error => errors.push(error.message));
  await late.addInitScript(() => { const original = window.setTimeout; window.setTimeout = (fn, ms, ...args) => original(fn, ms === 15000 ? 1200 : ms, ...args); });
  await late.goto(base + '/workbench-lite?scope=A&work_type=maintenance', { waitUntil: 'domcontentloaded' });
  await late.locator('.async-page-state.failed').waitFor();
  frameGate = null; releaseFrame();
  await late.frameLocator('iframe[title="通告管理"]').locator('.ongoing-row').first().waitFor();
  await late.locator('.frame-state').waitFor({ state: 'detached' });
  await late.close();
  const offline = await context.newPage();
  offline.on('pageerror', error => errors.push(error.message));
  await offline.goto(base + '/plan-convergence');
  await offline.locator('.pc-record').first().waitFor();
  assert.match(await offline.locator('.pc-alert.warning').innerText(), /电脑连接 VPN/);
  assert.deepEqual(planReads, ['/api/plan-convergence/bootstrap', '/api/plan-convergence/rulesets'], 'opening must only read local state');
  await offline.getByRole('button', { name: '刷新屏蔽记录', exact: true }).click();
  await offline.locator('.pc-alert.error').waitFor();
  assert.match(await offline.locator('.pc-alert.error').innerText(), /VPN/);
  assert.match(await offline.locator('.pc-record').innerText(), /离线缓存记录/);
  assert.equal(await offline.getByRole('button', { name: '刷新屏蔽记录', exact: true }).isEnabled(), true);
  await offline.screenshot({ path: path.join(output, 'vpn-offline-plan.png'), fullPage: true });
  await offline.getByRole('button', { name: '规则配置', exact: true }).click();
  await offline.locator('.pc-rules').waitFor({ state: 'visible' });
  await offline.evaluate(() => { history.pushState({}, '', '/cabinet-power'); dispatchEvent(new PopStateEvent('popstate')); });
  await offline.locator('.buildings .building').nth(4).waitFor();
  await offline.screenshot({ path: path.join(output, 'vpn-offline-other-page.png'), fullPage: true });
  await offline.close();
  assert.deepEqual(errors, []);
  console.log('[NoticeFastBrowser] OK: stable local drawers, retained navigation, wide PC buildings and isolated VPN failures. No business writes.');
} finally {
  await browser.close();
  await new Promise(resolve => server.close(resolve));
}
