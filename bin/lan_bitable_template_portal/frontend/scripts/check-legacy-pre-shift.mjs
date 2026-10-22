import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createRequire } from 'node:module';
import { build } from 'esbuild';
import { chromium } from 'playwright';
import { preview } from 'vite';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const compiled = await build({ entryPoints: [path.join(root, 'src/preShiftUtils.ts')], bundle: true, platform: 'node', format: 'cjs', write: false });
const module = { exports: {} };
new Function('module', 'exports', 'require', compiled.outputFiles[0].text)(module, module.exports, createRequire(import.meta.url));
const { preShiftScopes, preShiftSummary } = module.exports;
const user = { open_id: 'legacy-check-user', name: '隔离测试人员', role: 'building', category: 'operator', level: 'L4', home_scope: 'A', scope_rules: ['A'], modules: ['maintenance'] };
assert.deepEqual(preShiftScopes(user), ['A']);
assert.deepEqual(preShiftScopes({ ...user, category: 'engineer' }), []);
assert.deepEqual(preShiftScopes({ ...user, role: 'guest' }), []);
assert.deepEqual(preShiftScopes({ ...user, modules: [] }), []);
assert.deepEqual(preShiftScopes({ ...user, category: 'supervisor', scope_rules: ['B'] }), ['B']);
assert.equal(preShiftScopes({ ...user, level: 'L1', scope_rules: ['ALL'] }).length, 7);
const now = Date.parse('2026-10-17T01:00:00+08:00') / 1000;
const items = [{ id: 'one', text: '核对交接班记录' }, { id: 'two', text: '检查本班设备运行状态' }];
const task = { id: 'legacy-shift', scope: 'A', day: '2026-10-16', shift: '夜', due: now - 3600, deadline: now + 3600, items, submitted: '', people: [{ name: user.name }], can_submit: true, definition_version: 1, answers: [], files: [], can_upload_handover: true };
const state = { server_time: '2026-10-17T01:00:00+08:00', scope: 'A', scopes: ['A'], definition: { version: 1, items }, tasks: [task], roster_ready: true, can_manage: false };
assert.deepEqual(preShiftSummary(state), { pending: 1, completed: 0 }, 'overnight checks remain pending');
for (const changed of [{ due: now + 1 }, { items: [] }]) {
  assert.deepEqual(preShiftSummary({ ...state, tasks: [{ ...task, ...changed }] }), { pending: 0, completed: 0 });
}
assert.deepEqual(preShiftSummary({ ...state, definition: { items: [] } }), { pending: 0, completed: 0 });
assert.deepEqual(preShiftSummary({ ...state, tasks: [{ ...task, submitted: 'done' }] }), { pending: 0, completed: 1 });
const done = { ...task, submitted: 'done' };
assert.deepEqual(preShiftSummary({ ...state, tasks: [done, { ...task, id: 'later' }] }), { pending: 1, completed: 1 }, 'another due group must remain independent');
assert.deepEqual(preShiftSummary({ ...state, tasks: [{ ...task, deadline: now }] }), { pending: 1, completed: 0 }, 'overdue unfinished work remains visible for make-up');
assert.deepEqual(preShiftSummary({ ...state, tasks: [{ ...task, submitted: 'done', deadline: now }] }), { pending: 0, completed: 0 }, 'completed work hides when its window ends');
assert.deepEqual(preShiftSummary({ ...state, tasks: [done, { ...task, due: now + 60 }] }), { pending: 0, completed: 1 }, 'a future check never pulses');
for (const other of [{ shift: '白' }, { scope: 'B' }, { day: '2026-10-17' }]) {
  assert.deepEqual(preShiftSummary({ ...state, tasks: [done, { ...task, ...other }] }), { pending: 1, completed: 1 }, 'completion never crosses building, date or shift');
}
assert.throws(() => preShiftSummary({ ...state, server_time: 'bad' }));
assert.throws(() => preShiftSummary({ ...state, tasks: [{ ...task, deadline: null }] }));

const output = path.resolve(root, '../../../output/playwright/legacy-pre-shift');
await mkdir(output, { recursive: true });
const server = await preview({ root, ...(process.env.CLIPFLOW_LEGACY_BUILD_DIR ? {build:{outDir:process.env.CLIPFLOW_LEGACY_BUILD_DIR}} : {}), logLevel: 'error', preview: { host: '127.0.0.1', port: 0 } });
const origin = 'http://127.0.0.1:' + server.httpServer.address().port;
const browser = await chromium.launch({ headless: true });
let page, mode = 'operator', submitted = false, readFail = false, submitFail = false, checks = 0, writes = 0, uploads = 0;
let scheduledTasks = null, serverTime = state.server_time;
const errors = [];
try {
  const context = await browser.newContext({ viewport: { width: 1440, height: 960 } });
  await context.route('**/*', async route => {
    const req = route.request(), url = new URL(req.url());
    if (url.origin !== origin) return route.abort();
    if (!url.pathname.startsWith('/api/')) return route.continue();
    const ok = data => route.fulfill({ json: { ok: true, data } });
    if (url.pathname === '/api/auth/status') return ok({ logged_in: true, user: { ...user, category: mode, role: mode === 'guest' ? 'guest' : 'building' }, scope_options: [{ value: 'A', label: 'A楼' }] });
    if (url.pathname === '/api/pre-shift-checks') {
      checks++;
      if (readFail) return route.fulfill({ status: 503, json: { ok: false, error: '隔离测试：状态读取失败' } });
      return ok({ ...state, server_time: serverTime, can_manage: mode === 'manager', tasks: scheduledTasks || [{ ...task, submitted: submitted ? '2026-10-17T01:01:00+08:00' : '', actor_name: user.name, can_submit: !submitted, sync_status: 'synced', files: uploads ? [{ id: 'test-file', name: '检查附件.txt', item_id: 'one' }] : [], answers: submitted ? items.map(item => ({ ...item, status: 'normal', note: '' })) : [] }] });
    }
    if (url.pathname === '/api/pre-shift-checks/submit') {
      writes++;
      if (submitFail) { submitFail = false; return route.fulfill({ status: 503, json: { ok: false, error: '隔离测试：提交暂未确认' } }); }
      assert.deepEqual(req.postDataJSON().answers, [{ id: 'one', status: 'normal', note: '' }, { id: 'two', status: 'abnormal', note: '待跟进测试问题' }]);
      submitted = true; return ok({ completed: true });
    }
    if (url.pathname === '/api/pre-shift-checks/files') { uploads++; return ok({ id: 'test-file' }); }
    if (url.pathname === '/api/scope-overview') return ok({ scopes: { A: {} }, source_snapshot_ready: true });
    if (url.pathname === '/api/health') return route.fulfill({ json: { ok: true, service: 'clipflow_backend', instance_id: 'legacy-check-fixture' } });
    return ok({ items: [], records: [], stats: {}, summary: {} });
  });
  page = await context.newPage(); page.on('pageerror', error => errors.push(error.message));
  await page.goto(origin + '/');
  const card = page.locator('.module-pre_shift');
  await card.locator('.module-check-alert').waitFor();
  assert(await page.locator('.module-card').first().evaluate(el => el.classList.contains('module-pre_shift')), 'authorized checklist is the first home card');
  assert(await card.evaluate(el => el.classList.contains('needs-attention')));
  assert.match(await card.evaluate(el => getComputedStyle(el, '::after').animationName), /^homeCheckPulse/);
  await page.emulateMedia({ reducedMotion: 'reduce' });
  assert.equal(await card.evaluate(el => getComputedStyle(el, '::after').animationName), 'none');
  for (const { width, height } of [{ width: 1440, height: 900 }, { width: 402, height: 874 }, { width: 440, height: 956 }, { width: 390, height: 960 }]) {
    await page.setViewportSize({ width, height });
    assert(await page.locator('.module-card').first().evaluate(el => el.classList.contains('module-pre_shift')), 'checklist order is unchanged across viewports');
    assert(!await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1));
    await page.screenshot({ path: path.join(output, `home-${width}.png`), fullPage: true });
  }
  await card.getByRole('button').click();
  const check = page.getByRole('region', { name: 'A楼班前检查', exact: true });
  await check.getByText('本次待检查', { exact: true }).waitFor();
  assert.match(page.url(), /entry=pre_shift/);
  await check.getByRole('button', { name: '提交检查', exact: true }).click();
  await check.getByText('请逐项选择检查结果，并填写异常项备注。', { exact: true }).waitFor(); assert.equal(writes, 0);
  const rows = check.locator('fieldset');
  await rows.nth(0).locator('.n-radio-button').filter({ hasText: '无异常' }).click();
  await rows.nth(1).locator('.n-radio-button').filter({ hasText: '异常' }).last().click();
  await check.getByRole('button', { name: '提交检查', exact: true }).click(); assert.equal(writes, 0);
  await rows.nth(1).getByRole('textbox').fill('待跟进测试问题');
  await page.waitForFunction(()=>getComputedStyle(document.querySelector('.v2-check-normal.n-radio-button--checked')).backgroundColor==='rgb(21, 128, 61)'&&getComputedStyle(document.querySelector('.v2-check-abnormal.n-radio-button--checked')).backgroundColor==='rgb(180, 35, 24)');
  await page.getByRole('button', { name: '返回', exact: true }).click();
  await page.getByText('检查尚未提交', { exact: true }).waitFor();
  await page.getByRole('button', { name: '继续填写', exact: true }).click();
  assert.match(page.url(), /entry=pre_shift/);
  assert.equal(await rows.nth(0).getByRole('group',{name:'检查附件上传区',exact:true}).count(),0,'empty attachment controls start collapsed');
  await rows.nth(0).getByRole('button',{name:items[0].text+'附件',exact:true}).click();
  await rows.nth(0).getByLabel('上传检查附件', { exact: true }).setInputFiles({ name: '检查附件.txt', mimeType: 'text/plain', buffer: Buffer.from('isolated') });
  await check.getByRole('link', { name: '检查附件.txt', exact: true }).waitFor(); assert.equal(uploads, 1);
  await rows.nth(0).getByRole('button',{name:items[0].text+'附件',exact:true}).click();
  assert.equal(await rows.nth(0).getByRole('button',{name:items[0].text+'附件',exact:true}).innerText(),'1','collapsed attachments keep a visible count');
  assert.equal(await rows.nth(1).getByRole('textbox').inputValue(),'待跟进测试问题','collapsing attachments keeps notes');
  for (const width of [1440, 390, 360]) {
    await page.setViewportSize({ width, height: width < 700 ? 844 : 960 });
    assert(!await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1));
    await page.screenshot({ path: path.join(output, `check-${width}.png`), fullPage: true });
  }
  submitFail = true;
  await check.getByRole('button', { name: '提交检查', exact: true }).click();
  await check.getByText('隔离测试：提交暂未确认', { exact: true }).waitFor();
  assert.equal(await rows.nth(1).getByRole('textbox').inputValue(), '待跟进测试问题');
  await check.getByRole('button', { name: '提交检查', exact: true }).click();
  await check.getByText('本次已完成', { exact: true }).waitFor(); assert.equal(writes, 2);
  await page.getByRole('button', { name: '返回', exact: true }).click();
  await card.getByText('已完成', { exact: true }).waitFor();
  assert(!await card.evaluate(el => el.classList.contains('needs-attention')));
  readFail = true;
  await page.evaluate(() => window.dispatchEvent(new Event('clipflow-business-changed')));
  await card.getByText('状态读取失败，点击查看或稍后重试', { exact: true }).waitFor();
  assert.equal(await card.getByText('已完成', { exact: true }).count(), 0, 'a read error never reports completed');
  readFail = false;
  const advance = { ...task, id: 'advance', due: now + 7200, deadline: now + 18000, submitted: serverTime, actor_name: '值班长二', can_submit: false, answers: items.map(item => ({ ...item, status: 'normal', note: '' })) };
  const upcoming = { ...task, id: 'next', day: '2026-10-18', due: now + 86400, deadline: now + 100000 };
  const overdue = { ...task, id: 'overdue', due: now - 86400, deadline: now - 1 };
  scheduledTasks = [upcoming, advance, { ...overdue, id: 'expired-done', submitted: serverTime }, overdue];
  await page.goto(origin + '/?entry=pre_shift');
  await check.getByText('逾期待补做', { exact: true }).waitFor();
  const choices = check.locator('.v2-check-choice');
  assert.equal(await choices.count(), 3, 'expired completed checks hide, unfinished ones remain');
  assert.match(await choices.nth(0).innerText(), /已逾期，可补做/);
  assert.match(await choices.nth(1).innerText(), /提前完成 · 值班长二/);
  assert.match(await choices.nth(2).innerText(), /未到时间，可提前检查/);
  assert(await check.evaluate(el => el.classList.contains('has-pending-checks')));
  overdue.submitted = serverTime;
  await check.getByRole('button', { name: '刷新班前检查', exact: true }).click();
  await choices.nth(2).waitFor({ state: 'detached' });
  assert(!await check.evaluate(el => el.classList.contains('has-pending-checks')), 'no pulse while all due work is completed');
  serverTime = new Date((advance.deadline + 1) * 1000).toISOString();
  await check.getByRole('button', { name: '刷新班前检查', exact: true }).click();
  await choices.nth(1).waitFor({ state: 'detached' });
  assert.equal(await choices.count(), 1, 'early completion remains until the interval ends');
  serverTime = new Date(upcoming.due * 1000).toISOString();
  await check.getByRole('button', { name: '刷新班前检查', exact: true }).click();
  await check.locator('.v2-check-alert').waitFor();
  assert(await check.evaluate(el => el.classList.contains('has-pending-checks')), 'the next occurrence starts alerting only when due');
  scheduledTasks = null; serverTime = state.server_time;
  mode = 'manager'; submitted = false;
  await page.goto(origin + '/?entry=pre_shift');
  for (const width of [1440,390]) {
    await page.setViewportSize({width,height:width<700?844:960});
    await check.getByRole('button',{name:'提交检查',exact:true}).waitFor();
    assert(await check.getByRole('button',{name:'提交检查',exact:true}).isEnabled(),'legacy managers can submit on PC/mobile');
    assert(await check.locator('.n-radio-input').first().isEnabled(),'legacy manager fields are editable');
  }
  await check.getByRole('button', { name: '管理清单', exact: true }).click();
  const editor = page.locator('.v2-check-editor');
  await editor.getByRole('textbox', { name: '检查项1', exact: true }).waitFor();
  await editor.getByRole('button', { name: '取消', exact: true }).click();
  scheduledTasks=[{...task,items:Array.from({length:25},(_,i)=>({id:'long-'+i,text:'核对本楼设备运行状态、交接记录与现场安全措施，逐项确认检查内容 '+(i+1)}))}];
  await check.getByRole('button',{name:'刷新班前检查',exact:true}).click();await rows.nth(24).waitFor();
  const itemList=check.getByRole('group',{name:'本次检查项目',exact:true});
  for(const viewport of [{width:1440,height:900},{width:402,height:874},{width:440,height:956},{width:360,height:640}]){
    await page.setViewportSize(viewport);
    assert(await itemList.evaluate(el=>el.scrollHeight>el.clientHeight&&el.clientHeight<=481),'legacy long list is height-limited');
    assert(await rows.first().locator('legend').evaluate(el=>parseFloat(getComputedStyle(el).fontSize)>=16),'legacy description font is at least 16px');
    assert(!(await check.locator('.v2-check-choice').first().innerText()).includes('核对本楼设备运行状态'),'shift cards do not duplicate all check descriptions');
    await rows.first().locator('.v2-check-normal').click();await rows.last().locator('.v2-check-abnormal').click();await rows.last().getByRole('textbox').fill('末项异常备注');
    assert(await itemList.evaluate(el=>el.scrollTop>0));assert(await rows.first().getByRole('radio',{name:'无异常',exact:true}).isChecked());
    assert(!await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1));
    await check.getByRole('button',{name:'提交检查',exact:true}).scrollIntoViewIfNeeded();await itemList.evaluate(el=>el.scrollTop=0);
    await page.screenshot({path:path.join(output,'long-check-'+viewport.width+'.png'),fullPage:true});
  }
  for (mode of ['engineer']) {
    const before = checks; await page.goto(origin + '/');
    await page.locator('.module-card').first().waitFor();
    assert.equal(await card.count(), 0); assert.equal(checks, before, 'unauthorized home never reads checklists');
    await page.goto(origin + '/?entry=pre_shift');
    await page.getByText('当前账号没有班前检查权限。', { exact: true }).waitFor(); assert.equal(checks, before);
  }
  mode = 'guest';
  const beforeGuest = checks;
  await page.goto(origin + '/?entry=pre_shift');
  await page.waitForURL('**/cabinet-power');
  assert.equal(await card.count(), 0); assert.equal(checks, beforeGuest, 'guest remains cabinet-only');
  assert.deepEqual(errors, []);
  console.log('PASS: legacy module, live pending/completed/error states, overnight windows, permissions, reduced motion, dirty guard, validation/retry, attachments, PC/mobile. No real writes.');
} catch (error) {
  if (page) { await page.screenshot({ path: path.join(output, 'failure.png'), fullPage: true }); console.error(await page.locator('body').innerText()); }
  throw error;
} finally { await browser.close(); await server.close(); }
