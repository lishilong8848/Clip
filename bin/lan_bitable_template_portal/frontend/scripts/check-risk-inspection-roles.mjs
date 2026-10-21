import assert from 'node:assert/strict';
import {mkdir, writeFile} from 'node:fs/promises';
import {spawn} from 'node:child_process';
import {once} from 'node:events';
import {createServer} from 'node:net';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {chromium} from 'playwright';

const root = fileURLToPath(new URL('..', import.meta.url));
const project = path.resolve(root, '../../..');
const output = path.join(project, 'output/playwright/risk-roles');
await mkdir(output, {recursive: true});
const reservation = createServer();
await new Promise(resolve => reservation.listen(0, '127.0.0.1', resolve));
const port = reservation.address().port;
await new Promise(resolve => reservation.close(resolve));
const base = 'http://127.0.0.1:' + port;
const server = spawn(path.join(project, 'bin/.venv/Scripts/python.exe'), ['-B', 'bin/tools/lighthouse_stream_preview.py'], {
  cwd: project, windowsHide: true, env: {...process.env, LIGHTHOUSE_NATIVE_PREVIEW: '0', LIGHTHOUSE_PREVIEW_PORT: String(port)},
  stdio: ['ignore', 'pipe', 'pipe'],
});
let log = '';
server.stdout.on('data', value => log += value); server.stderr.on('data', value => log += value);
const reports = [], browser = await chromium.launch({headless: true});
const fields = [
  {key: 'result', field_id: 'result', field_name: '排查结果', type: 1, role: 'input', required: true},
  {key: 'photo', field_id: 'photo', field_name: '现场照片', type: 17, role: 'input'},
];
const names = {alice: '双身份甲', bob: '排查人员乙', reviewer: '专职审核人', owner: '发布管理员'};
function state() {
  const item = (id, scope, status) => ({id, scope, title: scope + '楼设备检查', status, values: status === 'submitted' ? {result: '已核对'} : {}, files: [], system_values: {}});
  return {actor: 'alice', reviewers: ['alice', 'reviewer'], writes: [], calls: [],
    people: [{id: 'alice', name: names.alice, scope: 'A', scopes: ['A', 'B'], version: 1, revoked: false,
      items: [item('a-A', 'A', 'pending'), item('a-B', 'B', 'submitted')]},
    {id: 'bob', name: names.bob, scope: 'B', scopes: ['B'], version: 1, revoked: false, items: [item('b-B', 'B', 'submitted')]}]};
}
function detail(s) {
  return {id: 'release', name: '角色分离隔离验收', owner: names.owner, status: 'active', deadline: Date.now() + 86400000,
    fields, self_id: s.actor, is_manager: s.actor === 'owner', is_reviewer: s.reviewers.includes(s.actor),
    reviewer: names[s.reviewers[0]], reviewer_id: s.reviewers[0], reviewer_ids: s.reviewers,
    reviewers: s.reviewers.map(id => ({id, name: names[id]})), reviewer_version: 1,
    people: structuredClone(s.people.filter(p => p.id === s.actor || s.reviewers.includes(s.actor) || s.actor === 'owner')),
    can_manage_release: s.actor === 'owner', can_delete_release: s.actor === 'owner', can_claim: s.actor === 'owner',
    claim_scopes: ['A', 'B'], jobs: [], audit: [], final_managers: []};
}
async function mock(context, s) {
  await context.route('**/*', async route => {
    const req = route.request(), url = new URL(req.url());
    if (url.origin !== base) { s.calls.push('BLOCKED ' + url.origin); return route.abort(); }
    if (!url.pathname.startsWith('/api/')) return route.continue();
    if (url.pathname === '/api/health') return route.continue();
    const ok = data => route.fulfill({json: {ok: true, data}}), fail = (status, error) => route.fulfill({status, json: {ok: false, error}});
    s.calls.push(req.method() + ' ' + url.pathname + url.search);
    if (url.pathname === '/api/auth/status') return ok({logged_in: true, user: {open_id: 'offline-' + s.actor, name: names[s.actor],
      role: s.actor === 'owner' ? 'admin' : 'building', level: s.actor === 'owner' ? 'L0' : 'L4', modules: ['risk_inspection'],
      scope_rules: ['A', 'B'], home_scope: 'A', risk_inspection_assigned: true},
      scope_options: [{value: 'A', label: 'A楼'}, {value: 'B', label: 'B楼'}], default_scope: 'A'});
    if (url.pathname.startsWith('/api/workspace-layouts/')) {
      assert.equal(req.method(), 'GET');
      return ok(url.pathname.endsWith('/workspace') ? {revision: 1, value: {version: 1, widgets: [
        {id: 'risk-fixture', kind: 'module', title: '风险排查', module: 'risk_inspection', scope: 'current',
          display: 'bar', period: 'week', metric: 'ongoing', presentation: 'default', links: [], x: 0, y: 0, w: 12, h: 18},
      ]}} : {revision: 0, value: null});
    }
    if (url.pathname === '/api/risk-inspections') {
      assert.equal(req.method(), 'GET');
      let view = url.searchParams.get('view') || 'all';
      if (view === 'published' && s.actor !== 'owner') view = 'mine';
      const people = view === 'mine' ? s.people.filter(p => p.id === s.actor) : view === 'review' ? s.reviewers.includes(s.actor) ? s.people : [] : s.actor === 'owner' ? s.people : [];
      const items = people.flatMap(p => p.items), canClaim = s.actor === 'owner' && view === 'mine';
      const row = {...detail(s), people_count: people.length, total: items.length, submitted: items.filter(i => ['submitted', 'approved'].includes(i.status)).length, approved: items.filter(i => i.status === 'approved').length, can_claim: canClaim};
      return ok({view, items: items.length || canClaim ? [row] : [], total: items.length || canClaim ? 1 : 0, page: 1,
        is_manager: s.actor === 'owner', can_edit_template: s.actor === 'owner'});
    }
    if (url.pathname === '/api/risk-inspections/release') return ok(detail(s));
    if (url.pathname.endsWith('/actions')) {
      assert.equal(req.method(), 'POST');
      const body = req.postDataJSON(), person = s.people.find(p => p.id === url.pathname.split('/')[5]);
      assert(person, 'Only fixture people can be modified');
      const item = person.items.find(i => i.id === body.item_id);
      assert(item); assert.equal(body.version, person.version);
      if (['save', 'submit'].includes(body.action)) {
        if (person.id !== s.actor) return fail(403, '禁止代填');
        assert(['pending', 'returned'].includes(item.status)); item.values = body.values;
        if (body.action === 'submit') item.status = 'submitted';
      } else {
        if (!s.reviewers.includes(s.actor)) return fail(403, '审核权限已撤销');
        assert.equal(body.action, 'approve'); assert.equal(item.status, 'submitted'); item.status = 'approved';
      }
      person.version++; s.writes.push({person: person.id, ...body}); return ok(detail(s));
    }
    assert.equal(req.method(), 'GET', 'No unrelated writes are permitted: ' + url.pathname);
    return ok({items: [], records: [], total: 0, scopes: {}, links: {}, summary: {}});
  });
}
function url(version, view, id) {
  const target = new URL(version === 'v2' ? '/assets/workspace-v2/index.html' : '/risk-inspections', base);
  if (version === 'v2') target.searchParams.set('__page', '/risk-inspections');
  target.searchParams.set('scope', 'A'); if (view) target.searchParams.set('view', view); if (id) target.searchParams.set('id', id);
  return target.href;
}
async function capture(page, tag) {
  await page.screenshot({path: path.join(output, tag + '.png'), fullPage: true, animations: 'disabled'});
  assert(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1), tag + ': page must not overflow');
}
const drawer = page => page.locator('.risk-feedback-drawer:visible');
const detailReads = s => s.calls.filter(call => call === 'GET /api/risk-inspections/release').length;
async function closeDrawer(page) { await page.keyboard.press('Escape'); await drawer(page).waitFor({state: 'hidden'}); }
async function openItem(page, person, title) { await page.getByRole('button', {name: '处理 ' + person + ' · ' + title, exact: true}).click(); await drawer(page).waitFor(); }
async function confirm(page) { await page.locator('.risk-confirm').getByRole('button', {name: '确认', exact: true}).click(); await page.locator('.risk-confirm').waitFor({state: 'hidden'}); }
async function readOnlyReview(page) {
  assert.equal(await drawer(page).locator('input[type="file"],textarea,input:not([type="hidden"])').count(), 0);
  for (const name of ['保存草稿', '提交审核', '领取本人任务']) assert.equal(await page.getByRole('button', {name, exact: true}).count(), 0);
}
try {
  for (let i = 0; i < 100; i++) {
    try { if ((await fetch(base + '/api/health')).ok) break; } catch {}
    if (server.exitCode !== null || i === 99) throw new Error(log || 'isolated preview did not start');
    await new Promise(resolve => setTimeout(resolve, 200));
  }
  assert.equal((await (await fetch(base + '/api/health')).json()).instance_id, 'isolated-lighthouse-stream');
  for (const version of ['legacy', 'v2']) for (const viewport of [{width: 1440, height: 900}, {width: 402, height: 874}, {width: 440, height: 956}]) {
    const s = state(), context = await browser.newContext({viewport, reducedMotion: 'reduce', serviceWorkers: 'block'});
    await mock(context, s);
    const page = await context.newPage(), errors = [], scripts = new Set(), tag = version + '-' + viewport.width;
    page.setDefaultTimeout(15000); page.on('pageerror', error => errors.push(error.message));
    page.on('request', req => { if (req.resourceType() === 'script') scripts.add(new URL(req.url()).pathname); });
    try {
      await page.goto(url(version, 'mine')); await page.locator('.risk-task-name:visible').waitFor();
      let reads = detailReads(s); await page.locator('.risk-task-name:visible').click();
      await page.locator('.risk-feedback-table').waitFor(); assert.equal(new URL(page.url()).searchParams.get('id'), 'release');
      await page.waitForLoadState('networkidle'); assert.equal(detailReads(s) - reads, 1, 'Opening a task reads its detail once without remounting');
      assert.equal(await page.locator('.risk-feedback-table').getByRole('button', {name: '查看 ' + names.bob + '的反馈 · B楼设备检查', exact: true}).count(), 0, 'Mine excludes other people');
      await openItem(page, names.alice, 'A楼设备检查');
      await drawer(page).getByRole('textbox', {name: '排查结果', exact: true}).fill('不可丢失的本楼反馈');
      assert.equal(await drawer(page).getByRole('button', {name: '审核通过', exact: true}).count(), 0);
      assert.equal(await drawer(page).locator('input[type="file"]').count(), 1);
      await page.goBack(); await page.getByRole('button', {name: '继续填写', exact: true}).click();
      assert.equal(await drawer(page).getByRole('textbox', {name: '排查结果', exact: true}).inputValue(), '不可丢失的本楼反馈');
      assert.equal(new URL(page.url()).searchParams.get('id'), 'release', 'Browser back is guarded');
      await drawer(page).getByRole('button', {name: '保存草稿', exact: true}).click(); await drawer(page).getByText('草稿已保存。', {exact: true}).waitFor();
      await capture(page, tag + '-mine');
      await drawer(page).getByRole('button', {name: '提交审核', exact: true}).click(); await confirm(page);
      assert.equal(await drawer(page).getByRole('button', {name: '审核通过', exact: true}).count(), 0, 'Submission does not turn the filling page into review');
      await closeDrawer(page); reads = detailReads(s); await page.getByRole('button', {name: '进入审核任务', exact: true}).click();
      await page.locator('.risk-feedback-table').getByRole('button', {name: '查看 ' + names.bob + '的反馈 · B楼设备检查', exact: true}).waitFor();
      await page.waitForLoadState('networkidle'); assert.equal(detailReads(s) - reads, 1, 'Role switching reads the current detail only once');
      assert.equal(new URL(page.url()).searchParams.get('view'), 'review');
      await openItem(page, names.alice, 'A楼设备检查'); await readOnlyReview(page); await capture(page, tag + '-review');
      await drawer(page).getByRole('button', {name: '审核通过', exact: true}).click(); await confirm(page);
      assert.equal(s.people[0].items[0].status, 'approved', 'Existing self-review remains supported');
      await closeDrawer(page); await page.getByRole('button', {name: '我的审核任务', exact: true}).click();
      await page.locator('.risk-task-name:visible').waitFor(); assert.equal(new URL(page.url()).searchParams.get('id'), null);
      await page.reload(); await page.getByRole('tab', {name: '我的审核任务', exact: true}).waitFor();
      assert.equal(await page.getByRole('tab', {name: '我的审核任务', exact: true}).getAttribute('aria-selected'), 'true');
      reads = detailReads(s); await page.goBack(); await page.locator('.risk-feedback-table').waitFor();
      await page.waitForLoadState('networkidle'); assert.equal(detailReads(s) - reads, 1, 'Browser return reads detail once');
      assert.equal(new URL(page.url()).searchParams.get('view'), 'review');
      assert.deepEqual(s.writes.map(write => write.action), ['save', 'submit', 'approve']);

      s.actor = 'reviewer'; await page.goto(url(version, null, 'release')); await page.locator('.risk-role-view').getByText('我的审核任务', {exact: true}).waitFor();
      await openItem(page, names.bob, 'B楼设备检查'); await readOnlyReview(page); await closeDrawer(page);
      await page.goto(url(version, 'mine')); await page.locator('.risk-list-footer').getByText('共 0 个排查任务', {exact: true}).waitFor();
      s.actor = 'bob'; await page.goto(url(version, null, 'release')); await page.locator('.risk-role-view').getByText('我的排查任务', {exact: true}).waitFor();
      assert.equal(await page.locator('.risk-feedback-table').getByRole('button', {name: '查看 ' + names.alice + '的反馈 · A楼设备检查', exact: true}).count(), 0);
      await page.goto(url(version, 'review')); await page.locator('.risk-list-footer').getByText('共 0 个排查任务', {exact: true}).waitFor();
      await page.goto(url(version, 'review', 'release')); await page.getByText('你不是本次排查的当前审核人', {exact: true}).waitFor();
      assert.equal(await page.locator('.risk-feedback-table').count(), 0, 'Forging the review view grants nothing');
      s.actor = 'alice'; s.reviewers = ['reviewer'];
      await page.goto(url(version, 'review', 'release')); await page.getByText('你不是本次排查的当前审核人', {exact: true}).waitFor();
      await page.goto(url(version, 'mine', 'release')); await page.locator('.risk-feedback-table').waitFor();
      assert.equal(await page.getByRole('button', {name: '进入审核任务', exact: true}).count(), 0, 'Revoking review does not revoke the own assignment');
      s.actor = 'owner'; await page.goto(url(version, 'published')); await page.getByRole('button', {name: '发布排查', exact: true}).waitFor();
      await page.locator('.risk-task-name:visible').click(); await page.locator('.risk-feedback-table').waitFor();
      assert.equal(await page.getByRole('button', {name: '领取本人任务', exact: true}).count(), 0);
      await page.getByRole('button', {name: '切换到我的排查', exact: true}).click(); await page.getByRole('button', {name: '领取本人任务', exact: true}).waitFor();
      assert.equal(new URL(page.url()).searchParams.get('view'), 'mine');
      if (version === 'v2') {
        s.actor = 'alice'; s.reviewers = ['alice', 'reviewer']; s.people = state().people;
        const home = new URL('/assets/workspace-v2/index.html?__page=%2F&view=home&scope=A', base).href;
        await page.goto(home);
        const widget = page.locator('[data-module-widget="risk_inspection"]');
        await widget.locator('.risk-task-name:visible').waitFor(); reads = detailReads(s);
        await widget.locator('.risk-task-name:visible').click(); await widget.locator('.risk-feedback-table').waitFor();
        await page.waitForLoadState('networkidle'); assert.equal(detailReads(s) - reads, 1, 'Embedded open reads detail only once');
        assert.equal(page.url(), home, 'Embedded navigation must not replace the dashboard URL');
        await widget.getByRole('button', {name: '进入审核任务', exact: true}).click();
        await widget.locator('.risk-role-view').getByText('我的审核任务', {exact: true}).waitFor();
        await openItem(page, names.alice, 'A楼设备检查'); await readOnlyReview(page);
        assert.equal(await drawer(page).getByRole('button', {name: '审核通过', exact: true}).count(), 0, 'Unsubmitted work cannot be reviewed');
        await closeDrawer(page);
        reads = detailReads(s); await widget.locator('.v2-module-toolbar').getByRole('button', {name: '返回', exact: true}).click();
        await widget.locator('.risk-role-view').getByText('我的排查任务', {exact: true}).waitFor();
        await page.waitForLoadState('networkidle'); assert.equal(detailReads(s) - reads, 1, 'Embedded return reads detail once');
        await openItem(page, names.alice, 'A楼设备检查');
        await drawer(page).getByRole('textbox', {name: '排查结果', exact: true}).fill('嵌入页未保存内容');
        // The drawer covers the widget toolbar; invoke its real handler to verify its leave guard.
        await widget.locator('.v2-module-toolbar').getByRole('button', {name: '返回', exact: true}).evaluate(button => button.click());
        await page.getByRole('button', {name: '继续填写', exact: true}).click();
        assert.equal(await drawer(page).getByRole('textbox', {name: '排查结果', exact: true}).inputValue(), '嵌入页未保存内容');
        await page.keyboard.press('Escape'); await page.getByRole('button', {name: '放弃修改', exact: true}).click(); await drawer(page).waitFor({state: 'hidden'});
        await widget.locator('.v2-module-toolbar').getByRole('button', {name: '返回', exact: true}).click();
        await widget.locator('.risk-task-name:visible').waitFor();
        await page.waitForLoadState('networkidle'); assert.equal(page.url(), home); await capture(page, tag + '-embedded');
      }
      assert(scripts.size > 0 && [...scripts].every(src => src.startsWith('/assets/')), 'Must validate real dist, never Vite source');
      assert.deepEqual(errors, []); assert(!s.calls.some(call => call.startsWith('BLOCKED')));
      reports.push({tag, tested: 'real dist: dual roles, self-review, old links, browser back, unsaved guard, revoked review, separate lists, owner claim; V2 embedded routes and return', writes: s.writes, scripts: [...scripts]});
      console.log('PASS ' + tag);
    } catch (error) {
      await page.screenshot({path: path.join(output, tag + '-failure.png'), fullPage: true});
      await writeFile(path.join(output, tag + '-failure.json'), JSON.stringify({url: page.url(), text: await page.locator('body').innerText(), errors, calls: s.calls}, null, 2)); throw error;
    } finally { await context.close(); }
  }
} finally {
  await browser.close(); if (server.exitCode === null) { server.kill(); await once(server, 'exit'); }
  await writeFile(path.join(output, 'report.json'), JSON.stringify(reports, null, 2));
}
console.log('PASS risk role separation against both production builds. No production data or Feishu requests. Evidence: ' + output);
