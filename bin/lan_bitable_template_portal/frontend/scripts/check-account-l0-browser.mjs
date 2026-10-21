import assert from 'node:assert/strict';
import {mkdir, writeFile} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {preview} from 'vite';
import {chromium} from 'playwright';
import {layoutApiFixture} from './layout-api-fixture.mjs';

const root = fileURLToPath(new URL('..', import.meta.url));
const output = path.resolve(root, '../../../output/playwright/account-l0');
await mkdir(output, {recursive: true});
const server = await preview({root, logLevel: 'error', preview: {host: '127.0.0.1', port: 0}});
const origin = 'http://127.0.0.1:' + server.httpServer.address().port;
const browser = await chromium.launch({headless: true});
const scopes = ['A', 'B', 'H'].map(value => ({value, label: value + '楼'}));
const modules = [{value: 'accounts', label: '账号与设置', group: '系统管理'}, {value: 'learning', label: '画像学练', group: '协作与资料'}, {value: 'maintenance', label: '维保通告', group: '通告办理'}];
const categories = [{value: 'engineer', label: '工程师'}, {value: 'manager', label: '经理'}];
const results = [];

async function run(version, viewport, actorKind) {
  const fixed = actorKind === 'fixed', level = fixed ? 'L0' : actorKind;
  const elevated = ['L0', 'L1'].includes(level);
  const allowedScopes = elevated ? ['A', 'B', 'H'] : ['H'];
  const user = {open_id: 'actor-' + actorKind, name: '隔离授权人', level, role: elevated ? 'admin' : 'building',
    can_manage_accounts: level !== 'L4', is_fixed_superuser: fixed, home_scope: 'H', scope_rules: allowedScopes,
    modules: level === 'L4' ? ['learning'] : ['accounts', 'learning']};
  const person = (id, extra = {}) => ({id, record_id: id, open_id: 'ou-' + id, name: id, employee_no: id, kind: 'external',
    level: 'L4', role: 'building', own_scopes: ['A'], home_scope: 'A', scopes: ['A'], modules: ['learning'],
    position: '工程师', category: 'engineer', enabled: true, authorized: true, revision: 'revision-1',
    can_set_password: false, can_impersonate: false, can_access_signature: false, can_delete: false,
    can_edit: true, can_edit_access: level !== 'L2', can_grant_l0: true, ...extra});
  const target = person('待授权工程师', level === 'L3' ? {own_scopes: ['H'], home_scope: 'H', scopes: ['H']} : {});
  const locked = person('李世龙', {level: 'L0', role: 'admin', locked: true, can_edit: false, can_edit_access: false, can_grant_l0: false});
  const self = person('本人账号', {open_id: user.open_id, level, is_self: true, can_edit: false, can_edit_access: false, can_grant_l0: false});
  const peer = person('同级L0', {level: 'L0', role: 'admin', can_edit: level === 'L0', can_edit_access: level === 'L0', can_grant_l0: level === 'L0'});
  const items = level === 'L2' || level === 'L3' ? [target] : [target, locked, self, peer];
  const assignable = level === 'L2' ? ['L0'] : level === 'L3' ? ['L0', 'L4'] : level === 'L1' ? ['L0', 'L2', 'L3', 'L4'] : ['L0', 'L1', 'L2', 'L3', 'L4'];
  const context = await browser.newContext({viewport, reducedMotion: 'reduce'});
  const layouts = layoutApiFixture(), writes = [], errors = [], reads = [];
  await context.addInitScript(({version}) => localStorage.setItem('clipflow_ui_version', version === 'v2' ? 'v2' : 'legacy'), {version});
  await context.route('**/*', async route => {
    const req = route.request(), url = new URL(req.url());
    if (url.origin !== origin) return route.abort();
    if (!url.pathname.startsWith('/api/')) return route.continue();
    if (await layouts.handle(route, user.open_id)) return;
    const ok = data => route.fulfill({json: {ok: true, data}});
    if (req.method() === 'GET') reads.push(url.pathname);
    if (url.pathname === '/api/health') return route.fulfill({json: {ok: true, service: 'clipflow_backend', instance_id: 'isolated-l0'}});
    if (url.pathname === '/api/auth/status') return ok({logged_in: true, user, scope_options: scopes});
    if (url.pathname === '/api/auth/accounts') {
      if (level === 'L4') return route.fulfill({status: 403, json: {ok: false, error: '无账号管理权限'}});
      return ok({items, scope_options: scopes, module_options: modules, category_options: categories,
        level_options: assignable.map(value => ({value, label: value + ' 权限'})), policy: {...user}});
    }
    if (url.pathname === '/api/auth/accounts/permissions' && req.method() === 'POST') {
      const body = req.postDataJSON(); writes.push(body);
      const change = body.changes[0]; Object.assign(items.find(item => item.id === change.id), change, {revision: 'revision-2'});
      return ok({changed: 1});
    }
    if (req.method() !== 'GET') {errors.push('Unexpected write: ' + url.pathname); return route.abort();}
    return ok({items: [], users: [], records: [], scope_options: scopes, settings: {}});
  });
  const page = await context.newPage(); page.setDefaultTimeout(10000);
  page.on('pageerror', error => errors.push(error.message));
  const name = `${version}-${actorKind}-${viewport.width}`;
  const entry = version === 'v2' ? '/assets/workspace-v2/index.html?__page=/admin/settings' : '/admin/settings';
  async function open(name) {
    await page.getByRole('button', {name: '管理' + name, exact: true}).click();
    const editor = page.getByRole('dialog', {name, exact: true}); await editor.waitFor(); return editor;
  }
  async function select(editor, label, value) {
    const native = editor.locator(`select[aria-label="${label}"]`);
    if (version === 'legacy') await native.selectOption(value);
    else {
      const text = await native.locator(`option[value="${value}"]`).innerText();
      await editor.getByRole('combobox', {name: label, exact: true}).click();
      await page.locator('.n-base-select-option').filter({hasText: text}).click();
    }
    assert.equal(await native.inputValue(), value);
  }
  async function screenshot(editor, suffix = '') {
    const save = editor.getByRole('button', {name: '保存权限', exact: true});
    await save.scrollIntoViewIfNeeded();
    const box = await editor.boundingBox();
    assert(box.x >= -1 && box.x + box.width <= viewport.width + 1, 'dialog stays inside the viewport');
    assert(!await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1), 'no document horizontal overflow');
    await page.screenshot({path: path.join(output, name + suffix + '.png'), animations: 'disabled'});
  }
  try {
    await page.goto(origin + entry);
    if (level === 'L4') {
      await page.locator('body').getByText(/没有该功能权限|无权|未开放|不可用|权限不足/).first().waitFor();
      assert.equal(await page.getByRole('button', {name: '管理待授权工程师', exact: true}).count(), 0);
      assert.equal(await page.getByRole('button', {name: '保存权限', exact: true}).count(), 0);
      assert.equal(writes.length, 0);
      assert(!reads.includes('/api/auth/accounts'), 'L4 does not fetch the accounts directory');
    } else {
      await page.getByRole('button', {name: '管理待授权工程师', exact: true}).waitFor();
      if (version === 'v2') assert.equal(await page.getByRole('button', {name: '申请修改本人权限', exact: true}).count(), fixed ? 0 : 1);
      if (level === 'L0' || level === 'L1') {
        for (const name of ['李世龙', '本人账号', ...(level === 'L1' ? ['同级L0'] : [])]) {
          const readonly = await open(name);
          assert(await readonly.getByRole('button', {name: '保存启用状态', exact: true}).isDisabled());
          assert.equal(await readonly.locator('.account-access').count(), 0);
          assert.equal(await readonly.getByRole('button', {name: '保存权限', exact: true}).count(), 0);
          assert.equal(await readonly.locator('.account-signature-editor').count(), 0);
          await readonly.getByRole('button', {name: '关闭账号详情'}).click();
        }
      }
      const editor = await open('待授权工程师');
      const save = editor.getByRole('button', {name: '保存权限', exact: true});
      if (level === 'L2') {
        const current = editor.locator('select[aria-label="权限级别"] option[value="L4"]');
        assert.equal(await current.evaluate(option => option.disabled), true, 'current non-assignable level remains visible but cannot be selected');
        assert(await save.isDisabled());
        assert(await editor.locator('select[aria-label="岗位分类"]').isDisabled());
        assert(await editor.locator('.module-options input').first().isDisabled());
        await editor.getByRole('checkbox', {name: '启用登录', exact: true}).uncheck();
        await editor.getByRole('button', {name: '保存启用状态', exact: true}).click();
        await editor.waitFor({state: 'hidden'});
        assert.equal(writes.length, 1); assert.equal(writes[0].changes[0].enabled, false);
        assert(!('level' in writes[0].changes[0]), 'state-only editing does not promote');
        await open('待授权工程师');
        await editor.getByRole('checkbox', {name: '启用登录', exact: true}).check();
      }
      await select(editor, '权限级别', 'L0');
      assert(!await save.isDisabled());
      assert(!await editor.locator('select[aria-label="岗位分类"]').isDisabled());
      assert.equal(await editor.locator('.module-options input[value="maintenance"]').count(), 0, 'unowned modules never offered');
      assert.deepEqual(await editor.locator('.check-group input:checked').evaluateAll(nodes => nodes.map(node => node.value).sort()), [...allowedScopes].sort());
      assert(await editor.locator('.check-group input').first().isDisabled(), 'L0 scopes use the same automatic selection as L1');
      await screenshot(editor);
      const before = writes.length;
      await save.click(); await editor.waitFor({state: 'hidden'});
      assert.equal(writes.length, before + 1, 'single save produces a single mocked write');
      const change = writes.at(-1).changes[0];
      assert.equal(change.level, 'L0'); assert.equal(change.role, 'admin');
      assert.deepEqual([...change.scopes].sort(), [...allowedScopes].sort());
      assert.deepEqual([...change.modules].sort(), [...user.modules].sort());
      if (level === 'L0') {
        const peerEditor = await open('同级L0');
        await select(peerEditor, '岗位分类', 'manager'); await screenshot(peerEditor, '-peer');
        await peerEditor.getByRole('button', {name: '保存权限', exact: true}).click(); await peerEditor.waitFor({state: 'hidden'});
        assert.equal(writes.at(-1).changes[0].id, peer.id);
      }
    }
    assert.deepEqual(errors, []);
    assert(!reads.some(url => url.startsWith('/api/signatures/')), 'no protected signature requests');
    results.push({name, status: 'PASS', writes: writes.length}); console.log('PASS ' + name);
  } catch (error) {
    results.push({name, status: 'FAIL', error: error.stack, errors});
    await page.screenshot({path: path.join(output, name + '-failure.png'), animations: 'disabled'});
    throw error;
  } finally { await context.close(); }
}
try {
  for (const version of ['legacy', 'v2']) {
    for (const viewport of [{width: 1440, height: 900}, {width: 402, height: 874}, {width: 440, height: 956}]) {
      for (const level of ['L2', 'L0']) await run(version, viewport, level);
    }
    for (const level of ['L1', 'L3', 'fixed', 'L4']) await run(version, {width: 1440, height: 900}, level);
  }
  console.log('PASS: L0 grants, actor scope/module limits, peer edit, protected/self accounts, L4 denial; all writes mocked.');
} finally {
  await writeFile(path.join(output, 'results.json'), JSON.stringify(results, null, 2));
  await browser.close(); await new Promise(resolve => server.httpServer.close(resolve));
}
