import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';
import { createServer } from 'vite';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const output = path.resolve(root, '../../../output/playwright/admin-accounts');
await mkdir(output, { recursive: true });
// Use the on-demand dev server (not the prebuilt dist) so this isolated script always
// verifies the current AdminAccounts source against the same origin that the app uses.
const server = await createServer({ root, logLevel: 'error', server: { host: '127.0.0.1', port: 0 }, preview: {} });
await server.listen();
const origin = `http://127.0.0.1:${server.config.server.port}`;
const browser = await chromium.launch({ headless: true });
const scopeOptions = ['110', ...'ABCDEH'].map(value => ({ value, label: value === '110' ? '110站' : value + '楼' }));
const categories = [{ value: 'shift_leader', label: '值班长' }, { value: 'operator', label: '值班员' },
  { value: 'engineer', label: '工程师' }, { value: 'supervisor', label: '主管' },
  { value: 'manager', label: '经理' }, { value: 'duty', label: '固定值班账号' }, { value: 'other', label: '其他/待分类' }];

// admin exercises the full AdminAccounts flow (read-only role/scope, enabled toggle, password,
// audit, pagination) plus the workspace "other settings" navigation; denied has no account rights.
const actors = {
  admin: { open_id: 'ou_admin', name: '隔离管理员', role: 'admin', can_manage_accounts: true, home_scope: 'H' },
  denied: { open_id: 'ou_denied', name: '隔离普通账号', role: 'building', can_manage_accounts: false, home_scope: 'H' },
};
let actor = actors.admin;
let policyLevel = 'L0';
let failSave = false;
let passwordPosts = 0, permissionPosts = 0, legacyReads = 0;
const savePosts = [];
const adminReads = [];
const apiCalls = [];
let auditFixture = [];
const errors = [];

function baseItems() {
  // 26 rows: even indices are engineers, odd indices are shift leaders. items[2] is re-tagged
  // duty, leaving exactly 12 engineers so the category filter can assert an exact page count.
  const items = Array.from({ length: 26 }, (_, i) => ({
    id: 'recTest' + i, record_id: 'recTest' + i, open_id: 'ou_test' + i,
    name: '测试人员' + i, employee_no: 'TEST-' + i, kind: 'external', own_scopes: ['H'], scopes: ['H'], role: 'building',
    position: i % 2 ? '值班长' : '暖通工程师', category: i % 2 ? 'shift_leader' : 'engineer',
    enabled: true, authorized: true, locked: false, blocked_reason: '', can_set_password: true, password_set: i % 2 === 0,
    revision: 'v' + i, level: i % 3 === 0 ? 'L3' : 'L4', can_edit: true, can_delete: true,
    last_login: 1700000000 + i, account_nature: '外部人员',
  }));
  items[1] = { ...items[1], name: '内部测试', kind: 'vnet', can_set_password: false, can_edit: true, can_delete: false };
  items[2] = { ...items[2], name: 'E楼值班', kind: 'duty', category: 'duty', position: '', can_set_password: false, can_delete: false };
  items[3] = { ...items[3], name: '固定管理员', kind: 'vnet', role: 'admin', locked: true, can_set_password: false, can_edit: false, can_delete: false, level: 'L1' };
  items[4] = { ...items[4], name: '身份待核对', blocked_reason: '多人使用同一OpenID', can_set_password: false };
  return items;
}

function isApiRequest(url) {
  try { return new URL(url).pathname.startsWith('/api/'); } catch { return false; }
}

async function newPage() {
  const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
  page.setDefaultTimeout(30000);
  page.on('pageerror', error => errors.push(error.message));
  await page.route(url => isApiRequest(url), async route => {
    const req = route.request();
    const url = new URL(req.url());
    const method = req.method();
    apiCalls.push({ method, path: url.pathname });
    if (url.pathname.startsWith('/api/admin/')) adminReads.push(url.pathname);
    const ok = data => route.fulfill({ json: { ok: true, data } });
    if (url.pathname === '/api/auth/status') return ok({
      logged_in: true,
      user: { open_id: actor.open_id, name: actor.name, role: actor.role, can_manage_accounts: actor.can_manage_accounts, login_method: 'feishu' },
      scope_options: scopeOptions,
    });
    if (url.pathname === '/api/health') return route.fulfill({ json: { ok: true, service: 'clipflow_backend', instance_id: 'isolated-admin-accounts' } });
    if (url.pathname === '/api/auth/accounts' && method === 'GET') {
      return ok({
        items: baseItems(),
        category_options: categories,
        scope_options: scopeOptions,
        policy: { level: policyLevel, home_scope: actor.home_scope, can_manage_accounts: actor.can_manage_accounts, can_view_accounts: true },
        loaded_at: Date.now() / 1000,
      });
    }
    if (url.pathname === '/api/auth/accounts/permissions') {
      permissionPosts++;
      const body = req.postDataJSON();
      for (const change of body.changes) {
        // Semantic backend shape: only enabled/id/revision/role/scopes are ever sent.
        assert.deepEqual(Object.keys(change).sort(), ['enabled', 'id', 'revision', 'role', 'scopes']);
        // The unified drawer can only change the enabled flag: scope arrays must stay untouched.
        const item = baseItems().find(x => x.id === change.id);
        assert.deepEqual(change.scopes, item ? item.scopes : change.scopes);
      }
      if (failSave) return route.fulfill({ status: 409, json: { ok: false, error: '该账号权限已变化，请刷新后重新确认。' } });
      savePosts.push(body);
      return ok({ changed: body.changes.length });
    }
    if (url.pathname.endsWith('/password') && method === 'POST') {
      passwordPosts++;
      assert.deepEqual(req.postDataJSON(), { new_password: 'FixtureNew123!', confirm_password: 'FixtureNew123!' });
      return ok({ password_changed: true });
    }
    if (url.pathname === '/api/auth/accounts/audit') return ok({ items: auditFixture });
    if (url.pathname === '/api/auth/permissions') return ok({ users: baseItems().filter(x => x.authorized), scope_options: scopeOptions });
    if (url.pathname === '/api/auth/permission-requests/admin') return ok({ items: [] });
    if (url.pathname === '/api/handover-links') return ok({ links: { A: 'https://example.example/handover-a', H: '' }, password: '' });
    if (url.pathname === '/api/assistant/conversation') return ok({ enabled: false, turns: [] });
    if (url.pathname === '/api/auth/permission-directory') legacyReads++;
    return ok({ items: [] });
  });
  return page;
}

const privilegedPrefixes = [
  '/api/backend/stats',
  '/api/backend/perf',
  '/api/backend/queues',
  '/api/backend/consistency',
  '/api/backend/notice-diagnostic',
  '/api/backend/preflight',
  '/api/jobs/',
  '/api/admin/mop-settings',
  '/api/backend/mock-pressure',
  '/api/backend/operation-audits',
];

function assertNoPrivilegedCalls(startIndex) {
  const fired = apiCalls.slice(startIndex).filter(call => privilegedPrefixes.some(prefix => call.path.startsWith(prefix)));
  assert.deepEqual(fired, [], `no privileged/global admin requests may fire for ${policyLevel}`);
}

try {
  // ---------- Admin entry + full AdminAccounts flow ----------
  let start = apiCalls.length;
  const page = await newPage();
  await page.goto(origin);
  await page.getByRole('button', { name: '打开账号与权限设置', exact: true }).click();
  const accounts = page.locator('.accounts');
  await accounts.locator('tbody').waitFor();
  assert.equal(new URL(page.url()).pathname, '/admin/settings');
  assert.equal(await page.locator('.admin-shell').evaluate(node => getComputedStyle(node).position), 'static');
  assert.equal(await page.getByRole('dialog').count(), 0, 'settings must be a normal page, no stray dialogs');
  assert.equal(await page.getByRole('button', { name: '其他设置', exact: true }).getAttribute('aria-expanded'), 'false');
  assert.ok(!adminReads.some(value => value.startsWith('/api/admin/')), 'opening accounts must not fetch diagnostics');

  // Category filtering + pagination: 12 engineers, then 20 + 6 on the full list.
  const categoryNav = accounts.getByRole('navigation', { name: '岗位分类筛选' });
  const categoryNames = await categoryNav.getByRole('button').allTextContents();
  await categoryNav.getByRole('button', { name: /^工程师/ }).click();
  assert.equal(await accounts.locator('tbody tr').count(), 12, '工程师 category shows exactly 12 rows');
  assert.equal(await categoryNav.getByRole('button').count(), categoryNames.length, 'category navigation stays stable');
  await categoryNav.getByRole('button', { name: /^全部/ }).click();
  assert.equal(await accounts.locator('tbody tr').count(), 20, 'full list page 1 shows 20 rows');
  await page.screenshot({ path: path.join(output, 'accounts-p1.png'), fullPage: true });
  await accounts.getByRole('button', { name: '下一页', exact: true }).click();
  assert.equal(await accounts.locator('tbody tr').count(), 6, 'pagination page 2 shows remaining 6 rows');
  await accounts.getByRole('searchbox', { name: '搜索账号' }).fill('TEST-0');
  assert.equal(await accounts.locator('tbody tr').count(), 1, 'search narrows to a single row');
  await accounts.getByRole('searchbox', { name: '搜索账号' }).fill('');
  await categoryNav.getByRole('button', { name: /^全部/ }).click();
  assert.equal(legacyReads, 0, 'new page must not load the separate legacy permission directory');

  // ---------- Unified drawer: read-only role/scope + enabled toggle failure/retry ----------
  await accounts.getByRole('searchbox', { name: '搜索账号' }).fill('TEST-0');
  await accounts.getByRole('button', { name: '管理测试人员0', exact: true }).click();
  let dialog = page.getByRole('dialog');
  await dialog.getByRole('checkbox', { name: '启用登录', exact: true }).waitFor();
  assert.equal(await dialog.getByRole('combobox').count(), 0, 'no role select in unified permissions drawer');
  assert.equal(await dialog.getByRole('checkbox', { name: /楼/ }).count(), 0, 'no scope checkboxes in unified permissions drawer');
  assert.equal(await dialog.getByRole('checkbox', { name: '启用登录', exact: true }).isEnabled(), true);
  assert.equal(await dialog.getByRole('button', { name: '保存启用状态', exact: true }).isEnabled(), true);

  const beforeSave = savePosts.length;
  await dialog.getByRole('checkbox', { name: '启用登录', exact: true }).uncheck();
  failSave = true;
  await dialog.getByRole('button', { name: '保存启用状态', exact: true }).click();
  await dialog.getByRole('alert').waitFor();
  assert.equal(await dialog.getByRole('checkbox', { name: '启用登录', exact: true }).isChecked(), false, 'failed save keeps the local enabled change');
  assert.equal(savePosts.length, beforeSave, 'failed save must not persist a success payload');
  failSave = false;
  await dialog.getByRole('button', { name: '保存启用状态', exact: true }).click();
  await dialog.waitFor({ state: 'hidden' });
  assert.equal(savePosts.length, beforeSave + 1, 'retry saves exactly once');
  assert.equal(savePosts[savePosts.length - 1].changes.length, 1);
  assert.deepEqual(Object.keys(savePosts[savePosts.length - 1].changes[0]).sort(), ['enabled', 'id', 'revision', 'role', 'scopes']);
  assert.equal(savePosts[savePosts.length - 1].changes[0].enabled, false);
  assert.deepEqual(savePosts[savePosts.length - 1].changes[0].scopes, ['H'], 'scope arrays unchanged on enable-only save');
  await page.screenshot({ path: path.join(output, 'account-save.png'), fullPage: true });

  // ---------- Password mismatch + clear after save / no storage leakage ----------
  await accounts.getByRole('searchbox', { name: '搜索账号' }).fill('TEST-0');
  await accounts.getByRole('button', { name: '管理测试人员0', exact: true }).click();
  dialog = page.getByRole('dialog');
  await dialog.getByLabel('新密码', { exact: true }).fill('FixtureNew123!');
  await dialog.getByLabel('再次输入新密码', { exact: true }).fill('Different123');
  await dialog.getByRole('button', { name: '确认修改密码' }).click();
  await dialog.getByText('两次输入的密码不一致', { exact: true }).waitFor();
  assert.equal(passwordPosts, 0, 'mismatched passwords must not hit the backend');
  await dialog.getByLabel('再次输入新密码', { exact: true }).fill('FixtureNew123!');
  await dialog.getByRole('button', { name: '确认修改密码' }).click();
  await page.waitForFunction(() => Array.from(document.querySelectorAll('.success')).some(node => node.textContent.includes('需重新登录')));
  assert.equal(passwordPosts, 1);
  assert.equal(await dialog.getByLabel('新密码', { exact: true }).inputValue(), '', 'password input cleared after successful save');
  const storage = await page.evaluate(() => JSON.stringify([Object.entries(localStorage), Object.entries(sessionStorage)]));
  assert.ok(!storage.includes('FixtureNew123!'), 'password must never leak into storage');
  await page.screenshot({ path: path.join(output, 'account-password.png'), fullPage: true });
  await dialog.getByRole('button', { name: '关闭账号详情' }).click();

  // ---------- VNET account: no password form, guidance text ----------
  await accounts.getByRole('searchbox', { name: '搜索账号' }).fill('内部测试');
  await accounts.getByRole('button', { name: '管理内部测试' }).click();
  await page.getByText('飞书账号密码请在飞书中修改。', { exact: true }).waitFor();
  assert.equal(await page.locator('#admin-account-new-password').count(), 0);
  await page.getByRole('button', { name: '关闭账号详情' }).click();

  // ---------- Fixed root (locked admin) is read-only ----------
  await accounts.getByRole('searchbox', { name: '搜索账号' }).fill('固定管理员');
  await accounts.getByRole('button', { name: '管理固定管理员', exact: true }).click();
  dialog = page.getByRole('dialog');
  assert.equal(await dialog.getByRole('button', { name: '保存启用状态', exact: true }).isDisabled(), true, 'fixed root cannot be edited');
  assert.equal(await dialog.getByRole('combobox').count(), 0, 'fixed root drawer has no role select');
  assert.equal(await dialog.getByRole('checkbox', { name: /楼/ }).count(), 0, 'fixed root drawer has no scope checkboxes');
  await dialog.getByText(/当前级别只能查看该账号/).waitFor();
  await page.keyboard.press('Escape');

  // ---------- Audit tab ----------
  start = apiCalls.length;
  auditFixture = [
    { audit_id: 'aud-pwd', action: 'password', status: 'success', updated_at: Date.now() / 1000, actor_name: '隔离管理员', metadata: { name: '测试人员1' } },
    { audit_id: 'aud-del', action: 'delete', status: 'success', updated_at: Date.now() / 1000, actor_name: '隔离管理员', metadata: { name: '测试人员2' } },
    { audit_id: 'aud-denied', action: 'permissions', status: 'denied', updated_at: Date.now() / 1000, actor_name: '楼栋主管', metadata: { changes: [{ name: '测试人员0', before: { scopes: ['H'] }, after: { scopes: ['H'] }, enabled: false }] } },
  ];
  await accounts.locator('nav[aria-label="账号管理"]').getByRole('button', { name: '变更记录', exact: true }).click();
  await accounts.getByRole('cell', { name: '修改密码', exact: true }).waitFor();
  assert.equal(await accounts.getByRole('cell', { name: '撤销登录', exact: true }).count(), 1, 'audit renders 撤销登录 label');
  assert.equal(await accounts.getByRole('cell', { name: '修改权限', exact: true }).count(), 1, 'audit renders 修改权限 label');
  assert.equal(await accounts.getByRole('cell', { name: '已拒绝', exact: true }).count(), 1, 'audit renders 已拒绝 status');
  await page.screenshot({ path: path.join(output, 'audit.png'), fullPage: true });
  auditFixture = [];

  // ---------- Own page -> other settings -> handover navigation ----------
  const otherToggle = page.getByRole('button', { name: '其他设置', exact: true });
  await otherToggle.click();
  assert.equal(await otherToggle.getAttribute('aria-expanded'), 'true');
  await page.getByRole('button', { name: '交接班链接', exact: true }).click();
  await page.waitForURL('**/admin/settings?tab=handover');
  await page.reload();
  await page.getByRole('button', { name: '交接班链接', exact: true }).waitFor();
  assert.equal(await page.getByRole('button', { name: '其他设置', exact: true }).getAttribute('aria-expanded'), 'true');
  await page.getByRole('button', { name: '返回首页', exact: true }).click();
  await page.waitForURL(origin + '/');

  // ---------- L4 denied: no accounts mount, no account API loads ----------
  start = apiCalls.length;
  actor = actors.denied;
  policyLevel = 'L4';
  await page.goto(origin + '/admin/settings');
  await page.locator('.settings-denied').waitFor();
  assert.equal(await page.locator('.admin-shell').count(), 0, 'L4 has no admin shell');
  assert.equal(await page.locator('.accounts').count(), 0, 'L4 has no accounts pane');
  assert.equal(apiCalls.slice(start).filter(c => c.path.startsWith('/api/auth/accounts')).length, 0, 'L4 must not load /api/auth/accounts');
  assert.match(await page.locator('.settings-denied').innerText(), /没有人员管理权限/);
  await page.screenshot({ path: path.join(output, 'admin-denied.png'), fullPage: true });
  assertNoPrivilegedCalls(start);

  assert.deepEqual(errors, []);
  console.log(JSON.stringify({ ok: true, permissionPosts, passwordPosts, savePosts: savePosts.length, legacyReads, adminReads: adminReads.length, apiCalls: apiCalls.length, screenshots: output }));
} catch (error) {
  const page = browser.contexts()[0]?.pages()[0];
  if (page) await page.screenshot({ path: path.join(output, 'failure.png'), fullPage: true }).catch(() => null);
  throw error;
} finally {
  await browser.close();
  await server.close();
}