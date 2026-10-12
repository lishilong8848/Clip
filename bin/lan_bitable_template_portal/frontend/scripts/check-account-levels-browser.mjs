import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';
import { createServer } from 'vite';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const output = path.resolve(root, '../../../output/playwright/account-levels');
await mkdir(output, { recursive: true });
// Use the on-demand dev server (not the prebuilt dist) so this isolated script always
// verifies the current AdminAccounts source without requiring a full production build.
const server = await createServer({ root, logLevel: 'error', server: { host: '127.0.0.1', port: 0 }, preview: {} });
await server.listen();
const origin = `http://127.0.0.1:${server.config.server.port}`;
const browser = await chromium.launch({ headless: true });
const errors = [];
const scopeOptions = ['110', ...'ABCDEH'].map(value => ({ value, label: value === '110' ? '110站' : value + '楼' }));
const categories = [{ value: 'shift_leader', label: '值班长' }, { value: 'operator', label: '值班员' },
  { value: 'engineer', label: '工程师' }, { value: 'supervisor', label: '主管' },
  { value: 'manager', label: '经理' }, { value: 'duty', label: '固定值班账号' }, { value: 'other', label: '其他/待分类' }];

// Per-level acting session: L0/L1 are full admins (system admin), L2/L3 are building
// managers that can manage accounts (accounts-only UI, no admin workspace chrome),
// L4 has no account-management permission at all.
const actors = {
  L0: { open_id: 'ou_levels_admin', name: '隔离测试超级管理员', role: 'admin', can_manage_accounts: true, home_scope: 'H' },
  L1: { open_id: 'ou_levels_campus', name: '隔离测试跨楼管理员', role: 'admin', can_manage_accounts: true, home_scope: 'H' },
  L2: { open_id: 'ou_levels_engineer_editor', name: '隔离测试工程师编辑', role: 'building', can_manage_accounts: true, home_scope: 'H' },
  L3: { open_id: 'ou_levels_building_supervisor', name: '隔离测试楼栋主管', role: 'building', can_manage_accounts: true, home_scope: 'A' },
  L4: { open_id: 'ou_levels_plain', name: '隔离测试普通账号', role: 'building', can_manage_accounts: false, home_scope: 'H' },
};
let actor = actors.L0;
let policyLevel = 'L0';
let refreshDowngradeId = null;
const deletePosts = [];
const savePosts = [];
const apiCalls = [];

function baseItems() {
  const items = [
    // A-building engineer managed by the L2 engineer editor and the L3 A-building supervisor.
    { id: 'rec0', record_id: 'rec0', open_id: 'ou0', name: '张工程师', employee_no: 'E-000', kind: 'external', own_scopes: ['A'], scopes: ['A'], role: 'building', position: '暖通工程师', category: 'engineer', enabled: true, authorized: true, locked: false, blocked_reason: '', can_set_password: true, password_set: false, revision: 'v0', level: 'L2', can_edit: true, can_delete: true, last_login: 1700000000, account_nature: '外部人员' },
    // Same-building supervisor: L3 never manages supervisors/managers themselves.
    { id: 'rec1', record_id: 'rec1', open_id: 'ou1', name: '李主管', employee_no: 'E-001', kind: 'external', own_scopes: ['A'], scopes: ['A'], role: 'building', position: '值班主管', category: 'supervisor', enabled: true, authorized: true, locked: false, blocked_reason: '', can_set_password: true, password_set: true, revision: 'v1', level: 'L3', can_edit: true, can_delete: true, last_login: 1700000001, account_nature: '外部人员' },
    // H fixed duty account: L1 full admin (fully editable scope) but still undeletable.
    { id: 'rec2', record_id: '', open_id: 'ou_duty', name: 'H楼固定值班', employee_no: '', kind: 'duty', own_scopes: ['H'], scopes: ['H'], role: 'building', position: '', category: 'duty', enabled: true, authorized: true, locked: false, blocked_reason: '', can_set_password: false, password_set: false, revision: 'v2', level: 'L1', can_edit: true, can_delete: false, account_nature: '值班账号' },
    // Read-only operator in a foreign building (E) so L3 (A supervisor) cannot reach it.
    { id: 'rec3', record_id: 'rec3', open_id: 'ou3', name: '只读人员', employee_no: 'E-003', kind: 'external', own_scopes: ['E'], scopes: ['E'], role: 'building', position: '值班员', category: 'operator', enabled: true, authorized: true, locked: false, blocked_reason: '', can_set_password: false, password_set: false, revision: 'v3', level: 'L4', can_edit: false, can_delete: false, last_login: 1700000003, account_nature: '外部人员' },
    { id: 'rec4', record_id: 'rec4', open_id: 'ou4', name: '历史管理员', employee_no: 'E-004', kind: 'vnet', own_scopes: ['H'], scopes: ['ALL'], role: 'admin', position: '经理', category: 'manager', enabled: true, authorized: true, locked: true, blocked_reason: '', can_set_password: false, password_set: false, revision: 'v4', level: 'L1', can_edit: false, can_delete: false, last_login: 1700000004, account_nature: 'VNET' },
    // Engineer in another building: foreign to the L3 A-building supervisor.
    { id: 'rec5', record_id: 'rec5', open_id: 'ou5', name: '王工程师', employee_no: 'E-005', kind: 'external', own_scopes: ['E'], scopes: ['E'], role: 'building', position: '给排水工程师', category: 'engineer', enabled: true, authorized: true, locked: false, blocked_reason: '', can_set_password: true, password_set: true, revision: 'v5', level: 'L2', can_edit: true, can_delete: true, last_login: 1700000005, account_nature: '外部人员' },
    // A-building fixed duty managed by the L3 A-building supervisor; undeletable like all duty rows.
    { id: 'rec6', record_id: 'rec6', open_id: 'ou6', name: 'A楼固定值班', employee_no: '', kind: 'duty', own_scopes: ['A'], scopes: ['A'], role: 'building', position: '', category: 'duty', enabled: true, authorized: true, locked: false, blocked_reason: '', can_set_password: false, password_set: false, revision: 'v6', level: 'L4', can_edit: true, can_delete: false, account_nature: '值班账号' },
    { id: 'rec7', record_id: 'rec7', open_id: 'ou7', name: '钱值班长', employee_no: 'E-007', kind: 'external', own_scopes: ['A'], scopes: ['A'], role: 'building', position: '值班长', category: 'shift_leader', enabled: true, authorized: true, locked: false, blocked_reason: '', can_set_password: true, password_set: false, revision: 'v7', level: 'L4', can_edit: true, can_delete: true, last_login: 1700000007, account_nature: '外部人员' },
    { id: 'rec8', record_id: 'rec8', open_id: 'ou8', name: '孙值班员', employee_no: 'E-008', kind: 'external', own_scopes: ['A'], scopes: ['A'], role: 'building', position: '值班员', category: 'operator', enabled: true, authorized: true, locked: false, blocked_reason: '', can_set_password: true, password_set: false, revision: 'v8', level: 'L4', can_edit: true, can_delete: true, last_login: 1700000008, account_nature: '外部人员' },
  ];
  for (const item of items) {
    if (item.category === 'engineer' && item.own_scopes[0] !== 'H') item.level = 'L4';
    if (item.kind === 'duty' && item.own_scopes[0] === 'H') {
      item.role = 'admin';
      item.scopes = ['ALL', 'CAMPUS', ...scopeOptions.map(scope => scope.value)];
    }
  }
  return items;
}

function itemsForLevel(level) {
  const items = baseItems();
  // Mirror backend: L2 view is a minimal engineer edit selector without private activity
  // fields, and L2 can edit engineers but never delete (server always reports can_delete=false).
  if (level === 'L2') {
    for (const item of items) {
      delete item.last_login;
      delete item.password_set;
      delete item.account_nature;
      delete item.employee_no;
      item.can_delete = false;
    }
    return items.filter(x => x.category === 'engineer');
  }
  // L3 supervises only its own building (home_scope) and only engineer/operator/shift_leader/duty rows
  // (no supervisors, managers, locked admins, or other buildings).
  if (level === 'L3') {
    const managed = new Set(['engineer', 'operator', 'shift_leader', 'duty']);
    const home = actor.home_scope;
    return items.filter(x => managed.has(x.category)
      && x.own_scopes.length === 1 && x.own_scopes[0] === home);
  }
  return items;
}

function isApiRequest(url) {
  try { return new URL(url).pathname.startsWith('/api/'); } catch { return false; }
}

async function openSettings(level) {
  const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
  page.setDefaultTimeout(30000);
  page.on('pageerror', error => errors.push(error.message));
  await page.route(url => isApiRequest(url), async route => {
    const req = route.request();
    const url = new URL(req.url());
    const method = req.method();
    apiCalls.push({ method, path: url.pathname });
    const ok = data => route.fulfill({ json: { ok: true, data } });
    if (url.pathname === '/api/auth/status') return ok({
      logged_in: true,
      user: { open_id: actor.open_id, name: actor.name, role: actor.role, can_manage_accounts: actor.can_manage_accounts, login_method: 'feishu' },
      scope_options: scopeOptions,
    });
    if (url.pathname === '/api/health') return route.fulfill({ json: { ok: true, service: 'clipflow_backend', instance_id: 'isolated-account-levels' } });
    if (url.pathname === '/api/auth/permissions') return ok({ users: baseItems().filter(x => x.authorized), scope_options: scopeOptions });
    if (url.pathname === '/api/auth/permission-requests/admin') return ok({ items: [] });
    if (url.pathname === '/api/auth/accounts' && method === 'GET') {
      const levelNow = policyLevel;
      const items = itemsForLevel(levelNow);
      // Simulate a permission downgrade on manual refresh: the backend no longer reports
      // edit authority for the tracked target, so AdminAccounts must close a stale editor.
      if (url.searchParams.get('refresh') === 'true' && refreshDowngradeId) {
        for (const item of items) {
          if (item.id === refreshDowngradeId) {
            item.can_edit = false;
            if (item.can_delete) item.can_delete = false;
          }
        }
      }
      return ok({
        items,
        category_options: categories,
        scope_options: levelNow === 'L3' ? scopeOptions.filter(x => x.value === actor.home_scope) : scopeOptions,
        policy: { level: levelNow, home_scope: actor.home_scope, can_manage_accounts: actor.can_manage_accounts, can_view_accounts: levelNow !== 'L2' },
        loaded_at: Date.now() / 1000,
      });
    }
    if (url.pathname === '/api/auth/accounts/permissions') {
      savePosts.push(req.postDataJSON());
      for (const change of req.postDataJSON().changes) {
        assert.deepEqual(Object.keys(change).sort(), ['enabled', 'id', 'revision', 'role', 'scopes']);
      }
      return ok({ changed: req.postDataJSON().changes.length });
    }
    if (url.pathname === '/api/auth/accounts/audit') return ok({ items: auditFixture });
    if (method === 'DELETE' && url.pathname.startsWith('/api/auth/accounts/')) {
      deletePosts.push({ path: url.pathname, body: req.postDataJSON() });
      assert.deepEqual(Object.keys(req.postDataJSON()).sort(), ['revision']);
      return ok({ deleted: true, history_preserved: true });
    }
    if (url.pathname === '/api/assistant/conversation') return ok({ enabled: false, turns: [] });
    return ok({ items: [] });
  });
  await page.goto(origin + '/admin/settings');
  await Promise.race([
    page.locator('.admin-shell').waitFor(),
    page.locator('.settings-denied').waitFor(),
  ]);
  return page;
}

async function openAccounts(level) {
  const page = await openSettings(level);
  const chrome = page.locator('.admin-workspace-tabs');
  if (level === 'L0' || level === 'L1') {
    await chrome.waitFor();
    assert.equal(await chrome.count(), 1, `${level} renders full admin workspace tabs`);
    await chrome.getByRole('button', { name: /^账号与权限/ }).click();
  } else {
    assert.equal(await chrome.count(), 0, `${level} is accounts-only (no admin workspace tabs)`);
  }
  const accounts = page.locator('.accounts');
  await accounts.waitFor();
  return page;
}

// Audit rows served to /api/auth/accounts/audit; tests override this per page.
let auditFixture = [];

function newApiCallsSince(startIndex) {
  return apiCalls.slice(startIndex);
}

// Global/privileged endpoints that must never fire for non-system-admin levels (L2/L3/L4).
const privilegedPrefixes = [
  '/api/auth/permissions',
  '/api/auth/permission-requests/admin',
  '/api/backend/stats',
  '/api/backend/perf',
  '/api/backend/queues',
  '/api/backend/consistency',
  '/api/backend/notice-diagnostic',
  '/api/backend/preflight',
  '/api/backend/jobs',
  '/api/jobs/',
  '/api/handover-links',
  '/api/admin/mop-settings',
  '/api/backend/mock-pressure',
  '/api/backend/operation-audits',
  '/api/backend/notice-projection-repair',
];

function assertNoPrivilegedCalls(startIndex) {
  const fired = newApiCallsSince(startIndex).filter(call => privilegedPrefixes.some(prefix => call.path.startsWith(prefix)));
  assert.deepEqual(fired, [], `no privileged/global admin requests may fire for ${policyLevel}`);
}

try {
  // ---------- L0: full admin chrome, tabs, metrics, last_login, delete ----------
  let start = apiCalls.length;
  policyLevel = 'L0';
  actor = actors.L0;
  let page = await openAccounts('L0');
  const accounts = page.locator('.accounts');
  const tabNav = accounts.locator('nav[aria-label="账号管理"]');
  assert.equal((await tabNav.getByRole('button').allTextContents()).length, 3, 'L0 shows accounts, requests, audit tabs');
  assert.equal(await accounts.locator('.metrics').count(), 1, 'L0 shows metrics');
  assert.equal(await accounts.locator('th', { hasText: '最近登录' }).count(), 1, 'L0 shows last_login column');
  assert.equal(await accounts.getByRole('button', { name: '撤销张工程师的登录权限', exact: true }).count(), 1, 'L0 shows delete for can_delete=true');
  assert.equal(await accounts.getByRole('button', { name: '撤销H楼固定值班的登录权限', exact: true }).count(), 0, 'duty cannot delete (can_delete=false)');
  assert.equal(await accounts.getByRole('button', { name: '撤销只读人员的登录权限', exact: true }).count(), 0, 'can_delete=false hides delete');
  await accounts.locator('tbody').getByText('L4 普通账号', { exact: true }).first().waitFor();
  await page.screenshot({ path: path.join(output, 'l0-accounts.png'), fullPage: true });

  // Delete flow: confirm dialog + DELETE /api/auth/accounts/{id} with {revision}.
  let dialogHandler = dialog => { assert.match(dialog.message(), /撤销.*登录权限/); dialog.accept(); };
  page.on('dialog', dialogHandler);
  await accounts.getByRole('button', { name: '撤销张工程师的登录权限', exact: true }).click();
  await page.waitForFunction(() => document.querySelector('.success')?.textContent.includes('已撤销'));
  assert.equal(deletePosts.length, 1);
  assert.equal(deletePosts[0].path, '/api/auth/accounts/rec0');
  assert.equal(deletePosts[0].body.revision, 'v0');
  page.removeListener('dialog', dialogHandler);
  await page.close();

  // Drawer: role/scopes are read-only (no role select / no scope checkboxes), only enabled toggle.
  page = await openAccounts('L0');
  const accounts2 = page.locator('.accounts');
  await accounts2.getByRole('button', { name: '管理张工程师', exact: true }).click();
  let dialog = page.getByRole('dialog');
  await dialog.getByText('L4 普通账号', { exact: true }).waitFor();
  assert.equal(await dialog.getByRole('combobox').count(), 0, 'no role select in unified permissions drawer');
  assert.equal(await dialog.getByRole('checkbox', { name: /楼/ }).count(), 0, 'no scope checkboxes in unified permissions drawer');
  assert.equal(await dialog.getByRole('checkbox', { name: '启用登录', exact: true }).isEnabled(), true);
  assert.equal(await dialog.getByRole('button', { name: '保存启用状态', exact: true }).isEnabled(), true);
  await dialog.getByRole('button', { name: '关闭账号详情' }).click();
  // can_edit=false keeps the drawer read-only.
  await accounts2.getByRole('button', { name: '管理只读人员', exact: true }).click();
  dialog = page.getByRole('dialog');
  assert.equal(await dialog.getByRole('button', { name: '保存启用状态', exact: true }).isDisabled(), true);
  await dialog.getByRole('button', { name: '关闭账号详情' }).click();
  await page.close();

  // Audit labels: delete => 撤销登录, permissions/denied => 修改权限/已拒绝, password/started => 修改密码/处理中.
  start = apiCalls.length;
  policyLevel = 'L0';
  auditFixture = [
    { audit_id: 'aud-delete', action: 'delete', status: 'success', updated_at: Date.now() / 1000, actor_name: '超级管理员', metadata: { name: '张工程师' } },
    { audit_id: 'aud-denied', action: 'permissions', status: 'denied', updated_at: Date.now() / 1000, actor_name: '楼栋主管', metadata: { changes: [] } },
    { audit_id: 'aud-password-start', action: 'password', status: 'started', updated_at: Date.now() / 1000, actor_name: '管理员', metadata: { name: '王工程师' } },
  ];
  page = await openAccounts('L0');
  const l0Accounts = page.locator('.accounts');
  await l0Accounts.locator('nav[aria-label="账号管理"]').getByRole('button', { name: '变更记录', exact: true }).click();
  await l0Accounts.getByRole('cell', { name: '撤销登录', exact: true }).waitFor();
  assert.equal(await l0Accounts.getByRole('cell', { name: '已拒绝', exact: true }).count(), 1, 'denied audit shows 已拒绝');
  assert.equal(await l0Accounts.getByRole('cell', { name: '修改权限', exact: true }).count(), 1, 'permissions audit shows 修改权限');
  assert.equal(await l0Accounts.getByRole('cell', { name: '修改密码', exact: true }).count(), 1, 'password audit shows 修改密码');
  await page.screenshot({ path: path.join(output, 'l0-audit-labels.png'), fullPage: true });
  await page.close();
  auditFixture = [];

  // ---------- L1: separate full-admin scenario (跨楼超级管理员) ----------
  start = apiCalls.length;
  policyLevel = 'L1';
  actor = actors.L1;
  page = await openAccounts('L1');
  const l1Accounts = page.locator('.accounts');
  assert.equal(await page.locator('.admin-workspace-tabs').count(), 1, 'L1 shows admin workspace tabs');
  const l1Tabs = l1Accounts.locator('nav[aria-label="账号管理"]');
  assert.equal((await l1Tabs.getByRole('button').allTextContents()).length, 3, 'L1 shows accounts, requests, audit tabs');
  assert.equal(await l1Accounts.locator('.metrics').count(), 1, 'L1 shows metrics');
  assert.equal(await l1Accounts.locator('th', { hasText: '最近登录' }).count(), 1, 'L1 shows last_login column');
  assert.equal(await l1Accounts.locator('tbody').getByText('L1 跨楼超级管理员', { exact: true }).count(), 2, 'H manager and duty both render L1 level');
  // The L1 H fixed duty account is now a full L1 admin entry: it appears in the L1 list,
  // exposes the full admin tabs, and is still undeletable (can_delete=false).
  assert.equal(await l1Accounts.getByRole('button', { name: '管理H楼固定值班', exact: true }).count(), 1, 'L1 shows H duty entry');
  assert.equal(await l1Accounts.getByRole('button', { name: '撤销H楼固定值班的登录权限', exact: true }).count(), 0, 'H duty remains undeletable at L1');
  assert.equal(await l1Accounts.locator('tbody tr', { hasText: 'H楼固定值班' }).getByText('L1 跨楼超级管理员', { exact: true }).count(), 1, 'H duty routes L1 full-admin level label');
  assert.equal(await l1Accounts.getByRole('button', { name: '撤销张工程师的登录权限', exact: true }).count(), 1, 'L1 shows delete for can_delete=true');
  await page.screenshot({ path: path.join(output, 'l1-accounts.png'), fullPage: true });
  await page.close();

  // ---------- L2: accounts-only, minimal engineer edit selector, no admin chrome/global calls ----------
  start = apiCalls.length;
  policyLevel = 'L2';
  actor = actors.L2;
  page = await openAccounts('L2');
  const accounts3 = page.locator('.accounts');
  const tabNav2 = accounts3.locator('nav[aria-label="账号管理"]');
  assert.equal(await page.locator('.admin-workspace-tabs').count(), 0, 'L2 renders accounts-only (no admin workspace tabs)');
  assert.equal((await tabNav2.getByRole('button').allTextContents()).length, 1, 'L2 has only accounts tab');
  assert.equal(await tabNav2.getByRole('button', { name: '账号列表', exact: true }).count(), 1);
  assert.equal(await accounts3.locator('.metrics').count(), 0, 'L2 hides metrics');
  assert.equal(await accounts3.locator('th', { hasText: '最近登录' }).count(), 0, 'L2 hides last_login column');
  assert.equal(await accounts3.locator('tbody tr').count(), 2, 'L2 shows engineer edit selection only');
  assert.equal(await accounts3.getByRole('button', { name: /撤销/ }).count(), 0, 'L2 has no delete capability');
  await page.screenshot({ path: path.join(output, 'l2-accounts.png'), fullPage: true });
  await accounts3.getByRole('button', { name: '管理张工程师', exact: true }).click();
  const dialog2 = page.getByRole('dialog');
  assert.equal(await dialog2.getByText('工号', { exact: true }).count(), 0, 'L2 drawer omits employee_no');
  assert.equal(await dialog2.getByRole('combobox').count(), 0, 'L2 drawer has no role select');
  assert.equal(await dialog2.getByRole('checkbox', { name: /楼/ }).count(), 0, 'L2 drawer has no scope checkboxes');
  assert.equal(await dialog2.getByRole('checkbox', { name: '启用登录', exact: true }).isEnabled(), true, 'engineer edit selector allows enabled toggle');
  await page.getByRole('button', { name: '关闭账号详情' }).click();
  await page.close();
  assertNoPrivilegedCalls(start);

  // ---------- L2 downgrade: a refresh that revokes edit on an open target closes the stale editor ----------
  policyLevel = 'L2';
  actor = actors.L2;
  refreshDowngradeId = 'rec0';
  page = await openAccounts('L2');
  const downgradeAccounts = page.locator('.accounts');
  await downgradeAccounts.getByRole('button', { name: '管理张工程师', exact: true }).click();
  await page.getByRole('dialog').waitFor();
  // Manual "同步人员" triggers load(true); backend now reports rec0 as can_edit=false.
  // The open drawer overlay intercepts pointer events, so invoke the button's click handler
  // directly to verify the reload itself closes the stale editor.
  await page.evaluate(() => { const button = document.querySelector('.account-header button'); button?.click(); });
  await page.waitForFunction(() => !document.querySelector('.account-editor'));
  assert.equal(await page.getByRole('dialog').count(), 0, 'stale editor closes after edit authority is revoked on refresh');
  await page.close();
  refreshDowngradeId = null;

  // ---------- L3: accounts-only, own-building engineer/operator/shift_leader/duty rows only, no global tabs ----------
  start = apiCalls.length;
  policyLevel = 'L3';
  actor = actors.L3;
  auditFixture = [{ audit_id: 'own-audit', action: 'permissions', status: 'success', updated_at: Date.now() / 1000, actor_name: actor.name, metadata: { changes: [] } }];
  page = await openAccounts('L3');
  const accounts4 = page.locator('.accounts');
  const tabNav3 = accounts4.locator('nav[aria-label="账号管理"]');
  assert.equal(await page.locator('.admin-workspace-tabs').count(), 0, 'L3 renders accounts-only (no admin workspace tabs)');
  assert.equal(await tabNav3.getByRole('button', { name: '权限申请', exact: true }).count(), 0, 'L3 has no global requests tab');
  assert.equal(await tabNav3.getByRole('button', { name: '变更记录', exact: true }).count(), 1, 'L3 keeps audit tab (own audits)');
  assert.equal(await accounts4.locator('.metrics').count(), 1, 'L3 shows metrics');
  assert.equal(await accounts4.locator('tbody tr').count(), 4, 'L3 shows only own-building managed rows');
  await accounts4.getByRole('button', { name: '管理张工程师', exact: true }).waitFor();
  await accounts4.getByRole('button', { name: '管理A楼固定值班', exact: true }).waitFor();
  await accounts4.getByRole('button', { name: '管理钱值班长', exact: true }).waitFor();
  await accounts4.getByRole('button', { name: '管理孙值班员', exact: true }).waitFor();
  assert.equal(await accounts4.getByRole('button', { name: /李主管/ }).count(), 0, 'L3 excludes supervisor in own building');
  assert.equal(await accounts4.getByRole('button', { name: /历史管理员/ }).count(), 0, 'L3 excludes manager/admin in own building');
  assert.equal(await accounts4.getByRole('button', { name: /王工程师/ }).count(), 0, 'L3 excludes engineer from other building');
  assert.equal(await accounts4.getByRole('button', { name: /只读人员/ }).count(), 0, 'L3 excludes operator from other building');
  await accounts4.locator('tbody').getByText('L4 普通账号', { exact: true }).first().waitFor();
  await page.screenshot({ path: path.join(output, 'l3-accounts.png'), fullPage: true });
  await tabNav3.getByRole('button', { name: '变更记录', exact: true }).click();
  await accounts4.getByRole('cell', { name: '修改权限', exact: true }).waitFor();
  assert.equal(await accounts4.locator('tbody tr').count(), 1, 'L3 audit returns own rows only');
  await page.screenshot({ path: path.join(output, 'l3-audit.png'), fullPage: true });
  await page.close();
  auditFixture = [];
  assertNoPrivilegedCalls(start);

  // ---------- L4: denied settings, no accounts/settings API loads ----------
  start = apiCalls.length;
  policyLevel = 'L4';
  actor = actors.L4;
  page = await openSettings('L4');
  assert.equal(await page.locator('.settings-denied').count(), 1, 'L4 shows settings denied');
  assert.equal(await page.locator('.admin-shell').count(), 0, 'L4 has no admin shell');
  assert.match(await page.locator('.settings-denied').innerText(), /没有人员管理权限/);
  assert.equal(await page.locator('.accounts').count(), 0, 'L4 has no accounts pane');
  assert.equal(apiCalls.slice(start).filter(c => c.path.startsWith('/api/auth/accounts')).length, 0, 'L4 must not load /api/auth/accounts');
  await page.screenshot({ path: path.join(output, 'l4-denied.png'), fullPage: true });
  await page.close();
  assertNoPrivilegedCalls(start);

  // ---------- Save payload keeps id/revision/role/scopes/enabled ----------
  const beforeSave = savePosts.length;
  policyLevel = 'L0';
  actor = actors.L0;
  page = await openAccounts('L0');
  const accounts5 = page.locator('.accounts');
  await accounts5.getByRole('button', { name: '管理张工程师', exact: true }).click();
  const dialog3 = page.getByRole('dialog');
  await dialog3.getByRole('checkbox', { name: '启用登录', exact: true }).uncheck();
  await dialog3.getByRole('button', { name: '保存启用状态', exact: true }).click();
  await page.waitForFunction(() => document.querySelectorAll('.success').length > 0);
  assert.equal(savePosts.length, beforeSave + 1);
  assert.deepEqual(Object.keys(savePosts[savePosts.length - 1].changes[0]).sort(), ['enabled', 'id', 'revision', 'role', 'scopes']);
  assert.equal(savePosts[savePosts.length - 1].changes[0].enabled, false);
  await page.close();

  // A persisted session opened before the first trusted roster refresh recovers without a reload.
  page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
  page.on('pageerror', error => errors.push(error.message));
  let authReads = 0;
  await page.route(url => isApiRequest(url), route => {
    const url = new URL(route.request().url());
    if (url.pathname === '/api/auth/status') {
      authReads++;
      const pending = authReads === 1;
      return route.fulfill({ json: { ok: true, data: { logged_in: true, directory_pending: pending,
        user: actors.L3, scope_options: pending ? [] : scopeOptions.filter(scope => scope.value === 'A') } } });
    }
    return route.fulfill({ json: { ok: true, data: { items: [] } } });
  });
  await page.goto(origin + '/admin/settings');
  await page.getByText('正在确认登录状态', { exact: true }).waitFor();
  assert.equal(await page.getByText('当前账号暂无门户权限', { exact: true }).count(), 0);
  await page.locator('.accounts').waitFor();
  assert.ok(authReads >= 2, 'roster readiness automatically refreshes authorization');
  await page.close();

  assert.deepEqual(errors, []);
  console.log(JSON.stringify({ ok: true, deletePosts: deletePosts.length, savePosts: savePosts.length, apiCalls: apiCalls.length, screenshots: output }));
} catch (error) {
  const ctx = browser.contexts()[0];
  const page = ctx ? ctx.pages()[0] : undefined;
  if (page) await page.screenshot({ path: path.join(output, 'failure.png'), fullPage: true });
  throw error;
} finally {
  await browser.close();
  await server.close();
}
