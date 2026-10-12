import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';
import { preview } from 'vite';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const output = path.resolve(root, '../../../output/playwright/admin-accounts');
await mkdir(output, { recursive: true });
const server = await preview({ root, logLevel: 'error', preview: { host: '127.0.0.1', port: 0 } });
const origin = `http://127.0.0.1:${server.httpServer.address().port}`;
const browser = await chromium.launch({ headless: true });
const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
page.setDefaultTimeout(12000);
const scopeOptions = ['110', ...'ABCDEH'].map(value => ({ value, label: value === '110' ? '110站' : value + '楼' }));
const items = Array.from({ length: 26 }, (_, i) => ({ id: 'recTest' + i, record_id: 'recTest' + i, open_id: 'ou_test' + i,
  name: '测试人员' + i, employee_no: 'TEST-' + i, kind: 'external', own_scopes: ['A'], scopes: ['A'], role: 'building',
  enabled: true, authorized: true, locked: false, blocked_reason: '', can_set_password: true, password_set: true, revision: 'v1' }));
items[1] = { ...items[1], name: '内部测试', kind: 'vnet', can_set_password: false };
items[2] = { ...items[2], name: 'E楼值班', kind: 'duty', own_scopes: ['E'], scopes: ['E'], can_set_password: false };
items[3] = { ...items[3], name: '固定管理员', kind: 'vnet', role: 'admin', locked: true, can_set_password: false };
items[4] = { ...items[4], name: '身份待核对', blocked_reason: '多人使用同一OpenID', can_set_password: false };
let failSave = false, passwordPosts = 0, permissionPosts = 0, legacyReads = 0;
const errors = [];
page.on('pageerror', error => errors.push(error.message));
await page.route('**/api/**', async route => {
  const request = route.request(), url = new URL(request.url()), body = request.method() === 'POST' ? request.postDataJSON() : {};
  const ok = data => route.fulfill({ json: { ok: true, data } });
  if (url.pathname === '/api/auth/status') return ok({ logged_in: true, user: { open_id: 'ou_admin', name: '隔离管理员', role: 'admin', login_method: 'feishu' }, scope_options: scopeOptions });
  if (url.pathname === '/api/health') return route.fulfill({ json: { ok: true, service: 'clipflow_backend', instance_id: 'isolated-accounts' } });
  if (url.pathname === '/api/auth/accounts') return ok({ items, scope_options: scopeOptions, loaded_at: Date.now() / 1000 });
  if (url.pathname === '/api/auth/permissions') return ok({ users: items.filter(x => x.authorized), scope_options: scopeOptions });
  if (url.pathname === '/api/auth/permission-directory') legacyReads++;
  if (url.pathname === '/api/auth/accounts/permissions') {
    permissionPosts++;
    if (failSave) return route.fulfill({ status: 409, json: { ok: false, error: '该账号权限已变化，请刷新后重新确认。' } });
    for (const change of body.changes) {
      assert.deepEqual(Object.keys(change).sort(), ['enabled', 'id', 'revision', 'role', 'scopes']);
      Object.assign(items.find(x => x.id === change.id), change, { revision: 'v' + (permissionPosts + 1) });
    }
    return ok({ changed: body.changes.length });
  }
  if (url.pathname === '/api/auth/accounts/recTest0/password') {
    passwordPosts++;
    assert.deepEqual(body, { new_password: 'FixtureNew123!', confirm_password: 'FixtureNew123!' });
    return ok({ password_changed: true });
  }
  if (url.pathname === '/api/auth/accounts/audit') return ok({ items: [{ audit_id: 'fixture-audit', action: 'password', status: 'success', updated_at: Date.now() / 1000, actor_name: '隔离管理员', metadata: { name: '测试人员0' } }] });
  if (url.pathname === '/api/assistant/conversation') return ok({ enabled: false, turns: [] });
  return ok({ items: [] });
});

try {
  await page.goto(origin);
  await page.getByRole('button', { name: '打开管理员诊断和权限管理' }).click();
  await page.locator('.admin-workspace-tabs').getByRole('button', { name: /^账号与权限/ }).click();
  const accounts = page.locator('.accounts');
  await accounts.getByRole('button', { name: '管理测试人员0', exact: true }).waitFor();
  assert.equal(await accounts.locator('tbody tr').count(), 20);
  assert.equal(legacyReads, 0, 'new page must not load the separate legacy directory');
  await page.screenshot({ path: path.join(output, 'accounts-desktop.png'), fullPage: true });
  await accounts.getByRole('button', { name: '下一页', exact: true }).click();
  assert.equal(await accounts.locator('tbody tr').count(), 6);
  await accounts.getByRole('searchbox', { name: '搜索账号' }).fill('TEST-0');
  assert.equal(await accounts.locator('tbody tr').count(), 1);
  await accounts.getByRole('button', { name: '管理测试人员0', exact: true }).click();
  let dialog = page.getByRole('dialog');
  await dialog.getByRole('checkbox', { name: 'B楼', exact: true }).check();
  failSave = true;
  await dialog.getByRole('button', { name: '保存权限', exact: true }).click();
  await dialog.getByRole('alert').waitFor();
  assert.equal(await dialog.getByRole('checkbox', { name: 'B楼', exact: true }).isChecked(), true);
  failSave = false;
  await dialog.getByRole('button', { name: '保存权限', exact: true }).click();
  await dialog.waitFor({ state: 'hidden' });
  assert.deepEqual(items[0].scopes, ['A', 'B']);
  await accounts.getByRole('button', { name: '管理测试人员0', exact: true }).click();
  dialog = page.getByRole('dialog');
  await dialog.getByLabel('新密码', { exact: true }).fill('FixtureNew123!');
  await dialog.getByLabel('再次输入新密码', { exact: true }).fill('Different123');
  await dialog.getByRole('button', { name: '确认修改密码' }).click();
  await dialog.getByText('两次输入的密码不一致', { exact: true }).waitFor();
  assert.equal(passwordPosts, 0);
  await dialog.getByLabel('再次输入新密码', { exact: true }).fill('FixtureNew123!');
  await dialog.getByRole('button', { name: '确认修改密码' }).click();
  await page.waitForFunction(() => document.querySelector('.success')?.textContent.includes('需重新登录'));
  assert.equal(passwordPosts, 1);
  assert.equal(await dialog.getByLabel('新密码', { exact: true }).inputValue(), '');
  await page.screenshot({ path: path.join(output, 'account-password.png'), fullPage: true });
  await dialog.getByRole('button', { name: '关闭账号详情' }).click();
  await accounts.getByRole('searchbox', { name: '搜索账号' }).fill('内部测试');
  await accounts.getByRole('button', { name: '管理内部测试' }).click();
  await page.getByText('飞书账号密码请在飞书中修改。', { exact: true }).waitFor();
  assert.equal(await page.locator('#admin-account-new-password').count(), 0);
  await page.getByRole('button', { name: '关闭账号详情' }).click();
  await accounts.getByRole('searchbox', { name: '搜索账号' }).fill('固定管理员');
  await accounts.getByRole('button', { name: '管理固定管理员' }).click();
  assert.equal(await page.getByRole('dialog').getByRole('button', { name: '保存权限' }).isDisabled(), true);
  await page.keyboard.press('Escape');
  await accounts.getByRole('searchbox', { name: '搜索账号' }).fill('TEST-0');
  await accounts.getByRole('checkbox', { name: '选择测试人员0', exact: true }).check();
  await accounts.getByRole('button', { name: '增加楼栋权限' }).click();
  await page.getByRole('dialog').getByRole('checkbox', { name: 'C楼', exact: true }).check();
  await page.getByRole('button', { name: '确认增加权限', exact: true }).click();
  await page.getByRole('dialog').waitFor({ state: 'hidden' });
  assert.deepEqual(items[0].scopes, ['A', 'B', 'C']);
  await accounts.getByRole('button', { name: '变更记录', exact: true }).click();
  await accounts.getByRole('cell', { name: '修改密码', exact: true }).waitFor();
  const storage = await page.evaluate(() => JSON.stringify([Object.entries(localStorage), Object.entries(sessionStorage)]));
  assert.ok(!storage.includes('FixtureNew123!'));
  assert.deepEqual(errors, []);
  console.log(JSON.stringify({ ok: true, permissionPosts, passwordPosts, legacyReads, screenshots: output }));
} catch (error) {
  await page.screenshot({ path: path.join(output, 'failure.png'), fullPage: true });
  throw error;
} finally {
  await browser.close();
  await new Promise(resolve => server.httpServer.close(resolve));
}
