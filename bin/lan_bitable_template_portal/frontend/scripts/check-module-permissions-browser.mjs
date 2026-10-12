import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';
import { preview } from 'vite';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const output = path.resolve(root, '../../../output/playwright/module-permissions');
await mkdir(output, { recursive: true });
const server = await preview({ root, logLevel: 'error', preview: { host: '127.0.0.1', port: 0 } });
const origin = `http://127.0.0.1:${server.httpServer.address().port}`;
const browser = await chromium.launch({ headless: true });
const errors = [], calls = [], saves = [];
const scopes = ['A', 'B', 'C', 'D', 'E', 'H', '110'].map(value => ({ value, label: value + '楼' }));
const modules = [
  ['event', '事件管理', '业务管理'], ['maintenance', '维保通告', '通告办理'], ['change', '变更通告', '通告办理'],
  ['engineer_mop', '维护单管理', '业务管理'], ['repair_management', '检修管理（维修单）', '业务管理'], ['critical_guard', '风险管理（重保检查）', '业务管理'],
  ['water', '水耗管理', '业务管理'], ['cabinet', '机柜管理', '业务管理'], ['drills', '演练管理', '业务管理'], ['learning', '画像学练', '业务管理'],
  ['plan_convergence', '计划收敛审查', '业务管理'], ['daily', '每日任务', '业务管理'],
  ['repair', '检修通告', '通告办理'], ['power', '上下电通告', '通告办理'], ['polling', '设备轮巡', '通告办理'], ['adjust', '设备调整', '通告办理'],
  ['signatures', '签名管理', '协作与资料'], ['knowledge', '共享知识库', '协作与资料'], ['links', '多维表导航', '协作与资料'],
  ['life_guide', '参考人生指南', '协作与资料'], ['handover', '交接班审核', '协作与资料'], ['assistant', '灯塔助手与飞书智能体', '协作与资料'],
  ['accounts', '账号与设置', '系统管理'],
].map(([value, label, group]) => ({ value, label, group }));
let user = { open_id: 'ou_fixture', name: '测试管理员', role: 'admin', level: 'L1', modules: modules.map(x => x.value), can_manage_accounts: true };
const target = { id: 'rec_fixture', record_id: 'rec_fixture', open_id: 'ou_target', name: '测试人员', employee_no: 'fixture',
  kind: 'external', own_scopes: ['A'], home_scope: 'A', scopes: ['A'], level: 'L4', role: 'building', position: '值班员',
  category: 'operator', modules: ['learning'], enabled: true, authorized: true, locked: false, blocked_reason: '',
  can_edit: true, can_edit_access: true, can_delete: true, can_set_password: true, revision: 'v1', password_set: true };
async function newPage() {
  const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
  page.setDefaultTimeout(15000);
  page.on('pageerror', error => errors.push(error.message));
  await page.route('**/api/**', async route => {
    const req = route.request(), url = new URL(req.url());
    calls.push(url.pathname);
    const ok = data => route.fulfill({ json: { ok: true, data } });
    if (url.pathname === '/api/auth/status') return ok({ logged_in: true, user, scope_options: scopes });
    if (url.pathname === '/api/health') return route.fulfill({ json: { ok: true, service: 'clipflow_backend', instance_id: 'module-fixture' } });
    if (url.pathname === '/api/auth/permissions') return ok({ users: [], scope_options: scopes });
    if (url.pathname === '/api/auth/accounts') return ok({ items: [target], scope_options: scopes, module_options: modules,
      level_options: ['L1', 'L2', 'L3', 'L4'].map(value => ({ value, label: value })),
      category_options: [{ value: 'operator', label: '值班员' }, { value: 'engineer', label: '工程师' }],
      policy: { level: 'L1', modules: modules.map(x => x.value), can_manage_accounts: true, can_view_accounts: true } });
    if (url.pathname === '/api/auth/accounts/permissions') {
      const payload = req.postDataJSON(); saves.push(payload); Object.assign(target, payload.changes[0]); return ok({ changed: 1 });
    }
    if (url.pathname === '/api/scope-overview') return ok({ scopes: {}, source_snapshot_ready: true });
    if (url.pathname === '/api/assistant/conversation') return ok({ enabled: false, turns: [] });
    return ok({ items: [] });
  });
  return page;
}
try {
  const page = await newPage();
  await page.goto(origin + '/admin/settings');
  await page.getByRole('button', { name: /^管理测试人员$/ }).click();
  const editor = page.getByRole('dialog', { name: '测试人员' });
  assert.equal(await editor.getByLabel('维保通告', { exact: true }).isChecked(), false, 'opening editor preserves explicit grants');
  await editor.getByLabel('权限级别', { exact: true }).selectOption('L1');
  assert.equal(await editor.locator('.account-access input[type=checkbox]:checked').count(), scopes.length + modules.length);
  assert.equal(await editor.getByLabel('签名管理', { exact: true }).isDisabled(), false, 'assignable L1 modules remain configurable');
  await editor.getByLabel('权限级别', { exact: true }).selectOption('L4');
  assert.equal(await editor.getByLabel('账号与设置', { exact: true }).isChecked(), false);
  assert.equal(await editor.getByLabel('A楼', { exact: true }).isChecked(), true);
  assert.equal(await editor.getByLabel('B楼', { exact: true }).isChecked(), false);
  await editor.getByLabel('B楼', { exact: true }).check();
  await editor.getByLabel('签名管理', { exact: true }).uncheck();
  await page.screenshot({ path: path.join(output, 'account-access.png'), fullPage: true });
  const box = await editor.boundingBox();
  assert.ok(box.width > 1000 && box.x > 50 && box.y > 0, 'permission editor is a centered wide modal');
  await editor.getByRole('button', { name: '保存权限', exact: true }).click();
  await page.getByText('已保存 1 个账号的权限', { exact: true }).first().waitFor();
  assert.equal(saves[0].changes[0].level, 'L4');
  assert.deepEqual(saves[0].changes[0].scopes, ['A', 'B']);
  assert.deepEqual(saves[0].changes[0].modules, modules.map(x => x.value).filter(x => !['accounts', 'signatures'].includes(x)));
  await page.close();

  user = { open_id: 'ou_person', name: '当前测试人员', role: 'building', level: 'L4', modules: ['maintenance'],
    can_manage_accounts: false, login_method: 'password', personnel_record_id: 'rec_person' };
  const ordinary = await newPage();
  const start = calls.length;
  await ordinary.goto(origin + '/learning');
  await ordinary.getByRole('heading', { name: '没有该功能权限' }).waitFor();
  assert.equal(calls.slice(start).some(x => x.startsWith('/api/learning/')), false);
  assert.equal(await ordinary.locator('.lighthouse-root').count(), 0);
  await ordinary.goto(origin + '/?account=password');
  await ordinary.getByText('当前测试人员', { exact: true }).last().waitFor();
  assert.equal(await ordinary.locator('#personnel-name, #personnel-reset-name').count(), 0);
  await ordinary.getByRole('button', { name: '忘记密码', exact: true }).click();
  await ordinary.getByText('验证本人身份后即可设置新密码').waitFor();
  assert.equal(await ordinary.locator('#personnel-reset-name').count(), 0);
  assert.equal(calls.slice(start).some(x => x === '/api/auth/password/people'), false);
  await ordinary.screenshot({ path: path.join(output, 'current-person-password.png'), fullPage: true });
  await ordinary.close();
  assert.deepEqual(errors, []);
  console.log('PASS: built UI level transitions, module grant payload, denied module, current-person password flow');
} finally {
  await browser.close();
  await new Promise(resolve => server.httpServer.close(resolve));
}
