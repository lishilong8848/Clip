import assert from 'node:assert/strict';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';
import { preview } from 'vite';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const output = await mkdtemp(path.join(tmpdir(), 'module-permissions-'));
console.log('Screenshots:', output);
const bundle = process.env.CLIPFLOW_WORKSPACE_BUILD_DIR;
const previewPort = process.env.CLIPFLOW_ISOLATED_PREVIEW_PORT;
if (previewPort) assert.match(previewPort, /^\d{1,5}$/);
const server = previewPort ? null : await preview({ root, configFile: false, envDir: false, logLevel: 'error',
  ...(bundle ? { base: '/assets/workspace-v2/', build: { outDir: path.resolve(root, bundle) } } : {}),
  preview: { host: '127.0.0.1', port: 0, proxy: {} } });
const origin = 'http://127.0.0.1:' + (previewPort || server.httpServer.address().port);
const entry = origin + '/assets/workspace-v2/index.html';
const browser = await chromium.launch({ headless: true });
const errors = [], calls = [], saves = [];
const scopes = ['A', 'B', 'C', 'D', 'E', 'H', '110'].map(value => ({ value, label: value === '110' ? '110站' : value + '楼' }));
const modules = [
  ['maintenance', '维保通告', '通告办理'],
  ['change', '变更通告', '通告办理'],
  ['repair', '检修通告', '通告办理'],
  ['polling', '设备轮巡', '通告办理'],
  ['adjust', '设备调整', '通告办理'],
  ['power', '上下电通告', '通告办理'],
  ['event', '事件', '业务管理'],
  ['engineer_mop', '维保单办理', '业务管理'],
  ['repair_management', '维修项目', '业务管理'],
  ['cabinet', '机柜管理', '业务管理'],
  ['daily', '每日任务', '业务管理'],
  ['water', '水耗管理', '业务管理'],
  ['critical_guard', '重保管理', '业务管理'],
  ['risk_inspection', '专项排查', '业务管理'],
  ['drills', '演练管理', '业务管理'],
  ['learning', '画像学练', '业务管理'],
  ['plan_convergence', '计划收敛审查', '业务管理'],
  ['signatures', '签名管理', '协作与资料'],
  ['knowledge', '共享知识库', '协作与资料'],
  ['links', '多维表导航', '协作与资料'],
  ['life_guide', '参考人生指南', '协作与资料'],
  ['handover', '交接班审核', '协作与资料'],
  ['work_handover', '人员工作交接', '协作与资料'],
  ['assistant', '灯塔助手与飞书智能体', '协作与资料'],
  ['accounts', '账号与权限', '系统管理'],
  ['tool_management', '工具管理', '业务管理'],
  ['daily_inspection', '日常巡检', '业务管理'],
  ['cold_source', '冷源运行状态', '业务管理'],
  ['eop_scenarios', 'EOP 场景维护', '业务管理'],
  ['documents', '南通文件', '业务管理'],
  ['sops', 'SOP 步骤编辑', '业务管理'],
  ['pre_shift', '班前检查', '业务管理'],
  ['zhihang_data', '智航数据拉取', '系统管理', true],
  ['dingtalk_settings', '钉钉推送', '系统管理', true],
  ['handover_settings', '交接班链接配置', '系统管理', true],
  ['mop_settings', 'MOP 配置', '系统管理', true],
  ['feishu_settings', '飞书智能体配置', '系统管理', true],
  ['release_journal', '开发问题与更新记录', '系统管理', true],
  ['notice_history', '通告历史记忆', '系统管理', true],
  ['backend_status', '后台状态', '系统管理', true],
  ['backend_diagnostics', '高级诊断', '系统管理', true],
].map(([value, label, group, admin_only = false]) => ({ value, label, group, admin_only }));
const actorModules = modules.map(option => option.value).filter(id => id !== 'backend_diagnostics');
const l4Modules = modules.filter(option => actorModules.includes(option.value) && !option.admin_only && option.value !== 'accounts').map(option => option.value);
let user = { open_id: 'ou_fixture', name: '测试管理员', role: 'admin', level: 'L0', home_scope: 'A', category: 'manager',
  modules: actorModules, can_manage_accounts: true };
const target = { id: 'rec_fixture', record_id: 'rec_fixture', open_id: 'ou_target', name: '测试人员', employee_no: 'fixture',
  kind: 'external', own_scopes: ['A'], home_scope: 'A', scopes: ['A'], level: 'L4', role: 'building', position: '值班员',
  category: 'operator', modules: ['learning'], enabled: true, authorized: true, locked: false, blocked_reason: '',
  can_edit: true, can_edit_access: true, can_delete: true, can_set_password: true, revision: 'v1', password_set: true };
async function newPage() {
  const page = await browser.newPage({ serviceWorkers: 'block', viewport: { width: 1440, height: 1000 } });
  page.setDefaultTimeout(15000);
  page.on('pageerror', error => errors.push(error.message));
  await page.context().route('**/*', async route => {
    const req = route.request(), url = new URL(req.url());
    if (url.origin !== origin) return route.abort();
    if (!url.pathname.startsWith('/api/')) return req.method() === 'GET' ? route.continue() : route.abort();
    calls.push(url.pathname);
    if (req.method() !== 'GET' && !(req.method() === 'POST' && url.pathname === '/api/auth/accounts/permissions')) {
      throw new Error('Unexpected write ' + req.method() + ' ' + url.pathname);
    }
    const ok = data => route.fulfill({ json: { ok: true, data } });
    if (url.pathname === '/api/auth/status') return ok({ logged_in: true, user, scope_options: user.role === 'admin' ? scopes : scopes.slice(0, 1) });
    if (url.pathname === '/api/health') return route.fulfill({ json: { ok: true, service: 'clipflow_backend', instance_id: 'module-fixture' } });
    if (url.pathname === '/api/auth/permissions') return ok({ users: [], scope_options: scopes });
    if (url.pathname === '/api/auth/accounts') return ok({ items: [target], scope_options: scopes, module_options: modules,
      level_options: ['L0', 'L1', 'L2', 'L3', 'L4'].map(value => ({ value, label: value })),
      category_options: [{ value: 'operator', label: '值班员' }, { value: 'engineer', label: '工程师' }],
      policy: { level: 'L0', scope_rules: scopes.map(x => x.value), modules: actorModules, can_manage_accounts: true, can_view_accounts: true } });
    if (req.method() === 'POST' && url.pathname === '/api/auth/accounts/permissions') {
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
  await page.goto(entry + '?__page=/admin/settings&tab=permissions');
  await page.locator('.account-directory article').filter({ has: page.getByText(target.name, { exact: true }) })
    .getByRole('button', { name: '查看账号', exact: true }).click();
  const editor = page.getByRole('dialog', { name: target.name + ' 账号详情', exact: true });
  const level = editor.getByRole('combobox', { name: '权限等级', exact: true });
  const grants = editor.locator('.account-grants');
  const scopeGrants = editor.getByRole('group', { name: '授权楼栋', exact: true });
  const checked = group => group.locator('input:checked').evaluateAll(nodes => nodes.map(input => input.value).sort());
  assert.deepEqual(await checked(grants), ['learning'], 'opening preserves explicit grants');
  assert.equal(await grants.getByRole('checkbox').count(), modules.length, 'full module catalog is visible');
  await level.selectOption('L1');
  assert.deepEqual(await checked(scopeGrants), scopes.map(x => x.value).sort());
  assert.deepEqual(await checked(grants), [...actorModules].sort(), 'L1 receives exactly the modules held by the administrator');
  assert.equal(await grants.getByLabel('高级诊断', { exact: true }).isDisabled(), true);
  assert.equal(await grants.getByLabel('签名管理', { exact: true }).isEnabled(), true);
  await level.selectOption('L4');
  assert.deepEqual(await checked(grants), [...l4Modules].sort());
  for (const opt of modules.filter(x => x.admin_only || x.value === 'accounts')) {
    assert.equal(await grants.locator(`input[value="${opt.value}"]`).isDisabled(), true, opt.label + ' is unavailable to L4');
  }
  assert.deepEqual(await checked(scopeGrants), ['A']);
  await editor.getByLabel('B楼', { exact: true }).check();
  await editor.getByLabel('签名管理', { exact: true }).uncheck();
  await page.screenshot({ path: path.join(output, 'account-access.png'), fullPage: true });
  const box = await editor.boundingBox();
  const viewport = page.viewportSize();
  assert.ok(box.width >= 700 && Math.abs(box.x * 2 + box.width - viewport.width) <= 2 &&
    box.y >= 0 && box.y + box.height <= viewport.height + 1, 'permission editor is centered and fits the viewport');
  await editor.getByRole('button', { name: '保存授权', exact: true }).click();
  await page.getByText('已保存 1 个账号的权限', { exact: true }).first().waitFor();
  assert.equal(saves.length, 1);
  assert.deepEqual(saves[0].changes, [{ id: 'rec_fixture', revision: 'v1', level: 'L4', category: 'operator',
    role: 'building', enabled: true, scopes: ['A', 'B'], modules: l4Modules.filter(id => id !== 'signatures') }]);
  assert.ok(!calls.includes('/api/auth/permissions'), 'V2 never loads the unrestricted legacy directory');
  const beforeDeniedTab = calls.length;
  await page.goto(entry + '?__page=/admin/settings&tab=pressure');
  await page.getByText('没有该功能权限', { exact: true }).waitFor();
  assert.ok(!calls.slice(beforeDeniedTab).some(path => path.startsWith('/api/backend/')),
    'accounts grant alone never authorizes an ungranted diagnostics tab');
  await page.close();

  user = { open_id: 'ou_person', name: '当前测试人员', role: 'building', level: 'L4', modules: ['maintenance'],
    can_manage_accounts: false, login_method: 'password', personnel_record_id: 'rec_person' };
  const ordinary = await newPage();
  const start = calls.length;
  await ordinary.goto(entry + '?__page=/learning');
  await ordinary.getByText('没有该功能权限', { exact: true }).waitFor();
  assert.equal(calls.slice(start).some(x => x === '/api/learning' || x.startsWith('/api/learning/')), false);
  assert.equal(await ordinary.locator('[data-lighthouse-surface]').count(), 0);
  await ordinary.goto(entry + '?view=my');
  await ordinary.getByRole('button', { name: '修改密码', exact: true }).click();
  const change = ordinary.getByRole('form', { name: '修改账号密码', exact: true });
  await change.getByText(user.name, { exact: true }).waitFor();
  const peoplePicker = ordinary.locator('#personnel-name, #personnel-name-select, #personnel-reset-name');
  assert.equal(await peoplePicker.count(), 0);
  await change.getByRole('button', { name: '忘记密码', exact: true }).click();
  const reset = ordinary.getByRole('form', { name: '找回账号密码', exact: true });
  await reset.getByText(user.name, { exact: true }).waitFor();
  await reset.getByLabel('本人身份证号', { exact: true }).waitFor();
  assert.ok(await reset.getByLabel('新密码', { exact: true }).isDisabled());
  assert.equal(await peoplePicker.count(), 0);
  assert.equal(calls.slice(start).some(x => x === '/api/auth/password/people'), false);
  await ordinary.screenshot({ path: path.join(output, 'current-person-password.png'), fullPage: true });
  await ordinary.close();
  assert.deepEqual(errors, []);
  console.log('PASS: built UI level transitions, module grant payload, denied module, current-person password flow');
} finally {
  await browser.close();
  if (server) await server.close();
}
