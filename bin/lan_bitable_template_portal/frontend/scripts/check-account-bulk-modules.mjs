// Real AdminAccounts SFC, legacy + V2 dev transforms, synthetic local API only.
import assert from 'node:assert/strict';
import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';
import vue from '@vitejs/plugin-vue';
import { createServer } from 'vite';

const root = fileURLToPath(new URL('..', import.meta.url));
const output = path.resolve(root, '../../../output/playwright/account-bulk-modules');
await mkdir(output, { recursive: true });
const modules = [
  { value: 'event', label: '事件管理', group: '业务管理' },
  { value: 'water', label: '水耗管理', group: '业务管理' },
  { value: 'engineer_mop', label: '维护单管理', group: '业务管理' },
  { value: 'repair_management', label: '检修管理（维修单）', group: '业务管理' },
  { value: 'cabinet', label: '机柜管理', group: '业务管理' },
  { value: 'daily', label: '每日任务', group: '业务管理' },
  { value: 'critical_guard', label: '重保管理', group: '业务管理' },
  { value: 'risk_inspection', label: '风险排查', group: '业务管理' },
  { value: 'drills', label: '演练管理', group: '业务管理' },
  { value: 'learning', label: '画像学练', group: '业务管理' },
  { value: 'plan_convergence', label: '计划收敛审查', group: '业务管理' },
  { value: 'maintenance', label: '维保通告', group: '通告办理' },
  { value: 'change', label: '变更通告', group: '通告办理' },
  { value: 'repair', label: '检修通告', group: '通告办理' },
  { value: 'polling', label: '设备轮巡', group: '通告办理' },
  { value: 'adjust', label: '设备调整', group: '通告办理' },
  { value: 'power', label: '上下电通告', group: '通告办理' },
  { value: 'knowledge', label: '共享知识库', group: '协作与资料' },
  { value: 'links', label: '多维表导航', group: '协作与资料' },
  { value: 'signatures', label: '签名管理', group: '协作与资料' },
  { value: 'life_guide', label: '参考人生指南', group: '协作与资料' },
  { value: 'handover', label: '交接班审核', group: '协作与资料' },
  { value: 'work_handover', label: '人员工作交接', group: '协作与资料' },
  { value: 'assistant', label: '灯塔助手与飞书智能体', group: '协作与资料' },
  { value: 'accounts', label: '账号与设置', group: '系统管理' },
];
const scopeOptions = ['A', 'B'].map(value => ({ value, label: value + '楼' }));
function fixture(count = 24) {
  const people = Array.from({ length: count }, (_, index) => ({
    id: 'rec' + index, record_id: 'rec' + index, open_id: 'ou_bulk_' + index,
    name: '隔离人员' + index, employee_no: 'TEST-' + index, kind: 'external', account_nature: '外部账号',
    position: '值班员', category: 'operator', own_scopes: [index % 2 ? 'B' : 'A'],
    scopes: [index % 2 ? 'B' : 'A'], role: 'building', enabled: true, authorized: true,
    locked: false, blocked_reason: '', can_set_password: false, password_set: true,
    revision: 'v' + index, level: 'L4', can_edit: true, can_edit_access: true,
    modules: index % 2 ? ['maintenance', 'knowledge'] : ['event', 'links'], can_delete: false,
  }));
  return { people, policy: { level: 'L0', modules: modules.map(module => module.value), scope_rules: ['A', 'B'], can_manage_accounts: true },
    posts: [], fail: '', failStatus: 409, readFail: false, changedCount: null, delay: false, reads: 0 };
}
const evidence = [];
const pageErrors = [];
let state;
function harness(v2) {
  return {
    name: 'isolated-account-bulk-fixture',
    resolveId(id) { if (id === 'virtual:account-bulk-fixture') return '\0account-bulk-fixture'; },
    load(id) {
      if (id !== '\0account-bulk-fixture') return;
      return `
import { createApp, h } from 'vue';
import { NConfigProvider, NDialogProvider, NMessageProvider, darkTheme } from 'naive-ui';
import Accounts from '/src/components/AdminAccounts.vue';
import LoadingIndicator from '/src/components/LoadingIndicator.vue';
import UiTransition from '/src/components/UiTransition.vue';
import '/src/global.css';
${v2 ? ["workspace", "interactions", "account", "accountPresentation", "pageFilters", "pageLayout", "feedback", "night"].map(name => `import '/src/workspace-v2/${name}.css';`).join('\n') : ''}
const dark = new URLSearchParams(location.search).has('dark');
${v2 ? "document.documentElement.classList.add('workspace-v2'); document.documentElement.dataset.v2Theme = dark ? 'dark' : 'light';" : ''}
const app = createApp({ render: () => h(NConfigProvider, {theme: dark ? darkTheme : null}, {default: () => h(NDialogProvider, null, {default: () => h(NMessageProvider, null, {default: () => h(Accounts, {pendingCount: 0, permissions: []})})})}) });
app.component('LoadingIndicator', LoadingIndicator).component('UiTransition', UiTransition).mount('#app');
`;
    },
    configureServer(server) {
      server.middlewares.use(async (req, res, next) => {
        const url = new URL(req.url, 'http://fixture');
        if (url.pathname === '/__account-bulk-fixture') {
          const html = await server.transformIndexHtml(req.url, '<!doctype html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>隔离账号模块测试</title></head><body><main class="v2-main" style="max-width:1360px;margin:auto;padding:16px;min-width:0"><div id="app"></div></main><script type="module" src="/@id/virtual:account-bulk-fixture"></script></body></html>');
          res.setHeader('Content-Type', 'text/html; charset=utf-8'); res.end(html); return;
        }
        if (!url.pathname.startsWith('/api/')) return next();
        const send = (data, status = 200) => { res.statusCode = status; res.setHeader('Content-Type', 'application/json'); res.end(JSON.stringify(status === 200 ? { ok: true, data } : { ok: false, error: data })); };
        if (url.pathname === '/api/auth/accounts' && req.method === 'GET') {
          state.reads++;
          if (state.readFail) return send('隔离目录读取失败', 503);
          return send({ items: state.people, policy: state.policy, scope_options: scopeOptions,
            category_options: [{ value: 'operator', label: '值班员' }], module_options: modules,
            level_options: ['L1', 'L2', 'L3', 'L4'].map(value => ({ value, label: value })), loaded_at: 1700000000 });
        }
        if (url.pathname === '/api/auth/accounts/permissions' && req.method === 'POST') {
          const chunks = []; for await (const chunk of req) chunks.push(chunk);
          const payload = JSON.parse(Buffer.concat(chunks).toString());
          state.posts.push(payload);
          if (state.delay) await new Promise(resolve => setTimeout(resolve, 150));
          if (state.fail) return send(state.fail, state.failStatus);
          // Verify every member before applying any member, like the real backend.
          if (payload.changes.some(change => state.people.find(item => item.id === change.id)?.revision !== change.revision)) return send('该账号权限已变化，请刷新后重新确认。', 409);
          if (state.changedCount !== null) return send({ changed: state.changedCount });
          for (const change of payload.changes) {
            const item = state.people.find(item => item.id === change.id);
            item.modules = [...change.modules]; item.revision += '-saved';
          }
          return send({ changed: payload.changes.length });
        }
        return send({ items: [] });
      });
    },
  };
}
const browser = await chromium.launch({ headless: true });
let server;
async function select(page, name) { await page.getByRole('checkbox', { name: '选择' + name, exact: true }).check(); }
async function openBulk(page) { await page.getByRole('button', { name: '批量设置模块', exact: true }).click(); }
const modal = page => page.getByRole('dialog', { name: '批量设置模块', exact: true });
async function preview(page) { await modal(page).getByRole('button', { name: '预览变更' }).click(); }
async function refresh(page) {
  const reads = state.reads;
  await page.getByRole('button', { name: '同步人员', exact: true }).click();
  await page.waitForFunction(() => !document.querySelector('.account-header button:disabled'));
  assert.ok(state.reads > reads);
}
function assertPayload(payload, before, operation, chosen) {
  assert.deepEqual(Object.keys(payload), ['changes']);
  for (const change of payload.changes) {
    assert.deepEqual(Object.keys(change).sort(), ['enabled', 'id', 'modules', 'revision']);
    const item = before.find(item => item.id === change.id);
    assert.equal(change.enabled, item.enabled); assert.equal(change.revision, item.revision);
    const expected = operation === 'add' ? [...new Set([...item.modules, ...chosen])] : item.modules.filter(module => !chosen.includes(module));
    assert.deepEqual(change.modules, expected);
  }
}
async function visualCheck(page, label) {
  const geometry = await page.evaluate(() => {
    const dialog = document.querySelector('.account-bulk-editor');
    const footer = dialog.querySelector('.editor-footer').getBoundingClientRect();
    const header = dialog.querySelector('header').getBoundingClientRect();
    const body = dialog.querySelector('.account-bulk-body').getBoundingClientRect();
    const bounds = dialog.getBoundingClientRect();
    const out = [...dialog.querySelectorAll('button, .module-options label, li')].filter(el => {
      const box = el.getBoundingClientRect(); return box.left < bounds.left - 1 || box.right > bounds.right + 1 || el.scrollWidth > el.clientWidth + 2;
    }).map(el => el.textContent);
    const labels = [...dialog.querySelectorAll('.module-options label')].map(el => el.getBoundingClientRect());
    const overlaps = labels.some((a, i) => labels.some((b, j) => i < j && Math.min(a.right, b.right) - Math.max(a.left, b.left) > 1 && Math.min(a.bottom, b.bottom) - Math.max(a.top, b.top) > 1));
    return { width: innerWidth, height: innerHeight, scrollWidth: document.documentElement.scrollWidth, out,
      overlaps, contained: bounds.top >= 0 && bounds.bottom <= innerHeight + 1,
      separated: header.bottom <= body.top + 1 && body.bottom <= footer.top + 1,
      color: getComputedStyle(dialog).color, background: getComputedStyle(dialog).backgroundColor };
  });
  assert.equal(geometry.scrollWidth <= geometry.width, true, label + ': no page overflow');
  assert.deepEqual(geometry.out, [], label + ': no clipped text/controls');
  assert.equal(geometry.overlaps, false, label + ': no overlapping module options');
  assert.equal(geometry.contained && geometry.separated, true, label + ': header/body/footer stay separate');
  await page.screenshot({ path: path.join(output, label + '.png') });
  evidence.push({ label, url: page.url(), capturedAt: new Date().toISOString(), ...geometry });
}
try {
  for (const v2 of [false, true]) {
    state = fixture();
    server = await createServer({ root, configFile: v2 ? path.join(root, 'vite.workspace-v2.config.ts') : false,
      plugins: [...(v2 ? [] : [vue()]), harness(v2)], optimizeDeps: { noDiscovery: true, include: ['vue', 'naive-ui', 'lucide-vue-next'] },
      logLevel: 'error', server: { host: '127.0.0.1', port: 0 } });
    await server.listen();
    const origin = `http://127.0.0.1:${server.httpServer.address().port}`;
    const context = await browser.newContext({ viewport: { width: 1440, height: 1000 }, reducedMotion: 'reduce' });
    await context.route('**/*', route => new URL(route.request().url()).origin === origin ? route.continue() : route.abort());
    const page = await context.newPage();
    page.on('pageerror', error => pageErrors.push(error.message));
    const goto = async (query = '') => { await page.goto(origin + '/__account-bulk-fixture' + query); await page.getByRole('checkbox', { name: '选择隔离人员0', exact: true }).waitFor(); };
    await goto();
    // Explicit backend flags and fail-closed selection for self/locked/blocked/disabled.
    Object.assign(state.people[1], { is_self: true });
    Object.assign(state.people[2], { locked: true });
    Object.assign(state.people[3], { blocked_reason: '隔离待核对' });
    Object.assign(state.people[4], { can_edit_access: false });
    Object.assign(state.people[5], { enabled: false, modules: [] });
    await refresh(page);
    for (let index = 1; index <= 5; index++) assert.equal(await page.getByRole('checkbox', { name: '选择隔离人员' + index, exact: true }).isDisabled(), true);
    await page.getByRole('checkbox', { name: '选择本页可管理人员' }).check();
    assert.match(await page.locator('.account-bulk-bar').innerText(), /已选 15 \/ 300 人/);
    await page.getByRole('button', { name: '下一页', exact: true }).click();
    await select(page, '隔离人员20');
    assert.match(await page.locator('.account-bulk-bar').innerText(), /已选 16/);
    await page.getByRole('button', { name: '上一页', exact: true }).click();
    assert.equal(await page.getByRole('checkbox', { name: '选择隔离人员0', exact: true }).isChecked(), true);
    await page.getByRole('button', { name: '清空选择' }).click();
    await select(page, '隔离人员0');
    await page.getByRole('button', { name: '下一页', exact: true }).click();
    await select(page, '隔离人员21');
    await page.getByRole('searchbox', { name: '搜索账号' }).fill('没有此人');
    assert.match(await page.locator('.account-bulk-bar').innerText(), /已选 2/);
    await openBulk(page);
    assert.equal(await modal(page).getByRole('radio', { name: '添加模块', exact: true }).isChecked(), true);
    for (const group of ['业务管理', '通告办理', '协作与资料', '系统管理']) assert.equal(await modal(page).getByRole('group', { name: new RegExp(group) }).count(), 1);
    assert.equal(await modal(page).getByRole('checkbox', { name: '账号与设置', exact: true }).isDisabled(), true);
    const businessGroup = modal(page).getByRole('checkbox', { name: '选择业务管理全部模块', exact: true });
    const systemGroup = modal(page).getByRole('checkbox', { name: '选择系统管理全部模块', exact: true });
    await businessGroup.check();
    for (const option of modules.filter(option => option.group === '业务管理')) assert.equal(await modal(page).getByRole('checkbox', { name: option.label, exact: true }).isChecked(), true);
    await modal(page).getByRole('checkbox', { name: '水耗管理', exact: true }).uncheck();
    assert.equal(await businessGroup.evaluate(input => input.indeterminate), true, 'group tracks partial module selection');
    await modal(page).getByRole('checkbox', { name: '共享知识库', exact: true }).check();
    await businessGroup.check(); await businessGroup.uncheck();
    assert.equal(await businessGroup.evaluate(input => input.indeterminate), false);
    assert.equal(await modal(page).getByRole('checkbox', { name: '共享知识库', exact: true }).isChecked(), true, 'clearing one group keeps other groups');
    await modal(page).getByRole('checkbox', { name: '共享知识库', exact: true }).uncheck();
    assert.equal(await systemGroup.isDisabled(), true, 'L4 cannot select accounts through its group');
    await modal(page).getByRole('radio', { name: '移除模块', exact: true }).check(); await systemGroup.check();
    assert.equal(await modal(page).getByRole('checkbox', { name: '账号与设置', exact: true }).isChecked(), true);
    await modal(page).getByRole('radio', { name: '添加模块', exact: true }).check();
    assert.equal(await modal(page).getByRole('checkbox', { name: '账号与设置', exact: true }).isChecked(), false, 'switching to add prunes forbidden accounts');
    assert.equal(await systemGroup.isDisabled(), true);
    await modal(page).getByRole('checkbox', { name: '水耗管理' }).check();
    await modal(page).getByRole('checkbox', { name: '变更通告', exact: true }).check();
    await preview(page);
    const beforeAdd = structuredClone(state.people);
    assert.deepEqual(await modal(page).locator('li>span:first-child').allTextContents().then(names => names.map(name => name.split('TEST-')[0])), ['隔离人员0', '隔离人员21']);
    assert.equal(state.posts.length, 0, 'preview sends no request');
    for (const size of [{ width: 1440, height: 1000 }, { width: 402, height: 874 }, { width: 440, height: 956 }]) {
      await page.setViewportSize(size); await visualCheck(page, `${v2 ? 'v2' : 'legacy'}-${size.width}x${size.height}`);
    }
    // Focus is trapped and Escape closes without discarding people selection.
    await modal(page).getByRole('button', { name: '关闭批量模块设置' }).focus();
    await page.keyboard.press('Shift+Tab');
    assert.match(await page.evaluate(() => document.activeElement.textContent), /确认提交/);
    state.delay = true;
    await modal(page).getByRole('button', { name: '确认提交 2 人' }).click();
    assert.equal(await modal(page).getByRole('button', { name: '正在提交' }).isDisabled(), true);
    await modal(page).waitFor({ state: 'hidden' }); state.delay = false;
    assert.equal(state.posts.length, 1); assertPayload(state.posts[0], beforeAdd, 'add', ['water', 'change']);
    assert.match(await page.locator('.success').innerText(), /已为 2 人添加/);
    assert.match(await page.locator('.account-bulk-bar').innerText(), /已选 2/);
    await openBulk(page);
    await modal(page).getByRole('radio', { name: '移除模块' }).check();
    await modal(page).getByRole('checkbox', { name: '变更通告', exact: true }).check();
    await preview(page);
    const beforeRemove = structuredClone(state.people);
    await modal(page).getByRole('button', { name: '确认提交 2 人' }).click();
    await modal(page).waitFor({ state: 'hidden' });
    assert.equal(state.posts.length, 2); assertPayload(state.posts[1], beforeRemove, 'remove', ['change']);
    evidence.push({ label: v2 ? 'v2-payloads' : 'legacy-payloads', add: state.posts[0], remove: state.posts[1] });
    // A stale second member must keep both selections and all module choices.
    await openBulk(page); await modal(page).getByRole('checkbox', { name: '变更通告', exact: true }).check(); await preview(page);
    state.people[21].revision += '-concurrent';
    const staleBefore = structuredClone(state.people);
    await modal(page).getByRole('button', { name: '确认提交 2 人' }).click();
    await modal(page).getByRole('button', { name: '刷新账号' }).waitFor();
    assert.deepEqual(state.people, staleBefore, 'a failed batch changes nobody');
    assert.equal(await modal(page).getByRole('checkbox', { name: '变更通告', exact: true }).isChecked(), true);
    assert.match(await modal(page).getByRole('alert').innerText(), /版本冲突.*刷新/);
    assert.equal(await page.locator('.success').count(), 0);
    state.readFail = true;
    await modal(page).getByRole('button', { name: '刷新账号' }).click();
    await page.waitForFunction(() => document.querySelector('.accounts>.error')?.textContent.includes('隔离目录读取失败'));
    assert.equal(await modal(page).getByRole('button', { name: '预览变更' }).isDisabled(), true);
    assert.equal(await modal(page).locator('li').count(), 2);
    state.readFail = false;
    await modal(page).getByRole('button', { name: '刷新账号' }).click();
    await modal(page).getByRole('button', { name: '预览变更' }).waitFor({ state: 'visible' });
    await page.waitForFunction(() => !document.querySelector('.account-bulk-editor button.primary:disabled'));
    await preview(page);
    await modal(page).getByRole('button', { name: '确认提交 2 人' }).click(); await modal(page).waitFor({ state: 'hidden' });
    assert.equal(state.posts.at(-1).changes.find(item => item.id === 'rec21').revision, staleBefore[21].revision);
    // Authority revoked after preview: a rejection keeps everyone and reports no success.
    await openBulk(page); await modal(page).getByRole('checkbox', { name: '事件管理', exact: true }).check(); await preview(page);
    state.fail = '当前账号无权管理此人员，未修改任何数据。'; state.failStatus = 403;
    const deniedBefore = structuredClone(state.people);
    await modal(page).getByRole('button', { name: '确认提交 2 人' }).click();
    await modal(page).getByRole('alert').waitFor();
    assert.deepEqual(state.people, deniedBefore);
    assert.equal(await modal(page).locator('li').count(), 2);
    assert.equal(await modal(page).getByRole('checkbox', { name: '事件管理', exact: true }).isChecked(), true);
    assert.equal(await page.locator('.success').count(), 0);
    await modal(page).getByRole('button', { name: '取消', exact: true }).click(); state.fail = ''; state.failStatus = 409;
    // A response that confirms fewer people must never show success.
    state.changedCount = 1;
    await openBulk(page); await modal(page).getByRole('checkbox', { name: '共享知识库' }).check(); await preview(page);
    await modal(page).getByRole('button', { name: '确认提交 2 人' }).click();
    await modal(page).getByRole('alert').waitFor();
    assert.equal(await page.locator('.success').count(), 0);
    assert.match(await modal(page).getByRole('alert').innerText(), /未确认.*成功/);
    await modal(page).getByRole('button', { name: '取消', exact: true }).click(); state.changedCount = null;
    // Refresh retains valid IDs and explicitly names removed people.
    await page.getByRole('searchbox', { name: '搜索账号' }).fill('');
    state.people[21].can_edit_access = false; await refresh(page);
    assert.match(await page.locator('.account-bulk-bar').innerText(), /已选 1/);
    assert.match(await page.locator('.accounts>.warning').innerText(), /已移除.*隔离人员21/);
    await openBulk(page);
    await modal(page).getByRole('checkbox', { name: '水耗管理' }).check(); await preview(page);
    assert.match(await modal(page).getByRole('alert').innerText(), /无需提交/);
    await page.keyboard.press('Escape'); await modal(page).waitFor({ state: 'hidden' });
    // Never strip modules outside the actor's grant; identify the blocked person.
    state.policy.modules = ['maintenance', 'water', 'accounts']; await refresh(page);
    await openBulk(page);
    assert.equal(await modal(page).getByRole('checkbox', { name: '共享知识库' }).count(), 0);
    await modal(page).getByRole('checkbox', { name: '维保通告', exact: true }).check();
    const postCount = state.posts.length; await preview(page);
    assert.match(await modal(page).getByRole('alert').innerText(), /隔离人员0.*超出.*上级/);
    assert.equal(state.posts.length, postCount);
    await page.keyboard.press('Escape');
    state.people[0].open_id = 'ou_changed_identity'; await refresh(page);
    assert.match(await page.locator('.accounts>.warning').innerText(), /身份变化.*隔离人员0/);
    assert.match(await page.locator('.account-bulk-bar').innerText(), /已选 0/);
    // L2 keeps base editing only, even though backend can_edit remains true.
    state.policy.level = 'L2'; state.people.forEach(item => { item.can_edit_access = false; }); await refresh(page);
    assert.equal(await page.getByRole('button', { name: '批量设置模块', exact: true }).count(), 0);
    assert.equal(await page.getByRole('checkbox', { name: '选择隔离人员0', exact: true }).isDisabled(), true);
    // Capacity and single-person management-module grant for a legal L3 target.
    state = fixture(302); await goto();
    for (let index = 0; index < 15; index++) {
      await page.getByRole('checkbox', { name: '选择本页可管理人员' }).check();
      if (index < 14) await page.getByRole('button', { name: '下一页', exact: true }).click();
    }
    assert.match(await page.locator('.account-bulk-bar').innerText(), /已选 300/);
    await page.getByRole('button', { name: '下一页', exact: true }).click();
    assert.equal(await page.getByRole('checkbox', { name: '选择隔离人员300', exact: true }).isDisabled(), true);
    await page.getByRole('checkbox', { name: '选择本页可管理人员' }).check();
    assert.match(await page.locator('.accounts>.warning').innerText(), /最多.*300/);
    await page.getByRole('button', { name: '清空选择' }).click();
    state.people[300].level = 'L3'; await refresh(page);
    await select(page, '隔离人员300'); await openBulk(page);
    await modal(page).getByRole('checkbox', { name: '选择系统管理全部模块', exact: true }).check(); await preview(page);
    await modal(page).getByRole('button', { name: '确认提交 1 人' }).click(); await modal(page).waitFor({ state: 'hidden' });
    assert.deepEqual(state.posts[0].changes[0].modules, ['event', 'links', 'accounts']);
    if (v2) {
      state = fixture(); await goto('?dark'); await select(page, '隔离人员0'); await openBulk(page);
      await modal(page).getByRole('checkbox', { name: '水耗管理' }).check(); await preview(page);
      for (const size of [{ width: 1440, height: 1000 }, { width: 402, height: 874 }, { width: 440, height: 956 }]) {
        await page.setViewportSize(size); await visualCheck(page, `v2-dark-${size.width}x${size.height}`);
      }
    }
    console.log(`PASS: ${v2 ? 'V2' : 'legacy'} single/multi-person, group all/partial selection, cross-page retention, add/remove, preview, revision retry, partial-response protection, refresh pruning, actor limits, L4/L2, 300-person limit, responsive modal`);
    await context.close(); await server.close(); server = null;
  }
  assert.deepEqual(pageErrors, [], 'no browser runtime errors');
  await writeFile(path.join(output, 'evidence.json'), JSON.stringify(evidence, null, 2));
  console.log('PASS: isolated account bulk module checks; evidence: ' + output);
} finally { await browser.close(); if (server) await server.close(); }
