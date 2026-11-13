import assert from 'node:assert/strict';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';
import { preview } from 'vite';

const root = fileURLToPath(new URL('..', import.meta.url));
const outDir = process.env.CLIPFLOW_ZHIHANG_PREVIEW_DIR || '../../../output/playwright/zhihang-data';
const server = await preview({ root, logLevel: 'error', build: { outDir }, preview: { host: '127.0.0.1', port: 0 } });
const origin = 'http://127.0.0.1:' + server.httpServer.address().port;
const entry = origin + '/assets/workspace-v2/index.html';
const base = '/api/zhihang-data';
const stamp = '2026-11-13T08:30:00+08:00';
const task = { kind: 'inspection', label: '巡检', times: ['07:00', '11:00'], enabled: false, confirmed: false,
  targets: [{ label: '原巡检表', url: 'https://vnet.feishu.cn/base/sample' }], notification_targets: ['原巡检群'],
  latest: null, next_at: null };
const auth = { has_token: false, expired: false };
const calls = [];
const exceptions = [];
const history = [];
const detailReads = new Map();
let login = { status: 'idle' };
let loginReads = 0;
let failTest = true;
let browser;

async function install(context, user) {
  await context.route('**/*', async route => {
    const request = route.request();
    const { origin: requestOrigin, pathname, searchParams } = new URL(request.url());
    if (requestOrigin !== origin) return route.abort();
    if (!pathname.startsWith('/api/')) return route.continue();
    const ok = data => route.fulfill({ json: { ok: true, data } });
    if (pathname === '/api/auth/status') return ok({ logged_in: true, user, scope_options: [{ value: 'A', label: 'A楼' }] });
    if (pathname === '/api/health') return route.fulfill({ json: { ok: true, service: 'clipflow_backend', instance_id: 'zhihang-fixture' } });
    assert(!pathname.startsWith('/api/plan-convergence/'), 'no dependence on the plan convergence grant');
    if (!pathname.startsWith(base)) return ok({});
    const path = pathname.slice(base.length);
    const method = request.method();
    calls.push([user.open_id, method, path]);
    const payload = method === 'GET' ? undefined : request.postDataJSON();
    if (path === '') return ok({ auth, tasks: [task], history });
    if (path === '/login' && method === 'GET') {
      if (login.status === 'waiting' && ++loginReads === 2) {
        login = { ...login, status: 'success', message: '智航浏览器登录成功' };
        Object.assign(auth, { has_token: true, expires_at: 1794600000 });
      }
      return ok(login);
    }
    if (path === '/login' && method === 'POST') {
      loginReads = 0;
      login = { job_id: 'shared-job', status: 'waiting', message: '等待主机智航登录' };
      return ok(login);
    }
    if (path === '/login/cancel') {
      assert.deepEqual(payload, { job_id: 'shared-job' });
      login = { ...login, status: 'cancelled', message: '已取消登录' };
      return ok(login);
    }
    if (path === '/test') {
      if (failTest) {
        failTest = false;
        return route.fulfill({ status: 400, json: { ok: false, error: '智航连接失败，请核对 VPN' } });
      }
      return ok({ connected: true, message: '智航连接正常' });
    }
    if (path === '/inspection/settings') {
      assert.equal(method, 'PUT');
      assert.deepEqual(payload, { times: ['07:00', '11:00'], enabled: true, confirmed: true, notify: true });
      Object.assign(task, payload);
      return ok(payload);
    }
    if (path === '/inspection/preview' || path === '/inspection/sync') {
      const previewMode = path.endsWith('/preview');
      assert.deepEqual(payload, previewMode ? {} : { preview_id: 'fixture-preview' });
      if (previewMode) assert.equal(task.confirmed, false);
      else assert.equal(task.confirmed, true);
      const item = { id: previewMode ? 'fixture-preview' : 'fixture-sync', kind: task.kind,
        mode: previewMode ? 'preview' : 'sync', status: 'queued', created_at: stamp };
      task.latest = item;
      history.unshift(item);
      return ok(item);
    }
    if (path.startsWith('/runs/')) {
      const id = path.slice('/runs/'.length);
      const item = history.find(row => row.id === id);
      assert(item);
      const reads = (detailReads.get(id) || 0) + 1;
      detailReads.set(id, reads);
      if (id === 'fixture-sync' && reads === 1) Object.assign(item, { status: 'pending_verify', error: '回读核验中' });
      else if (reads >= 2) {
        Object.assign(item, { status: 'success', finished_at: stamp, error: '' });
        if (item.mode === 'preview') task.last_preview = item;
        else {
          task.last_sync = item;
          item.notification = { status: 'sent', targets: ['原巡检群'] };
        }
      }
      const page = Number(searchParams.get('page'));
      return ok({ ...item, counts: { create: 2, update: 1, delete: 0 },
        rows: [{ 楼栋: 'D楼', 巡检项目: '隔离巡检数据 ' + page }], total: 51, page, page_size: 50 });
    }
    throw new Error('Unexpected API call: ' + method + ' ' + path);
  });
}

try {
  browser = await chromium.launch();
  const admin = { open_id: 'admin', name: '管理员', role: 'admin', level: 'L0', modules: ['accounts'] };
  const context = await browser.newContext({ viewport: { width: 1440, height: 900 }, reducedMotion: 'reduce' });
  await install(context, admin);
  const page = await context.newPage();
  page.on('pageerror', error => exceptions.push(error.message));
  page.setDefaultTimeout(12000);
  await page.goto(entry);
  const sidebar = page.locator('.v2-sidebar');
  await sidebar.hover();
  await sidebar.getByRole('button', { name: '系统管理', exact: true }).click();
  await sidebar.getByRole('button', { name: '系统运维', exact: true }).click();
  await sidebar.getByRole('button', { name: '智航数据拉取', exact: true }).click();
  await page.getByRole('heading', { name: '智航数据拉取', exact: true }).waitFor();
  assert.equal(new URL(page.url()).searchParams.get('__page'), '/zhihang-data');
  assert.equal(new URL(page.url()).searchParams.has('scope'), false);
  await page.getByRole('link', { name: '原巡检表' }).waitFor();
  await page.getByRole('button', { name: '刷新', exact: true }).hover();
  await page.getByRole('button', { name: '拉取预览', exact: true }).click();
  await page.getByText('隔离巡检数据 1', { exact: true }).waitFor();
  const runDialog = page.getByRole('dialog');
  await runDialog.getByText('成功', { exact: true }).waitFor();
  assert(await runDialog.getByRole('button', { name: '同步原表', exact: true }).isDisabled());
  await runDialog.getByRole('button', { name: '下一页', exact: true }).click();
  await runDialog.getByText('隔离巡检数据 2', { exact: true }).waitFor();
  await page.keyboard.press('Escape');
  await runDialog.waitFor({ state: 'hidden' });
  await page.getByRole('button', { name: '定时设置' }).click();
  assert.equal(await runDialog.getByRole('checkbox', { name: '同步成功后发送原有卡片通知' }).getAttribute('aria-checked'), 'false');
  await runDialog.getByLabel('运行时间（北京时间）').fill('25:01');
  await runDialog.getByRole('button', { name: '保存设置' }).click();
  await runDialog.getByRole('alert').getByText('请输入 1 至 24 个运行时间，格式为 HH:mm。').waitFor();
  await runDialog.getByLabel('运行时间（北京时间）').fill('11:00、07:00、11:00');
  for (const name of ['确认旧工具已停止写入，由灯塔同步原表', '启用定时同步', '同步成功后发送原有卡片通知']) {
    await runDialog.getByRole('checkbox', { name }).click();
  }
  await runDialog.getByRole('button', { name: '保存设置' }).click();
  await runDialog.waitFor({ state: 'hidden' });
  await page.getByRole('button', { name: '查看预览', exact: true }).click();
  await runDialog.getByRole('button', { name: '同步原表', exact: true }).click();
  await page.getByRole('button', { name: '取消', exact: true }).click();
  assert.equal(calls.filter(([, , path]) => path === '/inspection/sync').length, 0);
  await runDialog.getByRole('button', { name: '同步原表', exact: true }).click();
  await page.getByRole('button', { name: '确认同步', exact: true }).click();
  await runDialog.getByText('待核验', { exact: true }).waitFor();
  await runDialog.getByText('已发送', { exact: true }).waitFor();
  await page.keyboard.press('Escape');
  await runDialog.waitFor({ state: 'hidden' });
  await page.getByRole('button', { name: '登录智航' }).click();
  await page.getByRole('button', { name: '取消登录' }).click();
  await page.getByText('已取消登录', { exact: true }).waitFor();
  await page.getByRole('button', { name: '登录智航' }).click();
  await page.getByText('智航已登录', { exact: true }).waitFor();
  await page.getByRole('button', { name: '测试连接' }).click();
  await page.getByText('智航连接失败，请核对 VPN', { exact: true }).waitFor();
  await page.getByRole('button', { name: '测试连接' }).click();
  await page.getByText('智航连接正常', { exact: true }).waitFor();
  await page.setViewportSize({ width: 390, height: 844 });
  await page.getByRole('button', { name: '功能', exact: true }).click();
  await page.getByRole('button', { name: '智航数据拉取', exact: true }).click();
  await page.getByRole('heading', { name: '智航数据拉取', exact: true }).waitFor();
  assert(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1));
  assert.deepEqual(exceptions, []);
  await context.close();

  const deniedUsers = [
    { ...admin, open_id: 'member', role: 'member', level: 'L4' },
    { ...admin, open_id: 'no-grant', modules: ['plan_convergence'] },
    { ...admin, open_id: 'guest', role: 'guest' },
    { ...admin, open_id: 'impersonated', impersonation: { id: 'temporary', active: true, expires_at: Date.now() + 600000 } },
  ];
  for (const user of deniedUsers) {
    const deniedContext = await browser.newContext();
    await install(deniedContext, user);
    const denied = await deniedContext.newPage();
    denied.setDefaultTimeout(12000);
    await denied.goto(entry + '?__page=%2Fzhihang-data');
    if (user.role === 'guest') await denied.waitForURL(url => url.searchParams.get('__page') === '/cabinet-power');
    else await denied.getByText('没有该功能权限', { exact: true }).waitFor();
    assert.equal(await denied.locator('.v2-sidebar').getByRole('button', { name: '智航数据拉取', exact: true }).count(), 0);
    assert.equal(calls.filter(([id]) => id === user.open_id).length, 0);
    await deniedContext.close();
  }
  console.log('PASS: Zhihang navigation, admin grant, preview before cutover, settings, synchronization and notifications, shared login and mobile.');
} finally {
  await browser?.close();
  await new Promise(resolve => server.httpServer.close(resolve));
}
