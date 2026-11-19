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
const inspectionCardSetting = '参与日报并发送独立巡检异常提醒';
const calls = [];
const exceptions = [];
const history = [{ id: 'fixture-failed', kind: 'inspection', mode: 'preview', status: 'failed',
  created_at: stamp, error: '智航 inspection_list：分页返回重复记录，无法确认是否漏页。' }];
for (let index = 1; index <= 11; index++) {
  history.push({ id: 'history-' + index, kind: 'risk', mode: 'scheduled', status: 'success',
    created_at: `2026-11-12T08:${String(59-index).padStart(2, '0')}:00+08:00` });
}
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
    if (path === '') {
      const pageSize = 10;
      const page = Math.min(Number(searchParams.get('history_page') || 1), Math.max(1, Math.ceil(history.length / pageSize)));
      return ok({ auth, tasks: [task], history: history.slice((page - 1) * pageSize, page * pageSize),
        notification_digests: [
          { kind: 'daily', label: '智航数据同步', times: ['08:00'], prepare_at: '00:10',
            modules: ['inspection', 'change', 'event'], enabled_modules: task.notify ? ['inspection'] : [],
            target: { label: '汇总通知群' }, latest: {} },
          { kind: 'month_end', label: '风险排查与演练推进同步', times: ['09:00', '15:00'], start_day: 25,
            modules: ['risk', 'drill'], enabled_modules: [], target: { label: '汇总通知群' }, latest: {} },
        ],
        history_pagination: { page, page_size: pageSize, total: history.length } });
    }
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
      if (id === 'fixture-failed') return ok({
        ...item, stage: '巡检换页重拉', checks: { 巡检换页重拉: { 每页: 50, 源端总数: 120, 已读取: 50, 当前页: 2 } },
        rows: [], total: 0, page: 1, page_size: 50,
      });
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
  const historyPager = page.getByRole('navigation', { name: '执行记录分页' });
  await historyPager.getByText('共 12 条 · 第 1 / 2 页').waitFor();
  assert(await historyPager.getByRole('button', { name: '上一页', exact: true }).isDisabled());
  assert.equal(await page.locator('tbody tr').count(), 10);
  await historyPager.getByRole('button', { name: '下一页', exact: true }).click();
  await historyPager.getByText('共 12 条 · 第 2 / 2 页').waitFor();
  assert.equal(await page.locator('tbody tr').count(), 2);
  const pageTwoRefresh = page.waitForResponse(response => new URL(response.url()).searchParams.get('history_page') === '2');
  await page.getByRole('button', { name: '刷新', exact: true }).click();
  await pageTwoRefresh;
  assert(await historyPager.getByRole('button', { name: '下一页', exact: true }).isDisabled());
  await historyPager.getByRole('button', { name: '上一页', exact: true }).click();
  await historyPager.getByText('共 12 条 · 第 1 / 2 页').waitFor();
  await page.locator('.run-failure').getByText(history[0].error, { exact: true }).waitFor();
  await page.getByRole('button', { name: '查看巡检执行详情' }).click();
  const runDialog = page.getByRole('dialog');
  await runDialog.getByText('失败阶段', { exact: true }).waitFor();
  await runDialog.getByText('每页 50 · 源端总数 120 · 已读取 50 · 当前页 2', { exact: true }).waitFor();
  assert.equal(await runDialog.getByRole('button', { name: '同步原表', exact: true }).count(), 0);
  await page.keyboard.press('Escape');
  await runDialog.waitFor({ state: 'hidden' });
  await page.getByRole('button', { name: '刷新', exact: true }).hover();
  await page.getByRole('button', { name: '拉取预览', exact: true }).click();
  await page.getByText('隔离巡检数据 1', { exact: true }).waitFor();
  await runDialog.getByText('成功', { exact: true }).waitFor();
  assert(await runDialog.getByRole('button', { name: '同步原表', exact: true }).isDisabled());
  await runDialog.getByRole('button', { name: '下一页', exact: true }).click();
  await runDialog.getByText('隔离巡检数据 2', { exact: true }).waitFor();
  await page.keyboard.press('Escape');
  await runDialog.waitFor({ state: 'hidden' });
  await page.getByRole('button', { name: '定时设置' }).click();
  assert.equal(await runDialog.getByRole('checkbox', { name: inspectionCardSetting }).getAttribute('aria-checked'), 'false');
  await runDialog.getByText(/每日 00:10 准备，08:00 发送昨日汇总/).waitFor();
  await runDialog.getByLabel('运行时间（北京时间）').fill('25:01');
  await runDialog.getByRole('button', { name: '保存设置' }).click();
  await runDialog.getByRole('alert').getByText('请输入 1 至 24 个运行时间，格式为 HH:mm。').waitFor();
  await runDialog.getByLabel('运行时间（北京时间）').fill('11:00、07:00、11:00');
  for (const name of ['确认旧工具已停止写入，由灯塔同步原表', '启用定时同步', inspectionCardSetting]) {
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
