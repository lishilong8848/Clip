import assert from 'node:assert/strict';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';
import { preview } from 'vite';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const outDir = path.resolve(root, process.env.CLIPFLOW_HOME_BUILD_DIR || 'dist');
const server = await preview({ root, build: { outDir }, logLevel: 'error', preview: { host: '127.0.0.1', port: 0 } });
const origin = `http://127.0.0.1:${server.httpServer.address().port}`;
const browser = await chromium.launch({ headless: true });
const failures = new Map(), calls = new Map(), errors = [], writes = [];
const user = { open_id: 'home-status-fixture', name: '隔离测试', role: 'building', level: 'L4',
  category: 'engineer', home_scope: 'A', scope_rules: ['A'], modules: ['maintenance'] };
const authPath = '/api/auth/status', overviewPath = '/api/scope-overview', linksPath = '/api/handover-links';
let authStatus = 200;
let overviewReady = true;
let userName = user.name, delayNextAuth = false, releaseAuth;
try {
  const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  await context.route('**/*', async route => {
    const req = route.request(), url = new URL(req.url()), key = url.pathname;
    if (url.origin !== origin) return route.abort();
    if (!key.startsWith('/api/')) return route.continue();
    if (req.method() !== 'GET') { writes.push(key); return route.fulfill({ status: 503, json: { ok: false } }); }
    calls.set(key, (calls.get(key) || 0) + 1);
    if (key === '/api/health') return route.fulfill({ json: { ok: true, service: 'clipflow_backend', instance_id: 'home-status-isolated' } });
    if (key === '/api/auth/login') return route.fulfill({ contentType: 'text/html', body: '<h1>Login fixture</h1>' });
    if (key === authPath && delayNextAuth) {
      delayNextAuth = false;
      const oldUser = { ...user, name: userName };
      await new Promise(resolve => { releaseAuth = resolve; });
      return route.fulfill({ json: { ok: true, data: { logged_in: true, user: oldUser, scope_options: [{ value: 'A', label: 'A楼' }] } } });
    }
    if (key === authPath && authStatus === 401) return route.fulfill({ status: 401, json: { ok: false, auth_required: true } });
    const fail = failures.get(key) || 0;
    if (fail > 0) { failures.set(key, fail - 1); return route.fulfill({ status: 503, json: { ok: false } }); }
    const data = key === authPath ? { logged_in: true, user: { ...user, name: userName }, scope_options: [{ value: 'A', label: 'A楼' }] }
      : key === overviewPath ? { scopes: { A: {} }, source_snapshot_ready: overviewReady }
      : key === linksPath ? { links: {} } : { items: [], records: [], stats: {}, summary: {} };
    return route.fulfill({ json: { ok: true, data } });
  });
  const page = await context.newPage();
  page.on('pageerror', error => errors.push(error.message));
  const notice = page.locator('.status-banner.warning');
  const openHome = async () => {
    calls.clear();
    await page.goto(origin + '/');
    await page.locator('.module-card').first().waitFor();
  };

  failures.set(overviewPath, 1);
  await openHome();
  await page.waitForFunction(() => !document.querySelector('.overview-refreshing'));
  assert.equal(calls.get(overviewPath), 2, 'one transient read error retries once');
  assert.equal(calls.get(linksPath) || 0, 0, 'homepage does not preload links without the handover grant');
  assert.equal(await notice.count(), 0, 'a recovered read leaves no global service error');

  failures.set(overviewPath, 100);
  await openHome();
  await notice.getByText(/首页概览暂未读取/).waitFor();
  assert.equal(calls.get(overviewPath), 3, 'persistent failure is bounded to three GET attempts');
  assert(!/服务异常|稍后会自动重试/.test(await page.locator('body').innerText()));
  assert.equal(await page.locator('#clipflow-connection-warning,#clipflow-connection-guard').count(), 0, 'a healthy health endpoint is not treated as disconnected');
  assert(await page.locator('.module-card').first().isVisible());
  failures.set(overviewPath, 0);
  await notice.getByRole('button', { name: '重新读取', exact: true }).click();
  await notice.waitFor({ state: 'detached' });

  overviewReady = false;
  await openHome();
  await notice.getByText(/首页概览暂未读取/).waitFor();
  assert.equal(calls.get(overviewPath), 1, 'a partial successful response is not blindly retried');
  overviewReady = true;
  await notice.getByRole('button', { name: '重新读取', exact: true }).click();
  await notice.waitFor({ state: 'detached' });

  failures.set(overviewPath, 100);
  failures.set(linksPath, 100);
  user.modules.push('handover');
  await openHome();
  await notice.getByText(/首页概览暂未读取.*交接班链接暂未读取|交接班链接暂未读取.*首页概览暂未读取/).waitFor();
  failures.set(overviewPath, 0);
  await notice.getByRole('button', { name: '重新读取', exact: true }).click();
  await page.waitForFunction(() => {
    const text = document.querySelector('.status-banner.warning')?.textContent || '';
    return text.includes('交接班链接暂未读取') && !text.includes('首页概览暂未读取');
  });
  await page.waitForTimeout(3100);
  assert.match(await notice.innerText(), /交接班链接暂未读取/, 'another successful request cannot clear an outstanding data error');
  const beforeRevocation = calls.get(linksPath);
  user.modules = ['maintenance'];
  const revokedAuth = page.waitForResponse(response => new URL(response.url()).pathname === authPath);
  await page.evaluate(() => dispatchEvent(new Event('auth-account-changed')));
  await revokedAuth;
  await notice.waitFor({ state: 'detached' });
  assert.equal(calls.get(linksPath), beforeRevocation, 'revoked grants clear stale errors without reloading forbidden links');
  failures.set(linksPath, 0);
  user.modules.push('handover');
  await openHome();
  assert.equal(calls.get(linksPath), 1, 'an authorized user still loads handover links');

  failures.set(authPath, 100);
  calls.clear();
  await page.goto(origin + '/');
  await notice.getByText(/登录状态暂未核验/).waitFor();
  assert.equal(calls.get(authPath), 3);
  assert(!page.url().includes('/api/auth/login'), '503 does not expire the login or redirect to authorization');
  failures.set(authPath, 0);
  await notice.getByRole('button', { name: '重新读取', exact: true }).click();
  await notice.waitFor({ state: 'detached' });
  await page.locator('.module-card').first().waitFor();
  assert.match(await page.locator('body').innerText(), /隔离测试/);

  delayNextAuth = true;
  await page.evaluate(() => dispatchEvent(new Event('auth-account-changed')));
  for (let attempt = 0; typeof releaseAuth !== 'function' && attempt < 50; attempt++) await page.waitForTimeout(20);
  assert.equal(typeof releaseAuth, 'function');
  userName = '已更新的账号资料';
  await page.evaluate(() => dispatchEvent(new Event('auth-account-changed')));
  await page.waitForFunction(() => document.body.textContent.includes('已更新的账号资料'));
  const oldReply = page.waitForResponse(response => new URL(response.url()).pathname === authPath);
  releaseAuth();
  await oldReply;
  await page.waitForTimeout(100);
  assert.match(await page.locator('body').innerText(), /已更新的账号资料/, 'an older successful reply cannot roll back newer account information');
  assert.equal(await notice.count(), 0);

  for (const width of [1440, 390]) {
    await page.setViewportSize({ width, height: 900 });
    assert(!await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1));
  }
  failures.set(overviewPath, 100);
  await openHome();
  const beforeUnmount = calls.get(overviewPath);
  assert(beforeUnmount >= 1);
  await page.evaluate(() => document.querySelector('#app').__vue_app__.unmount());
  await page.waitForTimeout(3100);
  assert.equal(calls.get(overviewPath), beforeUnmount, 'unmount cancels delayed retries instead of requesting data for an abandoned page');
  assert.deepEqual(writes, [], 'read recovery never submits business operations');
  assert.deepEqual(errors, [], 'auth read failures do not cause unhandled rejections');
  authStatus = 401;
  await page.goto(origin + '/');
  await page.waitForURL(/\/api\/auth\/login/);
  console.log('PASS: real homepage, healthy service with failing data APIs, bounded GET retry, per-endpoint recovery, 503 retains login and 401 still authorizes, desktop/mobile. All APIs mocked.');
} finally {
  await browser.close();
  await server.close();
}
