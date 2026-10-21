// Serve only the existing production dist; every API response is synthetic.
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { createHash } from 'node:crypto';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const root = fileURLToPath(new URL('./lan_bitable_template_portal/frontend/', import.meta.url));
const dist = path.join(root, 'dist');
const output = fileURLToPath(new URL('../output/playwright/learning-dates-dist/', import.meta.url));
const { chromium } = createRequire(path.join(root, 'package.json'))('playwright');
const loaded = new Map(), failures = [], cases = [];
const hash = value => createHash('sha256').update(value).digest('hex');
const types = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css',
  '.json': 'application/json', '.svg': 'image/svg+xml', '.png': 'image/png', '.woff2': 'font/woff2' };
const server = createServer(async (request, response) => {
  const url = new URL(request.url, 'http://fixture');
  const relative = ['/', '/learning'].includes(url.pathname) ? 'index.html' : decodeURIComponent(url.pathname).slice(1);
  const file = path.resolve(dist, relative);
  if (!file.startsWith(dist + path.sep) || request.method !== 'GET' || url.pathname.startsWith('/api/')) {
    failures.push(`Unexpected static request: ${request.method} ${url.pathname}`);
    response.writeHead(403); response.end(); return;
  }
  try {
    const data = await readFile(file), digest = hash(data);
    assert.equal(loaded.get(relative) || digest, digest, `Dist changed during checks: ${relative}`);
    loaded.set(relative, digest);
    response.writeHead(200, { 'Content-Type': types[path.extname(file)] || 'application/octet-stream', 'Cache-Control': 'no-store' });
    response.end(data);
  } catch (error) {
    failures.push(`Static asset failed: ${relative}: ${error.message}`);
    response.writeHead(404); response.end();
  }
});
await mkdir(output, { recursive: true });
await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
const base = `http://127.0.0.1:${server.address().port}`;
const dayOf = instant => new Date(Date.parse(instant) + 8 * 3600000).toISOString().slice(0, 10);
let browser, activePage;
try {
  browser = await chromium.launch({ headless: true });
  for (const v2 of [false, true]) for (const timezoneId of ['UTC', 'Asia/Shanghai']) {
    const label = `${v2 ? 'v2' : 'legacy'}-${timezoneId.replace('/', '-')}`;
    const requests = [];
    let instant = '2024-02-28T15:59:59.999Z';
    const context = await browser.newContext({ timezoneId, locale: timezoneId === 'UTC' ? 'en-US' : 'zh-CN',
      serviceWorkers: 'block', viewport: { width: 1440, height: 1000 } });
    await context.route('**/*', async route => {
      const request = route.request(), url = new URL(request.url()), endpoint = url.pathname;
      if (url.origin !== base) { failures.push(`External request: ${request.url()}`); return route.abort(); }
      if (!endpoint.startsWith('/api/')) return route.continue();
      const query = Object.fromEntries(url.searchParams);
      requests.push({ endpoint, method: request.method(), query });
      const ok = data => route.fulfill({ json: { ok: true, data } });
      const fail = message => route.fulfill({ status: 400, json: { ok: false, error: message } });
      if (request.method() !== 'GET') { failures.push(`Unexpected write: ${endpoint}`); return fail('No writes in date checks'); }
      if (endpoint === '/api/health') return route.fulfill({ json: { ok: true, service: 'clipflow_backend', instance_id: 'dates-dist-fixture' } });
      if (endpoint === '/api/auth/status') return ok({ logged_in: true,
        user: { open_id: 'synthetic-date-admin', name: '日期验收', role: 'admin', level: 'L0', modules: ['learning'] },
        scope_options: [{ value: 'A', label: 'A楼' }] });
      if (endpoint === '/api/assistant/appearance') return ok({ enabled: false });
      if (endpoint === '/api/scope-overview') return ok({ scopes: {} });
      if (endpoint === '/api/handover-links') return ok({ links: {} });
      if (endpoint === '/api/auth/permission-requests/current') return ok({});
      if (endpoint.startsWith('/api/workspace-layouts/')) return ok({ revision: 0, value: null });
      if (endpoint === '/api/learning/bootstrap') return ok({ is_admin: true, can_answer: false, can_view_buildings: true,
        scopes: [{ value: 'A', label: 'A楼' }], scope: 'A', today: dayOf(instant), settings: {},
        sync: { status: 'ready', pending: 0 }, summary: {} });
      if (endpoint === '/api/learning/profile') return ok({ summary: { answered: 0 }, people: [], trend: [],
        from: query.from || new Date(Date.parse(dayOf(instant)) - (Number(query.period || 7) - 1) * 86400000).toISOString().slice(0, 10),
        to: query.to || dayOf(instant) });
      if (endpoint === '/api/learning/completion') {
        const from = query.from || dayOf(instant), to = query.to || dayOf(instant);
        const valid = value => /^(?!0000)\d{4}-\d{2}-\d{2}$/.test(value) && Number.isFinite(Date.parse(value))
          && new Date(value).toISOString().slice(0, 10) === value;
        if (!valid(from) || !valid(to)) return fail('请选择有效统计日期。');
        const days = (Date.parse(to) - Date.parse(from)) / 86400000;
        if (days < 0 || days > 30) return fail('一次最多查询31天，开始日期不能晚于结束日期。');
        return ok({ from, to, items: [], total: 0, summary: {} });
      }
      failures.push(`Unexpected endpoint: ${endpoint}`); return fail('Unexpected fixture request');
    });
    const page = await context.newPage(); activePage = page;
    page.setDefaultTimeout(15000);
    page.on('pageerror', error => failures.push(error.message));
    await page.clock.setFixedTime(new Date(instant));
    await page.addInitScript(value => { Date.prototype.toLocaleDateString = () => value; },
      timezoneId === 'UTC' ? '02/28/2024' : '2024年2月28日');
    const entry = base + (v2 ? '/assets/workspace-v2/index.html?__page=/learning' : '/learning?frontend=legacy');
    const profileReads = () => requests.filter(request => request.endpoint === '/api/learning/profile');
    const completionReads = () => requests.filter(request => request.endpoint === '/api/learning/completion');
    const ready = () => page.locator('.learning-page[aria-busy="false"]').waitFor();
    const completionReady = () => page.locator('.completion[aria-busy="false"]').waitFor();
    for (const suffix of ['', '&from=&to=', '&from=2024-02-01&to=2024-02-28']) {
      const count = profileReads().length;
      await page.goto(entry + suffix); await ready(); await page.locator('.profile-toolbar').waitFor();
      assert.equal(profileReads().length, count + 1, `${label}: one initial profile fetch`);
      const { query } = profileReads().at(-1);
      assert.equal(query.page, '1'); assert.equal(query.page_size, '20');
      if (suffix.includes('2024-02-01')) {
        assert.equal(query.from, '2024-02-01'); assert.equal(query.to, '2024-02-28');
        assert.equal(query.period, undefined);
      } else {
        assert.equal(query.period, '7'); assert.equal(query.from, undefined); assert.equal(query.to, undefined);
      }
      assert.equal(await page.getByText('请选择有效统计日期。', { exact: true }).count(), 0);
    }
    const beforePeriod = profileReads().length;
    if (v2) await page.getByText('近30天', { exact: true }).click();
    else {
      await page.locator('.profile-toolbar').getByRole('combobox').click();
      await page.getByRole('option', { name: '近30天', exact: true }).click();
    }
    await ready();
    assert.equal(profileReads().length, beforePeriod + 1);
    assert.equal(profileReads().at(-1).query.period, '30');
    assert.equal(profileReads().at(-1).query.from, undefined);
    assert.equal(profileReads().at(-1).query.to, undefined);
    const beforeMount = completionReads().length;
    await page.goto(entry + '&tab=completion'); await ready(); await completionReady();
    assert.equal(completionReads().length, beforeMount + 1);
    assert.equal(completionReads().at(-1).query.from, '2024-02-28');
    assert.equal(completionReads().at(-1).query.to, '2024-02-28');
    assert.equal(await page.locator('.completion-error').count(), 0);
    const inputs = page.locator('.completion-filters input[type="date"]');
    instant = '2024-02-28T16:00:00.000Z';
    await page.clock.setFixedTime(new Date(instant));
    await page.evaluate(() => { for (let i = 0; i < 3; i++) document.dispatchEvent(new Event('visibilitychange')); });
    await page.waitForFunction(() => document.querySelector('.completion-filters input').value === '2024-02-29');
    await completionReady();
    assert.equal(completionReads().length, beforeMount + 2, `${label}: rollover reads once`);
    assert.equal(completionReads().at(-1).query.to, '2024-02-29');
    const beforeInvalid = completionReads().length;
    await inputs.first().evaluate(input => { input.value = '2024-02-30'; input.dispatchEvent(new Event('input', { bubbles: true })); });
    await page.locator('.completion-error').waitFor();
    await page.locator('.completion-filters').dispatchEvent('submit');
    await page.getByRole('button', { name: '刷新完成情况', exact: true }).click();
    assert.equal(completionReads().length, beforeInvalid, 'Invalid/cleared explicit input must not fetch');
    await inputs.first().fill('2024-02-01'); await inputs.last().fill('2024-03-03');
    assert.match(await page.locator('.completion-error').innerText(), /最多查询31天/);
    await page.locator('.completion-filters').dispatchEvent('submit');
    assert.equal(completionReads().length, beforeInvalid);
    await inputs.last().fill('2024-03-02');
    await page.getByRole('button', { name: '查询', exact: true }).click(); await completionReady();
    assert.equal(completionReads().length, beforeInvalid + 1, 'Inclusive 31-day range remains valid');
    assert.equal(completionReads().at(-1).query.to, '2024-03-02');
    await inputs.last().fill('2024-01-31');
    assert.match(await page.locator('.completion-error').innerText(), /开始日期不能晚于结束日期/);
    await page.locator('.completion-filters').dispatchEvent('submit');
    assert.equal(completionReads().length, beforeInvalid + 1);
    await inputs.last().fill('2024-02-29');
    await page.getByRole('button', { name: '查询', exact: true }).click(); await completionReady();
    const beforeCustom = completionReads().length;
    instant = '2024-02-29T16:00:00Z';
    await page.clock.setFixedTime(new Date(instant));
    await page.evaluate(() => document.dispatchEvent(new Event('visibilitychange')));
    await page.getByRole('button', { name: '刷新完成情况', exact: true }).click(); await completionReady();
    assert.equal(completionReads().length, beforeCustom + 1, 'Custom range refreshes once, without automatic rollover');
    assert.equal(completionReads().at(-1).query.from, '2024-02-01');
    assert.equal(completionReads().at(-1).query.to, '2024-02-29');
    assert.equal(await page.locator('.completion-error').count(), 0);
    await page.screenshot({ path: path.join(output, label + '.png'), fullPage: true });
    cases.push({ label, profileReads: profileReads().length, completionReads: completionReads().length, requests });
    console.log(`PASS ${label}: actual dist entry, blank filters, periods, Shanghai midnight, strict dates and fetch counts`);
    await context.close(); activePage = undefined;
  }
  assert.deepEqual(failures, []);
  for (const [file, digest] of loaded) assert.equal(hash(await readFile(path.join(dist, file))), digest, `Dist changed: ${file}`);
  await writeFile(path.join(output, 'result.json'), JSON.stringify({ cases, assets: Object.fromEntries(loaded), failures }, null, 2));
  console.log(`PASS: ${cases.length} actual-dist cases; ${loaded.size} served assets unchanged; report ${output}`);
} catch (error) {
  if (activePage && !activePage.isClosed()) {
    console.error((await activePage.locator('body').innerText()).slice(0, 2200));
    await activePage.screenshot({ path: path.join(output, 'failed.png'), fullPage: true });
  }
  console.error(failures); throw error;
} finally {
  await browser?.close();
  await new Promise(resolve => server.close(resolve));
}
