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
    let admin = true;
    let recoveredDelivery = false;
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
      if (endpoint === '/api/learning/bootstrap') return ok({ is_admin: admin, can_answer: false, can_view_buildings: true,
        scopes: [{ value: 'A', label: 'A楼' }], scope: 'A', today: dayOf(instant), settings: {enabled:true},
        delivery_status: { date: dayOf(instant), shifts: [
          {shift:'白',state:'success',label:'发布及发送成功',expected:2,sent:2,failed:0,pending:0,issues:[]},
          recoveredDelivery ? {shift:'夜',state:'success',label:'发布及发送成功',expected:3,sent:3,failed:0,pending:0,issues:[]} :
          {shift:'夜',state:'partial',label:'部分未完成',expected:3,sent:1,failed:1,pending:1,issues:[
            {name:'夜班测试甲',scope:'A',status:'failed',reason:'机器人对该人员不可用'},
            {name:'夜班测试乙',scope:'B',status:'pending',reason:'通知待发送'}]},
        ] },
        sync: { status: 'retry_waiting', pending: 9, active: false, waiting: 9, history_issue_count: 51, history_issues: [
          { record_id: 'recSyntheticIdentity', name: '测试同名人员', date: '2026-10-21', scope: 'A楼', reason: '人员表中存在重名，未自动选择',
            candidates: [{ record_id: 'recDirectoryOne', name: '测试同名人员', employee_no: 'TEST01', scopes: ['A', 'B'] }] },
          { record_id: 'recSyntheticMissing', name: '', date: '', scope: '', reason: '原记录没有可核验的人员姓名', candidates: [] },
        ] }, summary: {} });
      if (endpoint === '/api/learning/settings') return ok({ enabled: true, publish_time: '13:00', night_publish_time: '20:00', reminder_time: '17:00' });
      if (endpoint === '/api/learning/export' && query.kind === 'history_identity') return route.fulfill({
        headers: { 'Content-Type': 'text/csv; charset=utf-8', 'Content-Disposition': "attachment; filename*=UTF-8''history-identity.csv" }, body: 'name,reason\ntest,ambiguous\n' });
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
    for (const viewport of [{ width: 402, height: 874 }, { width: 440, height: 956 }, { width: 1440, height: 900 }]) {
      await page.setViewportSize(viewport);
      recoveredDelivery = false;
      await page.goto(entry); await ready();
      if (v2) {
        await page.getByRole('button', { name: '学练管理', exact: true }).click();
        await page.getByText('发布设置', { exact: true }).click();
      } else await page.getByRole('button', { name: '发布设置', exact: true }).click();
      await ready();
      assert(await page.locator('.sync-label').isVisible(), 'Pending status stays visible on phones');
      assert.match(await page.locator('.sync-label').innerText(), /等待重试.*9/);
      assert.equal(await page.locator('.sync-label .spin').count(), 0, 'Pending rows never imply active sync');
      const shiftRows = page.locator('.shift-publish-row');
      assert.equal(await shiftRows.count(), 2);
      assert.match(await shiftRows.nth(0).innerText(), /发布及发送成功.*已发送 2 \/ 2 人/s);
      assert.match(await shiftRows.nth(1).innerText(), /部分未完成.*已发送 1 \/ 3 人/s);
      await shiftRows.nth(1).locator('summary').click();
      await shiftRows.nth(1).getByText('机器人对该人员不可用', { exact: true }).waitFor();
      assert.match(await shiftRows.nth(1).innerText(), /夜班测试甲.*机器人对该人员不可用/s);
      assert.match(await shiftRows.nth(1).innerText(), /夜班测试乙.*通知待发送/s);
      const issues = page.locator('.history-identity-issues');
      await issues.locator('summary').click();
      await issues.getByText('人员表中存在重名，未自动选择', { exact: true }).waitFor();
      assert.match(await issues.innerText(), /TEST01/);
      assert.match(await issues.innerText(), /前 50 条/);
      const download = page.waitForEvent('download');
      await issues.getByRole('button', { name: '导出完整清单', exact: true }).click();
      assert.equal((await download).suggestedFilename(), 'history-identity.csv');
      await ready();
      assert.equal(await shiftRows.locator('[popover],[data-v2-feedback]').count(), 0, 'Publication status stays inline instead of becoming a toast');
      for (const status of await shiftRows.locator('.shift-delivery-status').all()) {
        assert(!['fixed', 'absolute'].includes(await status.evaluate(el => getComputedStyle(el).position)), 'Status remains beside its publication time');
      }
      assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1), false);
      const box = await issues.boundingBox();
      assert.ok(box.x >= 0 && box.x + box.width <= viewport.width + 1, 'Report fits viewport');
      await page.screenshot({ path: path.join(output, `${label}-identity-${viewport.width}.png`), fullPage: true });
      for (let shift = 0; shift < 2; shift++) {
        const input = shiftRows.nth(shift).locator('input');
        await input.evaluate(el => el.scrollIntoView({ block: 'center', behavior: 'instant' }));
        assert(await input.evaluate(el => {
          const r = el.getBoundingClientRect();
          const hit = document.elementFromPoint(r.x + r.width / 2, r.y + r.height / 2);
          return hit === el || el.contains(hit);
        }), `${label}/${viewport.width}/${shift}: time field can be scrolled clear of fixed navigation`);
      }
      await page.screenshot({ path: path.join(output, `${label}-publication-${viewport.width}.png`) });
      if (!v2 && timezoneId === 'UTC' && viewport.width === 1440) {
        await page.getByRole('textbox', { name: '白班题单发布时间', exact: true }).fill('13:15');
        recoveredDelivery = true;
        await shiftRows.nth(1).getByText('发布及发送成功', { exact: false }).waitFor({ timeout: 35000 });
        assert.equal(await shiftRows.nth(1).locator('details').count(), 0, 'Success removes previous failure names');
        assert.equal(await page.getByRole('textbox', { name: '白班题单发布时间', exact: true }).inputValue(), '13:15', 'Background status refresh preserves unsaved schedule');
        await page.getByRole('textbox', { name: '白班题单发布时间', exact: true }).fill('13:00');
      }
    }
    admin = false;
    await page.goto(entry + '&tab=settings'); await ready();
    assert.equal(await page.locator('.history-identity-issues').count(), 0, 'Non-admin cannot render report');
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
