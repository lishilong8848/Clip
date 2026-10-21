// Synthetic date regression: real components/API client, no business backend.
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';

const root = fileURLToPath(new URL('./lan_bitable_template_portal/frontend/', import.meta.url));
const require = createRequire(path.join(root, 'package.json'));
const { createServer } = require('vite');
const { chromium } = require('playwright');
const v2 = process.argv.includes('--v2');
const entryPath = v2 ? '/assets/workspace-v2/index.html' : '/assets/index.html';
const output = await mkdtemp(path.join(tmpdir(), 'learning-dates-'));
const requests = [], errors = [];
const validDay = value => /^\d{4}-\d{2}-\d{2}$/.test(value || '') && Number.isFinite(Date.parse(value))
  && new Date(value).toISOString().slice(0, 10) === value;
const server = await createServer({ root, base: '/', configFile: path.join(root, v2 ? 'vite.workspace-v2.config.ts' : 'vite.config.ts'),
  optimizeDeps: { noDiscovery: true, include: ['vue', 'naive-ui', 'lucide-vue-next'] },
  cacheDir: path.join(output, 'vite'), logLevel: 'error', server: { host: '127.0.0.1', port: 0, hmr: false },
  plugins: [{ name: 'learning-date-fixture', configureServer(vite) {
    vite.middlewares.use(async (req, res, next) => {
      const url = new URL(req.url, 'http://fixture.local');
      if (url.pathname === '/__learning_dates_stop') { res.end('stopped'); setTimeout(() => void server.close(), 20); return; }
      if (url.pathname === '/__learning_dates' || url.pathname === entryPath) {
        const entry = url.pathname === entryPath;
        res.setHeader('Content-Type', 'text/html; charset=utf-8');
        return res.end(await vite.transformIndexHtml(req.url, `<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head><body class="${v2 ? 'workspace-v2' : ''}"><div id="app"></div><script type="module">
          import {createApp,h,provide,ref} from 'vue'; import LearningCompletion from '/src/components/LearningCompletion.vue';
          import LearningPage from '/src/components/LearningPage.vue'; import UiTransition from '/src/components/UiTransition.vue';
          import {NConfigProvider,NDialogProvider,NMessageProvider} from 'naive-ui'; import {pageContext} from '/src/workspace-v2/pageLayout';
          createApp({setup(){const refresh=ref(0),scope=ref('A');window.refreshCompletion=()=>refresh.value++;window.changeCompletionScope=()=>{scope.value='B';refresh.value++;};
            provide(pageContext,{user:ref({open_id:'synthetic-dates',modules:['learning']}),scope,scopes:ref(['A','B']),module:ref('learning')});
            return()=>h(NConfigProvider,null,{default:()=>h(NDialogProvider,null,{default:()=>h(NMessageProvider,null,{default:()=>${entry
              ? "h(LearningPage,{scope:'A',userId:'synthetic-dates'})"
              : "h(LearningCompletion,{scope:scope.value,canViewBuildings:true,refreshKey:refresh.value})"}})})});
          }}).component('UiTransition',UiTransition).mount('#app');
        </script></body></html>`));
      }
      if (!url.pathname.startsWith('/api/')) return next();
      requests.push({ path: url.pathname, method: req.method, query: Object.fromEntries(url.searchParams) });
      res.setHeader('Content-Type', 'application/json');
      const ok = data => res.end(JSON.stringify({ ok: true, data }));
      if (url.pathname.startsWith('/api/workspace-layouts/')) return ok({ revision: 0, value: null });
      if (url.pathname === '/api/health') return res.end(JSON.stringify({ ok: true, service: 'clipflow_backend', instance_id: 'date-fixture' }));
      if (url.pathname === '/api/learning/bootstrap') return ok({ is_admin: true, can_answer: false,
        can_view_buildings: true, scopes: [{ value: 'A', label: 'A楼' }], scope: 'A', today: '2024-02-29',
        settings: {}, sync: { status: 'ready', pending: 0 } });
      if (url.pathname === '/api/learning/profile') return ok({ summary: { answered: 0 }, people: [], trend: [],
        from: url.searchParams.get('from') || '2024-02-23', to: url.searchParams.get('to') || '2024-02-29' });
      if (url.pathname === '/api/learning/completion') {
        const from = url.searchParams.get('from') || '2024-02-29', to = url.searchParams.get('to') || '2024-02-29';
        if (!validDay(from) || !validDay(to)) {
          res.statusCode = 400;
          return res.end(JSON.stringify({ ok: false, error: '请选择有效统计日期。' }));
        }
        return res.end(JSON.stringify({ ok: true, data: { from, to, items: [], total: 0, summary: {} } }));
      }
      errors.push(`Unexpected endpoint: ${req.method} ${url.pathname}`);
      res.statusCode = 500; res.end(JSON.stringify({ ok: false, error: 'Unexpected fixture request' }));
    });
  } }],
});
await server.listen();
const base = `http://127.0.0.1:${server.httpServer.address().port}`;
const url = `${base}/__learning_dates`;
let browser, passed = false;
try {
  browser = await chromium.launch({ headless: true });
  for (const [locale, timezoneId, instant, day] of [
    ['en-GB', 'UTC', '2024-02-28T15:59:59.999Z', '2024-02-28'],
    ['en-US', 'America/Los_Angeles', '2024-02-28T15:59:59.999Z', '2024-02-28'],
    ['zh-CN', 'Asia/Shanghai', '2024-02-28T16:00:00.000Z', '2024-02-29'],
    ['fr-FR', 'Europe/Paris', '2024-02-29T16:00:00.000Z', '2024-03-01'],
    ['th-TH', 'Asia/Bangkok', '2026-02-28T16:00:00.000Z', '2026-03-01'],
    ['ar-EG', 'Pacific/Honolulu', '2026-12-31T16:00:00.000Z', '2027-01-01'],
  ]) {
    const context = await browser.newContext({ locale, timezoneId, viewport: { width: 1440, height: 900 } });
    const page = await context.newPage();
    page.on('pageerror', error => errors.push(error.message));
    await page.route('**/*', route => new URL(route.request().url()).origin === base ? route.continue() : route.abort());
    await page.clock.setFixedTime(new Date(instant));
    await page.addInitScript(() => {
      const original = Date.prototype.toLocaleDateString;
      // Reproduce a locale/ICU fallback that formats en-CA as a slash date.
      Date.prototype.toLocaleDateString = function(locale, options) {
        return original.call(this, locale === 'en-CA' ? 'en-US' : locale, options);
      };
    });
    const beforeMount = requests.length;
    await page.goto(url);
    await page.locator('.completion[aria-busy="false"]').waitFor();
    assert.deepEqual(requests.at(-1)?.query, { from: day, to: day, scope: 'A', page: '1', page_size: '30' },
      `ISO query despite locale fallback (${locale}); actual input: ${await page.locator('.completion-filters input').first().inputValue()}`);
    assert.equal(await page.locator('.completion-error').count(), 0);
    assert.equal(requests.length, beforeMount + 1, 'Mount must fetch once');
    const beforeValidation = requests.length;
    const checks = await page.evaluate(async () => {
      const { beijingDate } = await import('/src/workspace-v2/eventPeriod.ts');
      const { completionDateError } = await import('/src/learningDates.ts');
      const { getLearningCompletion } = await import('/src/api/learning.ts');
      const valid = ['0001-01-01', '2000-02-29', '2024-02-29', '2026-10-21', '9999-12-31'];
      const invalid = ['', '02/29/2024', '2024-2-29', '2026-02-29', '1900-02-29', '2100-02-29',
        '2024-04-31', '2024-00-10', '2024-13-01', '0000-01-01', '2024-02-29T00:00:00Z', ' 2024-02-29'];
      const rejected = [];
      const queries = invalid.filter(Boolean).flatMap(value => [{ from: value, to: '2024-02-29' }, { from: '2024-02-29', to: value }]);
      queries.push({ from: '2024-02-01', to: '2024-03-03' }, { from: '2024-03-02', to: '2024-02-01' });
      for (const query of queries) {
        try { await getLearningCompletion(query); rejected.push(false); }
        catch { rejected.push(true); }
      }
      return { today: beijingDate(), valid: valid.map(day => completionDateError(day, day)),
        invalid: invalid.map(day => !!completionDateError(day, day)), rejected,
        range: [['2024-02-01', '2024-03-02'], ['2024-02-01', '2024-03-03'], ['2024-03-02', '2024-02-01']]
          .map(([from, to]) => !!completionDateError(from, to)) };
    });
    assert.equal(checks.today, day);
    assert.ok(checks.valid.every(value => value === ''));
    assert.ok(checks.invalid.every(Boolean)); assert.ok(checks.rejected.every(Boolean));
    assert.deepEqual(checks.range, [false, true, true]);
    assert.equal(requests.length, beforeValidation, 'API validation must reject without fetching');
    await page.evaluate(async () => {
      const { getLearningCompletion } = await import('/src/api/learning.ts');
      await getLearningCompletion({ from: '', to: '', status: '', shift: '' });
      await getLearningCompletion({});
    });
    assert.deepEqual(requests.slice(-2).map(request => request.query), [{}, {}], 'Unset dates must be omitted');
    const count = requests.length;
    // Native date inputs sanitize malformed values to empty, just as clearing does.
    const start = page.locator('.completion-filters input').first();
    await start.evaluate(input => { input.value = 'bad-date'; input.dispatchEvent(new Event('input', { bubbles: true })); });
    await page.getByRole('alert').waitFor();
    await page.getByRole('button', { name: '查询', exact: true }).click();
    for (let i = 0; i < 3; i++) await page.getByRole('button', { name: '刷新完成情况', exact: true }).click();
    await page.evaluate(() => window.refreshCompletion());
    await page.locator('.completion-filters').dispatchEvent('submit');
    await page.locator('.completion[aria-busy="false"]').waitFor();
    assert.equal(requests.length, count, 'Invalid/empty refresh must not send or default to today');
    await start.fill(day);
    if (v2) await start.press('Tab');
    await page.getByRole('alert').waitFor({ state: 'detached' });
    assert.equal(await page.locator('.completion-filters').evaluate(form => form.checkValidity()), true);
    await page.getByRole('button', { name: '查询', exact: true }).click();
    await page.locator('.completion[aria-busy="false"]').waitFor();
    assert.equal(requests.length, count + 1);
    assert.equal(requests.at(-1).query.from, day);
    if (timezoneId === 'UTC' || timezoneId === 'Asia/Shanghai') {
      await page.goto(url);
      await page.locator('.completion[aria-busy="false"]').waitFor();
      const beforeRollover = requests.length;
      const nextInstant = new Date(`${day}T16:00:00Z`);
      const nextDay = new Date(nextInstant.getTime() + 8 * 3600000).toISOString().slice(0, 10);
      await page.clock.setFixedTime(nextInstant);
      await page.evaluate(() => {
        for (let i = 0; i < 3; i++) document.dispatchEvent(new Event('visibilitychange'));
      });
      await page.waitForFunction(expected => document.querySelector('.completion-filters input').value === expected, nextDay);
      await page.locator('.completion[aria-busy="false"]').waitFor();
      assert.equal(requests.length, beforeRollover + 1, 'Midnight visibility events must fetch once');
      assert.equal(requests.at(-1).query.to, nextDay);
      const beforeScopeChange = requests.length;
      await page.evaluate(() => window.changeCompletionScope());
      await page.locator('.completion[aria-busy="false"]').waitFor();
      assert.equal(requests.length, beforeScopeChange + 1, 'Scope and refresh changes in one render fetch once');
      assert.equal(requests.at(-1).query.scope, 'B');
      await start.fill(day);
      await page.getByRole('button', { name: '查询', exact: true }).click();
      await page.locator('.completion[aria-busy="false"]').waitFor();
      const beforeCustomRefresh = requests.length;
      await page.clock.setFixedTime(new Date(nextInstant.getTime() + 86400000));
      await page.evaluate(() => document.dispatchEvent(new Event('visibilitychange')));
      await page.getByRole('button', { name: '刷新完成情况', exact: true }).click();
      await page.locator('.completion[aria-busy="false"]').waitFor();
      assert.equal(requests.length, beforeCustomRefresh + 1, 'Custom range does not auto-fetch at midnight');
      assert.equal(requests.at(-1).query.from, day);
      assert.equal(requests.at(-1).query.to, nextDay);
    }
    if (locale === 'zh-CN') {
      for (const [name, width, height] of [['desktop', 1440, 900], ['tablet', 768, 1024], ['mobile', 390, 844]]) {
        await page.setViewportSize({ width, height });
        assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth), false);
        await page.screenshot({ path: path.join(output, name + '.png'), fullPage: true });
      }
    }
    await context.close();
  }
  for (const timezoneId of ['UTC', 'Asia/Shanghai']) {
    const context = await browser.newContext({ timezoneId, locale: 'zh-CN', viewport: { width: 1440, height: 900 } });
    const page = await context.newPage();
    page.on('pageerror', error => errors.push(error.message));
    await page.route('**/*', route => new URL(route.request().url()).origin === base ? route.continue() : route.abort());
    await page.clock.setFixedTime(new Date('2024-02-28T16:00:00Z'));
    await page.addInitScript(() => { Date.prototype.toLocaleDateString = () => '2024年2月29日'; });
    const profileRequests = () => requests.filter(request => request.path === '/api/learning/profile');
    for (const suffix of ['', '&from=&to=', '&from=2024-02-01&to=2024-02-29']) {
      const before = profileRequests().length;
      await page.goto(`${base}${entryPath}?__page=/learning${suffix}`);
      await page.locator('.profile-toolbar').waitFor();
      await page.locator('.loading-line').waitFor({ state: 'detached' });
      assert.equal(profileRequests().length, before + 1, 'Learning entry reads the profile once');
      const custom = suffix.includes('2024-02-01');
      assert.deepEqual(profileRequests().at(-1).query, { scope: 'A', page: '1', page_size: '20',
        ...(custom ? { from: '2024-02-01', to: '2024-02-29' } : { period: '7' }) });
      assert.equal(await page.getByText('请选择有效统计日期。', { exact: true }).count(), 0);
    }
    const beforePeriod = profileRequests().length;
    if (v2) await page.getByText('近30天', { exact: true }).click();
    else {
      await page.locator('.profile-toolbar').getByRole('combobox').click();
      await page.getByRole('option', { name: '近30天', exact: true }).click();
    }
    await page.locator('.loading-line').waitFor({ state: 'detached' });
    assert.equal(profileRequests().length, beforePeriod + 1, 'Period change must fetch once');
    assert.deepEqual(profileRequests().at(-1).query, { scope: 'A', page: '1', page_size: '20', period: '30' });
    await page.goto(`${base}${entryPath}?__page=/learning&tab=completion`);
    await page.locator('.completion[aria-busy="false"]').waitFor();
    assert.equal(requests.at(-1).query.from, '2024-02-29');
    assert.equal(requests.at(-1).query.to, '2024-02-29');
    assert.equal(await page.locator('.completion-error').count(), 0);
    await context.close();
  }
  assert.ok(requests.every(request => request.method === 'GET'
    && (request.query.from === undefined || validDay(request.query.from))
    && (request.query.to === undefined || validDay(request.query.to))));
  assert.deepEqual(errors, []);
  passed = true;
  console.log(`PASS (${v2 ? 'v2' : 'legacy'}): learning entry/empty filters/custom/30-day period, UTC/Shanghai and locale variants, midnight rollover, strict input/API, no duplicate fetches. Screenshots: ${output}`);
  if (process.argv.includes('--serve')) console.log(`Synthetic preview: ${url}`);
} finally {
  await browser?.close();
  if (!passed || !process.argv.includes('--serve')) await server.close();
}
