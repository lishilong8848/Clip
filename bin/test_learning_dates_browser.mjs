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
      if (url.pathname === '/__learning_dates') {
        res.setHeader('Content-Type', 'text/html; charset=utf-8');
        return res.end(await vite.transformIndexHtml(req.url, `<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head><body class="${v2 ? 'workspace-v2' : ''}"><div id="app"></div><script type="module">
          import {createApp,h,ref} from 'vue'; import LearningCompletion from '/src/components/LearningCompletion.vue';
          import {NConfigProvider} from 'naive-ui';
          createApp({setup(){const refresh=ref(0);window.refreshCompletion=()=>refresh.value++;return()=>h(NConfigProvider,null,{default:()=>h(LearningCompletion,{scope:'A',canViewBuildings:true,refreshKey:refresh.value})});}}).mount('#app');
        </script></body></html>`));
      }
      if (!url.pathname.startsWith('/api/')) return next();
      requests.push({ path: url.pathname, method: req.method, query: Object.fromEntries(url.searchParams) });
      res.setHeader('Content-Type', 'application/json');
      if (url.pathname === '/api/learning/completion') {
        const from = url.searchParams.get('from'), to = url.searchParams.get('to');
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
    await page.goto(url);
    await page.locator('.completion[aria-busy="false"]').waitFor();
    assert.deepEqual(requests.at(-1)?.query, { from: day, to: day, scope: 'A', page: '1', page_size: '30' },
      `ISO query despite locale fallback (${locale}); actual input: ${await page.locator('.completion-filters input').first().inputValue()}`);
    assert.equal(await page.locator('.completion-error').count(), 0);
    const beforeValidation = requests.length;
    const checks = await page.evaluate(async () => {
      const { beijingDate } = await import('/src/workspace-v2/eventPeriod.ts');
      const { completionDateError } = await import('/src/learningDates.ts');
      const { getLearningCompletion } = await import('/src/api/learning.ts');
      const valid = ['0001-01-01', '2000-02-29', '2024-02-29', '2026-10-21', '9999-12-31'];
      const invalid = ['', '02/29/2024', '2024-2-29', '2026-02-29', '1900-02-29', '2100-02-29',
        '2024-04-31', '2024-00-10', '2024-13-01', '0000-01-01', '2024-02-29T00:00:00Z', ' 2024-02-29'];
      const rejected = [];
      const queries = invalid.flatMap(value => [{ from: value, to: '2024-02-29' }, { from: '2024-02-29', to: value }]);
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
    if (locale === 'zh-CN') {
      for (const [name, width, height] of [['desktop', 1440, 900], ['tablet', 768, 1024], ['mobile', 390, 844]]) {
        await page.setViewportSize({ width, height });
        assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth), false);
        await page.screenshot({ path: path.join(output, name + '.png'), fullPage: true });
      }
    }
    await context.close();
  }
  assert.ok(requests.every(request => request.method === 'GET' && validDay(request.query.from) && validDay(request.query.to)));
  assert.deepEqual(errors, []);
  passed = true;
  console.log(`PASS (${v2 ? 'v2' : 'legacy'}): locale fallback, Shanghai midnight/leap/year boundary, strict helper/API, invalid refresh, correction. Screenshots: ${output}`);
  if (process.argv.includes('--serve')) console.log(`Synthetic preview: ${url}`);
} finally {
  await browser?.close();
  if (!passed || !process.argv.includes('--serve')) await server.close();
}
