import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../..');
const python = path.join(root, 'bin/.venv', process.platform === 'win32' ? 'Scripts/python.exe' : 'bin/python');
// Use the native filtering service with synthetic state, not a second JS implementation.
const fixture = JSON.parse(execFileSync(python, ['-c', `import json,sys
sys.path.insert(0,'bin')
from test_event_month_selection import _make_service
service=_make_service()
for row in service._state_store._snapshot['records']: row['title']=row['record_id']
result={}
for month in ('2026-09','2026-10'):
 result[month]={'overview':service.get_event_monthly_overview(month=month),'scopes':{scope:service.get_event_monthly_snapshot(scope=scope,month=month,date_field='occurrence_time') for scope in ('ALL','110','A','B','C','D','E','H')}}
print(json.dumps(result,ensure_ascii=False))
`], { cwd: root, encoding: 'utf8', env: { ...process.env, PYTHONIOENCODING: 'utf-8' } }));
const base = 'http://127.0.0.1:19003';
const output = path.join(root, 'output/playwright/event-month');
await mkdir(output, { recursive: true });
const browser = await chromium.launch({ headless: true });
try {
  for (const width of [1440, 390]) {
    const context = await browser.newContext({ viewport: { width, height: 1000 } });
    assert.equal((await (await context.request.get(base + '/api/health')).json()).instance_id, 'isolated-lighthouse-stream');
    await context.addCookies([{ name: 'fixture_user', value: 'ALL', url: base }]);
    const page = await context.newPage(), requests = [], errors = [];
    await page.clock.setFixedTime(new Date('2026-10-02T04:00:00Z'));
    page.on('pageerror', error => errors.push(error.message));
    await page.route('**/api/events/**', route => {
      const url = new URL(route.request().url()), month = url.searchParams.get('month');
      assert.equal(route.request().method(), 'GET');
      assert(fixture[month], 'Unexpected month: ' + month);
      if (url.pathname.endsWith('/monthly')) {
        assert.equal(url.searchParams.get('date_field'), 'occurrence_time');
        const scope = url.searchParams.get('scope');
        requests.push(scope);
        return route.fulfill({ json: { ok: true, data: fixture[month].scopes[scope] } });
      }
      assert(url.pathname.endsWith('/overview'));
      return route.fulfill({ json: { ok: true, data: fixture[month].overview } });
    });
    await page.goto(base + '/?mode=events&scope=ALL');
    const month = page.locator('input[type="month"]');
    await month.waitFor();
    await month.fill('2026-10');
    await page.getByText('2 条记录缺少有效的事件发生时间，未计入所选月份。', { exact: true }).waitFor();
    const total = page.locator('.event-stat-card').filter({ hasText: '本月新增事件' }).locator('.stat-main strong');
    assert.equal(await total.innerText(), '3');
    await page.locator('.event-stat-card').filter({ hasText: '本月I3级事件' }).click();
    await page.getByRole('dialog').waitFor();
    const modalText = await page.getByRole('dialog').innerText();
    assert(modalText.includes('rec_c_oct_ended_oct'));
    assert(!modalText.includes('rec_a_sept_ended_oct'));
    assert(requests.length > 0);
    await page.getByRole('dialog').getByRole('button', { name: '关闭', exact: true }).click();
    await page.locator('.building-card').filter({ has: page.locator('strong', { hasText: /^B楼$/ }) }).click();
    await page.locator('.event-list-panel').getByText('rec_b_oct_processing', { exact: true }).waitFor();
    assert.equal(await total.innerText(), '1');
    assert(!await page.locator('.event-list-panel').innerText().then(text => text.includes('rec_a_sept_ended_oct')));
    await month.fill('2026-09');
    await page.getByText('本月暂无事件', { exact: true }).waitFor();
    assert.equal(await total.innerText(), '0');
    await month.fill('2026-10');
    await page.locator('.event-list-panel').getByText('rec_b_oct_processing', { exact: true }).waitFor();
    assert.equal(await total.innerText(), '1');
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
    await page.screenshot({ path: path.join(output, `events-${width}.png`), fullPage: true });
    assert.deepEqual(errors, []);
    await context.close();
  }
  console.log('Native event occurrence-month overview/list/modal desktop/mobile OK');
} finally { await browser.close(); }
