import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../..');
const python = path.join(root, 'bin/.venv', process.platform === 'win32' ? 'Scripts/python.exe' : 'bin/python');
const fixtures = JSON.parse(execFileSync(python, ['-c', `import asyncio,json,sys
sys.path.insert(0,'bin')
from test_lighthouse_daily_water_workflows import DailyWaterWorkflowTests,ACTOR
t=DailyWaterWorkflowTests();t.setUp()
async def derive():
    daily=t.prepare([{'api_id':'POST /api/daily-tasks/send','body':{'scope':'A','date':'2026-10-02'}}])
    field=daily['fields'][0]
    loaded=await t.agent.field_options(ACTOR,daily['id'],field['name'],t.fixture.request)
    chosen=[item['value'] for item in loaded['fields'][0]['options']]
    amended=t.agent.amend(ACTOR,daily['id'],{'version':loaded['version'],'values':{field['name']:chosen}})
    water=t.prepare([{'api_id':'POST /api/capacity/water/records','body':{'scope':'A','meter':'中水','frequency':'每日','shift':'白班',
        'statistic_date':'2026-10-02','meter_value':200}}])
    pending=await t.run_plan(water)
    ready=t.agent.amend(ACTOR,pending['id'],{'version':pending['version'],'values':{pending['fields'][0]['name']:'已核对，本次设备补水。'}})
    print(json.dumps({'daily':{'plan':t.agent.public_plan(daily),'loaded':loaded,'amended':amended},
        'water':{'plan':t.agent.public_plan(pending),'amended':ready}},ensure_ascii=False))
try:asyncio.run(derive())
finally:
    for callback,args,kwargs in reversed(t._cleanups):callback(*args,**kwargs)
    t._cleanups.clear()`], { cwd: root, env: { ...process.env, PYTHONIOENCODING: 'utf-8', PYTHONWARNINGS: 'ignore' }, encoding: 'utf8' }));
const output = path.join(root, 'output/playwright/assistant-daily-water');
assert.equal(fixtures.water.plan.status, 'needs_input', JSON.stringify(fixtures.water.plan));
await mkdir(output, { recursive: true });
const base = 'http://127.0.0.1:19003';
const browser = await chromium.launch({ headless: true });
try {
  for (const width of [1440, 390]) {
    const context = await browser.newContext({ viewport: { width, height: 1000 } });
    assert.equal((await (await context.request.get(base + '/api/health')).json()).instance_id, 'isolated-lighthouse-stream');
    const page = await context.newPage(), errors = [], saves = [];
    page.on('pageerror', error => errors.push(error.message));
    let mode = 'daily', plan = structuredClone(fixtures.daily.plan);
    await page.route('**/api/assistant/conversation', route => route.fulfill({ json: { ok: true, data: {
      conversation_id: 'daily-water-preview', enabled: true, configured: true, busy: false,
      turns: [{ operation_id: 'daily-water', question: mode === 'daily' ? '把工作汇总发送给两位同事' : '保存水耗记录', answer: '请核对以下内容。', status: 'completed', plan }],
    } } }));
    await page.route('**/api/assistant/plans/*/options?*', route => {
      assert.equal(new URL(route.request().url()).searchParams.get('q'), '同名');
      plan = structuredClone(fixtures.daily.loaded);
      return route.fulfill({ json: { ok: true, data: plan } });
    });
    await page.route('**/api/assistant/plans/*', route => {
      assert.equal(route.request().method(), 'PATCH');
      saves.push(route.request().postDataJSON());
      plan = structuredClone(fixtures[mode].amended);
      return route.fulfill({ json: { ok: true, data: plan } });
    });
    await page.goto(base);
    await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
    const form = page.locator('.plan-form');
    await form.getByLabel('查找可选记录').fill('同名');
    await form.getByRole('button', { name: '查找', exact: true }).click();
    for (const label of ['同名人员 · 101', '同名人员 · 102']) await form.getByRole('checkbox', { name: label, exact: true }).check();
    assert.equal(await form.getByRole('checkbox').count(), 2);
    assert.doesNotMatch(await form.innerHTML(), /ou-private|private-token/);
    await page.screenshot({ path: path.join(output, `daily-${width}.png`), fullPage: true });
    await form.getByRole('button', { name: '补充并继续', exact: true }).click();
    await page.getByRole('button', { name: '确认操作清单', exact: true }).waitFor();
    assert.equal(saves.at(-1).values[fixtures.daily.plan.fields[0].name].length, 2);
    assert.match(await page.locator('.plan-steps').innerText(), /收件人[\s\S]*同名人员 · 101、同名人员 · 102/);
    mode = 'water'; plan = structuredClone(fixtures.water.plan);
    await page.reload();
    const reason = form.getByLabel(fixtures.water.plan.fields[0].label, { exact: true });
    try { await reason.waitFor({ timeout: 5000 }); }
    catch (error) {
      await page.screenshot({ path: path.join(output, `water-failure-${width}.png`), fullPage: true });
      console.error('Water form:', await page.locator('.operation-plan').innerText());
      throw error;
    }
    assert.equal(await reason.getAttribute('maxlength'), '1000');
    assert.equal(await form.evaluate(el => el.checkValidity()), false);
    await reason.fill('已核对，本次设备补水。');
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
    await page.screenshot({ path: path.join(output, `water-${width}.png`), fullPage: true });
    await form.getByRole('button', { name: '补充并继续', exact: true }).click();
    await page.getByRole('button', { name: '确认操作清单', exact: true }).waitFor();
    assert.equal(saves.at(-1).values[fixtures.water.plan.fields[0].name], '已核对，本次设备补水。');
    assert.match(await page.locator('.plan-steps').innerText(), /异常原因[\s\S]*已核对[,，]本次设备补水/);
    assert.deepEqual(errors, []);
    await context.close();
    console.log(`Assistant daily/water ${width}px: name choices, no private IDs, required reason, reviewed values and no overflow OK`);
  }
} finally { await browser.close(); }
