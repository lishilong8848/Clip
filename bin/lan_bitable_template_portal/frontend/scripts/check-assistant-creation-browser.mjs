import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../..');
const fixture = spawnSync(path.join(root, 'bin/.venv/Scripts/python.exe'), ['-c', `
import sys,json,asyncio
sys.path.insert(0,'bin')
from test_lighthouse_creation_workflows import CreationWorkflowTests,ACTOR,MORNING,DRILL
async def main():
    fixture=CreationWorkflowTests();fixture.setUp()
    try:
        plans={}
        for mode,api in [('morning',MORNING),('drill',DRILL)]:
            initial=fixture.prepare(api)
            upload=fixture.fixture.files.upload(ACTOR,'template.xlsx',b'fixture',extract=False) if mode=='drill' else None
            review=fixture.amend(initial,{'name':'演练草稿','month':'2027-01','assigned_scopes':['B','E']} if mode=='drill' else {'weather_condition':'多云','dry_bulb_temperature':21.3},file_id=upload['id'] if upload else None)
            done=await fixture.execute(review)
            plans[mode]={'initial':fixture.agent.public_plan(initial,ACTOR),'review':review,'done':fixture.agent.public_plan(done,ACTOR),'upload':upload}
        print(json.dumps(plans,ensure_ascii=False))
    finally: fixture.fixture.tmp.cleanup()
asyncio.run(main())
`], { cwd: root, encoding: 'utf8', env: { ...process.env, PYTHONIOENCODING: 'utf-8', PYTHONWARNINGS: 'ignore' } });
assert.equal(fixture.status, 0, fixture.stderr);
const plans = JSON.parse(fixture.stdout), base = 'http://127.0.0.1:19003';
const output = path.join(root, 'output/playwright/assistant-creation');
await mkdir(output, { recursive: true });
const browser = await chromium.launch({ headless: true });
try {
  for (const width of [1440, 390]) {
    const context = await browser.newContext({ viewport: { width, height: 1000 } });
    assert.equal((await (await context.request.get(base + '/api/health')).json()).instance_id, 'isolated-lighthouse-stream');
    const page = await context.newPage(), errors = [], saves = [], uploads = [];
    let mode, plan, businessWrites = 0;
    page.on('pageerror', error => errors.push(error.message));
    await page.route('**/api/drills', route => { businessWrites++; return route.abort(); });
    await page.route('**/api/daily-tasks/morning-meeting/generate', route => { businessWrites++; return route.abort(); });
    await page.route('**/api/assistant/conversation', route => route.fulfill({ json: { ok: true, data: {
      conversation_id: 'creation-fixture', configured: true, enabled: true, busy: false,
      turns: [{ operation_id: 'creation-fixture', question: '准备填写', answer: '请核对填写后确认。', status: 'completed', plan }],
    } } }));
    await page.route('**/api/assistant/files?purpose=drill_template', route => {
      uploads.push(route.request().url());
      return route.fulfill({ json: { ok: true, data: { files: [plans.drill.upload] } } });
    });
    await page.route('**/api/assistant/plans/**', route => {
      if (route.request().method() === 'PATCH') {
        saves.push(route.request().postDataJSON());
        plan = structuredClone(plans[mode].review);
      } else if (route.request().url().endsWith('/confirm')) plan = structuredClone(plans[mode].done);
      return route.fulfill({ json: { ok: true, data: plan } });
    });
    for (const current of ['morning', 'drill']) {
      mode = current; plan = structuredClone(plans[mode].initial);
      await page.goto(base);
      await page.locator('.assistant-launcher, .assistant-panel').waitFor();
      const open = page.getByRole('button', { name: '打开灯塔助手', exact: true });
      if (await open.isVisible()) await open.click();
      const form = page.locator('.plan-form'); await form.waitFor();
      const field = plan.fields.find(item => item.type === 'object');
      if (mode === 'morning') {
        const date = form.getByLabel('日期', { exact: true });
        assert.equal(await date.getAttribute('type'), 'date');
        assert.equal(await date.getAttribute('min'), await date.inputValue());
        assert.equal(await date.getAttribute('max'), await date.inputValue());
        await form.getByLabel('天气', { exact: true }).fill('多云');
        const dry = form.getByLabel(field.children.find(child => child.path === 'dry_bulb_temperature').label, { exact: true });
        assert.equal(await dry.getAttribute('type'), 'number');
        assert.equal(await dry.inputValue(), '');
        await dry.fill('81'); assert.equal(await form.evaluate(el => el.checkValidity()), false);
        await dry.fill('21.3');
      } else {
        await form.getByLabel('演练名称', { exact: true }).fill('演练草稿');
        const month = form.getByLabel('演练月份', { exact: true });
        assert.equal(await month.getAttribute('type'), 'month'); await month.fill('2027-01');
        const group = form.getByRole('group', { name: '参演楼栋', exact: true });
        assert.equal(await group.getByRole('checkbox').count(), 5);
        for (const scope of 'ABCDE') await group.getByRole('checkbox', { name: scope + '楼', exact: true }).setChecked('BE'.includes(scope));
        const file = form.getByLabel(plan.fields.find(item => item.type === 'file').label, { exact: true });
        assert.equal(await file.getAttribute('accept'), '.xlsx');
        assert.equal(await file.getAttribute('multiple'), null);
        await file.setInputFiles({ name: 'template.xlsx', mimeType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', buffer: Buffer.from('fixture') });
        await form.getByRole('link', { name: 'template.xlsx', exact: true }).waitFor();
        assert.match(uploads.at(-1), /purpose=drill_template$/);
      }
      assert.equal(await form.evaluate(el => el.checkValidity()), true);
      assert.equal(await form.evaluate(el => el.scrollWidth <= el.clientWidth + 1), true);
      assert.doesNotMatch(await form.innerText(), /\$query|operation_id|assigned_scopes/);
      await page.screenshot({ path: path.join(output, `${mode}-form-${width}.png`), fullPage: true });
      await form.getByRole('button', { name: '补充并继续', exact: true }).click();
      await page.getByRole('button', { name: '确认操作清单', exact: true }).waitFor();
      const values = saves.at(-1).values[field.name];
      if (mode === 'morning') assert.equal(values.dry_bulb_temperature, 21.3);
      else { assert.equal(values.month, '2027-01'); assert.deepEqual(values.assigned_scopes, ['B', 'E']); }
      await page.getByRole('button', { name: '确认操作清单', exact: true }).click();
      if (mode === 'morning') {
        const download = page.getByRole('link', { name: plans.morning.done.results[0].downloads[0].name, exact: true });
        await download.waitFor();
        assert.match(await download.getAttribute('href'), /^\/api\/daily-tasks\/morning-meeting\/download\?date=\d{4}-\d{2}-\d{2}$/);
      } else await page.getByText(plans.drill.done.results[0].query_reply, { exact: true }).waitFor();
      await page.screenshot({ path: path.join(output, `${mode}-result-${width}.png`), fullPage: true });
    }
    assert.equal(businessWrites, 0); assert.deepEqual(errors, []);
    await context.close();
  }
  console.log('Creation browser checks passed: desktop/mobile native fields, file purpose, review, result download; no business writes.');
} finally { await browser.close(); }
