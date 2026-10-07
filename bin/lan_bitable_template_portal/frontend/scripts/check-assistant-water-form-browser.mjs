import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../..');
const generated = spawnSync(path.join(root, 'bin/.venv/Scripts/python.exe'), ['-c', `
import sys,json,asyncio,base64
from pathlib import Path
sys.path.insert(0,'bin')
from test_lighthouse_water_workflows import WaterWorkflowTests,ACTOR
async def main():
    f=WaterWorkflowTests();f.setUp()
    try:
        plans={}
        for mode in ('create','edit'):
            initial=f.prepare(editing=mode=='edit')
            photo=f.photo('water.png'); item=f.fixture.files.get(ACTOR,photo)
            review=f.amend(initial,{'meter':'中水','meter_value':120.5,'corrected_usage':-2.25,'retained_image_ids':['old-two'] if mode=='edit' else []},[photo])
            plans[mode]={'initial':f.agent.public_plan(initial,ACTOR),'review':review,'upload':f.fixture.files.public(item),
                         'image':base64.b64encode(Path(item['path']).read_bytes()).decode()}
        print(json.dumps(plans,ensure_ascii=False))
    finally: f.fixture.tmp.cleanup()
asyncio.run(main())
`], { cwd: root, encoding: 'utf8', env: { ...process.env, PYTHONIOENCODING: 'utf-8', PYTHONWARNINGS: 'ignore' } });
assert.equal(generated.status, 0, generated.stderr);
const plans = JSON.parse(generated.stdout), base = 'http://127.0.0.1:19003';
const output = path.join(root, 'output/playwright/assistant-water-form');
await mkdir(output, { recursive: true });
const browser = await chromium.launch({ headless: true });
try {
  for (const width of [1440, 390]) {
    const context = await browser.newContext({ viewport: { width, height: 1000 } });
    assert.equal((await (await context.request.get(base + '/api/health')).json()).instance_id, 'isolated-lighthouse-stream');
    const page = await context.newPage(), errors = [], saved = [], uploads = [];
    let mode, plan, writes = 0;
    page.on('pageerror', error => errors.push(error.message));
    await page.route('**/api/capacity/water/**', route => {
      if (route.request().method() === 'GET' && route.request().url().includes('/images/')) return route.fulfill({ contentType: 'image/png', body: Buffer.from(plans.edit.image, 'base64') });
      writes++; return route.abort();
    });
    await page.route('**/api/assistant/files/**', route => route.fulfill({ contentType: 'image/png', body: Buffer.from(plans.edit.image, 'base64') }));
    await page.route('**/api/assistant/files?purpose=water_photo', route => {
      uploads.push(route.request().url()); return route.fulfill({ json: { ok: true, data: { files: [plans[mode].upload] } } });
    });
    await page.route('**/api/assistant/conversation', route => route.fulfill({ json: { ok: true, data: {
      conversation_id: 'water-fixture', configured: true, enabled: true, busy: false,
      turns: [{ operation_id: 'water-fixture', question: '填写水耗', answer: '请核对填写。', status: 'completed', plan }],
    } } }));
    await page.route('**/api/assistant/plans/**', route => {
      assert.equal(route.request().method(), 'PATCH');
      saved.push(route.request().postDataJSON()); plan = structuredClone(plans[mode].review);
      return route.fulfill({ json: { ok: true, data: plan } });
    });
    for (const current of ['create', 'edit']) {
      mode = current; plan = structuredClone(plans[mode].initial);
      await page.goto(base); await page.locator('.assistant-launcher, .assistant-panel').waitFor();
      const open = page.getByRole('button', { name: '打开灯塔助手', exact: true });
      if (await open.isVisible()) await open.click();
      const form = page.locator('.plan-form'); await form.waitFor();
      const field = plan.fields.find(item => item.native_water_record);
      assert.equal(await form.getByLabel('统计日期', { exact: true }).getAttribute('type'), 'date');
      for (const label of ['水表', '统计频次', '班次']) assert.equal(await form.getByLabel(label, { exact: true }).evaluate(el => el.tagName), 'SELECT');
      await form.getByLabel('水表', { exact: true }).selectOption('中水');
      await form.getByLabel('水表数值', { exact: true }).fill('120.5');
      await form.getByLabel(field.children.find(c => c.path === 'corrected_usage').label, { exact: true }).fill('-2.25');
      const proceed = form.getByRole('button', { name: '补充并继续', exact: true });
      if (mode === 'create') assert.equal(await proceed.isDisabled(), true, 'at least one photo is required');
      else {
        const choices = form.getByRole('group', { name: '保留水表照片' });
        assert.equal(await choices.getByRole('checkbox').count(), 2);
        await choices.getByRole('checkbox', { name: '原照片一.png', exact: true }).uncheck();
        assert.equal(await choices.locator('img').first().evaluate(img => img.complete && img.naturalWidth > 0), true);
        assert.match(await choices.getByRole('link').first().getAttribute('href'), /variant=original$/);
      }
      const zone = form.getByLabel('添加水表照片上传区域', { exact: true });
      await zone.evaluate((el, { image, mode }) => {
        const bytes = Uint8Array.from(atob(image), c => c.charCodeAt(0));
        const data = new DataTransfer(); data.items.add(new File([bytes], 'water.png', { type: 'image/png' }));
        el.dispatchEvent(mode === 'create' ? new ClipboardEvent('paste', { clipboardData: data, bubbles: true, cancelable: true })
          : new DragEvent('drop', { dataTransfer: data, bubbles: true, cancelable: true }));
      }, { image: plans[mode].image, mode });
      await zone.getByRole('link', { name: 'water.png', exact: true }).waitFor();
      assert.equal(await zone.locator('img').evaluate(img => img.complete && img.naturalWidth > 0), true);
      assert.match(uploads.at(-1), /purpose=water_photo$/);
      assert.equal(await proceed.isDisabled(), false);
      await zone.getByRole('button', { name: '移除water.png', exact: true }).click();
      if (mode === 'create') assert.equal(await proceed.isDisabled(), true);
      await form.getByLabel('添加水表照片', { exact: true }).setInputFiles({ name: 'water.png', mimeType: 'image/png', buffer: Buffer.from(plans[mode].image, 'base64') });
      await zone.getByRole('link', { name: 'water.png', exact: true }).waitFor();
      assert.equal(await form.evaluate(el => el.checkValidity()), true);
      assert.equal(await form.evaluate(el => el.scrollWidth <= el.clientWidth + 1), true);
      assert.doesNotMatch(await form.innerText(), /recWater|old-one|expected_version|upload_ids|file_token/);
      await page.screenshot({ path: path.join(output, `${mode}-${width}.png`), fullPage: true });
      await proceed.click(); await page.getByRole('button', { name: '确认操作清单', exact: true }).waitFor();
      const value = saved.at(-1).values[field.name];
      assert.equal(value.meter_value, 120.5); assert.equal(value.corrected_usage, -2.25);
      assert.deepEqual(value.retained_image_ids, mode === 'edit' ? ['old-two'] : []);
      await page.getByText(`保留 ${mode === 'edit' ? 1 : 0} 张，新增 1 张`, { exact: true }).waitFor();
      await page.screenshot({ path: path.join(output, `${mode}-review-${width}.png`), fullPage: true });
    }
    assert.equal(writes, 0); assert.deepEqual(errors, []); await context.close();
  }
  console.log('Water forms passed desktop/mobile: native controls, old-photo preview/selection, paste/drop/upload/remove, review, no business writes.');
} finally { await browser.close(); }
