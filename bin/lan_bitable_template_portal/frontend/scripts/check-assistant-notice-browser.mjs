import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../..');
const python = path.join(root, 'bin/.venv/Scripts/python.exe');
const result = spawnSync(python, ['-c', `import sys,json,tempfile
sys.path.insert(0,'bin')
from pathlib import Path
from unittest.mock import Mock
from fastapi import FastAPI
from clipflow_backend.api_models import WorkbenchActionRequest
from lan_bitable_template_portal.lighthouse_ai import LighthouseAssistant
from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog
from lan_bitable_template_portal.lighthouse_agent import PortalAgent
from lan_bitable_template_portal.lighthouse_files import LighthouseFiles
from lan_bitable_template_portal.workbench_lite import WORK_TYPE_LABELS,REQUIRED_UPLOAD_FIELDS_BY_WORK_TYPE
from test_lighthouse_agent_workflows import ACTOR,Store
tmp=tempfile.TemporaryDirectory();store=Store(Path(tmp.name)/'s.sqlite3')
assistant=LighthouseAssistant(store,Mock(side_effect=AssertionError('No queries')))
app=FastAPI()
@app.post('/api/workbench-actions')
async def submit(body:WorkbenchActionRequest): raise AssertionError('No writes')
agent=PortalAgent(assistant,PortalAPICatalog(app),LighthouseFiles(store))
plans={}
for work in WORK_TYPE_LABELS:
    draft={key:'原填写' for key in REQUIRED_UPLOAD_FIELDS_BY_WORK_TYPE[work]}
    draft.update(title='A楼隔离测试通告',building_codes=['A'],specialty='电气',execution_party='自维',maintenance_cycle='每月',level='低' if work=='repair' else 'I3',start_time='2026-10-03T09:00',end_time='2026-10-03T10:00',notice_type='下电通告',quantity='2')
    p=agent.prepare(ACTOR,{'operations':[{'api_id':'POST /api/workbench-actions','body':{'command_format':'notice_command','scope':'A','action':'start','work_type':work,'manual':True,'manual_binding_choice':'unbound','polling_work_order_exempt':True,'patch':draft}}]},'notice-'+work,[])
    plans[work]=agent.public_plan(p,ACTOR)
print(json.dumps(plans,ensure_ascii=False))
`], { cwd: root, encoding: 'utf8', env: { ...process.env, PYTHONIOENCODING: 'utf-8', PYTHONWARNINGS: 'ignore' } });
assert.equal(result.status, 0, result.stderr);
const plans = JSON.parse(result.stdout);
const base = 'http://127.0.0.1:19003';
const output = path.join(root, 'output/playwright/assistant-notices');
await mkdir(output, { recursive: true });
const browser = await chromium.launch({ headless: true });
try {
  for (const width of [1440, 390]) {
    const context = await browser.newContext({ viewport: { width, height: 1000 } });
    assert.equal((await (await context.request.get(base + '/api/health')).json()).instance_id, 'isolated-lighthouse-stream');
    const page = await context.newPage(), errors = [], saves = [];
    let plan, writes = 0;
    page.on('pageerror', error => errors.push(error.message));
    await page.route('**/api/workbench-actions', route => { writes += 1; return route.abort(); });
    await page.route('**/api/assistant/conversation', route => route.fulfill({ json: { ok: true, data: {
      conversation_id: 'notice-fixture', configured: true, enabled: true, busy: false,
      turns: [{ operation_id: 'notice-fixture', question: '填写通告', answer: '请核对通告。', status: 'completed', plan }],
    } } }));
    await page.route('**/api/assistant/plans/*', route => {
      assert.equal(route.request().method(), 'PATCH');
      const body = route.request().postDataJSON();
      saves.push(body);
      plan = { ...plan, version: plan.version + 1, fields: [], status: 'awaiting_confirmation', can_edit: true };
      return route.fulfill({ json: { ok: true, data: plan } });
    });
    for (const [work, original] of Object.entries(plans)) {
      plan = structuredClone(original);
      await page.goto(base);
      await page.locator('.assistant-launcher, .assistant-panel').waitFor();
      const open = page.getByRole('button', { name: '打开灯塔助手', exact: true });
      if (await open.isVisible()) await open.click();
      const form = page.locator('.plan-form');
      await form.waitFor();
      const fields = original.fields.find(field => field.native_notice).children;
      for (const field of fields) {
        if (field.path === 'building_codes') {
          assert.equal(await form.getByRole('checkbox', { name: 'A楼', exact: true }).isChecked(), true);
          assert.equal(await form.locator('.lh-sf-choices').getByRole('checkbox').count(), 1, 'only authorized buildings offered');
          continue;
        }
        const control = form.getByLabel(field.label, { exact: true });
        assert.equal(await control.count(), 1, `${work}/${field.path} must have one control`);
        if (field.type === 'datetime-local') {
          assert.equal(await control.getAttribute('type'), 'datetime-local');
          await control.fill('2026-10-03T11:30');
        }
        if (field.path === 'specialty') await control.selectOption('暖通');
        if (field.path === 'maintenance_cycle') {
          const list = await control.getAttribute('list');
          assert.ok(list, 'native maintenance cycle suggestions retained');
          assert.ok(await page.locator('datalist option').count() > 2);
          await control.fill('自定义周期');
        }
        if (field.path === 'progress') await control.fill('隔离测试已准备');
      }
      assert.equal(await form.evaluate(el => el.checkValidity()), true, `${work} valid native form`);
      assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
      assert.equal(await form.evaluate(el => el.scrollWidth <= el.clientWidth + 1), true);
      assert.doesNotMatch(await form.innerText(), /query_form_|target_record_id|\$query/);
      assert.equal(await form.locator('textarea').evaluateAll(els => els.some(el => /^\s*\{/.test(el.value))), false, 'no raw JSON editor');
      await page.screenshot({ path: path.join(output, `${work}-${width}.png`), fullPage: true });
      await form.getByRole('button', { name: '补充并继续', exact: true }).click();
      await page.getByRole('button', { name: '确认操作清单', exact: true }).waitFor();
      const draft = saves.at(-1).values['step0.patch'];
      assert.equal(draft.specialty, '暖通');
      assert.equal(draft.progress, '隔离测试已准备');
      assert.equal(draft.start_time, '2026-10-03T11:30');
      if (work === 'maintenance') assert.equal(draft.maintenance_cycle, '自定义周期');
    }
    assert.equal(writes, 0, 'editing/review never sends a business notice');
    assert.deepEqual(errors, []);
    await context.close();
    console.log(`Native notice forms ${width}px: six types/dates/selects/suggestions/scopes/preview/no write/no overflow OK`);
  }
} finally { await browser.close(); }
