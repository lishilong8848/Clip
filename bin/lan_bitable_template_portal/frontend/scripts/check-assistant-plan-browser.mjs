import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../..');
const python = path.join(root, 'bin/.venv', process.platform === 'win32' ? 'Scripts/python.exe' : 'bin/python');
const fixtures = JSON.parse(execFileSync(python, ['-c', `import asyncio,io,json,sys
sys.path.insert(0,'bin')
from test_lighthouse_plan_workflows import PlanWorkflowTests,ACTOR
from lan_bitable_template_portal.lighthouse_agent import _plan_query_reply
from lan_bitable_template_portal.plan_convergence import PlanConvergenceService
from openpyxl import Workbook
t=PlanWorkflowTests(); t.setUp()
async def derive():
    workbook=Workbook(); sheet=workbook.active; sheet.title='冷机核验'
    sheet.append(['设备域','关联资源','关联设备','关联告警规则'])
    sheet.append(['冷机','设备间','冷机一号,冷机二号','高温'])
    blob=io.BytesIO(); workbook.save(blob)
    excel=PlanConvergenceService.parse_excel(blob.getvalue(),'场景.xlsx')
    detail=await t.agent._invoke(ACTOR,{'api_id':'GET /api/plan-convergence/blocks/{id}','path_params':{'id':'123'}},t.request)
    result={}
    for name,operation,queries in [('match',{'api_id':'POST /api/plan-convergence/rulesets/{id}/match'},{}),
        ('compare',{'api_id':'POST /api/plan-convergence/compare'},{'excel':excel}),
        ('snapshot',{'api_id':'POST /api/plan-convergence/snapshots'},{'detail':detail['_raw']})]:
        plan=t.prepare(operation,queries=queries)
        for field in plan['fields']:
            if field.get('options_source'): await t.agent.field_options(ACTOR,plan['id'],field['name'],t.request)
        result[name]=t.agent.public_plan(t.agent.get_plan(ACTOR,plan['id']))
    native=PlanConvergenceService(t.runtime.state_store).compare({'block_id':'123','scenarios':[{'scenario_name':excel['sheets'][0]['name'],'rows':excel['sheets'][0]['rows']}]})
    result['reply']=_plan_query_reply('POST /api/plan-convergence/compare',native)
    print(json.dumps(result,ensure_ascii=False))
try: asyncio.run(derive())
finally: t.doCleanups()`], { cwd: root, env: { ...process.env, PYTHONIOENCODING: 'utf-8', PYTHONWARNINGS: 'ignore' }, encoding: 'utf8' }));
const output = path.join(root, 'output/playwright/assistant-plan');
await mkdir(output, { recursive: true });
const base = 'http://127.0.0.1:19003';
const browser = await chromium.launch({ headless: true });
try {
  for (const width of [1440, 390]) {
    const context = await browser.newContext({ viewport: { width, height: 1000 } });
    assert.equal((await (await context.request.get(base + '/api/health')).json()).instance_id, 'isolated-lighthouse-stream');
    const page = await context.newPage(), errors = [], saved = [];
    page.on('pageerror', error => errors.push(error.message));
    let plan = structuredClone(fixtures.match);
    const conversation = () => ({ ok: true, data: { conversation_id: 'plan-browser-fixture', configured: true, enabled: true, busy: false,
      turns: [{ operation_id: 'plan-fixture', question: '请核对计划收敛', answer: '请选择核对范围。', status: 'completed', plan }] } });
    await page.route('**/api/assistant/conversation', route => route.fulfill({ json: conversation() }));
    await page.route('**/api/assistant/plans/*/confirm', route => {
      assert.equal(route.request().postDataJSON().stage, 'review');
      plan = { ...plan, status: 'completed', results: [{ ok: true, query_reply: fixtures.reply }] };
      return route.fulfill({ json: { ok: true, data: plan } });
    });
    await page.route('**/api/assistant/plans/*', route => {
      assert.equal(route.request().method(), 'PATCH');
      saved.push(route.request().postDataJSON());
      plan = { ...plan, status: 'awaiting_confirmation', fields: [], version: plan.version + 1 };
      return route.fulfill({ json: { ok: true, data: plan } });
    });
    await page.goto(base);
    await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
    for (const name of ['match', 'compare', 'snapshot']) {
      if (name !== 'match') { plan = structuredClone(fixtures[name]); await page.reload(); }
      const form = page.locator('.plan-form');
      await form.waitFor();
      assert.equal(await form.locator('textarea').count(), 0, '不可要求手写JSON或ID');
      for (const field of plan.fields) {
        assert.equal(field.type, 'select');
        await form.getByLabel(field.label, { exact: true }).selectOption(String(field.options[0].value));
      }
      assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
      await page.screenshot({ path: path.join(output, `${name}-${width}.png`), fullPage: true });
      const expected = Object.fromEntries(plan.fields.map(field => [field.name, field.options[0].value]));
      await form.getByRole('button', { name: '补充并继续', exact: true }).click();
      await page.getByRole('button', { name: '确认操作清单', exact: true }).waitFor();
      assert.deepEqual(saved.at(-1).values, expected);
      if (name === 'compare') {
        await page.getByRole('button', { name: '确认操作清单', exact: true }).click();
        await page.locator('.plan-results .assistant-rich-reply').waitFor();
        assert.match(await page.locator('.plan-results strong').innerText(), /冷机核验[:：]\s*存在缺项/);
        assert.match(await page.locator('.plan-results').innerText(), /冷机二号/);
        await page.screenshot({ path: path.join(output, `result-${width}.png`), fullPage: true });
      }
    }
    assert.deepEqual(errors, []);
    await context.close();
    console.log(`Assistant plan ${width}px: native name/sheet/detail selectors, opaque refs, no JSON, gap results and no overflow OK`);
  }
} finally { await browser.close(); }
