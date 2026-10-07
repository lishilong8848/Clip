import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../..');
const result = spawnSync(path.join(root, 'bin/.venv/Scripts/python.exe'), ['-c', `
import sys,json,asyncio,tempfile
sys.path.insert(0,'bin')
from pathlib import Path
from unittest.mock import Mock
from fastapi import FastAPI,Request
from clipflow_backend.api_models import EventTransferRepairRequest
from lan_bitable_template_portal.lighthouse_agent import PortalAgent
from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog
from lan_bitable_template_portal.lighthouse_ai import LighthouseAssistant
from lan_bitable_template_portal.lighthouse_files import LighthouseFiles
from test_lighthouse_agent_workflows import Store
async def main():
    with tempfile.TemporaryDirectory() as folder:
        store=Store(Path(folder)/'state.sqlite3');app=FastAPI();actor={'id':'fixture','scopes':['A','B'],'is_admin':False}
        @app.post('/api/events/transfer-repair')
        async def transfer(body:EventTransferRepairRequest): raise AssertionError('No writes')
        @app.get('/api/events/monthly')
        async def monthly(scope:str,month:str,date_field:str):
            return {'ok':True,'data':{'month':month,'date_field':date_field,'snapshot_exists':True,'records':[{'record_id':f'rec-{scope}-{month}','scope':scope,'title':f'{scope}楼测试空调报警','occurrence_time':month+'-01 09:00','status':'处理中','transfer_to_overhaul':False}]}}
        agent=PortalAgent(LighthouseAssistant(store,Mock()),PortalAPICatalog(app),LighthouseFiles(store))
        plan=agent.prepare(actor,{'operations':[{'api_id':'POST /api/events/transfer-repair','body':{'scope':'A','month':'2026-10'}}]},'browser-event',[])
        data={'initial':agent.public_plan(plan,actor),'options':{}}
        for scope,month in [('A','2026-10'),('B','2026-10'),('B','2026-09')]:
            request=Request({'type':'http','scheme':'http','server':('testserver',80),'client':('127.0.0.1',1),'path':'/api/assistant','root_path':'','query_string':f'scope={scope}&month={month}'.encode(),'headers':[(b'origin',b'http://testserver')]})
            data['options'][scope+month]=await agent.field_options(actor,plan['id'],'step0.record_id',request)
        print(json.dumps(data,ensure_ascii=False))
asyncio.run(main())
`], { cwd: root, encoding: 'utf8', env: { ...process.env, PYTHONIOENCODING: 'utf-8', PYTHONWARNINGS: 'ignore' } });
assert.equal(result.status, 0, result.stderr);
const fixtures = JSON.parse(result.stdout), base = 'http://127.0.0.1:19003';
const output = path.join(root, 'output/playwright/assistant-event-transfer');
await mkdir(output, { recursive: true });
const browser = await chromium.launch({ headless: true });
try {
  for (const width of [1440, 390]) {
    const context = await browser.newContext({ viewport: { width, height: 1000 } });
    assert.equal((await (await context.request.get(base + '/api/health')).json()).instance_id, 'isolated-lighthouse-stream');
    const page = await context.newPage(), errors = [], reads = [], saves = [];
    let plan = structuredClone(fixtures.initial);
    page.on('pageerror', error => errors.push(error.message));
    await page.route('**/api/assistant/conversation', route => route.fulfill({ json: { ok: true, data: {
      conversation_id: 'event-transfer', configured: true, enabled: true, busy: false,
      turns: [{ operation_id: 'event-transfer', question: '标记事件转检修', answer: '请选择。', status: 'completed', plan }],
    } } }));
    await page.route('**/api/assistant/plans/*/options?*', route => {
      const query = Object.fromEntries(new URL(route.request().url()).searchParams);
      reads.push(query);
      assert.equal(query.selected, '[]');
      plan = structuredClone(fixtures.options[query.scope + query.month]);
      return route.fulfill({ json: { ok: true, data: plan } });
    });
    await page.route('**/api/assistant/plans/*', route => {
      assert.equal(route.request().method(), 'PATCH');
      saves.push(route.request().postDataJSON());
      plan = { ...plan, fields: [], status: 'awaiting_confirmation', version: plan.version + 1,
        operations: [{ ...plan.operations[0], body: { scope: 'B', month: '2026-09', record_id: 'rec-B-2026-09' }, selected_labels: { record_id: 'B楼测试空调报警' } }] };
      return route.fulfill({ json: { ok: true, data: plan } });
    });
    await page.goto(base);
    await page.locator('.assistant-launcher, .assistant-panel').waitFor();
    const launcher = page.getByRole('button', { name: '打开灯塔助手', exact: true });
    if (await launcher.isVisible()) await launcher.click();
    const scope = page.getByLabel('事件楼栋', { exact: true }), month = page.getByLabel('事件月份', { exact: true });
    const record = page.getByLabel('选择转检修事件', { exact: true });
    assert.equal(await month.getAttribute('type'), 'month');
    assert.deepEqual(await scope.locator('option').evaluateAll(rows => rows.map(row => row.value).filter(Boolean)), ['A', 'B']);
    await page.getByRole('button', { name: '查找', exact: true }).click();
    await record.selectOption('rec-A-2026-10');
    await scope.selectOption('B');
    assert.equal(await record.inputValue(), '');
    await page.getByRole('button', { name: '查找', exact: true }).click();
    await record.selectOption('rec-B-2026-10');
    await month.fill('2026-09'); await month.blur();
    assert.equal(await record.inputValue(), '');
    await page.getByRole('button', { name: '查找', exact: true }).click();
    await record.selectOption('rec-B-2026-09');
    assert.equal(reads.length, 3);
    const form = page.locator('.plan-form');
    assert.doesNotMatch(await form.innerText(), /rec-[AB]-|record_id|query_form_/);
    assert.equal(await form.evaluate(element => element.scrollWidth <= element.clientWidth + 1), true);
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
    await page.screenshot({ path: path.join(output, `event-${width}.png`) });
    await page.getByRole('button', { name: '补充并继续', exact: true }).click();
    await page.getByRole('button', { name: '确认操作清单', exact: true }).waitFor();
    assert.deepEqual(saves[0].values, { 'step0.scope': 'B', 'step0.month': '2026-09', 'step0.record_id': 'rec-B-2026-09' });
    assert.ok((await page.locator('.operation-plan').innerText()).includes('标记转检修，未填写维修单'));
    assert.deepEqual(errors, []);
    await context.close();
    console.log(`Event transfer ${width}px: scope/month controls, stale selection reset, exact target, no write OK`);
  }
} finally { await browser.close(); }
