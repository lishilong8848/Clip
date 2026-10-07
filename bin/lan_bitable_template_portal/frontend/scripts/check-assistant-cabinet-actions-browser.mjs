import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../..');
const python = path.join(root, 'bin/.venv', process.platform === 'win32' ? 'Scripts/python.exe' : 'bin/python');
const fixtures = JSON.parse(execFileSync(python, ['-c', `import json
from bin.test_lighthouse_cabinet_batch_actions import CabinetBatchActionTests,E_ACTOR
t=CabinetBatchActionTests();t.setUp();items=[]
try:
    for action,flag,label in [('confirm','confirmable','确认'),('rollback','rollbackable','回退'),('restore-rows','restorable','恢复')]:
        agent,writes=t._agent('batch-ui',lambda *_: (_ for _ in ()).throw(AssertionError('No writes')),method=action)
        snapshot={'batch_id':'batch-ui','version':4,'scopes':['E'],'rows':[
            {'row_id':'row-'+rack,'scope':scope,'room':'202','rack':rack,'action':'上测试电','actual':'2026-10-02 09:00:00',flag:True}
            for rack,scope in [('A11','E'),('A12','E'),('B17','A')]]}
        plan=agent.prepare(E_ACTOR,{'title':label+'本批机柜','operations':[{'api_id':'POST /api/cabinet-power/batches/{batch_id}/'+action,'path_params':{'batch_id':'batch-ui'}}]},'browser-'+action,[],queries={'query_batch':snapshot})
        public=agent.public_plan(plan)
        amended=agent.amend(E_ACTOR,plan['id'],{'version':plan['version'],'values':{'step0.row_ids':['row-A11']}})
        assert not writes
        items.append({'action':action,'label':label,'plan':public,'amended':amended})
    print(json.dumps(items,ensure_ascii=False))
finally:
    for callback,args,kwargs in reversed(t._cleanups):callback(*args,**kwargs)
`], { cwd: root, encoding: 'utf8', env: { ...process.env, PYTHONIOENCODING: 'utf-8', PYTHONWARNINGS: 'ignore' } }));
const base = 'http://127.0.0.1:19003';
const output = path.join(root, 'output/playwright/assistant-cabinet-actions');
await mkdir(output, { recursive: true });
const browser = await chromium.launch();
try {
  for (const width of [1440, 390]) {
    const context = await browser.newContext({ viewport: { width, height: 1000 } });
    await context.addCookies([{ name: 'fixture_user', value: 'E', url: base }]);
    assert.equal((await (await context.request.get(base + '/api/health')).json()).instance_id, 'isolated-lighthouse-stream');
    const page = await context.newPage(), errors = [], nativeCalls = [], patches = [];
    let fixture = fixtures[0], plan = fixture.plan;
    page.on('pageerror', error => errors.push(error.message));
    await page.route('**/api/cabinet-power/**', route => { nativeCalls.push(route.request().method()); return route.abort(); });
    await page.route('**/api/assistant/conversation', route => route.fulfill({ json: { ok: true, data: {
      enabled: true, configured: true, busy: false, turns: [{ operation_id: fixture.action, question: '办理本批机柜', answer: '请选择机柜。', status: 'completed', plan }],
    } } }));
    await page.route('**/api/assistant/plans/*', route => {
      assert.equal(route.request().method(), 'PATCH');
      patches.push(route.request().postDataJSON());
      plan = fixture.amended;
      return route.fulfill({ json: { ok: true, data: plan } });
    });
    for (fixture of fixtures) {
      plan = fixture.plan;
      await page.goto(base);
      if (fixture === fixtures[0]) await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
      const form = page.locator('.plan-form');
      await form.waitFor();
      assert.equal(await form.locator('input[type=number],input[type=text],textarea').count(), 0, '版本/行编号不应要求手填');
      assert.equal(await form.getByRole('checkbox').count(), 2, '只显示本楼可操作机柜');
      await form.getByRole('checkbox', { name: /E楼 202\/A11/ }).check();
      assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1), true);
      await page.screenshot({ path: path.join(output, `${fixture.action}-${width}.png`) });
      await form.getByRole('button', { name: '补充并继续', exact: true }).click();
      await page.getByRole('button', { name: '确认操作清单', exact: true }).waitFor();
      assert.deepEqual(patches.at(-1).values, { 'step0.row_ids': ['row-A11'] });
      await page.locator('.plan-steps').getByText(/E楼 202\/A11/).waitFor();
      assert.equal(await page.locator('.plan-steps').getByText('row-A11', { exact: true }).count(), 0);
    }
    assert.equal(patches.length, 3);
    assert.deepEqual(nativeCalls, []);
    assert.deepEqual(errors, []);
    await context.close();
    console.log(`Cabinet actions ${width}px: named row selection, native version, scope isolation and review-only OK`);
  }
} finally { await browser.close(); }
