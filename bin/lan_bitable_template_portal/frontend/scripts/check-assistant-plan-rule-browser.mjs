import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../..');
const python = path.join(root, 'bin/.venv', process.platform === 'win32' ? 'Scripts/python.exe' : 'bin/python');
const fixtures = JSON.parse(execFileSync(python, ['-c', `import json,sys
sys.path.insert(0,'bin')
from test_lighthouse_plan_workflows import PlanWorkflowTests,ACTOR
from lan_bitable_template_portal import plan_convergence_rules as rules
from lan_bitable_template_portal.plan_convergence import PlanConvergenceService
t=PlanWorkflowTests(); t.setUp()
try:
    original=rules.get_set(t.set_id)
    plan=t.prepare({'api_id':'PUT /api/plan-convergence/rulesets/{id}','path_params':{'id':str(t.set_id)}},queries={'original':original})
    queries=[{'kind':'types'},{'kind':'zones'},{'kind':'buildings','zone':'HD8'},
        {'kind':'floors','zone':'HD8','building':'南通数据中心B'},
        {'kind':'rooms','zone':'HD8','building':'南通数据中心B','floor':'B-F2'}]
    for obj in ('','冷机','水泵'):
        for space in ({},{'zone':'HD8'},{'zone':'HD8','building':'南通数据中心B'},
            {'zone':'HD8','building':'南通数据中心B','floor':'B-F2'},
            {'rooms':'HD8|南通数据中心B|B-F2|设备间-H楼2F'}):
            queries.append({'kind':'devices','objs':obj,**space})
    queries.extend({'kind':'rules',**filters} for filters in ({},{'obj':'冷机'},{'obj':'水泵'},
        {'inst':'冷机一号'},{'rooms':'HD8|南通数据中心B|B-F2|设备间-H楼2F'}))
    print(json.dumps({'plan':t.agent.public_plan(plan),'original':original,
        'catalog':[{'params':{k:v for k,v in q.items() if v},'data':PlanConvergenceService.catalog(q)} for q in queries]},ensure_ascii=False))
finally:
    for callback,args,kwargs in reversed(t._cleanups): callback(*args,**kwargs)
    t._cleanups.clear()`], { cwd: root, env: { ...process.env, PYTHONIOENCODING: 'utf-8', PYTHONWARNINGS: 'ignore' }, encoding: 'utf8' }));
const output = path.join(root, 'output/playwright/assistant-plan-rules');
await mkdir(output, { recursive: true });
const base = 'http://127.0.0.1:19003';
const key = params => JSON.stringify(Object.entries(params).filter(([, value]) => value).sort());
const catalog = new Map(fixtures.catalog.map(item => [key(item.params), item.data]));
const browser = await chromium.launch({ headless: true });
try {
  for (const width of [1440, 390]) {
    const context = await browser.newContext({ viewport: { width, height: 1000 } });
    assert.equal((await (await context.request.get(base + '/api/health')).json()).instance_id, 'isolated-lighthouse-stream');
    const page = await context.newPage(), errors = [], writes = [], catalogs = [];
    page.on('pageerror', error => errors.push(error.message));
    let plan = structuredClone(fixtures.plan);
    await page.route('**/api/assistant/conversation', route => route.fulfill({ json: { ok: true, data: {
      conversation_id: 'rule-editor-fixture', configured: true, enabled: true, busy: false,
      turns: [{ operation_id: 'rule-editor', question: '编辑规则集', answer: '请调整屏蔽范围。', status: 'completed', plan }],
    } } }));
    await page.route('**/api/plan-convergence/**', route => {
      const url = new URL(route.request().url());
      assert.equal(route.request().method(), 'GET', '编辑期间不能写入业务');
      assert.equal(url.pathname, '/api/plan-convergence/catalog', '草稿模式不加载另一份可独立保存的规则集');
      const params = Object.fromEntries(url.searchParams);
      catalogs.push(params);
      const data = catalog.get(key(params));
      assert.ok(data, '未预期目录查询: ' + key(params));
      return route.fulfill({ json: { ok: true, data } });
    });
    await page.route('**/api/assistant/plans/*', route => {
      assert.equal(route.request().method(), 'PATCH');
      writes.push(route.request().postDataJSON());
      plan = { ...plan, status: 'awaiting_confirmation', fields: [], version: plan.version + 1 };
      return route.fulfill({ json: { ok: true, data: plan } });
    });
    await page.goto(base);
    await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
    const form = page.locator('.plan-form'), editor = form.locator('.pc-rules');
    await editor.getByLabel('规则集名称', { exact: true }).fill('本次冷机和水泵范围');
    assert.equal(await editor.getByRole('button', { name: '保存规则集', exact: true }).count(), 0);
    assert.equal(await editor.locator('form').count(), 0, '不得嵌套提交表单');
    const choose = (column, label) => editor.locator('.picker-col').nth(column).locator('.cand').filter({ hasText: label }).getByRole('checkbox');
    await choose(0, '冷机').check();
    const space = editor.locator('.picker-col').nth(1);
    for (const label of ['HD8', '南通数据中心B', 'B-F2']) {
      await space.locator('li').filter({ hasText: label }).getByRole('button', { name: '下钻', exact: true }).click();
    }
    await choose(1, '设备间-H楼2F').check();
    await choose(2, '冷机一号').check();
    await choose(3, '高温').check();
    await editor.getByRole('button', { name: '添加屏蔽范围', exact: true }).click();
    await editor.getByLabel('第2组名称').waitFor();
    await choose(0, '水泵').check();
    await editor.getByRole('button', { name: '添加屏蔽范围', exact: true }).click();
    await editor.getByLabel('第3组名称').waitFor();
    const groups = editor.locator('.draft-list > li');
    await groups.nth(0).getByRole('button', { name: '普通', exact: true }).click();
    for (const index of [1, 2]) await groups.nth(index).getByRole('button', { name: '选择参与合并', exact: true }).click();
    await editor.getByRole('button', { name: '合并(2)', exact: true }).click();
    assert.equal(await groups.count(), 2);
    await editor.getByLabel('第2组名称').fill('冷机告警和水泵');
    await groups.nth(1).getByRole('button', { name: '查看条目', exact: true }).click();
    const preview = page.locator('.pc-modal');
    await preview.waitFor();
    assert.equal(await preview.evaluate(el => getComputedStyle(el).backgroundColor), 'rgb(35, 42, 37)', 'teleported rule detail must be dark inside the assistant');
    assert.equal(await preview.evaluate(el => getComputedStyle(el).color), 'rgb(226, 232, 226)');
    await page.screenshot({ path: path.join(output, `detail-${width}.png`), fullPage: true });
    assert.match(await preview.innerText(), /高温/);
    assert.equal(await preview.evaluate(el => { const r = el.getBoundingClientRect(); return el.contains(document.elementFromPoint(r.x + r.width / 2, r.y + r.height / 2)); }), true);
    await page.keyboard.press('Escape');
    await preview.waitFor({ state: 'detached' });
    assert.equal(await editor.isVisible(), true, '关闭子弹窗不得关闭助手');
    await editor.getByRole('button', { name: '清空', exact: true }).click();
    const confirmation = page.getByRole('dialog', { name: '清除全部内容' });
    await confirmation.waitFor();
    await confirmation.getByRole('button', { name: '取消', exact: true }).click();
    assert.equal(await groups.count(), 2);
    assert.equal(writes.length, 0, '选择、合并、取消不得误提交助手表单');
    assert.ok(catalogs.some(p => p.kind === 'devices' && p.rooms === 'HD8|南通数据中心B|B-F2|设备间-H楼2F'));
    assert.ok(catalogs.some(p => p.kind === 'rules' && p.inst === '冷机一号'));
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
    assert.equal(await editor.evaluate(el => el.scrollWidth <= el.clientWidth + 1), true, '编辑器不能横向溢出');
    await page.screenshot({ path: path.join(output, `draft-${width}.png`), fullPage: true });
    await form.getByRole('button', { name: '补充并继续', exact: true }).click();
    await page.getByRole('button', { name: '确认操作清单', exact: true }).waitFor();
    const value = writes.at(-1).values[fixtures.plan.fields[0].name];
    assert.equal(value.name, '本次冷机和水泵范围');
    assert.equal(value.items.length, 3);
    assert.equal(value.items[0].id, fixtures.original.items[0].id);
    assert.equal(value.items[0].rule_type, 'common');
    assert.equal(value.items[1].alarm_config_id, 'ALM-1');
    assert.equal(value.items[1].inst_name, '冷机一号');
    assert.equal(value.items[2].obj_name, '水泵');
    assert.equal(value.items[1].rule_group_no, value.items[2].rule_group_no);
    assert.equal(value.items[1].rule_label, '冷机告警和水泵');
    assert.deepEqual(errors, []);
    await context.close();
    console.log(`Assistant rule editor ${width}px: native cascade, grouping, dialogs, identity, no premature write or overflow OK`);
  }
  const context = await browser.newContext({ viewport: { width: 1440, height: 1000 } });
  await context.addCookies([{ name: 'fixture_user', value: 'ALL', url: base }]);
  const page = await context.newPage(), saves = [];
  let current = structuredClone(fixtures.original);
  await page.route('**/api/plan-convergence/**', route => {
    const url = new URL(route.request().url());
    let data;
    if (url.pathname.endsWith('/catalog')) data = catalog.get(key(Object.fromEntries(url.searchParams)));
    else if (url.pathname.endsWith('/bootstrap')) data = { is_admin: true, catalog_ready: true, blocks: { items: [] } };
    else if (url.pathname.endsWith('/rulesets')) data = [{ ...current, item_count: current.items.length }];
    else if (url.pathname.endsWith('/rulesets/' + current.id)) {
      if (route.request().method() === 'PUT') { saves.push(route.request().postDataJSON()); current = { ...current, ...saves.at(-1) }; }
      data = current;
    }
    assert.ok(data, 'Unexpected standalone request: ' + url.pathname);
    return route.fulfill({ json: { ok: true, data } });
  });
  await page.goto(base + '/plan-convergence?tab=rules');
  const editor = page.locator('.pc-rules');
  await editor.locator('.set-row').click();
  assert.match(await editor.getByLabel('规则集名称', { exact: true }).evaluate(el => getComputedStyle(el).backgroundColor), /^rgba?\(255, 255, 255(?:,|\))/, 'native business pages retain their light theme');
  await editor.getByRole('button', { name: '查看条目', exact: true }).first().click();
  assert.match(await page.locator('.pc-modal').evaluate(el => getComputedStyle(el).backgroundColor), /^rgba?\(255, 255, 255(?:,|\))/);
  await page.keyboard.press('Escape');
  await editor.getByLabel('规则集名称', { exact: true }).fill('原入口仍可保存');
  await editor.getByRole('button', { name: '保存规则集', exact: true }).click();
  await editor.getByText('已保存规则集', { exact: false }).waitFor();
  assert.equal(saves.length, 1);
  assert.equal(saves[0].name, '原入口仍可保存');
  assert.equal(saves[0].items.length, fixtures.original.items.length);
  await page.getByRole('button', { name: '核对台', exact: true }).click();
  const status = page.locator('#pc-block-status');
  await status.waitFor();
  assert.equal(await status.evaluate(el => getComputedStyle(el).backgroundColor), 'rgb(255, 255, 255)');
  await status.click();
  assert.equal(await page.locator('.vnet-select-menu').evaluate(el => getComputedStyle(el).backgroundColor), 'rgb(255, 255, 255)', 'native dropdowns stay light outside the assistant');
  await page.keyboard.press('Escape');
  await context.close();
  console.log('Standalone rule editor: native save remains available and preserves items OK');
} finally { await browser.close(); }
